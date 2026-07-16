//! Semantic receipts for proposal-only SuccinctArchive source paging.
//!
//! These tests keep the source contract visible: the twelve triple-pattern
//! bound schemas are compared page-for-page with ordinary eager proposals,
//! complete queries are checked against TribleSet's sequential scheduler, and
//! first-pull/drop receipts prove that width-one demand does not materialize a
//! large archive frontier.

use std::sync::{Arc, Mutex};

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, ResidualDeltaOutput,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, TriblePattern, Variable,
    VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

fn id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are nonzero")
}

fn value(tag: u8) -> Inline<UnknownInline> {
    Inline::new([tag; 32])
}

fn fixture(
    entity_count: u8,
    attribute_count: u8,
    value_count: u8,
) -> (
    TribleSet,
    Vec<Inline<GenId>>,
    Vec<Inline<GenId>>,
    Vec<Inline<UnknownInline>>,
) {
    let entities: Vec<_> = (1..=entity_count).map(|tag| id(tag).to_inline()).collect();
    let attributes: Vec<_> = (1..=attribute_count)
        .map(|tag| id(32 + tag).to_inline())
        .collect();
    let values: Vec<_> = (1..=value_count).map(|tag| value(96 + tag)).collect();
    let mut set = TribleSet::new();
    for entity in &entities {
        for attribute in &attributes {
            for value in &values {
                let entity = Id::new(entity.raw[16..].try_into().unwrap()).unwrap();
                let attribute = Id::new(attribute.raw[16..].try_into().unwrap()).unwrap();
                set.insert(&Trible::force(&entity, &attribute, value));
            }
        }
    }
    (set, entities, attributes, values)
}

/// Exact CPU implementation of the optional Ring batch seam. Source paging
/// must be identical with this attached even though direct candidates do not
/// need a batched confirmation probe.
struct CpuRing<'a>(&'a SuccinctArchive<OrderedUniverse>);

impl RingBatchQuery for CpuRing<'_> {
    fn rank_batch(
        &self,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        positions
            .iter()
            .zip(values)
            .map(|(&position, &value)| self.0.ring_col(rotation).rank(position, value).unwrap())
            .collect()
    }
}

fn assert_pages_equal_eager<'a, C>(
    name: &str,
    constraint: &C,
    variable: VariableId,
    view: &RowsView<'_>,
) where
    C: Constraint<'a> + ?Sized,
{
    assert!(
        constraint.residual_proposal_source_is_paged(variable, view),
        "{name}: supported proposal schema did not expose its source"
    );

    let mut eager = Vec::new();
    constraint.propose(variable, view, &mut CandidateSink::Values(&mut eager));

    let mut actual = Vec::new();
    let mut cursor = ResidualDeltaSourceCursor::Start;
    let mut pages = 0usize;
    loop {
        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let page = constraint
            .residual_delta_source_page(variable, view, None, cursor, 1, &mut roots, &mut direct)
            .unwrap_or_else(|| panic!("{name}: advertised source became unsupported"));
        assert!(page.examined <= 1, "{name}: page exceeded demand");
        assert!(
            roots.len() + direct.len() <= page.examined,
            "{name}: roots + direct exceeded examined"
        );
        assert!(roots.is_empty(), "{name}: a direct source invented roots");
        assert_eq!(
            direct.len(),
            page.examined,
            "{name}: an exact archive source rejected its own candidate"
        );
        actual.extend(direct);
        pages += 1;
        assert!(pages <= eager.len() + 1, "{name}: source did not terminate");

        let Some(next) = page.next else {
            break;
        };
        match (cursor, next) {
            (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
            (
                ResidualDeltaSourceCursor::After(previous),
                ResidualDeltaSourceCursor::After(next),
            ) => assert!(next > previous, "{name}: cursor failed strict progress"),
            (_, ResidualDeltaSourceCursor::Start) => panic!("{name}: cursor restarted"),
        }
        cursor = next;
    }

    assert_eq!(actual, eager, "{name}: paged proposal changed exact order");
}

#[test]
fn all_twelve_pattern_bound_schemas_page_exactly_on_cpu_and_ring_backend() {
    let (set, entities, attributes, values) = fixture(3, 3, 3);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let ring = CpuRing(&archive);
    let entity = Variable::<GenId>::new(0);
    let attribute = Variable::<GenId>::new(1);
    let value = Variable::<UnknownInline>::new(2);

    let cases = [
        ("zero/e", entity.index, vec![], vec![]),
        ("zero/a", attribute.index, vec![], vec![]),
        ("zero/v", value.index, vec![], vec![]),
        (
            "e/a",
            attribute.index,
            vec![entity.index],
            vec![entities[0].raw],
        ),
        (
            "e/v",
            value.index,
            vec![entity.index],
            vec![entities[0].raw],
        ),
        (
            "a/e",
            entity.index,
            vec![attribute.index],
            vec![attributes[0].raw],
        ),
        (
            "a/v",
            value.index,
            vec![attribute.index],
            vec![attributes[0].raw],
        ),
        ("v/e", entity.index, vec![value.index], vec![values[0].raw]),
        (
            "v/a",
            attribute.index,
            vec![value.index],
            vec![values[0].raw],
        ),
        (
            "av/e",
            entity.index,
            vec![attribute.index, value.index],
            vec![attributes[0].raw, values[0].raw],
        ),
        (
            "ev/a",
            attribute.index,
            vec![entity.index, value.index],
            vec![entities[0].raw, values[0].raw],
        ),
        (
            "ea/v",
            value.index,
            vec![entity.index, attribute.index],
            vec![entities[0].raw, attributes[0].raw],
        ),
    ];

    for backend in [false, true] {
        let constraint = if backend {
            SuccinctArchiveConstraint::with_ring_batch(entity, attribute, value, &archive, &ring)
        } else {
            SuccinctArchiveConstraint::new(entity, attribute, value, &archive)
        };
        for (schema, variable, vars, row) in &cases {
            let view = if vars.is_empty() {
                RowsView::EMPTY
            } else {
                RowsView::new(vars, row)
            };
            assert_pages_equal_eager(
                &format!("{schema}/{}", if backend { "ring" } else { "cpu" }),
                &constraint,
                *variable,
                &view,
            );
        }
    }
}

fn project_pattern(axes: [VariableId; 3]) -> impl Fn(&Binding) -> Option<[RawInline; 3]> {
    move |binding| {
        Some([
            *binding.get(axes[0])?,
            *binding.get(axes[1])?,
            *binding.get(axes[2])?,
        ])
    }
}

fn sorted_sequential<'a, C>(constraint: C, axes: [VariableId; 3]) -> Vec<[RawInline; 3]>
where
    C: Constraint<'a> + 'a,
{
    let mut rows: Vec<_> = Query::new(constraint, project_pattern(axes))
        .sequential()
        .collect();
    rows.sort_unstable();
    rows
}

fn sorted_residual<'a, C>(constraint: C, axes: [VariableId; 3]) -> Vec<[RawInline; 3]>
where
    C: Constraint<'a> + 'a,
{
    let mut rows: Vec<_> = Query::new(constraint, project_pattern(axes))
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .collect();
    rows.sort_unstable();
    rows
}

#[test]
fn each_zero_bound_axis_drains_to_the_tribleset_sequential_oracle() {
    let (set, _, _, _) = fixture(3, 3, 3);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    for (name, e_index, a_index, v_index) in [
        ("e-first", 0, 1, 2),
        ("a-first", 1, 0, 2),
        ("v-first", 1, 2, 0),
    ] {
        let entity = Variable::<GenId>::new(e_index);
        let attribute = Variable::<GenId>::new(a_index);
        let value = Variable::<UnknownInline>::new(v_index);
        let axes = [e_index, a_index, v_index];
        let expected = sorted_sequential(set.pattern(entity, attribute, value), axes);
        let archive_sequential = sorted_sequential(archive.pattern(entity, attribute, value), axes);
        let archive_residual = sorted_residual(archive.pattern(entity, attribute, value), axes);
        assert_eq!(
            archive_sequential, expected,
            "{name}: archive scalar oracle"
        );
        assert_eq!(archive_residual, expected, "{name}: residual source bag");
    }
}

fn sorted_values<'a, C>(constraint: C, variable: VariableId, residual: bool) -> Vec<RawInline>
where
    C: Constraint<'a> + 'a,
{
    let project = move |binding: &Binding| binding.get(variable).copied();
    let mut values: Vec<_> = if residual {
        Query::new(constraint, project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect()
    } else {
        Query::new(constraint, project).sequential().collect()
    };
    values.sort_unstable();
    values
}

#[test]
fn succinct_value_range_pages_and_matches_the_tribleset_oracle() {
    let (set, _, _, values) = fixture(2, 2, 6);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let min = values[1];
    let max = values[4];

    let source = archive.value_in_range(variable, min, max);
    assert_pages_equal_eager("value-range", &source, variable.index, &RowsView::EMPTY);
    let expected = sorted_values(
        set.value_in_range(variable, min, max),
        variable.index,
        false,
    );
    let archive_sequential = sorted_values(
        archive.value_in_range(variable, min, max),
        variable.index,
        false,
    );
    let archive_residual = sorted_values(
        archive.value_in_range(variable, min, max),
        variable.index,
        true,
    );
    assert_eq!(archive_sequential, expected);
    assert_eq!(archive_residual, expected);
}

#[derive(Clone)]
struct SourceTrace<C> {
    inner: C,
    pages: Arc<Mutex<Vec<(usize, usize, usize, usize)>>>,
}

impl<'a, C: Constraint<'a>> Constraint<'a> for SourceTrace<C> {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.inner.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        let roots_before = roots.len();
        let accepted_before = accepted.len();
        let page = self
            .inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted);
        if let Some(page) = page {
            self.pages.lock().unwrap().push((
                limit,
                page.examined,
                roots.len() - roots_before,
                accepted.len() - accepted_before,
            ));
        }
        page
    }
}

#[test]
fn first_pull_is_one_direct_candidate_and_drop_cancels_the_rest() {
    let (set, entities, attributes, values) = fixture(1, 1, 24);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let pages = Arc::new(Mutex::new(Vec::new()));
    let root = SourceTrace {
        inner: SuccinctArchiveConstraint::new(entities[0], attributes[0], variable, &archive),
        pages: Arc::clone(&pages),
    };
    let mut query = Query::new(root, move |binding: &Binding| {
        binding.get(variable.index).copied()
    })
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .cap(1);

    assert_eq!(query.next(), Some(values[0].raw));
    assert_eq!(*pages.lock().unwrap(), vec![(1, 1, 0, 1)]);
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_roots, 0);
    drop(query);
    assert_eq!(
        *pages.lock().unwrap(),
        vec![(1, 1, 0, 1)],
        "dropping the iterator must not pull another source page"
    );
}

#[derive(Clone)]
struct ParentDomain {
    variable: VariableId,
    values: [RawInline; 2],
}

impl<'a> Constraint<'a> for ParentDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            for row in 0..view.len() as u32 {
                candidates.extend_row(row, self.values);
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|_, value| self.values.contains(value));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable).map_or(true, |column| {
            view.iter().all(|row| self.values.contains(&row[column]))
        })
    }
}

type DynConstraint<'a> = Box<dyn Constraint<'a> + 'a>;

#[test]
fn direct_sources_preserve_affine_parent_multiplicity() {
    let (set, entities, attributes, values) = fixture(1, 1, 4);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let parent = Variable::<UnknownInline>::new(1);
    let make_root = || {
        IntersectionConstraint::new(vec![
            Box::new(ParentDomain {
                variable: parent.index,
                values: [[201; 32], [202; 32]],
            }) as DynConstraint<'_>,
            Box::new(SuccinctArchiveConstraint::new(
                entities[0],
                attributes[0],
                variable,
                &archive,
            )) as DynConstraint<'_>,
        ])
    };
    let project = move |binding: &Binding| binding.get(variable.index).copied();
    let mut residual_query = Query::new(make_root(), project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1);
    let mut residual: Vec<_> = residual_query.by_ref().collect();
    let mut sequential: Vec<_> = Query::new(make_root(), project).sequential().collect();
    residual.sort_unstable();
    sequential.sort_unstable();
    let mut expected: Vec<_> = values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    expected.sort_unstable();
    assert_eq!(residual, sequential);
    assert_eq!(residual, expected);
    assert_eq!(residual_query.stats().delta_source_candidates_examined, 8);
    assert_eq!(residual_query.stats().delta_source_roots, 0);
}

fn fixed_pair_results(
    set: &TribleSet,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
) -> Vec<RawInline> {
    let archive: SuccinctArchive<OrderedUniverse> = set.into();
    let variable = Variable::<UnknownInline>::new(0);
    sorted_values(
        SuccinctArchiveConstraint::new(entity, attribute, variable, &archive),
        variable.index,
        true,
    )
}

#[test]
fn monotone_archive_growth_only_adds_direct_source_results() {
    let (base, entities, attributes, _) = fixture(1, 1, 2);
    let (grown, _, _, _) = fixture(1, 1, 5);
    let before = fixed_pair_results(&base, entities[0], attributes[0]);
    let after = fixed_pair_results(&grown, entities[0], attributes[0]);
    assert!(
        before.iter().all(|value| after.contains(value)),
        "monotone archive growth retracted a direct proposal"
    );
    assert!(before.len() < after.len());
}
