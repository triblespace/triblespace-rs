//! Semantic receipts for normalized proposal paging across Succinct shards.

use std::sync::{Arc, Mutex};

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveConstraint,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, ResidualDeltaOutput,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, TriblePattern, Variable,
    VariableId, VariableSet,
};
use triblespace_core::repo::index_home::UnionArchive;
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

fn fixed_shard(entity: Id, attribute: Id, values: impl IntoIterator<Item = u8>) -> TribleSet {
    let mut set = TribleSet::new();
    for tag in values {
        set.insert(&Trible::force(&entity, &attribute, &value(tag)));
    }
    set
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
        "{name}: supported shard-union schema did not expose its source"
    );

    let mut eager = Vec::new();
    constraint.propose(variable, view, &mut CandidateSink::Values(&mut eager));

    let mut actual = Vec::new();
    let mut cursor = ResidualDeltaSourceCursor::Start;
    loop {
        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let page = constraint
            .residual_delta_source_page(variable, view, None, cursor, 2, &mut roots, &mut direct)
            .unwrap_or_else(|| panic!("{name}: admitted source became unsupported"));
        assert!(page.examined <= 2, "{name}: page exceeded demand");
        assert_eq!(direct.len(), page.examined);
        assert!(roots.is_empty());
        actual.extend(direct);
        let Some(next) = page.next else {
            break;
        };
        match (cursor, next) {
            (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
            (
                ResidualDeltaSourceCursor::After(previous),
                ResidualDeltaSourceCursor::After(next),
            ) => assert!(next > previous, "{name}: cursor did not progress"),
            _ => panic!("{name}: source changed cursor families"),
        }
        cursor = next;
    }

    assert_eq!(actual, eager, "{name}: paging changed normalized order");
    assert!(actual.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn identical_shards_page_one_normalized_union_for_all_twelve_schemas() {
    let (set, entities, attributes, values) = fixture(3, 3, 3);
    let empty = TribleSet::new();
    let archives: Vec<SuccinctArchive<OrderedUniverse>> =
        vec![(&set).into(), (&empty).into(), (&set).into()];
    let union = UnionArchive::new(&archives);
    let entity = Variable::<GenId>::new(0);
    let attribute = Variable::<GenId>::new(1);
    let value = Variable::<UnknownInline>::new(2);
    let constraint = union.pattern(entity, attribute, value);
    assert!(
        constraint.residual_union_children().is_none(),
        "the normalized shard source must remain one atomic formula action"
    );

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

    for (name, variable, vars, row) in &cases {
        let view = if vars.is_empty() {
            RowsView::EMPTY
        } else {
            RowsView::new(vars, row)
        };
        assert_pages_equal_eager(name, &constraint, *variable, &view);
    }
}

#[test]
fn interleaved_shards_page_global_order_and_cross_shard_duplicates_once() {
    let entity = id(0x41);
    let attribute = id(0x42);
    let left = fixed_shard(entity, attribute, [1, 3, 5]);
    let right = fixed_shard(entity, attribute, [2, 3, 4, 6]);
    let archives: Vec<SuccinctArchive<OrderedUniverse>> = vec![(&left).into(), (&right).into()];
    let union = UnionArchive::new(&archives);
    let variable = Variable::<UnknownInline>::new(0);
    let entity: Inline<GenId> = entity.to_inline();
    let attribute: Inline<GenId> = attribute.to_inline();
    let constraint = union.pattern(entity, attribute, variable);

    assert_pages_equal_eager(
        "interleaved/v",
        &constraint,
        variable.index,
        &RowsView::EMPTY,
    );
    let mut eager = Vec::new();
    constraint.propose(
        variable.index,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut eager),
    );
    assert_eq!(eager, (1..=6).map(|tag| value(tag).raw).collect::<Vec<_>>());
}

#[test]
fn generic_union_and_repeated_target_shapes_remain_nonpaged() {
    let (set, entities, attributes, values) = fixture(2, 2, 2);
    let archives: Vec<SuccinctArchive<OrderedUniverse>> = vec![(&set).into(), (&set).into()];
    let value_var = Variable::<UnknownInline>::new(0);
    let generic = UnionConstraint::new(
        archives
            .iter()
            .map(|archive| {
                SuccinctArchiveConstraint::new(entities[0], attributes[0], value_var, archive)
            })
            .collect(),
    );
    assert!(!generic.residual_proposal_source_is_paged(value_var.index, &RowsView::EMPTY));
    assert!(
        generic
            .residual_delta_source_page(
                value_var.index,
                &RowsView::EMPTY,
                None,
                ResidualDeltaSourceCursor::Start,
                1,
                &mut Vec::new(),
                &mut Vec::new(),
            )
            .is_none(),
        "generic OR has no common cursor-family contract"
    );

    let union = UnionArchive::new(&archives);
    let repeated = Variable::<GenId>::new(1);
    let repeated = union.pattern(repeated, repeated, values[0]);
    assert!(!repeated.residual_proposal_source_is_paged(1, &RowsView::EMPTY));
    assert!(repeated
        .residual_delta_source_page(
            1,
            &RowsView::EMPTY,
            None,
            ResidualDeltaSourceCursor::Start,
            1,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .is_none());
}

#[test]
#[should_panic(expected = "UnionArchive source received an ordinal cursor")]
fn shard_union_rejects_ordinal_cursors() {
    let (set, entities, attributes, _) = fixture(1, 1, 1);
    let archives: Vec<SuccinctArchive<OrderedUniverse>> = vec![(&set).into()];
    let union = UnionArchive::new(&archives);
    let value = Variable::<UnknownInline>::new(0);
    let constraint = union.pattern(entities[0], attributes[0], value);
    let _ = constraint.residual_delta_source_page(
        value.index,
        &RowsView::EMPTY,
        None,
        ResidualDeltaSourceCursor::Offset(1),
        1,
        &mut Vec::new(),
        &mut Vec::new(),
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PageTrace {
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    examined: usize,
    accepted: Vec<RawInline>,
    next: Option<ResidualDeltaSourceCursor>,
}

struct SourceTrace<C> {
    inner: C,
    pages: Arc<Mutex<Vec<PageTrace>>>,
}

impl<'a, C> Constraint<'a> for SourceTrace<C>
where
    C: Constraint<'a>,
{
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
        let accepted_base = accepted.len();
        let page = self
            .inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted);
        if let Some(page) = page {
            self.pages.lock().unwrap().push(PageTrace {
                cursor,
                limit,
                examined: page.examined,
                accepted: accepted[accepted_base..].to_vec(),
                next: page.next,
            });
        }
        page
    }
}

fn project_value(binding: &Binding) -> Option<RawInline> {
    binding.get(0).copied()
}

#[test]
fn width_one_drop_and_live_clone_preserve_the_exact_normalized_remainder() {
    let (set, entities, attributes, values) = fixture(1, 1, 8);
    let archives: Vec<SuccinctArchive<OrderedUniverse>> =
        vec![(&set).into(), (&set).into(), (&set).into()];
    let union = UnionArchive::new(&archives);
    let value = Variable::<UnknownInline>::new(0);
    let pages = Arc::new(Mutex::new(Vec::new()));
    let root = Arc::new(SourceTrace {
        inner: union.pattern(entities[0], attributes[0], value),
        pages: Arc::clone(&pages),
    });
    let mut query = Query::new(root, project_value)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);

    let first = query.next().expect("the union has eight values");
    assert_eq!(first, values[0].raw);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    assert_eq!(query.stats().delta_source_roots, 0);
    assert_eq!(
        pages.lock().unwrap().as_slice(),
        [PageTrace {
            cursor: ResidualDeltaSourceCursor::Start,
            limit: 1,
            examined: 1,
            accepted: vec![values[0].raw],
            next: Some(ResidualDeltaSourceCursor::After(values[0].raw)),
        }]
    );

    let clone = query.clone();
    let remainder: Vec<_> = query.collect();
    let cloned_remainder: Vec<_> = clone.collect();
    assert_eq!(cloned_remainder, remainder);
    let reconstructed: Vec<_> = std::iter::once(first).chain(remainder).collect();
    assert_eq!(
        reconstructed,
        values.iter().map(|value| value.raw).collect::<Vec<_>>()
    );

    let drop_pages = Arc::new(Mutex::new(Vec::new()));
    let root = Arc::new(SourceTrace {
        inner: union.pattern(entities[0], attributes[0], value),
        pages: Arc::clone(&drop_pages),
    });
    let mut dropped = Query::new(root, project_value)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    assert_eq!(dropped.next(), Some(values[0].raw));
    let before_drop = drop_pages.lock().unwrap().clone();
    drop(dropped);
    assert_eq!(
        *drop_pages.lock().unwrap(),
        before_drop,
        "dropping the query pulled another normalized page"
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
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

type DynConstraint<'a> = Box<dyn Constraint<'a> + Send + Sync + 'a>;

#[test]
fn normalized_union_preserves_affine_parents_and_monotone_shard_growth() {
    let (base, entities, attributes, base_values) = fixture(1, 1, 3);
    let (grown, _, _, grown_values) = fixture(1, 1, 5);
    let base_archives: Vec<SuccinctArchive<OrderedUniverse>> = vec![(&base).into(), (&base).into()];
    let grown_archives: Vec<SuccinctArchive<OrderedUniverse>> =
        vec![(&base).into(), (&base).into(), (&grown).into()];
    let value = Variable::<UnknownInline>::new(0);
    let parent = Variable::<UnknownInline>::new(1);

    let solve = |archives: &[SuccinctArchive<OrderedUniverse>], sequential| {
        let union = UnionArchive::new(archives);
        let root = IntersectionConstraint::new(vec![
            Box::new(ParentDomain {
                variable: parent.index,
                values: [[201; 32], [202; 32]],
            }) as DynConstraint<'_>,
            Box::new(union.pattern(entities[0], attributes[0], value)) as DynConstraint<'_>,
        ]);
        let query = Query::new(root, project_value);
        let mut results: Vec<_> = if sequential {
            query.sequential().collect()
        } else {
            query
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .start_width(1)
                .cap(1)
                .collect()
        };
        results.sort_unstable();
        results
    };

    let base_sequential = solve(&base_archives, true);
    let base_residual = solve(&base_archives, false);
    assert_eq!(base_residual, base_sequential);
    let mut expected: Vec<_> = base_values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    expected.sort_unstable();
    assert_eq!(base_residual, expected);

    let pages = Arc::new(Mutex::new(Vec::new()));
    let union = UnionArchive::new(&base_archives);
    let root = IntersectionConstraint::new(vec![
        Box::new(ParentDomain {
            variable: parent.index,
            values: [[201; 32], [202; 32]],
        }) as DynConstraint<'_>,
        Box::new(SourceTrace {
            inner: union.pattern(entities[0], attributes[0], value),
            pages: Arc::clone(&pages),
        }) as DynConstraint<'_>,
    ]);
    let profiled = Query::new(root, project_value)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(3)
        .cap(3)
        .collect_profiled();
    let mut profiled_results = profiled.results;
    profiled_results.sort_unstable();
    assert_eq!(profiled_results, expected);
    assert_eq!(profiled.stats.max_delta_source_cohort, 2);
    assert_eq!(profiled.stats.delta_source_cohorts, 2);
    let pages = pages.lock().unwrap();
    assert_eq!(pages.len(), 4);
    assert!(
        pages
            .chunks_exact(2)
            .all(|cohort| cohort.iter().map(|page| page.limit).sum::<usize>() == 3),
        "affine pages multiplied the global geometric width"
    );

    let grown_residual = solve(&grown_archives, false);
    for inherited in base_residual {
        assert!(
            grown_residual.contains(&inherited),
            "adding a shard retracted an affine result"
        );
    }
    let mut grown_expected: Vec<_> = grown_values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    grown_expected.sort_unstable();
    assert_eq!(grown_residual, grown_expected);
}
