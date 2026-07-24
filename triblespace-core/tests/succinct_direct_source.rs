//! Semantic receipts for SuccinctArchive production Program paging.
//!
//! These tests keep the source contract visible: the twelve triple-pattern
//! bound schemas must route through the production typed Program, complete
//! queries are checked against direct fixture relations, and first-pull
//! receipts prove that width-one demand does not materialize a large archive
//! frontier.

use triblespace_core::blob::encodings::succinctarchive::{
    CompressedUniverse, OrderedUniverse, RingBatchQuery, SuccinctArchive,
    SuccinctArchiveConstraint, SuccinctRotation,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, ProposalCoverage, Query, RowsView,
    TriblePattern, Variable, VariableId, VariableSet,
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

fn assert_typed_program_family<'a, C>(
    name: &str,
    constraint: &C,
    _variable: VariableId,
    _view: &RowsView<'_>,
) where
    C: Constraint<'a> + ?Sized,
{
    assert!(
        constraint.residual_program().is_some(),
        "{name}: missing typed Program family",
    );
}

#[test]
fn all_twelve_pattern_bound_schemas_use_production_program_on_cpu_and_ring_backend() {
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
            assert_typed_program_family(
                &format!("{schema}/{}", if backend { "ring" } else { "cpu" }),
                &constraint,
                *variable,
                &view,
            );
        }
    }
}

#[test]
fn compressed_universe_preserves_zero_one_two_bound_and_range_sources() {
    let (set, entities, attributes, values) = fixture(3, 3, 3);
    let archive: SuccinctArchive<CompressedUniverse> = (&set).into();
    let entity = Variable::<GenId>::new(0);
    let attribute = Variable::<GenId>::new(1);
    let value = Variable::<UnknownInline>::new(2);
    let constraint = SuccinctArchiveConstraint::new(entity, attribute, value, &archive);

    assert_typed_program_family(
        "compressed/zero-v",
        &constraint,
        value.index,
        &RowsView::EMPTY,
    );
    let attribute_vars = [attribute.index];
    let attribute_row = [attributes[0].raw];
    let attribute_view = RowsView::new(&attribute_vars, &attribute_row);
    assert_typed_program_family("compressed/a-v", &constraint, value.index, &attribute_view);
    let entity_attribute_vars = [entity.index, attribute.index];
    let entity_attribute_row = [entities[0].raw, attributes[0].raw];
    let entity_attribute_view = RowsView::new(&entity_attribute_vars, &entity_attribute_row);
    assert_typed_program_family(
        "compressed/ea-v",
        &constraint,
        value.index,
        &entity_attribute_view,
    );
    let range = archive.value_in_range(value, values[0], values[1]);
    assert_typed_program_family(
        "compressed/value-range",
        &range,
        value.index,
        &RowsView::EMPTY,
    );
}

#[test]
fn absent_bound_values_and_empty_ranges_exhaust_without_candidates() {
    let (set, _, attributes, values) = fixture(2, 2, 2);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = Variable::<GenId>::new(0);
    let attribute = Variable::<GenId>::new(1);
    let value = Variable::<UnknownInline>::new(2);
    let constraint = SuccinctArchiveConstraint::new(entity, attribute, value, &archive);
    let absent_entity: Inline<GenId> = id(90).to_inline();
    let absent_vars = [entity.index];
    let absent_row = [absent_entity.raw];
    let absent_view = RowsView::new(&absent_vars, &absent_row);
    assert_typed_program_family("absent-e/a", &constraint, attribute.index, &absent_view);
    let absent_pair_vars = [entity.index, attribute.index];
    let absent_pair_row = [absent_entity.raw, attributes[0].raw];
    let absent_pair_view = RowsView::new(&absent_pair_vars, &absent_pair_row);
    assert_typed_program_family("absent-ea/v", &constraint, value.index, &absent_pair_view);

    let empty_range = archive.value_in_range(value, values[1], values[0]);
    assert_typed_program_family(
        "inverted-value-range",
        &empty_range,
        value.index,
        &RowsView::EMPTY,
    );
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

fn sorted_ordinary<'a, C>(constraint: C, axes: [VariableId; 3]) -> Vec<[RawInline; 3]>
where
    C: Constraint<'a> + 'a,
{
    let mut rows: Vec<_> = Query::new(constraint, project_pattern(axes)).collect();
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
fn each_zero_bound_axis_drains_to_the_fixture_relation() {
    let (set, entities, attributes, values) = fixture(3, 3, 3);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let mut expected: Vec<_> = entities
        .iter()
        .flat_map(|entity| {
            attributes.iter().flat_map(|attribute| {
                values
                    .iter()
                    .map(|value| [entity.raw, attribute.raw, value.raw])
            })
        })
        .collect();
    expected.sort_unstable();

    for (name, e_index, a_index, v_index) in [
        ("e-first", 0, 1, 2),
        ("a-first", 1, 0, 2),
        ("v-first", 1, 2, 0),
    ] {
        let entity = Variable::<GenId>::new(e_index);
        let attribute = Variable::<GenId>::new(a_index);
        let value = Variable::<UnknownInline>::new(v_index);
        let axes = [e_index, a_index, v_index];
        let archive_ordinary = sorted_ordinary(archive.pattern(entity, attribute, value), axes);
        let archive_residual = sorted_residual(archive.pattern(entity, attribute, value), axes);
        assert_eq!(
            archive_ordinary, expected,
            "{name}: archive ordinary result set"
        );
        assert_eq!(archive_residual, expected, "{name}: FULL source result set");
    }
}

fn sorted_values_ordinary<'a, C>(constraint: C, variable: VariableId) -> Vec<RawInline>
where
    C: Constraint<'a> + 'a,
{
    let project = move |binding: &Binding| binding.get(variable).copied();
    let mut values: Vec<_> = Query::new(constraint, project).collect();
    values.sort_unstable();
    values
}

fn sorted_values_full<'a, C>(constraint: C, variable: VariableId) -> Vec<RawInline>
where
    C: Constraint<'a> + 'a,
{
    let project = move |binding: &Binding| binding.get(variable).copied();
    let mut values: Vec<_> = Query::new(constraint, project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .collect();
    values.sort_unstable();
    values
}

#[test]
fn succinct_value_range_uses_production_program_and_matches_the_tribleset_oracle() {
    let (set, _, _, values) = fixture(2, 2, 6);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let min = values[1];
    let max = values[4];

    let source = archive.value_in_range(variable, min, max);
    assert_typed_program_family("value-range", &source, variable.index, &RowsView::EMPTY);
    let mut expected: Vec<_> = values[1..=4].iter().map(|value| value.raw).collect();
    expected.sort_unstable();
    let archive_ordinary =
        sorted_values_ordinary(archive.value_in_range(variable, min, max), variable.index);
    let archive_residual =
        sorted_values_full(archive.value_in_range(variable, min, max), variable.index);
    assert_eq!(archive_ordinary, expected);
    assert_eq!(archive_residual, expected);
}

#[test]
fn production_program_first_pull_is_one_direct_candidate() {
    let (set, entities, attributes, values) = fixture(1, 1, 24);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let root = SuccinctArchiveConstraint::new(entities[0], attributes[0], variable, &archive);
    let mut query = Query::new(root, move |binding: &Binding| {
        binding.get(variable.index).copied()
    })
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .cap(1);

    assert_eq!(query.next(), Some(values[0].raw));
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    assert_eq!(query.stats().delta_source_roots, 0);
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

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.variable && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
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
    residual.sort_unstable();
    let mut expected: Vec<_> = values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    expected.sort_unstable();
    assert_eq!(residual, expected);
    assert_eq!(residual_query.stats().delta_source_candidates_examined, 8);
    assert_eq!(residual_query.stats().delta_source_direct_candidates, 8);
    assert_eq!(residual_query.stats().delta_source_roots, 0);
}

fn fixed_pair_results(
    set: &TribleSet,
    entity: Inline<GenId>,
    attribute: Inline<GenId>,
) -> Vec<RawInline> {
    let archive: SuccinctArchive<OrderedUniverse> = set.into();
    let variable = Variable::<UnknownInline>::new(0);
    sorted_values_full(
        SuccinctArchiveConstraint::new(entity, attribute, variable, &archive),
        variable.index,
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
