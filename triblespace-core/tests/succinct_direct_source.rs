//! Semantic receipts for SuccinctArchive query execution.
//!
//! Complete queries are checked against direct fixture relations so physical
//! execution changes cannot alter the public result contract.

use triblespace_core::blob::encodings::succinctarchive::{
    CompressedUniverse, OrderedUniverse, SuccinctArchive, SuccinctArchiveConstraint,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
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

#[test]
fn compressed_universe_value_range_matches_known_values() {
    let (set, _, _, values) = fixture(3, 3, 3);
    let archive: SuccinctArchive<CompressedUniverse> = (&set).into();
    let value = Variable::<UnknownInline>::new(0);
    let mut expected = vec![values[0].raw, values[1].raw];
    expected.sort_unstable();

    assert_eq!(
        sorted_values_ordinary(
            archive.value_in_range(value, values[0], values[1]),
            value.index,
        ),
        expected
    );
    assert_eq!(
        sorted_values_full(
            archive.value_in_range(value, values[0], values[1]),
            value.index,
        ),
        expected
    );
}

#[test]
fn absent_bound_values_and_empty_ranges_have_no_results() {
    let (set, _, attributes, values) = fixture(2, 2, 2);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let value = Variable::<UnknownInline>::new(0);
    let absent_entity: Inline<GenId> = id(90).to_inline();
    let absent_pair = SuccinctArchiveConstraint::new(absent_entity, attributes[0], value, &archive);
    assert!(sorted_values_ordinary(absent_pair.clone(), value.index).is_empty());
    assert!(sorted_values_full(absent_pair, value.index).is_empty());

    assert!(sorted_values_ordinary(
        archive.value_in_range(value, values[1], values[0]),
        value.index,
    )
    .is_empty());
    assert!(sorted_values_full(
        archive.value_in_range(value, values[1], values[0]),
        value.index,
    )
    .is_empty());
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
        .solve_residual_state_lazy()
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
        assert_eq!(
            archive_residual, expected,
            "{name}: production source result set"
        );
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
        .solve_residual_state_lazy()
        .collect();
    values.sort_unstable();
    values
}

#[test]
fn succinct_value_range_ordinary_action_matches_the_tribleset_oracle() {
    let (set, _, _, values) = fixture(2, 2, 6);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = Variable::<UnknownInline>::new(0);
    let min = values[1];
    let max = values[4];

    let expected = sorted_values_full(set.value_in_range(variable, min, max), variable.index);
    let mut known: Vec<_> = values[1..=4].iter().map(|value| value.raw).collect();
    known.sort_unstable();
    assert_eq!(expected, known);
    let archive_ordinary =
        sorted_values_ordinary(archive.value_in_range(variable, min, max), variable.index);
    let archive_residual =
        sorted_values_full(archive.value_in_range(variable, min, max), variable.index);
    assert_eq!(archive_ordinary, expected);
    assert_eq!(archive_residual, expected);
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
fn succinct_constraints_preserve_affine_parent_multiplicity() {
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
    let mut residual: Vec<_> = Query::new(make_root(), project)
        .solve_residual_state_lazy()
        .collect();
    residual.sort_unstable();
    let mut expected: Vec<_> = values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    expected.sort_unstable();
    assert_eq!(residual, expected);
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
