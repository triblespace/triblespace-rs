//! Semantic receipts for normalized production Program paging across Succinct shards.

use std::sync::Arc;

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
    Binding, CandidateSink, Constraint, EstimateSink, ProposalCoverage, Query, RowsView,
    TriblePattern, Variable, VariableId, VariableSet,
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
        "{name}: normalized archive has no typed Program family",
    );
}

#[test]
fn identical_shards_use_one_production_program_for_all_twelve_schemas() {
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
        assert_typed_program_family(name, &constraint, *variable, &view);
    }
}

#[test]
fn interleaved_shards_keep_eager_order_and_production_program_set_parity() {
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

    assert_typed_program_family(
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
    let mut full: Vec<_> = Query::new(constraint, project_value)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .collect();
    full.sort_unstable();
    assert_eq!(full, eager);
}

#[test]
fn generic_union_keeps_its_boundary_and_repeated_targets_keep_full_parity() {
    let (set, entities, attributes, _) = fixture(2, 2, 2);
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
    assert!(
        generic.residual_program().is_none(),
        "generic OR keeps its normalization boundary",
    );

    let union = UnionArchive::new(&archives);
    let x = Variable::<GenId>::new(1);
    for (name, ordinary, full) in [
        (
            "E=V",
            union.pattern(x, attributes[0], x),
            union.pattern(x, attributes[0], x),
        ),
        (
            "E=A",
            union.pattern(x, x, entities[0]),
            union.pattern(x, x, entities[0]),
        ),
        (
            "A=V",
            union.pattern(entities[0], x, x),
            union.pattern(entities[0], x, x),
        ),
        ("E=A=V", union.pattern(x, x, x), union.pattern(x, x, x)),
    ] {
        let mut ordinary_values: Vec<_> =
            Query::new(ordinary, |binding: &Binding| binding.get(x.index).copied()).collect();
        let mut full_values: Vec<_> =
            Query::new(full, |binding: &Binding| binding.get(x.index).copied())
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .start_width(1)
                .collect();
        ordinary_values.sort_unstable();
        full_values.sort_unstable();
        assert_eq!(full_values, ordinary_values, "{name}");
    }
}

fn project_value(binding: &Binding) -> Option<RawInline> {
    binding.get(0).copied()
}

#[test]
fn width_one_production_program_and_live_clone_preserve_the_exact_normalized_remainder() {
    let (set, entities, attributes, values) = fixture(1, 1, 8);
    let archives: Vec<SuccinctArchive<OrderedUniverse>> =
        vec![(&set).into(), (&set).into(), (&set).into()];
    let union = UnionArchive::new(&archives);
    let value = Variable::<UnknownInline>::new(0);
    let root = Arc::new(union.pattern(entities[0], attributes[0], value));
    let mut query = Query::new(root, project_value)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);

    let first = query.next().expect("the union has eight values");
    assert_eq!(first, values[0].raw);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    assert_eq!(query.stats().delta_source_roots, 0);

    let clone = query.clone();
    let remainder: Vec<_> = query.collect();
    let cloned_remainder: Vec<_> = clone.collect();
    assert_eq!(cloned_remainder, remainder);
    let reconstructed: Vec<_> = std::iter::once(first).chain(remainder).collect();
    assert_eq!(
        reconstructed,
        values.iter().map(|value| value.raw).collect::<Vec<_>>()
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

    let solve = |archives: &[SuccinctArchive<OrderedUniverse>], conservative| {
        let union = UnionArchive::new(archives);
        let root = IntersectionConstraint::new(vec![
            Box::new(ParentDomain {
                variable: parent.index,
                values: [[201; 32], [202; 32]],
            }) as DynConstraint<'_>,
            Box::new(union.pattern(entities[0], attributes[0], value)) as DynConstraint<'_>,
        ]);
        let query = Query::new(root, project_value);
        let mut results: Vec<_> = if conservative {
            query
                .solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
                .collect()
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

    let base_conservative = solve(&base_archives, true);
    let base_full = solve(&base_archives, false);
    assert_eq!(base_full, base_conservative);
    let mut expected: Vec<_> = base_values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    expected.sort_unstable();
    assert_eq!(base_full, expected);

    let grown_full = solve(&grown_archives, false);
    for inherited in base_full {
        assert!(
            grown_full.contains(&inherited),
            "adding a shard retracted an affine result"
        );
    }
    let mut grown_expected: Vec<_> = grown_values
        .iter()
        .flat_map(|value| [value.raw, value.raw])
        .collect();
    grown_expected.sort_unstable();
    assert_eq!(grown_full, grown_expected);
}
