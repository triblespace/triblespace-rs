//! Executable capability and scheduler-parity matrix for built-in constraints.
//!
//! Exact projected result sets alone cannot distinguish a native residual capability
//! from the deliberately correct opaque fallback.  These fixtures therefore
//! pair scheduler parity with static capability receipts and residual runtime
//! counters.  A future capability change must update both halves consciously.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use triblespace::core::debug::query::{DebugConstraint, EstimateOverrideConstraint};
use triblespace::core::query::equalityconstraint::EqualityConstraint;
use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
use triblespace::core::query::{
    Binding, Constraint, ConstraintShape, ProgramAction, ProgramExposure, ProgramRequest, Query,
    RowsView, TypedProgramSpec, Variable, VariableId, VariableSet,
};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

#[derive(Debug)]
struct MatrixProfiles {
    conservative_geometric: ResidualStateStats,
    full_geometric: ResidualStateStats,
}

fn sorted_rows<R: Ord>(mut values: Vec<R>) -> Vec<R> {
    values.sort_unstable();
    values
}

/// Runs every execution shape whose semantic agreement matters to the switch.
///
/// The cloned remainder is deliberately taken after one successful pull.  It
/// therefore checks an exact live affine frontier rather than merely proving
/// that a fresh query can be cloned.
fn assert_scheduler_matrix<'a, C, P, R, Make>(
    label: &str,
    expected: Vec<R>,
    make_query: Make,
) -> MatrixProfiles
where
    C: Constraint<'a> + Clone + 'a,
    P: Fn(&Binding) -> Option<R> + Clone,
    R: Clone + Debug + Ord,
    Make: Fn() -> Query<C, P, R>,
{
    let expected = sorted_rows(expected);
    let assert_engine = |engine: &str, actual: Vec<R>| {
        assert_eq!(sorted_rows(actual), expected, "{label}: {engine}");
    };

    assert_engine("sequential oracle", make_query().sequential().collect());
    assert_engine("eager DAG oracle", make_query().solve_dag());
    assert_engine("ordinary production selection", make_query().collect());

    let conservative_eager = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
        .cap(usize::MAX)
        .start_width(usize::MAX)
        .growth(1)
        .collect_profiled();
    assert_engine("conservative eager residual", conservative_eager.results);

    let full_eager = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(usize::MAX)
        .start_width(usize::MAX)
        .growth(1)
        .collect_profiled();
    assert_engine("FULL eager residual", full_eager.results);

    let conservative_width_one = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
        .cap(1)
        .start_width(1)
        .growth(1)
        .collect_profiled();
    assert_engine(
        "conservative fixed width one",
        conservative_width_one.results,
    );

    let full_width_one = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .growth(1)
        .collect_profiled();
    assert_engine("FULL fixed width one", full_width_one.results);

    let conservative_geometric = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
        .cap(16)
        .start_width(1)
        .growth(2)
        .collect_profiled();
    assert_engine("conservative geometric", conservative_geometric.results);

    let full_geometric = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(16)
        .start_width(1)
        .growth(2)
        .collect_profiled();
    assert_engine("FULL geometric", full_geometric.results);

    let mut original = make_query()
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(16)
        .start_width(1)
        .growth(2);
    let first = original
        .next()
        .unwrap_or_else(|| panic!("{label}: nonempty fixture produced no first result"));
    let cloned = original.clone();
    let original_remainder: Vec<_> = original.collect();
    let cloned_remainder: Vec<_> = cloned.collect();
    assert_eq!(
        sorted_rows(original_remainder.clone()),
        sorted_rows(cloned_remainder),
        "{label}: cloned exact remainder"
    );
    assert_engine(
        "first pull plus cloned remainder",
        std::iter::once(first).chain(original_remainder).collect(),
    );

    MatrixProfiles {
        conservative_geometric: conservative_geometric.stats,
        full_geometric: full_geometric.stats,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityReceipt {
    opaque_shape: bool,
    finite_union_arms: Option<usize>,
    page_local_confirm: bool,
    direct_proposal_source: bool,
    typed_program: bool,
}

fn capability_receipt<'a, C: Constraint<'a>>(
    constraint: &C,
    variable: VariableId,
) -> CapabilityReceipt {
    CapabilityReceipt {
        opaque_shape: matches!(constraint.residual_shape(), ConstraintShape::Opaque),
        finite_union_arms: constraint
            .residual_union_children()
            .map(|children| children.len()),
        page_local_confirm: constraint.residual_confirm_is_page_local(),
        direct_proposal_source: constraint
            .residual_proposal_source_is_paged(variable, &RowsView::EMPTY),
        typed_program: constraint.residual_program().is_some(),
    }
}

fn value(tag: u8) -> Inline<UnknownInline> {
    Inline::new([tag; 32])
}

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture ids are nonzero")
}

fn insert_tag(set: &mut TribleSet, entity: &Id, tag: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(entity),
        &triblespace::core::metadata::tag.id(),
        &tag.to_inline(),
    ));
}

#[test]
fn built_in_capability_receipts_distinguish_native_paths_from_opaque_fallbacks() {
    let x = Variable::<UnknownInline>::new(0);
    let y = Variable::<UnknownInline>::new(1);
    let a = value(1);
    let b = value(2);
    let c = value(3);
    let sorted_values = [a, b, c];
    let sorted = SortedSlice::new(&sorted_values).unwrap();
    let set = HashSet::from([a, b]);
    let map = HashMap::from([(a, 1_u8), (b, 2_u8)]);

    assert_eq!(
        capability_receipt(&x.is(a), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: false,
            typed_program: true,
        },
        "ConstantConstraint"
    );
    assert_eq!(
        capability_receipt(&EqualityConstraint::new(x.index, y.index), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: false,
            typed_program: true,
        },
        "EqualityConstraint"
    );
    assert_eq!(
        capability_receipt(&value_range(x, a, c), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: false,
            typed_program: true,
        },
        "InlineRange"
    );
    assert_eq!(
        capability_receipt(&sorted.has(x), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: true,
            typed_program: true,
        },
        "SortedSliceConstraint"
    );
    assert_eq!(
        capability_receipt(&(&set).has(x), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: false,
            typed_program: true,
        },
        "HashSet SetConstraint"
    );
    assert_eq!(
        capability_receipt(&(&map).has(x), x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: false,
            typed_program: true,
        },
        "HashMap KeysConstraint"
    );

    let union = or!(x.is(a), x.is(b), x.is(a));
    assert_eq!(
        capability_receipt(&union, x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: Some(3),
            page_local_confirm: false,
            direct_proposal_source: false,
            typed_program: false,
        },
        "UnionConstraint exposes finite formula arms but keeps group-global confirm"
    );

    let debug = DebugConstraint::new(sorted.has(x), Rc::new(RefCell::new(Vec::new())));
    assert_eq!(
        capability_receipt(&debug, x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: false,
            direct_proposal_source: false,
            typed_program: false,
        },
        "DebugConstraint honestly stays on the instrumented opaque protocol"
    );
    let estimate = EstimateOverrideConstraint::new(sorted.has(x));
    assert_eq!(
        capability_receipt(&estimate, x.index),
        CapabilityReceipt {
            opaque_shape: true,
            finite_union_arms: None,
            page_local_confirm: true,
            direct_proposal_source: true,
            typed_program: true,
        },
        "EstimateOverrideConstraint keeps shape opaque but forwards exact execution capabilities"
    );
    let wrapped_and = EstimateOverrideConstraint::new(and!(x.is(a), y.is(b)));
    assert!(matches!(
        wrapped_and.residual_shape(),
        ConstraintShape::Opaque
    ));
    assert!(
        wrapped_and.residual_union_children().is_none(),
        "opening an estimate wrapper's composite child would bypass its planning override"
    );
}

#[test]
fn atomic_constraints_have_exact_sets_across_residual_widths() {
    let a = value(1);
    let b = value(2);
    let c = value(3);
    let d = value(4);

    let constant = assert_scheduler_matrix(
        "constant",
        vec![a],
        || find!(x: Inline<UnknownInline>, Arc::new(x.is(a))),
    );
    assert_eq!(constant.conservative_geometric.delta_source_pages, 0);
    assert_eq!(constant.full_geometric.delta_source_pages, 1);
    assert_eq!(constant.full_geometric.delta_source_candidates_examined, 1);
    assert_eq!(constant.full_geometric.delta_source_direct_candidates, 1);

    let left = Arc::new(HashSet::from([a, b, c]));
    let right = Arc::new(HashSet::from([b, c, d]));
    let equality = assert_scheduler_matrix("equality", vec![(b, b), (c, c)], || {
        find!(
            (x: Inline<UnknownInline>, y: Inline<UnknownInline>),
            and!(
                left.clone().has(x),
                right.clone().has(y),
                EqualityConstraint::new(x.index, y.index),
            )
        )
    });
    assert_eq!(equality.conservative_geometric.delta_source_pages, 0);
    assert!(
        equality.full_geometric.delta_source_pages > 0,
        "FULL lowering must route peer-bound equality through its typed proposal Program"
    );
    assert!(
        equality.full_geometric.delta_source_direct_candidates > 0,
        "the routed equality Program must publish its exact peer values as direct candidates"
    );

    let domain = Arc::new(HashSet::from([a, b, c, d, value(5)]));
    let range = assert_scheduler_matrix("inclusive inline range", vec![b, c, d], || {
        find!(
            x: Inline<UnknownInline>,
            and!(domain.clone().has(x), value_range(x, b, d))
        )
    });
    assert_eq!(range.conservative_geometric.delta_source_pages, 0);
    assert_eq!(range.full_geometric.delta_source_pages, 0);
}

#[test]
fn membership_constraints_record_native_and_fallback_execution() {
    let a = value(1);
    let b = value(2);
    let c = value(3);
    let sorted_values = [a, a, b, c];
    let sorted = SortedSlice::new(&sorted_values).unwrap();
    let sorted_profiles = assert_scheduler_matrix(
        "sorted slice preserves duplicate proposal occurrences",
        vec![a, b, c],
        || find!(x: Inline<UnknownInline>, Arc::new(sorted.has(x))),
    );
    assert_eq!(
        sorted_profiles.conservative_geometric.delta_source_pages, 0,
        "conservative lowering keeps the atom on the opaque protocol"
    );
    assert!(
        sorted_profiles.full_geometric.delta_source_pages > 0,
        "FULL lowering must exercise SortedSlice's bounded native proposal source"
    );
    assert_eq!(
        sorted_profiles
            .full_geometric
            .delta_source_direct_candidates,
        sorted_values.len()
    );

    let set = Arc::new(HashSet::from([a, b, c]));
    let set_profiles = assert_scheduler_matrix("hash-set membership", vec![a, b, c], || {
        find!(
            x: Inline<UnknownInline>,
            Arc::new(set.clone().has(x))
        )
    });
    assert_eq!(set_profiles.conservative_geometric.delta_source_pages, 0);
    assert_eq!(
        set_profiles.full_geometric.delta_source_pages, 0,
        "HashSet has no honest budgeted proposal cursor"
    );

    let variable = Variable::<UnknownInline>::new(0);
    let set_program = set.clone().has(variable);
    assert!(set_program
        .route(ProgramRequest {
            action: ProgramAction::Propose(variable.index),
            bound: VariableSet::new_empty(),
        })
        .is_none());
    let set_confirm = set_program
        .route(ProgramRequest {
            action: ProgramAction::Confirm(variable.index),
            bound: VariableSet::new_empty(),
        })
        .expect("hash-set confirm Program route");
    assert_eq!(set_confirm.exposure, ProgramExposure::Explicit);

    let lawful_values = [a, b, c];
    let lawful_source = SortedSlice::new(&lawful_values).unwrap();
    let mut full_set: Vec<_> = Query::new(
        and!(lawful_source.has(variable), set.clone().has(variable)),
        move |binding| binding.get(variable.index).copied(),
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1)
    .growth(1)
    .collect();
    full_set.sort_unstable();
    assert_eq!(full_set, [a, b, c].map(|value| value.raw));

    let map = Arc::new(HashMap::from([(a, 10_u8), (b, 20_u8), (c, 30_u8)]));
    let map_profiles = assert_scheduler_matrix("hash-map key membership", vec![a, b, c], || {
        find!(
            x: Inline<UnknownInline>,
            Arc::new(map.clone().has(x))
        )
    });
    assert_eq!(map_profiles.conservative_geometric.delta_source_pages, 0);
    assert_eq!(
        map_profiles.full_geometric.delta_source_pages, 0,
        "HashMap has no honest budgeted proposal cursor"
    );
    let map_program = map.clone().has(variable);
    assert!(map_program
        .route(ProgramRequest {
            action: ProgramAction::Propose(variable.index),
            bound: VariableSet::new_empty(),
        })
        .is_none());
    let map_confirm = map_program
        .route(ProgramRequest {
            action: ProgramAction::Confirm(variable.index),
            bound: VariableSet::new_empty(),
        })
        .expect("hash-map confirm Program route");
    assert_eq!(map_confirm.exposure, ProgramExposure::Explicit);

    let mut full_map: Vec<_> = Query::new(
        and!(lawful_source.has(variable), map.clone().has(variable)),
        move |binding| binding.get(variable.index).copied(),
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1)
    .growth(1)
    .collect();
    full_map.sort_unstable();
    assert_eq!(full_map, [a, b, c].map(|value| value.raw));
}

#[test]
fn finite_union_and_wrappers_have_explicit_execution_receipts() {
    let a = value(1);
    let b = value(2);
    let union_profiles =
        assert_scheduler_matrix("finite union deduplicates per parent", vec![a, b], || {
            find!(
                x: Inline<UnknownInline>,
                or!(x.is(a), x.is(b), x.is(a))
            )
        });
    assert_eq!(union_profiles.conservative_geometric.support_calls, 0);
    assert!(
        union_profiles.full_geometric.support_calls > 0,
        "FULL lowering must execute the exposed finite formula arms"
    );

    let sorted_values = [a, a, b];
    let sorted = SortedSlice::new(&sorted_values).unwrap();
    let debug_record = Rc::new(RefCell::new(Vec::new()));
    let debug_profiles = assert_scheduler_matrix("debug wrapper", vec![a, b], || {
        find!(
            x: Inline<UnknownInline>,
            Arc::new(DebugConstraint::new(
                sorted.has(x),
                Rc::clone(&debug_record)
            ))
        )
    });
    assert!(!debug_record.borrow().is_empty());
    assert_eq!(debug_profiles.conservative_geometric.delta_source_pages, 0);
    assert_eq!(
        debug_profiles.full_geometric.delta_source_pages, 0,
        "direct paging would bypass DebugConstraint's proposal log"
    );

    let estimate_profiles =
        assert_scheduler_matrix("estimate override wrapper", vec![a, b], || {
            find!(
                x: Inline<UnknownInline>,
                Arc::new(EstimateOverrideConstraint::new(sorted.has(x)))
            )
        });
    assert_eq!(
        estimate_profiles.conservative_geometric.delta_source_pages,
        0
    );
    assert_eq!(
        estimate_profiles
            .full_geometric
            .delta_source_direct_candidates,
        sorted_values.len(),
        "EstimateOverrideConstraint must preserve the inner bounded proposal source"
    );
    assert!(estimate_profiles.full_geometric.delta_source_pages > 0);

    let parent_a = value(21);
    let parent_b = value(22);
    let parents = [parent_a, parent_a, parent_b];
    let parents = SortedSlice::new(&parents).unwrap();
    let wrapped_values = [a, a, b];
    let wrapped_values = SortedSlice::new(&wrapped_values).unwrap();
    let affine_profiles = assert_scheduler_matrix(
        "estimate wrapper preserves direct occurrences for every affine parent",
        vec![(parent_a, a), (parent_a, b), (parent_b, a), (parent_b, b)],
        || {
            find!(
                (parent: Inline<UnknownInline>, x: Inline<UnknownInline>),
                and!(
                    parents.has(parent),
                    Arc::new({
                        let mut values = EstimateOverrideConstraint::new(wrapped_values.has(x));
                        values.set_estimate(x.index, 4);
                        values
                    }),
                )
            )
        },
    );
    assert_eq!(affine_profiles.conservative_geometric.delta_source_pages, 0);
    assert!(affine_profiles.full_geometric.delta_source_pages > 0);
}

#[test]
fn estimate_override_forwards_transition_programs_without_opening_its_shape() {
    let start = fixture_id(51);
    let middle = fixture_id(52);
    let end = fixture_id(53);
    let start_value: Inline<GenId> = (&start).to_inline();
    let middle_value: Inline<GenId> = (&middle).to_inline();
    let end_value: Inline<GenId> = (&end).to_inline();
    let mut graph = TribleSet::new();
    insert_tag(&mut graph, &start, &middle);
    insert_tag(&mut graph, &middle, &end);

    let profiles = assert_scheduler_matrix(
        "estimate override around a repeated transition program",
        vec![(start_value, middle_value), (start_value, end_value)],
        || {
            find!(
                (source: Inline<GenId>, target: Inline<GenId>),
                and!(
                    source.is(start_value),
                    Arc::new({
                        let mut path = EstimateOverrideConstraint::new(path!(
                            graph.clone(),
                            source triblespace::core::metadata::tag+ target
                        ));
                        path.set_estimate(target.index, 128);
                        path
                    }),
                )
            )
        },
    );
    assert_eq!(profiles.conservative_geometric.delta_transition_pages, 0);
    assert!(
        profiles.full_geometric.delta_transition_pages > 0,
        "FULL lowering must reach the wrapped path's native transition frontier"
    );
}

#[test]
fn repeated_projected_variable_desugaring_matches_every_scheduler() {
    let self_a = fixture_id(31);
    let self_b = fixture_id(32);
    let other = fixture_id(33);
    let mut data = TribleSet::new();
    insert_tag(&mut data, &self_a, &self_a);
    insert_tag(&mut data, &self_b, &self_b);
    insert_tag(&mut data, &other, &self_a);

    let expected: Vec<Inline<GenId>> = vec![(&self_a).to_inline(), (&self_b).to_inline()];
    assert_scheduler_matrix("repeated projected entity/value variable", expected, || {
        find!(
            entity: Inline<GenId>,
            pattern!(&data, [{ ?entity @ triblespace::core::metadata::tag: ?entity }])
        )
    });
}
