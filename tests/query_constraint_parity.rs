//! Executable representability and scheduler-parity probes for query constraints.
//!
//! These fixtures deliberately distinguish a canonical structural control key
//! from activation-private affine state.  Two equal outer bindings are still
//! two derivation occurrences: a reducer may deduplicate alternatives *within*
//! each occurrence, and a fixpoint may deduplicate path witnesses *within*
//! each occurrence, but neither may accidentally deduplicate the occurrences.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::debug::query::EstimateOverrideConstraint;
use triblespace::core::query::residual::{FormulaScope, ResidualLowering};
use triblespace::core::query::{Binding, Constraint, Query};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod parity {
    use triblespace::prelude::*;

    attributes! {
        "02F7829F30F4D71ACAB744AD72F49ED6" as edge: inlineencodings::GenId;
        "6F86595AC3C32EDBA80FA5F97FEEA2F1" as activates: inlineencodings::GenId;
    }
}

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture tags are non-zero")
}

fn insert_edge(set: &mut TribleSet, from: &Id, to: &Id) {
    insert_relation(set, from, &parity::edge, to);
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

fn multiset<T: Eq + Hash>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
    let mut counts = HashMap::new();
    for item in items {
        *counts.entry(item).or_default() += 1;
    }
    counts
}

fn combined_effects() -> ResidualLowering {
    ResidualLowering::new(FormulaScope::UnionLeaves, true)
}

/// Runs the stable schedulers around one replaceable residual-capability seam.
///
/// Union/RPQ lowering experiments can swap only `run_residual`; the oracle and
/// all surrounding engine comparisons remain unchanged, so capability work
/// does not proliferate public solver entry points or bespoke fixtures.
fn assert_scheduler_matrix<'a, C, P, R, Make>(label: &str, expected: Vec<R>, make_query: Make)
where
    C: Constraint<'a> + Clone + Send + 'a,
    P: Fn(&Binding) -> Option<R> + Clone + Send,
    R: Clone + Debug + Eq + Hash + Send,
    Make: Fn() -> Query<C, P, R>,
{
    let expected = multiset(expected);
    let assert_engine = |engine: &str, actual: Vec<R>| {
        assert_eq!(multiset(actual), expected, "{label}: {engine}");
    };

    assert_engine("sequential", make_query().sequential().collect());
    assert_engine("ordinary shape-selected", make_query().collect());
    assert_engine("lazy DAG", make_query().solve_dag_lazy().collect());

    let mut lazy = make_query().solve_dag_lazy();
    let first = lazy.next();
    assert_eq!(
        first.is_some(),
        !expected.is_empty(),
        "{label}: lazy DAG first-result liveness"
    );
    let lazy_all = first.into_iter().chain(lazy).collect();
    assert_engine("lazy DAG after first pull", lazy_all);

    // This is the single integration seam for every fixture below. Composite
    // effects are selected together; adding another structural capability
    // must extend this value rather than create a solver-specific entry point.
    let run_residual = |query: Query<C, P, R>| {
        query
            .solve_residual_state_lazy_with(combined_effects())
            .collect()
    };
    assert_engine("combined residual capability", run_residual(make_query()));

    #[cfg(feature = "parallel")]
    {
        assert_engine(
            "parallel scalar",
            make_query().into_par_iter().collect::<Vec<_>>(),
        );
        assert_engine(
            "parallel DAG",
            make_query().into_par_dag_iter().collect::<Vec<_>>(),
        );
        assert_engine(
            "parallel residual",
            make_query()
                .solve_residual_state_lazy_with(combined_effects())
                .into_par_iter()
                .collect::<Vec<_>>(),
        );
    }
}

/// A finite reducer's branch accumulator is activation-private.
///
/// Three distinct hidden activation entities map to projected bindings
/// `[a, a, b]`.  Unlike duplicate candidates for one parent (which set-valued
/// constraints may legitimately deduplicate), these are three derivation rows.
/// Exactly one Union arm is live for each row.  Lowering is allowed to cohort
/// their shared structural action, but must neither let a dead row-local arm
/// contribute nor merge the two equal projected `a` activations.
#[test]
fn union_dead_arms_are_row_local_and_duplicate_activations_are_affine() {
    let a = fixture_id(1);
    let b = fixture_id(2);
    let y_a = fixture_id(11);
    let y_b = fixture_id(12);
    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let y_a_v: Inline<GenId> = (&y_a).to_inline();
    let y_b_v: Inline<GenId> = (&y_b).to_inline();
    let activation_1 = fixture_id(31);
    let activation_2 = fixture_id(32);
    let activation_3 = fixture_id(33);
    let mut activations = TribleSet::new();
    insert_relation(&mut activations, &activation_1, &parity::activates, &a);
    insert_relation(&mut activations, &activation_2, &parity::activates, &a);
    insert_relation(&mut activations, &activation_3, &parity::activates, &b);

    let expected = vec![(a_v, y_a_v), (a_v, y_a_v), (b_v, y_b_v)];
    assert_scheduler_matrix("row-varying-dead-union-arm", expected, || {
        find!((x: Inline<GenId>, y: Inline<GenId>),
            and!(
                pattern!(&activations, [{ _?activation @ parity::activates: ?x }]),
                or!(
                    and!(x.is(a_v), y.is(y_a_v)),
                    and!(x.is(b_v), y.is(y_b_v)),
                ),
            )
        )
    });
}

/// Union confirmation is a finite set reducer, not a pointwise filter: equal
/// candidate values for one parent collapse after the live arms are merged.
#[test]
fn union_confirm_deduplicates_equal_candidates_within_one_parent() {
    let a_v: Inline<GenId> = (&fixture_id(41)).to_inline();
    let candidates = [a_v, a_v];
    let candidates = SortedSlice::new(&candidates).unwrap();

    assert_scheduler_matrix("union-confirm-deduplicates-one-parent", vec![a_v], || {
        find!(x: Inline<GenId>, {
            let mut source = EstimateOverrideConstraint::new(candidates.has(x));
            source.set_estimate(x.index, 0);
            and!(source, or!(x.is(a_v), x.is(a_v)))
        })
    });
}

/// A recursive fixpoint's visited/frontier/result sets are activation-private.
///
/// `a -> c` and `a -> b -> c` are duplicate witnesses for `c`; each source
/// activation must emit `c` once.  The `a <-> b` cycle makes the Plus closure
/// include its origin via a non-empty path.  Finally `[a, a, b]` requires the
/// two equal outer `a` occurrences to survive as two independent activations.
#[test]
fn rpq_plus_deduplicates_witnesses_not_outer_activations() {
    let a = fixture_id(21);
    let b = fixture_id(22);
    let c = fixture_id(23);
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &a, &b);
    insert_edge(&mut graph, &b, &a);
    insert_edge(&mut graph, &a, &c);
    insert_edge(&mut graph, &b, &c);

    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let c_v: Inline<GenId> = (&c).to_inline();
    let outer = [a_v, a_v, b_v];
    let outer = SortedSlice::new(&outer).unwrap();

    let expected = vec![
        (a_v, a_v),
        (a_v, a_v),
        (a_v, b_v),
        (a_v, b_v),
        (a_v, c_v),
        (a_v, c_v),
        (b_v, a_v),
        (b_v, b_v),
        (b_v, c_v),
    ];
    assert_scheduler_matrix("rpq-plus-affine-fixpoint", expected, || {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                outer.has(source),
                path!(graph.clone(), source parity::edge+ target),
            )
        )
    });
}

/// In contrast to Union, RPQ confirmation is a pointwise reachability filter.
/// It may reject candidates but must preserve equal occurrences that were
/// supplied by another constraint for the same parent.
#[test]
fn rpq_confirm_preserves_equal_candidate_occurrences() {
    let source_id = fixture_id(51);
    let target_id = fixture_id(52);
    let source_v: Inline<GenId> = (&source_id).to_inline();
    let target_v: Inline<GenId> = (&target_id).to_inline();
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &source_id, &target_id);
    let target_candidates = [target_v, target_v];
    let target_candidates = SortedSlice::new(&target_candidates).unwrap();

    assert_scheduler_matrix(
        "rpq-confirm-preserves-candidate-occurrences",
        vec![(source_v, target_v), (source_v, target_v)],
        || {
            find!((source: Inline<GenId>, target: Inline<GenId>), {
                let mut start = EstimateOverrideConstraint::new(source.is(source_v));
                start.set_estimate(source.index, 0);
                let mut targets = EstimateOverrideConstraint::new(target_candidates.has(target));
                targets.set_estimate(target.index, 1);
                and!(
                    start,
                    targets,
                    path!(graph.clone(), source parity::edge+ target),
                )
            })
        },
    );
}

/// Both effect families activate sequentially in one root without sharing
/// affine state. The finite Union proposes `source`; once that value is bound,
/// the eligible `edge+` RPQ delta-proposes `target` for each resulting row.
#[test]
fn finite_union_and_cyclic_rpq_coexist_in_one_root() {
    let a = fixture_id(61);
    let b = fixture_id(62);
    let c = fixture_id(63);
    let d = fixture_id(64);
    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let c_v: Inline<GenId> = (&c).to_inline();
    let d_v: Inline<GenId> = (&d).to_inline();
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &a, &b);
    insert_edge(&mut graph, &b, &a);
    insert_edge(&mut graph, &b, &c);
    insert_edge(&mut graph, &d, &c);

    let expected = vec![(a_v, a_v), (a_v, b_v), (a_v, c_v), (d_v, c_v)];
    assert_scheduler_matrix("finite-union-plus-cyclic-rpq", expected, || {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(source.is(a_v), source.is(d_v)),
                path!(graph.clone(), source parity::edge+ target),
            )
        )
    });
}
