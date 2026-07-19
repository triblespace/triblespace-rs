//! Soundness tests for `UnionConstraint` (`or!`) and the `satisfied()`
//! protocol law.
//!
//! Two shipped bugs motivated these tests, and engine-vs-engine parity
//! checks could not catch either (both engines share the composite
//! constraints):
//!
//! 1. `UnionConstraint::propose` let every variant work on one shared
//!    scratch vector. A composite variant (an `and!`) filters the sink it
//!    is handed through its children's `confirm`, so a later variant could
//!    delete candidates an earlier variant produced — the union's result
//!    depended on variant order, and monotonically growing the data behind
//!    a `pattern_changes!` query could *remove* results (CALM violation).
//!
//! 2. The union skips dead variants based on `satisfied()`, but several
//!    leaf constraints left the optimistic default `true` even when their
//!    variable was fully bound — so a dead variant kept proposing values
//!    for the union's other variables, emitting rows no single variant
//!    accepts.

use proptest::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::rngid;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::shortstring::ShortString;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::patch::{Entry, IdentitySchema, PATCH};
use triblespace_core::prelude::*;
use triblespace_core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace_core::query::residual::{FormulaScope, ResidualLowering, ResidualShadowEpoch};
use triblespace_core::query::sortedsliceconstraint::SortedSlice;
use triblespace_core::query::{
    Constraint, ContainsConstraint, RowsView, TriblePattern, Variable, VariableContext,
};

mod test_ns {
    use triblespace_core::prelude::*;
    attributes! {
        "CC00000000000000CC00000000000001" as pub rel_r: inlineencodings::ShortString;
        "CC00000000000000CC00000000000002" as pub rel_s: inlineencodings::ShortString;
        "CC00000000000000CC00000000000003" as pub link: inlineencodings::GenId;
    }
}

/// The canonical 32-byte value form of a 16-byte id: left-padded with
/// zeros (mirrors the crate-private `id_into_value`).
fn id_value(id: &[u8; 16]) -> [u8; 32] {
    let mut data = [0u8; 32];
    data[16..32].copy_from_slice(id);
    data
}

// ── Bug #2: union variants must not filter each other's proposals ──────

/// `or!` over composite (`and!`) arms whose intersections confirm after
/// proposing. With a shared scratch vector the second arm's confirming
/// constant deleted the first arm's candidate; the result was `[two]`
/// instead of `[one, two]` and depended on arm order.
#[test]
fn or_with_composite_arms_is_order_independent() {
    let one = rngid();
    let two = rngid();
    let one_val: Inline<GenId> = (&one).to_inline();
    let two_val: Inline<GenId> = (&two).to_inline();

    let mut forward: Vec<[u8; 32]> = find!(
        x: Inline<GenId>,
        or!(
            and!(x.is(one_val), x.is(one_val)),
            and!(x.is(two_val), x.is(two_val)),
        )
    )
    .map(|x: Inline<GenId>| x.raw)
    .collect();
    forward.sort_unstable();

    let mut backward: Vec<[u8; 32]> = find!(
        x: Inline<GenId>,
        or!(
            and!(x.is(two_val), x.is(two_val)),
            and!(x.is(one_val), x.is(one_val)),
        )
    )
    .map(|x: Inline<GenId>| x.raw)
    .collect();
    backward.sort_unstable();

    let mut expected = vec![one_val.raw, two_val.raw];
    expected.sort_unstable();

    assert_eq!(
        forward, expected,
        "or!(A, B) must contain the results of every arm"
    );
    assert_eq!(
        backward, expected,
        "or!(B, A) must equal or!(A, B) — a union is arm-order independent"
    );
}

/// The CALM counterexample: `pattern_changes!` results must never shrink
/// when full and delta grow monotonically.
///
/// full1 = {R(a), S(a)}, delta1 = {R(a)} yields `a` (R from delta joins S
/// from full). Growing to full2 = {R(a), S(a), S(b), S(c)},
/// delta2 = {R(a), S(b), S(c)} must still yield `a` — under the shared-
/// scratch bug the full-R/delta-S variant's confirm scrubbed `a` out of
/// the shared vector and the result went EMPTY.
#[test]
fn pattern_changes_monotone_growth_keeps_results() {
    let a = rngid();
    let b = rngid();
    let c = rngid();

    let mut delta1 = TribleSet::new();
    delta1 += entity! { &a @ test_ns::rel_r: "r" };
    let mut full1 = delta1.clone();
    full1 += entity! { &a @ test_ns::rel_s: "s" };

    // Monotone growth: add S(b) and S(c) to both full and delta.
    let mut growth = TribleSet::new();
    growth += entity! { &b @ test_ns::rel_s: "s" };
    growth += entity! { &c @ test_ns::rel_s: "s" };
    let full2 = full1.clone() + growth.clone();
    let delta2 = delta1.clone() + growth;

    let baseline_query =
        |full: &TribleSet, delta: &TribleSet, sequential: bool| -> HashSet<[u8; 32]> {
            let query = find!(
            e: Inline<GenId>,
            pattern_changes!(full, delta, [
                { ?e @ test_ns::rel_r: _?rv, test_ns::rel_s: _?sv }
            ])
            );
            if sequential {
                query.sequential().map(|e: Inline<GenId>| e.raw).collect()
            } else {
                query.map(|e: Inline<GenId>| e.raw).collect()
            }
        };

    let residual_query = |full: &TribleSet, delta: &TribleSet, lowering: ResidualLowering| {
        find!(
            e: Inline<GenId>,
            pattern_changes!(full, delta, [
                { ?e @ test_ns::rel_r: _?rv, test_ns::rel_s: _?sv }
            ])
        )
        .solve_residual_state_lazy_with(lowering)
        .shadow(ResidualShadowEpoch::new())
        .collect_profiled()
    };

    let ordinary_before = baseline_query(&full1, &delta1, false);
    let ordinary_after = baseline_query(&full2, &delta2, false);
    assert_eq!(
        baseline_query(&full1, &delta1, true),
        ordinary_before,
        "the ordinary and sequential schedulers must agree before growth"
    );
    assert_eq!(
        baseline_query(&full2, &delta2, true),
        ordinary_after,
        "the ordinary and sequential schedulers must agree after growth"
    );

    // Transition programs cannot affect this RPQ-free fixture. Formula scope
    // is a chain: WholeRoot absorbs UnionLeaves, so there are three forms.
    let cases = [
        ("opaque", ResidualLowering::CONSERVATIVE, false),
        (
            "union-leaves",
            ResidualLowering::new(FormulaScope::UnionLeaves, false),
            false,
        ),
        (
            "whole-root",
            ResidualLowering::new(FormulaScope::WholeRoot, false),
            true,
        ),
    ];

    let raw_set = |results: &[Inline<GenId>]| -> HashSet<[u8; 32]> {
        results.iter().map(|value| value.raw).collect()
    };
    let mut action_counts = Vec::new();
    for (name, lowering, synthetic_root) in cases {
        let before = residual_query(&full1, &delta1, lowering);
        let after = residual_query(&full2, &delta2, lowering);
        let before_set = raw_set(&before.results);
        let after_set = raw_set(&after.results);

        assert_eq!(
            before_set, ordinary_before,
            "residual capability case {name} must match ordinary execution before growth"
        );
        assert_eq!(
            after_set, ordinary_after,
            "residual capability case {name} must match ordinary execution after growth"
        );
        assert!(
            before_set.is_subset(&after_set),
            "residual capability case {name} must preserve monotone pattern_changes! growth"
        );

        action_counts.push(after.shadow.events.len());
        if synthetic_root {
            let occurrences: HashSet<_> = after
                .shadow
                .events
                .iter()
                .map(|event| event.site.leaf_occurrence)
                .collect();
            assert!(
                occurrences.len() >= 2 && occurrences.iter().all(|&occurrence| occurrence > 0),
                "synthetic-root lowering must execute multiple compiled formula occurrences"
            );
        }
    }
    assert!(
        action_counts[1] > action_counts[0],
        "finite-Union lowering must execute its child actions, not the opaque Union fallback"
    );

    let a_val: Inline<GenId> = (&a).to_inline();
    assert!(
        ordinary_before.contains(&a_val.raw),
        "sanity: `a` joins R (delta) with S (full) in the initial state"
    );
    assert!(
        ordinary_after.contains(&a_val.raw),
        "monotonicity: growing full+delta must not remove `a` from the result"
    );
    assert!(
        ordinary_before.is_subset(&ordinary_after),
        "monotonicity: Q(A) ⊆ Q(A ∪ B) for pattern_changes!"
    );
}

// ── Bug #3: satisfied() must be exact when fully bound ──────────────────

/// A dead union variant must not propose values for the union's other
/// variables. With `a ∈ set1, a ∉ set2` the second arm is dead once
/// `x = a` is bound; before `SetConstraint::satisfied` did the exact
/// check, the impossible row `(a, two)` was emitted.
#[test]
fn dead_union_variant_cannot_emit_impossible_rows() {
    let set1: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
    let set2: HashSet<String> = ["b", "c"].iter().map(|s| s.to_string()).collect();

    let a_val: Inline<ShortString> = "a".to_inline();
    let one_val: Inline<ShortString> = "one".to_inline();
    let two_val: Inline<ShortString> = "two".to_inline();

    let mut rows: Vec<([u8; 32], [u8; 32])> = find!(
        (x: Inline<ShortString>, y: Inline<ShortString>),
        and!(
            x.is(a_val),
            or!(
                and!(set1.has(x), y.is(one_val)),
                and!(set2.has(x), y.is(two_val)),
            )
        )
    )
    .map(|(x, y): (Inline<ShortString>, Inline<ShortString>)| (x.raw, y.raw))
    .collect();
    rows.sort_unstable();

    assert_eq!(
        rows,
        vec![(a_val.raw, one_val.raw)],
        "only (a, one) is derivable; (a, two) would require a ∈ set2"
    );
}

/// Exercises every leaf constraint that gained an exact fully-bound
/// `satisfied()`: bound-to-member ⇒ true, bound-to-non-member ⇒ false,
/// unbound ⇒ optimistic true. The block-native protocol expresses "bound
/// to v" as a single-row [`RowsView`] with one column; `RowsView::EMPTY`
/// is the seed block (one zero-width row — nothing bound).
#[test]
fn set_constraint_satisfied_exact_when_bound() {
    let set: HashSet<String> = ["a"].iter().map(|s| s.to_string()).collect();
    let mut ctx = VariableContext::new();
    let x: Variable<ShortString> = ctx.next_variable();
    let constraint = (&set).has(x);

    let member: Inline<ShortString> = "a".to_inline();
    let outsider: Inline<ShortString> = "z".to_inline();

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let vars = [x.index];
    assert!(
        constraint.satisfied(&RowsView::new(&vars, &[member.raw])),
        "bound to member: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&vars, &[outsider.raw])),
        "bound to non-member: false"
    );
}

#[test]
fn keys_constraint_satisfied_exact_when_bound() {
    let mut map: HashMap<String, u32> = HashMap::new();
    map.insert("a".to_string(), 1);
    let mut ctx = VariableContext::new();
    let x: Variable<ShortString> = ctx.next_variable();
    let constraint = (&map).has(x);

    let member: Inline<ShortString> = "a".to_inline();
    let outsider: Inline<ShortString> = "z".to_inline();

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let vars = [x.index];
    assert!(
        constraint.satisfied(&RowsView::new(&vars, &[member.raw])),
        "bound to key: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&vars, &[outsider.raw])),
        "bound to non-key: false"
    );
}

#[test]
fn sorted_slice_constraint_satisfied_exact_when_bound() {
    let data: Vec<String> = vec!["a".to_string(), "b".to_string()];
    let slice = SortedSlice::new(&data).unwrap();
    let mut ctx = VariableContext::new();
    let x: Variable<ShortString> = ctx.next_variable();
    let constraint = slice.has(x);

    let member: Inline<ShortString> = "b".to_inline();
    let outsider: Inline<ShortString> = "z".to_inline();

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let vars = [x.index];
    assert!(
        constraint.satisfied(&RowsView::new(&vars, &[member.raw])),
        "bound to member: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&vars, &[outsider.raw])),
        "bound to non-member: false"
    );
}

#[test]
fn patch_value_constraint_satisfied_exact_when_bound() {
    let member: [u8; 32] = [7; 32];
    let outsider: [u8; 32] = [9; 32];
    let mut patch: PATCH<32, IdentitySchema, ()> = PATCH::new();
    patch.insert(&Entry::new(&member));

    let mut ctx = VariableContext::new();
    let x: Variable<UnknownInline> = ctx.next_variable();
    let constraint = (&patch).has(x);

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let vars = [x.index];
    assert!(
        constraint.satisfied(&RowsView::new(&vars, &[member])),
        "bound to member: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&vars, &[outsider])),
        "bound to non-member: false"
    );
}

#[test]
fn patch_id_constraint_satisfied_exact_when_bound() {
    let member_id: [u8; 16] = [7; 16];
    let outsider_id: [u8; 16] = [9; 16];
    let mut patch: PATCH<16, IdentitySchema, ()> = PATCH::new();
    patch.insert(&Entry::new(&member_id));

    let mut ctx = VariableContext::new();
    let x: Variable<GenId> = ctx.next_variable();
    let constraint = patch.has(x);

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let vars = [x.index];
    assert!(
        constraint.satisfied(&RowsView::new(&vars, &[id_value(&member_id)])),
        "bound to member: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&vars, &[id_value(&outsider_id)])),
        "bound to non-member: false"
    );
}

#[test]
fn regular_path_constraint_satisfied_exact_when_bound() {
    let a = rngid();
    let b = rngid();
    let c = rngid();
    let mut set = TribleSet::new();
    set += entity! { &a @ test_ns::link: &b };

    let mut ctx = VariableContext::new();
    let s: Variable<GenId> = ctx.next_variable();
    let d: Variable<GenId> = ctx.next_variable();
    let constraint = RegularPathConstraint::new(set, s, d, &[PathOp::Attr(test_ns::link.raw())]);

    let a_val: Inline<GenId> = (&a).to_inline();
    let b_val: Inline<GenId> = (&b).to_inline();
    let c_val: Inline<GenId> = (&c).to_inline();

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let s_vars = [s.index];
    assert!(
        constraint.satisfied(&RowsView::new(&s_vars, &[a_val.raw])),
        "one endpoint bound: optimistic true"
    );
    let sd_vars = [s.index, d.index];
    assert!(
        constraint.satisfied(&RowsView::new(&sd_vars, &[a_val.raw, b_val.raw])),
        "a → b exists: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&sd_vars, &[a_val.raw, c_val.raw])),
        "a → c does not exist: false"
    );
}

#[test]
fn succinct_archive_constraint_satisfied_exact_when_bound() {
    let e = rngid();
    let mut set = TribleSet::new();
    set += entity! { &e @ test_ns::rel_r: "x" };
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let ve: Variable<GenId> = ctx.next_variable();
    let va: Variable<GenId> = ctx.next_variable();
    let vv: Variable<ShortString> = ctx.next_variable();
    let constraint = archive.pattern(ve, va, vv);

    let e_val: Inline<GenId> = (&e).to_inline();
    let a_val = id_value(&test_ns::rel_r.raw());
    let present: Inline<ShortString> = "x".to_inline();
    let absent: Inline<ShortString> = "y".to_inline();

    assert!(
        constraint.satisfied(&RowsView::EMPTY),
        "unbound: optimistic true"
    );
    let ea_vars = [ve.index, va.index];
    assert!(
        constraint.satisfied(&RowsView::new(&ea_vars, &[e_val.raw, a_val])),
        "value unbound: optimistic true"
    );
    let eav_vars = [ve.index, va.index, vv.index];
    assert!(
        constraint.satisfied(&RowsView::new(&eav_vars, &[e_val.raw, a_val, present.raw])),
        "triple present: true"
    );
    assert!(
        !constraint.satisfied(&RowsView::new(&eav_vars, &[e_val.raw, a_val, absent.raw])),
        "triple absent: false"
    );
}

// ── Property tests: union = setwise union of arms; monotonicity ────────

/// Query `or!(and!(s1.has(x), s2.has(x)), and!(s3.has(x), s4.has(x)))`
/// as a set of raw values.
fn union_of_intersections(
    s1: &HashSet<String>,
    s2: &HashSet<String>,
    s3: &HashSet<String>,
    s4: &HashSet<String>,
) -> HashSet<[u8; 32]> {
    find!(
        x: Inline<ShortString>,
        or!(and!(s1.has(x), s2.has(x)), and!(s3.has(x), s4.has(x)))
    )
    .solve_dag_lazy()
    .map(|x: Inline<ShortString>| x.raw)
    .collect()
}

fn setwise_oracle(
    s1: &HashSet<String>,
    s2: &HashSet<String>,
    s3: &HashSet<String>,
    s4: &HashSet<String>,
) -> HashSet<[u8; 32]> {
    let arm1 = s1.intersection(s2);
    let arm2 = s3.intersection(s4);
    arm1.chain(arm2)
        .map(|s| {
            let v: Inline<ShortString> = s.as_str().to_inline();
            v.raw
        })
        .collect()
}

fn small_set() -> impl Strategy<Value = HashSet<String>> {
    proptest::collection::hash_set("[a-e]", 0..5)
}

proptest! {
    /// or!(A, B) == dedup(results(A) ∪ results(B)) with composite
    /// (and!) arms, for both arm orderings.
    #[test]
    fn union_equals_setwise_union_of_arms(
        s1 in small_set(),
        s2 in small_set(),
        s3 in small_set(),
        s4 in small_set(),
    ) {
        let expected = setwise_oracle(&s1, &s2, &s3, &s4);
        let forward = union_of_intersections(&s1, &s2, &s3, &s4);
        let backward = union_of_intersections(&s3, &s4, &s1, &s2);

        prop_assert_eq!(&forward, &expected,
            "or!(A, B) must equal results(A) ∪ results(B)");
        prop_assert_eq!(&backward, &expected,
            "or!(B, A) must equal or!(A, B)");
    }

    /// Monotonicity: growing every arm's backing data can only grow the
    /// union's result — Q(A) ⊆ Q(A ∪ B).
    #[test]
    fn union_query_is_monotone_under_growth(
        s1 in small_set(),
        s2 in small_set(),
        s3 in small_set(),
        s4 in small_set(),
        e1 in small_set(),
        e2 in small_set(),
        e3 in small_set(),
        e4 in small_set(),
    ) {
        let small = union_of_intersections(&s1, &s2, &s3, &s4);

        let g1: HashSet<String> = s1.union(&e1).cloned().collect();
        let g2: HashSet<String> = s2.union(&e2).cloned().collect();
        let g3: HashSet<String> = s3.union(&e3).cloned().collect();
        let g4: HashSet<String> = s4.union(&e4).cloned().collect();
        let big = union_of_intersections(&g1, &g2, &g3, &g4);

        prop_assert!(small.is_subset(&big),
            "adding data must never remove union results");
    }
}
