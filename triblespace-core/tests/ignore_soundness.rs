//! Contract tests for `ignore!`'s historical anonymous-wildcard semantics.
//!
//! Ignored variables are removed from the outer plan rather than solved as
//! existential witnesses. A clause with no surviving variables is therefore
//! inert, and repeating an ignored name across clauses does not create a
//! hidden join. Clauses which also mention a visible variable still constrain
//! that variable through the ordinary propose/confirm protocol.

use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::iu256::U256BE;
use triblespace_core::prelude::*;
use triblespace_core::query::residual::ResidualLowering;

use std::collections::HashSet;

mod world {
    use triblespace_core::prelude::*;

    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as r: inlineencodings::GenId;
    }
}

fn sorted<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values
}

fn insert_edge(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

/// A fully hidden constraint has no scheduling edge into the outer query.
/// Whether its inner pattern has a match or not, it is omitted and the
/// zero-variable query produces its single unit row.
#[test]
fn fully_hidden_present_and_absent_are_both_omitted() {
    let human = ufoid();
    let robot = ufoid();
    let person = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &person @ world::kind: &human };
    let human = *human;
    let robot = *robot;

    macro_rules! present {
        () => {
            find!(
                (),
                ignore!((h), pattern!(&kb, [{ ?h @ world::kind: human }]))
            )
        };
    }
    macro_rules! absent {
        () => {
            find!(
                (),
                ignore!((h), pattern!(&kb, [{ ?h @ world::kind: robot }]))
            )
        };
    }

    assert_eq!(present!().collect::<Vec<_>>(), vec![()]);
    assert_eq!(present!().solve_dag(), vec![()]);
    assert_eq!(present!().solve_residual_state(), vec![()]);
    assert_eq!(
        present!()
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect::<Vec<_>>(),
        vec![()]
    );
    assert_eq!(absent!().collect::<Vec<_>>(), vec![()]);
    assert_eq!(absent!().solve_dag(), vec![()]);
    assert_eq!(absent!().solve_residual_state(), vec![()]);
    assert_eq!(
        absent!()
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect::<Vec<_>>(),
        vec![()]
    );
}

/// A hidden-only false clause cannot act as an exists-filter. Adding a
/// witness later consequently cannot remove or duplicate any outer rows.
#[test]
fn hidden_only_clause_cannot_filter_a_conjunction() {
    let human = ufoid();
    let robot = ufoid();
    let people: Vec<_> = (0..5).map(|_| ufoid()).collect();
    let mut before = TribleSet::new();
    for person in &people {
        before += entity! { person @ world::kind: &human };
    }
    let mut after = before.clone();
    let robot_entity = ufoid();
    after += entity! { &robot_entity @ world::kind: &robot };
    let human = *human;
    let robot = *robot;

    macro_rules! query {
        ($store:expr) => {
            find!(
                x: Inline<GenId>,
                and!(
                    pattern!($store, [{ ?x @ world::kind: human }]),
                    ignore!((h), pattern!($store, [{ ?h @ world::kind: robot }]))
                )
            )
        };
    }

    let expected = sorted(people.iter().map(|id| id.to_inline()).collect());
    let before_seq = sorted(query!(&before).collect());
    let before_dag = sorted(query!(&before).solve_dag());
    let before_residual = sorted(query!(&before).solve_residual_state());
    let before_full = sorted(
        query!(&before)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect(),
    );
    let after_seq = sorted(query!(&after).collect());
    let after_dag = sorted(query!(&after).solve_dag());
    let after_residual = sorted(query!(&after).solve_residual_state());
    let after_full = sorted(
        query!(&after)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect(),
    );
    assert_eq!(before_seq, expected);
    assert_eq!(before_dag, expected);
    assert_eq!(before_residual, expected);
    assert_eq!(before_full, expected);
    assert_eq!(after_seq, expected);
    assert_eq!(after_dag, expected);
    assert_eq!(after_residual, expected);
    assert_eq!(after_full, expected);
}

/// Reusing the spelling `h` does not turn an ignored wildcard into a hidden
/// join key. The `q(h, target)` clause is hidden-only and inert; the visible
/// `p(x, h)` clause still requires each returned `x` to have some `p` edge.
#[test]
fn repeated_ignored_name_does_not_join_across_clauses() {
    let human = ufoid();
    let target = ufoid();
    let wrong_target = ufoid();
    let matching = ufoid();
    let mismatching = ufoid();
    let no_p = ufoid();
    let matching_h = ufoid();
    let mismatching_h = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &matching @ world::kind: &human, world::p: &matching_h };
    kb += entity! { &matching_h @ world::q: &target };
    kb += entity! { &mismatching @ world::kind: &human, world::p: &mismatching_h };
    kb += entity! { &mismatching_h @ world::q: &wrong_target };
    kb += entity! { &no_p @ world::kind: &human };

    macro_rules! query {
        () => {
            find!(
                x: Inline<GenId>,
                and!(
                    pattern!(&kb, [{ ?x @ world::kind: (&human) }]),
                    ignore!(
                        (h),
                        and!(
                            pattern!(&kb, [{ ?x @ world::p: ?h }]),
                            pattern!(&kb, [{ ?h @ world::q: (&target) }])
                        )
                    )
                )
            )
        };
    }

    let expected = sorted(vec![matching.to_inline(), mismatching.to_inline()]);
    assert_eq!(sorted(query!().collect()), expected);
    assert_eq!(sorted(query!().solve_dag()), expected);
    assert_eq!(sorted(query!().solve_residual_state()), expected);
    assert_eq!(
        sorted(
            query!()
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .collect()
        ),
        expected
    );
}

/// A wildcard-scoped conjunction must prove every already-bound visible
/// component before it can contribute another variable through a union arm.
/// Otherwise a dead `p(x, _)` half can borrow `x` from the fallback arm and
/// leak the unrelated `q(y, target)` half into a hybrid row.
#[test]
fn partially_bound_ignored_union_arm_cannot_leak_a_hybrid_row() {
    let good_x = ufoid();
    let pinned_x = ufoid();
    let hidden = ufoid();
    let target = ufoid();
    let primary_y: Vec<_> = (0..3).map(|_| ufoid()).collect();
    let fallback_y = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &good_x @ world::p: &hidden };
    for y in &primary_y {
        kb += entity! { y @ world::q: &target };
    }
    let pinned_x = *pinned_x;
    let target = *target;
    let fallback_y = *fallback_y;

    let query = || {
        find!(
            (x: Inline<GenId>, y: Inline<GenId>),
            and!(
                x.is(pinned_x.to_inline()),
                or!(
                    ignore!(
                        (h),
                        and!(
                            pattern!(&kb, [{ ?x @ world::p: ?h }]),
                            pattern!(&kb, [{ ?y @ world::q: &target }])
                        )
                    ),
                    and!(
                        x.is(pinned_x.to_inline()),
                        y.is(fallback_y.to_inline())
                    )
                )
            )
        )
    };

    let expected = vec![(pinned_x.to_inline(), fallback_y.to_inline())];
    assert_eq!(query().sequential().collect::<Vec<_>>(), expected);
    assert_eq!(query().collect::<Vec<_>>(), expected);
    assert_eq!(query().solve_dag(), expected);
    assert_eq!(query().solve_residual_state(), expected);
    assert_eq!(
        query()
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .collect::<Vec<_>>(),
        expected
    );
}

/// A semantic OR below the wildcard scope remains guarded by that OR, while
/// Support for the enclosing ignored conjunction remains one atomic replay.
/// The dead outer arm must not borrow a pinned `x`; either live inner arm may
/// independently activate the primary arm without exposing its hidden value.
#[test]
fn nested_union_inside_ignore_keeps_scope_support_atomic() {
    let p_x = ufoid();
    let r_x = ufoid();
    let dead_x = ufoid();
    let p_hidden = ufoid();
    let r_hidden = ufoid();
    let target = ufoid();
    let primary_y: Vec<_> = (0..2).map(|_| ufoid()).collect();
    let fallback_y = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &p_x @ world::p: &p_hidden };
    kb += entity! { &r_x @ world::r: &r_hidden };
    for y in &primary_y {
        kb += entity! { y @ world::q: &target };
    }
    let p_x = *p_x;
    let r_x = *r_x;
    let dead_x = *dead_x;
    let target = *target;
    let fallback_y = *fallback_y;

    macro_rules! query {
        ($pin:expr) => {
            find!(
                (x: Inline<GenId>, y: Inline<GenId>),
                and!(
                    x.is($pin),
                    or!(
                        ignore!(
                            (h),
                            and!(
                                or!(
                                    pattern!(&kb, [{ ?x @ world::p: ?h }]),
                                    pattern!(&kb, [{ ?x @ world::r: ?h }])
                                ),
                                pattern!(&kb, [{ ?y @ world::q: &target }])
                            )
                        ),
                        and!(
                            x.is(dead_x.to_inline()),
                            y.is(fallback_y.to_inline())
                        )
                    )
                )
            )
        };
    }

    let assert_all = |pin: Inline<GenId>, expected: Vec<(Inline<GenId>, Inline<GenId>)>| {
        assert_eq!(sorted(query!(pin).sequential().collect()), expected);
        assert_eq!(sorted(query!(pin).collect()), expected);
        assert_eq!(sorted(query!(pin).solve_dag()), expected);
        assert_eq!(sorted(query!(pin).solve_residual_state()), expected);
        assert_eq!(
            sorted(
                query!(pin)
                    .solve_residual_state_lazy_with(ResidualLowering::FULL)
                    .collect()
            ),
            expected
        );
    };

    assert_all(
        dead_x.to_inline(),
        vec![(dead_x.to_inline(), fallback_y.to_inline())],
    );
    for live_x in [p_x, r_x] {
        assert_all(
            live_x.to_inline(),
            sorted(
                primary_y
                    .iter()
                    .map(|y| (live_x.to_inline(), y.to_inline()))
                    .collect(),
            ),
        );
    }
}

/// An atomic pattern with a surviving `x` is not inert merely because its
/// object is ignored. Adding a new `p` edge grows the result monotonically.
#[test]
fn atomic_wildcard_still_constrains_visible_variable() {
    let human = ufoid();
    let a = ufoid();
    let b = ufoid();
    let h1 = ufoid();
    let h2 = ufoid();
    let mut before = TribleSet::new();
    before += entity! { &a @ world::kind: &human, world::p: &h1 };
    before += entity! { &b @ world::kind: &human };
    let mut after = before.clone();
    after += entity! { &b @ world::p: &h2 };
    let human = *human;
    let a = *a;
    let b = *b;

    macro_rules! query {
        ($store:expr) => {
            find!(
                x: Inline<GenId>,
                and!(
                    pattern!($store, [{ ?x @ world::kind: human }]),
                    ignore!((h), pattern!($store, [{ ?x @ world::p: ?h }]))
                )
            )
        };
    }

    let before_expected = vec![a.to_inline()];
    let after_expected = sorted(vec![a.to_inline(), b.to_inline()]);
    assert_eq!(query!(&before).collect::<Vec<_>>(), before_expected);
    assert_eq!(query!(&before).solve_dag(), before_expected);
    assert_eq!(sorted(query!(&after).collect()), after_expected);
    assert_eq!(sorted(query!(&after).solve_dag()), after_expected);
}

fn build_union_world() -> (TribleSet, Id, Id, Id, Id, Id, Id) {
    let x1 = ufoid();
    let x2 = ufoid();
    let h1 = ufoid();
    let h2 = ufoid();
    let z1 = ufoid();
    let z2 = ufoid();
    let z3 = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &x1 @ world::p: &h1, world::q: &z1 };
    kb += entity! { &x2 @ world::q: &z2, world::r: &z3 };
    (kb, *x1, *x2, *h2, *z1, *z2, *z3)
}

macro_rules! union_query {
    ($store:expr) => {
        find!(
            (x: Inline<GenId>, z: Inline<GenId>),
            or!(
                and!(
                    ignore!((h), pattern!($store, [{ ?x @ world::p: ?h }])),
                    pattern!($store, [{ ?x @ world::q: ?z }])
                ),
                pattern!($store, [{ ?x @ world::r: ?z }])
            )
        )
    };
    ($store:expr, $pin:expr) => {
        find!(
            (x: Inline<GenId>, z: Inline<GenId>),
            and!(
                x.is($pin),
                or!(
                    and!(
                        ignore!((h), pattern!($store, [{ ?x @ world::p: ?h }])),
                        pattern!($store, [{ ?x @ world::q: ?z }])
                    ),
                    pattern!($store, [{ ?x @ world::r: ?z }])
                )
            )
        )
    };
}

/// A union must not replay a visible arm whose wildcard pattern has already
/// rejected the bound `x`. Once a matching `p(x, _)` fact is added, that arm
/// becomes live and contributes its row without removing the other arm.
#[test]
fn dead_visible_union_arm_only_replays_after_monotonic_growth() {
    let (before, x1, x2, h2, z1, z2, z3) = build_union_world();
    let before_expected = sorted(vec![
        (x1.to_inline(), z1.to_inline()),
        (x2.to_inline(), z3.to_inline()),
    ]);

    assert_eq!(
        union_query!(&before, x2.to_inline()).collect::<Vec<_>>(),
        vec![(x2.to_inline(), z3.to_inline())]
    );
    assert_eq!(
        union_query!(&before, x2.to_inline()).solve_dag(),
        vec![(x2.to_inline(), z3.to_inline())]
    );
    assert_eq!(sorted(union_query!(&before).collect()), before_expected);
    assert_eq!(sorted(union_query!(&before).solve_dag()), before_expected);

    let mut after = before.clone();
    insert_edge(&mut after, &x2, &world::p, &h2);
    let after_expected = sorted(vec![
        (x1.to_inline(), z1.to_inline()),
        (x2.to_inline(), z2.to_inline()),
        (x2.to_inline(), z3.to_inline()),
    ]);
    assert_eq!(sorted(union_query!(&after).collect()), after_expected);
    assert_eq!(sorted(union_query!(&after).solve_dag()), after_expected);
}

/// Wildcard replay uses the historical confirmation path, not proposal
/// enumeration. This matters for filters such as `value_range`, which
/// intentionally never propose: the primary arm must stay live for an
/// in-range bound value and die for an out-of-range one.
#[test]
fn union_replay_respects_confirm_only_range() {
    let below = U256BE::inline_from(5u64);
    let inside = U256BE::inline_from(15u64);
    let above = U256BE::inline_from(25u64);
    let hidden = U256BE::inline_from(99u64);
    let min = U256BE::inline_from(10u64);
    let max = U256BE::inline_from(20u64);
    let primary_z = U256BE::inline_from(1u64);
    let fallback_z = U256BE::inline_from(2u64);

    let domain = HashSet::from([below, inside, above]);
    let primary = HashSet::from([primary_z]);
    let fallback = HashSet::from([fallback_z]);

    macro_rules! query {
        ($pin:expr) => {
            find!(
                (x: Inline<U256BE>, z: Inline<U256BE>),
                and!(
                    x.is($pin),
                    or!(
                        and!(
                            ignore!(
                                (h),
                                and!(value_range(x, min, max), h.is(hidden))
                            ),
                            primary.has(z)
                        ),
                        and!(domain.has(x), fallback.has(z))
                    )
                )
            )
        };
    }

    let inside_expected = sorted(vec![(inside, primary_z), (inside, fallback_z)]);
    assert_eq!(sorted(query!(inside).collect()), inside_expected);
    assert_eq!(sorted(query!(inside).solve_dag()), inside_expected);

    for outside in [below, above] {
        let expected = vec![(outside, fallback_z)];
        assert_eq!(query!(outside).collect::<Vec<_>>(), expected);
        assert_eq!(query!(outside).solve_dag(), expected);
    }
}
