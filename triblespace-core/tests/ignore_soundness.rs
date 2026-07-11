//! REGRESSION (constant folding × `IgnoreConstraint`): `ignore!` hides
//! variables from the outer query, so a wrapped constraint can reach
//! "all visible variables bound" — with constant folding even at
//! construction time (an EMPTY visible set, e.g.
//! `ignore!((h), pattern!(kb, [{ ?h @ attr: lit }]))`) — while the hidden
//! variables are still free. The exact-when-fully-bound `satisfied()` law
//! then demands the *existential* over the hidden variables. Before the
//! override, `IgnoreConstraint` inherited the optimistic-`true` default:
//! a fully-hidden existence check settled as vacuously true (one row on
//! an empty kb), an exists-filter inside `and!` was silently dropped, and
//! a dead `or!` arm containing an `ignore!` kept proposing — emitting
//! rows satisfied by NO arm (the exact leak shape the fully-bound-exact
//! fix closed for the other constraint leaves).
//!
//! Attribute ids reuse the canonical test-world attributes minted for
//! `tests/solve_blocked.rs`.

use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::prelude::*;

mod world {
    use triblespace_core::prelude::*;

    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as r: inlineencodings::GenId;
    }
}

/// A fully-hidden pattern (`?h` ignored, attribute and value constant)
/// has an EMPTY visible variable set, so `Query::new` settles it with one
/// `satisfied()` probe. That probe must be the existential over `h`: one
/// row when a witness exists, none when it doesn't — on both engines.
#[test]
fn fully_hidden_existence_check_present_vs_absent() {
    let human = ufoid();
    let robot = ufoid();
    let e = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &e @ world::kind: &human };
    let human = *human;
    let robot = *robot;

    let present: Vec<()> =
        find!((), ignore!((h), pattern!(&kb, [{ ?h @ world::kind: human }]))).collect();
    assert_eq!(
        present.len(),
        1,
        "a witness (an entity of kind=human) exists — exactly one row"
    );
    assert_eq!(
        find!((), ignore!((h), pattern!(&kb, [{ ?h @ world::kind: human }])))
            .solve_dag()
            .len(),
        1,
        "DAG engine diverged on the present fully-hidden existence check"
    );

    let absent: Vec<()> =
        find!((), ignore!((h), pattern!(&kb, [{ ?h @ world::kind: robot }]))).collect();
    assert!(
        absent.is_empty(),
        "no entity has kind=robot — the hidden existential is false, zero rows"
    );
    assert!(
        find!((), ignore!((h), pattern!(&kb, [{ ?h @ world::kind: robot }])))
            .solve_dag()
            .is_empty(),
        "DAG engine diverged on the absent fully-hidden existence check"
    );
}

/// An `ignore!`-wrapped constant-position pattern used as an exists-filter
/// inside `and!`: it has no visible variables, so the search never
/// consults it via propose/confirm — the settlement probe is the ONLY
/// gate. A false existential must empty the whole conjunction; a true one
/// must not filter anything.
#[test]
fn fully_hidden_exists_filter_gates_the_conjunction() {
    let human = ufoid();
    let robot = ufoid();
    let people: Vec<_> = (0..5).map(|_| ufoid()).collect();
    let mut kb = TribleSet::new();
    for person in &people {
        kb += entity! { person @ world::kind: &human };
    }
    let human = *human;
    let robot = *robot;

    let with_witness: Vec<Inline<GenId>> = find!(
        (x: Inline<_>),
        and!(
            pattern!(&kb, [{ ?x @ world::kind: human }]),
            ignore!((h), pattern!(&kb, [{ ?h @ world::kind: human }]))
        )
    )
    .map(|(x,)| x)
    .collect();
    assert_eq!(
        with_witness.len(),
        5,
        "a satisfied exists-filter must not drop rows"
    );

    let without_witness: Vec<Inline<GenId>> = find!(
        (x: Inline<_>),
        and!(
            pattern!(&kb, [{ ?x @ world::kind: human }]),
            ignore!((h), pattern!(&kb, [{ ?h @ world::kind: robot }]))
        )
    )
    .map(|(x,)| x)
    .collect();
    assert!(
        without_witness.is_empty(),
        "an exists-filter with no witness must empty the conjunction, not be silently dropped"
    );
}

/// Fixture for the `or!`-arm leak: arm A is
/// `and!(ignore!((h), ?x p ?h), ?x q ?z)`, arm B is `?x r ?z`.
///
/// - `x1` has `p → h1` and `q → z1`: arm A yields `(x1, z1)`.
/// - `x2` has `q → z2` and `r → z3` but NO `p`-edge: arm A is
///   semantically dead for `x2` (the hidden existential over `h` is
///   false), arm B yields `(x2, z3)`.
///
/// The leak: with `x = x2` bound, the union gates arm A through
/// `satisfied()`; an optimistic answer keeps the dead arm proposing `z`
/// candidates from its q-pattern, emitting `(x2, z2)` — a row satisfied
/// by NO arm.
fn build_union_world() -> (TribleSet, Id, Id, Id, Id, Id) {
    let x1 = ufoid();
    let x2 = ufoid();
    let h1 = ufoid();
    let z1 = ufoid();
    let z2 = ufoid();
    let z3 = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &x1 @ world::p: &h1, world::q: &z1 };
    kb += entity! { &x2 @ world::q: &z2, world::r: &z3 };
    (kb, *x1, *x2, *z1, *z2, *z3)
}

// The whole `find!` lives inside the macro so the `?x`/`?z`/`?h` tokens
// share one hygiene context with the variable declarations (the
// `star_query!` pattern from `tests/engine_seam.rs`).
macro_rules! union_query {
    ($kb:expr) => {
        find!(
            (x: Inline<_>, z: Inline<_>),
            or!(
                and!(
                    ignore!((h), pattern!(&$kb, [{ ?x @ world::p: ?h }])),
                    pattern!(&$kb, [{ ?x @ world::q: ?z }])
                ),
                pattern!(&$kb, [{ ?x @ world::r: ?z }])
            )
        )
    };
    ($kb:expr, $pin:expr) => {
        find!(
            (x: Inline<_>, z: Inline<_>),
            and!(
                x.is($pin),
                or!(
                    and!(
                        ignore!((h), pattern!(&$kb, [{ ?x @ world::p: ?h }])),
                        pattern!(&$kb, [{ ?x @ world::q: ?z }])
                    ),
                    pattern!(&$kb, [{ ?x @ world::r: ?z }])
                )
            )
        )
    };
}

/// Deterministic order (x pinned, so it binds first): with `x = x2` the
/// arm-A existential over `h` is false, so only arm B's `(x2, z3)` may
/// appear — the dead arm must not leak `(x2, z2)`.
#[test]
fn dead_union_arm_with_ignore_does_not_leak() {
    let (kb, _x1, x2, _z1, _z2, z3) = build_union_world();

    let rows: Vec<(Inline<GenId>, Inline<GenId>)> =
        union_query!(kb, x2.to_inline()).collect();
    assert_eq!(
        rows,
        vec![(x2.to_inline(), z3.to_inline())],
        "x2 has no p-edge: arm A is dead; its q-target z2 must not leak through"
    );

    let dag_rows: Vec<(Inline<GenId>, Inline<GenId>)> =
        union_query!(kb, x2.to_inline()).solve_dag();
    assert_eq!(
        dag_rows,
        vec![(x2.to_inline(), z3.to_inline())],
        "DAG engine diverged on the dead-arm leak fixture"
    );
}

/// The full (unpinned) multiset over both arms: exactly arm-A's
/// `(x1, z1)` and arm-B's `(x2, z3)`, whatever variable order the engine
/// picks — and identical across engines.
#[test]
fn union_with_ignore_full_multiset() {
    let (kb, x1, x2, z1, _z2, z3) = build_union_world();

    let mut expected = vec![
        (x1.to_inline(), z1.to_inline()),
        (x2.to_inline(), z3.to_inline()),
    ];
    expected.sort();

    let mut rows: Vec<(Inline<GenId>, Inline<GenId>)> = union_query!(kb).collect();
    rows.sort();
    assert_eq!(rows, expected, "union-with-ignore multiset is wrong");

    let mut dag_rows: Vec<(Inline<GenId>, Inline<GenId>)> = union_query!(kb).solve_dag();
    dag_rows.sort();
    assert_eq!(
        dag_rows, expected,
        "DAG engine diverged on the union-with-ignore multiset"
    );
}

/// Non-regression: the ordinary use of `ignore!` — hiding a variable that
/// still participates in joins through propose/confirm — keeps working,
/// including when the hidden variable's existential is checked per-row
/// inside a union.
#[test]
fn ignore_still_hides_while_joining() {
    let human = ufoid();
    let target = ufoid();
    let a = ufoid();
    let b = ufoid();
    let mut kb = TribleSet::new();
    kb += entity! { &a @ world::kind: &human, world::p: &target };
    kb += entity! { &b @ world::kind: &human };
    let human = *human;
    let a = *a;

    // Only `a` has a p-edge, so the hidden join keeps `b` out.
    let rows: Vec<Inline<GenId>> = find!(
        (x: Inline<_>),
        and!(
            pattern!(&kb, [{ ?x @ world::kind: human }]),
            ignore!((h), pattern!(&kb, [{ ?x @ world::p: ?h }]))
        )
    )
    .map(|(x,)| x)
    .collect();
    assert_eq!(rows, vec![a.to_inline()]);
}
