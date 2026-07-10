//! `or!` over `pattern!` arms — the constant-folding fix.
//!
//! `UnionConstraint::new` requires every variant to declare the same
//! variable set. The macro layer used to allocate a FRESH hidden variable
//! (plus a `ConstantConstraint`) for every attribute constant and literal
//! value, so two separate `pattern!` invocations — even structurally
//! identical ones — never declared equal sets and the documented
//! `or!(pattern!(..), pattern!(..))` form deterministically panicked.
//!
//! The fix folds attribute constants, literal values, and constant entity
//! ids into the pattern constraint as constant `Term`s: they behave like
//! positions the engine has already bound but never appear in the
//! variable set, so union arms compare only the query variables the
//! caller actually wrote. A side effect is that a fully-constant pattern
//! has an EMPTY variable set; the engine settles those with one exact
//! `satisfied()` check at query start (they are "fully bound" with zero
//! variables), which the `fully_constant_*` tests below pin down.

use proptest::prelude::*;
use std::collections::HashSet;
use triblespace_core::id::rngid;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::prelude::*;
use triblespace_core::query::{Constraint, VariableContext, VariableSet};

mod profile {
    use triblespace_core::prelude::*;
    attributes! {
        "CC00000000000000DD00000000000001" as pub nickname: inlineencodings::ShortString;
        "CC00000000000000DD00000000000002" as pub display_name: inlineencodings::ShortString;
        "CC00000000000000DD00000000000003" as pub city: inlineencodings::ShortString;
    }
}

/// The book's own `or!` example (query-language.md, "Alternatives"):
/// nickname vs display_name arms use DIFFERENT attributes. This is the
/// exact shape that used to trip the union's variable-set assertion.
#[test]
fn book_nickname_display_name_example() {
    let alice = rngid();
    let bob = rngid();
    let carol = rngid();

    let mut dataset = TribleSet::new();
    dataset += entity! { &alice @ profile::nickname: "Ali" };
    dataset += entity! { &bob @
        profile::nickname: "Bobby",
        profile::display_name: "Robert",
    };
    dataset += entity! { &carol @ profile::city: "Caladan" };

    let mut aliases: Vec<String> = find!(
        (alias: Inline<_>),
        temp!(
            (entity),
            or!(
                pattern!(&dataset, [{ ?entity @ profile::nickname: ?alias }]),
                pattern!(&dataset, [{ ?entity @ profile::display_name: ?alias }])
            )
        )
    )
    .map(|(alias,)| alias.try_from_inline::<&str>().unwrap().to_string())
    .collect();
    aliases.sort_unstable();

    // Bob has both attributes and yields two rows; Carol has neither
    // and yields none.
    assert_eq!(aliases, ["Ali", "Bobby", "Robert"]);
}

/// Arms with the same attribute but different literal values.
#[test]
fn or_arms_with_different_literals() {
    let alice = rngid();
    let bob = rngid();
    let carol = rngid();

    let mut dataset = TribleSet::new();
    dataset += entity! { &alice @ profile::nickname: "Ali" };
    dataset += entity! { &bob @ profile::nickname: "Bobby" };
    dataset += entity! { &carol @ profile::nickname: "Caro" };

    let people: HashSet<[u8; 32]> = find!(
        (person: Inline<GenId>),
        or!(
            pattern!(&dataset, [{ ?person @ profile::nickname: "Ali" }]),
            pattern!(&dataset, [{ ?person @ profile::nickname: "Bobby" }])
        )
    )
    .map(|(person,)| person.raw)
    .collect();

    let alice_val: Inline<GenId> = (&alice).to_inline();
    let bob_val: Inline<GenId> = (&bob).to_inline();
    let expected: HashSet<[u8; 32]> = [alice_val.raw, bob_val.raw].into_iter().collect();
    assert_eq!(people, expected);
}

/// Arms over different datasets (each `pattern!` invocation is separate,
/// so this also used to panic even with identical attributes).
#[test]
fn or_arms_over_different_sets() {
    let alice = rngid();
    let bob = rngid();

    let mut set_a = TribleSet::new();
    set_a += entity! { &alice @ profile::nickname: "Ali" };
    let mut set_b = TribleSet::new();
    set_b += entity! { &bob @ profile::nickname: "Bobby" };

    let mut names: Vec<String> = find!(
        (person: Inline<GenId>, name: Inline<_>),
        or!(
            pattern!(&set_a, [{ ?person @ profile::nickname: ?name }]),
            pattern!(&set_b, [{ ?person @ profile::nickname: ?name }])
        )
    )
    .map(|(_, name)| name.try_from_inline::<&str>().unwrap().to_string())
    .collect();
    names.sort_unstable();

    assert_eq!(names, ["Ali", "Bobby"]);
}

/// Mixed leaf/composite arms: a single pattern vs an `and!` of two
/// patterns. Both arms mention exactly `{person}`.
#[test]
fn or_arms_mixed_leaf_and_composite() {
    let ali_only = rngid();
    let full_profile = rngid();
    let display_only = rngid();

    let mut dataset = TribleSet::new();
    dataset += entity! { &ali_only @ profile::nickname: "Ali" };
    dataset += entity! { &full_profile @
        profile::nickname: "Bobby",
        profile::display_name: "Robert",
    };
    dataset += entity! { &display_only @ profile::display_name: "Carola" };

    let query = |flipped: bool| -> HashSet<[u8; 32]> {
        let leaf_first = find!(
            (person: Inline<GenId>),
            or!(
                pattern!(&dataset, [{ ?person @ profile::nickname: "Ali" }]),
                and!(
                    pattern!(&dataset, [{ ?person @ profile::nickname: "Bobby" }]),
                    pattern!(&dataset, [{ ?person @ profile::display_name: "Robert" }])
                )
            )
        );
        let composite_first = find!(
            (person: Inline<GenId>),
            or!(
                and!(
                    pattern!(&dataset, [{ ?person @ profile::nickname: "Bobby" }]),
                    pattern!(&dataset, [{ ?person @ profile::display_name: "Robert" }])
                ),
                pattern!(&dataset, [{ ?person @ profile::nickname: "Ali" }])
            )
        );
        if flipped {
            composite_first.map(|(p,)| p.raw).collect()
        } else {
            leaf_first.map(|(p,)| p.raw).collect()
        }
    };

    let ali_val: Inline<GenId> = (&ali_only).to_inline();
    let full_val: Inline<GenId> = (&full_profile).to_inline();
    let expected: HashSet<[u8; 32]> = [ali_val.raw, full_val.raw].into_iter().collect();

    assert_eq!(query(false), expected, "or!(leaf, composite)");
    assert_eq!(
        query(true),
        expected,
        "or!(composite, leaf) — arm order must not matter"
    );
}

/// The lowering emits NO variables for attribute constants, literal
/// values, or constant entity ids: after expanding a pattern that uses
/// all three kinds of constants alongside one query variable, the
/// context has allocated exactly that one variable and the constraint's
/// visible set contains nothing else.
#[test]
fn pattern_constants_allocate_no_helper_variables() {
    let alice = rngid();
    let mut dataset = TribleSet::new();
    dataset += entity! { &alice @
        profile::nickname: "Ali",
        profile::city: "Caladan",
    };

    let mut ctx = VariableContext::new();
    macro_rules! __local_find_context {
        () => {
            &mut ctx
        };
    }
    let name = ctx.next_variable::<triblespace_core::prelude::inlineencodings::ShortString>();
    let constraint = pattern!(&dataset, [{ &alice @
        profile::nickname: ?name,
        profile::city: "Caladan",
    }]);

    assert_eq!(
        constraint.variables(),
        VariableSet::new_singleton(name.index),
        "constants must not appear in the constraint's variable set"
    );
    assert_eq!(
        ctx.next_index, 1,
        "no hidden variables may be allocated for constants \
         (entity id, attribute constants, literal value)"
    );
}

/// A fully-constant pattern has an empty variable set; its truth is
/// settled by the engine's satisfied() check at query start.
#[test]
fn fully_constant_pattern_is_an_existence_check() {
    let alice = rngid();
    let mut dataset = TribleSet::new();
    dataset += entity! { &alice @ profile::nickname: "Ali" };

    assert!(exists!(
        pattern!(&dataset, [{ &alice @ profile::nickname: "Ali" }])
    ));
    assert!(!exists!(
        pattern!(&dataset, [{ &alice @ profile::nickname: "Bobby" }])
    ));
    assert!(!exists!(
        pattern!(&dataset, [{ &alice @ profile::display_name: "Ali" }])
    ));
}

/// A dead fully-constant pattern inside `and!` kills the whole
/// conjunction even though the search never proposes for it.
#[test]
fn fully_constant_pattern_composes_with_and() {
    let alice = rngid();
    let bob = rngid();
    let mut dataset = TribleSet::new();
    dataset += entity! { &alice @ profile::nickname: "Ali" };
    dataset += entity! { &bob @ profile::nickname: "Bobby" };

    let live: Vec<_> = find!(
        (person: Inline<GenId>),
        and!(
            pattern!(&dataset, [{ ?person @ profile::nickname: "Bobby" }]),
            pattern!(&dataset, [{ &alice @ profile::nickname: "Ali" }])
        )
    )
    .collect();
    assert_eq!(live.len(), 1, "a satisfied constant pattern is a tautology");

    let dead: Vec<_> = find!(
        (person: Inline<GenId>),
        and!(
            pattern!(&dataset, [{ ?person @ profile::nickname: "Bobby" }]),
            pattern!(&dataset, [{ &alice @ profile::nickname: "NotAli" }])
        )
    )
    .collect();
    assert!(
        dead.is_empty(),
        "a failed constant pattern must empty the conjunction"
    );
}

/// Genuinely different query variables across arms still panic — the
/// union's variable-set requirement is about visible variables, and
/// that contract stays.
#[test]
#[should_panic(expected = "must mention the same query variables")]
fn or_panics_when_arms_mention_different_variables() {
    let dataset = TribleSet::new();

    let _ = find!(
        (alias: Inline<_>),
        temp!(
            (x, y),
            or!(
                pattern!(&dataset, [{ ?x @ profile::nickname: ?alias }]),
                pattern!(&dataset, [{ ?y @ profile::display_name: ?alias }])
            )
        )
    )
    .count();
}

proptest! {
    /// Oracle check: `or!` over two attributes equals the set-union of
    /// the two single-attribute queries, on random data, in both arm
    /// orders (mirrors the style of union_soundness.rs).
    #[test]
    fn or_equals_union_oracle(
        assignments in proptest::collection::vec(
            (0usize..6, prop_oneof![Just(0u8), Just(1u8)], 0usize..4),
            0..24,
        )
    ) {
        let entities: Vec<_> = (0..6).map(|_| rngid()).collect();
        let values = ["v0", "v1", "v2", "v3"];

        let mut dataset = TribleSet::new();
        for (e, which, v) in &assignments {
            let entity = &entities[*e];
            dataset += match which {
                0 => entity! { entity @ profile::nickname: values[*v] },
                _ => entity! { entity @ profile::display_name: values[*v] },
            };
        }

        let nick_rows: HashSet<([u8; 32], [u8; 32])> = find!(
            (person: Inline<GenId>, alias: Inline<_>),
            pattern!(&dataset, [{ ?person @ profile::nickname: ?alias }])
        )
        .map(|(p, a)| (p.raw, a.raw))
        .collect();
        let disp_rows: HashSet<([u8; 32], [u8; 32])> = find!(
            (person: Inline<GenId>, alias: Inline<_>),
            pattern!(&dataset, [{ ?person @ profile::display_name: ?alias }])
        )
        .map(|(p, a)| (p.raw, a.raw))
        .collect();
        let oracle: HashSet<_> = nick_rows.union(&disp_rows).copied().collect();

        let forward: HashSet<([u8; 32], [u8; 32])> = find!(
            (person: Inline<GenId>, alias: Inline<_>),
            or!(
                pattern!(&dataset, [{ ?person @ profile::nickname: ?alias }]),
                pattern!(&dataset, [{ ?person @ profile::display_name: ?alias }])
            )
        )
        .map(|(p, a)| (p.raw, a.raw))
        .collect();
        let backward: HashSet<([u8; 32], [u8; 32])> = find!(
            (person: Inline<GenId>, alias: Inline<_>),
            or!(
                pattern!(&dataset, [{ ?person @ profile::display_name: ?alias }]),
                pattern!(&dataset, [{ ?person @ profile::nickname: ?alias }])
            )
        )
        .map(|(p, a)| (p.raw, a.raw))
        .collect();

        prop_assert_eq!(&forward, &oracle, "or! must equal the set-union oracle");
        prop_assert_eq!(&backward, &oracle, "or! must be arm-order independent");
    }
}
