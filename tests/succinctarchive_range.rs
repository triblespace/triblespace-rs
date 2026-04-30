//! Mirrors `triblesetrangeconstraint`'s test on `SuccinctArchive`. The
//! bench-relevant case is range-filtered queries (date windows on
//! `cwork:dateModified`), which collapsed before this constraint
//! existed because the previous fallback was a full V-column scan.

use triblespace::core::blob::schemas::succinctarchive::OrderedUniverse;
use triblespace::core::blob::schemas::succinctarchive::SuccinctArchive;
use triblespace::core::query::Constraint;
use triblespace::core::query::VariableContext;
use triblespace::prelude::valueschemas::R256BE;
use triblespace::prelude::*;

attributes! {
    "BB00000000000000BB00000000000000" as range_test_score: R256BE;
}

#[test]
fn value_in_range_proposes_correctly() {
    let e1 = ufoid();
    let e2 = ufoid();
    let e3 = ufoid();
    let e4 = ufoid();

    let v10: Value<R256BE> = 10i128.to_value();
    let v50: Value<R256BE> = 50i128.to_value();
    let v90: Value<R256BE> = 90i128.to_value();
    let v100: Value<R256BE> = 100i128.to_value();

    let mut set = TribleSet::new();
    set += entity! { &e1 @ range_test_score: v10 };
    set += entity! { &e2 @ range_test_score: v50 };
    set += entity! { &e3 @ range_test_score: v90 };
    set += entity! { &e4 @ range_test_score: v100 };
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    // Without range: all 4 results.
    let all: Vec<Value<R256BE>> = find!(
        v: Value<R256BE>,
        pattern!(&archive, [{ range_test_score: ?v }])
    )
    .collect();
    assert_eq!(all.len(), 4);

    // With value_in_range [20..=95]: only v50 and v90.
    let min: Value<R256BE> = 20i128.to_value();
    let max: Value<R256BE> = 95i128.to_value();
    let mut filtered: Vec<Value<R256BE>> = find!(
        v: Value<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, min, max),
        )
    )
    .collect();
    filtered.sort();
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0], v50);
    assert_eq!(filtered[1], v90);

    // Boundary: exact match on min and max.
    let min_exact: Value<R256BE> = 50i128.to_value();
    let max_exact: Value<R256BE> = 90i128.to_value();
    let mut exact: Vec<Value<R256BE>> = find!(
        v: Value<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, min_exact, max_exact),
        )
    )
    .collect();
    exact.sort();
    assert_eq!(exact.len(), 2);
    assert_eq!(exact[0], v50);
    assert_eq!(exact[1], v90);

    // Empty range: no results.
    let min_empty: Value<R256BE> = 91i128.to_value();
    let max_empty: Value<R256BE> = 99i128.to_value();
    let empty: Vec<Value<R256BE>> = find!(
        v: Value<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, min_empty, max_empty),
        )
    )
    .collect();
    assert_eq!(empty.len(), 0);

    // Inverted range (min > max): empty.
    let inverted: Vec<Value<R256BE>> = find!(
        v: Value<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, v90, v10),
        )
    )
    .collect();
    assert_eq!(inverted.len(), 0);
}

#[test]
fn estimate_counts_distinct_v_position_codes() {
    // Three distinct scores in V position, each shared by multiple
    // entities. The estimate should report 3 (codes in V) regardless of
    // how many tribles point at them.
    let v10: Value<R256BE> = 10i128.to_value();
    let v50: Value<R256BE> = 50i128.to_value();
    let v90: Value<R256BE> = 90i128.to_value();

    let mut set = TribleSet::new();
    for _ in 0..1 {
        set += entity! { &ufoid() @ range_test_score: v10 };
    }
    for _ in 0..4 {
        set += entity! { &ufoid() @ range_test_score: v50 };
    }
    for _ in 0..1 {
        set += entity! { &ufoid() @ range_test_score: v90 };
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    let mut ctx = VariableContext::new();
    let v = ctx.next_variable::<R256BE>();
    let min: Value<R256BE> = 0i128.to_value();
    let max: Value<R256BE> = 100i128.to_value();
    let constraint = archive.value_in_range(v, min, max);

    let estimate = constraint.estimate(v.index, &Default::default());
    assert_eq!(
        estimate,
        Some(3),
        "estimate must count distinct V-position codes in range"
    );
}
