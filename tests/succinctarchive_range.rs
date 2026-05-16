//! Mirrors `triblesetrangeconstraint`'s test on `SuccinctArchive`. The
//! bench-relevant case is range-filtered queries (date windows on
//! `cwork:dateModified`), which collapsed before this constraint
//! existed because the previous fallback was a full V-column scan.

use triblespace::core::blob::schemas::succinctarchive::OrderedUniverse;
use triblespace::core::blob::schemas::succinctarchive::SuccinctArchive;
use triblespace::core::query::Constraint;
use triblespace::core::query::VariableContext;
use triblespace::prelude::inlineschemas::R256BE;
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

    let v10: Inline<R256BE> = 10i128.to_inline();
    let v50: Inline<R256BE> = 50i128.to_inline();
    let v90: Inline<R256BE> = 90i128.to_inline();
    let v100: Inline<R256BE> = 100i128.to_inline();

    let mut set = TribleSet::new();
    set += entity! { &e1 @ range_test_score: v10 };
    set += entity! { &e2 @ range_test_score: v50 };
    set += entity! { &e3 @ range_test_score: v90 };
    set += entity! { &e4 @ range_test_score: v100 };
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    // Without range: all 4 results.
    let all: Vec<Inline<R256BE>> = find!(
        v: Inline<R256BE>,
        pattern!(&archive, [{ range_test_score: ?v }])
    )
    .collect();
    assert_eq!(all.len(), 4);

    // With value_in_range [20..=95]: only v50 and v90.
    let min: Inline<R256BE> = 20i128.to_inline();
    let max: Inline<R256BE> = 95i128.to_inline();
    let mut filtered: Vec<Inline<R256BE>> = find!(
        v: Inline<R256BE>,
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
    let min_exact: Inline<R256BE> = 50i128.to_inline();
    let max_exact: Inline<R256BE> = 90i128.to_inline();
    let mut exact: Vec<Inline<R256BE>> = find!(
        v: Inline<R256BE>,
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
    let min_empty: Inline<R256BE> = 91i128.to_inline();
    let max_empty: Inline<R256BE> = 99i128.to_inline();
    let empty: Vec<Inline<R256BE>> = find!(
        v: Inline<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, min_empty, max_empty),
        )
    )
    .collect();
    assert_eq!(empty.len(), 0);

    // Inverted range (min > max): empty.
    let inverted: Vec<Inline<R256BE>> = find!(
        v: Inline<R256BE>,
        and!(
            pattern!(&archive, [{ range_test_score: ?v }]),
            archive.value_in_range(v, v90, v10),
        )
    )
    .collect();
    assert_eq!(inverted.len(), 0);
}

#[test]
fn estimate_is_universe_code_range_upper_bound() {
    // The cardinality estimate is the *upper bound*: the count of
    // universe codes whose byte-lex value falls in [min, max], not
    // restricted to those that appear in V position. That's an O(1)
    // cached value; the V-position filter happens during propose.
    //
    // The estimate must always be >= the actual distinct V-codes that
    // would be proposed. For three distinct V-position codes (10, 50,
    // 90) all in the range [0, 100], the estimate must be >= 3.
    let v10: Inline<R256BE> = 10i128.to_inline();
    let v50: Inline<R256BE> = 50i128.to_inline();
    let v90: Inline<R256BE> = 90i128.to_inline();

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
    let min: Inline<R256BE> = 0i128.to_inline();
    let max: Inline<R256BE> = 100i128.to_inline();
    let constraint = archive.value_in_range(v, min, max);

    let estimate = constraint
        .estimate(v.index, &Default::default())
        .expect("estimate is Some for the V variable");
    assert!(
        estimate >= 3,
        "estimate must upper-bound actual V-codes-in-range; got {estimate}, need >= 3"
    );
    // Verify propose enumerates exactly the 3 distinct V values.
    use triblespace::core::query::Binding;
    let mut proposals = Vec::new();
    constraint.propose(v.index, &Binding::default(), &mut proposals);
    assert_eq!(
        proposals.len(),
        3,
        "propose must yield exactly the V-position codes in range"
    );
}
