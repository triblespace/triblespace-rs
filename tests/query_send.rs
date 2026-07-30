//! Auto-trait and resumable-state regressions for the ordinary query iterator.

use std::rc::Rc;
use std::sync::Mutex;

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::query::{Query, VariableContext};
use triblespace::prelude::*;

fn assert_send<T: Send>(_: T) {}

/// The manual `Query::clone` bound must stay independent of `R: Clone` after
/// the residual cursor has started.
#[cfg(feature = "parallel")]
#[test]
fn clone_after_iteration_does_not_require_clone_output() {
    struct NonClone;

    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let values = [1u64, 2, 3, 4].map(U256BE::inline_from);
    let constraint = or!(
        variable.is(values[0]),
        variable.is(values[1]),
        variable.is(values[2]),
        variable.is(values[3])
    );
    let mut query = Query::new(constraint, |_| Some(NonClone));

    assert!(query.next().is_some());
    assert!(query.next().is_some());
    let cloned = query.clone();
    assert_eq!(query.count(), cloned.count());
}

/// A started query has already published progress. Ordinary Rayon conversion
/// drains its residual state as one exact leaf instead of restarting or
/// repartitioning it.
#[cfg(feature = "parallel")]
#[test]
fn partially_consumed_query_into_par_iter_keeps_exact_remainder() {
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let values = [1u64, 2, 3, 4].map(U256BE::inline_from);
    let constraint = or!(
        variable.is(values[0]),
        variable.is(values[1]),
        variable.is(values[2]),
        variable.is(values[3])
    );
    let mut query = Query::new(constraint, move |binding| {
        binding.get(variable.index).copied()
    });

    assert!(query.next().is_some());
    let mut expected = query.clone().collect::<Vec<_>>();
    let mut actual = query.into_par_iter().collect::<Vec<_>>();
    // The engine's contract is bag equivalence, not emission order.
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

/// Ordinary parallel residual execution must descend through an initially
/// deterministic chain, reach a late block-native branch, and preserve
/// postprocessor filtering (`None`) in every shard.
#[cfg(feature = "parallel")]
#[test]
fn fresh_parallel_query_handles_a_deep_late_branch() {
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    let mut context = VariableContext::new();
    let a = context.next_variable::<U256BE>();
    let b = context.next_variable::<U256BE>();
    let c = context.next_variable::<U256BE>();
    let d = context.next_variable::<U256BE>();
    let branch = context.next_variable::<U256BE>();
    let values = [10u64, 11, 12, 13, 14, 15, 16, 17].map(U256BE::inline_from);
    let constraint = and!(
        a.is(U256BE::inline_from(1u64)),
        b.is(U256BE::inline_from(2u64)),
        c.is(U256BE::inline_from(3u64)),
        d.is(U256BE::inline_from(4u64)),
        or!(
            branch.is(values[0]),
            branch.is(values[1]),
            branch.is(values[2]),
            branch.is(values[3]),
            branch.is(values[4]),
            branch.is(values[5]),
            branch.is(values[6]),
            branch.is(values[7])
        )
    );
    let query = Query::new(constraint, move |binding| {
        let value = *binding.get(branch.index)?;
        values
            .iter()
            .position(|candidate| candidate.raw == value)
            .filter(|index| index % 2 == 0)
            .map(|_| value)
    });
    let one_worker = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();
    let four_workers = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    // Construct outside the custom pool: shard budgets must be derived when
    // the iterator is consumed, not from whichever pool happened to create it.
    let one_iter = query.clone().into_par_iter();
    let mut one_actual = one_worker.install(|| one_iter.collect::<Vec<_>>());

    let four_iter = query.into_par_iter();
    let mut actual = four_workers.install(|| four_iter.collect::<Vec<_>>());

    one_actual.sort_unstable();
    actual.sort_unstable();
    let mut expected = [values[0].raw, values[2].raw, values[4].raw, values[6].raw];
    expected.sort_unstable();
    assert_eq!(one_actual, expected);
    assert_eq!(actual, expected);
}
