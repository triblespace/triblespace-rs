//! Auto-trait and resumable-state regressions for the ordinary query iterator.

use std::rc::Rc;
use std::sync::Mutex;

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::query::{Query, VariableContext};
use triblespace::prelude::*;

fn assert_send<T: Send>(_: T) {}

/// `Query` stores the constraint and postprocessor, but not values returned by
/// that postprocessor. A non-`Send` result type must therefore not make an
/// otherwise `Send` ordinary query non-`Send`.
#[test]
fn ordinary_query_with_non_send_output_is_send() {
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let query = Query::new(constraint, |_| Some(Rc::new(())));

    assert_send(query);

    // Starting the residual solver must not change the query type's auto
    // traits: projected values are postprocessed on demand, never stored in
    // the worklist.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut started = Query::new(constraint, |_| Some(Rc::new(())));
    assert!(started.next().is_some());
    assert_send(started);
}

#[test]
fn ordinary_query_uses_residual_for_exposed_overlapping_and() {
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let constraint = and!(variable.is(one), variable.is(one));
    let mut query = Query::new(constraint, |_| Some(()));

    assert_eq!(query.next(), Some(()));
    let state = format!("{query:?}");
    assert!(state.contains("residual_started: true"), "{state}");
}

#[test]
fn ordinary_query_uses_residual_for_disjoint_and_leaves() {
    let mut context = VariableContext::new();
    let left = context.next_variable::<U256BE>();
    let right = context.next_variable::<U256BE>();
    let constraint = and!(
        left.is(U256BE::inline_from(1u64)),
        right.is(U256BE::inline_from(2u64))
    );
    let mut query = Query::new(constraint, |_| Some(()));

    assert_eq!(query.next(), Some(()));
    let state = format!("{query:?}");
    assert!(state.contains("residual_started: true"), "{state}");
}

#[test]
fn ordinary_query_uses_residual_for_opaque_root() {
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut query = Query::new(constraint, |_| Some(()));

    assert_eq!(query.next(), Some(()));
    let state = format!("{query:?}");
    assert!(state.contains("residual_started: true"), "{state}");
}

#[test]
fn ordinary_query_uses_residual_for_exposed_one_leaf_and() {
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = and!(variable.is(U256BE::inline_from(1u64)));
    let mut query = Query::new(constraint, |_| Some(()));

    assert_eq!(query.next(), Some(()));
    let state = format!("{query:?}");
    assert!(state.contains("residual_started: true"), "{state}");
}

/// Cloning an ordinary residual iterator after a pull snapshots its raw
/// worklist and staged rows exactly.
#[cfg(feature = "parallel")]
#[test]
fn clone_after_iteration_snapshots_remaining_residual_state() {
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
    assert!(query.next().is_some());
    let cloned = query.clone();
    assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
}

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

#[test]
fn ordinary_residual_projection_filter_and_panic_resume_are_exact() {
    use std::sync::Arc;

    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let values = [1u64, 2, 3, 4].map(U256BE::inline_from);
    let constraint = or!(
        variable.is(values[0]),
        variable.is(values[1]),
        variable.is(values[2]),
        variable.is(values[3])
    );
    let mut filtered = Query::new(constraint, move |binding| {
        let value = *binding.get(variable.index)?;
        (value[31] % 2 == 0).then_some(value)
    })
    .collect::<Vec<_>>();
    filtered.sort_unstable();
    assert_eq!(filtered, vec![values[1].raw, values[3].raw]);

    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = or!(
        variable.is(values[0]),
        variable.is(values[1]),
        variable.is(values[2]),
        variable.is(values[3])
    );
    let projected = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&projected);
    let mut panicking = Query::new(constraint, move |binding| {
        let value = *binding.get(variable.index)?;
        let mut projected = record.lock().unwrap_or_else(|poison| poison.into_inner());
        projected.push(value);
        assert_ne!(projected.len(), 1, "first projection panics");
        Some(value)
    });

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panicking.next()));
    assert!(panic.is_err());
    let resumed = panicking.next().expect("a later raw row remains");
    let projected = projected
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    assert_eq!(projected.len(), 2);
    assert_ne!(projected[0], projected[1], "panicking row was repeated");
    assert_eq!(resumed, projected[1]);
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
    let expected = query.clone().collect::<Vec<_>>();
    let actual = query.into_par_iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[cfg(feature = "parallel")]
#[test]
fn pulled_query_rejects_seed_restarting_configuration() {
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

    let panics = [
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.solve_residual_state_lazy())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.solve_residual_state_lazy())
        })),
    ];
    assert!(panics.into_iter().all(|result| result.is_err()));
}

#[cfg(feature = "parallel")]
#[test]
fn fresh_query_into_par_iter_matches_explicit_residual_raw_set() {
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
    let query = Query::new(constraint, move |binding| {
        binding.get(variable.index).copied()
    });

    let mut expected = values.map(|value| value.raw).to_vec();
    let mut explicit_residual = query
        .clone()
        .solve_residual_state_lazy()
        .into_par_iter()
        .collect::<Vec<_>>();
    let mut ordinary = query.into_par_iter().collect::<Vec<_>>();
    expected.sort_unstable();
    explicit_residual.sort_unstable();
    ordinary.sort_unstable();
    assert_eq!(ordinary, expected);
    assert_eq!(ordinary, explicit_residual);

    // An overlapping conjunction exercises residual checked-state
    // reconvergence while retaining the same raw SET.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = and!(
        or!(
            variable.is(values[0]),
            variable.is(values[1]),
            variable.is(values[2]),
            variable.is(values[3])
        ),
        or!(
            variable.is(values[0]),
            variable.is(values[1]),
            variable.is(values[2]),
            variable.is(values[3])
        )
    );
    let selected_query = Query::new(constraint, move |binding| {
        binding.get(variable.index).copied()
    });
    let mut ordinary = selected_query.clone().into_par_iter().collect::<Vec<_>>();
    let mut explicit_residual = selected_query
        .solve_residual_state_lazy()
        .into_par_iter()
        .collect::<Vec<_>>();
    ordinary.sort_unstable();
    explicit_residual.sort_unstable();
    assert_eq!(ordinary, expected);
    assert_eq!(ordinary, explicit_residual);
}

#[cfg(feature = "parallel")]
#[test]
fn ordinary_parallel_residual_honors_early_consumer_cancellation() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let alternatives = (0u64..128)
        .map(|value| variable.is(U256BE::inline_from(value)))
        .collect();
    let projected = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&projected);
    let query = Query::new(
        Arc::new(triblespace::core::query::unionconstraint::UnionConstraint::new(alternatives)),
        move |binding| {
            calls.fetch_add(1, Ordering::SeqCst);
            binding.get(variable.index).copied()
        },
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    let found = pool.install(move || query.into_par_iter().find_any(|_| true));
    assert!(found.is_some());
    assert_eq!(
        projected.load(Ordering::SeqCst),
        1,
        "a full consumer must stop the residual fold before projecting another row"
    );
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
