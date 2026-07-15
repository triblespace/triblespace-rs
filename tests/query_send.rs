//! Auto-trait and resumable-state regressions for the ordinary query iterator.

use std::rc::Rc;
use std::sync::Mutex;

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::query::{dag_stats, Query, VariableContext};
use triblespace::prelude::*;

static DAG_STATS_TEST_LOCK: Mutex<()> = Mutex::new(());

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

    // Starting the shape-selected default scheduler must not change the query
    // type's auto traits: projected values are postprocessed on demand, never
    // stored in either worklist.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut started = Query::new(constraint, |_| Some(Rc::new(())));
    assert!(started.next().is_some());
    assert_send(started);

    // The explicit lazy-DAG fallback likewise stores only raw rows and
    // planning state, never a projected `R`.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut dag = Query::new(constraint, |_| Some(Rc::new(()))).lazy_dag_scheduler();
    assert!(dag.next().is_some());
    assert_send(dag);

    // The residual runtime also stores only raw rows and planning state.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut residual = Query::new(constraint, |_| Some(Rc::new(()))).residual_state_scheduler();
    assert!(residual.next().is_some());
    assert_send(residual);
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
    assert!(state.contains("scheduler: ResidualState"), "{state}");
    assert!(state.contains("residual_started: true"), "{state}");
    assert!(state.contains("dag_started: false"), "{state}");
}

#[test]
fn ordinary_query_keeps_lazy_dag_for_disjoint_and_leaves() {
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
    assert!(state.contains("scheduler: LazyDag"), "{state}");
    assert!(state.contains("residual_started: false"), "{state}");
    assert!(state.contains("dag_started: true"), "{state}");
}

#[test]
fn ordinary_query_keeps_lazy_dag_for_opaque_root() {
    let _stats_guard = DAG_STATS_TEST_LOCK.lock().unwrap();
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));

    dag_stats::reset();
    dag_stats::set_enabled(true);
    let rows = Query::new(constraint, |_| Some(())).count();
    let pops = dag_stats::pops();
    dag_stats::set_enabled(false);

    assert_eq!(rows, 1);
    assert!(
        pops > 0,
        "ordinary Query iteration did not run the DAG worklist"
    );
}

#[test]
fn ordinary_query_keeps_lazy_dag_for_exposed_one_leaf_and() {
    let _stats_guard = DAG_STATS_TEST_LOCK.lock().unwrap();
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = and!(variable.is(U256BE::inline_from(1u64)));

    dag_stats::reset();
    dag_stats::set_enabled(true);
    let rows = Query::new(constraint, |_| Some(())).count();
    let pops = dag_stats::pops();
    dag_stats::set_enabled(false);

    assert_eq!(rows, 1);
    assert!(pops > 0, "one-leaf AND did not retain the lazy DAG");
}

#[test]
fn explicit_lazy_dag_override_bypasses_overlapping_residual_default() {
    let _stats_guard = DAG_STATS_TEST_LOCK.lock().unwrap();
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let constraint = and!(variable.is(one), variable.is(one));

    dag_stats::reset();
    dag_stats::set_enabled(true);
    let rows = Query::new(constraint, |_| Some(()))
        .lazy_dag_scheduler()
        .count();
    let pops = dag_stats::pops();
    dag_stats::set_enabled(false);

    assert_eq!(rows, 1);
    assert!(pops > 0, "explicit lazy-DAG override was ignored");
}

/// Cloning the ordinary lazy-DAG iterator after a pull snapshots its raw
/// worklist and staged rows exactly, without requiring the output type itself
/// to implement `Clone`.
#[cfg(feature = "parallel")]
#[test]
fn clone_after_iteration_snapshots_remaining_dag_state() {
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
    // The second resumption has width two: it stages two raw rows and yields
    // one, so the clone must include both the residual worklist and the
    // unconsumed staged row.
    assert!(query.next().is_some());
    let cloned = query.clone();
    assert_eq!(query.collect::<Vec<_>>(), cloned.collect::<Vec<_>>());
}

/// The clone operation snapshots raw rows, not already projected items, so its
/// bounds must remain independent of `R: Clone` even after the DAG has started.
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

/// The same manual `Query::clone` bound must stay independent of `R: Clone`
/// when the owned cursor is the residual state machine.
#[cfg(feature = "parallel")]
#[test]
fn residual_clone_after_iteration_does_not_require_clone_output() {
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
    let mut query = Query::new(constraint, |_| Some(NonClone)).residual_state_scheduler();

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
    .residual_state_scheduler()
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
    })
    .residual_state_scheduler();

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

/// A partially consumed ordinary query owns a DAG worklist while its legacy
/// DFS cursor is untouched. Converting it to rayon must drain that remaining
/// worklist as one leaf, not split and restart the DFS cursor from the seed.
#[cfg(feature = "parallel")]
#[test]
fn partially_consumed_dag_query_into_par_iter_keeps_exact_remainder() {
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
    let started_for_explicit_dag = query.clone();
    let expected = query.clone().collect::<Vec<_>>();
    let actual = query.into_par_iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);

    let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _ = started_for_explicit_dag.into_par_dag_iter();
    }))
    .expect_err("the explicit parallel DAG entry point must require a fresh query");
    let message = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("cannot initialize parallel DAG iteration"));
}

/// A started residual cursor has no scalar cursor position to split. Ordinary
/// Rayon conversion therefore drains it as one exact leaf.
#[cfg(feature = "parallel")]
#[test]
fn partially_consumed_residual_query_into_par_iter_keeps_exact_remainder() {
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
    })
    .residual_state_scheduler();

    assert!(query.next().is_some());
    let expected = query.clone().collect::<Vec<_>>();
    let actual = query.into_par_iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[cfg(feature = "parallel")]
#[test]
fn pulled_query_rejects_every_seed_restarting_selector() {
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
    })
    .residual_state_scheduler();
    assert!(query.next().is_some());

    let panics = [
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.sequential())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.residual_state_scheduler())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.lazy_dag_scheduler())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.solve_dag_lazy())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe({
            let query = query.clone();
            move || drop(query.solve_residual_state_lazy())
        })),
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drop(query.into_par_dag_iter())
        })),
    ];
    assert!(panics.into_iter().all(|result| result.is_err()));
}

#[cfg(feature = "parallel")]
#[test]
fn fresh_query_into_par_iter_matches_scalar_scheduler() {
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

    let mut expected = query.clone().sequential().collect::<Vec<_>>();
    let mut actual = query.into_par_iter().collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);

    // Ordinary Rayon conversion remains the scalar splitter even when the
    // automatic shape selector chose residual execution.
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
    let mut selected = Query::new(constraint, move |binding| {
        binding.get(variable.index).copied()
    })
    .into_par_iter()
    .collect::<Vec<_>>();
    selected.sort_unstable();
    assert_eq!(selected, expected);
}

/// The explicit parallel-DAG path must descend through an initially
/// deterministic chain, split a late block-native branch within the `N - 1`
/// budget, and preserve postprocessor filtering (`None`) in every shard.
#[cfg(feature = "parallel")]
#[test]
fn fresh_parallel_query_splits_a_deep_late_branch() {
    use rayon::iter::ParallelIterator;

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

    let _stats_guard = DAG_STATS_TEST_LOCK.lock().unwrap();
    dag_stats::reset();
    dag_stats::set_enabled(true);
    // Construct outside the custom pool: shard budgets must be derived when
    // the iterator is consumed, not from whichever pool happened to create it.
    let one_iter = query.clone().into_par_dag_iter();
    let mut one_actual = one_worker.install(|| one_iter.collect::<Vec<_>>());
    let one_splits = dag_stats::parallel_splits();

    dag_stats::reset();
    let four_iter = query.into_par_dag_iter();
    let mut actual = four_workers.install(|| four_iter.collect::<Vec<_>>());
    let pops = dag_stats::pops();
    let splits = dag_stats::parallel_splits();
    dag_stats::set_enabled(false);

    one_actual.sort_unstable();
    actual.sort_unstable();
    let mut expected = [values[0].raw, values[2].raw, values[4].raw, values[6].raw];
    expected.sort_unstable();
    assert_eq!(one_actual, expected);
    assert_eq!(actual, expected);
    assert_eq!(one_splits, 0, "one worker must create no DAG shards");
    assert!(pops > 0, "parallel query bypassed the DAG worklist");
    assert!(
        (1..=3).contains(&splits),
        "four-worker query must split its late affine frontier without exceeding N-1; got {splits}"
    );
}

#[test]
fn sequential_opt_in_preserves_scalar_iterator() {
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let two = U256BE::inline_from(2u64);
    let constraint = or!(variable.is(one), variable.is(two));
    let mut rows = Query::new(constraint, move |binding| {
        binding.get(variable.index).copied()
    })
    .sequential()
    .collect::<Vec<_>>();
    rows.sort_unstable();

    assert_eq!(rows, vec![one.raw, two.raw]);
}
