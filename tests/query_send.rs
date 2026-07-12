//! Auto-trait and resumable-state regressions for the ordinary query iterator.

use std::rc::Rc;

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::query::{dag_stats, Query, VariableContext};
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

    // Starting the default DAG scheduler must not change the query type's
    // auto traits: projected values are postprocessed on demand, never stored
    // in the worklist.
    let mut context = VariableContext::new();
    let variable = context.next_variable::<U256BE>();
    let constraint = variable.is(U256BE::inline_from(1u64));
    let mut started = Query::new(constraint, |_| Some(Rc::new(())));
    assert!(started.next().is_some());
    assert_send(started);
}

#[test]
fn ordinary_query_uses_lazy_dag_by_default() {
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
    let expected = query.clone().collect::<Vec<_>>();
    let actual = query.into_par_iter().collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[cfg(feature = "parallel")]
#[test]
fn fresh_query_into_par_iter_matches_dag_default() {
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

    let mut expected = query.clone().collect::<Vec<_>>();
    let mut actual = query.into_par_iter().collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
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
