use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::inline::{Inline, RawInline, TryFromInline};
use triblespace::core::query::{
    CandidateSink, Constraint, EstimateSink, Query, RowsView, VariableContext, VariableId,
    VariableSet,
};
use triblespace::prelude::*;

fn projected_query() -> impl Iterator<Item = [u8; 32]> {
    let mut context = VariableContext::new();
    let head = context.next_variable::<U256BE>();
    let witness = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let left = U256BE::inline_from(10u64);
    let right = U256BE::inline_from(20u64);
    Query::new_projected(
        and!(head.is(one), or!(witness.is(left), witness.is(right))),
        [head.index],
        move |binding| binding.get(head.index).copied(),
    )
}

#[derive(Clone)]
struct CountingHiddenFanout {
    witness: VariableId,
    tail: VariableId,
    tail_rows: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct LateUnaryFilter {
    variable: VariableId,
    accepted: RawInline,
    rejected_seen: Arc<AtomicUsize>,
    accepted_seen: Arc<AtomicUsize>,
}

impl Constraint<'_> for LateUnaryFilter {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable {
            return false;
        }
        out.fill(usize::MAX, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        assert_ne!(variable, self.variable, "late filter became the proposer");
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, self.variable);
        candidates.retain(|_, value| {
            if *value == self.accepted {
                self.accepted_seen.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                self.rejected_seen.fetch_add(1, Ordering::SeqCst);
                false
            }
        });
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| row[column] == self.accepted))
    }
}

impl Constraint<'_> for CountingHiddenFanout {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.witness).union(VariableSet::new_singleton(self.tail))
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let estimate = if variable == self.witness {
            1
        } else if variable == self.tail {
            2
        } else {
            return false;
        };
        out.fill(estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.witness {
            for row in 0..view.len() {
                for value in 0..64 {
                    candidates.push(row as u32, U256BE::inline_from(value as u64).raw);
                }
            }
        } else if variable == self.tail {
            self.tail_rows.fetch_add(view.len(), Ordering::SeqCst);
            for row in 0..view.len() {
                candidates.push(row as u32, U256BE::inline_from(255u64).raw);
            }
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }
}

#[test]
fn explicit_projection_is_distinct_across_serial_schedulers() {
    assert_eq!(projected_query().count(), 1);

    let mut context = VariableContext::new();
    let head = context.next_variable::<U256BE>();
    let witness = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let left = U256BE::inline_from(10u64);
    let right = U256BE::inline_from(20u64);
    let make = move || {
        Query::new_projected(
            and!(head.is(one), or!(witness.is(left), witness.is(right))),
            [head.index],
            move |binding| binding.get(head.index).copied(),
        )
    };

    assert_eq!(make().sequential().count(), 1);
    assert_eq!(make().lazy_dag_scheduler().count(), 1);
    assert_eq!(make().residual_state_scheduler().count(), 1);
    assert_eq!(make().solve_dag_lazy().count(), 1);
    assert_eq!(make().solve_residual_state_lazy().count(), 1);
    assert_eq!(make().solve_residual_state().len(), 1);
}

#[test]
fn hidden_existential_claim_waits_for_a_complete_correlated_witness() {
    let one = U256BE::inline_from(1u64);
    let rejected = U256BE::inline_from(10u64);
    let accepted = U256BE::inline_from(20u64);
    let rejected_seen = Arc::new(AtomicUsize::new(0));
    let accepted_seen = Arc::new(AtomicUsize::new(0));

    let rows = find!(
        x: Inline<U256BE>,
        temp!((y), and!(
            x.is(one),
            or!(y.is(rejected), y.is(accepted)),
            LateUnaryFilter {
                variable: y.index,
                accepted: accepted.raw,
                rejected_seen: Arc::clone(&rejected_seen),
                accepted_seen: Arc::clone(&accepted_seen),
            }
        ))
    )
    .collect::<Vec<_>>();

    assert_eq!(rows, vec![one]);
    assert!(
        rejected_seen.load(Ordering::SeqCst) > 0,
        "the rejected R witness must reach the correlated S filter"
    );
    assert!(
        accepted_seen.load(Ordering::SeqCst) > 0,
        "a later complete witness must remain able to claim the projected key"
    );
}

#[test]
fn projection_key_includes_every_visible_tail_across_hidden_witnesses() {
    let x_value = U256BE::inline_from(1u64);
    let y_left = U256BE::inline_from(10u64);
    let y_right = U256BE::inline_from(20u64);
    let z_left = U256BE::inline_from(100u64);
    let z_right = U256BE::inline_from(200u64);
    let make = || {
        find!(
            (x: Inline<U256BE>, z: Inline<U256BE>),
            temp!((y), and!(
                x.is(x_value),
                or!(y.is(y_left), y.is(y_right)),
                or!(
                    and!(y.is(y_left), z.is(z_left)),
                    and!(y.is(y_right), z.is(z_right))
                )
            ))
        )
    };
    let expected = vec![(x_value, z_left), (x_value, z_right)];
    let sorted = |mut rows: Vec<(Inline<U256BE>, Inline<U256BE>)>| {
        rows.sort_unstable_by_key(|(_, z)| z.raw);
        rows
    };

    assert_eq!(sorted(make().collect()), expected);
    assert_eq!(sorted(make().sequential().collect()), expected);
    assert_eq!(sorted(make().solve_dag_lazy().collect()), expected);
    assert_eq!(sorted(make().solve_residual_state_lazy().collect()), expected);
}

#[test]
fn query_new_uses_the_complete_constraint_variable_head() {
    let mut context = VariableContext::new();
    let head = context.next_variable::<U256BE>();
    let witness = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let left = U256BE::inline_from(10u64);
    let right = U256BE::inline_from(20u64);

    let make = || {
        Query::new(
            and!(head.is(one), or!(witness.is(left), witness.is(right))),
            move |binding| binding.get(head.index).copied(),
        )
    };

    assert_eq!(make().collect::<Vec<_>>(), vec![one.raw, one.raw]);
    assert_eq!(make().sequential().count(), 2);
    assert_eq!(make().lazy_dag_scheduler().count(), 2);
    assert_eq!(make().residual_state_scheduler().count(), 2);
    assert_eq!(make().solve_dag_lazy().count(), 2);
    assert_eq!(make().solve_residual_state_lazy().count(), 2);
    assert_eq!(make().solve_residual_state().len(), 2);
}

#[test]
fn explicit_projection_hides_non_head_bindings_from_the_mapper() {
    let mut context = VariableContext::new();
    let head = context.next_variable::<U256BE>();
    let witness = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let hidden = U256BE::inline_from(10u64);

    let rows = Query::new_projected(
        and!(head.is(one), witness.is(hidden)),
        [head.index],
        move |binding| {
            assert!(binding.get(witness.index).is_none());
            binding.get(head.index).copied()
        },
    )
    .collect::<Vec<_>>();

    assert_eq!(rows, vec![one.raw]);
}

#[test]
fn find_supplies_its_declared_head_and_unit_is_a_singleton() {
    let one = U256BE::inline_from(1u64);
    let left = U256BE::inline_from(10u64);
    let right = U256BE::inline_from(20u64);

    let projected = find!(
        head: Inline<U256BE>,
        temp!((witness), and!(
            head.is(one),
            or!(witness.is(left), witness.is(right))
        ))
    )
    .collect::<Vec<_>>();
    assert_eq!(projected, vec![one]);

    let unit = find!(
        (),
        temp!(
            (head, witness),
            and!(head.is(one), or!(witness.is(left), witness.is(right)))
        )
    )
    .collect::<Vec<_>>();
    assert_eq!(unit, vec![()]);
}

#[test]
fn empty_head_stops_after_the_first_complete_hidden_witness() {
    let tail_rows = Arc::new(AtomicUsize::new(0));
    let mut query = Query::new_projected(
        CountingHiddenFanout {
            witness: 0,
            tail: 1,
            tail_rows: Arc::clone(&tail_rows),
        },
        [],
        |_| Some(()),
    );

    assert_eq!(query.next(), Some(()));
    assert_eq!(tail_rows.load(Ordering::SeqCst), 1);
    assert_eq!(query.next(), None);
    assert_eq!(tail_rows.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_head_none_and_panic_finish_the_singleton_key() {
    let make = |tail_rows: Arc<AtomicUsize>, calls: Arc<AtomicUsize>, panic_first: bool| {
        Query::new_projected(
            CountingHiddenFanout {
                witness: 0,
                tail: 1,
                tail_rows,
            },
            [],
            move |_| {
                let invocation = calls.fetch_add(1, Ordering::SeqCst);
                assert!(!(panic_first && invocation == 0), "mapper panic");
                None::<()>
            },
        )
    };

    let none_rows = Arc::new(AtomicUsize::new(0));
    let none_calls = Arc::new(AtomicUsize::new(0));
    let mut filtered = make(Arc::clone(&none_rows), Arc::clone(&none_calls), false);
    assert_eq!(filtered.next(), None);
    assert_eq!(filtered.next(), None);
    assert_eq!(none_rows.load(Ordering::SeqCst), 1);
    assert_eq!(none_calls.load(Ordering::SeqCst), 1);

    let panic_rows = Arc::new(AtomicUsize::new(0));
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let mut panicking = make(Arc::clone(&panic_rows), Arc::clone(&panic_calls), true);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panicking.next()));
    assert!(panic.is_err());
    assert_eq!(panicking.next(), None);
    assert_eq!(panic_rows.load(Ordering::SeqCst), 1);
    assert_eq!(panic_calls.load(Ordering::SeqCst), 1);
}

#[derive(Debug, Eq, PartialEq)]
struct Collapsed;

impl TryFromInline<'_, U256BE> for Collapsed {
    type Error = std::convert::Infallible;

    fn try_from_inline(_: &Inline<U256BE>) -> Result<Self, Self::Error> {
        Ok(Self)
    }
}

static FAILED_CONVERSIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct AlwaysFails;

impl TryFromInline<'_, U256BE> for AlwaysFails {
    type Error = ();

    fn try_from_inline(_: &Inline<U256BE>) -> Result<Self, Self::Error> {
        FAILED_CONVERSIONS.fetch_add(1, Ordering::SeqCst);
        Err(())
    }
}

#[test]
fn distinctness_uses_raw_head_bytes_not_converted_rust_equality() {
    let one = U256BE::inline_from(1u64);
    let two = U256BE::inline_from(2u64);
    let rows = find!(value: Collapsed, or!(value.is(one), value.is(two))).collect::<Vec<_>>();

    assert_eq!(rows, vec![Collapsed, Collapsed]);
}

#[test]
fn failed_conversion_consumes_the_raw_key_before_hidden_witnesses_retry_it() {
    FAILED_CONVERSIONS.store(0, Ordering::SeqCst);

    let one = U256BE::inline_from(1u64);
    let left = U256BE::inline_from(10u64);
    let right = U256BE::inline_from(20u64);
    let rows = find!(
        head: AlwaysFails,
        temp!((witness), and!(
            head.is(one),
            or!(witness.is(left), witness.is(right))
        ))
    )
    .collect::<Vec<_>>();

    assert!(rows.is_empty());
    assert_eq!(FAILED_CONVERSIONS.load(Ordering::SeqCst), 1);
}

#[test]
fn ordered_raw_head_tuples_do_not_collapse_swapped_values() {
    let one = U256BE::inline_from(1u64);
    let two = U256BE::inline_from(2u64);
    let mut rows = find!(
        (left: Inline<U256BE>, right: Inline<U256BE>),
        or!(
            and!(left.is(one), right.is(two)),
            and!(left.is(two), right.is(one))
        )
    )
    .collect::<Vec<_>>();
    rows.sort_unstable_by_key(|(left, _)| left.raw);

    assert_eq!(rows, vec![(one, two), (two, one)]);
}

#[test]
fn mapper_none_and_panic_each_consume_the_raw_key() {
    let make = |calls: Arc<AtomicUsize>, panic_first: bool| {
        let mut context = VariableContext::new();
        let head = context.next_variable::<U256BE>();
        let witness = context.next_variable::<U256BE>();
        let one = U256BE::inline_from(1u64);
        let left = U256BE::inline_from(10u64);
        let right = U256BE::inline_from(20u64);
        Query::new_projected(
            and!(head.is(one), or!(witness.is(left), witness.is(right))),
            [head.index],
            move |_| {
                let invocation = calls.fetch_add(1, Ordering::SeqCst);
                assert!(!(panic_first && invocation == 0), "mapper panic");
                None::<()>
            },
        )
    };

    let none_calls = Arc::new(AtomicUsize::new(0));
    assert!(make(Arc::clone(&none_calls), false).next().is_none());
    assert_eq!(none_calls.load(Ordering::SeqCst), 1);

    let panic_calls = Arc::new(AtomicUsize::new(0));
    let mut panicking = make(Arc::clone(&panic_calls), true);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panicking.next()));
    assert!(panic.is_err());
    assert!(panicking.next().is_none());
    assert_eq!(panic_calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "parallel")]
#[test]
fn rayon_shards_share_one_projection_claim_domain() {
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    let make = || {
        let mut context = VariableContext::new();
        let head = context.next_variable::<U256BE>();
        let witness = context.next_variable::<U256BE>();
        let one = U256BE::inline_from(1u64);
        let alternatives = (0..64)
            .map(|value| witness.is(U256BE::inline_from(value as u64)))
            .collect::<Vec<_>>();
        Query::new_projected(
            and!(
                head.is(one),
                triblespace::core::query::unionconstraint::UnionConstraint::new(alternatives)
            ),
            [head.index],
            move |binding| binding.get(head.index).copied(),
        )
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    assert_eq!(pool.install(|| make().into_par_iter().count()), 1);
    assert_eq!(pool.install(|| make().into_par_dag_iter().count()), 1);
    assert_eq!(
        pool.install(|| make().into_par_residual_state_iter().count()),
        1
    );
}

#[cfg(feature = "parallel")]
#[test]
fn rayon_full_heads_preserve_every_distinct_complete_binding_without_claims() {
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    let make = || {
        let mut context = VariableContext::new();
        let head = context.next_variable::<U256BE>();
        let witness = context.next_variable::<U256BE>();
        let one = U256BE::inline_from(1u64);
        let alternatives = (0..64)
            .map(|value| witness.is(U256BE::inline_from(value as u64)))
            .collect::<Vec<_>>();
        Query::new(
            and!(
                head.is(one),
                triblespace::core::query::unionconstraint::UnionConstraint::new(alternatives)
            ),
            move |binding| binding.get(head.index).copied(),
        )
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    assert_eq!(pool.install(|| make().into_par_iter().count()), 64);
    assert_eq!(pool.install(|| make().into_par_dag_iter().count()), 64);
    assert_eq!(
        pool.install(|| make().into_par_residual_state_iter().count()),
        64
    );
}

#[cfg(feature = "parallel")]
#[test]
fn cloning_a_started_query_snapshots_claims_independently() {
    let mut context = VariableContext::new();
    let head = context.next_variable::<U256BE>();
    let one = U256BE::inline_from(1u64);
    let two = U256BE::inline_from(2u64);
    let three = U256BE::inline_from(3u64);
    let mut query = Query::new_projected(
        or!(head.is(one), head.is(two), head.is(three)),
        [head.index],
        move |binding| binding.get(head.index).copied(),
    );

    assert!(query.next().is_some());
    let cloned = query.clone();
    let mut left = query.collect::<Vec<_>>();
    let mut right = cloned.collect::<Vec<_>>();
    left.sort_unstable();
    right.sort_unstable();
    assert!(!left.is_empty());
    assert_eq!(left, right);
}
