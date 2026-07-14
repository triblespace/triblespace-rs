//! PROBE: candidate-granular residual confirmation plus exact physical
//! continuation sprinting on one high-fanout parent.
//!
//! Usage:
//!     cargo run --release --example residual_candidate_pages_bench -- [fanout=16384] [reps=21]

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::residual::ResidualStateStats;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, TriblePattern,
    VariableContext, VariableId, VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::*;

#[derive(Clone)]
struct Atomic<C>(C);

impl<'a, C: Constraint<'a>> Constraint<'a> for Atomic<C> {
    fn variables(&self) -> VariableSet {
        self.0.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.0.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.0.influence(variable)
    }
}

#[derive(Clone, Copy)]
struct Fixture {
    person: Id,
    attrs: [Id; 3],
}

fn id(domain: u32, index: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&domain.to_be_bytes());
    raw[4..12].copy_from_slice(&(index as u64).to_be_bytes());
    raw[12..].copy_from_slice(&(index as u32).rotate_left(13).to_be_bytes());
    Id::new(raw).expect("fixture IDs are non-nil")
}

fn insert(set: &mut TribleSet, person: &Id, attr: &Id, value: Id) {
    set.insert(&Trible::force(
        person,
        attr,
        &inlineencodings::GenId::inline_from(value),
    ));
}

fn build(fanout: usize, common: Option<usize>) -> (TribleSet, Fixture) {
    let person = id(1, 0);
    let attrs = [id(2, 0), id(2, 1), id(2, 2)];
    let mut set = TribleSet::new();
    for index in 0..fanout {
        insert(&mut set, &person, &attrs[0], id(3, index));
    }
    if let Some(index) = common {
        insert(&mut set, &person, &attrs[1], id(3, index));
        insert(&mut set, &person, &attrs[2], id(3, index));
    }
    for index in 0..fanout {
        insert(&mut set, &person, &attrs[1], id(4, index));
        insert(&mut set, &person, &attrs[2], id(5, index));
    }
    // Keep A uniquely tight while making the two confirmers progressively
    // wider. These values cannot overlap A or each other.
    insert(&mut set, &person, &attrs[1], id(4, fanout));
    insert(&mut set, &person, &attrs[2], id(5, fanout));
    insert(&mut set, &person, &attrs[2], id(5, fanout + 1));
    (set, Fixture { person, attrs })
}

fn project(binding: &Binding, p: VariableId, x: VariableId) -> Option<(RawInline, RawInline)> {
    Some((*binding.get(p)?, *binding.get(x)?))
}

#[derive(Clone, Copy)]
enum Mode {
    Sequential,
    Dag,
    ResidualAtomic,
    ResidualPaged,
}

fn run<S: TriblePattern>(store: &S, fixture: Fixture, mode: Mode, first: bool) -> (usize, u64) {
    let mut context = VariableContext::new();
    let p = context.next_variable::<inlineencodings::GenId>();
    let x = context.next_variable::<inlineencodings::GenId>();
    let attrs = fixture.attrs.map(inlineencodings::GenId::inline_from);

    let tally = |items: &mut dyn Iterator<Item = (RawInline, RawInline)>| {
        let mut count = 0usize;
        let mut hash = 0u64;
        for item in items {
            let mut hasher = DefaultHasher::new();
            item.hash(&mut hasher);
            hash = hash.wrapping_add(hasher.finish());
            count += 1;
        }
        (count, hash)
    };

    match mode {
        Mode::ResidualAtomic => {
            let root = IntersectionConstraint::new(vec![
                Atomic(store.pattern(p, attrs[0], x)),
                Atomic(store.pattern(p, attrs[1], x)),
                Atomic(store.pattern(p, attrs[2], x)),
            ]);
            let mut iter = Query::new(root, move |binding| project(binding, p.index, x.index))
                .solve_residual_state_lazy();
            if first {
                tally(&mut iter.take(1))
            } else {
                tally(&mut iter)
            }
        }
        Mode::Sequential | Mode::Dag | Mode::ResidualPaged => {
            let root = IntersectionConstraint::new(vec![
                store.pattern(p, attrs[0], x),
                store.pattern(p, attrs[1], x),
                store.pattern(p, attrs[2], x),
            ]);
            let query = Query::new(root, move |binding| project(binding, p.index, x.index));
            match mode {
                Mode::Sequential => {
                    let mut iter = query.sequential();
                    if first {
                        tally(&mut iter.take(1))
                    } else {
                        tally(&mut iter)
                    }
                }
                Mode::Dag => {
                    if first {
                        tally(&mut query.solve_dag_lazy().take(1))
                    } else {
                        tally(&mut query.solve_dag().into_iter())
                    }
                }
                Mode::ResidualPaged => {
                    let mut iter = query.solve_residual_state_lazy();
                    if first {
                        tally(&mut iter.take(1))
                    } else {
                        tally(&mut iter)
                    }
                }
                Mode::ResidualAtomic => unreachable!(),
            }
        }
    }
}

fn paged_first_profile<S: TriblePattern>(
    store: &S,
    fixture: Fixture,
) -> ((usize, u64), ResidualStateStats) {
    let mut context = VariableContext::new();
    let p = context.next_variable::<inlineencodings::GenId>();
    let x = context.next_variable::<inlineencodings::GenId>();
    let attrs = fixture.attrs.map(inlineencodings::GenId::inline_from);
    let root = IntersectionConstraint::new(vec![
        store.pattern(p, attrs[0], x),
        store.pattern(p, attrs[1], x),
        store.pattern(p, attrs[2], x),
    ]);
    let mut iter = Query::new(root, move |binding| project(binding, p.index, x.index))
        .solve_residual_state_lazy();
    let result = iter.next();
    let signature = result.map_or((0, 0), |item| {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        (1, hasher.finish())
    });
    (signature, iter.stats().clone())
}

fn percentile(samples: &[f64], percentile: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index]
}

fn bench_backend<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: Fixture,
    expected: usize,
    reps: usize,
) {
    let modes = [
        ("seq", Mode::Sequential),
        ("dag", Mode::Dag),
        ("res-atomic", Mode::ResidualAtomic),
        ("res-paged", Mode::ResidualPaged),
    ];
    for &(_, mode) in &modes {
        std::hint::black_box(run(store, fixture, mode, true));
        std::hint::black_box(run(store, fixture, mode, false));
    }

    let mut first_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut full_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut first_signatures = vec![(0, 0); modes.len()];
    let mut full_signatures = vec![(0, 0); modes.len()];
    for repetition in 0..reps {
        for offset in 0..modes.len() {
            let index = (repetition + offset) % modes.len();
            let start = Instant::now();
            first_signatures[index] = run(store, fixture, modes[index].1, true);
            first_samples[index].push(start.elapsed().as_secs_f64() * 1e6);

            let start = Instant::now();
            full_signatures[index] = run(store, fixture, modes[index].1, false);
            full_samples[index].push(start.elapsed().as_secs_f64() * 1e6);
        }
    }

    let full_reference = full_signatures[0];
    assert_eq!(
        full_reference.0, expected,
        "{backend}: unexpected result count"
    );
    println!("  {backend}:");
    for (index, &(name, _)) in modes.iter().enumerate() {
        assert_eq!(
            first_signatures[index].0,
            expected.min(1),
            "{backend} {name}: first-result existence mismatch"
        );
        assert_eq!(
            full_signatures[index], full_reference,
            "{backend} {name}: full-drain bag mismatch"
        );
        println!(
            "    {name:<10} first med/p95 {:>10.1}/{:>10.1} us   full med {:>10.1} us",
            percentile(&first_samples[index], 0.5),
            percentile(&first_samples[index], 0.95),
            percentile(&full_samples[index], 0.5),
        );
    }
    let (_, stats) = paged_first_profile(store, fixture);
    println!(
        "    paged first profile: propose {} calls, {} candidates (max {}); \
         confirm {} calls, {} candidates (max page {}); pops {} continuation {} \
         (underfilled {}) partial {}, width grows {}",
        stats.propose_calls,
        stats.candidates_proposed,
        stats.max_propose_candidates,
        stats.confirm_calls,
        stats.candidates_confirmed,
        stats.max_confirm_candidates,
        stats.state_pops,
        stats.continuation_pops,
        stats.underfilled_continuation_pops,
        stats.partial_pops,
        stats.width_increases,
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let fanout = args
        .get(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(16_384);
    let reps = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(21);
    assert!(fanout > 1);
    assert!(reps > 0);

    for (label, common) in [
        ("first-page", Some(fanout - 1)),
        ("second-page", Some(fanout - 2)),
        ("midpoint", Some(fanout / 2)),
        ("late", Some(0)),
        ("absent", None),
    ] {
        let (set, fixture) = build(fanout, common);
        assert_eq!(fixture.person, id(1, 0));
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        println!("\n== {label}, fanout {fanout}, reps {reps} ==");
        bench_backend(
            "TribleSet",
            &set,
            fixture,
            usize::from(common.is_some()),
            reps,
        );
        bench_backend(
            "SuccinctArchive",
            &archive,
            fixture,
            usize::from(common.is_some()),
            reps,
        );
    }
}
