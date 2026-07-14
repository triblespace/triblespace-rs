//! PROBE (residual intersection): row-local proposer flips followed by
//! canonical confirmation-state reconvergence.
//!
//! Query: `?p @ a: ?x, b: ?x, c: ?x`.  At even person indices `a` is
//! selective and `b` is wide; at odd indices those fanouts are mirrored.
//! `c` is wider still.  Every person has exactly one value common to all
//! three attributes.  Since the global `?p` cardinality is smaller than any
//! `?x` cardinality, `?p` binds first; the next joint action then splits by
//! population before both histories reconverge at checked `{a, b}`.
//!
//! Usage:
//!     cargo run --release --example residual_intersection_bench -- \
//!         [n_per_parity=512] [fan=32] [c_fan=96] [reps=5]

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::residual::ResidualStateStats;
use triblespace::core::query::TriblePattern;
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

mod world {
    use triblespace::prelude::*;

    // Reuse the synthetic p/q/r IDs from blocked_group_skew_bench.
    attributes! {
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as a: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as b: inlineencodings::GenId;
        "27791B9EFCFADF397CFDBCDEE0B1FB22" as c: inlineencodings::GenId;
    }
}

/// Deterministic UFOID-shaped IDs make both archive layout and signatures
/// reproducible across runs.
struct FixtureIds(u64);

impl FixtureIds {
    fn splitmix64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn mint(&mut self) -> ExclusiveId {
        let counter = self.0;
        self.0 = self.0.checked_add(1).expect("fixture ID space exhausted");
        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46A_0002u32.to_be_bytes());
        raw[4..12].copy_from_slice(&Self::splitmix64(counter).to_be_bytes());
        raw[12..]
            .copy_from_slice(&Self::splitmix64(counter ^ 0xD1B5_4A32_D192_ED03).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("fixture prefix makes every ID non-nil"))
    }
}

fn add_values(
    kb: &mut TribleSet,
    ids: &mut FixtureIds,
    person: &ExclusiveId,
    good: &ExclusiveId,
    count: usize,
    attr: usize,
) {
    for index in 0..count {
        let junk = (index != 0).then(|| ids.mint());
        let value = junk.as_ref().unwrap_or(good);
        *kb += match attr {
            0 => entity! { person @ world::a: value },
            1 => entity! { person @ world::b: value },
            2 => entity! { person @ world::c: value },
            _ => unreachable!(),
        };
    }
}

fn add_person(kb: &mut TribleSet, ids: &mut FixtureIds, a_fan: usize, b_fan: usize, c_fan: usize) {
    let person = ids.mint();
    let good = ids.mint();
    add_values(kb, ids, &person, &good, a_fan, 0);
    add_values(kb, ids, &person, &good, b_fan, 1);
    add_values(kb, ids, &person, &good, c_fan, 2);
}

fn build_world(n_per_parity: usize, fan: usize, c_fan: usize) -> (TribleSet, usize) {
    assert!(n_per_parity > 0, "n_per_parity must be nonzero");
    assert!(fan > 1, "fan must exceed one so proposer choice flips");
    assert!(c_fan > fan, "c_fan must exceed fan so c remains wider");

    let mut kb = TribleSet::new();
    let mut ids = FixtureIds(1);
    for person_index in 0..2 * n_per_parity {
        if person_index % 2 == 0 {
            add_person(&mut kb, &mut ids, 1, fan, c_fan);
        } else {
            add_person(&mut kb, &mut ids, fan, 1, c_fan);
        }
    }
    (kb, 2 * n_per_parity)
}

fn tally<T: std::hash::Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};

    let mut count = 0usize;
    let mut hash = 0u64;
    for item in items {
        let mut item_hash = DefaultHasher::new();
        item.hash(&mut item_hash);
        hash = hash.wrapping_add(item_hash.finish());
        count += 1;
    }
    (count, hash)
}

#[derive(Clone, Copy)]
enum Mode {
    Sequential,
    Dag,
    Residual,
    ResidualLazy,
}

fn run_query<S: TriblePattern>(kb: &S, mode: Mode) -> (usize, u64) {
    let query = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    );
    match mode {
        Mode::Sequential => tally(query.sequential()),
        Mode::Dag => tally(query.solve_dag()),
        Mode::Residual => tally(query.solve_residual_state()),
        Mode::ResidualLazy => tally(query.solve_residual_state_lazy()),
    }
}

fn run_residual_profiled<S: TriblePattern>(kb: &S) -> ((usize, u64), ResidualStateStats) {
    let solve = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_profiled();
    (tally(solve.results), solve.stats)
}

fn run_lazy_residual_profiled<S: TriblePattern>(kb: &S) -> ((usize, u64), ResidualStateStats) {
    let solve = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy()
    .collect_profiled();
    (tally(solve.results), solve.stats)
}

fn run_nested_query<S: TriblePattern>(kb: &S, mode: Mode) -> (usize, u64) {
    let query = find!(
        (p: Inline<_>, x: Inline<_>),
        and!(
            pattern!(kb, [{ ?p @ world::a: ?x }]),
            and!(
                pattern!(kb, [{ ?p @ world::b: ?x }]),
                pattern!(kb, [{ ?p @ world::c: ?x }]),
            ),
        )
    );
    match mode {
        Mode::Sequential => tally(query.sequential()),
        Mode::Dag => tally(query.solve_dag()),
        Mode::Residual => tally(query.solve_residual_state()),
        Mode::ResidualLazy => tally(query.solve_residual_state_lazy()),
    }
}

fn run_nested_residual_profiled<S: TriblePattern>(kb: &S) -> ((usize, u64), ResidualStateStats) {
    let solve = find!(
        (p: Inline<_>, x: Inline<_>),
        and!(
            pattern!(kb, [{ ?p @ world::a: ?x }]),
            and!(
                pattern!(kb, [{ ?p @ world::b: ?x }]),
                pattern!(kb, [{ ?p @ world::c: ?x }]),
            ),
        )
    )
    .solve_residual_state_profiled();
    (tally(solve.results), solve.stats)
}

#[derive(Clone, Copy)]
enum FirstMode {
    Sequential,
    DagLazy,
    ResidualEager,
    ResidualLazy,
}

fn run_first<S: TriblePattern>(kb: &S, mode: FirstMode) -> (usize, u64) {
    let query = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    );
    match mode {
        FirstMode::Sequential => tally(query.sequential().take(1)),
        FirstMode::DagLazy => tally(query.solve_dag_lazy().take(1)),
        FirstMode::ResidualEager => tally(query.solve_residual_state().into_iter().take(1)),
        FirstMode::ResidualLazy => tally(query.solve_residual_state_lazy().take(1)),
    }
}

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn bench_first_result<S: TriblePattern>(label: &str, kb: &S, reps: usize) {
    let modes = [
        ("seq", FirstMode::Sequential),
        ("dag-lazy", FirstMode::DagLazy),
        ("res-eager", FirstMode::ResidualEager),
        ("res-lazy", FirstMode::ResidualLazy),
    ];
    for &(_, mode) in &modes {
        std::hint::black_box(run_first(kb, mode));
    }

    let mut samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut signatures = vec![(0, 0); modes.len()];
    for repetition in 0..reps {
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let start = Instant::now();
            signatures[mode_index] = run_first(kb, modes[mode_index].1);
            samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    println!("  first result ({label}):");
    for (mode_index, &(name, _)) in modes.iter().enumerate() {
        // Search order is intentionally scheduler-dependent, so only the
        // existence of one prefix row is shared here. Full-drain signatures
        // below remain the exact semantic parity gate.
        assert_eq!(
            signatures[mode_index].0, 1,
            "{label} {name} must produce one first result"
        );
        println!("    {name:<11} {:>9.3} ms", median(&samples[mode_index]));
    }
}

fn bench_backend<S: TriblePattern>(label: &str, kb: &S, expected: usize, reps: usize) {
    let modes = [
        ("seq", Mode::Sequential),
        ("dag", Mode::Dag),
        ("residual", Mode::Residual),
        ("res-lazy", Mode::ResidualLazy),
    ];

    // Untimed full drains pay any one-time setup before the measurements.
    for &(_, mode) in &modes {
        std::hint::black_box(run_query(kb, mode));
    }

    println!("\n== {label} ==");
    bench_first_result(label, kb, reps);
    println!("  full drain:");
    let mut samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut signatures = vec![(0, 0); modes.len()];
    for repetition in 0..reps {
        // Rotate the first mode so thermal/frequency drift is not assigned to
        // one solver merely because the benchmark ran it first every time.
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let mode = modes[mode_index].1;
            let start = Instant::now();
            signatures[mode_index] = run_query(kb, mode);
            samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    let reference = signatures[0];
    for (mode_index, &(name, _)) in modes.iter().enumerate() {
        let signature = signatures[mode_index];
        let parity = signature == reference && signature.0 == expected;
        println!(
            "  {name:<9} {:>9.3} ms  signature ({:>7}, {:#018x})  {}",
            median(&samples[mode_index]),
            signature.0,
            signature.1,
            if parity { "ok" } else { "MISMATCH" },
        );
        assert!(parity, "{label} {name} result signature mismatch");
    }

    let (signature, stats) = run_residual_profiled(kb);
    assert_eq!(
        signature.0, expected,
        "profiled residual row-count mismatch"
    );
    assert_eq!(
        signature, reference,
        "profiled residual result signature mismatch"
    );
    println!(
        "  profile: states {} + hits {}, pops {}, bucket merges {} ({} rows); \
         propose {} calls/{} rows/max {}, confirm {} calls/{} rows/max {}",
        stats.states_interned,
        stats.interner_hits,
        stats.state_pops,
        stats.bucket_merges,
        stats.rows_merged,
        stats.propose_calls,
        stats.propose_rows,
        stats.max_propose_rows,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.max_confirm_rows,
    );

    let (lazy_signature, lazy_stats) = run_lazy_residual_profiled(kb);
    assert_eq!(
        lazy_signature, reference,
        "profiled lazy residual result signature mismatch"
    );
    println!(
        "  lazy profile: states {} + hits {}, pops {} (sprint {} / harvest {} / partial {}), \
         live merges {} ({} rows), reentries {} ({} rows); propose {} calls/{} rows/max {}, \
         confirm {} calls/{} rows/max {}",
        lazy_stats.states_interned,
        lazy_stats.interner_hits,
        lazy_stats.state_pops,
        lazy_stats.sprint_pops,
        lazy_stats.harvest_pops,
        lazy_stats.partial_pops,
        lazy_stats.bucket_merges,
        lazy_stats.rows_merged,
        lazy_stats.state_reentries,
        lazy_stats.rows_reentered,
        lazy_stats.propose_calls,
        lazy_stats.propose_rows,
        lazy_stats.max_propose_rows,
        lazy_stats.confirm_calls,
        lazy_stats.confirm_rows,
        lazy_stats.max_confirm_rows,
    );
}

fn bench_nested_backend<S: TriblePattern>(label: &str, kb: &S, expected: usize, reps: usize) {
    let modes = [
        ("seq", Mode::Sequential),
        ("dag", Mode::Dag),
        ("residual", Mode::Residual),
        ("res-lazy", Mode::ResidualLazy),
    ];
    for &(_, mode) in &modes {
        std::hint::black_box(run_nested_query(kb, mode));
    }

    println!("\n== {label}: explicit nested AND ==");
    let mut samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut signatures = vec![(0, 0); modes.len()];
    for repetition in 0..reps {
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let start = Instant::now();
            signatures[mode_index] = run_nested_query(kb, modes[mode_index].1);
            samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    let reference = signatures[0];
    for (mode_index, &(name, _)) in modes.iter().enumerate() {
        let signature = signatures[mode_index];
        let parity = signature == reference && signature.0 == expected;
        println!(
            "  {name:<9} {:>9.3} ms  signature ({:>7}, {:#018x})  {}",
            median(&samples[mode_index]),
            signature.0,
            signature.1,
            if parity { "ok" } else { "MISMATCH" },
        );
        assert!(parity, "{label} nested {name} result signature mismatch");
    }

    let (signature, stats) = run_nested_residual_profiled(kb);
    assert_eq!(signature, reference, "profiled nested residual mismatch");
    println!(
        "  nested profile: states {} + hits {}, pops {}, bucket merges {} ({} rows); \
         propose {} calls/{} rows/max {}, confirm {} calls/{} rows/max {}",
        stats.states_interned,
        stats.interner_hits,
        stats.state_pops,
        stats.bucket_merges,
        stats.rows_merged,
        stats.propose_calls,
        stats.propose_rows,
        stats.max_propose_rows,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.max_confirm_rows,
    );
}

fn parse_arg(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n_per_parity = parse_arg(1, 512);
    let fan = parse_arg(2, 32);
    let c_fan = parse_arg(3, 96);
    let reps = parse_arg(4, 5);
    assert!(reps > 0, "reps must be nonzero");

    let start = Instant::now();
    let (kb, expected) = build_world(n_per_parity, fan, c_fan);
    eprintln!(
        "world: {expected} people ({n_per_parity}/parity), fan {fan}, c_fan {c_fan}, \
         {} tribles, built in {:?}",
        kb.len(),
        start.elapsed(),
    );

    bench_backend("TribleSet", &kb, expected, reps);
    bench_nested_backend("TribleSet", &kb, expected, reps);

    let start = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    eprintln!("archive built in {:?}", start.elapsed());
    bench_backend("SuccinctArchive", &archive, expected, reps);
    bench_nested_backend("SuccinctArchive", &archive, expected, reps);
}
