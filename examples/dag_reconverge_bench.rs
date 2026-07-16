//! PROBE (dag-frontier): the reconvergence fixture — data built so that
//! bucket merging (the DAG solver's raison d'être) has maximal purchase.
//!
//! Query (6 vars): `?e p1..p4 ?x1..?x4 . ?x_i t_i K_i . ?e s ?z . ?z tz Kz`
//! over 24 sub-populations, one per permutation σ of the four p-attributes.
//! An entity of pop σ carries `fans[k] ∈ {1,2,4,8}` values on attribute
//! `p_{σ(k)}`, exactly one of which has the marker edge — so after `?e`
//! binds, the row walks σ exactly (ascending fans) and the marker confirm
//! prunes the frontier back to one row per entity at every level. Routes
//! through the bound-set lattice are therefore **thin** (n rows each) and
//! **many** (24), reconverging pairwise at every depth and totally at
//! `{e, x1..x4}` — where the expensive shared variable `?z` (z_fan
//! candidates per row, marker-pruned to 1, always chosen last) is still
//! unbound. Merged buckets re-fatten the final batch 24×; the lineage
//! control (`solve_dag_unmerged`) and the recursive solvers keep 24 thin
//! batches.
//! In canonical column order, `x1 -> x2` reaches `{e,x1,x2}` by inserting
//! at column 2 while `x2 -> x1` reaches the same Ready state by inserting at
//! column 1. The four-variable convergence exercises insertion positions
//! 1 through 4, then measures shared work after a fixed five-column bound
//! prefix. `n_per_pop` controls cohort width and `z_fan` controls that work.
//!
//! Usage:
//!     cargo run --release --example dag_reconverge_bench -- \
//!         [n_per_pop=48] [z_fan=16] [reps=5]
//!     cargo run --release --features gpu --example dag_reconverge_bench -- \
//!         [n_per_pop=48] [z_fan=16] [reps=5]
//!     # Fat-shard GPU comparison (~1.77M tribles):
//!     cargo run --release --features gpu --example dag_reconverge_bench -- \
//!         2048 16 8
//!     # Repeat only the controlled GPU matrix after the archive is built:
//!     TRIBLES_WGPU_ONLY=1 cargo run --release --features gpu \
//!         --example dag_reconverge_bench -- 2048 16 8
//!     # Run only the exact ordinary oracle and latency ladder on both backends:
//!     TRIBLES_ORDINARY_ONLY=1 cargo run --release \
//!         --example dag_reconverge_bench -- 2048 16 8
//!
//! Runs sequential / ordinary parallel-scalar / explicit parallel-DAG /
//! explicit parallel residual-state /
//! blocked-v1 / grouped / dag / eager residual-state / lazy residual-state /
//! agglomerative / dag-unmerged on both backends
//! and prints per mode: min/median/max wall time, parity signature, and for the
//! frontier engines the group/batch structure, materialized rows, peak live
//! row-store cells, and the DAG's bucket/merge census.
//! Before those historical modes, an isolated ordinary-`Query` section checks
//! the complete sorted relation against a fixture-derived oracle and measures
//! construction, pull-to-first, geometric output prefixes, and full drain.
//! The source embeds `ENGINE_REVISION` when supplied at build time so binaries
//! transplanted across engine revisions remain attributable.
//! With `--features gpu`, an additional controlled comparison interleaves the
//! canonical CPU archive, the WGPU wrapper forced to its CPU rank path, forced
//! WGPU rank dispatch, and the default gated hybrid. Each case is equally
//! warmed and checked against the canonical archive by exact sorted output.
//! Parallel timings include a parallel signature fold, so compare the two
//! parallel schedulers directly; sequential/parallel ratios are end-to-end
//! query-plus-consumer throughput rather than isolated engine scaling.

use std::time::{Duration, Instant};

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::residual::ResidualStateStats;
use triblespace::core::query::{blocked_stats, dag_stats, TriblePattern};
use triblespace::core::trible::TribleSet;
#[cfg(feature = "gpu")]
use triblespace::gpu::{WgpuQueryStats, WgpuSuccinctArchive, DEFAULT_MIN_RANK_BATCH};
use triblespace::prelude::*;

const ENGINE_REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};

type QueryRow = (
    Inline<inlineencodings::GenId>,
    Inline<inlineencodings::GenId>,
    Inline<inlineencodings::GenId>,
    Inline<inlineencodings::GenId>,
    Inline<inlineencodings::GenId>,
    Inline<inlineencodings::GenId>,
);

mod world {
    use triblespace::prelude::*;

    attributes! {
        "3C3FCF6D97AE8EBF7C0927B5E317A4B8" as rp1: inlineencodings::GenId;
        "E0D70C1FB8E95BE40A6A02218DA7C8C0" as rp2: inlineencodings::GenId;
        "9398CD61E3D8A87B8C26B9647473F8E0" as rp3: inlineencodings::GenId;
        "A771D8F7C3BE63EB0EC6BA6682C2A412" as rp4: inlineencodings::GenId;
        "92C2F2C22151123A359A2F7F51F3519A" as rt1: inlineencodings::GenId;
        "357DC9D201D1A0FDC4569C740219F831" as rt2: inlineencodings::GenId;
        "8FB9F5E089C3212D899E8787DC1FA0AD" as rt3: inlineencodings::GenId;
        "10515585D7503F3EFCCCB994A3418577" as rt4: inlineencodings::GenId;
        "0EFC41641FCD73A30E2414AE78DEC219" as rs: inlineencodings::GenId;
        "BCB248E3850EA6ACF22E7B175B574E12" as rtz: inlineencodings::GenId;
    }
}

/// Keep the relation under test literally identical across the historical
/// scheduler matrix, the ordinary latency ladder, and the exact oracle gate.
macro_rules! reconvergence_query {
    ($kb:expr, $markers:expr) => {{
        let (k1, k2, k3, k4, kz) = $markers;
        find!(
            (e: Inline<_>, x1: Inline<_>, x2: Inline<_>, x3: Inline<_>, x4: Inline<_>, z: Inline<_>),
            pattern!($kb, [
                { ?e @ world::rp1: ?x1, world::rp2: ?x2, world::rp3: ?x3, world::rp4: ?x4, world::rs: ?z },
                { ?x1 @ world::rt1: k1 },
                { ?x2 @ world::rt2: k2 },
                { ?x3 @ world::rt3: k3 },
                { ?x4 @ world::rt4: k4 },
                { ?z @ world::rtz: kz }
            ])
        )
    }};
}

/// (count, order-independent multiset hash) — parity signature.
fn tally<T: std::hash::Hash>(items: impl IntoIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};
    let mut count = 0usize;
    let mut acc = 0u64;
    for item in items {
        let mut h = DefaultHasher::new();
        item.hash(&mut h);
        acc = acc.wrapping_add(h.finish());
        count += 1;
    }
    (count, acc)
}

/// Parallel equivalent of [`tally`]: the query and the signature are both
/// consumed through Rayon's fold/reduce path, so the parallel modes do not pay
/// for an intermediate `Vec` and a second scalar pass.
#[cfg(feature = "parallel")]
fn tally_par<T: std::hash::Hash + Send>(items: impl ParallelIterator<Item = T>) -> (usize, u64) {
    use std::hash::{DefaultHasher, Hasher};

    items
        .fold(
            || (0usize, 0u64),
            |(count, acc), item| {
                let mut h = DefaultHasher::new();
                item.hash(&mut h);
                (count + 1, acc.wrapping_add(h.finish()))
            },
        )
        .reduce(
            || (0usize, 0u64),
            |(left_count, left_acc), (right_count, right_acc)| {
                (left_count + right_count, left_acc.wrapping_add(right_acc))
            },
        )
}

/// Deterministic UFOID-shaped IDs keep the archive layout reproducible while
/// retaining a shared 32-bit locality prefix and pseudo-random suffixes.
struct FixtureIds {
    next: u64,
}

impl FixtureIds {
    fn new() -> Self {
        Self { next: 1 }
    }

    fn splitmix64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn mint(&mut self) -> ExclusiveId {
        let counter = self.next;
        self.next = self
            .next
            .checked_add(1)
            .expect("fixture ID space exhausted");

        let mut raw = [0u8; 16];
        raw[..4].copy_from_slice(&0xD46A_0001u32.to_be_bytes());
        raw[4..12].copy_from_slice(&Self::splitmix64(counter).to_be_bytes());
        raw[12..]
            .copy_from_slice(&Self::splitmix64(counter ^ 0xD1B5_4A32_D192_ED03).to_be_bytes()[..4]);
        ExclusiveId::force(Id::new(raw).expect("fixture prefix makes every ID non-nil"))
    }
}

fn build_world(n_per_pop: usize, z_fan: usize) -> (TribleSet, (Id, Id, Id, Id, Id), Vec<QueryRow>) {
    assert!(
        z_fan > 8,
        "z must be chosen after every x (fans go up to 8)"
    );
    let mut kb = TribleSet::new();
    let mut ids = FixtureIds::new();
    let markers: Vec<_> = (0..4).map(|_| ids.mint()).collect();
    let z_marker = ids.mint();
    let fans = [1usize, 2, 4, 8];
    let mut expected_rows = Vec::with_capacity(24 * n_per_pop);

    let mut perms: Vec<[usize; 4]> = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            if b == a {
                continue;
            }
            for c in 0..4 {
                if c == a || c == b {
                    continue;
                }
                let d = 6 - a - b - c;
                perms.push([a, b, c, d]);
            }
        }
    }
    assert_eq!(perms.len(), 24);

    for sigma in &perms {
        for _ in 0..n_per_pop {
            let e = ids.mint();
            let mut real_by_attribute = [None; 4];
            for (k, &attr_idx) in sigma.iter().enumerate() {
                let values: Vec<_> = (0..fans[k]).map(|_| ids.mint()).collect();
                for v in &values {
                    kb += match attr_idx {
                        0 => entity! { &e @ world::rp1: v },
                        1 => entity! { &e @ world::rp2: v },
                        2 => entity! { &e @ world::rp3: v },
                        _ => entity! { &e @ world::rp4: v },
                    };
                }
                let real = &values[0];
                real_by_attribute[attr_idx] = Some(real.id);
                let marker = &markers[attr_idx];
                kb += match attr_idx {
                    0 => entity! { real @ world::rt1: marker },
                    1 => entity! { real @ world::rt2: marker },
                    2 => entity! { real @ world::rt3: marker },
                    _ => entity! { real @ world::rt4: marker },
                };
            }
            let z_values: Vec<_> = (0..z_fan).map(|_| ids.mint()).collect();
            for v in &z_values {
                kb += entity! { &e @ world::rs: v };
            }
            kb += entity! { &z_values[0] @ world::rtz: &z_marker };
            expected_rows.push((
                e.id.to_inline(),
                real_by_attribute[0]
                    .expect("every entity has rp1")
                    .to_inline(),
                real_by_attribute[1]
                    .expect("every entity has rp2")
                    .to_inline(),
                real_by_attribute[2]
                    .expect("every entity has rp3")
                    .to_inline(),
                real_by_attribute[3]
                    .expect("every entity has rp4")
                    .to_inline(),
                z_values[0].id.to_inline(),
            ));
        }
    }
    expected_rows.sort_unstable();
    (
        kb,
        (
            *markers[0],
            *markers[1],
            *markers[2],
            *markers[3],
            *z_marker,
        ),
        expected_rows,
    )
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Seq,
    #[cfg(feature = "parallel")]
    ParScalar,
    #[cfg(feature = "parallel")]
    ParDag,
    #[cfg(feature = "parallel")]
    ParResidual,
    Blk,
    Grp,
    Dag,
    Residual,
    ResidualLazy,
    Agglomerative,
    DagU,
}

fn run_query<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id), mode: Mode) -> (usize, u64) {
    let q = reconvergence_query!(kb, markers);
    match mode {
        Mode::Seq => tally(q.sequential()),
        #[cfg(feature = "parallel")]
        Mode::ParScalar => tally_par(q.into_par_iter()),
        #[cfg(feature = "parallel")]
        Mode::ParDag => tally_par(q.into_par_dag_iter()),
        #[cfg(feature = "parallel")]
        Mode::ParResidual => tally_par(q.into_par_residual_state_iter()),
        Mode::Blk => tally(q.solve_blocked()),
        Mode::Grp => tally(q.solve_blocked_grouped()),
        Mode::Dag => tally(q.solve_dag()),
        Mode::Residual => tally(q.solve_residual_state()),
        Mode::ResidualLazy => tally(q.solve_residual_state_lazy()),
        Mode::Agglomerative => tally(
            q.solve_dag_lazy()
                .start_width(1)
                .growth(2)
                .agglomerative_partition(),
        ),
        Mode::DagU => tally(q.solve_dag_unmerged()),
    }
}

fn run_residual_profiled<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
) -> ((usize, u64), ResidualStateStats) {
    let solve = reconvergence_query!(kb, markers).solve_residual_state_profiled();
    (tally(solve.results), solve.stats)
}

fn run_lazy_residual_profiled<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
) -> ((usize, u64), ResidualStateStats) {
    let solve = reconvergence_query!(kb, markers)
        .solve_residual_state_lazy()
        .collect_profiled();
    (tally(solve.results), solve.stats)
}

fn timing_summary(v: &[f64]) -> (f64, f64, f64) {
    assert!(!v.is_empty(), "timing summary requires at least one sample");
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let middle = s.len() / 2;
    let median = if s.len().is_multiple_of(2) {
        (s[middle - 1] + s[middle]) / 2.0
    } else {
        s[middle]
    };
    (s[0], median, s[s.len() - 1])
}

fn ordinary_collect<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id)) -> Vec<QueryRow> {
    reconvergence_query!(kb, markers).collect()
}

fn check_ordinary_oracle<S: TriblePattern>(
    label: &str,
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    expected: &[QueryRow],
) {
    let mut actual = ordinary_collect(kb, markers);
    actual.sort_unstable();
    assert_eq!(actual, expected, "{label}: exact ordinary oracle mismatch");
    println!(
        "  ordinary exact oracle: {} sorted rows match",
        expected.len()
    );
}

fn ordinary_construct<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id)) {
    drop(std::hint::black_box(reconvergence_query!(kb, markers)));
}

/// Construct outside the timer so this isolates the iterator's first pull.
fn ordinary_pull<S: TriblePattern>(kb: &S, markers: (Id, Id, Id, Id, Id)) -> (Duration, bool) {
    let mut query = reconvergence_query!(kb, markers);
    let start = Instant::now();
    let found = std::hint::black_box(query.next()).is_some();
    (start.elapsed(), found)
}

/// End-to-end prefix: construction is deliberately included.
fn ordinary_prefix<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    limit: usize,
) -> (usize, u64) {
    tally(reconvergence_query!(kb, markers).take(limit))
}

fn bench_ordinary<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    expected: &[QueryRow],
    reps: usize,
) {
    let expected_rows = expected.len();
    assert!(expected_rows > 0, "ordinary benchmark needs one result");

    ordinary_construct(kb, markers);
    assert!(ordinary_pull(kb, markers).1);
    for limit in [1, 10, 100, usize::MAX] {
        std::hint::black_box(ordinary_prefix(kb, markers, limit));
    }

    let mut construct_samples = Vec::with_capacity(reps);
    let mut pull_samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        ordinary_construct(kb, markers);
        construct_samples.push(start.elapsed().as_secs_f64() * 1e6);

        let (elapsed, found) = ordinary_pull(kb, markers);
        assert!(found, "ordinary first result disappeared");
        pull_samples.push(elapsed.as_secs_f64() * 1e6);
    }

    let mut points: Vec<usize> = [1, 10, 100]
        .into_iter()
        .map(|point| point.min(expected_rows))
        .collect();
    points.sort_unstable();
    points.dedup();
    points.push(usize::MAX);

    let mut point_samples = vec![Vec::with_capacity(reps); points.len()];
    let mut signatures = vec![(0usize, 0u64); points.len()];
    for repetition in 0..reps {
        for offset in 0..points.len() {
            let point_index = (repetition + offset) % points.len();
            let start = Instant::now();
            signatures[point_index] =
                std::hint::black_box(ordinary_prefix(kb, markers, points[point_index]));
            point_samples[point_index].push(start.elapsed().as_secs_f64());
        }
    }

    let (construct_min, construct_median, construct_max) = timing_summary(&construct_samples);
    let (pull_min, pull_median, pull_max) = timing_summary(&pull_samples);
    println!("  ordinary Query latency ladder (canonical bound-prefix width 5):");
    println!(
        "    construct+drop  {:>10.3} us  [{construct_min:.3}..{construct_max:.3}]",
        construct_median,
    );
    println!(
        "    pull->first     {:>10.3} us  [{pull_min:.3}..{pull_max:.3}]",
        pull_median,
    );

    let expected_signature = tally(expected.iter());
    for (point_index, &point) in points.iter().enumerate() {
        let expected_at_point = if point == usize::MAX {
            expected_rows
        } else {
            point
        };
        assert_eq!(
            signatures[point_index].0, expected_at_point,
            "ordinary prefix count mismatch"
        );
        let (min, median, max) = timing_summary(&point_samples[point_index]);
        if point == usize::MAX {
            assert_eq!(
                signatures[point_index], expected_signature,
                "ordinary full-drain signature disagrees with exact oracle"
            );
            println!(
                "    full drain      {:>10.3} ms  [{:.3}..{:.3}]  {:>12.0} rows/s",
                median * 1e3,
                min * 1e3,
                max * 1e3,
                expected_rows as f64 / median,
            );
        } else {
            println!(
                "    e2e take {point:<3}   {:>10.3} us  [{:.3}..{:.3}]",
                median * 1e6,
                min * 1e6,
                max * 1e6,
            );
        }
    }
}

fn bench_backend<S: TriblePattern>(
    label: &str,
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    expected_rows: &[QueryRow],
    reps: usize,
    ordinary_only: bool,
) {
    check_ordinary_oracle(label, kb, markers, expected_rows);
    bench_ordinary(kb, markers, expected_rows, reps);
    if ordinary_only {
        return;
    }
    let expected = expected_rows.len();

    let mut modes = vec![("seq", Mode::Seq)];
    #[cfg(feature = "parallel")]
    modes.extend([
        ("par-scalar", Mode::ParScalar),
        ("par-dag", Mode::ParDag),
        ("par-residual", Mode::ParResidual),
    ]);
    modes.extend([
        ("blk", Mode::Blk),
        ("grp", Mode::Grp),
        ("dag", Mode::Dag),
        ("residual", Mode::Residual),
        ("res-lazy", Mode::ResidualLazy),
        ("agglomerative", Mode::Agglomerative),
        ("dagu", Mode::DagU),
    ]);
    for &(_, mode) in &modes {
        std::hint::black_box(run_query(kb, markers, mode));
    }

    let mut sigs = vec![(0, 0); modes.len()];
    let mut times = vec![Vec::with_capacity(reps); modes.len()];
    for repetition in 0..reps {
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let t = Instant::now();
            sigs[mode_index] = run_query(kb, markers, modes[mode_index].1);
            times[mode_index].push(t.elapsed().as_secs_f64() * 1e3);
        }
    }

    let mut meds = Vec::new();
    let mut ranges = Vec::new();
    for mode_times in &times {
        let (min, med, max) = timing_summary(mode_times);
        meds.push(med);
        ranges.push((min, max));
    }
    let parity = sigs.iter().all(|&s| s == sigs[0]) && sigs[0].0 == expected;
    println!(
        "{label:<24} rows {:>7}  {}",
        sigs[0].0,
        if parity { "ok" } else { "MISMATCH" }
    );
    assert!(parity, "{label} result signature mismatch");
    for (((name, _), median), (min, max)) in modes.iter().zip(&meds).zip(&ranges) {
        println!("  {name:<14} {median:>10.3} ms  [{min:.3}..{max:.3}]");
    }
    #[cfg(feature = "parallel")]
    {
        let par_scalar = meds[modes
            .iter()
            .position(|(name, _)| *name == "par-scalar")
            .unwrap()];
        let par_dag = meds[modes
            .iter()
            .position(|(name, _)| *name == "par-dag")
            .unwrap()];
        let par_residual = meds[modes
            .iter()
            .position(|(name, _)| *name == "par-residual")
            .unwrap()];
        println!(
            "  scheduler ratio  dag/scalar {:>7.3}x  residual/scalar {:>7.3}x",
            par_scalar / par_dag,
            par_scalar / par_residual,
        );
    }
    // Instrumented single passes: group/batch structure, intermediates,
    // peak cells; bucket/merge census for the dag modes.
    blocked_stats::set_enabled(true);
    dag_stats::set_enabled(true);
    for &(name, mode) in &modes[1..] {
        if mode == Mode::Residual {
            let (_, stats) = run_residual_profiled(kb, markers);
            println!(
                "  residual states: {} interned / {} hits / {} bucket merges ({} rows); calls propose {} (max {} rows), confirm {} (max {} rows)",
                stats.states_interned,
                stats.interner_hits,
                stats.bucket_merges,
                stats.rows_merged,
                stats.propose_calls,
                stats.max_propose_rows,
                stats.confirm_calls,
                stats.max_confirm_rows,
            );
            continue;
        }
        if mode == Mode::ResidualLazy {
            let (_, stats) = run_lazy_residual_profiled(kb, markers);
            println!(
                "  residual lazy states: {} interned / {} hits / {} reentries ({} rows); pops {} ({} full / {} readiness / {} partial); calls propose {} (max {} rows), confirm {} (max {} rows)",
                stats.states_interned,
                stats.interner_hits,
                stats.state_reentries,
                stats.rows_reentered,
                stats.state_pops,
                stats.full_pops,
                stats.readiness_pops,
                stats.partial_pops,
                stats.propose_calls,
                stats.max_propose_rows,
                stats.confirm_calls,
                stats.max_confirm_rows,
            );
            continue;
        }
        blocked_stats::reset();
        dag_stats::reset();
        run_query(kb, markers, mode);
        println!("  {name}: {}", blocked_stats::report());
        let is_dag = matches!(mode, Mode::Dag | Mode::Agglomerative | Mode::DagU);
        #[cfg(feature = "parallel")]
        let is_dag = is_dag || matches!(mode, Mode::ParDag);
        if is_dag {
            println!("  {name} buckets: {}", dag_stats::report());
        }
    }
    blocked_stats::set_enabled(false);
    dag_stats::set_enabled(false);
}

#[cfg(feature = "gpu")]
fn format_batch_range(stats: WgpuQueryStats) -> String {
    match (stats.min_gpu_batch, stats.max_gpu_batch) {
        (Some(min), Some(max)) => format!("{min}/{max}"),
        _ => "n/a".to_owned(),
    }
}

/// Collects a full result multiset for the global DAG, saturated serial
/// residual, and two affine Rayon scheduler shapes used by the controlled GPU
/// comparison. Callers sort the result before comparison, because Rayon
/// deliberately does not promise encounter order here.
#[cfg(feature = "gpu")]
fn collect_query<S: TriblePattern>(
    kb: &S,
    markers: (Id, Id, Id, Id, Id),
    mode: Mode,
) -> Vec<QueryRow> {
    let q = reconvergence_query!(kb, markers);
    match mode {
        Mode::Dag => q.solve_dag(),
        Mode::Residual => q.solve_residual_state(),
        Mode::ParDag => q.into_par_dag_iter().collect(),
        Mode::ParResidual => q.into_par_residual_state_iter().collect(),
        _ => unreachable!("controlled WGPU comparison only uses affine schedulers"),
    }
}

#[cfg(feature = "gpu")]
#[derive(Clone, Copy)]
struct RankCase {
    label: &'static str,
    threshold: Option<usize>,
}

#[cfg(feature = "gpu")]
const RANK_CASES: [RankCase; 4] = [
    RankCase {
        label: "canonical-cpu",
        threshold: None,
    },
    RankCase {
        label: "wrapper-cpu-control",
        threshold: Some(usize::MAX),
    },
    RankCase {
        label: "forced-wgpu-rank",
        threshold: Some(1),
    },
    RankCase {
        label: "gated-hybrid",
        threshold: Some(DEFAULT_MIN_RANK_BATCH),
    },
];

/// A four-treatment balanced Latin square: across each four-repetition block,
/// every case occupies every ordinal position and every ordered adjacent pair
/// occurs once. Later blocks repeat the design deterministically.
#[cfg(feature = "gpu")]
const BALANCED_CASE_ORDERS: [[usize; 4]; 4] =
    [[0, 1, 3, 2], [1, 2, 0, 3], [2, 3, 1, 0], [3, 0, 2, 1]];

#[cfg(feature = "gpu")]
struct RunMeasurement {
    elapsed_ms: f64,
    signature: (usize, u64),
    stats: Option<WgpuQueryStats>,
}

#[cfg(feature = "gpu")]
fn configure_rank_case(case: RankCase, gpu_archive: &mut WgpuSuccinctArchive<OrderedUniverse>) {
    if let Some(threshold) = case.threshold {
        gpu_archive.set_min_rank_batch(threshold);
        gpu_archive.reset_stats();
    }
}

#[cfg(feature = "gpu")]
fn run_rank_case(
    case: RankCase,
    archive: &SuccinctArchive<OrderedUniverse>,
    gpu_archive: &WgpuSuccinctArchive<OrderedUniverse>,
    markers: (Id, Id, Id, Id, Id),
    mode: Mode,
) -> (usize, u64) {
    if case.threshold.is_some() {
        run_query(gpu_archive, markers, mode)
    } else {
        run_query(archive, markers, mode)
    }
}

#[cfg(feature = "gpu")]
fn collect_rank_case(
    case: RankCase,
    archive: &SuccinctArchive<OrderedUniverse>,
    gpu_archive: &WgpuSuccinctArchive<OrderedUniverse>,
    markers: (Id, Id, Id, Id, Id),
    mode: Mode,
) -> Vec<QueryRow> {
    let mut rows = if case.threshold.is_some() {
        collect_query(gpu_archive, markers, mode)
    } else {
        collect_query(archive, markers, mode)
    };
    rows.sort_unstable();
    rows
}

#[cfg(feature = "gpu")]
fn rank_executor(stats: Option<WgpuQueryStats>) -> &'static str {
    match stats {
        None => "rank=CPU (canonical)",
        Some(stats) => match (stats.gpu_dispatches != 0, stats.cpu_fallback_batches != 0) {
            (true, false) => "rank=GPU",
            (true, true) => "rank=mixed",
            (false, true) => "rank=CPU",
            (false, false) => "rank=none",
        },
    }
}

#[cfg(feature = "gpu")]
fn case_threshold(case: RankCase) -> String {
    match case.threshold {
        None => "canonical".to_owned(),
        Some(usize::MAX) => "usize::MAX (CPU control)".to_owned(),
        Some(threshold) => threshold.to_string(),
    }
}

/// Measures the global DAG, saturated serial residual, and Rayon-sharded
/// DAG/residual schedulers with a controlled, interleaved rank-executor
/// comparison.
///
/// Construction only measures host preparation and device enqueue. A separate
/// first forced query accounts for any deferred synchronization and pipeline
/// setup, outside the timed repetitions. Every policy then receives one exact
/// collection pass plus one tally warm-up before the rotated timing rounds.
#[cfg(feature = "gpu")]
fn bench_wgpu_backend(
    archive: &SuccinctArchive<OrderedUniverse>,
    markers: (Id, Id, Id, Id, Id),
    expected: usize,
    reps: usize,
) {
    let clone_started = Instant::now();
    let device_source = archive.clone();
    let clone_elapsed = clone_started.elapsed();
    let enqueue_started = Instant::now();
    let mut gpu_archive = WgpuSuccinctArchive::new(device_source)
        .expect("failed to prepare SuccinctArchive ring columns for WGPU");
    let enqueue_elapsed = enqueue_started.elapsed();
    eprintln!(
        "WGPU host preparation: archive clone {clone_elapsed:?}; adapter construction/device enqueue {enqueue_elapsed:?}",
    );

    let canonical_setup_signature = run_query(archive, markers, Mode::Dag);
    gpu_archive.set_min_rank_batch(1);
    gpu_archive.reset_stats();
    let ready_started = Instant::now();
    let ready_signature = run_query(&gpu_archive, markers, Mode::Dag);
    let ready_elapsed = ready_started.elapsed();
    let ready_stats = gpu_archive.stats();
    assert_eq!(
        ready_signature, canonical_setup_signature,
        "first WGPU setup query differed from the canonical CPU archive"
    );
    eprintln!(
        "first forced-WGPU DAG query/setup-to-ready: {ready_elapsed:?} (outside timed reps; includes deferred synchronization/pipeline setup; {} dispatches, {} probes)",
        ready_stats.gpu_dispatches,
        ready_stats.gpu_probes,
    );

    let modes = [
        ("dag", Mode::Dag),
        ("residual", Mode::Residual),
        ("par-dag", Mode::ParDag),
        ("par-residual", Mode::ParResidual),
    ];
    println!(
        "\n== Controlled SuccinctArchive rank executors ({} Rayon threads) ==",
        rayon::current_num_threads(),
    );
    println!(
        "   four cases are interleaved in a rotating order; min/median/max cover {reps} timed runs per case"
    );

    for (mode_index, &(mode_name, mode)) in modes.iter().enumerate() {
        // Exact output parity is deliberately untimed and stronger than the
        // compact signature used to guard every timed repetition.
        configure_rank_case(RANK_CASES[0], &mut gpu_archive);
        let canonical_rows = collect_rank_case(RANK_CASES[0], archive, &gpu_archive, markers, mode);
        assert_eq!(
            canonical_rows.len(),
            expected,
            "canonical {mode_name} output count"
        );
        let canonical_signature = tally(canonical_rows.iter());
        for &case in &RANK_CASES[1..] {
            configure_rank_case(case, &mut gpu_archive);
            let rows = collect_rank_case(case, archive, &gpu_archive, markers, mode);
            assert_eq!(
                rows, canonical_rows,
                "{} {mode_name} exact output differs from canonical CPU",
                case.label,
            );
        }

        // One additional, equally counted warm-up per policy and scheduler.
        // Use a different balanced order row for each scheduler mode.
        for case_index in BALANCED_CASE_ORDERS[mode_index] {
            let case = RANK_CASES[case_index];
            configure_rank_case(case, &mut gpu_archive);
            let warm_signature = run_rank_case(case, archive, &gpu_archive, markers, mode);
            assert_eq!(
                warm_signature, canonical_signature,
                "{} {mode_name} warm-up signature",
                case.label,
            );
        }

        let mut measurements: [Vec<RunMeasurement>; 4] =
            std::array::from_fn(|_| Vec::with_capacity(reps));

        // The balanced order keeps all four observations adjacent enough to
        // share a thermal/cache regime without giving any case a fixed ordinal
        // position or predecessor over a complete four-repetition block.
        for repetition in 0..reps {
            let order = BALANCED_CASE_ORDERS[(repetition + mode_index) % RANK_CASES.len()];
            for case_index in order {
                let case = RANK_CASES[case_index];
                configure_rank_case(case, &mut gpu_archive);

                let started = Instant::now();
                let signature = run_rank_case(case, archive, &gpu_archive, markers, mode);
                let elapsed_ms = started.elapsed().as_secs_f64() * 1e3;
                let stats = case.threshold.map(|_| gpu_archive.stats());
                assert_eq!(
                    signature,
                    canonical_signature,
                    "{} {mode_name} timed repetition {} signature",
                    case.label,
                    repetition + 1,
                );
                measurements[case_index].push(RunMeasurement {
                    elapsed_ms,
                    signature,
                    stats,
                });
            }
        }

        println!("\n  scheduler: {mode_name} (exact sorted output parity: ok)");
        for (case_index, case) in RANK_CASES.iter().copied().enumerate() {
            let runs = &measurements[case_index];
            let times: Vec<_> = runs.iter().map(|run| run.elapsed_ms).collect();
            let (min, median, max) = timing_summary(&times);
            let first_executor = rank_executor(runs[0].stats);
            let stable_executor = runs
                .iter()
                .all(|run| rank_executor(run.stats) == first_executor);
            println!(
                "    {:<20} min/median/max {:>9.3}/{:>9.3}/{:>9.3} ms  {}  threshold {}",
                case.label,
                min,
                median,
                max,
                if stable_executor {
                    first_executor
                } else {
                    "rank=varies (see runs)"
                },
                case_threshold(case),
            );
            for (repetition, run) in runs.iter().enumerate() {
                match run.stats {
                    None => println!(
                        "      run {:>2}: {:>9.3} ms  {}  rows {}",
                        repetition + 1,
                        run.elapsed_ms,
                        rank_executor(None),
                        run.signature.0,
                    ),
                    Some(stats) => println!(
                        "      run {:>2}: {:>9.3} ms  {}  rows {}  GPU dispatches/probes {}/{}; CPU batches/probes {}/{}; GPU batch min/max {}",
                        repetition + 1,
                        run.elapsed_ms,
                        rank_executor(Some(stats)),
                        run.signature.0,
                        stats.gpu_dispatches,
                        stats.gpu_probes,
                        stats.cpu_fallback_batches,
                        stats.cpu_fallback_probes,
                        format_batch_range(stats),
                    ),
                }
            }
        }
    }
}

fn main() {
    let n_per_pop: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(48);
    let z_fan: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let reps: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    assert!(reps > 0, "reps must be at least one");
    let ordinary_only = std::env::var("TRIBLES_ORDINARY_ONLY").as_deref() == Ok("1");
    #[cfg(feature = "gpu")]
    let controlled_gpu_only = std::env::var_os("TRIBLES_WGPU_ONLY").is_some() && !ordinary_only;
    #[cfg(not(feature = "gpu"))]
    let controlled_gpu_only = false;

    eprintln!(
        "revision: {ENGINE_REVISION}; block row cap: {}",
        triblespace::core::query::block_row_cap()
    );
    if ordinary_only {
        println!("ordinary-only mode: enabled");
    }
    #[cfg(feature = "parallel")]
    eprintln!("Rayon worker threads: {}", rayon::current_num_threads());

    let t0 = Instant::now();
    let (kb, markers, expected_rows) = build_world(n_per_pop, z_fan);
    let expected = expected_rows.len();
    eprintln!(
        "reconvergence world: {} entities (24 pops x {n_per_pop}), z_fan {z_fan}, {} tribles, built in {:?}",
        expected,
        kb.len(),
        t0.elapsed()
    );

    if !controlled_gpu_only {
        println!("\n== TribleSet backend (default blocked delegation) ==");
        bench_backend(
            "reconverge 24-route",
            &kb,
            markers,
            &expected_rows,
            reps,
            ordinary_only,
        );
    }

    let t0 = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&kb).into();
    eprintln!("\narchive built in {:?}", t0.elapsed());

    if !controlled_gpu_only {
        println!("\n== SuccinctArchive backend (batched blocked overrides) ==");
        bench_backend(
            "reconverge 24-route",
            &archive,
            markers,
            &expected_rows,
            reps,
            ordinary_only,
        );
    }

    #[cfg(feature = "gpu")]
    {
        if !ordinary_only {
            bench_wgpu_backend(&archive, markers, expected, reps);
        }
    }
}
