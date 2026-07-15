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
//! The coalescing ablation keeps canonical `StateDesc` interning in both arms.
//! Its isolated arm merely gives every nonempty filing a separate physical
//! bucket. Saturated full drains therefore isolate cross-history
//! reconvergence; lazy first-result measurements include the value of all
//! physical coalescing, including same-history reassembly. Both arms retain
//! the canonical readiness scheduler, so this is not a separately tuned
//! no-merge executor.
//!
//! Usage:
//!     cargo run --release --example residual_intersection_bench -- \
//!         [n_per_parity=512] [fan=32] [c_fan=96] [reps=5]
//!     cargo run --release --features gpu \
//!         --example residual_intersection_bench -- 512 32 96 5
//!
//! With `gpu`, the benchmark additionally discovers a final-confirm action
//! whose canonical rank stream is wider than every corresponding isolated
//! stream, places WGPU admission strictly between them, and reports exact
//! shadow-attributed routes plus global dispatch/fallback counters.

use std::time::Instant;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::residual::ResidualStateStats;
#[cfg(feature = "gpu")]
use triblespace::core::query::residual::{ActionSite, ResidualShadowEpoch, ResidualShadowSnapshot};
use triblespace::core::query::TriblePattern;
use triblespace::core::trible::TribleSet;
#[cfg(feature = "gpu")]
use triblespace::gpu::{WgpuQueryStats, WgpuSuccinctArchive};
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

#[derive(Clone, Copy)]
enum CoalescingMode {
    Canonical,
    IsolatedFilings,
}

impl CoalescingMode {
    fn isolated(self) -> bool {
        matches!(self, Self::IsolatedFilings)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::IsolatedFilings => "isolated",
        }
    }
}

fn run_saturated_ablation<S: TriblePattern>(kb: &S, mode: CoalescingMode) -> (usize, u64) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy()
    .cap(usize::MAX)
    .start_width(usize::MAX)
    .growth(1);
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    tally(residual)
}

fn run_saturated_ablation_profiled<S: TriblePattern>(
    kb: &S,
    mode: CoalescingMode,
) -> ((usize, u64), ResidualStateStats) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy()
    .cap(usize::MAX)
    .start_width(usize::MAX)
    .growth(1);
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    let solve = residual.collect_profiled();
    (tally(solve.results), solve.stats)
}

fn run_lazy_ablation_profiled<S: TriblePattern>(
    kb: &S,
    mode: CoalescingMode,
) -> ((usize, u64), ResidualStateStats) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy();
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    let solve = residual.collect_profiled();
    (tally(solve.results), solve.stats)
}

fn run_lazy_ablation<S: TriblePattern>(kb: &S, mode: CoalescingMode) -> (usize, u64) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy();
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    tally(residual)
}

fn run_lazy_ablation_first<S: TriblePattern>(kb: &S, mode: CoalescingMode) -> (usize, u64) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy();
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    tally(residual.take(1))
}

fn run_nonreconvergent_control<S: TriblePattern>(kb: &S, mode: CoalescingMode) -> (usize, u64) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x }])
    )
    .solve_residual_state_lazy()
    .cap(usize::MAX)
    .start_width(usize::MAX)
    .growth(1);
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    tally(residual)
}

fn run_nonreconvergent_control_profiled<S: TriblePattern>(
    kb: &S,
    mode: CoalescingMode,
) -> ((usize, u64), ResidualStateStats) {
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(kb, [{ ?p @ world::a: ?x }])
    )
    .solve_residual_state_lazy()
    .cap(usize::MAX)
    .start_width(usize::MAX)
    .growth(1);
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    let solve = residual.collect_profiled();
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

fn distribution(samples: &[f64]) -> (f64, f64, f64) {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        *sorted.first().unwrap(),
        median(&sorted),
        *sorted.last().unwrap(),
    )
}

fn print_ablation_stats(label: &str, stats: &ResidualStateStats) {
    println!(
        "      {label:<20} states {} + hits {}; merges {} / {} rows; \
         isolated {} / {} rows; reentries {} / {} rows; \
         pops {} [full {} ready {} continuation {} partial {}]; \
         Ready plan {} [preferred {} scheduled {} proposal {} agglomerated {}]; \
         Candidate plan {} [confirmation groups {}]; \
         propose {} calls / {} rows / max rows {} / max candidates {}; \
         confirm {} calls / {} rows / max rows {} / max candidates {}",
        stats.states_interned,
        stats.interner_hits,
        stats.bucket_merges,
        stats.rows_merged,
        stats.isolated_filings,
        stats.rows_isolated,
        stats.state_reentries,
        stats.rows_reentered,
        stats.state_pops,
        stats.full_pops,
        stats.readiness_pops,
        stats.continuation_pops,
        stats.partial_pops,
        stats.ready_plan_pops,
        stats.ready_preferred_variable_groups,
        stats.ready_scheduled_variable_groups,
        stats.ready_proposal_groups,
        stats.agglomerated_ready_pops,
        stats.candidate_plan_pops,
        stats.candidate_confirmation_groups,
        stats.propose_calls,
        stats.propose_rows,
        stats.max_propose_rows,
        stats.max_propose_candidates,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.max_confirm_rows,
        stats.max_confirm_candidates,
    );
}

fn bench_coalescing_ablation<S: TriblePattern>(
    label: &str,
    kb: &S,
    reference: (usize, u64),
    reps: usize,
) {
    let modes = [CoalescingMode::Canonical, CoalescingMode::IsolatedFilings];
    for &mode in &modes {
        std::hint::black_box(run_lazy_ablation_first(kb, mode));
        std::hint::black_box(run_lazy_ablation(kb, mode));
        std::hint::black_box(run_saturated_ablation(kb, mode));
        std::hint::black_box(run_nonreconvergent_control(kb, mode));
    }

    let mut first_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut first_signatures = vec![(0, 0); modes.len()];
    let mut lazy_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut lazy_signatures = vec![(0, 0); modes.len()];
    let mut drain_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut drain_signatures = vec![(0, 0); modes.len()];
    let mut control_samples = vec![Vec::with_capacity(reps); modes.len()];
    let mut control_signatures = vec![(0, 0); modes.len()];
    for repetition in 0..reps {
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let start = Instant::now();
            first_signatures[mode_index] = run_lazy_ablation_first(kb, modes[mode_index]);
            first_samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset + 1) % modes.len();
            let start = Instant::now();
            lazy_signatures[mode_index] = run_lazy_ablation(kb, modes[mode_index]);
            lazy_samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset) % modes.len();
            let start = Instant::now();
            drain_signatures[mode_index] = run_saturated_ablation(kb, modes[mode_index]);
            drain_samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
        for offset in 0..modes.len() {
            let mode_index = (repetition + offset + 1) % modes.len();
            let start = Instant::now();
            control_signatures[mode_index] = run_nonreconvergent_control(kb, modes[mode_index]);
            control_samples[mode_index].push(start.elapsed().as_secs_f64() * 1e3);
        }
    }

    println!("  physical-coalescing ablation ({label}):");
    println!("    lazy first result (min / p50 / max):");
    for (index, mode) in modes.iter().copied().enumerate() {
        assert_eq!(
            first_signatures[index].0,
            1,
            "{label} {} must produce one first result",
            mode.label()
        );
        let (minimum, p50, maximum) = distribution(&first_samples[index]);
        println!(
            "      {:<10} {:>9.3} / {:>9.3} / {:>9.3} ms",
            mode.label(),
            minimum,
            p50,
            maximum
        );
    }
    println!("    geometric lazy full drain (min / p50 / max):");
    for (index, mode) in modes.iter().copied().enumerate() {
        assert_eq!(
            lazy_signatures[index],
            reference,
            "{label} {} lazy result signature mismatch",
            mode.label()
        );
        let (minimum, p50, maximum) = distribution(&lazy_samples[index]);
        println!(
            "      {:<10} {:>9.3} / {:>9.3} / {:>9.3} ms",
            mode.label(),
            minimum,
            p50,
            maximum
        );
    }
    println!("    saturated full drain (min / p50 / max):");
    for (index, mode) in modes.iter().copied().enumerate() {
        assert_eq!(
            drain_signatures[index],
            reference,
            "{label} {} saturated result signature mismatch",
            mode.label()
        );
        let (minimum, p50, maximum) = distribution(&drain_samples[index]);
        println!(
            "      {:<10} {:>9.3} / {:>9.3} / {:>9.3} ms",
            mode.label(),
            minimum,
            p50,
            maximum
        );
    }
    assert_eq!(control_signatures[0], control_signatures[1]);
    println!("    nonreconvergent one-leaf control (min / p50 / max):");
    for (index, mode) in modes.iter().copied().enumerate() {
        let (minimum, p50, maximum) = distribution(&control_samples[index]);
        println!(
            "      {:<10} {:>9.3} / {:>9.3} / {:>9.3} ms",
            mode.label(),
            minimum,
            p50,
            maximum
        );
    }

    println!("    action/bucket profiles:");
    for &mode in &modes {
        let (signature, stats) = run_saturated_ablation_profiled(kb, mode);
        assert_eq!(signature, reference);
        print_ablation_stats(&format!("saturated {}", mode.label()), &stats);
        let (signature, stats) = run_lazy_ablation_profiled(kb, mode);
        assert_eq!(signature, reference);
        print_ablation_stats(&format!("lazy {}", mode.label()), &stats);
    }
    for &mode in &modes {
        let (signature, stats) = run_nonreconvergent_control_profiled(kb, mode);
        assert_eq!(signature, control_signatures[0]);
        assert_eq!(
            stats.isolated_filings,
            0,
            "one-leaf control unexpectedly reconverged in {} mode",
            mode.label()
        );
        print_ablation_stats(&format!("control {}", mode.label()), &stats);
    }
}

#[cfg(feature = "gpu")]
struct WgpuProbeRun {
    signature: (usize, u64),
    gpu: WgpuQueryStats,
    shadow: ResidualShadowSnapshot,
}

#[cfg(feature = "gpu")]
fn run_observed_wgpu_ablation(
    gpu: &WgpuSuccinctArchive<OrderedUniverse>,
    mode: CoalescingMode,
) -> WgpuProbeRun {
    gpu.reset_stats();
    let observed = gpu.observe_residual_actions();
    let epoch = ResidualShadowEpoch::new();
    let residual = find!(
        (p: Inline<_>, x: Inline<_>),
        pattern!(&observed, [{ ?p @ world::a: ?x, world::b: ?x, world::c: ?x }])
    )
    .solve_residual_state_lazy()
    .cap(usize::MAX)
    .start_width(usize::MAX)
    .growth(1);
    let residual = if mode.isolated() {
        residual.isolated_filing_buckets()
    } else {
        residual
    };
    let solve = residual.shadow(epoch).collect_profiled();
    WgpuProbeRun {
        signature: tally(solve.results),
        gpu: gpu.stats(),
        shadow: solve.shadow,
    }
}

#[cfg(feature = "gpu")]
fn sampled_site_geometry(
    snapshot: &ResidualShadowSnapshot,
    site: ActionSite,
) -> Vec<(usize, usize, usize, &'static str)> {
    snapshot
        .events
        .iter()
        .filter(|event| event.site == site)
        .flat_map(|event| {
            event.executor_samples.iter().map(move |sample| {
                (
                    event.geometry.parent_rows,
                    event.geometry.candidate_occurrences,
                    sample.measurement.work_units,
                    sample.measurement.executor,
                )
            })
        })
        .collect()
}

#[cfg(feature = "gpu")]
fn print_wgpu_stats(label: &str, stats: WgpuQueryStats) {
    println!(
        "    {label:<10} GPU dispatches/probes {}/{} (batch {:?}..{:?}); \
         CPU fallback batches/probes {}/{}",
        stats.gpu_dispatches,
        stats.gpu_probes,
        stats.min_gpu_batch,
        stats.max_gpu_batch,
        stats.cpu_fallback_batches,
        stats.cpu_fallback_probes,
    );
}

#[cfg(feature = "gpu")]
fn bench_wgpu_coalescing_admission(archive: &SuccinctArchive<OrderedUniverse>) {
    let mut gpu = WgpuSuccinctArchive::new(archive.clone())
        .expect("failed to prepare SuccinctArchive ring columns for WGPU");
    gpu.set_min_rank_batch(usize::MAX);
    let canonical_cpu = run_observed_wgpu_ablation(&gpu, CoalescingMode::Canonical);
    let isolated_cpu = run_observed_wgpu_ablation(&gpu, CoalescingMode::IsolatedFilings);
    let cpu_reference = run_saturated_ablation(archive, CoalescingMode::Canonical);
    assert_eq!(canonical_cpu.signature, cpu_reference);
    assert_eq!(isolated_cpu.signature, cpu_reference);

    let mut discriminating = None;
    for event in &canonical_cpu.shadow.events {
        if event.executor_samples.len() != 1 {
            continue;
        }
        let canonical_probes = event.executor_samples[0].measurement.work_units;
        let isolated = sampled_site_geometry(&isolated_cpu.shadow, event.site);
        let isolated_max = isolated
            .iter()
            .map(|(_, _, probes, _)| *probes)
            .max()
            .unwrap_or(0);
        if isolated.len() >= 2 && isolated_max + 1 < canonical_probes {
            let replace = discriminating
                .as_ref()
                .is_none_or(|(_, best, _)| canonical_probes > *best);
            if replace {
                discriminating = Some((event.site, canonical_probes, isolated_max));
            }
        }
    }
    let (site, canonical_probes, isolated_max) = discriminating
        .expect("reconvergent fixture exposed no canonical action wider than its isolated filings");
    let threshold = isolated_max + (canonical_probes - isolated_max) / 2;
    assert!(isolated_max < threshold && threshold < canonical_probes);
    let canonical_discovery = sampled_site_geometry(&canonical_cpu.shadow, site);
    let isolated_discovery = sampled_site_geometry(&isolated_cpu.shadow, site);
    assert!(canonical_discovery
        .iter()
        .all(|(_, _, _, executor)| *executor == "cpu"));
    assert!(isolated_discovery
        .iter()
        .all(|(_, _, _, executor)| *executor == "cpu"));

    gpu.set_min_rank_batch(threshold);
    let canonical = run_observed_wgpu_ablation(&gpu, CoalescingMode::Canonical);
    let isolated = run_observed_wgpu_ablation(&gpu, CoalescingMode::IsolatedFilings);
    assert_eq!(canonical.signature, cpu_reference);
    assert_eq!(isolated.signature, cpu_reference);
    let canonical_routed = sampled_site_geometry(&canonical.shadow, site);
    let isolated_routed = sampled_site_geometry(&isolated.shadow, site);
    assert!(canonical_routed
        .iter()
        .all(|(_, _, _, executor)| *executor == "wgpu"));
    assert!(isolated_routed
        .iter()
        .all(|(_, _, _, executor)| *executor == "cpu"));

    println!("\n== Observed WGPU coalescing admission probe ==");
    println!(
        "  target site: {:?}; discovered canonical/isolated-max probes {}/{}; threshold {}",
        site, canonical_probes, isolated_max, threshold
    );
    println!("  CPU discovery geometry (parent rows, candidates, probes, executor):");
    println!("    canonical {canonical_discovery:?}");
    println!("    isolated  {isolated_discovery:?}");
    println!("  routed target geometry:");
    println!("    canonical {canonical_routed:?}");
    println!("    isolated  {isolated_routed:?}");
    print_wgpu_stats("canonical", canonical.gpu);
    print_wgpu_stats("isolated", isolated.gpu);
    println!("  exact result-bag parity: ok ({})", cpu_reference.0);
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

    bench_coalescing_ablation(label, kb, reference, reps);

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
        "  lazy profile: states {} + hits {}, pops {} (full {} / readiness {} / partial {}), \
         live merges {} ({} rows), reentries {} ({} rows); propose {} calls/{} rows/max {}, \
         confirm {} calls/{} rows/max {}",
        lazy_stats.states_interned,
        lazy_stats.interner_hits,
        lazy_stats.state_pops,
        lazy_stats.full_pops,
        lazy_stats.readiness_pops,
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
    #[cfg(feature = "gpu")]
    bench_wgpu_coalescing_admission(&archive);
}
