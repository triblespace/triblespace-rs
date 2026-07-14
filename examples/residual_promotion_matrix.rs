//! Compact promotion matrix for the canonical residual-state scheduler.
//!
//! This is deliberately a plain executable rather than Criterion: every
//! timing cell is preceded by an exact sorted-bag parity gate, and the same
//! fixture is then measured through sequential DFS, geometric lazy DAG,
//! geometric lazy residual state, and saturated (width 4096) DAG/residual
//! schedules.  The fixtures target scheduler failure modes rather than a
//! single happy-path throughput number:
//!
//! - late confirmation hits at the first/middle/absent candidate;
//! - a no-result 16K-parent ladder (the semantic-death width-ramp gate);
//! - ragged 90%x1 / 9%x32 / 1%x4096 candidate fanout;
//! - 120 variable-order routes that reconverge before shared work;
//! - an opaque union with live, dead, and duplicate arms; and
//! - a real opaque regular-path constraint inside a root intersection.
//!
//! Usage:
//!     cargo run --release --example residual_promotion_matrix -- [reps=21]
//!     cargo run --release --features gpu --example residual_promotion_matrix -- [reps=21]
//!
//! `--features gpu` additionally runs the 120-route archive fixture through
//! `WgpuSuccinctArchive` when a local adapter can be constructed. GPU setup is
//! outside the timing cells.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::inline::RawInline;
use triblespace::core::query::residual::{ResidualStateSolve, ResidualStateStats};
use triblespace::core::query::{blocked_stats, dag_stats, CandidateSink, Constraint, EstimateSink};
use triblespace::core::query::{RowsView, TriblePattern, VariableId, VariableSet};
use triblespace::core::trible::TribleSet;
#[cfg(feature = "gpu")]
use triblespace::gpu::WgpuSuccinctArchive;
use triblespace::prelude::*;

const SATURATED_WIDTH: usize = 4096;

mod world {
    use triblespace::prelude::*;

    // The first ten IDs are the existing reconvergence probe attributes. The
    // fifth route reuses two existing synthetic benchmark attributes rather
    // than introducing another schema ID solely for a benchmark.
    attributes! {
        "3C3FCF6D97AE8EBF7C0927B5E317A4B8" as p1: inlineencodings::GenId;
        "E0D70C1FB8E95BE40A6A02218DA7C8C0" as p2: inlineencodings::GenId;
        "9398CD61E3D8A87B8C26B9647473F8E0" as p3: inlineencodings::GenId;
        "A771D8F7C3BE63EB0EC6BA6682C2A412" as p4: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p5: inlineencodings::GenId;
        "92C2F2C22151123A359A2F7F51F3519A" as t1: inlineencodings::GenId;
        "357DC9D201D1A0FDC4569C740219F831" as t2: inlineencodings::GenId;
        "8FB9F5E089C3212D899E8787DC1FA0AD" as t3: inlineencodings::GenId;
        "10515585D7503F3EFCCCB994A3418577" as t4: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as t5: inlineencodings::GenId;
        "0EFC41641FCD73A30E2414AE78DEC219" as z: inlineencodings::GenId;
        "BCB248E3850EA6ACF22E7B175B574E12" as tz: inlineencodings::GenId;
    }
}

/// A semantically transparent estimate override. The wrapped constraint still
/// owns proposal/confirmation truth; only scheduling advice changes.
struct EstimateOverride<C> {
    inner: C,
    estimate: usize,
}

impl<C> EstimateOverride<C> {
    fn new(inner: C, estimate: usize) -> Self {
        Self { inner, estimate }
    }
}

impl<'a, C: Constraint<'a>> Constraint<'a> for EstimateOverride<C> {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if !self.inner.variables().is_set(variable) {
            return false;
        }
        out.fill(self.estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}

/// Leaf-level query-protocol instrumentation. Wrapping the pattern backend
/// rather than the root intersection makes DAG and residual counts directly
/// comparable: both totals describe the same estimate/propose/confirm calls
/// into storage leaves (plus explicitly wrapped synthetic leaves).
#[derive(Default)]
struct ProtocolCounts {
    estimate_calls: AtomicU64,
    estimate_rows: AtomicU64,
    propose_calls: AtomicU64,
    propose_rows: AtomicU64,
    confirm_calls: AtomicU64,
    confirm_rows: AtomicU64,
    satisfied_calls: AtomicU64,
    satisfied_rows: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
struct ProtocolSnapshot {
    estimate_calls: u64,
    estimate_rows: u64,
    propose_calls: u64,
    propose_rows: u64,
    confirm_calls: u64,
    confirm_rows: u64,
    satisfied_calls: u64,
    satisfied_rows: u64,
}

impl ProtocolSnapshot {
    fn action_calls(self) -> u64 {
        self.propose_calls + self.confirm_calls
    }
}

impl ProtocolCounts {
    fn snapshot(&self) -> ProtocolSnapshot {
        ProtocolSnapshot {
            estimate_calls: self.estimate_calls.load(AtomicOrdering::Relaxed),
            estimate_rows: self.estimate_rows.load(AtomicOrdering::Relaxed),
            propose_calls: self.propose_calls.load(AtomicOrdering::Relaxed),
            propose_rows: self.propose_rows.load(AtomicOrdering::Relaxed),
            confirm_calls: self.confirm_calls.load(AtomicOrdering::Relaxed),
            confirm_rows: self.confirm_rows.load(AtomicOrdering::Relaxed),
            satisfied_calls: self.satisfied_calls.load(AtomicOrdering::Relaxed),
            satisfied_rows: self.satisfied_rows.load(AtomicOrdering::Relaxed),
        }
    }
}

struct CountedConstraint<C> {
    inner: C,
    counts: Arc<ProtocolCounts>,
}

impl<C> CountedConstraint<C> {
    fn new(inner: C, counts: Arc<ProtocolCounts>) -> Self {
        Self { inner, counts }
    }
}

impl<'a, C: Constraint<'a>> Constraint<'a> for CountedConstraint<C> {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.counts
            .estimate_calls
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.counts
            .estimate_rows
            .fetch_add(view.len() as u64, AtomicOrdering::Relaxed);
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.counts
            .propose_calls
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.counts
            .propose_rows
            .fetch_add(view.len() as u64, AtomicOrdering::Relaxed);
        self.inner.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.counts
            .confirm_calls
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.counts
            .confirm_rows
            .fetch_add(view.len() as u64, AtomicOrdering::Relaxed);
        self.inner.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.counts
            .satisfied_calls
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.counts
            .satisfied_rows
            .fetch_add(view.len() as u64, AtomicOrdering::Relaxed);
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }
}

struct CountingPattern<'a, S> {
    inner: &'a S,
    counts: Arc<ProtocolCounts>,
}

impl<'a, S> CountingPattern<'a, S> {
    fn new(inner: &'a S, counts: Arc<ProtocolCounts>) -> Self {
        Self { inner, counts }
    }
}

impl<S: TriblePattern> TriblePattern for CountingPattern<'_, S> {
    type PatternConstraint<'a>
        = CountedConstraint<S::PatternConstraint<'a>>
    where
        Self: 'a;

    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<inlineencodings::GenId>>,
        a: impl Into<Term<inlineencodings::GenId>>,
        v: impl Into<Term<V>>,
    ) -> Self::PatternConstraint<'a> {
        CountedConstraint::new(self.inner.pattern(e, a, v), Arc::clone(&self.counts))
    }
}

fn print_protocol_comparison(
    label: &str,
    dag_geo: ProtocolSnapshot,
    residual_geo: ProtocolSnapshot,
    dag_saturated: ProtocolSnapshot,
    residual_saturated: ProtocolSnapshot,
) {
    fn row(name: &str, snapshot: ProtocolSnapshot) {
        println!(
            "    {name:<8} estimate {}/{}r; propose {}/{}r; confirm {}/{}r; satisfied {}/{}r; p+c {}",
            snapshot.estimate_calls,
            snapshot.estimate_rows,
            snapshot.propose_calls,
            snapshot.propose_rows,
            snapshot.confirm_calls,
            snapshot.confirm_rows,
            snapshot.satisfied_calls,
            snapshot.satisfied_rows,
            snapshot.action_calls(),
        );
    }

    println!("  {label} leaf-protocol calls:");
    row("dag-geo", dag_geo);
    row("res-geo", residual_geo);
    row("dag-sat", dag_saturated);
    row("res-sat", residual_saturated);
    println!(
        "  {label} p+c nonincrease gate: geometric {} ({} <= {}), saturated {} ({} <= {})",
        if residual_geo.action_calls() <= dag_geo.action_calls() {
            "PASS"
        } else {
            "FAIL"
        },
        residual_geo.action_calls(),
        dag_geo.action_calls(),
        if residual_saturated.action_calls() <= dag_saturated.action_calls() {
            "PASS"
        } else {
            "FAIL"
        },
        residual_saturated.action_calls(),
        dag_saturated.action_calls(),
    );
}

/// Keeps exactly the first fanout value for each bound parent while reporting
/// a deliberately loose estimate, forcing the storage constraint to expose
/// the complete ragged candidate frontier before this leaf confirms it.
struct FirstPerParent {
    parent: VariableId,
    value: VariableId,
    first: Vec<(RawInline, RawInline)>,
}

impl FirstPerParent {
    fn new(parent: VariableId, value: VariableId, mut first: Vec<(RawInline, RawInline)>) -> Self {
        first.sort_unstable_by_key(|(parent, _)| *parent);
        Self {
            parent,
            value,
            first,
        }
    }

    fn accepted(&self, parent: &RawInline) -> Option<RawInline> {
        self.first
            .binary_search_by_key(parent, |(candidate, _)| *candidate)
            .ok()
            .map(|index| self.first[index].1)
    }
}

impl<'a> Constraint<'a> for FirstPerParent {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.value))
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.value {
            return false;
        }
        out.fill(1 << 20, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.value {
            return;
        }
        let Some(parent_col) = view.col(self.parent) else {
            return;
        };
        for (row_index, row) in view.iter().enumerate() {
            if let Some(value) = self.accepted(&row[parent_col]) {
                candidates.push(row_index as u32, value);
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.value {
            return;
        }
        let Some(parent_col) = view.col(self.parent) else {
            candidates.retain(|_, _| false);
            return;
        };
        candidates.retain(|row, value| {
            self.accepted(&view.row(row as usize)[parent_col]) == Some(*value)
        });
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match (view.col(self.parent), view.col(self.value)) {
            (Some(parent_col), Some(value_col)) => view
                .iter()
                .all(|row| self.accepted(&row[parent_col]) == Some(row[value_col])),
            _ => true,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Timing {
    median: Duration,
    p95: Duration,
}

fn timing(samples: &mut [Duration]) -> Timing {
    samples.sort_unstable();
    let median = samples[samples.len() / 2];
    let p95_index = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    Timing {
        median,
        p95: samples[p95_index],
    }
}

fn duration_us(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

fn paired_ratio(
    numerator: &[Duration],
    denominator: &[Duration],
    denominator_floor: Duration,
) -> (f64, f64) {
    assert_eq!(numerator.len(), denominator.len());
    let mut ratios: Vec<_> = numerator
        .iter()
        .zip(denominator)
        .map(|(numerator, denominator)| {
            numerator.as_secs_f64() / denominator.max(&denominator_floor).as_secs_f64()
        })
        .collect();
    ratios.sort_by(f64::total_cmp);
    let p95_index = (ratios.len() * 95).div_ceil(100).saturating_sub(1);
    (ratios[ratios.len() / 2], ratios[p95_index])
}

fn bag_digest<R: Hash>(bag: &[R]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bag.hash(&mut hasher);
    hasher.finish()
}

fn timed<T>(f: &mut impl FnMut() -> T) -> (Duration, T) {
    let started = Instant::now();
    let value = f();
    (started.elapsed(), value)
}

#[allow(clippy::too_many_arguments)]
fn benchmark_case<R>(
    label: &str,
    reps: usize,
    mut sequential_first: impl FnMut() -> Option<R>,
    mut dag_first: impl FnMut() -> Option<R>,
    mut residual_first: impl FnMut() -> Option<R>,
    mut sequential_full: impl FnMut() -> Vec<R>,
    mut dag_full: impl FnMut() -> Vec<R>,
    mut residual_full: impl FnMut() -> Vec<R>,
    mut dag_saturated: impl FnMut() -> Vec<R>,
    mut residual_saturated: impl FnMut() -> Vec<R>,
    mut residual_geo_profile: impl FnMut() -> ResidualStateSolve<R>,
    mut residual_sat_profile: impl FnMut() -> ResidualStateSolve<R>,
) where
    R: Clone + std::fmt::Debug + Hash + Ord,
{
    assert!(reps > 0, "timing repetitions must be nonzero");

    let mut reference = sequential_full();
    reference.sort_unstable();
    for (name, mut bag) in [
        ("dag-lazy", dag_full()),
        ("res-lazy", residual_full()),
        ("dag-sat", dag_saturated()),
        ("res-sat", residual_saturated()),
    ] {
        bag.sort_unstable();
        assert_eq!(bag, reference, "{label}: {name} exact result bag differs");
    }
    let reference_has_result = !reference.is_empty();
    assert_eq!(sequential_first().is_some(), reference_has_result);
    assert_eq!(dag_first().is_some(), reference_has_result);
    assert_eq!(residual_first().is_some(), reference_has_result);

    // Equal warm-up count per engine before interleaved measurements.
    black_box(sequential_first());
    black_box(dag_first());
    black_box(residual_first());
    black_box(sequential_full());
    black_box(dag_full());
    black_box(residual_full());
    black_box(dag_saturated());
    black_box(residual_saturated());

    let mut first_samples: [Vec<Duration>; 3] = std::array::from_fn(|_| Vec::with_capacity(reps));
    let mut full_samples: [Vec<Duration>; 5] = std::array::from_fn(|_| Vec::with_capacity(reps));
    for repetition in 0..reps {
        for offset in 0..3 {
            let mode = (repetition + offset) % 3;
            let (elapsed, value) = match mode {
                0 => timed(&mut sequential_first),
                1 => timed(&mut dag_first),
                _ => timed(&mut residual_first),
            };
            assert_eq!(value.is_some(), reference_has_result);
            first_samples[mode].push(elapsed);
            black_box(value);
        }
        for offset in 0..5 {
            let mode = (repetition + offset) % 5;
            let (elapsed, bag) = match mode {
                0 => timed(&mut sequential_full),
                1 => timed(&mut dag_full),
                2 => timed(&mut residual_full),
                3 => timed(&mut dag_saturated),
                _ => timed(&mut residual_saturated),
            };
            assert_eq!(bag.len(), reference.len());
            full_samples[mode].push(elapsed);
            black_box(bag);
        }
    }

    let ttfr_floor = Duration::from_micros(25);
    let paired_first = paired_ratio(&first_samples[2], &first_samples[1], ttfr_floor);
    let paired_geo = paired_ratio(&full_samples[2], &full_samples[1], Duration::ZERO);
    let paired_saturated = paired_ratio(&full_samples[4], &full_samples[3], Duration::ZERO);
    let first = first_samples.each_mut().map(|samples| timing(samples));
    let full = full_samples.each_mut().map(|samples| timing(samples));
    println!(
        "\n== {label} ==\n  exact bag: {} rows, digest {:#018x}",
        reference.len(),
        bag_digest(&reference),
    );
    for (name, result) in ["seq", "dag-lazy", "res-lazy"].into_iter().zip(first) {
        println!(
            "  first {name:<8} median/p95 {:>10.2}/{:>10.2} us",
            duration_us(result.median),
            duration_us(result.p95),
        );
    }
    for (name, result) in ["seq", "dag-lazy", "res-lazy", "dag-sat", "res-sat"]
        .into_iter()
        .zip(full)
    {
        println!(
            "  full  {name:<8} median/p95 {:>10.2}/{:>10.2} us",
            duration_us(result.median),
            duration_us(result.p95),
        );
    }
    let dag_ttfr_median = first[1].median.max(ttfr_floor);
    let dag_ttfr_p95 = first[1].p95.max(ttfr_floor);
    println!(
        "  ratios res/dag: first median {:.3}x, p95 {:.3}x (25us denominator floor); full geo {:.3}x; full saturated {:.3}x",
        first[2].median.as_secs_f64() / dag_ttfr_median.as_secs_f64(),
        first[2].p95.as_secs_f64() / dag_ttfr_p95.as_secs_f64(),
        full[2].median.as_secs_f64() / full[1].median.as_secs_f64(),
        full[4].median.as_secs_f64() / full[3].median.as_secs_f64(),
    );
    println!(
        "  paired res/dag median/p95: first {:.3}/{:.3}x; full geo {:.3}/{:.3}x; full saturated {:.3}/{:.3}x",
        paired_first.0,
        paired_first.1,
        paired_geo.0,
        paired_geo.1,
        paired_saturated.0,
        paired_saturated.1,
    );

    blocked_stats::set_enabled(true);
    dag_stats::set_enabled(true);
    blocked_stats::reset();
    dag_stats::reset();
    let mut observed_dag = dag_saturated();
    observed_dag.sort_unstable();
    assert_eq!(observed_dag, reference, "{label}: instrumented DAG bag");
    println!("  saturated DAG work: {}", blocked_stats::report());
    println!("  saturated DAG frontier: {}", dag_stats::report());
    blocked_stats::set_enabled(false);
    dag_stats::set_enabled(false);

    let mut geo = residual_geo_profile();
    geo.results.sort_unstable();
    assert_eq!(
        geo.results, reference,
        "{label}: profiled geometric residual bag"
    );
    let mut saturated = residual_sat_profile();
    saturated.results.sort_unstable();
    assert_eq!(
        saturated.results, reference,
        "{label}: profiled saturated residual bag"
    );
    print_residual_profile("geo", &geo.stats);
    print_residual_profile("sat", &saturated.stats);
    println!(
        "  residual live-frontier peak: unavailable (the residual profile exposes exact parent-row traffic, not simultaneous live rows)"
    );
}

fn print_residual_profile(label: &str, stats: &ResidualStateStats) {
    println!(
        "  residual {label}: states {}+{}hits, pops {} (full/readiness/partial {}/{}/{}; dead {}; emit {}), ready groups preferred/scheduled/proposal {}/{}/{} ({} coalesced pops), candidate confirmer groups {}, merges {} rows/{}, reentries {} rows/{}, propose {} calls/{} rows/max {}, confirm {} calls/{} rows/max {}, width increases {}",
        stats.states_interned,
        stats.interner_hits,
        stats.state_pops,
        stats.full_pops,
        stats.readiness_pops,
        stats.partial_pops,
        stats.dead_action_pops,
        stats.emit_pops,
        stats.ready_preferred_variable_groups,
        stats.ready_scheduled_variable_groups,
        stats.ready_proposal_groups,
        stats.agglomerated_ready_pops,
        stats.candidate_confirmation_groups,
        stats.bucket_merges,
        stats.rows_merged,
        stats.state_reentries,
        stats.rows_reentered,
        stats.propose_calls,
        stats.propose_rows,
        stats.max_propose_rows,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.max_confirm_rows,
        stats.width_increases,
    );
}

macro_rules! measure_query {
    ($label:expr, $reps:expr, $query:expr) => {{
        benchmark_case(
            $label,
            $reps,
            || ($query).sequential().next(),
            || ($query).solve_dag_lazy().next(),
            || ($query).solve_residual_state_lazy().next(),
            || ($query).sequential().collect(),
            || ($query).solve_dag_lazy().collect(),
            || ($query).solve_residual_state_lazy().collect(),
            || {
                ($query)
                    .solve_dag_lazy()
                    .cap(SATURATED_WIDTH)
                    .start_width(SATURATED_WIDTH)
                    .growth(1)
                    .collect()
            },
            || {
                ($query)
                    .solve_residual_state_lazy()
                    .cap(SATURATED_WIDTH)
                    .start_width(SATURATED_WIDTH)
                    .growth(1)
                    .collect()
            },
            || ($query).solve_residual_state_lazy().collect_profiled(),
            || {
                ($query)
                    .solve_residual_state_lazy()
                    .cap(SATURATED_WIDTH)
                    .start_width(SATURATED_WIDTH)
                    .growth(1)
                    .collect_profiled()
            },
        )
    }};
}

macro_rules! collect_protocol_engine {
    ($query:expr, dag_geo) => {
        ($query).solve_dag_lazy().collect::<Vec<_>>()
    };
    ($query:expr, residual_geo) => {
        ($query).solve_residual_state_lazy().collect::<Vec<_>>()
    };
    ($query:expr, dag_saturated) => {
        ($query)
            .solve_dag_lazy()
            .cap(SATURATED_WIDTH)
            .start_width(SATURATED_WIDTH)
            .growth(1)
            .collect::<Vec<_>>()
    };
    ($query:expr, residual_saturated) => {
        ($query)
            .solve_residual_state_lazy()
            .cap(SATURATED_WIDTH)
            .start_width(SATURATED_WIDTH)
            .growth(1)
            .collect::<Vec<_>>()
    };
}

fn deterministic_id(namespace: u32, counter: u64) -> ExclusiveId {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&counter.to_be_bytes());
    ExclusiveId::force(Id::new(raw).expect("nonzero namespace yields a valid deterministic ID"))
}

fn build_ladder_base(n: usize) -> (TribleSet, ExclusiveId) {
    let root = deterministic_id(0xD46A_1001, 1);
    let mut kb = TribleSet::new();
    for index in 0..n {
        let parent = deterministic_id(0xD46A_1002, index as u64 + 1);
        let child = deterministic_id(0xD46A_1003, index as u64 + 1);
        kb += entity! { &root @ world::p1: &parent };
        kb += entity! { &parent @ world::p2: &child };
    }
    (kb, root)
}

/// Incumbent lazy-DAG enumeration order for the unfiltered ladder. Defining
/// first/middle against this order makes the latency comparison independent of
/// a backend's physical cursor direction.
fn ladder_dag_order<S: TriblePattern>(
    kb: &S,
    root: &ExclusiveId,
) -> Vec<Inline<inlineencodings::GenId>> {
    find!(
        (
            p: Inline<inlineencodings::GenId>,
            x: Inline<inlineencodings::GenId>
        ),
        and!(
            EstimateOverride::new(pattern!(kb, [{ root @ world::p1: ?p }]), 0),
            pattern!(kb, [{ ?p @ world::p2: ?x }]),
        )
    )
    .solve_dag_lazy()
    .map(|(_, x)| x)
    .collect()
}

fn add_ladder_markers(
    kb: &mut TribleSet,
    root: &ExclusiveId,
    first_attribute: Attribute<inlineencodings::GenId>,
    middle_attribute: Attribute<inlineencodings::GenId>,
    order: &[Inline<inlineencodings::GenId>],
) {
    assert!(!order.is_empty());
    *kb += entity! { root @ first_attribute: order[0] };
    *kb += entity! { root @ middle_attribute: order[order.len() / 2] };
}

#[allow(clippy::too_many_arguments)]
fn bench_ladder<S: TriblePattern>(
    label: &str,
    kb: &S,
    root: &ExclusiveId,
    first_attribute: Attribute<inlineencodings::GenId>,
    middle_attribute: Attribute<inlineencodings::GenId>,
    absent_attribute: Attribute<inlineencodings::GenId>,
    n: usize,
    reps: usize,
) {
    macro_rules! rung {
        ($name:literal, $attribute:expr) => {
            measure_query!(
                &format!("late-hit {label} {} (N={n})", $name),
                reps,
                find!(
                    (
                        p: Inline<inlineencodings::GenId>,
                        x: Inline<inlineencodings::GenId>
                    ),
                    and!(
                        EstimateOverride::new(pattern!(kb, [{ root @ world::p1: ?p }]), 0),
                        pattern!(kb, [{ ?p @ world::p2: ?x }]),
                        EstimateOverride::new(pattern!(kb, [{ root @ $attribute: ?x }]), 16),
                    )
                )
            );
        };
    }
    rung!("first", first_attribute);
    rung!("middle", middle_attribute);
    rung!("absent", absent_attribute);
}

fn build_negative_ladder(n: usize) -> (TribleSet, ExclusiveId) {
    let root = deterministic_id(0xD46A_1101, 1);
    let mut kb = TribleSet::new();
    for index in 0..n {
        let parent = deterministic_id(0xD46A_1102, index as u64 + 1);
        let child = deterministic_id(0xD46A_1103, index as u64 + 1);
        kb += entity! { &root @ world::p1: &parent };
        kb += entity! { &parent @ world::p2: &child };
    }
    // root@p3 has no values. Its estimate is overridden to 16 so p2
    // proposes one child per parent and p3 kills each branch on confirm.
    (kb, root)
}

fn bench_negative<S: TriblePattern>(
    label: &str,
    kb: &S,
    root: &ExclusiveId,
    n: usize,
    reps: usize,
) {
    macro_rules! query {
        () => {
            find!(
                (p: Inline<inlineencodings::GenId>, x: Inline<inlineencodings::GenId>),
                and!(
                    EstimateOverride::new(pattern!(kb, [{ root @ world::p1: ?p }]), 0),
                    pattern!(kb, [{ ?p @ world::p2: ?x }]),
                    EstimateOverride::new(pattern!(kb, [{ root @ world::p3: ?x }]), 16),
                )
            )
        };
    }
    measure_query!(
        &format!("negative semantic-death {label} (N={n})"),
        reps,
        query!()
    );

    let fixed_reps = reps.min(7).max(3);
    let mut fixed = Vec::with_capacity(fixed_reps);
    let mut geometric = Vec::with_capacity(fixed_reps);
    for repetition in 0..fixed_reps {
        if repetition % 2 == 0 {
            let (elapsed, bag) = timed(&mut || {
                query!()
                    .solve_residual_state_lazy()
                    .cap(1)
                    .growth(1)
                    .collect::<Vec<_>>()
            });
            assert!(bag.is_empty());
            fixed.push(elapsed);
            let (elapsed, bag) =
                timed(&mut || query!().solve_residual_state_lazy().collect::<Vec<_>>());
            assert!(bag.is_empty());
            geometric.push(elapsed);
        } else {
            let (elapsed, bag) =
                timed(&mut || query!().solve_residual_state_lazy().collect::<Vec<_>>());
            assert!(bag.is_empty());
            geometric.push(elapsed);
            let (elapsed, bag) = timed(&mut || {
                query!()
                    .solve_residual_state_lazy()
                    .cap(1)
                    .growth(1)
                    .collect::<Vec<_>>()
            });
            assert!(bag.is_empty());
            fixed.push(elapsed);
        }
    }
    let paired = paired_ratio(&fixed, &geometric, Duration::ZERO);
    let fixed = timing(&mut fixed);
    let geometric = timing(&mut geometric);
    println!(
        "  semantic-death ramp speedup over width-1 control: median {:.2}x, p95 {:.2}x; paired median/p95 {:.2}/{:.2}x",
        fixed.median.as_secs_f64() / geometric.median.as_secs_f64(),
        fixed.p95.as_secs_f64() / geometric.p95.as_secs_f64(),
        paired.0,
        paired.1,
    );
}

fn build_ragged() -> (TribleSet, ExclusiveId, Vec<(RawInline, RawInline)>, usize) {
    let root = deterministic_id(0xD46A_1201, 1);
    let mut kb = TribleSet::new();
    let mut first = Vec::new();
    let mut candidate_pairs = 0usize;
    for parent_index in 0..100 {
        let fan = match parent_index {
            0..=89 => 1,
            90..=98 => 32,
            _ => 4096,
        };
        let parent = deterministic_id(0xD46A_1202, parent_index as u64 + 1);
        kb += entity! { &root @ world::p1: &parent };
        for value_index in 0..fan {
            let sequence = parent_index as u64 * 5000 + value_index as u64 + 1;
            let value = deterministic_id(0xD46A_1203, sequence);
            kb += entity! { &parent @ world::p2: &value };
            if value_index == 0 {
                let parent_inline: Inline<inlineencodings::GenId> = parent.clone().to_inline();
                let value_inline: Inline<inlineencodings::GenId> = value.clone().to_inline();
                first.push((parent_inline.raw, value_inline.raw));
            }
        }
        candidate_pairs += fan;
    }
    (kb, root, first, candidate_pairs)
}

fn bench_ragged<S: TriblePattern>(
    label: &str,
    kb: &S,
    root: &ExclusiveId,
    first: &[(RawInline, RawInline)],
    candidate_pairs: usize,
    reps: usize,
) {
    measure_query!(
        &format!("ragged fanout {label} (90x1/9x32/1x4096)"),
        reps,
        find!(
            (p: Inline<inlineencodings::GenId>, x: Inline<inlineencodings::GenId>),
            and!(
                pattern!(kb, [{ root @ world::p1: ?p }]),
                pattern!(kb, [{ ?p @ world::p2: ?x }]),
                FirstPerParent::new(p.index, x.index, first.to_vec()),
            )
        )
    );
    println!(
        "  ragged fixture truth: {candidate_pairs} storage-proposed (parent,value) pairs -> 100 live rows; parent occupancy alone hides {:.2} candidates/parent",
        candidate_pairs as f64 / 100.0,
    );

    macro_rules! counted_run {
        ($mode:ident) => {{
            let counts = Arc::new(ProtocolCounts::default());
            let counted = CountingPattern::new(kb, Arc::clone(&counts));
            let bag = collect_protocol_engine!(
                find!(
                    (
                        p: Inline<inlineencodings::GenId>,
                        x: Inline<inlineencodings::GenId>
                    ),
                    and!(
                        pattern!(&counted, [{ root @ world::p1: ?p }]),
                        pattern!(&counted, [{ ?p @ world::p2: ?x }]),
                        CountedConstraint::new(
                            FirstPerParent::new(p.index, x.index, first.to_vec()),
                            Arc::clone(&counts),
                        ),
                    )
                ),
                $mode
            );
            (bag, counts.snapshot())
        }};
    }

    let (mut dag_geo_bag, dag_geo) = counted_run!(dag_geo);
    let (mut residual_geo_bag, residual_geo) = counted_run!(residual_geo);
    let (mut dag_saturated_bag, dag_saturated) = counted_run!(dag_saturated);
    let (mut residual_saturated_bag, residual_saturated) = counted_run!(residual_saturated);
    for bag in [
        &mut dag_geo_bag,
        &mut residual_geo_bag,
        &mut dag_saturated_bag,
        &mut residual_saturated_bag,
    ] {
        bag.sort_unstable();
    }
    assert_eq!(residual_geo_bag, dag_geo_bag, "ragged counted geo bag");
    assert_eq!(dag_saturated_bag, dag_geo_bag, "ragged counted DAG bag");
    assert_eq!(
        residual_saturated_bag, dag_geo_bag,
        "ragged counted saturated bag"
    );
    print_protocol_comparison(
        &format!("ragged {label}"),
        dag_geo,
        residual_geo,
        dag_saturated,
        residual_saturated,
    );
}

fn build_union() -> (TribleSet, ExclusiveId) {
    let root = deterministic_id(0xD46A_1301, 1);
    let a = deterministic_id(0xD46A_1302, 1);
    let b = deterministic_id(0xD46A_1302, 2);
    let c = deterministic_id(0xD46A_1302, 3);
    let d = deterministic_id(0xD46A_1302, 4);
    let impossible = deterministic_id(0xD46A_1302, 5);
    let mut kb = TribleSet::new();
    kb += entity! { &root @ world::p1: &a };
    kb += entity! { &root @ world::p1: &b };
    kb += entity! { &root @ world::p2: &c };
    kb += entity! { &root @ world::p3: &impossible };
    kb += entity! { &root @ world::p4: &b };
    kb += entity! { &root @ world::p4: &d };
    (kb, root)
}

fn bench_union<S: TriblePattern>(label: &str, kb: &S, root: &ExclusiveId, reps: usize) {
    measure_query!(
        &format!("opaque Union {label} (live/dead/duplicate arms)"),
        reps,
        find!(
            x: Inline<inlineencodings::GenId>,
            and!(or!(
                pattern!(kb, [{ root @ world::p1: ?x }]),
                and!(
                    pattern!(kb, [{ root @ world::p2: ?x }]),
                    pattern!(kb, [{ root @ world::p3: ?x }]),
                ),
                pattern!(kb, [{ root @ world::p4: ?x }]),
            ))
        )
    );
}

fn permutations_5() -> Vec<[usize; 5]> {
    let mut permutations = Vec::with_capacity(120);
    for a in 0..5 {
        for b in 0..5 {
            if b == a {
                continue;
            }
            for c in 0..5 {
                if c == a || c == b {
                    continue;
                }
                for d in 0..5 {
                    if d == a || d == b || d == c {
                        continue;
                    }
                    let e = 10 - a - b - c - d;
                    permutations.push([a, b, c, d, e]);
                }
            }
        }
    }
    assert_eq!(permutations.len(), 120);
    permutations
}

type RouteMarkers = (Id, Id, Id, Id, Id, Id);

fn build_routes(n_per_route: usize, z_fan: usize) -> (TribleSet, RouteMarkers, usize) {
    let mut kb = TribleSet::new();
    let markers: Vec<_> = (0..5)
        .map(|index| deterministic_id(0xD46A_1401, index + 1))
        .collect();
    let z_marker = deterministic_id(0xD46A_1402, 1);
    let fans = [1usize, 2, 3, 4, 5];
    let mut counter = 1u64;

    for permutation in permutations_5() {
        for _ in 0..n_per_route {
            let entity = deterministic_id(0xD46A_1403, counter);
            counter += 1;
            for (rank, &attribute_index) in permutation.iter().enumerate() {
                let values: Vec<_> = (0..fans[rank])
                    .map(|_| {
                        let value = deterministic_id(0xD46A_1404, counter);
                        counter += 1;
                        value
                    })
                    .collect();
                for value in &values {
                    kb += match attribute_index {
                        0 => entity! { &entity @ world::p1: value },
                        1 => entity! { &entity @ world::p2: value },
                        2 => entity! { &entity @ world::p3: value },
                        3 => entity! { &entity @ world::p4: value },
                        _ => entity! { &entity @ world::p5: value },
                    };
                }
                let real = &values[0];
                let marker = &markers[attribute_index];
                kb += match attribute_index {
                    0 => entity! { real @ world::t1: marker },
                    1 => entity! { real @ world::t2: marker },
                    2 => entity! { real @ world::t3: marker },
                    3 => entity! { real @ world::t4: marker },
                    _ => entity! { real @ world::t5: marker },
                };
            }
            let z_values: Vec<_> = (0..z_fan)
                .map(|_| {
                    let value = deterministic_id(0xD46A_1405, counter);
                    counter += 1;
                    value
                })
                .collect();
            for value in &z_values {
                kb += entity! { &entity @ world::z: value };
            }
            kb += entity! { &z_values[0] @ world::tz: &z_marker };
        }
    }
    let expected = 120 * n_per_route;
    (
        kb,
        (
            *markers[0],
            *markers[1],
            *markers[2],
            *markers[3],
            *markers[4],
            *z_marker,
        ),
        expected,
    )
}

fn bench_routes<S: TriblePattern>(
    label: &str,
    kb: &S,
    markers: RouteMarkers,
    expected: usize,
    reps: usize,
) {
    let (k1, k2, k3, k4, k5, kz) = markers;
    measure_query!(
        &format!("120-route reconvergence {label}"),
        reps,
        find!(
            (
                e: Inline<inlineencodings::GenId>,
                x1: Inline<inlineencodings::GenId>,
                x2: Inline<inlineencodings::GenId>,
                x3: Inline<inlineencodings::GenId>,
                x4: Inline<inlineencodings::GenId>,
                x5: Inline<inlineencodings::GenId>,
                z: Inline<inlineencodings::GenId>
            ),
            pattern!(kb, [
                { ?e @ world::p1: ?x1, world::p2: ?x2, world::p3: ?x3, world::p4: ?x4, world::p5: ?x5, world::z: ?z },
                { ?x1 @ world::t1: k1 },
                { ?x2 @ world::t2: k2 },
                { ?x3 @ world::t3: k3 },
                { ?x4 @ world::t4: k4 },
                { ?x5 @ world::t5: k5 },
                { ?z @ world::tz: kz },
            ])
        )
    );
    println!("  route fixture truth: 120 distinct order routes, {expected} result rows");

    macro_rules! counted_run {
        ($mode:ident) => {{
            let counts = Arc::new(ProtocolCounts::default());
            let counted = CountingPattern::new(kb, Arc::clone(&counts));
            let bag = collect_protocol_engine!(
                find!(
                    (
                        e: Inline<inlineencodings::GenId>,
                        x1: Inline<inlineencodings::GenId>,
                        x2: Inline<inlineencodings::GenId>,
                        x3: Inline<inlineencodings::GenId>,
                        x4: Inline<inlineencodings::GenId>,
                        x5: Inline<inlineencodings::GenId>,
                        z: Inline<inlineencodings::GenId>
                    ),
                    pattern!(&counted, [
                        { ?e @ world::p1: ?x1, world::p2: ?x2, world::p3: ?x3, world::p4: ?x4, world::p5: ?x5, world::z: ?z },
                        { ?x1 @ world::t1: k1 },
                        { ?x2 @ world::t2: k2 },
                        { ?x3 @ world::t3: k3 },
                        { ?x4 @ world::t4: k4 },
                        { ?x5 @ world::t5: k5 },
                        { ?z @ world::tz: kz },
                    ])
                ),
                $mode
            );
            (bag, counts.snapshot())
        }};
    }

    let (mut dag_geo_bag, dag_geo) = counted_run!(dag_geo);
    let (mut residual_geo_bag, residual_geo) = counted_run!(residual_geo);
    let (mut dag_saturated_bag, dag_saturated) = counted_run!(dag_saturated);
    let (mut residual_saturated_bag, residual_saturated) = counted_run!(residual_saturated);
    for bag in [
        &mut dag_geo_bag,
        &mut residual_geo_bag,
        &mut dag_saturated_bag,
        &mut residual_saturated_bag,
    ] {
        bag.sort_unstable();
    }
    assert_eq!(residual_geo_bag, dag_geo_bag, "route counted geo bag");
    assert_eq!(dag_saturated_bag, dag_geo_bag, "route counted DAG bag");
    assert_eq!(
        residual_saturated_bag, dag_geo_bag,
        "route counted saturated bag"
    );
    print_protocol_comparison(
        &format!("route {label}"),
        dag_geo,
        residual_geo,
        dag_saturated,
        residual_saturated,
    );
}

fn build_rpq(
    nodes: usize,
) -> (
    TribleSet,
    Inline<inlineencodings::GenId>,
    ExclusiveId,
    usize,
) {
    let chain: Vec<_> = (0..nodes)
        .map(|index| deterministic_id(0xD46A_1501, index as u64 + 1))
        .collect();
    let marker = deterministic_id(0xD46A_1502, 1);
    let mut kb = TribleSet::new();
    for index in 0..nodes - 1 {
        if index % 2 == 0 {
            kb += entity! { &chain[index] @ world::p1: &chain[index + 1] };
        } else {
            kb += entity! { &chain[index] @ world::p2: &chain[index + 1] };
        }
    }
    let mut expected = 0usize;
    for index in (16..nodes).step_by(16) {
        kb += entity! { &chain[index] @ world::p3: &marker };
        expected += 1;
    }
    (kb, chain[0].clone().to_inline(), marker, expected)
}

fn bench_rpq(
    kb: &TribleSet,
    start: Inline<inlineencodings::GenId>,
    marker: &ExclusiveId,
    expected: usize,
    reps: usize,
) {
    measure_query!(
        "opaque RPQ inside AND (TribleSet)",
        reps,
        find!(
            (
                s: Inline<inlineencodings::GenId>,
                e: Inline<inlineencodings::GenId>
            ),
            and!(
                s.is(start),
                path!(kb.clone(), s (world::p1 | world::p2)+ e),
                pattern!(kb, [{ ?e @ world::p3: marker }]),
            )
        )
    );
    println!("  RPQ fixture truth: {expected} tagged reachable endpoints");
}

fn main() {
    let reps = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(21);
    assert!(reps >= 3, "use at least three repetitions");
    eprintln!(
        "residual promotion matrix: exact candidate 9d74fbdab3717ffb6a0ec5acc80b77e26c55bb3e; reps {reps}; saturated width {SATURATED_WIDTH}"
    );

    let (mut ladder, ladder_root) = build_ladder_base(4096);
    let tribleset_order = ladder_dag_order(&ladder, &ladder_root);
    let preliminary_archive: SuccinctArchive<OrderedUniverse> = (&ladder).into();
    let archive_order = ladder_dag_order(&preliminary_archive, &ladder_root);
    add_ladder_markers(
        &mut ladder,
        &ladder_root,
        world::p3.clone(),
        world::p4.clone(),
        &tribleset_order,
    );
    add_ladder_markers(
        &mut ladder,
        &ladder_root,
        world::t1.clone(),
        world::t2.clone(),
        &archive_order,
    );
    let ladder_archive: SuccinctArchive<OrderedUniverse> = (&ladder).into();
    bench_ladder(
        "TribleSet",
        &ladder,
        &ladder_root,
        world::p3.clone(),
        world::p4.clone(),
        world::p5.clone(),
        4096,
        reps,
    );
    bench_ladder(
        "SuccinctArchive",
        &ladder_archive,
        &ladder_root,
        world::t1.clone(),
        world::t2.clone(),
        world::t3.clone(),
        4096,
        reps,
    );

    let (negative, negative_root) = build_negative_ladder(16_384);
    let negative_archive: SuccinctArchive<OrderedUniverse> = (&negative).into();
    bench_negative("TribleSet", &negative, &negative_root, 16_384, reps);
    bench_negative(
        "SuccinctArchive",
        &negative_archive,
        &negative_root,
        16_384,
        reps,
    );

    let (ragged, ragged_root, first, candidate_pairs) = build_ragged();
    let ragged_archive: SuccinctArchive<OrderedUniverse> = (&ragged).into();
    bench_ragged(
        "TribleSet",
        &ragged,
        &ragged_root,
        &first,
        candidate_pairs,
        reps,
    );
    bench_ragged(
        "SuccinctArchive",
        &ragged_archive,
        &ragged_root,
        &first,
        candidate_pairs,
        reps,
    );

    let (union, union_root) = build_union();
    let union_archive: SuccinctArchive<OrderedUniverse> = (&union).into();
    bench_union("TribleSet", &union, &union_root, reps);
    bench_union("SuccinctArchive", &union_archive, &union_root, reps);

    let (routes, markers, expected_routes) = build_routes(4, 16);
    let routes_archive: SuccinctArchive<OrderedUniverse> = (&routes).into();
    bench_routes("TribleSet", &routes, markers, expected_routes, reps);
    bench_routes(
        "SuccinctArchive",
        &routes_archive,
        markers,
        expected_routes,
        reps,
    );

    let (rpq, start, marker, expected_rpq) = build_rpq(257);
    bench_rpq(&rpq, start, &marker, expected_rpq, reps);

    #[cfg(feature = "gpu")]
    {
        let mut gpu = WgpuSuccinctArchive::new(routes_archive).expect(
            "failed to construct WGPU archive; rerun without --features gpu if no adapter exists",
        );
        gpu.set_min_rank_batch(1);
        gpu.reset_stats();
        bench_routes("WgpuSuccinctArchive", &gpu, markers, expected_routes, reps);
        let stats = gpu.stats();
        println!(
            "  WGPU aggregate: dispatches/probes {}/{}; CPU fallback batches/probes {}/{}; GPU batch min/max {:?}/{:?}",
            stats.gpu_dispatches,
            stats.gpu_probes,
            stats.cpu_fallback_batches,
            stats.cpu_fallback_probes,
            stats.min_gpu_batch,
            stats.max_gpu_batch,
        );
    }
}
