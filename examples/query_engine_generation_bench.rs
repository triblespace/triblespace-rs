//! Cross-generation query-engine benchmark.
//!
//! This source is intentionally kept byte-identical on the last legacy
//! Binding/Vec DFS revision and on the residual-engine revision. Select the
//! terminal adapter at compile time:
//!
//! ```text
//! cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_legacy_binding" cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_scalar" cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual" cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual --cfg engine_prefix_checkpoints" \
//!   cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual --cfg engine_prefix_checkpoints \
//!   --cfg engine_counter_only --cfg engine_counter_production" \
//!   cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual --cfg engine_prefix_checkpoints \
//!   --cfg formula_delta_transport_probe --cfg formula_prepared_continuation_probe" \
//!   cargo run --release --example query_engine_generation_bench
//! ```
//!
//! Fixture/archive construction and the independent relational oracles are
//! outside all timings. Every measured engine must exactly match the oracle
//! before its samples are reported.

#![allow(unexpected_cfgs)]
#![cfg_attr(engine_prefix_checkpoints, allow(dead_code))]

#[cfg(all(engine_counter_only, not(engine_prefix_checkpoints)))]
compile_error!("engine_counter_only requires engine_prefix_checkpoints");

#[cfg(all(engine_counter_only, not(engine_current_residual)))]
compile_error!("engine_counter_only requires engine_current_residual");

#[cfg(all(
    formula_delta_transport_probe,
    not(all(engine_prefix_checkpoints, engine_current_residual))
))]
compile_error!("formula_delta_transport_probe requires current residual prefix mode");

#[cfg(all(formula_delta_transport_probe, engine_counter_only))]
compile_error!("formula_delta_transport_probe is its own three-mode harness");

#[cfg(all(
    formula_delta_transport_probe,
    not(formula_prepared_continuation_probe)
))]
compile_error!("the Formula continuation panel requires formula_prepared_continuation_probe");

#[cfg(all(
    formula_prepared_continuation_probe,
    not(formula_delta_transport_probe)
))]
compile_error!("formula_prepared_continuation_probe requires the Formula continuation panel");

#[cfg(all(engine_counter_opaque_production, engine_counter_production))]
compile_error!("select exactly one counter lowering");

#[cfg(all(
    engine_counter_only,
    not(any(engine_counter_opaque_production, engine_counter_production))
))]
compile_error!("engine_counter_only requires an explicit counter lowering");

#[cfg(all(
    any(engine_counter_opaque_production, engine_counter_production),
    not(engine_counter_only)
))]
compile_error!("counter lowering selectors require engine_counter_only");

#[cfg(any(
    all(engine_legacy_binding, engine_current_scalar),
    all(engine_legacy_binding, engine_current_residual),
    all(engine_current_scalar, engine_current_residual),
))]
compile_error!("select exactly one benchmark engine");

use std::hint::black_box;
use std::time::{Duration, Instant};

#[cfg(engine_allocation_probe)]
mod allocation_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct CountingAllocator;

    static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
    static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
    static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
    static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
    const LIMITS: [usize; 12] = [
        32,
        64,
        128,
        256,
        512,
        1024,
        4096,
        16384,
        65536,
        262144,
        1048576,
        usize::MAX,
    ];
    static BINS: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];
    static BIN_BYTES: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];

    fn record_allocation(size: usize) {
        let bytes = size as u64;
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
        let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
        PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
        let bin = LIMITS.partition_point(|&limit| limit < size);
        BINS[bin].fetch_add(1, Ordering::Relaxed);
        BIN_BYTES[bin].fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_deallocation(size: usize) {
        let bytes = size as u64;
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(bytes, Ordering::Relaxed);
    }

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                record_allocation(layout.size());
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            record_deallocation(layout.size());
            unsafe { System.dealloc(ptr, layout) };
        }

        unsafe fn realloc(&self, ptr: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = unsafe { System.realloc(ptr, old, new_size) };
            if !new_ptr.is_null() {
                record_deallocation(old.size());
                record_allocation(new_size);
            }
            new_ptr
        }
    }

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    pub struct Snapshot {
        allocations: u64,
        deallocations: u64,
        allocated_bytes: u64,
        deallocated_bytes: u64,
        live_bytes: u64,
        peak_live_bytes: u64,
        bins: [u64; 12],
        bin_bytes: [u64; 12],
    }

    impl Snapshot {
        pub fn now() -> Self {
            Self {
                allocations: ALLOCATIONS.load(Ordering::Relaxed),
                deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
                allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
                deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
                live_bytes: LIVE_BYTES.load(Ordering::Relaxed),
                peak_live_bytes: PEAK_LIVE_BYTES.load(Ordering::Relaxed),
                bins: std::array::from_fn(|i| BINS[i].load(Ordering::Relaxed)),
                bin_bytes: std::array::from_fn(|i| BIN_BYTES[i].load(Ordering::Relaxed)),
            }
        }

        pub fn report_since(&self, before: &Self, label: &str, repetitions: usize) {
            println!(
                "alloc_profile cell={label:?} repetitions={repetitions} calls={} frees={} \
                 allocated_bytes={} deallocated_bytes={} live_delta={} peak_above_baseline={}",
                self.allocations - before.allocations,
                self.deallocations - before.deallocations,
                self.allocated_bytes - before.allocated_bytes,
                self.deallocated_bytes - before.deallocated_bytes,
                self.live_bytes as i128 - before.live_bytes as i128,
                self.peak_live_bytes.saturating_sub(before.live_bytes),
            );
            for index in 0..LIMITS.len() {
                let count = self.bins[index] - before.bins[index];
                if count != 0 {
                    println!(
                        "alloc_bin upper={} count={} bytes={}",
                        LIMITS[index],
                        count,
                        self.bin_bytes[index] - before.bin_bytes[index],
                    );
                }
            }
        }
    }

    pub fn reset_peak_to_live() {
        PEAK_LIVE_BYTES.store(LIVE_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
#[cfg(formula_delta_transport_probe)]
use triblespace::core::query::residual::{
    formula_delta_transport_probe_select, formula_prepared_continuation_probe_select,
    FormulaDeltaTransportProbeSelector, FormulaPreparedContinuationProbeSelector, FormulaScope,
    ProgramScope, ResidualLowering, ResidualStateStats,
};
use triblespace::core::query::TriblePattern;
use triblespace::core::trible::TribleSet;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod bench_schema {
    use triblespace::prelude::*;

    // Reuse the query-engine oracle attributes. No benchmark-local protocol
    // identifiers are introduced.
    attributes! {
        "522EB8351DA60956D2D16E6ED9745BA7" as kind: inlineencodings::GenId;
        "FDD49F6E08AC2CCB79EE6C8B1256AD02" as p: inlineencodings::GenId;
        "A4D08AA59273B336F5B977CE1511D141" as q: inlineencodings::GenId;
    }
}

#[cfg(engine_legacy_binding)]
const ENGINE: &str = "legacy Binding DFS";
#[cfg(engine_current_scalar)]
const ENGINE: &str = "current scalar DFS";
#[cfg(engine_current_residual)]
const ENGINE: &str = "current residual";
#[cfg(not(any(engine_legacy_binding, engine_current_scalar, engine_current_residual)))]
const ENGINE: &str = "ordinary Query iterator";

const REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};

#[cfg(all(engine_counter_only, engine_counter_opaque_production))]
const COUNTER_LOWERING: triblespace::core::query::residual::ResidualLowering =
    triblespace::core::query::residual::ResidualLowering::OPAQUE_PRODUCTION;
#[cfg(all(engine_counter_only, engine_counter_production))]
const COUNTER_LOWERING: triblespace::core::query::residual::ResidualLowering =
    triblespace::core::query::residual::ResidualLowering::PRODUCTION;

#[cfg(all(engine_counter_only, engine_counter_opaque_production))]
const COUNTER_LOWERING_LABEL: &str = "OPAQUE_PRODUCTION";
#[cfg(all(engine_counter_only, engine_counter_production))]
const COUNTER_LOWERING_LABEL: &str = "PRODUCTION";

type Pair = (Inline<GenId>, Inline<GenId>);

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormulaTransportProbeMode {
    StableFiled,
    GFiled,
    GOwned,
}

#[cfg(formula_delta_transport_probe)]
impl FormulaTransportProbeMode {
    const ALL: [Self; 3] = [Self::StableFiled, Self::GFiled, Self::GOwned];

    fn label(self) -> &'static str {
        match self {
            Self::StableFiled => "D_STABLE_FILED",
            Self::GFiled => "GF_G_FILED",
            Self::GOwned => "GO_G_OWNED",
        }
    }

    fn lowering(self) -> ResidualLowering {
        match self {
            Self::StableFiled | Self::GFiled | Self::GOwned => ResidualLowering::PRODUCTION,
        }
    }

    fn arm(self) {
        let (transport, continuation) = match self {
            Self::StableFiled => (
                FormulaDeltaTransportProbeSelector::StableAll,
                FormulaPreparedContinuationProbeSelector::Filed,
            ),
            Self::GFiled => (
                FormulaDeltaTransportProbeSelector::StableProposeConfirm,
                FormulaPreparedContinuationProbeSelector::Filed,
            ),
            Self::GOwned => (
                FormulaDeltaTransportProbeSelector::StableProposeConfirm,
                FormulaPreparedContinuationProbeSelector::Owned,
            ),
        };
        formula_delta_transport_probe_select(transport);
        formula_prepared_continuation_probe_select(continuation);
    }
}

#[cfg(formula_delta_transport_probe)]
macro_rules! formula_transport_probe_query {
    ($store:expr, $fixture:expr, $mode:expr) => {{
        let mode = $mode;
        mode.arm();
        mixed_formula_rpq_query!($store, $fixture).solve_residual_state_lazy_with(mode.lowering())
    }};
}

macro_rules! engine_query {
    ($query:expr) => {{
        let query = $query;
        #[cfg(engine_current_scalar)]
        {
            query.sequential()
        }
        #[cfg(not(engine_current_scalar))]
        {
            query
        }
    }};
}

macro_rules! finite_union_query {
    ($store:expr, $fixture:expr) => {
        engine_query!(find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            or!(
                and!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
                ),
                and!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                    pattern!($store, [{ ?source @ bench_schema::q: ?target }]),
                ),
            )
        ))
    };
}

macro_rules! nested_formula_query {
    ($store:expr, $fixture:expr) => {
        engine_query!(find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                ),
                or!(
                    and!(
                        pattern!($store, [{ ?source @ bench_schema::p: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                    and!(
                        pattern!($store, [{ ?source @ bench_schema::q: ?target }]),
                        or!(
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                            pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                        ),
                    ),
                ),
            )
        ))
    };
}

macro_rules! cyclic_rpq_query {
    ($fixture:expr) => {
        engine_query!(find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            path!(
                ($fixture).graph.clone(),
                source (bench_schema::p | bench_schema::q)+ target
            )
        ))
    };
}

macro_rules! mixed_formula_rpq_query {
    ($store:expr, $fixture:expr) => {
        engine_query!(find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                    pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).alternate) }]),
                ),
                path!(
                    ($fixture).graph.clone(),
                    source (bench_schema::p | bench_schema::q)+ target
                ),
                or!(
                    pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).red) }]),
                    pattern!($store, [{ ?target @ bench_schema::kind: (&($fixture).blue) }]),
                ),
            )
        ))
    };
}

struct Fixture {
    graph: TribleSet,
    components: Vec<Vec<Id>>,
    seed: Id,
    alternate: Id,
    red: Id,
    blue: Id,
    fanout: usize,
}

fn fixture_id(namespace: u64, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&ordinal.checked_add(1).unwrap().to_be_bytes());
    Id::new(raw).expect("the fixture namespace is non-zero")
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

impl Fixture {
    fn new(component_count: usize, ring_size: usize, fanout: usize) -> Self {
        assert!(component_count > 0, "component count must be non-zero");
        assert!(
            ring_size >= 4 && ring_size % 4 == 0,
            "ring size must be divisible by four"
        );
        assert!(fanout > 0, "fanout must be non-zero");
        assert!(
            2 * fanout < ring_size,
            "p and q edge bands must be disjoint"
        );

        const NODE_NAMESPACE: u64 = 0xD46A_0003_0000_0001;
        const MARKER_NAMESPACE: u64 = 0xD46A_0003_0000_0002;
        let seed = fixture_id(MARKER_NAMESPACE, 0);
        let alternate = fixture_id(MARKER_NAMESPACE, 1);
        let red = fixture_id(MARKER_NAMESPACE, 2);
        let blue = fixture_id(MARKER_NAMESPACE, 3);
        let mut graph = TribleSet::new();
        let mut ordinal = 0u64;
        let components: Vec<Vec<Id>> = (0..component_count)
            .map(|_| {
                (0..ring_size)
                    .map(|_| {
                        let id = fixture_id(NODE_NAMESPACE, ordinal);
                        ordinal += 1;
                        id
                    })
                    .collect()
            })
            .collect();

        for component in &components {
            for (position, node) in component.iter().enumerate() {
                let source_class = if position % 4 == 0 {
                    &seed
                } else if position % 4 == 1 {
                    &alternate
                } else {
                    // Every node remains visible to the graph, but only half
                    // are selected by the source formula.
                    &red
                };
                insert_relation(&mut graph, node, &bench_schema::kind, source_class);
                insert_relation(
                    &mut graph,
                    node,
                    &bench_schema::kind,
                    if position % 2 == 0 { &red } else { &blue },
                );

                for offset in 1..=fanout {
                    insert_relation(
                        &mut graph,
                        node,
                        &bench_schema::p,
                        &component[(position + offset) % ring_size],
                    );
                    insert_relation(
                        &mut graph,
                        node,
                        &bench_schema::q,
                        &component[(position + fanout + offset) % ring_size],
                    );
                }
            }
        }

        Self {
            graph,
            components,
            seed,
            alternate,
            red,
            blue,
            fanout,
        }
    }

    fn finite_union_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                let offsets = match position % 4 {
                    0 => 1..=self.fanout,
                    1 => self.fanout + 1..=2 * self.fanout,
                    _ => continue,
                };
                for offset in offsets {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows.sort_unstable();
        rows
    }

    fn nested_formula_oracle(&self) -> Vec<Pair> {
        let ring_size = self.components[0].len();
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 > 1 {
                    continue;
                }
                for offset in 1..=2 * self.fanout {
                    rows.push((
                        source.to_inline(),
                        component[(position + offset) % ring_size].to_inline(),
                    ));
                }
            }
        }
        rows.sort_unstable();
        rows
    }

    fn cyclic_rpq_oracle(&self) -> Vec<Pair> {
        let mut rows = Vec::new();
        for component in &self.components {
            for source in component {
                for target in component {
                    // p includes the +1 ring edge, so every node reaches every
                    // node, including itself through a non-empty cycle.
                    rows.push((source.to_inline(), target.to_inline()));
                }
            }
        }
        rows.sort_unstable();
        rows
    }

    fn mixed_formula_rpq_oracle(&self) -> Vec<Pair> {
        let mut rows = Vec::new();
        for component in &self.components {
            for (position, source) in component.iter().enumerate() {
                if position % 4 > 1 {
                    continue;
                }
                for target in component {
                    rows.push((source.to_inline(), target.to_inline()));
                }
            }
        }
        rows.sort_unstable();
        rows
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Signature {
    rows: usize,
    checksum: u64,
}

#[inline]
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[inline]
fn pair_checksum((left, right): &Pair) -> u64 {
    let left = u64::from_be_bytes(left.raw[24..32].try_into().unwrap());
    let right = u64::from_be_bytes(right.raw[24..32].try_into().unwrap());
    mix64(left ^ right.rotate_left(29))
}

fn tally(rows: impl IntoIterator<Item = Pair>) -> Signature {
    let mut signature = Signature {
        rows: 0,
        checksum: 0,
    };
    for row in rows {
        signature.rows += 1;
        signature.checksum = signature.checksum.wrapping_add(pair_checksum(&row));
    }
    signature
}

fn finite_collect<S: TriblePattern>(store: &S, fixture: &Fixture) -> Vec<Pair> {
    finite_union_query!(store, fixture).collect()
}

fn finite_construct<S: TriblePattern>(store: &S, fixture: &Fixture) {
    drop(black_box(finite_union_query!(store, fixture)));
}

fn finite_pull<S: TriblePattern>(store: &S, fixture: &Fixture) -> (Duration, bool) {
    let mut query = finite_union_query!(store, fixture);
    let start = Instant::now();
    let found = black_box(query.next()).is_some();
    (start.elapsed(), found)
}

fn finite_prefix<S: TriblePattern>(store: &S, fixture: &Fixture, limit: usize) -> Signature {
    tally(finite_union_query!(store, fixture).take(limit))
}

fn nested_collect<S: TriblePattern>(store: &S, fixture: &Fixture) -> Vec<Pair> {
    nested_formula_query!(store, fixture).collect()
}

fn nested_construct<S: TriblePattern>(store: &S, fixture: &Fixture) {
    drop(black_box(nested_formula_query!(store, fixture)));
}

fn nested_pull<S: TriblePattern>(store: &S, fixture: &Fixture) -> (Duration, bool) {
    let mut query = nested_formula_query!(store, fixture);
    let start = Instant::now();
    let found = black_box(query.next()).is_some();
    (start.elapsed(), found)
}

fn nested_prefix<S: TriblePattern>(store: &S, fixture: &Fixture, limit: usize) -> Signature {
    tally(nested_formula_query!(store, fixture).take(limit))
}

fn rpq_collect(fixture: &Fixture) -> Vec<Pair> {
    cyclic_rpq_query!(fixture).collect()
}

fn rpq_construct(fixture: &Fixture) {
    drop(black_box(cyclic_rpq_query!(fixture)));
}

fn rpq_pull(fixture: &Fixture) -> (Duration, bool) {
    let mut query = cyclic_rpq_query!(fixture);
    let start = Instant::now();
    let found = black_box(query.next()).is_some();
    (start.elapsed(), found)
}

fn rpq_prefix(fixture: &Fixture, limit: usize) -> Signature {
    tally(cyclic_rpq_query!(fixture).take(limit))
}

fn mixed_collect<S: TriblePattern>(store: &S, fixture: &Fixture) -> Vec<Pair> {
    mixed_formula_rpq_query!(store, fixture).collect()
}

fn mixed_construct<S: TriblePattern>(store: &S, fixture: &Fixture) {
    drop(black_box(mixed_formula_rpq_query!(store, fixture)));
}

fn mixed_pull<S: TriblePattern>(store: &S, fixture: &Fixture) -> (Duration, bool) {
    let mut query = mixed_formula_rpq_query!(store, fixture);
    let start = Instant::now();
    let found = black_box(query.next()).is_some();
    (start.elapsed(), found)
}

fn mixed_prefix<S: TriblePattern>(store: &S, fixture: &Fixture, limit: usize) -> Signature {
    tally(mixed_formula_rpq_query!(store, fixture).take(limit))
}

fn percentile(samples: &[f64], quantile: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn timed(repetitions: usize, mut operation: impl FnMut()) -> Vec<f64> {
    let mut samples = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let start = Instant::now();
        operation();
        samples.push(start.elapsed().as_secs_f64());
    }
    samples
}

fn exact_check(mut actual: Vec<Pair>, expected: &[Pair], label: &str, backend: &str) {
    actual.sort_unstable();
    assert_eq!(
        actual, expected,
        "{label}/{backend}: relational oracle mismatch"
    );
}

fn bench_case(
    label: &str,
    backend: &str,
    expected: &[Pair],
    repetitions: usize,
    mut construct: impl FnMut(),
    mut pull: impl FnMut() -> (Duration, bool),
    mut prefix: impl FnMut(usize) -> Signature,
) {
    let expected_rows = expected.len();
    assert!(expected_rows > 0);

    // Warm every measured path once. The archive and fixture already exist,
    // so all results below are explicitly hot-cache query measurements.
    construct();
    assert!(pull().1);
    for limit in [1, 10, 100, 1_000, usize::MAX] {
        black_box(prefix(limit));
    }

    let construction = timed(repetitions, &mut construct);
    let mut pull_samples = Vec::with_capacity(repetitions);
    for _ in 0..repetitions {
        let (elapsed, found) = pull();
        assert!(found, "{label}/{backend}: first result disappeared");
        pull_samples.push(elapsed.as_secs_f64());
    }

    let mut points: Vec<usize> = [1, 10, 100, 1_000]
        .into_iter()
        .map(|point| point.min(expected_rows))
        .collect();
    points.sort_unstable();
    points.dedup();
    points.push(usize::MAX);
    let mut point_samples = vec![Vec::with_capacity(repetitions); points.len()];
    let mut point_signatures = vec![
        Signature {
            rows: 0,
            checksum: 0
        };
        points.len()
    ];
    for repetition in 0..repetitions {
        // Rotate prefix order so geometric growth and full-drain samples do
        // not receive a stable thermal/frequency advantage.
        for offset in 0..points.len() {
            let point_index = (repetition + offset) % points.len();
            let start = Instant::now();
            point_signatures[point_index] = black_box(prefix(points[point_index]));
            point_samples[point_index].push(start.elapsed().as_secs_f64());
        }
    }

    println!("\n{label} / {backend}  ({expected_rows} rows)");
    println!(
        "  construct+drop       p50 {:>10.3} us  p95 {:>10.3} us",
        percentile(&construction, 0.50) * 1e6,
        percentile(&construction, 0.95) * 1e6,
    );
    println!(
        "  pull->first          p50 {:>10.3} us  p95 {:>10.3} us",
        percentile(&pull_samples, 0.50) * 1e6,
        percentile(&pull_samples, 0.95) * 1e6,
    );

    for (point_index, &point) in points.iter().enumerate() {
        let expected_at_point = if point == usize::MAX {
            expected_rows
        } else {
            point
        };
        let signature = point_signatures[point_index];
        assert_eq!(
            signature.rows, expected_at_point,
            "{label}/{backend}: prefix count mismatch"
        );
        let p50 = percentile(&point_samples[point_index], 0.50);
        let p95 = percentile(&point_samples[point_index], 0.95);
        if point == usize::MAX {
            println!(
                "  full drain           p50 {:>10.3} ms  p95 {:>10.3} ms  \
                 {:>12.0} rows/s  {:>9.2} query/s  checksum {:#018x}",
                p50 * 1e3,
                p95 * 1e3,
                expected_rows as f64 / p50,
                1.0 / p50,
                signature.checksum,
            );
        } else {
            println!(
                "  end-to-end {:>4} rows p50 {:>10.3} us  p95 {:>10.3} us",
                point,
                p50 * 1e6,
                p95 * 1e6,
            );
        }
    }
}

fn parse_arg(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(not(engine_prefix_checkpoints))]
fn profile_cell<I, F>(label: &str, expected: &[Pair], repetitions: usize, mut make: F)
where
    I: Iterator<Item = Pair>,
    F: FnMut() -> I,
{
    let expected = tally(expected.iter().copied());
    println!("profile cell={label:?} repetitions={repetitions}");
    #[cfg(engine_allocation_probe)]
    allocation_probe::reset_peak_to_live();
    #[cfg(engine_allocation_probe)]
    let before = allocation_probe::Snapshot::now();
    for _ in 0..repetitions {
        assert_eq!(black_box(tally(make())), expected);
    }
    #[cfg(engine_allocation_probe)]
    allocation_probe::Snapshot::now().report_since(&before, label, repetitions);
}

#[cfg(not(engine_prefix_checkpoints))]
fn main() {
    let component_count = parse_arg(1, 32);
    let ring_size = parse_arg(2, 64);
    let fanout = parse_arg(3, 2);
    let repetitions = parse_arg(4, 21);
    assert!(repetitions >= 3, "use at least three repetitions");

    let fixture_start = Instant::now();
    let fixture = Fixture::new(component_count, ring_size, fanout);
    let fixture_elapsed = fixture_start.elapsed();
    let archive_start = Instant::now();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
    let archive_elapsed = archive_start.elapsed();

    let finite_expected = fixture.finite_union_oracle();
    let nested_expected = fixture.nested_formula_oracle();
    let rpq_expected = fixture.cyclic_rpq_oracle();
    let mixed_expected = fixture.mixed_formula_rpq_oracle();

    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    println!(
        "fixture: {component_count} components x {ring_size} nodes, fanout {fanout}, \
         {} tribles; built in {:?}; archive built in {:?} (excluded)",
        fixture.graph.len(),
        fixture_elapsed,
        archive_elapsed,
    );
    println!("samples: {repetitions}; hot cache; release profile");

    exact_check(
        finite_collect(&fixture.graph, &fixture),
        &finite_expected,
        "finite OR-of-AND",
        "TribleSet",
    );
    exact_check(
        finite_collect(&archive, &fixture),
        &finite_expected,
        "finite OR-of-AND",
        "SuccinctArchive",
    );
    exact_check(
        nested_collect(&fixture.graph, &fixture),
        &nested_expected,
        "recursive AND/OR",
        "TribleSet",
    );
    exact_check(
        nested_collect(&archive, &fixture),
        &nested_expected,
        "recursive AND/OR",
        "SuccinctArchive",
    );
    exact_check(
        rpq_collect(&fixture),
        &rpq_expected,
        "cyclic RPQ",
        "TribleSet",
    );
    exact_check(
        mixed_collect(&fixture.graph, &fixture),
        &mixed_expected,
        "formula + cyclic RPQ",
        "TribleSet sibling",
    );
    exact_check(
        mixed_collect(&archive, &fixture),
        &mixed_expected,
        "formula + cyclic RPQ",
        "SuccinctArchive sibling",
    );
    println!("oracle parity: all seven query/backend cells exact");

    if let Ok(cell) = std::env::var("ENGINE_PROFILE_CELL") {
        match cell.as_str() {
            "finite-trible" => profile_cell(&cell, &finite_expected, repetitions, || {
                finite_union_query!(&fixture.graph, &fixture)
            }),
            "finite-succinct" => profile_cell(&cell, &finite_expected, repetitions, || {
                finite_union_query!(&archive, &fixture)
            }),
            "formula-trible" => profile_cell(&cell, &nested_expected, repetitions, || {
                nested_formula_query!(&fixture.graph, &fixture)
            }),
            "formula-succinct" => profile_cell(&cell, &nested_expected, repetitions, || {
                nested_formula_query!(&archive, &fixture)
            }),
            "cyclic-trible" => profile_cell(&cell, &rpq_expected, repetitions, || {
                cyclic_rpq_query!(&fixture)
            }),
            "mixed-trible" => profile_cell(&cell, &mixed_expected, repetitions, || {
                mixed_formula_rpq_query!(&fixture.graph, &fixture)
            }),
            "mixed-succinct" => profile_cell(&cell, &mixed_expected, repetitions, || {
                mixed_formula_rpq_query!(&archive, &fixture)
            }),
            _ => panic!("unknown ENGINE_PROFILE_CELL {cell:?}"),
        }
        return;
    }

    bench_case(
        "finite OR-of-AND",
        "TribleSet",
        &finite_expected,
        repetitions,
        || finite_construct(&fixture.graph, &fixture),
        || finite_pull(&fixture.graph, &fixture),
        |limit| finite_prefix(&fixture.graph, &fixture, limit),
    );
    bench_case(
        "finite OR-of-AND",
        "SuccinctArchive",
        &finite_expected,
        repetitions,
        || finite_construct(&archive, &fixture),
        || finite_pull(&archive, &fixture),
        |limit| finite_prefix(&archive, &fixture, limit),
    );
    bench_case(
        "recursive AND/OR",
        "TribleSet",
        &nested_expected,
        repetitions,
        || nested_construct(&fixture.graph, &fixture),
        || nested_pull(&fixture.graph, &fixture),
        |limit| nested_prefix(&fixture.graph, &fixture, limit),
    );
    bench_case(
        "recursive AND/OR",
        "SuccinctArchive",
        &nested_expected,
        repetitions,
        || nested_construct(&archive, &fixture),
        || nested_pull(&archive, &fixture),
        |limit| nested_prefix(&archive, &fixture, limit),
    );
    bench_case(
        "cyclic RPQ",
        "TribleSet",
        &rpq_expected,
        repetitions,
        || rpq_construct(&fixture),
        || rpq_pull(&fixture),
        |limit| rpq_prefix(&fixture, limit),
    );
    bench_case(
        "formula + cyclic RPQ",
        "TribleSet sibling",
        &mixed_expected,
        repetitions,
        || mixed_construct(&fixture.graph, &fixture),
        || mixed_pull(&fixture.graph, &fixture),
        |limit| mixed_prefix(&fixture.graph, &fixture, limit),
    );
    bench_case(
        "formula + cyclic RPQ",
        "SuccinctArchive sibling",
        &mixed_expected,
        repetitions,
        || mixed_construct(&archive, &fixture),
        || mixed_pull(&archive, &fixture),
        |limit| mixed_prefix(&archive, &fixture, limit),
    );
}

#[cfg(all(
    engine_prefix_checkpoints,
    not(engine_counter_only),
    not(formula_delta_transport_probe)
))]
const PREFIX_CHECKPOINTS: [usize; 7] = [1, 10, 63, 64, 65, 100, 1_000];

#[cfg(all(
    engine_prefix_checkpoints,
    any(engine_counter_only, formula_delta_transport_probe)
))]
const PREFIX_CHECKPOINTS: [usize; 4] = [63, 64, 65, 100];

#[cfg(engine_prefix_checkpoints)]
#[derive(Clone, Copy, Debug)]
struct PrefixSample {
    checkpoint: usize,
    cumulative: Duration,
    fresh_time_to_n: Duration,
    fresh_drop_at_n: Duration,
    fresh_total: Duration,
}

#[cfg(engine_prefix_checkpoints)]
#[derive(Clone, Debug)]
struct PrefixEvidence {
    rows: usize,
    checksum: u64,
    ordered_digest: u64,
    last: Option<Pair>,
    distinct_sources: std::collections::BTreeSet<Inline<GenId>>,
}

#[cfg(engine_prefix_checkpoints)]
impl PrefixEvidence {
    fn new() -> Self {
        Self {
            rows: 0,
            checksum: 0,
            ordered_digest: 0x6A09_E667_F3BC_C909,
            last: None,
            distinct_sources: std::collections::BTreeSet::new(),
        }
    }

    fn observe(&mut self, row: Pair) {
        let row_hash = pair_order_hash(&row);
        self.rows += 1;
        self.checksum = self.checksum.wrapping_add(pair_checksum(&row));
        self.ordered_digest = mix64(
            self.ordered_digest ^ row_hash ^ (self.rows as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        self.distinct_sources.insert(row.0);
        self.last = Some(row);
    }
}

#[cfg(engine_prefix_checkpoints)]
fn pair_order_hash((left, right): &Pair) -> u64 {
    left.raw
        .chunks_exact(8)
        .chain(right.raw.chunks_exact(8))
        .enumerate()
        .fold(0xBB67_AE85_84CA_A73B, |digest, (index, chunk)| {
            let word = u64::from_be_bytes(chunk.try_into().unwrap());
            mix64(digest ^ word.rotate_left((index * 7) as u32))
        })
}

#[cfg(engine_prefix_checkpoints)]
fn inline_hex(value: &Inline<GenId>) -> String {
    use std::fmt::Write;

    let mut rendered = String::with_capacity(64);
    for byte in value.raw {
        write!(&mut rendered, "{byte:02x}").unwrap();
    }
    rendered
}

#[cfg(engine_prefix_checkpoints)]
fn render_pair(pair: Option<Pair>) -> String {
    match pair {
        Some((source, target)) => format!("{}:{}", inline_hex(&source), inline_hex(&target)),
        None => "none".to_owned(),
    }
}

#[cfg(engine_prefix_checkpoints)]
fn checkpoint_evidence<I>(label: &str, query: I) -> Vec<PrefixEvidence>
where
    I: Iterator<Item = Pair>,
{
    let mut snapshots = Vec::with_capacity(PREFIX_CHECKPOINTS.len());
    let mut evidence = PrefixEvidence::new();
    let mut checkpoint_index = 0;

    for row in query {
        evidence.observe(row);
        if evidence.rows == PREFIX_CHECKPOINTS[checkpoint_index] {
            snapshots.push(evidence.clone());
            checkpoint_index += 1;
            if checkpoint_index == PREFIX_CHECKPOINTS.len() {
                break;
            }
        }
    }

    assert_eq!(
        snapshots.len(),
        PREFIX_CHECKPOINTS.len(),
        "{label}: iterator ended before the final prefix checkpoint"
    );
    snapshots
}

#[cfg(all(engine_prefix_checkpoints, engine_counter_only))]
fn print_counter_evidence<I>(label: &str, query: I)
where
    I: Iterator<Item = Pair>,
{
    for (checkpoint, evidence) in PREFIX_CHECKPOINTS
        .into_iter()
        .zip(checkpoint_evidence(label, query))
    {
        println!(
            "evidence cell={label:?} checkpoint={checkpoint} count={} checksum={:#018x} \
             ordered_digest={:#018x} last_pair={} distinct_sources={}",
            evidence.rows,
            evidence.checksum,
            evidence.ordered_digest,
            render_pair(evidence.last),
            evidence.distinct_sources.len(),
        );
    }
}

#[cfg(engine_prefix_checkpoints)]
fn consume_exact<I>(label: &str, mut query: I, count: usize) -> Signature
where
    I: Iterator<Item = Pair>,
{
    let signature = tally(query.by_ref().take(count));
    assert_eq!(
        signature.rows, count,
        "{label}: iterator ended before checkpoint {count}"
    );
    black_box(signature)
}

#[cfg(engine_prefix_checkpoints)]
fn bench_prefix_cell<I, F>(label: &str, expected: &[Pair], repetitions: usize, mut make: F)
where
    I: Iterator<Item = Pair>,
    F: FnMut() -> I,
{
    assert!(
        expected.len() >= *PREFIX_CHECKPOINTS.last().unwrap(),
        "{label}: fixture has {} rows, but the diagnostic requires at least {}",
        expected.len(),
        PREFIX_CHECKPOINTS.last().unwrap(),
    );

    // Warm both timing shapes without retaining their observations. The
    // fixture, archive, and full sorted oracle were already built and checked.
    consume_exact(label, make(), *PREFIX_CHECKPOINTS.last().unwrap());
    for checkpoint in PREFIX_CHECKPOINTS {
        consume_exact(label, make(), checkpoint);
    }

    let evidence = checkpoint_evidence(label, make());
    for (checkpoint, evidence) in PREFIX_CHECKPOINTS.into_iter().zip(&evidence) {
        println!(
            "evidence cell={label:?} checkpoint={checkpoint} count={} checksum={:#018x} \
             ordered_digest={:#018x} last_pair={} distinct_sources={}",
            evidence.rows,
            evidence.checksum,
            evidence.ordered_digest,
            render_pair(evidence.last),
            evidence.distinct_sources.len(),
        );
    }

    let mut samples = Vec::with_capacity(repetitions * PREFIX_CHECKPOINTS.len());
    for repetition in 0..repetitions {
        // One iterator supplies every cumulative timestamp in this repetition.
        // Each timestamp is captured while the exact remainder is still live;
        // dropping that remainder happens only after checkpoint 1,000.
        let cumulative_start = Instant::now();
        let mut cumulative_query = make();
        let mut cumulative_signature = Signature {
            rows: 0,
            checksum: 0,
        };
        let mut cumulative = [Duration::ZERO; PREFIX_CHECKPOINTS.len()];
        let mut checkpoint_index = 0;
        while checkpoint_index < PREFIX_CHECKPOINTS.len() {
            let row = cumulative_query.next().unwrap_or_else(|| {
                panic!(
                    "{label}: cumulative iterator ended before checkpoint {}",
                    PREFIX_CHECKPOINTS[checkpoint_index]
                )
            });
            cumulative_signature.rows += 1;
            cumulative_signature.checksum = cumulative_signature
                .checksum
                .wrapping_add(pair_checksum(&row));
            if cumulative_signature.rows == PREFIX_CHECKPOINTS[checkpoint_index] {
                cumulative[checkpoint_index] = cumulative_start.elapsed();
                checkpoint_index += 1;
            }
        }
        black_box(cumulative_signature);
        drop(cumulative_query);

        // Rotate the fresh-query checkpoints to avoid giving one fixed prefix
        // a permanent thermal/frequency position within every repetition.
        for offset in 0..PREFIX_CHECKPOINTS.len() {
            let point_index = (repetition + offset) % PREFIX_CHECKPOINTS.len();
            let checkpoint = PREFIX_CHECKPOINTS[point_index];
            let fresh_start = Instant::now();
            let mut fresh_query = make();
            let fresh_signature = tally(fresh_query.by_ref().take(checkpoint));
            assert_eq!(
                fresh_signature.rows, checkpoint,
                "{label}: fresh iterator ended before checkpoint {checkpoint}"
            );
            black_box(fresh_signature);
            let fresh_time_to_n = fresh_start.elapsed();
            let drop_start = Instant::now();
            drop(fresh_query);
            let fresh_drop_at_n = drop_start.elapsed();
            let fresh_total = fresh_start.elapsed();
            samples.push((
                repetition,
                PrefixSample {
                    checkpoint,
                    cumulative: cumulative[point_index],
                    fresh_time_to_n,
                    fresh_drop_at_n,
                    fresh_total,
                },
            ));
        }
    }

    samples.sort_unstable_by_key(|(repetition, sample)| (*repetition, sample.checkpoint));
    for (repetition, sample) in &samples {
        println!(
            "raw cell={label:?} repetition={repetition} checkpoint={} cumulative_ns={} \
             fresh_time_to_n_ns={} fresh_drop_at_n_ns={} fresh_total_ns={}",
            sample.checkpoint,
            sample.cumulative.as_nanos(),
            sample.fresh_time_to_n.as_nanos(),
            sample.fresh_drop_at_n.as_nanos(),
            sample.fresh_total.as_nanos(),
        );
    }

    for checkpoint in PREFIX_CHECKPOINTS {
        let at_checkpoint: Vec<_> = samples
            .iter()
            .filter_map(|(_, sample)| (sample.checkpoint == checkpoint).then_some(*sample))
            .collect();
        let durations = |project: fn(PrefixSample) -> Duration| {
            at_checkpoint
                .iter()
                .map(|sample| project(*sample).as_secs_f64())
                .collect::<Vec<_>>()
        };
        let cumulative = durations(|sample| sample.cumulative);
        let fresh_time_to_n = durations(|sample| sample.fresh_time_to_n);
        let fresh_drop_at_n = durations(|sample| sample.fresh_drop_at_n);
        let fresh_total = durations(|sample| sample.fresh_total);
        println!(
            "summary cell={label:?} checkpoint={checkpoint} \
             cumulative_p50_us={:.3} cumulative_p95_us={:.3} \
             fresh_time_to_n_p50_us={:.3} fresh_time_to_n_p95_us={:.3} \
             fresh_drop_at_n_p50_us={:.3} fresh_drop_at_n_p95_us={:.3} \
             fresh_total_p50_us={:.3} fresh_total_p95_us={:.3}",
            percentile(&cumulative, 0.50) * 1e6,
            percentile(&cumulative, 0.95) * 1e6,
            percentile(&fresh_time_to_n, 0.50) * 1e6,
            percentile(&fresh_time_to_n, 0.95) * 1e6,
            percentile(&fresh_drop_at_n, 0.50) * 1e6,
            percentile(&fresh_drop_at_n, 0.95) * 1e6,
            percentile(&fresh_total, 0.50) * 1e6,
            percentile(&fresh_total, 0.95) * 1e6,
        );
    }
}

#[cfg(all(engine_prefix_checkpoints, engine_current_residual))]
fn residual_checkpoint_stats<I, F>(label: &str, mut query: I, snapshot: F)
where
    I: Iterator<Item = Pair>,
    F: Fn(&I) -> (usize, String),
{
    let mut rows = 0;
    let mut checkpoint_index = 0;
    while checkpoint_index < PREFIX_CHECKPOINTS.len() {
        query.next().unwrap_or_else(|| {
            panic!(
                "{label}: residual iterator ended before stats checkpoint {}",
                PREFIX_CHECKPOINTS[checkpoint_index]
            )
        });
        rows += 1;
        if rows == PREFIX_CHECKPOINTS[checkpoint_index] {
            let (current_width, stats) = snapshot(&query);
            println!(
                "residual_stats cell={label:?} checkpoint={} current_width={} stats={}",
                PREFIX_CHECKPOINTS[checkpoint_index], current_width, stats,
            );
            checkpoint_index += 1;
        }
    }

    for _ in query.by_ref() {
        rows += 1;
    }
    let (current_width, stats) = snapshot(&query);
    println!(
        "residual_stats cell={label:?} checkpoint=full rows={rows} current_width={current_width} \
         stats={stats}",
    );
}

#[cfg(all(engine_prefix_checkpoints, not(formula_delta_transport_probe)))]
fn main() {
    let component_count = parse_arg(1, 32);
    let ring_size = parse_arg(2, 64);
    let fanout = parse_arg(3, 2);
    let repetitions = parse_arg(4, 21);
    assert!(repetitions >= 3, "use at least three repetitions");

    #[cfg(not(engine_counter_only))]
    let (fixture, fixture_elapsed, archive, archive_elapsed) = {
        let fixture_start = Instant::now();
        let fixture = Fixture::new(component_count, ring_size, fanout);
        let fixture_elapsed = fixture_start.elapsed();
        let archive_start = Instant::now();
        let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
        let archive_elapsed = archive_start.elapsed();
        (fixture, fixture_elapsed, archive, archive_elapsed)
    };
    #[cfg(engine_counter_only)]
    let (fixture, archive) = {
        let fixture = Fixture::new(component_count, ring_size, fanout);
        let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
        (fixture, archive)
    };

    let rpq_expected = fixture.cyclic_rpq_oracle();
    let mixed_expected = fixture.mixed_formula_rpq_oracle();

    println!("diagnostic: source-identical prefix checkpoints");
    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    #[cfg(not(engine_counter_only))]
    println!(
        "fixture: {component_count} components x {ring_size} nodes, fanout {fanout}, \
         {} tribles; built in {:?}; archive built in {:?} (excluded)",
        fixture.graph.len(),
        fixture_elapsed,
        archive_elapsed,
    );
    #[cfg(engine_counter_only)]
    println!(
        "fixture: {component_count} components x {ring_size} nodes, fanout {fanout}, \
         {} tribles",
        fixture.graph.len(),
    );
    #[cfg(not(engine_counter_only))]
    println!("samples: {repetitions}; hot cache; release profile");
    #[cfg(engine_counter_only)]
    println!("mode: counter-only; lowering: {COUNTER_LOWERING_LABEL}; no timed samples");
    println!("checkpoints: {PREFIX_CHECKPOINTS:?}");

    #[cfg(not(engine_counter_only))]
    {
        exact_check(
            rpq_collect(&fixture),
            &rpq_expected,
            "cyclic RPQ",
            "TribleSet",
        );
        exact_check(
            mixed_collect(&fixture.graph, &fixture),
            &mixed_expected,
            "formula + cyclic RPQ",
            "TribleSet sibling",
        );
        exact_check(
            mixed_collect(&archive, &fixture),
            &mixed_expected,
            "formula + cyclic RPQ",
            "SuccinctArchive sibling",
        );
    }
    #[cfg(engine_counter_only)]
    {
        exact_check(
            cyclic_rpq_query!(&fixture)
                .solve_residual_state_lazy_with(COUNTER_LOWERING)
                .collect(),
            &rpq_expected,
            "cyclic RPQ",
            "TribleSet",
        );
        exact_check(
            mixed_formula_rpq_query!(&fixture.graph, &fixture)
                .solve_residual_state_lazy_with(COUNTER_LOWERING)
                .collect(),
            &mixed_expected,
            "formula + cyclic RPQ",
            "TribleSet sibling",
        );
        exact_check(
            mixed_formula_rpq_query!(&archive, &fixture)
                .solve_residual_state_lazy_with(COUNTER_LOWERING)
                .collect(),
            &mixed_expected,
            "formula + cyclic RPQ",
            "SuccinctArchive sibling",
        );
    }
    println!("oracle parity: all three prefix-diagnostic cells exact");

    #[cfg(engine_counter_only)]
    {
        print_counter_evidence(
            "cyclic RPQ / TribleSet",
            cyclic_rpq_query!(&fixture).solve_residual_state_lazy_with(COUNTER_LOWERING),
        );
        print_counter_evidence(
            "formula + cyclic RPQ / TribleSet sibling",
            mixed_formula_rpq_query!(&fixture.graph, &fixture)
                .solve_residual_state_lazy_with(COUNTER_LOWERING),
        );
        print_counter_evidence(
            "formula + cyclic RPQ / SuccinctArchive sibling",
            mixed_formula_rpq_query!(&archive, &fixture)
                .solve_residual_state_lazy_with(COUNTER_LOWERING),
        );
    }

    #[cfg(engine_current_residual)]
    {
        use triblespace::core::query::residual::ResidualLowering;

        #[cfg(not(engine_counter_only))]
        const PREFIX_LOWERING: ResidualLowering = ResidualLowering::FULL;
        #[cfg(engine_counter_only)]
        const PREFIX_LOWERING: ResidualLowering = COUNTER_LOWERING;

        residual_checkpoint_stats(
            "cyclic RPQ / TribleSet",
            cyclic_rpq_query!(&fixture).solve_residual_state_lazy_with(PREFIX_LOWERING),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
        residual_checkpoint_stats(
            "formula + cyclic RPQ / TribleSet sibling",
            mixed_formula_rpq_query!(&fixture.graph, &fixture)
                .solve_residual_state_lazy_with(PREFIX_LOWERING),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
        residual_checkpoint_stats(
            "formula + cyclic RPQ / SuccinctArchive sibling",
            mixed_formula_rpq_query!(&archive, &fixture)
                .solve_residual_state_lazy_with(PREFIX_LOWERING),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
    }

    #[cfg(not(engine_counter_only))]
    {
        bench_prefix_cell("cyclic RPQ / TribleSet", &rpq_expected, repetitions, || {
            cyclic_rpq_query!(&fixture)
        });
        bench_prefix_cell(
            "formula + cyclic RPQ / TribleSet sibling",
            &mixed_expected,
            repetitions,
            || mixed_formula_rpq_query!(&fixture.graph, &fixture),
        );
        bench_prefix_cell(
            "formula + cyclic RPQ / SuccinctArchive sibling",
            &mixed_expected,
            repetitions,
            || mixed_formula_rpq_query!(&archive, &fixture),
        );
    }
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_order_receipt(rows: &[Pair]) -> (Signature, u64) {
    let signature = tally(rows.iter().copied());
    let ordered_digest =
        rows.iter()
            .enumerate()
            .fold(0x6A09_E667_F3BC_C909, |digest, (index, row)| {
                mix64(
                    digest
                        ^ pair_checksum(row)
                        ^ ((index + 1) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                )
            });
    (signature, ordered_digest)
}

#[cfg(formula_delta_transport_probe)]
fn format_formula_transport_probe_stats(stats: &ResidualStateStats) -> String {
    format!(
        "width_increases={} activations_completed={} source_pages={} source_cohorts={} \
         source_examined={} transition_pages={} transition_cohorts={} transition_examined={} \
         state_reentries={} rows_reentered={} bucket_merges={} rows_merged={} \
         formula_filings={} formula_bucket_merges={} formula_state_reentries={} \
         formula_rows_reentered={} formula_delta_attempts={} support_attempts={} \
         propose_attempts={} confirm_attempts={} forced_stable_declines={} \
         forced_support={} forced_propose={} forced_confirm={} natural_stable_declines={} \
         formula_program_selected={} support_program_selected={} \
         propose_program_selected={} confirm_program_selected={} formula_program_seeded={} \
         formula_program_seeded_parents={} support_program_seeded={} \
         support_program_seeded_parents={} propose_program_seeded={} \
         propose_program_seeded_parents={} confirm_program_seeded={} \
         confirm_program_seeded_parents={} formula_source_seeded={} \
         formula_source_seeded_parents={} formula_legacy_seeded={} \
         formula_legacy_seeded_parents={} formula_stable_support_calls={} \
         formula_stable_support_rows={} formula_stable_propose_calls={} \
         formula_stable_propose_rows={} formula_stable_propose_candidates={} \
         formula_stable_confirm_calls={} formula_stable_confirm_rows={} \
         formula_stable_confirm_candidates_in={} formula_stable_confirm_candidates_out={} \
         quiescent_formula_complete_actions={} propose_calls={} propose_rows={} \
         confirm_calls={} confirm_rows={} candidates_confirmed={} \
         prepared_receipts={} prepared_rows={} prepared_candidates={} prepared_atoms={} \
         prepared_equal_key_appends={} prepared_atoms_committed={} \
         prepared_atoms_consumed={} prepared_atoms_owned={} prepared_split_flushes={} \
         prepared_probe_one_pops={} prepared_cohort_pops={} prepared_identity_checks={}",
        stats.width_increases,
        stats.delta_activations_completed,
        stats.delta_source_pages,
        stats.delta_source_cohorts,
        stats.delta_source_candidates_examined,
        stats.delta_transition_pages,
        stats.delta_transition_cohorts,
        stats.delta_transition_candidates_examined,
        stats.state_reentries,
        stats.rows_reentered,
        stats.bucket_merges,
        stats.rows_merged,
        stats.probe_formula_filings,
        stats.probe_formula_bucket_merges,
        stats.probe_formula_state_reentries,
        stats.probe_formula_rows_reentered,
        stats.probe_formula_delta_attempts,
        stats.probe_formula_support_attempts,
        stats.probe_formula_propose_attempts,
        stats.probe_formula_confirm_attempts,
        stats.probe_formula_forced_stable_declines,
        stats.probe_formula_forced_stable_support,
        stats.probe_formula_forced_stable_propose,
        stats.probe_formula_forced_stable_confirm,
        stats.probe_formula_natural_stable_declines,
        stats.probe_formula_program_selected,
        stats.probe_formula_support_program_selected,
        stats.probe_formula_propose_program_selected,
        stats.probe_formula_confirm_program_selected,
        stats.probe_formula_program_seeded,
        stats.probe_formula_program_seeded_parents,
        stats.probe_formula_support_program_seeded,
        stats.probe_formula_support_program_seeded_parents,
        stats.probe_formula_propose_program_seeded,
        stats.probe_formula_propose_program_seeded_parents,
        stats.probe_formula_confirm_program_seeded,
        stats.probe_formula_confirm_program_seeded_parents,
        stats.probe_formula_source_seeded,
        stats.probe_formula_source_seeded_parents,
        stats.probe_formula_legacy_seeded,
        stats.probe_formula_legacy_seeded_parents,
        stats.probe_formula_stable_support_calls,
        stats.probe_formula_stable_support_rows,
        stats.probe_formula_stable_propose_calls,
        stats.probe_formula_stable_propose_rows,
        stats.probe_formula_stable_propose_candidates,
        stats.probe_formula_stable_confirm_calls,
        stats.probe_formula_stable_confirm_rows,
        stats.probe_formula_stable_confirm_candidates_in,
        stats.probe_formula_stable_confirm_candidates_out,
        stats.delta_quiescent_formula_complete_actions,
        stats.propose_calls,
        stats.propose_rows,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.candidates_confirmed,
        stats.probe_formula_prepared_receipts,
        stats.probe_formula_prepared_rows,
        stats.probe_formula_prepared_candidates,
        stats.probe_formula_prepared_atoms,
        stats.probe_formula_prepared_equal_key_appends,
        stats.probe_formula_prepared_atoms_committed,
        stats.probe_formula_prepared_atoms_consumed,
        formula_prepared_owned_atoms(stats),
        stats.probe_formula_prepared_split_flushes,
        stats.probe_formula_prepared_probe_one_pops,
        stats.probe_formula_prepared_cohort_pops,
        stats.probe_formula_prepared_identity_checks,
    )
}

#[cfg(formula_delta_transport_probe)]
fn formula_prepared_owned_atoms(stats: &ResidualStateStats) -> usize {
    stats
        .probe_formula_prepared_atoms
        .checked_sub(
            stats
                .probe_formula_prepared_atoms_committed
                .checked_add(stats.probe_formula_prepared_atoms_consumed)
                .expect("prepared Formula settled-atom count overflow"),
        )
        .expect("prepared Formula committed or consumed more atoms than it created")
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct FormulaStageReceipt([usize; 29]);

#[cfg(formula_delta_transport_probe)]
fn formula_stage_receipt(stats: &ResidualStateStats) -> FormulaStageReceipt {
    FormulaStageReceipt([
        stats.probe_formula_delta_attempts,
        stats.probe_formula_support_attempts,
        stats.probe_formula_propose_attempts,
        stats.probe_formula_confirm_attempts,
        stats.probe_formula_forced_stable_declines,
        stats.probe_formula_forced_stable_support,
        stats.probe_formula_forced_stable_propose,
        stats.probe_formula_forced_stable_confirm,
        stats.probe_formula_natural_stable_declines,
        stats.probe_formula_program_selected,
        stats.probe_formula_support_program_selected,
        stats.probe_formula_propose_program_selected,
        stats.probe_formula_confirm_program_selected,
        stats.probe_formula_program_seeded,
        stats.probe_formula_program_seeded_parents,
        stats.probe_formula_support_program_seeded,
        stats.probe_formula_support_program_seeded_parents,
        stats.probe_formula_propose_program_seeded,
        stats.probe_formula_propose_program_seeded_parents,
        stats.probe_formula_confirm_program_seeded,
        stats.probe_formula_confirm_program_seeded_parents,
        stats.probe_formula_source_seeded,
        stats.probe_formula_source_seeded_parents,
        stats.probe_formula_legacy_seeded,
        stats.probe_formula_legacy_seeded_parents,
        stats.probe_formula_stable_support_calls,
        stats.probe_formula_stable_support_rows,
        stats.probe_formula_stable_propose_calls,
        stats.probe_formula_stable_confirm_calls,
    ])
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct FormulaCandidateReceipt([usize; 17]);

#[cfg(formula_delta_transport_probe)]
fn formula_candidate_receipt(stats: &ResidualStateStats) -> FormulaCandidateReceipt {
    FormulaCandidateReceipt([
        stats.probe_formula_stable_propose_rows,
        stats.probe_formula_stable_propose_candidates,
        stats.probe_formula_stable_confirm_rows,
        stats.probe_formula_stable_confirm_candidates_in,
        stats.probe_formula_stable_confirm_candidates_out,
        stats.propose_calls,
        stats.support_calls,
        stats.confirm_calls,
        stats.propose_rows,
        stats.support_rows,
        stats.confirm_rows,
        stats.candidates_proposed,
        stats.candidates_confirmed,
        stats.max_propose_candidates,
        stats.max_confirm_candidates,
        stats.max_propose_rows,
        stats.max_confirm_rows,
    ])
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct FormulaTransitionReceipt([usize; 23]);

#[cfg(formula_delta_transport_probe)]
fn formula_transition_receipt(stats: &ResidualStateStats) -> FormulaTransitionReceipt {
    FormulaTransitionReceipt([
        stats.delta_source_pages,
        stats.delta_source_cohorts,
        stats.max_delta_source_cohort,
        stats.delta_source_candidates_examined,
        stats.delta_source_roots,
        stats.delta_transition_pages,
        stats.delta_transition_cohorts,
        stats.max_delta_transition_cohort,
        stats.delta_transition_candidates_examined,
        stats.delta_transition_dead_pages,
        stats.delta_source_direct_candidates,
        stats.delta_source_dead_pages,
        stats.delta_source_negative_steps,
        stats.delta_transition_negative_steps,
        stats.delta_activations_completed,
        stats.delta_activation_width_increases,
        stats.delta_handoff_probe_pops,
        stats.delta_quiescent_formula_complete_actions,
        stats.delta_quiescent_formula_complete_parents,
        stats.delta_quiescent_formula_complete_raw_occurrences,
        stats.delta_quiescent_formula_exact_empty_or_transfers,
        stats.delta_quiescent_formula_complete_declines,
        stats.width_increases,
    ])
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FormulaFilingReceipt {
    filings: usize,
    merges: usize,
    reentries: usize,
    rows_reentered: usize,
}

#[cfg(formula_delta_transport_probe)]
fn formula_filing_receipt(stats: &ResidualStateStats) -> FormulaFilingReceipt {
    FormulaFilingReceipt {
        filings: stats.probe_formula_filings,
        merges: stats.probe_formula_bucket_merges,
        reentries: stats.probe_formula_state_reentries,
        rows_reentered: stats.probe_formula_rows_reentered,
    }
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct FormulaPreparedReceipt {
    receipts: usize,
    rows: usize,
    candidates: usize,
    atoms: usize,
    equal_key_appends: usize,
    committed: usize,
    consumed: usize,
    owned: usize,
    split_flushes: usize,
    probe_one_pops: usize,
    cohort_pops: usize,
    identity_checks: usize,
}

#[cfg(formula_delta_transport_probe)]
fn formula_prepared_receipt(stats: &ResidualStateStats) -> FormulaPreparedReceipt {
    FormulaPreparedReceipt {
        receipts: stats.probe_formula_prepared_receipts,
        rows: stats.probe_formula_prepared_rows,
        candidates: stats.probe_formula_prepared_candidates,
        atoms: stats.probe_formula_prepared_atoms,
        equal_key_appends: stats.probe_formula_prepared_equal_key_appends,
        committed: stats.probe_formula_prepared_atoms_committed,
        consumed: stats.probe_formula_prepared_atoms_consumed,
        owned: formula_prepared_owned_atoms(stats),
        split_flushes: stats.probe_formula_prepared_split_flushes,
        probe_one_pops: stats.probe_formula_prepared_probe_one_pops,
        cohort_pops: stats.probe_formula_prepared_cohort_pops,
        identity_checks: stats.probe_formula_prepared_identity_checks,
    }
}

#[cfg(formula_delta_transport_probe)]
fn expected_formula_physical_receipts(
    backend: &str,
    limit: Option<usize>,
) -> (
    FormulaFilingReceipt,
    FormulaFilingReceipt,
    FormulaPreparedReceipt,
) {
    let succinct = backend.starts_with("SuccinctArchive");
    match (succinct, limit) {
        (false, Some(63 | 64)) => (
            FormulaFilingReceipt {
                filings: 20,
                merges: 0,
                reentries: 0,
                rows_reentered: 0,
            },
            FormulaFilingReceipt {
                filings: 5,
                merges: 0,
                reentries: 0,
                rows_reentered: 0,
            },
            FormulaPreparedReceipt {
                receipts: 20,
                rows: 20,
                candidates: 0,
                atoms: 20,
                equal_key_appends: 0,
                committed: 5,
                consumed: 15,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 2,
                cohort_pops: 13,
                identity_checks: 20,
            },
        ),
        (false, Some(65 | 100)) => (
            FormulaFilingReceipt {
                filings: 60,
                merges: 30,
                reentries: 10,
                rows_reentered: 160,
            },
            FormulaFilingReceipt {
                filings: 6,
                merges: 0,
                reentries: 1,
                rows_reentered: 30,
            },
            FormulaPreparedReceipt {
                receipts: 60,
                rows: 210,
                candidates: 0,
                atoms: 210,
                equal_key_appends: 30,
                committed: 35,
                consumed: 175,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 3,
                cohort_pops: 22,
                identity_checks: 60,
            },
        ),
        (false, None) => (
            FormulaFilingReceipt {
                filings: 64,
                merges: 30,
                reentries: 14,
                rows_reentered: 280,
            },
            FormulaFilingReceipt {
                filings: 10,
                merges: 0,
                reentries: 5,
                rows_reentered: 150,
            },
            FormulaPreparedReceipt {
                receipts: 64,
                rows: 330,
                candidates: 0,
                atoms: 330,
                equal_key_appends: 30,
                committed: 155,
                consumed: 175,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 3,
                cohort_pops: 22,
                identity_checks: 64,
            },
        ),
        (true, Some(63 | 64)) => (
            FormulaFilingReceipt {
                filings: 20,
                merges: 0,
                reentries: 0,
                rows_reentered: 0,
            },
            FormulaFilingReceipt {
                filings: 3,
                merges: 0,
                reentries: 0,
                rows_reentered: 0,
            },
            FormulaPreparedReceipt {
                receipts: 20,
                rows: 20,
                candidates: 0,
                atoms: 20,
                equal_key_appends: 0,
                committed: 3,
                consumed: 17,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 6,
                cohort_pops: 11,
                identity_checks: 20,
            },
        ),
        (true, Some(65 | 100)) => (
            FormulaFilingReceipt {
                filings: 60,
                merges: 30,
                reentries: 10,
                rows_reentered: 100,
            },
            FormulaFilingReceipt {
                filings: 4,
                merges: 0,
                reentries: 1,
                rows_reentered: 30,
            },
            FormulaPreparedReceipt {
                receipts: 60,
                rows: 150,
                candidates: 0,
                atoms: 150,
                equal_key_appends: 30,
                committed: 33,
                consumed: 117,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 9,
                cohort_pops: 18,
                identity_checks: 60,
            },
        ),
        (true, None) => (
            FormulaFilingReceipt {
                filings: 128,
                merges: 87,
                reentries: 21,
                rows_reentered: 223,
            },
            FormulaFilingReceipt {
                filings: 10,
                merges: 0,
                reentries: 7,
                rows_reentered: 203,
            },
            FormulaPreparedReceipt {
                receipts: 128,
                rows: 330,
                candidates: 0,
                atoms: 330,
                equal_key_appends: 87,
                committed: 206,
                consumed: 124,
                owned: 0,
                split_flushes: 0,
                probe_one_pops: 12,
                cohort_pops: 22,
                identity_checks: 128,
            },
        ),
        (_, Some(other)) => panic!("unregistered Formula checkpoint {other}"),
    }
}

#[cfg(formula_delta_transport_probe)]
struct FormulaProbeObservation {
    rows: Vec<Pair>,
    plan: triblespace::core::query::residual::FormulaDeltaTransportProbePlanReceipt,
    width: usize,
    stats: ResidualStateStats,
}

#[cfg(formula_delta_transport_probe)]
fn formula_probe_observe<S: TriblePattern>(
    store: &S,
    fixture: &Fixture,
    mode: FormulaTransportProbeMode,
    limit: Option<usize>,
) -> FormulaProbeObservation {
    let mut query = formula_transport_probe_query!(store, fixture, mode);
    let plan = query.formula_delta_transport_probe_plan_receipt();
    let rows: Vec<Pair> = match limit {
        Some(limit) => query.by_ref().take(limit).collect(),
        None => query.by_ref().collect(),
    };
    if let Some(limit) = limit {
        assert_eq!(rows.len(), limit, "query ended before checkpoint {limit}");
    }
    FormulaProbeObservation {
        rows,
        plan,
        width: query.current_width(),
        stats: query.stats().clone(),
    }
}

#[cfg(formula_delta_transport_probe)]
fn assert_g_transport_seam(backend: &str, stats: &ResidualStateStats) {
    assert_eq!(
        stats.probe_formula_delta_attempts,
        stats.probe_formula_support_attempts
            + stats.probe_formula_propose_attempts
            + stats.probe_formula_confirm_attempts,
    );
    assert_eq!(stats.probe_formula_forced_stable_support, 0);
    assert!(stats.probe_formula_support_attempts > 0);
    assert!(stats.probe_formula_propose_attempts > 0);
    assert!(stats.probe_formula_confirm_attempts > 0);
    assert_eq!(
        stats.probe_formula_forced_stable_propose,
        stats.probe_formula_propose_attempts,
    );
    assert_eq!(
        stats.probe_formula_forced_stable_confirm,
        stats.probe_formula_confirm_attempts,
    );
    assert_eq!(stats.probe_formula_propose_program_selected, 0);
    assert_eq!(stats.probe_formula_confirm_program_selected, 0);
    assert_eq!(stats.probe_formula_propose_program_seeded, 0);
    assert_eq!(stats.probe_formula_confirm_program_seeded, 0);
    if backend.starts_with("SuccinctArchive") {
        assert_eq!(stats.probe_formula_natural_stable_declines, 0);
        assert_eq!(
            stats.probe_formula_support_program_selected,
            stats.probe_formula_support_attempts,
        );
        assert_eq!(
            stats.probe_formula_support_program_seeded,
            stats.probe_formula_support_program_selected,
        );
        assert_eq!(stats.probe_formula_stable_support_calls, 0);
    } else {
        assert_eq!(stats.probe_formula_support_program_selected, 0);
        assert_eq!(stats.probe_formula_support_program_seeded, 0);
        assert_eq!(
            stats.probe_formula_natural_stable_declines,
            stats.probe_formula_support_attempts,
        );
        assert_eq!(
            stats.probe_formula_stable_support_calls,
            stats.probe_formula_support_attempts,
        );
    }
}

#[cfg(formula_delta_transport_probe)]
fn assert_stable_transport_seam(stats: &ResidualStateStats) {
    assert_eq!(
        stats.probe_formula_delta_attempts,
        stats.probe_formula_support_attempts
            + stats.probe_formula_propose_attempts
            + stats.probe_formula_confirm_attempts,
    );
    assert!(stats.probe_formula_support_attempts > 0);
    assert!(stats.probe_formula_propose_attempts > 0);
    assert!(stats.probe_formula_confirm_attempts > 0);
    assert_eq!(
        stats.probe_formula_forced_stable_declines,
        stats.probe_formula_delta_attempts,
    );
    assert_eq!(
        stats.probe_formula_forced_stable_support,
        stats.probe_formula_support_attempts,
    );
    assert_eq!(
        stats.probe_formula_forced_stable_propose,
        stats.probe_formula_propose_attempts,
    );
    assert_eq!(
        stats.probe_formula_forced_stable_confirm,
        stats.probe_formula_confirm_attempts,
    );
    assert_eq!(stats.probe_formula_natural_stable_declines, 0);
    assert_eq!(stats.probe_formula_program_selected, 0);
    assert_eq!(stats.probe_formula_program_seeded, 0);
    assert_eq!(
        stats.probe_formula_stable_support_calls,
        stats.probe_formula_support_attempts
    );
    assert_eq!(
        stats.probe_formula_stable_propose_calls,
        stats.probe_formula_propose_attempts
    );
    assert_eq!(
        stats.probe_formula_stable_confirm_calls,
        stats.probe_formula_confirm_attempts
    );
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_probe_backend<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &Fixture,
    expected: &[Pair],
) {
    for limit in [Some(63), Some(64), Some(65), Some(100), None] {
        let label = limit.map_or("full".to_owned(), |limit| limit.to_string());
        let observations: Vec<_> = FormulaTransportProbeMode::ALL
            .into_iter()
            .map(|mode| formula_probe_observe(store, fixture, mode, limit))
            .collect();
        let [stable_filed, g_filed, g_owned] = observations.as_slice() else {
            unreachable!("the panel has exactly three modes")
        };

        assert_eq!(stable_filed.plan, g_filed.plan);
        assert_eq!(g_filed.plan, g_owned.plan);
        assert_eq!(g_filed.plan.formula_scope, FormulaScope::ProductionRegions);
        assert_eq!(g_filed.plan.program_scope, ProgramScope::Production);
        assert!(g_filed.plan.production_formula_leaves > 0);
        assert!(g_filed.plan.formula_nodes > 0);
        assert!(g_filed.plan.production_region_marks > 0);
        assert_eq!(g_filed.plan.opaque_production_program_leaves, 1);
        assert_eq!(
            g_filed.rows, g_owned.rows,
            "{backend}/{label} GF != GO raw order"
        );
        assert_eq!(
            g_filed.width, g_owned.width,
            "{backend}/{label} GF != GO width"
        );
        assert_eq!(
            formula_stage_receipt(&g_filed.stats),
            formula_stage_receipt(&g_owned.stats),
            "{backend}/{label} GF != GO stage receipt",
        );
        assert_eq!(
            formula_candidate_receipt(&g_filed.stats),
            formula_candidate_receipt(&g_owned.stats),
            "{backend}/{label} GF != GO candidate receipt",
        );
        assert_eq!(
            formula_transition_receipt(&g_filed.stats),
            formula_transition_receipt(&g_owned.stats),
            "{backend}/{label} GF != GO transition receipt",
        );

        assert_eq!(
            formula_prepared_receipt(&stable_filed.stats),
            FormulaPreparedReceipt::default(),
        );
        assert_eq!(
            formula_prepared_receipt(&g_filed.stats),
            FormulaPreparedReceipt::default(),
        );
        let prepared = formula_prepared_receipt(&g_owned.stats);
        assert_eq!(prepared.receipts, prepared.identity_checks);
        assert_eq!(
            prepared.atoms,
            prepared.committed + prepared.consumed + prepared.owned
        );
        assert_stable_transport_seam(&stable_filed.stats);
        assert_g_transport_seam(backend, &g_filed.stats);
        assert_g_transport_seam(backend, &g_owned.stats);

        let (expected_gf_filing, expected_go_filing, expected_go_prepared) =
            expected_formula_physical_receipts(backend, limit);
        // Filed G names every logical Formula successor at its transition
        // boundary. Owned G names the same receipts before arbitration and
        // counts equal-key coalescing at that boundary; its later physical
        // filings and atom settlement are frozen separately below.
        assert_eq!(formula_filing_receipt(&g_filed.stats), expected_gf_filing);
        assert_eq!(formula_filing_receipt(&g_owned.stats), expected_go_filing);
        assert_eq!(prepared, expected_go_prepared);
        assert_eq!(expected_gf_filing.filings, prepared.receipts);
        assert_eq!(expected_gf_filing.merges, prepared.equal_key_appends);

        if limit.is_none() {
            for (mode, observation) in FormulaTransportProbeMode::ALL
                .into_iter()
                .zip(&observations)
            {
                exact_check(
                    observation.rows.clone(),
                    expected,
                    mode.label(),
                    &format!("{backend} full SET"),
                );
            }
            assert_eq!(prepared.owned, 0, "full solve retained a prepared tail");
        }

        let (signature, ordered_digest) = formula_transport_order_receipt(&g_filed.rows);
        println!(
            "probe_receipt backend={backend:?} checkpoint={label} rows={} checksum={:#018x} \
             ordered_digest={:#018x} plan_equal=true gf_go_raw_equal=true \
             gf_go_stage_equal=true gf_go_candidate_equal=true gf_go_transition_equal=true \
             gf_filing={:?} go_filing={:?} go_prepared={:?}",
            signature.rows,
            signature.checksum,
            ordered_digest,
            formula_filing_receipt(&g_filed.stats),
            formula_filing_receipt(&g_owned.stats),
            prepared,
        );
    }
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_correctness_main() {
    const PROBE_BASE: &str = "d064dd23b2b1cb1c505caaad5ae64c9c9643eb70";
    const COMPONENT_COUNT: usize = 1;
    const RING_SIZE: usize = 64;
    const FANOUT: usize = 2;

    let fixture = Fixture::new(COMPONENT_COUNT, RING_SIZE, FANOUT);
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
    let expected = fixture.mixed_formula_rpq_oracle();
    assert_eq!(expected.len(), 2_048, "the original causal cell drifted");

    println!("diagnostic: Formula Support prepared-continuation probe; no timed samples");
    println!("probe base: {PROBE_BASE}");
    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    println!(
        "fixture: {COMPONENT_COUNT} component x {RING_SIZE} nodes, fanout {FANOUT}, \
         {} tribles; expected rows: {}",
        fixture.graph.len(),
        expected.len(),
    );
    println!("checkpoints: {PREFIX_CHECKPOINTS:?}");

    let trible_plan = formula_transport_probe_query!(
        &fixture.graph,
        &fixture,
        FormulaTransportProbeMode::StableFiled
    )
    .formula_delta_transport_probe_plan_receipt();
    let succinct_plan =
        formula_transport_probe_query!(&archive, &fixture, FormulaTransportProbeMode::StableFiled)
            .formula_delta_transport_probe_plan_receipt();
    assert_eq!(
        trible_plan, succinct_plan,
        "backend changed the compiled plan"
    );
    formula_transport_probe_backend("TribleSet sibling", &fixture.graph, &fixture, &expected);
    formula_transport_probe_backend("SuccinctArchive sibling", &archive, &fixture, &expected);
    formula_delta_transport_probe_select(FormulaDeltaTransportProbeSelector::Typed);
    formula_prepared_continuation_probe_select(FormulaPreparedContinuationProbeSelector::Filed);
    println!(
        "probe verdict: all 30 backend/mode/checkpoint cells preserve SET; GF and GO preserve \
         raw order plus plan/stage/candidate/transition receipts"
    );
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormulaTransportTimingBackend {
    TribleSet,
    SuccinctArchive,
}

#[cfg(formula_delta_transport_probe)]
impl FormulaTransportTimingBackend {
    const ALL: [Self; 2] = [Self::TribleSet, Self::SuccinctArchive];

    fn label(self) -> &'static str {
        match self {
            Self::TribleSet => "TribleSet sibling",
            Self::SuccinctArchive => "SuccinctArchive sibling",
        }
    }
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormulaTransportTimingPoint {
    Prefix(usize),
    Full,
}

#[cfg(formula_delta_transport_probe)]
impl FormulaTransportTimingPoint {
    const ALL: [Self; 5] = [
        Self::Prefix(63),
        Self::Prefix(64),
        Self::Prefix(65),
        Self::Prefix(100),
        Self::Full,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Prefix(63) => "63",
            Self::Prefix(64) => "64",
            Self::Prefix(65) => "65",
            Self::Prefix(100) => "100",
            Self::Full => "full",
            Self::Prefix(_) => unreachable!("the formal panel has fixed checkpoints"),
        }
    }
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FormulaTransportTimingTask {
    backend: FormulaTransportTimingBackend,
    mode: FormulaTransportProbeMode,
    point: FormulaTransportTimingPoint,
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FormulaTransportTimingReceipt {
    signature: Signature,
    ordered_digest: u64,
}

#[cfg(formula_delta_transport_probe)]
#[derive(Clone, Copy, Debug)]
struct FormulaTransportTimingSample {
    round: usize,
    position: usize,
    task: usize,
    time_to_n: Duration,
    drop_at_n: Duration,
    total: Duration,
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_timing_tasks() -> Vec<FormulaTransportTimingTask> {
    let mut tasks = Vec::with_capacity(30);
    // Interleave backends inside modes inside checkpoints. The formal order
    // then rotates this base by a stride coprime to 30 and mirrors every
    // second 30-round half, rather than privileging this construction order.
    for point in FormulaTransportTimingPoint::ALL {
        for mode in FormulaTransportProbeMode::ALL {
            for backend in FormulaTransportTimingBackend::ALL {
                tasks.push(FormulaTransportTimingTask {
                    backend,
                    mode,
                    point,
                });
            }
        }
    }
    assert_eq!(tasks.len(), 30);
    tasks
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_timing_order(round: usize, task_count: usize) -> Vec<usize> {
    assert_eq!(task_count, 30);
    const STRIDE: usize = 7;
    let start = (round % task_count) * STRIDE % task_count;
    let mut order: Vec<_> = (0..task_count)
        .map(|position| (start + position) % task_count)
        .collect();
    if (round / task_count) % 2 == 1 {
        order.reverse();
    }
    order
}

#[cfg(formula_delta_transport_probe)]
fn assert_formula_transport_timing_balance(task_count: usize) {
    let mut positions = vec![vec![0usize; task_count]; task_count];
    for round in 0..FORMULA_TRANSPORT_FORMAL_ROUNDS {
        for (position, task) in formula_transport_timing_order(round, task_count)
            .into_iter()
            .enumerate()
        {
            positions[task][position] += 1;
        }
    }
    assert!(positions
        .iter()
        .flatten()
        .all(|&observations| observations == FORMULA_TRANSPORT_FORMAL_ROUNDS / task_count));
}

#[cfg(formula_delta_transport_probe)]
const FORMULA_TRANSPORT_FORMAL_ROUNDS: usize = 120;

#[cfg(formula_delta_transport_probe)]
fn run_formula_transport_timing_query<S: TriblePattern>(
    store: &S,
    fixture: &Fixture,
    mode: FormulaTransportProbeMode,
    point: FormulaTransportTimingPoint,
) -> (FormulaTransportTimingReceipt, Duration, Duration, Duration) {
    mode.arm();
    let start = Instant::now();
    let mut query =
        mixed_formula_rpq_query!(store, fixture).solve_residual_state_lazy_with(mode.lowering());
    let mut signature = Signature {
        rows: 0,
        checksum: 0,
    };
    let mut ordered_digest = 0x6A09_E667_F3BC_C909;
    let mut observe = |row: Pair| {
        signature.rows += 1;
        signature.checksum = signature.checksum.wrapping_add(pair_checksum(&row));
        ordered_digest = mix64(
            ordered_digest
                ^ pair_checksum(&row)
                ^ (signature.rows as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
        black_box(row);
    };
    match point {
        FormulaTransportTimingPoint::Prefix(limit) => {
            for _ in 0..limit {
                observe(
                    query
                        .next()
                        .unwrap_or_else(|| panic!("timed query ended before checkpoint {limit}")),
                );
            }
        }
        FormulaTransportTimingPoint::Full => {
            for row in query.by_ref() {
                observe(row);
            }
        }
    }
    drop(observe);
    let time_to_n = start.elapsed();
    let drop_start = Instant::now();
    drop(query);
    let drop_at_n = drop_start.elapsed();
    let total = start.elapsed();
    (
        FormulaTransportTimingReceipt {
            signature,
            ordered_digest,
        },
        time_to_n,
        drop_at_n,
        total,
    )
}

#[cfg(formula_delta_transport_probe)]
fn run_formula_transport_timing_task(
    task: FormulaTransportTimingTask,
    fixture: &Fixture,
    archive: &SuccinctArchive<OrderedUniverse>,
) -> (FormulaTransportTimingReceipt, Duration, Duration, Duration) {
    match task.backend {
        FormulaTransportTimingBackend::TribleSet => {
            run_formula_transport_timing_query(&fixture.graph, fixture, task.mode, task.point)
        }
        FormulaTransportTimingBackend::SuccinctArchive => {
            run_formula_transport_timing_query(archive, fixture, task.mode, task.point)
        }
    }
}

#[cfg(formula_delta_transport_probe)]
fn percentile_duration(samples: &[Duration], quantile: f64) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

#[cfg(formula_delta_transport_probe)]
fn print_formula_transport_timing_plan(tasks: &[FormulaTransportTimingTask], rounds: usize) {
    assert_eq!(rounds, FORMULA_TRANSPORT_FORMAL_ROUNDS);
    assert_formula_transport_timing_balance(tasks.len());
    println!("timing panel: {rounds} rounds x {} tasks", tasks.len());
    println!("balance: every task occupies every ordinal exactly four times over 120 rounds");
    println!("order: cyclic stride 7 over 30 tasks; mirrored after each 30 rounds");
    println!("timed fields: fresh time-to-N, drop-at-N, and total; fixture/archive excluded");
    for (task, cell) in tasks.iter().enumerate() {
        println!(
            "timing_task task={task} backend={:?} mode={} checkpoint={}",
            cell.backend.label(),
            cell.mode.label(),
            cell.point.label(),
        );
    }
}

#[cfg(formula_delta_transport_probe)]
fn formula_transport_timing_main(rounds: usize) {
    const PROBE_BASE: &str = "d064dd23b2b1cb1c505caaad5ae64c9c9643eb70";
    const COMPONENT_COUNT: usize = 1;
    const RING_SIZE: usize = 64;
    const FANOUT: usize = 2;
    assert_eq!(rounds, FORMULA_TRANSPORT_FORMAL_ROUNDS);

    let fixture = Fixture::new(COMPONENT_COUNT, RING_SIZE, FANOUT);
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();
    let expected = fixture.mixed_formula_rpq_oracle();
    assert_eq!(expected.len(), 2_048, "the original causal cell drifted");
    let tasks = formula_transport_timing_tasks();

    println!("formal timing: Formula Support prepared-continuation panel");
    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    println!("probe base: {PROBE_BASE}");
    println!(
        "fixture: {COMPONENT_COUNT} component x {RING_SIZE} nodes, fanout {FANOUT}, \
         {} tribles; expected rows: {}",
        fixture.graph.len(),
        expected.len(),
    );
    print_formula_transport_timing_plan(&tasks, rounds);

    // All 30 correctness cells, including the frozen physical receipts, are
    // discharged and dropped before any clock is read.
    formula_transport_probe_backend("TribleSet sibling", &fixture.graph, &fixture, &expected);
    formula_transport_probe_backend("SuccinctArchive sibling", &archive, &fixture, &expected);
    println!(
        "timing preflight: all 30 D/GF/GO cells exact; GF/GO raw and control receipts identical"
    );

    // One untimed fresh pass per task both warms the exact path and freezes its
    // row/order receipt. Every formal observation must reproduce that receipt.
    let mut expected_receipts = Vec::with_capacity(tasks.len());
    let expected_receipt_capacity = expected_receipts.capacity();
    for task in tasks.iter().copied() {
        expected_receipts.push(run_formula_transport_timing_task(task, &fixture, &archive).0);
    }
    assert_eq!(expected_receipts.capacity(), expected_receipt_capacity);
    println!("timing warmup: all 30 fresh task receipts frozen");

    let mut samples = Vec::with_capacity(rounds * tasks.len());
    let sample_capacity = samples.capacity();
    for round in 0..rounds {
        for (position, task_index) in formula_transport_timing_order(round, tasks.len())
            .into_iter()
            .enumerate()
        {
            let (receipt, time_to_n, drop_at_n, total) =
                run_formula_transport_timing_task(tasks[task_index], &fixture, &archive);
            assert_eq!(
                receipt, expected_receipts[task_index],
                "formal timing changed a frozen row/order receipt for task {task_index}"
            );
            samples.push(FormulaTransportTimingSample {
                round,
                position,
                task: task_index,
                time_to_n,
                drop_at_n,
                total,
            });
        }
    }
    assert_eq!(
        samples.capacity(),
        sample_capacity,
        "formal sample storage grew"
    );
    formula_delta_transport_probe_select(FormulaDeltaTransportProbeSelector::Typed);
    formula_prepared_continuation_probe_select(FormulaPreparedContinuationProbeSelector::Filed);

    // Emit only after the measured panel so stdout cannot perturb later cells.
    for sample in &samples {
        let task = tasks[sample.task];
        let receipt = expected_receipts[sample.task];
        println!(
            "timing_raw round={} position={} task={} backend={:?} mode={} checkpoint={} \
             time_to_n_ns={} drop_at_n_ns={} total_ns={} rows={} checksum={:#018x} \
             ordered_digest={:#018x}",
            sample.round,
            sample.position,
            sample.task,
            task.backend.label(),
            task.mode.label(),
            task.point.label(),
            sample.time_to_n.as_nanos(),
            sample.drop_at_n.as_nanos(),
            sample.total.as_nanos(),
            receipt.signature.rows,
            receipt.signature.checksum,
            receipt.ordered_digest,
        );
    }
    for (task_index, task) in tasks.iter().copied().enumerate() {
        let cell: Vec<_> = samples
            .iter()
            .filter(|sample| sample.task == task_index)
            .collect();
        let time_to_n: Vec<_> = cell.iter().map(|sample| sample.time_to_n).collect();
        let drop_at_n: Vec<_> = cell.iter().map(|sample| sample.drop_at_n).collect();
        let total: Vec<_> = cell.iter().map(|sample| sample.total).collect();
        println!(
            "timing_summary task={task_index} backend={:?} mode={} checkpoint={} samples={} \
             time_to_n_p50_ns={} time_to_n_p95_ns={} drop_p50_ns={} drop_p95_ns={} \
             total_p50_ns={} total_p95_ns={}",
            task.backend.label(),
            task.mode.label(),
            task.point.label(),
            cell.len(),
            percentile_duration(&time_to_n, 0.50).as_nanos(),
            percentile_duration(&time_to_n, 0.95).as_nanos(),
            percentile_duration(&drop_at_n, 0.50).as_nanos(),
            percentile_duration(&drop_at_n, 0.95).as_nanos(),
            percentile_duration(&total, 0.50).as_nanos(),
            percentile_duration(&total, 0.95).as_nanos(),
        );
    }
    println!("timing verdict: all formal samples preserved their frozen row/order receipts");
}

#[cfg(formula_delta_transport_probe)]
fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => formula_transport_correctness_main(),
        Some("--timing-plan") => {
            assert!(
                args.next().is_none(),
                "the frozen timing plan takes no arguments"
            );
            let tasks = formula_transport_timing_tasks();
            print_formula_transport_timing_plan(&tasks, FORMULA_TRANSPORT_FORMAL_ROUNDS);
        }
        Some("--timing") => {
            assert!(
                args.next().is_none(),
                "the frozen timing panel takes no arguments"
            );
            formula_transport_timing_main(FORMULA_TRANSPORT_FORMAL_ROUNDS);
        }
        Some(other) => panic!("unknown Formula transport probe argument {other:?}"),
    }
}
