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
//! RUSTFLAGS="--cfg engine_current_hybrid" cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_full" cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual --cfg engine_prefix_checkpoints" \
//!   cargo run --release --example query_engine_generation_bench
//! RUSTFLAGS="--cfg engine_current_residual --cfg engine_counter_geometry" \
//!   cargo run --release --example query_engine_generation_bench
//! ```
//!
//! Fixture/archive construction and the independent relational oracles are
//! outside all timings. Every measured engine must exactly match the oracle
//! before its samples are reported.

#![allow(unexpected_cfgs)]
#![cfg_attr(
    any(engine_prefix_checkpoints, engine_counter_geometry),
    allow(dead_code)
)]

#[cfg(any(
    all(engine_legacy_binding, engine_current_scalar),
    all(engine_legacy_binding, engine_current_residual),
    all(engine_legacy_binding, engine_current_hybrid),
    all(engine_legacy_binding, engine_current_full),
    all(engine_current_scalar, engine_current_residual),
    all(engine_current_scalar, engine_current_hybrid),
    all(engine_current_scalar, engine_current_full),
    all(engine_current_residual, engine_current_hybrid),
    all(engine_current_residual, engine_current_full),
    all(engine_current_hybrid, engine_current_full),
))]
compile_error!("select exactly one benchmark engine");

#[cfg(all(
    engine_counter_geometry,
    not(any(engine_current_residual, engine_current_hybrid, engine_current_full))
))]
compile_error!("engine_counter_geometry requires one residual-engine selector");

#[cfg(all(engine_prefix_checkpoints, engine_counter_geometry))]
compile_error!("select at most one diagnostic mode");

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
const ENGINE: &str = "current residual / ordinary lowering";
#[cfg(engine_current_hybrid)]
const ENGINE: &str = "current residual / explicit HYBRID lowering";
#[cfg(engine_current_full)]
const ENGINE: &str = "current residual / explicit FULL lowering";
#[cfg(not(any(
    engine_legacy_binding,
    engine_current_scalar,
    engine_current_residual,
    engine_current_hybrid,
    engine_current_full
)))]
const ENGINE: &str = "ordinary Query iterator";

const REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};

type Pair = (Inline<GenId>, Inline<GenId>);

macro_rules! engine_query {
    ($query:expr) => {{
        let query = $query;
        #[cfg(engine_current_scalar)]
        {
            query.sequential()
        }
        #[cfg(engine_current_hybrid)]
        {
            query.residual_lowering(triblespace::core::query::residual::ResidualLowering::HYBRID)
        }
        #[cfg(engine_current_full)]
        {
            query.residual_lowering(triblespace::core::query::residual::ResidualLowering::FULL)
        }
        #[cfg(not(any(engine_current_scalar, engine_current_hybrid, engine_current_full)))]
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

#[cfg(not(any(engine_prefix_checkpoints, engine_counter_geometry)))]
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

#[cfg(not(any(engine_prefix_checkpoints, engine_counter_geometry)))]
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

#[cfg(engine_counter_geometry)]
fn counter_geometry_lowering() -> triblespace::core::query::residual::ResidualLowering {
    #[cfg(engine_current_residual)]
    {
        triblespace::core::query::residual::ResidualLowering::WHOLE_ROOT_PRODUCTION
    }
    #[cfg(engine_current_hybrid)]
    {
        triblespace::core::query::residual::ResidualLowering::HYBRID
    }
    #[cfg(engine_current_full)]
    {
        triblespace::core::query::residual::ResidualLowering::FULL
    }
}

#[cfg(engine_counter_geometry)]
fn counter_geometry_cell<I, F>(label: &str, expected: &[Pair], mut query: I, snapshot: F)
where
    I: Iterator<Item = Pair>,
    F: Fn(&I) -> (usize, String),
{
    let actual: Vec<_> = query.by_ref().collect();
    let signature = tally(actual.iter().copied());
    let (current_width, stats) = snapshot(&query);
    exact_check(actual, expected, label, "counter geometry");
    println!(
        "counter_geometry cell={label:?} rows={} checksum={:#018x} \
         current_width={current_width} stats={stats}",
        signature.rows, signature.checksum,
    );
}

#[cfg(engine_counter_geometry)]
fn main() {
    let component_count = parse_arg(1, 32);
    let ring_size = parse_arg(2, 64);
    let fanout = parse_arg(3, 2);
    let fixture = Fixture::new(component_count, ring_size, fanout);
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.graph).into();

    let finite_expected = fixture.finite_union_oracle();
    let nested_expected = fixture.nested_formula_oracle();
    let rpq_expected = fixture.cyclic_rpq_oracle();
    let mixed_expected = fixture.mixed_formula_rpq_oracle();
    let lowering = counter_geometry_lowering();

    println!("diagnostic: exact untimed counter geometry");
    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    println!("lowering: {lowering:?}");
    println!(
        "fixture: {component_count} components x {ring_size} nodes, fanout {fanout}, \
         {} tribles",
        fixture.graph.len(),
    );

    counter_geometry_cell(
        "finite OR-of-AND / TribleSet",
        &finite_expected,
        finite_union_query!(&fixture.graph, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "finite OR-of-AND / SuccinctArchive",
        &finite_expected,
        finite_union_query!(&archive, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "recursive AND/OR / TribleSet",
        &nested_expected,
        nested_formula_query!(&fixture.graph, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "recursive AND/OR / SuccinctArchive",
        &nested_expected,
        nested_formula_query!(&archive, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "cyclic RPQ / TribleSet",
        &rpq_expected,
        cyclic_rpq_query!(&fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "formula + cyclic RPQ / TribleSet sibling",
        &mixed_expected,
        mixed_formula_rpq_query!(&fixture.graph, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    counter_geometry_cell(
        "formula + cyclic RPQ / SuccinctArchive sibling",
        &mixed_expected,
        mixed_formula_rpq_query!(&archive, &fixture).solve_residual_state_lazy_with(lowering),
        |query| (query.current_width(), format!("{:?}", query.stats())),
    );
    println!("oracle parity: all seven counter-geometry cells exact");
}

#[cfg(engine_prefix_checkpoints)]
const PREFIX_CHECKPOINTS: [usize; 7] = [1, 10, 63, 64, 65, 100, 1_000];

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

#[cfg(all(
    engine_prefix_checkpoints,
    any(engine_current_residual, engine_current_hybrid, engine_current_full)
))]
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

#[cfg(all(
    engine_prefix_checkpoints,
    any(engine_current_residual, engine_current_hybrid, engine_current_full)
))]
fn benchmark_residual_lowering() -> triblespace::core::query::residual::ResidualLowering {
    #[cfg(engine_current_residual)]
    {
        triblespace::core::query::residual::ResidualLowering::WHOLE_ROOT_PRODUCTION
    }
    #[cfg(engine_current_hybrid)]
    {
        triblespace::core::query::residual::ResidualLowering::HYBRID
    }
    #[cfg(engine_current_full)]
    {
        triblespace::core::query::residual::ResidualLowering::FULL
    }
}

#[cfg(engine_prefix_checkpoints)]
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

    let rpq_expected = fixture.cyclic_rpq_oracle();
    let mixed_expected = fixture.mixed_formula_rpq_oracle();

    println!("diagnostic: source-identical prefix checkpoints");
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
    println!("checkpoints: {PREFIX_CHECKPOINTS:?}");

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
    println!("oracle parity: all three prefix-diagnostic cells exact");

    #[cfg(any(engine_current_residual, engine_current_hybrid, engine_current_full))]
    {
        residual_checkpoint_stats(
            "cyclic RPQ / TribleSet",
            cyclic_rpq_query!(&fixture)
                .solve_residual_state_lazy_with(benchmark_residual_lowering()),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
        residual_checkpoint_stats(
            "formula + cyclic RPQ / TribleSet sibling",
            mixed_formula_rpq_query!(&fixture.graph, &fixture)
                .solve_residual_state_lazy_with(benchmark_residual_lowering()),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
        residual_checkpoint_stats(
            "formula + cyclic RPQ / SuccinctArchive sibling",
            mixed_formula_rpq_query!(&archive, &fixture)
                .solve_residual_state_lazy_with(benchmark_residual_lowering()),
            |query| (query.current_width(), format!("{:?}", query.stats())),
        );
    }

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
