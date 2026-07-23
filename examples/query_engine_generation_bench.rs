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
//!   --cfg rpq_confirm_admission_probe" \
//!   cargo run --release --example query_engine_generation_bench
//! RPQ_FIT_CLOSED_RUNS_TIMING=FLEET_IDLE_RELEASED \
//!   target/release/examples/query_engine_generation_bench 48
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
    rpq_confirm_admission_probe,
    not(all(engine_prefix_checkpoints, engine_current_residual))
))]
compile_error!("rpq_confirm_admission_probe requires current residual prefix mode");

#[cfg(all(rpq_confirm_admission_probe, engine_counter_only))]
compile_error!("rpq_confirm_admission_probe is its own C/O crossover harness");

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

#[cfg(rpq_confirm_admission_probe)]
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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

#[cfg(not(rpq_confirm_admission_probe))]
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
#[cfg(rpq_confirm_admission_probe)]
use triblespace::core::query::regularpathconstraint::{
    rpq_confirm_admission_probe_bound_confirm_batches,
    rpq_confirm_admission_probe_fit_closed_present_child_ordered,
    rpq_confirm_admission_probe_fit_closed_runs,
    rpq_confirm_admission_probe_force_first_target_ordinary,
    rpq_confirm_admission_probe_force_ordinary,
    rpq_confirm_admission_probe_force_singleton_ordinary,
    rpq_confirm_admission_probe_forced_confirm_batches,
    rpq_confirm_admission_probe_record_receipts, rpq_confirm_admission_probe_reset_callbacks,
    rpq_confirm_admission_probe_snapshot, rpq_confirm_admission_probe_target_action,
    rpq_confirm_admission_probe_target_decisions, RpqConfirmAdmissionProbeSnapshot,
};
#[cfg(rpq_confirm_admission_probe)]
use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
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
        // Probe-local source-to-candidate predicate. Minted with
        // `trible genid` for this causal branch.
        "94E9C866F2979CFE08864577938A14BA" as confirm_candidate: inlineencodings::GenId;
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

#[cfg(rpq_confirm_admission_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeMode {
    Certified,
    Ordinary,
    Hybrid,
    ProbeOne,
    FitClosedRuns,
    FitClosedPresentChild,
}

#[cfg(rpq_confirm_admission_probe)]
const PROBE_TIMING_ORDER_COUNT: usize = 24;

#[cfg(rpq_confirm_admission_probe)]
impl ProbeMode {
    const ALL: [Self; 6] = [
        Self::Certified,
        Self::Ordinary,
        Self::Hybrid,
        Self::ProbeOne,
        Self::FitClosedRuns,
        Self::FitClosedPresentChild,
    ];
    const TIMED: [Self; 4] = [
        Self::Certified,
        Self::Hybrid,
        Self::ProbeOne,
        Self::FitClosedRuns,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Certified => "C_TYPED_CERTIFIED",
            Self::Ordinary => "O_FORCED_ORDINARY",
            Self::Hybrid => "H_SINGLETON_ORDINARY",
            Self::ProbeOne => "J_PROBE_ONE_ORDINARY",
            Self::FitClosedRuns => "K_FIT_CLOSED_RUNS",
            Self::FitClosedPresentChild => "KP_PRESENT_CHILD_ORDERED",
        }
    }

    fn lowering(self) -> ResidualLowering {
        ResidualLowering::PRODUCTION
    }

    fn arm(self) {
        rpq_confirm_admission_probe_force_ordinary(matches!(self, Self::Ordinary));
        rpq_confirm_admission_probe_force_singleton_ordinary(matches!(
            self,
            Self::Hybrid | Self::FitClosedRuns | Self::FitClosedPresentChild
        ));
        rpq_confirm_admission_probe_force_first_target_ordinary(matches!(self, Self::ProbeOne));
        rpq_confirm_admission_probe_fit_closed_runs(matches!(
            self,
            Self::FitClosedRuns | Self::FitClosedPresentChild
        ));
        rpq_confirm_admission_probe_fit_closed_present_child_ordered(matches!(
            self,
            Self::FitClosedPresentChild
        ));
    }
}

#[cfg(rpq_confirm_admission_probe)]
macro_rules! probe_mixed_query {
    ($store:expr, $fixture:expr, $mode:expr) => {{
        let mode = $mode;
        mode.arm();
        probe_rpq_confirm_query!($store, $fixture).solve_residual_state_lazy_with(mode.lowering())
    }};
}

type Pair = (Inline<GenId>, Inline<GenId>);

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

// Causal crossover fixture: the source marker wins first, then the exact
// source-local candidate predicate wins naturally over the wider bound RPQ
// estimate. The RPQ consequently executes Confirm in both modes. C admits its
// typed Production route; O forces only that already-selected route back to
// the stable ordinary verb.
#[cfg(rpq_confirm_admission_probe)]
macro_rules! probe_rpq_confirm_query {
    ($store:expr, $fixture:expr) => {
        engine_query!(find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                pattern!($store, [{ ?source @ bench_schema::kind: (&($fixture).seed) }]),
                pattern!($store, [{ ?source @ bench_schema::confirm_candidate: ?target }]),
                path!(
                    ($fixture).graph.clone(),
                    source (bench_schema::p | bench_schema::q)+ target
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

#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_COMPONENTS: usize = 8;
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_CORE_NODES: usize = 64;
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_SOURCES_PER_COMPONENT: usize = 32;
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_CANDIDATES_PER_SIDE: usize = 8;
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_CORE_FANOUT_PER_ATTRIBUTE: usize = 16;
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_WIDTHS: [usize; 4] = [2, 4, 8, 16];
#[cfg(rpq_confirm_admission_probe)]
const CROSSOVER_PARENT_COUNT: usize = CROSSOVER_COMPONENTS * CROSSOVER_SOURCES_PER_COMPONENT;
#[cfg(rpq_confirm_admission_probe)]
const HYBRID_WIDTH: usize = 4;
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_C_K4_FRAGMENTS: &[(usize, usize)] = &[(1, 4), (1, 4), (254, 1016)];
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_O_K4_FRAGMENTS: &[(usize, usize)] = &[
    (1, 4),
    (1, 4),
    (1, 4),
    (1, 4),
    (3, 12),
    (1, 4),
    (1, 4),
    (1, 4),
    (15, 60),
    (1, 4),
    (1, 4),
    (31, 124),
    (1, 4),
    (1, 4),
    (63, 252),
    (1, 4),
    (1, 4),
    (127, 508),
    (1, 4),
    (3, 12),
];
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_H_K4_FRAGMENTS: &[(usize, usize)] =
    &[(1, 4), (1, 4), (1, 4), (1, 4), (3, 12), (1, 4), (248, 992)];
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_J_K4_FRAGMENTS: &[(usize, usize)] = &[(1, 4), (1, 4), (1, 4), (252, 1008), (1, 4)];
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_J_K4_DECISIONS: &[(usize, usize, bool)] = &[
    (1, 4, true),
    (1, 4, false),
    (1, 4, false),
    (252, 1008, false),
    (1, 4, false),
];
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_K4_SET_CHECKSUM: u64 = 0x3625_4e32_a5b9_3ffd;
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_C_K4_ORDER_DIGEST: u64 = 0x593f_683d_9cf6_af36;
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_O_K4_ORDER_DIGEST: u64 = 0x2ef2_942e_a64d_dbef;
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_H_K4_ORDER_DIGEST: u64 = 0xdf50_77a1_f5bf_d635;
#[cfg(rpq_confirm_admission_probe)]
const FROZEN_J_K4_ORDER_DIGEST: u64 = 0xa879_7847_e607_79cc;

/// One fixed graph shared by every width cell.
///
/// Each of the eight disconnected components has a 64-node core whose `p`
/// ring alone is strongly connected. Every selected core source owns eight
/// reachable leaf targets. It also has eight decoy targets whose sole incoming
/// graph edge comes from the corresponding source in the next component. A
/// decoy therefore has byte-for-byte the same leaf geometry as a survivor but
/// remains unreachable from the candidate's source.
#[cfg(rpq_confirm_admission_probe)]
struct CrossoverFixture {
    graph: TribleSet,
    components: Vec<Vec<Id>>,
    sources: Vec<Id>,
    local_targets: Vec<Vec<Id>>,
    remote_targets: Vec<Vec<Id>>,
    seed: Id,
    graph_digest: u64,
}

#[cfg(rpq_confirm_admission_probe)]
struct CrossoverCell {
    width: usize,
    candidates: TribleSet,
    expected: Vec<Pair>,
}

#[cfg(rpq_confirm_admission_probe)]
fn byte_digest<'a>(chunks: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    chunks
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325, |mut hash, chunk| {
            for &byte in chunk {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            hash
        })
}

#[cfg(rpq_confirm_admission_probe)]
fn tribleset_byte_digest(set: &TribleSet) -> u64 {
    let mut tribles: Vec<_> = set.iter().map(|trible| trible.data).collect();
    tribles.sort_unstable();
    byte_digest(tribles.iter().map(|trible| trible.as_slice()))
}

#[cfg(rpq_confirm_admission_probe)]
impl CrossoverFixture {
    fn new() -> Self {
        const CORE_NAMESPACE: u64 = 0xC055_0001_0000_0001;
        const LOCAL_NAMESPACE: u64 = 0xC055_0001_0000_0002;
        const REMOTE_NAMESPACE: u64 = 0xC055_0001_0000_0003;
        const MARKER_NAMESPACE: u64 = 0xC055_0001_0000_0004;

        let seed = fixture_id(MARKER_NAMESPACE, 0);
        let components: Vec<Vec<Id>> = (0..CROSSOVER_COMPONENTS)
            .map(|component| {
                (0..CROSSOVER_CORE_NODES)
                    .map(|position| {
                        fixture_id(
                            CORE_NAMESPACE,
                            (component * CROSSOVER_CORE_NODES + position) as u64,
                        )
                    })
                    .collect()
            })
            .collect();
        let sources: Vec<Id> = components
            .iter()
            .flat_map(|component| {
                component
                    .iter()
                    .take(CROSSOVER_SOURCES_PER_COMPONENT)
                    .copied()
            })
            .collect();
        assert_eq!(sources.len(), CROSSOVER_PARENT_COUNT);

        let target_table = |namespace| {
            (0..CROSSOVER_PARENT_COUNT)
                .map(|source| {
                    (0..CROSSOVER_CANDIDATES_PER_SIDE)
                        .map(|candidate| {
                            fixture_id(
                                namespace,
                                (source * CROSSOVER_CANDIDATES_PER_SIDE + candidate) as u64,
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let local_targets = target_table(LOCAL_NAMESPACE);
        let remote_targets = target_table(REMOTE_NAMESPACE);

        let mut graph = TribleSet::new();
        for (component_index, component) in components.iter().enumerate() {
            // `p` contains offset one, so each 64-node core is strongly
            // connected. The disjoint p/q bands give every core node the same
            // 32-way bound-endpoint estimate before its leaf edges.
            for (position, source) in component.iter().enumerate() {
                for offset in 1..=CROSSOVER_CORE_FANOUT_PER_ATTRIBUTE {
                    insert_relation(
                        &mut graph,
                        source,
                        &bench_schema::p,
                        &component[(position + offset) % CROSSOVER_CORE_NODES],
                    );
                    insert_relation(
                        &mut graph,
                        source,
                        &bench_schema::q,
                        &component[(position + CROSSOVER_CORE_FANOUT_PER_ATTRIBUTE + offset)
                            % CROSSOVER_CORE_NODES],
                    );
                }
            }

            for source_position in 0..CROSSOVER_SOURCES_PER_COMPONENT {
                let source_ordinal =
                    component_index * CROSSOVER_SOURCES_PER_COMPONENT + source_position;
                let source = &component[source_position];
                let next_source =
                    &components[(component_index + 1) % CROSSOVER_COMPONENTS][source_position];
                for candidate in 0..CROSSOVER_CANDIDATES_PER_SIDE {
                    let attribute = if candidate % 2 == 0 {
                        &bench_schema::p
                    } else {
                        &bench_schema::q
                    };
                    insert_relation(
                        &mut graph,
                        source,
                        attribute,
                        &local_targets[source_ordinal][candidate],
                    );
                    // This decoy belongs to `source`'s candidate vocabulary,
                    // but its sole graph parent is the corresponding source in
                    // the next disconnected component.
                    insert_relation(
                        &mut graph,
                        next_source,
                        attribute,
                        &remote_targets[source_ordinal][candidate],
                    );
                }
            }
        }

        let all_targets: BTreeSet<_> = local_targets
            .iter()
            .chain(&remote_targets)
            .flatten()
            .map(|id| (*id).to_inline())
            .collect();
        assert_eq!(
            all_targets.len(),
            CROSSOVER_PARENT_COUNT * CROSSOVER_CANDIDATES_PER_SIDE * 2,
            "leaf targets are not globally unique"
        );
        let mut graph_inverse_degree = BTreeMap::<Inline<GenId>, usize>::new();
        for trible in graph.iter() {
            let target = *trible.v::<GenId>();
            if all_targets.contains(&target) {
                *graph_inverse_degree.entry(target).or_default() += 1;
            }
        }
        assert_eq!(graph_inverse_degree.len(), all_targets.len());
        assert!(
            graph_inverse_degree.values().all(|&degree| degree == 1),
            "a leaf target does not have exact graph inverse degree one"
        );

        let graph_digest = tribleset_byte_digest(&graph);
        Self {
            graph,
            components,
            sources,
            local_targets,
            remote_targets,
            seed,
            graph_digest,
        }
    }

    fn cell(&self, width: usize) -> CrossoverCell {
        assert!(CROSSOVER_WIDTHS.contains(&width));
        let side = width / 2;
        let mut candidates = TribleSet::new();
        let mut local_count = 0usize;
        let mut remote_count = 0usize;

        for (source_ordinal, source) in self.sources.iter().enumerate() {
            insert_relation(&mut candidates, source, &bench_schema::kind, &self.seed);
            for target in self.local_targets[source_ordinal].iter().take(side) {
                insert_relation(
                    &mut candidates,
                    source,
                    &bench_schema::confirm_candidate,
                    target,
                );
                local_count += 1;
            }
            for target in self.remote_targets[source_ordinal].iter().take(side) {
                insert_relation(
                    &mut candidates,
                    source,
                    &bench_schema::confirm_candidate,
                    target,
                );
                remote_count += 1;
            }
        }

        let candidate_attribute = bench_schema::confirm_candidate.id();
        let mut forward = BTreeMap::<Inline<GenId>, usize>::new();
        let mut inverse = BTreeMap::<Inline<GenId>, usize>::new();
        let mut candidate_facts = 0usize;
        for trible in candidates.iter() {
            if trible.a() != &candidate_attribute {
                continue;
            }
            candidate_facts += 1;
            *forward.entry(trible.e().to_inline()).or_default() += 1;
            *inverse.entry(*trible.v::<GenId>()).or_default() += 1;
        }
        assert_eq!(candidate_facts, CROSSOVER_PARENT_COUNT * width);
        assert_eq!(
            inverse.len(),
            candidate_facts,
            "candidate targets are not distinct"
        );
        assert_eq!(forward.len(), CROSSOVER_PARENT_COUNT);
        assert!(forward.values().all(|&count| count == width));
        assert!(inverse.values().all(|&count| count == 1));
        assert_eq!(local_count, CROSSOVER_PARENT_COUNT * side);
        assert_eq!(remote_count, CROSSOVER_PARENT_COUNT * side);
        assert_eq!(
            tribleset_byte_digest(&self.graph),
            self.graph_digest,
            "a width cell mutated the fixed RPQ graph"
        );
        let expected = self.independent_oracle(&candidates);
        assert_eq!(expected.len(), CROSSOVER_PARENT_COUNT * side);

        CrossoverCell {
            width,
            candidates,
            expected,
        }
    }

    /// Direct nested reference semantics for the unchanged three-clause
    /// query. This intentionally knows nothing about the local/remote target
    /// tables used to construct the fixture: it reads marker and candidate
    /// facts, computes graph reachability with a queue, and intersects the two
    /// sets per source.
    fn independent_oracle(&self, candidates: &TribleSet) -> Vec<Pair> {
        let marker_attribute = bench_schema::kind.id();
        let candidate_attribute = bench_schema::confirm_candidate.id();
        let p = bench_schema::p.id();
        let q = bench_schema::q.id();
        let seed = self.seed.to_inline();

        let mut marked = BTreeSet::new();
        let mut proposed = BTreeMap::<Inline<GenId>, Vec<Inline<GenId>>>::new();
        for trible in candidates.iter() {
            let source = trible.e().to_inline();
            if trible.a() == &marker_attribute && *trible.v::<GenId>() == seed {
                marked.insert(source);
            } else if trible.a() == &candidate_attribute {
                proposed
                    .entry(source)
                    .or_default()
                    .push(*trible.v::<GenId>());
            }
        }
        assert_eq!(marked.len(), CROSSOVER_PARENT_COUNT);

        let mut adjacency = BTreeMap::<Inline<GenId>, Vec<Inline<GenId>>>::new();
        for trible in self.graph.iter() {
            if trible.a() == &p || trible.a() == &q {
                adjacency
                    .entry(trible.e().to_inline())
                    .or_default()
                    .push(*trible.v::<GenId>());
            }
        }

        let mut expected = BTreeSet::new();
        for source in marked {
            let mut reachable = BTreeSet::new();
            let mut queue = VecDeque::from([source]);
            while let Some(node) = queue.pop_front() {
                for &target in adjacency.get(&node).into_iter().flatten() {
                    if reachable.insert(target) {
                        queue.push_back(target);
                    }
                }
            }
            for &target in proposed.get(&source).into_iter().flatten() {
                if reachable.contains(&target) {
                    expected.insert((source, target));
                }
            }
        }
        expected.into_iter().collect()
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

#[cfg(all(not(engine_prefix_checkpoints), not(rpq_confirm_admission_probe)))]
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
    not(rpq_confirm_admission_probe)
))]
const PREFIX_CHECKPOINTS: [usize; 7] = [1, 10, 63, 64, 65, 100, 1_000];

#[cfg(all(
    engine_prefix_checkpoints,
    any(engine_counter_only, rpq_confirm_admission_probe)
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

#[cfg(all(engine_prefix_checkpoints, rpq_confirm_admission_probe))]
fn main() {
    let repetitions = parse_arg(1, PROBE_TIMING_ORDER_COUNT);
    assert!(
        repetitions >= PROBE_TIMING_ORDER_COUNT && repetitions % PROBE_TIMING_ORDER_COUNT == 0,
        "use a positive multiple of 24 balanced four-mode timing repetitions"
    );
    let timing_request = std::env::var("RPQ_FIT_CLOSED_RUNS_TIMING").ok();
    let run_timing = timing_request.as_deref() == Some("FLEET_IDLE_RELEASED");
    assert!(
        timing_request.is_none() || run_timing,
        "RPQ_FIT_CLOSED_RUNS_TIMING must equal FLEET_IDLE_RELEASED; correctness-only is the default"
    );
    println!("engine: {ENGINE}");
    println!("revision: {REVISION}");
    println!("timing_repetitions: {repetitions}; timing_enabled: {run_timing}");
    run_rpq_confirm_crossover_probe(repetitions, run_timing);
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

#[cfg(all(
    engine_prefix_checkpoints,
    any(engine_counter_only, rpq_confirm_admission_probe)
))]
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

#[cfg(rpq_confirm_admission_probe)]
fn probe_order_receipt(rows: &[Pair]) -> (Signature, u64) {
    let signature = tally(rows.iter().copied());
    let ordered_digest =
        rows.iter()
            .enumerate()
            .fold(0x6A09_E667_F3BC_C909, |digest, (index, row)| {
                mix64(
                    digest
                        ^ pair_order_hash(row)
                        ^ ((index + 1) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                )
            });
    (signature, ordered_digest)
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn format_probe_stats(
    stats: &ResidualStateStats,
    callbacks: RpqConfirmAdmissionProbeSnapshot,
) -> String {
    format!(
        "offers={} admissions={} deferred={} forced_rpq_confirm_declines={} \
         program_activation_parents_opened={} activations_completed={} \
         transition_pages={} transition_cohorts={} transition_examined={} \
         source_pages={} source_cohorts={} source_examined={} \
         state_reentries={} rows_reentered={} bucket_merges={} rows_merged={} \
         formula_filings={} formula_bucket_merges={} formula_state_reentries={} \
         formula_rows_reentered={} receipt_local_fused_steps={} \
         receipt_local_refiles_avoided={} confirm_calls={} confirm_rows={} \
         candidates_confirmed={} rpq_ordinary_confirm_calls={} \
         rpq_ordinary_confirm_rows={} rpq_ordinary_candidates_in={} \
         rpq_ordinary_candidates_out={} rpq_ordinary_propose_calls={} \
         rpq_ordinary_propose_rows={} rpq_ordinary_propose_candidates={} \
         rpq_satisfied_calls={} rpq_satisfied_rows={} \
         rpq_satisfied_false_calls={} rpq_program_seed_calls={} \
         rpq_program_seed_parents={} rpq_program_seed_propose_calls={} \
         rpq_program_seed_confirm_calls={} rpq_program_seed_support_calls={} \
         rpq_route_calls={} \
         rpq_route_propose_calls={} rpq_route_confirm_calls={} \
         rpq_route_support_calls={} rpq_route_bound_confirm_calls={} \
         rpq_route_forced_calls={} rpq_biased_bound_estimate_rows={}",
        stats.probe_program_offers,
        stats.probe_program_admissions,
        stats.probe_program_deferred,
        stats.probe_forced_rpq_confirm_declines,
        stats.probe_program_activation_parents_opened,
        stats.delta_activations_completed,
        stats.delta_transition_pages,
        stats.delta_transition_cohorts,
        stats.delta_transition_candidates_examined,
        stats.delta_source_pages,
        stats.delta_source_cohorts,
        stats.delta_source_candidates_examined,
        stats.state_reentries,
        stats.rows_reentered,
        stats.bucket_merges,
        stats.rows_merged,
        stats.probe_formula_filings,
        stats.probe_formula_bucket_merges,
        stats.probe_formula_state_reentries,
        stats.probe_formula_rows_reentered,
        stats.delta_program_receipt_local_fused_steps,
        stats.delta_program_receipt_local_refiles_avoided,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.candidates_confirmed,
        callbacks.ordinary_confirm_calls,
        callbacks.ordinary_confirm_rows,
        callbacks.ordinary_confirm_candidates_in,
        callbacks.ordinary_confirm_candidates_out,
        callbacks.ordinary_propose_calls,
        callbacks.ordinary_propose_rows,
        callbacks.ordinary_propose_candidates,
        callbacks.satisfied_calls,
        callbacks.satisfied_rows,
        callbacks.satisfied_false_calls,
        callbacks.program_seed_calls,
        callbacks.program_seed_parents,
        callbacks.program_seed_propose_calls,
        callbacks.program_seed_confirm_calls,
        callbacks.program_seed_support_calls,
        callbacks.route_calls,
        callbacks.route_propose_calls,
        callbacks.route_confirm_calls,
        callbacks.route_support_calls,
        callbacks.route_bound_confirm_calls,
        callbacks.route_forced_calls,
        callbacks.biased_bound_estimate_rows,
    )
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn probe_counter_cell<S: TriblePattern>(backend: &str, store: &S, fixture: &Fixture) {
    for mode in ProbeMode::ALL {
        mode.arm();
        rpq_confirm_admission_probe_reset_callbacks();
        let label = format!("{} / {backend}", mode.label());
        let query = probe_mixed_query!(store, fixture, mode);
        residual_checkpoint_stats(&label, query, |query| {
            (
                query.current_width(),
                format_probe_stats(query.stats(), rpq_confirm_admission_probe_snapshot()),
            )
        });
    }
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn original_mixed_falsifier_cell<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &Fixture,
    expected: &[Pair],
) {
    let mut production = None;
    let mut forced = None;
    for mode in ProbeMode::ALL {
        mode.arm_original_mixed();
        rpq_confirm_admission_probe_reset_callbacks();
        let mut query = probe_original_mixed_query!(store, fixture, mode);
        let rows: Vec<Pair> = query.by_ref().collect();
        exact_check(rows, expected, mode.label(), "original mixed falsifier SET");
        let stats = query.stats().clone();
        let callbacks = rpq_confirm_admission_probe_snapshot();
        assert_eq!(
            stats.probe_forced_rpq_confirm_declines, 0,
            "original mixed fixture unexpectedly executed a forced RPQ Confirm decline"
        );
        assert_eq!(
            callbacks.ordinary_confirm_calls, 0,
            "original mixed fixture unexpectedly invoked ordinary RPQ Confirm"
        );
        match mode {
            ProbeMode::Production => production = Some((stats.clone(), callbacks)),
            ProbeMode::ProductionForcedOrdinaryConfirm => {
                forced = Some((stats.clone(), callbacks));
            }
            ProbeMode::OpaqueProduction => {}
        }
        println!(
            "probe_original_mixed_falsifier backend={backend:?} mode={} rows={} \
             current_width={} stats={}",
            mode.label(),
            expected.len(),
            query.current_width(),
            format_probe_stats(&stats, callbacks),
        );
    }
    let (production_stats, production_callbacks) = production.unwrap();
    let (forced_stats, forced_callbacks) = forced.unwrap();
    assert_eq!(
        production_stats, forced_stats,
        "original mixed C/F execution stats diverged despite zero executed bound Confirm"
    );
    assert_eq!(
        production_callbacks.program_seed_confirm_calls,
        forced_callbacks.program_seed_confirm_calls,
        "original mixed C/F RPQ Program callback count diverged"
    );
    assert!(
        forced_callbacks.route_forced_calls > 0,
        "original mixed compile scan never observed the structurally available bound Confirm route"
    );
    println!(
        "probe_original_mixed_verdict backend={backend:?} c_f_stats_equal=true \
         executed_forced_declines=0 ordinary_rpq_confirm_callbacks=0 \
         compile_only_forced_routes={}",
        forced_callbacks.route_forced_calls,
    );
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn causal_seam_assertion_cell<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &Fixture,
    expected: &[Pair],
) {
    let run = |mode: ProbeMode| {
        mode.arm();
        rpq_confirm_admission_probe_reset_callbacks();
        let mut query = probe_mixed_query!(store, fixture, mode);
        let rows: Vec<Pair> = query.by_ref().collect();
        exact_check(rows, expected, mode.label(), "causal seam assertion SET");
        (
            query.stats().clone(),
            rpq_confirm_admission_probe_snapshot(),
        )
    };
    let (production_stats, production_callbacks) = run(ProbeMode::Production);
    let (forced_stats, forced_callbacks) = run(ProbeMode::ProductionForcedOrdinaryConfirm);

    assert_eq!(production_stats.probe_forced_rpq_confirm_declines, 0);
    assert_eq!(production_callbacks.ordinary_confirm_calls, 0);
    assert!(forced_stats.probe_forced_rpq_confirm_declines > 0);
    assert!(forced_callbacks.ordinary_confirm_calls > 0);
    assert_eq!(
        forced_callbacks.ordinary_confirm_candidates_in, forced_callbacks.ordinary_confirm_rows,
        "causal fixture lost its one-candidate-per-parent geometry"
    );
    assert!(
        forced_callbacks.ordinary_confirm_candidates_in
            > forced_callbacks.ordinary_confirm_candidates_out,
        "forced ordinary RPQ Confirm did not filter the mixed local/remote candidate batch"
    );
    assert!(production_stats.delta_transition_pages > 0);
    assert_eq!(forced_stats.delta_transition_pages, 0);
    assert!(
        production_callbacks.program_seed_confirm_calls
            > forced_callbacks.program_seed_confirm_calls,
        "F did not replace any RPQ Program Confirm seeds"
    );
    assert_eq!(
        forced_callbacks.ordinary_confirm_candidates_in,
        forced_callbacks.ordinary_confirm_candidates_out * 2,
        "causal fixture did not retain exactly its reachable local half"
    );
    let formula_shape = |stats: &ResidualStateStats| {
        (
            stats.probe_formula_filings,
            stats.probe_formula_bucket_merges,
            stats.probe_formula_state_reentries,
            stats.probe_formula_rows_reentered,
        )
    };
    assert_eq!(
        formula_shape(&production_stats),
        formula_shape(&forced_stats),
        "C/F formula topology counters diverged; route isolation was not structural"
    );
    println!(
        "probe_causal_seam_verdict backend={backend:?} c_transition_pages={} \
         f_transition_pages={} f_forced_declines={} f_ordinary_callbacks={} \
         candidates_in={} candidates_out={} c_rpq_program_confirm_seeds={} \
         f_rpq_program_confirm_seeds={} formula_topology_equal=true",
        production_stats.delta_transition_pages,
        forced_stats.delta_transition_pages,
        forced_stats.probe_forced_rpq_confirm_declines,
        forced_callbacks.ordinary_confirm_calls,
        forced_callbacks.ordinary_confirm_candidates_in,
        forced_callbacks.ordinary_confirm_candidates_out,
        production_callbacks.program_seed_confirm_calls,
        forced_callbacks.program_seed_confirm_calls,
    );
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
#[derive(Clone, Copy)]
struct ProbeTimingSample {
    mode: ProbeMode,
    point: usize,
    time_to_n: Duration,
    drop_at_n: Duration,
    total: Duration,
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn probe_timing_cell<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &Fixture,
    expected_rows: usize,
    repetitions: usize,
) {
    let points = [63usize, 64, 65, 100, expected_rows];
    assert!(expected_rows > 100);
    let order_a = [
        ProbeMode::OpaqueProduction,
        ProbeMode::Production,
        ProbeMode::ProductionForcedOrdinaryConfirm,
        ProbeMode::ProductionForcedOrdinaryConfirm,
        ProbeMode::Production,
        ProbeMode::OpaqueProduction,
    ];
    let order_b = [
        ProbeMode::ProductionForcedOrdinaryConfirm,
        ProbeMode::Production,
        ProbeMode::OpaqueProduction,
        ProbeMode::OpaqueProduction,
        ProbeMode::Production,
        ProbeMode::ProductionForcedOrdinaryConfirm,
    ];

    // One untimed hot-cache visit per mode and point.
    for mode in ProbeMode::ALL {
        for &point in &points {
            let mut query = probe_mixed_query!(store, fixture, mode);
            let signature = if point == expected_rows {
                tally(query.by_ref())
            } else {
                tally(query.by_ref().take(point))
            };
            assert_eq!(signature.rows, point);
            black_box(signature);
        }
    }

    let mut samples = Vec::with_capacity(repetitions * points.len() * order_a.len());
    for repetition in 0..repetitions {
        let order = if repetition % 2 == 0 {
            order_a
        } else {
            order_b
        };
        for offset in 0..points.len() {
            let point = points[(repetition + offset) % points.len()];
            for mode in order {
                let started = Instant::now();
                let mut query = probe_mixed_query!(store, fixture, mode);
                let signature = if point == expected_rows {
                    tally(query.by_ref())
                } else {
                    tally(query.by_ref().take(point))
                };
                assert_eq!(signature.rows, point);
                black_box(signature);
                let time_to_n = started.elapsed();
                let drop_started = Instant::now();
                drop(query);
                let drop_at_n = drop_started.elapsed();
                let total = started.elapsed();
                samples.push(ProbeTimingSample {
                    mode,
                    point,
                    time_to_n,
                    drop_at_n,
                    total,
                });
            }
        }
    }

    for (sample_index, sample) in samples.iter().enumerate() {
        let point = if sample.point == expected_rows {
            "full".to_owned()
        } else {
            sample.point.to_string()
        };
        println!(
            "probe_raw backend={backend:?} sample={sample_index} mode={} point={} \
             time_to_n_ns={} drop_at_n_ns={} total_ns={}",
            sample.mode.label(),
            point,
            sample.time_to_n.as_nanos(),
            sample.drop_at_n.as_nanos(),
            sample.total.as_nanos(),
        );
    }

    for mode in ProbeMode::ALL {
        for &point in &points {
            let selected: Vec<_> = samples
                .iter()
                .filter(|sample| sample.mode == mode && sample.point == point)
                .copied()
                .collect();
            let durations = |project: fn(ProbeTimingSample) -> Duration| {
                selected
                    .iter()
                    .map(|sample| project(*sample).as_secs_f64())
                    .collect::<Vec<_>>()
            };
            let time_to_n = durations(|sample| sample.time_to_n);
            let drop_at_n = durations(|sample| sample.drop_at_n);
            let total = durations(|sample| sample.total);
            let point = if point == expected_rows {
                "full".to_owned()
            } else {
                point.to_string()
            };
            println!(
                "probe_summary backend={backend:?} mode={} point={} samples={} \
                 time_to_n_p50_us={:.3} time_to_n_p95_us={:.3} \
                 drop_at_n_p50_us={:.3} drop_at_n_p95_us={:.3} \
                 total_p50_us={:.3} total_p95_us={:.3}",
                mode.label(),
                point,
                selected.len(),
                percentile(&time_to_n, 0.50) * 1e6,
                percentile(&time_to_n, 0.95) * 1e6,
                percentile(&drop_at_n, 0.50) * 1e6,
                percentile(&drop_at_n, 0.95) * 1e6,
                percentile(&total, 0.50) * 1e6,
                percentile(&total, 0.95) * 1e6,
            );
        }
    }
}

#[cfg(all(rpq_confirm_admission_probe, any()))]
fn run_rpq_confirm_admission_probe(
    fixture: &Fixture,
    archive: &SuccinctArchive<OrderedUniverse>,
    original_mixed_expected: &[Pair],
    expected: &[Pair],
    repetitions: usize,
) {
    let confirm_archive: SuccinctArchive<OrderedUniverse> = (&fixture.confirm_graph).into();
    println!("probe: RPQ bound-endpoint Confirm route admission");
    println!(
        "invariant: B uses OpaqueLeaves; C and F both use ProductionRegions; \
         F changes only the already-fixed RPQ Confirm route exposure at execution"
    );
    println!(
        "original mixed falsifier: ordinary estimator, no selector bias; \
         this tests whether the historical cliff executed bound RPQ Confirm"
    );
    original_mixed_falsifier_cell(
        "TribleSet sibling",
        &fixture.graph,
        fixture,
        original_mixed_expected,
    );
    original_mixed_falsifier_cell(
        "SuccinctArchive sibling",
        archive,
        fixture,
        original_mixed_expected,
    );
    println!(
        "causal selector fixture: bound RPQ proposal cost is probe-biased equally in B/C/F; \
         only F changes Confirm route admission"
    );
    println!(
        "causal fixture store: {} tribles; original mixed store: {} tribles",
        fixture.confirm_graph.len(),
        fixture.graph.len(),
    );

    // Independent exact SET oracle plus raw physical-order receipts. Order is
    // reported (including a same-cell repeat), rather than asserted across
    // runs, backends, or lowerings: equal-cost state buckets may legitimately
    // use a different hash iteration order while denoting the same SET.
    for mode in ProbeMode::ALL {
        mode.arm();
        let tribleset_rows: Vec<Pair> =
            probe_mixed_query!(&fixture.confirm_graph, fixture, mode).collect();
        mode.arm();
        let tribleset_repeat: Vec<Pair> =
            probe_mixed_query!(&fixture.confirm_graph, fixture, mode).collect();
        mode.arm();
        let archive_rows: Vec<Pair> = probe_mixed_query!(&confirm_archive, fixture, mode).collect();
        mode.arm();
        let archive_repeat: Vec<Pair> =
            probe_mixed_query!(&confirm_archive, fixture, mode).collect();

        exact_check(
            tribleset_rows.clone(),
            expected,
            mode.label(),
            "TribleSet SET",
        );
        exact_check(
            archive_rows.clone(),
            expected,
            mode.label(),
            "SuccinctArchive SET",
        );
        exact_check(
            tribleset_repeat.clone(),
            expected,
            mode.label(),
            "TribleSet repeat SET",
        );
        exact_check(
            archive_repeat.clone(),
            expected,
            mode.label(),
            "SuccinctArchive repeat SET",
        );
        for (backend, rows) in [
            ("TribleSet", tribleset_rows.as_slice()),
            ("SuccinctArchive", archive_rows.as_slice()),
        ] {
            let (signature, ordered_digest) = probe_order_receipt(rows);
            println!(
                "probe_order_oracle mode={} backend={backend:?} rows={} \
                 set_checksum={:#018x} ordered_digest={:#018x} first={} last={}",
                mode.label(),
                signature.rows,
                signature.checksum,
                ordered_digest,
                render_pair(rows.first().copied()),
                render_pair(rows.last().copied()),
            );
        }
        println!(
            "probe_cross_backend_order mode={} equal={}",
            mode.label(),
            tribleset_rows == archive_rows,
        );
        for (backend, first, repeat) in [
            (
                "TribleSet",
                tribleset_rows.as_slice(),
                tribleset_repeat.as_slice(),
            ),
            (
                "SuccinctArchive",
                archive_rows.as_slice(),
                archive_repeat.as_slice(),
            ),
        ] {
            let (_, first_digest) = probe_order_receipt(first);
            let (_, repeat_digest) = probe_order_receipt(repeat);
            println!(
                "probe_order_repeat mode={} backend={backend:?} equal={} \
                 first_digest={first_digest:#018x} repeat_digest={repeat_digest:#018x}",
                mode.label(),
                first == repeat,
            );
        }
    }
    println!("probe oracle parity: exact SET pass; raw physical order receipts recorded");

    for mode in ProbeMode::ALL {
        let label = format!("{} / TribleSet sibling", mode.label());
        print_counter_evidence(
            &label,
            probe_mixed_query!(&fixture.confirm_graph, fixture, mode),
        );
        let label = format!("{} / SuccinctArchive sibling", mode.label());
        print_counter_evidence(&label, probe_mixed_query!(&confirm_archive, fixture, mode));
    }

    probe_counter_cell("TribleSet sibling", &fixture.confirm_graph, fixture);
    probe_counter_cell("SuccinctArchive sibling", &confirm_archive, fixture);
    causal_seam_assertion_cell(
        "TribleSet sibling",
        &fixture.confirm_graph,
        fixture,
        expected,
    );
    causal_seam_assertion_cell(
        "SuccinctArchive sibling",
        &confirm_archive,
        fixture,
        expected,
    );
    if std::env::var_os("RPQ_PROBE_COUNTER_ONLY").is_none() {
        probe_timing_cell(
            "TribleSet sibling",
            &fixture.confirm_graph,
            fixture,
            expected.len(),
            repetitions,
        );
        probe_timing_cell(
            "SuccinctArchive sibling",
            &confirm_archive,
            fixture,
            expected.len(),
            repetitions,
        );
    } else {
        println!("probe timing skipped: RPQ_PROBE_COUNTER_ONLY is set");
    }
    rpq_confirm_admission_probe_force_ordinary(false);
}

#[cfg(rpq_confirm_admission_probe)]
#[derive(Clone)]
struct CrossoverRun {
    rows: Vec<Pair>,
    order_digest: u64,
    stats: ResidualStateStats,
    callbacks: RpqConfirmAdmissionProbeSnapshot,
    bound_confirm_batches: Vec<(u32, usize, usize)>,
    forced_confirm_batches: Vec<(u32, usize, usize)>,
    target_decisions: Vec<(u32, usize, usize, bool)>,
}

#[cfg(rpq_confirm_admission_probe)]
fn discover_crossover_target_token(backend: &str, cell: &CrossoverCell, run: &CrossoverRun) -> u32 {
    let mut by_token = BTreeMap::<u32, Vec<(usize, usize)>>::new();
    for &(token, parents, candidates) in &run.bound_confirm_batches {
        by_token
            .entry(token)
            .or_default()
            .push((parents, candidates));
    }
    let matches: Vec<_> = by_token
        .iter()
        .filter_map(|(&token, fragments)| {
            let parents = fragments.iter().map(|fragment| fragment.0).sum::<usize>();
            let candidates = fragments.iter().map(|fragment| fragment.1).sum::<usize>();
            (parents == CROSSOVER_PARENT_COUNT
                && candidates == CROSSOVER_PARENT_COUNT * cell.width
                && fragments.iter().all(|&(parents, candidates)| {
                    parents > 0 && candidates == parents * cell.width
                }))
            .then_some(token)
        })
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "{backend}/k={}: expected exactly one request-local RPQ token with aggregate \
         parents={} candidates={} and uniform width {}, observed {by_token:?}",
        cell.width,
        CROSSOVER_PARENT_COUNT,
        CROSSOVER_PARENT_COUNT * cell.width,
        cell.width,
    );
    matches[0]
}

#[cfg(rpq_confirm_admission_probe)]
fn assert_crossover_target_receipt(
    backend: &str,
    cell: &CrossoverCell,
    token: u32,
    run: &CrossoverRun,
    expected_fragments: &[(usize, usize)],
) {
    let fragments: Vec<_> = run
        .bound_confirm_batches
        .iter()
        .filter_map(|&(observed, parents, candidates)| {
            (observed == token).then_some((parents, candidates))
        })
        .collect();
    assert_eq!(
        fragments, expected_fragments,
        "{backend}/k={}: request-local target paging changed",
        cell.width,
    );
    assert_eq!(
        fragments.iter().map(|fragment| fragment.0).sum::<usize>(),
        CROSSOVER_PARENT_COUNT,
    );
    assert_eq!(
        fragments.iter().map(|fragment| fragment.1).sum::<usize>(),
        CROSSOVER_PARENT_COUNT * cell.width,
    );
    assert_eq!(run.callbacks.target_batch_route_calls, fragments.len());
    assert_eq!(run.callbacks.target_batch_parents, CROSSOVER_PARENT_COUNT);
    assert_eq!(
        run.callbacks.target_batch_candidates,
        CROSSOVER_PARENT_COUNT * cell.width,
    );
}

#[cfg(rpq_confirm_admission_probe)]
fn crossover_target_fragments_for(run: &CrossoverRun, token: u32) -> Vec<(usize, usize)> {
    run.bound_confirm_batches
        .iter()
        .filter_map(|&(observed, parents, candidates)| {
            (observed == token).then_some((parents, candidates))
        })
        .collect()
}

#[cfg(rpq_confirm_admission_probe)]
fn crossover_formula_shape(stats: &ResidualStateStats) -> (u64, usize, usize, usize) {
    (
        stats.probe_formula_fingerprint,
        stats.probe_formula_nodes,
        stats.probe_formula_roots,
        stats.probe_residual_leaves,
    )
}

#[cfg(rpq_confirm_admission_probe)]
fn crossover_graph_work(stats: &ResidualStateStats) -> (usize, usize) {
    (
        stats.delta_transition_candidates_examined,
        stats.delta_source_candidates_examined,
    )
}

#[cfg(rpq_confirm_admission_probe)]
fn run_crossover_mode<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &CrossoverFixture,
    cell: &CrossoverCell,
    mode: ProbeMode,
    repeat: usize,
    target_token: Option<u32>,
) -> CrossoverRun {
    rpq_confirm_admission_probe_record_receipts(true);
    rpq_confirm_admission_probe_reset_callbacks();
    if let Some(token) = target_token {
        rpq_confirm_admission_probe_target_action(
            token,
            CROSSOVER_PARENT_COUNT,
            CROSSOVER_PARENT_COUNT * cell.width,
        );
    }
    mode.arm();
    let mut query = probe_mixed_query!(store, fixture, mode);
    let rows: Vec<Pair> = query.by_ref().collect();
    exact_check(rows.clone(), &cell.expected, mode.label(), backend);
    let stats = query.stats().clone();
    let callbacks = rpq_confirm_admission_probe_snapshot();
    let bound_confirm_batches = rpq_confirm_admission_probe_bound_confirm_batches();
    let forced_confirm_batches = rpq_confirm_admission_probe_forced_confirm_batches();
    let target_decisions = rpq_confirm_admission_probe_target_decisions();
    let (signature, order_digest) = probe_order_receipt(&rows);
    println!(
        "crossover_order backend={backend:?} width={} mode={} repeat={repeat} rows={} \
         set_checksum={:#018x} order_digest={:#018x} first={} last={}",
        cell.width,
        mode.label(),
        signature.rows,
        signature.checksum,
        order_digest,
        render_pair(rows.first().copied()),
        render_pair(rows.last().copied()),
    );
    println!(
        "crossover_counters backend={backend:?} width={} mode={} \
         formula_fingerprint={:#018x} formula_nodes={} formula_roots={} residual_leaves={} \
         formula_filings={} formula_merges={} offers={} admissions={} deferred={} \
         forced_declines={} activation_parents={} transition_pages={} \
         transition_cohorts={} max_transition_cohort={} transition_examined={} \
         nonterminal_calls={} active_lease_steps={} activations_completed={} \
         activation_width_increases={} width_increases={} \
         terminal_admissions={} terminal_wide_admissions={} \
         max_terminal_admission_parents={} terminal_demand_width_promotions={} \
         rpq_bulk_transition_cohorts={} rpq_pageable_transition_pages={} \
         source_examined={} candidates_confirmed={} \
         rpq_program_seed_confirm_calls={} rpq_program_seed_parents={} \
         rpq_ordinary_confirm_calls={} rpq_ordinary_confirm_rows={} \
         rpq_ordinary_candidates_in={} rpq_ordinary_candidates_out={} \
         rpq_ordinary_propose_calls={} route_bound_confirm_calls={} route_forced_calls={} \
         probe_one_consumptions={} \
         target_batch_route_calls={} bound_estimate_samples={} bound_estimate_min={} \
         bound_estimate_max={} target_batch_parents={} target_batch_candidates={} \
         bound_confirm_batches={:?} forced_confirm_batches={:?} target_decisions={:?}",
        cell.width,
        mode.label(),
        stats.probe_formula_fingerprint,
        stats.probe_formula_nodes,
        stats.probe_formula_roots,
        stats.probe_residual_leaves,
        stats.probe_formula_filings,
        stats.probe_formula_bucket_merges,
        stats.probe_program_offers,
        stats.probe_program_admissions,
        stats.probe_program_deferred,
        stats.probe_forced_rpq_confirm_declines,
        stats.probe_program_activation_parents_opened,
        stats.delta_transition_pages,
        stats.delta_transition_cohorts,
        stats.max_delta_transition_cohort,
        stats.delta_transition_candidates_examined,
        stats.delta_nonterminal_calls,
        stats.delta_active_lease_steps,
        stats.delta_activations_completed,
        stats.delta_activation_width_increases,
        stats.width_increases,
        stats.delta_terminal_admissions,
        stats.delta_terminal_demand_wide_admissions,
        stats.max_delta_terminal_admission_parents,
        stats.terminal_demand_width_promotions,
        callbacks.bulk_transition_cohorts,
        callbacks.pageable_transition_pages,
        stats.delta_source_candidates_examined,
        stats.candidates_confirmed,
        callbacks.program_seed_confirm_calls,
        callbacks.program_seed_parents,
        callbacks.ordinary_confirm_calls,
        callbacks.ordinary_confirm_rows,
        callbacks.ordinary_confirm_candidates_in,
        callbacks.ordinary_confirm_candidates_out,
        callbacks.ordinary_propose_calls,
        callbacks.route_bound_confirm_calls,
        callbacks.route_forced_calls,
        callbacks.first_target_confirm_consumptions,
        callbacks.target_batch_route_calls,
        callbacks.bound_estimate_samples,
        callbacks.bound_estimate_min,
        callbacks.bound_estimate_max,
        callbacks.target_batch_parents,
        callbacks.target_batch_candidates,
        bound_confirm_batches,
        forced_confirm_batches,
        target_decisions,
    );
    if matches!(
        mode,
        ProbeMode::FitClosedRuns | ProbeMode::FitClosedPresentChild
    ) {
        println!(
            "fit_closed_run_receipt backend={backend:?} width={} repeat={repeat} \
             original_mixed_cohorts={} bulk_runs={} bulk_inputs={} pageable_runs={} \
             pageable_inputs={} salvaged_fit_inputs={} max_run_inputs={} \
             max_bulk_run_inputs={} max_pageable_run_inputs={} \
             nonfit_resumed={} nonfit_empty_program={} nonfit_non_positive={} \
             nonfit_grant={} present_child_branch_slot_scans={} \
             present_child_lookups={} absent_child_lookups_eliminated={}",
            cell.width,
            callbacks.fit_closed_original_mixed_cohorts,
            callbacks.fit_closed_bulk_runs,
            callbacks.fit_closed_bulk_inputs,
            callbacks.fit_closed_pageable_runs,
            callbacks.fit_closed_pageable_inputs,
            callbacks.fit_closed_salvaged_fit_inputs,
            callbacks.fit_closed_max_run_inputs,
            callbacks.fit_closed_max_bulk_run_inputs,
            callbacks.fit_closed_max_pageable_run_inputs,
            callbacks.fit_closed_nonfit_resumed_inputs,
            callbacks.fit_closed_nonfit_empty_program_inputs,
            callbacks.fit_closed_nonfit_non_positive_inputs,
            callbacks.fit_closed_nonfit_grant_inputs,
            callbacks.fit_closed_present_child_branch_slot_scans,
            callbacks.fit_closed_present_child_lookups,
            callbacks.fit_closed_absent_child_lookups_eliminated,
        );
    }

    CrossoverRun {
        rows,
        order_digest,
        stats,
        callbacks,
        bound_confirm_batches,
        forced_confirm_batches,
        target_decisions,
    }
}

#[cfg(rpq_confirm_admission_probe)]
fn assert_natural_candidate_route(
    backend: &str,
    cell: &CrossoverCell,
    certified: &CrossoverRun,
    ordinary: &CrossoverRun,
) {
    let fail_selection = |mode: ProbeMode, run: &CrossoverRun, reason: &str| -> ! {
        panic!(
            "natural candidate selection failed for {backend}/k={}/{}: {reason}; \
             exact candidate estimate={} observed bound-RPQ estimate samples={} min={} max={}",
            cell.width,
            mode.label(),
            cell.width,
            run.callbacks.bound_estimate_samples,
            run.callbacks.bound_estimate_min,
            run.callbacks.bound_estimate_max,
        )
    };

    for (mode, run) in [
        (ProbeMode::Certified, certified),
        (ProbeMode::Ordinary, ordinary),
    ] {
        if run.callbacks.bound_estimate_samples == 0 {
            fail_selection(
                mode,
                run,
                "the planner never requested a bound RPQ estimate",
            );
        }
        if run.callbacks.bound_estimate_min <= cell.width {
            fail_selection(
                mode,
                run,
                "the natural RPQ quote was not wider than the candidate predicate",
            );
        }
        if run.callbacks.route_bound_confirm_calls == 0 {
            fail_selection(mode, run, "no bound-endpoint RPQ Confirm route was offered");
        }
        if run.callbacks.ordinary_propose_calls != 0 {
            fail_selection(
                mode,
                run,
                "RPQ executed Propose, so the candidate predicate did not win",
            );
        }
    }

    assert_eq!(certified.stats.probe_forced_rpq_confirm_declines, 0);
    assert_eq!(certified.callbacks.ordinary_confirm_calls, 0);
    assert!(certified.forced_confirm_batches.is_empty());
    let certified_fragments = certified.callbacks.target_batch_route_calls;
    let ordinary_fragments = ordinary.callbacks.target_batch_route_calls;
    assert_eq!(
        certified.callbacks.target_batch_route_calls,
        certified_fragments,
    );
    assert_eq!(
        ordinary.callbacks.target_batch_route_calls,
        ordinary_fragments
    );
    assert_eq!(ordinary.callbacks.route_forced_calls, ordinary_fragments);
    assert_eq!(
        ordinary.stats.probe_forced_rpq_confirm_declines,
        ordinary_fragments,
    );
    let target_token = ordinary
        .forced_confirm_batches
        .first()
        .map(|batch| batch.0)
        .expect("O did not force the request-local target token");
    assert!(
        ordinary
            .forced_confirm_batches
            .iter()
            .all(|batch| batch.0 == target_token),
        "O forced more than one request-local token"
    );
    let targeted_batches: Vec<_> = ordinary
        .bound_confirm_batches
        .iter()
        .copied()
        .filter(|batch| batch.0 == target_token)
        .collect();
    assert_eq!(
        ordinary.forced_confirm_batches, targeted_batches,
        "O left a target fragment typed or forced a non-target fragment"
    );
    assert!(certified.callbacks.program_seed_confirm_calls >= certified_fragments);
    assert!(certified.callbacks.program_seed_parents >= CROSSOVER_PARENT_COUNT);

    assert_eq!(
        ordinary.callbacks.ordinary_confirm_calls,
        ordinary_fragments
    );
    assert_eq!(
        ordinary.callbacks.ordinary_confirm_rows, CROSSOVER_PARENT_COUNT,
        "O ordinary RPQ Confirm was not isolated to the exact selected-source batch"
    );
    assert_eq!(
        ordinary.callbacks.ordinary_confirm_candidates_in,
        CROSSOVER_PARENT_COUNT * cell.width,
        "O did not receive the complete source-local candidate groups"
    );
    assert_eq!(
        ordinary.callbacks.ordinary_confirm_candidates_out,
        CROSSOVER_PARENT_COUNT * cell.width / 2,
        "O did not retain exactly the local half"
    );
    assert_eq!(
        crossover_formula_shape(&certified.stats),
        crossover_formula_shape(&ordinary.stats),
        "C/O compiled Formula topology diverged"
    );
    println!(
        "crossover_route_verdict backend={backend:?} width={} candidate_estimate={} \
         rpq_estimate_min={} rpq_estimate_max={} candidate_proposer_proven=true \
         typed_confirm=true ordinary_confirm=true exact_half_survives=true \
         formula_fingerprint_equal=true route_policy_total_effect=true \
         certified_fragments={} ordinary_fragments={} non_target_forces=0",
        cell.width,
        cell.width,
        certified.callbacks.bound_estimate_min,
        certified.callbacks.bound_estimate_max,
        certified_fragments,
        ordinary_fragments,
    );
}

#[cfg(rpq_confirm_admission_probe)]
fn assert_fragment_hybrid_route(
    backend: &str,
    cell: &CrossoverCell,
    target_token: u32,
    certified: &CrossoverRun,
    ordinary: &CrossoverRun,
    hybrid: &CrossoverRun,
) {
    let fragments = crossover_target_fragments_for(hybrid, target_token);
    let decisions: Vec<_> = hybrid
        .target_decisions
        .iter()
        .map(|&(token, parents, candidates, forced)| {
            assert_eq!(token, target_token, "H recorded a non-target decision");
            (parents, candidates, forced)
        })
        .collect();
    assert_eq!(
        decisions
            .iter()
            .map(|&(parents, candidates, _)| (parents, candidates))
            .collect::<Vec<_>>(),
        fragments,
        "H decision trace diverged from its concrete CandidateBatch trace",
    );
    assert!(
        decisions
            .iter()
            .all(|&(parents, candidates, _)| candidates == parents * cell.width),
        "H received a non-uniform target candidate group",
    );
    assert!(
        decisions
            .iter()
            .all(|&(parents, _, forced)| forced == (parents == 1)),
        "H violated its preregistered singleton-only ordinary rule",
    );

    let first_typed = decisions
        .iter()
        .position(|&(_, _, forced)| !forced)
        .expect("H FALSIFIED: no multi-parent typed tail emerged");
    assert!(
        first_typed > 0,
        "H did not expose an ordinary latency prefix"
    );
    let &(tail_parents, _, tail_forced) = decisions
        .last()
        .expect("H did not execute the request-local target action");
    assert!(
        !tail_forced && tail_parents > 1,
        "H FALSIFIED: the final fragment was not a multi-parent typed tail",
    );
    assert!(
        fragments.len() <= crossover_target_fragments_for(ordinary, target_token).len(),
        "H FALSIFIED: hybrid fragmentation exceeded the frozen all-ordinary baseline",
    );

    let forced: Vec<_> = decisions
        .iter()
        .filter_map(|&(parents, candidates, forced)| {
            forced.then_some((target_token, parents, candidates))
        })
        .collect();
    assert_eq!(hybrid.forced_confirm_batches, forced);
    let ordinary_parents = forced.iter().map(|batch| batch.1).sum::<usize>();
    let ordinary_candidates = forced.iter().map(|batch| batch.2).sum::<usize>();
    let typed_parents = CROSSOVER_PARENT_COUNT - ordinary_parents;
    let ordinary_fragments = decisions.iter().filter(|decision| decision.2).count();
    let typed_fragments = decisions.len() - ordinary_fragments;
    let late_singletons = decisions[first_typed..]
        .iter()
        .filter(|decision| decision.2)
        .count();
    let decision_switches = decisions
        .windows(2)
        .filter(|pair| pair[0].2 != pair[1].2)
        .count();
    assert!(typed_parents > 1, "H did not retain a real typed cohort");
    assert_eq!(hybrid.callbacks.ordinary_confirm_calls, forced.len());
    assert_eq!(hybrid.callbacks.ordinary_confirm_rows, ordinary_parents);
    assert_eq!(
        hybrid.callbacks.ordinary_confirm_candidates_in,
        ordinary_candidates,
    );
    assert_eq!(
        hybrid.callbacks.ordinary_confirm_candidates_out,
        ordinary_candidates / 2,
    );
    assert_eq!(hybrid.callbacks.route_forced_calls, forced.len());
    assert_eq!(hybrid.stats.probe_forced_rpq_confirm_declines, forced.len(),);

    assert_eq!(
        crossover_formula_shape(&certified.stats),
        crossover_formula_shape(&hybrid.stats),
        "H changed the frozen Production/ParentAtomic formula topology",
    );
    assert_eq!(
        crossover_formula_shape(&ordinary.stats),
        crossover_formula_shape(&hybrid.stats),
        "H formula topology diverged from O",
    );
    assert_eq!(
        hybrid.stats.candidates_confirmed,
        certified.stats.candidates_confirmed,
    );
    assert_eq!(
        hybrid.stats.candidates_confirmed,
        ordinary.stats.candidates_confirmed,
    );
    assert_eq!(
        hybrid.stats.delta_source_candidates_examined,
        certified.stats.delta_source_candidates_examined,
    );
    assert_eq!(
        hybrid.stats.delta_source_candidates_examined,
        ordinary.stats.delta_source_candidates_examined,
    );
    assert_eq!(
        certified.stats.delta_transition_candidates_examined % CROSSOVER_PARENT_COUNT,
        0,
        "frozen C transition work was not parent-affine",
    );
    let typed_work_per_parent =
        certified.stats.delta_transition_candidates_examined / CROSSOVER_PARENT_COUNT;
    assert_eq!(
        hybrid.stats.delta_transition_candidates_examined,
        typed_work_per_parent * typed_parents,
        "H typed graph work did not equal frozen C's exact per-parent work on its typed tail",
    );

    println!(
        "hybrid_fragment_verdict backend={backend:?} width={} token={} decisions={decisions:?} \
         first_typed_fragment={} ordinary_fragments={} typed_fragments={} late_singletons={} \
         decision_switches={} final_typed_tail_parents={} ordinary_parents={} \
         typed_parents={} typed_work_per_parent={} fragments_vs_ordinary={}/{} \
         formula_fingerprint_equal=true counters_affine=true non_target_forces=0",
        cell.width,
        target_token,
        first_typed,
        ordinary_fragments,
        typed_fragments,
        late_singletons,
        decision_switches,
        tail_parents,
        ordinary_parents,
        typed_parents,
        typed_work_per_parent,
        fragments.len(),
        crossover_target_fragments_for(ordinary, target_token).len(),
    );
}

#[cfg(rpq_confirm_admission_probe)]
fn assert_probe_one_route(
    backend: &str,
    cell: &CrossoverCell,
    target_token: u32,
    certified: &CrossoverRun,
    ordinary: &CrossoverRun,
    hybrid: &CrossoverRun,
    probe_one: &CrossoverRun,
) {
    let fragments = crossover_target_fragments_for(probe_one, target_token);
    let decisions: Vec<_> = probe_one
        .target_decisions
        .iter()
        .map(|&(token, parents, candidates, forced)| {
            assert_eq!(
                token, target_token,
                "J FALSIFIED: recorded a non-target decision"
            );
            (parents, candidates, forced)
        })
        .collect();
    assert_eq!(
        decisions
            .iter()
            .map(|&(parents, candidates, _)| (parents, candidates))
            .collect::<Vec<_>>(),
        fragments,
        "J FALSIFIED: decision trace diverged from its concrete CandidateBatch trace",
    );
    assert_eq!(
        decisions, FROZEN_J_K4_DECISIONS,
        "sealed J k=4 route-decision receipt changed",
    );
    assert!(
        decisions.len() > 1,
        "J FALSIFIED: first ordinary batch had no later typed remainder",
    );
    assert!(
        decisions[0].2,
        "J FALSIFIED: first concrete target batch was not ordinary",
    );
    assert!(
        decisions[1..].iter().all(|decision| !decision.2),
        "J FALSIFIED: one-shot ordinary force rearmed on a later target fragment",
    );
    assert!(
        decisions
            .iter()
            .all(|&(parents, candidates, _)| candidates == parents * cell.width),
        "J received a non-uniform target candidate group",
    );

    let forced = vec![(target_token, decisions[0].0, decisions[0].1)];
    assert_eq!(
        probe_one.forced_confirm_batches, forced,
        "J FALSIFIED: force escaped the target or did not remain one-shot",
    );
    assert_eq!(
        probe_one.callbacks.first_target_confirm_consumptions, 1,
        "J FALSIFIED: request-local one-shot flag was not consumed exactly once",
    );
    assert_eq!(probe_one.callbacks.route_forced_calls, 1);
    assert_eq!(probe_one.stats.probe_forced_rpq_confirm_declines, 1);
    assert_eq!(probe_one.callbacks.ordinary_confirm_calls, 1);
    assert_eq!(probe_one.callbacks.ordinary_confirm_rows, decisions[0].0);
    assert_eq!(
        probe_one.callbacks.ordinary_confirm_candidates_in,
        decisions[0].1,
    );
    assert_eq!(
        probe_one.callbacks.ordinary_confirm_candidates_out,
        decisions[0].1 / 2,
    );

    for (label, control) in [("C", certified), ("O", ordinary), ("H", hybrid)] {
        assert_eq!(
            crossover_formula_shape(&probe_one.stats),
            crossover_formula_shape(&control.stats),
            "J changed the frozen Production/ParentAtomic formula topology relative to {label}",
        );
        assert_eq!(
            probe_one.stats.candidates_confirmed, control.stats.candidates_confirmed,
            "J changed semantic candidate confirmation work relative to {label}",
        );
        assert_eq!(
            probe_one.stats.delta_source_candidates_examined,
            control.stats.delta_source_candidates_examined,
            "J changed source work relative to {label}",
        );
    }
    assert_eq!(
        certified.stats.delta_transition_candidates_examined % CROSSOVER_PARENT_COUNT,
        0,
        "frozen C transition work was not parent-affine",
    );
    let typed_work_per_parent =
        certified.stats.delta_transition_candidates_examined / CROSSOVER_PARENT_COUNT;
    let ordinary_parents = decisions[0].0;
    let typed_parents = CROSSOVER_PARENT_COUNT - ordinary_parents;
    assert!(typed_parents > 0, "J FALSIFIED: typed remainder was empty");
    assert_eq!(
        probe_one.stats.delta_transition_candidates_examined,
        typed_work_per_parent * typed_parents,
        "J typed graph work did not equal frozen C's exact per-parent work",
    );

    println!(
        "probe_one_fragment_verdict backend={backend:?} width={} token={} decisions={decisions:?} \
         ordinary_fragments=1 typed_fragments={} ordinary_parents={} typed_parents={} \
         typed_work_per_parent={} fragments_vs_hybrid={}/{} fragments_vs_ordinary={}/{} \
         first_only=true typed_remainder=true formula_fingerprint_equal=true \
         counters_affine=true non_target_forces=0",
        cell.width,
        target_token,
        decisions.len() - 1,
        ordinary_parents,
        typed_parents,
        typed_work_per_parent,
        fragments.len(),
        crossover_target_fragments_for(hybrid, target_token).len(),
        fragments.len(),
        crossover_target_fragments_for(ordinary, target_token).len(),
    );
}

#[cfg(rpq_confirm_admission_probe)]
fn nonplacement_callbacks(
    mut callbacks: RpqConfirmAdmissionProbeSnapshot,
) -> RpqConfirmAdmissionProbeSnapshot {
    callbacks.bulk_transition_cohorts = 0;
    callbacks.pageable_transition_pages = 0;
    callbacks.fit_closed_original_mixed_cohorts = 0;
    callbacks.fit_closed_bulk_runs = 0;
    callbacks.fit_closed_bulk_inputs = 0;
    callbacks.fit_closed_pageable_runs = 0;
    callbacks.fit_closed_pageable_inputs = 0;
    callbacks.fit_closed_salvaged_fit_inputs = 0;
    callbacks.fit_closed_max_run_inputs = 0;
    callbacks.fit_closed_max_bulk_run_inputs = 0;
    callbacks.fit_closed_max_pageable_run_inputs = 0;
    callbacks.fit_closed_nonfit_resumed_inputs = 0;
    callbacks.fit_closed_nonfit_empty_program_inputs = 0;
    callbacks.fit_closed_nonfit_non_positive_inputs = 0;
    callbacks.fit_closed_nonfit_grant_inputs = 0;
    callbacks.fit_closed_present_child_branch_slot_scans = 0;
    callbacks.fit_closed_present_child_lookups = 0;
    callbacks.fit_closed_absent_child_lookups_eliminated = 0;
    callbacks
}

#[cfg(rpq_confirm_admission_probe)]
fn assert_fit_closed_runs_equivalence(
    backend: &str,
    cell: &CrossoverCell,
    hybrid: &CrossoverRun,
    fit_closed: &CrossoverRun,
    fit_closed_repeat: &CrossoverRun,
) {
    assert_eq!(
        fit_closed.rows, hybrid.rows,
        "K FALSIFIED: raw result order diverged from H for {backend}/k={}",
        cell.width,
    );
    assert_eq!(
        fit_closed.order_digest, hybrid.order_digest,
        "K FALSIFIED: physical order digest diverged from H",
    );
    assert_eq!(
        fit_closed.stats, hybrid.stats,
        "K FALSIFIED: scheduler, Formula, or semantic work receipt diverged from H",
    );
    assert_eq!(
        fit_closed.bound_confirm_batches, hybrid.bound_confirm_batches,
        "K FALSIFIED: target fragments diverged from H",
    );
    assert_eq!(
        fit_closed.forced_confirm_batches, hybrid.forced_confirm_batches,
        "K FALSIFIED: H admission decisions changed",
    );
    assert_eq!(
        fit_closed.target_decisions, hybrid.target_decisions,
        "K FALSIFIED: route-decision trace diverged from H",
    );
    assert_eq!(
        nonplacement_callbacks(fit_closed.callbacks),
        nonplacement_callbacks(hybrid.callbacks),
        "K FALSIFIED: a non-placement callback receipt diverged from H",
    );
    assert_eq!(
        fit_closed.callbacks.bulk_transition_cohorts, hybrid.callbacks.bulk_transition_cohorts,
        "K changed the unchanged whole-cohort all-fit hit count",
    );

    assert_eq!(fit_closed_repeat.rows, fit_closed.rows);
    assert_eq!(fit_closed_repeat.order_digest, fit_closed.order_digest);
    assert_eq!(fit_closed_repeat.stats, fit_closed.stats);
    assert_eq!(fit_closed_repeat.callbacks, fit_closed.callbacks);
    assert_eq!(
        fit_closed_repeat.bound_confirm_batches,
        fit_closed.bound_confirm_batches,
    );
    assert_eq!(
        fit_closed_repeat.forced_confirm_batches,
        fit_closed.forced_confirm_batches,
    );
    assert_eq!(
        fit_closed_repeat.target_decisions,
        fit_closed.target_decisions,
    );

    let callbacks = fit_closed.callbacks;
    assert!(callbacks.fit_closed_original_mixed_cohorts > 0);
    assert!(callbacks.fit_closed_bulk_runs > 0);
    assert!(callbacks.fit_closed_pageable_runs > 0);
    assert!(callbacks.fit_closed_bulk_inputs > 0);
    assert!(callbacks.fit_closed_pageable_inputs > 0);
    assert_eq!(
        callbacks.fit_closed_salvaged_fit_inputs,
        callbacks.fit_closed_bulk_inputs,
    );
    assert!(callbacks.fit_closed_max_run_inputs > 0);
    assert_eq!(
        callbacks.fit_closed_nonfit_resumed_inputs
            + callbacks.fit_closed_nonfit_empty_program_inputs
            + callbacks.fit_closed_nonfit_non_positive_inputs
            + callbacks.fit_closed_nonfit_grant_inputs,
        callbacks.fit_closed_pageable_inputs,
    );
    assert!(
        callbacks.pageable_transition_pages < hybrid.callbacks.pageable_transition_pages,
        "K exercised mixed cohorts but salvaged no pageable transition pages",
    );
}

#[cfg(rpq_confirm_admission_probe)]
fn correctness_backend<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &CrossoverFixture,
    cell: &CrossoverCell,
) -> (
    CrossoverRun,
    CrossoverRun,
    CrossoverRun,
    CrossoverRun,
    CrossoverRun,
    u32,
) {
    let certified_discovery =
        run_crossover_mode(backend, store, fixture, cell, ProbeMode::Certified, 0, None);
    let target_token = discover_crossover_target_token(backend, cell, &certified_discovery);
    let certified = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::Certified,
        1,
        Some(target_token),
    );
    assert_eq!(
        certified_discovery.rows, certified.rows,
        "same-cell C physical order changed on repeat for {backend}/k={}",
        cell.width
    );
    let certified_fragments = crossover_target_fragments_for(&certified_discovery, target_token);
    assert_eq!(
        certified_fragments, FROZEN_C_K4_FRAGMENTS,
        "sealed C k=4 fragment receipt changed",
    );
    assert_eq!(
        certified_discovery.order_digest, FROZEN_C_K4_ORDER_DIGEST,
        "sealed C k=4 order digest changed",
    );
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &certified,
        &certified_fragments,
    );

    let ordinary = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::Ordinary,
        0,
        Some(target_token),
    );
    let ordinary_fragments = crossover_target_fragments_for(&ordinary, target_token);
    assert_eq!(
        ordinary_fragments, FROZEN_O_K4_FRAGMENTS,
        "sealed O k=4 fragment receipt changed",
    );
    assert_eq!(
        ordinary.order_digest, FROZEN_O_K4_ORDER_DIGEST,
        "sealed O k=4 order digest changed",
    );
    let ordinary_repeat = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::Ordinary,
        1,
        Some(target_token),
    );
    assert_eq!(
        ordinary.rows, ordinary_repeat.rows,
        "same-cell O physical order changed on repeat for {backend}/k={}",
        cell.width
    );
    assert_crossover_target_receipt(backend, cell, target_token, &ordinary, &ordinary_fragments);
    assert_eq!(
        ordinary_repeat.forced_confirm_batches,
        ordinary_repeat
            .bound_confirm_batches
            .iter()
            .copied()
            .filter(|batch| batch.0 == target_token)
            .collect::<Vec<_>>(),
        "repeat O force escaped or incompletely covered its request-local token"
    );
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &ordinary_repeat,
        &ordinary_fragments,
    );

    let hybrid = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::Hybrid,
        0,
        Some(target_token),
    );
    let hybrid_fragments = crossover_target_fragments_for(&hybrid, target_token);
    assert_eq!(
        hybrid_fragments, FROZEN_H_K4_FRAGMENTS,
        "sealed H k=4 fragment receipt changed",
    );
    assert_eq!(
        hybrid.order_digest, FROZEN_H_K4_ORDER_DIGEST,
        "sealed H k=4 order digest changed",
    );
    let hybrid_repeat = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::Hybrid,
        1,
        Some(target_token),
    );
    assert_eq!(
        hybrid.rows, hybrid_repeat.rows,
        "same-cell H physical order changed on repeat for {backend}/k={}",
        cell.width,
    );
    assert_eq!(
        hybrid.target_decisions, hybrid_repeat.target_decisions,
        "same-cell H route decisions changed on repeat",
    );
    assert_crossover_target_receipt(backend, cell, target_token, &hybrid, &hybrid_fragments);
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &hybrid_repeat,
        &hybrid_fragments,
    );

    let fit_closed = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::FitClosedRuns,
        0,
        Some(target_token),
    );
    let fit_closed_fragments = crossover_target_fragments_for(&fit_closed, target_token);
    assert_eq!(
        fit_closed_fragments, FROZEN_H_K4_FRAGMENTS,
        "K FALSIFIED: target fragments diverged from frozen H",
    );
    assert_eq!(
        fit_closed.order_digest, FROZEN_H_K4_ORDER_DIGEST,
        "K FALSIFIED: order digest diverged from frozen H",
    );
    let fit_closed_repeat = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::FitClosedRuns,
        1,
        Some(target_token),
    );
    assert_crossover_target_receipt(backend, cell, target_token, &fit_closed, &hybrid_fragments);
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &fit_closed_repeat,
        &hybrid_fragments,
    );
    assert_fit_closed_runs_equivalence(backend, cell, &hybrid, &fit_closed, &fit_closed_repeat);

    let fit_closed_present = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::FitClosedPresentChild,
        0,
        Some(target_token),
    );
    let fit_closed_present_repeat = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::FitClosedPresentChild,
        1,
        Some(target_token),
    );
    assert_eq!(
        crossover_target_fragments_for(&fit_closed_present, target_token),
        FROZEN_H_K4_FRAGMENTS,
        "present-child traversal changed the frozen H target fragments",
    );
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &fit_closed_present,
        &hybrid_fragments,
    );
    assert_fit_closed_runs_equivalence(
        backend,
        cell,
        &hybrid,
        &fit_closed_present,
        &fit_closed_present_repeat,
    );
    assert_eq!(
        fit_closed_present.rows, fit_closed.rows,
        "present-child traversal changed K's exact callback order",
    );
    assert_eq!(
        fit_closed_present.order_digest, fit_closed.order_digest,
        "present-child traversal changed K's ordered result digest",
    );
    assert_eq!(
        fit_closed_present.stats, fit_closed.stats,
        "present-child traversal changed the complete residual-state receipt",
    );
    assert_eq!(
        nonplacement_callbacks(fit_closed_present.callbacks),
        nonplacement_callbacks(fit_closed.callbacks),
        "present-child traversal changed a non-structural callback receipt",
    );
    assert_eq!(
        fit_closed
            .callbacks
            .fit_closed_present_child_branch_slot_scans,
        0,
        "the frozen all-byte control unexpectedly used the present-child walk",
    );
    assert!(
        fit_closed_present
            .callbacks
            .fit_closed_present_child_branch_slot_scans
            > 0
    );
    assert!(
        fit_closed_present
            .callbacks
            .fit_closed_present_child_lookups
            > 0
    );
    assert!(
        fit_closed_present
            .callbacks
            .fit_closed_absent_child_lookups_eliminated
            > 0
    );

    let probe_one = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::ProbeOne,
        0,
        Some(target_token),
    );
    let probe_one_fragments = crossover_target_fragments_for(&probe_one, target_token);
    assert_eq!(
        probe_one_fragments, FROZEN_J_K4_FRAGMENTS,
        "sealed J k=4 fragment receipt changed",
    );
    assert_eq!(
        probe_one.order_digest, FROZEN_J_K4_ORDER_DIGEST,
        "sealed J k=4 order digest changed",
    );
    let probe_one_repeat = run_crossover_mode(
        backend,
        store,
        fixture,
        cell,
        ProbeMode::ProbeOne,
        1,
        Some(target_token),
    );
    assert_eq!(
        probe_one.rows, probe_one_repeat.rows,
        "J FALSIFIED: same-cell physical order changed on repeat for {backend}/k={}",
        cell.width,
    );
    assert_eq!(
        probe_one.target_decisions, probe_one_repeat.target_decisions,
        "J FALSIFIED: route decisions changed on repeat",
    );
    assert_eq!(
        probe_one.forced_confirm_batches, probe_one_repeat.forced_confirm_batches,
        "J FALSIFIED: forced-batch receipt changed on repeat",
    );
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &probe_one,
        &probe_one_fragments,
    );
    assert_crossover_target_receipt(
        backend,
        cell,
        target_token,
        &probe_one_repeat,
        &probe_one_fragments,
    );
    for (label, control) in [
        ("C", &certified),
        ("O", &ordinary),
        ("H", &hybrid),
        ("K", &fit_closed),
        ("KP", &fit_closed_present),
    ] {
        assert_eq!(
            control.callbacks.first_target_confirm_consumptions, 0,
            "{label} accidentally consumed J's request-local one-shot flag",
        );
    }
    for (label, run) in [
        ("C", &certified),
        ("O", &ordinary),
        ("H", &hybrid),
        ("J", &probe_one),
        ("K", &fit_closed),
        ("KP", &fit_closed_present),
    ] {
        let signature = tally(run.rows.iter().copied());
        assert_eq!(signature.rows, cell.expected.len());
        assert_eq!(
            signature.checksum, FROZEN_K4_SET_CHECKSUM,
            "sealed {label} k=4 SET checksum changed",
        );
    }
    println!(
        "crossover_fragment_receipt backend={backend:?} width={} token={} \
         certified={certified_fragments:?} ordinary={ordinary_fragments:?} \
         hybrid={hybrid_fragments:?} probe_one={probe_one_fragments:?} \
         fit_closed={fit_closed_fragments:?} repeat_stable=true",
        cell.width, target_token,
    );
    assert_natural_candidate_route(backend, cell, &certified, &ordinary);
    assert_fragment_hybrid_route(backend, cell, target_token, &certified, &ordinary, &hybrid);
    assert_probe_one_route(
        backend,
        cell,
        target_token,
        &certified,
        &ordinary,
        &hybrid,
        &probe_one,
    );
    println!(
        "crossover_cross_route_order backend={backend:?} width={} equal={}",
        cell.width,
        certified.rows == ordinary.rows,
    );
    println!(
        "hybrid_order_equivalence backend={backend:?} width={} h_equals_c={} h_equals_o={} \
         h_digest={:#018x} c_digest={:#018x} o_digest={:#018x} repeat_stable=true",
        cell.width,
        hybrid.rows == certified.rows,
        hybrid.rows == ordinary.rows,
        hybrid.order_digest,
        certified.order_digest,
        ordinary.order_digest,
    );
    println!(
        "probe_one_order_equivalence backend={backend:?} width={} j_equals_c={} j_equals_o={} \
         j_equals_h={} j_digest={:#018x} c_digest={:#018x} o_digest={:#018x} \
         h_digest={:#018x} repeat_stable=true",
        cell.width,
        probe_one.rows == certified.rows,
        probe_one.rows == ordinary.rows,
        probe_one.rows == hybrid.rows,
        probe_one.order_digest,
        certified.order_digest,
        ordinary.order_digest,
        hybrid.order_digest,
    );
    println!(
        "fit_closed_order_equivalence backend={backend:?} width={} k_equals_h={} \
         k_digest={:#018x} h_digest={:#018x} stats_equal={} callbacks_nonplacement_equal={} \
         repeat_stable=true",
        cell.width,
        fit_closed.rows == hybrid.rows,
        fit_closed.order_digest,
        hybrid.order_digest,
        fit_closed.stats == hybrid.stats,
        nonplacement_callbacks(fit_closed.callbacks) == nonplacement_callbacks(hybrid.callbacks),
    );
    (
        certified,
        ordinary,
        hybrid,
        probe_one,
        fit_closed,
        target_token,
    )
}

#[cfg(rpq_confirm_admission_probe)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrossoverTimingPoint {
    First,
    Full,
}

#[cfg(rpq_confirm_admission_probe)]
impl CrossoverTimingPoint {
    const ALL: [Self; 2] = [Self::First, Self::Full];

    fn label(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Full => "full",
        }
    }
}

#[cfg(rpq_confirm_admission_probe)]
#[derive(Clone, Copy)]
struct CrossoverTimingSample {
    mode: ProbeMode,
    point: CrossoverTimingPoint,
    duration: Duration,
}

#[cfg(rpq_confirm_admission_probe)]
fn balanced_probe_orders() -> Vec<[ProbeMode; 4]> {
    let mut orders = Vec::with_capacity(PROBE_TIMING_ORDER_COUNT);
    for first in ProbeMode::TIMED {
        for second in ProbeMode::TIMED {
            if second == first {
                continue;
            }
            for third in ProbeMode::TIMED {
                if third == first || third == second {
                    continue;
                }
                for fourth in ProbeMode::TIMED {
                    if fourth == first || fourth == second || fourth == third {
                        continue;
                    }
                    orders.push([first, second, third, fourth]);
                }
            }
        }
    }
    assert_eq!(orders.len(), PROBE_TIMING_ORDER_COUNT);
    for mode in ProbeMode::TIMED {
        for position in 0..ProbeMode::TIMED.len() {
            assert_eq!(
                orders
                    .iter()
                    .filter(|order| order[position] == mode)
                    .count(),
                PROBE_TIMING_ORDER_COUNT / ProbeMode::TIMED.len(),
                "four-mode timing orders were not position-balanced",
            );
        }
    }
    orders
}

#[cfg(rpq_confirm_admission_probe)]
fn timing_backend<S: TriblePattern>(
    backend: &str,
    store: &S,
    fixture: &CrossoverFixture,
    cell: &CrossoverCell,
    target_token: u32,
    repetitions: usize,
) {
    // Correctness has already frozen the request-token and fragment receipts.
    // Keep the causal token switch armed, but remove diagnostic Vec growth
    // from the measured route-policy total effect.
    rpq_confirm_admission_probe_record_receipts(false);
    let expected_signature = tally(cell.expected.iter().copied());
    let balanced_orders = balanced_probe_orders();

    for mode in ProbeMode::TIMED {
        for point in CrossoverTimingPoint::ALL {
            rpq_confirm_admission_probe_target_action(
                target_token,
                CROSSOVER_PARENT_COUNT,
                CROSSOVER_PARENT_COUNT * cell.width,
            );
            mode.arm();
            let mut query = probe_mixed_query!(store, fixture, mode);
            match point {
                CrossoverTimingPoint::First => {
                    assert!(black_box(query.next()).is_some());
                }
                CrossoverTimingPoint::Full => {
                    assert_eq!(black_box(tally(query.by_ref())), expected_signature);
                }
            }
        }
    }

    let mut samples = Vec::with_capacity(repetitions * 8);
    for repetition in 0..repetitions {
        let order = balanced_orders[repetition % balanced_orders.len()];
        let points = if repetition % 2 == 0 {
            CrossoverTimingPoint::ALL
        } else {
            [CrossoverTimingPoint::Full, CrossoverTimingPoint::First]
        };
        for point in points {
            for mode in order {
                rpq_confirm_admission_probe_target_action(
                    target_token,
                    CROSSOVER_PARENT_COUNT,
                    CROSSOVER_PARENT_COUNT * cell.width,
                );
                mode.arm();
                let started = Instant::now();
                let mut query = probe_mixed_query!(store, fixture, mode);
                match point {
                    CrossoverTimingPoint::First => {
                        assert!(black_box(query.next()).is_some());
                    }
                    CrossoverTimingPoint::Full => {
                        assert_eq!(black_box(tally(query.by_ref())), expected_signature);
                    }
                }
                let duration = started.elapsed();
                drop(query);
                samples.push(CrossoverTimingSample {
                    mode,
                    point,
                    duration,
                });
            }
        }
    }
    assert_eq!(samples.len(), repetitions * ProbeMode::TIMED.len() * 2);

    for (sample, value) in samples.iter().enumerate() {
        println!(
            "fit_closed_runs_timing_raw backend={backend:?} width={} sample={sample} mode={} point={} ns={}",
            cell.width,
            value.mode.label(),
            value.point.label(),
            value.duration.as_nanos(),
        );
    }
    for point in CrossoverTimingPoint::ALL {
        for mode in ProbeMode::TIMED {
            let durations: Vec<_> = samples
                .iter()
                .filter(|sample| sample.mode == mode && sample.point == point)
                .map(|sample| sample.duration.as_secs_f64())
                .collect();
            println!(
                "fit_closed_runs_timing_summary backend={backend:?} width={} mode={} point={} primary={} \
                 samples={} p50_us={:.3} p95_us={:.3}",
                cell.width,
                mode.label(),
                point.label(),
                point == CrossoverTimingPoint::Full,
                durations.len(),
                percentile(&durations, 0.50) * 1e6,
                percentile(&durations, 0.95) * 1e6,
            );
        }
    }
    rpq_confirm_admission_probe_record_receipts(true);
}

#[cfg(rpq_confirm_admission_probe)]
fn run_rpq_confirm_crossover_probe(repetitions: usize, run_timing: bool) {
    let built = Instant::now();
    let fixture = CrossoverFixture::new();
    let fixture_elapsed = built.elapsed();
    assert_eq!(fixture.components.len(), CROSSOVER_COMPONENTS);
    assert!(fixture
        .components
        .iter()
        .all(|component| component.len() == CROSSOVER_CORE_NODES));
    println!("probe: RPQ H admission plus ordered bounded fit-closed transition runs");
    println!(
        "fixture: components={} core_nodes={} selected_parents={} graph_tribles={} \
         graph_digest={:#018x} built_ms={:.3}",
        CROSSOVER_COMPONENTS,
        CROSSOVER_CORE_NODES,
        CROSSOVER_PARENT_COUNT,
        fixture.graph.len(),
        fixture.graph_digest,
        fixture_elapsed.as_secs_f64() * 1e3,
    );
    println!(
        "invariant: C, O, H, J, and K all compile the same Production route and ParentAtomic \
         grouping. K retains H admission and scheduler cohorts exactly, changing only mixed \
         transition inputs from pageable traversal to receipt-equivalent ordered bounded runs."
    );

    let cell = fixture.cell(HYBRID_WIDTH);
    println!(
        "probe_one_geometry backend=\"TribleSet\" width={} candidate_facts={} \
         distinct_targets={} forward_groups={} forward_group_width={} \
         inverse_group_width=1 local={} remote={} expected_survivors={} \
         candidate_store_tribles={} graph_digest={:#018x}",
        HYBRID_WIDTH,
        CROSSOVER_PARENT_COUNT * HYBRID_WIDTH,
        CROSSOVER_PARENT_COUNT * HYBRID_WIDTH,
        CROSSOVER_PARENT_COUNT,
        HYBRID_WIDTH,
        CROSSOVER_PARENT_COUNT * HYBRID_WIDTH / 2,
        CROSSOVER_PARENT_COUNT * HYBRID_WIDTH / 2,
        cell.expected.len(),
        cell.candidates.len(),
        fixture.graph_digest,
    );

    let (certified, ordinary, hybrid, probe_one, fit_closed, target_token) =
        correctness_backend("TribleSet", &cell.candidates, &fixture, &cell);
    println!(
        "probe_one_counter_equivalence backend=\"TribleSet\" width={} \
         candidates_confirmed_c={} candidates_confirmed_o={} candidates_confirmed_h={} \
         candidates_confirmed_j={} candidates_confirmed_k={} source_examined_c={} \
         source_examined_o={} \
         source_examined_h={} source_examined_j={} source_examined_k={} transition_examined_c={} \
         transition_examined_o={} transition_examined_h={} transition_examined_j={} \
         transition_examined_k={}",
        cell.width,
        certified.stats.candidates_confirmed,
        ordinary.stats.candidates_confirmed,
        hybrid.stats.candidates_confirmed,
        probe_one.stats.candidates_confirmed,
        fit_closed.stats.candidates_confirmed,
        certified.stats.delta_source_candidates_examined,
        ordinary.stats.delta_source_candidates_examined,
        hybrid.stats.delta_source_candidates_examined,
        probe_one.stats.delta_source_candidates_examined,
        fit_closed.stats.delta_source_candidates_examined,
        certified.stats.delta_transition_candidates_examined,
        ordinary.stats.delta_transition_candidates_examined,
        hybrid.stats.delta_transition_candidates_examined,
        probe_one.stats.delta_transition_candidates_examined,
        fit_closed.stats.delta_transition_candidates_examined,
    );
    for (mode, run) in [
        (ProbeMode::Certified, &certified),
        (ProbeMode::Ordinary, &ordinary),
        (ProbeMode::Hybrid, &hybrid),
        (ProbeMode::ProbeOne, &probe_one),
        (ProbeMode::FitClosedRuns, &fit_closed),
    ] {
        println!(
            "probe_one_physical_receipt backend=\"TribleSet\" width={} mode={} \
             transition_pages={} transition_cohorts={} max_transition_cohort={} \
             nonterminal_calls={} active_lease_steps={} activations_completed={} \
             activation_width_increases={} width_increases={} terminal_admissions={} \
             terminal_wide_admissions={} max_terminal_admission_parents={} \
             terminal_demand_width_promotions={} rpq_bulk_transition_cohorts={} \
             rpq_pageable_transition_pages={}",
            cell.width,
            mode.label(),
            run.stats.delta_transition_pages,
            run.stats.delta_transition_cohorts,
            run.stats.max_delta_transition_cohort,
            run.stats.delta_nonterminal_calls,
            run.stats.delta_active_lease_steps,
            run.stats.delta_activations_completed,
            run.stats.delta_activation_width_increases,
            run.stats.width_increases,
            run.stats.delta_terminal_admissions,
            run.stats.delta_terminal_demand_wide_admissions,
            run.stats.max_delta_terminal_admission_parents,
            run.stats.terminal_demand_width_promotions,
            run.callbacks.bulk_transition_cohorts,
            run.callbacks.pageable_transition_pages,
        );
    }

    if run_timing {
        timing_backend(
            "TribleSet",
            &cell.candidates,
            &fixture,
            &cell,
            target_token,
            repetitions,
        );
    }
    if !run_timing {
        println!(
            "timing withheld: set RPQ_FIT_CLOSED_RUNS_TIMING=FLEET_IDLE_RELEASED only \
             after an explicit fleet-idle release"
        );
    }
    rpq_confirm_admission_probe_force_ordinary(false);
    rpq_confirm_admission_probe_force_singleton_ordinary(false);
    rpq_confirm_admission_probe_force_first_target_ordinary(false);
    rpq_confirm_admission_probe_fit_closed_runs(false);
}

#[cfg(all(engine_prefix_checkpoints, not(rpq_confirm_admission_probe)))]
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

    #[cfg(not(rpq_confirm_admission_probe))]
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

    #[cfg(rpq_confirm_admission_probe)]
    {
        let probe_expected = fixture.rpq_confirm_admission_oracle();
        run_rpq_confirm_admission_probe(
            &fixture,
            &archive,
            &mixed_expected,
            &probe_expected,
            repetitions,
        );
    }

    #[cfg(not(rpq_confirm_admission_probe))]
    {
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
}
