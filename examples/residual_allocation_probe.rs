//! Allocation and latency probe for the canonical residual scheduler.
//!
//! The counting allocator is deliberately confined to this example. Counts
//! include query construction plus the requested iterator prefix/drain, but
//! exclude fixture construction and result-bag materialization.
//!
//! Usage:
//!     cargo run --release --example residual_allocation_probe -- [reps=9]
//!
//! On an Apple M4, exact base `589bc0eb`, changing only residual `ChildSet`
//! from `Vec<u64>` to `SmallVec<[u64; 2]>` produced the following deterministic
//! allocator-call counts (query construction and execution included; fixture
//! construction and result collection excluded):
//!
//! | workload | DAG | residual `Vec` | residual inline | reduction |
//! | --- | ---: | ---: | ---: | ---: |
//! | one-leaf TribleSet, first | 11+10r | 40+10r | 30+10r | 10 allocs |
//! | 120-route TribleSet, first | 73+15r | 512+35r | 278+35r | 234 allocs |
//! | 120-route TribleSet, full | 574+118r | 4379+772r | 2320+720r | 2059 allocs |
//! | 120-route archive, first | 139+15r | 578+35r | 344+35r | 234 allocs |
//! | 120-route archive, full | 6181+116r | 9206+643r | 7734+643r | 1472 allocs |
//!
//! Here `r` denotes reallocations. Requested-byte traffic stayed essentially
//! flat: inline child sets remove allocator latency and fragmentation rather
//! than the storage frontiers that dominate byte volume.
//!
//! A subsequent bounded `ResidualScratch` probe pooled Ready/Candidate planning
//! vectors. It reduced 120-route TribleSet first-result traffic from `278+35r`
//! to `133+31r`, and full-drain traffic from `2320+720r` to `800+515r`.
//! However, nine alternating checkpoint/prototype process pairs measured only
//! a 3.6% direct median TTFR gain (normalized residual/DAG median: 2.7%), below
//! the probe's 5% promotion threshold and still around 1.11x the DAG. The
//! SuccinctArchive and one-leaf action/group profiles were byte-for-byte equal;
//! TribleSet profiles vary between identical-process reruns because PATCH uses
//! a per-process random SipHash key. The scratch implementation is therefore a
//! useful negative result, not a promotion recommendation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::query::residual::ResidualStateStats;
use triblespace::core::query::TriblePattern;
use triblespace::core::trible::TribleSet;
use triblespace::prelude::*;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static REQUESTED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: forwarding the unchanged layout to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: forwarding the unchanged layout to the system allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this forwarding allocator with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            REALLOCS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: forwarding the original allocation and requested new size.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AllocationTraffic {
    allocs: u64,
    reallocs: u64,
    requested_bytes: u64,
}

fn counted<T>(f: impl FnOnce() -> T) -> (T, AllocationTraffic) {
    ALLOCS.store(0, Ordering::Relaxed);
    REALLOCS.store(0, Ordering::Relaxed);
    REQUESTED_BYTES.store(0, Ordering::Relaxed);
    assert!(!COUNTING.swap(true, Ordering::SeqCst));
    let value = f();
    assert!(COUNTING.swap(false, Ordering::SeqCst));
    (
        value,
        AllocationTraffic {
            allocs: ALLOCS.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
            requested_bytes: REQUESTED_BYTES.load(Ordering::Relaxed),
        },
    )
}

mod world {
    use triblespace::prelude::*;

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

fn deterministic_id(namespace: u32, counter: u64) -> ExclusiveId {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&counter.to_be_bytes());
    ExclusiveId::force(Id::new(raw).expect("nonzero namespace is a valid deterministic ID"))
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
                    permutations.push([a, b, c, d, 10 - a - b - c - d]);
                }
            }
        }
    }
    assert_eq!(permutations.len(), 120);
    permutations
}

type RouteMarkers = (Id, Id, Id, Id, Id, Id);

fn build_one_leaf(values: usize) -> (TribleSet, ExclusiveId) {
    let root = deterministic_id(0xD46A_2001, 1);
    let mut kb = TribleSet::new();
    for index in 0..values {
        let value = deterministic_id(0xD46A_2002, index as u64 + 1);
        kb += entity! { &root @ world::p1: &value };
    }
    (kb, root)
}

fn build_routes(n_per_route: usize, z_fan: usize) -> (TribleSet, RouteMarkers) {
    let mut kb = TribleSet::new();
    let markers: Vec<_> = (0..5)
        .map(|index| deterministic_id(0xD46A_2101, index + 1))
        .collect();
    let z_marker = deterministic_id(0xD46A_2102, 1);
    let fans = [1usize, 2, 3, 4, 5];
    let mut counter = 1u64;

    for permutation in permutations_5() {
        for _ in 0..n_per_route {
            let entity = deterministic_id(0xD46A_2103, counter);
            counter += 1;
            for (rank, &attribute_index) in permutation.iter().enumerate() {
                let values: Vec<_> = (0..fans[rank])
                    .map(|_| {
                        let value = deterministic_id(0xD46A_2104, counter);
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
                    let value = deterministic_id(0xD46A_2105, counter);
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
    )
}

#[derive(Clone, Copy)]
enum Engine {
    Dag,
    Residual,
}

fn fold_digest<T: Hash>(iter: impl Iterator<Item = T>) -> (usize, u64) {
    iter.fold((0usize, 0u64), |(count, digest), item| {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        (count + 1, digest.wrapping_add(hasher.finish()))
    })
}

fn one_leaf_first<S: TriblePattern>(kb: &S, root: &ExclusiveId, engine: Engine) -> bool {
    let query = find!(
        x: Inline<inlineencodings::GenId>,
        pattern!(kb, [{ root @ world::p1: ?x }])
    );
    match engine {
        Engine::Dag => query.solve_dag_lazy().next().is_some(),
        Engine::Residual => query.solve_residual_state_lazy().next().is_some(),
    }
}

fn one_leaf_full<S: TriblePattern>(kb: &S, root: &ExclusiveId, engine: Engine) -> (usize, u64) {
    let query = find!(
        x: Inline<inlineencodings::GenId>,
        pattern!(kb, [{ root @ world::p1: ?x }])
    );
    match engine {
        Engine::Dag => fold_digest(query.solve_dag_lazy()),
        Engine::Residual => fold_digest(query.solve_residual_state_lazy()),
    }
}

fn one_leaf_stats<S: TriblePattern>(kb: &S, root: &ExclusiveId) -> ResidualStateStats {
    find!(
        x: Inline<inlineencodings::GenId>,
        pattern!(kb, [{ root @ world::p1: ?x }])
    )
    .solve_residual_state_lazy()
    .collect_profiled()
    .stats
}

fn route_first<S: TriblePattern>(kb: &S, markers: RouteMarkers, engine: Engine) -> bool {
    let (k1, k2, k3, k4, k5, kz) = markers;
    let query = find!(
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
    );
    match engine {
        Engine::Dag => query.solve_dag_lazy().next().is_some(),
        Engine::Residual => query.solve_residual_state_lazy().next().is_some(),
    }
}

fn route_full<S: TriblePattern>(kb: &S, markers: RouteMarkers, engine: Engine) -> (usize, u64) {
    let (k1, k2, k3, k4, k5, kz) = markers;
    let query = find!(
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
    );
    match engine {
        Engine::Dag => fold_digest(query.solve_dag_lazy()),
        Engine::Residual => fold_digest(query.solve_residual_state_lazy()),
    }
}

fn route_stats<S: TriblePattern>(kb: &S, markers: RouteMarkers) -> ResidualStateStats {
    let (k1, k2, k3, k4, k5, kz) = markers;
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
    .solve_residual_state_lazy()
    .collect_profiled()
    .stats
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn measure_case(
    label: &str,
    reps: usize,
    mut dag_first: impl FnMut() -> bool,
    mut residual_first: impl FnMut() -> bool,
    mut dag_full: impl FnMut() -> (usize, u64),
    mut residual_full: impl FnMut() -> (usize, u64),
) {
    assert_eq!(
        dag_first(),
        residual_first(),
        "{label}: first-result parity"
    );
    assert_eq!(dag_full(), residual_full(), "{label}: full digest parity");

    let (_, dag_first_alloc) = counted(&mut dag_first);
    let (_, residual_first_alloc) = counted(&mut residual_first);
    let (dag_digest, dag_full_alloc) = counted(&mut dag_full);
    let (residual_digest, residual_full_alloc) = counted(&mut residual_full);
    assert_eq!(dag_digest, residual_digest, "{label}: counted full parity");

    let mut first_times: [Vec<Duration>; 2] = std::array::from_fn(|_| Vec::with_capacity(reps));
    let mut full_times: [Vec<Duration>; 2] = std::array::from_fn(|_| Vec::with_capacity(reps));
    for repetition in 0..reps {
        for offset in 0..2 {
            let engine = (repetition + offset) % 2;
            let started = Instant::now();
            let present = if engine == 0 {
                dag_first()
            } else {
                residual_first()
            };
            first_times[engine].push(started.elapsed());
            black_box(present);
        }
        for offset in 0..2 {
            let engine = (repetition + offset) % 2;
            let started = Instant::now();
            let digest = if engine == 0 {
                dag_full()
            } else {
                residual_full()
            };
            full_times[engine].push(started.elapsed());
            black_box(digest);
        }
    }
    let first = first_times.each_mut().map(|samples| median(samples));
    let full = full_times.each_mut().map(|samples| median(samples));

    println!("\n== {label} ==");
    println!("  first dag      {dag_first_alloc:?}");
    println!("  first residual {residual_first_alloc:?}");
    println!("  full  dag      {dag_full_alloc:?}");
    println!("  full  residual {residual_full_alloc:?}");
    println!(
        "  first median dag/residual {:.2}/{:.2} us = {:.3}x",
        first[0].as_secs_f64() * 1e6,
        first[1].as_secs_f64() * 1e6,
        first[1].as_secs_f64() / first[0].as_secs_f64(),
    );
    println!(
        "  full  median dag/residual {:.2}/{:.2} us = {:.3}x",
        full[0].as_secs_f64() * 1e6,
        full[1].as_secs_f64() * 1e6,
        full[1].as_secs_f64() / full[0].as_secs_f64(),
    );
}

fn main() {
    let reps = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(9);
    assert!(reps >= 9, "use at least nine interleaved repetitions");

    let (one, one_root) = build_one_leaf(4096);
    let one_archive: SuccinctArchive<OrderedUniverse> = (&one).into();
    let (routes, markers) = build_routes(4, 16);
    let routes_archive: SuccinctArchive<OrderedUniverse> = (&routes).into();

    measure_case(
        "one leaf / TribleSet",
        reps,
        || one_leaf_first(&one, &one_root, Engine::Dag),
        || one_leaf_first(&one, &one_root, Engine::Residual),
        || one_leaf_full(&one, &one_root, Engine::Dag),
        || one_leaf_full(&one, &one_root, Engine::Residual),
    );
    measure_case(
        "one leaf / SuccinctArchive",
        reps,
        || one_leaf_first(&one_archive, &one_root, Engine::Dag),
        || one_leaf_first(&one_archive, &one_root, Engine::Residual),
        || one_leaf_full(&one_archive, &one_root, Engine::Dag),
        || one_leaf_full(&one_archive, &one_root, Engine::Residual),
    );
    measure_case(
        "120 routes / TribleSet",
        reps,
        || route_first(&routes, markers, Engine::Dag),
        || route_first(&routes, markers, Engine::Residual),
        || route_full(&routes, markers, Engine::Dag),
        || route_full(&routes, markers, Engine::Residual),
    );
    measure_case(
        "120 routes / SuccinctArchive",
        reps,
        || route_first(&routes_archive, markers, Engine::Dag),
        || route_first(&routes_archive, markers, Engine::Residual),
        || route_full(&routes_archive, markers, Engine::Dag),
        || route_full(&routes_archive, markers, Engine::Residual),
    );
    println!(
        "\nprofile one-leaf/TribleSet: {:?}",
        one_leaf_stats(&one, &one_root)
    );
    println!(
        "profile one-leaf/SuccinctArchive: {:?}",
        one_leaf_stats(&one_archive, &one_root)
    );
    println!(
        "profile routes/TribleSet: {:?}",
        route_stats(&routes, markers)
    );
    println!(
        "profile routes/SuccinctArchive: {:?}",
        route_stats(&routes_archive, markers)
    );
}
