//! Focused SimpleArchive decode benchmark for PATCH insertion-hash experiments.
//!
//! Run with `ARCHIVE_BENCH_WORKERS=1` or `16`. Each reported sample is a
//! batch average; fixture construction and validation stay outside timing.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use triblespace::core::blob::encodings::simplearchive::SimpleArchive;
use triblespace::core::blob::Blob;
use triblespace::core::inline::Encodes;
use triblespace::core::trible::{Trible, TribleSet};

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: forwarding the allocator contract unchanged to System.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` came from the matching System allocation.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
enum Workload {
    Decode,
    DecodeFingerprint,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::DecodeFingerprint => "decode_fingerprint",
        }
    }
}

fn make_trible(i: u64) -> Trible {
    let mut data = [0u8; 64];
    data[..8].copy_from_slice(&i.to_be_bytes());
    data[8] = 1;
    data[16..24].copy_from_slice(&(i ^ 0xdead_beef_dead_beef).to_be_bytes());
    data[24] = 2;
    data[32..40].copy_from_slice(&i.to_be_bytes());
    data[40..48].copy_from_slice(&i.wrapping_mul(31).to_be_bytes());
    Trible::force_raw(data).expect("fixture entity and attribute are non-nil")
}

fn fixture(n: usize) -> Blob<SimpleArchive> {
    let mut source = TribleSet::new();
    for i in 0..n as u64 {
        source.insert(&make_trible(i));
    }
    assert_eq!(source.len(), n);
    let archive = SimpleArchive::encode(&source);
    assert_eq!(archive.bytes.len(), n * 64);
    archive
}

fn run_once(archive: &Blob<SimpleArchive>, n: usize, workload: Workload) {
    let set: TribleSet = archive.clone().try_from_blob().expect("valid fixture");
    assert_eq!(set.len(), n);
    match workload {
        Workload::Decode => {
            black_box(&set);
        }
        Workload::DecodeFingerprint => {
            black_box(set.fingerprint());
        }
    }
    // Include destruction in the timed unit. Otherwise delayed allocator work
    // can leak from one sample into the next.
    drop(set);
}

fn allocation_sample(
    pool: &rayon::ThreadPool,
    archive: &Blob<SimpleArchive>,
    n: usize,
    workload: Workload,
) -> (usize, usize) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::SeqCst);
    pool.install(|| run_once(archive, n, workload));
    COUNT_ALLOCATIONS.store(false, Ordering::SeqCst);
    (
        ALLOCATIONS.load(Ordering::Relaxed),
        ALLOCATED_BYTES.load(Ordering::Relaxed),
    )
}

fn loops_for(n: usize) -> usize {
    match n {
        0..=1_024 => 200,
        1_025..=10_000 => 20,
        _ => 2,
    }
}

fn median(values: &[f64]) -> f64 {
    values[values.len() / 2]
}

fn main() {
    let workers: usize = std::env::var("ARCHIVE_BENCH_WORKERS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .expect("ARCHIVE_BENCH_WORKERS must be a positive integer");
    let variant = std::env::var("ARCHIVE_BENCH_VARIANT").unwrap_or_else(|_| "unknown".into());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .expect("rayon pool");

    println!("meta,variant={variant},workers={workers},samples=11");
    for n in [1_024usize, 10_000, 100_000] {
        let archive = fixture(n);
        for workload in [Workload::Decode, Workload::DecodeFingerprint] {
            // Warm both the allocator and the worker pool before measuring.
            pool.install(|| run_once(&archive, n, workload));
            let (allocations, allocated_bytes) = allocation_sample(&pool, &archive, n, workload);
            let loops = loops_for(n);
            let mut samples = Vec::with_capacity(11);
            for round in 0..11 {
                let started = Instant::now();
                pool.install(|| {
                    for _ in 0..loops {
                        run_once(&archive, n, workload);
                    }
                });
                let ns = started.elapsed().as_nanos() as f64 / loops as f64;
                println!(
                    "sample,variant={variant},workers={workers},mode={},n={n},loops={loops},round={round},ns={ns:.3}",
                    workload.name(),
                );
                samples.push(ns);
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "result,variant={variant},workers={workers},mode={},n={n},median_ns={:.3},p25_ns={:.3},p75_ns={:.3},min_ns={:.3},max_ns={:.3},allocs={allocations},alloc_bytes={allocated_bytes}",
                workload.name(),
                median(&samples),
                samples[2],
                samples[8],
                samples[0],
                samples[10],
            );
        }
    }
}
