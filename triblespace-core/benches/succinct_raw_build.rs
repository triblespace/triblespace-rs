//! Compares canonical SimpleArchive -> raw SuccinctArchive construction with
//! the historical query-runtime/Rank9 path used only to recover the raw blob.
//!
//! The allocation figures are requested-byte proxies from the global allocator:
//! total bytes requested while the operation ran and maximum additional live
//! requested bytes above its starting baseline. Run with:
//!
//! ```text
//! cargo bench -p triblespace-core --bench succinct_raw_build
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anybytes::Bytes;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob,
};
use triblespace_core::blob::{Blob, IntoBlob, TryFromBlob};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);

struct MeasuringAllocator;

fn record_live(delta: isize) {
    let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
    if MEASURING.load(Ordering::Relaxed) {
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for MeasuringAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_live(layout.size() as isize);
            if MEASURING.load(Ordering::Relaxed) {
                ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_live(layout.size() as isize);
            if MEASURING.load(Ordering::Relaxed) {
                ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record_live(-(layout.size() as isize));
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            record_live(new_size as isize - layout.size() as isize);
            if MEASURING.load(Ordering::Relaxed) {
                ALLOCATED.fetch_add(new_size, Ordering::Relaxed);
                ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            }
        }
        new_pointer
    }
}

#[global_allocator]
static ALLOCATOR: MeasuringAllocator = MeasuringAllocator;

#[derive(Clone, Copy)]
struct Measurement {
    elapsed: Duration,
    allocations: usize,
    allocated_bytes: usize,
    peak_live_bytes: usize,
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, Measurement) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    ALLOCATED.store(0, Ordering::Relaxed);
    ALLOCATIONS.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::SeqCst);
    let start = Instant::now();
    let output = operation();
    let elapsed = start.elapsed();
    MEASURING.store(false, Ordering::SeqCst);
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline) as usize;
    (
        output,
        Measurement {
            elapsed,
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED.load(Ordering::Relaxed),
            peak_live_bytes: peak,
        },
    )
}

fn id(prefix: u8, ordinal: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = prefix;
    id[8..].copy_from_slice(&ordinal.to_be_bytes());
    id
}

/// Eight facts per entity, a small shared attribute set, and unique values.
/// This gives the domain realistic reuse without making the benchmark depend
/// on PATCH construction before the measured operation.
fn source(rows: usize) -> Blob<SimpleArchive> {
    let mut tribles = Vec::with_capacity(rows);
    for ordinal in 0..rows {
        let entity = ordinal / 8;
        let attribute = ordinal % 8;
        let mut row = [0u8; 64];
        row[..16].copy_from_slice(&id(1, entity as u64));
        row[16..32].copy_from_slice(&id(2, attribute as u64));
        row[32] = 0x80;
        row[56..].copy_from_slice(&(ordinal as u64).to_be_bytes());
        tribles.push(row);
    }
    Blob::new(Bytes::from(tribles))
}

fn old_runtime_path(source: &Blob<SimpleArchive>) -> Blob<SuccinctArchiveBlob> {
    let archive = SuccinctArchive::<OrderedUniverse>::try_from_blob(source.clone()).unwrap();
    let raw: Blob<SuccinctArchiveBlob> = archive.to_blob();
    raw
}

fn print_measurement(label: &str, rows: usize, measurement: Measurement) {
    println!(
        "{label:>12}  N={rows:>8}  {:>8.1} ms  {:>8.1} Mrow/s  allocs={:>8}  requested={:>9.1} MiB  peak-live={:>9.1} MiB",
        measurement.elapsed.as_secs_f64() * 1_000.0,
        rows as f64 / measurement.elapsed.as_secs_f64() / 1_000_000.0,
        measurement.allocations,
        measurement.allocated_bytes as f64 / (1024.0 * 1024.0),
        measurement.peak_live_bytes as f64 / (1024.0 * 1024.0),
    );
}

fn main() {
    let warm = source(1_024);
    drop(SuccinctArchiveBlob::build_from_simple_archive(&warm).unwrap());
    drop(old_runtime_path(&warm));

    for rows in [10_000usize, 100_000, 500_000] {
        let source = source(rows);
        let (raw, raw_measurement) =
            measure(|| SuccinctArchiveBlob::build_from_simple_archive(&source).unwrap());
        let (legacy, legacy_measurement) = measure(|| old_runtime_path(&source));
        assert_eq!(raw.bytes, legacy.bytes);
        println!(
            "\nportable bytes: {} ({:.1} B/row)",
            raw.bytes.len(),
            raw.bytes.len() as f64 / rows as f64
        );
        print_measurement("raw-only", rows, raw_measurement);
        print_measurement("runtime", rows, legacy_measurement);
        println!(
            "       delta  time={:.2}x  requested={:.2}x  peak-live={:.2}x",
            legacy_measurement.elapsed.as_secs_f64() / raw_measurement.elapsed.as_secs_f64(),
            legacy_measurement.allocated_bytes as f64 / raw_measurement.allocated_bytes as f64,
            legacy_measurement.peak_live_bytes as f64 / raw_measurement.peak_live_bytes as f64,
        );
        drop((raw, legacy, source));
    }
}
