//! Measures heap-allocation behaviour of `SimpleArchive` ingestion to
//! verify the `LocalLeaf` path actually eliminates per-trible heap
//! `Leaf` allocations.
//!
//! Strategy: install a counting global allocator, build a TribleSet
//! through the normal `insert` path (forcing heap Leaves), encode to
//! a `SimpleArchive` blob, then decode through `try_from_blob` while
//! counting allocations performed during the decode. The Leaf-heap
//! path costs ~96 bytes per trible; the LocalLeaf path costs zero
//! per-trible alloc bytes (Branch overhead grows with tree shape,
//! not trible count).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use triblespace::core::blob::encodings::simplearchive::SimpleArchive;
use triblespace::core::blob::Blob;
use triblespace::core::inline::Encodes;
use triblespace::core::trible::Trible;
use triblespace::core::trible::TribleSet;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) != 0 {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: CountingAllocator = CountingAllocator;

fn make_trible(i: u64) -> Trible {
    let mut data = [0u8; 64];
    data[..8].copy_from_slice(&i.to_be_bytes());
    data[8] = 1;
    data[16..24].copy_from_slice(&(i ^ 0xdead_beef_dead_beef).to_be_bytes());
    data[24] = 2;
    data[32..40].copy_from_slice(&i.to_be_bytes());
    data[40..48].copy_from_slice(&(i.wrapping_mul(31)).to_be_bytes());
    Trible::force_raw(data).expect("non-nil entity/attribute")
}

fn measure<R>(f: impl FnOnce() -> R) -> (R, usize, usize) {
    ALLOCS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(1, Ordering::Relaxed);
    let r = f();
    COUNTING.store(0, Ordering::Relaxed);
    (
        r,
        ALLOCS.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

#[test]
fn simplearchive_decode_uses_archive_owner() {
    measure_at(1_024);
    measure_at(10_000);
    measure_at(100_000);
}

#[allow(non_snake_case)]
fn measure_at(n: usize) {
    let N = n;

    let mut source = TribleSet::new();
    for i in 0..N as u64 {
        source.insert(&make_trible(i));
    }
    assert_eq!(source.len(), N);

    let archive: Blob<SimpleArchive> = SimpleArchive::encode(&source);
    assert_eq!(archive.bytes.len(), N * 64);

    // Baseline: rebuild the same TribleSet via the heap-Leaf path by
    // re-inserting each trible. The archive's `Bytes` view still
    // exists, but every Leaf hitting the PATCH is freshly allocated.
    let (heap_set, heap_allocs, heap_bytes) = measure(|| {
        let mut s = TribleSet::new();
        for i in 0..N as u64 {
            s.insert(&make_trible(i));
        }
        s
    });
    assert_eq!(heap_set.len(), N);

    // Decode through the archive ingest path; LocalLeaf eliminates the
    // per-trible heap Leaf allocation, leaving only Branch overhead.
    let (archive_set, archive_allocs, archive_bytes) =
        measure(|| -> TribleSet { archive.try_from_blob().unwrap() });
    assert_eq!(archive_set.len(), N);

    let heap_per = heap_bytes as f64 / N as f64;
    let archive_per = archive_bytes as f64 / N as f64;
    let savings_pct = (1.0 - archive_per / heap_per) * 100.0;

    eprintln!("--- N={N} ---");
    eprintln!(
        "heap path:    allocs={heap_allocs}, alloc_bytes={heap_bytes}, \
         bytes/trible={heap_per:.2}"
    );
    eprintln!(
        "archive path: allocs={archive_allocs}, alloc_bytes={archive_bytes}, \
         bytes/trible={archive_per:.2}"
    );
    eprintln!("savings: {savings_pct:.1}%");

    assert!(
        archive_bytes < heap_bytes,
        "archive-ingest path should allocate fewer bytes than the \
         heap-Leaf path (heap={heap_bytes}, archive={archive_bytes})"
    );
    assert!(
        archive_allocs < heap_allocs,
        "archive-ingest path should perform fewer allocations than \
         the heap-Leaf path (heap_allocs={heap_allocs}, \
         archive_allocs={archive_allocs})"
    );
}
