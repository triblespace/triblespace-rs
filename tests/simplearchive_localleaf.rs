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
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::time::Instant;
use triblespace::core::blob::encodings::simplearchive::{try_from_blob_heap_only, SimpleArchive};
use triblespace::core::blob::Blob;
use triblespace::core::inline::Encodes;
use triblespace::core::trible::Trible;
use triblespace::core::trible::TribleSet;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);
static COUNTING: AtomicUsize = AtomicUsize::new(0);
/// Serializes tests that use the counting allocator so concurrent
/// tests in this file don't pollute each other's counters.
static COUNTING_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

/// Two archive-backed TribleSets with **overlapping** keys, unioned
/// together. Decode chunks are sorted-disjoint, so the existing
/// parallel-reduce path never hits a LocalLeaf-vs-LocalLeaf merge.
/// This test triggers that path on purpose to keep the LocalLeaf-aware
/// union honest: it would have panicked at `unreachable!()` before
/// the `union` work in the wee hours of 2026-06-03.
/// Regression: union two archive-backed TribleSets with overlapping
/// keys *from different archive Arcs*. The merge path drives both
/// the six-index `TribleSet` fan-out and PATCH's owner-aware merge,
/// exercising every LocalLeaf-vs-LocalLeaf collision in the trie. The
/// sources are dropped before any
/// reads so the result must keep all LocalLeaves' backing bytes
/// alive transitively via the surviving PATCH owner sets.
#[test]
fn union_two_overlapping_archives() {
    // Serialize with the decode-allocation test — they share the
    // process-global counting allocator and would race on its state
    // if run in parallel.
    let _guard = COUNTING_LOCK.lock().expect("counting mutex poisoned");
    // Big enough to engage TribleSet's six-index fan-out and many nested
    // owner-reconciliation paths.
    const N: usize = 8_192;

    // Build two archive-backed TribleSets whose keys overlap heavily.
    // Both are decoded from SimpleArchive blobs so they're LocalLeaf-
    // backed, *with different owner Arcs* — the two blobs are
    // independent so `try_from_blob_inner` wraps each in its own
    // `Arc<Bytes>`. After the union consumes both inputs, the
    // merged tree must keep all LocalLeaves' underlying bytes
    // alive transitively (via the root owner sets on the surviving
    // PATCHes) — otherwise we get a use-after-free when we
    // walk the merged data.
    let mut a_src = TribleSet::new();
    let mut b_src = TribleSet::new();
    for i in 0..N as u64 {
        a_src.insert(&make_trible(i));
        b_src.insert(&make_trible(i + N as u64 / 2));
    }
    let expected_len = (0..N as u64 + N as u64 / 2).count();

    let a_blob: Blob<SimpleArchive> = SimpleArchive::encode(&a_src);
    let b_blob: Blob<SimpleArchive> = SimpleArchive::encode(&b_src);

    // Drop the source TribleSets so the union's correctness depends
    // entirely on the archive Arcs the LocalLeaves carry.
    drop(a_src);
    drop(b_src);

    let a: TribleSet = triblespace::core::blob::TryFromBlob::try_from_blob(a_blob).unwrap();
    let b: TribleSet = triblespace::core::blob::TryFromBlob::try_from_blob(b_blob).unwrap();

    // These sizes cross the public TribleSet parallel gates for all three
    // operations. The borrowed results must retain both (intersect) or the
    // left-hand (difference) archive owners independently of their sources.
    let intersection = a.intersect(&b);
    let difference = a.difference(&b);
    let unioned = a + b;
    assert_eq!(
        unioned.len(),
        expected_len,
        "archive-vs-archive union should contain every distinct key"
    );
    for stats in [
        unioned.eav.node_stats(),
        unioned.eva.node_stats(),
        unioned.aev.node_stats(),
        unioned.ave.node_stats(),
        unioned.vea.node_stats(),
        unioned.vae.node_stats(),
    ] {
        assert_eq!(stats.2, 0, "parallel cross-owner union reified heap Leafs");
        assert_eq!(
            stats.3, expected_len as u64,
            "parallel cross-owner union lost LocalLeaf storage",
        );
    }
    for (set, expected, operation) in [
        (&intersection, N / 2, "parallel cross-owner intersection"),
        (&difference, N / 2, "parallel cross-owner difference"),
    ] {
        assert_eq!(set.len(), expected);
        for stats in [
            set.eav.node_stats(),
            set.eva.node_stats(),
            set.aev.node_stats(),
            set.ave.node_stats(),
            set.vea.node_stats(),
            set.vae.node_stats(),
        ] {
            assert_eq!(stats.2, 0, "{operation} reified heap Leafs");
            assert_eq!(
                stats.3, expected as u64,
                "{operation} lost LocalLeaf storage",
            );
        }
    }

    // Walk every key via the eav iterator — if any LocalLeaf points
    // into freed archive bytes, this will read garbage or fault.
    let count: usize = unioned.eav.iter_ordered().count();
    assert_eq!(count, expected_len);
    drop(unioned);
    let union_noise = vec![0xa5u8; N * 64];
    std::hint::black_box(&union_noise);
    assert_eq!(intersection.eav.iter_ordered().count(), N / 2);
    drop(intersection);
    let intersection_noise = vec![0x5au8; N * 64];
    std::hint::black_box(&intersection_noise);
    assert_eq!(difference.eav.iter_ordered().count(), N / 2);
}

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

#[test]
fn simplearchive_batch_layout_and_all_index_parity() {
    let _guard = COUNTING_LOCK.lock().expect("counting mutex poisoned");

    for n in 0usize..=3 {
        let mut source = TribleSet::new();
        for i in 0..n as u64 {
            source.insert(&make_trible(i));
        }
        let expected: BTreeSet<[u8; 64]> = source.eav.iter_ordered().copied().collect();
        let archive: Blob<SimpleArchive> = SimpleArchive::encode(&source);
        drop(source);

        let decoded: TribleSet = archive.try_from_blob().unwrap();
        assert_eq!(decoded.len(), n);
        let stats = [
            decoded.eav.node_stats(),
            decoded.eva.node_stats(),
            decoded.aev.node_stats(),
            decoded.ave.node_stats(),
            decoded.vea.node_stats(),
            decoded.vae.node_stats(),
        ];
        match n {
            0 => assert!(stats.iter().all(|stat| *stat == (0, 0, 0, 0))),
            1 => assert!(stats.iter().all(|stat| *stat == (0, 0, 0, 1))),
            2 => assert!(stats.iter().all(|stat| *stat == (1, 2, 0, 2))),
            _ => assert!(stats.iter().all(|stat| stat.2 == 0 && stat.3 == n as u64)),
        }

        macro_rules! assert_index {
            ($index:expr) => {{
                let actual: BTreeSet<[u8; 64]> = $index.iter_ordered().copied().collect();
                assert_eq!(actual, expected);
            }};
        }
        assert_index!(decoded.eav);
        assert_index!(decoded.eva);
        assert_index!(decoded.aev);
        assert_index!(decoded.ave);
        assert_index!(decoded.vea);
        assert_index!(decoded.vae);

        if n == 1 {
            let pointers = [
                decoded.eav.iter().next().unwrap().as_ptr(),
                decoded.eva.iter().next().unwrap().as_ptr(),
                decoded.aev.iter().next().unwrap().as_ptr(),
                decoded.ave.iter().next().unwrap().as_ptr(),
                decoded.vea.iter().next().unwrap().as_ptr(),
                decoded.vae.iter().next().unwrap().as_ptr(),
            ];
            assert!(pointers.iter().all(|pointer| *pointer == pointers[0]));
        }

        if n == 3 {
            let surviving_clone = decoded.clone();
            drop(decoded);
            let noise = vec![0x5au8; 3 * 64 * 32];
            std::hint::black_box(&noise);
            assert_eq!(surviving_clone.eav.iter_ordered().count(), 3);
            assert_eq!(surviving_clone.eva.iter_ordered().count(), 3);
            assert_eq!(surviving_clone.aev.iter_ordered().count(), 3);
            assert_eq!(surviving_clone.ave.iter_ordered().count(), 3);
            assert_eq!(surviving_clone.vea.iter_ordered().count(), 3);
            assert_eq!(surviving_clone.vae.iter_ordered().count(), 3);
        }
    }
}

#[cfg(feature = "parallel")]
#[test]
fn simplearchive_parallel_chunks_remain_all_local() {
    let _guard = COUNTING_LOCK.lock().expect("counting mutex poisoned");
    const N: usize = 4_096;

    let mut source = TribleSet::new();
    for i in 0..N as u64 {
        source.insert(&make_trible(i));
    }
    let archive: Blob<SimpleArchive> = SimpleArchive::encode(&source);
    drop(source);

    // Force four non-singleton chunks, each of which must bootstrap directly
    // from its first pair before the same-owner parallel reduction.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let decoded: TribleSet = pool.install(|| archive.try_from_blob().unwrap());
    let stats = [
        decoded.eav.node_stats(),
        decoded.eva.node_stats(),
        decoded.aev.node_stats(),
        decoded.ave.node_stats(),
        decoded.vea.node_stats(),
        decoded.vae.node_stats(),
    ];
    assert!(stats.iter().all(|stat| stat.2 == 0 && stat.3 == N as u64));
    assert_eq!(decoded.eav.iter_ordered().count(), N);
    assert_eq!(decoded.eva.iter_ordered().count(), N);
    assert_eq!(decoded.aev.iter_ordered().count(), N);
    assert_eq!(decoded.ave.iter_ordered().count(), N);
    assert_eq!(decoded.vea.iter_ordered().count(), N);
    assert_eq!(decoded.vae.iter_ordered().count(), N);
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
    let _guard = COUNTING_LOCK.lock().expect("counting mutex poisoned");
    // Both paths share the same parallel-reduce gate, so at small N they're
    // naturally serial and at large N both build chunks via rayon. To keep
    // the *per-trible* signal comparable across scales, set
    // `RAYON_NUM_THREADS=1` when running this test:
    //
    //     RAYON_NUM_THREADS=1 cargo test --release --test simplearchive_localleaf
    //
    // Otherwise expect the archive vs heap ratio to widen as N
    // crosses the rayon threshold purely due to thread count, not
    // per-insert cost.
    measure_at(1_024);
    measure_at(4_095);
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

    // Heap-Leaf baseline: same SimpleArchive ingest pipeline, but the
    // owner Arc is forced to None so every trible allocates a fresh
    // heap `Leaf`. This isolates the cost of the per-trible Leaf
    // alloc from any unrelated validation/iteration overhead.
    let (heap_set, heap_allocs, heap_bytes) =
        measure(|| -> TribleSet { try_from_blob_heap_only(archive.clone()).unwrap() });
    assert_eq!(heap_set.len(), N);

    // LocalLeaf archive ingest: identical validation/iteration, but
    // each trible lands as a LocalLeaf backed by the shared owner Arc.
    let (archive_set, archive_allocs, archive_bytes) =
        measure(|| -> TribleSet { archive.clone().try_from_blob().unwrap() });
    assert_eq!(archive_set.len(), N);
    if N >= 2 {
        let stats = [
            archive_set.eav.node_stats(),
            archive_set.eva.node_stats(),
            archive_set.aev.node_stats(),
            archive_set.ave.node_stats(),
            archive_set.vea.node_stats(),
            archive_set.vae.node_stats(),
        ];
        assert!(stats.iter().all(|stat| stat.2 == 0 && stat.3 == N as u64));
    }

    // Wall-clock timing: warm up once, then take a min-of-3 to filter
    // GC/allocator noise. Min beats mean for tight ingest loops where
    // the floor is the signal and the tail is OS jitter.
    let iters = 3usize;
    let _ = try_from_blob_heap_only(archive.clone()).unwrap();
    let heap_time = (0..iters)
        .map(|_| {
            let t = Instant::now();
            let s = try_from_blob_heap_only(archive.clone()).unwrap();
            let d = t.elapsed();
            drop(s);
            d
        })
        .min()
        .unwrap();
    let _ = archive.clone().try_from_blob::<TribleSet>().unwrap();
    let archive_time = (0..iters)
        .map(|_| {
            let t = Instant::now();
            let s: TribleSet = archive.clone().try_from_blob().unwrap();
            let d = t.elapsed();
            drop(s);
            d
        })
        .min()
        .unwrap();

    let heap_per = heap_bytes as f64 / N as f64;
    let archive_per = archive_bytes as f64 / N as f64;
    let savings_pct = (1.0 - archive_per / heap_per) * 100.0;
    let speedup = heap_time.as_nanos() as f64 / archive_time.as_nanos() as f64;
    let time_savings = (1.0 - 1.0 / speedup) * 100.0;

    eprintln!("--- N={N} ---");
    eprintln!(
        "heap path:    allocs={heap_allocs}, alloc_bytes={heap_bytes}, \
         bytes/trible={heap_per:.2}, time={:?}",
        heap_time
    );
    eprintln!(
        "archive path: allocs={archive_allocs}, alloc_bytes={archive_bytes}, \
         bytes/trible={archive_per:.2}, time={:?}",
        archive_time
    );
    eprintln!(
        "memory savings: {savings_pct:.1}%, time speedup: {speedup:.2}× \
         ({time_savings:.1}% faster)"
    );

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
