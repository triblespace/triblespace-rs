//! ARCHIVE-REGIME (zero-copy LocalLeaf) memory for single-byte PATCH vs
//! variable-width VWPATCH — the real "load a pile from disk and query it"
//! production profile that `vwpatch_realmem` never touched (it used the
//! heap-`Leaf` build-from-scratch path).
//!
//! A `LocalLeaf` is a tagged pointer into archive-resident bytes — no
//! per-key heap `Leaf` allocation. The production loader this mirrors is
//! `impl TryFromBlob<SimpleArchive> for TribleSet`
//! (`src/blob/encodings/simplearchive.rs`): align the blob bytes, wrap an
//! owner Arc, make an `ArchiveEntry` per trible, call `insert_archive`.
//!
//! For each ordering we build four indexes over the SAME 9.97M tribles and
//! measure actual allocator bytes (malloc_size) plus node_stats:
//!   1. archive-PATCH (insert_archive, shared owner)   — LocalLeaves
//!   2. archive-VWPATCH (insert_archive, shared owner) — LocalLeaves
//!   3. heap-PATCH (insert)                            — heap Leaves
//!   4. heap-VWPATCH (insert)                          — heap Leaves
//! and ASSERT the archive builds actually produce LocalLeaves (heap_leaf
//! ~0, local_leaf_slots == n). If vwpatch's local-leaf path silently
//! reifies to heap, the assertion fires — that is a real finding.
//!
//! Run: cargo bench -p triblespace-core --features vwpatch --bench vwpatch_archivemem

use std::alloc::{GlobalAlloc, Layout, System};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use triblespace_core::patch::{
    ArchiveEntry as PaArchiveEntry, ArchiveOwner as PaOwner, Entry as PatchEntry, KeySchema, PATCH,
};
use triblespace_core::trible::{AEVOrder, AVEOrder, EAVOrder, EVAOrder, VAEOrder, VEAOrder};
use triblespace_core::vwpatch::{
    ArchiveEntry as VwArchiveEntry, ArchiveOwner as VwOwner, Entry as VwEntry, VWPATCH,
};

extern "C" {
    fn malloc_size(ptr: *const core::ffi::c_void) -> usize;
}

static REQUESTED: AtomicI64 = AtomicI64::new(0);
static ACTUAL: AtomicI64 = AtomicI64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            REQUESTED.fetch_add(layout.size() as i64, Ordering::Relaxed);
            ACTUAL.fetch_add(malloc_size(p as *const _) as i64, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        REQUESTED.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        ACTUAL.fetch_sub(malloc_size(ptr as *const _) as i64, Ordering::Relaxed);
        System.dealloc(ptr, layout);
    }
}

#[global_allocator]
static A: Counting = Counting;

const TRIBLE_LEN: usize = 64;
type Key = [u8; TRIBLE_LEN];

/// 16-byte-aligned owner of the archive bytes. `Vec<u128>` guarantees a
/// 16-aligned base; every 64-byte stride from there is 16-aligned too.
/// `Send + Sync + 'static` means the blanket `ArchiveOwner` impls in both
/// `patch` and `vwpatch` apply.
struct AlignedArchive {
    words: Vec<u128>,
    n: usize,
}

impl AlignedArchive {
    fn load() -> Self {
        let bytes = std::fs::read("/tmp/facts.simplearchive").expect("fixture");
        assert!(bytes.len() % TRIBLE_LEN == 0, "not a multiple of 64");
        let n = bytes.len() / TRIBLE_LEN;
        // Copy into a Vec<u128> so the base is 16-byte aligned.
        let mut words: Vec<u128> = vec![0u128; bytes.len() / 16];
        // SAFETY: dst has exactly bytes.len() bytes; both are plain bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                words.as_mut_ptr() as *mut u8,
                bytes.len(),
            );
        }
        let base = words.as_ptr() as usize;
        debug_assert_eq!(base & 0x0f, 0, "archive base must be 16-byte aligned");
        AlignedArchive { words, n }
    }

    fn base(&self) -> *const u8 {
        self.words.as_ptr() as *const u8
    }

    /// Raw key slice view over the same bytes (for the heap `insert` path).
    fn keys(&self) -> &[Key] {
        // SAFETY: words holds n*64 contiguous bytes, 16-aligned (so 1-aligned
        // for [u8;64]); lifetime tied to &self.
        unsafe { std::slice::from_raw_parts(self.base() as *const Key, self.n) }
    }

    /// 16-byte-aligned pointer to trible `i`.
    fn ptr(&self, i: usize) -> NonNull<Key> {
        let p = unsafe { self.base().add(i * TRIBLE_LEN) } as *mut Key;
        debug_assert_eq!(p as usize & 0x0f, 0, "trible ptr must be 16-byte aligned");
        unsafe { NonNull::new_unchecked(p) }
    }
}

fn live() -> i64 {
    ACTUAL.load(Ordering::Relaxed)
}

/// Build inside the closure, return (actual_delta, result). Caller drops the
/// result after extracting stats so the next build starts near baseline.
fn measure<T>(build: impl FnOnce() -> T) -> (i64, T) {
    let a0 = live();
    let held = build();
    let a1 = live();
    (a1 - a0, held)
}

/// (branches, slots, heap_leaves, local_leaf_slots)
type Stats = (u64, u64, u64, u64);

fn ps(label: &str, bytes: i64, n: f64, s: Stats) {
    println!(
        "    {:<14} {:>7.2} B/tr | branches {:>9} slots {:>10} heap_leaves {:>9} local_leaf_slots {:>9}",
        label,
        bytes as f64 / n,
        s.0,
        s.1,
        s.2,
        s.3,
    );
}

/// Returns (archive_PATCH_bytes, bulk_VW_bytes) for summing into the full
/// 6-index TribleSet archive footprint.
fn run<O: KeySchema<TRIBLE_LEN>>(name: &str, arch: &AlignedArchive) -> (i64, i64) {
    let n = arch.n;
    let nf = n as f64;
    println!("\n=== ordering {name} ===");

    // Two owner Arcs over the SAME bytes — one typed for each trait. Both are
    // pure refcount handles; coercion is from a single Arc<AlignedArchive>-ish
    // pattern but the traits are distinct, so we build separate dyn Arcs.
    // We wrap a zero-sized marker keyed to the archive lifetime.
    struct Keep;
    let pa_owner: Arc<dyn PaOwner> = Arc::new(Keep);
    let vw_owner: Arc<dyn VwOwner> = Arc::new(Keep);

    // --- archive PATCH ---
    let (pa_arch_bytes, pa_arch) = measure(|| {
        let mut t = PATCH::<TRIBLE_LEN, O, ()>::new();
        for i in 0..n {
            // SAFETY: ptr is 16-aligned and stays valid (arch outlives t);
            // owner keeps the marker alive — bytes are kept alive by `arch`.
            let e = unsafe { PaArchiveEntry::new(arch.ptr(i), &pa_owner) };
            t.insert_archive(&e);
        }
        t
    });
    let pa_arch_stats = pa_arch.node_stats();

    // --- archive VWPATCH (INCREMENTAL insert, narrow-on-partial-data spans) ---
    let (vw_arch_bytes, vw_arch) = measure(|| {
        let mut t = VWPATCH::<TRIBLE_LEN, O, ()>::new();
        for i in 0..n {
            let e = unsafe { VwArchiveEntry::new(arch.ptr(i), &vw_owner) };
            t.insert_archive(&e);
        }
        t
    });
    let vw_arch_stats = vw_arch.node_stats();

    // --- archive VWPATCH (OPTIMAL SORTED BULK build, widest dense spans) ---
    // Every key in hand at once ⇒ `build_dense_node_owned` picks each span at
    // its widest valid width (segment-checkpoint + max_fanout capped), reaching
    // the bulk-optimal node count — vs the incremental build above, which
    // narrows spans on partial data and over-splits.
    // A SimpleArchive is sorted in EAV order ONLY. `from_sorted_archive` needs
    // its input sorted in THIS ordering's tree order, so sort the entry sequence
    // by `O::tree_ordered` first (a no-op permutation for EAV, a real sort for
    // the other five). Without this, a non-eav bulk build groups mis-sorted keys
    // and over-splits — never reaching its true bulk optimum.
    let keys_for_sort = arch.keys();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_unstable_by(|&a, &b| {
        O::tree_ordered(&keys_for_sort[a]).cmp(&O::tree_ordered(&keys_for_sort[b]))
    });
    let vw_bulk_owner: Arc<dyn VwOwner> = Arc::new(Keep);
    let (vw_bulk_bytes, vw_bulk) = measure(|| {
        let entries = order
            .iter()
            .map(|&i| unsafe { VwArchiveEntry::new(arch.ptr(i), &vw_bulk_owner) });
        VWPATCH::<TRIBLE_LEN, O, ()>::from_sorted_archive(entries, vw_bulk_owner.clone())
    });
    let vw_bulk_stats = vw_bulk.node_stats();

    // --- correctness spot-check on the archive indexes (LocalLeaf reads) ---
    let keys = arch.keys();
    let probe: [usize; 5] = [0, n / 4, n / 2, (3 * n) / 4, n - 1];
    for &i in &probe {
        // `get`/`has_prefix` index the query by tree position, so the lookup
        // key must be tree-ordered (identity for EAV, a real permutation for VEA).
        let tk = O::tree_ordered(&keys[i]);
        assert!(pa_arch.get(&tk).is_some(), "archive PATCH lost key {i}");
        assert!(vw_arch.get(&tk).is_some(), "archive VWPATCH lost key {i}");
        assert!(vw_bulk.get(&tk).is_some(), "bulk VWPATCH lost key {i}");
        // Negative control: flip a byte -> must be absent.
        let mut miss = tk;
        miss[0] ^= 0xff;
        assert!(pa_arch.get(&miss).is_none(), "archive PATCH false positive {i}");
        assert!(vw_bulk.get(&miss).is_none(), "bulk VWPATCH false positive {i}");
    }
    // Full positive sweep over the bulk build: every one of the 9.97M keys
    // must resolve (the decisive correctness gate for the optimal build).
    for k in keys {
        let tk = O::tree_ordered(k);
        debug_assert!(vw_bulk.get(&tk).is_some());
    }
    assert!(
        keys.iter().all(|k| vw_bulk.get(&O::tree_ordered(k)).is_some()),
        "bulk VWPATCH lost a key in the full sweep"
    );
    // has_prefix at a 16-byte segment boundary, in tree-ordered space.
    let tree0 = O::tree_ordered(&keys[0]);
    let mut seg16 = [0u8; 16];
    seg16.copy_from_slice(&tree0[..16]);
    assert!(
        pa_arch.has_prefix(&seg16),
        "archive PATCH has_prefix(seg16) failed"
    );
    assert!(
        vw_arch.has_prefix(&seg16),
        "archive VWPATCH has_prefix(seg16) failed"
    );
    assert!(
        vw_bulk.has_prefix(&seg16),
        "bulk VWPATCH has_prefix(seg16) failed"
    );

    // Set-hash oracle: the optimal bulk build and the incremental build must
    // agree bit-for-bit on the root hash (structure-independent XOR of leaf
    // hashes ⇒ identical key set). This is THE proof the bulk shape is correct.
    assert_eq!(
        vw_bulk, vw_arch,
        "bulk VWPATCH root hash != incremental VWPATCH root hash"
    );

    // --- ASSERT LocalLeaves actually formed (the does-it-work proof) ---
    // By design, the FIRST insert into an empty PATCH/VWPATCH becomes a
    // single heap `Leaf` at the standalone root (a root can't host the
    // owner field — only Branches do); every subsequent key is a
    // LocalLeaf. So the healthy invariant is heap_leaf == 1 and
    // local_leaf_slots == n-1. A genuine silent reification would show
    // heap_leaf ~= n instead — that is what these asserts catch.
    assert_eq!(
        pa_arch_stats.2, 1,
        "archive PATCH heap leaves = {} (expected exactly 1 root-bootstrap leaf)",
        pa_arch_stats.2
    );
    assert_eq!(
        pa_arch_stats.3,
        (n - 1) as u64,
        "archive PATCH local_leaf_slots {} != n-1 {}",
        pa_arch_stats.3,
        n - 1
    );
    assert_eq!(
        vw_arch_stats.2, 1,
        "ARCHIVE VWPATCH SILENTLY REIFIED TO HEAP: {} heap leaves (expected exactly 1)",
        vw_arch_stats.2
    );
    assert_eq!(
        vw_arch_stats.3,
        (n - 1) as u64,
        "archive VWPATCH local_leaf_slots {} != n-1 {} (local-leaf path failed)",
        vw_arch_stats.3,
        n - 1
    );
    // The BULK build's root is itself a Branch (it carries the owner), so it
    // has NO bootstrap heap leaf at all: heap_leaf == 0 and local_leaf_slots == n.
    assert_eq!(
        vw_bulk_stats.2, 0,
        "BULK VWPATCH SILENTLY REIFIED TO HEAP: {} heap leaves (expected 0)",
        vw_bulk_stats.2
    );
    assert_eq!(
        vw_bulk_stats.3, n as u64,
        "bulk VWPATCH local_leaf_slots {} != n {} (local-leaf path failed)",
        vw_bulk_stats.3, n
    );
    // The whole point: the optimal bulk build's branch count must come in WELL
    // below the incremental build's (incremental over-splits on partial data).
    assert!(
        vw_bulk_stats.0 < vw_arch_stats.0,
        "bulk branches {} not below incremental {}",
        vw_bulk_stats.0,
        vw_arch_stats.0
    );

    drop(pa_arch);
    drop(vw_arch);
    drop(vw_bulk);

    // --- heap PATCH ---
    let (pa_heap_bytes, pa_heap) = measure(|| {
        let mut t = PATCH::<TRIBLE_LEN, O, ()>::new();
        for k in keys {
            t.insert(&PatchEntry::new(k));
        }
        t
    });
    let pa_heap_stats = pa_heap.node_stats();
    drop(pa_heap);

    // --- heap VWPATCH ---
    let (vw_heap_bytes, vw_heap) = measure(|| {
        let mut t = VWPATCH::<TRIBLE_LEN, O, ()>::new();
        for k in keys {
            t.insert(&VwEntry::new(k));
        }
        t
    });
    let vw_heap_stats = vw_heap.node_stats();
    drop(vw_heap);

    ps("archive-PATCH", pa_arch_bytes, nf, pa_arch_stats);
    ps("archive-VW(inc)", vw_arch_bytes, nf, vw_arch_stats);
    ps("archive-VW(BULK)", vw_bulk_bytes, nf, vw_bulk_stats);
    ps("heap-PATCH", pa_heap_bytes, nf, pa_heap_stats);
    ps("heap-VW", vw_heap_bytes, nf, vw_heap_stats);
    println!(
        "    --> incremental archive vw/patch ratio {:.4}x ({:.2} / {:.2} B/tr) | heap vw/patch {:.4}x",
        vw_arch_bytes as f64 / pa_arch_bytes as f64,
        vw_arch_bytes as f64 / nf,
        pa_arch_bytes as f64 / nf,
        vw_heap_bytes as f64 / pa_heap_bytes as f64,
    );
    // THE DECISION NUMBER: optimal sorted bulk build vs single-byte archive
    // PATCH — maximum span compression meeting free LocalLeaves.
    println!(
        "    ==> BULK archive vw/patch ratio {:.4}x ({:.2} B/tr bulk-VW vs {:.2} B/tr PATCH)  \
         [bulk branches {} ({:.4}/key) slots {} | incremental branches {} ({:.4}/key) | PATCH branches {} slots {}]  \
         VERDICT: vwpatch is {} PATCH in its best case",
        vw_bulk_bytes as f64 / pa_arch_bytes as f64,
        vw_bulk_bytes as f64 / nf,
        pa_arch_bytes as f64 / nf,
        vw_bulk_stats.0,
        vw_bulk_stats.0 as f64 / nf,
        vw_bulk_stats.1,
        vw_arch_stats.0,
        vw_arch_stats.0 as f64 / nf,
        pa_arch_stats.0,
        pa_arch_stats.1,
        if vw_bulk_bytes < pa_arch_bytes { "SMALLER than" } else { "still LARGER than" },
    );
    (pa_arch_bytes, vw_bulk_bytes)
}

fn main() {
    let arch = AlignedArchive::load();
    println!(
        "archive: {} tribles ({} bytes), base {:#x} (16-aligned: {})",
        arch.n,
        arch.n * TRIBLE_LEN,
        arch.base() as usize,
        arch.base() as usize & 0x0f == 0,
    );

    // All 6 covering orderings. A SimpleArchive is eav-sorted; each non-eav
    // bulk build sorts into its own tree order first (inside `run`). Sum the
    // per-ordering archive footprints into the full TribleSet total — the real
    // "load a pile, query from a laptop" memory.
    let mut pa_total = 0i64;
    let mut vw_total = 0i64;
    for (pa, vw) in [
        run::<EAVOrder>("eav", &arch),
        run::<EVAOrder>("eva", &arch),
        run::<AEVOrder>("aev", &arch),
        run::<AVEOrder>("ave", &arch),
        run::<VEAOrder>("vea", &arch),
        run::<VAEOrder>("vae", &arch),
    ] {
        pa_total += pa;
        vw_total += vw;
    }
    let nf = arch.n as f64;
    println!(
        "\n===== FULL 6-INDEX TRIBLESET (archive regime, optimal bulk vwpatch) =====\n  \
         archive-PATCH  {:.1} B/tr\n  bulk-VWPATCH   {:.1} B/tr\n  RATIO vw/patch = {:.4}x  ({})",
        pa_total as f64 / nf,
        vw_total as f64 / nf,
        vw_total as f64 / pa_total as f64,
        if vw_total < pa_total { "vwpatch SMALLER" } else { "vwpatch LARGER" },
    );
}
