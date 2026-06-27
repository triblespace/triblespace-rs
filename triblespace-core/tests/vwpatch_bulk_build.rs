//! Correctness gate for the optimal sorted bulk archive build
//! (`VWPATCH::from_sorted_archive`).
//!
//! Small-scale (random keys, no fixture) so it runs fast in DEBUG and catches
//! logic bugs before the 10M release bench. Proves, over the same key set:
//!   - bulk build's set-hash == incremental archive build's set-hash (XOR
//!     oracle: identical key sets ⇒ identical root hash, structure-independent);
//!   - bulk build's branch count is <= the incremental build's (the whole point
//!     of seeing every key at once: widest spans, fewer over-splits);
//!   - bulk build is all LocalLeaves (heap_leaf == 0, local_leaf_slots == n);
//!   - every present key is found, absent keys rejected, has_prefix at a 16-byte
//!     segment boundary correct.
//!
//! Run: cargo test -p triblespace-core --features vwpatch --test vwpatch_bulk_build
#![cfg(feature = "vwpatch")]

use std::sync::Arc;

use triblespace_core::trible::{EAVOrder, VEAOrder, TRIBLE_LEN};
use triblespace_core::vwpatch::{ArchiveEntry, ArchiveOwner, KeySchema, VWPATCH};

type Key = [u8; TRIBLE_LEN];

struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// 16-byte-aligned archive buffer (Vec<u128> base ⇒ every 64-byte stride is
/// 16-aligned, which `ArchiveEntry::new` requires for the LocalLeaf tag bits).
struct AlignedArchive {
    words: Vec<u128>,
    n: usize,
}
impl AlignedArchive {
    fn from_keys(keys: &[Key]) -> Self {
        let n = keys.len();
        let mut words = vec![0u128; n * (TRIBLE_LEN / 16)];
        unsafe {
            std::ptr::copy_nonoverlapping(
                keys.as_ptr() as *const u8,
                words.as_mut_ptr() as *mut u8,
                n * TRIBLE_LEN,
            );
        }
        AlignedArchive { words, n }
    }
    fn base(&self) -> *const u8 {
        self.words.as_ptr() as *const u8
    }
    fn keys(&self) -> &[Key] {
        unsafe { std::slice::from_raw_parts(self.base() as *const Key, self.n) }
    }
    fn ptr(&self, i: usize) -> std::ptr::NonNull<Key> {
        let p = unsafe { self.base().add(i * TRIBLE_LEN) } as *mut Key;
        assert_eq!(p as usize & 0x0f, 0);
        unsafe { std::ptr::NonNull::new_unchecked(p) }
    }
}

fn distinct_keys(n: usize, seed: u64) -> Vec<Key> {
    let mut rng = SplitMix64(seed);
    let mut set = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let mut k = [0u8; TRIBLE_LEN];
        for w in k.chunks_exact_mut(8) {
            w.copy_from_slice(&rng.next().to_le_bytes());
        }
        if set.insert(k) {
            out.push(k);
        }
    }
    out
}

fn gate<O: KeySchema<TRIBLE_LEN>>(name: &str, arch: &AlignedArchive) {
    let n = arch.n;
    let owner_bulk: Arc<dyn ArchiveOwner> = Arc::new(());
    let owner_inc: Arc<dyn ArchiveOwner> = Arc::new(());

    // Bulk build.
    let entries: Vec<ArchiveEntry<'_, TRIBLE_LEN>> =
        (0..n).map(|i| unsafe { ArchiveEntry::new(arch.ptr(i), &owner_bulk) }).collect();
    let bulk = VWPATCH::<TRIBLE_LEN, O, ()>::from_sorted_archive(entries, owner_bulk.clone());

    // Incremental archive build.
    let mut inc = VWPATCH::<TRIBLE_LEN, O, ()>::new();
    for i in 0..n {
        let e = unsafe { ArchiveEntry::new(arch.ptr(i), &owner_inc) };
        inc.insert_archive(&e);
    }

    let (b_branches, b_slots, b_heap, b_local) = bulk.node_stats();
    let (i_branches, _i_slots, _i_heap, _i_local) = inc.node_stats();

    // Set-hash oracle: identical key sets ⇒ identical root hash.
    assert_eq!(bulk, inc, "[{name}] bulk vs incremental root-hash mismatch");

    // Bulk should reach <= incremental branch count (the whole point).
    assert!(
        b_branches <= i_branches,
        "[{name}] bulk branches {b_branches} > incremental {i_branches}"
    );

    // All LocalLeaves (root is a Branch carrying the owner).
    assert_eq!(b_heap, 0, "[{name}] bulk heap leaves = {b_heap}, expected 0");
    assert_eq!(
        b_local, n as u64,
        "[{name}] bulk local_leaf_slots {b_local} != n {n}"
    );
    assert_eq!(bulk.len(), n as u64, "[{name}] bulk len {} != {n}", bulk.len());

    // Every present key found (tree-ordered query), each absent key rejected.
    let keys = arch.keys();
    let tree_set: std::collections::HashSet<Key> =
        keys.iter().map(|k| O::tree_ordered(k)).collect();
    for tk in &tree_set {
        assert!(bulk.get(tk).is_some(), "[{name}] bulk lost a key");
        let mut miss = *tk;
        miss[TRIBLE_LEN - 1] ^= 0xff;
        if !tree_set.contains(&miss) {
            assert!(bulk.get(&miss).is_none(), "[{name}] bulk false positive");
        }
    }

    // has_prefix at the 16-byte segment boundary (tree-ordered).
    let tree0 = O::tree_ordered(&keys[0]);
    let mut seg16 = [0u8; 16];
    seg16.copy_from_slice(&tree0[..16]);
    assert!(bulk.has_prefix(&seg16), "[{name}] bulk has_prefix(seg16) failed");

    println!(
        "[{name}] n={n} bulk_branches={b_branches} ({:.4}/key) slots={b_slots} \
         | incremental_branches={i_branches} ({:.4}/key) | bulk/inc={:.3}",
        b_branches as f64 / n as f64,
        i_branches as f64 / n as f64,
        b_branches as f64 / i_branches as f64,
    );
}

#[test]
fn bulk_build_matches_and_compresses() {
    let keys = distinct_keys(60_000, 0x1234_5678_9abc_def0);
    let arch = AlignedArchive::from_keys(&keys);
    gate::<EAVOrder>("eav", &arch);
    gate::<VEAOrder>("vea", &arch);
}
