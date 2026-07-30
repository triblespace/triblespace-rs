//! Persistent Adaptive Trie with Cuckoo-compression and
//! Hash-maintenance (PATCH).
//!
//! See the [PATCH](../book/src/deep-dive/patch.md) chapter of the Tribles Book
//! for the full design description and hashing scheme.
//!
//! Values stored in leaves are not part of hashing or equality comparisons.
//! Two [`PATCH`](crate::patch::PATCH)es are considered equal if they contain the same set of keys,
//! even if the associated values differ. This allows using the structure as an
//! idempotent blobstore where a value's hash determines its key. Consequently,
//! union does not promise which value survives a duplicate key: cached-equal
//! subtrees may keep the left tree wholesale, while structurally traversed
//! (including dirty-hash) trees retain values according to the ordinary
//! in-place/swap path. Key-set semantics are invariant across both cases.
//!
#![allow(unstable_name_collisions)]

mod branch;
/// Byte-indexed lookup tables used by PATCH branch nodes.
pub mod bytetable;
mod entry;
mod leaf;

use arrayvec::ArrayVec;

/// Test-only accounting for allocation work while a known archive partition
/// is assembled into Branches. Explicit per-build sinks avoid global counters
/// and parallel-test interference; timed calls use the uncounted recursive
/// monomorphization.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BranchBuildStats {
    pub(crate) branches: u64,
    pub(crate) initial_slots: u64,
    pub(crate) grow_calls: u64,
    pub(crate) heads_moved_by_grow: u64,
    pub(crate) grow_scanned_slots: u64,
    pub(crate) grow_allocated_slots: u64,
    pub(crate) final_slots: u64,
}

/// Re-export of [`Entry`](entry::Entry).
use branch::*;
pub use entry::{ArchiveEntry, Entry};
use leaf::*;

/// Re-export of all byte table utilities.
pub use bytetable::*;
use rand::thread_rng;
use rand::RngCore;
use std::cmp::Reverse;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::{Arc, Once};

/// Marker trait for opaque owners of bytes referenced by archive-backed
/// PATCH leaves. The trait is intentionally empty: an owner exists solely to
/// keep its allocation alive while a `LocalLeaf` points into it.
pub trait ArchiveOwner: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + ?Sized> ArchiveOwner for T {}

/// One node in the exact persistent set of retained archive allocations.
///
/// This is a binary Patricia trie over owner allocation addresses. A branch's
/// `mask` is the highest bit at which the addresses below it differ; masks
/// strictly decrease on every root-to-leaf path. The shape is consequently a
/// canonical function of the address set, independent of insertion order, and
/// its height cannot exceed [`usize::BITS`]. Rebuilt insertion paths share all
/// untouched child Arcs with older PATCH snapshots.
enum OwnerNode {
    Owner {
        address: usize,
        owner: Arc<dyn ArchiveOwner>,
    },
    Branch {
        mask: usize,
        zero: Arc<Self>,
        one: Arc<Self>,
    },
}

/// Exact persistent set of archive allocations retained by one PATCH.
///
/// Membership is keyed by the allocation's data address. An owner remains live
/// in its leaf, so its address cannot be reused while it occurs in the set.
/// `latest_address` is deliberately separate from the canonical trie: archive
/// ingestion can recognize its overwhelmingly common repeated-owner case in
/// O(1), while older-owner adoption still has exact set semantics.
#[derive(Clone)]
struct OwnerCover {
    latest_address: usize,
    len: usize,
    root: Arc<OwnerNode>,
}

impl core::fmt::Debug for OwnerCover {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnerCover")
            .field("len", &self.len)
            .finish()
    }
}

impl OwnerNode {
    #[inline]
    fn takes_one(address: usize, mask: usize) -> bool {
        address & mask != 0
    }

    #[inline]
    fn critical_mask(left: usize, right: usize) -> usize {
        let differing = left ^ right;
        debug_assert_ne!(differing, 0);
        1usize << (usize::BITS - 1 - differing.leading_zeros())
    }

    fn matching_leaf(root: &Arc<Self>, address: usize) -> &Arc<Self> {
        let mut node = root;
        loop {
            match node.as_ref() {
                Self::Owner { .. } => return node,
                Self::Branch {
                    mask, zero, one, ..
                } => {
                    node = if Self::takes_one(address, *mask) {
                        one
                    } else {
                        zero
                    };
                }
            }
        }
    }

    #[cfg(any(debug_assertions, test))]
    fn contains(root: &Arc<Self>, address: usize) -> bool {
        matches!(
            Self::matching_leaf(root, address).as_ref(),
            Self::Owner {
                address: found,
                ..
            } if *found == address
        )
    }

    /// Persistently insert one owner, returning the unchanged root on a hit.
    fn insert(
        root: &Arc<Self>,
        address: usize,
        owner: &Arc<dyn ArchiveOwner>,
    ) -> (Arc<Self>, bool) {
        let matching = Self::matching_leaf(root, address);
        let Self::Owner {
            address: existing_address,
            owner: existing_owner,
        } = matching.as_ref()
        else {
            unreachable!("Patricia lookup must end at an owner leaf");
        };
        if *existing_address == address {
            debug_assert!(Arc::ptr_eq(existing_owner, owner));
            return (root.clone(), false);
        }

        let critical_mask = Self::critical_mask(address, *existing_address);
        let inserted = Arc::new(Self::Owner {
            address,
            owner: owner.clone(),
        });
        (
            Self::insert_at(root, inserted, address, *existing_address, critical_mask),
            true,
        )
    }

    fn insert_at(
        root: &Arc<Self>,
        inserted: Arc<Self>,
        inserted_address: usize,
        existing_address: usize,
        critical_mask: usize,
    ) -> Arc<Self> {
        if let Self::Branch { mask, zero, one } = root.as_ref() {
            if *mask > critical_mask {
                if Self::takes_one(inserted_address, *mask) {
                    return Arc::new(Self::Branch {
                        mask: *mask,
                        zero: zero.clone(),
                        one: Self::insert_at(
                            one,
                            inserted,
                            inserted_address,
                            existing_address,
                            critical_mask,
                        ),
                    });
                }
                return Arc::new(Self::Branch {
                    mask: *mask,
                    zero: Self::insert_at(
                        zero,
                        inserted,
                        inserted_address,
                        existing_address,
                        critical_mask,
                    ),
                    one: one.clone(),
                });
            }
            debug_assert_ne!(*mask, critical_mask);
        }

        debug_assert_ne!(
            Self::takes_one(inserted_address, critical_mask),
            Self::takes_one(existing_address, critical_mask),
        );
        let (zero, one) = if Self::takes_one(inserted_address, critical_mask) {
            (root.clone(), inserted)
        } else {
            (inserted, root.clone())
        };
        Arc::new(Self::Branch {
            mask: critical_mask,
            zero,
            one,
        })
    }

    fn for_each_owner<F>(&self, f: &mut F)
    where
        F: FnMut(usize, &Arc<dyn ArchiveOwner>),
    {
        match self {
            Self::Owner { address, owner } => f(*address, owner),
            Self::Branch { zero, one, .. } => {
                zero.for_each_owner(f);
                one.for_each_owner(f);
            }
        }
    }
}

impl OwnerCover {
    #[inline]
    fn address(owner: &Arc<dyn ArchiveOwner>) -> usize {
        Arc::as_ptr(owner) as *const () as usize
    }

    fn singleton(owner: &Arc<dyn ArchiveOwner>) -> Arc<Self> {
        let address = Self::address(owner);
        Arc::new(Self {
            latest_address: address,
            len: 1,
            root: Arc::new(OwnerNode::Owner {
                address,
                owner: owner.clone(),
            }),
        })
    }

    fn retain(current: &mut Option<Arc<Self>>, owner: &Arc<dyn ArchiveOwner>) {
        let address = Self::address(owner);
        let Some(existing) = current.as_mut() else {
            *current = Some(Self::singleton(owner));
            return;
        };
        if existing.latest_address == address {
            return;
        }

        // Build the persistent path before touching the installed cover. If
        // allocation panics, the old guard remains intact. Arc::make_mut then
        // clones only the three-word cover when six PATCH indexes share it.
        let (root, inserted) = OwnerNode::insert(&existing.root, address, owner);
        let cover = Arc::make_mut(existing);
        cover.root = root;
        cover.len += usize::from(inserted);
        cover.latest_address = address;
    }

    /// Transactionally replace `current` with its exact union with `other`.
    ///
    /// The installed receipt is not changed until the complete persistent trie
    /// exists. Thus a caught allocation panic leaves the original PATCH root
    /// guarded, while the caller keeps `other` live through the subsequent
    /// Head merge as before.
    fn merge_into(current: &mut Option<Arc<Self>>, other: &Option<Arc<Self>>) {
        let joined = Self::union(current.clone(), other);
        *current = joined;
    }

    fn union(left: Option<Arc<Self>>, right: &Option<Arc<Self>>) -> Option<Arc<Self>> {
        let Some(right) = right.as_ref() else {
            return left;
        };
        let Some(left) = left else {
            return Some(right.clone());
        };
        if Arc::ptr_eq(&left, right) {
            return Some(left);
        }

        // Inserting the smaller exact set into the larger bounds the first
        // implementation's work without complicating the representation.
        // Canonical Patricia shape makes the result independent of this choice
        // and leaves room for a future structural union.
        let (mut result, additions) = if left.len >= right.len {
            (left.clone(), right)
        } else {
            (right.clone(), &left)
        };
        additions.root.for_each_owner(&mut |address, owner| {
            let (root, inserted) = OwnerNode::insert(&result.root, address, owner);
            if inserted {
                let cover = Arc::make_mut(&mut result);
                cover.root = root;
                cover.len += 1;
            }
        });

        // Union remains directionally hot: the right receipt's latest owner
        // is the latest owner of the result, independent of which trie was the
        // larger base.
        if result.latest_address != right.latest_address {
            Arc::make_mut(&mut result).latest_address = right.latest_address;
        }
        Some(result)
    }

    /// Exact subset check used to audit unsafe receipt replacement in debug
    /// builds. This is intentionally cold; the production path relies on the
    /// construction proof documented at [`PATCH::set_owner_guard`].
    #[cfg(debug_assertions)]
    fn covers(&self, covered: &Self) -> bool {
        if self.len < covered.len {
            return false;
        }
        let mut covers = true;
        covered.root.for_each_owner(&mut |address, _| {
            covers &= OwnerNode::contains(&self.root, address);
        });
        covers
    }
}

/// Opaque lifetime receipt for archive-backed PATCH leaves.
///
/// Aggregate structures can exactly join receipts, add one archive owner, and
/// install a proved superset. Trie heads and concrete owner-set nodes remain
/// private to PATCH.
#[derive(Clone, Debug, Default)]
pub(crate) struct PATCHOwnerGuard(Option<Arc<OwnerCover>>);

impl PATCHOwnerGuard {
    /// Retain exactly the union of owners held by either receipt.
    pub(crate) fn join(self, other: Self) -> Self {
        Self(OwnerCover::union(self.0, &other.0))
    }

    /// Add one archive allocation before any LocalLeaf into it is installed.
    pub(crate) fn retain_archive_owner(&mut self, owner: &Arc<dyn ArchiveOwner>) {
        OwnerCover::retain(&mut self.0, owner);
    }

    #[cfg(debug_assertions)]
    fn covers(&self, current: &Option<Arc<OwnerCover>>) -> bool {
        let Some(current) = current else {
            return true;
        };
        let Some(replacement) = self.0.as_ref() else {
            return false;
        };
        Arc::ptr_eq(current, replacement) || replacement.covers(current)
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

#[cfg(test)]
#[derive(Default)]
struct OwnerCoverStats {
    owners: usize,
    branches: usize,
    max_depth: usize,
}

#[cfg(test)]
impl OwnerNode {
    fn collect_stats(&self, depth: usize, stats: &mut OwnerCoverStats) {
        match self {
            Self::Owner { .. } => {
                stats.owners += 1;
                stats.max_depth = stats.max_depth.max(depth);
            }
            Self::Branch { zero, one, .. } => {
                stats.branches += 1;
                zero.collect_stats(depth + 1, stats);
                one.collect_stats(depth + 1, stats);
            }
        }
    }

    fn same_shape(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Owner { address: left, .. }, Self::Owner { address: right, .. }) => {
                left == right
            }
            (
                Self::Branch {
                    mask: left_mask,
                    zero: left_zero,
                    one: left_one,
                },
                Self::Branch {
                    mask: right_mask,
                    zero: right_zero,
                    one: right_one,
                },
            ) => {
                left_mask == right_mask
                    && left_zero.same_shape(right_zero)
                    && left_one.same_shape(right_one)
            }
            _ => false,
        }
    }

    fn leaf(root: &Arc<Self>, address: usize) -> Option<&Arc<Self>> {
        let leaf = Self::matching_leaf(root, address);
        match leaf.as_ref() {
            Self::Owner { address: found, .. } if *found == address => Some(leaf),
            _ => None,
        }
    }
}

#[cfg(test)]
impl OwnerCover {
    fn stats(&self) -> OwnerCoverStats {
        let mut stats = OwnerCoverStats::default();
        self.root.collect_stats(0, &mut stats);
        stats
    }

    fn owner_count(&self) -> usize {
        self.len
    }
}

#[cfg(not(target_pointer_width = "64"))]
compile_error!("PATCH tagged pointers require 64-bit targets");

static mut SIP_KEY: [u8; 16] = [0; 16];
static INIT: Once = Once::new();

#[cfg(test)]
std::thread_local! {
    // The focused counter probes stay on the serial/small-union lane. Keeping
    // their accounting thread-local prevents unrelated parallel unit tests
    // from racing a reset or contributing incidental verification hashes.
    static LOCAL_LEAF_HASH_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_local_leaf_hash_calls() {
    LOCAL_LEAF_HASH_CALLS.set(0);
}

#[cfg(test)]
pub(crate) fn local_leaf_hash_calls() -> usize {
    LOCAL_LEAF_HASH_CALLS.get()
}

/// Minimum `other.leaf_count` at which [`Head::par_union`] takes the
/// scatter + bitset + rayon::scope-spawn path on the equal-depth-
/// branch arm. Below this, the per-key `modify_child` loop wins
/// because asymmetric merges only touch a handful of slots.
#[cfg(feature = "parallel")]
const PARALLEL_PATCH_UNION_THRESHOLD: usize = 4096;

/// Parallel-aware PATCH union, with a shared work-stealing budget
/// carried across the entire recursive descent.
///
/// Two-phase model per parallel call:
///   1. Spawn phase (collect sequentially, dispatch per child):
///      drain "both" pairs, for each: claim 1 unit from the
///      shared budget — if successful, spawn the child union as
///      a `rayon::scope` task; if budget is exhausted, run the
///      child serially via `Head::union`.
///   2. Install phase (purely serial): scatter-collected resolved
///      heads + single-side pass-throughs land in the parent
///      branch. Non-hash aggregates are rebuilt in one pass; the new
///      branch stays fingerprint-dirty until a consumer asks.
///
/// The budget is a single shared atomic — `num_threads²` total
/// spawns across the entire descent, after which everything is
/// sequential. This caps overhead without restricting the depth
/// at which parallelism is reached: a heavy subtree near the
/// root claims many units; a balanced descent spreads them.
#[cfg(feature = "parallel")]
mod parallel_union {
    use core::sync::atomic::{AtomicUsize, Ordering};

    /// Carries the shared spawn budget across recursive
    /// `par_union_with_ctx` calls.
    pub(crate) struct ParUnionCtx {
        pub(crate) budget: AtomicUsize,
    }

    impl ParUnionCtx {
        pub(crate) fn new() -> Self {
            let n = rayon::current_num_threads();
            Self {
                budget: AtomicUsize::new(n.saturating_mul(n).max(2)),
            }
        }

        /// Try to claim one spawn unit. Returns `true` if a unit was
        /// claimed (caller should spawn), `false` if the budget was
        /// already exhausted (caller should run serially).
        ///
        /// A naive `fetch_sub(1)` would wrap `0 → usize::MAX` on
        /// over-subtract, briefly letting other threads see a huge
        /// budget — so we use compare-exchange to refuse the claim
        /// without ever observing the underflow.
        pub(crate) fn try_claim(&self) -> bool {
            let mut current = self.budget.load(Ordering::Relaxed);
            loop {
                if current == 0 {
                    return false;
                }
                match self.budget.compare_exchange_weak(
                    current,
                    current - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(observed) => current = observed,
                }
            }
        }
    }

    /// Raw-pointer wrapper for a scatter-write target. Each spawned task
    /// writes to slot `k` for its specific key byte; keys are pairwise
    /// distinct by construction (each "both" bit in the partition uniquely
    /// identifies a slot), so the writes are non-aliasing despite sharing a
    /// `*mut` across threads.
    ///
    /// The accessors exist as inherent methods (rather than callers
    /// reading the `*mut` field directly) so that move closures
    /// capture the whole wrapper — Rust 2021 precise-capture would
    /// otherwise grab the raw pointer field, dropping the manual
    /// `Send`/`Sync` impls and triggering a Send error.
    pub(crate) struct ScatterPtr<T>(pub *mut T);

    // Manual `Copy`/`Clone` impls so `T` doesn't get a spurious
    // `T: Copy` / `T: Clone` bound from derive — the wrapper holds a
    // raw pointer, which is always `Copy` regardless of `T`.
    impl<T> Clone for ScatterPtr<T> {
        fn clone(&self) -> Self {
            *self
        }
    }
    impl<T> Copy for ScatterPtr<T> {}

    unsafe impl<T: Send> Send for ScatterPtr<T> {}
    unsafe impl<T: Send> Sync for ScatterPtr<T> {}

    impl<T> ScatterPtr<T> {
        /// SAFETY: `i` must be in-bounds of the underlying buffer,
        /// and the caller must guarantee no other thread is writing
        /// to slot `i` concurrently.
        pub(crate) unsafe fn write_at(self, i: usize, v: T) {
            self.0.add(i).write(v);
        }

        /// SAFETY: `i` must be in-bounds of the initialized buffer, and the
        /// caller must guarantee exclusive access to slot `i` for the
        /// duration of the replacement.
        pub(crate) unsafe fn replace_at(self, i: usize, v: T) -> T {
            self.0.add(i).replace(v)
        }
    }
}

/// Initializes the SIP key used for key hashing.
/// Every constructor that caches a PATCH-compatible hash calls this before
/// hashing, including PATCH, heap Leaf, and ArchiveEntry construction.
///
/// `pub(crate)` (was private) so the `vwpatch` clone can route its own SIP-key
/// initialization through this single `Once`, guaranteeing one shared key.
pub(crate) fn init_sip_key() {
    INIT.call_once(|| {
        bytetable::init();

        let mut rng = thread_rng();
        unsafe {
            rng.fill_bytes(&mut SIP_KEY[..]);
        }
    });
}

/// Hash one PATCH key with the process-local set-fingerprint key.
///
/// Keeping this at the set boundary prevents heap leaves, archive entries,
/// demand hashing, and deletion proofs from growing subtly different copies
/// of the same unsafe key access.
#[inline]
pub(crate) fn hash_key(bytes: &[u8]) -> u128 {
    init_sip_key();
    use siphasher::sip128::SipHasher24;
    use std::ptr::addr_of;
    // SAFETY: `init_sip_key` completed the `Once`; the key is immutable after
    // that publication and every later access is read-only.
    let key = unsafe { *addr_of!(SIP_KEY) };
    SipHasher24::new_with_key(&key).hash(bytes).into()
}

/// Builds a per-byte segment map from the segment lengths.
///
/// The returned table maps each key byte to its segment index.
pub const fn build_segmentation<const N: usize, const M: usize>(lens: [usize; M]) -> [usize; N] {
    let mut res = [0; N];
    let mut seg = 0;
    let mut off = 0;
    while seg < M {
        let len = lens[seg];
        let mut i = 0;
        while i < len {
            res[off + i] = seg;
            i += 1;
        }
        off += len;
        seg += 1;
    }
    res
}

/// Builds an identity permutation table of length `N`.
pub const fn identity_map<const N: usize>() -> [usize; N] {
    let mut res = [0; N];
    let mut i = 0;
    while i < N {
        res[i] = i;
        i += 1;
    }
    res
}

/// Builds a table translating indices from key order to tree order.
///
/// `lens` describes the segment lengths in key order and `perm` is the
/// permutation of those segments in tree order.
pub const fn build_key_to_tree<const N: usize, const M: usize>(
    lens: [usize; M],
    perm: [usize; M],
) -> [usize; N] {
    let mut key_starts = [0; M];
    let mut off = 0;
    let mut i = 0;
    while i < M {
        key_starts[i] = off;
        off += lens[i];
        i += 1;
    }

    let mut tree_starts = [0; M];
    off = 0;
    i = 0;
    while i < M {
        let seg = perm[i];
        tree_starts[seg] = off;
        off += lens[seg];
        i += 1;
    }

    let mut res = [0; N];
    let mut seg = 0;
    while seg < M {
        let len = lens[seg];
        let ks = key_starts[seg];
        let ts = tree_starts[seg];
        let mut j = 0;
        while j < len {
            res[ks + j] = ts + j;
            j += 1;
        }
        seg += 1;
    }
    res
}

/// Inverts a permutation table.
pub const fn invert<const N: usize>(arr: [usize; N]) -> [usize; N] {
    let mut res = [0; N];
    let mut i = 0;
    while i < N {
        res[arr[i]] = i;
        i += 1;
    }
    res
}

/// For each tree-depth `d`, the end (exclusive) of the segment that contains
/// `d`, derived from a segmentation table (in key order) and a tree→key map.
///
/// Each logical segment is contiguous in tree order, so the boundary after a
/// depth is simply the first deeper depth whose segment id differs (or
/// `KEY_LEN`). Used by [`KeySchema::next_boundary`] / `SEGMENT_ENDS` to cap
/// variable-width branch spans so they never cross a segment checkpoint.
pub const fn build_segment_ends<const N: usize>(
    segments: [usize; N],
    tree_to_key: [usize; N],
) -> [usize; N] {
    let mut ends = [0usize; N];
    let mut d = 0;
    while d < N {
        let seg = segments[tree_to_key[d]];
        let mut e = d + 1;
        while e < N && segments[tree_to_key[e]] == seg {
            e += 1;
        }
        ends[d] = e;
        d += 1;
    }
    ends
}

#[doc(hidden)]
#[macro_export]
macro_rules! key_segmentation {
    (@count $($e:expr),* $(,)?) => {
        <[()]>::len(&[$($crate::key_segmentation!(@sub $e)),*])
    };
    (@sub $e:expr) => { () };
    ($(#[$meta:meta])* $name:ident, $len:expr, [$($seg_len:expr),+ $(,)?]) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Debug)]
        pub struct $name;
        impl $name {
            pub const SEG_LENS: [usize; $crate::key_segmentation!(@count $($seg_len),*)] = [$($seg_len),*];
        }
        impl $crate::patch::KeySegmentation<$len> for $name {
            const SEGMENTS: [usize; $len] = $crate::patch::build_segmentation::<$len, {$crate::key_segmentation!(@count $($seg_len),*)}>(Self::SEG_LENS);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! key_schema {
    (@count $($e:expr),* $(,)?) => {
        <[()]>::len(&[$($crate::key_schema!(@sub $e)),*])
    };
    (@sub $e:expr) => { () };
    ($(#[$meta:meta])* $name:ident, $seg:ty, $len:expr, [$($perm:expr),+ $(,)?]) => {
        $(#[$meta])*
        #[derive(Copy, Clone, Debug)]
        pub struct $name;
        impl $crate::patch::KeySchema<$len> for $name {
            type Segmentation = $seg;
            const SEGMENT_PERM: &'static [usize] = &[$($perm),*];
            const KEY_TO_TREE: [usize; $len] = $crate::patch::build_key_to_tree::<$len, {$crate::key_schema!(@count $($perm),*)}>(<$seg>::SEG_LENS, [$($perm),*]);
            const TREE_TO_KEY: [usize; $len] = $crate::patch::invert(Self::KEY_TO_TREE);
        }
    };
}

/// A trait is used to provide a re-ordered view of the keys stored in the PATCH.
/// This allows for different PATCH instances share the same leaf nodes,
/// independent of the key ordering used in the tree.
pub trait KeySchema<const KEY_LEN: usize>: Copy + Clone + Debug {
    /// The segmentation this ordering operates over.
    type Segmentation: KeySegmentation<KEY_LEN>;
    /// Order of segments from key layout to tree layout.
    const SEGMENT_PERM: &'static [usize];
    /// Maps each key index to its position in the tree view.
    const KEY_TO_TREE: [usize; KEY_LEN];
    /// Maps each tree index to its position in the key view.
    const TREE_TO_KEY: [usize; KEY_LEN];

    /// For each tree-depth, the exclusive end of the segment containing it.
    ///
    /// Purely additive (a provided default derived from `Segmentation` +
    /// `TREE_TO_KEY`); it does not affect single-byte PATCH behaviour. A
    /// variable-width trie would use it to start branch spans segment-wide and
    /// guarantee a span never crosses a checkpoint. For EAV over a 64-byte
    /// trible this yields ends `{16,32,64}`; for VEA `{32,48,64}`.
    const SEGMENT_ENDS: [usize; KEY_LEN] = build_segment_ends::<KEY_LEN>(
        <Self::Segmentation as KeySegmentation<KEY_LEN>>::SEGMENTS,
        Self::TREE_TO_KEY,
    );

    /// The exclusive end of the segment containing tree-depth `tree_depth`.
    ///
    /// A variable-width branch starting at `span_start` may widen its span up
    /// to `next_boundary(span_start)` but no further, so each branch stays
    /// within a single segment.
    fn next_boundary(tree_depth: usize) -> usize {
        Self::SEGMENT_ENDS[tree_depth]
    }

    /// Reorders the key from the shared key ordering to the tree ordering.
    fn tree_ordered(key: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
        let mut new_key = [0; KEY_LEN];
        let mut i = 0;
        while i < KEY_LEN {
            new_key[Self::KEY_TO_TREE[i]] = key[i];
            i += 1;
        }
        new_key
    }

    /// Reorders the key from the tree ordering to the shared key ordering.
    fn key_ordered(tree_key: &[u8; KEY_LEN]) -> [u8; KEY_LEN] {
        let mut new_key = [0; KEY_LEN];
        let mut i = 0;
        while i < KEY_LEN {
            new_key[Self::TREE_TO_KEY[i]] = tree_key[i];
            i += 1;
        }
        new_key
    }

    /// Return the segment index for the byte at `at_depth` in tree ordering.
    ///
    /// Default implementation reads the static segmentation table and the
    /// tree->key mapping. Having this as a method makes call sites clearer and
    /// reduces the verbosity of expressions that access the segmentation table.
    fn segment_of_tree_depth(at_depth: usize) -> usize {
        <Self::Segmentation as KeySegmentation<KEY_LEN>>::SEGMENTS[Self::TREE_TO_KEY[at_depth]]
    }

    /// Return true if the tree-ordered bytes at `a` and `b` belong to the same
    /// logical segment.
    fn same_segment_tree(a: usize, b: usize) -> bool {
        <Self::Segmentation as KeySegmentation<KEY_LEN>>::SEGMENTS[Self::TREE_TO_KEY[a]]
            == <Self::Segmentation as KeySegmentation<KEY_LEN>>::SEGMENTS[Self::TREE_TO_KEY[b]]
    }
}

/// This trait is used to segment keys stored in the PATCH.
/// The segmentation is used to determine sub-fields of the key,
/// allowing for segment based operations, like counting the number
/// of elements in a segment with a given prefix without traversing the tree.
///
/// Note that the segmentation is defined on the shared key ordering,
/// and should thus be only implemented once, independent of additional key orderings.
///
/// See [TribleSegmentation](crate::trible::TribleSegmentation) for an example that segments keys into entity,
/// attribute, and value segments.
pub trait KeySegmentation<const KEY_LEN: usize>: Copy + Clone + Debug {
    /// Segment index for each position in the key.
    const SEGMENTS: [usize; KEY_LEN];
}

/// A `KeySchema` that does not reorder the keys.
/// This is useful for keys that are already ordered in the desired way.
/// This is the default ordering.
#[derive(Copy, Clone, Debug)]
pub struct IdentitySchema {}

/// A `KeySegmentation` that does not segment the keys.
/// This is useful for keys that do not have a segment structure.
/// This is the default segmentation.
#[derive(Copy, Clone, Debug)]
pub struct SingleSegmentation {}
impl<const KEY_LEN: usize> KeySchema<KEY_LEN> for IdentitySchema {
    type Segmentation = SingleSegmentation;
    const SEGMENT_PERM: &'static [usize] = &[0];
    const KEY_TO_TREE: [usize; KEY_LEN] = identity_map::<KEY_LEN>();
    const TREE_TO_KEY: [usize; KEY_LEN] = identity_map::<KEY_LEN>();
}

impl<const KEY_LEN: usize> KeySegmentation<KEY_LEN> for SingleSegmentation {
    const SEGMENTS: [usize; KEY_LEN] = [0; KEY_LEN];
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[repr(u8)]
pub(crate) enum HeadTag {
    // Stored in the low 4 bits of `Head::tptr` (see Head::new).
    //
    // Branch values encode log2(branch_size) (i.e. `Branch2 == 1`, `Branch256
    // == 8`). `0` is reserved for leaf nodes, which lets us compute the branch
    // size as `1 << tag` without any offset. The derived `Ord` therefore
    // compares branch sizes — `tag_a > tag_b` ⟺ `size_a > size_b`, and the
    // 2× swap threshold reduces to a single tag-byte compare.
    //
    // `LocalLeaf` (9) is appended at the end so the Branch widths' `1 << tag`
    // arithmetic and the Leaf-vs-Branch threshold comparisons are unaffected.
    // It represents a leaf whose key bytes live in an archive's mmap'd buffer,
    // referenced via a thin pointer in the Head body slot rather than via a
    // heap-allocated `Leaf<KEY_LEN, V>`. Lifetime is guaranteed by the owner
    // set on the enclosing PATCH value.
    Leaf = 0,
    Branch2 = 1,
    Branch4 = 2,
    Branch8 = 3,
    Branch16 = 4,
    Branch32 = 5,
    Branch64 = 6,
    Branch128 = 7,
    Branch256 = 8,
    LocalLeaf = 9,
}

impl HeadTag {
    #[inline]
    fn from_raw(raw: u8) -> Self {
        debug_assert!(raw <= HeadTag::LocalLeaf as u8);
        // SAFETY: `HeadTag` is `#[repr(u8)]` with a contiguous discriminant
        // range 0..=9. The tag bits are written by Head::new/set_body and
        // Branch::tag, which only emit valid discriminants.
        unsafe { std::mem::transmute(raw) }
    }
}

pub(crate) enum BodyPtr<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(NonNull<Leaf<KEY_LEN, V>>),
    /// Thin pointer to a `[u8; KEY_LEN]` trible living in an archive's
    /// mmap'd buffer. Lifetime is implicit — guaranteed by the enclosing
    /// PATCH's owner cover.
    LocalLeaf(NonNull<[u8; KEY_LEN]>),
    Branch(branch::BranchNN<KEY_LEN, O, V>),
}

/// Immutable borrow view of a Head body.
/// Returned by `body_ref()` and tied to the lifetime of the `&Head`.
pub(crate) enum BodyRef<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a Leaf<KEY_LEN, V>),
    /// Reference to a trible's bytes within an archive. The slice's
    /// lifetime is bound to `&'a Head` via the body pointer; the actual
    /// underlying allocation is kept alive by the enclosing PATCH.
    LocalLeaf(&'a [u8; KEY_LEN]),
    Branch(&'a Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>),
}

/// Mutable borrow view of a Head body.
/// Returned by `body_mut()` and tied to the lifetime of the `&mut Head`.
pub(crate) enum BodyMut<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a mut Leaf<KEY_LEN, V>),
    /// `LocalLeaf` is read-only by construction (it points into immutable
    /// archive bytes), so the mutable view yields a shared reference. Structural
    /// operations may move the Head while its PATCH owner guard remains live.
    LocalLeaf(&'a [u8; KEY_LEN]),
    Branch(&'a mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>),
}

pub(crate) trait Body {
    fn tag(body: NonNull<Self>) -> HeadTag;
}

#[repr(C)]
pub(crate) struct Head<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    tptr: std::ptr::NonNull<u8>,
    key_ordering: PhantomData<O>,
    key_segments: PhantomData<O::Segmentation>,
    value: PhantomData<V>,
}

unsafe impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Send for Head<KEY_LEN, O, V> {}
unsafe impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Sync for Head<KEY_LEN, O, V> {}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Head<KEY_LEN, O, V> {
    // Tagged pointer layout (64-bit only):
    // - bits 0..=3:   HeadTag (requires 16-byte aligned bodies)
    // - bits 4..=55:  body pointer bits (52 bits)
    // - bits 56..=63: key byte for cuckoo table lookup
    const TAG_MASK: u64 = 0x0f;
    const BODY_MASK: u64 = 0x00_ff_ff_ff_ff_ff_ff_f0;
    const KEY_MASK: u64 = 0xff_00_00_00_00_00_00_00;

    pub(crate) fn new<T: Body + ?Sized>(key: u8, body: NonNull<T>) -> Self {
        unsafe {
            let tptr =
                std::ptr::NonNull::new_unchecked((body.as_ptr() as *mut u8).map_addr(|addr| {
                    debug_assert_eq!(addr as u64 & Self::TAG_MASK, 0);
                    ((addr as u64 & Self::BODY_MASK)
                        | ((key as u64) << 56)
                        | (<T as Body>::tag(body) as u64)) as usize
                }));
            Self {
                tptr,
                key_ordering: PhantomData,
                key_segments: PhantomData,
                value: PhantomData,
            }
        }
    }

    #[inline]
    pub(crate) fn tag(&self) -> HeadTag {
        HeadTag::from_raw((self.tptr.as_ptr() as u64 & Self::TAG_MASK) as u8)
    }

    #[inline]
    pub(crate) fn key(&self) -> u8 {
        (self.tptr.as_ptr() as u64 >> 56) as u8
    }

    /// Exact structural identity, independent of the contextual routing byte
    /// stored in the Head. Branch and heap-leaf bodies are immutable while
    /// shared, and LocalLeaf bytes are immutable for their retained-owner
    /// lifetime, so one body pointer denotes one key set without consulting a
    /// probabilistic fingerprint.
    #[inline]
    fn same_body(&self, other: &Self) -> bool {
        let body_and_tag = Self::BODY_MASK | Self::TAG_MASK;
        let this_body = self.tptr.as_ptr() as u64 & body_and_tag;
        let other_body = other.tptr.as_ptr() as u64 & body_and_tag;
        this_body == other_body
    }

    #[inline]
    pub(crate) fn with_key(mut self, key: u8) -> Self {
        self.tptr =
            std::ptr::NonNull::new(self.tptr.as_ptr().map_addr(|addr| {
                ((addr as u64 & !Self::KEY_MASK) | ((key as u64) << 56)) as usize
            }))
            .unwrap();
        self
    }

    #[inline]
    pub(crate) fn set_body<T: Body + ?Sized>(&mut self, body: NonNull<T>) {
        unsafe {
            self.tptr = NonNull::new_unchecked((body.as_ptr() as *mut u8).map_addr(|addr| {
                debug_assert_eq!(addr as u64 & Self::TAG_MASK, 0);
                ((addr as u64 & Self::BODY_MASK)
                    | (self.tptr.as_ptr() as u64 & Self::KEY_MASK)
                    | (<T as Body>::tag(body) as u64)) as usize
            }))
        }
    }

    pub(crate) fn with_start(self, new_start_depth: usize) -> Head<KEY_LEN, O, V> {
        let leaf_key = self.childleaf_key();
        let i = O::TREE_TO_KEY[new_start_depth];
        let key = leaf_key[i];
        self.with_key(key)
    }

    // Removed childleaf_matches_key_from in favor of composing the existing
    // has_prefix primitives directly at call sites. Use
    // `self.has_prefix::<KEY_LEN>(at_depth, key)` or for partial checks
    // `self.childleaf().has_prefix::<O>(at_depth, &key[..limit])` instead.

    pub(crate) fn body(&self) -> BodyPtr<KEY_LEN, O, V> {
        unsafe {
            let ptr = NonNull::new_unchecked(self.tptr.as_ptr().map_addr(|addr| {
                let masked = (addr as u64) & Self::BODY_MASK;
                masked as usize
            }));
            match self.tag() {
                HeadTag::Leaf => BodyPtr::Leaf(ptr.cast()),
                HeadTag::LocalLeaf => BodyPtr::LocalLeaf(ptr.cast()),
                branch_tag => {
                    let count = 1 << (branch_tag as usize);
                    BodyPtr::Branch(NonNull::new_unchecked(std::ptr::slice_from_raw_parts(
                        ptr.as_ptr(),
                        count,
                    )
                        as *mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>))
                }
            }
        }
    }

    pub(crate) fn body_mut(&mut self) -> BodyMut<'_, KEY_LEN, O, V> {
        unsafe {
            match self.body() {
                BodyPtr::Leaf(mut leaf) => BodyMut::Leaf(leaf.as_mut()),
                BodyPtr::LocalLeaf(ptr) => BodyMut::LocalLeaf(ptr.as_ref()),
                BodyPtr::Branch(mut branch) => {
                    // Ensure ownership: try copy-on-write and update local pointer if needed.
                    let mut branch_nn = branch;
                    if Branch::rc_cow(&mut branch_nn).is_some() {
                        self.set_body(branch_nn);
                        BodyMut::Branch(branch_nn.as_mut())
                    } else {
                        BodyMut::Branch(branch.as_mut())
                    }
                }
            }
        }
    }

    /// Returns an immutable borrow of the body (Leaf, LocalLeaf, or Branch)
    /// tied to &self.
    pub(crate) fn body_ref(&self) -> BodyRef<'_, KEY_LEN, O, V> {
        match self.body() {
            BodyPtr::Leaf(nn) => BodyRef::Leaf(unsafe { nn.as_ref() }),
            BodyPtr::LocalLeaf(nn) => BodyRef::LocalLeaf(unsafe { nn.as_ref() }),
            BodyPtr::Branch(nn) => BodyRef::Branch(unsafe { nn.as_ref() }),
        }
    }

    pub(crate) fn count(&self) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => 1,
            BodyRef::Branch(branch) => branch.leaf_count,
        }
    }

    pub(crate) fn count_segment(&self, at_depth: usize) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => 1,
            BodyRef::Branch(branch) => branch.count_segment(at_depth),
        }
    }

    /// Return a hash already resident in this node without traversing it.
    /// LocalLeaves have no hash field; Branches carry an explicit publication
    /// bit, so exact zero remains distinguishable from an unknown cache.
    #[inline]
    fn known_hash(&self) -> Option<u128> {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => Some(leaf.hash),
            BodyRef::LocalLeaf(_) => None,
            BodyRef::Branch(branch) => branch.cached_hash(),
        }
    }

    /// Publish an independently proven exact hash without traversing this
    /// subtree. Branches memoize the proof for every shared snapshot; heap
    /// leaves already carry the same value. A LocalLeaf has nowhere to retain
    /// it, so singleton archive roots remain demand-hashed.
    #[inline]
    fn publish_known_hash(&self, hash: u128) {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => debug_assert_eq!(leaf.hash, hash),
            BodyRef::LocalLeaf(_) => {}
            BodyRef::Branch(branch) => branch.publish_cached_hash(hash),
        }
    }

    pub(crate) fn hash(&self) -> u128 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.hash,
            BodyRef::LocalLeaf(bytes) => {
                #[cfg(test)]
                LOCAL_LEAF_HASH_CALLS.set(LOCAL_LEAF_HASH_CALLS.get() + 1);
                hash_key(&bytes[..])
            }
            BodyRef::Branch(branch) => {
                if let Some(hash) = branch.cached_hash() {
                    return hash;
                }
                let hash = branch
                    .child_table
                    .iter()
                    .flatten()
                    .fold(0, |hash, child| hash ^ child.hash());
                branch.publish_cached_hash(hash);
                hash
            }
        }
    }

    /// Recompute a subtree hash from leaf keys while ignoring every Branch
    /// cache, asserting each nonzero cache against the resulting semantics.
    /// This is deliberately separate from `hash()` so deep debug audits do not
    /// make dirty descendants appear clean merely because an ancestor cache is
    /// populated.
    #[cfg(debug_assertions)]
    pub(super) fn debug_semantic_hash(&self) -> u128 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => {
                let semantic = hash_key(&leaf.key[..]);
                debug_assert_eq!(leaf.hash, semantic, "heap Leaf hash mismatch");
                semantic
            }
            BodyRef::LocalLeaf(bytes) => hash_key(&bytes[..]),
            BodyRef::Branch(branch) => {
                let semantic = branch
                    .child_table
                    .iter()
                    .flatten()
                    .fold(0, |hash, child| hash ^ child.debug_semantic_hash());
                debug_assert!(
                    branch.cached_hash().map_or(true, |hash| hash == semantic),
                    "resident Branch hash disagrees with leaf-derived semantics",
                );
                semantic
            }
        }
    }

    pub(crate) fn end_depth(&self) -> usize {
        match self.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => KEY_LEN,
            BodyRef::Branch(branch) => branch.end_depth as usize,
        }
    }

    /// Returns the raw key-bytes pointer of the representative child
    /// leaf for use in low-level operations (Branch construction,
    /// invariant checks). For heap `Leaf`, that's `&leaf.key`; for
    /// `LocalLeaf`, the archive-resident bytes pointer; for `Branch`,
    /// the branch's already-computed childleaf pointer.
    pub(crate) fn childleaf_ptr(&self) -> *const [u8; KEY_LEN] {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => &leaf.key as *const [u8; KEY_LEN],
            BodyRef::LocalLeaf(bytes) => bytes as *const [u8; KEY_LEN],
            BodyRef::Branch(branch) => branch.childleaf_ptr(),
        }
    }

    pub(crate) fn childleaf_key(&self) -> &[u8; KEY_LEN] {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => &leaf.key,
            BodyRef::LocalLeaf(bytes) => bytes,
            BodyRef::Branch(branch) => branch.childleaf_key(),
        }
    }

    // Slot wrapper defined at module level (moved to below the impl block)

    /// Find the first depth in [start_depth, limit) where the tree-ordered
    /// bytes of `self` and `other` differ. The comparison limit is computed
    /// as min(self.end_depth(), other.end_depth(), KEY_LEN) which is the
    /// natural bound for comparing two heads. Returns `Some((depth, a, b))`
    /// where `a` and `b` are the differing bytes at that depth, or `None`
    /// if no divergence is found in the range.
    pub(crate) fn first_divergence(
        &self,
        other: &Self,
        start_depth: usize,
    ) -> Option<(usize, u8, u8)> {
        let limit = std::cmp::min(std::cmp::min(self.end_depth(), other.end_depth()), KEY_LEN);
        debug_assert!(limit <= KEY_LEN);
        let this_key = self.childleaf_key();
        let other_key = other.childleaf_key();
        let mut depth = start_depth;
        while depth < limit {
            let i = O::TREE_TO_KEY[depth];
            let a = this_key[i];
            let b = other_key[i];
            if a != b {
                return Some((depth, a, b));
            }
            depth += 1;
        }
        None
    }

    // Mutable access to the child slots for this head. If the head is a
    // branch, returns a mutable slice referencing the underlying child table
    // (each element is Option<Head>). If the head is a leaf an empty slice
    // is returned.
    //
    // The caller receives a &mut slice tied to the borrow of `self` and may
    // reorder entries in-place (e.g., sort_unstable) and then take them using
    // `Option::take()` to extract Head values. The call uses `body_mut()` so
    // COW semantics are preserved and callers have exclusive access to the
    // branch storage while the mutable borrow lasts.
    // NOTE: mut_children removed — prefer matching on BodyRef returned by
    // `body_mut()` and operating directly on the `&mut Branch` reference.

    pub(crate) fn remove_leaf(
        slot: &mut Option<Self>,
        leaf_key: &[u8; KEY_LEN],
        start_depth: usize,
    ) {
        if let Some(this) = slot {
            let end_depth = std::cmp::min(this.end_depth(), KEY_LEN);
            // Check reachable equality by asking the head to test the prefix
            // up to its end_depth. Using the head/leaf primitive centralises the
            // unsafe deref into Branch::childleaf()/Leaf::has_prefix.
            if !this.has_prefix::<KEY_LEN>(start_depth, leaf_key) {
                return;
            }
            if matches!(this.tag(), HeadTag::Leaf | HeadTag::LocalLeaf) {
                slot.take();
            } else {
                let mut ed = crate::patch::branch::BranchMut::from_head(this);
                let key = leaf_key[end_depth];
                ed.modify_child(key, |mut opt| {
                    Self::remove_leaf(&mut opt, leaf_key, end_depth);
                    opt
                });

                // If the branch now contains a single remaining child we
                // collapse the branch upward into that child. We must pull
                // the remaining child out while `ed` is still borrowed,
                // then drop `ed` before writing back into `slot` to avoid
                // double mutable borrows of the slot.
                let occupied_children = ed.child_table.iter().flatten().take(2).count();
                if occupied_children == 0 {
                    drop(ed);
                    slot.take();
                } else if occupied_children == 1 {
                    let mut remaining: Option<Head<KEY_LEN, O, V>> = None;
                    for slot_child in &mut ed.child_table {
                        if let Some(child) = slot_child.take() {
                            remaining = Some(child.with_start(start_depth));
                            break;
                        }
                    }
                    drop(ed);
                    if let Some(child) = remaining {
                        slot.replace(child);
                    }
                } else {
                    // ensure we drop the editor when not collapsing so the
                    // final pointer is committed back into the head.
                    drop(ed);
                }
            }
        }
    }

    // NOTE: slot-level wrappers removed; callers should take the slot and call
    // the owned helpers (insert_leaf / replace_leaf / union)
    // directly. This reduces the indirection and keeps ownership semantics
    // explicit at the call site.

    // Owned variants of the slot-based helpers. These accept the existing
    // Head by value and return the new Head after performing the
    // modification. They are used with the split `insert_child` /
    // `update_child` APIs so we no longer need `Branch::upsert_child`.
    pub(crate) fn insert_leaf(mut this: Self, leaf: Self, start_depth: usize) -> Self {
        if let Some((depth, this_byte_key, leaf_byte_key)) =
            this.first_divergence(&leaf, start_depth)
        {
            let old_key = this.key();
            let new_body = crate::patch::branch::Branch::new(
                depth,
                this.with_key(this_byte_key),
                leaf.with_key(leaf_byte_key),
            );
            return Head::new(old_key, new_body);
        }

        let end_depth = this.end_depth();
        if end_depth != KEY_LEN {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);
            let inserted = leaf.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::insert_leaf(old, inserted, end_depth)),
                None => Some(inserted),
            });
        }
        this
    }
}

// Archive-backed leaf construction, available only when V = ().
impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>> Head<KEY_LEN, O, ()> {
    /// Constructs a `LocalLeaf` Head pointing directly at a `[u8; KEY_LEN]`
    /// trible inside an archive's mmap'd buffer. Restricting construction to
    /// `V = ()` makes the value-type invariant available to generic readers
    /// and union code that encounter the `LocalLeaf` tag.
    ///
    /// The pointer's address must be 16-byte aligned (so the low 4 bits are
    /// free for the `HeadTag`); for `SimpleArchive` buffers this holds whenever
    /// the base allocation is 16-byte aligned and tribles are 64 bytes wide.
    ///
    /// # Safety
    /// - `trible_ptr` must remain valid for at least as long as this Head
    ///   exists, which the caller arranges by retaining its owner in the
    ///   enclosing PATCH's root owner cover.
    /// - The pointed-to bytes must remain fully initialized and immutable for
    ///   that lifetime. LocalLeaf routing and fingerprints read them through
    ///   shared references.
    /// - The pointer must be 16-byte aligned; this is debug-asserted.
    unsafe fn new_local_leaf(key: u8, trible_ptr: NonNull<[u8; KEY_LEN]>) -> Self {
        unsafe {
            let tptr = std::ptr::NonNull::new_unchecked((trible_ptr.as_ptr() as *mut u8).map_addr(
                |addr| {
                    debug_assert_eq!(
                        addr as u64 & Self::TAG_MASK,
                        0,
                        "LocalLeaf trible pointer must be 16-byte aligned"
                    );
                    ((addr as u64 & Self::BODY_MASK)
                        | ((key as u64) << 56)
                        | (HeadTag::LocalLeaf as u64)) as usize
                },
            ));
            Self {
                tptr,
                key_ordering: PhantomData,
                key_segments: PhantomData,
                value: PhantomData,
            }
        }
    }
}

// Resume generic-V `Head` impl for the remaining methods (replace_leaf,
// union, intersect, query operations, etc.) which don't care about V
// shape and so remain in the V-generic impl block.
impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Head<KEY_LEN, O, V> {
    pub(crate) fn replace_leaf(mut this: Self, leaf: Self, start_depth: usize) -> Self {
        if let Some((depth, this_byte_key, leaf_byte_key)) =
            this.first_divergence(&leaf, start_depth)
        {
            let old_key = this.key();
            let new_body = Branch::new(
                depth,
                this.with_key(this_byte_key),
                leaf.with_key(leaf_byte_key),
            );

            return Head::new(old_key, new_body);
        }

        let end_depth = this.end_depth();
        if end_depth == KEY_LEN {
            let old_key = this.key();
            return leaf.with_key(old_key);
        } else {
            // Use the editor view for branch mutation instead of raw pointer ops.
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);
            let inserted = leaf.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::replace_leaf(old, inserted, end_depth)),
                None => Some(inserted),
            });
        }
        this
    }

    /// Sequential PATCH-trie union. Always serial; the parallel
    /// dispatch lives in [`Self::par_union`] which calls back into
    /// `union` once budget is exhausted.
    /// Union is a structural operation. It preserves a resident hash when the
    /// result can be derived from already-resident child hashes, but never
    /// hashes a `LocalLeaf` merely to keep the result cache warm. The first
    /// actual fingerprint consumer pays for whatever dirty region remains.
    pub(crate) fn union(mut this: Self, mut other: Self, at_depth: usize) -> Self {
        if this.same_body(&other) {
            return this;
        }
        let this_depth = this.end_depth();
        let other_depth = other.end_depth();
        let this_hash = this.known_hash();
        let other_hash = other.known_hash();

        // Singleton equality is exact byte equality. Decide it before asking
        // for a fingerprint: LocalLeaf intentionally has no cached hash, and
        // distinct singleton children need those hashes only once when their
        // first Branch is formed.
        if this_depth == KEY_LEN && other_depth == KEY_LEN {
            if let Some((depth, this_byte_key, other_byte_key)) =
                this.first_divergence(&other, at_depth)
            {
                let old_key = this.key();
                let new_body = Branch::new_with_optional_child_hashes(
                    depth,
                    this.with_key(this_byte_key),
                    other.with_key(other_byte_key),
                    this_hash,
                    other_hash,
                );
                return Head::new(old_key, new_body);
            }
            return this;
        }

        // Only use the probabilistic whole-subtree equality shortcut when both
        // aggregate hashes are already cached. Dirty trees descend instead of
        // forcing every LocalLeaf hash merely to ask the question.
        if this.count() == other.count() {
            if let (Some(left), Some(right)) = (this_hash, other_hash) {
                if left == right {
                    return this;
                }
            }
        }

        if let Some((depth, this_byte_key, other_byte_key)) =
            this.first_divergence(&other, at_depth)
        {
            let old_key = this.key();
            let new_body = Branch::new_with_optional_child_hashes(
                depth,
                this.with_key(this_byte_key),
                other.with_key(other_byte_key),
                this_hash,
                other_hash,
            );
            return Head::new(old_key, new_body);
        }

        if this_depth < other_depth {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);
            let inserted = other.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::union(old, inserted, this_depth)),
                None => Some(inserted),
            });
            drop(ed);
            return this;
        }

        if other_depth < this_depth {
            let old_key = this.key();
            let this_head = this;
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut other);
            let inserted = this_head.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::union(old, inserted, other_depth)),
                None => Some(inserted),
            });
            drop(ed);
            return other.with_key(old_key);
        }

        // Equal depth, hashes differ → walk `other`'s children and resolve
        // collisions through the canonical child mutation primitive. Its
        // resident-only delta accounting keeps the parent clean exactly when
        // every changed contribution is already known; otherwise it marks the
        // parent dirty without forcing a hash.
        //
        // Union is commutative; mutating either side in place is
        // semantically equivalent. Swap when `other`'s child_table
        // is at least 2× larger than `this`'s — start with the
        // bigger capacity so cuckoo grows are mostly avoided during
        // insert. Branch tags encode `log2(child_table_size)`, so
        // the 2× ratio reduces to `other_tag > this_tag` (no body
        // deref needed; the tag bits live in the head's pointer).
        if other.tag() > this.tag() {
            std::mem::swap(&mut this, &mut other);
        }
        let BodyMut::Branch(other_branch_ref) = other.body_mut() else {
            unreachable!();
        };
        let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);

        for other_child in other_branch_ref
            .child_table
            .iter_mut()
            .filter_map(Option::take)
        {
            let inserted = other_child.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::union(old, inserted, this_depth)),
                None => Some(inserted),
            });
        }
        drop(ed);
        this
    }

    /// Parallel-aware top-level union entry. Allocates a fresh
    /// [`parallel_union::ParUnionCtx`] with a budget of
    /// `num_threads²` shared spawns, then delegates to
    /// [`Self::par_union_with_ctx`]. The budget persists across the
    /// entire recursive descent — once exhausted, the rest is
    /// sequential.
    #[cfg(feature = "parallel")]
    pub(crate) fn par_union(this: Self, other: Self, at_depth: usize) -> Self
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        let ctx = parallel_union::ParUnionCtx::new();
        Self::par_union_with_ctx(this, other, at_depth, &ctx)
    }

    /// Recursive parallel-aware union. The large equal-depth arm scatters
    /// child pairs and resolves them in parallel. Bulk collection rebuilds the
    /// structural aggregates but deliberately leaves the hash dirty rather
    /// than traversing archive-backed children.
    #[cfg(feature = "parallel")]
    fn par_union_with_ctx(
        mut this: Self,
        mut other: Self,
        at_depth: usize,
        ctx: &parallel_union::ParUnionCtx,
    ) -> Self
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if this.same_body(&other) {
            return this;
        }
        let this_depth = this.end_depth();
        let other_depth = other.end_depth();

        // Singleton pairs have no fan-out work for rayon and the serial rule
        // decides them exactly without a fingerprint.
        if this_depth == KEY_LEN && other_depth == KEY_LEN {
            return Self::union(this, other, at_depth);
        }

        let this_hash = this.known_hash();
        let other_hash = other.known_hash();
        if this.count() == other.count() {
            if let (Some(left), Some(right)) = (this_hash, other_hash) {
                if left == right {
                    return this;
                }
            }
        }

        if let Some((depth, this_byte_key, other_byte_key)) =
            this.first_divergence(&other, at_depth)
        {
            let old_key = this.key();
            let new_body = Branch::new_with_optional_child_hashes(
                depth,
                this.with_key(this_byte_key),
                other.with_key(other_byte_key),
                this_hash,
                other_hash,
            );
            return Head::new(old_key, new_body);
        }

        if this_depth != other_depth {
            // Asymmetric — no fan-out opportunity, serial path wins.
            return Self::union(this, other, at_depth);
        }

        // Equal depth, hashes differ → branch merge. Swap when
        // `other`'s child_table is ≥2× `this`'s so the in-place
        // target starts with the bigger capacity (fewer cuckoo
        // grows when scattering children back via
        // `install_child_growing`). Branch tags encode
        // `log2(child_table_size)`, so the 2× ratio reduces to
        // `other_tag > this_tag` — single byte compare from the
        // head pointer, no body deref / CoW risk.
        if other.tag() > this.tag() {
            std::mem::swap(&mut this, &mut other);
        }

        // Threshold check via `body_ref` (no CoW); fall back to
        // serial when the source side is too small to amortise the
        // scatter machinery.
        let small = match other.body_ref() {
            BodyRef::Branch(b) => (b.leaf_count as usize) < PARALLEL_PATCH_UNION_THRESHOLD,
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => unreachable!(),
        };
        if small {
            return Self::union(this, other, at_depth);
        }

        let BodyMut::Branch(other_branch_ref) = other.body_mut() else {
            unreachable!();
        };

        {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);
            let end_depth = ed.end_depth as usize;

            // Scatter both child tables into key-indexed 256-slot arrays.
            // Other-only children land directly in `this_arr`; `other_arr`
            // holds only colliding operands, and `both` names exactly those
            // slots that need recursive union.
            let mut this_arr: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
            let mut other_arr: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
            let mut both = crate::patch::bytetable::ByteSet::new_empty();

            for slot in ed.child_table.iter_mut() {
                if let Some(head) = slot.take() {
                    let key = head.key();
                    this_arr[key as usize] = Some(head);
                }
            }
            for slot in other_branch_ref.child_table.iter_mut() {
                if let Some(head) = slot.take() {
                    let head = head.with_start(end_depth);
                    let key = head.key();
                    let i = key as usize;
                    if this_arr[i].is_some() {
                        both.insert(key);
                        other_arr[i] = Some(head);
                    } else {
                        this_arr[i] = Some(head);
                    }
                }
            }
            let known_hash = if both == crate::patch::bytetable::ByteSet::new_empty() {
                match (this_hash, other_hash) {
                    (Some(left), Some(right)) => Some(left ^ right),
                    _ => None,
                }
            } else {
                None
            };

            // Reuse `this_arr` as the resolved-head target. A both-side slot is
            // taken before dispatch, so each task writes into a distinct empty
            // slot; if a task panics, rayon joins the remaining work and normal
            // array drops reclaim every result that was already written.
            let this_arr_ptr = parallel_union::ScatterPtr(this_arr.as_mut_ptr());

            rayon::scope(|s| {
                // Drain `both` pairs serially in the parent; per
                // pair, either claim a spawn unit and dispatch as a
                // task, or run serially via `union` here on
                // the parent thread. The atomic budget is shared
                // with all nested `par_union_with_ctx` calls.
                while let Some(k) = both.drain_next_ascending() {
                    let i = k as usize;
                    // SAFETY: after publishing `this_arr_ptr`, every access to
                    // this array inside the scope uses the raw disjoint-slot
                    // primitive. The parent empties `i` before spawning its
                    // sole writer, and `both` yields each index exactly once.
                    // Safe array access resumes only after rayon joins them.
                    let t = unsafe { this_arr_ptr.replace_at(i, None) }.expect("both ⇒ this");
                    let o = other_arr[i].take().expect("both ⇒ other");
                    if ctx.try_claim() {
                        s.spawn(move |_| {
                            let head = Self::par_union_with_ctx(t, o, this_depth, ctx);
                            // SAFETY: each task has a distinct
                            // key `k`, so both writes at `i` are
                            // non-aliasing with every other task.
                            unsafe {
                                this_arr_ptr.write_at(i, Some(head));
                            }
                        });
                    } else {
                        // Budget exhausted — fall back to fully
                        // serial union on this pair, then scatter its
                        // result. SAFETY: same disjointness
                        // invariant; the parent thread races only
                        // with tasks targeting distinct keys.
                        let head = Self::union(t, o, this_depth);
                        unsafe {
                            this_arr_ptr.write_at(i, Some(head));
                        }
                    }
                }
            });
            // After scope: all spawned tasks have completed; the
            // scatter writes are all sequenced-before here by rayon's join
            // semantics.

            debug_assert!(other_arr.iter().all(Option::is_none));
            for slot in &mut this_arr {
                if let Some(head) = slot.take() {
                    ed.install_child_growing(head);
                }
            }

            ed.finish_bulk_aggregates(known_hash);
            drop(ed);
            return this;
        }
    }

    /// Parallel-aware top-level intersect entry. Allocates a fresh
    /// [`parallel_union::ParUnionCtx`] (shared budget across the
    /// descent) and delegates to [`Self::par_intersect_with_ctx`].
    /// Intersect builds a fresh tree, so there is no in-place
    /// target — the parallel work is purely "compute per-pair
    /// intersections in parallel, then collect into a new Branch."
    #[cfg(feature = "parallel")]
    pub(crate) fn par_intersect(&self, other: &Self, at_depth: usize) -> Option<Self>
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        let ctx = parallel_union::ParUnionCtx::new();
        self.par_intersect_with_ctx(other, at_depth, &ctx)
    }

    /// Recursive parallel-aware intersect. At the equal-depth-branch
    /// arm, scatter-spawns one task per matching `(self_child,
    /// other_child)` pair (under budget), then collects results
    /// into a fresh `Branch`. Hash-equal / divergence / asymmetric-
    /// depth arms delegate to serial [`Self::intersect`] — they
    /// don't generate fan-out work.
    #[cfg(feature = "parallel")]
    pub(crate) fn par_intersect_with_ctx(
        &self,
        other: &Self,
        at_depth: usize,
        ctx: &parallel_union::ParUnionCtx,
    ) -> Option<Self>
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if self.same_body(other) {
            return Some(self.clone());
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth == KEY_LEN && other_depth == KEY_LEN {
            return self
                .first_divergence(other, at_depth)
                .is_none()
                .then(|| self.clone());
        }
        if self.count() == other.count() {
            if let (Some(left), Some(right)) = (self.known_hash(), other.known_hash()) {
                if left == right {
                    return Some(self.clone());
                }
            }
        }
        if self.first_divergence(other, at_depth).is_some() {
            return None;
        }
        if self_depth != other_depth {
            return self.intersect(other, at_depth);
        }

        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };

        // Intersect work is bounded by the smaller side — pairs only
        // exist where keys appear in both branches.
        let min_leaves = self_branch.leaf_count.min(other_branch.leaf_count) as usize;
        if min_leaves < PARALLEL_PATCH_UNION_THRESHOLD {
            return self.intersect(other, at_depth);
        }

        let mut resolved: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
        let resolved_ptr = parallel_union::ScatterPtr(resolved.as_mut_ptr());

        // `in_place_scope` runs the outer closure on the calling
        // thread (no `Send` bound), which lets us hold `&Branch`
        // borrows across the spawn loop. `Branch` is `!Sync` due
        // to its raw `*const Leaf` pointer field, so a regular
        // `rayon::scope` would reject the captures.
        rayon::in_place_scope(|s| {
            for slot in self_branch.child_table.iter() {
                let Some(self_child) = slot.as_ref() else {
                    continue;
                };
                let key = self_child.key();
                let Some(other_child) = other_branch.child_table.table_get(key) else {
                    continue;
                };

                if ctx.try_claim() {
                    s.spawn(move |_| {
                        let result =
                            self_child.par_intersect_with_ctx(other_child, self_depth, ctx);
                        // SAFETY: distinct keys → disjoint slots.
                        unsafe {
                            resolved_ptr.write_at(key as usize, result);
                        }
                    });
                } else {
                    let result = self_child.intersect(other_child, self_depth);
                    unsafe {
                        resolved_ptr.write_at(key as usize, result);
                    }
                }
            }
        });

        // Collect non-None results into a fresh Branch. Stick with
        // per-key `modify_child` here — intersect's collection
        // phase typically has FEW children (heavy filtering kept
        // only the matching subset), so the per-call aggregate
        // updates beat the fixed `recompute_aggregates` cost. Bench
        // sanity-checked: install+recompute regressed intersect
        // +18% on the 4M/50%-overlap dataset.
        let mut iter = resolved.into_iter().flatten();
        let first = iter.next()?;
        let Some(second) = iter.next() else {
            return Some(first);
        };
        let new_branch = Branch::new(
            self_depth,
            first.with_start(self_depth),
            second.with_start(self_depth),
        );
        let mut head_for_branch = Head::new(0, new_branch);
        {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut head_for_branch);
            for child in iter {
                let inserted = child.with_start(self_depth);
                let k = inserted.key();
                ed.modify_child(k, |_opt| Some(inserted));
            }
        }
        Some(head_for_branch)
    }

    /// Parallel-aware top-level difference entry. Allocates a fresh
    /// [`parallel_union::ParUnionCtx`] and delegates to
    /// [`Self::par_difference_with_ctx`].
    #[cfg(feature = "parallel")]
    pub(crate) fn par_difference(&self, other: &Self, at_depth: usize) -> Option<Self>
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        let ctx = parallel_union::ParUnionCtx::new();
        self.par_difference_with_ctx(other, at_depth, &ctx)
    }

    /// Recursive parallel-aware difference. Same scatter-and-spawn
    /// shape as `par_intersect_with_ctx`, plus the "no match in
    /// other" branch where we clone `self_child` unchanged into
    /// the resolved array (no recursive work).
    #[cfg(feature = "parallel")]
    pub(crate) fn par_difference_with_ctx(
        &self,
        other: &Self,
        at_depth: usize,
        ctx: &parallel_union::ParUnionCtx,
    ) -> Option<Self>
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if self.same_body(other) {
            return None;
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth == KEY_LEN && other_depth == KEY_LEN {
            return self
                .first_divergence(other, at_depth)
                .is_some()
                .then(|| self.clone());
        }
        if self.count() == other.count() {
            if let (Some(left), Some(right)) = (self.known_hash(), other.known_hash()) {
                if left == right {
                    return None;
                }
            }
        }
        if self.first_divergence(other, at_depth).is_some() {
            return Some(self.clone());
        }
        if self_depth != other_depth {
            return self.difference(other, at_depth);
        }

        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };

        // Difference work is bounded by `self` (every key in self is
        // either kept or filtered against other).
        if (self_branch.leaf_count as usize) < PARALLEL_PATCH_UNION_THRESHOLD {
            return self.difference(other, at_depth);
        }

        let mut resolved: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
        let resolved_ptr = parallel_union::ScatterPtr(resolved.as_mut_ptr());

        // See `par_intersect_with_ctx` for why this is
        // `in_place_scope` rather than `scope`.
        rayon::in_place_scope(|s| {
            for slot in self_branch.child_table.iter() {
                let Some(self_child) = slot.as_ref() else {
                    continue;
                };
                let key = self_child.key();

                match other_branch.child_table.table_get(key) {
                    Some(other_child) => {
                        if ctx.try_claim() {
                            s.spawn(move |_| {
                                let result = self_child.par_difference_with_ctx(
                                    other_child,
                                    self_depth,
                                    ctx,
                                );
                                unsafe {
                                    resolved_ptr.write_at(key as usize, result);
                                }
                            });
                        } else {
                            let result = self_child.difference(other_child, self_depth);
                            unsafe {
                                resolved_ptr.write_at(key as usize, result);
                            }
                        }
                    }
                    None => {
                        // No match in other ⇒ keep `self_child`
                        // unchanged. Clone is cheap (Arc-style rc
                        // bump on Branch, leaf is small).
                        let cloned = self_child.clone();
                        unsafe {
                            resolved_ptr.write_at(key as usize, Some(cloned));
                        }
                    }
                }
            }
        });

        // Collect non-None results into a fresh Branch. Difference's
        // collection phase typically has MANY children (most keys
        // in `self` survive — only matching+empty subtrees get
        // filtered), so `install_child_growing` + one
        // structural-finalization pass wins handily over per-call
        // `modify_child`. Hashing the surviving children here would turn
        // difference into an eager fingerprint consumer, so the rebuilt root
        // remains dirty unless the PATCH boundary later proves it unchanged.
        // Intersect uses `modify_child` because its collection phase has far
        // fewer children (heavy filtering).
        let mut iter = resolved.into_iter().flatten();
        let first = iter.next()?;
        let Some(second) = iter.next() else {
            return Some(first);
        };
        let new_branch = Branch::new(
            self_depth,
            first.with_start(self_depth),
            second.with_start(self_depth),
        );
        let mut head_for_branch = Head::new(0, new_branch);
        {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut head_for_branch);
            for child in iter {
                ed.install_child_growing(child.with_start(self_depth));
            }
            ed.finish_bulk_aggregates(None);
        }
        Some(head_for_branch)
    }

    pub(crate) fn infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.infixes::<PREFIX_LEN, INFIX_LEN, O, F>(prefix, at_depth, f),
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::infixes::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O, F>(
                    bytes, prefix, at_depth, f,
                )
            }
            BodyRef::Branch(branch) => {
                branch.infixes::<PREFIX_LEN, INFIX_LEN, F>(prefix, at_depth, f)
            }
        }
    }

    pub(crate) fn infixes_range<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.infixes_range::<PREFIX_LEN, INFIX_LEN, O, F>(
                prefix, at_depth, min_infix, max_infix, f,
            ),
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::infixes_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O, F>(
                    bytes, prefix, at_depth, min_infix, max_infix, f,
                )
            }
            BodyRef::Branch(branch) => branch.infixes_range::<PREFIX_LEN, INFIX_LEN, F>(
                prefix, at_depth, min_infix, max_infix, f,
            ),
        }
    }

    pub(crate) fn first_infix_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.first_infix_range::<PREFIX_LEN, INFIX_LEN, O>(
                prefix, at_depth, min_infix, max_infix,
            ),
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::first_infix_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O>(
                    bytes, prefix, at_depth, min_infix, max_infix,
                )
            }
            BodyRef::Branch(branch) => branch
                .first_infix_range::<PREFIX_LEN, INFIX_LEN>(prefix, at_depth, min_infix, max_infix),
        }
    }

    pub(crate) fn count_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => {
                leaf.count_range::<PREFIX_LEN, INFIX_LEN, O>(prefix, at_depth, min_infix, max_infix)
            }
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::count_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O>(
                    bytes, prefix, at_depth, min_infix, max_infix,
                )
            }
            BodyRef::Branch(branch) => {
                branch.count_range::<PREFIX_LEN, INFIX_LEN>(prefix, at_depth, min_infix, max_infix)
            }
        }
    }

    pub(crate) fn has_prefix<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> bool {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.has_prefix::<O>(at_depth, prefix),
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::has_prefix::<KEY_LEN, O>(bytes, at_depth, prefix)
            }
            BodyRef::Branch(branch) => branch.has_prefix::<PREFIX_LEN>(at_depth, prefix),
        }
    }

    pub(crate) fn traversal_depth<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> usize {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        match self.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => 1,
            BodyRef::Branch(branch) => branch.traversal_depth::<PREFIX_LEN>(at_depth, prefix),
        }
    }

    pub(crate) fn get<'a>(&'a self, at_depth: usize, key: &[u8; KEY_LEN]) -> Option<&'a V>
    where
        O: 'a,
    {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.get::<O>(at_depth, key),
            BodyRef::LocalLeaf(bytes) => {
                if !leaf::key_ops::matches::<KEY_LEN, O>(bytes, at_depth, key) {
                    return None;
                }
                // SAFETY: LocalLeaf is only constructed by the SimpleArchive
                // ingestion path (step 3), which constrains the PATCH to
                // `V = ()`. The `Option<&V>` here therefore points at a
                // zero-sized value; a static `()` provides the address.
                // For non-`()` V this branch is unreachable because
                // `Head::new_local_leaf` is defined only for `V = ()`.
                static UNIT: () = ();
                let unit_ref: &V = unsafe {
                    debug_assert_eq!(std::mem::size_of::<V>(), 0, "LocalLeaf requires V = ()");
                    &*(&UNIT as *const () as *const V)
                };
                Some(unit_ref)
            }
            BodyRef::Branch(branch) => branch.get(at_depth, key),
        }
    }

    pub(crate) fn segmented_len<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.segmented_len::<O, PREFIX_LEN>(at_depth, prefix),
            BodyRef::LocalLeaf(bytes) => {
                leaf::key_ops::segmented_len::<KEY_LEN, PREFIX_LEN, O>(bytes, at_depth, prefix)
            }
            BodyRef::Branch(branch) => branch.segmented_len::<PREFIX_LEN>(at_depth, prefix),
        }
    }

    /// Locate the shallowest subtree whose keys all share `prefix`.
    ///
    /// Unlike composing [`Self::segmented_len`] with [`Self::infixes`], this
    /// returns the already-located head so a caller can inspect its cached
    /// segment count and then enumerate that same subtree without descending
    /// the fixed prefix a second time.
    fn locate_prefix<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> Option<&Self> {
        let node_end_depth = self.end_depth();
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return None;
        }
        if PREFIX_LEN <= node_end_depth {
            return Some(self);
        }
        let BodyRef::Branch(branch) = self.body_ref() else {
            unreachable!("a leaf always covers the complete key");
        };
        branch
            .child_table
            .table_get(prefix[node_end_depth])
            .and_then(|child| child.locate_prefix(node_end_depth, prefix))
    }

    /// Enumerate a whole infix segment after `prefix` has already been
    /// matched for every key below this head.
    fn infixes_from_matched_prefix<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        for_each: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        if PREFIX_LEN + INFIX_LEN <= self.end_depth() {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            for_each(&infix);
            return;
        }

        let BodyRef::Branch(branch) = self.body_ref() else {
            unreachable!("a leaf always covers the complete key");
        };
        for child in branch.child_table.iter().flatten() {
            child.infixes_from_matched_prefix::<PREFIX_LEN, INFIX_LEN, F>(for_each);
        }
    }

    /// Diagnostic: accumulate (branch nodes, total child-table slots,
    /// heap-`Leaf` nodes, `LocalLeaf` slots) over the subtree. Used to
    /// decompose a PATCH's *structural* byte size (vs resident RSS).
    /// `branches` × `BRANCH_BASE_SIZE` + `slots` × 8 is the branch
    /// allocation total; heap leaves add one `Leaf` node each.
    pub(crate) fn node_stats(&self, acc: &mut (u64, u64, u64, u64)) {
        match self.body_ref() {
            BodyRef::Leaf(_) => acc.2 += 1,
            BodyRef::LocalLeaf(_) => acc.3 += 1,
            BodyRef::Branch(branch) => {
                acc.0 += 1;
                acc.1 += branch.child_table.len() as u64;
                for child in branch.child_table.iter().flatten() {
                    child.node_stats(acc);
                }
            }
        }
    }

    /// Per-end-depth branch census: `hist[d] = (branch_count, filled_children)`
    /// for branches whose branching point is at byte-depth `d`. Reveals where
    /// the branches sit and their fanout — the input to the HOT/variable-width
    /// densification question.
    pub(crate) fn branch_hist(&self, hist: &mut [(u64, u64); 65]) {
        if let BodyRef::Branch(branch) = self.body_ref() {
            let d = self.end_depth().min(64);
            let fanout = branch.child_table.iter().flatten().count() as u64;
            hist[d].0 += 1;
            hist[d].1 += fanout;
            for child in branch.child_table.iter().flatten() {
                child.branch_hist(hist);
            }
        }
    }

    /// Per-fanout branch census: `hist[f] = branch_count` for branches with
    /// exactly `f` filled children.
    pub(crate) fn branch_fanout_hist(&self, hist: &mut [u64; 257]) {
        if let BodyRef::Branch(branch) = self.body_ref() {
            let fanout = branch.child_table.iter().flatten().count();
            hist[fanout.min(256)] += 1;
            for child in branch.child_table.iter().flatten() {
                child.branch_fanout_hist(hist);
            }
        }
    }

    // NOTE: slot-level union wrapper removed; callers should take the slot and
    // call the owned helper `union` directly.

    pub(crate) fn intersect(&self, other: &Self, at_depth: usize) -> Option<Self> {
        if self.same_body(other) {
            return Some(self.clone());
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth == KEY_LEN && other_depth == KEY_LEN {
            return self
                .first_divergence(other, at_depth)
                .is_none()
                .then(|| self.clone());
        }
        if self.count() == other.count() {
            if let (Some(left), Some(right)) = (self.known_hash(), other.known_hash()) {
                if left == right {
                    return Some(self.clone());
                }
            }
        }

        if self.first_divergence(other, at_depth).is_some() {
            return None;
        }

        if self_depth < other_depth {
            // This means that there can be at most one child in self
            // that might intersect with other.
            let BodyRef::Branch(branch) = self.body_ref() else {
                unreachable!();
            };
            return branch
                .child_table
                .table_get(other.childleaf_key()[O::TREE_TO_KEY[self_depth]])
                .and_then(|self_child| other.intersect(self_child, self_depth));
        }

        if other_depth < self_depth {
            // This means that there can be at most one child in other
            // that might intersect with self.
            // If the depth of other is less than the depth of self, then it can't be a leaf.
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            return other_branch
                .child_table
                .table_get(self.childleaf_key()[O::TREE_TO_KEY[other_depth]])
                .and_then(|other_child| self.intersect(other_child, other_depth));
        }

        // If we reached this point then the depths are equal. The only way to have a leaf
        // is if the other is a leaf as well, which is already handled by the hash check if they are equal,
        // and by the key check if they are not equal.
        // If one of them is a leaf and the other is a branch, then they would also have different depths,
        // which is already handled by the above code.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };

        let mut intersected_children = self_branch
            .child_table
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|self_child| {
                let other_child = other_branch.child_table.table_get(self_child.key())?;
                self_child.intersect(other_child, self_depth)
            });
        let first_child = intersected_children.next()?;
        let Some(second_child) = intersected_children.next() else {
            return Some(first_child);
        };
        let second_child = second_child;
        let new_branch = Branch::new(
            self_depth,
            first_child.with_start(self_depth),
            second_child.with_start(self_depth),
        );
        // Use a BranchMut editor to perform all child insertions via the
        // safe editor API instead of manipulating the NonNull pointer
        // directly. The editor will perform COW and commit the final
        // pointer into the Head when it is dropped.
        let mut head_for_branch = Head::new(0, new_branch);
        {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut head_for_branch);
            for child in intersected_children {
                let inserted = child.with_start(self_depth);
                let k = inserted.key();
                ed.modify_child(k, |_opt| Some(inserted));
            }
            // ed dropped here commits the final branch pointer into head_for_branch
        }
        Some(head_for_branch)
    }

    /// Returns the difference between self and other.
    /// This is the set of elements that are in self but not in other.
    /// If the difference is empty, None is returned.
    pub(crate) fn difference(&self, other: &Self, at_depth: usize) -> Option<Self> {
        if self.same_body(other) {
            return None;
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth == KEY_LEN && other_depth == KEY_LEN {
            return self
                .first_divergence(other, at_depth)
                .is_some()
                .then(|| self.clone());
        }
        if self.count() == other.count() {
            if let (Some(left), Some(right)) = (self.known_hash(), other.known_hash()) {
                if left == right {
                    return None;
                }
            }
        }

        if self.first_divergence(other, at_depth).is_some() {
            return Some(self.clone());
        }

        if self_depth < other_depth {
            // This means that there can be at most one child in self
            // that might intersect with other. It's the only child that may not be in the difference.
            // The other children are definitely in the difference, as they have no corresponding byte in other.
            // Thus the cheapest way to compute the difference is compute the difference of the only child
            // that might intersect with other, copy self with it's correctly filled byte table, then
            // remove the old child, and insert the new child.
            let mut new_branch = self.clone();
            let other_byte_key = other.childleaf_key()[O::TREE_TO_KEY[self_depth]];
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut new_branch);
            ed.modify_child(other_byte_key, |opt| {
                opt.and_then(|child| child.difference(other, self_depth))
            });

            // A two-child Branch can lose its only matching child here.
            // Preserve the irreducible-tree invariant rather than returning
            // a unary Branch. The PATCH-level owner cover makes a LocalLeaf
            // survivor valid at any trie depth, including the root.
            let occupied_children = ed.child_table.iter().flatten().take(2).count();
            if occupied_children == 0 {
                drop(ed);
                return None;
            }
            if occupied_children == 1 {
                let remaining = ed
                    .child_table
                    .iter_mut()
                    .find_map(Option::take)
                    .expect("a one-child Branch must contain one child");
                let remaining = remaining.with_start(at_depth);
                drop(ed);
                return Some(remaining);
            }
            drop(ed);
            return Some(new_branch);
        }

        if other_depth < self_depth {
            // This means that we need to check if there is a child in other
            // that matches the path at the current depth of self.
            // There is no such child, then then self must be in the difference.
            // If there is such a child, then we have to compute the difference
            // between self and that child.
            // We know that other must be a branch.
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            let self_byte_key = self.childleaf_key()[O::TREE_TO_KEY[other_depth]];
            if let Some(other_child) = other_branch.child_table.table_get(self_byte_key) {
                return self.difference(other_child, at_depth);
            } else {
                return Some(self.clone());
            }
        }

        // If we reached this point then the depths are equal. The only way to have a leaf
        // is if the other is a leaf as well, which is already handled by the hash check if they are equal,
        // and by the key check if they are not equal.
        // If one of them is a leaf and the other is a branch, then they would also have different depths,
        // which is already handled by the above code.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };

        let mut differenced_children = self_branch
            .child_table
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|self_child| {
                if let Some(other_child) = other_branch.child_table.table_get(self_child.key()) {
                    self_child.difference(other_child, self_depth)
                } else {
                    Some(self_child.clone())
                }
            });

        let first_child = differenced_children.next()?;
        let second_child = match differenced_children.next() {
            Some(sc) => sc,
            None => return Some(first_child),
        };

        let new_branch = Branch::new(
            self_depth,
            first_child.with_start(self_depth),
            second_child.with_start(self_depth),
        );
        let mut head_for_branch = Head::new(0, new_branch);
        {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut head_for_branch);
            for child in differenced_children {
                let inserted = child.with_start(self_depth);
                let k = inserted.key();
                ed.modify_child(k, |_opt| Some(inserted));
            }
            // ed dropped here commits the final branch pointer into head_for_branch
        }
        // The key will be set later, because we don't know it yet.
        // The difference might remove multiple levels of branches,
        // so we can't just take the key from self or other.
        Some(head_for_branch)
    }
}

unsafe impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> ByteEntry for Head<KEY_LEN, O, V> {
    fn key(&self) -> u8 {
        self.key()
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> fmt::Debug for Head<KEY_LEN, O, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.tag().fmt(f)
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Clone for Head<KEY_LEN, O, V> {
    fn clone(&self) -> Self {
        unsafe {
            match self.body() {
                BodyPtr::Leaf(leaf) => Self::new(self.key(), Leaf::rc_inc(leaf)),
                BodyPtr::LocalLeaf(_) => {
                    // LocalLeaf has no refcount. Its enclosing PATCH values
                    // retain the archive allocation, so cloning the Head only
                    // copies the tagged pointer.
                    Self {
                        tptr: self.tptr,
                        key_ordering: PhantomData,
                        key_segments: PhantomData,
                        value: PhantomData,
                    }
                }
                BodyPtr::Branch(branch) => Self::new(self.key(), Branch::rc_inc(branch)),
            }
        }
    }
}

// The Slot wrapper was removed in favor of using BranchMut::from_slot(&mut
// Option<Head<...>>) directly. This keeps the API surface smaller and
// avoids an extra helper type that simply forwarded to BranchMut.

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Drop for Head<KEY_LEN, O, V> {
    fn drop(&mut self) {
        unsafe {
            match self.body() {
                BodyPtr::Leaf(leaf) => Leaf::rc_dec(leaf),
                BodyPtr::LocalLeaf(_) => {
                    // No-op: the enclosing PATCH owner cover, not the leaf,
                    // retains the archive bytes.
                }
                BodyPtr::Branch(branch) => Branch::rc_dec(branch),
            }
        }
    }
}

/// A PATCH is a persistent data structure that stores a set of keys.
/// Each key can be reordered and segmented, based on the provided key ordering and segmentation.
///
/// The patch supports efficient set operations, like union, intersection, and difference,
/// because it efficiently maintains a hash for all keys that are part of a sub-tree.
///
/// The tree itself is a path- and node-compressed a 256-ary trie.
/// Each nodes stores its children in a byte oriented cuckoo hash table,
/// allowing for O(1) access to children, while keeping the memory overhead low.
/// Table sizes are powers of two, starting at 2.
///
/// Having a single node type for all branching factors simplifies the implementation,
/// compared to other adaptive trie implementations, like ARTs or Judy Arrays
///
/// The PATCH allows for cheap copy-on-write operations, with `clone` being O(1).
#[derive(Debug)]
pub struct PATCH<const KEY_LEN: usize, O = IdentitySchema, V = ()>
where
    O: KeySchema<KEY_LEN>,
{
    // Field order is deliberate: Heads drop before the owner guard.
    root: Option<Head<KEY_LEN, O, V>>,
    /// Conservative lifetime guard for every LocalLeaf anywhere below root.
    /// The concrete Arc is thin, so this adds eight bytes per PATCH while
    /// removing sixteen bytes from every Branch.
    owners: Option<Arc<OwnerCover>>,
}

/// A prefix-located PATCH infix traversal whose exact cardinality has already
/// been proved to fit a caller-supplied bound.
///
/// The view borrows the located trie head, so [`Self::for_each`] starts at that
/// same subtree and never repeats the fixed-prefix descent.
#[must_use = "call for_each to enumerate the bounded infixes"]
pub struct PATCHBoundedInfixes<
    'a,
    const KEY_LEN: usize,
    const PREFIX_LEN: usize,
    const INFIX_LEN: usize,
    O: KeySchema<KEY_LEN>,
    V,
> {
    located: Option<&'a Head<KEY_LEN, O, V>>,
    count: u64,
}

impl<
        'a,
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
        V,
    > PATCHBoundedInfixes<'a, KEY_LEN, PREFIX_LEN, INFIX_LEN, O, V>
{
    /// Exact number of distinct infixes this view will emit.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Whether this bounded traversal has no matching infixes.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Enumerate the already-located subtree in the same callback order as
    /// [`PATCH::infixes`].
    pub fn for_each<F>(self, mut for_each: F)
    where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        if let Some(located) = self.located {
            located.infixes_from_matched_prefix::<PREFIX_LEN, INFIX_LEN, F>(&mut for_each);
        }
    }
}

impl<const KEY_LEN: usize, O, V> Clone for PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            owners: self.owners.clone(),
        }
    }
}

impl<const KEY_LEN: usize, O, V> Default for PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const KEY_LEN: usize, O, V> PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    /// Creates a new empty PATCH.
    pub fn new() -> Self {
        init_sip_key();
        PATCH {
            root: None,
            owners: None,
        }
    }

    #[inline]
    fn same_root_body(&self, other: &Self) -> bool {
        match (&self.root, &other.root) {
            (None, None) => true,
            (Some(left), Some(right)) => left.same_body(right),
            (None, Some(_)) | (Some(_), None) => false,
        }
    }

    /// Apply the exact set-hash delta for an operation whose only possible
    /// key-set change is inserting `inserted_hash` once. Descendant caches do
    /// not participate in this proof: equal cardinality means a duplicate,
    /// while growth by one means XORing in the new key.
    #[inline]
    fn publish_insert_one_delta(
        &self,
        old_count: u64,
        old_hash: Option<u128>,
        inserted_hash: u128,
    ) {
        let Some(root) = &self.root else {
            return;
        };
        let delta = root.count().checked_sub(old_count);
        debug_assert!(
            matches!(delta, Some(0 | 1)),
            "one-key insertion must change cardinality by zero or one",
        );
        let derived = match delta {
            Some(0) => old_hash,
            Some(1) => old_hash.map(|hash| hash ^ inserted_hash),
            _ => None,
        };
        if let Some(hash) = derived {
            root.publish_known_hash(hash);
        }
    }

    /// Apply the symmetric one-key deletion law. A failed removal donates the
    /// old root unchanged; a successful removal XORs the removed key out of a
    /// resident old fingerprint. The key itself is hashed only in that latter
    /// case and only when there is an old fingerprint worth maintaining.
    #[inline]
    fn publish_remove_one_delta(
        &self,
        old_count: u64,
        old_hash: Option<u128>,
        removed_tree_key: &[u8; KEY_LEN],
    ) {
        let delta = old_count.checked_sub(self.len());
        debug_assert!(
            matches!(delta, Some(0 | 1)),
            "one-key removal must change cardinality by zero or one",
        );
        let Some(root) = &self.root else {
            return;
        };
        let derived = match delta {
            Some(0) => old_hash,
            Some(1) => old_hash.map(|hash| {
                let removed_key = O::key_ordered(removed_tree_key);
                hash ^ hash_key(&removed_key)
            }),
            _ => None,
        };
        if let Some(hash) = derived {
            root.publish_known_hash(hash);
        }
    }

    /// Publish an operand hash when inclusion plus equal cardinality proves
    /// that operand and result are the same finite key set. Every candidate
    /// supplied by the caller must already be known to contain the result or
    /// be contained by it; cardinality alone is not sufficient.
    #[inline]
    fn publish_inclusion_equal_hash<const N: usize>(&self, candidates: [(u64, Option<u128>); N]) {
        let Some(root) = &self.root else {
            return;
        };
        let result_count = root.count();
        let hash = candidates
            .into_iter()
            .find_map(|(count, hash)| (count == result_count).then_some(hash).flatten());
        if let Some(hash) = hash {
            root.publish_known_hash(hash);
        }
    }

    /// Derive every exact union case visible from operand/result
    /// cardinalities: equality with either operand proves a subset union,
    /// while `|A ∪ B| = |A| + |B|` proves disjointness and therefore XOR.
    #[inline]
    fn publish_union_hash(&self, left: (u64, Option<u128>), right: (u64, Option<u128>)) {
        let Some(root) = &self.root else {
            return;
        };
        let result_count = root.count();
        let inclusion_hash = [left, right]
            .into_iter()
            .find_map(|(count, hash)| (count == result_count).then_some(hash).flatten());
        let disjoint_hash = left
            .0
            .checked_add(right.0)
            .filter(|&sum| sum == result_count)
            .and_then(|_| left.1.zip(right.1))
            .map(|(left, right)| left ^ right);
        if let Some(hash) = inclusion_hash.or(disjoint_hash) {
            root.publish_known_hash(hash);
        }
    }

    /// Derive the exact difference cases visible from cardinality. An
    /// unchanged result donates the left operand. When
    /// `|A ∖ B| + |B| = |A|`, finite-set arithmetic proves `B ⊆ A`, so
    /// the result fingerprint is `hash(A) XOR hash(B)`.
    #[inline]
    fn publish_difference_hash(&self, left: (u64, Option<u128>), right: (u64, Option<u128>)) {
        let Some(root) = &self.root else {
            return;
        };
        let result_count = root.count();
        let unchanged_hash = (result_count == left.0).then_some(left.1).flatten();
        let contained_hash = result_count
            .checked_add(right.0)
            .filter(|&sum| sum == left.0)
            .and_then(|_| left.1.zip(right.1))
            .map(|(left, right)| left ^ right);
        if let Some(hash) = unchanged_hash.or(contained_hash) {
            root.publish_known_hash(hash);
        }
    }

    /// Inserts a shared key into the PATCH.
    ///
    /// Takes an [Entry] object that can be created from a key,
    /// and inserted into multiple PATCH instances.
    ///
    /// If the key is already present, this is a no-op.
    pub fn insert(&mut self, entry: &Entry<KEY_LEN, V>) {
        let old_count = self.len();
        let old_hash = self.root.as_ref().and_then(Head::known_hash);
        let leaf = entry.leaf();
        let inserted_hash = leaf
            .known_hash()
            .expect("a heap Entry must carry a resident key hash");
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::insert_leaf(this, leaf, 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(leaf);
        }
        self.publish_insert_one_delta(old_count, old_hash, inserted_hash);
        self.debug_check_owner_invariant();
    }

    /// Inserts a key into the PATCH, replacing the value if it already exists.
    pub fn replace(&mut self, entry: &Entry<KEY_LEN, V>) {
        let old_count = self.len();
        let old_hash = self.root.as_ref().and_then(Head::known_hash);
        let leaf = entry.leaf();
        let inserted_hash = leaf
            .known_hash()
            .expect("a heap Entry must carry a resident key hash");
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::replace_leaf(this, leaf, 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(leaf);
        }
        self.publish_insert_one_delta(old_count, old_hash, inserted_hash);
        self.debug_check_owner_invariant();
    }

    /// Removes a key from the PATCH.
    ///
    /// `key` is expressed in this PATCH's tree ordering.
    ///
    /// If the key is not present, this is a no-op.
    pub fn remove(&mut self, key: &[u8; KEY_LEN]) {
        let old_count = self.len();
        let old_hash = self.root.as_ref().and_then(Head::known_hash);
        Head::remove_leaf(&mut self.root, key, 0);
        if self.root.is_none() {
            self.owners = None;
        }
        self.publish_remove_one_delta(old_count, old_hash, key);
        self.debug_check_owner_invariant();
    }

    /// Returns the number of keys in the PATCH.
    pub fn len(&self) -> u64 {
        if let Some(root) = &self.root {
            root.count()
        } else {
            0
        }
    }

    /// Diagnostic structural census: returns
    /// `(branch_nodes, child_table_slots, heap_leaf_nodes, local_leaf_slots)`.
    /// Structural branch bytes ≈ `branches * BRANCH_BASE_SIZE + slots * 8`;
    /// heap leaves add a `Leaf` node each (the key is shared across the six
    /// orderings, so count it once per trible, not once per ordering).
    pub fn node_stats(&self) -> (u64, u64, u64, u64) {
        let mut acc = (0u64, 0u64, 0u64, 0u64);
        if let Some(root) = &self.root {
            root.node_stats(&mut acc);
        }
        acc
    }

    #[cfg(debug_assertions)]
    fn debug_check_owner_invariant(&self) {
        debug_assert!(
            self.root.as_ref().map(|root| root.tag()) != Some(HeadTag::LocalLeaf)
                || self.owners.is_some(),
            "a root LocalLeaf must retain its archive owner",
        );
    }

    #[cfg(not(debug_assertions))]
    #[inline]
    fn debug_check_owner_invariant(&self) {}

    /// Returns the total capacity of all branch child tables.
    ///
    /// This counts allocated table slots (`child_table.len()`), not filled
    /// children.
    pub fn total_table_slots(&self) -> u64 {
        self.node_stats().1
    }

    /// Fixed branch header bytes, excluding the trailing child table.
    pub fn branch_header_bytes() -> usize {
        std::mem::size_of::<Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>; 0], V>>()
    }

    /// Per-end-depth `(branch_count, filled_children)` histogram (65 buckets,
    /// byte-depths 0..=64), for analysing trie shape — where branches sit and
    /// their fanout distribution.
    pub fn branch_histogram(&self) -> [(u64, u64); 65] {
        let mut hist = [(0u64, 0u64); 65];
        if let Some(root) = &self.root {
            root.branch_hist(&mut hist);
        }
        hist
    }

    /// Per-fanout branch census: returns `hist[f] = branch_count` for each
    /// exact fanout `0..=256`.
    pub fn branch_fanout_histogram(&self) -> [u64; 257] {
        let mut hist = [0u64; 257];
        if let Some(root) = &self.root {
            root.branch_fanout_hist(&mut hist);
        }
        hist
    }

    /// Returns true if the PATCH contains no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn root_hash(&self) -> Option<u128> {
        self.root.as_ref().map(|root| root.hash())
    }

    /// Expensive debug oracle: derive the root hash from leaf bytes while
    /// recursively checking every resident Branch cache. Kept explicit rather
    /// than attached to each mutation so ordinary debug insertion does not
    /// become quadratic in the number of leaves.
    #[cfg(debug_assertions)]
    fn debug_check_deep_hash_invariant(&self) {
        if let Some(root) = &self.root {
            let _ = root.debug_semantic_hash();
        }
    }

    /// Clone the opaque archive-owner receipt without exposing the root Head.
    pub(crate) fn owner_guard(&self) -> PATCHOwnerGuard {
        PATCHOwnerGuard(self.owners.clone())
    }

    /// Whether this PATCH and another PATCH share the same owner-cover Arc.
    pub(crate) fn shares_owner_guard<OO, VV>(&self, other: &PATCH<KEY_LEN, OO, VV>) -> bool
    where
        OO: KeySchema<KEY_LEN>,
    {
        match (&self.owners, &other.owners) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Whether `owner` is the most recently adopted owner in this exact set.
    ///
    /// The dedicated field keeps this overwhelmingly common check O(1).
    /// Adopting an older owner performs an exact Patricia lookup and updates
    /// only this recency discriminator; it never duplicates an owner leaf.
    pub(crate) fn owner_guard_latest_is(&self, owner: &Arc<dyn ArchiveOwner>) -> bool {
        self.owners
            .as_ref()
            .is_some_and(|cover| cover.latest_address == OwnerCover::address(owner))
    }

    /// Install an opaque owner set that is known to be a superset of this
    /// PATCH's current set.
    ///
    /// Empty PATCHes may retain a receipt so all indexes of an aggregate can
    /// share one cover Arc. The debug assertion checks exact set inclusion.
    ///
    /// # Safety
    ///
    /// `guard` must retain every archive allocation retained by the current
    /// owner guard. Violating this requirement can leave a LocalLeaf dangling.
    pub(crate) unsafe fn set_owner_guard(&mut self, guard: &PATCHOwnerGuard) {
        #[cfg(debug_assertions)]
        debug_assert!(
            guard.covers(&self.owners),
            "a PATCH owner guard may only be replaced by an owner-set superset",
        );
        let already_installed = match (&self.owners, &guard.0) {
            (None, None) => true,
            (Some(current), Some(replacement)) => Arc::ptr_eq(current, replacement),
            _ => false,
        };
        if !already_installed {
            self.owners = guard.0.clone();
        }
        self.debug_check_owner_invariant();
    }

    /// Returns the value associated with `key` if present.
    pub fn get(&self, key: &[u8; KEY_LEN]) -> Option<&V> {
        self.root.as_ref().and_then(|root| root.get(0, key))
    }

    /// Allows iteratig over all infixes of a given length with a given prefix.
    /// Each infix is passed to the provided closure.
    ///
    /// The entire operation is performed over the tree view ordering of the keys.
    ///
    /// The length of the prefix and the infix is provided as type parameters,
    /// but will usually inferred from the arguments.
    ///
    /// The sum of `PREFIX_LEN` and `INFIX_LEN` must be less than or equal to `KEY_LEN`
    /// or a compile-time assertion will fail.
    ///
    /// Because all infixes are iterated in one go, less bookkeeping is required,
    /// than when using an Iterator, allowing for better performance.
    pub fn infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        mut for_each: F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        const {
            assert!(PREFIX_LEN + INFIX_LEN <= KEY_LEN);
        }
        assert!(
            O::same_segment_tree(PREFIX_LEN, PREFIX_LEN + INFIX_LEN - 1)
                && (PREFIX_LEN + INFIX_LEN == KEY_LEN
                    || !O::same_segment_tree(PREFIX_LEN + INFIX_LEN - 1, PREFIX_LEN + INFIX_LEN)),
            "INFIX_LEN must cover a whole segment"
        );
        if let Some(root) = &self.root {
            root.infixes(prefix, 0, &mut for_each);
        }
    }

    /// Locate all distinct infixes for `prefix` only when their exact count is
    /// at most `limit`.
    ///
    /// `Some(view)` is an all-or-nothing proof that [`PATCHBoundedInfixes::len`]
    /// infixes fit the bound; [`PATCHBoundedInfixes::for_each`] then enumerates
    /// every one from the already-located subtree. `None` means the cached
    /// segment count exceeded `limit`. A missing prefix is a successful empty
    /// view.
    ///
    /// Locating the view costs `O(prefix depth)`. Visiting it costs
    /// `O(count)`, where `count <= limit`, so paged callers retain a hard
    /// geometric work bound while reserving output storage from the exact
    /// count before enumeration.
    pub fn bounded_infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        limit: u64,
    ) -> Option<PATCHBoundedInfixes<'_, KEY_LEN, PREFIX_LEN, INFIX_LEN, O, V>> {
        const {
            assert!(PREFIX_LEN + INFIX_LEN <= KEY_LEN);
        }
        assert!(
            O::same_segment_tree(PREFIX_LEN, PREFIX_LEN + INFIX_LEN - 1)
                && (PREFIX_LEN + INFIX_LEN == KEY_LEN
                    || !O::same_segment_tree(PREFIX_LEN + INFIX_LEN - 1, PREFIX_LEN + INFIX_LEN)),
            "INFIX_LEN must cover a whole segment"
        );
        const {
            if PREFIX_LEN > 0 && PREFIX_LEN < KEY_LEN {
                assert!(
                    <O as KeySchema<KEY_LEN>>::Segmentation::SEGMENTS
                        [O::TREE_TO_KEY[PREFIX_LEN - 1]]
                        != <O as KeySchema<KEY_LEN>>::Segmentation::SEGMENTS
                            [O::TREE_TO_KEY[PREFIX_LEN]],
                    "PREFIX_LEN must align to segment boundary",
                );
            }
        }

        let Some(root) = &self.root else {
            return Some(PATCHBoundedInfixes {
                located: None,
                count: 0,
            });
        };
        let Some(located) = root.locate_prefix(0, prefix) else {
            return Some(PATCHBoundedInfixes {
                located: None,
                count: 0,
            });
        };
        let count = located.count_segment(PREFIX_LEN);
        if count > limit {
            return None;
        }
        Some(PATCHBoundedInfixes {
            located: Some(located),
            count,
        })
    }

    /// Like [`infixes`](Self::infixes) but only yields infixes in the
    /// byte range `[min_infix, max_infix]` (inclusive).
    ///
    /// The trie is pruned at each depth: branches whose byte key falls
    /// outside the range at the current infix position are skipped
    /// entirely, avoiding traversal of irrelevant subtrees.
    pub fn infixes_range<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
        mut for_each: F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        const {
            assert!(PREFIX_LEN + INFIX_LEN <= KEY_LEN);
        }
        assert!(
            O::same_segment_tree(PREFIX_LEN, PREFIX_LEN + INFIX_LEN - 1)
                && (PREFIX_LEN + INFIX_LEN == KEY_LEN
                    || !O::same_segment_tree(PREFIX_LEN + INFIX_LEN - 1, PREFIX_LEN + INFIX_LEN)),
            "INFIX_LEN must cover a whole segment"
        );
        if let Some(root) = &self.root {
            root.infixes_range(prefix, 0, min_infix, max_infix, &mut for_each);
        }
    }

    /// Return the lexicographically first distinct infix in the inclusive
    /// range `[min_infix, max_infix]` for `prefix`.
    ///
    /// This performs ordered lower-bound descent through the PATCH trie. It
    /// does not depend on the physical cuckoo-table order and does not
    /// materialize or sort the matching infixes.
    pub fn first_infix_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        const {
            assert!(PREFIX_LEN + INFIX_LEN <= KEY_LEN);
        }
        assert!(
            O::same_segment_tree(PREFIX_LEN, PREFIX_LEN + INFIX_LEN - 1)
                && (PREFIX_LEN + INFIX_LEN == KEY_LEN
                    || !O::same_segment_tree(PREFIX_LEN + INFIX_LEN - 1, PREFIX_LEN + INFIX_LEN)),
            "INFIX_LEN must cover a whole segment"
        );
        if min_infix > max_infix {
            return None;
        }
        self.root
            .as_ref()
            .and_then(|root| root.first_infix_range(prefix, 0, min_infix, max_infix))
    }

    /// Return the first distinct infix strictly after `after`, bounded above
    /// by `max_infix` (inclusive).
    ///
    /// The successor is computed in lexicographic byte order and then passed
    /// to [`Self::first_infix_range`]. `None` is returned when `after` is the
    /// all-`0xff` value or when no later infix exists.
    pub fn next_infix_after<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        after: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        let mut lower = *after;
        let mut cursor = INFIX_LEN;
        loop {
            if cursor == 0 {
                return None;
            }
            cursor -= 1;
            if lower[cursor] != u8::MAX {
                lower[cursor] += 1;
                for byte in &mut lower[cursor + 1..] {
                    *byte = u8::MIN;
                }
                break;
            }
        }
        self.first_infix_range(prefix, &lower, max_infix)
    }

    /// Count entries whose infix falls within [min_infix, max_infix].
    ///
    /// Uses cached `leaf_count` on branches to skip entire subtrees that
    /// are fully inside the range, making the count O(boundary_nodes)
    /// rather than O(matching_leaves).
    pub fn count_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> u64 {
        const {
            assert!(PREFIX_LEN + INFIX_LEN <= KEY_LEN);
        }
        match &self.root {
            Some(root) => root.count_range(prefix, 0, min_infix, max_infix),
            None => 0,
        }
    }

    /// Returns true if the PATCH has a key with the given prefix.
    ///
    /// `PREFIX_LEN` must be less than or equal to `KEY_LEN` or a compile-time
    /// assertion will fail.
    pub fn has_prefix<const PREFIX_LEN: usize>(&self, prefix: &[u8; PREFIX_LEN]) -> bool {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        if let Some(root) = &self.root {
            root.has_prefix(0, prefix)
        } else {
            PREFIX_LEN == 0
        }
    }

    /// Returns the number of PATCH nodes inspected by a prefix lookup.
    ///
    /// This is a diagnostic companion to [`PATCH::has_prefix`]. A miss counts
    /// the node where the mismatch or missing child is discovered; an empty
    /// PATCH reports zero.
    pub fn traversal_depth<const PREFIX_LEN: usize>(&self, prefix: &[u8; PREFIX_LEN]) -> usize {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        self.root
            .as_ref()
            .map(|root| root.traversal_depth(0, prefix))
            .unwrap_or(0)
    }

    /// Returns the number of unique segments in keys with the given prefix.
    pub fn segmented_len<const PREFIX_LEN: usize>(&self, prefix: &[u8; PREFIX_LEN]) -> u64 {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
            if PREFIX_LEN > 0 && PREFIX_LEN < KEY_LEN {
                assert!(
                    <O as KeySchema<KEY_LEN>>::Segmentation::SEGMENTS
                        [O::TREE_TO_KEY[PREFIX_LEN - 1]]
                        != <O as KeySchema<KEY_LEN>>::Segmentation::SEGMENTS
                            [O::TREE_TO_KEY[PREFIX_LEN]],
                    "PREFIX_LEN must align to segment boundary",
                );
            }
        }
        if let Some(root) = &self.root {
            root.segmented_len(0, prefix)
        } else {
            0
        }
    }

    /// Iterates over all keys in the PATCH.
    /// The keys are returned in key ordering but random order.
    pub fn iter<'a>(&'a self) -> PATCHIterator<'a, KEY_LEN, O, V> {
        PATCHIterator::new(self)
    }

    /// Iterates over all keys in the PATCH in key order.
    ///
    /// The traversal visits every key in lexicographic key order, without
    /// accepting a prefix filter. For prefix-aware iteration, see
    /// [`PATCH::iter_prefix_count`].
    pub fn iter_ordered<'a>(&'a self) -> PATCHOrderedIterator<'a, KEY_LEN, O, V> {
        PATCHOrderedIterator::new(self)
    }

    /// Iterate over all prefixes of the given length in the PATCH.
    /// The prefixes are naturally returned in tree ordering and tree order.
    /// A count of the number of elements for the given prefix is also returned.
    pub fn iter_prefix_count<'a, const PREFIX_LEN: usize>(
        &'a self,
    ) -> PATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V> {
        PATCHPrefixIterator::new(self)
    }

    /// Unions this PATCH with another PATCH.
    ///
    /// The other PATCH is consumed, and this PATCH is updated in place.
    /// Key-set semantics are preserved, but when duplicate keys carry
    /// different values, which value survives is unspecified.
    pub fn union(&mut self, mut other: Self)
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        // The installed owner guard already covers this exact root. Returning
        // here also avoids joining two conservative owner covers for a set
        // whose persistent structure is literally unchanged.
        if self.same_root_body(&other) {
            return;
        }
        if let Some(other_root) = other.root.take() {
            if self.root.is_some() {
                // Union's result contains both operands. If its cardinality
                // equals either operand's, the finite key sets are identical,
                // so that operand's resident hash is an exact result hash.
                // Capture this boundary evidence before consuming the roots;
                // the structural merge itself remains fully hash-lazy.
                let this_count = self.len();
                let this_hash = self.root.as_ref().and_then(Head::known_hash);
                let other_count = other_root.count();
                let other_hash = other_root.known_hash();

                // Extend the installed lifetime guard before Head::union can
                // detach or move either side's LocalLeaves. Owner-cover carry
                // is monotone and transactional, so a caught allocation panic
                // leaves this PATCH's existing root fully guarded. Keep
                // `other.owners` in its PATCH until the Head merge completes,
                // so unwind also drops the other Head before its guard.
                OwnerCover::merge_into(&mut self.owners, &other.owners);
                let this = self.root.take().expect("root should not be empty");
                #[cfg(feature = "parallel")]
                let merged = Head::par_union(this, other_root, 0);
                #[cfg(not(feature = "parallel"))]
                let merged = Head::union(this, other_root, 0);
                self.root.replace(merged);
                self.publish_union_hash((this_count, this_hash), (other_count, other_hash));
            } else {
                self.root.replace(other_root);
                self.owners = other.owners.take();
            }
        }
        self.debug_check_owner_invariant();
    }

    /// Intersects this PATCH with another PATCH.
    ///
    /// Returns a new PATCH that contains only the keys that are present in both PATCHes.
    pub fn intersect(&self, other: &Self) -> Self
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if self.same_root_body(other) {
            return self.clone();
        }
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                let candidates = [
                    (root.count(), root.known_hash()),
                    (other_root.count(), other_root.known_hash()),
                ];
                #[cfg(feature = "parallel")]
                let result = root.par_intersect(other_root, 0);
                #[cfg(not(feature = "parallel"))]
                let result = root.intersect(other_root, 0);
                let root = result.map(|root| root.with_start(0));
                let owners = root
                    .as_ref()
                    .and_then(|_| OwnerCover::union(self.owners.clone(), &other.owners));
                let result = Self { root, owners };
                // Intersection is a subset of both operands.
                result.publish_inclusion_equal_hash(candidates);
                result.debug_check_owner_invariant();
                return result;
            }
        }
        Self::new()
    }

    /// Returns the difference between this PATCH and another PATCH.
    ///
    /// Returns a new PATCH that contains only the keys that are present in this PATCH,
    /// but not in the other PATCH.
    pub fn difference(&self, other: &Self) -> Self
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if self.same_root_body(other) {
            return Self::new();
        }
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                let left = (root.count(), root.known_hash());
                let right = (other_root.count(), other_root.known_hash());
                #[cfg(feature = "parallel")]
                let result = root.par_difference(other_root, 0);
                #[cfg(not(feature = "parallel"))]
                let result = root.difference(other_root, 0);
                let owners = result.as_ref().and(self.owners.clone());
                let result = Self {
                    root: result,
                    owners,
                };
                result.publish_difference_hash(left, right);
                result.debug_check_owner_invariant();
                result
            } else {
                (*self).clone()
            }
        } else {
            Self::new()
        }
    }

    /// Calculates the average fill level for branch nodes grouped by their
    /// branching factor. The returned array contains eight entries for branch
    /// sizes `2`, `4`, `8`, `16`, `32`, `64`, `128` and `256` in that order.
    //#[cfg(debug_assertions)]
    pub fn debug_branch_fill(&self) -> [f32; 8] {
        let mut counts = [0u64; 8];
        let mut used = [0u64; 8];

        if let Some(root) = &self.root {
            let mut stack = Vec::new();
            stack.push(root);

            while let Some(head) = stack.pop() {
                match head.body_ref() {
                    BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {}
                    BodyRef::Branch(b) => {
                        let size = b.child_table.len();
                        let idx = size.trailing_zeros() as usize - 1;
                        counts[idx] += 1;
                        used[idx] += b.child_table.iter().filter(|c| c.is_some()).count() as u64;
                        for child in b.child_table.iter().filter_map(|c| c.as_ref()) {
                            stack.push(child);
                        }
                    }
                }
            }
        }

        let mut avg = [0f32; 8];
        for i in 0..8 {
            if counts[i] > 0 {
                let size = 1u64 << (i + 1);
                avg[i] = used[i] as f32 / (counts[i] as f32 * size as f32);
            }
        }
        avg
    }
}

/// Archive-backed insertion path, available only for `V = ()` because
/// [`ArchiveEntry`] does not carry a value. Every inserted key remains a
/// LocalLeaf while the PATCH's root owner cover retains its allocation.
impl<const KEY_LEN: usize, O> PATCH<KEY_LEN, O, ()>
where
    O: KeySchema<KEY_LEN>,
{
    /// Builds one PATCH index bottom-up from keys already sorted in this
    /// schema's tree order.
    ///
    /// This is deliberately a test-only probe. It tests whether archive
    /// ingestion fundamentally needs online insertion, without committing the
    /// production API to a second construction strategy. The recursive result
    /// carries its exact XOR fingerprint beside the transient `Head`; that
    /// lets every Branch publish a resident receipt even though LocalLeaves do
    /// not grow a persistent hash descriptor.
    ///
    /// # Safety
    ///
    /// - Every key pointer must be 16-byte aligned and remain valid and
    ///   immutable for as long as `owner` is retained.
    /// - `owner` must keep the allocation containing every key alive.
    /// - `keys` must be strictly increasing in `O`'s tree order.
    #[cfg(test)]
    unsafe fn from_sorted_archive_keys_for_test(
        keys: &[[u8; KEY_LEN]],
        owner: &Arc<dyn ArchiveOwner>,
    ) -> Self {
        if keys.is_empty() {
            return Self::new();
        }

        #[cfg(debug_assertions)]
        for pair in keys.windows(2) {
            let ordering = (0..KEY_LEN)
                .map(|depth| pair[0][O::TREE_TO_KEY[depth]].cmp(&pair[1][O::TREE_TO_KEY[depth]]))
                .find(|ordering| !ordering.is_eq())
                .unwrap_or(std::cmp::Ordering::Equal);
            debug_assert_eq!(
                ordering,
                std::cmp::Ordering::Less,
                "bottom-up archive input must be strictly tree-ordered",
            );
        }

        let (root, _) = unsafe { Self::build_sorted_archive_head_for_test(keys, owner, 0) };
        let mut guard = PATCHOwnerGuard::default();
        guard.retain_archive_owner(owner);
        let result = Self {
            // Recursive builders return a context-free placeholder routing
            // byte. The root still participates as a movable Head in later
            // unions, so install its actual depth-zero byte too.
            root: Some(root.with_start(0)),
            owners: guard.0,
        };
        result.debug_check_owner_invariant();
        result
    }

    /// Recursive worker for [`Self::from_sorted_archive_keys_for_test`].
    ///
    /// The returned Head has a placeholder routing byte. Its caller installs
    /// the byte at that caller's divergence depth. `hash` is the exact XOR of
    /// every leaf below the Head and is never persisted beside an individual
    /// LocalLeaf.
    #[cfg(test)]
    unsafe fn build_sorted_archive_head_for_test(
        keys: &[[u8; KEY_LEN]],
        owner: &Arc<dyn ArchiveOwner>,
        start_depth: usize,
    ) -> (Head<KEY_LEN, O, ()>, u128) {
        debug_assert!(!keys.is_empty());
        if keys.len() == 1 {
            let ptr = NonNull::from(&keys[0]);
            // SAFETY: forwarded from the caller of the test-only bulk builder.
            let entry = unsafe { ArchiveEntry::new(ptr, owner) };
            let (head, _, hash) = entry.leaf::<O>();
            return (head, hash);
        }

        let first = &keys[0];
        let last = &keys[keys.len() - 1];
        let mut end_depth = start_depth;
        while end_depth < KEY_LEN {
            let key_index = O::TREE_TO_KEY[end_depth];
            if first[key_index] != last[key_index] {
                break;
            }
            end_depth += 1;
        }
        assert!(
            end_depth < KEY_LEN,
            "strictly ordered archive keys must eventually diverge",
        );

        let key_index = O::TREE_TO_KEY[end_depth];
        let mut group_start = 0;
        let first_byte = keys[0][key_index];
        let mut first_end = 1;
        while first_end < keys.len() && keys[first_end][key_index] == first_byte {
            first_end += 1;
        }
        debug_assert!(first_end < keys.len(), "a Branch needs two child groups");

        let second_byte = keys[first_end][key_index];
        let mut second_end = first_end + 1;
        while second_end < keys.len() && keys[second_end][key_index] == second_byte {
            second_end += 1;
        }

        let (first_head, first_hash) = unsafe {
            Self::build_sorted_archive_head_for_test(
                &keys[group_start..first_end],
                owner,
                end_depth + 1,
            )
        };
        group_start = first_end;
        let (second_head, second_hash) = unsafe {
            Self::build_sorted_archive_head_for_test(
                &keys[group_start..second_end],
                owner,
                end_depth + 1,
            )
        };
        group_start = second_end;

        let body = Branch::new_with_child_hashes(
            end_depth,
            first_head.with_key(first_byte),
            second_head.with_key(second_byte),
            first_hash,
            second_hash,
        );
        let mut root = Head::new(0, body);
        let mut hash = first_hash ^ second_hash;

        while group_start < keys.len() {
            let byte = keys[group_start][key_index];
            let mut group_end = group_start + 1;
            while group_end < keys.len() && keys[group_end][key_index] == byte {
                group_end += 1;
            }
            let (child, child_hash) = unsafe {
                Self::build_sorted_archive_head_for_test(
                    &keys[group_start..group_end],
                    owner,
                    end_depth + 1,
                )
            };
            let mut editor = BranchMut::from_head(&mut root);
            editor.modify_child(byte, |old| {
                debug_assert!(old.is_none());
                Some(child.with_key(byte))
            });
            drop(editor);
            hash ^= child_hash;
            group_start = group_end;
        }

        // The child editor may have dirtied this Branch when it encountered a
        // LocalLeaf with no resident descriptor. The recursion independently
        // proved the exact aggregate, so publish it without descending again.
        root.publish_known_hash(hash);
        (root, hash)
    }

    /// Builds a canonical PATCH directly from an unordered row permutation by
    /// partitioning that one buffer in place at each trie depth.
    ///
    /// This all-six archive experiment deliberately fuses ordering and trie
    /// construction: no sorted pointer array or per-row leaf descriptor is
    /// retained. `hashes[row]` is the one transient hash computed for that
    /// archive row and shared by every index build.
    ///
    /// # Safety
    ///
    /// - Every `rows` entry must occur exactly once and index both `keys` and
    ///   `hashes`.
    /// - Every key pointer must be 16-byte aligned, immutable, and kept alive
    ///   by an archive owner already retained in `guard`.
    /// - `keys` must contain no duplicates.
    #[cfg(test)]
    pub(crate) unsafe fn from_archive_partition_for_test(
        keys: &[[u8; KEY_LEN]],
        hashes: &[u128],
        rows: &mut [u32],
        guard: &PATCHOwnerGuard,
    ) -> Self {
        unsafe {
            Self::from_archive_partition_with_stats_sink_for_test::<false>(
                keys,
                hashes,
                rows,
                guard,
                std::ptr::null_mut(),
            )
        }
    }

    /// Counted or zero-cost-uninstrumented implementation of
    /// [`Self::from_archive_partition_for_test`]. The all-six census passes one
    /// shared sink through every index; ordinary and timed construction uses
    /// the `false` monomorphization, in which every counter branch disappears.
    /// When `COUNT` is true, `stats` must be a valid exclusive pointer for the
    /// complete call; when false it is never dereferenced and may be null.
    #[cfg(test)]
    pub(crate) unsafe fn from_archive_partition_with_stats_sink_for_test<const COUNT: bool>(
        keys: &[[u8; KEY_LEN]],
        hashes: &[u128],
        rows: &mut [u32],
        guard: &PATCHOwnerGuard,
        stats: *mut BranchBuildStats,
    ) -> Self {
        debug_assert!(!COUNT || !stats.is_null());
        // Branch child tables share the randomness initialized alongside the
        // SIP key. Initialize at this boundary so a prehashed caller cannot
        // build under the zero permutation and later observe a changed lookup.
        init_sip_key();
        assert_eq!(keys.len(), hashes.len());
        assert_eq!(keys.len(), rows.len());
        assert!(
            u32::try_from(rows.len()).is_ok(),
            "archive row ordinals must fit the partition metadata",
        );
        if rows.is_empty() {
            return Self::new();
        }

        let (root, _) = unsafe {
            Self::build_archive_partition_head_for_test::<COUNT>(keys, hashes, rows, 0, stats)
        };
        let result = Self {
            root: Some(root.with_start(0)),
            owners: guard.0.clone(),
        };
        result.debug_check_owner_invariant();
        result
    }

    /// Representative-LCP plus in-place MSD-radix worker for
    /// [`Self::from_archive_partition_for_test`].
    ///
    /// After examining `k` rows, `end_depth` is the minimum LCP length between
    /// the representative and those rows, bounded below by the incoming
    /// `depth`. Consequently every row shares `[depth, end_depth)`, and for a
    /// unique multi-row input at least two byte buckets exist at `end_depth`.
    /// Finding that compressed boundary row-major avoids one whole-slice pass
    /// for every shared byte of a long prefix.
    #[cfg(test)]
    unsafe fn build_archive_partition_head_for_test<const COUNT: bool>(
        keys: &[[u8; KEY_LEN]],
        hashes: &[u128],
        rows: &mut [u32],
        depth: usize,
        stats: *mut BranchBuildStats,
    ) -> (Head<KEY_LEN, O, ()>, u128) {
        debug_assert!(!rows.is_empty());
        if rows.len() == 1 {
            let row = rows[0] as usize;
            let ptr = NonNull::from(&keys[row]);
            // SAFETY: the caller proves that every archive key stays aligned,
            // immutable, and covered by the shared root owner guard.
            let head = unsafe { Head::new_local_leaf(0, ptr) };
            return (head, hashes[row]);
        }
        let representative = &keys[rows[0] as usize];
        let mut end_depth = KEY_LEN;
        for &row in &rows[1..] {
            let key = &keys[row as usize];
            let mut candidate_depth = depth;
            while candidate_depth < end_depth {
                let key_index = O::TREE_TO_KEY[candidate_depth];
                if representative[key_index] != key[key_index] {
                    end_depth = candidate_depth;
                    break;
                }
                candidate_depth += 1;
            }
            if end_depth == depth {
                break;
            }
        }
        assert!(
            end_depth < KEY_LEN,
            "duplicate archive keys cannot form a finite trie",
        );

        let key_index = O::TREE_TO_KEY[end_depth];
        let mut ends = [0u32; 256];
        let mut occupied = ByteSet::new_empty();
        for &row in rows.iter() {
            let byte = keys[row as usize][key_index];
            let count = &mut ends[byte as usize];
            if *count == 0 {
                occupied.insert(byte);
            }
            *count += 1;
        }
        let mut child_buckets = occupied;
        let first_bucket = child_buckets
            .drain_next_ascending()
            .expect("a non-empty partition has one bucket");
        let second_bucket = child_buckets
            .drain_next_ascending()
            .expect("a unique multi-row node has two buckets");
        let first_extra = child_buckets.drain_next_ascending();
        let fanout = first_extra.map_or(2, |_| 3 + child_buckets.popcount() as usize);
        debug_assert!((2..=256).contains(&fanout));
        let initial_slots = if first_extra.is_none() {
            // Keep binary construction free of the fanout popcount and wider
            // capacity path; draining the absent third bucket was already the
            // parent arm's early-return test.
            2
        } else {
            fanout.next_power_of_two()
        };
        if COUNT {
            let stats = unsafe { &mut *stats };
            stats.branches += 1;
            stats.initial_slots += initial_slots as u64;
        }

        // Turn the histogram into cumulative exclusive ends. `next` advances
        // the first unfilled position in each occupied range. u32 is exact:
        // the public probe rejects archives whose row ordinals do not fit it.
        let mut next = [0u32; 256];
        let mut offset = 0u32;
        let mut prefix_buckets = occupied;
        while let Some(byte) = prefix_buckets.drain_next_ascending() {
            let count = ends[byte as usize];
            next[byte as usize] = offset;
            offset += count;
            ends[byte as usize] = offset;
        }
        debug_assert_eq!(offset as usize, rows.len());

        // American-flag partition. Every swap permanently fills one
        // destination position, so the pass is linear and needs no second
        // permutation buffer. Absent byte buckets require no work.
        let mut partition_buckets = occupied;
        while let Some(byte) = partition_buckets.drain_next_ascending() {
            let bucket = byte as usize;
            while next[bucket] < ends[bucket] {
                let position = next[bucket] as usize;
                let row = rows[position] as usize;
                let destination = keys[row][key_index] as usize;
                if destination == bucket {
                    next[bucket] += 1;
                } else {
                    let destination_slot = next[destination] as usize;
                    debug_assert!(destination_slot < ends[destination] as usize);
                    rows.swap(position, destination_slot);
                    next[destination] += 1;
                }
            }
        }

        let first_end = ends[first_bucket as usize] as usize;
        let (first_head, first_hash) = unsafe {
            Self::build_archive_partition_head_for_test::<COUNT>(
                keys,
                hashes,
                &mut rows[..first_end],
                end_depth + 1,
                stats,
            )
        };
        let second_end = ends[second_bucket as usize] as usize;
        let (second_head, second_hash) = unsafe {
            Self::build_archive_partition_head_for_test::<COUNT>(
                keys,
                hashes,
                &mut rows[first_end..second_end],
                end_depth + 1,
                stats,
            )
        };

        let body = if initial_slots == 2 {
            // Preserve the old binary fast path exactly: its one bucket is the
            // only case where raw slots zero and one are valid lookup slots.
            Branch::new_with_child_hashes(
                end_depth,
                first_head.with_key(first_bucket),
                second_head.with_key(second_bucket),
                first_hash,
                second_hash,
            )
        } else {
            Branch::new_with_child_hashes_capacity(
                end_depth,
                first_head.with_key(first_bucket),
                second_head.with_key(second_bucket),
                first_hash,
                second_hash,
                initial_slots,
            )
        };
        let mut root = Head::new(0, body);
        let mut hash = first_hash ^ second_hash;
        let Some(mut byte) = first_extra else {
            debug_assert_eq!(second_end, rows.len());
            if COUNT {
                let stats = unsafe { &mut *stats };
                stats.final_slots += 2;
            }
            return (root, hash);
        };
        let mut editor = BranchMut::from_head(&mut root);
        let mut range_start = second_end;
        let mut resident_children = 2usize;

        loop {
            let range_end = ends[byte as usize] as usize;
            let (child, child_hash) = unsafe {
                Self::build_archive_partition_head_for_test::<COUNT>(
                    keys,
                    hashes,
                    &mut rows[range_start..range_end],
                    end_depth + 1,
                    stats,
                )
            };
            hash ^= child_hash;
            if COUNT {
                let stats = unsafe { &mut *stats };
                editor.install_child_growing_counted(
                    child.with_key(byte),
                    resident_children,
                    stats,
                );
            } else {
                editor.install_child_growing(child.with_key(byte));
            }
            resident_children += 1;
            range_start = range_end;
            let Some(next_byte) = child_buckets.drain_next_ascending() else {
                break;
            };
            byte = next_byte;
        }
        debug_assert_eq!(range_start, rows.len());
        debug_assert_eq!(resident_children, fanout);
        if COUNT {
            let stats = unsafe { &mut *stats };
            stats.final_slots += editor.child_table.len() as u64;
        }

        // Bulk installation deliberately leaves the first-two aggregates
        // untouched until every remaining child is present. Rebuild counts,
        // choose any valid representative, and publish the independently
        // accumulated exact XOR in one physical table scan.
        editor.finish_bulk_aggregates(Some(hash));
        drop(editor);
        (root, hash)
    }

    /// Builds the smallest valid compressed trie for two distinct entries
    /// from the same archive owner. Because the batch cardinality is already
    /// known, both roots can remain LocalLeaves under one ordinary Branch; no
    /// heap seed or unary Branch is required.
    #[cfg(test)]
    pub(crate) fn from_archive_pair(
        first: &ArchiveEntry<'_, KEY_LEN>,
        second: &ArchiveEntry<'_, KEY_LEN>,
    ) -> Self {
        let guard = PATCHOwnerGuard::default();
        Self::from_archive_pair_with_guard(first, second, &guard)
    }

    /// Build an archive pair under a receipt already shared by an aggregate.
    /// This avoids constructing one equivalent singleton cover per index.
    pub(crate) fn from_archive_pair_with_guard(
        first: &ArchiveEntry<'_, KEY_LEN>,
        second: &ArchiveEntry<'_, KEY_LEN>,
        guard: &PATCHOwnerGuard,
    ) -> Self {
        let (first_head, first_owner, first_hash) = first.leaf::<O>();
        let (second_head, second_owner, second_hash) = second.leaf::<O>();
        assert!(
            std::sync::Arc::ptr_eq(first_owner, second_owner),
            "an archive bootstrap pair must share one owner",
        );
        // Retain here so this safe constructor remains sound even if a future
        // crate-internal caller supplies an unrelated receipt. In the
        // aggregate fast path the owner is already latest, preserving the
        // shared Arc without another cover node.
        let mut guard = guard.clone();
        guard.retain_archive_owner(first_owner);
        let (depth, first_key, second_key) = first_head
            .first_divergence(&second_head, 0)
            .expect("an archive bootstrap pair must contain distinct keys");
        let root_key = first_head.key();
        let branch = Branch::new_with_child_hashes(
            depth,
            first_head.with_key(first_key),
            second_head.with_key(second_key),
            first_hash,
            second_hash,
        );
        let result = Self {
            root: Some(Head::new(root_key, branch)),
            owners: guard.0,
        };
        result.debug_check_owner_invariant();
        result
    }

    /// Inserts an archive-backed key and retains its allocation in the PATCH's
    /// persistent root owner cover.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, KEY_LEN>) {
        let old_count = self.len();
        let old_hash = self.root.as_ref().and_then(Head::known_hash);
        let (leaf_head, leaf_owner, leaf_hash) = entry.leaf::<O>();
        OwnerCover::retain(&mut self.owners, leaf_owner);
        if let Some(this) = self.root.take() {
            // Trie mutation is storage-agnostic. Archive-specific hash
            // knowledge belongs to the PATCH operation boundary below, where
            // cardinality proves whether this was a duplicate or a new key.
            let new_head = Head::insert_leaf(this, leaf_head, 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(leaf_head);
        }
        self.publish_insert_one_delta(old_count, old_hash, leaf_hash);
        self.debug_check_owner_invariant();
    }
}

impl<const KEY_LEN: usize, O, V> PartialEq for PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn eq(&self, other: &Self) -> bool {
        if self.same_root_body(other) {
            return true;
        }
        self.root.as_ref().map(|root| root.hash()) == other.root.as_ref().map(|root| root.hash())
    }
}

impl<const KEY_LEN: usize, O, V> Eq for PATCH<KEY_LEN, O, V> where O: KeySchema<KEY_LEN> {}

impl<'a, const KEY_LEN: usize, O, V> IntoIterator for &'a PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    type Item = &'a [u8; KEY_LEN];
    type IntoIter = PATCHIterator<'a, KEY_LEN, O, V>;

    fn into_iter(self) -> Self::IntoIter {
        PATCHIterator::new(self)
    }
}

/// An iterator over all keys in a PATCH.
/// The keys are returned in key ordering but in random order.
pub struct PATCHIterator<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    stack: ArrayVec<std::slice::Iter<'a, Option<Head<KEY_LEN, O, V>>>, KEY_LEN>,
    remaining: usize,
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> PATCHIterator<'a, KEY_LEN, O, V> {
    /// Creates an iterator over all keys in `patch`.
    pub fn new(patch: &'a PATCH<KEY_LEN, O, V>) -> Self {
        let mut r = PATCHIterator {
            stack: ArrayVec::new(),
            remaining: patch.len().min(usize::MAX as u64) as usize,
        };
        r.stack.push(std::slice::from_ref(&patch.root).iter());
        r
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for PATCHIterator<'a, KEY_LEN, O, V>
{
    type Item = &'a [u8; KEY_LEN];

    fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.stack.last_mut()?;
        loop {
            if let Some(child) = iter.next() {
                if let Some(child) = child {
                    match child.body_ref() {
                        BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                            self.remaining = self.remaining.saturating_sub(1);
                            // Use the safe accessor on the child reference to obtain the leaf key bytes.
                            return Some(child.childleaf_key());
                        }
                        BodyRef::Branch(branch) => {
                            self.stack.push(branch.child_table.iter());
                            iter = self.stack.last_mut()?;
                        }
                    }
                }
            } else {
                self.stack.pop();
                iter = self.stack.last_mut()?;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> ExactSizeIterator
    for PATCHIterator<'a, KEY_LEN, O, V>
{
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> std::iter::FusedIterator
    for PATCHIterator<'a, KEY_LEN, O, V>
{
}

/// An iterator over every key in a PATCH, returned in key order.
///
/// Keys are yielded in lexicographic key order regardless of their physical
/// layout in the underlying tree. This iterator walks the full tree and does
/// not accept a prefix filter. For prefix-aware iteration, use
/// [`PATCHPrefixIterator`], constructed via [`PATCH::iter_prefix_count`].
pub struct PATCHOrderedIterator<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    stack: Vec<ArrayVec<&'a Head<KEY_LEN, O, V>, 256>>,
    remaining: usize,
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> PATCHOrderedIterator<'a, KEY_LEN, O, V> {
    pub fn new(patch: &'a PATCH<KEY_LEN, O, V>) -> Self {
        let mut r = PATCHOrderedIterator {
            stack: Vec::with_capacity(KEY_LEN),
            remaining: patch.len().min(usize::MAX as u64) as usize,
        };
        if let Some(root) = &patch.root {
            r.stack.push(ArrayVec::new());
            match root.body_ref() {
                BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                    r.stack[0].push(root);
                }
                BodyRef::Branch(branch) => {
                    let first_level = &mut r.stack[0];
                    first_level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                    first_level.sort_unstable_by_key(|&k| Reverse(k.key())); // We need to reverse here because we pop from the vec.
                }
            }
        }
        r
    }
}

// --- Owned consuming iterators ---
/// Iterator that owns a PATCH and yields keys in key-order. The iterator
/// consumes the PATCH, drains owned Heads through a queue, and keeps the
/// PATCH's archive-owner cover alive until every LocalLeaf has been copied out.
pub struct PATCHIntoIterator<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    // Field order is deliberate: queued Heads drop before the owner guard.
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
    _owners: Option<Arc<OwnerCover>>,
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> PATCHIntoIterator<KEY_LEN, O, V> {}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator for PATCHIntoIterator<KEY_LEN, O, V> {
    type Item = [u8; KEY_LEN];

    fn next(&mut self) -> Option<Self::Item> {
        let q = &mut self.queue;
        while let Some(mut head) = q.pop() {
            // Match on the mutable body directly. For leaves we can return the
            // stored key (the array is Copy), for branches we take children out
            // of the table and push them onto the stack so they are visited
            // depth-first.
            match head.body_mut() {
                BodyMut::Leaf(leaf) => {
                    self.remaining = self.remaining.saturating_sub(1);
                    return Some(leaf.key);
                }
                BodyMut::LocalLeaf(bytes) => {
                    self.remaining = self.remaining.saturating_sub(1);
                    return Some(*bytes);
                }
                BodyMut::Branch(branch) => {
                    for slot in branch.child_table.iter_mut().rev() {
                        if let Some(c) = slot.take() {
                            q.push(c);
                        }
                    }
                }
            }
        }
        None
    }
}

/// Iterator that owns a PATCH and yields keys in key order.
pub struct PATCHIntoOrderedIterator<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    // Field order is deliberate: queued Heads drop before the owner guard.
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
    _owners: Option<Arc<OwnerCover>>,
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for PATCHIntoOrderedIterator<KEY_LEN, O, V>
{
    type Item = [u8; KEY_LEN];

    fn next(&mut self) -> Option<Self::Item> {
        let q = &mut self.queue;
        while let Some(mut head) = q.pop() {
            // Match the mutable body directly — we own `head` so calling
            // `body_mut()` is safe and allows returning the copied leaf key
            // or mutating the branch child table in-place.
            match head.body_mut() {
                BodyMut::Leaf(leaf) => {
                    self.remaining = self.remaining.saturating_sub(1);
                    return Some(leaf.key);
                }
                BodyMut::LocalLeaf(bytes) => {
                    self.remaining = self.remaining.saturating_sub(1);
                    return Some(*bytes);
                }
                BodyMut::Branch(branch) => {
                    let slice: &mut [Option<Head<KEY_LEN, O, V>>] = &mut branch.child_table;
                    // Sort children by their byte-key, placing empty slots (None)
                    // after all occupied slots. Using `sort_unstable_by_key` with
                    // a simple key projection is clearer than a custom
                    // comparator; it also avoids allocating temporaries. The
                    // old comparator manually handled None/Some cases — we
                    // express that intent directly by sorting on the tuple
                    // (is_none, key_opt).
                    slice
                        .sort_unstable_by_key(|opt| (opt.is_none(), opt.as_ref().map(|h| h.key())));
                    for slot in slice.iter_mut().rev() {
                        if let Some(c) = slot.take() {
                            q.push(c);
                        }
                    }
                }
            }
        }
        None
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> IntoIterator for PATCH<KEY_LEN, O, V> {
    type Item = [u8; KEY_LEN];
    type IntoIter = PATCHIntoIterator<KEY_LEN, O, V>;

    fn into_iter(self) -> Self::IntoIter {
        let remaining = self.len().min(usize::MAX as u64) as usize;
        let PATCH { root, owners } = self;
        let mut q = Vec::new();
        if let Some(root) = root {
            q.push(root);
        }
        PATCHIntoIterator {
            queue: q,
            remaining,
            _owners: owners,
        }
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> PATCH<KEY_LEN, O, V> {
    /// Consume and return an iterator that yields keys in key order.
    pub fn into_iter_ordered(self) -> PATCHIntoOrderedIterator<KEY_LEN, O, V> {
        let remaining = self.len().min(usize::MAX as u64) as usize;
        let PATCH { root, owners } = self;
        let mut q = Vec::new();
        if let Some(root) = root {
            q.push(root);
        }
        PATCHIntoOrderedIterator {
            queue: q,
            remaining,
            _owners: owners,
        }
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for PATCHOrderedIterator<'a, KEY_LEN, O, V>
{
    type Item = &'a [u8; KEY_LEN];

    fn next(&mut self) -> Option<Self::Item> {
        let mut level = self.stack.last_mut()?;
        loop {
            if let Some(child) = level.pop() {
                match child.body_ref() {
                    BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                        self.remaining = self.remaining.saturating_sub(1);
                        return Some(child.childleaf_key());
                    }
                    BodyRef::Branch(branch) => {
                        self.stack.push(ArrayVec::new());
                        level = self.stack.last_mut()?;
                        level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                        level.sort_unstable_by_key(|&k| Reverse(k.key())); // We need to reverse here because we pop from the vec.
                    }
                }
            } else {
                self.stack.pop();
                level = self.stack.last_mut()?;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> ExactSizeIterator
    for PATCHOrderedIterator<'a, KEY_LEN, O, V>
{
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> std::iter::FusedIterator
    for PATCHOrderedIterator<'a, KEY_LEN, O, V>
{
}

/// An iterator over all keys in a PATCH that have a given prefix.
/// The keys are returned in tree ordering and in tree order.
pub struct PATCHPrefixIterator<
    'a,
    const KEY_LEN: usize,
    const PREFIX_LEN: usize,
    O: KeySchema<KEY_LEN>,
    V,
> {
    stack: Vec<ArrayVec<&'a Head<KEY_LEN, O, V>, 256>>,
}

impl<'a, const KEY_LEN: usize, const PREFIX_LEN: usize, O: KeySchema<KEY_LEN>, V>
    PATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V>
{
    fn new(patch: &'a PATCH<KEY_LEN, O, V>) -> Self {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        let mut r = PATCHPrefixIterator {
            stack: Vec::with_capacity(PREFIX_LEN),
        };
        if let Some(root) = &patch.root {
            r.stack.push(ArrayVec::new());
            if root.end_depth() >= PREFIX_LEN {
                r.stack[0].push(root);
            } else {
                let BodyRef::Branch(branch) = root.body_ref() else {
                    unreachable!();
                };
                let first_level = &mut r.stack[0];
                first_level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                first_level.sort_unstable_by_key(|&k| Reverse(k.key())); // We need to reverse here because we pop from the vec.
            }
        }
        r
    }
}

impl<'a, const KEY_LEN: usize, const PREFIX_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for PATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V>
{
    type Item = ([u8; PREFIX_LEN], u64);

    fn next(&mut self) -> Option<Self::Item> {
        let mut level = self.stack.last_mut()?;
        loop {
            if let Some(child) = level.pop() {
                if child.end_depth() >= PREFIX_LEN {
                    let key = O::tree_ordered(child.childleaf_key());
                    let suffix_count = child.count();
                    return Some((key[0..PREFIX_LEN].try_into().unwrap(), suffix_count));
                } else {
                    let BodyRef::Branch(branch) = child.body_ref() else {
                        unreachable!();
                    };
                    self.stack.push(ArrayVec::new());
                    level = self.stack.last_mut()?;
                    level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                    level.sort_unstable_by_key(|&k| Reverse(k.key())); // We need to reverse here because we pop from the vec.
                }
            } else {
                self.stack.pop();
                level = self.stack.last_mut()?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::blob::{Blob, TryFromBlob};
    use crate::inline::Encodes;
    use crate::trible::{Trible, TribleSet};
    use itertools::Itertools;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::convert::TryInto;
    use std::iter::FromIterator;
    use std::mem;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn patch_unowned_direct_local_leaves<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
        patch: &PATCH<KEY_LEN, O, V>,
    ) -> usize {
        if patch.owners.is_none() {
            patch.node_stats().3 as usize
        } else {
            0
        }
    }

    #[repr(C, align(16))]
    struct AlignedArchiveKey<const KEY_LEN: usize>([u8; KEY_LEN]);

    /// Return a PATCH whose root owner cover is the only remaining owner of the
    /// two archive rows. This makes lifetime regressions deterministic: no
    /// fixture Arc can accidentally keep dangling LocalLeaves alive.
    fn owned_archive_pair<const KEY_LEN: usize>(
        keys: [[u8; KEY_LEN]; 2],
    ) -> PATCH<KEY_LEN, IdentitySchema> {
        let storage = std::sync::Arc::new([AlignedArchiveKey(keys[0]), AlignedArchiveKey(keys[1])]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let patch = {
            let entries: [ArchiveEntry<'_, KEY_LEN>; 2] = std::array::from_fn(|i| unsafe {
                ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
            });
            PATCH::from_archive_pair(&entries[0], &entries[1])
        };
        drop(owner);
        drop(storage);
        patch
    }

    fn owned_archive_single<const KEY_LEN: usize>(key: [u8; KEY_LEN]) -> PATCH<KEY_LEN> {
        let storage = std::sync::Arc::new(AlignedArchiveKey(key));
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let mut patch = PATCH::new();
        let entry = unsafe { ArchiveEntry::new(NonNull::from(&storage.0), &owner) };
        patch.insert_archive(&entry);
        drop(owner);
        drop(storage);
        patch
    }

    fn sorted_archive_fixture(len: usize, leading_byte: u8) -> Arc<Vec<AlignedArchiveKey<64>>> {
        let mut rows = Vec::with_capacity(len);
        for index in 0..len {
            let serial = u64::try_from(index + 1)
                .expect("bottom-up fixture cardinality must fit u64")
                .checked_add(u64::from(leading_byte) << 56)
                .expect("bottom-up fixture serial must not overflow")
                .to_be_bytes();
            let mut key = [0u8; 64];
            key[..8].copy_from_slice(&serial);

            // Keep ordering controlled solely by the leading serial while
            // making the remaining bytes nontrivial enough to exercise hash
            // cost on the same 64-byte keys as a SimpleArchive index.
            let mut state = index as u64 ^ 0x9e37_79b9_7f4a_7c15;
            for chunk in key[8..].chunks_exact_mut(8) {
                state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let mut mixed = state;
                mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                mixed ^= mixed >> 31;
                chunk.copy_from_slice(&mixed.to_be_bytes());
            }
            rows.push(AlignedArchiveKey(key));
        }

        Arc::new(rows)
    }

    fn aligned_archive_keys(rows: &[AlignedArchiveKey<64>]) -> &[[u8; 64]] {
        assert_eq!(mem::size_of::<AlignedArchiveKey<64>>(), 64);
        assert_eq!(mem::align_of::<AlignedArchiveKey<64>>(), 16);
        // SAFETY: the repr(C, align(16)) wrapper has the same 64-byte size as
        // its sole field, so this preserves element boundaries and lifetime.
        unsafe { std::slice::from_raw_parts(rows.as_ptr().cast::<[u8; 64]>(), rows.len()) }
    }

    unsafe fn online_sorted_archive_index(
        keys: &[[u8; 64]],
        owner: &Arc<dyn ArchiveOwner>,
    ) -> PATCH<64> {
        let Some(first_key) = keys.first() else {
            return PATCH::new();
        };
        let first = unsafe { ArchiveEntry::new(NonNull::from(first_key), owner) };
        let Some(second_key) = keys.get(1) else {
            let mut patch = PATCH::new();
            patch.insert_archive(&first);
            return patch;
        };
        let second = unsafe { ArchiveEntry::new(NonNull::from(second_key), owner) };
        let mut patch = PATCH::from_archive_pair(&first, &second);
        for key in &keys[2..] {
            let entry = unsafe { ArchiveEntry::new(NonNull::from(key), owner) };
            patch.insert_archive(&entry);
        }
        patch
    }

    fn assert_all_branch_hashes_resident<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        head: &Head<KEY_LEN, O, ()>,
    ) {
        if let BodyRef::Branch(branch) = head.body_ref() {
            assert!(
                branch.cached_hash().is_some(),
                "bottom-up Branch must retain its exact XOR receipt",
            );
            for child in branch.child_table.iter().flatten() {
                assert_all_branch_hashes_resident(child);
            }
        }
    }

    #[test]
    fn direct_capacity_archive_branches_cover_known_fanouts() {
        for fanout in [2usize, 3, 4, 5, 127, 128, 129, 255, 256] {
            // Multiplication by an odd byte permutes 0..=255. Prefixes of this
            // sequence therefore exercise sparse, cross-word, and dense byte
            // sets without introducing duplicate children.
            let storage = Arc::new(
                (0..fanout)
                    .map(|index| {
                        let byte = (index as u8).wrapping_mul(137).wrapping_add(53);
                        let mut key = [0u8; 64];
                        key[0] = byte;
                        AlignedArchiveKey(key)
                    })
                    .collect::<Vec<_>>(),
            );
            let keys = aligned_archive_keys(&storage);
            let owner: Arc<dyn ArchiveOwner> = storage.clone();
            let mut guard = PATCHOwnerGuard::default();
            guard.retain_archive_owner(&owner);
            let hashes = keys
                .iter()
                .map(|key| hash_key(&key[..]))
                .collect::<Vec<_>>();
            let mut rows = (0..fanout as u32).rev().collect::<Vec<_>>();
            let mut stats = BranchBuildStats::default();

            let patch = unsafe {
                PATCH::<64>::from_archive_partition_with_stats_sink_for_test::<true>(
                    keys,
                    &hashes,
                    &mut rows,
                    &guard,
                    &mut stats,
                )
            };

            let mut expected = keys.to_vec();
            expected.sort_unstable();
            assert_eq!(patch.iter_ordered().copied().collect_vec(), expected);
            for key in keys {
                assert!(
                    patch.get(key).is_some(),
                    "direct-capacity child must occupy a valid cuckoo lookup slot",
                );
            }
            assert_eq!(
                patch.root_hash(),
                hashes.iter().copied().fold(0, |acc, hash| acc ^ hash),
            );
            assert_eq!(patch.branch_fanout_histogram()[fanout], 1);
            assert_eq!(stats.branches, 1);
            assert_eq!(stats.initial_slots, fanout.next_power_of_two() as u64);
            assert_eq!(stats.final_slots, patch.total_table_slots());
            assert!(stats.final_slots >= stats.initial_slots);
            assert!(stats.final_slots <= 256);
            assert!(stats.final_slots.is_power_of_two());
            assert_eq!(
                stats.grow_calls,
                (stats.final_slots / stats.initial_slots).ilog2() as u64,
            );
            assert_eq!(
                stats.grow_scanned_slots,
                stats.final_slots - stats.initial_slots,
            );
            assert_eq!(
                stats.grow_allocated_slots,
                2 * (stats.final_slots - stats.initial_slots),
            );
            assert!(
                stats.heads_moved_by_grow
                    <= (fanout as u64 - 1) * stats.grow_calls,
            );
            if stats.initial_slots == 256 {
                assert_eq!(stats.grow_calls, 0);
                assert_eq!(stats.heads_moved_by_grow, 0);
            }

            let survivor = patch.clone();
            drop(patch);
            drop(guard);
            drop(owner);
            drop(storage);
            std::hint::black_box(vec![0xa5u8; fanout * 64]);
            assert_eq!(survivor.iter_ordered().copied().collect_vec(), expected);
        }
    }

    #[test]
    fn bottom_up_sorted_archive_matches_online_canonical_index() {
        for (len, leading_byte) in [
            (1usize, 0x00),
            (2, 0x80),
            (3, 0x80),
            (257, 0x80),
            (8_192, 0x00),
        ] {
            let storage = sorted_archive_fixture(len, leading_byte);
            let keys = aligned_archive_keys(&storage);
            let owner: Arc<dyn ArchiveOwner> = storage.clone();
            let online = unsafe { online_sorted_archive_index(keys, &owner) };
            let bottom_up = unsafe { PATCH::<64>::from_sorted_archive_keys_for_test(keys, &owner) };

            assert_eq!(bottom_up.len(), len as u64);
            assert_eq!(
                bottom_up.root.as_ref().expect("non-empty fixture").key(),
                leading_byte,
                "the top-level Head must not retain its recursive placeholder byte",
            );
            assert_eq!(
                bottom_up.iter_ordered().copied().collect_vec(),
                online.iter_ordered().copied().collect_vec(),
            );
            assert_eq!(bottom_up.node_stats(), online.node_stats());
            assert_eq!(bottom_up.branch_histogram(), online.branch_histogram());
            assert_eq!(
                bottom_up.branch_fanout_histogram(),
                online.branch_fanout_histogram(),
            );
            assert_eq!(bottom_up.root_hash(), online.root_hash());
            #[cfg(debug_assertions)]
            bottom_up.debug_check_deep_hash_invariant();
            if let Some(root) = bottom_up.root.as_ref() {
                assert_all_branch_hashes_resident(root);
            }

            if leading_byte != 0 {
                let mut novel_key = [0u8; 64];
                novel_key[0] = leading_byte - 1;
                novel_key[63] = 0x5a;
                let novel_storage = Arc::new(AlignedArchiveKey(novel_key));
                let novel_owner: Arc<dyn ArchiveOwner> = novel_storage.clone();
                let novel =
                    unsafe { ArchiveEntry::new(NonNull::from(&novel_storage.0), &novel_owner) };
                let mut online_extended = online.clone();
                let mut bottom_up_extended = bottom_up.clone();
                online_extended.insert_archive(&novel);
                bottom_up_extended.insert_archive(&novel);
                assert_eq!(
                    bottom_up_extended.iter_ordered().copied().collect_vec(),
                    online_extended.iter_ordered().copied().collect_vec(),
                    "a nonzero root routing byte must survive later divergence",
                );
                assert_eq!(bottom_up_extended.root_hash(), online_extended.root_hash());
            }

            // Leave the PATCH owner cover as the only live archive receipt,
            // churn the allocator, then traverse every retained pointer.
            let survivor = bottom_up.clone();
            drop(bottom_up);
            drop(online);
            drop(owner);
            drop(storage);
            std::hint::black_box(vec![0xa5u8; len.saturating_mul(64).min(1 << 20)]);
            assert_eq!(survivor.iter_ordered().count(), len);
        }
    }

    /// Construction-only timing probe for the one-index falsifier. Run with:
    ///
    /// `cargo test -p triblespace-core --release bottom_up_archive_builder_timing -- --ignored --nocapture`
    #[test]
    #[ignore = "manual 100k/1m construction benchmark"]
    fn bottom_up_archive_builder_timing() {
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        fn time_one(keys: &[[u8; 64]], owner: &Arc<dyn ArchiveOwner>, bottom_up: bool) -> Duration {
            let start = Instant::now();
            let patch = if bottom_up {
                unsafe { PATCH::<64>::from_sorted_archive_keys_for_test(keys, owner) }
            } else {
                unsafe { online_sorted_archive_index(keys, owner) }
            };
            let elapsed = start.elapsed();
            black_box(patch.len());
            black_box(patch.root.as_ref().and_then(Head::known_hash));
            drop(patch);
            elapsed
        }

        fn median(samples: &mut [Duration]) -> Duration {
            samples.sort_unstable();
            samples[samples.len() / 2]
        }

        for (len, rounds) in [(100_000usize, 5usize), (1_000_000, 3)] {
            let storage = sorted_archive_fixture(len, 0);
            let keys = aligned_archive_keys(&storage);
            let owner: Arc<dyn ArchiveOwner> = storage.clone();

            let oracle = unsafe { online_sorted_archive_index(keys, &owner) };
            let candidate = unsafe { PATCH::<64>::from_sorted_archive_keys_for_test(keys, &owner) };
            assert_eq!(candidate.len(), oracle.len());
            assert_eq!(candidate.root_hash(), oracle.root_hash());
            assert_eq!(candidate.node_stats(), oracle.node_stats());
            drop(candidate);
            drop(oracle);

            let mut online_samples = Vec::with_capacity(rounds);
            let mut bottom_up_samples = Vec::with_capacity(rounds);
            for round in 0..rounds {
                if round % 2 == 0 {
                    online_samples.push(time_one(keys, &owner, false));
                    bottom_up_samples.push(time_one(keys, &owner, true));
                } else {
                    bottom_up_samples.push(time_one(keys, &owner, true));
                    online_samples.push(time_one(keys, &owner, false));
                }
            }

            let online = median(&mut online_samples);
            let bottom_up = median(&mut bottom_up_samples);
            println!(
                "bottom_up_archive_builder len={len} online_ms={:.3} bottom_up_ms={:.3} speedup={:.3}x online_ns_per_key={:.1} bottom_up_ns_per_key={:.1}",
                online.as_secs_f64() * 1e3,
                bottom_up.as_secs_f64() * 1e3,
                online.as_secs_f64() / bottom_up.as_secs_f64(),
                online.as_secs_f64() * 1e9 / len as f64,
                bottom_up.as_secs_f64() * 1e9 / len as f64,
            );
        }
    }

    /// Build one exact archive-backed row per selected first-byte bucket.
    /// Balanced unions of these variants create a resident root whose direct
    /// children deliberately remain dirty: the root knows the input hashes
    /// and disjoint overlap, while each LocalLeaf collision does not.
    #[cfg(feature = "parallel")]
    fn owned_archive_variant(
        bucket_start: usize,
        bucket_count: usize,
        variant: usize,
    ) -> PATCH<16> {
        assert!(bucket_start + bucket_count <= 256);
        assert!(bucket_count >= 2);
        assert!(variant < 256);
        let storage = Arc::new(
            (bucket_start..bucket_start + bucket_count)
                .map(|bucket| {
                    let mut key = [0u8; 16];
                    key[0] = bucket as u8;
                    key[1] = variant as u8;
                    AlignedArchiveKey(key)
                })
                .collect::<Vec<_>>(),
        );
        let owner: Arc<dyn ArchiveOwner> = storage.clone();
        let first = unsafe { ArchiveEntry::new(NonNull::from(&storage[0].0), &owner) };
        let second = unsafe { ArchiveEntry::new(NonNull::from(&storage[1].0), &owner) };
        let mut patch = PATCH::from_archive_pair(&first, &second);
        for key in storage.iter().skip(2) {
            let entry = unsafe { ArchiveEntry::new(NonNull::from(&key.0), &owner) };
            patch.insert_archive(&entry);
        }
        drop(owner);
        drop(storage);
        patch
    }

    #[cfg(feature = "parallel")]
    fn owned_archive_dirty_parent(
        bucket_start: usize,
        bucket_count: usize,
        variant_start: usize,
        variant_count: usize,
    ) -> PATCH<16> {
        assert!(variant_count.is_power_of_two());
        let mut groups: Vec<_> = (variant_start..variant_start + variant_count)
            .map(|variant| owned_archive_variant(bucket_start, bucket_count, variant))
            .collect();

        while groups.len() > 1 {
            let mut next = Vec::with_capacity((groups.len() + 1) / 2);
            let mut iter = groups.into_iter();
            while let Some(mut left) = iter.next() {
                if let Some(right) = iter.next() {
                    left.union(right);
                }
                next.push(left);
            }
            groups = next;
        }

        let mut result = groups.pop().expect("at least one archive variant");

        // Fully structural union deliberately leaves this naturally-built
        // root dirty. These parallel tests need the more specific state their
        // names promise: one exact root over dirty direct children. Install
        // the independently-derived aggregate without traversing (and thereby
        // memoizing) those LocalLeaf children.
        let exact = heap_hash_oracle(&result);
        let BodyMut::Branch(root) = result.root.as_mut().unwrap().body_mut() else {
            panic!("fixture root must be a Branch");
        };
        root.replace_cached_hash(Some(exact));
        result
    }

    #[cfg(feature = "parallel")]
    fn direct_dirty_branch_children(patch: &PATCH<16>) -> usize {
        let BodyRef::Branch(root) = patch.root.as_ref().expect("non-empty PATCH").body_ref() else {
            panic!("fixture root must be a Branch");
        };
        root.child_table
            .iter()
            .flatten()
            .filter(|child| {
                matches!(child.body_ref(), BodyRef::Branch(branch) if branch.cached_hash().is_none())
            })
            .count()
    }

    /// Drive the large scatter path while keeping child resolution on this
    /// test thread. This makes the thread-local LocalLeaf hash census exact
    /// without changing production instrumentation or introducing a global
    /// counter that would race unrelated tests.
    #[cfg(feature = "parallel")]
    fn union_with_exhausted_parallel_budget(
        mut left: PATCH<16>,
        mut right: PATCH<16>,
    ) -> PATCH<16> {
        OwnerCover::merge_into(&mut left.owners, &right.owners);
        let this = left.root.take().expect("left root");
        let other = right.root.take().expect("right root");
        let ctx = parallel_union::ParUnionCtx {
            budget: AtomicUsize::new(0),
        };
        left.root = Some(Head::par_union_with_ctx(this, other, 0, &ctx));
        left.debug_check_owner_invariant();
        left
    }

    /// Drive the large difference scatter on this thread so the thread-local
    /// LocalLeaf hash census covers every surviving child.
    #[cfg(feature = "parallel")]
    fn difference_with_exhausted_parallel_budget(left: &PATCH<16>, right: &PATCH<16>) -> PATCH<16> {
        let ctx = parallel_union::ParUnionCtx {
            budget: AtomicUsize::new(0),
        };
        let root = left
            .root
            .as_ref()
            .expect("left root")
            .par_difference_with_ctx(right.root.as_ref().expect("right root"), 0, &ctx);
        let owners = root.as_ref().and(left.owners.clone());
        let result = PATCH { root, owners };
        result.debug_check_owner_invariant();
        result
    }

    fn test_archive_owner(byte: u8) -> Arc<dyn ArchiveOwner> {
        Arc::new([byte])
    }

    fn heap_hash_oracle<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        patch: &PATCH<KEY_LEN, O>,
    ) -> u128 {
        let mut oracle = PATCH::<KEY_LEN, O>::new();
        for key in patch.iter() {
            oracle.insert(&Entry::new(key));
        }
        oracle
            .root_hash()
            .expect("a non-empty PATCH must have a root hash")
    }

    fn branch_cached_hash<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        patch: &PATCH<KEY_LEN, O>,
    ) -> u128 {
        let Some(root) = patch.root.as_ref() else {
            panic!("expected a non-empty PATCH");
        };
        let BodyRef::Branch(branch) = root.body_ref() else {
            panic!("expected a Branch root");
        };
        branch.cached_hash().unwrap_or(0)
    }

    fn demote_root_hash<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        patch: &mut PATCH<KEY_LEN, O>,
    ) {
        let Some(root) = patch.root.as_mut() else {
            panic!("expected a non-empty PATCH");
        };
        let BodyMut::Branch(branch) = root.body_mut() else {
            panic!("expected a Branch root");
        };
        branch.replace_cached_hash(None);
    }

    fn deep_hash_audit<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(patch: &PATCH<KEY_LEN, O>) {
        #[cfg(debug_assertions)]
        patch.debug_check_deep_hash_invariant();
        #[cfg(not(debug_assertions))]
        let _ = patch;
    }

    #[test]
    fn branch_cache_distinguishes_exact_zero_from_unknown() {
        let mut patch = PATCH::<2>::new();
        patch.insert(&Entry::new(&[0, 0]));
        patch.insert(&Entry::new(&[1, 0]));
        let original = match patch.root.as_ref().unwrap().body_ref() {
            BodyRef::Branch(branch) => branch.cached_hash(),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                panic!("fixture root must be a Branch")
            }
        };
        assert!(original.is_some());

        {
            let BodyMut::Branch(branch) = patch.root.as_mut().unwrap().body_mut() else {
                panic!("fixture root must be a Branch");
            };
            branch.replace_cached_hash(None);
            assert_eq!(branch.cached_hash(), None);
            branch.replace_cached_hash(Some(0));
            assert_eq!(branch.cached_hash(), Some(0));
        }

        // COW must preserve both the publication bit and the exact-zero words.
        // The synthetic value is restored before any semantic consumer runs.
        let mut snapshot = patch.clone();
        {
            let BodyMut::Branch(branch) = patch.root.as_mut().unwrap().body_mut() else {
                panic!("fixture root must be a Branch");
            };
            assert_eq!(branch.cached_hash(), Some(0));
            branch.replace_cached_hash(original);
        }
        {
            let BodyMut::Branch(branch) = snapshot.root.as_mut().unwrap().body_mut() else {
                panic!("fixture root must be a Branch");
            };
            assert_eq!(branch.cached_hash(), Some(0));
            branch.replace_cached_hash(original);
        }

        deep_hash_audit(&patch);
        deep_hash_audit(&snapshot);
    }

    #[test]
    fn first_fingerprint_consumer_memoizes_for_shared_snapshots() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;
        let mut patch = owned_archive_single(a);
        patch.union(owned_archive_single(b));
        let snapshot = patch.clone();
        assert_eq!(branch_cached_hash(&patch), 0);
        assert_eq!(branch_cached_hash(&snapshot), 0);

        let expected = heap_hash_oracle(&patch);
        reset_local_leaf_hash_calls();
        assert_eq!(patch.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 2);
        assert_eq!(branch_cached_hash(&snapshot), expected);
        assert_eq!(snapshot.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 2);
        deep_hash_audit(&patch);
        deep_hash_audit(&snapshot);
    }

    #[test]
    fn concurrent_first_consumers_publish_one_exact_value() {
        const KEY_LEN: usize = 8;
        let mut patch = owned_archive_single([0u8; KEY_LEN]);
        for byte in 1..32 {
            let mut key = [0u8; KEY_LEN];
            key[0] = byte;
            patch.union(owned_archive_single(key));
        }
        assert_eq!(branch_cached_hash(&patch), 0);
        let expected = heap_hash_oracle(&patch);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| assert_eq!(patch.root_hash(), Some(expected)));
            }
        });

        assert_eq!(branch_cached_hash(&patch), expected);
        deep_hash_audit(&patch);
    }

    #[test]
    fn shared_body_shortcuts_need_neither_fingerprints_nor_matching_route_bytes() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;

        let mut original = owned_archive_single(a);
        original.union(owned_archive_single(b));
        assert_eq!(branch_cached_hash(&original), 0);
        let snapshot = original.clone();

        let root = snapshot.root.as_ref().expect("fixture must be non-empty");
        let rerouted = root.clone().with_key(root.key().wrapping_add(1));
        assert!(root.same_body(&rerouted));
        drop(rerouted);

        reset_local_leaf_hash_calls();
        original.union(snapshot.clone());
        assert!(
            original
                .root
                .as_ref()
                .unwrap()
                .same_body(snapshot.root.as_ref().unwrap()),
            "union should retain the exact shared body",
        );
        assert_eq!(local_leaf_hash_calls(), 0);

        let intersection = original.intersect(&snapshot);
        assert!(intersection
            .root
            .as_ref()
            .unwrap()
            .same_body(snapshot.root.as_ref().unwrap()));
        assert!(original.difference(&snapshot).is_empty());
        assert_eq!(original, snapshot);
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&original);
    }

    #[test]
    fn head_shared_body_shortcuts_preserve_an_unpublished_branch_cache() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;

        let mut patch = owned_archive_single(a);
        patch.union(owned_archive_single(b));
        let root = patch.root.as_ref().expect("fixture must be non-empty");
        let BodyRef::Branch(branch) = root.body_ref() else {
            panic!("fixture root must be a Branch");
        };
        assert_eq!(branch.cached_hash(), None);

        let rerouted = root.clone().with_key(root.key().wrapping_add(1));
        reset_local_leaf_hash_calls();

        let union = Head::union(root.clone(), rerouted.clone(), 0);
        assert!(union.same_body(root));
        assert!(root
            .intersect(&rerouted, 0)
            .expect("shared bodies intersect to themselves")
            .same_body(root));
        assert!(root.difference(&rerouted, 0).is_none());

        #[cfg(feature = "parallel")]
        {
            assert!(Head::par_union(root.clone(), rerouted.clone(), 0).same_body(root));
            assert!(root
                .par_intersect(&rerouted, 0)
                .expect("shared bodies intersect to themselves")
                .same_body(root));
            assert!(root.par_difference(&rerouted, 0).is_none());
        }

        let BodyRef::Branch(branch) = root.body_ref() else {
            unreachable!("the root body cannot change");
        };
        assert_eq!(branch.cached_hash(), None);
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&patch);
    }

    #[test]
    fn lazy_union_keeps_disjoint_local_leaves_unhashed_across_dirty_children() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[1] = 1;
        let mut c = a;
        c[0] = 1;
        let mut d = c;
        d[1] = 1;
        let mut e = a;
        e[1] = 2;

        reset_local_leaf_hash_calls();

        let mut left = owned_archive_single(a);
        left.union(owned_archive_single(b));
        assert_eq!(branch_cached_hash(&left), 0);

        let mut right = owned_archive_single(c);
        right.union(owned_archive_single(d));
        assert_eq!(branch_cached_hash(&right), 0);

        // The new two-slot root has no way to know either dirty child's
        // aggregate. It must stay dirty rather than crossing the information
        // boundary by hashing all four LocalLeaves.
        left.union(right);
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(local_leaf_hash_calls(), 0);

        // A later asymmetric union descends through both dirty levels. The
        // dirty sentinel propagates, while all non-hash aggregates stay exact.
        left.union(owned_archive_single(e));
        assert_eq!(left.len(), 5);
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&left);

        let expected = heap_hash_oracle(&left);
        let before = local_leaf_hash_calls();
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls() - before, 5);
        assert_eq!(branch_cached_hash(&left), expected);

        // The first real consumer memoizes the exact aggregate through the
        // shared Branch. Repeated consumers are constant-time.
        let before = local_leaf_hash_calls();
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls() - before, 0);
        assert_eq!(branch_cached_hash(&left), expected);
    }

    #[test]
    fn lazy_union_dirty_root_survives_clone_and_cow_mutation() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[1] = 1;
        let mut c = a;
        c[1] = 2;

        let mut original = owned_archive_single(a);
        reset_local_leaf_hash_calls();
        original.union(owned_archive_single(b));
        assert_eq!(branch_cached_hash(&original), 0);
        assert_eq!(local_leaf_hash_calls(), 0);

        let mut changed = original.clone();
        changed.insert(&Entry::new(&c));
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&original), 0);
        assert_eq!(branch_cached_hash(&changed), 0);
        assert_eq!(original.len(), 2);
        assert_eq!(changed.len(), 3);
        deep_hash_audit(&original);
        deep_hash_audit(&changed);

        let original_expected = heap_hash_oracle(&original);
        let changed_expected = heap_hash_oracle(&changed);
        assert_eq!(original.root_hash(), Some(original_expected));
        assert_eq!(changed.root_hash(), Some(changed_expected));
    }

    #[test]
    fn ordinary_insert_updates_a_clean_ancestor_over_a_dirty_child() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[1] = 1;
        let mut c = a;
        c[0] = 1;
        let mut d = c;
        d[1] = 1;
        let mut inserted = a;
        inserted[1] = 2;
        let mut disjoint_insert = a;
        disjoint_insert[0] = 2;

        // Both input roots are exact because the archive-pair constructor has
        // both leaf hashes. Their key sets are globally disjoint despite
        // overlapping both first-byte buckets, so cardinality proves the
        // result hash while each collision remains a dirty LocalLeaf child.
        let mut patch = owned_archive_pair([a, c]);
        let other = owned_archive_pair([b, d]);
        let union_hash = patch.root_hash().unwrap() ^ other.root_hash().unwrap();
        reset_local_leaf_hash_calls();
        patch.union(other);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&patch), union_hash);
        let dirty_children = match patch.root.as_ref().unwrap().body_ref() {
            BodyRef::Branch(root) => root
                .child_table
                .iter()
                .flatten()
                .filter(
                    |child| matches!(child.body_ref(), BodyRef::Branch(branch) if branch.cached_hash().is_none()),
                )
                .count(),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => 0,
        };
        assert_eq!(dirty_children, 2);
        deep_hash_audit(&patch);

        // An unrelated known heap child can extend the exact parent without
        // consulting either dirty sibling. The ordinary local debug audit must
        // likewise remain resident-only.
        let mut extended = patch.clone();
        reset_local_leaf_hash_calls();
        extended.insert(&Entry::new(&disjoint_insert));
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_ne!(branch_cached_hash(&extended), 0);
        let before = local_leaf_hash_calls();
        assert!(extended.root_hash().is_some());
        assert_eq!(local_leaf_hash_calls(), before);
        deep_hash_audit(&extended);

        reset_local_leaf_hash_calls();
        patch.insert(&Entry::new(&inserted));
        assert_eq!(
            local_leaf_hash_calls(),
            0,
            "ordinary mutation must not recursively hash a dirty old child",
        );
        let expected = heap_hash_oracle(&patch);
        assert_eq!(
            branch_cached_hash(&patch),
            expected,
            "the known one-key delta must update the clean ancestor exactly",
        );
        assert_eq!(patch.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&patch);
    }

    #[test]
    fn archive_insert_updates_a_resident_ancestor_over_a_dirty_collision() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;
        let mut inserted = a;
        inserted[1] = 1;

        let storage = Arc::new([
            AlignedArchiveKey(a),
            AlignedArchiveKey(b),
            AlignedArchiveKey(inserted),
        ]);
        let owner: Arc<dyn ArchiveOwner> = storage.clone();
        let entries: [ArchiveEntry<'_, KEY_LEN>; 3] = std::array::from_fn(|i| unsafe {
            ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
        });
        let mut patch: PATCH<KEY_LEN> = PATCH::from_archive_pair(&entries[0], &entries[1]);
        let before = branch_cached_hash(&patch);
        let expected = before ^ entries[2].hash;

        reset_local_leaf_hash_calls();
        patch.insert_archive(&entries[2]);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&patch), expected);
        assert_eq!(patch.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        // The first-byte collision creates a dirty child, but the public
        // insertion boundary still knows the exact whole-set delta.
        let BodyRef::Branch(root) = patch.root.as_ref().unwrap().body_ref() else {
            panic!("three-key fixture must have a Branch root");
        };
        let collided = root
            .child_table
            .table_get(a[0])
            .expect("the collided first-byte child must remain present");
        let BodyRef::Branch(collided) = collided.body_ref() else {
            panic!("the first-byte collision must create a nested Branch");
        };
        assert_eq!(collided.cached_hash(), None);

        // Grow only one clone through the same dirty child, then independently
        // reinsert a duplicate. Both one-key outcomes are exact without
        // teaching Branch mutation about insertion semantics.
        let snapshot = patch.clone();
        let mut novel = inserted;
        novel[1] = 2;
        let novel_storage = Arc::new(AlignedArchiveKey(novel));
        let novel_owner: Arc<dyn ArchiveOwner> = novel_storage.clone();
        let novel_entry =
            unsafe { ArchiveEntry::new(NonNull::from(&novel_storage.0), &novel_owner) };
        let grown_expected = expected ^ novel_entry.hash;
        reset_local_leaf_hash_calls();
        patch.insert_archive(&novel_entry);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&snapshot), expected);
        assert_eq!(snapshot.root_hash(), Some(expected));
        assert_eq!(branch_cached_hash(&patch), grown_expected);
        assert_eq!(patch.root_hash(), Some(grown_expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        let duplicate_storage = Arc::new(AlignedArchiveKey(inserted));
        let duplicate_owner: Arc<dyn ArchiveOwner> = duplicate_storage.clone();
        let duplicate =
            unsafe { ArchiveEntry::new(NonNull::from(&duplicate_storage.0), &duplicate_owner) };
        patch.insert_archive(&duplicate);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&patch), grown_expected);
        assert_eq!(patch.root_hash(), Some(grown_expected));
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(snapshot.len(), 3);
        assert_eq!(patch.len(), 4);
        assert_eq!(
            snapshot.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([a, b, inserted])
        );
        assert_eq!(
            patch.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([a, b, inserted, novel])
        );
        deep_hash_audit(&snapshot);
        deep_hash_audit(&patch);
    }

    #[test]
    fn remove_boundary_hashes_the_canonical_key_under_a_nonidentity_order() {
        use crate::trible::AEVOrder;

        let mut first = [0u8; 64];
        first[0] = 1;
        first[16] = 10;
        first[32] = 20;
        let mut removed = first;
        removed[0] = 2;
        removed[16] = 11;
        removed[32] = 21;
        let mut third = first;
        third[0] = 3;
        third[16] = 12;
        third[32] = 22;

        let mut patch = PATCH::<64, AEVOrder>::new();
        patch.insert(&Entry::new(&first));
        patch.insert(&Entry::new(&removed));
        patch.insert(&Entry::new(&third));
        let old_hash = patch.root_hash().expect("three keys have a root hash");

        let removed_tree_key = AEVOrder::tree_ordered(&removed);
        assert_ne!(removed_tree_key, removed);
        patch.remove(&removed_tree_key);

        let expected = old_hash ^ hash_key(&removed);
        assert_eq!(patch.len(), 2);
        assert_eq!(patch.root_hash(), Some(expected));
        assert!(patch.get(&removed_tree_key).is_none());
        deep_hash_audit(&patch);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn remove_boundary_updates_a_resident_ancestor_over_dirty_children() {
        let mut patch = owned_archive_dirty_parent(0, 2, 0, 2);
        let snapshot = patch.clone();
        let old_hash = branch_cached_hash(&patch);
        assert_ne!(old_hash, 0);
        assert!(direct_dirty_branch_children(&patch) > 0);

        let mut removed = [0u8; 16];
        removed[0] = 0;
        removed[1] = 0;
        let expected = old_hash ^ hash_key(&removed);

        reset_local_leaf_hash_calls();
        patch.remove(&removed);
        assert_eq!(patch.len(), 3);
        assert_eq!(branch_cached_hash(&patch), expected);
        assert_eq!(patch.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);
        assert!(direct_dirty_branch_children(&patch) > 0);

        // A miss may still traverse and COW a dirty child. Cardinality proves
        // that the public operation left the key set unchanged and restores
        // the old exact root without consulting that child.
        let mut missing = removed;
        missing[1] = 99;
        patch.remove(&missing);
        assert_eq!(patch.len(), 3);
        assert_eq!(branch_cached_hash(&patch), expected);
        assert_eq!(patch.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        assert_eq!(snapshot.len(), 4);
        assert_eq!(snapshot.root_hash(), Some(old_hash));
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&snapshot);
        deep_hash_audit(&patch);
    }

    #[test]
    fn promoted_dirty_results_remain_correct_through_later_unions() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[1] = 1;
        let mut c = a;
        c[0] = 1;
        let mut d = c;
        d[1] = 1;
        let mut e = a;
        e[1] = 2;
        let mut f = c;
        f[1] = 2;
        let mut g = a;
        g[1] = 3;

        let mut left = owned_archive_single(a);
        left.union(owned_archive_single(b));
        let mut right = owned_archive_single(c);
        right.union(owned_archive_single(d));
        let mut whole = left.clone();
        whole.union(right.clone());

        // Intersect and difference each collapse the two-slot outer root and
        // promote one still-dirty Branch child to the PATCH root.
        let intersection = whole.intersect(&left);
        let difference = whole.difference(&left);
        assert_eq!(branch_cached_hash(&intersection), 0);
        assert_eq!(branch_cached_hash(&difference), 0);
        deep_hash_audit(&whole);
        deep_hash_audit(&intersection);
        deep_hash_audit(&difference);

        // Remove collapses a dirty Branch all the way to one LocalLeaf.
        let mut removed = left;
        removed.remove(&a);
        assert_eq!(removed.root.as_ref().unwrap().tag(), HeadTag::LocalLeaf);

        for (mut promoted, extra, expected) in [
            (intersection, e, HashSet::from([a, b, e])),
            (difference, f, HashSet::from([c, d, f])),
            (removed, g, HashSet::from([b, g])),
        ] {
            reset_local_leaf_hash_calls();
            promoted.union(owned_archive_single(extra));
            assert_eq!(
                local_leaf_hash_calls(),
                0,
                "a later disjoint union crossed a promoted dirty boundary",
            );
            assert_eq!(promoted.iter().copied().collect::<HashSet<_>>(), expected);
            deep_hash_audit(&promoted);
            let oracle = heap_hash_oracle(&promoted);
            assert_eq!(promoted.root_hash(), Some(oracle));
        }
    }

    #[test]
    fn dirty_intersection_and_difference_use_exact_keys_without_hashing() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;
        let mut c = a;
        c[0] = 2;

        let mut left = owned_archive_single(a);
        left.union(owned_archive_single(b));
        let mut right = owned_archive_single(b);
        right.union(owned_archive_single(c));
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(branch_cached_hash(&right), 0);

        reset_local_leaf_hash_calls();
        let intersection = left.intersect(&right);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(intersection.iter().copied().collect::<Vec<_>>(), vec![b]);
        deep_hash_audit(&intersection);

        reset_local_leaf_hash_calls();
        let difference = left.difference(&right);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(difference.iter().copied().collect::<Vec<_>>(), vec![a]);
        deep_hash_audit(&difference);
    }

    #[test]
    fn duplicate_dirty_local_roots_do_not_materialize_discarded_overlap() {
        const KEY_LEN: usize = 8;
        let key = [7u8; KEY_LEN];
        let mut left = owned_archive_single(key);
        let right = owned_archive_single(key);

        reset_local_leaf_hash_calls();
        left.union(right);
        assert_eq!(left.len(), 1);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(left.root.as_ref().unwrap().tag(), HeadTag::LocalLeaf);

        let expected = heap_hash_oracle(&left);
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 1);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn union_boundary_donates_an_exact_root_over_dirty_children() {
        let mut key = [0u8; 16];
        key[0] = 0;
        key[1] = 0;

        let mut target = owned_archive_dirty_parent(0, 2, 0, 2);
        let expected = branch_cached_hash(&target);
        assert_ne!(expected, 0);
        assert!(direct_dirty_branch_children(&target) > 0);
        let snapshot = target.clone();

        reset_local_leaf_hash_calls();
        target.union(owned_archive_single(key));
        assert_eq!(target.len(), 4);
        assert_eq!(branch_cached_hash(&target), expected);
        assert_eq!(target.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(snapshot.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        // The proof is symmetric in the operands: when the resident superset
        // is consumed on the right, its hash can still be donated to the
        // result after the structural union chooses or mutates either root.
        let superset = owned_archive_dirty_parent(0, 2, 0, 2);
        let superset_hash = branch_cached_hash(&superset);
        let mut subset = owned_archive_single(key);
        reset_local_leaf_hash_calls();
        subset.union(superset);
        assert_eq!(subset.len(), 4);
        assert_eq!(branch_cached_hash(&subset), superset_hash);
        assert_eq!(subset.root_hash(), Some(superset_hash));
        assert_eq!(local_leaf_hash_calls(), 0);

        deep_hash_audit(&snapshot);
        deep_hash_audit(&target);
        deep_hash_audit(&subset);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn subset_operation_boundaries_donate_exact_operand_roots() {
        let left = owned_archive_dirty_parent(0, 2, 0, 2);
        let superset = owned_archive_dirty_parent(0, 2, 0, 4);
        let disjoint = owned_archive_dirty_parent(0, 2, 2, 2);
        let expected = branch_cached_hash(&left);
        assert_ne!(expected, 0);
        assert!(direct_dirty_branch_children(&left) > 0);

        reset_local_leaf_hash_calls();
        let intersection = left.intersect(&superset);
        assert_eq!(intersection.len(), left.len());
        assert_eq!(branch_cached_hash(&intersection), expected);
        assert_eq!(intersection.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        reset_local_leaf_hash_calls();
        let difference = left.difference(&disjoint);
        assert_eq!(difference.len(), left.len());
        assert_eq!(branch_cached_hash(&difference), expected);
        assert_eq!(difference.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        // A proper contained subtraction is the other exact cardinality case:
        // `|A ∖ B| + |B| = |A|` proves B is wholly inside A.
        let containing = owned_archive_dirty_parent(0, 2, 0, 4);
        let contained = owned_archive_dirty_parent(0, 2, 2, 2);
        let contained_expected = branch_cached_hash(&containing) ^ branch_cached_hash(&contained);
        reset_local_leaf_hash_calls();
        let remainder = containing.difference(&contained);
        assert_eq!(remainder.len(), 4);
        assert_eq!(branch_cached_hash(&remainder), contained_expected);
        assert_eq!(remainder.root_hash(), Some(contained_expected));
        assert_eq!(local_leaf_hash_calls(), 0);

        // Borrowed operands retain their own exact roots and dirty children.
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 0);
        deep_hash_audit(&left);
        deep_hash_audit(&intersection);
        deep_hash_audit(&difference);
    }

    #[test]
    fn dirty_partial_overlap_is_deferred_until_a_fingerprint_is_requested() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;
        let mut c = a;
        c[0] = 2;

        let mut left = owned_archive_single(a);
        left.union(owned_archive_single(b));
        let mut right = owned_archive_single(b);
        right.union(owned_archive_single(c));
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(branch_cached_hash(&right), 0);

        reset_local_leaf_hash_calls();
        left.union(right);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(
            left.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([a, b, c]),
        );
        deep_hash_audit(&left);

        let expected = heap_hash_oracle(&left);
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 3);
        assert_eq!(branch_cached_hash(&left), expected);
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 3);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_union_threshold_preserves_dirty_descendants_without_hashing() {
        // Each input has exactly 4,096 rows: the inclusive threshold boundary.
        // Its root hash is resident, but every direct child is a dirty Branch
        // over 32 archive-backed leaves. The two first-byte ranges are
        // disjoint, so the result hash is derivable from the two resident
        // input hashes without touching a LocalLeaf.
        let mut left = owned_archive_dirty_parent(0, 128, 0, 32);
        let right = owned_archive_dirty_parent(128, 128, 0, 32);
        assert_eq!(left.len(), PARALLEL_PATCH_UNION_THRESHOLD as u64);
        assert_eq!(right.len(), PARALLEL_PATCH_UNION_THRESHOLD as u64);
        assert_ne!(branch_cached_hash(&left), 0);
        assert_ne!(branch_cached_hash(&right), 0);
        assert_eq!(direct_dirty_branch_children(&left), 128);
        assert_eq!(direct_dirty_branch_children(&right), 128);

        let expected_hash = left.root_hash().unwrap() ^ right.root_hash().unwrap();
        reset_local_leaf_hash_calls();
        left.union(right);

        assert_eq!(left.len(), 8_192);
        assert_eq!(direct_dirty_branch_children(&left), 256);
        assert_eq!(branch_cached_hash(&left), expected_hash);
        assert_eq!(
            local_leaf_hash_calls(),
            0,
            "the parallel bulk finalizer must not hash disjoint dirty children",
        );

        // Immediate verification consumes the algebraically derived root
        // cache rather than descending into all 8,192 LocalLeaves.
        let before = local_leaf_hash_calls();
        assert_eq!(left.root_hash(), Some(expected_hash));
        assert_eq!(local_leaf_hash_calls(), before);
        deep_hash_audit(&left);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_union_defers_overlapping_hash_work_until_requested() {
        // The two 4,096-row inputs share exactly variant 31: one leaf in each
        // of 128 root buckets. Their direct child hashes are deliberately
        // dirty, so structural descent must identify those 128 equal keys.
        let left = owned_archive_dirty_parent(0, 128, 0, 32);
        let right = owned_archive_dirty_parent(0, 128, 31, 32);
        assert_ne!(branch_cached_hash(&left), 0);
        assert_ne!(branch_cached_hash(&right), 0);
        assert_eq!(direct_dirty_branch_children(&left), 128);
        assert_eq!(direct_dirty_branch_children(&right), 128);

        // Exhausting the spawn budget keeps structural work on this thread.
        // Composition still does not hash the 128 duplicates merely to keep a
        // new cache warm.
        reset_local_leaf_hash_calls();
        let serial_scatter = union_with_exhausted_parallel_budget(left.clone(), right.clone());
        assert_eq!(serial_scatter.len(), 8_064);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&serial_scatter), 0);
        let serial_oracle = heap_hash_oracle(&serial_scatter);
        assert_eq!(serial_scatter.root_hash(), Some(serial_oracle));
        assert_eq!(local_leaf_hash_calls(), 8_064);
        assert_eq!(branch_cached_hash(&serial_scatter), serial_oracle);
        assert_eq!(serial_scatter.root_hash(), Some(serial_oracle));
        assert_eq!(local_leaf_hash_calls(), 8_064);

        // The ordinary context spends its budget on rayon tasks. It has no
        // scalar fingerprint sidecar; validate its structure independently.
        let mut parallel = left;
        reset_local_leaf_hash_calls();
        parallel.union(right);
        assert_eq!(parallel.len(), 8_064);
        assert_eq!(branch_cached_hash(&parallel), 0);
        let parallel_oracle = heap_hash_oracle(&parallel);
        assert_eq!(parallel.root_hash(), Some(parallel_oracle));
        assert_eq!(branch_cached_hash(&parallel), parallel_oracle);
        deep_hash_audit(&parallel);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_difference_defers_hashing_a_large_partial_archive_result() {
        // Rebuild a genuine 128-child result: every bucket loses its upper 16
        // variants, so no unchanged/empty/unary shortcut can decide the root.
        let left = owned_archive_dirty_parent(0, 128, 0, 32);
        let right = owned_archive_dirty_parent(0, 128, 16, 16);
        assert_eq!(left.len(), PARALLEL_PATCH_UNION_THRESHOLD as u64);
        assert_eq!(right.len(), 2_048);
        assert_ne!(branch_cached_hash(&left), 0);
        assert_eq!(direct_dirty_branch_children(&left), 128);

        reset_local_leaf_hash_calls();
        let difference = difference_with_exhausted_parallel_budget(&left, &right);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(difference.len(), 2_048);
        assert_eq!(branch_cached_hash(&difference), 0);
        assert!(difference.shares_owner_guard(&left));

        // The result guard remains sufficient after both source values drop;
        // exact iteration is structural and consumes no fingerprint.
        drop(left);
        drop(right);
        let actual = difference.iter().copied().collect::<HashSet<_>>();
        let expected = (0u8..128)
            .flat_map(|bucket| {
                (0u8..16).map(move |variant| {
                    let mut key = [0u8; 16];
                    key[0] = bucket;
                    key[1] = variant;
                    key
                })
            })
            .collect::<HashSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(local_leaf_hash_calls(), 0);

        // The first actual consumer pays exactly for the surviving frontier
        // and memoizes it; neither removed rows nor later consumers are hashed.
        let expected_hash = heap_hash_oracle(&difference);
        let before = local_leaf_hash_calls();
        assert_eq!(difference.root_hash(), Some(expected_hash));
        assert_eq!(local_leaf_hash_calls() - before, 2_048);
        assert_eq!(branch_cached_hash(&difference), expected_hash);
        let before = local_leaf_hash_calls();
        assert_eq!(difference.root_hash(), Some(expected_hash));
        assert_eq!(local_leaf_hash_calls() - before, 0);
        deep_hash_audit(&difference);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_union_stays_hash_free_below_dirty_roots() {
        let mut left = owned_archive_dirty_parent(0, 128, 0, 32);
        let mut right = owned_archive_dirty_parent(0, 128, 31, 32);
        assert_ne!(branch_cached_hash(&left), 0);
        assert_ne!(branch_cached_hash(&right), 0);
        demote_root_hash(&mut left);
        demote_root_hash(&mut right);
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(branch_cached_hash(&right), 0);
        assert_eq!(direct_dirty_branch_children(&left), 128);
        assert_eq!(direct_dirty_branch_children(&right), 128);
        let mut spawned_left = left.clone();
        let spawned_right = right.clone();

        // The same 128 duplicate LocalLeaves as the resident-root fixture are
        // discovered structurally. Exhaust the spawn budget for an exact TLS
        // census.
        reset_local_leaf_hash_calls();
        let union = union_with_exhausted_parallel_budget(left, right);
        assert_eq!(union.len(), 8_064);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&union), 0);
        deep_hash_audit(&union);

        let expected = heap_hash_oracle(&union);
        assert_eq!(union.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 8_064);
        assert_eq!(branch_cached_hash(&union), expected);

        // Exercise the ordinary spawned-task path as well. Its worker-local
        // census is intentionally not asserted; structure plus the immediate
        // fingerprint oracle cover the fingerprint-free scatter path.
        reset_local_leaf_hash_calls();
        spawned_left.union(spawned_right);
        assert_eq!(spawned_left.len(), 8_064);
        assert_eq!(branch_cached_hash(&spawned_left), 0);
        let spawned_expected = heap_hash_oracle(&spawned_left);
        assert_eq!(spawned_left.root_hash(), Some(spawned_expected));
        assert_eq!(branch_cached_hash(&spawned_left), spawned_expected);
        deep_hash_audit(&spawned_left);
    }

    #[test]
    fn singleton_overlap_reuses_the_left_leaf_without_hashing() {
        const KEY_LEN: usize = 2;
        let key = [7, 9];
        let mut local = owned_archive_single(key);
        let mut heap = PATCH::<KEY_LEN>::new();
        heap.insert(&Entry::new(&key));

        reset_local_leaf_hash_calls();
        local.union(heap);

        assert_eq!(local.len(), 1);
        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(local.root.as_ref().unwrap().tag(), HeadTag::LocalLeaf);
    }

    #[test]
    fn overlapping_cached_roots_do_not_make_composition_a_hash_consumer() {
        const KEY_LEN: usize = 8;
        let a = [0u8; KEY_LEN];
        let mut b = a;
        b[0] = 1;
        let mut c = a;
        c[0] = 2;

        let mut left = owned_archive_pair([a, b]);
        let right = owned_archive_pair([b, c]);
        assert_ne!(branch_cached_hash(&left), 0);
        assert_ne!(branch_cached_hash(&right), 0);

        reset_local_leaf_hash_calls();
        left.union(right);
        assert_eq!(
            local_leaf_hash_calls(),
            0,
            "structural union must not hash an equal LocalLeaf merely to repair a cache",
        );
        assert_eq!(branch_cached_hash(&left), 0);
        assert_eq!(
            left.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([a, b, c])
        );
        deep_hash_audit(&left);

        let expected = heap_hash_oracle(&left);
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(
            local_leaf_hash_calls(),
            3,
            "the first fingerprint consumer hashes the three result leaves once",
        );
        assert_eq!(branch_cached_hash(&left), expected);
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls(), 3);
    }

    fn owner_cover_in_order(owners: &[Arc<dyn ArchiveOwner>], order: &[usize]) -> Arc<OwnerCover> {
        let mut cover = None;
        for &index in order {
            OwnerCover::retain(&mut cover, &owners[index]);
        }
        cover.expect("test owner cover must not be empty")
    }

    /// Construct a cover over synthetic address keys. Production leaves always
    /// use their Arc allocation address; this fixture lets tests exercise the
    /// otherwise-unreachable high half of the unsigned address space.
    fn keyed_owner_cover(
        order: &[usize],
        owners: &BTreeMap<usize, Arc<dyn ArchiveOwner>>,
    ) -> Option<Arc<OwnerCover>> {
        let mut cover: Option<Arc<OwnerCover>> = None;
        for &address in order {
            let owner = owners.get(&address).expect("missing synthetic owner");
            let Some(existing) = cover.as_mut() else {
                cover = Some(Arc::new(OwnerCover {
                    latest_address: address,
                    len: 1,
                    root: Arc::new(OwnerNode::Owner {
                        address,
                        owner: owner.clone(),
                    }),
                }));
                continue;
            };
            let (root, inserted) = OwnerNode::insert(&existing.root, address, owner);
            let existing = Arc::make_mut(existing);
            existing.root = root;
            existing.len += usize::from(inserted);
            existing.latest_address = address;
        }
        cover
    }

    fn owner_cover_keys(cover: &OwnerCover) -> BTreeSet<usize> {
        let mut keys = BTreeSet::new();
        cover
            .root
            .for_each_owner(&mut |address, _| assert!(keys.insert(address)));
        keys
    }

    #[test]
    fn owner_cover_repeated_latest_owner_is_an_identity() {
        let owner = test_archive_owner(1);
        let mut cover = None;
        OwnerCover::retain(&mut cover, &owner);
        let first = cover.as_ref().unwrap().clone();

        for _ in 0..4096 {
            OwnerCover::retain(&mut cover, &owner);
        }

        let cover = cover.unwrap();
        assert!(Arc::ptr_eq(&cover, &first));
        let stats = cover.stats();
        assert_eq!(stats.owners, 1);
        assert_eq!(stats.branches, 0);
        assert_eq!(stats.max_depth, 0);
        assert_eq!(cover.len, 1);
    }

    #[test]
    fn owner_cover_shape_is_insertion_order_independent_and_exact() {
        let owners: Vec<_> = (0..32).map(test_archive_owner).collect();
        let forward: Vec<_> = (0..owners.len()).collect();
        let reverse: Vec<_> = forward.iter().rev().copied().collect();
        let interleaved: Vec<_> = (0..owners.len())
            .step_by(2)
            .chain((1..owners.len()).step_by(2))
            .collect();

        let first = owner_cover_in_order(&owners, &forward);
        let second = owner_cover_in_order(&owners, &reverse);
        let third = owner_cover_in_order(&owners, &interleaved);

        assert!(first.root.same_shape(&second.root));
        assert!(first.root.same_shape(&third.root));
        for owner in &owners {
            assert!(OwnerNode::contains(&first.root, OwnerCover::address(owner)));
        }
        let unrelated = test_archive_owner(255);
        assert!(!OwnerNode::contains(
            &first.root,
            OwnerCover::address(&unrelated)
        ));

        let stats = first.stats();
        assert_eq!(stats.owners, owners.len());
        assert_eq!(stats.branches, owners.len() - 1);
        assert!(stats.max_depth <= usize::BITS as usize);
    }

    #[test]
    fn owner_cover_masks_are_unsigned_across_the_top_bit() {
        let top = 1usize << (usize::BITS - 1);
        let keys = [0, 1, top, top | 1, usize::MAX];
        let owners: BTreeMap<_, Arc<dyn ArchiveOwner>> = keys
            .into_iter()
            .map(|key| (key, Arc::new(key) as Arc<dyn ArchiveOwner>))
            .collect();
        let reverse = [usize::MAX, top | 1, top, 1, 0];
        let first = keyed_owner_cover(&keys, &owners).unwrap();
        let second = keyed_owner_cover(&reverse, &owners).unwrap();

        assert_eq!(OwnerNode::critical_mask(0, top), top);
        assert_eq!(OwnerNode::critical_mask(0, usize::MAX), top);
        assert!(first.root.same_shape(&second.root));
        assert_eq!(owner_cover_keys(&first), BTreeSet::from(keys));
        assert!(matches!(
            first.root.as_ref(),
            OwnerNode::Branch { mask, .. } if *mask == top
        ));
        let stats = first.stats();
        assert_eq!(stats.owners, keys.len());
        assert_eq!(stats.branches, keys.len() - 1);
        assert!(stats.max_depth <= usize::BITS as usize);
    }

    #[test]
    fn owner_cover_union_of_equal_sets_reuses_the_left_shape() {
        let owner = test_archive_owner(1);
        let mut left = None;
        let mut right = None;
        OwnerCover::retain(&mut left, &owner);
        OwnerCover::retain(&mut right, &owner);
        let left_snapshot = left.as_ref().unwrap().clone();

        let singleton_union = OwnerCover::union(left, &right).unwrap();
        assert!(Arc::ptr_eq(&singleton_union, &left_snapshot));

        let identical_union = OwnerCover::union(
            Some(singleton_union.clone()),
            &Some(singleton_union.clone()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&identical_union, &singleton_union));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn owner_cover_join_exactly_proves_both_inputs() {
        let owners: Vec<_> = (0..19).map(test_archive_owner).collect();
        let left_owners = &owners[..13];
        let right_owners = &owners[6..];
        let mut left = None;
        let mut right = None;
        for owner in left_owners {
            OwnerCover::retain(&mut left, owner);
        }
        for owner in right_owners {
            OwnerCover::retain(&mut right, owner);
        }
        let left_snapshot = left.clone().unwrap();
        let right_snapshot = right.clone().unwrap();

        let joined = OwnerCover::union(left, &right).unwrap();

        assert!(joined.covers(&left_snapshot));
        assert!(joined.covers(&right_snapshot));
        assert_eq!(joined.len, owners.len());

        let unrelated = OwnerCover::singleton(&test_archive_owner(255));
        assert!(!joined.covers(&unrelated));
    }

    #[test]
    fn owner_cover_overlapping_diamond_stays_two_owners_and_one_branch() {
        let owners = [test_archive_owner(1), test_archive_owner(2)];
        let mut a = Some(owner_cover_in_order(&owners, &[0, 1]));
        let mut b = Some(owner_cover_in_order(&owners, &[1, 0]));
        let a_root = a.as_ref().unwrap().root.clone();
        let b_root = b.as_ref().unwrap().root.clone();
        assert!(!Arc::ptr_eq(&a_root, &b_root));
        assert!(a_root.same_shape(&b_root));

        // A_{k+1} = A_k ∪ B_k and B_{k+1} = B_k ∪ A_k used to grow
        // the non-exact provenance forest even when independently materialized
        // inputs already represented the same logical set. Exact Patricia
        // membership turns every later join into a node identity: only the
        // directional latest-owner field may change.
        for _ in 0..4096 {
            let next_a = OwnerCover::union(a.clone(), &b);
            let next_b = OwnerCover::union(b.clone(), &a);
            a = next_a;
            b = next_b;

            for cover in [a.as_ref().unwrap(), b.as_ref().unwrap()] {
                let stats = cover.stats();
                assert_eq!(cover.len, 2);
                assert_eq!(stats.owners, 2);
                assert_eq!(stats.branches, 1);
                assert_eq!(stats.max_depth, 1);
            }
            assert!(Arc::ptr_eq(&a.as_ref().unwrap().root, &a_root));
            assert!(Arc::ptr_eq(&b.as_ref().unwrap().root, &b_root));
        }
    }

    #[test]
    fn owner_cover_snapshots_share_nodes_and_retain_exact_lifetimes() {
        struct CountedOwner(Arc<AtomicUsize>);

        impl Drop for CountedOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let first: Arc<dyn ArchiveOwner> = Arc::new(CountedOwner(drops.clone()));
        let second: Arc<dyn ArchiveOwner> = Arc::new(CountedOwner(drops.clone()));
        let third: Arc<dyn ArchiveOwner> = Arc::new(CountedOwner(drops.clone()));
        let mut cover = None;
        OwnerCover::retain(&mut cover, &first);
        OwnerCover::retain(&mut cover, &second);
        let snapshot = cover.clone();
        OwnerCover::retain(&mut cover, &third);

        let snapshot = snapshot.unwrap();
        let cover = cover.unwrap();
        for owner in [&first, &second] {
            let address = OwnerCover::address(owner);
            let old_leaf = OwnerNode::leaf(&snapshot.root, address).unwrap();
            let new_leaf = OwnerNode::leaf(&cover.root, address).unwrap();
            assert!(Arc::ptr_eq(old_leaf, new_leaf));
        }

        drop(first);
        drop(second);
        drop(third);
        drop(cover);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(snapshot);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    proptest! {
        #[test]
        fn owner_cover_union_matches_btree_set(
            left in prop::collection::btree_set(any::<usize>(), 0..48),
            right in prop::collection::btree_set(any::<usize>(), 0..48),
        ) {
            let expected: BTreeSet<_> = left.union(&right).copied().collect();
            let owners: BTreeMap<_, Arc<dyn ArchiveOwner>> = expected
                .iter()
                .copied()
                .map(|key| (key, Arc::new(key) as Arc<dyn ArchiveOwner>))
                .collect();
            let left_order: Vec<_> = left.iter().copied().collect();
            let right_order: Vec<_> = right.iter().rev().copied().collect();
            let canonical_order: Vec<_> = expected.iter().copied().collect();
            let left_cover = keyed_owner_cover(&left_order, &owners);
            let right_cover = keyed_owner_cover(&right_order, &owners);
            let joined = OwnerCover::union(left_cover, &right_cover);

            if expected.is_empty() {
                prop_assert!(joined.is_none());
            } else {
                let joined = joined.unwrap();
                let canonical = keyed_owner_cover(&canonical_order, &owners).unwrap();
                prop_assert_eq!(owner_cover_keys(&joined), expected);
                prop_assert!(joined.root.same_shape(&canonical.root));
                let stats = joined.stats();
                prop_assert_eq!(stats.owners, joined.len);
                prop_assert_eq!(stats.branches + 1, joined.len);
                prop_assert!(stats.max_depth <= usize::BITS as usize);
            }
        }
    }

    #[test]
    fn exact_owner_union_keeps_disjoint_dirty_local_leaves_alive_without_hashing() {
        const KEY_LEN: usize = 8;
        let left_key = [0u8; KEY_LEN];
        let mut right_key = left_key;
        right_key[0] = 1;

        // Each fixture has already dropped every source Arc: its PATCH guard
        // is the sole remaining lifetime witness for the LocalLeaf bytes.
        let mut left = owned_archive_single(left_key);
        let right = owned_archive_single(right_key);
        assert_eq!(left.owners.as_ref().unwrap().owner_count(), 1);
        assert_eq!(right.owners.as_ref().unwrap().owner_count(), 1);

        let expected = heap_hash_oracle(&left) ^ heap_hash_oracle(&right);
        reset_local_leaf_hash_calls();
        left.union(right);

        assert_eq!(local_leaf_hash_calls(), 0);
        assert_eq!(branch_cached_hash(&left), 0);
        let cover = left.owners.as_ref().expect("union must retain both owners");
        let stats = cover.stats();
        assert_eq!(cover.owner_count(), 2);
        assert_eq!(stats.owners, 2);
        assert_eq!(stats.branches, 1);
        assert_eq!(stats.max_depth, 1);
        assert_eq!(
            left.iter().copied().collect::<HashSet<_>>(),
            HashSet::from([left_key, right_key]),
        );
        deep_hash_audit(&left);

        // Deferred verification still dereferences both rows safely after the
        // consumed source lineage and its standalone owner receipt are gone.
        let before = local_leaf_hash_calls();
        assert_eq!(left.root_hash(), Some(expected));
        assert_eq!(local_leaf_hash_calls() - before, 2);
        assert_eq!(branch_cached_hash(&left), expected);
    }

    #[test]
    fn exact_owner_diamond_stays_bounded_around_dirty_branch_heads() {
        const KEY_LEN: usize = 8;
        let left_key = [0u8; KEY_LEN];
        let mut right_key = left_key;
        right_key[0] = 1;

        let left_storage = Arc::new(AlignedArchiveKey(left_key));
        let right_storage = Arc::new(AlignedArchiveKey(right_key));
        let left_owner: Arc<dyn ArchiveOwner> = left_storage.clone();
        let right_owner: Arc<dyn ArchiveOwner> = right_storage.clone();
        let (mut first, mut second) = {
            let left_entry =
                unsafe { ArchiveEntry::new(NonNull::from(&left_storage.0), &left_owner) };
            let right_entry =
                unsafe { ArchiveEntry::new(NonNull::from(&right_storage.0), &right_owner) };

            let mut first = PATCH::<KEY_LEN, IdentitySchema>::new();
            first.insert_archive(&left_entry);
            first.insert_archive(&right_entry);

            let mut second = PATCH::<KEY_LEN, IdentitySchema>::new();
            second.insert_archive(&right_entry);
            second.insert_archive(&left_entry);
            (first, second)
        };

        assert_eq!(branch_cached_hash(&first), 0);
        assert_eq!(branch_cached_hash(&second), 0);
        let first_owner_root = Arc::as_ptr(&first.owners.as_ref().unwrap().root) as usize;
        let second_owner_root = Arc::as_ptr(&second.owners.as_ref().unwrap().root) as usize;
        assert_ne!(first_owner_root, second_owner_root);
        let expected = heap_hash_oracle(&first);

        // Leave each PATCH receipt as the only owner of its archive bytes.
        drop(left_owner);
        drop(right_owner);
        drop(left_storage);
        drop(right_storage);

        // Exercise the PATCH-level diamond, not just OwnerCover directly.
        // Each fingerprint consumer memoizes the Branch; clear that cache
        // after checking it so every iteration still stresses the structural
        // dirty-Branch path and opposite owner joins.
        for _ in 0..256 {
            let mut next_first = first.clone();
            let mut next_second = second.clone();
            next_first.union(second.clone());
            next_second.union(first.clone());
            first = next_first;
            second = next_second;

            for (patch, owner_root) in [(&first, first_owner_root), (&second, second_owner_root)] {
                let cover = patch.owners.as_ref().expect("dirty Head lost its owners");
                let stats = cover.stats();
                assert_eq!(cover.owner_count(), 2);
                assert_eq!(stats.owners, 2);
                assert_eq!(stats.branches, 1);
                assert_eq!(stats.max_depth, 1);
                assert_eq!(Arc::as_ptr(&cover.root) as usize, owner_root);
                deep_hash_audit(patch);
                assert_eq!(patch.root_hash(), Some(expected));
                assert_eq!(branch_cached_hash(patch), expected);
            }
            demote_root_hash(&mut first);
            demote_root_hash(&mut second);
        }
    }

    #[test]
    fn archive_singleton_is_a_guarded_root_local_leaf() {
        const KEY_SIZE: usize = 8;
        let key = [0x5au8; KEY_SIZE];
        let singleton = owned_archive_single(key);

        assert_eq!(singleton.node_stats(), (0, 0, 0, 1));
        assert_eq!(singleton.owners.as_ref().unwrap().owner_count(), 1);
        assert_eq!(singleton.root.as_ref().unwrap().tag(), HeadTag::LocalLeaf);

        // The fixture owner was dropped before `owned_archive_single`
        // returned, leaving the root guard as the only lifetime witness.
        let noise = vec![0xa5u8; KEY_SIZE * 64];
        std::hint::black_box(&noise);
        assert_eq!(singleton.iter().copied().collect_vec(), vec![key]);

        let mut emptied = singleton.clone();
        emptied.remove(&key);
        assert!(emptied.root.is_none());
        assert!(
            emptied.owners.is_none(),
            "an empty PATCH must release its owners"
        );
    }

    #[test]
    fn union_reconciles_independent_archive_owners() {
        fn decoded_archive(owner: u8) -> TribleSet {
            let mut source = TribleSet::new();
            for attribute in [1u8, 2] {
                let mut data = [0u8; 64];
                data[0] = 1;
                data[15] = owner;
                data[16] = attribute;
                data[32] = 1;
                data[47] = owner;
                data[63] = attribute;
                source.insert(&Trible::force_raw(data).unwrap());
            }
            let blob: Blob<SimpleArchive> = SimpleArchive::encode(&source);
            drop(source);
            TribleSet::try_from_blob(blob).unwrap()
        }

        // Two entries are enough for each decoder to retain its own archive
        // allocation behind LocalLeaves. Keeping the attribute bytes equal
        // makes AEV/AVE union below the attribute prefix, exercising owner-cover
        // propagation independently of the resulting trie shape.
        let left = decoded_archive(1);
        let right = decoded_archive(2);
        assert!(left.aev.node_stats().3 > 0);
        assert!(right.aev.node_stats().3 > 0);

        let unioned = left + right;
        assert_eq!(unioned.len(), 4);
        assert_eq!(unioned.eav.iter_ordered().count(), 4);

        for stats in [
            unioned.eav.node_stats(),
            unioned.eva.node_stats(),
            unioned.aev.node_stats(),
            unioned.ave.node_stats(),
            unioned.vea.node_stats(),
            unioned.vae.node_stats(),
        ] {
            assert_eq!(stats.2, 0, "cross-owner union materialized a heap Leaf");
            assert_eq!(stats.3, 4, "cross-owner union lost a LocalLeaf");
        }

        assert_eq!(patch_unowned_direct_local_leaves(&unioned.eav), 0);
        assert_eq!(patch_unowned_direct_local_leaves(&unioned.eva), 0);
        assert_eq!(patch_unowned_direct_local_leaves(&unioned.aev), 0);
        assert_eq!(patch_unowned_direct_local_leaves(&unioned.ave), 0);
        assert_eq!(patch_unowned_direct_local_leaves(&unioned.vea), 0);
        assert_eq!(patch_unowned_direct_local_leaves(&unioned.vae), 0);
    }

    #[test]
    fn archive_batch_uses_irreducible_zero_one_two_layouts() {
        #[repr(C, align(16))]
        struct AlignedTrible([u8; 64]);

        fn raw(i: u8) -> AlignedTrible {
            let mut data = [0u8; 64];
            data[0] = 1;
            data[15] = i;
            data[16] = 2;
            data[31] = i;
            data[32] = 3;
            data[63] = i;
            AlignedTrible(data)
        }

        fn assert_layout(set: &TribleSet, heap: u64, local: u64) {
            fn one<O: KeySchema<64>>(patch: &PATCH<64, O>, heap: u64, local: u64) {
                let stats = patch.node_stats();
                assert_eq!(stats.2, heap, "unexpected heap-leaf count: {stats:?}");
                assert_eq!(stats.3, local, "unexpected LocalLeaf count: {stats:?}");
                assert_eq!(patch_unowned_direct_local_leaves(patch), 0);
            }
            one(&set.eav, heap, local);
            one(&set.eva, heap, local);
            one(&set.aev, heap, local);
            one(&set.ave, heap, local);
            one(&set.vea, heap, local);
            one(&set.vae, heap, local);
        }

        let storage = std::sync::Arc::new([raw(1), raw(2), raw(3)]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();

        // Construct an empty TribleSet before ArchiveEntry creation so the
        // process-local PATCH hash key is initialized.
        let mut empty = TribleSet::new();
        let entries: [ArchiveEntry<'_, 64>; 3] = std::array::from_fn(|i| unsafe {
            ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
        });
        empty.insert_archive_batch(&entries[..0]);
        assert_layout(&empty, 0, 0);

        let mut one = TribleSet::new();
        one.insert_archive_batch(&entries[..1]);
        assert_layout(&one, 0, 1);
        let single_ptrs = [
            one.eav.iter().next().unwrap().as_ptr(),
            one.eva.iter().next().unwrap().as_ptr(),
            one.aev.iter().next().unwrap().as_ptr(),
            one.ave.iter().next().unwrap().as_ptr(),
            one.vea.iter().next().unwrap().as_ptr(),
            one.vae.iter().next().unwrap().as_ptr(),
        ];
        assert!(single_ptrs.iter().all(|ptr| *ptr == single_ptrs[0]));

        let mut two = TribleSet::new();
        two.insert_archive_batch(&entries[..2]);
        assert_layout(&two, 0, 2);
        fn assert_pair<O: KeySchema<64>>(patch: &PATCH<64, O>) {
            assert_eq!(patch.node_stats(), (1, 2, 0, 2));
        }
        assert_pair(&two.eav);
        assert_pair(&two.eva);
        assert_pair(&two.aev);
        assert_pair(&two.ave);
        assert_pair(&two.vea);
        assert_pair(&two.vae);

        let mut three = TribleSet::new();
        three.insert_archive_batch(&entries);
        assert_layout(&three, 0, 3);
        let surviving_clone = three.clone();
        drop(three);
        drop(two);
        drop(one);
        drop(empty);
        drop(owner);
        drop(storage);

        // The clone's PATCH owner covers are now the only things retaining the
        // archive rows. Force allocation churn, then dereference every index.
        let noise = vec![0xa5u8; 3 * 64 * 32];
        std::hint::black_box(&noise);
        fn assert_three<O: KeySchema<64>>(patch: &PATCH<64, O>) {
            assert_eq!(patch.iter_ordered().count(), 3);
        }
        assert_three(&surviving_clone.eav);
        assert_three(&surviving_clone.eva);
        assert_three(&surviving_clone.aev);
        assert_three(&surviving_clone.ave);
        assert_three(&surviving_clone.vea);
        assert_three(&surviving_clone.vae);
    }

    #[test]
    fn archive_entries_initialize_hashing_before_batch_construction() {
        #[repr(C, align(16))]
        struct AlignedTrible([u8; 64]);

        let mut first_raw = [0u8; 64];
        first_raw[0] = 1;
        first_raw[16] = 2;
        first_raw[32] = 3;
        let mut second_raw = first_raw;
        second_raw[63] = 4;
        let storage = std::sync::Arc::new([AlignedTrible(first_raw), AlignedTrible(second_raw)]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();

        // Deliberately create entries before constructing any PATCH. Their
        // cached hashes must initialize and share PATCH's process-local key.
        let entries: [ArchiveEntry<'_, 64>; 2] = std::array::from_fn(|i| unsafe {
            ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
        });
        let mut archive_set = TribleSet::new();
        archive_set.insert_archive_batch(&entries);

        let mut heap_set = TribleSet::new();
        heap_set.insert(&Trible::force_raw(first_raw).unwrap());
        heap_set.insert(&Trible::force_raw(second_raw).unwrap());
        assert_eq!(archive_set, heap_set);
        assert_eq!(archive_set.eav.root_hash(), heap_set.eav.root_hash());
        assert_eq!(archive_set.eva.root_hash(), heap_set.eva.root_hash());
        assert_eq!(archive_set.aev.root_hash(), heap_set.aev.root_hash());
        assert_eq!(archive_set.ave.root_hash(), heap_set.ave.root_hash());
        assert_eq!(archive_set.vea.root_hash(), heap_set.vea.root_hash());
        assert_eq!(archive_set.vae.root_hash(), heap_set.vae.root_hash());
    }

    #[test]
    fn patch_owner_guard_preserves_local_leaves_across_set_operations() {
        const KEY_SIZE: usize = 8;
        let a = [0u8; KEY_SIZE];
        let mut b = [0u8; KEY_SIZE];
        b[0] = 1;
        let mut c = [0u8; KEY_SIZE];
        c[1] = 1;

        let archive = owned_archive_pair([a, b]);
        assert_eq!(archive.node_stats(), (1, 2, 0, 2));

        let only_a = owned_archive_single(a);

        // Intersect and difference may collapse a Branch to a root LocalLeaf;
        // each result carries the conservative PATCH owner guard it needs.
        let intersection = archive.intersect(&only_a);
        assert_eq!(intersection.node_stats(), (0, 0, 0, 1));
        assert_eq!(intersection.iter().copied().collect_vec(), vec![a]);

        let difference = archive.difference(&only_a);
        assert_eq!(difference.node_stats(), (0, 0, 0, 1));
        assert_eq!(difference.iter().copied().collect_vec(), vec![b]);
        drop(only_a);

        let mut removed = archive.clone();
        removed.remove(&a);
        assert_eq!(removed.node_stats(), (0, 0, 0, 1));
        assert_eq!(removed.iter().copied().collect_vec(), vec![b]);

        // Ordinary insertion and replacement can freely reshape the trie;
        // untouched archive rows remain local because ownership is not tied
        // to a particular Branch.
        let mut inserted = archive;
        inserted.insert(&Entry::new(&c));
        assert_eq!(inserted.len(), 3);
        assert_eq!(inserted.node_stats().2, 1);
        assert_eq!(inserted.node_stats().3, 2);
        assert_eq!(patch_unowned_direct_local_leaves(&inserted), 0);
        let actual: HashSet<[u8; KEY_SIZE]> = inserted.iter().copied().collect();
        assert_eq!(actual, HashSet::from([a, b, c]));

        let mut replaced = owned_archive_pair([a, b]);
        replaced.replace(&Entry::new(&a));
        assert_eq!(replaced.len(), 2);
        assert_eq!(replaced.node_stats().2, 1);
        assert_eq!(replaced.node_stats().3, 1);
        assert_eq!(patch_unowned_direct_local_leaves(&replaced), 0);
        let actual: HashSet<[u8; KEY_SIZE]> = replaced.iter().copied().collect();
        assert_eq!(actual, HashSet::from([a, b]));

        // `inserted` consumed the original archive PATCH. The derived values
        // still dereference their LocalLeaf roots after that source lineage is
        // gone.
        let noise = vec![0x3cu8; KEY_SIZE * 128];
        std::hint::black_box(&noise);
        assert_eq!(intersection.iter().copied().collect_vec(), vec![a]);
        assert_eq!(difference.iter().copied().collect_vec(), vec![b]);
        assert_eq!(removed.iter().copied().collect_vec(), vec![b]);
    }

    #[test]
    fn archive_insertion_keeps_an_existing_direct_local_leaf_on_duplicates() {
        const KEY_SIZE: usize = 8;
        let a = [0u8; KEY_SIZE];
        let mut b = [0u8; KEY_SIZE];
        b[0] = 1;

        let storage = std::sync::Arc::new([AlignedArchiveKey(a), AlignedArchiveKey(b)]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let entries: [ArchiveEntry<'_, KEY_SIZE>; 2] = std::array::from_fn(|i| unsafe {
            ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
        });
        let mut archive: PATCH<KEY_SIZE, IdentitySchema> =
            PATCH::from_archive_pair(&entries[0], &entries[1]);
        let original_hash = archive.root_hash();
        let snapshot = archive.clone();
        assert!(Arc::ptr_eq(
            archive.owners.as_ref().unwrap(),
            snapshot.owners.as_ref().unwrap(),
        ));

        // Same-owner duplication stays entirely local.
        archive.insert_archive(&entries[0]);
        assert_eq!(archive.node_stats(), (1, 2, 0, 2));
        assert_eq!(archive.root_hash(), original_hash);
        assert!(Arc::ptr_eq(
            archive.owners.as_ref().unwrap(),
            snapshot.owners.as_ref().unwrap(),
        ));

        // A duplicate from another owner extends the conservative root guard
        // without replacing the existing LocalLeaf.
        let duplicate_storage = std::sync::Arc::new(AlignedArchiveKey(a));
        let duplicate_owner: std::sync::Arc<dyn ArchiveOwner> = duplicate_storage.clone();
        let duplicate =
            unsafe { ArchiveEntry::new(NonNull::from(&duplicate_storage.0), &duplicate_owner) };
        archive.insert_archive(&duplicate);
        assert_eq!(archive.node_stats(), (1, 2, 0, 2));
        assert_eq!(archive.root_hash(), original_hash);
        assert_eq!(archive.owners.as_ref().unwrap().owner_count(), 2);
        assert_eq!(snapshot.owners.as_ref().unwrap().owner_count(), 1);
        assert!(!Arc::ptr_eq(
            archive.owners.as_ref().unwrap(),
            snapshot.owners.as_ref().unwrap(),
        ));
        assert_eq!(patch_unowned_direct_local_leaves(&archive), 0);
        let actual: HashSet<[u8; KEY_SIZE]> = archive.iter().copied().collect();
        assert_eq!(actual, HashSet::from([a, b]));
    }

    #[test]
    fn difference_collapses_a_unary_parent_with_a_branch_survivor() {
        const KEY_SIZE: usize = 8;
        let a = [0u8; KEY_SIZE];
        let mut b = [0u8; KEY_SIZE];
        b[1] = 1;
        let mut c = [0u8; KEY_SIZE];
        c[0] = 1;

        let storage = std::sync::Arc::new([
            AlignedArchiveKey(a),
            AlignedArchiveKey(b),
            AlignedArchiveKey(c),
        ]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let archive = {
            let entries: [ArchiveEntry<'_, KEY_SIZE>; 3] = std::array::from_fn(|i| unsafe {
                ArchiveEntry::new(NonNull::from(&storage[i].0), &owner)
            });
            let mut patch = PATCH::from_archive_pair(&entries[0], &entries[1]);
            patch.insert_archive(&entries[2]);
            patch
        };
        drop(owner);
        drop(storage);
        assert_eq!(archive.node_stats().0, 2);

        let mut removed = archive.clone();
        removed.remove(&c);
        assert_eq!(removed.node_stats(), (1, 2, 0, 2));
        assert_eq!(patch_unowned_direct_local_leaves(&removed), 0);

        let mut only_c = PATCH::<KEY_SIZE, IdentitySchema>::new();
        only_c.insert(&Entry::new(&c));
        let archive = archive.difference(&only_c);

        // The remaining A/B subtree is already self-contained. It should be
        // promoted directly, with no unary parent and no heap materialization.
        assert_eq!(archive.node_stats(), (1, 2, 0, 2));
        assert_eq!(patch_unowned_direct_local_leaves(&archive), 0);
        let actual: HashSet<[u8; KEY_SIZE]> = archive.iter().copied().collect();
        assert_eq!(actual, HashSet::from([a, b]));
    }

    #[test]
    fn archive_consuming_iterators_retain_root_owner_guards() {
        const KEY_SIZE: usize = 8;
        let a = [0u8; KEY_SIZE];
        let mut b = [0u8; KEY_SIZE];
        b[0] = 1;

        let unordered_iter = owned_archive_pair([a, b]).into_iter();
        let noise = vec![0x69u8; KEY_SIZE * 128];
        std::hint::black_box(&noise);
        let unordered: HashSet<[u8; KEY_SIZE]> = unordered_iter.collect();
        assert_eq!(unordered, HashSet::from([a, b]));

        let ordered_iter = owned_archive_pair([a, b]).into_iter_ordered();
        let noise = vec![0x96u8; KEY_SIZE * 128];
        std::hint::black_box(&noise);
        let ordered: Vec<[u8; KEY_SIZE]> = ordered_iter.collect();
        assert_eq!(ordered, vec![a, b]);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_set_operation_entrypoints_retain_root_local_leaves() {
        const KEY_SIZE: usize = 8;
        let a = [0u8; KEY_SIZE];
        let mut b = [0u8; KEY_SIZE];
        b[0] = 1;

        let archive = owned_archive_pair([a, b]);
        let only_a = owned_archive_single(a);
        let intersection = archive.intersect(&only_a);
        let difference = archive.difference(&only_a);
        drop(archive);
        drop(only_a);

        let noise = vec![0x77u8; KEY_SIZE * 128];
        std::hint::black_box(&noise);
        assert_eq!(intersection.node_stats(), (0, 0, 0, 1));
        assert_eq!(difference.node_stats(), (0, 0, 0, 1));
        assert_eq!(intersection.iter().copied().collect_vec(), vec![a]);
        assert_eq!(difference.iter().copied().collect_vec(), vec![b]);
    }

    #[test]
    fn head_tag() {
        let head = Head::<64, IdentitySchema, ()>::new::<Leaf<64, ()>>(0, NonNull::dangling());
        assert_eq!(head.tag(), HeadTag::Leaf);
        mem::forget(head);
    }

    #[test]
    fn head_key() {
        for k in 0..=255 {
            let head = Head::<64, IdentitySchema, ()>::new::<Leaf<64, ()>>(k, NonNull::dangling());
            assert_eq!(head.key(), k);
            mem::forget(head);
        }
    }

    #[test]
    fn head_size() {
        assert_eq!(mem::size_of::<Head<64, IdentitySchema, ()>>(), 8);
    }

    #[test]
    fn option_head_size() {
        assert_eq!(mem::size_of::<Option<Head<64, IdentitySchema, ()>>>(), 8);
    }

    #[test]
    fn empty_tree() {
        let _tree = PATCH::<64, IdentitySchema, ()>::new();
    }

    #[test]
    fn tree_put_one() {
        const KEY_SIZE: usize = 64;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
    }

    #[test]
    fn heap_entry_initializes_hashing_before_patch_construction() {
        const KEY_SIZE: usize = 64;
        let key = [0x5au8; KEY_SIZE];

        // Deliberately construct the Entry first. It and an Entry created
        // after PATCH initialization must receive the same process-local hash.
        let before = Entry::new(&key);
        let mut before_patch = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        before_patch.insert(&before);

        let mut after_patch = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let after = Entry::new(&key);
        after_patch.insert(&after);
        assert_eq!(before_patch.root_hash(), after_patch.root_hash());
        assert_eq!(before_patch, after_patch);
    }

    #[test]
    fn tree_clone_one() {
        const KEY_SIZE: usize = 64;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
        let _clone = tree.clone();
    }

    #[test]
    fn tree_put_same() {
        const KEY_SIZE: usize = 64;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
        tree.insert(&entry);
    }

    #[test]
    fn ordered_infix_bounds_include_all_zero_and_all_ff() {
        let mut tree = PATCH::<4, IdentitySchema, ()>::new();
        tree.insert(&Entry::new(&[0x00; 4]));
        tree.insert(&Entry::new(&[0x80, 0x00, 0x00, 0x00]));
        tree.insert(&Entry::new(&[0xff; 4]));

        assert_eq!(
            tree.first_infix_range(&[], &[0x00; 4], &[0xff; 4]),
            Some([0x00; 4]),
        );
        assert_eq!(
            tree.next_infix_after(&[], &[0x00; 4], &[0xff; 4]),
            Some([0x80, 0x00, 0x00, 0x00]),
        );
        assert_eq!(
            tree.first_infix_range(&[], &[0xff; 4], &[0xff; 4]),
            Some([0xff; 4]),
        );
        assert_eq!(tree.next_infix_after(&[], &[0xff; 4], &[0xff; 4]), None,);
        assert_eq!(tree.first_infix_range(&[], &[0xff; 4], &[0x00; 4]), None,);
    }

    #[test]
    fn ordered_infix_descent_reads_local_leaves() {
        #[repr(C, align(16))]
        struct AlignedKey([u8; 16]);

        let storage = std::sync::Arc::new([
            AlignedKey([0x10; 16]),
            AlignedKey([0x20; 16]),
            AlignedKey([0xf0; 16]),
        ]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let mut tree = PATCH::<16, IdentitySchema, ()>::new();
        for key in storage.iter() {
            let entry = unsafe { ArchiveEntry::new(NonNull::from(&key.0), &owner) };
            tree.insert_archive(&entry);
        }

        assert!(tree.node_stats().3 > 0, "fixture must contain a LocalLeaf");
        assert_eq!(
            tree.first_infix_range(&[], &[0x11; 16], &[0xff; 16]),
            Some([0x20; 16]),
        );
        assert_eq!(
            tree.next_infix_after(&[], &[0x20; 16], &[0xff; 16]),
            Some([0xf0; 16]),
        );
    }

    #[test]
    fn bounded_infixes_are_atomic_over_archive_local_leaves() {
        #[repr(C, align(16))]
        struct AlignedKey([u8; 16]);

        let storage = std::sync::Arc::new([
            AlignedKey([0x10; 16]),
            AlignedKey([0x20; 16]),
            AlignedKey([0xf0; 16]),
        ]);
        let owner: std::sync::Arc<dyn ArchiveOwner> = storage.clone();
        let mut tree = PATCH::<16, IdentitySchema, ()>::new();
        for key in storage.iter() {
            let entry = unsafe { ArchiveEntry::new(NonNull::from(&key.0), &owner) };
            tree.insert_archive(&entry);
        }
        assert!(tree.node_stats().3 > 0, "fixture must contain a LocalLeaf");

        assert!(tree.bounded_infixes::<0, 16>(&[], 2).is_none());

        let mut expected = Vec::new();
        tree.infixes(&[], |value: &[u8; 16]| expected.push(*value));
        let mut accepted = Vec::new();
        let bounded = tree
            .bounded_infixes::<0, 16>(&[], 3)
            .expect("the exact count fits");
        assert_eq!(bounded.len(), 3);
        bounded.for_each(|value: &[u8; 16]| accepted.push(*value));
        assert_eq!(accepted, expected);
    }

    #[test]
    fn tree_replace_existing() {
        const KEY_SIZE: usize = 64;
        let key = [1u8; KEY_SIZE];
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let entry1 = Entry::with_value(&key, 1);
        tree.insert(&entry1);
        let entry2 = Entry::with_value(&key, 2);
        tree.replace(&entry2);
        assert_eq!(tree.get(&key), Some(&2));
    }

    #[test]
    fn tree_replace_childleaf_updates_branch() {
        const KEY_SIZE: usize = 64;
        let key1 = [0u8; KEY_SIZE];
        let key2 = [1u8; KEY_SIZE];
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let entry1 = Entry::with_value(&key1, 1);
        let entry2 = Entry::with_value(&key2, 2);
        tree.insert(&entry1);
        tree.insert(&entry2);
        let entry1b = Entry::with_value(&key1, 3);
        tree.replace(&entry1b);
        assert_eq!(tree.get(&key1), Some(&3));
        assert_eq!(tree.get(&key2), Some(&2));
    }

    #[test]
    fn update_child_refreshes_childleaf_on_replace() {
        const KEY_SIZE: usize = 4;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let key1 = [0u8; KEY_SIZE];
        let key2 = [1u8; KEY_SIZE];
        tree.insert(&Entry::with_value(&key1, 1));
        tree.insert(&Entry::with_value(&key2, 2));

        // Determine which child currently provides the branch childleaf.
        let root_ref = tree.root.as_ref().expect("root exists");
        let before_childleaf = *root_ref.childleaf_key();

        // Find the slot key (the byte index used in the branch table) for the child
        // that currently provides the childleaf.
        let slot_key = match root_ref.body_ref() {
            BodyRef::Branch(branch) => branch
                .child_table
                .iter()
                .filter_map(|c| c.as_ref())
                .find(|c| c.childleaf_key() == &before_childleaf)
                .expect("child exists")
                .key(),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => panic!("root should be a branch"),
        };

        // Replace that child with a new leaf that has a different childleaf key.
        let new_key = [2u8; KEY_SIZE];
        {
            let mut ed = crate::patch::branch::BranchMut::from_slot(&mut tree.root);
            ed.modify_child(slot_key, |_| {
                Some(Entry::with_value(&new_key, 42).leaf::<IdentitySchema>())
            });
            // drop(ed) commits
        }

        let after = tree.root.as_ref().expect("root exists");
        assert_eq!(after.childleaf_key(), &new_key);
    }

    #[test]
    fn remove_childleaf_updates_branch() {
        const KEY_SIZE: usize = 4;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let key1 = [0u8; KEY_SIZE];
        let key2 = [1u8; KEY_SIZE];
        tree.insert(&Entry::with_value(&key1, 1));
        tree.insert(&Entry::with_value(&key2, 2));

        let childleaf_before = *tree.root.as_ref().unwrap().childleaf_key();
        // remove the leaf that currently provides the branch.childleaf
        tree.remove(&childleaf_before);

        // Ensure the removed key is gone and the other key remains and is now the childleaf.
        let other = if childleaf_before == key1 { key2 } else { key1 };
        assert_eq!(tree.get(&childleaf_before), None);
        assert_eq!(tree.get(&other), Some(&2u32));
        let after_childleaf = tree.root.as_ref().unwrap().childleaf_key();
        assert_eq!(after_childleaf, &other);
    }

    #[test]
    fn remove_collapses_branch_to_single_child() {
        const KEY_SIZE: usize = 4;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let key1 = [0u8; KEY_SIZE];
        let key2 = [1u8; KEY_SIZE];
        tree.insert(&Entry::with_value(&key1, 1));
        tree.insert(&Entry::with_value(&key2, 2));

        // Remove one key and ensure the root collapses to the remaining child.
        tree.remove(&key1);
        assert_eq!(tree.get(&key1), None);
        assert_eq!(tree.get(&key2), Some(&2u32));
        let root = tree.root.as_ref().expect("root exists");
        match root.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {}
            BodyRef::Branch(_) => panic!("root should have collapsed to a leaf"),
        }
    }

    #[test]
    fn branch_size() {
        // Ownership lives once on PATCH, leaving a 48-byte Branch header.
        // Each child is an 8-byte tagged Head.
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 2], ()>>(
            ),
            48 + 8 * 2
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 4], ()>>(
            ),
            48 + 8 * 4
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 8], ()>>(
            ),
            48 + 8 * 8
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 16], ()>,
            >(),
            48 + 8 * 16
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<32, IdentitySchema, ()>>; 32], ()>,
            >(),
            48 + 8 * 32
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 64], ()>,
            >(),
            48 + 8 * 64
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 128], ()>,
            >(),
            48 + 8 * 128
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 256], ()>,
            >(),
            48 + 8 * 256
        );
    }

    #[test]
    fn patch_root_owner_guard_is_one_thin_arc() {
        assert_eq!(mem::size_of::<Option<Arc<OwnerCover>>>(), 8);
        assert_eq!(mem::size_of::<PATCH<64, IdentitySchema, ()>>(), 16);
    }

    /// Checks what happens if we join two PATCHes that
    /// only contain a single element each, that differs in the last byte.
    #[test]
    fn tree_union_single() {
        const KEY_SIZE: usize = 8;
        let mut left = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let mut right = PATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let left_entry = Entry::new(&[0, 0, 0, 0, 0, 0, 0, 0]);
        let right_entry = Entry::new(&[0, 0, 0, 0, 0, 0, 0, 1]);
        left.insert(&left_entry);
        right.insert(&right_entry);
        left.union(right);
        assert_eq!(left.len(), 2);
    }

    // Small unit tests that ensure BranchMut-based editing is used by
    // the higher-level set operations like intersect/difference. These are
    // ordinary unit tests (not proptest) and must appear outside the
    // `proptest!` macro below.

    proptest! {
        #[test]
        fn tree_insert(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = PATCH::<64, IdentitySchema, ()>::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
            }
        }

        #[test]
        fn tree_len(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = PATCH::<64, IdentitySchema, ()>::new();
            let mut set = HashSet::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
                set.insert(key);
            }

            prop_assert_eq!(set.len() as u64, tree.len())
        }

        #[test]
        fn tree_infixes(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = PATCH::<64, IdentitySchema, ()>::new();
            let mut set = HashSet::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
                set.insert(key);
            }
            let mut set_vec = Vec::from_iter(set.into_iter());
            let mut tree_vec = vec![];
            tree.infixes(&[0; 0], &mut |&x: &[u8; 64]| tree_vec.push(x));

            set_vec.sort();
            tree_vec.sort();

            prop_assert_eq!(set_vec, tree_vec);
        }

        #[test]
        fn tree_iter(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = PATCH::<64, IdentitySchema, ()>::new();
            let mut set = HashSet::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
                set.insert(key);
            }
            let mut set_vec = Vec::from_iter(set.into_iter());
            let mut tree_vec = vec![];
            for key in &tree {
                tree_vec.push(*key);
            }

            set_vec.sort();
            tree_vec.sort();

            prop_assert_eq!(set_vec, tree_vec);
        }

        #[test]
        fn tree_union(left in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200),
                        right in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200)) {
            let mut set = HashSet::new();

            let mut left_tree = PATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                let entry = Entry::new(&key);
                left_tree.insert(&entry);
                set.insert(key);
            }

            let mut right_tree = PATCH::<64, IdentitySchema, ()>::new();
            for entry in right {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                let entry = Entry::new(&key);
                right_tree.insert(&entry);
                set.insert(key);
            }

            left_tree.union(right_tree);

            let mut set_vec = Vec::from_iter(set.into_iter());
            let mut tree_vec = vec![];
            left_tree.infixes(&[0; 0], &mut |&x: &[u8;64]| tree_vec.push(x));

            set_vec.sort();
            tree_vec.sort();

            prop_assert_eq!(set_vec, tree_vec);
            }

        #[test]
        fn tree_union_empty(left in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 2)) {
            let mut set = HashSet::new();

            let mut left_tree = PATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                let entry = Entry::new(&key);
                left_tree.insert(&entry);
                set.insert(key);
            }

            let right_tree = PATCH::<64, IdentitySchema, ()>::new();

            left_tree.union(right_tree);

            let mut set_vec = Vec::from_iter(set.into_iter());
            let mut tree_vec = vec![];
            left_tree.infixes(&[0; 0], &mut |&x: &[u8;64]| tree_vec.push(x));

            set_vec.sort();
            tree_vec.sort();

            prop_assert_eq!(set_vec, tree_vec);
            }

        // I got a feeling that we're not testing COW properly.
        // We should check if a tree remains the same after a clone of it
        // is modified by inserting new keys.

    #[test]
    fn cow_on_insert(base_keys in prop::collection::vec(prop::collection::vec(0u8..=255, 8), 1..1024),
                         new_keys in prop::collection::vec(prop::collection::vec(0u8..=255, 8), 1..1024)) {
            // Note that we can't compare the trees directly, as that uses the hash,
            // which might not be affected by nodes in lower levels being changed accidentally.
            // Instead we need to iterate over the keys and check if they are the same.

            let mut tree = PATCH::<8, IdentitySchema, ()>::new();
            for key in base_keys {
                let key: [u8; 8] = key[..].try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
            }
            let base_tree_content: Vec<[u8; 8]> = tree.iter().copied().collect();

            let mut tree_clone = tree.clone();
            for key in new_keys {
                let key: [u8; 8] = key[..].try_into().unwrap();
                let entry = Entry::new(&key);
                tree_clone.insert(&entry);
            }

            let new_tree_content: Vec<[u8; 8]> = tree.iter().copied().collect();
            prop_assert_eq!(base_tree_content, new_tree_content);
        }

        #[test]
    fn cow_on_union(base_keys in prop::collection::vec(prop::collection::vec(0u8..=255, 8), 1..1024),
                         new_keys in prop::collection::vec(prop::collection::vec(0u8..=255, 8), 1..1024)) {
            // Note that we can't compare the trees directly, as that uses the hash,
            // which might not be affected by nodes in lower levels being changed accidentally.
            // Instead we need to iterate over the keys and check if they are the same.

            let mut tree = PATCH::<8, IdentitySchema, ()>::new();
            for key in base_keys {
                let key: [u8; 8] = key[..].try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
            }
            let base_tree_content: Vec<[u8; 8]> = tree.iter().copied().collect();

            let mut tree_clone = tree.clone();
            let mut new_tree = PATCH::<8, IdentitySchema, ()>::new();
            for key in new_keys {
                let key: [u8; 8] = key[..].try_into().unwrap();
                let entry = Entry::new(&key);
                new_tree.insert(&entry);
            }
            tree_clone.union(new_tree);

            let new_tree_content: Vec<[u8; 8]> = tree.iter().copied().collect();
            prop_assert_eq!(base_tree_content, new_tree_content);
        }
    }

    #[test]
    fn intersect_multiple_common_children_commits_branchmut() {
        const KEY_SIZE: usize = 4;
        let mut left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let a = [0u8, 0u8, 0u8, 1u8];
        let b = [0u8, 0u8, 0u8, 2u8];
        let c = [0u8, 0u8, 0u8, 3u8];
        let d = [2u8, 0u8, 0u8, 0u8];
        let e = [3u8, 0u8, 0u8, 0u8];

        left.insert(&Entry::with_value(&a, 1));
        left.insert(&Entry::with_value(&b, 2));
        left.insert(&Entry::with_value(&c, 3));
        left.insert(&Entry::with_value(&d, 4));

        right.insert(&Entry::with_value(&a, 10));
        right.insert(&Entry::with_value(&b, 11));
        right.insert(&Entry::with_value(&c, 12));
        right.insert(&Entry::with_value(&e, 13));

        let res = left.intersect(&right);
        // A, B, C are common
        assert_eq!(res.len(), 3);
        assert!(res.get(&a).is_some());
        assert!(res.get(&b).is_some());
        assert!(res.get(&c).is_some());
    }

    #[test]
    fn difference_multiple_children_commits_branchmut() {
        const KEY_SIZE: usize = 4;
        let mut left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let a = [0u8, 0u8, 0u8, 1u8];
        let b = [0u8, 0u8, 0u8, 2u8];
        let c = [0u8, 0u8, 0u8, 3u8];
        let d = [2u8, 0u8, 0u8, 0u8];
        let e = [3u8, 0u8, 0u8, 0u8];

        left.insert(&Entry::with_value(&a, 1));
        left.insert(&Entry::with_value(&b, 2));
        left.insert(&Entry::with_value(&c, 3));
        left.insert(&Entry::with_value(&d, 4));

        right.insert(&Entry::with_value(&a, 10));
        right.insert(&Entry::with_value(&b, 11));
        right.insert(&Entry::with_value(&c, 12));
        right.insert(&Entry::with_value(&e, 13));

        let res = left.difference(&right);
        // left only has d
        assert_eq!(res.len(), 1);
        assert!(res.get(&d).is_some());
    }

    #[test]
    fn difference_empty_left_is_empty() {
        const KEY_SIZE: usize = 4;
        let left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let key = [1u8, 2u8, 3u8, 4u8];
        right.insert(&Entry::with_value(&key, 7));

        let res = left.difference(&right);
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn difference_empty_right_returns_left() {
        const KEY_SIZE: usize = 4;
        let mut left = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let right = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let key = [1u8, 2u8, 3u8, 4u8];
        left.insert(&Entry::with_value(&key, 7));

        let res = left.difference(&right);
        assert_eq!(res.len(), 1);
        assert!(res.get(&key).is_some());
    }

    #[test]
    fn slot_edit_branchmut_insert_update() {
        // Small unit test demonstrating the Slot::edit -> BranchMut insert/update pattern.
        const KEY_SIZE: usize = 8;
        let mut tree = PATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let entry1 = Entry::with_value(&[0u8; KEY_SIZE], 1u32);
        let entry2 = Entry::with_value(&[1u8; KEY_SIZE], 2u32);
        tree.insert(&entry1);
        tree.insert(&entry2);
        assert_eq!(tree.len(), 2);

        // Edit the root slot in-place using the BranchMut editor.
        {
            let mut ed = crate::patch::branch::BranchMut::from_slot(&mut tree.root);

            // Compute the insertion start depth first to avoid borrowing `ed` inside the closure.
            let start_depth = ed.end_depth as usize;
            let inserted = Entry::with_value(&[2u8; KEY_SIZE], 3u32)
                .leaf::<IdentitySchema>()
                .with_start(start_depth);
            let key = inserted.key();

            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::insert_leaf(old, inserted, start_depth)),
                None => Some(inserted),
            });
            // BranchMut is dropped here and commits the updated branch pointer back into the head.
        }

        assert_eq!(tree.len(), 3);
        assert_eq!(tree.get(&[2u8; KEY_SIZE]), Some(&3u32));
    }
}
