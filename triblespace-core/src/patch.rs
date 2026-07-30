//! Persistent Adaptive Trie with Cuckoo-compression and
//! Hash-maintenance (PATCH).
//!
//! See the [PATCH](../book/src/deep-dive/patch.md) chapter of the Tribles Book
//! for the full design description and hashing scheme.
//!
//! Values stored in leaves are not part of hashing or equality comparisons.
//! Two [`PATCH`](crate::patch::PATCH)es are considered equal if they contain the same set of keys,
//! even if the associated values differ. This allows using the structure as an
//! idempotent blobstore where a value's hash determines its key.
//!
#![allow(unstable_name_collisions)]

mod branch;
/// Byte-indexed lookup tables used by PATCH branch nodes.
pub mod bytetable;
mod entry;
mod leaf;
#[cfg(feature = "patch-probe")]
pub(crate) mod probe;

use arrayvec::ArrayVec;

/// Re-export of [`Entry`](entry::Entry).
use branch::*;
pub use entry::{ArchiveEntry, Entry};
use leaf::*;

/// Re-export of all byte table utilities.
pub use bytetable::*;
#[cfg(feature = "patch-probe")]
use rand::rngs::StdRng;
#[cfg(not(feature = "patch-probe"))]
use rand::thread_rng;
use rand::RngCore;
#[cfg(feature = "patch-probe")]
use rand::SeedableRng;
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

/// One complete binary tree in an archive-owner cover.
///
/// Rank-zero trees store the owner Arc directly. Joining two equal-rank trees
/// allocates only their shared parent, so binary carrying shares every
/// untouched subtree with older PATCH snapshots.
#[derive(Clone)]
// In production the tree is intentionally opaque: retaining and eventually
// dropping its fields is the operation. Tests traverse it to verify shape.
#[cfg_attr(not(test), allow(dead_code))]
enum OwnerTree {
    Owner(Arc<dyn ArchiveOwner>),
    Pair(Arc<OwnerPair>),
}

#[cfg_attr(not(test), allow(dead_code))]
struct OwnerPair {
    left: OwnerTree,
    right: OwnerTree,
}

/// Persistent monotone cover of archive allocations retained by one PATCH.
///
/// `roots[rank]` is either empty or a complete owner tree with `2^rank`
/// leaves. Retaining one owner or joining two covers uses ordinary binary
/// carrying. The cover deliberately does not maintain an exact global set:
/// only the latest owner and the two structurally trivial union cases are
/// deduplicated. This keeps its shape balanced and its maintenance independent
/// of the number of distinct archive allocations.
#[derive(Clone)]
struct OwnerCover {
    /// Direct hot-path key. Archive ingestion overwhelmingly repeats the most
    /// recently added owner.
    latest_address: usize,
    roots: Vec<Option<OwnerTree>>,
}

impl core::fmt::Debug for OwnerCover {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OwnerCover")
            .field("ranks", &self.roots.len())
            .finish()
    }
}

impl OwnerCover {
    #[inline]
    fn address(owner: &Arc<dyn ArchiveOwner>) -> usize {
        Arc::as_ptr(owner) as *const () as usize
    }

    fn singleton(owner: &Arc<dyn ArchiveOwner>) -> Arc<Self> {
        Arc::new(Self {
            latest_address: Self::address(owner),
            roots: vec![Some(OwnerTree::Owner(owner.clone()))],
        })
    }

    /// Add one rank-`rank` tree, joining equal ranks by binary carry.
    ///
    /// Existing slots are cloned into the new pair spine before any slot is
    /// changed. Capacity is likewise reserved first. Publishing the final
    /// tree before clearing the carried slots means the installed cover never
    /// stops retaining an owner, even if an allocation panic is caught.
    fn carry(&mut self, rank: usize, mut tree: OwnerTree) {
        let mut destination = rank;
        while destination < self.roots.len() && self.roots[destination].is_some() {
            destination += 1;
        }
        if destination >= self.roots.len() {
            self.roots.reserve(destination + 1 - self.roots.len());
        }

        for occupied_rank in rank..destination {
            let left = self.roots[occupied_rank]
                .as_ref()
                .expect("binary carry crossed an empty owner-cover rank")
                .clone();
            tree = OwnerTree::Pair(Arc::new(OwnerPair { left, right: tree }));
        }

        if destination < self.roots.len() {
            self.roots[destination] = Some(tree);
        } else {
            self.roots.resize_with(destination, || None);
            self.roots.push(Some(tree));
        }
        for occupied_rank in rank..destination {
            self.roots[occupied_rank] = None;
        }
    }

    #[inline]
    fn singleton_address(&self) -> Option<usize> {
        match self.roots.as_slice() {
            [Some(OwnerTree::Owner(owner))] => Some(Self::address(owner)),
            _ => None,
        }
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

        // Arc::make_mut clones the logarithmic root vector only when another
        // PATCH snapshot shares it. The common unique case performs an
        // amortized-constant binary carry in place.
        let cover = Arc::make_mut(existing);
        cover.carry(0, OwnerTree::Owner(owner.clone()));
        cover.latest_address = address;
    }

    /// Monotonically extend an installed cover with every tree in `other`.
    ///
    /// Each individual carry is transactional. If a later carry panics, the
    /// installed cover may contain a conservative prefix of `other`, but it
    /// still covers its original PATCH root. The caller retains `other` and
    /// its root for the duration of the operation, so no LocalLeaf can dangle
    /// during unwind.
    fn merge_into(current: &mut Option<Arc<Self>>, other: Option<Arc<Self>>) {
        let Some(other) = other else {
            return;
        };
        let Some(left) = current.as_mut() else {
            *current = Some(other);
            return;
        };
        if Arc::ptr_eq(left, &other) {
            return;
        }
        let left_singleton = left.singleton_address();
        if left_singleton.is_some() && left_singleton == other.singleton_address() {
            return;
        }

        let latest_address = other.latest_address;
        let cover = Arc::make_mut(left);
        for (rank, tree) in other.roots.iter().enumerate() {
            if let Some(tree) = tree {
                cover.carry(rank, tree.clone());
            }
        }
        cover.latest_address = latest_address;
    }

    fn union(left: Option<Arc<Self>>, right: Option<Arc<Self>>) -> Option<Arc<Self>> {
        let mut result = left;
        Self::merge_into(&mut result, right);
        result
    }
}

#[cfg(test)]
#[derive(Default)]
struct OwnerCoverStats {
    owners: usize,
    pairs: usize,
    max_depth: usize,
}

#[cfg(test)]
impl OwnerTree {
    fn collect_stats(&self, depth: usize, stats: &mut OwnerCoverStats) {
        match self {
            Self::Owner(_) => {
                stats.owners += 1;
                stats.max_depth = stats.max_depth.max(depth);
            }
            Self::Pair(pair) => {
                stats.pairs += 1;
                pair.left.collect_stats(depth + 1, stats);
                pair.right.collect_stats(depth + 1, stats);
            }
        }
    }
}

#[cfg(test)]
impl OwnerCover {
    fn stats(&self) -> OwnerCoverStats {
        let mut stats = OwnerCoverStats::default();
        for tree in self.roots.iter().flatten() {
            tree.collect_stats(0, &mut stats);
        }
        stats
    }

    fn owner_count(&self) -> usize {
        self.stats().owners
    }

    fn root_count(&self) -> usize {
        self.roots.iter().flatten().count()
    }
}

#[cfg(not(target_pointer_width = "64"))]
compile_error!("PATCH tagged pointers require 64-bit targets");

static mut SIP_KEY: [u8; 16] = [0; 16];
static INIT: Once = Once::new();

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
///      branch, then `recompute_aggregates` rebuilds the
///      hash/leaf_count/segment_count/childleaf in one pass.
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

    /// Raw-pointer wrapper for the scatter-write target. Each
    /// spawned task writes to `resolved[k]` for its specific key
    /// byte `k`; keys are pairwise distinct by construction (each
    /// "both" bit in the partition uniquely identifies a slot), so
    /// the writes are non-aliasing despite sharing a `*mut` across
    /// threads.
    ///
    /// `write_at` exists as an inherent method (rather than callers
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

    unsafe impl<T> Send for ScatterPtr<T> {}
    unsafe impl<T> Sync for ScatterPtr<T> {}

    impl<T> ScatterPtr<T> {
        /// SAFETY: `i` must be in-bounds of the underlying buffer,
        /// and the caller must guarantee no other thread is writing
        /// to slot `i` concurrently.
        pub(crate) unsafe fn write_at(self, i: usize, v: T) {
            self.0.add(i).write(v);
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

        #[cfg(feature = "patch-probe")]
        let mut rng = StdRng::seed_from_u64(0x5041_5443_485f_5349);
        #[cfg(not(feature = "patch-probe"))]
        let mut rng = thread_rng();
        unsafe {
            rng.fill_bytes(&mut SIP_KEY[..]);
        }
    });
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

    pub(crate) fn hash(&self) -> u128 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.hash,
            BodyRef::LocalLeaf(bytes) => {
                use siphasher::sip128::SipHasher24;
                use std::ptr::addr_of;
                #[cfg(feature = "patch-probe")]
                probe::record_local_leaf_hash();
                // SAFETY: SIP_KEY is initialized at startup; we only read it.
                let key = unsafe { *addr_of!(SIP_KEY) };
                SipHasher24::new_with_key(&key).hash(&bytes[..]).into()
            }
            BodyRef::Branch(branch) => branch.hash,
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

// Archive-aware insertion path, available only when V = ().
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

    /// Inserts a LocalLeaf whose hash was already computed by ArchiveEntry.
    /// The enclosing PATCH retains the leaf's archive owner independently of
    /// trie shape, so LocalLeaves can move through ordinary Branch operations.
    pub(crate) fn insert_archive_leaf(
        mut this: Self,
        leaf: Self,
        leaf_hash: u128,
        start_depth: usize,
    ) -> Self {
        if let Some((depth, this_byte_key, leaf_byte_key)) =
            this.first_divergence(&leaf, start_depth)
        {
            let old_key = this.key();
            let new_body = crate::patch::branch::Branch::new_with_rchild_hash(
                depth,
                this.with_key(this_byte_key),
                leaf.with_key(leaf_byte_key),
                leaf_hash,
            );
            return Head::new(old_key, new_body);
        }

        let end_depth = this.end_depth();
        if end_depth != KEY_LEN {
            let mut ed = crate::patch::branch::BranchMut::from_head(&mut this);
            let inserted = leaf.with_start(ed.end_depth as usize);
            let key = inserted.key();
            ed.modify_child_with_inserted_hint(key, leaf_hash, |opt| match opt {
                None => Some(inserted),
                Some(old) => Some(Head::insert_archive_leaf(
                    old, inserted, leaf_hash, end_depth,
                )),
            });
        }
        this
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
    pub(crate) fn union(mut this: Self, mut other: Self, at_depth: usize) -> Self {
        if this.hash() == other.hash() {
            return this;
        }

        if let Some((depth, this_byte_key, other_byte_key)) =
            this.first_divergence(&other, at_depth)
        {
            let old_key = this.key();
            let new_body = Branch::new(
                depth,
                this.with_key(this_byte_key),
                other.with_key(other_byte_key),
            );

            return Head::new(old_key, new_body);
        }

        let this_depth = this.end_depth();
        let other_depth = other.end_depth();
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
        // collisions with `modify_child`'s per-call accounting.
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

    /// Recursive parallel-aware union: at the equal-depth-branch
    /// arm, drains the "both" pairs and, for each pair, either
    /// claims a budget unit and spawns a parallel task or falls
    /// back to serial `Self::union`. All other arms (hash-equal,
    /// divergence, asymmetric depth) delegate to `Self::union` —
    /// they don't generate fan-out work for the budget to spend.
    #[cfg(feature = "parallel")]
    pub(crate) fn par_union_with_ctx(
        mut this: Self,
        mut other: Self,
        at_depth: usize,
        ctx: &parallel_union::ParUnionCtx,
    ) -> Self
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if this.hash() == other.hash() {
            return this;
        }

        if let Some((depth, this_byte_key, other_byte_key)) =
            this.first_divergence(&other, at_depth)
        {
            let old_key = this.key();
            let new_body = Branch::new(
                depth,
                this.with_key(this_byte_key),
                other.with_key(other_byte_key),
            );
            return Head::new(old_key, new_body);
        }

        let this_depth = this.end_depth();
        let other_depth = other.end_depth();
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

            // Scatter both child tables into key-indexed 256-slot
            // arrays + present bitsets. The bitset partition tells us
            // which keys need a recursive union ("both") vs which are
            // simple pass-throughs ("only").
            let mut this_arr: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
            let mut other_arr: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
            let mut this_present = crate::patch::bytetable::ByteSet::new_empty();
            let mut other_present = crate::patch::bytetable::ByteSet::new_empty();

            for slot in ed.child_table.iter_mut() {
                if let Some(head) = slot.take() {
                    let key = head.key();
                    this_present.insert(key);
                    this_arr[key as usize] = Some(head);
                }
            }
            for slot in other_branch_ref.child_table.iter_mut() {
                if let Some(head) = slot.take() {
                    let head = head.with_start(end_depth);
                    let key = head.key();
                    other_present.insert(key);
                    other_arr[key as usize] = Some(head);
                }
            }

            let mut both = this_present.intersect(&other_present);
            let mut only = this_present.symmetric_difference(&other_present);

            // Pre-allocated scatter-write target. Each spawned task
            // writes to `resolved[k]` for its specific key byte —
            // disjoint by construction. The raw pointer wrapper
            // (`ScatterPtr`) makes the cross-thread sharing explicit.
            let mut resolved: [Option<Head<KEY_LEN, O, V>>; 256] = std::array::from_fn(|_| None);
            let resolved_ptr = parallel_union::ScatterPtr(resolved.as_mut_ptr());

            rayon::scope(|s| {
                // Drain `both` pairs serially in the parent; per
                // pair, either claim a spawn unit and dispatch as a
                // task, or run serially via `Head::union` here on
                // the parent thread. The atomic budget is shared
                // with all nested `par_union_with_ctx` calls.
                while let Some(k) = both.drain_next_ascending() {
                    let i = k as usize;
                    let t = this_arr[i].take().expect("both ⇒ this");
                    let o = other_arr[i].take().expect("both ⇒ other");
                    if ctx.try_claim() {
                        s.spawn(move |_| {
                            let head = Self::par_union_with_ctx(t, o, this_depth, ctx);
                            // SAFETY: each task has a distinct
                            // key `k`, so the writes to
                            // `resolved[i]` are non-aliasing.
                            unsafe {
                                resolved_ptr.write_at(i, Some(head));
                            }
                        });
                    } else {
                        // Budget exhausted — fall back to fully
                        // serial union on this pair, then scatter
                        // the result. SAFETY: same disjointness
                        // invariant; the parent thread races only
                        // with tasks targeting distinct keys.
                        let head = Self::union(t, o, this_depth);
                        unsafe {
                            resolved_ptr.write_at(i, Some(head));
                        }
                    }
                }
            });
            // After scope: all spawned tasks have completed; the
            // scatter writes to `resolved` are all sequenced-before
            // here by rayon's join semantics.

            for slot in resolved.iter_mut() {
                if let Some(head) = slot.take() {
                    ed.install_child_growing(head);
                }
            }
            while let Some(k) = only.drain_next_ascending() {
                let i = k as usize;
                let head = this_arr[i]
                    .take()
                    .or_else(|| other_arr[i].take())
                    .expect("only ⇒ exactly one side");
                ed.install_child_growing(head);
            }

            ed.recompute_aggregates();
        }
        this
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
        if self.hash() == other.hash() {
            return Some(self.clone());
        }
        if self.first_divergence(other, at_depth).is_some() {
            return None;
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
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
        if self.hash() == other.hash() {
            return None;
        }
        if self.first_divergence(other, at_depth).is_some() {
            return Some(self.clone());
        }
        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
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
        // `recompute_aggregates` pass wins handily over per-call
        // `modify_child`. Mirror of the union pattern; intersect
        // uses `modify_child` because its collection phase has
        // far fewer children (heavy filtering).
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
            ed.recompute_aggregates();
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
        if self.hash() == other.hash() {
            return Some(self.clone());
        }

        if self.first_divergence(other, at_depth).is_some() {
            return None;
        }

        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
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
        if self.hash() == other.hash() {
            return None;
        }

        if self.first_divergence(other, at_depth).is_some() {
            return Some(self.clone());
        }

        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
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

    /// Inserts a shared key into the PATCH.
    ///
    /// Takes an [Entry] object that can be created from a key,
    /// and inserted into multiple PATCH instances.
    ///
    /// If the key is already present, this is a no-op.
    pub fn insert(&mut self, entry: &Entry<KEY_LEN, V>) {
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::insert_leaf(this, entry.leaf(), 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(entry.leaf());
        }
        self.debug_check_owner_invariant();
    }

    /// Inserts a key into the PATCH, replacing the value if it already exists.
    pub fn replace(&mut self, entry: &Entry<KEY_LEN, V>) {
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::replace_leaf(this, entry.leaf(), 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(entry.leaf());
        }
        self.debug_check_owner_invariant();
    }

    /// Removes a key from the PATCH.
    ///
    /// If the key is not present, this is a no-op.
    pub fn remove(&mut self, key: &[u8; KEY_LEN]) {
        Head::remove_leaf(&mut self.root, key, 0);
        if self.root.is_none() {
            self.owners = None;
        }
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
            self.root.is_some() || self.owners.is_none(),
            "an empty PATCH must not retain archive owners",
        );
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
    pub fn union(&mut self, other: Self)
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        let Self {
            root: other_root,
            owners: other_owners,
        } = other;
        if let Some(other) = other_root {
            if self.root.is_some() {
                // Extend the installed lifetime guard before Head::union can
                // detach or move either side's LocalLeaves. Owner-cover carry
                // is monotone and transactional, so a caught allocation panic
                // leaves this PATCH's existing root fully guarded.
                OwnerCover::merge_into(&mut self.owners, other_owners);
                let this = self.root.take().expect("root should not be empty");
                #[cfg(feature = "parallel")]
                let merged = Head::par_union(this, other, 0);
                #[cfg(not(feature = "parallel"))]
                let merged = Head::union(this, other, 0);
                self.root.replace(merged);
            } else {
                self.root.replace(other);
                self.owners = other_owners;
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
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                #[cfg(feature = "parallel")]
                let result = root.par_intersect(other_root, 0);
                #[cfg(not(feature = "parallel"))]
                let result = root.intersect(other_root, 0);
                let root = result.map(|root| root.with_start(0));
                let owners = root
                    .as_ref()
                    .and_then(|_| OwnerCover::union(self.owners.clone(), other.owners.clone()));
                let result = Self { root, owners };
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
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                #[cfg(feature = "parallel")]
                let result = root.par_difference(other_root, 0);
                #[cfg(not(feature = "parallel"))]
                let result = root.difference(other_root, 0);
                let owners = result.as_ref().and(self.owners.clone());
                let result = Self {
                    root: result,
                    owners,
                };
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
    /// Builds the smallest valid compressed trie for two distinct entries
    /// from the same archive owner. Because the batch cardinality is already
    /// known, both roots can remain LocalLeaves under one ordinary Branch; no
    /// heap seed or unary Branch is required.
    pub(crate) fn from_archive_pair(
        first: &ArchiveEntry<'_, KEY_LEN>,
        second: &ArchiveEntry<'_, KEY_LEN>,
    ) -> Self {
        let (first_head, first_owner, first_hash) = first.leaf::<O>();
        let (second_head, second_owner, second_hash) = second.leaf::<O>();
        assert!(
            std::sync::Arc::ptr_eq(first_owner, second_owner),
            "an archive bootstrap pair must share one owner",
        );
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
            owners: Some(OwnerCover::singleton(first_owner)),
        };
        result.debug_check_owner_invariant();
        result
    }

    /// Inserts an archive-backed key and retains its allocation in the PATCH's
    /// persistent root owner cover.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, KEY_LEN>) {
        let (leaf_head, leaf_owner, leaf_hash) = entry.leaf::<O>();
        OwnerCover::retain(&mut self.owners, leaf_owner);
        if let Some(this) = self.root.take() {
            let new_head = Head::insert_archive_leaf(this, leaf_head, leaf_hash, 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(leaf_head);
        }
        self.debug_check_owner_invariant();
    }
}

impl<const KEY_LEN: usize, O, V> PartialEq for PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn eq(&self, other: &Self) -> bool {
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
    use std::collections::HashSet;
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

    fn test_archive_owner(byte: u8) -> Arc<dyn ArchiveOwner> {
        Arc::new([byte])
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
        assert_eq!(stats.pairs, 0);
        assert_eq!(stats.max_depth, 0);
        assert_eq!(cover.root_count(), 1);
    }

    #[test]
    fn owner_cover_alternation_stays_balanced_and_linear() {
        const RETAINS: usize = 4093;
        let owners = [test_archive_owner(1), test_archive_owner(2)];
        let mut cover = None;
        for i in 0..RETAINS {
            OwnerCover::retain(&mut cover, &owners[i & 1]);
        }

        let cover = cover.unwrap();
        let stats = cover.stats();
        let expected_roots = RETAINS.count_ones() as usize;
        let expected_depth = usize::BITS as usize - 1 - RETAINS.leading_zeros() as usize;
        assert_eq!(stats.owners, RETAINS);
        assert_eq!(stats.pairs, RETAINS - expected_roots);
        assert_eq!(stats.max_depth, expected_depth);
        assert_eq!(cover.root_count(), expected_roots);
        assert!(stats.owners + stats.pairs < 2 * RETAINS);
    }

    #[test]
    fn owner_cover_singleton_union_fold_is_binary_carry() {
        const UNIONS: usize = 512;
        let owners = [test_archive_owner(1), test_archive_owner(2)];
        let mut cover = None;
        OwnerCover::merge_into(&mut cover, Some(OwnerCover::singleton(&owners[0])));
        let allocation = Arc::as_ptr(cover.as_ref().unwrap());
        for i in 1..UNIONS {
            OwnerCover::merge_into(
                &mut cover,
                Some(OwnerCover::singleton(&owners[i & 1])),
            );
        }

        let cover = cover.unwrap();
        assert_eq!(Arc::as_ptr(&cover), allocation);
        let stats = cover.stats();
        assert_eq!(stats.owners, UNIONS);
        assert_eq!(stats.pairs, UNIONS - 1);
        assert_eq!(stats.max_depth, 9);
        assert_eq!(cover.root_count(), 1);
    }

    #[test]
    fn owner_cover_only_collapses_structural_identities() {
        let owner = test_archive_owner(1);
        let mut left = None;
        let mut right = None;
        OwnerCover::retain(&mut left, &owner);
        OwnerCover::retain(&mut right, &owner);
        let left_snapshot = left.as_ref().unwrap().clone();

        let singleton_union = OwnerCover::union(left, right).unwrap();
        assert!(Arc::ptr_eq(&singleton_union, &left_snapshot));

        let identical_union = OwnerCover::union(
            Some(singleton_union.clone()),
            Some(singleton_union.clone()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&identical_union, &singleton_union));
    }

    #[test]
    fn owner_cover_snapshots_retain_owners_until_the_last_drop() {
        struct CountedOwner(Arc<AtomicUsize>);

        impl Drop for CountedOwner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let first: Arc<dyn ArchiveOwner> = Arc::new(CountedOwner(drops.clone()));
        let second: Arc<dyn ArchiveOwner> = Arc::new(CountedOwner(drops.clone()));
        let mut cover = None;
        for i in 0..65 {
            let owner = if i & 1 == 0 { &first } else { &second };
            OwnerCover::retain(&mut cover, owner);
        }
        let snapshot = cover.clone();

        drop(first);
        drop(second);
        drop(cover);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(snapshot);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
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
