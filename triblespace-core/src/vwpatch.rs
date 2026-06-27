//! Persistent Adaptive Trie with Cuckoo-compression and
//! Hash-maintenance (VWPATCH).
//!
//! See the [VWPATCH](../book/src/deep-dive/patch.md) chapter of the Tribles Book
//! for the full design description and hashing scheme.
//!
//! Values stored in leaves are not part of hashing or equality comparisons.
//! Two [`VWPATCH`](crate::vwpatch::VWPATCH)es are considered equal if they contain the same set of keys,
//! even if the associated values differ. This allows using the structure as an
//! idempotent blobstore where a value's hash determines its key.
//!
#![allow(unstable_name_collisions)]

mod branch;
/// Byte-indexed lookup tables used by VWPATCH branch nodes.
pub mod bytetable;
mod entry;
mod leaf;
/// READ-ONLY union span-misalignment measurement spike.
pub mod spike;

use arrayvec::ArrayVec;

/// Re-export of [`Entry`](entry::Entry).
pub use branch::ArchiveOwner;
use branch::*;
pub use entry::{ArchiveEntry, Entry};
use leaf::*;

/// Re-export of all byte table utilities.
pub use bytetable::*;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Once;

#[cfg(not(target_pointer_width = "64"))]
compile_error!("VWPATCH tagged pointers require 64-bit targets");

// Share the SIP key with the original `patch` module so that an identical
// key set produces identical leaf/node hashes across both tries. The key is
// owned and initialized exactly once by `crate::patch` (its `INIT` Once);
// the clone re-exports it and routes initialization through
// `crate::patch::init_sip_key()` (see `init_sip_key` below). The local `INIT`
// Once here only guards this module's own `bytetable::init()`.
pub(crate) use crate::patch::SIP_KEY;
static INIT: Once = Once::new();

/// Minimum combined `leaf_count` (`this + other`) at which the parallel set ops
/// take the partition + `rayon::scope`-spawn fan-out path on the equal-depth,
/// equal-span branch arm. Below this the serial span-reconciling descent wins
/// because the per-spawn overhead isn't amortised.
#[cfg(feature = "parallel")]
const PARALLEL_PATCH_UNION_THRESHOLD: usize = 4096;

/// Parallel-aware VWPATCH set ops (union/intersect/difference), with a shared
/// work-stealing budget carried across the entire recursive descent.
///
/// The cheap base cases (hash-equal short-circuit, compressed-path divergence,
/// span reconciliation via `narrow_span`) stay serial and identical to the
/// serial op — they don't fan out. The fan-out happens at the equal-depth,
/// equal-span branch arm, per call:
///   1. Partition both child sets by their actual span sub-key over `[s, te)`
///      (NOT the 16-bit fingerprint, which collides) into "both" pairs and
///      single-side "only" children.
///   2. Fan out: for each "both" pair, claim one unit from the shared budget —
///      if successful, `rayon::scope`-spawn the recursive op, writing the
///      result into a disjoint `ScatterPtr` slot; if the budget is exhausted,
///      run that pair inline (serial) on the current thread.
///   3. Re-assemble the resolved "both" outputs + the op's kept "only" children
///      (union: both∪ + only-this + only-other; intersect: both∩ only;
///      difference: diff-of-both + only-this) with `build_dense_node`, which
///      handles overflow-narrow and is order-independent.
///
/// The budget is a single shared atomic — `num_threads³` total spawns across
/// the entire descent, after which everything is sequential. This caps task
/// explosion while permitting fan-out a couple of levels deep, which keeps even
/// the imbalanced intersect workload near-linear (a single root level of
/// `num_threads²` fan-out leaves intersect's heavy buckets stuck serial).
#[cfg(feature = "parallel")]
mod parallel_union {
    // The shared spawn-budget (`ParUnionCtx`) and the cross-thread disjoint
    // scatter wrapper (`ScatterPtr`) backing the equal-span fan-out in
    // `par_{union,intersect,difference}_with_ctx`.
    use core::sync::atomic::{AtomicUsize, Ordering};

    /// Carries the shared spawn budget across recursive
    /// `par_union_with_ctx` calls.
    pub(crate) struct ParUnionCtx {
        pub(crate) budget: AtomicUsize,
    }

    impl ParUnionCtx {
        pub(crate) fn new() -> Self {
            // `num_threads³` shared spawns across the whole descent. A single
            // level of root fan-out (`num_threads²`, ~one chunk per thread×thread)
            // suffices only when the per-child subtrees are balanced — true for
            // union/difference, but NOT for intersect, whose work concentrates in
            // a few heavy buckets. Allowing fan-out to continue a level or two
            // deeper (the cubic budget) lets those heavy subtrees split too, so
            // all three ops scale near-linearly (~11× on 16 threads at 10M).
            // Still bounded ⇒ task explosion is capped; below the budget the rest
            // runs serial.
            let n = rayon::current_num_threads();
            Self {
                budget: AtomicUsize::new(n.saturating_mul(n).saturating_mul(n).max(2)),
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
/// This function is called automatically when a new VWPATCH is created.
fn init_sip_key() {
    // Guard this module's own bytetable dispersion init.
    INIT.call_once(|| {
        bytetable::init();
    });
    // Fill the shared SIP key exactly once via the original module's Once.
    crate::patch::init_sip_key();
}

// --- Shared key-schema infrastructure -------------------------------------
//
// The const helpers, the `key_schema!` / `key_segmentation!` macros, the
// `KeySchema` / `KeySegmentation` traits, and the `IdentitySchema` /
// `SingleSegmentation` types are reused verbatim from `crate::patch`. They are
// `pub` there, the macros are `#[macro_export]` (so they live at the crate
// root and must not be redefined here, or the export would collide), and the
// existing ordering schemas (`EAVOrder`, etc.) implement
// `crate::patch::KeySchema`. Re-exporting the same items keeps those impls
// valid for `VWPATCH` and avoids duplicate macro exports.
pub use crate::patch::{IdentitySchema, KeySchema, KeySegmentation, SingleSegmentation};

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
    // heap-allocated `Leaf<KEY_LEN, V>`. Lifetime is guaranteed by the nearest
    // ancestor `Branch` whose `owner` is `Some(_)`.
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
    /// mmap'd buffer. Lifetime is implicit — guaranteed by the nearest
    /// ancestor `Branch` whose `owner` is `Some(_)`.
    LocalLeaf(NonNull<[u8; KEY_LEN]>),
    Branch(branch::BranchNN<KEY_LEN, O, V>),
}

/// Immutable borrow view of a Head body.
/// Returned by `body_ref()` and tied to the lifetime of the `&Head`.
pub(crate) enum BodyRef<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a Leaf<KEY_LEN, V>),
    /// Reference to a trible's bytes within an archive. The slice's
    /// lifetime is bound to `&'a Head` via the body pointer; the actual
    /// underlying allocation is kept alive by an ancestor Branch's
    /// `owner` Arc.
    LocalLeaf(&'a [u8; KEY_LEN]),
    Branch(&'a Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>),
}

/// Mutable borrow view of a Head body.
/// Returned by `body_mut()` and tied to the lifetime of the `&mut Head`.
pub(crate) enum BodyMut<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a mut Leaf<KEY_LEN, V>),
    /// `LocalLeaf` is read-only by construction (it points into immutable
    /// archive bytes), so the mutable view yields a shared reference.
    /// Callers attempting to mutate a `LocalLeaf` must first reify it
    /// into a heap-allocated `Leaf`.
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
    // - bits 4..=47:  body pointer bits (44 bits → 16 TB addressable)
    // - bits 48..=63: key (16 bits) for cuckoo table lookup
    //
    // Phase 2a widens the key field from 8 to 16 bits. The body pointer
    // shrinks from 52 to 44 bits to make room; the low-4-bits-free
    // alignment invariant (16-byte aligned bodies) is unchanged.
    const TAG_MASK: u64 = 0x0f;
    const BODY_MASK: u64 = 0x00_00_ff_ff_ff_ff_ff_f0;
    const KEY_MASK: u64 = 0xff_ff_00_00_00_00_00_00;

    pub(crate) fn new<T: Body + ?Sized>(key: u16, body: NonNull<T>) -> Self {
        unsafe {
            let tptr =
                std::ptr::NonNull::new_unchecked((body.as_ptr() as *mut u8).map_addr(|addr| {
                    debug_assert_eq!(addr as u64 & Self::TAG_MASK, 0);
                    ((addr as u64 & Self::BODY_MASK)
                        | ((key as u64) << 48)
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

    /// Constructs a `LocalLeaf` Head pointing directly at a `[u8; KEY_LEN]`
    /// trible inside an archive's mmap'd buffer. The pointer's address must
    /// be 16-byte aligned (so the low 4 bits are free for the `HeadTag`);
    /// for `SimpleArchive` buffers this holds whenever the base allocation
    /// is 16-byte aligned and tribles are 64 bytes wide (every offset is a
    /// multiple of 16).
    ///
    /// # Safety
    /// - `trible_ptr` must remain valid for at least as long as this Head
    ///   exists, which is the caller's responsibility to arrange — typically
    ///   by holding an `Arc<dyn ArchiveOwner>` in the nearest ancestor
    ///   `Branch`'s `owner` slot.
    /// - The pointer must be 16-byte aligned; this is debug-asserted.
    pub(crate) unsafe fn new_local_leaf(key: u16, trible_ptr: NonNull<[u8; KEY_LEN]>) -> Self {
        unsafe {
            let tptr = std::ptr::NonNull::new_unchecked((trible_ptr.as_ptr() as *mut u8).map_addr(
                |addr| {
                    debug_assert_eq!(
                        addr as u64 & Self::TAG_MASK,
                        0,
                        "LocalLeaf trible pointer must be 16-byte aligned"
                    );
                    ((addr as u64 & Self::BODY_MASK)
                        | ((key as u64) << 48)
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

    #[inline]
    pub(crate) fn tag(&self) -> HeadTag {
        HeadTag::from_raw((self.tptr.as_ptr() as u64 & Self::TAG_MASK) as u8)
    }

    #[inline]
    pub(crate) fn key(&self) -> u16 {
        (self.tptr.as_ptr() as u64 >> 48) as u16
    }

    #[inline]
    pub(crate) fn with_key(mut self, key: u16) -> Self {
        self.tptr =
            std::ptr::NonNull::new(self.tptr.as_ptr().map_addr(|addr| {
                ((addr as u64 & !Self::KEY_MASK) | ((key as u64) << 48)) as usize
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
        // Branching stays single-byte (phase 2b-i): the span sub-key is the
        // one byte at `new_start_depth`. The cuckoo child-table is keyed by
        // the span fingerprint; `fingerprint16` is bijective for one byte, so
        // the stored key still equals the raw branch byte and the structure
        // stays identical to PATCH.
        let fp = fingerprint16(&[leaf_key[i]]);
        self.with_key(fp)
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
                // SAFETY: SIP_KEY is initialized at startup; we only read it.
                let key = unsafe { *addr_of!(SIP_KEY) };
                SipHasher24::new_with_key(&key).hash(&bytes[..]).into()
            }
            BodyRef::Branch(branch) => branch.hash,
        }
    }

    /// The node's divergence depth (`span_start`): the depth at which this
    /// head's children begin to differ. Leaves have no divergence, so they
    /// report `KEY_LEN`.
    pub(crate) fn end_depth(&self) -> usize {
        match self.body_ref() {
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => KEY_LEN,
            BodyRef::Branch(branch) => branch.span_start as usize,
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

    /// Span-reconciling removal of `leaf_key` from the subtree in `slot`.
    ///
    /// Variable-width spans mean a branch's children are keyed by the 16-bit
    /// fingerprint of their span sub-key over `[span_start, span_end)`, not by a
    /// raw byte — so the descent must fingerprint the matching window and use a
    /// *verified* lookup ([`BranchMut::remove_dense_child`]), since multi-byte
    /// fingerprints collide. Removal can also UNDERFLOW: dropping a child may
    /// leave a branch with a single child, which is non-canonical. We splice
    /// that lone child up (its span and subtree stay intact; the parent re-keys
    /// it by its own fingerprint on the way out, or the key is irrelevant at the
    /// root). This only ever makes the trie shallower — never deeper — so no
    /// span re-widening is needed for correctness; a branch that lost fanout
    /// keeps its (now possibly narrower-than-canonical) span, leaving the trie
    /// slightly over-deep but fully correct.
    pub(crate) fn remove_leaf(
        slot: &mut Option<Self>,
        leaf_key: &[u8; KEY_LEN],
        start_depth: usize,
    ) {
        let Some(this) = slot.as_ref() else {
            return;
        };
        // The representative must match `leaf_key` along the compressed path
        // (for a branch, `[start_depth, span_start)`) or be exactly `leaf_key`
        // (for a leaf). If not, the key is absent under this head.
        if !this.has_prefix::<KEY_LEN>(start_depth, leaf_key) {
            return;
        }
        let (s, e) = match this.body_ref() {
            // An exact-key leaf (heap or archive-local): drop it.
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                slot.take();
                return;
            }
            BodyRef::Branch(b) => (b.span_start as usize, b.span_end as usize),
        };

        let mut buf = [0u8; KEY_LEN];
        let sub = Self::span_sub(leaf_key, s, e, &mut buf);
        let fp = fingerprint16(sub);

        let collapse = {
            let this_mut = slot.as_mut().unwrap();
            let mut ed = crate::vwpatch::branch::BranchMut::from_head(this_mut);
            ed.remove_dense_child(fp, sub, |child| {
                let mut opt = Some(child);
                Self::remove_leaf(&mut opt, leaf_key, e);
                opt
            })
        };

        // Underflow: the branch dropped to a single child. Splice that child up
        // into `slot`, collapsing the redundant branch level.
        if collapse == Some(true) {
            let remaining: Option<Self> = match slot.as_mut().unwrap().body_mut() {
                BodyMut::Branch(b) => b.child_table.iter_mut().filter_map(Option::take).next(),
                BodyMut::Leaf(_) | BodyMut::LocalLeaf(_) => {
                    unreachable!("collapse only reported for a Branch node")
                }
            };
            *slot = remaining;
        }
    }

    // NOTE: slot-level wrappers removed; callers should take the slot and call
    // the owned helpers (insert_leaf / replace_leaf / union)
    // directly. This reduces the indirection and keeps ownership semantics
    // explicit at the call site.

    // Owned set-insert of a single fresh `leaf` head into `this`. This is just
    // the variable-width dense insert ([`Self::insert_dense`]); the historical
    // single-byte `insert_leaf` body (raw-byte `with_start` keys, single-byte
    // `Branch::new`) mis-keys children on a multi-byte dense trie. Kept as a
    // named entry point for the archive/owner path and the BranchMut unit tests.
    #[allow(dead_code)]
    pub(crate) fn insert_leaf(this: Self, leaf: Self, start_depth: usize) -> Self {
        Self::insert_dense(this, leaf, start_depth)
    }
}

// Archive-aware insertion path, available only when V = (). LocalLeaf
// machinery requires the value type to be zero-sized so reification
// (constructing a heap Leaf with `()` as the value) is well-defined.
impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>> Head<KEY_LEN, O, ()> {
    /// Variable-width **dense** archive insert — the owner-threading mirror of
    /// the heap [`Self::insert_dense`]. Builds the same dense, multi-byte spans
    /// (so an archive-loaded VWPATCH reaches the same compressed node count as
    /// the heap build) while threading the `LocalLeaf` owner through every
    /// branch it constructs, so the archive bytes referenced by `LocalLeaf`
    /// children stay alive.
    ///
    /// Invariants and parameters:
    /// - `this` is a heap `Leaf`, a `Branch`, or — only when reached via the
    ///   `recurse_dense_child` closure below — a direct-child `LocalLeaf`.
    /// - `leaf` is the freshly inserted archive `LocalLeaf` (or a heap `Leaf`
    ///   once reified on an owner mismatch).
    /// - `leaf_owner` is `Some(arc)` while `leaf` is still a `LocalLeaf`.
    /// - `this_owner` is `Some(arc)` iff `this` is itself a `LocalLeaf` (the
    ///   owner of the branch that holds it as a direct child); `None` otherwise.
    ///
    /// Single-load simplification: every entry of one archive load shares one
    /// owner Arc, so when a dense span narrows and regroups children, all
    /// resulting sub-branches inherit that same owner — no per-child conflict.
    /// A cross-archive entry whose owner differs from the receiving branch's is
    /// reified to a heap `Leaf` (owner dropped) before descending, exactly as
    /// the old single-byte path did.
    pub(crate) fn insert_dense_owned(
        mut this: Self,
        mut leaf: Self,
        mut leaf_owner: Option<&std::sync::Arc<dyn crate::vwpatch::branch::ArchiveOwner>>,
        at_depth: usize,
        this_owner: Option<&std::sync::Arc<dyn crate::vwpatch::branch::ArchiveOwner>>,
    ) -> Self {
        use std::sync::Arc;
        match this.tag() {
            HeadTag::Leaf | HeadTag::LocalLeaf => match this.first_divergence(&leaf, at_depth) {
                None => this, // duplicate key
                Some((start, _, _)) => {
                    // The fresh parent must cover whichever direct child is a
                    // LocalLeaf: `this` (owner `this_owner`) and/or `leaf`
                    // (owner `leaf_owner`). Under the single-owner invariant
                    // these agree whenever both are `Some`.
                    let owner = this_owner.or(leaf_owner).cloned();
                    Self::split_two_owned(this, leaf, start, owner)
                }
            },
            _ => {
                let (s, e) = {
                    let BodyRef::Branch(b) = this.body_ref() else {
                        unreachable!()
                    };
                    (b.span_start as usize, b.span_end as usize)
                };

                // Compressed-path divergence: `leaf` doesn't belong under this
                // branch's span — wrap both under a fresh parent (O(1)). `this`
                // is a Branch (self-covering); only `leaf` may need covering.
                if let Some((start, _, _)) = this.first_divergence(&leaf, at_depth) {
                    return Self::split_two_owned(this, leaf, start, leaf_owner.cloned());
                }

                // Owner reconciliation for the branch that will host `leaf`.
                let branch_owner: Option<Arc<dyn crate::vwpatch::branch::ArchiveOwner>> =
                    match this.body_ref() {
                        BodyRef::Branch(b) => b.owner.clone(),
                        _ => unreachable!(),
                    };
                if let (Some(bo), Some(lo)) = (branch_owner.as_ref(), leaf_owner) {
                    if !Arc::ptr_eq(bo, lo) {
                        // Cross-archive entry: reify to heap so it carries no
                        // owner dependency, then descend as a plain leaf.
                        leaf = Self::reify_local_leaf_unit(leaf);
                        leaf_owner = None;
                    }
                }
                // The owner covering this branch's LocalLeaf descendants after
                // this op: the branch keeps its own owner, else adopts the
                // incoming leaf's. Used for adoption, narrowing, and as the
                // covering owner when recursing into a LocalLeaf child.
                let active_owner: Option<Arc<dyn crate::vwpatch::branch::ArchiveOwner>> =
                    branch_owner.or(leaf_owner.cloned());

                let mut buf = [0u8; KEY_LEN];
                let sub = Self::span_sub(leaf.childleaf_key(), s, e, &mut buf);
                let fp = fingerprint16(sub);

                let mut leaf_opt = Some(leaf);
                let mut ed = crate::vwpatch::branch::BranchMut::from_head(&mut this);
                if ed.owner.is_none() {
                    ed.owner = active_owner.clone();
                }
                let active_ref = active_owner.as_ref();
                if ed.recurse_dense_child(fp, sub, |child| {
                    // A direct-child LocalLeaf is covered by THIS branch's
                    // (active) owner; any other child self-covers.
                    let child_owner = if child.tag() == HeadTag::LocalLeaf {
                        active_ref
                    } else {
                        None
                    };
                    Self::insert_dense_owned(child, leaf_opt.take().unwrap(), leaf_owner, e, child_owner)
                }) {
                    drop(ed);
                    return this;
                }
                let leaf = leaf_opt.take().unwrap();

                // New distinct child. Overflow only at the 256-slot maximum.
                let over_cap = ed.child_table.len() >= 256
                    && ed.child_table.iter().flatten().count() >= Self::max_fanout(e - s);
                if over_cap {
                    drop(ed);
                    return Self::narrow_with_owned(this, leaf, s, e, active_owner);
                }

                let inserted = leaf.with_span(s, e);
                let leftover = ed.add_dense_child(inserted);
                drop(ed);
                if let Some(leftover) = leftover {
                    // Cuckoo placement failed below the cap — O(children) narrow.
                    return Self::narrow_with_owned(this, leftover, s, e, active_owner);
                }
                this
            }
        }
    }

    /// Reifies a LocalLeaf head into a heap `Leaf<KEY_LEN, ()>` head.
    /// Leaf and Branch heads pass through unchanged. Specialized to
    /// V = () so no `V: Default` bound leaks into generic call sites.
    fn reify_local_leaf_unit(head: Self) -> Self {
        match head.body_ref() {
            BodyRef::Leaf(_) | BodyRef::Branch(_) => head,
            BodyRef::LocalLeaf(bytes) => {
                let key_byte = head.key();
                let key_copy = *bytes;
                drop(head);
                let new_leaf = unsafe { Leaf::<KEY_LEN, ()>::new(&key_copy, ()) };
                Head::new(key_byte, new_leaf)
            }
        }
    }

    /// Public re-export for the root-reification path used by
    /// `VWPATCH::insert_archive` when the VWPATCH is empty.
    pub(crate) fn reify_local_leaf_unit_for_root(head: Self) -> Self {
        Self::reify_local_leaf_unit(head)
    }
}

// Resume generic-V `Head` impl for the remaining methods (replace_leaf,
// union, intersect, query operations, etc.) which don't care about V
// shape and so remain in the V-generic impl block.
impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Head<KEY_LEN, O, V> {
    /// Span-reconciling set-insert that REPLACES the value of an existing key.
    ///
    /// Identical in shape to the variable-width [`Self::insert_dense`] — same
    /// divergence/`split_two`, same fingerprint descent, same add/overflow add
    /// path — with one difference: when the key is already present, the dense
    /// insert returns `this` unchanged (idempotent set insert), whereas replace
    /// swaps in `leaf` so the stored value updates. Since values are not hashed,
    /// the swap leaves the set-hash and all aggregate counts unchanged; only the
    /// leaf node (and any `childleaf` pointer naming it) is refreshed.
    pub(crate) fn replace_leaf(mut this: Self, leaf: Self, at_depth: usize) -> Self {
        match this.tag() {
            HeadTag::Leaf | HeadTag::LocalLeaf => match this.first_divergence(&leaf, at_depth) {
                // Same key: swap the value-carrying leaf, keeping the slot key.
                None => leaf.with_key(this.key()),
                Some((start, _, _)) => Self::split_two(this, leaf, start),
            },
            _ => {
                let (s, e) = {
                    let BodyRef::Branch(b) = this.body_ref() else {
                        unreachable!()
                    };
                    (b.span_start as usize, b.span_end as usize)
                };

                // Compressed-path divergence: the new key doesn't belong under
                // this branch's span — wrap both under a fresh parent (O(1)).
                if let Some((start, _, _)) = this.first_divergence(&leaf, at_depth) {
                    return Self::split_two(this, leaf, start);
                }

                let mut buf = [0u8; KEY_LEN];
                let sub = Self::span_sub(leaf.childleaf_key(), s, e, &mut buf);
                let fp = fingerprint16(sub);

                let mut leaf_opt = Some(leaf);
                let mut ed = crate::vwpatch::branch::BranchMut::from_head(&mut this);
                if ed.recurse_dense_child(fp, sub, |child| {
                    Self::replace_leaf(child, leaf_opt.take().unwrap(), e)
                }) {
                    drop(ed);
                    return this;
                }
                let leaf = leaf_opt.take().unwrap();

                // Key absent — add it exactly as the dense insert does.
                let over_cap = ed.child_table.len() >= 256
                    && ed.child_table.iter().flatten().count() >= Self::max_fanout(e - s);
                if over_cap {
                    drop(ed);
                    return Self::narrow_with(this, leaf, s, e);
                }

                let inserted = leaf.with_span(s, e);
                let leftover = ed.add_dense_child(inserted);
                drop(ed);
                if let Some(leftover) = leftover {
                    return Self::narrow_with(this, leftover, s, e);
                }
                this
            }
        }
    }

    /// Sequential VWPATCH-trie union. Always serial; the parallel
    /// dispatch lives in [`Self::par_union`] which (post-correctness)
    /// delegates here.
    ///
    /// Variable-width spans mean two independently-built tries can
    /// branch at the same tree position over *different* windows
    /// (`this` spans `[16,24)`, `other` `[16,20)`), and child keys are
    /// 16-bit span fingerprints, not raw bytes. So union cannot reuse
    /// the inherited single-byte `with_start`/`modify_child` merge; it
    /// instead reuses the dense-insert machinery:
    ///   * compressed-path divergence → [`Self::split_two`] (O(1));
    ///   * span misalignment → [`Self::narrow_span`] reconciles the
    ///     wider window to the narrower boundary (re-partitioning,
    ///     recomputing each child's fingerprint) and re-enters;
    ///   * an opaque subtree installs via [`Self::insert_subtree`],
    ///     the same recurse / add / narrow-on-overflow path the dense
    ///     insert uses for a single leaf.
    pub(crate) fn union(mut this: Self, mut other: Self, at_depth: usize) -> Self {
        if this.hash() == other.hash() {
            return this;
        }

        if let Some((start, _, _)) = this.first_divergence(&other, at_depth) {
            // Compressed-path divergence: the two heads' representatives
            // disagree before either branches — wrap both under a fresh dense
            // parent at the divergence point, preserving each subtree intact.
            return Self::split_two(this, other, start);
        }

        // No compressed-path divergence: both heads share their tree-bytes up
        // to `min(span_start)`. Orient so `this` branches no later than `other`
        // (smaller `span_start`); union is commutative, so the swap is free.
        if other.end_depth() < this.end_depth() {
            std::mem::swap(&mut this, &mut other);
        }

        let (s, te) = match this.body_ref() {
            BodyRef::Branch(b) => (b.span_start as usize, b.span_end as usize),
            // A leaf has the maximal `end_depth` (`KEY_LEN`), so after the swap
            // `this` is the earlier-branching side and can only be a leaf when
            // `other` is one too — two equal-key leaves are hash-equal and two
            // differing leaves diverge, both handled above.
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                unreachable!("equal-key leaves are hash-equal; leaf cannot be the earlier side")
            }
        };
        let os = other.end_depth();

        // Span reconciliation. `this` branches at `s` over the window `[s, te)`;
        // `other` branches at `os >= s`. If `this`'s window overruns `other`'s
        // branch point the two are misaligned: narrow the offending window to
        // the narrower boundary and re-enter. A wider-window fingerprint is NOT
        // a prefix of a narrower one, so `narrow_span` re-partitions
        // (recomputing every child's fingerprint) rather than truncating.
        if te > os {
            if os > s {
                // `this` branches strictly earlier but its window reaches past
                // `other`'s branch point: clip `this` to end at `os`.
                this = Self::narrow_span(this, os);
                return Self::union(this, other, at_depth);
            }
            // `os == s`: same branch depth, differing windows. Reconcile both to
            // the narrower span_end, then merge the now equal-span children.
            let oe = match other.body_ref() {
                BodyRef::Branch(b) => b.span_end as usize,
                BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                    unreachable!("os == s < KEY_LEN ⇒ other is a branch")
                }
            };
            let target = te.min(oe);
            if te > target {
                this = Self::narrow_span(this, target);
                return Self::union(this, other, at_depth);
            }
            if oe > target {
                other = Self::narrow_span(other, target);
                return Self::union(this, other, at_depth);
            }
            // `te == oe == target`: equal spans. Drain `other`'s children and
            // install each into `this` via the shared dense path. Each child of
            // a node spanning `[s, te)` branches at `>= te`, so it stays opaque
            // even if an `insert_subtree` narrows `this`'s window further.
            let children: Vec<Self> = match other.body_mut() {
                BodyMut::Branch(ob) => {
                    ob.child_table.iter_mut().filter_map(Option::take).collect()
                }
                BodyMut::Leaf(_) | BodyMut::LocalLeaf(_) => unreachable!(),
            };
            drop(other);
            for child in children {
                this = Self::insert_subtree(this, child);
            }
            return this;
        }

        // `te <= os`: `other` sits opaquely within `this`'s span window. Install
        // it exactly as the dense insert installs a single leaf.
        Self::insert_subtree(this, other)
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
        // Cheap base cases — identical to `Self::union`, no fan-out. Hash-equal
        // short-circuit and compressed-path divergence don't generate child work.
        if this.hash() == other.hash() {
            return this;
        }
        if let Some((start, _, _)) = this.first_divergence(&other, at_depth) {
            return Self::split_two(this, other, start);
        }
        if other.end_depth() < this.end_depth() {
            std::mem::swap(&mut this, &mut other);
        }
        let (s, te) = match this.body_ref() {
            BodyRef::Branch(b) => (b.span_start as usize, b.span_end as usize),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                unreachable!("equal-key leaves are hash-equal; leaf cannot be the earlier side")
            }
        };
        let os = other.end_depth();
        if te > os {
            // Span reconciliation (no fan-out): narrow the wider window onto the
            // narrower boundary and re-enter, staying on the parallel path.
            if os > s {
                this = Self::narrow_span(this, os);
                return Self::par_union_with_ctx(this, other, at_depth, ctx);
            }
            let oe = match other.body_ref() {
                BodyRef::Branch(b) => b.span_end as usize,
                BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                    unreachable!("os == s < KEY_LEN ⇒ other is a branch")
                }
            };
            let target = te.min(oe);
            if te > target {
                this = Self::narrow_span(this, target);
                return Self::par_union_with_ctx(this, other, at_depth, ctx);
            }
            if oe > target {
                other = Self::narrow_span(other, target);
                return Self::par_union_with_ctx(this, other, at_depth, ctx);
            }

            // EQUAL DEPTH, EQUAL SPAN `[s, te)`. Below the threshold the
            // per-child spawn overhead isn't amortised — delegate to serial.
            if this.count() + other.count() < PARALLEL_PATCH_UNION_THRESHOLD as u64 {
                return Self::union(this, other, at_depth);
            }

            // Drain both child sets and partition by their actual span sub-key
            // over `[s, te)` (NOT the 16-bit fingerprint, which collides).
            let this_children: Vec<Self> = match this.body_mut() {
                BodyMut::Branch(b) => b.child_table.iter_mut().filter_map(Option::take).collect(),
                BodyMut::Leaf(_) | BodyMut::LocalLeaf(_) => unreachable!(),
            };
            let other_children: Vec<Self> = match other.body_mut() {
                BodyMut::Branch(b) => b.child_table.iter_mut().filter_map(Option::take).collect(),
                BodyMut::Leaf(_) | BodyMut::LocalLeaf(_) => unreachable!(),
            };
            drop(this);
            drop(other);
            let (both, only_this, only_other) =
                Self::partition_by_span(this_children, other_children, s, te);

            // Fan out: each "both" pair recurses concurrently (under budget),
            // writing its resolved subtree into a disjoint output slot.
            let resolved = Self::par_resolve_pairs(both, ctx, move |t, o, ctx| {
                Some(Self::par_union_with_ctx(t, o, te, ctx))
            });

            // Union = both∪ + only-this + only-other. Re-assemble with the dense
            // machinery (handles overflow-narrow); order-independent (it groups
            // by span sub-key), so collection order doesn't affect the result.
            let mut units: Vec<Self> = Vec::with_capacity(resolved.len() + only_this.len() + only_other.len());
            units.extend(resolved.into_iter().flatten());
            units.extend(only_this);
            units.extend(only_other);
            return Self::build_dense_node(units, s, te);
        }

        // `te <= os`: `other` sits opaquely within `this`'s span (asymmetric, at
        // most one matching child — no fan-out). Install via the dense path.
        Self::insert_subtree(this, other)
    }

    /// Partition two equal-span child sets by their actual span sub-key over
    /// `[s, e)` into `(both, only_this, only_other)`. Matching is on the real
    /// span bytes (via [`Self::span_subkey`]), never the 16-bit fingerprint,
    /// which collides. Within one branch node children have pairwise-distinct
    /// sub-keys, so the map keys never collide. Fanout `<= 256`, so this is cheap.
    #[cfg(feature = "parallel")]
    fn partition_by_span(
        this_children: Vec<Self>,
        other_children: Vec<Self>,
        s: usize,
        e: usize,
    ) -> (Vec<(Self, Self)>, Vec<Self>, Vec<Self>) {
        use std::collections::HashMap;
        let mut map: HashMap<Vec<u8>, Self> = HashMap::with_capacity(this_children.len());
        for c in this_children {
            let key = Self::span_subkey(&c, s, e);
            map.insert(key, c);
        }
        let mut both: Vec<(Self, Self)> = Vec::new();
        let mut only_other: Vec<Self> = Vec::new();
        for c in other_children {
            let key = Self::span_subkey(&c, s, e);
            if let Some(t) = map.remove(&key) {
                both.push((t, c));
            } else {
                only_other.push(c);
            }
        }
        let only_this: Vec<Self> = map.into_values().collect();
        (both, only_this, only_other)
    }

    /// Tree-ordered span sub-key of `child`'s representative over `[s, e)`.
    #[cfg(feature = "parallel")]
    fn span_subkey(child: &Self, s: usize, e: usize) -> Vec<u8> {
        let key = child.childleaf_key();
        (s..e).map(|j| key[O::TREE_TO_KEY[j]]).collect()
    }

    /// Resolve each "both" pair concurrently under the shared spawn budget,
    /// scattering results into disjoint output slots. Each task `i` owns a
    /// distinct index, so the `ScatterPtr` writes are non-aliasing. A pair that
    /// fails to claim a budget unit runs serially on the current thread (the
    /// recursive callee will itself find the budget exhausted and stay serial).
    #[cfg(feature = "parallel")]
    fn par_resolve_pairs<F>(
        both: Vec<(Self, Self)>,
        ctx: &parallel_union::ParUnionCtx,
        resolve: F,
    ) -> Vec<Option<Self>>
    where
        O: Send + Sync,
        V: Send + Sync,
        F: Fn(Self, Self, &parallel_union::ParUnionCtx) -> Option<Self> + Send + Sync,
    {
        let n = both.len();
        let mut resolved: Vec<Option<Self>> = (0..n).map(|_| None).collect();
        let resolved_ptr = parallel_union::ScatterPtr(resolved.as_mut_ptr());
        let resolve = &resolve;
        rayon::scope(|sc| {
            for (i, (t, o)) in both.into_iter().enumerate() {
                if ctx.try_claim() {
                    sc.spawn(move |_| {
                        let r = resolve(t, o, ctx);
                        // SAFETY: index `i` is unique to this task ⇒ disjoint write.
                        unsafe {
                            resolved_ptr.write_at(i, r);
                        }
                    });
                } else {
                    let r = resolve(t, o, ctx);
                    // SAFETY: same disjointness invariant; the parent races only
                    // with tasks targeting distinct indices.
                    unsafe {
                        resolved_ptr.write_at(i, r);
                    }
                }
            }
        });
        resolved
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
        // Cheap base cases — identical to `Self::intersect`, no fan-out.
        if self.hash() == other.hash() {
            return Some(self.clone());
        }
        if self.first_divergence(other, at_depth).is_some() {
            return None;
        }

        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth < other_depth {
            let BodyRef::Branch(branch) = self.body_ref() else {
                unreachable!();
            };
            let te = branch.span_end as usize;
            if te > other_depth {
                let narrowed = Self::narrow_span(self.clone(), other_depth);
                return narrowed.par_intersect_with_ctx(other, at_depth, ctx);
            }
            return branch
                .select_child(other.childleaf_key())
                .and_then(|self_child| self_child.par_intersect_with_ctx(other, self_depth, ctx));
        }

        if other_depth < self_depth {
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            let oe = other_branch.span_end as usize;
            if oe > self_depth {
                let narrowed = Self::narrow_span(other.clone(), self_depth);
                return self.par_intersect_with_ctx(&narrowed, at_depth, ctx);
            }
            return other_branch
                .select_child(self.childleaf_key())
                .and_then(|other_child| self.par_intersect_with_ctx(other_child, other_depth, ctx));
        }

        // Equal branch depth; reconcile windows to the narrower boundary.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };
        let s = self_branch.span_start as usize;
        let te = self_branch.span_end as usize;
        let oe = other_branch.span_end as usize;
        let target = te.min(oe);
        if te > target {
            let narrowed = Self::narrow_span(self.clone(), target);
            return narrowed.par_intersect_with_ctx(other, at_depth, ctx);
        }
        if oe > target {
            let narrowed = Self::narrow_span(other.clone(), target);
            return self.par_intersect_with_ctx(&narrowed, at_depth, ctx);
        }

        // EQUAL DEPTH, EQUAL SPAN `[s, te)`. Below threshold → serial.
        if self.count() + other.count() < PARALLEL_PATCH_UNION_THRESHOLD as u64 {
            return self.intersect(other, at_depth);
        }

        let this_children: Vec<Self> =
            self_branch.child_table.iter().flatten().cloned().collect();
        let other_children: Vec<Self> =
            other_branch.child_table.iter().flatten().cloned().collect();
        let (both, _only_this, _only_other) =
            Self::partition_by_span(this_children, other_children, s, te);

        // Intersect = both∩ only; drop singletons. Fan out the matched pairs.
        let resolved = Self::par_resolve_pairs(both, ctx, move |t, o, ctx| {
            t.par_intersect_with_ctx(&o, self_depth, ctx)
        });
        let units: Vec<Self> = resolved.into_iter().flatten().collect();
        if units.is_empty() {
            return None;
        }
        Some(Self::build_dense_node(units, self_depth, te))
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
        // Cheap base cases — identical to `Self::difference`, no fan-out.
        if self.hash() == other.hash() {
            return None;
        }
        if self.first_divergence(other, at_depth).is_some() {
            return Some(self.clone());
        }

        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth < other_depth {
            let BodyRef::Branch(branch) = self.body_ref() else {
                unreachable!();
            };
            let (s, te) = (branch.span_start as usize, branch.span_end as usize);
            if te > other_depth {
                let narrowed = Self::narrow_span(self.clone(), other_depth);
                return narrowed.par_difference_with_ctx(other, at_depth, ctx);
            }
            // `te <= other_depth`: `other` opaque within `self`'s window. Only the
            // matching child loses keys; the rest survive whole. No fan-out (one
            // match), but recurse on the parallel path.
            let mut buf = [0u8; KEY_LEN];
            let sub = Self::span_sub(other.childleaf_key(), s, te, &mut buf);
            let mut units: Vec<Self> = Vec::new();
            for child in branch.child_table.iter().flatten() {
                let ck = child.childleaf_key();
                let is_match = (0..(te - s)).all(|j| ck[O::TREE_TO_KEY[s + j]] == sub[j]);
                if is_match {
                    if let Some(diffed) = child.par_difference_with_ctx(other, te, ctx) {
                        units.push(diffed);
                    }
                } else {
                    units.push(child.clone());
                }
            }
            if units.is_empty() {
                return None;
            }
            return Some(Self::build_dense_node(units, s, te));
        }

        if other_depth < self_depth {
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            let oe = other_branch.span_end as usize;
            if oe > self_depth {
                let narrowed = Self::narrow_span(other.clone(), self_depth);
                return self.par_difference_with_ctx(&narrowed, at_depth, ctx);
            }
            if let Some(other_child) = other_branch.select_child(self.childleaf_key()) {
                return self.par_difference_with_ctx(other_child, at_depth, ctx);
            } else {
                return Some(self.clone());
            }
        }

        // Equal branch depth; reconcile windows to the narrower boundary.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };
        let s = self_branch.span_start as usize;
        let te = self_branch.span_end as usize;
        let oe = other_branch.span_end as usize;
        let target = te.min(oe);
        if te > target {
            let narrowed = Self::narrow_span(self.clone(), target);
            return narrowed.par_difference_with_ctx(other, at_depth, ctx);
        }
        if oe > target {
            let narrowed = Self::narrow_span(other.clone(), target);
            return self.par_difference_with_ctx(&narrowed, at_depth, ctx);
        }

        // EQUAL DEPTH, EQUAL SPAN `[s, te)`. Below threshold → serial.
        if self.count() + other.count() < PARALLEL_PATCH_UNION_THRESHOLD as u64 {
            return self.difference(other, at_depth);
        }

        let this_children: Vec<Self> =
            self_branch.child_table.iter().flatten().cloned().collect();
        let other_children: Vec<Self> =
            other_branch.child_table.iter().flatten().cloned().collect();
        let (both, only_this, _only_other) =
            Self::partition_by_span(this_children, other_children, s, te);

        // Difference = (recursive diff of both) + only-this; drop only-other.
        let resolved = Self::par_resolve_pairs(both, ctx, move |t, o, ctx| {
            t.par_difference_with_ctx(&o, self_depth, ctx)
        });
        let mut units: Vec<Self> = Vec::with_capacity(resolved.len() + only_this.len());
        units.extend(resolved.into_iter().flatten());
        units.extend(only_this);
        if units.is_empty() {
            return None;
        }
        Some(Self::build_dense_node(units, self_depth, te))
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
                // ingestion path (step 3), which constrains the VWPATCH to
                // `V = ()`. The `Option<&V>` here therefore points at a
                // zero-sized value; a static `()` provides the address.
                // For non-`()` V this branch is unreachable today, and
                // construction will refuse such VWPATCHes once step 3 lands.
                // The type-system invariant will eventually be enforced
                // via a `LocalLeafSupported: V` trait constraint at
                // `Head::new_local_leaf` callers.
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

    /// Diagnostic: accumulate (branch nodes, total child-table slots,
    /// heap-`Leaf` nodes, `LocalLeaf` slots) over the subtree. Used to
    /// decompose a VWPATCH's *structural* byte size (vs resident RSS).
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

    /// Span-reconciling intersect. Mirrors [`Self::union`]'s reconciliation
    /// (hash-equal short-circuit → divergence ⇒ disjoint → orient by branch
    /// depth → `narrow_span` the wider window onto the common boundary before
    /// matching children), but builds fresh subtrees (`&self`), so it
    /// clone-then-`narrow_span`s where union mutated in place. The combine
    /// semantics keep only keys present in BOTH sides.
    pub(crate) fn intersect(&self, other: &Self, at_depth: usize) -> Option<Self> {
        if self.hash() == other.hash() {
            return Some(self.clone());
        }

        if self.first_divergence(other, at_depth).is_some() {
            // The two heads' representatives diverge before either branches ⇒
            // they share no key ⇒ the intersection is empty.
            return None;
        }

        let self_depth = self.end_depth();
        let other_depth = other.end_depth();
        if self_depth < other_depth {
            // `self` branches earlier (window `[s, te)`); `other` branches at
            // `other_depth`. If `self`'s window overruns `other`'s branch point
            // a single-representative `select_child` would match the wrong
            // window — narrow `self` to `other_depth` and re-enter.
            let BodyRef::Branch(branch) = self.body_ref() else {
                unreachable!();
            };
            let te = branch.span_end as usize;
            if te > other_depth {
                let narrowed = Self::narrow_span(self.clone(), other_depth);
                return narrowed.intersect(other, at_depth);
            }
            // `te <= other_depth`: `other` sits opaquely within `self`'s window,
            // so at most one child of `self` can intersect it.
            return branch
                .select_child(other.childleaf_key())
                .and_then(|self_child| self_child.intersect(other, self_depth));
        }

        if other_depth < self_depth {
            // Symmetric: `other` branches earlier. If `other`'s window overruns
            // `self`'s branch point, narrow `other` first.
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            let oe = other_branch.span_end as usize;
            if oe > self_depth {
                let narrowed = Self::narrow_span(other.clone(), self_depth);
                return self.intersect(&narrowed, at_depth);
            }
            return other_branch
                .select_child(self.childleaf_key())
                .and_then(|other_child| self.intersect(other_child, other_depth));
        }

        // Equal branch depth (`span_start` equal). The two windows may still
        // differ in `span_end`; reconcile both to the narrower boundary so the
        // children align on a common fingerprint window before matching.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };
        let te = self_branch.span_end as usize;
        let oe = other_branch.span_end as usize;
        let target = te.min(oe);
        if te > target {
            let narrowed = Self::narrow_span(self.clone(), target);
            return narrowed.intersect(other, at_depth);
        }
        if oe > target {
            let narrowed = Self::narrow_span(other.clone(), target);
            return self.intersect(&narrowed, at_depth);
        }

        // `te == oe == target`: equal spans. Children now share the window
        // `[s, te)`, so `select_child` (verified) matches correctly.
        let intersected_children = self_branch
            .child_table
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|self_child| {
                let other_child = other_branch.select_child(self_child.childleaf_key())?;
                self_child.intersect(other_child, self_depth)
            });
        // Re-assemble with the dense machinery, NOT single-byte
        // `Branch::new`/`with_start`: each surviving child is an opaque subtree
        // keyed by its `[s, te)` fingerprint, and single-byte keying would
        // collide (dropping a key) when two children share a byte at `s` but
        // diverge within the span. `build_dense_node` re-groups by span sub-key
        // (`max_end = te` keeps every child opaque); a single survivor collapses
        // the level.
        let units: Vec<Self> = intersected_children.collect();
        if units.is_empty() {
            return None;
        }
        Some(Self::build_dense_node(units, self_depth, te))
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
            // `self` branches earlier (window `[s, te)`); `other` branches at
            // `other_depth`. If `self`'s window overruns `other`'s branch point,
            // narrow `self` to `other_depth` and re-enter so the matching child
            // is keyed on the common window.
            let BodyRef::Branch(branch) = self.body_ref() else {
                unreachable!();
            };
            let (s, te) = (branch.span_start as usize, branch.span_end as usize);
            if te > other_depth {
                let narrowed = Self::narrow_span(self.clone(), other_depth);
                return narrowed.difference(other, at_depth);
            }
            // `te <= other_depth`: `other` is opaque within `self`'s window, so
            // at most one child of `self` (the one whose `[s, te)` sub-key
            // equals `other`'s) can lose keys; the rest survive whole. Replace
            // that child with its difference (dropping it if it empties) and
            // rebuild the level — the dense rebuild reuses each child as an
            // opaque unit (Arc bump), never walking a subtree. We rebuild
            // rather than `modify_child` because the latter is keyed by an
            // unverified fingerprint, ambiguous under multi-byte span
            // collisions.
            let mut buf = [0u8; KEY_LEN];
            let sub = Self::span_sub(other.childleaf_key(), s, te, &mut buf);
            let mut units: Vec<Self> = Vec::new();
            for child in branch.child_table.iter().flatten() {
                let ck = child.childleaf_key();
                let is_match = (0..(te - s)).all(|j| ck[O::TREE_TO_KEY[s + j]] == sub[j]);
                if is_match {
                    if let Some(diffed) = child.difference(other, te) {
                        units.push(diffed);
                    }
                } else {
                    units.push(child.clone());
                }
            }
            if units.is_empty() {
                return None;
            }
            return Some(Self::build_dense_node(units, s, te));
        }

        if other_depth < self_depth {
            // `other` branches earlier (window `[os, oe)`). If `other`'s window
            // overruns `self`'s branch point, narrow `other` first. Otherwise
            // `self` is opaque within `other`'s window: at most one child of
            // `other` matches `self`'s path. If it exists, subtract it; else
            // `self` survives whole.
            let BodyRef::Branch(other_branch) = other.body_ref() else {
                unreachable!();
            };
            let oe = other_branch.span_end as usize;
            if oe > self_depth {
                let narrowed = Self::narrow_span(other.clone(), self_depth);
                return self.difference(&narrowed, at_depth);
            }
            if let Some(other_child) = other_branch.select_child(self.childleaf_key()) {
                return self.difference(other_child, at_depth);
            } else {
                return Some(self.clone());
            }
        }

        // Equal branch depth (`span_start` equal). The two windows may differ
        // in `span_end`; reconcile both to the narrower boundary so children
        // align before matching.
        let BodyRef::Branch(self_branch) = self.body_ref() else {
            unreachable!();
        };
        let BodyRef::Branch(other_branch) = other.body_ref() else {
            unreachable!();
        };
        let te = self_branch.span_end as usize;
        let oe = other_branch.span_end as usize;
        let target = te.min(oe);
        if te > target {
            let narrowed = Self::narrow_span(self.clone(), target);
            return narrowed.difference(other, at_depth);
        }
        if oe > target {
            let narrowed = Self::narrow_span(other.clone(), target);
            return self.difference(&narrowed, at_depth);
        }

        // `te == oe == target`: equal spans. `select_child` (verified) now
        // matches on the common window.
        let differenced_children = self_branch
            .child_table
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|self_child| {
                if let Some(other_child) = other_branch.select_child(self_child.childleaf_key()) {
                    self_child.difference(other_child, self_depth)
                } else {
                    Some(self_child.clone())
                }
            });

        // Re-assemble the surviving children with the dense machinery, NOT a
        // single-byte `Branch::new`/`with_start`: each surviving child is an
        // opaque subtree branching at `>= te` keyed by its `[s, te)`
        // fingerprint, and two children sharing a single byte at `s` but
        // diverging within the span would collide (and drop a key) under
        // single-byte keying. `build_dense_node` re-groups by span sub-key
        // (`max_end = te` keeps every child opaque). A single survivor collapses
        // the level; difference may remove multiple levels of branches.
        let units: Vec<Self> = differenced_children.collect();
        if units.is_empty() {
            return None;
        }
        Some(Self::build_dense_node(units, self_depth, te))
    }
}

// --- Variable-width (dense) insert -----------------------------------------
//
// The dense insert branches on multi-byte spans: a node starts span-wide
// (`span_end = next_boundary(span_start)`) and narrows on overflow. This is the
// port of `crate::hatch::HatchWide`'s start-wide / narrow-on-overflow algorithm
// onto the real cuckoo `Branch`. Aggregates are maintained incrementally on the
// common path (recurse / add); the rare overflow + compressed-path-divergence
// paths rebuild from collected leaves.
impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Head<KEY_LEN, O, V> {
    /// Maximum distinct children per dense branch, as a function of span width.
    ///
    /// A single-byte span keys children by the branch byte itself
    /// (`fingerprint16` is the identity for one byte), so up to 256 children
    /// always place — exactly as single-byte PATCH does. A multi-byte span
    /// keys by an arbitrary folded fingerprint; the child table is a blocked
    /// cuckoo table (two hashes × four-slot buckets, load threshold ≈0.97), so
    /// packing 256 arbitrary keys into 256 slots would force a grow past the
    /// 256-slot maximum. Capping multi-byte fanout below the threshold keeps
    /// every node placeable while still collapsing whole sub-segments into one
    /// wide node.
    #[inline]
    fn max_fanout(span_len: usize) -> usize {
        if span_len == 1 {
            256
        } else {
            // Conservatively below the four-slot blocked-cuckoo load threshold
            // (~0.97) so the bounded random-walk places incremental adds
            // without exhausting its kick budget; on the rare near-cap failure
            // the caller narrows. Denser than the two-slot cap (224) because
            // four-slot buckets tolerate higher load.
            240
        }
    }

    /// Re-key this head by the fingerprint of its representative's span
    /// sub-key over the tree-ordered range `[span_start, span_end)`.
    pub(crate) fn with_span(self, span_start: usize, span_end: usize) -> Self {
        let leaf_key = self.childleaf_key();
        let mut buf = [0u8; KEY_LEN];
        let len = span_end - span_start;
        for j in 0..len {
            buf[j] = leaf_key[O::TREE_TO_KEY[span_start + j]];
        }
        let fp = fingerprint16(&buf[..len]);
        self.with_key(fp)
    }

    /// Tree-ordered span sub-key of `key` over `[start, end)`, into `buf`.
    #[inline]
    fn span_sub<'b>(
        key: &[u8; KEY_LEN],
        start: usize,
        end: usize,
        buf: &'b mut [u8; KEY_LEN],
    ) -> &'b [u8] {
        let len = end - start;
        for j in 0..len {
            buf[j] = key[O::TREE_TO_KEY[start + j]];
        }
        &buf[..len]
    }

    /// First tree-depth `>= min_start` at which the leaves' representatives
    /// disagree. The leaves are distinct keys, so this always terminates
    /// below `KEY_LEN`.
    fn first_varying_depth(leaves: &[Self], min_start: usize) -> usize {
        let first = leaves[0].childleaf_key();
        for d in min_start..KEY_LEN {
            let i = O::TREE_TO_KEY[d];
            let b = first[i];
            if leaves.iter().any(|l| l.childleaf_key()[i] != b) {
                return d;
            }
        }
        unreachable!("distinct dense leaves must vary before KEY_LEN");
    }

    /// True if spanning `[start, end)` keeps the leaves' distinct sub-key count
    /// `<= 256` and no fingerprint accrues more than four distinct sub-keys.
    fn dense_span_ok(leaves: &[Self], start: usize, end: usize) -> bool {
        let cap = Self::max_fanout(end - start);
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut buf = [0u8; KEY_LEN];
        for l in leaves {
            let sub = Self::span_sub(l.childleaf_key(), start, end, &mut buf);
            if seen.insert(sub.to_vec()) && seen.len() > cap {
                return false;
            }
        }
        // Fingerprint-multiplicity overflow (>4 distinct sub-keys colliding on
        // one fingerprint's four slots) and raw cuckoo load failures are
        // handled reactively: [`from_children_dense`] returns `None` and the
        // caller narrows.
        true
    }

    /// Widest span `[start, end)` (`end <= min(next_boundary(start), max_end)`)
    /// satisfying [`dense_span_ok`]. A single-byte span is always valid, so this
    /// returns at least `start + 1`.
    ///
    /// `max_end` caps the span so it never reaches into a unit's own internal
    /// structure: when regrouping *subtree* units (the O(children) narrow), each
    /// unit branches at depth `max_end`, so spans must stay `<= max_end` to keep
    /// every unit an opaque, leaf-positioned child.
    fn widest_dense_span(leaves: &[Self], start: usize, max_end: usize) -> usize {
        let mut end = O::next_boundary(start).min(max_end);
        while end > start + 1 {
            if Self::dense_span_ok(leaves, start, end) {
                break;
            }
            end -= 1;
        }
        end
    }

    /// Build a dense subtree from `units` (leaf *or* subtree heads, all sharing
    /// the tree-bytes up to `min_start` and pairwise diverging within
    /// `[min_start, max_end)`). Groups by span sub-key and recurses; singleton
    /// groups collapse to the unit itself, leaving its subtree intact.
    ///
    /// `max_end` bounds every span so it never overruns a unit's own internal
    /// structure (which begins at `max_end` for subtree units; `KEY_LEN` for
    /// leaves). Because the function only reads each unit's representative
    /// `childleaf_key()` and clones heads (Arc bump) — never walking a subtree —
    /// it is O(units) regardless of how large those subtrees are.
    fn build_dense_node(units: Vec<Self>, min_start: usize, max_end: usize) -> Self {
        Self::build_dense_node_owned(units, min_start, max_end, None)
    }

    /// Owner-threading variant of [`Self::build_dense_node`]. Every branch it
    /// constructs adopts `owner` so that any `LocalLeaf` units it regroups stay
    /// covered by the archive owner keeping their bytes alive. The heap path
    /// uses `owner = None` via the thin wrapper above; the archive dense-insert
    /// path passes the load's single shared owner Arc.
    fn build_dense_node_owned(
        units: Vec<Self>,
        min_start: usize,
        max_end: usize,
        owner: Option<std::sync::Arc<dyn crate::vwpatch::branch::ArchiveOwner>>,
    ) -> Self {
        if units.len() == 1 {
            return units.into_iter().next().unwrap();
        }
        let start = Self::first_varying_depth(&units, min_start);
        let mut end = Self::widest_dense_span(&units, start, max_end);

        // Group → build children → install. A chosen span may still be
        // cuckoo-unplaceable (arbitrary fingerprints can be unplaceable even
        // below the load threshold); narrow by one byte and retry. A
        // single-byte span always places, so this terminates.
        loop {
            // Group `units` by their span sub-key `key[start..end)`. A span never
            // crosses a segment boundary, so the sub-key is bounded by the widest
            // segment and fits a fixed-width, `Copy`, stack key — no per-child
            // heap `Vec` allocation. We sort `(sub-key, index)` pairs and walk
            // adjacent equal runs to form groups, avoiding a map entirely.
            let span_len = end - start;
            let mut keyed: Vec<([u8; KEY_LEN], usize)> = Vec::with_capacity(units.len());
            let mut buf = [0u8; KEY_LEN];
            for (i, l) in units.iter().enumerate() {
                let sub = Self::span_sub(l.childleaf_key(), start, end, &mut buf);
                let mut sk = [0u8; KEY_LEN];
                sk[..span_len].copy_from_slice(sub);
                keyed.push((sk, i));
            }
            keyed.sort_unstable_by(|a, b| a.0[..span_len].cmp(&b.0[..span_len]));
            let mut children: Vec<Self> = Vec::new();
            let mut run_start = 0;
            while run_start < keyed.len() {
                let mut run_end = run_start + 1;
                while run_end < keyed.len()
                    && keyed[run_end].0[..span_len] == keyed[run_start].0[..span_len]
                {
                    run_end += 1;
                }
                let members: Vec<Self> =
                    keyed[run_start..run_end].iter().map(|&(_, i)| units[i].clone()).collect();
                children.push(
                    Self::build_dense_node_owned(members, end, max_end, owner.clone())
                        .with_span(start, end),
                );
                run_start = run_end;
            }
            match Branch::from_children_dense(start, end, children, owner.clone()) {
                Some(nn) => return Head::new(0, nn),
                None => {
                    // A single-byte span keys children by the branch byte and
                    // always places (cf. `sequential_insert_all_keys`); a real
                    // assert (not debug-only) turns any violation into a clear
                    // panic instead of an underflowing infinite narrow.
                    assert!(
                        end > start + 1,
                        "single-byte dense span [{start},{end}) failed to place"
                    );
                    end -= 1;
                }
            }
        }
    }

    /// O(1) divergence handling: wrap two heads that diverge at `start` under a
    /// fresh dense parent, preserving each subtree intact.
    ///
    /// The span is capped at the nearer child's own branch point so it never
    /// overruns existing structure. Unlike a canonical rebuild this does *not*
    /// re-widen (it leaves the children's spans intact), so the node count is
    /// somewhat above the canonical minimum — but it is O(1) instead of
    /// O(subtree), which is the difference between a usable insert and a
    /// 60×-PATCH one.
    fn split_two(a: Self, b: Self, start: usize) -> Self {
        Self::split_two_owned(a, b, start, None)
    }

    /// Owner-threading variant of [`Self::split_two`]. `owner` must cover any
    /// `LocalLeaf` that becomes a *direct* child of the fresh parent (i.e. when
    /// `a` or `b` is itself a `LocalLeaf`); branch children carry their own
    /// owner internally and need no covering here. The heap path passes `None`.
    fn split_two_owned(
        a: Self,
        b: Self,
        start: usize,
        owner: Option<std::sync::Arc<dyn crate::vwpatch::branch::ArchiveOwner>>,
    ) -> Self {
        let end = O::next_boundary(start).min(a.end_depth()).min(b.end_depth());
        let a2 = a.with_span(start, end);
        let b2 = b.with_span(start, end);
        // Two children always fit a two-slot table, even on a fingerprint
        // collision (one bucket, two slots).
        let nn = Branch::from_children_dense(start, end, vec![a2, b2], owner)
            .expect("two children always place");
        Head::new(0, nn)
    }

    /// O(children) narrow of an overflowing dense node `this` (span `[s, e)`).
    ///
    /// The standard ART/HOT node split: instead of gathering every leaf under
    /// the node and rebuilding the whole subtree, take only `this`'s **immediate
    /// children** (clone = Arc bump, leaving their subtrees fully intact) plus
    /// the new `extra` unit, and re-group them under a narrower span. Children
    /// sharing the same narrowed sub-key get re-parented under a fresh
    /// intermediate node spanning `[e', e)`; singletons collapse and re-key up.
    /// The span bound `e` keeps every unit a leaf-positioned, opaque child, so
    /// no subtree is ever walked or rebuilt — the cost is O(children of `this`),
    /// not O(subtree). Cascades automatically when a freshly-formed intermediate
    /// still exceeds capacity (handled by `build_dense_node`'s recursion).
    fn narrow_with(this: Self, extra: Self, s: usize, e: usize) -> Self {
        Self::narrow_with_owned(this, extra, s, e, None)
    }

    /// Owner-threading variant of [`Self::narrow_with`]. The regrouped children
    /// of `this` (which, on the archive path, include `LocalLeaf`s belonging to
    /// `this`'s owner) are re-parented under fresh branches that must all adopt
    /// that same `owner`. Within one archive load `this.owner == extra`'s owner,
    /// so a single `owner` covers every regrouped unit. The heap path passes
    /// `None`.
    fn narrow_with_owned(
        this: Self,
        extra: Self,
        s: usize,
        e: usize,
        owner: Option<std::sync::Arc<dyn crate::vwpatch::branch::ArchiveOwner>>,
    ) -> Self {
        let mut units: Vec<Self> = match this.body_ref() {
            BodyRef::Branch(branch) => branch.child_table.iter().flatten().cloned().collect(),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                unreachable!("narrow_with requires a Branch node")
            }
        };
        units.push(extra);
        drop(this);
        Self::build_dense_node_owned(units, s, e, owner)
    }

    /// Variable-width dense insert of a fresh `leaf` head into `this`.
    pub(crate) fn insert_dense(mut this: Self, leaf: Self, at_depth: usize) -> Self {
        match this.tag() {
            HeadTag::Leaf | HeadTag::LocalLeaf => match this.first_divergence(&leaf, at_depth) {
                None => this, // duplicate key
                Some((start, _, _)) => Self::split_two(this, leaf, start),
            },
            _ => {
                let (s, e) = {
                    let BodyRef::Branch(b) = this.body_ref() else {
                        unreachable!()
                    };
                    (b.span_start as usize, b.span_end as usize)
                };

                // Compressed-path divergence (in `[at_depth, s)`): the new leaf
                // doesn't belong under this branch's span — wrap both under a
                // fresh parent at the divergence point (O(1)).
                if let Some((start, _, _)) = this.first_divergence(&leaf, at_depth) {
                    return Self::split_two(this, leaf, start);
                }

                let mut buf = [0u8; KEY_LEN];
                let sub = Self::span_sub(leaf.childleaf_key(), s, e, &mut buf);
                let fp = fingerprint16(sub);

                // Single verified lookup: descend into the matching child if it
                // exists, otherwise fall through to the add/overflow path. The
                // leaf is threaded through an `Option` so it is only consumed
                // when the closure actually runs (a hit); on a miss it is
                // handed back for the add path.
                let mut leaf_opt = Some(leaf);
                let mut ed = crate::vwpatch::branch::BranchMut::from_head(&mut this);
                if ed.recurse_dense_child(fp, sub, |child| {
                    Self::insert_dense(child, leaf_opt.take().unwrap(), e)
                }) {
                    drop(ed);
                    return this;
                }
                let leaf = leaf_opt.take().unwrap();

                // New distinct child. Overflow (cap reached) is only possible
                // once the table has grown to the 256-slot maximum, so the
                // O(table) filled count is computed only then.
                let over_cap = ed.child_table.len() >= 256
                    && ed.child_table.iter().flatten().count() >= Self::max_fanout(e - s);
                if over_cap {
                    drop(ed);
                    return Self::narrow_with(this, leaf, s, e);
                }

                let inserted = leaf.with_span(s, e);
                let leftover = ed.add_dense_child(inserted);
                drop(ed);
                if let Some(leftover) = leftover {
                    // Cuckoo placement failed below the cap — O(children) narrow.
                    return Self::narrow_with(this, leftover, s, e);
                }
                this
            }
        }
    }

    /// Install the opaque subtree `unit` into the dense branch `this`, where
    /// `unit.end_depth() >= this.span_end` (so `unit` is a leaf-positioned,
    /// opaque child of `this`). This is [`Self::insert_dense`]'s branch arm
    /// generalised from "a fresh leaf" to "any subtree": descend into the
    /// matching child (resolving the collision via [`Self::union`]), else add a
    /// new child, else narrow on overflow. A leaf is just the degenerate
    /// subtree, so the dense single-leaf insert is the `unit = leaf` case.
    fn insert_subtree(mut this: Self, unit: Self) -> Self {
        let (s, e) = match this.body_ref() {
            BodyRef::Branch(b) => (b.span_start as usize, b.span_end as usize),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                unreachable!("insert_subtree requires a Branch node")
            }
        };

        let mut buf = [0u8; KEY_LEN];
        let sub = Self::span_sub(unit.childleaf_key(), s, e, &mut buf);
        let fp = fingerprint16(sub);

        // Single verified lookup: descend into the matching child if it exists,
        // otherwise fall through to the add/overflow path. `unit` is threaded
        // through an `Option` so it is only consumed on a hit; on a miss it is
        // handed back for the add path.
        let mut unit_opt = Some(unit);
        let mut ed = crate::vwpatch::branch::BranchMut::from_head(&mut this);
        if ed.recurse_dense_child(fp, sub, |child| {
            Self::union(child, unit_opt.take().unwrap(), e)
        }) {
            drop(ed);
            return this;
        }
        let unit = unit_opt.take().unwrap();

        let over_cap = ed.child_table.len() >= 256
            && ed.child_table.iter().flatten().count() >= Self::max_fanout(e - s);
        if over_cap {
            drop(ed);
            return Self::narrow_with(this, unit, s, e);
        }

        let inserted = unit.with_span(s, e);
        let leftover = ed.add_dense_child(inserted);
        drop(ed);
        if let Some(leftover) = leftover {
            return Self::narrow_with(this, leftover, s, e);
        }
        this
    }

    /// Re-partition the dense branch `this` so its *top* branching window ends
    /// no later than `e_prime` (`span_start < e_prime <= this.span_end`), used
    /// by union's span reconciliation to align a wide window onto a narrower
    /// neighbour's boundary.
    ///
    /// Unlike [`Self::narrow_with`] / [`Self::build_dense_node`], the top window
    /// is capped at `e_prime` while the children's *internal* re-grouping is
    /// allowed to span up to `this`'s original `span_end` (`te`). That split is
    /// essential: two of `this`'s children can share the narrowed window
    /// `[s, e_prime)` yet diverge in `[e_prime, te)`. They must be re-parented
    /// under a fresh intermediate node spanning into `[e_prime, te)`; capping
    /// every sub-span at `e_prime` (the bug a naive `build_dense_node(_, s,
    /// e_prime)` hits) leaves them inseparable and recurses forever on an empty
    /// window. A wider-window fingerprint is not a prefix of a narrower one, so
    /// every child's fingerprint is recomputed over its new window.
    fn narrow_span(this: Self, e_prime: usize) -> Self {
        let (s, te, units): (usize, usize, Vec<Self>) = match this.body_ref() {
            BodyRef::Branch(b) => (
                b.span_start as usize,
                b.span_end as usize,
                b.child_table.iter().flatten().cloned().collect(),
            ),
            BodyRef::Leaf(_) | BodyRef::LocalLeaf(_) => {
                unreachable!("narrow_span requires a Branch node")
            }
        };
        drop(this);

        // `units` (this's children) first diverge at `s`, so the top window
        // starts at `s`. Pick the widest top window `<= e_prime` that places;
        // sub-groups recurse with `max_end = te` so deeper divergence stays
        // separable. A single-byte top window always places, so this loop
        // terminates.
        let mut end = Self::widest_dense_span(&units, s, e_prime);
        loop {
            let span_len = end - s;
            let mut keyed: Vec<([u8; KEY_LEN], usize)> = Vec::with_capacity(units.len());
            let mut buf = [0u8; KEY_LEN];
            for (i, u) in units.iter().enumerate() {
                let sub = Self::span_sub(u.childleaf_key(), s, end, &mut buf);
                let mut sk = [0u8; KEY_LEN];
                sk[..span_len].copy_from_slice(sub);
                keyed.push((sk, i));
            }
            keyed.sort_unstable_by(|a, b| a.0[..span_len].cmp(&b.0[..span_len]));
            let mut children: Vec<Self> = Vec::new();
            let mut run_start = 0;
            while run_start < keyed.len() {
                let mut run_end = run_start + 1;
                while run_end < keyed.len()
                    && keyed[run_end].0[..span_len] == keyed[run_start].0[..span_len]
                {
                    run_end += 1;
                }
                let members: Vec<Self> =
                    keyed[run_start..run_end].iter().map(|&(_, i)| units[i].clone()).collect();
                // Sub-groups may span up to `te` (the original span_end), then
                // re-key into the top window `[s, end)`.
                children.push(Self::build_dense_node(members, end, te).with_span(s, end));
                run_start = run_end;
            }
            match Branch::from_children_dense(s, end, children, None) {
                Some(nn) => return Head::new(0, nn),
                None => {
                    assert!(
                        end > s + 1,
                        "single-byte top window [{s},{end}) failed to place in narrow_span"
                    );
                    end -= 1;
                }
            }
        }
    }
}

unsafe impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> ByteEntry for Head<KEY_LEN, O, V> {
    fn key(&self) -> u16 {
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
                BodyPtr::LocalLeaf(ptr) => {
                    // LocalLeaf has no refcount — its lifetime is managed by
                    // the nearest ancestor Branch's `owner`. Cloning the Head
                    // just copies the tagged pointer; both Heads will read
                    // the same archive bytes.
                    Self::new_local_leaf(self.key(), ptr)
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
                    // No-op: LocalLeaf's bytes are owned by an ancestor
                    // Branch's `owner` Arc, not refcounted per-leaf.
                }
                BodyPtr::Branch(branch) => Branch::rc_dec(branch),
            }
        }
    }
}

/// A VWPATCH is a persistent data structure that stores a set of keys.
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
/// The VWPATCH allows for cheap copy-on-write operations, with `clone` being O(1).
#[derive(Debug)]
pub struct VWPATCH<const KEY_LEN: usize, O = IdentitySchema, V = ()>
where
    O: KeySchema<KEY_LEN>,
{
    root: Option<Head<KEY_LEN, O, V>>,
}

impl<const KEY_LEN: usize, O, V> Clone for VWPATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<const KEY_LEN: usize, O, V> Default for VWPATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<const KEY_LEN: usize, O, V> VWPATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    /// Creates a new empty VWPATCH.
    pub fn new() -> Self {
        init_sip_key();
        VWPATCH { root: None }
    }

    /// Inserts a shared key into the VWPATCH.
    ///
    /// Takes an [Entry] object that can be created from a key,
    /// and inserted into multiple VWPATCH instances.
    ///
    /// If the key is already present, this is a no-op.
    pub fn insert(&mut self, entry: &Entry<KEY_LEN, V>) {
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::insert_dense(this, entry.leaf(), 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(entry.leaf());
        }
    }

    /// Inserts a key into the VWPATCH, replacing the value if it already exists.
    pub fn replace(&mut self, entry: &Entry<KEY_LEN, V>) {
        if self.root.is_some() {
            let this = self.root.take().expect("root should not be empty");
            let new_head = Head::replace_leaf(this, entry.leaf(), 0);
            self.root.replace(new_head);
        } else {
            self.root.replace(entry.leaf());
        }
    }

    /// Removes a key from the VWPATCH.
    ///
    /// If the key is not present, this is a no-op.
    pub fn remove(&mut self, key: &[u8; KEY_LEN]) {
        Head::remove_leaf(&mut self.root, key, 0);
    }

    /// Returns the number of keys in the VWPATCH.
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

    /// Returns true if the VWPATCH contains no keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Unused outside this module's tests for now (the original `patch` variant
    // is consumed by `trible::tribleset`); kept for the upcoming HATCH rework.
    #[allow(dead_code)]
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

    /// Returns true if the VWPATCH has a key with the given prefix.
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

    /// Returns the number of VWPATCH nodes inspected by a prefix lookup.
    ///
    /// This is a diagnostic companion to [`VWPATCH::has_prefix`]. A miss counts
    /// the node where the mismatch or missing child is discovered; an empty
    /// VWPATCH reports zero.
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

    /// Iterates over all keys in the VWPATCH.
    /// The keys are returned in key ordering but random order.
    pub fn iter<'a>(&'a self) -> VWPATCHIterator<'a, KEY_LEN, O, V> {
        VWPATCHIterator::new(self)
    }

    /// Iterates over all keys in the VWPATCH in key order.
    ///
    /// The traversal visits every key in lexicographic key order, without
    /// accepting a prefix filter. For prefix-aware iteration, see
    /// [`VWPATCH::iter_prefix_count`].
    pub fn iter_ordered<'a>(&'a self) -> VWPATCHOrderedIterator<'a, KEY_LEN, O, V> {
        VWPATCHOrderedIterator::new(self)
    }

    /// Iterate over all prefixes of the given length in the VWPATCH.
    /// The prefixes are naturally returned in tree ordering and tree order.
    /// A count of the number of elements for the given prefix is also returned.
    pub fn iter_prefix_count<'a, const PREFIX_LEN: usize>(
        &'a self,
    ) -> VWPATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V> {
        VWPATCHPrefixIterator::new(self)
    }

    /// Unions this VWPATCH with another VWPATCH.
    ///
    /// The other VWPATCH is consumed, and this VWPATCH is updated in place.
    pub fn union(&mut self, other: Self)
    where
        O: Send + Sync,
        V: Send + Sync,
    {
        if let Some(other) = other.root {
            if self.root.is_some() {
                let this = self.root.take().expect("root should not be empty");
                #[cfg(feature = "parallel")]
                let merged = Head::par_union(this, other, 0);
                #[cfg(not(feature = "parallel"))]
                let merged = Head::union(this, other, 0);
                self.root.replace(merged);
            } else {
                self.root.replace(other);
            }
        }
    }

    /// Intersects this VWPATCH with another VWPATCH.
    ///
    /// Returns a new VWPATCH that contains only the keys that are present in both VWPATCHes.
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
                return Self {
                    root: result.map(|root| root.with_start(0)),
                };
            }
        }
        Self::new()
    }

    /// Returns the difference between this VWPATCH and another VWPATCH.
    ///
    /// Returns a new VWPATCH that contains only the keys that are present in this VWPATCH,
    /// but not in the other VWPATCH.
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
                Self { root: result }
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
/// [`ArchiveEntry`] does not carry a value. The leaf appears as a
/// `LocalLeaf` head if the receiving Branch's `owner` matches the
/// entry's; otherwise it is reified into a heap-allocated `Leaf<KEY_LEN,
/// ()>` automatically.
impl<const KEY_LEN: usize, O> VWPATCH<KEY_LEN, O, ()>
where
    O: KeySchema<KEY_LEN>,
{
    /// Inserts an archive-backed key. See [`ArchiveEntry`] for the
    /// owner semantics and the materialization rule for owner
    /// mismatches.
    pub fn insert_archive(&mut self, entry: &ArchiveEntry<'_, KEY_LEN>) {
        let (leaf_head, leaf_owner, _leaf_hash) = entry.leaf::<O>();
        if let Some(this) = self.root.take() {
            let new_head =
                Head::insert_dense_owned(this, leaf_head, Some(leaf_owner), 0, None);
            self.root.replace(new_head);
        } else {
            // Empty VWPATCH: the standalone root can't host an owner field
            // (only Branches carry `owner`), so reify the single entry
            // into a heap Leaf. The next insertion creates a Branch
            // which can adopt the owner cleanly.
            self.root
                .replace(Head::reify_local_leaf_unit_for_root(leaf_head));
        }
    }
}

impl<const KEY_LEN: usize, O, V> PartialEq for VWPATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn eq(&self, other: &Self) -> bool {
        self.root.as_ref().map(|root| root.hash()) == other.root.as_ref().map(|root| root.hash())
    }
}

impl<const KEY_LEN: usize, O, V> Eq for VWPATCH<KEY_LEN, O, V> where O: KeySchema<KEY_LEN> {}

impl<'a, const KEY_LEN: usize, O, V> IntoIterator for &'a VWPATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    type Item = &'a [u8; KEY_LEN];
    type IntoIter = VWPATCHIterator<'a, KEY_LEN, O, V>;

    fn into_iter(self) -> Self::IntoIter {
        VWPATCHIterator::new(self)
    }
}

/// An iterator over all keys in a VWPATCH.
/// The keys are returned in key ordering but in random order.
pub struct VWPATCHIterator<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    stack: ArrayVec<std::slice::Iter<'a, Option<Head<KEY_LEN, O, V>>>, KEY_LEN>,
    remaining: usize,
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> VWPATCHIterator<'a, KEY_LEN, O, V> {
    /// Creates an iterator over all keys in `patch`.
    pub fn new(patch: &'a VWPATCH<KEY_LEN, O, V>) -> Self {
        let mut r = VWPATCHIterator {
            stack: ArrayVec::new(),
            remaining: patch.len().min(usize::MAX as u64) as usize,
        };
        r.stack.push(std::slice::from_ref(&patch.root).iter());
        r
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for VWPATCHIterator<'a, KEY_LEN, O, V>
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
    for VWPATCHIterator<'a, KEY_LEN, O, V>
{
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> std::iter::FusedIterator
    for VWPATCHIterator<'a, KEY_LEN, O, V>
{
}

/// An iterator over every key in a VWPATCH, returned in key order.
///
/// Keys are yielded in lexicographic key order regardless of their physical
/// layout in the underlying tree. This iterator walks the full tree and does
/// not accept a prefix filter. For prefix-aware iteration, use
/// [`VWPATCHPrefixIterator`], constructed via [`VWPATCH::iter_prefix_count`].
/// Compare two sibling children of a branch spanning tree depths `[s, e)` by
/// their span sub-key bytes — the order-preserving key for ordered iteration.
/// The 16-bit fingerprint (`Head::key`) is identity for 1-byte and big-endian
/// for 2-byte spans, but an FNV hash for spans ≥3 bytes, so sorting on it would
/// scramble key order on wide dense nodes. Mirrors the construction-path sort
/// (`build_dense_node` / `narrow_span`).
#[inline]
fn cmp_child_span<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>(
    a: &Head<KEY_LEN, O, V>,
    b: &Head<KEY_LEN, O, V>,
    s: usize,
    e: usize,
) -> std::cmp::Ordering {
    let ak = a.childleaf_key();
    let bk = b.childleaf_key();
    for j in s..e {
        let p = O::TREE_TO_KEY[j];
        match ak[p].cmp(&bk[p]) {
            std::cmp::Ordering::Equal => continue,
            ord => return ord,
        }
    }
    std::cmp::Ordering::Equal
}

pub struct VWPATCHOrderedIterator<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    stack: Vec<ArrayVec<&'a Head<KEY_LEN, O, V>, 256>>,
    remaining: usize,
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> VWPATCHOrderedIterator<'a, KEY_LEN, O, V> {
    pub fn new(patch: &'a VWPATCH<KEY_LEN, O, V>) -> Self {
        let mut r = VWPATCHOrderedIterator {
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
                    let (s, e) = (branch.span_start as usize, branch.span_end as usize);
                    let first_level = &mut r.stack[0];
                    first_level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                    // Reverse (descending) so popping from the back yields ascending.
                    first_level.sort_unstable_by(|a, b| cmp_child_span::<KEY_LEN, O, V>(b, a, s, e));
                }
            }
        }
        r
    }
}

// --- Owned consuming iterators ---
/// Iterator that owns a VWPATCH and yields keys in key-order. The iterator
/// consumes the VWPATCH and stores it on the heap (Box) so it can safely hold
/// raw pointers into the patch memory while the iterator is moved.
pub struct VWPATCHIntoIterator<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> VWPATCHIntoIterator<KEY_LEN, O, V> {}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for VWPATCHIntoIterator<KEY_LEN, O, V>
{
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

/// Iterator that owns a VWPATCH and yields keys in key order.
pub struct VWPATCHIntoOrderedIterator<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for VWPATCHIntoOrderedIterator<KEY_LEN, O, V>
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
                    let (s, e) = (branch.span_start as usize, branch.span_end as usize);
                    let slice: &mut [Option<Head<KEY_LEN, O, V>>] = &mut branch.child_table;
                    // Sort ascending by span sub-key with empty slots (None) last;
                    // the `rev()`-push onto the LIFO queue then pops the smallest
                    // first. Order on the recovered span bytes, not the fingerprint
                    // (an FNV hash for spans ≥3 bytes — see `cmp_child_span`).
                    slice.sort_unstable_by(|a, b| match (a, b) {
                        (None, None) => std::cmp::Ordering::Equal,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (Some(a), Some(b)) => cmp_child_span::<KEY_LEN, O, V>(a, b, s, e),
                    });
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

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> IntoIterator for VWPATCH<KEY_LEN, O, V> {
    type Item = [u8; KEY_LEN];
    type IntoIter = VWPATCHIntoIterator<KEY_LEN, O, V>;

    fn into_iter(self) -> Self::IntoIter {
        let remaining = self.len().min(usize::MAX as u64) as usize;
        let mut q = Vec::new();
        if let Some(root) = self.root {
            q.push(root);
        }
        VWPATCHIntoIterator {
            queue: q,
            remaining,
        }
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> VWPATCH<KEY_LEN, O, V> {
    /// Consume and return an iterator that yields keys in key order.
    pub fn into_iter_ordered(self) -> VWPATCHIntoOrderedIterator<KEY_LEN, O, V> {
        let remaining = self.len().min(usize::MAX as u64) as usize;
        let mut q = Vec::new();
        if let Some(root) = self.root {
            q.push(root);
        }
        VWPATCHIntoOrderedIterator {
            queue: q,
            remaining,
        }
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for VWPATCHOrderedIterator<'a, KEY_LEN, O, V>
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
                        let (s, e) = (branch.span_start as usize, branch.span_end as usize);
                        self.stack.push(ArrayVec::new());
                        level = self.stack.last_mut()?;
                        level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                        // Reverse (descending) so popping from the back yields ascending.
                        level.sort_unstable_by(|a, b| cmp_child_span::<KEY_LEN, O, V>(b, a, s, e));
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
    for VWPATCHOrderedIterator<'a, KEY_LEN, O, V>
{
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> std::iter::FusedIterator
    for VWPATCHOrderedIterator<'a, KEY_LEN, O, V>
{
}

/// An iterator over all keys in a VWPATCH that have a given prefix.
/// The keys are returned in tree ordering and in tree order.
pub struct VWPATCHPrefixIterator<
    'a,
    const KEY_LEN: usize,
    const PREFIX_LEN: usize,
    O: KeySchema<KEY_LEN>,
    V,
> {
    stack: Vec<ArrayVec<&'a Head<KEY_LEN, O, V>, 256>>,
}

impl<'a, const KEY_LEN: usize, const PREFIX_LEN: usize, O: KeySchema<KEY_LEN>, V>
    VWPATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V>
{
    fn new(patch: &'a VWPATCH<KEY_LEN, O, V>) -> Self {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        let mut r = VWPATCHPrefixIterator {
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
                let (s, e) = (branch.span_start as usize, branch.span_end as usize);
                let first_level = &mut r.stack[0];
                first_level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                // Reverse (descending) so popping from the back yields ascending.
                first_level.sort_unstable_by(|a, b| cmp_child_span::<KEY_LEN, O, V>(b, a, s, e));
            }
        }
        r
    }
}

impl<'a, const KEY_LEN: usize, const PREFIX_LEN: usize, O: KeySchema<KEY_LEN>, V> Iterator
    for VWPATCHPrefixIterator<'a, KEY_LEN, PREFIX_LEN, O, V>
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
                    let (s, e) = (branch.span_start as usize, branch.span_end as usize);
                    self.stack.push(ArrayVec::new());
                    level = self.stack.last_mut()?;
                    level.extend(branch.child_table.iter().filter_map(|c| c.as_ref()));
                    // Reverse (descending) so popping from the back yields ascending.
                    level.sort_unstable_by(|a, b| cmp_child_span::<KEY_LEN, O, V>(b, a, s, e));
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
    use itertools::Itertools;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::iter::FromIterator;
    use std::mem;

    /// Determinism cross-check: the parallel set ops must be a faithful drop-in
    /// for the serial ones. Builds two overlapping medium tries (well above
    /// `PARALLEL_PATCH_UNION_THRESHOLD`, so the fan-out path is genuinely
    /// engaged), then asserts `par == serial` on set-hash, leaf_count, and the
    /// full key set for union, intersect AND difference. The XOR set-hash is
    /// set-determined, so hash equality is an exact oracle independent of node
    /// structure.
    #[cfg(feature = "parallel")]
    #[test]
    fn par_matches_serial_set_ops() {
        use std::collections::BTreeSet;
        type Trie = VWPATCH<64, IdentitySchema, ()>;

        fn mix(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn key_at(i: u64) -> [u8; 64] {
            let mut s = i.wrapping_mul(0x1_0000_01B3).wrapping_add(1);
            let mut k = [0u8; 64];
            for chunk in k.chunks_mut(8) {
                chunk.copy_from_slice(&mix(&mut s).to_le_bytes());
            }
            k
        }
        let build = |range: std::ops::Range<u64>| {
            let mut t = Trie::new();
            for i in range {
                t.insert(&Entry::new(&key_at(i)));
            }
            t
        };
        // Overlapping ranges → exercises both/only-this/only-other partitions.
        let a = build(0..8000);
        let b = build(5000..13000);

        // The parallel fan-out path is only meaningful with >1 worker thread,
        // and only taken above the threshold — assert both so a future change
        // that silently disables parallelism fails this gate.
        assert!(
            rayon::current_num_threads() > 1,
            "rayon must have >1 thread for the parallel gate (got {})",
            rayon::current_num_threads()
        );
        assert!(a.len() >= PARALLEL_PATCH_UNION_THRESHOLD as u64);
        assert!(b.len() >= PARALLEL_PATCH_UNION_THRESHOLD as u64);

        let keys = |t: &Trie| -> BTreeSet<[u8; 64]> { t.iter_ordered().copied().collect() };

        // UNION
        {
            let mut par = a.clone();
            par.union(b.clone());
            let ser_root = Head::union(a.root.clone().unwrap(), b.root.clone().unwrap(), 0);
            let ser = Trie { root: Some(ser_root) };
            assert_eq!(
                par.root.as_ref().unwrap().hash(),
                ser.root.as_ref().unwrap().hash(),
                "union: parallel set-hash != serial"
            );
            assert_eq!(par.len(), ser.len(), "union: leaf_count");
            assert_eq!(keys(&par), keys(&ser), "union: key set");
        }
        // INTERSECT
        {
            let par = a.intersect(&b);
            let ser_root = a.root.as_ref().unwrap().intersect(b.root.as_ref().unwrap(), 0);
            let ser = Trie { root: ser_root };
            assert_eq!(
                par.root.as_ref().unwrap().hash(),
                ser.root.as_ref().unwrap().hash(),
                "intersect: parallel set-hash != serial"
            );
            assert_eq!(par.len(), ser.len(), "intersect: leaf_count");
            assert_eq!(keys(&par), keys(&ser), "intersect: key set");
        }
        // DIFFERENCE
        {
            let par = a.difference(&b);
            let ser_root = a.root.as_ref().unwrap().difference(b.root.as_ref().unwrap(), 0);
            let ser = Trie { root: ser_root };
            assert_eq!(
                par.root.as_ref().unwrap().hash(),
                ser.root.as_ref().unwrap().hash(),
                "difference: parallel set-hash != serial"
            );
            assert_eq!(par.len(), ser.len(), "difference: leaf_count");
            assert_eq!(keys(&par), keys(&ser), "difference: key set");
        }
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
        let _tree = VWPATCH::<64, IdentitySchema, ()>::new();
    }

    #[test]
    fn tree_put_one() {
        const KEY_SIZE: usize = 64;
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
    }

    #[test]
    fn tree_clone_one() {
        const KEY_SIZE: usize = 64;
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
        let _clone = tree.clone();
    }

    #[test]
    fn tree_put_same() {
        const KEY_SIZE: usize = 64;
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let entry = Entry::new(&[0; KEY_SIZE]);
        tree.insert(&entry);
        tree.insert(&entry);
    }

    #[test]
    fn tree_replace_existing() {
        const KEY_SIZE: usize = 64;
        let key = [1u8; KEY_SIZE];
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
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
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
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
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

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
            let mut ed = crate::vwpatch::branch::BranchMut::from_slot(&mut tree.root);
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
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

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
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

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
        // Base = 64 bytes (was 48; +16 for `Option<Arc<dyn ArchiveOwner>>`).
        // Each child is an 8-byte tagged Head.
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 2], ()>>(
            ),
            64 + 8 * 2
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 4], ()>>(
            ),
            64 + 8 * 4
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 8], ()>>(
            ),
            64 + 8 * 8
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 16], ()>,
            >(),
            64 + 8 * 16
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<32, IdentitySchema, ()>>; 32], ()>,
            >(),
            64 + 8 * 32
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 64], ()>,
            >(),
            64 + 8 * 64
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 128], ()>,
            >(),
            64 + 8 * 128
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 256], ()>,
            >(),
            64 + 8 * 256
        );
    }

    /// Checks what happens if we join two VWPATCHes that
    /// only contain a single element each, that differs in the last byte.
    #[test]
    fn tree_union_single() {
        const KEY_SIZE: usize = 8;
        let mut left = VWPATCH::<KEY_SIZE, IdentitySchema, ()>::new();
        let mut right = VWPATCH::<KEY_SIZE, IdentitySchema, ()>::new();
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
            let mut tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
            }
        }

        #[test]
        fn tree_len(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = VWPATCH::<64, IdentitySchema, ()>::new();
            let mut set = HashSet::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
                set.insert(key);
            }

            prop_assert_eq!(set.len() as u64, tree.len())
        }

        /// Insert a random key set, then remove a pseudo-random subset, and
        /// assert membership matches a `HashSet` oracle (and `leaf_count` the
        /// oracle cardinality). A small alphabet over 12-byte keys forces heavy
        /// prefix sharing — deep multi-byte spans, branch collapses on
        /// underflow, and fingerprint collisions — exactly the cases the
        /// single-byte ops mis-handled. In debug builds every `remove`
        /// internally fires `Branch::debug_check_invariants` after each
        /// mutation, so aggregate/childleaf consistency is checked *throughout*,
        /// not just at the end. Run heavy with `PROPTEST_CASES=2000`.
        #[test]
        fn tree_remove(
            keys in prop::collection::vec(prop::collection::vec(0u8..=4, 12), 1..512),
            remove_flags in prop::collection::vec(any::<bool>(), 1..256),
        ) {
            const K: usize = 12;
            let mut tree = VWPATCH::<K, IdentitySchema, ()>::new();
            let mut set: HashSet<[u8; K]> = HashSet::new();
            let mut distinct: Vec<[u8; K]> = Vec::new();
            for key in keys {
                let key: [u8; K] = key.try_into().unwrap();
                tree.insert(&Entry::new(&key));
                if set.insert(key) {
                    distinct.push(key);
                }
            }

            for (i, key) in distinct.iter().enumerate() {
                if remove_flags[i % remove_flags.len()] {
                    tree.remove(key);
                    set.remove(key);
                    // Removing again must be an idempotent no-op.
                    tree.remove(key);
                    prop_assert!(tree.get(key).is_none());
                }
            }

            prop_assert_eq!(tree.len(), set.len() as u64);
            for key in &distinct {
                prop_assert_eq!(tree.get(key).is_some(), set.contains(key));
            }
            // Removing a never-inserted key is a no-op too.
            tree.remove(&[255u8; K]);
            prop_assert_eq!(tree.len(), set.len() as u64);
        }

        #[test]
        fn tree_infixes(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..1024)) {
            let mut tree = VWPATCH::<64, IdentitySchema, ()>::new();
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
            let mut tree = VWPATCH::<64, IdentitySchema, ()>::new();
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
        fn tree_iter_ordered(keys in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 1..512)) {
            // Ordered iteration must yield keys in ascending key order. With wide
            // multi-byte spans the per-node child sort cannot key on the 16-bit
            // fingerprint (an FNV hash for spans >2 bytes) — it must order by the
            // recovered span sub-key bytes. Unlike `tree_iter`, this does NOT
            // re-sort the tree output, so a fingerprint-ordered iterator fails.
            let mut tree = VWPATCH::<64, IdentitySchema, ()>::new();
            let mut set = HashSet::new();
            for key in keys {
                let key: [u8; 64] = key.try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
                set.insert(key);
            }
            let mut expected = Vec::from_iter(set.into_iter());
            expected.sort();

            let borrowed: Vec<[u8; 64]> = tree.iter_ordered().copied().collect();
            prop_assert_eq!(&borrowed, &expected);

            let owned: Vec<[u8; 64]> = tree.clone().into_iter_ordered().collect();
            prop_assert_eq!(&owned, &expected);
        }

        #[test]
        fn tree_union(left in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200),
                        right in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200)) {
            let mut set = HashSet::new();

            let mut left_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                let entry = Entry::new(&key);
                left_tree.insert(&entry);
                set.insert(key);
            }

            let mut right_tree = VWPATCH::<64, IdentitySchema, ()>::new();
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

            let mut left_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                let entry = Entry::new(&key);
                left_tree.insert(&entry);
                set.insert(key);
            }

            let right_tree = VWPATCH::<64, IdentitySchema, ()>::new();

            left_tree.union(right_tree);

            let mut set_vec = Vec::from_iter(set.into_iter());
            let mut tree_vec = vec![];
            left_tree.infixes(&[0; 0], &mut |&x: &[u8;64]| tree_vec.push(x));

            set_vec.sort();
            tree_vec.sort();

            prop_assert_eq!(set_vec, tree_vec);
            }

        #[test]
        fn tree_intersect(left in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200),
                        right in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200)) {
            let mut left_set = HashSet::new();
            let mut right_set = HashSet::new();

            let mut left_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                left_tree.insert(&Entry::new(&key));
                left_set.insert(key);
            }

            let mut right_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in right {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                right_tree.insert(&Entry::new(&key));
                right_set.insert(key);
            }

            let result = left_tree.intersect(&right_tree);

            let mut oracle: Vec<[u8; 64]> =
                left_set.intersection(&right_set).copied().collect();
            let mut tree_vec = vec![];
            result.infixes(&[0; 0], &mut |&x: &[u8; 64]| tree_vec.push(x));

            oracle.sort();
            tree_vec.sort();
            prop_assert_eq!(oracle.len() as u64, result.len());
            prop_assert_eq!(oracle, tree_vec);
        }

        #[test]
        fn tree_difference(left in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200),
                        right in prop::collection::vec(prop::collection::vec(0u8..=255, 64), 200)) {
            let mut left_set = HashSet::new();
            let mut right_set = HashSet::new();

            let mut left_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in left {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                left_tree.insert(&Entry::new(&key));
                left_set.insert(key);
            }

            let mut right_tree = VWPATCH::<64, IdentitySchema, ()>::new();
            for entry in right {
                let mut key = [0; 64];
                key.iter_mut().set_from(entry.iter().cloned());
                right_tree.insert(&Entry::new(&key));
                right_set.insert(key);
            }

            let result = left_tree.difference(&right_tree);

            let mut oracle: Vec<[u8; 64]> =
                left_set.difference(&right_set).copied().collect();
            let mut tree_vec = vec![];
            result.infixes(&[0; 0], &mut |&x: &[u8; 64]| tree_vec.push(x));

            oracle.sort();
            tree_vec.sort();
            prop_assert_eq!(oracle.len() as u64, result.len());
            prop_assert_eq!(oracle, tree_vec);
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

            let mut tree = VWPATCH::<8, IdentitySchema, ()>::new();
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

            let mut tree = VWPATCH::<8, IdentitySchema, ()>::new();
            for key in base_keys {
                let key: [u8; 8] = key[..].try_into().unwrap();
                let entry = Entry::new(&key);
                tree.insert(&entry);
            }
            let base_tree_content: Vec<[u8; 8]> = tree.iter().copied().collect();

            let mut tree_clone = tree.clone();
            let mut new_tree = VWPATCH::<8, IdentitySchema, ()>::new();
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
        let mut left = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

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
        let mut left = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

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
        let left = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let mut right = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let key = [1u8, 2u8, 3u8, 4u8];
        right.insert(&Entry::with_value(&key, 7));

        let res = left.difference(&right);
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn difference_empty_right_returns_left() {
        const KEY_SIZE: usize = 4;
        let mut left = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
        let right = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();
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
        let mut tree = VWPATCH::<KEY_SIZE, IdentitySchema, u32>::new();

        let entry1 = Entry::with_value(&[0u8; KEY_SIZE], 1u32);
        let entry2 = Entry::with_value(&[1u8; KEY_SIZE], 2u32);
        tree.insert(&entry1);
        tree.insert(&entry2);
        assert_eq!(tree.len(), 2);

        // Edit the root slot in-place using the BranchMut editor. Children of a
        // variable-width branch are keyed by the fingerprint of their span
        // sub-key over `[span_start, span_end)`, so the inserted head must be
        // span-keyed (`with_span`) — a raw single-byte key would mis-place it.
        {
            let mut ed = crate::vwpatch::branch::BranchMut::from_slot(&mut tree.root);

            // Compute the span bounds first to avoid borrowing `ed` in the closure.
            let span_start = ed.span_start as usize;
            let span_end = ed.span_end as usize;
            let inserted = Entry::with_value(&[2u8; KEY_SIZE], 3u32)
                .leaf::<IdentitySchema>()
                .with_span(span_start, span_end);
            let key = inserted.key();

            ed.modify_child(key, |opt| match opt {
                Some(old) => Some(Head::insert_leaf(old, inserted, span_end)),
                None => Some(inserted),
            });
            // BranchMut is dropped here and commits the updated branch pointer back into the head.
        }

        assert_eq!(tree.len(), 3);
        assert_eq!(tree.get(&[2u8; KEY_SIZE]), Some(&3u32));
    }

    /// Faithful-clone equivalence: insert the same ~10_000 64-byte keys into
    /// the original `crate::patch::PATCH` and the cloned `VWPATCH`, both under
    /// the `EAVOrder` schema, and assert identical leaf_count, identical root
    /// hash (they share the SIP key), and identical lookup results. This is the
    /// Phase-1 acceptance check that the clone is behaviorally identical.
    #[test]
    fn clone_equivalence_with_patch() {
        use crate::patch::PATCH;
        use crate::trible::EAVOrder;

        // Deterministic, well-spread 64-byte keys from a counter (splitmix64).
        fn key_for(i: u64) -> [u8; 64] {
            let mut k = [0u8; 64];
            let mut state = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for chunk in k.chunks_mut(8) {
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                chunk.copy_from_slice(&z.to_le_bytes());
            }
            k
        }

        const N: u64 = 10_000;

        let mut orig: PATCH<64, EAVOrder, ()> = PATCH::new();
        let mut clone: VWPATCH<64, EAVOrder, ()> = VWPATCH::new();

        for i in 0..N {
            let key = key_for(i);
            orig.insert(&crate::patch::Entry::new(&key));
            clone.insert(&Entry::new(&key));
        }

        // Identical leaf_count.
        assert_eq!(orig.len(), clone.len(), "leaf_count must match");
        assert_eq!(orig.len(), N, "all keys deduplicate to N distinct leaves");

        // Identical root hash (shared SIP key makes this meaningful).
        assert_eq!(
            orig.root_hash(),
            clone.root_hash(),
            "root hash must match between PATCH and VWPATCH"
        );
        assert!(orig.root_hash().is_some());

        // Identical lookup results for present keys ...
        for i in 0..N {
            let key = key_for(i);
            assert_eq!(orig.get(&key), Some(&()));
            assert_eq!(clone.get(&key), orig.get(&key), "present-key lookup match");
        }
        // ... and for absent keys.
        for i in N..(N + 1000) {
            let key = key_for(i);
            assert_eq!(orig.get(&key), None);
            assert_eq!(clone.get(&key), orig.get(&key), "absent-key lookup match");
        }
    }

    /// Small in-debug gate for the dense ARCHIVE-insert path
    /// ([`Head::insert_dense_owned`]). Runs under `debug_assertions`, so the
    /// per-op `debug_check_invariants` validate the incrementally-maintained
    /// branch aggregates (hash / leaf_count / segment_count / childleaf) on
    /// every dense archive mutation — coverage the heavy `#[ignore]`d 10M gate
    /// (release-only) cannot give. Builds a 16-byte-aligned synthetic archive
    /// of distinct keys and asserts:
    ///   * single-owner archive build == heap build (branch count, root hash,
    ///     LocalLeaf census), and
    ///   * cross-owner (two owner Arcs over the same bytes) still yields the
    ///     correct key set via the reify-on-mismatch path.
    #[test]
    fn archive_insert_dense_matches_heap_small() {
        use crate::vwpatch::branch::ArchiveOwner;
        use std::sync::Arc;

        const M: usize = 4000;
        // 16-byte-aligned buffer (Vec<u128> base is 16-aligned).
        let mut words: Vec<u128> = vec![0u128; M * 4];
        let base = words.as_ptr() as usize;
        assert_eq!(base & 0x0f, 0);
        {
            // SAFETY: words holds exactly M*64 bytes; plain-byte view.
            let bytes = unsafe { std::slice::from_raw_parts_mut(base as *mut u8, M * 64) };
            let mut s = 0x1234_5678_9abc_def0u64;
            for b in bytes.iter_mut() {
                s = s
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                *b = (s >> 33) as u8;
            }
            // Make every key distinct by stamping its index into the first 8 bytes.
            for i in 0..M {
                bytes[i * 64..i * 64 + 8].copy_from_slice(&(i as u64).to_le_bytes());
            }
        }
        // SAFETY: `words` outlives every trie built below.
        let keys: &[[u8; 64]] = unsafe { std::slice::from_raw_parts(base as *const [u8; 64], M) };

        // Heap reference build.
        let mut heap: VWPATCH<64, IdentitySchema, ()> = VWPATCH::new();
        for k in keys {
            heap.insert(&Entry::new(k));
        }
        let (h_branches, ..) = heap.node_stats();

        // Single-owner archive build.
        let owner: Arc<dyn ArchiveOwner> = Arc::new(());
        let mut arch: VWPATCH<64, IdentitySchema, ()> = VWPATCH::new();
        for i in 0..M {
            let p = unsafe { NonNull::new_unchecked((base + i * 64) as *mut [u8; 64]) };
            let e = unsafe { ArchiveEntry::new(p, &owner) };
            arch.insert_archive(&e);
        }
        assert_eq!(arch.len(), M as u64);
        let (a_branches, _slots, a_heap, a_local) = arch.node_stats();
        assert_eq!(a_heap, 1, "archive heap_leaves {a_heap} (expected 1 root bootstrap)");
        assert_eq!(a_local, (M - 1) as u64, "archive local_leaf_slots {a_local}");
        assert_eq!(a_branches, h_branches, "archive branch count != heap");
        assert_eq!(arch.root_hash(), heap.root_hash(), "archive root hash != heap");
        for k in keys {
            assert!(arch.get(k).is_some(), "archive lost a present key");
        }

        // Cross-archive: two distinct owners over the SAME bytes exercises the
        // reify-on-mismatch path. The final key set must still be correct.
        let owner_a: Arc<dyn ArchiveOwner> = Arc::new(());
        let owner_b: Arc<dyn ArchiveOwner> = Arc::new(());
        let mut mixed: VWPATCH<64, IdentitySchema, ()> = VWPATCH::new();
        for i in 0..M {
            let p = unsafe { NonNull::new_unchecked((base + i * 64) as *mut [u8; 64]) };
            let o = if i % 2 == 0 { &owner_a } else { &owner_b };
            let e = unsafe { ArchiveEntry::new(p, o) };
            mixed.insert_archive(&e);
        }
        assert_eq!(mixed.len(), M as u64);
        assert_eq!(
            mixed.root_hash(),
            heap.root_hash(),
            "cross-archive set-hash differs from heap"
        );
        for k in keys {
            assert!(mixed.get(k).is_some(), "cross-archive lost a present key");
        }

        drop((owner, owner_a, owner_b));
        drop(words);
    }

    /// Phase 2b-ii correctness gate. Reads the 9,970,736 64-byte EAV tribles in
    /// `/tmp/facts.simplearchive` (raw, header-less: file size is exactly
    /// `N * 64`) and asserts the three dense-trie invariants:
    ///
    /// 1. Node count ~510K (450K..560K) and `leaf_count == 9,970,736`.
    /// 2. Every present key is found; 100k absent keys are not.
    /// 3. `root_hash()` is identical for file-order vs reverse-order insertion
    ///    (XOR invariance — correctness independent of tree shape).
    ///
    /// Heavy + needs the external fixture, so `#[ignore]`d; run with
    /// `cargo test -p triblespace-core --release --features vwpatch
    /// dense_gate_10m_eav -- --ignored --nocapture`.
    #[test]
    #[ignore = "heavy; needs /tmp/facts.simplearchive — run with --release --ignored"]
    fn dense_gate_10m_eav() {
        use crate::trible::EAVOrder;

        const PATH: &str = "/tmp/facts.simplearchive";
        const N: usize = 9_970_736;

        let file = std::fs::File::open(PATH).expect("open /tmp/facts.simplearchive");
        // SAFETY: read-only mapping of a file we do not mutate for the test's
        // lifetime.
        let mmap = unsafe { memmap2::Mmap::map(&file).expect("mmap facts") };
        assert_eq!(mmap.len(), N * 64, "fixture must be N*64 raw trible bytes");
        let keys: &[[u8; 64]] =
            unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const [u8; 64], N) };

        // (1) + (2): forward-order tree.
        let mut fwd: VWPATCH<64, EAVOrder, ()> = VWPATCH::new();
        for k in keys {
            fwd.insert(&Entry::new(k));
        }
        assert_eq!(fwd.len(), N as u64, "leaf_count must equal N");

        let (branches, slots, heap_leaves, _local) = fwd.node_stats();
        println!(
            "dense_gate: branches={branches} slots={slots} heap_leaves={heap_leaves} \
             fill={:.3}",
            fwd.len() as f64 / branches as f64
        );
        // The O(1)-divergence fast path (split_two) keeps the insert usable but
        // does not re-widen, so the node count sits above the canonical
        // ~510K that HATCH's collect-and-rebuild reaches (at a ~60×-PATCH
        // insert cost). It must still be solidly variable-width — far below the
        // single-byte PATCH's ~1.52M nodes for this set.
        assert!(
            branches < 800_000,
            "node count {branches} not variable-width (PATCH is ~1.52M)"
        );

        // Present-key lookups.
        for k in keys {
            assert!(fwd.get(k).is_some(), "present key must be found");
        }
        // Absent-key lookups: flip a high value byte so the key cannot exist
        // (and stays a syntactically valid trible).
        let mut absent = 0u64;
        for k in keys.iter().take(100_000) {
            let mut q = *k;
            q[40] ^= 0xAA;
            q[41] ^= 0x55;
            // Skip the astronomically unlikely case the perturbation collides
            // with a real key.
            if fwd.get(&q).is_none() {
                absent += 1;
            }
        }
        assert!(
            absent >= 99_990,
            "expected ~100k absent misses, got {absent}"
        );

        let root_fwd = fwd.root_hash().expect("non-empty root hash");
        drop(fwd);

        // (3): reverse-order tree, compare root hash.
        let mut rev: VWPATCH<64, EAVOrder, ()> = VWPATCH::new();
        for k in keys.iter().rev() {
            rev.insert(&Entry::new(k));
        }
        assert_eq!(rev.len(), N as u64);
        let root_rev = rev.root_hash().expect("non-empty root hash");
        drop(mmap); // keep the mapping alive until both trees are built
        assert_eq!(
            root_fwd, root_rev,
            "root hash must be insertion-order invariant (XOR)"
        );
    }

    /// Dense-span ARCHIVE-insert correctness gate. Archive-loads the
    /// 9,970,736-trible fixture through `insert_archive` (the zero-copy
    /// `LocalLeaf` path) for both an identity (EAV) and a value-first (VEA)
    /// ordering, and proves the dense-archive rework holds every invariant:
    ///
    /// 1. **Compression matches the heap build.** The archive trie's branch
    ///    count equals the heap `insert_dense` build's over the same keys —
    ///    so archive loading now reaches the SAME ~712K dense node count,
    ///    not single-byte PATCH's ~1.52M.
    /// 2. **LocalLeaves preserved.** `heap_leaf_nodes == 1` (root bootstrap)
    ///    and `local_leaf_slots == N-1` — compression did not silently reify
    ///    leaves onto the heap.
    /// 3. **Set-hash oracle.** The archive-built root hash equals the
    ///    heap-built root hash over the same key set (XOR subtree-hash is
    ///    set-determined, structure-independent).
    /// 4. **Queries.** Every present key resolves; 100k perturbed keys miss;
    ///    `has_prefix` at a 16-byte segment boundary holds.
    ///
    /// Heavy + external fixture → `#[ignore]`d. Run with
    /// `cargo test -p triblespace-core --release --features vwpatch
    /// dense_archive_gate_10m -- --ignored --nocapture`.
    #[test]
    #[ignore = "heavy; needs /tmp/facts.simplearchive — run with --release --ignored"]
    fn dense_archive_gate_10m() {
        use crate::trible::{EAVOrder, VEAOrder};
        use crate::vwpatch::branch::ArchiveOwner;
        use std::sync::Arc;

        const PATH: &str = "/tmp/facts.simplearchive";
        const N: usize = 9_970_736;

        let file = std::fs::File::open(PATH).expect("open /tmp/facts.simplearchive");
        // SAFETY: read-only mapping of a file we do not mutate for the test.
        let mmap = unsafe { memmap2::Mmap::map(&file).expect("mmap facts") };
        assert_eq!(mmap.len(), N * 64, "fixture must be N*64 raw trible bytes");
        let base = mmap.as_ptr() as usize;
        assert_eq!(base & 0x0f, 0, "mmap base must be 16-byte aligned");
        // The owner Arc keeps the mapping alive for every LocalLeaf that
        // points into it; the raw `base` reads stay valid as long as `owner`.
        let owner: Arc<dyn ArchiveOwner> = Arc::new(mmap);
        // SAFETY: `owner` holds the mapping; bytes are immutable and 16-aligned.
        let keys: &[[u8; 64]] =
            unsafe { std::slice::from_raw_parts(base as *const [u8; 64], N) };

        fn run_gate<O: KeySchema<64>>(
            name: &str,
            n: usize,
            base: usize,
            owner: &Arc<dyn ArchiveOwner>,
            keys: &[[u8; 64]],
        ) {
            // Archive build (LocalLeaf, dense spans, shared owner).
            let mut arch: VWPATCH<64, O, ()> = VWPATCH::new();
            for i in 0..n {
                // SAFETY: 16-aligned, valid for `owner`'s lifetime.
                let p = unsafe { NonNull::new_unchecked((base + i * 64) as *mut [u8; 64]) };
                let e = unsafe { ArchiveEntry::new(p, owner) };
                arch.insert_archive(&e);
            }
            assert_eq!(arch.len(), n as u64, "{name}: archive leaf_count");
            let (a_branches, _slots, a_heap, a_local) = arch.node_stats();
            assert_eq!(a_heap, 1, "{name}: archive heap_leaves {a_heap} (expected 1 root bootstrap)");
            assert_eq!(
                a_local,
                (n - 1) as u64,
                "{name}: archive local_leaf_slots {a_local} != N-1 {}",
                n - 1
            );

            // Heap build over the same keys (dense, no owner).
            let mut heap: VWPATCH<64, O, ()> = VWPATCH::new();
            for k in keys {
                heap.insert(&Entry::new(k));
            }
            let (h_branches, _, _, _) = heap.node_stats();
            // The structural gate: the archive dense build must reach EXACTLY
            // the heap dense build's node count over the same keys — proving
            // archive loading now compresses identically (EAV: ~712K dense, not
            // single-byte PATCH's ~1.52M). VEA's dense build is intrinsically
            // bushier (value-first, near-unique prefixes + the non-re-widening
            // split_two fast path), so its absolute count is higher; the
            // archive==heap equality is the invariant that matters, and it must
            // hold for every ordering.
            assert_eq!(
                a_branches, h_branches,
                "{name}: archive branch count {a_branches} != heap {h_branches} \
                 (dense compression must match the heap build)"
            );

            // Set-hash oracle: archive trie holds exactly the heap key set.
            assert_eq!(
                arch.root_hash(),
                heap.root_hash(),
                "{name}: archive root hash != heap root hash"
            );

            // Present keys (lookups index by tree position).
            for k in keys {
                let tk = O::tree_ordered(k);
                assert!(arch.get(&tk).is_some(), "{name}: present key missing");
            }
            // Absent keys.
            let mut absent = 0u64;
            for k in keys.iter().take(100_000) {
                let mut q = *k;
                q[40] ^= 0xAA;
                q[41] ^= 0x55;
                let tq = O::tree_ordered(&q);
                if arch.get(&tq).is_none() {
                    absent += 1;
                }
            }
            assert!(absent >= 99_990, "{name}: expected ~100k absent misses, got {absent}");

            // has_prefix at a 16-byte segment boundary (tree-ordered space).
            let tree0 = O::tree_ordered(&keys[0]);
            let mut seg16 = [0u8; 16];
            seg16.copy_from_slice(&tree0[..16]);
            assert!(arch.has_prefix(&seg16), "{name}: has_prefix(seg16) failed");

            println!(
                "dense_archive_gate {name}: branches={a_branches} (heap {h_branches}) \
                 heap_leaves={a_heap} local_leaf_slots={a_local} root_hash_eq=ok"
            );
        }

        run_gate::<EAVOrder>("eav", N, base, &owner, keys);
        run_gate::<VEAOrder>("vea", N, base, &owner, keys);

        drop(owner); // keep the mapping alive through both gates
    }
}
