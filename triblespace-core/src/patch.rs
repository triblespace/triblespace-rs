//! Persistent Adaptive Trie with Cuckoo-compression and
//! Hash-maintenance (PATCH).
//!
//! See the [PATCH](../book/src/deep-dive/patch.md) chapter of the Tribles Book
//! for the full design description and hashing scheme.
//!
//! Values stored in leaves are not part of hashing or equality comparisons.
//! Two `PATCH`es are considered equal if they contain the same set of keys,
//! even if the associated values differ. This allows using the structure as an
//! idempotent blobstore where a value's hash determines its key.
//!
#![allow(unstable_name_collisions)]

mod branch;
pub mod bytetable;
mod entry;
mod leaf;

use arrayvec::ArrayVec;

use branch::*;
pub use entry::Entry;
use leaf::*;

pub use bytetable::*;
use rand::thread_rng;
use rand::RngCore;
use std::cmp::Reverse;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Once;

#[cfg(not(target_pointer_width = "64"))]
compile_error!("PATCH tagged pointers require 64-bit targets");

static mut SIP_KEY: [u8; 16] = [0; 16];
static INIT: Once = Once::new();

/// Initializes the SIP key used for key hashing.
/// This function is called automatically when a new PATCH is created.
fn init_sip_key() {
    INIT.call_once(|| {
        bytetable::init();

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

#[doc(hidden)]
#[macro_export]
macro_rules! key_segmentation {
    (@count $($e:expr),* $(,)?) => {
        <[()]>::len(&[$($crate::key_segmentation!(@sub $e)),*])
    };
    (@sub $e:expr) => { () };
    ($name:ident, $len:expr, [$($seg_len:expr),+ $(,)?]) => {
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
    ($name:ident, $seg:ty, $len:expr, [$($perm:expr),+ $(,)?]) => {
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
#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub(crate) enum HeadTag {
    // Stored in the low 4 bits of `Head::tptr` (see Head::new).
    //
    // Branch values encode log2(branch_size) (i.e. `Branch2 == 1`, `Branch256
    // == 8`). `0` is reserved for leaf nodes, which lets us compute the branch
    // size as `1 << tag` without any offset.
    Leaf = 0,
    Branch2 = 1,
    Branch4 = 2,
    Branch8 = 3,
    Branch16 = 4,
    Branch32 = 5,
    Branch64 = 6,
    Branch128 = 7,
    Branch256 = 8,
}

impl HeadTag {
    #[inline]
    fn from_raw(raw: u8) -> Self {
        debug_assert!(raw <= HeadTag::Branch256 as u8);
        // SAFETY: `HeadTag` is `#[repr(u8)]` with a contiguous discriminant
        // range 0..=8. The tag bits are written by Head::new/set_body and
        // Branch::tag, which only emit valid discriminants.
        unsafe { std::mem::transmute(raw) }
    }
}

pub(crate) enum BodyPtr<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(NonNull<Leaf<KEY_LEN, V>>),
    Branch(branch::BranchNN<KEY_LEN, O, V>),
}

/// Immutable borrow view of a Head body.
/// Returned by `body_ref()` and tied to the lifetime of the `&Head`.
pub(crate) enum BodyRef<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a Leaf<KEY_LEN, V>),
    Branch(&'a Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>),
}

/// Mutable borrow view of a Head body.
/// Returned by `body_mut()` and tied to the lifetime of the `&mut Head`.
pub(crate) enum BodyMut<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    Leaf(&'a mut Leaf<KEY_LEN, V>),
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

    #[inline]
    fn fix_body_addr(addr: u64) -> u64 {
        // On x86_64, canonical addresses are sign-extended, so reconstruct the
        // upper bits from the highest retained bit (55). On aarch64, user
        // pointers are typically zero-extended, so keep the masked value.
        #[cfg(target_arch = "x86_64")]
        {
            ((addr << 8) as i64 >> 8) as u64
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            addr
        }
    }

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
        self.tptr = std::ptr::NonNull::new(self.tptr.as_ptr().map_addr(|addr| {
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
                Self::fix_body_addr(masked) as usize
            }));
            match self.tag() {
                HeadTag::Leaf => BodyPtr::Leaf(ptr.cast()),
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

    /// Returns an immutable borrow of the body (Leaf or Branch) tied to &self.
    pub(crate) fn body_ref(&self) -> BodyRef<'_, KEY_LEN, O, V> {
        match self.body() {
            BodyPtr::Leaf(nn) => BodyRef::Leaf(unsafe { nn.as_ref() }),
            BodyPtr::Branch(nn) => BodyRef::Branch(unsafe { nn.as_ref() }),
        }
    }

    pub(crate) fn count(&self) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(_) => 1,
            BodyRef::Branch(branch) => branch.leaf_count,
        }
    }

    pub(crate) fn count_segment(&self, at_depth: usize) -> u64 {
        match self.body_ref() {
            BodyRef::Leaf(_) => 1,
            BodyRef::Branch(branch) => branch.count_segment(at_depth),
        }
    }

    pub(crate) fn hash(&self) -> u128 {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.hash,
            BodyRef::Branch(branch) => branch.hash,
        }
    }

    pub(crate) fn end_depth(&self) -> usize {
        match self.body_ref() {
            BodyRef::Leaf(_) => KEY_LEN,
            BodyRef::Branch(branch) => branch.end_depth as usize,
        }
    }

    /// Return the raw pointer to the child leaf for use in low-level
    /// operations (for example when constructing a Branch). Prefer
    /// `childleaf_key()` or other safe accessors when you only need the
    /// key or value; those avoid unsafe dereferences.
    pub(crate) fn childleaf_ptr(&self) -> *const Leaf<KEY_LEN, V> {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf as *const Leaf<KEY_LEN, V>,
            BodyRef::Branch(branch) => branch.childleaf_ptr(),
        }
    }

    pub(crate) fn childleaf_key(&self) -> &[u8; KEY_LEN] {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => &leaf.key,
            BodyRef::Branch(branch) => &branch.childleaf().key,
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
            if this.tag() == HeadTag::Leaf {
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
                if ed.leaf_count == 1 {
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
            let new_body = Branch::new(
                depth,
                this.with_key(this_byte_key),
                leaf.with_key(leaf_byte_key),
            );
            return Head::new(old_key, new_body);
        }

        let end_depth = this.end_depth();
        if end_depth != KEY_LEN {
            // Use the editable BranchMut view to perform mutations without
            // exposing pointer juggling at the call site.
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
            // Use BranchMut to edit `this` safely and avoid pointer juggling.
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

        // both depths are equal and the hashes differ: merge children
        let BodyMut::Branch(other_branch_ref) = other.body_mut() else {
            unreachable!();
        };
        {
            // Editable branch view: construct a BranchMut from the owned `this`
            // head and perform all mutations via that editor. The editor
            // performs COW up-front and writes the final pointer back into
            // `this` when it is dropped.
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
        }
        this
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
            BodyRef::Branch(branch) => {
                branch.infixes::<PREFIX_LEN, INFIX_LEN, F>(prefix, at_depth, f)
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
            BodyRef::Branch(branch) => branch.has_prefix::<PREFIX_LEN>(at_depth, prefix),
        }
    }

    pub(crate) fn get<'a>(&'a self, at_depth: usize, key: &[u8; KEY_LEN]) -> Option<&'a V>
    where
        O: 'a,
    {
        match self.body_ref() {
            BodyRef::Leaf(leaf) => leaf.get::<O>(at_depth, key),
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
            BodyRef::Branch(branch) => branch.segmented_len::<PREFIX_LEN>(at_depth, prefix),
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
            {
                let mut ed = crate::patch::branch::BranchMut::from_head(&mut new_branch);
                ed.modify_child(other_byte_key, |opt| {
                    opt.and_then(|child| child.difference(other, self_depth))
                });
            }
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
    root: Option<Head<KEY_LEN, O, V>>,
}

impl<const KEY_LEN: usize, O, V> Clone for PATCH<KEY_LEN, O, V>
where
    O: KeySchema<KEY_LEN>,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
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
        PATCH { root: None }
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
    }

    /// Removes a key from the PATCH.
    ///
    /// If the key is not present, this is a no-op.
    pub fn remove(&mut self, key: &[u8; KEY_LEN]) {
        Head::remove_leaf(&mut self.root, key, 0);
    }

    /// Returns the number of keys in the PATCH.
    pub fn len(&self) -> u64 {
        if let Some(root) = &self.root {
            root.count()
        } else {
            0
        }
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
    pub fn union(&mut self, other: Self) {
        if let Some(other) = other.root {
            if self.root.is_some() {
                let this = self.root.take().expect("root should not be empty");
                let merged = Head::union(this, other, 0);
                self.root.replace(merged);
            } else {
                self.root.replace(other);
            }
        }
    }

    /// Intersects this PATCH with another PATCH.
    ///
    /// Returns a new PATCH that contains only the keys that are present in both PATCHes.
    pub fn intersect(&self, other: &Self) -> Self {
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                return Self {
                    root: root.intersect(other_root, 0).map(|root| root.with_start(0)),
                };
            }
        }
        Self::new()
    }

    /// Returns the difference between this PATCH and another PATCH.
    ///
    /// Returns a new PATCH that contains only the keys that are present in this PATCH,
    /// but not in the other PATCH.
    pub fn difference(&self, other: &Self) -> Self {
        if let Some(root) = &self.root {
            if let Some(other_root) = &other.root {
                Self {
                    root: root.difference(other_root, 0),
                }
            } else {
                (*self).clone()
            }
        } else {
            (*other).clone()
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
                    BodyRef::Leaf(_) => {}
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
                        BodyRef::Leaf(_) => {
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
                BodyRef::Leaf(_) => {
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
/// consumes the PATCH and stores it on the heap (Box) so it can safely hold
/// raw pointers into the patch memory while the iterator is moved.
pub struct PATCHIntoIterator<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
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
    queue: Vec<Head<KEY_LEN, O, V>>,
    remaining: usize,
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
        let mut q = Vec::new();
        if let Some(root) = self.root {
            q.push(root);
        }
        PATCHIntoIterator {
            queue: q,
            remaining,
        }
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> PATCH<KEY_LEN, O, V> {
    /// Consume and return an iterator that yields keys in key order.
    pub fn into_iter_ordered(self) -> PATCHIntoOrderedIterator<KEY_LEN, O, V> {
        let remaining = self.len().min(usize::MAX as u64) as usize;
        let mut q = Vec::new();
        if let Some(root) = self.root {
            q.push(root);
        }
        PATCHIntoOrderedIterator {
            queue: q,
            remaining,
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
                    BodyRef::Leaf(_) => {
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
    use itertools::Itertools;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::convert::TryInto;
    use std::iter::FromIterator;
    use std::mem;

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
            BodyRef::Leaf(_) => panic!("root should be a branch"),
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
            BodyRef::Leaf(_) => {}
            BodyRef::Branch(_) => panic!("root should have collapsed to a leaf"),
        }
    }

    #[test]
    fn branch_size() {
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 2], ()>>(
            ),
            64
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 4], ()>>(
            ),
            48 + 16 * 2
        );
        assert_eq!(
            mem::size_of::<Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 8], ()>>(
            ),
            48 + 16 * 4
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 16], ()>,
            >(),
            48 + 16 * 8
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<32, IdentitySchema, ()>>; 32], ()>,
            >(),
            48 + 16 * 16
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 64], ()>,
            >(),
            48 + 16 * 32
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 128], ()>,
            >(),
            48 + 16 * 64
        );
        assert_eq!(
            mem::size_of::<
                Branch<64, IdentitySchema, [Option<Head<64, IdentitySchema, ()>>; 256], ()>,
            >(),
            48 + 16 * 128
        );
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
