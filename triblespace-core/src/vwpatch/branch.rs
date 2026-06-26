use super::*;
use core::sync::atomic;
use core::sync::atomic::Ordering::Acquire;
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::Ordering::Release;
use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::alloc::Layout;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::addr_of;
use std::ptr::addr_of_mut;
use std::sync::Arc;

const BRANCH_ALIGN: usize = 16;
const BRANCH_BASE_SIZE: usize = 64;
const TABLE_ENTRY_SIZE: usize = 8;

/// Marker trait for opaque owners of bytes referenced by archive-backed
/// PATCH nodes. An `Option<Arc<dyn ArchiveOwner>>` lives on each
/// [`Branch`]; when `Some(arc)`, the Arc keeps the underlying bytes
/// (typically a memory-mapped archive blob) alive so that any
/// `LocalLeaf` children — which are thin pointers into those bytes —
/// remain valid for the Branch's lifetime. The trait is intentionally
/// empty: the owner's only job is to drop the bytes when its refcount
/// hits zero.
pub trait ArchiveOwner: Send + Sync + 'static {}

impl<T: Send + Sync + 'static + ?Sized> ArchiveOwner for T {}

/// Fingerprint of a span sub-key, used as the cuckoo child-table key.
///
/// For spans of length `<= 2` the mapping is bijective (the raw byte / the
/// big-endian `u16`), so single-byte spans (phase 2b-i) stay collision-free
/// and the table key equals the raw branch byte — keeping the VWPATCH
/// structurally identical to PATCH. Longer spans (phase 2b-ii) fold through
/// FNV-1a, which is *not* injective; lookups must therefore confirm a
/// fingerprint match against the candidate child's actual span sub-key (see
/// [`Branch::select_child`]).
#[inline]
pub(crate) fn fingerprint16(bytes: &[u8]) -> u16 {
    match bytes {
        [one] => return *one as u16,
        [first, second] => return u16::from_be_bytes([*first, *second]),
        _ => {}
    }

    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h ^ (h >> 32) ^ (h >> 16)) as u16
}

#[inline]
pub(crate) fn dst_len<T>(ptr: *const [T]) -> usize {
    let ptr: *const [()] = ptr as _;
    // SAFETY: There is no aliasing as () is zero-sized
    let slice: &[()] = unsafe { &*ptr };
    slice.len()
}

// Mutable editor for a Branch body. This lives in the branch module and
// encapsulates NonNull/pointer handling for mutating operations. When the
// editor is dropped it automatically writes the final pointer back into the
// owning Head via Head::set_body.
pub(crate) type BranchNN<const KEY_LEN: usize, O, V> =
    NonNull<Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>>;

pub(crate) struct BranchMut<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> {
    head: &'a mut Head<KEY_LEN, O, V>,
    branch_nn: BranchNN<KEY_LEN, O, V>,
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> BranchMut<'a, KEY_LEN, O, V> {
    pub(crate) fn from_head(head: &'a mut Head<KEY_LEN, O, V>) -> Self {
        match head.body_mut() {
            BodyMut::Branch(branch_ref) => {
                let nn = unsafe { NonNull::new_unchecked(branch_ref as *mut _) };
                Self {
                    head,
                    branch_nn: nn,
                }
            }
            BodyMut::Leaf(_) | BodyMut::LocalLeaf(_) => {
                panic!("BranchMut requires a Branch body")
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_slot(slot: &'a mut Option<Head<KEY_LEN, O, V>>) -> Self {
        let head = slot.as_mut().expect("slot should not be empty");
        Self::from_head(head)
    }

    pub fn modify_child<F>(&mut self, key: u16, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        // Delegate to the low-level NonNull based primitive which may grow and
        // update the pointer in-place.
        Branch::modify_child(&mut self.branch_nn, key, f);
    }

    /// Like [`modify_child`] but uses the supplied `inserted_hash`
    /// for the empty-slot insertion case instead of calling
    /// `inserted.hash()`. Lets archive ingest avoid recomputing
    /// the LocalLeaf siphash24 once per index — the caller already
    /// has it from `ArchiveEntry::hash`.
    ///
    /// The hint MUST equal the hash of whatever `f(None)` returns.
    /// When the slot is non-empty and `f(Some(_))` runs, the result
    /// is hashed normally (recursion result, hash already cached on
    /// the Branch).
    pub fn modify_child_with_inserted_hint<F>(&mut self, key: u16, inserted_hash: u128, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Branch::modify_child_with_inserted_hint(&mut self.branch_nn, key, inserted_hash, f);
    }

    /// Insert `head` into the child table, growing the allocation if cuckoo
    /// placement fails. Does *not* update the branch's aggregates —
    /// pair with [`Self::recompute_aggregates`] for bulk rewrites.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub fn install_child_growing(&mut self, head: Head<KEY_LEN, O, V>) {
        unsafe {
            Branch::install_child_growing(&mut self.branch_nn, head);
        }
    }

    /// Rebuild aggregates (hash/leaf_count/segment_count/childleaf) in one
    /// linear pass over `child_table`. Call once after a batch of
    /// [`Self::install_child_growing`] mutations.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub fn recompute_aggregates(&mut self) {
        unsafe {
            Branch::recompute_aggregates(&mut self.branch_nn);
        }
    }

    /// If a child whose span sub-key equals `sub` (fingerprint `fp`) exists,
    /// replace it with `f(child)`, update the branch aggregates incrementally
    /// (XOR hash delta, leaf/segment sums, childleaf refresh), and return
    /// `true`. Otherwise return `false` without touching anything.
    ///
    /// Folding the membership test and the mutable descent into a single
    /// verified slot lookup avoids scanning + span-verifying the cuckoo table
    /// twice on the hot descend path.
    #[must_use]
    pub fn recurse_dense_child<F>(&mut self, fp: u16, sub: &[u8], f: F) -> bool
    where
        F: FnOnce(Head<KEY_LEN, O, V>) -> Head<KEY_LEN, O, V>,
    {
        let span_start = self.span_start as usize;
        let branch_childleaf = self.childleaf;
        let base_hash = self.hash;
        let base_seg = self.segment_count;
        let base_leaf = self.leaf_count;

        let branch = unsafe { self.branch_nn.as_mut() };
        let Some(slot) = branch.child_table.table_get_slot_verified(fp, |child| {
            let ck = child.childleaf_key();
            (0..sub.len()).all(|j| ck[O::TREE_TO_KEY[span_start + j]] == sub[j])
        }) else {
            return false;
        };

        let child = slot.take().unwrap();
        let old_hash = child.hash();
        let old_seg = child.count_segment(span_start);
        let old_leaf = child.count();
        let replaced_childleaf = child.childleaf_ptr() == branch_childleaf;

        let new_child = f(child);
        let new_hash = new_child.hash();
        let new_seg = new_child.count_segment(span_start);
        let new_leaf = new_child.count();
        let new_cl = new_child.childleaf_ptr();
        *slot = Some(new_child.with_key(fp));

        branch.hash = (base_hash ^ old_hash) ^ new_hash;
        branch.segment_count = (base_seg - old_seg) + new_seg;
        branch.leaf_count = (base_leaf - old_leaf) + new_leaf;
        if replaced_childleaf {
            branch.childleaf = new_cl;
        }
        #[cfg(debug_assertions)]
        unsafe {
            self.branch_nn.as_ref().debug_check_invariants();
        }
        true
    }

    /// Add a brand-new distinct child (already keyed by its span fingerprint),
    /// updating aggregates incrementally and growing the table if needed.
    ///
    /// Returns `Some(child)` if cuckoo placement failed within 256 slots; the
    /// table and aggregates are then left unchanged and the caller must narrow
    /// (rebuild). Returns `None` on success.
    #[must_use]
    pub fn add_dense_child(&mut self, child: Head<KEY_LEN, O, V>) -> Option<Head<KEY_LEN, O, V>> {
        let span_start = self.span_start as usize;
        // Metrics captured before the move so they can be applied post-install.
        let add_leaf = child.count();
        let add_seg = child.count_segment(span_start);
        let add_hash = child.hash();
        let add_cl = child.childleaf_ptr();

        if let Some(leftover) =
            unsafe { Branch::install_child_growing_dup(&mut self.branch_nn, child) }
        {
            return Some(leftover);
        }

        let branch = unsafe { self.branch_nn.as_mut() };
        branch.leaf_count += add_leaf;
        branch.segment_count += add_seg;
        branch.hash ^= add_hash;
        if branch.childleaf.is_null() {
            branch.childleaf = add_cl;
        }
        #[cfg(debug_assertions)]
        unsafe {
            self.branch_nn.as_ref().debug_check_invariants();
        }
        None
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Deref for BranchMut<'a, KEY_LEN, O, V> {
    type Target = Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.branch_nn.as_ref() }
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> DerefMut for BranchMut<'a, KEY_LEN, O, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.branch_nn.as_mut() }
    }
}

impl<'a, const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Drop for BranchMut<'a, KEY_LEN, O, V> {
    fn drop(&mut self) {
        // Commit the final branch pointer into the owning Head.
        self.head.set_body(self.branch_nn);
    }
}

#[repr(C, align(16))]
pub(crate) struct Branch<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, Table: ?Sized, V> {
    key_ordering: PhantomData<O>,
    key_segments: PhantomData<O::Segmentation>,
    /// Phantom `V`: the value type is no longer stored on the branch
    /// itself (the childleaf is just `*const [u8; KEY_LEN]`), but it
    /// stays carried so child `Head<KEY_LEN, O, V>` slots in
    /// `child_table` and the `Body` impl for the concrete child-table
    /// shape stay generic in `V`.
    _value: PhantomData<fn() -> V>,

    rc: atomic::AtomicU32,
    /// Divergence depth — where this branch's children begin to differ.
    /// All children share the tree-ordered bytes in `[parent_depth,
    /// span_start)` (the compressed path) and branch on the span
    /// `[span_start, span_end)`, whose fingerprint selects the child.
    pub span_start: u16,
    /// One-past-the-end of the branch span. In phase 2b-i branching stays
    /// single-byte, so `span_end == span_start + 1`; phase 2b-ii widens the
    /// span for dense nodes. Depths range over `0..=KEY_LEN` (fits `u16`).
    pub span_end: u16,
    /// Thin pointer to the key bytes of a representative descendant
    /// leaf, used for prefix-matching shortcuts. Points either into a
    /// heap [`Leaf`]'s inline `key` field (offset 0 thanks to
    /// `#[repr(C)]`) or into archive memory referenced by a
    /// `LocalLeaf`. The unified `*const [u8; KEY_LEN]` representation
    /// lets both leaf flavors serve as the childleaf.
    pub childleaf: *const [u8; KEY_LEN],
    pub leaf_count: u64,
    pub segment_count: u64,
    pub hash: u128,
    /// Owner reference keeping `LocalLeaf` children's underlying bytes alive.
    /// `None` for pure-memory branches; `Some(arc)` for archive-backed
    /// branches. Niche-optimized to 16 bytes via the inner Arc's `NonNull`
    /// data pointer — no discriminator byte. See [`ArchiveOwner`].
    pub owner: Option<Arc<dyn ArchiveOwner>>,
    pub child_table: Table,
}

// Manual Debug since `Option<Arc<dyn ArchiveOwner>>` doesn't impl Debug
// (the trait is intentionally minimal — no Debug bound).
impl<
        const KEY_LEN: usize,
        O: KeySchema<KEY_LEN>,
        Table: ?Sized + core::fmt::Debug,
        V: core::fmt::Debug,
    > core::fmt::Debug for Branch<KEY_LEN, O, Table, V>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Branch")
            .field("rc", &self.rc)
            .field("span_start", &self.span_start)
            .field("span_end", &self.span_end)
            .field("childleaf", &self.childleaf)
            .field("leaf_count", &self.leaf_count)
            .field("segment_count", &self.segment_count)
            .field("hash", &self.hash)
            .field("owner", &self.owner.as_ref().map(|_| "<archive owner>"))
            .field("child_table", &&self.child_table)
            .finish()
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, Table: ?Sized, V> Branch<KEY_LEN, O, Table, V> {
    /// Returns the key bytes of the representative child leaf. The
    /// pointer is set to a heap `Leaf`'s `key` field (offset 0) or to
    /// a `LocalLeaf`'s archive-resident bytes; both yield the same
    /// reference shape.
    pub fn childleaf_key(&self) -> &[u8; KEY_LEN] {
        unsafe { &*self.childleaf }
    }

    /// Returns the raw key-bytes pointer of the representative child
    /// leaf. Used for pointer-identity comparisons during invariant
    /// checks and for propagating the representative through
    /// branch-construction paths.
    pub fn childleaf_ptr(&self) -> *const [u8; KEY_LEN] {
        self.childleaf
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V> Body
    for Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>
{
    fn tag(body: NonNull<Self>) -> HeadTag {
        unsafe {
            let ptr = addr_of!((*body.as_ptr()).child_table);
            let exp = dst_len(ptr).ilog2() as u8;
            debug_assert!((1..=8).contains(&exp));
            HeadTag::from_raw(exp)
        }
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, V>
    Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>
{
    pub(super) fn new(
        span_start: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
    ) -> NonNull<Self> {
        Self::new_with_owner(span_start, lchild, rchild, None)
    }

    /// Like [`Self::new`] but sets the branch's `owner` field — used by
    /// the archive-leaf-elimination path so that a Branch created when
    /// inserting a `LocalLeaf` adopts the entry's archive owner.
    pub(super) fn new_with_owner(
        span_start: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
        owner: Option<Arc<dyn ArchiveOwner>>,
    ) -> NonNull<Self> {
        // Compute rchild's hash via the normal path. For LocalLeaf
        // this triggers siphash24; the
        // [`new_with_owner_and_rchild_hash`] variant skips it when
        // the caller has the hash already.
        let rchild_hash = rchild.hash();
        Self::new_with_owner_and_rchild_hash(span_start, lchild, rchild, owner, rchild_hash)
    }

    /// Variant of [`Self::new_with_owner`] that takes a precomputed
    /// `rchild_hash` and uses it instead of calling `rchild.hash()`.
    /// Lets archive-ingest divergence paths reuse the
    /// `ArchiveEntry::hash` they already have instead of recomputing
    /// siphash24 over the LocalLeaf bytes.
    ///
    /// `rchild_hash` MUST equal `rchild.hash()`. The lchild hash
    /// still goes through the normal path — it's typically a Branch
    /// (cached) or heap Leaf (cached), so the only LocalLeaf hash
    /// recompute that matters is on the freshly inserted side.
    pub(super) fn new_with_owner_and_rchild_hash(
        span_start: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
        owner: Option<Arc<dyn ArchiveOwner>>,
        rchild_hash: u128,
    ) -> NonNull<Self> {
        unsafe {
            // The smallest table is one full bucket (`BUCKET_ENTRY_COUNT`
            // slots): it is a single bucket, so the cheap/rand hashes both
            // compress to bucket 0 and the two children placed directly at
            // slots 0 and 1 are always found by a full-bucket scan. (Two-slot
            // tables are unrepresentable with four-slot buckets.)
            let size = BUCKET_ENTRY_COUNT;
            // SAFETY: `BRANCH_ALIGN` is a power of two and `size` is small enough
            // that the computed layout size is valid.
            let layout = Layout::from_size_align_unchecked(
                BRANCH_BASE_SIZE + (TABLE_ENTRY_SIZE * size),
                BRANCH_ALIGN,
            );
            let Some(ptr) =
                NonNull::new(std::ptr::slice_from_raw_parts(alloc_zeroed(layout), size)
                    as *mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>)
            else {
                handle_alloc_error(layout);
            };
            addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(1));
            // Single-byte span (phase 2b-i): span_end == span_start + 1.
            addr_of_mut!((*ptr.as_ptr()).span_start).write(span_start as u16);
            addr_of_mut!((*ptr.as_ptr()).span_end).write((span_start + 1) as u16);
            addr_of_mut!((*ptr.as_ptr()).childleaf).write(lchild.childleaf_ptr());
            addr_of_mut!((*ptr.as_ptr()).leaf_count).write(lchild.count() + rchild.count());
            addr_of_mut!((*ptr.as_ptr()).segment_count)
                .write(lchild.count_segment(span_start) + rchild.count_segment(span_start));
            addr_of_mut!((*ptr.as_ptr()).hash).write(lchild.hash() ^ rchild_hash);
            addr_of_mut!((*ptr.as_ptr()).owner).write(owner);
            (*ptr.as_ptr()).child_table[0] = Some(lchild);
            (*ptr.as_ptr()).child_table[1] = Some(rchild);

            ptr
        }
    }

    pub(super) unsafe fn rc_inc(branch: NonNull<Self>) -> NonNull<Self> {
        unsafe {
            let branch = branch.as_ptr();
            let mut current = (*branch).rc.load(Relaxed);
            loop {
                if current == u32::MAX {
                    panic!("max refcount exceeded");
                }
                match (*branch)
                    .rc
                    .compare_exchange(current, current + 1, Relaxed, Relaxed)
                {
                    Ok(_) => return NonNull::new_unchecked(branch),
                    Err(v) => current = v,
                }
            }
        }
    }

    pub(super) unsafe fn rc_dec(branch: NonNull<Self>) {
        unsafe {
            let branch = branch.as_ptr();
            if (*branch).rc.fetch_sub(1, Release) != 1 {
                return;
            }
            (*branch).rc.load(Acquire);

            let size = dst_len(addr_of!((*branch).child_table));

            std::ptr::drop_in_place(branch);

            // SAFETY: layout parameters are constructed from constants and a
            // runtime `size` that ensures alignment and size validity.
            let layout = Layout::from_size_align_unchecked(
                BRANCH_BASE_SIZE + (TABLE_ENTRY_SIZE * size),
                BRANCH_ALIGN,
            );
            let ptr = branch as *mut u8;
            dealloc(ptr, layout);
        }
    }

    /// Ensure the branch is uniquely owned. If it is shared (rc > 1) a
    /// copy is allocated and `*branch_nn` is updated to point to the new unique
    /// allocation. Returns `Some(())` if a copy was made, or `None` if the
    /// branch was already unique.
    pub(super) unsafe fn rc_cow(branch_nn: &mut NonNull<Self>) -> Option<()> {
        unsafe {
            let branch = branch_nn.as_ptr();
            if (*branch).rc.load(Acquire) == 1 {
                None
            } else {
                let size = dst_len(addr_of!((*branch).child_table));
                // SAFETY: `size` preserves alignment requirements and the size
                // calculation cannot overflow for the allowed range.
                let layout = Layout::from_size_align_unchecked(
                    BRANCH_BASE_SIZE + (TABLE_ENTRY_SIZE * size),
                    BRANCH_ALIGN,
                );
                if let Some(ptr) =
                    NonNull::new(std::ptr::slice_from_raw_parts(alloc_zeroed(layout), size)
                        as *mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>)
                {
                    addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(1));
                    addr_of_mut!((*ptr.as_ptr()).span_start).write((*branch).span_start);
                    addr_of_mut!((*ptr.as_ptr()).span_end).write((*branch).span_end);
                    addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                    addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                    addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                    addr_of_mut!((*ptr.as_ptr()).hash).write((*branch).hash);
                    addr_of_mut!((*ptr.as_ptr()).owner).write((*branch).owner.clone());
                    (*ptr.as_ptr())
                        .child_table
                        .clone_from_slice(&(*branch).child_table);

                    Self::rc_dec(NonNull::new_unchecked(branch));
                    *branch_nn = ptr;
                    Some(())
                } else {
                    handle_alloc_error(layout);
                }
            }
        }
    }

    /// Grow the branch's allocation in-place by updating the provided
    /// `branch_nn` to point to a larger allocation. The caller must provide a
    /// mutable reference to the owned pointer; this function updates it when a
    /// new allocation is made.
    pub(crate) fn grow(branch_nn: &mut NonNull<Self>) {
        unsafe {
            let branch = branch_nn.as_ptr();
            let old_size = dst_len(addr_of!((*branch).child_table));
            let new_size = old_size * 2;
            assert!(new_size <= 256);

            // SAFETY: `new_size` is bounded and alignment is constant, so the
            // resulting layout is valid for allocation.
            let layout = Layout::from_size_align_unchecked(
                BRANCH_BASE_SIZE + (TABLE_ENTRY_SIZE * new_size),
                BRANCH_ALIGN,
            );
            if let Some(ptr) = NonNull::new(std::ptr::slice_from_raw_parts(
                alloc_zeroed(layout),
                new_size,
            )
                as *mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>)
            {
                addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(1));
                addr_of_mut!((*ptr.as_ptr()).span_start).write((*branch).span_start);
                addr_of_mut!((*ptr.as_ptr()).span_end).write((*branch).span_end);
                addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                addr_of_mut!((*ptr.as_ptr()).hash).write((*branch).hash);
                addr_of_mut!((*ptr.as_ptr()).owner).write((*branch).owner.clone());
                // Note that the child_table is already zeroed by the allocator and therefore None initialized.

                (*branch)
                    .child_table
                    .table_grow(&mut (*ptr.as_ptr()).child_table);

                Branch::<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>::rc_dec(
                    NonNull::new_unchecked(branch),
                );

                *branch_nn = ptr;
            } else {
                handle_alloc_error(layout);
            }
        }
    }

    // Insert-child helper removed — use `modify_child` which consolidates
    // insert/update/remove logic and handles potential growth in-place.

    /// Generalized modify/insert/remove primitive for a child slot.
    ///
    /// The closure receives the current child if present (Some) or None when
    /// the slot is empty and should return the new child to place into the
    /// slot (Some) or None to remove/leave empty. This consolidates the
    /// insert/update/remove logic in one place and updates branch aggregates
    /// and `childleaf` as needed. The `branch_nn` pointer may be updated in
    /// place when the underlying allocation grows.
    pub(super) fn modify_child<F>(branch_nn: &mut NonNull<Self>, key: u16, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        unsafe {
            let branch = branch_nn.as_ptr();
            // Segment accounting keys off the branch's divergence depth.
            let span_start = (*branch).span_start as usize;

            // If a slot exists, operate on the existing child in-place.
            if let Some(slot) = (*branch).child_table.table_get_slot(key) {
                let child = slot.take().unwrap();
                let old_child_hash = child.hash();
                let old_child_segment_count = child.count_segment(span_start);
                let old_child_leaf_count = child.count();

                let replaced_childleaf = child.childleaf_ptr() == (*branch).childleaf;

                if let Some(new_child) = f(Some(child)) {
                    // Replace existing child
                    (*branch).hash = ((*branch).hash ^ old_child_hash) ^ new_child.hash();
                    (*branch).segment_count = ((*branch).segment_count - old_child_segment_count)
                        + new_child.count_segment(span_start);
                    (*branch).leaf_count =
                        ((*branch).leaf_count - old_child_leaf_count) + new_child.count();

                    if replaced_childleaf {
                        (*branch).childleaf = new_child.childleaf_ptr();
                    }

                    if slot.replace(new_child.with_key(key)).is_some() {
                        unreachable!();
                    }
                } else {
                    // Remove existing child
                    (*branch).hash ^= old_child_hash;
                    (*branch).segment_count -= old_child_segment_count;
                    (*branch).leaf_count -= old_child_leaf_count;

                    if replaced_childleaf {
                        if let Some(other) = (*branch).child_table.iter().find_map(|s| s.as_ref()) {
                            (*branch).childleaf = other.childleaf_ptr();
                        }
                    }
                }
            } else {
                // No current slot — the closure can choose to insert a child.
                if let Some(mut inserted) = f(None) {
                    // The caller is expected to pass an inserted Head that is
                    // already prepared (with_start set to the appropriate depth).
                    // Update aggregates before attempting insertion.
                    (*branch).leaf_count += inserted.count();
                    (*branch).segment_count += inserted.count_segment(span_start);
                    (*branch).hash ^= inserted.hash();

                    // Cuckoo insert loop, growing the table when necessary.
                    let mut branch_ptr = branch_nn.as_ptr();
                    while let Some(new_displaced) = (*branch_ptr).child_table.table_insert(inserted)
                    {
                        inserted = new_displaced;
                        Self::grow(branch_nn);
                        // Refresh local pointer after potential reallocation.
                        branch_ptr = branch_nn.as_ptr();
                    }
                }
            }
            // Debug invariant check (no-op in release builds).
            #[cfg(debug_assertions)]
            branch_nn.as_ref().debug_check_invariants();
        }
    }

    /// Variant of [`Self::modify_child`] that takes a precomputed
    /// `inserted_hash` and uses it for the empty-slot insertion path
    /// instead of calling `inserted.hash()`. The hint MUST equal the
    /// hash of whatever `f(None)` returns. The non-empty path uses
    /// `new_child.hash()` as normal (the recursive result is a Branch
    /// whose hash is already cached, so the call is O(1)).
    pub(super) fn modify_child_with_inserted_hint<F>(
        branch_nn: &mut NonNull<Self>,
        key: u16,
        inserted_hash: u128,
        f: F,
    ) where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        unsafe {
            let branch = branch_nn.as_ptr();
            let span_start = (*branch).span_start as usize;

            if let Some(slot) = (*branch).child_table.table_get_slot(key) {
                let child = slot.take().unwrap();
                let old_child_hash = child.hash();
                let old_child_segment_count = child.count_segment(span_start);
                let old_child_leaf_count = child.count();

                let replaced_childleaf = child.childleaf_ptr() == (*branch).childleaf;

                if let Some(new_child) = f(Some(child)) {
                    // Recursion result — its hash is cached on the
                    // returned Head (Branch.hash field), so calling
                    // .hash() is cheap.
                    (*branch).hash = ((*branch).hash ^ old_child_hash) ^ new_child.hash();
                    (*branch).segment_count = ((*branch).segment_count - old_child_segment_count)
                        + new_child.count_segment(span_start);
                    (*branch).leaf_count =
                        ((*branch).leaf_count - old_child_leaf_count) + new_child.count();

                    if replaced_childleaf {
                        (*branch).childleaf = new_child.childleaf_ptr();
                    }

                    if slot.replace(new_child.with_key(key)).is_some() {
                        unreachable!();
                    }
                } else {
                    (*branch).hash ^= old_child_hash;
                    (*branch).segment_count -= old_child_segment_count;
                    (*branch).leaf_count -= old_child_leaf_count;

                    if replaced_childleaf {
                        if let Some(other) = (*branch).child_table.iter().find_map(|s| s.as_ref()) {
                            (*branch).childleaf = other.childleaf_ptr();
                        }
                    }
                }
            } else {
                if let Some(mut inserted) = f(None) {
                    // Use the caller-supplied hint instead of
                    // recomputing siphash24 over the LocalLeaf bytes.
                    (*branch).leaf_count += inserted.count();
                    (*branch).segment_count += inserted.count_segment(span_start);
                    (*branch).hash ^= inserted_hash;

                    let mut branch_ptr = branch_nn.as_ptr();
                    while let Some(new_displaced) = (*branch_ptr).child_table.table_insert(inserted)
                    {
                        inserted = new_displaced;
                        Self::grow(branch_nn);
                        branch_ptr = branch_nn.as_ptr();
                    }
                }
            }
            #[cfg(debug_assertions)]
            branch_nn.as_ref().debug_check_invariants();
        }
    }

    // Note: upsert_child removed in favor of explicit insert_child / update_child

    // The old in-place `update_child` helper has been superseded by
    // `modify_child` which accepts an Option<Head> and handles insert/update/remove
    // uniformly. The thin adapter was removed to centralize behavior; callers
    // should use `modify_child` or BranchMut::modify_child.

    /// Insert `head` into the child table, growing if cuckoo placement
    /// fails. Does NOT touch aggregates — used by bulk-rewrite paths
    /// that recompute aggregates in one pass at the end via
    /// [`recompute_aggregates`](Self::recompute_aggregates).
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) unsafe fn install_child_growing(
        branch_nn: &mut NonNull<Self>,
        head: Head<KEY_LEN, O, V>,
    ) {
        let mut to_insert = head;
        let mut branch_ptr = branch_nn.as_ptr();
        while let Some(displaced) = (*branch_ptr).child_table.table_insert(to_insert) {
            to_insert = displaced;
            Self::grow(branch_nn);
            branch_ptr = branch_nn.as_ptr();
        }
    }

    /// Like [`install_child_growing`] but allows the child's fingerprint key
    /// to already exist in the table (a true span fingerprint collision
    /// between distinct sub-keys — possible once spans go multi-byte).
    ///
    /// Returns `Some(head)` if the child could not be placed within the
    /// 256-slot maximum (a cuckoo placement failure — arbitrary fingerprints
    /// can be unplaceable even below the load threshold). On failure the table
    /// is left set-unchanged (cuckoo displacement backtracks cleanly), so the
    /// caller can narrow the span and rebuild. Returns `None` on success.
    #[must_use]
    pub(crate) unsafe fn install_child_growing_dup(
        branch_nn: &mut NonNull<Self>,
        head: Head<KEY_LEN, O, V>,
    ) -> Option<Head<KEY_LEN, O, V>> {
        let mut to_insert = head;
        loop {
            let branch_ptr = branch_nn.as_ptr();
            match (*branch_ptr).child_table.table_insert_allow_dup(to_insert) {
                None => return None,
                Some(displaced) => {
                    if dst_len(addr_of!((*branch_ptr).child_table)) >= 256 {
                        return Some(displaced);
                    }
                    to_insert = displaced;
                    Self::grow(branch_nn);
                }
            }
        }
    }

    /// Allocate a fresh dense branch spanning `[span_start, span_end)` and
    /// install the already-keyed `children` (each child's `key()` must be the
    /// fingerprint of its span sub-key over this span). Aggregates are rebuilt
    /// in one pass. The initial table is sized to the next power of two that
    /// holds all children, so the common case installs without a grow.
    ///
    /// Returns `None` if the children cannot be cuckoo-placed within the
    /// 256-slot maximum; the caller must then narrow the span and rebuild. On
    /// failure all allocated state (the partial branch and any not-yet-installed
    /// children) is dropped cleanly.
    ///
    /// Used only by the variable-width dense insert / rebuild paths.
    pub(super) fn from_children_dense(
        span_start: usize,
        span_end: usize,
        children: Vec<Head<KEY_LEN, O, V>>,
    ) -> Option<NonNull<Self>> {
        debug_assert!(children.len() >= 2, "dense branch needs >= 2 children");
        debug_assert!(children.len() <= 256, "dense fanout exceeds 256");
        // Start at one full bucket (`BUCKET_ENTRY_COUNT`); doubling keeps every
        // table a power-of-two number of slots up to the 256 maximum.
        let mut size = BUCKET_ENTRY_COUNT;
        while size < children.len() {
            size *= 2;
        }
        unsafe {
            let layout = Layout::from_size_align_unchecked(
                BRANCH_BASE_SIZE + (TABLE_ENTRY_SIZE * size),
                BRANCH_ALIGN,
            );
            let Some(mut ptr) =
                NonNull::new(std::ptr::slice_from_raw_parts(alloc_zeroed(layout), size)
                    as *mut Branch<KEY_LEN, O, [Option<Head<KEY_LEN, O, V>>], V>)
            else {
                handle_alloc_error(layout);
            };
            addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(1));
            addr_of_mut!((*ptr.as_ptr()).span_start).write(span_start as u16);
            addr_of_mut!((*ptr.as_ptr()).span_end).write(span_end as u16);
            addr_of_mut!((*ptr.as_ptr()).childleaf).write(std::ptr::null());
            addr_of_mut!((*ptr.as_ptr()).leaf_count).write(0);
            addr_of_mut!((*ptr.as_ptr()).segment_count).write(0);
            addr_of_mut!((*ptr.as_ptr()).hash).write(0);
            addr_of_mut!((*ptr.as_ptr()).owner).write(None);
            for child in children {
                if let Some(leftover) = Self::install_child_growing_dup(&mut ptr, child) {
                    // Placement failed. Drop the leftover and the partially
                    // built branch (which rc_dec's the already-installed
                    // children); the for-loop's iterator drops the rest.
                    drop(leftover);
                    Self::rc_dec(ptr);
                    return None;
                }
            }
            Self::recompute_aggregates(&mut ptr);
            Some(ptr)
        }
    }

    /// Rebuild aggregate fields (`hash`, `leaf_count`, `segment_count`,
    /// `childleaf`) from the current child table in one linear pass.
    /// Cheaper than paying `modify_child`'s per-call accounting when
    /// many children are being installed in bulk.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) unsafe fn recompute_aggregates(branch_nn: &mut NonNull<Self>) {
        let branch = branch_nn.as_ptr();
        let span_start = (*branch).span_start as usize;
        let mut agg_leaf_count: u64 = 0;
        let mut agg_segment_count: u64 = 0;
        let mut agg_hash: u128 = 0;
        let mut first_childleaf: *const [u8; KEY_LEN] = std::ptr::null();

        for child in (*branch).child_table.iter().flatten() {
            agg_leaf_count += child.count();
            agg_segment_count += child.count_segment(span_start);
            agg_hash ^= child.hash();
            if first_childleaf.is_null() {
                first_childleaf = child.childleaf_ptr();
            }
        }

        (*branch).leaf_count = agg_leaf_count;
        (*branch).segment_count = agg_segment_count;
        (*branch).hash = agg_hash;
        if !first_childleaf.is_null() {
            (*branch).childleaf = first_childleaf;
        }

        #[cfg(debug_assertions)]
        branch_nn.as_ref().debug_check_invariants();
    }

    pub fn count_segment(&self, at_depth: usize) -> u64 {
        let node_end = self.span_start as usize;
        if !O::same_segment_tree(at_depth, node_end) {
            1
        } else {
            self.segment_count
        }
    }

    /// Debug-only invariant checker. Validates that the aggregate fields
    /// (leaf_count, segment_count, hash, childleaf) are consistent with the
    /// current child table. Exists only in debug builds so it adds zero
    /// overhead in release binaries.
    #[cfg(debug_assertions)]
    pub fn debug_check_invariants(&self) {
        let span_start: usize = self.span_start as usize;
        let mut agg_leaf_count: u64 = 0;
        let mut agg_segment_count: u64 = 0;
        let mut agg_hash: u128 = 0;
        let mut match_found = false;

        for child in self.child_table.iter().flatten() {
            agg_leaf_count = agg_leaf_count.saturating_add(child.count());
            agg_segment_count = agg_segment_count.saturating_add(child.count_segment(span_start));
            agg_hash ^= child.hash();
            if child.childleaf_ptr() == self.childleaf {
                match_found = true;
            }
        }

        debug_assert_eq!(
            agg_leaf_count, self.leaf_count,
            "branch.leaf_count mismatch"
        );
        debug_assert_eq!(
            agg_segment_count, self.segment_count,
            "branch.segment_count mismatch"
        );
        debug_assert_eq!(agg_hash, self.hash, "branch.hash mismatch");

        // If there are any leaves aggregated in this branch then the
        // `childleaf` pointer must match one of the children. When the
        // aggregate count is zero the equality check above already guarantees
        // `self.leaf_count == 0`, so the explicit empty-branch assertion is
        // redundant and can be omitted.
        if agg_leaf_count > 0 {
            debug_assert!(match_found, "branch.childleaf pointer mismatch");
        }
    }

    /// Fingerprint the query's span sub-key (the tree-ordered bytes
    /// `query[span_start..span_end)`), then return the child whose own span
    /// sub-key actually equals it.
    ///
    /// A fingerprint match is only a *candidate*: `fingerprint16` is not
    /// injective for multi-byte spans, so several distinct span sub-keys may
    /// collide on one `u16`. [`ByteTable::table_get_verified`] scans every
    /// table entry sharing the fingerprint and confirms each candidate's real
    /// span sub-key (recovered from its `childleaf` over `[span_start,
    /// span_end)`). `query` is the tree-ordered query key/prefix and must be
    /// at least `span_end` bytes long (guaranteed at the descent call sites by
    /// the segment-alignment invariant: a span never crosses a checkpoint, so
    /// any segment-aligned prefix that reaches into the span reaches past it).
    #[inline]
    pub(super) fn select_child(&self, query: &[u8]) -> Option<&Head<KEY_LEN, O, V>> {
        let s = self.span_start as usize;
        let e = self.span_end as usize;
        let sub = &query[s..e];
        let fp = fingerprint16(sub);
        self.child_table.table_get_verified(fp, |child| {
            let ck = child.childleaf_key();
            (0..(e - s)).all(|j| ck[O::TREE_TO_KEY[s + j]] == sub[j])
        })
    }

    /// Return true if this branch's childleaf key matches the provided
    /// `prefix` for all tree-ordered bytes in [at_depth, PREFIX_LEN).
    pub fn infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        // Early-prune: if the branch's representative childleaf doesn't match
        // the prefix then no child in this branch can match.
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        // If the branch's representative childleaf does NOT match the
        // provided prefix then no child in this branch can match and we can
        // early-return. The previous logic inverted this check which caused
        // branches to be pruned incorrectly.
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return;
        }

        // The infix ends within the current node's compressed path.
        if PREFIX_LEN + INFIX_LEN <= span_start {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            f(&infix);
            return;
        }
        // The prefix ends in a child of this node — fingerprint-select the
        // span child and descend past the span (to span_end).
        if PREFIX_LEN > span_start {
            if let Some(child) = self.select_child(prefix) {
                child.infixes(prefix, span_end, f);
            }
            return;
        }

        // The prefix ends in this node, but the infix ends in a child.
        for entry in self.child_table.iter().flatten() {
            entry.infixes(prefix, span_end, f);
        }
    }

    /// Like [`infixes`](Self::infixes) but only yields infixes in the
    /// byte range `[min_infix, max_infix]` (inclusive).
    ///
    /// In Case 3 (prefix ends in this node, infix in children), filters
    /// children by their byte key against the range bounds at the current
    /// depth, pruning entire subtrees outside the range.
    pub fn infixes_range<const PREFIX_LEN: usize, const INFIX_LEN: usize, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return;
        }

        // Case 1: infix ends within this node's compressed path — extract and
        // range-check.
        if PREFIX_LEN + INFIX_LEN <= span_start {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            if &infix >= min_infix && &infix <= max_infix {
                f(&infix);
            }
            return;
        }

        // Case 2: prefix extends into a specific child.
        if PREFIX_LEN > span_start {
            if let Some(child) = self.select_child(prefix) {
                child.infixes_range(prefix, span_end, min_infix, max_infix, f);
            }
            return;
        }

        // Case 3: prefix ends here, infix spans children.
        // First check the compressed path (bytes PREFIX_LEN..span_start)
        // against the range. All children share these bytes (path compression).
        let infix_byte_idx = span_start - PREFIX_LEN;
        let mut min_tight = true; // still on the min boundary
        let mut max_tight = true; // still on the max boundary
        for i in 0..infix_byte_idx {
            let path_byte = self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]];
            if min_tight {
                if path_byte < min_infix[i] {
                    return;
                } // whole branch below min
                if path_byte > min_infix[i] {
                    min_tight = false;
                } // safely above min
            }
            if max_tight {
                if path_byte > max_infix[i] {
                    return;
                } // whole branch above max
                if path_byte < max_infix[i] {
                    max_tight = false;
                } // safely below max
            }
        }

        // Now iterate children, filtering by their byte at infix_byte_idx
        // only when we're still tight on that boundary.
        for entry in self.child_table.iter().flatten() {
            let child_byte = entry.key() as u8;
            if min_tight && infix_byte_idx < INFIX_LEN && child_byte < min_infix[infix_byte_idx] {
                continue;
            }
            if max_tight && infix_byte_idx < INFIX_LEN && child_byte > max_infix[infix_byte_idx] {
                continue;
            }
            entry.infixes_range(prefix, span_end, min_infix, max_infix, f);
        }
    }

    /// Count leaves whose infix falls within [min_infix, max_infix].
    ///
    /// Counts **distinct first-segment values** under this branch whose
    /// infix falls within `[min_infix, max_infix]` — matching the
    /// cardinality that `infixes_range` would yield for the same range.
    ///
    /// Interior children (strictly inside the range at the current byte)
    /// contribute their cached `segment_count` via [`count_segment`]
    /// without recursion. Only the min- and max-boundary children recurse
    /// deeper.
    pub fn count_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> u64 {
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return 0;
        }

        // Case 1: infix ends within this node's compressed path. The full
        // infix is determined by this branch's path, so every leaf below
        // shares it — exactly one distinct infix value exists under self.
        if PREFIX_LEN + INFIX_LEN <= span_start {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            return if &infix >= min_infix && &infix <= max_infix {
                1
            } else {
                0
            };
        }

        // Case 2: prefix extends into a specific child.
        if PREFIX_LEN > span_start {
            if let Some(child) = self.select_child(prefix) {
                return child.count_range(prefix, span_end, min_infix, max_infix);
            }
            return 0;
        }

        // Case 3: prefix ends here, infix spans children.
        // Check compressed path against range (same logic as infixes_range).
        let infix_byte_idx = span_start - PREFIX_LEN;
        let mut min_tight = true;
        let mut max_tight = true;
        for i in 0..infix_byte_idx {
            let path_byte = self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]];
            if min_tight {
                if path_byte < min_infix[i] {
                    return 0;
                }
                if path_byte > min_infix[i] {
                    min_tight = false;
                }
            }
            if max_tight {
                if path_byte > max_infix[i] {
                    return 0;
                }
                if path_byte < max_infix[i] {
                    max_tight = false;
                }
            }
        }

        let mut total = 0u64;
        for entry in self.child_table.iter().flatten() {
            let child_byte = entry.key() as u8;
            let below_min = min_tight && child_byte < min_infix[infix_byte_idx];
            let above_max = max_tight && child_byte > max_infix[infix_byte_idx];
            if below_min || above_max {
                continue;
            }
            let on_min = min_tight && child_byte == min_infix[infix_byte_idx];
            let on_max = max_tight && child_byte == max_infix[infix_byte_idx];
            if on_min || on_max {
                // Boundary child — descend past the span to recount.
                total += entry.count_range(prefix, span_end, min_infix, max_infix);
            } else {
                // Interior child — its cached segment_count is relative to
                // this branch's divergence depth (span_start), matching how
                // `Branch::new`/`modify_child` aggregated it.
                total += entry.count_segment(span_start);
            }
        }
        total
    }

    pub fn has_prefix<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> bool {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return false;
        }

        if PREFIX_LEN <= span_start {
            return true;
        }

        if let Some(child) = self.select_child(prefix) {
            return child.has_prefix::<PREFIX_LEN>(span_end, prefix);
        }

        false
    }

    pub(crate) fn traversal_depth<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> usize {
        const {
            assert!(PREFIX_LEN <= KEY_LEN);
        }
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return 1;
        }

        if PREFIX_LEN <= span_start {
            return 1;
        }

        if let Some(child) = self.select_child(prefix) {
            return 1 + child.traversal_depth::<PREFIX_LEN>(span_end, prefix);
        }

        1
    }

    pub fn get<'a>(&'a self, at_depth: usize, key: &[u8; KEY_LEN]) -> Option<&'a V>
    where
        O: 'a,
    {
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(KEY_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &key[..limit],
        ) {
            return None;
        }
        if span_start >= KEY_LEN {
            // Childleaf prefix matched and span_start == KEY_LEN means the
            // representative IS the lookup target. For ZST `V` (the only
            // shape compatible with `LocalLeaf`-backed childleaves) we
            // synthesize a reference from a dangling pointer; otherwise
            // the childleaf points at a heap `Leaf<KEY_LEN, V>` whose
            // `key` field is at offset 0, so casting recovers the Leaf.
            if std::mem::size_of::<V>() == 0 {
                return Some(unsafe { std::ptr::NonNull::<V>::dangling().as_ref() });
            }
            let leaf_ptr = self.childleaf as *const Leaf<KEY_LEN, V>;
            return Some(unsafe { &(*leaf_ptr).value });
        }

        if let Some(child) = self.select_child(key) {
            return child.get(span_end, key);
        }
        None
    }

    pub fn segmented_len<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        let span_start = self.span_start as usize;
        let span_end = self.span_end as usize;
        let limit = std::cmp::min(PREFIX_LEN, span_start);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return 0;
        }
        if PREFIX_LEN <= span_start {
            if !O::same_segment_tree(PREFIX_LEN, span_start) {
                return 1;
            } else {
                return self.segment_count;
            }
        }
        if let Some(child) = self.select_child(prefix) {
            child.segmented_len::<PREFIX_LEN>(span_end, prefix)
        } else {
            0
        }
    }

    // Instance methods implemented directly on &Branch — these contain any
    // required unsafe access (childleaf deref) locally and avoid forwarding
    // through more wrappers. This keeps the call graph minimal and makes the
    // logic easier to maintain.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole archive-leaf-elimination design depends on
    /// `Option<Arc<dyn ArchiveOwner>>` niche-optimizing to exactly 16
    /// bytes (no discriminator byte added). The inner `Arc<dyn Trait>`
    /// is a fat pointer (data + vtable) whose data pointer is `NonNull`,
    /// so `None` is represented by a null data pointer — same width as
    /// `Some`. If this size ever increases, the Branch struct grows
    /// silently and the design's cost analysis no longer holds; surface
    /// the regression here.
    #[test]
    fn option_arc_dyn_archive_owner_is_sixteen_bytes() {
        assert_eq!(
            std::mem::size_of::<Option<Arc<dyn ArchiveOwner>>>(),
            16,
            "Option<Arc<dyn ArchiveOwner>> must niche-optimize to 16 bytes"
        );
    }
}
