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

const BRANCH_ALIGN: usize = 16;
const BRANCH_BASE_SIZE: usize = 48;
const TABLE_ENTRY_SIZE: usize = 8;
const HASH_KNOWN: u32 = 1 << 31;
const RC_MASK: u32 = HASH_KNOWN - 1;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ChildEditSemantics {
    Arbitrary,
    MonotoneSetGrowth,
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

    pub fn modify_child<F>(&mut self, key: u8, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        // Delegate to the low-level NonNull based primitive which may grow and
        // update the pointer in-place.
        Branch::modify_child(&mut self.branch_nn, key, f);
    }

    /// Modify a child with an operation whose result key set is guaranteed to
    /// contain the input key set. Equal pre/post leaf counts then prove an
    /// exact semantic no-op, so a resident parent hash can be retained without
    /// consulting either child hash. `union` and insertion satisfy this
    /// contract; replacement, removal, and difference do not.
    pub fn modify_child_monotonic<F>(&mut self, key: u8, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Branch::modify_child_monotonic(&mut self.branch_nn, key, f);
    }

    /// Like [`Self::modify_child_monotonic`] but treats the supplied
    /// `inserted_hash` as a known contribution for an empty-slot insertion.
    /// This lets archive ingest maintain a clean parent around a LocalLeaf
    /// without recomputing siphash24 once per index—the caller already has it
    /// from `ArchiveEntry::hash`.
    ///
    /// The hint MUST equal the hash of whatever `f(None)` returns.
    /// An occupied equal-count edit retains the parent by the monotonicity
    /// proof; a growing replacement uses resident child hashes when available.
    pub fn modify_child_monotonic_with_inserted_hint<F>(
        &mut self,
        key: u8,
        inserted_hash: u128,
        f: F,
    ) where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Branch::modify_child_monotonic_with_inserted_hint(
            &mut self.branch_nn,
            key,
            inserted_hash,
            f,
        );
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

    /// Finish a bulk rewrite without traversing children for their hashes.
    ///
    /// Counts and the representative child pointer are structural and can be
    /// rebuilt in one table scan. The hash is different: asking every child
    /// for it would cross dirty archive-backed subtrees and defeat lazy hash
    /// maintenance. `known_hash` can install an independently derived exact
    /// aggregate, including zero; `None` marks the cache unknown.
    #[cfg(feature = "parallel")]
    pub fn finish_bulk_aggregates(&mut self, known_hash: Option<u128>) {
        unsafe {
            Branch::finish_bulk_aggregates(&mut self.branch_nn, known_hash);
        }
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
    pub end_depth: u32,
    /// Thin pointer to the key bytes of a representative descendant
    /// leaf, used for prefix-matching shortcuts. Points either into a
    /// heap [`Leaf`]'s inline `key` field (offset 0 thanks to
    /// `#[repr(C)]`) or into archive memory referenced by a
    /// `LocalLeaf`. The unified `*const [u8; KEY_LEN]` representation
    /// lets both leaf flavors serve as the childleaf.
    pub childleaf: *const [u8; KEY_LEN],
    pub leaf_count: u64,
    pub segment_count: u64,
    /// The exact subtree fingerprint, split into atomic words so an immutable
    /// reader can memoize a dirty Branch without COW. `HASH_KNOWN` in `rc`
    /// publishes both words and distinguishes exact zero from unknown.
    hash_lo: atomic::AtomicU64,
    hash_hi: atomic::AtomicU64,
    pub child_table: Table,
}

impl<
        const KEY_LEN: usize,
        O: KeySchema<KEY_LEN>,
        Table: ?Sized + core::fmt::Debug,
        V: core::fmt::Debug,
    > core::fmt::Debug for Branch<KEY_LEN, O, Table, V>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Branch")
            .field("rc", &(self.rc.load(Relaxed) & RC_MASK))
            .field("end_depth", &self.end_depth)
            .field("childleaf", &self.childleaf)
            .field("leaf_count", &self.leaf_count)
            .field("segment_count", &self.segment_count)
            .field("hash", &self.cached_hash())
            .field("child_table", &&self.child_table)
            .finish()
    }
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, Table: ?Sized, V> Branch<KEY_LEN, O, Table, V> {
    /// Return the resident exact fingerprint without traversing children.
    ///
    /// The known flag is published with `Release` after both words and loaded
    /// with `Acquire` before them. Multiple immutable readers may race to
    /// install the same semantic value; atomic word stores make that benign.
    #[inline]
    pub(crate) fn cached_hash(&self) -> Option<u128> {
        if self.rc.load(Acquire) & HASH_KNOWN == 0 {
            return None;
        }
        let lo = self.hash_lo.load(Relaxed) as u128;
        let hi = self.hash_hi.load(Relaxed) as u128;
        Some(lo | (hi << 64))
    }

    /// Publish the exact value computed by an immutable hash consumer.
    /// Concurrent consumers may race, but immutable structure guarantees that
    /// they all publish the same semantic value.
    #[inline]
    pub(crate) fn publish_cached_hash(&self, hash: u128) {
        if let Some(resident) = self.cached_hash() {
            debug_assert_eq!(resident, hash, "immutable Branch hash changed");
            return;
        }
        self.hash_lo.store(hash as u64, Relaxed);
        self.hash_hi.store((hash >> 64) as u64, Relaxed);
        self.rc.fetch_or(HASH_KNOWN, Release);
    }

    /// Replace or clear the cache while structurally mutating a uniquely
    /// borrowed Branch. Requiring `&mut self` keeps invalidation from racing
    /// safe immutable consumers.
    #[inline]
    pub(crate) fn replace_cached_hash(&mut self, hash: Option<u128>) {
        let state = self.rc.get_mut();
        debug_assert_eq!(
            *state & RC_MASK,
            1,
            "cache replacement requires a uniquely owned Branch",
        );
        *state &= RC_MASK;
        if let Some(hash) = hash {
            *self.hash_lo.get_mut() = hash as u64;
            *self.hash_hi.get_mut() = (hash >> 64) as u64;
            *state |= HASH_KNOWN;
        }
    }

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
        end_depth: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
    ) -> NonNull<Self> {
        let lchild_hash = lchild.known_hash();
        let rchild_hash = rchild.known_hash();
        Self::new_with_optional_child_hashes(end_depth, lchild, rchild, lchild_hash, rchild_hash)
    }

    /// Variant of [`Self::new`] that takes a precomputed
    /// `rchild_hash` and uses it instead of calling `rchild.hash()`.
    /// Lets archive-ingest divergence paths reuse the
    /// `ArchiveEntry::hash` they already have instead of recomputing
    /// siphash24 over the LocalLeaf bytes.
    ///
    /// `rchild_hash` MUST equal `rchild.hash()`. The left contribution is used
    /// only when it is already cached; a dirty left subtree makes the new
    /// parent dirty instead of forcing a recursive hash.
    pub(super) fn new_with_rchild_hash(
        end_depth: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
        rchild_hash: u128,
    ) -> NonNull<Self> {
        let lchild_hash = lchild.known_hash();
        Self::new_with_optional_child_hashes(
            end_depth,
            lchild,
            rchild,
            lchild_hash,
            Some(rchild_hash),
        )
    }

    /// Variant used when a known archive batch bootstraps a Branch directly
    /// from two LocalLeaves. Both hashes come from the two ArchiveEntries, so
    /// the six TribleSet indexes do not each hash the same bytes again.
    pub(super) fn new_with_child_hashes(
        end_depth: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
        lchild_hash: u128,
        rchild_hash: u128,
    ) -> NonNull<Self> {
        Self::new_with_optional_child_hashes(
            end_depth,
            lchild,
            rchild,
            Some(lchild_hash),
            Some(rchild_hash),
        )
    }

    /// Construct a branch whose aggregate is cached only when both child
    /// hashes are already known. The publication bit distinguishes exact zero
    /// from an unknown aggregate without enlarging the Branch.
    pub(super) fn new_with_optional_child_hashes(
        end_depth: usize,
        lchild: Head<KEY_LEN, O, V>,
        rchild: Head<KEY_LEN, O, V>,
        lchild_hash: Option<u128>,
        rchild_hash: Option<u128>,
    ) -> NonNull<Self> {
        unsafe {
            let size = 2;
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
            let hash = match (lchild_hash, rchild_hash) {
                (Some(left), Some(right)) => Some(left ^ right),
                _ => None,
            };
            let state = 1 | hash.is_some().then_some(HASH_KNOWN).unwrap_or(0);
            addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(state));
            addr_of_mut!((*ptr.as_ptr()).end_depth).write(end_depth as u32);
            addr_of_mut!((*ptr.as_ptr()).childleaf).write(lchild.childleaf_ptr());
            addr_of_mut!((*ptr.as_ptr()).leaf_count).write(lchild.count() + rchild.count());
            addr_of_mut!((*ptr.as_ptr()).segment_count)
                .write(lchild.count_segment(end_depth) + rchild.count_segment(end_depth));
            let hash = hash.unwrap_or(0);
            addr_of_mut!((*ptr.as_ptr()).hash_lo).write(atomic::AtomicU64::new(hash as u64));
            addr_of_mut!((*ptr.as_ptr()).hash_hi)
                .write(atomic::AtomicU64::new((hash >> 64) as u64));
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
                if current & RC_MASK == RC_MASK {
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
            if (*branch).rc.fetch_sub(1, Release) & RC_MASK != 1 {
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
            if (*branch).rc.load(Acquire) & RC_MASK == 1 {
                None
            } else {
                let size = dst_len(addr_of!((*branch).child_table));
                let cached_hash = (*branch).cached_hash();
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
                    let state = 1 | cached_hash.is_some().then_some(HASH_KNOWN).unwrap_or(0);
                    addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(state));
                    addr_of_mut!((*ptr.as_ptr()).end_depth).write((*branch).end_depth);
                    addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                    addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                    addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                    let hash = cached_hash.unwrap_or(0);
                    addr_of_mut!((*ptr.as_ptr()).hash_lo)
                        .write(atomic::AtomicU64::new(hash as u64));
                    addr_of_mut!((*ptr.as_ptr()).hash_hi)
                        .write(atomic::AtomicU64::new((hash >> 64) as u64));
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
            let cached_hash = (*branch).cached_hash();
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
                let state = 1 | cached_hash.is_some().then_some(HASH_KNOWN).unwrap_or(0);
                addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(state));
                addr_of_mut!((*ptr.as_ptr()).end_depth).write((*branch).end_depth);
                addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                let hash = cached_hash.unwrap_or(0);
                addr_of_mut!((*ptr.as_ptr()).hash_lo).write(atomic::AtomicU64::new(hash as u64));
                addr_of_mut!((*ptr.as_ptr()).hash_hi)
                    .write(atomic::AtomicU64::new((hash >> 64) as u64));
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
    pub(super) fn modify_child<F>(branch_nn: &mut NonNull<Self>, key: u8, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Self::modify_child_impl(branch_nn, key, None, ChildEditSemantics::Arbitrary, f);
    }

    pub(super) fn modify_child_monotonic<F>(branch_nn: &mut NonNull<Self>, key: u8, f: F)
    where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Self::modify_child_impl(
            branch_nn,
            key,
            None,
            ChildEditSemantics::MonotoneSetGrowth,
            f,
        );
    }

    /// Shared child mutation machinery. `empty_slot_hash_hint` is consulted
    /// only when `f(None)` inserts a child; an occupied replacement always
    /// derives both contributions from the resident heads. `Some(0)` is a
    /// legitimate known-zero contribution and must remain distinct from no
    /// hint.
    fn modify_child_impl<F>(
        branch_nn: &mut NonNull<Self>,
        key: u8,
        empty_slot_hash_hint: Option<u128>,
        edit_semantics: ChildEditSemantics,
        f: F,
    ) where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        unsafe {
            let branch = branch_nn.as_ptr();
            let end_depth = (*branch).end_depth as usize;
            // Incremental maintenance is valid only when the parent and every
            // replaced contribution are already known; otherwise dirtiness
            // propagates upward.
            let cached_parent_hash = (*branch).cached_hash();

            // If a slot exists, operate on the existing child in-place.
            if let Some(slot) = (*branch).child_table.table_get_slot(key) {
                let child = slot.take().unwrap();
                let old_child_hash = cached_parent_hash.and_then(|_| child.known_hash());
                let old_child_segment_count = child.count_segment(end_depth);
                let old_child_leaf_count = child.count();

                let replaced_childleaf = child.childleaf_ptr() == (*branch).childleaf;

                if let Some(new_child) = f(Some(child)) {
                    // Replace existing child
                    let new_child_leaf_count = new_child.count();
                    debug_assert!(
                        edit_semantics != ChildEditSemantics::MonotoneSetGrowth
                            || new_child_leaf_count >= old_child_leaf_count,
                        "a monotone child edit cannot remove keys"
                    );
                    let same_contribution = edit_semantics == ChildEditSemantics::MonotoneSetGrowth
                        && new_child_leaf_count == old_child_leaf_count;
                    let hash = if same_contribution {
                        // Monotonicity plus unchanged cardinality proves the
                        // exact same key set. This remains valid when a Branch
                        // mutated in place and when a LocalLeaf has no hash.
                        cached_parent_hash
                    } else {
                        let new_child_hash = new_child.known_hash();
                        match (cached_parent_hash, old_child_hash, new_child_hash) {
                            (Some(parent), Some(old), Some(new)) => Some(parent ^ old ^ new),
                            _ => None,
                        }
                    };
                    (*branch).segment_count = ((*branch).segment_count - old_child_segment_count)
                        + new_child.count_segment(end_depth);
                    (*branch).leaf_count =
                        ((*branch).leaf_count - old_child_leaf_count) + new_child_leaf_count;

                    if replaced_childleaf {
                        (*branch).childleaf = new_child.childleaf_ptr();
                    }

                    if slot.replace(new_child.with_key(key)).is_some() {
                        unreachable!();
                    }
                    (&mut *branch).replace_cached_hash(hash);
                } else {
                    debug_assert_eq!(edit_semantics, ChildEditSemantics::Arbitrary);
                    // Remove existing child
                    let hash = match (cached_parent_hash, old_child_hash) {
                        (Some(parent), Some(old)) => Some(parent ^ old),
                        _ => None,
                    };
                    (*branch).segment_count -= old_child_segment_count;
                    (*branch).leaf_count -= old_child_leaf_count;

                    if replaced_childleaf {
                        if let Some(other) = (*branch).child_table.iter().find_map(|s| s.as_ref()) {
                            (*branch).childleaf = other.childleaf_ptr();
                        }
                    }
                    (&mut *branch).replace_cached_hash(hash);
                }
            } else {
                // No current slot — the closure can choose to insert a child.
                if let Some(mut inserted) = f(None) {
                    // The caller is expected to pass an inserted Head that is
                    // already prepared (with_start set to the appropriate depth).
                    // Update aggregates before attempting insertion.
                    (*branch).leaf_count += inserted.count();
                    (*branch).segment_count += inserted.count_segment(end_depth);
                    let inserted_hash = empty_slot_hash_hint.or_else(|| inserted.known_hash());
                    let hash = match (cached_parent_hash, inserted_hash) {
                        (Some(parent), Some(inserted)) => Some(parent ^ inserted),
                        _ => None,
                    };
                    // Cuckoo insert loop, growing the table when necessary.
                    let mut branch_ptr = branch_nn.as_ptr();
                    while let Some(new_displaced) = (*branch_ptr).child_table.table_insert(inserted)
                    {
                        inserted = new_displaced;
                        Self::grow(branch_nn);
                        // Refresh local pointer after potential reallocation.
                        branch_ptr = branch_nn.as_ptr();
                    }
                    (&mut *branch_ptr).replace_cached_hash(hash);
                }
            }
            // Debug invariant check (no-op in release builds).
            #[cfg(debug_assertions)]
            branch_nn.as_ref().debug_check_invariants();
        }
    }

    /// Variant of [`Self::modify_child_monotonic`] that takes a precomputed
    /// `inserted_hash` as a known empty-slot contribution. The hint MUST equal
    /// the hash of whatever `f(None)` returns.
    pub(super) fn modify_child_monotonic_with_inserted_hint<F>(
        branch_nn: &mut NonNull<Self>,
        key: u8,
        inserted_hash: u128,
        f: F,
    ) where
        F: FnOnce(Option<Head<KEY_LEN, O, V>>) -> Option<Head<KEY_LEN, O, V>>,
    {
        Self::modify_child_impl(
            branch_nn,
            key,
            Some(inserted_hash),
            ChildEditSemantics::MonotoneSetGrowth,
            f,
        );
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

    /// Rebuild aggregate fields (`hash`, `leaf_count`, `segment_count`,
    /// `childleaf`) from the current child table in one linear pass.
    /// Cheaper than paying `modify_child`'s per-call accounting when
    /// many children are being installed in bulk.
    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    pub(crate) unsafe fn recompute_aggregates(branch_nn: &mut NonNull<Self>) {
        let branch = branch_nn.as_ptr();
        let end_depth = (*branch).end_depth as usize;
        let mut agg_leaf_count: u64 = 0;
        let mut agg_segment_count: u64 = 0;
        let mut agg_hash: u128 = 0;
        let mut first_childleaf: *const [u8; KEY_LEN] = std::ptr::null();

        for child in (*branch).child_table.iter().flatten() {
            agg_leaf_count += child.count();
            agg_segment_count += child.count_segment(end_depth);
            agg_hash ^= child.hash();
            if first_childleaf.is_null() {
                first_childleaf = child.childleaf_ptr();
            }
        }

        (*branch).leaf_count = agg_leaf_count;
        (*branch).segment_count = agg_segment_count;
        (&mut *branch).replace_cached_hash(Some(agg_hash));
        if !first_childleaf.is_null() {
            (*branch).childleaf = first_childleaf;
        }

        #[cfg(debug_assertions)]
        branch_nn.as_ref().debug_check_invariants();
    }

    /// Rebuild non-hash aggregates after a bulk rewrite and optionally install
    /// an independently-derived hash without traversing a child for it.
    ///
    /// `known_hash` is exact when present, including exact zero.
    #[cfg(feature = "parallel")]
    pub(crate) unsafe fn finish_bulk_aggregates(
        branch_nn: &mut NonNull<Self>,
        known_hash: Option<u128>,
    ) {
        let branch = branch_nn.as_ptr();
        let end_depth = (*branch).end_depth as usize;
        let mut agg_leaf_count: u64 = 0;
        let mut agg_segment_count: u64 = 0;
        let mut first_childleaf: *const [u8; KEY_LEN] = std::ptr::null();

        for child in (*branch).child_table.iter().flatten() {
            agg_leaf_count += child.count();
            agg_segment_count += child.count_segment(end_depth);
            if first_childleaf.is_null() {
                first_childleaf = child.childleaf_ptr();
            }
        }

        (*branch).leaf_count = agg_leaf_count;
        (*branch).segment_count = agg_segment_count;
        (&mut *branch).replace_cached_hash(known_hash);
        if !first_childleaf.is_null() {
            (*branch).childleaf = first_childleaf;
        }

        #[cfg(debug_assertions)]
        branch_nn.as_ref().debug_check_invariants();
    }

    pub fn count_segment(&self, at_depth: usize) -> u64 {
        let node_end = self.end_depth as usize;
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
        let end_depth: usize = self.end_depth as usize;
        let mut agg_leaf_count: u64 = 0;
        let mut agg_segment_count: u64 = 0;
        let mut agg_hash: u128 = 0;
        let mut match_found = false;
        let cached_hash = self.cached_hash();
        let has_cached_hash = cached_hash.is_some();
        let mut all_child_hashes_known = true;

        for child in self.child_table.iter().flatten() {
            agg_leaf_count = agg_leaf_count.saturating_add(child.count());
            agg_segment_count = agg_segment_count.saturating_add(child.count_segment(end_depth));
            if has_cached_hash {
                if let Some(hash) = child.known_hash() {
                    agg_hash ^= hash;
                } else {
                    all_child_hashes_known = false;
                }
            }
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
        // A parent may legally carry an exact cache over dirty descendants
        // after algebraic repair; keep this local audit resident-only and
        // leave that case to the explicit deep boundary oracle.
        if let (Some(cached_hash), true) = (cached_hash, all_child_hashes_known) {
            debug_assert_eq!(agg_hash, cached_hash, "branch.hash mismatch");
        }

        // If there are any leaves aggregated in this branch then the
        // `childleaf` pointer must match one of the children. When the
        // aggregate count is zero the equality check above already guarantees
        // `self.leaf_count == 0`, so the explicit empty-branch assertion is
        // redundant and can be omitted.
        if agg_leaf_count > 0 {
            debug_assert!(match_found, "branch.childleaf pointer mismatch");
        }
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
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
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

        // The infix ends within the current node.
        if PREFIX_LEN + INFIX_LEN <= node_end_depth {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            f(&infix);
            return;
        }
        // The prefix ends in a child of this node.
        if PREFIX_LEN > node_end_depth {
            if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
                child.infixes(prefix, node_end_depth, f);
            }
            return;
        }

        // The prefix ends in this node, but the infix ends in a child.
        for entry in self.child_table.iter().flatten() {
            entry.infixes(prefix, node_end_depth, f);
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
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return;
        }

        // Case 1: infix ends within this node — extract and range-check.
        if PREFIX_LEN + INFIX_LEN <= node_end_depth {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            if &infix >= min_infix && &infix <= max_infix {
                f(&infix);
            }
            return;
        }

        // Case 2: prefix extends into a specific child.
        if PREFIX_LEN > node_end_depth {
            if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
                child.infixes_range(prefix, node_end_depth, min_infix, max_infix, f);
            }
            return;
        }

        // Case 3: prefix ends here, infix spans children.
        // First check the compressed path (bytes PREFIX_LEN..node_end_depth)
        // against the range. All children share these bytes (path compression).
        let infix_byte_idx = node_end_depth - PREFIX_LEN;
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
            let child_byte = entry.key();
            if min_tight && infix_byte_idx < INFIX_LEN && child_byte < min_infix[infix_byte_idx] {
                continue;
            }
            if max_tight && infix_byte_idx < INFIX_LEN && child_byte > max_infix[infix_byte_idx] {
                continue;
            }
            entry.infixes_range(prefix, node_end_depth, min_infix, max_infix, f);
        }
    }

    /// Return the lexicographically first infix in the inclusive range.
    ///
    /// Child slots are cuckoo-ordered rather than lexicographically ordered,
    /// so lower-bound descent probes child byte values in order and follows
    /// only the first subtree that can contain a match. This keeps the cursor
    /// independent of the branch's physical table layout.
    pub fn first_infix_range<const PREFIX_LEN: usize, const INFIX_LEN: usize>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return None;
        }

        // The complete infix lies on this compressed path, so every
        // descendant represents the same infix value.
        if PREFIX_LEN + INFIX_LEN <= node_end_depth {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            return (&infix >= min_infix && &infix <= max_infix).then_some(infix);
        }

        // The prefix fixes the one child that can contain a result.
        if PREFIX_LEN > node_end_depth {
            return self
                .child_table
                .table_get(prefix[node_end_depth])
                .and_then(|child| {
                    child.first_infix_range(prefix, node_end_depth, min_infix, max_infix)
                });
        }

        // The fixed part of this compressed path must be compatible with the
        // range. Track whether the next branching byte is still constrained
        // by either boundary.
        let infix_byte_idx = node_end_depth - PREFIX_LEN;
        let mut min_tight = true;
        let mut max_tight = true;
        for i in 0..infix_byte_idx {
            let path_byte = self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]];
            if min_tight {
                if path_byte < min_infix[i] {
                    return None;
                }
                if path_byte > min_infix[i] {
                    min_tight = false;
                }
            }
            if max_tight {
                if path_byte > max_infix[i] {
                    return None;
                }
                if path_byte < max_infix[i] {
                    max_tight = false;
                }
            }
        }

        let lower = if min_tight {
            min_infix[infix_byte_idx]
        } else {
            u8::MIN
        };
        let upper = if max_tight {
            max_infix[infix_byte_idx]
        } else {
            u8::MAX
        };

        for child_byte in lower..=upper {
            let Some(child) = self.child_table.table_get(child_byte) else {
                continue;
            };
            if let Some(infix) =
                child.first_infix_range(prefix, node_end_depth, min_infix, max_infix)
            {
                return Some(infix);
            }
        }
        None
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
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
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
        if PREFIX_LEN + INFIX_LEN <= node_end_depth {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf_key()[O::TREE_TO_KEY[PREFIX_LEN + i]]);
            return if &infix >= min_infix && &infix <= max_infix {
                1
            } else {
                0
            };
        }

        // Case 2: prefix extends into a specific child.
        if PREFIX_LEN > node_end_depth {
            if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
                return child.count_range(prefix, node_end_depth, min_infix, max_infix);
            }
            return 0;
        }

        // Case 3: prefix ends here, infix spans children.
        // Check compressed path against range (same logic as infixes_range).
        let infix_byte_idx = node_end_depth - PREFIX_LEN;
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
            let child_byte = entry.key();
            let below_min = min_tight && child_byte < min_infix[infix_byte_idx];
            let above_max = max_tight && child_byte > max_infix[infix_byte_idx];
            if below_min || above_max {
                continue;
            }
            let on_min = min_tight && child_byte == min_infix[infix_byte_idx];
            let on_max = max_tight && child_byte == max_infix[infix_byte_idx];
            if on_min || on_max {
                total += entry.count_range(prefix, node_end_depth, min_infix, max_infix);
            } else {
                total += entry.count_segment(node_end_depth);
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
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return false;
        }

        if PREFIX_LEN <= node_end_depth {
            return true;
        }

        if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
            return child.has_prefix::<PREFIX_LEN>(node_end_depth, prefix);
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
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return 1;
        }

        if PREFIX_LEN <= node_end_depth {
            return 1;
        }

        if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
            return 1 + child.traversal_depth::<PREFIX_LEN>(node_end_depth, prefix);
        }

        1
    }

    pub fn get<'a>(&'a self, at_depth: usize, key: &[u8; KEY_LEN]) -> Option<&'a V>
    where
        O: 'a,
    {
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(KEY_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &key[..limit],
        ) {
            return None;
        }
        if node_end_depth >= KEY_LEN {
            // Childleaf prefix matched and end_depth == KEY_LEN means the
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

        if let Some(child) = self.child_table.table_get(key[node_end_depth]) {
            return child.get(node_end_depth, key);
        }
        None
    }

    pub fn segmented_len<const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(PREFIX_LEN, node_end_depth);
        if !super::leaf::key_ops::has_prefix::<KEY_LEN, O>(
            self.childleaf_key(),
            at_depth,
            &prefix[..limit],
        ) {
            return 0;
        }
        if PREFIX_LEN <= node_end_depth {
            if !O::same_segment_tree(PREFIX_LEN, node_end_depth) {
                return 1;
            } else {
                return self.segment_count;
            }
        }
        if let Some(child) = self.child_table.table_get(prefix[node_end_depth]) {
            child.segmented_len::<PREFIX_LEN>(node_end_depth, prefix)
        } else {
            0
        }
    }

    // Instance methods implemented directly on &Branch — these contain any
    // required unsafe access (childleaf deref) locally and avoid forwarding
    // through more wrappers. This keeps the call graph minimal and makes the
    // logic easier to maintain.
}
