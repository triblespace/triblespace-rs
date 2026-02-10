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
            BodyMut::Leaf(_) => panic!("BranchMut requires a Branch body"),
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

#[derive(Debug)]
#[repr(C, align(16))]
pub(crate) struct Branch<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, Table: ?Sized, V> {
    key_ordering: PhantomData<O>,
    key_segments: PhantomData<O::Segmentation>,

    rc: atomic::AtomicU32,
    pub end_depth: u32,
    pub childleaf: *const Leaf<KEY_LEN, V>,
    pub leaf_count: u64,
    pub segment_count: u64,
    pub hash: u128,
    pub child_table: Table,
}

impl<const KEY_LEN: usize, O: KeySchema<KEY_LEN>, Table: ?Sized, V> Branch<KEY_LEN, O, Table, V> {
    /// Returns a shared reference to the child leaf referenced by this
    /// branch's `childleaf` pointer. This centralizes the unsafe pointer
    /// dereference in one place so callers can use a safe reference.
    pub fn childleaf(&self) -> &Leaf<KEY_LEN, V> {
        unsafe { &*self.childleaf }
    }

    /// Returns the raw pointer to the child leaf.
    pub fn childleaf_ptr(&self) -> *const Leaf<KEY_LEN, V> {
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
            addr_of_mut!((*ptr.as_ptr()).rc).write(atomic::AtomicU32::new(1));
            addr_of_mut!((*ptr.as_ptr()).end_depth).write(end_depth as u32);
            addr_of_mut!((*ptr.as_ptr()).childleaf).write(lchild.childleaf_ptr());
            addr_of_mut!((*ptr.as_ptr()).leaf_count).write(lchild.count() + rchild.count());
            addr_of_mut!((*ptr.as_ptr()).segment_count)
                .write(lchild.count_segment(end_depth) + rchild.count_segment(end_depth));
            addr_of_mut!((*ptr.as_ptr()).hash).write(lchild.hash() ^ rchild.hash());
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
                    addr_of_mut!((*ptr.as_ptr()).end_depth).write((*branch).end_depth);
                    addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                    addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                    addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                    addr_of_mut!((*ptr.as_ptr()).hash).write((*branch).hash);
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
                addr_of_mut!((*ptr.as_ptr()).end_depth).write((*branch).end_depth);
                addr_of_mut!((*ptr.as_ptr()).leaf_count).write((*branch).leaf_count);
                addr_of_mut!((*ptr.as_ptr()).segment_count).write((*branch).segment_count);
                addr_of_mut!((*ptr.as_ptr()).childleaf).write((*branch).childleaf);
                addr_of_mut!((*ptr.as_ptr()).hash).write((*branch).hash);
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
        unsafe {
            let branch = branch_nn.as_ptr();
            let end_depth = (*branch).end_depth as usize;

            // If a slot exists, operate on the existing child in-place.
            if let Some(slot) = (*branch).child_table.table_get_slot(key) {
                let child = slot.take().unwrap();
                let old_child_hash = child.hash();
                let old_child_segment_count = child.count_segment(end_depth);
                let old_child_leaf_count = child.count();

                let replaced_childleaf = child.childleaf_ptr() == (*branch).childleaf;

                if let Some(new_child) = f(Some(child)) {
                    // Replace existing child
                    (*branch).hash = ((*branch).hash ^ old_child_hash) ^ new_child.hash();
                    (*branch).segment_count = ((*branch).segment_count - old_child_segment_count)
                        + new_child.count_segment(end_depth);
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
                    (*branch).segment_count += inserted.count_segment(end_depth);
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

    // Note: upsert_child removed in favor of explicit insert_child / update_child

    // The old in-place `update_child` helper has been superseded by
    // `modify_child` which accepts an Option<Head> and handles insert/update/remove
    // uniformly. The thin adapter was removed to centralize behavior; callers
    // should use `modify_child` or BranchMut::modify_child.

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

        for child in self.child_table.iter().flatten() {
            agg_leaf_count = agg_leaf_count.saturating_add(child.count());
            agg_segment_count = agg_segment_count.saturating_add(child.count_segment(end_depth));
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
        if !self.childleaf().has_prefix::<O>(at_depth, &prefix[..limit]) {
            return;
        }

        // The infix ends within the current node.
        if PREFIX_LEN + INFIX_LEN <= node_end_depth {
            let infix: [u8; INFIX_LEN] =
                core::array::from_fn(|i| self.childleaf().key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
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
        if !self.childleaf().has_prefix::<O>(at_depth, &prefix[..limit]) {
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

    pub fn get<'a>(&'a self, at_depth: usize, key: &[u8; KEY_LEN]) -> Option<&'a V>
    where
        O: 'a,
    {
        let node_end_depth = self.end_depth as usize;
        let limit = std::cmp::min(KEY_LEN, node_end_depth);
        if !self.childleaf().has_prefix::<O>(at_depth, &key[..limit]) {
            return None;
        }
        if node_end_depth >= KEY_LEN {
            return Some(&self.childleaf().value);
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
        if !self.childleaf().has_prefix::<O>(at_depth, &prefix[..limit]) {
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
