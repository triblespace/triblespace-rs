use super::*;
use std::sync::Arc;

/// Reference-counted handle to a heap-allocated leaf node in a PATCH trie.
///
/// `Entry` is the unit of insertion for the memory-only path: it owns a
/// shared `Leaf<KEY_LEN, V>` and can be inserted into multiple PATCH
/// instances (each PATCH gets its own Head pointing at the shared
/// refcounted Leaf). The archive-backed counterpart is [`ArchiveEntry`],
/// which only exists for `V = ()` since archive bytes carry no value
/// field.
#[derive(Debug)]
#[repr(C)]
pub struct Entry<const KEY_LEN: usize, V = ()> {
    ptr: NonNull<Leaf<KEY_LEN, V>>,
}

impl<const KEY_LEN: usize> Entry<KEY_LEN> {
    /// Creates a new entry with the given key and a unit value.
    pub fn new(key: &[u8; KEY_LEN]) -> Self {
        unsafe {
            let ptr = Leaf::<KEY_LEN, ()>::new(key, ());
            Self { ptr }
        }
    }
}

impl<const KEY_LEN: usize, V> Entry<KEY_LEN, V> {
    /// Creates a new entry with the given key and associated value.
    pub fn with_value(key: &[u8; KEY_LEN], value: V) -> Self {
        unsafe {
            let ptr = Leaf::<KEY_LEN, V>::new(key, value);
            Self { ptr }
        }
    }

    /// Returns a reference to the value stored in this entry.
    pub fn value(&self) -> &V {
        unsafe { &self.ptr.as_ref().value }
    }

    pub(super) fn leaf<O: KeySchema<KEY_LEN>>(&self) -> Head<KEY_LEN, O, V> {
        unsafe { Head::new(0, Leaf::rc_inc(self.ptr)) }
    }
}

impl<const KEY_LEN: usize, V> Clone for Entry<KEY_LEN, V> {
    fn clone(&self) -> Self {
        unsafe {
            Self {
                ptr: Leaf::rc_inc(self.ptr),
            }
        }
    }
}

impl<const KEY_LEN: usize, V> Drop for Entry<KEY_LEN, V> {
    fn drop(&mut self) {
        unsafe {
            Leaf::rc_dec(self.ptr);
        }
    }
}

/// Insertion entry for archive-backed PATCHes (`V = ()` only).
///
/// Holds a thin pointer into an archive's bytes plus an
/// `Arc<dyn ArchiveOwner>` that keeps those bytes alive. When inserted
/// via [`PATCH::insert_archive`], the entry's key becomes a
/// [`Head::new_local_leaf`] under a Branch whose `owner` matches; on
/// owner mismatch the leaf is automatically reified into a heap-
/// allocated `Leaf<KEY_LEN, ()>` so the result is owner-consistent.
///
/// Only valid for `V = ()` because archive bytes don't carry a value
/// field — the constructor's type parameter enforces this.
pub struct ArchiveEntry<const KEY_LEN: usize> {
    pub(super) ptr: NonNull<[u8; KEY_LEN]>,
    pub(super) owner: Arc<dyn ArchiveOwner>,
}

impl<const KEY_LEN: usize> ArchiveEntry<KEY_LEN> {
    /// Creates an `ArchiveEntry` referencing a `[u8; KEY_LEN]` trible
    /// inside an archive's bytes.
    ///
    /// # Safety
    /// - `ptr` must remain valid for as long as `owner` is held.
    /// - `ptr` must be 16-byte aligned (so [`Head::new_local_leaf`]'s
    ///   tagged-pointer encoding has room for the `LocalLeaf` tag in
    ///   the low 4 bits). Any `[u8; 64]` at an offset that's a
    ///   multiple of 16 from a 16-byte aligned base satisfies this.
    pub unsafe fn new(ptr: NonNull<[u8; KEY_LEN]>, owner: Arc<dyn ArchiveOwner>) -> Self {
        debug_assert_eq!(
            ptr.as_ptr() as usize & 0x0f,
            0,
            "ArchiveEntry pointer must be 16-byte aligned"
        );
        Self { ptr, owner }
    }

    /// Returns a `LocalLeaf` head for this entry, plus a clone of the
    /// owner Arc so it can be threaded into the receiving Branch.
    pub(super) fn leaf<O: KeySchema<KEY_LEN>>(
        &self,
    ) -> (Head<KEY_LEN, O, ()>, Arc<dyn ArchiveOwner>) {
        unsafe { (Head::new_local_leaf(0, self.ptr), self.owner.clone()) }
    }

    /// Borrows the owner Arc without cloning.
    pub fn owner(&self) -> &Arc<dyn ArchiveOwner> {
        &self.owner
    }
}

impl<const KEY_LEN: usize> Clone for ArchiveEntry<KEY_LEN> {
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            owner: self.owner.clone(),
        }
    }
}

impl<const KEY_LEN: usize> core::fmt::Debug for ArchiveEntry<KEY_LEN> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ArchiveEntry")
            .field("ptr", &self.ptr)
            .field("owner", &"<archive owner>")
            .finish()
    }
}
