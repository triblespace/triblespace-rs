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
/// Holds a thin pointer into an archive's bytes plus a *borrow* of
/// the `Arc<dyn ArchiveOwner>` that keeps those bytes alive. When
/// inserted via [`PATCH::insert_archive`], the entry's key becomes a
/// `Head::new_local_leaf` and the owner joins the PATCH's persistent root
/// owner set. Trie shape and owner identity are independent.
///
/// The owner is borrowed (not owned) so the ingest hot loop pays
/// **zero** atomic ref-count traffic per trible — the receiving PATCH only
/// clones an owner the first time that allocation joins its owner set. The
/// caller (typically a chunked-archive
/// decoder) keeps one `Arc` alive on the stack for the whole batch.
///
/// Only valid for `V = ()` because archive bytes don't carry a value
/// field — the constructor's type parameter enforces this.
pub struct ArchiveEntry<'a, const KEY_LEN: usize> {
    pub(super) ptr: NonNull<[u8; KEY_LEN]>,
    pub(super) owner: &'a Arc<dyn ArchiveOwner>,
    /// Pre-computed siphash24 of the trible bytes (matches what
    /// `Head::hash()` would compute on the resulting `LocalLeaf`).
    /// Cached once at `ArchiveEntry::new` so the 6-way fan-out across
    /// covering indexes runs one hash instead of six.
    pub(super) hash: u128,
}

impl<'a, const KEY_LEN: usize> ArchiveEntry<'a, KEY_LEN> {
    /// Creates an `ArchiveEntry` referencing a `[u8; KEY_LEN]` trible
    /// inside an archive's bytes. Computes the siphash24 of the
    /// trible's bytes eagerly so the 6 covering indexes can share it.
    ///
    /// # Safety
    /// - `ptr` must designate a fully initialized `[u8; KEY_LEN]` and remain
    ///   valid for as long as any clone of `owner` retained by a PATCH exists.
    /// - The pointed-to bytes must not be mutated during that retained-owner
    ///   lifetime, including through concurrent aliases or interior
    ///   mutability. PATCH caches their hash and uses them as immutable trie
    ///   routing keys; changing them would invalidate both properties.
    /// - `ptr` must be 16-byte aligned (so `Head::new_local_leaf`'s
    ///   tagged-pointer encoding has room for the `LocalLeaf` tag in
    ///   the low 4 bits). Any `[u8; 64]` at an offset that's a
    ///   multiple of 16 from a 16-byte aligned base satisfies this.
    pub unsafe fn new(ptr: NonNull<[u8; KEY_LEN]>, owner: &'a Arc<dyn ArchiveOwner>) -> Self {
        // The cached hash must use the same process-local key as PATCH Leaves
        // even when callers construct ArchiveEntries before their first
        // PATCH. A completed Once makes subsequent calls a single fast check.
        crate::patch::init_sip_key();
        debug_assert_eq!(
            ptr.as_ptr() as usize & 0x0f,
            0,
            "ArchiveEntry pointer must be 16-byte aligned"
        );
        let hash = unsafe {
            use siphasher::sip128::SipHasher24;
            use std::ptr::addr_of;
            let key = *addr_of!(crate::patch::SIP_KEY);
            SipHasher24::new_with_key(&key)
                .hash(&ptr.as_ref()[..])
                .into()
        };
        Self { ptr, owner, hash }
    }

    /// Returns the archive-resident key bytes borrowed for this entry's
    /// lifetime. Used to reject duplicate bootstrap pairs before constructing
    /// the two-child root Branch.
    pub(crate) fn key(&self) -> &[u8; KEY_LEN] {
        unsafe { self.ptr.as_ref() }
    }

    /// Returns a `LocalLeaf` head for this entry, the borrowed owner
    /// Arc, and the pre-computed leaf hash.
    pub(super) fn leaf<O: KeySchema<KEY_LEN>>(
        &self,
    ) -> (Head<KEY_LEN, O, ()>, &'a Arc<dyn ArchiveOwner>, u128) {
        unsafe { (Head::new_local_leaf(0, self.ptr), self.owner, self.hash) }
    }

    /// Borrows the owner Arc without cloning.
    pub fn owner(&self) -> &'a Arc<dyn ArchiveOwner> {
        self.owner
    }
}

impl<'a, const KEY_LEN: usize> Copy for ArchiveEntry<'a, KEY_LEN> {}

impl<'a, const KEY_LEN: usize> Clone for ArchiveEntry<'a, KEY_LEN> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, const KEY_LEN: usize> core::fmt::Debug for ArchiveEntry<'a, KEY_LEN> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ArchiveEntry")
            .field("ptr", &self.ptr)
            .field("owner", &"<archive owner>")
            .finish()
    }
}
