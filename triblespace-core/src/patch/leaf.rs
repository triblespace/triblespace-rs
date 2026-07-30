use core::sync::atomic;
use core::sync::atomic::Ordering::Acquire;
use core::sync::atomic::Ordering::Relaxed;
use core::sync::atomic::Ordering::Release;
use siphasher::sip128::SipHasher24;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::alloc::Layout;
use std::ptr::addr_of;

use super::*;

/// Process-local metadata shared by every PATCH index that references one
/// archive-resident key.
///
/// A [`Head`] carrying the `HashedLocalLeaf` tag points at this descriptor,
/// rather than at the key bytes themselves. The descriptor is deliberately
/// not serialized: its hash uses PATCH's process-local [`SIP_KEY`], and its
/// key pointer is meaningful only while the owning process keeps the archive
/// allocation alive. [`Branch::childleaf`] remains a raw key pointer, so the
/// descriptor adds no indirection to branch routing after construction.
///
/// `align(16)` reserves the low four pointer bits for [`HeadTag`]. Its size is
/// consequently a multiple of 16, so every element of a descriptor slab is a
/// valid tagged-pointer body.
#[derive(Debug)]
#[repr(C, align(16))]
pub(crate) struct ArchiveLeafDescriptor<const KEY_LEN: usize> {
    key: NonNull<[u8; KEY_LEN]>,
    hash: u128,
}

// SAFETY: construction requires the pointed-to key to remain initialized and
// immutable for the descriptor's complete lifetime. The descriptor itself is
// immutable after publication. SimpleArchive enforces the lifetime half by
// retaining both the key bytes and descriptor slab in one composite owner.
unsafe impl<const KEY_LEN: usize> Send for ArchiveLeafDescriptor<KEY_LEN> {}
unsafe impl<const KEY_LEN: usize> Sync for ArchiveLeafDescriptor<KEY_LEN> {}

impl<const KEY_LEN: usize> ArchiveLeafDescriptor<KEY_LEN> {
    /// Construct process-local metadata for one archive-resident key.
    ///
    /// # Safety
    ///
    /// - `key` must remain valid, fully initialized, and immutable for the
    ///   complete lifetime of this descriptor, including concurrent access.
    /// - The allocation that eventually owns this descriptor must remain live
    ///   for every `Head::HashedLocalLeaf` that points at it.
    /// - The descriptor must be placed at a 16-byte-aligned stable address
    ///   before a Head is constructed from it. A boxed slice of this type
    ///   satisfies that requirement.
    pub(crate) unsafe fn new(key: NonNull<[u8; KEY_LEN]>) -> Self {
        init_sip_key();
        let hash = unsafe {
            SipHasher24::new_with_key(&*addr_of!(SIP_KEY))
                .hash(&key.as_ref()[..])
                .into()
        };
        Self { key, hash }
    }

    #[inline]
    pub(crate) fn key_ptr(&self) -> NonNull<[u8; KEY_LEN]> {
        self.key
    }

    #[inline]
    pub(crate) fn key(&self) -> &[u8; KEY_LEN] {
        // SAFETY: this is precisely the invariant required by `new`; all safe
        // access remains bounded by a shared descriptor borrow.
        unsafe { self.key.as_ref() }
    }

    #[inline]
    pub(crate) fn hash(&self) -> u128 {
        self.hash
    }
}

#[derive(Debug)]
#[repr(C, align(16))]
pub(crate) struct Leaf<const KEY_LEN: usize, V> {
    pub key: [u8; KEY_LEN],
    pub hash: u128,
    rc: atomic::AtomicU32,
    pub value: V,
}

impl<const KEY_LEN: usize, V> Body for Leaf<KEY_LEN, V> {
    fn tag(_body: NonNull<Self>) -> HeadTag {
        HeadTag::Leaf
    }
}

impl<const KEY_LEN: usize, V> Leaf<KEY_LEN, V> {
    pub(super) unsafe fn new(key: &[u8; KEY_LEN], value: V) -> NonNull<Self> {
        // Entry values may be constructed before the first PATCH. Initialize
        // the shared process-local key at the hash-construction boundary so a
        // later PATCH never observes a stale zero-key leaf hash.
        init_sip_key();
        unsafe {
            let layout = Layout::new::<Self>();
            let Some(ptr) = NonNull::new(alloc(layout) as *mut Self) else {
                handle_alloc_error(layout);
            };
            let hash = SipHasher24::new_with_key(&*addr_of!(SIP_KEY))
                .hash(&key[..])
                .into();

            ptr.write(Self {
                key: *key,
                hash,
                rc: atomic::AtomicU32::new(1),
                value,
            });

            ptr
        }
    }

    pub(crate) unsafe fn rc_inc(leaf: NonNull<Self>) -> NonNull<Self> {
        unsafe {
            let leaf = leaf.as_ptr();
            let mut current = (*leaf).rc.load(Relaxed);
            loop {
                if current == u32::MAX {
                    panic!("max refcount exceeded");
                }
                match (*leaf)
                    .rc
                    .compare_exchange(current, current + 1, Relaxed, Relaxed)
                {
                    Ok(_) => return NonNull::new_unchecked(leaf),
                    Err(v) => current = v,
                }
            }
        }
    }

    pub(crate) unsafe fn rc_dec(leaf: NonNull<Self>) {
        unsafe {
            let ptr = leaf.as_ptr();
            let rc = (*ptr).rc.fetch_sub(1, Release);
            if rc != 1 {
                return;
            }
            (*ptr).rc.load(Acquire);

            std::ptr::drop_in_place(ptr);

            let layout = Layout::new::<Self>();
            let ptr = ptr as *mut u8;
            dealloc(ptr, layout);
        }
    }

    // Instance-safe wrappers that operate on &Leaf references. All read-only
    // key-bytes logic now lives in the `key_ops` free functions below so that
    // `LocalLeaf` — which has no `Leaf` struct, just a thin pointer to the
    // archive bytes — can share the same code paths without duplication.

    pub fn infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize, O: KeySchema<KEY_LEN>, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        key_ops::infixes::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O, F>(&self.key, prefix, at_depth, f)
    }

    pub fn infixes_range<
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
        F,
    >(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        key_ops::infixes_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O, F>(
            &self.key, prefix, at_depth, min_infix, max_infix, f,
        )
    }

    pub fn first_infix_range<
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
    >(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        key_ops::first_infix_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O>(
            &self.key, prefix, at_depth, min_infix, max_infix,
        )
    }

    pub fn count_range<const PREFIX_LEN: usize, const INFIX_LEN: usize, O: KeySchema<KEY_LEN>>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> u64 {
        key_ops::count_range::<KEY_LEN, PREFIX_LEN, INFIX_LEN, O>(
            &self.key, prefix, at_depth, min_infix, max_infix,
        )
    }

    pub fn has_prefix<O: KeySchema<KEY_LEN>>(&self, at_depth: usize, prefix: &[u8]) -> bool {
        key_ops::has_prefix::<KEY_LEN, O>(&self.key, at_depth, prefix)
    }

    pub fn get<'a, O: KeySchema<KEY_LEN> + 'a>(
        &'a self,
        at_depth: usize,
        key: &[u8; KEY_LEN],
    ) -> Option<&'a V> {
        if key_ops::matches::<KEY_LEN, O>(&self.key, at_depth, key) {
            Some(&self.value)
        } else {
            None
        }
    }

    pub fn segmented_len<O: KeySchema<KEY_LEN>, const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        key_ops::segmented_len::<KEY_LEN, PREFIX_LEN, O>(&self.key, at_depth, prefix)
    }
}

/// Free functions implementing the read-only key-bytes logic shared by
/// `Leaf` (which carries the key inline) and `LocalLeaf` (a thin pointer
/// to a key in archive memory). The dispatching code in `patch.rs`'s
/// `Head` methods calls into these for both leaf flavors with the
/// appropriate key reference.
pub(crate) mod key_ops {
    use super::KeySchema;

    #[inline]
    pub fn has_prefix<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        key: &[u8; KEY_LEN],
        at_depth: usize,
        prefix: &[u8],
    ) -> bool {
        let limit = std::cmp::min(prefix.len(), KEY_LEN);
        for (depth, &p) in prefix.iter().enumerate().take(limit).skip(at_depth) {
            if key[O::TREE_TO_KEY[depth]] != p {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn matches<const KEY_LEN: usize, O: KeySchema<KEY_LEN>>(
        key: &[u8; KEY_LEN],
        at_depth: usize,
        query: &[u8; KEY_LEN],
    ) -> bool {
        for (depth, &qbyte) in query.iter().enumerate().take(KEY_LEN).skip(at_depth) {
            if key[O::TREE_TO_KEY[depth]] != qbyte {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn infixes<
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
        F,
    >(
        key: &[u8; KEY_LEN],
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        if !has_prefix::<KEY_LEN, O>(key, at_depth, prefix) {
            return;
        }
        let infix: [u8; INFIX_LEN] = core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        f(&infix);
    }

    #[inline]
    pub fn infixes_range<
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
        F,
    >(
        key: &[u8; KEY_LEN],
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        if !has_prefix::<KEY_LEN, O>(key, at_depth, prefix) {
            return;
        }
        let infix: [u8; INFIX_LEN] = core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        if &infix >= min_infix && &infix <= max_infix {
            f(&infix);
        }
    }

    #[inline]
    pub fn first_infix_range<
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
    >(
        key: &[u8; KEY_LEN],
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> Option<[u8; INFIX_LEN]> {
        if !has_prefix::<KEY_LEN, O>(key, at_depth, prefix) {
            return None;
        }
        let infix: [u8; INFIX_LEN] = core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        (&infix >= min_infix && &infix <= max_infix).then_some(infix)
    }

    #[inline]
    pub fn count_range<
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        const INFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
    >(
        key: &[u8; KEY_LEN],
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        min_infix: &[u8; INFIX_LEN],
        max_infix: &[u8; INFIX_LEN],
    ) -> u64 {
        if !has_prefix::<KEY_LEN, O>(key, at_depth, prefix) {
            return 0;
        }
        let infix: [u8; INFIX_LEN] = core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        if &infix >= min_infix && &infix <= max_infix {
            1
        } else {
            0
        }
    }

    #[inline]
    pub fn segmented_len<const KEY_LEN: usize, const PREFIX_LEN: usize, O: KeySchema<KEY_LEN>>(
        key: &[u8; KEY_LEN],
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        let limit = PREFIX_LEN;
        for (depth, &p) in prefix.iter().enumerate().take(limit).skip(at_depth) {
            if key[O::TREE_TO_KEY[depth]] != p {
                return 0;
            }
        }
        1
    }
}
