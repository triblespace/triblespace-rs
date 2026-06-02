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
        let infix: [u8; INFIX_LEN] =
            core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
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
        let infix: [u8; INFIX_LEN] =
            core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        if &infix >= min_infix && &infix <= max_infix {
            f(&infix);
        }
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
        let infix: [u8; INFIX_LEN] =
            core::array::from_fn(|i| key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        if &infix >= min_infix && &infix <= max_infix {
            1
        } else {
            0
        }
    }

    #[inline]
    pub fn segmented_len<
        const KEY_LEN: usize,
        const PREFIX_LEN: usize,
        O: KeySchema<KEY_LEN>,
    >(
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
