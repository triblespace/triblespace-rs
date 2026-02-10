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

    // Instance-safe wrappers that operate on &Leaf references.
    pub fn infixes<const PREFIX_LEN: usize, const INFIX_LEN: usize, O: KeySchema<KEY_LEN>, F>(
        &self,
        prefix: &[u8; PREFIX_LEN],
        at_depth: usize,
        f: &mut F,
    ) where
        F: FnMut(&[u8; INFIX_LEN]),
    {
        // Delegate to the runtime has_prefix which accepts slices; the
        // compiler will coerce the array reference to a slice.
        if !self.has_prefix::<O>(at_depth, prefix) {
            return;
        }

        let infix: [u8; INFIX_LEN] =
            core::array::from_fn(|i| self.key[O::TREE_TO_KEY[PREFIX_LEN + i]]);
        f(&infix);
    }

    pub fn has_prefix<O: KeySchema<KEY_LEN>>(&self, at_depth: usize, prefix: &[u8]) -> bool {
        let limit = std::cmp::min(prefix.len(), KEY_LEN);
        for (depth, &p) in prefix.iter().enumerate().take(limit).skip(at_depth) {
            if self.key[O::TREE_TO_KEY[depth]] != p {
                return false;
            }
        }
        true
    }

    pub fn get<'a, O: KeySchema<KEY_LEN> + 'a>(
        &'a self,
        at_depth: usize,
        key: &[u8; KEY_LEN],
    ) -> Option<&'a V> {
        let limit = KEY_LEN;
        for (depth, &kbyte) in key.iter().enumerate().take(limit).skip(at_depth) {
            let idx = O::TREE_TO_KEY[depth];
            if self.key[idx] != kbyte {
                return None;
            }
        }
        Some(&self.value)
    }

    pub fn segmented_len<O: KeySchema<KEY_LEN>, const PREFIX_LEN: usize>(
        &self,
        at_depth: usize,
        prefix: &[u8; PREFIX_LEN],
    ) -> u64 {
        let limit = PREFIX_LEN;
        for (depth, &p) in prefix.iter().enumerate().take(limit).skip(at_depth) {
            let key_depth = O::TREE_TO_KEY[depth];
            if self.key[key_depth] != p {
                return 0;
            }
        }
        1
    }
}
