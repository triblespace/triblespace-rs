use super::*;

#[derive(Debug)]
#[repr(C)]
pub struct Entry<const KEY_LEN: usize, V = ()> {
    ptr: NonNull<Leaf<KEY_LEN, V>>,
}

impl<const KEY_LEN: usize> Entry<KEY_LEN> {
    pub fn new(key: &[u8; KEY_LEN]) -> Self {
        unsafe {
            let ptr = Leaf::<KEY_LEN, ()>::new(key, ());
            Self { ptr }
        }
    }
}

impl<const KEY_LEN: usize, V> Entry<KEY_LEN, V> {
    pub fn with_value(key: &[u8; KEY_LEN], value: V) -> Self {
        unsafe {
            let ptr = Leaf::<KEY_LEN, V>::new(key, value);
            Self { ptr }
        }
    }

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
