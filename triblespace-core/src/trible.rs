//! Representation of a single knowledge graph edge.
//!
//! For layout details and edge semantics see the [Trible Structure](../book/src/deep-dive/trible-structure.md) chapter of the Tribles Book.

mod tribleset;

use std::convert::TryInto;

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::value::Value;
use crate::value::ValueSchema;

pub use tribleset::TribleSet;

/// The length of a trible in bytes.
pub const TRIBLE_LEN: usize = 64;

/// The start index of the entity in a trible.
pub const E_START: usize = 0;
/// The end index of the entity in a trible (inclusive).
pub const E_END: usize = 15;

/// The start index of the attribute in a trible.
pub const A_START: usize = 16;
/// The end index of the attribute in a trible (inclusive).
pub const A_END: usize = 31;

/// The start index of the value in a trible.
pub const V_START: usize = 32;
/// The end index of the value in a trible (inclusive).
pub const V_END: usize = 63;

/// Fundamentally a trible is always a collection of 64 bytes.
pub type RawTrible = [u8; TRIBLE_LEN];

/// Fundamental 64-byte tuple of entity, attribute and value used throughout the
/// knowledge graph.
///
/// See the [Trible Structure](../book/src/deep-dive/trible-structure.md)
/// chapter of the Tribles Book for a detailed discussion of the layout and its
/// design rationale.
#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Trible {
    pub data: RawTrible,
}

impl Trible {
    /// Creates a new trible from an entity, an attribute, and a value.
    ///
    /// # Arguments
    ///
    /// * `e` - The entity of the trible.
    /// * `a` - The attribute of the trible.
    /// * `v` - The value of the trible.
    ///
    /// # Returns
    ///
    /// A new trible.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    /// use valueschemas::R256;
    ///
    /// let e = fucid();
    /// let a = fucid();
    /// let v: Value<R256> = R256::value_from(42);
    /// let trible = Trible::new(&e, &a, &v);
    /// ```
    pub fn new<V: ValueSchema>(e: &ExclusiveId, a: &Id, v: &Value<V>) -> Trible {
        let mut data = [0; TRIBLE_LEN];
        data[E_START..=E_END].copy_from_slice(&e[..]);
        data[A_START..=A_END].copy_from_slice(&a[..]);
        data[V_START..=V_END].copy_from_slice(&v.raw[..]);

        Self { data }
    }

    /// Creates a new trible from an entity, an attribute, and a value.
    /// This is similar to [Trible::new], but takes a plain entity id instead of an owned id.
    /// Allowing to circumvent the ownership system, which can be used to inject
    /// data into a local knowledge graph without owning the entity.
    /// This is useful for loading existing trible data, for example when loading
    /// an existing [crate::trible::TribleSet] from a blob, or when declaring
    /// a namespace.
    ///
    /// # Arguments
    ///
    /// * `e` - The entity of the trible.
    /// * `a` - The attribute of the trible.
    /// * `v` - The value of the trible.
    ///
    /// # Returns
    ///
    /// A new trible.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    /// use valueschemas::R256;
    ///
    /// let e = fucid();
    /// let a = fucid();
    /// let v: Value<R256> = R256::value_from(42);
    /// let trible = Trible::force(&e, &a, &v);
    ///
    /// assert_eq!(trible.e(), &*e);
    /// ```
    pub fn force<V: ValueSchema>(e: &Id, a: &Id, v: &Value<V>) -> Trible {
        Trible::new(ExclusiveId::force_ref(e), a, v)
    }

    /// Creates a new trible from a raw trible (a 64-byte array).
    /// It circumvents the ownership system, and is useful for loading existing trible data,
    /// just like [Trible::force].
    ///
    /// # Arguments
    ///
    /// * `data` - The raw trible.
    ///
    /// # Returns
    ///
    /// A new trible if the entity and attribute are not nil
    /// (i.e. they are not all zeroes), otherwise `None`.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    ///
    /// let data = [
    ///    // Entity
    ///    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ///    // Attribute
    ///    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ///    // Value
    ///    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    ///    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    /// ];
    /// let trible = Trible::force_raw(data);
    /// assert!(trible.is_some());
    /// ```
    pub fn force_raw(data: RawTrible) -> Option<Trible> {
        if data[E_START..=E_END].iter().all(|&x| x == 0)
            || data[A_START..=A_END].iter().all(|&x| x == 0)
        {
            return None;
        }
        Some(Self { data })
    }

    /// Transmutes a raw trible reference into a trible reference.
    /// Circumvents the ownership system, and is useful for loading existing trible data,
    /// just like [Trible::force] and [Trible::force_raw].
    ///
    /// # Arguments
    ///
    /// * `data` - The raw trible reference.
    ///
    /// # Returns
    ///
    /// A new trible reference if the entity and attribute are not nil
    /// (i.e. they are not all zeroes), otherwise `None`.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    ///
    /// let data = [
    ///   // Entity
    ///   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ///   // Attribute
    ///   16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ///   // Value
    ///   32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    ///   48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    /// ];
    /// let trible = Trible::as_transmute_force_raw(&data);
    /// assert!(trible.is_some());
    /// ```
    pub fn as_transmute_force_raw(data: &RawTrible) -> Option<&Self> {
        if data[E_START..=E_END].iter().all(|&x| x == 0)
            || data[A_START..=A_END].iter().all(|&x| x == 0)
        {
            return None;
        }
        Some(unsafe { std::mem::transmute::<&RawTrible, &Self>(data) })
    }

    /// Transmutes a raw trible reference into a trible reference.
    /// Circumvents the ownership system, and does not check if the entity and attribute are nil.
    /// Should only be used if it it certain that the `RawTrible` is actually valid.
    pub fn as_transmute_raw_unchecked(data: &RawTrible) -> &Self {
        unsafe { std::mem::transmute::<&RawTrible, &Self>(data) }
    }

    /// Returns the entity of the trible.
    ///
    /// # Returns
    ///
    /// The entity of the trible.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    ///
    /// let data = [
    ///   // Entity
    ///   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ///   // Attribute
    ///   16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ///   // Value
    ///   32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    ///   48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    /// ];
    /// let trible = Trible::force_raw(data).unwrap();
    /// let entity = trible.e();
    /// assert_eq!(entity, &Id::new([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]).unwrap());
    /// ```
    pub fn e(&self) -> &Id {
        Id::as_transmute_raw(self.data[E_START..=E_END].try_into().unwrap()).unwrap()
    }

    /// Returns the attribute of the trible.
    ///
    /// # Returns
    ///
    /// The attribute of the trible.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    ///
    /// let data = [
    ///   // Entity
    ///   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ///   // Attribute
    ///   16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ///   // Value
    ///   32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    ///   48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    /// ];
    /// let trible = Trible::force_raw(data).unwrap();
    /// let attribute = trible.a();
    /// assert_eq!(attribute, &Id::new([16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]).unwrap());
    /// ```
    pub fn a(&self) -> &Id {
        Id::as_transmute_raw(self.data[A_START..=A_END].try_into().unwrap()).unwrap()
    }

    /// Returns the value of the trible.
    ///
    /// # Returns
    ///
    /// The value of the trible.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    /// use valueschemas::R256;
    ///
    /// let data = [
    ///   // Entity
    ///   0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ///   // Attribute
    ///   16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    ///   // Value
    ///   32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    ///   48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    /// ];
    /// let trible = Trible::force_raw(data).unwrap();
    /// let value = trible.v::<R256>();
    /// assert_eq!(value, &Value::new([32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    /// 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63]));
    /// ```
    pub fn v<V: ValueSchema>(&self) -> &Value<V> {
        Value::as_transmute_raw(self.data[V_START..=V_END].try_into().unwrap())
    }
}

crate::key_segmentation!(TribleSegmentation, TRIBLE_LEN, [16, 16, 32]);

crate::key_schema!(EAVOrder, TribleSegmentation, TRIBLE_LEN, [0, 1, 2]);
crate::key_schema!(EVAOrder, TribleSegmentation, TRIBLE_LEN, [0, 2, 1]);
crate::key_schema!(AEVOrder, TribleSegmentation, TRIBLE_LEN, [1, 0, 2]);
crate::key_schema!(AVEOrder, TribleSegmentation, TRIBLE_LEN, [1, 2, 0]);
crate::key_schema!(VEAOrder, TribleSegmentation, TRIBLE_LEN, [2, 0, 1]);
crate::key_schema!(VAEOrder, TribleSegmentation, TRIBLE_LEN, [2, 1, 0]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::KeySchema;

    #[rustfmt::skip]
    #[test]
    fn order_eav() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        let tree_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
            48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        assert_eq!(EAVOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(EAVOrder::key_ordered(&tree_bytes), key_bytes);
    }

    #[rustfmt::skip]
    #[test]
    fn order_eva() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        let tree_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
            48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ];
        assert_eq!(EVAOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(EVAOrder::key_ordered(&tree_bytes), key_bytes);
    }

    #[rustfmt::skip]
    #[test]
    fn order_aev() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        let tree_bytes = [
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 10, 11, 12, 13, 14, 15, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        assert_eq!(AEVOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(AEVOrder::key_ordered(&tree_bytes), key_bytes);
    }

    #[rustfmt::skip]
    #[test]
    fn order_ave() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        let tree_bytes = [
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
            38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
            60, 61, 62, 63, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ];
        assert_eq!(AVEOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(AVEOrder::key_ordered(&tree_bytes), key_bytes);
    }

    #[rustfmt::skip]
    #[test]
    fn order_vea() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ]; 
        let tree_bytes = [
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
            48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
        ];
        assert_eq!(VEAOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(VEAOrder::key_ordered(&tree_bytes), key_bytes);
    }

    #[rustfmt::skip]
    #[test]
    fn order_vae() {
        let key_bytes = [
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
            46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
        ];
        let tree_bytes = [
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
            48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ];
        assert_eq!(VAEOrder::tree_ordered(&key_bytes), tree_bytes);
        assert_eq!(VAEOrder::key_ordered(&tree_bytes), key_bytes);
    }
}
