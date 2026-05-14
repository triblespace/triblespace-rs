//! Value type and conversion traits for schema types. For a deeper look at
//! portability goals, common formats, and schema design, refer to the
//! "Portability & Common Formats" chapter in the project book.
//!
//! # Example
//!
//! ```
//! use triblespace_core::value::{Value, ValueSchema, ToValue, TryFromValue};
//! use triblespace_core::metadata::MetaDescribe;
//! use triblespace_core::trible::{Fragment, TribleSet};
//! use triblespace_core::macros::id_hex;
//! use std::convert::{TryInto, Infallible};
//!
//! // Define a new schema type.
//! // We're going to define an unsigned integer type that is stored as a little-endian 32-byte array.
//! // Note that makes our example easier, as we don't have to worry about sign-extension or padding bytes.
//! pub struct MyNumber;
//!
//! // The schema's identity hex lives inline in its describe body — that's
//! // the only place it appears; callers reach the id via MyNumber::id().
//! impl MetaDescribe for MyNumber {
//!    fn describe() -> Fragment {
//!        Fragment::rooted(id_hex!("345EAC0C5B5D7D034C87777280B88AE2"), TribleSet::new())
//!    }
//! }
//! impl ValueSchema for MyNumber {
//!    type ValidationError = ();
//!    // Every bit pattern is valid for this schema.
//! }
//!
//! // Implement conversion functions for the schema type.
//! // Use `Error = Infallible` when the conversion cannot fail.
//! impl TryFromValue<'_, MyNumber> for u32 {
//!    type Error = Infallible;
//!    fn try_from_value(v: &Value<MyNumber>) -> Result<Self, Infallible> {
//!      Ok(u32::from_le_bytes(v.raw[0..4].try_into().unwrap()))
//!    }
//! }
//!
//! impl ToValue<MyNumber> for u32 {
//!   fn to_value(self) -> Value<MyNumber> {
//!      // Convert the Rust type to the schema type, i.e. a 32-byte array.
//!      let mut bytes = [0; 32];
//!      bytes[0..4].copy_from_slice(&self.to_le_bytes());
//!      Value::new(bytes)
//!   }
//! }
//!
//! // Use the schema type to store and retrieve a Rust type.
//! let value: Value<MyNumber> = MyNumber::value_from(42u32);
//! let i: u32 = value.from_value();
//! assert_eq!(i, 42);
//!
//! // You can also implement conversion functions for other Rust types.
//! impl TryFromValue<'_, MyNumber> for u64 {
//!   type Error = Infallible;
//!   fn try_from_value(v: &Value<MyNumber>) -> Result<Self, Infallible> {
//!    Ok(u64::from_le_bytes(v.raw[0..8].try_into().unwrap()))
//!   }
//! }
//!
//! impl ToValue<MyNumber> for u64 {
//!  fn to_value(self) -> Value<MyNumber> {
//!   let mut bytes = [0; 32];
//!   bytes[0..8].copy_from_slice(&self.to_le_bytes());
//!   Value::new(bytes)
//!   }
//! }
//!
//! let value: Value<MyNumber> = MyNumber::value_from(42u64);
//! let i: u64 = value.from_value();
//! assert_eq!(i, 42);
//!
//! // And use a value round-trip to convert between Rust types.
//! let value: Value<MyNumber> = MyNumber::value_from(42u32);
//! let i: u64 = value.from_value();
//! assert_eq!(i, 42);
//! ```

/// Built-in value schema types and their conversion implementations.
pub mod schemas;

use crate::metadata::MetaDescribe;

use core::fmt;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use hex::ToHex;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;
use zerocopy::TryFromBytes;
use zerocopy::Unaligned;

/// The length of a value in bytes.
pub const VALUE_LEN: usize = 32;

/// A raw value is simply a 32-byte array.
pub type RawValue = [u8; VALUE_LEN];

/// A value is a 32-byte array that can be (de)serialized as a Rust type.
/// The schema type parameter is an abstract type that represents the meaning
/// and valid bit patterns of the bytes.
///
/// # Example
///
/// ```
/// use triblespace_core::prelude::*;
/// use valueschemas::R256;
/// use num_rational::Ratio;
///
/// let ratio = Ratio::new(1, 2);
/// let value: Value<R256> = R256::value_from(ratio);
/// let ratio2: Ratio<i128> = value.try_from_value().unwrap();
/// assert_eq!(ratio, ratio2);
/// ```
#[derive(TryFromBytes, IntoBytes, Unaligned, Immutable, KnownLayout)]
#[repr(transparent)]
pub struct Value<T: ValueSchema> {
    /// The 32-byte representation of this value.
    pub raw: RawValue,
    _schema: PhantomData<T>,
}

impl<S: ValueSchema> Value<S> {
    /// Create a new value from a 32-byte array.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::value::{Value, ValueSchema};
    /// use triblespace_core::value::schemas::UnknownValue;
    ///
    /// let bytes = [0; 32];
    /// let value = Value::<UnknownValue>::new(bytes);
    /// ```
    pub fn new(value: RawValue) -> Self {
        Self {
            raw: value,
            _schema: PhantomData,
        }
    }

    /// Validate this value using its schema.
    pub fn validate(self) -> Result<Self, S::ValidationError> {
        S::validate(self)
    }

    /// Check if this value conforms to its schema.
    pub fn is_valid(&self) -> bool {
        S::validate(*self).is_ok()
    }

    /// Transmute a value from one schema type to another.
    /// This is a safe operation, as the bytes are not changed.
    /// The schema type is only changed in the type system.
    /// This is a zero-cost operation.
    /// This is useful when you have a value with an abstract schema type,
    /// but you know the concrete schema type.
    pub fn transmute<O>(self) -> Value<O>
    where
        O: ValueSchema,
    {
        Value::new(self.raw)
    }

    /// Transmute a value reference from one schema type to another.
    /// This is a safe operation, as the bytes are not changed.
    /// The schema type is only changed in the type system.
    /// This is a zero-cost operation.
    /// This is useful when you have a value reference with an abstract schema type,
    /// but you know the concrete schema type.
    pub fn as_transmute<O>(&self) -> &Value<O>
    where
        O: ValueSchema,
    {
        unsafe { std::mem::transmute(self) }
    }

    /// Transmute a raw value reference to a value reference.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::value::{Value, ValueSchema};
    /// use triblespace_core::value::schemas::UnknownValue;
    /// use std::borrow::Borrow;
    ///
    /// let bytes = [0; 32];
    /// let value: Value<UnknownValue> = Value::new(bytes);
    /// let value_ref: &Value<UnknownValue> = &value;
    /// let raw_value_ref: &[u8; 32] = value_ref.borrow();
    /// let value_ref2: &Value<UnknownValue> = Value::as_transmute_raw(raw_value_ref);
    /// assert_eq!(&value, value_ref2);
    /// ```
    pub fn as_transmute_raw(value: &RawValue) -> &Self {
        unsafe { std::mem::transmute(value) }
    }

    /// Deserialize a value with an abstract schema type to a concrete Rust type.
    ///
    /// This method only works for infallible conversions (where `Error = Infallible`).
    /// For fallible conversions, use the [Value::try_from_value] method.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    /// use valueschemas::F64;
    ///
    /// let value: Value<F64> = (3.14f64).to_value();
    /// let concrete: f64 = value.from_value();
    /// ```
    pub fn from_value<'a, T>(&'a self) -> T
    where
        T: TryFromValue<'a, S, Error = std::convert::Infallible>,
    {
        match <T as TryFromValue<'a, S>>::try_from_value(self) {
            Ok(v) => v,
            Err(e) => match e {},
        }
    }

    /// Deserialize a value with an abstract schema type to a concrete Rust type.
    ///
    /// This method returns an error if the conversion is not possible.
    /// This might happen if the bytes are not valid for the schema type or if the
    /// rust type can't represent the specific value of the schema type,
    /// e.g. if the schema type is a fractional number and the rust type is an integer.
    ///
    /// For infallible conversions, use the [Value::from_value] method.
    ///
    /// # Example
    ///
    /// ```
    /// use triblespace_core::prelude::*;
    /// use valueschemas::R256;
    /// use num_rational::Ratio;
    ///
    /// let value: Value<R256> = R256::value_from(Ratio::new(1, 2));
    /// let concrete: Result<Ratio<i128>, _> = value.try_from_value();
    /// ```
    ///
    pub fn try_from_value<'a, T>(&'a self) -> Result<T, <T as TryFromValue<'a, S>>::Error>
    where
        T: TryFromValue<'a, S>,
    {
        <T as TryFromValue<'a, S>>::try_from_value(self)
    }
}

impl<T: ValueSchema> Copy for Value<T> {}

impl<T: ValueSchema> Clone for Value<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ValueSchema> PartialEq for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl<T: ValueSchema> Eq for Value<T> {}

impl<T: ValueSchema> Hash for Value<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw.hash(state);
    }
}

impl<T: ValueSchema> Ord for Value<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl<T: ValueSchema> PartialOrd for Value<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: ValueSchema> Borrow<RawValue> for Value<S> {
    fn borrow(&self) -> &RawValue {
        &self.raw
    }
}

impl<T: ValueSchema> Debug for Value<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Value<{}>({})",
            std::any::type_name::<T>(),
            ToHex::encode_hex::<String>(&self.raw)
        )
    }
}

/// A trait that represents an abstract schema type that can be (de)serialized as a [Value].
///
/// This trait is usually implemented on a type-level empty struct,
/// but may contain additional information about the schema type as associated constants or types.
/// The [Handle](crate::value::schemas::hash::Handle) type for example contains type information about the hash algorithm,
/// and the schema of the referenced blob.
///
/// See the [value](crate::value) module for more information.
/// See the [BlobSchema](crate::blob::BlobSchema) trait for the counterpart trait for blobs.
pub trait ValueSchema: MetaDescribe + Sized + 'static {
    /// The error type returned by [`validate`](ValueSchema::validate).
    /// Use `()` or [`Infallible`](std::convert::Infallible) when every bit pattern is valid.
    type ValidationError;

    /// Check if the given value conforms to this schema.
    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        Ok(value)
    }

    /// Create a new value from a concrete Rust type.
    /// This is a convenience method that calls the [ToValue] trait.
    /// This method might panic if the conversion is not possible.
    ///
    /// See the [ValueSchema::value_try_from] method for a conversion that returns a result.
    fn value_from<T: ToValue<Self>>(t: T) -> Value<Self> {
        t.to_value()
    }

    /// Create a new value from a concrete Rust type.
    /// This is a convenience method that calls the [TryToValue] trait.
    /// This method might return an error if the conversion is not possible.
    ///
    /// See the [ValueSchema::value_from] method for a conversion that always succeeds (or panics).
    fn value_try_from<T: TryToValue<Self>>(
        t: T,
    ) -> Result<Value<Self>, <T as TryToValue<Self>>::Error> {
        t.try_to_value()
    }
}

/// A trait for converting a Rust type to a [Value] with a specific schema type.
/// This trait is implemented on the concrete Rust type.
///
/// This might cause a panic if the conversion is not possible,
/// see [TryToValue] for a conversion that returns a result.
///
/// This is the counterpart to the [TryFromValue] trait.
///
/// See [ToBlob](crate::blob::ToBlob) for the counterpart trait for blobs.
pub trait ToValue<S: ValueSchema> {
    /// Convert the Rust type to a [Value] with a specific schema type.
    /// This might cause a panic if the conversion is not possible.
    ///
    /// See the [TryToValue] trait for a conversion that returns a result.
    fn to_value(self) -> Value<S>;
}

/// A trait for converting a Rust type to a [Value] with a specific schema type.
/// This trait is implemented on the concrete Rust type.
///
/// This might return an error if the conversion is not possible,
/// see [ToValue] for cases where the conversion is guaranteed to succeed (or panic).
///
/// This is the counterpart to the [TryFromValue] trait.
///
pub trait TryToValue<S: ValueSchema> {
    /// The error type returned when the conversion fails.
    type Error;
    /// Convert the Rust type to a [Value] with a specific schema type.
    /// This might return an error if the conversion is not possible.
    ///
    /// See the [ToValue] trait for a conversion that always succeeds (or panics).
    fn try_to_value(self) -> Result<Value<S>, Self::Error>;
}

/// Convert a value into the pair `(Value<S>, Option<Bytes>)` an
/// `entity!{}` field expects: the encoded value goes into the
/// fragment's facts, and any side-bytes the conversion produced
/// (typically a freshly-hashed blob for a `Handle`-typed field) get
/// absorbed into the fragment's local blob store.
///
/// This is the entry point the `entity!{}` macro uses internally so
/// that a fragment is *self-contained by construction* — every
/// handle that appears in the fragment's facts has its bytes
/// available without consulting an external blob store. The Bytes
/// half is type-erased on purpose: the blob store is keyed by
/// content hash, so the original `BlobSchema` only matters to derive
/// the handle (which the impl has already done by the time bytes
/// come out of this trait).
///
/// Why a separate trait from [`ToValue`]? `ToValue<S>` is a pure
/// (value → Value<S>) conversion with no notion of side payload —
/// it's the right abstraction at the *value* layer. `IntoFieldValue`
/// is the *insertion-side* abstraction that knows handles need
/// their bytes deposited somewhere too. Every `ToValue<S>` impl
/// composes into `IntoFieldValue<S>` via the blanket below; the only
/// case that needs explicit handling is `Blob<T>` targeting
/// `Handle<T>`, where the bytes need to come along.
pub trait IntoFieldValue<S: ValueSchema> {
    /// Produce the `(value, optional-bytes)` pair for this field.
    /// The bytes (if any) are content-addressed under the same hash
    /// that the value's handle references.
    fn into_field_value(self) -> (Value<S>, Option<anybytes::Bytes>);
}

/// Blanket: any `ToValue<S>` is an `IntoFieldValue<S>` with no
/// side-bytes. Covers `Value<S>` itself, `&Value<S>`, native types
/// (`u32`, `bool`, `&str`, …) that schemas implement `ToValue` for,
/// and so on.
impl<S, V> IntoFieldValue<S> for V
where
    S: ValueSchema,
    V: ToValue<S>,
{
    fn into_field_value(self) -> (Value<S>, Option<anybytes::Bytes>) {
        (self.to_value(), None)
    }
}

/// A trait for converting a [Value] with a specific schema type to a Rust type.
/// This trait is implemented on the concrete Rust type.
///
/// Values are 32-byte arrays that represent data at a deserialization boundary.
/// Conversions may fail depending on the schema and target type. Use
/// `Error = Infallible` for conversions that genuinely cannot fail (e.g.
/// `ethnum::U256` from `U256BE`), and a real error type for narrowing
/// conversions (e.g. `u64` from `U256BE`).
///
/// This is the counterpart to the [TryToValue] trait.
///
/// See [TryFromBlob](crate::blob::TryFromBlob) for the counterpart trait for blobs.
pub trait TryFromValue<'a, S: ValueSchema>: Sized {
    /// The error type returned when the conversion fails.
    type Error;
    /// Convert the [Value] with a specific schema type to the Rust type.
    fn try_from_value(v: &'a Value<S>) -> Result<Self, Self::Error>;
}

impl<S: ValueSchema> ToValue<S> for Value<S> {
    fn to_value(self) -> Value<S> {
        self
    }
}

impl<S: ValueSchema> ToValue<S> for &Value<S> {
    fn to_value(self) -> Value<S> {
        *self
    }
}

impl<'a, S: ValueSchema> TryFromValue<'a, S> for Value<S> {
    type Error = std::convert::Infallible;
    fn try_from_value(v: &'a Value<S>) -> Result<Self, std::convert::Infallible> {
        Ok(*v)
    }
}

impl<'a, S: ValueSchema> TryFromValue<'a, S> for () {
    type Error = std::convert::Infallible;
    fn try_from_value(_v: &'a Value<S>) -> Result<Self, std::convert::Infallible> {
        Ok(())
    }
}
