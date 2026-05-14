//! Value type and conversion traits for schema types. For a deeper look at
//! portability goals, common formats, and schema design, refer to the
//! "Portability & Common Formats" chapter in the project book.
//!
//! # Example
//!
//! ```
//! use triblespace_core::value::{Value, ValueSchema, IntoValue, TryFromValue};
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
//! impl IntoValue<MyNumber> for u32 {
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
//! impl IntoValue<MyNumber> for u64 {
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

    /// The trait parameter to dispatch via for `entity!{}` field
    /// conversion. For *inline* schemas (32-byte data lives in the
    /// trible), set `FieldKind = Self` — sources convert via
    /// `IntoSchema<Self> { Form = Value<Self> }`. For
    /// [`Handle<T>`](crate::value::schemas::hash::Handle), set
    /// `FieldKind = T` — sources convert via `IntoSchema<T> { Form =
    /// Blob<T> }`. The BlobSchema `T` sitting directly at trait
    /// position 0 is what lets downstream impl `IntoSchema<MyBlob>
    /// for MyType` without bumping into the orphan rule.
    type FieldKind;

    /// Check if the given value conforms to this schema.
    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        Ok(value)
    }

    /// Create a new value from a concrete Rust type via [`IntoValue`].
    /// Panics if the underlying conversion panics.
    fn value_from<T: IntoValue<Self>>(t: T) -> Value<Self> {
        t.to_value()
    }

    /// Create a new value from a concrete Rust type via [`TryToValue`].
    /// Returns an error if the conversion fails.
    fn value_try_from<T: TryToValue<Self>>(
        t: T,
    ) -> Result<Value<Self>, <T as TryToValue<Self>>::Error> {
        t.try_to_value()
    }

    /// Expand an already-encoded `Value<Self>` into the field-pair
    /// shape `entity!{}` consumes. Inline schemas: `(value, None)`,
    /// no side-blob.
    ///
    /// Overridable if a schema has unusual storage semantics. The
    /// blob-path counterpart lives on
    /// [`BlobSchema::into_field_pair`](crate::blob::BlobSchema::into_field_pair).
    fn into_field_pair(
        form: Value<Self>,
    ) -> (
        Value<Self>,
        Option<crate::blob::Blob<crate::blob::schemas::UnknownBlob>>,
    ) {
        (form, None)
    }
}

/// Fallible variant of value conversion — `T → Result<Value<S>, Error>`.
///
/// Kept as a standalone trait (not folded into [`IntoSchema`])
/// because the error type is part of the per-source/per-target contract.
/// Used for parses that can fail (e.g. `&str → Hash<Blake3>` via
/// hex-decoding).
pub trait TryToValue<S: ValueSchema> {
    /// The error type returned when the conversion fails.
    type Error;
    /// Convert the Rust type to a [Value] with a specific schema type.
    fn try_to_value(self) -> Result<Value<S>, Self::Error>;
}

/// Convert a value into its **form** for a schema target — either a
/// directly-encoded `Value<S>` (inline path, `S: ValueSchema`) or a
/// `Blob<S>` for content-addressed storage (handle path, `S: BlobSchema`).
///
/// `IntoSchema<S>` is the **sole** source-to-schema conversion trait.
/// `S` is intentionally unbounded so the same trait can target either
/// a `ValueSchema` (Form = `Value<S>`) or a `BlobSchema`
/// (Form = `Blob<S>`). The Form's relationship to `S` is captured by
/// [`FieldFormFor`], which knows how to expand the form into the
/// `(Value<V>, Option<Blob<UnknownBlob>>)` pair that the `entity!{}`
/// macro folds into a Fragment.
///
/// The key property: with `S` at trait position 0, downstream that
/// defines a local `MyBlobSchema` writes `impl IntoSchema<MyBlobSchema>
/// for MyType` — the local type sits at trait position 0, which
/// makes Rust's orphan rule see the impl as legal even when `MyType`
/// is a foreign type (like `Vec<u8>` or a third-party crate's view).
/// This is the property the IntoValue/IntoBlob split provided
/// historically; preserved here by keeping the schema type unbuiried.
pub trait IntoSchema<S> {
    /// The concrete form this source produces.
    type Form;
    /// Run the conversion.
    fn into_schema(self) -> Self::Form;
}

/// Shorthand bound for `IntoSchema<S, Form = Value<S>>` — "this
/// source produces a directly-encoded `Value<S>`, no side-blob."
///
/// `IntoValue` is a supertrait alias over [`IntoSchema`]: any type
/// that implements `IntoSchema<S>` with `Form = Value<S>`
/// automatically becomes `IntoValue<S>`, and gains the
/// `to_value(self) -> Value<S>` convenience method.
pub trait IntoValue<S: ValueSchema>: IntoSchema<S, Form = Value<S>> {
    /// Convert directly to `Value<S>`.
    fn to_value(self) -> Value<S>
    where
        Self: Sized,
    {
        self.into_schema()
    }
}
impl<S, T> IntoValue<S> for T
where
    S: ValueSchema,
    T: IntoSchema<S, Form = Value<S>>,
{
}

/// Expand an [`IntoSchema::Form`] into the `(Value<V>, Option<Blob<UnknownBlob>>)`
/// pair that the `entity!{}` macro folds into a Fragment.
///
/// `V` is the *attribute's* value schema. Two impls cover everything:
/// - `Value<V>` delegates to [`ValueSchema::into_field_pair`] — inline
///   path, default `(value, None)`.
/// - `Blob<T>` targeting `Handle<T>` delegates to
///   [`BlobSchema::into_field_pair`](crate::blob::BlobSchema::into_field_pair) —
///   handle path, default `(cached_handle, Some(transmuted_blob))`.
///
/// This trait is the **dispatch shim** for the macro layer; the
/// actual logic lives on the schema traits so users (and overriding
/// schemas) can call it directly without going through the trait.
/// The split between `IntoSchema` (which produces a `Form` keyed on
/// whatever discriminator the schema uses) and `FieldFormFor` (which
/// expands the form keyed on the actual value-schema `V`) lets the
/// per-source impls of `IntoSchema` stay one-line.
pub trait FieldFormFor<V: ValueSchema> {
    /// Produce the (value, optional-blob) pair the macro absorbs.
    fn into_field_pair(
        self,
    ) -> (
        Value<V>,
        Option<crate::blob::Blob<crate::blob::schemas::UnknownBlob>>,
    );
}

impl<V: ValueSchema> FieldFormFor<V> for Value<V> {
    fn into_field_pair(
        self,
    ) -> (
        Value<V>,
        Option<crate::blob::Blob<crate::blob::schemas::UnknownBlob>>,
    ) {
        <V as ValueSchema>::into_field_pair(self)
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

impl<S: ValueSchema> IntoSchema<S> for Value<S> {
    type Form = Value<S>;
    fn into_schema(self) -> Value<S> {
        self
    }
}

impl<S: ValueSchema> IntoSchema<S> for &Value<S> {
    type Form = Value<S>;
    fn into_schema(self) -> Value<S> {
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
