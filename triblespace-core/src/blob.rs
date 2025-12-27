//! Anything that can be represented as a byte sequence.
//!
//! Blobs store larger data items outside tribles and values. For the design
//! rationale and an extended usage example see the [Blobs
//! chapter](../book/src/deep-dive/blobs.md) of the Tribles Book.

// Converting Rust types to blobs is infallible in practice, so only `ToBlob`
// and `TryFromBlob` are used throughout the codebase.  `TryToBlob` and
// `FromBlob` were never required and have been removed for simplicity.

mod cache;
mod memoryblobstore;
pub mod schemas;

use crate::metadata::ConstMetadata;
use crate::value::schemas::hash::Handle;
use crate::value::schemas::hash::HashProtocol;
use crate::value::Value;
use crate::value::ValueSchema;

use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self};
use std::hash::Hash;
use std::marker::PhantomData;

pub use cache::BlobCache;
pub use memoryblobstore::MemoryBlobStore;

pub use anybytes::Bytes;

/// A blob is a immutable sequence of bytes that can be used to represent any kind of data.
/// It is the fundamental building block of data storage and transmission.
/// The `BlobSchema` type parameter is used to define the abstract schema type of a blob.
/// This is similar to the `Value` type and the `ValueSchema` trait in the [`value`](crate::value) module.
#[repr(transparent)]
pub struct Blob<S: BlobSchema> {
    pub bytes: Bytes,
    _schema: PhantomData<S>,
}

impl<S: BlobSchema> Blob<S> {
    /// Creates a new blob from a sequence of bytes.
    /// The bytes are stored in the blob as-is.
    pub fn new(bytes: Bytes) -> Self {
        Self {
            bytes,
            _schema: PhantomData,
        }
    }

    /// Reinterprets the contained bytes as a blob of a different schema.
    ///
    /// This is a zero-copy transformation that simply changes the compile-time
    /// schema marker. It does **not** validate that the data actually conforms
    /// to the new schema.
    pub fn transmute<T: BlobSchema>(self) -> Blob<T> {
        Blob {
            bytes: self.bytes,
            _schema: PhantomData,
        }
    }

    /// Transmutes the blob to a blob of a different schema.
    /// This is a zero-cost operation.
    /// If the schema types are not compatible, this will not cause undefined behavior,
    /// but it might cause unexpected results.
    ///
    /// This is primarily used to give blobs with an [UnknownBlob](crate::blob::schemas::UnknownBlob) schema a more specific schema.
    /// Use with caution.
    pub fn as_transmute<T: BlobSchema>(&self) -> &Blob<T> {
        unsafe { std::mem::transmute(self) }
    }

    // Note: Do we want to cache the handle somewhere so that we don't have to compute the hash every time?
    // We could use WeakBytes for this, but it would require one hash-map per HashProtocol.

    /// Hashes the blob with the given hash protocol and returns the hash as a handle.
    pub fn get_handle<H>(&self) -> Value<Handle<H, S>>
    where
        H: HashProtocol,
        Handle<H, S>: ValueSchema,
    {
        let digest = H::digest(&self.bytes);
        Value::new(digest.into())
    }

    /// Tries to convert the blob to a concrete Rust type.
    /// If the conversion fails, an error is returned.
    pub fn try_from_blob<T>(self) -> Result<T, <T as TryFromBlob<S>>::Error>
    where
        T: TryFromBlob<S>,
    {
        <T as TryFromBlob<S>>::try_from_blob(self)
    }
}

impl<T: BlobSchema> Clone for Blob<T> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            _schema: PhantomData,
        }
    }
}

impl<T: BlobSchema> PartialEq for Blob<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T: BlobSchema> Eq for Blob<T> {}

impl<T: BlobSchema> Hash for Blob<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}

impl<T: BlobSchema> Debug for Blob<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Blob<{}>", std::any::type_name::<T>())
    }
}

/// A trait for defining the abstract schema type of a blob.
/// This is similar to the `ValueSchema` trait in the [`value`](crate::value) module.
pub trait BlobSchema: ConstMetadata + Sized + 'static {
    /// Converts a concrete Rust type to a blob with this schema.
    /// If the conversion fails, this might cause a panic.
    fn blob_from<T: ToBlob<Self>>(t: T) -> Blob<Self> {
        t.to_blob()
    }
}

/// A trait for converting a Rust type to a [Blob] with a specific schema.
/// This trait is implemented on the concrete Rust type.
///
/// Conversions are infallible.  Use [`TryFromBlob`] on the target type to
/// perform the fallible reverse conversion.
///
/// See [ToValue](crate::value::ToValue) for the counterpart trait for values.
pub trait ToBlob<S: BlobSchema> {
    fn to_blob(self) -> Blob<S>;
}

/// A trait for converting a [Blob] with a specific schema to a Rust type.
/// This trait is implemented on the concrete Rust type.
///
/// This might return an error if the conversion is not possible,
/// This is the counterpart to the [`ToBlob`] trait.
///
/// See [TryFromValue](crate::value::TryFromValue) for the counterpart trait for values.
pub trait TryFromBlob<S: BlobSchema>: Sized {
    type Error: Error;
    fn try_from_blob(b: Blob<S>) -> Result<Self, Self::Error>;
}

impl<S: BlobSchema> TryFromBlob<S> for Blob<S> {
    type Error = Infallible;

    fn try_from_blob(b: Blob<S>) -> Result<Self, Self::Error> {
        Ok(b)
    }
}

impl<S: BlobSchema> ToBlob<S> for Blob<S> {
    fn to_blob(self) -> Blob<S> {
        self
    }
}
