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
/// Built-in blob schema types and their conversion implementations.
pub mod schemas;

use crate::metadata::MetaDescribe;
use crate::value::schemas::hash::Handle;
use crate::value::Value;
use crate::value::ValueSchema;

use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self};
use std::hash::Hash;
use std::marker::PhantomData;

/// Re-export of the blob cache wrapper.
pub use cache::BlobCache;
/// Re-export of the in-memory blob store.
pub use memoryblobstore::MemoryBlobStore;

/// Re-export of `anybytes::Bytes` for blob payloads.
pub use anybytes::Bytes;

/// A blob is an immutable sequence of bytes plus its eagerly-cached
/// Blake3 content-addressed handle.
///
/// The handle is computed once at construction (`Blob::new` / `Blob::transmute`)
/// and cached in the struct, so subsequent calls to `get_handle()` and
/// `MemoryBlobStore::insert(blob)` are O(1) — no recompute. Schemas
/// are still distinguished at the type level via the phantom marker,
/// and `transmute` carries the cached handle across schema casts (the
/// hash is over bytes, not over schema, so the handle bytes don't
/// change).
///
/// The layout was previously `#[repr(transparent)]` around `Bytes` for
/// the same-size guarantee. We've deliberately given that up: the
/// double-hash that came with computing the handle at every insert
/// site was a real cost the cache eliminates, and the only call that
/// relied on transparency (`as_transmute`'s `mem::transmute`) still
/// works because `Blob<S>` and `Blob<T>` have identical layouts for
/// any `S`/`T: BlobSchema` (the phantoms are zero-sized and the
/// handle is `[u8; 32] + PhantomData`).
pub struct Blob<S: BlobSchema> {
    /// The raw byte content of this blob.
    pub bytes: Bytes,
    /// Cached content-addressed handle. Computed eagerly at
    /// construction time; reused on every `get_handle` call and on
    /// `MemoryBlobStore::insert`.
    handle: Value<Handle<S>>,
    _schema: PhantomData<S>,
}

impl<S> Blob<S>
where
    S: BlobSchema,
    Handle<S>: ValueSchema,
{
    /// Creates a new blob from a sequence of bytes.
    ///
    /// **Hashes eagerly**: this call runs Blake3 over `bytes` once and
    /// caches the resulting handle. Subsequent `get_handle` /
    /// `MemoryBlobStore::insert` calls reuse the cached value at O(1).
    /// For most use cases this is what callers want — `Blob::new`
    /// almost always precedes an `insert` or a `get_handle`. If you
    /// have a blob path that's *never* hashed and the eager cost
    /// matters, reach for the raw `Bytes` instead.
    pub fn new(bytes: Bytes) -> Self {
        let digest = crate::value::schemas::hash::Blake3::digest(&bytes);
        Self {
            bytes,
            handle: Value::new(digest),
            _schema: PhantomData,
        }
    }

    /// Constructs a blob from bytes *and* a precomputed handle,
    /// skipping the hash step.
    ///
    /// Used by blob-store readers (`MemoryBlobStoreReader::get` and
    /// friends) and pile-format decoders that already know the
    /// handle the blob is stored under — they read the bytes out of
    /// their backing storage already keyed by hash, so recomputing
    /// it would be pure overhead.
    ///
    /// # Safety
    ///
    /// The caller asserts that `handle == Blake3(bytes)`. The cache
    /// is trusted on read paths; if these diverge,
    /// `MemoryBlobStore::insert(blob)` will store the bytes under
    /// `handle` (not the true Blake3 hash), and subsequent lookups
    /// will silently miss or return wrong data. Always pair this
    /// with a hash you got from a trusted source (the same store
    /// you're reading from, the pile header, a verified network
    /// fetch). For callers without that guarantee, use
    /// [`Blob::new`] which hashes from bytes.
    pub fn with_handle(bytes: Bytes, handle: Value<Handle<S>>) -> Self {
        Self {
            bytes,
            handle,
            _schema: PhantomData,
        }
    }

    /// Reinterprets the contained bytes as a blob of a different schema.
    ///
    /// This is a zero-copy transformation: bytes pass through and the
    /// cached handle is recast at the phantom level. It does **not**
    /// validate that the data actually conforms to the new schema.
    pub fn transmute<T: BlobSchema>(self) -> Blob<T>
    where
        Handle<T>: ValueSchema,
    {
        Blob {
            bytes: self.bytes,
            handle: self.handle.transmute(),
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

    /// Returns the cached Blake3 handle. O(1) — no rehash.
    pub fn get_handle(&self) -> Value<Handle<S>> {
        self.handle
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

impl<T> Clone for Blob<T>
where
    T: BlobSchema,
    Handle<T>: ValueSchema,
{
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            handle: self.handle,
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
/// This is similar to the [`ValueSchema`] trait in the [`value`](crate::value) module.
pub trait BlobSchema: MetaDescribe + Sized + 'static {
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
    /// Converts this value into a blob.
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
    /// The error type returned when the conversion fails.
    type Error: Error + Send + Sync + 'static;
    /// Attempts to convert a blob into this type.
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

/// A `Blob<T>` passed as a `Handle<T>`-typed field in `entity!{}`
/// auto-puts itself: the macro absorbs its bytes into the fragment's
/// local blob store, and the field value is the handle derived from
/// those same bytes. The blob store is content-addressed, so the
/// bytes round-trip cleanly even though the schema is erased at the
/// storage boundary.
impl<T> crate::value::IntoFieldValue<Handle<T>> for Blob<T>
where
    T: BlobSchema,
    Handle<T>: ValueSchema,
{
    fn into_field_value(self) -> (Value<Handle<T>>, Option<Bytes>) {
        // O(1) — handle was computed eagerly at Blob::new.
        (self.handle, Some(self.bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::schemas::UnknownBlob;
    use crate::value::schemas::hash::Blake3;

    #[test]
    fn new_computes_and_caches_handle() {
        let b: Blob<UnknownBlob> = Blob::new(Bytes::from(b"hello".to_vec()));
        let h1 = b.get_handle();
        let h2 = b.get_handle();
        // Same handle on repeat — cache is stable.
        assert_eq!(h1, h2);
        // And matches a fresh independent Blake3 of the bytes.
        let independent = Value::new(Blake3::digest(b"hello"));
        let h_typed: Value<Handle<UnknownBlob>> = independent;
        assert_eq!(h1, h_typed);
    }

    #[test]
    fn with_handle_trusts_the_provided_handle() {
        // Construct a blob with a *deliberately bogus* handle. The
        // cache returns it verbatim — proving we don't recompute from
        // bytes. This is the optimization read paths exploit (they
        // already know the handle, no point re-hashing).
        let bogus: Value<Handle<UnknownBlob>> = Value::new([0xAA; 32]);
        let b: Blob<UnknownBlob> = Blob::with_handle(
            Bytes::from(b"any bytes".to_vec()),
            bogus,
        );
        assert_eq!(b.get_handle(), bogus);
    }

    #[test]
    fn transmute_carries_cached_handle() {
        let b: Blob<UnknownBlob> = Blob::new(Bytes::from(b"shared".to_vec()));
        let h_before: Value<Handle<UnknownBlob>> = b.get_handle();
        // Schema cast — handle bytes stay identical, only the phantom
        // changes.
        let b2: Blob<crate::blob::schemas::longstring::LongString> =
            b.transmute::<crate::blob::schemas::longstring::LongString>();
        let h_after = b2.get_handle();
        assert_eq!(h_before.raw, h_after.raw);
    }
}
