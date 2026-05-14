use crate::blob::schemas::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::patch::{Entry, IdentitySchema, PATCH};
use crate::repo::BlobStore;
use crate::repo::BlobStoreGet;
use crate::repo::BlobStoreKeep;
use crate::repo::BlobStoreList;
use crate::repo::BlobStorePut;
use crate::value::schemas::hash::Handle;
use crate::value::Value;
use crate::value::VALUE_LEN;

use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self};
use std::iter::FromIterator;

use super::TryFromBlob;

/// In-memory blob storage keyed by content-hash handle.
///
/// Internally a [`PATCH`] mapping the 32-byte raw handle to a
/// [`Blob<UnknownBlob>`]. Writes go through `&mut self` (the
/// type system enforces single-writer); [`reader`] hands out
/// owned snapshots that are independent of the original
/// store. PATCH's structural sharing makes those snapshots
/// O(1) clones — the writer keeps mutating the canonical
/// PATCH, readers each hold a pinned Arc-clone.
///
/// [`reader`]: BlobStore::reader
pub struct MemoryBlobStore {
    blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
}

impl Debug for MemoryBlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryBlobStore")
    }
}

#[derive(Debug)]
/// Snapshot view into a [`MemoryBlobStore`]. Independent from
/// the source store — subsequent writes to the store are not
/// visible to a reader produced earlier; call [`reader`] again
/// to pick them up.
///
/// `Clone` is O(1) (PATCH structural sharing). The reader is
/// `Send + Sync` and freely composes through `find!` /
/// `pattern!` / `and!` / `or!`.
///
/// [`reader`]: BlobStore::reader
pub struct MemoryBlobStoreReader {
    blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
}

impl Clone for MemoryBlobStoreReader {
    fn clone(&self) -> Self {
        MemoryBlobStoreReader {
            blobs: self.blobs.clone(),
        }
    }
}

impl PartialEq for MemoryBlobStoreReader {
    fn eq(&self, other: &Self) -> bool {
        self.blobs == other.blobs
    }
}

impl Eq for MemoryBlobStoreReader {}

impl MemoryBlobStoreReader {
    fn new(blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>) -> Self {
        MemoryBlobStoreReader { blobs }
    }

    /// Number of blobs in this snapshot.
    pub fn len(&self) -> usize {
        self.blobs.len() as usize
    }

    /// True iff the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterator over `(handle, blob)` pairs in this snapshot.
    /// Iteration order is unspecified.
    pub fn iter(&self) -> MemoryBlobStoreIter {
        let for_iter = self.blobs.clone();
        let lookup = for_iter.clone();
        MemoryBlobStoreIter {
            keys: for_iter.into_iter(),
            lookup,
        }
    }
}

impl Clone for MemoryBlobStore {
    fn clone(&self) -> Self {
        MemoryBlobStore {
            blobs: self.blobs.clone(),
        }
    }
}

impl PartialEq for MemoryBlobStore {
    fn eq(&self, other: &Self) -> bool {
        self.blobs == other.blobs
    }
}

impl Eq for MemoryBlobStore {}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBlobStore {
    /// Creates a new [`MemoryBlobStore`] with no blobs.
    pub fn new() -> MemoryBlobStore {
        MemoryBlobStore {
            blobs: PATCH::new(),
        }
    }

    /// Inserts `blob` into the store and returns its handle.
    ///
    /// O(1) over the handle computation — the handle was hashed once
    /// at `Blob::new` and cached in the blob; this method reuses it.
    /// Idempotent at the PATCH level: re-inserting the same handle is
    /// a no-op, which matches the content-addressed semantics
    /// (same handle ⇒ same bytes).
    pub fn insert<S>(&mut self, blob: Blob<S>) -> Value<Handle<S>>
    where
        S: BlobSchema,
        Handle<S>: crate::value::ValueSchema,
    {
        let handle: Value<Handle<S>> = blob.get_handle();
        let unknown_handle: Value<Handle<UnknownBlob>> = handle.transmute();
        let blob: Blob<UnknownBlob> = blob.transmute::<UnknownBlob>();
        let entry = Entry::with_value(&unknown_handle.raw, blob);
        self.blobs.insert(&entry);
        handle
    }

    /// Number of distinct blobs in the store.
    pub fn len(&self) -> usize {
        self.blobs.len() as usize
    }

    /// True iff the store contains no blobs.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert raw bytes keyed only by their content hash, without
    /// committing to a typed [`BlobSchema`].
    ///
    /// Used by the `entity!{}` macro to absorb the bytes returned
    /// from [`IntoFieldValue::into_field_value`](crate::value::IntoFieldValue)
    /// — the schema is already captured on the value side (the
    /// returned `Value<Handle<T>>` carries the type), so the
    /// storage path only cares about the bytes.
    pub fn insert_bytes(&mut self, bytes: anybytes::Bytes) {
        let blob: Blob<UnknownBlob> = Blob::new(bytes);
        let _ = self.insert(blob);
    }

    /// Structurally merge `other` into this store, consuming `other`.
    ///
    /// Handle bytes match by content-addressing — duplicate keys
    /// collapse via PATCH's union semantics (idempotent). Faster
    /// than per-blob `BlobStorePut::put`: PATCH's `union` is a
    /// structural merge — cost is bounded by the size of the
    /// non-overlapping subtrees, not the total blob count.
    pub fn union(&mut self, other: Self) {
        self.blobs.union(other.blobs);
    }

    /// Drops any blobs that are not referenced by one of the provided tribles.
    pub fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Value<Handle<UnknownBlob>>>,
    {
        let mut surviving = PATCH::new();
        for handle in handles {
            if let Some(blob) = self.blobs.get(&handle.raw) {
                let entry = Entry::with_value(&handle.raw, blob.clone());
                surviving.insert(&entry);
            }
        }
        self.blobs = surviving;
    }
}

impl BlobStoreKeep for MemoryBlobStore {
    fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Value<Handle<UnknownBlob>>>,
    {
        MemoryBlobStore::keep(self, handles);
    }
}

impl FromIterator<(Value<Handle<UnknownBlob>>, Blob<UnknownBlob>)> for MemoryBlobStore {
    fn from_iter<I: IntoIterator<Item = (Value<Handle<UnknownBlob>>, Blob<UnknownBlob>)>>(
        iter: I,
    ) -> Self {
        let mut store = MemoryBlobStore::new();
        for (handle, blob) in iter {
            let entry = Entry::with_value(&handle.raw, blob);
            store.blobs.insert(&entry);
        }
        store
    }
}

impl IntoIterator for MemoryBlobStoreReader {
    type Item = (Value<Handle<UnknownBlob>>, Blob<UnknownBlob>);
    type IntoIter = MemoryBlobStoreIter;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub enum MemoryStoreGetError<E: Error> {
    /// This error occurs when a blob is requested that does not exist in the store.
    NotFound(),
    /// This error occurs when a blob is requested that exists, but cannot be converted to the requested type.
    ConversionFailed(E),
}

impl<E: Error> fmt::Display for MemoryStoreGetError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryStoreGetError::NotFound() => write!(f, "Blob not found in memory store"),
            MemoryStoreGetError::ConversionFailed(e) => write!(f, "Blob conversion failed: {e}"),
        }
    }
}

impl<E: Error> Error for MemoryStoreGetError<E> {}

/// Iterator returned by [`MemoryBlobStoreReader::iter`].
///
/// Yields `(Handle, Blob)` pairs. Owned snapshot via PATCH
/// clones — does not borrow from the source reader.
pub struct MemoryBlobStoreIter {
    keys: crate::patch::PATCHIntoIterator<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
    lookup: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
}

impl Debug for MemoryBlobStoreIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBlobStoreIter").finish()
    }
}

impl Iterator for MemoryBlobStoreIter {
    type Item = (Value<Handle<UnknownBlob>>, Blob<UnknownBlob>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.next()?;
        let handle: Value<Handle<UnknownBlob>> = Value::new(key);
        let blob = self
            .lookup
            .get(&key)
            .cloned()
            .expect("key from PATCH iterator must resolve in the same snapshot");
        Some((handle, blob))
    }
}

/// Adapter over [`MemoryBlobStoreIter`] that yields only blob handles.
pub struct MemoryBlobStoreListIter {
    inner: MemoryBlobStoreIter,
}

impl Iterator for MemoryBlobStoreListIter {
    type Item = Result<Value<Handle<UnknownBlob>>, Infallible>;

    fn next(&mut self) -> Option<Self::Item> {
        let (handle, _) = self.inner.next()?;
        Some(Ok(handle))
    }
}

impl BlobStoreList for MemoryBlobStoreReader {
    type Iter<'a> = MemoryBlobStoreListIter;
    type Err = Infallible;

    fn blobs(&self) -> Self::Iter<'static> {
        MemoryBlobStoreListIter { inner: self.iter() }
    }
}

impl BlobStoreGet for MemoryBlobStoreReader {
    type GetError<E: Error + Send + Sync + 'static> = MemoryStoreGetError<E>;

    fn get<T, S>(
        &self,
        handle: Value<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema,
        T: TryFromBlob<S>,
    {
        let handle: Value<Handle<UnknownBlob>> = handle.transmute();
        let Some(blob) = self.blobs.get(&handle.raw) else {
            return Err(MemoryStoreGetError::NotFound());
        };
        let blob: Blob<S> = blob.clone().transmute();
        match blob.try_from_blob() {
            Ok(value) => Ok(value),
            Err(e) => Err(MemoryStoreGetError::ConversionFailed(e)),
        }
    }
}

impl crate::repo::BlobChildren for MemoryBlobStoreReader {}

impl BlobStorePut for MemoryBlobStore {
    type PutError = Infallible;

    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<S>>, Self::PutError>
    where
        S: BlobSchema,
        T: ToBlob<S>,
    {
        let blob = item.to_blob();
        let handle = blob.get_handle();
        self.insert(blob);
        Ok(handle)
    }
}

impl BlobStore for MemoryBlobStore {
    type Reader = MemoryBlobStoreReader;
    type ReaderError = Infallible;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        Ok(MemoryBlobStoreReader::new(self.blobs.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    use super::*;
    use anybytes::Bytes;
    use fake::faker::name::raw::Name;
    use fake::locales::EN;
    use fake::Fake;

    use blobschemas::LongString;
    use valueschemas::Handle;

    attributes! {
        "5AD0FAFB1FECBC197A385EC20166899E" as description: Handle<LongString>;
    }

    #[test]
    fn keep() {
        use crate::repo::potential_handles;
        use crate::trible::TribleSet;

        let mut kb = TribleSet::new();
        let mut blobs = MemoryBlobStore::new();
        for _i in 0..200 {
            kb += entity! {
               description: blobs.put(Bytes::from_source(Name(EN).fake::<String>()).view().unwrap()).unwrap()
            };
        }
        blobs.keep(potential_handles(&kb));
    }

    /// `MemoryBlobStoreReader` must be `Send + Sync` so it composes
    /// through the parallel-iter ready `and!` / `or!` macros.
    #[test]
    fn reader_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemoryBlobStoreReader>();
    }

    /// `reader()` returns an independent snapshot — writes after
    /// the reader is produced are not visible to that reader.
    #[test]
    fn reader_is_a_pinned_snapshot() {
        let mut store = MemoryBlobStore::new();
        let blob_a: Value<Handle<LongString>> =
            store.put(Bytes::from_source("hello".to_string()).view().unwrap()).unwrap();
        let snapshot = store.reader().unwrap();
        assert_eq!(snapshot.len(), 1);

        let _blob_b: Value<Handle<LongString>> =
            store.put(Bytes::from_source("world".to_string()).view().unwrap()).unwrap();
        // The snapshot still has only the original blob.
        assert_eq!(snapshot.len(), 1);
        use anybytes::View;
        let recovered: View<str> =
            snapshot.get::<View<str>, LongString>(blob_a).unwrap();
        assert_eq!(&*recovered, "hello");

        // A fresh reader sees both.
        let fresh = store.reader().unwrap();
        assert_eq!(fresh.len(), 2);
    }

    /// `union` structurally merges two stores; handles round-trip.
    #[test]
    fn union_merges_and_preserves_handles() {
        let mut a = MemoryBlobStore::new();
        let h_hello: Value<Handle<LongString>> = a
            .put(Bytes::from_source("hello".to_string()).view().unwrap())
            .unwrap();
        let mut b = MemoryBlobStore::new();
        let h_world: Value<Handle<LongString>> = b
            .put(Bytes::from_source("world".to_string()).view().unwrap())
            .unwrap();
        // Idempotent overlap: putting "hello" in b too — union should
        // collapse the duplicate, not double-count.
        let _h_hello_b: Value<Handle<LongString>> = b
            .put(Bytes::from_source("hello".to_string()).view().unwrap())
            .unwrap();

        a.union(b);
        assert_eq!(a.reader().unwrap().len(), 2, "duplicates collapse via union");

        use anybytes::View;
        let recovered_hello: View<str> = a
            .reader()
            .unwrap()
            .get::<View<str>, LongString>(h_hello)
            .unwrap();
        assert_eq!(&*recovered_hello, "hello");
        let recovered_world: View<str> = a
            .reader()
            .unwrap()
            .get::<View<str>, LongString>(h_world)
            .unwrap();
        assert_eq!(&*recovered_world, "world");
    }
}
