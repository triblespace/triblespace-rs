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
use crate::value::schemas::hash::HashProtocol;
use crate::value::Value;
use crate::value::VALUE_LEN;

use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self};
use std::iter::FromIterator;
use std::marker::PhantomData;

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
pub struct MemoryBlobStore<H: HashProtocol> {
    blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
    _marker: PhantomData<H>,
}

impl<H: HashProtocol> Debug for MemoryBlobStore<H> {
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
pub struct MemoryBlobStoreReader<H: HashProtocol> {
    blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
    _marker: PhantomData<H>,
}

impl<H: HashProtocol> Clone for MemoryBlobStoreReader<H> {
    fn clone(&self) -> Self {
        MemoryBlobStoreReader {
            blobs: self.blobs.clone(),
            _marker: PhantomData,
        }
    }
}

impl<H: HashProtocol> PartialEq for MemoryBlobStoreReader<H> {
    fn eq(&self, other: &Self) -> bool {
        // PATCH equality is by key set (values aren't part of equality
        // — see the patch module docs). Good enough for our blob-store
        // sense of equality, since values are content-addressed by key.
        self.blobs == other.blobs
    }
}

impl<H: HashProtocol> Eq for MemoryBlobStoreReader<H> {}

impl<H: HashProtocol> MemoryBlobStoreReader<H> {
    fn new(blobs: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>) -> Self {
        MemoryBlobStoreReader {
            blobs,
            _marker: PhantomData,
        }
    }

    /// Number of blobs in this snapshot.
    pub fn len(&self) -> usize {
        self.blobs.len() as usize
    }

    /// Iterator over `(handle, blob)` pairs in this snapshot.
    /// Iteration order is unspecified.
    pub fn iter(&self) -> MemoryBlobStoreIter<H> {
        // Two clones: one drives iteration (yields keys), the
        // other retains the values for lookup. Both are O(1)
        // PATCH clones; the iterator owns its data so it can
        // outlive a borrowing reference to the reader.
        let for_iter = self.blobs.clone();
        let lookup = for_iter.clone();
        MemoryBlobStoreIter {
            keys: for_iter.into_iter(),
            lookup,
            _marker: PhantomData,
        }
    }
}

impl<H: HashProtocol> Clone for MemoryBlobStore<H> {
    fn clone(&self) -> Self {
        MemoryBlobStore {
            blobs: self.blobs.clone(),
            _marker: PhantomData,
        }
    }
}

impl<H: HashProtocol> PartialEq for MemoryBlobStore<H> {
    fn eq(&self, other: &Self) -> bool {
        // PATCH equality is by key set (values aren't part of equality
        // — see the patch module docs). Good enough for blob-store
        // equality, since values are content-addressed by key.
        self.blobs == other.blobs
    }
}

impl<H: HashProtocol> Eq for MemoryBlobStore<H> {}

impl<H: HashProtocol> Default for MemoryBlobStore<H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: HashProtocol> MemoryBlobStore<H> {
    /// Creates a new [`MemoryBlobStore`] with no blobs.
    pub fn new() -> MemoryBlobStore<H> {
        MemoryBlobStore {
            blobs: PATCH::new(),
            _marker: PhantomData,
        }
    }

    /// Inserts `blob` into the store and returns the newly computed handle.
    ///
    /// Idempotent: PATCH's `insert` is a no-op when the key is already
    /// present, which matches blob-store semantics (handles are
    /// content-addressed, so a duplicate-key insert is also a duplicate-value
    /// insert).
    pub fn insert<S>(&mut self, blob: Blob<S>) -> Value<Handle<H, S>>
    where
        S: BlobSchema,
    {
        let handle: Value<Handle<H, S>> = blob.get_handle();
        let unknown_handle: Value<Handle<H, UnknownBlob>> = handle.transmute();
        let blob: Blob<UnknownBlob> = blob.transmute();
        let entry = Entry::with_value(&unknown_handle.raw, blob);
        self.blobs.insert(&entry);
        handle
    }

    // Note that keep is conservative and keeps every blob for which there exists
    // a corresponding trible value, irrespective of that tribles attribute type.
    // This could theoretically allow an attacker to DOS blob garbage collection
    // by introducting values that look like existing hashes, but are actually of
    // a different type. But this is under the assumption that an attacker is only
    // allowed to write non-handle typed triples, otherwise they might as well
    // introduce blobs directly.
    /// Structurally merge `other` into this store, consuming `other`.
    ///
    /// Both stores share the same `H` hash protocol so handle bytes
    /// match by content-addressing — duplicate keys collapse via
    /// PATCH's union semantics (idempotent). Used by callers that
    /// produced a scratch store (e.g. a `Describe::blobs()` impl
    /// returning the schema's description blobs) and want to fold
    /// it into a longer-lived store like a workspace's local blobs.
    ///
    /// Faster than per-blob `BlobStorePut::put`: PATCH's `union`
    /// is a structural merge — cost is bounded by the size of the
    /// non-overlapping subtrees, not the total blob count.
    pub fn union(&mut self, other: Self) {
        self.blobs.union(other.blobs);
    }

    /// Drops any blobs that are not referenced by one of the provided tribles.
    pub fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Value<Handle<H, UnknownBlob>>>,
    {
        // PATCH has no `retain`, so build a fresh PATCH containing only
        // the surviving entries. Keep is rare (consolidation / explicit
        // GC); the O(n) rebuild cost is fine.
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

impl<H: HashProtocol> BlobStoreKeep<H> for MemoryBlobStore<H> {
    fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Value<Handle<H, UnknownBlob>>>,
    {
        MemoryBlobStore::keep(self, handles);
    }
}

impl<H> FromIterator<(Value<Handle<H, UnknownBlob>>, Blob<UnknownBlob>)> for MemoryBlobStore<H>
where
    H: HashProtocol,
{
    fn from_iter<I: IntoIterator<Item = (Value<Handle<H, UnknownBlob>>, Blob<UnknownBlob>)>>(
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

impl<H> IntoIterator for MemoryBlobStoreReader<H>
where
    H: HashProtocol,
{
    type Item = (Value<Handle<H, UnknownBlob>>, Blob<UnknownBlob>);
    type IntoIter = MemoryBlobStoreIter<H>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub enum MemoryStoreGetError<E: Error> {
    /// This error occurs when a blob is requested that does not exist in the store.
    /// It is used to indicate that the requested blob could not be found.
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
pub struct MemoryBlobStoreIter<H>
where
    H: HashProtocol,
{
    keys: crate::patch::PATCHIntoIterator<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
    lookup: PATCH<VALUE_LEN, IdentitySchema, Blob<UnknownBlob>>,
    _marker: PhantomData<H>,
}

impl<H> Debug for MemoryBlobStoreIter<H>
where
    H: HashProtocol,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBlobStoreIter").finish()
    }
}

impl<H> Iterator for MemoryBlobStoreIter<H>
where
    H: HashProtocol,
{
    type Item = (Value<Handle<H, UnknownBlob>>, Blob<UnknownBlob>);

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.keys.next()?;
        let handle: Value<Handle<H, UnknownBlob>> = Value::new(key);
        let blob = self
            .lookup
            .get(&key)
            .cloned()
            .expect("key from PATCH iterator must resolve in the same snapshot");
        Some((handle, blob))
    }
}

/// Adapter over [`MemoryBlobStoreIter`] that yields only blob handles.
pub struct MemoryBlobStoreListIter<H>
where
    H: HashProtocol,
{
    inner: MemoryBlobStoreIter<H>,
}

impl<H> Iterator for MemoryBlobStoreListIter<H>
where
    H: HashProtocol,
{
    type Item = Result<Value<Handle<H, UnknownBlob>>, Infallible>;

    fn next(&mut self) -> Option<Self::Item> {
        let (handle, _) = self.inner.next()?;
        Some(Ok(handle))
    }
}

impl<H> BlobStoreList<H> for MemoryBlobStoreReader<H>
where
    H: HashProtocol,
{
    type Iter<'a> = MemoryBlobStoreListIter<H>;
    type Err = Infallible;

    fn blobs(&self) -> Self::Iter<'static> {
        MemoryBlobStoreListIter { inner: self.iter() }
    }
}

impl<H> BlobStoreGet<H> for MemoryBlobStoreReader<H>
where
    H: HashProtocol,
{
    type GetError<E: Error + Send + Sync + 'static> = MemoryStoreGetError<E>;

    fn get<T, S>(
        &self,
        handle: Value<Handle<H, S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema,
        T: TryFromBlob<S>,
    {
        let handle: Value<Handle<H, UnknownBlob>> = handle.transmute();
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

impl<H: HashProtocol> crate::repo::BlobChildren<H> for MemoryBlobStoreReader<H> {}

impl<H> BlobStorePut<H> for MemoryBlobStore<H>
where
    H: HashProtocol,
{
    type PutError = Infallible;

    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<H, S>>, Self::PutError>
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

impl<H: HashProtocol> BlobStore<H> for MemoryBlobStore<H> {
    type Reader = MemoryBlobStoreReader<H>;
    type ReaderError = Infallible;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        // O(1) PATCH clone — structural sharing means readers
        // are independent snapshots without copying the data.
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
    use valueschemas::Blake3;
    use valueschemas::Handle;

    attributes! {
        "5AD0FAFB1FECBC197A385EC20166899E" as description: Handle<Blake3, LongString>;
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
        blobs.keep(potential_handles::<Blake3>(&kb));
    }

    /// `MemoryBlobStoreReader` must be `Send + Sync` so it composes
    /// through the parallel-iter ready `and!` / `or!` macros.
    #[test]
    fn reader_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemoryBlobStoreReader<Blake3>>();
    }

    /// `reader()` returns an independent snapshot — writes after
    /// the reader is produced are not visible to that reader.
    /// Same shape as `PileReader`.
    #[test]
    fn reader_is_a_pinned_snapshot() {
        let mut store = MemoryBlobStore::<Blake3>::new();
        let blob_a: Value<Handle<Blake3, LongString>> =
            store.put(Bytes::from_source("hello".to_string()).view().unwrap()).unwrap();
        let snapshot = store.reader().unwrap();
        assert_eq!(snapshot.len(), 1);

        let _blob_b: Value<Handle<Blake3, LongString>> =
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
        let mut a = MemoryBlobStore::<Blake3>::new();
        let h_hello: Value<Handle<Blake3, LongString>> = a
            .put(Bytes::from_source("hello".to_string()).view().unwrap())
            .unwrap();
        let mut b = MemoryBlobStore::<Blake3>::new();
        let h_world: Value<Handle<Blake3, LongString>> = b
            .put(Bytes::from_source("world".to_string()).view().unwrap())
            .unwrap();
        // Idempotent overlap: putting "hello" in b too — union should
        // collapse the duplicate, not double-count.
        let _h_hello_b: Value<Handle<Blake3, LongString>> = b
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
