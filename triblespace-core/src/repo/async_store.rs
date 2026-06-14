//! Async store traits — the honest contract for *remote* backends.
//!
//! The sync [`BlobStore`](crate::repo::BlobStore) family is the right
//! contract for *local* backends: `MemoryBlobStore` and a
//! `Pile`-over-mmap are genuinely synchronous, and a sync `get` that
//! returns a `Result` is the truth. But genuinely *remote* backends —
//! `ObjectStore` (cloud object storage) and a networked `Peer` — are
//! async at their core. Today they fake sync by owning a private tokio
//! `Runtime` and `block_on`-ing every call, which is both wasteful and
//! actively broken (`block_on` inside an existing runtime panics, so a
//! sync-faked remote store can't be used from async code at all).
//!
//! This module gives those backends an honest home: an async mirror of
//! the blob-store traits, written in the same explicit
//! `-> impl Future<…> + Send` (RPITIT) style as the network
//! [`Transport`](../../../triblespace_net/transport/trait.Transport.html)
//! trait, so the returned futures carry a `Send` bound.
//!
//! Two adapters bridge the worlds:
//! - [`SyncAsAsync`] lifts any sync store into the async traits via
//!   zero-await futures — so an async consumer can read a local store
//!   for free, with no runtime and no blocking (the futures resolve on
//!   first poll).
//! - the inverse (an async store behind a single `block_on` boundary)
//!   is `Blocking`, landing in a later increment so the scattered
//!   `block_on`s in `ObjectStore` collapse into one place.

use std::error::Error;
use std::fmt::Debug;
use std::future::Future;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::{BlobEncoding, IntoBlob, TryFromBlob};
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding};
use crate::repo::{
    BlobMetadata, BlobStore, BlobStoreForget, BlobStoreGet, BlobStoreList, BlobStoreMeta,
    BlobStorePut, PinStore, PushResult,
};
// Only used by the `object-store`-gated `Blocking` impls below.
#[cfg(feature = "object-store")]
use crate::repo::{BlobChildren, StorageClose};

/// Async counterpart of [`BlobStoreGet`](crate::repo::BlobStoreGet).
///
/// `get` returns a `Send` future so it can be driven on a multi-thread
/// runtime. The output `T` need not be `Send` — it is produced at
/// completion, not held across an await — so this mirrors the sync
/// signature's bounds exactly.
pub trait AsyncBlobStoreGet {
    /// Error type for get operations, parameterised by the
    /// deserialization error (mirrors the sync GAT).
    type GetError<E: Error + Send + Sync + 'static>: Error + Send + Sync + 'static;

    /// Retrieve a blob by handle, awaiting whatever I/O the backend
    /// needs (a cloud GET, a swarm fetch).
    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>> + Send
    where
        // Bounds mirror the sync `BlobStoreGet::get` exactly — notably
        // NO `S: Send`. The phantom-typed handle is `!Send` when the
        // schema is, so impls must extract the raw 32 bytes before any
        // await rather than capturing the typed handle (see
        // `SyncAsAsync`). This keeps the trait a drop-in mirror, which a
        // sync `Blocking` adapter relies on (its sync `get` can't add an
        // `S: Send` bound the sync trait doesn't have).
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding;
}

/// Async counterpart of [`BlobStorePut`](crate::repo::BlobStorePut).
///
/// Bounds mirror the sync `put` exactly (no `T: Send`). Impls must
/// serialise `item` to bytes *before* the first await and carry only
/// those `Send` bytes across it — never the phantom-typed value — so
/// the future is `Send` without constraining `T`. That keeps the trait
/// a drop-in mirror the sync `Blocking` adapter can lower through.
pub trait AsyncBlobStorePut {
    /// Error type for put operations.
    type PutError: Error + Send + Sync + 'static;

    /// Serialise `item`, store it (awaiting the backend write), and
    /// return its handle.
    fn put<S, T>(
        &mut self,
        item: T,
    ) -> impl Future<Output = Result<Inline<Handle<S>>, Self::PutError>> + Send
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding;
}

/// Async counterpart of [`BlobStoreList`](crate::repo::BlobStoreList).
///
/// Returns the listing eagerly as a `Vec` rather than a `Stream` — that
/// keeps the trait dependency-free (only `std::future`) and is fine for
/// blob enumeration, which is metadata-sized. A streaming variant can
/// be added later if a backend's listing is genuinely unbounded.
pub trait AsyncBlobStoreList {
    /// Error type for listing operations.
    type Err: Error + Debug + Send + Sync + 'static;

    /// List all blob handles in the store.
    fn blobs(
        &self,
    ) -> impl Future<Output = Vec<Result<Inline<Handle<UnknownBlob>>, Self::Err>>> + Send;
}

/// Async counterpart of [`BlobStore`](crate::repo::BlobStore): combined
/// read/write with a shareable reader snapshot.
pub trait AsyncBlobStore: AsyncBlobStorePut {
    /// A clonable async reader handle for concurrent blob lookups.
    /// Mirrors the sync `Reader` bound (so it can round-trip through a
    /// `Blocking` adapter into a full sync `BlobStore::Reader`).
    type Reader: AsyncBlobStoreGet
        + AsyncBlobStoreList
        + Clone
        + Send
        + Sync
        + PartialEq
        + Eq
        + 'static;
    /// Error type for creating a reader.
    type ReaderError: Error + Send + Sync + 'static;

    /// Create a shareable reader snapshot of the current store state.
    fn reader(
        &mut self,
    ) -> impl Future<Output = Result<Self::Reader, Self::ReaderError>> + Send;
}

/// Async counterpart of [`PinStore`](crate::repo::PinStore): named,
/// atomically-updatable handles to `SimpleArchive` blobs.
pub trait AsyncPinStore {
    /// Error type for listing pins.
    type PinsError: Error + Debug + Send + Sync + 'static;
    /// Error type for head lookups.
    type HeadError: Error + Debug + Send + Sync + 'static;
    /// Error type for CAS updates.
    type UpdateError: Error + Debug + Send + Sync + 'static;

    /// List every pin id (eagerly collected — see [`AsyncBlobStoreList`]
    /// for why `Vec` over `Stream`).
    fn pins(
        &mut self,
    ) -> impl Future<Output = Result<Vec<Result<Id, Self::PinsError>>, Self::PinsError>> + Send;

    /// Current head of a pin: `Some(head)`, `None` if tombstoned.
    fn head(
        &mut self,
        id: Id,
    ) -> impl Future<Output = Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError>> + Send;

    /// Compare-and-swap update of a pin's head.
    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> impl Future<Output = Result<PushResult, Self::UpdateError>> + Send;
}

/// Async counterpart of [`BlobStoreMeta`](crate::repo::BlobStoreMeta).
pub trait AsyncBlobStoreMeta {
    /// Error type for metadata calls.
    type MetaError: Error + Send + Sync + 'static;

    /// Metadata for the blob `handle`, or `None` if absent.
    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<Option<BlobMetadata>, Self::MetaError>> + Send
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;
}

/// Async counterpart of [`BlobStoreForget`](crate::repo::BlobStoreForget).
pub trait AsyncBlobStoreForget {
    /// Error type for forget operations.
    type ForgetError: Error + Send + Sync + 'static;

    /// Drop the materialised blob `handle` (monotonic, idempotent).
    fn forget<S>(
        &mut self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<(), Self::ForgetError>> + Send
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;
}

/// Lift a synchronous store into the async traits via zero-await
/// futures.
///
/// Local backends (`MemoryBlobStore`, `Pile`) are genuinely
/// synchronous; this wrapper lets an async consumer read them without
/// each backend reimplementing the async surface and without spinning
/// up a runtime. The futures contain no `.await`, so they resolve on
/// the first poll — there is no blocking, no executor required, just
/// the sync call wrapped in a future shell. It is the async-side
/// identity for things that were never really async.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncAsAsync<S>(pub S);

impl<S> SyncAsAsync<S> {
    /// Wrap a sync store.
    pub fn new(store: S) -> Self {
        Self(store)
    }

    /// Unwrap back to the sync store.
    pub fn into_inner(self) -> S {
        self.0
    }
}

impl<S> AsyncBlobStoreGet for SyncAsAsync<S>
where
    S: BlobStoreGet + Sync,
{
    type GetError<E: Error + Send + Sync + 'static> = S::GetError<E>;

    fn get<T, Sch>(
        &self,
        handle: Inline<Handle<Sch>>,
    ) -> impl Future<Output = Result<T, Self::GetError<<T as TryFromBlob<Sch>>::Error>>> + Send
    where
        Sch: BlobEncoding + 'static,
        T: TryFromBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // Extract the raw 32 bytes *before* the async block so the
        // future captures only `[u8; 32]` (Send) and `&self` (Send iff
        // S: Sync) — never the phantom-typed handle, which is `!Send`
        // when `Sch` is. The typed handle is rebuilt inside, used in the
        // same poll with no await in between, so it is never part of the
        // future's held state.
        let raw = handle.raw;
        async move { self.0.get::<T, Sch>(Inline::new(raw)) }
    }
}

impl<S> AsyncBlobStorePut for SyncAsAsync<S>
where
    S: BlobStorePut + Send,
{
    type PutError = S::PutError;

    fn put<Sch, T>(
        &mut self,
        item: T,
    ) -> impl Future<Output = Result<Inline<Handle<Sch>>, Self::PutError>> + Send
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // Serialise synchronously and capture only the `Send` bytes +
        // raw handle — never the phantom-typed item/blob/handle — so the
        // future is Send without bounding `T` (mirrors the `get` trick).
        let blob: crate::blob::Blob<Sch> = item.to_blob();
        let raw = blob.get_handle().raw;
        let bytes = blob.bytes;
        async move {
            self.0
                .put::<Sch, crate::blob::Blob<Sch>>(crate::blob::Blob::new(bytes))
                .map(|_| Inline::new(raw))
        }
    }
}

impl<S> AsyncBlobStoreList for SyncAsAsync<S>
where
    S: BlobStoreList + Sync,
{
    type Err = S::Err;

    fn blobs(
        &self,
    ) -> impl Future<Output = Vec<Result<Inline<Handle<UnknownBlob>>, Self::Err>>> + Send {
        // The borrowed iterator is created and drained inside the
        // future (no await), so only `&self` (Send iff S: Sync) is held.
        async move { self.0.blobs().collect() }
    }
}

impl<S> AsyncBlobStore for SyncAsAsync<S>
where
    S: BlobStore + Send + Sync,
    S::Reader: Sync,
{
    type Reader = SyncAsAsync<S::Reader>;
    type ReaderError = S::ReaderError;

    fn reader(
        &mut self,
    ) -> impl Future<Output = Result<Self::Reader, Self::ReaderError>> + Send {
        async move { self.0.reader().map(SyncAsAsync) }
    }
}

impl<S> AsyncPinStore for SyncAsAsync<S>
where
    S: PinStore + Send,
{
    type PinsError = S::PinsError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;

    fn pins(
        &mut self,
    ) -> impl Future<Output = Result<Vec<Result<Id, Self::PinsError>>, Self::PinsError>> + Send {
        async move { self.0.pins().map(|it| it.collect()) }
    }

    fn head(
        &mut self,
        id: Id,
    ) -> impl Future<Output = Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError>> + Send
    {
        async move { self.0.head(id) }
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> impl Future<Output = Result<PushResult, Self::UpdateError>> + Send {
        async move { self.0.update(id, old, new) }
    }
}

impl<S> AsyncBlobStoreMeta for SyncAsAsync<S>
where
    S: BlobStoreMeta + Sync,
{
    type MetaError = S::MetaError;

    fn metadata<Sch>(
        &self,
        handle: Inline<Handle<Sch>>,
    ) -> impl Future<Output = Result<Option<BlobMetadata>, Self::MetaError>> + Send
    where
        Sch: BlobEncoding + 'static,
        Handle<Sch>: InlineEncoding,
    {
        let raw = handle.raw;
        async move { self.0.metadata::<Sch>(Inline::new(raw)) }
    }
}

impl<S> AsyncBlobStoreForget for SyncAsAsync<S>
where
    S: BlobStoreForget + Send,
{
    type ForgetError = S::ForgetError;

    fn forget<Sch>(
        &mut self,
        handle: Inline<Handle<Sch>>,
    ) -> impl Future<Output = Result<(), Self::ForgetError>> + Send
    where
        Sch: BlobEncoding + 'static,
        Handle<Sch>: InlineEncoding,
    {
        let raw = handle.raw;
        async move { self.0.forget::<Sch>(Inline::new(raw)) }
    }
}

/// Drive an async store from synchronous code through a single
/// `block_on` boundary.
///
/// The inverse of [`SyncAsAsync`]: where that lifts a sync store into
/// async with zero-await futures, `Blocking` lowers an async store into
/// the sync traits by owning a tokio runtime and `block_on`-ing each
/// call. It exists so the scattered `block_on`s that backends like
/// `ObjectStore` carry internally collapse into *one* place — and so
/// genuinely-sync call sites (a CLI `main`) can still use an async
/// backend.
///
/// Caveat inherited from `block_on`: calling a `Blocking` method from
/// *within* an existing tokio runtime panics. It is an edge adapter for
/// sync boundaries, not something to thread through async code — async
/// code should depend on the async traits directly.
#[cfg(feature = "object-store")]
pub struct Blocking<A> {
    inner: A,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "object-store")]
impl<A: Clone> Clone for Blocking<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
        }
    }
}

#[cfg(feature = "object-store")]
impl<A: std::fmt::Debug> std::fmt::Debug for Blocking<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The runtime is a driver, not state — show only the inner store.
        f.debug_struct("Blocking").field("inner", &self.inner).finish()
    }
}

// Identity ignores the runtime: two blocking wrappers are equal iff
// their inner snapshots are. The runtime is a driver, not part of the
// store's value. (Required so `Blocking<Reader>` satisfies the sync
// `BlobStore::Reader: PartialEq + Eq` bound.)
#[cfg(feature = "object-store")]
impl<A: PartialEq> PartialEq for Blocking<A> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
#[cfg(feature = "object-store")]
impl<A: Eq> Eq for Blocking<A> {}

#[cfg(feature = "object-store")]
impl<A> Blocking<A> {
    /// Wrap an async store, owning a fresh current-thread runtime to
    /// drive it. Current-thread (with all drivers enabled) is enough
    /// for sequential `block_on` and far lighter than a multi-thread
    /// runtime per store.
    pub fn new(inner: A) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            inner,
            rt: std::sync::Arc::new(rt),
        })
    }

    /// Wrap an async store, sharing a caller-provided runtime (e.g. a
    /// multi-thread one the cloud SDK wants for its connection pool).
    pub fn with_runtime(inner: A, rt: std::sync::Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, rt }
    }

    /// Unwrap back to the async store.
    pub fn into_inner(self) -> A {
        self.inner
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStoreGet> BlobStoreGet for Blocking<A> {
    type GetError<E: Error + Send + Sync + 'static> = A::GetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.rt.block_on(self.inner.get::<T, S>(handle))
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStoreList> BlobStoreList for Blocking<A> {
    type Iter<'a>
        = std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, A::Err>>
    where
        A: 'a;
    type Err = A::Err;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        self.rt.block_on(self.inner.blobs()).into_iter()
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStorePut> BlobStorePut for Blocking<A> {
    type PutError = A::PutError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.rt.block_on(self.inner.put::<S, T>(item))
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStore> BlobStore for Blocking<A> {
    type Reader = Blocking<A::Reader>;
    type ReaderError = A::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let reader = self.rt.block_on(self.inner.reader())?;
        Ok(Blocking {
            inner: reader,
            rt: self.rt.clone(),
        })
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncPinStore> PinStore for Blocking<A> {
    type PinsError = A::PinsError;
    type HeadError = A::HeadError;
    type UpdateError = A::UpdateError;
    type ListIter<'a>
        = std::vec::IntoIter<Result<Id, A::PinsError>>
    where
        A: 'a;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        self.rt.block_on(self.inner.pins()).map(|v| v.into_iter())
    }

    fn head(
        &mut self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        self.rt.block_on(self.inner.head(id))
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        self.rt.block_on(self.inner.update(id, old, new))
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStoreMeta> BlobStoreMeta for Blocking<A> {
    type MetaError = A::MetaError;

    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<Option<BlobMetadata>, Self::MetaError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.rt.block_on(self.inner.metadata::<S>(handle))
    }
}

#[cfg(feature = "object-store")]
impl<A: AsyncBlobStoreForget> BlobStoreForget for Blocking<A> {
    type ForgetError = A::ForgetError;

    fn forget<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::ForgetError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.rt.block_on(self.inner.forget::<S>(handle))
    }
}

// The conservative reference scan rides the (sync) `BlobStoreGet` that
// Blocking already provides, so any Blocking reader gets `children` for
// free via the default scan-and-check.
#[cfg(feature = "object-store")]
impl<A: AsyncBlobStoreGet> BlobChildren for Blocking<A> {}

// Lifecycle teardown forwards to the inner store (and drops the
// runtime). `close` is not a storage op, so it stays synchronous.
#[cfg(feature = "object-store")]
impl<A: StorageClose> StorageClose for Blocking<A> {
    type Error = A::Error;

    fn close(self) -> Result<(), Self::Error> {
        self.inner.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::blob::Blob;
    use crate::blob::MemoryBlobStore;
    use crate::id::{ExclusiveId, Id};
    use crate::macros::entity;
    use crate::trible::TribleSet;
    use futures::executor::block_on;

    fn blob(tag: u8) -> Blob<SimpleArchive> {
        let e = Id::new([tag; 16]).unwrap();
        let ts: TribleSet = entity! {
            ExclusiveId::force_ref(&e) @
            crate::metadata::tag: Id::new([tag.wrapping_add(3).max(1); 16]).unwrap(),
        }
        .into();
        ts.to_blob()
    }

    #[test]
    fn sync_store_reads_and_writes_through_async_facade() {
        let mut store = SyncAsAsync::new(MemoryBlobStore::new());
        let b = blob(1);

        let handle = block_on(store.put::<SimpleArchive, _>(b.clone())).unwrap();
        let reader = block_on(store.reader()).unwrap();
        let got: Blob<SimpleArchive> = block_on(reader.get(handle)).unwrap();
        assert_eq!(got.bytes, b.bytes);
    }

    #[test]
    fn missing_blob_is_an_error_not_a_hang() {
        let mut store = SyncAsAsync::new(MemoryBlobStore::new());
        let reader = block_on(store.reader()).unwrap();
        let missing = blob(9).get_handle();
        let got = block_on(reader.get::<Blob<SimpleArchive>, SimpleArchive>(missing));
        assert!(got.is_err(), "absent blob resolves to Err, immediately");
    }

    #[test]
    fn async_list_through_facade() {
        let mut store = SyncAsAsync::new(MemoryBlobStore::new());
        let h1 = block_on(store.put::<SimpleArchive, _>(blob(1))).unwrap();
        let h2 = block_on(store.put::<SimpleArchive, _>(blob(2))).unwrap();
        let reader = block_on(store.reader()).unwrap();
        let listed: Vec<_> = block_on(reader.blobs())
            .into_iter()
            .filter_map(Result::ok)
            .map(|h| h.raw)
            .collect();
        assert!(listed.contains(&h1.raw) && listed.contains(&h2.raw));
    }

    #[test]
    fn async_pins_on_fresh_repo_are_empty() {
        use crate::repo::memoryrepo::MemoryRepo;
        let mut repo = SyncAsAsync::new(MemoryRepo::default());
        let pins = block_on(repo.pins()).unwrap();
        assert!(pins.is_empty(), "fresh repo has no pins");
        let head = block_on(repo.head(Id::new([7u8; 16]).unwrap())).unwrap();
        assert!(head.is_none(), "unknown pin has no head");
    }

    // Blocking and SyncAsAsync are inverses: a sync store wrapped up
    // into async and back down through Blocking behaves as a plain sync
    // store. This is the round-trip that proves Blocking yields a full,
    // working sync `BlobStore` surface over an async backend.
    #[cfg(feature = "object-store")]
    #[test]
    fn blocking_over_async_roundtrips_as_a_sync_store() {
        use crate::repo::{BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut};

        let mut store = Blocking::new(SyncAsAsync::new(MemoryBlobStore::new())).unwrap();
        let b = blob(5);
        // Pure sync calls — no `.await`, no visible runtime.
        let h = store.put::<SimpleArchive, _>(b.clone()).unwrap();
        let reader = store.reader().unwrap();
        let got: Blob<SimpleArchive> = reader.get(h).unwrap();
        assert_eq!(got.bytes, b.bytes);
        let listed: Vec<_> = reader.blobs().filter_map(Result::ok).map(|h| h.raw).collect();
        assert!(listed.contains(&h.raw));
    }

    // Statically assert the futures are `Send` — the whole point of the
    // RPITIT style. If the zero-await blocks ever captured something
    // non-Send, this would stop compiling.
    fn _assert_send<F: Send>(_: F) {}
    #[allow(dead_code)]
    fn _send_proof(store: &mut SyncAsAsync<MemoryBlobStore>) {
        _assert_send(store.put::<SimpleArchive, _>(blob(2)));
        _assert_send(store.reader());
    }
}
