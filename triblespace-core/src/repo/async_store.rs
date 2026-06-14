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
    BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult,
};

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
        // `Send` on the schema: the handle (which is phantom-typed by
        // `S`) is captured by the returned future, so it must be `Send`.
        // Schemas are unit markers, so this is free in practice.
        S: BlobEncoding + Send + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding;
}

/// Async counterpart of [`BlobStorePut`](crate::repo::BlobStorePut).
///
/// `item: T` is captured by the returned future, so it must be `Send`
/// (it crosses to the storage layer across the await) — the one place
/// the async bounds are stricter than the sync ones.
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
        T: IntoBlob<S> + Send,
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
        Sch: BlobEncoding + Send + 'static,
        T: TryFromBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        // No `.await`: the future captures only `&self` (Send iff
        // S: Sync) and the Copy handle, so it is Send regardless of
        // whether the output `T` is. `ready(..)` would instead require
        // the output Send — hence the zero-await block.
        async move { self.0.get::<T, Sch>(handle) }
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
        T: IntoBlob<Sch> + Send,
        Handle<Sch>: InlineEncoding,
    {
        async move { self.0.put::<Sch, T>(item) }
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
