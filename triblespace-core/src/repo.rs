#![allow(clippy::type_complexity)]
//! Content-addressed commit storage with grow-only signed branch assertions.
//!
//! Blobs are immutable and addressed by their hashes. A branch is not a mutable
//! `(id -> head)` cell: its replicated state is the set of signed assertions
//! that exact identity has made. [`branch_frontier::resolve_branch`] removes
//! only definitely dominated tips under commit ancestry. A singleton frontier
//! is its own head; a complete divergent frontier has one deterministic flat
//! authorless merge as its derived read view.
//!
//! This separation removes compare-and-swap from collaboration. Two writers
//! may publish while partitioned; both assertions survive, and resolution
//! reconciles them when their state is unioned. The signed assertion set—not a
//! synthetic merge—is the authority-bearing replicated state.
//!
//! [`Repository`] is deliberately an own-key local-authoring boundary. Its
//! [`BranchIdentity`] is the exact `(author key, name handle)` descriptor.
//! Foreign assertion ingest needs authorization and overload policy and is a
//! separate capability; local resolve, pull, and push reject foreign keys
//! before touching storage.
//!
//! ## Basic usage
//!
//! ```rust,ignore
//! use ed25519_dalek::SigningKey;
//! use rand::rngs::OsRng;
//! use triblespace::prelude::*;
//! use triblespace::repo::{memoryrepo::MemoryRepo, Repository};
//!
//! let storage = MemoryRepo::default();
//! let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
//! let mut workspace = repo.create_workspace("main").expect("open blob snapshot");
//! let identity = *workspace.identity();
//! workspace.commit(entity! { literature::title: "Dune" }, "initial commit");
//! repo.push(&mut workspace).expect("publish assertion");
//!
//! let mut current = repo.pull(identity).expect("complete branch frontier");
//! let checkout = current.checkout(..).expect("read history");
//! ```
//!
//! [`Repository::create_workspace`] writes no branch state. An empty branch is
//! unrepresentable; its first changed [`Repository::push`] uploads and flushes
//! the staged blobs, validates the proposed canonical commit metadata, and
//! durably appends exactly one signed assertion. [`Repository::pull`] yields a
//! writable workspace only for [`BranchResolution::Complete`]; `Absent`,
//! `TipPending`, and `Partial` remain explicit states.
//!
/// Branch metadata construction and signature verification.
pub mod async_store;

pub mod branch;
/// Immutable, signed branch assertions and their grow-only snapshots.
pub mod branch_assertion;
/// Partial-ancestry resolution of branch assertion frontiers.
pub mod branch_frontier;
/// Capability-based authorization for triblespace networks.
pub mod capability;
/// Commit metadata construction and signature verification.
pub mod commit;
/// Storage adapter that delegates blobs and signed assertions to separate backends.
pub mod hybridstore;
/// Range-native derived-index manifests and typed artifacts.
pub mod index_home;
pub mod index_range;
/// No-network lazy reader: local get, durable want on miss ([`lazy::Lazy`]).
pub mod lazy;
/// Fully in-memory repository implementation for tests and ephemeral use.
pub mod memoryrepo;
#[cfg(feature = "object-store")]
/// Blob and replica-local-pin backend for `object_store`-compatible remotes.
pub mod objectstore;
/// Local file-based pile storage backend.
pub mod pile;
/// Generational collection of piles for lazy-retention blob storage.
pub mod yard;

/// Trait for storage backends that require explicit close/cleanup.
///
/// Not all storage backends need to implement this; implementations that have
/// nothing to do on close may return Ok(()) or use `Infallible` as the error
/// type.
pub trait StorageClose {
    /// Error type returned by `close`.
    type Error: std::error::Error;

    /// Consume the storage and perform any necessary cleanup.
    fn close(self) -> Result<(), Self::Error>;
}

/// Trait for storage backends that can make pending writes crash-durable.
///
/// Mirrors [`StorageClose`]: backends with buffered/unsynced state
/// ([`pile::Pile`]'s appended records are not durable until
/// [`pile::Pile::flush`]) expose it here so generic code can demand
/// durability at a specific point — most importantly when recording a
/// weak-pin **want** whose writer may exit immediately afterwards (a
/// faculty process recording a demand for a sync daemon to service).
/// Backends with nothing to sync (in-memory stores) return `Ok(())` with
/// `Infallible` as the error type.
pub trait StorageFlush {
    /// Error type returned by `flush`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist all pending writes and markers durably.
    fn flush(&mut self) -> Result<(), Self::Error>;
}

// Convenience impl for repositories whose storage supports explicit close.
impl<Storage> Repository<Storage>
where
    Storage: BlobStore + StorageClose,
{
    /// Close the repository's underlying storage if it supports explicit
    /// close operations.
    ///
    /// This method is only available when the storage type implements
    /// [`StorageClose`]. It consumes the repository and delegates to the
    /// storage's `close` implementation, returning any error produced.
    pub fn close(self) -> Result<(), <Storage as StorageClose>::Error> {
        self.storage.close()
    }
}

use crate::macros::pattern;
use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self};

use hifitime::Epoch;
use itertools::Itertools;

use crate::blob::encodings::simplearchive::UnarchiveError;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::blob::MemoryBlobStore;
use crate::blob::TryFromBlob;
use crate::find;
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::INLINE_LEN;
use crate::patch::Entry;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::prelude::inlineencodings::GenId;
use crate::trible::TribleSet;
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::repo::branch_assertion::{
    AssertionId, BranchAssertion, BranchAssertionStore, BranchId, BranchIdentity,
};
use crate::repo::branch_frontier::{
    BranchResolution, PartialCommitDag, PartialFrontier, ResolvedHead, TipPendingFrontier,
};

use crate::blob::encodings::longstring::LongString;
use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::inline::encodings::ed25519 as ed;
use crate::inline::encodings::shortstring::ShortString;
use crate::prelude::*;

attributes! {
    /// The actual data of the commit.
    "4DD4DDD05CC31734B03ABB4E43188B1F" as pub content: Handle<SimpleArchive>;
    /// Metadata describing the commit content.
    "88B59BD497540AC5AECDB7518E737C87" as pub metadata: Handle<SimpleArchive>;
    /// A commit that this commit is based on.
    "317044B612C690000D798CA660ECFD2A" as pub parent: Handle<SimpleArchive>;
    /// A (potentially long) message describing the commit.
    "B59D147839100B6ED4B165DF76EDF3BB" as pub message: Handle<LongString>;
    /// A short message describing the commit.
    "12290C0BE0E9207E324F24DDE0D89300" as pub short_message: ShortString;
    /// The hash of the first commit in the commit chain of the branch.
    "272FBC56108F336C4D2E17289468C35F" as pub head: Handle<SimpleArchive>;
    /// An id used to track the branch.
    "8694CC73AF96A5E1C7635C677D1B928A" as pub branch: GenId;
    /// The author of the signature identified by their ed25519 public key.
    "ADB4FFAD247C886848161297EFF5A05B" as pub signed_by: ed::ED25519PublicKey;
    /// The `r` part of a ed25519 signature.
    "9DF34F84959928F93A3C40AEB6E9E499" as pub signature_r: ed::ED25519RComponent;
    /// The `s` part of a ed25519 signature.
    "1ACE03BF70242B289FDF00E4327C3BC6" as pub signature_s: ed::ED25519SComponent;
}

/// The `ListBlobs` trait is used to list all blobs in a repository.
pub trait BlobStoreList {
    /// Iterator over blob handles in the store.
    type Iter<'a>: Iterator<Item = Result<Inline<Handle<UnknownBlob>>, Self::Err>>
    where
        Self: 'a;
    /// Error type for listing operations.
    type Err: Error + Debug + Send + Sync + 'static;

    /// Lists all blobs in the repository.
    fn blobs<'a>(&'a self) -> Self::Iter<'a>;

    /// Lists blobs in `self` that are not in `old`.
    ///
    /// Backends with true snapshot semantics (e.g. [`Pile`],
    /// where each [`Reader`](BlobStore::Reader) holds a frozen clone of the
    /// in-memory blob index) compute the difference cheaply via the index's
    /// own set-difference operation. Backends without snapshot semantics
    /// (e.g. an object store, where the Reader is just a handle to the live
    /// remote) fall back to the default implementation, which lists all
    /// current blobs — over-eager but always correct.
    ///
    /// Use this for "what blobs are new since I last looked" patterns
    /// (e.g. announcing newly-imported blobs to a DHT) where holding the
    /// previous Reader as a baseline gives you the delta.
    fn blobs_diff<'a>(&'a self, _old: &Self) -> Self::Iter<'a> {
        self.blobs()
    }
}

/// Metadata about a blob in a repository.
#[derive(Debug, Clone)]
pub struct BlobMetadata {
    /// Timestamp in milliseconds since UNIX epoch when the blob was created/stored.
    pub timestamp: u64,
    /// Length of the blob in bytes.
    pub length: u64,
}

/// Trait exposing metadata lookup for blobs available in a repository reader.
pub trait BlobStoreMeta {
    /// Error type returned by metadata calls.
    type MetaError: std::error::Error + Send + Sync + 'static;

    /// Returns metadata for the blob identified by `handle`, or `None` if
    /// the blob is not present.
    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<Option<BlobMetadata>, Self::MetaError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;
}

/// Trait exposing a monotonic "forget" operation.
///
/// Forget is idempotent and monotonic: it removes materialization from a
/// particular repository but does not semantically delete derived facts.
pub trait BlobStoreForget {
    /// Error type for forget operations.
    type ForgetError: std::error::Error + Send + Sync + 'static;

    /// Removes the materialized blob identified by `handle` from this store.
    fn forget<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::ForgetError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;
}

/// The `GetBlob` trait is used to retrieve blobs from a repository.
pub trait BlobStoreGet {
    /// Error type for get operations, parameterised by the deserialization error.
    type GetError<E: std::error::Error + Send + Sync + 'static>: Error + Send + Sync + 'static;

    /// Retrieves a blob from the repository by its handle.
    /// The handle is a unique identifier for the blob, and is used to retrieve it from the repository.
    /// The blob is returned as a [`Blob`] object, which contains the raw bytes of the blob,
    /// which can be deserialized via the appropriate schema type, which is specified by the `T` type parameter.
    ///
    /// # Errors
    /// Returns an error if the blob could not be found in the repository.
    /// The error type is specified by the `Err` associated type.
    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding;
}

/// The `PutBlob` trait is used to store blobs in a repository.
pub trait BlobStorePut {
    /// Error type for put operations.
    type PutError: Error + Debug + Send + Sync + 'static;

    /// Serialises `item` as a blob, stores it, and returns its handle.
    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding;
}

/// Combined read/write blob storage.
///
/// Extends [`BlobStorePut`] with the ability to create a shareable
/// [`Reader`](BlobStore::Reader) snapshot for concurrent reads.
pub trait BlobStore: BlobStorePut {
    /// A clonable reader handle for concurrent blob lookups.
    type Reader: BlobStoreGet + BlobStoreList + Clone + Send + PartialEq + Eq + 'static;
    /// Error type for creating a reader.
    type ReaderError: Error + Debug + Send + Sync + 'static;
    /// Creates a shareable reader snapshot of the current store state.
    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError>;
}

/// Trait for blob stores that can retain a supplied set of handles.
pub trait BlobStoreKeep {
    /// Retain only the blobs identified by `handles`.
    fn keep<I>(&mut self, handles: I)
    where
        I: IntoIterator<Item = Inline<Handle<UnknownBlob>>>;
}

/// Trait for stores that can enumerate a blob's child references.
///
/// "Children" are the 32-byte-aligned values in a blob that correspond
/// to existing blobs in the store — the conservative set of references.
///
/// The default implementation scans the blob's bytes and checks each
/// 32-byte chunk with [`BlobStoreGet::get`]. Backends with batch
/// capabilities (e.g. a network store with a SYNC protocol) can
/// override this for efficiency.
pub trait BlobChildren: BlobStoreGet {
    /// Return handles of blobs referenced by `handle` that exist in this store.
    fn children(&self, handle: Inline<Handle<UnknownBlob>>) -> Vec<Inline<Handle<UnknownBlob>>> {
        let Ok(blob) = self.get::<Blob<UnknownBlob>, UnknownBlob>(handle) else {
            return Vec::new();
        };
        let bytes = blob.bytes.as_ref();
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + INLINE_LEN <= bytes.len() {
            let mut raw = [0u8; INLINE_LEN];
            raw.copy_from_slice(&bytes[offset..offset + INLINE_LEN]);
            let candidate = Inline::<Handle<UnknownBlob>>::new(raw);
            if self.get::<anybytes::Bytes, UnknownBlob>(candidate).is_ok() {
                result.push(candidate);
            }
            offset += INLINE_LEN;
        }
        result
    }
}

// No blanket impl — types opt in explicitly so they can provide
// optimized implementations (e.g. network stores with batch protocols).
// Use `impl_blob_children_default!` for the scan-and-check fallback.

/// Outcome of a compare-and-swap update on the separate mutable
/// [`PinStore`] primitive. StrongPin branch publication does not use this type.
#[derive(Debug)]
pub enum PushResult {
    /// The CAS succeeded — the pin now points to the new value.
    Success(),
    /// The CAS failed — the pin's head had advanced. Contains the
    /// current head, or `None` if the pin was tombstoned concurrently.
    Conflict(Option<Inline<Handle<SimpleArchive>>>),
}

/// A point-in-time snapshot of (pin id → head) mappings.
///
/// PATCH keyed by 16-byte pin id, valued by the pinned head's handle.
/// Cloning is O(1) (refcount bump), so this is the right primitive for
/// handing pin state across threads or into long-lived serving views.
///
/// Returned by [`PinStore::pin_snapshot`].
pub type PinSnapshot = PATCH<16, IdentitySchema, Inline<Handle<SimpleArchive>>>;

/// Storage backend for pins: named, atomically-updatable handles to
/// SimpleArchive blobs.
///
/// A *pin* is the storage primitive — a named cell holding a single
/// `Inline<Handle<SimpleArchive>>`, updated via compare-and-swap. The
/// pile's compaction sweep treats every pin head as a reachability
/// root: blobs reachable from a pin survive; the rest are reclaimed.
///
/// Pins back several specialized local or legacy use patterns,
/// distinguished at higher layers via metadata markers:
/// - A **tracking pin** mirrors a legacy remote HEAD observation and carries
///   `tracking_remote_pin` + `remote_name`.
/// - A **local-only pin** (renewal policy, pending requests,
///   per-team cap holdings) carries `local_only_pin: <kind>` and is
///   excluded from gossip publication.
/// - Older stores may contain mutable content-branch heads. They remain
///   readable as legacy pins but are not StrongPin branch authority.
///
/// `PinStore` itself doesn't know about these distinctions — it just
/// provides the primitive: enumerate ids, read the current head, CAS
/// an update. The two-level taxonomy lives at higher layers
/// (decide#6de2dd95).
///
/// This trait is the stateful counterpart to [`BlobStore`]: blob
/// stores are content-addressed and orderless; pin stores track a
/// single mutable pointer per pin. The update operation uses
/// compare-and-swap semantics so multiple writers can coordinate
/// without locks.
pub trait PinStore {
    /// Error type for listing pins.
    type PinsError: Error + Debug + Send + Sync + 'static;
    /// Error type for head lookups.
    type HeadError: Error + Debug + Send + Sync + 'static;
    /// Error type for CAS updates.
    type UpdateError: Error + Debug + Send + Sync + 'static;

    /// Iterator over pin IDs.
    type ListIter<'a>: Iterator<Item = Result<Id, Self::PinsError>>
    where
        Self: 'a;

    /// Lists every pin in the store. Returns a fallible iterator over ids of
    /// every role. Callers classify higher-level roles from the referenced
    /// metadata; this enumeration does not imply that any pin is a branch.
    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError>;

    /// Cheap point-in-time snapshot of the (pin id → head) map.
    ///
    /// Returns a [`PinSnapshot`] — a PATCH keyed by pin id, valued by
    /// the pinned head's handle. Cloning the returned PATCH is O(1)
    /// (refcount bump), so this is also the right primitive for handing
    /// the pin state to background threads / async tasks without
    /// re-querying the store.
    ///
    /// The default impl walks `pins()` + `head()`. Stores with a
    /// PATCH-backed pin index ([`crate::repo::pile::Pile`]) override to
    /// clone the index directly. Head errors during the default walk
    /// are skipped silently — partial snapshots are acceptable for the
    /// "serving view" use case; callers that need strict atomicity
    /// should drive [`pins`](Self::pins) + [`head`](Self::head)
    /// themselves and handle errors.
    fn pin_snapshot(&mut self) -> Result<PinSnapshot, Self::PinsError> {
        let mut out = PinSnapshot::new();
        let ids: Vec<Id> = self.pins()?.filter_map(|r| r.ok()).collect();
        for id in ids {
            if let Ok(Some(h)) = self.head(id) {
                let bid: [u8; 16] = id.into();
                let entry = Entry::with_value(&bid, h);
                out.insert(&entry);
            }
        }
        Ok(out)
    }

    /// Retrieves the current head of a pin by its id.
    ///
    /// Returns `Ok(Some(handle))` if the pin exists and has a head,
    /// `Ok(None)` if the pin is tombstoned (deleted), and an error if
    /// the underlying store failed to read.
    ///
    /// # Parameters
    /// * `id` — The id of the pin to look up.
    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError>;

    /// Compare-and-swap update of a pin's head.
    ///
    /// Used to create a fresh pin, advance an existing one, or
    /// tombstone (delete) one. The CAS guard (`old`) lets multiple
    /// writers coordinate without locks: a stale writer's update
    /// returns `PushResult::Conflict(current)` carrying the actual
    /// current head for retry / merge.
    ///
    /// # Parameters
    /// * `id` — The id of the pin to update.
    /// * `old` — Expected current head (`None` when creating a fresh pin).
    /// * `new` — New head (`None` tombstones the pin).
    ///
    /// # Returns
    /// * `Success` — The pin now points at `new`.
    /// * `Conflict(current)` — Some other writer advanced first; the
    ///   pin's current head is `current`.
    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError>;
}

/// Storage backend for *weak* pins: anonymous, per-blob retention markers.
///
/// Retention is one strength axis, resolved last-writer-wins by log
/// position: `pin ⊐ weak-pin ⊐ weak-unpin ⊐ unpin`. A [`PinStore`]
/// record is `pin`, its tombstone is `unpin`; this trait adds the soft
/// siblings. Unlike a strong pin, a weak pin has no name — it is keyed
/// by the blob handle itself.
///
/// A weak pin is demand-born: "I want this blob; fetch it if absent;
/// keep it while there's room; evictable under pressure." One marker is
/// simultaneously the want-signal a sync daemon works from (fetch what
/// is weak-pinned but absent), the cache-retention marker, and the
/// eviction target. `unpin_weak` retracts it.
///
/// Mutable pins are hard local retention roots alongside every accepted branch
/// assertion. Weak state may be evicted under pressure and never blocks either
/// kind of hard root.
pub trait WeakPinStore: PinStore {
    /// Error type for weak-pin operations.
    type WeakPinError: Error + Debug + Send + Sync + 'static;

    /// Iterator over the LWW-resolved weak-pinned handles.
    type WeakListIter<'a>: Iterator<Item = Result<Inline<Handle<UnknownBlob>>, Self::WeakPinError>>
    where
        Self: 'a;

    /// Records a weak pin for `handle`. Later records win: a weak pin
    /// after a weak unpin of the same handle re-pins it.
    fn pin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;

    /// Retracts a weak pin for `handle` (last-writer-wins).
    fn unpin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;

    /// Lists every weakly pinned handle (the LWW-resolved set). This is
    /// the enumeration surface for sync daemons (fetch the absent ones)
    /// and GC (the budgeted-weak side of the keep set).
    fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError>;
}

/// Error returned by [`transfer`] when copying blobs between stores.
#[derive(Debug)]
pub enum TransferError<ListErr, LoadErr, StoreErr> {
    /// Failed to list handles from the source.
    List(ListErr),
    /// Failed to load a blob from the source.
    Load(LoadErr),
    /// Failed to store a blob in the target.
    Store(StoreErr),
}

impl<ListErr, LoadErr, StoreErr> fmt::Display for TransferError<ListErr, LoadErr, StoreErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to transfer blob")
    }
}

impl<ListErr, LoadErr, StoreErr> Error for TransferError<ListErr, LoadErr, StoreErr>
where
    ListErr: Debug + Error + 'static,
    LoadErr: Debug + Error + 'static,
    StoreErr: Debug + Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::List(e) => Some(e),
            Self::Load(e) => Some(e),
            Self::Store(e) => Some(e),
        }
    }
}

/// Copies the specified blob handles from `source` into `target`.
pub fn transfer<'a, BS, BT, Handles>(
    source: &'a BS,
    target: &'a mut BT,
    handles: Handles,
) -> impl Iterator<
    Item = Result<
        (Inline<Handle<UnknownBlob>>, Inline<Handle<UnknownBlob>>),
        TransferError<
            Infallible,
            <BS as BlobStoreGet>::GetError<Infallible>,
            <BT as BlobStorePut>::PutError,
        >,
    >,
> + 'a
where
    BS: BlobStoreGet + 'a,
    BT: BlobStorePut + 'a,
    Handles: IntoIterator<Item = Inline<Handle<UnknownBlob>>> + 'a,
    Handles::IntoIter: 'a,
{
    handles.into_iter().map(move |source_handle| {
        let blob: Blob<UnknownBlob> = source.get(source_handle).map_err(TransferError::Load)?;

        Ok((
            source_handle,
            (target.put(blob).map_err(TransferError::Store)?),
        ))
    })
}

/// Iterator that visits every blob handle reachable from a set of roots.
///
/// Uses [`BlobChildren`] to enumerate references at each level,
/// so backends with batch capabilities get efficient traversal.
pub struct ReachableHandles<'a, BS>
where
    BS: BlobChildren,
{
    source: &'a BS,
    queue: VecDeque<Inline<Handle<UnknownBlob>>>,
    visited: HashSet<[u8; INLINE_LEN]>,
}

impl<'a, BS> ReachableHandles<'a, BS>
where
    BS: BlobChildren,
{
    fn new(source: &'a BS, roots: impl IntoIterator<Item = Inline<Handle<UnknownBlob>>>) -> Self {
        let mut queue = VecDeque::new();
        for handle in roots {
            queue.push_back(handle);
        }

        Self {
            source,
            queue,
            visited: HashSet::new(),
        }
    }
}

impl<'a, BS> Iterator for ReachableHandles<'a, BS>
where
    BS: BlobChildren,
{
    type Item = Inline<Handle<UnknownBlob>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(handle) = self.queue.pop_front() {
            let raw = handle.raw;

            if !self.visited.insert(raw) {
                continue;
            }

            // Use BlobChildren to get references — backends can override
            // with batch-optimized implementations.
            for child in self.source.children(handle) {
                if !self.visited.contains(&child.raw) {
                    self.queue.push_back(child);
                }
            }

            return Some(handle);
        }

        None
    }
}

/// Create a breadth-first iterator over blob handles reachable from `roots`.
///
/// Uses [`BlobChildren`] for reference enumeration, so network-backed
/// stores can provide optimized batch implementations.
pub fn reachable<'a, BS>(
    source: &'a BS,
    roots: impl IntoIterator<Item = Inline<Handle<UnknownBlob>>>,
) -> ReachableHandles<'a, BS>
where
    BS: BlobChildren,
{
    ReachableHandles::new(source, roots)
}

/// Iterate over every 32-byte candidate in the value column of a [`TribleSet`].
///
/// This is a conservative conversion used when scanning metadata for potential
/// blob handles. Each 32-byte chunk is treated as a `Handle<UnknownBlob>`.
/// Callers can feed the resulting iterator into [`BlobStoreKeep::keep`] or other
/// helpers that accept collections of handles.
pub fn potential_handles<'a>(
    set: &'a TribleSet,
) -> impl Iterator<Item = Inline<Handle<UnknownBlob>>> + 'a {
    set.vae.iter().map(|raw| {
        let mut value = [0u8; INLINE_LEN];
        value.copy_from_slice(&raw[0..INLINE_LEN]);
        Inline::<Handle<UnknownBlob>>::new(value)
    })
}

/// An error that can occur when creating a commit.
/// This error can be caused by a failure to store the content or metadata blobs.
#[derive(Debug)]
pub enum CreateCommitError<BlobErr: Error + Debug + Send + Sync + 'static> {
    /// Failed to store the content blob.
    ContentStorageError(BlobErr),
    /// Failed to store the commit metadata blob.
    CommitStorageError(BlobErr),
}

impl<BlobErr: Error + Debug + Send + Sync + 'static> fmt::Display for CreateCommitError<BlobErr> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreateCommitError::ContentStorageError(e) => write!(f, "Content storage failed: {e}"),
            CreateCommitError::CommitStorageError(e) => {
                write!(f, "Commit metadata storage failed: {e}")
            }
        }
    }
}

impl<BlobErr: Error + Debug + Send + Sync + 'static> Error for CreateCommitError<BlobErr> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CreateCommitError::ContentStorageError(e) => Some(e),
            CreateCommitError::CommitStorageError(e) => Some(e),
        }
    }
}

/// Error returned by [`Workspace::merge`].
#[derive(Debug)]
pub enum MergeError {
    /// The merge failed because the workspaces have different base repos.
    DifferentRepos(),
    /// The ancestry walk failed because one or more commit blobs along the
    /// chain weren't readable from the workspace's view. The merge refuses
    /// to fall through to a divergent-merge in this case — creating a merge
    /// commit referencing an unknown chain would leave a dangling parent in
    /// the resulting branch, and the append-only pile keeps that corruption
    /// forever.
    ///
    /// Callers should ensure both heads' full closures are locally present
    /// (e.g. via `fetch_reachable`) before retrying. The contained string
    /// is a human-readable description of the underlying read failure.
    AncestryWalkFailed(String),
}

/// An assertion-native repository that publishes branches owned by one key.
///
/// Blob storage and grow-only branch assertions are separate capabilities.
/// Merely constructing a repository therefore needs only [`BlobStore`]; branch
/// resolution and publication are available when the backend additionally
/// implements [`BranchAssertionStore`].
pub struct Repository<Storage: BlobStore> {
    storage: Storage,
    signing_key: SigningKey,
    commit_metadata: MetadataHandle,
}

/// A caller presented a branch descriptor owned by a different key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForeignBranchIdentity {
    expected: [u8; 32],
    actual: [u8; 32],
}

impl ForeignBranchIdentity {
    /// Repository key required at this local-authoring boundary.
    pub const fn expected(&self) -> [u8; 32] {
        self.expected
    }

    /// Key carried by the rejected branch descriptor.
    pub const fn actual(&self) -> [u8; 32] {
        self.actual
    }
}

impl fmt::Display for ForeignBranchIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "branch is owned by {}, but this repository publishes only as {}",
            hex::encode(self.actual),
            hex::encode(self.expected)
        )
    }
}

impl Error for ForeignBranchIdentity {}

/// Failure while creating a detached, as-yet-unpublished workspace.
#[derive(Debug)]
pub enum CreateWorkspaceError<ReaderErr> {
    /// Failed to snapshot the repository's blob store.
    StorageReader(ReaderErr),
}

/// Failure while resolving an own-key branch identity.
#[derive(Debug)]
pub enum ResolveBranchError<AssertionErr, ReaderErr, DagErr> {
    /// The requested descriptor is outside this repository's authoring key.
    ForeignIdentity(ForeignBranchIdentity),
    /// Failed to obtain a coherent assertion snapshot.
    AssertionStore(AssertionErr),
    /// Failed to snapshot the blob store.
    StorageReader(ReaderErr),
    /// Commit ancestry could not be read or decoded.
    CommitDag(DagErr),
}

/// Failure while turning an assertion frontier into a writable workspace.
#[derive(Debug)]
pub enum AssertionPullError<AssertionErr, ReaderErr, DagErr> {
    /// The requested descriptor is outside this repository's authoring key.
    ForeignIdentity(ForeignBranchIdentity),
    /// Failed to obtain a coherent assertion snapshot.
    AssertionStore(AssertionErr),
    /// Failed to snapshot the blob store.
    StorageReader(ReaderErr),
    /// Commit ancestry could not be read or decoded.
    CommitDag(DagErr),
    /// No assertion exists for this exact branch descriptor.
    Absent,
    /// At least one asserted tip has not arrived locally yet.
    TipPending(TipPendingFrontier),
    /// Tip metadata is readable, but missing ancestry keeps the frontier partial.
    Partial(PartialFrontier),
}

/// Result of publishing a workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The workspace head already equals its base; no assertion was added.
    NoChange,
    /// A verified assertion was durably appended (or was already present).
    Published(AssertionId),
}

/// Failure before a workspace assertion reaches its durable append point.
#[derive(Debug)]
pub enum PublishError<PutErr, FlushErr, ReaderErr, GetErr, AssertionErr> {
    /// The workspace descriptor is outside this repository's authoring key.
    ForeignIdentity(ForeignBranchIdentity),
    /// Uploading a staged blob failed.
    StoragePut(PutErr),
    /// Making uploaded blobs durable failed.
    StorageFlush(FlushErr),
    /// Snapshotting the now-durable blob store failed.
    StorageReader(ReaderErr),
    /// The proposed commit metadata could not be loaded or decoded.
    StorageGet(GetErr),
    /// The proposed commit is not one of the canonical commit shapes.
    BadCommitMetadata(commit::CommitMetadataError),
    /// A changed workspace unexpectedly has no proposed head.
    MissingHead,
    /// Durably appending the signed branch assertion failed.
    AssertionStore(AssertionErr),
}

impl<Storage> Repository<Storage>
where
    Storage: BlobStore,
{
    /// Creates a new repository with the given storage, signing key, and
    /// repo-wide commit metadata.
    ///
    /// `commit_metadata` accepts anything convertible into a [`Fragment`] —
    /// either a raw [`TribleSet`] (auto-promoted with empty blob store via
    /// `impl From<TribleSet> for Fragment`), or a Fragment built up via
    /// `entity!{}` / `attributes!::describe()` that carries auxiliary blobs
    /// (e.g. `Handle<LongString>` doc strings). The Fragment's blobs are
    /// absorbed into storage so handles referenced by the metadata facts
    /// stay resolvable for any downstream reader that pulls a commit and
    /// calls [`Workspace::checkout_metadata`].
    ///
    /// The resulting metadata blob is referenced from every commit produced
    /// by workspaces of this repository.
    pub fn new<F: Into<crate::trible::Fragment>>(
        mut storage: Storage,
        signing_key: SigningKey,
        commit_metadata: F,
    ) -> Result<Self, <Storage as BlobStorePut>::PutError> {
        let (facts, mut blobs) = commit_metadata.into().into_facts_and_blobs();
        // Persist any blobs the Fragment carried — typically `Handle<LongString>`
        // doc strings or other handle-referenced payloads. They're stored as
        // `UnknownBlob` (raw bytes) because the storage layer is encoding-agnostic;
        // readers recover the schema via the handle's declared encoding.
        let reader = blobs
            .reader()
            .expect("MemoryBlobStore::reader is infallible");
        for (_handle, blob) in reader {
            storage.put::<UnknownBlob, _>(blob)?;
        }
        let commit_metadata = storage.put(facts)?;
        Ok(Self {
            storage,
            signing_key,
            commit_metadata,
        })
    }

    /// Consume the repository and return the underlying storage backend.
    ///
    /// This is useful for callers that need to take ownership of the storage
    /// (for example to call `close()` on a [`Pile`]) instead of letting the
    /// repository drop it implicitly.
    pub fn into_storage(self) -> Storage {
        self.storage
    }

    /// Borrow the underlying storage backend.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Borrow the underlying storage backend mutably.
    pub fn storage_mut(&mut self) -> &mut Storage {
        &mut self.storage
    }

    /// Returns the repository commit metadata handle.
    pub fn commit_metadata(&self) -> MetadataHandle {
        self.commit_metadata
    }

    /// Public key that owns every branch this repository may publish.
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Derive this repository's exact branch descriptor for a human name.
    pub fn branch_identity(&self, name: &str) -> BranchIdentity {
        let name: Blob<LongString> = name.to_owned().to_blob();
        BranchIdentity::new(self.verifying_key(), name.get_handle())
    }

    /// Create an empty workspace without publishing an empty branch.
    ///
    /// The name blob is staged locally so a later publication makes the exact
    /// branch descriptor self-describing. No assertion and no repository blob
    /// is written by this operation; empty branches remain unrepresentable.
    pub fn create_workspace(
        &mut self,
        name: &str,
    ) -> Result<Workspace<Storage>, CreateWorkspaceError<Storage::ReaderError>> {
        let name_blob: Blob<LongString> = name.to_owned().to_blob();
        let identity = BranchIdentity::new(self.verifying_key(), name_blob.get_handle());
        let mut staged = MemoryBlobStore::new();
        staged
            .put::<LongString, _>(name_blob)
            .expect("MemoryBlobStore::put is infallible");
        let base_blobs = self
            .storage
            .reader()
            .map_err(CreateWorkspaceError::StorageReader)?;

        Ok(Workspace {
            staged,
            base_blobs,
            identity,
            head: None,
            base_head: None,
            signing_key: self.signing_key.clone(),
            commit_metadata: self.commit_metadata,
        })
    }

    fn require_own_identity(&self, identity: &BranchIdentity) -> Result<(), ForeignBranchIdentity> {
        let expected = self.verifying_key().to_bytes();
        let actual = identity.author().to_bytes();
        if actual == expected {
            Ok(())
        } else {
            Err(ForeignBranchIdentity { expected, actual })
        }
    }
}

impl<Storage> Repository<Storage>
where
    Storage: BlobStore + BranchAssertionStore,
    Storage::Reader: PartialCommitDag,
{
    /// Resolve the grow-only assertions for one exact own-key identity.
    ///
    /// Foreign identities are rejected before the assertion store or blob
    /// reader is touched. Accepting replicated foreign assertions is a
    /// separate, policy-bearing ingest operation rather than an authoring
    /// convenience on this repository.
    pub fn resolve(
        &mut self,
        identity: &BranchIdentity,
    ) -> Result<
        BranchResolution,
        ResolveBranchError<
            <Storage as BranchAssertionStore>::Error,
            Storage::ReaderError,
            <Storage::Reader as PartialCommitDag>::Error,
        >,
    > {
        self.require_own_identity(identity)
            .map_err(ResolveBranchError::ForeignIdentity)?;
        let snapshot = self
            .storage
            .assertion_snapshot()
            .map_err(ResolveBranchError::AssertionStore)?;
        let mut reader = self
            .storage
            .reader()
            .map_err(ResolveBranchError::StorageReader)?;
        branch_frontier::resolve_branch(&snapshot, identity, &mut reader)
            .map_err(ResolveBranchError::CommitDag)
    }

    /// Resolve this repository's branch descriptor for a human name.
    pub fn resolve_name(
        &mut self,
        name: &str,
    ) -> Result<
        BranchResolution,
        ResolveBranchError<
            <Storage as BranchAssertionStore>::Error,
            Storage::ReaderError,
            <Storage::Reader as PartialCommitDag>::Error,
        >,
    > {
        let identity = self.branch_identity(name);
        self.resolve(&identity)
    }

    /// Open a complete assertion frontier as a writable workspace.
    ///
    /// A missing asserted tip or unresolved ancestry remains explicit. Partial
    /// frontiers expose a deterministic candidate-root descriptor only,
    /// but cannot be checked out or license a new authored descendant until
    /// their maximal antichain is known completely.
    pub fn pull(
        &mut self,
        identity: BranchIdentity,
    ) -> Result<
        Workspace<Storage>,
        AssertionPullError<
            <Storage as BranchAssertionStore>::Error,
            Storage::ReaderError,
            <Storage::Reader as PartialCommitDag>::Error,
        >,
    > {
        self.require_own_identity(&identity)
            .map_err(AssertionPullError::ForeignIdentity)?;
        let snapshot = self
            .storage
            .assertion_snapshot()
            .map_err(AssertionPullError::AssertionStore)?;
        let mut base_blobs = self
            .storage
            .reader()
            .map_err(AssertionPullError::StorageReader)?;
        let resolution = branch_frontier::resolve_branch(&snapshot, &identity, &mut base_blobs)
            .map_err(AssertionPullError::CommitDag)?;
        let complete = match resolution {
            BranchResolution::Absent => return Err(AssertionPullError::Absent),
            BranchResolution::TipPending(frontier) => {
                return Err(AssertionPullError::TipPending(frontier));
            }
            BranchResolution::Partial(frontier) => {
                return Err(AssertionPullError::Partial(frontier));
            }
            BranchResolution::Complete(frontier) => frontier,
        };

        let mut staged = MemoryBlobStore::new();
        let resolved = match complete.resolved_head() {
            ResolvedHead::Existing(commit) => commit,
            ResolvedHead::Synthetic(blob) => staged
                .put::<SimpleArchive, _>(blob)
                .expect("MemoryBlobStore::put is infallible"),
        };

        Ok(Workspace {
            staged,
            base_blobs,
            identity,
            head: Some(resolved),
            base_head: Some(resolved),
            signing_key: self.signing_key.clone(),
            commit_metadata: self.commit_metadata,
        })
    }
}

impl<Storage> Repository<Storage>
where
    Storage: BlobStore + StorageFlush + BranchAssertionStore,
{
    /// Publish the workspace head as one signed grow-only assertion.
    ///
    /// All staged blobs cross the storage durability boundary before the
    /// assertion is appended. The reader used to validate the proposed tip is
    /// acquired before that append and retained by the workspace afterwards,
    /// leaving no fallible operation after the publication point.
    pub fn push(
        &mut self,
        workspace: &mut Workspace<Storage>,
    ) -> Result<
        PublishOutcome,
        PublishError<
            <Storage as BlobStorePut>::PutError,
            <Storage as StorageFlush>::Error,
            Storage::ReaderError,
            <Storage::Reader as BlobStoreGet>::GetError<UnarchiveError>,
            <Storage as BranchAssertionStore>::Error,
        >,
    > {
        self.require_own_identity(&workspace.identity)
            .map_err(PublishError::ForeignIdentity)?;

        let staged = workspace
            .staged
            .reader()
            .expect("MemoryBlobStore::reader is infallible");
        for (_handle, blob) in staged {
            self.storage
                .put::<UnknownBlob, _>(blob)
                .map_err(PublishError::StoragePut)?;
        }
        self.storage.flush().map_err(PublishError::StorageFlush)?;

        let reader = self.storage.reader().map_err(PublishError::StorageReader)?;
        if workspace.head == workspace.base_head {
            workspace.base_blobs = reader;
            workspace.staged = MemoryBlobStore::new();
            return Ok(PublishOutcome::NoChange);
        }

        let proposed = workspace.head.ok_or(PublishError::MissingHead)?;
        let commit_meta: TribleSet = reader.get(proposed).map_err(PublishError::StorageGet)?;
        commit::direct_parents(&commit_meta).map_err(PublishError::BadCommitMetadata)?;

        let assertion =
            BranchAssertion::sign(&self.signing_key, workspace.identity.name(), proposed);
        let assertion_id = assertion.id();
        self.storage
            .append_assertion(assertion)
            .map_err(PublishError::AssertionStore)?;

        workspace.base_blobs = reader;
        workspace.base_head = Some(proposed);
        workspace.staged = MemoryBlobStore::new();
        Ok(PublishOutcome::Published(assertion_id))
    }
}

/// A handle to a commit blob in the repository.
pub type CommitHandle = Inline<Handle<SimpleArchive>>;
type MetadataHandle = Inline<Handle<SimpleArchive>>;
/// A set of commit handles, used by [`CommitSelector`] and [`Checkout`].
pub type CommitSet = PATCH<INLINE_LEN, IdentitySchema, ()>;

/// The result of a [`Workspace::checkout`] operation: a [`TribleSet`] paired
/// with the set of commits that produced it. Pass the commit set as the start
/// of a range selector to obtain incremental deltas on the next checkout.
///
/// [`Checkout`] dereferences to [`TribleSet`], so it can be used directly with
/// `find!`, `pattern!`, and `pattern_changes!`.
///
/// # Example: incremental updates
///
/// ```rust,ignore
/// let mut changed = repo.pull(branch_identity)?.checkout(..)?;
/// let mut full = changed.facts().clone();
///
/// loop {
///     // full already includes changed
///     for result in pattern_changes!(&full, &changed, [{ ... }]) {
///         // process new results
///     }
///
///     // Advance — exclude exactly the commits we already processed.
///     changed = repo.pull(branch_identity)?.checkout(changed.commits()..)?;
///     full += &changed;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Checkout {
    facts: TribleSet,
    commits: CommitSet,
}

impl PartialEq<TribleSet> for Checkout {
    fn eq(&self, other: &TribleSet) -> bool {
        self.facts == *other
    }
}

impl PartialEq<Checkout> for TribleSet {
    fn eq(&self, other: &Checkout) -> bool {
        *self == other.facts
    }
}

impl Checkout {
    /// The checked-out tribles.
    pub fn facts(&self) -> &TribleSet {
        &self.facts
    }

    /// The set of commits that produced this checkout. Use as the start of a
    /// range selector (`checkout.commits()..`) to exclude these commits
    /// on the next checkout and obtain only new data.
    pub fn commits(&self) -> CommitSet {
        self.commits.clone()
    }

    /// Consume the checkout and return the inner TribleSet.
    pub fn into_facts(self) -> TribleSet {
        self.facts
    }
}

impl std::ops::Deref for Checkout {
    type Target = TribleSet;
    fn deref(&self) -> &TribleSet {
        &self.facts
    }
}

impl std::ops::AddAssign<&Checkout> for Checkout {
    fn add_assign(&mut self, rhs: &Checkout) {
        self.facts += rhs.facts.clone();
        self.commits.union(rhs.commits.clone());
    }
}

impl std::ops::Add for Checkout {
    type Output = Self;
    fn add(mut self, rhs: Self) -> Self {
        self.facts += rhs.facts;
        self.commits.union(rhs.commits);
        self
    }
}

impl std::ops::Add<&Checkout> for Checkout {
    type Output = Self;
    fn add(mut self, rhs: &Checkout) -> Self {
        self += rhs;
        self
    }
}

/// The Workspace represents the mutable working area or "staging" state.
/// It was formerly known as `Head`. It is sent to worker threads,
/// modified (via commits, merges, etc.), and then merged back into the Repository.
pub struct Workspace<Blobs: BlobStore> {
    /// Staged blobs — added to this workspace but not yet pushed to
    /// the underlying repo. Analogous to git's staging area (the
    /// index): blobs accumulate here via `put` and friends, then
    /// `repo.push(&mut ws)` ships everything as one batch to the
    /// durable backend.
    pub staged: MemoryBlobStore,
    /// The blob storage base for the workspace.
    base_blobs: Blobs::Reader,
    /// Exact `(author key, name handle)` descriptor this workspace publishes.
    identity: BranchIdentity,
    /// Handle to the current commit in the working branch. `None` for an empty branch.
    head: Option<CommitHandle>,
    /// Resolved head from which local work began.
    ///
    /// Equality with `head` means there is no new branch claim to publish. A
    /// divergent complete frontier uses its canonical synthetic merge here.
    base_head: Option<CommitHandle>,
    /// Signing key used for authored commits.
    signing_key: SigningKey,
    /// Metadata handle for commits created in this workspace.
    commit_metadata: MetadataHandle,
}

impl<Blobs> fmt::Debug for Workspace<Blobs>
where
    Blobs: BlobStore,
    Blobs::Reader: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Workspace")
            .field("staged", &self.staged)
            .field("base_blobs", &self.base_blobs)
            .field("identity", &self.identity)
            .field("base_head", &self.base_head)
            .field("head", &self.head)
            .field("commit_metadata", &self.commit_metadata)
            .finish()
    }
}

/// Helper trait for [`Workspace::checkout`] specifying commit handles or ranges.
pub trait CommitSelector<Blobs: BlobStore> {
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >;
}

/// Selector that returns every commit reachable from a starting selector.
pub struct Ancestors<S>(pub S);

/// Convenience function to create an [`Ancestors`] selector.
pub fn ancestors<S>(selector: S) -> Ancestors<S> {
    Ancestors(selector)
}

/// Selector that walks every commit in the input set back N parent steps,
/// following all parent links (including merge parents). Returns the set
/// of all commits found at exactly depth N from the starting set.
///
/// This is a wavefront expansion: at each step, every commit in the current
/// frontier is replaced by all of its parents. After N steps the frontier
/// is the result.
pub struct NthAncestors<S>(pub S, pub usize);

/// Walk `selector` back `n` parent steps through all parent links.
pub fn nth_ancestors<S>(selector: S, n: usize) -> NthAncestors<S> {
    NthAncestors(selector, n)
}

/// Selector that returns the direct parents of commits from a starting selector.
pub struct Parents<S>(pub S);

/// Convenience function to create a [`Parents`] selector.
pub fn parents<S>(selector: S) -> Parents<S> {
    Parents(selector)
}

/// Selector that returns commits reachable from either of two selectors but
/// not both.
pub struct SymmetricDiff<A, B>(pub A, pub B);

/// Convenience function to create a [`SymmetricDiff`] selector.
pub fn symmetric_diff<A, B>(a: A, b: B) -> SymmetricDiff<A, B> {
    SymmetricDiff(a, b)
}

/// Selector that returns the union of commits returned by two selectors.
pub struct Union<A, B> {
    left: A,
    right: B,
}

/// Convenience function to create a [`Union`] selector.
pub fn union<A, B>(left: A, right: B) -> Union<A, B> {
    Union { left, right }
}

/// Selector that returns the intersection of commits returned by two selectors.
pub struct Intersect<A, B> {
    left: A,
    right: B,
}

/// Convenience function to create an [`Intersect`] selector.
pub fn intersect<A, B>(left: A, right: B) -> Intersect<A, B> {
    Intersect { left, right }
}

/// Selector that returns commits from the left selector that are not also
/// returned by the right selector.
pub struct Difference<A, B> {
    left: A,
    right: B,
}

/// Convenience function to create a [`Difference`] selector.
pub fn difference<A, B>(left: A, right: B) -> Difference<A, B> {
    Difference { left, right }
}

/// Selector that returns commits with timestamps in the given inclusive range.
pub struct TimeRange(pub Epoch, pub Epoch);

/// Convenience function to create a [`TimeRange`] selector.
pub fn time_range(start: Epoch, end: Epoch) -> TimeRange {
    TimeRange(start, end)
}

/// Selector that filters commits returned by another selector.
pub struct Filter<S, F> {
    selector: S,
    filter: F,
}

/// Convenience function to create a [`Filter`] selector.
pub fn filter<S, F>(selector: S, filter: F) -> Filter<S, F> {
    Filter { selector, filter }
}

impl<Blobs> CommitSelector<Blobs> for CommitHandle
where
    Blobs: BlobStore,
{
    fn select(
        self,
        _ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut patch = CommitSet::new();
        patch.insert(&Entry::new(&self.raw));
        Ok(patch)
    }
}

impl<Blobs> CommitSelector<Blobs> for CommitSet
where
    Blobs: BlobStore,
{
    fn select(
        self,
        _ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        Ok(self)
    }
}

impl<Blobs> CommitSelector<Blobs> for Vec<CommitHandle>
where
    Blobs: BlobStore,
{
    fn select(
        self,
        _ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut patch = CommitSet::new();
        for handle in self {
            patch.insert(&Entry::new(&handle.raw));
        }
        Ok(patch)
    }
}

impl<Blobs> CommitSelector<Blobs> for &[CommitHandle]
where
    Blobs: BlobStore,
{
    fn select(
        self,
        _ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut patch = CommitSet::new();
        for handle in self {
            patch.insert(&Entry::new(&handle.raw));
        }
        Ok(patch)
    }
}

impl<Blobs> CommitSelector<Blobs> for Option<CommitHandle>
where
    Blobs: BlobStore,
{
    fn select(
        self,
        _ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut patch = CommitSet::new();
        if let Some(handle) = self {
            patch.insert(&Entry::new(&handle.raw));
        }
        Ok(patch)
    }
}

impl<S, Blobs> CommitSelector<Blobs> for Ancestors<S>
where
    S: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let seeds = self.0.select(ws)?;
        collect_reachable_from_patch(ws, seeds)
    }
}

impl<Blobs, S> CommitSelector<Blobs> for NthAncestors<S>
where
    Blobs: BlobStore,
    S: CommitSelector<Blobs>,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut frontier = self.0.select(ws)?;
        let mut remaining = self.1;

        while remaining > 0 && !frontier.is_empty() {
            // Collect current frontier keys before mutating.
            let keys: Vec<[u8; INLINE_LEN]> = frontier.iter().copied().collect();
            let mut next_frontier = CommitSet::new();
            for raw in keys {
                let handle = CommitHandle::new(raw);
                let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;
                for (p,) in find!((p: Inline<_>), pattern!(&meta, [{ parent: ?p }])) {
                    next_frontier.insert(&Entry::new(&p.raw));
                }
            }
            frontier = next_frontier;
            remaining -= 1;
        }

        Ok(frontier)
    }
}

impl<S, Blobs> CommitSelector<Blobs> for Parents<S>
where
    S: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let seeds = self.0.select(ws)?;
        let mut result = CommitSet::new();
        for raw in seeds.iter() {
            let handle = Inline::new(*raw);
            let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;
            for (p,) in find!((p: Inline<_>), pattern!(&meta, [{ parent: ?p }])) {
                result.insert(&Entry::new(&p.raw));
            }
        }
        Ok(result)
    }
}

impl<A, B, Blobs> CommitSelector<Blobs> for SymmetricDiff<A, B>
where
    A: CommitSelector<Blobs>,
    B: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let seeds_a = self.0.select(ws)?;
        let seeds_b = self.1.select(ws)?;
        let a = collect_reachable_from_patch(ws, seeds_a)?;
        let b = collect_reachable_from_patch(ws, seeds_b)?;
        let inter = a.intersect(&b);
        let mut union = a;
        union.union(b);
        Ok(union.difference(&inter))
    }
}

impl<A, B, Blobs> CommitSelector<Blobs> for Union<A, B>
where
    A: CommitSelector<Blobs>,
    B: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let mut left = self.left.select(ws)?;
        let right = self.right.select(ws)?;
        left.union(right);
        Ok(left)
    }
}

impl<A, B, Blobs> CommitSelector<Blobs> for Intersect<A, B>
where
    A: CommitSelector<Blobs>,
    B: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let left = self.left.select(ws)?;
        let right = self.right.select(ws)?;
        Ok(left.intersect(&right))
    }
}

impl<A, B, Blobs> CommitSelector<Blobs> for Difference<A, B>
where
    A: CommitSelector<Blobs>,
    B: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let left = self.left.select(ws)?;
        let right = self.right.select(ws)?;
        Ok(left.difference(&right))
    }
}

impl<S, F, Blobs> CommitSelector<Blobs> for Filter<S, F>
where
    Blobs: BlobStore,
    S: CommitSelector<Blobs>,
    F: for<'x, 'y> Fn(&'x TribleSet, &'y TribleSet) -> bool,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let patch = self.selector.select(ws)?;
        let mut result = CommitSet::new();
        let filter = self.filter;
        for raw in patch.iter() {
            let handle = Inline::new(*raw);
            let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;

            let Ok((content_handle,)) = find!(
                (c: Inline<_>),
                pattern!(&meta, [{ content: ?c }])
            )
            .exactly_one() else {
                return Err(WorkspaceCheckoutError::BadCommitMetadata());
            };

            let payload: TribleSet = ws
                .get(content_handle)
                .map_err(WorkspaceCheckoutError::Storage)?;

            if filter(&meta, &payload) {
                result.insert(&Entry::new(raw));
            }
        }
        Ok(result)
    }
}

/// Selector that yields commits touching a specific entity.
pub struct HistoryOf(pub Id);

/// Convenience function to create a [`HistoryOf`] selector.
pub fn history_of(entity: Id) -> HistoryOf {
    HistoryOf(entity)
}

impl<Blobs> CommitSelector<Blobs> for HistoryOf
where
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let Some(head_) = ws.head else {
            return Ok(CommitSet::new());
        };
        let entity = self.0;
        filter(
            ancestors(head_),
            move |_: &TribleSet, payload: &TribleSet| payload.iter().any(|t| t.e() == &entity),
        )
        .select(ws)
    }
}

// Generic range selectors: allow any selector type to be used as a range
// endpoint. We still walk the history reachable from the end selector but now
// stop descending a branch as soon as we encounter a commit produced by the
// start selector. This keeps the mechanics explicit—`start..end` literally
// walks from `end` until it hits `start`—while continuing to support selectors
// such as `Ancestors(...)` at either boundary.

/// Select commits and return them in deterministic ancestor-before-child
/// order without loading their content blobs.
///
/// This is the commit-leaf traversal used by derived-index bootstrap and
/// on-push maintenance. A commit reachable through several merge parents is
/// returned once. Ordering between unrelated commits is deterministic but has
/// no semantic significance.
pub fn commits_topological<S, Blobs>(
    ws: &mut Workspace<Blobs>,
    selector: S,
) -> Result<
    Vec<CommitHandle>,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
>
where
    S: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    let commits = selector.select(ws)?;
    topological_commits(ws, &commits)
}

fn topological_commits<Blobs: BlobStore>(
    ws: &mut Workspace<Blobs>,
    commits: &CommitSet,
) -> Result<
    Vec<CommitHandle>,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
> {
    let mut ordered = Vec::with_capacity(commits.len() as usize);
    let mut emitted: HashSet<CommitHandle> = HashSet::new();

    // Iterative post-order DFS avoids recursion depth depending on history
    // length. Starting points and parent lists are sorted so the result is
    // reproducible even though PATCH's ordinary iterator is intentionally
    // unordered.
    for raw in commits.iter_ordered() {
        let root = Inline::new(*raw);
        if emitted.contains(&root) {
            continue;
        }
        let mut stack = vec![(root, false)];
        while let Some((commit, expanded)) = stack.pop() {
            if emitted.contains(&commit) {
                continue;
            }
            if expanded {
                emitted.insert(commit);
                ordered.push(commit);
                continue;
            }

            stack.push((commit, true));
            let meta: TribleSet = ws.get(commit).map_err(WorkspaceCheckoutError::Storage)?;
            let mut parents: Vec<CommitHandle> = find!(
                (parent_: Inline<Handle<SimpleArchive>>),
                pattern!(&meta, [{ parent: ?parent_ }])
            )
            .map(|(parent_,)| parent_)
            .filter(|parent_| commits.get(&parent_.raw).is_some())
            .filter(|parent_| !emitted.contains(parent_))
            .collect();
            parents.sort_unstable_by_key(|parent_| parent_.raw);
            parents.dedup_by_key(|parent_| parent_.raw);
            for parent_ in parents.into_iter().rev() {
                stack.push((parent_, false));
            }
        }
    }
    Ok(ordered)
}

fn collect_reachable_from_patch<Blobs: BlobStore>(
    ws: &mut Workspace<Blobs>,
    patch: CommitSet,
) -> Result<
    CommitSet,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
> {
    let mut result = CommitSet::new();
    for raw in patch.iter() {
        let handle = Inline::new(*raw);
        let reach = collect_reachable(ws, handle)?;
        result.union(reach);
    }
    Ok(result)
}

fn collect_reachable_from_patch_until<Blobs: BlobStore>(
    ws: &mut Workspace<Blobs>,
    seeds: CommitSet,
    stop: &CommitSet,
) -> Result<
    CommitSet,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
> {
    let mut visited = HashSet::new();
    let mut stack: Vec<CommitHandle> = seeds.iter().map(|raw| Inline::new(*raw)).collect();
    let mut result = CommitSet::new();

    while let Some(commit) = stack.pop() {
        if !visited.insert(commit) {
            continue;
        }

        if stop.get(&commit.raw).is_some() {
            continue;
        }

        result.insert(&Entry::new(&commit.raw));

        let meta: TribleSet = ws
            .staged
            .reader()
            .unwrap()
            .get(commit)
            .or_else(|_| ws.base_blobs.get(commit))
            .map_err(WorkspaceCheckoutError::Storage)?;

        for (p,) in find!((p: Inline<_>,), pattern!(&meta, [{ parent: ?p }])) {
            stack.push(p);
        }
    }

    Ok(result)
}

impl<T, Blobs> CommitSelector<Blobs> for std::ops::Range<T>
where
    T: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let end_patch = self.end.select(ws)?;
        let start_patch = self.start.select(ws)?;

        collect_reachable_from_patch_until(ws, end_patch, &start_patch)
    }
}

impl<T, Blobs> CommitSelector<Blobs> for std::ops::RangeFrom<T>
where
    T: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let Some(head_) = ws.head else {
            return Ok(CommitSet::new());
        };
        let exclude_patch = self.start.select(ws)?;

        let mut head_patch = CommitSet::new();
        head_patch.insert(&Entry::new(&head_.raw));

        collect_reachable_from_patch_until(ws, head_patch, &exclude_patch)
    }
}

impl<T, Blobs> CommitSelector<Blobs> for std::ops::RangeTo<T>
where
    T: CommitSelector<Blobs>,
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let end_patch = self.end.select(ws)?;
        collect_reachable_from_patch(ws, end_patch)
    }
}

impl<Blobs> CommitSelector<Blobs> for std::ops::RangeFull
where
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let Some(head_) = ws.head else {
            return Ok(CommitSet::new());
        };
        collect_reachable(ws, head_)
    }
}

impl<Blobs> CommitSelector<Blobs> for TimeRange
where
    Blobs: BlobStore,
{
    fn select(
        self,
        ws: &mut Workspace<Blobs>,
    ) -> Result<
        CommitSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    > {
        let Some(head_) = ws.head else {
            return Ok(CommitSet::new());
        };
        let start = self.0;
        let end = self.1;
        filter(
            ancestors(head_),
            move |meta: &TribleSet, _payload: &TribleSet| {
                if let Ok(Some(((ts_start, ts_end),))) =
                    find!((t: (Epoch, Epoch)), pattern!(meta, [{ crate::metadata::created_at: ?t }])).at_most_one()
                {
                    ts_start <= end && ts_end >= start
                } else {
                    false
                }
            },
        )
        .select(ws)
    }
}

/// Minimum number of commits at which `checkout_commits*` switches
/// from the serial loop to a `rayon::par_iter().try_reduce()` over
/// the commits. Each commit involves one (or two) blob fetches plus
/// an unarchive — independent work per commit — so the crossover is
/// small. Below this the rayon overhead dominates.
#[cfg(feature = "parallel")]
const PARALLEL_CHECKOUT_THRESHOLD: usize = 8;

impl<Blobs: BlobStore> Workspace<Blobs> {
    /// Returns the exact branch identity associated with this workspace.
    pub fn identity(&self) -> &BranchIdentity {
        &self.identity
    }

    /// Returns the intrinsic branch index prefix associated with this workspace.
    pub fn branch_id(&self) -> BranchId {
        self.identity.id()
    }

    /// Returns the current commit handle if one exists.
    pub fn head(&self) -> Option<CommitHandle> {
        self.head
    }

    /// Returns the workspace metadata handle.
    pub fn metadata(&self) -> MetadataHandle {
        self.commit_metadata
    }

    /// Adds a blob to the workspace's local blob store.
    /// Mirrors [`BlobStorePut::put`](crate::repo::BlobStorePut) for ease of use.
    pub fn put<S, T>(&mut self, item: T) -> Inline<Handle<S>>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.staged.put(item).expect("infallible blob put")
    }

    /// Retrieves a blob from the workspace.
    ///
    /// The method first checks the workspace's local blob store and falls back
    /// to the base blob store if the blob is not found locally.
    pub fn get<T, S>(
        &mut self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, <Blobs::Reader as BlobStoreGet>::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.staged
            .reader()
            .unwrap()
            .get(handle)
            .or_else(|_| self.base_blobs.get(handle))
    }

    /// Performs a commit in the workspace.
    ///
    /// Accepts anything that converts into a [`Fragment`] — either a
    /// raw [`TribleSet`] (auto-promoted to a Fragment with empty blob
    /// store), or a Fragment built up via `entity!{}` /
    /// `MetaDescribe::describe()` whose embedded blobs get absorbed
    /// into `self.staged` alongside the commit-content blob.
    /// This method creates a new commit blob (stored in the local
    /// blobset) and updates the current commit handle.
    pub fn commit(&mut self, content_: impl Into<Fragment>, message_: &str) {
        self.commit_internal(content_.into(), Some(self.commit_metadata), Some(message_));
    }

    /// Like [`commit`](Self::commit) but attaches one-off metadata
    /// instead of the repository default.
    ///
    /// Accepts anything that converts into a [`Fragment`]: the
    /// fragment's embedded blobs are absorbed into the staged blob
    /// store and its facts are archived as a `SimpleArchive` blob
    /// whose handle lands in the commit's metadata slot. Archiving is
    /// content-addressed, so committing the same metadata fragment
    /// repeatedly converges on one blob — no caller-side caching
    /// needed for correctness or storage. When the metadata is
    /// already archived (e.g. shared across many commits and you hold
    /// its handle), use
    /// [`commit_with_metadata_handle`](Self::commit_with_metadata_handle)
    /// to skip re-serialization.
    pub fn commit_with_metadata(
        &mut self,
        content_: impl Into<Fragment>,
        metadata_: impl Into<Fragment>,
        message_: &str,
    ) {
        let (meta_facts, meta_blobs) = metadata_.into().into_facts_and_blobs();
        self.staged.union(meta_blobs);
        let metadata_handle = self.put(meta_facts);
        self.commit_internal(content_.into(), Some(metadata_handle), Some(message_));
    }

    /// Like [`commit`](Self::commit) but attaches an already-archived
    /// metadata handle instead of the repository default. The handle
    /// variant of [`commit_with_metadata`](Self::commit_with_metadata):
    /// no serialization happens, so this is the right form when the
    /// same metadata archive is shared across many commits.
    pub fn commit_with_metadata_handle(
        &mut self,
        content_: impl Into<Fragment>,
        metadata_: MetadataHandle,
        message_: &str,
    ) {
        self.commit_internal(content_.into(), Some(metadata_), Some(message_));
    }

    fn commit_internal(
        &mut self,
        content_: Fragment,
        metadata_handle: Option<MetadataHandle>,
        message_: Option<&str>,
    ) {
        let (content_facts, content_blobs) = content_.into_facts_and_blobs();
        // 0. Absorb any blobs the Fragment carried with it into the
        //    staging area before producing the commit blob, so handles
        //    inside `content_facts` resolve against `self.staged`.
        self.staged.union(content_blobs);
        // 1. Create a commit blob from the current head, content, metadata and the commit message.
        let content_blob: Blob<SimpleArchive> = content_facts.to_blob();
        // If a message is provided, store it as a LongString blob and pass the handle.
        let message_handle = message_.map(|m| self.put(m.to_string()));
        let parents = self.head.iter().copied();

        let commit_set = crate::repo::commit::commit_metadata(
            &self.signing_key,
            parents,
            message_handle,
            Some(content_blob.clone()),
            metadata_handle,
        );
        // 2. Store the content and commit blobs in `self.staged`.
        let _ = self
            .staged
            .put::<SimpleArchive, _>(content_blob)
            .expect("failed to put content blob");
        let commit_handle = self
            .staged
            .put(commit_set)
            .expect("failed to put commit blob");
        // 3. Update `self.head` to point to the new commit.
        self.head = Some(commit_handle);
    }

    /// Merge another workspace into this one.
    ///
    /// Always copies the *staged* blobs from `other.staged` into
    /// `self.staged` (so standalone blobs that aren't referenced by any
    /// commit chain still come along — useful when the other workspace was
    /// being used to stage content).
    ///
    /// Then integrates `other.head` via [`merge_commit`](Self::merge_commit),
    /// which picks no-op / fast-forward / merge commit as appropriate.
    ///
    /// Returns the workspace's new head, or `None` if both workspaces were
    /// empty (nothing to merge into anything).
    ///
    /// Notes:
    /// - The merge does *not* automatically import the entire base history
    ///   reachable from `other`'s head. Cross-repository callers must import
    ///   that closure explicitly (for example via
    ///   `repo::transfer(reachable(...))`) before ancestry can be classified.
    ///   Missing or malformed ancestry makes the merge fail loudly rather than
    ///   creating a commit with an unverified parent.
    pub fn merge(
        &mut self,
        other: &mut Workspace<Blobs>,
    ) -> Result<Option<CommitHandle>, MergeError> {
        // 1. Always transfer staged blobs from `other`. They may include
        //    standalone blobs (no commit referring to them yet) that the
        //    caller wanted to stash in the workspace independent of any
        //    branch state.
        let other_local = other.staged.reader().unwrap();
        for r in other_local.blobs() {
            let handle = r.expect("infallible blob enumeration");
            let blob: Blob<UnknownBlob> = other_local.get(handle).expect("infallible blob read");
            self.staged
                .put::<UnknownBlob, _>(blob)
                .expect("infallible blob put");
        }

        // 2. Integrate `other`'s head via the smart merge_commit. If `other`
        //    has no head, there's nothing further to integrate — just return
        //    our current head (which may or may not exist).
        match other.head {
            Some(other_head) => Ok(Some(self.merge_commit(other_head)?)),
            None => Ok(self.head),
        }
    }

    /// Integrate another commit into this workspace's history.
    ///
    /// Picks the cheapest correct strategy:
    ///
    /// - **No-op** if the workspace has no head and `other` *is* the head, or
    ///   if `other` is already in the current head's ancestry.
    /// - **Fast-forward** if the workspace has no head, or if the current head
    ///   is in `other`'s ancestry — `self.head` is set to `other` directly.
    /// - **Merge commit** otherwise — a new commit with `[current_head, other]`
    ///   as parents is created and `self.head` advances to it.
    ///
    /// Returns the workspace's new head in all cases.
    ///
    /// The ancestor checks are strict. If either history is missing or
    /// malformed in the workspace view, the function returns
    /// [`MergeError::AncestryWalkFailed`] and does not invent a divergent merge
    /// over an unverified parent. Callers that mirror remote chains must import
    /// the reachable closure (for example via `reachable` + `transfer`) first.
    pub fn merge_commit(
        &mut self,
        other: Inline<Handle<SimpleArchive>>,
    ) -> Result<CommitHandle, MergeError> {
        // Trivial cases first.
        let local_head = match self.head {
            None => {
                // No local head — fast-forward to `other`.
                self.head = Some(other);
                return Ok(other);
            }
            Some(h) if h == other => {
                // Identical — no-op.
                return Ok(h);
            }
            Some(h) => h,
        };

        // Walk both ancestry chains. If either walk fails because a commit
        // blob is missing locally, refuse to merge — falling through to a
        // divergent-merge here would write a new commit referencing an
        // unknown parent, which `pile diagnose check` would later report as
        // a chain break and which `fetch_reachable`'s Phase-1
        // `have_local` short-circuit would never re-fetch. Better to fail
        // loudly so the caller can re-sync the missing closure and retry.
        let remote_in_local = ancestors(local_head)
            .select(self)
            .map_err(|e| MergeError::AncestryWalkFailed(format!("walking local ancestry: {e:?}")))?
            .get(&other.raw)
            .is_some();
        if remote_in_local {
            // `other` is already in our history → no-op.
            return Ok(local_head);
        }

        let local_in_remote = ancestors(other)
            .select(self)
            .map_err(|e| MergeError::AncestryWalkFailed(format!("walking remote ancestry: {e:?}")))?
            .get(&local_head.raw)
            .is_some();
        if local_in_remote {
            // We're behind `other` → fast-forward.
            self.head = Some(other);
            return Ok(other);
        }

        // Truly divergent — create a merge commit.
        let parents = self.head.iter().copied().chain(Some(other));
        let merge_commit = crate::repo::commit::merge_metadata(parents);
        let commit_handle = self
            .staged
            .put(merge_commit)
            .expect("failed to put merge commit blob");
        self.head = Some(commit_handle);
        Ok(commit_handle)
    }

    /// Move the workspace's head to `commit` without creating a new commit.
    ///
    /// This is the "fast-forward" case: when the new commit is a descendant
    /// of (or equal to) the current head, you can advance directly without
    /// a merge commit. The caller is responsible for verifying the
    /// descendancy relationship — typically via [`ancestors`] over `commit`.
    ///
    /// Use this in pull/sync flows to avoid spurious merge commits when one
    /// peer is simply behind the other.
    pub fn set_head(&mut self, commit: CommitHandle) {
        self.head = Some(commit);
    }

    /// Returns the combined [`TribleSet`] for the specified commits.
    ///
    /// Each commit handle must reference a commit blob stored either in the
    /// workspace's local blob store or the repository's base store. The
    /// associated content blobs are loaded and unioned together. An error is
    /// returned if any commit or content blob is missing or malformed.
    fn checkout_commits<I>(
        &mut self,
        commits: I,
    ) -> Result<
        TribleSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        I: IntoIterator<Item = CommitHandle>,
    {
        let local = self.staged.reader().unwrap();
        let commits: Vec<CommitHandle> = commits.into_iter().collect();

        #[cfg(feature = "parallel")]
        {
            if commits.len() >= PARALLEL_CHECKOUT_THRESHOLD {
                use rayon::prelude::*;
                let base = self.base_blobs.clone();
                return commits
                    .into_par_iter()
                    .map_with(
                        (local, base),
                        |(local, base), commit| -> Result<TribleSet, _> {
                            let meta: TribleSet = local
                                .get(commit)
                                .or_else(|_| base.get(commit))
                                .map_err(WorkspaceCheckoutError::Storage)?;
                            let content_opt = match find!(
                                (c: Inline<_>),
                                pattern!(&meta, [{ content: ?c }])
                            )
                            .at_most_one()
                            {
                                Ok(Some((c,))) => Some(c),
                                Ok(None) => None,
                                Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                            };
                            if let Some(c) = content_opt {
                                let set: TribleSet = local
                                    .get(c)
                                    .or_else(|_| base.get(c))
                                    .map_err(WorkspaceCheckoutError::Storage)?;
                                Ok(set)
                            } else {
                                Ok(TribleSet::new())
                            }
                        },
                    )
                    .try_reduce(TribleSet::new, |a, b| Ok(a + b));
            }
        }

        let mut result = TribleSet::new();
        for commit in commits {
            let meta: TribleSet = local
                .get(commit)
                .or_else(|_| self.base_blobs.get(commit))
                .map_err(WorkspaceCheckoutError::Storage)?;

            // Some commits (for example merge commits) intentionally do not
            // carry a content blob. Treat those as no-ops during checkout so
            // callers can request ancestor ranges without failing when a
            // merge commit is encountered.
            let content_opt =
                match find!((c: Inline<_>), pattern!(&meta, [{ content: ?c }])).at_most_one() {
                    Ok(Some((c,))) => Some(c),
                    Ok(None) => None,
                    Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                };

            if let Some(c) = content_opt {
                let set: TribleSet = local
                    .get(c)
                    .or_else(|_| self.base_blobs.get(c))
                    .map_err(WorkspaceCheckoutError::Storage)?;
                result += set;
            } else {
                // No content for this commit (e.g. merge-only commit); skip it.
                continue;
            }
        }
        Ok(result)
    }

    fn checkout_commits_metadata<I>(
        &mut self,
        commits: I,
    ) -> Result<
        TribleSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        I: IntoIterator<Item = CommitHandle>,
    {
        let local = self.staged.reader().unwrap();
        let commits: Vec<CommitHandle> = commits.into_iter().collect();

        #[cfg(feature = "parallel")]
        {
            if commits.len() >= PARALLEL_CHECKOUT_THRESHOLD {
                use rayon::prelude::*;
                let base = self.base_blobs.clone();
                return commits
                    .into_par_iter()
                    .map_with(
                        (local, base),
                        |(local, base), commit| -> Result<TribleSet, _> {
                            let meta: TribleSet = local
                                .get(commit)
                                .or_else(|_| base.get(commit))
                                .map_err(WorkspaceCheckoutError::Storage)?;
                            let metadata_opt = match find!(
                                (c: Inline<_>),
                                pattern!(&meta, [{ metadata: ?c }])
                            )
                            .at_most_one()
                            {
                                Ok(Some((c,))) => Some(c),
                                Ok(None) => None,
                                Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                            };
                            if let Some(c) = metadata_opt {
                                let set: TribleSet = local
                                    .get(c)
                                    .or_else(|_| base.get(c))
                                    .map_err(WorkspaceCheckoutError::Storage)?;
                                Ok(set)
                            } else {
                                Ok(TribleSet::new())
                            }
                        },
                    )
                    .try_reduce(TribleSet::new, |a, b| Ok(a + b));
            }
        }

        let mut result = TribleSet::new();
        for commit in commits {
            let meta: TribleSet = local
                .get(commit)
                .or_else(|_| self.base_blobs.get(commit))
                .map_err(WorkspaceCheckoutError::Storage)?;

            let metadata_opt =
                match find!((c: Inline<_>), pattern!(&meta, [{ metadata: ?c }])).at_most_one() {
                    Ok(Some((c,))) => Some(c),
                    Ok(None) => None,
                    Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                };

            if let Some(c) = metadata_opt {
                let set: TribleSet = local
                    .get(c)
                    .or_else(|_| self.base_blobs.get(c))
                    .map_err(WorkspaceCheckoutError::Storage)?;
                result += set;
            }
        }
        Ok(result)
    }

    fn checkout_commits_with_metadata<I>(
        &mut self,
        commits: I,
    ) -> Result<
        (TribleSet, TribleSet),
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        I: IntoIterator<Item = CommitHandle>,
    {
        let local = self.staged.reader().unwrap();
        let commits: Vec<CommitHandle> = commits.into_iter().collect();

        #[cfg(feature = "parallel")]
        {
            if commits.len() >= PARALLEL_CHECKOUT_THRESHOLD {
                use rayon::prelude::*;
                let base = self.base_blobs.clone();
                return commits
                    .into_par_iter()
                    .map_with(
                        (local, base),
                        |(local, base), commit| -> Result<(TribleSet, TribleSet), _> {
                            let meta: TribleSet = local
                                .get(commit)
                                .or_else(|_| base.get(commit))
                                .map_err(WorkspaceCheckoutError::Storage)?;
                            let content_opt = match find!(
                                (c: Inline<_>),
                                pattern!(&meta, [{ content: ?c }])
                            )
                            .at_most_one()
                            {
                                Ok(Some((c,))) => Some(c),
                                Ok(None) => None,
                                Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                            };
                            let data_set = if let Some(c) = content_opt {
                                local
                                    .get(c)
                                    .or_else(|_| base.get(c))
                                    .map_err(WorkspaceCheckoutError::Storage)?
                            } else {
                                TribleSet::new()
                            };
                            let metadata_opt = match find!(
                                (c: Inline<_>),
                                pattern!(&meta, [{ metadata: ?c }])
                            )
                            .at_most_one()
                            {
                                Ok(Some((c,))) => Some(c),
                                Ok(None) => None,
                                Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                            };
                            let metadata_set = if let Some(c) = metadata_opt {
                                local
                                    .get(c)
                                    .or_else(|_| base.get(c))
                                    .map_err(WorkspaceCheckoutError::Storage)?
                            } else {
                                TribleSet::new()
                            };
                            Ok((data_set, metadata_set))
                        },
                    )
                    .try_reduce(
                        || (TribleSet::new(), TribleSet::new()),
                        |(a_data, a_meta), (b_data, b_meta)| Ok((a_data + b_data, a_meta + b_meta)),
                    );
            }
        }

        let mut data = TribleSet::new();
        let mut metadata_set = TribleSet::new();
        for commit in commits {
            let meta: TribleSet = local
                .get(commit)
                .or_else(|_| self.base_blobs.get(commit))
                .map_err(WorkspaceCheckoutError::Storage)?;

            let content_opt =
                match find!((c: Inline<_>), pattern!(&meta, [{ content: ?c }])).at_most_one() {
                    Ok(Some((c,))) => Some(c),
                    Ok(None) => None,
                    Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                };

            if let Some(c) = content_opt {
                let set: TribleSet = local
                    .get(c)
                    .or_else(|_| self.base_blobs.get(c))
                    .map_err(WorkspaceCheckoutError::Storage)?;
                data += set;
            }

            let metadata_opt =
                match find!((c: Inline<_>), pattern!(&meta, [{ metadata: ?c }])).at_most_one() {
                    Ok(Some((c,))) => Some(c),
                    Ok(None) => None,
                    Err(_) => return Err(WorkspaceCheckoutError::BadCommitMetadata()),
                };

            if let Some(c) = metadata_opt {
                let set: TribleSet = local
                    .get(c)
                    .or_else(|_| self.base_blobs.get(c))
                    .map_err(WorkspaceCheckoutError::Storage)?;
                metadata_set += set;
            }
        }
        Ok((data, metadata_set))
    }

    /// Returns the combined [`TribleSet`] for the specified commits or commit
    /// ranges. `spec` can be a single [`CommitHandle`], an iterator of handles
    /// or any of the standard range types over [`CommitHandle`].
    pub fn checkout<R>(
        &mut self,
        spec: R,
    ) -> Result<
        Checkout,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        R: CommitSelector<Blobs>,
    {
        let commits = spec.select(self)?;
        let facts = self.checkout_commits(commits.iter().map(|raw| Inline::new(*raw)))?;
        Ok(Checkout { facts, commits })
    }

    /// Returns the combined metadata [`TribleSet`] for the specified commits.
    /// Commits without metadata handles contribute an empty set.
    pub fn checkout_metadata<R>(
        &mut self,
        spec: R,
    ) -> Result<
        TribleSet,
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        R: CommitSelector<Blobs>,
    {
        let patch = spec.select(self)?;
        let commits = patch.iter().map(|raw| Inline::new(*raw));
        self.checkout_commits_metadata(commits)
    }

    /// Returns the combined data and metadata [`TribleSet`] for the specified commits.
    /// Metadata is loaded from each commit's `metadata` handle, when present.
    pub fn checkout_with_metadata<R>(
        &mut self,
        spec: R,
    ) -> Result<
        (TribleSet, TribleSet),
        WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
    >
    where
        R: CommitSelector<Blobs>,
    {
        let patch = spec.select(self)?;
        let commits = patch.iter().map(|raw| Inline::new(*raw));
        self.checkout_commits_with_metadata(commits)
    }
}

#[derive(Debug)]
pub enum WorkspaceCheckoutError<GetErr: Error> {
    /// Error retrieving blobs from storage.
    Storage(GetErr),
    /// Commit metadata is malformed or ambiguous.
    BadCommitMetadata(),
}

impl<E: Error + fmt::Debug> fmt::Display for WorkspaceCheckoutError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkspaceCheckoutError::Storage(e) => write!(f, "storage error: {e}"),
            WorkspaceCheckoutError::BadCommitMetadata() => {
                write!(f, "commit metadata malformed")
            }
        }
    }
}

impl<E: Error + fmt::Debug> Error for WorkspaceCheckoutError<E> {}

fn collect_reachable<Blobs: BlobStore>(
    ws: &mut Workspace<Blobs>,
    from: CommitHandle,
) -> Result<
    CommitSet,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
> {
    let mut visited = HashSet::new();
    let mut stack = vec![from];
    let mut result = CommitSet::new();

    while let Some(commit) = stack.pop() {
        if !visited.insert(commit) {
            continue;
        }
        result.insert(&Entry::new(&commit.raw));

        let meta: TribleSet = ws
            .staged
            .reader()
            .unwrap()
            .get(commit)
            .or_else(|_| ws.base_blobs.get(commit))
            .map_err(WorkspaceCheckoutError::Storage)?;

        for (p,) in find!((p: Inline<_>,), pattern!(&meta, [{ parent: ?p }])) {
            stack.push(p);
        }
    }

    Ok(result)
}
