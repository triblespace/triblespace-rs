#![allow(clippy::type_complexity)]
//! This module provides a high-level API for storing and retrieving data from repositories.
//! The design is inspired by Git, but with a focus on object/content-addressed storage.
//! It separates storage concerns from the data model, and reduces the mutable state of the repository,
//! to an absolute minimum, making it easier to reason about and allowing for different storage backends.
//!
//! Blob repositories are collections of blobs that can be content-addressed by their hash.
//! This is typically local `.pile` file or a S3 bucket or a similar service.
//! On their own they have no notion of branches or commits, or other stateful constructs.
//! As such they also don't have a notion of time, order or history,
//! massively relaxing the constraints on storage.
//! This makes it possible to use a wide range of storage services, including those that don't support
//! atomic transactions or have other limitations.
//!
//! Branch repositories on the other hand are a stateful construct that can be used to represent a branch pointing to a specific commit.
//! They are stored in a separate repository, typically a  local `.pile` file, a database or an S3 compatible service with a compare-and-swap operation,
//! and can be used to represent the state of a repository at a specific point in time.
//!
//! Technically, branches are just a mapping from a branch id to a blob hash,
//! But because TribleSets are themselves easily stored in a blob, and because
//! trible commit histories are an append-only chain of TribleSet metadata,
//! the hash of the head is sufficient to represent the entire history of a branch.
//!
//! ## Basic usage
//!
//! ```rust,ignore
//! use ed25519_dalek::SigningKey;
//! use rand::rngs::OsRng;
//! use triblespace::prelude::*;
//! use triblespace::prelude::valueschemas::{GenId, ShortString};
//! use triblespace::repo::{memoryrepo::MemoryRepo, Repository};
//!
//! let storage = MemoryRepo::default();
//! let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
//! let branch_id = repo.create_branch("main", None).expect("create branch");
//! let mut ws = repo.pull(*branch_id).expect("pull branch");
//!
//! attributes! {
//!     "8F180883F9FD5F787E9E0AF0DF5866B9" as pub author: GenId;
//!     "0DBB530B37B966D137C50B943700EDB2" as pub firstname: ShortString;
//!     "6BAA463FD4EAF45F6A103DB9433E4545" as pub lastname: ShortString;
//! }
//! let author = fucid();
//! ws.commit(
//!     entity!{ &author @
//!         literature::firstname: "Frank",
//!         literature::lastname: "Herbert",
//!      },
//!     "initial commit",
//! );
//!
//! // Single-attempt push: `try_push` uploads local blobs and attempts a
//! // single CAS update. On conflict it returns a workspace containing the
//! // new branch state which you should merge into before retrying.
//! match repo.try_push(&mut ws).expect("try_push") {
//!     None => {}
//!     Some(_) => panic!("unexpected conflict"),
//! }
//! ```
//!
//! `create_branch` registers a new branch and returns an [`ExclusiveId`](crate::id::ExclusiveId) guard.
//! `pull` creates a new workspace from an existing branch while
//! `branch_from` can be used to start a new branch from a specific commit
//! handle. See `examples/workspace.rs` for a more complete example.
//!
//! ## Handling conflicts
//!
//! The single-attempt primitive is [`Repository::try_push`](crate::repo::Repository::try_push). It returns
//! `Ok(None)` on success or `Ok(Some(conflict_ws))` when the branch advanced
//! concurrently. Callers that want explicit conflict handling may use this
//! form:
//!
//! ```rust,ignore
//! while let Some(mut other) = repo.try_push(&mut ws)? {
//!     // Merge our staged changes into the incoming workspace and retry.
//!     other.merge(&mut ws)?;
//!     ws = other;
//! }
//! ```
//!
//! For convenience `Repository::push` is provided as a retrying wrapper that
//! performs the merge-and-retry loop for you. Call `push` when you prefer the
//! repository to handle conflicts automatically; call `try_push` when you need
//! to inspect or control the intermediate conflict workspace yourself.
//!
//! `push` performs a compare‐and‐swap (CAS) update on the branch metadata.
//! This optimistic concurrency control keeps branches consistent without
//! locking and can be emulated by many storage systems (for example by
//! using conditional writes on S3).
//!
//! ## Git parallels
//!
//! The API deliberately mirrors concepts from Git to make its usage familiar:
//!
//! - A [`Repository`](crate::repo::Repository) stores commits and branch metadata similar to a remote.
//! - [`Workspace`](crate::repo::Workspace) is akin to a working directory combined with an index. It
//!   tracks changes against a branch head until you `push` them.
//! - `create_branch` and `branch_from` correspond to creating new branches from
//!   scratch or from a specific commit, respectively.
//! - `push` updates the repository atomically. If the branch advanced in the
//!   meantime, you receive a conflict workspace which can be merged before
//!   retrying the push.
//! - `pull` is similar to cloning a branch into a new workspace.
//!
//! `pull` uses the repository's default signing key for new commits. If you
//! need to work with a different identity, the `_with_key` variants allow providing
//! an explicit key when creating branches or pulling workspaces.
//!
//! These parallels should help readers leverage their Git knowledge when
//! working with trible repositories.
//!
/// Branch metadata construction and signature verification.
pub mod branch;
/// Capability-based authorization for triblespace networks.
pub mod capability;
/// Commit metadata construction and signature verification.
pub mod commit;
/// Storage adapter that delegates blobs and branches to separate backends.
pub mod hybridstore;
/// Fully in-memory repository implementation for tests and ephemeral use.
pub mod memoryrepo;
#[cfg(feature = "object-store")]
/// Repository backed by an `object_store`-compatible remote (S3, local FS, etc.).
pub mod objectstore;
/// Local file-based pile storage backend.
pub mod pile;

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

// Convenience impl for repositories whose storage supports explicit close.
impl<Storage> Repository<Storage>
where
    Storage: BlobStore + BranchStore + StorageClose,
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

use commit::commit_metadata;
use hifitime::Epoch;
use itertools::Itertools;

use crate::blob::schemas::simplearchive::UnarchiveError;
use crate::blob::schemas::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::MemoryBlobStore;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::find;
use crate::id::genid;
use crate::id::Id;
use crate::patch::Entry;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::prelude::valueschemas::GenId;
use crate::repo::branch::branch_metadata;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Handle;
use crate::value::Value;
use crate::value::ValueSchema;
use crate::value::VALUE_LEN;
use ed25519_dalek::SigningKey;

use crate::blob::schemas::longstring::LongString;
use crate::blob::schemas::simplearchive::SimpleArchive;
use crate::blob::schemas::succinctarchive::SuccinctArchiveBlob;
use crate::prelude::*;
use crate::value::schemas::ed25519 as ed;
use crate::value::schemas::hash::Blake3;
use crate::value::schemas::shortstring::ShortString;

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
    /// Optional SuccinctArchive rollup of the branch HEAD's logical contents.
    ///
    /// Readers can fetch this blob via the repository's blob store to obtain
    /// a compact, instantly-queryable representation of the branch's state
    /// without having to materialise the TribleSet from the commit chain.
    /// Absent on branches that haven't had a rollup built yet. Soft state:
    /// the rollup is redundant with (and must agree with) whatever
    /// `ws.checkout(..)` would return for the same HEAD.
    "D7D14C6737AA27A51E1E08D380D13EF9" as pub rollup: Handle<SuccinctArchiveBlob>;
}

/// The `ListBlobs` trait is used to list all blobs in a repository.
pub trait BlobStoreList {
    /// Iterator over blob handles in the store.
    type Iter<'a>: Iterator<Item = Result<Value<Handle<UnknownBlob>>, Self::Err>>
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
        handle: Value<Handle<S>>,
    ) -> Result<Option<BlobMetadata>, Self::MetaError>
    where
        S: BlobSchema + 'static,
        Handle<S>: ValueSchema;
}

/// Trait exposing a monotonic "forget" operation.
///
/// Forget is idempotent and monotonic: it removes materialization from a
/// particular repository but does not semantically delete derived facts.
pub trait BlobStoreForget {
    /// Error type for forget operations.
    type ForgetError: std::error::Error + Send + Sync + 'static;

    /// Removes the materialized blob identified by `handle` from this store.
    fn forget<S>(&mut self, handle: Value<Handle<S>>) -> Result<(), Self::ForgetError>
    where
        S: BlobSchema + 'static,
        Handle<S>: ValueSchema;
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
        handle: Value<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema + 'static,
        T: TryFromBlob<S>,
        Handle<S>: ValueSchema;
}

/// The `PutBlob` trait is used to store blobs in a repository.
pub trait BlobStorePut {
    /// Error type for put operations.
    type PutError: Error + Debug + Send + Sync + 'static;

    /// Serialises `item` as a blob, stores it, and returns its handle.
    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<S>: ValueSchema;
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
        I: IntoIterator<Item = Value<Handle<UnknownBlob>>>;
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
    fn children(
        &self,
        handle: Value<Handle<UnknownBlob>>,
    ) -> Vec<Value<Handle<UnknownBlob>>> {
        let Ok(blob) = self.get::<Blob<UnknownBlob>, UnknownBlob>(handle) else {
            return Vec::new();
        };
        let bytes = blob.bytes.as_ref();
        let mut result = Vec::new();
        let mut offset = 0usize;
        while offset + VALUE_LEN <= bytes.len() {
            let mut raw = [0u8; VALUE_LEN];
            raw.copy_from_slice(&bytes[offset..offset + VALUE_LEN]);
            let candidate = Value::<Handle<UnknownBlob>>::new(raw);
            if self.get::<anybytes::Bytes, UnknownBlob>(candidate).is_ok() {
                result.push(candidate);
            }
            offset += VALUE_LEN;
        }
        result
    }
}

// No blanket impl — types opt in explicitly so they can provide
// optimized implementations (e.g. network stores with batch protocols).
// Use `impl_blob_children_default!` for the scan-and-check fallback.

/// Outcome of a compare-and-swap branch update.
#[derive(Debug)]
pub enum PushResult {
    /// The CAS succeeded — the branch now points to the new commit.
    Success(),
    /// The CAS failed — the branch had advanced. Contains the current
    /// head, or `None` if the branch was deleted concurrently.
    Conflict(Option<Value<Handle<SimpleArchive>>>),
}

/// Storage backend for branch metadata (branch-id → commit-handle mapping).
///
/// This is the stateful counterpart to [`BlobStore`]: blob stores are
/// content-addressed and orderless, while branch stores track a single
/// mutable pointer per branch. The update operation uses compare-and-swap
/// semantics so multiple writers can coordinate without locks.
pub trait BranchStore {
    /// Error type for listing branches.
    type BranchesError: Error + Debug + Send + Sync + 'static;
    /// Error type for head lookups.
    type HeadError: Error + Debug + Send + Sync + 'static;
    /// Error type for CAS updates.
    type UpdateError: Error + Debug + Send + Sync + 'static;

    /// Iterator over branch IDs.
    type ListIter<'a>: Iterator<Item = Result<Id, Self::BranchesError>>
    where
        Self: 'a;

    /// Lists all branches in the repository.
    /// This function returns a stream of branch ids.
    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError>;

    // NOTE: keep the API lean — callers may call `branches()` and handle the
    // fallible iterator directly; we avoid adding an extra helper here.

    /// Retrieves a branch from the repository by its id.
    /// The id is a unique identifier for the branch, and is used to retrieve it from the repository.
    ///
    /// # Errors
    /// Returns an error if the branch could not be found in the repository.
    ///
    /// # Parameters
    /// * `id` - The id of the branch to retrieve.
    ///
    /// # Returns
    /// * A future that resolves to the handle of the branch.
    /// * The handle is a unique identifier for the branch, and is used to retrieve it from the repository.
    fn head(&mut self, id: Id) -> Result<Option<Value<Handle<SimpleArchive>>>, Self::HeadError>;

    /// Puts a branch on the repository, creating or updating it.
    ///
    /// # Parameters
    /// * `old` - Expected current value of the branch (None if creating new)
    /// * `new` - Value to update the branch to (None deletes the branch)
    ///
    /// # Returns
    /// * `Success` - Push completed successfully
    /// * `Conflict(current)` - Failed because the branch's current value doesn't match `old`
    ///   (contains the actual current value for conflict resolution)
    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<SimpleArchive>>>,
        new: Option<Value<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError>;
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
        (
            Value<Handle<UnknownBlob>>,
            Value<Handle<UnknownBlob>>,
        ),
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
    Handles: IntoIterator<Item = Value<Handle<UnknownBlob>>> + 'a,
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
    queue: VecDeque<Value<Handle<UnknownBlob>>>,
    visited: HashSet<[u8; VALUE_LEN]>,
}

impl<'a, BS> ReachableHandles<'a, BS>
where
    BS: BlobChildren,
{
    fn new(source: &'a BS, roots: impl IntoIterator<Item = Value<Handle<UnknownBlob>>>) -> Self {
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
    type Item = Value<Handle<UnknownBlob>>;

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
    roots: impl IntoIterator<Item = Value<Handle<UnknownBlob>>>,
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
) -> impl Iterator<Item = Value<Handle<UnknownBlob>>> + 'a {
    set.vae.iter().map(|raw| {
        let mut value = [0u8; VALUE_LEN];
        value.copy_from_slice(&raw[0..VALUE_LEN]);
        Value::<Handle<UnknownBlob>>::new(value)
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
}

/// Error returned by [`Repository::push`] and [`Repository::try_push`].
/// Error type for [`Repository::compute_rollup`].
#[derive(Debug)]
pub enum RollupError<Storage: BranchStore + BlobStore> {
    /// The branch was not found in the underlying storage.
    UnknownBranch,
    /// The branch is empty — no HEAD to roll up.
    EmptyBranch,
    /// The branch HEAD advanced between checkout and CAS-update. The
    /// caller may retry (`compute_rollup` is content-addressed so repeat
    /// calls dedupe against already-uploaded blobs).
    HeadAdvanced,
    /// Underlying push / storage error during the attach step.
    Push(PushError<Storage>),
    /// Could not pull the branch to obtain a workspace.
    Pull(PullError<Storage::HeadError,
                    <Storage as BlobStore>::ReaderError,
                    <<Storage as BlobStore>::Reader as BlobStoreGet>::GetError<UnarchiveError>>),
    /// Could not check out the branch state to build the archive.
    Checkout(WorkspaceCheckoutError<
        <<Storage as BlobStore>::Reader as BlobStoreGet>::GetError<UnarchiveError>>),
}

#[derive(Debug)]
pub enum PushError<Storage: BranchStore + BlobStore> {
    /// An error occurred while enumerating the branch storage branches.
    StorageBranches(Storage::BranchesError),
    /// An error occurred while creating a blob reader.
    StorageReader(<Storage as BlobStore>::ReaderError),
    /// An error occurred while reading metadata blobs.
    StorageGet(
        <<Storage as BlobStore>::Reader as BlobStoreGet>::GetError<UnarchiveError>,
    ),
    /// An error occurred while transferring blobs to the repository.
    StoragePut(<Storage as BlobStorePut>::PutError),
    /// An error occurred while updating the branch storage.
    BranchUpdate(Storage::UpdateError),
    /// Malformed branch metadata.
    BadBranchMetadata(),
    /// Merge failed while retrying a push.
    MergeError(MergeError),
}

// Allow using the `?` operator to convert MergeError into PushError in
// contexts where PushError is the function error type. This keeps call sites
// succinct by avoiding manual mapping closures like
// `.map_err(|e| PushError::MergeError(e))?`.
impl<Storage> From<MergeError> for PushError<Storage>
where
    Storage: BranchStore + BlobStore,
{
    fn from(e: MergeError) -> Self {
        PushError::MergeError(e)
    }
}

// Note: we intentionally avoid generic `From` impls for storage-associated
// error types because they can overlap with other blanket implementations
// and lead to coherence conflicts. Call sites use explicit mapping via the
// enum variant constructors (e.g. `map_err(PushError::StoragePut)`) where
// needed which keeps conversions explicit and stable.

/// Error returned by [`Repository::create_branch`] and related methods.
#[derive(Debug)]
pub enum BranchError<Storage>
where
    Storage: BranchStore + BlobStore,
{
    /// An error occurred while creating a blob reader.
    StorageReader(<Storage as BlobStore>::ReaderError),
    /// An error occurred while reading metadata blobs.
    StorageGet(
        <<Storage as BlobStore>::Reader as BlobStoreGet>::GetError<UnarchiveError>,
    ),
    /// An error occurred while storing blobs.
    StoragePut(<Storage as BlobStorePut>::PutError),
    /// An error occurred while retrieving branch heads.
    BranchHead(Storage::HeadError),
    /// An error occurred while updating the branch storage.
    BranchUpdate(Storage::UpdateError),
    /// The branch already exists.
    AlreadyExists(),
    /// The referenced base branch does not exist.
    BranchNotFound(Id),
}

/// Error returned by [`Repository::lookup_branch`].
#[derive(Debug)]
pub enum LookupError<Storage>
where
    Storage: BranchStore + BlobStore,
{
    /// Failed to enumerate branches.
    StorageBranches(Storage::BranchesError),
    /// Failed to read a branch head.
    BranchHead(Storage::HeadError),
    /// Failed to create a blob reader.
    StorageReader(<Storage as BlobStore>::ReaderError),
    /// Failed to read a metadata blob.
    StorageGet(
        <<Storage as BlobStore>::Reader as BlobStoreGet>::GetError<UnarchiveError>,
    ),
    /// Multiple branches were found with the given name.
    NameConflict(Vec<Id>),
    /// Branch metadata is malformed.
    BadBranchMetadata(),
}

/// Error returned by [`Repository::ensure_branch`].
#[derive(Debug)]
pub enum EnsureBranchError<Storage>
where
    Storage: BranchStore + BlobStore,
{
    /// Failed to look up the branch.
    Lookup(LookupError<Storage>),
    /// Failed to create the branch.
    Create(BranchError<Storage>),
}

/// High-level wrapper combining a blob store and branch store into a usable
/// repository API.
///
/// The [`Repository`] type exposes convenience methods for creating branches,
/// committing data and pushing changes while delegating actual storage to the
/// given [`BlobStore`] and [`BranchStore`] implementations.
pub struct Repository<Storage: BlobStore + BranchStore> {
    storage: Storage,
    signing_key: SigningKey,
    commit_metadata: MetadataHandle,
}

/// Error returned by [`Repository::pull`].
pub enum PullError<BranchStorageErr, BlobReaderErr, BlobStorageErr>
where
    BranchStorageErr: Error,
    BlobReaderErr: Error,
    BlobStorageErr: Error,
{
    /// The branch does not exist in the repository.
    BranchNotFound(Id),
    /// An error occurred while accessing the branch storage.
    BranchStorage(BranchStorageErr),
    /// An error occurred while creating a blob reader.
    BlobReader(BlobReaderErr),
    /// An error occurred while accessing the blob storage.
    BlobStorage(BlobStorageErr),
    /// The branch metadata is malformed or does not contain the expected fields.
    BadBranchMetadata(),
}

impl<B, R, C> fmt::Debug for PullError<B, R, C>
where
    B: Error + fmt::Debug,
    R: Error + fmt::Debug,
    C: Error + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PullError::BranchNotFound(id) => f.debug_tuple("BranchNotFound").field(id).finish(),
            PullError::BranchStorage(e) => f.debug_tuple("BranchStorage").field(e).finish(),
            PullError::BlobReader(e) => f.debug_tuple("BlobReader").field(e).finish(),
            PullError::BlobStorage(e) => f.debug_tuple("BlobStorage").field(e).finish(),
            PullError::BadBranchMetadata() => f.debug_tuple("BadBranchMetadata").finish(),
        }
    }
}

impl<Storage> Repository<Storage>
where
    Storage: BlobStore + BranchStore,
{
    /// Creates a new repository with the given storage, signing key, and commit metadata.
    ///
    /// The `commit_metadata` TribleSet is stored as a blob in the repository and attached
    /// to every commit created through this repository's workspaces.
    pub fn new(
        mut storage: Storage,
        signing_key: SigningKey,
        commit_metadata: TribleSet,
    ) -> Result<Self, <Storage as BlobStorePut>::PutError> {
        let commit_metadata = storage.put(commit_metadata)?;
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

    /// Replace the repository signing key.
    pub fn set_signing_key(&mut self, signing_key: SigningKey) {
        self.signing_key = signing_key;
    }

    /// Returns the repository commit metadata handle.
    pub fn commit_metadata(&self) -> MetadataHandle {
        self.commit_metadata
    }

    /// Initializes a new branch in the repository.
    /// Branches are the only mutable state in the repository,
    /// and are used to represent the state of a commit chain at a specific point in time.
    /// A branch must always point to a commit, and this function can be used to create a new branch.
    ///
    /// Creates a new branch in the repository.
    /// This branch is a pointer to a specific commit in the repository.
    /// The branch is created with name and is initialized to point to the opionally given commit.
    /// The branch is signed by the branch signing key.
    ///
    /// # Parameters
    /// * `branch_name` - Name of the new branch.
    /// * `commit` - Commit to initialize the branch from.
    pub fn create_branch(
        &mut self,
        branch_name: &str,
        commit: Option<CommitHandle>,
    ) -> Result<ExclusiveId, BranchError<Storage>> {
        self.create_branch_with_key(branch_name, commit, self.signing_key.clone())
    }

    /// Same as [`Self::create_branch`] but uses the provided signing key.
    pub fn create_branch_with_key(
        &mut self,
        branch_name: &str,
        commit: Option<CommitHandle>,
        signing_key: SigningKey,
    ) -> Result<ExclusiveId, BranchError<Storage>> {
        let branch_id = genid();
        let name_blob = branch_name.to_owned().to_blob();
        let name_handle = name_blob.get_handle();
        self.storage
            .put(name_blob)
            .map_err(|e| BranchError::StoragePut(e))?;

        let branch_set = if let Some(commit) = commit {
            let reader = self
                .storage
                .reader()
                .map_err(|e| BranchError::StorageReader(e))?;
            let set: TribleSet = reader.get(commit).map_err(|e| BranchError::StorageGet(e))?;

            branch::branch_metadata(
                &signing_key,
                *branch_id,
                name_handle,
                Some(set.to_blob()),
                None,
            )
        } else {
            branch::branch_unsigned(*branch_id, name_handle, None, None)
        };

        let branch_blob = branch_set.to_blob();
        let branch_handle = self
            .storage
            .put(branch_blob)
            .map_err(|e| BranchError::StoragePut(e))?;
        let push_result = self
            .storage
            .update(*branch_id, None, Some(branch_handle))
            .map_err(|e| BranchError::BranchUpdate(e))?;

        match push_result {
            PushResult::Success() => Ok(branch_id),
            PushResult::Conflict(_) => Err(BranchError::AlreadyExists()),
        }
    }

    /// Look up a branch by name.
    ///
    /// Iterates all branches, reads each one's metadata, and returns the ID
    /// of the branch whose name matches. Returns `Ok(None)` if no branch has
    /// that name, or `LookupError::NameConflict` if multiple branches share it.
    pub fn lookup_branch(&mut self, name: &str) -> Result<Option<Id>, LookupError<Storage>> {
        let branch_ids: Vec<Id> = self
            .storage
            .branches()
            .map_err(LookupError::StorageBranches)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(LookupError::StorageBranches)?;

        let mut matches = Vec::new();

        for branch_id in branch_ids {
            let Some(meta_handle) = self
                .storage
                .head(branch_id)
                .map_err(LookupError::BranchHead)?
            else {
                continue;
            };

            let reader = self.storage.reader().map_err(LookupError::StorageReader)?;
            let meta_set: TribleSet = reader.get(meta_handle).map_err(LookupError::StorageGet)?;

            let Ok((name_handle,)) = find!(
                (n: Value<Handle<LongString>>),
                pattern!(&meta_set, [{ crate::metadata::name: ?n }])
            )
            .exactly_one() else {
                continue;
            };

            let Ok(branch_name): Result<anybytes::View<str>, _> = reader.get(name_handle) else {
                continue;
            };

            if branch_name.as_ref() == name {
                matches.push(branch_id);
            }
        }

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches[0])),
            _ => Err(LookupError::NameConflict(matches)),
        }
    }

    /// Ensure a branch with the given name exists, creating it if necessary.
    ///
    /// If a branch named `name` already exists, returns its ID.
    /// If no such branch exists, creates a new one (optionally from the given
    /// commit) and returns its ID.
    ///
    /// Errors if multiple branches share the same name (ambiguous).
    pub fn ensure_branch(
        &mut self,
        name: &str,
        commit: Option<CommitHandle>,
    ) -> Result<Id, EnsureBranchError<Storage>> {
        match self
            .lookup_branch(name)
            .map_err(EnsureBranchError::Lookup)?
        {
            Some(id) => Ok(id),
            None => {
                let id = self
                    .create_branch(name, commit)
                    .map_err(EnsureBranchError::Create)?;
                Ok(*id)
            }
        }
    }

    /// Pulls an existing branch using the repository's signing key.
    /// The workspace inherits the repository default metadata if configured.
    pub fn pull(
        &mut self,
        branch_id: Id,
    ) -> Result<
        Workspace<Storage>,
        PullError<
            Storage::HeadError,
            Storage::ReaderError,
            <Storage::Reader as BlobStoreGet>::GetError<UnarchiveError>,
        >,
    > {
        self.pull_with_key(branch_id, self.signing_key.clone())
    }

    /// Same as [`Self::pull`] but overrides the signing key.
    pub fn pull_with_key(
        &mut self,
        branch_id: Id,
        signing_key: SigningKey,
    ) -> Result<
        Workspace<Storage>,
        PullError<
            Storage::HeadError,
            Storage::ReaderError,
            <Storage::Reader as BlobStoreGet>::GetError<UnarchiveError>,
        >,
    > {
        // 1. Get the branch metadata head from the branch store.
        let base_branch_meta_handle = match self.storage.head(branch_id) {
            Ok(Some(handle)) => handle,
            Ok(None) => return Err(PullError::BranchNotFound(branch_id)),
            Err(e) => return Err(PullError::BranchStorage(e)),
        };
        // 2. Get the current commit from the branch metadata.
        let reader = self.storage.reader().map_err(PullError::BlobReader)?;
        let base_branch_meta: TribleSet = match reader.get(base_branch_meta_handle) {
            Ok(meta_set) => meta_set,
            Err(e) => return Err(PullError::BlobStorage(e)),
        };

        let head_ = match find!(
            (head_: Value<_>),
            pattern!(&base_branch_meta, [{ head: ?head_ }])
        )
        .at_most_one()
        {
            Ok(Some((h,))) => Some(h),
            Ok(None) => None,
            Err(_) => return Err(PullError::BadBranchMetadata()),
        };
        // Create workspace with the current commit and base blobs.
        let base_blobs = self.storage.reader().map_err(PullError::BlobReader)?;
        Ok(Workspace {
            base_blobs,
            staged: MemoryBlobStore::new(),
            head: head_,
            base_head: head_,
            base_branch_id: branch_id,
            base_branch_meta: base_branch_meta_handle,
            signing_key,
            commit_metadata: self.commit_metadata,
        })
    }

    /// Pushes the workspace's new blobs and commit to the persistent repository.
    /// This syncs the local BlobSet with the repository's BlobStore and performs
    /// an atomic branch update (using the stored base_branch_meta).
    pub fn push(&mut self, workspace: &mut Workspace<Storage>) -> Result<(), PushError<Storage>> {
        // Retrying push: attempt a single push and, on conflict, merge the
        // local workspace into the returned conflict workspace and retry.
        // This implements the common push-merge-retry loop as a convenience
        // wrapper around `try_push`.
        while let Some(mut conflict_ws) = self.try_push(workspace)? {
            // Keep the previous merge order: merge the caller's staged
            // changes into the incoming conflict workspace. This preserves
            // the semantic ordering of parents used in the merge commit.
            conflict_ws.merge(workspace)?;

            // Move the merged incoming workspace into the caller's workspace
            // so the next try_push operates against the fresh branch state.
            // Using assignment here is equivalent to `swap` but avoids
            // retaining the previous `workspace` contents in the temp var.
            *workspace = conflict_ws;
        }

        Ok(())
    }

    /// Single-attempt push: upload local blobs and try to update the branch
    /// head once. Returns `Ok(None)` on success, or `Ok(Some(conflict_ws))`
    /// when the branch was updated concurrently and the caller should merge.
    pub fn try_push(
        &mut self,
        workspace: &mut Workspace<Storage>,
    ) -> Result<Option<Workspace<Storage>>, PushError<Storage>> {
        // 1. Sync `workspace.staged` to repository's BlobStore.
        let workspace_reader = workspace.staged.reader().unwrap();
        for handle in workspace_reader.blobs() {
            let handle = handle.expect("infallible blob enumeration");
            let blob: Blob<UnknownBlob> =
                workspace_reader.get(handle).expect("infallible blob read");
            self.storage.put(blob).map_err(PushError::StoragePut)?;
        }

        // 1.5 If the workspace's head did not change since the workspace was
        // created, there's no commit to reference and therefore no branch
        // metadata update is required. This avoids touching the branch store
        // in the common case where only blobs were staged or nothing changed.
        if workspace.base_head == workspace.head {
            return Ok(None);
        }

        // 2. Create a new branch meta blob referencing the new workspace head.
        let repo_reader = self.storage.reader().map_err(PushError::StorageReader)?;
        let base_branch_meta: TribleSet = repo_reader
            .get(workspace.base_branch_meta)
            .map_err(PushError::StorageGet)?;

        let Ok((branch_name,)) = find!(
            (name: Value<Handle<LongString>>),
            pattern!(base_branch_meta, [{ crate::metadata::name: ?name }])
        )
        .exactly_one() else {
            return Err(PushError::BadBranchMetadata());
        };

        let head_handle = workspace.head.ok_or(PushError::BadBranchMetadata())?;
        let head_: TribleSet = repo_reader
            .get(head_handle)
            .map_err(PushError::StorageGet)?;

        let branch_meta = branch_metadata(
            &workspace.signing_key,
            workspace.base_branch_id,
            branch_name,
            Some(head_.to_blob()),
            // A fresh commit invalidates any prior rollup (it was computed
            // against the old HEAD). Readers fall back to checkout until
            // `compute_rollup` runs against the new HEAD.
            None,
        );

        let branch_meta_handle = self
            .storage
            .put(branch_meta)
            .map_err(PushError::StoragePut)?;

        // 3. Use CAS (comparing against workspace.base_branch_meta) to update the branch pointer.
        let result = self
            .storage
            .update(
                workspace.base_branch_id,
                Some(workspace.base_branch_meta),
                Some(branch_meta_handle),
            )
            .map_err(PushError::BranchUpdate)?;

        match result {
            PushResult::Success() => {
                // Update workspace base pointers so subsequent pushes can detect
                // that the workspace is already synchronized and avoid re-upload.
                workspace.base_branch_meta = branch_meta_handle;
                workspace.base_head = workspace.head;
                // Refresh the workspace base blob reader to ensure newly
                // uploaded blobs are visible to subsequent checkout operations.
                workspace.base_blobs = self.storage.reader().map_err(PushError::StorageReader)?;
                // Clear staged local blobs now that they have been uploaded and
                // the branch metadata updated. This frees memory and prevents
                // repeated uploads of the same staged blobs on subsequent pushes.
                workspace.staged = MemoryBlobStore::new();
                Ok(None)
            }
            PushResult::Conflict(conflicting_meta) => {
                let conflicting_meta = conflicting_meta.ok_or(PushError::BadBranchMetadata())?;

                let repo_reader = self.storage.reader().map_err(PushError::StorageReader)?;
                let branch_meta: TribleSet = repo_reader
                    .get(conflicting_meta)
                    .map_err(PushError::StorageGet)?;

                let head_ = match find!((head_: Value<_>),
                    pattern!(&branch_meta, [{ head: ?head_ }])
                )
                .at_most_one()
                {
                    Ok(Some((h,))) => Some(h),
                    Ok(None) => None,
                    Err(_) => return Err(PushError::BadBranchMetadata()),
                };

                let conflict_ws = Workspace {
                    base_blobs: self.storage.reader().map_err(PushError::StorageReader)?,
                    staged: MemoryBlobStore::new(),
                    head: head_,
                    base_head: head_,
                    base_branch_id: workspace.base_branch_id,
                    base_branch_meta: conflicting_meta,
                    signing_key: workspace.signing_key.clone(),
                    commit_metadata: workspace.commit_metadata,
                };

                Ok(Some(conflict_ws))
            }
        }
    }

    /// Builds a [`SuccinctArchive`](crate::blob::schemas::succinctarchive::SuccinctArchive) rollup of the branch's current HEAD,
    /// stores it as a blob in the underlying storage, and attaches the
    /// resulting handle to the branch metadata via CAS.
    ///
    /// Returns the new rollup handle on success.
    ///
    /// Returns [`RollupError::HeadAdvanced`] if the branch HEAD moved
    /// between `pull` and the CAS-update. The caller may retry — the
    /// archive blob is content-addressed, so subsequent calls dedupe
    /// against already-uploaded blobs.
    ///
    /// Returns [`RollupError::EmptyBranch`] if the branch has no HEAD
    /// commit yet (nothing to roll up).
    ///
    /// This is the sole public write-path for rollups. The companion
    /// read-path is [`Workspace::rollup`].
    pub fn compute_rollup(
        &mut self,
        branch_id: Id,
    ) -> Result<
        Value<Handle<crate::blob::schemas::succinctarchive::SuccinctArchiveBlob>>,
        RollupError<Storage>,
    > {
        use crate::blob::schemas::succinctarchive::{OrderedUniverse, SuccinctArchive};
        use crate::blob::ToBlob;

        let mut ws = self.pull(branch_id).map_err(RollupError::Pull)?;
        let head_handle = ws.head().ok_or(RollupError::EmptyBranch)?;

        // Materialise the branch state from its commit chain and build the
        // succinct index over it.
        let space = ws.checkout(..).map_err(RollupError::Checkout)?;
        let archive: SuccinctArchive<OrderedUniverse> = (&*space).into();
        drop(space);

        // Upload the archive blob directly to storage — no workspace-local
        // staging needed; the CAS below references it by handle.
        let archive_blob = (&archive).to_blob();
        let handle: Value<
            Handle<crate::blob::schemas::succinctarchive::SuccinctArchiveBlob>,
        > = self
            .storage
            .put(archive_blob)
            .map_err(|e| RollupError::Push(PushError::StoragePut(e)))?;

        // Construct a fresh branch meta that carries the same head as
        // `base_branch_meta` plus the new rollup attribute.
        let reader = self
            .storage
            .reader()
            .map_err(|e| RollupError::Push(PushError::StorageReader(e)))?;
        let base_meta: TribleSet = reader
            .get(ws.base_branch_meta)
            .map_err(|e| RollupError::Push(PushError::StorageGet(e)))?;
        let (branch_name,) = find!(
            (name: Value<Handle<LongString>>),
            pattern!(&base_meta, [{ crate::metadata::name: ?name }])
        )
        .exactly_one()
        .map_err(|_| RollupError::Push(PushError::BadBranchMetadata()))?;
        let head_blob: TribleSet = reader
            .get(head_handle)
            .map_err(|e| RollupError::Push(PushError::StorageGet(e)))?;

        let new_meta = branch::branch_metadata(
            &ws.signing_key,
            branch_id,
            branch_name,
            Some(head_blob.to_blob()),
            Some(handle),
        );
        let new_meta_handle = self
            .storage
            .put(new_meta)
            .map_err(|e| RollupError::Push(PushError::StoragePut(e)))?;

        // CAS: swap `base_branch_meta` for the new meta. On conflict, the
        // head advanced between our pull and this CAS — the rollup we built
        // is stale against the new head, so report upstream.
        let update_result = self
            .storage
            .update(branch_id, Some(ws.base_branch_meta), Some(new_meta_handle))
            .map_err(|e| RollupError::Push(PushError::BranchUpdate(e)))?;
        match update_result {
            PushResult::Success() => Ok(handle),
            PushResult::Conflict(_) => Err(RollupError::HeadAdvanced),
        }
    }
}

/// A handle to a commit blob in the repository.
pub type CommitHandle = Value<Handle<SimpleArchive>>;
type MetadataHandle = Value<Handle<SimpleArchive>>;
/// A set of commit handles, used by [`CommitSelector`] and [`Checkout`].
pub type CommitSet = PATCH<VALUE_LEN, IdentitySchema, ()>;
type BranchMetaHandle = Value<Handle<SimpleArchive>>;

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
/// let mut changed = repo.pull(branch_id)?.checkout(..)?;
/// let mut full = changed.facts().clone();
///
/// loop {
///     // full already includes changed
///     for result in pattern_changes!(&full, &changed, [{ ... }]) {
///         // process new results
///     }
///
///     // Advance — exclude exactly the commits we already processed.
///     changed = repo.pull(branch_id)?.checkout(changed.commits()..)?;
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
    /// The branch id this workspace is tracking; None for a detached workspace.
    base_branch_id: Id,
    /// The meta-handle corresponding to the base branch state used for CAS.
    base_branch_meta: BranchMetaHandle,
    /// Handle to the current commit in the working branch. `None` for an empty branch.
    head: Option<CommitHandle>,
    /// The branch head snapshot when this workspace was created (pull time).
    ///
    /// This allows `try_push` to cheaply detect whether the commit head has
    /// advanced since the workspace was created without querying the remote
    /// branch store.
    base_head: Option<CommitHandle>,
    /// Signing key used for commit/branch signing.
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
            .field("base_branch_id", &self.base_branch_id)
            .field("base_branch_meta", &self.base_branch_meta)
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
            let keys: Vec<[u8; VALUE_LEN]> = frontier.iter().copied().collect();
            let mut next_frontier = CommitSet::new();
            for raw in keys {
                let handle = CommitHandle::new(raw);
                let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;
                for (p,) in find!((p: Value<_>), pattern!(&meta, [{ parent: ?p }])) {
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
            let handle = Value::new(*raw);
            let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;
            for (p,) in find!((p: Value<_>), pattern!(&meta, [{ parent: ?p }])) {
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
            let handle = Value::new(*raw);
            let meta: TribleSet = ws.get(handle).map_err(WorkspaceCheckoutError::Storage)?;

            let Ok((content_handle,)) = find!(
                (c: Value<_>),
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

fn collect_reachable_from_patch<Blobs: BlobStore>(
    ws: &mut Workspace<Blobs>,
    patch: CommitSet,
) -> Result<
    CommitSet,
    WorkspaceCheckoutError<<Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>>,
> {
    let mut result = CommitSet::new();
    for raw in patch.iter() {
        let handle = Value::new(*raw);
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
    let mut stack: Vec<CommitHandle> = seeds.iter().map(|raw| Value::new(*raw)).collect();
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

        for (p,) in find!((p: Value<_>,), pattern!(&meta, [{ parent: ?p }])) {
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

impl<Blobs: BlobStore> Workspace<Blobs> {
    /// Returns the branch id associated with this workspace.
    pub fn branch_id(&self) -> Id {
        self.base_branch_id
    }

    /// Returns the current commit handle if one exists.
    pub fn head(&self) -> Option<CommitHandle> {
        self.head
    }

    /// Returns the workspace metadata handle.
    pub fn metadata(&self) -> MetadataHandle {
        self.commit_metadata
    }

    /// Reads the rollup handle, if any, from the workspace's base branch
    /// metadata. Returns `None` if the branch has no rollup yet or if the
    /// metadata is missing the attribute. Readers can use this to fetch
    /// the archive blob directly and skip `checkout(..)` for warm queries:
    ///
    /// ```rust,ignore
    /// let mut ws = repo.pull(branch)?;
    /// match ws.rollup()? {
    ///     Some(h) => {
    ///         let archive: SuccinctArchive<_> = ws.get(h)?;
    ///         // query archive
    ///     }
    ///     None => {
    ///         let space = ws.checkout(..)?;
    ///         // query space (commit-chain materialisation)
    ///     }
    /// }
    /// ```
    ///
    /// Writers don't go through this — attach a rollup via
    /// [`Repository::compute_rollup`] instead.
    pub fn rollup(
        &mut self,
    ) -> Result<
        Option<Value<Handle<SuccinctArchiveBlob>>>,
        <Blobs::Reader as BlobStoreGet>::GetError<UnarchiveError>,
    > {
        let base_meta: TribleSet = self.base_blobs.get(self.base_branch_meta)?;
        Ok(
            find!(
                (r: Value<Handle<SuccinctArchiveBlob>>),
                pattern!(&base_meta, [{ rollup: ?r }])
            )
            .next()
            .map(|(r,)| r),
        )
    }

    /// Adds a blob to the workspace's local blob store.
    /// Mirrors [`BlobStorePut::put`](crate::repo::BlobStorePut) for ease of use.
    pub fn put<S, T>(&mut self, item: T) -> Value<Handle<S>>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<S>: ValueSchema,
    {
        self.staged.put(item).expect("infallible blob put")
    }


    /// Retrieves a blob from the workspace.
    ///
    /// The method first checks the workspace's local blob store and falls back
    /// to the base blob store if the blob is not found locally.
    pub fn get<T, S>(
        &mut self,
        handle: Value<Handle<S>>,
    ) -> Result<T, <Blobs::Reader as BlobStoreGet>::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema + 'static,
        T: TryFromBlob<S>,
        Handle<S>: ValueSchema,
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

    /// Like [`commit`](Self::commit) but attaches a one-off metadata handle
    /// instead of the repository default.
    pub fn commit_with_metadata(
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
        let content_blob = content_facts.to_blob();
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
            .put(content_blob)
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
    ///   reachable from `other`'s head. If the incoming parent commits
    ///   reference blobs that do not exist in this repository's storage,
    ///   reading those commits later will fail until the missing blobs are
    ///   explicitly imported (for example via `repo::transfer(reachable(...))`).
    /// - This design keeps merge permissive and leaves cross-repository blob
    ///   import as an explicit user action.
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
            self.staged.put(blob).expect("infallible blob put");
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
    /// The ancestor checks are best-effort: if the relevant commit blobs are
    /// missing from the workspace's view, the function falls through to the
    /// always-correct merge-commit path. Callers that mirror remote chains
    /// should ensure reachable blobs were imported (e.g. via `reachable` +
    /// `transfer`) for the optimization to kick in.
    pub fn merge_commit(
        &mut self,
        other: Value<Handle<SimpleArchive>>,
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

        // Best-effort ancestry checks. If the walks fail (missing blobs,
        // unreadable metadata), fall through to the always-correct merge.
        let remote_in_local = ancestors(local_head)
            .select(self)
            .ok()
            .map(|set| set.get(&other.raw).is_some())
            .unwrap_or(false);
        if remote_in_local {
            // `other` is already in our history → no-op.
            return Ok(local_head);
        }

        let local_in_remote = ancestors(other)
            .select(self)
            .ok()
            .map(|set| set.get(&local_head.raw).is_some())
            .unwrap_or(false);
        if local_in_remote {
            // We're behind `other` → fast-forward.
            self.head = Some(other);
            return Ok(other);
        }

        // Truly divergent — create a merge commit.
        let parents = self.head.iter().copied().chain(Some(other));
        let merge_commit = commit_metadata(&self.signing_key, parents, None, None, None);
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
                match find!((c: Value<_>), pattern!(&meta, [{ content: ?c }])).at_most_one() {
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
        let mut result = TribleSet::new();
        for commit in commits {
            let meta: TribleSet = local
                .get(commit)
                .or_else(|_| self.base_blobs.get(commit))
                .map_err(WorkspaceCheckoutError::Storage)?;

            let metadata_opt =
                match find!((c: Value<_>), pattern!(&meta, [{ metadata: ?c }])).at_most_one() {
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
        let mut data = TribleSet::new();
        let mut metadata_set = TribleSet::new();
        for commit in commits {
            let meta: TribleSet = local
                .get(commit)
                .or_else(|_| self.base_blobs.get(commit))
                .map_err(WorkspaceCheckoutError::Storage)?;

            let content_opt =
                match find!((c: Value<_>), pattern!(&meta, [{ content: ?c }])).at_most_one() {
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
                match find!((c: Value<_>), pattern!(&meta, [{ metadata: ?c }])).at_most_one() {
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
        let facts = self.checkout_commits(commits.iter().map(|raw| Value::new(*raw)))?;
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
        let commits = patch.iter().map(|raw| Value::new(*raw));
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
        let commits = patch.iter().map(|raw| Value::new(*raw));
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

        for (p,) in find!((p: Value<_>,), pattern!(&meta, [{ parent: ?p }])) {
            stack.push(p);
        }
    }

    Ok(result)
}
