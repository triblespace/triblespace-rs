use std::array::TryFromSliceError;
use std::convert::Infallible;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use anybytes::Bytes;
use futures::StreamExt;

use object_store::parse_url;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::UpdateVersion;
use object_store::{self};
use url::Url;

use hex::FromHex;

use crate::blob::encodings::simplearchive::UnarchiveError;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::id::Id;
use crate::id::RawId;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::prelude::blobencodings::SimpleArchive;
use crate::trible::TribleSet;

use super::async_store::{
    AsyncBlobStore, AsyncBlobStoreForget, AsyncBlobStoreGet, AsyncBlobStoreList,
    AsyncBlobStoreMeta, AsyncBlobStorePut, AsyncPartialCommitDag, AsyncPinStore,
};
use super::branch_frontier::ParentLookup;
use super::commit::{self, StoredCommitError};
use super::BlobMetadata;
use super::PushResult;

const LOCAL_PIN_INFIX: &str = "pins";
const BLOB_INFIX: &str = "blobs";

/// Blob and replica-local pin storage backed by an [`object_store`]
/// compatible backend.
///
/// All data is stored in an external service (e.g. S3, local filesystem)
/// via the `object_store` crate, which is async at its core — so this
/// type is **async-native**: it implements the
/// [`AsyncBlobStore`] family
/// directly, awaiting each operation, with no owned runtime.
/// It deliberately does not implement branch-assertion storage or a crash
/// durability barrier, so it is not by itself a StrongPin repository backend.
///
/// Synchronous callers wrap it in
/// [`Blocking`](super::async_store::Blocking), which carries the single
/// `block_on` boundary:
///
/// ```no_run
/// # use url::Url;
/// # use triblespace_core::repo::objectstore::ObjectStoreRemote;
/// # use triblespace_core::repo::async_store::Blocking;
/// # fn f(url: &Url) -> Result<(), Box<dyn std::error::Error>> {
/// let remote = ObjectStoreRemote::with_url(url)?;
/// let mut store = Blocking::new(remote)?; // now a plain sync BlobStore
/// # let _ = &mut store;
/// # Ok(())
/// # }
/// ```
pub struct ObjectStoreRemote {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl fmt::Debug for ObjectStoreRemote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreRemote")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl fmt::Debug for ObjectStoreReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreReader")
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// Read-only handle into an [`ObjectStoreRemote`] that can be cloned and
/// shared.
#[derive(Clone)]
pub struct ObjectStoreReader {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl PartialEq for ObjectStoreReader {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.store, &other.store) && self.prefix == other.prefix
    }
}

impl Eq for ObjectStoreReader {}

impl ObjectStoreRemote {
    /// Creates a storage handle pointing at the object store described by
    /// `url`. The returned value is async-native — wrap it in
    /// [`Blocking`](super::async_store::Blocking) for synchronous use.
    pub fn with_url(url: &Url) -> Result<ObjectStoreRemote, object_store::Error> {
        let (store, path) = parse_url(url)?;
        Ok(ObjectStoreRemote {
            store: Arc::from(store),
            prefix: path,
        })
    }
}

impl AsyncBlobStorePut for ObjectStoreRemote {
    type PutError = object_store::Error;

    fn put<S, T>(
        &mut self,
        item: T,
    ) -> impl Future<Output = Result<Inline<Handle<S>>, Self::PutError>> + Send
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        // Serialise + capture only Send primitives before the await (the
        // phantom-typed handle is `!Send` when the schema is).
        let blob = item.to_blob();
        let raw = blob.get_handle().raw;
        let bytes: bytes::Bytes = blob.bytes.into();
        async move {
            let path = self.prefix.child(BLOB_INFIX).child(hex::encode(raw));
            let result = self
                .store
                .put_opts(&path, bytes.into(), PutMode::Create.into())
                .await;
            match result {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(Inline::new(raw)),
                Err(e) => Err(e),
            }
        }
    }
}

impl AsyncBlobStore for ObjectStoreRemote {
    type Reader = ObjectStoreReader;
    type ReaderError = Infallible;

    fn reader(&mut self) -> impl Future<Output = Result<Self::Reader, Self::ReaderError>> + Send {
        let reader = ObjectStoreReader {
            store: self.store.clone(),
            prefix: self.prefix.clone(),
        };
        async move { Ok(reader) }
    }
}

impl AsyncPinStore for ObjectStoreRemote {
    type PinsError = ListPinsErr;
    type HeadError = ReadPinErr;
    type UpdateError = UpdatePinErr;

    fn pins(
        &mut self,
    ) -> impl Future<Output = Result<Vec<Result<Id, Self::PinsError>>, Self::PinsError>> + Send
    {
        async move {
            // These CAS cells are replica-local transport/retention pins, not
            // shared StrongPin branch authority.
            let prefix = self.prefix.child(LOCAL_PIN_INFIX);
            let stream = self.store.list(Some(&prefix)).filter_map(|r| async move {
                match r {
                    Ok(meta) if meta.size == 0 => None, // tombstoned local pin (0-byte object)
                    Ok(meta) => {
                        let name = match meta.location.filename() {
                            Some(name) => name,
                            None => return Some(Err(ListPinsErr::NotAFile("no filename"))),
                        };
                        let digest = match RawId::from_hex(name) {
                            Ok(digest) => digest,
                            Err(e) => return Some(Err(ListPinsErr::BadNameHex(e))),
                        };
                        let Some(id) = Id::new(digest) else {
                            return Some(Err(ListPinsErr::BadId));
                        };
                        Some(Ok(id))
                    }
                    Err(e) => Some(Err(ListPinsErr::List(e))),
                }
            });
            Ok(stream.collect().await)
        }
    }

    fn head(
        &mut self,
        id: Id,
    ) -> impl Future<Output = Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError>> + Send
    {
        async move {
            let path = self.prefix.child(LOCAL_PIN_INFIX).child(hex::encode(id));
            match self.store.get(&path).await {
                Ok(object) => {
                    let bytes = object.bytes().await?;
                    if bytes.is_empty() {
                        return Ok(None);
                    }
                    let value = (&bytes[..]).try_into()?;
                    Ok(Some(Inline::new(value)))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(ReadPinErr::StoreErr(e)),
            }
        }
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> impl Future<Output = Result<PushResult, Self::UpdateError>> + Send {
        async move {
            let path = self.prefix.child(LOCAL_PIN_INFIX).child(hex::encode(id));
            // We encode a cleared local pin as an empty object. This lets us
            // preserve CAS semantics for delete via conditional PUT
            // (PutMode::Update), since `object_store` does not currently
            // expose conditional delete.
            //
            // TODO: Once `object_store` supports conditional delete,
            // migrate away from 0-byte tombstones and treat empty objects
            // as corruption.
            let new_bytes = match new {
                Some(new) => bytes::Bytes::copy_from_slice(&new.raw),
                None => bytes::Bytes::new(),
            };

            let parse_pin = |bytes: &bytes::Bytes| -> Result<
                Option<Inline<Handle<SimpleArchive>>>,
                TryFromSliceError,
            > {
                if bytes.is_empty() {
                    return Ok(None);
                }
                let value = (&bytes[..]).try_into()?;
                Ok(Some(Inline::new(value)))
            };

            if let Some(old_hash) = old {
                let mut result = self.store.get(&path).await;
                loop {
                    match result {
                        Ok(obj) => {
                            let version = UpdateVersion {
                                e_tag: obj.meta.e_tag.clone(),
                                version: obj.meta.version.clone(),
                            };
                            let stored_bytes = obj.bytes().await?;
                            let stored_hash = parse_pin(&stored_bytes)?;
                            if stored_hash != Some(old_hash) {
                                return Ok(PushResult::Conflict(stored_hash));
                            }
                            match self
                                .store
                                .put_opts(
                                    &path,
                                    new_bytes.clone().into(),
                                    PutMode::Update(version).into(),
                                )
                                .await
                            {
                                Ok(_) => return Ok(PushResult::Success()),
                                Err(object_store::Error::Precondition { .. }) => {
                                    result = self.store.get(&path).await;
                                    continue;
                                }
                                Err(e) => return Err(UpdatePinErr::StoreErr(e)),
                            }
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            return Ok(PushResult::Conflict(None));
                        }
                        Err(e) => return Err(UpdatePinErr::StoreErr(e)),
                    }
                }
            } else {
                loop {
                    match self
                        .store
                        .put_opts(&path, new_bytes.clone().into(), PutMode::Create.into())
                        .await
                    {
                        Ok(_) => return Ok(PushResult::Success()),
                        Err(object_store::Error::AlreadyExists { .. }) => {
                            let mut result = self.store.get(&path).await;
                            loop {
                                match result {
                                    Ok(obj) => {
                                        let version = UpdateVersion {
                                            e_tag: obj.meta.e_tag.clone(),
                                            version: obj.meta.version.clone(),
                                        };
                                        let stored_bytes = obj.bytes().await?;
                                        let stored_hash = parse_pin(&stored_bytes)?;
                                        if stored_hash.is_some() {
                                            return Ok(PushResult::Conflict(stored_hash));
                                        }
                                        match self
                                            .store
                                            .put_opts(
                                                &path,
                                                new_bytes.clone().into(),
                                                PutMode::Update(version).into(),
                                            )
                                            .await
                                        {
                                            Ok(_) => return Ok(PushResult::Success()),
                                            Err(object_store::Error::Precondition { .. }) => {
                                                result = self.store.get(&path).await;
                                                continue;
                                            }
                                            Err(e) => return Err(UpdatePinErr::StoreErr(e)),
                                        }
                                    }
                                    // raced with delete; retry create
                                    Err(object_store::Error::NotFound { .. }) => break,
                                    Err(e) => return Err(UpdatePinErr::StoreErr(e)),
                                }
                            }
                            continue;
                        }
                        Err(e) => return Err(UpdatePinErr::StoreErr(e)),
                    }
                }
            }
        }
    }
}

impl AsyncBlobStoreForget for ObjectStoreRemote {
    type ForgetError = object_store::Error;

    fn forget<S>(
        &mut self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<(), Self::ForgetError>> + Send
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let raw = handle.raw;
        async move {
            let path = self.prefix.child(BLOB_INFIX).child(hex::encode(raw));
            match self.store.delete(&path).await {
                Ok(_) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(e),
            }
        }
    }
}

impl crate::repo::StorageClose for ObjectStoreRemote {
    type Error = Infallible;

    fn close(self) -> Result<(), Self::Error> {
        // No explicit close necessary for the remote object store adapter.
        Ok(())
    }
}

impl ObjectStoreReader {
    fn blob_path(&self, handle_hex: String) -> Path {
        self.prefix.child(BLOB_INFIX).child(handle_hex)
    }
}

impl AsyncBlobStoreGet for ObjectStoreReader {
    type GetError<E: Error + Send + Sync + 'static> = GetBlobErr<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>> + Send
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let raw = handle.raw;
        async move {
            let path = self.blob_path(hex::encode(raw));
            let object = self.store.get(&path).await?;
            let bytes = object.bytes().await?;
            let bytes: Bytes = bytes.into();
            let blob: Blob<S> = Blob::new(bytes);
            let actual = blob.get_handle().raw;
            if actual != raw {
                return Err(GetBlobErr::Validation {
                    expected: raw,
                    actual,
                });
            }
            blob.try_from_blob().map_err(GetBlobErr::Conversion)
        }
    }
}

impl AsyncPartialCommitDag for ObjectStoreReader {
    type Error = StoredCommitError<GetBlobErr<UnarchiveError>>;

    fn parents(
        &mut self,
        commit: super::CommitHandle,
    ) -> impl Future<Output = Result<ParentLookup, Self::Error>> + Send {
        async move {
            match self.get::<TribleSet, SimpleArchive>(commit).await {
                Ok(metadata) => commit::direct_parents(&metadata)
                    .map(ParentLookup::Present)
                    .map_err(StoredCommitError::Metadata),
                Err(GetBlobErr::Store(object_store::Error::NotFound { .. })) => {
                    Ok(ParentLookup::Missing)
                }
                Err(error) => Err(StoredCommitError::Read(error)),
            }
        }
    }
}

impl AsyncBlobStoreList for ObjectStoreReader {
    type Err = ListBlobsErr;

    fn blobs(
        &self,
    ) -> impl Future<Output = Vec<Result<Inline<Handle<UnknownBlob>>, Self::Err>>> + Send {
        async move {
            let prefix = self.prefix.child(BLOB_INFIX);
            let stream = self.store.list(Some(&prefix)).map(|r| match r {
                Ok(meta) => {
                    let blob_name = meta
                        .location
                        .filename()
                        .ok_or(ListBlobsErr::NotAFile("no filename"))?;
                    let digest =
                        RawInline::from_hex(blob_name).map_err(ListBlobsErr::BadNameHex)?;
                    Ok(Inline::new(digest))
                }
                Err(e) => Err(ListBlobsErr::List(e)),
            });
            stream.collect().await
        }
    }
}

impl AsyncBlobStoreMeta for ObjectStoreReader {
    type MetaError = object_store::Error;

    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> impl Future<Output = Result<Option<BlobMetadata>, Self::MetaError>> + Send
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let raw = handle.raw;
        async move {
            let path = self.prefix.child(BLOB_INFIX).child(hex::encode(raw));
            match self.store.head(&path).await {
                Ok(meta) => {
                    let ts = meta.last_modified.timestamp_millis() as u64;
                    let len = meta.size;
                    Ok(Some(BlobMetadata {
                        timestamp: ts,
                        length: len,
                    }))
                }
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(e),
            }
        }
    }
}

/// Error returned when retrieving a blob from the object store.
#[derive(Debug)]
pub enum GetBlobErr<E: Error> {
    /// The underlying object store operation failed.
    Store(object_store::Error),
    /// The object bytes do not hash to the content-addressed path requested.
    Validation {
        /// Handle encoded by the requested object path.
        expected: [u8; 32],
        /// Handle derived from the returned bytes.
        actual: [u8; 32],
    },
    /// The blob bytes could not be converted to the requested type.
    Conversion(E),
}

impl<E: Error> fmt::Display for GetBlobErr<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => write!(f, "object store error: {e}"),
            Self::Validation { expected, actual } => write!(
                f,
                "object content hash mismatch: expected {}, got {}",
                hex::encode(expected),
                hex::encode(actual)
            ),
            Self::Conversion(e) => write!(f, "conversion error: {e}"),
        }
    }
}

impl<E: Error> Error for GetBlobErr<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(e) => Some(e),
            Self::Validation { .. } => None,
            Self::Conversion(_) => None,
        }
    }
}

impl<E: Error> From<object_store::Error> for GetBlobErr<E> {
    fn from(e: object_store::Error) -> Self {
        Self::Store(e)
    }
}

/// Error returned when listing blobs from the object store.
#[derive(Debug)]
pub enum ListBlobsErr {
    /// The underlying list operation failed.
    List(object_store::Error),
    /// A listed object had no filename component.
    NotAFile(&'static str),
    /// A listed object's filename was not valid hexadecimal.
    BadNameHex(<RawInline as FromHex>::Error),
}

impl fmt::Display for ListBlobsErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::List(e) => write!(f, "list failed: {e}"),
            Self::NotAFile(e) => write!(f, "list failed: {e}"),
            Self::BadNameHex(e) => write!(f, "list failed: {e}"),
        }
    }
}
impl Error for ListBlobsErr {}

/// Error returned when listing replica-local pins from the object store.
#[derive(Debug)]
pub enum ListPinsErr {
    /// The underlying list operation failed.
    List(object_store::Error),
    /// A listed object had no filename component.
    NotAFile(&'static str),
    /// A listed object's filename was not valid hexadecimal.
    BadNameHex(<RawId as FromHex>::Error),
    /// The decoded bytes represent the nil identifier.
    BadId,
}

impl fmt::Display for ListPinsErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::List(e) => write!(f, "list failed: {e}"),
            Self::NotAFile(e) => write!(f, "list failed: {e}"),
            Self::BadNameHex(e) => write!(f, "list failed: {e}"),
            Self::BadId => write!(f, "list failed: bad id"),
        }
    }
}
impl Error for ListPinsErr {}

/// Error returned when reading a replica-local pin from the object store.
#[derive(Debug)]
pub enum ReadPinErr {
    /// The stored bytes could not be parsed as a valid handle.
    ValidationErr(TryFromSliceError),
    /// The underlying object store operation failed.
    StoreErr(object_store::Error),
}

impl fmt::Display for ReadPinErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::StoreErr(e) => write!(f, "pin read failed: {e}"),
            Self::ValidationErr(e) => write!(f, "pin read failed: {e}"),
        }
    }
}

impl Error for ReadPinErr {}

impl From<object_store::Error> for ReadPinErr {
    fn from(err: object_store::Error) -> Self {
        Self::StoreErr(err)
    }
}

impl From<TryFromSliceError> for ReadPinErr {
    fn from(err: TryFromSliceError) -> Self {
        Self::ValidationErr(err)
    }
}

/// Error returned when updating a replica-local pin in the object store.
#[derive(Debug)]
pub enum UpdatePinErr {
    /// The stored bytes could not be parsed as a valid handle during a
    /// compare-and-swap.
    ValidationErr(TryFromSliceError),
    /// The underlying object store operation failed.
    StoreErr(object_store::Error),
}

impl fmt::Display for UpdatePinErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ValidationErr(e) => write!(f, "pin update failed: {e}"),
            Self::StoreErr(e) => write!(f, "pin update failed: {e}"),
        }
    }
}

impl Error for UpdatePinErr {}

impl From<object_store::Error> for UpdatePinErr {
    fn from(err: object_store::Error) -> Self {
        Self::StoreErr(err)
    }
}

impl From<TryFromSliceError> for UpdatePinErr {
    fn from(err: TryFromSliceError) -> Self {
        Self::ValidationErr(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::branch_frontier::PartialCommitDag;
    use futures::executor::block_on;
    use object_store::memory::InMemory;

    fn remote() -> ObjectStoreRemote {
        ObjectStoreRemote {
            store: Arc::new(InMemory::new()),
            prefix: Path::from("repo"),
        }
    }

    #[test]
    fn partial_commit_dag_distinguishes_absence_from_malformed_content() {
        let mut remote = remote();
        let mut reader = block_on(remote.reader()).unwrap();
        let missing = Inline::new([31; 32]);
        assert_eq!(
            block_on(AsyncPartialCommitDag::parents(&mut reader, missing)).unwrap(),
            ParentLookup::Missing
        );

        let malformed = Blob::<SimpleArchive>::new(Bytes::from(vec![1]));
        let malformed_handle = block_on(remote.put::<SimpleArchive, _>(malformed)).unwrap();
        let mut reader = block_on(remote.reader()).unwrap();
        let error = block_on(AsyncPartialCommitDag::parents(
            &mut reader,
            malformed_handle,
        ))
        .unwrap_err();
        assert!(matches!(
            error,
            StoredCommitError::Read(GetBlobErr::Conversion(_))
        ));

        // The sync edge adapter preserves the same absence semantics used by
        // Repository's frontier resolver.
        let mut blocking = crate::repo::async_store::Blocking::new(reader).unwrap();
        assert_eq!(blocking.parents(missing).unwrap(), ParentLookup::Missing);
    }
}
