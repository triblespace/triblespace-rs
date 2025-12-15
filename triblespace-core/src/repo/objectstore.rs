use std::array::TryFromSliceError;
use std::convert::Infallible;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use anybytes::Bytes;
use crossbeam_channel::{bounded, Receiver};
use futures::Stream;
use futures::StreamExt;
use tokio::runtime::Runtime;

use object_store::parse_url;
use object_store::path::Path;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::UpdateVersion;
use object_store::{self};
use url::Url;

use hex::FromHex;

use crate::blob::schemas::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::id::Id;
use crate::id::RawId;
use crate::prelude::blobschemas::SimpleArchive;
use crate::value::schemas::hash::Handle;
use crate::value::schemas::hash::HashProtocol;
use crate::value::RawValue;
use crate::value::Value;
use crate::value::ValueSchema;

use super::BlobStore;
use super::BlobStoreGet;
use super::BlobStoreList;
use super::BlobStorePut;
use super::BranchStore;
use super::PushResult;

const BRANCH_INFIX: &str = "branches";
const BLOB_INFIX: &str = "blobs";

/// Repository backed by an [`object_store`] compatible storage backend.
///
/// All data is stored in an external service (e.g. S3, local filesystem) via
/// the `object_store` crate.
pub struct ObjectStoreRemote<H> {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    rt: Arc<Runtime>,
    _hasher: PhantomData<H>,
}

impl<H> fmt::Debug for ObjectStoreRemote<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreRemote")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl<H> fmt::Debug for ObjectStoreReader<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreReader")
            .field("prefix", &self.prefix)
            .finish()
    }
}

#[derive(Clone)]
pub struct ObjectStoreReader<H> {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    rt: Arc<Runtime>,
    _hasher: PhantomData<H>,
}

pub struct BlockingIter<T> {
    rx: Receiver<T>,
}

impl<T> BlockingIter<T> {
    fn from_stream<S>(handle: tokio::runtime::Handle, stream: S, capacity: usize) -> Self
    where
        S: Stream<Item = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = bounded(capacity);
        let handle_for_spawn = handle.clone();
        let handle_for_task = handle.clone();
        handle_for_spawn.spawn(async move {
            let mut s = Box::pin(stream);
            let rt = handle_for_task;
            while let Some(item) = s.next().await {
                let tx_clone = tx.clone();
                let bh = rt.clone();
                // send on blocking pool to avoid blocking a runtime worker
                match bh.spawn_blocking(move || tx_clone.send(item)).await {
                    Ok(Ok(())) => {}
                    _ => break,
                }
            }
            // tx dropped here -> closes channel
        });
        BlockingIter { rx }
    }
}

impl<T> Iterator for BlockingIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl<H> PartialEq for ObjectStoreReader<H> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.store, &other.store) && self.prefix == other.prefix
    }
}

impl<H> Eq for ObjectStoreReader<H> {}

impl<H> ObjectStoreRemote<H> {
    /// Creates a repository pointing at the object store described by `url`.
    pub fn with_url(url: &Url) -> Result<ObjectStoreRemote<H>, object_store::Error> {
        let (store, path) = parse_url(url)?;
        Ok(ObjectStoreRemote {
            store: Arc::from(store),
            prefix: path,
            rt: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("build runtime"),
            ),
            _hasher: PhantomData,
        })
    }
}

impl<H> BlobStorePut<H> for ObjectStoreRemote<H>
where
    H: HashProtocol,
{
    type PutError = object_store::Error;

    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<H, S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<H, S>: ValueSchema,
    {
        let blob = item.to_blob();
        let handle = blob.get_handle();
        let path = self.prefix.child(BLOB_INFIX).child(hex::encode(handle.raw));
        let bytes: bytes::Bytes = blob.bytes.into();
        let result = self.rt.block_on(async {
            self.store
                .put_opts(&path, bytes.into(), PutMode::Create.into())
                .await
        });
        match result {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => Ok(handle),
            Err(e) => Err(e),
        }
    }
}

impl<H> BlobStore<H> for ObjectStoreRemote<H>
where
    H: HashProtocol,
{
    type Reader = ObjectStoreReader<H>;
    type ReaderError = Infallible;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        Ok(ObjectStoreReader {
            store: self.store.clone(),
            prefix: self.prefix.clone(),
            rt: self.rt.clone(),
            _hasher: PhantomData,
        })
    }
}

impl<H> BranchStore<H> for ObjectStoreRemote<H>
where
    H: HashProtocol,
{
    type BranchesError = ListBranchesErr;
    type HeadError = PullBranchErr;
    type UpdateError = PushBranchErr;

    type ListIter<'a> = BlockingIter<Result<Id, Self::BranchesError>>;

    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError> {
        let prefix = self.prefix.child(BRANCH_INFIX);
        let stream = self.store.list(Some(&prefix)).map(|r| match r {
            Ok(meta) => {
                let name = meta
                    .location
                    .filename()
                    .ok_or(ListBranchesErr::NotAFile("no filename"))?;
                let digest = RawId::from_hex(name).map_err(ListBranchesErr::BadNameHex)?;
                let Some(id) = Id::new(digest) else {
                    return Err(ListBranchesErr::BadId);
                };
                Ok(id)
            }
            Err(e) => Err(ListBranchesErr::List(e)),
        });
        Ok(BlockingIter::from_stream(
            self.rt.handle().clone(),
            stream,
            16,
        ))
    }

    fn head(&mut self, id: Id) -> Result<Option<Value<Handle<H, SimpleArchive>>>, Self::HeadError> {
        let path = self.prefix.child(BRANCH_INFIX).child(hex::encode(id));
        let result = self.rt.block_on(async { self.store.get(&path).await });
        match result {
            Ok(object) => {
                let bytes = self.rt.block_on(object.bytes())?;
                let value = (&bytes[..]).try_into()?;
                Ok(Some(Value::new(value)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(PullBranchErr::StoreErr(e)),
        }
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<H, SimpleArchive>>>,
        new: Value<Handle<H, SimpleArchive>>,
    ) -> Result<PushResult<H>, Self::UpdateError> {
        let path = self.prefix.child(BRANCH_INFIX).child(hex::encode(id));
        let new_bytes = bytes::Bytes::copy_from_slice(&new.raw);
        if let Some(old_hash) = old {
            let mut result = self.rt.block_on(async { self.store.get(&path).await });
            loop {
                match result {
                    Ok(obj) => {
                        let version = UpdateVersion {
                            e_tag: obj.meta.e_tag.clone(),
                            version: obj.meta.version.clone(),
                        };
                        let stored_bytes = self.rt.block_on(obj.bytes())?;
                        let stored_value = (&stored_bytes[..]).try_into()?;
                        let stored_hash = Value::new(stored_value);
                        if old_hash != stored_hash {
                            return Ok(PushResult::Conflict(Some(stored_hash)));
                        }
                        match self.rt.block_on(async {
                            self.store
                                .put_opts(
                                    &path,
                                    new_bytes.clone().into(),
                                    PutMode::Update(version).into(),
                                )
                                .await
                        }) {
                            Ok(_) => return Ok(PushResult::Success()),
                            Err(object_store::Error::Precondition { .. }) => {
                                result = self.rt.block_on(async { self.store.get(&path).await });
                                continue;
                            }
                            Err(e) => return Err(PushBranchErr::StoreErr(e)),
                        }
                    }
                    Err(object_store::Error::NotFound { .. }) => {
                        return Ok(PushResult::Conflict(None));
                    }
                    Err(e) => return Err(PushBranchErr::StoreErr(e)),
                }
            }
        } else {
            loop {
                match self.rt.block_on(async {
                    self.store
                        .put_opts(&path, new_bytes.clone().into(), PutMode::Create.into())
                        .await
                }) {
                    Ok(_) => return Ok(PushResult::Success()),
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        let result = self.rt.block_on(async { self.store.get(&path).await });
                        match result {
                            Ok(obj) => {
                                let bytes = self.rt.block_on(obj.bytes())?;
                                let value = (&bytes[..]).try_into()?;
                                return Ok(PushResult::Conflict(Some(Value::new(value))));
                            }
                            Err(object_store::Error::NotFound { .. }) => continue,
                            Err(e) => return Err(PushBranchErr::StoreErr(e)),
                        }
                    }
                    Err(e) => return Err(PushBranchErr::StoreErr(e)),
                }
            }
        }
    }
}

impl<H> crate::repo::StorageClose for ObjectStoreRemote<H> {
    type Error = Infallible;

    fn close(self) -> Result<(), Self::Error> {
        // No explicit close necessary for the remote object store adapter.
        Ok(())
    }
}

impl<H> ObjectStoreReader<H> {
    fn blob_path(&self, handle_hex: String) -> Path {
        self.prefix.child(BLOB_INFIX).child(handle_hex)
    }
}

impl<H> BlobStoreList<H> for ObjectStoreReader<H>
where
    H: HashProtocol,
{
    type Err = ListBlobsErr;
    type Iter<'a> = BlockingIter<Result<Value<Handle<H, UnknownBlob>>, Self::Err>>;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        let prefix = self.prefix.child(BLOB_INFIX);
        let stream = self.store.list(Some(&prefix)).map(|r| match r {
            Ok(meta) => {
                let blob_name = meta
                    .location
                    .filename()
                    .ok_or(ListBlobsErr::NotAFile("no filename"))?;
                let digest = RawValue::from_hex(blob_name).map_err(ListBlobsErr::BadNameHex)?;
                Ok(Value::new(digest))
            }
            Err(e) => Err(ListBlobsErr::List(e)),
        });
        BlockingIter::from_stream(self.rt.handle().clone(), stream, 16)
    }
}

#[derive(Debug)]
pub enum GetBlobErr<E: Error> {
    Store(object_store::Error),
    Conversion(E),
}

impl<E: Error> fmt::Display for GetBlobErr<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => write!(f, "object store error: {e}"),
            Self::Conversion(e) => write!(f, "conversion error: {e}"),
        }
    }
}

impl<E: Error> Error for GetBlobErr<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(e) => Some(e),
            Self::Conversion(_) => None,
        }
    }
}

impl<E: Error> From<object_store::Error> for GetBlobErr<E> {
    fn from(e: object_store::Error) -> Self {
        Self::Store(e)
    }
}

impl<H> BlobStoreGet<H> for ObjectStoreReader<H>
where
    H: HashProtocol,
{
    type GetError<E: Error> = GetBlobErr<E>;

    fn get<T, S>(
        &self,
        handle: Value<Handle<H, S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema + 'static,
        T: TryFromBlob<S>,
        Handle<H, S>: ValueSchema,
    {
        let path = self.blob_path(hex::encode(handle.raw));
        let object = self.rt.block_on(async { self.store.get(&path).await })?;
        let bytes = self.rt.block_on(object.bytes())?;
        let bytes: Bytes = bytes.into();
        let blob: Blob<S> = Blob::new(bytes);
        blob.try_from_blob().map_err(GetBlobErr::Conversion)
    }
}

#[derive(Debug)]
pub enum ListBlobsErr {
    List(object_store::Error),
    NotAFile(&'static str),
    BadNameHex(<RawValue as FromHex>::Error),
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

#[derive(Debug)]
pub enum ListBranchesErr {
    List(object_store::Error),
    NotAFile(&'static str),
    BadNameHex(<RawId as FromHex>::Error),
    BadId,
}

impl fmt::Display for ListBranchesErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::List(e) => write!(f, "list failed: {e}"),
            Self::NotAFile(e) => write!(f, "list failed: {e}"),
            Self::BadNameHex(e) => write!(f, "list failed: {e}"),
            Self::BadId => write!(f, "list failed: bad id"),
        }
    }
}
impl Error for ListBranchesErr {}

#[derive(Debug)]
pub enum PullBranchErr {
    ValidationErr(TryFromSliceError),
    StoreErr(object_store::Error),
}

impl fmt::Display for PullBranchErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::StoreErr(e) => write!(f, "pull failed: {e}"),
            Self::ValidationErr(e) => write!(f, "pull failed: {e}"),
        }
    }
}

impl Error for PullBranchErr {}

impl From<object_store::Error> for PullBranchErr {
    fn from(err: object_store::Error) -> Self {
        Self::StoreErr(err)
    }
}

impl From<TryFromSliceError> for PullBranchErr {
    fn from(err: TryFromSliceError) -> Self {
        Self::ValidationErr(err)
    }
}

#[derive(Debug)]
pub enum PushBranchErr {
    ValidationErr(TryFromSliceError),
    StoreErr(object_store::Error),
}

impl fmt::Display for PushBranchErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ValidationErr(e) => write!(f, "commit failed: {e}"),
            Self::StoreErr(e) => write!(f, "commit failed: {e}"),
        }
    }
}

impl Error for PushBranchErr {}

impl From<object_store::Error> for PushBranchErr {
    fn from(err: object_store::Error) -> Self {
        Self::StoreErr(err)
    }
}

impl From<TryFromSliceError> for PushBranchErr {
    fn from(err: TryFromSliceError) -> Self {
        Self::ValidationErr(err)
    }
}

impl<H> crate::repo::BlobStoreMeta<H> for ObjectStoreReader<H>
where
    H: HashProtocol,
{
    type MetaError = object_store::Error;

    fn metadata<S>(
        &self,
        handle: Value<Handle<H, S>>,
    ) -> Result<Option<crate::repo::BlobMetadata>, Self::MetaError>
    where
        S: BlobSchema + 'static,
        Handle<H, S>: ValueSchema,
    {
        let handle_hex = hex::encode(handle.raw);
        let path = self.prefix.child(BLOB_INFIX).child(handle_hex);
        match self.rt.block_on(async { self.store.head(&path).await }) {
            Ok(meta) => {
                let ts = meta.last_modified.timestamp_millis() as u64;
                let len = meta.size;
                Ok(Some(crate::repo::BlobMetadata {
                    timestamp: ts,
                    length: len,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<H> crate::repo::BlobStoreForget<H> for ObjectStoreRemote<H>
where
    H: HashProtocol,
{
    type ForgetError = object_store::Error;

    fn forget<S>(&mut self, handle: Value<Handle<H, S>>) -> Result<(), Self::ForgetError>
    where
        S: BlobSchema + 'static,
        Handle<H, S>: ValueSchema,
    {
        let handle_hex = hex::encode(handle.raw);
        let path = self.prefix.child(BLOB_INFIX).child(handle_hex);
        match self.rt.block_on(async { self.store.delete(&path).await }) {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
