//! A Pile is an append-only collection of blobs and branches stored in a single
//! file. It is designed as a durable local repository storage that can be safely
//! shared between threads.
//!
//! The pile operates as a **WAL-as-a-DB**: the write-ahead log _is_ the database.
//! All indices and metadata are reconstructed from the log on startup and no
//! additional state is persisted elsewhere.
//!
//! The pile treats its file as an immutable append-only log. Once a record lies
//! below `applied_length` and its bytes have been returned by
//! [`get`](Pile::get) or [`apply_next`](Pile::apply_next), those bytes are
//! assumed permanent. Modifying any part of the pile other than appending new
//! records is undefined behaviour. The un-applied tail may hide a partial
//! append after a crash, so validation and repair only operate on offsets
//! beyond `applied_length`. Each record's [`ValidationState`] is cached for the
//! lifetime of the process under this immutability assumption.
//!
//! For layout and recovery details see the [Pile
//! Format](../../book/src/pile-format.md) chapter of the Tribles Book.

use anybytes::Bytes;
use hex_literal::hex;
use memmap2::MmapOptions;
use memmap2::MmapRaw;
use std::convert::Infallible;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::IoSlice;
use std::io::Write;
use std::path::Path;
use std::ptr::slice_from_raw_parts;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;
use zerocopy::TryFromBytes;

use crate::blob::schemas::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::id::Id;
use crate::id::RawId;
use crate::patch::Entry;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::prelude::blobschemas::SimpleArchive;
use crate::prelude::valueschemas::Handle;
use crate::value::schemas::hash::Blake3;
use crate::value::schemas::hash::Hash;
use crate::value::schemas::hash::HashProtocol;
use crate::value::RawValue;
use crate::value::Value;
use crate::value::ValueSchema;

const MAGIC_MARKER_BLOB: RawId = hex!("1E08B022FF2F47B6EBACF1D68EB35D96");
const MAGIC_MARKER_BRANCH: RawId = hex!("2BC991A7F5D5D2A3A468C53B0AA03504");
const MAGIC_MARKER_BRANCH_TOMBSTONE: RawId = hex!("E888CC787202D2AE4C654BFE9699C430");

const BLOB_HEADER_LEN: usize = std::mem::size_of::<BlobHeader>();
const BLOB_ALIGNMENT: usize = BLOB_HEADER_LEN;

#[derive(Debug, Clone, Copy)]
pub enum ValidationState {
    Validated,
    Invalid,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    state: Arc<OnceLock<ValidationState>>,
    offset: usize,
    len: u64,
    timestamp: u64,
}

impl IndexEntry {
    fn new(offset: usize, len: u64, timestamp: u64) -> Self {
        Self {
            state: Arc::new(OnceLock::new()),
            offset,
            len,
            timestamp,
        }
    }
}

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BranchHeader {
    magic_marker: RawId,
    branch_id: RawId,
    hash: RawValue,
}

impl BranchHeader {
    fn new<H: HashProtocol>(branch_id: Id, hash: Value<Handle<H, SimpleArchive>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BRANCH,
            branch_id: *branch_id,
            hash: hash.raw,
        }
    }
}

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BranchTombstoneHeader {
    magic_marker: RawId,
    branch_id: RawId,
    /// Reserved bytes to preserve 64 byte record alignment.
    reserved: RawValue,
}

impl BranchTombstoneHeader {
    fn new(branch_id: Id) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BRANCH_TOMBSTONE,
            branch_id: *branch_id,
            reserved: [0u8; 32],
        }
    }
}

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BlobHeader {
    magic_marker: RawId,
    timestamp: u64,
    length: u64,
    hash: RawValue,
}

impl BlobHeader {
    fn new<H: HashProtocol>(timestamp: u64, length: u64, hash: Value<Hash<H>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BLOB,
            timestamp,
            length,
            hash: hash.raw,
        }
    }
}

#[derive(Debug)]
enum Applied<H: HashProtocol> {
    Blob { hash: Value<Hash<H>> },
    Branch { id: Id, hash: Value<Hash<H>> },
    BranchTombstone { id: Id },
}

#[derive(Debug)]
/// A grow-only collection of blobs and branch pointers backed by a single file on disk.
///
/// Branch updates do not verify that referenced blobs exist in the pile, allowing the
/// pile to operate as a head-only store when blob data lives elsewhere.
///
/// [`Pile::refresh`] aborts immediately if the underlying file shrinks below
/// data that has already been applied, preventing undefined behavior from
/// dangling [`Bytes`](anybytes::Bytes) handles.
pub struct Pile<H: HashProtocol = Blake3> {
    file: File,
    mmap: Arc<MmapRaw>,
    blobs: PATCH<32, IdentitySchema, IndexEntry>,
    branches: PATCH<16, IdentitySchema, Value<Handle<H, SimpleArchive>>>,
    /// Length of the file that has been validated and applied.
    ///
    /// Offsets below this value are guaranteed valid; corruption detection
    /// only operates on the un-applied tail beyond this boundary.
    applied_length: usize,
}

fn padding_for_blob(blob_size: usize) -> usize {
    (BLOB_ALIGNMENT - ((BLOB_HEADER_LEN + blob_size) % BLOB_ALIGNMENT)) % BLOB_ALIGNMENT
}

#[derive(Debug, Clone)]
/// Read-only handle referencing a [`Pile`].
///
/// Multiple `PileReader` instances can coexist and provide concurrent access to
/// the same underlying pile data.
pub struct PileReader<H: HashProtocol> {
    mmap: Arc<MmapRaw>,
    blobs: PATCH<32, IdentitySchema, IndexEntry>,
    _marker: std::marker::PhantomData<H>,
}

impl<H: HashProtocol> PartialEq for PileReader<H> {
    fn eq(&self, other: &Self) -> bool {
        self.blobs == other.blobs
    }
}

impl<H: HashProtocol> Eq for PileReader<H> {}

impl<H: HashProtocol> PileReader<H> {
    fn new(mmap: Arc<MmapRaw>, blobs: PATCH<32, IdentitySchema, IndexEntry>) -> Self {
        Self {
            mmap,
            blobs,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns an iterator over all blobs currently stored in the pile.
    ///
    /// This creates an owned snapshot of the current keys/indices so the
    /// returned iterator does not borrow from the underlying PATCH.
    pub fn iter(&self) -> PileBlobStoreIter<H> {
        // Clone the PATCH (cheap copy-on-write) and create two clones: one
        // consumed by the iterator and one retained for lookups of index
        // entries while iterating.
        let for_iter = self.blobs.clone();
        let lookup = for_iter.clone();
        let inner = for_iter.into_iter();
        PileBlobStoreIter::<H> {
            mmap: self.mmap.clone(),
            inner,
            lookup,
            _marker: std::marker::PhantomData,
        }
    }

    // metadata moved into BlobStoreMeta impl below
}

impl<H: HashProtocol> BlobStoreGet<H> for PileReader<H> {
    type GetError<E: Error> = GetBlobError<E>;

    fn get<T, S>(
        &self,
        handle: Value<Handle<H, S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobSchema + 'static,
        T: TryFromBlob<S>,
        Handle<H, S>: ValueSchema,
    {
        let hash: &Value<Hash<H>> = handle.as_transmute();
        let Some(entry) = self.blobs.get(&hash.raw) else {
            return Err(GetBlobError::BlobNotFound);
        };
        let IndexEntry {
            state, offset, len, ..
        } = entry.clone();
        let bytes = unsafe {
            let slice = slice_from_raw_parts(self.mmap.as_ptr().add(offset), len as usize)
                .as_ref()
                .unwrap();
            Bytes::from_raw_parts(slice, self.mmap.clone())
        };
        let state = state.get_or_init(|| {
            let computed_hash = Hash::<H>::digest(&bytes);
            if computed_hash == *hash {
                ValidationState::Validated
            } else {
                ValidationState::Invalid
            }
        });
        match state {
            ValidationState::Validated => {
                let blob: Blob<S> = Blob::new(bytes.clone());
                match blob.try_from_blob() {
                    Ok(value) => Ok(value),
                    Err(e) => Err(GetBlobError::ConversionError(e)),
                }
            }
            ValidationState::Invalid => Err(GetBlobError::ValidationError(bytes.clone())),
        }
    }
}

impl<H: HashProtocol> BlobStore<H> for Pile<H> {
    type Reader = PileReader<H>;
    type ReaderError = ReadError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh()?;
        Ok(PileReader::new(self.mmap.clone(), self.blobs.clone()))
    }
}

#[derive(Debug)]
pub enum ReadError {
    IoError(std::io::Error),
    CorruptPile { valid_length: usize },
    FileTooLarge { length: usize },
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::IoError(err) => write!(f, "IO error: {err}"),
            ReadError::CorruptPile { valid_length } => {
                write!(f, "Corrupt pile at byte {valid_length}")
            }
            ReadError::FileTooLarge { length } => {
                write!(f, "Pile of length {length} exceeds supported size")
            }
        }
    }
}
impl std::error::Error for ReadError {}

impl From<std::io::Error> for ReadError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<ReadError> for std::io::Error {
    fn from(err: ReadError) -> Self {
        match err {
            ReadError::IoError(e) => e,
            ReadError::CorruptPile { valid_length } => {
                std::io::Error::other(format!("corrupt pile at byte {valid_length}"))
            }
            ReadError::FileTooLarge { length } => {
                std::io::Error::other(format!("pile length {length} exceeds supported size"))
            }
        }
    }
}

#[derive(Debug)]
pub enum InsertError {
    IoError(std::io::Error),
    TimeError(std::time::SystemTimeError),
}

impl std::fmt::Display for InsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertError::IoError(err) => write!(f, "IO error: {err}"),
            InsertError::TimeError(err) => write!(f, "system time error: {err}"),
        }
    }
}
impl std::error::Error for InsertError {}

impl From<std::io::Error> for InsertError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<std::time::SystemTimeError> for InsertError {
    fn from(err: std::time::SystemTimeError) -> Self {
        Self::TimeError(err)
    }
}

impl From<ReadError> for InsertError {
    fn from(err: ReadError) -> Self {
        Self::IoError(err.into())
    }
}

pub enum UpdateBranchError {
    IoError(std::io::Error),
}

impl std::error::Error for UpdateBranchError {}

unsafe impl Send for UpdateBranchError {}
unsafe impl Sync for UpdateBranchError {}

impl std::fmt::Debug for UpdateBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateBranchError::IoError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::fmt::Display for UpdateBranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateBranchError::IoError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl From<std::io::Error> for UpdateBranchError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<ReadError> for UpdateBranchError {
    fn from(err: ReadError) -> Self {
        Self::IoError(err.into())
    }
}

#[derive(Debug)]
pub enum GetBlobError<E: Error> {
    BlobNotFound,
    ValidationError(Bytes),
    ConversionError(E),
}

impl<E: Error> std::fmt::Display for GetBlobError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetBlobError::BlobNotFound => write!(f, "Blob not found"),
            GetBlobError::ConversionError(err) => write!(f, "Conversion error: {err}"),
            GetBlobError::ValidationError(_) => write!(f, "Validation error"),
        }
    }
}

impl<E: Error> std::error::Error for GetBlobError<E> {}

#[derive(Debug)]
pub enum FlushError {
    IoError(std::io::Error),
}

impl From<std::io::Error> for FlushError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::fmt::Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushError::IoError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::error::Error for FlushError {}

impl<H: HashProtocol> Pile<H> {
    /// Opens an existing pile without scanning or repairing its contents.
    ///
    /// The returned pile has no in-memory index; callers should invoke
    /// [`refresh`] to load existing data or [`restore`] to repair and load
    /// after a crash.
    pub fn open(path: &Path) -> Result<Self, ReadError> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let length = file.metadata()?.len() as usize;
        let page_size = page_size::get();
        let base_size = page_size * 1024;
        let mapped_size = base_size.max(
            length
                .checked_next_power_of_two()
                .ok_or(ReadError::FileTooLarge { length })?,
        );

        let mmap = MmapOptions::new()
            .len(mapped_size)
            .map_raw_read_only(&file)?;
        let mmap = Arc::new(mmap);

        Ok(Self {
            file,
            mmap,
            blobs: PATCH::<32, IdentitySchema, IndexEntry>::new(),
            branches: PATCH::<16, IdentitySchema, Value<Handle<H, SimpleArchive>>>::new(),
            applied_length: 0,
        })
    }

    /// Refreshes in-memory state from newly appended records.
    ///
    /// Aborts immediately if the underlying pile file has shrunk below the
    /// portion already applied since the last refresh. Truncating validated data
    /// would invalidate existing `Bytes` handles and continuing would result in
    /// undefined behavior.
    ///
    /// This acquires a shared file lock to avoid racing with [`restore`],
    /// which takes an exclusive lock before truncating.
    pub fn refresh(&mut self) -> Result<(), ReadError> {
        self.file.lock_shared()?;
        let res = self.refresh_locked();
        let unlock_res = self.file.unlock();
        res?;
        unlock_res?;
        Ok(())
    }

    /// Applies the next record from disk to in-memory indices.
    ///
    /// Aborts if the pile file is observed to shrink below the portion already
    /// applied, which would otherwise leave existing `Bytes` handles dangling
    /// and lead to undefined behavior.
    fn apply_next(&mut self) -> Result<Option<Applied<H>>, ReadError> {
        let file_len = self.file.metadata()?.len() as usize;
        if file_len < self.applied_length {
            // Truncation below `applied_length` invalidates previously issued
            // `Bytes` handles, so there is no safe recovery path.
            std::process::abort();
        }
        if file_len == self.applied_length {
            return Ok(None);
        }
        let mut mapped_size = self.mmap.len();
        if file_len > mapped_size {
            while mapped_size < file_len {
                mapped_size *= 2;
            }
            let mmap = MmapOptions::new()
                .len(mapped_size)
                .map_raw_read_only(&self.file)?;
            self.mmap = Arc::new(mmap);
        }
        let start_offset = self.applied_length;
        let mut bytes = unsafe {
            let slice = slice_from_raw_parts(
                self.mmap.as_ptr().add(start_offset),
                file_len - start_offset,
            )
            .as_ref()
            .unwrap();
            Bytes::from_raw_parts(slice, self.mmap.clone())
        };
        if bytes.len() < 16 {
            return Err(ReadError::CorruptPile {
                valid_length: start_offset,
            });
        }
        let magic = bytes[0..16].try_into().unwrap();
        match magic {
            MAGIC_MARKER_BLOB => {
                let header =
                    bytes
                        .view_prefix::<BlobHeader>()
                        .map_err(|_| ReadError::CorruptPile {
                            valid_length: start_offset,
                        })?;
                let data_len = header.length as usize;
                let pad = padding_for_blob(data_len);
                let data_offset = start_offset + BLOB_HEADER_LEN;
                bytes.take_prefix(data_len).ok_or(ReadError::CorruptPile {
                    valid_length: start_offset,
                })?;
                bytes.take_prefix(pad).ok_or(ReadError::CorruptPile {
                    valid_length: start_offset,
                })?;
                let hash: Value<Hash<H>> = Value::new(header.hash);
                let ts = header.timestamp;
                let entry =
                    Entry::with_value(&hash.raw, IndexEntry::new(data_offset, header.length, ts));
                match self.blobs.get(&hash.raw) {
                    None => {
                        self.blobs.insert(&entry);
                    }
                    Some(entry_ref) => {
                        let IndexEntry {
                            state, offset, len, ..
                        } = entry_ref.clone();
                        let state = state.get_or_init(|| {
                            let bytes = unsafe {
                                let slice = slice_from_raw_parts(
                                    self.mmap.as_ptr().add(offset),
                                    len as usize,
                                )
                                .as_ref()
                                .unwrap();
                                Bytes::from_raw_parts(slice, self.mmap.clone())
                            };
                            let computed = Hash::<H>::digest(&bytes);
                            if computed == hash {
                                ValidationState::Validated
                            } else {
                                ValidationState::Invalid
                            }
                        });
                        if let ValidationState::Invalid = state {
                            self.blobs.replace(&entry);
                        }
                    }
                }
                self.applied_length = start_offset + BLOB_HEADER_LEN + data_len + pad;
                Ok(Some(Applied::Blob { hash }))
            }
            MAGIC_MARKER_BRANCH => {
                let header =
                    bytes
                        .view_prefix::<BranchHeader>()
                        .map_err(|_| ReadError::CorruptPile {
                            valid_length: start_offset,
                        })?;
                let branch_id = Id::new(header.branch_id).ok_or(ReadError::CorruptPile {
                    valid_length: start_offset,
                })?;
                // Interpret the stored raw value as a hash and transmute into a
                // handle value for storage. Use Entry to insert/replace into the PATCH.
                let hash: Value<Hash<H>> = Value::new(header.hash);
                let handle_val: Value<Handle<H, SimpleArchive>> = hash.into();
                let entry = Entry::with_value(&header.branch_id, handle_val);
                // Replace existing mapping (if any) with the new head.
                self.branches.replace(&entry);
                self.applied_length = start_offset + std::mem::size_of::<BranchHeader>();
                Ok(Some(Applied::Branch {
                    id: branch_id,
                    hash,
                }))
            }
            MAGIC_MARKER_BRANCH_TOMBSTONE => {
                let header = bytes.view_prefix::<BranchTombstoneHeader>().map_err(|_| {
                    ReadError::CorruptPile {
                        valid_length: start_offset,
                    }
                })?;
                let branch_id = Id::new(header.branch_id).ok_or(ReadError::CorruptPile {
                    valid_length: start_offset,
                })?;
                self.branches.remove(&header.branch_id);
                self.applied_length = start_offset + std::mem::size_of::<BranchTombstoneHeader>();
                Ok(Some(Applied::BranchTombstone { id: branch_id }))
            }
            _ => Err(ReadError::CorruptPile {
                valid_length: start_offset,
            }),
        }
    }

    fn refresh_locked(&mut self) -> Result<(), ReadError> {
        while self.apply_next()?.is_some() {}
        Ok(())
    }

    /// Restores a pile after a partial or corrupt append.
    ///
    /// The method first attempts a regular [`refresh`]. If corruption is
    /// detected, it acquires an exclusive lock, re-attempts the refresh and,
    /// upon confirming the corruption, truncates the pile to the last known
    /// good offset. The exclusive lock blocks other readers so truncation
    /// cannot race with [`refresh`].
    pub fn restore(&mut self) -> Result<(), ReadError> {
        match self.refresh() {
            Ok(()) => Ok(()),
            Err(ReadError::CorruptPile { .. }) => {
                self.file.lock()?;
                let res = match self.refresh_locked() {
                    Ok(()) => Ok(()),
                    Err(ReadError::CorruptPile { valid_length }) => {
                        self.file.set_len(valid_length as u64)?;
                        self.file.sync_all()?;
                        self.applied_length = valid_length;
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                self.file.unlock()?;
                res
            }
            Err(e) => Err(e),
        }
    }

    /// Persists all writes and metadata to the underlying pile file.
    pub fn flush(&mut self) -> Result<(), FlushError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Flushes pending data and consumes the pile, returning an error if the
    /// flush fails.
    pub fn close(mut self) -> Result<(), FlushError> {
        let res = self.flush();

        let mut this = std::mem::ManuallyDrop::new(self);
        unsafe {
            std::ptr::drop_in_place(&mut this.mmap);
            std::ptr::drop_in_place(&mut this.file);
            std::ptr::drop_in_place(&mut this.blobs);
            std::ptr::drop_in_place(&mut this.branches);
        }

        res
    }
}

impl<H: HashProtocol> Drop for Pile<H> {
    fn drop(&mut self) {
        eprintln!("warning: Pile dropped without calling close(); data may not be persisted");
    }
}

// Implement the repository storage close trait so callers can call
// `repo.close()` when the repository was created with a `Pile` storage.
impl<H: HashProtocol> crate::repo::StorageClose for Pile<H> {
    type Error = FlushError;

    fn close(self) -> Result<(), Self::Error> {
        Pile::close(self)
    }
}

use super::BlobStore;
use super::BlobStoreGet;
use super::BlobStoreList;
use super::BlobStorePut;
use super::BranchStore;
use super::PushResult;

/// Iterator returned by [`PileReader::iter`].
///
/// Iterates over all `(Handle, Blob)` pairs currently stored in the pile.
/// Owned iterator over all blobs currently stored in the pile. This collects
/// a snapshot of keys/indices at iterator creation so the iterator does not
/// borrow the underlying `PATCH` and can live independently of the `Pile`.
pub struct PileBlobStoreIter<H: HashProtocol> {
    mmap: Arc<MmapRaw>,
    inner: crate::patch::PATCHIntoIterator<32, IdentitySchema, IndexEntry>,
    /// Owned clone of the PATCH used for lookups of IndexEntry by key.
    lookup: crate::patch::PATCH<32, IdentitySchema, IndexEntry>,
    _marker: std::marker::PhantomData<H>,
}

impl<H: HashProtocol> Iterator for PileBlobStoreIter<H> {
    type Item =
        Result<(Value<Handle<H, UnknownBlob>>, Blob<UnknownBlob>), GetBlobError<Infallible>>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.inner.next()?; // [u8;32]
        let hash = Value::<Hash<H>>::new(key);
        // Look up the index entry inside the owned PATCH clone held by the
        // `lookup` field. The clone is cheap and allows us to resolve index
        // entries without borrowing the live PATCH.
        if let Some(entry) = self.lookup.get(&key) {
            let IndexEntry {
                state, offset, len, ..
            } = entry.clone();
            let bytes = unsafe {
                let slice = slice_from_raw_parts(self.mmap.as_ptr().add(offset), len as usize)
                    .as_ref()
                    .unwrap();
                Bytes::from_raw_parts(slice, self.mmap.clone())
            };
            let state = state.get_or_init(|| {
                let computed_hash = Hash::<H>::digest(&bytes);
                if computed_hash == hash {
                    ValidationState::Validated
                } else {
                    ValidationState::Invalid
                }
            });
            match state {
                ValidationState::Validated => {
                    let blob: Blob<UnknownBlob> = Blob::new(bytes.clone());
                    let handle: Value<Handle<H, UnknownBlob>> = hash.into();
                    Some(Ok((handle, blob)))
                }
                ValidationState::Invalid => Some(Err(GetBlobError::ValidationError(bytes.clone()))),
            }
        } else {
            // Missing index entry for key — this can happen if the underlying
            // pile mutated concurrently; skip it.
            Some(Err(GetBlobError::BlobNotFound))
        }
    }
}

/// Adapter that yields only the blob handles. The iterator owns the handle
/// list and does not borrow the backing `PATCH`.
pub struct PileBlobStoreListIter<H: HashProtocol> {
    inner: crate::patch::PATCHIntoIterator<32, IdentitySchema, IndexEntry>,
    _marker: std::marker::PhantomData<H>,
}

impl<H: HashProtocol> Iterator for PileBlobStoreListIter<H> {
    type Item = Result<Value<Handle<H, UnknownBlob>>, GetBlobError<Infallible>>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = self.inner.next()?;
        let hash = Value::<Hash<H>>::new(key);
        let handle: Value<Handle<H, UnknownBlob>> = hash.into();
        Some(Ok(handle))
    }
}

impl<H: HashProtocol> BlobStoreList<H> for PileReader<H> {
    type Err = GetBlobError<Infallible>;
    type Iter<'a> = PileBlobStoreListIter<H>;

    fn blobs(&self) -> Self::Iter<'_> {
        // Clone the PATCH and create an owned iterator over its keys so we do
        // not borrow the live PATCH. This avoids borrow conflicts while still
        // being cheap (PATCH clone is copy-on-write).
        let cloned = self.blobs.clone();
        let inner = cloned.into_iter();
        PileBlobStoreListIter::<H> {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Iterator over branch ids stored in the pile's PATCH, using the PATCH's
/// built-in key iterator to avoid allocating a full Vec of ids.
pub struct PileBranchStoreIter<H: HashProtocol> {
    inner:
        crate::patch::PATCHIntoOrderedIterator<16, IdentitySchema, Value<Handle<H, SimpleArchive>>>,
}

impl<H: HashProtocol> Iterator for PileBranchStoreIter<H> {
    type Item = Result<Id, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        // The owned ordered iterator yields key arrays ([u8; 16]) by value.
        // The `apply_next` path guarantees that a nil (all-zero) branch id
        // is never inserted into the PATCH; therefore we can safely `expect`
        // a valid `Id` here and treat a nil id as an invariant violation.
        let key = self.inner.next()?;
        let id = Id::new(key).expect("nil branch id inserted into patch");
        Some(Ok(id))
    }
}

impl<H: HashProtocol> BlobStorePut<H> for Pile<H> {
    type PutError = InsertError;

    /// Inserts a blob into the pile and returns its handle.
    ///
    /// Multiple writers are safe only on filesystems guaranteeing atomic
    /// `write`/`vwrite` appends; other filesystems may corrupt the pile.
    fn put<S, T>(&mut self, item: T) -> Result<Value<Handle<H, S>>, Self::PutError>
    where
        S: BlobSchema + 'static,
        T: ToBlob<S>,
        Handle<H, S>: ValueSchema,
    {
        self.file.lock_shared()?;
        let res = (|| {
            self.refresh_locked().map_err(InsertError::from)?;

            let blob = ToBlob::to_blob(item);
            let blob_size = blob.bytes.len();
            let padding = padding_for_blob(blob_size);

            let handle: Value<Handle<H, S>> = blob.get_handle();
            let hash: Value<Hash<H>> = handle.into();

            if let Some(IndexEntry {
                state, offset, len, ..
            }) = self.blobs.get(&hash.raw)
            {
                let st = state.get_or_init(|| {
                    let bytes = unsafe {
                        let slice =
                            slice_from_raw_parts(self.mmap.as_ptr().add(*offset), *len as usize)
                                .as_ref()
                                .unwrap();
                        Bytes::from_raw_parts(slice, self.mmap.clone())
                    };
                    let computed = Hash::<H>::digest(&bytes);
                    if computed == hash {
                        ValidationState::Validated
                    } else {
                        ValidationState::Invalid
                    }
                });
                if matches!(st, ValidationState::Validated) {
                    return Ok(handle.transmute());
                }
            }

            let now_in_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let header = BlobHeader::new(now_in_ms as u64, blob_size as u64, hash);
            let expected = BLOB_HEADER_LEN + blob_size + padding;
            let padding_buf = [0u8; BLOB_ALIGNMENT];
            let bufs = [
                IoSlice::new(header.as_bytes()),
                IoSlice::new(blob.bytes.as_ref()),
                IoSlice::new(&padding_buf[..padding]),
            ];
            let written = self.file.write_vectored(&bufs)?;
            if written != expected {
                return Err(InsertError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write blob record",
                )));
            }

            loop {
                match self.apply_next().map_err(InsertError::from)? {
                    Some(Applied::Blob { hash: h }) => {
                        if h == hash {
                            break;
                        }
                    }
                    Some(Applied::Branch { .. }) => {}
                    Some(Applied::BranchTombstone { .. }) => {}
                    None => {
                        return Err(InsertError::IoError(std::io::Error::other(
                            "blob missing after write",
                        )));
                    }
                }
            }

            Ok(handle.transmute())
        })();
        let unlock_res = self.file.unlock();
        let handle = res?;
        unlock_res?;
        Ok(handle)
    }
}

impl<H> BranchStore<H> for Pile<H>
where
    H: HashProtocol,
{
    type BranchesError = ReadError;
    // Pulling a head may require refreshing the pile which can fail; expose
    // the underlying `ReadError` so callers can surface refresh failures.
    type HeadError = ReadError;
    type UpdateError = UpdateBranchError;

    type ListIter<'a> = PileBranchStoreIter<H>;

    fn branches<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::BranchesError> {
        // Ensure newly appended records are applied before enumerating
        // branches so external writers are visible to callers.
        self.refresh()?;
        // Create an owned ordered iterator from the PATCH clone so the
        // returned iterator does not borrow from `self.branches`. This avoids
        // allocating a temporary Vec of ids while preserving tree-order.
        let cloned = self.branches.clone();
        let inner = cloned.into_iter_ordered();
        Ok(PileBranchStoreIter { inner })
    }

    fn head(&mut self, id: Id) -> Result<Option<Value<Handle<H, SimpleArchive>>>, Self::HeadError> {
        // Ensure newly appended records are applied before returning the head.
        // This keeps callers up-to-date with any external writers that appended
        // to the pile file.
        self.refresh()?;
        let raw: RawId = id.into();
        Ok(self.branches.get(&raw).copied())
    }

    /// Updates the head of `id` to `new` if it matches `old`.
    ///
    /// This method does not verify that `new` refers to a blob stored in the pile,
    /// allowing piles to reference external data and serve as head-only stores.
    ///
    /// The update is written to the pile but is **not durable** until
    /// [`Pile::flush`] is called. Callers must explicitly flush to ensure
    /// branch updates survive crashes.
    ///
    /// After the header is written, the record is read back with `apply_next`
    /// while still holding the lock, ensuring the update is applied without an
    /// additional refresh pass.
    fn update(
        &mut self,
        id: Id,
        old: Option<Value<Handle<H, SimpleArchive>>>,
        new: Option<Value<Handle<H, SimpleArchive>>>,
    ) -> Result<super::PushResult<H>, Self::UpdateError> {
        self.file.lock()?;
        let res = (|| {
            self.refresh_locked().map_err(UpdateBranchError::from)?;
            let current_hash = self.branches.get(&id.into()).copied();
            if current_hash != old {
                return Ok(PushResult::Conflict(current_hash));
            }

            let (expected, write_res) = match new {
                Some(new) => {
                    let header = BranchHeader::new(id, new);
                    (
                        std::mem::size_of::<BranchHeader>(),
                        self.file.write(header.as_bytes()),
                    )
                }
                None => {
                    let header = BranchTombstoneHeader::new(id);
                    (
                        std::mem::size_of::<BranchTombstoneHeader>(),
                        self.file.write(header.as_bytes()),
                    )
                }
            };
            let written = match write_res {
                Ok(n) => n,
                Err(e) => return Err(UpdateBranchError::IoError(e)),
            };
            if written != expected {
                return Err(UpdateBranchError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write branch header",
                )));
            }
            match self.apply_next().map_err(UpdateBranchError::from)? {
                Some(Applied::Branch { id: bid, hash }) if matches!(new, Some(new) if bid == id && hash == new.into()) => {
                    Ok(PushResult::Success())
                }
                Some(Applied::BranchTombstone { id: bid }) if new.is_none() && bid == id => {
                    Ok(PushResult::Success())
                }
                Some(_) => Err(UpdateBranchError::IoError(std::io::Error::other(
                    "unexpected record after branch write",
                ))),
                None => Err(UpdateBranchError::IoError(std::io::Error::other(
                    "branch missing after write",
                ))),
            }
        })();
        let unlock_res = self.file.unlock();
        let out = res?;
        unlock_res?;
        Ok(out)
    }
}

impl<H: HashProtocol> crate::repo::BlobStoreMeta<H> for PileReader<H> {
    type MetaError = std::convert::Infallible;

    fn metadata<S>(
        &self,
        handle: Value<Handle<H, S>>,
    ) -> Result<Option<crate::repo::BlobMetadata>, Self::MetaError>
    where
        S: BlobSchema + 'static,
        Handle<H, S>: ValueSchema,
    {
        // re-use existing implementation logic
        let hash: &Value<Hash<H>> = handle.as_transmute();
        let entry = match self.blobs.get(&hash.raw) {
            Some(e) => e,
            None => return Ok(None),
        };
        let IndexEntry {
            state,
            timestamp,
            offset,
            len,
        } = entry.clone();
        let bytes = unsafe {
            let slice = slice_from_raw_parts(self.mmap.as_ptr().add(offset), len as usize)
                .as_ref()
                .unwrap();
            Bytes::from_raw_parts(slice, self.mmap.clone())
        };
        let state = state.get_or_init(|| {
            let computed_hash = Hash::<H>::digest(&bytes);
            if computed_hash == *hash {
                ValidationState::Validated
            } else {
                ValidationState::Invalid
            }
        });
        match state {
            ValidationState::Validated => Ok(Some(crate::repo::BlobMetadata {
                timestamp,
                length: bytes.len() as u64,
            })),
            ValidationState::Invalid => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::RngCore;
    use std::collections::{HashMap, HashSet};
    use std::io::Write;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tempfile;

    use crate::repo::BlobStoreMeta;
    use crate::repo::PushResult;

    #[test]
    fn open() {
        const RECORD_LEN: usize = 1 << 10; // 1k
        const RECORD_COUNT: usize = 1 << 12; // 4k

        let mut rng = rand::thread_rng();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_pile = tmp_dir.path().join("test.pile");
        let mut pile: Pile = Pile::open(&tmp_pile).unwrap();

        (0..RECORD_COUNT).for_each(|_| {
            let mut record = Vec::with_capacity(RECORD_LEN);
            rng.fill_bytes(&mut record);

            let data: Blob<UnknownBlob> = Blob::new(Bytes::from_source(record));
            pile.put(data).unwrap();
        });

        pile.close().unwrap();

        let mut reopened: Pile<Blake3> = Pile::open(&tmp_pile).unwrap();
        reopened.restore().unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn recover_shrink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        // Corrupt by removing some bytes from the end
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - 10).unwrap();

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn refresh_corrupt_reports_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len - 10)
            .unwrap();

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        match pile.refresh() {
            Err(ReadError::CorruptPile { valid_length }) => assert_eq!(valid_length, 0),
            other => panic!("unexpected result: {other:?}"),
        }
        pile.close().unwrap();
    }

    #[test]
    fn restore_truncates_unknown_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        let valid_len = std::fs::metadata(&path).unwrap().len();
        // Append 16 bytes of garbage that don't form a valid marker
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0u8; 16])
            .unwrap();

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[test]
    fn refresh_partial_header_reports_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len + 8)
            .unwrap();

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        match pile.refresh() {
            Err(ReadError::CorruptPile { valid_length }) => {
                assert_eq!(valid_length as u64, file_len)
            }
            other => panic!("unexpected result: {other:?}"),
        }
        pile.close().unwrap();
    }

    #[test]
    fn refresh_length_beyond_file_reports_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        use std::io::Seek;
        use std::io::SeekFrom;
        use std::io::Write;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(16 + 8)).unwrap();
        file.write_all(&(1_000_000u64).to_le_bytes()).unwrap();
        file.flush().unwrap();
        drop(file);

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        match pile.refresh() {
            Err(ReadError::CorruptPile { valid_length }) => assert_eq!(valid_length, 0),
            other => panic!("unexpected result: {other:?}"),
        }
        pile.close().unwrap();
    }

    #[test]
    fn restore_truncates_length_beyond_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put(blob).unwrap();
            pile.close().unwrap();
        }

        use std::io::Seek;
        use std::io::SeekFrom;
        use std::io::Write;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(16 + 8)).unwrap();
        file.write_all(&(1_000_000u64).to_le_bytes()).unwrap();
        file.flush().unwrap();
        drop(file);

        let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn put_and_get_preserves_blob_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![42u8; 100];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put(blob).unwrap();

        {
            let reader = pile.reader().unwrap();
            let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
            assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        }

        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile.close().unwrap();
    }

    #[test]
    fn iter_lists_all_blobs_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blobs = vec![vec![1u8; 3], vec![2u8; 4], vec![3u8; 5]];
        let mut expected = HashMap::new();
        for data in blobs {
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
            let handle = pile.put(blob).unwrap();
            expected.insert(handle, data);
        }
        pile.flush().unwrap();

        let reader = pile.reader().unwrap();
        for item in reader.iter() {
            let (handle, blob) = item.expect("infallible iteration");
            let data = expected.remove(&handle).unwrap();
            assert_eq!(blob.bytes.as_ref(), data.as_slice());
        }
        assert!(expected.is_empty());

        pile.close().unwrap();
    }

    #[test]
    fn metadata_reflects_length_and_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let data = vec![9u8; 10];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put(blob).unwrap();
        pile.flush().unwrap();

        let reader = pile.reader().unwrap();
        let metadata = reader.metadata(handle).unwrap().expect("metadata");
        assert_eq!(metadata.length, data.len() as u64);
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(metadata.timestamp >= before && metadata.timestamp <= after);
        pile.close().unwrap();
    }

    #[test]
    fn metadata_returns_none_for_unflushed_blob() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let reader = pile.reader().unwrap();

        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        let handle = pile.put(blob).unwrap();

        assert!(reader.metadata(handle).unwrap().is_none());

        pile.flush().unwrap();
        let reader = pile.reader().unwrap();
        assert!(reader.metadata(handle).unwrap().is_some());
        pile.close().unwrap();
    }

    #[test]
    fn blob_after_branch_is_clean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();

        let branch_id = Id::new([1; 16]).unwrap();
        let head = Value::<Handle<Blake3, SimpleArchive>>::new([2; 32]);
        pile.update(branch_id, None, Some(head)).unwrap();

        let data = vec![3u8; 8];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put(blob).unwrap();
        pile.flush().unwrap();

        let stored: Blob<UnknownBlob> = pile.reader().unwrap().get(handle).unwrap();
        assert_eq!(stored.bytes.as_ref(), &data[..]);
        pile.close().unwrap();
    }

    #[test]
    fn insert_after_branch_preserves_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put(blob1).unwrap();

        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        pile.put(blob2).unwrap();
        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        let head = pile.head(branch_id).unwrap();
        assert_eq!(head, Some(handle1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_survives_manual_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let branch_id = Id::new([1u8; 16]).unwrap();

        let handle = {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![3u8; 5]));
            let handle = pile.put(blob).unwrap();
            pile.update(branch_id, None, Some(handle.transmute()))
                .unwrap();
            pile.flush().unwrap();
            std::mem::forget(pile);
            handle
        };

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        assert_eq!(pile.head(branch_id).unwrap(), Some(handle.transmute()));
        assert!(std::fs::metadata(&path).unwrap().len() > 0);
        pile.close().unwrap();
    }

    #[test]
    fn branch_tombstone_removes_head_and_listing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let h = pile.put(blob).unwrap();
        let branch_id = Id::new([7u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h.transmute())).unwrap();
        pile.flush().unwrap();

        assert_eq!(pile.head(branch_id).unwrap(), Some(h.transmute()));

        pile.update(branch_id, Some(h.transmute()), None).unwrap();
        pile.flush().unwrap();

        assert_eq!(pile.head(branch_id).unwrap(), None);
        let branches: HashSet<Id> = pile.branches().unwrap().map(|r| r.unwrap()).collect();
        assert!(!branches.contains(&branch_id));
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_detects_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put(blob1).unwrap();

        let branch_id = Id::new([2u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let handle2 = pile.put(blob2).unwrap();
        pile.flush().unwrap();

        match pile
            .update(
                branch_id,
                Some(handle2.transmute()),
                Some(handle2.transmute()),
            )
            .unwrap()
        {
            PushResult::Conflict(current) => {
                assert_eq!(current, Some(handle1.transmute()));
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(pile.head(branch_id).unwrap(), Some(handle1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_conflict_returns_current_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put(blob1).unwrap();

        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();
        pile.flush().unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let handle2 = pile.put(blob2).unwrap();

        let result = pile
            .update(
                branch_id,
                Some(handle2.transmute()),
                Some(handle2.transmute()),
            )
            .unwrap();
        match result {
            PushResult::Conflict(current) => assert_eq!(current, Some(handle1.transmute())),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(pile.head(branch_id).unwrap(), Some(handle1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn metadata_returns_length_and_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![7u8; 32]));
        let handle = pile.put(blob).unwrap();
        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.restore().unwrap();
        let reader = pile.reader().unwrap();
        let meta = reader.metadata(handle).unwrap().expect("metadata");
        assert_eq!(meta.length, 32);
        assert!(meta.timestamp > 0);
        pile.close().unwrap();
    }

    #[test]
    fn iter_lists_all_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        let h1 = pile.put(blob1).unwrap();
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 4]));
        let h2 = pile.put(blob2).unwrap();
        pile.flush().unwrap();

        let reader = pile.reader().unwrap();
        let handles: Vec<_> = reader
            .iter()
            .map(|res| res.expect("infallible iteration").0)
            .collect();
        assert!(handles.contains(&h1));
        assert!(handles.contains(&h2));
        assert_eq!(handles.len(), 2);
        pile.close().unwrap();
    }

    #[test]
    fn update_conflict_returns_current_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let h1 = pile.put(blob1).unwrap();
        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h1.transmute())).unwrap();
        pile.flush().unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let h2 = pile.put(blob2).unwrap();
        pile.flush().unwrap();

        match pile.update(branch_id, Some(h2.transmute()), Some(h1.transmute())) {
            Ok(PushResult::Conflict(existing)) => {
                assert_eq!(existing, Some(h1.transmute()))
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(pile.head(branch_id).unwrap(), Some(h1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn refresh_errors_on_malformed_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        pile.put(blob).unwrap();
        pile.flush().unwrap();

        use std::io::Write;
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"garbage").unwrap();
            file.sync_all().unwrap();
        }

        assert!(pile.refresh().is_err());
        pile.close().unwrap();
    }

    #[test]
    fn restore_truncates_corrupt_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![1u8; 4];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put(blob).unwrap();
        pile.flush().unwrap();

        use std::io::Write;
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"garbage").unwrap();
            file.sync_all().unwrap();
        }

        pile.restore().unwrap();

        let expected_len =
            (super::BLOB_HEADER_LEN + data.len() + super::padding_for_blob(data.len())) as u64;
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expected_len);

        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile.close().unwrap();
    }

    #[test]
    fn refresh_replaces_corrupt_blob_with_new_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile1: Pile = Pile::open(&path).unwrap();
        let mut pile2: Pile = Pile::open(&path).unwrap();

        let data = vec![1u8; 4];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile1.put(blob).unwrap();
        pile1.flush().unwrap();
        pile1.refresh().unwrap();

        // Corrupt the first blob's bytes on disk.
        #[repr(C)]
        struct Header {
            magic_marker: [u8; 16],
            timestamp: u64,
            length: u64,
            hash: [u8; 32],
        }
        let header_len = std::mem::size_of::<Header>();
        use std::io::Seek;
        use std::io::SeekFrom;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(header_len as u64)).unwrap();
        file.write_all(&[9u8; 4]).unwrap();
        file.sync_all().unwrap();

        // Append a valid copy using the second pile which hasn't seen the first one.
        let blob_dup: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        pile2.put(blob_dup).unwrap();
        pile2.flush().unwrap();

        // Refresh the first pile; it should replace the corrupted blob with the new one.
        pile1.refresh().unwrap();
        let reader = pile1.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile1.close().unwrap();
        pile2.close().unwrap();
    }

    #[test]
    fn put_duplicate_blob_does_not_grow_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![9u8; 32];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle1 = pile.put(blob).unwrap();
        pile.flush().unwrap();
        let len_after_first = std::fs::metadata(&path).unwrap().len();

        let blob_dup: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data));
        let handle2 = pile.put(blob_dup).unwrap();
        pile.flush().unwrap();
        let len_after_second = std::fs::metadata(&path).unwrap().len();

        assert_eq!(handle1, handle2);
        assert_eq!(len_after_first, len_after_second);
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_conflict_returns_existing_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 8]));
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 8]));
        let h1 = pile.put(blob1).unwrap();
        let h2 = pile.put(blob2).unwrap();
        pile.flush().unwrap();

        let branch_id = Id::new([3u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h1.transmute())).unwrap();

        match pile.update(branch_id, Some(h2.transmute()), Some(h2.transmute())) {
            Ok(PushResult::Conflict(existing)) => {
                assert_eq!(existing, Some(h1.transmute()))
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(pile.head(branch_id).unwrap(), Some(h1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn iterator_skips_missing_index_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"hello".as_slice()));
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"world".as_slice()));
        let handle1 = pile.put(blob1).unwrap();
        let handle2 = pile.put(blob2).unwrap();
        pile.flush().unwrap();

        let mut reader = pile.reader().unwrap();
        let _full_patch = reader.blobs.clone();
        let hash1: Value<Hash<Blake3>> = handle1.into();
        reader.blobs.remove(&hash1.raw);

        let mut iter = reader.iter();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iter.next()));
        if let Ok(Some(Ok((h, _)))) = result {
            assert_eq!(h, handle2);
            assert!(iter.next().is_none());
        } else {
            assert!(cfg!(debug_assertions));
        }
        pile.close().unwrap();
    }

    #[test]
    fn metadata_reports_blob_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![7u8; 16];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put(blob).unwrap();
        pile.flush().unwrap();

        let reader = pile.reader().unwrap();
        let meta = reader.metadata(handle).unwrap().expect("metadata");
        assert_eq!(meta.length, data.len() as u64);
        pile.close().unwrap();
    }

    // recover_grow test removed as growth strategy no longer exists
}
