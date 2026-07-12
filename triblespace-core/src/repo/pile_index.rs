//! Experimental persistent locator snapshots for [`Pile`](crate::repo::pile::Pile).
//!
//! A snapshot is a derived cache over an exact, immutable prefix of a pile.
//! It is deliberately separate from the authoritative append-only pile file:
//! deleting it merely restores the normal PATCH replay path.  The first
//! implementation is static and opt-in. `Pile::open_indexed` may compose the
//! immutable mapped prefix with a canonical in-memory tail; incremental index
//! publication is intentionally left for later work.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anybytes::Bytes;

use crate::blob::encodings::UnknownBlob;
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::prelude::blobencodings::SimpleArchive;

use super::pile::{decode_record, PileRecord, PileRecordContent, PileRecords, ReadError};

// Minted with `trible genid` for the `.pidx` format on 2026-07-12.
const INDEX_MAGIC: [u8; 16] = hex_literal::hex!("080FB58E9F63E801C625DB2F2EFA292B");
const INDEX_VERSION: u32 = 1;
const INDEX_HEADER_LEN: usize = 256;

const BLOB_RECORD_LEN: usize = 56;
const PIN_RECORD_LEN: usize = 56;
const WEAK_RECORD_LEN: usize = 40;

const PIN_OP_RECORD_LEN: usize = 64;
const WEAK_OP_RECORD_LEN: usize = 48;

/// A spill chunk is intentionally small enough that building a snapshot never
/// replaces the Pile's corpus-scale PATCH with a corpus-scale sorting vector.
#[cfg(not(test))]
const SPILL_CHUNK_BYTES: usize = 4 * 1024 * 1024;
#[cfg(test)]
const SPILL_CHUNK_BYTES: usize = 4 * 1024;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Location and metadata for one blob payload in a pile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PileBlobLocator {
    /// Absolute byte offset at which the unpadded blob payload begins.
    pub payload_offset: u64,
    /// Unpadded payload length.
    pub payload_len: u64,
    /// Millisecond timestamp stored in the blob record header.
    pub timestamp: u64,
}

/// Minimal read surface shared by the current PATCH oracle and the mapped
/// static proof.
///
/// The methods return sorted, logical state: duplicate blob hashes are
/// collapsed with Pile replay semantics, while pin and weak-pin operations are
/// last-writer-wins by pile offset.
pub trait PileIndex {
    /// Streaming blob-handle iterator.
    type BlobHandles<'a>: Iterator<Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>>
    where
        Self: 'a;
    /// Streaming pin-id iterator.
    type PinIds<'a>: Iterator<Item = Result<Id, PileIndexError>>
    where
        Self: 'a;
    /// Streaming weak-pin iterator.
    type WeakPinHandles<'a>: Iterator<Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>>
    where
        Self: 'a;

    /// Exact pile byte length represented by this index view.
    fn covered_len(&self) -> u64;

    /// Finds a blob payload locator by its content handle.
    fn blob_locator(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<PileBlobLocator>, PileIndexError>;

    /// Lists all unique blob handles in byte order. This diagnostic trait
    /// surface may validate every locator against the authoritative pile;
    /// `PileReader` uses a separate owned key stream after the complete
    /// sidecar checksum and ordering checks, avoiding a sparse pile-header
    /// fault for every listed key.
    fn blob_handles(&self) -> Self::BlobHandles<'_>;

    /// Returns the current head of a pin.
    fn pin_head(&self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, PileIndexError>;

    /// Lists active pin ids in byte order.
    fn pin_ids(&self) -> Self::PinIds<'_>;

    /// Tests whether a weak-pin cell is active.
    fn has_weak_pin(&self, handle: Inline<Handle<UnknownBlob>>) -> Result<bool, PileIndexError> {
        for candidate in self.weak_pin_handles() {
            if candidate? == handle {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Lists active weak-pin handles in byte order.
    fn weak_pin_handles(&self) -> Self::WeakPinHandles<'_>;
}

/// Outcome of a static snapshot build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticPileIndexBuildStats {
    /// Number of raw pile records decoded.
    pub pile_records: u64,
    /// Unique blob locators emitted.
    pub blobs: u64,
    /// Active pins emitted after tombstones were applied.
    pub pins: u64,
    /// Active weak pins emitted after weak-unpins were applied.
    pub weak_pins: u64,
    /// Final snapshot length in bytes.
    pub index_bytes: u64,
}

/// Failure while building or opening a static locator snapshot.
#[derive(Debug)]
pub enum PileIndexError {
    /// Underlying filesystem failure.
    Io(io::Error),
    /// Failure reading the derived index cache.
    IndexIo(io::Error),
    /// Failure reading the authoritative pile.
    PileIo(io::Error),
    /// The authoritative pile could not be decoded.
    Pile(ReadError),
    /// The snapshot bytes are malformed or fail a checksum.
    InvalidIndex(String),
    /// The snapshot belongs to a different or differently-sized pile.
    StaleIndex(String),
    /// The pile changed while a static snapshot was being built.
    PileChanged,
    /// The requested index output resolves to the authoritative pile itself.
    IndexAliasesPile,
    /// Stable file identity is not implemented for this platform.
    UnsupportedPlatform,
    /// A mapped locator no longer names the exact record it claims to name.
    StaleLocator { offset: u64 },
}

impl fmt::Display for PileIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::IndexIo(err) => write!(f, "pile-index I/O error: {err}"),
            Self::PileIo(err) => write!(f, "pile I/O error: {err}"),
            Self::Pile(err) => write!(f, "pile error: {err}"),
            Self::InvalidIndex(reason) => write!(f, "invalid pile index: {reason}"),
            Self::StaleIndex(reason) => write!(f, "stale pile index: {reason}"),
            Self::PileChanged => write!(f, "pile changed while its static index was built"),
            Self::IndexAliasesPile => {
                write!(f, "pile index output aliases the authoritative pile file")
            }
            Self::UnsupportedPlatform => {
                write!(f, "static pile indexes require a stable OS file identity")
            }
            Self::StaleLocator { offset } => {
                write!(f, "pile index locator at byte {offset} no longer matches")
            }
        }
    }
}

impl Error for PileIndexError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::IndexIo(err) => Some(err),
            Self::PileIo(err) => Some(err),
            Self::Pile(err) => Some(err),
            Self::InvalidIndex(_)
            | Self::StaleIndex(_)
            | Self::PileChanged
            | Self::IndexAliasesPile
            | Self::UnsupportedPlatform
            | Self::StaleLocator { .. } => None,
        }
    }
}

impl From<io::Error> for PileIndexError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ReadError> for PileIndexError {
    fn from(value: ReadError) -> Self {
        Self::Pile(value)
    }
}

/// Read-only mmap-style view of one exact static `.pidx` snapshot.
#[derive(Debug, Clone)]
pub struct MappedPileIndex {
    pile: Bytes,
    index: Bytes,
    covered_len: u64,
    blobs_offset: usize,
    blobs_count: usize,
    pins_offset: usize,
    pins_count: usize,
    weak_offset: usize,
    weak_count: usize,
}

/// Streaming iterator over a mapped snapshot's blob keys.
pub struct MappedBlobHandles<'a> {
    index: &'a MappedPileIndex,
    position: usize,
}

/// Owned counterpart used by reader snapshots whose iterator must not borrow
/// a live `Pile`. Cloning `MappedPileIndex` only bumps the two `Bytes` owners.
pub(crate) struct OwnedMappedBlobHandles {
    index: MappedPileIndex,
    position: usize,
}

impl Iterator for OwnedMappedBlobHandles {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.index.blobs_count {
            return None;
        }
        let record = self.index.blob_record(self.position);
        self.position += 1;
        // `open_on_file` already checksums the complete index body and proves
        // this table strictly sorted and unique. Live logical-key scans can
        // therefore stay in the compact sidecar instead of randomly touching
        // one authoritative pile header per key. Point reads still validate
        // the selected locator against the pile before exposing bytes.
        Some(Ok(Inline::new(record.key)))
    }
}

impl Iterator for MappedBlobHandles<'_> {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.index.blobs_count {
            return None;
        }
        let record = self.index.blob_record(self.position);
        self.position += 1;
        Some(
            self.index
                .validate_blob(record)
                .map(|_| Inline::new(record.key)),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.index.blobs_count - self.position;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MappedBlobHandles<'_> {}

/// Streaming iterator over a mapped snapshot's active pin ids.
pub struct MappedPinIds<'a> {
    index: &'a MappedPileIndex,
    position: usize,
}

impl Iterator for MappedPinIds<'_> {
    type Item = Result<Id, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.index.pins_count {
            return None;
        }
        let record = self.index.pin_record(self.position);
        self.position += 1;
        Some(self.index.validate_pin(record).and_then(|()| {
            Id::new(record.key).ok_or_else(|| invalid("snapshot contains a nil pin id"))
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.index.pins_count - self.position;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MappedPinIds<'_> {}

/// Streaming iterator over a mapped snapshot's active weak pins.
pub struct MappedWeakPinHandles<'a> {
    index: &'a MappedPileIndex,
    position: usize,
}

impl Iterator for MappedWeakPinHandles<'_> {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position == self.index.weak_count {
            return None;
        }
        let record = self.index.weak_record(self.position);
        self.position += 1;
        Some(
            self.index
                .validate_weak(record)
                .map(|()| Inline::new(record.key)),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.index.weak_count - self.position;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MappedWeakPinHandles<'_> {}

impl MappedPileIndex {
    pub(crate) fn owned_blob_handles(&self) -> OwnedMappedBlobHandles {
        OwnedMappedBlobHandles {
            index: self.clone(),
            position: 0,
        }
    }
    /// Builds and atomically installs a static snapshot for `pile_path`.
    ///
    /// The builder uses the canonical [`PileRecords`] decoder and spills
    /// sorted runs bounded by `SPILL_CHUNK_BYTES`. It fails if the pile
    /// changes during the scan; it never publishes a partial snapshot.
    pub fn build(
        pile_path: &Path,
        index_path: &Path,
    ) -> Result<StaticPileIndexBuildStats, PileIndexError> {
        build_static_index(pile_path, index_path)
    }

    /// Opens and validates an exact static snapshot.
    ///
    /// Static snapshots deliberately do not cover a tail: if the pile has
    /// grown or shrunk since the build, this returns [`PileIndexError::StaleIndex`]
    /// so the caller can use the PATCH oracle or rebuild.
    pub fn open(pile_path: &Path, index_path: &Path) -> Result<Self, PileIndexError> {
        let pile_file = File::open(pile_path).map_err(PileIndexError::PileIo)?;
        let pile_len = pile_file.metadata().map_err(PileIndexError::PileIo)?.len();
        let pile = if pile_len == 0 {
            Bytes::empty()
        } else {
            let pile_len = usize_from_u64(pile_len, "pile length")?;
            // SAFETY: piles are immutable below their append point. Keep the
            // exact descriptor that supplied the identity alive until after
            // the mapping has been established.
            unsafe {
                Bytes::map_file_region(&pile_file, 0, pile_len).map_err(PileIndexError::PileIo)?
            }
        };
        Self::open_on_file(&pile_file, pile, index_path, true)
    }

    /// Opens a snapshot as an immutable prefix of an already-open pile.
    ///
    /// `pile` must be a bounded view backed by the same descriptor as
    /// `pile_file`. Unlike [`Self::open`], appends after `covered_len` are
    /// accepted; callers replay that canonical tail themselves. This is kept
    /// crate-private so the safe public entry point can bind the descriptor,
    /// mapping, watermark, and replay state as one operation.
    pub(crate) fn open_prefix_on_file(
        pile_file: &File,
        pile: Bytes,
        index_path: &Path,
    ) -> Result<Self, PileIndexError> {
        Self::open_on_file(pile_file, pile, index_path, false)
    }

    fn open_on_file(
        pile_file: &File,
        pile: Bytes,
        index_path: &Path,
        require_exact_len: bool,
    ) -> Result<Self, PileIndexError> {
        let index_file = File::open(index_path).map_err(PileIndexError::IndexIo)?;
        let index_len = index_file
            .metadata()
            .map_err(PileIndexError::IndexIo)?
            .len() as usize;
        if index_len < INDEX_HEADER_LEN {
            return Err(invalid("file is shorter than its header"));
        }
        let index = unsafe { Bytes::map_file(&index_file).map_err(PileIndexError::IndexIo)? };
        let header = ParsedHeader::parse(&index)?;

        let pile_meta = pile_file.metadata().map_err(PileIndexError::PileIo)?;
        let actual_identity = file_identity(&pile_meta)?;
        if actual_identity != header.identity {
            return Err(stale("pile file identity changed"));
        }
        let covered_len = usize_from_u64(header.covered_len, "covered length")?;
        if pile_meta.len() < header.covered_len || pile.len() < covered_len {
            return Err(stale("pile is shorter than the indexed prefix"));
        }
        if require_exact_len && pile_meta.len() != header.covered_len {
            return Err(stale("pile length differs from the static snapshot"));
        }
        header.validate_content(&index)?;
        validate_sorted_tables(&index, &header)?;

        validate_anchors(&pile, &header)?;
        let final_meta = pile_file.metadata().map_err(PileIndexError::PileIo)?;
        if file_identity(&final_meta)? != header.identity
            || final_meta.len() < header.covered_len
            || (require_exact_len && final_meta.len() != header.covered_len)
        {
            return Err(stale("pile changed while the index was attached"));
        }
        let pile = pile.slice(..covered_len);

        Ok(Self {
            pile,
            index,
            covered_len: header.covered_len,
            blobs_offset: header.blobs_offset,
            blobs_count: header.blobs_count,
            pins_offset: header.pins_offset,
            pins_count: header.pins_count,
            weak_offset: header.weak_offset,
            weak_count: header.weak_count,
        })
    }

    /// Best-effort cache open. Missing, malformed, stale, and identity-mismatched
    /// snapshots return `Ok(None)` so callers can transparently use PATCH.
    pub fn open_or_none(
        pile_path: &Path,
        index_path: &Path,
    ) -> Result<Option<Self>, PileIndexError> {
        match Self::open(pile_path, index_path) {
            Ok(index) => Ok(Some(index)),
            Err(PileIndexError::IndexIo(_))
            | Err(PileIndexError::InvalidIndex(_))
            | Err(PileIndexError::StaleIndex(_))
            | Err(PileIndexError::StaleLocator { .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn blob_record(&self, index: usize) -> BlobSnapshotRecord {
        let start = self.blobs_offset + index * BLOB_RECORD_LEN;
        BlobSnapshotRecord::decode_slice(&self.index[start..start + BLOB_RECORD_LEN])
    }

    fn pin_record(&self, index: usize) -> PinSnapshotRecord {
        let start = self.pins_offset + index * PIN_RECORD_LEN;
        PinSnapshotRecord::decode(&self.index[start..start + PIN_RECORD_LEN])
    }

    fn weak_record(&self, index: usize) -> WeakSnapshotRecord {
        let start = self.weak_offset + index * WEAK_RECORD_LEN;
        WeakSnapshotRecord::decode(&self.index[start..start + WEAK_RECORD_LEN])
    }

    fn find_blob(&self, key: &[u8; 32]) -> Option<BlobSnapshotRecord> {
        let mut lo = 0usize;
        let mut hi = self.blobs_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let record = self.blob_record(mid);
            match record.key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(record),
            }
        }
        None
    }

    fn find_pin(&self, key: &[u8; 16]) -> Option<PinSnapshotRecord> {
        let mut lo = 0usize;
        let mut hi = self.pins_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let record = self.pin_record(mid);
            match record.key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(record),
            }
        }
        None
    }

    fn find_weak(&self, key: &[u8; 32]) -> Option<WeakSnapshotRecord> {
        let mut lo = 0usize;
        let mut hi = self.weak_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let record = self.weak_record(mid);
            match record.key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(record),
            }
        }
        None
    }

    fn validate_blob(&self, record: BlobSnapshotRecord) -> Result<u64, PileIndexError> {
        validate_blob_record(
            &self.pile,
            &record.key,
            record.record_offset,
            record.payload_len,
            record.timestamp,
            self.covered_len,
        )
    }

    fn validate_pin(&self, record: PinSnapshotRecord) -> Result<(), PileIndexError> {
        let offset = usize_from_u64(record.record_offset, "pin offset")?;
        let pile_record = decode_at(&self.pile, offset)?;
        if pile_record.offset as u64 + pile_record.len as u64 > self.covered_len {
            return Err(PileIndexError::StaleLocator {
                offset: record.record_offset,
            });
        }
        match pile_record.content {
            PileRecordContent::Branch { branch_id, head }
                if <[u8; 16]>::from(branch_id) == record.key && head.raw == record.head =>
            {
                Ok(())
            }
            _ => Err(PileIndexError::StaleLocator {
                offset: record.record_offset,
            }),
        }
    }

    fn validate_weak(&self, record: WeakSnapshotRecord) -> Result<(), PileIndexError> {
        let offset = usize_from_u64(record.record_offset, "weak-pin offset")?;
        let pile_record = decode_at(&self.pile, offset)?;
        if pile_record.offset as u64 + pile_record.len as u64 > self.covered_len {
            return Err(PileIndexError::StaleLocator {
                offset: record.record_offset,
            });
        }
        match pile_record.content {
            PileRecordContent::WeakPin { handle } if handle.raw == record.key => Ok(()),
            _ => Err(PileIndexError::StaleLocator {
                offset: record.record_offset,
            }),
        }
    }
}

impl PileIndex for MappedPileIndex {
    type BlobHandles<'a> = MappedBlobHandles<'a>;
    type PinIds<'a> = MappedPinIds<'a>;
    type WeakPinHandles<'a> = MappedWeakPinHandles<'a>;

    fn covered_len(&self) -> u64 {
        self.covered_len
    }

    fn blob_locator(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<PileBlobLocator>, PileIndexError> {
        let Some(record) = self.find_blob(&handle.raw) else {
            return Ok(None);
        };
        let payload_offset = self.validate_blob(record)?;
        Ok(Some(PileBlobLocator {
            payload_offset,
            payload_len: record.payload_len,
            timestamp: record.timestamp,
        }))
    }

    fn blob_handles(&self) -> Self::BlobHandles<'_> {
        MappedBlobHandles {
            index: self,
            position: 0,
        }
    }

    fn pin_head(&self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, PileIndexError> {
        let key: [u8; 16] = id.into();
        let Some(record) = self.find_pin(&key) else {
            return Ok(None);
        };
        self.validate_pin(record)?;
        Ok(Some(Inline::new(record.head)))
    }

    fn pin_ids(&self) -> Self::PinIds<'_> {
        MappedPinIds {
            index: self,
            position: 0,
        }
    }

    fn has_weak_pin(&self, handle: Inline<Handle<UnknownBlob>>) -> Result<bool, PileIndexError> {
        let Some(record) = self.find_weak(&handle.raw) else {
            return Ok(false);
        };
        self.validate_weak(record)?;
        Ok(true)
    }

    fn weak_pin_handles(&self) -> Self::WeakPinHandles<'_> {
        MappedWeakPinHandles {
            index: self,
            position: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn file_identity(metadata: &Metadata) -> Result<FileIdentity, PileIndexError> {
    use std::os::unix::fs::MetadataExt;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
fn file_identity(_metadata: &Metadata) -> Result<FileIdentity, PileIndexError> {
    Err(PileIndexError::UnsupportedPlatform)
}

#[derive(Debug)]
struct ParsedHeader {
    identity: FileIdentity,
    covered_len: u64,
    record_count: u64,
    last_record_offset: u64,
    last_record_len: u64,
    first_anchor: [u8; 32],
    last_anchor: [u8; 32],
    blobs_offset: usize,
    blobs_count: usize,
    pins_offset: usize,
    pins_count: usize,
    weak_offset: usize,
    weak_count: usize,
    content_checksum: [u8; 32],
}

impl ParsedHeader {
    fn parse(bytes: &[u8]) -> Result<Self, PileIndexError> {
        if bytes.len() < INDEX_HEADER_LEN || bytes[..16] != INDEX_MAGIC {
            return Err(invalid("bad magic or truncated header"));
        }
        if read_u32(bytes, 16)? != INDEX_VERSION {
            return Err(invalid("unsupported format version"));
        }
        if read_u32(bytes, 20)? as usize != INDEX_HEADER_LEN {
            return Err(invalid("unexpected header length"));
        }
        let expected_header_hash: [u8; 32] = bytes[224..256].try_into().unwrap();
        if *blake3::hash(&bytes[..224]).as_bytes() != expected_header_hash {
            return Err(invalid("header checksum mismatch"));
        }
        let declared_len = usize_from_u64(read_u64(bytes, 184)?, "index length")?;
        if declared_len != bytes.len() {
            return Err(invalid("declared index length does not match the file"));
        }
        let blobs_offset = usize_from_u64(read_u64(bytes, 136)?, "blob section offset")?;
        let blobs_count = usize_from_u64(read_u64(bytes, 144)?, "blob count")?;
        let pins_offset = usize_from_u64(read_u64(bytes, 152)?, "pin section offset")?;
        let pins_count = usize_from_u64(read_u64(bytes, 160)?, "pin count")?;
        let weak_offset = usize_from_u64(read_u64(bytes, 168)?, "weak section offset")?;
        let weak_count = usize_from_u64(read_u64(bytes, 176)?, "weak count")?;

        let expected_pins = section_end(blobs_offset, blobs_count, BLOB_RECORD_LEN)?;
        let expected_weak = section_end(pins_offset, pins_count, PIN_RECORD_LEN)?;
        let expected_end = section_end(weak_offset, weak_count, WEAK_RECORD_LEN)?;
        if blobs_offset != INDEX_HEADER_LEN
            || pins_offset != expected_pins
            || weak_offset != expected_weak
            || expected_end != bytes.len()
        {
            return Err(invalid("section bounds are not canonical"));
        }

        Ok(Self {
            identity: FileIdentity {
                device: read_u64(bytes, 24)?,
                inode: read_u64(bytes, 32)?,
            },
            covered_len: read_u64(bytes, 40)?,
            record_count: read_u64(bytes, 48)?,
            last_record_offset: read_u64(bytes, 56)?,
            last_record_len: read_u64(bytes, 64)?,
            first_anchor: bytes[72..104].try_into().unwrap(),
            last_anchor: bytes[104..136].try_into().unwrap(),
            blobs_offset,
            blobs_count,
            pins_offset,
            pins_count,
            weak_offset,
            weak_count,
            content_checksum: bytes[192..224].try_into().unwrap(),
        })
    }

    fn validate_content(&self, bytes: &[u8]) -> Result<(), PileIndexError> {
        if *blake3::hash(&bytes[INDEX_HEADER_LEN..]).as_bytes() != self.content_checksum {
            return Err(invalid("content checksum mismatch"));
        }
        Ok(())
    }
}

fn section_end(offset: usize, count: usize, width: usize) -> Result<usize, PileIndexError> {
    offset
        .checked_add(
            count
                .checked_mul(width)
                .ok_or_else(|| invalid("section size overflow"))?,
        )
        .ok_or_else(|| invalid("section end overflow"))
}

fn validate_sorted_tables(bytes: &[u8], header: &ParsedHeader) -> Result<(), PileIndexError> {
    let logical_count = header
        .blobs_count
        .checked_add(header.pins_count)
        .and_then(|count| count.checked_add(header.weak_count))
        .ok_or_else(|| invalid("logical record count overflow"))?;
    if logical_count as u64 > header.record_count {
        return Err(invalid("logical entries exceed decoded pile records"));
    }

    let mut previous_blob: Option<[u8; 32]> = None;
    for index in 0..header.blobs_count {
        let start = header.blobs_offset + index * BLOB_RECORD_LEN;
        let record = BlobSnapshotRecord::decode_slice(&bytes[start..start + BLOB_RECORD_LEN]);
        if previous_blob.is_some_and(|previous| previous >= record.key) {
            return Err(invalid("blob section is not strictly sorted and unique"));
        }
        let minimum_end = record
            .record_offset
            .checked_add(64)
            .and_then(|end| end.checked_add(record.payload_len))
            .ok_or_else(|| invalid("blob locator end overflow"))?;
        if minimum_end > header.covered_len {
            return Err(invalid("blob locator exceeds the covered pile prefix"));
        }
        previous_blob = Some(record.key);
    }

    let mut previous_pin: Option<[u8; 16]> = None;
    for index in 0..header.pins_count {
        let start = header.pins_offset + index * PIN_RECORD_LEN;
        let record = PinSnapshotRecord::decode(&bytes[start..start + PIN_RECORD_LEN]);
        if Id::new(record.key).is_none() {
            return Err(invalid("pin section contains a nil id"));
        }
        if previous_pin.is_some_and(|previous| previous >= record.key) {
            return Err(invalid("pin section is not strictly sorted and unique"));
        }
        if record
            .record_offset
            .checked_add(64)
            .is_none_or(|end| end > header.covered_len)
        {
            return Err(invalid("pin locator exceeds the covered pile prefix"));
        }
        previous_pin = Some(record.key);
    }

    let mut previous_weak: Option<[u8; 32]> = None;
    for index in 0..header.weak_count {
        let start = header.weak_offset + index * WEAK_RECORD_LEN;
        let record = WeakSnapshotRecord::decode(&bytes[start..start + WEAK_RECORD_LEN]);
        if previous_weak.is_some_and(|previous| previous >= record.key) {
            return Err(invalid(
                "weak-pin section is not strictly sorted and unique",
            ));
        }
        if record
            .record_offset
            .checked_add(64)
            .is_none_or(|end| end > header.covered_len)
        {
            return Err(invalid("weak-pin locator exceeds the covered pile prefix"));
        }
        previous_weak = Some(record.key);
    }
    Ok(())
}

fn validate_anchors(pile: &[u8], header: &ParsedHeader) -> Result<(), PileIndexError> {
    if header.record_count == 0 {
        if header.covered_len != 0
            || header.last_record_offset != 0
            || header.last_record_len != 0
            || header.first_anchor != [0; 32]
            || header.last_anchor != [0; 32]
        {
            return Err(stale("empty snapshot carries non-empty anchors"));
        }
        return Ok(());
    }
    if header.covered_len == 0 {
        return Err(stale("non-empty snapshot covers zero bytes"));
    }
    let first = decode_at(pile, 0).map_err(|_| stale("first record no longer decodes"))?;
    if record_anchor(pile, &first)? != header.first_anchor {
        return Err(stale("first record anchor changed"));
    }
    let last_offset = usize_from_u64(header.last_record_offset, "last record offset")?;
    let last = decode_at(pile, last_offset).map_err(|_| stale("last record no longer decodes"))?;
    if last.len as u64 != header.last_record_len
        || last.offset as u64 + last.len as u64 != header.covered_len
        || record_anchor(pile, &last)? != header.last_anchor
    {
        return Err(stale("last record anchor changed"));
    }
    Ok(())
}

fn decode_at(pile: &[u8], offset: usize) -> Result<PileRecord, PileIndexError> {
    let bytes = pile.get(offset..).ok_or(PileIndexError::StaleLocator {
        offset: offset as u64,
    })?;
    decode_record(bytes, offset).map_err(PileIndexError::Pile)
}

fn record_header_len(record: &PileRecord) -> usize {
    match record.content {
        PileRecordContent::Blob { data_offset, .. } => data_offset - record.offset,
        PileRecordContent::Branch { .. }
        | PileRecordContent::BranchTombstone { .. }
        | PileRecordContent::WeakPin { .. }
        | PileRecordContent::WeakUnpin { .. } => record.len,
    }
}

fn record_anchor(bytes: &[u8], record: &PileRecord) -> Result<[u8; 32], PileIndexError> {
    let end = record
        .offset
        .checked_add(record_header_len(record))
        .ok_or_else(|| invalid("record header end overflow"))?;
    let header = bytes
        .get(record.offset..end)
        .ok_or_else(|| invalid("record header lies outside the pile"))?;
    Ok(*blake3::hash(header).as_bytes())
}

fn validate_blob_record(
    pile: &[u8],
    key: &[u8; 32],
    record_offset: u64,
    payload_len: u64,
    expected_timestamp: u64,
    covered_len: u64,
) -> Result<u64, PileIndexError> {
    let decoded_offset = usize_from_u64(record_offset, "blob record offset")?;
    let record = decode_at(pile, decoded_offset)?;
    if let PileRecordContent::Blob {
        timestamp,
        hash,
        data_offset,
        data_len,
    } = record.content
    {
        if hash.raw == *key
            && data_len as u64 == payload_len
            && timestamp == expected_timestamp
            && record.offset as u64 + record.len as u64 <= covered_len
        {
            return Ok(data_offset as u64);
        }
    }
    Err(PileIndexError::StaleLocator {
        offset: record_offset,
    })
}

fn blob_payload_is_valid(pile: &[u8], record: BlobRunRecord) -> Result<bool, PileIndexError> {
    let start = usize_from_u64(
        validate_blob_record(
            pile,
            &record.key,
            record.record_offset,
            record.payload_len,
            record.timestamp,
            pile.len() as u64,
        )?,
        "blob payload offset",
    )?;
    let len = usize_from_u64(record.payload_len, "blob payload length")?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid("blob payload end overflow"))?;
    let payload = pile
        .get(start..end)
        .ok_or_else(|| invalid("blob payload lies outside the pile"))?;
    Ok(blake3::hash(payload).as_bytes() == &record.key)
}

fn build_static_index(
    pile_path: &Path,
    index_path: &Path,
) -> Result<StaticPileIndexBuildStats, PileIndexError> {
    let mut records = PileRecords::open(pile_path)?;
    let pile = records.bytes().clone();
    let initial_metadata = records.file_metadata()?;
    let identity = file_identity(&initial_metadata)?;
    if initial_metadata.len() as usize != pile.len() {
        return Err(PileIndexError::PileChanged);
    }
    match fs::metadata(index_path) {
        Ok(metadata) if file_identity(&metadata)? == identity => {
            return Err(PileIndexError::IndexAliasesPile);
        }
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(PileIndexError::IndexIo(err)),
    }

    let mut artifacts = TempArtifacts::default();
    let mut blobs = Spiller::<BlobRunRecord>::new(index_path, "blob");
    let mut pins = Spiller::<PinOpRecord>::new(index_path, "pin");
    let mut weak = Spiller::<WeakOpRecord>::new(index_path, "weak");

    let mut record_count = 0u64;
    let mut first_anchor = [0u8; 32];
    let mut last_anchor = [0u8; 32];
    let mut last_record_offset = 0u64;
    let mut last_record_len = 0u64;
    let mut valid_end = 0usize;

    for record in &mut records {
        let record = record?;
        if record_count == 0 {
            first_anchor = record_anchor(&pile, &record)?;
        }
        last_anchor = record_anchor(&pile, &record)?;
        last_record_offset = record.offset as u64;
        last_record_len = record.len as u64;
        valid_end = record.offset + record.len;
        record_count += 1;

        match record.content {
            PileRecordContent::Blob {
                timestamp,
                hash,
                data_len,
                ..
            } => blobs.push(BlobRunRecord {
                key: hash.raw,
                record_offset: record.offset as u64,
                payload_len: data_len as u64,
                timestamp,
            })?,
            PileRecordContent::Branch { branch_id, head } => pins.push(PinOpRecord {
                key: branch_id.into(),
                record_offset: record.offset as u64,
                tag: 1,
                head: head.raw,
            })?,
            PileRecordContent::BranchTombstone { branch_id } => pins.push(PinOpRecord {
                key: branch_id.into(),
                record_offset: record.offset as u64,
                tag: 0,
                head: [0; 32],
            })?,
            PileRecordContent::WeakPin { handle } => weak.push(WeakOpRecord {
                key: handle.raw,
                record_offset: record.offset as u64,
                tag: 1,
            })?,
            PileRecordContent::WeakUnpin { handle } => weak.push(WeakOpRecord {
                key: handle.raw,
                record_offset: record.offset as u64,
                tag: 0,
            })?,
        }
    }

    if valid_end != pile.len() {
        return Err(PileIndexError::PileChanged);
    }
    let final_metadata = records.file_metadata()?;
    if file_identity(&final_metadata)? != identity || final_metadata.len() != pile.len() as u64 {
        return Err(PileIndexError::PileChanged);
    }

    let blob_runs = blobs.finish()?;
    artifacts.paths.extend(blob_runs.iter().cloned());
    let pin_runs = pins.finish()?;
    artifacts.paths.extend(pin_runs.iter().cloned());
    let weak_runs = weak.finish()?;
    artifacts.paths.extend(weak_runs.iter().cloned());

    let output_temp = unique_temp_path(index_path, "snapshot");
    let output_file = create_owned_temp_file(&output_temp, &mut artifacts.paths)?;
    let mut output = BufWriter::new(output_file);
    output.write_all(&[0u8; INDEX_HEADER_LEN])?;

    let blobs_offset = INDEX_HEADER_LEN as u64;
    let blob_count = merge_blob_runs(&blob_runs, &pile, &mut output)?;
    let pins_offset = blobs_offset + blob_count * BLOB_RECORD_LEN as u64;
    let pin_count = merge_pin_runs(&pin_runs, &mut output)?;
    let weak_offset = pins_offset + pin_count * PIN_RECORD_LEN as u64;
    let weak_count = merge_weak_runs(&weak_runs, &mut output)?;
    output.flush()?;
    let mut output = output
        .into_inner()
        .map_err(|err| PileIndexError::Io(err.into_error()))?;
    let index_len = weak_offset + weak_count * WEAK_RECORD_LEN as u64;
    if output.metadata()?.len() != index_len {
        return Err(invalid("builder emitted a non-canonical snapshot length"));
    }

    let content_checksum = hash_file_range(&mut output, INDEX_HEADER_LEN as u64, index_len)?;
    let header = encode_header(HeaderFields {
        identity,
        covered_len: pile.len() as u64,
        record_count,
        last_record_offset,
        last_record_len,
        first_anchor,
        last_anchor,
        blobs_offset,
        blobs_count: blob_count,
        pins_offset,
        pins_count: pin_count,
        weak_offset,
        weak_count,
        index_len,
        content_checksum,
    });
    output.seek(SeekFrom::Start(0))?;
    output.write_all(&header)?;
    output.sync_all()?;
    drop(output);

    fs::rename(&output_temp, index_path)?;
    sync_parent_directory(index_path)?;

    Ok(StaticPileIndexBuildStats {
        pile_records: record_count,
        blobs: blob_count,
        pins: pin_count,
        weak_pins: weak_count,
        index_bytes: index_len,
    })
}

fn sync_parent_directory(path: &Path) -> Result<(), PileIndexError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let directory = File::open(parent.unwrap_or_else(|| Path::new(".")))?;
    directory.sync_all()?;
    Ok(())
}

fn hash_file_range(file: &mut File, start: u64, end: u64) -> Result<[u8; 32], PileIndexError> {
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = end
        .checked_sub(start)
        .ok_or_else(|| invalid("checksum range is inverted"))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    while remaining != 0 {
        let take = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..take])?;
        hasher.update(&buffer[..take]);
        remaining -= take as u64;
    }
    Ok(*hasher.finalize().as_bytes())
}

#[derive(Debug)]
struct HeaderFields {
    identity: FileIdentity,
    covered_len: u64,
    record_count: u64,
    last_record_offset: u64,
    last_record_len: u64,
    first_anchor: [u8; 32],
    last_anchor: [u8; 32],
    blobs_offset: u64,
    blobs_count: u64,
    pins_offset: u64,
    pins_count: u64,
    weak_offset: u64,
    weak_count: u64,
    index_len: u64,
    content_checksum: [u8; 32],
}

fn encode_header(fields: HeaderFields) -> [u8; INDEX_HEADER_LEN] {
    let mut bytes = [0u8; INDEX_HEADER_LEN];
    bytes[..16].copy_from_slice(&INDEX_MAGIC);
    put_u32(&mut bytes, 16, INDEX_VERSION);
    put_u32(&mut bytes, 20, INDEX_HEADER_LEN as u32);
    put_u64(&mut bytes, 24, fields.identity.device);
    put_u64(&mut bytes, 32, fields.identity.inode);
    put_u64(&mut bytes, 40, fields.covered_len);
    put_u64(&mut bytes, 48, fields.record_count);
    put_u64(&mut bytes, 56, fields.last_record_offset);
    put_u64(&mut bytes, 64, fields.last_record_len);
    bytes[72..104].copy_from_slice(&fields.first_anchor);
    bytes[104..136].copy_from_slice(&fields.last_anchor);
    put_u64(&mut bytes, 136, fields.blobs_offset);
    put_u64(&mut bytes, 144, fields.blobs_count);
    put_u64(&mut bytes, 152, fields.pins_offset);
    put_u64(&mut bytes, 160, fields.pins_count);
    put_u64(&mut bytes, 168, fields.weak_offset);
    put_u64(&mut bytes, 176, fields.weak_count);
    put_u64(&mut bytes, 184, fields.index_len);
    bytes[192..224].copy_from_slice(&fields.content_checksum);
    let header_checksum = blake3::hash(&bytes[..224]);
    bytes[224..256].copy_from_slice(header_checksum.as_bytes());
    bytes
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlobRunRecord {
    key: [u8; 32],
    record_offset: u64,
    payload_len: u64,
    timestamp: u64,
}

impl BlobRunRecord {
    fn decode_slice(bytes: &[u8]) -> Self {
        Self {
            key: bytes[..32].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            payload_len: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            timestamp: u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
        }
    }
}

impl SpillRecord for BlobRunRecord {
    const WIDTH: usize = BLOB_RECORD_LEN;

    fn encode(self, writer: &mut dyn Write) -> io::Result<()> {
        let mut bytes = [0u8; BLOB_RECORD_LEN];
        bytes[..32].copy_from_slice(&self.key);
        put_u64(&mut bytes, 32, self.record_offset);
        put_u64(&mut bytes, 40, self.payload_len);
        put_u64(&mut bytes, 48, self.timestamp);
        writer.write_all(&bytes)
    }

    fn decode(reader: &mut dyn Read) -> io::Result<Option<Self>> {
        let mut bytes = [0u8; BLOB_RECORD_LEN];
        if !read_exact_or_eof(reader, &mut bytes)? {
            return Ok(None);
        }
        Ok(Some(Self {
            key: bytes[..32].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            payload_len: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            timestamp: u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
        }))
    }
}

type BlobSnapshotRecord = BlobRunRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PinOpRecord {
    key: [u8; 16],
    record_offset: u64,
    tag: u8,
    head: [u8; 32],
}

impl SpillRecord for PinOpRecord {
    const WIDTH: usize = PIN_OP_RECORD_LEN;

    fn encode(self, writer: &mut dyn Write) -> io::Result<()> {
        let mut bytes = [0u8; PIN_OP_RECORD_LEN];
        bytes[..16].copy_from_slice(&self.key);
        put_u64(&mut bytes, 16, self.record_offset);
        bytes[24] = self.tag;
        bytes[32..64].copy_from_slice(&self.head);
        writer.write_all(&bytes)
    }

    fn decode(reader: &mut dyn Read) -> io::Result<Option<Self>> {
        let mut bytes = [0u8; PIN_OP_RECORD_LEN];
        if !read_exact_or_eof(reader, &mut bytes)? {
            return Ok(None);
        }
        Ok(Some(Self {
            key: bytes[..16].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            tag: bytes[24],
            head: bytes[32..64].try_into().unwrap(),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PinSnapshotRecord {
    key: [u8; 16],
    record_offset: u64,
    head: [u8; 32],
}

impl PinSnapshotRecord {
    fn encode(self, writer: &mut dyn Write) -> io::Result<()> {
        let mut bytes = [0u8; PIN_RECORD_LEN];
        bytes[..16].copy_from_slice(&self.key);
        put_u64(&mut bytes, 16, self.record_offset);
        bytes[24..56].copy_from_slice(&self.head);
        writer.write_all(&bytes)
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            key: bytes[..16].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            head: bytes[24..56].try_into().unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct WeakOpRecord {
    key: [u8; 32],
    record_offset: u64,
    tag: u8,
}

impl SpillRecord for WeakOpRecord {
    const WIDTH: usize = WEAK_OP_RECORD_LEN;

    fn encode(self, writer: &mut dyn Write) -> io::Result<()> {
        let mut bytes = [0u8; WEAK_OP_RECORD_LEN];
        bytes[..32].copy_from_slice(&self.key);
        put_u64(&mut bytes, 32, self.record_offset);
        bytes[40] = self.tag;
        writer.write_all(&bytes)
    }

    fn decode(reader: &mut dyn Read) -> io::Result<Option<Self>> {
        let mut bytes = [0u8; WEAK_OP_RECORD_LEN];
        if !read_exact_or_eof(reader, &mut bytes)? {
            return Ok(None);
        }
        Ok(Some(Self {
            key: bytes[..32].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            tag: bytes[40],
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WeakSnapshotRecord {
    key: [u8; 32],
    record_offset: u64,
}

impl WeakSnapshotRecord {
    fn encode(self, writer: &mut dyn Write) -> io::Result<()> {
        let mut bytes = [0u8; WEAK_RECORD_LEN];
        bytes[..32].copy_from_slice(&self.key);
        put_u64(&mut bytes, 32, self.record_offset);
        writer.write_all(&bytes)
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            key: bytes[..32].try_into().unwrap(),
            record_offset: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        }
    }
}

trait SpillRecord: Copy + Ord {
    const WIDTH: usize;
    fn encode(self, writer: &mut dyn Write) -> io::Result<()>;
    fn decode(reader: &mut dyn Read) -> io::Result<Option<Self>>;
}

struct Spiller<T: SpillRecord> {
    output: PathBuf,
    kind: &'static str,
    records: Vec<T>,
    runs: Vec<PathBuf>,
}

impl<T: SpillRecord> Spiller<T> {
    fn new(output: &Path, kind: &'static str) -> Self {
        Self {
            output: output.to_path_buf(),
            kind,
            records: Vec::with_capacity((SPILL_CHUNK_BYTES / T::WIDTH).max(1)),
            runs: Vec::new(),
        }
    }

    fn push(&mut self, record: T) -> Result<(), PileIndexError> {
        self.records.push(record);
        if self.records.len() * T::WIDTH >= SPILL_CHUNK_BYTES {
            self.flush_run()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), PileIndexError> {
        if self.records.is_empty() {
            return Ok(());
        }
        self.records.sort_unstable();
        let path = unique_temp_path(&self.output, self.kind);
        let file = create_owned_temp_file(&path, &mut self.runs)?;
        let mut writer = BufWriter::new(file);
        for record in self.records.drain(..) {
            record.encode(&mut writer)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<PathBuf>, PileIndexError> {
        self.flush_run()?;
        Ok(std::mem::take(&mut self.runs))
    }
}

impl<T: SpillRecord> Drop for Spiller<T> {
    fn drop(&mut self) {
        for path in &self.runs {
            let _ = fs::remove_file(path);
        }
    }
}

fn merge_blob_runs(
    runs: &[PathBuf],
    pile: &[u8],
    writer: &mut dyn Write,
) -> Result<u64, PileIndexError> {
    let mut current: Option<BlobRunRecord> = None;
    let mut current_valid = false;
    let mut count = 0u64;
    merge_runs(runs, |record: BlobRunRecord| {
        match current {
            None => current = Some(record),
            Some(existing) if existing.key != record.key => {
                existing.encode(writer)?;
                count += 1;
                current = Some(record);
                current_valid = false;
            }
            Some(existing) => {
                if !current_valid {
                    if blob_payload_is_valid(pile, existing)? {
                        current_valid = true;
                    } else {
                        current = Some(record);
                    }
                }
            }
        }
        Ok(())
    })?;
    if let Some(record) = current {
        record.encode(writer)?;
        count += 1;
    }
    Ok(count)
}

fn merge_pin_runs(runs: &[PathBuf], writer: &mut dyn Write) -> Result<u64, PileIndexError> {
    let mut current: Option<PinOpRecord> = None;
    let mut count = 0u64;
    merge_runs(runs, |record: PinOpRecord| {
        match current {
            Some(existing) if existing.key != record.key => {
                if existing.tag == 1 {
                    PinSnapshotRecord {
                        key: existing.key,
                        record_offset: existing.record_offset,
                        head: existing.head,
                    }
                    .encode(writer)?;
                    count += 1;
                }
                current = Some(record);
            }
            _ => current = Some(record),
        }
        Ok(())
    })?;
    if let Some(record) = current {
        if record.tag == 1 {
            PinSnapshotRecord {
                key: record.key,
                record_offset: record.record_offset,
                head: record.head,
            }
            .encode(writer)?;
            count += 1;
        }
    }
    Ok(count)
}

fn merge_weak_runs(runs: &[PathBuf], writer: &mut dyn Write) -> Result<u64, PileIndexError> {
    let mut current: Option<WeakOpRecord> = None;
    let mut count = 0u64;
    merge_runs(runs, |record: WeakOpRecord| {
        match current {
            Some(existing) if existing.key != record.key => {
                if existing.tag == 1 {
                    WeakSnapshotRecord {
                        key: existing.key,
                        record_offset: existing.record_offset,
                    }
                    .encode(writer)?;
                    count += 1;
                }
                current = Some(record);
            }
            _ => current = Some(record),
        }
        Ok(())
    })?;
    if let Some(record) = current {
        if record.tag == 1 {
            WeakSnapshotRecord {
                key: record.key,
                record_offset: record.record_offset,
            }
            .encode(writer)?;
            count += 1;
        }
    }
    Ok(count)
}

fn merge_runs<T, F>(runs: &[PathBuf], mut consume: F) -> Result<(), PileIndexError>
where
    T: SpillRecord,
    F: FnMut(T) -> Result<(), PileIndexError>,
{
    let mut readers: Vec<BufReader<File>> = runs
        .iter()
        .map(File::open)
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .map(BufReader::new)
        .collect();
    let mut heap: BinaryHeap<Reverse<(T, usize)>> = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = T::decode(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    while let Some(Reverse((record, run))) = heap.pop() {
        consume(record)?;
        if let Some(next) = T::decode(&mut readers[run])? {
            heap.push(Reverse((next, run)));
        }
    }
    Ok(())
}

#[derive(Default)]
struct TempArtifacts {
    paths: Vec<PathBuf>,
}

impl Drop for TempArtifacts {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

/// Creates a previously unowned temp path, then transfers cleanup ownership
/// before the caller can write anything. A collision is never registered and
/// therefore cannot delete a pre-existing file when the owner drops.
fn create_owned_temp_file(
    path: &Path,
    owned_paths: &mut Vec<PathBuf>,
) -> Result<File, PileIndexError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    owned_paths.push(path.to_path_buf());
    Ok(file)
}

fn unique_temp_path(output: &Path, kind: &str) -> PathBuf {
    let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("pile.pidx");
    output.with_file_name(format!(
        ".{name}.{kind}.{}.{sequence}.tmp",
        std::process::id()
    ))
}

fn read_exact_or_eof(reader: &mut dyn Read, bytes: &mut [u8]) -> io::Result<bool> {
    let mut read = 0usize;
    while read < bytes.len() {
        match reader.read(&mut bytes[read..])? {
            0 if read == 0 => return Ok(false),
            0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            n => read += n,
        }
    }
    Ok(true)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, PileIndexError> {
    let raw = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid("truncated u32"))?;
    Ok(u32::from_le_bytes(raw.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, PileIndexError> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid("truncated u64"))?;
    Ok(u64::from_le_bytes(raw.try_into().unwrap()))
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn usize_from_u64(value: u64, field: &str) -> Result<usize, PileIndexError> {
    value
        .try_into()
        .map_err(|_| invalid(format!("{field} exceeds usize")))
}

fn invalid(reason: impl Into<String>) -> PileIndexError {
    PileIndexError::InvalidIndex(reason.into())
}

fn stale(reason: impl Into<String>) -> PileIndexError {
    PileIndexError::StaleIndex(reason.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Seek, SeekFrom};

    use crate::blob::Blob;
    use crate::repo::pile::{legacy_v1_blob_record_for_test, Pile};
    use crate::repo::{BlobStorePut, PinStore, WeakPinStore};

    fn new_paths() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let pile = dir.path().join("test.pile");
        let index = dir.path().join("test.pile.pidx");
        File::create(&pile).unwrap();
        (dir, pile, index)
    }

    fn build_fixture(pile_path: &Path) -> (Inline<Handle<UnknownBlob>>, Id) {
        let mut pile = Pile::open(pile_path).unwrap();
        let blob: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                b"payload".to_vec(),
            )))
            .unwrap();
        let branch = Id::new([7; 16]).unwrap();
        let head: Inline<Handle<SimpleArchive>> = blob.transmute();
        assert!(matches!(
            pile.update(branch, None, Some(head)).unwrap(),
            super::super::PushResult::Success()
        ));
        pile.pin_weak(blob).unwrap();
        pile.close().unwrap();
        (blob, branch)
    }

    #[test]
    fn static_snapshot_matches_patch_oracle() {
        let (_dir, pile_path, index_path) = new_paths();
        let (blob, branch) = build_fixture(&pile_path);

        let mut pile = Pile::open(&pile_path).unwrap();
        let oracle = pile.patch_index().unwrap();
        pile.close().unwrap();

        let stats = MappedPileIndex::build(&pile_path, &index_path).unwrap();
        assert_eq!(stats.blobs, 1);
        assert_eq!(stats.pins, 1);
        assert_eq!(stats.weak_pins, 1);
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();

        assert_eq!(mapped.covered_len(), oracle.covered_len());
        assert_eq!(
            mapped
                .blob_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            oracle
                .blob_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );
        assert_eq!(
            mapped.blob_locator(blob).unwrap(),
            oracle.blob_locator(blob).unwrap()
        );
        assert_eq!(
            mapped.pin_ids().collect::<Result<Vec<_>, _>>().unwrap(),
            oracle.pin_ids().collect::<Result<Vec<_>, _>>().unwrap()
        );
        assert_eq!(
            mapped.pin_head(branch).unwrap(),
            oracle.pin_head(branch).unwrap()
        );
        assert_eq!(
            mapped
                .weak_pin_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            oracle
                .weak_pin_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );
    }

    #[test]
    fn external_runs_preserve_replay_semantics_across_boundaries() {
        let (_dir, pile_path, index_path) = new_paths();
        let mut pile = Pile::open(&pile_path).unwrap();
        let target: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                b"target".to_vec(),
            )))
            .unwrap();
        pile.close().unwrap();
        let target_record = fs::read(&pile_path).unwrap();
        let target_payload_offset = match decode_record(&target_record, 0).unwrap().content {
            PileRecordContent::Blob { data_offset, .. } => data_offset as u64,
            _ => unreachable!(),
        };

        let mut pile = Pile::open(&pile_path).unwrap();
        let mut fillers: Vec<Inline<Handle<UnknownBlob>>> = Vec::new();
        for index in 0..90 {
            fillers.push(
                pile.put(Blob::<UnknownBlob>::new(Bytes::from_source(
                    format!("filler-{index:03}").into_bytes(),
                )))
                .unwrap(),
            );
        }

        let target_branch = Id::new([240; 16]).unwrap();
        let head: Inline<Handle<SimpleArchive>> = target.transmute();
        assert!(matches!(
            pile.update(target_branch, None, Some(head)).unwrap(),
            super::super::PushResult::Success()
        ));
        for byte in 1..=70 {
            let branch = Id::new([byte; 16]).unwrap();
            assert!(matches!(
                pile.update(branch, None, Some(head)).unwrap(),
                super::super::PushResult::Success()
            ));
        }
        assert!(matches!(
            pile.update(target_branch, Some(head), None).unwrap(),
            super::super::PushResult::Success()
        ));
        for byte in 71..=140 {
            let branch = Id::new([byte; 16]).unwrap();
            assert!(matches!(
                pile.update(branch, None, Some(head)).unwrap(),
                super::super::PushResult::Success()
            ));
        }
        assert!(matches!(
            pile.update(target_branch, None, Some(head)).unwrap(),
            super::super::PushResult::Success()
        ));

        pile.pin_weak(target).unwrap();
        for &handle in &fillers {
            pile.pin_weak(handle).unwrap();
        }
        pile.unpin_weak(target).unwrap();
        for &handle in &fillers {
            pile.unpin_weak(handle).unwrap();
        }
        pile.pin_weak(target).unwrap();
        pile.close().unwrap();

        OpenOptions::new()
            .append(true)
            .open(&pile_path)
            .unwrap()
            .write_all(&target_record)
            .unwrap();

        let mut pile = Pile::open(&pile_path).unwrap();
        let oracle = pile.patch_index().unwrap();
        pile.close().unwrap();
        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();

        assert_eq!(
            mapped
                .blob_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            oracle
                .blob_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );
        assert_eq!(
            mapped.pin_ids().collect::<Result<Vec<_>, _>>().unwrap(),
            oracle.pin_ids().collect::<Result<Vec<_>, _>>().unwrap()
        );
        assert_eq!(
            mapped
                .weak_pin_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            oracle
                .weak_pin_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        );
        assert_eq!(
            mapped.pin_head(target_branch).unwrap(),
            oracle.pin_head(target_branch).unwrap()
        );
        let locator = mapped.blob_locator(target).unwrap().unwrap();
        assert_eq!(Some(locator), oracle.blob_locator(target).unwrap());
        assert_eq!(locator.payload_offset, target_payload_offset);
    }

    #[test]
    fn tombstones_and_weak_unpins_collapse() {
        let (_dir, pile_path, index_path) = new_paths();
        let (blob, branch) = build_fixture(&pile_path);
        let mut pile = Pile::open(&pile_path).unwrap();
        let head: Inline<Handle<SimpleArchive>> = blob.transmute();
        assert!(matches!(
            pile.update(branch, Some(head), None).unwrap(),
            super::super::PushResult::Success()
        ));
        pile.unpin_weak(blob).unwrap();
        pile.close().unwrap();

        let stats = MappedPileIndex::build(&pile_path, &index_path).unwrap();
        assert_eq!(stats.pins, 0);
        assert_eq!(stats.weak_pins, 0);
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        assert_eq!(mapped.pin_head(branch).unwrap(), None);
        assert_eq!(mapped.pin_ids().count(), 0);
        assert_eq!(mapped.weak_pin_handles().count(), 0);
    }

    #[test]
    fn valid_duplicate_reuses_first_locator_and_corrupt_first_uses_second() {
        let (_dir, pile_path, index_path) = new_paths();
        let (blob, _branch) = build_fixture(&pile_path);
        let original = fs::read(&pile_path).unwrap();
        let original_len = original.len();
        OpenOptions::new()
            .append(true)
            .open(&pile_path)
            .unwrap()
            .write_all(&original)
            .unwrap();

        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        let first = mapped.blob_locator(blob).unwrap().unwrap();
        assert!(first.payload_offset < original_len as u64);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pile_path)
            .unwrap();
        file.seek(SeekFrom::Start(first.payload_offset)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();

        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        let replacement = mapped.blob_locator(blob).unwrap().unwrap();
        assert!(replacement.payload_offset >= original_len as u64);

        let mut pile = Pile::open(&pile_path).unwrap();
        let oracle = pile.patch_index().unwrap();
        assert_eq!(Some(replacement), oracle.blob_locator(blob).unwrap());
        pile.close().unwrap();
    }

    #[test]
    fn mixed_v1_v3_duplicates_match_patch_even_when_all_copies_are_invalid() {
        let (_dir, pile_path, index_path) = new_paths();
        let payload = b"legacy payload";
        let (expected, mut legacy) = legacy_v1_blob_record_for_test(payload, 11);
        let legacy_payload_offset = match decode_record(&legacy, 0).unwrap().content {
            PileRecordContent::Blob { data_offset, .. } => data_offset,
            _ => unreachable!(),
        };
        legacy[legacy_payload_offset] ^= 0xFF;
        let legacy_len = legacy.len();
        fs::write(&pile_path, legacy).unwrap();

        let mut pile = Pile::open(&pile_path).unwrap();
        let replacement: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                payload.to_vec(),
            )))
            .unwrap();
        assert_eq!(replacement, expected);
        pile.close().unwrap();

        let bytes = fs::read(&pile_path).unwrap();
        let replacement_payload_offset = match decode_record(&bytes[legacy_len..], legacy_len)
            .unwrap()
            .content
        {
            PileRecordContent::Blob { data_offset, .. } => data_offset,
            _ => unreachable!(),
        };
        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        let locator = mapped.blob_locator(expected).unwrap().unwrap();
        assert_eq!(locator.payload_offset, replacement_payload_offset as u64);
        drop(mapped);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pile_path)
            .unwrap();
        file.seek(SeekFrom::Start(replacement_payload_offset as u64))
            .unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();

        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        let locator = mapped.blob_locator(expected).unwrap().unwrap();
        assert_eq!(locator.payload_offset, replacement_payload_offset as u64);

        let mut pile = Pile::open(&pile_path).unwrap();
        let oracle = pile.patch_index().unwrap();
        assert_eq!(Some(locator), oracle.blob_locator(expected).unwrap());
        pile.close().unwrap();
    }

    #[test]
    fn payload_locator_ignores_v3_shaped_bytes_before_a_v1_record() {
        const V3_BLOB_MAGIC: [u8; 16] = hex_literal::hex!("9C33EEB525065A62EAEC4BE43DCC355A");

        let (_dir, pile_path, index_path) = new_paths();
        let target_payload = b"ambiguous-target";
        let target_timestamp = 77;
        let (target, target_record) =
            legacy_v1_blob_record_for_test(target_payload, target_timestamp);

        // The carrier is 320 bytes. A fake V3 header 128 bytes into that
        // record points at the following V1 record's payload (320 + 64), so
        // subtracting a guessed 256-byte header from the payload is ambiguous.
        let mut carrier_payload = vec![0u8; 256];
        let fake_header = 64;
        carrier_payload[fake_header..fake_header + 16].copy_from_slice(&V3_BLOB_MAGIC);
        carrier_payload[fake_header + 16..fake_header + 24]
            .copy_from_slice(&target_timestamp.to_le_bytes());
        carrier_payload[fake_header + 24..fake_header + 32]
            .copy_from_slice(&(target_payload.len() as u64).to_le_bytes());
        carrier_payload[fake_header + 32..fake_header + 64].copy_from_slice(&target.raw);
        let (_, carrier_record) = legacy_v1_blob_record_for_test(&carrier_payload, 1);
        let (_, trailer_record) = legacy_v1_blob_record_for_test(&[0u8; 128], 2);
        assert_eq!(carrier_record.len(), 320);

        let mut pile_bytes = carrier_record;
        let target_record_offset = pile_bytes.len();
        pile_bytes.extend_from_slice(&target_record);
        pile_bytes.extend_from_slice(&trailer_record);
        fs::write(&pile_path, &pile_bytes).unwrap();

        let misleading_record_offset = target_record_offset - 192;
        let decoy = decode_record(
            &pile_bytes[misleading_record_offset..],
            misleading_record_offset,
        )
        .unwrap();
        let expected_payload_offset = (target_record_offset + 64) as u64;
        assert!(matches!(
            decoy.content,
            PileRecordContent::Blob {
                timestamp,
                hash,
                data_offset,
                data_len,
            } if timestamp == target_timestamp
                && hash.raw == target.raw
                && data_offset as u64 == expected_payload_offset
                && data_len == target_payload.len()
        ));

        let mut pile = Pile::open(&pile_path).unwrap();
        let oracle = pile.patch_index().unwrap();
        pile.close().unwrap();
        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        let locator = mapped.blob_locator(target).unwrap().unwrap();
        assert_eq!(Some(locator), oracle.blob_locator(target).unwrap());
        assert_eq!(locator.payload_offset, expected_payload_offset);
    }

    #[test]
    fn stale_malformed_and_wrong_identity_snapshots_fall_back() {
        let (_dir, pile_path, index_path) = new_paths();
        build_fixture(&pile_path);
        MappedPileIndex::build(&pile_path, &index_path).unwrap();

        let mut pile = Pile::open(&pile_path).unwrap();
        let _: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                b"tail".to_vec(),
            )))
            .unwrap();
        pile.close().unwrap();
        assert!(matches!(
            MappedPileIndex::open(&pile_path, &index_path),
            Err(PileIndexError::StaleIndex(_))
        ));
        assert!(MappedPileIndex::open_or_none(&pile_path, &index_path)
            .unwrap()
            .is_none());

        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let mut index = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&index_path)
            .unwrap();
        index
            .seek(SeekFrom::Start(INDEX_HEADER_LEN as u64))
            .unwrap();
        index.write_all(&[0xFF]).unwrap();
        index.sync_all().unwrap();
        assert!(matches!(
            MappedPileIndex::open(&pile_path, &index_path),
            Err(PileIndexError::InvalidIndex(_))
        ));
        assert!(MappedPileIndex::open_or_none(&pile_path, &index_path)
            .unwrap()
            .is_none());

        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let other = pile_path.with_file_name("other.pile");
        fs::copy(&pile_path, &other).unwrap();
        assert!(matches!(
            MappedPileIndex::open(&other, &index_path),
            Err(PileIndexError::StaleIndex(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn builder_rejects_pile_aliases_without_replacing_any_path() {
        use std::os::unix::fs::symlink;

        let (_dir, pile_path, index_path) = new_paths();
        build_fixture(&pile_path);
        let original = fs::read(&pile_path).unwrap();

        assert!(matches!(
            MappedPileIndex::build(&pile_path, &pile_path),
            Err(PileIndexError::IndexAliasesPile)
        ));
        assert_eq!(fs::read(&pile_path).unwrap(), original);

        let hardlink = index_path.with_extension("hardlink.pidx");
        fs::hard_link(&pile_path, &hardlink).unwrap();
        assert!(matches!(
            MappedPileIndex::build(&pile_path, &hardlink),
            Err(PileIndexError::IndexAliasesPile)
        ));
        assert_eq!(fs::read(&pile_path).unwrap(), original);
        assert_eq!(fs::read(&hardlink).unwrap(), original);

        let symlink_path = index_path.with_extension("symlink.pidx");
        symlink(&pile_path, &symlink_path).unwrap();
        assert!(matches!(
            MappedPileIndex::build(&pile_path, &symlink_path),
            Err(PileIndexError::IndexAliasesPile)
        ));
        assert_eq!(fs::read(&pile_path).unwrap(), original);
        assert!(fs::symlink_metadata(&symlink_path)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn temp_collision_is_not_registered_for_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let collision = dir.path().join("already-there.tmp");
        fs::write(&collision, b"belongs to someone else").unwrap();
        let mut artifacts = TempArtifacts::default();

        let err = create_owned_temp_file(&collision, &mut artifacts.paths).unwrap_err();
        assert!(matches!(
            err,
            PileIndexError::Io(ref source)
                if source.kind() == io::ErrorKind::AlreadyExists
        ));
        assert!(artifacts.paths.is_empty());
        drop(artifacts);
        assert_eq!(fs::read(&collision).unwrap(), b"belongs to someone else");
    }

    #[test]
    fn truncation_and_anchor_change_are_rejected() {
        let (_dir, pile_path, index_path) = new_paths();
        build_fixture(&pile_path);
        MappedPileIndex::build(&pile_path, &index_path).unwrap();
        let original = fs::read(&pile_path).unwrap();

        OpenOptions::new()
            .write(true)
            .open(&pile_path)
            .unwrap()
            .set_len((original.len() - 1) as u64)
            .unwrap();
        assert!(matches!(
            MappedPileIndex::open(&pile_path, &index_path),
            Err(PileIndexError::StaleIndex(_))
        ));

        fs::write(&pile_path, &original).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pile_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.sync_all().unwrap();
        assert!(matches!(
            MappedPileIndex::open(&pile_path, &index_path),
            Err(PileIndexError::StaleIndex(_))
        ));
    }

    #[test]
    fn empty_pile_round_trips() {
        let (_dir, pile_path, index_path) = new_paths();
        let stats = MappedPileIndex::build(&pile_path, &index_path).unwrap();
        assert_eq!(stats.pile_records, 0);
        let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
        assert_eq!(mapped.blob_handles().count(), 0);
        assert_eq!(mapped.pin_ids().count(), 0);
    }
}
