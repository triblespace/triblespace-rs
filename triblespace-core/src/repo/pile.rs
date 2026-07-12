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
//! `get` or `apply_next`, those bytes are
//! assumed permanent. Modifying any part of the pile other than appending new
//! records is undefined behaviour. The un-applied tail may hide a partial
//! append after a crash, so validation and repair only operate on offsets
//! beyond `applied_length`. Each record's [`ValidationState`](crate::repo::pile::ValidationState) is cached for the
//! lifetime of the process under this immutability assumption.
//!
//! For layout and recovery details see the [Pile
//! Format](../../book/src/pile-format.md) chapter of the Tribles Book.

use anybytes::Bytes;
use hex_literal::hex;
use memmap2::MmapOptions;
use memmap2::MmapRaw;
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::IoSlice;
use std::io::Write;
use std::path::Path;
use std::ptr::slice_from_raw_parts;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;
use zerocopy::TryFromBytes;

use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::id::Id;
use crate::id::RawId;
use crate::inline::encodings::hash::Blake3;
use crate::inline::encodings::hash::Hash;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::patch::Entry;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::prelude::blobencodings::SimpleArchive;
use crate::prelude::inlineencodings::Handle;

use super::pile_index::{
    MappedPileIndex, OwnedMappedBlobHandles, PileBlobLocator, PileIndex, PileIndexError,
};

const MAGIC_MARKER_BLOB: RawId = hex!("1E08B022FF2F47B6EBACF1D68EB35D96");
const MAGIC_MARKER_BRANCH: RawId = hex!("2BC991A7F5D5D2A3A468C53B0AA03504");
const MAGIC_MARKER_BRANCH_TOMBSTONE: RawId = hex!("E888CC787202D2AE4C654BFE9699C430");
/// V3 record markers — the uniform 256-byte-header format (minted 2026-06-29 via
/// `trible genid`). Every V3 record (blob/branch/tombstone) has a FIXED 256-byte
/// header and is padded to a 256-byte multiple. Consequences:
///   * blob data starts at a constant `record_start + V3_HEADER_LEN` — reads are
///     position-INDEPENDENT (no offset-derived pad), so a record survives
///     relocation/`cat` and is found correctly regardless of its offset;
///   * because every record is a 256-multiple, a pure-V3 pile stays 256-aligned
///     throughout under ATOMIC lock-free append (no exclusive lock needed), so
///     `cat a >> b` of two pure-V3 piles is a valid merge AND the data stays
///     256-aligned for zero-copy GPU aliasing (CUDA/Metal `min_storage_buffer_offset_alignment`).
/// New writes are V3; the reader still accepts the original V1
/// [`MAGIC_MARKER_BLOB`] record so existing piles read byte-identical.
const MAGIC_MARKER_BLOB_V3: RawId = hex!("9C33EEB525065A62EAEC4BE43DCC355A");
const MAGIC_MARKER_BRANCH_V3: RawId = hex!("AC363D04AFE1AF17B39581B1E23021D7");
const MAGIC_MARKER_BRANCH_TOMBSTONE_V3: RawId = hex!("D0CBA0C8EAAB4C0C73121C3205671E4F");
/// Weak-pin marker pair (minted 2026-07-01 via `trible genid`). Retention is
/// one strength axis resolved last-writer-wins by log position:
/// `pin ⊐ weak-pin ⊐ weak-unpin ⊐ unpin` — the branch record IS `pin`, the
/// branch tombstone IS `unpin`; these two are the soft siblings. A weak pin is
/// per-blob and anonymous (keyed by blob handle, no branch id): "I want this
/// blob; fetch it if absent; evictable under pressure." It is simultaneously
/// the demand/want-signal (a sync daemon's work queue), the cache-retention
/// marker, and the eviction target. `weak-unpin` retracts it. Because the
/// records are durable pile records, reopening a pile reloads the weak set.
const MAGIC_MARKER_WEAK_PIN_V3: RawId = hex!("8F3EEFEDECD491F63F6EAAA5FD6F3D5E");
const MAGIC_MARKER_WEAK_UNPIN_V3: RawId = hex!("2D76662DFF0187EC36A8C90B12BB8B0D");

const BLOB_HEADER_LEN: usize = std::mem::size_of::<BlobHeader>();
const BLOB_ALIGNMENT: usize = BLOB_HEADER_LEN;
/// GPU storage-buffer binding-offset requirement (CUDA / Metal
/// `min_storage_buffer_offset_alignment`); a V3 record's data start lands on this.
const GPU_DATA_ALIGNMENT: usize = 256;
/// V3 fixed header length and record alignment (== GPU_DATA_ALIGNMENT). A V3
/// header is always this many bytes; data follows at `record_start + V3_HEADER_LEN`,
/// and the whole record is padded to a multiple of this.
const V3_HEADER_LEN: usize = 256;
const V3_ALIGNMENT: usize = GPU_DATA_ALIGNMENT;
/// Post-data padding that rounds a V3 record up to a 256-byte multiple. The
/// header is already 256, so this only rounds the data length.
fn v3_post_pad(data_len: usize) -> usize {
    (V3_ALIGNMENT - (data_len % V3_ALIGNMENT)) % V3_ALIGNMENT
}

/// Largest single blob record we'll write with the concurrent `write_vectored`
/// fast path. Linux caps a single `writev` at `MAX_RW_COUNT` (`INT_MAX &
/// ~(PAGE_SIZE - 1)`, ~2 GiB) and macOS caps it at `INT_MAX`. Below this
/// threshold we rely on kernel atomicity and let concurrent writers hold a
/// shared lock. Above it we switch to an exclusive-lock fallback that
/// issues plain `write_all` calls — still append-only, still recoverable
/// via [`Pile::amputate`], just serialized with other writers for the
/// duration of the large append. The margin keeps us comfortably below
/// any platform's single-call ceiling.
const ATOMIC_WRITE_LIMIT: usize = 1 << 30;

/// Lazily-computed validation status of a blob record in the pile.
#[derive(Debug, Clone, Copy)]
pub enum ValidationState {
    /// The blob's hash matches its stored digest.
    Validated,
    /// The blob's hash does not match — the record is corrupt.
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
    hash: RawInline,
}

// `BranchHeader` / `BranchTombstoneHeader` have no constructors — new writes are
// V3; these structs exist only so the reader can decode legacy V1 records.

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BranchTombstoneHeader {
    magic_marker: RawId,
    branch_id: RawId,
    /// Reserved bytes to preserve 64 byte record alignment.
    reserved: RawInline,
}

#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BlobHeader {
    magic_marker: RawId,
    timestamp: u64,
    length: u64,
    hash: RawInline,
}

impl BlobHeader {
    /// V1 blob constructor — retained only for the legacy-format backward-compat
    /// test (new writes are V3; V1 blob records are otherwise read, never written).
    #[cfg(test)]
    fn new(timestamp: u64, length: u64, hash: Inline<Hash<Blake3>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BLOB,
            timestamp,
            length,
            hash: hash.raw,
        }
    }
}

/// V3 blob header — fixed 256 bytes. Same load-bearing fields as V1; the data
/// follows at `record_start + V3_HEADER_LEN` with no offset-derived pre-pad.
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BlobHeaderV3 {
    magic_marker: RawId,
    timestamp: u64,
    length: u64,
    hash: RawInline,
    /// Pads the header to V3_HEADER_LEN (256), zeroed. NOT part of the content
    /// hash, so it never affects blob identity or dedup. Deliberately empty:
    /// genuinely useful per-record metadata belongs in tribles (keyed by the
    /// referencing attribute), and the encoding/schema must NOT live here — else
    /// identical bytes would fork into distinct blobs. Fill only when a concrete,
    /// content-independent need names itself.
    reserved: [u8; 192],
}

impl BlobHeaderV3 {
    fn new(timestamp: u64, length: u64, hash: Inline<Hash<Blake3>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BLOB_V3,
            timestamp,
            length,
            hash: hash.raw,
            reserved: [0u8; 192],
        }
    }
}

/// V3 branch head — fixed 256 bytes (mirrors `BranchHeader` + reserved pad).
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BranchHeaderV3 {
    magic_marker: RawId,
    branch_id: RawId,
    hash: RawInline,
    reserved: [u8; 192],
}

impl BranchHeaderV3 {
    fn new(branch_id: Id, hash: Inline<Handle<SimpleArchive>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BRANCH_V3,
            branch_id: *branch_id,
            hash: hash.raw,
            reserved: [0u8; 192],
        }
    }
}

/// V3 branch tombstone — fixed 256 bytes.
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct BranchTombstoneHeaderV3 {
    magic_marker: RawId,
    branch_id: RawId,
    reserved: [u8; 224],
}

impl BranchTombstoneHeaderV3 {
    fn new(branch_id: Id) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_BRANCH_TOMBSTONE_V3,
            branch_id: *branch_id,
            reserved: [0u8; 224],
        }
    }
}

/// V3 weak-pin marker — fixed 256 bytes. Keyed by blob handle (no branch id);
/// see the docs on [`MAGIC_MARKER_WEAK_PIN_V3`] for the retention lattice.
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct WeakPinHeaderV3 {
    magic_marker: RawId,
    handle: RawInline,
    reserved: [u8; 208],
}

impl WeakPinHeaderV3 {
    fn new(handle: Inline<Handle<UnknownBlob>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_WEAK_PIN_V3,
            handle: handle.raw,
            reserved: [0u8; 208],
        }
    }
}

/// V3 weak-unpin marker — fixed 256 bytes. Retracts a prior weak pin on the
/// same handle (last-writer-wins by log position).
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone)]
#[repr(C)]
struct WeakUnpinHeaderV3 {
    magic_marker: RawId,
    handle: RawInline,
    reserved: [u8; 208],
}

impl WeakUnpinHeaderV3 {
    fn new(handle: Inline<Handle<UnknownBlob>>) -> Self {
        Self {
            magic_marker: MAGIC_MARKER_WEAK_UNPIN_V3,
            handle: handle.raw,
            reserved: [0u8; 208],
        }
    }
}

// Compile-time guarantee that every V3 header is exactly 256 bytes.
const _: () = {
    assert!(std::mem::size_of::<BlobHeaderV3>() == V3_HEADER_LEN);
    assert!(std::mem::size_of::<BranchHeaderV3>() == V3_HEADER_LEN);
    assert!(std::mem::size_of::<BranchTombstoneHeaderV3>() == V3_HEADER_LEN);
    assert!(std::mem::size_of::<WeakPinHeaderV3>() == V3_HEADER_LEN);
    assert!(std::mem::size_of::<WeakUnpinHeaderV3>() == V3_HEADER_LEN);
};

/// A single record decoded from a pile file.
///
/// Yielded by [`PileRecords`], the raw record-level view of a pile. The
/// record's header starts at `offset` and the whole record (header + payload +
/// padding) spans `len` bytes, so `offset + len` is the offset of the next
/// record. This is the same decoder the [`Pile`] itself replays on open, so it
/// understands every record format ever written (V1 64-byte records and V3
/// uniform 256-aligned records alike).
#[derive(Debug, Clone, Copy)]
pub struct PileRecord {
    /// Byte offset of the record header within the pile file.
    pub offset: usize,
    /// Total on-disk length of the record (header + payload + padding).
    pub len: usize,
    /// The decoded record content.
    pub content: PileRecordContent,
}

/// Decoded content of a [`PileRecord`], independent of on-disk format version.
#[derive(Debug, Clone, Copy)]
pub enum PileRecordContent {
    /// A blob record. The payload bytes live at
    /// `data_offset..data_offset + data_len` in the pile file; trailing
    /// alignment padding after the payload is not content and is not covered
    /// by `hash`.
    Blob {
        /// Insertion timestamp in milliseconds since the Unix epoch.
        timestamp: u64,
        /// Blake3 digest of the payload as recorded in the header.
        hash: Inline<Hash<Blake3>>,
        /// Byte offset of the payload within the pile file.
        data_offset: usize,
        /// Payload length in bytes (excluding padding).
        data_len: usize,
    },
    /// A branch head update.
    Branch {
        /// The branch being updated.
        branch_id: Id,
        /// The new head (a branch-metadata blob handle).
        head: Inline<Handle<SimpleArchive>>,
    },
    /// A branch tombstone (deletion marker).
    BranchTombstone {
        /// The branch being tombstoned.
        branch_id: Id,
    },
    /// A weak-pin marker for a blob handle.
    WeakPin {
        /// The pinned blob handle.
        handle: Inline<Handle<UnknownBlob>>,
    },
    /// A weak-unpin marker retracting a prior weak pin.
    WeakUnpin {
        /// The unpinned blob handle.
        handle: Inline<Handle<UnknownBlob>>,
    },
}

/// Decodes the record starting at the beginning of `bytes`, which is the pile
/// file's content from `offset` onward. This is the single source of truth for
/// record parsing: [`Pile::refresh`]/[`Pile::amputate`] replay records through
/// it, and [`PileRecords`] exposes it for raw inspection. An unknown magic
/// marker or a truncated record yields [`ReadError::CorruptPile`] pointing at
/// `offset` — never a silent stop.
pub(super) fn decode_record(bytes: &[u8], offset: usize) -> Result<PileRecord, ReadError> {
    let corrupt = || ReadError::CorruptPile {
        valid_length: offset,
    };
    if bytes.len() < 16 {
        return Err(corrupt());
    }
    let magic: RawId = bytes[0..16].try_into().unwrap();
    match magic {
        MAGIC_MARKER_BLOB => {
            let (header, _) = BlobHeader::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let data_len = header.length as usize;
            let pad = padding_for_blob(data_len);
            let len = BLOB_HEADER_LEN
                .checked_add(data_len)
                .and_then(|l| l.checked_add(pad))
                .ok_or_else(corrupt)?;
            if bytes.len() < len {
                return Err(corrupt());
            }
            Ok(PileRecord {
                offset,
                len,
                content: PileRecordContent::Blob {
                    timestamp: header.timestamp,
                    hash: Inline::new(header.hash),
                    data_offset: offset + BLOB_HEADER_LEN,
                    data_len,
                },
            })
        }
        MAGIC_MARKER_BRANCH => {
            let (header, _) = BranchHeader::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let branch_id = Id::new(header.branch_id).ok_or_else(corrupt)?;
            Ok(PileRecord {
                offset,
                len: std::mem::size_of::<BranchHeader>(),
                content: PileRecordContent::Branch {
                    branch_id,
                    head: Inline::<Hash<Blake3>>::new(header.hash).into(),
                },
            })
        }
        MAGIC_MARKER_BRANCH_TOMBSTONE => {
            let (header, _) =
                BranchTombstoneHeader::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let branch_id = Id::new(header.branch_id).ok_or_else(corrupt)?;
            Ok(PileRecord {
                offset,
                len: std::mem::size_of::<BranchTombstoneHeader>(),
                content: PileRecordContent::BranchTombstone { branch_id },
            })
        }
        MAGIC_MARKER_BLOB_V3 => {
            // Fixed 256-byte header; data at a constant `record_start +
            // V3_HEADER_LEN` (no offset-derived pad — position-independent),
            // record padded to a 256-byte multiple.
            let (header, _) = BlobHeaderV3::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let data_len = header.length as usize;
            let post_pad = v3_post_pad(data_len);
            let len = V3_HEADER_LEN
                .checked_add(data_len)
                .and_then(|l| l.checked_add(post_pad))
                .ok_or_else(corrupt)?;
            if bytes.len() < len {
                return Err(corrupt());
            }
            Ok(PileRecord {
                offset,
                len,
                content: PileRecordContent::Blob {
                    timestamp: header.timestamp,
                    hash: Inline::new(header.hash),
                    data_offset: offset + V3_HEADER_LEN,
                    data_len,
                },
            })
        }
        MAGIC_MARKER_BRANCH_V3 => {
            let (header, _) = BranchHeaderV3::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let branch_id = Id::new(header.branch_id).ok_or_else(corrupt)?;
            Ok(PileRecord {
                offset,
                len: V3_HEADER_LEN,
                content: PileRecordContent::Branch {
                    branch_id,
                    head: Inline::<Hash<Blake3>>::new(header.hash).into(),
                },
            })
        }
        MAGIC_MARKER_BRANCH_TOMBSTONE_V3 => {
            let (header, _) =
                BranchTombstoneHeaderV3::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            let branch_id = Id::new(header.branch_id).ok_or_else(corrupt)?;
            Ok(PileRecord {
                offset,
                len: V3_HEADER_LEN,
                content: PileRecordContent::BranchTombstone { branch_id },
            })
        }
        MAGIC_MARKER_WEAK_PIN_V3 => {
            let (header, _) =
                WeakPinHeaderV3::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            Ok(PileRecord {
                offset,
                len: V3_HEADER_LEN,
                content: PileRecordContent::WeakPin {
                    handle: Inline::new(header.handle),
                },
            })
        }
        MAGIC_MARKER_WEAK_UNPIN_V3 => {
            let (header, _) =
                WeakUnpinHeaderV3::try_read_from_prefix(bytes).map_err(|_| corrupt())?;
            Ok(PileRecord {
                offset,
                len: V3_HEADER_LEN,
                content: PileRecordContent::WeakUnpin {
                    handle: Inline::new(header.handle),
                },
            })
        }
        _ => Err(corrupt()),
    }
}

/// Iterator over the raw records of a pile file, in log order.
///
/// This is the record-level view of the append-only log: every blob, branch
/// update, branch tombstone, and weak-pin marker ever appended, including
/// records that later ones supersede (superseded branch heads, tombstoned
/// branches, retracted weak pins). It shares its decoder with the [`Pile`]
/// replay path, so both V1 and V3 records are understood; tools that need
/// history or forensics (reflogs, consolidation, corruption reports) should
/// consume this instead of hand-rolling a parser.
///
/// The iterator yields `Err(`[`ReadError::CorruptPile`]`)` and then ends when
/// it encounters an unknown magic marker or a truncated record — a partial
/// tail after a crash surfaces as an error, never as a silently shortened
/// record list.
#[derive(Debug)]
pub struct PileRecords {
    /// Keep the exact file descriptor used to create `bytes` so derived
    /// snapshots can bind their identity without a path-reopen TOCTOU.
    file: File,
    bytes: Bytes,
    offset: usize,
    failed: bool,
}

impl PileRecords {
    /// Opens the pile file at `path` read-only and returns an iterator over
    /// its records. No index is built and nothing is validated eagerly; blob
    /// payloads are not hashed.
    pub fn open(path: &Path) -> Result<Self, ReadError> {
        let file = File::open(path)?;
        let length = file.metadata()?.len();
        let bytes = if length == 0 {
            // Mapping a zero-length file is an error on most platforms; an
            // empty pile simply has no records.
            Bytes::empty()
        } else {
            // SAFETY: the pile file is append-only by contract; existing
            // bytes are never mutated, so the mapping stays valid.
            unsafe { Bytes::map_file(&file)? }
        };
        Ok(Self {
            file,
            bytes,
            offset: 0,
            failed: false,
        })
    }

    /// The raw bytes of the pile file, e.g. to inspect a blob payload at the
    /// `data_offset`/`data_len` reported by [`PileRecordContent::Blob`].
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Metadata from the same file descriptor that backs [`Self::bytes`].
    pub(crate) fn file_metadata(&self) -> Result<std::fs::Metadata, ReadError> {
        Ok(self.file.metadata()?)
    }
}

impl Iterator for PileRecords {
    type Item = Result<PileRecord, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.offset >= self.bytes.len() {
            return None;
        }
        match decode_record(&self.bytes[self.offset..], self.offset) {
            Ok(record) => {
                self.offset += record.len;
                Some(Ok(record))
            }
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

#[derive(Debug)]
enum Applied {
    Blob { hash: Inline<Hash<Blake3>> },
    Branch { id: Id, hash: Inline<Hash<Blake3>> },
    BranchTombstone { id: Id },
    WeakPin { handle: Inline<Handle<UnknownBlob>> },
    WeakUnpin { handle: Inline<Handle<UnknownBlob>> },
}

#[derive(Debug)]
/// A grow-only collection of blobs and pin heads backed by a single file on disk.
///
/// Branch updates do not verify that referenced blobs exist in the pile, allowing the
/// pile to operate as a head-only store when blob data lives elsewhere.
///
/// [`Pile::refresh`] aborts immediately if the underlying file shrinks below
/// data that has already been applied, preventing undefined behavior from
/// dangling [`Bytes`] handles.
pub struct Pile {
    file: File,
    mmap: Arc<MmapRaw>,
    /// Accepted immutable prefix. `None` is the unchanged full-PATCH path;
    /// when present, the PATCH fields below contain only the canonical tail.
    base: Option<IndexedPileBase>,
    blobs: PATCH<32, IdentitySchema, IndexEntry>,
    /// Tail blob keys absent from `base`. Corrupt-base replacements live in
    /// `blobs` but not here because they do not change the logical key set.
    blob_additions: Option<PATCH<32, IdentitySchema>>,
    branches: PATCH<16, IdentitySchema, Inline<Handle<SimpleArchive>>>,
    /// Negative tail cells are retained separately so an absent positive
    /// overlay never resurrects a mapped base pin.
    branch_tombstones: Option<PATCH<16, IdentitySchema>>,
    /// LWW-resolved weak-pin set: weak-pin records insert the handle,
    /// weak-unpin records remove it; log-order application makes the last
    /// record for a handle win by construction.
    weak_pins: PATCH<32, IdentitySchema>,
    /// Negative weak-pin tail cells, analogous to `branch_tombstones`.
    weak_unpins: Option<PATCH<32, IdentitySchema>>,
    /// Length of the file that has been validated and applied.
    ///
    /// Offsets below this value are guaranteed valid; corruption detection
    /// only operates on the un-applied tail beyond this boundary.
    applied_length: usize,
}

fn padding_for_blob(blob_size: usize) -> usize {
    (BLOB_ALIGNMENT - ((BLOB_HEADER_LEN + blob_size) % BLOB_ALIGNMENT)) % BLOB_ALIGNMENT
}

/// Canonical legacy V1 blob bytes for mixed-format regression tests.
#[cfg(test)]
pub(crate) fn legacy_v1_blob_record_for_test(
    payload: &[u8],
    timestamp: u64,
) -> (Inline<Handle<UnknownBlob>>, Vec<u8>) {
    let blob = Blob::<UnknownBlob>::new(Bytes::from_source(payload.to_vec()));
    let handle = blob.get_handle();
    let hash: Inline<Hash<Blake3>> = handle.into();
    let header = BlobHeader::new(timestamp, payload.len() as u64, hash);
    let record_len = BLOB_HEADER_LEN + payload.len() + padding_for_blob(payload.len());
    let mut record = Vec::with_capacity(record_len);
    record.extend_from_slice(header.as_bytes());
    record.extend_from_slice(payload);
    record.resize(record_len, 0);
    (handle, record)
}

#[derive(Debug, Clone)]
/// Read-only handle referencing a [`Pile`].
///
/// Multiple `PileReader` instances can coexist and provide concurrent access to
/// the same underlying pile data.
pub struct PileReader {
    mmap: Arc<MmapRaw>,
    base: Option<IndexedPileBase>,
    blobs: PATCH<32, IdentitySchema, IndexEntry>,
    blob_additions: Option<PATCH<32, IdentitySchema>>,
}

/// Shared mapped prefix plus a sparse validation cache for base blobs that
/// are actually read or challenged by a duplicate in the tail. The map only
/// stores touched keys; each cell lets concurrent readers hash outside the
/// short map lock and converge on one result.
#[derive(Debug, Clone)]
struct IndexedPileBase {
    index: Arc<MappedPileIndex>,
    validations: Arc<Mutex<HashMap<[u8; 32], Arc<OnceLock<ValidationState>>>>>,
}

impl IndexedPileBase {
    fn new(index: MappedPileIndex) -> Self {
        Self {
            index: Arc::new(index),
            validations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn validation_cell(&self, key: [u8; 32]) -> Arc<OnceLock<ValidationState>> {
        let mut cache = self.validations.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceLock::new()))
            .clone()
    }
}

fn read_base_blob(
    base: &IndexedPileBase,
    mmap: &Arc<MmapRaw>,
    handle: Inline<Handle<UnknownBlob>>,
) -> Result<Option<(Bytes, PileBlobLocator, ValidationState)>, PileIndexError> {
    let Some(locator) = base.index.blob_locator(handle)? else {
        return Ok(None);
    };
    let offset =
        usize::try_from(locator.payload_offset).map_err(|_| PileIndexError::StaleLocator {
            offset: locator.payload_offset,
        })?;
    let len = usize::try_from(locator.payload_len).map_err(|_| PileIndexError::StaleLocator {
        offset: locator.payload_offset,
    })?;
    let end = offset
        .checked_add(len)
        .ok_or(PileIndexError::StaleLocator {
            offset: locator.payload_offset,
        })?;
    if end > mmap.len() {
        return Err(PileIndexError::StaleLocator {
            offset: locator.payload_offset,
        });
    }
    let bytes = unsafe {
        let slice = slice_from_raw_parts(mmap.as_ptr().add(offset), len)
            .as_ref()
            .unwrap();
        Bytes::from_raw_parts(slice, mmap.clone())
    };
    let cell = base.validation_cell(handle.raw);
    let state = *cell.get_or_init(|| {
        let expected: Inline<Hash<Blake3>> = handle.into();
        if Hash::<Blake3>::digest(&bytes) == expected {
            ValidationState::Validated
        } else {
            ValidationState::Invalid
        }
    });
    Ok(Some((bytes, locator, state)))
}

fn index_error_as_read(err: PileIndexError) -> ReadError {
    ReadError::IndexError(Box::new(err))
}

/// Snapshot of the current heap PATCH indices, exposed only as the oracle for
/// the opt-in mapped-index proof. Normal [`Pile`] and [`PileReader`] behavior
/// remains unchanged.
#[derive(Debug, Clone)]
pub struct PatchPileIndex {
    blobs: PATCH<32, IdentitySchema, IndexEntry>,
    branches: PATCH<16, IdentitySchema, Inline<Handle<SimpleArchive>>>,
    weak_pins: PATCH<32, IdentitySchema>,
    covered_len: usize,
}

/// Streaming ordered keys from the PATCH blob-index oracle.
pub struct PatchBlobHandles {
    inner: crate::patch::PATCHIntoOrderedIterator<32, IdentitySchema, IndexEntry>,
}

impl Iterator for PatchBlobHandles {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|raw| Ok(Inline::new(raw)))
    }
}

/// Streaming ordered pin ids from the PATCH oracle.
pub struct PatchPinIds {
    inner:
        crate::patch::PATCHIntoOrderedIterator<16, IdentitySchema, Inline<Handle<SimpleArchive>>>,
}

impl Iterator for PatchPinIds {
    type Item = Result<Id, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|raw| {
            Id::new(raw)
                .ok_or_else(|| PileIndexError::InvalidIndex("PATCH contains a nil pin id".into()))
        })
    }
}

/// Streaming ordered weak-pin handles from the PATCH oracle.
pub struct PatchWeakPinHandles {
    inner: crate::patch::PATCHIntoOrderedIterator<32, IdentitySchema, ()>,
}

impl Iterator for PatchWeakPinHandles {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileIndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|raw| Ok(Inline::new(raw)))
    }
}

impl PartialEq for PileReader {
    fn eq(&self, other: &Self) -> bool {
        match (&self.base, &other.base) {
            (None, None) => self.blobs == other.blobs,
            (Some(left), Some(right)) if Arc::ptr_eq(&left.index, &right.index) => {
                self.blob_additions == other.blob_additions
            }
            _ => match (self.logical_blob_keys(), other.logical_blob_keys()) {
                (Ok(left), Ok(right)) => left == right,
                _ => false,
            },
        }
    }
}

impl Eq for PileReader {}

impl PileReader {
    fn new(
        mmap: Arc<MmapRaw>,
        base: Option<IndexedPileBase>,
        blobs: PATCH<32, IdentitySchema, IndexEntry>,
        blob_additions: Option<PATCH<32, IdentitySchema>>,
    ) -> Self {
        Self {
            mmap,
            base,
            blobs,
            blob_additions,
        }
    }

    /// Returns an iterator over all blobs currently stored in the pile.
    ///
    /// This creates an owned snapshot of the current keys/indices so the
    /// returned iterator does not borrow from the underlying PATCH.
    pub fn iter(&self) -> PileBlobStoreIter {
        PileBlobStoreIter {
            reader: self.clone(),
            handles: self.blobs(),
        }
    }

    fn base_blob(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<(Bytes, PileBlobLocator, ValidationState)>, PileIndexError> {
        let Some(base) = &self.base else {
            return Ok(None);
        };
        read_base_blob(base, &self.mmap, handle)
    }

    fn logical_blob_keys(&self) -> Result<Vec<[u8; 32]>, PileIndexError> {
        <Self as super::BlobStoreList>::blobs(self)
            .map(|item| match item {
                Ok(handle) => Ok(handle.raw),
                Err(GetBlobError::IndexError(err)) => Err(err),
                Err(_) => unreachable!("blob-key iteration only reports index errors"),
            })
            .collect()
    }

    // metadata moved into BlobStoreMeta impl below
}

impl BlobStoreGet for PileReader {
    type GetError<E: Error + Send + Sync + 'static> = GetBlobError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let hash: &Inline<Hash<Blake3>> = handle.as_transmute();
        let (bytes, state) = if let Some(entry) = self.blobs.get(&hash.raw) {
            let IndexEntry {
                state, offset, len, ..
            } = entry.clone();
            let bytes = unsafe {
                let slice = slice_from_raw_parts(self.mmap.as_ptr().add(offset), len as usize)
                    .as_ref()
                    .unwrap();
                Bytes::from_raw_parts(slice, self.mmap.clone())
            };
            let state = *state.get_or_init(|| {
                let computed_hash = Hash::<Blake3>::digest(&bytes);
                if computed_hash == *hash {
                    ValidationState::Validated
                } else {
                    ValidationState::Invalid
                }
            });
            (bytes, state)
        } else {
            let base_handle: Inline<Handle<UnknownBlob>> = handle.transmute();
            let Some((bytes, _, state)) = self
                .base_blob(base_handle)
                .map_err(GetBlobError::IndexError)?
            else {
                return Err(GetBlobError::BlobNotFound);
            };
            (bytes, state)
        };
        match state {
            ValidationState::Validated => {
                // The handle is what we just validated against — reuse
                // it to skip Blake3 recomputation in Blob::new.
                let blob: Blob<S> = Blob::with_handle(bytes.clone(), handle);
                match blob.try_from_blob() {
                    Ok(value) => Ok(value),
                    Err(e) => Err(GetBlobError::ConversionError(e)),
                }
            }
            ValidationState::Invalid => Err(GetBlobError::ValidationError(bytes)),
        }
    }
}

impl super::BlobChildren for PileReader {}

impl BlobStore for Pile {
    type Reader = PileReader;
    type ReaderError = ReadError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh()?;
        Ok(PileReader::new(
            self.mmap.clone(),
            self.base.clone(),
            self.blobs.clone(),
            self.blob_additions.clone(),
        ))
    }
}

impl Pile {
    fn logical_head(
        &self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, PileIndexError> {
        let raw: RawId = id.into();
        if let Some(head) = self.branches.get(&raw) {
            return Ok(Some(*head));
        }
        if self
            .branch_tombstones
            .as_ref()
            .is_some_and(|tombstones| tombstones.get(&raw).is_some())
        {
            return Ok(None);
        }
        match &self.base {
            Some(base) => base.index.pin_head(id),
            None => Ok(None),
        }
    }

    fn logical_weak_pin(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<bool, PileIndexError> {
        if self.weak_pins.get(&handle.raw).is_some() {
            return Ok(true);
        }
        if self
            .weak_unpins
            .as_ref()
            .is_some_and(|unpins| unpins.get(&handle.raw).is_some())
        {
            return Ok(false);
        }
        let Some(base) = &self.base else {
            return Ok(false);
        };
        base.index.has_weak_pin(handle)
    }

    /// Captures the current PATCH-backed locator state for differential testing
    /// against [`crate::repo::pile_index::MappedPileIndex`].
    ///
    /// This does not opt the Pile into persistent indexing; it performs the
    /// same full refresh as the existing default path.
    pub fn patch_index(&mut self) -> Result<PatchPileIndex, ReadError> {
        self.refresh()?;
        Ok(PatchPileIndex {
            blobs: self.materialize_blobs()?,
            branches: self.materialize_branches()?,
            weak_pins: self.materialize_weak_pins()?,
            covered_len: self.applied_length,
        })
    }

    fn materialize_blobs(&self) -> Result<PATCH<32, IdentitySchema, IndexEntry>, ReadError> {
        let Some(base) = &self.base else {
            return Ok(self.blobs.clone());
        };
        let mut blobs = PATCH::new();
        for handle in base.index.blob_handles() {
            let handle = handle.map_err(index_error_as_read)?;
            let locator = base
                .index
                .blob_locator(handle)
                .map_err(index_error_as_read)?
                .ok_or_else(|| {
                    index_error_as_read(PileIndexError::InvalidIndex(
                        "listed base blob has no locator".into(),
                    ))
                })?;
            let offset = usize::try_from(locator.payload_offset).map_err(|_| {
                index_error_as_read(PileIndexError::StaleLocator {
                    offset: locator.payload_offset,
                })
            })?;
            blobs.insert(&Entry::with_value(
                &handle.raw,
                IndexEntry::new(offset, locator.payload_len, locator.timestamp),
            ));
        }
        for key in self.blobs.clone().into_iter_ordered() {
            if let Some(entry) = self.blobs.get(&key) {
                blobs.replace(&Entry::with_value(&key, entry.clone()));
            }
        }
        Ok(blobs)
    }

    fn materialize_branches(
        &self,
    ) -> Result<PATCH<16, IdentitySchema, Inline<Handle<SimpleArchive>>>, ReadError> {
        let Some(base) = &self.base else {
            return Ok(self.branches.clone());
        };
        let mut branches = PATCH::new();
        for id in base.index.pin_ids() {
            let id = id.map_err(index_error_as_read)?;
            if let Some(head) = base.index.pin_head(id).map_err(index_error_as_read)? {
                branches.insert(&Entry::with_value(&id.into(), head));
            }
        }
        if let Some(tombstones) = &self.branch_tombstones {
            for key in tombstones.clone().into_iter_ordered() {
                branches.remove(&key);
            }
        }
        for key in self.branches.clone().into_iter_ordered() {
            if let Some(head) = self.branches.get(&key) {
                branches.replace(&Entry::with_value(&key, *head));
            }
        }
        Ok(branches)
    }

    fn materialize_weak_pins(&self) -> Result<PATCH<32, IdentitySchema>, ReadError> {
        let Some(base) = &self.base else {
            return Ok(self.weak_pins.clone());
        };
        let mut weak = PATCH::new();
        for handle in base.index.weak_pin_handles() {
            let handle = handle.map_err(index_error_as_read)?;
            weak.insert(&Entry::new(&handle.raw));
        }
        if let Some(unpins) = &self.weak_unpins {
            for key in unpins.clone().into_iter_ordered() {
                weak.remove(&key);
            }
        }
        for key in self.weak_pins.clone().into_iter_ordered() {
            weak.insert(&Entry::new(&key));
        }
        Ok(weak)
    }
}

impl PileIndex for PatchPileIndex {
    type BlobHandles<'a> = PatchBlobHandles;
    type PinIds<'a> = PatchPinIds;
    type WeakPinHandles<'a> = PatchWeakPinHandles;

    fn covered_len(&self) -> u64 {
        self.covered_len as u64
    }

    fn blob_locator(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<PileBlobLocator>, PileIndexError> {
        let hash: Inline<Hash<Blake3>> = handle.transmute();
        let Some(entry) = self.blobs.get(&hash.raw) else {
            return Ok(None);
        };
        Ok(Some(PileBlobLocator {
            payload_offset: entry.offset as u64,
            payload_len: entry.len,
            timestamp: entry.timestamp,
        }))
    }

    fn blob_handles(&self) -> Self::BlobHandles<'_> {
        PatchBlobHandles {
            inner: self.blobs.clone().into_iter_ordered(),
        }
    }

    fn pin_head(&self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, PileIndexError> {
        Ok(self.branches.get(&id.into()).copied())
    }

    fn pin_ids(&self) -> Self::PinIds<'_> {
        PatchPinIds {
            inner: self.branches.clone().into_iter_ordered(),
        }
    }

    fn has_weak_pin(&self, handle: Inline<Handle<UnknownBlob>>) -> Result<bool, PileIndexError> {
        Ok(self.weak_pins.get(&handle.raw).is_some())
    }

    fn weak_pin_handles(&self) -> Self::WeakPinHandles<'_> {
        PatchWeakPinHandles {
            inner: self.weak_pins.clone().into_iter_ordered(),
        }
    }
}

/// Error returned when opening or refreshing a [`Pile`].
#[derive(Debug)]
pub enum ReadError {
    /// Underlying I/O failure.
    IoError(std::io::Error),
    /// The pile contains corrupted data starting at `valid_length`.
    CorruptPile {
        /// Byte offset where the first invalid record was found.
        valid_length: usize,
    },
    /// The pile file exceeds the addressable range.
    FileTooLarge {
        /// Actual file length.
        length: usize,
    },
    /// A previously accepted mapped-prefix locator failed validation. This is
    /// distinct from authoritative tail corruption and is never amputatable.
    IndexError(Box<PileIndexError>),
}

/// Failure of the strict opt-in mapped-prefix open path.
#[derive(Debug)]
pub enum IndexedOpenError {
    /// Opening or reading the authoritative pile failed.
    Pile(ReadError),
    /// The derived `.pidx` cache was unavailable, stale, or malformed.
    Index(PileIndexError),
}

impl std::fmt::Display for IndexedOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pile(err) => write!(f, "failed to open pile: {err}"),
            Self::Index(err) => write!(f, "failed to attach pile index: {err}"),
        }
    }
}

impl std::error::Error for IndexedOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pile(err) => Some(err),
            Self::Index(err) => Some(err),
        }
    }
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
            ReadError::IndexError(err) => write!(f, "Pile index error: {err}"),
        }
    }
}
impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(err) => Some(err),
            Self::IndexError(err) => Some(err.as_ref()),
            Self::CorruptPile { .. } | Self::FileTooLarge { .. } => None,
        }
    }
}

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
            ReadError::IndexError(err) => std::io::Error::new(std::io::ErrorKind::InvalidData, err),
        }
    }
}

/// Error returned when appending a blob to a [`Pile`].
#[derive(Debug)]
pub enum InsertError {
    /// Underlying I/O failure.
    IoError(std::io::Error),
    /// System clock error when timestamping the record.
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
impl std::error::Error for InsertError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(err) => Some(err),
            Self::TimeError(err) => Some(err),
        }
    }
}

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

/// Error returned when appending a pin-head update or weak-pin marker
/// record to a [`Pile`].
pub enum PileWriteError {
    /// Underlying I/O failure.
    IoError(std::io::Error),
}

impl std::error::Error for PileWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError(err) => Some(err),
        }
    }
}

impl std::fmt::Debug for PileWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PileWriteError::IoError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl std::fmt::Display for PileWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PileWriteError::IoError(err) => write!(f, "IO error: {err}"),
        }
    }
}

impl From<std::io::Error> for PileWriteError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<ReadError> for PileWriteError {
    fn from(err: ReadError) -> Self {
        Self::IoError(err.into())
    }
}

/// Error returned when retrieving a blob from a [`Pile`].
#[derive(Debug)]
pub enum GetBlobError<E: Error> {
    /// No blob with the given handle exists in the pile.
    BlobNotFound,
    /// The blob's hash does not match its stored digest.
    ValidationError(Bytes),
    /// The blob was found and valid but deserialization failed.
    ConversionError(E),
    /// The accepted mapped prefix no longer yields a trustworthy locator.
    /// This is never translated into absence or a tail-corruption error.
    IndexError(PileIndexError),
}

impl<E: Error> std::fmt::Display for GetBlobError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetBlobError::BlobNotFound => write!(f, "Blob not found"),
            GetBlobError::ConversionError(err) => write!(f, "Conversion error: {err}"),
            GetBlobError::ValidationError(_) => write!(f, "Validation error"),
            GetBlobError::IndexError(err) => write!(f, "Pile index error: {err}"),
        }
    }
}

impl<E: Error> std::error::Error for GetBlobError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IndexError(err) => Some(err),
            _ => None,
        }
    }
}

/// Error returned by [`Pile::flush`] and [`Pile::close`].
#[derive(Debug)]
pub enum FlushError {
    /// Underlying I/O failure.
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

impl Pile {
    /// Opens an existing pile file. Returns an error if the file does not
    /// exist — create the file first with [`std::fs::File::create`] or
    /// equivalent if you need a fresh pile.
    ///
    /// The returned pile has no in-memory index; callers should invoke
    /// [`Self::refresh`] to load existing data. After a crash left a torn
    /// tail, [`Self::amputate`] loads and **truncates the file at the first
    /// invalid record** — a destructive last resort, not an open path.
    pub fn open(path: &Path) -> Result<Self, ReadError> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;
        let length_u64 = file.metadata()?.len();
        let length = usize::try_from(length_u64)
            .map_err(|_| ReadError::FileTooLarge { length: usize::MAX })?;
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
            base: None,
            blobs: PATCH::<32, IdentitySchema, IndexEntry>::new(),
            blob_additions: None,
            branches: PATCH::<16, IdentitySchema, Inline<Handle<SimpleArchive>>>::new(),
            branch_tombstones: None,
            weak_pins: PATCH::<32, IdentitySchema>::new(),
            weak_unpins: None,
            applied_length: 0,
        })
    }

    /// Strictly opens `path` with an immutable `.pidx` prefix. Like
    /// [`Self::open`], this seeds state but leaves the authoritative tail lazy;
    /// [`Self::refresh`] and normal operations replay it canonically. Cache
    /// failures are reported distinctly and never trigger a hidden fallback.
    pub fn open_indexed(path: &Path, index_path: &Path) -> Result<Self, IndexedOpenError> {
        let mut pile = Self::open(path).map_err(IndexedOpenError::Pile)?;
        match pile.attach_index(index_path) {
            Ok(()) => Ok(pile),
            Err(err) => {
                let _ = pile.close();
                Err(err)
            }
        }
    }

    /// Best-effort indexed open. A missing, malformed, stale, or
    /// identity-mismatched derived cache falls back to the unchanged full
    /// PATCH replay path. Like [`Self::open`], both outcomes leave replay lazy;
    /// the first normal operation performs it. Authoritative tail corruption
    /// after a base has been accepted still fails loud and never falls back.
    pub fn open_indexed_or_replay(path: &Path, index_path: &Path) -> Result<Self, ReadError> {
        let mut pile = Self::open(path)?;
        match pile.attach_index(index_path) {
            Ok(()) => Ok(pile),
            Err(IndexedOpenError::Index(_)) => Ok(pile),
            Err(IndexedOpenError::Pile(err)) => {
                let _ = pile.close();
                Err(err)
            }
        }
    }

    fn attach_index(&mut self, index_path: &Path) -> Result<(), IndexedOpenError> {
        self.file
            .lock_shared()
            .map_err(ReadError::from)
            .map_err(IndexedOpenError::Pile)?;
        let result = (|| {
            let file_len_u64 = self
                .file
                .metadata()
                .map_err(ReadError::from)
                .map_err(IndexedOpenError::Pile)?
                .len();
            let file_len = usize::try_from(file_len_u64).map_err(|_| {
                IndexedOpenError::Pile(ReadError::FileTooLarge { length: usize::MAX })
            })?;
            self.ensure_mapped(file_len)
                .map_err(IndexedOpenError::Pile)?;
            let pile_bytes = self.mapped_bytes(file_len);
            let mapped = MappedPileIndex::open_prefix_on_file(&self.file, pile_bytes, index_path)
                .map_err(|err| match err {
                PileIndexError::PileIo(err) => IndexedOpenError::Pile(ReadError::IoError(err)),
                other => IndexedOpenError::Index(other),
            })?;
            let covered_len = usize::try_from(mapped.covered_len()).map_err(|_| {
                IndexedOpenError::Index(PileIndexError::InvalidIndex(
                    "covered length is not addressable".into(),
                ))
            })?;

            // This is the commit point: every cache error above leaves the
            // ordinary full-replay state untouched. The tail intentionally
            // stays lazy, matching `Pile::open`, so callers can inspect or
            // explicitly amputate a torn tail from this accepted boundary.
            self.base = Some(IndexedPileBase::new(mapped));
            self.blob_additions = Some(PATCH::new());
            self.branch_tombstones = Some(PATCH::new());
            self.weak_unpins = Some(PATCH::new());
            self.applied_length = covered_len;
            Ok(())
        })();
        let unlock = self.file.unlock().map_err(ReadError::from);
        match (result, unlock) {
            (_, Err(err)) => Err(IndexedOpenError::Pile(err)),
            (Err(err), Ok(())) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn ensure_mapped(&mut self, file_len: usize) -> Result<(), ReadError> {
        if file_len <= self.mmap.len() {
            return Ok(());
        }
        let mapped_size = file_len
            .checked_next_power_of_two()
            .ok_or(ReadError::FileTooLarge { length: file_len })?;
        self.mmap = Arc::new(
            MmapOptions::new()
                .len(mapped_size)
                .map_raw_read_only(&self.file)?,
        );
        Ok(())
    }

    fn mapped_bytes(&self, len: usize) -> Bytes {
        if len == 0 {
            return Bytes::empty();
        }
        unsafe {
            let slice = slice_from_raw_parts(self.mmap.as_ptr(), len)
                .as_ref()
                .unwrap();
            Bytes::from_raw_parts(slice, self.mmap.clone())
        }
    }

    fn base_blob(
        &self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<(Bytes, PileBlobLocator, ValidationState)>, PileIndexError> {
        let Some(base) = &self.base else {
            return Ok(None);
        };
        read_base_blob(base, &self.mmap, handle)
    }

    /// Refreshes in-memory state from newly appended records.
    ///
    /// Aborts immediately if the underlying pile file has shrunk below the
    /// portion already applied since the last refresh. Truncating validated data
    /// would invalidate existing `Bytes` handles and continuing would result in
    /// undefined behavior.
    ///
    /// This acquires a shared file lock to avoid racing with [`Self::amputate`],
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
    fn apply_next(&mut self) -> Result<Option<Applied>, ReadError> {
        let file_len_u64 = self.file.metadata()?.len();
        let file_len = usize::try_from(file_len_u64)
            .map_err(|_| ReadError::FileTooLarge { length: usize::MAX })?;
        if file_len < self.applied_length {
            // Truncation below `applied_length` invalidates previously issued
            // `Bytes` handles, so there is no safe recovery path.
            std::process::abort();
        }
        if file_len == self.applied_length {
            return Ok(None);
        }
        self.ensure_mapped(file_len)?;
        let start_offset = self.applied_length;
        let slice = unsafe {
            slice_from_raw_parts(
                self.mmap.as_ptr().add(start_offset),
                file_len - start_offset,
            )
            .as_ref()
            .unwrap()
        };
        // Single decoder shared with [`PileRecords`] — understands every
        // record format ever written (V1 and V3 alike).
        let record = decode_record(slice, start_offset)?;
        let next_applied_length = start_offset + record.len;
        let applied = match record.content {
            PileRecordContent::Blob {
                timestamp,
                hash,
                data_offset,
                data_len,
            } => {
                let entry = Entry::with_value(
                    &hash.raw,
                    IndexEntry::new(data_offset, data_len as u64, timestamp),
                );
                let mut insert_tail = false;
                let mut is_addition = false;
                match self.blobs.get(&hash.raw) {
                    None => {
                        let handle: Inline<Handle<UnknownBlob>> = hash.into();
                        match self
                            .base_blob(handle)
                            .map_err(index_error_as_read)?
                            .map(|(_, _, state)| state)
                        {
                            Some(ValidationState::Validated) => {}
                            Some(ValidationState::Invalid) => insert_tail = true,
                            None => {
                                insert_tail = true;
                                is_addition = self.base.is_some();
                            }
                        }
                    }
                    Some(entry_ref) => {
                        // Duplicate hash: keep the existing record unless it
                        // turns out to be corrupt, in which case the fresh
                        // copy replaces it.
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
                            let computed = Hash::<Blake3>::digest(&bytes);
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
                if insert_tail {
                    self.blobs.insert(&entry);
                    if is_addition {
                        self.blob_additions
                            .as_mut()
                            .expect("indexed pile has an additions overlay")
                            .insert(&Entry::new(&hash.raw));
                    }
                }
                Applied::Blob { hash }
            }
            PileRecordContent::Branch { branch_id, head } => {
                let entry = Entry::with_value(&branch_id.into(), head);
                // Replace existing mapping (if any) with the new head.
                self.branches.replace(&entry);
                if let Some(tombstones) = &mut self.branch_tombstones {
                    tombstones.remove(&branch_id.into());
                }
                Applied::Branch {
                    id: branch_id,
                    hash: head.into(),
                }
            }
            PileRecordContent::BranchTombstone { branch_id } => {
                self.branches.remove(&branch_id.into());
                if let Some(tombstones) = &mut self.branch_tombstones {
                    tombstones.insert(&Entry::new(&branch_id.into()));
                }
                Applied::BranchTombstone { id: branch_id }
            }
            PileRecordContent::WeakPin { handle } => {
                self.weak_pins.insert(&Entry::new(&handle.raw));
                if let Some(unpins) = &mut self.weak_unpins {
                    unpins.remove(&handle.raw);
                }
                Applied::WeakPin { handle }
            }
            PileRecordContent::WeakUnpin { handle } => {
                self.weak_pins.remove(&handle.raw);
                if let Some(unpins) = &mut self.weak_unpins {
                    unpins.insert(&Entry::new(&handle.raw));
                }
                Applied::WeakUnpin { handle }
            }
        };
        self.applied_length = next_applied_length;
        Ok(Some(applied))
    }

    fn refresh_locked(&mut self) -> Result<(), ReadError> {
        while self.apply_next()?.is_some() {}
        Ok(())
    }

    /// Amputates the pile's tail: **TRUNCATES the file at the first invalid
    /// record, destroying everything after it.**
    ///
    /// This is a last-resort surgical recovery for a torn tail left by a
    /// crashed or interrupted append — never a routine open path. Everything
    /// past the first record this binary cannot parse is *gone from disk*,
    /// including data that a newer binary would have read fine (a stale
    /// binary sees newer record formats as corruption and would amputate
    /// them). If you are not certain the tail is a torn write, take a copy
    /// of the file first and prefer the non-mutating [`Self::refresh`],
    /// which fails loud without touching the file.
    ///
    /// The method first attempts a regular [`Self::refresh`]. If corruption is
    /// detected, it acquires an exclusive lock, re-attempts the refresh and,
    /// upon confirming the corruption, truncates the pile to the last known
    /// good offset. The exclusive lock blocks other readers so truncation
    /// cannot race with [`Self::refresh`].
    pub fn amputate(&mut self) -> Result<(), ReadError> {
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
            std::ptr::drop_in_place(&mut this.base);
            std::ptr::drop_in_place(&mut this.blobs);
            std::ptr::drop_in_place(&mut this.blob_additions);
            std::ptr::drop_in_place(&mut this.branches);
            std::ptr::drop_in_place(&mut this.branch_tombstones);
            std::ptr::drop_in_place(&mut this.weak_pins);
            std::ptr::drop_in_place(&mut this.weak_unpins);
        }

        res
    }
}

impl Drop for Pile {
    fn drop(&mut self) {
        eprintln!("warning: Pile dropped without calling close(); data may not be persisted");
    }
}

// Implement the repository storage close trait so callers can call
// `repo.close()` when the repository was created with a `Pile` storage.
impl crate::repo::StorageClose for Pile {
    type Error = FlushError;

    fn close(self) -> Result<(), Self::Error> {
        Pile::close(self)
    }
}

// Generic durability hook: appended records (blobs, branch updates,
// weak-pin markers) are not crash-durable until flushed — see the
// inherent [`Pile::flush`].
impl crate::repo::StorageFlush for Pile {
    type Error = FlushError;

    fn flush(&mut self) -> Result<(), Self::Error> {
        Pile::flush(self)
    }
}

use super::BlobStore;
use super::BlobStoreGet;
use super::BlobStoreList;
use super::BlobStorePut;
use super::PinStore;
use super::PushResult;
use super::WeakPinStore;

/// Iterator returned by [`PileReader::iter`].
///
/// Iterates over all `(Handle, Blob)` pairs currently stored in the pile.
/// Owned iterator over all blobs currently stored in the pile. This collects
/// a snapshot of keys/indices at iterator creation so the iterator does not
/// borrow the underlying [`PATCH`] and can live independently of the [`Pile`].
pub struct PileBlobStoreIter {
    reader: PileReader,
    handles: PileBlobStoreListIter,
}

impl Iterator for PileBlobStoreIter {
    type Item = Result<(Inline<Handle<UnknownBlob>>, Blob<UnknownBlob>), GetBlobError<Infallible>>;

    fn next(&mut self) -> Option<Self::Item> {
        let handle = match self.handles.next()? {
            Ok(handle) => handle,
            Err(err) => return Some(Err(err)),
        };
        Some(
            self.reader
                .get::<Blob<UnknownBlob>, UnknownBlob>(handle)
                .map(|blob| (handle, blob)),
        )
    }
}

enum TailBlobKeys {
    Full(crate::patch::PATCHIntoOrderedIterator<32, IdentitySchema, IndexEntry>),
    Additions(crate::patch::PATCHIntoOrderedIterator<32, IdentitySchema, ()>),
    Materialized(std::vec::IntoIter<[u8; 32]>),
}

impl TailBlobKeys {
    fn next(&mut self) -> Option<[u8; 32]> {
        match self {
            Self::Full(inner) => inner.next(),
            Self::Additions(inner) => inner.next(),
            Self::Materialized(inner) => inner.next(),
        }
    }
}

/// Sorted merge of a mapped base key stream with owned tail additions.
pub struct PileBlobStoreListIter {
    base: Option<OwnedMappedBlobHandles>,
    tail: TailBlobKeys,
    next_base: Option<Result<Inline<Handle<UnknownBlob>>, PileIndexError>>,
    next_tail: Option<[u8; 32]>,
    pending_error: Option<PileIndexError>,
}

impl Iterator for PileBlobStoreListIter {
    type Item = Result<Inline<Handle<UnknownBlob>>, GetBlobError<Infallible>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.pending_error.take() {
            return Some(Err(GetBlobError::IndexError(err)));
        }
        if self.next_base.is_none() {
            self.next_base = self.base.as_mut().and_then(Iterator::next);
        }
        if matches!(self.next_base.as_ref(), Some(Err(_))) {
            let Err(err) = self.next_base.take().unwrap() else {
                unreachable!()
            };
            return Some(Err(GetBlobError::IndexError(err)));
        }
        if self.next_tail.is_none() {
            self.next_tail = self.tail.next();
        }

        match (self.next_base.as_ref(), self.next_tail) {
            (None, None) => None,
            (Some(Ok(_)), None) => Some(Ok(self.next_base.take().unwrap().unwrap())),
            (None, Some(key)) => {
                self.next_tail = None;
                Some(Ok(Inline::new(key)))
            }
            (Some(Ok(base)), Some(tail)) => match base.raw.cmp(&tail) {
                std::cmp::Ordering::Less => Some(Ok(self.next_base.take().unwrap().unwrap())),
                std::cmp::Ordering::Greater => {
                    self.next_tail = None;
                    Some(Ok(Inline::new(tail)))
                }
                std::cmp::Ordering::Equal => {
                    self.next_tail = None;
                    self.next_base.take();
                    Some(Ok(Inline::new(tail)))
                }
            },
            (Some(Err(_)), _) => unreachable!(),
        }
    }
}

impl BlobStoreList for PileReader {
    type Err = GetBlobError<Infallible>;
    type Iter<'a> = PileBlobStoreListIter;

    fn blobs(&self) -> Self::Iter<'_> {
        match (&self.base, &self.blob_additions) {
            (Some(base), Some(additions)) => PileBlobStoreListIter {
                base: Some(base.index.owned_blob_handles()),
                tail: TailBlobKeys::Additions(additions.clone().into_iter_ordered()),
                next_base: None,
                next_tail: None,
                pending_error: None,
            },
            _ => PileBlobStoreListIter {
                base: None,
                tail: TailBlobKeys::Full(self.blobs.clone().into_iter_ordered()),
                next_base: None,
                next_tail: None,
                pending_error: None,
            },
        }
    }

    /// Cheap PATCH-level set difference between this reader's blob index
    /// and `old`'s. Both readers hold copy-on-write clones of their pile's
    /// PATCH, so this gives the exact set of blob hashes added between
    /// the two snapshots without having to enumerate either side.
    fn blobs_diff(&self, old: &Self) -> Self::Iter<'_> {
        match (
            &self.base,
            &old.base,
            &self.blob_additions,
            &old.blob_additions,
        ) {
            (Some(base), Some(old_base), Some(additions), Some(old_additions))
                if Arc::ptr_eq(&base.index, &old_base.index) =>
            {
                let diff = additions.difference(old_additions);
                PileBlobStoreListIter {
                    base: None,
                    tail: TailBlobKeys::Additions(diff.into_iter_ordered()),
                    next_base: None,
                    next_tail: None,
                    pending_error: None,
                }
            }
            (None, None, _, _) => {
                let diff = self.blobs.difference(&old.blobs);
                PileBlobStoreListIter {
                    base: None,
                    tail: TailBlobKeys::Full(diff.into_iter_ordered()),
                    next_base: None,
                    next_tail: None,
                    pending_error: None,
                }
            }
            _ => match (self.logical_blob_keys(), old.logical_blob_keys()) {
                (Ok(current), Ok(previous)) => {
                    let mut difference = Vec::new();
                    let mut old_position = 0usize;
                    for key in current {
                        while old_position < previous.len() && previous[old_position] < key {
                            old_position += 1;
                        }
                        if old_position == previous.len() || previous[old_position] != key {
                            difference.push(key);
                        }
                    }
                    PileBlobStoreListIter {
                        base: None,
                        tail: TailBlobKeys::Materialized(difference.into_iter()),
                        next_base: None,
                        next_tail: None,
                        pending_error: None,
                    }
                }
                (Err(err), _) | (_, Err(err)) => PileBlobStoreListIter {
                    base: None,
                    tail: TailBlobKeys::Materialized(Vec::new().into_iter()),
                    next_base: None,
                    next_tail: None,
                    pending_error: Some(err),
                },
            },
        }
    }
}

/// Iterator over pin ids stored in the pile's PATCH, using the PATCH's
/// built-in key iterator to avoid allocating a full Vec of ids.
pub struct PileBranchStoreIter {
    inner:
        crate::patch::PATCHIntoOrderedIterator<16, IdentitySchema, Inline<Handle<SimpleArchive>>>,
}

impl Iterator for PileBranchStoreIter {
    type Item = Result<Id, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        // The owned ordered iterator yields key arrays ([u8; 16]) by value.
        // The `apply_next` path guarantees that a nil (all-zero) pin id
        // is never inserted into the PATCH; therefore we can safely `expect`
        // a valid `Id` here and treat a nil id as an invariant violation.
        let key = self.inner.next()?;
        let id = Id::new(key).expect("nil pin id inserted into patch");
        Some(Ok(id))
    }
}

impl BlobStorePut for Pile {
    type PutError = InsertError;

    /// Inserts a blob into the pile and returns its handle.
    ///
    /// For records up to `ATOMIC_WRITE_LIMIT` the append relies on the
    /// kernel's atomic `write_vectored` guarantee, so multiple writers can
    /// hold a shared file lock and proceed concurrently. Larger records
    /// take an exclusive lock and append via plain `write_all`, trading
    /// concurrency for reach — the recovery path
    /// ([`Pile::amputate`]) truncates any partial tail left by a crash,
    /// so a multi-`write` record is still crash-safe. Multiple writers
    /// are safe only on filesystems guaranteeing atomic `write`/`vwrite`
    /// appends; other filesystems may corrupt the pile.
    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.put_impl(item)
    }
}

impl Pile {
    /// Shared blob-append. Writes a V3 record: a fixed 256-byte header, the blob
    /// data at `record_start + V3_HEADER_LEN`, and post-padding to a 256-byte
    /// multiple. Because V3 has no offset-derived pad, the append uses the atomic
    /// shared-lock fast path for records up to `ATOMIC_WRITE_LIMIT` (no exclusive lock needed —
    /// a fixed header has no start offset to stabilize). The data is
    /// absolutely 256-aligned (zero-copy GPU-aliasable) in a pure-V3 pile, which
    /// stays 256-aligned because every V3 record is a 256-byte multiple.
    fn put_impl<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, InsertError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let blob = IntoBlob::to_blob(item);
        let blob_size = blob.bytes.len();
        let padding = v3_post_pad(blob_size);
        let record_size = V3_HEADER_LEN + blob_size + padding;
        let use_atomic = record_size <= ATOMIC_WRITE_LIMIT;

        if use_atomic {
            self.file.lock_shared()?;
        } else {
            // Oversized record: exclude other writers for the duration of
            // the multi-syscall append. Shared readers ([`refresh`]) block
            // until unlock, so they never observe a partially-written tail.
            self.file.lock()?;
        }
        let res = (|| {
            self.refresh_locked().map_err(InsertError::from)?;

            let handle: Inline<Handle<S>> = blob.get_handle();
            let hash: Inline<Hash<Blake3>> = handle.into();

            let tail_candidate = self.blobs.get(&hash.raw).is_some();
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
                    let computed = Hash::<Blake3>::digest(&bytes);
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
            if !tail_candidate {
                let base_handle: Inline<Handle<UnknownBlob>> = handle.transmute();
                if matches!(
                    self.base_blob(base_handle)
                        .map_err(|err| InsertError::from(index_error_as_read(err)))?
                        .map(|(_, _, state)| state),
                    Some(ValidationState::Validated)
                ) {
                    return Ok(handle.transmute());
                }
            }

            let now_in_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let header = BlobHeaderV3::new(now_in_ms as u64, blob_size as u64, hash);
            let actual_record_size = V3_HEADER_LEN + blob_size + padding;
            // post-pad is < 256.
            let zero_buf = [0u8; V3_ALIGNMENT];
            if use_atomic {
                let bufs = [
                    IoSlice::new(header.as_bytes()),
                    IoSlice::new(blob.bytes.as_ref()),
                    IoSlice::new(&zero_buf[..padding]),
                ];
                let written = self.file.write_vectored(&bufs)?;
                if written != actual_record_size {
                    return Err(InsertError::IoError(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write blob record",
                    )));
                }
            } else {
                // Separate `write_all` calls — payload dominates, so the extra
                // syscalls for header/padding are negligible. Any partial
                // completion after a crash is caught by `amputate`.
                self.file.write_all(header.as_bytes())?;
                self.file.write_all(blob.bytes.as_ref())?;
                if padding > 0 {
                    self.file.write_all(&zero_buf[..padding])?;
                }
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
                    Some(Applied::WeakPin { .. }) => {}
                    Some(Applied::WeakUnpin { .. }) => {}
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

impl PinStore for Pile {
    type PinsError = ReadError;
    // Pulling a head may require refreshing the pile which can fail; expose
    // the underlying `ReadError` so callers can surface refresh failures.
    type HeadError = ReadError;
    type UpdateError = PileWriteError;

    type ListIter<'a> = PileBranchStoreIter;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        // Ensure newly appended records are applied before enumerating
        // branches so external writers are visible to callers.
        self.refresh()?;
        // Create an owned ordered iterator from the PATCH clone so the
        // returned iterator does not borrow from `self.branches`. This avoids
        // allocating a temporary Vec of ids while preserving tree-order.
        let cloned = self.materialize_branches()?;
        let inner = cloned.into_iter_ordered();
        Ok(PileBranchStoreIter { inner })
    }

    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        // Ensure newly appended records are applied before returning the head.
        // This keeps callers up-to-date with any external writers that appended
        // to the pile file.
        self.refresh()?;
        self.logical_head(id).map_err(index_error_as_read)
    }

    fn pin_snapshot(&mut self) -> Result<super::PinSnapshot, Self::PinsError> {
        // Pin index is already a PATCH; clone is an O(1) refcount bump.
        self.refresh()?;
        self.materialize_branches()
    }

    /// Updates the head of `id` to `new` if it matches `old`.
    ///
    /// This method does not verify that `new` refers to a blob stored in the pile,
    /// allowing piles to reference external data and serve as head-only stores.
    ///
    /// The update is written to the pile but is **not durable** until
    /// [`Pile::flush`] is called. Callers must explicitly flush to ensure
    /// pin updates survive crashes.
    ///
    /// After the header is written, the record is read back with `apply_next`
    /// while still holding the lock, ensuring the update is applied without an
    /// additional refresh pass.
    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<super::PushResult, Self::UpdateError> {
        self.file.lock()?;
        let res = (|| {
            self.refresh_locked().map_err(PileWriteError::from)?;
            let current_hash = self
                .logical_head(id)
                .map_err(|err| PileWriteError::from(index_error_as_read(err)))?;
            if current_hash != old {
                return Ok(PushResult::Conflict(current_hash));
            }

            // No-op short-circuit: if the requested head is already
            // what we have, return success without appending a record.
            // The pin table is logically a (id → head) map; a write
            // where new == current carries no information and would
            // just churn the append-only file. Steady-state gossip
            // rebroadcasts of unchanged heads (e.g. tracking-pin
            // re-publication at 30s ticks) hit this path heavily.
            if current_hash == new {
                return Ok(PushResult::Success());
            }

            // V3 branch/tombstone records: fixed 256-byte header, no data, so the
            // record is exactly one 256-byte unit — keeping a pure-V3 pile
            // 256-aligned throughout (branches write under the exclusive lock).
            let (expected, write_res) = match new {
                Some(new) => {
                    let header = BranchHeaderV3::new(id, new);
                    (V3_HEADER_LEN, self.file.write(header.as_bytes()))
                }
                None => {
                    let header = BranchTombstoneHeaderV3::new(id);
                    (V3_HEADER_LEN, self.file.write(header.as_bytes()))
                }
            };
            let written = match write_res {
                Ok(n) => n,
                Err(e) => return Err(PileWriteError::IoError(e)),
            };
            if written != expected {
                return Err(PileWriteError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write branch header",
                )));
            }
            match self.apply_next().map_err(PileWriteError::from)? {
                Some(Applied::Branch { id: bid, hash }) if matches!(new, Some(new) if bid == id && hash == new.into()) => {
                    Ok(PushResult::Success())
                }
                Some(Applied::BranchTombstone { id: bid }) if new.is_none() && bid == id => {
                    Ok(PushResult::Success())
                }
                Some(_) => Err(PileWriteError::IoError(std::io::Error::other(
                    "unexpected record after branch write",
                ))),
                None => Err(PileWriteError::IoError(std::io::Error::other(
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

/// Iterator over the LWW-resolved weak-pinned handles stored in the pile,
/// using the PATCH's ordered key iterator (byte order, deterministic).
pub struct PileWeakPinIter {
    inner: crate::patch::PATCHIntoOrderedIterator<32, IdentitySchema, ()>,
}

impl Iterator for PileWeakPinIter {
    type Item = Result<Inline<Handle<UnknownBlob>>, PileWriteError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw = self.inner.next()?;
        Some(Ok(Inline::<Handle<UnknownBlob>>::new(raw)))
    }
}

impl Pile {
    /// Shared weak-marker append. Mirrors [`PinStore::update`]'s write path:
    /// exclusive lock, refresh, no-op short-circuit when the LWW state already
    /// matches, a single fixed 256-byte header write (keeping a pure-V3 pile
    /// 256-aligned), and an `apply_next` read-back while still holding the
    /// lock. Like branch updates, the record is **not durable** until
    /// [`Pile::flush`] is called.
    fn write_weak_marker(
        &mut self,
        handle: Inline<Handle<UnknownBlob>>,
        pin: bool,
    ) -> Result<(), PileWriteError> {
        self.file.lock()?;
        let res = (|| {
            self.refresh_locked().map_err(PileWriteError::from)?;

            // No-op short-circuit: the weak set is logically a per-handle
            // LWW cell; re-asserting the current state carries no
            // information and would just churn the append-only file.
            let current = self
                .logical_weak_pin(handle)
                .map_err(|err| PileWriteError::from(index_error_as_read(err)))?;
            if current == pin {
                return Ok(());
            }

            let write_res = if pin {
                let header = WeakPinHeaderV3::new(handle);
                self.file.write(header.as_bytes())
            } else {
                let header = WeakUnpinHeaderV3::new(handle);
                self.file.write(header.as_bytes())
            };
            let written = write_res.map_err(PileWriteError::IoError)?;
            if written != V3_HEADER_LEN {
                return Err(PileWriteError::IoError(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write weak-pin header",
                )));
            }
            match self.apply_next().map_err(PileWriteError::from)? {
                Some(Applied::WeakPin { handle: h }) if pin && h == handle => Ok(()),
                Some(Applied::WeakUnpin { handle: h }) if !pin && h == handle => Ok(()),
                Some(_) => Err(PileWriteError::IoError(std::io::Error::other(
                    "unexpected record after weak-pin write",
                ))),
                None => Err(PileWriteError::IoError(std::io::Error::other(
                    "weak-pin marker missing after write",
                ))),
            }
        })();
        let unlock_res = self.file.unlock();
        res?;
        unlock_res?;
        Ok(())
    }
}

impl WeakPinStore for Pile {
    type WeakPinError = PileWriteError;
    type WeakListIter<'a> = PileWeakPinIter;

    /// Appends a weak-pin record for `handle`. Durable across reopen (the
    /// record is replayed by the scan), subject to the same flush rules as
    /// branch updates: call [`Pile::flush`] to make it crash-durable.
    fn pin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.write_weak_marker(handle.transmute(), true)
    }

    /// Appends a weak-unpin record for `handle`, retracting any prior weak
    /// pin (last-writer-wins by log position).
    fn unpin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.write_weak_marker(handle.transmute(), false)
    }

    fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError> {
        // Ensure newly appended records are applied before enumerating so
        // external writers are visible to callers (mirrors `pins`).
        self.refresh()?;
        let cloned = self.materialize_weak_pins().map_err(PileWriteError::from)?;
        Ok(PileWeakPinIter {
            inner: cloned.into_iter_ordered(),
        })
    }
}

impl crate::repo::BlobStoreMeta for PileReader {
    type MetaError = PileIndexError;

    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<Option<crate::repo::BlobMetadata>, Self::MetaError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let hash: &Inline<Hash<Blake3>> = handle.as_transmute();
        let (bytes, timestamp, state) = if let Some(entry) = self.blobs.get(&hash.raw) {
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
            let state = *state.get_or_init(|| {
                let computed_hash = Hash::<Blake3>::digest(&bytes);
                if computed_hash == *hash {
                    ValidationState::Validated
                } else {
                    ValidationState::Invalid
                }
            });
            (bytes, timestamp, state)
        } else {
            let base_handle: Inline<Handle<UnknownBlob>> = handle.transmute();
            let Some((bytes, locator, state)) = self.base_blob(base_handle)? else {
                return Ok(None);
            };
            (bytes, locator.timestamp, state)
        };
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
    use std::io::{Seek, Write};
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tempfile;

    use crate::repo::BlobStoreMeta;
    use crate::repo::PushResult;

    fn fresh_empty_pile_path(dir: &tempfile::TempDir, name: &str) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::File::create(&path).unwrap();
        path
    }

    fn v3_blob_record(
        handle: Inline<Handle<UnknownBlob>>,
        payload: &[u8],
        timestamp: u64,
    ) -> Vec<u8> {
        let hash: Inline<Hash<Blake3>> = handle.into();
        let header = BlobHeaderV3::new(timestamp, payload.len() as u64, hash);
        let record_len = V3_HEADER_LEN + payload.len() + v3_post_pad(payload.len());
        let mut record = Vec::with_capacity(record_len);
        record.extend_from_slice(header.as_bytes());
        record.extend_from_slice(payload);
        record.resize(record_len, 0);
        record
    }

    fn append_raw(path: &Path, bytes: &[u8]) {
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    fn v3_branch_record(branch: Id, head: Option<Inline<Handle<SimpleArchive>>>) -> Vec<u8> {
        match head {
            Some(head) => BranchHeaderV3::new(branch, head).as_bytes().to_vec(),
            None => BranchTombstoneHeaderV3::new(branch).as_bytes().to_vec(),
        }
    }

    fn v3_weak_record(handle: Inline<Handle<UnknownBlob>>, pin: bool) -> Vec<u8> {
        if pin {
            WeakPinHeaderV3::new(handle).as_bytes().to_vec()
        } else {
            WeakUnpinHeaderV3::new(handle).as_bytes().to_vec()
        }
    }

    fn overlay_count<const N: usize, V: Clone>(patch: &PATCH<N, IdentitySchema, V>) -> usize {
        patch.clone().into_iter_ordered().count()
    }

    #[test]
    fn indexed_valid_base_beats_tail_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "valid-base.pile");
        let index_path = dir.path().join("valid-base.pidx");
        let payload = b"chosen";
        let mut pile = Pile::open(&path).unwrap();
        let handle: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                payload.to_vec(),
            )))
            .unwrap();
        pile.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();
        let base_locator = MappedPileIndex::open(&path, &index_path)
            .unwrap()
            .blob_locator(handle)
            .unwrap()
            .unwrap();

        append_raw(&path, &v3_blob_record(handle, b"broken", 2));
        append_raw(&path, &v3_blob_record(handle, payload, 3));

        let mut pile = Pile::open_indexed(&path, &index_path).unwrap();
        pile.refresh().unwrap();
        assert_eq!(overlay_count(&pile.blobs), 0);
        assert_eq!(overlay_count(pile.blob_additions.as_ref().unwrap()), 0);
        assert_eq!(
            pile.base
                .as_ref()
                .unwrap()
                .validations
                .lock()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            pile.patch_index()
                .unwrap()
                .blob_locator(handle)
                .unwrap()
                .unwrap(),
            base_locator
        );
        let reader = pile.reader().unwrap();
        assert_eq!(
            reader.blobs().collect::<Result<Vec<_>, _>>().unwrap(),
            vec![handle]
        );
        assert_eq!(
            reader
                .get::<Blob<UnknownBlob>, UnknownBlob>(handle)
                .unwrap()
                .bytes
                .as_ref(),
            payload
        );
        drop(reader);
        pile.close().unwrap();
    }

    #[test]
    fn indexed_corrupt_base_yields_to_first_valid_tail_and_last_invalid_tail() {
        fn run(all_invalid: bool) {
            let dir = tempfile::tempdir().unwrap();
            let path = fresh_empty_pile_path(&dir, "corrupt-base.pile");
            let index_path = dir.path().join("corrupt-base.pidx");
            let payload = b"target";
            let handle =
                Blob::<UnknownBlob>::new(Bytes::from_source(payload.to_vec())).get_handle();
            append_raw(&path, &v3_blob_record(handle, b"base!!", 1));
            MappedPileIndex::build(&path, &index_path).unwrap();

            append_raw(&path, &v3_blob_record(handle, b"tail-a", 2));
            if all_invalid {
                append_raw(&path, &v3_blob_record(handle, b"tail-b", 3));
            } else {
                append_raw(&path, &v3_blob_record(handle, payload, 3));
                append_raw(&path, &v3_blob_record(handle, payload, 4));
            }

            let mut pile = Pile::open_indexed(&path, &index_path).unwrap();
            pile.refresh().unwrap();
            assert_eq!(overlay_count(&pile.blobs), 1);
            assert_eq!(overlay_count(pile.blob_additions.as_ref().unwrap()), 0);
            let locator = pile
                .patch_index()
                .unwrap()
                .blob_locator(handle)
                .unwrap()
                .unwrap();
            assert_eq!(locator.timestamp, 3);
            let reader = pile.reader().unwrap();
            assert_eq!(
                reader.blobs().collect::<Result<Vec<_>, _>>().unwrap(),
                vec![handle]
            );
            if all_invalid {
                assert!(matches!(
                    reader.get::<Blob<UnknownBlob>, UnknownBlob>(handle),
                    Err(GetBlobError::ValidationError(_))
                ));
            } else {
                assert_eq!(
                    reader
                        .get::<Blob<UnknownBlob>, UnknownBlob>(handle)
                        .unwrap()
                        .bytes
                        .as_ref(),
                    payload
                );
            }
            drop(reader);
            pile.close().unwrap();
        }

        run(false);
        run(true);
    }

    #[test]
    fn indexed_tail_pin_and_weak_tombstones_never_resurrect_base() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pin-tail.pile");
        let index_path = dir.path().join("pin-tail.pidx");
        let mut base = Pile::open(&path).unwrap();
        let blob: Inline<Handle<UnknownBlob>> = base
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                b"pin".to_vec(),
            )))
            .unwrap();
        let branch = Id::new([44; 16]).unwrap();
        let head_a: Inline<Handle<SimpleArchive>> = blob.transmute();
        assert!(matches!(
            base.update(branch, None, Some(head_a)).unwrap(),
            PushResult::Success()
        ));
        base.pin_weak(blob).unwrap();
        base.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut pile = Pile::open_indexed(&path, &index_path).unwrap();
        assert!(matches!(
            pile.update(branch, Some(head_a), None).unwrap(),
            PushResult::Success()
        ));
        pile.unpin_weak(blob).unwrap();
        assert_eq!(pile.head(branch).unwrap(), None);
        assert!(pile.pins().unwrap().next().is_none());
        assert!(pile.weak_pins().unwrap().next().is_none());

        // The two LWW cell families are independent across the boundary.
        pile.pin_weak(blob).unwrap();
        assert_eq!(pile.head(branch).unwrap(), None);
        assert_eq!(pile.weak_pins().unwrap().next().unwrap().unwrap(), blob);

        let head_b = Inline::<Handle<SimpleArchive>>::new([9; 32]);
        assert!(matches!(
            pile.update(branch, None, Some(head_b)).unwrap(),
            PushResult::Success()
        ));
        pile.unpin_weak(blob).unwrap();
        assert_eq!(pile.head(branch).unwrap(), Some(head_b));
        assert!(pile.weak_pins().unwrap().next().is_none());
        pile.close().unwrap();

        let mut reopened = Pile::open_indexed(&path, &index_path).unwrap();
        assert_eq!(reopened.head(branch).unwrap(), Some(head_b));
        assert_eq!(
            reopened
                .pins()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![branch]
        );
        assert!(reopened.weak_pins().unwrap().next().is_none());
        reopened.close().unwrap();
    }

    #[test]
    fn indexed_open_falls_back_only_before_tail_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "fallback.pile");
        let index_path = dir.path().join("fallback.pidx");
        let mut pile = Pile::open(&path).unwrap();
        let handle: Inline<Handle<UnknownBlob>> = pile
            .put(Blob::<UnknownBlob>::new(Bytes::from_source(
                b"base".to_vec(),
            )))
            .unwrap();
        pile.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut index = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&index_path)
            .unwrap();
        index.seek(std::io::SeekFrom::Start(256)).unwrap();
        index.write_all(&[0xAA]).unwrap();
        index.sync_all().unwrap();
        assert!(matches!(
            Pile::open_indexed(&path, &index_path),
            Err(IndexedOpenError::Index(PileIndexError::InvalidIndex(_)))
        ));
        let mut fallback = Pile::open_indexed_or_replay(&path, &index_path).unwrap();
        assert!(fallback.base.is_none());
        assert!(fallback
            .reader()
            .unwrap()
            .get::<Blob<UnknownBlob>, UnknownBlob>(handle)
            .is_ok());
        fallback.close().unwrap();

        let missing = dir.path().join("missing.pidx");
        let missing_fallback = Pile::open_indexed_or_replay(&path, &missing).unwrap();
        assert!(missing_fallback.base.is_none());
        missing_fallback.close().unwrap();
    }

    #[test]
    fn indexed_truncated_tail_reports_absolute_boundary_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "truncated-tail.pile");
        let index_path = dir.path().join("truncated-tail.pidx");
        let mut pile = Pile::open(&path).unwrap();
        pile.put::<UnknownBlob, _>(Bytes::from_source(b"base".to_vec()))
            .unwrap();
        pile.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();
        let covered = std::fs::metadata(&path).unwrap().len() as usize;

        let tail_blob = Blob::<UnknownBlob>::new(Bytes::from_source(b"tail".to_vec()));
        let tail_handle = tail_blob.get_handle();
        let tail = v3_blob_record(tail_handle, b"tail", 7);
        append_raw(&path, &tail[..80]);
        let mut reopened = Pile::open_indexed(&path, &index_path).unwrap();
        assert!(matches!(
            reopened.refresh(),
            Err(ReadError::CorruptPile { valid_length })
                if valid_length == covered
        ));
        assert_eq!(reopened.applied_length, covered);

        append_raw(&path, &tail[80..]);
        reopened.refresh().unwrap();
        assert!(reopened
            .reader()
            .unwrap()
            .get::<Blob<UnknownBlob>, UnknownBlob>(tail_handle)
            .is_ok());

        let second_blob = Blob::<UnknownBlob>::new(Bytes::from_source(b"second".to_vec()));
        let second_handle = second_blob.get_handle();
        let second_record = v3_blob_record(second_handle, b"second", 8);
        let repaired_len = reopened.applied_length + second_record.len();
        append_raw(&path, &second_record);
        append_raw(&path, &[0xAA; 31]);
        assert!(matches!(
            reopened.refresh(),
            Err(ReadError::CorruptPile { valid_length }) if valid_length == repaired_len
        ));
        assert_eq!(reopened.applied_length, repaired_len);
        reopened.amputate().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len() as usize,
            repaired_len
        );
        assert!(reopened.base.is_some());
        assert!(reopened
            .reader()
            .unwrap()
            .get::<Blob<UnknownBlob>, UnknownBlob>(second_handle)
            .is_ok());
        reopened.close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn indexed_pile_retains_original_descriptor_across_path_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "retained.pile");
        let moved = dir.path().join("moved.pile");
        let index_path = dir.path().join("retained.pidx");
        let mut base = Pile::open(&path).unwrap();
        base.put::<UnknownBlob, _>(Bytes::from_source(b"base".to_vec()))
            .unwrap();
        base.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut retained = Pile::open_indexed(&path, &index_path).unwrap();
        std::fs::rename(&path, &moved).unwrap();
        File::create(&path).unwrap();
        let tail: Inline<Handle<UnknownBlob>> = retained
            .put(Bytes::from_source(b"old-inode-tail".to_vec()))
            .unwrap();
        retained.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert!(std::fs::metadata(&moved).unwrap().len() > 0);
        assert!(matches!(
            Pile::open_indexed(&path, &index_path),
            Err(IndexedOpenError::Index(PileIndexError::StaleIndex(_)))
        ));
        let mut moved_pile = Pile::open_indexed(&moved, &index_path).unwrap();
        assert!(moved_pile
            .reader()
            .unwrap()
            .get::<Blob<UnknownBlob>, UnknownBlob>(tail)
            .is_ok());
        moved_pile.close().unwrap();
    }

    #[test]
    fn indexed_cold_private_state_is_tail_sized() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "shape.pile");
        let index_path = dir.path().join("shape.pidx");
        let mut base = Pile::open(&path).unwrap();
        let mut handles = Vec::new();
        for byte in 0..128u8 {
            let handle = base
                .put::<UnknownBlob, _>(Bytes::from_source(vec![byte; 16]))
                .unwrap();
            handles.push(handle);
            base.pin_weak(handle).unwrap();
            let branch = Id::new([byte.wrapping_add(1); 16]).unwrap();
            let head: Inline<Handle<SimpleArchive>> = handle.transmute();
            assert!(matches!(
                base.update(branch, None, Some(head)).unwrap(),
                PushResult::Success()
            ));
        }
        base.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut pile = Pile::open_indexed(&path, &index_path).unwrap();
        assert_eq!(overlay_count(&pile.blobs), 0);
        assert_eq!(overlay_count(pile.blob_additions.as_ref().unwrap()), 0);
        assert_eq!(overlay_count(&pile.branches), 0);
        assert_eq!(overlay_count(pile.branch_tombstones.as_ref().unwrap()), 0);
        assert_eq!(overlay_count(&pile.weak_pins), 0);
        assert_eq!(overlay_count(pile.weak_unpins.as_ref().unwrap()), 0);
        assert_eq!(
            pile.base
                .as_ref()
                .unwrap()
                .validations
                .lock()
                .unwrap()
                .len(),
            0
        );
        assert!(pile.logical_weak_pin(handles[0]).unwrap());
        assert!(pile
            .logical_head(Id::new([1; 16]).unwrap())
            .unwrap()
            .is_some());
        assert_eq!(pile.reader().unwrap().blobs().count(), 128);
        pile.close().unwrap();
    }

    #[test]
    fn indexed_reader_equality_and_diff_are_representation_independent() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "reader-equality.pile");
        let index_path = dir.path().join("reader-equality.pidx");
        let mut writer = Pile::open(&path).unwrap();
        for byte in 1..=8u8 {
            writer
                .put::<UnknownBlob, _>(Bytes::from_source(vec![byte; 8]))
                .unwrap();
        }
        writer.close().unwrap();
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut first_pile = Pile::open_indexed(&path, &index_path).unwrap();
        let first = first_pile.reader().unwrap();
        let mut second_pile = Pile::open_indexed(&path, &index_path).unwrap();
        let second = second_pile.reader().unwrap();
        let mut full_pile = Pile::open(&path).unwrap();
        let full = full_pile.reader().unwrap();

        assert_eq!(first, second);
        assert_eq!(first, full);
        assert!(second.blobs_diff(&first).next().is_none());
        assert!(first.blobs_diff(&full).next().is_none());
        assert!(full.blobs_diff(&first).next().is_none());

        drop((first, second, full));
        first_pile.close().unwrap();
        second_pile.close().unwrap();
        full_pile.close().unwrap();
    }

    #[test]
    fn indexed_reader_diff_reports_additions_but_not_candidate_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "reader-diff.pile");
        let index_path = dir.path().join("reader-diff.pidx");
        let payload = b"target";
        let target = Blob::<UnknownBlob>::new(Bytes::from_source(payload.to_vec())).get_handle();
        append_raw(&path, &v3_blob_record(target, b"broken", 1));
        MappedPileIndex::build(&path, &index_path).unwrap();

        let mut pile = Pile::open_indexed(&path, &index_path).unwrap();
        let baseline = pile.reader().unwrap();
        append_raw(&path, &v3_blob_record(target, payload, 2));
        pile.refresh().unwrap();
        let override_only = pile.reader().unwrap();
        assert_eq!(baseline, override_only);
        assert!(override_only.blobs_diff(&baseline).next().is_none());
        assert!(matches!(
            baseline.get::<Blob<UnknownBlob>, UnknownBlob>(target),
            Err(GetBlobError::ValidationError(_))
        ));
        assert!(override_only
            .get::<Blob<UnknownBlob>, UnknownBlob>(target)
            .is_ok());

        let added_blob = Blob::<UnknownBlob>::new(Bytes::from_source(b"added".to_vec()));
        let added = added_blob.get_handle();
        append_raw(&path, &v3_blob_record(added, b"added", 3));
        pile.refresh().unwrap();
        let current = pile.reader().unwrap();
        assert_eq!(
            current
                .blobs_diff(&override_only)
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![added]
        );

        drop((baseline, override_only, current));
        pile.close().unwrap();
    }

    #[test]
    fn indexed_prefix_tail_composition_matches_full_replay_at_every_boundary() {
        let alpha_bytes = b"alpha";
        let bravo_bytes = b"bravo";
        let alpha = Blob::<UnknownBlob>::new(Bytes::from_source(alpha_bytes.to_vec())).get_handle();
        let bravo = Blob::<UnknownBlob>::new(Bytes::from_source(bravo_bytes.to_vec())).get_handle();
        let branch = Id::new([73; 16]).unwrap();
        let alpha_head: Inline<Handle<SimpleArchive>> = alpha.transmute();
        let bravo_head: Inline<Handle<SimpleArchive>> = bravo.transmute();
        let records = vec![
            v3_blob_record(alpha, b"ALPHA", 1),
            v3_blob_record(bravo, bravo_bytes, 2),
            v3_branch_record(branch, Some(alpha_head)),
            v3_weak_record(alpha, true),
            v3_blob_record(alpha, alpha_bytes, 5),
            v3_blob_record(alpha, alpha_bytes, 6),
            v3_blob_record(bravo, b"BRAVO", 7),
            v3_branch_record(branch, None),
            v3_weak_record(alpha, false),
            v3_branch_record(branch, Some(bravo_head)),
            v3_weak_record(alpha, true),
        ];

        for split in 0..=records.len() {
            let dir = tempfile::tempdir().unwrap();
            let path = fresh_empty_pile_path(&dir, "differential.pile");
            let index_path = dir.path().join("differential.pidx");
            for record in &records[..split] {
                append_raw(&path, record);
            }
            MappedPileIndex::build(&path, &index_path).unwrap();
            for record in &records[split..] {
                append_raw(&path, record);
            }

            let mut indexed_pile = Pile::open_indexed(&path, &index_path).unwrap();
            let indexed = indexed_pile.patch_index().unwrap();
            indexed_pile.close().unwrap();
            let mut full_pile = Pile::open(&path).unwrap();
            let full = full_pile.patch_index().unwrap();
            full_pile.close().unwrap();

            assert_eq!(indexed.covered_len(), full.covered_len(), "split {split}");
            let indexed_handles = indexed
                .blob_handles()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let full_handles = full.blob_handles().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(indexed_handles, full_handles, "split {split}");
            for handle in indexed_handles {
                assert_eq!(
                    indexed.blob_locator(handle).unwrap(),
                    full.blob_locator(handle).unwrap(),
                    "split {split}, handle {handle:?}"
                );
            }
            assert_eq!(
                indexed.pin_ids().collect::<Result<Vec<_>, _>>().unwrap(),
                full.pin_ids().collect::<Result<Vec<_>, _>>().unwrap(),
                "split {split}"
            );
            assert_eq!(
                indexed.pin_head(branch).unwrap(),
                full.pin_head(branch).unwrap(),
                "split {split}"
            );
            assert_eq!(
                indexed
                    .weak_pin_handles()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                full.weak_pin_handles()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                "split {split}"
            );
        }
    }

    #[test]
    fn open() {
        const RECORD_LEN: usize = 1 << 10; // 1k
        const RECORD_COUNT: usize = 1 << 12; // 4k

        let mut rng = rand::thread_rng();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_pile = fresh_empty_pile_path(&tmp_dir, "test.pile");
        let mut pile: Pile = Pile::open(&tmp_pile).unwrap();

        (0..RECORD_COUNT).for_each(|_| {
            let mut record = Vec::with_capacity(RECORD_LEN);
            rng.fill_bytes(&mut record);

            let data: Blob<UnknownBlob> = Blob::new(Bytes::from_source(record));
            pile.put::<UnknownBlob, _>(data).unwrap();
        });

        pile.close().unwrap();

        let mut reopened: Pile = Pile::open(&tmp_pile).unwrap();
        reopened.amputate().unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn put_v3_256_aligned_roundtrip() {
        // Every V3 record is a 256-byte multiple with the data at a fixed
        // header offset, so plain `put` yields absolutely 256-aligned
        // (GPU-aliasable) data in a pure-V3 pile.
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "v3.pile");
        // Sizes around the 64/256 boundaries to exercise the post-pad.
        let sizes = [1usize, 7, 33, 64, 100, 192, 255, 256, 257, 1000, 4096];
        let mut hashes = Vec::new();
        let mut datas: Vec<Vec<u8>> = Vec::new();
        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            for &sz in &sizes {
                let data: Vec<u8> = (0..sz).map(|i| (i % 251) as u8).collect();
                let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
                let h = pile.put::<UnknownBlob, _>(blob).unwrap();
                let hash: Inline<Hash<Blake3>> = h.into();
                hashes.push(hash);
                datas.push(data);
            }
            pile.close().unwrap();
        }
        // Reopen fresh — the scan rebuilds the index from the on-disk V3 records.
        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        for (hash, expected) in hashes.iter().zip(&datas) {
            let entry = pile
                .blobs
                .get(&hash.raw)
                .expect("V3 blob missing after reopen")
                .clone();
            let IndexEntry { offset, len, .. } = entry;
            assert_eq!(
                offset % GPU_DATA_ALIGNMENT,
                0,
                "V3 data offset {offset} not {GPU_DATA_ALIGNMENT}-aligned (size {})",
                expected.len()
            );
            let got =
                unsafe { std::slice::from_raw_parts(pile.mmap.as_ptr().add(offset), len as usize) };
            assert_eq!(
                got,
                &expected[..],
                "V3 roundtrip mismatch (size {})",
                expected.len()
            );
        }
        pile.close().unwrap();
    }

    /// The whole point of uniform-V3: `cat a.pile >> b.pile` is a valid merge —
    /// every record from both piles is found and byte-correct, the data stays
    /// 256-aligned, and `amputate()` does not truncate the concatenation as
    /// corrupt. This is what an offset-derived pad could never survive.
    #[test]
    fn v3_cat_merge_preserves_all_blobs_and_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = fresh_empty_pile_path(&dir, "a.pile");
        let path_b = fresh_empty_pile_path(&dir, "b.pile");
        let sizes = [1usize, 33, 100, 256, 257, 1000, 4096];
        let mut handles: Vec<(Inline<Hash<Blake3>>, Vec<u8>)> = Vec::new();

        {
            let mut a: Pile = Pile::open(&path_a).unwrap();
            for (k, &sz) in sizes.iter().enumerate() {
                let data: Vec<u8> = (0..sz).map(|i| ((i + k) % 251) as u8).collect();
                let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
                let h: Inline<Hash<Blake3>> = a.put::<UnknownBlob, _>(blob).unwrap().into();
                handles.push((h, data));
            }
            a.close().unwrap();
        }
        {
            let mut b: Pile = Pile::open(&path_b).unwrap();
            for (k, &sz) in sizes.iter().enumerate() {
                // Distinct content so no hash collisions with pile A.
                let data: Vec<u8> = (0..sz).map(|i| ((i + k + 128) % 251) as u8).collect();
                let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
                let h: Inline<Hash<Blake3>> = b.put::<UnknownBlob, _>(blob).unwrap().into();
                handles.push((h, data));
            }
            b.close().unwrap();
        }

        // Each pure-V3 pile is a whole number of 256-byte units — the precondition
        // that makes the appended pile land on a 256-aligned offset.
        assert_eq!(
            std::fs::metadata(&path_a).unwrap().len() % V3_ALIGNMENT as u64,
            0
        );
        assert_eq!(
            std::fs::metadata(&path_b).unwrap().len() % V3_ALIGNMENT as u64,
            0
        );

        // cat a.pile >> b.pile
        {
            let a_bytes = std::fs::read(&path_a).unwrap();
            let mut bf = std::fs::OpenOptions::new()
                .append(true)
                .open(&path_b)
                .unwrap();
            bf.write_all(&a_bytes).unwrap();
            bf.sync_all().unwrap();
        }
        let merged_len = std::fs::metadata(&path_b).unwrap().len();

        let mut merged: Pile = Pile::open(&path_b).unwrap();
        merged.amputate().unwrap();
        assert_eq!(
            std::fs::metadata(&path_b).unwrap().len(),
            merged_len,
            "cat-merged pile was truncated — cat is not a valid V3 merge"
        );
        for (hash, expected) in &handles {
            let entry = merged
                .blobs
                .get(&hash.raw)
                .expect("blob lost after cat-merge")
                .clone();
            let IndexEntry { offset, len, .. } = entry;
            assert_eq!(
                offset % V3_ALIGNMENT,
                0,
                "post-cat data offset not 256-aligned"
            );
            let got = unsafe {
                std::slice::from_raw_parts(merged.mmap.as_ptr().add(offset), len as usize)
            };
            assert_eq!(got, &expected[..], "blob bytes wrong after cat-merge");
        }
        // Still 256-aligned, so it can be cat'd again.
        assert_eq!(
            std::fs::metadata(&path_b).unwrap().len() % V3_ALIGNMENT as u64,
            0
        );
        merged.close().unwrap();
    }

    /// Existing piles are V1; the V3-capable reader must read them unchanged.
    #[test]
    fn v3_reader_still_reads_legacy_v1_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "legacy_v1.pile");
        let data = vec![9u8; 40];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle: Inline<Handle<UnknownBlob>> = blob.get_handle();
        let hash: Inline<Hash<Blake3>> = handle.into();
        // Hand-write a legacy V1 blob record: 64-byte header + data + 64-pad.
        {
            let header = BlobHeader::new(42, data.len() as u64, hash);
            let pad = padding_for_blob(data.len());
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(header.as_bytes()).unwrap();
            f.write_all(&data).unwrap();
            f.write_all(&vec![0u8; pad]).unwrap();
            f.sync_all().unwrap();
        }
        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(
            fetched.bytes.as_ref(),
            data.as_slice(),
            "legacy V1 blob not read by the V3-capable reader"
        );
        pile.close().unwrap();
    }

    #[test]
    fn recover_shrink() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.close().unwrap();
        }

        // Corrupt by removing some bytes from the end
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - 10).unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn refresh_corrupt_reports_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.close().unwrap();
        }

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len - 10)
            .unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        match pile.refresh() {
            Err(ReadError::CorruptPile { valid_length }) => assert_eq!(valid_length, 0),
            other => panic!("unexpected result: {other:?}"),
        }
        pile.close().unwrap();
    }

    #[test]
    fn amputate_truncates_unknown_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
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

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[test]
    fn refresh_partial_header_reports_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.close().unwrap();
        }

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len + 8)
            .unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
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

        let mut pile: Pile = Pile::open(&path).unwrap();
        match pile.refresh() {
            Err(ReadError::CorruptPile { valid_length }) => assert_eq!(valid_length, 0),
            other => panic!("unexpected result: {other:?}"),
        }
        pile.close().unwrap();
    }

    #[test]
    fn amputate_truncates_length_beyond_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 20]));
            pile.put::<UnknownBlob, _>(blob).unwrap();
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

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        pile.close().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn put_and_get_preserves_blob_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![42u8; 100];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();

        {
            let reader = pile.reader().unwrap();
            let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
            assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        }

        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile.close().unwrap();
    }

    #[test]
    fn iter_lists_all_blobs_handles() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blobs = vec![vec![1u8; 3], vec![2u8; 4], vec![3u8; 5]];
        let mut expected = HashMap::new();
        for data in blobs {
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
            let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
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
    fn blobs_diff_returns_only_new_handles() {
        use crate::repo::BlobStoreList;
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();

        // Stage three baseline blobs and snapshot the reader.
        let mut baseline_handles: HashSet<Inline<Handle<UnknownBlob>>> = HashSet::new();
        for data in [vec![1u8; 3], vec![2u8; 4], vec![3u8; 5]] {
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data));
            let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
            baseline_handles.insert(handle);
        }
        let baseline = pile.reader().unwrap();

        // Stage two more blobs after taking the baseline snapshot.
        let mut new_handles: HashSet<Inline<Handle<UnknownBlob>>> = HashSet::new();
        for data in [vec![4u8; 6], vec![5u8; 7]] {
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data));
            let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
            new_handles.insert(handle);
        }

        // Diff the current reader against the baseline.
        let current = pile.reader().unwrap();
        let diffed: HashSet<Inline<Handle<UnknownBlob>>> = current
            .blobs_diff(&baseline)
            .map(|r| r.expect("infallible diff iter"))
            .collect();

        // Diff should equal exactly the new blobs — none of the baseline ones.
        assert_eq!(diffed, new_handles);
        for h in &baseline_handles {
            assert!(!diffed.contains(h), "baseline blob leaked into diff");
        }

        // Round-trip sanity: diffing a reader against itself yields nothing.
        let empty: HashSet<_> = current
            .blobs_diff(&current)
            .map(|r| r.expect("infallible"))
            .collect();
        assert!(empty.is_empty());

        pile.close().unwrap();
    }

    #[test]
    fn metadata_reflects_length_and_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let data = vec![9u8; 10];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let reader = pile.reader().unwrap();

        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();

        assert!(reader.metadata(handle).unwrap().is_none());

        pile.flush().unwrap();
        let reader = pile.reader().unwrap();
        assert!(reader.metadata(handle).unwrap().is_some());
        pile.close().unwrap();
    }

    #[test]
    fn blob_after_branch_is_clean() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();

        let branch_id = Id::new([1; 16]).unwrap();
        let head = Inline::<Handle<SimpleArchive>>::new([2; 32]);
        pile.update(branch_id, None, Some(head)).unwrap();

        let data = vec![3u8; 8];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
        pile.flush().unwrap();

        let stored: Blob<UnknownBlob> = pile.reader().unwrap().get(handle).unwrap();
        assert_eq!(stored.bytes.as_ref(), &data[..]);
        pile.close().unwrap();
    }

    #[test]
    fn insert_after_branch_preserves_head() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put::<UnknownBlob, _>(blob1).unwrap();

        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        pile.put::<UnknownBlob, _>(blob2).unwrap();
        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let head = pile.head(branch_id).unwrap();
        assert_eq!(head, Some(handle1.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_survives_manual_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let branch_id = Id::new([1u8; 16]).unwrap();

        let handle = {
            let mut pile: Pile = Pile::open(&path).unwrap();
            let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![3u8; 5]));
            let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
            pile.update(branch_id, None, Some(handle.transmute()))
                .unwrap();
            pile.flush().unwrap();
            std::mem::forget(pile);
            handle
        };

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        assert_eq!(pile.head(branch_id).unwrap(), Some(handle.transmute()));
        assert!(std::fs::metadata(&path).unwrap().len() > 0);
        pile.close().unwrap();
    }

    #[test]
    fn branch_tombstone_removes_head_and_listing() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let h = pile.put::<UnknownBlob, _>(blob).unwrap();
        let branch_id = Id::new([7u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h.transmute())).unwrap();
        pile.flush().unwrap();

        assert_eq!(pile.head(branch_id).unwrap(), Some(h.transmute()));

        pile.update(branch_id, Some(h.transmute()), None).unwrap();
        pile.flush().unwrap();

        assert_eq!(pile.head(branch_id).unwrap(), None);
        let branches: HashSet<Id> = pile.pins().unwrap().map(|r| r.unwrap()).collect();
        assert!(!branches.contains(&branch_id));
        pile.close().unwrap();
    }

    #[test]
    fn branch_update_detects_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put::<UnknownBlob, _>(blob1).unwrap();

        let branch_id = Id::new([2u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let handle2 = pile.put::<UnknownBlob, _>(blob2).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let handle1 = pile.put::<UnknownBlob, _>(blob1).unwrap();

        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(handle1.transmute()))
            .unwrap();
        pile.flush().unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let handle2 = pile.put::<UnknownBlob, _>(blob2).unwrap();

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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![7u8; 32]));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
        pile.close().unwrap();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let reader = pile.reader().unwrap();
        let meta = reader.metadata(handle).unwrap().expect("metadata");
        assert_eq!(meta.length, 32);
        assert!(meta.timestamp > 0);
        pile.close().unwrap();
    }

    #[test]
    fn iter_lists_all_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        let h1 = pile.put::<UnknownBlob, _>(blob1).unwrap();
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 4]));
        let h2 = pile.put::<UnknownBlob, _>(blob2).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 5]));
        let h1 = pile.put::<UnknownBlob, _>(blob1).unwrap();
        let branch_id = Id::new([1u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h1.transmute())).unwrap();
        pile.flush().unwrap();

        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 5]));
        let h2 = pile.put::<UnknownBlob, _>(blob2).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 4]));
        pile.put::<UnknownBlob, _>(blob).unwrap();
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
    fn amputate_truncates_corrupt_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![1u8; 4];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
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

        pile.amputate().unwrap();

        // Blobs are now written as V3 records (fixed 256-byte header, padded to a
        // 256-byte multiple).
        let expected_len =
            (super::V3_HEADER_LEN + data.len() + super::v3_post_pad(data.len())) as u64;
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expected_len);

        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile.close().unwrap();
    }

    #[test]
    fn refresh_replaces_corrupt_blob_with_new_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile1: Pile = Pile::open(&path).unwrap();
        let mut pile2: Pile = Pile::open(&path).unwrap();

        let data = vec![1u8; 4];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile1.put(blob).unwrap();
        pile1.flush().unwrap();
        pile1.refresh().unwrap();

        // Corrupt the first V3 blob's payload (the fixed header is 256 bytes).
        use std::io::Seek;
        use std::io::SeekFrom;
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(V3_HEADER_LEN as u64)).unwrap();
        file.write_all(&[9u8; 4]).unwrap();
        file.sync_all().unwrap();

        // Append a valid copy using the second pile which hasn't seen the first one.
        let blob_dup: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        pile2.put::<UnknownBlob, _>(blob_dup).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![9u8; 32];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle1 = pile.put::<UnknownBlob, _>(blob).unwrap();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![1u8; 8]));
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![2u8; 8]));
        let h1 = pile.put::<UnknownBlob, _>(blob1).unwrap();
        let h2 = pile.put::<UnknownBlob, _>(blob2).unwrap();
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
    fn branch_update_noop_does_not_grow_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(vec![7u8; 8]));
        let h = pile.put::<UnknownBlob, _>(blob).unwrap();
        pile.flush().unwrap();

        let branch_id = Id::new([4u8; 16]).unwrap();
        pile.update(branch_id, None, Some(h.transmute())).unwrap();
        pile.flush().unwrap();
        let len_after_first = std::fs::metadata(&path).unwrap().len();

        match pile.update(branch_id, Some(h.transmute()), Some(h.transmute())) {
            Ok(PushResult::Success()) => {}
            other => panic!("expected no-op success, got {other:?}"),
        }
        pile.flush().unwrap();
        let len_after_noop = std::fs::metadata(&path).unwrap().len();

        assert_eq!(
            len_after_first, len_after_noop,
            "no-op branch update must not append a new record"
        );
        assert_eq!(pile.head(branch_id).unwrap(), Some(h.transmute()));
        pile.close().unwrap();
    }

    #[test]
    fn iterator_skips_missing_index_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"hello".as_slice()));
        let blob2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"world".as_slice()));
        let handle1 = pile.put::<UnknownBlob, _>(blob1).unwrap();
        let handle2 = pile.put::<UnknownBlob, _>(blob2).unwrap();
        pile.flush().unwrap();

        let mut reader = pile.reader().unwrap();
        let _full_patch = reader.blobs.clone();
        let hash1: Inline<Hash<Blake3>> = handle1.into();
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
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let mut pile: Pile = Pile::open(&path).unwrap();
        let data = vec![7u8; 16];
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
        pile.flush().unwrap();

        let reader = pile.reader().unwrap();
        let meta = reader.metadata(handle).unwrap().expect("metadata");
        assert_eq!(meta.length, data.len() as u64);
        pile.close().unwrap();
    }

    /// Durable weak pins: a weak-pin record survives close + reopen — the
    /// scan rebuilds the LWW-resolved weak set from the on-disk markers.
    #[test]
    fn weak_pin_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        // A weak pin is a want — the blob need not exist in the pile.
        let wanted: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![7u8; 21])).get_handle();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.pin_weak(wanted).unwrap();
        let pinned: HashSet<_> = pile.weak_pins().unwrap().map(|r| r.unwrap()).collect();
        assert!(pinned.contains(&wanted));
        pile.close().unwrap();

        let mut reopened: Pile = Pile::open(&path).unwrap();
        reopened.amputate().unwrap();
        let pinned: HashSet<_> = reopened.weak_pins().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(pinned.len(), 1);
        assert!(
            pinned.contains(&wanted),
            "weak pin lost across reopen — restart amnesia"
        );
        reopened.close().unwrap();
    }

    /// LWW by log position: the last marker for a handle wins, both live and
    /// across a fresh scan of the on-disk record sequence.
    #[test]
    fn weak_pin_lww_last_writer_wins() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let a: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![1u8; 9])).get_handle();
        let b: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![2u8; 9])).get_handle();

        let mut pile: Pile = Pile::open(&path).unwrap();
        // a: pin, unpin, pin — three real records; last writer says pinned.
        pile.pin_weak(a).unwrap();
        pile.unpin_weak(a).unwrap();
        pile.pin_weak(a).unwrap();
        // b: pin then unpin — last writer says unpinned.
        pile.pin_weak(b).unwrap();
        pile.unpin_weak(b).unwrap();

        let pinned: HashSet<_> = pile.weak_pins().unwrap().map(|r| r.unwrap()).collect();
        assert!(pinned.contains(&a));
        assert!(!pinned.contains(&b));
        pile.close().unwrap();

        // The same resolution must fall out of a fresh log replay.
        let mut reopened: Pile = Pile::open(&path).unwrap();
        reopened.amputate().unwrap();
        let pinned: HashSet<_> = reopened.weak_pins().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(pinned.len(), 1);
        assert!(pinned.contains(&a));
        assert!(!pinned.contains(&b));
        reopened.close().unwrap();
    }

    /// Re-asserting the current weak state is a no-op append (mirrors the
    /// branch-update no-op rule): the LWW cell carries no new information.
    #[test]
    fn weak_pin_noop_does_not_grow_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        let h: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![3u8; 5])).get_handle();

        let mut pile: Pile = Pile::open(&path).unwrap();
        // Unpinning a never-pinned handle records nothing.
        pile.unpin_weak(h).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        pile.pin_weak(h).unwrap();
        let len_after_pin = std::fs::metadata(&path).unwrap().len();
        assert_eq!(len_after_pin, V3_HEADER_LEN as u64);

        pile.pin_weak(h).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_after_pin);
        pile.close().unwrap();
    }

    /// Mixed pile: a legacy V1 blob, V3 blobs, branch records, and weak
    /// markers interleaved — the scan walks every record kind cleanly and
    /// each index (blobs, branches, weak pins) resolves correctly.
    #[test]
    fn mixed_v1_v3_branch_and_weak_markers_interleave() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "mixed.pile");

        // Hand-write a legacy V1 blob record first (64-byte header + pad).
        let v1_data = vec![9u8; 40];
        let v1_blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(v1_data.clone()));
        let v1_handle: Inline<Handle<UnknownBlob>> = v1_blob.get_handle();
        {
            let v1_hash: Inline<Hash<Blake3>> = v1_handle.into();
            let header = BlobHeader::new(42, v1_data.len() as u64, v1_hash);
            let pad = padding_for_blob(v1_data.len());
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(header.as_bytes()).unwrap();
            f.write_all(&v1_data).unwrap();
            f.write_all(&vec![0u8; pad]).unwrap();
            f.sync_all().unwrap();
        }

        let branch_id = Id::new([5u8; 16]).unwrap();
        let want: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![11u8; 13])).get_handle();
        let retracted: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![12u8; 13])).get_handle();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();

        // Interleave: weak-pin, V3 blob, branch head, weak-pin + weak-unpin,
        // another V3 blob.
        pile.pin_weak(want).unwrap();
        let d1 = vec![1u8; 300];
        let b1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(d1.clone()));
        let h1 = pile.put::<UnknownBlob, _>(b1).unwrap();
        pile.update(branch_id, None, Some(h1.transmute())).unwrap();
        pile.pin_weak(retracted).unwrap();
        pile.unpin_weak(retracted).unwrap();
        let d2 = vec![2u8; 77];
        let b2: Blob<UnknownBlob> = Blob::new(Bytes::from_source(d2.clone()));
        let h2 = pile.put::<UnknownBlob, _>(b2).unwrap();
        pile.close().unwrap();

        // Fresh scan must walk the whole interleaved sequence.
        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();

        let reader = pile.reader().unwrap();
        let got_v1: Blob<UnknownBlob> = reader.get(v1_handle).unwrap();
        assert_eq!(got_v1.bytes.as_ref(), v1_data.as_slice());
        let got1: Blob<UnknownBlob> = reader.get(h1).unwrap();
        assert_eq!(got1.bytes.as_ref(), d1.as_slice());
        let got2: Blob<UnknownBlob> = reader.get(h2).unwrap();
        assert_eq!(got2.bytes.as_ref(), d2.as_slice());
        drop(reader);

        assert_eq!(pile.head(branch_id).unwrap(), Some(h1.transmute()));

        let pinned: HashSet<_> = pile.weak_pins().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(pinned.len(), 1);
        assert!(pinned.contains(&want));
        assert!(!pinned.contains(&retracted));
        pile.close().unwrap();
    }

    /// [`PileRecords`] walks a mixed V1/V3 pile record-by-record: every
    /// record kind appears in log order, offsets tile the file exactly, blob
    /// payloads are addressable through `data_offset`/`data_len`, and a
    /// garbage tail surfaces as `Err(CorruptPile)` — never a silent stop.
    #[test]
    fn pile_records_walks_mixed_pile_and_fails_loud() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "records.pile");

        // Hand-write a legacy V1 blob record first (64-byte header + pad).
        let v1_data = vec![9u8; 40];
        let v1_blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(v1_data.clone()));
        let v1_handle: Inline<Handle<UnknownBlob>> = v1_blob.get_handle();
        {
            let v1_hash: Inline<Hash<Blake3>> = v1_handle.into();
            let header = BlobHeader::new(42, v1_data.len() as u64, v1_hash);
            let pad = padding_for_blob(v1_data.len());
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(header.as_bytes()).unwrap();
            f.write_all(&v1_data).unwrap();
            f.write_all(&vec![0u8; pad]).unwrap();
            f.sync_all().unwrap();
        }

        let branch_id = Id::new([5u8; 16]).unwrap();
        let want: Inline<Handle<UnknownBlob>> =
            Blob::<UnknownBlob>::new(Bytes::from_source(vec![11u8; 13])).get_handle();

        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let d1 = vec![1u8; 300];
        let b1: Blob<UnknownBlob> = Blob::new(Bytes::from_source(d1.clone()));
        let h1 = pile.put::<UnknownBlob, _>(b1).unwrap();
        pile.update(branch_id, None, Some(h1.transmute())).unwrap();
        pile.pin_weak(want).unwrap();
        pile.unpin_weak(want).unwrap();
        pile.update(branch_id, Some(h1.transmute()), None).unwrap();
        pile.close().unwrap();

        let mut records = PileRecords::open(&path).unwrap();
        let bytes = records.bytes().clone();
        let decoded: Vec<PileRecord> = (&mut records)
            .map(|r| r.expect("well-formed pile decodes cleanly"))
            .collect();

        // Records tile the file: each starts where the previous ended.
        let mut expected_offset = 0;
        for record in &decoded {
            assert_eq!(record.offset, expected_offset);
            expected_offset += record.len;
        }
        assert_eq!(expected_offset, bytes.len());

        // Exact sequence: V1 blob, V3 blob, branch set, weak-pin,
        // weak-unpin, branch tombstone.
        assert_eq!(decoded.len(), 6);
        match decoded[0].content {
            PileRecordContent::Blob {
                timestamp,
                hash,
                data_offset,
                data_len,
            } => {
                assert_eq!(timestamp, 42);
                assert_eq!(hash, v1_handle.into());
                assert_eq!(data_offset, BLOB_HEADER_LEN);
                assert_eq!(&bytes[data_offset..data_offset + data_len], &v1_data[..]);
            }
            other => panic!("expected V1 blob record, got {other:?}"),
        }
        match decoded[1].content {
            PileRecordContent::Blob {
                hash,
                data_offset,
                data_len,
                ..
            } => {
                assert_eq!(hash, h1.into());
                assert_eq!(data_offset, decoded[1].offset + V3_HEADER_LEN);
                assert_eq!(&bytes[data_offset..data_offset + data_len], &d1[..]);
            }
            other => panic!("expected V3 blob record, got {other:?}"),
        }
        match decoded[2].content {
            PileRecordContent::Branch {
                branch_id: bid,
                head,
            } => {
                assert_eq!(bid, branch_id);
                assert_eq!(head, h1.transmute());
            }
            other => panic!("expected branch record, got {other:?}"),
        }
        match decoded[3].content {
            PileRecordContent::WeakPin { handle } => assert_eq!(handle, want),
            other => panic!("expected weak-pin record, got {other:?}"),
        }
        match decoded[4].content {
            PileRecordContent::WeakUnpin { handle } => assert_eq!(handle, want),
            other => panic!("expected weak-unpin record, got {other:?}"),
        }
        match decoded[5].content {
            PileRecordContent::BranchTombstone { branch_id: bid } => {
                assert_eq!(bid, branch_id)
            }
            other => panic!("expected branch tombstone record, got {other:?}"),
        }

        // A garbage tail is an error at its offset, then the iterator ends.
        let garbage_offset = std::fs::metadata(&path).unwrap().len() as usize;
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0xFFu8; 32]).unwrap();
            f.sync_all().unwrap();
        }
        let mut records = PileRecords::open(&path).unwrap();
        let mut ok = 0;
        let err = loop {
            match records.next() {
                Some(Ok(_)) => ok += 1,
                Some(Err(e)) => break e,
                None => panic!("iterator ended without reporting the corrupt tail"),
            }
        };
        assert_eq!(ok, 6);
        match err {
            ReadError::CorruptPile { valid_length } => assert_eq!(valid_length, garbage_offset),
            other => panic!("expected CorruptPile, got {other:?}"),
        }
        assert!(records.next().is_none(), "iterator must end after an error");
    }

    // recover_grow test removed as growth strategy no longer exists

    /// Exercise the `ATOMIC_WRITE_LIMIT` fallback: an oversized blob must
    /// still round-trip correctly through the exclusive-lock multi-write
    /// path. Marked `#[ignore]` because the test allocates ~1 GiB and
    /// writes ~2 GiB to disk; run explicitly with
    /// `cargo test --release -- --ignored put_and_get_oversized_blob`.
    #[test]
    #[ignore]
    fn put_and_get_oversized_blob() {
        let dir = tempfile::tempdir().unwrap();
        let path = fresh_empty_pile_path(&dir, "pile.pile");

        // Slightly over the threshold so we land in the non-atomic branch.
        let size = ATOMIC_WRITE_LIMIT + 1_024;
        let mut data = vec![0u8; size];
        // Sprinkle some non-trivial pattern so `Bytes` equality has teeth.
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(13).wrapping_add(7);
        }

        let mut pile: Pile = Pile::open(&path).unwrap();
        let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
        let handle = pile.put::<UnknownBlob, _>(blob).unwrap();

        {
            let reader = pile.reader().unwrap();
            let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
            assert_eq!(fetched.bytes.len(), size);
            assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        }

        pile.close().unwrap();

        // Round-trip across open+amputate to ensure the on-disk record
        // is fully self-describing and recoverable.
        let mut pile: Pile = Pile::open(&path).unwrap();
        pile.amputate().unwrap();
        let reader = pile.reader().unwrap();
        let fetched: Blob<UnknownBlob> = reader.get(handle).unwrap();
        assert_eq!(fetched.bytes.as_ref(), data.as_slice());
        pile.close().unwrap();
    }
}
