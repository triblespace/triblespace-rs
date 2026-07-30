use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::Encodes;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::patch::ArchiveEntry;
use crate::patch::ArchiveLeafDescriptor;
use crate::patch::ArchiveOwner;
use crate::trible::Fragment;
use crate::trible::Trible;
use crate::trible::TribleSet;

use anybytes::Bytes;
use anybytes::View;
use std::ptr::NonNull;
use std::sync::Arc;

/// Canonical trible sequence stored as raw 64-byte entries.
///
/// The simplest portable archive format — a flat byte array of tribles
/// in canonical EAV order with no compression. Used for commits,
/// streaming, hashing, and audit trails where byte-for-byte stability
/// matters.
pub struct SimpleArchive;

impl BlobEncoding for SimpleArchive {}

impl MetaDescribe for SimpleArchive {
    fn describe() -> Fragment {
        let id: Id = id_hex!("8F4A27C8581DADCBA1ADA8BA228069B6");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "simplearchive",
                metadata::description: "Canonical trible sequence stored as raw 64-byte entries. This is the simplest portable archive format and preserves the exact trible ordering expected by the canonicalization rules.\n\nUse SimpleArchive for export, import, streaming, hashing, or audit trails where you want a byte-for-byte stable representation. Prefer SuccinctArchiveBlob when you need compact indexed storage and fast offline queries, and keep a SimpleArchive around if you want a source of truth that can be re-indexed or validated.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl Encodes<TribleSet> for SimpleArchive
where
    crate::inline::encodings::hash::Handle<SimpleArchive>: crate::inline::InlineEncoding,
{
    type Output = Blob<SimpleArchive>;
    fn encode(source: TribleSet) -> Blob<SimpleArchive> {
        let mut tribles: Vec<[u8; 64]> = Vec::with_capacity(source.len());
        tribles.extend(source.eav.iter_ordered());
        let bytes: Bytes = tribles.into();
        Blob::new(bytes)
    }
}

impl Encodes<&TribleSet> for SimpleArchive
where
    crate::inline::encodings::hash::Handle<SimpleArchive>: crate::inline::InlineEncoding,
{
    type Output = Blob<SimpleArchive>;
    fn encode(source: &TribleSet) -> Blob<SimpleArchive> {
        let mut tribles: Vec<[u8; 64]> = Vec::with_capacity(source.len());
        tribles.extend(source.eav.iter_ordered());
        let bytes: Bytes = tribles.into();
        Blob::new(bytes)
    }
}

/// Error returned when deserializing a [`SimpleArchive`] blob into a [`TribleSet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnarchiveError {
    /// The blob length is not a multiple of 64 bytes.
    BadArchive,
    /// A 64-byte entry has a nil entity or attribute.
    BadTrible,
    /// The archive contains duplicate tribles.
    BadCanonicalizationRedundancy,
    /// The tribles are not in ascending canonical order.
    BadCanonicalizationOrdering,
}

impl std::fmt::Display for UnarchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnarchiveError::BadArchive => write!(f, "The archive is malformed or invalid."),
            UnarchiveError::BadTrible => write!(f, "A trible in the archive is malformed."),
            UnarchiveError::BadCanonicalizationRedundancy => {
                write!(f, "The archive contains redundant tribles.")
            }
            UnarchiveError::BadCanonicalizationOrdering => {
                write!(f, "The tribles in the archive are not in canonical order.")
            }
        }
    }
}

impl std::error::Error for UnarchiveError {}

/// Below this many tribles, serial unarchive wins (rayon overhead
/// dominates).
#[cfg(feature = "parallel")]
const PARALLEL_UNARCHIVE_THRESHOLD: usize = 4096;

/// One process-local owner for both halves of descriptor-backed archive
/// leaves. `_bytes` keeps every raw key pointer valid; `descriptors` keeps the
/// tagged Head bodies valid. Neither the descriptor slab nor its SIP_KEY
/// fingerprints are part of the portable SimpleArchive encoding.
struct SimpleArchiveLeafOwner {
    _bytes: Bytes,
    descriptors: Box<[ArchiveLeafDescriptor<64>]>,
}

/// Decoder-time access to a composite owner through both its concrete type
/// (for descriptor indexing) and its erased type (for PATCH owner covers).
/// Both Arcs point at the same allocation.
struct SimpleArchiveLeafBacking {
    composite: Arc<SimpleArchiveLeafOwner>,
    owner: Arc<dyn ArchiveOwner>,
}

#[derive(Copy, Clone)]
struct SimpleArchiveLeafSlice<'a> {
    owner: &'a Arc<dyn ArchiveOwner>,
    descriptors: &'a [ArchiveLeafDescriptor<64>],
}

impl SimpleArchiveLeafBacking {
    fn new(bytes: &Bytes, rows: &[[u8; 64]]) -> Self {
        // Prototype tradeoff: descriptor construction happens before
        // `serial_unarchive` validates even the first row. Malformed archives
        // therefore pay the full slab allocation and hashing pass before
        // failing. Keeping descriptor publication immutable makes this path
        // small and auditable, but it deliberately gives up the former fused
        // fail-fast validation/insertion behavior; measurements must include
        // this extra pass rather than describing it as free.
        // Build all process-local descriptors before publishing any pointers
        // into their slab. `into_boxed_slice` fixes one 16-aligned allocation
        // whose element addresses remain stable for the composite owner's
        // lifetime.
        let descriptors: Box<_> = rows
            .iter()
            .map(|row| {
                // SAFETY: `row` lives in the immutable `bytes` allocation
                // retained below. The resulting descriptor is moved only
                // until the boxed slab is finalized, before a Head observes
                // its address.
                unsafe { ArchiveLeafDescriptor::new(NonNull::from(row)) }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let composite = Arc::new(SimpleArchiveLeafOwner {
            _bytes: bytes.clone(),
            descriptors,
        });
        let owner: Arc<dyn ArchiveOwner> = composite.clone();
        Self { composite, owner }
    }

    fn rows(&self) -> SimpleArchiveLeafSlice<'_> {
        SimpleArchiveLeafSlice {
            owner: &self.owner,
            descriptors: &self.composite.descriptors,
        }
    }
}

impl TryFromBlob<SimpleArchive> for TribleSet {
    type Error = UnarchiveError;

    fn try_from_blob(blob: Blob<SimpleArchive>) -> Result<Self, Self::Error> {
        try_from_blob_inner(blob, /*archive_backed:*/ true)
    }
}

/// Decode a [`SimpleArchive`] blob into a [`TribleSet`] forcing the
/// heap-`Leaf` ingest path (no archive-backed leaf). Exposed for measurement
/// so descriptor-backed decoding can be compared against the legacy heap
/// behaviour on identical input.
pub fn try_from_blob_heap_only(blob: Blob<SimpleArchive>) -> Result<TribleSet, UnarchiveError> {
    try_from_blob_inner(blob, /*archive_backed:*/ false)
}

fn try_from_blob_inner(
    blob: Blob<SimpleArchive>,
    archive_backed: bool,
) -> Result<TribleSet, UnarchiveError> {
    let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
        return Err(UnarchiveError::BadArchive);
    };
    let slice: &[[u8; 64]] = &packed_tribles;

    // SimpleArchive's process-local descriptor slab is 16-byte aligned even
    // when the portable byte view is not. Heads tag descriptor pointers while
    // Branch childleaf fields retain the raw key pointers, so archive-backed
    // decoding no longer needs an allocator-alignment fallback.
    let backing = archive_backed.then(|| SimpleArchiveLeafBacking::new(&blob.bytes, slice));

    #[cfg(feature = "parallel")]
    {
        if slice.len() >= PARALLEL_UNARCHIVE_THRESHOLD {
            return parallel_unarchive(slice, backing);
        }
    }

    serial_unarchive(slice, backing.as_ref().map(SimpleArchiveLeafBacking::rows))
}

/// Serial construction after any descriptor prepass. Ordering and redundancy
/// are validated inline with insertion. When `archive` is `Some`, each trible
/// is inserted as a descriptor-backed `ArchiveEntry`; when `None`, the
/// heap-Leaf path is taken directly without that prepass.
fn serial_unarchive(
    slice: &[[u8; 64]],
    archive: Option<SimpleArchiveLeafSlice<'_>>,
) -> Result<TribleSet, UnarchiveError> {
    debug_assert!(archive.is_none_or(|archive| archive.descriptors.len() == slice.len()));
    let mut tribles = TribleSet::new();
    let mut first_archive_entry = None;
    let mut archive_batch_started = false;
    let mut prev_trible: Option<&[u8; 64]> = None;
    for (index, t) in slice.iter().enumerate() {
        let Some(trible) = Trible::as_transmute_force_raw(t) else {
            return Err(UnarchiveError::BadTrible);
        };
        if let Some(prev) = prev_trible {
            if prev == t {
                return Err(UnarchiveError::BadCanonicalizationRedundancy);
            }
            if prev > t {
                return Err(UnarchiveError::BadCanonicalizationOrdering);
            }
        }
        prev_trible = Some(t);
        match archive {
            Some(archive) => {
                let descriptor = NonNull::from(&archive.descriptors[index]);
                // SAFETY: the erased owner and concrete descriptor view point
                // at the same composite allocation. It owns this descriptor
                // slab and the immutable Bytes allocation referenced by every
                // descriptor key pointer.
                let entry = unsafe { ArchiveEntry::from_descriptor(descriptor, archive.owner) };
                if archive_batch_started {
                    tribles.insert_archive(&entry);
                } else if let Some(first) = first_archive_entry.take() {
                    // The first two validated rows are a same-owner, distinct
                    // stack batch. They directly bootstrap each PATCH index
                    // as a Branch over two descriptor-backed leaves.
                    tribles.insert_archive_batch(&[first, entry]);
                    archive_batch_started = true;
                } else {
                    first_archive_entry = Some(entry);
                }
            }
            None => tribles.insert(trible),
        }
    }
    if let Some(first) = first_archive_entry {
        // A PATCH root can be descriptor-backed because its owner cover is
        // independent of trie shape.
        tribles.insert_archive_batch(&[first]);
    }
    Ok(tribles)
}

/// Parallel unarchive: chunk the blob, validate internal ordering
/// per chunk in parallel, build per-chunk `TribleSet`s, verify
/// boundary ordering between adjacent chunks, then reduce via
/// `TribleSet::union` (which itself fans out across the six
/// indexes — three levels of parallelism stacked).
#[cfg(feature = "parallel")]
fn parallel_unarchive(
    slice: &[[u8; 64]],
    backing: Option<SimpleArchiveLeafBacking>,
) -> Result<TribleSet, UnarchiveError> {
    use rayon::prelude::*;

    let n_threads = rayon::current_num_threads().max(1);
    // Aim for ~1 chunk per worker so each thread gets a clean slice
    // to crunch with maximal cache locality. Round up.
    let chunk_size = slice.len().div_ceil(n_threads).max(1);
    let chunks: Vec<&[[u8; 64]]> = slice.chunks(chunk_size).collect();

    // Phase 1: validate boundary ordering (sequential, but it's a
    // tiny O(num_chunks) scan over already-cache-hot slice ends).
    for w in chunks.windows(2) {
        let last_a = w[0].last().expect("non-empty chunk");
        let first_b = w[1].first().expect("non-empty chunk");
        if last_a == first_b {
            return Err(UnarchiveError::BadCanonicalizationRedundancy);
        }
        if last_a > first_b {
            return Err(UnarchiveError::BadCanonicalizationOrdering);
        }
    }

    // Phase 2: per-chunk serial unarchive in parallel. Every chunk
    // shares the same archive owner, so persistent owner-cover union later
    // deduplicates the guard while adopting descriptor-backed leaves wholesale.
    let chunk_sets: Result<Vec<TribleSet>, UnarchiveError> = chunks
        .par_iter()
        .enumerate()
        .map(|(chunk_index, chunk)| {
            let archive = backing.as_ref().map(|backing| {
                let start = chunk_index * chunk_size;
                let end = start + chunk.len();
                let rows = backing.rows();
                debug_assert!(end <= rows.descriptors.len());
                SimpleArchiveLeafSlice {
                    owner: rows.owner,
                    descriptors: &rows.descriptors[start..end],
                }
            });
            serial_unarchive(chunk, archive)
        })
        .collect();

    // Phase 3: reduce the per-chunk sets via TribleSet::union (the
    // 6-way index fan-out kicks in for any chunk pair above its
    // own threshold).
    Ok(chunk_sets?
        .into_par_iter()
        .reduce(TribleSet::new, |a, b| a + b))
}
