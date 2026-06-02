use crate::inline::Encodes;
use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::patch::ArchiveEntry;
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
where crate::inline::encodings::hash::Handle<SimpleArchive>: crate::inline::InlineEncoding,
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
where crate::inline::encodings::hash::Handle<SimpleArchive>: crate::inline::InlineEncoding,
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

impl TryFromBlob<SimpleArchive> for TribleSet {
    type Error = UnarchiveError;

    fn try_from_blob(blob: Blob<SimpleArchive>) -> Result<Self, Self::Error> {
        try_from_blob_inner(blob, /*archive_backed:*/ true)
    }
}

/// Decode a [`SimpleArchive`] blob into a [`TribleSet`] forcing the
/// heap-`Leaf` ingest path (no `LocalLeaf`). Exposed for measurement
/// so the LocalLeaf path can be compared against the legacy heap
/// behaviour on identical input.
pub fn try_from_blob_heap_only(
    blob: Blob<SimpleArchive>,
) -> Result<TribleSet, UnarchiveError> {
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

    // ArchiveEntry / LocalLeaf require the trible pointer to be
    // 16-byte aligned (the low 4 bits encode `HeadTag::LocalLeaf`).
    // Every 64-byte stride preserves alignment, so it's enough to
    // check the slice base. Modern allocators (and mmap'd files)
    // satisfy this; the heap-Leaf fallback handles the rare miss.
    let owner: Option<Arc<dyn ArchiveOwner>> =
        if archive_backed && (slice.as_ptr() as usize) & 0x0f == 0 {
            Some(Arc::new(blob.bytes.clone()))
        } else {
            None
        };

    #[cfg(feature = "parallel")]
    {
        if slice.len() >= PARALLEL_UNARCHIVE_THRESHOLD {
            return parallel_unarchive(slice, owner);
        }
    }

    serial_unarchive(slice, owner.as_ref())
}

/// Serial fallback. Validates ordering + redundancy inline with
/// insertion — every byte read once. When `owner` is `Some`, each
/// trible is inserted as an `ArchiveEntry` (LocalLeaf-backed); when
/// `None`, the heap-Leaf path is taken.
fn serial_unarchive(
    slice: &[[u8; 64]],
    owner: Option<&Arc<dyn ArchiveOwner>>,
) -> Result<TribleSet, UnarchiveError> {
    let mut tribles = TribleSet::new();
    let mut prev_trible: Option<&[u8; 64]> = None;
    for t in slice.iter() {
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
        match owner {
            Some(owner_arc) => {
                // SAFETY: `t` points into the archive bytes kept alive
                // by `owner_arc`, and base-alignment + 64-byte stride
                // guarantees this element is 16-byte aligned.
                let ptr = NonNull::from(t);
                let entry = unsafe { ArchiveEntry::new(ptr, owner_arc) };
                tribles.insert_archive(&entry);
            }
            None => tribles.insert(trible),
        }
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
    owner: Option<Arc<dyn ArchiveOwner>>,
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
    // shares the same archive owner, so `union` later sees identical
    // owner Arcs and can adopt LocalLeaves wholesale.
    let chunk_sets: Result<Vec<TribleSet>, UnarchiveError> = chunks
        .par_iter()
        .map(|chunk| serial_unarchive(chunk, owner.as_ref()))
        .collect();

    // Phase 3: reduce the per-chunk sets via TribleSet::union (the
    // 6-way index fan-out kicks in for any chunk pair above its
    // own threshold).
    Ok(chunk_sets?
        .into_par_iter()
        .reduce(TribleSet::new, |a, b| a + b))
}
