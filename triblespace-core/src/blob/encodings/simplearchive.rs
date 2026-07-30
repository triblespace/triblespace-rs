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
#[cfg(test)]
use crate::patch::hash_key;
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
    let mut first_archive_entry = None;
    let mut archive_batch_started = false;
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
                if archive_batch_started {
                    tribles.insert_archive(&entry);
                } else if let Some(first) = first_archive_entry.take() {
                    // The first two validated rows are a same-owner, distinct
                    // stack batch. They directly bootstrap each PATCH index
                    // as a Branch over two LocalLeaves.
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
        // A PATCH root can be a LocalLeaf because its owner cover is independent
        // of trie shape.
        tribles.insert_archive_batch(&[first]);
    }
    Ok(tribles)
}

/// Phase accounting for the test-only fused all-six construction probe.
#[cfg(test)]
pub(crate) struct BottomUpArchiveProbe {
    pub(crate) set: TribleSet,
    pub(crate) validation_and_hash: std::time::Duration,
    pub(crate) partition_and_build: std::time::Duration,
    pub(crate) total: std::time::Duration,
    pub(crate) row_hashes: usize,
    pub(crate) hash_bytes: usize,
    pub(crate) permutation_bytes: usize,
}

/// Calls the actual serial production decoder regardless of the crate's
/// `parallel` feature. This wrapper exists only to give the construction probe
/// an exact apples-to-apples baseline.
#[cfg(test)]
pub(crate) fn try_from_blob_serial_for_test(
    blob: Blob<SimpleArchive>,
) -> Result<TribleSet, UnarchiveError> {
    let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
        return Err(UnarchiveError::BadArchive);
    };
    let slice: &[[u8; 64]] = &packed_tribles;
    let owner: Option<Arc<dyn ArchiveOwner>> = ((slice.as_ptr() as usize) & 0x0f == 0)
        .then(|| Arc::new(blob.bytes.clone()) as Arc<dyn ArchiveOwner>);
    serial_unarchive(slice, owner.as_ref())
}

#[cfg(test)]
fn validate_and_hash_archive_slice_for_test(
    slice: &[[u8; 64]],
) -> Result<Vec<u128>, UnarchiveError> {
    let mut hashes = Vec::with_capacity(slice.len());
    let mut previous: Option<&[u8; 64]> = None;
    for row in slice {
        if Trible::as_transmute_force_raw(row).is_none() {
            return Err(UnarchiveError::BadTrible);
        }
        if let Some(previous) = previous {
            if previous == row {
                return Err(UnarchiveError::BadCanonicalizationRedundancy);
            }
            if previous > row {
                return Err(UnarchiveError::BadCanonicalizationOrdering);
            }
        }
        previous = Some(row);
        hashes.push(hash_key(&row[..]));
    }
    Ok(hashes)
}

/// Validates one canonical EAV archive, hashes each row exactly once, and then
/// constructs all six PATCH indexes through the test-only in-place MSD radix
/// builder.
#[cfg(test)]
pub(crate) fn try_from_blob_bottom_up_for_test(
    blob: Blob<SimpleArchive>,
) -> Result<BottomUpArchiveProbe, UnarchiveError> {
    let total_start = std::time::Instant::now();
    let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
        return Err(UnarchiveError::BadArchive);
    };
    let slice: &[[u8; 64]] = &packed_tribles;
    assert_eq!(
        slice.as_ptr() as usize & 0x0f,
        0,
        "the archive-backed bottom-up probe requires aligned bytes",
    );
    let owner: Arc<dyn ArchiveOwner> = Arc::new(blob.bytes.clone());

    let validation_start = std::time::Instant::now();
    let hashes = validate_and_hash_archive_slice_for_test(slice)?;
    let validation_and_hash = validation_start.elapsed();
    let row_hashes = hashes.len();
    let hash_bytes = hashes.capacity() * std::mem::size_of::<u128>();

    let build_start = std::time::Instant::now();
    let (set, permutation_bytes) =
        unsafe { TribleSet::from_archive_partition_for_test(slice, &hashes, &owner) };
    let partition_and_build = build_start.elapsed();

    Ok(BottomUpArchiveProbe {
        set,
        validation_and_hash,
        partition_and_build,
        total: total_start.elapsed(),
        row_hashes,
        hash_bytes,
        permutation_bytes,
    })
}

/// Production-shaped bottom-up probe: retain the existing parallel unarchive
/// topology (one contiguous EAV chunk per worker followed by disjoint set
/// reduction), but replace each chunk's online insertion loop with the fused
/// all-six partition builder. Across all live chunks the explicit hash and
/// permutation storage remains 20 bytes per archive row.
#[cfg(all(test, feature = "parallel"))]
fn try_from_blob_chunked_bottom_up_for_test(
    blob: Blob<SimpleArchive>,
) -> Result<TribleSet, UnarchiveError> {
    use rayon::prelude::*;

    let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
        return Err(UnarchiveError::BadArchive);
    };
    let slice: &[[u8; 64]] = &packed_tribles;
    let owner: Option<Arc<dyn ArchiveOwner>> = ((slice.as_ptr() as usize) & 0x0f == 0)
        .then(|| Arc::new(blob.bytes.clone()) as Arc<dyn ArchiveOwner>);

    if slice.len() < PARALLEL_UNARCHIVE_THRESHOLD {
        return serial_unarchive(slice, owner.as_ref());
    }

    let n_threads = rayon::current_num_threads().max(1);
    let chunk_size = slice.len().div_ceil(n_threads).max(1);
    let Some(owner) = owner else {
        return parallel_unarchive(slice, None);
    };
    if u32::try_from(chunk_size).is_err() {
        return parallel_unarchive(slice, Some(owner));
    }
    let chunks: Vec<&[[u8; 64]]> = slice.chunks(chunk_size).collect();

    for pair in chunks.windows(2) {
        let left = pair[0].last().expect("non-empty chunk");
        let right = pair[1].first().expect("non-empty chunk");
        if left == right {
            return Err(UnarchiveError::BadCanonicalizationRedundancy);
        }
        if left > right {
            return Err(UnarchiveError::BadCanonicalizationOrdering);
        }
    }

    let chunk_sets: Result<Vec<TribleSet>, UnarchiveError> = chunks
        .par_iter()
        .map(|chunk| {
            let hashes = validate_and_hash_archive_slice_for_test(chunk)?;
            let (set, _) =
                unsafe { TribleSet::from_archive_partition_for_test(chunk, &hashes, &owner) };
            Ok(set)
        })
        .collect();

    Ok(chunk_sets?
        .into_par_iter()
        .reduce(TribleSet::new, |left, right| left + right))
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
    // shares the same archive owner, so persistent owner-cover union later
    // deduplicates the guard while adopting LocalLeaves wholesale.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patch::{KeySchema, PATCH};
    use crate::trible::{AEVOrder, AVEOrder, EAVOrder, EVAOrder, VAEOrder, VEAOrder};
    use std::hint::black_box;
    use std::time::{Duration, Instant};

    fn fixture_row(index: usize) -> [u8; 64] {
        const FACTS_PER_ENTITY: usize = 8;
        let entity = index / FACTS_PER_ENTITY + 1;
        let attribute = index % FACTS_PER_ENTITY + 1;
        let mut row = [0u8; 64];
        row[8..16].copy_from_slice(&(entity as u64).to_be_bytes());
        row[24..32].copy_from_slice(&(attribute as u64).to_be_bytes());

        let mut state = index as u64 ^ 0x9e37_79b9_7f4a_7c15;
        for chunk in row[32..].chunks_exact_mut(8) {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^= mixed >> 31;
            chunk.copy_from_slice(&mixed.to_be_bytes());
        }
        row
    }

    fn fixture_blob(len: usize) -> Blob<SimpleArchive> {
        let rows: Vec<[u8; 64]> = (0..len).map(fixture_row).collect();
        assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            rows.as_ptr() as usize & 0x0f,
            0,
            "archive fixture allocation must support LocalLeaves",
        );
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    #[cfg(feature = "parallel")]
    #[derive(Clone, Copy)]
    enum VariedArchiveGeometry {
        EntityLike,
        HighEntropy,
        LongPrefixSkew,
    }

    #[cfg(feature = "parallel")]
    impl VariedArchiveGeometry {
        fn name(self) -> &'static str {
            match self {
                Self::EntityLike => "entity_like_control",
                Self::HighEntropy => "high_entropy_uniform_eav",
                Self::LongPrefixSkew => "long_prefix_low_cardinality",
            }
        }

        fn row(self, index: usize) -> [u8; 64] {
            match self {
                Self::EntityLike => fixture_row(index),
                Self::HighEntropy => high_entropy_row(index),
                Self::LongPrefixSkew => long_prefix_skew_row(index),
            }
        }
    }

    #[cfg(feature = "parallel")]
    fn avalanche_word(mut word: u64) -> u64 {
        // Every operation is bijective on u64, so each fixed-salt output word
        // remains an injective function of the row ordinal.
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^ (word >> 31)
    }

    #[cfg(feature = "parallel")]
    fn high_entropy_row(index: usize) -> [u8; 64] {
        let ordinal = u64::try_from(index).expect("fixture ordinal must fit u64");
        let mut row = [0u8; 64];
        for (word_index, chunk) in row.chunks_exact_mut(8).enumerate() {
            let salt = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(word_index as u64 + 1);
            chunk.copy_from_slice(&avalanche_word(ordinal ^ salt).to_be_bytes());
        }
        // Distinct salts mean neither 16-byte identifier can have both words
        // zero. The first word alone is also injective, proving row uniqueness.
        debug_assert!(row[..16].iter().any(|byte| *byte != 0));
        debug_assert!(row[16..32].iter().any(|byte| *byte != 0));
        row
    }

    #[cfg(feature = "parallel")]
    fn long_prefix_skew_row(index: usize) -> [u8; 64] {
        const ENTITY_CARDINALITY: usize = 256;
        const ATTRIBUTE_CARDINALITY: usize = 16;

        let mut row = [0u8; 64];
        let entity = index % ENTITY_CARDINALITY + 1;
        let attribute = (index / ENTITY_CARDINALITY) % ATTRIBUTE_CARDINALITY + 1;
        let value = index + 1;
        row[12..16].copy_from_slice(
            &u32::try_from(entity)
                .expect("fixture entity must fit u32")
                .to_be_bytes(),
        );
        row[28..32].copy_from_slice(
            &u32::try_from(attribute)
                .expect("fixture attribute must fit u32")
                .to_be_bytes(),
        );
        row[56..64].copy_from_slice(
            &u64::try_from(value)
                .expect("fixture value must fit u64")
                .to_be_bytes(),
        );
        row
    }

    #[cfg(feature = "parallel")]
    fn varied_fixture_blob(geometry: VariedArchiveGeometry, len: usize) -> Blob<SimpleArchive> {
        let mut rows = (0..len)
            .map(|index| geometry.row(index))
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(rows.len(), len);
        assert!(rows
            .iter()
            .all(|row| Trible::as_transmute_force_raw(row).is_some()));
        assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            rows.as_ptr() as usize & 0x0f,
            0,
            "archive fixture allocation must support LocalLeaves",
        );
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    fn blob_from_rows(rows: Vec<[u8; 64]>) -> Blob<SimpleArchive> {
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    fn assert_index_parity<O: KeySchema<64>>(
        candidate: &PATCH<64, O>,
        baseline: &PATCH<64, O>,
        len: usize,
    ) {
        assert_eq!(candidate.len(), len as u64);
        assert_eq!(
            candidate.iter_ordered().copied().collect::<Vec<_>>(),
            baseline.iter_ordered().copied().collect::<Vec<_>>(),
        );
        assert_eq!(candidate.root_hash(), baseline.root_hash());
        assert_eq!(
            candidate.branch_fanout_histogram(),
            baseline.branch_fanout_histogram(),
        );
        let candidate_stats = candidate.node_stats();
        let baseline_stats = baseline.node_stats();
        assert_eq!(candidate_stats.0, baseline_stats.0, "branch count differs");
        assert_eq!(candidate_stats.2, 0, "candidate materialized heap leaves");
        assert_eq!(candidate_stats.3, len as u64, "candidate lost LocalLeaves");
        assert_eq!(baseline_stats.2, 0, "baseline materialized heap leaves");
        assert_eq!(baseline_stats.3, len as u64, "baseline lost LocalLeaves");
    }

    fn assert_all_six_parity(candidate: &TribleSet, baseline: &TribleSet, len: usize) {
        assert_index_parity::<EAVOrder>(&candidate.eav, &baseline.eav, len);
        assert_index_parity::<EVAOrder>(&candidate.eva, &baseline.eva, len);
        assert_index_parity::<AEVOrder>(&candidate.aev, &baseline.aev, len);
        assert_index_parity::<AVEOrder>(&candidate.ave, &baseline.ave, len);
        assert_index_parity::<VEAOrder>(&candidate.vea, &baseline.vea, len);
        assert_index_parity::<VAEOrder>(&candidate.vae, &baseline.vae, len);
    }

    #[test]
    fn all_six_bottom_up_archive_matches_serial_production() {
        for len in [1usize, 2, 3, 257, 8_192] {
            let blob = fixture_blob(len);
            let baseline = try_from_blob_serial_for_test(blob.clone()).unwrap();
            let probe = try_from_blob_bottom_up_for_test(blob.clone()).unwrap();
            assert_eq!(probe.row_hashes, len);
            assert_eq!(probe.hash_bytes, len * std::mem::size_of::<u128>());
            assert_eq!(probe.permutation_bytes, len * std::mem::size_of::<u32>(),);
            assert_all_six_parity(&probe.set, &baseline, len);

            let survivor = probe.set.clone();
            drop(probe.set);
            drop(baseline);
            drop(blob);
            black_box(vec![0xa5u8; len.saturating_mul(64).min(1 << 20)]);
            assert_eq!(survivor.eav.iter_ordered().count(), len);
            assert_eq!(survivor.eva.iter_ordered().count(), len);
            assert_eq!(survivor.aev.iter_ordered().count(), len);
            assert_eq!(survivor.ave.iter_ordered().count(), len);
            assert_eq!(survivor.vea.iter_ordered().count(), len);
            assert_eq!(survivor.vae.iter_ordered().count(), len);
        }
    }

    #[test]
    fn all_six_bottom_up_validates_canonical_eav_input() {
        let first = fixture_row(0);
        let second = fixture_row(1);
        assert_eq!(
            try_from_blob_bottom_up_for_test(blob_from_rows(vec![first, first]))
                .err()
                .expect("duplicate must fail"),
            UnarchiveError::BadCanonicalizationRedundancy,
        );
        assert_eq!(
            try_from_blob_bottom_up_for_test(blob_from_rows(vec![second, first]))
                .err()
                .expect("descending input must fail"),
            UnarchiveError::BadCanonicalizationOrdering,
        );
        let mut invalid = first;
        invalid[..16].fill(0);
        assert_eq!(
            try_from_blob_bottom_up_for_test(blob_from_rows(vec![invalid]))
                .err()
                .expect("nil entity must fail"),
            UnarchiveError::BadTrible,
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn chunked_bottom_up_archive_matches_public_parallel_path() {
        for len in [4_095usize, 4_096, 8_192] {
            let blob = fixture_blob(len);
            let baseline = TribleSet::try_from_blob(blob.clone()).unwrap();
            let candidate = try_from_blob_chunked_bottom_up_for_test(blob.clone()).unwrap();
            assert_all_six_parity(&candidate, &baseline, len);

            let survivor = candidate.clone();
            drop(candidate);
            drop(baseline);
            drop(blob);
            black_box(vec![0x5au8; len.saturating_mul(64).min(1 << 20)]);
            assert_eq!(survivor.eav.iter_ordered().count(), len);
            assert_eq!(survivor.eva.iter_ordered().count(), len);
            assert_eq!(survivor.aev.iter_ordered().count(), len);
            assert_eq!(survivor.ave.iter_ordered().count(), len);
            assert_eq!(survivor.vea.iter_ordered().count(), len);
            assert_eq!(survivor.vae.iter_ordered().count(), len);
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn chunked_bottom_up_archive_preserves_public_errors() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                fn assert_error_parity(rows: Vec<[u8; 64]>, expected: UnarchiveError) {
                    let blob = blob_from_rows(rows);
                    assert_eq!(
                        TribleSet::try_from_blob(blob.clone())
                            .expect_err("public decoder must reject invalid input"),
                        expected,
                    );
                    assert_eq!(
                        try_from_blob_chunked_bottom_up_for_test(blob)
                            .expect_err("chunked bottom-up decoder must reject invalid input"),
                        expected,
                    );
                }

                let len = PARALLEL_UNARCHIVE_THRESHOLD;
                let chunk_size = len.div_ceil(rayon::current_num_threads());

                let mut duplicate = (0..len).map(fixture_row).collect::<Vec<_>>();
                duplicate[chunk_size] = duplicate[chunk_size - 1];
                assert_error_parity(duplicate, UnarchiveError::BadCanonicalizationRedundancy);

                let mut descending = (0..len).map(fixture_row).collect::<Vec<_>>();
                descending.swap(chunk_size - 1, chunk_size);
                assert_error_parity(descending, UnarchiveError::BadCanonicalizationOrdering);

                let invalid = (0..len)
                    .map(|index| {
                        let mut row = [0u8; 64];
                        row[31] = 1;
                        row[56..64].copy_from_slice(&((index + 1) as u64).to_be_bytes());
                        row
                    })
                    .collect();
                assert_error_parity(invalid, UnarchiveError::BadTrible);

                let malformed = Blob::new(Bytes::from(vec![0u8; 63]));
                assert_eq!(
                    TribleSet::try_from_blob(malformed.clone())
                        .expect_err("public decoder must reject malformed bytes"),
                    UnarchiveError::BadArchive,
                );
                assert_eq!(
                    try_from_blob_chunked_bottom_up_for_test(malformed)
                        .expect_err("chunked bottom-up decoder must reject malformed bytes"),
                    UnarchiveError::BadArchive,
                );
            });
    }

    /// End-to-end serial production vs fused all-six MSD construction timing.
    /// Run with:
    ///
    /// `cargo test -p triblespace-core --release all_six_bottom_up_archive_timing -- --ignored --nocapture`
    #[test]
    #[ignore = "manual 100k/1m all-six construction benchmark"]
    fn all_six_bottom_up_archive_timing() {
        fn median(samples: &mut [Duration]) -> Duration {
            samples.sort_unstable();
            samples[samples.len() / 2]
        }

        for (len, rounds) in [(100_000usize, 5usize), (1_000_000, 3)] {
            let blob = fixture_blob(len);
            let baseline_oracle = try_from_blob_serial_for_test(blob.clone()).unwrap();
            let candidate_oracle = try_from_blob_bottom_up_for_test(blob.clone()).unwrap();
            assert_all_six_parity(&candidate_oracle.set, &baseline_oracle, len);
            assert_eq!(candidate_oracle.row_hashes, len);
            let hash_bytes = candidate_oracle.hash_bytes;
            let permutation_bytes = candidate_oracle.permutation_bytes;
            drop(candidate_oracle.set);
            drop(baseline_oracle);

            let mut baseline_samples = Vec::with_capacity(rounds);
            let mut candidate_samples = Vec::with_capacity(rounds);
            let mut validation_samples = Vec::with_capacity(rounds);
            let mut build_samples = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let baseline = || {
                    let start = Instant::now();
                    let set = try_from_blob_serial_for_test(black_box(blob.clone())).unwrap();
                    let elapsed = start.elapsed();
                    black_box(set.len());
                    drop(set);
                    elapsed
                };
                let candidate = || {
                    let start = Instant::now();
                    let probe = try_from_blob_bottom_up_for_test(black_box(blob.clone())).unwrap();
                    let elapsed = start.elapsed();
                    black_box(probe.set.len());
                    let phases = (probe.validation_and_hash, probe.partition_and_build);
                    debug_assert!(probe.total <= elapsed);
                    drop(probe.set);
                    (elapsed, phases)
                };
                let (baseline_elapsed, candidate_result) = if round % 2 == 0 {
                    let baseline_elapsed = baseline();
                    let candidate_result = candidate();
                    (baseline_elapsed, candidate_result)
                } else {
                    let candidate_result = candidate();
                    let baseline_elapsed = baseline();
                    (baseline_elapsed, candidate_result)
                };
                baseline_samples.push(baseline_elapsed);
                candidate_samples.push(candidate_result.0);
                validation_samples.push(candidate_result.1 .0);
                build_samples.push(candidate_result.1 .1);
            }

            let baseline = median(&mut baseline_samples);
            let candidate = median(&mut candidate_samples);
            let validation = median(&mut validation_samples);
            let build = median(&mut build_samples);
            println!(
                "all_six_bottom_up_archive len={len} baseline_ms={:.3} candidate_ms={:.3} speedup={:.3}x validation_hash_ms={:.3} partition_build_ms={:.3} hash_bytes={hash_bytes} permutation_bytes={permutation_bytes} transient_bytes={} transient_mib={:.3}",
                baseline.as_secs_f64() * 1e3,
                candidate.as_secs_f64() * 1e3,
                baseline.as_secs_f64() / candidate.as_secs_f64(),
                validation.as_secs_f64() * 1e3,
                build.as_secs_f64() * 1e3,
                hash_bytes + permutation_bytes,
                (hash_bytes + permutation_bytes) as f64 / (1024.0 * 1024.0),
            );
        }
    }

    /// Actual public parallel chunk+online+union path versus the same chunk
    /// DAG with the sparse bottom-up builder inside every worker.
    #[cfg(feature = "parallel")]
    #[test]
    #[ignore = "manual 100k/1m production-shaped construction benchmark"]
    fn chunked_bottom_up_archive_timing() {
        fn median(samples: &mut [Duration]) -> Duration {
            samples.sort_unstable();
            samples[samples.len() / 2]
        }

        for (len, rounds) in [(100_000usize, 6usize), (1_000_000, 4)] {
            let blob = fixture_blob(len);
            let baseline_oracle = TribleSet::try_from_blob(blob.clone()).unwrap();
            let candidate_oracle = try_from_blob_chunked_bottom_up_for_test(blob.clone()).unwrap();
            assert_all_six_parity(&candidate_oracle, &baseline_oracle, len);
            drop(candidate_oracle);
            drop(baseline_oracle);

            let mut baseline_samples = Vec::with_capacity(rounds);
            let mut candidate_samples = Vec::with_capacity(rounds);
            for round in 0..rounds {
                let baseline = || {
                    let start = Instant::now();
                    let set = TribleSet::try_from_blob(black_box(blob.clone())).unwrap();
                    let elapsed = start.elapsed();
                    black_box(set.len());
                    drop(set);
                    elapsed
                };
                let candidate = || {
                    let start = Instant::now();
                    let set =
                        try_from_blob_chunked_bottom_up_for_test(black_box(blob.clone())).unwrap();
                    let elapsed = start.elapsed();
                    black_box(set.len());
                    drop(set);
                    elapsed
                };
                let (baseline_elapsed, candidate_elapsed) = if round % 2 == 0 {
                    (baseline(), candidate())
                } else {
                    let candidate_elapsed = candidate();
                    (baseline(), candidate_elapsed)
                };
                baseline_samples.push(baseline_elapsed);
                candidate_samples.push(candidate_elapsed);
            }

            let baseline = median(&mut baseline_samples);
            let candidate = median(&mut candidate_samples);
            let transient_bytes = len * (std::mem::size_of::<u128>() + std::mem::size_of::<u32>());
            println!(
                "chunked_bottom_up_archive len={len} threads={} baseline_ms={:.3} candidate_ms={:.3} speedup={:.3}x transient_bytes={transient_bytes} transient_mib={:.3}",
                rayon::current_num_threads(),
                baseline.as_secs_f64() * 1e3,
                candidate.as_secs_f64() * 1e3,
                baseline.as_secs_f64() / candidate.as_secs_f64(),
                transient_bytes as f64 / (1024.0 * 1024.0),
            );
        }
    }

    /// Public parallel chunk+online+union versus candidate D's matching chunk
    /// DAG with bottom-up workers, across materially different trie shapes.
    /// Dataset generation, sorting, canonical validation, and parity oracles
    /// are outside the timed samples; result destruction is after each sample.
    ///
    /// The reported logical payload accounting is exact for candidate D's
    /// explicit arrays: one `u128` hash and one `u32` permutation slot per row.
    /// `full_overlap_payload_bytes` is their worst-case sum if every chunk is
    /// building concurrently; instantaneous overlap may be lower. This excludes
    /// allocator metadata, Rayon task/result vectors, input archive bytes, and
    /// the output PATCH nodes shared by both paths.
    /// Run with:
    ///
    /// `cargo test -p triblespace-core --release --features parallel chunked_bottom_up_archive_varied_data_timing -- --ignored --nocapture --test-threads=1`
    #[cfg(feature = "parallel")]
    #[test]
    #[ignore = "manual varied-data 100k/1m construction benchmark"]
    fn chunked_bottom_up_archive_varied_data_timing() {
        fn median(samples: &mut [Duration]) -> Duration {
            samples.sort_unstable();
            samples[samples.len() / 2]
        }

        let geometries = [
            VariedArchiveGeometry::HighEntropy,
            VariedArchiveGeometry::LongPrefixSkew,
            VariedArchiveGeometry::EntityLike,
        ];
        for geometry in geometries {
            for (len, rounds) in [(100_000usize, 6usize), (1_000_000, 4)] {
                let blob = varied_fixture_blob(geometry, len);

                let baseline_oracle = TribleSet::try_from_blob(blob.clone()).unwrap();
                let candidate_oracle =
                    try_from_blob_chunked_bottom_up_for_test(blob.clone()).unwrap();
                assert_all_six_parity(&candidate_oracle, &baseline_oracle, len);
                drop(candidate_oracle);
                drop(baseline_oracle);

                let mut baseline_samples = Vec::with_capacity(rounds);
                let mut candidate_samples = Vec::with_capacity(rounds);
                for round in 0..rounds {
                    let baseline = || {
                        let start = Instant::now();
                        let set = TribleSet::try_from_blob(black_box(blob.clone())).unwrap();
                        let elapsed = start.elapsed();
                        black_box(set.len());
                        drop(set);
                        elapsed
                    };
                    let candidate = || {
                        let start = Instant::now();
                        let set = try_from_blob_chunked_bottom_up_for_test(black_box(blob.clone()))
                            .unwrap();
                        let elapsed = start.elapsed();
                        black_box(set.len());
                        drop(set);
                        elapsed
                    };
                    let (baseline_elapsed, candidate_elapsed) = if round % 2 == 0 {
                        (baseline(), candidate())
                    } else {
                        let candidate_elapsed = candidate();
                        (baseline(), candidate_elapsed)
                    };
                    baseline_samples.push(baseline_elapsed);
                    candidate_samples.push(candidate_elapsed);
                }

                let baseline = median(&mut baseline_samples);
                let candidate = median(&mut candidate_samples);
                let threads = rayon::current_num_threads().max(1);
                let chunk_size = len.div_ceil(threads).max(1);
                let chunk_count = len.div_ceil(chunk_size);
                let hash_payload_bytes = len * std::mem::size_of::<u128>();
                let permutation_payload_bytes = len * std::mem::size_of::<u32>();
                let full_overlap_payload_bytes = hash_payload_bytes + permutation_payload_bytes;
                let max_chunk_rows = len.min(chunk_size);
                let max_chunk_payload_bytes =
                    max_chunk_rows * (std::mem::size_of::<u128>() + std::mem::size_of::<u32>());
                println!(
                    "chunked_bottom_up_archive_varied geometry={} len={len} threads={threads} chunks={chunk_count} baseline_ms={:.3} candidate_ms={:.3} speedup={:.3}x hash_payload_bytes={hash_payload_bytes} permutation_payload_bytes={permutation_payload_bytes} full_overlap_payload_bytes={full_overlap_payload_bytes} full_overlap_payload_mib={:.3} max_chunk_payload_bytes={max_chunk_payload_bytes}",
                    geometry.name(),
                    baseline.as_secs_f64() * 1e3,
                    candidate.as_secs_f64() * 1e3,
                    baseline.as_secs_f64() / candidate.as_secs_f64(),
                    full_overlap_payload_bytes as f64 / (1024.0 * 1024.0),
                );
            }
        }
    }
}
