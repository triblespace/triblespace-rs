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
#[cfg(any(test, feature = "parallel"))]
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
/// heap-`Leaf` ingest path (no `LocalLeaf`). Below the parallel threshold this
/// isolates leaf representation on the same serial decoder; above it this is
/// an end-to-end heap-online baseline for the public bottom-up decoder.
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

#[cfg(any(test, feature = "parallel"))]
fn validate_and_hash_archive_slice(slice: &[[u8; 64]]) -> Result<Vec<u128>, UnarchiveError> {
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

#[cfg(any(test, feature = "parallel"))]
#[inline]
fn bottom_up_chunk_rows_fit(chunk_size: usize) -> bool {
    u32::try_from(chunk_size).is_ok()
}

/// Parallel unarchive: chunk the blob, verify boundary ordering, validate and
/// build aligned chunks bottom-up, then reduce through `TribleSet::union`.
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
    let use_bottom_up = owner.is_some() && bottom_up_chunk_rows_fit(chunk_size);

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

    // Phase 2: each aligned, u32-addressable chunk is validated, hashed once
    // per row, and partition-built into all six indexes. Unaligned input keeps
    // the heap-Leaf worker; an impractically large chunk keeps the established
    // online archive worker.
    let chunk_sets: Result<Vec<TribleSet>, UnarchiveError> = chunks
        .par_iter()
        .map(|chunk| {
            if use_bottom_up {
                let owner = owner
                    .as_ref()
                    .expect("bottom-up eligibility requires an archive owner");
                let hashes = validate_and_hash_archive_slice(chunk)?;
                // SAFETY: owner presence proves 16-byte base alignment (and
                // 64-byte strides preserve it); validation proves canonical,
                // distinct tribles; hashes correspond index-for-index to this
                // chunk; and `use_bottom_up` proves every row ordinal fits u32.
                Ok(unsafe { TribleSet::from_archive_partition(chunk, &hashes, owner) })
            } else {
                serial_unarchive(chunk, owner.as_ref())
            }
        })
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
    #[cfg(feature = "parallel")]
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
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    #[cfg(feature = "parallel")]
    #[derive(Clone, Copy)]
    enum BenchmarkGeometry {
        EntityLike,
        HighEntropy,
        LongPrefixLowCardinality,
    }

    #[cfg(feature = "parallel")]
    impl BenchmarkGeometry {
        fn name(self) -> &'static str {
            match self {
                Self::EntityLike => "entity_like",
                Self::HighEntropy => "high_entropy",
                Self::LongPrefixLowCardinality => "long_prefix_low_cardinality",
            }
        }

        fn row(self, index: usize) -> [u8; 64] {
            match self {
                Self::EntityLike => fixture_row(index),
                Self::HighEntropy => high_entropy_row(index),
                Self::LongPrefixLowCardinality => long_prefix_low_cardinality_row(index),
            }
        }
    }

    #[cfg(feature = "parallel")]
    fn avalanche_word(mut word: u64) -> u64 {
        // This is a bijection on u64. Distinct ordinals therefore remain
        // distinct in each fixed-salt word.
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
        debug_assert!(row[..16].iter().any(|byte| *byte != 0));
        debug_assert!(row[16..32].iter().any(|byte| *byte != 0));
        row
    }

    #[cfg(feature = "parallel")]
    fn long_prefix_low_cardinality_row(index: usize) -> [u8; 64] {
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
    fn benchmark_blob(geometry: BenchmarkGeometry, len: usize) -> Blob<SimpleArchive> {
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
            "benchmark archive allocation must support LocalLeaves",
        );
        Blob::new(Bytes::from(rows))
    }

    fn blob_from_rows(rows: Vec<[u8; 64]>) -> Blob<SimpleArchive> {
        Blob::new(Bytes::from(rows))
    }

    fn serial_for_test(blob: Blob<SimpleArchive>) -> Result<TribleSet, UnarchiveError> {
        let Ok(packed): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
            return Err(UnarchiveError::BadArchive);
        };
        let slice: &[[u8; 64]] = &packed;
        let owner: Option<Arc<dyn ArchiveOwner>> = ((slice.as_ptr() as usize) & 0x0f == 0)
            .then(|| Arc::new(blob.bytes.clone()) as Arc<dyn ArchiveOwner>);
        serial_unarchive(slice, owner.as_ref())
    }

    fn bottom_up_for_test(blob: Blob<SimpleArchive>) -> Result<TribleSet, UnarchiveError> {
        let Ok(packed): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
            return Err(UnarchiveError::BadArchive);
        };
        let slice: &[[u8; 64]] = &packed;
        assert!(
            slice.is_empty() || slice.as_ptr() as usize & 0x0f == 0,
            "bottom-up test archives must be aligned",
        );
        let hashes = validate_and_hash_archive_slice(slice)?;
        let owner: Arc<dyn ArchiveOwner> = Arc::new(blob.bytes.clone());
        // SAFETY: the test checks alignment; validation proves well-formed,
        // canonical, distinct rows and produces the matching hash vector.
        Ok(unsafe { TribleSet::from_archive_partition(slice, &hashes, &owner) })
    }

    /// The production decoder immediately before the bottom-up change, copied
    /// from parent `677401ee`. Keeping this oracle in the same optimized test
    /// binary removes cross-binary and cross-revision noise from the causal
    /// benchmark below.
    #[cfg(feature = "parallel")]
    fn legacy_try_from_blob_for_benchmark(
        blob: Blob<SimpleArchive>,
    ) -> Result<TribleSet, UnarchiveError> {
        let Ok(packed_tribles): Result<View<[[u8; 64]]>, _> = blob.bytes.clone().view() else {
            return Err(UnarchiveError::BadArchive);
        };
        let slice: &[[u8; 64]] = &packed_tribles;
        let owner: Option<Arc<dyn ArchiveOwner>> = if (slice.as_ptr() as usize) & 0x0f == 0 {
            Some(Arc::new(blob.bytes.clone()))
        } else {
            None
        };

        if slice.len() >= PARALLEL_UNARCHIVE_THRESHOLD {
            legacy_parallel_unarchive_for_benchmark(slice, owner)
        } else {
            serial_unarchive(slice, owner.as_ref())
        }
    }

    /// Exact former production worker DAG: chunk, validate and insert rows
    /// online inside each worker, then reduce the chunk sets through union.
    #[cfg(feature = "parallel")]
    fn legacy_parallel_unarchive_for_benchmark(
        slice: &[[u8; 64]],
        owner: Option<Arc<dyn ArchiveOwner>>,
    ) -> Result<TribleSet, UnarchiveError> {
        use rayon::prelude::*;

        let n_threads = rayon::current_num_threads().max(1);
        let chunk_size = slice.len().div_ceil(n_threads).max(1);
        let chunks: Vec<&[[u8; 64]]> = slice.chunks(chunk_size).collect();

        for window in chunks.windows(2) {
            let last_left = window[0].last().expect("non-empty chunk");
            let first_right = window[1].first().expect("non-empty chunk");
            if last_left == first_right {
                return Err(UnarchiveError::BadCanonicalizationRedundancy);
            }
            if last_left > first_right {
                return Err(UnarchiveError::BadCanonicalizationOrdering);
            }
        }

        let chunk_sets: Result<Vec<TribleSet>, UnarchiveError> = chunks
            .par_iter()
            .map(|chunk| serial_unarchive(chunk, owner.as_ref()))
            .collect();

        Ok(chunk_sets?
            .into_par_iter()
            .reduce(TribleSet::new, |left, right| left + right))
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
        assert_eq!(candidate.node_stats().0, baseline.node_stats().0);
    }

    fn assert_all_six_parity(candidate: &TribleSet, baseline: &TribleSet, len: usize) {
        assert_index_parity::<EAVOrder>(&candidate.eav, &baseline.eav, len);
        assert_index_parity::<EVAOrder>(&candidate.eva, &baseline.eva, len);
        assert_index_parity::<AEVOrder>(&candidate.aev, &baseline.aev, len);
        assert_index_parity::<AVEOrder>(&candidate.ave, &baseline.ave, len);
        assert_index_parity::<VEAOrder>(&candidate.vea, &baseline.vea, len);
        assert_index_parity::<VAEOrder>(&candidate.vae, &baseline.vae, len);
    }

    #[cfg(feature = "parallel")]
    fn assert_benchmark_semantic_parity(candidate: &TribleSet, baseline: &TribleSet, len: usize) {
        assert_eq!(candidate.len(), len);
        assert_eq!(baseline.len(), len);
        assert!(candidate.eav.iter_ordered().eq(baseline.eav.iter_ordered()));
        assert_eq!(candidate.eav.root_hash(), baseline.eav.root_hash());
        assert_eq!(candidate.eva.root_hash(), baseline.eva.root_hash());
        assert_eq!(candidate.aev.root_hash(), baseline.aev.root_hash());
        assert_eq!(candidate.ave.root_hash(), baseline.ave.root_hash());
        assert_eq!(candidate.vea.root_hash(), baseline.vea.root_hash());
        assert_eq!(candidate.vae.root_hash(), baseline.vae.root_hash());
    }

    #[test]
    fn bottom_up_all_six_matches_serial_topology_and_lifetime() {
        for len in [0usize, 1, 2, 3, 257, 8_192] {
            let blob = fixture_blob(len);
            let baseline = serial_for_test(blob.clone()).unwrap();
            let candidate = bottom_up_for_test(blob.clone()).unwrap();
            assert_all_six_parity(&candidate, &baseline, len);

            if len > 1 {
                for stats in [
                    candidate.eav.node_stats(),
                    candidate.eva.node_stats(),
                    candidate.aev.node_stats(),
                    candidate.ave.node_stats(),
                    candidate.vea.node_stats(),
                    candidate.vae.node_stats(),
                ] {
                    assert_eq!(stats.2, 0, "bottom-up build materialized heap leaves");
                    assert_eq!(stats.3, len as u64, "bottom-up build lost LocalLeaves");
                }
            }

            let survivor = candidate.clone();
            drop(candidate);
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
    fn bottom_up_owner_guards_cover_every_archive_branch() {
        let set = bottom_up_for_test(fixture_blob(8_192)).unwrap();
        for stats in [
            set.eav.archive_owner_placement_stats(),
            set.eva.archive_owner_placement_stats(),
            set.aev.archive_owner_placement_stats(),
            set.ave.archive_owner_placement_stats(),
            set.vea.archive_owner_placement_stats(),
            set.vae.archive_owner_placement_stats(),
        ] {
            assert!(stats.0 > 0, "fixture did not exercise direct LocalLeaves");
            assert_eq!(stats.1, 0, "a direct LocalLeaf has no owner guard");
            assert!(
                stats.2 > 0,
                "fixture did not exercise owning ancestor-only Branches",
            );
            assert_eq!(
                stats.3, 0,
                "an archive Branch has no owner for later LocalLeaf movement",
            );
        }
    }

    #[test]
    fn bottom_up_full_byte_fanout_matches_serial() {
        let rows = (0u16..=255)
            .map(|byte| {
                let mut row = [0u8; 64];
                row[0] = byte as u8;
                if byte == 0 {
                    row[15] = 1;
                }
                row[31] = 1;
                row
            })
            .collect::<Vec<_>>();
        assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
        let blob = blob_from_rows(rows);
        let baseline = serial_for_test(blob.clone()).unwrap();
        let candidate = bottom_up_for_test(blob).unwrap();
        assert_all_six_parity(&candidate, &baseline, 256);
        assert_eq!(candidate.eav.branch_fanout_histogram()[256], 1);
    }

    #[cfg(feature = "proptest")]
    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn arbitrary_canonical_rows_match_serial_in_all_six_orders(
                raw_rows in prop::collection::vec(
                    prop::collection::vec(any::<u8>(), 64),
                    0..128,
                ),
                shared_prefix_len in 0usize..64,
            ) {
                let mut rows = raw_rows
                    .into_iter()
                    .map(|bytes| {
                        let mut row: [u8; 64] = bytes.try_into().expect("fixed row width");
                        row[..shared_prefix_len].fill(0x5a);
                        if row[..16].iter().all(|byte| *byte == 0) {
                            row[15] = 1;
                        }
                        if row[16..32].iter().all(|byte| *byte == 0) {
                            row[31] = 1;
                        }
                        row
                    })
                    .collect::<Vec<_>>();
                rows.sort_unstable();
                rows.dedup();

                let len = rows.len();
                let blob = blob_from_rows(rows);
                let serial = serial_for_test(blob.clone()).unwrap();
                let bottom_up = bottom_up_for_test(blob).unwrap();
                assert_all_six_parity(&bottom_up, &serial, len);
            }
        }
    }

    #[test]
    fn bottom_up_validates_canonical_eav_input() {
        let first = fixture_row(0);
        let second = fixture_row(1);
        assert_eq!(
            bottom_up_for_test(blob_from_rows(vec![first, first])).unwrap_err(),
            UnarchiveError::BadCanonicalizationRedundancy,
        );
        assert_eq!(
            bottom_up_for_test(blob_from_rows(vec![second, first])).unwrap_err(),
            UnarchiveError::BadCanonicalizationOrdering,
        );
        let mut invalid = first;
        invalid[..16].fill(0);
        assert_eq!(
            bottom_up_for_test(blob_from_rows(vec![invalid])).unwrap_err(),
            UnarchiveError::BadTrible,
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn production_archive_matches_serial_and_retains_source() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                for len in [0usize, 1, 2, 3, 257, 4_095, 4_096, 8_192] {
                    let blob = fixture_blob(len);
                    let baseline = serial_for_test(blob.clone()).unwrap();
                    let candidate = TribleSet::try_from_blob(blob.clone()).unwrap();
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
            });
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn production_archive_preserves_errors_and_boundary_precedence() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                fn assert_public_error(rows: Vec<[u8; 64]>, expected: UnarchiveError) {
                    assert_eq!(
                        TribleSet::try_from_blob(blob_from_rows(rows)).unwrap_err(),
                        expected,
                    );
                }

                let len = PARALLEL_UNARCHIVE_THRESHOLD;
                let chunk_size = len.div_ceil(rayon::current_num_threads());

                let mut duplicate_inside = (0..len).map(fixture_row).collect::<Vec<_>>();
                duplicate_inside[1] = duplicate_inside[0];
                assert_public_error(
                    duplicate_inside,
                    UnarchiveError::BadCanonicalizationRedundancy,
                );

                let mut duplicate = (0..len).map(fixture_row).collect::<Vec<_>>();
                duplicate[chunk_size] = duplicate[chunk_size - 1];
                assert_public_error(duplicate, UnarchiveError::BadCanonicalizationRedundancy);

                let mut descending_inside = (0..len).map(fixture_row).collect::<Vec<_>>();
                descending_inside.swap(0, 1);
                assert_public_error(
                    descending_inside,
                    UnarchiveError::BadCanonicalizationOrdering,
                );

                let mut descending = (0..len).map(fixture_row).collect::<Vec<_>>();
                descending.swap(chunk_size - 1, chunk_size);
                assert_public_error(descending, UnarchiveError::BadCanonicalizationOrdering);

                let invalid = (0..len)
                    .map(|index| {
                        let mut row = [0u8; 64];
                        row[31] = 1;
                        row[56..64].copy_from_slice(&((index + 1) as u64).to_be_bytes());
                        row
                    })
                    .collect();
                assert_public_error(invalid, UnarchiveError::BadTrible);

                // Boundary errors are checked before worker validation.
                let mut invalid_and_descending = (0..len).map(fixture_row).collect::<Vec<_>>();
                invalid_and_descending[0][..16].fill(0);
                invalid_and_descending.swap(chunk_size - 1, chunk_size);
                assert_public_error(
                    invalid_and_descending,
                    UnarchiveError::BadCanonicalizationOrdering,
                );

                let malformed = Blob::new(Bytes::from(vec![0u8; 63]));
                assert_eq!(
                    TribleSet::try_from_blob(malformed).unwrap_err(),
                    UnarchiveError::BadArchive,
                );
            });
    }

    #[test]
    fn bottom_up_chunk_row_limit_is_exact() {
        assert!(bottom_up_chunk_rows_fit(u32::MAX as usize));
        #[cfg(target_pointer_width = "64")]
        assert!(!bottom_up_chunk_rows_fit(u32::MAX as usize + 1));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_heap_only_fallback_keeps_heap_leaves() {
        let len = PARALLEL_UNARCHIVE_THRESHOLD;
        let blob = fixture_blob(len);
        let archive_backed = TribleSet::try_from_blob(blob.clone()).unwrap();
        let heap_only = try_from_blob_heap_only(blob).unwrap();
        assert_all_six_parity(&heap_only, &archive_backed, len);

        for stats in [
            heap_only.eav.node_stats(),
            heap_only.eva.node_stats(),
            heap_only.aev.node_stats(),
            heap_only.ave.node_stats(),
            heap_only.vea.node_stats(),
            heap_only.vae.node_stats(),
        ] {
            assert_eq!(stats.2, len as u64, "heap fallback lost heap Leaves");
            assert_eq!(stats.3, 0, "heap fallback created LocalLeaves");
        }
    }

    /// Causal comparison of the former production chunk-online+union decoder
    /// and its bottom-up replacement in one release test binary.
    ///
    /// Fixture generation, canonical sorting, warmup/parity oracles, input
    /// cloning, and result destruction are outside every timed interval. The
    /// four-position `ABBA` order cycle gives each decoder the same number of
    /// first and second positions. The 4,095/4,096 cases expose the production
    /// threshold discontinuity; the larger cases exercise scale and distinct
    /// trie geometries.
    ///
    /// Run from a clean worktree with no competing compiler or benchmark:
    ///
    /// `cargo test -p triblespace-core --release --features parallel bottom_up_clean_causal_benchmark -- --ignored --nocapture --test-threads=1`
    ///
    /// The harness prints its exact executable path. Record `shasum -a 256` of
    /// that file externally alongside the output rather than putting file I/O
    /// in the benchmark process.
    #[cfg(feature = "parallel")]
    #[test]
    #[ignore = "manual clean-lineage bottom-up causal benchmark"]
    fn bottom_up_clean_causal_benchmark() {
        #[derive(Clone, Copy)]
        enum SampleOrder {
            LegacyFirst,
            BottomUpFirst,
        }

        fn timed_decode(
            input: Blob<SimpleArchive>,
            decode: impl FnOnce(Blob<SimpleArchive>) -> Result<TribleSet, UnarchiveError>,
        ) -> Duration {
            let start = Instant::now();
            let set = decode(black_box(input)).expect("canonical benchmark fixture must decode");
            let len = black_box(set.len());
            let elapsed = start.elapsed();
            black_box(len);
            drop(set);
            elapsed
        }

        fn median_seconds(samples: &mut [Duration]) -> f64 {
            samples.sort_unstable();
            let middle = samples.len() / 2;
            if samples.len() % 2 == 0 {
                (samples[middle - 1].as_secs_f64() + samples[middle].as_secs_f64()) / 2.0
            } else {
                samples[middle].as_secs_f64()
            }
        }

        fn sample_milliseconds(samples: &[Duration]) -> String {
            samples
                .iter()
                .map(|sample| format!("{:.3}", sample.as_secs_f64() * 1e3))
                .collect::<Vec<_>>()
                .join(",")
        }

        fn git_output(worktree: &std::path::Path, args: &[&str]) -> String {
            let Ok(output) = std::process::Command::new("git")
                .current_dir(worktree)
                .args(args)
                .output()
            else {
                return "unavailable".to_owned();
            };
            if !output.status.success() {
                return format!("error:{}", output.status);
            }
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }

        const BOTTOM_UP_INTRODUCTION: &str = "649d3d9b98ec2b8dd3ba1d5e5f369fe6f6b0f782";
        const CANDIDATE_PARENT: &str = "ab330e0fee695ae786ce2aa4c562ee8a5cad4b1a";
        const ORDER_CYCLE: [SampleOrder; 4] = [
            SampleOrder::LegacyFirst,
            SampleOrder::BottomUpFirst,
            SampleOrder::BottomUpFirst,
            SampleOrder::LegacyFirst,
        ];
        let worktree = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate must have a repository parent");
        let head = git_output(worktree, &["rev-parse", "HEAD"]);
        let dirty =
            !git_output(worktree, &["status", "--porcelain", "--untracked-files=no"]).is_empty();
        let executable = std::env::current_exe().expect("test executable path must be available");
        println!(
            "bottom_up_clean_context head={head} candidate_parent={CANDIDATE_PARENT} bottom_up_introduction={BOTTOM_UP_INTRODUCTION} worktree={} dirty={dirty} executable={} rayon_threads={} rayon_num_threads_env={} debug_assertions={} command={}",
            worktree.display(),
            executable.display(),
            rayon::current_num_threads(),
            std::env::var("RAYON_NUM_THREADS").unwrap_or_else(|_| "unset".to_owned()),
            cfg!(debug_assertions),
            "cargo test -p triblespace-core --release --features parallel bottom_up_clean_causal_benchmark -- --ignored --nocapture --test-threads=1",
        );
        println!(
            "bottom_up_clean_executable_hash_plan command=shasum_-a_256 path={}",
            executable.display(),
        );

        let cases = [
            (BenchmarkGeometry::EntityLike, 4_095usize, 8usize),
            (BenchmarkGeometry::EntityLike, 4_096, 8),
            (BenchmarkGeometry::EntityLike, 100_000, 8),
            (BenchmarkGeometry::EntityLike, 1_000_000, 4),
            (BenchmarkGeometry::HighEntropy, 1_000_000, 4),
            (BenchmarkGeometry::LongPrefixLowCardinality, 1_000_000, 4),
        ];

        for (geometry, len, sample_count) in cases {
            assert_eq!(sample_count % ORDER_CYCLE.len(), 0);
            let blob = benchmark_blob(geometry, len);

            // These calls also warm both implementations and initialize Rayon
            // before measurement. Parity traversal and root hashes are never
            // part of a timed sample.
            let legacy_oracle = legacy_try_from_blob_for_benchmark(blob.clone()).unwrap();
            let bottom_up_oracle = TribleSet::try_from_blob(blob.clone()).unwrap();
            assert_benchmark_semantic_parity(&bottom_up_oracle, &legacy_oracle, len);
            drop(bottom_up_oracle);
            drop(legacy_oracle);

            let mut legacy_samples = Vec::with_capacity(sample_count);
            let mut bottom_up_samples = Vec::with_capacity(sample_count);
            for round in 0..sample_count {
                let order = ORDER_CYCLE[round % ORDER_CYCLE.len()];
                let (legacy, bottom_up) = match order {
                    SampleOrder::LegacyFirst => {
                        let legacy = timed_decode(blob.clone(), legacy_try_from_blob_for_benchmark);
                        let bottom_up = timed_decode(blob.clone(), TribleSet::try_from_blob);
                        (legacy, bottom_up)
                    }
                    SampleOrder::BottomUpFirst => {
                        let bottom_up = timed_decode(blob.clone(), TribleSet::try_from_blob);
                        let legacy = timed_decode(blob.clone(), legacy_try_from_blob_for_benchmark);
                        (legacy, bottom_up)
                    }
                };
                legacy_samples.push(legacy);
                bottom_up_samples.push(bottom_up);
            }

            let legacy_raw = sample_milliseconds(&legacy_samples);
            let bottom_up_raw = sample_milliseconds(&bottom_up_samples);
            let legacy_seconds = median_seconds(&mut legacy_samples);
            let bottom_up_seconds = median_seconds(&mut bottom_up_samples);
            let candidate_regime = if len < PARALLEL_UNARCHIVE_THRESHOLD {
                "serial"
            } else {
                "parallel_bottom_up"
            };
            println!(
                "bottom_up_clean_case geometry={} len={len} samples={sample_count} order_cycle=ABBA candidate_regime={candidate_regime} legacy_median_ms={:.3} bottom_up_median_ms={:.3} speedup={:.3}x legacy_mtribles_per_s={:.3} bottom_up_mtribles_per_s={:.3} legacy_samples_ms=[{legacy_raw}] bottom_up_samples_ms=[{bottom_up_raw}]",
                geometry.name(),
                legacy_seconds * 1e3,
                bottom_up_seconds * 1e3,
                legacy_seconds / bottom_up_seconds,
                len as f64 / legacy_seconds / 1e6,
                len as f64 / bottom_up_seconds / 1e6,
            );
        }
    }
}
