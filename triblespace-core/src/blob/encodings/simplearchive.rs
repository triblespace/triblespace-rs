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

/// Validates one canonical EAV archive, hashes each row exactly once, and then
/// constructs all six PATCH indexes through the production in-place MSD radix
/// builder while retaining phase timings for the manual benchmark.
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
    let hashes = validate_and_hash_archive_slice(slice)?;
    let validation_and_hash = validation_start.elapsed();
    let row_hashes = hashes.len();
    let hash_bytes = hashes.capacity() * std::mem::size_of::<u128>();

    let build_start = std::time::Instant::now();
    let set = unsafe { TribleSet::from_archive_partition(slice, &hashes, &owner) };
    let permutation_bytes = slice.len() * std::mem::size_of::<u32>();
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

#[cfg(any(test, feature = "parallel"))]
#[inline]
fn bottom_up_chunk_rows_fit(chunk_size: usize) -> bool {
    u32::try_from(chunk_size).is_ok()
}

/// Parallel unarchive: chunk the blob, verify boundary ordering, validate and
/// build per-chunk `TribleSet`s in parallel, then reduce via
/// `TribleSet::union` (which itself fans out across the six indexes — three
/// levels of parallelism stacked).
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

    // Phase 2: build each aligned, u32-addressable chunk bottom-up. Unaligned
    // input keeps the heap-Leaf worker, and an impractically large chunk keeps
    // the online archive worker. Every archive-backed chunk shares the same
    // owner, so persistent owner-cover union later deduplicates the guard.
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
        if !rows.is_empty() {
            assert_eq!(
                rows.as_ptr() as usize & 0x0f,
                0,
                "archive fixture allocation must support LocalLeaves",
            );
        }
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    fn blob_from_rows(rows: Vec<[u8; 64]>) -> Blob<SimpleArchive> {
        let bytes: Bytes = rows.into();
        Blob::new(bytes)
    }

    fn assert_index_logical_parity<O: KeySchema<64>>(
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
    }

    fn assert_index_parity<O: KeySchema<64>>(
        candidate: &PATCH<64, O>,
        baseline: &PATCH<64, O>,
        len: usize,
    ) {
        assert_index_logical_parity(candidate, baseline, len);
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
    fn production_archive_matches_serial_and_retains_source() {
        for len in [0usize, 1, 2, 3, 257, 4_095, 4_096, 8_192] {
            let blob = fixture_blob(len);
            let baseline = try_from_blob_serial_for_test(blob.clone()).unwrap();
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
                    let blob = blob_from_rows(rows);
                    assert_eq!(
                        TribleSet::try_from_blob(blob)
                            .expect_err("public decoder must reject invalid input"),
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

                // Boundary checks intentionally precede worker validation.
                // Preserve the public ordering error even though a worker
                // would otherwise reject the first row as malformed.
                let mut invalid_and_descending = (0..len).map(fixture_row).collect::<Vec<_>>();
                invalid_and_descending[0][..16].fill(0);
                invalid_and_descending.swap(chunk_size - 1, chunk_size);
                assert_public_error(
                    invalid_and_descending,
                    UnarchiveError::BadCanonicalizationOrdering,
                );

                let malformed = Blob::new(Bytes::from(vec![0u8; 63]));
                assert_eq!(
                    TribleSet::try_from_blob(malformed)
                        .expect_err("public decoder must reject malformed bytes"),
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

        assert_index_logical_parity(&heap_only.eav, &archive_backed.eav, len);
        assert_index_logical_parity(&heap_only.eva, &archive_backed.eva, len);
        assert_index_logical_parity(&heap_only.aev, &archive_backed.aev, len);
        assert_index_logical_parity(&heap_only.ave, &archive_backed.ave, len);
        assert_index_logical_parity(&heap_only.vea, &archive_backed.vea, len);
        assert_index_logical_parity(&heap_only.vae, &archive_backed.vae, len);

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
}
