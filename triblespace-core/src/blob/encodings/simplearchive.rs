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
    let mut hashes = Vec::with_capacity(slice.len());
    let mut prev_trible: Option<&[u8; 64]> = None;
    for row in slice {
        if Trible::as_transmute_force_raw(row).is_none() {
            return Err(UnarchiveError::BadTrible);
        }
        if let Some(previous) = prev_trible {
            if previous == row {
                return Err(UnarchiveError::BadCanonicalizationRedundancy);
            }
            if previous > row {
                return Err(UnarchiveError::BadCanonicalizationOrdering);
            }
        }
        prev_trible = Some(row);
        hashes.push(hash_key(&row[..]));
    }
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
    use crate::patch::{
        composition_probe_counters, reset_composition_probe_counters, KeySchema, PATCH,
    };
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

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum ReceiptState {
        RootOnly,
        AllInternal,
    }

    impl ReceiptState {
        fn name(self) -> &'static str {
            match self {
                Self::RootOnly => "root_only",
                Self::AllInternal => "all_internal",
            }
        }
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum CompositionOperation {
        Union,
        Intersect,
        Difference,
    }

    impl CompositionOperation {
        fn name(self) -> &'static str {
            match self {
                Self::Union => "union",
                Self::Intersect => "intersect",
                Self::Difference => "difference",
            }
        }

        fn apply(self, mut left: TribleSet, right: TribleSet) -> TribleSet {
            match self {
                Self::Union => {
                    left.union(right);
                    left
                }
                Self::Intersect => left.intersect(&right),
                Self::Difference => left.difference(&right),
            }
        }

        fn expected_rows(self, left: &[[u8; 64]], right: &[[u8; 64]]) -> Vec<[u8; 64]> {
            match self {
                Self::Union => {
                    let mut rows = Vec::with_capacity(left.len() + right.len());
                    rows.extend_from_slice(left);
                    rows.extend_from_slice(right);
                    rows.sort_unstable();
                    rows.dedup();
                    rows
                }
                Self::Intersect => left
                    .iter()
                    .copied()
                    .filter(|row| right.binary_search(row).is_ok())
                    .collect(),
                Self::Difference => left
                    .iter()
                    .copied()
                    .filter(|row| right.binary_search(row).is_err())
                    .collect(),
            }
        }
    }

    struct CompositionGeometry {
        name: &'static str,
        left: Vec<[u8; 64]>,
        right: Vec<[u8; 64]>,
        operations: &'static [CompositionOperation],
    }

    fn composition_row(prefix: u8, lane: u8, ordinal: u32) -> [u8; 64] {
        assert_ne!(prefix, 0, "entity and attribute ids must remain non-nil");
        let mut row = [0u8; 64];
        for start in [0usize, 16, 32] {
            row[start] = prefix;
            row[start + 1] = lane;
            row[start + 2..start + 6].copy_from_slice(&ordinal.to_be_bytes());
        }
        row
    }

    fn region_rows(prefix: u8, len: usize) -> Vec<[u8; 64]> {
        (0..len)
            .map(|ordinal| composition_row(prefix, 1, ordinal as u32))
            .collect()
    }

    fn composition_geometries() -> Vec<CompositionGeometry> {
        const UNION_ONLY: &[CompositionOperation] = &[CompositionOperation::Union];
        const ALL_SET_OPERATIONS: &[CompositionOperation] = &[
            CompositionOperation::Union,
            CompositionOperation::Intersect,
            CompositionOperation::Difference,
        ];

        // Two adjacent slices of one canonical EAV sequence model the reduction
        // inputs produced by `parallel_unarchive`. They are globally disjoint,
        // but their A/V-first indexes still collide structurally.
        let canonical_left = (0..512).map(fixture_row).collect();
        let canonical_right = (512..1_024).map(fixture_row).collect();

        // S, L, and R are complete first-byte regions in every index because
        // each segment carries the same routing prefix. A=S+L and B=S+R expose
        // one large, semantically equal Branch per index while preventing every
        // PATCH-level cardinality donation law.
        let shared = region_rows(0x20, 256);
        let left_only = region_rows(0x40, 256);
        let right_only = region_rows(0x60, 256);
        let mut aligned_left = shared.clone();
        aligned_left.extend_from_slice(&left_only);
        aligned_left.sort_unstable();
        let mut aligned_right = shared;
        aligned_right.extend_from_slice(&right_only);
        aligned_right.sort_unstable();

        // Every top-level bucket contains one shared and one side-specific
        // LocalLeaf. No equal non-singleton subtree exists, so resident Branch
        // receipts cannot prune; the exact leaf collision poisons the rebuilt
        // path in both receipt states.
        let mut scattered_left = Vec::with_capacity(128);
        let mut scattered_right = Vec::with_capacity(128);
        for bucket in 1u8..=64 {
            let shared = composition_row(bucket, 0x10, 0);
            scattered_left.extend([shared, composition_row(bucket, 0x20, 0)]);
            scattered_right.extend([shared, composition_row(bucket, 0x30, 0)]);
        }
        scattered_left.sort_unstable();
        scattered_right.sort_unstable();

        vec![
            CompositionGeometry {
                name: "canonical_disjoint_chunks",
                left: canonical_left,
                right: canonical_right,
                operations: UNION_ONLY,
            },
            CompositionGeometry {
                name: "branch_aligned_partial_overlap",
                left: aligned_left,
                right: aligned_right,
                operations: ALL_SET_OPERATIONS,
            },
            CompositionGeometry {
                name: "leaf_scattered_partial_overlap",
                left: scattered_left,
                right: scattered_right,
                operations: ALL_SET_OPERATIONS,
            },
        ]
    }

    fn normalize_root_only_receipts(set: &mut TribleSet) {
        set.eav.normalize_root_only_receipt_for_test();
        set.eva.normalize_root_only_receipt_for_test();
        set.aev.normalize_root_only_receipt_for_test();
        set.ave.normalize_root_only_receipt_for_test();
        set.vea.normalize_root_only_receipt_for_test();
        set.vae.normalize_root_only_receipt_for_test();
    }

    fn tribleset_receipt_stats(set: &TribleSet) -> (usize, usize) {
        [
            set.eav.branch_receipt_stats_for_test(),
            set.eva.branch_receipt_stats_for_test(),
            set.aev.branch_receipt_stats_for_test(),
            set.ave.branch_receipt_stats_for_test(),
            set.vea.branch_receipt_stats_for_test(),
            set.vae.branch_receipt_stats_for_test(),
        ]
        .into_iter()
        .fold((0, 0), |(known, dirty), (index_known, index_dirty)| {
            (known + index_known, dirty + index_dirty)
        })
    }

    fn all_roots_known(set: &TribleSet) -> bool {
        set.eav.root_receipt_is_known_for_test()
            && set.eva.root_receipt_is_known_for_test()
            && set.aev.root_receipt_is_known_for_test()
            && set.ave.root_receipt_is_known_for_test()
            && set.vea.root_receipt_is_known_for_test()
            && set.vae.root_receipt_is_known_for_test()
    }

    fn build_composition_set(rows: &[[u8; 64]], state: ReceiptState) -> TribleSet {
        assert!(rows.windows(2).all(|pair| pair[0] < pair[1]));
        let mut set = try_from_blob_bottom_up_for_test(blob_from_rows(rows.to_vec()))
            .expect("composition fixture must be a canonical archive")
            .set;
        let all_internal = tribleset_receipt_stats(&set);
        assert!(all_internal.0 > 6, "fixture needs Branch descendants");
        assert_eq!(all_internal.1, 0, "bottom-up fixture must be fully known");
        assert!(all_roots_known(&set));

        if state == ReceiptState::RootOnly {
            normalize_root_only_receipts(&mut set);
            let root_only = tribleset_receipt_stats(&set);
            assert_eq!(root_only.0, 6, "only the six roots should remain known");
            assert!(root_only.1 > 0, "root-only control needs dirty descendants");
            assert!(all_roots_known(&set));
        }
        set
    }

    #[derive(Copy, Clone)]
    struct CompositionSample {
        structure: Duration,
        first_fingerprint: Duration,
        second_fingerprint: Duration,
        head_visits: usize,
        equality_prunes: usize,
        structure_leaf_hashes: usize,
        first_leaf_hashes: usize,
        second_leaf_hashes: usize,
        result_root_known: bool,
    }

    fn composition_sample(
        geometry: &CompositionGeometry,
        operation: CompositionOperation,
        state: ReceiptState,
        expected: &[[u8; 64]],
    ) -> CompositionSample {
        let left = build_composition_set(&geometry.left, state);
        let right = build_composition_set(&geometry.right, state);

        reset_composition_probe_counters();
        let structure_start = Instant::now();
        let result = operation.apply(black_box(left), black_box(right));
        let structure = structure_start.elapsed();
        let after_structure = composition_probe_counters();
        let result_root_known = result.eav.root_receipt_is_known_for_test();

        let first_start = Instant::now();
        let first = black_box(result.fingerprint());
        let first_fingerprint = first_start.elapsed();
        let after_first = composition_probe_counters();

        let second_start = Instant::now();
        let second = black_box(result.fingerprint());
        let second_fingerprint = second_start.elapsed();
        let after_second = composition_probe_counters();

        assert_eq!(first, second);
        assert_eq!(result.len(), expected.len());
        assert_eq!(
            result.eav.iter_ordered().copied().collect::<Vec<_>>(),
            expected,
        );

        CompositionSample {
            structure,
            first_fingerprint,
            second_fingerprint,
            head_visits: after_structure.0,
            equality_prunes: after_structure.1,
            structure_leaf_hashes: after_structure.2,
            first_leaf_hashes: after_first.2 - after_structure.2,
            second_leaf_hashes: after_second.2 - after_first.2,
            result_root_known,
        }
    }

    fn median_duration(
        samples: &[CompositionSample],
        project: impl Fn(&CompositionSample) -> Duration,
    ) -> Duration {
        let mut values: Vec<_> = samples.iter().map(project).collect();
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn summarize_composition_samples(samples: &[CompositionSample]) -> CompositionSample {
        let causal = samples[0];
        assert!(samples.iter().all(|sample| {
            sample.head_visits == causal.head_visits
                && sample.equality_prunes == causal.equality_prunes
                && sample.structure_leaf_hashes == causal.structure_leaf_hashes
                && sample.first_leaf_hashes == causal.first_leaf_hashes
                && sample.second_leaf_hashes == causal.second_leaf_hashes
                && sample.result_root_known == causal.result_root_known
        }));
        CompositionSample {
            structure: median_duration(samples, |sample| sample.structure),
            first_fingerprint: median_duration(samples, |sample| sample.first_fingerprint),
            second_fingerprint: median_duration(samples, |sample| sample.second_fingerprint),
            ..causal
        }
    }

    fn assert_composition_prediction(
        geometry: &CompositionGeometry,
        state: ReceiptState,
        expected_len: usize,
        sample: &CompositionSample,
    ) {
        assert_eq!(
            sample.structure_leaf_hashes, 0,
            "structural composition must remain hash-lazy",
        );
        assert_eq!(
            sample.second_leaf_hashes, 0,
            "second fingerprint is memoized"
        );

        match geometry.name {
            "canonical_disjoint_chunks" => {
                assert_eq!(sample.equality_prunes, 0);
                assert!(sample.result_root_known);
                assert_eq!(sample.first_leaf_hashes, 0);
            }
            "branch_aligned_partial_overlap" => match state {
                ReceiptState::AllInternal => {
                    assert_eq!(sample.equality_prunes, 6, "one shared Branch per index");
                    assert!(sample.result_root_known);
                    assert_eq!(sample.first_leaf_hashes, 0);
                }
                ReceiptState::RootOnly => {
                    assert_eq!(sample.equality_prunes, 0);
                    assert!(!sample.result_root_known);
                    assert_eq!(sample.first_leaf_hashes, expected_len);
                }
            },
            "leaf_scattered_partial_overlap" => {
                assert_eq!(sample.equality_prunes, 0);
                assert!(!sample.result_root_known);
                assert_eq!(sample.first_leaf_hashes, expected_len);
            }
            _ => unreachable!("unknown composition geometry"),
        }
    }

    fn print_composition_summary(
        geometry: &CompositionGeometry,
        operation: CompositionOperation,
        state: ReceiptState,
        expected_len: usize,
        sample: CompositionSample,
    ) {
        println!(
            "bottom_up_receipt_composition geometry={} operation={} state={} left_rows={} right_rows={} result_rows={} structure_us={:.3} head_visits={} equality_prunes={} structure_leaf_hashes={} result_root_known={} first_fingerprint_us={:.3} first_leaf_hashes={} second_fingerprint_us={:.3} second_leaf_hashes={}",
            geometry.name,
            operation.name(),
            state.name(),
            geometry.left.len(),
            geometry.right.len(),
            expected_len,
            sample.structure.as_secs_f64() * 1e6,
            sample.head_visits,
            sample.equality_prunes,
            sample.structure_leaf_hashes,
            sample.result_root_known,
            sample.first_fingerprint.as_secs_f64() * 1e6,
            sample.first_leaf_hashes,
            sample.second_fingerprint.as_secs_f64() * 1e6,
            sample.second_leaf_hashes,
        );
    }

    /// Causal matrix for descendant receipt value during composition. Run with:
    ///
    /// `cargo test -p triblespace-core --release bottom_up_receipt_composition_matrix -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore = "manual receipt-composition benchmark"]
    fn bottom_up_receipt_composition_matrix() {
        const ROUNDS: usize = 7;

        for geometry in composition_geometries() {
            for &operation in geometry.operations {
                let expected = operation.expected_rows(&geometry.left, &geometry.right);
                let mut root_only_samples = Vec::with_capacity(ROUNDS);
                let mut all_internal_samples = Vec::with_capacity(ROUNDS);

                for round in 0..ROUNDS {
                    let root_only = || {
                        composition_sample(&geometry, operation, ReceiptState::RootOnly, &expected)
                    };
                    let all_internal = || {
                        composition_sample(
                            &geometry,
                            operation,
                            ReceiptState::AllInternal,
                            &expected,
                        )
                    };
                    if round % 2 == 0 {
                        root_only_samples.push(root_only());
                        all_internal_samples.push(all_internal());
                    } else {
                        all_internal_samples.push(all_internal());
                        root_only_samples.push(root_only());
                    }
                }

                let root_only = summarize_composition_samples(&root_only_samples);
                let all_internal = summarize_composition_samples(&all_internal_samples);
                assert_composition_prediction(
                    &geometry,
                    ReceiptState::RootOnly,
                    expected.len(),
                    &root_only,
                );
                assert_composition_prediction(
                    &geometry,
                    ReceiptState::AllInternal,
                    expected.len(),
                    &all_internal,
                );

                match geometry.name {
                    "branch_aligned_partial_overlap" => assert!(
                        all_internal.head_visits < root_only.head_visits,
                        "resident shared-Branch receipts must prune structural descent",
                    ),
                    "canonical_disjoint_chunks" | "leaf_scattered_partial_overlap" => assert_eq!(
                        all_internal.head_visits, root_only.head_visits,
                        "without an equal Branch, receipts must not change structural descent",
                    ),
                    _ => unreachable!(),
                }

                print_composition_summary(
                    &geometry,
                    operation,
                    ReceiptState::RootOnly,
                    expected.len(),
                    root_only,
                );
                print_composition_summary(
                    &geometry,
                    operation,
                    ReceiptState::AllInternal,
                    expected.len(),
                    all_internal,
                );
            }
        }
    }
}
