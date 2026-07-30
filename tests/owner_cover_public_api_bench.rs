//! Manual public-API probe for archive-owner cover composition.
//!
//! This deliberately lives as an ignored integration test: it uses no PATCH
//! internals and can be cherry-picked unchanged across competing owner-cover
//! implementations. Run it in release mode with one test thread; the printed
//! labels are stable CSV-shaped records suitable for an A/B transcript.

use std::hint::black_box;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};
use std::time::Instant;

use triblespace::core::patch::{ArchiveEntry, ArchiveOwner};
use triblespace::core::trible::{Trible, TribleSet};

const SET_COUNT: usize = 512;
const ROWS_PER_SET: usize = 3;
const TOTAL_ROWS: usize = SET_COUNT * ROWS_PER_SET;
const WARMUP_ROUNDS: usize = 3;
const SAMPLE_COUNT: usize = 11;

#[derive(Clone, Copy)]
#[repr(C, align(16))]
struct AlignedRow([u8; 64]);

#[repr(C, align(16))]
struct DistinctOwnerRows([AlignedRow; ROWS_PER_SET]);

fn valid_row(set_index: usize, row_index: usize) -> [u8; 64] {
    let ordinal = (set_index * ROWS_PER_SET + row_index + 1) as u64;
    let mut row = [0u8; 64];

    // Non-nil 16-byte entity and attribute domains. The ordinal also makes
    // every complete EAV row globally unique while leaving three nearby rows
    // per source set for a repeatable compressed-trie geometry.
    row[0] = 0xe1;
    row[8..16].copy_from_slice(&ordinal.to_be_bytes());
    row[16] = 0xa7;
    row[24..32].copy_from_slice(&(row_index as u64 + 1).to_be_bytes());
    row[32..40].copy_from_slice(&ordinal.to_be_bytes());
    row[40..48].copy_from_slice(&ordinal.wrapping_mul(0x9e37_79b9).to_be_bytes());
    row[48..56].copy_from_slice(&ordinal.rotate_left(17).to_be_bytes());
    row[56..64].copy_from_slice(&(!ordinal).to_be_bytes());

    assert!(
        Trible::force_raw(row).is_some(),
        "benchmark rows must be valid tribles",
    );
    row
}

fn insert_archive_rows(set: &mut TribleSet, rows: &[AlignedRow], owner: &Arc<dyn ArchiveOwner>) {
    for row in rows {
        let ptr = NonNull::from(&row.0);
        assert_eq!(
            ptr.as_ptr() as usize & 0x0f,
            0,
            "archive rows must remain 16-byte aligned",
        );
        // SAFETY: `AlignedRow` gives every immutable row 16-byte alignment.
        // `owner` retains the complete allocation, and PATCH adopts that owner
        // before the returned entry's pointer can outlive this call.
        let entry = unsafe { ArchiveEntry::new(ptr, owner) };
        set.insert_archive(&entry);
    }
}

fn distinct_owner_sources() -> (Vec<TribleSet>, Vec<Weak<DistinctOwnerRows>>) {
    let mut sets = Vec::with_capacity(SET_COUNT);
    let mut witnesses = Vec::with_capacity(SET_COUNT);

    for set_index in 0..SET_COUNT {
        let storage = Arc::new(DistinctOwnerRows(std::array::from_fn(|row_index| {
            AlignedRow(valid_row(set_index, row_index))
        })));
        let witness = Arc::downgrade(&storage);
        let owner: Arc<dyn ArchiveOwner> = storage.clone();
        let mut set = TribleSet::new();
        insert_archive_rows(&mut set, &storage.0, &owner);
        assert_eq!(set.len(), ROWS_PER_SET);

        // From here onward, only the PATCH owner cover retains this allocation.
        drop(owner);
        drop(storage);
        assert!(witness.upgrade().is_some());
        sets.push(set);
        witnesses.push(witness);
    }

    (sets, witnesses)
}

fn same_owner_sources() -> (Vec<TribleSet>, Weak<Vec<AlignedRow>>) {
    let storage = Arc::new(
        (0..SET_COUNT)
            .flat_map(|set_index| {
                (0..ROWS_PER_SET).map(move |row_index| AlignedRow(valid_row(set_index, row_index)))
            })
            .collect::<Vec<_>>(),
    );
    let witness = Arc::downgrade(&storage);
    let owner: Arc<dyn ArchiveOwner> = storage.clone();
    let mut sets = Vec::with_capacity(SET_COUNT);

    for set_index in 0..SET_COUNT {
        let begin = set_index * ROWS_PER_SET;
        let mut set = TribleSet::new();
        insert_archive_rows(&mut set, &storage[begin..begin + ROWS_PER_SET], &owner);
        assert_eq!(set.len(), ROWS_PER_SET);
        sets.push(set);
    }

    // All 512 source covers now retain the one shared allocation; neither
    // typed nor erased fixture ownership remains outside the PATCHes.
    drop(owner);
    drop(storage);
    assert!(witness.upgrade().is_some());
    (sets, witness)
}

fn sequential_fold(sources: &[TribleSet]) -> TribleSet {
    let mut result = TribleSet::new();
    for source in black_box(sources) {
        result.union(black_box(source.clone()));
    }
    result
}

fn balanced_fold(sources: &[TribleSet]) -> TribleSet {
    let mut level = black_box(sources).to_vec();
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        let mut current = level.into_iter();
        while let Some(mut left) = current.next() {
            if let Some(right) = current.next() {
                left.union(black_box(right));
            }
            next.push(left);
        }
        level = next;
    }
    level.pop().unwrap_or_default()
}

fn measure(
    owner_shape: &str,
    fold_shape: &str,
    sources: &[TribleSet],
    fold: fn(&[TribleSet]) -> TribleSet,
) {
    const { assert!(SAMPLE_COUNT % 2 == 1) };

    for _ in 0..WARMUP_ROUNDS {
        let result = fold(black_box(sources));
        assert_eq!(black_box(result.len()), TOTAL_ROWS);
    }

    let mut samples = [0u128; SAMPLE_COUNT];
    for sample in &mut samples {
        let started = Instant::now();
        let result = fold(black_box(sources));
        *sample = started.elapsed().as_nanos();
        assert_eq!(black_box(result.len()), TOTAL_ROWS);
        black_box(&result);
    }
    samples.sort_unstable();
    let median_ns = samples[SAMPLE_COUNT / 2];
    let median_us = median_ns as f64 / 1_000.0;
    println!(
        "OWNER_COVER_PUBLIC_API_BENCH,owner_shape={owner_shape},fold={fold_shape},sets={SET_COUNT},rows_per_set={ROWS_PER_SET},samples={SAMPLE_COUNT},median_ns={median_ns},median_us={median_us:.3}",
    );
}

#[test]
#[ignore = "manual release-mode owner-cover A/B probe"]
fn owner_cover_public_api_benchmark() {
    let (distinct_sets, distinct_witnesses) = distinct_owner_sources();
    let (same_sets, same_witness) = same_owner_sources();

    measure("distinct", "sequential", &distinct_sets, sequential_fold);
    measure("distinct", "balanced", &distinct_sets, balanced_fold);
    measure("same", "sequential", &same_sets, sequential_fold);
    measure("same", "balanced", &same_sets, balanced_fold);

    drop(distinct_sets);
    assert!(
        distinct_witnesses
            .iter()
            .all(|witness| witness.upgrade().is_none()),
        "dropping every distinct-owner source must release every allocation",
    );
    drop(same_sets);
    assert!(
        same_witness.upgrade().is_none(),
        "dropping every same-owner source must release its shared allocation",
    );
}
