//! Public-API benchmark fixtures for PATCH hash-receipt experiments.
//!
//! Archive-backed cases are built from deterministic `Trible`s, encoded as a
//! `SimpleArchive`, and decoded back through `TribleSet::eav`. Clean controls
//! use ordinary `Entry` insertion over equally deterministic trible bytes.
//! Neither path reaches through PATCH internals.

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use triblespace::core::blob::encodings::simplearchive::SimpleArchive;
use triblespace::core::blob::Blob;
use triblespace::core::inline::Encodes;
use triblespace::core::patch::{Entry, PATCH};
use triblespace::core::trible::{EAVOrder, Trible, TribleSet, TRIBLE_LEN};

const BUCKETS: u8 = 128;
const REMOVAL_VARIANTS: u8 = 16;
const DIFFERENCE_VARIANTS: u8 = 32;

type EavPatch = PATCH<TRIBLE_LEN, EAVOrder, ()>;

fn raw_trible(bucket: u8, variant: u8) -> [u8; TRIBLE_LEN] {
    let mut data = [0u8; TRIBLE_LEN];

    // Give every E/A/V segment the same two-byte trie prefix. The final byte
    // keeps entity and attribute non-nil even for (bucket=0, variant=0).
    data[0] = bucket;
    data[1] = variant;
    data[15] = 0xe1;
    data[16] = bucket;
    data[17] = variant;
    data[31] = 0xa1;
    data[32] = bucket;
    data[33] = variant;
    data[63] = 0x51;

    data
}

fn raw_clean_trible(side: u8, index: u32) -> [u8; TRIBLE_LEN] {
    let mut data = [0u8; TRIBLE_LEN];
    let bucket = (index % BUCKETS as u32) as u8;
    let serial = index.to_be_bytes();

    for (start, end, sentinel) in [(0, 15, 0xe1), (16, 31, 0xa1), (32, 63, 0x51)] {
        data[start] = bucket;
        data[start + 1] = side;
        data[start + 2..start + 6].copy_from_slice(&serial);
        data[end] = sentinel;
    }

    data
}

fn ordered_clean_rows(side: u8, len: usize) -> Vec<[u8; TRIBLE_LEN]> {
    assert!(u32::try_from(len).is_ok());
    let mut rows: Vec<_> = (0..len)
        .map(|index| raw_clean_trible(side, index as u32))
        .collect();
    rows.sort_unstable();
    rows
}

fn ordered_archive_rows(
    bucket_start: u16,
    bucket_count: u16,
    variant_start: u8,
    variant_count: u8,
) -> Vec<[u8; TRIBLE_LEN]> {
    assert!(bucket_start + bucket_count <= 256);
    assert!(variant_start as u16 + variant_count as u16 <= 256);
    let mut rows = Vec::with_capacity(bucket_count as usize * variant_count as usize);
    for variant_offset in 0..variant_count {
        let variant = variant_start + variant_offset;
        for bucket in bucket_start..bucket_start + bucket_count {
            rows.push(raw_trible(bucket as u8, variant));
        }
    }
    rows.sort_unstable();
    rows
}

fn ordered_rows(variant_count: u8) -> Vec<[u8; TRIBLE_LEN]> {
    ordered_archive_rows(0, BUCKETS as u16, 0, variant_count)
}

fn heap_tribleset(rows: &[[u8; TRIBLE_LEN]]) -> TribleSet {
    let mut source = TribleSet::new();
    for row in rows {
        let trible = Trible::force_raw(*row).expect("fixture trible must be valid");
        source.insert(&trible);
    }
    source
}

fn archive_tribleset(rows: &[[u8; TRIBLE_LEN]]) -> TribleSet {
    let source = heap_tribleset(rows);
    let archive: Blob<SimpleArchive> = SimpleArchive::encode(&source);
    archive
        .try_from_blob()
        .expect("fixture archive must decode")
}

fn archive_patch(rows: &[[u8; TRIBLE_LEN]]) -> EavPatch {
    archive_tribleset(rows).eav
}

/// Turn an exact archive template into a semantically identical dirty one
/// using only public operations.
///
/// A fresh archive singleton is used only to subtract the duplicate from the
/// exact fixture. Subtracting that remainder from the fixture derives the same
/// singleton LocalLeaf under the fixture's existing owner cover. Unioning the
/// derived leaf back cannot change the rows, but the exact union formula cannot
/// retain the parent's fingerprint without both input fingerprints. This is
/// the fixture shape needed to exercise an undemanded parallel union without
/// adding another archive owner to the benchmark operand.
fn demote_via_duplicate(mut exact: EavPatch, duplicate: [u8; TRIBLE_LEN]) -> EavPatch {
    assert!(exact.iter().any(|row| row == &duplicate));
    let expected_len = exact.len();
    let singleton_rows = std::slice::from_ref(&duplicate);
    let fresh = archive_patch(singleton_rows);
    assert_local_rows(
        "PATCH duplicate-demotion fresh singleton",
        &fresh,
        singleton_rows,
    );

    // PATCH::difference retains only the left operand's owners. The second
    // difference therefore isolates a pointer-equal LocalLeaf covered by
    // `exact`, rather than carrying the fresh singleton's 33rd owner forward.
    let without = exact.difference(&fresh);
    assert_eq!(without.len(), expected_len - 1);
    let derived = exact.difference(&without);
    assert_local_rows(
        "PATCH duplicate-demotion derived singleton",
        &derived,
        singleton_rows,
    );
    assert_eq!(derived.node_stats(), (0, 0, 0, 1));
    drop(without);
    drop(fresh);

    exact.union(derived);
    assert_eq!(exact.len(), expected_len);
    let stats = exact.node_stats();
    assert_eq!(stats.2, 0);
    assert_eq!(stats.3, expected_len);
    exact
}

fn archive_variant(bucket_start: u16, bucket_count: u16, variant: u8) -> EavPatch {
    let rows = ordered_archive_rows(bucket_start, bucket_count, variant, 1);
    archive_patch(&rows)
}

fn balanced_merge<T>(mut values: Vec<T>, mut merge: impl FnMut(&mut T, T)) -> T {
    assert!(
        !values.is_empty(),
        "balanced merge needs at least one value"
    );
    while values.len() > 1 {
        let mut next = Vec::with_capacity(values.len().div_ceil(2));
        let mut iter = values.into_iter();
        while let Some(mut left) = iter.next() {
            if let Some(right) = iter.next() {
                merge(&mut left, right);
            }
            next.push(left);
        }
        values = next;
    }
    values.pop().expect("non-empty merge round")
}

fn archive_variants_in(
    bucket_start: u16,
    bucket_count: u16,
    variant_start: u8,
    variant_count: u8,
) -> EavPatch {
    balanced_merge(
        (variant_start as u16..variant_start as u16 + variant_count as u16)
            .map(|variant| archive_variant(bucket_start, bucket_count, variant as u8))
            .collect(),
        |left, right| left.union(right),
    )
}

fn archive_variants(variant_count: u8) -> EavPatch {
    archive_variants_in(0, BUCKETS as u16, 0, variant_count)
}

fn archive_tribleset_variant(bucket_start: u16, bucket_count: u16, variant: u8) -> TribleSet {
    let rows = ordered_archive_rows(bucket_start, bucket_count, variant, 1);
    archive_tribleset(&rows)
}

fn archive_tribleset_variants_in(
    bucket_start: u16,
    bucket_count: u16,
    variant_start: u8,
    variant_count: u8,
) -> TribleSet {
    balanced_merge(
        (variant_start as u16..variant_start as u16 + variant_count as u16)
            .map(|variant| archive_tribleset_variant(bucket_start, bucket_count, variant as u8))
            .collect(),
        |left, right| left.union(right),
    )
}

fn archive_tribleset_variants(variant_count: u8) -> TribleSet {
    archive_tribleset_variants_in(0, BUCKETS as u16, 0, variant_count)
}

fn heap_oracle(rows: &[[u8; TRIBLE_LEN]]) -> EavPatch {
    let mut oracle = EavPatch::new();
    for row in rows {
        oracle.insert(&Entry::new(row));
    }
    oracle
}

fn union_rows(left: &[[u8; TRIBLE_LEN]], right: &[[u8; TRIBLE_LEN]]) -> Vec<[u8; TRIBLE_LEN]> {
    let mut rows = Vec::with_capacity(left.len() + right.len());
    rows.extend_from_slice(left);
    rows.extend_from_slice(right);
    rows.sort_unstable();
    rows.dedup();
    rows
}

fn without_rows(
    rows: &[[u8; TRIBLE_LEN]],
    removed: impl Fn(&[u8; TRIBLE_LEN]) -> bool,
) -> Vec<[u8; TRIBLE_LEN]> {
    rows.iter().copied().filter(|row| !removed(row)).collect()
}

fn assert_local_rows(label: &str, patch: &EavPatch, expected: &[[u8; TRIBLE_LEN]]) {
    assert_eq!(patch.len(), expected.len() as u64, "{label}: wrong length");
    let stats = patch.node_stats();
    assert_eq!(stats.2, 0, "{label}: unexpectedly contains heap leaves");
    assert_eq!(
        stats.3,
        expected.len() as u64,
        "{label}: wrong LocalLeaf count"
    );
    let actual: Vec<_> = patch.iter_ordered().copied().collect();
    assert_eq!(actual, expected, "{label}: wrong ordered rows");
}

fn assert_heap_rows(label: &str, patch: &EavPatch, expected: &[[u8; TRIBLE_LEN]]) {
    assert_eq!(patch.len(), expected.len() as u64, "{label}: wrong length");
    let stats = patch.node_stats();
    assert_eq!(
        stats.2,
        expected.len() as u64,
        "{label}: wrong heap-leaf count"
    );
    assert_eq!(stats.3, 0, "{label}: unexpectedly contains LocalLeaves");
    let actual: Vec<_> = patch.iter_ordered().copied().collect();
    assert_eq!(actual, expected, "{label}: wrong ordered rows");
}

#[derive(Copy, Clone)]
enum FixtureStorage {
    Heap,
    Local,
}

fn assert_fixture_rows(
    storage: FixtureStorage,
    label: &str,
    patch: &EavPatch,
    expected: &[[u8; TRIBLE_LEN]],
) {
    match storage {
        FixtureStorage::Heap => assert_heap_rows(label, patch, expected),
        FixtureStorage::Local => assert_local_rows(label, patch, expected),
    }
}

fn assert_tribleset_rows(
    storage: FixtureStorage,
    label: &str,
    set: &TribleSet,
    expected: &[[u8; TRIBLE_LEN]],
) {
    assert_eq!(set.len(), expected.len(), "{label}: wrong length");
    for (index, stats) in [
        ("eav", set.eav.node_stats()),
        ("eva", set.eva.node_stats()),
        ("aev", set.aev.node_stats()),
        ("ave", set.ave.node_stats()),
        ("vea", set.vea.node_stats()),
        ("vae", set.vae.node_stats()),
    ] {
        match storage {
            FixtureStorage::Heap => {
                assert_eq!(
                    stats.2,
                    expected.len() as u64,
                    "{label}/{index}: heap leaves"
                );
                assert_eq!(stats.3, 0, "{label}/{index}: unexpected LocalLeaves");
            }
            FixtureStorage::Local => {
                assert_eq!(stats.2, 0, "{label}/{index}: unexpected heap leaves");
                assert_eq!(
                    stats.3,
                    expected.len() as u64,
                    "{label}/{index}: LocalLeaves"
                );
            }
        }
    }

    macro_rules! assert_index_rows {
        ($index:ident) => {{
            let mut actual: Vec<_> = set.$index.iter().copied().collect();
            actual.sort_unstable();
            assert_eq!(
                actual,
                expected,
                "{label}/{}: wrong rows",
                stringify!($index)
            );
        }};
    }
    assert_index_rows!(eav);
    assert_index_rows!(eva);
    assert_index_rows!(aev);
    assert_index_rows!(ave);
    assert_index_rows!(vea);
    assert_index_rows!(vae);
}

fn demote_tribleset_via_duplicate(
    mut exact: TribleSet,
    expected: &[[u8; TRIBLE_LEN]],
    duplicate: [u8; TRIBLE_LEN],
) -> TribleSet {
    assert!(expected.binary_search(&duplicate).is_ok());
    assert_tribleset_rows(
        FixtureStorage::Local,
        "TribleSet duplicate-demotion source",
        &exact,
        expected,
    );
    let singleton_rows = std::slice::from_ref(&duplicate);
    let fresh = archive_tribleset(singleton_rows);
    assert_tribleset_rows(
        FixtureStorage::Local,
        "TribleSet duplicate-demotion fresh singleton",
        &fresh,
        singleton_rows,
    );

    // Each index difference retains only the left operand's owners. The
    // second difference isolates all six pointer-equal LocalLeaves under
    // `exact`'s owner cover, so the semantic no-op does not add a 33rd owner.
    let without = exact.difference(&fresh);
    assert_eq!(without.len(), expected.len() - 1);
    let derived = exact.difference(&without);
    assert_tribleset_rows(
        FixtureStorage::Local,
        "TribleSet duplicate-demotion derived singleton",
        &derived,
        singleton_rows,
    );
    for (index, stats) in [
        ("eav", derived.eav.node_stats()),
        ("eva", derived.eva.node_stats()),
        ("aev", derived.aev.node_stats()),
        ("ave", derived.ave.node_stats()),
        ("vea", derived.vea.node_stats()),
        ("vae", derived.vae.node_stats()),
    ] {
        assert_eq!(
            stats,
            (0, 0, 0, 1),
            "TribleSet duplicate-demotion derived singleton/{index}: wrong shape"
        );
    }
    drop(without);
    drop(fresh);

    // All six derived singleton indexes have unknown LocalLeaf roots. The
    // semantic no-op therefore demotes all six aggregate roots independently.
    exact.union(derived);
    assert_tribleset_rows(
        FixtureStorage::Local,
        "TribleSet duplicate-demotion result",
        &exact,
        expected,
    );
    exact
}

struct UnionCase {
    name: &'static str,
    left: EavPatch,
    right: EavPatch,
    oracle: EavPatch,
}

fn checked_union_case(
    name: &'static str,
    storage: FixtureStorage,
    left: EavPatch,
    left_rows: &[[u8; TRIBLE_LEN]],
    right: EavPatch,
    right_rows: &[[u8; TRIBLE_LEN]],
) -> UnionCase {
    // Both sources span multiple EAV byte-zero buckets, proving that both
    // roots branch at depth zero. Their union therefore reaches the
    // equal-depth branch merge instead of the first-divergence shortcut.
    assert_ne!(left_rows.first().unwrap()[0], left_rows.last().unwrap()[0]);
    assert_ne!(
        right_rows.first().unwrap()[0],
        right_rows.last().unwrap()[0]
    );
    assert_fixture_rows(storage, &format!("{name} left source"), &left, left_rows);
    assert_fixture_rows(storage, &format!("{name} right source"), &right, right_rows);

    let expected = union_rows(left_rows, right_rows);
    let oracle = heap_oracle(&expected);
    let mut result = left.clone();
    result.union(right.clone());
    assert_fixture_rows(storage, &format!("{name} union result"), &result, &expected);

    // Ordered row comparison above is the exact correctness oracle. Avoid a
    // setup-time PATCH equality check here: equality consumes fingerprints,
    // and a memoizing implementation may publish knowledge into descendants
    // shared with the reusable operand templates. Timed equality workloads
    // must be the first fingerprint consumers in this harness.

    // Union consumes its operands, so the benchmark feeds it cheap clones.
    // Prove fixture validation did not mutate the reusable templates.
    assert_fixture_rows(
        storage,
        &format!("{name} left source after union"),
        &left,
        left_rows,
    );
    assert_fixture_rows(
        storage,
        &format!("{name} right source after union"),
        &right,
        right_rows,
    );

    UnionCase {
        name,
        left,
        right,
        oracle,
    }
}

fn clean_union_case(len: usize) -> UnionCase {
    let left_rows = ordered_clean_rows(0, len);
    let right_rows = ordered_clean_rows(1, len);
    checked_union_case(
        match len {
            4_095 => "clean_4095",
            4_096 => "clean_4096",
            65_536 => "clean_65536",
            _ => unreachable!("unsupported clean-union fixture size"),
        },
        FixtureStorage::Heap,
        heap_oracle(&left_rows),
        &left_rows,
        heap_oracle(&right_rows),
        &right_rows,
    )
}

fn union_cases() -> Vec<UnionCase> {
    let disjoint_left_rows = ordered_archive_rows(0, 128, 0, 32);
    let disjoint_right_rows = ordered_archive_rows(128, 128, 0, 32);
    let overlap_left_rows = ordered_archive_rows(0, 128, 0, 32);
    let overlap_right_rows = ordered_archive_rows(0, 128, 31, 32);
    assert_eq!(
        union_rows(&disjoint_left_rows, &disjoint_right_rows).len(),
        8_192
    );
    assert_eq!(
        union_rows(&overlap_left_rows, &overlap_right_rows).len(),
        8_064
    );

    // Balanced variant unions produce an exact root over dirty direct
    // children. Demote both operands independently so dirty_disjoint and
    // dirty_overlap128 exercise the no-root-demand path. Each helper derives
    // its duplicate leaf under the corresponding fixture's existing owners.
    let dirty_disjoint_left =
        demote_via_duplicate(archive_variants_in(0, 128, 0, 32), raw_trible(0, 0));
    let dirty_disjoint_right =
        demote_via_duplicate(archive_variants_in(128, 128, 0, 32), raw_trible(128, 0));
    let dirty_overlap_left =
        demote_via_duplicate(archive_variants_in(0, 128, 0, 32), raw_trible(0, 0));
    let dirty_overlap_right =
        demote_via_duplicate(archive_variants_in(0, 128, 31, 32), raw_trible(0, 31));

    vec![
        clean_union_case(4_095),
        clean_union_case(4_096),
        clean_union_case(65_536),
        checked_union_case(
            "dirty_disjoint",
            FixtureStorage::Local,
            dirty_disjoint_left,
            &disjoint_left_rows,
            dirty_disjoint_right,
            &disjoint_right_rows,
        ),
        checked_union_case(
            "dirty_overlap128",
            FixtureStorage::Local,
            dirty_overlap_left,
            &overlap_left_rows,
            dirty_overlap_right,
            &overlap_right_rows,
        ),
    ]
}

fn bench_union(c: &mut Criterion) {
    let cases = union_cases();
    let mut group = c.benchmark_group("patch_receipts/union");
    group.sample_size(20);

    for case in &cases {
        group.throughput(Throughput::Elements(case.left.len() + case.right.len()));
        for (workload, equality_repeats) in [("union_only", 0), ("union_eq1", 1), ("union_eq8", 8)]
        {
            group.bench_function(BenchmarkId::new(case.name, workload), |b| {
                b.iter_batched(
                    || (case.left.clone(), case.right.clone()),
                    |(mut left, right)| {
                        left.union(black_box(right));
                        for _ in 0..equality_repeats {
                            black_box(black_box(&left) == black_box(&case.oracle));
                        }
                        black_box(left)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
        group.throughput(Throughput::Elements(
            8 * (case.left.len() + case.right.len()),
        ));
        group.bench_function(BenchmarkId::new(case.name, "union_repeat8"), |b| {
            b.iter_batched(
                || {
                    (
                        case.left.clone(),
                        (0..8).map(|_| case.right.clone()).collect::<Vec<_>>(),
                    )
                },
                |(mut left, rights)| {
                    for right in rights {
                        left.union(black_box(right));
                    }
                    black_box(left)
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

struct RemovalCase {
    name: &'static str,
    template: EavPatch,
    key: [u8; TRIBLE_LEN],
    oracle: EavPatch,
    equality_repeats: &'static [(&'static str, usize)],
}

fn removal_cases() -> Vec<RemovalCase> {
    const RESIDENT_WORK: &[(&str, usize)] =
        &[("delete_only", 0), ("delete_eq1", 1), ("delete_eq8", 8)];
    const DIRTY_WORK: &[(&str, usize)] =
        &[("delete_only", 0), ("delete_eq1", 1), ("delete_eq8", 8)];

    let resident_rows = ordered_rows(REMOVAL_VARIANTS);
    let resident = archive_variants(REMOVAL_VARIANTS);
    assert_local_rows("resident removal source", &resident, &resident_rows);

    let removed_variant = archive_variant(0, BUCKETS as u16, 0);
    let removed_variant_rows = ordered_rows(1);
    assert_local_rows(
        "dirty-removal subtraction source",
        &removed_variant,
        &removed_variant_rows,
    );
    let dirty = resident.difference(&removed_variant);
    let dirty_rows = without_rows(&resident_rows, |row| row[1] == 0);
    assert_local_rows("dirty removal source", &dirty, &dirty_rows);

    // Difference is borrowed: prove both source operands retained their exact
    // rows before the timing harness starts cloning either template.
    assert_local_rows(
        "resident removal source after difference",
        &resident,
        &resident_rows,
    );
    assert_local_rows(
        "subtraction source after difference",
        &removed_variant,
        &removed_variant_rows,
    );

    let key = raw_trible(BUCKETS / 2, REMOVAL_VARIANTS - 1);
    let resident_expected = without_rows(&resident_rows, |row| row == &key);
    let dirty_expected = without_rows(&dirty_rows, |row| row == &key);
    let resident_oracle = heap_oracle(&resident_expected);
    let dirty_oracle = heap_oracle(&dirty_expected);

    let mut resident_result = resident.clone();
    resident_result.remove(&key);
    assert_local_rows(
        "resident removal result",
        &resident_result,
        &resident_expected,
    );
    assert!(!resident_result.iter().any(|row| row == &key));

    let mut dirty_result = dirty.clone();
    dirty_result.remove(&key);
    assert_local_rows("dirty removal result", &dirty_result, &dirty_expected);
    assert!(!dirty_result.iter().any(|row| row == &key));

    vec![
        RemovalCase {
            name: "resident",
            template: resident,
            key,
            oracle: resident_oracle,
            equality_repeats: RESIDENT_WORK,
        },
        RemovalCase {
            name: "dirty",
            template: dirty,
            key,
            oracle: dirty_oracle,
            equality_repeats: DIRTY_WORK,
        },
    ]
}

fn bench_removal(c: &mut Criterion) {
    let cases = removal_cases();
    let mut group = c.benchmark_group("patch_receipts/removal");
    group.sample_size(20);
    group.throughput(Throughput::Elements(1));

    for case in &cases {
        for &(workload, equality_repeats) in case.equality_repeats {
            group.bench_function(BenchmarkId::new(case.name, workload), |b| {
                b.iter_batched(
                    || case.template.clone(),
                    |mut patch| {
                        patch.remove(black_box(&case.key));
                        for _ in 0..equality_repeats {
                            black_box(black_box(&patch) == black_box(&case.oracle));
                        }
                        black_box(patch)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

struct DifferenceCase {
    name: &'static str,
    left: EavPatch,
    right: EavPatch,
    oracle: EavPatch,
}

fn checked_difference_case(
    name: &'static str,
    left: EavPatch,
    left_rows: &[[u8; TRIBLE_LEN]],
    right: EavPatch,
    right_rows: &[[u8; TRIBLE_LEN]],
) -> DifferenceCase {
    assert_local_rows(&format!("{name} left source"), &left, left_rows);
    assert_local_rows(&format!("{name} right source"), &right, right_rows);

    let expected = without_rows(left_rows, |row| right_rows.binary_search(row).is_ok());
    let oracle = heap_oracle(&expected);
    let result = left.difference(&right);
    assert_local_rows(&format!("{name} difference result"), &result, &expected);

    // `assert_local_rows` compares the complete ordered result exactly. Do
    // not consume a fingerprint during fixture validation: immutable cache
    // publication can otherwise warm structure shared with `left` or `right`.

    // The operation borrows both inputs. Keep this explicit because receipt
    // experiments must not win by accidentally consuming or changing them.
    assert_local_rows(
        &format!("{name} left source after difference"),
        &left,
        left_rows,
    );
    assert_local_rows(
        &format!("{name} right source after difference"),
        &right,
        right_rows,
    );

    DifferenceCase {
        name,
        left,
        right,
        oracle,
    }
}

fn difference_cases() -> Vec<DifferenceCase> {
    let left_rows = ordered_rows(DIFFERENCE_VARIANTS);

    let sparse_rows = vec![
        raw_trible(0, 0),
        raw_trible(BUCKETS - 1, DIFFERENCE_VARIANTS - 1),
    ];
    let half_rows = ordered_rows(DIFFERENCE_VARIANTS / 2);
    let heavy_rows = ordered_rows(DIFFERENCE_VARIANTS - 1);

    vec![
        checked_difference_case(
            "sparse2",
            archive_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_patch(&sparse_rows),
            &sparse_rows,
        ),
        checked_difference_case(
            "half",
            archive_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_variants(DIFFERENCE_VARIANTS / 2),
            &half_rows,
        ),
        checked_difference_case(
            "heavy",
            archive_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_variants(DIFFERENCE_VARIANTS - 1),
            &heavy_rows,
        ),
    ]
}

fn bench_difference(c: &mut Criterion) {
    let cases = difference_cases();
    let mut group = c.benchmark_group("patch_receipts/difference");
    group.sample_size(20);
    group.throughput(Throughput::Elements(
        BUCKETS as u64 * DIFFERENCE_VARIANTS as u64,
    ));

    for case in &cases {
        for (workload, equality_repeats) in [
            ("difference_only", 0),
            ("difference_eq1", 1),
            ("difference_eq8", 8),
        ] {
            group.bench_function(BenchmarkId::new(case.name, workload), |b| {
                b.iter_batched_ref(
                    || (case.left.clone(), case.right.clone()),
                    |(left, right)| {
                        let result = black_box(&*left).difference(black_box(&*right));
                        for _ in 0..equality_repeats {
                            black_box(black_box(&result) == black_box(&case.oracle));
                        }
                        black_box(result)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

struct TribleSetBinaryCase {
    name: &'static str,
    left: TribleSet,
    right: TribleSet,
    oracle: TribleSet,
}

fn union_triblesets(left: &TribleSet, right: &TribleSet) -> TribleSet {
    let mut result = left.clone();
    result.union(right.clone());
    result
}

fn checked_tribleset_case(
    name: &'static str,
    operation: &str,
    left: TribleSet,
    left_rows: &[[u8; TRIBLE_LEN]],
    right: TribleSet,
    right_rows: &[[u8; TRIBLE_LEN]],
    expected: &[[u8; TRIBLE_LEN]],
    evaluate: impl FnOnce(&TribleSet, &TribleSet) -> TribleSet,
) -> TribleSetBinaryCase {
    assert_tribleset_rows(
        FixtureStorage::Local,
        &format!("{name} TribleSet {operation} left source"),
        &left,
        left_rows,
    );
    assert_tribleset_rows(
        FixtureStorage::Local,
        &format!("{name} TribleSet {operation} right source"),
        &right,
        right_rows,
    );

    let oracle = heap_tribleset(expected);
    assert_tribleset_rows(
        FixtureStorage::Heap,
        &format!("{name} TribleSet {operation} heap oracle"),
        &oracle,
        expected,
    );
    let result = evaluate(&left, &right);
    assert_tribleset_rows(
        FixtureStorage::Local,
        &format!("{name} TribleSet {operation} result"),
        &result,
        expected,
    );

    // All six indexes were compared row-for-row above. A TribleSet equality
    // check would inspect only EAV and, for memoizing probes, could publish
    // fingerprints into descendants shared with the benchmark templates.

    assert_tribleset_rows(
        FixtureStorage::Local,
        &format!("{name} TribleSet {operation} left source after operation"),
        &left,
        left_rows,
    );
    assert_tribleset_rows(
        FixtureStorage::Local,
        &format!("{name} TribleSet {operation} right source after operation"),
        &right,
        right_rows,
    );

    TribleSetBinaryCase {
        name,
        left,
        right,
        oracle,
    }
}

fn tribleset_union_cases() -> Vec<TribleSetBinaryCase> {
    let left_rows = ordered_archive_rows(0, 128, 0, 32);
    let right_rows = ordered_archive_rows(0, 128, 31, 32);
    let expected = union_rows(&left_rows, &right_rows);
    assert_eq!(expected.len(), 8_064);

    // The balanced templates have exact roots over dirty direct children.
    // Build a fresh second pair before demotion so the two benchmark cases
    // have the same rows and topology without cross-case refcount coupling;
    // only the singleton no-op changes root knowledge in the demoted pair.
    let exact_left = archive_tribleset_variants_in(0, 128, 0, 32);
    let exact_right = archive_tribleset_variants_in(0, 128, 31, 32);
    let demoted_left = demote_tribleset_via_duplicate(
        archive_tribleset_variants_in(0, 128, 0, 32),
        &left_rows,
        raw_trible(0, 0),
    );
    let demoted_right = demote_tribleset_via_duplicate(
        archive_tribleset_variants_in(0, 128, 31, 32),
        &right_rows,
        raw_trible(0, 31),
    );

    vec![
        checked_tribleset_case(
            "exact_overlap128",
            "union",
            exact_left,
            &left_rows,
            exact_right,
            &right_rows,
            &expected,
            union_triblesets,
        ),
        checked_tribleset_case(
            "demoted_overlap128",
            "union",
            demoted_left,
            &left_rows,
            demoted_right,
            &right_rows,
            &expected,
            union_triblesets,
        ),
    ]
}

fn bench_tribleset_union(c: &mut Criterion) {
    let cases = tribleset_union_cases();
    let mut group = c.benchmark_group("patch_receipts/tribleset_union");
    group.sample_size(20);

    for case in &cases {
        group.throughput(Throughput::Elements(8_192));
        for (workload, equality_repeats) in [("union_only", 0), ("union_eq1", 1), ("union_eq8", 8)]
        {
            group.bench_function(BenchmarkId::new(case.name, workload), |b| {
                b.iter_batched(
                    || (case.left.clone(), case.right.clone()),
                    |(mut left, right)| {
                        left.union(black_box(right));
                        for _ in 0..equality_repeats {
                            // The union runs all six indexes; public equality
                            // consumes only the EAV result fingerprint.
                            black_box(black_box(&left) == black_box(&case.oracle));
                        }
                        black_box(left)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
        group.throughput(Throughput::Elements(8 * 8_192));
        group.bench_function(BenchmarkId::new(case.name, "union_repeat8"), |b| {
            b.iter_batched(
                || {
                    (
                        case.left.clone(),
                        (0..8).map(|_| case.right.clone()).collect::<Vec<_>>(),
                    )
                },
                |(mut left, rights)| {
                    for right in rights {
                        left.union(black_box(right));
                    }
                    black_box(left)
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn tribleset_difference_cases() -> Vec<TribleSetBinaryCase> {
    let left_rows = ordered_rows(DIFFERENCE_VARIANTS);
    let sparse_rows = vec![
        raw_trible(0, 0),
        raw_trible(BUCKETS - 1, DIFFERENCE_VARIANTS - 1),
    ];
    let half_rows = ordered_rows(DIFFERENCE_VARIANTS / 2);
    let heavy_rows = ordered_rows(DIFFERENCE_VARIANTS - 1);
    let sparse_expected = without_rows(&left_rows, |row| sparse_rows.binary_search(row).is_ok());
    let half_expected = without_rows(&left_rows, |row| half_rows.binary_search(row).is_ok());
    let heavy_expected = without_rows(&left_rows, |row| heavy_rows.binary_search(row).is_ok());

    vec![
        checked_tribleset_case(
            "sparse2",
            "difference",
            archive_tribleset_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_tribleset(&sparse_rows),
            &sparse_rows,
            &sparse_expected,
            TribleSet::difference,
        ),
        checked_tribleset_case(
            "half",
            "difference",
            archive_tribleset_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_tribleset_variants(DIFFERENCE_VARIANTS / 2),
            &half_rows,
            &half_expected,
            TribleSet::difference,
        ),
        checked_tribleset_case(
            "heavy",
            "difference",
            archive_tribleset_variants(DIFFERENCE_VARIANTS),
            &left_rows,
            archive_tribleset_variants(DIFFERENCE_VARIANTS - 1),
            &heavy_rows,
            &heavy_expected,
            TribleSet::difference,
        ),
    ]
}

fn bench_tribleset_difference(c: &mut Criterion) {
    let cases = tribleset_difference_cases();
    let mut group = c.benchmark_group("patch_receipts/tribleset_difference");
    group.sample_size(20);
    group.throughput(Throughput::Elements(
        BUCKETS as u64 * DIFFERENCE_VARIANTS as u64,
    ));

    for case in &cases {
        for (workload, equality_repeats) in [
            ("difference_only", 0),
            ("difference_eq1", 1),
            ("difference_eq8", 8),
        ] {
            group.bench_function(BenchmarkId::new(case.name, workload), |b| {
                b.iter_batched_ref(
                    || (case.left.clone(), case.right.clone()),
                    |(left, right)| {
                        let result = black_box(&*left).difference(black_box(&*right));
                        for _ in 0..equality_repeats {
                            // TribleSet equality intentionally consumes only EAV,
                            // while the difference above computes all six indexes.
                            black_box(black_box(&result) == black_box(&case.oracle));
                        }
                        black_box(result)
                    },
                    BatchSize::LargeInput,
                );
            });
        }
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    bench_union(c);
    bench_removal(c);
    bench_difference(c);
    bench_tribleset_union(c);
    bench_tribleset_difference(c);
}

criterion_group!(patch_receipt_benches, benches);
criterion_main!(patch_receipt_benches);
