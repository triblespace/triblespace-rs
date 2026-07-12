//! Cross-version gate for the additive SuccinctArchive Rank9 sidecar format.
//!
//! The old package is pinned to the last reader before sidecars. Keeping both
//! package identities in-process proves both directions without a checked-in,
//! architecture-specific binary fixture.

use old_triblespace_core as old;
use triblespace::core as new;

type RawTrible = [u8; 64];

fn raw_trible(entity: [u8; 16], attribute: [u8; 16], value: [u8; 32]) -> RawTrible {
    let mut out = [0; 64];
    out[..16].copy_from_slice(&entity);
    out[16..32].copy_from_slice(&attribute);
    out[32..].copy_from_slice(&value);
    out
}

fn four_fact_rows() -> Vec<RawTrible> {
    let e0 = [0x01; 16];
    let e1 = [0x02; 16];
    let a0 = [0x11; 16];
    let a1 = [0x12; 16];
    let v0 = [0x81; 32];
    let v1 = [0x82; 32];
    vec![
        raw_trible(e0, a0, v0),
        raw_trible(e0, a0, v1),
        raw_trible(e0, a1, v0),
        raw_trible(e1, a0, v0),
    ]
}

fn fanout_rows(n: usize) -> Vec<RawTrible> {
    (0..n)
        .map(|i| {
            let mut value = [0x90; 32];
            value[24..].copy_from_slice(&(i as u64).to_be_bytes());
            raw_trible([0x31; 16], [0x41; 16], value)
        })
        .collect()
}

fn new_set(rows: &[RawTrible]) -> new::trible::TribleSet {
    rows.iter()
        .copied()
        .map(|data| new::trible::Trible { data })
        .collect()
}

fn old_set(rows: &[RawTrible]) -> old::trible::TribleSet {
    rows.iter()
        .copied()
        .map(|data| old::trible::Trible { data })
        .collect()
}

fn new_rows<U>(
    archive: &new::blob::encodings::succinctarchive::SuccinctArchive<U>,
) -> Vec<RawTrible>
where
    U: new::blob::encodings::succinctarchive::Universe,
{
    archive.iter().map(|trible| trible.data).collect()
}

fn old_rows<U>(
    archive: &old::blob::encodings::succinctarchive::SuccinctArchive<U>,
) -> Vec<RawTrible>
where
    U: old::blob::encodings::succinctarchive::Universe,
{
    archive.iter().map(|trible| trible.data).collect()
}

fn new_query_signature<U>(
    archive: &new::blob::encodings::succinctarchive::SuccinctArchive<U>,
) -> [usize; 6]
where
    U: new::blob::encodings::succinctarchive::Universe,
{
    let full = 0..archive.iter().count();
    [
        archive.distinct_in(&archive.changed_e_a, &full),
        archive.distinct_in(&archive.changed_e_v, &full),
        archive.distinct_in(&archive.changed_a_e, &full),
        archive.distinct_in(&archive.changed_a_v, &full),
        archive.distinct_in(&archive.changed_v_e, &full),
        archive.distinct_in(&archive.changed_v_a, &full),
    ]
}

fn old_query_signature<U>(
    archive: &old::blob::encodings::succinctarchive::SuccinctArchive<U>,
) -> [usize; 6]
where
    U: old::blob::encodings::succinctarchive::Universe,
{
    let full = 0..archive.iter().count();
    [
        archive.distinct_in(&archive.changed_e_a, &full),
        archive.distinct_in(&archive.changed_e_v, &full),
        archive.distinct_in(&archive.changed_a_e, &full),
        archive.distinct_in(&archive.changed_a_v, &full),
        archive.distinct_in(&archive.changed_v_e, &full),
        archive.distinct_in(&archive.changed_v_a, &full),
    ]
}

fn old_bytes(bytes: &[u8]) -> old::blob::Bytes {
    // Copy deliberately so the gate keeps compiling if the revisions later
    // resolve different anybytes package identities.
    old::blob::Bytes::from_source(bytes.to_vec())
}

fn new_bytes(bytes: &[u8]) -> new::blob::Bytes {
    new::blob::Bytes::from_source(bytes.to_vec())
}

fn check_ordered(rows: &[RawTrible], expected_query_signature: [usize; 6]) {
    use new::blob::encodings::succinctarchive::{
        OrderedUniverse as NewUniverse, SuccinctArchive as NewArchive,
        SuccinctArchiveBlob as NewBlobEncoding,
    };
    use old::blob::encodings::succinctarchive::{
        OrderedUniverse as OldUniverse, SuccinctArchive as OldArchive,
        SuccinctArchiveBlob as OldBlobEncoding,
    };

    let new_archive: NewArchive<NewUniverse> = (&new_set(rows)).into();
    let old_archive: OldArchive<OldUniverse> = (&old_set(rows)).into();

    let old_meta_len = std::mem::size_of_val(&old_archive.meta());
    let new_meta_len = std::mem::size_of_val(&new_archive.meta());
    assert_eq!(old_meta_len, new_meta_len);
    let old_meta_start = old_archive.bytes.len() - old_meta_len;

    // The additive-format contract: legacy raw sections and EOF metadata are
    // byte-identical; only the gap between them grows.
    assert_eq!(
        &old_archive.bytes[..old_meta_start],
        &new_archive.bytes[..old_meta_start]
    );
    assert_eq!(
        &old_archive.bytes[old_meta_start..],
        &new_archive.bytes[new_archive.bytes.len() - new_meta_len..]
    );
    assert!(new_archive.bytes.len() > old_archive.bytes.len());

    let old_blob = old::blob::Blob::<OldBlobEncoding>::new(old_bytes(&new_archive.bytes));
    let old_from_v2: OldArchive<OldUniverse> = old_blob.try_from_blob().unwrap();
    assert_eq!(old_rows(&old_from_v2), old_rows(&old_archive));
    assert_eq!(old_query_signature(&old_from_v2), expected_query_signature);

    let new_blob = new::blob::Blob::<NewBlobEncoding>::new(new_bytes(&old_archive.bytes));
    let new_from_v1: NewArchive<NewUniverse> = new_blob.try_from_blob().unwrap();
    assert_eq!(new_rows(&new_from_v1), new_rows(&new_archive));
    assert_eq!(new_query_signature(&new_from_v1), expected_query_signature);
    assert_eq!(
        new_query_signature(&new_archive),
        old_query_signature(&old_archive)
    );
}

fn check_compressed(rows: &[RawTrible], expected_query_signature: [usize; 6]) {
    use new::blob::encodings::succinctarchive::{
        CompressedUniverse as NewUniverse, SuccinctArchive as NewArchive,
        SuccinctArchiveBlob as NewBlobEncoding,
    };
    use old::blob::encodings::succinctarchive::{
        CompressedUniverse as OldUniverse, SuccinctArchive as OldArchive,
        SuccinctArchiveBlob as OldBlobEncoding,
    };

    let new_archive: NewArchive<NewUniverse> = (&new_set(rows)).into();
    let old_archive: OldArchive<OldUniverse> = (&old_set(rows)).into();
    let old_meta_len = std::mem::size_of_val(&old_archive.meta());
    let new_meta_len = std::mem::size_of_val(&new_archive.meta());
    assert_eq!(old_meta_len, new_meta_len);
    let old_meta_start = old_archive.bytes.len() - old_meta_len;
    assert_eq!(
        &old_archive.bytes[..old_meta_start],
        &new_archive.bytes[..old_meta_start]
    );
    assert_eq!(
        &old_archive.bytes[old_meta_start..],
        &new_archive.bytes[new_archive.bytes.len() - new_meta_len..]
    );
    assert!(new_archive.bytes.len() > old_archive.bytes.len());

    let old_blob = old::blob::Blob::<OldBlobEncoding>::new(old_bytes(&new_archive.bytes));
    let old_from_v2: OldArchive<OldUniverse> = old_blob.try_from_blob().unwrap();
    assert_eq!(old_rows(&old_from_v2), old_rows(&old_archive));
    assert_eq!(old_query_signature(&old_from_v2), expected_query_signature);

    let new_blob = new::blob::Blob::<NewBlobEncoding>::new(new_bytes(&old_archive.bytes));
    let new_from_v1: NewArchive<NewUniverse> = new_blob.try_from_blob().unwrap();
    assert_eq!(new_rows(&new_from_v1), new_rows(&new_archive));
    assert_eq!(new_query_signature(&new_from_v1), expected_query_signature);
    assert_eq!(
        new_query_signature(&new_archive),
        old_query_signature(&old_archive)
    );
}

#[test]
fn empty_archives_cross_the_format_boundary() {
    check_ordered(&[], [0; 6]);
    check_compressed(&[], [0; 6]);
}

#[test]
fn mixed_pair_groups_cross_the_format_boundary() {
    let rows = four_fact_rows();
    check_ordered(&rows, [3; 6]);
    check_compressed(&rows, [3; 6]);
}

#[test]
fn rank9_select_hint_threshold_crosses_the_format_boundary() {
    // Rank9 emits a hint only once rank/zero-rank is strictly greater than
    // 1024. One fixed (E,A) pair plus 1026 distinct values is the smallest
    // archive that gives a changed-pair vector 1025 zeroes while other pair
    // vectors exceed 1024 ones.
    let rows = fanout_rows(1026);
    let signature = [1, 1026, 1, 1026, 1026, 1026];
    check_ordered(&rows, signature);
    check_compressed(&rows, signature);
}
