use triblespace::core::blob::encodings::succinctarchive::{
    CompressedUniverse, OrderedUniverse, SuccinctArchive, SuccinctArchiveBlob,
};
use triblespace::core::blob::encodings::UnknownBlob;
use triblespace::core::blob::{Blob, Bytes, MemoryBlobStore};
use triblespace::core::inline::encodings::hash::Handle;
use triblespace::core::inline::Inline;
use triblespace::core::repo::{reachable, BlobStore, BlobStoreGet};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::blobencodings::SuccinctArchiveRank9IndexBlob;

type RawTrible = [u8; 64];

fn raw_trible(entity: [u8; 16], attribute: [u8; 16], value: [u8; 32]) -> RawTrible {
    let mut out = [0; 64];
    out[..16].copy_from_slice(&entity);
    out[16..32].copy_from_slice(&attribute);
    out[32..].copy_from_slice(&value);
    out
}

fn set(seed: u8) -> TribleSet {
    [
        raw_trible([seed; 16], [0x11; 16], [0x81; 32]),
        raw_trible([seed; 16], [0x12; 16], [0x82; 32]),
    ]
    .into_iter()
    .map(|data| Trible { data })
    .collect()
}

fn pair(
    seed: u8,
) -> (
    Blob<SuccinctArchiveBlob>,
    Blob<SuccinctArchiveRank9IndexBlob>,
) {
    let archive: SuccinctArchive<OrderedUniverse> = (&set(seed)).into();
    archive.to_blob_pair()
}

#[test]
fn rank9_blob_starts_with_its_source_handle_and_pair_roundtrips() {
    let expected = set(1);
    let (raw, rank9) = pair(1);
    assert_eq!(&rank9.bytes.as_ref()[..32], &raw.get_handle().raw);

    let archive = SuccinctArchive::<OrderedUniverse>::from_blob_pair(raw, rank9).unwrap();
    assert_eq!(archive.iter().collect::<TribleSet>(), expected);
}

#[test]
fn rank9_root_discovers_and_retains_its_raw_archive() {
    let (raw, rank9) = pair(5);
    let mut store = MemoryBlobStore::new();
    let raw_handle = store.insert(raw);
    let rank9_handle = store.insert(rank9);
    let orphan_handle = store.insert(Blob::<UnknownBlob>::new(Bytes::from_source(vec![0xA5; 64])));

    let root: Inline<Handle<UnknownBlob>> = rank9_handle.transmute();
    let raw_unknown: Inline<Handle<UnknownBlob>> = raw_handle.transmute();
    let reader = store.reader().unwrap();
    let reachable_from_rank9: Vec<_> = reachable(&reader, [root]).collect();
    assert_eq!(reachable_from_rank9[0], root);
    assert!(reachable_from_rank9.contains(&raw_unknown));
    assert_eq!(reachable_from_rank9.len(), 2);

    store.keep(reachable(&reader, [root]));
    let retained = store.reader().unwrap();
    assert_eq!(retained.len(), 2);
    assert!(retained
        .get::<Blob<SuccinctArchiveRank9IndexBlob>, SuccinctArchiveRank9IndexBlob>(rank9_handle)
        .is_ok());
    assert!(retained
        .get::<Blob<SuccinctArchiveBlob>, SuccinctArchiveBlob>(raw_handle)
        .is_ok());
    assert!(retained
        .get::<Blob<UnknownBlob>, UnknownBlob>(orphan_handle)
        .is_err());
}

#[test]
fn compressed_universe_pair_roundtrips() {
    let expected = set(4);
    let (raw, rank9) = SuccinctArchive::<CompressedUniverse>::build_blob_pair(&expected);
    let archive = SuccinctArchive::<CompressedUniverse>::from_blob_pair(raw, rank9).unwrap();
    assert_eq!(archive.iter().collect::<TribleSet>(), expected);
}

#[test]
fn pair_attachment_rejects_missing_mismatched_and_corrupt_indexes() {
    let (raw_a, rank9_a) = pair(1);
    let (raw_b, rank9_b) = pair(2);

    assert!(SuccinctArchive::<OrderedUniverse>::from_optional_blob_pair(
        raw_a.clone(),
        None::<Blob<SuccinctArchiveRank9IndexBlob>>,
    )
    .is_err());
    assert!(SuccinctArchive::<OrderedUniverse>::from_blob_pair(raw_a.clone(), rank9_b).is_err());

    let mut corrupt = rank9_a.bytes.as_ref().to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    let corrupt = Blob::<SuccinctArchiveRank9IndexBlob>::new(Bytes::from_source(corrupt));
    assert!(SuccinctArchive::<OrderedUniverse>::from_blob_pair(raw_a, corrupt).is_err());

    // Keep both variables meaningfully distinct: B's raw content is not A.
    assert_ne!(raw_b.get_handle().raw, pair(1).0.get_handle().raw);
}

#[test]
fn raw_identity_is_deterministic_and_independent_of_rank9_bytes() {
    let (raw_a, rank9_a) = pair(7);
    let (raw_b, rank9_b) = pair(7);
    assert_eq!(raw_a.get_handle(), raw_b.get_handle());
    assert_eq!(raw_a.bytes, raw_b.bytes);
    assert_eq!(rank9_a.get_handle(), rank9_b.get_handle());

    let mut alternative_index = rank9_a.bytes.as_ref().to_vec();
    alternative_index[32] ^= 1;
    let alternative_index =
        Blob::<SuccinctArchiveRank9IndexBlob>::new(Bytes::from_source(alternative_index));
    assert_ne!(alternative_index.get_handle(), rank9_b.get_handle());
    assert_eq!(raw_a.get_handle(), raw_b.get_handle());
}

#[test]
fn old_embedded_suffix_is_not_a_raw_succinct_archive() {
    let archive: SuccinctArchive<OrderedUniverse> = (&set(9)).into();
    let meta_size = std::mem::size_of_val(&archive.meta());
    let (raw, rank9) = archive.to_blob_pair();
    let meta_start = raw.bytes.len() - meta_size;
    let mut embedded = Vec::with_capacity(raw.bytes.len() + rank9.bytes.len());
    embedded.extend_from_slice(&raw.bytes.as_ref()[..meta_start]);
    embedded.extend_from_slice(rank9.bytes.as_ref());
    embedded.extend_from_slice(&raw.bytes.as_ref()[meta_start..]);

    let embedded = Blob::<SuccinctArchiveBlob>::new(Bytes::from_source(embedded));
    let decoded: Result<SuccinctArchive<OrderedUniverse>, _> = embedded.try_from_blob();
    assert!(decoded.is_err());
}

#[test]
fn raw_only_decode_rebuilds_the_same_detached_index_explicitly() {
    let (raw, rank9) = pair(3);
    let rebuilt_rank9 = SuccinctArchive::<OrderedUniverse>::build_rank9_index(raw.clone()).unwrap();
    let rebuilt: SuccinctArchive<OrderedUniverse> = raw.clone().try_from_blob().unwrap();
    let (rebuilt_raw, _) = rebuilt.to_blob_pair();
    assert_eq!(rebuilt_raw.get_handle(), raw.get_handle());
    assert_eq!(rebuilt_rank9.get_handle(), rank9.get_handle());
}
