use anybytes::Bytes;
use std::collections::HashSet;

use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::blob::{Blob, MemoryBlobStore};
use triblespace::core::repo::BlobStore;
use triblespace::core::repo::{reachable, transfer, BlobStoreGet};
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::core::value::VALUE_LEN;

#[test]
fn reachable_keep_and_transfer() {
    let mut source = MemoryBlobStore::<Blake3>::new();

    // Insert a child blob that will be referenced by the root.
    let child_blob = Blob::<UnknownBlob>::new(Bytes::from(vec![1u8; VALUE_LEN * 2]));
    let child_handle = source.insert(child_blob);

    // Insert an orphan blob that should be dropped by keep.
    let orphan_blob = Blob::<UnknownBlob>::new(Bytes::from(vec![2u8; VALUE_LEN * 2]));
    let orphan_handle = source.insert(orphan_blob);

    // Root blob references the child handle in its first 32-byte slot.
    let mut root_bytes = Vec::with_capacity(VALUE_LEN * 2);
    root_bytes.extend_from_slice(&child_handle.raw);
    root_bytes.extend_from_slice(&[0u8; VALUE_LEN]);
    let root_blob = Blob::<UnknownBlob>::new(Bytes::from(root_bytes));
    let root_handle = source.insert(root_blob);

    // Retain only blobs reachable from the root handle.
    let reader = source.reader().expect("reader");
    source.keep(reachable(&reader, [root_handle]));

    let refreshed = source.reader().expect("refreshed reader");
    assert!(refreshed
        .get::<Blob<UnknownBlob>, UnknownBlob>(root_handle)
        .is_ok());
    assert!(refreshed
        .get::<Blob<UnknownBlob>, UnknownBlob>(child_handle)
        .is_ok());
    assert!(refreshed
        .get::<Blob<UnknownBlob>, UnknownBlob>(orphan_handle)
        .is_err());

    // Copy only the handles reported by the reachable walker into a fresh store.
    let reader = source.reader().expect("post-keep reader");
    let mut target = MemoryBlobStore::<Blake3>::new();
    let copied = transfer(&reader, &mut target, reachable(&reader, [root_handle]))
        .collect::<Result<Vec<_>, _>>()
        .expect("transfer handles");

    let copied_handles: HashSet<_> = copied.iter().map(|(old, _)| *old).collect();
    assert_eq!(copied_handles.len(), 2);
    assert!(copied_handles.contains(&root_handle));
    assert!(copied_handles.contains(&child_handle));

    let target_reader = target.reader().expect("target reader");
    assert_eq!(target_reader.len(), 2);
    assert!(target_reader
        .get::<Blob<UnknownBlob>, UnknownBlob>(root_handle)
        .is_ok());
    assert!(target_reader
        .get::<Blob<UnknownBlob>, UnknownBlob>(child_handle)
        .is_ok());
}
