use ed25519_dalek::SigningKey;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{IntoBlob, MemoryBlobStore};
use triblespace_core::inline::Inline;
use triblespace_core::prelude::*;
use triblespace_core::repo::commit;
use triblespace_core::repo::{
    materialize_commit_contents, BlobStore, BlobStorePut, CommitHandle, WorkspaceCheckoutError,
};

fn facts(label: &str) -> TribleSet {
    entity! { triblespace_core::repo::short_message: label }.into()
}

fn store_commit(
    store: &mut MemoryBlobStore,
    signing_key: &SigningKey,
    content: TribleSet,
) -> CommitHandle {
    let content_blob = content.to_blob();
    store.insert(content_blob.clone());
    let metadata = commit::commit_metadata(signing_key, [], None, Some(content_blob), None);
    store
        .put::<SimpleArchive, _>(metadata)
        .expect("memory put is infallible")
}

#[test]
fn empty_selection_materializes_the_empty_set() {
    let mut store = MemoryBlobStore::new();
    let reader = store.reader().unwrap();
    let commits: &[CommitHandle] = &[];

    assert!(materialize_commit_contents(&reader, commits)
        .unwrap()
        .is_empty());
}

#[test]
fn contentless_commit_is_the_union_identity() {
    let mut store = MemoryBlobStore::new();
    let parents = [Inline::new([1; 32]), Inline::new([2; 32])];
    let metadata: TribleSet = entity! {
        triblespace_core::repo::parent*: parents,
    }
    .into();
    let commit = store
        .put::<SimpleArchive, _>(metadata)
        .expect("memory put is infallible");
    let reader = store.reader().unwrap();

    assert!(materialize_commit_contents(&reader, [commit])
        .unwrap()
        .is_empty());
}

#[test]
fn exact_selection_unions_and_deduplicates_content() {
    let mut store = MemoryBlobStore::new();
    let signing_key = SigningKey::from_bytes(&[7; 32]);
    let shared = facts("shared");
    let left = shared.clone() + facts("left");
    let right = shared.clone() + facts("right");
    let left_commit = store_commit(&mut store, &signing_key, left.clone());
    let right_commit = store_commit(&mut store, &signing_key, right.clone());
    let reader = store.reader().unwrap();

    let forward =
        materialize_commit_contents(&reader, &[left_commit, right_commit, left_commit][..])
            .unwrap();
    let reverse = materialize_commit_contents(&reader, [right_commit, left_commit]).unwrap();

    assert_eq!(forward, left + right);
    assert_eq!(forward, reverse);
}

#[test]
fn missing_commit_or_content_preserves_the_storage_error() {
    let mut store = MemoryBlobStore::new();
    let missing_content = facts("not stored").to_blob();
    let metadata = commit::commit_metadata(
        &SigningKey::from_bytes(&[9; 32]),
        [],
        None,
        Some(missing_content),
        None,
    );
    let commit_without_content = store
        .put::<SimpleArchive, _>(metadata)
        .expect("memory put is infallible");
    let reader = store.reader().unwrap();
    let absent = Inline::new([0xFF; 32]);

    let error = materialize_commit_contents(&reader, [absent]).unwrap_err();
    assert!(matches!(error, WorkspaceCheckoutError::Storage(_)));

    let error = materialize_commit_contents(&reader, [commit_without_content]).unwrap_err();
    assert!(matches!(error, WorkspaceCheckoutError::Storage(_)));
}

#[test]
fn ambiguous_content_is_bad_commit_metadata() {
    let mut store = MemoryBlobStore::new();
    let first = store
        .put::<SimpleArchive, _>(facts("first"))
        .expect("memory put is infallible");
    let second = store
        .put::<SimpleArchive, _>(facts("second"))
        .expect("memory put is infallible");
    let malformed: TribleSet = entity! {
        triblespace_core::repo::content*: [first, second],
    }
    .into();
    let malformed = store
        .put::<SimpleArchive, _>(malformed)
        .expect("memory put is infallible");
    let reader = store.reader().unwrap();

    let error = materialize_commit_contents(&reader, [malformed]).unwrap_err();
    assert!(matches!(error, WorkspaceCheckoutError::BadCommitMetadata()));
}
