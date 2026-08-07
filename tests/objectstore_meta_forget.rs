#![cfg(feature = "object-store")]

use url::Url;

#[test]
fn objectstore_metadata_and_forget_file_backend() -> Result<(), Box<dyn std::error::Error>> {
    use tempfile::tempdir;

    use triblespace::core::blob::encodings::UnknownBlob;
    use triblespace::core::blob::Blob;
    use triblespace::core::blob::Bytes;
    use triblespace::core::repo::async_store::Blocking;
    use triblespace::core::repo::objectstore::ObjectStoreRemote;
    use triblespace::core::repo::{BlobStoreForget, BlobStoreList, BlobStoreMeta};

    use triblespace::prelude::BlobStorePut;

    let dir = tempdir()?;
    let url = Url::parse(&format!("file://{}", dir.path().display()))?;
    let mut remote = Blocking::new(ObjectStoreRemote::with_url(&url)?)?;

    let contents = b"hello world".to_vec();
    let blob: Blob<UnknownBlob> = Blob::new(Bytes::from(contents.clone()));

    let handle = remote.put::<UnknownBlob, _>(blob)?;

    // metadata should be present and report the correct length
    use triblespace::prelude::BlobStore;

    let reader = remote.reader()?;
    let meta = reader.metadata(handle)?;
    assert!(meta.is_some());
    let meta = meta.unwrap();
    assert_eq!(meta.length, contents.len() as u64);
    let listed: Vec<_> = reader.blobs().collect::<Result<_, _>>()?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].handle, handle);
    assert_eq!(listed[0].length, contents.len() as u64);

    // forget removes the blob and is idempotent
    remote.forget(handle)?;
    let meta2 = reader.metadata(handle)?;
    assert!(meta2.is_none());
    // second call should succeed as well
    remote.forget(handle)?;

    Ok(())
}

#[test]
fn objectstore_get_rejects_bytes_that_do_not_match_the_path_hash(
) -> Result<(), Box<dyn std::error::Error>> {
    use tempfile::tempdir;

    use triblespace::core::blob::encodings::UnknownBlob;
    use triblespace::core::blob::{Blob, Bytes};
    use triblespace::core::inline::encodings::hash::{Blake3, Hash};
    use triblespace::core::inline::Inline;
    use triblespace::core::repo::async_store::Blocking;
    use triblespace::core::repo::objectstore::{GetBlobErr, ObjectStoreRemote};
    use triblespace::core::repo::{BlobStore, BlobStoreGet};

    let dir = tempdir()?;
    let expected_blob = Blob::<UnknownBlob>::new(Bytes::from(b"expected".to_vec()));
    let expected_handle = expected_blob.get_handle();
    let expected_hash: Inline<Hash<Blake3>> = expected_handle.into();
    let blob_dir = dir.path().join("blobs");
    std::fs::create_dir(&blob_dir)?;
    std::fs::write(
        blob_dir.join(Hash::<Blake3>::to_hex(&expected_hash).to_ascii_lowercase()),
        b"tampered",
    )?;

    let url = Url::parse(&format!("file://{}", dir.path().display()))?;
    let mut remote = Blocking::new(ObjectStoreRemote::with_url(&url)?)?;
    let reader = remote.reader()?;
    let err = reader
        .get::<Bytes, UnknownBlob>(expected_handle)
        .expect_err("wrong bytes at a content-addressed path must be rejected");

    let actual_hash: Inline<Hash<Blake3>> =
        Blob::<UnknownBlob>::new(Bytes::from(b"tampered".to_vec()))
            .get_handle()
            .into();
    assert!(matches!(
        err,
        GetBlobErr::HashMismatch { expected, actual }
            if expected == expected_hash && actual == actual_hash
    ));

    Ok(())
}
