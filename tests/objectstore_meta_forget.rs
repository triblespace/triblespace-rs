use url::Url;

#[test]
fn objectstore_metadata_and_forget_file_backend() -> Result<(), Box<dyn std::error::Error>> {
    use tempfile::tempdir;

    use triblespace::core::blob::schemas::UnknownBlob;
    use triblespace::core::blob::Blob;
    use triblespace::core::blob::Bytes;
    use triblespace::core::repo::objectstore::ObjectStoreRemote;
    use triblespace::core::repo::{BlobStoreForget, BlobStoreMeta};
    use triblespace::core::value::schemas::hash::Blake3;
    use triblespace::prelude::BlobStorePut;

    let dir = tempdir()?;
    let url = Url::parse(&format!("file://{}", dir.path().display()))?;
    let mut remote: ObjectStoreRemote<Blake3> = ObjectStoreRemote::with_url(&url)?;

    let contents = b"hello world".to_vec();
    let blob = Blob::new(Bytes::from(contents.clone()));

    let handle = remote.put::<UnknownBlob, _>(blob)?;

    // metadata should be present and report the correct length
    use triblespace::prelude::BlobStore;

    let reader = remote.reader()?;
    let meta = reader.metadata(handle.clone())?;
    assert!(meta.is_some());
    let meta = meta.unwrap();
    assert_eq!(meta.length, contents.len() as u64);

    // forget removes the blob and is idempotent
    remote.forget(handle.clone())?;
    let meta2 = reader.metadata(handle.clone())?;
    assert!(meta2.is_none());
    // second call should succeed as well
    remote.forget(handle)?;

    Ok(())
}
