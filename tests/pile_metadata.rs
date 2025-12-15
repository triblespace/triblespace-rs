use anybytes::Bytes;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use tempfile::tempdir;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::blob::Blob;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::BlobStoreMeta;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStorePut;

#[test]
fn metadata_detects_corrupted_blob() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("pile.pile");

    let mut pile: Pile<Blake3> = Pile::open(&path).unwrap();
    let data = b"hello metadata".to_vec();
    let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(data.clone()));
    let handle = pile.put(blob).unwrap();
    pile.flush().unwrap();
    assert!(pile.reader().unwrap().metadata(handle).unwrap().is_some());
    drop(pile);

    {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();
        let pos = contents
            .windows(data.len())
            .position(|window| window == data.as_slice())
            .expect("blob not found");
        file.seek(SeekFrom::Start(pos as u64)).unwrap();
        let new_byte = contents[pos] ^ 0xff;
        file.write_all(&[new_byte]).unwrap();
    }

    let mut reopened: Pile<Blake3> = Pile::open(&path).unwrap();
    reopened.restore().unwrap();
    let reader = reopened.reader().unwrap();
    assert!(reader.metadata(handle).unwrap().is_none());
}
