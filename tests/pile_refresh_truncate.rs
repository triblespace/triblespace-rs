use anybytes::Bytes;
use std::io::Write;
use std::sync::Arc;
use std::sync::Barrier;
use triblespace::core::blob::encodings::UnknownBlob;
use triblespace::prelude::*;

#[test]
fn refresh_during_amputate_truncation_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pile.pile");

    // Write a valid blob and flush it
    std::fs::File::create(&path).unwrap();
    let mut pile: Pile = Pile::open(&path).unwrap();
    let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"data".to_vec()));
    let handle = pile.put::<UnknownBlob, _>(blob).unwrap();
    pile.flush().unwrap();
    pile.close().unwrap();

    // Append garbage to simulate a truncated write
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[0, 1, 2]).unwrap();
    }

    // Open two handles on the same pile
    let mut pile_refresh: Pile = Pile::open(&path).unwrap();
    let mut pile_amputate: Pile = Pile::open(&path).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let refresh_thread = std::thread::spawn(move || {
        b1.wait();
        let _ = pile_refresh.refresh();
        pile_refresh.close().unwrap();
    });

    let b2 = barrier.clone();
    let amputate_thread = std::thread::spawn(move || {
        b2.wait();
        pile_amputate.amputate().unwrap();
        pile_amputate.close().unwrap();
    });

    refresh_thread.join().unwrap();
    amputate_thread.join().unwrap();

    // The pile should be valid after amputate
    let mut pile: Pile = Pile::open(&path).unwrap();
    pile.refresh().unwrap();
    let blob = pile
        .reader()
        .unwrap()
        .get::<Blob<UnknownBlob>, _>(handle)
        .unwrap();
    assert_eq!(blob.bytes.as_ref(), b"data");
    pile.close().unwrap();
}
