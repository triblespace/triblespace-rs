use anybytes::Bytes;
use std::io::Write;
use std::sync::Arc;
use std::sync::Barrier;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::prelude::*;

#[test]
fn refresh_during_restore_truncation_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pile.pile");

    // Write a valid blob and flush it
    let mut pile: Pile = Pile::open(&path).unwrap();
    let blob: Blob<UnknownBlob> = Blob::new(Bytes::from_source(b"data".to_vec()));
    let handle = pile.put(blob).unwrap();
    pile.flush().unwrap();
    drop(pile);

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
    let mut pile_restore: Pile = Pile::open(&path).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let b1 = barrier.clone();
    let refresh_thread = std::thread::spawn(move || {
        b1.wait();
        let _ = pile_refresh.refresh();
    });

    let b2 = barrier.clone();
    let restore_thread = std::thread::spawn(move || {
        b2.wait();
        pile_restore.restore().unwrap();
    });

    refresh_thread.join().unwrap();
    restore_thread.join().unwrap();

    // The pile should be valid after restore
    let mut pile: Pile = Pile::open(&path).unwrap();
    pile.refresh().unwrap();
    let blob = pile
        .reader()
        .unwrap()
        .get::<Blob<UnknownBlob>, _>(handle)
        .unwrap();
    assert_eq!(blob.bytes.as_ref(), b"data");
}
