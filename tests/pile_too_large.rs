use triblespace::core::repo::pile::ReadError;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::*;

#[test]
#[cfg(target_pointer_width = "64")]
fn open_near_usize_max_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pile.pile");
    let file = std::fs::File::create(&path).unwrap();
    let large = (usize::MAX as u64 / 2) + 1;
    if file.set_len(large).is_err() {
        return;
    }
    drop(file);
    match Pile::<Blake3>::open(&path) {
        Err(ReadError::FileTooLarge { .. }) => {}
        Err(e) => panic!("unexpected error: {e:?}"),
        Ok(_) => panic!("expected error opening overly large pile"),
    }
}
