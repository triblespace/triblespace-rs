use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreList;
use triblespace::prelude::PinStore;
use triblespace_core::repo::pile::Pile;

#[test]
fn create_initializes_empty_pile() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("create_test.pile");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let mut pile: Pile = Pile::open(&path).unwrap();
    // Explicitly refresh after open to populate in-memory indices.
    pile.refresh().unwrap();
    let mut iter = pile.pins().unwrap();
    assert!(iter.next().is_none());
    pile.close().unwrap();
}

#[test]
fn create_refuses_to_replace_an_existing_pile() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("existing.pile");
    let original = b"append-only evidence";
    std::fs::write(&path, original).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", path.to_str().unwrap()])
        .assert()
        .failure();

    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[test]
fn create_refuses_a_missing_parent_directory() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("nested")
        .join("dirs")
        .join("create_test.pile");

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", path.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("does not exist"));

    assert!(!path.exists());
    assert!(!path.parent().unwrap().exists());
}

#[test]
fn create_accepts_a_relative_filename_without_a_parent_component() {
    let dir = tempdir().unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .current_dir(dir.path())
        .args(["pile", "create", "relative.pile"])
        .assert()
        .success();

    assert!(dir.path().join("relative.pile").exists());
}

#[test]
fn put_ingests_file() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("put_test.pile");
    std::fs::File::create(&pile_path).unwrap();
    let input_path = dir.path().join("input.bin");
    std::fs::write(&input_path, b"hello world").unwrap();

    let digest = blake3::hash(b"hello world").to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match(pattern).unwrap());

    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    let reader = pile.reader().unwrap();
    assert!(reader.blobs().next().is_some());
    drop(reader);
    pile.close().unwrap();
}

#[test]
fn get_restores_blob() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("get_test.pile");
    std::fs::File::create(&pile_path).unwrap();
    let input_path = dir.path().join("input.bin");
    let output_path = dir.path().join("output.bin");
    let contents = b"fetch me";
    std::fs::write(&input_path, contents).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "get",
            pile_path.to_str().unwrap(),
            &handle,
            output_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let out = std::fs::read(&output_path).unwrap();
    assert_eq!(contents, &out[..]);
}

#[test]
fn list_blobs_outputs_expected_handle() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("list_blobs.pile");
    std::fs::File::create(&pile_path).unwrap();
    let input_path = dir.path().join("input.bin");
    let contents = b"hello";
    std::fs::write(&input_path, contents).unwrap();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!("^{handle}\\n$");

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "blob", "list", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());
}

#[test]
fn list_blobs_with_metadata_outputs_details() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("list_blobs_meta.pile");
    std::fs::File::create(&pile_path).unwrap();
    let input_path = dir.path().join("input.bin");
    let contents = b"hello";
    std::fs::write(&input_path, contents).unwrap();

    let digest = blake3::hash(contents).to_hex().to_string();
    let handle = format!("blake3:{digest}");
    let pattern = format!(r"^{}\t\S+\t{}\n$", handle, contents.len());

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            input_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "list",
            "--metadata",
            pile_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match(&pattern).unwrap());
}

#[test]
fn diagnose_reports_healthy() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("diag.pile");

    // create an empty pile file
    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "create", pile_path.to_str().unwrap()])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "diagnose", "check", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("healthy"));
}

#[test]
fn diagnose_reports_invalid_hash() {
    use std::io::Seek;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("bad.pile");
    std::fs::File::create(&pile_path).unwrap();
    let blob_path = dir.path().join("blob.bin");
    std::fs::write(&blob_path, b"good data").unwrap();

    // put a blob into the pile
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            blob_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    // corrupt the blob bytes directly
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&pile_path)
        .unwrap();
    // first blob payload starts after the fixed 256-byte V3 header
    file.seek(std::io::SeekFrom::Start(256)).unwrap();
    file.write_all(b"X").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "diagnose", "check", pile_path.to_str().unwrap()])
        .assert()
        .failure()
        .stdout(predicate::str::contains("Blobs: 1 (1 invalid)"));
}

#[test]
fn diagnose_fail_fast_stops_at_the_invalid_blob() {
    use std::io::{Seek, Write};

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("bad-fast.pile");
    std::fs::File::create(&pile_path).unwrap();
    let blob_path = dir.path().join("blob.bin");
    std::fs::write(&blob_path, b"good data").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            blob_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&pile_path)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(256)).unwrap();
    file.write_all(b"X").unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "diagnose",
            "check",
            pile_path.to_str().unwrap(),
            "--fail-fast",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("blob scan failed"))
        .stdout(predicate::str::contains("Blobs:").not());
}

#[test]
fn inspect_outputs_tribles() {
    use triblespace::prelude::*;
    use triblespace_core::examples;
    use triblespace_core::inline::encodings::hash::Handle;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("inspect.pile");
    std::fs::File::create(&pile_path).unwrap();

    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::blob::{Blob, IntoBlob};
    let dataset = examples::dataset();
    let blob: Blob<SimpleArchive> = dataset.to_blob();

    let handle_str = {
        let mut pile: Pile = Pile::open(&pile_path).unwrap();
        let handle = pile.put::<SimpleArchive, _>(blob).unwrap();
        pile.close().unwrap();

        let hash = Handle::to_hash(handle);
        hash.from_inline::<String>()
    };

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "inspect",
            pile_path.to_str().unwrap(),
            &handle_str,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Length:"));
}

#[test]
fn diagnose_locate_hash_reports_header_and_payload_refs() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("locate_hash.pile");
    std::fs::File::create(&pile_path).unwrap();

    // Put blob1 and capture its handle string.
    let blob1_path = dir.path().join("blob1.bin");
    std::fs::write(&blob1_path, b"blob1").unwrap();
    let out1 = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            blob1_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let handle1 = String::from_utf8(out1).unwrap();
    let handle1 = handle1.trim().to_string();

    // Put blob2 containing the raw digest bytes of blob1 in its payload, so the
    // locator can find a payload reference.
    let digest_hex = handle1.strip_prefix("blake3:").expect("handle prefix");
    let digest_bytes = hex::decode(digest_hex).expect("decode digest hex");
    let mut payload = b"prefix".to_vec();
    payload.extend_from_slice(&digest_bytes);
    payload.extend_from_slice(b"suffix");

    let blob2_path = dir.path().join("blob2.bin");
    std::fs::write(&blob2_path, payload).unwrap();
    let out2 = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "blob",
            "put",
            pile_path.to_str().unwrap(),
            blob2_path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let handle2 = String::from_utf8(out2).unwrap();
    let handle2 = handle2.trim().to_string();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "diagnose",
            "locate-hash",
            pile_path.to_str().unwrap(),
            &handle1,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("blob header match"))
        .stdout(predicate::str::contains(&format!(
            "payload reference in {handle2}"
        )))
        .stdout(predicate::str::contains("Summary"));
}

/// A corrupt (torn-tail) source pile must make generation rewrites fail loud
/// without truncating the source or publishing a plausible partial result.
#[test]
fn corrupt_source_fails_loud_without_truncation() {
    use ed25519_dalek::SigningKey;
    use std::io::Write;
    use triblespace_core::repo::Repository;
    use triblespace_core::trible::TribleSet;

    let dir = tempdir().unwrap();
    let src_path = dir.path().join("corrupt_src.pile");
    std::fs::File::create(&src_path).unwrap();

    let identity = {
        let key = SigningKey::from_bytes(&[7; 32]);
        let pile: Pile = Pile::open(&src_path).unwrap();
        let mut repo = Repository::new(pile, key, TribleSet::new()).unwrap();
        let mut workspace = repo.create_workspace("main").unwrap();
        let identity = *workspace.identity();
        workspace
            .commit(TribleSet::new(), "seed")
            .expect("workspace rank has room");
        repo.push(&mut workspace).unwrap();
        repo.close().unwrap();
        identity
    };

    // Tear the tail: append garbage that decodes as no known record.
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&src_path)
            .unwrap();
        file.write_all(&[0xFFu8; 33]).unwrap();
        file.sync_all().unwrap();
    }
    let len_before = std::fs::metadata(&src_path).unwrap().len();

    let fail_loud = predicate::str::contains("trible pile amputate");

    // Forget scans and verifies every record before publishing a new
    // generation. The corrupt tail therefore prevents destination creation.
    let dest = dir.path().join("forgotten.pile");
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "forget",
            src_path.to_str().unwrap(),
            dest.to_str().unwrap(),
            &identity.to_string(),
        ])
        .assert()
        .failure()
        .stderr(fail_loud.clone());
    assert!(
        !dest.exists(),
        "forget must not publish a destination from a corrupt source"
    );

    // Read-only branch observation also refuses to open a corrupt pile.
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "list",
            src_path.to_str().unwrap(),
            "--all",
        ])
        .assert()
        .failure()
        .stderr(fail_loud);

    let len_after = std::fs::metadata(&src_path).unwrap().len();
    assert_eq!(
        len_before, len_after,
        "source pile must not be truncated by a failed open"
    );
}

#[test]
fn net_sync_refuses_a_corrupt_pile_before_starting_transport() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("corrupt-net.pile");
    let corrupt = vec![0u8; 33];
    std::fs::write(&path, &corrupt).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "net",
            "sync",
            path.to_str().unwrap(),
            "--duration",
            "0",
            "--no-lazy",
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("refusing to auto-repair"));

    assert_eq!(
        std::fs::read(&path).unwrap(),
        corrupt,
        "a failed sync open must leave the corrupt generation byte-identical"
    );
}

#[test]
fn net_sync_rejects_an_explicit_malformed_peer() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid-peer.pile");
    std::fs::File::create(&path).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "net",
            "sync",
            path.to_str().unwrap(),
            "--peers",
            "definitely-not-an-endpoint-id",
            "--duration",
            "0",
            "--no-lazy",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid --peers value"));
}
