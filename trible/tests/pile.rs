use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use predicates::prelude::*;
use tempfile::tempdir;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreList;
use triblespace::prelude::PinStore;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;

fn random_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).expect("getrandom");
    SigningKey::from_bytes(&seed)
}

#[test]
fn list_branches_outputs_branch_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.pile");
    std::fs::File::create(&path).unwrap();

    {
        let pile: Pile = Pile::open(&path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();
        repo.create_branch("main", None).expect("create branch");
        repo.into_storage().close().unwrap();
    }

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\t-\\tmain\\n$").unwrap());
}

#[test]
fn delete_branch_removes_branch_id_from_list() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("delete_test.pile");
    std::fs::File::create(&path).unwrap();

    let branch_id = {
        let pile: Pile = Pile::open(&path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();
        let branch_id = repo.create_branch("main", None).expect("create branch");
        let pile = repo.into_storage();
        pile.close().unwrap();
        *branch_id
    };

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "delete",
            path.to_str().unwrap(),
            &format!("{branch_id:X}"),
        ])
        .assert()
        .success();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());

    let mut pile: Pile = Pile::open(&path).unwrap();
    pile.refresh().unwrap();
    assert_eq!(pile.head(branch_id).unwrap(), None);
    pile.close().unwrap();
}

#[test]
fn branch_stats_reports_fast_and_full_counts() {
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::*;

    let dir = tempdir().unwrap();
    let path = dir.path().join("stats_test.pile");
    std::fs::File::create(&path).unwrap();

    let branch_id = {
        let pile: Pile = Pile::open(&path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();
        let branch_id = repo.create_branch("main", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull");

        let entity_id = ufoid();
        let mut content = TribleSet::new();
        let label = ws.put::<LongString, _>("stats-test".to_string());
        content += entity! { &entity_id @ triblespace_core::metadata::name: label };
        ws.commit(content, "seed");

        let push_res = repo.try_push(&mut ws).expect("push");
        assert!(push_res.is_none(), "unexpected push conflict");

        let pile = repo.into_storage();
        pile.close().unwrap();
        *branch_id
    };

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "stats",
            path.to_str().unwrap(),
            &format!("{branch_id:X}"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Commits: 1"))
        .stdout(predicate::str::contains("Content blobs (accum): 1"))
        .stdout(predicate::str::contains("Content bytes (accum): 64"))
        .stdout(predicate::str::contains("Triples (accum): 1"));

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "stats",
            path.to_str().unwrap(),
            &format!("{branch_id:X}"),
            "--full",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Triples (unique): 1"))
        .stdout(predicate::str::contains("Entities: 1"))
        .stdout(predicate::str::contains("Attributes: 1"));
}

#[test]
fn create_initializes_empty_pile() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("create_test.pile");
    std::fs::File::create(&path).unwrap();

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
fn create_creates_parent_directories() {
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
        .success();

    assert!(path.exists());
    assert!(path.parent().unwrap().exists());
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
    std::fs::File::create(&pile_path).unwrap();

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
        .stdout(predicate::str::contains("incorrect hashes"));
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

#[test]
fn pile_branch_create_outputs_id() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("create_branch.pile");
    std::fs::File::create(&pile_path).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "create",
            pile_path.to_str().unwrap(),
            "main",
        ])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\n$").unwrap());

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::is_match("^[A-F0-9]{32}\\t-\\tmain\\n$").unwrap());
}
