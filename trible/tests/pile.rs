use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use predicates::prelude::*;
use tempfile::tempdir;
use triblespace::prelude::BlobStore;
use triblespace::prelude::BlobStoreGet;
use triblespace::prelude::BlobStoreList;
use triblespace::prelude::PinStore;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::pile_index::{MappedPileIndex, PileIndex};
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;

fn random_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).expect("getrandom");
    SigningKey::from_bytes(&seed)
}

#[test]
fn pile_index_build_refreshes_the_derived_snapshot_without_mutating_the_pile() {
    use anybytes::Bytes;
    use triblespace::prelude::blobencodings::UnknownBlob;
    use triblespace::prelude::BlobStorePut;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("indexed.pile");
    let index_path = dir.path().join("indexed.pile.pidx");
    std::fs::File::create(&pile_path).unwrap();

    let first = {
        let mut pile = Pile::open(&pile_path).unwrap();
        let handle = pile
            .put::<UnknownBlob, _>(Bytes::from_source(b"first".to_vec()))
            .unwrap();
        pile.close().unwrap();
        handle
    };
    let pile_before = std::fs::read(&pile_path).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "index", "build", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("1 pile records"))
        .stdout(predicate::str::contains("1 blobs"));

    assert_eq!(std::fs::read(&pile_path).unwrap(), pile_before);
    let first_snapshot = std::fs::read(&index_path).unwrap();
    let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
    assert!(mapped.blob_locator(first).unwrap().is_some());

    let second = {
        let mut pile = Pile::open(&pile_path).unwrap();
        let handle = pile
            .put::<UnknownBlob, _>(Bytes::from_source(b"second".to_vec()))
            .unwrap();
        pile.close().unwrap();
        handle
    };
    let pile_with_tail = std::fs::read(&pile_path).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "index", "build", pile_path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("2 pile records"))
        .stdout(predicate::str::contains("2 blobs"));

    assert_eq!(std::fs::read(&pile_path).unwrap(), pile_with_tail);
    assert_ne!(std::fs::read(&index_path).unwrap(), first_snapshot);
    let mapped = MappedPileIndex::open(&pile_path, &index_path).unwrap();
    assert!(mapped.blob_locator(first).unwrap().is_some());
    assert!(mapped.blob_locator(second).unwrap().is_some());
}

#[test]
fn pile_index_build_failure_preserves_the_previous_snapshot() {
    use anybytes::Bytes;
    use std::io::Write;
    use triblespace::prelude::blobencodings::UnknownBlob;
    use triblespace::prelude::BlobStorePut;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("torn.pile");
    let index_path = dir.path().join("custom.pidx");
    std::fs::File::create(&pile_path).unwrap();

    {
        let mut pile = Pile::open(&pile_path).unwrap();
        pile.put::<UnknownBlob, _>(Bytes::from_source(b"valid".to_vec()))
            .unwrap();
        pile.close().unwrap();
    }
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "index",
            "build",
            pile_path.to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let snapshot = std::fs::read(&index_path).unwrap();

    let mut pile = std::fs::OpenOptions::new()
        .append(true)
        .open(&pile_path)
        .unwrap();
    pile.write_all(b"torn").unwrap();
    pile.sync_all().unwrap();
    drop(pile);

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "index",
            "build",
            pile_path.to_str().unwrap(),
            "--output",
            index_path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("build locator index"));

    assert_eq!(std::fs::read(&index_path).unwrap(), snapshot);
}

#[test]
fn pile_index_build_is_safe_with_a_concurrent_atomic_writer() {
    use anybytes::Bytes;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    use triblespace::prelude::blobencodings::UnknownBlob;
    use triblespace::prelude::BlobStorePut;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("concurrent.pile");
    let index_path = dir.path().join("concurrent.pile.pidx");
    std::fs::File::create(&pile_path).unwrap();

    {
        let mut pile = Pile::open(&pile_path).unwrap();
        pile.put::<UnknownBlob, _>(Bytes::from_source(b"seed".to_vec()))
            .unwrap();
        pile.close().unwrap();
    }
    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "index", "build", pile_path.to_str().unwrap()])
        .assert()
        .success();
    let previous_snapshot = std::fs::read(&index_path).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let writer_path = pile_path.clone();
    let writer_barrier = barrier.clone();
    let writer = std::thread::spawn(move || {
        let mut pile = Pile::open(&writer_path).unwrap();
        writer_barrier.wait();
        for value in 0u64..256 {
            pile.put::<UnknownBlob, _>(Bytes::from_source(value.to_le_bytes().to_vec()))
                .unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
        pile.close().unwrap();
    });

    barrier.wait();
    let output = Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "index", "build", pile_path.to_str().unwrap()])
        .output()
        .unwrap();
    writer.join().unwrap();

    if !output.status.success() {
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("build locator index"),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read(&index_path).unwrap(), previous_snapshot);
    }

    // A successful race may publish either the exact file or an older valid
    // prefix. Both are safe: indexed open accepts that prefix and canonical
    // tail replay recovers every concurrently appended record.
    let mut indexed = Pile::open_indexed(&pile_path, &index_path).unwrap();
    let logical = indexed.patch_index().unwrap();
    assert_eq!(logical.blob_handles().count(), 257);
    indexed.close().unwrap();

    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
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

#[test]
fn reid_and_rename_preserve_typed_manifest_facts() {
    use triblespace::prelude::blobencodings::LongString;
    use triblespace::prelude::BlobStorePut;
    use triblespace_core::repo;
    use triblespace_core::repo::index_home::{Manifest, SuccinctRollup};

    let dir = tempdir().unwrap();
    let source_path = dir.path().join("manifest-source.pile");
    let reid_path = dir.path().join("manifest-reid.pile");
    std::fs::File::create(&source_path).unwrap();

    let manifest_facts = Manifest::new(&SuccinctRollup).unwrap().to_tribles();
    {
        let mut pile = Pile::open(&source_path).unwrap();
        let branch_id = triblespace_core::id::genid();
        let name = pile.put::<LongString, _>("original".to_string()).unwrap();
        let mut metadata =
            repo::branch::branch_metadata(&random_signing_key(), *branch_id, name, None);
        metadata += manifest_facts.clone();
        let metadata_handle = pile.put(metadata).unwrap();
        pile.update(*branch_id, None, Some(metadata_handle))
            .unwrap();
        pile.close().unwrap();
    }

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "reid",
            source_path.to_str().unwrap(),
            reid_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let reid_branch = {
        let mut pile = Pile::open(&reid_path).unwrap();
        pile.refresh().unwrap();
        let branch = pile.pins().unwrap().next().unwrap().unwrap();
        let metadata_handle = pile.head(branch).unwrap().unwrap();
        let reader = pile.reader().unwrap();
        let metadata: TribleSet = reader.get(metadata_handle).unwrap();
        assert_eq!(
            repo::index_home::manifest_tribles(&metadata),
            manifest_facts
        );
        drop(reader);
        pile.close().unwrap();
        branch
    };

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "rename",
            reid_path.to_str().unwrap(),
            &format!("{reid_branch:X}"),
            "renamed",
        ])
        .assert()
        .success();

    let mut pile = Pile::open(&reid_path).unwrap();
    pile.refresh().unwrap();
    let metadata_handle = pile.head(reid_branch).unwrap().unwrap();
    let reader = pile.reader().unwrap();
    let metadata: TribleSet = reader.get(metadata_handle).unwrap();
    assert_eq!(
        repo::index_home::manifest_tribles(&metadata),
        manifest_facts
    );
    drop(reader);
    pile.close().unwrap();
}

/// A corrupt (torn-tail) source pile must make `reid`, `squash`, and
/// `migrate` fail loud — pointing at `trible pile amputate` — without
/// truncating the source file. Silent auto-repair on open is reserved
/// for the explicit `trible pile amputate` command.
#[test]
fn corrupt_source_fails_loud_without_truncation() {
    use std::io::Write;

    let dir = tempdir().unwrap();
    let src_path = dir.path().join("corrupt_src.pile");
    std::fs::File::create(&src_path).unwrap();

    // Seed a valid pile with one branch.
    {
        let pile: Pile = Pile::open(&src_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();
        repo.create_branch("main", None).expect("create branch");
        repo.into_storage().close().unwrap();
    }

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

    // reid: fails loud, source untouched, destination never created.
    let dest = dir.path().join("reid_dst.pile");
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "reid",
            src_path.to_str().unwrap(),
            dest.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(fail_loud.clone());
    assert!(
        !dest.exists(),
        "reid must not create dest on corrupt source"
    );

    // squash: same contract.
    let dest = dir.path().join("squash_dst.pile");
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "squash",
            src_path.to_str().unwrap(),
            dest.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(fail_loud.clone());
    assert!(
        !dest.exists(),
        "squash must not create dest on corrupt source"
    );

    // migrate (in-place rewrite): still refuses to open a corrupt pile.
    Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "migrate", src_path.to_str().unwrap(), "list"])
        .assert()
        .failure()
        .stderr(fail_loud);

    let len_after = std::fs::metadata(&src_path).unwrap().len();
    assert_eq!(
        len_before, len_after,
        "source pile must not be truncated by a failed open"
    );
}
