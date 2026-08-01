use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use tempfile::tempdir;

use triblespace::prelude::View;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::repo::branch_assertion::{BranchAssertionStore, BranchIdentity};
use triblespace_core::repo::pile::{Pile, PileRecordContent, PileRecords};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, CommitHandle, Repository};
use triblespace_core::trible::TribleSet;

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

fn key_file(path: &std::path::Path, key: &SigningKey) {
    std::fs::write(path, hex::encode(key.to_bytes())).unwrap();
}

fn seed_commit(path: &std::path::Path, key: &SigningKey) -> (BranchIdentity, CommitHandle) {
    std::fs::File::create(path).unwrap();
    let pile = Pile::open(path).unwrap();
    let mut repository = Repository::new(pile, key.clone(), TribleSet::new()).unwrap();
    let mut workspace = repository.create_workspace("source").unwrap();
    let identity = *workspace.identity();
    workspace.commit(TribleSet::new(), "seed");
    let commit = workspace.head().unwrap();
    repository.push(&mut workspace).unwrap();
    repository.close().unwrap();
    (identity, commit)
}

#[test]
fn exact_assert_list_show_log_and_local_forget_compose() {
    let directory = tempdir().unwrap();
    let source = directory.path().join("source.pile");
    let forgotten = directory.path().join("forgotten.pile");
    let first_key = key(7);
    let second_key = key(11);
    let first_key_path = directory.path().join("first.key");
    let second_key_path = directory.path().join("second.key");
    key_file(&first_key_path, &first_key);
    key_file(&second_key_path, &second_key);
    let (source_identity, commit) = seed_commit(&source, &first_key);
    let commit_text = format!("blake3:{}", hex::encode(commit.raw));
    let name_blob: Blob<LongString> = "main".to_owned().to_blob();
    let first_identity = BranchIdentity::new(first_key.verifying_key(), name_blob.get_handle());
    let second_identity = BranchIdentity::new(second_key.verifying_key(), name_blob.get_handle());

    for key_path in [&first_key_path, &second_key_path] {
        Command::cargo_bin("trible")
            .unwrap()
            .args([
                "pile",
                "branch",
                "assert",
                source.to_str().unwrap(),
                "main",
                &commit_text,
                "--signing-key",
                key_path.to_str().unwrap(),
            ])
            .assert()
            .success();
    }

    let before_duplicate = std::fs::metadata(&source).unwrap().len();
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "assert",
            source.to_str().unwrap(),
            "main",
            &commit_text,
            "--signing-key",
            first_key_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert_eq!(
        std::fs::metadata(&source).unwrap().len(),
        before_duplicate,
        "publishing an identical assertion must not append physical records"
    );

    let all = Command::cargo_bin("trible")
        .unwrap()
        .args(["pile", "branch", "list", source.to_str().unwrap(), "--all"])
        .output()
        .unwrap();
    assert!(
        all.status.success(),
        "{}",
        String::from_utf8_lossy(&all.stderr)
    );
    let all = String::from_utf8(all.stdout).unwrap();
    assert!(all.contains(&first_identity.to_string()));
    assert!(all.contains(&second_identity.to_string()));
    assert!(all.contains("state=complete"));
    assert!(all.contains("name=\"main\""));

    let own = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "list",
            source.to_str().unwrap(),
            "--signing-key",
            first_key_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        own.status.success(),
        "{}",
        String::from_utf8_lossy(&own.stderr)
    );
    let own = String::from_utf8(own.stdout).unwrap();
    assert!(own.contains(&first_identity.to_string()));
    assert!(own.contains(&source_identity.to_string()));
    assert!(!own.contains(&second_identity.to_string()));

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "show",
            source.to_str().unwrap(),
            &first_identity.to_string(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Resolution: complete"))
        .stdout(predicates::str::contains("Advance-safe: yes"))
        .stdout(predicates::str::contains("(existing)"));

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "log",
            source.to_str().unwrap(),
            &first_identity.to_string(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains(format!(
            "depth=0\tcommit={commit_text}\tstatus=present"
        )));

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "diagnose",
            "branch-history",
            source.to_str().unwrap(),
            &first_identity.to_string(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "Physical assertion arrival order only",
        ))
        .stdout(predicates::str::contains(format!(
            "branch={first_identity}"
        )))
        .stdout(predicates::str::contains("duplicate=no"));

    let source_before = std::fs::read(&source).unwrap();
    let expected_forgotten = {
        let mut records = PileRecords::open(&source).unwrap();
        let raw = records.bytes().to_vec();
        let mut retained = Vec::new();
        for record in &mut records {
            let record = record.unwrap();
            let forgotten = matches!(
                record.content,
                PileRecordContent::BranchAssertion { assertion }
                    if assertion.identity() == &first_identity
            );
            if !forgotten {
                retained.extend_from_slice(&raw[record.offset..record.offset + record.len]);
            }
        }
        retained
    };
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "forget",
            source.to_str().unwrap(),
            forgotten.to_str().unwrap(),
            &first_identity.to_string(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "Warning: this is local physical forgetting",
        ));
    assert_eq!(std::fs::read(&source).unwrap(), source_before);
    assert_eq!(
        std::fs::read(&forgotten).unwrap(),
        expected_forgotten,
        "forget must copy every retained raw record byte-for-byte in log order"
    );

    let mut pile = Pile::open(&forgotten).unwrap();
    pile.refresh().unwrap();
    let snapshot = pile.assertion_snapshot().unwrap();
    assert!(snapshot.for_branch(&first_identity).is_empty());
    assert_eq!(snapshot.for_branch(&second_identity).len(), 1);
    assert_eq!(snapshot.for_branch(&source_identity).len(), 1);
    let reader = pile.reader().unwrap();
    let _: TribleSet = reader.get(commit).unwrap();
    let name: View<str> = reader.get(first_identity.name()).unwrap();
    assert_eq!(name.as_ref(), "main");
    drop(reader);
    pile.close().unwrap();
}

#[test]
fn assert_refuses_an_absent_commit_without_publishing() {
    let directory = tempdir().unwrap();
    let pile_path = directory.path().join("absent.pile");
    std::fs::File::create(&pile_path).unwrap();
    let key = key(7);
    let key_path = directory.path().join("key");
    key_file(&key_path, &key);
    let before = std::fs::metadata(&pile_path).unwrap().len();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "assert",
            pile_path.to_str().unwrap(),
            "main",
            &format!("blake3:{}", "11".repeat(32)),
            "--signing-key",
            key_path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "canonical commit metadata is not present",
        ));

    assert_eq!(std::fs::metadata(&pile_path).unwrap().len(), before);
    let mut pile = Pile::open(&pile_path).unwrap();
    assert!(pile.assertion_snapshot().unwrap().is_empty());
    pile.close().unwrap();
}

#[test]
fn forget_refuses_to_replace_an_existing_destination() {
    let directory = tempdir().unwrap();
    let source = directory.path().join("source.pile");
    let destination = directory.path().join("existing.pile");
    let (identity, _) = seed_commit(&source, &key(7));
    let existing = b"irreplaceable destination";
    std::fs::write(&destination, existing).unwrap();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "forget",
            source.to_str().unwrap(),
            destination.to_str().unwrap(),
            &identity.to_string(),
        ])
        .assert()
        .failure();

    assert_eq!(std::fs::read(&destination).unwrap(), existing);
}

#[test]
fn assert_refuses_present_malformed_commit_without_writing() {
    let directory = tempdir().unwrap();
    let pile_path = directory.path().join("malformed.pile");
    std::fs::File::create(&pile_path).unwrap();
    let signing_key = key(7);
    let key_path = directory.path().join("key");
    key_file(&key_path, &signing_key);

    let mut pile = Pile::open(&pile_path).unwrap();
    let malformed = pile
        .put::<SimpleArchive, _>(TribleSet::new())
        .expect("store a hash-valid but non-commit archive");
    pile.close().unwrap();
    let before = std::fs::metadata(&pile_path).unwrap().len();

    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "assert",
            pile_path.to_str().unwrap(),
            "main",
            &format!("blake3:{}", hex::encode(malformed.raw)),
            "--signing-key",
            key_path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicates::str::contains("malformed commit metadata"));

    assert_eq!(std::fs::metadata(&pile_path).unwrap().len(), before);
    let mut pile = Pile::open(&pile_path).unwrap();
    assert!(pile.assertion_snapshot().unwrap().is_empty());
    pile.close().unwrap();
}
