use assert_cmd::Command;
use tempfile::tempdir;
use triblespace::prelude::blobschemas;
use triblespace::prelude::blobschemas::SimpleArchive;
use triblespace::prelude::*;
use triblespace_core::id::id_hex;
use triblespace_core::repo::pile::Pile;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Blake3;

#[test]
fn branch_set_creates_and_updates_with_cas() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-branch-set.pile");
    std::fs::File::create(&pile_path).unwrap();

    let branch_id = id_hex!("22222222222222222222222222222222");

    let (h1, h2) = {
        let mut pile: Pile = Pile::open(&pile_path).unwrap();
        pile.restore().unwrap();

        let mut a = TribleSet::new();
        let a_name = pile
            .put::<blobschemas::LongString, _>("a".to_string())
            .unwrap();
        a += entity! { &ufoid() @ triblespace_core::metadata::name: a_name };
        let h1 = pile.put::<SimpleArchive, _>(a).unwrap();

        let mut b = TribleSet::new();
        let b_name = pile
            .put::<blobschemas::LongString, _>("b".to_string())
            .unwrap();
        b += entity! { &ufoid() @ triblespace_core::metadata::name: b_name };
        let h2 = pile.put::<SimpleArchive, _>(b).unwrap();

        pile.close().unwrap();
        (h1, h2)
    };

    let h1_arg = format!("blake3:{}", hex::encode(h1.raw));
    let h2_arg = format!("blake3:{}", hex::encode(h2.raw));

    // Creates (sets from None).
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "set",
            pile_path.to_str().unwrap(),
            &format!("{branch_id:X}"),
            &h1_arg,
        ])
        .assert()
        .success();

    // Updates using implicit expected=current-head.
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "set",
            pile_path.to_str().unwrap(),
            &format!("{branch_id:X}"),
            &h2_arg,
        ])
        .assert()
        .success();

    // Conflicts with stale expected.
    Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "set",
            pile_path.to_str().unwrap(),
            &format!("{branch_id:X}"),
            &h1_arg,
            "--expected",
            &h1_arg,
        ])
        .assert()
        .failure();

    // Verify the head is set to h2.
    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    pile.restore().unwrap();
    assert_eq!(pile.head(branch_id).unwrap(), Some(h2));
    pile.close().unwrap();
}

#[test]
fn branch_list_all_deleted_lists_deleted_branches() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-branch-journal.pile");
    std::fs::File::create(&pile_path).unwrap();

    let branch_id = id_hex!("33333333333333333333333333333333");

    {
        let mut pile: Pile = Pile::open(&pile_path).unwrap();
        pile.restore().unwrap();

        let mut a = TribleSet::new();
        let a_name = pile
            .put::<blobschemas::LongString, _>("a".to_string())
            .unwrap();
        a += entity! { &ufoid() @ triblespace_core::metadata::name: a_name };
        let h1 = pile.put::<SimpleArchive, _>(a).unwrap();

        pile.update(branch_id, None, Some(h1)).unwrap();
        pile.update(branch_id, Some(h1), None).unwrap();
        pile.close().unwrap();
    }

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "list",
            pile_path.to_str().unwrap(),
            "--all",
            "--deleted",
        ])
        .output()
        .expect("run trible");

    assert!(
        out.status.success(),
        "list --all --deleted failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains(&format!("{branch_id:X}\tdelete\t")),
        "expected branch id and delete state in list --all --deleted output, got:\n{stdout}"
    );
}
