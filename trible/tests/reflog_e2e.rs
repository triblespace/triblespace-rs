use assert_cmd::Command;
use tempfile::tempdir;
use triblespace::prelude::blobencodings::SimpleArchive;
use triblespace::prelude::*;
use triblespace_core::id::id_hex;
use triblespace_core::repo::pile::Pile;
use triblespace_core::trible::TribleSet;

#[test]
fn reflog_lists_branch_updates_and_tombstones() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-reflog.pile");
    std::fs::File::create(&pile_path).unwrap();

    let branch_id = id_hex!("11111111111111111111111111111111");

    // Create a few branch update records by directly updating the pile's branch store.
    {
        let mut pile: Pile = Pile::open(&pile_path).unwrap();
        pile.restore().unwrap();

        let mut a = TribleSet::new();
        let a_name = pile
            .put::<blobencodings::LongString, _>("a".to_string())
            .unwrap();
        a += entity! { &ufoid() @ triblespace_core::metadata::name: a_name };
        let h1 = pile.put::<SimpleArchive, _>(a).unwrap();

        let mut b = TribleSet::new();
        let b_name = pile
            .put::<blobencodings::LongString, _>("b".to_string())
            .unwrap();
        b += entity! { &ufoid() @ triblespace_core::metadata::name: b_name };
        let h2 = pile.put::<SimpleArchive, _>(b).unwrap();

        // Create/update branch head twice, then tombstone it.
        pile.update(branch_id, None, Some(h1)).unwrap();
        pile.update(branch_id, Some(h1), Some(h2)).unwrap();
        pile.update(branch_id, Some(h2), None).unwrap();

        pile.close().unwrap();
    }

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "reflog",
            pile_path.to_str().unwrap(),
            &format!("{branch_id:X}"),
            "--limit",
            "10",
        ])
        .output()
        .expect("run trible");

    assert!(
        out.status.success(),
        "reflog failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    // Latest-first includes the delete record.
    assert!(
        stdout.contains("\tdelete\t"),
        "expected delete entry in reflog output, got:\n{stdout}"
    );
    assert!(
        stdout.contains("\tset\t"),
        "expected set entry in reflog output, got:\n{stdout}"
    );
}
