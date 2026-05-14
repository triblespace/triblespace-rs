use assert_cmd::Command;
use ed25519_dalek::SigningKey;
use std::collections::HashSet;
use std::convert::TryInto;
use tempfile::tempdir;
use triblespace::prelude::blobschemas::SimpleArchive;
use triblespace::prelude::*;
use triblespace_core::id::id_hex;
use triblespace_core::metadata;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::Repository;
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::Handle;
use triblespace_core::value::Inline;

fn random_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).expect("getrandom");
    SigningKey::from_bytes(&seed)
}

/// End-to-end test: create multiple branches with the same name, run the
/// consolidate command and verify the resulting merge commit parents match
/// the original branch heads.
#[test]
fn consolidate_merges_branch_heads() {
    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-consolidate.pile");
    std::fs::File::create(&pile_path).unwrap();

    // Create a repository and three branches with the same name.
    let mut original_heads: Vec<String> = Vec::new();
    let mut branch_ids: Vec<String> = Vec::new();
    {
        let pile: Pile = Pile::open(&pile_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();

        for i in 0..3 {
            let branch_id = repo.create_branch("mem", None).expect("create branch");
            branch_ids.push(format!("{:X}", *branch_id));
            let mut ws = repo.pull(*branch_id).expect("pull");
            let e = ufoid();
            let mut content = TribleSet::new();
            let label = ws.put::<blobschemas::LongString, _>(format!("branch-{i}"));
            content += entity! { &e @ metadata::name: label };
            ws.commit(content, &format!("commit-{i}"));

            // Push and assert no conflict
            let res = repo.try_push(&mut ws).expect("push");
            assert!(res.is_none(), "unexpected push conflict");

            let head = ws.head().expect("head present");
            let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                Handle::to_hash(head);
            original_heads.push(hh.from_inline());
        }
        repo.close().unwrap();
    }

    // Write a signing key file (hex) used by the trible CLI when creating the merge commit.
    let sk = random_signing_key();
    let sk_hex = hex::encode(sk.to_bytes());
    let key_path = dir.path().join("signing.key");
    std::fs::write(&key_path, sk_hex).unwrap();

    // Run the CLI consolidate command
    let mut args: Vec<String> = vec![
        "pile".to_string(),
        "branch".to_string(),
        "consolidate".to_string(),
        pile_path.to_str().unwrap().to_string(),
    ];
    args.extend(branch_ids);
    args.extend([
        "--out-name".to_string(),
        "mem-out".to_string(),
        "--signing-key".to_string(),
        key_path.to_str().unwrap().to_string(),
    ]);

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args(args)
        .output()
        .expect("run trible");

    assert!(
        out.status.success(),
        "consolidate failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);

    // Parse new branch id (32 hex chars)
    let id_hex = stdout
        .split_whitespace()
        .rev()
        .find(|tok| tok.len() == 32 && tok.chars().all(|c| c.is_ascii_hexdigit()))
        .expect("new branch id in output");

    // Open the pile and read the resulting branch metadata and commit
    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    pile.refresh().unwrap();
    let raw = hex::decode(id_hex).unwrap();
    let raw16: [u8; 16] = raw.as_slice().try_into().unwrap();
    let bid = triblespace_core::id::Id::new(raw16).unwrap();

    let reader = pile.reader().unwrap();
    let meta_handle = pile.head(bid).unwrap().expect("new branch metadata");
    let meta: TribleSet = reader.get(meta_handle).unwrap();

    // repo head attribute id
    let repo_head_attr: triblespace_core::id::Id = id_hex!("272FBC56108F336C4D2E17289468C35F");
    let repo_parent_attr: triblespace_core::id::Id = id_hex!("317044B612C690000D798CA660ECFD2A");

    // extract the commit handle for the branch head
    let mut head_handle_opt: Option<Inline<Handle<SimpleArchive>>> = None;
    for t in meta.iter() {
        if t.a() == &repo_head_attr {
            head_handle_opt = Some(*t.v::<Handle<SimpleArchive>>());
            break;
        }
    }
    let head_handle = head_handle_opt.expect("branch head set");

    // read commit metadata
    let commit_meta: TribleSet = reader.get(head_handle).unwrap();

    // collect parent commits
    let mut parents: HashSet<String> = HashSet::new();
    for t in commit_meta.iter() {
        if t.a() == &repo_parent_attr {
            let p = *t.v::<Handle<SimpleArchive>>();
            let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                Handle::to_hash(p);
            parents.insert(hh.from_inline());
        }
    }

    // original_heads may contain duplicates if some branches had no head; use set
    let orig_set: HashSet<String> = original_heads.into_iter().collect();
    assert_eq!(
        parents, orig_set,
        "parents of merge commit do not match original heads"
    );
    drop(reader);
    pile.close().unwrap();
}

/// End-to-end test for --by-name-include-deleted: create branches, tombstone some,
/// run consolidate --by-name-include-deleted, and verify same-name branches are merged
/// with subsumption detection.
#[test]
fn consolidate_by_name_include_deleted_recovers_tombstoned_branches() {
    use triblespace_core::repo::BranchStore;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-include-deleted.pile");
    std::fs::File::create(&pile_path).unwrap();

    // Scenario:
    // - Create branch "alpha" (branch A) with commit C1, then tombstone it.
    // - Create branch "alpha" (branch B) with commit C2 (independent).
    // - Create branch "beta" (branch C) with commit C3, leave active.
    //
    // Expected result of --by-name-include-deleted:
    // - "alpha" group: C1 and C2 are independent → merge commit with both as parents.
    // - "beta" group: single head C3 → branch created directly (no merge commit).

    let mut alpha_heads: Vec<String> = Vec::new();
    let beta_head: String;
    let alpha_a_id: String;
    {
        let pile: Pile = Pile::open(&pile_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();

        // Branch A: "alpha", commit, push, tombstone.
        let bid_a = repo.create_branch("alpha", None).expect("create alpha-A");
        alpha_a_id = format!("{:X}", *bid_a);
        let mut ws = repo.pull(*bid_a).expect("pull alpha-A");
        let e = ufoid();
        let mut content = TribleSet::new();
        let label = ws.put::<blobschemas::LongString, _>("alpha-A-data".to_string());
        content += entity! { &e @ metadata::name: label };
        ws.commit(content, "alpha-A commit");
        assert!(repo.try_push(&mut ws).expect("push").is_none());
        let head_a = ws.head().expect("head");
        let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
            Handle::to_hash(head_a);
        alpha_heads.push(hh.from_inline());

        // Tombstone branch A.
        let old = repo.storage_mut().head(*bid_a).unwrap().unwrap();
        match repo.storage_mut().update(*bid_a, Some(old), None).unwrap() {
            triblespace_core::repo::PushResult::Success() => {}
            _ => panic!("failed to tombstone branch A"),
        }

        // Branch B: "alpha", independent commit, push, leave active.
        let bid_b = repo.create_branch("alpha", None).expect("create alpha-B");
        let mut ws = repo.pull(*bid_b).expect("pull alpha-B");
        let e = ufoid();
        let mut content = TribleSet::new();
        let label = ws.put::<blobschemas::LongString, _>("alpha-B-data".to_string());
        content += entity! { &e @ metadata::name: label };
        ws.commit(content, "alpha-B commit");
        assert!(repo.try_push(&mut ws).expect("push").is_none());
        let head_b = ws.head().expect("head");
        let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
            Handle::to_hash(head_b);
        alpha_heads.push(hh.from_inline());

        // Branch C: "beta", commit, push, leave active.
        let bid_c = repo.create_branch("beta", None).expect("create beta");
        let mut ws = repo.pull(*bid_c).expect("pull beta");
        let e = ufoid();
        let mut content = TribleSet::new();
        let label = ws.put::<blobschemas::LongString, _>("beta-data".to_string());
        content += entity! { &e @ metadata::name: label };
        ws.commit(content, "beta commit");
        assert!(repo.try_push(&mut ws).expect("push").is_none());
        let head_c = ws.head().expect("head");
        let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
            Handle::to_hash(head_c);
        beta_head = hh.from_inline();

        repo.close().unwrap();
    }

    // Verify branch A is actually tombstoned.
    {
        let mut pile: Pile = Pile::open(&pile_path).unwrap();
        pile.refresh().unwrap();
        let raw = hex::decode(&alpha_a_id).unwrap();
        let raw16: [u8; 16] = raw.as_slice().try_into().unwrap();
        let bid = triblespace_core::id::Id::new(raw16).unwrap();
        assert!(
            pile.head(bid).unwrap().is_none(),
            "branch A should be tombstoned"
        );
        pile.close().unwrap();
    }

    // Write signing key.
    let sk = random_signing_key();
    let sk_hex = hex::encode(sk.to_bytes());
    let key_path = dir.path().join("signing.key");
    std::fs::write(&key_path, sk_hex).unwrap();

    // Run consolidate --by-name-include-deleted.
    let args = vec![
        "pile",
        "branch",
        "consolidate",
        pile_path.to_str().unwrap(),
        "--by-name-include-deleted",
        "--signing-key",
        key_path.to_str().unwrap(),
    ];

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args(args)
        .output()
        .expect("run trible");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "consolidate --by-name-include-deleted failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Verify: should have created branches for both "alpha" and "beta".
    assert!(
        stdout.contains("created branch 'alpha'"),
        "expected alpha branch in output:\n{stdout}"
    );
    // beta has a single active branch — should be recognized as already consolidated.
    assert!(
        stdout.contains("already consolidated"),
        "expected 'already consolidated' for beta in output:\n{stdout}"
    );

    // Open the pile and verify the resulting branches.
    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    pile.refresh().unwrap();
    let reader = pile.reader().unwrap();

    let repo_head_attr: triblespace_core::id::Id = id_hex!("272FBC56108F336C4D2E17289468C35F");
    let repo_parent_attr: triblespace_core::id::Id = id_hex!("317044B612C690000D798CA660ECFD2A");
    let name_attr = triblespace_core::metadata::name.id();

    // Collect all branches by name.
    let mut found: std::collections::HashMap<String, Vec<triblespace_core::id::Id>> =
        std::collections::HashMap::new();
    for branch_res in pile.branches().unwrap() {
        let bid = branch_res.unwrap();
        let mh = pile.head(bid).unwrap().unwrap();
        let meta: TribleSet = reader.get(mh).unwrap();
        let mut branch_name = None;
        for t in meta.iter() {
            if t.a() == &name_attr {
                let h: Inline<Handle<blobschemas::LongString>> = *t.v();
                if let Ok(view) = reader.get::<View<str>, _>(h) {
                    branch_name = Some(view.as_ref().to_string());
                }
            }
        }
        if let Some(name) = branch_name {
            found.entry(name).or_default().push(bid);
        }
    }

    // "alpha" group should have a new consolidated branch.
    let alpha_branches = found.get("alpha").expect("alpha branches exist");
    // Find the consolidated one (has a merge commit with both alpha heads as parents).
    let mut found_merge = false;
    for &bid in alpha_branches {
        let mh = pile.head(bid).unwrap().unwrap();
        let meta: TribleSet = reader.get(mh).unwrap();
        let mut head_handle = None;
        for t in meta.iter() {
            if t.a() == &repo_head_attr {
                head_handle = Some(*t.v::<Handle<SimpleArchive>>());
            }
        }
        if let Some(hh) = head_handle {
            if let Ok(commit_meta) = reader.get::<TribleSet, SimpleArchive>(hh) {
                let mut parents: HashSet<String> = HashSet::new();
                for t in commit_meta.iter() {
                    if t.a() == &repo_parent_attr {
                        let p = *t.v::<Handle<SimpleArchive>>();
                        let hash: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                            Handle::to_hash(p);
                        parents.insert(hash.from_inline());
                    }
                }
                let alpha_set: HashSet<String> = alpha_heads.iter().cloned().collect();
                if parents == alpha_set {
                    found_merge = true;
                }
            }
        }
    }
    assert!(
        found_merge,
        "expected a consolidated alpha branch with both original heads as parents"
    );

    // "beta" group should exist with its original head (no merge commit).
    let beta_branches = found.get("beta").expect("beta branches exist");
    let mut found_beta_direct = false;
    for &bid in beta_branches {
        let mh = pile.head(bid).unwrap().unwrap();
        let meta: TribleSet = reader.get(mh).unwrap();
        let mut head_handle = None;
        for t in meta.iter() {
            if t.a() == &repo_head_attr {
                head_handle = Some(*t.v::<Handle<SimpleArchive>>());
            }
        }
        if let Some(hh) = head_handle {
            let hash: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                Handle::to_hash(hh);
            if hash.from_inline::<String>() == beta_head {
                found_beta_direct = true;
            }
        }
    }
    assert!(
        found_beta_direct,
        "expected beta branch with original head directly"
    );

    drop(reader);
    pile.close().unwrap();
}

/// Test subsumption: when one head is an ancestor of another in the same name
/// group, only the descendant should be kept.
#[test]
fn consolidate_by_name_include_deleted_detects_subsumption() {
    use triblespace_core::repo::BranchStore;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-subsumption.pile");
    std::fs::File::create(&pile_path).unwrap();

    // Scenario:
    // - Create branch "gamma" (branch A) with commit C1, push, tombstone.
    // - Create branch "gamma" (branch B), pull from A's commit (C1 as ancestor),
    //   add commit C2 on top, push.
    // Expected: C1 is subsumed by C2. Only one non-subsumed head → no merge commit.

    let descendant_head: String;
    {
        let pile: Pile = Pile::open(&pile_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();

        // Branch A: "gamma", commit C1.
        let bid_a = repo.create_branch("gamma", None).expect("create gamma-A");
        let mut ws_a = repo.pull(*bid_a).expect("pull gamma-A");
        let e = ufoid();
        let mut content = TribleSet::new();
        let label = ws_a.put::<blobschemas::LongString, _>("gamma-A".to_string());
        content += entity! { &e @ metadata::name: label };
        ws_a.commit(content, "gamma-A commit");
        assert!(repo.try_push(&mut ws_a).expect("push").is_none());
        let head_a = ws_a.head().expect("head");

        // Tombstone branch A.
        let old = repo.storage_mut().head(*bid_a).unwrap().unwrap();
        match repo.storage_mut().update(*bid_a, Some(old), None).unwrap() {
            triblespace_core::repo::PushResult::Success() => {}
            _ => panic!("tombstone failed"),
        }

        // Branch B: "gamma", start from C1 (merge it in), add C2 on top.
        let bid_b = repo.create_branch("gamma", None).expect("create gamma-B");
        let mut ws_b = repo.pull(*bid_b).expect("pull gamma-B");
        // Merge branch A's head into B so C1 becomes an ancestor of C2.
        ws_b.merge_commit(head_a).expect("merge C1 into B");
        let e2 = ufoid();
        let mut content2 = TribleSet::new();
        let label2 = ws_b.put::<blobschemas::LongString, _>("gamma-B".to_string());
        content2 += entity! { &e2 @ metadata::name: label2 };
        ws_b.commit(content2, "gamma-B commit");
        assert!(repo.try_push(&mut ws_b).expect("push").is_none());
        let head_b = ws_b.head().expect("head");
        let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
            Handle::to_hash(head_b);
        descendant_head = hh.from_inline();

        repo.close().unwrap();
    }

    // Write signing key.
    let sk = random_signing_key();
    let key_path = dir.path().join("signing.key");
    std::fs::write(&key_path, hex::encode(sk.to_bytes())).unwrap();

    // Run consolidate --by-name-include-deleted.
    let out = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "consolidate",
            pile_path.to_str().unwrap(),
            "--by-name-include-deleted",
            "--signing-key",
            key_path.to_str().unwrap(),
        ])
        .output()
        .expect("run trible");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "consolidate failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Should report subsumption.
    assert!(
        stdout.contains("subsumed"),
        "expected subsumption message in output:\n{stdout}"
    );
    // gamma B is active and has the sole non-subsumed head → already consolidated.
    assert!(
        stdout.contains("already consolidated"),
        "expected 'already consolidated' in output:\n{stdout}"
    );

    // Verify: the existing active gamma branch still points to the descendant head.
    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    pile.refresh().unwrap();
    let reader = pile.reader().unwrap();

    let repo_head_attr: triblespace_core::id::Id = id_hex!("272FBC56108F336C4D2E17289468C35F");
    let name_attr = triblespace_core::metadata::name.id();

    let mut found_gamma = false;
    for branch_res in pile.branches().unwrap() {
        let bid = branch_res.unwrap();
        let mh = pile.head(bid).unwrap().unwrap();
        let meta: TribleSet = reader.get(mh).unwrap();
        let mut is_gamma = false;
        let mut head_handle = None;
        for t in meta.iter() {
            if t.a() == &name_attr {
                let h: Inline<Handle<blobschemas::LongString>> = *t.v();
                if let Ok(view) = reader.get::<View<str>, _>(h) {
                    if view.as_ref() == "gamma" {
                        is_gamma = true;
                    }
                }
            }
            if t.a() == &repo_head_attr {
                head_handle = Some(*t.v::<Handle<SimpleArchive>>());
            }
        }
        if is_gamma {
            if let Some(hh) = head_handle {
                let hash: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                    Handle::to_hash(hh);
                let hex: String = hash.from_inline();
                // The active branch should still point to the descendant head.
                assert_eq!(
                    hex, descendant_head,
                    "gamma branch should point to descendant"
                );
                found_gamma = true;
            }
        }
    }
    assert!(found_gamma, "expected to find gamma branch");

    drop(reader);
    pile.close().unwrap();
}

/// Test --by-name: consolidate active branches by name (no tombstone scanning).
#[test]
fn consolidate_by_name_merges_active_same_name_branches() {
    use triblespace_core::repo::BranchStore;

    let dir = tempdir().unwrap();
    let pile_path = dir.path().join("test-by-name.pile");
    std::fs::File::create(&pile_path).unwrap();

    // Create two active branches named "delta" with independent commits.
    let mut delta_heads: Vec<String> = Vec::new();
    {
        let pile: Pile = Pile::open(&pile_path).unwrap();
        let mut repo = Repository::new(pile, random_signing_key(), TribleSet::new()).unwrap();

        for i in 0..2 {
            let bid = repo
                .create_branch("delta", None)
                .expect("create delta branch");
            let mut ws = repo.pull(*bid).expect("pull");
            let e = ufoid();
            let mut content = TribleSet::new();
            let label = ws.put::<blobschemas::LongString, _>(format!("delta-{i}"));
            content += entity! { &e @ metadata::name: label };
            ws.commit(content, &format!("delta-{i}"));
            assert!(repo.try_push(&mut ws).expect("push").is_none());
            let head = ws.head().expect("head");
            let hh: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                Handle::to_hash(head);
            delta_heads.push(hh.from_inline());
        }

        // Also create a single "epsilon" branch.
        let bid = repo
            .create_branch("epsilon", None)
            .expect("create epsilon");
        let mut ws = repo.pull(*bid).expect("pull");
        let e = ufoid();
        let mut content = TribleSet::new();
        let label = ws.put::<blobschemas::LongString, _>("epsilon-data".to_string());
        content += entity! { &e @ metadata::name: label };
        ws.commit(content, "epsilon");
        assert!(repo.try_push(&mut ws).expect("push").is_none());

        repo.close().unwrap();
    }

    let sk = random_signing_key();
    let key_path = dir.path().join("signing.key");
    std::fs::write(&key_path, hex::encode(sk.to_bytes())).unwrap();

    let out = Command::cargo_bin("trible")
        .unwrap()
        .args([
            "pile",
            "branch",
            "consolidate",
            pile_path.to_str().unwrap(),
            "--by-name",
            "--signing-key",
            key_path.to_str().unwrap(),
        ])
        .output()
        .expect("run trible");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "consolidate --by-name failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    assert!(
        stdout.contains("created branch 'delta'"),
        "expected delta in output:\n{stdout}"
    );
    // epsilon has a single active branch — already consolidated.
    assert!(
        stdout.contains("already consolidated"),
        "expected 'already consolidated' for epsilon in output:\n{stdout}"
    );

    // Verify delta has a merge commit with both heads.
    let mut pile: Pile = Pile::open(&pile_path).unwrap();
    pile.refresh().unwrap();
    let reader = pile.reader().unwrap();

    let repo_head_attr: triblespace_core::id::Id = id_hex!("272FBC56108F336C4D2E17289468C35F");
    let repo_parent_attr: triblespace_core::id::Id = id_hex!("317044B612C690000D798CA660ECFD2A");
    let name_attr = triblespace_core::metadata::name.id();

    let mut found_delta_merge = false;
    for branch_res in pile.branches().unwrap() {
        let bid = branch_res.unwrap();
        let mh = pile.head(bid).unwrap().unwrap();
        let meta: TribleSet = reader.get(mh).unwrap();
        let mut is_delta = false;
        let mut head_handle = None;
        for t in meta.iter() {
            if t.a() == &name_attr {
                let h: Inline<Handle<blobschemas::LongString>> = *t.v();
                if let Ok(view) = reader.get::<View<str>, _>(h) {
                    if view.as_ref() == "delta" {
                        is_delta = true;
                    }
                }
            }
            if t.a() == &repo_head_attr {
                head_handle = Some(*t.v::<Handle<SimpleArchive>>());
            }
        }
        if is_delta {
            if let Some(hh) = head_handle {
                if let Ok(commit_meta) = reader.get::<TribleSet, SimpleArchive>(hh) {
                    let mut parents: HashSet<String> = HashSet::new();
                    for t in commit_meta.iter() {
                        if t.a() == &repo_parent_attr {
                            let p = *t.v::<Handle<SimpleArchive>>();
                            let hash: Inline<triblespace_core::value::schemas::hash::Hash<triblespace_core::value::schemas::hash::Blake3>> =
                                Handle::to_hash(p);
                            parents.insert(hash.from_inline());
                        }
                    }
                    let expected: HashSet<String> = delta_heads.iter().cloned().collect();
                    if parents == expected {
                        found_delta_merge = true;
                    }
                }
            }
        }
    }
    assert!(
        found_delta_merge,
        "expected delta branch with merge commit containing both original heads"
    );

    drop(reader);
    pile.close().unwrap();
}
