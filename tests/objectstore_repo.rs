use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::objectstore::ObjectStoreRemote;
use triblespace::core::repo::Repository;
use triblespace::core::repo::{self};
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::prelude::*;
use url::Url;

#[test]
fn objectstore_branch_creates_branch() {
    let url = Url::parse("memory:///repo").unwrap();
    let storage = ObjectStoreRemote::<Blake3>::with_url(&url).unwrap();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");

    repo.pull(*branch_id).expect("pull");
}

#[test]
fn objectstore_workspace_commit_updates_head() {
    let url = Url::parse("memory:///repo2").unwrap();
    let storage = ObjectStoreRemote::<Blake3>::with_url(&url).unwrap();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    ws.commit(TribleSet::new(), None, Some("change"));

    repo.push(&mut ws).expect("push");
}

#[test]
fn objectstore_branch_from_and_pull_with_key() {
    let url = Url::parse("memory:///repo3").unwrap();
    let mut store = ObjectStoreRemote::<Blake3>::with_url(&url).unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let commit_set = repo::commit::commit_metadata(&key, [], None, None, None);
    let initial = store.put(commit_set).unwrap();

    let mut repo = Repository::new(store, key.clone());
    let branch_id = repo
        .create_branch("feature", Some(initial))
        .expect("branch from");
    let mut ws = repo.pull(*branch_id).expect("pull");
    ws.commit(TribleSet::new(), None, Some("work"));
    repo.push(&mut ws).expect("push");

    let other_key = SigningKey::generate(&mut OsRng);
    let branch_id = ws.branch_id();
    repo.pull_with_key(branch_id, other_key).expect("pull");
}

#[test]
fn objectstore_push_and_merge_conflict_resolution() {
    let url = Url::parse("memory:///repo4").unwrap();
    let storage = ObjectStoreRemote::<Blake3>::with_url(&url).unwrap();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws1 = repo.pull(*branch_id).expect("pull");
    let mut ws2 = repo.pull(*branch_id).expect("pull");

    ws1.commit(TribleSet::new(), None, Some("first"));
    ws2.commit(TribleSet::new(), None, Some("second"));

    repo.push(&mut ws1).expect("push");

    let mut conflict_ws = match repo.try_push(&mut ws2).expect("push") {
        Some(ws) => ws,
        _ => panic!("expected conflict"),
    };

    conflict_ws.merge(&mut ws2).unwrap();

    repo.push(&mut conflict_ws).expect("push");
}
