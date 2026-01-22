use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::commit;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

#[test]
fn branch_from_and_pull_with_key() {
    // prepare storage with an initial commit
    let mut store = MemoryRepo::default();
    let key = SigningKey::generate(&mut OsRng);
    let commit_set = commit::commit_metadata(&key, [], None, None, None);
    let initial = store.put(commit_set).unwrap();

    let mut repo = Repository::new(store, key.clone());
    let branch_id = repo
        .create_branch("feature", Some(initial))
        .expect("branch from");
    let mut ws = repo.pull(*branch_id).expect("pull");
    ws.commit(TribleSet::new(), None, Some("work"));
    repo.push(&mut ws).expect("push");

    // pull using a different key should succeed
    let other_key = SigningKey::generate(&mut OsRng);
    repo.pull_with_key(*branch_id, other_key).expect("pull");
}
