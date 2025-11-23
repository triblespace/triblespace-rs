use crate::entity;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("repo.pile");

    // Create a local pile to store blobs and branches
    let mut pile = Pile::open(&path).expect("open pile");
    pile.restore().expect("restore pile");

    // Create a repository from the pile and initialize the main branch
    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws1 = repo.pull(*branch_id).expect("pull");

    // First workspace adds Alice and pushes
    let mut change = TribleSet::new();
    change += entity! { &ufoid() @ literature::firstname: "Alice" };

    ws1.commit(change, Some("add alice"));
    // Single-attempt push; handle conflicts manually when required.
    repo.try_push(&mut ws1).expect("try_push ws1");

    // Second workspace adds Bob and attempts to push, merging on conflict
    let mut ws2 = repo.pull(*branch_id).expect("pull");
    let mut change = TribleSet::new();
    change += entity! { &ufoid() @ literature::firstname: "Bob" };
    ws2.commit(change, Some("add bob"));

    match repo.try_push(&mut ws2).expect("try_push ws2") {
        None => println!("Push ws2 succeeded"),
        Some(mut other) => loop {
            other.merge(&mut ws2).expect("merge");
            match repo.try_push(&mut other).expect("push conflict") {
                None => break,
                Some(next) => other = next,
            }
        },
    }
}
