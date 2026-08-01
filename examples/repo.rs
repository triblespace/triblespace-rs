use crate::entity;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::examples::literature;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

fn main() {
    let tmp = tempfile::tempdir().expect("tmp dir");
    let path = tmp.path().join("repo.pile");

    // Create a local pile to store blobs and branches. `open` does not
    // create missing files, and loading is fail-loud: `refresh` errors on
    // a corrupt tail (repair is explicit via `Pile::amputate`).
    std::fs::File::create(&path).expect("create pile file");
    let mut pile = Pile::open(&path).expect("open pile");
    pile.refresh().expect("load pile");

    // Create a repository from the pile and stage the first main workspace.
    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng), TribleSet::new())
        .expect("create repo");
    let identity = repo.branch_identity("main");
    let mut ws1 = repo.create_workspace("main").expect("create workspace");

    // First workspace adds Alice and pushes
    let mut change = TribleSet::new();
    change += entity! { &ufoid() @ literature::firstname: "Alice" };

    ws1.commit(change, "add alice");
    repo.push(&mut ws1).expect("publish ws1");

    // A later workspace resolves the branch assertions and adds Bob.
    let mut ws2 = repo.pull(identity).expect("pull");
    let mut change = TribleSet::new();
    change += entity! { &ufoid() @ literature::firstname: "Bob" };
    ws2.commit(change, "add bob");

    repo.push(&mut ws2).expect("publish ws2");
    println!("Published two signed branch assertions");

    repo.close().expect("close pile");
}
