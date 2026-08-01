use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

fn main() {
    let mut repo = Repository::new(
        MemoryRepo::default(),
        SigningKey::generate(&mut OsRng),
        TribleSet::new(),
    )
    .unwrap();

    // Create a detached workspace and add its first commit. Empty branches are
    // deliberately unrepresentable until a commit is published.
    let mut workspace = repo.create_workspace("feature").expect("create workspace");
    workspace.commit(TribleSet::new(), "start feature work");

    // Publication appends a signed grow-only assertion. Concurrent stale
    // workspaces may publish too; branch resolution later derives the maximal
    // commit frontier instead of returning a CAS conflict.
    repo.push(&mut workspace).expect("publish");
    println!("pushed");
}
