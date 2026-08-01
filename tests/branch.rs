use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::branch_frontier::BranchResolution;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::{PublishOutcome, Repository};

#[test]
fn repository_publishes_a_branch_on_its_first_commit() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(
        storage,
        SigningKey::generate(&mut OsRng),
        triblespace::prelude::TribleSet::new(),
    )
    .unwrap();
    let identity = repo.branch_identity("main");
    let mut workspace = repo.create_workspace("main").expect("create workspace");

    assert!(matches!(
        repo.resolve(&identity).expect("resolve detached workspace"),
        BranchResolution::Absent
    ));

    workspace.commit(triblespace::prelude::TribleSet::new(), "first commit");
    assert!(matches!(
        repo.push(&mut workspace).expect("publish first commit"),
        PublishOutcome::Published(_)
    ));
    repo.pull(identity).expect("pull published branch");
}
