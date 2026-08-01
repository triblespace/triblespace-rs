use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::branch_frontier::BranchResolution;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::{PublishOutcome, Repository};
use triblespace::prelude::*;

#[test]
fn concurrent_publications_form_a_complete_frontier() {
    let storage = MemoryRepo::default();
    let mut repo =
        Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let mut ws1 = repo
        .create_workspace("main")
        .expect("create first workspace");
    let mut ws2 = repo
        .create_workspace("main")
        .expect("create second workspace");

    ws1.commit(TribleSet::new(), "first");
    ws2.commit(TribleSet::new(), "second");

    assert!(matches!(
        repo.push(&mut ws1).expect("publish first workspace"),
        PublishOutcome::Published(_)
    ));
    assert!(matches!(
        repo.push(&mut ws2).expect("publish second workspace"),
        PublishOutcome::Published(_)
    ));

    let BranchResolution::Complete(frontier) =
        repo.resolve(&identity).expect("resolve concurrent tips")
    else {
        panic!("concurrent readable tips must form a complete frontier");
    };
    assert_eq!(frontier.tips().len(), 2);
    repo.pull(identity)
        .expect("a complete divergent frontier is writable");
}
