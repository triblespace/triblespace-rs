use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::{AssertionPullError, Repository};
use triblespace::prelude::*;

#[test]
fn repository_refuses_a_foreign_branch_identity() {
    let owner = SigningKey::generate(&mut OsRng);
    let foreign = SigningKey::generate(&mut OsRng);
    let owner_repo = Repository::new(MemoryRepo::default(), owner, TribleSet::new()).unwrap();
    let identity = owner_repo.branch_identity("feature");
    let mut foreign_repo =
        Repository::new(MemoryRepo::default(), foreign, TribleSet::new()).unwrap();

    assert!(matches!(
        foreign_repo.pull(identity),
        Err(AssertionPullError::ForeignIdentity(_))
    ));
}
