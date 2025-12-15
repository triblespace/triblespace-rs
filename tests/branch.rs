use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::Repository;

#[test]
fn repository_branch_creates_branch() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");

    match repo.pull(*branch_id) {
        Ok(_) => {}
        Err(_) => panic!("pull failed"),
    }
}
