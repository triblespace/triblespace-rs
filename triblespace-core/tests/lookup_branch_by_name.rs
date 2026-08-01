//! Branch names now identify a deterministic `(author, name handle)` pair.
//! Resolution is exact and assertion-driven; there is no mutable name index
//! and no ambiguity-producing fresh branch id.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace_core::prelude::*;
use triblespace_core::repo::branch_frontier::BranchResolution;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::Repository;

fn repo() -> Repository<MemoryRepo> {
    Repository::new(
        MemoryRepo::default(),
        SigningKey::generate(&mut OsRng),
        TribleSet::new(),
    )
    .expect("repo")
}

#[test]
fn equal_names_under_one_author_have_one_identity() {
    let repo = repo();
    assert_eq!(repo.branch_identity("main"), repo.branch_identity("main"));
}

#[test]
fn name_matching_is_exact_and_content_derived() {
    let repo = repo();
    let main = repo.branch_identity("main");

    assert_ne!(main, repo.branch_identity("mai"));
    assert_ne!(main, repo.branch_identity("main "));
    assert_ne!(main, repo.branch_identity("Main"));
    assert_ne!(main, repo.branch_identity("feature/main"));
}

#[test]
fn the_author_is_part_of_the_branch_identity() {
    let repo_a = repo();
    let repo_b = repo();
    assert_ne!(
        repo_a.branch_identity("main"),
        repo_b.branch_identity("main")
    );
}

#[test]
fn long_names_are_valid_identity_material() {
    let repo = repo();
    let long = "b".repeat(4096);
    let mut nearly = long.clone();
    nearly.pop();

    assert_eq!(repo.branch_identity(&long), repo.branch_identity(&long));
    assert_ne!(repo.branch_identity(&long), repo.branch_identity(&nearly));
}

#[test]
fn an_unpublished_name_resolves_as_absent() {
    let mut repo = repo();
    let workspace = repo.create_workspace("main").expect("workspace");
    assert_eq!(*workspace.identity(), repo.branch_identity("main"));
    assert!(matches!(
        repo.resolve_name("main").expect("resolution"),
        BranchResolution::Absent
    ));
}
