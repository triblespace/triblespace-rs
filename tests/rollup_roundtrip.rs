//! Integration tests for the rollup attribute on branch metadata.
//!
//! The public API is:
//!   - Writer:  `repo.compute_rollup(branch_id)` — pulls, checks out,
//!              builds SuccinctArchive, stores blob, CAS-attaches handle
//!              to branch meta in one call.
//!   - Reader:  `ws.rollup()` — reads the rollup handle (if any) from
//!              the pulled workspace's base branch meta.
//!
//! These tests prove the primitive round-trips and that its failure
//! modes (empty branch, no rollup attached) behave as documented.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::blob::encodings::longstring::LongString;
use triblespace::core::id::{fucid, ExclusiveId};
use triblespace::core::macros::{attributes, entity};
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::{Repository, RollupError};
use triblespace::core::value::encodings::hash::Handle;
use triblespace::core::value::Inline;
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::TribleSet;

attributes! {
    "8D3F3519C4B19203B4B2D2CF9F67F2A1" as title: Handle<LongString>;
    "1E9C3CE3F3D70FF25CF4D9DFB5A6FE29" as friend: GenId;
}

#[test]
fn compute_rollup_round_trips() {
    let storage = MemoryRepo::default();
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repo = Repository::new(storage, signing_key, TribleSet::new()).unwrap();

    let branch_id = repo.create_branch("main", None).unwrap();
    let mut ws = repo.pull(*branch_id).unwrap();

    let alice: ExclusiveId = fucid();
    let bob: ExclusiveId = fucid();
    let alice_title: Inline<Handle<LongString>> = ws.put("Alice".to_string());
    let bob_title: Inline<Handle<LongString>> = ws.put("Bob".to_string());
    let mut change = TribleSet::new();
    change += entity! { &alice @ title: alice_title, friend: &bob };
    change += entity! { &bob @ title: bob_title };
    ws.commit(change, "initial");
    repo.push(&mut ws).unwrap();

    // Single call: pull, checkout, build archive, put blob, CAS branch meta.
    let handle = repo.compute_rollup(*branch_id).unwrap();

    // Reader sees the rollup on a fresh pull.
    let mut ws2 = repo.pull(*branch_id).unwrap();
    assert_eq!(ws2.rollup().unwrap(), Some(handle));
}

#[test]
fn compute_rollup_is_idempotent_on_unchanged_head() {
    let storage = MemoryRepo::default();
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repo = Repository::new(storage, signing_key, TribleSet::new()).unwrap();

    let branch_id = repo.create_branch("main", None).unwrap();
    let mut ws = repo.pull(*branch_id).unwrap();
    let alice: ExclusiveId = fucid();
    let alice_title: Inline<Handle<LongString>> = ws.put("Alice".to_string());
    let change: TribleSet = entity! { &alice @ title: alice_title }.into();
    ws.commit(change, "initial");
    repo.push(&mut ws).unwrap();

    // The archive bytes are content-addressed, so two rollups against the
    // same HEAD produce the same handle.
    let h1 = repo.compute_rollup(*branch_id).unwrap();
    let h2 = repo.compute_rollup(*branch_id).unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn compute_rollup_on_empty_branch_returns_empty_error() {
    let storage = MemoryRepo::default();
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repo = Repository::new(storage, signing_key, TribleSet::new()).unwrap();

    let branch_id = repo.create_branch("main", None).unwrap();
    match repo.compute_rollup(*branch_id) {
        Err(RollupError::EmptyBranch) => {}
        other => panic!("expected EmptyBranch, got {other:?}"),
    }
}

#[test]
fn pull_without_rollup_returns_none() {
    let storage = MemoryRepo::default();
    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repo = Repository::new(storage, signing_key, TribleSet::new()).unwrap();

    let branch_id = repo.create_branch("main", None).unwrap();
    let mut ws = repo.pull(*branch_id).unwrap();

    let alice: ExclusiveId = fucid();
    let alice_title: Inline<Handle<LongString>> = ws.put("Alice".to_string());
    let change: TribleSet = entity! { &alice @ title: alice_title }.into();
    ws.commit(change, "initial");
    repo.push(&mut ws).unwrap();

    let mut ws2 = repo.pull(*branch_id).unwrap();
    assert_eq!(ws2.rollup().unwrap(), None);
}
