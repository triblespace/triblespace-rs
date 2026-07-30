//! `lookup_branch` resolves a branch name to an id — and had NO test
//! coverage at all, including for its documented `NameConflict` path.
//!
//! That matters more than usual here, because branch names are not unique and
//! are deliberately not enforced to be: `consolidate --by-name` repairs a
//! collision by minting a fresh id under the SAME name while the old members
//! are still live, and `reid` + `cat` + `consolidate --by-name` exists so two
//! piles that each have a "main" can be merged into one file. So the
//! multiple-match case is a NORMAL state of a real pile, not a corruption —
//! and the one behaviour a caller must be able to rely on is that lookup
//! refuses to guess which one you meant.
//!
//! These tests also pin the equivalence that lets the scan skip a blob read
//! per branch: a branch's name is stored as a content-addressed `LongString`
//! blob and the metadata holds only its handle, so hashing the *sought* name
//! and comparing handles is equivalent to fetching each name and comparing
//! strings — handle equality IS content equality. The equivalence holds
//! because `create_branch_with_key` builds the blob with the same
//! `to_owned().to_blob()` construction, and it is worth pinning precisely
//! because a change to either side would silently break lookup rather than
//! fail to compile.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace_core::prelude::*;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{LookupError, Repository};

fn repo() -> Repository<MemoryRepo> {
    Repository::new(
        MemoryRepo::default(),
        SigningKey::generate(&mut OsRng),
        TribleSet::new(),
    )
    .expect("repo")
}

#[test]
fn lookup_resolves_a_name_to_its_branch() {
    let mut repo = repo();
    let main = repo.create_branch("main", None).expect("main");
    let other = repo.create_branch("feature/x", None).expect("other");

    assert_eq!(
        repo.lookup_branch("main").expect("lookup"),
        Some(*main),
        "a uniquely named branch must resolve to its own id"
    );
    assert_eq!(
        repo.lookup_branch("feature/x").expect("lookup"),
        Some(*other),
        "names are matched exactly, including slashes"
    );
}

#[test]
fn lookup_of_an_absent_name_is_none_not_an_error() {
    let mut repo = repo();
    repo.create_branch("main", None).expect("main");
    assert_eq!(
        repo.lookup_branch("nope").expect("lookup"),
        None,
        "an absent name is a negative answer, not a failure"
    );
}

#[test]
fn nameless_pins_are_not_branch_lookup_errors() {
    let mut repo = repo();
    let main = *repo.create_branch("main", None).expect("main");
    let pin_id = *genid();
    let pin_meta: TribleSet = entity! { triblespace_core::repo::branch: pin_id }.into();
    let pin_head = repo
        .storage_mut()
        .put(pin_meta)
        .expect("store pin metadata");
    assert!(matches!(
        repo.storage_mut().update(pin_id, None, Some(pin_head)),
        Ok(triblespace_core::repo::PushResult::Success())
    ));

    assert_eq!(repo.lookup_branch("main").expect("lookup"), Some(main));
    assert_eq!(repo.lookup_branch("absent").expect("lookup"), None);
}

#[test]
fn multiple_names_on_the_actual_branch_entity_fail_closed() {
    let mut repo = repo();
    let branch_id = *repo.create_branch("main", None).expect("main");
    let old = repo.storage_mut().head(branch_id).unwrap().unwrap();
    let reader = repo.storage_mut().reader().unwrap();
    let mut meta: TribleSet = reader.get(old).unwrap();
    let branch_entity = triblespace_core::repo::branch::branch_entity(&meta, branch_id).unwrap();
    let other_name: Inline<Handle<LongString>> = repo
        .storage_mut()
        .put::<LongString, _>("other".to_owned().to_blob())
        .unwrap();
    meta += entity! { ExclusiveId::force_ref(&branch_entity) @
        triblespace_core::metadata::name: other_name
    };
    let new = repo.storage_mut().put(meta).unwrap();
    assert!(matches!(
        repo.storage_mut().update(branch_id, Some(old), Some(new)),
        Ok(triblespace_core::repo::PushResult::Success())
    ));

    assert_eq!(
        repo.lookup_branch("main").expect("lookup"),
        None,
        "an ambiguous actual branch entity must not answer to either name"
    );
}

/// The documented-but-untested path, and the one that actually occurs in
/// merged piles.
#[test]
fn two_branches_sharing_a_name_are_a_conflict_not_a_guess() {
    let mut repo = repo();
    let a = repo.create_branch("main", None).expect("a");
    let b = repo.create_branch("main", None).expect("b");
    assert_ne!(*a, *b, "create_branch mints a fresh id every time");

    match repo.lookup_branch("main") {
        Err(LookupError::NameConflict(ids)) => {
            assert_eq!(ids.len(), 2, "both branches must be reported, got {ids:?}");
            assert!(ids.contains(&*a) && ids.contains(&*b));
        }
        other => panic!("expected NameConflict listing both branches, got {other:?}"),
    }
}

/// A name is matched by the content of its blob, so two branches created with
/// the same name in separate calls share one name blob and still collide —
/// i.e. the handle comparison is not accidentally distinguishing them by
/// identity. Conversely a name that differs by a single byte must not match.
#[test]
fn name_matching_is_by_content_not_by_identity() {
    let mut repo = repo();
    repo.create_branch("main", None).expect("a");
    repo.create_branch("main", None).expect("b");

    assert!(
        matches!(
            repo.lookup_branch("main"),
            Err(LookupError::NameConflict(_))
        ),
        "separately created equal names must collide — equal content, equal handle"
    );

    assert_eq!(
        repo.lookup_branch("mai").expect("lookup"),
        None,
        "a prefix must not match"
    );
    assert_eq!(
        repo.lookup_branch("main ").expect("lookup"),
        None,
        "a trailing space must not match"
    );
    assert_eq!(
        repo.lookup_branch("Main").expect("lookup"),
        None,
        "matching is case-sensitive"
    );
}

/// Names have no length ceiling — the reason a fast lookup needs no on-disk
/// fixed-width name field. A 4 KiB name resolves like any other.
#[test]
fn a_name_far_longer_than_any_header_field_resolves() {
    let mut repo = repo();
    let long = "b".repeat(4096);
    let id = repo.create_branch(&long, None).expect("long");

    assert_eq!(
        repo.lookup_branch(&long).expect("lookup"),
        Some(*id),
        "a 4 KiB branch name must resolve exactly"
    );

    let mut nearly = long.clone();
    nearly.pop();
    assert_eq!(
        repo.lookup_branch(&nearly).expect("lookup"),
        None,
        "differing in the last byte of 4096 must not match"
    );
}

#[test]
fn ensure_branch_is_idempotent_for_an_existing_name() {
    let mut repo = repo();
    let first = repo.ensure_branch("main", None).expect("first");
    let again = repo.ensure_branch("main", None).expect("again");
    assert_eq!(
        first, again,
        "ensure_branch must return the existing branch, not mint a second"
    );
    assert_eq!(repo.lookup_branch("main").expect("lookup"), Some(first));
}

/// `ensure_branch` cannot recover once a name is already ambiguous: there is
/// no single branch to return. Pinned so the behaviour is a deliberate,
/// visible error rather than an arbitrary pick.
#[test]
fn ensure_branch_refuses_an_already_ambiguous_name() {
    let mut repo = repo();
    repo.create_branch("main", None).expect("a");
    repo.create_branch("main", None).expect("b");

    assert!(
        repo.ensure_branch("main", None).is_err(),
        "ensure_branch must not pick one of two same-named branches"
    );
}
