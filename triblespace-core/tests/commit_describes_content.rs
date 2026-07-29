//! Every commit describes the attributes its content uses.
//!
//! This is the invariant `trible pile diagnose describes <pile>` audits:
//! it reports commits whose content uses attributes their metadata does
//! not describe. Because `commit()` now writes the content fragment's
//! own metafacts as that commit's metadata, a pile written through the
//! ordinary path has nothing to report — describing and committing are
//! the same act, so there is no opt-in step left to skip.

use std::collections::HashSet;

use ed25519_dalek::SigningKey;
use tempfile::tempdir;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::utf8string::UTF8String;
use triblespace_core::id::{fucid, Id};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::macros::{attributes, entity, find, pattern};
use triblespace_core::metadata;
use triblespace_core::prelude::inlineencodings::ShortString;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{ancestors, Repository, Workspace};
use triblespace_core::trible::TribleSet;

attributes! {
    /// A person's display name.
    "0C0FBB2E5A0B4C0D8E9F1A2B3C4D5E6F" as pub person_name: ShortString;
    /// The city a person lives in.
    "1D1FCC3F6B1C5D1E9FA02B3C4D5E6F70" as pub person_city: ShortString;
    /// A long-form note about a person.
    "2E20DD407C2D6E2FA0B13C4D5E6F7081" as pub person_note: Handle<UTF8String>;
}

fn signing_key() -> SigningKey {
    SigningKey::from_bytes(&[7u8; 32])
}

/// The attribute ids appearing in a set of content facts.
fn attributes_used(facts: &TribleSet) -> HashSet<Id> {
    facts.iter().map(|trible| *trible.a()).collect()
}

/// Reads one commit's metadata handle, mirroring what the diagnose
/// tool has to do to audit a pile.
fn metadata_handle_of<B>(
    ws: &mut Workspace<B>,
    commit: Inline<Handle<SimpleArchive>>,
) -> Option<Inline<Handle<SimpleArchive>>>
where
    B: triblespace_core::repo::BlobStore,
{
    let commit_facts: TribleSet = ws.get(commit).expect("commit blob");
    find!(
        (h: Inline<_>),
        pattern!(&commit_facts, [{ triblespace_core::repo::metadata: ?h }])
    )
    .next()
    .map(|(h,)| h)
}

/// The core audit: every attribute used by `facts` must be described by
/// `meta`. "Described" means the metadata says what encoding the
/// attribute's values are in — the one fact a reader holding nothing
/// but the pile cannot reconstruct.
fn undescribed_attributes(facts: &TribleSet, meta: &TribleSet) -> Vec<Id> {
    attributes_used(facts)
        .into_iter()
        .filter(|attr| {
            let attr = *attr;
            find!(
                (schema: Id),
                pattern!(meta, [{ attr @ metadata::value_encoding: ?schema }])
            )
            .next()
            .is_none()
        })
        .collect()
}

/// A pile written entirely through `commit()` reports zero commits
/// whose content uses attributes their metadata does not describe.
#[test]
fn a_pile_written_through_commit_describes_all_of_its_content() {
    let dir = tempdir().expect("tempdir");
    let pile_path = dir.path().join("described.pile");
    std::fs::File::create(&pile_path).expect("create pile file");

    let branch_id;
    let commits = {
        let pile: Pile = Pile::open(&pile_path).expect("open pile");
        let mut repo = Repository::new(pile, signing_key());
        branch_id = *repo.create_branch("main", None).expect("create branch");
        let mut ws = repo.pull(branch_id).expect("pull");

        for i in 0..20 {
            let person = fucid();
            // Deliberately vary which attributes each commit uses, so
            // the audit cannot pass by accident on a single fixed shape.
            let content = match i % 3 {
                0 => entity! { &person @ person_name: "Alice" },
                1 => entity! { &person @ person_name: "Bob", person_city: "Bremen" },
                _ => entity! { &person @
                    person_city: "Hamburg",
                    person_note: format!("note {i}"),
                },
            };
            ws.commit(content, "write a person");
        }

        repo.push(&mut ws).expect("push");
        let head = ws.head().expect("head");
        let selected = ws.checkout(ancestors(head)).expect("checkout ancestors");
        let commits: Vec<_> = selected.commits().iter().map(|raw| Inline::new(*raw)).collect();
        repo.into_storage().close().expect("close pile");
        commits
    };

    // Re-open the pile from disk: the audit must hold for a reader that
    // has nothing but the file.
    let pile: Pile = Pile::open(&pile_path).expect("reopen pile");
    let mut repo = Repository::new(pile, signing_key());
    let mut ws = repo.pull(branch_id).expect("pull");

    let mut audited = 0usize;
    let mut offenders = Vec::new();
    for commit in commits {
        let facts = ws.checkout(commit).expect("checkout commit");
        if facts.is_empty() {
            continue; // merge commits carry no content to describe
        }
        let meta = ws.checkout_metadata(commit).expect("checkout metadata");
        let missing = undescribed_attributes(&facts, &meta);
        if !missing.is_empty() {
            offenders.push((commit, missing));
        }
        audited += 1;
    }

    assert_eq!(audited, 20, "every content commit must have been audited");
    assert!(
        offenders.is_empty(),
        "commits using attributes their metadata does not describe: {offenders:?}",
    );
}

/// Metadata archives are content-addressed, so a tool committing over
/// the same handful of attributes converges on a handful of distinct
/// metadata blobs rather than one per commit. If this count ever tracks
/// the commit count, something non-deterministic (a timestamp, a freshly
/// minted id) has leaked into the metafacts and defeated dedup.
#[test]
fn repeated_commits_converge_on_few_metadata_blobs() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, signing_key());
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut commits = Vec::new();
    for i in 0..100 {
        let person = fucid();
        // Two distinct attribute shapes across 100 commits.
        let content = if i % 2 == 0 {
            entity! { &person @ person_name: "Alice" }
        } else {
            entity! { &person @ person_name: "Bob", person_city: "Bremen" }
        };
        ws.commit(content, "write a person");
        commits.push(ws.head().expect("head"));
    }

    let mut distinct = HashSet::new();
    for commit in &commits {
        let handle = metadata_handle_of(&mut ws, *commit).expect("commit has metadata");
        distinct.insert(handle.raw);
    }

    assert_eq!(commits.len(), 100);
    assert_eq!(
        distinct.len(),
        2,
        "100 commits over 2 attribute shapes must converge on 2 metadata blobs, got {}",
        distinct.len()
    );
}
