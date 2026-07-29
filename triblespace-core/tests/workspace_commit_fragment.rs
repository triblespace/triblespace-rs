//! `Workspace::commit` takes a `Fragment`, so a fragment built via
//! `entity!{}` (which may carry blobs from its `*:` spreads or its own
//! `Fragment::put` calls) commits *with* those blobs absorbed into
//! `Workspace::staged`. The blob bytes round-trip through
//! `staged.reader()`.
//!
//! Counter-test: `Fragment::undescribed` is the way to commit a bare
//! `TribleSet`, and it is honest about the consequence — such a commit
//! carries no metadata at all. There is no `From<TribleSet>` to do
//! this silently, which is the point: dropping the descriptions is a
//! decision, not a side effect of the accumulator type you happened to
//! reach for.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace_core::blob::encodings::utf8string::UTF8String;
use triblespace_core::id::rngid;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::prelude::*;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStore, BlobStoreGet, Repository};
use triblespace_core::trible::Fragment;

mod ns {
    use triblespace_core::prelude::*;
    attributes! {
        "DD00000000000000DD00000000000010" as pub note: inlineencodings::Handle<blobencodings::UTF8String>;
    }
}

#[test]
fn commit_fragment_absorbs_blobs() {
    let storage = MemoryRepo::default();
    let mut repo =
        Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    // Build a self-contained Fragment: the note handle bytes live
    // inside the Fragment's own MemoryBlobStore, not in the workspace
    // staging area yet.
    let e = rngid();
    let mut frag = Fragment::empty();
    let note_handle = frag.put::<UTF8String, _>("hello from a fragment");
    frag += entity! { &e @ ns::note: note_handle };

    // Pre-condition: the fresh workspace's staged store does NOT
    // contain the note bytes yet.
    {
        let mut staged = ws.staged.clone();
        let reader = staged.reader().expect("reader");
        assert!(
            reader
                .get::<anybytes::View<str>, UTF8String>(note_handle)
                .is_err(),
            "note bytes shouldn't be in staged before commit"
        );
    }

    // Commit-via-Fragment must absorb the fragment's blobs into staged
    // before producing the commit content blob, so the handle resolves.
    ws.commit(frag, "commit with fragment payload");

    let mut staged = ws.staged.clone();
    let reader = staged.reader().expect("reader");
    let resolved: anybytes::View<str> = reader
        .get::<anybytes::View<str>, UTF8String>(note_handle)
        .expect("note bytes must round-trip through commit absorption");
    assert_eq!(&*resolved, "hello from a fragment");
}

#[test]
fn undescribed_commit_records_no_metadata() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    // A bare TribleSet, deliberately promoted without descriptions.
    let mut data = TribleSet::new();
    let e = rngid();
    let h: triblespace_core::inline::Inline<Handle<UTF8String>> = ws.put("tribleset-side bytes");
    data += entity! { &e @ ns::note: h };
    let expected = data.clone();

    ws.commit(Fragment::undescribed(data), "undescribed commit");
    let commit = ws.head().expect("head");

    // The content is intact...
    let checkout = ws.checkout(commit).expect("checkout");
    assert_eq!(*checkout, expected);

    // ...and the commit carries no metadata handle at all. Not an empty
    // archive — no handle. `undescribed` means undescribed, and the
    // pile records that honestly instead of implying a description that
    // was never supplied.
    let commit_facts: TribleSet = ws.get(commit).expect("commit blob");
    let metadata_handles = find!(
        (h: Inline<_>),
        pattern!(&commit_facts, [{ triblespace_core::repo::metadata: ?h }])
    )
    .count();
    assert_eq!(
        metadata_handles, 0,
        "an undescribed commit must not reference a metadata blob",
    );
    assert!(ws
        .checkout_metadata(commit)
        .expect("checkout metadata")
        .is_empty());

    // Blobs the content references still land in staging, exactly as
    // for a described commit — only the description is absent.
    let mut staged = ws.staged.clone();
    let reader = staged.reader().expect("reader");
    let resolved: anybytes::View<str> = reader
        .get::<anybytes::View<str>, UTF8String>(h)
        .expect("note bytes were already in staged");
    assert_eq!(&*resolved, "tribleset-side bytes");
}
