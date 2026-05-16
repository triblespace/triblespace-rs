//! `Workspace::commit` accepts `impl Into<Fragment>` so a Fragment
//! built via `entity!{}` (which may carry blobs from its `*:`
//! spreads or its own `Fragment::put` calls) commits *with* those
//! blobs absorbed into `Workspace::staged`. The blob bytes round-trip
//! through `staged.reader()`.
//!
//! Counter-test: passing a raw `TribleSet` works too (auto-promotes
//! to a Fragment with empty blob store), with no behaviour change vs.
//! the pre-Into-flip API.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::id::rngid;
use triblespace_core::prelude::*;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStore, BlobStoreGet, Repository};
use triblespace_core::trible::Fragment;
use triblespace_core::inline::encodings::hash::Handle;

mod ns {
    use triblespace_core::prelude::*;
    attributes! {
        "DD00000000000000DD00000000000010" as pub note: inlineencodings::Handle<blobencodings::LongString>;
    }
}

#[test]
fn commit_fragment_absorbs_blobs() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(
        storage,
        SigningKey::generate(&mut OsRng),
        TribleSet::new(),
    )
    .expect("repo");
    let branch_id = repo.create_branch("main", None).expect("branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    // Build a self-contained Fragment: the note handle bytes live
    // inside the Fragment's own MemoryBlobStore, not in the workspace
    // staging area yet.
    let e = rngid();
    let mut frag = Fragment::empty();
    let note_handle = frag.put::<LongString, _>("hello from a fragment");
    frag += entity! { &e @ ns::note: note_handle };

    // Pre-condition: the fresh workspace's staged store does NOT
    // contain the note bytes yet.
    {
        let mut staged = ws.staged.clone();
        let reader = staged.reader().expect("reader");
        assert!(
            reader
                .get::<anybytes::View<str>, LongString>(note_handle)
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
        .get::<anybytes::View<str>, LongString>(note_handle)
        .expect("note bytes must round-trip through commit absorption");
    assert_eq!(&*resolved, "hello from a fragment");
}

#[test]
fn commit_tribleset_auto_promotes() {
    // The existing TribleSet-callers continue to work via the new
    // `impl From<TribleSet> for Fragment` (lossless promotion: no
    // exports, empty blob store).
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(
        storage,
        SigningKey::generate(&mut OsRng),
        TribleSet::new(),
    )
    .expect("repo");
    let branch_id = repo.create_branch("main", None).expect("branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut data = TribleSet::new();
    let e = rngid();
    // Put the blob via the workspace's staged store the old way,
    // pass a bare TribleSet to commit.
    let h: triblespace_core::inline::Inline<Handle<LongString>> =
        ws.put("tribleset-side bytes");
    data += entity! { &e @ ns::note: h };
    ws.commit(data, "tribleset commit");

    let mut staged = ws.staged.clone();
    let reader = staged.reader().expect("reader");
    let resolved: anybytes::View<str> = reader
        .get::<anybytes::View<str>, LongString>(h)
        .expect("note bytes were already in staged");
    assert_eq!(&*resolved, "tribleset-side bytes");
}
