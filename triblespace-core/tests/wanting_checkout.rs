//! Repository over a `Wanting` store — the "lazy checkout" contract.
//!
//! `Repository`/`Workspace` need ZERO changes to run over a
//! `Wanting<S>`: a checkout drives the reader's **sync probe**, so a
//! closure that is only partially present fails with
//! `WantGetError::NotYet` (bubbled through
//! `WorkspaceCheckoutError::Storage`) while the miss has already
//! enqueued a durable weak-pin want for the absent blob — exactly what
//! a sync daemon needs to service the demand before a retry. (An async
//! consumer would instead suspend on the reader's `AsyncBlobStoreGet`
//! until the blob lands.)

use ed25519_dalek::SigningKey;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::prelude::*;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::wanting::WantGetError;
use triblespace_core::repo::WorkspaceCheckoutError;

mod test_ns {
    use triblespace_core::prelude::*;
    attributes! {
        "DD00000000000000DD00000000000002" as pub label: inlineencodings::ShortString;
    }
}

#[test]
fn checkout_over_wanting_fails_notyet_and_enqueues_wants() {
    let key = SigningKey::from_bytes(&[7u8; 32]);

    // ── Source repo: one branch, one commit ──────────────────────────
    let mut repo_a = Repository::new(MemoryRepo::default(), key.clone(), TribleSet::new())
        .expect("source repo");
    let branch_id = *repo_a.create_branch("main", None).expect("create branch");
    let mut ws = repo_a.pull(branch_id).expect("pull");

    let e = triblespace_core::id::rngid();
    let data: TribleSet = entity! { &e @ test_ns::label: "payload" }.into();
    ws.commit(data.clone(), "the payload commit");
    repo_a.push(&mut ws).expect("push");

    // The commit's content blob — the one we withhold from the replica.
    let content_blob: Blob<
        triblespace_core::blob::encodings::simplearchive::SimpleArchive,
    > = data.to_blob();
    let content_handle = content_blob.get_handle();

    // ── Replica: everything EXCEPT the content blob ──────────────────
    let mut replica = MemoryRepo::default();
    {
        let src = repo_a.storage_mut().reader().expect("source reader");
        for handle in src.blobs() {
            let handle = handle.expect("blob handle");
            if handle.raw == content_handle.raw {
                continue; // withhold the content blob
            }
            let blob: Blob<UnknownBlob> = src.get(handle).expect("source blob");
            replica.put::<UnknownBlob, _>(blob).expect("replica put");
        }
    }
    let head = repo_a
        .storage_mut()
        .head(branch_id)
        .expect("head lookup")
        .expect("branch has a head");
    replica
        .update(branch_id, None, Some(head))
        .expect("replica branch pin");

    // ── Lazy checkout over the Wanting replica ────────────────────────
    let wanting = Wanting::new(replica);
    let mut repo_b =
        Repository::new(wanting, key, TribleSet::new()).expect("replica repo");
    let mut ws_b = repo_b.pull(branch_id).expect("pull succeeds — branch meta + commit present");

    let err = ws_b
        .checkout(..)
        .expect_err("partially-absent closure must not check out");
    assert!(
        matches!(err, WorkspaceCheckoutError::Storage(WantGetError::NotYet)),
        "checkout must fail NotYet, got {err:?}"
    );
    drop(ws_b);

    // The miss enqueued a durable want for exactly the withheld blob.
    let wants: Vec<_> = repo_b
        .storage_mut()
        .weak_pins()
        .expect("weak pins")
        .map(Result::unwrap)
        .collect();
    assert!(
        wants.iter().any(|h| h.raw == content_handle.raw),
        "the absent content blob must be enqueued as a want: {wants:?}"
    );
}
