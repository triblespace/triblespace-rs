//! Verifies that `update_tracking_branch` rejects gossip updates whose
//! `metadata::updated_at` is not strictly newer than what the tracking
//! branch already holds. This is the regression protection that prevents
//! a late-finishing fetch for an older HEAD from clobbering a newer HEAD
//! that already advanced the tracking branch.

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::id::{genid, Id};
use triblespace_core::macros::entity;
use triblespace_core::prelude::BranchStore;
use triblespace_core::repo::branch::branch_unsigned;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::BlobStorePut;
use triblespace_core::trible::TribleSet;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_net::tracking;

/// Build a remote-style branch metadata blob and put it into `store`.
/// Returns the metadata blob's raw hash (what gossip would broadcast).
fn publish_remote_meta(
    store: &mut MemoryRepo,
    remote_branch_id: Id,
    name: &str,
    commit_content: &str,
) -> [u8; 32] {
    let name_handle: Inline<Handle<LongString>> =
        store.put(name.to_string()).unwrap();

    // Fabricate a "commit" blob — contents don't matter for this test,
    // we just need a valid SimpleArchive the tracking machinery can
    // resolve as the branch's head.
    let eid = genid();
    let content_handle: Inline<Handle<LongString>> =
        store.put(commit_content.to_string()).unwrap();
    let mut commit_set = TribleSet::new();
    commit_set += entity! { &eid @ triblespace_core::metadata::name: content_handle };
    let commit_blob: Blob<SimpleArchive> = commit_set.to_blob();
    let _commit_handle: Inline<Handle<SimpleArchive>> =
        store.put(commit_blob.clone()).unwrap();

    let meta_set = branch_unsigned(remote_branch_id, name_handle, Some(commit_blob), None);
    let meta_handle: Inline<Handle<SimpleArchive>> = store.put(meta_set).unwrap();
    meta_handle.raw
}

#[test]
fn stale_updated_at_rejected() {
    let mut store = MemoryRepo::default();
    let remote_branch_id = genid();
    let publisher = [0u8; 32];

    // Publish M1 at T1, then M2 at T2 > T1 (branch_unsigned stamps via
    // Epoch::now(); the sleep guarantees a visible TAI delta).
    let m1 = publish_remote_meta(&mut store, *remote_branch_id, "branch", "v1");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let m2 = publish_remote_meta(&mut store, *remote_branch_id, "branch", "v2");
    assert_ne!(m1, m2, "distinct timestamps should produce distinct blobs");

    // Create the tracking branch pointing at M1.
    let tracking_id = tracking::create_tracking_branch(
        &mut store,
        *remote_branch_id,
        &m1,
        "branch",
        &publisher,
    )
    .expect("create tracking");

    // Advance to M2 — strictly newer timestamp, must succeed.
    tracking::update_tracking_branch(
        &mut store,
        tracking_id,
        *remote_branch_id,
        &m2,
        "branch",
        &publisher,
    )
    .expect("strictly newer update must succeed");

    let head_after_advance = store.head(tracking_id).unwrap().unwrap().raw;

    // Now the regression-inducing step: try to re-apply M1 (older).
    // update_tracking_branch must return None and leave the branch head
    // bit-for-bit identical to where M2 placed it.
    let regress = tracking::update_tracking_branch(
        &mut store,
        tracking_id,
        *remote_branch_id,
        &m1,
        "branch",
        &publisher,
    );
    assert!(regress.is_none(), "stale update must be rejected");

    let head_after_stale_attempt = store.head(tracking_id).unwrap().unwrap().raw;
    assert_eq!(
        head_after_advance, head_after_stale_attempt,
        "tracking branch head must not regress under a stale gossip replay"
    );
}

#[test]
fn equal_updated_at_rejected() {
    // When the incoming metadata has the same timestamp as what we
    // already store, we treat it as a duplicate and skip the update —
    // saves the CAS round trip and keeps the log clean.
    let mut store = MemoryRepo::default();
    let remote_branch_id = genid();
    let publisher = [0u8; 32];

    let m1 = publish_remote_meta(&mut store, *remote_branch_id, "branch", "v1");

    let tracking_id = tracking::create_tracking_branch(
        &mut store,
        *remote_branch_id,
        &m1,
        "branch",
        &publisher,
    )
    .unwrap();

    // Re-apply the same metadata — same timestamp, so it's equal-not-newer.
    let rerun = tracking::update_tracking_branch(
        &mut store,
        tracking_id,
        *remote_branch_id,
        &m1,
        "branch",
        &publisher,
    );
    assert!(rerun.is_none(), "equal timestamp must be rejected as non-newer");
}
