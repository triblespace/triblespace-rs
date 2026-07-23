//! Tracking pin management.
//!
//! A tracking pin is a local reification of a remote branch's head.
//! It's a [`PinStore`] entry — a named, atomically-updatable handle —
//! that mirrors what a remote peer reported their branch head was at
//! the time of last sync. Per the Pin/Branch taxonomy (decide#6de2dd95):
//! locally, the tracking entry has no commit history of its own, so
//! it's a Pin, not a Branch. The thing it points at on the remote IS
//! a Branch.
//!
//! Tracking pins use `remote_name` instead of `metadata::name` in
//! their pin metadata, making them invisible to normal content-branch
//! operations (`ensure_branch`, `resolve_branch_name`, faculties).
//!
//! The tracking pin has its own local pin id. Repository can pull/merge
//! it like any other commit-history pin (a Branch).

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::prelude::attributes;
use triblespace_core::prelude::inlineencodings::{ED25519PublicKey, GenId};
use triblespace_core::repo::{
    BlobStore, BlobStoreGet, BlobStorePut, PinStore, PushResult, Repository,
};
use triblespace_core::trible::TribleSet;

use crate::channel::PublisherKey;
use crate::protocol::RawHash;

// Minted attribute IDs for tracking pins.
attributes! {
    "FD45B98C108B3F9F2D18C0B5373BC9FB" as pub remote_name: Handle<LongString>;
    "ACEBAE99F0B5B1E12DAE3FDC1E2BC575" as pub tracking_remote_pin: GenId;
    "C52A223988BB237B0859319661DA23F5" as pub tracking_peer: ED25519PublicKey;
    // Presence marks a *weak* (lazy/evictable) tracking pin: its history
    // is synced but content blobs are fetched on demand and may be
    // evicted under budget, rather than eagerly replicated via
    // `fetch_reachable`. Valued by the pin's own id (a pure marker).
    "CCD0C9D01CD09EFAC0BA04A804E6D7A0" as pub weak_tracking: GenId;
}

/// Returns true if the given pin is a tracking pin (has the
/// `tracking_remote_pin` attribute in its metadata).
///
/// Tracking pins are local-only state that must not be re-gossipped.
pub fn is_tracking_pin<S>(store: &mut S, branch_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(branch_id) else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else {
        return false;
    };
    find!(
        v: Id,
        pattern!(&meta, [{ _?e @ tracking_remote_pin: ?v }])
    )
    .next()
    .is_some()
}

/// Returns true if the given pin is a *weak* tracking pin — its history
/// is synced but content is fetched lazily and is evictable (the
/// `weak_tracking` marker is present in its metadata). A weak pin is
/// still a tracking pin; `is_tracking_pin` also returns true for it.
pub fn is_weak_tracking_pin<S>(store: &mut S, branch_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(branch_id) else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else {
        return false;
    };
    find!(
        v: Id,
        pattern!(&meta, [{ _?e @ weak_tracking: ?v }])
    )
    .next()
    .is_some()
}

/// Information about a tracking pin.
#[derive(Debug, Clone)]
pub struct TrackingPinInfo {
    /// The local pin id under which the tracking pin is registered.
    pub local_id: Id,
    /// The remote node's branch id that this tracking pin mirrors.
    /// (The remote side is a branch — has commit history — even
    /// though the local mirror is a pin.)
    pub remote_branch_id: Id,
    /// The branch name on the remote (stored as `remote_name` to keep
    /// it invisible to normal `metadata::name` lookups, which only
    /// surface content branches).
    pub remote_name: String,
}

/// Enumerate all tracking pins currently in `store`.
///
/// This is the canonical "what remote branches do I know about" query —
/// the persistent equivalent of an in-memory remote-head map. Use it from
/// auto-merge loops, status displays, etc.
pub fn list_tracking_pins<S>(store: &mut S) -> Vec<TrackingPinInfo>
where
    S: BlobStore + PinStore,
{
    let mut result = Vec::new();
    let Ok(iter) = store.pins() else {
        return result;
    };
    let bids: Vec<Id> = iter.filter_map(|r| r.ok()).collect();

    for bid in bids {
        let Ok(Some(meta_handle)) = store.head(bid) else {
            continue;
        };
        let Ok(reader) = store.reader() else {
            continue;
        };
        let Ok(meta): Result<TribleSet, _> = reader.get(meta_handle) else {
            continue;
        };

        let Some(remote_branch_id) = find!(
            v: Id,
            pattern!(&meta, [{ _?e @ tracking_remote_pin: ?v }])
        )
        .next() else {
            continue;
        };

        let Some(name_handle) = find!(
            h: Inline<Handle<LongString>>,
            pattern!(&meta, [{ _?e @ remote_name: ?h }])
        )
        .next() else {
            continue;
        };

        let Ok(name_view): Result<anybytes::View<str>, _> = reader.get(name_handle) else {
            continue;
        };

        result.push(TrackingPinInfo {
            local_id: bid,
            remote_branch_id,
            remote_name: name_view.as_ref().to_string(),
        });
    }
    result
}

/// Find the local tracking pin for the given remote branch id, if any.
/// Returns the pin id (the same `Id` used as the storage key in
/// `PinStore`).
pub fn find_tracking_pin<S>(store: &mut S, remote_branch_id: Id) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    list_tracking_pins(store)
        .into_iter()
        .find(|info| info.remote_branch_id == remote_branch_id)
        .map(|info| info.local_id)
}

/// Read the actual commit handle from a remote branch's metadata blob.
///
/// The network protocol gossips the branch metadata blob hash as "HEAD"
/// (because that's what's stored on the publisher's pin head), but
/// inside that metadata `repo::head` points to a commit. This resolves
/// the indirection so tracking pins store actual commit handles in
/// their local head — which lets `Repository::pull(tracking_pin)`
/// behave the same as a checkout of a real branch.
fn resolve_commit_in_branch_meta<S: BlobStore>(
    store: &mut S,
    branch_meta_hash: &RawHash,
) -> Option<Inline<Handle<SimpleArchive>>> {
    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*branch_meta_hash);
    let meta: TribleSet = reader.get(meta_handle).ok()?;
    find!(
        h: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ _?e @ triblespace_core::repo::head: ?h }])
    )
    .next()
}

/// Read the `metadata::updated_at` attribute from a branch metadata blob,
/// if present. Returns `None` if the blob is missing, can't be parsed, or
/// doesn't carry a timestamp.
fn read_updated_at<S: BlobStore>(
    store: &mut S,
    branch_meta_hash: &RawHash,
) -> Option<Inline<NsTAIInterval>> {
    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*branch_meta_hash);
    let meta: TribleSet = reader.get(meta_handle).ok()?;
    find!(
        ts: Inline<NsTAIInterval>,
        pattern!(&meta, [{ _?e @ triblespace_core::metadata::updated_at: ?ts }])
    )
    .next()
}

/// Create a new tracking pin. Returns the local pin id.
///
/// `remote_head_hash` is the (remote) branch metadata blob hash
/// gossiped over the network. The tracking pin resolves it to the
/// inner commit handle so `Repository::pull(pin_id).head()` returns
/// a real commit.
pub fn create_tracking_pin<S>(
    store: &mut S,
    remote_branch_id: Id,
    remote_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    // Resolve the gossiped branch metadata hash to the actual commit.
    let commit_handle = resolve_commit_in_branch_meta(store, remote_head_hash)?;
    // Mirror the remote's publication timestamp so future updates can
    // reject stale gossips without needing an ancestry walk.
    let remote_updated_at = read_updated_at(store, remote_head_hash);

    // tracking_id stays random (it's the pin's identity in the local
    // pile and must not collide across tracking setups). The metadata
    // entity id is intrinsic — derived from the actual tribles below.
    let tracking_id: Id = *genid();

    let name_string = remote_name_str.to_string();
    let name_handle: Inline<Handle<LongString>> =
        store.put::<LongString, String>(name_string).ok()?;

    let pub_key = ed25519_dalek::VerifyingKey::from_bytes(publisher).ok()?;

    let meta_set: TribleSet = entity! {
        triblespace_core::repo::branch: tracking_id,
        triblespace_core::repo::head: commit_handle,
        remote_name: name_handle,
        tracking_remote_pin: remote_branch_id,
        tracking_peer: pub_key,
        triblespace_core::metadata::updated_at?: remote_updated_at,
        weak_tracking?: weak.then_some(tracking_id),
    }
    .into();
    let meta_handle: Inline<Handle<SimpleArchive>> = store.put(meta_set).ok()?;

    match store.update(tracking_id, None, Some(meta_handle)).ok()? {
        PushResult::Success() => Some(tracking_id),
        PushResult::Conflict(_) => None,
    }
}

/// Update a tracking pin's head. `new_head_hash` is the gossiped
/// (remote) branch metadata blob hash, which is resolved to the inner
/// commit handle before storage.
pub fn update_tracking_pin<S>(
    store: &mut S,
    tracking_pin_id: Id,
    remote_branch_id: Id,
    new_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let old_meta = store.head(tracking_pin_id).ok()??;

    // No wall-clock gate here. Idempotency on no-op updates lives at
    // the storage layer (`Pile::update` short-circuits when
    // `new == current`), so a repeated identical gossip just resolves
    // to the same meta_handle and is dropped by Pile without writing.
    // Out-of-order semantically different heads are handled correctly
    // downstream by `merge_commit`'s ancestry check (no-op if remote
    // is already in local's ancestry; fast-forward if local is in
    // remote's ancestry; merge commit otherwise).
    let new_ts = read_updated_at(store, new_head_hash);

    let commit_handle = resolve_commit_in_branch_meta(store, new_head_hash)?;

    let name_string = remote_name_str.to_string();
    let name_handle: Inline<Handle<LongString>> =
        store.put::<LongString, String>(name_string).ok()?;

    let pub_key = ed25519_dalek::VerifyingKey::from_bytes(publisher).ok()?;

    // Metadata entity id is intrinsic — matches the pattern used in
    // triblespace-core's branch_metadata / commit_metadata.
    let meta_set: TribleSet = entity! {
        triblespace_core::repo::branch: tracking_pin_id,
        triblespace_core::repo::head: commit_handle,
        remote_name: name_handle,
        tracking_remote_pin: remote_branch_id,
        tracking_peer: pub_key,
        triblespace_core::metadata::updated_at?: new_ts,
        weak_tracking?: weak.then_some(tracking_pin_id),
    }
    .into();

    let meta_handle: Inline<Handle<SimpleArchive>> = store.put(meta_set).ok()?;

    match store
        .update(tracking_pin_id, Some(old_meta), Some(meta_handle))
        .ok()?
    {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Find or create a tracking pin for `(remote_branch_id, publisher)`.
/// Returns the local pin id.
pub fn ensure_tracking_pin<S>(
    store: &mut S,
    remote_branch_id: Id,
    remote_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    if let Some(tracking_id) = find_tracking_pin(store, remote_branch_id) {
        update_tracking_pin(
            store,
            tracking_id,
            remote_branch_id,
            remote_head_hash,
            remote_name_str,
            publisher,
            weak,
        );
        Some(tracking_id)
    } else {
        create_tracking_pin(
            store,
            remote_branch_id,
            remote_head_hash,
            remote_name_str,
            publisher,
            weak,
        )
    }
}

/// Outcome of [`merge_tracking_into_local`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Tracking pin had no head — nothing to merge.
    Empty,
    /// Local branch was already up-to-date with the tracking pin.
    UpToDate,
    /// Local branch advanced to `new_head` (fast-forward or merge commit).
    Merged {
        new_head: Inline<Handle<SimpleArchive>>,
    },
}

/// Merge a tracking pin into its same-named local branch.
///
/// Looks up (or creates) a local branch named `local_name`, then uses
/// [`Workspace::merge_commit`](triblespace_core::repo::Workspace::merge_commit)
/// to decide between no-op / fast-forward / merge commit. The tracking
/// pin itself is never modified — this is a one-way "pull from the
/// tracking pin into the local content branch".
///
/// Used by `pile net pull` (one-shot) and `pile net sync` (periodic
/// auto-merge loop). Factored out here so both share the same semantics
/// and a single test point.
pub fn merge_tracking_into_local<S>(
    repo: &mut Repository<S>,
    tracking_id: Id,
    local_name: &str,
) -> anyhow::Result<MergeOutcome>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let local_id = repo
        .ensure_branch(local_name, None)
        .map_err(|_| anyhow::anyhow!("ensure branch '{local_name}'"))?;
    let remote_ws = repo
        .pull(tracking_id)
        .map_err(|_| anyhow::anyhow!("pull tracking pin"))?;
    let Some(remote_commit) = remote_ws.head() else {
        return Ok(MergeOutcome::Empty);
    };

    let mut local_ws = repo
        .pull(local_id)
        .map_err(|_| anyhow::anyhow!("pull local branch"))?;
    let prev_head = local_ws.head();
    let new_head = local_ws
        .merge_commit(remote_commit)
        .map_err(|e| anyhow::anyhow!("merge: {e:?}"))?;
    if Some(new_head) == prev_head {
        return Ok(MergeOutcome::UpToDate);
    }
    repo.push(&mut local_ws)
        .map_err(|_| anyhow::anyhow!("push merged branch"))?;
    Ok(MergeOutcome::Merged { new_head })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::Blob;
    use triblespace_core::id::genid;
    use triblespace_core::repo::memoryrepo::MemoryRepo;

    fn test_repo() -> Repository<MemoryRepo> {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let store = MemoryRepo::default();
        Repository::new(store, signing_key, TribleSet::new()).unwrap()
    }

    #[test]
    fn merge_tracking_ff_into_empty_local() {
        // Tracking has a commit, local "main" doesn't exist yet. Merge
        // should create main and fast-forward it to the tracking head.
        let mut repo = test_repo();

        let source_id = repo.ensure_branch("source", None).unwrap();
        let mut src_ws = repo.pull(source_id).unwrap();
        src_ws.commit(TribleSet::new(), "remote commit");
        let source_head = src_ws.head().unwrap();
        repo.push(&mut src_ws).unwrap();

        let outcome = merge_tracking_into_local(&mut repo, source_id, "main").unwrap();
        assert_eq!(
            outcome,
            MergeOutcome::Merged {
                new_head: source_head
            }
        );

        let main_id = repo.lookup_branch("main").unwrap().expect("main exists");
        let main_ws = repo.pull(main_id).unwrap();
        assert_eq!(main_ws.head(), Some(source_head));
    }

    #[test]
    fn merge_tracking_up_to_date_is_noop() {
        // Local "main" already at the tracking head. Merge should be
        // a no-op.
        let mut repo = test_repo();

        let source_id = repo.ensure_branch("source", None).unwrap();
        let mut src_ws = repo.pull(source_id).unwrap();
        src_ws.commit(TribleSet::new(), "shared commit");
        let shared_head = src_ws.head().unwrap();
        repo.push(&mut src_ws).unwrap();

        // Seed main with the same head via a first merge.
        let _ = merge_tracking_into_local(&mut repo, source_id, "main").unwrap();

        // Second call should report UpToDate.
        let outcome = merge_tracking_into_local(&mut repo, source_id, "main").unwrap();
        assert_eq!(outcome, MergeOutcome::UpToDate);

        let main_id = repo.lookup_branch("main").unwrap().unwrap();
        let main_ws = repo.pull(main_id).unwrap();
        assert_eq!(main_ws.head(), Some(shared_head));
    }

    #[test]
    fn merge_tracking_divergent_produces_merge_commit() {
        // Local "main" at commit_a, tracking at unrelated commit_b.
        // Merge should produce a new merge commit with both as parents.
        let mut repo = test_repo();

        let main_id = repo.ensure_branch("main", None).unwrap();
        let mut main_ws = repo.pull(main_id).unwrap();
        main_ws.commit(TribleSet::new(), "local commit");
        let commit_a = main_ws.head().unwrap();
        repo.push(&mut main_ws).unwrap();

        let source_id = repo.ensure_branch("source", None).unwrap();
        let mut src_ws = repo.pull(source_id).unwrap();
        src_ws.commit(TribleSet::new(), "remote commit");
        let commit_b = src_ws.head().unwrap();
        repo.push(&mut src_ws).unwrap();

        let outcome = merge_tracking_into_local(&mut repo, source_id, "main").unwrap();
        let merge_head = match outcome {
            MergeOutcome::Merged { new_head } => new_head,
            other => panic!("expected Merged, got {other:?}"),
        };
        assert_ne!(merge_head, commit_a, "merge commit must advance past local");
        assert_ne!(
            merge_head, commit_b,
            "merge commit must not just fast-forward to remote"
        );

        // Local main should now be at the merge commit, and both
        // parents should appear in its ancestor set.
        let mut main_ws = repo.pull(main_id).unwrap();
        assert_eq!(main_ws.head(), Some(merge_head));

        use triblespace_core::repo::CommitSelector;
        let ancestor_set = triblespace_core::repo::ancestors(merge_head)
            .select(&mut main_ws)
            .expect("ancestors walk");
        assert!(
            ancestor_set.get(&commit_a.raw).is_some(),
            "commit_a in ancestry"
        );
        assert!(
            ancestor_set.get(&commit_b.raw).is_some(),
            "commit_b in ancestry"
        );
    }

    #[test]
    fn merge_tracking_empty_source_is_empty_outcome() {
        // Tracking branch exists but has no head (no commits yet).
        // Merge should report Empty and leave main untouched.
        let mut repo = test_repo();

        let source_id = repo.ensure_branch("source", None).unwrap();
        let outcome = merge_tracking_into_local(&mut repo, source_id, "main").unwrap();
        assert_eq!(outcome, MergeOutcome::Empty);

        // No main branch was created either — there was nothing to
        // fast-forward to.
        // (ensure_branch inside the helper *does* create it though —
        // that's fine, it's just empty.)
        let main_id = repo.lookup_branch("main").unwrap().expect("main created");
        let main_ws = repo.pull(main_id).unwrap();
        assert_eq!(main_ws.head(), None);
    }

    #[test]
    fn find_tracking_branch_roundtrips() {
        let mut store = MemoryRepo::default();

        // Build a fake remote branch metadata blob first so we have something
        // to point to. Use branch_unsigned to avoid signing-key plumbing.
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::blob::encodings::longstring::LongString;
        use triblespace_core::repo::branch::branch_unsigned;
        let name_blob = "remote-branch".to_string().to_blob();
        let name_handle: Inline<Handle<LongString>> = store.put(name_blob).unwrap();
        let remote_branch_id = genid();
        // Create a dummy commit blob and set it as the remote head.
        let commit_meta: TribleSet = TribleSet::new();
        let commit_blob: Blob<SimpleArchive> = commit_meta.to_blob();
        let commit_handle = store.put::<SimpleArchive, _>(commit_blob.clone()).unwrap();
        let remote_meta = branch_unsigned(*remote_branch_id, name_handle, Some(commit_blob), None);
        let remote_meta_handle = store.put::<SimpleArchive, _>(remote_meta).unwrap();

        let publisher = [0u8; 32];
        let remote_head_hash: RawHash = remote_meta_handle.raw;

        // Create the tracking pin.
        let tracking_id = create_tracking_pin(
            &mut store,
            *remote_branch_id,
            &remote_head_hash,
            "remote-branch",
            &publisher,
            false,
        )
        .expect("create");

        // Now find it.
        let found = find_tracking_pin(&mut store, *remote_branch_id);
        assert_eq!(
            found,
            Some(tracking_id),
            "should find the tracking pin we just created"
        );

        // is_tracking_pin should return true for the tracking pin.
        assert!(is_tracking_pin(&mut store, tracking_id));

        // ensure should be idempotent.
        let same = ensure_tracking_pin(
            &mut store,
            *remote_branch_id,
            &remote_head_hash,
            "remote-branch",
            &publisher,
            false,
        );
        assert_eq!(
            same,
            Some(tracking_id),
            "ensure should return the existing tracking pin"
        );

        // Verify the tracking pin resolved the inner commit, not the metadata blob.
        let mut store2 = store;
        let reader = store2.reader().unwrap();
        let track_meta_handle = store2.head(tracking_id).unwrap().unwrap();
        let track_meta: TribleSet = reader.get(track_meta_handle).unwrap();
        let track_head: Inline<Handle<SimpleArchive>> = find!(
            h: Inline<Handle<SimpleArchive>>,
            pattern!(&track_meta, [{ _?e @ triblespace_core::repo::head: ?h }])
        )
        .next()
        .expect("tracking pin should have a head");
        assert_eq!(
            track_head, commit_handle,
            "tracking pin head should be the inner commit, not the branch metadata blob"
        );
    }

    #[test]
    fn weak_marker_distinguishes_weak_from_strong_tracking() {
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::repo::branch::branch_unsigned;

        let mut store = MemoryRepo::default();

        // Build a remote branch metadata blob to point a tracking pin at.
        // Returns (remote_branch_id, remote_meta_hash).
        let mut make_remote = |label: &str| -> (Id, RawHash) {
            let name_handle: Inline<Handle<LongString>> =
                store.put(label.to_string().to_blob()).unwrap();
            let remote_branch_id = genid();
            let commit_blob: Blob<SimpleArchive> = TribleSet::new().to_blob();
            let _commit_handle = store.put::<SimpleArchive, _>(commit_blob.clone()).unwrap();
            let remote_meta =
                branch_unsigned(*remote_branch_id, name_handle, Some(commit_blob), None);
            let remote_meta_handle = store.put::<SimpleArchive, _>(remote_meta).unwrap();
            (*remote_branch_id, remote_meta_handle.raw)
        };

        let publisher = [0u8; 32];

        let (strong_remote, strong_head) = make_remote("strong-branch");
        let (weak_remote, weak_head) = make_remote("weak-branch");

        let strong_id = create_tracking_pin(
            &mut store,
            strong_remote,
            &strong_head,
            "strong-branch",
            &publisher,
            false,
        )
        .expect("create strong");
        let weak_id = create_tracking_pin(
            &mut store,
            weak_remote,
            &weak_head,
            "weak-branch",
            &publisher,
            true,
        )
        .expect("create weak");

        // Both are tracking pins...
        assert!(is_tracking_pin(&mut store, strong_id));
        assert!(is_tracking_pin(&mut store, weak_id));

        // ...but only the weak one carries the weak marker.
        assert!(
            !is_weak_tracking_pin(&mut store, strong_id),
            "strong pin must not be weak"
        );
        assert!(
            is_weak_tracking_pin(&mut store, weak_id),
            "weak pin must be weak"
        );

        // ensure_tracking_pin preserves weakness on update (idempotent
        // re-ensure with weak=true keeps the marker).
        let same = ensure_tracking_pin(
            &mut store,
            weak_remote,
            &weak_head,
            "weak-branch",
            &publisher,
            true,
        );
        assert_eq!(same, Some(weak_id));
        assert!(
            is_weak_tracking_pin(&mut store, weak_id),
            "weak marker survives re-ensure"
        );
    }
}
