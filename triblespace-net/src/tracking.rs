//! Tracking branch management.
//!
//! A tracking branch is a local reification of a remote branch. It uses
//! `remote_name` instead of `name` in its metadata, making it invisible
//! to normal operations (ensure_branch, resolve_branch_name, faculties).
//!
//! The tracking branch has its own local ID. Repository can pull/merge
//! it like any other branch.

use tracing::debug;

use triblespace_core::blob::schemas::longstring::LongString;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, BranchStore, PushResult, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::time::NsTAIInterval;
use triblespace_core::value::Value;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::prelude::valueschemas::{GenId, ED25519PublicKey};
use triblespace_core::prelude::attributes;
use triblespace_core::macros::{find, pattern, entity};

use crate::channel::PublisherKey;
use crate::protocol::RawHash;

// Minted attribute IDs for tracking branches.
attributes! {
    "FD45B98C108B3F9F2D18C0B5373BC9FB" as pub remote_name: Handle<LongString>;
    "ACEBAE99F0B5B1E12DAE3FDC1E2BC575" as pub tracking_remote_branch: GenId;
    "C52A223988BB237B0859319661DA23F5" as pub tracking_peer: ED25519PublicKey;
}

/// Returns true if the given branch is a tracking branch (has the
/// `tracking_remote_branch` attribute in its metadata).
///
/// Tracking branches are local-only state that should not be re-gossipped.
pub fn is_tracking_branch<S>(store: &mut S, branch_id: Id) -> bool
where
    S: BlobStore + BranchStore,
{
    let Ok(Some(head_handle)) = store.head(branch_id) else { return false; };
    let Ok(reader) = store.reader() else { return false; };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else { return false; };
    find!(
        v: Id,
        pattern!(&meta, [{ _?e @ tracking_remote_branch: ?v }])
    ).next().is_some()
}

/// Information about a tracking branch.
#[derive(Debug, Clone)]
pub struct TrackingBranchInfo {
    /// The local branch id under which the tracking branch is registered.
    pub local_id: Id,
    /// The remote node's branch id that this tracking branch mirrors.
    pub remote_branch_id: Id,
    /// The branch name on the remote (stored as `remote_name` to keep it
    /// invisible to normal `metadata::name` lookups).
    pub remote_name: String,
}

/// Enumerate all tracking branches currently in `store`.
///
/// This is the canonical "what remote branches do I know about" query —
/// the persistent equivalent of an in-memory remote-head map. Use it from
/// auto-merge loops, status displays, etc.
pub fn list_tracking_branches<S>(store: &mut S) -> Vec<TrackingBranchInfo>
where
    S: BlobStore + BranchStore,
{
    let mut result = Vec::new();
    let Ok(iter) = store.branches() else { return result; };
    let bids: Vec<Id> = iter.filter_map(|r| r.ok()).collect();

    for bid in bids {
        let Ok(Some(meta_handle)) = store.head(bid) else { continue; };
        let Ok(reader) = store.reader() else { continue; };
        let Ok(meta): Result<TribleSet, _> = reader.get(meta_handle) else { continue; };

        let Some(remote_branch_id) = find!(
            v: Id,
            pattern!(&meta, [{ _?e @ tracking_remote_branch: ?v }])
        ).next() else { continue; };

        let Some(name_handle) = find!(
            h: Value<Handle<LongString>>,
            pattern!(&meta, [{ _?e @ remote_name: ?h }])
        ).next() else { continue; };

        let Ok(name_view): Result<anybytes::View<str>, _> = reader.get(name_handle) else { continue; };

        result.push(TrackingBranchInfo {
            local_id: bid,
            remote_branch_id,
            remote_name: name_view.as_ref().to_string(),
        });
    }
    result
}

/// Find a tracking branch for the given remote branch ID.
/// Returns the local tracking branch ID if found.
pub fn find_tracking_branch<S>(
    store: &mut S,
    remote_branch_id: Id,
) -> Option<Id>
where
    S: BlobStore + BranchStore,
{
    list_tracking_branches(store)
        .into_iter()
        .find(|info| info.remote_branch_id == remote_branch_id)
        .map(|info| info.local_id)
}

/// Read the actual commit handle from a branch metadata blob.
///
/// The network protocol gossips the branch metadata blob hash as "HEAD",
/// but `repo::head` in branch metadata points to a commit. This resolves
/// the indirection so tracking branches store actual commit handles.
fn resolve_commit_in_branch_meta<S: BlobStore>(
    store: &mut S,
    branch_meta_hash: &RawHash,
) -> Option<Value<Handle<SimpleArchive>>> {
    let reader = store.reader().ok()?;
    let meta_handle = Value::<Handle<SimpleArchive>>::new(*branch_meta_hash);
    let meta: TribleSet = reader.get(meta_handle).ok()?;
    find!(
        h: Value<Handle<SimpleArchive>>,
        pattern!(&meta, [{ _?e @ triblespace_core::repo::head: ?h }])
    ).next()
}

/// Read the `metadata::updated_at` attribute from a branch metadata blob,
/// if present. Returns `None` if the blob is missing, can't be parsed, or
/// doesn't carry a timestamp.
fn read_updated_at<S: BlobStore>(
    store: &mut S,
    branch_meta_hash: &RawHash,
) -> Option<Value<NsTAIInterval>> {
    let reader = store.reader().ok()?;
    let meta_handle = Value::<Handle<SimpleArchive>>::new(*branch_meta_hash);
    let meta: TribleSet = reader.get(meta_handle).ok()?;
    find!(
        ts: Value<NsTAIInterval>,
        pattern!(&meta, [{ _?e @ triblespace_core::metadata::updated_at: ?ts }])
    ).next()
}

/// Compare two `NsTAIInterval` values by their lower bound (both bounds
/// are identical for point-in-time timestamps). Returns true iff `new` is
/// strictly newer than `current`.
fn is_newer(new: Value<NsTAIInterval>, current: Value<NsTAIInterval>) -> bool {
    let Ok((new_ns, _)): Result<(i128, i128), _> = new.try_from_value() else {
        return false;
    };
    let Ok((current_ns, _)): Result<(i128, i128), _> = current.try_from_value() else {
        return false;
    };
    new_ns > current_ns
}

/// Create a new tracking branch. Returns the local tracking branch ID.
///
/// `remote_head_hash` is the branch metadata blob hash gossiped over the
/// network. The tracking branch resolves it to the inner commit handle so
/// `Repository::pull(tracking_id).head()` returns a real commit.
pub fn create_tracking_branch<S>(
    store: &mut S,
    remote_branch_id: Id,
    remote_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    // Resolve the gossiped branch metadata hash to the actual commit.
    let commit_handle = resolve_commit_in_branch_meta(store, remote_head_hash)?;
    // Mirror the remote's publication timestamp so future updates can
    // reject stale gossips without needing an ancestry walk.
    let remote_updated_at = read_updated_at(store, remote_head_hash);

    // tracking_id stays random (it's the branch's identity in the local
    // pile and must not collide across tracking setups). The metadata
    // entity id is intrinsic — derived from the actual tribles below.
    let tracking_id: Id = *genid();

    let name_string = remote_name_str.to_string();
    let name_handle: Value<Handle<LongString>> =
        store.put::<LongString, String>(name_string).ok()?;

    let pub_key = ed25519_dalek::VerifyingKey::from_bytes(publisher).ok()?;

    let meta_set: TribleSet = entity! {
        triblespace_core::repo::branch: tracking_id,
        triblespace_core::repo::head: commit_handle,
        remote_name: name_handle,
        tracking_remote_branch: remote_branch_id,
        tracking_peer: pub_key,
        triblespace_core::metadata::updated_at?: remote_updated_at,
    }
    .into();
    let meta_handle: Value<Handle<SimpleArchive>> = store.put(meta_set).ok()?;

    match store.update(tracking_id, None, Some(meta_handle)).ok()? {
        PushResult::Success() => Some(tracking_id),
        PushResult::Conflict(_) => None,
    }
}

/// Update a tracking branch's head. `new_head_hash` is the gossiped branch
/// metadata blob hash, which is resolved to the inner commit handle.
pub fn update_tracking_branch<S>(
    store: &mut S,
    tracking_branch_id: Id,
    remote_branch_id: Id,
    new_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    let old_meta = store.head(tracking_branch_id).ok()??;

    // Reject stale updates: if we can read a timestamp from both the
    // currently-stored tracking metadata and the incoming remote metadata,
    // require the incoming one to be strictly newer. This prevents a
    // late-finishing fetch for an older HEAD from overwriting a
    // newer HEAD that already advanced the tracking branch.
    let new_ts = read_updated_at(store, new_head_hash);
    let current_ts = read_updated_at(store, &old_meta.raw);
    if let (Some(current), Some(new)) = (current_ts, new_ts) {
        if !is_newer(new, current) {
            debug!(
                branch = %hex::encode(&remote_branch_id.raw()[..4]),
                "tracking: skip stale update (incoming ts ≤ current)"
            );
            return None;
        }
    }

    let commit_handle = resolve_commit_in_branch_meta(store, new_head_hash)?;

    let name_string = remote_name_str.to_string();
    let name_handle: Value<Handle<LongString>> =
        store.put::<LongString, String>(name_string).ok()?;

    let pub_key = ed25519_dalek::VerifyingKey::from_bytes(publisher).ok()?;

    // Metadata entity id is intrinsic — matches the pattern used in
    // triblespace-core's branch_metadata / commit_metadata.
    let meta_set: TribleSet = entity! {
        triblespace_core::repo::branch: tracking_branch_id,
        triblespace_core::repo::head: commit_handle,
        remote_name: name_handle,
        tracking_remote_branch: remote_branch_id,
        tracking_peer: pub_key,
        triblespace_core::metadata::updated_at?: new_ts,
    }
    .into();

    let meta_handle: Value<Handle<SimpleArchive>> = store.put(meta_set).ok()?;

    match store.update(tracking_branch_id, Some(old_meta), Some(meta_handle)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Find or create a tracking branch. Returns the local tracking branch ID.
pub fn ensure_tracking_branch<S>(
    store: &mut S,
    remote_branch_id: Id,
    remote_head_hash: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + BranchStore,
{
    if let Some(tracking_id) = find_tracking_branch(store, remote_branch_id) {
        update_tracking_branch(store, tracking_id, remote_branch_id, remote_head_hash, remote_name_str, publisher);
        Some(tracking_id)
    } else {
        create_tracking_branch(store, remote_branch_id, remote_head_hash, remote_name_str, publisher)
    }
}

/// Outcome of [`merge_tracking_into_local`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Tracking branch had no head — nothing to merge.
    Empty,
    /// Local branch was already up-to-date with the tracking branch.
    UpToDate,
    /// Local branch advanced to `new_head` (fast-forward or merge commit).
    Merged { new_head: Value<Handle<SimpleArchive>> },
}

/// Merge a tracking branch into its same-named local branch.
///
/// Looks up (or creates) a local branch named `local_name`, then uses
/// [`Workspace::merge_commit`](triblespace_core::repo::Workspace::merge_commit)
/// to decide between no-op / fast-forward / merge commit. The tracking
/// branch itself is never modified — this is a one-way "pull from
/// tracking into local".
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
    S: BlobStore + BlobStorePut + BranchStore,
{
    let local_id = repo
        .ensure_branch(local_name, None)
        .map_err(|_| anyhow::anyhow!("ensure branch '{local_name}'"))?;
    let remote_ws = repo
        .pull(tracking_id)
        .map_err(|_| anyhow::anyhow!("pull tracking branch"))?;
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
        assert_eq!(outcome, MergeOutcome::Merged { new_head: source_head });

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
        assert_ne!(merge_head, commit_b, "merge commit must not just fast-forward to remote");

        // Local main should now be at the merge commit, and both
        // parents should appear in its ancestor set.
        let mut main_ws = repo.pull(main_id).unwrap();
        assert_eq!(main_ws.head(), Some(merge_head));

        use triblespace_core::repo::CommitSelector;
        let ancestor_set = triblespace_core::repo::ancestors(merge_head)
            .select(&mut main_ws)
            .expect("ancestors walk");
        assert!(ancestor_set.get(&commit_a.raw).is_some(), "commit_a in ancestry");
        assert!(ancestor_set.get(&commit_b.raw).is_some(), "commit_b in ancestry");
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
        use triblespace_core::repo::branch::branch_unsigned;
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::blob::schemas::longstring::LongString;
        let name_blob = "remote-branch".to_string().to_blob();
        let name_handle: Value<Handle<LongString>> = store.put(name_blob).unwrap();
        let remote_branch_id = genid();
        // Create a dummy commit blob and set it as the remote head.
        let commit_meta: TribleSet = TribleSet::new();
        let commit_blob: Blob<SimpleArchive> = commit_meta.to_blob();
        let commit_handle = store.put::<SimpleArchive, _>(commit_blob.clone()).unwrap();
        let remote_meta = branch_unsigned(*remote_branch_id, name_handle, Some(commit_blob), None);
        let remote_meta_handle = store.put::<SimpleArchive, _>(remote_meta).unwrap();

        let publisher = [0u8; 32];
        let remote_head_hash: RawHash = remote_meta_handle.raw;

        // Create the tracking branch.
        let tracking_id = create_tracking_branch(
            &mut store, *remote_branch_id, &remote_head_hash, "remote-branch", &publisher,
        ).expect("create");

        // Now find it.
        let found = find_tracking_branch(&mut store, *remote_branch_id);
        assert_eq!(found, Some(tracking_id), "should find the tracking branch we just created");

        // is_tracking_branch should return true for the tracking branch.
        assert!(is_tracking_branch(&mut store, tracking_id));

        // ensure should be idempotent.
        let same = ensure_tracking_branch(
            &mut store, *remote_branch_id, &remote_head_hash, "remote-branch", &publisher,
        );
        assert_eq!(same, Some(tracking_id), "ensure should return the existing tracking branch");

        // Verify the tracking branch resolved the inner commit, not the metadata blob.
        let mut store2 = store;
        let reader = store2.reader().unwrap();
        let track_meta_handle = store2.head(tracking_id).unwrap().unwrap();
        let track_meta: TribleSet = reader.get(track_meta_handle).unwrap();
        let track_head: Value<Handle<SimpleArchive>> = find!(
            h: Value<Handle<SimpleArchive>>,
            pattern!(&track_meta, [{ _?e @ triblespace_core::repo::head: ?h }])
        ).next().expect("tracking branch should have a head");
        assert_eq!(track_head, commit_handle,
            "tracking branch head should be the inner commit, not the branch metadata blob");
    }
}
