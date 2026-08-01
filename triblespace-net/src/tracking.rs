//! Tracking pin management.
//!
//! A tracking pin is a local reification of a legacy remote mutable-pin
//! observation. It's a [`PinStore`] entry — a named, atomically-updatable
//! handle — that mirrors the metadata blob a remote publisher reported for
//! one of its pin ids. The `(publisher hint, remote pin id)` tuple is transport
//! namespace, not an authenticated [`StrongPin`](triblespace_core::repo::StrongPin)
//! identity and not branch authority.
//!
//! Tracking pins use `remote_name` instead of `metadata::name` in
//! their pin metadata, keeping this mutable transport state separate from
//! exact, signed content-branch assertions.
//!
//! The tracking pin has its own local pin id. [`merge_tracking_into_local`]
//! reads its mirrored commit directly, then publishes that commit through the
//! repository's exact local `(author, name handle)` identity.

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::prelude::attributes;
use triblespace_core::prelude::inlineencodings::{ED25519PublicKey, GenId};
use triblespace_core::repo::branch_assertion::BranchAssertionStore;
use triblespace_core::repo::branch_frontier::PartialCommitDag;
use triblespace_core::repo::{
    AssertionPullError, BlobStore, BlobStoreGet, BlobStorePut, PinStore, PushResult, Repository,
    StorageFlush,
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
    // the legacy hint walker. Valued by the pin's own id (a pure marker).
    "CCD0C9D01CD09EFAC0BA04A804E6D7A0" as pub weak_tracking: GenId;
}

/// Returns true if the given pin is a tracking pin (has the
/// `tracking_remote_pin` attribute in its metadata).
///
/// Tracking pins are local-only state that must not be re-gossipped.
pub fn is_tracking_pin<S>(store: &mut S, pin_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(pin_id) else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else {
        return false;
    };
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
        return false;
    };
    find!(
        v: Id,
        pattern!(&meta, [{ branch_entity @ tracking_remote_pin: ?v }])
    )
    .next()
    .is_some()
}

/// Returns true if the given pin is a *weak* tracking pin — its history
/// is synced but content is fetched lazily and is evictable (the
/// `weak_tracking` marker is present in its metadata). A weak pin is
/// still a tracking pin; `is_tracking_pin` also returns true for it.
pub fn is_weak_tracking_pin<S>(store: &mut S, pin_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(pin_id) else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else {
        return false;
    };
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
        return false;
    };
    find!(
        v: Id,
        pattern!(&meta, [{ branch_entity @ weak_tracking: ?v }])
    )
    .next()
    .is_some()
}

/// Information about a tracking pin.
#[derive(Debug, Clone)]
pub struct TrackingPinInfo {
    /// The local pin id under which the tracking pin is registered.
    pub local_id: Id,
    /// Legacy remote pin id carried by the observation. It is scoped by
    /// [`Self::publisher`] and is not an exact StrongPin identity.
    pub remote_pin_id: Id,
    /// Publisher key carried by the legacy frame. It scopes the remote-id
    /// namespace but is a routing/observation hint, not authenticated authorship.
    pub publisher: PublisherKey,
    /// The presentation name on the remote (stored as `remote_name` to keep
    /// it invisible to normal `metadata::name` lookups, which only
    /// surface content branches).
    pub remote_name: String,
}

/// Enumerate all tracking pins currently in `store`.
///
/// This is the canonical "what legacy remote observations do I retain" query,
/// the persistent equivalent of an in-memory remote-head map. Use it for
/// diagnostics or an explicit admission/authorship workflow.
pub fn list_tracking_pins<S>(store: &mut S) -> Vec<TrackingPinInfo>
where
    S: BlobStore + PinStore,
{
    let mut result = Vec::new();
    let Ok(iter) = store.pins() else {
        return result;
    };
    let pin_ids: Vec<Id> = iter.filter_map(|r| r.ok()).collect();

    for pin_id in pin_ids {
        let Ok(Some(meta_handle)) = store.head(pin_id) else {
            continue;
        };
        let Ok(reader) = store.reader() else {
            continue;
        };
        let Ok(meta): Result<TribleSet, _> = reader.get(meta_handle) else {
            continue;
        };

        let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
            continue;
        };

        let Some((remote_pin_id, publisher)) = tracking_identity(&meta, branch_entity) else {
            continue;
        };

        let mut name_handles = find!(
            h: Inline<Handle<LongString>>,
            pattern!(&meta, [{ branch_entity @ remote_name: ?h }])
        );
        let (Some(name_handle), None) = (name_handles.next(), name_handles.next()) else {
            continue;
        };

        let Ok(name_view): Result<anybytes::View<str>, _> = reader.get(name_handle) else {
            continue;
        };

        result.push(TrackingPinInfo {
            local_id: pin_id,
            remote_pin_id,
            publisher,
            remote_name: name_view.as_ref().to_string(),
        });
    }
    result
}

/// Read the immutable observation key carried by tracking metadata.
///
/// Both fields must be unique on the tracking pin's scoped metadata entity.
/// The publisher bytes must also decode as an Ed25519 public key; malformed
/// metadata is never allowed to alias a valid tracking identity.
fn tracking_identity(meta: &TribleSet, branch_entity: Id) -> Option<(Id, PublisherKey)> {
    let mut remote_ids = find!(
        v: Id,
        pattern!(meta, [{ branch_entity @ tracking_remote_pin: ?v }])
    );
    let (Some(remote_pin_id), None) = (remote_ids.next(), remote_ids.next()) else {
        return None;
    };

    let mut publishers = find!(
        publisher: Inline<ED25519PublicKey>,
        pattern!(meta, [{ branch_entity @ tracking_peer: ?publisher }])
    );
    let (Some(publisher), None) = (publishers.next(), publishers.next()) else {
        return None;
    };
    ed25519_dalek::VerifyingKey::from_bytes(&publisher.raw).ok()?;

    Some((remote_pin_id, publisher.raw))
}

/// Find the local tracking pin for the exact `(remote pin id, publisher)`
/// identity, if any.
/// Returns the pin id (the same `Id` used as the storage key in
/// `PinStore`).
pub fn find_tracking_pin<S>(
    store: &mut S,
    remote_pin_id: Id,
    publisher: &PublisherKey,
) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    let pin_ids: Vec<Id> = store.pins().ok()?.filter_map(Result::ok).collect();
    for pin_id in pin_ids {
        let Ok(Some(meta_handle)) = store.head(pin_id) else {
            continue;
        };
        let reader = store.reader().ok()?;
        let Ok(meta): Result<TribleSet, _> = reader.get(meta_handle) else {
            continue;
        };
        let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
            continue;
        };
        if tracking_identity(&meta, branch_entity) == Some((remote_pin_id, *publisher)) {
            return Some(pin_id);
        }
    }
    None
}

/// Read the actual commit handle from a legacy pin-metadata blob.
///
/// The legacy network protocol gossips the pin-metadata blob hash as
/// "HEAD" (because that is what the publisher's pin contains), while the
/// metadata's `repo::head` points to the actual commit. This helper resolves
/// that indirection before a local tracking-pin metadata blob is written.
fn resolve_commit_in_legacy_pin_metadata<S: BlobStore>(
    store: &mut S,
    metadata_head: &RawHash,
    remote_pin_id: Id,
) -> Option<Inline<Handle<SimpleArchive>>> {
    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*metadata_head);
    let meta: TribleSet = reader.get(meta_handle).ok()?;
    let branch_entity = triblespace_core::repo::branch::branch_entity(&meta, remote_pin_id).ok()?;
    let mut heads = find!(
        h: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ branch_entity @ triblespace_core::repo::head: ?h }])
    );
    let head = heads.next()?;
    heads.next().is_none().then_some(head)
}

/// Create a new tracking pin. Returns the local pin id.
///
/// `remote_metadata_head` is the legacy pin-metadata blob hash gossiped over
/// the network. The tracking pin resolves it to the inner commit handle
/// consumed by [`merge_tracking_into_local`].
pub fn create_tracking_pin<S>(
    store: &mut S,
    remote_pin_id: Id,
    remote_metadata_head: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    // Resolve the gossiped legacy pin metadata to the actual commit.
    let commit_handle =
        resolve_commit_in_legacy_pin_metadata(store, remote_metadata_head, remote_pin_id)?;
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
        tracking_remote_pin: remote_pin_id,
        tracking_peer: pub_key,
        weak_tracking?: weak.then_some(tracking_id),
    }
    .into();
    let meta_handle: Inline<Handle<SimpleArchive>> = store.put(meta_set).ok()?;

    match store.update(tracking_id, None, Some(meta_handle)).ok()? {
        PushResult::Success() => Some(tracking_id),
        PushResult::Conflict(_) => None,
    }
}

/// Update a tracking pin's head. `new_remote_metadata_head` is the gossiped
/// legacy pin-metadata blob hash, which is resolved to the inner
/// commit handle before storage.
pub fn update_tracking_pin<S>(
    store: &mut S,
    tracking_pin_id: Id,
    remote_pin_id: Id,
    new_remote_metadata_head: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let old_meta = store.head(tracking_pin_id).ok()??;

    // A tracking pin's remote identity is immutable. In particular, a caller
    // must never be able to reuse a local pin selected for one publisher as
    // the mirror of the same 16-byte remote id from another publisher.
    let reader = store.reader().ok()?;
    let old_meta_set: TribleSet = reader.get(old_meta).ok()?;
    let branch_entity =
        triblespace_core::repo::branch::branch_entity(&old_meta_set, tracking_pin_id).ok()?;
    if tracking_identity(&old_meta_set, branch_entity) != Some((remote_pin_id, *publisher)) {
        return None;
    }

    // No wall-clock gate here. Idempotency on no-op updates lives at
    // the storage layer (`Pile::update` short-circuits when
    // `new == current`), so a repeated identical gossip just resolves
    // to the same meta_handle and is dropped by Pile without writing.
    // Out-of-order semantically different heads are handled correctly
    // downstream by `merge_commit`'s ancestry check (no-op if remote
    // is already in local's ancestry; fast-forward if local is in
    // remote's ancestry; merge commit otherwise).
    let commit_handle =
        resolve_commit_in_legacy_pin_metadata(store, new_remote_metadata_head, remote_pin_id)?;

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
        tracking_remote_pin: remote_pin_id,
        tracking_peer: pub_key,
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

/// Find or create a tracking pin for `(remote_pin_id, publisher)`.
/// Returns the local pin id.
pub fn ensure_tracking_pin<S>(
    store: &mut S,
    remote_pin_id: Id,
    remote_metadata_head: &RawHash,
    remote_name_str: &str,
    publisher: &PublisherKey,
    weak: bool,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    if let Some(tracking_id) = find_tracking_pin(store, remote_pin_id, publisher) {
        update_tracking_pin(
            store,
            tracking_id,
            remote_pin_id,
            remote_metadata_head,
            remote_name_str,
            publisher,
            weak,
        )?;
        Some(tracking_id)
    } else {
        create_tracking_pin(
            store,
            remote_pin_id,
            remote_metadata_head,
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
/// Reads the mirrored commit from the local tracking pin, opens the exact
/// `(repository author, local_name)` branch identity, then uses
/// [`Workspace::merge_commit`](triblespace_core::repo::Workspace::merge_commit)
/// to decide between no-op / fast-forward / merge commit. The tracking
/// pin itself is never modified — this is a one-way "pull from the
/// tracking pin into the local signed branch".
///
/// This is deliberately not called by the default sync loop. A caller must
/// first make its own admission decision, then invoke this helper as an
/// explicit local-authorship act; the legacy publisher field is not a verified
/// StrongPin signature.
pub fn merge_tracking_into_local<S>(
    repo: &mut Repository<S>,
    tracking_id: Id,
    local_name: &str,
) -> anyhow::Result<MergeOutcome>
where
    S: BlobStore + PinStore + StorageFlush + BranchAssertionStore,
    S::Reader: PartialCommitDag,
{
    let Some(remote_commit) = tracking_commit(repo.storage_mut(), tracking_id)? else {
        return Ok(MergeOutcome::Empty);
    };

    let local_identity = repo.branch_identity(local_name);
    let mut local_ws = match repo.pull(local_identity) {
        Ok(workspace) => workspace,
        Err(AssertionPullError::Absent) => repo
            .create_workspace(local_name)
            .map_err(|_| anyhow::anyhow!("create local branch '{local_name}'"))?,
        Err(_) => return Err(anyhow::anyhow!("pull local branch '{local_name}'")),
    };
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

/// Resolve the commit mirrored by a legacy local tracking pin.
///
/// Tracking pins remain mutable local transport state; they are deliberately
/// not branch assertions and therefore cannot be passed to
/// [`Repository::pull`]. Only the destination branch crosses the repository's
/// exact-identity authoring boundary.
fn tracking_commit<S>(
    store: &mut S,
    tracking_id: Id,
) -> anyhow::Result<Option<Inline<Handle<SimpleArchive>>>>
where
    S: BlobStore + PinStore,
{
    let Some(meta_handle) = store
        .head(tracking_id)
        .map_err(|_| anyhow::anyhow!("read tracking pin"))?
    else {
        return Ok(None);
    };
    let reader = store
        .reader()
        .map_err(|_| anyhow::anyhow!("open tracking pin blob snapshot"))?;
    let meta: TribleSet = reader
        .get(meta_handle)
        .map_err(|_| anyhow::anyhow!("read tracking pin metadata"))?;
    let branch_entity = triblespace_core::repo::branch::branch_entity(&meta, tracking_id)
        .map_err(|_| anyhow::anyhow!("malformed tracking pin metadata"))?;

    if tracking_identity(&meta, branch_entity).is_none() {
        return Err(anyhow::anyhow!(
            "pin {tracking_id:X} is not a well-formed tracking pin"
        ));
    }

    let mut heads = find!(
        head: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ branch_entity @ triblespace_core::repo::head: ?head }])
    );
    match (heads.next(), heads.next()) {
        (None, None) => Ok(None),
        (Some(head), None) => Ok(Some(head)),
        _ => Err(anyhow::anyhow!(
            "tracking pin {tracking_id:X} has multiple heads"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::IntoBlob;
    use triblespace_core::id::genid;
    use triblespace_core::repo::branch_frontier::BranchResolution;
    use triblespace_core::repo::memoryrepo::MemoryRepo;

    fn test_repo() -> Repository<MemoryRepo> {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let store = MemoryRepo::default();
        Repository::new(store, signing_key, TribleSet::new()).unwrap()
    }

    fn publish_commit(
        repo: &mut Repository<MemoryRepo>,
        name: &str,
        message: &str,
    ) -> Inline<Handle<SimpleArchive>> {
        let mut workspace = repo.create_workspace(name).unwrap();
        workspace.commit(TribleSet::new(), message);
        let commit = workspace.head().unwrap();
        repo.push(&mut workspace).unwrap();
        commit
    }

    fn test_tracking_pin(
        repo: &mut Repository<MemoryRepo>,
        commit: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Id {
        let tracking_id = *genid();
        let remote_pin_id = *genid();
        let name_handle: Inline<Handle<LongString>> = repo
            .storage_mut()
            .put("remote-main".to_string().to_blob())
            .unwrap();
        let publisher = SigningKey::from_bytes(&[9u8; 32]).verifying_key();
        let meta: TribleSet = entity! {
            triblespace_core::repo::branch: tracking_id,
            triblespace_core::repo::head?: commit,
            remote_name: name_handle,
            tracking_remote_pin: remote_pin_id,
            tracking_peer: publisher,
        }
        .into();
        let meta_handle = repo.storage_mut().put(meta).unwrap();
        assert!(matches!(
            repo.storage_mut()
                .update(tracking_id, None, Some(meta_handle))
                .unwrap(),
            PushResult::Success()
        ));
        tracking_id
    }

    #[test]
    fn legacy_metadata_head_ignores_carried_annotation_entities() {
        let mut store = MemoryRepo::default();
        let remote_pin_id = *genid();
        let actual_commit: Inline<Handle<SimpleArchive>> = store
            .put::<SimpleArchive, _>(TribleSet::new().to_blob())
            .unwrap();
        let mut decoy_set = TribleSet::new();
        decoy_set += entity! { triblespace_core::metadata::tag: remote_pin_id };
        let decoy_commit: Inline<Handle<SimpleArchive>> =
            store.put::<SimpleArchive, _>(decoy_set.to_blob()).unwrap();
        let mut meta: TribleSet = entity! {
            triblespace_core::repo::branch: remote_pin_id,
            triblespace_core::repo::head: actual_commit,
        }
        .into();
        let annotation = genid();
        meta += entity! { &annotation @
            triblespace_core::repo::head: decoy_commit,
        };
        let meta_handle: Inline<Handle<SimpleArchive>> = store.put(meta).unwrap();

        assert_eq!(
            resolve_commit_in_legacy_pin_metadata(&mut store, &meta_handle.raw, remote_pin_id),
            Some(actual_commit)
        );
    }

    #[test]
    fn merge_tracking_ff_into_empty_local() {
        // Tracking has a commit, local "main" doesn't exist yet. Merge
        // should publish main directly at the tracking head.
        let mut repo = test_repo();

        let source_head = publish_commit(&mut repo, "source", "remote commit");
        let tracking_id = test_tracking_pin(&mut repo, Some(source_head));

        let outcome = merge_tracking_into_local(&mut repo, tracking_id, "main").unwrap();
        assert_eq!(
            outcome,
            MergeOutcome::Merged {
                new_head: source_head
            }
        );

        let main_ws = repo.pull(repo.branch_identity("main")).unwrap();
        assert_eq!(main_ws.head(), Some(source_head));
    }

    #[test]
    fn merge_tracking_up_to_date_is_noop() {
        // Local "main" already at the tracking head. Merge should be
        // a no-op.
        let mut repo = test_repo();

        let shared_head = publish_commit(&mut repo, "source", "shared commit");
        let tracking_id = test_tracking_pin(&mut repo, Some(shared_head));

        // Seed main with the same head via a first merge.
        let _ = merge_tracking_into_local(&mut repo, tracking_id, "main").unwrap();

        // Second call should report UpToDate.
        let outcome = merge_tracking_into_local(&mut repo, tracking_id, "main").unwrap();
        assert_eq!(outcome, MergeOutcome::UpToDate);

        let main_ws = repo.pull(repo.branch_identity("main")).unwrap();
        assert_eq!(main_ws.head(), Some(shared_head));
    }

    #[test]
    fn merge_tracking_divergent_produces_merge_commit() {
        // Local "main" at commit_a, tracking at unrelated commit_b.
        // Merge should produce a new merge commit with both as parents.
        let mut repo = test_repo();

        let mut main_ws = repo.create_workspace("main").unwrap();
        main_ws.commit(TribleSet::new(), "local commit");
        let commit_a = main_ws.head().unwrap();
        repo.push(&mut main_ws).unwrap();

        let commit_b = publish_commit(&mut repo, "source", "remote commit");
        let tracking_id = test_tracking_pin(&mut repo, Some(commit_b));

        let outcome = merge_tracking_into_local(&mut repo, tracking_id, "main").unwrap();
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
        let mut main_ws = repo.pull(repo.branch_identity("main")).unwrap();
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
        // Tracking pin exists but has no mirrored commit.
        // Merge should report Empty and leave main untouched.
        let mut repo = test_repo();

        let tracking_id = test_tracking_pin(&mut repo, None);
        let outcome = merge_tracking_into_local(&mut repo, tracking_id, "main").unwrap();
        assert_eq!(outcome, MergeOutcome::Empty);

        // Empty branches are unrepresentable in the assertion model, so the
        // helper must not publish or otherwise manufacture one.
        assert!(matches!(
            repo.resolve_name("main").unwrap(),
            BranchResolution::Absent
        ));
    }

    #[test]
    fn merge_rejects_non_tracking_pin() {
        let mut repo = test_repo();
        let commit = publish_commit(&mut repo, "source", "source commit");
        let ordinary_pin = *genid();
        let meta: TribleSet = entity! {
            triblespace_core::repo::branch: ordinary_pin,
            triblespace_core::repo::head: commit,
        }
        .into();
        let meta_handle = repo.storage_mut().put(meta).unwrap();
        repo.storage_mut()
            .update(ordinary_pin, None, Some(meta_handle))
            .unwrap();

        let error = merge_tracking_into_local(&mut repo, ordinary_pin, "main").unwrap_err();
        assert!(error.to_string().contains("not a well-formed tracking pin"));
        assert!(matches!(
            repo.resolve_name("main").unwrap(),
            BranchResolution::Absent
        ));
    }

    #[test]
    fn find_tracking_pin_roundtrips() {
        let mut store = MemoryRepo::default();

        // Build a fake legacy pin-metadata blob first so we have something
        // to point to. Use branch_unsigned to avoid signing-key plumbing.
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::blob::encodings::longstring::LongString;
        use triblespace_core::repo::branch::branch_unsigned;
        let name_blob = "remote-branch".to_string().to_blob();
        let name_handle: Inline<Handle<LongString>> = store.put(name_blob).unwrap();
        let remote_pin_id = genid();
        // Create a dummy commit blob and set it as the remote head.
        let commit_meta: TribleSet = TribleSet::new();
        let commit_blob: Blob<SimpleArchive> = commit_meta.to_blob();
        let commit_handle = store.put::<SimpleArchive, _>(commit_blob.clone()).unwrap();
        let remote_meta = branch_unsigned(*remote_pin_id, name_handle, Some(commit_blob));
        let remote_meta_handle = store.put::<SimpleArchive, _>(remote_meta).unwrap();

        let publisher = [0u8; 32];
        let remote_metadata_head: RawHash = remote_meta_handle.raw;

        // Create the tracking pin.
        let tracking_id = create_tracking_pin(
            &mut store,
            *remote_pin_id,
            &remote_metadata_head,
            "remote-branch",
            &publisher,
            false,
        )
        .expect("create");

        // Now find it.
        let found = find_tracking_pin(&mut store, *remote_pin_id, &publisher);
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
            *remote_pin_id,
            &remote_metadata_head,
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
            "tracking pin head should be the inner commit, not the pin-metadata blob"
        );
    }

    #[test]
    fn ensure_does_not_report_success_when_existing_pin_update_fails() {
        use triblespace_core::repo::branch::branch_unsigned;

        let mut store = MemoryRepo::default();
        let remote_pin_id = *genid();
        let name_handle: Inline<Handle<LongString>> =
            store.put("remote".to_owned().to_blob()).unwrap();
        let commit_blob: Blob<SimpleArchive> = TribleSet::new().to_blob();
        let metadata = branch_unsigned(remote_pin_id, name_handle, Some(commit_blob));
        let metadata_head: Inline<Handle<SimpleArchive>> = store.put(metadata).unwrap();
        let publisher = SigningKey::from_bytes(&[31; 32]).verifying_key().to_bytes();

        let tracking_id = ensure_tracking_pin(
            &mut store,
            remote_pin_id,
            &metadata_head.raw,
            "remote",
            &publisher,
            false,
        )
        .expect("initial tracking pin");
        let old_head = store.head(tracking_id).unwrap();

        let absent_metadata_head = [0xFF; 32];
        assert_eq!(
            ensure_tracking_pin(
                &mut store,
                remote_pin_id,
                &absent_metadata_head,
                "remote",
                &publisher,
                false,
            ),
            None,
            "a failed update must not be reported as the existing tracking pin"
        );
        assert_eq!(store.head(tracking_id).unwrap(), old_head);
    }

    #[test]
    fn same_remote_id_from_two_publishers_stays_in_distinct_tracking_pins() {
        use triblespace_core::repo::branch::branch_unsigned;

        fn remote_metadata_head(
            store: &mut MemoryRepo,
            remote_pin_id: Id,
            name_handle: Inline<Handle<LongString>>,
            marker: Id,
        ) -> (RawHash, Inline<Handle<SimpleArchive>>) {
            let commit_set: TribleSet = entity! {
                triblespace_core::metadata::tag: marker,
            }
            .into();
            let commit_blob: Blob<SimpleArchive> = commit_set.to_blob();
            let commit_handle = store.put(commit_blob.clone()).unwrap();
            let remote_meta = branch_unsigned(remote_pin_id, name_handle, Some(commit_blob));
            let remote_meta_handle: Inline<Handle<SimpleArchive>> = store.put(remote_meta).unwrap();
            (remote_meta_handle.raw, commit_handle)
        }

        fn tracking_head_and_publisher(
            store: &mut MemoryRepo,
            tracking_id: Id,
        ) -> (Inline<Handle<SimpleArchive>>, PublisherKey) {
            let meta_handle = store.head(tracking_id).unwrap().unwrap();
            let reader = store.reader().unwrap();
            let meta: TribleSet = reader.get(meta_handle).unwrap();
            let branch_entity =
                triblespace_core::repo::branch::branch_entity(&meta, tracking_id).unwrap();
            let mut heads = find!(
                head: Inline<Handle<SimpleArchive>>,
                pattern!(&meta, [{ branch_entity @ triblespace_core::repo::head: ?head }])
            );
            let (Some(head), None) = (heads.next(), heads.next()) else {
                panic!("tracking pin must carry one scoped head");
            };
            let (_, publisher) = tracking_identity(&meta, branch_entity)
                .expect("tracking pin must carry one exact remote identity");
            (head, publisher)
        }

        let mut store = MemoryRepo::default();
        let remote_pin_id = *genid();
        let name_handle: Inline<Handle<LongString>> =
            store.put("shared-remote-id".to_owned().to_blob()).unwrap();
        let publisher_a = SigningKey::from_bytes(&[17; 32]).verifying_key().to_bytes();
        let publisher_b = SigningKey::from_bytes(&[23; 32]).verifying_key().to_bytes();
        let (head_a, commit_a) =
            remote_metadata_head(&mut store, remote_pin_id, name_handle, *genid());
        let (head_b, commit_b) =
            remote_metadata_head(&mut store, remote_pin_id, name_handle, *genid());

        let tracking_a = ensure_tracking_pin(
            &mut store,
            remote_pin_id,
            &head_a,
            "shared-remote-id",
            &publisher_a,
            false,
        )
        .expect("create publisher A tracking pin");
        let tracking_b = ensure_tracking_pin(
            &mut store,
            remote_pin_id,
            &head_b,
            "shared-remote-id",
            &publisher_b,
            false,
        )
        .expect("create publisher B tracking pin");

        assert_ne!(
            tracking_a, tracking_b,
            "remote pin ids belong to each publisher's namespace"
        );
        assert_eq!(
            find_tracking_pin(&mut store, remote_pin_id, &publisher_a),
            Some(tracking_a)
        );
        assert_eq!(
            find_tracking_pin(&mut store, remote_pin_id, &publisher_b),
            Some(tracking_b)
        );
        assert_eq!(
            tracking_head_and_publisher(&mut store, tracking_a),
            (commit_a, publisher_a)
        );
        assert_eq!(
            tracking_head_and_publisher(&mut store, tracking_b),
            (commit_b, publisher_b)
        );

        // A later update from A must select only A's tracking pin and leave
        // B's same-numbered remote pin untouched.
        let (head_a2, commit_a2) =
            remote_metadata_head(&mut store, remote_pin_id, name_handle, *genid());
        assert_eq!(
            ensure_tracking_pin(
                &mut store,
                remote_pin_id,
                &head_a2,
                "shared-remote-id",
                &publisher_a,
                false,
            ),
            Some(tracking_a)
        );
        assert_eq!(
            tracking_head_and_publisher(&mut store, tracking_a),
            (commit_a2, publisher_a)
        );
        assert_eq!(
            tracking_head_and_publisher(&mut store, tracking_b),
            (commit_b, publisher_b),
            "publisher A's update must not overwrite publisher B's pin"
        );
    }

    #[test]
    fn weak_marker_distinguishes_weak_from_strong_tracking() {
        use triblespace_core::blob::IntoBlob;
        use triblespace_core::repo::branch::branch_unsigned;

        let mut store = MemoryRepo::default();

        // Build a legacy pin-metadata blob to point a tracking pin at.
        // Returns (remote_pin_id, remote_meta_hash).
        let mut make_remote = |label: &str| -> (Id, RawHash) {
            let name_handle: Inline<Handle<LongString>> =
                store.put(label.to_string().to_blob()).unwrap();
            let remote_pin_id = genid();
            let commit_blob: Blob<SimpleArchive> = TribleSet::new().to_blob();
            let _commit_handle = store.put::<SimpleArchive, _>(commit_blob.clone()).unwrap();
            let remote_meta = branch_unsigned(*remote_pin_id, name_handle, Some(commit_blob));
            let remote_meta_handle = store.put::<SimpleArchive, _>(remote_meta).unwrap();
            (*remote_pin_id, remote_meta_handle.raw)
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
