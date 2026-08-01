//! Two-peer local-acceptance properties, exercised deterministically without
//! a replication protocol. Blobs are copied directly between independent
//! `Repository<MemoryRepo>` instances and a local tracking pin is pointed at
//! the copied commit. The tests cover `merge_tracking_into_local`; they do not
//! pretend that legacy scalar-HEAD gossip replicates StrongPin assertions.
//!
//! Key property documented here: **sequential acceptance converges in one
//! round-pair.** When peers accept each other's states one-at-a-time, the
//! first peer to merge produces a
//! merge commit `AM` whose ancestry already contains the other peer's
//! original commit. The second peer's sync then sees `AM` in its
//! local tracking pin, finds its own head (`commit_B`) already in
//! `ancestors(AM)`, and fast-forwards. No second merge commit is needed.
//!
//! Second property exercised here: **parallel local merges converge in
//! zero extra rounds.** Merge commits in triblespace are content-addressed:
//! they carry no author-specific bits (no signature, no `created_at`, no
//! random entity id), so two peers merging the same parent set produce
//! bit-identical merge commits that dedup via blob hash. Parallel-merge
//! scenarios that would have diverged in any centralized-signer system
//! just… don't.

use ed25519_dalek::{SigningKey, VerifyingKey};
use triblespace_core::blob::IntoBlob;
use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::macros::entity;
use triblespace_core::prelude::{BlobStore, PinStore};
use triblespace_core::repo::branch_assertion::BranchIdentity;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStoreGet, BlobStoreList, BlobStorePut, PushResult, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_net::tracking::{self, MergeOutcome, merge_tracking_into_local};

fn new_repo(seed: u8) -> Repository<MemoryRepo> {
    let signing_key = SigningKey::from_bytes(&[seed; 32]);
    let store = MemoryRepo::default();
    Repository::new(store, signing_key, TribleSet::new()).expect("repo")
}

/// Copy every blob from `src`'s store into `dst`'s store. Content-addressed,
/// so dupes are harmless. Simulates a fire-hose "pull everything reachable
/// from head" fetch.
fn copy_all_blobs(src: &mut Repository<MemoryRepo>, dst: &mut Repository<MemoryRepo>) {
    let reader = src.storage_mut().reader().expect("src reader");
    let handles: Vec<_> = reader.blobs().filter_map(|r| r.ok()).collect();
    for handle in handles {
        let bytes: anybytes::Bytes = reader
            .get::<anybytes::Bytes, triblespace_core::blob::encodings::UnknownBlob>(handle)
            .expect("src has the blob");
        let _ = dst
            .storage_mut()
            .put::<triblespace_core::blob::encodings::UnknownBlob, _>(bytes);
    }
}

fn mirror_commit(
    local: &mut Repository<MemoryRepo>,
    remote_identity: BranchIdentity,
    remote_commit: Inline<Handle<SimpleArchive>>,
    branch_name: &str,
    remote_publisher: [u8; 32],
) -> Id {
    let tracking_id = *genid();
    let name_handle: Inline<Handle<LongString>> = local
        .storage_mut()
        .put(branch_name.to_owned().to_blob())
        .unwrap();
    let publisher = VerifyingKey::from_bytes(&remote_publisher).unwrap();
    let meta: TribleSet = entity! {
        triblespace_core::repo::branch: tracking_id,
        triblespace_core::repo::head: remote_commit,
        tracking::remote_name: name_handle,
        tracking::tracking_remote_pin: remote_identity.id().entity(),
        tracking::tracking_peer: publisher,
    }
    .into();
    let meta_handle = local.storage_mut().put(meta).unwrap();
    assert!(matches!(
        local
            .storage_mut()
            .update(tracking_id, None, Some(meta_handle))
            .unwrap(),
        PushResult::Success()
    ));
    tracking_id
}

/// Simulate one local acceptance round from `remote` into `local`:
/// copy the immutable blobs, create a local tracking pin over the exact remote
/// identity's index plus commit, then merge into the local authored branch.
fn sync_round(
    local: &mut Repository<MemoryRepo>,
    remote: &mut Repository<MemoryRepo>,
    branch_name: &str,
) -> MergeOutcome {
    copy_all_blobs(remote, local);
    let remote_identity = remote.branch_identity(branch_name);
    let remote_commit = head_commit(remote, branch_name);
    let tracking_id = mirror_commit(
        local,
        remote_identity,
        remote_commit,
        branch_name,
        remote.verifying_key().to_bytes(),
    );
    merge_tracking_into_local(local, tracking_id, branch_name).expect("merge")
}

fn head_commit(repo: &mut Repository<MemoryRepo>, name: &str) -> Inline<Handle<SimpleArchive>> {
    let ws = repo.pull(repo.branch_identity(name)).unwrap();
    ws.head().expect("branch has head")
}

#[test]
fn sequential_sync_converges_under_divergent_commits() {
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);

    // Both peers independently commit to "main".
    {
        let mut ws = a.create_workspace("main").unwrap();
        ws.commit(TribleSet::new(), "A's commit");
        a.push(&mut ws).unwrap();
    }
    {
        let mut ws = b.create_workspace("main").unwrap();
        ws.commit(TribleSet::new(), "B's commit");
        b.push(&mut ws).unwrap();
    }

    let initial_a = head_commit(&mut a, "main");
    let initial_b = head_commit(&mut b, "main");
    assert_ne!(initial_a, initial_b, "peers start with divergent commits");

    // First sync: A pulls B's commit, merges into A's local "main" →
    // produces a merge commit AM whose parents are (commit_A, commit_B).
    let out_a = sync_round(&mut a, &mut b, "main");
    assert!(
        matches!(out_a, MergeOutcome::Merged { .. }),
        "A must produce a merge commit (commits are divergent)"
    );
    let a_after_merge = head_commit(&mut a, "main");
    assert_ne!(a_after_merge, initial_a, "A's main should advance");
    assert_ne!(
        a_after_merge, initial_b,
        "A's main must not equal B's commit"
    );

    // Second sync: B pulls A's state — which now includes AM — and
    // observes that its own local head (commit_B) is already in the
    // ancestors of AM. merge_commit takes the fast-forward path.
    let out_b = sync_round(&mut b, &mut a, "main");
    assert!(
        matches!(out_b, MergeOutcome::Merged { .. }),
        "B must advance (fast-forward reports Merged too)"
    );

    // Converged: both peers now point at AM.
    let final_a = head_commit(&mut a, "main");
    let final_b = head_commit(&mut b, "main");
    assert_eq!(
        final_a, final_b,
        "sequential sync must converge in one round-pair"
    );
    assert_eq!(
        final_a, a_after_merge,
        "B converges to A's merge, not a new one"
    );

    // A third sync round is now a no-op on both sides.
    let a_again = sync_round(&mut a, &mut b, "main");
    let b_again = sync_round(&mut b, &mut a, "main");
    assert!(matches!(a_again, MergeOutcome::UpToDate));
    assert!(matches!(b_again, MergeOutcome::UpToDate));
}

#[test]
fn parallel_merges_produce_identical_commits() {
    // Simulated parallel acceptance: both peers see each other's original
    // commits first, then BOTH merge before either has seen the other's
    // merge. Because merge commits are content-addressed (no signature,
    // no `created_at`, entity id derived from the parent set), the two
    // sides produce **bit-identical** merge commits and converge
    // immediately — no extra round needed to resolve divergence.
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);

    // Both peers commit independently.
    {
        let mut ws = a.create_workspace("main").unwrap();
        ws.commit(TribleSet::new(), "A's commit");
        a.push(&mut ws).unwrap();
    }
    {
        let mut ws = b.create_workspace("main").unwrap();
        ws.commit(TribleSet::new(), "B's commit");
        b.push(&mut ws).unwrap();
    }

    // Exchange only the original commits — no merges in the store yet.
    copy_all_blobs(&mut a, &mut b);
    copy_all_blobs(&mut b, &mut a);

    let a_identity = a.branch_identity("main");
    let b_identity = b.branch_identity("main");
    let a_head = head_commit(&mut a, "main");
    let b_head = head_commit(&mut b, "main");
    let pub_a = a.verifying_key().to_bytes();
    let pub_b = b.verifying_key().to_bytes();

    let tracking_in_a = mirror_commit(&mut a, b_identity, b_head, "main", pub_b);
    let tracking_in_b = mirror_commit(&mut b, a_identity, a_head, "main", pub_a);

    // Parallel merge: both sides merge against their pre-merge views,
    // against the same parent set.
    merge_tracking_into_local(&mut a, tracking_in_a, "main").unwrap();
    merge_tracking_into_local(&mut b, tracking_in_b, "main").unwrap();

    let a_after = head_commit(&mut a, "main");
    let b_after = head_commit(&mut b, "main");
    assert_eq!(
        a_after, b_after,
        "content-addressed merges: same parent set → same merge commit"
    );

    // And a follow-up sync is a pure no-op — both sides are already at
    // the same head, no merge commit to produce or fast-forward to.
    let a_next = sync_round(&mut a, &mut b, "main");
    let b_next = sync_round(&mut b, &mut a, "main");
    assert!(matches!(a_next, MergeOutcome::UpToDate));
    assert!(matches!(b_next, MergeOutcome::UpToDate));
}

#[test]
fn single_round_converges_when_only_one_side_advanced() {
    // If only A commits and B is empty, one sync round fast-forwards B
    // without producing a merge commit.
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);

    {
        let mut ws = a.create_workspace("main").unwrap();
        ws.commit(TribleSet::new(), "A's only commit");
        a.push(&mut ws).unwrap();
    }

    let outcome = sync_round(&mut b, &mut a, "main");
    assert!(
        matches!(outcome, MergeOutcome::Merged { .. }),
        "fast-forward still reports Merged (advance-to-tip)"
    );
    assert_eq!(
        head_commit(&mut a, "main"),
        head_commit(&mut b, "main"),
        "one round is enough when only one side advanced"
    );

    // Second round is a no-op on both sides.
    let again = sync_round(&mut b, &mut a, "main");
    assert!(matches!(again, MergeOutcome::UpToDate));
}
