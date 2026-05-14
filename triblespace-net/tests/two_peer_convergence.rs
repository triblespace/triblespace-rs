//! Two-peer convergence properties, exercised deterministically without
//! the iroh transport in the loop. "Gossip" is simulated by copying
//! blobs directly between two independent `Repository<MemoryRepo>`
//! instances and hand-creating tracking branches — the interesting bit
//! is what `merge_tracking_into_local` does on each side, which is
//! where the distributed sync design actually lives.
//!
//! Key property documented here: **sequential gossip converges in one
//! round-pair.** When peers see each other's states one-at-a-time
//! (the realistic gossip ordering), the first peer to merge produces a
//! merge commit `AM` whose ancestry already contains the other peer's
//! original commit. The second peer's sync then sees `AM` in its
//! tracking branch, finds its own head (`commit_B`) already in
//! `ancestors(AM)`, and fast-forwards. No second merge commit is needed.
//!
//! Second property exercised here: **parallel gossip merges converge in
//! zero extra rounds.** Merge commits in triblespace are content-addressed:
//! they carry no author-specific bits (no signature, no `created_at`, no
//! random entity id), so two peers merging the same parent set produce
//! bit-identical merge commits that dedup via blob hash. Parallel-merge
//! scenarios that would have diverged in any centralized-signer system
//! just… don't.

use ed25519_dalek::SigningKey;
use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::prelude::{BlobStore, BranchStore};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStoreGet, BlobStoreList, BlobStorePut, Repository,
};
use triblespace_core::trible::TribleSet;
use triblespace_core::value::schemas::hash::{Blake3, Handle};
use triblespace_core::value::Inline;
use triblespace_net::tracking::{
    ensure_tracking_branch, merge_tracking_into_local, MergeOutcome,
};

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
    let handles: Vec<_> = reader
        .blobs()
        .filter_map(|r| r.ok())
        .collect();
    for handle in handles {
        let bytes: anybytes::Bytes = reader
            .get::<anybytes::Bytes, triblespace_core::blob::schemas::UnknownBlob>(handle)
            .expect("src has the blob");
        let _ = dst
            .storage_mut()
            .put::<triblespace_core::blob::schemas::UnknownBlob, _>(bytes);
    }
}

/// Return the hash of the branch-metadata blob that `repo`'s named
/// branch currently points at. This is what a real gossip message would
/// carry for that branch.
fn remote_head_hash(repo: &mut Repository<MemoryRepo>, name: &str) -> [u8; 32] {
    let branch_id = repo
        .lookup_branch(name)
        .expect("lookup branch")
        .expect("branch exists");
    repo.storage_mut()
        .head(branch_id)
        .expect("head")
        .expect("branch has head")
        .raw
}

fn lookup_id(repo: &mut Repository<MemoryRepo>, name: &str) -> Id {
    repo.lookup_branch(name).unwrap().unwrap()
}

/// Simulate one sync round from `remote` into `local`:
/// - copy all of remote's blobs into local
/// - ensure/update a tracking branch in local pointing at remote's HEAD
/// - run `merge_tracking_into_local` on `local` for the named branch
fn sync_round(
    local: &mut Repository<MemoryRepo>,
    remote: &mut Repository<MemoryRepo>,
    branch_name: &str,
    remote_publisher: &[u8; 32],
) -> MergeOutcome {
    copy_all_blobs(remote, local);
    let remote_branch_id = lookup_id(remote, branch_name);
    let remote_head = remote_head_hash(remote, branch_name);
    let tracking_id = ensure_tracking_branch(
        local.storage_mut(),
        remote_branch_id,
        &remote_head,
        branch_name,
        remote_publisher,
    )
    .expect("ensure tracking");
    merge_tracking_into_local(local, tracking_id, branch_name).expect("merge")
}

fn head_commit(repo: &mut Repository<MemoryRepo>, name: &str) -> Inline<Handle<SimpleArchive>> {
    let id = lookup_id(repo, name);
    let ws = repo.pull(id).unwrap();
    ws.head().expect("branch has head")
}

#[test]
fn sequential_sync_converges_under_divergent_commits() {
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);
    let pub_a = [0x0Au8; 32];
    let pub_b = [0x0Bu8; 32];

    // Both peers independently commit to "main".
    {
        let id = a.ensure_branch("main", None).unwrap();
        let mut ws = a.pull(id).unwrap();
        ws.commit(TribleSet::new(), "A's commit");
        a.push(&mut ws).unwrap();
    }
    {
        let id = b.ensure_branch("main", None).unwrap();
        let mut ws = b.pull(id).unwrap();
        ws.commit(TribleSet::new(), "B's commit");
        b.push(&mut ws).unwrap();
    }

    let initial_a = head_commit(&mut a, "main");
    let initial_b = head_commit(&mut b, "main");
    assert_ne!(initial_a, initial_b, "peers start with divergent commits");

    // First sync: A pulls B's commit, merges into A's local "main" →
    // produces a merge commit AM whose parents are (commit_A, commit_B).
    let out_a = sync_round(&mut a, &mut b, "main", &pub_b);
    assert!(
        matches!(out_a, MergeOutcome::Merged { .. }),
        "A must produce a merge commit (commits are divergent)"
    );
    let a_after_merge = head_commit(&mut a, "main");
    assert_ne!(a_after_merge, initial_a, "A's main should advance");
    assert_ne!(a_after_merge, initial_b, "A's main must not equal B's commit");

    // Second sync: B pulls A's state — which now includes AM — and
    // observes that its own local head (commit_B) is already in the
    // ancestors of AM. merge_commit takes the fast-forward path.
    let out_b = sync_round(&mut b, &mut a, "main", &pub_a);
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
    assert_eq!(final_a, a_after_merge, "B converges to A's merge, not a new one");

    // A third sync round is now a no-op on both sides.
    let a_again = sync_round(&mut a, &mut b, "main", &pub_b);
    let b_again = sync_round(&mut b, &mut a, "main", &pub_a);
    assert!(matches!(a_again, MergeOutcome::UpToDate));
    assert!(matches!(b_again, MergeOutcome::UpToDate));
}

#[test]
fn parallel_merges_produce_identical_commits() {
    // Simulated parallel gossip: both peers see each other's original
    // commits first, then BOTH merge before either has seen the other's
    // merge. Because merge commits are content-addressed (no signature,
    // no `created_at`, entity id derived from the parent set), the two
    // sides produce **bit-identical** merge commits and converge
    // immediately — no extra round needed to resolve divergence.
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);
    let pub_a = [0x0Au8; 32];
    let pub_b = [0x0Bu8; 32];

    // Both peers commit independently.
    {
        let id = a.ensure_branch("main", None).unwrap();
        let mut ws = a.pull(id).unwrap();
        ws.commit(TribleSet::new(), "A's commit");
        a.push(&mut ws).unwrap();
    }
    {
        let id = b.ensure_branch("main", None).unwrap();
        let mut ws = b.pull(id).unwrap();
        ws.commit(TribleSet::new(), "B's commit");
        b.push(&mut ws).unwrap();
    }

    // Exchange only the original commits — no merges in the store yet.
    copy_all_blobs(&mut a, &mut b);
    copy_all_blobs(&mut b, &mut a);

    let a_branch_id = lookup_id(&mut a, "main");
    let b_branch_id = lookup_id(&mut b, "main");
    let a_head = remote_head_hash(&mut a, "main");
    let b_head = remote_head_hash(&mut b, "main");

    let tracking_in_a = ensure_tracking_branch(
        a.storage_mut(), b_branch_id, &b_head, "main", &pub_b,
    )
    .unwrap();
    let tracking_in_b = ensure_tracking_branch(
        b.storage_mut(), a_branch_id, &a_head, "main", &pub_a,
    )
    .unwrap();

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
    let a_next = sync_round(&mut a, &mut b, "main", &pub_b);
    let b_next = sync_round(&mut b, &mut a, "main", &pub_a);
    assert!(matches!(a_next, MergeOutcome::UpToDate));
    assert!(matches!(b_next, MergeOutcome::UpToDate));
}

#[test]
fn single_round_converges_when_only_one_side_advanced() {
    // If only A commits and B is empty, one sync round fast-forwards B
    // without producing a merge commit.
    let mut a = new_repo(0x0A);
    let mut b = new_repo(0x0B);
    let pub_a = [0x0Au8; 32];

    {
        let id = a.ensure_branch("main", None).unwrap();
        let mut ws = a.pull(id).unwrap();
        ws.commit(TribleSet::new(), "A's only commit");
        a.push(&mut ws).unwrap();
    }

    let outcome = sync_round(&mut b, &mut a, "main", &pub_a);
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
    let again = sync_round(&mut b, &mut a, "main", &pub_a);
    assert!(matches!(again, MergeOutcome::UpToDate));
}
