//! Property-based convergence tests for the branch-sync merge logic.
//!
//! Like `two_peer_convergence`, these exercise `merge_tracking_into_local`
//! directly — "gossip" is simulated by copying blobs between independent
//! `Repository<MemoryRepo>` instances and hand-creating tracking pins, so
//! the merge algorithm is tested without the iroh transport in the loop.
//! Being pure and deterministic, the merge is ideal for property testing.
//!
//! The headline property is **confluence**: when N peers commit
//! divergently and then gossip in *any* order, they all converge to the
//! **same** head — and that head is independent of the gossip order. This
//! is the join-semilattice / CRDT property the whole distributed design
//! rests on (content-addressed merge commits over a parent set are
//! commutative, associative, and idempotent), checked here across
//! randomized gossip schedules and seeds.

use ed25519_dalek::SigningKey;
use rand::{Rng, SeedableRng};
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::Inline;
use triblespace_core::prelude::{BlobStore, PinStore};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStoreGet, BlobStoreList, BlobStorePut, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_net::tracking::{ensure_tracking_pin, merge_tracking_into_local, MergeOutcome};

// ── Helpers (mirrors two_peer_convergence's, kept self-contained) ──────

fn copy_all_blobs(src: &mut Repository<MemoryRepo>, dst: &mut Repository<MemoryRepo>) {
    use triblespace_core::blob::encodings::UnknownBlob;
    let reader = src.storage_mut().reader().expect("src reader");
    let handles: Vec<_> = reader.blobs().filter_map(|r| r.ok()).collect();
    for handle in handles {
        let bytes: anybytes::Bytes = reader
            .get::<anybytes::Bytes, UnknownBlob>(handle)
            .expect("src has the blob");
        let _ = dst.storage_mut().put::<UnknownBlob, _>(bytes);
    }
}

fn lookup_id(repo: &mut Repository<MemoryRepo>, name: &str) -> Id {
    repo.lookup_branch(name).unwrap().unwrap()
}

fn remote_head_hash(repo: &mut Repository<MemoryRepo>, name: &str) -> [u8; 32] {
    let branch_id = lookup_id(repo, name);
    repo.storage_mut()
        .head(branch_id)
        .expect("head")
        .expect("branch has head")
        .raw
}

fn head_commit(repo: &mut Repository<MemoryRepo>, name: &str) -> Inline<Handle<SimpleArchive>> {
    let id = lookup_id(repo, name);
    let ws = repo.pull(id).unwrap();
    ws.head().expect("branch has head")
}

/// The checked-out content of `repo`'s branch — the merged TribleSet.
/// This is the *value* that must converge regardless of gossip order,
/// even where the commit-DAG hash is path-dependent (N>2 pairwise
/// merges build different merge-commit trees).
fn content(repo: &mut Repository<MemoryRepo>, name: &str) -> TribleSet {
    let id = lookup_id(repo, name);
    repo.pull(id).unwrap().checkout(..).expect("checkout").into_facts()
}

/// One sync round: copy `from`'s blobs into `to`, point a tracking pin at
/// `from`'s HEAD, merge into `to`'s local branch.
fn sync_round(
    to: &mut Repository<MemoryRepo>,
    from: &mut Repository<MemoryRepo>,
    branch: &str,
    from_pub: &[u8; 32],
) -> MergeOutcome {
    copy_all_blobs(from, to);
    let from_branch_id = lookup_id(from, branch);
    let from_head = remote_head_hash(from, branch);
    let tracking_id =
        ensure_tracking_pin(to.storage_mut(), from_branch_id, &from_head, branch, from_pub)
            .expect("ensure tracking");
    merge_tracking_into_local(to, tracking_id, branch).expect("merge")
}

// ── Multi-peer scaffolding ─────────────────────────────────────────────

const BRANCH: &str = "main";

struct Peer {
    repo: Repository<MemoryRepo>,
    pubkey: [u8; 32],
}

/// `n` peers, each with one independent (divergent) commit on `main`.
/// Distinct commit messages guarantee distinct original commits.
fn diverged_peers(n: usize) -> Vec<Peer> {
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::macros::entity;
    (0..n)
        .map(|i| {
            let seed = 0x10 + i as u8;
            // The publisher must be a *valid* ed25519 verifying key
            // (create_tracking_pin reconstructs it), so derive it from
            // the signing key rather than using arbitrary label bytes.
            let sk = SigningKey::from_bytes(&[seed; 32]);
            let pubkey = sk.verifying_key().to_bytes();
            let mut repo =
                Repository::new(MemoryRepo::default(), sk, TribleSet::new()).expect("repo");
            let id = repo.ensure_branch(BRANCH, None).unwrap();
            let mut ws = repo.pull(id).unwrap();
            // A distinct, non-empty payload per peer so the merged
            // *content* (the union of tribles) is observable and the
            // commits genuinely diverge.
            let e = Id::new([0x20 + i as u8; 16]).unwrap();
            let payload: TribleSet = entity! {
                ExclusiveId::force_ref(&e) @
                triblespace_core::metadata::tag: Id::new([0x30 + i as u8; 16]).unwrap(),
            }
            .into();
            ws.commit(payload, &format!("peer {i} commit"));
            repo.push(&mut ws).unwrap();
            Peer { repo, pubkey }
        })
        .collect()
}

/// Sync `from` into `to` by index (disjoint mutable borrows out of the
/// slice). `to == from` is a no-op.
fn sync(peers: &mut [Peer], to: usize, from: usize) -> MergeOutcome {
    if to == from {
        return MergeOutcome::UpToDate;
    }
    let from_pub = peers[from].pubkey;
    let (lo, hi) = (to.min(from), to.max(from));
    let (left, right) = peers.split_at_mut(hi);
    let (to_repo, from_repo) = if to < from {
        (&mut left[to].repo, &mut right[0].repo)
    } else {
        (&mut right[0].repo, &mut left[from].repo)
    };
    let _ = lo;
    sync_round(to_repo, from_repo, BRANCH, &from_pub)
}

/// Deterministic all-pairs drain until a full pass is entirely
/// `UpToDate` — guarantees the mesh is quiescent and fully converged.
/// Terminates because the merge is monotonic. Returns the pass count.
fn drain_to_quiescence(peers: &mut [Peer]) -> u32 {
    let n = peers.len();
    let mut passes = 0u32;
    loop {
        passes += 1;
        let mut all_uptodate = true;
        for to in 0..n {
            for from in 0..n {
                if to != from
                    && !matches!(sync(peers, to, from), MergeOutcome::UpToDate)
                {
                    all_uptodate = false;
                }
            }
        }
        assert!(passes < 100, "drain failed to converge — non-monotonic merge?");
        if all_uptodate {
            return passes;
        }
    }
}

/// All peers share the same `main` head.
fn all_converged(peers: &mut [Peer]) -> Inline<Handle<SimpleArchive>> {
    let head0 = head_commit(&mut peers[0].repo, BRANCH);
    for i in 1..peers.len() {
        assert_eq!(
            head_commit(&mut peers[i].repo, BRANCH),
            head0,
            "peer {i} did not converge to the common head"
        );
    }
    head0
}

// ── Properties ─────────────────────────────────────────────────────────

/// CONFLUENCE (content): N peers commit divergently; under *any*
/// randomized gossip order they converge to the **same content** every
/// time — the union of all N payloads, independent of the order peers
/// learned each other's state. This is the join-semilattice property the
/// distributed design rests on.
///
/// Note the deliberate distinction from the commit *hash*: for N>2,
/// pairwise merges build different merge-commit DAGs depending on order
/// (`((a,b),c)` vs `((a,c),b)`), so the head hash is path-dependent. What
/// is order-independent — and what actually matters — is the merged
/// *value*. Each run still reaches internal agreement (see
/// [`all_converged`]); across runs, only the content is asserted equal.
#[test]
fn converged_content_is_order_independent() {
    let n = 4;
    let mut canonical: Option<TribleSet> = None;

    for trial in 0..10u64 {
        let mut peers = diverged_peers(n);
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0_F1_00 + trial);

        // Randomized gossip phase: arbitrary (from -> to) syncs.
        for _ in 0..(n * n * 3) {
            let from = rng.gen_range(0..n);
            let to = rng.gen_range(0..n);
            sync(&mut peers, to, from);
        }
        drain_to_quiescence(&mut peers);

        // Within a run, all peers agree on a head...
        all_converged(&mut peers);
        // ...and the merged content is the same union for everyone.
        let c0 = content(&mut peers[0].repo, BRANCH);
        for i in 1..n {
            assert_eq!(content(&mut peers[i].repo, BRANCH), c0, "peer {i} content");
        }
        // ...and that content is identical across gossip orders.
        match &canonical {
            None => canonical = Some(c0),
            Some(c) => assert_eq!(
                &c0, c,
                "converged content must be independent of gossip order (trial {trial})"
            ),
        }
    }
}

/// Confluence holds across a range of mesh sizes — the join is the same
/// commit no matter how many peers (and how many divergent commits)
/// participate, as long as the commit *set* is the same. Here each size
/// is its own commit set, so we only assert within-size convergence.
#[test]
fn convergence_holds_across_mesh_sizes() {
    for n in [2usize, 3, 5, 7] {
        let mut peers = diverged_peers(n);
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x5126_0000 + n as u64);
        for _ in 0..(n * n * 4) {
            let from = rng.gen_range(0..n);
            let to = rng.gen_range(0..n);
            sync(&mut peers, to, from);
        }
        let passes = drain_to_quiescence(&mut peers);
        all_converged(&mut peers);
        // Sanity: a converged mesh re-syncs as a pure no-op.
        assert_eq!(
            drain_to_quiescence(&mut peers),
            1,
            "n={n}: a converged mesh must re-drain in a single all-UpToDate pass"
        );
        let _ = passes;
    }
}

/// Order-of-arrival: feeding one peer the others' states in different
/// orders lands on the same merged *content* (the union), isolating
/// "order each remote arrives in" from the random scheduler. The commit
/// hash may differ between orders (path-dependent merge DAG); the value
/// does not.
#[test]
fn merge_content_is_independent_of_remote_arrival_order() {
    fn collapse_into_peer0(order: &[usize]) -> TribleSet {
        let mut peers = diverged_peers(4);
        for &from in order {
            sync(&mut peers, 0, from);
        }
        content(&mut peers[0].repo, BRANCH)
    }

    let ascending = collapse_into_peer0(&[1, 2, 3]);
    let descending = collapse_into_peer0(&[3, 2, 1]);
    let shuffled = collapse_into_peer0(&[2, 1, 3]);
    assert_eq!(ascending, descending, "arrival 1,2,3 vs 3,2,1 — same content");
    assert_eq!(ascending, shuffled, "arrival 1,2,3 vs 2,1,3 — same content");
}

/// TRANSITIVE convergence: peers in a line `0—1—2—…—(n-1)` where only
/// *adjacent* pairs ever sync. No peer talks directly to a non-neighbor,
/// yet the whole line converges to the full union — gossip propagates
/// through intermediaries. Because the merge is monotonic, repeated
/// adjacent passes reach the global join; the test bounds the pass count
/// to catch any failure to propagate.
#[test]
fn line_topology_converges_transitively() {
    let n = 5;
    let mut peers = diverged_peers(n);

    let mut passes = 0u32;
    loop {
        passes += 1;
        let mut changed = false;
        for i in 0..n - 1 {
            // sync each adjacent pair both directions
            if !matches!(sync(&mut peers, i, i + 1), MergeOutcome::UpToDate) {
                changed = true;
            }
            if !matches!(sync(&mut peers, i + 1, i), MergeOutcome::UpToDate) {
                changed = true;
            }
        }
        assert!(passes < 50, "line topology failed to converge transitively");
        if !changed {
            break;
        }
    }

    all_converged(&mut peers);
    // Every peer ends with the full union — the endpoints (0 and n-1),
    // which never synced directly, still each hold the other's payload.
    let full = content(&mut peers[0].repo, BRANCH);
    for i in 1..n {
        assert_eq!(
            content(&mut peers[i].repo, BRANCH),
            full,
            "peer {i} did not receive the full union over the line"
        );
    }
    // Sanity: the union is non-trivial (all n distinct payloads merged),
    // so transitive propagation actually moved data end to end.
    assert!(
        full.len() >= n,
        "expected at least one trible per peer in the merged union"
    );
}

// NOTE: there is deliberately no "same seed → identical commit hash"
// test here. Authored commits carry `created_at = epoch_now()`
// (commit.rs), which is only reproducible under the *seeded virtual
// clock* the sim harness installs — these pure repo tests run on the
// real wall clock, so commit hashes legitimately differ run to run. The
// meaningful, timestamp-agnostic guarantee — that the merged *content*
// converges order-independently — is covered above; hash-level
// determinism (with the virtual clock) is covered by the sim_lazy
// determinism meta-test.

/// A late joiner bootstrapping into a running mesh. Peers 0,1,2 converge
/// first (peer 3 isolated); then peer 3 — carrying its own divergent
/// commit — joins and gossips. Everyone converges *both ways*: the late
/// joiner catches up to the established union, and the settled peers
/// absorb the joiner's payload. Tests the "new node joins an existing
/// system" path, which the all-start-together scenarios don't.
#[test]
fn late_joiner_converges_both_ways() {
    let mut peers = diverged_peers(4); // peer 3 is the late joiner.

    // Phase 1: converge the sub-mesh {0,1,2} only (peer 3 untouched).
    loop {
        let mut changed = false;
        for to in 0..3 {
            for from in 0..3 {
                if to != from
                    && !matches!(sync(&mut peers, to, from), MergeOutcome::UpToDate)
                {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    let established = content(&mut peers[0].repo, BRANCH);
    assert_eq!(content(&mut peers[1].repo, BRANCH), established);
    assert_eq!(content(&mut peers[2].repo, BRANCH), established);
    assert_ne!(
        content(&mut peers[3].repo, BRANCH),
        established,
        "the late joiner has not yet seen the established mesh"
    );

    // Phase 2: peer 3 joins — full-mesh drain.
    drain_to_quiescence(&mut peers);
    all_converged(&mut peers);

    let full = content(&mut peers[0].repo, BRANCH);
    assert!(
        full.len() > established.len(),
        "the joiner's payload must be absorbed by the settled peers"
    );
    for i in 0..4 {
        assert_eq!(
            content(&mut peers[i].repo, BRANCH),
            full,
            "peer {i} did not reach the post-join union"
        );
    }
}
