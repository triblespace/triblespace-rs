//! N-node seeded fault-script property test — DST stage 4.
//!
//! A `World` of N nodes runs the full production protocol stack over
//! the deterministic [`SimNet`]. A seeded script interleaves random
//! operations — commits on random nodes, partitions, heals, crashes,
//! revivals — then heals everything and drives to quiescence.
//!
//! Invariants asserted per run:
//!
//! 1. **Convergence** — after faults clear, every node's local "main"
//!    reaches the same head commit (multi-writer: heads merge; merge
//!    commits are content-addressed so parallel merges dedup).
//! 2. **Closure** — every node can `checkout(..)` its full history:
//!    the workspace walk touches every commit + content blob, so one
//!    missing blob fails the walk. This is the "stored head ⇒ stored
//!    closure" invariant `fetch_reachable`'s abort logic protects.
//! 3. **Deterministic replay** — the same seed reproduces the same
//!    op log, the same quiescence tick, the same final head, and the
//!    same per-node blob counts.
//!
//! Run: `cargo test -p triblespace-net --features sim --test sim_swarm`
#![cfg(feature = "sim")]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::id::rngid::seed_ids;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStoreList, BlobStorePut, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::tracking;
use triblespace_net::transport::sim::{SimConfig, SimNet};

fn vclock() -> Arc<VirtualClock> {
    static CLOCK: OnceLock<Arc<VirtualClock>> = OnceLock::new();
    CLOCK
        .get_or_init(|| {
            let base = hifitime::Epoch::from_gregorian_utc_at_midnight(2026, 1, 1);
            let vc = VirtualClock::new(base);
            clock::install_virtual(vc.clone()).expect("first clock install");
            vc
        })
        .clone()
}

fn admin_cap(
    root: &SigningKey,
    subject: &SigningKey,
) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::id::ufoid;
    use triblespace_core::macros::entity;
    let scope_root = *ufoid();
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: PERM_ADMIN,
    });
    let now = clock::epoch_now();
    let expiry: Inline<NsTAIInterval> = (now, now + hifitime::Duration::from_days(30.0))
        .try_to_inline()
        .expect("interval");
    capability::build_capability(
        root,
        subject.verifying_key(),
        None,
        scope_root,
        scope_facts,
        expiry,
    )
    .expect("build cap")
}

struct Node {
    id: [u8; 32],
    repo: Repository<Peer<MemoryRepo>>,
    commits_made: u32,
}

/// One operation in the seeded fault script. Logged as the run trace;
/// trace equality across same-seed runs is part of the determinism
/// assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Commit { node: usize },
    Partition { a: usize, b: usize },
    Heal { a: usize, b: usize },
    Crash { node: usize },
    Revive { node: usize },
}

#[derive(Debug, PartialEq, Eq)]
struct WorldReport {
    ops: Vec<Op>,
    final_head: [u8; 32],
    quiesced_at_ns: u64,
    blob_counts: Vec<usize>,
    commits_made: Vec<u32>,
}

fn run_world(seed: u64, n_nodes: usize, n_ops: usize) -> WorldReport {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("error")),
        )
        .with_test_writer()
        .try_init();
    let vc = vclock();
    vc.reset();
    seed_ids(seed);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(world_body(vc, seed, n_nodes, n_ops)))
}

async fn world_body(vc: Arc<VirtualClock>, seed: u64, n_nodes: usize, n_ops: usize) -> WorldReport {
    let start_ns = vc.now_ns();
    let net = SimNet::new(seed, SimConfig::default());
    // Script RNG is separate from the net's link RNG so adding ops
    // doesn't shift latencies and vice versa.
    let mut script = StdRng::seed_from_u64(seed ^ 0x5157_4152_4d21);

    // ── Identities + full cap matrix ─────────────────────────────────
    let root = SigningKey::from_bytes(&[0xEE; 32]);
    let team_root = root.verifying_key();
    let keys: Vec<SigningKey> = (0..n_nodes)
        .map(|i| SigningKey::from_bytes(&[0x10 + i as u8; 32]))
        .collect();
    let caps: Vec<(Blob<SimpleArchive>, Blob<SimpleArchive>)> =
        keys.iter().map(|k| admin_cap(&root, k)).collect();

    // ── Bring up nodes ───────────────────────────────────────────────
    let mut nodes: Vec<Node> = Vec::with_capacity(n_nodes);
    for (i, k) in keys.iter().enumerate() {
        let mut store = MemoryRepo::default();
        for (cap, sig) in &caps {
            store.put::<SimpleArchive, _>(cap.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig.clone()).unwrap();
        }
        let id = k.verifying_key().to_bytes();
        let self_cap = (&caps[i].1).get_handle().raw;
        let harness = net.join(id, true);
        let (sender, receiver, wiring) =
            host::wire(EndpointId::from_bytes(&id).expect("endpoint id"));
        tokio::task::spawn_local(host::run_host(
            harness,
            PeerConfig {
                peers: Vec::new(),
                gossip: true,
                team_root,
                self_cap,
                direction: SyncDirection::Bidirectional,
            },
            wiring,
        ));
        let peer = Peer::with_wiring(
            store,
            k.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender,
            receiver,
        );
        let repo = Repository::new(peer, k.clone(), TribleSet::new()).expect("repo");
        nodes.push(Node {
            id,
            repo,
            commits_made: 0,
        });
    }

    // Every node creates its local "main" up front so multi-writer
    // merges have a target on every side.
    for node in nodes.iter_mut() {
        let _ = node.repo.ensure_branch("main", None);
    }

    // One sim tick: advance time, refresh every node, merge every
    // node's tracking pins into its local branches.
    async fn tick(vc: &VirtualClock, nodes: &mut [Node]) {
        SimNet::step(vc, Duration::from_millis(50)).await;
        for node in nodes.iter_mut() {
            node.repo.storage_mut().refresh();
            for info in tracking::list_tracking_pins(node.repo.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut node.repo,
                    info.local_id,
                    &info.remote_name,
                );
            }
        }
    }

    fn head_of(node: &mut Node) -> Option<[u8; 32]> {
        node.repo
            .lookup_branch("main")
            .ok()
            .flatten()
            .and_then(|id| node.repo.pull(id).ok())
            .and_then(|ws| ws.head())
            .map(|h| h.raw)
    }

    // ── The seeded fault script ──────────────────────────────────────
    let mut ops: Vec<Op> = Vec::with_capacity(n_ops);
    let mut partitions: Vec<(usize, usize)> = Vec::new();
    let mut crashed: Vec<usize> = Vec::new();

    for _ in 0..n_ops {
        let op = match script.gen_range(0..100u32) {
            // Commits dominate — they're the payload the network is
            // moving; faults are the weather.
            0..=49 => Op::Commit {
                node: script.gen_range(0..n_nodes),
            },
            50..=64 => {
                let a = script.gen_range(0..n_nodes);
                let b = script.gen_range(0..n_nodes);
                if a == b {
                    Op::Commit { node: a }
                } else {
                    Op::Partition {
                        a: a.min(b),
                        b: a.max(b),
                    }
                }
            }
            65..=79 => match partitions.last().copied() {
                Some((a, b)) => Op::Heal { a, b },
                None => Op::Commit {
                    node: script.gen_range(0..n_nodes),
                },
            },
            80..=89 => {
                let node = script.gen_range(0..n_nodes);
                if crashed.contains(&node) {
                    Op::Revive { node }
                } else {
                    Op::Crash { node }
                }
            }
            _ => match crashed.last().copied() {
                Some(node) => Op::Revive { node },
                None => Op::Commit {
                    node: script.gen_range(0..n_nodes),
                },
            },
        };

        match &op {
            Op::Commit { node } => {
                let nd = &mut nodes[*node];
                if let Ok(Some(branch_id)) = nd.repo.lookup_branch("main") {
                    if let Ok(mut ws) = nd.repo.pull(branch_id) {
                        let label = format!("commit {} from node {}", nd.commits_made, node);
                        ws.commit(TribleSet::new(), &label);
                        if nd.repo.push(&mut ws).is_ok() {
                            nd.commits_made += 1;
                        }
                    }
                }
            }
            Op::Partition { a, b } => {
                net.partition(nodes[*a].id, nodes[*b].id);
                if !partitions.contains(&(*a, *b)) {
                    partitions.push((*a, *b));
                }
            }
            Op::Heal { a, b } => {
                net.heal(nodes[*a].id, nodes[*b].id);
                partitions.retain(|p| p != &(*a, *b));
            }
            Op::Crash { node } => {
                net.crash(nodes[*node].id);
                crashed.push(*node);
            }
            Op::Revive { node } => {
                net.revive(nodes[*node].id);
                crashed.retain(|c| c != node);
            }
        }
        ops.push(op);

        // Let the world breathe between ops.
        for _ in 0..10 {
            tick(&vc, &mut nodes).await;
        }
    }

    // ── Clear all faults, drive to quiescence ────────────────────────
    for (a, b) in partitions.drain(..) {
        net.heal(nodes[a].id, nodes[b].id);
    }
    for node in crashed.drain(..) {
        net.revive(nodes[node].id);
    }

    // Quiescence: all heads equal and stable across a full gossip
    // rebroadcast period (the recovery mechanism for anything a
    // partition swallowed fires every 30 virtual seconds).
    let mut quiesced_at_ns = None;
    let mut stable_since: Option<u64> = None;
    for round in 0..1_500u32 {
        tick(&vc, &mut nodes).await;
        let heads: Vec<Option<[u8; 32]>> = nodes.iter_mut().map(head_of).collect();
        if round % 100 == 0 {
            let hp: Vec<String> = heads
                .iter()
                .map(|h| h.map(|h| hex::encode(&h[..4])).unwrap_or("none".into()))
                .collect();
            let pins: Vec<usize> = nodes
                .iter_mut()
                .map(|n| tracking::list_tracking_pins(n.repo.storage_mut()).len())
                .collect();
            eprintln!(
                "[world] tick {round} t={}s heads={hp:?} pins={pins:?}",
                (vc.now_ns() - start_ns) / 1_000_000_000
            );
        }
        let all_equal = heads.first().map(|h| h.is_some()).unwrap_or(false)
            && heads.windows(2).all(|w| w[0] == w[1]);
        if all_equal {
            let since = *stable_since.get_or_insert(vc.now_ns());
            // Stability must outlive one full 30s gossip rebroadcast
            // period: convergence has to survive a re-flood, not
            // just touch equality.
            if vc.now_ns() - since >= 35_000_000_000 {
                quiesced_at_ns = Some(vc.now_ns() - start_ns);
                break;
            }
        } else {
            stable_since = None;
        }
    }
    let quiesced_at_ns = quiesced_at_ns.expect(
        "world must quiesce: all heads equal + stable for a full \
         rebroadcast period within the tick budget",
    );

    // ── Invariants ───────────────────────────────────────────────────
    let final_head = head_of(&mut nodes[0]).expect("converged head");

    // Closure: every node replays its FULL history. checkout(..)
    // walks every commit blob and content blob reachable from the
    // head — a single missing blob errors the walk.
    for (i, node) in nodes.iter_mut().enumerate() {
        let branch_id = node
            .repo
            .lookup_branch("main")
            .ok()
            .flatten()
            .expect("branch exists");
        let mut ws = node.repo.pull(branch_id).expect("pull");
        let _facts = ws
            .checkout(..)
            .unwrap_or_else(|e| panic!("closure violated on node {i}: checkout failed: {e:?}"));
    }

    let blob_counts: Vec<usize> = nodes
        .iter_mut()
        .map(|n| {
            let reader = n.repo.storage_mut().reader().expect("reader");
            reader.blobs().filter_map(|r| r.ok()).count()
        })
        .collect();

    WorldReport {
        ops,
        final_head,
        quiesced_at_ns,
        blob_counts,
        commits_made: nodes.iter().map(|n| n.commits_made).collect(),
    }
}

/// Sim tests share the process-global virtual clock — serialize.
static SIM_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn sim_guard() -> std::sync::MutexGuard<'static, ()> {
    match SIM_SERIAL.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[test]
fn three_node_fault_script_converges_and_replays() {
    let _serial = sim_guard();
    let first = run_world(0xDB, 3, 12);
    assert!(
        first.commits_made.iter().sum::<u32>() > 0,
        "script must have committed something: {:?}",
        first.ops
    );

    let second = run_world(0xDB, 3, 12);
    assert_eq!(
        first.ops, second.ops,
        "same seed must replay the same script"
    );
    assert_eq!(
        first, second,
        "same seed must produce a bit-identical world report"
    );
}

#[test]
fn seed_sweep_converges() {
    let _serial = sim_guard();
    // A handful of distinct seeds — each generates a different fault
    // script; all must converge with closure intact. (Convergence,
    // closure, and quiescence asserts live inside run_world.)
    for seed in [1u64, 2, 3] {
        let report = run_world(seed, 3, 10);
        assert!(
            report.quiesced_at_ns > 0,
            "seed {seed} converged (asserted in run_world)"
        );
    }
}
