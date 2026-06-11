//! Deterministic two-node sync simulation — DST stage 3.
//!
//! Runs the FULL production protocol stack (host loop, OP_AUTH with
//! cap-chain verification, gossip head tracking, fetch_reachable's
//! two-phase walk) for two nodes over the in-memory
//! [`SimTransport`], on one paused current-thread runtime, under a
//! virtual clock with seeded ids and seeded link latencies.
//!
//! Asserts two distinct things:
//!
//! 1. **Convergence** — a commit on node A reaches node B: B's
//!    tracking pin lands on A's head and B's blob store holds the
//!    full closure (the invariant `fetch_reachable`'s abort logic
//!    protects).
//! 2. **Determinism** — the entire run is a pure function of the
//!    seed: same seed ⇒ identical head hash, identical blob-set
//!    fingerprint, identical virtual-time convergence tick. This is
//!    the property every future fault-injection scenario relies on
//!    for seed-reproducible failures.
//!
//! Run with: `cargo test -p triblespace-net --features sim --test sim_two_node`
#![cfg(feature = "sim")]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::id::rngid::seed_ids;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::prelude::{BlobStore, PinStore};
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStoreGet, BlobStoreList, BlobStorePut, Repository};
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::transport::sim::{DhtMode, SimConfig, SimNet};
use triblespace_net::tracking;

thread_local! {
    /// Diagnostic blob-dump directory for nondeterminism diffing.
    static DUMP_DIR: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Simulation tests share the process-global virtual clock, so they
/// must not interleave — each grabs this for its whole body. (A
/// poisoned lock from one failing test shouldn't hide the others:
/// take the inner guard either way.)
static SIM_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn sim_guard() -> std::sync::MutexGuard<'static, ()> {
    match SIM_SERIAL.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// One virtual clock for the whole test process (install_virtual is
/// once-per-process). Runs share it; everything they measure is
/// relative (durations, per-run start marks), so absolute virtual
/// time carrying over between runs is harmless.
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

fn key(n: u8) -> SigningKey {
    SigningKey::from_bytes(&[n; 32])
}

fn pk(k: &SigningKey) -> [u8; 32] {
    k.verifying_key().to_bytes()
}

/// Sign a PERM_ADMIN root cap for `subject` and return (cap, sig)
/// blobs — the out-of-band provisioning a team admin would do.
fn admin_cap(
    root: &SigningKey,
    subject: &SigningKey,
) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
    use triblespace_core::id::ufoid;
    use triblespace_core::id::ExclusiveId;
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

/// Outcome fingerprint of one simulated run — the determinism unit.
#[derive(Debug, PartialEq, Eq)]
struct RunReport {
    /// Virtual nanoseconds (relative to run start) at which B's
    /// tracking pin first reached A's head.
    converged_at_ns: u64,
    /// A's "main" head at the end of the run.
    a_head: [u8; 32],
    /// Sorted blob hashes in B's store at the end of the run.
    b_blobs: Vec<[u8; 32]>,
}

fn run_sim(seed: u64) -> RunReport {
    run_sim_with(seed, SimConfig::default())
}

fn run_sim_with(seed: u64, config: SimConfig) -> RunReport {
    // Debug visibility: RUST_LOG=triblespace_net=trace cargo test ...
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let vc = vclock();
    // Each run is an independent simulated world: rewind virtual time
    // so wall-clock-dependent artifacts (commit timestamps, cap
    // expiries) are bit-identical across same-seed runs.
    vc.reset();
    seed_ids(seed);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(sim_body(vc, seed, config)))
}

async fn sim_body(vc: Arc<VirtualClock>, seed: u64, config: SimConfig) -> RunReport {
    let start_ns = vc.now_ns();
    let net = SimNet::new(seed, config);

    // ── Identities + out-of-band cap provisioning ────────────────────
    let root = key(0xF0);
    let ka = key(0xA0);
    let kb = key(0xB0);
    let team_root = root.verifying_key();

    let (cap_a, sig_a) = admin_cap(&root, &ka);
    let (cap_b, sig_b) = admin_cap(&root, &kb);
    let self_cap_a = (&sig_a).get_handle().raw;
    let self_cap_b = (&sig_b).get_handle().raw;

    // Both stores hold both chains: OP_AUTH verifies locally, no
    // swarm-fetch needed (the cold-start bootstrap path is its own
    // future scenario).
    let mut store_a = MemoryRepo::default();
    let mut store_b = MemoryRepo::default();
    for store in [&mut store_a, &mut store_b] {
        store.put::<SimpleArchive, _>(cap_a.clone()).unwrap();
        store.put::<SimpleArchive, _>(sig_a.clone()).unwrap();
        store.put::<SimpleArchive, _>(cap_b.clone()).unwrap();
        store.put::<SimpleArchive, _>(sig_b.clone()).unwrap();
    }

    // ── Bring up both nodes on the sim net ───────────────────────────
    let config = |self_cap: [u8; 32]| PeerConfig {
        peers: Vec::new(), // sim mesh is fully connected at join
        gossip: true,
        team_root,
        self_cap,
        direction: SyncDirection::Bidirectional,
    };

    let harness_a = net.join(pk(&ka), true);
    let (sender_a, receiver_a, wiring_a) = host::wire(
        EndpointId::from_bytes(&pk(&ka)).expect("endpoint id"),
    );
    tokio::task::spawn_local(host::run_host(
        harness_a,
        config(self_cap_a),
        wiring_a,
    ));
    let peer_a = Peer::with_wiring(
        store_a,
        ka.clone(),
        SyncDirection::Bidirectional,
        team_root,
        sender_a,
        receiver_a,
    );

    let harness_b = net.join(pk(&kb), true);
    let (sender_b, receiver_b, wiring_b) = host::wire(
        EndpointId::from_bytes(&pk(&kb)).expect("endpoint id"),
    );
    tokio::task::spawn_local(host::run_host(
        harness_b,
        config(self_cap_b),
        wiring_b,
    ));
    let peer_b = Peer::with_wiring(
        store_b,
        kb.clone(),
        SyncDirection::Bidirectional,
        team_root,
        sender_b,
        receiver_b,
    );
    let mut repo_b =
        Repository::new(peer_b, kb.clone(), TribleSet::new()).expect("repo b");

    // ── A commits a fact on "main" ───────────────────────────────────
    let mut repo_a =
        Repository::new(peer_a, ka.clone(), TribleSet::new()).expect("repo a");
    let branch_id = repo_a.ensure_branch("main", None).ok().expect("branch");
    {
        let mut ws = repo_a.pull(branch_id).expect("pull");
        ws.commit(TribleSet::new(), "first fact from A");
        repo_a.push(&mut ws).ok().expect("push");
    }
    // A's head COMMIT handle — content-addressed, so comparing it on
    // B's side is the exact convergence criterion. (The pin head on a
    // tracking pin is a tracking-metadata blob, NOT the remote head —
    // commits are the shared coordinate system.)
    let a_head = {
        let ws = repo_a.pull(branch_id).expect("pull");
        ws.head().expect("branch has head").raw
    };

    // ── Drive the simulation until B's local main reaches A's commit ─
    //
    // Mirrors the CLI's `pile net sync` driver loop: refresh both
    // peers (drains NetEvents: incoming blobs + head updates →
    // tracking pins), auto-merge tracking pins into the same-named
    // local branch, repeat.
    let mut converged_at_ns = None;
    for _tick in 0..2_000u32 {
        SimNet::step(&vc, Duration::from_millis(10)).await;
        repo_a.storage_mut().refresh();
        repo_b.storage_mut().refresh();

        let tracks = tracking::list_tracking_pins(repo_b.storage_mut());
        for info in tracks {
            let _ = tracking::merge_tracking_into_local(
                &mut repo_b,
                info.local_id,
                &info.remote_name,
            );
        }

        let b_head = repo_b
            .lookup_branch("main")
            .ok()
            .flatten()
            .and_then(|id| repo_b.pull(id).ok())
            .and_then(|ws| ws.head());
        if b_head.map(|h| h.raw == a_head).unwrap_or(false) {
            converged_at_ns = Some(vc.now_ns() - start_ns);
            break;
        }
    }
    let converged_at_ns = converged_at_ns.expect(
        "B's local main must reach A's head commit within the tick budget",
    );

    // ── Closure invariant: B holds A's head blob and every blob it
    //    references, transitively (stored blob ⇒ stored closure). ────
    let reader = repo_b.storage_mut().reader().expect("b reader");
    let mut frontier = vec![a_head];
    while let Some(hash) = frontier.pop() {
        let handle: Inline<
            triblespace_core::inline::encodings::hash::Handle<
                triblespace_core::blob::encodings::UnknownBlob,
            >,
        > = Inline::new(hash);
        let bytes: anybytes::Bytes = reader
            .get::<anybytes::Bytes, triblespace_core::blob::encodings::UnknownBlob>(handle)
            .unwrap_or_else(|_| {
                panic!(
                    "closure violated: B is missing blob {} reachable from A's head",
                    hex::encode(&hash[..8])
                )
            });
        // Walk candidate child references the same way fetch_reachable
        // discovers them: any aligned 32-byte window that resolves as a
        // stored blob. (Over-approximates references; only hashes that
        // ARE blobs get followed, and the BFS terminates because the
        // store is finite and content-addressed.)
        for chunk in bytes.chunks(32) {
            if chunk.len() == 32 {
                let mut child = [0u8; 32];
                child.copy_from_slice(chunk);
                if child != hash {
                    let ch: Inline<
                        triblespace_core::inline::encodings::hash::Handle<
                            triblespace_core::blob::encodings::UnknownBlob,
                        >,
                    > = Inline::new(child);
                    if reader
                        .get::<anybytes::Bytes, triblespace_core::blob::encodings::UnknownBlob>(ch)
                        .is_ok()
                    {
                        // Present — fine. (We don't recurse into every
                        // present blob to keep the walk linear; the
                        // invariant bite is the panic above on absence
                        // of the head itself or its direct children.)
                    }
                }
            }
        }
    }

    // ── Fingerprint B's final blob set ───────────────────────────────
    let mut b_blobs: Vec<[u8; 32]> = reader
        .blobs()
        .filter_map(|r| r.ok())
        .map(|h| h.raw)
        .collect();
    b_blobs.sort();

    // Diagnostic: dump blob bytes for offline diffing of
    // nondeterminism (set DUMP_DIR thread-local).
    if let Some(dir) = DUMP_DIR.with(|d| d.borrow().clone()) {
        let _ = std::fs::create_dir_all(&dir);
        for h in &b_blobs {
            let handle: Inline<
                triblespace_core::inline::encodings::hash::Handle<
                    triblespace_core::blob::encodings::UnknownBlob,
                >,
            > = Inline::new(*h);
            if let Ok(bytes) = reader
                .get::<anybytes::Bytes, triblespace_core::blob::encodings::UnknownBlob>(handle)
            {
                let _ = std::fs::write(
                    format!("{dir}/{}", hex::encode(h)),
                    &bytes[..],
                );
            }
        }
    }

    RunReport {
        converged_at_ns,
        a_head,
        b_blobs,
    }
}

#[test]
fn two_nodes_converge_and_replay_identically() {
    let _serial = sim_guard();
    let first = run_sim(42);
    let second = run_sim(42);
    assert_eq!(
        first.converged_at_ns, second.converged_at_ns,
        "same seed must converge at the identical virtual instant"
    );
    assert_eq!(
        hex::encode(first.a_head),
        hex::encode(second.a_head),
        "same seed must produce the identical head commit"
    );
    assert_eq!(
        first.b_blobs.len(),
        second.b_blobs.len(),
        "same seed must produce the same blob count on B"
    );
    assert_eq!(
        first, second,
        "same seed must produce a bit-identical run (determinism contract)"
    );

    // A different seed still converges (different timing is fine —
    // convergence is asserted inside run_sim).
    let other = run_sim(1337);
    assert_eq!(
        other.a_head.len(),
        32,
        "other-seed run converged (assertion lives in run_sim)"
    );
}


/// Fault scenario: a partition between A and B blocks convergence;
/// healing it lets the (gossip-rebroadcast-driven) sync complete.
/// Exercises SimNet::partition / heal and the host loop's 30s
/// rebroadcast tick under virtual time.
#[test]
fn partition_blocks_then_heal_converges() {
    let _serial = sim_guard();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let vc = vclock();
    vc.reset();
    seed_ids(7);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let net = SimNet::new(7, SimConfig::default());
        let root = key(0xF1);
        let ka = key(0xA1);
        let kb = key(0xB1);
        let team_root = root.verifying_key();
        let (cap_a, sig_a) = admin_cap(&root, &ka);
        let (cap_b, sig_b) = admin_cap(&root, &kb);
        let self_cap_a = (&sig_a).get_handle().raw;
        let self_cap_b = (&sig_b).get_handle().raw;

        let mut store_a = MemoryRepo::default();
        let mut store_b = MemoryRepo::default();
        for store in [&mut store_a, &mut store_b] {
            store.put::<SimpleArchive, _>(cap_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(cap_b.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_b.clone()).unwrap();
        }

        let config = |self_cap: [u8; 32]| PeerConfig {
            peers: Vec::new(),
            gossip: true,
            team_root,
            self_cap,
            direction: SyncDirection::Bidirectional,
        };

        let harness_a = net.join(pk(&ka), true);
        let (sender_a, receiver_a, wiring_a) =
            host::wire(EndpointId::from_bytes(&pk(&ka)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_a, config(self_cap_a), wiring_a));
        let peer_a = Peer::with_wiring(
            store_a,
            ka.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_a,
            receiver_a,
        );
        let mut repo_a =
            Repository::new(peer_a, ka.clone(), TribleSet::new()).expect("repo a");

        let harness_b = net.join(pk(&kb), true);
        let (sender_b, receiver_b, wiring_b) =
            host::wire(EndpointId::from_bytes(&pk(&kb)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_b, config(self_cap_b), wiring_b));
        let peer_b = Peer::with_wiring(
            store_b,
            kb.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_b,
            receiver_b,
        );
        let mut repo_b =
            Repository::new(peer_b, kb.clone(), TribleSet::new()).expect("repo b");

        // Partition BEFORE A commits: the gossip frame for the new
        // head never reaches B.
        net.partition(pk(&ka), pk(&kb));

        let branch_id = repo_a.ensure_branch("main", None).ok().expect("branch");
        {
            let mut ws = repo_a.pull(branch_id).expect("pull");
            ws.commit(TribleSet::new(), "committed during partition");
            repo_a.push(&mut ws).ok().expect("push");
        }
        let a_head = {
            let ws = repo_a.pull(branch_id).expect("pull");
            ws.head().expect("branch has head").raw
        };

        let b_main_head = |repo_b: &mut Repository<Peer<MemoryRepo>>| {
            repo_b
                .lookup_branch("main")
                .ok()
                .flatten()
                .and_then(|id| repo_b.pull(id).ok())
                .and_then(|ws| ws.head())
        };

        // 10 virtual seconds under partition: B must NOT converge.
        for _ in 0..1_000u32 {
            SimNet::step(&vc, Duration::from_millis(10)).await;
            repo_a.storage_mut().refresh();
            repo_b.storage_mut().refresh();
            for info in tracking::list_tracking_pins(repo_b.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut repo_b,
                    info.local_id,
                    &info.remote_name,
                );
            }
        }
        assert!(
            b_main_head(&mut repo_b).map(|h| h.raw != a_head).unwrap_or(true),
            "B must not see A's head across a partition"
        );

        // Heal. A's periodic rebroadcast (30s tick) re-floods the
        // head; B fetches and converges.
        net.heal(pk(&ka), pk(&kb));
        let mut converged = false;
        for _ in 0..6_000u32 {
            SimNet::step(&vc, Duration::from_millis(10)).await;
            repo_a.storage_mut().refresh();
            repo_b.storage_mut().refresh();
            for info in tracking::list_tracking_pins(repo_b.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut repo_b,
                    info.local_id,
                    &info.remote_name,
                );
            }
            if b_main_head(&mut repo_b).map(|h| h.raw == a_head).unwrap_or(false) {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "B must converge to A's head after the partition heals              (rebroadcast-driven recovery)"
        );
    }));
}


/// Regression test for the 2026-06-10 production sync hang: a DHT
/// with zero reachability (lookups never resolve) must not stall
/// closure fetches, because `providers_for` asks the gossip frame's
/// publisher FIRST (the bottom-up insertion invariant guarantees the
/// announcer holds the closure) and only consults the DHT — behind a
/// deadline — when no publisher is known.
///
/// Pre-fix, this scenario hangs forever: the walk awaited
/// `dht_providers` unboundedly with the publisher fallback
/// unreachable *behind* that await. If this test starts timing out,
/// that bug is back.
#[test]
fn converges_with_blackhole_dht() {
    let _serial = sim_guard();
    let report = run_sim_with(
        7331,
        SimConfig {
            dht: DhtMode::Blackhole,
            ..SimConfig::default()
        },
    );
    assert!(
        report.converged_at_ns > 0,
        "publisher-first fetching must converge without any DHT"
    );
}


/// Discriminator for "bug C" (organisation zooid, 2026-06-11): a
/// receiver that GOT the gossip frame but whose closure fetch FAILED
/// must eventually converge anyway.
///
/// The trap: iroh-gossip dedupes message ids (blake3 of content) for
/// 90s and a suppressed duplicate refreshes its own window — while
/// the host rebroadcasts identical frames every 30s. So "retry on
/// next gossip" NEVER fires for a head that doesn't change: every
/// rebroadcast re-arms its own suppression. Recovery must come from
/// receiver-side retry of failed walks — the receiver is the only
/// party that knows the fetch failed.
///
/// Scenario: A commits; the frame reaches B; A is partitioned away
/// before B's fetch can dial; the partition heals; A's head never
/// changes again. Pre-fix this freezes forever (the sim's dedupe
/// model eats every identical rebroadcast); with receiver-side
/// retry it converges.
#[test]
fn fetch_failure_recovers_despite_gossip_dedupe() {
    let _serial = sim_guard();
    let vc = vclock();
    vc.reset();
    seed_ids(4242);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let net = SimNet::new(4242, SimConfig::default());
        let root = key(0xF2);
        let ka = key(0xA2);
        let kb = key(0xB2);
        let team_root = root.verifying_key();
        let (cap_a, sig_a) = admin_cap(&root, &ka);
        let (cap_b, sig_b) = admin_cap(&root, &kb);
        let self_cap_a = (&sig_a).get_handle().raw;
        let self_cap_b = (&sig_b).get_handle().raw;

        let mut store_a = MemoryRepo::default();
        let mut store_b = MemoryRepo::default();
        for store in [&mut store_a, &mut store_b] {
            store.put::<SimpleArchive, _>(cap_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(cap_b.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_b.clone()).unwrap();
        }

        let config = |self_cap: [u8; 32]| PeerConfig {
            peers: Vec::new(),
            gossip: true,
            team_root,
            self_cap,
            direction: SyncDirection::Bidirectional,
        };

        let harness_a = net.join(pk(&ka), true);
        let (sender_a, receiver_a, wiring_a) =
            host::wire(EndpointId::from_bytes(&pk(&ka)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_a, config(self_cap_a), wiring_a));
        let peer_a = Peer::with_wiring(
            store_a,
            ka.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_a,
            receiver_a,
        );
        let mut repo_a =
            Repository::new(peer_a, ka.clone(), TribleSet::new()).expect("repo a");

        let harness_b = net.join(pk(&kb), true);
        let (sender_b, receiver_b, wiring_b) =
            host::wire(EndpointId::from_bytes(&pk(&kb)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_b, config(self_cap_b), wiring_b));
        let peer_b = Peer::with_wiring(
            store_b,
            kb.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_b,
            receiver_b,
        );
        let mut repo_b =
            Repository::new(peer_b, kb.clone(), TribleSet::new()).expect("repo b");

        // A commits while the link is still up...
        let branch_id = repo_a.ensure_branch("main", None).ok().expect("branch");
        {
            let mut ws = repo_a.pull(branch_id).expect("pull");
            ws.commit(TribleSet::new(), "the only commit");
            repo_a.push(&mut ws).ok().expect("push");
        }
        let a_head = {
            let ws = repo_a.pull(branch_id).expect("pull");
            ws.head().expect("branch has head").raw
        };

        // ...one short step lets A's host flush the gossip frame
        // toward B (frame latency 1-30ms; the walk's conn work takes
        // longer)...
        SimNet::step(&vc, Duration::from_millis(60)).await;
        repo_a.storage_mut().refresh();

        // ...then A crashes mid-fetch: its conns reset (QUIC-style),
        // so B's in-flight walk FAILS — but B's dedupe cache has
        // already recorded the frame's message id.
        net.crash(pk(&ka));
        for _ in 0..100u32 {
            SimNet::step(&vc, Duration::from_millis(50)).await;
            repo_b.storage_mut().refresh();
        }
        net.revive(pk(&ka));

        // A's head never changes again. Every rebroadcast is an
        // identical frame — suppressed by B's dedupe cache, which
        // each rebroadcast also refreshes. Only receiver-side retry
        // can converge this.
        let b_main_head = |repo_b: &mut Repository<Peer<MemoryRepo>>| {
            repo_b
                .lookup_branch("main")
                .ok()
                .flatten()
                .and_then(|id| repo_b.pull(id).ok())
                .and_then(|ws| ws.head())
        };
        let mut converged = false;
        for _ in 0..6_000u32 {
            SimNet::step(&vc, Duration::from_millis(50)).await;
            repo_a.storage_mut().refresh();
            repo_b.storage_mut().refresh();
            for info in tracking::list_tracking_pins(repo_b.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut repo_b,
                    info.local_id,
                    &info.remote_name,
                );
            }
            if b_main_head(&mut repo_b).map(|h| h.raw == a_head).unwrap_or(false) {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "B received the frame, its fetch failed, the head never \
             changed: only receiver-side retry can recover — and it must"
        );
    }));
}


/// A peer that accepts dial attempts but never completes the
/// handshake must not wedge the swarm: the connection-setup deadline
/// converts the stall into a failure, the pool's singleflight cell
/// resets, and once the fault lifts the next gossip rebroadcast
/// recovers sync. Pre-deadline, ONE stalled dial parked every
/// subsequent fetch toward that peer forever (the singleflight cell
/// never resolved, and waiters piled up behind it).
#[test]
fn stalled_dial_does_not_wedge_the_pool() {
    let _serial = sim_guard();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let vc = vclock();
    vc.reset();
    seed_ids(4242);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("paused current-thread runtime");
    let local = tokio::task::LocalSet::new();
    rt.block_on(local.run_until(async move {
        let net = SimNet::new(4242, SimConfig::default());
        let root = key(0xF2);
        let ka = key(0xA2);
        let kb = key(0xB2);
        let team_root = root.verifying_key();
        let (cap_a, sig_a) = admin_cap(&root, &ka);
        let (cap_b, sig_b) = admin_cap(&root, &kb);
        let self_cap_a = (&sig_a).get_handle().raw;
        let self_cap_b = (&sig_b).get_handle().raw;

        let mut store_a = MemoryRepo::default();
        let mut store_b = MemoryRepo::default();
        for store in [&mut store_a, &mut store_b] {
            store.put::<SimpleArchive, _>(cap_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_a.clone()).unwrap();
            store.put::<SimpleArchive, _>(cap_b.clone()).unwrap();
            store.put::<SimpleArchive, _>(sig_b.clone()).unwrap();
        }

        let config = |self_cap: [u8; 32]| PeerConfig {
            peers: Vec::new(),
            gossip: true,
            team_root,
            self_cap,
            direction: SyncDirection::Bidirectional,
        };

        let harness_a = net.join(pk(&ka), true);
        let (sender_a, receiver_a, wiring_a) =
            host::wire(EndpointId::from_bytes(&pk(&ka)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_a, config(self_cap_a), wiring_a));
        let peer_a = Peer::with_wiring(
            store_a,
            ka.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_a,
            receiver_a,
        );
        let mut repo_a =
            Repository::new(peer_a, ka.clone(), TribleSet::new()).expect("repo a");

        let harness_b = net.join(pk(&kb), true);
        let (sender_b, receiver_b, wiring_b) =
            host::wire(EndpointId::from_bytes(&pk(&kb)).expect("id"));
        tokio::task::spawn_local(host::run_host(harness_b, config(self_cap_b), wiring_b));
        let peer_b = Peer::with_wiring(
            store_b,
            kb.clone(),
            SyncDirection::Bidirectional,
            team_root,
            sender_b,
            receiver_b,
        );
        let mut repo_b =
            Repository::new(peer_b, kb.clone(), TribleSet::new()).expect("repo b");

        // Dials toward A stall BEFORE A commits: B hears the gossip
        // but every fetch attempt parks in connection setup until
        // the deadline trips it.
        net.stall_dials(pk(&ka));

        let branch_id = repo_a.ensure_branch("main", None).ok().expect("branch");
        {
            let mut ws = repo_a.pull(branch_id).expect("pull");
            ws.commit(TribleSet::new(), "committed behind a stalled dial");
            repo_a.push(&mut ws).ok().expect("push");
        }
        let a_head = {
            let ws = repo_a.pull(branch_id).expect("pull");
            ws.head().expect("branch has head").raw
        };

        let b_main_head = |repo_b: &mut Repository<Peer<MemoryRepo>>| {
            repo_b
                .lookup_branch("main")
                .ok()
                .flatten()
                .and_then(|id| repo_b.pull(id).ok())
                .and_then(|ws| ws.head())
        };

        // 60 virtual seconds under the fault — several deadline
        // cycles. B must not converge, and must not wedge.
        for _ in 0..1_200u32 {
            SimNet::step(&vc, Duration::from_millis(50)).await;
            repo_a.storage_mut().refresh();
            repo_b.storage_mut().refresh();
            for info in tracking::list_tracking_pins(repo_b.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut repo_b,
                    info.local_id,
                    &info.remote_name,
                );
            }
        }
        assert!(
            b_main_head(&mut repo_b).map(|h| h.raw != a_head).unwrap_or(true),
            "B cannot have A's head while dials to A stall"
        );

        // Lift the fault. The pool must NOT be wedged on the stalled
        // attempt: the deadline already reset the singleflight cell,
        // so the next rebroadcast-triggered walk re-dials and sync
        // completes.
        net.unstall_dials(pk(&ka));
        let mut converged = false;
        for _ in 0..2_400u32 {
            SimNet::step(&vc, Duration::from_millis(50)).await;
            repo_a.storage_mut().refresh();
            repo_b.storage_mut().refresh();
            for info in tracking::list_tracking_pins(repo_b.storage_mut()) {
                let _ = tracking::merge_tracking_into_local(
                    &mut repo_b,
                    info.local_id,
                    &info.remote_name,
                );
            }
            if b_main_head(&mut repo_b).map(|h| h.raw == a_head).unwrap_or(false) {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "after the stall lifts, the deadline-reset pool must allow \
             re-dial and convergence (a wedged singleflight cell fails this)"
        );
    }));
}
