//! Two-process, two-pile live sync demo over REAL iroh QUIC — the
//! v0.47.0 release-gate transport proof, stage 3.
//!
//! Run role `a` and role `b` as two separate OS processes sharing a
//! demo directory:
//!
//! ```text
//! cargo run -p triblespace-net --example two_pile_sync_demo -- a /tmp/demo &
//! cargo run -p triblespace-net --example two_pile_sync_demo -- b /tmp/demo
//! ```
//!
//! Role A creates `a.pile`, provisions the team (root key + admin caps
//! for both nodes), stores a content payload blob that is never
//! committed to any branch, commits a fact on "main", and serves.
//! Role B creates `b.pile` from the out-of-band handoff (cap blobs +
//! A's direct address), then proves the two release-gate properties in
//! order:
//!
//! 1. **EAGER**: A's HEAD gossip reaches B over the team topic; B's
//!    host walks the closure (OP_CHILDREN / OP_GET_BLOB over QUIC)
//!    and `merge_tracking_into_local` advances B's "main" to A's head
//!    commit. B prints `EAGER-OK <head>`.
//! 2. **LAZY**: B durably records a weak-pin *want* for the payload
//!    hash (pin + flush — the demand survives a crash), and a
//!    `Reconciler::tick` services it via the swarm fetch
//!    (publisher-first: A is gossip-known from stage 1). B prints
//!    `LAZY-OK <hash>` and writes the done-file; A sees it and exits.
//!
//! Transport realism: every byte moves as iroh QUIC over real UDP
//! sockets (loopback). Discovery is pinned rather than global — the
//! endpoints run relay-free with a `MemoryLookup` seeded from the
//! handoff (`PeerConfig::peers` carries ids only by design; iroh's
//! address-lookup layer owns resolution, and `MemoryLookup` is that
//! layer's direct-addressing form). Everything above the endpoint —
//! DHT node, protocol router + OP_AUTH cap verification, gossip topic,
//! host loop, Peer, Reconciler — is the unmodified production stack
//! via `transport::iroh::bind_with_endpoint`.
//!
//! Demo-only shortcut: role A generates BOTH node keys and hands B its
//! secret through the handoff file. Real deployments never move
//! secrets — B would mint its own key and request a cap
//! (`trible team request-join` / `team approve`).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use iroh_base::{EndpointAddr, EndpointId, TransportAddr};
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{BlobStoreGet, BlobStorePut, Repository, WeakPinStore};
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::reconcile::Reconciler;
use triblespace_net::tracking;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let mut args = std::env::args().skip(1);
    let role = args.next().unwrap_or_default();
    let dir = PathBuf::from(args.next().unwrap_or_default());
    if dir.as_os_str().is_empty() {
        eprintln!("usage: two_pile_sync_demo <a|b> <demo-dir>");
        std::process::exit(2);
    }
    std::fs::create_dir_all(&dir).expect("create demo dir");
    match role.as_str() {
        "a" => run_a(&dir),
        "b" => run_b(&dir),
        _ => {
            eprintln!("usage: two_pile_sync_demo <a|b> <demo-dir>");
            std::process::exit(2);
        }
    }
}

// ── Shared helpers ───────────────────────────────────────────────────

/// Sign a PERM_ADMIN root cap for `subject` — the same out-of-band
/// provisioning the integration tests and the sim suite use.
fn admin_cap(
    root: &SigningKey,
    subject: &SigningKey,
) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
    use triblespace_core::id::{ufoid, ExclusiveId};
    use triblespace_core::macros::entity;

    let scope_root = *ufoid();
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: PERM_ADMIN,
    });
    let now = triblespace_net::clock::epoch_now();
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

/// Bring one node up: bind a relay-free iroh endpoint on real UDP
/// loopback sockets (address resolution = a `MemoryLookup` seeded with
/// `known`), wire the full production transport stack over it
/// (`bind_with_endpoint`: DHT node, protocol router, gossip topic),
/// run the host loop on its own thread + runtime (exactly what
/// `host::spawn` does in production), and wrap the pile in a `Peer`.
///
/// Returns the Peer plus this endpoint's dialable direct addresses.
fn bring_up(
    signing_key: &SigningKey,
    store: Pile,
    team_root: ed25519_dalek::VerifyingKey,
    self_cap: [u8; 32],
    known: Vec<EndpointAddr>,
) -> (Peer<Pile>, EndpointAddr) {
    let secret = triblespace_net::identity::iroh_secret(signing_key);
    let id: EndpointId = secret.public().into();
    let (sender, receiver, wiring) = host::wire(id);
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<EndpointAddr>();

    let bootstrap: Vec<EndpointAddr> = known.clone();
    let _host_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async move {
            use iroh::address_lookup::{EndpointInfo, MemoryLookup};
            use iroh::endpoint::presets;

            let lookup = MemoryLookup::from_endpoint_info(
                known.iter().cloned().map(EndpointInfo::from),
            );
            let ep = iroh::Endpoint::builder(presets::N0)
                .secret_key(secret)
                .relay_mode(iroh::RelayMode::Disabled)
                .clear_address_lookup()
                .address_lookup(lookup)
                .bind()
                .await
                .expect("bind endpoint");

            // Publish our dialable loopback addresses for the handoff.
            let addrs: Vec<TransportAddr> = ep
                .bound_sockets()
                .into_iter()
                .map(|sa| {
                    let port = sa.port();
                    let ip: std::net::IpAddr = if sa.is_ipv6() {
                        std::net::Ipv6Addr::LOCALHOST.into()
                    } else {
                        std::net::Ipv4Addr::LOCALHOST.into()
                    };
                    TransportAddr::Ip(std::net::SocketAddr::new(ip, port))
                })
                .collect();
            let _ = addr_tx.send(EndpointAddr::from_parts(ep.id(), addrs));

            let config = PeerConfig {
                peers: bootstrap,
                gossip: true,
                team_root,
                self_cap,
                direction: SyncDirection::Bidirectional,
            };
            let harness =
                triblespace_net::transport::iroh::bind_with_endpoint(ep, &config).await;
            host::run_host(harness, config, wiring).await;
        });
    });

    let peer = Peer::with_wiring(
        store,
        signing_key.clone(),
        SyncDirection::Bidirectional,
        team_root,
        sender,
        receiver,
    );
    let addr = addr_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("endpoint bound within 10s");
    (peer, addr)
}

fn fresh_pile(path: &Path) -> Pile {
    let _ = std::fs::remove_file(path);
    std::fs::File::create(path).expect("create pile file");
    Pile::open(path).expect("open pile")
}

fn handoff_path(dir: &Path) -> PathBuf {
    dir.join("handoff.txt")
}

fn done_path(dir: &Path) -> PathBuf {
    dir.join("done.txt")
}

// ── Role A: provision, commit, serve ─────────────────────────────────

fn run_a(dir: &Path) {
    use rand::rngs::OsRng;

    // Team provisioning: fresh root + both node keys (see the header
    // for why A minting B's key is a demo-only shortcut).
    let root = SigningKey::generate(&mut OsRng);
    let ka = SigningKey::generate(&mut OsRng);
    let kb = SigningKey::generate(&mut OsRng);
    let team_root = root.verifying_key();
    let (cap_a, sig_a) = admin_cap(&root, &ka);
    let (cap_b, sig_b) = admin_cap(&root, &kb);
    let self_cap_a = (&sig_a).get_handle().raw;

    // Pile A: both cap chains (so OP_AUTH from B verifies locally),
    // plus the lazy payload — a blob committed to NO branch, so eager
    // sync never ships it. Only a want can move it.
    let mut pile = fresh_pile(&dir.join("a.pile"));
    for blob in [&cap_a, &sig_a, &cap_b, &sig_b] {
        pile.put::<SimpleArchive, _>(blob.clone()).expect("seed cap");
    }
    let payload: TribleSet = {
        use triblespace_core::id::{ufoid, ExclusiveId};
        use triblespace_core::macros::entity;
        let e = *ufoid();
        let tag = *ufoid();
        TribleSet::from(entity! {
            ExclusiveId::force_ref(&e) @
            triblespace_core::metadata::tag: tag,
        })
    };
    let payload_blob: Blob<SimpleArchive> = payload.to_blob();
    let payload_hash = payload_blob.get_handle().raw;
    pile.put::<SimpleArchive, _>(payload_blob).expect("seed payload");
    pile.flush().expect("flush pile a");

    let (peer, my_addr) = bring_up(&ka, pile, team_root, self_cap_a, Vec::new());
    let mut repo = Repository::new(peer, ka.clone(), TribleSet::new()).expect("repo a");

    // The eager fact: one commit on "main".
    let branch_id = repo.ensure_branch("main", None).ok().expect("branch");
    let a_head = {
        let mut ws = repo.pull(branch_id).expect("pull");
        ws.commit(TribleSet::new(), "two-pile sync demo: fact from A");
        repo.push(&mut ws).ok().expect("push");
        let ws = repo.pull(branch_id).expect("pull");
        ws.head().expect("branch has head").raw
    };

    // Handoff (write + atomic rename so B never reads a partial file).
    let addr_strs: Vec<String> = my_addr
        .addrs
        .iter()
        .filter_map(|a| match a {
            TransportAddr::Ip(sa) => Some(sa.to_string()),
            _ => None,
        })
        .collect();
    let handoff = format!(
        "team_root={}\nkb_secret={}\ncap_a={}\nsig_a={}\ncap_b={}\nsig_b={}\na_id={}\na_addrs={}\na_head={}\npayload={}\n",
        hex::encode(team_root.to_bytes()),
        hex::encode(kb.to_bytes()),
        hex::encode(&cap_a.bytes[..]),
        hex::encode(&sig_a.bytes[..]),
        hex::encode(&cap_b.bytes[..]),
        hex::encode(&sig_b.bytes[..]),
        hex::encode(my_addr.id.as_bytes()),
        addr_strs.join(","),
        hex::encode(a_head),
        hex::encode(payload_hash),
    );
    let tmp = dir.join("handoff.txt.tmp");
    std::fs::write(&tmp, handoff).expect("write handoff");
    std::fs::rename(&tmp, handoff_path(dir)).expect("publish handoff");

    println!("A: node {}", hex::encode(my_addr.id.as_bytes()));
    println!("A: head commit {}", hex::encode(a_head));
    println!("A: payload blob {} (never committed)", hex::encode(payload_hash));
    println!("A: serving; waiting for B's done-file…");

    // Serve until B reports success (or 300s).
    let started = Instant::now();
    let mut tick = 0u32;
    while started.elapsed() < Duration::from_secs(300) {
        std::thread::sleep(Duration::from_millis(100));
        repo.storage_mut().refresh();
        tick += 1;
        if tick % 20 == 0 {
            // Fast republish so a B that joins late still hears the
            // head promptly (the host's own rebroadcast tick is 30s).
            repo.storage_mut().republish_branches();
        }
        if done_path(dir).exists() {
            println!("A: B reported success; exiting.");
            return;
        }
    }
    eprintln!("A: timed out waiting for B");
    std::process::exit(1);
}

// ── Role B: converge eagerly, then want lazily ───────────────────────

fn parse_handoff(text: &str) -> std::collections::HashMap<String, String> {
    text.lines()
        .filter_map(|l| {
            let (k, v) = l.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

fn run_b(dir: &Path) {
    // Wait for A's handoff.
    let started = Instant::now();
    let handoff = loop {
        if let Ok(text) = std::fs::read_to_string(handoff_path(dir)) {
            break parse_handoff(&text);
        }
        if started.elapsed() > Duration::from_secs(120) {
            eprintln!("B: no handoff within 120s");
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let field = |k: &str| handoff.get(k).unwrap_or_else(|| panic!("handoff field {k}"));
    let hex32 = |k: &str| -> [u8; 32] {
        let v = hex::decode(field(k)).expect("hex field");
        v.as_slice().try_into().expect("32-byte field")
    };

    let kb = SigningKey::from_bytes(&hex32("kb_secret"));
    let team_root =
        ed25519_dalek::VerifyingKey::from_bytes(&hex32("team_root")).expect("team root");
    let a_head = hex32("a_head");
    let payload_hash = hex32("payload");
    let a_id = EndpointId::from_bytes(&hex32("a_id")).expect("a id");
    let a_addrs = field("a_addrs")
        .split(',')
        .filter_map(|s| s.parse::<std::net::SocketAddr>().ok())
        .map(TransportAddr::Ip);
    let a_addr = EndpointAddr::from_parts(a_id, a_addrs);

    // Pile B: seeded with the out-of-band cap chains only.
    let mut pile = fresh_pile(&dir.join("b.pile"));
    let mut self_cap_b = [0u8; 32];
    for k in ["cap_a", "sig_a", "cap_b", "sig_b"] {
        let bytes = hex::decode(field(k)).expect("cap blob hex");
        let blob: Blob<SimpleArchive> = Blob::new(anybytes::Bytes::from(bytes));
        let handle = pile
            .put::<SimpleArchive, _>(blob)
            .expect("seed cap blob");
        if k == "sig_b" {
            self_cap_b = handle.raw;
        }
    }
    pile.flush().expect("flush pile b");

    let (peer, my_addr) = bring_up(&kb, pile, team_root, self_cap_b, vec![a_addr]);
    let mut repo = Repository::new(peer, kb.clone(), TribleSet::new()).expect("repo b");
    println!("B: node {}", hex::encode(my_addr.id.as_bytes()));

    // ── Stage 1: eager gossip convergence ────────────────────────────
    let started = Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(120) {
            eprintln!("B: EAGER FAILED — main did not reach A's head within 120s");
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(100));
        repo.storage_mut().refresh();
        for info in tracking::list_tracking_pins(repo.storage_mut()) {
            let _ =
                tracking::merge_tracking_into_local(&mut repo, info.local_id, &info.remote_name);
        }
        let b_head = repo
            .lookup_branch("main")
            .ok()
            .flatten()
            .and_then(|id| repo.pull(id).ok())
            .and_then(|ws| ws.head());
        if b_head.map(|h| h.raw == a_head).unwrap_or(false) {
            break;
        }
    }
    println!("EAGER-OK {}", hex::encode(a_head));

    // ── Stage 2: lazy weak-pin want ──────────────────────────────────
    {
        let peer = repo.storage_mut();
        let reader = peer.reader().expect("reader");
        let held: Result<anybytes::Bytes, _> =
            BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(payload_hash));
        assert!(
            held.is_err(),
            "precondition: B must NOT hold the never-committed payload after eager sync"
        );
        // The durable want: weak-pin + flush BEFORE any fetch.
        let mut store = peer.store();
        store
            .pin_weak(Inline::<Handle<UnknownBlob>>::new(payload_hash))
            .expect("record want");
        store.flush().expect("flush want");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("reconcile runtime");
    let mut reconciler = Reconciler::with_backoff(Duration::from_millis(500), Duration::from_secs(5))
        .with_fetch_budget(Duration::from_secs(15));
    let started = Instant::now();
    loop {
        if started.elapsed() > Duration::from_secs(120) {
            eprintln!("B: LAZY FAILED — want not serviced within 120s");
            std::process::exit(1);
        }
        let stats = rt.block_on(reconciler.tick(repo.storage_mut()));
        if stats.fetched >= 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    // Verify the landed bytes and the still-recorded want.
    {
        let peer = repo.storage_mut();
        let reader = peer.reader().expect("reader");
        let got: anybytes::Bytes =
            BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(payload_hash))
                .expect("payload landed in pile B");
        assert_eq!(blake3::hash(&got).as_bytes(), &payload_hash);
        let still_pinned = peer
            .store()
            .weak_pins()
            .expect("weak pins")
            .filter_map(Result::ok)
            .any(|h| h.raw == payload_hash);
        assert!(still_pinned, "weak pin stays on record as retention marker");
    }
    println!("LAZY-OK {}", hex::encode(payload_hash));

    std::fs::write(done_path(dir), "ok\n").expect("write done file");
    println!("B: done.");
}
