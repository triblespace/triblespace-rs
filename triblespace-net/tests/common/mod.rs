//! Shared scaffolding for the deterministic-simulation integration
//! tests. Included via `mod common;` in each `sim_*.rs` test binary.
//! (Cargo treats `tests/common/mod.rs` as a submodule, not its own
//! test binary.)
#![allow(dead_code)]
#![cfg(feature = "sim")]

use std::sync::{Arc, OnceLock};

use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::Blob;
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::id::rngid::seed_ids;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::BlobStorePut;
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
use triblespace_net::transport::sim::SimNet;

/// One virtual clock per test process (install_virtual is
/// once-per-process; each test binary is its own process).
pub fn vclock() -> Arc<VirtualClock> {
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

/// Sim tests share the process-global virtual clock — serialize them.
pub fn sim_guard() -> std::sync::MutexGuard<'static, ()> {
    static SIM_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    match SIM_SERIAL.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub fn key(n: u8) -> SigningKey {
    SigningKey::from_bytes(&[n; 32])
}

pub fn pk(k: &SigningKey) -> [u8; 32] {
    k.verifying_key().to_bytes()
}

/// Sign a PERM_ADMIN root cap for `subject` and return (cap, sig)
/// blobs — the out-of-band provisioning a team admin would do.
pub fn admin_cap(
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

/// A paused, single-thread tokio runtime + LocalSet runner — the
/// deterministic-sim execution context. `body` is the async test.
pub fn run_paused<F, T>(seed: u64, body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
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
    rt.block_on(local.run_until(body))
}

/// Bring one node up on `net`: join the sim mesh, wire the host loop
/// as a local task, return the `Peer<MemoryRepo>`. `store` is the
/// node's pre-seeded local store (caps already inserted). `gossip`
/// controls team-topic participation.
pub fn bring_up(
    net: &SimNet,
    signing_key: &SigningKey,
    store: MemoryRepo,
    team_root: ed25519_dalek::VerifyingKey,
    self_cap: [u8; 32],
    gossip: bool,
) -> Peer<MemoryRepo> {
    let id = pk(signing_key);
    let harness = net.join(id, gossip);
    let (sender, receiver, wiring) =
        host::wire(EndpointId::from_bytes(&id).expect("endpoint id"));
    tokio::task::spawn_local(host::run_host(
        harness,
        PeerConfig {
            peers: Vec::new(),
            gossip,
            team_root,
            self_cap,
            direction: SyncDirection::Bidirectional,
        },
        wiring,
    ));
    Peer::with_wiring(
        store,
        signing_key.clone(),
        SyncDirection::Bidirectional,
        team_root,
        sender,
        receiver,
    )
}

/// Like [`bring_up`] but with a bounded [`BoundedBlobStore`] cache tier
/// of `capacity` blobs — a lazy node that caches swarm-fetched content
/// instead of eagerly replicating it.
pub fn bring_up_cached(
    net: &SimNet,
    signing_key: &SigningKey,
    store: MemoryRepo,
    cache_capacity: usize,
    team_root: ed25519_dalek::VerifyingKey,
    self_cap: [u8; 32],
    gossip: bool,
) -> Peer<MemoryRepo, triblespace_net::cache::BoundedBlobStore> {
    let id = pk(signing_key);
    let harness = net.join(id, gossip);
    let (sender, receiver, wiring) =
        host::wire(EndpointId::from_bytes(&id).expect("endpoint id"));
    tokio::task::spawn_local(host::run_host(
        harness,
        PeerConfig {
            peers: Vec::new(),
            gossip,
            team_root,
            self_cap,
            direction: SyncDirection::Bidirectional,
        },
        wiring,
    ));
    Peer::with_wiring_and_cache(
        store,
        triblespace_net::cache::BoundedBlobStore::new(cache_capacity),
        signing_key.clone(),
        SyncDirection::Bidirectional,
        team_root,
        sender,
        receiver,
    )
}

/// A MemoryRepo seeded with the given cap+sig blobs (so OP_AUTH
/// verifies locally without a chain swarm-fetch).
pub fn store_with_caps(caps: &[(Blob<SimpleArchive>, Blob<SimpleArchive>)]) -> MemoryRepo {
    let mut store = MemoryRepo::default();
    for (cap, sig) in caps {
        store.put::<SimpleArchive, _>(cap.clone()).unwrap();
        store.put::<SimpleArchive, _>(sig.clone()).unwrap();
    }
    store
}

/// Convenience: the `self_cap` handle (sig blob hash) for a (cap, sig)
/// pair.
pub fn self_cap_of(sig: &Blob<SimpleArchive>) -> [u8; 32] {
    sig.get_handle().raw
}
