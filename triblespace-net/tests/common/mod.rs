//! Shared scaffolding for the deterministic-simulation integration
//! tests. Included via `mod common;` in each `sim_*.rs` test binary.
//! (Cargo treats `tests/common/mod.rs` as a submodule, not its own
//! test binary.)
#![allow(dead_code)]
#![cfg(feature = "sim")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::id::rngid::seed_ids;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::repo::BlobStorePut;
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::trible::TribleSet;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig};
use triblespace_net::recipient_ledger::{RecipientWriteOutcome, accept_credential, declare_intent};
use triblespace_net::transport::sim::SimNet;

#[derive(Clone)]
struct AdminCredentialFixture {
    cap: Blob<SimpleArchive>,
    sig: Blob<SimpleArchive>,
    proof_caps: Vec<Blob<SimpleArchive>>,
}

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

fn admin_credential_registry() -> &'static Mutex<HashMap<[u8; 32], AdminCredentialFixture>> {
    static REGISTRY: OnceLock<Mutex<HashMap<[u8; 32], AdminCredentialFixture>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build a finite PERM_ADMIN credential for `subject` below its explicit
/// root-signed founder anchor. The returned pair is the operational leaf used
/// by existing scenarios; [`store_with_caps`] transparently seeds the anchor
/// cap registered for that leaf so verification sees the complete proof.
pub fn admin_cap(
    root: &SigningKey,
    subject: &SigningKey,
) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::id::ufoid;
    use triblespace_core::macros::entity;

    let anchor_scope = *ufoid();
    let anchor_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&anchor_scope) @
        triblespace_core::metadata::tag: PERM_ADMIN,
    });
    let anchor =
        capability::build_founder_anchor(root, subject.verifying_key(), anchor_scope, anchor_facts)
            .expect("build founder anchor");
    let scope_root = *ufoid();
    let scope_facts = TribleSet::from(entity! {
        ExclusiveId::force_ref(&scope_root) @
        triblespace_core::metadata::tag: PERM_ADMIN,
    });
    let now = clock::epoch_now();
    let expiry: Inline<NsTAIInterval> = (now, now + hifitime::Duration::from_days(30.0))
        .try_to_inline()
        .expect("interval");
    let credential = capability::build_capability(
        subject,
        subject.verifying_key(),
        anchor.clone(),
        scope_root,
        scope_facts,
        expiry,
    )
    .expect("build finite founder credential");
    admin_credential_registry().lock().unwrap().insert(
        credential.1.get_handle().raw,
        AdminCredentialFixture {
            cap: credential.0.clone(),
            sig: credential.1.clone(),
            proof_caps: vec![anchor.0.clone()],
        },
    );
    credential
}

/// Publish one fixture credential as this node's durable recipient decision.
/// The cap itself is always included in the persisted verification closure;
/// `proof_caps` supplies its remaining parent capability blobs.
pub fn seed_recipient_credential<I>(
    store: &mut MemoryRepo,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    credential: &(Blob<SimpleArchive>, Blob<SimpleArchive>),
    proof_caps: I,
) where
    I: IntoIterator<Item = Blob<SimpleArchive>>,
{
    let declared = declare_intent(store, signing_key, team_root, credential.0.clone())
        .expect("publish fixture recipient intent");
    assert!(
        matches!(declared, RecipientWriteOutcome::Published(_)),
        "fixture recipient intent must be accepted, got {declared:?}"
    );

    let closure = std::iter::once(credential.0.clone()).chain(proof_caps);
    let accepted = accept_credential(
        store,
        signing_key,
        team_root,
        credential.1.clone(),
        closure,
        clock::epoch_now(),
    )
    .expect("publish fixture recipient credential");
    assert!(
        matches!(accepted, RecipientWriteOutcome::Published(_)),
        "fixture recipient credential must be accepted, got {accepted:?}"
    );
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
/// node's pre-seeded local store (caps already inserted).
pub fn bring_up(
    net: &SimNet,
    signing_key: &SigningKey,
    mut store: MemoryRepo,
    team_root: ed25519_dalek::VerifyingKey,
    credential_sig: [u8; 32],
) -> Peer<MemoryRepo> {
    let fixture = admin_credential_registry()
        .lock()
        .unwrap()
        .get(&credential_sig)
        .cloned()
        .expect("fixture credential signature must have a registered proof closure");
    seed_recipient_credential(
        &mut store,
        signing_key,
        team_root,
        &(fixture.cap, fixture.sig),
        fixture.proof_caps,
    );
    bring_up_preseeded(net, signing_key, store, team_root)
}

/// Wire a peer whose recipient ledger has already been prepared by the test.
/// This is the path for scenarios which need to compose recipient history
/// explicitly before startup (for example an accepted predecessor).
pub fn bring_up_preseeded(
    net: &SimNet,
    signing_key: &SigningKey,
    store: MemoryRepo,
    team_root: ed25519_dalek::VerifyingKey,
) -> Peer<MemoryRepo> {
    let id = pk(signing_key);
    let harness = net.join(id);
    let (sender, receiver, wiring) = host::wire(EndpointId::from_bytes(&id).expect("endpoint id"));
    tokio::task::spawn_local(host::run_host(
        harness,
        PeerConfig {
            peers: Vec::new(),
            team_root,
        },
        wiring,
    ));
    Peer::with_wiring(store, signing_key.clone(), team_root, sender, receiver)
}

/// A MemoryRepo seeded with the given cap+sig blobs (so OP_AUTH
/// verifies locally without a chain swarm-fetch).
pub fn store_with_caps(caps: &[(Blob<SimpleArchive>, Blob<SimpleArchive>)]) -> MemoryRepo {
    let mut store = MemoryRepo::default();
    for (cap, sig) in caps {
        if let Some(fixture) = admin_credential_registry()
            .lock()
            .unwrap()
            .get(&sig.get_handle().raw)
            .cloned()
        {
            for blob in fixture.proof_caps {
                store.put::<SimpleArchive, _>(blob).unwrap();
            }
        }
        store.put::<SimpleArchive, _>(cap.clone()).unwrap();
        store.put::<SimpleArchive, _>(sig.clone()).unwrap();
    }
    store
}

/// Signature-blob handle used to select a registered credential fixture.
pub fn credential_sig_handle(sig: &Blob<SimpleArchive>) -> [u8; 32] {
    sig.get_handle().raw
}
