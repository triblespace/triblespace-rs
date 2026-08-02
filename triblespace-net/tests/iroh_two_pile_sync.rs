//! Two-pile blob fetch over the real iroh transport stack.
//!
//! This test proves the transport path: two `Peer<Pile>`s — real pile files
//! on disk — run the full production
//! stack (`transport::iroh::bind_with_endpoint`: embedded DHT node,
//! protocol router, DHT discovery, OP_AUTH with cap-chain
//! verification) over real iroh QUIC endpoints wired through
//! `iroh::test_utils` `TestNetwork` (an in-memory packet transport —
//! no relays, no DNS, no OS sockets — everything above the packet
//! layer is the production code path).
//!
//! A content blob lives only in pile A. After both nodes have joined, A
//! announces that immutable blob to the DHT. B durably appends an authored
//! want for its hash; `Reconciler::tick` fetches and verifies the bytes and
//! lands them in pile B. Branch replication is deliberately outside this test.
//!
//! Piles are created under `std::env::temp_dir()` — set `TMPDIR` to
//! redirect.
//!
//! Run with:
//! `cargo test -p triblespace-net --test iroh_two_pile_sync`

use std::time::Duration;

use ed25519_dalek::SigningKey;
use iroh::Endpoint;
use iroh::endpoint::presets;
use iroh::test_utils::test_transport::TestNetwork;
use iroh_base::{EndpointAddr, EndpointId, SecretKey};
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::inline::{Inline, TryToInline};
use triblespace_core::prelude::BlobStore;
use triblespace_core::repo::capability::{self, PERM_ADMIN};
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::want::WantStore;
use triblespace_core::repo::{BlobStoreGet, BlobStorePut, Repository, StorageFlush};
use triblespace_core::trible::TribleSet;
use triblespace_net::clock;
use triblespace_net::host;
use triblespace_net::peer::{Peer, PeerConfig};
use triblespace_net::recipient_ledger::{RecipientWriteOutcome, accept_credential, declare_intent};
use triblespace_net::reconcile::Reconciler;

fn key(n: u8) -> SigningKey {
    SigningKey::from_bytes(&[n; 32])
}

/// Build the root-signed founder anchor plus the founder's finite
/// operational PERM_ADMIN credential.
fn admin_cap(
    root: &SigningKey,
    subject: &SigningKey,
) -> (
    (Blob<SimpleArchive>, Blob<SimpleArchive>),
    (Blob<SimpleArchive>, Blob<SimpleArchive>),
) {
    use triblespace_core::id::{ExclusiveId, ufoid};
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
    let cap = capability::build_capability(
        subject,
        subject.verifying_key(),
        anchor.clone(),
        scope_root,
        scope_facts,
        expiry,
    )
    .expect("build finite founder credential");
    (anchor, cap)
}

/// A fresh pile file in a temp dir, seeded with the given cap+sig
/// blobs so OP_AUTH verifies locally on both ends.
fn fresh_pile(
    dir: &std::path::Path,
    name: &str,
    caps: &[(Blob<SimpleArchive>, Blob<SimpleArchive>)],
) -> Pile {
    let path = dir.join(name);
    std::fs::File::create(&path).expect("create pile file");
    let mut pile = Pile::open(&path).expect("open pile");
    for (cap, sig) in caps {
        pile.put::<SimpleArchive, _>(cap.clone()).expect("seed cap");
        pile.put::<SimpleArchive, _>(sig.clone()).expect("seed sig");
    }
    pile.flush().expect("flush seeded pile");
    pile
}

/// Publish one fixture credential as the node's durable recipient decision
/// before the peer can cause any authenticated network effect.
fn seed_recipient_credential(
    pile: &mut Pile,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    anchor_cap: Blob<SimpleArchive>,
    credential: &(Blob<SimpleArchive>, Blob<SimpleArchive>),
) {
    let declared = declare_intent(pile, signing_key, team_root, credential.0.clone())
        .expect("publish fixture recipient intent");
    assert!(
        matches!(declared, RecipientWriteOutcome::Published(_)),
        "fixture recipient intent must be accepted, got {declared:?}"
    );

    let accepted = accept_credential(
        pile,
        signing_key,
        team_root,
        credential.1.clone(),
        [credential.0.clone(), anchor_cap],
        clock::epoch_now(),
    )
    .expect("publish fixture recipient credential");
    assert!(
        matches!(accepted, RecipientWriteOutcome::Published(_)),
        "fixture recipient credential must be accepted, got {accepted:?}"
    );
}

/// Bind a real iroh endpoint whose only packet path is the shared
/// `TestNetwork` (mirrors `auth_handshake_e2e::test_endpoint`), with
/// the network's address-lookup service replacing the N0 discovery
/// stack so bare-`EndpointId` dials resolve without DNS/pkarr.
async fn test_endpoint(network: &TestNetwork, secret: SecretKey) -> Endpoint {
    let transport = network
        .create_transport(secret.public())
        .expect("create test transport");
    Endpoint::builder(presets::N0)
        .secret_key(secret)
        .relay_mode(iroh::RelayMode::Disabled)
        .ca_tls_config(iroh::tls::CaTlsConfig::insecure_skip_verify())
        .add_custom_transport(transport)
        .clear_ip_transports()
        .clear_address_lookup()
        .address_lookup(network.address_lookup())
        .bind()
        .await
        .expect("bind endpoint")
}

/// Bring one node up over the TestNetwork: bind the endpoint, wire the
/// full production transport stack (`bind_with_endpoint`: DHT node and
/// protocol router), spawn the host loop as a tokio
/// task, and wrap the pile in a `Peer`.
async fn bring_up(
    network: &TestNetwork,
    signing_key: &SigningKey,
    store: Pile,
    team_root: ed25519_dalek::VerifyingKey,
    bootstrap: Vec<EndpointAddr>,
) -> Peer<Pile> {
    let secret = triblespace_net::identity::iroh_secret(signing_key);
    let id: EndpointId = secret.public().into();
    let ep = test_endpoint(network, secret).await;
    let config = PeerConfig {
        peers: bootstrap,
        team_root,
    };
    let harness = triblespace_net::transport::iroh::bind_with_endpoint(ep, &config).await;
    let (sender, receiver, wiring) = host::wire(id);
    tokio::spawn(host::run_host(harness, config, wiring));
    Peer::with_wiring(store, signing_key.clone(), team_root, sender, receiver)
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// The shared two-node bring-up: team root + two admin caps, two piles
/// (both seeded with both chains), A up first with no bootstrap, B up
/// second bootstrapping its DHT off A.
struct TwoNodes {
    repo_a: Repository<Peer<Pile>>,
    repo_b: Repository<Peer<Pile>>,
    _dir: tempfile::TempDir,
}

async fn two_nodes(network: &TestNetwork, ka: &SigningKey, kb: &SigningKey) -> TwoNodes {
    let root = key(0xF0);
    let team_root = root.verifying_key();
    let (anchor_a, cap_a) = admin_cap(&root, ka);
    let (anchor_b, cap_b) = admin_cap(&root, kb);
    let caps = [
        anchor_a.clone(),
        cap_a.clone(),
        anchor_b.clone(),
        cap_b.clone(),
    ];

    let dir = tempfile::tempdir().expect("temp dir for piles");
    let mut pile_a = fresh_pile(dir.path(), "a.pile", &caps);
    seed_recipient_credential(&mut pile_a, ka, team_root, anchor_a.0.clone(), &cap_a);
    let mut pile_b = fresh_pile(dir.path(), "b.pile", &caps);
    seed_recipient_credential(&mut pile_b, kb, team_root, anchor_b.0.clone(), &cap_b);

    let peer_a = bring_up(network, ka, pile_a, team_root, Vec::new()).await;
    let a_id: EndpointAddr = peer_a.id().into();
    let peer_b = bring_up(network, kb, pile_b, team_root, vec![a_id]).await;

    let repo_a = Repository::new(peer_a, ka.clone(), TribleSet::new()).expect("repo a");
    let repo_b = Repository::new(peer_b, kb.clone(), TribleSet::new()).expect("repo b");
    TwoNodes {
        repo_a,
        repo_b,
        _dir: dir,
    }
}

/// A content blob lives only in pile A. B records a durable authored want;
/// the reconciler obtains it through content-addressed blob transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asserted_want_fetches_from_holder_over_iroh() {
    init_tracing();
    let network = TestNetwork::new();
    let ka = key(0xA1);
    let kb = key(0xB1);

    // The lazy payload: a blob in pile A outside any branch history.
    let payload: TribleSet = {
        use triblespace_core::id::{ExclusiveId, ufoid};
        use triblespace_core::macros::entity;
        let e = *ufoid();
        let tag = *ufoid();
        TribleSet::from(entity! {
            ExclusiveId::force_ref(&e) @
            triblespace_core::metadata::tag: tag,
        })
    };
    let blob: Blob<SimpleArchive> = payload.to_blob();
    let hash = blob.get_handle().raw;

    let TwoNodes {
        mut repo_a,
        mut repo_b,
        _dir,
    } = two_nodes(&network, &ka, &kb).await;

    // Put through Peer only after both nodes have joined, so the provider
    // announcement exercises the ordinary DHT path without a branch HEAD.
    repo_a
        .storage_mut()
        .put::<SimpleArchive, _>(blob.clone())
        .expect("store payload");
    repo_a.storage_mut().flush().expect("flush payload");

    // Precondition: B does not hold the payload before servicing its want.
    {
        let reader = repo_b.storage_mut().reader().expect("b reader");
        let held: Result<anybytes::Bytes, _> =
            BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(hash));
        assert!(held.is_err(), "precondition: B must not hold A's payload");
    }

    // The durable want is signed by B and durably appended before reconcile.
    // `append_pin_assertion` itself is the persistence boundary, so no extra
    // flush belongs here.
    repo_b
        .storage_mut()
        .assert_want(Inline::<Handle<UnknownBlob>>::new(hash))
        .expect("record authored want");

    // Service the want. Each tick diffs wants against presence and
    // drives the swarm fetch for the missing ones.
    let mut reconciler =
        Reconciler::with_backoff(Duration::from_millis(200), Duration::from_secs(2))
            .with_fetch_budget(Duration::from_secs(10));
    let mut fetched = false;
    for _ in 0..60u32 {
        repo_a.storage_mut().refresh().unwrap(); // keep A serving a fresh snapshot
        let stats = reconciler.tick(repo_b.storage_mut()).await;
        if stats.fetched >= 1 {
            fetched = true;
            break;
        }
        // wants=1 expected throughout; missing goes 1 → 0 on success.
        assert!(stats.wants >= 1, "the recorded want must stay on record");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        fetched,
        "Reconciler must fetch the authored want from A over the iroh transport"
    );

    // The payload landed in pile B…
    {
        let reader = repo_b.storage_mut().reader().expect("b reader");
        let got: anybytes::Bytes =
            BlobStoreGet::get::<anybytes::Bytes, UnknownBlob>(&reader, Inline::new(hash))
                .expect("B holds the payload after reconcile");
        assert_eq!(
            blake3::hash(&got).as_bytes(),
            &hash,
            "landed bytes verify against the requested hash"
        );
    }
    // …and B's grow-only want remains on record, now inert because the bytes
    // are locally present.
    let wants = repo_b.storage_mut().wants().expect("authored wants");
    assert!(
        wants.contains(&Inline::<Handle<UnknownBlob>>::new(hash)),
        "the authored want stays on record"
    );
}
