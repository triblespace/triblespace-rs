//! End-to-end collection-host cutover coverage over deterministic transport.
#![cfg(feature = "sim")]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::capability::{
    CapabilityAction, CapabilityAtom, CapabilityClaim, CapabilityMode, CapabilityProofBundle,
    CapabilityResource,
};
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::collection::{
    ACTION_READ, ACTION_WRITE, AdmissionPolicy, Collection, CollectionCommit, CollectionData,
    CollectionHandle, CollectionPolicy, CollectionRead, CollectionRecord, CollectionStore,
    CollectionStoreExt, empty_metadata_handle,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStorePut, CapabilityProofRead, CapabilityProofStore, SnapshotSource, StorageFlush,
    WantRequest, WantStore,
};
use triblespace_net::host::{self, PeerConfig};
use triblespace_net::inventory::{ReconcileDirection, ReconcileQos};
use triblespace_net::peer::Peer;
use triblespace_net::reconcile::{ReconcileStats, Reconciler};
use triblespace_net::transport::sim::{SimConfig, SimNet};

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

fn virtual_clock() -> Arc<VirtualClock> {
    static CLOCK: OnceLock<Arc<VirtualClock>> = OnceLock::new();
    CLOCK
        .get_or_init(|| {
            let clock =
                VirtualClock::new(hifitime::Epoch::from_gregorian_utc_at_midnight(2026, 1, 1));
            clock::install_virtual(clock.clone()).expect("first virtual-clock install");
            clock
        })
        .clone()
}

fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bundle(
    root: &SigningKey,
    leaf: &SigningKey,
    action: triblespace_core::capability::CapabilityAction,
    collection: CollectionHandle,
) -> CapabilityProofBundle {
    CapabilityProofBundle::issue_root(
        root,
        CapabilityClaim::root(
            CapabilityAtom::new(action, CapabilityResource::from(collection)),
            CapabilityMode::Invoke,
            None,
        ),
        leaf.verifying_key(),
    )
    .unwrap()
}

fn store_bundle(store: &mut MemoryRepo, bundle: CapabilityProofBundle) {
    let (proof, claims) = bundle.into_parts();
    for claim in claims {
        store.put::<SimpleArchive, _>(claim).unwrap();
    }
    store.insert_proof(proof).unwrap();
}

fn register(store: &mut MemoryRepo, policy: CollectionPolicy) -> Collection<SimpleArchive> {
    store.collection("collection-host-e2e", policy).unwrap()
}

fn bring_up(
    net: &SimNet,
    endpoint: &SigningKey,
    store: MemoryRepo,
    peers: Vec<[u8; 32]>,
    direction: ReconcileDirection,
) -> Peer<MemoryRepo> {
    let id = endpoint.verifying_key().to_bytes();
    let harness = net.join(endpoint);
    let (sender, receiver, wiring) =
        host::wire(EndpointId::from_bytes(&id).expect("valid endpoint id"));
    let qos = ReconcileQos { direction };
    tokio::task::spawn_local(host::run_host(
        harness,
        PeerConfig {
            peers: peers
                .into_iter()
                .map(|peer| {
                    iroh_base::EndpointAddr::from(
                        EndpointId::from_bytes(&peer).expect("valid configured peer"),
                    )
                })
                .collect(),
            qos,
            provider_publication_budget: None,
        },
        wiring,
    ));
    Peer::with_wiring(store, qos, sender, receiver)
}

async fn advance(clock: &Arc<VirtualClock>, peers: &mut [&mut Peer<MemoryRepo>], seconds: u64) {
    for _ in 0..seconds * 10 {
        SimNet::step(clock, std::time::Duration::from_millis(100)).await;
        for peer in peers.iter_mut() {
            peer.refresh();
        }
    }
}

async fn reconcile_once(
    clock: &Arc<VirtualClock>,
    reconciler: &mut Reconciler,
    peer: &mut Peer<MemoryRepo>,
    others: &mut [&mut Peer<MemoryRepo>],
) -> ReconcileStats {
    let mut tick = Box::pin(reconciler.tick(peer));
    loop {
        tokio::select! {
            stats = &mut tick => break stats,
            () = SimNet::step(clock, std::time::Duration::from_millis(100)) => {
                for other in others.iter_mut() {
                    other.refresh();
                }
            }
        }
    }
}

#[test]
fn write_proof_later_activates_repaired_commit_without_reaching_publisher() {
    let _guard = test_guard();
    let clock = virtual_clock();
    clock.reset();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let net = SimNet::new(0xC011_EC71, SimConfig::default());
        let server_key = key(1);
        let reader_key = key(2);
        let read_root = key(3);
        let write_root = key(4);
        let writer = key(5);
        let policy = CollectionPolicy::new(
            AdmissionPolicy::direct(read_root.verifying_key()),
            AdmissionPolicy::direct(write_root.verifying_key()),
        );

        let mut server_store = MemoryRepo::default();
        let collection = register(&mut server_store, policy.clone());
        server_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &writer,
                collection.handle(),
                CollectionData::new([9; 32]),
                empty_metadata_handle(),
            )))
            .unwrap();
        let write = bundle(
            &write_root,
            &writer,
            CapabilityAction::new(ACTION_WRITE),
            collection.handle(),
        );

        let mut reader_store = MemoryRepo::default();
        let reader_collection = register(&mut reader_store, policy);
        assert_eq!(reader_collection.handle(), collection.handle());
        store_bundle(
            &mut reader_store,
            bundle(
                &read_root,
                &reader_key,
                CapabilityAction::new(ACTION_READ),
                collection.handle(),
            ),
        );

        let server_id = server_key.verifying_key().to_bytes();
        let mut server = bring_up(
            &net,
            &server_key,
            server_store,
            Vec::new(),
            ReconcileDirection::WriteOnly,
        );
        let mut reader = bring_up(
            &net,
            &reader_key,
            reader_store,
            vec![server_id],
            ReconcileDirection::ReadOnly,
        );
        server.activate_collection(collection.handle());
        reader.activate_collection(collection.handle());

        advance(&clock, &mut [&mut server, &mut reader], 3).await;
        let before = reader.snapshot().unwrap();
        assert_eq!(before.records().unwrap().count(), 0);
        assert!(reader_collection.admitted(&before).unwrap().is_empty());

        let bootstrap = reconcile_once(
            &clock,
            &mut Reconciler::default(),
            &mut server,
            &mut [&mut reader],
        )
        .await;
        assert_eq!(
            bootstrap.fulfilled, 1,
            "the server resolves the reader's claim through Blob(H) before retry admission"
        );

        advance(&clock, &mut [&mut server, &mut reader], 32).await;
        let repaired = reader.snapshot().unwrap();
        assert_eq!(repaired.records().unwrap().count(), 1);
        assert!(reader_collection.admitted(&repaired).unwrap().is_empty());

        // The grant can arrive after the record at the receiver. The
        // WriteOnly publisher never receives or presents it.
        store_bundle(&mut reader.store(), write);
        reader.refresh();
        let after = reader.snapshot().unwrap();
        assert_eq!(after.records().unwrap().count(), 1);
        assert_eq!(after.proofs().unwrap().count(), 2);
        assert_eq!(reader_collection.admitted(&after).unwrap().len(), 1);
        let publisher = server.snapshot().unwrap();
        assert_eq!(publisher.proofs().unwrap().count(), 1);
        assert!(collection.admitted(&publisher).unwrap().is_empty());
    }));
}

#[test]
fn native_read_proof_bootstraps_on_retry_and_rejects_writer_only_peer() {
    let _guard = test_guard();
    let clock = virtual_clock();
    clock.reset();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let net = SimNet::new(0xC011_EC72, SimConfig::default());
        let server_key = key(11);
        let reader_key = key(12);
        let writer_key = key(13);
        let read_root = key(14);
        let write_root = key(15);
        let policy = CollectionPolicy::new(
            AdmissionPolicy::direct(read_root.verifying_key()),
            AdmissionPolicy::direct(write_root.verifying_key()),
        );

        let mut server_store = MemoryRepo::default();
        let collection = register(&mut server_store, policy.clone());
        let write = bundle(
            &write_root,
            &writer_key,
            CapabilityAction::new(ACTION_WRITE),
            collection.handle(),
        );
        store_bundle(&mut server_store, write.clone());
        server_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &writer_key,
                collection.handle(),
                CollectionData::new([17; 32]),
                empty_metadata_handle(),
            )))
            .unwrap();

        let mut reader_store = MemoryRepo::default();
        let reader_collection = register(&mut reader_store, policy.clone());
        store_bundle(
            &mut reader_store,
            bundle(
                &read_root,
                &reader_key,
                CapabilityAction::new(ACTION_READ),
                collection.handle(),
            ),
        );
        let mut writer_store = MemoryRepo::default();
        register(&mut writer_store, policy);
        store_bundle(&mut writer_store, write);

        let server_id = server_key.verifying_key().to_bytes();
        let mut server = bring_up(
            &net,
            &server_key,
            server_store,
            Vec::new(),
            ReconcileDirection::WriteOnly,
        );
        let mut reader = bring_up(
            &net,
            &reader_key,
            reader_store,
            vec![server_id],
            ReconcileDirection::ReadOnly,
        );
        let mut writer_only = bring_up(
            &net,
            &writer_key,
            writer_store,
            vec![server_id],
            ReconcileDirection::ReadOnly,
        );
        for peer in [&mut server, &mut reader, &mut writer_only] {
            peer.activate_collection(collection.handle());
        }

        advance(&clock, &mut [&mut server, &mut reader, &mut writer_only], 4).await;
        let stats = reconcile_once(
            &clock,
            &mut Reconciler::default(),
            &mut server,
            &mut [&mut reader, &mut writer_only],
        )
        .await;
        assert_eq!(
            stats.fulfilled, 1,
            "the cold server resolves the bootstrapped READ proof's claim through Blob(H)"
        );
        advance(
            &clock,
            &mut [&mut server, &mut reader, &mut writer_only],
            32,
        )
        .await;
        assert_eq!(reader.snapshot().unwrap().records().unwrap().count(), 1);
        let stats = reconcile_once(
            &clock,
            &mut Reconciler::default(),
            &mut reader,
            &mut [&mut server, &mut writer_only],
        )
        .await;
        assert_eq!(
            stats.fulfilled, 1,
            "the repaired WRITE proof's claim also arrives through Blob(H)"
        );
        let reader_snapshot = reader.snapshot().unwrap();
        assert_eq!(
            reader_collection.admitted(&reader_snapshot).unwrap().len(),
            1
        );
        let writer_snapshot = writer_only.snapshot().unwrap();
        assert_eq!(
            writer_snapshot.records().unwrap().count(),
            0,
            "WRITE(C) without READ(C) must not learn even the collection manifest"
        );
    }));
}

#[test]
fn durable_bearer_want_materializes_without_any_collection() {
    let _guard = test_guard();
    let clock = virtual_clock();
    clock.reset();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let net = SimNet::new(0xC011_EC74, SimConfig::default());
        let server_key = key(41);
        let reader_key = key(42);
        let mut server_store = MemoryRepo::default();
        let payload = Bytes::from_source(b"durable H-only WANT".to_vec());
        let payload_handle = server_store.put::<UnknownBlob, _>(payload.clone()).unwrap();

        let mut reader_store = MemoryRepo::default();
        let wanted = WantRequest::blob(payload_handle);
        let absent = WantRequest::blob(Inline::<Handle<UnknownBlob>>::new([0xFF; 32]));
        for request in [wanted, absent] {
            reader_store.want(request).unwrap();
        }
        reader_store.flush().unwrap();

        let server_id = server_key.verifying_key().to_bytes();
        let mut server = bring_up(
            &net,
            &server_key,
            server_store,
            Vec::new(),
            ReconcileDirection::WriteOnly,
        );
        let mut reader = bring_up(
            &net,
            &reader_key,
            reader_store,
            vec![server_id],
            ReconcileDirection::ReadOnly,
        );
        advance(&clock, &mut [&mut server, &mut reader], 4).await;

        let mut reconciler = Reconciler::with_backoff(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(60),
        )
        .with_fetch_budget(std::time::Duration::from_secs(2));
        let mut tick = Box::pin(reconciler.tick(&mut reader));
        let stats = loop {
            tokio::select! {
                stats = &mut tick => break stats,
                () = SimNet::step(&clock, std::time::Duration::from_millis(100)) => {
                    server.refresh();
                }
            }
        };
        drop(tick);
        assert_eq!(
            stats,
            ReconcileStats {
                wants: 2,
                missing: 2,
                attempted: 2,
                fulfilled: 1,
                pending: 1,
            },
            "the exact resident H resolves globally while a wrong H stays pending"
        );
        assert_eq!(reader.try_local(payload_handle.raw), Some(payload));
        let wants: BTreeSet<_> = {
            let mut store = reader.store();
            store.wants().unwrap().map(Result::unwrap).collect()
        };
        assert_eq!(wants, BTreeSet::from([wanted, absent]));
    }));
}
