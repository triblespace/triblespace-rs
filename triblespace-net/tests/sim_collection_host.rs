//! End-to-end collection-host cutover coverage over deterministic transport.
#![cfg(feature = "sim")]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, OnceLock};

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::IntoBlob;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::capability::{
    Capability, CapabilityAction, CapabilityMode, CapabilityProof, CapabilityResource,
};
use triblespace_core::clock::{self, VirtualClock};
use triblespace_core::collection::{
    ACTION_READ, ACTION_WRITE, AdmissionPolicy, Collection, CollectionCommit, CollectionHandle,
    CollectionPolicy, CollectionRead, CollectionRecord, CollectionStore, CollectionStoreExt,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStorePut, CapabilityProofRead, CapabilityProofStore, SnapshotSource, StorageFlush,
    WantRead, WantRequest, WantStore,
};
use triblespace_core::trible::TribleSet;
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

fn proof(
    root: &SigningKey,
    leaf: &SigningKey,
    action: CapabilityAction,
    collection: CollectionHandle,
) -> CapabilityProof {
    CapabilityProof::issue_root(
        root,
        CapabilityResource::from(collection),
        Capability::new(action, CapabilityMode::Invoke),
        None,
        leaf.verifying_key(),
    )
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
    bring_up_with_publication_budget(net, endpoint, store, peers, direction, None)
}

fn bring_up_with_publication_budget(
    net: &SimNet,
    endpoint: &SigningKey,
    store: MemoryRepo,
    peers: Vec<[u8; 32]>,
    direction: ReconcileDirection,
    provider_publication_budget: Option<u64>,
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
            provider_publication_budget,
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

async fn acquire_once(
    clock: &Arc<VirtualClock>,
    peer: &mut Peer<MemoryRepo>,
    handle: Inline<Handle<UnknownBlob>>,
    others: &mut [&mut Peer<MemoryRepo>],
) -> Option<Bytes> {
    let mut acquire = Box::pin(peer.acquire(handle));
    loop {
        tokio::select! {
            result = &mut acquire => break result.unwrap(),
            () = SimNet::step(clock, std::time::Duration::from_millis(100)) => {
                for other in others.iter_mut() {
                    other.refresh();
                }
            }
        }
    }
}

#[test]
fn issuer_held_read_proof_bootstraps_a_handle_only_recipient() {
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
        let net = SimNet::new(0xC011_EC70, SimConfig::default());
        let issuer_key = key(71);
        let recipient_key = key(72);
        let policy = CollectionPolicy::new(
            AdmissionPolicy::direct(issuer_key.verifying_key()),
            AdmissionPolicy::direct(issuer_key.verifying_key()),
        );

        let mut issuer_store = MemoryRepo::default();
        let collection = register(&mut issuer_store, policy);
        let read_proof = proof(
            &issuer_key,
            &recipient_key,
            CapabilityAction::new(ACTION_READ),
            collection.handle(),
        );
        issuer_store.insert_proof(read_proof.clone()).unwrap();
        let payload_handle = issuer_store
            .put::<SimpleArchive, _>(TribleSet::new().to_blob())
            .unwrap();
        issuer_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &issuer_key,
                collection.handle(),
                Handle::<SimpleArchive>::to_hash(payload_handle),
                payload_handle,
            )))
            .unwrap();
        let issuer_id = issuer_key.verifying_key().to_bytes();
        let mut issuer = bring_up(
            &net,
            &issuer_key,
            issuer_store,
            Vec::new(),
            ReconcileDirection::WriteOnly,
        );
        let mut recipient = bring_up(
            &net,
            &recipient_key,
            MemoryRepo::default(),
            vec![issuer_id],
            ReconcileDirection::ReadOnly,
        );
        issuer.activate_collection(collection.handle());
        recipient.activate_collection(collection.handle());

        // The recipient begins with only C and one issuer endpoint. An initial
        // exact-H lookup also gives the issuer's provider leases a reachable
        // DHT replica; a retry then obtains the self-describing C bytes.
        let descriptor = acquire_once(
            &clock,
            &mut recipient,
            Inline::new(collection.handle().raw),
            &mut [&mut issuer],
        )
        .await;
        if descriptor.is_none() {
            advance(&clock, &mut [&mut issuer, &mut recipient], 32).await;
            assert!(
                acquire_once(
                    &clock,
                    &mut recipient,
                    Inline::new(collection.handle().raw),
                    &mut [&mut issuer],
                )
                .await
                .is_some()
            );
        }
        let recipient_collection = {
            let snapshot = recipient.snapshot().unwrap();
            Collection::<SimpleArchive>::open(&snapshot, collection.handle()).unwrap()
        };

        // With C resident, normal collection repair uses the issuer's
        // self-contained READ(C) proof to admit this endpoint and sends only
        // native proof and collection records.
        advance(&clock, &mut [&mut issuer, &mut recipient], 32).await;
        let dangling = recipient.snapshot().unwrap();
        assert_eq!(dangling.records().unwrap().count(), 1);
        let received = dangling
            .proofs()
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(received, [read_proof]);
        assert!(
            recipient_collection
                .reader_is_admitted_at(&dangling, recipient_key.verifying_key(), clock::epoch_now())
                .unwrap(),
            "the repaired self-contained proof admits its recipient",
        );
        assert_eq!(dangling.wants().unwrap().count(), 0);
        drop(dangling);

        // The committed payload remains an ordinary exact-H bearer read.
        // Repair manufactures no durable WANT.
        assert!(
            acquire_once(
                &clock,
                &mut recipient,
                Inline::new(payload_handle.raw),
                &mut [&mut issuer],
            )
            .await
            .is_some(),
            "the repaired commit payload follows the ordinary exact-H path",
        );

        let ready = recipient.snapshot().unwrap();
        assert!(
            recipient_collection
                .reader_is_admitted_at(&ready, recipient_key.verifying_key(), clock::epoch_now())
                .unwrap()
        );
        assert_eq!(
            recipient_collection
                .admitted_at(&ready, clock::epoch_now())
                .unwrap()
                .len(),
            1,
        );
        let facts = recipient_collection
            .read_at::<TribleSet, _>(&ready, clock::epoch_now())
            .unwrap();
        assert!(facts.is_empty());
        assert_eq!(ready.wants().unwrap().count(), 0);
    }));
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
        let payload = TribleSet::new().to_blob();
        let payload_handle = server_store.put::<SimpleArchive, _>(payload).unwrap();
        server_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &writer,
                collection.handle(),
                Handle::<SimpleArchive>::to_hash(payload_handle),
                payload_handle,
            )))
            .unwrap();
        let write = proof(
            &write_root,
            &writer,
            CapabilityAction::new(ACTION_WRITE),
            collection.handle(),
        );

        let mut reader_store = MemoryRepo::default();
        let reader_collection = register(&mut reader_store, policy);
        assert_eq!(reader_collection.handle(), collection.handle());
        reader_store
            .insert_proof(proof(
                &read_root,
                &reader_key,
                CapabilityAction::new(ACTION_READ),
                collection.handle(),
            ))
            .unwrap();

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
        // Background collection repair may already have transferred the
        // signed record. Without WRITE evidence it remains semantically inert.
        assert!(
            reader_collection
                .admitted_at(&before, clock::epoch_now())
                .unwrap()
                .is_empty()
        );

        let bootstrap = reconcile_once(
            &clock,
            &mut Reconciler::default(),
            &mut server,
            &mut [&mut reader],
        )
        .await;
        assert_eq!(
            bootstrap.fulfilled, 0,
            "proof receipt must not manufacture a durable blob WANT"
        );
        assert_eq!(server.snapshot().unwrap().wants().unwrap().count(), 0);

        advance(&clock, &mut [&mut server, &mut reader], 32).await;
        let repaired = reader.snapshot().unwrap();
        assert_eq!(repaired.records().unwrap().count(), 1);
        assert!(
            reader_collection
                .admitted_at(&repaired, clock::epoch_now())
                .unwrap()
                .is_empty()
        );

        assert!(
            acquire_once(
                &clock,
                &mut reader,
                Inline::new(payload_handle.raw),
                &mut [&mut server],
            )
            .await
            .is_some(),
            "active collection use acquires the exact committed payload",
        );
        assert_eq!(reader.snapshot().unwrap().wants().unwrap().count(), 0);

        // The grant can arrive after the record at the receiver. The
        // WriteOnly publisher never receives or presents it.
        reader.store().insert_proof(write).unwrap();
        reader.refresh();
        let after = reader.snapshot().unwrap();
        assert_eq!(after.records().unwrap().count(), 1);
        assert_eq!(after.proofs().unwrap().count(), 2);
        assert_eq!(
            reader_collection
                .admitted_at(&after, clock::epoch_now())
                .unwrap()
                .len(),
            1
        );
        let publisher = server.snapshot().unwrap();
        assert_eq!(publisher.proofs().unwrap().count(), 1);
        assert!(
            collection
                .admitted_at(&publisher, clock::epoch_now())
                .unwrap()
                .is_empty()
        );
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
        let write = proof(
            &write_root,
            &writer_key,
            CapabilityAction::new(ACTION_WRITE),
            collection.handle(),
        );
        server_store.insert_proof(write.clone()).unwrap();
        let payload = TribleSet::new().to_blob();
        let payload_handle = server_store.put::<SimpleArchive, _>(payload).unwrap();
        server_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &writer_key,
                collection.handle(),
                Handle::<SimpleArchive>::to_hash(payload_handle),
                payload_handle,
            )))
            .unwrap();

        let mut reader_store = MemoryRepo::default();
        let reader_collection = register(&mut reader_store, policy.clone());
        reader_store
            .insert_proof(proof(
                &read_root,
                &reader_key,
                CapabilityAction::new(ACTION_READ),
                collection.handle(),
            ))
            .unwrap();
        let mut writer_store = MemoryRepo::default();
        register(&mut writer_store, policy);
        writer_store.insert_proof(write).unwrap();

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
            stats.fulfilled, 0,
            "proof receipt must not manufacture a durable blob WANT"
        );
        assert_eq!(server.snapshot().unwrap().wants().unwrap().count(), 0);
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
            stats.fulfilled, 0,
            "proof repair must not manufacture a durable blob WANT"
        );
        let dangling = reader.snapshot().unwrap();
        assert_eq!(
            dangling.proofs().unwrap().count(),
            2,
            "the self-contained WRITE proof repairs with the collection records",
        );
        assert!(
            reader_collection
                .admitted_at(&dangling, clock::epoch_now())
                .unwrap()
                .is_empty(),
            "a frozen snapshot hides a commit whose payload is absent",
        );
        drop(dangling);
        assert!(
            acquire_once(
                &clock,
                &mut reader,
                Inline::new(payload_handle.raw),
                &mut [&mut server, &mut writer_only],
            )
            .await
            .is_some(),
            "active collection use acquires the exact committed payload",
        );
        assert_eq!(reader.snapshot().unwrap().wants().unwrap().count(), 0);
        let reader_snapshot = reader.snapshot().unwrap();
        assert_eq!(
            reader_collection
                .admitted_at(&reader_snapshot, clock::epoch_now())
                .unwrap()
                .len(),
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
fn collection_wake_recovery_survives_a_partition_without_dht_or_restart() {
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
        let net = SimNet::new(0xC011_EC75, SimConfig::default());
        let server_key = key(51);
        let reader_key = key(52);
        let policy = CollectionPolicy::new(
            AdmissionPolicy::Open,
            AdmissionPolicy::direct(server_key.verifying_key()),
        );

        let mut first_facts = TribleSet::new();
        let mut first_raw = [1; triblespace_core::trible::TRIBLE_LEN];
        first_raw[16..32].fill(2);
        first_facts.insert(
            &triblespace_core::trible::Trible::force_raw(first_raw)
                .expect("non-nil entity and attribute"),
        );

        let mut server_store = MemoryRepo::default();
        let collection = register(&mut server_store, policy.clone());
        let first_payload = server_store
            .put::<SimpleArchive, _>(first_facts.to_blob())
            .unwrap();
        server_store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &server_key,
                collection.handle(),
                Handle::<SimpleArchive>::to_hash(first_payload),
                first_payload,
            )))
            .unwrap();

        let mut reader_store = MemoryRepo::default();
        let reader_collection = register(&mut reader_store, policy);
        assert_eq!(reader_collection.handle(), collection.handle());

        let server_id = server_key.verifying_key().to_bytes();
        let reader_id = reader_key.verifying_key().to_bytes();
        let mut server = bring_up_with_publication_budget(
            &net,
            &server_key,
            server_store,
            Vec::new(),
            ReconcileDirection::WriteOnly,
            Some(0),
        );
        let mut reader = bring_up_with_publication_budget(
            &net,
            &reader_key,
            reader_store,
            vec![server_id],
            ReconcileDirection::ReadOnly,
            Some(0),
        );
        server.activate_collection(collection.handle());
        reader.activate_collection(collection.handle());

        advance(&clock, &mut [&mut server, &mut reader], 5).await;
        assert_eq!(reader.snapshot().unwrap().records().unwrap().count(), 1);

        net.partition(server_id, reader_id);
        let mut second_facts = TribleSet::new();
        let mut second_raw = [3; triblespace_core::trible::TRIBLE_LEN];
        second_raw[16..32].fill(4);
        second_facts.insert(
            &triblespace_core::trible::Trible::force_raw(second_raw)
                .expect("non-nil entity and attribute"),
        );
        let second_payload = server
            .put::<SimpleArchive, _>(second_facts.to_blob())
            .unwrap();
        server
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &server_key,
                collection.handle(),
                Handle::<SimpleArchive>::to_hash(second_payload),
                second_payload,
            )))
            .unwrap();
        server.refresh();

        // Let the signed wake be lost, periodic repair fail, and at least one
        // recovery resubscription happen while the partition is still closed.
        advance(&clock, &mut [&mut server, &mut reader], 40).await;
        assert_eq!(reader.snapshot().unwrap().records().unwrap().count(), 1);

        // Healing alone must suffice: there is no DHT publication, new write,
        // process restart, or direct collection repair to a configured route.
        net.heal(server_id, reader_id);
        advance(&clock, &mut [&mut server, &mut reader], 95).await;
        assert_eq!(reader.snapshot().unwrap().records().unwrap().count(), 2);
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
            let snapshot = reader.snapshot().unwrap();
            snapshot.wants().unwrap().map(Result::unwrap).collect()
        };
        assert_eq!(wants, BTreeSet::from([wanted, absent]));
    }));
}
