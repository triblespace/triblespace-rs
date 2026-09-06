//! Exact-H acquisition through the production DHT and bearer RPC handlers.
//!
//! Only connection latency and the caller deadline advance in these tests.
//! Preinstalled, unexpired directory leases isolate acquisition from the
//! independent publication scheduler and require no global virtual clock.

use std::time::Duration;

use triblespace_core::collection::CollectionRead;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{BlobStorePut, SnapshotSource, WantRead};

use crate::transport::sim::{SimConfig, SimNet, SimTransport};

use super::*;

struct Fixture {
    net: SimNet,
    client: ProviderClient<SimTransport>,
    sender: NetSender,
    provider: PeerId,
    hash: RawHash,
    bytes: Bytes,
    store: MemoryRepo,
    events: tokio::sync::mpsc::Receiver<NetEventBatch>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    fn new(advertise: bool) -> Self {
        // SimNet charges one round trip for connection setup: four seconds,
        // beyond the background lookup cap but within the foreground budget.
        let latency = Duration::from_secs(2);
        let net = SimNet::new(
            0xC01D_D417,
            SimConfig {
                latency: latency..latency,
            },
        );
        let provider_key = SigningKey::from_bytes(&[91; 32]);
        let client_key = SigningKey::from_bytes(&[92; 32]);
        let provider = provider_key.verifying_key().to_bytes();
        let my_id = client_key.verifying_key().to_bytes();
        let mut server_harness = net.join(&provider_key);
        let client_harness = net.join(&client_key);
        let client = ProviderClient {
            transport: client_harness.transport,
            pool: new_shared_pool(),
            providers: Arc::new(Mutex::new(ProviderDirectory::new(my_id))),
            candidates: Arc::new(Mutex::new(RoutingTable::new(my_id, [provider]))),
            my_id,
        };
        let (sender, _receiver, wiring) = wire(EndpointId::from_bytes(&my_id).unwrap());
        wiring.install_test_capability(Arc::new(NetCap {
            client: client.clone(),
        }));

        let mut store = MemoryRepo::default();
        let bytes = Bytes::from_source(b"cold exact-H acquisition".to_vec());
        let hash = store.put::<UnknownBlob, _>(bytes.clone()).unwrap().raw;
        let snapshot = StoreSnapshot::from_store_changes(
            store.snapshot().unwrap(),
            &ActiveCollections::new(),
            provider_key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            None,
        )
        .unwrap();
        let mut providers = ProviderDirectory::new(provider);
        if advertise {
            assert!(providers.put(
                blob_locator(hash),
                provider,
                blob_provider_token(hash, provider),
                crate::clock::mono_now(),
            ));
        }
        let (events_tx, events) = tokio::sync::mpsc::channel(16);
        let handler = SnapshotHandler {
            snapshot: Arc::new(Mutex::new(Some(Arc::new(snapshot)))),
            candidates: Arc::new(Mutex::new(RoutingTable::new(provider, []))),
            providers: Arc::new(Mutex::new(providers)),
            serve_collections: false,
            local_id: provider,
            events: events_tx,
            inbound_connections: Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS)),
            inbound_requests: Arc::new(tokio::sync::Semaphore::new(MAX_REQUESTS_GLOBAL)),
        };
        let server = tokio::spawn(async move {
            while let Some(incoming) = server_harness.incoming.recv().await {
                assert_eq!(incoming.alpn, PILE_SYNC_ALPN);
                let permit = handler
                    .inbound_connections
                    .clone()
                    .try_acquire_owned()
                    .unwrap();
                let handler = handler.clone();
                tokio::spawn(async move {
                    handler.handle::<SimTransport>(incoming.conn, permit).await;
                });
            }
        });
        Self {
            net,
            client,
            sender,
            provider,
            hash,
            bytes,
            store,
            events,
            server,
        }
    }

    fn assert_no_control_effects(&mut self) {
        let snapshot = self.store.snapshot().unwrap();
        assert_eq!(snapshot.records().unwrap().count(), 0);
        assert_eq!(snapshot.wants().unwrap().count(), 0);
        assert!(matches!(
            self.events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test(start_paused = true)]
async fn cold_exact_lookup_outlives_background_cap_within_caller_deadline() {
    let mut fixture = Fixture::new(true);
    let started = tokio::time::Instant::now();
    assert_eq!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await,
        Some(fixture.bytes.clone()),
    );
    assert!(started.elapsed() > BACKGROUND_LOOKUP_DEADLINE);
    assert!(started.elapsed() < INTERACTIVE_FETCH_DEADLINE);

    let warmed = tokio::time::Instant::now();
    assert_eq!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await,
        Some(fixture.bytes.clone()),
    );
    assert_eq!(warmed.elapsed(), Duration::ZERO);
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn responsive_provider_is_not_held_behind_stalled_secondary_bootstrap() {
    let mut fixture = Fixture::new(true);
    let stalled_key = SigningKey::from_bytes(&[93; 32]);
    let stalled = stalled_key.verifying_key().to_bytes();
    let _stalled_harness = fixture.net.join(&stalled_key);
    fixture.net.stall_dials(stalled);
    *fixture.client.candidates.lock().unwrap() =
        RoutingTable::new(fixture.client.my_id, [fixture.provider, stalled]);

    let started = tokio::time::Instant::now();
    assert_eq!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await,
        Some(fixture.bytes.clone()),
    );
    assert_eq!(started.elapsed(), Duration::from_secs(7));
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn background_lookup_keeps_its_short_bound() {
    let mut fixture = Fixture::new(true);
    let started = tokio::time::Instant::now();
    let error = fixture
        .client
        .fetch_blob(fixture.hash, Some(BACKGROUND_LOOKUP_DEADLINE))
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), BACKGROUND_LOOKUP_DEADLINE);
    assert!(error.to_string().contains("no remote replica"));
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn warm_provider_miss_is_not_a_transport_failure_or_direct_peer_probe() {
    let mut fixture = Fixture::new(false);
    // The bootstrap endpoint holds H but has no lease for it. Warming the
    // topology must not turn that route into an alternate direct-H probe.
    pool_get(
        &fixture.client.transport,
        &fixture.client.pool,
        fixture.provider,
    )
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    assert!(
        fixture
            .client
            .fetch_blob(fixture.hash, None)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(started.elapsed(), Duration::ZERO);

    fixture
        .net
        .partition(fixture.client.my_id, fixture.provider);
    let error = fixture
        .client
        .fetch_blob(fixture.hash, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no remote replica"));
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn stalled_bootstrap_still_exhausts_the_one_foreground_deadline() {
    let mut fixture = Fixture::new(true);
    fixture.net.stall_dials(fixture.provider);
    let started = tokio::time::Instant::now();
    assert!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await
            .is_none()
    );
    assert_eq!(started.elapsed(), INTERACTIVE_FETCH_DEADLINE);
    fixture.assert_no_control_effects();
}
