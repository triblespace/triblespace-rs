//! Exact-H acquisition through the production DHT and bearer RPC handlers.
//!
//! Connection latency and caller deadlines use a paused Tokio timeline.
//! Directory leases remain unexpired; publication retries receive explicit
//! monotonic instants and require no global virtual clock.
//! The shared unit-test guard excludes body receivers on other test runtimes,
//! whose independent clocks cannot make progress on this paused timeline.

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
    provider_routes: Arc<Mutex<RoutingTable>>,
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
        let provider_routes = Arc::new(Mutex::new(RoutingTable::new(provider, [])));
        let handler = SnapshotHandler {
            snapshot: Arc::new(Mutex::new(Some(Arc::new(snapshot)))),
            candidates: provider_routes.clone(),
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
            provider_routes,
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
    let _guard = crate::protocol::exact_blob_receive_test_guard();
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
    let _guard = crate::protocol::exact_blob_receive_test_guard();
    let mut fixture = Fixture::new(true);
    let stalled_key = SigningKey::from_bytes(&[93; 32]);
    let stalled = stalled_key.verifying_key().to_bytes();
    let _stalled_harness = fixture.net.join(&stalled_key);
    fixture.net.stall_dials(stalled);
    let learned_key = SigningKey::from_bytes(&[94; 32]);
    let learned = learned_key.verifying_key().to_bytes();
    let _learned_harness = fixture.net.join(&learned_key);
    fixture.net.stall_dials(learned);
    *fixture.client.candidates.lock().unwrap() =
        RoutingTable::new(fixture.client.my_id, [fixture.provider, stalled]);
    fixture
        .client
        .candidates
        .lock()
        .unwrap()
        .promote_authenticated(learned);

    let started = tokio::time::Instant::now();
    assert_eq!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await,
        Some(fixture.bytes.clone()),
    );
    assert_eq!(started.elapsed(), Duration::from_secs(7));
    {
        let routes = fixture.client.candidates.lock().unwrap();
        assert_eq!(
            routes.state(fixture.provider),
            Some(crate::routing::RouteState::Verified)
        );
        assert_eq!(
            routes.state(stalled),
            Some(crate::routing::RouteState::Candidate)
        );
        assert_eq!(routes.state(learned), None);
    }
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn configured_only_cold_background_lookup_keeps_its_short_bound() {
    let _guard = crate::protocol::exact_blob_receive_test_guard();
    let mut fixture = Fixture::new(true);
    // A configured-only four-second cold dial still cannot complete within
    // the unchanged three-second background window. Failure must retain the
    // configured route, but does not itself warm a cancelled SimNet dial.
    for attempt in 1..=3 {
        let started = tokio::time::Instant::now();
        let error = fixture
            .client
            .fetch_blob(fixture.hash, Some(BACKGROUND_LOOKUP_DEADLINE))
            .await
            .unwrap_err();
        assert_eq!(started.elapsed(), BACKGROUND_LOOKUP_DEADLINE);
        assert!(error.to_string().contains("no remote replica"));
        assert_eq!(
            fixture
                .client
                .candidates
                .lock()
                .unwrap()
                .closest(fixture.hash, K),
            vec![fixture.provider]
        );
        assert_eq!(
            fixture
                .net
                .dial_count(fixture.client.my_id, fixture.provider),
            attempt
        );
    }
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn repeated_directory_gossip_does_not_erase_recent_local_route_failure() {
    let _guard = crate::protocol::exact_blob_receive_test_guard();
    let mut fixture = Fixture::new(false);
    let key = blob_locator(fixture.hash);
    // Warm only the reachable configured directory, so setup latency cannot
    // explain any subsequent full background routing window.
    fixture
        .client
        .find_node(fixture.provider, key)
        .await
        .unwrap();
    let stalled_key = SigningKey::from_bytes(&[93; 32]);
    let stalled = stalled_key.verifying_key().to_bytes();
    let _stalled_harness = fixture.net.join(&stalled_key);
    fixture.net.stall_dials(stalled);
    // The directory still has old direct-liveness evidence, so its production
    // FIND_NODE handler re-advertises this unreachable route on every reply.
    assert!(
        fixture
            .provider_routes
            .lock()
            .unwrap()
            .promote_authenticated(stalled)
    );
    let token = blob_provider_token(fixture.hash, fixture.client.my_id);
    let mut elapsed = Vec::new();
    let mut stalled_dials = Vec::new();
    for _ in 0..3 {
        let started = tokio::time::Instant::now();
        assert_eq!(
            fixture.client.announce_key(key, token).await,
            PublicationResult::Published
        );
        elapsed.push(started.elapsed());
        stalled_dials.push(fixture.net.dial_count(fixture.client.my_id, stalled));
    }
    fixture.assert_no_control_effects();
    // Dropping local evidence at timeout lets unverified gossip immediately
    // restore the stalled route, even though all three publications get ACKs.
    assert_eq!(
        elapsed,
        [BACKGROUND_LOOKUP_DEADLINE, Duration::ZERO, Duration::ZERO],
        "unchanged remote gossip must not repeatedly consume the routing window; stalled dial counts: {stalled_dials:?}"
    );
    assert_eq!(stalled_dials, [1, 1, 1]);
}

#[tokio::test(start_paused = true)]
async fn background_publication_retries_past_a_stale_issued_batch() {
    let _guard = crate::protocol::exact_blob_receive_test_guard();
    let mut fixture = Fixture::new(false);
    let key = blob_locator(fixture.hash);
    // The configured sibling is already responsive. Three learned stale
    // identities closer to this exact locator occupy the whole first batch.
    fixture
        .client
        .find_node(fixture.provider, key)
        .await
        .unwrap();
    let stale_keys: Vec<_> = (0u8..=255)
        .map(|byte| SigningKey::from_bytes(&[byte; 32]))
        .filter(|signer| {
            let peer = signer.verifying_key().to_bytes();
            peer != fixture.client.my_id
                && crate::routing::distance_cmp(key, peer, fixture.provider).is_lt()
        })
        .take(ALPHA)
        .collect();
    assert_eq!(stale_keys.len(), ALPHA);
    let stale: Vec<_> = stale_keys
        .iter()
        .map(|signer| signer.verifying_key().to_bytes())
        .collect();
    let _stale_harnesses: Vec<_> = stale_keys
        .iter()
        .map(|signer| fixture.net.join(signer))
        .collect();
    for peer in &stale {
        fixture.net.stall_dials(*peer);
        assert!(
            fixture
                .client
                .candidates
                .lock()
                .unwrap()
                .promote_authenticated(*peer)
        );
    }
    assert_eq!(
        fixture.client.candidates.lock().unwrap().closest(key, K)[ALPHA],
        fixture.provider
    );

    let now = crate::clock::mono_now();
    let locators = locator_index(&fixture.store.snapshot().unwrap()).unwrap();
    let mut publisher = ProviderPublisher::new(now);
    publisher.install(
        ProviderObservation::from_locators([], false, &locators).into_set(),
        now,
    );
    let work = publisher.next(now).expect("initial publication attempt");
    assert_eq!((work.key, work.identity), (key, fixture.hash));
    let token = blob_provider_token(fixture.hash, fixture.client.my_id);
    let started = tokio::time::Instant::now();
    let result = fixture.client.announce_key(key, token).await;
    assert_eq!(started.elapsed(), BACKGROUND_LOOKUP_DEADLINE);
    assert_eq!(result, PublicationResult::NoAuthenticatedRemoteReplica);
    let completed = now + started.elapsed();
    assert!(
        publisher
            .complete(work, result, completed)
            .topology_outage_started
    );
    assert_eq!(publisher.next(completed), None);

    let retry_at = completed + crate::RETRY_BACKOFF_BASE;
    let retry = publisher.next(retry_at).expect("topology retry probe");
    assert_eq!((retry.key, retry.identity), (key, fixture.hash));
    let retry_started = tokio::time::Instant::now();
    let result = fixture.client.announce_key(key, token).await;
    assert_eq!(result, PublicationResult::Published);
    assert_eq!(retry_started.elapsed(), Duration::ZERO);
    assert!(
        publisher
            .complete(retry, result, retry_at)
            .topology_recovered
    );
    assert_eq!(publisher.next(retry_at), None);
    assert_eq!(
        fixture.client.get(fixture.provider, key).await.unwrap(),
        vec![(fixture.client.my_id, token)]
    );
    for peer in stale {
        assert_eq!(fixture.client.candidates.lock().unwrap().state(peer), None);
        assert_eq!(fixture.net.dial_count(fixture.client.my_id, peer), 1);
    }
    fixture.assert_no_control_effects();
}

#[tokio::test(start_paused = true)]
async fn warm_provider_miss_is_not_a_transport_failure_or_direct_peer_probe() {
    let _guard = crate::protocol::exact_blob_receive_test_guard();
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
    let _guard = crate::protocol::exact_blob_receive_test_guard();
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

#[tokio::test(start_paused = true)]
async fn exact_receive_contention_remains_inside_the_caller_deadline() {
    use tokio::io::AsyncWriteExt as _;

    let _guard = crate::protocol::exact_blob_receive_test_guard();
    let mut fixture = Fixture::new(true);
    let (mut writer, mut reader) = tokio::io::duplex(1);
    let blocked = crate::protocol::recv_exact_blob_body(&mut reader, 1);
    tokio::pin!(blocked);
    // The receive owns the process-wide permit, then waits for its one byte.
    assert!(futures::poll!(&mut blocked).is_pending());

    let started = tokio::time::Instant::now();
    assert!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await
            .is_none()
    );
    assert_eq!(started.elapsed(), INTERACTIVE_FETCH_DEADLINE);
    assert_eq!(
        fixture
            .client
            .candidates
            .lock()
            .unwrap()
            .state(fixture.provider),
        Some(crate::routing::RouteState::Verified),
        "bootstrap authenticated before waiting for the local receive slot"
    );

    writer.write_all(b"x").await.unwrap();
    assert_eq!(blocked.await.unwrap().as_ref(), b"x");
    let started = tokio::time::Instant::now();
    assert_eq!(
        fixture
            .sender
            .fetch_blob(fixture.hash, INTERACTIVE_FETCH_DEADLINE)
            .await,
        Some(fixture.bytes.clone()),
    );
    assert_eq!(started.elapsed(), Duration::ZERO);
    fixture.assert_no_control_effects();
}
