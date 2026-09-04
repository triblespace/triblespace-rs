//! Collection-scoped network host.
//!
//! TLS authenticates endpoint identities, but establishing a transport
//! connection grants no team or collection authority. Each semantic repair is
//! one stream admitted from the server's complete local READ(C) closure. A
//! client's bounded native-proof bootstrap is retained as inert authorization
//! evidence for later coherent observations. DHT routing and provider-directory
//! operations discover collection participants through an opaque KDF(C). Exact bytes use
//! a separate H-only DHT rendezvous and mutual key-confirmation stream;
//! collection identity never participates.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures::{StreamExt as _, stream::FuturesUnordered};
use iroh_base::{EndpointAddr, EndpointId};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tracing::{Instrument as _, debug, debug_span, info_span, warn};
use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::{UnknownBlob, simplearchive::SimpleArchive};
use triblespace_core::capability::CapabilityProof;
use triblespace_core::collection::CollectionHandle;
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::patch::{Entry as PatchEntry, IdentitySchema, PATCH};
use triblespace_core::repo::{BlobStoreGet, StoreChanges, StoreRead};

use crate::bearer::{BearerLocatorIndex, blob_locator, locator_index, update_locator_index};
use crate::channel::{NetCommand, NetEvent, NetEventBatch, SnapshotNotice};
use crate::collection_activation::{
    CollectionReadBootstrapError, CollectionRepairOverlay, CollectionRepairOverlayError,
    collection_read_bootstrap_proofs_at, collection_repair_overlay,
};
use crate::collection_session::{pull_collection, serve_collection_repair};
use crate::collection_wire::{MAX_COLLECTION_READ_BOOTSTRAP_PROOFS, OP_COLLECTION_REPAIR};
use crate::identity::iroh_secret;
use crate::inventory::ReconcileQos;
use crate::protocol::{
    OP_FIND_NODE, OP_GET_BLOB, OP_PROVIDER_GET, OP_PROVIDER_PUT, PILE_SYNC_ALPN, PROVIDER_PUT_FULL,
    PROVIDER_PUT_OK, RawHash, op_find_node, op_get_blob, op_provider_get, op_provider_put,
    recv_hash, recv_u8, send_hash, send_u8, serve_get_blob,
};
use crate::provider::{
    ProviderDirectory, ProviderKey, ProviderObservation, ProviderPublisher, ProviderPutResult,
    ProviderToken, PublicationResult, blob_provider_token, collection_provider_key,
    collection_provider_token, provider_lease_token,
};
use crate::routing::{ALPHA, IterativeLookup, K, RoutingKey, RoutingTable};
use crate::transport::{Conn, Harness, PeerId, Transport};
use crate::wake::{
    CollectionWakeEvent, CollectionWakeNetwork, CollectionWakePlane, CollectionWakeRoot,
    CollectionWakeSubscription, ReceivedCollectionWake,
};

/// Ephemeral local collection interest. It is deliberately not a durable
/// marker or ambient registry.
pub(crate) type ActiveCollections = PATCH<32, IdentitySchema>;

/// Transport and local scheduling configuration.
///
/// Collection authority is intentionally absent. A connection is ordinary
/// mutually authenticated TLS; READ authority is supplied on each collection
/// repair stream.
#[derive(Clone)]
pub struct PeerConfig {
    /// Bootstrap endpoint routes.
    pub peers: Vec<EndpointAddr>,
    /// Local pull/serve scheduling choices. Never sent as authority.
    pub qos: ReconcileQos,
    /// Maximum DHT provider-announcement attempts during this process.
    ///
    /// `None` preserves the ordinary unlimited scheduler. `Some(0)` disables
    /// announcements without disabling exact H-authorized serving. Retries and
    /// renewals consume the same budget as first publication.
    pub provider_publication_budget: Option<u64>,
}

trait BlobSnapshotReader: Send + Sync + 'static {
    fn get_blob(&self, hash: RawHash) -> Option<Bytes>;
}

struct CloneableBlobSnapshotReader<R>(Mutex<R>);

impl<R> BlobSnapshotReader for CloneableBlobSnapshotReader<R>
where
    R: BlobStoreGet + Clone + Send + 'static,
{
    fn get_blob(&self, hash: RawHash) -> Option<Bytes> {
        let reader = self.0.lock().unwrap().clone();
        reader
            .get::<Bytes, UnknownBlob>(Inline::<Handle<UnknownBlob>>::new(hash))
            .ok()
    }
}

/// One collection's immutable server overlay plus the request evidence this
/// endpoint will present when it pulls the same collection.
pub(crate) struct CollectionSnapshot {
    repair: Arc<CollectionRepairOverlay>,
    read_bootstrap: Arc<[CapabilityProof]>,
}

impl CollectionSnapshot {
    pub(crate) fn collection(&self) -> CollectionHandle {
        self.repair.collection()
    }

    fn wake_root(&self) -> [u8; 32] {
        self.repair.wake_root()
    }
}

type CollectionSnapshotIndex = PATCH<32, IdentitySchema, Arc<CollectionSnapshot>>;

/// Immutable host observation indexed exactly by active collection handle.
///
/// Each value pins the repair product: record PATCH × native
/// authorization-evidence PATCH. No global team inventory, proof list, or
/// blob manifest is retained.
pub(crate) struct StoreSnapshot {
    collections: CollectionSnapshotIndex,
    blobs: Arc<dyn BlobSnapshotReader>,
    bearer_locators: Arc<BearerLocatorIndex>,
    observed_at: hifitime::Epoch,
    next_authorization_change: Option<hifitime::Epoch>,
}

impl StoreSnapshot {
    pub(crate) fn from_store_changes<R>(
        snapshot: R,
        active: &ActiveCollections,
        local: VerifyingKey,
        previous_store: Option<&R>,
        previous: Option<&Self>,
        changes: StoreChanges,
        authorization_changed: bool,
        next_authorization_change: Option<hifitime::Epoch>,
        instant: hifitime::Epoch,
    ) -> anyhow::Result<Self>
    where
        R: StoreRead + Clone,
    {
        let mut collections = CollectionSnapshotIndex::new();
        let bearer_locators = match (previous_store, previous) {
            (Some(previous_store), Some(previous)) if changes.contains(StoreChanges::BLOBS) => {
                Arc::new(update_locator_index(
                    &snapshot,
                    previous_store,
                    &previous.bearer_locators,
                )?)
            }
            (_, Some(previous)) => previous.bearer_locators.clone(),
            _ => Arc::new(locator_index(&snapshot)?),
        };
        let blob_reader: Arc<dyn BlobSnapshotReader> =
            Arc::new(CloneableBlobSnapshotReader(Mutex::new(snapshot.clone())));
        let repair_inputs_changed = changes.contains(StoreChanges::BLOBS)
            || changes.contains(StoreChanges::COLLECTION_RECORDS)
            || changes.contains(StoreChanges::CAPABILITY_PROOFS)
            || authorization_changed;
        for raw in active.iter_ordered() {
            let collection = CollectionHandle::new(*raw);
            if snapshot
                .get::<Blob<SimpleArchive>, SimpleArchive>(Inline::new(collection.raw))
                .is_err()
            {
                warn!(collection = %hex::encode(&collection.raw[..4]), "active collection descriptor unavailable; isolating pending collection");
                continue;
            }
            let prior = previous.and_then(|prior| prior.collection(collection));
            let repair_result = if !repair_inputs_changed {
                prior
                    .as_ref()
                    .map(|prior| prior.repair.clone())
                    .map_or_else(
                        || collection_repair_overlay(&snapshot, collection).map(Arc::new),
                        Ok,
                    )
            } else {
                collection_repair_overlay(&snapshot, collection).map(|fresh| {
                    prior
                        .as_ref()
                        .filter(|prior| prior.wake_root() == fresh.wake_root())
                        .map_or_else(|| Arc::new(fresh), |prior| prior.repair.clone())
                })
            };
            let repair = match repair_result {
                Ok(repair) => repair,
                Err(CollectionRepairOverlayError::Descriptor(error)) => {
                    warn!(collection = %hex::encode(&collection.raw[..4]), %error, "active collection descriptor is unavailable or invalid; isolating collection");
                    continue;
                }
                Err(error) => return Err(anyhow::Error::new(error)),
            };
            let read_bootstrap = if !repair_inputs_changed
                && !authorization_changed
                && prior.is_some()
            {
                prior.as_ref().unwrap().read_bootstrap.clone()
            } else {
                match collection_read_bootstrap_proofs_at(
                    &snapshot,
                    collection,
                    local,
                    MAX_COLLECTION_READ_BOOTSTRAP_PROOFS,
                    instant,
                ) {
                    Ok(evidence) => evidence.into(),
                    Err(CollectionReadBootstrapError::TooMany { count, limit }) => {
                        warn!(
                            collection = %hex::encode(&collection.raw[..4]),
                            count,
                            limit,
                            "collection READ bootstrap exceeds network bound; collection remains locally active but cannot bootstrap a cold remote"
                        );
                        Arc::from([])
                    }
                    Err(error) => return Err(anyhow::Error::new(error)),
                }
            };
            let value = Arc::new(CollectionSnapshot {
                repair,
                read_bootstrap,
            });
            collections.insert(&PatchEntry::with_value(raw, value));
        }
        Ok(Self {
            collections,
            blobs: blob_reader,
            bearer_locators,
            observed_at: instant,
            next_authorization_change,
        })
    }

    fn time_valid(&self) -> bool {
        let now = crate::clock::epoch_now();
        now >= self.observed_at
            && !self
                .next_authorization_change
                .is_some_and(|boundary| now >= boundary)
    }

    fn collection(&self, collection: CollectionHandle) -> Option<Arc<CollectionSnapshot>> {
        self.time_valid()
            .then(|| self.collections.get(&collection.raw).cloned())
            .flatten()
    }

    pub(crate) fn collections(&self) -> impl Iterator<Item = Arc<CollectionSnapshot>> + '_ {
        let valid = self.time_valid();
        self.collections
            .iter_ordered()
            .filter_map(move |key| valid.then(|| self.collections.get(key).cloned()).flatten())
    }

    fn notices(&self) -> Vec<(CollectionHandle, [u8; 32])> {
        self.collections()
            .map(|collection| (collection.collection(), collection.wake_root()))
            .collect()
    }

    fn get_blob(&self, hash: &RawHash) -> Option<Bytes> {
        self.blobs.get_blob(*hash)
    }

    fn bearer_handle(&self, locator: RawHash) -> Option<RawHash> {
        self.bearer_locators.get(&locator).copied()
    }

    pub(crate) fn bearer_locators(&self) -> &BearerLocatorIndex {
        &self.bearer_locators
    }
}

type SharedSnapshot = Arc<StoreSnapshot>;
type SnapshotSlot = Arc<Mutex<Option<SharedSnapshot>>>;

/// The async capability cloned into lazy readers.
pub(crate) trait NetCapability: Send + Sync {
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Bytes>>;
}

type RoutingCandidates = Arc<Mutex<RoutingTable>>;
const MAX_COLLECTION_PARTICIPANTS: usize = 128;
const COLLECTION_PARTICIPANT_LEASE: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const PERIODIC_REPAIR_SAMPLE: usize = 8;

/// Recovery state for one collection-provider rendezvous.
///
/// DHT discovery is a way into a collection, not its heartbeat. Once a leased
/// repair candidate exists, signed wakes and periodic exact repair keep that
/// lease alive without repeating the lookup traversal.
#[derive(Clone, Copy, Debug)]
struct DiscoveryState {
    in_flight: bool,
    attempts: u32,
    retry_at: crate::clock::Mono,
}

impl DiscoveryState {
    fn new(now: crate::clock::Mono) -> Self {
        Self {
            in_flight: false,
            attempts: 0,
            retry_at: now,
        }
    }

    fn start_if_due(&mut self, now: crate::clock::Mono, has_candidate: bool) -> bool {
        if self.in_flight || has_candidate || now < self.retry_at {
            return false;
        }
        self.in_flight = true;
        true
    }

    fn finish_attempt(&mut self, now: crate::clock::Mono) {
        self.in_flight = false;
        let shift = self.attempts.min(6);
        self.attempts = self.attempts.saturating_add(1);
        self.retry_at = now
            + crate::RETRY_BACKOFF_BASE
                .saturating_mul(1u32 << shift)
                .min(crate::RETRY_BACKOFF_CAP);
    }

    fn observe_success(&mut self, now: crate::clock::Mono) {
        self.attempts = 0;
        self.retry_at = now;
    }
}

/// Configured peers remain permanent bootstrap routes. Recently learned DHT
/// participants are a bounded, recency-ordered supplement which survives a
/// stock-gossip topic resubscription.
struct WakeBootstrapPeers {
    configured: Vec<EndpointId>,
    learned: VecDeque<EndpointId>,
}

impl WakeBootstrapPeers {
    fn new(configured: Vec<EndpointId>) -> Self {
        let mut unique = Vec::with_capacity(configured.len());
        for peer in configured {
            if !unique.contains(&peer) {
                unique.push(peer);
            }
        }
        Self {
            configured: unique,
            learned: VecDeque::new(),
        }
    }

    fn remember(&mut self, peers: impl IntoIterator<Item = EndpointId>) {
        for peer in peers {
            if self.configured.contains(&peer) {
                continue;
            }
            if let Some(position) = self.learned.iter().position(|known| *known == peer) {
                self.learned.remove(position);
            }
            self.learned.push_back(peer);
            if self.learned.len() > MAX_COLLECTION_PARTICIPANTS {
                self.learned.pop_front();
            }
        }
    }

    fn current(&self) -> Vec<EndpointId> {
        self.configured
            .iter()
            .chain(self.learned.iter())
            .copied()
            .collect()
    }

    /// Whether reopening the topic has any bootstrap route to retry.
    ///
    /// Configured endpoints remain generic topology seeds rather than repair
    /// participants; they are nevertheless valid routes through which the
    /// collection topic can reconnect to its mesh.
    fn has_recovery_route(&self) -> bool {
        !self.configured.is_empty() || !self.learned.is_empty()
    }
}

fn observe_participant(
    participants: &mut HashMap<[u8; 32], HashMap<PeerId, crate::clock::Mono>>,
    collection: [u8; 32],
    peer: PeerId,
    now: crate::clock::Mono,
) {
    let peers = participants.entry(collection).or_default();
    peers.retain(|_, seen| now.duration_since(*seen) <= COLLECTION_PARTICIPANT_LEASE);
    if !peers.contains_key(&peer)
        && peers.len() >= MAX_COLLECTION_PARTICIPANTS
        && let Some(oldest) = peers
            .iter()
            .min_by_key(|(peer, seen)| (**seen, **peer))
            .map(|(peer, _)| *peer)
    {
        peers.remove(&oldest);
    }
    peers.insert(peer, now);
}

fn live_participants(
    participants: &mut HashMap<[u8; 32], HashMap<PeerId, crate::clock::Mono>>,
    collection: [u8; 32],
    now: crate::clock::Mono,
) -> Vec<PeerId> {
    let Some(peers) = participants.get_mut(&collection) else {
        return Vec::new();
    };
    peers.retain(|_, seen| now.duration_since(*seen) <= COLLECTION_PARTICIPANT_LEASE);
    let mut live = peers.keys().copied().collect::<Vec<_>>();
    live.sort_unstable();
    live
}

fn forget_participant(
    participants: &mut HashMap<[u8; 32], HashMap<PeerId, crate::clock::Mono>>,
    collection: [u8; 32],
    peer: PeerId,
) -> bool {
    let empty = participants.get_mut(&collection).is_some_and(|peers| {
        peers.remove(&peer);
        peers.is_empty()
    });
    if empty {
        participants.remove(&collection);
    }
    empty
}

struct PoolEntry<C> {
    connection: tokio::sync::OnceCell<Result<C, Arc<anyhow::Error>>>,
}

impl<C> Default for PoolEntry<C> {
    fn default() -> Self {
        Self {
            connection: tokio::sync::OnceCell::new(),
        }
    }
}

#[derive(Clone)]
struct PooledConnection<C> {
    entry: Arc<PoolEntry<C>>,
    connection: C,
}

impl<C> PooledConnection<C> {
    fn conn(&self) -> &C {
        &self.connection
    }
}

struct ConnectionPool<C> {
    entries: HashMap<PeerId, Arc<PoolEntry<C>>>,
    least_to_most_recent: VecDeque<PeerId>,
}

impl<C> ConnectionPool<C> {
    fn entry(&mut self, peer: PeerId) -> Arc<PoolEntry<C>> {
        self.entries
            .entry(peer)
            .or_insert_with(|| Arc::new(PoolEntry::default()))
            .clone()
    }

    fn admit(&mut self, peer: PeerId, expected: &Arc<PoolEntry<C>>) -> Option<Arc<PoolEntry<C>>> {
        if !self
            .entries
            .get(&peer)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            return None;
        }
        self.least_to_most_recent
            .retain(|candidate| *candidate != peer);
        self.least_to_most_recent.push_back(peer);
        if self.least_to_most_recent.len() <= MAX_CONNECTIONS {
            return None;
        }
        let oldest = self.least_to_most_recent.pop_front().unwrap();
        self.entries.remove(&oldest)
    }

    fn remove_if(
        &mut self,
        peer: PeerId,
        expected: &Arc<PoolEntry<C>>,
    ) -> Option<Arc<PoolEntry<C>>> {
        if !self
            .entries
            .get(&peer)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            return None;
        }
        self.least_to_most_recent
            .retain(|candidate| *candidate != peer);
        self.entries.remove(&peer)
    }
}

type SharedPool<C> = Arc<Mutex<ConnectionPool<C>>>;

fn new_shared_pool<C>() -> SharedPool<C> {
    Arc::new(Mutex::new(ConnectionPool {
        entries: HashMap::new(),
        least_to_most_recent: VecDeque::new(),
    }))
}

async fn pool_get<T: Transport>(
    transport: &T,
    pool: &SharedPool<T::Conn>,
    peer: PeerId,
) -> anyhow::Result<PooledConnection<T::Conn>> {
    let entry = pool.lock().unwrap().entry(peer);
    let initialized = entry
        .connection
        .get_or_init(|| async {
            tokio::time::timeout(DIAL_DEADLINE, transport.dial(peer, PILE_SYNC_ALPN))
                .await
                .map_err(|_| anyhow::anyhow!("connection setup deadline exceeded"))
                .and_then(|result| result)
                .and_then(|connection| {
                    if connection.remote_id() != peer {
                        anyhow::bail!("dialed endpoint identity does not match requested peer")
                    }
                    Ok(connection)
                })
                .map_err(Arc::new)
        })
        .await;
    let connection = match initialized {
        Ok(connection) => connection.clone(),
        Err(error) => {
            pool.lock().unwrap().remove_if(peer, &entry);
            return Err(anyhow::anyhow!(error.to_string()));
        }
    };
    drop(pool.lock().unwrap().admit(peer, &entry));
    Ok(PooledConnection { entry, connection })
}

fn pool_invalidate<C: Conn>(pool: &SharedPool<C>, peer: PeerId, entry: &Arc<PoolEntry<C>>) {
    let removed = pool.lock().unwrap().remove_if(peer, entry);
    if removed.is_some()
        && let Some(Ok(connection)) = entry.connection.get()
    {
        connection.close(0, b"pool evict");
    }
}

#[derive(Clone)]
struct ProviderClient<T: Transport> {
    transport: T,
    pool: SharedPool<T::Conn>,
    providers: Arc<Mutex<ProviderDirectory>>,
    candidates: RoutingCandidates,
    my_id: PeerId,
}

struct NetCap<T: Transport> {
    client: ProviderClient<T>,
}

impl<T: Transport> NetCapability for NetCap<T> {
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Bytes>> {
        let client = self.client.clone();
        Box::pin(async move { client.fetch_blob(hash).await })
    }
}

/// Default end-to-end budget for an interactive exact blob read.
pub const INTERACTIVE_FETCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub struct NetSender {
    cmd_tx: mpsc::Sender<NetCommand>,
    snapshot: SnapshotSlot,
    cap: tokio::sync::watch::Receiver<Option<Arc<dyn NetCapability>>>,
    id: EndpointId,
}

impl NetSender {
    pub fn id(&self) -> EndpointId {
        self.id
    }

    pub(crate) fn current_snapshot(&self) -> Option<SharedSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }

    pub(crate) fn update_snapshot(&self, snapshot: StoreSnapshot, active: &ActiveCollections) {
        let mut notices = snapshot.notices();
        for raw in active.iter_ordered() {
            if !notices.iter().any(|(collection, _)| collection.raw == *raw) {
                notices.push((CollectionHandle::new(*raw), [0; 32]));
            }
        }
        let retired = self.snapshot.lock().unwrap().replace(Arc::new(snapshot));
        drop(retired);
        let _ = self
            .cmd_tx
            .send(NetCommand::SnapshotChanged(SnapshotNotice {
                collections: notices,
                installed: true,
            }));
    }

    pub(crate) fn update_providers(&self, providers: ProviderObservation) {
        let _ = self.cmd_tx.send(NetCommand::ProvidersUpdated(providers));
    }

    pub fn clear_snapshot(&self) {
        let had_snapshot = self.snapshot.lock().unwrap().take().is_some();
        if had_snapshot {
            let _ = self
                .cmd_tx
                .send(NetCommand::SnapshotChanged(SnapshotNotice {
                    collections: Vec::new(),
                    installed: false,
                }));
        }
        self.update_providers(ProviderObservation::default());
    }

    async fn ready_capability(&self) -> anyhow::Result<Arc<dyn NetCapability>> {
        let mut slot = self.cap.clone();
        loop {
            if let Some(capability) = slot.borrow().clone() {
                return Ok(capability);
            }
            slot.changed()
                .await
                .map_err(|_| anyhow::anyhow!("network host stopped before becoming ready"))?;
        }
    }

    pub async fn fetch_blob(&self, hash: RawHash, budget: std::time::Duration) -> Option<Bytes> {
        tokio::time::timeout(budget, async {
            self.ready_capability().await.ok()?.fetch_blob(hash).await
        })
        .await
        .ok()
        .flatten()
    }
}

pub struct NetReceiver {
    evt_rx: tokio::sync::mpsc::Receiver<NetEventBatch>,
}

impl NetReceiver {
    pub(crate) fn try_recv(&mut self) -> Option<NetEventBatch> {
        self.evt_rx.try_recv().ok()
    }
}

pub struct HostWiring {
    cmd_rx: mpsc::Receiver<NetCommand>,
    evt_tx: tokio::sync::mpsc::Sender<NetEventBatch>,
    snapshot: SnapshotSlot,
    cap_tx: tokio::sync::watch::Sender<Option<Arc<dyn NetCapability>>>,
}

#[cfg(test)]
impl HostWiring {
    pub(crate) async fn send_admission(&self, batch: NetEventBatch) {
        self.evt_tx.send(batch).await.unwrap();
    }
}

pub fn wire(id: EndpointId) -> (NetSender, NetReceiver, HostWiring) {
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (evt_tx, evt_rx) = tokio::sync::mpsc::channel(crate::channel::MAX_ADMISSION_BRIDGE_BATCHES);
    let snapshot = Arc::new(Mutex::new(None));
    let (cap_tx, cap_rx) = tokio::sync::watch::channel(None);
    (
        NetSender {
            cmd_tx,
            snapshot: snapshot.clone(),
            cap: cap_rx,
            id,
        },
        NetReceiver { evt_rx },
        HostWiring {
            cmd_rx,
            evt_tx,
            snapshot,
            cap_tx,
        },
    )
}

pub async fn run_host<T: Transport>(harness: Harness<T>, config: PeerConfig, wiring: HostWiring) {
    host_loop(harness, config, wiring).await;
}

pub fn spawn(
    key: SigningKey,
    config: PeerConfig,
) -> anyhow::Result<(NetSender, NetReceiver, CollectionWakePlane)> {
    let secret = iroh_secret(&key);
    let id: EndpointId = secret.public().into();
    let (sender, receiver, wiring) = wire(id);
    let (startup_tx, startup_rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("triblespace-net".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = startup_tx.send(Err(anyhow::Error::new(error)));
                    return;
                }
            };
            runtime.block_on(async move {
                let harness = match crate::transport::iroh::bind(secret, &config).await {
                    Ok(harness) => harness,
                    Err(error) => {
                        let _ = startup_tx.send(Err(error));
                        return;
                    }
                };
                let wake_plane = harness.transport.wake_plane();
                if startup_tx.send(Ok(wake_plane)).is_ok() {
                    run_host(harness, config, wiring).await;
                }
            });
        })?;
    let wake_plane = startup_rx
        .recv()
        .map_err(|_| anyhow::anyhow!("network host stopped during startup"))??;
    Ok((sender, receiver, wake_plane))
}

const DIAL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const OP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
const REPAIR_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);
const REPAIR_PERIOD: std::time::Duration = std::time::Duration::from_secs(30);
const HOST_POLL_PERIOD: std::time::Duration = std::time::Duration::from_millis(10);
const CONNECTION_IDLE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(120);
const REQUEST_DEADLINE: std::time::Duration = std::time::Duration::from_secs(300);
const MAX_CONNECTIONS: usize = 64;
const MAX_REQUESTS_PER_CONNECTION: usize = 16;
const MAX_REQUESTS_GLOBAL: usize = 16;
const MAX_CONCURRENT_REPAIRS: usize = 8;
const MAX_PENDING_REPAIRS: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RepairTarget {
    collection: CollectionHandle,
    peer: PeerId,
}

struct RepairOutcome {
    target: RepairTarget,
    success: bool,
    more: bool,
}

struct PublicationOutcome {
    key: ProviderKey,
    result: PublicationResult,
}

/// Process-lifetime admission gate for DHT provider announcements.
///
/// This sits outside [`ProviderPublisher`], so installing a newer resident
/// snapshot cannot refill a canary's network budget.
struct ProviderPublicationBudget {
    remaining: Option<u64>,
}

impl ProviderPublicationBudget {
    fn new(limit: Option<u64>) -> Self {
        Self { remaining: limit }
    }

    fn permits_attempt(&self) -> bool {
        self.remaining != Some(0)
    }

    fn consume_attempt(&mut self) {
        if let Some(remaining) = &mut self.remaining {
            debug_assert!(*remaining > 0);
            *remaining -= 1;
        }
    }

    fn is_exhausted(&self) -> bool {
        self.remaining == Some(0)
    }
}

fn enqueue_repair(
    queue: &mut VecDeque<RepairTarget>,
    pending: &mut HashSet<RepairTarget>,
    target: RepairTarget,
) {
    if pending.len() < MAX_PENDING_REPAIRS && pending.insert(target) {
        queue.push_back(target);
    }
}

fn has_repair_candidate(
    collection: CollectionHandle,
    peers: &[PeerId],
    failures: &HashMap<RepairTarget, (u32, crate::clock::Mono)>,
    local_peer: PeerId,
) -> bool {
    peers.iter().any(|peer| {
        *peer != local_peer
            && !failures.contains_key(&RepairTarget {
                collection,
                peer: *peer,
            })
    })
}

fn retain_active_repair_state<T>(state: &mut HashMap<RepairTarget, T>, active: &HashSet<[u8; 32]>) {
    state.retain(|target, _| active.contains(&target.collection.raw));
}

enum WakeCommand {
    Observe(CollectionWakeRoot),
    Join(Vec<EndpointId>),
    Resubscribe,
    Shutdown,
}

enum WakeNotice {
    Received {
        collection: CollectionHandle,
        received: ReceivedCollectionWake,
    },
    Lagged {
        collection: CollectionHandle,
    },
}

fn spawn_wake_topic<P: CollectionWakeNetwork>(
    plane: P,
    collection: CollectionHandle,
    bootstrap: Vec<EndpointId>,
    notices: tokio::sync::mpsc::Sender<WakeNotice>,
) -> tokio::sync::mpsc::UnboundedSender<WakeCommand> {
    let (commands, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut current_root = None;
        let mut bootstrap = WakeBootstrapPeers::new(bootstrap);
        loop {
            let mut topic = loop {
                match plane
                    .subscribe_network(collection, bootstrap.current())
                    .await
                {
                    Ok(topic) => break topic,
                    Err(error) => {
                        debug!(%error, "collection gossip subscription failed; retrying");
                        tokio::select! {
                            command = command_rx.recv() => match command {
                                Some(WakeCommand::Observe(root)) => current_root = Some(root),
                                Some(WakeCommand::Join(peers)) => bootstrap.remember(peers),
                                Some(WakeCommand::Resubscribe) => {}
                                Some(WakeCommand::Shutdown) | None => return,
                            },
                            () = tokio::time::sleep(crate::RETRY_BACKOFF_BASE) => {}
                        }
                    }
                }
            };
            if let Some(root) = current_root
                && let Err(error) = topic.broadcast_wake(root).await
            {
                debug!(%error, "collection wake broadcast after subscribe failed");
            }
            'events: loop {
                tokio::select! {
                    command = command_rx.recv() => match command {
                        Some(WakeCommand::Observe(root)) => {
                            current_root = Some(root);
                            if let Err(error) = topic.broadcast_wake(root).await {
                                debug!(%error, "collection wake broadcast failed");
                            }
                        }
                        Some(WakeCommand::Join(peers)) => {
                            bootstrap.remember(peers.iter().copied());
                            if let Err(error) = topic.join_wake_peers(peers).await {
                                debug!(%error, "joining DHT-discovered collection wake peers failed");
                            }
                        }
                        Some(WakeCommand::Resubscribe) => {
                            if bootstrap.has_recovery_route() {
                                debug!("reopening collection wake subscription for recovery");
                                break 'events;
                            }
                        }
                        Some(WakeCommand::Shutdown) | None => return,
                    },
                    event = topic.next_wake_event() => match event {
                        Ok(Some(CollectionWakeEvent::Received(received))) => {
                        bootstrap.remember([received.wake.origin()]);
                        // Wakes are repeatable hints. Dropping one under load
                        // preserves correctness while keeping a nonce flood
                        // behind a hard process-wide memory bound.
                        let _ = notices.try_send(WakeNotice::Received { collection, received });
                        }
                        Ok(Some(CollectionWakeEvent::Lagged)) => {
                            let _ = notices.try_send(WakeNotice::Lagged { collection });
                            if let Some(root) = current_root {
                                let _ = topic.broadcast_wake(root).await;
                            }
                        }
                        Ok(Some(CollectionWakeEvent::Rejected { error, .. })) => {
                            debug!(%error, "rejected invalid collection wake");
                        }
                        Ok(Some(CollectionWakeEvent::NeighborUp(_))) => {
                            if let Some(root) = current_root
                                && let Err(error) = topic.broadcast_wake(root).await
                            {
                                debug!(%error, "collection wake rebroadcast failed");
                            }
                        }
                        Ok(Some(CollectionWakeEvent::NeighborDown(_))) => {}
                        Ok(None) => {
                            debug!("collection wake subscription ended; retrying");
                            break 'events;
                        },
                        Err(error) => {
                            debug!(%error, "collection wake subscription failed; retrying");
                            break 'events;
                        }
                    }
                }
            }
        }
    });
    commands
}

async fn host_loop<T: Transport>(harness: Harness<T>, config: PeerConfig, wiring: HostWiring) {
    let Harness {
        transport,
        mut incoming,
    } = harness;
    let my_id = transport.local_id();
    let configured: Vec<_> = config
        .peers
        .iter()
        .map(|address| *address.id.as_bytes())
        .filter(|peer| *peer != my_id)
        .collect();
    let candidates = Arc::new(Mutex::new(RoutingTable::new(
        my_id,
        configured.iter().copied(),
    )));
    let participants = Arc::new(Mutex::new(HashMap::new()));
    let pool = new_shared_pool();
    let providers = Arc::new(Mutex::new(ProviderDirectory::new(my_id)));
    let provider_client = ProviderClient {
        transport: transport.clone(),
        pool: pool.clone(),
        providers: providers.clone(),
        candidates: candidates.clone(),
        my_id,
    };
    let cap = Arc::new(NetCap {
        client: provider_client.clone(),
    });
    let _ = wiring.cap_tx.send(Some(cap as Arc<dyn NetCapability>));

    let handler = SnapshotHandler {
        snapshot: wiring.snapshot.clone(),
        candidates: candidates.clone(),
        providers: providers.clone(),
        serve_collections: config.qos.direction.serves(),
        local_id: my_id,
        events: wiring.evt_tx.clone(),
        inbound_connections: Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS)),
        inbound_requests: Arc::new(tokio::sync::Semaphore::new(MAX_REQUESTS_GLOBAL)),
    };
    tokio::spawn(async move {
        while let Some(accepted) = incoming.recv().await {
            if accepted.alpn != PILE_SYNC_ALPN {
                accepted.conn.close(1, b"unknown protocol");
                continue;
            }
            let Ok(permit) = handler.inbound_connections.clone().try_acquire_owned() else {
                accepted.conn.close(1, b"inbound connection limit exceeded");
                continue;
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                handler.handle::<T>(accepted.conn, permit).await;
            });
        }
    });

    let wake_plane = transport.collection_wake_plane();
    let bootstrap_ids = config.peers.iter().map(|peer| peer.id).collect::<Vec<_>>();
    let (wake_tx, mut wake_rx) = tokio::sync::mpsc::channel::<WakeNotice>(256);
    let mut wake_topics: HashMap<[u8; 32], tokio::sync::mpsc::UnboundedSender<WakeCommand>> =
        HashMap::new();
    let (repair_tx, mut repair_rx) = tokio::sync::mpsc::unbounded_channel::<RepairOutcome>();
    let (discovery_tx, mut discovery_rx) =
        tokio::sync::mpsc::channel::<(CollectionHandle, Vec<PeerId>)>(64);
    let mut immediate = VecDeque::new();
    let mut pending = HashSet::new();
    let mut in_flight = HashSet::new();
    let mut failures: HashMap<RepairTarget, (u32, crate::clock::Mono)> = HashMap::new();
    let mut discovery: HashMap<[u8; 32], DiscoveryState> = HashMap::new();
    let mut current_roots: HashMap<[u8; 32], [u8; 32]> = HashMap::new();
    let mut next_period = crate::clock::mono_now();
    let mut next_discovery = crate::clock::mono_now();
    let mut publisher = ProviderPublisher::new(crate::clock::mono_now());
    let publication_limit = config.provider_publication_budget;
    let mut publication_budget = ProviderPublicationBudget::new(publication_limit);
    let mut publication_budget_reported = false;
    if publication_budget.is_exhausted() {
        warn!(
            "provider publication disabled by zero process budget; exact H-authorized serving remains enabled"
        );
        publication_budget_reported = true;
    }
    let (publication_tx, mut publication_rx) =
        tokio::sync::mpsc::unbounded_channel::<PublicationOutcome>();
    let mut publications_in_flight = HashSet::new();

    loop {
        let mut disconnected = false;
        loop {
            match wiring.cmd_rx.try_recv() {
                Ok(NetCommand::SnapshotChanged(notice)) => {
                    if !notice.installed {
                        current_roots.clear();
                        participants.lock().unwrap().clear();
                        discovery.clear();
                        immediate.clear();
                        pending.clear();
                        failures.clear();
                        for (_, topic) in wake_topics.drain() {
                            let _ = topic.send(WakeCommand::Shutdown);
                        }
                        continue;
                    }
                    let mut observed = HashSet::new();
                    for (collection, semantic_root) in notice.collections {
                        observed.insert(collection.raw);
                        if !current_roots.contains_key(&collection.raw) {
                            let now = crate::clock::mono_now();
                            discovery.insert(collection.raw, DiscoveryState::new(now));
                            next_discovery = next_discovery.min(now);
                        }
                        let changed = current_roots.insert(collection.raw, semantic_root)
                            != Some(semantic_root);
                        let topic = wake_topics.entry(collection.raw).or_insert_with(|| {
                            spawn_wake_topic(
                                wake_plane.clone(),
                                collection,
                                bootstrap_ids.clone(),
                                wake_tx.clone(),
                            )
                        });
                        if changed && semantic_root != [0; 32] {
                            let _ = topic
                                .send(WakeCommand::Observe(CollectionWakeRoot::new(semantic_root)));
                        }
                    }
                    current_roots.retain(|collection, _| observed.contains(collection));
                    discovery.retain(|collection, _| observed.contains(collection));
                    retain_active_repair_state(&mut failures, &observed);
                    participants
                        .lock()
                        .unwrap()
                        .retain(|collection, _| observed.contains(collection));
                    let stale = wake_topics
                        .keys()
                        .filter(|collection| !observed.contains(*collection))
                        .copied()
                        .collect::<Vec<_>>();
                    for collection in stale {
                        if let Some(topic) = wake_topics.remove(&collection) {
                            let _ = topic.send(WakeCommand::Shutdown);
                        }
                    }
                }
                Ok(NetCommand::ProvidersUpdated(observation)) => {
                    publisher.install(observation.into_set(), crate::clock::mono_now());
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if disconnected {
            transport.shutdown().await;
            return;
        }

        while let Ok(notice) = wake_rx.try_recv() {
            let (collection, received) = match notice {
                WakeNotice::Received {
                    collection,
                    received,
                } => (collection, received),
                WakeNotice::Lagged { collection } => {
                    if current_roots.contains_key(&collection.raw) {
                        let now = crate::clock::mono_now();
                        next_period = next_period.min(now);
                        next_discovery = next_discovery.min(now);
                    }
                    continue;
                }
            };
            let wake = received.wake;
            if wake.origin().as_bytes() == &my_id {
                continue;
            }
            if !current_roots.contains_key(&collection.raw) {
                continue;
            }
            observe_participant(
                &mut participants.lock().unwrap(),
                collection.raw,
                *wake.origin().as_bytes(),
                crate::clock::mono_now(),
            );
            if current_roots
                .get(&collection.raw)
                .is_some_and(|semantic| *semantic != [0; 32] && semantic != wake.root().as_bytes())
            {
                enqueue_repair(
                    &mut immediate,
                    &mut pending,
                    RepairTarget {
                        collection,
                        peer: *wake.origin().as_bytes(),
                    },
                );
            }
        }
        while let Ok(outcome) = repair_rx.try_recv() {
            in_flight.remove(&outcome.target);
            if !current_roots.contains_key(&outcome.target.collection.raw) {
                failures.remove(&outcome.target);
                continue;
            }
            if outcome.target.peer == my_id {
                failures.remove(&outcome.target);
                continue;
            }
            if outcome.success {
                failures.remove(&outcome.target);
                let now = crate::clock::mono_now();
                observe_participant(
                    &mut participants.lock().unwrap(),
                    outcome.target.collection.raw,
                    outcome.target.peer,
                    now,
                );
                discovery
                    .entry(outcome.target.collection.raw)
                    .or_insert_with(|| DiscoveryState::new(now))
                    .observe_success(now);
                if outcome.more {
                    enqueue_repair(&mut immediate, &mut pending, outcome.target);
                }
            } else {
                let participants_exhausted = forget_participant(
                    &mut participants.lock().unwrap(),
                    outcome.target.collection.raw,
                    outcome.target.peer,
                );
                let attempts = failures
                    .get(&outcome.target)
                    .map_or(1, |(attempts, _)| attempts.saturating_add(1));
                let shift = attempts.saturating_sub(1).min(6);
                if failures.len() < MAX_PENDING_REPAIRS || failures.contains_key(&outcome.target) {
                    failures.insert(
                        outcome.target,
                        (
                            attempts,
                            crate::clock::mono_now()
                                + crate::RETRY_BACKOFF_BASE.saturating_mul(1u32 << shift),
                        ),
                    );
                }
                if participants_exhausted
                    && let Some(topic) = wake_topics.get(&outcome.target.collection.raw)
                {
                    let _ = topic.send(WakeCommand::Resubscribe);
                }
                next_discovery = next_discovery.min(crate::clock::mono_now());
            }
        }
        while let Ok(outcome) = publication_rx.try_recv() {
            publications_in_flight.remove(&outcome.key);
            let effect = publisher.complete(outcome.key, outcome.result, crate::clock::mono_now());
            if effect.topology_outage_started {
                warn!(
                    "no authenticated remote DHT replica is reachable; provider publication is paused behind one bounded topology probe"
                );
            }
            if effect.topology_recovered {
                debug!("authenticated remote DHT replica reached; provider publication resumed");
            }
            if effect.retry_budget_full {
                warn!(
                    key = %hex::encode(&outcome.key[..4]),
                    "provider retry budget full after an authenticated remote rejected the announcement; exact discovery is degraded until the renewal cursor returns"
                );
            }
        }
        while let Ok((collection, peers)) = discovery_rx.try_recv() {
            let now = crate::clock::mono_now();
            let Some(state) = discovery.get_mut(&collection.raw) else {
                continue;
            };
            state.finish_attempt(now);
            next_discovery = next_discovery.min(state.retry_at);
            if !current_roots.contains_key(&collection.raw) {
                continue;
            }
            let peers = peers
                .into_iter()
                .filter(|peer| *peer != my_id)
                .collect::<Vec<_>>();
            if peers.is_empty() {
                if let Some(topic) = wake_topics.get(&collection.raw) {
                    let _ = topic.send(WakeCommand::Resubscribe);
                }
            } else if let Some(topic) = wake_topics.get(&collection.raw) {
                let joined = peers
                    .iter()
                    .filter_map(|peer| EndpointId::from_bytes(peer).ok())
                    .collect();
                let _ = topic.send(WakeCommand::Join(joined));
            }
            for peer in peers {
                observe_participant(&mut participants.lock().unwrap(), collection.raw, peer, now);
                enqueue_repair(
                    &mut immediate,
                    &mut pending,
                    RepairTarget { collection, peer },
                );
            }
        }

        let now = crate::clock::mono_now();
        if now >= next_period {
            next_period = now + REPAIR_PERIOD;
            // The anti-entropy tick also notices expired participant leases,
            // but DHT recovery keeps its own backoff and in-flight state.
            next_discovery = next_discovery.min(now);
            for raw in current_roots.keys() {
                if config.qos.direction.pulls() {
                    let collection = CollectionHandle::new(*raw);
                    let descriptor_missing = wiring
                        .snapshot
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_none_or(|snapshot| snapshot.get_blob(raw).is_none());
                    if descriptor_missing {
                        let client = provider_client.clone();
                        let events = wiring.evt_tx.clone();
                        tokio::spawn(async move {
                            if let Some(bytes) = client.fetch_blob(collection.raw).await {
                                let mut batch = NetEventBatch::default();
                                let _ = batch.try_push(NetEvent::Blob {
                                    expected: collection.raw,
                                    bytes,
                                });
                                let _ = events.send(batch).await;
                            }
                        });
                    }
                    let mut peers = live_participants(&mut participants.lock().unwrap(), *raw, now);
                    if !peers.is_empty() {
                        let rotation = (now.as_nanos() as usize
                            / REPAIR_PERIOD.as_nanos() as usize)
                            % peers.len();
                        peers.rotate_left(rotation);
                        peers.truncate(PERIODIC_REPAIR_SAMPLE);
                    }
                    for peer in peers {
                        enqueue_repair(
                            &mut immediate,
                            &mut pending,
                            RepairTarget {
                                collection: CollectionHandle::new(*raw),
                                peer,
                            },
                        );
                    }
                }
            }
        }

        if config.qos.direction.pulls() && now >= next_discovery {
            next_discovery = now + REPAIR_PERIOD;
            for raw in current_roots.keys() {
                let collection = CollectionHandle::new(*raw);
                let peers = live_participants(&mut participants.lock().unwrap(), *raw, now);
                let has_candidate = has_repair_candidate(collection, &peers, &failures, my_id);
                let state = discovery
                    .entry(*raw)
                    .or_insert_with(|| DiscoveryState::new(now));
                if state.start_if_due(now, has_candidate) {
                    let client = provider_client.clone();
                    let discovery_tx = discovery_tx.clone();
                    tokio::spawn(async move {
                        let peers = client
                            .find_key(
                                collection_provider_key(collection),
                                collection_provider_token,
                                collection.raw,
                            )
                            .await;
                        let _ = discovery_tx.send((collection, peers)).await;
                    });
                } else if !has_candidate && !state.in_flight {
                    next_discovery = next_discovery.min(state.retry_at);
                }
            }
        }

        if config.qos.direction.pulls() {
            while in_flight.len() < MAX_CONCURRENT_REPAIRS {
                let Some(target) = immediate.pop_front() else {
                    break;
                };
                pending.remove(&target);
                if in_flight.contains(&target)
                    || failures
                        .get(&target)
                        .is_some_and(|(_, retry_at)| now < *retry_at)
                {
                    continue;
                }
                let Some(local) = wiring
                    .snapshot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|snapshot| snapshot.collection(target.collection))
                else {
                    continue;
                };
                in_flight.insert(target);
                let transport = transport.clone();
                let pool = pool.clone();
                let events = wiring.evt_tx.clone();
                let repair_tx = repair_tx.clone();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        REPAIR_DEADLINE,
                        reconcile_collection_peer(&transport, &pool, target, local, &events),
                    )
                    .await;
                    let (success, more) = match result {
                        Ok(Ok(more)) => (true, more),
                        Ok(Err(error)) => {
                            debug!(%error, "collection repair failed");
                            (false, false)
                        }
                        Err(_) => (false, false),
                    };
                    let _ = repair_tx.send(RepairOutcome {
                        target,
                        success,
                        more,
                    });
                });
            }
        }

        while publications_in_flight.len() < ALPHA && publication_budget.permits_attempt() {
            let Some((key, identity)) = publisher.next(now) else {
                break;
            };
            if !publications_in_flight.insert(key) {
                let _ = publisher.retry(key, now);
                break;
            }
            publication_budget.consume_attempt();
            if publication_budget.is_exhausted() && !publication_budget_reported {
                warn!(
                    limit = publication_limit.expect("a finite budget can be exhausted"),
                    "provider publication budget reached; this is the final permitted announcement attempt, and later additions, retries, and renewals remain suppressed"
                );
                publication_budget_reported = true;
            }
            let client = provider_client.clone();
            let publication_tx = publication_tx.clone();
            let token = provider_lease_token(identity, key, my_id);
            tokio::spawn(async move {
                let result = client.announce_key(key, token).await;
                let _ = publication_tx.send(PublicationOutcome { key, result });
            });
        }

        tokio::time::sleep(HOST_POLL_PERIOD).await;
    }
}

struct AdmissionBatcher {
    events: tokio::sync::mpsc::Sender<NetEventBatch>,
    pending: NetEventBatch,
}

impl AdmissionBatcher {
    fn new(events: &tokio::sync::mpsc::Sender<NetEventBatch>) -> Self {
        Self {
            events: events.clone(),
            pending: NetEventBatch::default(),
        }
    }

    async fn push(&mut self, event: NetEvent) -> anyhow::Result<()> {
        if let Err(event) = self.pending.try_push(event) {
            self.flush().await?;
            self.pending
                .try_push(event)
                .expect("an empty admission batch accepts one indivisible event");
        }
        if self.pending.is_full() {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.events
            .send(std::mem::take(&mut self.pending))
            .await
            .map_err(|_| anyhow::anyhow!("store side stopped during collection admission"))
    }
}

async fn reconcile_collection_peer<T: Transport>(
    transport: &T,
    pool: &SharedPool<T::Conn>,
    target: RepairTarget,
    local: Arc<CollectionSnapshot>,
    events: &tokio::sync::mpsc::Sender<NetEventBatch>,
) -> anyhow::Result<bool> {
    let connection = pool_get(transport, pool, target.peer).await?;
    let delta = match pull_collection(
        connection.conn(),
        &local.repair,
        local.read_bootstrap.iter().cloned().collect(),
    )
    .await
    {
        Ok(delta) => delta,
        Err(error) => {
            pool_invalidate(pool, target.peer, &connection.entry);
            return Err(error);
        }
    };
    let mut admissions = AdmissionBatcher::new(events);
    for proof in delta.authorization_evidence {
        admissions.push(NetEvent::CapabilityProof(proof)).await?;
    }
    for record in delta.records {
        admissions.push(NetEvent::CollectionRecord(record)).await?;
    }
    admissions.flush().await?;
    Ok(delta.more)
}

impl<T: Transport> ProviderClient<T> {
    async fn find_node(&self, peer: PeerId, target: RoutingKey) -> anyhow::Result<Vec<PeerId>> {
        let connection = pool_get(&self.transport, &self.pool, peer).await?;
        let response = tokio::time::timeout(OP_DEADLINE, op_find_node(connection.conn(), &target))
            .await
            .map_err(|_| anyhow::anyhow!("FIND_NODE deadline exceeded"))?;
        match response {
            Ok(peers) => {
                self.candidates.lock().unwrap().promote_authenticated(peer);
                Ok(peers)
            }
            Err(error) => {
                pool_invalidate(&self.pool, peer, &connection.entry);
                Err(error)
            }
        }
    }

    async fn lookup_replicas(&self, target: RoutingKey) -> Vec<PeerId> {
        let seeds = self.candidates.lock().unwrap().closest(target, K);
        let mut lookup = IterativeLookup::new(self.my_id, target, seeds);
        let mut pending: FuturesUnordered<
            futures::future::BoxFuture<'_, (PeerId, anyhow::Result<Vec<PeerId>>)>,
        > = FuturesUnordered::new();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                for peer in lookup.next_batch() {
                    pending.push(Box::pin(async move {
                        let reply = self.find_node(peer, target).await;
                        (peer, reply)
                    }));
                }
                let Some((peer, reply)) = pending.next().await else {
                    break;
                };
                match reply {
                    Ok(peers) => {
                        let valid = peers
                            .into_iter()
                            .filter(|candidate| EndpointId::from_bytes(candidate).is_ok());
                        lookup.record_authenticated_response(
                            peer,
                            valid,
                            &mut self.candidates.lock().unwrap(),
                        );
                    }
                    Err(_) => {
                        lookup.record_failure(peer, &mut self.candidates.lock().unwrap());
                    }
                }
                if lookup.is_finished() && pending.is_empty() {
                    break;
                }
            }
        })
        .await;
        if completed.is_err() {
            drop(pending);
        }
        let mut replicas = lookup.closest_authenticated_responders().to_vec();
        replicas.push(self.my_id);
        replicas.sort_unstable_by(|a, b| crate::routing::distance_cmp(target, *a, *b));
        replicas.dedup();
        replicas.truncate(K);
        replicas
    }

    async fn put(&self, peer: PeerId, key: ProviderKey, token: ProviderToken) -> ProviderPutResult {
        if peer == self.my_id {
            return if self.providers.lock().unwrap().put(
                key,
                self.my_id,
                token,
                crate::clock::mono_now(),
            ) {
                ProviderPutResult::Accepted
            } else {
                ProviderPutResult::ExplicitlyRejected
            };
        }
        let Ok(connection) = pool_get(&self.transport, &self.pool, peer).await else {
            return ProviderPutResult::Unavailable;
        };
        match tokio::time::timeout(
            OP_DEADLINE,
            op_provider_put(connection.conn(), &key, &token),
        )
        .await
        {
            Ok(Ok(stored)) => {
                self.candidates.lock().unwrap().promote_authenticated(peer);
                if stored {
                    ProviderPutResult::Accepted
                } else {
                    ProviderPutResult::ExplicitlyRejected
                }
            }
            Ok(Err(_)) | Err(_) => {
                pool_invalidate(&self.pool, peer, &connection.entry);
                ProviderPutResult::Unavailable
            }
        }
    }

    async fn announce_key(&self, key: ProviderKey, token: ProviderToken) -> PublicationResult {
        let targets = self.lookup_replicas(key).await;
        let mut attempts = futures::stream::iter(targets)
            .map(|peer| async move { (peer, self.put(peer, key, token).await) })
            .buffer_unordered(ALPHA);
        let mut publication = PublicationResult::NoAuthenticatedRemoteReplica;
        while let Some((peer, result)) = attempts.next().await {
            publication = publication.observe_put(self.my_id, peer, result);
        }
        // A local directory copy is useful for single-node reads but cannot
        // make this endpoint discoverable after an isolated startup. Preserve
        // that topology failure separately from a rejection by a remote which
        // authenticated during this exact lookup.
        publication
    }

    async fn get(&self, peer: PeerId, key: ProviderKey) -> Vec<(PeerId, ProviderToken)> {
        if peer == self.my_id {
            return self
                .providers
                .lock()
                .unwrap()
                .get(key, crate::clock::mono_now());
        }
        let Ok(connection) = pool_get(&self.transport, &self.pool, peer).await else {
            return Vec::new();
        };
        match tokio::time::timeout(OP_DEADLINE, op_provider_get(connection.conn(), &key)).await {
            Ok(Ok(providers)) => {
                self.candidates.lock().unwrap().promote_authenticated(peer);
                providers
            }
            Ok(Err(_)) | Err(_) => {
                pool_invalidate(&self.pool, peer, &connection.entry);
                Vec::new()
            }
        }
    }

    async fn find_key(
        &self,
        key: ProviderKey,
        token_for: fn([u8; 32], PeerId) -> ProviderToken,
        identity: [u8; 32],
    ) -> Vec<PeerId> {
        let replicas = self.lookup_replicas(key).await;
        let mut replies = futures::stream::iter(replicas)
            .map(|peer| async move { self.get(peer, key).await })
            .buffer_unordered(ALPHA);
        let mut providers = Vec::new();
        while let Some(reply) = replies.next().await {
            for (provider, token) in reply {
                if token_for(identity, provider) == token {
                    providers.push(provider);
                }
            }
        }
        canonical_provider_subset(key, providers)
    }

    async fn fetch_from_providers(&self, hash: RawHash, providers: Vec<PeerId>) -> Option<Bytes> {
        let mut attempts = futures::stream::iter(providers)
            .map(|peer| async move {
                let connection = pool_get(&self.transport, &self.pool, peer).await.ok()?;
                let response = tokio::time::timeout(
                    OP_DEADLINE,
                    op_get_blob(connection.conn(), self.my_id, &hash),
                )
                .await;
                match response {
                    Ok(Ok(Some(bytes))) => {
                        self.candidates.lock().unwrap().promote_authenticated(peer);
                        Some(bytes)
                    }
                    Ok(Ok(None)) => None,
                    Ok(Err(_)) | Err(_) => {
                        pool_invalidate(&self.pool, peer, &connection.entry);
                        None
                    }
                }
            })
            .buffer_unordered(ALPHA);
        while let Some(result) = attempts.next().await {
            if result.is_some() {
                return result;
            }
        }
        None
    }

    /// Discover candidates only from H, then complete the H-only handshake.
    async fn fetch_blob(&self, hash: RawHash) -> Option<Bytes> {
        let providers = self
            .find_key(blob_locator(hash), blob_provider_token, hash)
            .await
            .into_iter()
            .filter(|peer| *peer != self.my_id)
            .collect::<Vec<_>>();
        self.fetch_from_providers(hash, providers).await
    }
}

/// Canonical globally bounded union of provider replies for one exact key.
///
/// Each queried DHT replica independently bounds its response, but their union
/// may still be `K` times larger. Ranking the deduplicated union by the same XOR
/// order as routing makes the selected subset independent of asynchronous reply
/// order and keeps one exact key's downstream connection fan-out bounded.
fn canonical_provider_subset(
    key: ProviderKey,
    providers: impl IntoIterator<Item = PeerId>,
) -> Vec<PeerId> {
    let mut providers = providers
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    providers.sort_unstable_by(|left, right| crate::routing::distance_cmp(key, *left, *right));
    providers.truncate(crate::provider::MAX_PROVIDERS_PER_KEY);
    providers
}

#[derive(Clone)]
struct SnapshotHandler {
    snapshot: SnapshotSlot,
    candidates: RoutingCandidates,
    providers: Arc<Mutex<ProviderDirectory>>,
    serve_collections: bool,
    local_id: PeerId,
    events: tokio::sync::mpsc::Sender<NetEventBatch>,
    inbound_connections: Arc<tokio::sync::Semaphore>,
    inbound_requests: Arc<tokio::sync::Semaphore>,
}

impl SnapshotHandler {
    async fn handle<T: Transport>(
        &self,
        connection: T::Conn,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let peer_id = connection.remote_id();
        let span = info_span!("connection", peer = %hex::encode(&peer_id[..4]));
        async move {
            let peer = match VerifyingKey::from_bytes(&peer_id) {
                Ok(peer) => peer,
                Err(error) => {
                    warn!(%error, "invalid transport peer key");
                    connection.close(1, b"invalid peer identity");
                    return;
                }
            };
            let per_connection = Arc::new(tokio::sync::Semaphore::new(MAX_REQUESTS_PER_CONNECTION));
            loop {
                let accepted = tokio::select! {
                    stream = connection.accept_bi() => stream,
                    () = tokio::time::sleep(CONNECTION_IDLE_DEADLINE) => {
                        connection.close(0, b"connection idle timeout");
                        return;
                    }
                };
                let Some((mut send, mut recv)) = accepted else {
                    return;
                };
                let Ok(connection_permit) = per_connection.clone().try_acquire_owned() else {
                    connection.close(1, b"request concurrency exceeded");
                    return;
                };
                let Ok(global_permit) = self.inbound_requests.clone().try_acquire_owned() else {
                    connection.close(1, b"global request concurrency exceeded");
                    return;
                };
                let handler = self.clone();
                tokio::spawn(
                    async move {
                        let operation = tokio::time::timeout(
                            REQUEST_DEADLINE,
                            handler.serve_stream::<T::Conn>(peer, &mut send, &mut recv),
                        )
                        .await;
                        match operation {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => debug!(%error, "direct RPC stream failed"),
                            Err(_) => warn!("direct RPC stream deadline exceeded"),
                        }
                        let _ = send.shutdown().await;
                        drop((connection_permit, global_permit));
                    }
                    .in_current_span(),
                );
            }
        }
        .instrument(span)
        .await;
    }

    async fn serve_stream<C: Conn>(
        &self,
        peer: VerifyingKey,
        send: &mut C::SendHalf,
        recv: &mut C::RecvHalf,
    ) -> anyhow::Result<()> {
        let op = recv_u8(recv).await?;
        let span = debug_span!("stream", op = op_name(op));
        let _entered = span.enter();
        match op {
            OP_COLLECTION_REPAIR => {
                if !self.serve_collections {
                    let _ = serve_collection_repair(recv, send, peer, |_| None).await?;
                } else {
                    let snapshot = self.snapshot.lock().unwrap().clone();
                    let bootstrap = serve_collection_repair(recv, send, peer, move |collection| {
                        snapshot
                            .as_ref()
                            .and_then(|snapshot| snapshot.collection(collection))
                            .map(|collection| collection.repair.clone())
                    })
                    .await?;
                    let mut admissions = AdmissionBatcher::new(&self.events);
                    for proof in bootstrap {
                        admissions.push(NetEvent::CapabilityProof(proof)).await?;
                    }
                    admissions.flush().await?;
                }
            }
            OP_GET_BLOB => {
                let snapshot = self.snapshot.lock().unwrap().clone();
                let blob_snapshot = snapshot.clone();
                serve_get_blob(
                    recv,
                    send,
                    peer.to_bytes(),
                    self.local_id,
                    move |locator| {
                        snapshot
                            .as_ref()
                            .and_then(|snapshot| snapshot.bearer_handle(locator))
                    },
                    move |handle| {
                        blob_snapshot
                            .as_ref()
                            .and_then(|snapshot| snapshot.get_blob(&handle))
                    },
                )
                .await?;
            }
            OP_PROVIDER_PUT => {
                let key = recv_hash(recv).await?;
                let token = recv_hash(recv).await?;
                require_stream_eof(recv).await?;
                let stored = self.providers.lock().unwrap().put(
                    key,
                    peer.to_bytes(),
                    token,
                    crate::clock::mono_now(),
                );
                send_u8(
                    send,
                    if stored {
                        PROVIDER_PUT_OK
                    } else {
                        PROVIDER_PUT_FULL
                    },
                )
                .await?;
            }
            OP_PROVIDER_GET => {
                let key = recv_exact_key(recv).await?;
                let providers = self
                    .providers
                    .lock()
                    .unwrap()
                    .get(key, crate::clock::mono_now());
                send_u8(send, providers.len() as u8).await?;
                for (provider, token) in providers {
                    send_hash(send, &provider).await?;
                    send_hash(send, &token).await?;
                }
            }
            OP_FIND_NODE => {
                let target = recv_exact_key(recv).await?;
                let mut peers = self.candidates.lock().unwrap().closest_verified(target, K);
                peers.retain(|candidate| *candidate != peer.to_bytes());
                send_u8(send, peers.len() as u8).await?;
                for peer in peers {
                    send_hash(send, &peer).await?;
                }
            }
            _ => anyhow::bail!("unknown direct RPC operation {op:#x}"),
        }
        self.candidates
            .lock()
            .unwrap()
            .promote_authenticated(peer.to_bytes());
        Ok(())
    }
}

async fn recv_exact_key<R: tokio::io::AsyncRead + Unpin>(recv: &mut R) -> anyhow::Result<[u8; 32]> {
    let key = recv_hash(recv).await?;
    require_stream_eof(recv).await?;
    Ok(key)
}

async fn require_stream_eof<R: tokio::io::AsyncRead + Unpin>(recv: &mut R) -> anyhow::Result<()> {
    let mut trailing = [0u8; 1];
    if recv.read(&mut trailing).await? != 0 {
        anyhow::bail!("request contains trailing bytes");
    }
    Ok(())
}

fn op_name(op: u8) -> &'static str {
    match op {
        OP_GET_BLOB => "GET_BLOB",
        OP_PROVIDER_PUT => "PROVIDER_PUT",
        OP_PROVIDER_GET => "PROVIDER_GET",
        OP_FIND_NODE => "FIND_NODE",
        OP_COLLECTION_REPAIR => "COLLECTION_REPAIR",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use crate::provider::{
        MAX_PROVIDERS_PER_KEY, ProviderObservation, ProviderPublisher, ProviderPutResult,
        PublicationResult,
    };
    use crate::routing::K;
    use crate::transport::PeerId;
    use ed25519_dalek::SigningKey;
    use iroh_base::EndpointId;
    use triblespace_core::collection::CollectionHandle;

    use super::{
        COLLECTION_PARTICIPANT_LEASE, DiscoveryState, MAX_COLLECTION_PARTICIPANTS,
        MAX_PENDING_REPAIRS, ProviderPublicationBudget, RepairTarget, WakeBootstrapPeers,
        canonical_provider_subset, enqueue_repair, forget_participant, has_repair_candidate,
        live_participants, observe_participant, retain_active_repair_state,
    };

    fn endpoint(byte: u8) -> EndpointId {
        EndpointId::from_bytes(
            SigningKey::from_bytes(&[byte; 32])
                .verifying_key()
                .as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn typed_put_results_separate_topology_loss_from_explicit_rejection() {
        let local = [1; 32];
        let remote = [2; 32];
        assert_eq!(
            PublicationResult::from_put_results(
                local,
                [
                    (local, ProviderPutResult::Accepted),
                    (remote, ProviderPutResult::Unavailable),
                ],
            ),
            PublicationResult::NoAuthenticatedRemoteReplica,
            "local acceptance and a vanished FIND_NODE responder prove no remote publication"
        );
        assert_eq!(
            PublicationResult::from_put_results(
                local,
                [(remote, ProviderPutResult::ExplicitlyRejected)],
            ),
            PublicationResult::RemoteRejected
        );
        assert_eq!(
            PublicationResult::from_put_results(local, [(remote, ProviderPutResult::Accepted)],),
            PublicationResult::Published
        );
    }

    #[test]
    fn zero_provider_publication_budget_permits_no_attempts() {
        let budget = ProviderPublicationBudget::new(Some(0));
        assert!(!budget.permits_attempt());
        assert!(budget.is_exhausted());
    }

    #[test]
    fn finite_provider_publication_budget_survives_resident_snapshot_updates() {
        let now = crate::clock::mono_now();
        let first = triblespace_core::collection::CollectionHandle::new([0x61; 32]);
        let second = triblespace_core::collection::CollectionHandle::new([0x62; 32]);
        let third = triblespace_core::collection::CollectionHandle::new([0x63; 32]);
        let mut publisher = ProviderPublisher::new(now);
        let mut budget = ProviderPublicationBudget::new(Some(2));

        publisher.install(
            ProviderObservation::from_collections([first], true).into_set(),
            now,
        );
        assert!(budget.permits_attempt());
        assert!(publisher.next(now).is_some());
        budget.consume_attempt();

        publisher.install(
            ProviderObservation::from_collections([first, second], true).into_set(),
            now,
        );
        assert!(budget.permits_attempt());
        assert!(publisher.next(now).is_some());
        budget.consume_attempt();
        assert!(budget.is_exhausted());

        publisher.install(
            ProviderObservation::from_collections([first, second, third], true).into_set(),
            now,
        );
        assert!(
            !budget.permits_attempt(),
            "installing a later resident snapshot must not refill a process budget"
        );
    }

    #[test]
    fn absent_provider_publication_budget_remains_unlimited() {
        let mut budget = ProviderPublicationBudget::new(None);
        for _ in 0..10_000 {
            assert!(budget.permits_attempt());
            budget.consume_attempt();
        }
        assert!(!budget.is_exhausted());
    }

    #[test]
    fn aggregated_provider_replies_are_canonical_deduplicated_and_globally_bounded() {
        fn provider(index: u16) -> PeerId {
            let mut peer = [0; 32];
            peer[30..].copy_from_slice(&index.to_be_bytes());
            peer
        }

        let key_index = 0x0234_u16;
        let mut key = [0; 32];
        key[30..].copy_from_slice(&key_index.to_be_bytes());
        let replies = (0..K)
            .map(|replica| {
                // Every replica repeats the same 16 providers and contributes
                // 48 providers disjoint from every other replica.
                (1..=16)
                    .chain(100 + replica as u16 * 48..148 + replica as u16 * 48)
                    .map(provider)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(replies.len(), K);
        assert!(
            replies
                .iter()
                .all(|reply| reply.len() == MAX_PROVIDERS_PER_KEY)
        );

        let forward = canonical_provider_subset(key, replies.iter().flatten().copied());
        let reversed = canonical_provider_subset(
            key,
            replies
                .iter()
                .rev()
                .flat_map(|reply| reply.iter().rev().copied()),
        );
        let mut expected_indices = (1..=16).chain(100..100 + K as u16 * 48).collect::<Vec<_>>();
        expected_indices.sort_unstable_by_key(|index| index ^ key_index);
        expected_indices.truncate(MAX_PROVIDERS_PER_KEY);
        let expected = expected_indices
            .into_iter()
            .map(provider)
            .collect::<Vec<_>>();

        assert_eq!(forward.len(), MAX_PROVIDERS_PER_KEY);
        assert_eq!(forward, expected);
        assert_eq!(reversed, forward);
        assert_eq!(
            forward.iter().copied().collect::<BTreeSet<_>>().len(),
            forward.len()
        );
    }

    #[test]
    fn collection_participant_hints_are_bounded_under_signed_wake_flood() {
        let collection = [0x41; 32];
        let now = crate::clock::mono_now();
        let mut participants = HashMap::new();
        for index in 0..(MAX_COLLECTION_PARTICIPANTS * 2) {
            let mut peer = [0u8; 32];
            peer[..8].copy_from_slice(&(index as u64).to_be_bytes());
            observe_participant(&mut participants, collection, peer, now);
        }
        let live = live_participants(&mut participants, collection, now);
        assert_eq!(live.len(), MAX_COLLECTION_PARTICIPANTS);
        assert!(live.contains(&{
            let mut newest = [0u8; 32];
            newest[..8]
                .copy_from_slice(&((MAX_COLLECTION_PARTICIPANTS * 2 - 1) as u64).to_be_bytes());
            newest
        }));
    }

    #[test]
    fn healthy_collection_performs_only_its_initial_discovery() {
        let started = crate::clock::mono_now();
        let mut discovery = DiscoveryState::new(started);
        let mut lookups = 0;

        assert!(discovery.start_if_due(started, false));
        lookups += 1;
        discovery.finish_attempt(started);

        for period in 1..=100 {
            let now = started + super::REPAIR_PERIOD.saturating_mul(period);
            if discovery.start_if_due(now, true) {
                lookups += 1;
            }
        }
        assert_eq!(lookups, 1, "a leased repair candidate replaces DHT polling");
    }

    #[test]
    fn expired_or_all_failed_candidates_reenter_bounded_discovery() {
        let started = crate::clock::mono_now();
        let mut discovery = DiscoveryState::new(started);
        assert!(discovery.start_if_due(started, false));
        discovery.finish_attempt(started);
        assert!(!discovery.start_if_due(started, false));

        let first_retry = started + crate::RETRY_BACKOFF_BASE;
        assert!(discovery.start_if_due(first_retry, false));
        discovery.finish_attempt(first_retry);
        assert_eq!(
            discovery.retry_at.duration_since(first_retry),
            crate::RETRY_BACKOFF_BASE.saturating_mul(2)
        );

        for _ in 0..10 {
            let retry = discovery.retry_at;
            assert!(discovery.start_if_due(retry, false));
            discovery.finish_attempt(retry);
            assert!(
                discovery.retry_at.duration_since(retry) <= crate::RETRY_BACKOFF_CAP,
                "recovery attempts must remain live without becoming a tight loop"
            );
        }

        let collection = CollectionHandle::new([0x31; 32]);
        let first = [0x32; 32];
        let second = [0x33; 32];
        let mut participants = HashMap::new();
        observe_participant(&mut participants, collection.raw, first, started);
        assert!(
            live_participants(
                &mut participants,
                collection.raw,
                started + COLLECTION_PARTICIPANT_LEASE + std::time::Duration::from_nanos(1)
            )
            .is_empty(),
            "an exhausted lease leaves no repair candidate"
        );
        let mut exhausted = DiscoveryState::new(started);
        exhausted.observe_success(started);
        assert!(exhausted.start_if_due(
            started + COLLECTION_PARTICIPANT_LEASE + std::time::Duration::from_nanos(1),
            false
        ));

        let peers = vec![first, second];
        let mut failures = HashMap::from([(
            RepairTarget {
                collection,
                peer: first,
            },
            (1, started),
        )]);
        assert!(has_repair_candidate(
            collection, &peers, &failures, [0x34; 32]
        ));
        failures.insert(
            RepairTarget {
                collection,
                peer: second,
            },
            (1, started),
        );
        assert!(!has_repair_candidate(
            collection, &peers, &failures, [0x34; 32]
        ));
        let mut failed = DiscoveryState::new(started);
        failed.observe_success(started);
        assert!(failed.start_if_due(started, false));
    }

    #[test]
    fn local_provider_is_never_a_collection_repair_candidate() {
        let collection = CollectionHandle::new([0x35; 32]);
        let local = [0x36; 32];
        assert!(!has_repair_candidate(
            collection,
            &[local],
            &HashMap::new(),
            local
        ));
    }

    #[test]
    fn an_untracked_failure_cannot_retain_a_healthy_participant_lease() {
        let collection = CollectionHandle::new([0x3b; 32]);
        let peer = [0x3c; 32];
        let now = crate::clock::mono_now();
        let mut participants = HashMap::new();
        observe_participant(&mut participants, collection.raw, peer, now);

        let failures = (0..MAX_PENDING_REPAIRS)
            .map(|index| {
                let mut failed_peer = [0u8; 32];
                failed_peer[..8].copy_from_slice(&(index as u64).to_be_bytes());
                (
                    RepairTarget {
                        collection: CollectionHandle::new([0x3d; 32]),
                        peer: failed_peer,
                    },
                    (1, now),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(failures.len(), MAX_PENDING_REPAIRS);

        forget_participant(&mut participants, collection.raw, peer);

        let live = live_participants(&mut participants, collection.raw, now);
        assert!(live.is_empty());
        assert!(!has_repair_candidate(
            collection, &live, &failures, [0x3e; 32]
        ));
    }

    #[test]
    fn repair_success_does_not_forge_discovery_task_completion() {
        let started = crate::clock::mono_now();
        let mut discovery = DiscoveryState::new(started);
        assert!(discovery.start_if_due(started, false));

        discovery.observe_success(started + std::time::Duration::from_secs(1));

        assert!(discovery.in_flight);
        assert!(!discovery.start_if_due(started + std::time::Duration::from_secs(2), false));
        discovery.finish_attempt(started + std::time::Duration::from_secs(3));
        assert!(!discovery.in_flight);
    }

    #[test]
    fn removed_collections_release_their_repair_state() {
        let retained = RepairTarget {
            collection: CollectionHandle::new([0x37; 32]),
            peer: [0x38; 32],
        };
        let removed = RepairTarget {
            collection: CollectionHandle::new([0x39; 32]),
            peer: [0x3a; 32],
        };
        let mut state = HashMap::from([(retained, 1u8), (removed, 2u8)]);
        let active = std::collections::HashSet::from([retained.collection.raw]);

        retain_active_repair_state(&mut state, &active);

        assert_eq!(state, HashMap::from([(retained, 1u8)]));
    }

    #[test]
    fn successful_repair_refreshes_the_participant_lease() {
        let collection = [0x42; 32];
        let peer = [0x43; 32];
        let started = crate::clock::mono_now();
        let refreshed = started + std::time::Duration::from_secs(4 * 60);
        let after_original_expiry =
            started + COLLECTION_PARTICIPANT_LEASE + std::time::Duration::from_secs(1);
        let mut participants = HashMap::new();
        let mut discovery = DiscoveryState::new(started);

        observe_participant(&mut participants, collection, peer, started);
        observe_participant(&mut participants, collection, peer, refreshed);
        discovery.observe_success(refreshed);

        assert_eq!(
            live_participants(&mut participants, collection, after_original_expiry),
            vec![peer],
            "a healthy identical repair keeps the origin live past its first observation"
        );
        assert!(!discovery.start_if_due(after_original_expiry, true));
        assert!(
            live_participants(
                &mut participants,
                collection,
                refreshed + COLLECTION_PARTICIPANT_LEASE + std::time::Duration::from_nanos(1)
            )
            .is_empty()
        );
    }

    #[test]
    fn learned_wake_peers_survive_resubscription_with_a_bounded_recent_set() {
        let configured = endpoint(0x51);
        let mut bootstrap = WakeBootstrapPeers::new(vec![configured]);
        assert!(
            bootstrap.has_recovery_route(),
            "a configured topology seed can reopen the collection topic without becoming a repair participant"
        );
        let learned = (0..=MAX_COLLECTION_PARTICIPANTS)
            .map(|index| endpoint((index as u8).wrapping_add(0x60)))
            .collect::<Vec<_>>();
        bootstrap.remember(learned.iter().copied());
        assert!(bootstrap.has_recovery_route());

        let resubscribe = bootstrap.current();
        assert_eq!(resubscribe.first(), Some(&configured));
        assert_eq!(resubscribe.len(), 1 + MAX_COLLECTION_PARTICIPANTS);
        assert!(!resubscribe.contains(&learned[0]));
        assert!(resubscribe.contains(learned.last().unwrap()));

        bootstrap.remember([learned[1]]);
        assert_eq!(bootstrap.current().last(), Some(&learned[1]));
    }

    #[test]
    fn gossip_recovery_waits_until_every_participant_failed() {
        let collection = [0x71; 32];
        let first = [0x72; 32];
        let second = [0x73; 32];
        let now = crate::clock::mono_now();
        let mut participants = HashMap::new();
        observe_participant(&mut participants, collection, first, now);
        observe_participant(&mut participants, collection, second, now);

        assert!(!forget_participant(&mut participants, collection, first));
        assert_eq!(
            live_participants(&mut participants, collection, now),
            vec![second]
        );
        assert!(forget_participant(&mut participants, collection, second));
        assert!(live_participants(&mut participants, collection, now).is_empty());
    }

    #[test]
    fn pending_repairs_are_coalesced_and_bounded_under_wake_flood() {
        let mut queue = std::collections::VecDeque::new();
        let mut pending = std::collections::HashSet::new();
        for index in 0..(MAX_PENDING_REPAIRS * 2) {
            let mut peer = [0u8; 32];
            peer[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let target = RepairTarget {
                collection: triblespace_core::collection::CollectionHandle::new([0x51; 32]),
                peer,
            };
            enqueue_repair(&mut queue, &mut pending, target);
            enqueue_repair(&mut queue, &mut pending, target);
        }
        assert_eq!(queue.len(), MAX_PENDING_REPAIRS);
        assert_eq!(pending.len(), MAX_PENDING_REPAIRS);
    }
}

/// Production relay defaults with trailing-dot hostnames normalized for HTTP
/// intermediaries that reject absolute-FQDN Host headers.
pub(crate) fn dot_stripped_default_relay_map() -> iroh::RelayMap {
    let original = iroh::defaults::prod::default_relay_map();
    let urls: Vec<String> = original
        .urls::<Vec<_>>()
        .into_iter()
        .map(|relay| {
            let mut url: url::Url = relay.into();
            if let Some(host) = url.host_str().and_then(|host| host.strip_suffix('.')) {
                let host = host.to_owned();
                let _ = url.set_host(Some(&host));
            }
            url.to_string()
        })
        .collect();
    iroh::RelayMap::try_from_iter(urls.iter().map(String::as_str))
        .expect("default relay URLs remain valid after hostname normalization")
}
