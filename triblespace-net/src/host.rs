//! Collection-scoped network host.
//!
//! TLS authenticates endpoint identities, but establishing a transport
//! connection grants no team or collection authority. Each semantic repair is
//! one stream whose request carries complete READ(C) evidence. DHT routing,
//! provider-directory operations discover collection participants through an
//! opaque KDF(C). Exact bytes use a separate H-only DHT rendezvous and mutual
//! key-confirmation stream; collection identity never participates.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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
use triblespace_core::capability::CapabilityProofBundle;
use triblespace_core::collection::{CollectionHandle, CollectionRecord};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::patch::{Entry as PatchEntry, IdentitySchema, PATCH};
use triblespace_core::repo::{BlobChildren, BlobStoreGet, BlobStoreList, StoreChanges, StoreRead};

use crate::bearer::{BearerLocatorIndex, blob_locator, locator_index, update_locator_index};
use crate::channel::{NetCommand, NetEvent, NetEventBatch, SnapshotNotice};
use crate::collection_activation::{
    CollectionActivationOverlay, CollectionActivationOverlayError, CollectionReadEvidenceError,
    collection_activation_overlay_at, collection_read_evidence_bundles_at,
};
use crate::collection_session::{
    DisclosureForestPatch, FullReplicaCursor, FullReplicaState, pull_collection,
    serve_collection_repair,
};
use crate::collection_wire::{MAX_COLLECTION_READ_BUNDLES, OP_COLLECTION_REPAIR};
use crate::identity::iroh_secret;
use crate::inventory::{BlobReplication, ReconcileQos};
use crate::patch_repair::PatchSummary;
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
    activation: Arc<CollectionActivationOverlay>,
    read_evidence: Arc<[CapabilityProofBundle]>,
    full: Arc<FullReplicaState>,
    blobs: Arc<dyn BlobSnapshotReader>,
}

impl CollectionSnapshot {
    pub(crate) fn collection(&self) -> CollectionHandle {
        self.activation.collection()
    }

    fn wake_root(&self) -> [u8; 32] {
        self.activation.wake_root()
    }
}

type CollectionSnapshotIndex = PATCH<32, IdentitySchema, Arc<CollectionSnapshot>>;

type ResidentChildEdge = (u64, RawHash);
type ResidentBlobSet = HashSet<RawHash>;

const HANDLE_PREFIX_BITMAP_WORDS: usize = (1 << 16) / u64::BITS as usize;

/// Exact membership on the usually small newly-resident side of a refresh.
///
/// Every aligned candidate pays only one 16-bit bitmap probe. A full 32-byte
/// comparison happens only when some newly resident handle shares that prefix;
/// there is no hash computation and neither false positives nor false
/// negatives.
struct AddedBlobMatcher {
    handles: Vec<RawHash>,
    prefixes: Vec<u64>,
}

impl AddedBlobMatcher {
    fn new(mut handles: Vec<RawHash>) -> Self {
        handles.sort_unstable();
        handles.dedup();
        let mut prefixes = vec![0; HANDLE_PREFIX_BITMAP_WORDS];
        for handle in &handles {
            let prefix = usize::from(u16::from_be_bytes([handle[0], handle[1]]));
            prefixes[prefix / u64::BITS as usize] |= 1 << (prefix % u64::BITS as usize);
        }
        Self { handles, prefixes }
    }

    fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    fn contains_handle(&self, handle: &RawHash) -> bool {
        self.handles.binary_search(handle).is_ok()
    }

    #[inline(always)]
    fn contains_chunk(&self, chunk: &[u8]) -> bool {
        let prefix = usize::from(u16::from_be_bytes([chunk[0], chunk[1]]));
        if self.prefixes[prefix / u64::BITS as usize] & (1 << (prefix % u64::BITS as usize)) == 0 {
            return false;
        }
        self.contains_prefixed_chunk(chunk)
    }

    #[cold]
    #[inline(never)]
    fn contains_prefixed_chunk(&self, chunk: &[u8]) -> bool {
        self.handles
            .binary_search_by(|handle| handle.as_slice().cmp(chunk))
            .is_ok()
    }
}

#[derive(Default)]
struct BlobDeltaInvalidationStats {
    #[cfg(test)]
    added: usize,
    #[cfg(test)]
    removed: usize,
    #[cfg(test)]
    unique_parents: usize,
    #[cfg(test)]
    parent_scans: usize,
    #[cfg(test)]
    word_probes: usize,
    #[cfg(test)]
    matched_parents: usize,
    #[cfg(test)]
    invalidated_collections: usize,
}

/// Freeze the exact resident-handle relation once for one immutable store
/// observation.
///
/// Full-replica discovery is a hash semijoin between the aligned 32-byte words
/// of reachable blobs and this set. Building the set once avoids asking the
/// store's persistent occurrence trie the same negative membership question
/// for every arbitrary word in a large blob.
fn resident_blob_set<R>(snapshot: &R) -> Result<ResidentBlobSet, R::Err>
where
    R: BlobStoreList,
{
    let mut resident = ResidentBlobSet::new();
    for info in snapshot.blobs() {
        resident.insert(info?.handle.raw);
    }
    Ok(resident)
}

#[derive(Default)]
struct FullReplicaChildCache {
    edges: HashMap<RawHash, Vec<ResidentChildEdge>>,
    unreadable: HashSet<RawHash>,
    #[cfg(test)]
    scans: usize,
    #[cfg(test)]
    membership_probes: usize,
}

impl FullReplicaChildCache {
    fn new() -> Self {
        Self::default()
    }
}

/// Discover the resident aligned child handles of one blob once per immutable
/// store observation.
///
/// Reachability remains collection-local: callers still apply their own
/// `visited` set and choose the canonical parent edge for their forest. This
/// cache shares only the expensive, collection-independent fact that a given
/// parent byte string contains a resident handle at a given aligned offset.
fn resident_child_edges<'a, R>(
    snapshot: &R,
    parent: RawHash,
    resident: &ResidentBlobSet,
    cache: &'a mut FullReplicaChildCache,
) -> (&'a [ResidentChildEdge], bool)
where
    R: BlobStoreGet,
{
    if !cache.edges.contains_key(&parent) {
        #[cfg(test)]
        {
            cache.scans += 1;
        }
        let mut edges = Vec::new();
        match snapshot.get::<Bytes, UnknownBlob>(Inline::new(parent)) {
            Ok(bytes) => {
                for (index, chunk) in bytes.chunks_exact(32).enumerate() {
                    let child: RawHash = chunk.try_into().expect("fixed-width chunk");
                    #[cfg(test)]
                    {
                        cache.membership_probes += 1;
                    }
                    if resident.contains(&child) {
                        edges.push((index as u64, child));
                    }
                }
            }
            Err(_) => {
                cache.unreadable.insert(parent);
            }
        }
        cache.edges.insert(parent, edges);
    }
    (
        cache.edges.get(&parent).unwrap(),
        cache.unreadable.contains(&parent),
    )
}

fn build_full_replica_state<R>(
    snapshot: &R,
    activation: &CollectionActivationOverlay,
    resident: &ResidentBlobSet,
    child_cache: &mut FullReplicaChildCache,
) -> FullReplicaState
where
    R: BlobStoreGet,
{
    let mut direct_roots = HashSet::new();
    direct_roots.insert(activation.collection().raw);
    for record in activation.records().records() {
        let CollectionRecord::Commit(commit) = record else {
            continue;
        };
        direct_roots.insert(commit.data().raw);
        direct_roots.insert(commit.metadata().raw);
    }
    let mut forest_keys = Vec::new();
    let mut visited = HashSet::new();
    let mut unreadable_parents = HashSet::new();
    let mut level = direct_roots.iter().copied().collect::<Vec<_>>();
    level.sort_unstable();
    level.retain(|handle| resident.contains(handle));
    for handle in &level {
        let mut key = [0; 80];
        key[8..40].copy_from_slice(handle);
        key[40..48].copy_from_slice(&u64::MAX.to_be_bytes());
        key[48..].copy_from_slice(handle);
        forest_keys.push(key);
        visited.insert(*handle);
    }
    let mut depth = 0_u64;
    while !level.is_empty() {
        let mut next = BTreeMap::<[u8; 32], ([u8; 32], u64)>::new();
        for parent in &level {
            let (edges, unreadable) =
                resident_child_edges(snapshot, *parent, resident, child_cache);
            if unreadable {
                unreadable_parents.insert(*parent);
            }
            for (index, child) in edges.iter().copied() {
                if visited.contains(&child) {
                    continue;
                }
                next.entry(child).or_insert((*parent, index));
            }
        }
        let Some(next_depth) = depth.checked_add(1) else {
            break;
        };
        depth = next_depth;
        level = Vec::with_capacity(next.len());
        for (child, (parent, index)) in next {
            let mut key = [0; 80];
            key[..8].copy_from_slice(&depth.to_be_bytes());
            key[8..40].copy_from_slice(&parent);
            key[40..48].copy_from_slice(&index.to_be_bytes());
            key[48..].copy_from_slice(&child);
            forest_keys.push(key);
            visited.insert(child);
            level.push(child);
        }
    }
    FullReplicaState {
        forest: DisclosureForestPatch::from_keys(forest_keys),
        direct_roots,
        unreadable_parents,
    }
}

/// Immutable host observation indexed exactly by active collection handle.
///
/// The semantic state of each value is the product constructed by
/// `CollectionActivationOverlay`: record PATCH × portable WRITE-evidence
/// PATCH. No global team inventory, proof list, or blob manifest is retained.
pub(crate) struct StoreSnapshot {
    collections: CollectionSnapshotIndex,
    blobs: Arc<dyn BlobSnapshotReader>,
    bearer_locators: Arc<BearerLocatorIndex>,
    resident_blobs: Option<Arc<ResidentBlobSet>>,
    observed_at: hifitime::Epoch,
    next_authorization_change: Option<hifitime::Epoch>,
}

/// Identify exactly which prior Full forests a semantic resident-blob delta
/// can change without retaining the enormous relation of absent aligned words.
///
/// If a new vertex becomes reachable, the first new vertex on its root path is
/// either a newly resident direct root or is named by an already reachable
/// parent. Removals can matter only when the removed vertex was reachable.
/// Consequently the existing forest is complete positive evidence: additions
/// require one shared streaming semijoin against old reachable parent bytes,
/// while removals need only a forest projection.
fn blob_delta_invalidations<R>(
    snapshot: &R,
    previous: &StoreSnapshot,
    previous_resident: &ResidentBlobSet,
    resident: &ResidentBlobSet,
) -> (HashSet<RawHash>, BlobDeltaInvalidationStats)
where
    R: BlobStoreGet,
{
    let added = AddedBlobMatcher::new(resident.difference(previous_resident).copied().collect());
    let removed = previous_resident
        .difference(resident)
        .copied()
        .collect::<HashSet<_>>();
    #[cfg(test)]
    let mut stats = BlobDeltaInvalidationStats::default();
    #[cfg(not(test))]
    let stats = BlobDeltaInvalidationStats::default();
    #[cfg(test)]
    {
        stats.added = added.handles.len();
        stats.removed = removed.len();
    }
    let prior = previous
        .collections
        .iter_ordered()
        .filter_map(|key| {
            previous
                .collections
                .get(key)
                .cloned()
                .map(|collection| (*key, collection))
        })
        .collect::<Vec<_>>();
    let mut invalidated = HashSet::new();
    let mut reachable_parents = HashSet::new();
    for (collection, prior) in &prior {
        if !prior.full.unreadable_parents.is_empty() {
            invalidated.insert(*collection);
        }
        if prior
            .full
            .direct_roots
            .iter()
            .any(|root| added.contains_handle(root))
        {
            invalidated.insert(*collection);
        }
        if added.is_empty() && removed.is_empty() {
            continue;
        }
        for entry in prior.full.forest.iter_ordered() {
            let reachable: RawHash = entry[48..].try_into().expect("forest child width");
            if !removed.is_empty() && removed.contains(&reachable) {
                invalidated.insert(*collection);
            }
            if !added.is_empty() {
                reachable_parents.insert(reachable);
            }
        }
    }

    if added.is_empty() {
        #[cfg(test)]
        {
            stats.invalidated_collections = invalidated.len();
        }
        return (invalidated, stats);
    }

    #[cfg(test)]
    {
        stats.unique_parents = reachable_parents.len();
    }
    let mut ordered_parents = reachable_parents.into_iter().collect::<Vec<_>>();
    ordered_parents.sort_unstable();
    let mut matched_parents = HashSet::new();
    for parent in ordered_parents {
        let Ok(bytes) = snapshot.get::<Bytes, UnknownBlob>(Inline::new(parent)) else {
            continue;
        };
        #[cfg(test)]
        {
            stats.parent_scans += 1;
        }
        for chunk in bytes.chunks_exact(32) {
            #[cfg(test)]
            {
                stats.word_probes += 1;
            }
            if added.contains_chunk(chunk) {
                matched_parents.insert(parent);
                break;
            }
        }
    }
    #[cfg(test)]
    {
        stats.matched_parents = matched_parents.len();
    }

    if !matched_parents.is_empty() {
        for (collection, prior) in &prior {
            if prior.full.forest.iter_ordered().any(|entry| {
                let reachable: RawHash = entry[48..].try_into().expect("forest child width");
                matched_parents.contains(&reachable)
            }) {
                invalidated.insert(*collection);
            }
        }
    }
    #[cfg(test)]
    {
        stats.invalidated_collections = invalidated.len();
    }
    (invalidated, stats)
}

impl StoreSnapshot {
    pub(crate) fn from_store_changes<R>(
        snapshot: R,
        active: &ActiveCollections,
        full_dirty: &ActiveCollections,
        local: VerifyingKey,
        previous_store: Option<&R>,
        previous: Option<&Self>,
        changes: StoreChanges,
        authorization_changed: bool,
        full_replication: bool,
        next_authorization_change: Option<hifitime::Epoch>,
        instant: hifitime::Epoch,
    ) -> anyhow::Result<Self>
    where
        R: StoreRead + BlobChildren + Clone,
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
        let activation_inputs_changed = changes.contains(StoreChanges::BLOBS)
            || changes.contains(StoreChanges::COLLECTION_RECORDS)
            || changes.contains(StoreChanges::CAPABILITY_PROOFS)
            || authorization_changed;
        let mut full_child_cache = FullReplicaChildCache::new();
        let resident_blobs = if !full_replication {
            None
        } else if changes.contains(StoreChanges::BLOBS)
            || previous
                .and_then(|prior| prior.resident_blobs.as_ref())
                .is_none()
        {
            Some(Arc::new(resident_blob_set(&snapshot)?))
        } else {
            previous.and_then(|prior| prior.resident_blobs.clone())
        };
        let resident_delta_known = previous
            .and_then(|prior| prior.resident_blobs.as_ref())
            .is_some();
        let blob_invalidated = match (
            changes.contains(StoreChanges::BLOBS),
            previous,
            previous.and_then(|prior| prior.resident_blobs.as_deref()),
            resident_blobs.as_deref(),
        ) {
            (true, Some(previous), Some(previous_resident), Some(resident)) => {
                blob_delta_invalidations(&snapshot, previous, previous_resident, resident).0
            }
            _ => HashSet::new(),
        };
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
            let activation_result = if !activation_inputs_changed {
                prior
                    .as_ref()
                    .map(|prior| prior.activation.clone())
                    .map_or_else(
                        || {
                            collection_activation_overlay_at(&snapshot, collection, instant)
                                .map(Arc::new)
                        },
                        Ok,
                    )
            } else {
                collection_activation_overlay_at(&snapshot, collection, instant).map(|fresh| {
                    prior
                        .as_ref()
                        .filter(|prior| prior.wake_root() == fresh.wake_root())
                        .map_or_else(|| Arc::new(fresh), |prior| prior.activation.clone())
                })
            };
            let activation = match activation_result {
                Ok(activation) => activation,
                Err(CollectionActivationOverlayError::Descriptor(error)) => {
                    warn!(collection = %hex::encode(&collection.raw[..4]), %error, "active collection descriptor is unavailable or invalid; isolating collection");
                    continue;
                }
                Err(error) => return Err(anyhow::Error::new(error)),
            };
            let read_evidence = if !activation_inputs_changed
                && !authorization_changed
                && prior.is_some()
            {
                prior.as_ref().unwrap().read_evidence.clone()
            } else {
                match collection_read_evidence_bundles_at(
                    &snapshot,
                    collection,
                    local,
                    MAX_COLLECTION_READ_BUNDLES,
                    instant,
                ) {
                    Ok(evidence) => evidence.into(),
                    Err(CollectionReadEvidenceError::TooMany { count, limit }) => {
                        warn!(
                            collection = %hex::encode(&collection.raw[..4]),
                            count,
                            limit,
                            "collection READ witness exceeds network bound; collection remains locally active but cannot be presented remotely"
                        );
                        Arc::from([])
                    }
                    Err(error) => return Err(anyhow::Error::new(error)),
                }
            };
            let full = if !full_replication {
                Arc::new(FullReplicaState {
                    forest: DisclosureForestPatch::new(),
                    direct_roots: HashSet::new(),
                    unreadable_parents: HashSet::new(),
                })
            } else if let Some(prior) = prior.as_ref().filter(|prior| {
                resident_delta_known
                    && Arc::ptr_eq(&activation, &prior.activation)
                    && (!changes.contains(StoreChanges::BLOBS)
                        || blob_invalidated.get(&collection.raw).is_none())
                    && full_dirty.get(&collection.raw).is_none()
            }) {
                prior.full.clone()
            } else {
                Arc::new(build_full_replica_state(
                    &snapshot,
                    &activation,
                    resident_blobs
                        .as_ref()
                        .expect("resident set was frozen before Full discovery"),
                    &mut full_child_cache,
                ))
            };
            let value = Arc::new(CollectionSnapshot {
                activation,
                read_evidence,
                full,
                blobs: blob_reader.clone(),
            });
            collections.insert(&PatchEntry::with_value(raw, value));
        }
        Ok(Self {
            collections,
            blobs: blob_reader,
            bearer_locators,
            resident_blobs,
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

    fn notices(&self) -> Vec<(CollectionHandle, [u8; 32], [u8; 32])> {
        self.collections()
            .map(|collection| {
                (
                    collection.collection(),
                    collection.wake_root(),
                    PatchSummary::from_patch(&collection.full.forest)
                        .root()
                        .unwrap_or([0; 32]),
                )
            })
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
type OperationalBlobSlot = Arc<Mutex<Option<Arc<dyn BlobSnapshotReader>>>>;

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
    can_fetch: bool,
}

impl<T: Transport> NetCapability for NetCap<T> {
    fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Bytes>> {
        let client = self.client.clone();
        let can_fetch = self.can_fetch;
        Box::pin(async move {
            if !can_fetch {
                return None;
            }
            client.fetch_blob(hash).await
        })
    }
}

/// Default end-to-end budget for an interactive exact blob read.
pub const INTERACTIVE_FETCH_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
pub struct NetSender {
    cmd_tx: mpsc::Sender<NetCommand>,
    snapshot: SnapshotSlot,
    operational_blobs: OperationalBlobSlot,
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

    pub(crate) fn update_operational_blobs<R>(&self, snapshot: R)
    where
        R: BlobStoreGet + Clone + Send + 'static,
    {
        *self.operational_blobs.lock().unwrap() =
            Some(Arc::new(CloneableBlobSnapshotReader(Mutex::new(snapshot))));
    }

    pub(crate) fn update_snapshot(&self, snapshot: StoreSnapshot, active: &ActiveCollections) {
        let mut notices = snapshot.notices();
        for raw in active.iter_ordered() {
            if !notices
                .iter()
                .any(|(collection, _, _)| collection.raw == *raw)
            {
                notices.push((CollectionHandle::new(*raw), [0; 32], [0; 32]));
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
    operational_blobs: OperationalBlobSlot,
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
    let operational_blobs = Arc::new(Mutex::new(None));
    let (cap_tx, cap_rx) = tokio::sync::watch::channel(None);
    (
        NetSender {
            cmd_tx,
            snapshot: snapshot.clone(),
            operational_blobs: operational_blobs.clone(),
            cap: cap_rx,
            id,
        },
        NetReceiver { evt_rx },
        HostWiring {
            cmd_rx,
            evt_tx,
            snapshot,
            operational_blobs,
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
    full_cursor: Option<FullReplicaCursor>,
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
        can_fetch: config.qos.direction.pulls(),
    });
    let _ = wiring.cap_tx.send(Some(cap as Arc<dyn NetCapability>));

    let handler = SnapshotHandler {
        snapshot: wiring.snapshot.clone(),
        candidates: candidates.clone(),
        providers: providers.clone(),
        serve_data: config.qos.direction.serves(),
        local_id: my_id,
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
    let mut full_cursors: HashMap<RepairTarget, FullReplicaCursor> = HashMap::new();
    let mut current_roots: HashMap<[u8; 32], ([u8; 32], [u8; 32])> = HashMap::new();
    let mut next_period = crate::clock::mono_now();
    let mut next_discovery = crate::clock::mono_now();
    let mut publisher = ProviderPublisher::new(crate::clock::mono_now());
    let publication_limit = config.provider_publication_budget;
    let mut publication_budget = ProviderPublicationBudget::new(publication_limit);
    let mut publication_budget_reported = false;
    if publication_budget.is_exhausted() && config.qos.direction.serves() {
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
                        full_cursors.clear();
                        for (_, topic) in wake_topics.drain() {
                            let _ = topic.send(WakeCommand::Shutdown);
                        }
                        continue;
                    }
                    let mut observed = HashSet::new();
                    for (collection, semantic_root, payload_root) in notice.collections {
                        observed.insert(collection.raw);
                        if !current_roots.contains_key(&collection.raw) {
                            let now = crate::clock::mono_now();
                            discovery.insert(collection.raw, DiscoveryState::new(now));
                            next_discovery = next_discovery.min(now);
                        }
                        let roots = (semantic_root, payload_root);
                        let changed = current_roots.insert(collection.raw, roots) != Some(roots);
                        let topic = wake_topics.entry(collection.raw).or_insert_with(|| {
                            spawn_wake_topic(
                                wake_plane.clone(),
                                collection,
                                bootstrap_ids.clone(),
                                wake_tx.clone(),
                            )
                        });
                        if changed && semantic_root != [0; 32] {
                            let _ = topic.send(WakeCommand::Observe(
                                CollectionWakeRoot::with_payload(semantic_root, payload_root),
                            ));
                        }
                    }
                    current_roots.retain(|collection, _| observed.contains(collection));
                    discovery.retain(|collection, _| observed.contains(collection));
                    retain_active_repair_state(&mut failures, &observed);
                    retain_active_repair_state(&mut full_cursors, &observed);
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
                .is_some_and(|(semantic, payload)| {
                    *semantic != [0; 32]
                        && (semantic != wake.root().as_bytes()
                            || (matches!(config.qos.blobs, BlobReplication::Full)
                                && payload != wake.root().payload_bytes()))
                })
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
                full_cursors.remove(&outcome.target);
                continue;
            }
            if outcome.target.peer == my_id {
                failures.remove(&outcome.target);
                full_cursors.remove(&outcome.target);
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
                    if let Some(cursor) = outcome.full_cursor {
                        full_cursors.insert(outcome.target, cursor);
                    }
                } else {
                    full_cursors.remove(&outcome.target);
                }
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
                let operational_blobs = wiring.operational_blobs.clone();
                let repair_tx = repair_tx.clone();
                let full = matches!(config.qos.blobs, BlobReplication::Full);
                let full_cursor = full_cursors.get(&target).cloned();
                tokio::spawn(async move {
                    let result = tokio::time::timeout(
                        REPAIR_DEADLINE,
                        reconcile_collection_peer(
                            &transport,
                            &pool,
                            target,
                            local,
                            full_cursor,
                            &operational_blobs,
                            &events,
                            full,
                        ),
                    )
                    .await;
                    let (success, more, full_cursor) = match result {
                        Ok(Ok((more, cursor))) => (true, more, cursor),
                        Ok(Err(error)) => {
                            debug!(%error, "collection repair failed");
                            (false, false, None)
                        }
                        Err(_) => (false, false, None),
                    };
                    let _ = repair_tx.send(RepairOutcome {
                        target,
                        success,
                        more,
                        full_cursor,
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
    prior_cursor: Option<FullReplicaCursor>,
    operational_blobs: &OperationalBlobSlot,
    events: &tokio::sync::mpsc::Sender<NetEventBatch>,
    full: bool,
) -> anyhow::Result<(bool, Option<FullReplicaCursor>)> {
    let connection = pool_get(transport, pool, target.peer).await?;
    let reader = operational_blobs
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| local.blobs.clone());
    let delta = match pull_collection(
        connection.conn(),
        &local.activation,
        local.read_evidence.iter().cloned().collect(),
        &local.full,
        prior_cursor.as_ref(),
        |hash| reader.get_blob(hash),
        full,
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
    for bundle in delta.write_evidence {
        admissions
            .push(NetEvent::CapabilityProofBundle(bundle))
            .await?;
    }
    for record in delta.records {
        admissions.push(NetEvent::CollectionRecord(record)).await?;
    }
    if !delta.blobs.is_empty() || (full && !delta.more) {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        admissions
            .push(NetEvent::FullPage {
                collection: target.collection,
                blobs: delta.blobs,
                final_page: !delta.more,
                ack: ack_tx,
            })
            .await?;
        admissions.flush().await?;
        ack_rx
            .await
            .map_err(|_| anyhow::anyhow!("store side dropped Full page before durability"))?;
        return Ok((delta.more, delta.full_cursor));
    }
    admissions.flush().await?;
    Ok((delta.more, delta.full_cursor))
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
            .collect();
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
    serve_data: bool,
    local_id: PeerId,
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
                if !self.serve_data {
                    serve_collection_repair(recv, send, peer, |_| None, |_, _| None).await?;
                } else {
                    let snapshot = self.snapshot.lock().unwrap().clone();
                    let blob_snapshot = snapshot.clone();
                    serve_collection_repair(
                        recv,
                        send,
                        peer,
                        move |collection| {
                            snapshot
                                .as_ref()
                                .and_then(|snapshot| snapshot.collection(collection))
                                .map(|collection| {
                                    (
                                        collection.activation.clone(),
                                        collection.read_evidence.clone(),
                                        collection.full.clone(),
                                    )
                                })
                        },
                        move |_collection, hash| {
                            blob_snapshot
                                .as_ref()
                                .and_then(|snapshot| snapshot.get_blob(&hash))
                        },
                    )
                    .await?;
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
                        self.serve_data
                            .then(|| {
                                snapshot
                                    .as_ref()
                                    .and_then(|snapshot| snapshot.bearer_handle(locator))
                            })
                            .flatten()
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
    use std::collections::{BTreeSet, HashMap, HashSet};
    use std::sync::Arc;

    use anybytes::Bytes;
    use ed25519_dalek::SigningKey;
    use iroh_base::EndpointId;
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::MemoryBlobStore;
    use triblespace_core::blob::encodings::UnknownBlob;
    use triblespace_core::collection::{
        AdmissionPolicy, Collection, CollectionHandle, CollectionPolicy, CollectionStoreExt,
    };
    use triblespace_core::id::{ExclusiveId, Id};
    use triblespace_core::inline::Inline;
    use triblespace_core::inline::encodings::hash::Handle;
    use triblespace_core::patch::{Entry as PatchEntry, PATCH};
    use triblespace_core::repo::hybridstore::HybridStore;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{
        BlobStoreGet, BlobStoreList, BlobStorePut, SnapshotSource, StoreChanges,
        StoreSnapshot as CoreStoreSnapshot,
    };
    use triblespace_core::trible::{Fragment, Trible, TribleSet};

    use crate::channel::{NetEvent, NetEventBatch};
    use crate::inventory::{BlobReplication, ReconcileQos};
    use crate::peer::Peer;
    use crate::provider::{
        MAX_PROVIDERS_PER_KEY, ProviderObservation, ProviderPublisher, ProviderPutResult,
        PublicationResult,
    };
    use crate::routing::K;
    use crate::transport::PeerId;

    use super::{
        ActiveCollections, AddedBlobMatcher, COLLECTION_PARTICIPANT_LEASE, DiscoveryState,
        FullReplicaChildCache, FullReplicaState, MAX_COLLECTION_PARTICIPANTS, MAX_PENDING_REPAIRS,
        ProviderPublicationBudget, RepairTarget, StoreSnapshot, WakeBootstrapPeers,
        blob_delta_invalidations, build_full_replica_state, canonical_provider_subset,
        collection_activation_overlay_at, enqueue_repair, forget_participant, has_repair_candidate,
        live_participants, observe_participant, resident_blob_set, resident_child_edges,
        retain_active_repair_state,
    };

    fn endpoint(byte: u8) -> EndpointId {
        EndpointId::from_bytes(
            SigningKey::from_bytes(&[byte; 32])
                .verifying_key()
                .as_bytes(),
        )
        .unwrap()
    }

    fn fragment(seed: u8, value: Inline<Handle<UnknownBlob>>) -> Fragment {
        let entity = Id::new([seed; 16]).unwrap();
        let attribute = Id::new([seed.wrapping_add(1); 16]).unwrap();
        let mut facts = TribleSet::new();
        facts.insert(&Trible::new(
            ExclusiveId::force_ref(&entity),
            &attribute,
            &value,
        ));
        Fragment::from_parts(facts, TribleSet::new(), MemoryBlobStore::new())
    }

    fn active(
        collections: impl IntoIterator<
            Item = Collection<triblespace_core::blob::encodings::simplearchive::SimpleArchive>,
        >,
    ) -> ActiveCollections {
        let mut active = PATCH::new();
        for collection in collections {
            active.insert(&PatchEntry::new(&collection.handle().raw));
        }
        active
    }

    fn full(
        snapshot: &StoreSnapshot,
        collection: Collection<triblespace_core::blob::encodings::simplearchive::SimpleArchive>,
    ) -> Arc<FullReplicaState> {
        snapshot
            .collection(collection.handle())
            .unwrap()
            .full
            .clone()
    }

    fn observed_at() -> hifitime::Epoch {
        hifitime::Epoch::from_gregorian_utc_at_midnight(2026, 1, 1)
    }

    #[test]
    fn added_blob_matcher_is_exact_across_empty_and_shared_prefixes() {
        let first = [0x11; 32];
        let mut same_prefix = [0x11; 32];
        same_prefix[31] = 0x12;
        let other = [0x22; 32];
        let matcher = AddedBlobMatcher::new(vec![other, first, same_prefix, first]);

        assert!(matcher.contains_handle(&first));
        assert!(matcher.contains_chunk(&same_prefix));
        assert!(matcher.contains_chunk(&other));
        let mut absent_same_prefix = first;
        absent_same_prefix[30] = 0x99;
        assert!(!matcher.contains_chunk(&absent_same_prefix));
        assert!(!matcher.contains_chunk(&[0x33; 32]));
        assert!(!AddedBlobMatcher::new(Vec::new()).contains_chunk(&[0; 32]));
    }

    #[test]
    fn resident_child_cache_reuses_schema_agnostic_aligned_discovery() {
        let mut store = MemoryRepo::default();
        let child = store
            .put::<UnknownBlob, _>(Bytes::from_source(b"resident child".to_vec()))
            .unwrap();
        let mut parent_bytes = vec![0xA5; 96];
        parent_bytes[32..64].copy_from_slice(&child.raw);
        let parent = store
            .put::<UnknownBlob, _>(Bytes::from_source(parent_bytes))
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let resident = resident_blob_set(&snapshot).unwrap();
        let mut cache = FullReplicaChildCache::new();

        let expected = snapshot
            .get::<Bytes, UnknownBlob>(parent)
            .unwrap()
            .chunks_exact(32)
            .enumerate()
            .filter_map(|(index, chunk)| {
                let child = <[u8; 32]>::try_from(chunk).unwrap();
                snapshot
                    .contains_blob(Inline::<Handle<UnknownBlob>>::new(child))
                    .unwrap()
                    .then_some((index as u64, child))
            })
            .collect::<Vec<_>>();
        let (first, first_unreadable) =
            resident_child_edges(&snapshot, parent.raw, &resident, &mut cache);
        let first_ptr = first.as_ptr();
        assert!(!first_unreadable);
        assert_eq!(first, expected);
        assert_eq!(first, &[(1, child.raw)]);
        let (second, second_unreadable) =
            resident_child_edges(&snapshot, parent.raw, &resident, &mut cache);

        assert_eq!(first_ptr, second.as_ptr());
        assert!(!second_unreadable);
        assert_eq!(cache.scans, 1);
        assert_eq!(cache.membership_probes, 3);
    }

    #[test]
    fn resident_child_cache_retains_unreadable_parent_evidence() {
        let mut store = MemoryRepo::default();
        let snapshot = store.snapshot().unwrap();
        let parent = [0xE7; 32];
        let resident = HashSet::from([parent]);
        let mut cache = FullReplicaChildCache::new();

        let (edges, unreadable) = resident_child_edges(&snapshot, parent, &resident, &mut cache);
        assert!(edges.is_empty());
        assert!(unreadable);
        let (_, unreadable_again) = resident_child_edges(&snapshot, parent, &resident, &mut cache);
        assert!(unreadable_again);
        assert_eq!(cache.scans, 1);
    }

    #[test]
    fn physical_blob_noop_retries_only_the_owner_of_an_unreadable_parent() {
        let key = SigningKey::from_bytes(&[0x3A; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let mut store = MemoryRepo::default();
        let affected = store
            .collection("unreadable-parent-owner", policy.clone())
            .unwrap();
        let untouched = store.collection("unreadable-parent-other", policy).unwrap();
        store
            .commit(affected, &key, fragment(22, Inline::new([0xB1; 32])))
            .unwrap();
        store
            .commit(untouched, &key, fragment(23, Inline::new([0xB2; 32])))
            .unwrap();
        let active = active([affected, untouched]);
        let clean = ActiveCollections::new();
        let store_snapshot = store.snapshot().unwrap();
        let baseline = StoreSnapshot::from_store_changes(
            store_snapshot.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();

        // Model a prior read failure without constructing a corrupt backend:
        // the positive repair evidence is the only state relevant to this
        // invalidation rule.
        let affected_prior = baseline.collection(affected.handle()).unwrap();
        let untouched_prior = baseline.collection(untouched.handle()).unwrap();
        let affected_full = Arc::new(FullReplicaState {
            forest: affected_prior.full.forest.clone(),
            direct_roots: affected_prior.full.direct_roots.clone(),
            unreadable_parents: HashSet::from([[0xE7; 32]]),
        });
        let mut collections = super::CollectionSnapshotIndex::new();
        collections.insert(&PatchEntry::with_value(
            &affected.handle().raw,
            Arc::new(super::CollectionSnapshot {
                activation: affected_prior.activation.clone(),
                read_evidence: affected_prior.read_evidence.clone(),
                full: affected_full.clone(),
                blobs: affected_prior.blobs.clone(),
            }),
        ));
        collections.insert(&PatchEntry::with_value(
            &untouched.handle().raw,
            Arc::new(super::CollectionSnapshot {
                activation: untouched_prior.activation.clone(),
                read_evidence: untouched_prior.read_evidence.clone(),
                full: untouched_prior.full.clone(),
                blobs: untouched_prior.blobs.clone(),
            }),
        ));
        let prior = StoreSnapshot {
            collections,
            blobs: baseline.blobs.clone(),
            bearer_locators: baseline.bearer_locators.clone(),
            resident_blobs: baseline.resident_blobs.clone(),
            observed_at: baseline.observed_at,
            next_authorization_change: baseline.next_authorization_change,
        };
        let resident = prior.resident_blobs.as_deref().unwrap();
        let (invalidated, stats) =
            blob_delta_invalidations(&store_snapshot, &prior, resident, resident);
        assert_eq!(stats.added, 0);
        assert_eq!(stats.removed, 0);
        assert_eq!(stats.parent_scans, 0);
        assert_eq!(stats.invalidated_collections, 1);
        assert!(invalidated.contains(&affected.handle().raw));
        assert!(!invalidated.contains(&untouched.handle().raw));

        let refreshed = StoreSnapshot::from_store_changes(
            store_snapshot.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&store_snapshot),
            Some(&prior),
            StoreChanges::BLOBS,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&affected_full, &full(&refreshed, affected)));
        assert!(Arc::ptr_eq(
            &untouched_prior.full,
            &full(&refreshed, untouched)
        ));
    }

    #[test]
    fn full_forests_share_discovery_but_keep_collection_local_reachability() {
        let key = SigningKey::from_bytes(&[0x37; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let mut store = MemoryRepo::default();
        let child = store
            .put::<UnknownBlob, _>(Bytes::from_source(b"shared collection child".to_vec()))
            .unwrap();
        let first = store
            .collection("shared-scan-first", policy.clone())
            .unwrap();
        let second = store.collection("shared-scan-second", policy).unwrap();
        let first_commit = store.commit(first, &key, fragment(9, child)).unwrap();
        let second_commit = store.commit(second, &key, fragment(9, child)).unwrap();
        assert_eq!(first_commit.data(), second_commit.data());

        let snapshot = store.snapshot().unwrap();
        let resident = resident_blob_set(&snapshot).unwrap();
        let first_activation =
            collection_activation_overlay_at(&snapshot, first.handle(), observed_at()).unwrap();
        let second_activation =
            collection_activation_overlay_at(&snapshot, second.handle(), observed_at()).unwrap();
        let mut baseline_first_cache = FullReplicaChildCache::new();
        let baseline_first = build_full_replica_state(
            &snapshot,
            &first_activation,
            &resident,
            &mut baseline_first_cache,
        );
        let mut baseline_second_cache = FullReplicaChildCache::new();
        let baseline_second = build_full_replica_state(
            &snapshot,
            &second_activation,
            &resident,
            &mut baseline_second_cache,
        );
        let baseline_scans = baseline_first_cache.scans + baseline_second_cache.scans;
        let baseline_probes =
            baseline_first_cache.membership_probes + baseline_second_cache.membership_probes;

        let mut shared_cache = FullReplicaChildCache::new();
        let first_full =
            build_full_replica_state(&snapshot, &first_activation, &resident, &mut shared_cache);
        let second_full =
            build_full_replica_state(&snapshot, &second_activation, &resident, &mut shared_cache);
        let forest_entries = first_full.forest.len() + second_full.forest.len();

        eprintln!(
            "full replica fixture: discovery scans {baseline_scans} -> {}, membership probes {baseline_probes} -> {}, forest entries {forest_entries} -> {forest_entries}",
            shared_cache.scans, shared_cache.membership_probes,
        );

        assert!(shared_cache.scans < baseline_scans);
        assert!(shared_cache.membership_probes < baseline_probes);
        assert_eq!(first_full.forest, baseline_first.forest);
        assert_eq!(second_full.forest, baseline_second.forest);
        assert_eq!(
            forest_entries,
            baseline_first.forest.len() + baseline_second.forest.len(),
            "sharing discovery must not share or suppress collection-local forest entries"
        );
        assert!(first_full.direct_roots.contains(&first.handle().raw));
        assert!(!first_full.direct_roots.contains(&second.handle().raw));
        assert!(second_full.direct_roots.contains(&second.handle().raw));
        assert!(!second_full.direct_roots.contains(&first.handle().raw));
        assert!(
            first_full
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );
        assert!(
            second_full
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );
    }

    #[test]
    fn delta_semijoin_selects_shared_owners_and_rebuild_recovers_a_batch_cascade() {
        let key = SigningKey::from_bytes(&[0x38; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let leaf_blob = Blob::<UnknownBlob>::new(Bytes::from_source(b"cascade leaf".to_vec()));
        let leaf = leaf_blob.get_handle();
        let parent_blob = Blob::<UnknownBlob>::new(Bytes::from_source(leaf.raw.to_vec()));
        let parent = parent_blob.get_handle();
        let mut store = MemoryRepo::default();
        let first = store
            .collection("delta-shared-first", policy.clone())
            .unwrap();
        let second = store
            .collection("delta-shared-second", policy.clone())
            .unwrap();
        let untouched = store.collection("delta-untouched", policy).unwrap();
        let first_commit = store.commit(first, &key, fragment(19, parent)).unwrap();
        let second_commit = store.commit(second, &key, fragment(19, parent)).unwrap();
        store
            .commit(untouched, &key, fragment(20, Inline::new([0xD0; 32])))
            .unwrap();
        assert_eq!(first_commit.data(), second_commit.data());

        let active = active([first, second, untouched]);
        let clean = ActiveCollections::new();
        let before_store = store.snapshot().unwrap();
        let before = StoreSnapshot::from_store_changes(
            before_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let first_before = full(&before, first);
        let second_before = full(&before, second);
        let untouched_before = full(&before, untouched);

        store.put::<UnknownBlob, _>(parent_blob).unwrap();
        store.put::<UnknownBlob, _>(leaf_blob).unwrap();
        store
            .put::<UnknownBlob, _>(Bytes::from_source(b"unrelated delta member".to_vec()))
            .unwrap();
        let after_store = store.snapshot().unwrap();
        let resident = resident_blob_set(&after_store).unwrap();
        let (invalidated, stats) = blob_delta_invalidations(
            &after_store,
            &before,
            before.resident_blobs.as_deref().unwrap(),
            &resident,
        );
        assert_eq!(stats.added, 3);
        assert_eq!(stats.removed, 0);
        assert_eq!(stats.parent_scans, stats.unique_parents);
        assert_eq!(stats.matched_parents, 1);
        assert_eq!(stats.invalidated_collections, 2);
        assert!(invalidated.contains(&first.handle().raw));
        assert!(invalidated.contains(&second.handle().raw));
        assert!(!invalidated.contains(&untouched.handle().raw));

        let after = StoreSnapshot::from_store_changes(
            after_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&before_store),
            Some(&before),
            after_store.changes_since(&before_store),
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        assert!(!Arc::ptr_eq(&first_before, &full(&after, first)));
        assert!(!Arc::ptr_eq(&second_before, &full(&after, second)));
        assert!(Arc::ptr_eq(&untouched_before, &full(&after, untouched)));
        for collection in [first, second] {
            let selective = full(&after, collection);
            assert!(
                selective
                    .forest
                    .iter_ordered()
                    .any(|entry| entry[48..] == parent.raw)
            );
            assert!(
                selective
                    .forest
                    .iter_ordered()
                    .any(|entry| entry[48..] == leaf.raw)
            );
            let activation =
                collection_activation_overlay_at(&after_store, collection.handle(), observed_at())
                    .unwrap();
            let mut oracle_cache = FullReplicaChildCache::new();
            let oracle =
                build_full_replica_state(&after_store, &activation, &resident, &mut oracle_cache);
            assert_eq!(selective.forest, oracle.forest);
            assert_eq!(selective.direct_roots, oracle.direct_roots);
        }
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
    fn full_forests_rebuild_only_for_semantic_blob_deltas_or_explicit_dirty_marks() {
        let key = SigningKey::from_bytes(&[0x31; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let mut store = MemoryRepo::default();
        let first = store
            .collection("selective-full-first", policy.clone())
            .unwrap();
        let second = store.collection("selective-full-second", policy).unwrap();
        store
            .commit(first, &key, fragment(1, Inline::new([0xA1; 32])))
            .unwrap();
        store
            .commit(second, &key, fragment(2, Inline::new([0xA2; 32])))
            .unwrap();
        let active = active([first, second]);
        let clean = ActiveCollections::new();
        let first_store = store.snapshot().unwrap();
        let first_serving = StoreSnapshot::from_store_changes(
            first_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let first_before = full(&first_serving, first);
        let second_before = full(&first_serving, second);

        store
            .put::<UnknownBlob, _>(Bytes::from_source(b"unrelated cache bytes".to_vec()))
            .unwrap();
        store
            .commit(first, &key, fragment(3, Inline::new([0xA3; 32])))
            .unwrap();
        let second_store = store.snapshot().unwrap();
        let second_serving = StoreSnapshot::from_store_changes(
            second_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&first_store),
            Some(&first_serving),
            second_store.changes_since(&first_store),
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let first_changed = full(&second_serving, first);
        let second_changed = full(&second_serving, second);
        assert!(!Arc::ptr_eq(&first_before, &first_changed));
        assert!(Arc::ptr_eq(&second_before, &second_changed));

        store
            .put::<UnknownBlob, _>(Bytes::from_source(b"another unrelated cache blob".to_vec()))
            .unwrap();
        let third_store = store.snapshot().unwrap();
        let third_serving = StoreSnapshot::from_store_changes(
            third_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&second_store),
            Some(&second_serving),
            third_store.changes_since(&second_store),
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&first_changed, &full(&third_serving, first)));
        assert!(Arc::ptr_eq(&second_changed, &full(&third_serving, second)));

        let mut dirty = ActiveCollections::new();
        dirty.insert(&PatchEntry::new(&second.handle().raw));
        let dirty_serving = StoreSnapshot::from_store_changes(
            third_store.clone(),
            &active,
            &dirty,
            key.verifying_key(),
            Some(&third_store),
            Some(&third_serving),
            StoreChanges::NONE,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            &full(&third_serving, first),
            &full(&dirty_serving, first)
        ));
        assert!(!Arc::ptr_eq(
            &full(&third_serving, second),
            &full(&dirty_serving, second)
        ));
    }

    #[test]
    fn enabling_full_replication_builds_the_previously_absent_forest() {
        let key = SigningKey::from_bytes(&[0x3B; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let mut store = MemoryRepo::default();
        let collection = store.collection("enable-full", policy).unwrap();
        store
            .commit(collection, &key, fragment(24, Inline::new([0xB3; 32])))
            .unwrap();
        let active = active([collection]);
        let clean = ActiveCollections::new();
        let store_snapshot = store.snapshot().unwrap();
        let demand = StoreSnapshot::from_store_changes(
            store_snapshot.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            false,
            None,
            observed_at(),
        )
        .unwrap();
        let demand_full = full(&demand, collection);
        assert!(demand.resident_blobs.is_none());
        assert!(demand_full.forest.is_empty());

        let replicated = StoreSnapshot::from_store_changes(
            store_snapshot.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&store_snapshot),
            Some(&demand),
            StoreChanges::NONE,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        assert!(replicated.resident_blobs.is_some());
        assert!(!Arc::ptr_eq(&demand_full, &full(&replicated, collection)));
        assert!(!full(&replicated, collection).forest.is_empty());
    }

    #[test]
    fn generic_blob_arrival_advances_an_active_full_forest() {
        let key = SigningKey::from_bytes(&[0x35; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let child_bytes = Bytes::from_source(b"bearer-fetched child".to_vec());
        let child = Blob::<UnknownBlob>::new(child_bytes.clone()).get_handle();
        let mut store = MemoryRepo::default();
        let collection = store.collection("generic-full-arrival", policy).unwrap();
        store.commit(collection, &key, fragment(11, child)).unwrap();
        let active = active([collection]);
        let clean = ActiveCollections::new();
        let before_store = store.snapshot().unwrap();
        let before = StoreSnapshot::from_store_changes(
            before_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let before_payload = before.notices()[0].2;
        assert!(
            !full(&before, collection)
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );

        store.put::<UnknownBlob, _>(child_bytes).unwrap();
        let after_store = store.snapshot().unwrap();
        let after = StoreSnapshot::from_store_changes(
            after_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&before_store),
            Some(&before),
            after_store.changes_since(&before_store),
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();

        assert_ne!(before_payload, after.notices()[0].2);
        assert!(
            full(&after, collection)
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );
    }

    #[test]
    fn removed_reachable_handle_invalidates_its_owner_and_matches_full_rebuild() {
        let key = SigningKey::from_bytes(&[0x39; 32]);
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let mut store = HybridStore::new(MemoryBlobStore::new(), MemoryRepo::default());
        let child = store
            .put::<UnknownBlob, _>(Bytes::from_source(b"removable child".to_vec()))
            .unwrap();
        let collection = store.collection("delta-removal", policy).unwrap();
        store.commit(collection, &key, fragment(21, child)).unwrap();
        let active = active([collection]);
        let clean = ActiveCollections::new();
        let before_store = store.snapshot().unwrap();
        let before = StoreSnapshot::from_store_changes(
            before_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            None,
            None,
            StoreChanges::ALL,
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let before_full = full(&before, collection);
        assert!(
            before_full
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );

        let retained = store
            .blobs
            .snapshot()
            .unwrap()
            .blobs()
            .map(|info| info.unwrap().handle)
            .filter(|handle| handle.raw != child.raw)
            .collect::<Vec<_>>();
        store.blobs.keep(retained);
        let after_store = store.snapshot().unwrap();
        let resident = resident_blob_set(&after_store).unwrap();
        let (invalidated, stats) = blob_delta_invalidations(
            &after_store,
            &before,
            before.resident_blobs.as_deref().unwrap(),
            &resident,
        );
        assert_eq!(stats.added, 0);
        assert_eq!(stats.removed, 1);
        assert_eq!(stats.parent_scans, 0);
        assert!(invalidated.contains(&collection.handle().raw));

        let after = StoreSnapshot::from_store_changes(
            after_store.clone(),
            &active,
            &clean,
            key.verifying_key(),
            Some(&before_store),
            Some(&before),
            after_store.changes_since(&before_store),
            false,
            true,
            None,
            observed_at(),
        )
        .unwrap();
        let selective = full(&after, collection);
        assert!(!Arc::ptr_eq(&before_full, &selective));
        assert!(
            !selective
                .forest
                .iter_ordered()
                .any(|entry| entry[48..] == child.raw)
        );
        let activation =
            collection_activation_overlay_at(&after_store, collection.handle(), observed_at())
                .unwrap();
        let mut oracle_cache = FullReplicaChildCache::new();
        let oracle =
            build_full_replica_state(&after_store, &activation, &resident, &mut oracle_cache);
        assert_eq!(selective.forest, oracle.forest);
        assert_eq!(selective.direct_roots, oracle.direct_roots);
    }

    #[tokio::test]
    async fn nonfinal_full_page_retains_collection_route_until_checkpoint() {
        let key = SigningKey::from_bytes(&[0x32; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let child_bytes = Bytes::from_source(b"late collection child".to_vec());
        let child = Blob::<UnknownBlob>::new(child_bytes.clone());
        let child_handle = child.get_handle();
        let mut store = MemoryRepo::default();
        let collection = store.collection("full-page-route", policy.clone()).unwrap();
        let untouched = store.collection("full-page-untouched", policy).unwrap();
        store
            .commit(collection, &key, fragment(4, child_handle))
            .unwrap();
        store
            .commit(untouched, &key, fragment(5, Inline::new([0xA5; 32])))
            .unwrap();
        let (sender, receiver, wiring) = super::wire(id);
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(
            store,
            ReconcileQos {
                direction: crate::inventory::ReconcileDirection::Bidirectional,
                blobs: BlobReplication::Full,
            },
            sender,
            receiver,
        );
        peer.activate_collections([collection.handle(), untouched.handle()]);
        let before = observer.current_snapshot().unwrap();
        let untouched_before = full(&before, untouched);

        let (first_ack, mut first_acked) = tokio::sync::oneshot::channel();
        let mut first_page = NetEventBatch::default();
        first_page
            .try_push(NetEvent::FullPage {
                collection: collection.handle(),
                blobs: vec![(child_handle.raw, child_bytes)],
                final_page: false,
                ack: first_ack,
            })
            .unwrap();
        wiring.evt_tx.send(first_page).await.unwrap();
        peer.refresh();
        assert!(first_acked.try_recv().is_ok());
        assert!(Arc::ptr_eq(&before, &observer.current_snapshot().unwrap()));

        let (final_ack, mut final_acked) = tokio::sync::oneshot::channel();
        let mut final_page = NetEventBatch::default();
        final_page
            .try_push(NetEvent::FullPage {
                collection: collection.handle(),
                blobs: Vec::new(),
                final_page: true,
                ack: final_ack,
            })
            .unwrap();
        wiring.evt_tx.send(final_page).await.unwrap();
        peer.refresh();
        assert!(final_acked.try_recv().is_ok());
        let after = observer.current_snapshot().unwrap();
        assert!(!Arc::ptr_eq(&before, &after));
        assert!(
            Arc::ptr_eq(&untouched_before, &full(&after, untouched)),
            "a checkpointed arrival must preserve an unrelated Full forest"
        );
        assert!(
            full(&after, collection)
                .forest
                .iter_ordered()
                .any(|key| key[48..] == child_handle.raw)
        );
    }

    #[tokio::test]
    async fn published_blob_arrival_invalidates_only_its_reachable_full_forest() {
        let key = SigningKey::from_bytes(&[0x34; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
        let child_bytes = Bytes::from_source(b"cross-collection partial child".to_vec());
        let child = Blob::<UnknownBlob>::new(child_bytes.clone());
        let child_handle = child.get_handle();
        let mut store = MemoryRepo::default();
        let partial = store
            .collection("full-page-partial", policy.clone())
            .unwrap();
        let completed = store.collection("full-page-completed", policy).unwrap();
        store
            .commit(partial, &key, fragment(9, child_handle))
            .unwrap();
        store
            .commit(completed, &key, fragment(10, Inline::new([0xAA; 32])))
            .unwrap();
        let (sender, receiver, wiring) = super::wire(id);
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(
            store,
            ReconcileQos {
                direction: crate::inventory::ReconcileDirection::Bidirectional,
                blobs: BlobReplication::Full,
            },
            sender,
            receiver,
        );
        peer.activate_collections([partial.handle(), completed.handle()]);
        let before = observer.current_snapshot().unwrap();
        let partial_before = full(&before, partial);
        let completed_before = full(&before, completed);

        let (partial_ack, mut partial_acked) = tokio::sync::oneshot::channel();
        let (completed_ack, mut completed_acked) = tokio::sync::oneshot::channel();
        let mut interleaved = NetEventBatch::default();
        interleaved
            .try_push(NetEvent::FullPage {
                collection: partial.handle(),
                blobs: vec![(child_handle.raw, child_bytes)],
                final_page: false,
                ack: partial_ack,
            })
            .unwrap();
        interleaved
            .try_push(NetEvent::FullPage {
                collection: completed.handle(),
                blobs: Vec::new(),
                final_page: true,
                ack: completed_ack,
            })
            .unwrap();
        wiring.evt_tx.send(interleaved).await.unwrap();
        peer.refresh();
        assert!(partial_acked.try_recv().is_ok());
        assert!(completed_acked.try_recv().is_ok());
        let interleaved_snapshot = observer.current_snapshot().unwrap();
        assert!(!Arc::ptr_eq(
            &partial_before,
            &full(&interleaved_snapshot, partial)
        ));
        assert!(Arc::ptr_eq(
            &completed_before,
            &full(&interleaved_snapshot, completed)
        ));
        assert!(
            full(&interleaved_snapshot, partial)
                .forest
                .iter_ordered()
                .any(|key| key[48..] == child_handle.raw)
        );

        let (final_ack, mut final_acked) = tokio::sync::oneshot::channel();
        let mut final_page = NetEventBatch::default();
        final_page
            .try_push(NetEvent::FullPage {
                collection: partial.handle(),
                blobs: Vec::new(),
                final_page: true,
                ack: final_ack,
            })
            .unwrap();
        wiring.evt_tx.send(final_page).await.unwrap();
        peer.refresh();
        assert!(final_acked.try_recv().is_ok());
        let after = observer.current_snapshot().unwrap();
        assert!(!Arc::ptr_eq(&partial_before, &full(&after, partial)));
        assert!(
            full(&after, partial)
                .forest
                .iter_ordered()
                .any(|key| key[48..] == child_handle.raw)
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
