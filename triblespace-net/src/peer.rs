//! A synchronous store wrapped in collection-scoped anti-entropy.
//!
//! The host runtime repairs immutable per-collection semantic overlays. This
//! side owns the only mutable store boundary: authenticated leaves are deduplicated,
//! inserted monotonically, flushed once per drain, and only then exposed in a
//! replacement serving snapshot. Explicit live blob acquisition is separate
//! from both frozen snapshot reads and durable WANT delegation.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::{BlobEncoding, IntoBlob};
use triblespace_core::collection::{
    CollectionHandle, CollectionStore, next_authorization_change_at,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::patch::{Entry as PatchEntry, PATCH};
use triblespace_core::repo::async_store::AsyncBlobStoreAcquire;
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, BlobStorePut, CapabilityProofStore, SnapshotSource,
    StorageFlush, StoreChanges, StoreRead, StoreSnapshot as CoreStoreSnapshot, WantStore,
};

use crate::channel::{MAX_ADMISSION_BRIDGE_BATCHES, NetEvent};
use crate::host::{self, ActiveCollections, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::RawHash;
use crate::provider::ProviderObservation;
use crate::wake::CollectionWakePlane;

pub use crate::host::PeerConfig;
pub use crate::inventory::{ReconcileDirection, ReconcileQos};

/// Failure while starting a production network host.
#[derive(Debug)]
pub enum PeerOpenError {
    /// The production network thread, runtime, or iroh endpoint could not start.
    HostStartup(anyhow::Error),
}

/// Failure while actively acquiring and caching an exact blob handle.
#[derive(Debug)]
pub struct PeerAcquireError(String);

impl fmt::Display for PeerAcquireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PeerAcquireError {}

impl fmt::Display for PeerOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostStartup(error) => write!(f, "cannot start network host: {error}"),
        }
    }
}

impl Error for PeerOpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::HostStartup(error) => Some(error.as_ref()),
        }
    }
}

/// Failure while freezing the local observation behind a [`Peer`].
#[derive(Debug)]
pub enum PeerSnapshotError<SnapshotError> {
    /// The backing store could not freeze its coherent observation.
    Store(SnapshotError),
    /// An active collection could not be projected from the coherent store
    /// observation. The previous serving view is withdrawn.
    Overlay(anyhow::Error),
}

impl<SnapshotError> fmt::Display for PeerSnapshotError<SnapshotError>
where
    SnapshotError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "cannot freeze peer store snapshot: {error}"),
            Self::Overlay(error) => write!(formatter, "cannot build collection snapshot: {error}"),
        }
    }
}

impl<SnapshotError> Error for PeerSnapshotError<SnapshotError>
where
    SnapshotError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store(error) => Some(error),
            Self::Overlay(error) => Some(error.as_ref()),
        }
    }
}

/// A store attached to a collection-scoped network host.
pub struct Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    store: Arc<Mutex<S>>,
    sender: NetSender,
    receiver: NetReceiver,
    wake_plane: Option<CollectionWakePlane>,
    qos: ReconcileQos,
    active: ActiveCollections,
    active_dirty: bool,
    /// Network admissions stay outside the advertised snapshot until their
    /// shared durability barrier succeeds. A failed flush is retried on every
    /// refresh without requiring the remote to redeliver the event first.
    pending_network_flush: bool,
    /// Last local observation used to build the installed immutable inventory.
    /// Equality is a cheap invalidation check supplied by the store; it is not
    /// a portable generation or a semantic version.
    last_store_snapshot: Option<S::Snapshot>,
    last_authorization_change: Option<hifitime::Epoch>,
    last_observed_at: Option<hifitime::Epoch>,
    /// Last snapshot-bound provider set sent to the host. Rebuilding it
    /// on every refresh lets proof expiry narrow publication even when the
    /// store prefix itself did not change.
    last_provider_observation: ProviderObservation,
    last_event_at: crate::clock::Mono,
    #[cfg(test)]
    serving_snapshot_rebuilds: usize,
}

impl<S> Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    /// Spawn a production host. No team scope or connection proof exists.
    pub fn new(store: S, key: SigningKey, config: PeerConfig) -> Result<Self, PeerOpenError> {
        let qos = config.qos;
        let (sender, receiver, wake_plane) =
            host::spawn(key, config).map_err(PeerOpenError::HostStartup)?;
        Ok(Self::assemble(
            store,
            qos,
            sender,
            receiver,
            Some(wake_plane),
        ))
    }

    /// Attach a store to a caller-owned host, most commonly the deterministic
    /// simulator.
    pub fn with_wiring(
        store: S,
        qos: ReconcileQos,
        sender: NetSender,
        receiver: NetReceiver,
    ) -> Self {
        Self::assemble(store, qos, sender, receiver, None)
    }

    fn assemble(
        store: S,
        qos: ReconcileQos,
        sender: NetSender,
        receiver: NetReceiver,
        wake_plane: Option<CollectionWakePlane>,
    ) -> Self {
        let mut peer = Self {
            store: Arc::new(Mutex::new(store)),
            sender,
            receiver,
            wake_plane,
            qos,
            active: PATCH::new(),
            active_dirty: true,
            pending_network_flush: false,
            last_store_snapshot: None,
            last_authorization_change: None,
            last_observed_at: None,
            last_provider_observation: ProviderObservation::default(),
            last_event_at: crate::clock::mono_now(),
            #[cfg(test)]
            serving_snapshot_rebuilds: 0,
        };
        peer.refresh();
        peer
    }

    pub fn id(&self) -> EndpointId {
        self.sender.id()
    }

    /// Stock gossip wake plane for a production iroh peer.
    ///
    /// Caller-owned wiring has no implicit wake handle and returns `None`.
    /// Collection possession is enough to join a production topic; following a
    /// wake into anti-entropy remains separately authorized.
    pub fn wake_plane(&self) -> Option<CollectionWakePlane> {
        self.wake_plane.clone()
    }

    pub const fn qos(&self) -> ReconcileQos {
        self.qos
    }

    pub fn last_event_at(&self) -> crate::clock::Mono {
        self.last_event_at
    }

    /// Activate one collection for serving, repair, and wake subscription.
    ///
    /// This is ephemeral process state. It writes no OFFER/GOSSIP marker and
    /// creates no global collection registry.
    pub fn activate_collection(&mut self, collection: CollectionHandle) {
        self.activate_collections([collection]);
    }

    /// Activate several collections and publish one coherent serving snapshot.
    ///
    /// Activation is ephemeral process state, just like
    /// [`Self::activate_collection`]. Batching only collapses the refresh
    /// boundary: every supplied handle is visible together in the one snapshot
    /// published after the iterator has been consumed.
    pub fn activate_collections(
        &mut self,
        collections: impl IntoIterator<Item = CollectionHandle>,
    ) {
        for collection in collections {
            self.active_dirty |= self.active.get(&collection.raw).is_none();
            self.active.insert(&PatchEntry::new(&collection.raw));
        }
        self.refresh();
    }

    /// Discover and fetch the exact bytes named by bearer handle `H`.
    pub async fn fetch_wanted_blob(&self, hash: RawHash) -> Option<Bytes> {
        self.sender
            .fetch_blob(hash, host::INTERACTIVE_FETCH_DEADLINE)
            .await
    }

    pub async fn fetch_wanted_blob_with_deadline(
        &self,
        hash: RawHash,
        budget: std::time::Duration,
    ) -> Option<Bytes> {
        self.sender.fetch_blob(hash, budget).await
    }

    /// Drain authenticated collection progress, cross one durability barrier,
    /// then replace the immutable active-collection snapshot. Calling this
    /// with no events is still meaningful: file-backed stores reobserve
    /// external appends before periodic repair uses them.
    pub fn refresh(&mut self) {
        if let Err(error) = self.try_refresh() {
            tracing::warn!(%error, "collection serving snapshot unavailable");
        }
    }

    /// Drain pending network evidence and publish one coherent store snapshot.
    pub fn try_refresh(&mut self) -> Result<(), PeerSnapshotError<S::SnapshotError>> {
        let result = self.refresh_checked();
        if result.is_err() {
            self.sender.clear_snapshot();
            self.last_store_snapshot = None;
            self.last_authorization_change = None;
            self.last_observed_at = None;
            self.last_provider_observation = ProviderObservation::default();
        }
        result
    }

    fn refresh_checked(&mut self) -> Result<(), PeerSnapshotError<S::SnapshotError>> {
        let mut incoming = Vec::new();
        for _ in 0..MAX_ADMISSION_BRIDGE_BATCHES {
            let Some(event) = self.receiver.try_recv() else {
                break;
            };
            self.last_event_at = crate::clock::mono_now();
            incoming.push(event);
        }

        let received_batches = incoming.len();
        let received = incoming.iter().map(|batch| batch.len()).sum::<usize>();
        let mut store = self.store.lock().expect("store mutex");
        for batch in incoming {
            for event in batch.into_events() {
                match event {
                    NetEvent::Blob { expected, bytes } => {
                        match store.put::<UnknownBlob, _>(bytes) {
                            Ok(handle) => {
                                self.pending_network_flush = true;
                                if handle.raw != expected {
                                    return Err(PeerSnapshotError::Overlay(anyhow::anyhow!(
                                        "network blob hash changed while landing"
                                    )));
                                }
                            }
                            Err(error) => {
                                return Err(PeerSnapshotError::Overlay(anyhow::anyhow!(
                                    "landing network blob failed: {error:?}"
                                )));
                            }
                        }
                    }
                    NetEvent::CollectionRecord(record) => match store.insert(record) {
                        Ok(()) => self.pending_network_flush = true,
                        Err(error) => {
                            tracing::warn!(?error, "admitting collection repair record failed")
                        }
                    },
                    // Proof repair carries evidence, not blob demand. Referenced
                    // claims stay inert until an actual consumer follows them
                    // through the ordinary H-addressed blob data plane.
                    NetEvent::CapabilityProof(proof) => match store.insert_proof(proof) {
                        Ok(()) => self.pending_network_flush = true,
                        Err(error) => {
                            tracing::warn!(
                                ?error,
                                "admitting collection authorization proof failed"
                            );
                            return Err(PeerSnapshotError::Overlay(anyhow::anyhow!(
                                "admitting collection authorization proof failed: {error:?}"
                            )));
                        }
                    },
                }
            }
        }
        if self.pending_network_flush {
            match store.flush() {
                Ok(()) => {
                    self.pending_network_flush = false;
                    tracing::debug!(
                        received,
                        received_batches,
                        "collection repair admission durable"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        received,
                        received_batches,
                        "collection repair flush failed; snapshot withheld"
                    );
                }
            }
        }
        if !self.pending_network_flush {
            let snapshot = match store.snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "store snapshot unavailable; keeping prior collection view"
                    );
                    return Ok(());
                }
            };
            let previous_snapshot = self.sender.current_snapshot();
            let changes = if previous_snapshot.is_none() {
                StoreChanges::ALL
            } else {
                self.last_store_snapshot
                    .as_ref()
                    .map_or(StoreChanges::ALL, |previous| {
                        snapshot.changes_since(previous)
                    })
            };
            let now = crate::clock::epoch_now();
            let authorization_inputs_changed = changes.contains(StoreChanges::BLOBS)
                || changes.contains(StoreChanges::COLLECTION_RECORDS)
                || changes.contains(StoreChanges::CAPABILITY_PROOFS);
            let authorization_changed =
                self.last_observed_at.is_some_and(|observed| now < observed)
                    || self
                        .last_authorization_change
                        .is_some_and(|boundary| now >= boundary);
            if received == 0
                && changes == StoreChanges::NONE
                && !authorization_changed
                && !self.active_dirty
                && previous_snapshot.is_some()
            {
                self.last_store_snapshot = Some(snapshot);
                return Ok(());
            }
            let next_authorization_change =
                if !authorization_inputs_changed && !authorization_changed {
                    self.last_authorization_change
                } else {
                    next_authorization_change_at(&snapshot, now)
                        .map_err(anyhow::Error::new)
                        .map_err(PeerSnapshotError::Overlay)?
                };
            // Even a semantic no-op installs the fresh read lease. Unchanged
            // semantic repair PATCHes retain their Arc while exact-GET advances to
            // the new immutable store observation.
            let serving = StoreSnapshot::from_store_changes(
                snapshot.clone(),
                &self.active,
                VerifyingKey::from_bytes(self.sender.id().as_bytes())
                    .expect("endpoint id is an Ed25519 key"),
                self.last_store_snapshot.as_ref(),
                previous_snapshot.as_deref(),
                changes,
                authorization_changed,
                next_authorization_change,
                now,
            )
            .map_err(PeerSnapshotError::Overlay)?;
            let serves_collections = self.qos.direction.serves();
            let provider_observation = ProviderObservation::from_locators(
                serving
                    .collections()
                    .map(|collection| collection.collection()),
                serves_collections,
                serving.bearer_locators(),
            );
            self.sender.update_snapshot(serving, &self.active);
            #[cfg(test)]
            {
                self.serving_snapshot_rebuilds += 1;
            }
            self.active_dirty = false;
            self.last_store_snapshot = Some(snapshot);
            self.last_authorization_change = next_authorization_change;
            self.last_observed_at = Some(now);
            Self::observe_provider_observation(
                &self.sender,
                &mut self.last_provider_observation,
                provider_observation,
            );
        } else {
            // A failed admission flush withholds a new snapshot, but the
            // already-installed prefix remains a valid read lease. Recompute
            // only its time-sensitive authorization boundary.
            // Keep the last immutable serving/provider observation until the
            // failed admission batch is retried successfully.
        }
        Ok(())
    }

    fn observe_provider_observation(
        sender: &NetSender,
        last: &mut ProviderObservation,
        observation: ProviderObservation,
    ) {
        if *last != observation {
            sender.update_providers(observation.clone());
            *last = observation;
        }
    }

    pub fn store(&self) -> MutexGuard<'_, S> {
        self.store.lock().expect("store mutex")
    }

    pub fn into_store(self) -> S {
        let Self { store, .. } = self;
        Arc::try_unwrap(store)
            .unwrap_or_else(|_| panic!("Peer::into_store: store still has an outstanding owner"))
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn try_local(&mut self, hash: RawHash) -> Option<Bytes> {
        self.snapshot()
            .ok()?
            .get::<Bytes, UnknownBlob>(Inline::new(hash))
            .ok()
    }

    /// Read an exact handle locally or acquire it from the network.
    ///
    /// Acquired bytes are checked by the backing store's content-addressed
    /// `put`, then read from a fresh snapshot. This operation never records a
    /// WANT; durable delegation is an explicit [`WantStore::want`] operation.
    pub async fn acquire(
        &mut self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> Result<Option<Bytes>, PeerAcquireError> {
        let hash = handle.raw;
        if let Some(bytes) = self.try_local(hash) {
            return Ok(Some(bytes));
        }
        let Some(raw) = self.fetch_wanted_blob(hash).await else {
            return Ok(None);
        };
        {
            let mut store = self.store.lock().expect("store mutex");
            let stored = store.put::<UnknownBlob, Bytes>(raw).map_err(|error| {
                PeerAcquireError(format!("cannot cache acquired blob: {error}"))
            })?;
            if stored.raw != hash {
                return Err(PeerAcquireError(
                    "peer returned bytes for a different content hash".into(),
                ));
            }
        }
        let snapshot = self.snapshot().map_err(|error| {
            PeerAcquireError(format!("cannot refresh after acquisition: {error}"))
        })?;
        let bytes = snapshot
            .get::<Bytes, UnknownBlob>(Inline::new(hash))
            .map_err(|error| {
                PeerAcquireError(format!("cached blob absent from fresh snapshot: {error}"))
            })?;
        Ok(Some(bytes))
    }
}

impl<S> AsyncBlobStoreAcquire for Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    type AcquireError = PeerAcquireError;

    fn acquire(
        &mut self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, Self::AcquireError>> + Send {
        Peer::acquire(self, handle)
    }
}

impl<S> BlobStorePut for Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        let mut store = self.store.lock().expect("store mutex");
        store.put(item)
    }
}

impl<S> SnapshotSource for Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    type Snapshot = S::Snapshot;
    type SnapshotError = PeerSnapshotError<S::SnapshotError>;

    fn snapshot(&mut self) -> Result<Self::Snapshot, Self::SnapshotError> {
        self.try_refresh()?;
        let mut store = self.store.lock().expect("store mutex");
        store.snapshot().map_err(PeerSnapshotError::Store)
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use iroh_base::EndpointId;
    use triblespace_core::capability::{
        CapabilityAction, CapabilityAtom, CapabilityClaim, CapabilityMode, CapabilityProofBundle,
        CapabilityResource,
    };
    use triblespace_core::collection::{AdmissionPolicy, CollectionPolicy, CollectionStoreExt};
    use triblespace_core::repo::CapabilityProofRead;
    use triblespace_core::repo::memoryrepo::MemoryRepo;

    use crate::channel::NetEventBatch;

    use super::*;

    #[tokio::test]
    async fn active_acquire_miss_does_not_record_a_want() {
        let key = SigningKey::from_bytes(&[90; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, _wiring) = host::wire(id);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let missing = Inline::<Handle<UnknownBlob>>::new([0x5a; 32]);

        assert!(peer.acquire(missing).await.unwrap().is_none());
        assert!(peer.store().wants().unwrap().next().is_none());
    }

    #[test]
    fn idle_refresh_reuses_the_installed_serving_snapshot() {
        let key = SigningKey::from_bytes(&[91; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, _wiring) = host::wire(id);
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let before = observer.current_snapshot().unwrap();

        for _ in 0..100 {
            peer.refresh();
        }

        let after = observer.current_snapshot().unwrap();
        assert!(Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn bulk_activation_publishes_all_collections_in_one_rebuild() {
        let key = SigningKey::from_bytes(&[92; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let policy = CollectionPolicy::new(
            AdmissionPolicy::direct(key.verifying_key()),
            AdmissionPolicy::direct(key.verifying_key()),
        );
        let mut store = MemoryRepo::default();
        let collections = (0..4)
            .map(|index| {
                let name = format!("bulk-activation-{index}");
                store.collection(&name, policy.clone()).unwrap().handle()
            })
            .collect::<Vec<_>>();
        let (sender, receiver, _wiring) = host::wire(id);
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(store, ReconcileQos::default(), sender, receiver);
        let rebuilds_before = peer.serving_snapshot_rebuilds;

        peer.activate_collections(collections.iter().copied());

        assert_eq!(peer.serving_snapshot_rebuilds - rebuilds_before, 1);
        let snapshot = observer.current_snapshot().unwrap();
        let active = snapshot
            .collections()
            .map(|collection| collection.collection())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(active, collections.into_iter().collect());
    }

    #[tokio::test]
    async fn native_authorization_proof_does_not_assert_claim_blob_demand() {
        let key = SigningKey::from_bytes(&[94; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(id);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let proof = CapabilityProofBundle::issue_root(
            &key,
            CapabilityClaim::root(
                CapabilityAtom::new(
                    CapabilityAction::new(triblespace_core::collection::ACTION_READ),
                    CapabilityResource::new([95; 32]),
                ),
                CapabilityMode::Invoke,
                None,
            ),
            SigningKey::from_bytes(&[96; 32]).verifying_key(),
        )
        .unwrap()
        .proof()
        .clone();
        let mut batch = NetEventBatch::default();
        batch
            .try_push(NetEvent::CapabilityProof(proof.clone()))
            .unwrap();
        wiring.send_admission(batch).await;

        peer.try_refresh().unwrap();
        let mut store = peer.into_store();
        assert_eq!(store.wants().unwrap().count(), 0);
        let snapshot = store.snapshot().unwrap();
        assert_eq!(
            snapshot
                .proofs()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [proof]
        );
    }
}
