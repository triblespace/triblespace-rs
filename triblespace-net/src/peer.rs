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
use triblespace_core::collection::{CollectionHandle, CollectionStore, next_authorization_change};
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::patch::{Entry as PatchEntry, PATCH};
use triblespace_core::repo::async_store::AsyncBlobStoreAcquire;
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, CapabilityProofStore,
    SnapshotSource, StorageClose, StorageFlush, StoreChanges, StoreRead,
    StoreSnapshot as CoreStoreSnapshot, WantStore,
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

enum HostState {
    /// Allocating channels does not start any runtime or network activity.
    Dormant(Box<dyn FnOnce() -> Result<Option<CollectionWakePlane>, PeerOpenError> + Send>),
    Running,
    /// Startup is attempted once per peer. Local operations remain available.
    Failed(String),
}

/// A store with an eager or acquisition-triggered collection network host.
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
    host: HostState,
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
            HostState::Running,
        ))
    }

    /// Attach a live acquisition capability without starting a network host.
    ///
    /// Resident reads, snapshots, local writes, flush, and close stay entirely
    /// local and build no serving inventory. The first missing exact-handle
    /// acquisition, explicit fetch, or collection activation starts the host.
    /// Startup is attempted once; a failure is reported by later network
    /// operations without disabling resident store access. No operation here
    /// implicitly creates a durable WANT.
    pub fn lazy(store: S, key: SigningKey, config: PeerConfig) -> Self {
        let id = crate::identity::iroh_secret(&key).public().into();
        let (sender, receiver, wiring) = host::wire(id);
        let qos = config.qos;
        let startup = Box::new(move || {
            host::start(key, config, wiring)
                .map(Some)
                .map_err(PeerOpenError::HostStartup)
        });
        Self::assemble(
            store,
            qos,
            sender,
            receiver,
            None,
            HostState::Dormant(startup),
        )
    }

    /// Attach a store to a caller-owned host, most commonly the deterministic
    /// simulator.
    pub fn with_wiring(
        store: S,
        qos: ReconcileQos,
        sender: NetSender,
        receiver: NetReceiver,
    ) -> Self {
        Self::assemble(store, qos, sender, receiver, None, HostState::Running)
    }

    fn assemble(
        store: S,
        qos: ReconcileQos,
        sender: NetSender,
        receiver: NetReceiver,
        wake_plane: Option<CollectionWakePlane>,
        host: HostState,
    ) -> Self {
        let mut peer = Self {
            store: Arc::new(Mutex::new(store)),
            sender,
            receiver,
            host,
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
        if matches!(peer.host, HostState::Running) {
            peer.refresh();
        }
        peer
    }

    fn start_host(&mut self) -> Result<(), PeerOpenError> {
        match &self.host {
            HostState::Running => return Ok(()),
            HostState::Failed(error) => {
                return Err(PeerOpenError::HostStartup(anyhow::anyhow!(error.clone())));
            }
            HostState::Dormant(_) => {}
        }
        let HostState::Dormant(start) = std::mem::replace(&mut self.host, HostState::Running)
        else {
            unreachable!("only a dormant host reaches startup")
        };
        match start() {
            Ok(wake_plane) => {
                self.wake_plane = wake_plane;
                Ok(())
            }
            Err(error) => {
                let PeerOpenError::HostStartup(source) = &error;
                self.host = HostState::Failed(source.to_string());
                Err(error)
            }
        }
    }

    pub fn id(&self) -> EndpointId {
        self.sender.id()
    }

    /// Stock gossip wake plane for a production iroh peer.
    ///
    /// Caller-owned wiring and a dormant lazy peer have no implicit wake handle
    /// and return `None`. This accessor never starts a host.
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
    /// A lazy peer starts its host here. A startup failure is logged and leaves
    /// the collection inactive; local store operations remain available.
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
        if let Err(error) = self.start_host() {
            tracing::warn!(%error, "cannot activate collections on network host");
            return;
        }
        for collection in collections {
            self.active_dirty |= self.active.get(&collection.raw).is_none();
            self.active.insert(&PatchEntry::new(&collection.raw));
        }
        self.refresh();
    }

    /// Discover and fetch the exact bytes named by bearer handle `H`.
    /// This explicit network operation starts a dormant host.
    pub async fn fetch_blob(&mut self, hash: RawHash) -> Option<Bytes> {
        self.fetch_blob_with_deadline(hash, host::INTERACTIVE_FETCH_DEADLINE)
            .await
    }

    /// Discover and fetch the exact bytes named by bearer handle `H`, bounded
    /// by the caller's deadline.
    pub async fn fetch_blob_with_deadline(
        &mut self,
        hash: RawHash,
        budget: std::time::Duration,
    ) -> Option<Bytes> {
        if let Err(error) = self.start_host() {
            tracing::warn!(%error, "cannot start exact-blob fetch");
            return None;
        }
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
        self.try_refresh_at(crate::clock::epoch_now())
    }

    fn try_refresh_at(
        &mut self,
        instant: hifitime::Epoch,
    ) -> Result<(), PeerSnapshotError<S::SnapshotError>> {
        // A local observation must not start networking, build bearer indexes,
        // or enqueue serving/provider notices for a dormant host.
        if !matches!(self.host, HostState::Running) {
            return Ok(());
        }
        let result = self.refresh_checked(instant);
        if result.is_err() {
            self.sender.clear_snapshot();
            self.last_store_snapshot = None;
            self.last_authorization_change = None;
            self.last_observed_at = None;
            self.last_provider_observation = ProviderObservation::default();
        }
        result
    }

    fn refresh_checked(
        &mut self,
        instant: hifitime::Epoch,
    ) -> Result<(), PeerSnapshotError<S::SnapshotError>> {
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
                    // Proof repair carries complete inline evidence, not blob
                    // demand. No content closure is implied by admission.
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
            let snapshot = match store.snapshot_at(instant) {
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
            let now = snapshot.instant();
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
                    next_authorization_change(&snapshot)
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

    /// Borrow the local backend without starting the host.
    /// Drop this guard before calling another peer operation or awaiting I/O.
    pub fn store(&self) -> MutexGuard<'_, S> {
        self.store.lock().expect("store mutex")
    }

    /// Withdraw serving snapshots and release host ownership before returning
    /// the local backend. This does not itself flush or close the backend.
    pub fn into_store(mut self) -> S {
        self.sender.clear_snapshot();
        self.last_store_snapshot = None;
        let Self {
            store,
            sender,
            receiver,
            host,
            wake_plane,
            ..
        } = self;
        drop((sender, receiver, host, wake_plane));
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
        {
            let snapshot = self.snapshot().map_err(|error| {
                PeerAcquireError(format!("cannot observe resident blob: {error}"))
            })?;
            let resident = snapshot.contains_blob(handle).map_err(|error| {
                PeerAcquireError(format!("cannot check blob residency: {error}"))
            })?;
            if resident {
                return snapshot
                    .get::<Bytes, UnknownBlob>(handle)
                    .map(Some)
                    .map_err(|error| {
                        PeerAcquireError(format!("cannot read resident blob: {error}"))
                    });
            }
        }
        self.start_host()
            .map_err(|error| PeerAcquireError(error.to_string()))?;
        let Some(raw) = self.fetch_blob(hash).await else {
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

impl<S> CollectionStore for Peer<S>
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
    type InsertError = <S as CollectionStore>::InsertError;

    fn insert(
        &mut self,
        record: triblespace_core::collection::CollectionRecord,
    ) -> Result<(), Self::InsertError> {
        self.store.lock().expect("store mutex").insert(record)
    }
}

impl<S> CapabilityProofStore for Peer<S>
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
    type InsertError = <S as CapabilityProofStore>::InsertError;

    fn insert_proof(
        &mut self,
        proof: triblespace_core::capability::CapabilityProof,
    ) -> Result<(), Self::InsertError> {
        self.store.lock().expect("store mutex").insert_proof(proof)
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

    fn snapshot_at(
        &mut self,
        instant: hifitime::Epoch,
    ) -> Result<Self::Snapshot, Self::SnapshotError> {
        self.try_refresh_at(instant)?;
        let mut store = self.store.lock().expect("store mutex");
        store.snapshot_at(instant).map_err(PeerSnapshotError::Store)
    }
}

impl<S> StorageFlush for Peer<S>
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
    type Error = <S as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.store.lock().expect("store mutex").flush()
    }
}

impl<S> StorageClose for Peer<S>
where
    S: BlobStore
        + CollectionStore
        + CapabilityProofStore
        + WantStore
        + StorageFlush
        + StorageClose
        + Send
        + 'static,
    S::Snapshot: StoreRead + BlobChildren,
{
    type Error = <S as StorageClose>::Error;

    fn close(self) -> Result<(), Self::Error> {
        self.into_store().close()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ed25519_dalek::SigningKey;
    use iroh_base::EndpointId;
    use triblespace_core::capability::{
        Capability, CapabilityAction, CapabilityMode, CapabilityProof, CapabilityResource,
    };
    use triblespace_core::collection::{AdmissionPolicy, CollectionPolicy, CollectionStoreExt};
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::pile::Pile;
    use triblespace_core::repo::{CapabilityProofRead, Store, WantRead};

    use crate::channel::NetEventBatch;

    use super::*;

    fn foreground_config() -> PeerConfig {
        PeerConfig {
            peers: Vec::new(),
            qos: ReconcileQos {
                direction: ReconcileDirection::ReadOnly,
            },
            provider_publication_budget: Some(0),
        }
    }

    struct ExactBlob {
        hash: RawHash,
        bytes: Bytes,
        requests: Arc<AtomicUsize>,
    }

    impl host::NetCapability for ExactBlob {
        fn fetch_blob(&self, hash: RawHash) -> futures::future::BoxFuture<'static, Option<Bytes>> {
            self.requests.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(
                (hash == self.hash).then(|| self.bytes.clone()),
            ))
        }
    }

    #[test]
    fn peer_is_a_live_collection_store() {
        fn assert_live_collection_store<S>()
        where
            S: Store + AsyncBlobStoreAcquire,
        {
        }

        assert_live_collection_store::<Peer<MemoryRepo>>();
    }

    #[test]
    fn lazy_resident_reads_writes_and_snapshots_stay_dormant_without_a_runtime() {
        let key = SigningKey::from_bytes(&[81; 32]);
        let bytes = Bytes::from_source(vec![1_u8, 2, 3]);
        let mut store = MemoryRepo::default();
        let handle = store.put::<UnknownBlob, _>(bytes.clone()).unwrap();
        let mut peer = Peer::lazy(store, key, foreground_config());
        let before = peer.snapshot().unwrap();

        assert_eq!(
            futures::executor::block_on(peer.acquire(handle)).unwrap(),
            Some(bytes)
        );
        let later = peer
            .put::<UnknownBlob, _>(Bytes::from_source(vec![4_u8, 5, 6]))
            .unwrap();
        peer.flush().unwrap();
        peer.refresh();
        let after = peer.snapshot().unwrap();

        assert!(!before.contains_blob(later).unwrap());
        assert!(after.contains_blob(later).unwrap());
        assert_eq!(after.wants().unwrap().count(), 0);
        assert!(matches!(peer.host, HostState::Dormant(_)));
        assert!(peer.wake_plane().is_none());
        assert!(peer.sender.current_snapshot().is_none());
        assert!(peer.last_store_snapshot.is_none());
        assert_eq!(peer.serving_snapshot_rebuilds, 0);
        peer.close().unwrap();
    }

    #[tokio::test]
    async fn lazy_misses_start_once_and_acquire_without_changing_frozen_snapshots_or_wants() {
        let key = SigningKey::from_bytes(&[82; 32]);
        let bytes = Bytes::from_source(vec![7_u8, 8, 9]);
        let mut source = MemoryRepo::default();
        let handle = source.put::<UnknownBlob, _>(bytes.clone()).unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (sender, receiver, wiring) =
            host::wire(crate::identity::iroh_secret(&key).public().into());
        let start_count = starts.clone();
        let capability = Arc::new(ExactBlob {
            hash: handle.raw,
            bytes: bytes.clone(),
            requests: requests.clone(),
        });
        let startup = Box::new(move || {
            start_count.fetch_add(1, Ordering::SeqCst);
            wiring.install_test_capability(capability);
            Ok(None)
        });
        let mut peer = Peer::assemble(
            MemoryRepo::default(),
            foreground_config().qos,
            sender,
            receiver,
            None,
            HostState::Dormant(startup),
        );
        let before = peer.snapshot().unwrap();

        assert_eq!(peer.acquire(handle).await.unwrap(), Some(bytes.clone()));
        assert_eq!(peer.acquire(handle).await.unwrap(), Some(bytes));
        let missing = Inline::<Handle<UnknownBlob>>::new([0x53; 32]);
        assert!(peer.acquire(missing).await.unwrap().is_none());

        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert!(!before.contains_blob(handle).unwrap());
        assert!(before.get::<Bytes, UnknownBlob>(handle).is_err());
        let after = peer.snapshot().unwrap();
        assert!(after.contains_blob(handle).unwrap());
        assert_eq!(after.wants().unwrap().count(), 0);
        assert!(matches!(peer.host, HostState::Running));
        peer.close().unwrap();
    }

    #[test]
    fn lazy_startup_failure_is_reported_once_and_keeps_local_store_usable() {
        let key = SigningKey::from_bytes(&[83; 32]);
        let (sender, receiver, wiring) =
            host::wire(crate::identity::iroh_secret(&key).public().into());
        let starts = Arc::new(AtomicUsize::new(0));
        let start_count = starts.clone();
        let startup = Box::new(move || {
            let _wiring = wiring;
            start_count.fetch_add(1, Ordering::SeqCst);
            Err(PeerOpenError::HostStartup(anyhow::anyhow!(
                "test startup failure"
            )))
        });
        let mut peer = Peer::assemble(
            MemoryRepo::default(),
            foreground_config().qos,
            sender,
            receiver,
            None,
            HostState::Dormant(startup),
        );
        let missing = Inline::<Handle<UnknownBlob>>::new([0x54; 32]);
        for _ in 0..2 {
            let error = futures::executor::block_on(peer.acquire(missing)).unwrap_err();
            assert_eq!(
                error.to_string(),
                "cannot start network host: test startup failure"
            );
        }
        let bytes = Bytes::from_source(vec![10_u8, 11, 12]);
        let handle = peer.put::<UnknownBlob, _>(bytes.clone()).unwrap();
        assert_eq!(
            futures::executor::block_on(peer.acquire(handle)).unwrap(),
            Some(bytes)
        );
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(peer.snapshot().unwrap().wants().unwrap().count(), 0);
        assert!(peer.sender.current_snapshot().is_none());
        peer.close().unwrap();
    }

    #[test]
    fn lazy_snapshot_failure_is_not_a_network_miss() {
        let path = tempfile::NamedTempFile::new().unwrap();
        let mut pile = Pile::open(path.path()).unwrap();
        let handle = pile
            .put::<UnknownBlob, _>(Bytes::from_source(vec![16_u8, 17, 18]))
            .unwrap();
        pile.close().unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path.path())
            .unwrap();
        file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        let mut peer = Peer::lazy(
            Pile::open(path.path()).unwrap(),
            SigningKey::from_bytes(&[86; 32]),
            foreground_config(),
        );

        let error = futures::executor::block_on(peer.acquire(handle)).unwrap_err();

        assert!(error.to_string().contains("cannot observe resident blob"));
        assert!(matches!(peer.host, HostState::Dormant(_)));
        assert!(peer.sender.current_snapshot().is_none());
        assert_eq!(peer.serving_snapshot_rebuilds, 0);
        peer.close().unwrap();
    }

    #[test]
    fn lazy_activation_starts_once_and_publishes_the_requested_collection() {
        let key = SigningKey::from_bytes(&[84; 32]);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "lazy-activation",
                CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open),
            )
            .unwrap()
            .handle();
        let (sender, receiver, wiring) =
            host::wire(crate::identity::iroh_secret(&key).public().into());
        let starts = Arc::new(AtomicUsize::new(0));
        let start_count = starts.clone();
        let startup = Box::new(move || {
            let _wiring = wiring;
            start_count.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        });
        let mut peer = Peer::assemble(
            store,
            foreground_config().qos,
            sender,
            receiver,
            None,
            HostState::Dormant(startup),
        );

        peer.activate_collection(collection);
        peer.activate_collection(collection);

        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(peer.serving_snapshot_rebuilds, 1);
        assert_eq!(
            peer.sender
                .current_snapshot()
                .unwrap()
                .collections()
                .count(),
            1
        );
        peer.close().unwrap();
    }

    #[test]
    fn close_withdraws_host_snapshot_and_persists_local_pile_writes() {
        let path = tempfile::NamedTempFile::new().unwrap();
        let key = SigningKey::from_bytes(&[85; 32]);
        let (sender, receiver, _wiring) =
            host::wire(crate::identity::iroh_secret(&key).public().into());
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(
            Pile::open(path.path()).unwrap(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let bytes = Bytes::from_source(vec![13_u8, 14, 15]);
        let handle = peer.put::<UnknownBlob, _>(bytes.clone()).unwrap();
        let frozen = peer.snapshot().unwrap();
        assert!(observer.current_snapshot().is_some());

        peer.close().unwrap();

        assert!(observer.current_snapshot().is_none());
        assert_eq!(frozen.get::<Bytes, UnknownBlob>(handle).unwrap(), bytes);
        let mut reopened = Pile::open(path.path()).unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .unwrap()
                .get::<Bytes, UnknownBlob>(handle)
                .unwrap(),
            bytes
        );
        reopened.close().unwrap();
    }

    #[test]
    fn snapshot_at_preserves_the_chosen_instant_through_peer_refresh() {
        let key = SigningKey::from_bytes(&[89; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, _wiring) = host::wire(id);
        let observer = sender.clone();
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let instant = hifitime::Epoch::from_tai_seconds(15.0);

        let snapshot = peer.snapshot_at(instant).unwrap();

        assert_eq!(snapshot.instant(), instant);
        assert_eq!(
            peer.last_store_snapshot.as_ref().unwrap().instant(),
            instant
        );
        assert_eq!(snapshot.clone().instant(), instant);

        let serving = observer.current_snapshot().unwrap();
        let later_instant = hifitime::Epoch::from_tai_seconds(16.0);
        let later = peer.snapshot_at(later_instant).unwrap();
        assert_eq!(later.instant(), later_instant);
        assert_eq!(later.changes_since(&snapshot), StoreChanges::NONE);
        assert!(Arc::ptr_eq(&serving, &observer.current_snapshot().unwrap()));
    }

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
        assert!(peer.snapshot().unwrap().wants().unwrap().next().is_none());
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
    async fn native_authorization_proof_does_not_assert_blob_demand() {
        let key = SigningKey::from_bytes(&[94; 32]);
        let id = EndpointId::from_bytes(&key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(id);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            ReconcileQos::default(),
            sender,
            receiver,
        );
        let proof = CapabilityProof::issue_root(
            &key,
            CapabilityResource::new([95; 32]),
            Capability::new(
                CapabilityAction::new(triblespace_core::collection::ACTION_READ),
                CapabilityMode::Invoke,
            ),
            None,
            SigningKey::from_bytes(&[96; 32]).verifying_key(),
        );
        let mut batch = NetEventBatch::default();
        batch
            .try_push(NetEvent::CapabilityProof(proof.clone()))
            .unwrap();
        wiring.send_admission(batch).await;

        peer.try_refresh().unwrap();
        let mut store = peer.into_store();
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.wants().unwrap().count(), 0);
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
