//! `Peer<S>`: a store wrapped in distributed network sync.
//!
//! Owns the inner store, spawns the iroh network thread on construction,
//! and exposes the standard storage traits (`BlobStore + BlobStorePut +
//! PinStore`) with two layers of legacy transport behavior built in:
//!
//! - **Reads** auto-call [`refresh`](Peer::refresh), which drains pending
//!   incoming blob and scalar-HEAD observations into the wrapped store and
//!   re-publishes eligible mutable-pin deltas from external writers (e.g.
//!   another process appended to the same pile file). Mirrors
//!   `Pile::refresh` — the explicit method is available for tight loops.
//!   Persistence failures are sticky and fail-stop: automatic trait refreshes
//!   cannot change their associated error types, but a later explicit
//!   [`refresh`](Peer::refresh) reports the retained [`PeerRefreshError`].
//! - **Writes** delegate to the inner store. Blobs are announced to the DHT;
//!   eligible mutable pins are announced through the legacy HEAD topic.
//!   Signed branch assertions are forwarded only to local storage and are not
//!   replicated or synthesized by this layer.
//!
//! There is no separate cache tier: `Peer<S>` takes a **single store**,
//! and any tiering (bounded weak retention, generational eviction) lives
//! in `S` — e.g. a [`Yard`](triblespace_core::repo::yard::Yard). Read-miss
//! swarm fetches land in `S` under a **weak pin** ([`WeakPinStore`]),
//! following the retention lattice `pin ⊐ weak-pin ⊐ weak-unpin ⊐ unpin`:
//! the weak pin is recorded durably *before* the fetch — pinned AND
//! flushed ([`StorageFlush`]), so the marker survives an immediate
//! process exit — the demand IS the want-signal (a sync daemon's work
//! queue), then the retention marker for the fetched blob, then the
//! eviction target. A failed fetch leaves the weak pin in place: it
//! remains an outstanding want. The want-on-record invariant holds
//! unconditionally: if the pin or its flush FAILS, the read errors out
//! ([`PeerReaderGetError::WantRecord`] /
//! [`Peer::get_or_fetch_async`]'s `Err`) instead of proceeding — the
//! caller never observes a fetch whose demand isn't durably recorded.
//! "Promote to durable" is not an operation — durability is
//! reachability from strong pins; the Peer performs no promotion.
//!
//! Legacy mutable-HEAD discovery is gossip-driven: observations flood the team
//! topic and arrive as local tracking pins while the network thread follows a
//! bounded, untrusted child-hint walk from each advertised metadata blob. These
//! observations are transport state, not StrongPin authority. There is
//! currently no signed-assertion wire protocol.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{BlobEncoding, IntoBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::branch_assertion::{
    BranchAssertion, BranchAssertionSnapshot, BranchAssertionStore,
};
use triblespace_core::repo::branch_frontier::{ParentLookup, PartialCommitDag};
use triblespace_core::repo::lazy::WantRecordError;
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult,
    StorageFlush, WeakPinStore,
};

use crate::channel::{NetEvent, PublisherKey};
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::RawHash;

pub use crate::host::{PeerConfig, SyncDirection};

/// A fail-stop persistence error observed while applying network events.
///
/// Once a peer records this error, every later [`Peer::refresh`] returns the
/// same value without consuming more events. In particular, a failed fetched
/// blob write can never be followed by materializing the associated legacy
/// HEAD. Callers may close or repair the wrapped store and restart the peer;
/// continuing after an unknown partial write would make the storage invariant
/// unprovable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{operation}: {detail}")]
pub struct PeerRefreshError {
    operation: &'static str,
    detail: String,
}

impl PeerRefreshError {
    fn new(operation: &'static str, error: impl std::fmt::Display) -> Self {
        Self {
            operation,
            detail: error.to_string(),
        }
    }

    /// Storage operation that first faulted this peer.
    pub fn operation(&self) -> &'static str {
        self.operation
    }

    /// Original storage error rendered at the boundary where it occurred.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// A store wrapped in distributed network sync.
///
/// See the [module-level docs](self) for the full mental model.
///
/// # Example
///
/// Single-user team-of-one setup against a [`Pile`]: the user is
/// their own team root, and the relay accepts only caps signed by
/// (or chained from) their own key. The `self_cap = [0u8; 32]`
/// sentinel will fail any remote `OP_AUTH` it sends — fine for
/// solo workflows where the peer is purely a server.
///
/// Multi-user setups load `team_root` and `self_cap` from the
/// `TRIBLE_TEAM_ROOT` and `TRIBLE_TEAM_CAP` environment variables;
/// see the [Capability Auth] book chapter for the full team
/// lifecycle.
///
/// [`Pile`]: triblespace_core::repo::pile::Pile
/// [Capability Auth]: https://docs.rs/triblespace/latest/triblespace/book/capability-auth/index.html
///
/// ```rust,no_run
/// use std::path::Path;
/// use ed25519_dalek::SigningKey;
/// use rand::rngs::OsRng;
/// use triblespace_core::repo::pile::Pile;
/// use triblespace_net::peer::{Peer, PeerConfig, SyncDirection};
///
/// let key = SigningKey::generate(&mut OsRng);
/// let pile: Pile = Pile::open(Path::new("./team.pile")).unwrap();
/// let peer = Peer::new(pile, key.clone(), PeerConfig {
///     peers: vec![],                       // bootstrap nodes
///     gossip: true,                        // false = serve/pull-only
///     team_root: key.verifying_key(),      // single-user fallback
///     self_cap: [0u8; 32],
///     direction: SyncDirection::Bidirectional,
/// });
/// // From here `peer` forwards the wrapped store's blob, local-pin,
/// // durability, and branch-assertion capabilities — wrap it in
/// // `Repository::new` and use it like any other storage.
/// drop(peer);
/// ```
pub struct Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    /// The wrapped store, shared behind a mutex: a `&self` async read on
    /// a [`PeerReader`] must be able to record a weak pin and land a
    /// swarm-fetched blob back into it (the one piece of Peer state the
    /// read snapshot must be able to mutate). All of Peer's own methods
    /// take the same lock.
    store: Arc<Mutex<S>>,

    sender: NetSender,
    receiver: NetReceiver,

    /// Baseline blob snapshot for diff-and-publish on `refresh`. The Reader
    /// is a frozen view (for backends with snapshot semantics like Pile) so
    /// `current.blobs_diff(&last)` returns exactly the blobs added since
    /// the last refresh.
    last_blob_reader: Option<S::Reader>,

    /// Baseline legacy mutable heads for diff-and-publish on `refresh`.
    /// Updated on every Peer-driven pin write so we do not double-gossip it.
    last_legacy_metadata_heads: HashMap<Id, RawHash>,

    /// Direction of swarm participation — controls whether we publish
    /// local HEADs and/or react to remote HEADs.
    direction: SyncDirection,

    /// Monotonic time of the most recent NetEvent absorbed in
    /// [`refresh`](Peer::refresh). Drives quiescence-based stopping
    /// in long-running sync drivers. Read through [`crate::clock`] so
    /// simulated runs measure quiescence in virtual time.
    last_event_at: crate::clock::Mono,

    /// First persistence failure while absorbing network events. Network
    /// ingestion is fail-stop: after this is set, no later event is consumed.
    refresh_error: Option<PeerRefreshError>,

    /// Team root pubkey, copied from `PeerConfig::team_root` so the
    /// refresh loop can verify incoming `CapDelivered` events against
    /// it without round-tripping through the network thread.
    team_root: ed25519_dalek::VerifyingKey,

    /// Cloned signing key. ed25519's SigningKey is 32 bytes of secret
    /// scalar so cloning is cheap, but we keep it as an explicit
    /// `Clone` instead of `Copy` so the surface area for accidental
    /// duplication stays auditable. Used by `renewal_tick` to sign
    /// fresh caps for entries on the renewal-policy pin.
    signing_key: SigningKey,

    /// Per-entry cooldown for undelivered-cap re-dispatch. The
    /// renewal daemon's tick runs every 100 ms; without this gate it
    /// would hammer iroh-connect attempts for any peer that's down.
    /// Recorded against `entry.id`. Cleared (entry-level) when the
    /// delivery confirms; the whole map is in-memory and rebuilds
    /// naturally if the daemon restarts.
    last_dispatch_attempt: HashMap<Id, crate::clock::Mono>,
}

impl<S> Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread
    /// internally; the thread lives for the Peer's lifetime and shuts
    /// down when the Peer drops.
    pub fn new(store: S, key: SigningKey, config: PeerConfig) -> Self {
        let direction = config.direction;
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);
        Self::assemble(store, sender, receiver, direction, team_root, signing_key)
    }

    /// Wrap a store in a Peer over caller-provided channel halves — the
    /// host loop runs wherever the caller put it (deterministic
    /// simulation: a local task on a shared paused runtime) instead of
    /// on an internally-spawned thread.
    ///
    /// Pair with [`crate::host::wire`] + [`crate::host::run_host`].
    pub fn with_wiring(
        store: S,
        signing_key: SigningKey,
        direction: SyncDirection,
        team_root: ed25519_dalek::VerifyingKey,
        sender: host::NetSender,
        receiver: host::NetReceiver,
    ) -> Self {
        Self::assemble(store, sender, receiver, direction, team_root, signing_key)
    }

    fn assemble(
        mut store: S,
        sender: host::NetSender,
        receiver: host::NetReceiver,
        direction: SyncDirection,
        team_root: ed25519_dalek::VerifyingKey,
        signing_key: SigningKey,
    ) -> Self {
        // Seed the snapshot served by the network thread so peers
        // requesting via the protocol see our current state immediately.
        if let Some(snap) = StoreSnapshot::from_store(&mut store) {
            sender.update_snapshot(snap);
        }

        // Baseline starts as None. The first `refresh` will diff the
        // store against this and announce every existing blob to the
        // DHT — same outcome as a dedicated startup sweep, but with no
        // race between sweep and baseline capture (a previous design
        // ran both as separate `reader()` calls; an external append
        // landing between them would slip into the baseline without
        // ever being announced).
        let mut peer = Peer {
            store: Arc::new(Mutex::new(store)),
            sender,
            receiver,
            last_blob_reader: None,
            last_legacy_metadata_heads: HashMap::new(),
            direction,
            last_event_at: crate::clock::mono_now(),
            refresh_error: None,
            team_root,
            signing_key,
            last_dispatch_attempt: HashMap::new(),
        };

        // Drive the first refresh synchronously so the DHT learns
        // about pre-existing blobs before construction returns and the
        // first incoming AUTH can land.
        let _ = peer.refresh();

        peer
    }

    /// Monotonic time of the most recent network event absorbed by
    /// [`refresh`](Self::refresh). Useful for quiescence-based stopping:
    /// long-running sync drivers can poll `peer.last_event_at().elapsed()`
    /// and shut down once the swarm goes silent.
    ///
    /// Constructed-at-`Peer::new` initial value, so the first quiescence
    /// window starts at construction rather than at the first event.
    /// Returned as a [`crate::clock::Mono`] — virtual-time-aware under
    /// simulation, `.elapsed()`-compatible either way.
    pub fn last_event_at(&self) -> crate::clock::Mono {
        self.last_event_at
    }

    /// Direction of swarm participation. See [`SyncDirection`].
    pub fn direction(&self) -> SyncDirection {
        self.direction
    }

    /// This peer's network identity (the iroh node id).
    pub fn id(&self) -> EndpointId {
        self.sender.id()
    }

    /// Swarm-addressed on-demand blob fetch — the lazy-replication
    /// read-miss primitive, run **inline** (no command round-trip).
    /// Awaits the verified bytes or `None` (Unavailable); a host that
    /// never came up also resolves to `None`, never a hang. Bounded
    /// end-to-end by [`host::INTERACTIVE_FETCH_DEADLINE`] (the
    /// per-stage dial/op deadlines alone could stack to 40s+ across a
    /// provider list); use
    /// [`fetch_blob_with_deadline`](Self::fetch_blob_with_deadline) to
    /// pass a different budget. Does NOT persist the result and records
    /// no want — that is the caller's policy choice (see
    /// [`get_or_fetch_async`](Self::get_or_fetch_async) for the
    /// weak-pin-then-fetch-then-put composition). Used in
    /// deterministic-sim drivers, polled while stepping the sim.
    pub async fn fetch_blob(&self, hash: RawHash) -> Option<Vec<u8>> {
        self.sender
            .fetch_blob(hash, host::INTERACTIVE_FETCH_DEADLINE)
            .await
    }

    /// [`fetch_blob`](Self::fetch_blob) with an explicit end-to-end
    /// budget. Interactive reads keep the tight default; background
    /// work (the want-reconciler's tick) passes a more generous one.
    /// Expiry resolves to `None` — same Unavailable semantics, and any
    /// recorded want stays recorded.
    pub async fn fetch_blob_with_deadline(
        &self,
        hash: RawHash,
        budget: std::time::Duration,
    ) -> Option<Vec<u8>> {
        self.sender.fetch_blob(hash, budget).await
    }

    /// Reconcile this peer with the latest external state.
    ///
    /// Two phases:
    ///
    /// 1. **Drain incoming events** — pulls any pending gossip
    ///    `NetEvent`s from the network thread into the wrapped store
    ///    (creating tracking pins as needed).
    /// 2. **Publish external writes** — diffs the wrapped store against
    ///    the last published baseline and gossips/announces any deltas
    ///    that didn't go through the Peer's own write path. Use this to
    ///    catch writes from another process that touched the pile file.
    ///
    /// Auto-called inside the BlobStore/PinStore read methods, so
    /// callers using the storage normally don't need to invoke it.
    /// Mirrors `Pile::refresh` — the explicit method is available for
    /// "do it now" semantics or tight loops with no read activity.
    ///
    /// Network ingestion is fail-stop. If persisting an incoming fetched blob
    /// fails, this returns an error before consuming a later HEAD event and
    /// remembers that error permanently for this `Peer`. This deliberately
    /// favors an explicit restart/repair over advancing tracking state across
    /// a possibly partial append.
    pub fn refresh(&mut self) -> Result<(), PeerRefreshError> {
        if let Some(error) = &self.refresh_error {
            return Err(error.clone());
        }
        match self.refresh_once() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.refresh_error = Some(error.clone());
                Err(error)
            }
        }
    }

    fn refresh_once(&mut self) -> Result<(), PeerRefreshError> {
        // ── Phase 1: drain incoming events ────────────────────────────
        // WriteOnly suppresses incoming blob/legacy-HEAD materialization, but
        // capability request, delivery, and confirmation events are control
        // traffic and must still reach local policy state.
        while let Some(event) = self.receiver.try_recv() {
            self.last_event_at = crate::clock::mono_now();
            match event {
                NetEvent::Blob(data) => {
                    if self.direction == SyncDirection::WriteOnly {
                        continue;
                    }
                    // `data` is already an anybytes::Bytes (refcounted) —
                    // pass it into the store without re-wrapping.
                    self.store
                        .lock()
                        .expect("store mutex")
                        .put::<UnknownBlob, Bytes>(data)
                        .map_err(|error| {
                            PeerRefreshError::new("persist incoming fetched blob", error)
                        })?;
                }
                NetEvent::LegacyHead {
                    pin,
                    metadata_head,
                    publisher,
                } => {
                    if self.direction == SyncDirection::WriteOnly {
                        continue;
                    }
                    if let Some(remote_id) = Id::new(pin) {
                        let mut store = self.store.lock().expect("store mutex");
                        match read_legacy_pin_name(&mut *store, &metadata_head, remote_id) {
                            Some(name) => {
                                let r = crate::tracking::ensure_tracking_pin(
                                    &mut *store,
                                    remote_id,
                                    &metadata_head,
                                    &name,
                                    &publisher,
                                    // Gossip-driven auto-tracking stays strong
                                    // (eager) for now; the weak/lazy path is
                                    // opt-in and wired separately.
                                    false,
                                );
                                tracing::trace!(
                                    metadata_head = %hex::encode(&metadata_head[..4]),
                                    ok = r.is_some(),
                                    "head event -> ensure_tracking_pin"
                                );
                            }
                            None => {
                                tracing::warn!(
                                    metadata_head = %hex::encode(&metadata_head[..4]),
                                    "peer: legacy HEAD event but pin metadata unreadable; dropped"
                                );
                            }
                        }
                    }
                }
                NetEvent::CapRequest {
                    requester,
                    partial_cap_bytes,
                } => {
                    self.absorb_cap_request(requester, partial_cap_bytes);
                }
                NetEvent::CapDelivered {
                    issuer,
                    cap_bytes,
                    sig_bytes,
                } => {
                    // Verify the delivered chain against our configured
                    // team root, then store both blobs locally. Pinning
                    // them into a per-team-cap pin (so compaction
                    // retains them) comes with the CLI subcommands —
                    // for now they're orphan blobs in the pile, same
                    // as our own outgoing-cap blobs.
                    self.absorb_cap_delivery(issuer, cap_bytes, sig_bytes);
                }
                NetEvent::CapDeliveryConfirmed {
                    subject,
                    sig_handle,
                } => {
                    // The subject's daemon authenticated against us with
                    // a cap we dispatched. `sig_handle` is the signature
                    // blob handle (what OP_AUTH wires) — match by
                    // subject + latest_sig and mark the entry delivered
                    // so the daemon's next tick skips it from the
                    // re-dispatch set.
                    use triblespace_core::inline::Inline;
                    use triblespace_core::inline::encodings::hash::Handle;
                    let subject_key = match ed25519_dalek::VerifyingKey::from_bytes(&subject) {
                        Ok(k) => k,
                        Err(_) => continue,
                    };
                    let sig_inline: Inline<Handle<SimpleArchive>> = Inline::new(sig_handle);
                    let mut store = self.store.lock().expect("store mutex");
                    if let Some(entry_id) = crate::policy::find_policy_entry_by_subject_and_sig(
                        &mut *store,
                        subject_key,
                        sig_inline,
                    ) {
                        let _ = crate::policy::mark_policy_delivered(&mut *store, entry_id);
                        tracing::debug!(
                            subject = %hex::encode(&subject[..4]),
                            sig = %hex::encode(&sig_handle[..4]),
                            entry = ?entry_id,
                            "delivery confirmed; policy entry marked delivered"
                        );
                    }
                }
            }
        }

        let mut store = self.store.lock().expect("store mutex");

        // ── Phase 2: refresh the snapshot served by the network thread ─
        //
        // MUST happen before any announce/gossip below: peers who hear
        // our announce/gossip will dial us to fetch the hinted subgraph, and
        // the network thread serves them out of this snapshot. If we
        // gossiped first, a fast-dialing peer would hit `has_blob =
        // false` on the still-stale snapshot and the server would deny
        // OP_CHILDREN/OP_GET_BLOB as "out of scope" — even though we
        // just told them we have it.
        if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
            self.sender.update_snapshot(snap);
        }

        // ── Phase 3: diff-and-publish blob deltas ─────────────────────
        // ReadOnly skips the publish: we still update the baseline
        // reader so we don't accumulate a publish backlog if the
        // direction later changes. On the first refresh the baseline
        // is `None`, so we announce every blob currently in the store —
        // covers the initial pile contents without a separate startup
        // sweep (and without the race that two separate `reader()`
        // calls introduced).
        let current = store
            .reader()
            .map_err(|error| PeerRefreshError::new("snapshot local blobs", error))?;
        if self.direction != SyncDirection::ReadOnly {
            match self.last_blob_reader.as_ref() {
                Some(baseline) => {
                    for handle in current.blobs_diff(baseline) {
                        let handle = handle
                            .map_err(|error| PeerRefreshError::new("diff local blobs", error))?;
                        self.sender.announce(handle.raw);
                    }
                }
                None => {
                    use triblespace_core::repo::BlobStoreList;
                    for handle in current.blobs() {
                        let handle = handle.map_err(|error| {
                            PeerRefreshError::new("enumerate local blobs", error)
                        })?;
                        self.sender.announce(handle.raw);
                    }
                }
            }
        }
        self.last_blob_reader = Some(current);

        // ── Phase 4: diff-and-publish legacy mutable-head deltas ──────
        // ReadOnly skips this entire phase — followers don't gossip.
        if self.direction != SyncDirection::ReadOnly {
            let pin_ids = store
                .pins()
                .map_err(|error| PeerRefreshError::new("enumerate local pins", error))?;
            let pin_ids: Vec<Id> = pin_ids
                .map(|result| {
                    result
                        .map_err(|error| PeerRefreshError::new("read local pin index entry", error))
                })
                .collect::<Result<_, _>>()?;
            for pin_id in pin_ids {
                let Some(metadata_head) = legacy_pin_metadata_head(&mut *store, pin_id) else {
                    continue;
                };
                if self.last_legacy_metadata_heads.get(&pin_id) != Some(&metadata_head.raw) {
                    let pin_bytes: [u8; 16] = pin_id.into();
                    self.sender.gossip_legacy_head(pin_bytes, metadata_head.raw);
                    self.last_legacy_metadata_heads
                        .insert(pin_id, metadata_head.raw);
                }
            }
        }
        Ok(())
    }

    /// Persist an incoming join request: store the partial-cap blob,
    /// then add a pending-request entity to the local pending-requests
    /// branch. The entity id becomes the value `team approve <id>`
    /// consumes; the partial-cap blob is recoverable from the entity's
    /// `request_partial_cap` handle.
    fn absorb_cap_request(&mut self, requester: PublisherKey, partial_cap_bytes: anybytes::Bytes) {
        use triblespace_core::blob::Blob;
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::inline::TryToInline;

        // Reconstitute the requester pubkey from bytes. If the bytes
        // aren't a valid ed25519 pubkey, drop on the floor — only
        // iroh-verified peers reach this code path, so this is
        // defensive only.
        let Ok(requester_pubkey) = ed25519_dalek::VerifyingKey::from_bytes(&requester) else {
            tracing::warn!(
                requester = %hex::encode(&requester[..4]),
                "CapRequest: bad requester pubkey; dropping"
            );
            return;
        };

        let mut store = self.store.lock().expect("store mutex");

        // Store the partial cap blob so the approver can later read
        // its declared subject/scope/expiry without B re-sending.
        // partial_cap_bytes is already an anybytes::Bytes — wrap it
        // into a typed Blob without re-allocating.
        let blob: Blob<SimpleArchive> = Blob::new(partial_cap_bytes);
        let Ok(partial_cap_handle) = store.put::<SimpleArchive, Blob<SimpleArchive>>(blob) else {
            tracing::warn!("CapRequest: failed to store partial cap blob");
            return;
        };

        // Point-interval at "now" — pending-requests timeline is
        // just "this arrived at T".
        let now = crate::clock::epoch_now();
        let received_at = (now, now).try_to_inline().expect("point interval");

        match crate::policy::record_pending_request(
            &mut *store,
            requester_pubkey,
            partial_cap_handle,
            received_at,
        ) {
            Some(req_id) => {
                let req_id_bytes: [u8; 16] = req_id.into();
                tracing::info!(
                    requester = %hex::encode(&requester[..4]),
                    request_id = %hex::encode(req_id_bytes),
                    "CapRequest recorded as pending"
                );
            }
            None => {
                tracing::warn!(
                    requester = %hex::encode(&requester[..4]),
                    "CapRequest: failed to record on pending-requests pin"
                );
            }
        }
    }

    /// Verify a peer-delivered cap chain against our configured team
    /// root and, on success, store both blobs locally.
    ///
    /// Pinning into a per-team-cap pin (for retention across
    /// compaction) is deferred — the CLI subcommands that surface
    /// "my current cap" will manage that pin. For now the cap+sig
    /// blobs live in the pile as orphan blobs, same as the cap blobs
    /// we issue ourselves via `team invite`. They become reachable
    /// from a branch once the CLI commits them.
    fn absorb_cap_delivery(
        &mut self,
        issuer: PublisherKey,
        cap_bytes: anybytes::Bytes,
        sig_bytes: anybytes::Bytes,
    ) {
        use triblespace_core::blob::Blob;
        use triblespace_core::repo::BlobStoreGet;

        // Verification + swarm-fetch of any missing chain blobs
        // already happened in the host thread's HandshakeHandler
        // (the OP_DELIVER_CAP path doesn't ack STATUS_OK until the
        // chain verifies under our pubkey). The cap+sig blobs +
        // every fetched parent have already arrived as earlier
        // `NetEvent::Blob` events on this channel, so by the time
        // we get here the store already holds them and we only
        // need to pin the team-cap pin onto the leaf pair.
        let cap_blob: Blob<SimpleArchive> = Blob::new(cap_bytes);
        let sig_blob: Blob<SimpleArchive> = Blob::new(sig_bytes);
        let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

        let mut store = self.store.lock().expect("store mutex");

        // Defensive sanity: the cap+sig blobs really are in the
        // store. If not, the host emitted the CapDelivered event
        // without the preceding Blob events somehow — log and bail
        // rather than pin handles that won't resolve.
        let Ok(reader) = store.reader() else {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: pile reader unavailable; dropping"
            );
            return;
        };
        if reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(cap_handle)
            .is_err()
            || reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(sig_handle)
                .is_err()
        {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: blobs missing from store (host should have emitted Blob events first)"
            );
            return;
        }

        match crate::policy::pin_team_cap(&mut *store, self.team_root, cap_handle, sig_handle) {
            Some(_bid) => {
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    sig = %hex::encode(&sig_handle.raw[..4]),
                    "CapDelivered: pinned on team-cap pin"
                );
            }
            None => {
                tracing::warn!(
                    issuer = %hex::encode(&issuer[..4]),
                    "CapDelivered: team-cap pin failed"
                );
            }
        }
    }

    /// Cooldown for re-dispatching undelivered cap blobs. The daemon's
    /// tick cadence is sub-second; without this gate we'd hammer
    /// iroh-connect against a down peer 10× per second.
    const UNDELIVERED_REDISPATCH_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(15);

    /// Re-dispatch the cap+sig pairs for every renewal-policy entry
    /// that's not yet been ack'd by its subject, rate-limited per
    /// entry via `last_dispatch_attempt`. The cap is NOT re-signed —
    /// the same `(latest_cap, latest_sig)` blobs are sent again, so
    /// idempotent on the receiver side (their OP_DELIVER_CAP handler
    /// content-hashes the bytes and dedupes against what's already
    /// pinned).
    ///
    /// Returns the count of entries dispatched this tick.
    fn redispatch_undelivered(&mut self) -> usize {
        use triblespace_core::blob::Blob;
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::repo::BlobStoreGet;

        let mut store = self.store.lock().expect("store mutex");

        let entries = crate::policy::undelivered_entries(&mut *store);
        if entries.is_empty() {
            return 0;
        }

        let now = crate::clock::mono_now();
        let Ok(reader) = store.reader() else {
            return 0;
        };

        let mut dispatched = 0usize;
        for entry in entries {
            // Per-entry cooldown.
            if let Some(prev) = self.last_dispatch_attempt.get(&entry.id) {
                if now.duration_since(*prev) < Self::UNDELIVERED_REDISPATCH_COOLDOWN {
                    continue;
                }
            }

            let Ok(cap_blob) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_cap)
            else {
                continue;
            };
            let Ok(sig_blob) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_sig)
            else {
                continue;
            };

            self.sender.deliver_cap(
                entry.subject.to_bytes(),
                cap_blob.bytes.clone(),
                sig_blob.bytes.clone(),
            );
            self.last_dispatch_attempt.insert(entry.id, now);
            dispatched += 1;
            tracing::debug!(
                subject = %hex::encode(entry.subject.to_bytes()),
                entry = ?entry.id,
                "redispatch_undelivered: re-sent OP_DELIVER_CAP"
            );
        }
        dispatched
    }

    /// Run one tick of the auto-renewal scan.
    ///
    /// Performs two pieces of work each tick:
    ///
    /// 1. **Redispatch undelivered entries.** For each renewal-policy
    ///    entry that's not yet been ack'd by its subject, re-send the
    ///    same `(latest_cap, latest_sig)` blobs via
    ///    `crate::channel::NetCommand::DeliverCap`, rate-limited per
    ///    entry by `Self::UNDELIVERED_REDISPATCH_COOLDOWN`. This is
    ///    what catches the case where the initial `team approve`
    ///    delivery failed (subject offline) and the subject comes back
    ///    later.
    ///
    /// 2. **Re-sign near-expiry entries.** For each entry whose current
    ///    cap upper bound falls within `renewal_window` of now, sign a
    ///    fresh cap+sig (using our team-cap as parent) and dispatch.
    ///    The policy entry is updated in lockstep, which also clears
    ///    any `delivered_at` so step (1) on the next tick picks the
    ///    fresh cap up for re-confirmation.
    ///
    /// Returns the total count of dispatches this tick (undelivered
    /// re-sends + fresh renewals). `0` on every tick after the swarm
    /// settles into steady state means the daemon is quiet.
    ///
    /// Designed to be called from `trible pile net sync`'s main loop
    /// alongside `refresh`. The 1-hour default window assumes a tick
    /// cadence well under that; tune both together for production
    /// deployments.
    pub fn renewal_tick(&mut self, renewal_window: hifitime::Duration) -> usize {
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::blob::{Blob, TryFromBlob};
        use triblespace_core::inline::encodings::hash::Handle;
        use triblespace_core::inline::{Inline, TryToInline};
        use triblespace_core::repo::BlobStoreGet;

        let redispatched = self.redispatch_undelivered();

        let mut store = self.store.lock().expect("store mutex");

        let entries = crate::policy::renewable_within(&mut *store, renewal_window);
        if entries.is_empty() {
            return redispatched;
        }

        // Our own current cap is the parent for every renewal. If
        // we don't have one, we can't sign — log and bail.
        let Some((parent_cap_handle, parent_sig_handle)) =
            crate::policy::current_team_cap(&mut *store, self.team_root)
        else {
            tracing::warn!(
                renewable = entries.len(),
                "renewal_tick: no team-cap pinned; cannot issue successors"
            );
            return 0;
        };

        let Ok(reader) = store.reader() else {
            tracing::warn!("renewal_tick: pile reader unavailable");
            return 0;
        };
        let Ok(parent_cap_blob) =
            reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_cap_handle)
        else {
            tracing::warn!("renewal_tick: parent cap blob missing");
            return 0;
        };
        let Ok(parent_sig_blob) =
            reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_sig_handle)
        else {
            tracing::warn!("renewal_tick: parent sig blob missing");
            return 0;
        };

        let mut dispatched = 0usize;
        for entry in entries {
            // Re-derive scope_facts from the previous cap blob —
            // policy entries carry only the scope_root id, not the
            // facts hanging off it.
            let Ok(prev_cap_blob) =
                reader.get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_cap)
            else {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: previous cap blob missing; skipping entry"
                );
                continue;
            };
            let Ok(prev_set): Result<triblespace_core::trible::TribleSet, _> =
                TryFromBlob::try_from_blob(prev_cap_blob)
            else {
                continue;
            };
            // Extract all tribles hanging off the scope_root entity.
            // pattern!() over the cap blob restricted to entities
            // whose entity-id == scope_root gives us the scope sub-graph.
            let scope_facts = extract_scope_subgraph(&prev_set, entry.scope);

            // Fresh expiry interval: [now, now + window * 2]. The
            // factor-of-two is a heuristic — we want the cap to cover
            // at least one more renewal cycle so missed ticks don't
            // immediately break the chain.
            let now = crate::clock::epoch_now();
            let new_upper = now + renewal_window * 2;
            let Ok(new_expiry) = (now, new_upper).try_to_inline() else {
                continue;
            };

            // Sign.
            let (new_cap, new_sig) = match triblespace_core::repo::capability::build_capability(
                &self.signing_key,
                entry.subject,
                Some((parent_cap_blob.clone(), parent_sig_blob.clone())),
                entry.scope,
                scope_facts,
                new_expiry,
            ) {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(
                        entry = ?entry.id,
                        error = ?e,
                        "renewal_tick: build_capability failed; skipping"
                    );
                    continue;
                }
            };

            let new_cap_handle: Inline<Handle<SimpleArchive>> = (&new_cap).get_handle();
            let new_sig_handle: Inline<Handle<SimpleArchive>> = (&new_sig).get_handle();

            // Persist locally — the next tick's policy update points
            // at these handles; the dispatch ships the bytes. Both
            // sites share the same refcounted `anybytes::Bytes`
            // backing the freshly-signed blob (clones are refcount
            // bumps, no byte-copy).
            let cap_bytes = new_cap.bytes.clone();
            let sig_bytes = new_sig.bytes.clone();
            let _ = store.put::<SimpleArchive, Blob<SimpleArchive>>(new_cap);
            let _ = store.put::<SimpleArchive, Blob<SimpleArchive>>(new_sig);

            // Dispatch over the wire.
            self.sender
                .deliver_cap(entry.subject.to_bytes(), cap_bytes, sig_bytes);
            // Record the attempt so the undelivered-redispatch path
            // doesn't immediately re-fire on the same entry within
            // its cooldown window.
            self.last_dispatch_attempt
                .insert(entry.id, crate::clock::mono_now());

            // Update the policy entry so we don't re-renew on the
            // next tick.
            if crate::policy::update_policy_entry(
                &mut *store,
                entry.id,
                new_expiry,
                new_cap_handle,
                new_sig_handle,
            )
            .is_some()
            {
                dispatched += 1;
                tracing::info!(
                    subject = %hex::encode(entry.subject.to_bytes()),
                    entry = ?entry.id,
                    "renewal_tick: re-issued and dispatched"
                );
            } else {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: re-issued but policy update failed; will retry"
                );
            }
        }
        dispatched + redispatched
    }

    /// Force-republish all positively identified legacy mutable pins carrying
    /// the old pin-metadata schema to the gossip topic, regardless of whether
    /// they appear changed since the last publish. Generic local pins never
    /// become replication roots merely by existing in [`PinStore`].
    ///
    /// Use this for an immediate "I'm still here, here's my legacy state"
    /// announcement. The host loop already performs periodic rebroadcasts.
    /// Each v2 frame carries a fresh anti-deduplication nonce so an explicit
    /// call is intentionally deliverable rather than free.
    ///
    /// Distinct from [`refresh`](Self::refresh): refresh publishes only
    /// the deltas it detects against its diff baselines. This method
    /// republishes everything unconditionally.
    pub fn republish_legacy_heads(&mut self) {
        // ReadOnly suppresses publishing entirely — even republish.
        if self.direction == SyncDirection::ReadOnly {
            return;
        }
        let mut store = self.store.lock().expect("store mutex");
        // Refresh the snapshot served by the network thread BEFORE
        // gossiping — see `refresh` Phase 2 for the ordering rationale.
        if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
            self.sender.update_snapshot(snap);
        }
        let pin_ids: Vec<Id> = match store.pins() {
            Ok(it) => it.filter_map(|r| r.ok()).collect(),
            Err(_) => return,
        };
        for pin_id in pin_ids {
            if let Some(metadata_head) = legacy_pin_metadata_head(&mut *store, pin_id) {
                let pin_bytes: [u8; 16] = pin_id.into();
                self.sender.gossip_legacy_head(pin_bytes, metadata_head.raw);
                self.last_legacy_metadata_heads
                    .insert(pin_id, metadata_head.raw);
            }
        }
    }

    /// Lock and borrow the underlying store. Use for store-specific
    /// methods that aren't part of the storage traits (e.g.
    /// `Pile::flush`, `Yard::collect`, `WeakPinStore::weak_pins`).
    ///
    /// Writes through this borrow bypass blob announcement and eligible legacy
    /// mutable-head gossip, becoming invisible to the network until the next
    /// [`refresh`](Self::refresh) (which is auto-called on the next
    /// read). Don't hold the guard across calls back into the Peer —
    /// its own methods take the same lock.
    pub fn store(&self) -> MutexGuard<'_, S> {
        self.store.lock().expect("store mutex")
    }

    /// Consume the Peer and return the underlying store. The network
    /// thread shuts down when the Peer drops.
    ///
    /// Call [`refresh`](Self::refresh) immediately before this when shutdown
    /// must account for queued network data. `into_store` intentionally only
    /// unwraps ownership; it cannot encode both the store and a sticky refresh
    /// error in its return type.
    ///
    /// # Panics
    ///
    /// Panics if an outstanding [`PeerReader`] still shares the store
    /// (each reader carries a fetch capability that can land blobs into
    /// it) — drop all readers first.
    pub fn into_store(self) -> S {
        let Self { store, .. } = self;
        match Arc::try_unwrap(store) {
            Ok(mutex) => mutex
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            Err(_) => panic!(
                "Peer::into_store: an outstanding PeerReader still shares the store; drop readers first"
            ),
        }
    }

    /// Read `hash` from the local store only, without touching the
    /// swarm. `Some(bytes)` on a local hit, `None` on a local miss —
    /// this is the cheap, non-blocking half of the read path, safe to
    /// call speculatively (e.g. the conservative reference scan asking
    /// "do I already hold this?"). Calls [`refresh`](Self::refresh)
    /// first so freshly-gossiped blobs count as local.
    pub fn try_local(&mut self, hash: RawHash) -> Option<Bytes> {
        let reader = self.reader().ok()?;
        reader.get::<Bytes, UnknownBlob>(Inline::new(hash)).ok()
    }

    /// Honest **async** lazy read: return `hash`'s bytes, fetching from
    /// the swarm and landing them weak-pinned into the store on a local
    /// miss.
    ///
    /// 1. **Local** — one lookup in the store
    ///    (via [`try_local`](Self::try_local)). Hit ⇒ return
    ///    immediately, no network, no pin.
    /// 2. **Miss** — the demand-born weak pin: `pin_weak(hash)` is
    ///    recorded durably FIRST — pinned and **flushed**, so the want
    ///    survives an immediate process exit. The weak pin IS the
    ///    want-signal (a sync daemon's work queue), then — once the
    ///    fetch lands — the retention marker for the fetched blob, then
    ///    the eviction target. Only then is the swarm-addressed fetch
    ///    awaited (DHT-routed, hash-verified) and the verified bytes
    ///    `put` into the store. If the fetch fails, the weak pin stays:
    ///    it remains an outstanding want.
    ///
    /// `Ok(None)` is *Unavailable*: nobody reachable served it before
    /// the budget expired. Existence is semidecidable — there is no
    /// "definitely absent" outcome — and the want stays on record.
    ///
    /// `Err` means the want could NOT be durably recorded (pin or flush
    /// failed). No fetch is attempted in that case: proceeding would
    /// hand the caller bytes whose demand isn't on record, silently
    /// breaking the want-on-record invariant every daemon relies on.
    ///
    /// The swarm fetch is *awaited*, never blocking the caller's thread:
    /// the reply rides a tokio oneshot, so this composes inside any async
    /// consumer and drives cleanly on a single-threaded runtime (the
    /// await yields, letting the host produce the reply).
    pub async fn get_or_fetch_async(
        &mut self,
        hash: RawHash,
    ) -> Result<Option<Bytes>, WantRecordError<S::WeakPinError, <S as StorageFlush>::Error>> {
        if let Some(bytes) = self.try_local(hash) {
            return Ok(Some(bytes));
        }
        // Record the want durably BEFORE the fetch — a failed fetch
        // must leave the demand on record, and a failed RECORD must be
        // an error, never a silent proceed. (Guard dropped before the
        // await: never hold the store lock across a suspension.)
        {
            let mut store = self.store.lock().expect("store mutex");
            store
                .pin_weak(Inline::<Handle<UnknownBlob>>::new(hash))
                .map_err(WantRecordError::Pin)?;
            store.flush().map_err(WantRecordError::Flush)?;
        }
        let Some(raw) = self.fetch_blob(hash).await else {
            return Ok(None);
        };
        let bytes = Bytes::from(raw);
        {
            let mut store = self.store.lock().expect("store mutex");
            if let Err(e) = store.put::<UnknownBlob, Bytes>(bytes.clone()) {
                // Landing failed but the verified bytes are in hand and
                // the want IS on record — a later reconcile pass re-lands
                // it. Loud trace, non-fatal.
                tracing::warn!(
                    hash = %hex::encode(&hash[..4]),
                    error = ?e,
                    "get_or_fetch: landing fetched blob failed"
                );
            }
        }
        Ok(Some(bytes))
    }
}

// ── Trait delegations ───────────────────────────────────────────────
//
// Reads (`reader`, `head`, `pins`) call `refresh()` first so they
// always see the latest gossiped state AND any external writes that
// landed since the last refresh get announced. Writes (`put`, `update`)
// delegate to the inner store and then push the new state out via the
// network thread, updating the diff baselines so refresh doesn't
// double-announce.

impl<S> BlobStorePut for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        let mut store = self.store.lock().expect("store mutex");
        let handle = store.put(item)?;
        // Snapshot first, then announce — see `refresh` Phase 2 for the
        // ordering rationale. Without this, DHT-receivers of the announce
        // dial us, OP_GET_BLOB hits the stale snapshot, returns missing,
        // and the receiver waits for backoff to retry.
        if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
            self.sender.update_snapshot(snap);
        }
        if self.direction != SyncDirection::ReadOnly {
            self.sender.announce(handle.raw);
        }
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = store.reader().ok();
        Ok(handle)
    }
}

impl<S> BlobStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Reader = PeerReader<S::Reader>;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let _ = self.refresh();
        let local = self.store.lock().expect("store mutex").reader()?;
        // The fetch capability: a clone of the command sender plus a
        // landing handle into the *shared* store, so a `&self` async
        // read can pull a missing blob from the swarm, record the
        // demand-born weak pin, and land the bytes.
        let fetch = Some(FetchCap {
            sender: self.sender.clone(),
            sink: Arc::new(SharedStore(self.store.clone())),
        });
        Ok(PeerReader { local, fetch })
    }
}

impl<S> StorageFlush for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type Error = <S as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.store.lock().expect("store mutex").flush()
    }
}

impl<S> BranchAssertionStore for Peer<S>
where
    S: BlobStore
        + BlobStorePut
        + PinStore
        + WeakPinStore
        + StorageFlush
        + BranchAssertionStore
        + Send
        + 'static,
{
    type Error = <S as BranchAssertionStore>::Error;

    fn assertion_snapshot(&mut self) -> Result<BranchAssertionSnapshot, Self::Error> {
        let _ = self.refresh();
        self.store.lock().expect("store mutex").assertion_snapshot()
    }

    fn append_assertion(&mut self, assertion: BranchAssertion) -> Result<(), Self::Error> {
        // Assertions are already immutable, verified values; unlike mutable
        // local tracking pins, appending one needs no CAS or scalar-head
        // announcement.
        // TODO(strongpin-assertion-replication): replicate the signed exact
        // assertion over a dedicated protocol; legacy HEAD gossip cannot
        // represent its `(author, name handle) -> commit` identity.
        self.store
            .lock()
            .expect("store mutex")
            .append_assertion(assertion)
    }
}

impl<S> PinStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + WeakPinStore + StorageFlush + Send + 'static,
{
    type PinsError = S::PinsError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;
    // Collected eagerly: the inner store's iterator would borrow the
    // mutex guard, which cannot leave this call.
    type ListIter<'a>
        = std::vec::IntoIter<Result<Id, S::PinsError>>
    where
        S: 'a;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        let _ = self.refresh();
        let mut store = self.store.lock().expect("store mutex");
        let ids: Vec<Result<Id, S::PinsError>> = store.pins()?.collect();
        Ok(ids.into_iter())
    }

    fn head(&mut self, id: Id) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        let _ = self.refresh();
        self.store.lock().expect("store mutex").head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let mut store = self.store.lock().expect("store mutex");
        let result = store.update(id, old, new.clone())?;
        if let PushResult::Success() = &result {
            if let Some(metadata_head) = new {
                // Refresh the snapshot served by the network thread
                // BEFORE gossiping — see `refresh` Phase 2 for the
                // ordering rationale.
                if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
                    self.sender.update_snapshot(snap);
                }
                // Tracking pins are local mirror state and must NOT be
                // re-gossiped — otherwise the publisher would receive its
                // own tracking pin back and create tracking-of-tracking,
                // ad infinitum. Same logic for policy pins
                // (renewal state, pending requests, per-team-cap pins) —
                // they're per-peer local state.
                if is_legacy_pin_metadata(&mut *store, id, metadata_head)
                    && self.direction != SyncDirection::ReadOnly
                {
                    let pin_bytes: [u8; 16] = id.into();
                    self.sender.gossip_legacy_head(pin_bytes, metadata_head.raw);
                    self.last_legacy_metadata_heads
                        .insert(id, metadata_head.raw);
                }
            }
        }
        Ok(result)
    }
}

/// Return the current head only when `pin_id` is positively identifiable as a
/// legacy mutable pin carrying the old pin-metadata schema.
///
/// Generic [`PinStore`] entries are local retention or bookkeeping primitives,
/// not an implicit replication surface. An eligible legacy pin must have a readable
/// metadata blob with exactly one `metadata::name` on the unique entity scoped
/// to the pin id, and must not carry a tracking or local-policy marker.
fn legacy_pin_metadata_head<S: BlobStore + PinStore>(
    store: &mut S,
    pin_id: Id,
) -> Option<Inline<Handle<SimpleArchive>>> {
    let metadata_head = store.head(pin_id).ok().flatten()?;
    is_legacy_pin_metadata(store, pin_id, metadata_head).then_some(metadata_head)
}

fn is_legacy_pin_metadata<S: BlobStore>(
    store: &mut S,
    pin_id: Id,
    metadata_head: Inline<Handle<SimpleArchive>>,
) -> bool {
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::macros::{find, pattern};

    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta): Result<triblespace_core::trible::TribleSet, _> = reader.get(metadata_head) else {
        return false;
    };
    let Ok(branch_entity) = triblespace_core::repo::branch::branch_entity(&meta, pin_id) else {
        return false;
    };
    let is_tracking = find!(
        remote: Id,
        pattern!(&meta, [{ branch_entity @ crate::tracking::tracking_remote_pin: ?remote }])
    )
    .next()
    .is_some();
    let is_local_only = find!(
        kind: Id,
        pattern!(&meta, [{ _?marker @ crate::policy::local_only_pin: ?kind }])
    )
    .next()
    .is_some();
    if is_tracking || is_local_only {
        return false;
    }
    let mut names = find!(
        name: Inline<Handle<LongString>>,
        pattern!(&meta, [{ branch_entity @ triblespace_core::metadata::name: ?name }])
    );
    matches!((names.next(), names.next()), (Some(_), None))
}

/// Read the display name from a positively identified legacy pin-metadata head.
///
/// Local tracking pins carry `remote_name`, but accepting that marker here
/// would create tracking-of-tracking across mixed-version peers. Only the
/// original legacy pin-metadata schema (`metadata::name`) is transport-eligible.
fn read_legacy_pin_name<S: BlobStore>(
    store: &mut S,
    metadata_head_hash: &RawHash,
    remote_pin_id: Id,
) -> Option<String> {
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::repo::BlobStoreGet;

    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*metadata_head_hash);
    let meta: triblespace_core::trible::TribleSet = reader.get(meta_handle).ok()?;
    let branch_entity = triblespace_core::repo::branch::branch_entity(&meta, remote_pin_id).ok()?;

    let mut names = find!(
        h: Inline<Handle<LongString>>,
        pattern!(&meta, [{ branch_entity @ triblespace_core::metadata::name: ?h }])
    );
    let name_handle = match (names.next(), names.next()) {
        (Some(name), None) => name,
        _ => return None,
    };

    let name_view: anybytes::View<str> = reader.get(name_handle).ok()?;
    Some(name_view.as_ref().to_string())
}

/// Extract every trible whose entity is `scope_root` from `set`,
/// returning them as a fresh TribleSet. Used by `renewal_tick` to
/// reconstruct the scope-facts argument to `build_capability` from
/// the previous-cap blob — policy entries carry only the
/// `scope_root` id, not the facts hanging off it.
fn extract_scope_subgraph(
    set: &triblespace_core::trible::TribleSet,
    scope_root: triblespace_core::id::Id,
) -> triblespace_core::trible::TribleSet {
    let mut result = triblespace_core::trible::TribleSet::new();
    for trible in set.iter() {
        if *trible.e() == scope_root {
            result.insert(trible);
        }
    }
    result
}

/// The read view of a [`Peer`]: the store's own reader (`L`) plus a
/// swarm-fetch capability.
///
/// Two read surfaces with deliberately different semantics:
/// - the **sync** [`BlobStoreGet`] is *local only* — one lookup in the
///   store snapshot, never the swarm. This keeps speculative gets (the
///   conservative reference scan, existence checks) cheap and total:
///   enumeration and existence stay local, the decomplecting that lets
///   "the layers above the blob substrate do whatever fancy dance they
///   like" hold.
/// - the **async** [`AsyncBlobStoreGet`] is *transparent* — local
///   lookup, else a demand-born weak pin followed by an awaited swarm
///   fetch that lands the result in the shared store. This is what
///   gives a generic async consumer (a lazy `Repository::checkout`)
///   lazy replication for free, without ever knowing it holds a `Peer`.
///
/// So existence-vs-retrieval is split by *which trait you call*, not by
/// a bespoke method: probe with the sync `get`, retrieve with the async
/// one.
///
/// [`AsyncBlobStoreGet`]: triblespace_core::repo::async_store::AsyncBlobStoreGet
pub struct PeerReader<L> {
    local: L,
    /// Swarm-fetch capability for the async transparent read. The sync
    /// reads never touch it; it carries the command sender plus a
    /// landing handle into the Peer's shared store.
    fetch: Option<FetchCap>,
}

/// The capability a [`PeerReader`] needs to pull a missing blob from the
/// swarm: the host command sender + a want-recording/landing sink into
/// the Peer's shared store.
#[derive(Clone)]
struct FetchCap {
    sender: NetSender,
    sink: Arc<dyn StoreSink>,
}

/// Interior-mutable access to the Peer's shared store for a `&self`
/// async read: record the demand-born weak pin, land the fetched bytes.
/// Erases the concrete store type `S` so `PeerReader` need not carry it
/// — which is also why `record_want`'s error is boxed.
trait StoreSink: Send + Sync {
    /// Durably record the want: weak-pin `hash` AND flush it BEFORE the
    /// fetch, so a failed fetch — or an immediate process exit — leaves
    /// the outstanding demand on record. A failed record is an error the
    /// read must surface, never a warn-and-continue.
    fn record_want(&self, hash: RawHash) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    /// Land fetched `bytes` as an `UnknownBlob` into the store.
    fn land(&self, bytes: Bytes);
}

/// `StoreSink` over the Peer's shared store handle.
struct SharedStore<S>(Arc<Mutex<S>>);

impl<S> StoreSink for SharedStore<S>
where
    S: BlobStorePut + WeakPinStore + StorageFlush + Send + 'static,
{
    fn record_want(&self, hash: RawHash) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut store = self.0.lock().expect("store mutex");
        store
            .pin_weak(Inline::<Handle<UnknownBlob>>::new(hash))
            .map_err(|e| {
                Box::new(WantRecordError::<_, <S as StorageFlush>::Error>::Pin(e))
                    as Box<dyn std::error::Error + Send + Sync>
            })?;
        store.flush().map_err(|e| {
            Box::new(WantRecordError::<S::WeakPinError, _>::Flush(e))
                as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(())
    }

    fn land(&self, bytes: Bytes) {
        if let Ok(mut store) = self.0.lock() {
            if let Err(e) = store.put::<UnknownBlob, Bytes>(bytes) {
                tracing::warn!(error = ?e, "reader fetch: landing fetched blob failed");
            }
        }
    }
}

// Identity ignores the fetch capability: two readers are equal iff their
// local store views are — the capability is a handle, not part of the
// snapshot's value. Hand-rolled because `NetSender` / `Arc<dyn
// StoreSink>` are neither `PartialEq` nor (for the sender) `Sync`, so
// the derive can't apply.
impl<L: Clone> Clone for PeerReader<L> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            fetch: self.fetch.clone(),
        }
    }
}
impl<L: PartialEq> PartialEq for PeerReader<L> {
    fn eq(&self, other: &Self) -> bool {
        self.local == other.local
    }
}
impl<L: Eq> Eq for PeerReader<L> {}

/// Error from the async transparent read on a [`PeerReader`].
#[derive(Debug)]
pub enum PeerReaderGetError<E> {
    /// The bytes (local or swarm-fetched) didn't convert to the
    /// requested type.
    Conversion(E),
    /// Not held locally and the swarm didn't serve it before the host
    /// resolved the fetch. Existence is semidecidable — this is
    /// "not obtained", never "definitely absent". The demand-born weak
    /// pin recorded before the fetch stays: the want is on record.
    Unavailable,
    /// Local miss AND the demand-born weak pin could not be durably
    /// recorded (pin or flush failed). No fetch was attempted — the
    /// want-on-record invariant must hold before any bytes move.
    /// Boxed because the reader's store type is erased behind the
    /// fetch capability; the concrete error is a
    /// [`WantRecordError`].
    WantRecord(Box<dyn std::error::Error + Send + Sync>),
}

impl<E: std::error::Error> std::fmt::Display for PeerReaderGetError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conversion(e) => write!(f, "blob conversion failed: {e}"),
            Self::Unavailable => write!(f, "blob unavailable (local miss + swarm did not serve)"),
            Self::WantRecord(e) => {
                write!(f, "blob missing and want not recorded: {e}")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PeerReaderGetError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Conversion(e) => Some(e),
            Self::Unavailable => None,
            Self::WantRecord(e) => Some(e.as_ref()),
        }
    }
}

impl<L> BlobStoreGet for PeerReader<L>
where
    L: BlobStoreGet,
{
    type GetError<E: std::error::Error + Send + Sync + 'static> = L::GetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.local.get::<T, S>(handle)
    }
}

impl<L> BlobStoreList for PeerReader<L>
where
    L: BlobStoreList,
{
    type Iter<'a>
        = L::Iter<'a>
    where
        L: 'a;
    type Err = L::Err;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        self.local.blobs()
    }
}

impl<L> PartialCommitDag for PeerReader<L>
where
    L: PartialCommitDag,
{
    type Error = L::Error;

    fn parents(
        &mut self,
        commit: Inline<Handle<SimpleArchive>>,
    ) -> Result<ParentLookup, Self::Error> {
        self.local.parents(commit)
    }
}

// Conservative reference discovery works through the local `get`: the
// default scan checks each 32-byte chunk against the store snapshot,
// which — post-fetch — also holds any weak-pinned lazily-landed blobs.
impl<L> BlobChildren for PeerReader<L> where L: BlobStoreGet {}

/// Transparent async read: local lookup → a demand-born weak pin + an
/// awaited swarm fetch that lands the result in the shared store. This
/// is the surface a *generic* async consumer depends on to get lazy
/// replication for free — it never needs to know it's holding a `Peer`.
impl<L> triblespace_core::repo::async_store::AsyncBlobStoreGet for PeerReader<L>
where
    L: BlobStoreGet + Clone + Send + 'static,
{
    type GetError<E: std::error::Error + Send + Sync + 'static> = PeerReaderGetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> impl std::future::Future<Output = Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>> + Send
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        // Clone the owned read handle + fetch capability *before* the
        // async block so the future captures only `Send` values — never
        // `&self` (`NetSender` is `!Sync`). Keeps the future `Send`
        // without forcing `L: Sync`.
        let raw = handle.raw;
        let local = self.local.clone();
        let fetch = self.fetch.clone();
        async move {
            // Universal byte read: the store snapshot locally, else the
            // swarm. Bytes-by-hash everywhere, so deserialization to the
            // requested schema happens once, below.
            let bytes: Bytes = if let Ok(b) = local.get::<Bytes, UnknownBlob>(Inline::new(raw)) {
                b
            } else if let Some(cap) = fetch {
                // The demand-born weak pin: record the want durably
                // FIRST (pin + flush), then fetch. A failed fetch
                // leaves the pin — it remains an outstanding want. A
                // failed RECORD is an error: never fetch bytes whose
                // demand isn't on record.
                cap.sink
                    .record_want(raw)
                    .map_err(PeerReaderGetError::WantRecord)?;
                // Inline swarm fetch; the host verified
                // blake3(bytes) == raw before returning. Interactive
                // budget: a transparent read is a caller actively
                // waiting.
                match cap
                    .sender
                    .fetch_blob(raw, crate::host::INTERACTIVE_FETCH_DEADLINE)
                    .await
                {
                    Some(v) => {
                        let b = Bytes::from(v);
                        cap.sink.land(b.clone());
                        b
                    }
                    None => return Err(PeerReaderGetError::Unavailable),
                }
            } else {
                return Err(PeerReaderGetError::Unavailable);
            };
            triblespace_core::blob::Blob::<S>::new(bytes)
                .try_from_blob()
                .map_err(PeerReaderGetError::Conversion)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::IntoBlob;
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::id::Id;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::trible::TribleSet;

    #[derive(Debug, thiserror::Error)]
    #[error("injected put failure")]
    struct InjectedPutError;

    #[derive(Default)]
    struct FailingPutRepo {
        inner: MemoryRepo,
    }

    impl BlobStorePut for FailingPutRepo {
        type PutError = InjectedPutError;

        fn put<S, T>(&mut self, _item: T) -> Result<Inline<Handle<S>>, Self::PutError>
        where
            S: BlobEncoding + 'static,
            T: IntoBlob<S>,
            Handle<S>: InlineEncoding,
        {
            Err(InjectedPutError)
        }
    }

    impl BlobStore for FailingPutRepo {
        type Reader = <MemoryRepo as BlobStore>::Reader;
        type ReaderError = <MemoryRepo as BlobStore>::ReaderError;

        fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
            self.inner.reader()
        }
    }

    impl PinStore for FailingPutRepo {
        type PinsError = <MemoryRepo as PinStore>::PinsError;
        type HeadError = <MemoryRepo as PinStore>::HeadError;
        type UpdateError = <MemoryRepo as PinStore>::UpdateError;
        type ListIter<'a> = <MemoryRepo as PinStore>::ListIter<'a>;

        fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
            self.inner.pins()
        }

        fn head(
            &mut self,
            id: Id,
        ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
            self.inner.head(id)
        }

        fn update(
            &mut self,
            id: Id,
            old: Option<Inline<Handle<SimpleArchive>>>,
            new: Option<Inline<Handle<SimpleArchive>>>,
        ) -> Result<PushResult, Self::UpdateError> {
            self.inner.update(id, old, new)
        }
    }

    impl WeakPinStore for FailingPutRepo {
        type WeakPinError = <MemoryRepo as WeakPinStore>::WeakPinError;
        type WeakListIter<'a> = <MemoryRepo as WeakPinStore>::WeakListIter<'a>;

        fn pin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
        where
            S: BlobEncoding + 'static,
            Handle<S>: InlineEncoding,
        {
            self.inner.pin_weak(handle)
        }

        fn unpin_weak<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::WeakPinError>
        where
            S: BlobEncoding + 'static,
            Handle<S>: InlineEncoding,
        {
            self.inner.unpin_weak(handle)
        }

        fn weak_pins<'a>(&'a mut self) -> Result<Self::WeakListIter<'a>, Self::WeakPinError> {
            self.inner.weak_pins()
        }
    }

    impl StorageFlush for FailingPutRepo {
        type Error = <MemoryRepo as StorageFlush>::Error;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.inner.flush()
        }
    }

    #[test]
    fn incoming_blob_failure_is_sticky_and_blocks_the_following_head() {
        let signing_key = SigningKey::from_bytes(&[6; 32]);
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            FailingPutRepo::default(),
            signing_key.clone(),
            SyncDirection::Bidirectional,
            signing_key.verifying_key(),
            sender,
            receiver,
        );

        wiring
            .evt_tx
            .send(NetEvent::Blob(Bytes::from(b"fetched blob".to_vec())))
            .unwrap();
        wiring
            .evt_tx
            .send(NetEvent::LegacyHead {
                pin: [1; 16],
                metadata_head: [2; 32],
                publisher: signing_key.verifying_key().to_bytes(),
            })
            .unwrap();

        let first = peer.refresh().unwrap_err();
        assert_eq!(first.operation(), "persist incoming fetched blob");
        assert_eq!(peer.refresh().unwrap_err(), first);
        assert!(
            peer.store.lock().unwrap().inner.pins.is_empty(),
            "the later HEAD must not materialize after a fetched-blob write failed"
        );
    }

    #[test]
    fn write_only_discards_data_events_but_absorbs_cap_requests() {
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let requester = SigningKey::from_bytes(&[8; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            SyncDirection::WriteOnly,
            signing_key.verifying_key(),
            sender,
            receiver,
        );

        let ignored = Bytes::from(b"incoming data must be ignored".to_vec());
        let ignored_handle = Blob::<UnknownBlob>::new(ignored.clone()).get_handle();
        wiring
            .evt_tx
            .send(NetEvent::Blob(ignored))
            .expect("event channel open");
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: Bytes::from(b"partial capability".to_vec()),
            })
            .expect("event channel open");

        peer.refresh().unwrap();

        let mut store = peer.store.lock().expect("store mutex");
        let reader = store.reader().expect("memory reader");
        assert!(
            reader.get::<Bytes, UnknownBlob>(ignored_handle).is_err(),
            "write-only peers must not materialize incoming blob events"
        );
        let pending = crate::policy::list_pending_requests(&mut *store);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].requester, requester);
    }

    #[test]
    fn generic_local_pins_are_not_gossiped_as_legacy_heads() {
        let signing_key = SigningKey::from_bytes(&[9; 32]);
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            SyncDirection::Bidirectional,
            signing_key.verifying_key(),
            sender,
            receiver,
        );

        let generic_id = Id::new([1; 16]).unwrap();
        let generic_head = peer
            .put::<SimpleArchive, _>(TribleSet::new())
            .expect("store generic pin value");
        while wiring.cmd_rx.try_recv().is_ok() {}
        peer.update(generic_id, None, Some(generic_head))
            .expect("update generic pin");
        assert!(
            wiring.cmd_rx.try_iter().all(|command| !matches!(
                command,
                crate::channel::NetCommand::GossipLegacyHead { .. }
            )),
            "generic local pins must not become legacy gossip roots"
        );

        let legacy_pin_id = Id::new([2; 16]).unwrap();
        let name: Inline<Handle<LongString>> = peer
            .put("main".to_owned().to_blob())
            .expect("store legacy pin name");
        let metadata = triblespace_core::repo::branch::branch_unsigned(legacy_pin_id, name, None);
        let metadata_head = peer.put(metadata).expect("store legacy pin metadata");
        while wiring.cmd_rx.try_recv().is_ok() {}
        peer.update(legacy_pin_id, None, Some(metadata_head))
            .expect("update legacy pin");
        assert!(wiring.cmd_rx.try_iter().any(|command| {
            matches!(
                command,
                crate::channel::NetCommand::GossipLegacyHead { pin, metadata_head: observed }
                    if pin == <[u8; 16]>::from(legacy_pin_id) && observed == metadata_head.raw
            )
        }));
    }
}
