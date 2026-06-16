//! `Peer<S>`: a store wrapped in distributed network sync.
//!
//! Owns the inner store, spawns the iroh network thread on construction,
//! and exposes the standard storage traits (`BlobStore + BlobStorePut +
//! PinStore`) with two layers of network behavior built in:
//!
//! - **Reads** auto-call [`refresh`](Peer::refresh), which drains pending
//!   incoming gossip events into the wrapped store and re-publishes any
//!   deltas from external writers (e.g. another process appended to the
//!   same pile file). Mirrors `Pile::refresh` — the explicit method is
//!   available for tight loops, but normal storage use Just Works.
//! - **Writes** delegate to the inner store and then announce blobs to
//!   the DHT and gossip branch updates over the topic mesh, all via the
//!   network thread.
//!
//! Branch-state discovery is gossip-driven: HEAD updates for the
//! team's branches flood the team topic and arrive via the
//! [`NetEvent`] channel; the network thread autonomously walks
//! reachable closures via DHT-routed blob fetches. There are no
//! peer-targeted RPCs on the public surface — peers serve content
//! but don't get asked "what branches do you have." That question
//! is asked of the team, via the topic, not of any individual peer.

use std::collections::HashMap;

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::{BlobEncoding, IntoBlob, TryFromBlob};
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;

use crate::cache::NullCache;
use crate::channel::{NetEvent, PublisherKey};
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::RawHash;

pub use crate::host::{PeerConfig, SyncDirection};

/// The cache tier's reader. Both [`NullCache`](crate::cache::NullCache)
/// and [`BoundedBlobStore`](crate::cache::BoundedBlobStore) expose
/// `MemoryBlobStore`'s snapshot reader, so the read-path union
/// ([`PeerReader`]) can name it concretely instead of threading a
/// second reader type parameter through every bound.
type CacheReader = <triblespace_core::blob::MemoryBlobStore as BlobStore>::Reader;

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
/// // From here `peer` is just a `BlobStore + BlobStorePut +
/// // PinStore` — wrap it in `Repository::new` and use it like
/// // any other triblespace storage.
/// drop(peer);
/// ```
pub struct Peer<S, C = NullCache>
where
    S: BlobStore + BlobStorePut + PinStore,
    C: BlobStore<Reader = CacheReader> + BlobStorePut,
{
    store: S,

    /// Cache tier: where read-miss swarm fetches land
    /// ([`get_or_fetch`](Peer::get_or_fetch)). Checked *after* `store`
    /// (Durable) on every read, and never pinned — eviction is always
    /// safe ("pins are promises, caches are free"). `NullCache` by
    /// default, which makes a `Peer<S>` behave exactly as before: eager
    /// history, no content cache. See [`crate::cache`].
    cache: C,

    sender: NetSender,
    receiver: NetReceiver,

    /// Baseline blob snapshot for diff-and-publish on `refresh`. The Reader
    /// is a frozen view (for backends with snapshot semantics like Pile) so
    /// `current.blobs_diff(&last)` returns exactly the blobs added since
    /// the last refresh.
    last_blob_reader: Option<S::Reader>,

    /// Baseline branch heads for diff-and-publish on `refresh`. Updated on
    /// every Peer-driven write so we don't double-gossip our own changes.
    last_branches: HashMap<Id, RawHash>,

    /// Direction of swarm participation — controls whether we publish
    /// local HEADs and/or react to remote HEADs.
    direction: SyncDirection,

    /// Monotonic time of the most recent NetEvent absorbed in
    /// [`refresh`](Peer::refresh). Drives quiescence-based stopping
    /// in long-running sync drivers. Read through [`crate::clock`] so
    /// simulated runs measure quiescence in virtual time.
    last_event_at: crate::clock::Mono,

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

impl<S> Peer<S, NullCache>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    /// Wrap a store in a Peer with no cache tier — eager history only,
    /// today's behavior. Spawns the iroh network thread internally.
    ///
    /// The thread lives for the Peer's lifetime and shuts down when the
    /// Peer drops. For a node that lazily caches swarm-fetched content
    /// in a bounded tier, use [`Peer::with_cache`].
    pub fn new(store: S, key: SigningKey, config: PeerConfig) -> Self {
        let direction = config.direction;
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);
        Self::assemble(
            store,
            NullCache::new(),
            sender,
            receiver,
            direction,
            team_root,
            signing_key,
        )
    }

    /// Wrap a store in a Peer (no cache tier) over caller-provided
    /// channel halves — the host loop runs wherever the caller put it
    /// (deterministic simulation: a local task on a shared paused
    /// runtime) instead of on an internally-spawned thread.
    ///
    /// Pair with [`crate::host::wire`] + [`crate::host::run_host`]. For
    /// a cached peer over caller-provided wiring, use
    /// [`Peer::with_wiring_and_cache`].
    pub fn with_wiring(
        store: S,
        signing_key: SigningKey,
        direction: SyncDirection,
        team_root: ed25519_dalek::VerifyingKey,
        sender: host::NetSender,
        receiver: host::NetReceiver,
    ) -> Self {
        Self::assemble(
            store,
            NullCache::new(),
            sender,
            receiver,
            direction,
            team_root,
            signing_key,
        )
    }
}

impl<S, C> Peer<S, C>
where
    S: BlobStore + BlobStorePut + PinStore,
    C: BlobStore<Reader = CacheReader> + BlobStorePut,
{
    /// Wrap a store in a Peer with an explicit cache tier (e.g. a
    /// [`BoundedBlobStore`](crate::cache::BoundedBlobStore)). Read-miss
    /// swarm fetches via [`get_or_fetch`](Self::get_or_fetch) land in
    /// `cache`, never in the Durable store. Spawns the iroh network
    /// thread internally.
    pub fn with_cache(store: S, cache: C, key: SigningKey, config: PeerConfig) -> Self {
        let direction = config.direction;
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);
        Self::assemble(
            store,
            cache,
            sender,
            receiver,
            direction,
            team_root,
            signing_key,
        )
    }

    /// Cached peer over caller-provided channel halves — the
    /// deterministic-simulation constructor for a two-tier node. See
    /// [`Self::with_wiring`] for the no-cache form and the wiring
    /// contract.
    pub fn with_wiring_and_cache(
        store: S,
        cache: C,
        signing_key: SigningKey,
        direction: SyncDirection,
        team_root: ed25519_dalek::VerifyingKey,
        sender: host::NetSender,
        receiver: host::NetReceiver,
    ) -> Self {
        Self::assemble(
            store,
            cache,
            sender,
            receiver,
            direction,
            team_root,
            signing_key,
        )
    }

    fn assemble(
        mut store: S,
        cache: C,
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
            store,
            cache,
            sender,
            receiver,
            last_blob_reader: None,
            last_branches: HashMap::new(),
            direction,
            last_event_at: crate::clock::mono_now(),
            team_root,
            signing_key,
            last_dispatch_attempt: HashMap::new(),
        };

        // Drive the first refresh synchronously so the DHT learns
        // about pre-existing blobs before construction returns and the
        // first incoming AUTH can land.
        peer.refresh();

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

    /// Issue a swarm-addressed on-demand blob fetch and return its
    /// oneshot reply receiver — the lazy-replication read-miss
    /// primitive. `.await` it for the verified bytes or `None`
    /// (Unavailable); a dropped sender (host gone) also resolves to
    /// `None`, never a hang. Does NOT persist the result — that is the
    /// caller's policy choice (cache tier vs pin). Used by
    /// [`get_or_fetch_async`](Self::get_or_fetch_async) and by
    /// deterministic-sim drivers (step the sim + `try_recv`).
    pub fn request_blob(
        &self,
        hash: RawHash,
    ) -> tokio::sync::oneshot::Receiver<Option<Vec<u8>>> {
        self.sender.request_blob(hash)
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
    pub fn refresh(&mut self) {
        // ── Phase 1: drain incoming events ────────────────────────────
        // WriteOnly suppresses incoming-event handling: we always
        // drain the channel to keep it from filling, but skip the
        // store mutation. The local node has nothing to learn from
        // the swarm.
        while let Some(event) = self.receiver.try_recv() {
            self.last_event_at = crate::clock::mono_now();
            if self.direction == SyncDirection::WriteOnly {
                continue;
            }
            match event {
                NetEvent::Blob(data) => {
                    // `data` is already an anybytes::Bytes (refcounted) —
                    // pass it into the store without re-wrapping.
                    let _ = self.store.put::<UnknownBlob, Bytes>(data);
                }
                NetEvent::Head { branch, head, publisher } => {
                    if let Some(remote_id) = Id::new(branch) {
                        match read_remote_name(&mut self.store, &head) {
                            Some(name) => {
                                let r = crate::tracking::ensure_tracking_pin(
                                    &mut self.store,
                                    remote_id,
                                    &head,
                                    &name,
                                    &publisher,
                                );
                                tracing::trace!(
                                    head = %hex::encode(&head[..4]),
                                    ok = r.is_some(),
                                    "head event -> ensure_tracking_pin"
                                );
                            }
                            None => {
                                tracing::warn!(
                                    head = %hex::encode(&head[..4]),
                                    "peer: head event but branch meta unreadable; dropped"
                                );
                            }
                        }
                    }
                }
                NetEvent::CapRequest { requester, partial_cap_bytes } => {
                    self.absorb_cap_request(requester, partial_cap_bytes);
                }
                NetEvent::CapDelivered { issuer, cap_bytes, sig_bytes } => {
                    // Verify the delivered chain against our configured
                    // team root, then store both blobs locally. Pinning
                    // them into a per-team-cap pin (so compaction
                    // retains them) comes with the CLI subcommands —
                    // for now they're orphan blobs in the pile, same
                    // as our own outgoing-cap blobs.
                    self.absorb_cap_delivery(issuer, cap_bytes, sig_bytes);
                }
                NetEvent::CapDeliveryConfirmed { subject, sig_handle } => {
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
                    let sig_inline: Inline<Handle<SimpleArchive>> =
                        Inline::new(sig_handle);
                    if let Some(entry_id) =
                        crate::policy::find_policy_entry_by_subject_and_sig(
                            &mut self.store,
                            subject_key,
                            sig_inline,
                        )
                    {
                        let _ = crate::policy::mark_policy_delivered(
                            &mut self.store,
                            entry_id,
                        );
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

        // ── Phase 2: refresh the snapshot served by the network thread ─
        //
        // MUST happen before any announce/gossip below: peers who hear
        // our announce/gossip will dial us to fetch the closure, and
        // the network thread serves them out of this snapshot. If we
        // gossiped first, a fast-dialing peer would hit `has_blob =
        // false` on the still-stale snapshot and the server would deny
        // OP_CHILDREN/OP_GET_BLOB as "out of scope" — even though we
        // just told them we have it.
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
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
        if let Ok(current) = self.store.reader() {
            if self.direction != SyncDirection::ReadOnly {
                match self.last_blob_reader.as_ref() {
                    Some(baseline) => {
                        for handle in current.blobs_diff(baseline).flatten() {
                            self.sender.announce(handle.raw);
                        }
                    }
                    None => {
                        use triblespace_core::repo::BlobStoreList;
                        for handle in current.blobs().filter_map(Result::ok) {
                            self.sender.announce(handle.raw);
                        }
                    }
                }
            }
            self.last_blob_reader = Some(current);
        }

        // ── Phase 4: diff-and-publish branch deltas ───────────────────
        // ReadOnly skips this entire phase — followers don't gossip.
        if self.direction != SyncDirection::ReadOnly {
            let bids: Vec<Id> = match self.store.pins() {
                Ok(it) => it.filter_map(|r| r.ok()).collect(),
                Err(_) => return,
            };
            for bid in bids {
                if crate::tracking::is_tracking_pin(&mut self.store, bid) {
                    continue;
                }
                // Local-only policy pins (renewal policy, pending
                // requests, per-team-cap pins) carry per-peer
                // state that mustn't leak to the team mesh. See
                // `crate::policy`.
                if crate::policy::is_local_only_pin(&mut self.store, bid) {
                    continue;
                }
                let head = match self.store.head(bid) {
                    Ok(Some(h)) => h,
                    _ => continue,
                };
                if self.last_branches.get(&bid) != Some(&head.raw) {
                    let bid_bytes: [u8; 16] = bid.into();
                    self.sender.gossip(bid_bytes, head.raw);
                    self.last_branches.insert(bid, head.raw);
                }
            }
        }
    }

    /// Persist an incoming join request: store the partial-cap blob,
    /// then add a pending-request entity to the local pending-requests
    /// branch. The entity id becomes the value `team approve <id>`
    /// consumes; the partial-cap blob is recoverable from the entity's
    /// `request_partial_cap` handle.
    fn absorb_cap_request(
        &mut self,
        requester: PublisherKey,
        partial_cap_bytes: anybytes::Bytes,
    ) {
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

        // Store the partial cap blob so the approver can later read
        // its declared subject/scope/expiry without B re-sending.
        // partial_cap_bytes is already an anybytes::Bytes — wrap it
        // into a typed Blob without re-allocating.
        let blob: Blob<SimpleArchive> = Blob::new(partial_cap_bytes);
        let Ok(partial_cap_handle) = self
            .store
            .put::<SimpleArchive, Blob<SimpleArchive>>(blob)
        else {
            tracing::warn!("CapRequest: failed to store partial cap blob");
            return;
        };

        // Point-interval at "now" — pending-requests timeline is
        // just "this arrived at T".
        let now = crate::clock::epoch_now();
        let received_at = (now, now).try_to_inline().expect("point interval");

        match crate::policy::record_pending_request(
            &mut self.store,
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
        // we get here `self.store` already holds them and we only
        // need to pin the team-cap pin onto the leaf pair.
        let cap_blob: Blob<SimpleArchive> = Blob::new(cap_bytes);
        let sig_blob: Blob<SimpleArchive> = Blob::new(sig_bytes);
        let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

        // Defensive sanity: the cap+sig blobs really are in the
        // store. If not, the host emitted the CapDelivered event
        // without the preceding Blob events somehow — log and bail
        // rather than pin handles that won't resolve.
        let Ok(reader) = self.store.reader() else {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: pile reader unavailable; dropping"
            );
            return;
        };
        if reader.get::<Blob<SimpleArchive>, SimpleArchive>(cap_handle).is_err()
            || reader.get::<Blob<SimpleArchive>, SimpleArchive>(sig_handle).is_err()
        {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: blobs missing from store (host should have emitted Blob events first)"
            );
            return;
        }

        match crate::policy::pin_team_cap(
            &mut self.store,
            self.team_root,
            cap_handle,
            sig_handle,
        ) {
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
    const UNDELIVERED_REDISPATCH_COOLDOWN: std::time::Duration =
        std::time::Duration::from_secs(15);

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

        let entries = crate::policy::undelivered_entries(&mut self.store);
        if entries.is_empty() {
            return 0;
        }

        let now = crate::clock::mono_now();
        let Ok(reader) = self.store.reader() else { return 0; };

        let mut dispatched = 0usize;
        for entry in entries {
            // Per-entry cooldown.
            if let Some(prev) = self.last_dispatch_attempt.get(&entry.id) {
                if now.duration_since(*prev) < Self::UNDELIVERED_REDISPATCH_COOLDOWN {
                    continue;
                }
            }

            let Ok(cap_blob) = reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_cap)
            else {
                continue;
            };
            let Ok(sig_blob) = reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_sig)
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
    ///    [`crate::channel::NetCommand::DeliverCap`], rate-limited per
    ///    entry by [`Self::UNDELIVERED_REDISPATCH_COOLDOWN`]. This is
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
        use triblespace_core::blob::{Blob, TryFromBlob};
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
        use triblespace_core::inline::{Inline, TryToInline};
        use triblespace_core::inline::encodings::hash::Handle;
        use triblespace_core::repo::BlobStoreGet;

        let redispatched = self.redispatch_undelivered();

        let entries = crate::policy::renewable_within(&mut self.store, renewal_window);
        if entries.is_empty() {
            return redispatched;
        }

        // Our own current cap is the parent for every renewal. If
        // we don't have one, we can't sign — log and bail.
        let Some((parent_cap_handle, parent_sig_handle)) =
            crate::policy::current_team_cap(&mut self.store, self.team_root)
        else {
            tracing::warn!(
                renewable = entries.len(),
                "renewal_tick: no team-cap pinned; cannot issue successors"
            );
            return 0;
        };

        let Ok(reader) = self.store.reader() else {
            tracing::warn!("renewal_tick: pile reader unavailable");
            return 0;
        };
        let Ok(parent_cap_blob) = reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(parent_cap_handle)
        else {
            tracing::warn!("renewal_tick: parent cap blob missing");
            return 0;
        };
        let Ok(parent_sig_blob) = reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(parent_sig_handle)
        else {
            tracing::warn!("renewal_tick: parent sig blob missing");
            return 0;
        };

        let mut dispatched = 0usize;
        for entry in entries {
            // Re-derive scope_facts from the previous cap blob —
            // policy entries carry only the scope_root id, not the
            // facts hanging off it.
            let Ok(prev_cap_blob) = reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_cap)
            else {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: previous cap blob missing; skipping entry"
                );
                continue;
            };
            let Ok(prev_set): Result<
                triblespace_core::trible::TribleSet,
                _,
            > = TryFromBlob::try_from_blob(prev_cap_blob) else {
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
            let _ = self
                .store
                .put::<SimpleArchive, Blob<SimpleArchive>>(new_cap);
            let _ = self
                .store
                .put::<SimpleArchive, Blob<SimpleArchive>>(new_sig);

            // Dispatch over the wire.
            self.sender.deliver_cap(entry.subject.to_bytes(), cap_bytes, sig_bytes);
            // Record the attempt so the undelivered-redispatch path
            // doesn't immediately re-fire on the same entry within
            // its cooldown window.
            self.last_dispatch_attempt
                .insert(entry.id, crate::clock::mono_now());

            // Update the policy entry so we don't re-renew on the
            // next tick.
            if crate::policy::update_policy_entry(
                &mut self.store,
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

    /// Force-republish all current non-tracking branches to the gossip
    /// topic, regardless of whether they appear changed since the last
    /// publish.
    ///
    /// Use this for periodic "I'm still here, here's my state"
    /// announcements that help newly-joined gossip neighbors learn about
    /// us. Long-running sync daemons typically call this every few seconds.
    /// Cheap to call repeatedly — iroh-gossip dedupes identical messages
    /// on the wire.
    ///
    /// Distinct from [`refresh`](Self::refresh): refresh publishes only
    /// the deltas it detects against its diff baselines. This method
    /// republishes everything unconditionally.
    pub fn republish_branches(&mut self) {
        // ReadOnly suppresses publishing entirely — even republish.
        if self.direction == SyncDirection::ReadOnly {
            return;
        }
        // Refresh the snapshot served by the network thread BEFORE
        // gossiping — see `refresh` Phase 2 for the ordering rationale.
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
        }
        let bids: Vec<Id> = match self.store.pins() {
            Ok(it) => it.filter_map(|r| r.ok()).collect(),
            Err(_) => return,
        };
        for bid in bids {
            if crate::tracking::is_tracking_pin(&mut self.store, bid) {
                continue;
            }
            if crate::policy::is_local_only_pin(&mut self.store, bid) {
                continue;
            }
            if let Ok(Some(head)) = self.store.head(bid) {
                let bid_bytes: [u8; 16] = bid.into();
                self.sender.gossip(bid_bytes, head.raw);
                self.last_branches.insert(bid, head.raw);
            }
        }
    }

    /// Borrow the underlying store. Use for store-specific methods that
    /// aren't part of the BlobStore/PinStore traits (e.g. `Pile::flush`).
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Mutably borrow the underlying store. Writes through this borrow
    /// bypass the Peer's auto-publish and become invisible to the network
    /// until the next [`refresh`](Self::refresh) (which is auto-called on
    /// the next read).
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Consume the Peer and return the underlying store. The network
    /// thread shuts down when the Peer drops. The cache tier is dropped
    /// — its contents are transient and re-fetchable by construction.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Read `hash` from local tiers only (Durable, then Cache) without
    /// touching the swarm. `Some(bytes)` on a local hit, `None` on a
    /// local miss — this is the cheap, non-blocking half of the read
    /// path, safe to call speculatively (e.g. the conservative
    /// reference scan asking "do I already hold this?"). Calls
    /// [`refresh`](Self::refresh) first so freshly-gossiped blobs count
    /// as local.
    pub fn try_local(&mut self, hash: RawHash) -> Option<Bytes> {
        let reader = self.reader().ok()?;
        reader.get::<Bytes, UnknownBlob>(Inline::new(hash)).ok()
    }

    /// Land swarm-fetched `bytes` into the Cache tier (never Durable).
    /// Caches are free and eviction is always safe, so this never needs
    /// the pin machinery. Returns the content handle. Used by
    /// [`get_or_fetch`](Self::get_or_fetch) and by deterministic-sim
    /// drivers that obtained the bytes through the non-blocking
    /// [`request_blob`](Self::request_blob).
    pub fn land_in_cache(&mut self, bytes: Bytes) -> Inline<Handle<UnknownBlob>> {
        // Cache put is `Infallible` for the in-memory tiers we ship
        // (`NullCache`, `BoundedBlobStore`); `expect` documents that.
        self.cache
            .put::<UnknownBlob, Bytes>(bytes)
            .expect("cache put is infallible (MemoryBlobStore-backed)")
    }

    /// Number of blobs currently resident in the Cache tier. Diagnostic
    /// / test hook — Durable holdings are not counted.
    pub fn cache_len(&mut self) -> usize {
        self.cache
            .reader()
            .ok()
            .map(|r| r.blobs().filter(Result::is_ok).count())
            .unwrap_or(0)
    }

    /// Honest **async** lazy read: return `hash`'s bytes, fetching from
    /// the swarm and landing them into the Cache tier on a local miss.
    ///
    /// 1. **Local** — Durable then Cache (via [`try_local`](Self::try_local)).
    ///    Hit ⇒ return immediately, no network.
    /// 2. **Swarm** — a swarm-addressed [`request_blob`](Self::request_blob)
    ///    (DHT-routed, hash-verified), `.await`ed. The fetched bytes land
    ///    in the Cache (not Durable — durability is a separate pin
    ///    decision), so the next read is a local hit until eviction.
    ///
    /// `None` is *Unavailable*: nobody reachable served it. Existence is
    /// semidecidable — there is no "definitely absent" outcome.
    ///
    /// The swarm fetch is *awaited*, never blocking the caller's thread:
    /// the reply rides a tokio oneshot, so this composes inside any async
    /// consumer and drives cleanly on a single-threaded runtime (the
    /// await yields, letting the host produce the reply). This is where
    /// the swarm fetch stops being a hidden block and becomes an honest
    /// `.await`.
    pub async fn get_or_fetch_async(&mut self, hash: RawHash) -> Option<Bytes> {
        if let Some(bytes) = self.try_local(hash) {
            return Some(bytes);
        }
        // Dropped sender (host gone) → Err → None, never a hang.
        let raw = self.request_blob(hash).await.ok().flatten()?;
        let bytes = Bytes::from(raw);
        self.land_in_cache(bytes.clone());
        Some(bytes)
    }
}


// ── Trait delegations ───────────────────────────────────────────────
//
// Reads (`reader`, `head`, `branches`) call `refresh()` first so they
// always see the latest gossiped state AND any external writes that
// landed since the last refresh get announced. Writes (`put`, `update`)
// delegate to the inner store and then push the new state out via the
// network thread, updating the diff baselines so refresh doesn't
// double-announce.

impl<S, C> BlobStorePut for Peer<S, C>
where
    S: BlobStore + BlobStorePut + PinStore,
    C: BlobStore<Reader = CacheReader> + BlobStorePut,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        let handle = self.store.put(item)?;
        // Snapshot first, then announce — see `refresh` Phase 2 for the
        // ordering rationale. Without this, DHT-receivers of the announce
        // dial us, OP_GET_BLOB hits the stale snapshot, returns missing,
        // and the receiver waits for backoff to retry.
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
        }
        if self.direction != SyncDirection::ReadOnly {
            self.sender.announce(handle.raw);
        }
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = self.store.reader().ok();
        Ok(handle)
    }
}

impl<S, C> BlobStore for Peer<S, C>
where
    S: BlobStore + BlobStorePut + PinStore,
    C: BlobStore<Reader = CacheReader> + BlobStorePut,
{
    type Reader = PeerReader<S::Reader>;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh();
        let durable = self.store.reader()?;
        // The cache reader is `MemoryBlobStore`-backed, whose
        // `ReaderError` is `Infallible` — snapshotting an in-memory
        // PATCH cannot fail. `expect` documents that invariant.
        let cache = self
            .cache
            .reader()
            .expect("cache reader is infallible (MemoryBlobStore-backed)");
        Ok(PeerReader { durable, cache })
    }
}

impl<S, C> PinStore for Peer<S, C>
where
    S: BlobStore + BlobStorePut + PinStore,
    C: BlobStore<Reader = CacheReader> + BlobStorePut,
{
    type PinsError = S::PinsError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;
    type ListIter<'a> = S::ListIter<'a> where S: 'a, C: 'a;

    fn pins<'a>(&'a mut self) -> Result<Self::ListIter<'a>, Self::PinsError> {
        self.refresh();
        self.store.pins()
    }

    fn head(
        &mut self,
        id: Id,
    ) -> Result<Option<Inline<Handle<SimpleArchive>>>, Self::HeadError> {
        self.refresh();
        self.store.head(id)
    }

    fn update(
        &mut self,
        id: Id,
        old: Option<Inline<Handle<SimpleArchive>>>,
        new: Option<Inline<Handle<SimpleArchive>>>,
    ) -> Result<PushResult, Self::UpdateError> {
        let result = self.store.update(id, old, new.clone())?;
        if let PushResult::Success() = &result {
            if let Some(head) = new {
                // Refresh the snapshot served by the network thread
                // BEFORE gossiping — see `refresh` Phase 2 for the
                // ordering rationale.
                if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
                    self.sender.update_snapshot(snap);
                }
                // Tracking branches are local mirror state and must NOT be
                // re-gossiped — otherwise the publisher would receive its
                // own tracking branch back and create a tracking-of-the-
                // tracking, ad infinitum. Same logic for policy branches
                // (renewal state, pending requests, per-team-cap pins) —
                // they're per-peer local state.
                if !crate::tracking::is_tracking_pin(&mut self.store, id)
                    && !crate::policy::is_local_only_pin(&mut self.store, id)
                    && self.direction != SyncDirection::ReadOnly
                {
                    let bid_bytes: [u8; 16] = id.into();
                    self.sender.gossip(bid_bytes, head.raw);
                    self.last_branches.insert(id, head.raw);
                }
            }
        }
        Ok(result)
    }
}

/// Read the branch name from a branch metadata blob. Tries `metadata::name`
/// first (normal branches) and falls back to `remote_name` (tracking
/// branches mirrored from a remote peer).
fn read_remote_name<S: BlobStore>(store: &mut S, head_hash: &RawHash) -> Option<String> {
    use triblespace_core::blob::encodings::longstring::LongString;
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::macros::{find, pattern};

    let reader = store.reader().ok()?;
    let meta_handle = Inline::<Handle<SimpleArchive>>::new(*head_hash);
    let meta: triblespace_core::trible::TribleSet = reader.get(meta_handle).ok()?;

    let name_handle: Inline<Handle<LongString>> = find!(
        h: Inline<Handle<LongString>>,
        pattern!(&meta, [{ _?e @ triblespace_core::metadata::name: ?h }])
    )
    .next()
    .or_else(|| {
        find!(
            h: Inline<Handle<LongString>>,
            pattern!(&meta, [{ _?e @ crate::tracking::remote_name: ?h }])
        )
        .next()
    })?;

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

/// The two-tier read view of a [`Peer`]: a Durable reader (`D`) over
/// the pinned store, unioned with the [`Cache`](crate::cache) tier's
/// `MemoryBlobStore`-backed reader.
///
/// Lookups fall through **Durable → Cache** (the order a lazy node
/// should prefer: pinned-and-promised before transient). The union is
/// *local only* — it does NOT reach the swarm. A read that wants to
/// pull missing content from the swarm uses the explicit
/// [`Peer::get_or_fetch`], keeping speculative gets (the conservative
/// reference scan, `has_blob` checks) cheap and non-blocking. This is
/// the decomplecting that makes "the layers above the blob substrate
/// can do whatever fancy dance they like" hold: enumeration and
/// existence stay local and total; only an explicit fetch is allowed
/// to block.
#[derive(Clone, PartialEq, Eq)]
pub struct PeerReader<D> {
    durable: D,
    cache: CacheReader,
}

impl<D> BlobStoreGet for PeerReader<D>
where
    D: BlobStoreGet + Clone + Send + PartialEq + Eq + 'static,
{
    // Report the Durable reader's error family. A Cache miss after a
    // Durable miss yields the Durable error (a "not found" shape) — the
    // two readers' error types don't unify, and Durable's is the
    // authoritative "this isn't here" signal.
    type GetError<E: std::error::Error + Send + Sync + 'static> = D::GetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        match self.durable.get::<T, S>(handle) {
            Ok(value) => Ok(value),
            // Durable miss → try Cache; on a Cache miss too, surface the
            // Durable error.
            Err(durable_err) => self.cache.get::<T, S>(handle).map_err(|_cache_err| durable_err),
        }
    }
}

impl<D> BlobStoreList for PeerReader<D>
where
    D: BlobStoreList + Clone + Send + PartialEq + Eq + 'static,
{
    type Iter<'a> = std::vec::IntoIter<Result<Inline<Handle<UnknownBlob>>, D::Err>> where D: 'a;
    type Err = D::Err;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        // Durable ∪ Cache. Collected eagerly so the two readers' iterator
        // types unify into one concrete `vec::IntoIter`. The cache
        // reader's list error is `Infallible`, so its `Err` arm is
        // unreachable and gets dropped (the empty-`match` proves it).
        let mut out: Vec<Result<Inline<Handle<UnknownBlob>>, D::Err>> =
            self.durable.blobs().collect();
        for entry in self.cache.blobs() {
            match entry {
                Ok(handle) => out.push(Ok(handle)),
                Err(never) => match never {},
            }
        }
        out.into_iter()
    }
}

// Conservative reference discovery works through the fall-through
// `get`: the default scan checks each 32-byte chunk against *both*
// tiers, so a blob whose children live partly in Cache still resolves
// its full local child set.
impl<D> BlobChildren for PeerReader<D> where
    D: BlobStoreGet + Clone + Send + PartialEq + Eq + 'static
{
}
