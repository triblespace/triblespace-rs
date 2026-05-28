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
use triblespace_core::blob::{BlobEncoding, IntoBlob};
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::repo::{
    BlobStore, BlobStoreList, BlobStorePut, PinStore, PushResult,
};
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;

use crate::channel::{NetEvent, PublisherKey};
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::protocol::RawHash;

pub use crate::host::{PeerConfig, SyncDirection};

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
pub struct Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    store: S,
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

    /// Wall-clock time of the most recent NetEvent absorbed in
    /// [`refresh`](Peer::refresh). Drives quiescence-based stopping
    /// in long-running sync drivers.
    last_event_at: std::time::Instant,

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
}

impl<S> Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread internally.
    ///
    /// The thread lives for the Peer's lifetime and shuts down when the
    /// Peer drops.
    pub fn new(mut store: S, key: SigningKey, config: PeerConfig) -> Self {
        let direction = config.direction;
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);

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
            sender,
            receiver,
            last_blob_reader: None,
            last_branches: HashMap::new(),
            direction,
            last_event_at: std::time::Instant::now(),
            team_root,
            signing_key,
        };

        // Drive the first refresh synchronously so the DHT learns
        // about pre-existing blobs before `Peer::new` returns and the
        // first incoming AUTH can land.
        peer.refresh();

        peer
    }

    /// Wall-clock time of the most recent network event absorbed by
    /// [`refresh`](Self::refresh). Useful for quiescence-based stopping:
    /// long-running sync drivers can poll `peer.last_event_at().elapsed()`
    /// and shut down once the swarm goes silent.
    ///
    /// Constructed-at-`Peer::new` initial value, so the first quiescence
    /// window starts at construction rather than at the first event.
    pub fn last_event_at(&self) -> std::time::Instant {
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
            self.last_event_at = std::time::Instant::now();
            if self.direction == SyncDirection::WriteOnly {
                continue;
            }
            match event {
                NetEvent::Blob(data) => {
                    let bytes: Bytes = data.into();
                    let _ = self.store.put::<UnknownBlob, Bytes>(bytes);
                }
                NetEvent::Head { branch, head, publisher } => {
                    if let Some(remote_id) = Id::new(branch) {
                        if let Some(name) = read_remote_name(&mut self.store, &head) {
                            crate::tracking::ensure_tracking_pin(
                                &mut self.store,
                                remote_id,
                                &head,
                                &name,
                                &publisher,
                            );
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
            }
        }

        // ── Phase 2: diff-and-publish blob deltas ─────────────────────
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

        // ── Phase 3: diff-and-publish branch deltas ───────────────────
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

        // ── Phase 4: refresh the snapshot served by the network thread ─
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
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
        partial_cap_bytes: Vec<u8>,
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
        let blob: Blob<SimpleArchive> =
            Blob::new(anybytes::Bytes::from_source(partial_cap_bytes));
        let Ok(partial_cap_handle) = self
            .store
            .put::<SimpleArchive, Blob<SimpleArchive>>(blob)
        else {
            tracing::warn!("CapRequest: failed to store partial cap blob");
            return;
        };

        // Point-interval at "now" — pending-requests timeline is
        // just "this arrived at T".
        let now = match hifitime::Epoch::now() {
            Ok(n) => n,
            Err(_) => {
                tracing::warn!("CapRequest: system time unavailable; dropping");
                return;
            }
        };
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
        cap_bytes: Vec<u8>,
        sig_bytes: Vec<u8>,
    ) {
        use triblespace_core::blob::Blob;
        use triblespace_core::repo::BlobStoreGet;

        // Reconstitute blobs.
        let cap_blob: Blob<SimpleArchive> =
            Blob::new(anybytes::Bytes::from_source(cap_bytes.clone()));
        let sig_blob: Blob<SimpleArchive> =
            Blob::new(anybytes::Bytes::from_source(sig_bytes.clone()));
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

        // Subject must be US — a delivered cap is for our pubkey. The
        // signing-key bytes aren't accessible here, but we know our
        // own pubkey via the iroh endpoint id baked into the snapshot
        // we serve; for v1 we just trust the verify_chain subject
        // check via expected_subject = team_root if the delivery is
        // the team-root case, or the requester pubkey for delegated
        // chains. We can't know which without re-deriving our own
        // pubkey, so for v1 we pass team_root and rely on
        // verify_chain's chain walk catching subject mismatches.
        let expected_subject = self.team_root;

        // Build a fetch closure that consults the in-process pile.
        let Ok(reader) = self.store.reader() else {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: pile reader unavailable; dropping"
            );
            return;
        };
        // The cap and sig blobs themselves aren't in the pile yet — we
        // need to short-circuit fetches for them via a map, falling
        // back to the pile for any intermediate chain caps.
        let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
        let cap_blob_for_fetch = cap_blob.clone();
        let sig_blob_for_fetch = sig_blob.clone();
        let fetch = move |h: Inline<Handle<SimpleArchive>>| -> Option<Blob<SimpleArchive>> {
            if h.raw == cap_handle.raw {
                return Some(cap_blob_for_fetch.clone());
            }
            if h.raw == sig_handle.raw {
                return Some(sig_blob_for_fetch.clone());
            }
            reader.get::<Blob<SimpleArchive>, SimpleArchive>(h).ok()
        };
        match triblespace_core::repo::capability::verify_chain(
            self.team_root,
            sig_handle,
            expected_subject,
            fetch,
        ) {
            Ok(_verified) => {
                // Persist both blobs into the local store, then pin
                // them via the per-team-cap pin. The pin is a
                // single-slot branch: each renewal overwrites the
                // head, making old cap+sig blobs unreachable so the
                // next compaction reclaims them. Forward security at
                // the storage layer falls out naturally — only the
                // current cap survives.
                let cap_bytes_anybytes: anybytes::Bytes =
                    anybytes::Bytes::from_source(cap_bytes);
                let sig_bytes_anybytes: anybytes::Bytes =
                    anybytes::Bytes::from_source(sig_bytes);
                let _ = self
                    .store
                    .put::<UnknownBlob, anybytes::Bytes>(cap_bytes_anybytes);
                let _ = self
                    .store
                    .put::<UnknownBlob, anybytes::Bytes>(sig_bytes_anybytes);
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
                            "CapDelivered: verified and pinned on team-cap pin"
                        );
                    }
                    None => {
                        tracing::warn!(
                            issuer = %hex::encode(&issuer[..4]),
                            "CapDelivered: verified but team-cap pin failed"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    issuer = %hex::encode(&issuer[..4]),
                    error = ?e,
                    "CapDelivered: verify_chain failed; dropping"
                );
            }
        }
    }

    /// Run one tick of the auto-renewal scan.
    ///
    /// Enumerates renewal-policy entries whose current cap expires
    /// within `renewal_window` of now and aren't retracted, signs a
    /// fresh cap+sig for each (using our team-cap as parent), and
    /// dispatches via `OP_DELIVER_CAP`. The policy entry is updated
    /// in lockstep so the next tick doesn't re-fire on the same entry.
    ///
    /// Returns the count of entries renewed this tick — useful for
    /// observability and tests. `0` on every tick after the swarm
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

        let entries = crate::policy::renewable_within(&mut self.store, renewal_window);
        if entries.is_empty() {
            return 0;
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
            let Ok(now) = hifitime::Epoch::now() else { continue };
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
            // at these handles; the dispatch ships the bytes.
            let cap_bytes = new_cap.bytes.to_vec();
            let sig_bytes = new_sig.bytes.to_vec();
            let _ = self
                .store
                .put::<SimpleArchive, Blob<SimpleArchive>>(new_cap);
            let _ = self
                .store
                .put::<SimpleArchive, Blob<SimpleArchive>>(new_sig);

            // Dispatch over the wire.
            self.sender.deliver_cap(entry.subject.to_bytes(), cap_bytes, sig_bytes);

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
        dispatched
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
        // Refresh the snapshot served by the network thread.
        if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
            self.sender.update_snapshot(snap);
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
    /// thread shuts down when the Peer drops.
    pub fn into_store(self) -> S {
        self.store
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

impl<S> BlobStorePut for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    type PutError = S::PutError;

    fn put<Sch, T>(&mut self, item: T) -> Result<Inline<Handle<Sch>>, Self::PutError>
    where
        Sch: BlobEncoding + 'static,
        T: IntoBlob<Sch>,
        Handle<Sch>: InlineEncoding,
    {
        let handle = self.store.put(item)?;
        if self.direction != SyncDirection::ReadOnly {
            self.sender.announce(handle.raw);
        }
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = self.store.reader().ok();
        Ok(handle)
    }
}

impl<S> BlobStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    type Reader = S::Reader;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.refresh();
        self.store.reader()
    }
}

impl<S> PinStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    type PinsError = S::PinsError;
    type HeadError = S::HeadError;
    type UpdateError = S::UpdateError;
    type ListIter<'a> = S::ListIter<'a> where S: 'a;

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
                // Refresh the snapshot served by the network thread.
                if let Some(snap) = StoreSnapshot::from_store(&mut self.store) {
                    self.sender.update_snapshot(snap);
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
