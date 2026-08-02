//! `Peer<S>`: a store wrapped in distributed network sync.
//!
//! Owns the inner store, spawns the iroh network thread on construction,
//! and exposes the standard storage traits (`BlobStore + BlobStorePut +
//! PinStore + PinAssertionStore`) with content-addressed transport behavior
//! built in:
//!
//! - **Reads** auto-call [`refresh`](Peer::refresh), which drains pending
//!   capability control events and publishes blobs appended by external
//!   writers (e.g. another process writing the same pile). Mirrors
//!   `Pile::refresh` — the explicit method is available for tight loops.
//!   Persistence failures are sticky and fail-stop: automatic trait refreshes
//!   cannot change their associated error types, but a later explicit
//!   [`refresh`](Peer::refresh) reports the retained [`PeerRefreshError`].
//! - **Writes** delegate to the inner store. Blobs are announced to the DHT.
//!   Signed asserted pins are forwarded only to local storage and are not
//!   replicated by this layer.
//!
//! There is no separate cache tier: `Peer<S>` takes a **single store**,
//! and any tiering (bounded retention, generational eviction) lives in `S`
//! — e.g. a [`Yard`](triblespace_core::repo::yard::Yard). Before a read-miss
//! starts a swarm fetch, the peer signs and durably appends a typed want
//! assertion under its own identity. A failed fetch therefore leaves durable
//! demand for the reconciler. Append itself is the durability boundary; no
//! separate flush is needed. If append fails, the read errors out through
//! [`PeerReaderGetError::WantRecord`] (or [`Peer::get_or_fetch_async`]'s
//! `Err`) without fetching. Wants are a grow-only demand ledger, not retention
//! policy: obtaining a blob makes its want inert, while `S` remains responsible
//! for deciding how fetched bytes are retained or evicted.
//!
//! There is currently no signed-assertion wire protocol.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use iroh_base::EndpointId;
use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::branch_frontier::{ParentLookup, PartialCommitDag};
use triblespace_core::repo::pin_assertion::{
    PinAssertion, PinAssertionSnapshot, PinAssertionStore,
};
use triblespace_core::repo::want::{
    WantCachePolicy, WantCachePolicySource, WantStore, selected_wants_for_author_in_snapshot,
    sign_want, wants_in_snapshot,
};
use triblespace_core::repo::{
    BlobChildren, BlobStore, BlobStoreGet, BlobStoreList, BlobStorePut, PinStore, PushResult,
    StorageFlush,
};

use crate::channel::{NetEvent, PublisherKey};
use crate::host::{self, NetReceiver, NetSender, StoreSnapshot};
use crate::policy_ledger::GrantIdentity;
use crate::protocol::RawHash;

pub use crate::host::PeerConfig;

/// A fail-stop persistence error observed while applying network events.
///
/// Once a peer records this error, every later [`Peer::refresh`] returns the
/// same value without consuming more events. Callers may close or repair the
/// wrapped store and restart the peer; continuing after an unknown partial
/// write would make the storage invariant unprovable.
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
/// use triblespace_net::peer::{Peer, PeerConfig};
///
/// let key = SigningKey::generate(&mut OsRng);
/// let pile: Pile = Pile::open(Path::new("./team.pile")).unwrap();
/// let peer = Peer::new(pile, key.clone(), PeerConfig {
///     peers: vec![],                       // bootstrap nodes
///     team_root: key.verifying_key(),      // single-user fallback
///     self_cap: [0u8; 32],
/// });
/// // From here `peer` forwards the wrapped store's blob, local-pin,
/// // durability, and asserted-pin capabilities — wrap it in
/// // `Repository::new` and use it like any other storage.
/// drop(peer);
/// ```
pub struct Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
{
    /// The wrapped store, shared behind a mutex: a `&self` async read on
    /// a [`PeerReader`] must be able to append a want and land a
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

    /// Per-grant cooldown for unauthenticated credential re-dispatch. The
    /// renewal daemon's tick runs every 100 ms; without this gate it
    /// would hammer iroh-connect attempts for any peer that's down.
    /// Each stable grant retains only its latest dispatched signature and
    /// attempt time, so a successor resets cooldown without accumulating
    /// stale signature keys. The map is in-memory and rebuilds naturally if
    /// the daemon restarts.
    last_dispatch_attempt:
        HashMap<GrantIdentity, (Inline<Handle<SimpleArchive>>, crate::clock::Mono)>,
}

impl<S> Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread
    /// internally; the thread lives for the Peer's lifetime and shuts
    /// down when the Peer drops.
    pub fn new(mut store: S, key: SigningKey, mut config: PeerConfig) -> Self {
        config.self_cap = startup_self_cap(&mut store, &key, &config);
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);
        Self::assemble(store, sender, receiver, team_root, signing_key)
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
        team_root: ed25519_dalek::VerifyingKey,
        sender: host::NetSender,
        receiver: host::NetReceiver,
    ) -> Self {
        Self::assemble(store, sender, receiver, team_root, signing_key)
    }

    fn assemble(
        mut store: S,
        sender: host::NetSender,
        receiver: host::NetReceiver,
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
    /// assert-want-then-fetch-then-put composition). Used in
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
    /// Three phases:
    ///
    /// 1. **Drain incoming events** — applies pending capability control
    ///    events from the network thread.
    /// 2. **Refresh the serving snapshot** used by authenticated blob reads.
    /// 3. **Publish external writes** — diffs the wrapped store against
    ///    the last published baseline and announces any blob deltas
    ///    that didn't go through the Peer's own write path. Use this to
    ///    catch writes from another process that touched the pile file.
    ///
    /// Auto-called inside the BlobStore/PinStore read methods, so
    /// callers using the storage normally don't need to invoke it.
    /// Mirrors `Pile::refresh` — the explicit method is available for
    /// "do it now" semantics or tight loops with no read activity.
    ///
    /// Network ingestion is fail-stop. A persistence failure is remembered
    /// permanently for this `Peer`, favoring explicit restart/repair over
    /// continuing from an unknown partial append.
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
        // A process may have stopped after durably entering Activating, or
        // after installing/flushing the team-cap but before exact journal
        // cleanup. Recover before accepting another delivery event so the
        // retained transaction lock has one deterministic outcome.
        {
            let mut store = self.store.lock().expect("store mutex");
            if let Some(self_cap) = recover_outbound_cap_activation(
                &mut *store,
                self.team_root,
                self.signing_key.verifying_key(),
            )
            .map_err(|error| PeerRefreshError::new("recover capability activation", error))?
            {
                self.sender.update_self_cap(self_cap);
            }
        }

        // ── Phase 1: drain incoming events ────────────────────────────
        while let Some(event) = self.receiver.try_recv() {
            self.last_event_at = crate::clock::mono_now();
            match event {
                NetEvent::CapRequest {
                    requester,
                    partial_cap_bytes,
                    admission: _admission,
                    completion,
                } => {
                    let result = self.absorb_cap_request(requester, partial_cap_bytes);
                    match result {
                        Ok(accepted) => {
                            // Only a known semantic outcome crosses this
                            // channel: true is the durable positive receipt,
                            // false is an explicit policy refusal.
                            let _ = completion.send(accepted);
                        }
                        Err(error) => {
                            // PinAssertionStore cannot promise that Err means
                            // no append took effect. Dropping the sender makes
                            // the host return STATUS_INDETERMINATE, preserving
                            // the requester's exact replayable intent.
                            drop(completion);
                            return Err(error);
                        }
                    }
                }
                NetEvent::CapDelivered {
                    issuer,
                    cap_bytes,
                    sig_bytes,
                    proof_blobs,
                    authority_expires_at,
                    admission: _admission,
                } => {
                    // Verify the delivered chain against our configured
                    // team root, then store both blobs locally. Pinning
                    // them into a per-team-cap pin (so compaction
                    // retains them) comes with the CLI subcommands —
                    // for now they're orphan blobs in the pile, same
                    // as our own outgoing-cap blobs.
                    self.absorb_cap_delivery(
                        issuer,
                        cap_bytes,
                        sig_bytes,
                        proof_blobs,
                        authority_expires_at,
                    )?;
                }
                NetEvent::CapDeliveryConfirmed {
                    subject,
                    sig_handle,
                    admission: _admission,
                } => {
                    let subject_key = match ed25519_dalek::VerifyingKey::from_bytes(&subject) {
                        Ok(k) => k,
                        Err(_) => continue,
                    };
                    let sig_inline: Inline<Handle<SimpleArchive>> = Inline::new(sig_handle);
                    let mut store = self.store.lock().expect("store mutex");
                    let snapshot = store.pin_assertion_snapshot().map_err(|error| {
                        PeerRefreshError::new("snapshot delivery-confirmation policy", error)
                    })?;
                    let reader = store.reader().map_err(|error| {
                        PeerRefreshError::new("read delivery-confirmation policy", error)
                    })?;
                    let resolution = crate::policy_ledger::resolve_policy_ledger(
                        &snapshot,
                        self.signing_key.verifying_key(),
                        |handle| {
                            reader
                                .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                                .ok()
                        },
                    );
                    drop(reader);

                    let view = match resolution {
                        crate::policy_ledger::PolicyLedgerResolution::Complete(view) => view,
                        crate::policy_ledger::PolicyLedgerResolution::Incomplete { missing } => {
                            return Err(PeerRefreshError::new(
                                "resolve delivery-confirmation policy",
                                format!(
                                    "policy ledger is incomplete; missing {} blob(s): {missing:?}",
                                    missing.len()
                                ),
                            ));
                        }
                        crate::policy_ledger::PolicyLedgerResolution::Invalid { diagnostics } => {
                            return Err(PeerRefreshError::new(
                                "resolve delivery-confirmation policy",
                                format!("policy ledger is invalid: {diagnostics:?}"),
                            ));
                        }
                    };

                    // Authentication recording is evidence about the exact
                    // historical issuance, even after a grant is disabled, so
                    // this path deliberately inspects `historical_issuance`.
                    // Dispatch paths must instead use `usable_at(now)` so a
                    // disabled or expired grant can never drive a send.
                    let grant = {
                        let mut matches =
                            view.grants().iter().filter_map(|(grant, state)| {
                                if grant.subject() != subject_key {
                                    return None;
                                }
                                match state.historical_issuance() {
                                    crate::policy_ledger::GrantIssuanceResolution::Current(
                                        current,
                                    ) if current.sig() == sig_inline => Some(*grant),
                                    crate::policy_ledger::GrantIssuanceResolution::Unissued
                                    | crate::policy_ledger::GrantIssuanceResolution::Current(_)
                                    | crate::policy_ledger::GrantIssuanceResolution::Conflicted {
                                        ..
                                    } => None,
                                }
                            });
                        match (matches.next(), matches.next()) {
                            (Some(grant), None) => Some(grant),
                            (None, _) | (Some(_), Some(_)) => None,
                        }
                    };
                    let Some(grant) = grant else {
                        continue;
                    };

                    let receipt = crate::policy_ledger::authenticate_credential(
                        &mut *store,
                        &self.signing_key,
                        grant,
                        sig_inline,
                    )
                    .map_err(|error| {
                        PeerRefreshError::new("record credential authentication", error)
                    })?;
                    tracing::debug!(
                        subject = %hex::encode(&subject[..4]),
                        sig = %hex::encode(&sig_handle[..4]),
                        event = %hex::encode(receipt.event().raw),
                        grant = ?grant,
                        "confirmed credential authentication asserted"
                    );
                }
            }
        }

        let mut store = self.store.lock().expect("store mutex");

        // ── Phase 2: refresh the snapshot served by the network thread ─
        //
        // MUST happen before any announce below: peers may dial immediately
        // after DHT discovery, and the network thread serves from this
        // snapshot.
        if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
            self.sender.update_snapshot(snap);
        } else {
            self.sender.clear_snapshot();
        }

        // ── Phase 3: diff-and-publish blob deltas ─────────────────────
        // On the first refresh the baseline is `None`, so we announce every
        // blob currently in the store —
        // covers the initial pile contents without a separate startup
        // sweep (and without the race that two separate `reader()`
        // calls introduced).
        let current = store
            .reader()
            .map_err(|error| PeerRefreshError::new("snapshot local blobs", error))?;
        match self.last_blob_reader.as_ref() {
            Some(baseline) => {
                for handle in current.blobs_diff(baseline) {
                    let handle =
                        handle.map_err(|error| PeerRefreshError::new("diff local blobs", error))?;
                    self.sender.announce(handle.raw);
                }
            }
            None => {
                use triblespace_core::repo::BlobStoreList;
                for handle in current.blobs() {
                    let handle = handle
                        .map_err(|error| PeerRefreshError::new("enumerate local blobs", error))?;
                    self.sender.announce(handle.raw);
                }
            }
        }
        self.last_blob_reader = Some(current);

        Ok(())
    }

    /// Validate and durably observe an incoming join request in this
    /// issuer's asserted policy ledger.
    ///
    /// Ordinary input/admission refusals return `Ok(false)`. Storage failures
    /// propagate so [`refresh`](Self::refresh) can fail-stop. `Ok(true)` is
    /// returned only after the request closure was flushed and its assertion
    /// durably appended, which is the receipt the host maps to `STATUS_OK`.
    fn absorb_cap_request(
        &mut self,
        requester: PublisherKey,
        partial_cap_bytes: anybytes::Bytes,
    ) -> Result<bool, PeerRefreshError> {
        use triblespace_core::blob::Blob;
        use triblespace_core::blob::encodings::simplearchive::SimpleArchive;

        // Reconstitute the requester pubkey from bytes. If the bytes
        // aren't a valid ed25519 pubkey, drop on the floor — only
        // iroh-verified peers reach this code path, so this is
        // defensive only.
        let Ok(requester_pubkey) = ed25519_dalek::VerifyingKey::from_bytes(&requester) else {
            tracing::warn!(
                requester = %hex::encode(&requester[..4]),
                "CapRequest: bad requester pubkey; dropping"
            );
            return Ok(false);
        };

        let blob: Blob<SimpleArchive> = Blob::new(partial_cap_bytes);
        let mut store = self.store.lock().expect("store mutex");

        match crate::policy_ledger::observe_request(
            &mut *store,
            &self.signing_key,
            requester_pubkey,
            blob,
        ) {
            Ok(crate::policy_ledger::ObserveRequestOutcome::Observed(receipt)) => {
                tracing::info!(
                    requester = %hex::encode(&requester[..4]),
                    request_event = %hex::encode(receipt.event().raw),
                    "CapRequest durably observed in policy ledger"
                );
                Ok(true)
            }
            Ok(crate::policy_ledger::ObserveRequestOutcome::Refused(reason)) => {
                tracing::warn!(
                    requester = %hex::encode(&requester[..4]),
                    reason = %reason,
                    "CapRequest: policy refused the request"
                );
                Ok(false)
            }
            Err(error) => Err(PeerRefreshError::new("observe capability request", error)),
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
        proof_blobs: Vec<anybytes::Bytes>,
        authority_expires_at: hifitime::Epoch,
    ) -> Result<(), PeerRefreshError> {
        use triblespace_core::blob::Blob;

        // Verification + exact fetch of any missing chain blobs
        // already happened in the host thread's HandshakeHandler
        // (the OP_DELIVER_CAP path doesn't ack STATUS_OK until the
        // chain verifies under our pubkey). The complete bounded bundle is
        // carried by this single control event, so WriteOnly filtering cannot
        // discard its proof members.
        let cap_blob: Blob<SimpleArchive> = Blob::new(cap_bytes);
        let sig_blob: Blob<SimpleArchive> = Blob::new(sig_bytes);
        let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();

        let mut store = self.store.lock().expect("store mutex");
        let Some(selection) = select_cap_delivery(
            &mut *store,
            self.team_root,
            cap_blob.clone(),
            authority_expires_at,
        ) else {
            tracing::warn!(
                issuer = %hex::encode(&issuer[..4]),
                "CapDelivered: valid chain was not selected by local request/renewal policy"
            );
            return Ok(());
        };
        for bytes in proof_blobs {
            store
                .put::<SimpleArchive, Blob<SimpleArchive>>(Blob::new(bytes))
                .map_err(|error| PeerRefreshError::new("persist delivered proof", error))?;
        }
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(cap_blob)
            .map_err(|error| PeerRefreshError::new("persist delivered capability", error))?;
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(sig_blob)
            .map_err(|error| PeerRefreshError::new("persist delivered signature", error))?;

        let credential = crate::policy::TeamCredential {
            cap: cap_handle,
            sig: sig_handle,
            founder_anchor_sig: selection.founder_anchor_sig,
        };

        if let Some(pending) = selection.initial_request {
            // First delivery crosses two independent CAS pins. Lock the exact
            // Pending request to this candidate, then make that journal (and
            // every proof blob it retains) durable before team activation.
            let activating = match crate::policy::begin_outbound_cap_activation_if_pending(
                &mut *store,
                pending,
                credential,
            ) {
                Some(crate::policy::OutboundRequestCasResult::Success(state)) => state,
                Some(crate::policy::OutboundRequestCasResult::Conflict) => {
                    tracing::info!(
                        issuer = %hex::encode(&issuer[..4]),
                        "CapDelivered: request intent changed during selection; dropping stale first delivery"
                    );
                    return Ok(());
                }
                None => {
                    return Err(PeerRefreshError::new(
                        "begin capability activation",
                        "outbound activation journal update failed",
                    ));
                }
            };
            debug_assert_eq!(activating.activation.map(|a| a.candidate), Some(credential));
            store.flush().map_err(|error| {
                PeerRefreshError::new("flush capability activation journal", error)
            })?;

            let recovered = recover_outbound_cap_activation(
                &mut *store,
                self.team_root,
                self.signing_key.verifying_key(),
            )
            .map_err(|error| PeerRefreshError::new("finish capability activation", error))?;
            if let Some(self_cap) = recovered {
                self.sender.update_self_cap(self_cap);
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    sig = %hex::encode(&self_cap[..4]),
                    "CapDelivered: first credential activated through durable journal"
                );
            } else {
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    "CapDelivered: candidate expired during activation; request restored"
                );
            }
            return Ok(());
        }

        match crate::policy::pin_team_credential_if_head(
            &mut *store,
            self.team_root,
            selection.expected_team_head,
            credential,
        ) {
            Some(crate::policy::TeamCredentialPinResult::Success(_pin_id)) => {
                store.flush().map_err(|error| {
                    PeerRefreshError::new("flush delivered capability activation", error)
                })?;
                // The pin is now the durable source of truth. Only after that
                // succeeds may future outbound dials begin presenting this
                // signature handle; the host command also evicts predecessor-
                // authenticated pooled connections.
                self.sender.update_self_cap(sig_handle.raw);
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    sig = %hex::encode(&sig_handle.raw[..4]),
                    "CapDelivered: pinned on team-cap pin"
                );
                Ok(())
            }
            Some(crate::policy::TeamCredentialPinResult::Conflict) => {
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    "CapDelivered: active credential changed during selection; dropping stale activation"
                );
                Ok(())
            }
            None => Err(PeerRefreshError::new(
                "pin delivered capability",
                format!(
                    "team-cap pin update failed for issuer {}",
                    hex::encode(issuer)
                ),
            )),
        }
    }

    /// Cooldown for re-dispatching unauthenticated asserted credentials. The
    /// daemon's tick cadence is sub-second; without this gate we'd hammer
    /// iroh-connect against a down peer 10× per second.
    const UNAUTHENTICATED_REDISPATCH_COOLDOWN: std::time::Duration =
        std::time::Duration::from_secs(15);

    /// Re-dispatch each current asserted credential for this Peer's configured
    /// team that has not yet been authenticated by its subject, rate-limited
    /// per exact grant/signature through `last_dispatch_attempt`. The
    /// credential is not re-signed: the selected current cap and signature
    /// blobs are sent byte-for-byte, so the receiver's content-addressed
    /// persistence remains idempotent.
    ///
    /// Only a complete locally-authored policy ledger may drive network work.
    /// Snapshot, reader, incomplete-ledger, invalid-ledger, flush, and serving-
    /// snapshot failures all defer the whole pass without dispatching.
    ///
    /// Returns the count of entries dispatched this tick.
    fn redispatch_unauthenticated(&mut self) -> usize {
        let mut store = self.store.lock().expect("store mutex");
        let assertion_snapshot = match store.pin_assertion_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "redispatch_unauthenticated: policy assertion snapshot unavailable; deferring"
                );
                return 0;
            }
        };
        let reader = match store.reader() {
            Ok(reader) => reader,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "redispatch_unauthenticated: policy blob reader unavailable; deferring"
                );
                return 0;
            }
        };
        let resolution = crate::policy_ledger::resolve_policy_ledger(
            &assertion_snapshot,
            self.signing_key.verifying_key(),
            |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            },
        );
        let view = match resolution {
            crate::policy_ledger::PolicyLedgerResolution::Complete(view) => view,
            crate::policy_ledger::PolicyLedgerResolution::Incomplete { missing } => {
                tracing::warn!(
                    missing = missing.len(),
                    handles = ?missing,
                    "redispatch_unauthenticated: asserted policy ledger incomplete; deferring"
                );
                return 0;
            }
            crate::policy_ledger::PolicyLedgerResolution::Invalid { diagnostics } => {
                tracing::warn!(
                    diagnostics = ?diagnostics,
                    "redispatch_unauthenticated: asserted policy ledger invalid; deferring"
                );
                return 0;
            }
        };
        let now = crate::clock::mono_now();
        let epoch_now = crate::clock::epoch_now();
        let local_subject = self.signing_key.verifying_key();
        let entries: Vec<_> = view
            .grants()
            .iter()
            // A signing identity may author policy for multiple teams. This
            // Peer instance serves exactly its configured team root.
            .filter(|(grant, _)| grant.team_root() == self.team_root)
            // Local credential rotation is a direct, durable founder action,
            // never an OP_DELIVER_CAP round-trip to ourselves.
            .filter(|(grant, _)| grant.subject() != local_subject)
            .filter_map(|(grant, state)| {
                let current = state.usable_at(epoch_now)?;
                (!current.authenticated()).then_some((*grant, current.cap(), current.sig()))
            })
            .filter(|(grant, _cap, sig)| {
                self.last_dispatch_attempt.get(grant).is_none_or(
                    |(previous_sig, previous_attempt)| {
                        previous_sig != sig
                            || now.duration_since(*previous_attempt)
                                >= Self::UNAUTHENTICATED_REDISPATCH_COOLDOWN
                    },
                )
            })
            .collect();
        if entries.is_empty() {
            return 0;
        }

        // Resolve exact bytes for the whole action set before any outward
        // effect. Although Complete proved every closure member readable once,
        // a backend may fail a subsequent read; that must defer the whole pass
        // rather than partially dispatch a prefix of grants.
        let mut ready = Vec::with_capacity(entries.len());
        for (grant, cap, sig) in entries {
            let cap_blob = match reader.get::<Blob<SimpleArchive>, SimpleArchive>(cap) {
                Ok(blob) => blob,
                Err(error) => {
                    tracing::warn!(
                        grant = ?grant,
                        cap = ?cap,
                        error = %error,
                        "redispatch_unauthenticated: selected cap blob unreadable; deferring"
                    );
                    return 0;
                }
            };
            let sig_blob = match reader.get::<Blob<SimpleArchive>, SimpleArchive>(sig) {
                Ok(blob) => blob,
                Err(error) => {
                    tracing::warn!(
                        grant = ?grant,
                        sig = ?sig,
                        error = %error,
                        "redispatch_unauthenticated: selected signature blob unreadable; deferring"
                    );
                    return 0;
                }
            };
            ready.push((grant, sig, cap_blob, sig_blob));
        }
        drop(reader);

        // Re-establish both barriers on every redispatch attempt; otherwise a
        // prior interrupted publication could advertise proof handles this
        // process has not made durably and coherently servable.
        if let Err(error) = store.flush() {
            tracing::warn!(
                pending = ready.len(),
                error = %error,
                "redispatch_unauthenticated: durable flush failed; deferring"
            );
            return 0;
        }
        let Some(snapshot) = StoreSnapshot::from_store(&mut *store) else {
            self.sender.clear_snapshot();
            tracing::warn!(
                pending = ready.len(),
                "redispatch_unauthenticated: serving snapshot failed; deferring"
            );
            return 0;
        };
        self.sender.update_snapshot(snapshot);

        let mut dispatched = 0usize;
        for (grant, sig, cap_blob, sig_blob) in ready {
            self.sender.deliver_cap(
                grant.subject().to_bytes(),
                cap_blob.bytes.clone(),
                sig_blob.bytes.clone(),
            );
            self.last_dispatch_attempt.insert(grant, (sig, now));
            dispatched += 1;
            tracing::debug!(
                subject = %hex::encode(grant.subject().to_bytes()),
                grant = ?grant,
                sig = ?sig,
                "redispatch_unauthenticated: re-sent OP_DELIVER_CAP"
            );
        }
        dispatched
    }

    /// Run one tick of the auto-renewal scan.
    ///
    /// Performs two pieces of work each tick:
    ///
    /// 1. **Redispatch unauthenticated asserted grants.** Resolve our complete
    ///    policy ledger and re-send each active current `(cap, sig)` that has
    ///    not acquired its exact `CredentialAuthenticated` fact, rate-limited
    ///    per stable grant and current signature. This catches an initial
    ///    delivery that missed an offline subject.
    ///
    /// 2. **Re-sign near-expiry entries.** For each entry whose current
    ///    cap upper bound falls within `renewal_window` of now, sign a
    ///    fresh cap+sig (using our team-cap as parent) and dispatch.
    ///    The legacy renewal policy entry is updated in lockstep. Its asserted
    ///    issuance migration is intentionally separate from this redispatch
    ///    consumer cut.
    ///
    /// Returns the total count of dispatches this tick (unauthenticated
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
        use triblespace_core::inline::{Inline, TryFromInline, TryToInline};
        use triblespace_core::repo::BlobStoreGet;

        let redispatched = self.redispatch_unauthenticated();

        let mut store = self.store.lock().expect("store mutex");
        let credential_state =
            crate::policy::current_team_credential_state(&mut *store, self.team_root);
        let founder_self_ready = credential_state.is_some_and(|state| {
            reconcile_founder_self_policy(
                &mut *store,
                &self.signing_key,
                self.team_root,
                &self.sender,
                state,
            )
        });
        // Re-read due state only after founder policy reconciliation. If a
        // crash left the policy naming the predecessor, this prevents an
        // unnecessary second sibling from being minted merely because the
        // stale expiry was still inside the renewal window.
        let mut entries = crate::policy::renewable_within(&mut *store, renewal_window);
        if entries.is_empty() {
            return redispatched;
        }

        let Some(credential_state) = credential_state else {
            tracing::warn!(
                renewable = entries.len(),
                "renewal_tick: no team-cap pinned; cannot issue successors"
            );
            return redispatched;
        };
        let mut credential = credential_state.credential;

        // The founder's own finite credential is a sibling of its
        // predecessor, not a child. Rotate it first from the retained anchor
        // so the refreshed authority can parent every other due issuance in
        // this same tick. Ordinary members have no anchor and cannot extend
        // their own authority.
        let local_subject = self.signing_key.verifying_key();
        let mut local_rotations = 0usize;
        let active_self = founder_self_ready.then(|| {
            entries.iter().position(|entry| {
                entry.subject == local_subject
                    && entry.latest_cap == credential.cap
                    && entry.latest_sig == credential.sig
            })
        });
        if let Some(index) = active_self.flatten() {
            let self_entry = entries.remove(index);
            if rotate_founder_self(
                &mut *store,
                &self.signing_key,
                self.team_root,
                &self.sender,
                &self_entry,
                credential_state,
                renewal_window,
            ) {
                local_rotations = 1;
                if let Some(current) =
                    crate::policy::current_team_credential_state(&mut *store, self.team_root)
                {
                    credential = current.credential;
                }
            }
        }
        let unrelated_local_entries = entries
            .iter()
            .filter(|entry| entry.subject == local_subject)
            .count();
        if unrelated_local_entries != 0 {
            tracing::warn!(
                entries = unrelated_local_entries,
                "renewal_tick: ignoring local-subject policy entries that do not name the active credential"
            );
            entries.retain(|entry| entry.subject != local_subject);
        }
        if entries.is_empty() {
            return redispatched + local_rotations;
        }

        // Our current finite operational cap parents every non-self renewal.
        let parent_cap_handle = credential.cap;
        let parent_sig_handle = credential.sig;

        let Ok(reader) = store.reader() else {
            tracing::warn!("renewal_tick: pile reader unavailable");
            return redispatched + local_rotations;
        };
        let Ok(parent_cap_blob) =
            reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_cap_handle)
        else {
            tracing::warn!("renewal_tick: parent cap blob missing");
            return redispatched + local_rotations;
        };
        let Ok(parent_sig_blob) =
            reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_sig_handle)
        else {
            tracing::warn!("renewal_tick: parent sig blob missing");
            return redispatched + local_rotations;
        };

        let parent_verified = match triblespace_core::repo::capability::verify_chain(
            self.team_root,
            parent_sig_handle,
            local_subject,
            |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            },
        ) {
            Ok(verified) => verified,
            Err(error) => {
                tracing::warn!(error = ?error, "renewal_tick: active parent credential is not live and valid");
                return redispatched + local_rotations;
            }
        };
        let parent_effective_upper = parent_verified.expires_at();

        let mut ready_to_dispatch = Vec::new();
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
            let desired_upper = now + renewal_window * 2;
            let new_upper = if parent_effective_upper < desired_upper {
                parent_effective_upper
            } else {
                desired_upper
            };
            let old_effective_upper =
                <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&entry.effective_expiry)
                    .ok()
                    .map(|(_lower, upper)| upper);
            if old_effective_upper.is_some_and(|old_upper| new_upper <= old_upper) {
                // Reissuing below the same parent cannot extend effective
                // authority. Wait for the parent's own rotation instead of
                // churning content-addressed siblings every tick.
                continue;
            }
            let Ok(new_expiry) = (now, new_upper).try_to_inline() else {
                continue;
            };

            // Sign.
            let (new_cap, new_sig) = match triblespace_core::repo::capability::build_capability(
                &self.signing_key,
                entry.subject,
                (parent_cap_blob.clone(), parent_sig_blob.clone()),
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

            if triblespace_core::repo::capability::verify_chain(
                self.team_root,
                new_sig_handle,
                entry.subject,
                |handle| {
                    if handle == new_sig_handle {
                        Some(new_sig.clone())
                    } else if handle == new_cap_handle {
                        Some(new_cap.clone())
                    } else {
                        reader
                            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                            .ok()
                    }
                },
            )
            .is_err()
            {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: constructed successor failed verification; skipping"
                );
                continue;
            }

            // Persist locally — the next tick's policy update points
            // at these handles; the dispatch ships the bytes. Both
            // sites share the same refcounted `anybytes::Bytes`
            // backing the freshly-signed blob (clones are refcount
            // bumps, no byte-copy).
            let cap_bytes = new_cap.bytes.clone();
            let sig_bytes = new_sig.bytes.clone();
            if store
                .put::<SimpleArchive, Blob<SimpleArchive>>(new_cap)
                .is_err()
                || store
                    .put::<SimpleArchive, Blob<SimpleArchive>>(new_sig)
                    .is_err()
            {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: failed to persist fresh capability; skipping"
                );
                continue;
            }

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
                let grant = GrantIdentity::new(self.team_root, entry.subject, entry.scope);
                ready_to_dispatch.push((
                    entry.id,
                    grant,
                    new_sig_handle,
                    entry.subject,
                    cap_bytes,
                    sig_bytes,
                ));
            } else {
                tracing::warn!(
                    entry = ?entry.id,
                    "renewal_tick: re-issued but policy update failed; will retry"
                );
            }
        }

        // Publish one coherent serving view containing every fresh leaf pair
        // before any recipient can ask this issuer for a missing parent proof.
        // The daemon calls refresh before renewal_tick, so dispatching from the
        // loop above would otherwise expose a predictably stale snapshot.
        let serving_ready = if ready_to_dispatch.is_empty() {
            true
        } else if let Err(error) = store.flush() {
            tracing::warn!(
                pending = ready_to_dispatch.len(),
                error = %error,
                "renewal_tick: durable flush failed; deferring dispatch"
            );
            false
        } else if let Some(snapshot) = StoreSnapshot::from_store(&mut *store) {
            self.sender.update_snapshot(snapshot);
            true
        } else {
            self.sender.clear_snapshot();
            tracing::warn!(
                pending = ready_to_dispatch.len(),
                "renewal_tick: fresh capabilities persisted but serving snapshot failed; deferring dispatch"
            );
            false
        };
        drop(store);

        let mut dispatched = 0usize;
        if serving_ready {
            for (entry_id, grant, sig, subject, cap_bytes, sig_bytes) in ready_to_dispatch {
                self.sender
                    .deliver_cap(subject.to_bytes(), cap_bytes, sig_bytes);
                self.last_dispatch_attempt
                    .insert(grant, (sig, crate::clock::mono_now()));
                dispatched += 1;
                tracing::info!(
                    subject = %hex::encode(subject.to_bytes()),
                    entry = ?entry_id,
                    "renewal_tick: re-issued and dispatched"
                );
            }
        }
        dispatched + redispatched + local_rotations
    }

    /// Lock and borrow the underlying store. Use for store-specific
    /// methods that aren't part of the storage traits (e.g.
    /// `Pile::flush`, `Yard::collect`).
    ///
    /// Writes through this borrow bypass blob announcement until the next
    /// [`refresh`](Self::refresh) (which is auto-called on the next read).
    /// Don't hold the guard across calls back into the Peer — its own methods
    /// take the same lock.
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
    /// first so externally appended blobs count as local.
    pub fn try_local(&mut self, hash: RawHash) -> Option<Bytes> {
        let reader = self.reader().ok()?;
        reader.get::<Bytes, UnknownBlob>(Inline::new(hash)).ok()
    }

    /// Honest **async** lazy read: return `hash`'s bytes, fetching from
    /// the swarm and landing them in the store on a local miss.
    ///
    /// 1. **Local** — one lookup in the store
    ///    (via [`try_local`](Self::try_local)). Hit ⇒ return
    ///    immediately, no network, no pin.
    /// 2. **Miss** — a typed want for `hash` is signed by this peer and
    ///    durably appended FIRST. The assertion survives an immediate
    ///    process exit and is the sync daemon's work queue. Only then is
    ///    the swarm-addressed fetch awaited (DHT-routed, hash-verified)
    ///    and the verified bytes `put` into the store. If the fetch fails,
    ///    the assertion remains an outstanding want; if it succeeds, local
    ///    presence makes the grow-only want inert.
    ///
    /// `Ok(None)` is *Unavailable*: nobody reachable served it before
    /// the budget expired. Existence is semidecidable — there is no
    /// "definitely absent" outcome — and the want stays on record.
    ///
    /// `Err` means the assertion could not be durably appended. No fetch is
    /// attempted in that case: proceeding would
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
    ) -> Result<Option<Bytes>, <S as PinAssertionStore>::Error> {
        if let Some(bytes) = self.try_local(hash) {
            return Ok(Some(bytes));
        }
        // Append the want durably BEFORE the fetch — a failed fetch must leave
        // demand on record, and a failed append must be an error, never a
        // silent proceed.
        self.assert_want(Inline::<Handle<UnknownBlob>>::new(hash))?;
        let Some(raw) = self.fetch_blob(hash).await else {
            return Ok(None);
        };
        let bytes = Bytes::from(raw);
        {
            let mut store = self.store.lock().expect("store mutex");
            if let Err(e) = store.put::<UnknownBlob, Bytes>(bytes.clone()) {
                // Landing failed but the verified bytes are in hand and
                // the want is on record — a later reconcile pass re-lands
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

/// Verify one stored operational credential and bind the cap named by its
/// signature to the cap handle retained by local policy state. Expired chains
/// remain distinguishable from corrupt chains for startup/recovery only.
fn verify_stored_credential<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    expected_subject: ed25519_dalek::VerifyingKey,
    credential: crate::policy::TeamCredential,
) -> Result<triblespace_core::repo::capability::VerifiedCapability, String>
where
    S: BlobStore,
{
    use triblespace_core::macros::{find, pattern};
    use triblespace_core::repo::capability::verify_chain_allow_expired;

    let reader = store
        .reader()
        .map_err(|error| format!("read credential store: {error}"))?;
    let sig_set: triblespace_core::trible::TribleSet = reader
        .get(credential.sig)
        .map_err(|error| format!("load configured signature: {error}"))?;
    let mut signed = find!(
        (entity: Id, cap: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{
            ?entity @ triblespace_core::repo::capability::sig_signs: ?cap,
        }])
    );
    let cap_handle = match (signed.next(), signed.next()) {
        (Some((_entity, cap)), None) => cap,
        _ => return Err("signature blob does not name exactly one leaf capability".into()),
    };
    if cap_handle != credential.cap {
        return Err("team-cap state names a cap different from its signature".into());
    }
    verify_chain_allow_expired(team_root, credential.sig, expected_subject, |handle| {
        reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
            .ok()
    })
    .map_err(|error| format!("credential verification failed: {error:?}"))
}

/// Re-validate the semantic relation which allowed a Pending request to enter
/// Activating. The journal is durable local authority, but it is not trusted
/// merely because it parses: recovery repeats both proof verification and the
/// request/candidate attenuation check before touching the team-cap pin.
fn verify_journaled_first_delivery<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    expected_subject: ed25519_dalek::VerifyingKey,
    state: crate::policy::OutboundRequestState,
) -> Result<
    (
        crate::policy::TeamCredential,
        triblespace_core::repo::capability::VerifiedCapability,
    ),
    String,
>
where
    S: BlobStore,
{
    use triblespace_core::repo::capability::scope_subsumes;

    let activation = state
        .activation
        .ok_or_else(|| "outbound request is not Activating".to_string())?;
    let candidate = activation.candidate;
    let verified = verify_stored_credential(store, team_root, expected_subject, candidate)?;
    let reader = store
        .reader()
        .map_err(|error| format!("read activation journal: {error}"))?;
    let requested_blob: Blob<SimpleArchive> = reader
        .get(state.partial_cap)
        .map_err(|error| format!("load activation request: {error}"))?;
    let candidate_blob: Blob<SimpleArchive> = reader
        .get(candidate.cap)
        .map_err(|error| format!("load activation candidate: {error}"))?;
    let requested = delivery_cap_fields(requested_blob)
        .ok_or_else(|| "activation request is malformed".to_string())?;
    let delivered = delivery_cap_fields(candidate_blob)
        .ok_or_else(|| "activation candidate is malformed".to_string())?;
    if requested.subject != expected_subject
        || delivered.subject != requested.subject
        || delivered.issuer != requested.issuer
        || verified.expires_at()
            > expiry_upper(&requested.expiry)
                .ok_or_else(|| "activation request expiry is malformed".to_string())?
        || !scope_subsumes(
            &requested.set,
            requested.scope_root,
            &delivered.set,
            delivered.scope_root,
        )
    {
        return Err("activation candidate does not match retained request intent".into());
    }
    Ok((candidate, verified))
}

/// Finish or reconcile an interrupted first-delivery transaction.
///
/// The only forward order is durable Activating journal -> team-cap CAS ->
/// durable team-cap -> exact journal clear. A live candidate with no winner is
/// resumed. An expired candidate with no team-cap winner restores the exact
/// retained Pending head. A different valid team-cap winner is never
/// overwritten; it wins and the stale journal is merely cleared.
fn recover_outbound_cap_activation<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    expected_subject: ed25519_dalek::VerifyingKey,
) -> Result<Option<RawHash>, String>
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush,
{
    let Some(journal) = crate::policy::current_outbound_cap_request_state(store) else {
        return Ok(None);
    };
    let Some(activation) = journal.activation else {
        return Ok(None);
    };
    let (candidate, candidate_verified) =
        verify_journaled_first_delivery(store, team_root, expected_subject, journal)?;

    let mut installed = crate::policy::current_team_credential_state(store, team_root);
    if installed.is_none() {
        if crate::policy::find_team_cap_pin(store, team_root).is_some() {
            return Err("team-cap pin is malformed while recovering activation".into());
        }
        if candidate_verified.is_expired() {
            let restored =
                crate::policy::restore_outbound_cap_request_pending_if_state(store, journal)
                    .ok_or_else(|| "restore expired activation to Pending failed".to_string())?;
            if !restored
                && crate::policy::current_outbound_cap_request_state(store) == Some(journal)
            {
                return Err("restore expired activation lost its head CAS".into());
            }
            store
                .flush()
                .map_err(|error| format!("flush restored Pending request: {error}"))?;
            return Ok(None);
        }

        match crate::policy::pin_team_credential_if_head(store, team_root, None, candidate) {
            Some(crate::policy::TeamCredentialPinResult::Success(_)) => {}
            Some(crate::policy::TeamCredentialPinResult::Conflict) => {}
            None => return Err("team-cap activation write failed".into()),
        }
        installed = crate::policy::current_team_credential_state(store, team_root);
    }

    let installed = installed
        .ok_or_else(|| "team-cap activation CAS conflicted with unreadable state".to_string())?;
    // This verifies both an exactly installed candidate and a different
    // concurrent winner. The latter is authoritative; recovery must not
    // overwrite it merely because the journal was created first.
    let installed_verified =
        verify_stored_credential(store, team_root, expected_subject, installed.credential)?;

    // Whether installed by this call or immediately before a crash, the
    // active credential must reach stable storage before intent is removed.
    store
        .flush()
        .map_err(|error| format!("flush recovered team-cap activation: {error}"))?;
    let cleared = crate::policy::clear_outbound_cap_request_if_state(store, journal)
        .ok_or_else(|| "clear recovered activation journal failed".to_string())?;
    if !cleared && crate::policy::current_outbound_cap_request_state(store) == Some(journal) {
        return Err("clear recovered activation journal lost its head CAS".into());
    }
    store
        .flush()
        .map_err(|error| format!("flush activation journal clear: {error}"))?;

    let raw = if installed_verified.is_expired() {
        [0; 32]
    } else {
        installed.credential.sig.raw
    };
    if installed.credential != activation.candidate {
        tracing::info!(
            sig = %hex::encode(&installed.credential.sig.raw[..4]),
            "first-delivery recovery preserved a concurrent team-cap winner"
        );
    }
    Ok(Some(raw))
}

/// Resolve the outbound credential before any network task can dial. The
/// durable team-cap pin is authoritative over process configuration. A legacy
/// nonzero configured handle is accepted only once: after complete local
/// verification it is promoted to that pin and flushed before host startup.
///
/// This method deliberately fails loudly for missing, corrupt, or
/// wrong-subject state. An otherwise-valid expired *durable pin* is the one
/// exception: it starts in recovery-only mode so retained founder authority
/// can rotate a fresh finite credential without first authenticating.
fn startup_self_cap<S>(store: &mut S, key: &SigningKey, config: &PeerConfig) -> RawHash
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush,
{
    let expected_subject = key.verifying_key();
    recover_outbound_cap_activation(store, config.team_root, expected_subject)
        .unwrap_or_else(|error| panic!("outbound activation recovery failed: {error}"));

    let resolve_pinned = |store: &mut S, state: crate::policy::TeamCredentialState| {
        let pinned = state.credential;
        let verified = verify_stored_credential(store, config.team_root, expected_subject, pinned)
            .unwrap_or_else(|error| panic!("invalid pinned outbound credential: {error}"));
        // An otherwise-valid expired pin is retained as recovery authority:
        // the peer starts inbound-only and founder renewal can replace it
        // locally from the durable anchor. Corrupt, unauthorized, or
        // incomplete pinned state still fails loudly above.
        if verified.is_expired() {
            [0; 32]
        } else {
            pinned.sig.raw
        }
    };

    if let Some(state) = crate::policy::current_team_credential_state(store, config.team_root) {
        return resolve_pinned(store, state);
    }
    if crate::policy::find_team_cap_pin(store, config.team_root).is_some() {
        panic!("durable team-cap pin has malformed or unreadable state");
    }

    if config.self_cap == [0; 32] {
        // Explicit server-only sentinel: inbound serving remains available,
        // while every attempted outbound AUTH predictably fails.
        return config.self_cap;
    }

    let configured_sig = Inline::<Handle<SimpleArchive>>::new(config.self_cap);
    let reader = store
        .reader()
        .unwrap_or_else(|error| panic!("read configured credential: {error}"));
    let sig_set: triblespace_core::trible::TribleSet = reader
        .get(configured_sig)
        .unwrap_or_else(|error| panic!("load configured signature: {error}"));
    use triblespace_core::macros::{find, pattern};
    let mut signed = find!(
        (entity: Id, cap: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{
            ?entity @ triblespace_core::repo::capability::sig_signs: ?cap,
        }])
    );
    let configured_cap = match (signed.next(), signed.next()) {
        (Some((_entity, cap)), None) => cap,
        _ => panic!("configured signature does not name exactly one leaf capability"),
    };
    drop(reader);
    let configured_credential = crate::policy::TeamCredential {
        cap: configured_cap,
        sig: configured_sig,
        founder_anchor_sig: None,
    };
    let configured_verified = verify_stored_credential(
        store,
        config.team_root,
        expected_subject,
        configured_credential,
    )
    .unwrap_or_else(|error| panic!("invalid configured outbound credential: {error}"));
    if configured_verified.is_expired() {
        panic!("invalid configured outbound credential: credential is expired");
    }
    match crate::policy::pin_team_credential_if_head(
        store,
        config.team_root,
        None,
        configured_credential,
    ) {
        Some(crate::policy::TeamCredentialPinResult::Success(_)) => {}
        Some(crate::policy::TeamCredentialPinResult::Conflict) => {
            let state = crate::policy::current_team_credential_state(store, config.team_root)
                .unwrap_or_else(|| {
                    panic!("concurrently installed team-cap pin is malformed or unreadable")
                });
            return resolve_pinned(store, state);
        }
        None => panic!("failed to promote configured outbound credential to team-cap pin"),
    }
    store.flush().unwrap_or_else(|error| {
        panic!("failed to durably flush promoted outbound credential: {error}")
    });
    configured_sig.raw
}

// ── Trait delegations ───────────────────────────────────────────────
//
// Reads (`reader`, `head`, `pins`) call `refresh()` first so they
// always see external writes and control events observed by the last refresh.
// Writes (`put`, `update`)
// delegate to the inner store and then push the new state out via the
// network thread, updating the diff baselines so refresh doesn't
// double-announce.

impl<S> BlobStorePut for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
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
        } else {
            self.sender.clear_snapshot();
        }
        self.sender.announce(handle.raw);
        // Update the blob baseline so refresh doesn't double-announce.
        self.last_blob_reader = store.reader().ok();
        Ok(handle)
    }
}

impl<S> BlobStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
{
    type Reader = PeerReader<S::Reader>;
    type ReaderError = S::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        let _ = self.refresh();
        let local = self.store.lock().expect("store mutex").reader()?;
        // The fetch capability: a clone of the command sender plus a
        // landing handle into the *shared* store, so a `&self` async
        // read can pull a missing blob from the swarm, record an
        // authored want, and land the bytes.
        let fetch = Some(FetchCap {
            sender: self.sender.clone(),
            sink: Arc::new(SharedStore {
                store: self.store.clone(),
                signing_key: self.signing_key.clone(),
            }),
        });
        Ok(PeerReader { local, fetch })
    }
}

impl<S> StorageFlush for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
{
    type Error = <S as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.store.lock().expect("store mutex").flush()
    }
}

impl<S> WantCachePolicySource for Peer<S>
where
    S: BlobStore
        + BlobStorePut
        + PinStore
        + PinAssertionStore
        + StorageFlush
        + WantCachePolicySource
        + Send
        + 'static,
{
    fn want_cache_policy(&self) -> WantCachePolicy {
        self.store.lock().expect("store mutex").want_cache_policy()
    }
}

/// Generic asserted pins are authority state rather than blob payloads. The
/// peer therefore forwards their coherent snapshots and durable appends to its
/// local store while leaving replication to a future assertion wire protocol.
impl<S> PinAssertionStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush + PinAssertionStore + Send + 'static,
{
    type Error = <S as PinAssertionStore>::Error;

    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
        let _ = self.refresh();
        self.store
            .lock()
            .expect("store mutex")
            .pin_assertion_snapshot()
    }

    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
        self.store
            .lock()
            .expect("store mutex")
            .append_pin_assertion(assertion)
    }
}

impl<S> Peer<S>
where
    S: BlobStore
        + BlobStorePut
        + PinStore
        + StorageFlush
        + PinAssertionStore
        + WantCachePolicySource
        + Send
        + 'static,
{
    /// Project the exact authored want count and the subset this artifact can
    /// stably materialise.
    ///
    /// Policy and assertions come from the same store lock. Selection first
    /// takes the canonical global prefix across every author and only then
    /// intersects it with this peer's author. The returned set is owned, so no
    /// storage lock crosses a network await in the reconciler.
    pub(crate) fn selected_authored_wants(
        &mut self,
    ) -> Result<(usize, BTreeSet<Inline<Handle<UnknownBlob>>>), <S as PinAssertionStore>::Error>
    {
        let _ = self.refresh();
        let author = self.signing_key.verifying_key();
        let (policy, snapshot) = {
            let mut store = self.store.lock().expect("store mutex");
            let policy = store.want_cache_policy();
            let snapshot = store.pin_assertion_snapshot()?;
            (policy, snapshot)
        };
        let authored = wants_in_snapshot(&snapshot, author);
        let selected = selected_wants_for_author_in_snapshot(&snapshot, author, policy);
        Ok((authored.len(), selected))
    }
}

/// Author-scoped typed wants for this peer.
///
/// The signing key never leaves the peer. Both mutation and enumeration are
/// bound to that key, so assertions authored by another peer remain preserved
/// in the generic store without becoming fetch work for this peer.
impl<S> WantStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush + PinAssertionStore + Send + 'static,
{
    fn assert_want<Sch>(
        &mut self,
        handle: Inline<Handle<Sch>>,
    ) -> Result<(), <Self as PinAssertionStore>::Error>
    where
        Sch: BlobEncoding + 'static,
        Handle<Sch>: InlineEncoding,
    {
        let assertion = sign_want(&self.signing_key, handle);
        self.append_pin_assertion(assertion)
    }

    fn wants(
        &mut self,
    ) -> Result<BTreeSet<Inline<Handle<UnknownBlob>>>, <Self as PinAssertionStore>::Error> {
        let author = self.signing_key.verifying_key();
        self.pin_assertion_snapshot()
            .map(|snapshot| wants_in_snapshot(&snapshot, author))
    }
}

impl<S> PinStore for Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
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
        let result = store.update(id, old, new)?;
        if let PushResult::Success() = &result {
            // Refresh the snapshot served by the network thread after every
            // successful pin mutation, including deletion. Otherwise a
            // branch-scoped requester could keep reading through stale roots.
            if let Some(snap) = StoreSnapshot::from_store(&mut *store) {
                self.sender.update_snapshot(snap);
            } else {
                self.sender.clear_snapshot();
            }
        }
        Ok(result)
    }
}

struct DeliveryCapFields {
    set: triblespace_core::trible::TribleSet,
    subject: ed25519_dalek::VerifyingKey,
    issuer: ed25519_dalek::VerifyingKey,
    scope_root: Id,
    expiry: Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
}

fn delivery_cap_fields(blob: Blob<SimpleArchive>) -> Option<DeliveryCapFields> {
    use triblespace_core::blob::TryFromBlob;
    use triblespace_core::inline::encodings::time::NsTAIInterval;
    use triblespace_core::macros::{find, pattern};

    let set: triblespace_core::trible::TribleSet = TryFromBlob::try_from_blob(blob).ok()?;
    let mut fields = find!(
        (
            cap: Id,
            subject: ed25519_dalek::VerifyingKey,
            issuer: ed25519_dalek::VerifyingKey,
            scope_root: Id,
            expiry: Inline<NsTAIInterval>,
        ),
        pattern!(&set, [{ ?cap @
            triblespace_core::repo::capability::cap_subject: ?subject,
            triblespace_core::repo::capability::cap_issuer: ?issuer,
            triblespace_core::repo::capability::cap_scope_root: ?scope_root,
            triblespace_core::metadata::expires_at: ?expiry,
        }])
    );
    let (_cap, subject, issuer, scope_root, expiry) = match (fields.next(), fields.next()) {
        (Some(fields), None) => fields,
        _ => return None,
    };
    Some(DeliveryCapFields {
        set,
        subject,
        issuer,
        scope_root,
        expiry,
    })
}

fn expiry_upper(
    expiry: &Inline<triblespace_core::inline::encodings::time::NsTAIInterval>,
) -> Option<hifitime::Epoch> {
    use triblespace_core::inline::TryFromInline;
    <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(expiry)
        .ok()
        .map(|(_, upper)| upper)
}

fn signature_leaf_cap_handle(
    sig_blob: Blob<SimpleArchive>,
) -> Option<Inline<Handle<SimpleArchive>>> {
    use triblespace_core::macros::{find, pattern};

    let sig_set: triblespace_core::trible::TribleSet = TryFromBlob::try_from_blob(sig_blob).ok()?;
    let mut signed = find!(
        (entity: Id, cap: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{
            ?entity @ triblespace_core::repo::capability::sig_signs: ?cap,
        }])
    );
    match (signed.next(), signed.next()) {
        (Some((_entity, cap)), None) => Some(cap),
        _ => None,
    }
}

/// Reconcile the founder's active credential with its logical self-renewal
/// policy before deciding what is due.
///
/// The credential and renewal policy intentionally live on separate CAS pins,
/// so rotation cannot update them atomically. The stable association is the
/// policy's own key, `(subject, scope)`, not its incidental entity id. A crash
/// after credential activation is therefore repaired by rewriting the unique
/// non-retracted matching policy to the already-verified active handles. A
/// missing entry is recreated (covering the analogous team-creation crash), a
/// retracted or ambiguous match fails closed, and unrelated narrower self
/// entries are never considered.
fn reconcile_founder_self_policy<S>(
    store: &mut S,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    sender: &NetSender,
    credential_state: crate::policy::TeamCredentialState,
) -> bool
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush,
{
    let credential = credential_state.credential;
    if credential.founder_anchor_sig.is_none() {
        return false;
    }
    let local_subject = signing_key.verifying_key();
    let verified = match verify_stored_credential(store, team_root, local_subject, credential) {
        Ok(verified) => verified,
        Err(error) => {
            tracing::warn!(error = %error, "founder policy reconciliation: active credential is invalid");
            return false;
        }
    };
    let fields = {
        let Ok(reader) = store.reader() else {
            return false;
        };
        let Ok(cap_blob) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(credential.cap) else {
            return false;
        };
        let Some(fields) = delivery_cap_fields(cap_blob) else {
            return false;
        };
        fields
    };
    if fields.subject != local_subject
        || fields.issuer != local_subject
        || fields.scope_root != verified.scope_root
    {
        tracing::warn!(
            "founder policy reconciliation: active self credential has inconsistent identity or scope"
        );
        return false;
    }
    // Policy scheduling must follow the verified chain lifetime, not merely
    // the leaf's declared interval. A self-delegated leaf may outlive an
    // operational parent even though its effective authority cannot.
    use triblespace_core::inline::{TryFromInline, TryToInline};
    let Ok((leaf_lower, _leaf_upper)) =
        <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&fields.expiry)
    else {
        return false;
    };
    let Ok(effective_expiry) = (leaf_lower, verified.expires_at()).try_to_inline() else {
        tracing::warn!(
            "founder policy reconciliation: verified effective lifetime is not representable"
        );
        return false;
    };

    let policies = crate::policy::list_renewal_policy(store);
    let mut dirty = false;
    let mut active_matches = policies.iter().filter(|entry| {
        entry.subject == local_subject
            && entry.scope == verified.scope_root
            && entry.retracted_at.is_none()
    });
    let first = active_matches.next();
    if active_matches.next().is_some() {
        tracing::warn!(
            "founder policy reconciliation: multiple active self policies share the credential scope"
        );
        return false;
    }

    let entry_id = match first {
        Some(entry) => {
            let needs_rewrite = entry.latest_cap != credential.cap
                || entry.latest_sig != credential.sig
                || entry.effective_expiry != effective_expiry;
            if needs_rewrite
                && crate::policy::update_policy_entry(
                    store,
                    entry.id,
                    effective_expiry,
                    credential.cap,
                    credential.sig,
                )
                .is_none()
            {
                tracing::warn!(entry = ?entry.id, "founder policy reconciliation: policy CAS lost");
                return false;
            }
            dirty |= needs_rewrite;
            entry.id
        }
        None => {
            if policies.iter().any(|entry| {
                entry.subject == local_subject
                    && entry.scope == verified.scope_root
                    && entry.retracted_at.is_some()
            }) {
                tracing::warn!(
                    "founder policy reconciliation: matching self policy is retracted; refusing to revive it"
                );
                return false;
            }
            let Some(entry_id) = crate::policy::record_policy_entry(
                store,
                local_subject,
                verified.scope_root,
                effective_expiry,
                credential.cap,
                credential.sig,
            ) else {
                tracing::warn!(
                    "founder policy reconciliation: missing self policy could not be recreated"
                );
                return false;
            };
            dirty = true;
            entry_id
        }
    };

    // For the founder's local entry, `delivered_at` is also the durable
    // publication journal for the live host credential. Rewriting a policy
    // clears it. We therefore flush the undelivered state first, publish a
    // serving snapshot and UpdateSelfCap in that order, and only then mark the
    // entry delivered. A crash or snapshot failure before publication leaves
    // an exact retry signal; a crash after publication merely repeats an
    // idempotent update on restart (whose host also starts from the team pin).
    // As with the rest of Peer policy execution, this assumes one live sync
    // daemon/host owns a pile; the marker is durable policy state, not a
    // per-process lease.
    let delivered = crate::policy::list_renewal_policy(store)
        .into_iter()
        .find(|entry| entry.id == entry_id)
        .and_then(|entry| entry.delivered_at)
        .is_some();
    if dirty || !delivered {
        if let Err(error) = store.flush() {
            tracing::warn!(entry = ?entry_id, error = %error, "founder policy reconciliation: durable flush failed");
            return false;
        }
    }
    if delivered {
        return true;
    }
    let Some(snapshot) = StoreSnapshot::from_store(store) else {
        sender.clear_snapshot();
        tracing::warn!(entry = ?entry_id, "founder policy reconciliation: serving snapshot failed");
        return false;
    };
    sender.update_snapshot(snapshot);
    sender.update_self_cap(credential.sig.raw);
    if crate::policy::mark_policy_delivered(store, entry_id).is_none() {
        tracing::warn!(entry = ?entry_id, "founder policy reconciliation: local delivery marker CAS lost");
        return false;
    }
    if let Err(error) = store.flush() {
        tracing::warn!(entry = ?entry_id, error = %error, "founder policy reconciliation: durable flush failed");
        // The live host already presents the durable credential. If this
        // marker did not reach stable storage, restart recovery safely repeats
        // publication from the authoritative team pin.
    }
    true
}

/// Rotate the founder's finite self credential as a sibling directly below
/// the durable non-expiring anchor. This is deliberately local: persistence,
/// pin replacement, serving-snapshot publication, and outbound AUTH update
/// happen without an OP_DELIVER_CAP round trip to ourselves.
fn rotate_founder_self<S>(
    store: &mut S,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    sender: &NetSender,
    entry: &crate::policy::PolicyEntry,
    credential_state: crate::policy::TeamCredentialState,
    renewal_window: hifitime::Duration,
) -> bool
where
    S: BlobStore + BlobStorePut + PinStore + StorageFlush,
{
    use triblespace_core::inline::TryToInline;
    use triblespace_core::repo::capability::verify_chain;

    let credential = credential_state.credential;
    let Some(anchor_sig_handle) = credential.founder_anchor_sig else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(anchor_sig_blob) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(anchor_sig_handle)
    else {
        tracing::warn!("founder renewal: anchor signature blob missing");
        return false;
    };
    let Some(anchor_cap_handle) = signature_leaf_cap_handle(anchor_sig_blob.clone()) else {
        tracing::warn!("founder renewal: anchor signature has malformed leaf shape");
        return false;
    };
    let Ok(anchor_cap_blob) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(anchor_cap_handle)
    else {
        tracing::warn!("founder renewal: anchor capability blob missing");
        return false;
    };
    let Ok(previous_cap) = reader.get::<Blob<SimpleArchive>, SimpleArchive>(entry.latest_cap)
    else {
        tracing::warn!(entry = ?entry.id, "founder renewal: previous operational cap missing");
        return false;
    };
    let Ok(previous_set): Result<triblespace_core::trible::TribleSet, _> =
        TryFromBlob::try_from_blob(previous_cap)
    else {
        return false;
    };
    let scope_facts = extract_scope_subgraph(&previous_set, entry.scope);
    let now = crate::clock::epoch_now();
    let new_upper = now + renewal_window * 2;
    let Ok(new_expiry) = (now, new_upper).try_to_inline() else {
        return false;
    };
    let Ok((new_cap, new_sig)) = triblespace_core::repo::capability::build_capability(
        signing_key,
        signing_key.verifying_key(),
        (anchor_cap_blob, anchor_sig_blob),
        entry.scope,
        scope_facts,
        new_expiry,
    ) else {
        return false;
    };
    let new_cap_handle: Inline<Handle<SimpleArchive>> = new_cap.get_handle();
    let new_sig_handle: Inline<Handle<SimpleArchive>> = new_sig.get_handle();

    // Validate the newly constructed chain before it can become durable state;
    // this simultaneously proves the retained anchor belongs to `team_root`
    // and authorizes this founder key and scope.
    if verify_chain(
        team_root,
        new_sig_handle,
        signing_key.verifying_key(),
        |handle| {
            if handle == new_sig_handle {
                Some(new_sig.clone())
            } else if handle == new_cap_handle {
                Some(new_cap.clone())
            } else {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            }
        },
    )
    .is_err()
    {
        tracing::warn!(entry = ?entry.id, "founder renewal: constructed sibling failed verification");
        return false;
    }
    drop(reader);

    if store
        .put::<SimpleArchive, Blob<SimpleArchive>>(new_cap)
        .is_err()
        || store
            .put::<SimpleArchive, Blob<SimpleArchive>>(new_sig)
            .is_err()
        || !matches!(
            crate::policy::pin_team_credential_if_head(
                store,
                team_root,
                Some(credential_state.head),
                crate::policy::TeamCredential {
                    cap: new_cap_handle,
                    sig: new_sig_handle,
                    founder_anchor_sig: Some(anchor_sig_handle),
                },
            ),
            Some(crate::policy::TeamCredentialPinResult::Success(_))
        )
    {
        tracing::warn!(entry = ?entry.id, "founder renewal: persistence or credential pin failed");
        return false;
    }
    if let Err(error) = store.flush() {
        tracing::warn!(entry = ?entry.id, error = %error, "founder renewal: durable credential flush failed");
        return false;
    }
    let policy_updated = crate::policy::update_policy_entry(
        store,
        entry.id,
        new_expiry,
        new_cap_handle,
        new_sig_handle,
    )
    .is_some();
    if policy_updated {
        if let Err(error) = store.flush() {
            tracing::warn!(entry = ?entry.id, error = %error, "founder renewal: policy flush failed");
            return false;
        }
    } else {
        tracing::warn!(entry = ?entry.id, "founder renewal: policy update failed; due entry will retry");
        return false;
    }

    // The cleared local-delivery marker above is the recovery journal for
    // this publication step. Never let the host present a credential whose
    // proof blobs are absent from its coherent serving snapshot.
    let Some(snapshot) = StoreSnapshot::from_store(store) else {
        sender.clear_snapshot();
        tracing::warn!(entry = ?entry.id, "founder renewal: serving snapshot failed");
        return false;
    };
    sender.update_snapshot(snapshot);
    sender.update_self_cap(new_sig_handle.raw);
    if crate::policy::mark_policy_delivered(store, entry.id).is_none() {
        tracing::warn!(entry = ?entry.id, "founder renewal: local delivery marker CAS lost");
    } else if let Err(error) = store.flush() {
        // Publication already succeeded. An unpersisted marker only causes a
        // harmless replay after restart, whose host starts from this pin.
        tracing::warn!(entry = ?entry.id, error = %error, "founder renewal: local delivery marker flush failed");
    }
    tracing::info!(entry = ?entry.id, sig = %hex::encode(&new_sig_handle.raw[..4]), "founder operational credential rotated locally");
    true
}

#[derive(Clone, Copy)]
struct DeliverySelection {
    expected_team_head: Option<Inline<Handle<SimpleArchive>>>,
    founder_anchor_sig: Option<Inline<Handle<SimpleArchive>>>,
    initial_request: Option<crate::policy::OutboundRequestState>,
}

/// Chain validity proves that an issuer *may describe* this capability; it
/// does not let arbitrary valid members select our active credential. First
/// delivery must match local request intent. Thereafter activation is
/// monotone in issuer, scope, and expiry, so delayed or attenuated candidates
/// cannot replace a stronger current cap.
fn select_cap_delivery<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    candidate_blob: Blob<SimpleArchive>,
    candidate_authority_expires_at: hifitime::Epoch,
) -> Option<DeliverySelection>
where
    S: BlobStore + PinStore,
{
    use triblespace_core::repo::BlobStoreGet;
    use triblespace_core::repo::capability::{VerifyError, scope_subsumes, verify_chain};

    let candidate = delivery_cap_fields(candidate_blob)?;
    let now = crate::clock::epoch_now();
    if candidate_authority_expires_at < now
        || candidate_authority_expires_at > expiry_upper(&candidate.expiry)?
    {
        // Events can wait behind synchronous store work. Verification at the
        // host boundary is not permission to activate an authority that died
        // while queued (and a claimed effective deadline may never exceed the
        // leaf's own deadline).
        return None;
    }
    if let Some(current_state) = crate::policy::current_team_credential_state(store, team_root) {
        let current_cap = current_state.credential.cap;
        let current_sig = current_state.credential.sig;
        let reader = store.reader().ok()?;
        let current_blob: Blob<SimpleArchive> = reader.get(current_cap).ok()?;
        let current = delivery_cap_fields(current_blob)?;
        if candidate.subject != current.subject
            || candidate.issuer != current.issuer
            || !scope_subsumes(
                &candidate.set,
                candidate.scope_root,
                &current.set,
                current.scope_root,
            )
        {
            return None;
        }
        match verify_chain(team_root, current_sig, current.subject, |handle| {
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                .ok()
        }) {
            Ok(current_verified)
                if candidate_authority_expires_at < current_verified.expires_at() =>
            {
                return None;
            }
            Ok(_) => {}
            // An expired predecessor may be recovered by a live candidate
            // with the same issuer and a non-weaker scope. Every other proof
            // failure is ambiguous/corrupt local state and fails closed.
            Err(VerifyError::Expired) => {}
            Err(_) => return None,
        }
        return Some(DeliverySelection {
            expected_team_head: Some(current_state.head),
            founder_anchor_sig: current_state.credential.founder_anchor_sig,
            initial_request: None,
        });
    }

    let requested_state = crate::policy::current_outbound_cap_request_state(store)?;
    if requested_state.activation.is_some() {
        // One exact candidate owns the first-delivery transaction until it is
        // committed or recovery restores the retained Pending head.
        return None;
    }
    let requested_handle = requested_state.partial_cap;
    let reader = store.reader().ok()?;
    let requested_blob: Blob<SimpleArchive> = reader.get(requested_handle).ok()?;
    let requested = delivery_cap_fields(requested_blob)?;
    if candidate.subject != requested.subject
        || candidate.issuer != requested.issuer
        || candidate_authority_expires_at > expiry_upper(&requested.expiry)?
        || !scope_subsumes(
            &requested.set,
            requested.scope_root,
            &candidate.set,
            candidate.scope_root,
        )
    {
        return None;
    }
    Some(DeliverySelection {
        expected_team_head: None,
        founder_anchor_sig: None,
        initial_request: Some(requested_state),
    })
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
///   lookup, else a peer-authored asserted want followed by an awaited
///   swarm fetch that lands the result in the shared store. This is what
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
/// async read: append the peer-authored want, then land fetched bytes.
/// Erases the concrete store type `S` so `PeerReader` need not carry it
/// — which is also why `record_want`'s error is boxed.
trait StoreSink: Send + Sync {
    /// Sign and durably append the want before fetching, so a failed fetch —
    /// or an immediate process exit — leaves outstanding demand on record.
    /// A failed append is an error the read must surface, never a
    /// warn-and-continue.
    fn record_want(&self, hash: RawHash) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    /// Land fetched `bytes` as an `UnknownBlob` into the store.
    fn land(&self, bytes: Bytes);
}

/// `StoreSink` over the Peer's shared store handle.
struct SharedStore<S> {
    store: Arc<Mutex<S>>,
    signing_key: SigningKey,
}

impl<S> StoreSink for SharedStore<S>
where
    S: BlobStorePut + PinAssertionStore + Send + 'static,
{
    fn record_want(&self, hash: RawHash) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let assertion = sign_want(&self.signing_key, Inline::<Handle<UnknownBlob>>::new(hash));
        self.store
            .lock()
            .expect("store mutex")
            .append_pin_assertion(assertion)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
    }

    fn land(&self, bytes: Bytes) {
        if let Ok(mut store) = self.store.lock() {
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
    /// "not obtained", never "definitely absent". The peer-authored want
    /// appended before the fetch stays on record.
    Unavailable,
    /// Local miss and the peer-authored want could not be durably appended.
    /// No fetch was attempted — the
    /// want-on-record invariant must hold before any bytes move.
    /// Boxed because the reader's store type is erased behind the fetch
    /// capability.
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
// which — post-fetch — also holds any lazily-landed blobs.
impl<L> BlobChildren for PeerReader<L> where L: BlobStoreGet {}

/// Transparent async read: local lookup → a peer-authored asserted want + an
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
                // Append the peer-authored want durably FIRST, then fetch. A
                // failed fetch leaves the assertion outstanding. A failed
                // append is an error: never fetch bytes whose
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use triblespace_core::blob::Blob;
    use triblespace_core::id::{ExclusiveId, Id, genid};
    use triblespace_core::inline::{TryFromInline, TryToInline};
    use triblespace_core::macros::entity;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::trible::TribleSet;

    fn partial_cap_bytes(
        subject: ed25519_dalek::VerifyingKey,
        issuer: ed25519_dalek::VerifyingKey,
    ) -> Bytes {
        let cap_id = genid();
        let scope_root = genid();
        let now = crate::clock::epoch_now();
        let expiry = (now, now + hifitime::Duration::from_days(1.0))
            .try_to_inline()
            .expect("request expiry interval");
        let mut cap: TribleSet = entity! {
            ExclusiveId::force_ref(&cap_id) @
            triblespace_core::repo::capability::cap_subject: subject,
            triblespace_core::repo::capability::cap_issuer: issuer,
            triblespace_core::repo::capability::cap_scope_root: *scope_root,
            triblespace_core::metadata::expires_at: expiry,
        }
        .into();
        cap += TribleSet::from(entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_READ,
        });
        let blob: Blob<SimpleArchive> = cap.to_blob();
        blob.bytes
    }

    fn cap_request_admission() -> tokio::sync::OwnedSemaphorePermit {
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("one request slot")
    }

    fn cap_delivery_admission() -> tokio::sync::OwnedSemaphorePermit {
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("one delivery slot")
    }

    #[derive(Debug, thiserror::Error)]
    #[error("injected flush failure")]
    struct InjectedFlushError;

    #[derive(Debug, thiserror::Error)]
    enum InjectedAssertionError {
        #[error("injected assertion append failure")]
        Injected,
        #[error(transparent)]
        Inner(#[from] triblespace_core::repo::pin_assertion::PinAssertionKeyCollision),
    }

    type MemoryReader = <MemoryRepo as BlobStore>::Reader;

    #[derive(Clone)]
    struct ProbeReadFailure {
        handle: [u8; 32],
        fail_at: usize,
        reads: Arc<AtomicUsize>,
    }

    impl ProbeReadFailure {
        fn should_fail(&self, handle: [u8; 32]) -> bool {
            handle == self.handle && self.reads.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at
        }
    }

    #[derive(Clone)]
    struct ProbeReader {
        inner: MemoryReader,
        failure: Option<ProbeReadFailure>,
    }

    impl PartialEq for ProbeReader {
        fn eq(&self, other: &Self) -> bool {
            let same_failure = match (&self.failure, &other.failure) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    left.handle == right.handle
                        && left.fail_at == right.fail_at
                        && Arc::ptr_eq(&left.reads, &right.reads)
                }
                (None, Some(_)) | (Some(_), None) => false,
            };
            self.inner == other.inner && same_failure
        }
    }

    impl Eq for ProbeReader {}

    #[derive(Debug)]
    enum ProbeGetError<E> {
        Injected,
        Inner(E),
    }

    impl<E> std::fmt::Display for ProbeGetError<E>
    where
        E: std::fmt::Display,
    {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Injected => formatter.write_str("injected blob read failure"),
                Self::Inner(error) => error.fmt(formatter),
            }
        }
    }

    impl<E> std::error::Error for ProbeGetError<E>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Injected => None,
                Self::Inner(error) => Some(error),
            }
        }
    }

    impl BlobStoreGet for ProbeReader {
        type GetError<E: std::error::Error + Send + Sync + 'static> =
            ProbeGetError<<MemoryReader as BlobStoreGet>::GetError<E>>;

        fn get<T, E>(
            &self,
            handle: Inline<Handle<E>>,
        ) -> Result<T, Self::GetError<<T as TryFromBlob<E>>::Error>>
        where
            E: BlobEncoding + 'static,
            T: TryFromBlob<E>,
            Handle<E>: InlineEncoding,
        {
            if self
                .failure
                .as_ref()
                .is_some_and(|failure| failure.should_fail(handle.raw))
            {
                return Err(ProbeGetError::Injected);
            }
            self.inner.get(handle).map_err(ProbeGetError::Inner)
        }
    }

    impl BlobStoreList for ProbeReader {
        type Iter<'a> = <MemoryReader as BlobStoreList>::Iter<'a>;
        type Err = <MemoryReader as BlobStoreList>::Err;

        fn blobs<'a>(&'a self) -> Self::Iter<'a> {
            self.inner.blobs()
        }

        fn blobs_diff<'a>(&'a self, old: &Self) -> Self::Iter<'a> {
            self.inner.blobs_diff(&old.inner)
        }
    }

    struct FlushProbe {
        inner: MemoryRepo,
        flushes: Arc<AtomicUsize>,
        fail_flush: bool,
        fail_append: bool,
        fail_after_append_once: bool,
        read_failure: Option<ProbeReadFailure>,
    }

    impl triblespace_core::repo::BlobStorePut for FlushProbe {
        type PutError = <MemoryRepo as triblespace_core::repo::BlobStorePut>::PutError;

        fn put<E, T>(&mut self, item: T) -> Result<Inline<Handle<E>>, Self::PutError>
        where
            E: BlobEncoding + 'static,
            T: IntoBlob<E>,
            Handle<E>: InlineEncoding,
        {
            self.inner.put(item)
        }
    }

    impl triblespace_core::repo::BlobStore for FlushProbe {
        type Reader = ProbeReader;
        type ReaderError = <MemoryRepo as triblespace_core::repo::BlobStore>::ReaderError;

        fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
            Ok(ProbeReader {
                inner: self.inner.reader()?,
                failure: self.read_failure.clone(),
            })
        }
    }

    impl triblespace_core::repo::PinStore for FlushProbe {
        type PinsError = <MemoryRepo as triblespace_core::repo::PinStore>::PinsError;
        type HeadError = <MemoryRepo as triblespace_core::repo::PinStore>::HeadError;
        type UpdateError = <MemoryRepo as triblespace_core::repo::PinStore>::UpdateError;
        type ListIter<'a> = <MemoryRepo as triblespace_core::repo::PinStore>::ListIter<'a>;

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

    impl PinAssertionStore for FlushProbe {
        type Error = InjectedAssertionError;

        fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
            self.inner
                .pin_assertion_snapshot()
                .map_err(InjectedAssertionError::from)
        }

        fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
            if self.fail_append {
                Err(InjectedAssertionError::Injected)
            } else {
                self.inner
                    .append_pin_assertion(assertion)
                    .map_err(InjectedAssertionError::from)?;
                if self.fail_after_append_once {
                    self.fail_after_append_once = false;
                    Err(InjectedAssertionError::Injected)
                } else {
                    Ok(())
                }
            }
        }
    }

    impl StorageFlush for FlushProbe {
        type Error = InjectedFlushError;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            if self.fail_flush {
                Err(InjectedFlushError)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn generic_pin_assertions_round_trip_through_peer() {
        use triblespace_core::repo::pin_assertion::{
            PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
        };

        let signing_key = SigningKey::from_bytes(&[0xA1; 32]);
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, _wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let pin = PinHandle::from_raw([0xB2; 32]);
        let assertion = PinAssertion::sign(
            &signing_key,
            pin,
            ValueHandle::from_raw([0xC3; 32]),
            SubsumptionLabel::from_raw([0xD4; 32]),
        );

        peer.append_pin_assertion(assertion)
            .expect("forward assertion append");
        peer.append_pin_assertion(assertion)
            .expect("duplicate append remains idempotent");

        let snapshot = peer
            .pin_assertion_snapshot()
            .expect("forward assertion snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot.for_pin(&PinIdentity::new(signing_key.verifying_key(), pin)),
            vec![assertion]
        );
    }

    #[test]
    fn selected_authored_wants_share_one_global_cache_prefix() {
        use triblespace_core::repo::want::sign_want;
        use triblespace_core::repo::yard::{Yard, YardConfig};

        let local = SigningKey::from_bytes(&[0xA2; 32]);
        let foreign = SigningKey::from_bytes(&[0xB2; 32]);
        let endpoint =
            EndpointId::from_bytes(&local.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, _wiring) = host::wire(endpoint);
        let dir = tempfile::tempdir().expect("temporary yard");
        let yard = Yard::create(
            [dir.path().join("young.pile")],
            YardConfig {
                want_budget: 2,
                ..YardConfig::default()
            },
        )
        .expect("create yard");
        let mut peer =
            Peer::with_wiring(yard, local.clone(), local.verifying_key(), sender, receiver);

        let foreign_low = Inline::<Handle<UnknownBlob>>::new([1; 32]);
        let local_middle = Inline::<Handle<UnknownBlob>>::new([2; 32]);
        let local_high = Inline::<Handle<UnknownBlob>>::new([3; 32]);
        peer.store()
            .append_pin_assertion(sign_want(&foreign, foreign_low))
            .unwrap();
        peer.assert_want(local_high).unwrap();
        peer.assert_want(local_middle).unwrap();

        let (authored_count, selected) = peer.selected_authored_wants().unwrap();
        assert_eq!(authored_count, 2, "the exact authored G-set stays visible");
        assert_eq!(
            selected,
            BTreeSet::from([local_middle]),
            "capacity applies globally before the local-author intersection"
        );
    }

    #[tokio::test]
    async fn bounded_want_collection_and_reconciliation_reach_a_fixed_point() {
        use triblespace_core::blob::encodings::rawbytes::RawBytes;
        use triblespace_core::repo::BlobStoreList;
        use triblespace_core::repo::yard::{Yard, YardConfig};

        fn resident(peer: &mut Peer<Yard>) -> BTreeSet<[u8; 32]> {
            let mut store = peer.store();
            let reader = store.reader().unwrap();
            reader.blobs().map(|handle| handle.unwrap().raw).collect()
        }

        let local = SigningKey::from_bytes(&[0xA3; 32]);
        let endpoint =
            EndpointId::from_bytes(&local.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, _wiring) = host::wire(endpoint);
        let dir = tempfile::tempdir().expect("temporary yard");
        let yard = Yard::create(
            [dir.path().join("young.pile")],
            YardConfig {
                want_budget: 1,
                ..YardConfig::default()
            },
        )
        .expect("create yard");
        let mut peer =
            Peer::with_wiring(yard, local.clone(), local.verifying_key(), sender, receiver);

        let mut blobs = [
            Bytes::from_source(b"fixed-point-a".to_vec()),
            Bytes::from_source(b"fixed-point-b".to_vec()),
        ]
        .into_iter()
        .map(|bytes| {
            let handle = Blob::<RawBytes>::new(bytes.clone()).get_handle();
            (handle, bytes)
        })
        .collect::<Vec<_>>();
        blobs.sort_by_key(|(handle, _)| handle.raw);
        for (handle, bytes) in &blobs {
            assert_eq!(
                peer.store().put::<RawBytes, _>(bytes.clone()).unwrap(),
                *handle
            );
            peer.assert_want(*handle).unwrap();
        }

        peer.store().collect().unwrap();
        let fixed = resident(&mut peer);
        assert_eq!(fixed, BTreeSet::from([blobs[0].0.raw]));

        let mut reconciler = crate::reconcile::Reconciler::new();
        for _ in 0..2 {
            let stats = reconciler.tick(&mut peer).await;
            assert_eq!(stats.wants, 2);
            assert_eq!(stats.selected, 1);
            assert_eq!(stats.missing, 0);
            assert_eq!(stats.attempted, 0, "the evicted tail is never refetched");
            peer.store().collect().unwrap();
            assert_eq!(
                resident(&mut peer),
                fixed,
                "two full reconcile/collect cycles must preserve the resident set"
            );
        }
    }

    fn test_capability(
        issuer: &SigningKey,
        subject: ed25519_dalek::VerifyingKey,
        permission: Id,
        valid_for_seconds: f64,
    ) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
        let scope = genid();
        let facts: TribleSet = entity! {
            ExclusiveId::force_ref(&scope) @
            triblespace_core::metadata::tag: permission,
        }
        .into();
        let now = crate::clock::epoch_now();
        let expiry = (
            now,
            now + hifitime::Duration::from_seconds(valid_for_seconds),
        )
            .try_to_inline()
            .expect("expiry interval");
        let synthetic_root = SigningKey::from_bytes(&[0xFA; 32]);
        let anchor_scope = genid();
        let anchor_facts: TribleSet = entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let parent = triblespace_core::repo::capability::build_founder_anchor(
            &synthetic_root,
            issuer.verifying_key(),
            *anchor_scope,
            anchor_facts,
        )
        .expect("build synthetic test anchor");
        triblespace_core::repo::capability::build_capability(
            issuer, subject, parent, *scope, facts, expiry,
        )
        .expect("build test capability")
    }

    fn capability_until(
        issuer: &SigningKey,
        subject: ed25519_dalek::VerifyingKey,
        parent: Option<(Blob<SimpleArchive>, Blob<SimpleArchive>)>,
        permission: Id,
        upper: hifitime::Epoch,
    ) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
        let scope = genid();
        let facts: TribleSet = entity! {
            ExclusiveId::force_ref(&scope) @
            triblespace_core::metadata::tag: permission,
        }
        .into();
        let now = crate::clock::epoch_now();
        let lower = if upper < now {
            upper - hifitime::Duration::from_seconds(1.0)
        } else {
            now
        };
        let expiry = (lower, upper).try_to_inline().expect("expiry interval");
        let parent = parent.unwrap_or_else(|| {
            let synthetic_root = SigningKey::from_bytes(&[0xFB; 32]);
            let anchor_scope = genid();
            let anchor_facts: TribleSet = entity! {
                ExclusiveId::force_ref(&anchor_scope) @
                triblespace_core::metadata::tag:
                    triblespace_core::repo::capability::PERM_ADMIN,
            }
            .into();
            triblespace_core::repo::capability::build_founder_anchor(
                &synthetic_root,
                issuer.verifying_key(),
                *anchor_scope,
                anchor_facts,
            )
            .expect("build synthetic bounded-test anchor")
        });
        triblespace_core::repo::capability::build_capability(
            issuer, subject, parent, *scope, facts, expiry,
        )
        .expect("build bounded test capability")
    }

    fn founder_credential_until(
        team_root: &SigningKey,
        founder: &SigningKey,
        subject: ed25519_dalek::VerifyingKey,
        permission: Id,
        upper: hifitime::Epoch,
    ) -> (
        (Blob<SimpleArchive>, Blob<SimpleArchive>),
        (Blob<SimpleArchive>, Blob<SimpleArchive>),
    ) {
        let anchor_scope = genid();
        let anchor_facts: TribleSet = entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let anchor = triblespace_core::repo::capability::build_founder_anchor(
            team_root,
            founder.verifying_key(),
            *anchor_scope,
            anchor_facts,
        )
        .expect("build founder anchor");
        let credential =
            capability_until(founder, subject, Some(anchor.clone()), permission, upper);
        (anchor, credential)
    }

    struct AssertedGrantSeries {
        grant: GrantIdentity,
        anchor: (Blob<SimpleArchive>, Blob<SimpleArchive>),
        scope_root: Id,
    }

    impl AssertedGrantSeries {
        fn new(
            team_root: &SigningKey,
            author: &SigningKey,
            subject: ed25519_dalek::VerifyingKey,
        ) -> Self {
            let anchor_scope = genid();
            let anchor_facts: TribleSet = entity! {
                ExclusiveId::force_ref(&anchor_scope) @
                triblespace_core::metadata::tag:
                    triblespace_core::repo::capability::PERM_ADMIN,
            }
            .into();
            let anchor = triblespace_core::repo::capability::build_founder_anchor(
                team_root,
                author.verifying_key(),
                *anchor_scope,
                anchor_facts,
            )
            .expect("build asserted grant anchor");
            let scope_root = *genid();
            Self {
                grant: GrantIdentity::new(team_root.verifying_key(), subject, scope_root),
                anchor,
                scope_root,
            }
        }

        fn issue<S>(
            &self,
            store: &mut S,
            author: &SigningKey,
            permission: Id,
            upper: hifitime::Epoch,
        ) -> (Inline<Handle<SimpleArchive>>, Inline<Handle<SimpleArchive>>)
        where
            S: BlobStore + StorageFlush + PinAssertionStore,
        {
            let facts: TribleSet = entity! {
                ExclusiveId::force_ref(&self.scope_root) @
                triblespace_core::metadata::tag: permission,
            }
            .into();
            let now = crate::clock::epoch_now();
            let lower = if upper < now {
                upper - hifitime::Duration::from_seconds(1.0)
            } else {
                now
            };
            let expiry = (lower, upper)
                .try_to_inline()
                .expect("asserted grant expiry");
            let (cap, sig) = triblespace_core::repo::capability::build_capability(
                author,
                self.grant.subject(),
                self.anchor.clone(),
                self.scope_root,
                facts,
                expiry,
            )
            .expect("build asserted credential");
            let cap_handle = cap.get_handle();
            let sig_handle = sig.get_handle();
            crate::policy_ledger::issue_grant(
                store,
                author,
                self.grant,
                sig,
                None,
                [cap, self.anchor.0.clone(), self.anchor.1.clone()],
            )
            .expect("issue asserted grant");
            (cap_handle, sig_handle)
        }
    }

    fn issue_asserted_grant<S>(
        store: &mut S,
        team_root: &SigningKey,
        author: &SigningKey,
        subject: ed25519_dalek::VerifyingKey,
    ) -> (
        GrantIdentity,
        Inline<Handle<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
    )
    where
        S: BlobStore + StorageFlush + PinAssertionStore,
    {
        let upper = crate::clock::epoch_now() + hifitime::Duration::from_hours(1.0);
        let series = AssertedGrantSeries::new(team_root, author, subject);
        let (cap, sig) = series.issue(
            store,
            author,
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        (series.grant, cap, sig)
    }

    fn take_dispatched_credentials(
        wiring: &host::HostWiring,
    ) -> Vec<(
        PublisherKey,
        Inline<Handle<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
    )> {
        wiring
            .cmd_rx
            .try_iter()
            .filter_map(|command| match command {
                crate::channel::NetCommand::DeliverCap {
                    subject,
                    cap_bytes,
                    sig_bytes,
                } => Some((
                    subject,
                    Blob::<SimpleArchive>::new(cap_bytes).get_handle(),
                    Blob::<SimpleArchive>::new(sig_bytes).get_handle(),
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn redispatch_sends_exact_unauthenticated_current_credential() {
        let team_root = SigningKey::from_bytes(&[0x92; 32]);
        let author = SigningKey::from_bytes(&[0x93; 32]);
        let subject = SigningKey::from_bytes(&[0x94; 32]).verifying_key();
        let mut store = MemoryRepo::default();
        let (grant, cap, sig) = issue_asserted_grant(&mut store, &team_root, &author, subject);

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer =
            Peer::with_wiring(store, author, team_root.verifying_key(), sender, receiver);
        assert!(take_dispatched_credentials(&wiring).is_empty());

        assert_eq!(peer.redispatch_unauthenticated(), 1);
        assert_eq!(
            take_dispatched_credentials(&wiring),
            vec![(subject.to_bytes(), cap, sig)]
        );
        assert_eq!(peer.last_dispatch_attempt.len(), 1);
        assert_eq!(peer.last_dispatch_attempt.get(&grant).unwrap().0, sig);

        assert_eq!(peer.redispatch_unauthenticated(), 0);
        assert!(
            take_dispatched_credentials(&wiring).is_empty(),
            "the same current signature is cooldown-limited"
        );
    }

    #[test]
    fn redispatch_suppresses_authenticated_disabled_conflicted_expired_local_and_foreign_team_grants()
     {
        let team_root = SigningKey::from_bytes(&[0x95; 32]);
        let foreign_root = SigningKey::from_bytes(&[0xA4; 32]);
        let author = SigningKey::from_bytes(&[0x96; 32]);
        let authenticated_subject = SigningKey::from_bytes(&[0x97; 32]).verifying_key();
        let disabled_subject = SigningKey::from_bytes(&[0x98; 32]).verifying_key();
        let conflicted_subject = SigningKey::from_bytes(&[0x99; 32]).verifying_key();
        let expired_subject = SigningKey::from_bytes(&[0xAA; 32]).verifying_key();
        let mut store = MemoryRepo::default();

        let (authenticated_grant, _cap, authenticated_sig) =
            issue_asserted_grant(&mut store, &team_root, &author, authenticated_subject);
        crate::policy_ledger::authenticate_credential(
            &mut store,
            &author,
            authenticated_grant,
            authenticated_sig,
        )
        .expect("authenticate asserted grant");

        let (disabled_grant, _cap, _sig) =
            issue_asserted_grant(&mut store, &team_root, &author, disabled_subject);
        crate::policy_ledger::disable_grant(&mut store, &author, disabled_grant)
            .expect("disable asserted grant");

        let conflicted = AssertedGrantSeries::new(&team_root, &author, conflicted_subject);
        let now = crate::clock::epoch_now();
        conflicted.issue(
            &mut store,
            &author,
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_hours(1.0),
        );
        conflicted.issue(
            &mut store,
            &author,
            triblespace_core::repo::capability::PERM_WRITE,
            now + hifitime::Duration::from_hours(2.0),
        );
        let expired = AssertedGrantSeries::new(&team_root, &author, expired_subject);
        expired.issue(
            &mut store,
            &author,
            triblespace_core::repo::capability::PERM_READ,
            now - hifitime::Duration::from_seconds(1.0),
        );

        let (local_grant, _cap, _sig) =
            issue_asserted_grant(&mut store, &team_root, &author, author.verifying_key());
        let foreign_subject = SigningKey::from_bytes(&[0xA5; 32]).verifying_key();
        let (foreign_grant, _cap, _sig) =
            issue_asserted_grant(&mut store, &foreign_root, &author, foreign_subject);

        let snapshot = store.pin_assertion_snapshot().unwrap();
        let reader = store.reader().unwrap();
        let crate::policy_ledger::PolicyLedgerResolution::Complete(view) =
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                author.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            )
        else {
            panic!("suppression fixture must be a complete policy ledger");
        };
        assert!(
            view.grants()
                .get(&authenticated_grant)
                .unwrap()
                .usable_at(now)
                .unwrap()
                .authenticated()
        );
        assert!(view.grants().get(&disabled_grant).unwrap().disabled());
        assert!(matches!(
            view.grants()
                .get(&conflicted.grant)
                .unwrap()
                .historical_issuance(),
            crate::policy_ledger::GrantIssuanceResolution::Conflicted { .. }
        ));
        let expired_current = match view
            .grants()
            .get(&expired.grant)
            .unwrap()
            .historical_issuance()
        {
            crate::policy_ledger::GrantIssuanceResolution::Current(current) => current,
            _ => panic!("expiry does not erase historical current selection"),
        };
        assert!(
            expired_current.capability().is_expired_at(now),
            "the fixture must isolate dispatch-time liveness from reduction"
        );
        assert!(
            view.grants()
                .get(&expired.grant)
                .unwrap()
                .usable_at(now)
                .is_none()
        );
        assert!(
            view.grants()
                .get(&local_grant)
                .unwrap()
                .usable_at(now)
                .is_some()
        );
        assert!(
            view.grants()
                .get(&foreign_grant)
                .unwrap()
                .usable_at(now)
                .is_some(),
            "foreign-team fixture must otherwise be dispatchable"
        );
        drop(reader);

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer =
            Peer::with_wiring(store, author, team_root.verifying_key(), sender, receiver);
        assert!(take_dispatched_credentials(&wiring).is_empty());

        assert_eq!(peer.redispatch_unauthenticated(), 0);
        assert!(take_dispatched_credentials(&wiring).is_empty());
        assert!(peer.last_dispatch_attempt.is_empty());
    }

    #[test]
    fn redispatch_new_current_signature_resets_cooldown_without_growing_map() {
        let team_root = SigningKey::from_bytes(&[0x9A; 32]);
        let author = SigningKey::from_bytes(&[0x9B; 32]);
        let subject = SigningKey::from_bytes(&[0x9C; 32]).verifying_key();
        let series = AssertedGrantSeries::new(&team_root, &author, subject);
        let now = crate::clock::epoch_now();
        let mut store = MemoryRepo::default();
        let first = series.issue(
            &mut store,
            &author,
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_hours(1.0),
        );

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            author.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        assert!(take_dispatched_credentials(&wiring).is_empty());
        assert_eq!(peer.redispatch_unauthenticated(), 1);
        assert_eq!(
            take_dispatched_credentials(&wiring),
            vec![(subject.to_bytes(), first.0, first.1)]
        );
        assert_eq!(peer.redispatch_unauthenticated(), 0);

        let successor = {
            let mut store = peer.store.lock().expect("store mutex");
            series.issue(
                &mut *store,
                &author,
                triblespace_core::repo::capability::PERM_READ,
                now + hifitime::Duration::from_hours(2.0),
            )
        };
        assert_ne!(successor, first);

        assert_eq!(
            peer.redispatch_unauthenticated(),
            1,
            "a selected successor signature bypasses its predecessor's cooldown"
        );
        assert_eq!(
            take_dispatched_credentials(&wiring),
            vec![(subject.to_bytes(), successor.0, successor.1)]
        );
        assert_eq!(peer.last_dispatch_attempt.len(), 1);
        assert_eq!(
            peer.last_dispatch_attempt.get(&series.grant).unwrap().0,
            successor.1
        );
    }

    #[test]
    fn redispatch_defers_incomplete_and_invalid_policy_ledgers() {
        let incomplete_root = SigningKey::from_bytes(&[0x9D; 32]);
        let incomplete_author = SigningKey::from_bytes(&[0x9E; 32]);
        let incomplete_subject = SigningKey::from_bytes(&[0x9F; 32]).verifying_key();
        let incomplete_grant = GrantIdentity::new(
            incomplete_root.verifying_key(),
            incomplete_subject,
            *genid(),
        );
        let missing_sig = Inline::<Handle<SimpleArchive>>::new([0xA0; 32]);
        let missing_event = crate::policy_ledger::PolicyEvent::GrantIssued {
            grant: incomplete_grant,
            sig: missing_sig,
            request: None,
        };
        let mut incomplete_store = MemoryRepo::default();
        incomplete_store
            .append_pin_assertion(crate::policy_ledger::sign_policy_event(
                &incomplete_author,
                missing_event,
            ))
            .expect("append assertion without event blob");
        let endpoint = EndpointId::from_bytes(&incomplete_author.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut incomplete_peer = Peer::with_wiring(
            incomplete_store,
            incomplete_author,
            incomplete_root.verifying_key(),
            sender,
            receiver,
        );
        assert_eq!(incomplete_peer.redispatch_unauthenticated(), 0);
        assert!(take_dispatched_credentials(&wiring).is_empty());

        let invalid_root = SigningKey::from_bytes(&[0xA1; 32]);
        let invalid_author = SigningKey::from_bytes(&[0xA2; 32]);
        let invalid_subject = SigningKey::from_bytes(&[0xA3; 32]).verifying_key();
        let invalid_grant =
            GrantIdentity::new(invalid_root.verifying_key(), invalid_subject, *genid());
        let malformed_sig_blob =
            crate::policy_ledger::PolicyEvent::GrantDisabled(invalid_grant).to_blob();
        let malformed_sig = malformed_sig_blob.get_handle();
        let invalid_event = crate::policy_ledger::PolicyEvent::GrantIssued {
            grant: invalid_grant,
            sig: malformed_sig,
            request: None,
        };
        let mut invalid_store = MemoryRepo::default();
        invalid_store
            .put::<SimpleArchive, _>(malformed_sig_blob)
            .expect("store malformed proof");
        invalid_store
            .put::<SimpleArchive, _>(invalid_event.to_blob())
            .expect("store invalid policy event");
        invalid_store
            .append_pin_assertion(crate::policy_ledger::sign_policy_event(
                &invalid_author,
                invalid_event,
            ))
            .expect("append invalid issuance assertion");
        let endpoint = EndpointId::from_bytes(&invalid_author.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut invalid_peer = Peer::with_wiring(
            invalid_store,
            invalid_author,
            invalid_root.verifying_key(),
            sender,
            receiver,
        );
        assert_eq!(invalid_peer.redispatch_unauthenticated(), 0);
        assert!(take_dispatched_credentials(&wiring).is_empty());
    }

    #[test]
    fn redispatch_second_selected_blob_read_failure_defers_the_whole_pass() {
        let team_root = SigningKey::from_bytes(&[0xA6; 32]);
        let author = SigningKey::from_bytes(&[0xA7; 32]);
        let first_subject = SigningKey::from_bytes(&[0xA8; 32]).verifying_key();
        let second_subject = SigningKey::from_bytes(&[0xA9; 32]).verifying_key();
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut store = FlushProbe {
            inner: MemoryRepo::default(),
            flushes,
            fail_flush: false,
            fail_append: false,
            fail_after_append_once: false,
            read_failure: None,
        };
        let first = issue_asserted_grant(&mut store, &team_root, &author, first_subject);
        let second = issue_asserted_grant(&mut store, &team_root, &author, second_subject);
        let (earlier, later) = if first.0 < second.0 {
            (first, second)
        } else {
            (second, first)
        };
        let reads = Arc::new(AtomicUsize::new(0));

        // Establish that one complete reduction reads this cap exactly once;
        // the injected second access below is therefore materialization after
        // selection, not an incomplete-ledger fixture.
        store.read_failure = Some(ProbeReadFailure {
            handle: later.1.raw,
            fail_at: usize::MAX,
            reads: Arc::clone(&reads),
        });
        let snapshot = store.pin_assertion_snapshot().unwrap();
        let reader = store.reader().unwrap();
        assert!(matches!(
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                author.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            ),
            crate::policy_ledger::PolicyLedgerResolution::Complete(_)
        ));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        reads.store(0, Ordering::SeqCst);
        store.read_failure = Some(ProbeReadFailure {
            handle: later.1.raw,
            fail_at: 2,
            reads: Arc::clone(&reads),
        });

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer =
            Peer::with_wiring(store, author, team_root.verifying_key(), sender, receiver);
        assert!(take_dispatched_credentials(&wiring).is_empty());

        assert_eq!(peer.redispatch_unauthenticated(), 0);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert!(
            take_dispatched_credentials(&wiring).is_empty(),
            "a later selected-blob failure must not leak the earlier grant"
        );
        assert!(peer.last_dispatch_attempt.is_empty());

        assert_eq!(
            peer.redispatch_unauthenticated(),
            2,
            "the one-shot read fault must leave both valid grants retryable"
        );
        assert_eq!(
            take_dispatched_credentials(&wiring),
            vec![
                (earlier.0.subject().to_bytes(), earlier.1, earlier.2),
                (later.0.subject().to_bytes(), later.1, later.2),
            ]
        );
    }

    #[test]
    fn cap_delivery_confirmation_asserts_authentication_for_disabled_current_issuance() {
        let team_root = SigningKey::from_bytes(&[0x81; 32]);
        let author = SigningKey::from_bytes(&[0x82; 32]);
        let subject = SigningKey::from_bytes(&[0x83; 32]).verifying_key();
        let mut store = MemoryRepo::default();
        let (grant, _cap, sig) = issue_asserted_grant(&mut store, &team_root, &author, subject);
        crate::policy_ledger::disable_grant(&mut store, &author, grant)
            .expect("disable asserted grant");

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            author.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        wiring
            .evt_tx
            .send(NetEvent::CapDeliveryConfirmed {
                subject: subject.to_bytes(),
                sig_handle: sig.raw,
                admission: cap_delivery_admission(),
            })
            .expect("event channel open");

        peer.refresh()
            .expect("record confirmed credential authentication");

        let mut store = peer.store.lock().expect("store mutex");
        let snapshot = store.pin_assertion_snapshot().unwrap();
        assert_eq!(snapshot.len(), 3, "issue, disable, and auth are asserted");
        let reader = store.reader().unwrap();
        let crate::policy_ledger::PolicyLedgerResolution::Complete(view) =
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                author.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            )
        else {
            panic!("confirmed policy ledger must remain complete");
        };
        let grant_view = view.grants().get(&grant).expect("issued grant");
        assert!(grant_view.disabled());
        let crate::policy_ledger::GrantIssuanceResolution::Current(current) =
            grant_view.historical_issuance()
        else {
            panic!("disabled grant retains its current historical issuance");
        };
        assert_eq!(current.sig(), sig);
        assert!(current.authenticated());
        assert!(view.event_handles().contains(
            &crate::policy_ledger::PolicyEvent::CredentialAuthenticated { grant, sig }.handle()
        ));
    }

    #[test]
    fn unknown_cap_delivery_confirmation_is_a_noop() {
        let team_root = SigningKey::from_bytes(&[0x84; 32]);
        let author = SigningKey::from_bytes(&[0x85; 32]);
        let subject = SigningKey::from_bytes(&[0x86; 32]).verifying_key();
        let mut store = MemoryRepo::default();
        let (grant, _cap, issued_sig) =
            issue_asserted_grant(&mut store, &team_root, &author, subject);
        let unknown_sig = Inline::<Handle<SimpleArchive>>::new([0x87; 32]);
        assert_ne!(unknown_sig, issued_sig);

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            author.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        wiring
            .evt_tx
            .send(NetEvent::CapDeliveryConfirmed {
                subject: subject.to_bytes(),
                sig_handle: unknown_sig.raw,
                admission: cap_delivery_admission(),
            })
            .expect("event channel open");

        peer.refresh().expect("an unknown confirmation is harmless");

        let mut store = peer.store.lock().expect("store mutex");
        let snapshot = store.pin_assertion_snapshot().unwrap();
        assert_eq!(snapshot.len(), 1, "an unknown signature appends no event");
        let reader = store.reader().unwrap();
        let crate::policy_ledger::PolicyLedgerResolution::Complete(view) =
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                author.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            )
        else {
            panic!("issued policy ledger must remain complete");
        };
        let crate::policy_ledger::GrantIssuanceResolution::Current(current) =
            view.grants().get(&grant).unwrap().historical_issuance()
        else {
            panic!("issued grant must remain current");
        };
        assert!(!current.authenticated());
    }

    #[test]
    fn incomplete_confirmation_policy_is_sticky_fail_stop() {
        let team_root = SigningKey::from_bytes(&[0x88; 32]);
        let author = SigningKey::from_bytes(&[0x89; 32]);
        let subject = SigningKey::from_bytes(&[0x8A; 32]).verifying_key();
        let grant =
            crate::policy_ledger::GrantIdentity::new(team_root.verifying_key(), subject, *genid());
        let missing_sig = Inline::<Handle<SimpleArchive>>::new([0x8B; 32]);
        let event = crate::policy_ledger::PolicyEvent::GrantIssued {
            grant,
            sig: missing_sig,
            request: None,
        };
        let mut store = MemoryRepo::default();
        store
            .append_pin_assertion(crate::policy_ledger::sign_policy_event(&author, event))
            .expect("append assertion without its named event blob");

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer =
            Peer::with_wiring(store, author, team_root.verifying_key(), sender, receiver);
        wiring
            .evt_tx
            .send(NetEvent::CapDeliveryConfirmed {
                subject: subject.to_bytes(),
                sig_handle: missing_sig.raw,
                admission: cap_delivery_admission(),
            })
            .expect("event channel open");

        let error = peer
            .refresh()
            .expect_err("an incomplete policy ledger must fail closed");
        assert_eq!(error.operation(), "resolve delivery-confirmation policy");
        assert!(error.detail().contains("policy ledger is incomplete"));
        assert_eq!(
            peer.refresh().expect_err("resolution failure stays sticky"),
            error
        );
    }

    #[test]
    fn invalid_confirmation_policy_is_sticky_fail_stop() {
        let team_root = SigningKey::from_bytes(&[0x8C; 32]);
        let author = SigningKey::from_bytes(&[0x8D; 32]);
        let subject = SigningKey::from_bytes(&[0x8E; 32]).verifying_key();
        let grant =
            crate::policy_ledger::GrantIdentity::new(team_root.verifying_key(), subject, *genid());
        let malformed_sig_blob = crate::policy_ledger::PolicyEvent::GrantDisabled(grant).to_blob();
        let malformed_sig = malformed_sig_blob.get_handle();
        let event = crate::policy_ledger::PolicyEvent::GrantIssued {
            grant,
            sig: malformed_sig,
            request: None,
        };
        let mut store = MemoryRepo::default();
        assert_eq!(
            store
                .put::<SimpleArchive, _>(malformed_sig_blob)
                .expect("store malformed proof"),
            malformed_sig
        );
        let event_blob = event.to_blob();
        assert_eq!(
            store
                .put::<SimpleArchive, _>(event_blob)
                .expect("store policy event"),
            event.handle()
        );
        store
            .append_pin_assertion(crate::policy_ledger::sign_policy_event(&author, event))
            .expect("append invalid issuance assertion");

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer =
            Peer::with_wiring(store, author, team_root.verifying_key(), sender, receiver);
        wiring
            .evt_tx
            .send(NetEvent::CapDeliveryConfirmed {
                subject: subject.to_bytes(),
                sig_handle: malformed_sig.raw,
                admission: cap_delivery_admission(),
            })
            .expect("event channel open");

        let error = peer
            .refresh()
            .expect_err("an invalid policy ledger must fail closed");
        assert_eq!(error.operation(), "resolve delivery-confirmation policy");
        assert!(error.detail().contains("policy ledger is invalid"));
        assert_eq!(
            peer.refresh().expect_err("resolution failure stays sticky"),
            error
        );
    }

    #[test]
    fn confirmation_authentication_append_failure_is_sticky_fail_stop() {
        let team_root = SigningKey::from_bytes(&[0x8F; 32]);
        let author = SigningKey::from_bytes(&[0x90; 32]);
        let subject = SigningKey::from_bytes(&[0x91; 32]).verifying_key();
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut store = FlushProbe {
            inner: MemoryRepo::default(),
            flushes: Arc::clone(&flushes),
            fail_flush: false,
            fail_append: false,
            fail_after_append_once: false,
            read_failure: None,
        };
        let (grant, _cap, sig) = issue_asserted_grant(&mut store, &team_root, &author, subject);
        store.fail_append = true;

        let endpoint =
            EndpointId::from_bytes(&author.verifying_key().to_bytes()).expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            author.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        wiring
            .evt_tx
            .send(NetEvent::CapDeliveryConfirmed {
                subject: subject.to_bytes(),
                sig_handle: sig.raw,
                admission: cap_delivery_admission(),
            })
            .expect("event channel open");

        let error = peer
            .refresh()
            .expect_err("authentication assertion failure must propagate");
        assert_eq!(error.operation(), "record credential authentication");
        assert!(error.detail().contains("failed to append policy assertion"));
        assert_eq!(flushes.load(Ordering::SeqCst), 2, "issue and auth flush");
        assert_eq!(
            peer.refresh().expect_err("append failure stays sticky"),
            error
        );

        let mut store = peer.store.lock().expect("store mutex");
        let snapshot = store.pin_assertion_snapshot().unwrap();
        assert_eq!(snapshot.len(), 1, "failed append asserted no auth event");
        let reader = store.reader().unwrap();
        let crate::policy_ledger::PolicyLedgerResolution::Complete(view) =
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                author.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            )
        else {
            panic!("pre-effect assertion failure leaves the ledger complete");
        };
        let crate::policy_ledger::GrantIssuanceResolution::Current(current) =
            view.grants().get(&grant).unwrap().historical_issuance()
        else {
            panic!("issued grant remains current");
        };
        assert!(!current.authenticated());
    }

    #[test]
    fn cap_request_positive_receipt_survives_ungraceful_pile_reopen() {
        use triblespace_core::repo::pile::Pile;

        let signing_key = SigningKey::from_bytes(&[0x71; 32]);
        let requester = SigningKey::from_bytes(&[0x72; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let dir = tempfile::tempdir().expect("temporary pile directory");
        let path = dir.path().join("durable-join-request.pile");
        std::fs::File::create(&path).expect("create empty pile");
        let mut peer = Peer::with_wiring(
            Pile::open(&path).expect("open pile"),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let partial_cap_bytes = partial_cap_bytes(requester, signing_key.verifying_key());
        let partial_cap_handle = Blob::<SimpleArchive>::new(partial_cap_bytes.clone()).get_handle();
        let (completion, mut receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes,
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        assert!(
            matches!(
                receipt.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "queue admission alone must not complete the wire receipt"
        );

        peer.refresh().expect("persist queued request");

        assert_eq!(
            receipt.try_recv(),
            Ok(true),
            "only the durable policy assertion receipt can license STATUS_OK"
        );
        // Deliberately do not call close(). A successful reopen validates that
        // the positive receipt followed the explicit flush and that the exact
        // closure plus assertion are replayable; this does not simulate power loss.
        drop(peer.into_store());
        let mut reopened = Pile::open(&path).expect("reopen flushed pile");
        let snapshot = reopened
            .pin_assertion_snapshot()
            .expect("replay policy assertion");
        assert_eq!(snapshot.len(), 1);
        let reader = reopened.reader().expect("open replay reader");
        let request = crate::policy_ledger::RequestIdentity::new(requester, partial_cap_handle);
        let crate::policy_ledger::PolicyLedgerResolution::Complete(view) =
            crate::policy_ledger::resolve_policy_ledger(
                &snapshot,
                signing_key.verifying_key(),
                |handle| {
                    reader
                        .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                        .ok()
                },
            )
        else {
            panic!("durably acknowledged request must reopen as a complete policy ledger");
        };
        let request_view = view
            .requests()
            .get(&request)
            .expect("exact request is present after reopen");
        assert!(request_view.observed());
        assert!(request_view.is_pending());
        assert!(
            view.event_handles()
                .contains(&crate::policy_ledger::PolicyEvent::RequestObserved(request).handle())
        );
        drop(reader);
        reopened.close().expect("close reopened pile");
    }

    #[test]
    fn cap_request_flush_failure_drops_receipt_and_is_fail_stop() {
        let signing_key = SigningKey::from_bytes(&[0x73; 32]);
        let requester = SigningKey::from_bytes(&[0x74; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut peer = Peer::with_wiring(
            FlushProbe {
                inner: MemoryRepo::default(),
                flushes: Arc::clone(&flushes),
                fail_flush: true,
                fail_append: false,
                fail_after_append_once: false,
                read_failure: None,
            },
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let (completion, mut receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: partial_cap_bytes(requester, signing_key.verifying_key()),
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        let error = peer.refresh().expect_err("flush failure must propagate");

        assert_eq!(error.operation(), "observe capability request");
        assert!(error.detail().contains("failed to flush policy closure"));
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert!(
            matches!(
                receipt.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "a storage error must close the receipt so the host reports an indeterminate outcome"
        );
        assert_eq!(
            peer.refresh()
                .expect_err("persistence failure stays sticky"),
            error
        );
        let mut store = peer.store.lock().expect("store mutex");
        assert!(store.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn cap_request_assertion_failure_drops_receipt_and_is_fail_stop() {
        let signing_key = SigningKey::from_bytes(&[0x75; 32]);
        let requester = SigningKey::from_bytes(&[0x76; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut peer = Peer::with_wiring(
            FlushProbe {
                inner: MemoryRepo::default(),
                flushes: Arc::clone(&flushes),
                fail_flush: false,
                fail_append: true,
                fail_after_append_once: false,
                read_failure: None,
            },
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let (completion, mut receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: partial_cap_bytes(requester, signing_key.verifying_key()),
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        let error = peer
            .refresh()
            .expect_err("assertion append failure must propagate");

        assert_eq!(error.operation(), "observe capability request");
        assert!(error.detail().contains("failed to append policy assertion"));
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert!(
            matches!(
                receipt.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "an assertion error must not manufacture a definitive refusal"
        );
        assert_eq!(
            peer.refresh().expect_err("assertion failure stays sticky"),
            error
        );
        let mut store = peer.store.lock().expect("store mutex");
        assert!(store.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn cap_request_post_effect_append_error_is_indeterminate_and_exact_replay_recovers() {
        let signing_key = SigningKey::from_bytes(&[0x77; 32]);
        let requester = SigningKey::from_bytes(&[0x78; 32]).verifying_key();
        let partial_cap_bytes = partial_cap_bytes(requester, signing_key.verifying_key());
        let partial_cap = Blob::<SimpleArchive>::new(partial_cap_bytes.clone()).get_handle();
        let request = crate::policy_ledger::RequestIdentity::new(requester, partial_cap);
        let expected = crate::policy_ledger::sign_policy_event(
            &signing_key,
            crate::policy_ledger::PolicyEvent::RequestObserved(request),
        );
        let flushes = Arc::new(AtomicUsize::new(0));

        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            FlushProbe {
                inner: MemoryRepo::default(),
                flushes: Arc::clone(&flushes),
                fail_flush: false,
                fail_append: false,
                fail_after_append_once: true,
                read_failure: None,
            },
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let (completion, mut receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: partial_cap_bytes.clone(),
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        let error = peer
            .refresh()
            .expect_err("post-effect assertion error must fail-stop");
        assert_eq!(error.operation(), "observe capability request");
        assert!(error.detail().contains("failed to append policy assertion"));
        assert!(matches!(
            receipt.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
        assert_eq!(
            peer.refresh()
                .expect_err("post-effect failure stays sticky"),
            error
        );
        {
            let mut store = peer.store.lock().expect("store mutex");
            let snapshot = store.pin_assertion_snapshot().unwrap();
            assert_eq!(snapshot.len(), 1);
            assert_eq!(
                snapshot.for_pin(&crate::policy_ledger::PolicyLedgerDescriptor::pin_identity(
                    signing_key.verifying_key(),
                )),
                vec![expected],
                "the failed append result does not prove the assertion had no effect"
            );
        }

        let retained = peer.into_store();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut restarted = Peer::with_wiring(
            retained,
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let (completion, mut replay_receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes,
                admission: cap_request_admission(),
                completion,
            })
            .expect("replay event channel open");

        restarted
            .refresh()
            .expect("exact replay resolves the indeterminate outcome");
        assert_eq!(replay_receipt.try_recv(), Ok(true));
        let mut store = restarted.store.lock().expect("store mutex");
        assert_eq!(
            store.pin_assertion_snapshot().unwrap().len(),
            1,
            "exact replay must not duplicate the already-applied assertion"
        );
        assert_eq!(flushes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cap_request_subject_must_match_authenticated_requester() {
        let signing_key = SigningKey::from_bytes(&[10; 32]);
        let requester = SigningKey::from_bytes(&[11; 32]).verifying_key();
        let different_subject = SigningKey::from_bytes(&[12; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let bytes = partial_cap_bytes(different_subject, signing_key.verifying_key());
        let handle = Blob::<SimpleArchive>::new(bytes.clone()).get_handle();
        let (completion, mut receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: bytes,
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        peer.refresh().unwrap();
        assert_eq!(receipt.try_recv(), Ok(false));

        let mut store = peer.store.lock().expect("store mutex");
        assert!(store.pin_assertion_snapshot().unwrap().is_empty());
        let reader = store.reader().expect("memory reader");
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                .is_err(),
            "a mismatched request must be rejected before blob persistence"
        );
    }

    #[test]
    fn cap_request_issuer_mismatch_is_refused_without_poisoning_peer() {
        let signing_key = SigningKey::from_bytes(&[0x13; 32]);
        let requester = SigningKey::from_bytes(&[0x14; 32]).verifying_key();
        let wrong_issuer = SigningKey::from_bytes(&[0x15; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );

        let refused_bytes = partial_cap_bytes(requester, wrong_issuer);
        let refused_handle = Blob::<SimpleArchive>::new(refused_bytes.clone()).get_handle();
        let (completion, mut refused_receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: refused_bytes,
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        peer.refresh()
            .expect("issuer mismatch is an ordinary refusal");
        assert_eq!(refused_receipt.try_recv(), Ok(false));

        let accepted_bytes = partial_cap_bytes(requester, signing_key.verifying_key());
        let (completion, mut accepted_receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: requester.to_bytes(),
                partial_cap_bytes: accepted_bytes,
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");

        peer.refresh()
            .expect("a later valid request proves the Peer was not poisoned");
        assert_eq!(accepted_receipt.try_recv(), Ok(true));

        let mut store = peer.store.lock().expect("store mutex");
        assert_eq!(store.pin_assertion_snapshot().unwrap().len(), 1);
        let reader = store.reader().expect("memory reader");
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(refused_handle)
                .is_err(),
            "refused claim must not become a closure orphan"
        );
    }

    #[test]
    fn cap_request_exact_replay_is_acknowledged_without_duplicate_assertion() {
        let signing_key = SigningKey::from_bytes(&[0x16; 32]);
        let requester = SigningKey::from_bytes(&[0x17; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );
        let bytes = partial_cap_bytes(requester, signing_key.verifying_key());

        for attempt in 0..2 {
            let (completion, mut receipt) = tokio::sync::oneshot::channel();
            wiring
                .evt_tx
                .send(NetEvent::CapRequest {
                    requester: requester.to_bytes(),
                    partial_cap_bytes: bytes.clone(),
                    admission: cap_request_admission(),
                    completion,
                })
                .expect("event channel open");
            peer.refresh().expect("exact request replay is idempotent");
            assert_eq!(
                receipt.try_recv(),
                Ok(true),
                "attempt {attempt} must receive the durable positive receipt"
            );
        }

        let mut store = peer.store.lock().expect("store mutex");
        assert_eq!(store.pin_assertion_snapshot().unwrap().len(), 1);
    }

    #[test]
    fn cap_request_admission_refusal_is_negative_without_poisoning_peer() {
        let signing_key = SigningKey::from_bytes(&[0x18; 32]);
        let requester = SigningKey::from_bytes(&[0x19; 32]).verifying_key();
        let other_requester = SigningKey::from_bytes(&[0x1A; 32]).verifying_key();
        let endpoint = EndpointId::from_bytes(&signing_key.verifying_key().to_bytes())
            .expect("valid endpoint id");
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            MemoryRepo::default(),
            signing_key.clone(),
            signing_key.verifying_key(),
            sender,
            receiver,
        );

        let first = partial_cap_bytes(requester, signing_key.verifying_key());
        let second = partial_cap_bytes(requester, signing_key.verifying_key());
        let second_handle = Blob::<SimpleArchive>::new(second.clone()).get_handle();
        for (bytes, expected) in [(first, true), (second, false)] {
            let (completion, mut receipt) = tokio::sync::oneshot::channel();
            wiring
                .evt_tx
                .send(NetEvent::CapRequest {
                    requester: requester.to_bytes(),
                    partial_cap_bytes: bytes,
                    admission: cap_request_admission(),
                    completion,
                })
                .expect("event channel open");
            peer.refresh()
                .expect("ordinary admission refusal must not fail-stop");
            assert_eq!(receipt.try_recv(), Ok(expected));
        }

        let (completion, mut later_receipt) = tokio::sync::oneshot::channel();
        wiring
            .evt_tx
            .send(NetEvent::CapRequest {
                requester: other_requester.to_bytes(),
                partial_cap_bytes: partial_cap_bytes(other_requester, signing_key.verifying_key()),
                admission: cap_request_admission(),
                completion,
            })
            .expect("event channel open");
        peer.refresh()
            .expect("a different requester remains admissible after refusal");
        assert_eq!(later_receipt.try_recv(), Ok(true));

        let mut store = peer.store.lock().expect("store mutex");
        assert_eq!(store.pin_assertion_snapshot().unwrap().len(), 2);
        let reader = store.reader().expect("memory reader");
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(second_handle)
                .is_err(),
            "refused alternate request must not be stored"
        );
    }

    #[test]
    fn startup_prefers_verified_durable_team_cap_over_stale_configuration() {
        let root = SigningKey::from_bytes(&[13; 32]);
        let founder = SigningKey::from_bytes(&[17; 32]);
        let subject = SigningKey::from_bytes(&[14; 32]);
        let (anchor, cap) = founder_credential_until(
            &root,
            &founder,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            crate::clock::epoch_now() + hifitime::Duration::from_seconds(600.0),
        );
        let mut store = MemoryRepo::default();
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(anchor.0.clone())
            .unwrap();
        let cap_handle = store.put(cap.0).unwrap();
        let sig_handle = store.put(cap.1).unwrap();
        crate::policy::pin_team_cap(&mut store, root.verifying_key(), cap_handle, sig_handle)
            .unwrap();
        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: [0xEE; 32],
        };

        assert_eq!(
            startup_self_cap(&mut store, &subject, &config),
            sig_handle.raw
        );
    }

    #[test]
    fn startup_verifies_and_promotes_unpinned_configured_credential() {
        let root = SigningKey::from_bytes(&[15; 32]);
        let founder = SigningKey::from_bytes(&[18; 32]);
        let subject = SigningKey::from_bytes(&[16; 32]);
        let (anchor, cap) = founder_credential_until(
            &root,
            &founder,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            crate::clock::epoch_now() + hifitime::Duration::from_seconds(600.0),
        );
        let mut store = MemoryRepo::default();
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(anchor.0)
            .unwrap();
        let cap_handle = store.put(cap.0).unwrap();
        let sig_handle = store.put(cap.1).unwrap();
        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: sig_handle.raw,
        };

        assert_eq!(
            startup_self_cap(&mut store, &subject, &config),
            sig_handle.raw
        );
        assert_eq!(
            crate::policy::current_team_cap(&mut store, root.verifying_key()),
            Some((cap_handle, sig_handle))
        );
    }

    #[test]
    fn startup_retains_valid_expired_pin_but_enters_recovery_only_mode() {
        let root = SigningKey::from_bytes(&[0xA1; 32]);
        let founder = SigningKey::from_bytes(&[0xA2; 32]);
        let now = crate::clock::epoch_now();
        let (anchor, cap) = founder_credential_until(
            &root,
            &founder,
            founder.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            now - hifitime::Duration::from_seconds(60.0),
        );
        let mut store = MemoryRepo::default();
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(anchor.0)
            .unwrap();
        let anchor_sig = store.put(anchor.1).unwrap();
        let cap_handle = store.put(cap.0).unwrap();
        let sig_handle = store.put(cap.1).unwrap();
        let credential = crate::policy::TeamCredential {
            cap: cap_handle,
            sig: sig_handle,
            founder_anchor_sig: Some(anchor_sig),
        };
        crate::policy::pin_team_credential(&mut store, root.verifying_key(), credential).unwrap();
        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: sig_handle.raw,
        };

        assert_eq!(startup_self_cap(&mut store, &founder, &config), [0; 32]);
        assert_eq!(
            crate::policy::current_team_credential(&mut store, root.verifying_key()),
            Some(credential),
            "recovery startup must retain the durable founder authority"
        );
    }

    #[test]
    fn startup_still_panics_for_internally_inconsistent_pinned_credential() {
        let root = SigningKey::from_bytes(&[0xA3; 32]);
        let founder = SigningKey::from_bytes(&[0xA4; 32]);
        let (anchor, cap) = founder_credential_until(
            &root,
            &founder,
            founder.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            crate::clock::epoch_now() + hifitime::Duration::from_seconds(600.0),
        );
        let mut store = MemoryRepo::default();
        let wrong_cap_handle = store.put(anchor.0).unwrap();
        let anchor_sig = store.put(anchor.1).unwrap();
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(cap.0)
            .unwrap();
        let sig_handle = store.put(cap.1).unwrap();
        crate::policy::pin_team_credential(
            &mut store,
            root.verifying_key(),
            crate::policy::TeamCredential {
                cap: wrong_cap_handle,
                sig: sig_handle,
                founder_anchor_sig: Some(anchor_sig),
            },
        )
        .unwrap();
        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: sig_handle.raw,
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            startup_self_cap(&mut store, &founder, &config)
        }));
        assert!(result.is_err(), "invalid pinned state must fail loudly");
    }

    #[test]
    fn founder_self_renewal_rotates_sibling_locally_and_preserves_anchor() {
        let root = SigningKey::from_bytes(&[19; 32]);
        let founder = SigningKey::from_bytes(&[20; 32]);
        let now = crate::clock::epoch_now();
        let (anchor, old) = founder_credential_until(
            &root,
            &founder,
            founder.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            now + hifitime::Duration::from_seconds(60.0),
        );
        // A second, narrower self-issued policy entry is valid data but is not
        // the constitutional credential slot. Its ordering in the policy set
        // must never decide which capability gets rotated under the anchor.
        let decoy = capability_until(
            &founder,
            founder.verifying_key(),
            Some(anchor.clone()),
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_seconds(60.0),
        );
        let old_fields = delivery_cap_fields(old.0.clone()).unwrap();
        let decoy_fields = delivery_cap_fields(decoy.0.clone()).unwrap();
        let mut store = MemoryRepo::default();
        let anchor_cap = store.put(anchor.0).unwrap();
        let anchor_sig = store.put(anchor.1).unwrap();
        let old_cap = store.put(old.0).unwrap();
        let old_sig = store.put(old.1).unwrap();
        let decoy_cap = store.put(decoy.0).unwrap();
        let decoy_sig = store.put(decoy.1).unwrap();
        crate::policy::pin_team_credential(
            &mut store,
            root.verifying_key(),
            crate::policy::TeamCredential {
                cap: old_cap,
                sig: old_sig,
                founder_anchor_sig: Some(anchor_sig),
            },
        )
        .unwrap();
        let entry = crate::policy::record_policy_entry(
            &mut store,
            founder.verifying_key(),
            old_fields.scope_root,
            old_fields.expiry,
            old_cap,
            old_sig,
        )
        .unwrap();
        crate::policy::mark_policy_delivered(&mut store, entry).unwrap();
        let decoy_entry = crate::policy::record_policy_entry(
            &mut store,
            founder.verifying_key(),
            decoy_fields.scope_root,
            decoy_fields.expiry,
            decoy_cap,
            decoy_sig,
        )
        .unwrap();
        crate::policy::mark_policy_delivered(&mut store, decoy_entry).unwrap();

        let endpoint = EndpointId::from_bytes(&founder.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            founder.clone(),
            root.verifying_key(),
            sender,
            receiver,
        );
        while wiring.cmd_rx.try_recv().is_ok() {}

        assert_eq!(
            peer.renewal_tick(hifitime::Duration::from_seconds(120.0)),
            1
        );
        let mut saw_rotation = None;
        for command in wiring.cmd_rx.try_iter() {
            match command {
                crate::channel::NetCommand::UpdateSelfCap(sig) => saw_rotation = Some(sig),
                crate::channel::NetCommand::DeliverCap { subject, .. }
                    if subject == founder.verifying_key().to_bytes() =>
                {
                    panic!("founder rotation must not self-deliver over the network")
                }
                _ => {}
            }
        }

        let mut store = peer.store.lock().unwrap();
        let current = crate::policy::current_team_credential(&mut *store, root.verifying_key())
            .expect("rotated credential remains pinned");
        assert_eq!(current.founder_anchor_sig, Some(anchor_sig));
        assert_ne!(current.cap, old_cap);
        assert_ne!(current.sig, old_sig);
        assert_eq!(saw_rotation, Some(current.sig.raw));
        let entries = crate::policy::list_renewal_policy(&mut *store);
        assert_eq!(entries.len(), 2);
        let active_entry = entries
            .iter()
            .find(|candidate| candidate.id == entry)
            .unwrap();
        assert_eq!(active_entry.latest_sig, current.sig);
        assert!(active_entry.delivered_at.is_some());
        let untouched_decoy = entries
            .iter()
            .find(|candidate| candidate.id == decoy_entry)
            .unwrap();
        assert_eq!(untouched_decoy.latest_cap, decoy_cap);
        assert_eq!(untouched_decoy.latest_sig, decoy_sig);
        let reader = store.reader().unwrap();
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(anchor_cap)
                .is_ok(),
            "the anchor cap remains retained through its stable signature"
        );
        let verified = triblespace_core::repo::capability::verify_chain(
            root.verifying_key(),
            current.sig,
            founder.verifying_key(),
            |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            },
        )
        .expect("rotated founder credential verifies as a fresh anchor sibling");
        assert!(
            verified
                .permissions()
                .contains(&triblespace_core::repo::capability::PERM_ADMIN),
            "the active admin credential, not a narrower self-policy entry, is rotated"
        );
    }

    #[test]
    fn founder_policy_reconciles_a_durable_credential_winner_before_renewing() {
        let root = SigningKey::from_bytes(&[0x31; 32]);
        let founder = SigningKey::from_bytes(&[0x32; 32]);
        let now = crate::clock::epoch_now();
        let (anchor, old) = founder_credential_until(
            &root,
            &founder,
            founder.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            now + hifitime::Duration::from_seconds(60.0),
        );
        let old_fields = delivery_cap_fields(old.0.clone()).unwrap();
        let fresh_expiry = (now, now + hifitime::Duration::from_seconds(3_600.0))
            .try_to_inline()
            .unwrap();
        let fresh = triblespace_core::repo::capability::build_capability(
            &founder,
            founder.verifying_key(),
            anchor.clone(),
            old_fields.scope_root,
            extract_scope_subgraph(&old_fields.set, old_fields.scope_root),
            fresh_expiry,
        )
        .unwrap();

        let mut store = MemoryRepo::default();
        let _anchor_cap: Inline<Handle<SimpleArchive>> = store.put(anchor.0).unwrap();
        let anchor_sig = store.put(anchor.1).unwrap();
        let old_cap = store.put(old.0).unwrap();
        let old_sig = store.put(old.1).unwrap();
        let fresh_cap = store.put(fresh.0).unwrap();
        let fresh_sig = store.put(fresh.1).unwrap();

        // Simulate a crash after the new credential pin became durable but
        // before rotate_founder_self could rewrite its separate policy pin.
        crate::policy::pin_team_credential(
            &mut store,
            root.verifying_key(),
            crate::policy::TeamCredential {
                cap: fresh_cap,
                sig: fresh_sig,
                founder_anchor_sig: Some(anchor_sig),
            },
        )
        .unwrap();
        let entry = crate::policy::record_policy_entry(
            &mut store,
            founder.verifying_key(),
            old_fields.scope_root,
            old_fields.expiry,
            old_cap,
            old_sig,
        )
        .unwrap();
        crate::policy::mark_policy_delivered(&mut store, entry).unwrap();

        let endpoint = EndpointId::from_bytes(&founder.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(store, founder, root.verifying_key(), sender, receiver);
        while wiring.cmd_rx.try_recv().is_ok() {}

        assert_eq!(
            peer.renewal_tick(hifitime::Duration::from_seconds(120.0)),
            0,
            "repairing the stale policy must not mint an unnecessary sibling"
        );
        let self_cap_updates: Vec<_> = wiring
            .cmd_rx
            .try_iter()
            .filter_map(|command| match command {
                crate::channel::NetCommand::UpdateSelfCap(sig) => Some(sig),
                _ => None,
            })
            .collect();
        assert_eq!(
            self_cap_updates,
            vec![fresh_sig.raw],
            "recovery must publish the durable credential winner to the live host"
        );

        let mut store = peer.store.lock().unwrap();
        assert_eq!(
            crate::policy::current_team_cap(&mut *store, root.verifying_key()),
            Some((fresh_cap, fresh_sig))
        );
        let policies = crate::policy::list_renewal_policy(&mut *store);
        let repaired = policies
            .iter()
            .find(|candidate| candidate.id == entry)
            .unwrap();
        assert_eq!(repaired.latest_cap, fresh_cap);
        assert_eq!(repaired.latest_sig, fresh_sig);
        assert_eq!(repaired.effective_expiry, fresh_expiry);
        assert!(repaired.delivered_at.is_some());
    }

    #[test]
    fn founder_policy_schedules_from_the_verified_chain_expiry() {
        let root = SigningKey::from_bytes(&[0x41; 32]);
        let founder = SigningKey::from_bytes(&[0x42; 32]);
        let now = crate::clock::epoch_now();
        let parent_upper = now + hifitime::Duration::from_seconds(600.0);
        let leaf_upper = now + hifitime::Duration::from_seconds(3_600.0);
        let (anchor, parent) = founder_credential_until(
            &root,
            &founder,
            founder.verifying_key(),
            triblespace_core::repo::capability::PERM_ADMIN,
            parent_upper,
        );
        let leaf = capability_until(
            &founder,
            founder.verifying_key(),
            Some(parent.clone()),
            triblespace_core::repo::capability::PERM_ADMIN,
            leaf_upper,
        );
        let leaf_fields = delivery_cap_fields(leaf.0.clone()).unwrap();

        let mut store = MemoryRepo::default();
        let _anchor_cap: Inline<Handle<SimpleArchive>> = store.put(anchor.0).unwrap();
        let anchor_sig = store.put(anchor.1).unwrap();
        let _parent_cap: Inline<Handle<SimpleArchive>> = store.put(parent.0).unwrap();
        let _parent_sig: Inline<Handle<SimpleArchive>> = store.put(parent.1).unwrap();
        let leaf_cap = store.put(leaf.0).unwrap();
        let leaf_sig = store.put(leaf.1).unwrap();
        crate::policy::pin_team_credential(
            &mut store,
            root.verifying_key(),
            crate::policy::TeamCredential {
                cap: leaf_cap,
                sig: leaf_sig,
                founder_anchor_sig: Some(anchor_sig),
            },
        )
        .unwrap();
        let entry = crate::policy::record_policy_entry(
            &mut store,
            founder.verifying_key(),
            leaf_fields.scope_root,
            leaf_fields.expiry,
            leaf_cap,
            leaf_sig,
        )
        .unwrap();
        crate::policy::mark_policy_delivered(&mut store, entry).unwrap();

        let endpoint = EndpointId::from_bytes(&founder.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(store, founder, root.verifying_key(), sender, receiver);
        while wiring.cmd_rx.try_recv().is_ok() {}

        assert_eq!(
            peer.renewal_tick(hifitime::Duration::from_seconds(1.0)),
            0,
            "the effective parent deadline is not yet in this tiny renewal window"
        );
        assert!(wiring.cmd_rx.try_iter().any(|command| matches!(
            command,
            crate::channel::NetCommand::UpdateSelfCap(sig) if sig == leaf_sig.raw
        )));

        let mut store = peer.store.lock().unwrap();
        let repaired = crate::policy::list_renewal_policy(&mut *store)
            .into_iter()
            .find(|candidate| candidate.id == entry)
            .unwrap();
        let (_lower, effective_upper) =
            <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&repaired.effective_expiry)
                .unwrap();
        assert_eq!(effective_upper, parent_upper);
        assert!(effective_upper < leaf_upper);
        assert!(repaired.delivered_at.is_some());
    }

    #[test]
    fn first_delivery_requires_matching_local_request_intent() {
        let mut store = MemoryRepo::default();
        let issuer = SigningKey::from_bytes(&[21; 32]);
        let other_issuer = SigningKey::from_bytes(&[22; 32]);
        let subject = SigningKey::from_bytes(&[23; 32]).verifying_key();
        let (requested, _) = test_capability(
            &issuer,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            600.0,
        );
        crate::policy::record_outbound_cap_request(&mut store, requested)
            .expect("record local intent");

        let (matching, _) = test_capability(
            &issuer,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            300.0,
        );
        let matching_expiry = expiry_upper(&delivery_cap_fields(matching.clone()).unwrap().expiry)
            .expect("matching expiry");
        assert!(matches!(
            select_cap_delivery(
                &mut store,
                issuer.verifying_key(),
                matching,
                matching_expiry,
            ),
            Some(DeliverySelection {
                initial_request: Some(_),
                ..
            })
        ));

        let (wrong_issuer, _) = test_capability(
            &other_issuer,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            300.0,
        );
        let wrong_expiry = expiry_upper(&delivery_cap_fields(wrong_issuer.clone()).unwrap().expiry)
            .expect("wrong expiry");
        assert!(
            select_cap_delivery(
                &mut store,
                issuer.verifying_key(),
                wrong_issuer,
                wrong_expiry,
            )
            .is_none()
        );

        let (stronger_than_requested, _) = test_capability(
            &issuer,
            subject,
            triblespace_core::repo::capability::PERM_ADMIN,
            300.0,
        );
        let stronger_expiry = expiry_upper(
            &delivery_cap_fields(stronger_than_requested.clone())
                .unwrap()
                .expiry,
        )
        .expect("stronger expiry");
        assert!(
            select_cap_delivery(
                &mut store,
                issuer.verifying_key(),
                stronger_than_requested,
                stronger_expiry,
            )
            .is_none()
        );
    }

    #[test]
    fn stale_first_delivery_selection_cannot_cross_request_replacement() {
        let mut store = MemoryRepo::default();
        let issuer_a = SigningKey::from_bytes(&[0x61; 32]);
        let issuer_b = SigningKey::from_bytes(&[0x62; 32]);
        let subject = SigningKey::from_bytes(&[0x63; 32]).verifying_key();

        let request_a = test_capability(
            &issuer_a,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            600.0,
        );
        crate::policy::record_outbound_cap_request(&mut store, request_a.0)
            .expect("record request A");
        let candidate_a = test_capability(
            &issuer_a,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            300.0,
        );
        let candidate_upper =
            expiry_upper(&delivery_cap_fields(candidate_a.0.clone()).unwrap().expiry).unwrap();
        let stale = select_cap_delivery(
            &mut store,
            issuer_a.verifying_key(),
            candidate_a.0.clone(),
            candidate_upper,
        )
        .expect("A initially matches local intent");
        let stale_request = stale.initial_request.expect("first-delivery witness");

        let request_b = test_capability(
            &issuer_b,
            subject,
            triblespace_core::repo::capability::PERM_READ,
            600.0,
        );
        let request_b_handle = request_b.0.get_handle();
        crate::policy::record_outbound_cap_request(&mut store, request_b.0)
            .expect("replace A with request B");

        let cap = store.put(candidate_a.0).unwrap();
        let sig = store.put(candidate_a.1).unwrap();
        assert_eq!(
            crate::policy::begin_outbound_cap_activation_if_pending(
                &mut store,
                stale_request,
                crate::policy::TeamCredential {
                    cap,
                    sig,
                    founder_anchor_sig: None,
                },
            ),
            Some(crate::policy::OutboundRequestCasResult::Conflict),
            "selection over A must not lock or activate after B wins the request CAS"
        );
        assert_eq!(
            crate::policy::expected_outbound_cap_request_handle(&mut store),
            Some(request_b_handle),
            "request B remains intact"
        );
        assert!(
            crate::policy::current_team_cap(&mut store, issuer_a.verifying_key()).is_none(),
            "stale A never reaches the team-cap pin"
        );
    }

    #[test]
    fn startup_recovers_crash_after_activating_before_team_cap() {
        let mut store = MemoryRepo::default();
        let root = SigningKey::from_bytes(&[0x64; 32]);
        let founder = SigningKey::from_bytes(&[0x65; 32]);
        let issuer = SigningKey::from_bytes(&[0x66; 32]);
        let subject = SigningKey::from_bytes(&[0x67; 32]);
        let upper = crate::clock::epoch_now() + hifitime::Duration::from_seconds(600.0);

        let request = capability_until(
            &issuer,
            subject.verifying_key(),
            None,
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        crate::policy::record_outbound_cap_request(&mut store, request.0)
            .expect("record first-delivery intent");

        let anchor_scope = genid();
        let anchor_facts: TribleSet = entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let anchor = triblespace_core::repo::capability::build_founder_anchor(
            &root,
            founder.verifying_key(),
            *anchor_scope,
            anchor_facts,
        )
        .unwrap();
        let parent = capability_until(
            &founder,
            issuer.verifying_key(),
            Some(anchor.clone()),
            triblespace_core::repo::capability::PERM_ADMIN,
            upper,
        );
        let candidate = capability_until(
            &issuer,
            subject.verifying_key(),
            Some(parent.clone()),
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        let selection =
            select_cap_delivery(&mut store, root.verifying_key(), candidate.0.clone(), upper)
                .expect("candidate matches Pending request");

        let _: Inline<Handle<SimpleArchive>> = store.put(anchor.0).unwrap();
        let _: Inline<Handle<SimpleArchive>> = store.put(parent.0).unwrap();
        let cap = store.put(candidate.0).unwrap();
        let sig = store.put(candidate.1).unwrap();
        let credential = crate::policy::TeamCredential {
            cap,
            sig,
            founder_anchor_sig: None,
        };
        let activating = match crate::policy::begin_outbound_cap_activation_if_pending(
            &mut store,
            selection.initial_request.unwrap(),
            credential,
        ) {
            Some(crate::policy::OutboundRequestCasResult::Success(state)) => state,
            other => panic!("Pending-to-Activating CAS failed: {other:?}"),
        };
        store.flush().unwrap();
        assert!(activating.activation.is_some());
        assert!(
            crate::policy::current_team_cap(&mut store, root.verifying_key()).is_none(),
            "simulated crash point is before the team-cap CAS"
        );
        let replacement = test_capability(
            &issuer,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_READ,
            300.0,
        );
        assert!(
            crate::policy::record_outbound_cap_request(&mut store, replacement.0).is_none(),
            "Activating is an exclusive journal phase"
        );
        assert_eq!(
            crate::policy::clear_outbound_cap_request_if(&mut store, activating.partial_cap),
            Some(false),
            "handle-only rejection cleanup cannot tear down Activating"
        );

        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: [0; 32],
        };
        assert_eq!(startup_self_cap(&mut store, &subject, &config), sig.raw);
        assert_eq!(
            crate::policy::current_team_credential(&mut store, root.verifying_key()),
            Some(credential),
            "startup finishes the interrupted activation"
        );
        assert!(
            crate::policy::expected_outbound_cap_request_handle(&mut store).is_none(),
            "the exact Activating journal is cleared only after durable activation"
        );
    }

    #[test]
    fn activation_recovery_preserves_a_different_team_cap_winner() {
        let mut store = MemoryRepo::default();
        let root = SigningKey::from_bytes(&[0x68; 32]);
        let founder = SigningKey::from_bytes(&[0x69; 32]);
        let subject = SigningKey::from_bytes(&[0x6A; 32]);
        let upper = crate::clock::epoch_now() + hifitime::Duration::from_seconds(600.0);

        let request = capability_until(
            &founder,
            subject.verifying_key(),
            None,
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        crate::policy::record_outbound_cap_request(&mut store, request.0).unwrap();
        let (anchor, candidate) = founder_credential_until(
            &root,
            &founder,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        let selection =
            select_cap_delivery(&mut store, root.verifying_key(), candidate.0.clone(), upper)
                .expect("candidate matches Pending request");
        let _: Inline<Handle<SimpleArchive>> = store.put(anchor.0.clone()).unwrap();
        let candidate_cap = store.put(candidate.0).unwrap();
        let candidate_sig = store.put(candidate.1).unwrap();
        let candidate_credential = crate::policy::TeamCredential {
            cap: candidate_cap,
            sig: candidate_sig,
            founder_anchor_sig: None,
        };
        assert!(matches!(
            crate::policy::begin_outbound_cap_activation_if_pending(
                &mut store,
                selection.initial_request.unwrap(),
                candidate_credential,
            ),
            Some(crate::policy::OutboundRequestCasResult::Success(_))
        ));
        store.flush().unwrap();

        // A separate exact team-cap writer wins before recovery resumes. Its
        // valid credential is authoritative; the stale activation journal may
        // be reconciled, never used to overwrite this winner.
        let winner = capability_until(
            &founder,
            subject.verifying_key(),
            Some(anchor),
            triblespace_core::repo::capability::PERM_READ,
            upper,
        );
        let winner_cap = store.put(winner.0).unwrap();
        let winner_sig = store.put(winner.1).unwrap();
        let winner_credential = crate::policy::TeamCredential {
            cap: winner_cap,
            sig: winner_sig,
            founder_anchor_sig: None,
        };
        crate::policy::pin_team_credential(&mut store, root.verifying_key(), winner_credential)
            .unwrap();
        store.flush().unwrap();

        let config = PeerConfig {
            peers: Vec::new(),
            team_root: root.verifying_key(),
            self_cap: [0; 32],
        };
        assert_eq!(
            startup_self_cap(&mut store, &subject, &config),
            winner_sig.raw
        );
        assert_eq!(
            crate::policy::current_team_credential(&mut store, root.verifying_key()),
            Some(winner_credential),
            "recovery must preserve the exact concurrent team-cap winner"
        );
        assert!(
            crate::policy::expected_outbound_cap_request_handle(&mut store).is_none(),
            "the stale journal is exactly cleared after the winner is verified and flushed"
        );
    }

    #[test]
    fn first_delivery_accepts_exact_requested_effective_upper_boundary() {
        let mut store = MemoryRepo::default();
        let root = SigningKey::from_bytes(&[31; 32]);
        let founder = SigningKey::from_bytes(&[39; 32]);
        let issuer = SigningKey::from_bytes(&[32; 32]);
        let subject = SigningKey::from_bytes(&[33; 32]).verifying_key();
        let now = crate::clock::epoch_now();
        let requested_upper = now + hifitime::Duration::from_seconds(300.0);

        let requested = capability_until(
            &issuer,
            subject,
            None,
            triblespace_core::repo::capability::PERM_READ,
            requested_upper,
        )
        .0;
        crate::policy::record_outbound_cap_request(&mut store, requested)
            .expect("record bounded request");

        let anchor_scope = genid();
        let anchor_facts: TribleSet = entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let anchor = triblespace_core::repo::capability::build_founder_anchor(
            &root,
            founder.verifying_key(),
            *anchor_scope,
            anchor_facts,
        )
        .unwrap();
        let parent = capability_until(
            &founder,
            issuer.verifying_key(),
            Some(anchor),
            triblespace_core::repo::capability::PERM_ADMIN,
            requested_upper,
        );
        let candidate = capability_until(
            &issuer,
            subject,
            Some(parent),
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_seconds(600.0),
        )
        .0;

        assert!(matches!(
            select_cap_delivery(&mut store, root.verifying_key(), candidate, requested_upper,),
            Some(DeliverySelection {
                initial_request: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn delivery_that_expires_in_queue_persists_and_activates_nothing() {
        let issuer = SigningKey::from_bytes(&[37; 32]);
        let subject = SigningKey::from_bytes(&[38; 32]);
        let mut store = MemoryRepo::default();
        let requested = test_capability(
            &issuer,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_READ,
            600.0,
        );
        crate::policy::record_outbound_cap_request(&mut store, requested.0)
            .expect("record local request");
        let expected = crate::policy::expected_outbound_cap_request_handle(&mut store)
            .expect("expectation exists");
        let delivered = test_capability(
            &issuer,
            subject.verifying_key(),
            triblespace_core::repo::capability::PERM_READ,
            300.0,
        );
        let delivered_cap: Inline<Handle<SimpleArchive>> = delivered.0.get_handle();
        let delivered_sig: Inline<Handle<SimpleArchive>> = delivered.1.get_handle();

        let endpoint = EndpointId::from_bytes(&subject.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(store, subject, issuer.verifying_key(), sender, receiver);
        wiring
            .evt_tx
            .send(NetEvent::CapDelivered {
                issuer: issuer.verifying_key().to_bytes(),
                cap_bytes: delivered.0.bytes,
                sig_bytes: delivered.1.bytes,
                proof_blobs: Vec::new(),
                authority_expires_at: crate::clock::epoch_now()
                    - hifitime::Duration::from_nanoseconds(1.0),
                admission: cap_delivery_admission(),
            })
            .unwrap();

        peer.refresh().unwrap();
        let mut store = peer.store.lock().unwrap();
        let reader = store.reader().unwrap();
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(delivered_cap)
                .is_err()
        );
        assert!(
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(delivered_sig)
                .is_err()
        );
        drop(reader);
        assert_eq!(
            crate::policy::expected_outbound_cap_request_handle(&mut *store),
            Some(expected),
            "an expired queued response must not clear request intent"
        );
        assert!(
            crate::policy::current_team_cap(&mut *store, issuer.verifying_key()).is_none(),
            "an expired queued response must not install an active credential"
        );
    }

    #[test]
    fn delegated_parent_deadline_prevents_effective_authority_downgrade() {
        let mut store = MemoryRepo::default();
        let root = SigningKey::from_bytes(&[34; 32]);
        let founder = SigningKey::from_bytes(&[40; 32]);
        let issuer = SigningKey::from_bytes(&[35; 32]);
        let subject = SigningKey::from_bytes(&[36; 32]).verifying_key();
        let now = crate::clock::epoch_now();
        let current_upper = now + hifitime::Duration::from_seconds(300.0);
        let candidate_upper = now + hifitime::Duration::from_seconds(100.0);

        let anchor_scope = genid();
        let anchor_facts: TribleSet = entity! {
            ExclusiveId::force_ref(&anchor_scope) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let anchor = triblespace_core::repo::capability::build_founder_anchor(
            &root,
            founder.verifying_key(),
            *anchor_scope,
            anchor_facts,
        )
        .unwrap();
        let current_parent = capability_until(
            &founder,
            issuer.verifying_key(),
            Some(anchor.clone()),
            triblespace_core::repo::capability::PERM_ADMIN,
            current_upper,
        );
        let current = capability_until(
            &issuer,
            subject,
            Some(current_parent.clone()),
            triblespace_core::repo::capability::PERM_READ,
            current_upper,
        );
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(anchor.0.clone())
            .unwrap();
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(current_parent.0)
            .unwrap();
        let current_cap = store.put(current.0).unwrap();
        let current_sig = store.put(current.1).unwrap();
        crate::policy::pin_team_cap(&mut store, root.verifying_key(), current_cap, current_sig)
            .expect("pin current delegated credential");

        let shorter_parent = capability_until(
            &founder,
            issuer.verifying_key(),
            Some(anchor),
            triblespace_core::repo::capability::PERM_ADMIN,
            candidate_upper,
        );
        let candidate = capability_until(
            &issuer,
            subject,
            Some(shorter_parent),
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_seconds(600.0),
        )
        .0;

        assert!(
            select_cap_delivery(&mut store, root.verifying_key(), candidate, candidate_upper,)
                .is_none(),
            "a later leaf may not hide an earlier parent-authority deadline"
        );
    }

    #[test]
    fn active_cap_selection_is_monotone_under_downgrade_and_reordering() {
        let mut store = MemoryRepo::default();
        let root = SigningKey::from_bytes(&[23; 32]);
        let issuer = SigningKey::from_bytes(&[24; 32]);
        let other_issuer = SigningKey::from_bytes(&[25; 32]);
        let subject = SigningKey::from_bytes(&[26; 32]).verifying_key();
        let team_root = root.verifying_key();
        let now = crate::clock::epoch_now();
        let (anchor, (current_cap, current_sig)) = founder_credential_until(
            &root,
            &issuer,
            subject,
            triblespace_core::repo::capability::PERM_ADMIN,
            now + hifitime::Duration::from_seconds(300.0),
        );
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(anchor.0.clone())
            .unwrap();
        let current_cap_handle = store.put(current_cap.clone()).unwrap();
        let current_sig_handle = store.put(current_sig).unwrap();
        crate::policy::pin_team_cap(
            &mut store,
            team_root,
            current_cap_handle,
            current_sig_handle,
        )
        .expect("pin current cap");

        let (weaker, _) = capability_until(
            &issuer,
            subject,
            Some(anchor.clone()),
            triblespace_core::repo::capability::PERM_READ,
            now + hifitime::Duration::from_seconds(600.0),
        );
        let weaker_expiry = expiry_upper(&delivery_cap_fields(weaker.clone()).unwrap().expiry)
            .expect("weaker expiry");
        assert!(select_cap_delivery(&mut store, team_root, weaker, weaker_expiry).is_none());

        let (older, _) = capability_until(
            &issuer,
            subject,
            Some(anchor.clone()),
            triblespace_core::repo::capability::PERM_ADMIN,
            now + hifitime::Duration::from_seconds(100.0),
        );
        let older_expiry = expiry_upper(&delivery_cap_fields(older.clone()).unwrap().expiry)
            .expect("older expiry");
        assert!(select_cap_delivery(&mut store, team_root, older, older_expiry).is_none());

        let (different_issuer, _) = test_capability(
            &other_issuer,
            subject,
            triblespace_core::repo::capability::PERM_ADMIN,
            600.0,
        );
        let different_issuer_expiry = expiry_upper(
            &delivery_cap_fields(different_issuer.clone())
                .unwrap()
                .expiry,
        )
        .expect("different issuer expiry");
        assert!(
            select_cap_delivery(
                &mut store,
                team_root,
                different_issuer,
                different_issuer_expiry,
            )
            .is_none()
        );

        let (newer, newer_sig) = capability_until(
            &issuer,
            subject,
            Some(anchor),
            triblespace_core::repo::capability::PERM_ADMIN,
            now + hifitime::Duration::from_seconds(600.0),
        );
        let newer_expiry = expiry_upper(&delivery_cap_fields(newer.clone()).unwrap().expiry)
            .expect("newer expiry");
        assert!(matches!(
            select_cap_delivery(&mut store, team_root, newer.clone(), newer_expiry),
            Some(DeliverySelection {
                initial_request: None,
                expected_team_head: Some(_),
                ..
            })
        ));
        let newer_cap_handle = store.put(newer).unwrap();
        let newer_sig_handle = store.put(newer_sig).unwrap();
        crate::policy::pin_team_cap(&mut store, team_root, newer_cap_handle, newer_sig_handle)
            .expect("activate newer cap");
        let current_expiry =
            expiry_upper(&delivery_cap_fields(current_cap.clone()).unwrap().expiry)
                .expect("current expiry");
        assert!(
            select_cap_delivery(&mut store, team_root, current_cap, current_expiry).is_none(),
            "a delayed older arrival must not regress the active credential"
        );
    }
}
