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
use crate::recipient_ledger::{
    FounderGrantResolution, RecipientCredentialResolution, RecipientLedgerResolution,
    accept_credential, resolve_recipient_ledger,
};

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
/// (or chained from) their own key. Without a durable recipient-selected
/// credential it remains server-only. Multi-user setups load `team_root`
/// from `TRIBLE_TEAM_ROOT`; outbound authority is always derived from the
/// pile rather than a bearer-handle environment variable. See the
/// [Capability Auth] book chapter for the full team lifecycle.
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
    /// asserted successor credentials.
    signing_key: SigningKey,

    /// AUTH credential currently believed to be installed in the live host.
    /// `None` means the first durable reconciliation has not yet run; zero is
    /// represented explicitly as `Some([0; 32])`. Keeping this
    /// process-local observation distinct from the durable projection lets a
    /// view that becomes actionable later publish an unchanged signature, and lets a
    /// view that becomes non-authorizing withdraw stale live authority.
    host_self_cap: Option<RawHash>,

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

/// Update live host AUTH and its process-local observation as one operation.
/// All Peer-side writers use this helper so later fail-closed reconciliation
/// cannot mistake a newly installed credential for an already-withdrawn host.
fn publish_host_self_cap(sender: &NetSender, observed: &mut Option<RawHash>, self_cap: RawHash) {
    sender.update_self_cap(self_cap);
    *observed = Some(self_cap);
}

/// One fresh, complete recipient-side projection that may drive live AUTH.
///
/// Founder authority is policy-authored but recipient-selected. Ordinary
/// authority is recipient-accepted. Keeping the two variants explicit makes
/// renewal recover the founder anchor only for the former while the host sees
/// the same signature-handle effect for either.
#[derive(Clone, Debug)]
enum RecipientOperationalAuthority {
    Founder(crate::policy_ledger::CurrentGrant),
    Accepted(crate::recipient_ledger::CurrentRecipientCredential),
}

impl RecipientOperationalAuthority {
    fn cap(&self) -> Inline<Handle<SimpleArchive>> {
        match self {
            Self::Founder(credential) => credential.cap(),
            Self::Accepted(credential) => credential.cap(),
        }
    }

    fn sig(&self) -> Inline<Handle<SimpleArchive>> {
        match self {
            Self::Founder(credential) => credential.sig(),
            Self::Accepted(credential) => credential.sig(),
        }
    }

    fn capability(&self) -> &triblespace_core::repo::capability::VerifiedCapability {
        match self {
            Self::Founder(credential) => credential.capability(),
            Self::Accepted(credential) => credential.capability(),
        }
    }
}

fn project_recipient_operational_authority(
    recipient: &crate::recipient_ledger::RecipientLedgerView,
    policy: Option<&crate::policy_ledger::PolicyLedgerView>,
    author: ed25519_dalek::VerifyingKey,
    team_root: ed25519_dalek::VerifyingKey,
    now: hifitime::Epoch,
) -> Option<RecipientOperationalAuthority> {
    let accepted = match recipient.credential(team_root) {
        Some(RecipientCredentialResolution::Current { credential, .. })
            if credential.usable_at(now) =>
        {
            Some(RecipientOperationalAuthority::Accepted(credential.clone()))
        }
        Some(RecipientCredentialResolution::Unaccepted)
        | Some(RecipientCredentialResolution::Current { .. })
        | Some(RecipientCredentialResolution::Conflicted { .. })
        | None => None,
    };

    let founder = match (recipient.founder_grant(team_root), policy) {
        (Some(FounderGrantResolution::Current(selection)), Some(policy)) => {
            let grant = GrantIdentity::new(team_root, author, selection.scope_root());
            policy
                .grants()
                .get(&grant)
                .and_then(|state| state.usable_at(now))
                .cloned()
                .map(RecipientOperationalAuthority::Founder)
        }
        (Some(FounderGrantResolution::Unselected), _)
        | (Some(FounderGrantResolution::Conflicted { .. }), _)
        | (Some(FounderGrantResolution::Current(_)), None)
        | (None, _) => None,
    };

    founder.or(accepted)
}

/// Resolve live authority from one exact assertion/content boundary.
///
/// An explicit founder selection wins when its exact self grant is selected by
/// a Complete policy view and live at `now`. A stale, disabled, conflicted,
/// incomplete, or otherwise inert founder selection does not mask an
/// independently usable ordinary recipient acceptance.
fn resolve_recipient_operational_authority_from<R>(
    snapshot: &PinAssertionSnapshot,
    reader: &R,
    author: ed25519_dalek::VerifyingKey,
    team_root: ed25519_dalek::VerifyingKey,
    now: hifitime::Epoch,
    operation: &'static str,
) -> Option<RecipientOperationalAuthority>
where
    R: BlobStoreGet,
{
    let recipient = match resolve_recipient_ledger(snapshot, author, |handle| {
        reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
            .ok()
    }) {
        RecipientLedgerResolution::Complete(view) => view,
        RecipientLedgerResolution::Incomplete {
            missing,
            unknown_parents,
        } => {
            tracing::warn!(
                operation,
                missing = missing.len(),
                unknown_parents = unknown_parents.len(),
                "recipient ledger incomplete; AUTH deferred"
            );
            return None;
        }
        RecipientLedgerResolution::Invalid { diagnostics } => {
            tracing::warn!(
                operation,
                diagnostics = ?diagnostics,
                "recipient ledger invalid; AUTH deferred"
            );
            return None;
        }
    };

    let policy = if matches!(
        recipient.founder_grant(team_root),
        Some(FounderGrantResolution::Current(_))
    ) {
        match crate::policy_ledger::resolve_policy_ledger(snapshot, author, |handle| {
            reader
                .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                .ok()
        }) {
            crate::policy_ledger::PolicyLedgerResolution::Complete(view) => Some(view),
            crate::policy_ledger::PolicyLedgerResolution::Incomplete { missing } => {
                tracing::warn!(
                    operation,
                    missing = missing.len(),
                    "founder policy ledger incomplete; trying ordinary acceptance"
                );
                None
            }
            crate::policy_ledger::PolicyLedgerResolution::Invalid { diagnostics } => {
                tracing::warn!(
                    operation,
                    diagnostics = ?diagnostics,
                    "founder policy ledger invalid; trying ordinary acceptance"
                );
                None
            }
        }
    } else {
        None
    };

    project_recipient_operational_authority(&recipient, policy.as_ref(), author, team_root, now)
}

/// Convenience resolver for callers which do not also publish a serving
/// snapshot. Assertions are captured before the reader: because event writers
/// flush proof closure before assertion append, the later reader contains every
/// blob licensed by that assertion boundary (and may safely contain more).
#[cfg(test)]
fn resolve_recipient_operational_authority<S>(
    store: &mut S,
    author: ed25519_dalek::VerifyingKey,
    team_root: ed25519_dalek::VerifyingKey,
    now: hifitime::Epoch,
    operation: &'static str,
) -> Option<RecipientOperationalAuthority>
where
    S: BlobStore + PinAssertionStore,
{
    let snapshot = match store.pin_assertion_snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(
                operation,
                error = %error,
                "recipient assertion snapshot unavailable; AUTH deferred"
            );
            return None;
        }
    };
    let reader = match store.reader() {
        Ok(reader) => reader,
        Err(error) => {
            tracing::warn!(
                operation,
                error = %error,
                "recipient blob reader unavailable; AUTH deferred"
            );
            return None;
        }
    };
    resolve_recipient_operational_authority_from(
        &snapshot, &reader, author, team_root, now, operation,
    )
}

/// Resolve both author ledgers at one coherent assertion/content boundary.
/// Renewal needs their joint state: a recipient founder selector licenses one
/// exact policy grant, and neither half may come from a different snapshot.
fn resolve_complete_recipient_and_policy<S>(
    store: &mut S,
    author: ed25519_dalek::VerifyingKey,
    operation: &'static str,
) -> Option<(
    crate::recipient_ledger::RecipientLedgerView,
    crate::policy_ledger::PolicyLedgerView,
)>
where
    S: BlobStore + PinAssertionStore,
{
    let snapshot = match store.pin_assertion_snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(operation, error = %error, "joint ledger snapshot unavailable");
            return None;
        }
    };
    let reader = match store.reader() {
        Ok(reader) => reader,
        Err(error) => {
            tracing::warn!(operation, error = %error, "joint ledger reader unavailable");
            return None;
        }
    };
    let recipient = match resolve_recipient_ledger(&snapshot, author, |handle| {
        reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
            .ok()
    }) {
        RecipientLedgerResolution::Complete(view) => view,
        RecipientLedgerResolution::Incomplete {
            missing,
            unknown_parents,
        } => {
            tracing::warn!(
                operation,
                missing = missing.len(),
                unknown_parents = unknown_parents.len(),
                "recipient ledger incomplete; joint action deferred"
            );
            return None;
        }
        RecipientLedgerResolution::Invalid { diagnostics } => {
            tracing::warn!(
                operation,
                diagnostics = ?diagnostics,
                "recipient ledger invalid; joint action deferred"
            );
            return None;
        }
    };
    let policy = match crate::policy_ledger::resolve_policy_ledger(&snapshot, author, |handle| {
        reader
            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
            .ok()
    }) {
        crate::policy_ledger::PolicyLedgerResolution::Complete(view) => view,
        crate::policy_ledger::PolicyLedgerResolution::Incomplete { missing } => {
            tracing::warn!(
                operation,
                missing = missing.len(),
                "policy ledger incomplete; joint action deferred"
            );
            return None;
        }
        crate::policy_ledger::PolicyLedgerResolution::Invalid { diagnostics } => {
            tracing::warn!(
                operation,
                diagnostics = ?diagnostics,
                "policy ledger invalid; joint action deferred"
            );
            return None;
        }
    };
    Some((recipient, policy))
}

impl<S> Peer<S>
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush + Send + 'static,
{
    /// Wrap a store in a Peer. Spawns the iroh network thread
    /// internally; the thread lives for the Peer's lifetime and shuts
    /// down when the Peer drops.
    pub fn new(store: S, key: SigningKey, config: PeerConfig) -> Self {
        let team_root = config.team_root;
        let signing_key = key.clone();
        let (sender, receiver) = host::spawn(key, config);
        Self::assemble(store, sender, receiver, team_root, signing_key, None)
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
        Self::assemble(store, sender, receiver, team_root, signing_key, None)
    }

    fn assemble(
        store: S,
        sender: host::NetSender,
        receiver: host::NetReceiver,
        team_root: ed25519_dalek::VerifyingKey,
        signing_key: SigningKey,
        host_self_cap: Option<RawHash>,
    ) -> Self {
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
            host_self_cap,
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
                    ..
                } => {
                    self.absorb_cap_delivery(issuer, cap_bytes, sig_bytes, proof_blobs)?;
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

        // ── Phase 2: publish one coherent proof/AUTH boundary ─────────
        //
        // MUST happen before any announce below: peers may dial immediately
        // after DHT discovery. The authority projection is derived from the
        // exact frozen reader then installed as the serving snapshot before its
        // signature reaches the host. A snapshot failure withdraws AUTH rather
        // than publishing a handle whose proof cannot be served.
        let _ = refresh_serving_and_reconcile_authority(
            &mut *store,
            self.signing_key.verifying_key(),
            self.team_root,
            &self.sender,
            &mut self.host_self_cap,
            "refresh recipient authority reconciliation",
        );

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

    /// Admit one peer-delivered proof as a durable recipient decision.
    ///
    /// The host already verified the bounded proof bundle before emitting the
    /// event, but the semantic writer verifies it again together with the
    /// current request/credential frontier. It flushes the complete closure
    /// before appending `CredentialAccepted`. Live AUTH is never changed here:
    /// the fresh Complete reconciliation at the end of `refresh_once` is the
    /// only operational materializer.
    fn absorb_cap_delivery(
        &mut self,
        issuer: PublisherKey,
        cap_bytes: anybytes::Bytes,
        sig_bytes: anybytes::Bytes,
        proof_blobs: Vec<anybytes::Bytes>,
    ) -> Result<(), PeerRefreshError> {
        use triblespace_core::blob::Blob;

        let cap_blob: Blob<SimpleArchive> = Blob::new(cap_bytes);
        let sig_blob: Blob<SimpleArchive> = Blob::new(sig_bytes);
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
        let closure = std::iter::once(cap_blob)
            .chain(proof_blobs.into_iter().map(Blob::<SimpleArchive>::new));

        let mut store = self.store.lock().expect("store mutex");
        match accept_credential(
            &mut *store,
            &self.signing_key,
            self.team_root,
            sig_blob,
            closure,
            crate::clock::epoch_now(),
        ) {
            Ok(crate::recipient_ledger::RecipientWriteOutcome::Published(receipt)) => {
                tracing::info!(
                    issuer = %hex::encode(&issuer[..4]),
                    sig = %hex::encode(&sig_handle.raw[..4]),
                    event = %hex::encode(receipt.event().raw),
                    "CapDelivered durably accepted in recipient ledger"
                );
                Ok(())
            }
            Ok(crate::recipient_ledger::RecipientWriteOutcome::Refused(reason)) => {
                tracing::warn!(
                    issuer = %hex::encode(&issuer[..4]),
                    sig = %hex::encode(&sig_handle.raw[..4]),
                    reason = ?reason,
                    "CapDelivered refused by recipient ledger"
                );
                Ok(())
            }
            Err(error) => Err(PeerRefreshError::new("accept delivered credential", error)),
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
        if !refresh_serving_and_reconcile_authority(
            &mut *store,
            self.signing_key.verifying_key(),
            self.team_root,
            &self.sender,
            &mut self.host_self_cap,
            "redispatch recipient authority reconciliation",
        ) {
            tracing::warn!(
                pending = ready.len(),
                "redispatch_unauthenticated: coherent serving boundary unavailable; deferring"
            );
            return 0;
        }

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

    /// Run one tick of asserted credential renewal and re-dispatch.
    ///
    /// A fresh, complete locally-authored policy ledger is the sole renewal
    /// authority. Enabled historical currents are eligible even after expiry;
    /// expiry is a dispatch guard, not a reason to lose the renewal seed. Every
    /// successor is durably asserted through `issue_grant` before the one
    /// post-production redispatch pass can send it.
    ///
    /// Founder self-rotation is local. Its successor is asserted first, then a
    /// fresh selected live winner is re-resolved, exposed through a coherent
    /// serving snapshot, and finally installed in the host. No scalar pin,
    /// delivery marker, or self-directed OP_DELIVER_CAP is involved.
    ///
    /// Returns remote redispatches plus successful founder rotations. `0` on
    /// every tick after the swarm settles into steady state means the daemon is
    /// quiet.
    ///
    /// Designed to be called from `trible pile net sync`'s main loop
    /// alongside `refresh`. The 1-hour default window assumes a tick
    /// cadence well under that; tune both together for production
    /// deployments.
    pub fn renewal_tick(&mut self, renewal_window: hifitime::Duration) -> usize {
        let local_rotations = {
            let mut store = self.store.lock().expect("store mutex");
            produce_asserted_renewals(
                &mut *store,
                &self.signing_key,
                self.team_root,
                &self.sender,
                &mut self.host_self_cap,
                renewal_window,
            )
        };

        local_rotations + self.redispatch_unauthenticated()
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
        // Install one coherent proof/AUTH boundary before announcing. If the
        // boundary cannot be frozen, leave the blob unannounced: the next
        // refresh observes it from the unchanged baseline and retries. We do
        // not advance that baseline here, so a concurrent external append can
        // never be hidden by this convenience write path; at worst refresh
        // harmlessly re-announces `handle` once.
        if refresh_serving_and_reconcile_authority(
            &mut *store,
            self.signing_key.verifying_key(),
            self.team_root,
            &self.sender,
            &mut self.host_self_cap,
            "blob put recipient authority reconciliation",
        ) {
            self.sender.announce(handle.raw);
        }
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
            // Refresh both the served proof boundary and live authority after
            // every successful pin mutation, including deletion. Otherwise a
            // branch-scoped requester could keep reading through stale roots,
            // or AUTH could name proof absent from the frozen reader.
            let _ = refresh_serving_and_reconcile_authority(
                &mut *store,
                self.signing_key.verifying_key(),
                self.team_root,
                &self.sender,
                &mut self.host_self_cap,
                "pin update recipient authority reconciliation",
            );
        }
        Ok(result)
    }
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

/// Renewal scheduling deliberately retains an enabled historical Current after
/// expiry so it can seed a strictly fresher successor.
fn enabled_historical_current(
    state: &crate::policy_ledger::GrantView,
) -> Option<&crate::policy_ledger::CurrentGrant> {
    if state.disabled() {
        return None;
    }
    match state.historical_issuance() {
        crate::policy_ledger::GrantIssuanceResolution::Current(current) => Some(current),
        crate::policy_ledger::GrantIssuanceResolution::Unissued
        | crate::policy_ledger::GrantIssuanceResolution::Conflicted { .. } => None,
    }
}

/// Withdraw cached live authority. Unknown caller-managed host state is
/// withdrawn conservatively; durable ledgers remain untouched for retry.
fn withdraw_host_authority(
    sender: &NetSender,
    host_self_cap: &mut Option<RawHash>,
    reason: &'static str,
) {
    if *host_self_cap == Some([0; 32]) {
        return;
    }
    publish_host_self_cap(sender, host_self_cap, [0; 32]);
    tracing::warn!(
        reason,
        "durable projection no longer authorizes AUTH; withdrawn"
    );
}

/// Publish a coherent proof snapshot before reconciling its selected AUTH
/// handle. Returns false when no snapshot can be served, in which case AUTH is
/// withdrawn and a later refresh retries from the unchanged ledgers.
fn refresh_serving_and_reconcile_authority<S>(
    store: &mut S,
    author: ed25519_dalek::VerifyingKey,
    team_root: ed25519_dalek::VerifyingKey,
    sender: &NetSender,
    host_self_cap: &mut Option<RawHash>,
    operation: &'static str,
) -> bool
where
    S: BlobStore + PinStore + PinAssertionStore,
{
    // Assertions first, reader second: validated writers flush every licensed
    // proof blob before assertion append, so the later frozen reader contains
    // the entire chosen assertion boundary. External appends after either
    // snapshot wait for the next refresh instead of mixing two moments.
    let assertions = match store.pin_assertion_snapshot() {
        Ok(assertions) => assertions,
        Err(error) => {
            tracing::warn!(operation, error = %error, "serving assertion snapshot unavailable");
            sender.clear_snapshot();
            withdraw_host_authority(sender, host_self_cap, "serving snapshot unavailable");
            return false;
        }
    };
    let Some(snapshot) = StoreSnapshot::from_store(store) else {
        sender.clear_snapshot();
        withdraw_host_authority(sender, host_self_cap, "serving snapshot unavailable");
        return false;
    };

    let desired = resolve_recipient_operational_authority_from(
        &assertions,
        &snapshot.reader,
        author,
        team_root,
        crate::clock::epoch_now(),
        operation,
    )
    .map_or([0; 32], |authority| authority.sig().raw);
    sender.update_snapshot(snapshot);
    if *host_self_cap != Some(desired) {
        publish_host_self_cap(sender, host_self_cap, desired);
    }
    true
}

/// Produce every due asserted successor for one peer tick.
///
/// This function has no remote-send path. Its only outward host effect is the
/// ordered local founder AUTH reconciliation (or fail-closed withdrawal); the
/// caller invokes asserted redispatch exactly once after releasing the lock.
fn produce_asserted_renewals<S>(
    store: &mut S,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    sender: &NetSender,
    host_self_cap: &mut Option<RawHash>,
    renewal_window: hifitime::Duration,
) -> usize
where
    S: BlobStore + BlobStorePut + PinStore + PinAssertionStore + StorageFlush,
{
    use triblespace_core::inline::TryToInline;

    let author = signing_key.verifying_key();
    let now = crate::clock::epoch_now();
    let cutoff = now + renewal_window;
    let mut founder_rotations = 0usize;
    let Some((recipient, mut view)) = resolve_complete_recipient_and_policy(
        store,
        author,
        "renewal_tick initial joint resolution",
    ) else {
        refresh_serving_and_reconcile_authority(
            store,
            author,
            team_root,
            sender,
            host_self_cap,
            "renewal ledgers unavailable authority reconciliation",
        );
        return 0;
    };
    let selected_founder = match recipient.founder_grant(team_root) {
        Some(FounderGrantResolution::Current(selection)) => Some(GrantIdentity::new(
            team_root,
            author,
            selection.scope_root(),
        )),
        Some(FounderGrantResolution::Unselected)
        | Some(FounderGrantResolution::Conflicted { .. })
        | None => None,
    };

    // An explicit founder selector names exactly one self grant. Its enabled
    // historical Current remains a renewal seed after expiry; the terminal
    // constitutional anchor is reconstructed from the selected proof rather
    // than retained in a second mutable pin.
    if let Some(founder_grant) = selected_founder
        && let Some(previous) = view
            .grants()
            .get(&founder_grant)
            .and_then(enabled_historical_current)
            .cloned()
    {
        if previous.effective_expiry() <= cutoff {
            issue_founder_successor(
                store,
                signing_key,
                team_root,
                founder_grant,
                &previous,
                renewal_window,
            );
        }

        // Publication errors are outcome-ambiguous and concurrent assertions
        // may alter either ledger, so only one fresh joint boundary identifies
        // an operational founder rotation.
        let Some((fresh_recipient, fresh_view)) = resolve_complete_recipient_and_policy(
            store,
            author,
            "founder post-issuance joint resolution",
        ) else {
            refresh_serving_and_reconcile_authority(
                store,
                author,
                team_root,
                sender,
                host_self_cap,
                "founder post-issuance authority reconciliation",
            );
            return 0;
        };
        let still_selected = matches!(
            fresh_recipient.founder_grant(team_root),
            Some(FounderGrantResolution::Current(selection))
                if GrantIdentity::new(team_root, author, selection.scope_root()) == founder_grant
        );
        view = fresh_view;
        founder_rotations = usize::from(
            still_selected
                && view
                    .grants()
                    .get(&founder_grant)
                    .and_then(|state| state.usable_at(crate::clock::epoch_now()))
                    .is_some_and(|winner| winner.sig() != previous.sig()),
        );
    }

    if !refresh_serving_and_reconcile_authority(
        store,
        author,
        team_root,
        sender,
        host_self_cap,
        "renewal host authority reconciliation",
    ) {
        return 0;
    }

    // Re-resolve both ledgers at one boundary before selecting remote grants
    // and their parent authority.
    let Some((recipient, fresh_view)) = resolve_complete_recipient_and_policy(
        store,
        author,
        "renewal_tick ordinary joint resolution",
    ) else {
        return founder_rotations;
    };
    view = fresh_view;

    let due: Vec<_> = view
        .grants()
        .iter()
        .filter(|(grant, _)| grant.team_root() == team_root && grant.subject() != author)
        .filter_map(|(grant, state)| {
            let current = enabled_historical_current(state)?;
            (current.effective_expiry() <= cutoff).then_some((*grant, current.clone()))
        })
        .collect();
    if due.is_empty() {
        return founder_rotations;
    }

    // Parent authority is the same fresh projection used for host AUTH:
    // explicit usable founder selection first, otherwise a usable accepted
    // recipient credential. No mutable team-cap pin participates.
    let Some(parent) = project_recipient_operational_authority(
        &recipient,
        Some(&view),
        author,
        team_root,
        crate::clock::epoch_now(),
    ) else {
        tracing::warn!(
            renewable = due.len(),
            "renewal_tick: no usable recipient authority; cannot issue successors"
        );
        return founder_rotations;
    };
    let parent_cap_handle = parent.cap();
    let parent_sig_handle = parent.sig();
    let parent_expiry = parent.capability().expires_at();
    let reader = match store.reader() {
        Ok(reader) => reader,
        Err(error) => {
            tracing::warn!(error = %error, "renewal_tick: parent blob reader unavailable");
            return founder_rotations;
        }
    };
    let parent_cap = match reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_cap_handle) {
        Ok(blob) => blob,
        Err(error) => {
            tracing::warn!(error = %error, "renewal_tick: parent cap blob unavailable");
            return founder_rotations;
        }
    };
    let parent_sig = match reader.get::<Blob<SimpleArchive>, SimpleArchive>(parent_sig_handle) {
        Ok(blob) => blob,
        Err(error) => {
            tracing::warn!(error = %error, "renewal_tick: parent signature blob unavailable");
            return founder_rotations;
        }
    };
    drop(reader);

    let desired_upper = now + renewal_window * 2;
    let new_upper = desired_upper.min(parent_expiry);
    for (grant, current) in due {
        if new_upper <= current.effective_expiry() {
            // Reissuing below this parent cannot extend effective authority.
            continue;
        }
        let Ok(expiry) = (now, new_upper).try_to_inline() else {
            tracing::warn!(grant = ?grant, "renewal_tick: successor lifetime is not representable");
            continue;
        };
        let scope_facts = extract_scope_subgraph(&current.capability().cap_set, grant.scope_root());
        let (new_cap, new_sig) = match triblespace_core::repo::capability::build_capability(
            signing_key,
            grant.subject(),
            (parent_cap.clone(), parent_sig.clone()),
            grant.scope_root(),
            scope_facts,
            expiry,
        ) {
            Ok(pair) => pair,
            Err(error) => {
                tracing::warn!(
                    grant = ?grant,
                    error = ?error,
                    "renewal_tick: failed to build asserted successor"
                );
                continue;
            }
        };
        let new_sig_handle = new_sig.get_handle();
        match crate::policy_ledger::issue_grant(store, signing_key, grant, new_sig, None, [new_cap])
        {
            Ok(_) => tracing::info!(
                grant = ?grant,
                sig = %hex::encode(&new_sig_handle.raw[..4]),
                "renewal_tick: asserted successor issued"
            ),
            Err(error) => {
                // Append errors are outcome-ambiguous. Stop producing, then let
                // the caller's fresh redispatch resolution decide whether this
                // candidate became selected and therefore sendable.
                tracing::warn!(
                    grant = ?grant,
                    error = %error,
                    "renewal_tick: successor publication indeterminate; ending producer pass"
                );
                break;
            }
        }
    }

    founder_rotations
}

/// Assert one founder successor directly below the terminal non-expiring
/// anchor reconstructed from the exact selected policy proof. The validated
/// writer prospectively verifies the entire new proof before publication.
fn issue_founder_successor<S>(
    store: &mut S,
    signing_key: &SigningKey,
    team_root: ed25519_dalek::VerifyingKey,
    grant: GrantIdentity,
    previous: &crate::policy_ledger::CurrentGrant,
    renewal_window: hifitime::Duration,
) where
    S: BlobStore + PinAssertionStore + StorageFlush,
{
    use triblespace_core::inline::TryToInline;

    let reader = match store.reader() {
        Ok(reader) => reader,
        Err(error) => {
            tracing::warn!(error = %error, "founder renewal: blob reader unavailable");
            return;
        }
    };
    let reconstructed = match triblespace_core::repo::capability::verify_chain_and_reconstruct_founder_anchor_allow_expired(
        team_root,
        previous.sig(),
        signing_key.verifying_key(),
        |handle| reader.get::<Blob<SimpleArchive>, SimpleArchive>(handle).ok(),
    ) {
        Ok(verified)
            if verified.chain.leaf_cap == previous.cap()
                && verified.chain.capability.scope_root == grant.scope_root() => verified,
        Ok(_) => {
            tracing::warn!(
                grant = ?grant,
                "founder renewal: selected policy proof does not match exact grant"
            );
            return;
        }
        Err(error) => {
            tracing::warn!(
                grant = ?grant,
                error = ?error,
                "founder renewal: failed to reconstruct terminal anchor"
            );
            return;
        }
    };
    let anchor_sig = reconstructed.founder_anchor_sig;
    let Some(anchor_cap_handle) = signature_leaf_cap_handle(anchor_sig.clone()) else {
        tracing::warn!("founder renewal: reconstructed anchor has malformed leaf shape");
        return;
    };
    let anchor_cap = match reader.get::<Blob<SimpleArchive>, SimpleArchive>(anchor_cap_handle) {
        Ok(blob) => blob,
        Err(error) => {
            tracing::warn!(error = %error, "founder renewal: retained anchor capability missing");
            return;
        }
    };
    drop(reader);

    let now = crate::clock::epoch_now();
    let new_upper = now + renewal_window * 2;
    if new_upper <= previous.effective_expiry() {
        return;
    }
    let Ok(expiry) = (now, new_upper).try_to_inline() else {
        tracing::warn!("founder renewal: successor lifetime is not representable");
        return;
    };
    let scope_facts = extract_scope_subgraph(&previous.capability().cap_set, grant.scope_root());
    let (new_cap, new_sig) = match triblespace_core::repo::capability::build_capability(
        signing_key,
        signing_key.verifying_key(),
        (anchor_cap, anchor_sig),
        grant.scope_root(),
        scope_facts,
        expiry,
    ) {
        Ok(pair) => pair,
        Err(error) => {
            tracing::warn!(error = ?error, "founder renewal: failed to build anchor sibling");
            return;
        }
    };
    let new_sig_handle = new_sig.get_handle();
    match crate::policy_ledger::issue_grant(store, signing_key, grant, new_sig, None, [new_cap]) {
        Ok(_) => tracing::info!(
            grant = ?grant,
            sig = %hex::encode(&new_sig_handle.raw[..4]),
            "founder renewal: asserted anchor sibling issued"
        ),
        Err(error) => tracing::warn!(
            grant = ?grant,
            error = %error,
            "founder renewal: sibling publication indeterminate; resolving fresh"
        ),
    }
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
    use triblespace_core::inline::TryToInline;
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
        fail_snapshot: bool,
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
            if self.fail_snapshot {
                return Err(InjectedAssertionError::Injected);
            }
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

    struct RecipientDeliveryFixture {
        partial: Blob<SimpleArchive>,
        cap: Blob<SimpleArchive>,
        sig: Blob<SimpleArchive>,
        proof: Vec<Blob<SimpleArchive>>,
    }

    fn recipient_delivery_fixture(
        team_root: &SigningKey,
        issuer: &SigningKey,
        recipient: &SigningKey,
        valid_for: hifitime::Duration,
    ) -> RecipientDeliveryFixture {
        let now = crate::clock::epoch_now();
        let upper = now + valid_for;
        let expiry = (now, upper).try_to_inline().expect("delivery interval");
        let scope_root = *genid();
        let scope: TribleSet = entity! {
            ExclusiveId::force_ref(&scope_root) @
            triblespace_core::metadata::tag:
                triblespace_core::repo::capability::PERM_ADMIN,
        }
        .into();
        let partial_fragment = entity! {
            triblespace_core::repo::capability::cap_subject: recipient.verifying_key(),
            triblespace_core::repo::capability::cap_issuer: issuer.verifying_key(),
            triblespace_core::repo::capability::cap_scope_root: scope_root,
            triblespace_core::metadata::expires_at: expiry,
        };
        let mut partial_set = TribleSet::from(partial_fragment);
        partial_set += scope.clone();
        let partial = partial_set.to_blob();
        let (anchor_cap, anchor_sig) = triblespace_core::repo::capability::build_founder_anchor(
            team_root,
            issuer.verifying_key(),
            scope_root,
            scope.clone(),
        )
        .expect("delivery founder anchor");
        let (cap, sig) = triblespace_core::repo::capability::build_capability(
            issuer,
            recipient.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope,
            expiry,
        )
        .expect("delivered credential");
        RecipientDeliveryFixture {
            partial,
            cap,
            sig,
            proof: vec![anchor_cap, anchor_sig],
        }
    }

    fn publish_test_acceptance(
        store: &mut MemoryRepo,
        team_root: &SigningKey,
        recipient: &SigningKey,
        delivery: &RecipientDeliveryFixture,
    ) {
        assert!(matches!(
            crate::recipient_ledger::declare_intent(
                store,
                recipient,
                team_root.verifying_key(),
                delivery.partial.clone(),
            )
            .expect("declare test intent"),
            crate::recipient_ledger::RecipientWriteOutcome::Published(_)
        ));
        assert!(matches!(
            accept_credential(
                store,
                recipient,
                team_root.verifying_key(),
                delivery.sig.clone(),
                std::iter::once(delivery.cap.clone()).chain(delivery.proof.iter().cloned()),
                crate::clock::epoch_now(),
            )
            .expect("accept test credential"),
            crate::recipient_ledger::RecipientWriteOutcome::Published(_)
        ));
    }

    struct SnapshotSequenceStore {
        inner: MemoryRepo,
        snapshots: Vec<PinAssertionSnapshot>,
        snapshot_calls: usize,
    }

    impl BlobStorePut for SnapshotSequenceStore {
        type PutError = <MemoryRepo as BlobStorePut>::PutError;

        fn put<E, T>(&mut self, item: T) -> Result<Inline<Handle<E>>, Self::PutError>
        where
            E: BlobEncoding + 'static,
            T: IntoBlob<E>,
            Handle<E>: InlineEncoding,
        {
            self.inner.put(item)
        }
    }

    impl BlobStore for SnapshotSequenceStore {
        type Reader = MemoryReader;
        type ReaderError = <MemoryRepo as BlobStore>::ReaderError;

        fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
            self.inner.reader()
        }
    }

    impl PinAssertionStore for SnapshotSequenceStore {
        type Error = <MemoryRepo as PinAssertionStore>::Error;

        fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
            let index = self.snapshot_calls.min(self.snapshots.len() - 1);
            self.snapshot_calls += 1;
            Ok(self.snapshots[index].clone())
        }

        fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
            self.inner.append_pin_assertion(assertion)
        }
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
            fail_snapshot: false,
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
            fail_snapshot: false,
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
                fail_snapshot: false,
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
                fail_snapshot: false,
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
                fail_snapshot: false,
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
    fn startup_and_initial_refresh_reconcile_a_durable_acceptance() {
        let team_root = SigningKey::from_bytes(&[0x31; 32]);
        let issuer = SigningKey::from_bytes(&[0x32; 32]);
        let recipient = SigningKey::from_bytes(&[0x33; 32]);
        let delivery = recipient_delivery_fixture(
            &team_root,
            &issuer,
            &recipient,
            hifitime::Duration::from_hours(1.0),
        );
        let mut store = MemoryRepo::default();
        publish_test_acceptance(&mut store, &team_root, &recipient, &delivery);

        assert_eq!(
            resolve_recipient_operational_authority(
                &mut store,
                recipient.verifying_key(),
                team_root.verifying_key(),
                crate::clock::epoch_now(),
                "test startup authority resolution",
            )
            .map(|authority| authority.sig().raw),
            Some(delivery.sig.get_handle().raw),
            "startup derives AUTH only from the durable recipient projection"
        );

        // Model a crash after CredentialAccepted became durable but before the
        // host effect: caller-provided wiring starts with unknown AUTH. The
        // constructor's first level-triggered refresh must publish the winner.
        let endpoint = EndpointId::from_bytes(&recipient.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let peer = Peer::with_wiring(
            store,
            recipient,
            team_root.verifying_key(),
            sender,
            receiver,
        );
        assert_eq!(peer.host_self_cap, Some(delivery.sig.get_handle().raw));
        assert_eq!(
            *wiring.self_cap.borrow(),
            Some(delivery.sig.get_handle().raw),
            "refresh returns only after the shared live authority is visible"
        );
    }

    #[test]
    fn unavailable_serving_boundary_clears_snapshot_and_withdraws_live_auth() {
        let team_root = SigningKey::from_bytes(&[0xB1; 32]);
        let issuer = SigningKey::from_bytes(&[0xB2; 32]);
        let recipient = SigningKey::from_bytes(&[0xB3; 32]);
        let delivery = recipient_delivery_fixture(
            &team_root,
            &issuer,
            &recipient,
            hifitime::Duration::from_hours(1.0),
        );
        let mut inner = MemoryRepo::default();
        publish_test_acceptance(&mut inner, &team_root, &recipient, &delivery);
        let store = FlushProbe {
            inner,
            flushes: Arc::new(AtomicUsize::new(0)),
            fail_flush: false,
            fail_snapshot: false,
            fail_append: false,
            fail_after_append_once: false,
            read_failure: None,
        };

        let endpoint = EndpointId::from_bytes(&recipient.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            recipient,
            team_root.verifying_key(),
            sender,
            receiver,
        );
        assert_eq!(peer.host_self_cap, Some(delivery.sig.get_handle().raw));
        assert!(wiring.snapshot.lock().unwrap().is_some());
        wiring.cmd_rx.try_iter().for_each(drop);

        peer.store.lock().unwrap().fail_snapshot = true;
        peer.refresh()
            .expect("projection unavailability is retryable rather than fail-stop");

        assert_eq!(peer.host_self_cap, Some([0; 32]));
        assert!(
            wiring.snapshot.lock().unwrap().is_none(),
            "proof serving and live AUTH must fail closed together"
        );
        assert_eq!(*wiring.self_cap.borrow(), Some([0; 32]));
        assert!(wiring.cmd_rx.try_iter().any(|command| {
            matches!(
                command,
                crate::channel::NetCommand::AuthRotated {
                    predecessor,
                    successor,
                } if predecessor == delivery.sig.get_handle().raw && successor == [0; 32]
            )
        }));
    }

    #[test]
    fn delivered_credential_is_accepted_before_refresh_materializes_auth() {
        let team_root = SigningKey::from_bytes(&[0x34; 32]);
        let issuer = SigningKey::from_bytes(&[0x35; 32]);
        let recipient = SigningKey::from_bytes(&[0x36; 32]);
        let delivery = recipient_delivery_fixture(
            &team_root,
            &issuer,
            &recipient,
            hifitime::Duration::from_hours(1.0),
        );
        let mut store = MemoryRepo::default();
        assert!(matches!(
            crate::recipient_ledger::declare_intent(
                &mut store,
                &recipient,
                team_root.verifying_key(),
                delivery.partial.clone(),
            )
            .unwrap(),
            crate::recipient_ledger::RecipientWriteOutcome::Published(_)
        ));

        let endpoint = EndpointId::from_bytes(&recipient.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            recipient.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        wiring.cmd_rx.try_iter().for_each(drop);
        wiring
            .evt_tx
            .send(NetEvent::CapDelivered {
                issuer: issuer.verifying_key().to_bytes(),
                cap_bytes: delivery.cap.bytes.clone(),
                sig_bytes: delivery.sig.bytes.clone(),
                proof_blobs: delivery
                    .proof
                    .iter()
                    .map(|blob| blob.bytes.clone())
                    .collect(),
                admission: cap_delivery_admission(),
            })
            .unwrap();
        peer.refresh().expect("accept and reconcile delivery");

        let mut store = peer.store.lock().unwrap();
        let (view, _policy) = resolve_complete_recipient_and_policy(
            &mut *store,
            recipient.verifying_key(),
            "test accepted delivery",
        )
        .expect("durable accepted view");
        assert!(matches!(
            view.credential(team_root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential, .. })
                if credential.sig() == delivery.sig.get_handle()
        ));
        drop(store);
        assert_eq!(peer.host_self_cap, Some(delivery.sig.get_handle().raw));
        assert_eq!(
            *wiring.self_cap.borrow(),
            Some(delivery.sig.get_handle().raw)
        );
        assert!(wiring.cmd_rx.try_iter().any(|command| {
            matches!(
                command,
                crate::channel::NetCommand::AuthRotated {
                    predecessor,
                    successor,
                } if predecessor == [0; 32] && successor == delivery.sig.get_handle().raw
            )
        }));
    }

    #[test]
    fn one_snapshot_prevents_mixed_founder_projection_and_disabled_founder_falls_back() {
        let team_root = SigningKey::from_bytes(&[0x37; 32]);
        let issuer = SigningKey::from_bytes(&[0x38; 32]);
        let recipient = SigningKey::from_bytes(&[0x39; 32]);
        let delivery = recipient_delivery_fixture(
            &team_root,
            &issuer,
            &recipient,
            hifitime::Duration::from_hours(1.0),
        );
        let accepted_sig = delivery.sig.get_handle();
        let mut store = MemoryRepo::default();
        publish_test_acceptance(&mut store, &team_root, &recipient, &delivery);

        let founder = AssertedGrantSeries::new(&team_root, &recipient, recipient.verifying_key());
        let (_, founder_sig) = founder.issue(
            &mut store,
            &recipient,
            triblespace_core::repo::capability::PERM_ADMIN,
            crate::clock::epoch_now() + hifitime::Duration::from_hours(1.0),
        );
        assert!(matches!(
            crate::recipient_ledger::select_founder_grant(
                &mut store,
                &recipient,
                team_root.verifying_key(),
                founder.scope_root,
            )
            .unwrap(),
            crate::recipient_ledger::RecipientWriteOutcome::Published(_)
        ));

        assert!(matches!(
            resolve_recipient_operational_authority(
                &mut store,
                recipient.verifying_key(),
                team_root.verifying_key(),
                crate::clock::epoch_now(),
                "test founder priority",
            ),
            Some(RecipientOperationalAuthority::Founder(current))
                if current.sig() == founder_sig
        ));

        // Capture a boundary where both ledgers license founder authority,
        // then prove a resolver seeing only recipient assertions in its one
        // snapshot cannot accidentally fetch policy from a later snapshot.
        let full = store.pin_assertion_snapshot().unwrap();
        let policy_pin =
            crate::policy_ledger::PolicyLedgerDescriptor::pin_identity(recipient.verifying_key());
        let mut recipient_only = PinAssertionSnapshot::default();
        for assertion in full.iter() {
            if assertion.identity() != &policy_pin {
                recipient_only.insert(*assertion).unwrap();
            }
        }

        crate::policy_ledger::disable_grant(&mut store, &recipient, founder.grant).unwrap();
        assert!(matches!(
            resolve_recipient_operational_authority(
                &mut store,
                recipient.verifying_key(),
                team_root.verifying_key(),
                crate::clock::epoch_now(),
                "test disabled founder fallback",
            ),
            Some(RecipientOperationalAuthority::Accepted(current))
                if current.sig() == accepted_sig
        ));

        let mut sequenced = SnapshotSequenceStore {
            inner: store,
            snapshots: vec![recipient_only, full],
            snapshot_calls: 0,
        };
        assert!(matches!(
            resolve_recipient_operational_authority(
                &mut sequenced,
                recipient.verifying_key(),
                team_root.verifying_key(),
                crate::clock::epoch_now(),
                "test coherent boundary",
            ),
            Some(RecipientOperationalAuthority::Accepted(current))
                if current.sig() == accepted_sig
        ));
        assert_eq!(
            sequenced.snapshot_calls, 1,
            "recipient and founder policy must share one assertion snapshot"
        );
    }

    #[test]
    fn founder_renewal_reconstructs_the_terminal_anchor_from_selected_proof() {
        let team_root = SigningKey::from_bytes(&[0x3A; 32]);
        let founder_key = SigningKey::from_bytes(&[0x3B; 32]);
        let series =
            AssertedGrantSeries::new(&team_root, &founder_key, founder_key.verifying_key());
        let mut store = MemoryRepo::default();
        let (_, old_sig) = series.issue(
            &mut store,
            &founder_key,
            triblespace_core::repo::capability::PERM_ADMIN,
            crate::clock::epoch_now() + hifitime::Duration::from_seconds(1.0),
        );
        assert!(matches!(
            crate::recipient_ledger::select_founder_grant(
                &mut store,
                &founder_key,
                team_root.verifying_key(),
                series.scope_root,
            )
            .unwrap(),
            crate::recipient_ledger::RecipientWriteOutcome::Published(_)
        ));

        let endpoint = EndpointId::from_bytes(&founder_key.verifying_key().to_bytes()).unwrap();
        let (sender, receiver, _wiring) = host::wire(endpoint);
        let mut peer = Peer::with_wiring(
            store,
            founder_key.clone(),
            team_root.verifying_key(),
            sender,
            receiver,
        );
        assert_eq!(
            peer.renewal_tick(hifitime::Duration::from_seconds(120.0)),
            1
        );

        let mut store = peer.store.lock().unwrap();
        let (_recipient, policy) = resolve_complete_recipient_and_policy(
            &mut *store,
            founder_key.verifying_key(),
            "test renewed founder",
        )
        .expect("fresh complete renewed ledgers");
        let winner = policy
            .grants()
            .get(&series.grant)
            .and_then(|state| state.usable_at(crate::clock::epoch_now()))
            .expect("usable renewed founder");
        assert_ne!(winner.sig(), old_sig);
        assert_eq!(peer.host_self_cap, Some(winner.sig().raw));
        let reader = store.reader().unwrap();
        let reconstructed = triblespace_core::repo::capability::verify_chain_and_reconstruct_founder_anchor_allow_expired(
            team_root.verifying_key(),
            winner.sig(),
            founder_key.verifying_key(),
            |handle| reader.get::<Blob<SimpleArchive>, SimpleArchive>(handle).ok(),
        )
        .expect("renewed proof still reconstructs the same constitutional anchor");
        assert_eq!(
            reconstructed.founder_anchor_sig.get_handle(),
            series.anchor.1.get_handle()
        );
    }
}
