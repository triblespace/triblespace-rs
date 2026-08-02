//! Local-only policy pins: outbound join intent and per-team cap holdings.
//!
//! These pins live only in the peer's repository. They are never branch
//! authority and are not part of the network protocol: they hold typed current
//! state with no commit history. [`is_local_only_pin`] lets serving snapshots
//! exclude them from the legacy mutable-pin roots that branch-scoped
//! capabilities may traverse.
//!
//! Two roles:
//!
//!   - **`KIND_TEAM_CAP`** — one pin per team this peer is a
//!     member of, holding the peer's own current cap chain so the
//!     pile retains it across compaction (the single-slot pin
//!     mechanism from decide#5ed64e57 — overwrite on renewal, old
//!     caps auto-GC). Identified by `cap_for_team: <team_root_pubkey>`.
//!
//!   - **`KIND_OUTBOUND_CAP_REQUEST`** — the exact request this peer sent and
//!     will accept as deliberate first-credential intent, including its
//!     crash-recovery activation journal.
//!
//! All roles are marked with the same `local_only_pin` attribute (value = the
//! kind tag) so a single helper distinguishes them from legacy mutable
//! content pins.
//!
use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::prelude::attributes;
use triblespace_core::prelude::inlineencodings::{ED25519PublicKey, GenId};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, PinStore, PushResult};
use triblespace_core::trible::TribleSet;

use crate::policy_ledger::request_partial_cap;

attributes! {
    // ── Pin role markers ──────────────────────────────────────────────
    /// Tags a pin as local-only so it is excluded from legacy mutable-content
    /// scope roots. Value is one of the `KIND_*` tags below indicating the
    /// role.
    "3361F2DE0BD68BA8712EC5B9CCC7EF2A" as pub local_only_pin: GenId;

    // ── Per-team-cap pin ──────────────────────────────────────────────
    /// Names the team this pin holds cap state for. Set on the pin
    /// head metadata entity alongside `local_only_pin =
    /// KIND_TEAM_CAP`.
    "E1EE471B597A4142AD26CA1FED368D2F" as pub cap_for_team: ED25519PublicKey;

    // ── Outbound request activation journal ───────────────────────────
    /// Exact outbound request-head metadata retained while a first delivered
    /// credential is being activated. Recovery can CAS the activation journal
    /// back to this already-durable Pending head if the candidate expires
    /// before the team-cap pin is installed.
    "F4DD1EF7EEA5B600A8D947A00BEE0468" as pub request_activation_pending_head: Handle<SimpleArchive>;
    // ── Per-team-cap pin ──────────────────────────────────────────────
    /// Handle of the currently-pinned cap blob for a team. Overwritten
    /// on each renewal so old caps become unreachable.
    "A2BBD772754BBB8EAFD7479F5A1249FD" as pub team_cap_handle: Handle<SimpleArchive>;
    /// Handle of the currently-pinned sig blob for a team. Updated in
    /// lockstep with `team_cap_handle`.
    "FAC14D0CAB23B1C7AC20D8CF1C843EBF" as pub team_sig_handle: Handle<SimpleArchive>;
    /// Handle of the team's non-expiring founder-anchor signature blob. This
    /// is present only on founder credentials: it is recovery/rotation
    /// authority, never the finite operational capability presented during
    /// authentication.
    "D2052C46A40827C37C540BF70CE9FCD1" as pub team_anchor_sig_handle: Handle<SimpleArchive>;
}

// ── Pin role kind tags ────────────────────────────────────────────────

/// Pin holds A's own cap chain for a specific team. The pin head
/// metadata also carries `cap_for_team: <team_root_pubkey>` so a
/// peer with membership in multiple teams can distinguish them.
pub const KIND_TEAM_CAP: Id = triblespace_core::id::id_hex!("9BB2E5027EDB67463CC6A7A85B6C362D");

/// Pin holds the requester's one locally expected first capability delivery.
/// A verified but unsolicited cap is not sufficient authority to replace local
/// credential state.
pub const KIND_OUTBOUND_CAP_REQUEST: Id =
    triblespace_core::id::id_hex!("3951F37FBF274D5C17D3A701BC9FD7EE");

/// Canonical pin id for a singleton local policy role.
///
/// The id is intrinsic to the role marker rather than allocated by the
/// process which happens to create the pin first. Independent writers thus
/// race the same compare-and-swap slot instead of successfully creating two
/// pins for one logical singleton.
fn local_only_pin_id(kind: Id) -> Id {
    entity! { local_only_pin: kind }
        .root()
        .expect("a local-only role marker exports one intrinsic entity")
}

/// Canonical pin id for one team's active capability slot.
fn team_cap_pin_id(team_root: ed25519_dalek::VerifyingKey) -> Id {
    entity! {
        local_only_pin: KIND_TEAM_CAP,
        cap_for_team: team_root,
    }
    .root()
    .expect("a team-cap role key exports one intrinsic entity")
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Returns true if the pin's head metadata carries the `local_only_pin`
/// attribute. Serving snapshots use this to prevent renewal decisions,
/// credentials, and outbound join intent from becoming legacy content roots.
pub fn is_local_only_pin<S>(store: &mut S, pin_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(pin_id) else {
        return false;
    };
    let Ok(reader) = store.reader() else {
        return false;
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else {
        return false;
    };
    find!(
        kind: Id,
        pattern!(&meta, [{ _?e @ local_only_pin: ?kind }])
    )
    .next()
    .is_some()
}

/// Look up the local team-cap pin for a given team root pubkey,
/// if one exists. Searches by pin head metadata for
/// `local_only_pin = KIND_TEAM_CAP` + `cap_for_team =
/// team_root`. Returns the pin id (caller can fetch the head or
/// list commits as needed).
pub fn find_team_cap_pin<S>(store: &mut S, team_root: ed25519_dalek::VerifyingKey) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    use triblespace_core::inline::IntoInline;

    let pin_id = team_cap_pin_id(team_root);
    let head = store.head(pin_id).ok()??;
    let reader = store.reader().ok()?;
    let meta = reader.get::<TribleSet, SimpleArchive>(head).ok()?;
    let team_root_inline: Inline<ED25519PublicKey> = team_root.to_inline();
    let matches = find!(
        (kind: Id, team: Inline<ED25519PublicKey>),
        pattern!(&meta, [{
            _?e @
            local_only_pin: ?kind,
            cap_for_team: ?team,
        }])
    )
    .any(|(kind, team)| kind == KIND_TEAM_CAP && team.raw == team_root_inline.raw);
    if matches { Some(pin_id) } else { None }
}

/// Find the local-only pin of a given kind. Pins of singleton kinds have one
/// intrinsic id per peer repository, so lookup never depends on pin-list order.
pub fn find_local_only_pin_of_kind<S>(store: &mut S, kind: Id) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    let pin_id = local_only_pin_id(kind);
    let head = store.head(pin_id).ok()??;
    let reader = store.reader().ok()?;
    let meta = reader.get::<TribleSet, SimpleArchive>(head).ok()?;
    let matches = find!(
        k: Id,
        pattern!(&meta, [{ _?e @ local_only_pin: ?k }])
    )
    .any(|found| found == kind);
    if matches { Some(pin_id) } else { None }
}

/// Persist the partial capability this node deliberately requested. Incoming
/// first-time deliveries are activated only when they match this local intent;
/// chain validity alone does not grant a remote issuer permission to select our
/// active credential.
pub fn record_outbound_cap_request<S>(store: &mut S, partial_cap: Blob<SimpleArchive>) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    let partial_handle: Inline<Handle<SimpleArchive>> = (&partial_cap).get_handle();
    let pin_id = local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST);
    let prev_head = store.head(pin_id).ok()?;
    if let Some(head) = prev_head {
        // Refuse to overwrite malformed local intent implicitly. Pending
        // exact replay is a true no-op; every Pending replacement is a CAS
        // from the exact state observed here. Activating is a transaction
        // lock: even an apparent exact replay must wait until recovery either
        // commits or restores the retained Pending head.
        let current = outbound_cap_request_at_head(store, head)?;
        if current.activation.is_some() {
            return None;
        }
        if current.partial_cap == partial_handle {
            return Some(());
        }
    }

    let stored = store
        .put::<SimpleArchive, Blob<SimpleArchive>>(partial_cap)
        .ok()?;
    debug_assert_eq!(stored, partial_handle);
    let marker = genid();
    let metadata: TribleSet = entity! {
        ExclusiveId::force_ref(&marker) @
        local_only_pin: KIND_OUTBOUND_CAP_REQUEST,
        request_partial_cap: partial_handle,
    }
    .into();
    let new_head: Inline<Handle<SimpleArchive>> = store.put(metadata).ok()?;
    match store.update(pin_id, prev_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Candidate handles retained by an in-progress first-delivery activation.
/// The proof closure is reachable through these content-addressed blobs; the
/// preceding Pending metadata head remains reachable explicitly so an expired
/// candidate can be rolled back without reconstructing intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundActivation {
    pub candidate: TeamCredential,
    pub pending_head: Inline<Handle<SimpleArchive>>,
}

/// One exact outbound-request observation. `head` is the CAS witness used by
/// first-delivery activation and compare-and-delete; keeping it beside the
/// full phase state prevents a decision over Pending A from mutating either a
/// newly installed Pending B or an activation already being recovered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OutboundRequestState {
    pub head: Inline<Handle<SimpleArchive>>,
    pub partial_cap: Inline<Handle<SimpleArchive>>,
    pub activation: Option<OutboundActivation>,
}

fn outbound_cap_request_at_head<S>(
    store: &mut S,
    head: Inline<Handle<SimpleArchive>>,
) -> Option<OutboundRequestState>
where
    S: BlobStore,
{
    let reader = store.reader().ok()?;
    let metadata: TribleSet = reader.get(head).ok()?;
    let mut requests = find!(
        (
            entity: Id,
            kind: Id,
            handle: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&metadata, [{
            ?entity @
            local_only_pin: ?kind,
            request_partial_cap: ?handle,
        }])
    );
    let (entity, kind, partial_cap) = match (requests.next(), requests.next()) {
        (Some(request), None) => request,
        _ => return None,
    };
    if kind != KIND_OUTBOUND_CAP_REQUEST {
        return None;
    }

    let mut candidates = find!(
        (
            cap: Inline<Handle<SimpleArchive>>,
            sig: Inline<Handle<SimpleArchive>>,
            pending_head: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&metadata, [{
            entity @
            team_cap_handle: ?cap,
            team_sig_handle: ?sig,
            request_activation_pending_head: ?pending_head,
        }])
    );
    let activation = match (candidates.next(), candidates.next()) {
        (None, None) => {
            // Reject partially written/malformed activation metadata. The
            // three attributes are one indivisible phase discriminator.
            let has_cap = find!(
                cap: Inline<Handle<SimpleArchive>>,
                pattern!(&metadata, [{ entity @ team_cap_handle: ?cap }])
            )
            .next()
            .is_some();
            let has_sig = find!(
                sig: Inline<Handle<SimpleArchive>>,
                pattern!(&metadata, [{ entity @ team_sig_handle: ?sig }])
            )
            .next()
            .is_some();
            let has_pending_head = find!(
                pending_head: Inline<Handle<SimpleArchive>>,
                pattern!(&metadata, [{
                    entity @ request_activation_pending_head: ?pending_head
                }])
            )
            .next()
            .is_some();
            if has_cap || has_sig || has_pending_head {
                return None;
            }
            None
        }
        (Some((cap, sig, pending_head)), None) => Some(OutboundActivation {
            candidate: TeamCredential {
                cap,
                sig,
                founder_anchor_sig: None,
            },
            pending_head,
        }),
        _ => return None,
    };
    Some(OutboundRequestState {
        head,
        partial_cap,
        activation,
    })
}

pub(crate) fn current_outbound_cap_request_state<S>(store: &mut S) -> Option<OutboundRequestState>
where
    S: BlobStore + PinStore,
{
    let head = store
        .head(local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST))
        .ok()??;
    outbound_cap_request_at_head(store, head)
}

pub fn expected_outbound_cap_request_handle<S>(
    store: &mut S,
) -> Option<Inline<Handle<SimpleArchive>>>
where
    S: BlobStore + PinStore,
{
    current_outbound_cap_request_state(store).map(|state| state.partial_cap)
}

pub fn expected_outbound_cap_request<S>(store: &mut S) -> Option<Blob<SimpleArchive>>
where
    S: BlobStore + PinStore,
{
    let handle = expected_outbound_cap_request_handle(store)?;
    let reader = store.reader().ok()?;
    reader.get(handle).ok()
}

/// Compare-and-delete one outbound request expectation.
///
/// The exact handle is part of the mutation: a delayed response for request A
/// must never clear a newer local request B. Returns `Some(true)` when the
/// expected request was removed and `Some(false)` when it was already absent,
/// had been replaced, or lost the final head CAS. Storage failures return
/// `None`.
pub fn clear_outbound_cap_request_if<S>(
    store: &mut S,
    expected: Inline<Handle<SimpleArchive>>,
) -> Option<bool>
where
    S: BlobStore + PinStore,
{
    let Some(state) = current_outbound_cap_request_state(store) else {
        return Some(false);
    };
    if state.partial_cap != expected {
        return Some(false);
    }
    // Public rejection cleanup may delete only idle Pending intent. Once the
    // exact request owns an Activating transaction, only recovery's
    // full-state compare-and-delete may release it.
    if state.activation.is_some() {
        return Some(false);
    }
    clear_outbound_cap_request_if_state(store, state)
}

/// Delete an outbound request only if its complete observed head remains
/// current. A concurrent replacement is preserved.
pub(crate) fn clear_outbound_cap_request_if_state<S>(
    store: &mut S,
    expected: OutboundRequestState,
) -> Option<bool>
where
    S: BlobStore + PinStore,
{
    // Revalidate the witness blob itself so callers cannot manufacture a
    // `(head, handle)` pair that was never a well-formed request state.
    if outbound_cap_request_at_head(store, expected.head) != Some(expected) {
        return Some(false);
    }
    let pin_id = local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST);
    match store.update(pin_id, Some(expected.head), None).ok()? {
        PushResult::Success() => Some(true),
        PushResult::Conflict(_) => Some(false),
    }
}

/// Result of an exact outbound-journal phase transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutboundRequestCasResult {
    Success(OutboundRequestState),
    Conflict,
}

/// Atomically lock an exact Pending request to one already-persisted first
/// delivery candidate. The returned Activating head strongly retains the
/// partial request, candidate cap/signature proof, and original Pending head.
/// Callers must durably flush this transition before attempting the team-cap
/// CAS.
pub(crate) fn begin_outbound_cap_activation_if_pending<S>(
    store: &mut S,
    expected: OutboundRequestState,
    candidate: TeamCredential,
) -> Option<OutboundRequestCasResult>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    if expected.activation.is_some() || candidate.founder_anchor_sig.is_some() {
        return None;
    }
    if outbound_cap_request_at_head(store, expected.head) != Some(expected) {
        return Some(OutboundRequestCasResult::Conflict);
    }

    let marker = genid();
    let metadata: TribleSet = entity! {
        ExclusiveId::force_ref(&marker) @
        local_only_pin: KIND_OUTBOUND_CAP_REQUEST,
        request_partial_cap: expected.partial_cap,
        request_activation_pending_head: expected.head,
        team_cap_handle: candidate.cap,
        team_sig_handle: candidate.sig,
    }
    .into();
    let new_head: Inline<Handle<SimpleArchive>> = store.put(metadata).ok()?;
    let pin_id = local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST);
    match store
        .update(pin_id, Some(expected.head), Some(new_head))
        .ok()?
    {
        PushResult::Success() => Some(OutboundRequestCasResult::Success(OutboundRequestState {
            head: new_head,
            partial_cap: expected.partial_cap,
            activation: Some(OutboundActivation {
                candidate,
                pending_head: expected.head,
            }),
        })),
        PushResult::Conflict(_) => Some(OutboundRequestCasResult::Conflict),
    }
}

/// Roll an exact Activating state back to the precise Pending metadata head it
/// retained. This is used only when recovery proves that the candidate is
/// structurally valid but has expired before any team-cap activation won.
pub(crate) fn restore_outbound_cap_request_pending_if_state<S>(
    store: &mut S,
    expected: OutboundRequestState,
) -> Option<bool>
where
    S: BlobStore + PinStore,
{
    let activation = expected.activation?;
    if outbound_cap_request_at_head(store, expected.head) != Some(expected) {
        return Some(false);
    }
    let pending = outbound_cap_request_at_head(store, activation.pending_head)?;
    if pending.activation.is_some() || pending.partial_cap != expected.partial_cap {
        return None;
    }
    let pin_id = local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST);
    match store
        .update(pin_id, Some(expected.head), Some(activation.pending_head))
        .ok()?
    {
        PushResult::Success() => Some(true),
        PushResult::Conflict(_) => Some(false),
    }
}

// ── Per-team-cap pin ──────────────────────────────────────────────────

/// The complete local credential state for one team.
///
/// Every member has one finite operational `(cap, sig)` pair. The founder also
/// retains the signature of the non-expiring founder anchor so it can rotate
/// that operational credential without reusing the offline root key. The
/// anchor is deliberately not returned by [`current_team_cap`] and must never
/// be offered as an authentication leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TeamCredential {
    pub cap: Inline<Handle<SimpleArchive>>,
    pub sig: Inline<Handle<SimpleArchive>>,
    pub founder_anchor_sig: Option<Inline<Handle<SimpleArchive>>>,
}

/// One exact credential-pin observation. `head` is the compare-and-swap
/// witness that keeps a policy decision bound to the state it inspected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TeamCredentialState {
    pub head: Inline<Handle<SimpleArchive>>,
    pub credential: TeamCredential,
}

/// Result of a conditional credential activation. Storage failures remain
/// `None`; a benign concurrent winner is represented explicitly so callers
/// can drop/retry stale policy decisions without entering fail-stop mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TeamCredentialPinResult {
    Success(Id),
    Conflict,
}

fn team_credential_at_head<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    head: Inline<Handle<SimpleArchive>>,
) -> Option<TeamCredential>
where
    S: BlobStore,
{
    let reader = store.reader().ok()?;
    let meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(head).ok()?;
    let mut credentials = find!(
        (
            e: Id,
            kind: Id,
            team: ed25519_dalek::VerifyingKey,
            cap: Inline<Handle<SimpleArchive>>,
            sig: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&meta, [{
            ?e @
            local_only_pin: ?kind,
            cap_for_team: ?team,
            team_cap_handle: ?cap,
            team_sig_handle: ?sig,
        }])
    );
    let (id, kind, team, cap, sig) = match (credentials.next(), credentials.next()) {
        (Some(credential), None) => credential,
        _ => return None,
    };
    if kind != KIND_TEAM_CAP || team != team_root {
        return None;
    }
    let mut anchors = find!(
        anchor: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ id @ team_anchor_sig_handle: ?anchor }])
    );
    let founder_anchor_sig = match (anchors.next(), anchors.next()) {
        (Some(anchor), None) => Some(anchor),
        (None, None) => None,
        _ => return None,
    };
    Some(TeamCredential {
        cap,
        sig,
        founder_anchor_sig,
    })
}

/// Read the exact current team-credential state and its CAS witness.
pub(crate) fn current_team_credential_state<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<TeamCredentialState>
where
    S: BlobStore + PinStore,
{
    let head = store.head(team_cap_pin_id(team_root)).ok()??;
    let credential = team_credential_at_head(store, team_root, head)?;
    Some(TeamCredentialState { head, credential })
}

/// Install `credential` only if the team-cap pin still has the exact head on
/// which the caller based its policy decision.
pub(crate) fn pin_team_credential_if_head<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    expected_head: Option<Inline<Handle<SimpleArchive>>>,
    credential: TeamCredential,
) -> Option<TeamCredentialPinResult>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    let pin_id = team_cap_pin_id(team_root);
    // Preserve exact-replay no-op behavior without weakening the CAS witness:
    // update(old, old) refreshes external writers and conflicts if the head
    // changed, but appends nothing when it did not.
    if let Some(head) = expected_head {
        if team_credential_at_head(store, team_root, head) == Some(credential) {
            return match store.update(pin_id, Some(head), Some(head)).ok()? {
                PushResult::Success() => Some(TeamCredentialPinResult::Success(pin_id)),
                PushResult::Conflict(_) => Some(TeamCredentialPinResult::Conflict),
            };
        }
    }

    let entity_id = genid();
    let meta: TribleSet = entity! {
        ExclusiveId::force_ref(&entity_id) @
        local_only_pin: KIND_TEAM_CAP,
        cap_for_team: team_root,
        team_cap_handle: credential.cap,
        team_sig_handle: credential.sig,
        team_anchor_sig_handle?: credential.founder_anchor_sig,
    }
    .into();
    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(pin_id, expected_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(TeamCredentialPinResult::Success(pin_id)),
        PushResult::Conflict(_) => Some(TeamCredentialPinResult::Conflict),
    }
}

/// Atomically replace the complete credential for `team_root`.
///
/// Old metadata and operational blobs become unreachable from this pin after
/// replacement. A founder anchor remains reachable through
/// `founder_anchor_sig`, whose signature in turn references the anchor cap.
/// Exact replays preserve the existing head and append no metadata blob.
pub fn pin_team_credential<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    credential: TeamCredential,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let expected_head = store.head(team_cap_pin_id(team_root)).ok()?;
    if let Some(head) = expected_head {
        // Never paper over malformed current authority. Callers that intend a
        // repair need an explicit migration, not an implicit credential write.
        team_credential_at_head(store, team_root, head)?;
    }
    match pin_team_credential_if_head(store, team_root, expected_head, credential)? {
        TeamCredentialPinResult::Success(pin_id) => Some(pin_id),
        TeamCredentialPinResult::Conflict => None,
    }
}

/// Replace only the operational credential while preserving any founder
/// anchor already held in the same atomic pin.
///
/// This compatibility-shaped helper is also the ordinary-member write path:
/// it cannot accidentally discard founder rotation authority.
pub fn pin_team_cap<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let expected_head = store.head(team_cap_pin_id(team_root)).ok()?;
    let founder_anchor_sig = match expected_head {
        Some(head) => team_credential_at_head(store, team_root, head)?.founder_anchor_sig,
        None => None,
    };
    match pin_team_credential_if_head(
        store,
        team_root,
        expected_head,
        TeamCredential {
            cap,
            sig,
            founder_anchor_sig,
        },
    )? {
        TeamCredentialPinResult::Success(pin_id) => Some(pin_id),
        TeamCredentialPinResult::Conflict => None,
    }
}

/// Read the complete currently-pinned credential for a team.
pub fn current_team_credential<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<TeamCredential>
where
    S: BlobStore + PinStore,
{
    current_team_credential_state(store, team_root).map(|state| state.credential)
}

/// Read only the finite operational `(cap, sig)` pair used by authentication.
/// Founder rotation authority is intentionally not exposed through this API.
pub fn current_team_cap<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<(Inline<Handle<SimpleArchive>>, Inline<Handle<SimpleArchive>>)>
where
    S: BlobStore + PinStore,
{
    current_team_credential(store, team_root).map(|credential| (credential.cap, credential.sig))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::trible::TribleSet;

    fn distinct_partial_cap(subject: ed25519_dalek::VerifyingKey) -> Blob<SimpleArchive> {
        use triblespace_core::id::ExclusiveId;

        let entity_id = genid();
        let set: TribleSet = entity! {
            ExclusiveId::force_ref(&entity_id) @
            triblespace_core::repo::capability::cap_subject: subject,
        }
        .into();
        {
            use triblespace_core::blob::IntoBlob;
            set.to_blob()
        }
    }

    fn key_for(index: usize) -> ed25519_dalek::VerifyingKey {
        let mut seed = [0_u8; 32];
        seed[..8].copy_from_slice(&(index as u64 + 1).to_le_bytes());
        SigningKey::from_bytes(&seed).verifying_key()
    }

    #[test]
    fn policy_pin_ids_are_intrinsic_and_keyed_by_role() {
        let first_team = key_for(40);
        let second_team = key_for(41);
        let first_team_pin = team_cap_pin_id(first_team);
        assert_eq!(first_team_pin, team_cap_pin_id(first_team));
        assert_ne!(first_team_pin, team_cap_pin_id(second_team));
        assert_ne!(first_team_pin, local_only_pin_id(KIND_TEAM_CAP));
    }

    #[test]
    fn outbound_request_and_team_cap_exact_replays_preserve_heads() {
        let mut store = MemoryRepo::default();
        let partial = distinct_partial_cap(key_for(0));
        let partial_handle = partial.get_handle();
        record_outbound_cap_request(&mut store, partial.clone()).expect("record expectation");
        let request_pin = find_local_only_pin_of_kind(&mut store, KIND_OUTBOUND_CAP_REQUEST)
            .expect("request pin");
        let request_head = store.head(request_pin).unwrap();
        record_outbound_cap_request(&mut store, partial).expect("exact replay");
        assert_eq!(store.head(request_pin).unwrap(), request_head);
        assert_eq!(
            expected_outbound_cap_request_handle(&mut store),
            Some(partial_handle)
        );

        let team = key_for(1);
        let cap = Inline::new([0xCA; 32]);
        let sig = Inline::new([0x51; 32]);
        let team_pin = pin_team_cap(&mut store, team, cap, sig).expect("pin team cap");
        let team_head = store.head(team_pin).unwrap();
        assert_eq!(pin_team_cap(&mut store, team, cap, sig), Some(team_pin));
        assert_eq!(store.head(team_pin).unwrap(), team_head);

        assert_eq!(
            clear_outbound_cap_request_if(&mut store, partial_handle),
            Some(true)
        );
        assert!(expected_outbound_cap_request_handle(&mut store).is_none());
    }

    #[test]
    fn founder_credential_roundtrips_and_operational_rotation_preserves_anchor() {
        let mut store = MemoryRepo::default();
        let team = key_for(2);
        let credential = TeamCredential {
            cap: Inline::new([0xC1; 32]),
            sig: Inline::new([0x51; 32]),
            founder_anchor_sig: Some(Inline::new([0xA1; 32])),
        };

        let pin = pin_team_credential(&mut store, team, credential).expect("pin credential");
        assert_eq!(current_team_credential(&mut store, team), Some(credential));
        assert_eq!(
            current_team_cap(&mut store, team),
            Some((credential.cap, credential.sig))
        );

        let exact_head = store.head(pin).unwrap();
        assert_eq!(pin_team_credential(&mut store, team, credential), Some(pin));
        assert_eq!(store.head(pin).unwrap(), exact_head);

        let next_cap = Inline::new([0xC2; 32]);
        let next_sig = Inline::new([0x52; 32]);
        assert_eq!(
            pin_team_cap(&mut store, team, next_cap, next_sig),
            Some(pin)
        );
        assert_eq!(
            current_team_credential(&mut store, team),
            Some(TeamCredential {
                cap: next_cap,
                sig: next_sig,
                founder_anchor_sig: credential.founder_anchor_sig,
            })
        );
    }

    #[test]
    fn stale_credential_decision_cannot_overwrite_a_concurrent_winner() {
        let mut store = MemoryRepo::default();
        let team = key_for(7);
        let first = TeamCredential {
            cap: Inline::new([0x11; 32]),
            sig: Inline::new([0x12; 32]),
            founder_anchor_sig: Some(Inline::new([0x13; 32])),
        };
        pin_team_credential(&mut store, team, first).expect("pin first credential");
        let stale = current_team_credential_state(&mut store, team).expect("observe first head");

        let winner = TeamCredential {
            cap: Inline::new([0x21; 32]),
            sig: Inline::new([0x22; 32]),
            founder_anchor_sig: Some(Inline::new([0x23; 32])),
        };
        pin_team_credential(&mut store, team, winner).expect("concurrent winner");
        let delayed = TeamCredential {
            cap: Inline::new([0x31; 32]),
            sig: Inline::new([0x32; 32]),
            founder_anchor_sig: stale.credential.founder_anchor_sig,
        };

        assert_eq!(
            pin_team_credential_if_head(&mut store, team, Some(stale.head), delayed),
            Some(TeamCredentialPinResult::Conflict)
        );
        assert_eq!(
            current_team_credential(&mut store, team),
            Some(winner),
            "the stale decision must neither regress authority nor restore its old anchor"
        );
    }

    #[test]
    fn ambiguous_team_credential_metadata_is_not_selected() {
        use triblespace_core::id::ExclusiveId;

        let mut store = MemoryRepo::default();
        let team = key_for(5);
        let first = genid();
        let second = genid();
        let mut meta: TribleSet = entity! {
            ExclusiveId::force_ref(&first) @
            local_only_pin: KIND_TEAM_CAP,
            cap_for_team: team,
            team_cap_handle: Inline::new([0x11; 32]),
            team_sig_handle: Inline::new([0x12; 32]),
        }
        .into();
        meta += TribleSet::from(entity! {
            ExclusiveId::force_ref(&second) @
            local_only_pin: KIND_TEAM_CAP,
            cap_for_team: team,
            team_cap_handle: Inline::new([0x21; 32]),
            team_sig_handle: Inline::new([0x22; 32]),
        });
        let head = store
            .put(meta)
            .expect("store malformed credential metadata");
        assert!(matches!(
            store.update(team_cap_pin_id(team), None, Some(head)),
            Ok(PushResult::Success())
        ));

        assert!(current_team_credential(&mut store, team).is_none());
        assert!(current_team_cap(&mut store, team).is_none());
    }

    #[test]
    fn delayed_request_response_cannot_clear_newer_local_intent() {
        let mut store = MemoryRepo::default();
        let first = distinct_partial_cap(key_for(3));
        let first_handle = first.get_handle();
        let second = distinct_partial_cap(key_for(4));
        let second_handle = second.get_handle();

        record_outbound_cap_request(&mut store, first).expect("record first request");
        record_outbound_cap_request(&mut store, second).expect("replace with newer intent");

        assert_eq!(
            clear_outbound_cap_request_if(&mut store, first_handle),
            Some(false),
            "a delayed response may clear only the exact request it belongs to"
        );
        assert_eq!(
            expected_outbound_cap_request_handle(&mut store),
            Some(second_handle)
        );
        assert_eq!(
            clear_outbound_cap_request_if(&mut store, second_handle),
            Some(true)
        );
        assert!(expected_outbound_cap_request_handle(&mut store).is_none());
    }
}
