//! Local-only policy pins: renewal state, outbound join intent, and per-team
//! cap holdings.
//!
//! These pins live only in the peer's repository. They are never branch
//! authority and are not part of the network protocol: they hold typed current
//! state with no commit history. [`is_local_only_pin`] lets serving snapshots
//! exclude them from the legacy mutable-pin roots that branch-scoped
//! capabilities may traverse.
//!
//! Three roles:
//!
//!   - **`KIND_RENEWAL_POLICY`** — A's per-issuer view: "I am willing
//!     to auto-renew these (subject, scope) pairs; here's the latest
//!     cap I issued to each; here are the ones I've retracted." The
//!     auto-renewal daemon scans this pin each tick.
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
//! See `decide#4b59ce27` (daemon + local-only retraction policy) for
//! the design rationale.

use triblespace_core::blob::Blob;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::{Id, genid};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::prelude::attributes;
use triblespace_core::prelude::inlineencodings::{ED25519PublicKey, GenId};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BlobStorePut, PinStore, PushResult};
use triblespace_core::trible::TribleSet;

use crate::policy_ledger::{policy_scope, policy_subject, request_partial_cap};

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

    // ── Legacy renewal policy entry ───────────────────────────────────
    /// Effective validity interval of the most recently signed capability
    /// chain. Its upper bound is the earliest expiry anywhere in the chain,
    /// not merely the leaf capability's expiry. The daemon's "near expiry?"
    /// check compares `now + renewal_window` against that upper bound.
    "AEF94EAB060C3D78AE373715885897C0" as pub policy_effective_expiry: NsTAIInterval;
    /// Handle of the most recent cap blob A signed for this entry.
    "BF6B9C894E3CA2AB5FBCC12B925C9680" as pub policy_latest_cap: Handle<SimpleArchive>;
    /// Handle of the most recent sig blob accompanying the cap above.
    "5A72B59BF016C7024385B6976BD8AD0E" as pub policy_latest_sig: Handle<SimpleArchive>;
    /// Set when A has chosen to stop auto-renewing this entry. The
    /// daemon skips entries with this attribute; the corresponding
    /// peer's chain dies naturally at the current cap's expiry.
    "57C45D022B79C4D3A021AC0114D973EE" as pub policy_retracted_at: NsTAIInterval;
    /// Legacy positive-delivery marker for the cap/signature named by this
    /// entry. Cleared when that legacy entry is re-signed. Asserted-policy
    /// redispatch derives authentication from `CredentialAuthenticated`
    /// events and deliberately does not consult this attribute.
    "2E289E766CFD4F2554D430C31337BE2B" as pub policy_delivered_at: NsTAIInterval;

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

/// Pin holds A's renewal policy state. Each entity on the pin head
/// metadata blob is one `(policy_subject, policy_scope)` pair with
/// associated cap + sig handles and an optional retraction timestamp.
pub const KIND_RENEWAL_POLICY: Id =
    triblespace_core::id::id_hex!("914CFF7C82FDE32CB84D85CE98613E62");

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

/// Find the local-only pin of a given kind (e.g. `KIND_RENEWAL_POLICY` or
/// `KIND_OUTBOUND_CAP_REQUEST`). Pins of these kinds have one intrinsic id per
/// peer repository, so lookup never depends on pin-list order.
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

/// A single renewal-policy entry as recorded on the renewal-policy
/// pin. The auto-renewal daemon enumerates these and re-issues a
/// fresh cap for any whose `effective_expiry` upper bound is within the
/// configured renewal window of `now` AND that don't carry a
/// `retracted_at` attribute.
pub struct PolicyEntry {
    pub id: Id,
    pub subject: ed25519_dalek::VerifyingKey,
    pub scope: Id,
    pub effective_expiry: Inline<NsTAIInterval>,
    pub latest_cap: Inline<Handle<SimpleArchive>>,
    pub latest_sig: Inline<Handle<SimpleArchive>>,
    /// `Some(t)` if A has chosen to stop auto-renewing this entry;
    /// the daemon must skip entries with this set.
    pub retracted_at: Option<Inline<NsTAIInterval>>,
    /// Legacy positive-delivery marker for the current cap/signature. The
    /// asserted-policy redispatch consumer does not consult this field.
    pub delivered_at: Option<Inline<NsTAIInterval>>,
}

/// Enumerate the current renewal-policy entries.
///
/// Includes retracted entries (with `retracted_at` populated) so
/// callers can render the full audit view; the daemon's renewal
/// loop filters them out at action time.
pub fn list_renewal_policy<S>(store: &mut S) -> Vec<PolicyEntry>
where
    S: BlobStore + PinStore,
{
    let Some(pin_id) = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY) else {
        return Vec::new();
    };
    let Ok(Some(head)) = store.head(pin_id) else {
        return Vec::new();
    };
    let Ok(reader) = store.reader() else {
        return Vec::new();
    };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head) else {
        return Vec::new();
    };

    // Required fields (effective expiry, latest cap/sig, subject, scope).
    let core: Vec<(
        Id,
        ed25519_dalek::VerifyingKey,
        Id,
        Inline<NsTAIInterval>,
        Inline<Handle<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
    )> = find!(
        (
            e: Id,
            subject: ed25519_dalek::VerifyingKey,
            scope: Id,
            effective_expiry: Inline<NsTAIInterval>,
            cap: Inline<Handle<SimpleArchive>>,
            sig: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&meta, [{
            ?e @
            policy_subject: ?subject,
            policy_scope: ?scope,
            policy_effective_expiry: ?effective_expiry,
            policy_latest_cap: ?cap,
            policy_latest_sig: ?sig,
        }])
    )
    .collect();

    // Optional retracted_at / delivered_at lookups per entry
    // (separate queries — keeping either in the main pattern would
    // filter out entries that lack the optional attribute, which is
    // the opposite of what we want).
    core.into_iter()
        .map(
            |(id, subject, scope, effective_expiry, latest_cap, latest_sig)| {
                let retracted_at = find!(
                    t: Inline<NsTAIInterval>,
                    pattern!(&meta, [{ id @ policy_retracted_at: ?t }])
                )
                .next();
                let delivered_at = find!(
                    t: Inline<NsTAIInterval>,
                    pattern!(&meta, [{ id @ policy_delivered_at: ?t }])
                )
                .next();
                PolicyEntry {
                    id,
                    subject,
                    scope,
                    effective_expiry,
                    latest_cap,
                    latest_sig,
                    retracted_at,
                    delivered_at,
                }
            },
        )
        .collect()
}

/// Filter `list_renewal_policy` to entries that are due for renewal:
/// not retracted, and the upper bound of their `effective_expiry` interval
/// falls within `renewal_window` of `now`. This schedules renewal from the
/// whole capability chain's authority lifetime rather than the leaf alone.
///
/// The daemon's typical call: `renewable_within(store,
/// Duration::from_secs(3600))` → entries whose current cap expires
/// in the next hour or already has. The window should be > the
/// daemon's tick cadence so a renewal isn't missed across one
/// missed tick.
pub fn renewable_within<S>(store: &mut S, renewal_window: hifitime::Duration) -> Vec<PolicyEntry>
where
    S: BlobStore + PinStore,
{
    let now = crate::clock::epoch_now();
    let cutoff = now + renewal_window;
    list_renewal_policy(store)
        .into_iter()
        .filter(|e| e.retracted_at.is_none())
        .filter(|e| {
            use triblespace_core::inline::TryFromInline;
            match <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&e.effective_expiry) {
                // The current chain's effective upper bound has already passed
                // `cutoff` — i.e. it expires sooner than the renewal
                // window says we want, so it's due.
                Ok((_lower, upper)) => upper <= cutoff,
                // A malformed interval treats as overdue (defensive —
                // re-issuing repairs the entry).
                Err(_) => true,
            }
        })
        .collect()
}

// ── Renewal-policy entry writes ───────────────────────────────────────

/// Insert (or refresh) a renewal-policy entry. Find-or-create the
/// renewal-policy pin on first call.
///
/// The entity id is fresh on each call — policy entries are keyed by
/// `(subject, scope)`, not by their generated entity id, and the
/// daemon's renewable-scan recomputes from the effective-expiry field rather
/// than relying on entity stability. If an entry for the same
/// `(subject, scope)` already exists, the caller should remove or
/// supersede it before adding the new one (typically via the
/// `update_policy_entry` helper below, which rewrites the effective expiry
/// + handles in place).
///
/// Returns the new entry's entity id.
///
/// Idempotent on `(subject, scope)`: if an *active* (non-retracted)
/// entry already exists for the same pair, returns that entry's id
/// without minting a duplicate. This handles the
/// killed-approve-then-retry case (the killed CLI's writes are
/// durable, the retry would otherwise create a phantom-twin entry
/// that the renewal daemon would dispatch in parallel with the
/// original — wasted wire bytes, no correctness benefit). Genuine
/// re-issuance with a fresh cap+sig should go through
/// [`update_policy_entry`] instead, which rewrites in place.
pub fn record_policy_entry<S>(
    store: &mut S,
    subject: ed25519_dalek::VerifyingKey,
    scope: Id,
    effective_expiry: Inline<NsTAIInterval>,
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    // Idempotent guard: if an active entry for this (subject, scope)
    // already exists, return its id rather than minting a duplicate.
    if let Some(existing) = list_renewal_policy(store)
        .into_iter()
        .find(|e| e.retracted_at.is_none() && e.subject == subject && e.scope == scope)
    {
        return Some(existing.id);
    }

    let (pin_id, prev_head) = match find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY) {
        Some(pin_id) => (pin_id, store.head(pin_id).ok().flatten()),
        None => (local_only_pin_id(KIND_RENEWAL_POLICY), None),
    };

    let mut meta: TribleSet = match &prev_head {
        Some(h) => {
            let reader = store.reader().ok()?;
            reader.get::<TribleSet, SimpleArchive>(*h).ok()?
        }
        None => {
            let marker_id = genid();
            entity! { ExclusiveId::force_ref(&marker_id) @
                local_only_pin: KIND_RENEWAL_POLICY,
            }
            .into()
        }
    };

    let entity_id = genid();
    let entry_set: TribleSet = entity! {
        ExclusiveId::force_ref(&entity_id) @
        policy_subject: subject,
        policy_scope: scope,
        policy_effective_expiry: effective_expiry,
        policy_latest_cap: cap,
        policy_latest_sig: sig,
    }
    .into();
    meta += entry_set;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(pin_id, prev_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(*entity_id),
        PushResult::Conflict(_) => None,
    }
}

/// Update an existing renewal-policy entry in place: rewrite its
/// `policy_effective_expiry`, `policy_latest_cap`, and `policy_latest_sig`
/// tribles. Called by the renewal daemon after each successful
/// re-sign + dispatch.
///
/// The `(subject, scope)` keys remain stable; only the time and
/// handle fields change.
pub fn update_policy_entry<S>(
    store: &mut S,
    entry_id: Id,
    new_effective_expiry: Inline<NsTAIInterval>,
    new_cap: Inline<Handle<SimpleArchive>>,
    new_sig: Inline<Handle<SimpleArchive>>,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    let pin_id = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(pin_id).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    // Remove the three existing tribles we're replacing.
    let cur_effective_expiry: Option<Inline<NsTAIInterval>> = find!(
        t: Inline<NsTAIInterval>,
        pattern!(&meta, [{ entry_id @ policy_effective_expiry: ?t }])
    )
    .next();
    let cur_cap: Option<Inline<Handle<SimpleArchive>>> = find!(
        h: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ entry_id @ policy_latest_cap: ?h }])
    )
    .next();
    let cur_sig: Option<Inline<Handle<SimpleArchive>>> = find!(
        h: Inline<Handle<SimpleArchive>>,
        pattern!(&meta, [{ entry_id @ policy_latest_sig: ?h }])
    )
    .next();

    if let Some(old) = cur_effective_expiry {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_effective_expiry: old,
        }
        .into();
        meta = meta.difference(&t);
    }
    if let Some(old) = cur_cap {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_latest_cap: old,
        }
        .into();
        meta = meta.difference(&t);
    }
    if let Some(old) = cur_sig {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_latest_sig: old,
        }
        .into();
        meta = meta.difference(&t);
    }

    // Re-signing supersedes the prior cap, so its legacy positive-delivery
    // marker no longer describes the entry's current signature.
    let cur_delivered_at: Option<Inline<NsTAIInterval>> = find!(
        t: Inline<NsTAIInterval>,
        pattern!(&meta, [{ entry_id @ policy_delivered_at: ?t }])
    )
    .next();
    if let Some(old) = cur_delivered_at {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_delivered_at: old,
        }
        .into();
        meta = meta.difference(&t);
    }

    let new_tribles: TribleSet = entity! {
        ExclusiveId::force_ref(&entry_id) @
        policy_effective_expiry: new_effective_expiry,
        policy_latest_cap: new_cap,
        policy_latest_sig: new_sig,
    }
    .into();
    meta += new_tribles;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(pin_id, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Mark a legacy renewal-policy entry as delivered (`policy_delivered_at =
/// now`). Retained while remaining local writers migrate to asserted policy;
/// asserted-policy redispatch does not consult this marker.
pub fn mark_policy_delivered<S>(store: &mut S, entry_id: Id) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::inline::TryToInline;

    let pin_id = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(pin_id).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    // Repeating the exact legacy mark is a true no-op. A fresh re-sign removes
    // this field in `update_policy_entry`, so an existing value already marks
    // the current `(subject, latest_sig)` selected by the caller.
    let cur: Option<Inline<NsTAIInterval>> = find!(
        t: Inline<NsTAIInterval>,
        pattern!(&meta, [{ entry_id @ policy_delivered_at: ?t }])
    )
    .next();
    if cur.is_some() {
        return Some(());
    }

    let now = crate::clock::epoch_now();
    let delivered_at: Inline<NsTAIInterval> = (now, now).try_to_inline().ok()?;

    let trible: TribleSet = entity! {
        ExclusiveId::force_ref(&entry_id) @
        policy_delivered_at: delivered_at,
    }
    .into();
    meta += trible;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(pin_id, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Mark a renewal-policy entry as retracted (sets `policy_retracted_at
/// = now`). The daemon's `renewable_within` filter then skips it on
/// subsequent ticks; the corresponding peer's chain dies naturally at
/// the current cap's expiry.
pub fn retract_policy_entry<S>(store: &mut S, entry_id: Id) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::inline::TryToInline;

    let pin_id = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(pin_id).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    let now = crate::clock::epoch_now();
    let retracted_at: Inline<NsTAIInterval> = (now, now).try_to_inline().ok()?;

    let trible: TribleSet = entity! {
        ExclusiveId::force_ref(&entry_id) @
        policy_retracted_at: retracted_at,
    }
    .into();
    meta += trible;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(pin_id, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::blob::{Blob, IntoBlob};
    use triblespace_core::inline::TryToInline;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::trible::TribleSet;

    fn point_now() -> Inline<NsTAIInterval> {
        let now = hifitime::Epoch::now().expect("system time");
        (now, now).try_to_inline().expect("point interval")
    }

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
        assert_ne!(
            local_only_pin_id(KIND_RENEWAL_POLICY),
            local_only_pin_id(KIND_OUTBOUND_CAP_REQUEST)
        );

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

    #[test]
    fn renewal_policy_is_scheduled_by_effective_chain_expiry() {
        let mut store = MemoryRepo::default();
        let subject = key_for(5);
        let due_scope = *genid();
        let later_scope = *genid();
        let now = crate::clock::epoch_now();
        let due_effective_expiry = (now, now + hifitime::Duration::from_seconds(30.0))
            .try_to_inline()
            .expect("due effective interval");
        let later_effective_expiry = (now, now + hifitime::Duration::from_days(1.0))
            .try_to_inline()
            .expect("later effective interval");

        let due_id = record_policy_entry(
            &mut store,
            subject,
            due_scope,
            due_effective_expiry,
            Inline::new([0xD0; 32]),
            Inline::new([0xD1; 32]),
        )
        .expect("record due policy");
        record_policy_entry(
            &mut store,
            subject,
            later_scope,
            later_effective_expiry,
            Inline::new([0xE0; 32]),
            Inline::new([0xE1; 32]),
        )
        .expect("record later policy");

        let listed = list_renewal_policy(&mut store);
        assert_eq!(listed.len(), 2);
        assert_eq!(
            listed
                .iter()
                .find(|entry| entry.id == due_id)
                .expect("due entry")
                .effective_expiry,
            due_effective_expiry
        );

        let renewable = renewable_within(&mut store, hifitime::Duration::from_hours(1.0));
        assert_eq!(renewable.len(), 1);
        assert_eq!(renewable[0].id, due_id);
    }

    #[test]
    fn repeated_mark_policy_delivered_is_a_head_preserving_noop() {
        let mut store = MemoryRepo::default();
        let subject = key_for(2);
        let scope = *genid();
        let cap = Inline::new([0xC0; 32]);
        let sig = Inline::new([0x52; 32]);
        let entry = record_policy_entry(&mut store, subject, scope, point_now(), cap, sig)
            .expect("record policy");
        mark_policy_delivered(&mut store, entry).expect("first confirmation");
        let pin = find_local_only_pin_of_kind(&mut store, KIND_RENEWAL_POLICY).expect("policy pin");
        let confirmed_head = store.head(pin).unwrap();
        mark_policy_delivered(&mut store, entry).expect("confirmation replay");
        assert_eq!(store.head(pin).unwrap(), confirmed_head);
    }
}
