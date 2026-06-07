//! Local-only policy pins: renewal state, pending join requests, and
//! per-team cap holdings.
//!
//! These pins live on the peer's pile but are **not** gossiped —
//! the implementation here mirrors the tracking-pin pattern from
//! `crate::tracking`. The `is_local_only_pin` check is consulted by
//! the gossip-publish loop in `Peer::refresh` to skip them. Per the
//! Pin/Branch taxonomy (decide#6de2dd95): these are pins, not
//! branches — they hold typed bags of entities with no commit
//! history.
//!
//! Three roles:
//!
//!   - **`KIND_RENEWAL_POLICY`** — A's per-issuer view: "I am willing
//!     to auto-renew these (subject, scope) pairs; here's the latest
//!     cap I issued to each; here are the ones I've retracted." The
//!     auto-renewal daemon scans this pin each tick.
//!
//!   - **`KIND_PENDING_REQUESTS`** — incoming `OP_REQUEST_CAP` payloads
//!     waiting for human approval (or auto-approval if the requester
//!     matches an existing renewal-policy entry). The CLI's
//!     `team list-pending` reads this pin; `team approve` mutates
//!     status entries on it.
//!
//!   - **`KIND_TEAM_CAP`** — one pin per team this peer is a
//!     member of, holding the peer's own current cap chain so the
//!     pile retains it across compaction (the single-slot pin
//!     mechanism from decide#5ed64e57 — overwrite on renewal, old
//!     caps auto-GC). Identified by `cap_for_team: <team_root_pubkey>`.
//!
//! All three are marked with the same `local_only_pin` attribute
//! (value = the kind tag) so a single helper distinguishes them from
//! gossipable team-data branches.
//!
//! See `decide#4b59ce27` (daemon + local-only retraction policy) for
//! the design rationale.

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

attributes! {
    // ── Pin role markers ──────────────────────────────────────────────
    /// Tags a pin as local-only (skip in gossip publish, skip in
    /// content-branch-name lookups). Value is one of the `KIND_*` tags
    /// below indicating the role.
    "3361F2DE0BD68BA8712EC5B9CCC7EF2A" as pub local_only_pin: GenId;

    // ── Per-team-cap pin ──────────────────────────────────────────────
    /// Names the team this pin holds cap state for. Set on the pin
    /// head metadata entity alongside `local_only_pin =
    /// KIND_TEAM_CAP`.
    "E1EE471B597A4142AD26CA1FED368D2F" as pub cap_for_team: ED25519PublicKey;

    // ── Renewal policy entry ──────────────────────────────────────────
    /// The pubkey this entry is willing to auto-renew (the subject of
    /// caps we'll keep issuing).
    "384D8A994AF026BBD1329CAD7041E3B8" as pub policy_subject: ED25519PublicKey;
    /// The scope-root id the renewal covers. Multiple entries with the
    /// same `policy_subject` but different `policy_scope` model
    /// per-scope approval/retraction independently (A can retract B's
    /// WRITE without touching B's READ).
    "D67D3CB1562B27504892BF0ACB55EA8B" as pub policy_scope: GenId;
    /// Interval covered by the most recently signed cap. The
    /// daemon's "near expiry?" check compares `now + renewal_window`
    /// against the upper bound of this interval.
    "AEF94EAB060C3D78AE373715885897C0" as pub policy_issued_at: NsTAIInterval;
    /// Handle of the most recent cap blob A signed for this entry.
    "BF6B9C894E3CA2AB5FBCC12B925C9680" as pub policy_latest_cap: Handle<SimpleArchive>;
    /// Handle of the most recent sig blob accompanying the cap above.
    "5A72B59BF016C7024385B6976BD8AD0E" as pub policy_latest_sig: Handle<SimpleArchive>;
    /// Set when A has chosen to stop auto-renewing this entry. The
    /// daemon skips entries with this attribute; the corresponding
    /// peer's chain dies naturally at the current cap's expiry.
    "57C45D022B79C4D3A021AC0114D973EE" as pub policy_retracted_at: NsTAIInterval;
    /// Set when the most recently dispatched `OP_DELIVER_CAP` to the
    /// subject returned a STATUS_OK ack — i.e. the subject's daemon
    /// confirmed receipt of `policy_latest_cap` / `policy_latest_sig`.
    /// Cleared (the attribute removed) every time we re-sign the cap
    /// (on a renewal tick), so the next dispatch round resumes
    /// retry-until-ack until the new cap also lands.
    ///
    /// The daemon's tick treats entries without this attribute as
    /// "still pending delivery" and re-dispatches them (rate-limited
    /// via an in-memory per-entry cooldown so a peer that's
    /// persistently unreachable doesn't get hammered).
    "2E289E766CFD4F2554D430C31337BE2B" as pub policy_delivered_at: NsTAIInterval;

    // ── Pending request entry ─────────────────────────────────────────
    /// The pubkey that sent the join request. Matches the iroh
    /// connection's `remote_id` at the time of receipt.
    "3583BC29C2155717639FA7E9314CC8B9" as pub request_requester: ED25519PublicKey;
    /// Handle of the partial cap blob the requester sent.
    "42903FA16A2913144A48072F575BB304" as pub request_partial_cap: Handle<SimpleArchive>;
    /// Wall-clock instant the request arrived (point interval).
    "8CC3155E937E416C8CFDC11630E9789E" as pub request_received_at: NsTAIInterval;
    /// Current resolution status (one of the `STATUS_*` tags).
    "4D72D56FF30DA693679F08D629DA7574" as pub request_status: GenId;

    // ── Per-team-cap pin ──────────────────────────────────────────────
    /// Handle of the currently-pinned cap blob for a team. Overwritten
    /// on each renewal so old caps become unreachable.
    "A2BBD772754BBB8EAFD7479F5A1249FD" as pub team_cap_handle: Handle<SimpleArchive>;
    /// Handle of the currently-pinned sig blob for a team. Updated in
    /// lockstep with `team_cap_handle`.
    "FAC14D0CAB23B1C7AC20D8CF1C843EBF" as pub team_sig_handle: Handle<SimpleArchive>;
}

// ── Pin role kind tags ────────────────────────────────────────────────

/// Pin holds A's renewal policy state. Each entity on the pin head
/// metadata blob is one `(policy_subject, policy_scope)` pair with
/// associated cap + sig handles and an optional retraction timestamp.
pub const KIND_RENEWAL_POLICY: Id =
    triblespace_core::id::id_hex!("914CFF7C82FDE32CB84D85CE98613E62");

/// Pin holds incoming `OP_REQUEST_CAP` payloads waiting for
/// resolution.
pub const KIND_PENDING_REQUESTS: Id =
    triblespace_core::id::id_hex!("A2010615F2E3B528B7069C761B38C102");

/// Pin holds A's own cap chain for a specific team. The pin head
/// metadata also carries `cap_for_team: <team_root_pubkey>` so a
/// peer with membership in multiple teams can distinguish them.
pub const KIND_TEAM_CAP: Id =
    triblespace_core::id::id_hex!("9BB2E5027EDB67463CC6A7A85B6C362D");

// ── Request status tags ───────────────────────────────────────────────

/// Request received, not yet acted on. CLI's `team list-pending`
/// shows entries with this status.
pub const STATUS_PENDING: Id =
    triblespace_core::id::id_hex!("08A49DEBF036B127CF60D8B33A7B9B31");

/// Request approved; a cap was issued and dispatched via
/// `OP_DELIVER_CAP`. The corresponding renewal-policy entry exists.
pub const STATUS_APPROVED: Id =
    triblespace_core::id::id_hex!("6186747FD38D84D23BA82F3ABE6D9952");

/// Request explicitly rejected. No cap issued.
pub const STATUS_REJECTED: Id =
    triblespace_core::id::id_hex!("3E54420C1F7EECFCED83203FA749C912");

// ── Helpers ───────────────────────────────────────────────────────────

/// Returns true if the pin's head metadata carries the
/// `local_only_pin` attribute. Used by the gossip-publish loop to
/// skip policy pins (they mustn't leak A's renewal decisions or
/// pending-request queue to the team).
pub fn is_local_only_pin<S>(store: &mut S, branch_id: Id) -> bool
where
    S: BlobStore + PinStore,
{
    let Ok(Some(head_handle)) = store.head(branch_id) else { return false; };
    let Ok(reader) = store.reader() else { return false; };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else { return false; };
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
pub fn find_team_cap_pin<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    use triblespace_core::inline::IntoInline;
    let team_root_inline: Inline<ED25519PublicKey> = team_root.to_inline();
    let bids: Vec<Id> = store
        .pins()
        .ok()?
        .filter_map(|r| r.ok())
        .collect();
    for bid in bids {
        let Ok(Some(head)) = store.head(bid) else { continue; };
        let Ok(reader) = store.reader() else { continue; };
        let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head) else { continue; };
        let matches = find!(
            (kind: Id, team: Inline<ED25519PublicKey>),
            pattern!(&meta, [{
                _?e @
                local_only_pin: ?kind,
                cap_for_team: ?team,
            }])
        )
        .any(|(kind, team)| kind == KIND_TEAM_CAP && team.raw == team_root_inline.raw);
        if matches {
            return Some(bid);
        }
    }
    None
}

/// Find the local-only branch of a given kind (e.g.
/// `KIND_RENEWAL_POLICY`, `KIND_PENDING_REQUESTS`). Branches of these
/// kinds are singletons per peer, so the first match wins.
pub fn find_local_only_pin_of_kind<S>(store: &mut S, kind: Id) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    let bids: Vec<Id> = store
        .pins()
        .ok()?
        .filter_map(|r| r.ok())
        .collect();
    for bid in bids {
        let Ok(Some(head)) = store.head(bid) else { continue; };
        let Ok(reader) = store.reader() else { continue; };
        let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head) else { continue; };
        let matches = find!(
            k: Id,
            pattern!(&meta, [{ _?e @ local_only_pin: ?k }])
        )
        .any(|k| k == kind);
        if matches {
            return Some(bid);
        }
    }
    None
}

/// A single pending request as recorded on the pending-requests pin.
pub struct PendingRequest {
    /// Entity id of this request inside the pin head metadata blob.
    /// Stable as long as the request isn't deleted; used as the
    /// argument to `team approve <id>`.
    pub id: Id,
    pub requester: ed25519_dalek::VerifyingKey,
    pub partial_cap: Inline<Handle<SimpleArchive>>,
    pub received_at: Inline<NsTAIInterval>,
    pub status: Id,
}

/// Snapshot of the current pending-requests set.
///
/// Branch metadata is "current state" rather than commit history —
/// the head metadata blob holds all currently-known requests as
/// distinct entities. This keeps the schema simple at low cardinality
/// (a peer realistically has at most a handful of pending requests
/// open at any time).
pub fn list_pending_requests<S>(store: &mut S) -> Vec<PendingRequest>
where
    S: BlobStore + PinStore,
{
    let Some(bid) = find_local_only_pin_of_kind(store, KIND_PENDING_REQUESTS) else {
        return Vec::new();
    };
    let Ok(Some(head)) = store.head(bid) else { return Vec::new(); };
    let Ok(reader) = store.reader() else { return Vec::new(); };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head) else { return Vec::new(); };

    find!(
        (
            e: Id,
            requester: ed25519_dalek::VerifyingKey,
            partial_cap: Inline<Handle<SimpleArchive>>,
            received_at: Inline<NsTAIInterval>,
            status: Id,
        ),
        pattern!(&meta, [{
            ?e @
            request_requester: ?requester,
            request_partial_cap: ?partial_cap,
            request_received_at: ?received_at,
            request_status: ?status,
        }])
    )
    .map(|(id, requester, partial_cap, received_at, status)| PendingRequest {
        id,
        requester,
        partial_cap,
        received_at,
        status,
    })
    .collect()
}

/// Record an incoming `OP_REQUEST_CAP` as a pending request entity on
/// the local pending-requests pin.
///
/// Find-or-create the pin on first call; subsequent calls extend
/// the head's metadata blob with one additional entity. The entity id
/// is fresh and is the value the CLI's `team approve <id>` consumes.
///
/// Returns the entity id of the new request entry. Returns `None` if
/// the underlying blob/branch writes fail (the caller decides whether
/// to retry, log, or drop).
pub fn record_pending_request<S>(
    store: &mut S,
    requester: ed25519_dalek::VerifyingKey,
    partial_cap: Inline<Handle<SimpleArchive>>,
    received_at: Inline<NsTAIInterval>,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    // Find or create the pending-requests pin.
    let (bid, prev_head) = match find_local_only_pin_of_kind(
        store,
        KIND_PENDING_REQUESTS,
    ) {
        Some(bid) => {
            let head = store.head(bid).ok().flatten();
            (bid, head)
        }
        None => (*genid(), None),
    };

    // Reconstitute the current metadata blob (if any), or start fresh
    // with just the pin-kind marker.
    let mut meta: TribleSet = match &prev_head {
        Some(h) => {
            let reader = store.reader().ok()?;
            reader.get::<TribleSet, SimpleArchive>(*h).ok()?
        }
        None => {
            use triblespace_core::id::ExclusiveId;
            let marker_id = genid();
            entity! { ExclusiveId::force_ref(&marker_id) @
                local_only_pin: KIND_PENDING_REQUESTS,
            }
            .into()
        }
    };

    // Add the new request entity. Its id is fresh — that's the value
    // the CLI's `team approve` consumes.
    let request_id = genid();
    let request_set: TribleSet = entity! {
        triblespace_core::id::ExclusiveId::force_ref(&request_id) @
        request_requester: requester,
        request_partial_cap: partial_cap,
        request_received_at: received_at,
        request_status: STATUS_PENDING,
    }
    .into();
    meta += request_set;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, prev_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(*request_id),
        PushResult::Conflict(_) => None,
    }
}

// ── Per-team-cap pin ──────────────────────────────────────────────────

/// Find or create the per-team-cap pin for `team_root`, then
/// overwrite its head with a metadata blob pointing at the supplied
/// `cap` and `sig` handles. Old metadata + old cap + old sig blobs
/// become unreachable from any pin head; the next compaction
/// reclaims them. This is the storage-layer expression of "the
/// active cap is what's current; old caps don't accumulate".
///
/// Returns the pin id on success. `None` on a blob-write or
/// branch-update failure (caller decides retry/log/drop).
pub fn pin_team_cap<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
) -> Option<Id>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    let (bid, prev_head) = match find_team_cap_pin(store, team_root) {
        Some(bid) => {
            let head = store.head(bid).ok().flatten();
            (bid, head)
        }
        None => (*genid(), None),
    };

    // Single-entity metadata blob: the pin-kind marker plus the
    // two handles. Entity id is fresh on each overwrite — the entity
    // doesn't need a stable identity since the pin head IS the
    // pin.
    let entity_id = genid();
    let meta: TribleSet = entity! {
        ExclusiveId::force_ref(&entity_id) @
        local_only_pin: KIND_TEAM_CAP,
        cap_for_team: team_root,
        team_cap_handle: cap,
        team_sig_handle: sig,
    }
    .into();

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, prev_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(bid),
        PushResult::Conflict(_) => None,
    }
}

/// Read the currently-pinned (cap, sig) handle pair for a team, if
/// any. Used by the auth flow at OP_AUTH time to find our own leaf
/// cap to present.
pub fn current_team_cap<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<(Inline<Handle<SimpleArchive>>, Inline<Handle<SimpleArchive>>)>
where
    S: BlobStore + PinStore,
{
    let bid = find_team_cap_pin(store, team_root)?;
    let head = store.head(bid).ok()??;
    let reader = store.reader().ok()?;
    let meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(head).ok()?;
    find!(
        (
            e: Id,
            cap: Inline<Handle<SimpleArchive>>,
            sig: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&meta, [{
            ?e @
            team_cap_handle: ?cap,
            team_sig_handle: ?sig,
        }])
    )
    .next()
    .map(|(_, cap, sig)| (cap, sig))
}

/// A single renewal-policy entry as recorded on the renewal-policy
/// branch. The auto-renewal daemon enumerates these and re-issues a
/// fresh cap for any whose `issued_at` upper bound is within the
/// configured renewal window of `now` AND that don't carry a
/// `retracted_at` attribute.
pub struct PolicyEntry {
    pub id: Id,
    pub subject: ed25519_dalek::VerifyingKey,
    pub scope: Id,
    pub issued_at: Inline<NsTAIInterval>,
    pub latest_cap: Inline<Handle<SimpleArchive>>,
    pub latest_sig: Inline<Handle<SimpleArchive>>,
    /// `Some(t)` if A has chosen to stop auto-renewing this entry;
    /// the daemon must skip entries with this set.
    pub retracted_at: Option<Inline<NsTAIInterval>>,
    /// `Some(t)` once the subject's daemon has ack'd receipt of the
    /// current `latest_cap` / `latest_sig` via OP_DELIVER_CAP's
    /// STATUS_OK. `None` means delivery is still pending — the
    /// renewal daemon's tick re-dispatches such entries until the
    /// ack lands.
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
    let Some(bid) = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY) else {
        return Vec::new();
    };
    let Ok(Some(head)) = store.head(bid) else { return Vec::new(); };
    let Ok(reader) = store.reader() else { return Vec::new(); };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head) else { return Vec::new(); };

    // Required fields (issued_at, latest cap/sig, subject, scope).
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
            issued_at: Inline<NsTAIInterval>,
            cap: Inline<Handle<SimpleArchive>>,
            sig: Inline<Handle<SimpleArchive>>,
        ),
        pattern!(&meta, [{
            ?e @
            policy_subject: ?subject,
            policy_scope: ?scope,
            policy_issued_at: ?issued_at,
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
        .map(|(id, subject, scope, issued_at, latest_cap, latest_sig)| {
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
                issued_at,
                latest_cap,
                latest_sig,
                retracted_at,
                delivered_at,
            }
        })
        .collect()
}

/// Filter `list_renewal_policy` to entries that are still pending
/// delivery: not retracted, and not yet ack'd by the subject's
/// daemon. These are the entries the renewal daemon re-dispatches on
/// each tick until the ack lands.
pub fn undelivered_entries<S>(store: &mut S) -> Vec<PolicyEntry>
where
    S: BlobStore + PinStore,
{
    list_renewal_policy(store)
        .into_iter()
        .filter(|e| e.retracted_at.is_none())
        .filter(|e| e.delivered_at.is_none())
        .collect()
}

/// Filter `list_renewal_policy` to entries that are due for renewal:
/// not retracted, and the upper bound of their `issued_at` interval
/// falls within `renewal_window` of `now`.
///
/// The daemon's typical call: `renewable_within(store,
/// Duration::from_secs(3600))` → entries whose current cap expires
/// in the next hour or already has. The window should be > the
/// daemon's tick cadence so a renewal isn't missed across one
/// missed tick.
pub fn renewable_within<S>(
    store: &mut S,
    renewal_window: hifitime::Duration,
) -> Vec<PolicyEntry>
where
    S: BlobStore + PinStore,
{
    let Ok(now) = hifitime::Epoch::now() else { return Vec::new(); };
    let cutoff = now + renewal_window;
    list_renewal_policy(store)
        .into_iter()
        .filter(|e| e.retracted_at.is_none())
        .filter(|e| {
            use triblespace_core::inline::TryFromInline;
            match <(hifitime::Epoch, hifitime::Epoch)>::try_from_inline(&e.issued_at) {
                // The current cap's upper bound has already passed
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
/// daemon's renewable-scan recomputes from the issued_at field rather
/// than relying on entity stability. If an entry for the same
/// `(subject, scope)` already exists, the caller should remove or
/// supersede it before adding the new one (typically via the
/// `update_policy_entry` helper below, which rewrites the issued_at
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
    issued_at: Inline<NsTAIInterval>,
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

    let (bid, prev_head) = match find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY) {
        Some(bid) => (bid, store.head(bid).ok().flatten()),
        None => (*genid(), None),
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
        policy_issued_at: issued_at,
        policy_latest_cap: cap,
        policy_latest_sig: sig,
    }
    .into();
    meta += entry_set;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, prev_head, Some(new_head)).ok()? {
        PushResult::Success() => Some(*entity_id),
        PushResult::Conflict(_) => None,
    }
}

/// Update an existing renewal-policy entry in place: rewrite its
/// `policy_issued_at`, `policy_latest_cap`, and `policy_latest_sig`
/// tribles. Called by the renewal daemon after each successful
/// re-sign + dispatch.
///
/// The `(subject, scope)` keys remain stable; only the time and
/// handle fields change.
pub fn update_policy_entry<S>(
    store: &mut S,
    entry_id: Id,
    new_issued_at: Inline<NsTAIInterval>,
    new_cap: Inline<Handle<SimpleArchive>>,
    new_sig: Inline<Handle<SimpleArchive>>,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;

    let bid = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(bid).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    // Remove the three existing tribles we're replacing.
    let cur_issued_at: Option<Inline<NsTAIInterval>> = find!(
        t: Inline<NsTAIInterval>,
        pattern!(&meta, [{ entry_id @ policy_issued_at: ?t }])
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

    if let Some(old) = cur_issued_at {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_issued_at: old,
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

    // Re-signing supersedes the prior cap. The subject's daemon
    // needs to ack the new (cap, sig) pair afresh, so clear any
    // existing `policy_delivered_at` and let the next tick's
    // `undelivered_entries` pick it up for re-dispatch.
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
        policy_issued_at: new_issued_at,
        policy_latest_cap: new_cap,
        policy_latest_sig: new_sig,
    }
    .into();
    meta += new_tribles;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Mark a renewal-policy entry as delivered (sets
/// `policy_delivered_at = now`). Called after the host loop's
/// `OP_DELIVER_CAP` task observes a STATUS_OK ack from the subject.
/// The daemon's `undelivered_entries` filter then skips this entry
/// on subsequent ticks; only renewable_within (near-expiry) picks
/// it up again, and `update_policy_entry` clears the field when the
/// daemon re-signs.
pub fn mark_policy_delivered<S>(
    store: &mut S,
    entry_id: Id,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::inline::TryToInline;

    let bid = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(bid).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    // Remove any prior delivered_at (re-marking after a fresh re-sign).
    let cur: Option<Inline<NsTAIInterval>> = find!(
        t: Inline<NsTAIInterval>,
        pattern!(&meta, [{ entry_id @ policy_delivered_at: ?t }])
    )
    .next();
    if let Some(old) = cur {
        let t: TribleSet = entity! {
            ExclusiveId::force_ref(&entry_id) @
            policy_delivered_at: old,
        }
        .into();
        meta = meta.difference(&t);
    }

    let now = hifitime::Epoch::now().ok()?;
    let delivered_at: Inline<NsTAIInterval> =
        (now, now).try_to_inline().ok()?;

    let trible: TribleSet = entity! {
        ExclusiveId::force_ref(&entry_id) @
        policy_delivered_at: delivered_at,
    }
    .into();
    meta += trible;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Look up a renewal-policy entry by `(subject, latest_sig)`. Used by
/// the Peer's `CapDeliveryConfirmed` handler to find which entry the
/// subject just authenticated with. The match key is the *signature*
/// handle because that's what OP_AUTH wires (and what the host's
/// `CapDeliveryConfirmed` event carries); the cap-blob handle is
/// reachable separately via the matched entry's `latest_cap` if a
/// caller needs it.
pub fn find_policy_entry_by_subject_and_sig<S>(
    store: &mut S,
    subject: ed25519_dalek::VerifyingKey,
    latest_sig: Inline<Handle<SimpleArchive>>,
) -> Option<Id>
where
    S: BlobStore + PinStore,
{
    list_renewal_policy(store)
        .into_iter()
        .find(|e| e.subject == subject && e.latest_sig == latest_sig)
        .map(|e| e.id)
}

/// Mark a renewal-policy entry as retracted (sets `policy_retracted_at
/// = now`). The daemon's `renewable_within` filter then skips it on
/// subsequent ticks; the corresponding peer's chain dies naturally at
/// the current cap's expiry.
pub fn retract_policy_entry<S>(
    store: &mut S,
    entry_id: Id,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    use triblespace_core::id::ExclusiveId;
    use triblespace_core::inline::TryToInline;

    let bid = find_local_only_pin_of_kind(store, KIND_RENEWAL_POLICY)?;
    let prev_head = store.head(bid).ok()??;
    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader.get::<TribleSet, SimpleArchive>(prev_head).ok()?;

    let now = hifitime::Epoch::now().ok()?;
    let retracted_at: Inline<NsTAIInterval> =
        (now, now).try_to_inline().ok()?;

    let trible: TribleSet = entity! {
        ExclusiveId::force_ref(&entry_id) @
        policy_retracted_at: retracted_at,
    }
    .into();
    meta += trible;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

/// Mark a pending request as approved or rejected. The entity-level
/// fact (request_status) is rewritten on the same branch's head blob.
///
/// This is what `team approve` and (eventually) `team reject` call
/// after they've taken their respective external actions (e.g. for
/// approve: signed + dispatched `OP_DELIVER_CAP`).
pub fn set_request_status<S>(
    store: &mut S,
    request_id: Id,
    new_status: Id,
) -> Option<()>
where
    S: BlobStore + BlobStorePut + PinStore,
{
    let bid = find_local_only_pin_of_kind(store, KIND_PENDING_REQUESTS)?;
    let prev_head = store.head(bid).ok()??;

    let reader = store.reader().ok()?;
    let mut meta: TribleSet = reader
        .get::<TribleSet, SimpleArchive>(prev_head)
        .ok()?;

    // Find the existing status trible and remove it; insert a fresh
    // one with the new status value. TribleSet is a set, so we
    // construct a single-trible set and use the diff-and-merge
    // primitives.
    let current_status: Option<Id> = find!(
        s: Id,
        pattern!(&meta, [{ request_id @ request_status: ?s }])
    )
    .next();
    if let Some(old) = current_status {
        let old_trible: TribleSet = entity! {
            triblespace_core::id::ExclusiveId::force_ref(&request_id) @
            request_status: old,
        }
        .into();
        // Set difference: remove the old trible.
        meta = meta.difference(&old_trible);
    }
    let new_trible: TribleSet = entity! {
        triblespace_core::id::ExclusiveId::force_ref(&request_id) @
        request_status: new_status,
    }
    .into();
    meta += new_trible;

    let new_head: Inline<Handle<SimpleArchive>> = store.put(meta).ok()?;
    match store.update(bid, Some(prev_head), Some(new_head)).ok()? {
        PushResult::Success() => Some(()),
        PushResult::Conflict(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use triblespace_core::blob::Blob;
    use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
    use triblespace_core::inline::TryToInline;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::trible::TribleSet;

    fn point_now() -> Inline<NsTAIInterval> {
        let now = hifitime::Epoch::now().expect("system time");
        (now, now).try_to_inline().expect("point interval")
    }

    fn empty_partial_cap_handle(
        store: &mut MemoryRepo,
    ) -> Inline<Handle<SimpleArchive>> {
        let set = TribleSet::new();
        let blob: Blob<SimpleArchive> = {
            use triblespace_core::blob::IntoBlob;
            set.to_blob()
        };
        store
            .put::<SimpleArchive, Blob<SimpleArchive>>(blob)
            .expect("put")
    }

    #[test]
    fn record_then_list_pending_round_trip() {
        let mut store = MemoryRepo::default();
        let requester = SigningKey::generate(&mut OsRng).verifying_key();
        let partial_cap = empty_partial_cap_handle(&mut store);

        let received_at = point_now();
        let id = record_pending_request(
            &mut store,
            requester,
            partial_cap,
            received_at,
        )
        .expect("record");

        let listed = list_pending_requests(&mut store);
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].requester, requester);
        assert_eq!(listed[0].status, STATUS_PENDING);
        assert_eq!(listed[0].partial_cap.raw, partial_cap.raw);
    }

    #[test]
    fn second_request_extends_pending_set() {
        let mut store = MemoryRepo::default();
        let req1 = SigningKey::generate(&mut OsRng).verifying_key();
        let req2 = SigningKey::generate(&mut OsRng).verifying_key();
        let partial = empty_partial_cap_handle(&mut store);

        let id1 = record_pending_request(&mut store, req1, partial, point_now())
            .expect("record 1");
        let id2 = record_pending_request(&mut store, req2, partial, point_now())
            .expect("record 2");
        assert_ne!(id1, id2);

        let listed = list_pending_requests(&mut store);
        assert_eq!(listed.len(), 2);
        let ids: std::collections::HashSet<Id> =
            listed.iter().map(|p| p.id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn set_request_status_flips_one_entry() {
        let mut store = MemoryRepo::default();
        let requester = SigningKey::generate(&mut OsRng).verifying_key();
        let partial = empty_partial_cap_handle(&mut store);

        let id = record_pending_request(&mut store, requester, partial, point_now())
            .expect("record");

        // Initial status is PENDING.
        let before = list_pending_requests(&mut store);
        assert_eq!(before[0].status, STATUS_PENDING);

        // Flip to APPROVED.
        set_request_status(&mut store, id, STATUS_APPROVED).expect("set status");
        let after = list_pending_requests(&mut store);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].status, STATUS_APPROVED);
        assert_eq!(after[0].id, id);
    }

    #[test]
    fn pending_branch_is_local_only() {
        // Recording a request must produce a branch carrying the
        // local-only marker so the gossip publisher skips it.
        let mut store = MemoryRepo::default();
        let requester = SigningKey::generate(&mut OsRng).verifying_key();
        let partial = empty_partial_cap_handle(&mut store);

        let _ = record_pending_request(&mut store, requester, partial, point_now())
            .expect("record");

        let bid = find_local_only_pin_of_kind(&mut store, KIND_PENDING_REQUESTS)
            .expect("branch exists");
        assert!(is_local_only_pin(&mut store, bid));
    }
}
