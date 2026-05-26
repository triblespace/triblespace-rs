//! Local-only policy branches: renewal state, pending join requests,
//! and per-team cap holdings.
//!
//! These branches live on the peer's pile but are **not** gossiped —
//! the implementation here mirrors the tracking-branch pattern from
//! `crate::tracking`. The `is_local_only_branch` check is consulted
//! by the gossip-publish loop in `Peer::refresh` to skip them.
//!
//! Three roles:
//!
//!   - **`KIND_RENEWAL_POLICY`** — A's per-issuer view: "I am willing
//!     to auto-renew these (subject, scope) pairs; here's the latest
//!     cap I issued to each; here are the ones I've retracted." The
//!     auto-renewal daemon scans this branch each tick.
//!
//!   - **`KIND_PENDING_REQUESTS`** — incoming `OP_REQUEST_CAP` payloads
//!     waiting for human approval (or auto-approval if the requester
//!     matches an existing renewal-policy entry). The CLI's
//!     `team list-pending` reads this branch; `team approve` mutates
//!     status entries on it.
//!
//!   - **`KIND_TEAM_CAP`** — one branch per team this peer is a
//!     member of, pinning the peer's own current cap chain so the
//!     pile retains it across compaction. Identified by
//!     `cap_for_team: <team_root_pubkey>`.
//!
//! All three are marked with the same `local_only_branch` attribute
//! (value = the kind tag) so a single helper distinguishes them from
//! gossipable team-data branches.
//!
//! See `decide#4b59ce27` (daemon + local-only retraction policy) for
//! the design rationale.

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::id::Id;
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::encodings::time::NsTAIInterval;
use triblespace_core::macros::{find, pattern};
use triblespace_core::prelude::attributes;
use triblespace_core::prelude::inlineencodings::{ED25519PublicKey, GenId};
use triblespace_core::repo::{BlobStore, BlobStoreGet, BranchStore};
use triblespace_core::trible::TribleSet;

attributes! {
    // ── Branch markers ────────────────────────────────────────────────
    /// Tags a branch as local-only (skip in gossip publish, skip in
    /// branch-name lookups). Value is one of the `KIND_*` tags below
    /// indicating the role.
    "3361F2DE0BD68BA8712EC5B9CCC7EF2A" as pub local_only_branch: GenId;

    // ── Per-team-cap branch ───────────────────────────────────────────
    /// Names the team this branch holds cap state for. Set on the
    /// branch metadata entity alongside `local_only_branch =
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
}

// ── Branch kind tags ──────────────────────────────────────────────────

/// Branch holds A's renewal policy state. Each entity on the branch
/// is one `(policy_subject, policy_scope)` pair with associated cap
/// + sig handles and an optional retraction timestamp.
pub const KIND_RENEWAL_POLICY: Id =
    triblespace_core::id::id_hex!("914CFF7C82FDE32CB84D85CE98613E62");

/// Branch holds incoming `OP_REQUEST_CAP` payloads waiting for
/// resolution.
pub const KIND_PENDING_REQUESTS: Id =
    triblespace_core::id::id_hex!("A2010615F2E3B528B7069C761B38C102");

/// Branch holds A's own cap chain for a specific team. The branch
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

/// Returns true if `branch_id`'s metadata carries the
/// `local_only_branch` attribute. Used by the gossip-publish loop to
/// skip policy branches (they mustn't leak A's renewal decisions or
/// pending-request queue to the team).
pub fn is_local_only_branch<S>(store: &mut S, branch_id: Id) -> bool
where
    S: BlobStore + BranchStore,
{
    let Ok(Some(head_handle)) = store.head(branch_id) else { return false; };
    let Ok(reader) = store.reader() else { return false; };
    let Ok(meta) = reader.get::<TribleSet, SimpleArchive>(head_handle) else { return false; };
    find!(
        kind: Id,
        pattern!(&meta, [{ _?e @ local_only_branch: ?kind }])
    )
    .next()
    .is_some()
}

/// Look up the local team-cap branch for a given team root pubkey,
/// if one exists. Searches by branch metadata for
/// `local_only_branch = KIND_TEAM_CAP` + `cap_for_team =
/// team_root`. Returns the branch id (caller can fetch the head or
/// list commits as needed).
pub fn find_team_cap_branch<S>(
    store: &mut S,
    team_root: ed25519_dalek::VerifyingKey,
) -> Option<Id>
where
    S: BlobStore + BranchStore,
{
    use triblespace_core::inline::IntoInline;
    let team_root_inline: Inline<ED25519PublicKey> = team_root.to_inline();
    let bids: Vec<Id> = store
        .branches()
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
                local_only_branch: ?kind,
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
