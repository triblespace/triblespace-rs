//! Capability-based authorization for triblespace networks.
//!
//! Implements a chain-of-trust capability system where:
//!
//! - A team has a single immutable root keypair (the "team root"), generated
//!   once at team creation and used to sign exactly one non-expiring founder
//!   anchor. The anchor is not an operational credential; the team root never
//!   operates online, and this signed link is the constitutional document for
//!   the team's identity.
//! - Every operational capability is finite and chains through that anchor.
//!   Any holder of a capability can sign a sub-capability for someone else, as
//!   long as the sub-cap's scope is a subset of their own. Verification walks
//!   the chain back to the team root.
//! - Each capability link is two blobs: a `cap` blob (the claim) and a `sig`
//!   blob (the issuer's signature over the cap blob's bytes). For chains of
//!   length > 1, each non-root cap embeds its parent's signature inline as a
//!   sub-entity, which halves the cold-cache verification fetch count by
//!   eliminating a separate round-trip per intermediate signature.
//! - Signatures attest to the cap blob's canonical bytes (SimpleArchive's
//!   serialization is already canonical), not to a hash of those bytes —
//!   matching the existing commit-signing convention. This keeps signatures
//!   hash-agnostic across any future Blake3 migration.
//!
//! Scope is encoded as tribles inside the cap blob, anchored at
//! `cap_scope_root`. Permissions are tagged via `metadata::tag` linking
//! to constants like `PERM_READ`, `PERM_WRITE`, `PERM_ADMIN`. Optional
//! per-resource restrictions like `scope_branch` narrow a permission to a
//! specific branch.
//!
//! (Names like `cap_scope_root`, `metadata::tag`, `scope_branch`, and
//! `PERM_*` are spelled in plain code formatting rather than as
//! intra-doc links because the macro-generated attribute items and
//! the `id_hex!`-defined constants don't reliably resolve as
//! rustdoc link targets from a `//!` block.)
//!
//! See `docs/sync_relay_auth_design.md` (or the `shared.pile` wiki fragment
//! titled "Sync Relay Auth Design") for the full design rationale.

use crate::id::Id;
use crate::id_hex;

/// Tag indicating a scope grants read access on the resources in scope.
pub const PERM_READ: Id = id_hex!("A75EED8224A553DD8002576E2E8A6823");
/// Tag indicating a scope grants write access on the resources in scope.
pub const PERM_WRITE: Id = id_hex!("C56AAF4191DD4FBB9F197B79435B881D");
/// Tag indicating a scope grants admin (delegation) authority.
pub const PERM_ADMIN: Id = id_hex!("EC68A0CBF9EF421F59A0A69ED80FD79F");

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::inline::encodings::ed25519 as ed;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;

triblespace_core_macros::attributes! {
    // ── Cap blob ──────────────────────────────────────────────────────
    /// The pubkey this capability authorizes. Must match the verified
    /// peer identity at connection time (i.e. the connecting peer's
    /// iroh `EndpointId`).
    "1A8A6A9D8CA1DA67FACAB373DE21233B" as pub cap_subject: ed::ED25519PublicKey;
    /// The pubkey of the entity that signed this capability. Must match
    /// the `signed_by` field of the accompanying signature blob.
    /// Recorded in the cap so verification can detect a sig-blob/cap
    /// issuer mismatch without an extra fetch.
    "2E9CD97ED0698FAF18EAEB74B5893685" as pub cap_issuer: ed::ED25519PublicKey;
    /// Entity id within the cap blob anchoring the scope tribles. The
    /// scope sub-graph hanging off this id encodes which permissions
    /// (and optionally which resources) the capability grants.
    "1A7DD2026BEFBE55A354CE10839CFDD6" as pub cap_scope_root: GenId;
    // Note: chain references (cap_parent, embedded parent sig) live in
    // the sig blob, not the cap blob. A cap blob is a pure declaration
    // of (subject, issuer, scope, lifetime kind) — independent of
    // which authority chain endorses it. See sig_parent_cap below.

    // ── Scope ─────────────────────────────────────────────────────────
    /// Optional restriction of a permission to a specific branch.
    /// Repeated when a permission applies to multiple branches; absent
    /// when the permission is unrestricted (applies to every branch
    /// the holder is otherwise authorised on).
    "46246789D627C1B0F81B21418E179DFD" as pub scope_branch: GenId;

    // ── Sig blob ──────────────────────────────────────────────────────
    /// Handle of the cap blob this signature attests to. The signature
    /// itself is over the cap blob's canonical bytes (i.e.
    /// `cap_blob.bytes`), not over the handle. SimpleArchive is already
    /// canonical, so the bytes the signer signs are exactly what the
    /// hasher hashes.
    "230E175A083E29155C860B38BD44F2F3" as pub sig_signs: Handle<SimpleArchive>;
    /// Handle of the parent cap blob in the chain. Absent when this
    /// entry's issuer is the team root (chain terminator). Present on
    /// every other sig-blob outer entity and recursive sub-entity.
    "ACF20EE95C6A4AE16B445590E88AB9BE" as pub sig_parent_cap: Handle<SimpleArchive>;
    /// Entity id within the same sig blob holding the parent's proof
    /// inline. The sub-entity carries `signed_by`, `signature_r`,
    /// `signature_s`, and (if the chain continues) its own
    /// `sig_parent_cap` + `sig_embedded_parent_proof`. Absent when
    /// the issuer is the team root.
    "8ED30E412129FB0A791BD335EACF2E82" as pub sig_embedded_parent_proof: GenId;
    // Note: sig_signer + sig_value (r/s) reuse the existing
    // `repo::signed_by`, `repo::signature_r`, `repo::signature_s`
    // attributes — same convention as commit signatures, plus
    // structural reuse (a sig blob has the same shape inside as the
    // signature portion of a commit's metadata blob).
}

/// Tag identifying a blob as a capability claim.
#[allow(dead_code)]
pub const KIND_CAPABILITY: Id = id_hex!("B8D76786ACD20F344A4E5CBFC0F75772");
/// Tag identifying a blob as a capability signature.
#[allow(dead_code)]
pub const KIND_CAPABILITY_SIG: Id = id_hex!("E6BB52CE6E02D51C3676ECE1EEA9094F");
/// Tag identifying the one non-expiring capability declaration that anchors a
/// team's founder authority.
///
/// Minted with `trible genid` on 2026-08-02. Unlike an operational
/// capability, a founder anchor carries no `expires_at`; verification accepts
/// it only as the root-signed terminator of a delegation proof.
pub const KIND_FOUNDER_ANCHOR: Id = id_hex!("56EF664DF4FCB4F52F2C486E9F6C55DB");

// ── Builder ──────────────────────────────────────────────────────────

use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::SigningKey;
use ed25519_dalek::VerifyingKey;

use crate::blob::encodings::simplearchive::UnarchiveError;
use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::inline::encodings::time::NsTAIInterval;
use crate::inline::Inline;
use crate::inline::IntoInline;
use crate::macros::entity;
use crate::macros::pattern;
use crate::query::find;
use crate::trible::TribleSet;

/// Errors returned by [`build_capability`].
#[derive(Debug)]
pub enum BuildError {
    /// The provided parent signature blob could not be parsed as a valid
    /// SimpleArchive.
    ParseParentSig(UnarchiveError),
    /// The provided parent signature blob did not contain exactly one
    /// signature entity (i.e. exactly one entity carrying [`sig_signs`]).
    ParentSigShape,
}

/// Build a capability link.
///
/// Returns the pair `(cap_blob, sig_blob)`:
/// - `cap_blob` carries the claim (subject pubkey, issuer pubkey, scope,
///   and expiry). Its content-addressed handle is what the sig blob
///   attests to.
/// - `sig_blob` carries the issuer's signature over `cap_blob.bytes`
///   plus the issuer's pubkey, alongside a `sig_signs` handle pointing
///   at the cap blob. For delegated capabilities it also carries the
///   parent cap handle and recursively embedded parent proof.
///
/// `parent` is required: the only valid root constructor is
/// [`build_founder_anchor`], so an unparented operational capability is
/// unrepresentable through this public builder.
///
/// The parent's signature is embedded inline in the new sig
/// blob (via [`sig_embedded_parent_proof`] pointing at a sub-entity
/// carrying `signed_by` + `signature_r` + `signature_s` reusing the
/// existing commit-signature attribute conventions) so verifiers can
/// walk one level up the chain without a separate fetch for the parent's
/// signature. Verification also requires the parent cap's subject to be
/// this child cap's issuer.
///
/// `scope_facts` should be a TribleSet anchored at `scope_root` describing
/// the capability's scope (permission tags via [`crate::metadata::tag`],
/// optional resource restrictions via [`scope_branch`], etc.). The caller
/// is responsible for producing a scope that's a subset of any parent
/// scope; this builder does not enforce subsumption.
///
/// # Example
///
/// Mint a finite founder credential beneath the root's non-expiring anchor.
/// The founder presents the finite sig blob's handle at connection time.
///
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use triblespace_core::id::{ufoid, ExclusiveId};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::inline::TryToInline;
/// use triblespace_core::repo::capability::{
///     build_capability, build_founder_anchor, PERM_READ,
/// };
/// use rand::rngs::OsRng;
///
/// let team_root = SigningKey::generate(&mut OsRng);
/// let founder = SigningKey::generate(&mut OsRng);
///
/// // PERM_READ scope, no branch restriction (read-everything cap).
/// let scope_root = ufoid();
/// let scope_facts: TribleSet = entity! {
///     ExclusiveId::force_ref(&scope_root) @
///     triblespace_core::metadata::tag: PERM_READ,
/// }
/// .into();
///
/// let now = hifitime::Epoch::now().unwrap();
/// let expiry = (now, now + hifitime::Duration::from_seconds(24.0 * 3600.0))
///     .try_to_inline()
///     .unwrap();
///
/// let (anchor_cap, anchor_sig) = build_founder_anchor(
///     &team_root,
///     founder.verifying_key(),
///     *scope_root,
///     scope_facts.clone(),
/// )
/// .expect("anchor builds");
/// let (cap_blob, sig_blob) = build_capability(
///     &founder,
///     founder.verifying_key(),
///     (anchor_cap, anchor_sig),
///     *scope_root,
///     scope_facts,
///     expiry,
/// )
/// .expect("cap builds");
///
/// // Both blobs go into the pile. The founder's "credential" is the
/// // sig blob's content-addressed handle.
/// assert!(!cap_blob.bytes.is_empty());
/// assert!(!sig_blob.bytes.is_empty());
/// ```
pub fn build_capability(
    issuer: &SigningKey,
    subject: VerifyingKey,
    parent: (Blob<SimpleArchive>, Blob<SimpleArchive>),
    scope_root: crate::id::Id,
    scope_facts: TribleSet,
    expiry: Inline<NsTAIInterval>,
) -> Result<(Blob<SimpleArchive>, Blob<SimpleArchive>), BuildError> {
    let issuer_pubkey: VerifyingKey = issuer.verifying_key();

    // Build the cap blob — pure declaration of (subject, issuer, scope,
    // expiry) and any caller-supplied scope facts. NO chain references;
    // those live in the sig blob.
    let cap_fragment = entity! {
        cap_subject: issuer_subject_value(subject),
        cap_issuer: issuer_subject_value(issuer_pubkey),
        cap_scope_root: scope_root,
        crate::metadata::expires_at: expiry,
    };

    let mut cap_set = TribleSet::from(cap_fragment);
    cap_set += scope_facts;

    build_signed_claim(issuer, cap_set, Some(parent))
}

/// Build the team's single, explicit founder anchor.
///
/// The returned blobs use the same ordinary claim/signature representation as
/// delegated capabilities, but the claim is distinguished by
/// [`KIND_FOUNDER_ANCHOR`] on its capability entity and deliberately has no
/// expiry interval. The anchor binds the offline team-root signer to the
/// founder key and `scope_facts`; it is not itself an operational credential.
/// [`verify_chain`] therefore rejects its signature handle as a leaf and only
/// accepts it as the final parent of a finite capability issued by `founder`.
pub fn build_founder_anchor(
    team_root: &SigningKey,
    founder: VerifyingKey,
    scope_root: crate::id::Id,
    scope_facts: TribleSet,
) -> Result<(Blob<SimpleArchive>, Blob<SimpleArchive>), BuildError> {
    let cap_fragment = entity! {
        cap_subject: issuer_subject_value(founder),
        cap_issuer: issuer_subject_value(team_root.verifying_key()),
        cap_scope_root: scope_root,
        crate::metadata::tag: KIND_FOUNDER_ANCHOR,
    };

    let mut cap_set = TribleSet::from(cap_fragment);
    cap_set += scope_facts;

    build_signed_claim(team_root, cap_set, None)
}

/// Sign a capability claim and, for delegated claims, fold the parent's exact
/// proof into the new signature blob.
fn build_signed_claim(
    issuer: &SigningKey,
    cap_set: TribleSet,
    parent: Option<(Blob<SimpleArchive>, Blob<SimpleArchive>)>,
) -> Result<(Blob<SimpleArchive>, Blob<SimpleArchive>), BuildError> {
    let issuer_pubkey = issuer.verifying_key();
    let cap_blob: Blob<SimpleArchive> = cap_set.to_blob();
    let cap_handle: Inline<Handle<SimpleArchive>> = (&cap_blob).get_handle();

    // Sign the cap blob's canonical bytes.
    let signature: Signature = issuer.sign(&cap_blob.bytes);

    // Build the sig blob. Outer entity carries the leaf sig over the
    // cap, plus (if there's a parent) `sig_parent_cap` + the parent's
    // entire proof. The parent's tribles are folded in under their
    // existing entity ids; the parent's outer entity becomes our
    // embedded proof sub-entity. We strip the parent's `sig_signs`
    // attribute on its outer entity — that attribute marks the leaf
    // entity of a sig blob, and once embedded as a sub-entity it's no
    // longer a leaf.
    let mut sig_set: TribleSet = TribleSet::from(entity! {
        sig_signs: cap_handle,
        crate::repo::signed_by: issuer_pubkey,
        crate::repo::signature_r: signature,
        crate::repo::signature_s: signature,
    });
    let leaf_outer_id: crate::id::Id = find!(
        (s: crate::id::Id, _h: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{ ?s @ sig_signs: ?_h }])
    )
    .map(|(s, _)| s)
    .next()
    .expect("just inserted our own outer sig entity");

    if let Some((parent_cap_blob, parent_sig_blob)) = parent {
        let parent_cap_handle: Inline<Handle<SimpleArchive>> = parent_cap_blob.get_handle();

        let parent_sig_set: TribleSet =
            TryFromBlob::<SimpleArchive>::try_from_blob(parent_sig_blob)
                .map_err(BuildError::ParseParentSig)?;

        // Locate the parent's outer leaf entity (the one with sig_signs).
        let mut parent_outer_iter = find!(
            (sig: crate::id::Id, _signed: Inline<Handle<SimpleArchive>>),
            pattern!(&parent_sig_set, [{ ?sig @ sig_signs: ?_signed }])
        )
        .map(|(sig, _)| sig);
        let parent_outer_id = match (parent_outer_iter.next(), parent_outer_iter.next()) {
            (Some(id), None) => id,
            _ => return Err(BuildError::ParentSigShape),
        };

        // Pull every trible from the parent sig blob into our sig blob,
        // dropping the parent's outer `sig_signs` trible (since that
        // entity is no longer a leaf in the merged sig blob).
        let sig_signs_attr_id = sig_signs.id();
        for trible in parent_sig_set.iter() {
            if *trible.e() == parent_outer_id && *trible.a() == sig_signs_attr_id {
                continue;
            }
            sig_set.insert(trible);
        }

        // Attach the parent linkage to our own outer entity.
        sig_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&leaf_outer_id) @
            sig_parent_cap: parent_cap_handle,
            sig_embedded_parent_proof: parent_outer_id,
        });
    }

    let sig_blob: Blob<SimpleArchive> = sig_set.to_blob();

    Ok((cap_blob, sig_blob))
}

/// Convenience: convert a `VerifyingKey` to a `Inline<ED25519PublicKey>`.
/// Inlined to avoid an explicit `IntoInline` import at the call sites in
/// the builder above.
fn issuer_subject_value(key: VerifyingKey) -> Inline<ed::ED25519PublicKey> {
    key.to_inline()
}

// ── Scope subsumption ────────────────────────────────────────────────

/// Collect the permission tag ids and branch restrictions from a scope
/// sub-graph anchored at `scope_root`.
fn collect_scope_facts(
    set: &TribleSet,
    scope_root: crate::id::Id,
) -> (HashSet<crate::id::Id>, HashSet<crate::id::Id>) {
    let perms: HashSet<crate::id::Id> = find!(
        (perm: crate::id::Id),
        pattern!(set, [{ scope_root @ crate::metadata::tag: ?perm }])
    )
    .map(|(p,)| p)
    .collect();

    let branches: HashSet<crate::id::Id> = find!(
        (branch: crate::id::Id),
        pattern!(set, [{ scope_root @ scope_branch: ?branch }])
    )
    .map(|(b,)| b)
    .collect();

    (perms, branches)
}

/// Check whether a parent scope authorises a child scope.
///
/// Rules:
/// - If parent grants `PERM_ADMIN`, it subsumes every *known permission* in
///   the child, but never bypasses branch restrictions or unknown-tag checks.
/// - Otherwise: every permission tag in the child must be in the
///   parent's set (with `PERM_WRITE` implying `PERM_READ` for upgrade
///   compatibility, but an explicit `PERM_READ`-only parent does *not*
///   imply `PERM_WRITE` for the child).
/// - Branch restriction: an empty `scope_branch` set means "all
///   branches"; a non-empty set restricts the scope to those branches.
///   The child's restriction set must be a subset of the parent's
///   (where empty parent = all branches allowed).
///
/// Unknown permission tags in the child cause subsumption to fail
/// closed.
pub fn scope_subsumes(
    parent_set: &TribleSet,
    parent_scope_root: crate::id::Id,
    child_set: &TribleSet,
    child_scope_root: crate::id::Id,
) -> bool {
    let (parent_perms, parent_branches) = collect_scope_facts(parent_set, parent_scope_root);
    let (child_perms, child_branches) = collect_scope_facts(child_set, child_scope_root);

    let parent_is_admin = parent_perms.contains(&PERM_ADMIN);

    for perm in &child_perms {
        if *perm != PERM_READ && *perm != PERM_WRITE && *perm != PERM_ADMIN {
            // Unknown permissions never become valid merely because an admin
            // signed them; fail closed at every delegation level.
            return false;
        }
        if parent_is_admin {
            continue;
        }
        if *perm == PERM_READ {
            if !parent_perms.contains(&PERM_READ) && !parent_perms.contains(&PERM_WRITE) {
                return false;
            }
        } else if *perm == PERM_WRITE {
            if !parent_perms.contains(&PERM_WRITE) {
                return false;
            }
        } else if *perm == PERM_ADMIN {
            // Parent isn't admin (already checked), so the child can't
            // claim admin either.
            return false;
        }
    }

    // Branch restriction subsumption.
    if !parent_branches.is_empty() {
        if child_branches.is_empty() {
            return false;
        }
        for b in &child_branches {
            if !parent_branches.contains(b) {
                return false;
            }
        }
    }

    true
}

// ── Verifier ──────────────────────────────────────────────────────────

use crate::inline::TryFromInline;
use ed25519_dalek::Verifier;
use hifitime::Epoch;
use std::collections::HashSet;

/// The parsed declaration carried by a founder-anchor capability blob.
///
/// This type describes the explicit, non-expiring constitutional link from a
/// team's offline root key to its founder key. It becomes trusted only after
/// [`verify_chain`] has checked that its issuer and signature both equal the
/// configured team root and that it terminates (rather than leads) the proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FounderAnchor {
    /// Entity containing the anchor declaration in the capability blob.
    pub entity: crate::id::Id,
    /// Founder key authorized to issue the first finite capability.
    pub subject: VerifyingKey,
    /// Declared issuer; a verified anchor requires this to be the team root.
    pub issuer: VerifyingKey,
    /// Root of the maximum scope the founder may delegate.
    pub scope_root: crate::id::Id,
}

/// Errors returned by [`verify_chain`].
#[derive(Debug)]
pub enum VerifyError {
    /// The leaf or some intermediate sig/cap blob could not be parsed
    /// as a valid SimpleArchive.
    ParseBlob(UnarchiveError),
    /// The caller-supplied fetch function could not retrieve the named
    /// signature or capability blob.
    MissingBlob(Inline<Handle<SimpleArchive>>),
    /// A signature failed to verify against the expected pubkey + cap
    /// blob bytes.
    BadSignature,
    /// The leaf cap's subject did not match the expected (connecting)
    /// peer pubkey.
    SubjectMismatch,
    /// A cap's `cap_issuer` did not match the accompanying sig's
    /// `signed_by`.
    IssuerMismatch,
    /// A parent cap authorizes a different subject than the issuer of
    /// the child cap, so it cannot delegate authority to that child.
    DelegationMismatch,
    /// A cap or one of its parent caps has expired.
    Expired,
    /// A child cap's scope was not a subset of its parent's scope.
    /// (Enforcement deferred to the scope-subsumption module — for now
    /// this variant is reserved for future use.)
    ScopeNotSubset,
    /// A cap blob is missing required attributes, has conflicting values, or
    /// mixes the finite-expiry and founder-anchor declaration shapes.
    MalformedCap,
    /// A founder anchor was presented as the operational leaf credential.
    FounderAnchorAsLeaf,
    /// A proof reached a root-signed finite capability rather than the
    /// explicit founder anchor required to terminate every valid chain.
    FounderAnchorRequired,
    /// A syntactically well-formed founder anchor was not both declared and
    /// signed by the configured team root.
    InvalidFounderAnchor,
    /// A sig blob is missing required attributes or has multiple
    /// conflicting values.
    MalformedSig,
    /// A non-root sig-blob entity (one whose signer differs from the
    /// team root) is missing either `sig_parent_cap` or
    /// `sig_embedded_parent_proof`.
    NonRootMissingParent,
    /// The chain exceeded a sanity-bound depth without terminating at
    /// the team root.
    ChainTooDeep,
}

impl From<UnarchiveError> for VerifyError {
    fn from(e: UnarchiveError) -> Self {
        VerifyError::ParseBlob(e)
    }
}

/// A successfully verified leaf capability.
///
/// Returned by [`verify_chain`] on a successful walk back to the
/// configured `team_root`. Carries the leaf cap's full `TribleSet` so
/// callers can ask:
///
/// - [`permissions`](Self::permissions) — which `PERM_*` tags are
///   hung on the scope root
/// - [`granted_branches`](Self::granted_branches) — `Some(set)` if the
///   cap restricts itself to specific branches, or `None` if it's
///   unrestricted within its permission set
/// - [`grants_read`](Self::grants_read) — convenience for "any read-
///   equivalent permission" (write/admin imply read)
/// - [`grants_read_on`](Self::grants_read_on) — combines the two:
///   read-permission AND (unrestricted OR branch-in-scope)
/// - [`expires_at`](Self::expires_at) — the inclusive upper bound of the
///   verified authority's lifetime (the earliest deadline in the chain)
///
/// # Example
///
/// Build a `VerifiedCapability` directly (skipping `verify_chain` —
/// the helpers operate on `cap_set` shape, not on the chain proof,
/// so a hand-crafted instance suffices for testing scope predicates):
///
/// ```rust
/// use std::collections::HashSet;
/// use triblespace_core::id::{ufoid, ExclusiveId, Id};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::repo::capability::{
///     scope_branch, VerifiedCapability, PERM_READ,
/// };
/// use ed25519_dalek::SigningKey;
/// use rand::rngs::OsRng;
///
/// let scope_root = ufoid();
/// let allowed_branch = ufoid();
/// // PERM_READ scope, restricted to one branch.
/// let mut cap_set = TribleSet::new();
/// cap_set += TribleSet::from(entity! {
///     ExclusiveId::force_ref(&scope_root) @
///     triblespace_core::metadata::tag: PERM_READ,
/// });
/// cap_set += TribleSet::from(entity! {
///     ExclusiveId::force_ref(&scope_root) @
///     scope_branch: *allowed_branch,
/// });
///
/// let verified = VerifiedCapability {
///     subject: SigningKey::generate(&mut OsRng).verifying_key(),
///     scope_root: *scope_root,
///     cap_set,
///     expires_at: hifitime::Epoch::now().unwrap()
///         + hifitime::Duration::from_days(1.0),
/// };
///
/// // permissions() exposes the raw tag set.
/// let perms = verified.permissions();
/// assert_eq!(perms.len(), 1);
/// assert!(perms.contains(&PERM_READ));
///
/// // granted_branches() returns Some(set) for restricted caps.
/// let branches = verified.granted_branches().expect("restricted");
/// assert!(branches.contains(&*allowed_branch));
///
/// // grants_read() short-circuits to "any read-equivalent perm".
/// assert!(verified.grants_read());
///
/// // grants_read_on() composes both checks.
/// assert!(verified.grants_read_on(&*allowed_branch));
/// let other_branch: Id = *ufoid();
/// assert!(!verified.grants_read_on(&other_branch));
/// ```
#[derive(Debug, Clone)]
pub struct VerifiedCapability {
    /// The subject pubkey the leaf cap authorizes.
    pub subject: VerifyingKey,
    /// The scope root entity id within the leaf cap blob.
    pub scope_root: crate::id::Id,
    /// The leaf cap's full TribleSet (caller can extract its scope by
    /// querying tribles anchored at `scope_root`).
    pub cap_set: TribleSet,
    /// Inclusive upper bound of this authority's lifetime. This is the
    /// earliest expiry in the verified delegation chain, so a child can never
    /// remain live after one of its parents expires.
    pub expires_at: Epoch,
}

/// A completely verified capability proof together with the authority
/// identities discovered while walking it.
///
/// [`verify_chain_and_extract_root_allow_expired`] uses the explicit founder
/// anchor as the unique constitutional terminator, so callers that are
/// migrating historical credentials do not need to guess a team root or try
/// every signer in the proof. This discovers the proof's identity; it does not
/// establish that an application trusts the discovered team.
#[derive(Debug, Clone)]
pub struct VerifiedCapabilityChain {
    /// The key that signed and declared the terminal founder anchor.
    pub team_root: VerifyingKey,
    /// Exact capability blob named by the outer leaf signature.
    pub leaf_cap: Inline<Handle<SimpleArchive>>,
    /// Issuer of the finite operational leaf capability.
    pub leaf_issuer: VerifyingKey,
    /// The verified operational authority carried by the leaf.
    pub capability: VerifiedCapability,
}

/// A fully verified capability chain together with the canonical standalone
/// signature blob of its terminal founder anchor.
///
/// Delegated signature blobs embed every parent proof but omit the embedded
/// proof entity's `sig_signs` fact. Verification already proves the canonical
/// entity id and the exact linear proof shape, so restoring that one fact for
/// the terminal anchor reconstructs the original standalone blob byte-for-byte.
/// This lets founder renewal retain only a stable grant selector instead of a
/// second mutable handle to material that is already present in the selected
/// proof.
#[derive(Debug, Clone)]
pub struct VerifiedCapabilityWithFounderAnchor {
    /// The verified operational leaf and authority identities.
    pub chain: VerifiedCapabilityChain,
    /// Canonical standalone signature over the terminal founder-anchor cap.
    pub founder_anchor_sig: Blob<SimpleArchive>,
}

/// Verified chain plus the already-validated ingredients needed to recreate
/// its terminal standalone anchor. Keeping the ingredients internal avoids
/// allocating and sorting an archive on ordinary verification paths that do
/// not ask for founder renewal material.
struct VerifiedCapabilityProof {
    chain: VerifiedCapabilityChain,
    founder_anchor_entity: crate::id::Id,
    founder_anchor_cap: Inline<Handle<SimpleArchive>>,
    founder_anchor_proof: VerifiedSigProof,
}

impl VerifiedCapability {
    /// Return the inclusive upper bound of this verified authority's lifetime.
    pub fn expires_at(&self) -> Epoch {
        self.expires_at
    }

    /// Return whether this authority is expired at `now`.
    ///
    /// Capability intervals have inclusive bounds, so the authority remains
    /// valid exactly at `expires_at` and expires immediately afterwards.
    pub fn is_expired_at(&self, now: Epoch) -> bool {
        self.expires_at < now
    }

    /// Return whether this authority is expired according to the project's
    /// virtualizable epoch clock.
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(crate::clock::epoch_now())
    }

    /// Returns the set of permissions tagged on this cap's scope root
    /// (a subset of `{`[`PERM_READ`]`,`[`PERM_WRITE`]`,`[`PERM_ADMIN`]`}`).
    pub fn permissions(&self) -> HashSet<crate::id::Id> {
        let (perms, _) = collect_scope_facts(&self.cap_set, self.scope_root);
        perms
    }

    /// Returns `Some(set)` if the cap restricts itself to a specific
    /// non-empty set of branches, or `None` if the cap is unrestricted
    /// (i.e. applies to every branch within the granted permission set).
    pub fn granted_branches(&self) -> Option<HashSet<crate::id::Id>> {
        let (_, branches) = collect_scope_facts(&self.cap_set, self.scope_root);
        if branches.is_empty() {
            None
        } else {
            Some(branches)
        }
    }

    /// Returns `true` if the cap grants any read-equivalent permission
    /// (read, write, or admin — write/admin imply read, matching the
    /// subsumption rules in [`scope_subsumes`]).
    pub fn grants_read(&self) -> bool {
        let perms = self.permissions();
        perms.contains(&PERM_READ) || perms.contains(&PERM_WRITE) || perms.contains(&PERM_ADMIN)
    }

    /// Returns `true` if the cap grants read-equivalent permission on
    /// the given branch — i.e. the cap [`grants_read`](Self::grants_read)
    /// AND either is unrestricted or its restriction set contains
    /// `branch`.
    pub fn grants_read_on(&self, branch: &crate::id::Id) -> bool {
        if !self.grants_read() {
            return false;
        }
        match self.granted_branches() {
            None => true,
            Some(set) => set.contains(branch),
        }
    }
}

/// Maximum number of capability levels in a chain, including the leaf.
/// Real chains are 1-3 levels typically; this is a sanity bound to
/// refuse adversarial deep chains. If the capability at this depth is
/// not root-issued, verification rejects before fetching another parent.
pub const MAX_CHAIN_DEPTH: usize = 32;

/// Verify a single signature blob's claim against a cap blob's bytes.
///
// The old `verify_sig_blob` helper was replaced by the
// `extract_and_verify_sig_at` helper used by `verify_chain` — that one
// works against an arbitrary entity inside a sig blob (outer leaf or
// embedded sub-entity), which is what the new chain walk needs.

/// Extract and classify a cap blob's declaration.
///
/// Finite operational capabilities have exactly one expiry and no type tag on
/// the declaration entity. Founder anchors instead have exactly the one
/// [`KIND_FOUNDER_ANCHOR`] tag and no expiry. Every mixed or unknown shape
/// fails closed.
fn extract_cap_fields(cap_set: &TribleSet) -> Result<CapFields, VerifyError> {
    let mut iter = find!(
        (cap: crate::id::Id,
         subject: VerifyingKey,
         issuer: VerifyingKey,
         scope_root: crate::id::Id),
        pattern!(cap_set, [{
            ?cap @
            cap_subject: ?subject,
            cap_issuer: ?issuer,
            cap_scope_root: ?scope_root,
        }])
    );
    let (cap_id, subject, issuer, scope_root) = match (iter.next(), iter.next()) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedCap),
    };

    let expiries: Vec<Inline<NsTAIInterval>> = find!(
        (expiry: Inline<NsTAIInterval>),
        pattern!(cap_set, [{ cap_id @ crate::metadata::expires_at: ?expiry }])
    )
    .map(|(expiry,)| expiry)
    .collect();
    let tags: Vec<crate::id::Id> = find!(
        (kind: crate::id::Id),
        pattern!(cap_set, [{ cap_id @ crate::metadata::tag: ?kind }])
    )
    .map(|(kind,)| kind)
    .collect();

    let kind = match (tags.as_slice(), expiries.as_slice()) {
        ([], [expiry]) => CapKind::Operational { expiry: *expiry },
        ([kind], []) if *kind == KIND_FOUNDER_ANCHOR => CapKind::FounderAnchor(FounderAnchor {
            entity: cap_id,
            subject,
            issuer,
            scope_root,
        }),
        _ => return Err(VerifyError::MalformedCap),
    };

    Ok(CapFields {
        cap_id,
        subject,
        issuer,
        scope_root,
        kind,
    })
}

/// Strictly parsed finite operational capability declaration before any
/// signature or delegation proof is applied.
///
/// This is the narrow boundary for authenticated request intent. It applies
/// the same unique declaration, finite-expiry, and founder-anchor distinction
/// as [`verify_chain`], without pretending that an unsigned claim carries
/// authority. Callers must separately bind `subject` and `issuer` to the
/// authenticated parties that are allowed to make the request.
#[derive(Debug, Clone)]
pub struct OperationalCapability {
    pub entity: crate::id::Id,
    pub subject: VerifyingKey,
    pub issuer: VerifyingKey,
    pub scope_root: crate::id::Id,
    pub cap_set: TribleSet,
    pub expiry: Inline<NsTAIInterval>,
    pub valid_from: Epoch,
    pub expires_at: Epoch,
}

/// Parse one canonical archive as a finite operational capability claim.
///
/// The interval must decode successfully. Founder anchors and mixed/ambiguous
/// declaration shapes are rejected through the ordinary verifier errors.
pub fn decode_operational_capability(
    blob: Blob<SimpleArchive>,
) -> Result<OperationalCapability, VerifyError> {
    let cap_set: TribleSet = TryFromBlob::try_from_blob(blob)?;
    let fields = extract_cap_fields(&cap_set)?;
    let CapKind::Operational { expiry } = fields.kind else {
        return Err(VerifyError::FounderAnchorAsLeaf);
    };
    let (valid_from, expires_at) = decode_expiry_interval(&expiry)?;
    Ok(OperationalCapability {
        entity: fields.cap_id,
        subject: fields.subject,
        issuer: fields.issuer,
        scope_root: fields.scope_root,
        cap_set,
        expiry,
        valid_from,
        expires_at,
    })
}

fn decode_expiry_interval(expiry: &Inline<NsTAIInterval>) -> Result<(Epoch, Epoch), VerifyError> {
    let (lower, upper) =
        <(Epoch, Epoch)>::try_from_inline(expiry).map_err(|_| VerifyError::Expired)?;
    if upper < lower {
        return Err(VerifyError::Expired);
    }
    Ok((lower, upper))
}

#[derive(Debug, Clone)]
enum CapKind {
    Operational { expiry: Inline<NsTAIInterval> },
    FounderAnchor(FounderAnchor),
}

#[derive(Debug, Clone)]
struct CapFields {
    cap_id: crate::id::Id,
    subject: VerifyingKey,
    issuer: VerifyingKey,
    scope_root: crate::id::Id,
    kind: CapKind,
}

/// Verify that a leaf signature blob plus its referenced cap blob form
/// a valid capability chain rooted at `team_root`, authorising the
/// `expected_subject` to act with the leaf cap's scope.
///
/// `fetch_blob` is called first for the leaf sig, then for its leaf cap,
/// and then for each parent cap referenced by `sig_parent_cap` during
/// the chain walk. Parent signatures are embedded in the leaf sig blob
/// and are never fetched separately. A failed lookup returns
/// [`VerifyError::MissingBlob`] with the exact handle that was requested.
///
/// Eviction in the descriptive-caps model is per-issuer non-renewal, not a
/// broadcast revocation blob. The issuer publishes terminal `GrantDisabled`
/// in its author-scoped asserted policy, but capability verification does not
/// consult that policy. It still verifies exact team-root and expected-subject
/// binding, signatures and proof shape, issuer/subject delegation splices,
/// scope attenuation, depth, intervals, and live expiry. The already-issued
/// chain dies at its next natural expiry once the issuer stops renewing it.
///
/// Returns the verified leaf capability on success.
///
/// # Example
///
/// End-to-end auth flow: the team root anchors its founder, the founder mints
/// a finite operational credential, and the verifier checks that chain.
///
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use std::collections::HashMap;
/// use triblespace_core::blob::Blob;
/// use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
/// use triblespace_core::id::{ufoid, ExclusiveId};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::inline::TryToInline;
/// use triblespace_core::inline::Inline;
/// use triblespace_core::inline::encodings::hash::Handle;
/// use triblespace_core::repo::capability::{
///     build_capability, build_founder_anchor, verify_chain, PERM_READ,
/// };
/// use rand::rngs::OsRng;
///
/// // The root signs the founder anchor once, then returns offline.
/// let team_root = SigningKey::generate(&mut OsRng);
/// let founder = SigningKey::generate(&mut OsRng);
///
/// // Scope: a single anchor entity tagged with PERM_READ.
/// let scope_root = ufoid();
/// let scope_facts: TribleSet = entity! {
///     ExclusiveId::force_ref(&scope_root) @
///     triblespace_core::metadata::tag: PERM_READ,
/// }
/// .into();
///
/// // 24-hour expiry interval, anchored at "now".
/// let now = hifitime::Epoch::now().unwrap();
/// let expiry = (now, now + hifitime::Duration::from_seconds(24.0 * 3600.0))
///     .try_to_inline()
///     .unwrap();
///
/// let (anchor_cap, anchor_sig) = build_founder_anchor(
///     &team_root,
///     founder.verifying_key(),
///     *scope_root,
///     scope_facts.clone(),
/// )
/// .unwrap();
///
/// // The online founder credential is finite and names the anchor as parent.
/// let (cap_blob, sig_blob) = build_capability(
///     &founder,
///     founder.verifying_key(),
///     (anchor_cap.clone(), anchor_sig.clone()),
///     *scope_root,
///     scope_facts,
///     expiry,
/// )
/// .unwrap();
///
/// // The peer presents the *sig* blob's handle on connection.
/// let leaf_sig_handle: Inline<Handle<SimpleArchive>> =
///     (&sig_blob).get_handle();
///
/// // The verifier needs both blobs available via the fetch closure.
/// let cap_handle: Inline<Handle<SimpleArchive>> =
///     (&cap_blob).get_handle();
/// let anchor_cap_handle: Inline<Handle<SimpleArchive>> =
///     (&anchor_cap).get_handle();
/// let mut blobs: HashMap<[u8; 32], Blob<SimpleArchive>> = HashMap::new();
/// blobs.insert(anchor_cap_handle.raw, anchor_cap);
/// blobs.insert(cap_handle.raw, cap_blob);
/// blobs.insert(leaf_sig_handle.raw, sig_blob);
///
/// let verified = verify_chain(
///     team_root.verifying_key(),
///     leaf_sig_handle,
///     founder.verifying_key(),
///     |h| blobs.get(&h.raw).cloned(),
/// )
/// .expect("chain valid");
///
/// assert_eq!(verified.subject, founder.verifying_key());
/// assert!(verified.grants_read());
/// ```
pub fn verify_chain<F>(
    team_root: VerifyingKey,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    fetch_blob: F,
) -> Result<VerifiedCapability, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let verified =
        verify_chain_allow_expired(team_root, leaf_sig_handle, expected_subject, fetch_blob)?;
    if verified.is_expired() {
        Err(VerifyError::Expired)
    } else {
        Ok(verified)
    }
}

/// Verify the complete capability proof without requiring its effective
/// expiry to be live at the current wall-clock time.
///
/// This is the narrow recovery seam for startup code that must distinguish an
/// otherwise-valid expired credential from corrupt or unauthorized state. It
/// still verifies every signature, exact proof shape, delegation splice,
/// scope-subsumption relation, founder anchor, depth bound, and interval
/// encoding, and returns the earliest finite operational expiry in the chain.
/// It must not be used to authorize ordinary network operations; use
/// [`verify_chain`] for that.
pub fn verify_chain_allow_expired<F>(
    team_root: VerifyingKey,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    fetch_blob: F,
) -> Result<VerifiedCapability, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    Ok(verify_chain_details_allow_expired(
        team_root,
        leaf_sig_handle,
        expected_subject,
        fetch_blob,
    )?
    .capability)
}

/// Verify a complete capability proof against a known team root while
/// retaining the exact leaf identities discovered during the walk.
///
/// This has the same expired-tolerant verification semantics as
/// [`verify_chain_allow_expired`], but returns the leaf capability handle and
/// issuer alongside the verified capability. Typed replicated-state reducers
/// use those fields to bind a signed effect to the exact proof it names rather
/// than reparsing the proof through a second, weaker path.
pub fn verify_chain_details_allow_expired<F>(
    team_root: VerifyingKey,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    fetch_blob: F,
) -> Result<VerifiedCapabilityChain, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    Ok(verify_chain_inner(
        Some(team_root),
        leaf_sig_handle,
        expected_subject,
        fetch_blob,
    )?
    .chain)
}

/// Verify a complete capability proof and reconstruct its terminal founder
/// anchor's standalone signature blob.
///
/// This has the same expired-tolerant trust boundary as
/// [`verify_chain_details_allow_expired`]: `team_root` and `expected_subject`
/// are checked before any reconstructed material is returned. The returned
/// anchor signature is not an operational credential. It is authority material
/// for minting a new finite founder sibling beneath the already-verified
/// constitutional anchor.
pub fn verify_chain_and_reconstruct_founder_anchor_allow_expired<F>(
    team_root: VerifyingKey,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    fetch_blob: F,
) -> Result<VerifiedCapabilityWithFounderAnchor, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let verified = verify_chain_inner(
        Some(team_root),
        leaf_sig_handle,
        expected_subject,
        fetch_blob,
    )?;
    let founder_anchor_sig = standalone_signature_blob(
        verified.founder_anchor_entity,
        verified.founder_anchor_cap,
        &verified.founder_anchor_proof,
    );
    Ok(VerifiedCapabilityWithFounderAnchor {
        chain: verified.chain,
        founder_anchor_sig,
    })
}

/// Verify a complete capability proof while discovering its team root.
///
/// The proof must terminate at one canonical [`FounderAnchor`]. Its verified
/// signer must equal the anchor's declared issuer; that key is returned as the
/// team root. This is intended for evidence-bound migration and recovery when
/// the exact historical credential is present but its authority-domain key was
/// not stored separately. Expiry is reported in the result but is not required
/// to be live.
///
/// This function discovers which team the proof names. Callers remain
/// responsible for deciding whether that team is trusted and for binding the
/// returned leaf cap and issuer to the state being migrated.
pub fn verify_chain_and_extract_root_allow_expired<F>(
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    fetch_blob: F,
) -> Result<VerifiedCapabilityChain, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    Ok(verify_chain_inner(None, leaf_sig_handle, expected_subject, fetch_blob)?.chain)
}

fn verify_chain_inner<F>(
    expected_team_root: Option<VerifyingKey>,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    mut fetch_blob: F,
) -> Result<VerifiedCapabilityProof, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    // A cap is valid through the inclusive upper bound of its expiry
    // interval. Malformed/inverted intervals fail closed as Expired.
    let expiry_upper = |expiry: &Inline<NsTAIInterval>| -> Result<Epoch, VerifyError> {
        decode_expiry_interval(expiry).map(|(_lower, upper)| upper)
    };

    // ── Leaf step ────────────────────────────────────────────────────
    //
    // The leaf sig blob carries: the leaf signature (over the leaf
    // cap), the leaf cap handle (via sig_signs), and — if the chain
    // extends beyond a single hop — the recursive chain proof
    // (sig_parent_cap + sig_embedded_parent_proof, each linking to the
    // next level's signer/signature/parent).
    let leaf_sig_blob =
        fetch_blob(leaf_sig_handle).ok_or(VerifyError::MissingBlob(leaf_sig_handle))?;
    let sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)?;

    // Find the leaf outer entity — the one carrying sig_signs.
    let mut leaf_outer_iter = find!(
        (sig: crate::id::Id, h: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{ ?sig @ sig_signs: ?h }])
    );
    let (mut current_outer_id, leaf_cap_handle) =
        match (leaf_outer_iter.next(), leaf_outer_iter.next()) {
            (Some(row), None) => row,
            _ => return Err(VerifyError::MalformedSig),
        };

    // Fetch + decode the leaf cap.
    let leaf_cap_blob =
        fetch_blob(leaf_cap_handle).ok_or(VerifyError::MissingBlob(leaf_cap_handle))?;
    let leaf_cap_set: TribleSet = TryFromBlob::try_from_blob(leaf_cap_blob.clone())?;
    let leaf_fields = extract_cap_fields(&leaf_cap_set)?;

    // Subject must match the connecting peer.
    if leaf_fields.subject != expected_subject {
        return Err(VerifyError::SubjectMismatch);
    }
    let leaf_expiry = match &leaf_fields.kind {
        CapKind::Operational { expiry } => expiry,
        CapKind::FounderAnchor(_) => return Err(VerifyError::FounderAnchorAsLeaf),
    };
    let leaf_expires_at = expiry_upper(leaf_expiry)?;

    // Verify the outer signature attests to the leaf cap's bytes,
    // signed by the leaf's claimed issuer.
    let mut current_proof = extract_and_verify_sig_at(&sig_set, current_outer_id, &leaf_cap_blob)?;
    if current_proof.signer != leaf_fields.issuer {
        return Err(VerifyError::IssuerMismatch);
    }

    // ── Walk back to root ────────────────────────────────────────────
    //
    // Loop invariant:
    //   - `current_outer_id`: the entity in `sig_set` whose signature
    //     we have just verified (over `current_cap_set`'s blob bytes).
    //   - `current_signer`: the pubkey that signed `current_cap_set`'s
    //     blob (== current cap's issuer).
    //   - `current_cap_set`: the decoded cap whose signature we've
    //     verified.
    let mut current_signer = current_proof.signer;
    let mut current_cap_handle = leaf_cap_handle;
    let mut current_cap_set = leaf_cap_set.clone();
    let mut current_fields = leaf_fields.clone();
    let mut authority_expires_at = leaf_expires_at;
    // `(entity, has_parent_link)` in the single traversed proof chain. The
    // final exact-shape check rejects any unrelated entity or attribute that
    // would otherwise create infinitely many content-distinct handles for the
    // same signed caps.
    let leaf_outer_id = current_outer_id;
    let mut proof_shape = vec![(current_outer_id, false)];
    // The already-fetched leaf is the first capability level.
    let mut depth = 1usize;

    loop {
        // A founder anchor is the one constitutional terminator. Its verified
        // signer and declared issuer must be the same key; that key is the
        // structurally discovered team root. A caller-supplied root, when
        // present, is an additional trust-domain equality check.
        if let CapKind::FounderAnchor(anchor) = &current_fields.kind {
            let team_root = anchor.issuer;
            if current_signer != team_root
                || current_fields.issuer != team_root
                || expected_team_root.is_some_and(|expected| expected != team_root)
            {
                return Err(VerifyError::InvalidFounderAnchor);
            }
            if canonical_sig_entity_id(&current_proof, current_cap_handle) != current_outer_id {
                return Err(VerifyError::MalformedSig);
            }
            validate_sig_proof_shape(&sig_set, leaf_outer_id, &proof_shape)?;
            return Ok(VerifiedCapabilityProof {
                chain: VerifiedCapabilityChain {
                    team_root,
                    leaf_cap: leaf_cap_handle,
                    leaf_issuer: leaf_fields.issuer,
                    capability: VerifiedCapability {
                        subject: leaf_fields.subject,
                        scope_root: leaf_fields.scope_root,
                        cap_set: leaf_cap_set,
                        expires_at: authority_expires_at,
                    },
                },
                founder_anchor_entity: current_outer_id,
                founder_anchor_cap: current_cap_handle,
                founder_anchor_proof: current_proof,
            });
        }

        // A configured root may sign only an explicit founder anchor. A finite
        // root-signed cap would quietly restore the old "root as an online
        // issuer" model, so it is not a valid terminator.
        if expected_team_root.is_some_and(|expected| current_signer == expected) {
            return Err(VerifyError::FounderAnchorRequired);
        }

        // The current cap needs a parent, but fetching one would exceed
        // the total-level bound (the leaf counts as level one).
        if depth >= MAX_CHAIN_DEPTH {
            return Err(VerifyError::ChainTooDeep);
        }

        // Non-root: the current outer entity must carry sig_parent_cap
        // + sig_embedded_parent_proof pointing at the next sub-entity.
        let mut parent_iter = find!(
            (ph: Inline<Handle<SimpleArchive>>, pid: crate::id::Id),
            pattern!(&sig_set, [{
                current_outer_id @
                sig_parent_cap: ?ph,
                sig_embedded_parent_proof: ?pid,
            }])
        );
        let (parent_cap_handle, parent_proof_id) = match (parent_iter.next(), parent_iter.next()) {
            (Some(row), None) => row,
            _ => return Err(VerifyError::NonRootMissingParent),
        };
        if canonical_sig_entity_id(&current_proof, current_cap_handle) != current_outer_id {
            return Err(VerifyError::MalformedSig);
        }
        proof_shape
            .last_mut()
            .expect("current proof entity is recorded")
            .1 = true;

        // Fetch + decode the parent cap.
        let parent_cap_blob =
            fetch_blob(parent_cap_handle).ok_or(VerifyError::MissingBlob(parent_cap_handle))?;
        let parent_cap_set: TribleSet = TryFromBlob::try_from_blob(parent_cap_blob.clone())?;
        let parent_fields = extract_cap_fields(&parent_cap_set)?;

        // Verify the parent proof's sig attests to the parent cap's
        // bytes, signed by some authority.
        let parent_proof = extract_and_verify_sig_at(&sig_set, parent_proof_id, &parent_cap_blob)?;
        let parent_signer = parent_proof.signer;
        match &parent_fields.kind {
            CapKind::FounderAnchor(anchor) => {
                if parent_signer != parent_fields.issuer
                    || anchor.issuer != parent_signer
                    || expected_team_root.is_some_and(|expected| expected != parent_signer)
                {
                    return Err(VerifyError::InvalidFounderAnchor);
                }
            }
            CapKind::Operational { .. } => {
                if parent_signer != parent_fields.issuer {
                    return Err(VerifyError::IssuerMismatch);
                }
            }
        }
        if parent_fields.subject != current_fields.issuer {
            return Err(VerifyError::DelegationMismatch);
        }
        if let CapKind::Operational { expiry } = &parent_fields.kind {
            let parent_expires_at = expiry_upper(expiry)?;
            if parent_expires_at < authority_expires_at {
                authority_expires_at = parent_expires_at;
            }
        }
        // Each child link's scope must be a subset of its parent's.
        if !scope_subsumes(
            &parent_cap_set,
            parent_fields.scope_root,
            &current_cap_set,
            current_fields.scope_root,
        ) {
            return Err(VerifyError::ScopeNotSubset);
        }

        // Step.
        current_outer_id = parent_proof_id;
        current_proof = parent_proof;
        current_cap_handle = parent_cap_handle;
        proof_shape.push((parent_proof_id, false));
        current_signer = parent_signer;
        current_cap_set = parent_cap_set;
        current_fields = parent_fields;
        depth += 1;
    }
}

/// The signature proof's entity identifiers are themselves part of the
/// canonical container. Each id is derived when its standalone signature
/// entity contains only `sig_signs`, `signed_by`, `signature_r`, and
/// `signature_s`; parent links are attached afterwards under that forced id.
/// Embedded proof entities omit `sig_signs` physically, so reconstruct exactly
/// those original four facts at every level. The separately validated linear
/// links then make the whole proof shape unique; merely renaming proof entities
/// cannot mint an equivalent credential handle.
fn canonical_sig_entity_id(
    proof: &VerifiedSigProof,
    signed_cap: Inline<Handle<SimpleArchive>>,
) -> crate::id::Id {
    let signature = Signature::from_components(proof.r, proof.s);
    let fragment = entity! {
        sig_signs: signed_cap,
        crate::repo::signed_by: proof.signer,
        crate::repo::signature_r: signature,
        crate::repo::signature_s: signature,
    };
    fragment
        .root()
        .expect("canonical signature entity exports exactly one intrinsic id")
}

/// Reconstruct the canonical standalone signature container for one proof
/// entity. Callers must first prove that `entity` is the canonical id for
/// `(proof, signed_cap)`; [`verify_chain_inner`] does so at every traversed
/// level and invokes this helper only for the verified founder terminator.
fn standalone_signature_blob(
    entity: crate::id::Id,
    signed_cap: Inline<Handle<SimpleArchive>>,
    proof: &VerifiedSigProof,
) -> Blob<SimpleArchive> {
    let signature = Signature::from_components(proof.r, proof.s);
    let set: TribleSet = entity! {
        ExclusiveId::force_ref(&entity) @
        sig_signs: signed_cap,
        crate::repo::signed_by: proof.signer,
        crate::repo::signature_r: signature,
        crate::repo::signature_s: signature,
    }
    .into();
    set.to_blob()
}

/// Require the signature container to be exactly the one linear proof chain
/// consumed above. Cap blobs deliberately admit scope facts; sig blobs do not
/// carry extensible application data, so accepting unrelated facts here would
/// make the credential handle malleable without a new signature.
fn validate_sig_proof_shape(
    sig_set: &TribleSet,
    leaf_outer_id: crate::id::Id,
    proof_shape: &[(crate::id::Id, bool)],
) -> Result<(), VerifyError> {
    for (entity, has_parent) in proof_shape {
        let expected =
            if *entity == leaf_outer_id { 4 } else { 3 } + if *has_parent { 2 } else { 0 };
        let mut actual = 0usize;
        for fact in sig_set.iter().filter(|fact| fact.e() == entity) {
            actual += 1;
            let attribute = *fact.a();
            let base = attribute == crate::repo::signed_by.id()
                || attribute == crate::repo::signature_r.id()
                || attribute == crate::repo::signature_s.id()
                || (*entity == leaf_outer_id && attribute == sig_signs.id());
            let parent = *has_parent
                && (attribute == sig_parent_cap.id()
                    || attribute == sig_embedded_parent_proof.id());
            if !base && !parent {
                return Err(VerifyError::MalformedSig);
            }
        }
        if actual != expected {
            return Err(VerifyError::MalformedSig);
        }
    }
    if sig_set
        .iter()
        .any(|fact| !proof_shape.iter().any(|(entity, _)| fact.e() == entity))
    {
        return Err(VerifyError::MalformedSig);
    }
    Ok(())
}

/// Extract a `(signed_by, signature_r, signature_s)` from a specific
/// entity inside a sig blob's TribleSet, verify it's a valid signature
/// over `signed_blob.bytes`, and return the signer.
struct VerifiedSigProof {
    signer: VerifyingKey,
    r: ed25519::ComponentBytes,
    s: ed25519::ComponentBytes,
}

fn extract_and_verify_sig_at(
    sig_set: &TribleSet,
    entity: crate::id::Id,
    signed_blob: &Blob<SimpleArchive>,
) -> Result<VerifiedSigProof, VerifyError> {
    let mut iter = find!(
        (signer: VerifyingKey, r, s),
        pattern!(sig_set, [{
            entity @
            crate::repo::signed_by: ?signer,
            crate::repo::signature_r: ?r,
            crate::repo::signature_s: ?s,
        }])
    );
    let (signer, r, s) = match (iter.next(), iter.next()) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedSig),
    };
    let signature = Signature::from_components(r, s);
    signer
        .verify(&signed_blob.bytes, &signature)
        .map_err(|_| VerifyError::BadSignature)?;
    Ok(VerifiedSigProof { signer, r, s })
}

#[cfg(test)]
mod tests {
    //! Tests for the descriptive-caps shape: cap blobs are pure
    //! declarations; sig blobs carry the chain proof as recursive
    //! embedded sub-entities. See decide#5ed64e57.
    use super::*;
    use crate::inline::TryToInline;
    use ed25519_dalek::SigningKey;
    use hifitime::Epoch;
    use rand::rngs::OsRng;
    use std::collections::HashMap;

    fn key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn interval(seconds_from_now: f64) -> Inline<NsTAIInterval> {
        let now = Epoch::now().expect("system time");
        let later = now + hifitime::Duration::from_seconds(seconds_from_now);
        (now, later).try_to_inline().expect("valid interval")
    }

    fn expired_interval() -> Inline<NsTAIInterval> {
        let now = Epoch::now().expect("system time");
        let past_start = now - hifitime::Duration::from_seconds(7200.0);
        let past_end = now - hifitime::Duration::from_seconds(3600.0);
        (past_start, past_end)
            .try_to_inline()
            .expect("valid interval")
    }

    fn empty_scope() -> (Id, TribleSet) {
        let scope_root = crate::id::ufoid();
        let facts = TribleSet::from(entity! { ExclusiveId::force_ref(&scope_root) @
            crate::metadata::tag: PERM_READ,
        });
        (*scope_root, facts)
    }

    fn scope_with(perms: &[Id], branches: &[Id]) -> (Id, TribleSet) {
        let root = crate::id::ufoid();
        let mut facts = TribleSet::new();
        for permission in perms {
            facts += TribleSet::from(entity! {
                ExclusiveId::force_ref(&root) @
                crate::metadata::tag: *permission,
            });
        }
        for branch in branches {
            facts += TribleSet::from(entity! {
                ExclusiveId::force_ref(&root) @
                scope_branch: *branch,
            });
        }
        (*root, facts)
    }

    fn anchor_for(
        team_root: &SigningKey,
        founder: &SigningKey,
    ) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
        let (scope_root, scope_facts) = scope_with(&[PERM_ADMIN], &[]);
        build_founder_anchor(team_root, founder.verifying_key(), scope_root, scope_facts)
            .expect("build founder anchor")
    }

    #[test]
    fn restricted_admin_never_bypasses_branch_or_known_permission_checks() {
        let branch_a = *crate::id::ufoid();
        let branch_b = *crate::id::ufoid();
        let unknown_permission = *crate::id::ufoid();
        let (parent_root, parent) = scope_with(&[PERM_ADMIN], &[branch_a]);

        let (same_root, same) = scope_with(&[PERM_WRITE], &[branch_a]);
        assert!(scope_subsumes(&parent, parent_root, &same, same_root));

        let (unrestricted_root, unrestricted) = scope_with(&[PERM_READ], &[]);
        assert!(!scope_subsumes(
            &parent,
            parent_root,
            &unrestricted,
            unrestricted_root
        ));

        let (other_root, other) = scope_with(&[PERM_READ], &[branch_b]);
        assert!(!scope_subsumes(&parent, parent_root, &other, other_root));

        let (unknown_root, unknown) = scope_with(&[unknown_permission], &[branch_a]);
        assert!(!scope_subsumes(
            &parent,
            parent_root,
            &unknown,
            unknown_root
        ));
    }

    /// Build a fetch_blob closure backed by an in-memory map.
    fn fetch_from(
        blobs: &[Blob<SimpleArchive>],
    ) -> impl FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>> + '_ {
        let map: HashMap<_, _> = blobs
            .iter()
            .map(|b| {
                let h: Inline<Handle<SimpleArchive>> = b.get_handle();
                (h.raw, b.clone())
            })
            .collect();
        move |h| map.get(&h.raw).cloned()
    }

    // ── Length-1 chain ────────────────────────────────────────────────

    #[test]
    fn founder_anchor_is_not_an_operational_leaf() {
        let team_root = key();
        let founder = key();
        let (cap_blob, sig_blob) = anchor_for(&team_root, &founder);
        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
        let blobs = [cap_blob, sig_blob];

        let err = verify_chain(
            team_root.verifying_key(),
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("the constitutional anchor is not an auth credential");

        assert!(matches!(err, VerifyError::FounderAnchorAsLeaf));
    }

    #[test]
    fn finite_founder_self_cap_verifies_under_anchor() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap, sig) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build finite founder credential");
        let sig_handle = sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, cap, sig];

        let verified = verify_chain(
            team_root.verifying_key(),
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("finite founder credential verifies");

        assert_eq!(verified.subject, founder.verifying_key());
        assert_eq!(verified.scope_root, scope_root);
    }

    #[test]
    fn expired_tolerant_walk_extracts_the_unique_anchor_root_and_leaf_identity() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap, sig) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build finite founder credential");
        let cap_handle = cap.get_handle();
        let sig_handle = sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, cap, sig];

        let discovered = verify_chain_and_extract_root_allow_expired(
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("canonical founder proof discovers its root");

        assert_eq!(discovered.team_root, team_root.verifying_key());
        assert_eq!(discovered.leaf_cap, cap_handle);
        assert_eq!(discovered.leaf_issuer, founder.verifying_key());
        assert_eq!(discovered.capability.subject, founder.verifying_key());
        assert_eq!(discovered.capability.scope_root, scope_root);

        // A mid-chain signer cannot masquerade as the root: when supplied as
        // the configured root it reaches an operational cap, not the required
        // constitutional founder anchor.
        assert!(matches!(
            verify_chain_allow_expired(
                founder.verifying_key(),
                sig_handle,
                founder.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::FounderAnchorRequired)
        ));
    }

    #[test]
    fn verified_founder_leaf_reconstructs_exact_standalone_anchor_signature() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap, sig) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build finite founder credential");
        let sig_handle = sig.get_handle();
        let blobs = [anchor_cap, anchor_sig.clone(), cap, sig];

        let reconstructed = verify_chain_and_reconstruct_founder_anchor_allow_expired(
            team_root.verifying_key(),
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("verified proof reconstructs its founder anchor");

        assert_eq!(reconstructed.founder_anchor_sig.bytes, anchor_sig.bytes);
        assert_eq!(
            reconstructed.founder_anchor_sig.get_handle(),
            anchor_sig.get_handle()
        );
    }

    #[test]
    fn deep_verified_leaf_reconstructs_terminal_anchor_and_keeps_trust_binding() {
        let (team_root, _founder, leaf_subject, blobs, leaf_sig_handle) = three_level_chain();
        let anchor_sig = blobs[1].clone();

        let reconstructed = verify_chain_and_reconstruct_founder_anchor_allow_expired(
            team_root.verifying_key(),
            leaf_sig_handle,
            leaf_subject.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("deep proof reconstructs the terminal anchor");
        assert_eq!(reconstructed.founder_anchor_sig.bytes, anchor_sig.bytes);

        let wrong_root = key();
        assert!(matches!(
            verify_chain_and_reconstruct_founder_anchor_allow_expired(
                wrong_root.verifying_key(),
                leaf_sig_handle,
                leaf_subject.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::InvalidFounderAnchor)
        ));

        let wrong_subject = key();
        assert!(matches!(
            verify_chain_and_reconstruct_founder_anchor_allow_expired(
                team_root.verifying_key(),
                leaf_sig_handle,
                wrong_subject.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::SubjectMismatch)
        ));
    }

    #[test]
    fn details_allow_expired_matches_projection_and_reports_leaf_identity() {
        let team_root = key();
        let founder = key();
        let subject = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap, sig) = build_capability(
            &founder,
            subject.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            expired_interval(),
        )
        .expect("build expired finite capability");
        let cap_handle = cap.get_handle();
        let sig_handle = sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, cap, sig];

        let details = verify_chain_details_allow_expired(
            team_root.verifying_key(),
            sig_handle,
            subject.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("details wrapper accepts an otherwise-valid expired proof");
        let projected = verify_chain_allow_expired(
            team_root.verifying_key(),
            sig_handle,
            subject.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("legacy projection accepts the same expired proof");

        assert_eq!(details.team_root, team_root.verifying_key());
        assert_eq!(details.leaf_cap, cap_handle);
        assert_eq!(details.leaf_issuer, founder.verifying_key());
        assert_eq!(details.capability.subject, projected.subject);
        assert_eq!(details.capability.scope_root, projected.scope_root);
        assert_eq!(details.capability.cap_set, projected.cap_set);
        assert_eq!(details.capability.expires_at(), projected.expires_at());
        assert!(details.capability.is_expired());
    }

    #[test]
    fn operational_capability_decode_preserves_claim_and_rejects_anchor() {
        let team_root = key();
        let founder = key();
        let subject = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let expiry = interval(3600.0);
        let (expected_lower, expected_upper) =
            <(Epoch, Epoch)>::try_from_inline(&expiry).expect("decode test interval");
        let (cap, _sig) = build_capability(
            &founder,
            subject.verifying_key(),
            (anchor_cap.clone(), anchor_sig),
            scope_root,
            scope_facts,
            expiry,
        )
        .expect("build finite capability claim");

        let decoded = decode_operational_capability(cap.clone())
            .expect("finite operational capability decodes");
        let expected_set: TribleSet = TryFromBlob::try_from_blob(cap).expect("decode cap set");
        let expected_entity = find!(
            (entity: Id, subject: VerifyingKey),
            pattern!(&expected_set, [{ ?entity @ cap_subject: ?subject }])
        )
        .map(|(entity, _)| entity)
        .next()
        .expect("capability declaration entity");

        assert_eq!(decoded.entity, expected_entity);
        assert_eq!(decoded.subject, subject.verifying_key());
        assert_eq!(decoded.issuer, founder.verifying_key());
        assert_eq!(decoded.scope_root, scope_root);
        assert_eq!(decoded.cap_set, expected_set);
        assert_eq!(decoded.expiry, expiry);
        assert_eq!(decoded.valid_from, expected_lower);
        assert_eq!(decoded.expires_at, expected_upper);
        assert!(matches!(
            decode_operational_capability(anchor_cap),
            Err(VerifyError::FounderAnchorAsLeaf)
        ));
    }

    #[test]
    fn inverted_operational_interval_fails_parser_and_details_verifier() {
        let team_root = key();
        let founder = key();
        let subject = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let valid = interval(3600.0);
        let mut inverted_raw = valid.raw;
        inverted_raw[..16].copy_from_slice(&valid.raw[16..]);
        inverted_raw[16..].copy_from_slice(&valid.raw[..16]);
        let inverted = Inline::<NsTAIInterval>::new(inverted_raw);
        assert!(!inverted.is_valid(), "fixture must actually be inverted");

        let (cap, sig) = build_capability(
            &founder,
            subject.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            inverted,
        )
        .expect("the builder signs typed bytes; verification validates them");
        let sig_handle = sig.get_handle();

        assert!(matches!(
            decode_operational_capability(cap.clone()),
            Err(VerifyError::Expired)
        ));

        let blobs = [anchor_cap, anchor_sig, cap, sig];
        assert!(matches!(
            verify_chain_details_allow_expired(
                team_root.verifying_key(),
                sig_handle,
                subject.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::Expired)
        ));
    }

    #[test]
    fn root_signed_finite_cap_cannot_replace_founder_anchor() {
        let team_root = key();
        let founder = key();
        let (scope_root, scope_facts) = empty_scope();
        let mut cap_set = TribleSet::from(entity! {
            cap_subject: issuer_subject_value(founder.verifying_key()),
            cap_issuer: issuer_subject_value(team_root.verifying_key()),
            cap_scope_root: scope_root,
            crate::metadata::expires_at: interval(3600.0),
        });
        cap_set += scope_facts;
        let (cap, sig) = build_signed_claim(&team_root, cap_set, None)
            .expect("build structurally valid but unanchored cap");
        let sig_handle = sig.get_handle();
        let blobs = [cap, sig];

        assert!(matches!(
            verify_chain(
                team_root.verifying_key(),
                sig_handle,
                founder.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::FounderAnchorRequired)
        ));
    }

    #[test]
    fn founder_rotation_uses_anchor_siblings_at_constant_depth() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let now = crate::clock::epoch_now();
        let first_upper = now + hifitime::Duration::from_seconds(60.0);
        let second_upper = now + hifitime::Duration::from_seconds(7200.0);

        let (first_scope, first_facts) = empty_scope();
        let (first_cap, first_sig) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            first_scope,
            first_facts,
            (now, first_upper).try_to_inline().unwrap(),
        )
        .expect("build first operational sibling");
        let (second_scope, second_facts) = empty_scope();
        let (second_cap, second_sig) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            second_scope,
            second_facts,
            (now, second_upper).try_to_inline().unwrap(),
        )
        .expect("build rotated operational sibling");

        let first_handle = first_sig.get_handle();
        let first_blobs = [
            anchor_cap.clone(),
            anchor_sig.clone(),
            first_cap,
            first_sig.clone(),
        ];
        let first = verify_chain(
            team_root.verifying_key(),
            first_handle,
            founder.verifying_key(),
            fetch_from(&first_blobs),
        )
        .expect("first sibling verifies");

        // Deliberately omit the predecessor: rotation is a new sibling under
        // the same anchor, not an ever-growing renewal chain.
        let second_handle = second_sig.get_handle();
        let second_blobs = [anchor_cap, anchor_sig, second_cap, second_sig.clone()];
        let second = verify_chain(
            team_root.verifying_key(),
            second_handle,
            founder.verifying_key(),
            fetch_from(&second_blobs),
        )
        .expect("rotated sibling verifies without predecessor");

        assert_eq!(first.expires_at(), first_upper);
        assert_eq!(second.expires_at(), second_upper);
        for proof in [&first_sig, &second_sig] {
            let set: TribleSet = TryFromBlob::try_from_blob(proof.clone()).unwrap();
            let levels = find!(
                (entity: Id, signer: VerifyingKey),
                pattern!(&set, [{ ?entity @ crate::repo::signed_by: ?signer }])
            )
            .count();
            assert_eq!(levels, 2, "each sibling remains anchor + finite leaf");
        }
    }

    fn verify_with_anchor_parent(
        team_root: &SigningKey,
        founder: &SigningKey,
        anchor_cap: Blob<SimpleArchive>,
        anchor_sig: Blob<SimpleArchive>,
    ) -> Result<VerifiedCapability, VerifyError> {
        let (scope_root, scope_facts) = empty_scope();
        let (leaf_cap, leaf_sig) = build_capability(
            founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build child of candidate anchor");
        let leaf_handle = leaf_sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, leaf_cap, leaf_sig];
        verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
    }

    #[test]
    fn nonroot_and_malformed_founder_anchors_fail_closed() {
        let team_root = key();
        let founder = key();
        let nonroot = key();

        let (scope_root, scope_facts) = scope_with(&[PERM_ADMIN], &[]);
        let (nonroot_cap, nonroot_sig) =
            build_founder_anchor(&nonroot, founder.verifying_key(), scope_root, scope_facts)
                .expect("build nonroot candidate");
        assert!(matches!(
            verify_with_anchor_parent(&team_root, &founder, nonroot_cap, nonroot_sig),
            Err(VerifyError::InvalidFounderAnchor)
        ));

        // Even a real root signature cannot rescue an anchor whose declaration
        // names another issuer.
        let (scope_root, scope_facts) = scope_with(&[PERM_ADMIN], &[]);
        let mut bad_issuer_set = TribleSet::from(entity! {
            cap_subject: issuer_subject_value(founder.verifying_key()),
            cap_issuer: issuer_subject_value(nonroot.verifying_key()),
            cap_scope_root: scope_root,
            crate::metadata::tag: KIND_FOUNDER_ANCHOR,
        });
        bad_issuer_set += scope_facts;
        let (bad_issuer_cap, bad_issuer_sig) =
            build_signed_claim(&team_root, bad_issuer_set, None).unwrap();
        assert!(matches!(
            verify_with_anchor_parent(&team_root, &founder, bad_issuer_cap, bad_issuer_sig),
            Err(VerifyError::InvalidFounderAnchor)
        ));

        let (valid_cap, _valid_sig) = anchor_for(&team_root, &founder);
        let mut mixed_set: TribleSet = TryFromBlob::try_from_blob(valid_cap).unwrap();
        let anchor_entity = find!(
            (entity: Id, subject: VerifyingKey),
            pattern!(&mixed_set, [{ ?entity @ cap_subject: ?subject }])
        )
        .map(|(entity, _)| entity)
        .next()
        .unwrap();
        mixed_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&anchor_entity) @
            crate::metadata::expires_at: interval(3600.0),
        });
        let (mixed_cap, mixed_sig) = build_signed_claim(&team_root, mixed_set, None).unwrap();
        assert!(matches!(
            verify_with_anchor_parent(&team_root, &founder, mixed_cap, mixed_sig),
            Err(VerifyError::MalformedCap)
        ));

        let (valid_cap, _valid_sig) = anchor_for(&team_root, &founder);
        let mut extra_tag_set: TribleSet = TryFromBlob::try_from_blob(valid_cap).unwrap();
        let anchor_entity = find!(
            (entity: Id, subject: VerifyingKey),
            pattern!(&extra_tag_set, [{ ?entity @ cap_subject: ?subject }])
        )
        .map(|(entity, _)| entity)
        .next()
        .unwrap();
        extra_tag_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&anchor_entity) @
            crate::metadata::tag: PERM_ADMIN,
        });
        let (extra_tag_cap, extra_tag_sig) =
            build_signed_claim(&team_root, extra_tag_set, None).unwrap();
        assert!(matches!(
            verify_with_anchor_parent(&team_root, &founder, extra_tag_cap, extra_tag_sig),
            Err(VerifyError::MalformedCap)
        ));
    }

    #[test]
    fn signature_proof_rejects_unsigned_extra_facts() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap_blob, sig_blob) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build");

        let mut sig_set: TribleSet = TryFromBlob::try_from_blob(sig_blob).expect("parse sig");
        let unrelated = crate::id::ufoid();
        sig_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&unrelated) @
            crate::metadata::tag: PERM_READ,
        });
        let malleated_sig: Blob<SimpleArchive> = sig_set.to_blob();
        let malleated_handle = malleated_sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, cap_blob, malleated_sig];

        assert!(matches!(
            verify_chain(
                team_root.verifying_key(),
                malleated_handle,
                founder.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::MalformedSig)
        ));
    }

    #[test]
    fn signature_proof_rejects_renamed_intrinsic_entities() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();
        let (cap_blob, sig_blob) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build");

        let original_handle = sig_blob.get_handle();
        let original_blobs = [
            anchor_cap.clone(),
            anchor_sig.clone(),
            cap_blob.clone(),
            sig_blob.clone(),
        ];
        verify_chain(
            team_root.verifying_key(),
            original_handle,
            founder.verifying_key(),
            fetch_from(&original_blobs),
        )
        .expect("the canonical proof verifies before entity renaming");

        let sig_set: TribleSet = TryFromBlob::try_from_blob(sig_blob).expect("parse sig");
        let leaf_entity = find!(
            (entity: Id, cap: Inline<Handle<SimpleArchive>>),
            pattern!(&sig_set, [{ ?entity @ sig_signs: ?cap }])
        )
        .map(|(entity, _cap)| entity)
        .next()
        .expect("leaf proof entity");
        let renamed = *crate::id::ufoid();
        let mut renamed_set = TribleSet::new();
        for fact in sig_set.iter() {
            let mut raw = fact.data;
            if *fact.e() == leaf_entity {
                raw[..crate::id::ID_LEN].copy_from_slice(&renamed[..]);
            }
            let renamed_fact = crate::trible::Trible::force_raw(raw).expect("valid renamed fact");
            renamed_set.insert(&renamed_fact);
        }
        let malleated_sig: Blob<SimpleArchive> = renamed_set.to_blob();
        let malleated_handle = malleated_sig.get_handle();
        let blobs = [anchor_cap, anchor_sig, cap_blob, malleated_sig];

        assert!(matches!(
            verify_chain(
                team_root.verifying_key(),
                malleated_handle,
                founder.verifying_key(),
                fetch_from(&blobs),
            ),
            Err(VerifyError::MalformedSig)
        ));
    }

    // ── Length-N chain ────────────────────────────────────────────────

    fn three_level_chain() -> (
        SigningKey,
        SigningKey,
        SigningKey,
        Vec<Blob<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
    ) {
        let team_root = key();
        let a = key();
        let b = key();

        // Level 1: the root's non-expiring founder anchor for A.
        let (cap_a, sig_a) = anchor_for(&team_root, &a);

        // Level 2: A → B (subject = B)
        let (scope2_root, scope2_facts) = empty_scope();
        let (cap_b, sig_b) = build_capability(
            &a,
            b.verifying_key(),
            (cap_a.clone(), sig_a.clone()),
            scope2_root,
            scope2_facts,
            interval(3600.0),
        )
        .expect("build level-2");

        let leaf_sig_handle: Inline<Handle<SimpleArchive>> = (&sig_b).get_handle();
        let blobs = vec![cap_a, sig_a, cap_b, sig_b];
        (team_root, a, b, blobs, leaf_sig_handle)
    }

    fn chain_with_level_count(
        levels: usize,
    ) -> (
        SigningKey,
        VerifyingKey,
        Vec<Blob<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
        Inline<Handle<SimpleArchive>>,
    ) {
        assert!(
            levels >= 2,
            "a valid chain needs an anchor and a finite leaf"
        );

        let team_root = key();
        let subjects: Vec<SigningKey> = (0..levels).map(|_| key()).collect();
        let mut blobs = Vec::with_capacity(levels * 2);
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &subjects[0]);
        let root_cap_handle = anchor_cap.get_handle();
        blobs.push(anchor_cap.clone());
        blobs.push(anchor_sig.clone());
        let mut parent = (anchor_cap, anchor_sig);

        for level in 1..levels {
            let (scope_root, scope_facts) = empty_scope();
            let (cap, sig) = build_capability(
                &subjects[level - 1],
                subjects[level].verifying_key(),
                parent,
                scope_root,
                scope_facts,
                interval(3600.0),
            )
            .expect("build capability level");

            blobs.push(cap.clone());
            blobs.push(sig.clone());
            parent = (cap, sig);
        }

        let leaf_sig_handle = parent.1.get_handle();
        (
            team_root,
            subjects
                .last()
                .expect("at least one subject")
                .verifying_key(),
            blobs,
            leaf_sig_handle,
            root_cap_handle,
        )
    }

    #[test]
    fn length_three_chain_round_trips() {
        let (team_root, _a, b, blobs, leaf_sig_handle) = three_level_chain();

        let verified = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("verify");

        assert_eq!(verified.subject, b.verifying_key());
    }

    #[test]
    fn verified_authority_expires_at_earliest_chain_deadline() {
        let team_root = key();
        let founder = key();
        let intermediary = key();
        let member = key();
        let now = crate::clock::epoch_now();
        let parent_expires_at = now + hifitime::Duration::from_seconds(60.0);
        let leaf_expires_at = now + hifitime::Duration::from_seconds(3600.0);

        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (parent_scope, parent_facts) = empty_scope();
        let (parent_cap, parent_sig) = build_capability(
            &founder,
            intermediary.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            parent_scope,
            parent_facts,
            (now, parent_expires_at).try_to_inline().unwrap(),
        )
        .expect("build parent");
        let (leaf_scope, leaf_facts) = empty_scope();
        let (leaf_cap, leaf_sig) = build_capability(
            &intermediary,
            member.verifying_key(),
            (parent_cap.clone(), parent_sig.clone()),
            leaf_scope,
            leaf_facts,
            (now, leaf_expires_at).try_to_inline().unwrap(),
        )
        .expect("build leaf");
        let leaf_sig_handle = leaf_sig.get_handle();
        let blobs = [
            anchor_cap, anchor_sig, parent_cap, parent_sig, leaf_cap, leaf_sig,
        ];

        let verified = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            member.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("verify delegated chain");

        assert_eq!(verified.expires_at(), parent_expires_at);
        assert!(!verified.is_expired_at(parent_expires_at));
        assert!(
            verified.is_expired_at(parent_expires_at + hifitime::Duration::from_nanoseconds(1.0))
        );
    }

    #[test]
    fn chain_depth_bound_counts_leaf_and_stops_before_extra_fetch() {
        let (team_root, subject, blobs, leaf_sig_handle, _root_cap_handle) =
            chain_with_level_count(MAX_CHAIN_DEPTH);
        verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            subject,
            fetch_from(&blobs),
        )
        .expect("a chain exactly at the level bound must verify");

        let (team_root, subject, blobs, leaf_sig_handle, root_cap_handle) =
            chain_with_level_count(MAX_CHAIN_DEPTH + 1);
        let mut fetch = fetch_from(&blobs);
        let mut requested = Vec::new();
        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            subject,
            |handle| {
                requested.push(handle);
                fetch(handle)
            },
        )
        .expect_err("a chain one level over the bound must be rejected");

        assert!(matches!(err, VerifyError::ChainTooDeep));
        assert_eq!(
            requested.len(),
            MAX_CHAIN_DEPTH + 1,
            "one leaf sig plus exactly MAX_CHAIN_DEPTH cap lookups"
        );
        assert!(
            !requested.contains(&root_cap_handle),
            "the out-of-bound parent cap must not be fetched"
        );
    }

    #[test]
    fn missing_blob_reports_each_requested_handle_in_order() {
        let (team_root, _a, b, blobs, leaf_sig_handle) = three_level_chain();
        let parent_cap_handle: Inline<Handle<SimpleArchive>> = (&blobs[0]).get_handle();
        let embedded_parent_sig_handle: Inline<Handle<SimpleArchive>> = (&blobs[1]).get_handle();
        let leaf_cap_handle: Inline<Handle<SimpleArchive>> = (&blobs[2]).get_handle();
        let leaf_sig_blob = blobs[3].clone();
        let leaf_cap_blob = blobs[2].clone();

        let mut requested = Vec::new();
        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            |handle| {
                requested.push(handle);
                None
            },
        )
        .expect_err("missing leaf sig must be reported");
        match err {
            VerifyError::MissingBlob(handle) => assert_eq!(handle, leaf_sig_handle),
            other => panic!("expected MissingBlob for leaf sig, got {other:?}"),
        }
        assert_eq!(requested, [leaf_sig_handle]);

        requested.clear();
        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            |handle| {
                requested.push(handle);
                (handle == leaf_sig_handle).then(|| leaf_sig_blob.clone())
            },
        )
        .expect_err("missing leaf cap must be reported");
        match err {
            VerifyError::MissingBlob(handle) => assert_eq!(handle, leaf_cap_handle),
            other => panic!("expected MissingBlob for leaf cap, got {other:?}"),
        }
        assert_eq!(requested, [leaf_sig_handle, leaf_cap_handle]);

        requested.clear();
        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            |handle| {
                requested.push(handle);
                if handle == leaf_sig_handle {
                    Some(leaf_sig_blob.clone())
                } else if handle == leaf_cap_handle {
                    Some(leaf_cap_blob.clone())
                } else {
                    None
                }
            },
        )
        .expect_err("missing parent cap must be reported");
        match err {
            VerifyError::MissingBlob(handle) => assert_eq!(handle, parent_cap_handle),
            other => panic!("expected MissingBlob for parent cap, got {other:?}"),
        }
        assert_eq!(
            requested,
            [leaf_sig_handle, leaf_cap_handle, parent_cap_handle]
        );
        assert!(
            !requested.contains(&embedded_parent_sig_handle),
            "the embedded parent signature must never be fetched"
        );
    }

    #[test]
    fn rejects_spliced_delegation_chain() {
        let team_root = key();
        let alice = key();
        let mallory = key();
        let bob = key();

        // The root authorizes Alice through the founder anchor.
        let (alice_cap, alice_sig) = anchor_for(&team_root, &alice);

        // Mallory splices Alice's valid proof into a leaf for Bob.
        // Every signature and issuer field is internally valid, but
        // the parent authorizes Alice rather than Mallory.
        let (bob_scope_root, bob_scope_facts) = empty_scope();
        let (bob_cap, bob_sig) = build_capability(
            &mallory,
            bob.verifying_key(),
            (alice_cap.clone(), alice_sig.clone()),
            bob_scope_root,
            bob_scope_facts,
            interval(3600.0),
        )
        .expect("build spliced Bob capability");

        let bob_sig_handle: Inline<Handle<SimpleArchive>> = (&bob_sig).get_handle();
        let blobs = [alice_cap, alice_sig, bob_cap, bob_sig];
        let err = verify_chain(
            team_root.verifying_key(),
            bob_sig_handle,
            bob.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("a signer not authorized by the parent must be rejected");

        assert!(matches!(err, VerifyError::DelegationMismatch));
    }

    #[test]
    fn rejects_subject_mismatch() {
        let (team_root, _a, _b, blobs, leaf_sig_handle) = three_level_chain();
        let imposter = key();

        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            imposter.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject subject mismatch");

        assert!(matches!(err, VerifyError::SubjectMismatch));
    }

    #[test]
    fn rejects_wrong_team_root() {
        let (_real_team_root, _a, b, blobs, leaf_sig_handle) = three_level_chain();
        let wrong_root = key();

        let err = verify_chain(
            wrong_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject wrong team root");

        assert!(matches!(err, VerifyError::InvalidFounderAnchor));
    }

    #[test]
    fn rejects_expired_leaf() {
        let team_root = key();
        let founder = key();
        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &founder);
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &founder,
            founder.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope_root,
            scope_facts,
            expired_interval(),
        )
        .expect("build");

        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
        let blobs = [anchor_cap, anchor_sig, cap_blob, sig_blob];

        let err = verify_chain(
            team_root.verifying_key(),
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject expired");

        assert!(matches!(err, VerifyError::Expired));

        let recovered = verify_chain_allow_expired(
            team_root.verifying_key(),
            sig_handle,
            founder.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("the explicit recovery API accepts an otherwise-valid proof");
        assert!(recovered.is_expired());

        // Expiry recovery does not soften cryptographic verification. Replace
        // only the leaf signature components while retaining a canonical proof
        // container and the same expired cap.
        let sig_set: TribleSet = TryFromBlob::try_from_blob(blobs[3].clone()).unwrap();
        let leaf_entity = find!(
            (entity: Id, cap: Inline<Handle<SimpleArchive>>),
            pattern!(&sig_set, [{ ?entity @ sig_signs: ?cap }])
        )
        .map(|(entity, _)| entity)
        .next()
        .unwrap();
        let mut tampered_set = TribleSet::new();
        for fact in sig_set.iter() {
            if *fact.e() == leaf_entity
                && (*fact.a() == crate::repo::signature_r.id()
                    || *fact.a() == crate::repo::signature_s.id())
            {
                continue;
            }
            tampered_set.insert(fact);
        }
        let bogus_signature = key().sign(b"not the expired capability");
        tampered_set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&leaf_entity) @
            crate::repo::signature_r: bogus_signature,
            crate::repo::signature_s: bogus_signature,
        });
        let tampered_sig: Blob<SimpleArchive> = tampered_set.to_blob();
        let tampered_handle = tampered_sig.get_handle();
        let tampered_blobs = [
            blobs[0].clone(),
            blobs[1].clone(),
            blobs[2].clone(),
            tampered_sig,
        ];
        assert!(matches!(
            verify_chain_allow_expired(
                team_root.verifying_key(),
                tampered_handle,
                founder.verifying_key(),
                fetch_from(&tampered_blobs),
            ),
            Err(VerifyError::BadSignature)
        ));
    }

    #[test]
    fn rejects_expired_intermediate() {
        // The anchor itself does not expire, but an expired finite A -> B
        // capability cannot authorize B's still-live capability for C.
        let team_root = key();
        let a = key();
        let b = key();
        let c = key();

        let (anchor_cap, anchor_sig) = anchor_for(&team_root, &a);

        let (scope1_root, scope1_facts) = empty_scope();
        let (cap_a, sig_a) = build_capability(
            &a,
            b.verifying_key(),
            (anchor_cap.clone(), anchor_sig.clone()),
            scope1_root,
            scope1_facts,
            expired_interval(),
        )
        .expect("build level-1");

        let (scope2_root, scope2_facts) = empty_scope();
        let (cap_b, sig_b) = build_capability(
            &b,
            c.verifying_key(),
            (cap_a.clone(), sig_a.clone()),
            scope2_root,
            scope2_facts,
            interval(3600.0),
        )
        .expect("build level-2");

        let leaf_sig_handle: Inline<Handle<SimpleArchive>> = (&sig_b).get_handle();
        let blobs = [anchor_cap, anchor_sig, cap_a, sig_a, cap_b, sig_b];

        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            c.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject expired intermediate");

        assert!(matches!(err, VerifyError::Expired));
    }

    // ── Structural checks ─────────────────────────────────────────────

    #[test]
    fn cap_blob_carries_no_chain_attributes() {
        // The whole point of the refactor: cap blobs are pure
        // declarations. Verify that even at depth > 1, the inner cap
        // blobs don't contain sig_parent_cap / sig_embedded_parent_proof
        // or any other chain reference.
        let (_team_root, _a, _b, blobs, _leaf_sig_handle) = three_level_chain();

        for blob in &blobs {
            let set: TribleSet = match TryFromBlob::try_from_blob(blob.clone()) {
                Ok(s) => s,
                Err(_) => continue, // not a SimpleArchive blob; skip
            };
            // If this set contains cap_subject, it's a cap blob —
            // those must NOT carry sig-blob-only attributes.
            let is_cap = find!(
                (e: Id, s: VerifyingKey),
                pattern!(&set, [{ ?e @ cap_subject: ?s }])
            )
            .next()
            .is_some();
            if !is_cap {
                continue;
            }

            let has_parent_link = find!(
                (e: Id, h: Inline<Handle<SimpleArchive>>),
                pattern!(&set, [{ ?e @ sig_parent_cap: ?h }])
            )
            .next()
            .is_some();
            assert!(
                !has_parent_link,
                "cap blob unexpectedly carries sig_parent_cap"
            );
        }
    }

    #[test]
    fn leaf_sig_blob_carries_full_chain() {
        // The leaf sig blob should carry every cap's handle in its
        // recursive embedded proof structure. Walk the structure and
        // confirm we see N entries for an N-deep chain.
        let (_team_root, _a, _b, blobs, leaf_sig_handle) = three_level_chain();

        let leaf_sig_blob = fetch_from(&blobs)(leaf_sig_handle).expect("fetch leaf sig");
        let sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob).expect("parse sig");

        // Count entities with signed_by — should be 2 (A signed cap_b,
        // team_root signed cap_a). Each level of the chain contributes
        // exactly one signed_by trible.
        let signed_by_entities: HashSet<Id> = find!(
            (e: Id, s: VerifyingKey),
            pattern!(&sig_set, [{ ?e @ crate::repo::signed_by: ?s }])
        )
        .map(|(e, _)| e)
        .collect();
        assert_eq!(
            signed_by_entities.len(),
            2,
            "expected 2 signed_by entities (one per chain level); got {}",
            signed_by_entities.len()
        );

        // Count entities with sig_parent_cap — should be 1 (the leaf's
        // outer entity points at A's cap; the embedded proof for A's
        // signature is itself the root level and has no further
        // sig_parent_cap).
        let parent_links: HashSet<Id> = find!(
            (e: Id, h: Inline<Handle<SimpleArchive>>),
            pattern!(&sig_set, [{ ?e @ sig_parent_cap: ?h }])
        )
        .map(|(e, _)| e)
        .collect();
        assert_eq!(
            parent_links.len(),
            1,
            "expected 1 sig_parent_cap entry for length-2 chain"
        );
    }
}
