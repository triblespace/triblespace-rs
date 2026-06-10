//! Capability-based authorization for triblespace networks.
//!
//! Implements a chain-of-trust capability system where:
//!
//! - A team has a single immutable root keypair (the "team root"), generated
//!   once at team creation and used to sign exactly one capability — the
//!   founder's. The team root never operates online; it's the constitutional
//!   document for the team's identity.
//! - All other capabilities chain off the founder's via delegation. Any holder
//!   of a capability can sign a sub-capability for someone else, as long as
//!   the sub-cap's scope is a subset of their own. Verification walks the
//!   chain back to the team root.
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

use crate::inline::encodings::ed25519 as ed;
use crate::blob::encodings::simplearchive::SimpleArchive;
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
    // of (subject, issuer, scope, expiry) — independent of which
    // authority chain endorses it. See sig_parent_cap below.

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

// ── Builder ──────────────────────────────────────────────────────────

use ed25519::Signature;
use ed25519_dalek::SigningKey;
use ed25519_dalek::VerifyingKey;
use ed25519::signature::Signer;

use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::blob::encodings::simplearchive::UnarchiveError;
use crate::id::ExclusiveId;
use crate::macros::entity;
use crate::macros::pattern;
use crate::query::find;
use crate::trible::TribleSet;
use crate::inline::Inline;
use crate::inline::IntoInline;
use crate::inline::encodings::time::NsTAIInterval;

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
/// - `cap_blob` carries the claim (subject pubkey, scope, expiry, parent
///   pointer, embedded parent signature). Its content-addressed handle is
///   what the sig blob attests to.
/// - `sig_blob` carries the issuer's signature over `cap_blob.bytes` plus
///   the issuer's pubkey, alongside a `sig_signs` handle pointing at the
///   cap blob.
///
/// `parent = None` constructs a root-issued capability: the issuer is
/// expected to be the team root keypair, and the resulting cap has no
/// `cap_parent` and no embedded parent signature. Verification terminates
/// at this link when the issuer pubkey matches the team root.
///
/// `parent = Some((parent_cap, parent_sig))` constructs a delegated
/// capability: the parent's signature is embedded inline in the new cap
/// blob (via [`cap_embedded_parent_sig`] pointing at a sub-entity carrying
/// `signed_by` + `signature_r` + `signature_s` reusing the existing
/// commit-signature attribute conventions) so verifiers can walk one level
/// up the chain without a separate fetch for the parent's signature.
///
/// `scope_facts` should be a TribleSet anchored at `scope_root` describing
/// the capability's scope (permission tags via [`crate::metadata::tag`],
/// optional resource restrictions via [`scope_branch`], etc.). The caller
/// is responsible for producing a scope that's a subset of any parent
/// scope; this builder does not enforce subsumption.
///
/// # Example
///
/// Mint a length-1 capability — team root signs the founder's cap
/// directly. The returned `(cap_blob, sig_blob)` pair is what callers
/// persist into the pile; the founder presents the sig blob's handle
/// at connection time.
///
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use triblespace_core::id::{ufoid, ExclusiveId};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::inline::TryToInline;
/// use triblespace_core::repo::capability::{build_capability, PERM_READ};
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
/// let (cap_blob, sig_blob) = build_capability(
///     &team_root,
///     founder.verifying_key(),
///     None, // no parent — direct child of the team root
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
    parent: Option<(Blob<SimpleArchive>, Blob<SimpleArchive>)>,
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
        let parent_cap_handle: Inline<Handle<SimpleArchive>> =
            parent_cap_blob.get_handle();

        let parent_sig_set: TribleSet =
            TryFromBlob::<SimpleArchive>::try_from_blob(parent_sig_blob)
                .map_err(BuildError::ParseParentSig)?;

        // Locate the parent's outer leaf entity (the one with sig_signs).
        let mut parent_outer_iter = find!(
            (sig: crate::id::Id, _signed: Inline<Handle<SimpleArchive>>),
            pattern!(&parent_sig_set, [{ ?sig @ sig_signs: ?_signed }])
        )
        .map(|(sig, _)| sig);
        let parent_outer_id = match (
            parent_outer_iter.next(),
            parent_outer_iter.next(),
        ) {
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
/// - If parent grants `PERM_ADMIN`, parent subsumes every child scope.
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
    let (parent_perms, parent_branches) =
        collect_scope_facts(parent_set, parent_scope_root);
    let (child_perms, child_branches) =
        collect_scope_facts(child_set, child_scope_root);

    if parent_perms.contains(&PERM_ADMIN) {
        return true;
    }

    for perm in &child_perms {
        if *perm == PERM_READ {
            if !parent_perms.contains(&PERM_READ)
                && !parent_perms.contains(&PERM_WRITE)
            {
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
        } else {
            // Unknown permission — fail closed.
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

use ed25519_dalek::Verifier;
use std::collections::HashSet;
use crate::inline::TryFromInline;
use hifitime::Epoch;

/// Errors returned by [`verify_chain`].
#[derive(Debug)]
pub enum VerifyError {
    /// The leaf or some intermediate sig/cap blob could not be parsed
    /// as a valid SimpleArchive.
    ParseBlob(UnarchiveError),
    /// Fetching a referenced blob (cap or sig) from the caller-supplied
    /// fetch function failed.
    Fetch,
    /// A signature failed to verify against the expected pubkey + cap
    /// blob bytes.
    BadSignature,
    /// The leaf cap's subject did not match the expected (connecting)
    /// peer pubkey.
    SubjectMismatch,
    /// A cap's `cap_issuer` did not match the accompanying sig's
    /// `signed_by`.
    IssuerMismatch,
    /// A cap or one of its parent caps has expired.
    Expired,
    /// A child cap's scope was not a subset of its parent's scope.
    /// (Enforcement deferred to the scope-subsumption module — for now
    /// this variant is reserved for future use.)
    ScopeNotSubset,
    /// A cap blob is missing required attributes (e.g. cap_subject,
    /// cap_issuer, cap_scope_root, expires_at) or has multiple
    /// conflicting values.
    MalformedCap,
    /// A sig blob is missing required attributes or has multiple
    /// conflicting values.
    MalformedSig,
    /// The leaf sig blob refers to a cap blob whose handle the verifier
    /// could not retrieve.
    LeafCapMissing,
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
}

impl VerifiedCapability {
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
        if branches.is_empty() { None } else { Some(branches) }
    }

    /// Returns `true` if the cap grants any read-equivalent permission
    /// (read, write, or admin — write/admin imply read, matching the
    /// subsumption rules in [`scope_subsumes`]).
    pub fn grants_read(&self) -> bool {
        let perms = self.permissions();
        perms.contains(&PERM_READ)
            || perms.contains(&PERM_WRITE)
            || perms.contains(&PERM_ADMIN)
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

/// Maximum chain depth the verifier will walk before giving up. Real
/// chains are 1-3 deep typically; this is a sanity bound to refuse
/// adversarial deep chains.
pub const MAX_CHAIN_DEPTH: usize = 32;

/// Verify a single signature blob's claim against a cap blob's bytes.
///
// The old `verify_sig_blob` helper was replaced by the
// `extract_and_verify_sig_at` helper used by `verify_chain` — that one
// works against an arbitrary entity inside a sig blob (outer leaf or
// embedded sub-entity), which is what the new chain walk needs.

/// Extract a cap blob's declared attributes: subject, issuer, scope
/// root, expiry. Cap blobs are pure declarations now — chain
/// references live in the sig blob, so this is just a four-field
/// projection.
fn extract_cap_fields(
    cap_set: &TribleSet,
) -> Result<CapFields, VerifyError> {
    let mut iter = find!(
        (cap: crate::id::Id,
         subject: VerifyingKey,
         issuer: VerifyingKey,
         scope_root: crate::id::Id,
         expiry: Inline<NsTAIInterval>),
        pattern!(cap_set, [{
            ?cap @
            cap_subject: ?subject,
            cap_issuer: ?issuer,
            cap_scope_root: ?scope_root,
            crate::metadata::expires_at: ?expiry,
        }])
    );
    let (cap_id, subject, issuer, scope_root, expiry) = match (iter.next(), iter.next()) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedCap),
    };

    Ok(CapFields {
        cap_id,
        subject,
        issuer,
        scope_root,
        expiry,
    })
}

#[derive(Debug, Clone)]
struct CapFields {
    #[allow(dead_code)]
    cap_id: crate::id::Id,
    subject: VerifyingKey,
    issuer: VerifyingKey,
    scope_root: crate::id::Id,
    expiry: Inline<NsTAIInterval>,
}

/// Verify that a leaf signature blob plus its referenced cap blob form
/// a valid capability chain rooted at `team_root`, authorising the
/// `expected_subject` to act with the leaf cap's scope.
///
/// `fetch_blob` is called to retrieve any cap blob referenced by a
/// `cap_parent` handle during chain walk. The leaf sig and leaf cap
/// blobs are also looked up via `fetch_blob`, given the
/// `leaf_sig_handle`.
///
/// Eviction in the descriptive-caps model is per-issuer non-renewal
/// (the issuer's local retraction-policy pin), not a broadcast
/// revocation blob. Verification therefore checks signatures and
/// expiry only; a "revoked" peer's chain dies at its next natural
/// expiry once the issuer stops renewing.
///
/// Returns the verified leaf capability on success.
///
/// # Example
///
/// End-to-end auth flow: team root mints a length-1 cap for a
/// member, then verifies it.
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
///     build_capability, verify_chain, PERM_READ,
/// };
/// use rand::rngs::OsRng;
///
/// // Team root mints itself; in a real deployment this happens
/// // once at team creation and the secret is archived offline.
/// let team_root = SigningKey::generate(&mut OsRng);
/// let member = SigningKey::generate(&mut OsRng);
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
/// // Length-1 chain: team root signs the member's cap directly.
/// let (cap_blob, sig_blob) = build_capability(
///     &team_root,
///     member.verifying_key(),
///     None, // No parent — directly off the root.
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
/// let mut blobs: HashMap<[u8; 32], Blob<SimpleArchive>> = HashMap::new();
/// blobs.insert(cap_handle.raw, cap_blob);
/// blobs.insert(leaf_sig_handle.raw, sig_blob);
///
/// let verified = verify_chain(
///     team_root.verifying_key(),
///     leaf_sig_handle,
///     member.verifying_key(),
///     |h| blobs.get(&h.raw).cloned(),
/// )
/// .expect("chain valid");
///
/// assert_eq!(verified.subject, member.verifying_key());
/// assert!(verified.grants_read());
/// ```
pub fn verify_chain<F>(
    team_root: VerifyingKey,
    leaf_sig_handle: Inline<Handle<SimpleArchive>>,
    expected_subject: VerifyingKey,
    mut fetch_blob: F,
) -> Result<VerifiedCapability, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    // Through the clock seam: simulated runs check expiry against
    // virtual time, so cap-lifetime scenarios (renewal windows,
    // expiry-during-partition) are deterministically scriptable.
    let now: Epoch = crate::clock::epoch_now();

    // Helper: a cap is valid until the *upper bound* of its expiry
    // interval. We compare that upper bound against `now`.
    let is_expired = |expiry: &Inline<NsTAIInterval>| -> bool {
        match <(Epoch, Epoch)>::try_from_inline(expiry) {
            Ok((_lower, upper)) => upper < now,
            // A malformed/inverted interval is treated as expired so
            // adversarial caps can't fall through.
            Err(_) => true,
        }
    };

    // ── Leaf step ────────────────────────────────────────────────────
    //
    // The leaf sig blob carries: the leaf signature (over the leaf
    // cap), the leaf cap handle (via sig_signs), and — if the chain
    // extends beyond a single hop — the recursive chain proof
    // (sig_parent_cap + sig_embedded_parent_proof, each linking to the
    // next level's signer/signature/parent).
    let leaf_sig_blob = fetch_blob(leaf_sig_handle).ok_or(VerifyError::Fetch)?;
    let sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)?;

    // Find the leaf outer entity — the one carrying sig_signs.
    let mut leaf_outer_iter = find!(
        (sig: crate::id::Id, h: Inline<Handle<SimpleArchive>>),
        pattern!(&sig_set, [{ ?sig @ sig_signs: ?h }])
    );
    let (mut current_outer_id, leaf_cap_handle) = match (
        leaf_outer_iter.next(),
        leaf_outer_iter.next(),
    ) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedSig),
    };

    // Fetch + decode the leaf cap.
    let leaf_cap_blob = fetch_blob(leaf_cap_handle).ok_or(VerifyError::LeafCapMissing)?;
    let leaf_cap_set: TribleSet = TryFromBlob::try_from_blob(leaf_cap_blob.clone())?;
    let leaf_fields = extract_cap_fields(&leaf_cap_set)?;

    // Subject must match the connecting peer.
    if leaf_fields.subject != expected_subject {
        return Err(VerifyError::SubjectMismatch);
    }
    if is_expired(&leaf_fields.expiry) {
        return Err(VerifyError::Expired);
    }

    // Verify the outer signature attests to the leaf cap's bytes,
    // signed by the leaf's claimed issuer.
    let outer_signer = extract_and_verify_sig_at(
        &sig_set,
        current_outer_id,
        &leaf_cap_blob,
    )?;
    if outer_signer != leaf_fields.issuer {
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
    let mut current_signer = outer_signer;
    let mut current_cap_set = leaf_cap_set.clone();
    let mut current_fields = leaf_fields.clone();
    let mut depth = 0usize;

    loop {
        // Termination: the issuer of the current cap is the team root.
        if current_signer == team_root {
            return Ok(VerifiedCapability {
                subject: leaf_fields.subject,
                scope_root: leaf_fields.scope_root,
                cap_set: leaf_cap_set,
            });
        }

        depth += 1;
        if depth > MAX_CHAIN_DEPTH {
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
        let (parent_cap_handle, parent_proof_id) = match (
            parent_iter.next(),
            parent_iter.next(),
        ) {
            (Some(row), None) => row,
            _ => return Err(VerifyError::NonRootMissingParent),
        };

        // Fetch + decode the parent cap.
        let parent_cap_blob = fetch_blob(parent_cap_handle).ok_or(VerifyError::Fetch)?;
        let parent_cap_set: TribleSet =
            TryFromBlob::try_from_blob(parent_cap_blob.clone())?;
        let parent_fields = extract_cap_fields(&parent_cap_set)?;

        // Verify the parent proof's sig attests to the parent cap's
        // bytes, signed by some authority.
        let parent_signer = extract_and_verify_sig_at(
            &sig_set,
            parent_proof_id,
            &parent_cap_blob,
        )?;
        if parent_signer != parent_fields.issuer {
            return Err(VerifyError::IssuerMismatch);
        }
        if is_expired(&parent_fields.expiry) {
            return Err(VerifyError::Expired);
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
        current_signer = parent_signer;
        current_cap_set = parent_cap_set;
        current_fields = parent_fields;
    }
}

/// Extract a `(signed_by, signature_r, signature_s)` from a specific
/// entity inside a sig blob's TribleSet, verify it's a valid signature
/// over `signed_blob.bytes`, and return the signer.
fn extract_and_verify_sig_at(
    sig_set: &TribleSet,
    entity: crate::id::Id,
    signed_blob: &Blob<SimpleArchive>,
) -> Result<VerifyingKey, VerifyError> {
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
    Ok(signer)
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
        (past_start, past_end).try_to_inline().expect("valid interval")
    }

    fn empty_scope() -> (Id, TribleSet) {
        let scope_root = crate::id::ufoid();
        let facts = TribleSet::from(entity! { ExclusiveId::force_ref(&scope_root) @
            crate::metadata::tag: PERM_READ,
        });
        (*scope_root, facts)
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
    fn length_one_chain_round_trips() {
        let team_root = key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            team_root.verifying_key(),
            None,
            scope_root,
            scope_facts,
            interval(3600.0),
        )
        .expect("build");

        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
        let blobs = [cap_blob.clone(), sig_blob.clone()];

        let verified = verify_chain(
            team_root.verifying_key(),
            sig_handle,
            team_root.verifying_key(),
            fetch_from(&blobs),
        )
        .expect("verify");

        assert_eq!(verified.subject, team_root.verifying_key());
        assert_eq!(verified.scope_root, scope_root);
    }

    // ── Length-N chain ────────────────────────────────────────────────

    fn three_level_chain()
    -> (SigningKey, SigningKey, SigningKey, Vec<Blob<SimpleArchive>>, Inline<Handle<SimpleArchive>>) {
        let team_root = key();
        let a = key();
        let b = key();

        // Level 1: team_root → A (subject = A)
        let (scope1_root, scope1_facts) = empty_scope();
        let (cap_a, sig_a) = build_capability(
            &team_root,
            a.verifying_key(),
            None,
            scope1_root,
            scope1_facts,
            interval(3600.0),
        )
        .expect("build level-1");

        // Level 2: A → B (subject = B)
        let (scope2_root, scope2_facts) = empty_scope();
        let (cap_b, sig_b) = build_capability(
            &a,
            b.verifying_key(),
            Some((cap_a.clone(), sig_a.clone())),
            scope2_root,
            scope2_facts,
            interval(3600.0),
        )
        .expect("build level-2");

        let leaf_sig_handle: Inline<Handle<SimpleArchive>> = (&sig_b).get_handle();
        let blobs = vec![cap_a, sig_a, cap_b, sig_b];
        (team_root, a, b, blobs, leaf_sig_handle)
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

        // With a wrong team root, the chain walk never finds a sig
        // signed by it — climbs to the actual root, finds no
        // sig_parent_cap there, errors with NonRootMissingParent
        // (current_signer != wrong_team_root && no parent linkage).
        let err = verify_chain(
            wrong_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject wrong team root");

        assert!(matches!(err, VerifyError::NonRootMissingParent));
    }

    #[test]
    fn rejects_expired_leaf() {
        let team_root = key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            team_root.verifying_key(),
            None,
            scope_root,
            scope_facts,
            expired_interval(),
        )
        .expect("build");

        let sig_handle: Inline<Handle<SimpleArchive>> = (&sig_blob).get_handle();
        let blobs = [cap_blob, sig_blob];

        let err = verify_chain(
            team_root.verifying_key(),
            sig_handle,
            team_root.verifying_key(),
            fetch_from(&blobs),
        )
        .expect_err("must reject expired");

        assert!(matches!(err, VerifyError::Expired));
    }

    #[test]
    fn rejects_expired_intermediate() {
        // Length-2 chain where team_root's cap to A has expired,
        // but A's cap to B has not. verify must reject.
        let team_root = key();
        let a = key();
        let b = key();

        let (scope1_root, scope1_facts) = empty_scope();
        let (cap_a, sig_a) = build_capability(
            &team_root,
            a.verifying_key(),
            None,
            scope1_root,
            scope1_facts,
            expired_interval(),
        )
        .expect("build level-1");

        let (scope2_root, scope2_facts) = empty_scope();
        let (cap_b, sig_b) = build_capability(
            &a,
            b.verifying_key(),
            Some((cap_a.clone(), sig_a.clone())),
            scope2_root,
            scope2_facts,
            interval(3600.0),
        )
        .expect("build level-2");

        let leaf_sig_handle: Inline<Handle<SimpleArchive>> = (&sig_b).get_handle();
        let blobs = [cap_a, sig_a, cap_b, sig_b];

        let err = verify_chain(
            team_root.verifying_key(),
            leaf_sig_handle,
            b.verifying_key(),
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
