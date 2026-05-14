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
/// Tag indicating a scope grants admin (delegation + revocation) authority.
pub const PERM_ADMIN: Id = id_hex!("EC68A0CBF9EF421F59A0A69ED80FD79F");

use crate::value::schemas::ed25519 as ed;
use crate::value::schemas::hash::Blake3;
use crate::blob::schemas::simplearchive::SimpleArchive;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::Handle;

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
    /// Handle of the parent cap blob in the chain. Absent on the
    /// founder's cap (the chain terminator), which is signed directly
    /// by the team root.
    "E825B3A8D387B4DAE1720B0EDCBFAA9E" as pub cap_parent: Handle<SimpleArchive>;
    /// Entity id within the cap blob holding the parent's signature
    /// inline (the "targeted merge" optimisation — see module docs).
    /// The sub-entity carries `signed_by`, `signature_r`, `signature_s`
    /// reusing the existing commit-signature attribute conventions.
    /// Absent on the founder's cap.
    "008F7784A309CA9DEF007E4F63F87121" as pub cap_embedded_parent_sig: GenId;

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
    // Note: sig_signer + sig_value (r/s) reuse the existing
    // `repo::signed_by`, `repo::signature_r`, `repo::signature_s`
    // attributes — same convention as commit signatures, plus
    // structural reuse (a sig blob has the same shape inside as the
    // signature portion of a commit's metadata blob).

    // ── Revocation blob ──────────────────────────────────────────────
    /// The pubkey being revoked. When this pubkey appears in any cap
    /// chain as either issuer or subject, that cap is invalidated.
    /// Revocation cascades transitively (revoking issuer K invalidates
    /// every cap K signed and every cap derived from those).
    "E146824999D1DA7F1F0E54025F52EE13" as pub rev_target: ed::ED25519PublicKey;
    // Note: revocation timestamp reuses `metadata::created_at`;
    // revocation signer + signature reuse `repo::signed_by` +
    // `repo::signature_r/s`.
}

/// Tag identifying a blob as a capability claim.
#[allow(dead_code)]
pub const KIND_CAPABILITY: Id = id_hex!("B8D76786ACD20F344A4E5CBFC0F75772");
/// Tag identifying a blob as a capability signature.
#[allow(dead_code)]
pub const KIND_CAPABILITY_SIG: Id = id_hex!("E6BB52CE6E02D51C3676ECE1EEA9094F");
/// Tag identifying a blob as a capability revocation.
#[allow(dead_code)]
pub const KIND_REVOCATION: Id = id_hex!("1EEAF2CF25A776547A26080E755D111C");

// ── Builder ──────────────────────────────────────────────────────────

use ed25519::Signature;
use ed25519_dalek::SigningKey;
use ed25519_dalek::VerifyingKey;
use ed25519::signature::Signer;

use crate::blob::Blob;
use crate::blob::IntoBlob;
use crate::blob::TryFromBlob;
use crate::blob::schemas::simplearchive::UnarchiveError;
use crate::id::ExclusiveId;
use crate::macros::entity;
use crate::macros::pattern;
use crate::query::find;
use crate::trible::TribleSet;
use crate::value::Inline;
use crate::value::IntoInline;
use crate::value::schemas::time::NsTAIInterval;

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
/// use triblespace_core::value::TryToInline;
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

    // Build the cap entity. `entity!` without an explicit id derives the
    // entity id by hashing the (attr, value) pairs — same trick commits
    // use, gives us content-addressed identity at the entity level for
    // free.
    let cap_fragment = entity! {
        cap_subject: issuer_subject_value(subject),
        cap_issuer: issuer_subject_value(issuer_pubkey),
        cap_scope_root: scope_root,
        crate::metadata::expires_at: expiry,
    };
    let cap_id = cap_fragment
        .root()
        .expect("entity! always produces a rooted fragment");

    let mut cap_set = TribleSet::from(cap_fragment);
    cap_set += scope_facts;

    if let Some((parent_cap_blob, parent_sig_blob)) = parent {
        let parent_cap_handle: Inline<Handle<SimpleArchive>> =
            parent_cap_blob.get_handle();

        // Decode the parent signature blob into its tribles, then locate
        // the single entity carrying `sig_signs` (the sig entity id).
        let parent_sig_set: TribleSet = TryFromBlob::<SimpleArchive>::try_from_blob(
            parent_sig_blob,
        )
        .map_err(BuildError::ParseParentSig)?;

        // The parent signature blob has exactly one entity carrying
        // sig_signs (its own sig entity). We project that id out; the
        // signed handle is unused here.
        let mut sig_id_iter = find!(
            (sig: crate::id::Id, _signed: Inline<Handle<SimpleArchive>>),
            pattern!(&parent_sig_set, [{ ?sig @ sig_signs: ?_signed }])
        )
        .map(|(sig, _)| sig);
        let sig_id = match (sig_id_iter.next(), sig_id_iter.next()) {
            (Some(sig), None) => sig,
            _ => return Err(BuildError::ParentSigShape),
        };

        // Embed the parent signature tribles directly under their existing
        // entity id. The verifier extracts them by querying for the
        // sub-entity at `sig_id` inside the cap blob.
        cap_set += parent_sig_set;

        // Add cap_parent + cap_embedded_parent_sig pointing back at the
        // cap entity (which we addressed via `cap_id`).
        cap_set += TribleSet::from(entity! { ExclusiveId::force_ref(&cap_id) @
            cap_parent: parent_cap_handle,
            cap_embedded_parent_sig: sig_id,
        });
    }

    let cap_blob: Blob<SimpleArchive> = cap_set.to_blob();

    // Sign the cap blob's canonical bytes.
    let signature: Signature = issuer.sign(&cap_blob.bytes);
    let cap_handle: Inline<Handle<SimpleArchive>> =
        (&cap_blob).get_handle();

    // Build the sig blob: handle pointer to the cap, signer pubkey,
    // signature components. Reuses the existing commit-signature
    // attribute conventions.
    let sig_fragment = entity! {
        sig_signs: cap_handle,
        crate::repo::signed_by: issuer_pubkey,
        crate::repo::signature_r: signature,
        crate::repo::signature_s: signature,
    };
    let sig_blob: Blob<SimpleArchive> = TribleSet::from(sig_fragment).to_blob();

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

// ── Revocation blobs ─────────────────────────────────────────────────

/// Build a revocation blob pair declaring `target` as revoked.
///
/// Returns `(rev_blob, sig_blob)`:
/// - `rev_blob` carries `rev_target` (the pubkey being revoked) and
///   `metadata::created_at` (when the revocation was issued).
/// - `sig_blob` mirrors the capability sig blob shape: `sig_signs`
///   pointing at the rev blob's handle, plus `signed_by` +
///   `signature_r` + `signature_s` reusing the existing
///   commit-signature attribute conventions.
///
/// The revoker key is whoever has authority over the revoked pubkey.
/// For v0 that's typically the team root (for blanket revocations) or
/// the issuer who originally issued the cap to the revoked subject.
/// The verifier decides whether a particular revoker is authorised at
/// verification time; this function only produces the signed
/// statement.
///
/// # Example
///
/// Mint a team-root-signed revocation pair, round-trip through
/// `verify_revocation`:
///
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use triblespace_core::repo::capability::{
///     build_revocation, verify_revocation,
/// };
/// use rand::rngs::OsRng;
///
/// let team_root = SigningKey::generate(&mut OsRng);
/// let target = SigningKey::generate(&mut OsRng);
///
/// let (rev_blob, sig_blob) =
///     build_revocation(&team_root, target.verifying_key());
///
/// // The pair verifies cleanly: signer recovered, target recovered.
/// let (out_revoker, out_target) =
///     verify_revocation(rev_blob, sig_blob).expect("verifies");
/// assert_eq!(out_revoker, team_root.verifying_key());
/// assert_eq!(out_target, target.verifying_key());
/// ```
pub fn build_revocation(
    revoker: &SigningKey,
    target: VerifyingKey,
) -> (Blob<SimpleArchive>, Blob<SimpleArchive>) {
    let now = hifitime::Epoch::now().expect("system time");
    let timestamp: Inline<NsTAIInterval> =
        (now, now).try_to_inline().expect("point interval");

    let rev_fragment = entity! {
        rev_target: target.to_inline(),
        crate::metadata::created_at: timestamp,
    };
    let rev_blob: Blob<SimpleArchive> = TribleSet::from(rev_fragment).to_blob();

    let signature: Signature = revoker.sign(&rev_blob.bytes);
    let revoker_pubkey = revoker.verifying_key();
    let rev_handle: Inline<Handle<SimpleArchive>> =
        (&rev_blob).get_handle();

    let sig_fragment = entity! {
        sig_signs: rev_handle,
        crate::repo::signed_by: revoker_pubkey,
        crate::repo::signature_r: signature,
        crate::repo::signature_s: signature,
    };
    let sig_blob: Blob<SimpleArchive> = TribleSet::from(sig_fragment).to_blob();

    (rev_blob, sig_blob)
}

/// Verify a revocation blob pair: the sig blob attests to the rev
/// blob's bytes by some pubkey. Returns `(revoker, target)` on
/// success — the caller is responsible for deciding whether the
/// revoker has authority to revoke the target.
///
/// For v0, the typical caller policy is: accept revocations signed by
/// the team root. Future extensions (e.g. "issuer can revoke caps they
/// originally issued") are pure caller policy — this function just
/// confirms the signature is valid and surfaces the (who, whom) pair.
pub fn verify_revocation(
    rev_blob: Blob<SimpleArchive>,
    sig_blob: Blob<SimpleArchive>,
) -> Result<(VerifyingKey, VerifyingKey), VerifyError> {
    let sig_set: TribleSet = TryFromBlob::try_from_blob(sig_blob)?;
    let revoker = verify_sig_blob(&sig_set, &rev_blob)?;

    let rev_set: TribleSet = TryFromBlob::try_from_blob(rev_blob)?;
    let mut iter = find!(
        (rev: crate::id::Id, target: VerifyingKey),
        pattern!(&rev_set, [{ ?rev @ rev_target: ?target }])
    );
    let (_rev_id, target) = match (iter.next(), iter.next()) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedCap),
    };

    Ok((revoker, target))
}

/// Build a `HashSet<VerifyingKey>` of revoked pubkeys from a collection
/// of revocation blob pairs, accepting only revocations signed by a
/// pubkey in `authorised_revokers`.
///
/// The typical v0 policy is `authorised_revokers = {team_root}`.
/// Future extensions can pass a wider set (e.g. all admin-scoped
/// pubkeys). Revocations signed by unauthorised pubkeys are silently
/// dropped from the result — the caller can still pass the rejected
/// blob through `verify_revocation` directly to surface them as
/// diagnostic events.
pub fn build_revocation_set<I>(
    authorised_revokers: &HashSet<VerifyingKey>,
    revocations: I,
) -> HashSet<VerifyingKey>
where
    I: IntoIterator<Item = (Blob<SimpleArchive>, Blob<SimpleArchive>)>,
{
    let mut revoked: HashSet<VerifyingKey> = HashSet::new();
    for (rev_blob, sig_blob) in revocations {
        if let Ok((revoker, target)) = verify_revocation(rev_blob, sig_blob) {
            if authorised_revokers.contains(&revoker) {
                revoked.insert(target);
            }
        }
    }
    revoked
}

/// Extract revocation `(rev_blob, sig_blob)` pairs from an arbitrary
/// collection of `SimpleArchive` blobs.
///
/// A blob is treated as a candidate signature if it decodes as a
/// `TribleSet` carrying exactly one `(?sig @ sig_signs: ?h)` triple.
/// The referenced handle is then looked up in the same blob set; if
/// that target blob carries a `(?rev @ rev_target: ?_)` triple, the
/// pair is emitted.
///
/// Decoding failures are silently dropped — the caller is iterating
/// over a heterogeneous blob store (caps, sigs, commits, payload data)
/// and most blobs will not be revocation-shaped.
///
/// The result feeds directly into [`build_revocation_set`] so the
/// usual call shape is:
///
/// ```text
/// let pairs = extract_revocation_pairs(snapshot_blobs);
/// let revoked = build_revocation_set(&authorised, pairs);
/// ```
///
/// Independent step from `build_revocation_set` because callers
/// commonly want to inspect / log the candidate pairs (e.g. for a
/// "tell me what revocations my pile contains" CLI) before deciding
/// which authorised-revoker policy to apply.
///
/// # Example
///
/// Given a heterogeneous blob set containing one valid revocation
/// pair plus an unrelated cap blob, `extract_revocation_pairs`
/// finds exactly the rev+sig pair and skips the cap. Composing
/// with [`build_revocation_set`] under a `{team_root}` authorised
/// set then surfaces the revoked target pubkey:
///
/// ```rust
/// use ed25519_dalek::SigningKey;
/// use std::collections::HashSet;
/// use triblespace_core::id::{ufoid, ExclusiveId};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::value::TryToInline;
/// use triblespace_core::repo::capability::{
///     build_capability, build_revocation, build_revocation_set,
///     extract_revocation_pairs, PERM_READ,
/// };
/// use rand::rngs::OsRng;
///
/// let team_root = SigningKey::generate(&mut OsRng);
/// let target = SigningKey::generate(&mut OsRng);
///
/// // The revocation pair we care about.
/// let (rev_blob, rev_sig_blob) =
///     build_revocation(&team_root, target.verifying_key());
///
/// // Add an unrelated cap blob to confirm the scanner doesn't
/// // misclassify it.
/// let founder = SigningKey::generate(&mut OsRng);
/// let scope_root = ufoid();
/// let scope_facts: TribleSet = entity! {
///     ExclusiveId::force_ref(&scope_root) @
///     triblespace_core::metadata::tag: PERM_READ,
/// }
/// .into();
/// let now = hifitime::Epoch::now().unwrap();
/// let expiry = (now, now + hifitime::Duration::from_seconds(3600.0))
///     .try_to_inline()
///     .unwrap();
/// let (cap_blob, cap_sig_blob) = build_capability(
///     &team_root,
///     founder.verifying_key(),
///     None,
///     *scope_root,
///     scope_facts,
///     expiry,
/// )
/// .unwrap();
///
/// // Scan: exactly one (rev, sig) pair surfaces; the cap+cap_sig
/// // are not revocation-shaped and get dropped.
/// let pairs = extract_revocation_pairs([
///     rev_blob, rev_sig_blob, cap_blob, cap_sig_blob,
/// ]);
/// assert_eq!(pairs.len(), 1);
///
/// // Apply an authorised-revoker policy: only revocations signed
/// // by the team root take effect. (For the relay, this is the
/// // typical v0 policy.)
/// let mut authorised = HashSet::new();
/// authorised.insert(team_root.verifying_key());
/// let revoked = build_revocation_set(&authorised, pairs);
/// assert!(revoked.contains(&target.verifying_key()));
/// ```
pub fn extract_revocation_pairs<I>(
    blobs: I,
) -> Vec<(Blob<SimpleArchive>, Blob<SimpleArchive>)>
where
    I: IntoIterator<Item = Blob<SimpleArchive>>,
{
    let blob_map: std::collections::HashMap<[u8; 32], Blob<SimpleArchive>> = blobs
        .into_iter()
        .map(|b| {
            let h: Inline<Handle<SimpleArchive>> = (&b).get_handle();
            (h.raw, b)
        })
        .collect();

    let mut pairs = Vec::new();
    for candidate_sig in blob_map.values() {
        // Try to decode as a sig blob.
        let Ok(sig_set): Result<TribleSet, _> =
            TryFromBlob::try_from_blob(candidate_sig.clone())
        else {
            continue;
        };
        let mut sig_iter = find!(
            (sig: crate::id::Id, h: Inline<Handle<SimpleArchive>>),
            pattern!(&sig_set, [{ ?sig @ sig_signs: ?h }])
        );
        // Exactly one sig_signs triple — anything else is non-sig-shaped
        // (e.g. a cap blob with embedded sub-entities of its own).
        let target_handle = match (sig_iter.next(), sig_iter.next()) {
            (Some((_, h)), None) => h,
            _ => continue,
        };

        // Look up the target blob in our local set. (Revocation pairs
        // are gossiped together; if only one half made it into this
        // collection we treat the pair as incomplete and skip.)
        let Some(target_blob) = blob_map.get(&target_handle.raw) else {
            continue;
        };

        // Confirm the target is a revocation blob (carries rev_target).
        let Ok(target_set): Result<TribleSet, _> =
            TryFromBlob::try_from_blob(target_blob.clone())
        else {
            continue;
        };
        let is_revocation = find!(
            (e: crate::id::Id, t: VerifyingKey),
            pattern!(&target_set, [{ ?e @ rev_target: ?t }])
        )
        .next()
        .is_some();
        if !is_revocation {
            continue;
        }

        pairs.push((target_blob.clone(), candidate_sig.clone()));
    }
    pairs
}

// ── Verifier ──────────────────────────────────────────────────────────

use ed25519_dalek::Verifier;
use std::collections::HashSet;
use crate::value::TryFromInline;
use crate::value::TryToInline;
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
    /// A cap or its embedded parent sig has expired.
    Expired,
    /// A pubkey appearing in the chain is in the revocation list.
    Revoked,
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
    /// A non-root cap (one whose issuer differs from the team root) is
    /// missing either `cap_parent` or `cap_embedded_parent_sig`.
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
/// Returns the issuer pubkey (extracted from the sig blob) on success.
/// Caller is responsible for cross-checking the issuer pubkey against
/// the cap's `cap_issuer` attribute.
fn verify_sig_blob(
    sig_set: &TribleSet,
    cap_blob: &Blob<SimpleArchive>,
) -> Result<VerifyingKey, VerifyError> {
    let cap_handle: Inline<Handle<SimpleArchive>> = cap_blob.get_handle();
    let mut iter = find!(
        (sig: crate::id::Id, signer: VerifyingKey, r, s),
        pattern!(sig_set, [{
            ?sig @
            sig_signs: cap_handle,
            crate::repo::signed_by: ?signer,
            crate::repo::signature_r: ?r,
            crate::repo::signature_s: ?s,
        }])
    );
    let (_sig_id, signer, r, s) = match (iter.next(), iter.next()) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedSig),
    };

    let signature = Signature::from_components(r, s);
    signer
        .verify(&cap_blob.bytes, &signature)
        .map_err(|_| VerifyError::BadSignature)?;
    Ok(signer)
}

/// Extract the leaf cap's expected attributes (subject, issuer,
/// scope_root, expiry, optionally parent + embedded sig sub-entity).
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

    // Optional: cap_parent + cap_embedded_parent_sig. Both present or
    // both absent.
    let parent_handle: Option<Inline<Handle<SimpleArchive>>> = find!(
        (h: Inline<Handle<SimpleArchive>>),
        pattern!(cap_set, [{ cap_id @ cap_parent: ?h }])
    )
    .next()
    .map(|(h,)| h);

    let embedded_sig: Option<crate::id::Id> = find!(
        (s: crate::id::Id),
        pattern!(cap_set, [{ cap_id @ cap_embedded_parent_sig: ?s }])
    )
    .next()
    .map(|(s,)| s);

    Ok(CapFields {
        cap_id,
        subject,
        issuer,
        scope_root,
        expiry,
        parent_handle,
        embedded_sig,
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
    parent_handle: Option<Inline<Handle<SimpleArchive>>>,
    embedded_sig: Option<crate::id::Id>,
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
/// `revoked` is a set of pubkeys whose presence anywhere in the chain
/// (as issuer or subject) invalidates the capability transitively.
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
/// use std::collections::{HashMap, HashSet};
/// use triblespace_core::blob::Blob;
/// use triblespace_core::blob::schemas::simplearchive::SimpleArchive;
/// use triblespace_core::id::{ufoid, ExclusiveId};
/// use triblespace_core::macros::entity;
/// use triblespace_core::trible::TribleSet;
/// use triblespace_core::value::TryToInline;
/// use triblespace_core::value::Inline;
/// use triblespace_core::value::schemas::hash::{Blake3, Handle};
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
/// // Scope: a single anchor entity tagged with PERM_READ. The
/// // anchor id goes into `cap_scope_root`; the tag triple goes
/// // into `scope_facts` so it lives in the cap blob.
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
///     None, // No parent — this is the founder/member directly off the root.
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
/// // Real callers wire this through their pile / blob store.
/// let mut blobs: HashMap<[u8; 32], Blob<SimpleArchive>> = HashMap::new();
/// let cap_handle: Inline<Handle<SimpleArchive>> =
///     (&cap_blob).get_handle();
/// blobs.insert(cap_handle.raw, cap_blob);
/// blobs.insert(leaf_sig_handle.raw, sig_blob);
///
/// // No revocations in play.
/// let revoked: HashSet<ed25519_dalek::VerifyingKey> = HashSet::new();
///
/// let verified = verify_chain(
///     team_root.verifying_key(),
///     leaf_sig_handle,
///     member.verifying_key(),
///     &revoked,
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
    revoked: &HashSet<VerifyingKey>,
    mut fetch_blob: F,
) -> Result<VerifiedCapability, VerifyError>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let now: Epoch = hifitime::Epoch::now().expect("system time");

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
    let leaf_sig_blob = fetch_blob(leaf_sig_handle).ok_or(VerifyError::Fetch)?;
    let leaf_sig_set: TribleSet = TryFromBlob::try_from_blob(leaf_sig_blob)?;

    // The leaf sig blob points at the leaf cap blob via sig_signs.
    let mut leaf_cap_handle_iter = find!(
        (sig: crate::id::Id, h: Inline<Handle<SimpleArchive>>),
        pattern!(&leaf_sig_set, [{
            ?sig @ sig_signs: ?h,
        }])
    );
    let (_sig_id, leaf_cap_handle) = match (
        leaf_cap_handle_iter.next(),
        leaf_cap_handle_iter.next(),
    ) {
        (Some(row), None) => row,
        _ => return Err(VerifyError::MalformedSig),
    };

    let leaf_cap_blob = fetch_blob(leaf_cap_handle).ok_or(VerifyError::LeafCapMissing)?;

    // Verify the leaf signature blob against the leaf cap bytes.
    let leaf_signer = verify_sig_blob(&leaf_sig_set, &leaf_cap_blob)?;

    // Decode the leaf cap into fields.
    let leaf_cap_set: TribleSet = TryFromBlob::try_from_blob(leaf_cap_blob.clone())?;
    let leaf_fields = extract_cap_fields(&leaf_cap_set)?;

    // Subject must match the connecting peer.
    if leaf_fields.subject != expected_subject {
        return Err(VerifyError::SubjectMismatch);
    }
    // Sig signer must match the cap's claimed issuer.
    if leaf_signer != leaf_fields.issuer {
        return Err(VerifyError::IssuerMismatch);
    }
    if is_expired(&leaf_fields.expiry) {
        return Err(VerifyError::Expired);
    }
    if revoked.contains(&leaf_fields.issuer)
        || revoked.contains(&leaf_fields.subject)
    {
        return Err(VerifyError::Revoked);
    }

    // ── Walk back to root ────────────────────────────────────────────
    let mut current_set = leaf_cap_set.clone();
    let mut current_fields = leaf_fields.clone();
    let mut depth = 0usize;

    loop {
        if current_fields.issuer == team_root {
            // Chain terminates at the team root.
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

        // Non-root cap: must have parent + embedded sig.
        let parent_handle = current_fields
            .parent_handle
            .ok_or(VerifyError::NonRootMissingParent)?;
        let embedded_sig_id = current_fields
            .embedded_sig
            .ok_or(VerifyError::NonRootMissingParent)?;

        // Fetch parent cap.
        let parent_cap_blob = fetch_blob(parent_handle).ok_or(VerifyError::Fetch)?;

        // Extract the embedded sig sub-entity from the *current* cap set
        // (the parent's signature, embedded inline) and verify it
        // attests to the parent cap's bytes.
        let mut sig_facts = find!(
            (signer: VerifyingKey, r, s),
            pattern!(&current_set, [{
                embedded_sig_id @
                crate::repo::signed_by: ?signer,
                crate::repo::signature_r: ?r,
                crate::repo::signature_s: ?s,
            }])
        );
        let (parent_signer, r, s) = match (sig_facts.next(), sig_facts.next()) {
            (Some(row), None) => row,
            _ => return Err(VerifyError::MalformedSig),
        };
        let signature = Signature::from_components(r, s);
        parent_signer
            .verify(&parent_cap_blob.bytes, &signature)
            .map_err(|_| VerifyError::BadSignature)?;

        // Decode parent cap and run the per-link checks.
        let parent_set: TribleSet = TryFromBlob::try_from_blob(parent_cap_blob)?;
        let parent_fields = extract_cap_fields(&parent_set)?;

        if parent_signer != parent_fields.issuer {
            return Err(VerifyError::IssuerMismatch);
        }
        if is_expired(&parent_fields.expiry) {
            return Err(VerifyError::Expired);
        }
        if revoked.contains(&parent_fields.issuer)
            || revoked.contains(&parent_fields.subject)
        {
            return Err(VerifyError::Revoked);
        }
        // Each child link's scope must be a subset of its parent's.
        if !scope_subsumes(
            &parent_set,
            parent_fields.scope_root,
            &current_set,
            current_fields.scope_root,
        ) {
            return Err(VerifyError::ScopeNotSubset);
        }

        // Step.
        current_set = parent_set;
        current_fields = parent_fields;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::TryToInline;
    use ed25519_dalek::Verifier;
    use hifitime::Epoch;
    use rand::rngs::OsRng;

    fn now_plus_24h() -> Inline<NsTAIInterval> {
        let now = Epoch::now().expect("system time");
        let later = now + hifitime::Duration::from_seconds(24.0 * 3600.0);
        (now, later).try_to_inline().expect("valid interval")
    }

    fn signing_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn empty_scope() -> (Id, TribleSet) {
        // Trivial scope: a single anchor entity tagged with a permission.
        let scope_root = crate::id::ufoid();
        let scope_facts = entity! { ExclusiveId::force_ref(&scope_root) @
            crate::metadata::tag: PERM_READ,
        };
        (*scope_root, TribleSet::from(scope_facts))
    }

    /// Build a scope with the given permission tags and (optionally)
    /// branch restrictions.
    fn scope_with(perms: &[Id], branches: &[Id]) -> (Id, TribleSet) {
        let scope_root = crate::id::ufoid();
        let mut facts = TribleSet::new();
        for perm in perms {
            facts += TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                crate::metadata::tag: *perm,
            });
        }
        for b in branches {
            facts += TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @
                scope_branch: *b,
            });
        }
        (*scope_root, facts)
    }

    /// Length-1 chain: the team root signs the founder's cap directly.
    /// Cap blob has no parent and no embedded sig; sig blob attests to
    /// the cap blob's bytes.
    #[test]
    fn build_root_capability() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = empty_scope();
        let expiry = now_plus_24h();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            expiry,
        )
        .expect("root cap builds");

        // Sig blob must verify against cap blob's bytes.
        let sig_set: TribleSet =
            <TribleSet as TryFromBlob<SimpleArchive>>::try_from_blob(sig_blob)
                .expect("valid sig blob");
        let mut sig_iter = find!(
            (sig: Id,
             handle: Inline<Handle<SimpleArchive>>,
             pubkey: VerifyingKey,
             r,
             s),
            pattern!(&sig_set, [{
                ?sig @
                sig_signs: ?handle,
                crate::repo::signed_by: ?pubkey,
                crate::repo::signature_r: ?r,
                crate::repo::signature_s: ?s,
            }])
        );
        let (_sig_entity, signed_handle, recovered_pubkey, r_v, s_v) =
            sig_iter.next().expect("exactly one sig entity");
        assert!(sig_iter.next().is_none(), "exactly one sig entity");

        // sig_signs must point at the cap blob.
        let cap_handle: Inline<Handle<SimpleArchive>> =
            (&cap_blob).get_handle();
        assert_eq!(signed_handle, cap_handle);

        // Pubkey is the team root.
        assert_eq!(recovered_pubkey, team_root.verifying_key());

        // Signature verifies over cap_blob.bytes.
        let signature = ed25519::Signature::from_components(r_v, s_v);
        team_root
            .verifying_key()
            .verify(&cap_blob.bytes, &signature)
            .expect("signature verifies over the cap blob bytes");

        // Cap blob has no cap_parent and no cap_embedded_parent_sig.
        let cap_set: TribleSet =
            <TribleSet as TryFromBlob<SimpleArchive>>::try_from_blob(cap_blob)
                .expect("valid cap blob");
        let parents: usize = find!(
            (e: Id, h: Inline<Handle<SimpleArchive>>),
            pattern!(&cap_set, [{ ?e @ cap_parent: ?h }])
        )
        .count();
        assert_eq!(parents, 0, "root cap has no cap_parent");

        let embedded: usize = find!(
            (e: Id, sig: Id),
            pattern!(&cap_set, [{ ?e @ cap_embedded_parent_sig: ?sig }])
        )
        .count();
        assert_eq!(embedded, 0, "root cap has no embedded parent sig");
    }

    /// Helper: build an in-memory blob store keyed by handle for the
    /// verifier's `fetch_blob` callback.
    fn store_for(blobs: &[&Blob<SimpleArchive>])
        -> impl FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>
    {
        let mut map = std::collections::HashMap::new();
        for blob in blobs {
            let handle: Inline<Handle<SimpleArchive>> = (*blob).get_handle();
            map.insert(handle.raw, (*blob).clone());
        }
        move |h: Inline<Handle<SimpleArchive>>| map.get(&h.raw).cloned()
    }

    /// Verify a length-1 chain (root signs founder directly). Should
    /// succeed and return the leaf's subject + scope_root.
    #[test]
    fn verify_root_chain() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("root cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let revoked = HashSet::new();
        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        );

        let verified = result.expect("chain verifies");
        assert_eq!(verified.subject, founder.verifying_key());
        assert_eq!(verified.scope_root, scope_root);
    }

    /// Verify a length-2 chain (root → founder → member). Should
    /// succeed.
    #[test]
    fn verify_delegated_chain() {
        let team_root = signing_key();
        let founder = signing_key();
        let member = signing_key();

        let (founder_scope_root, founder_scope_facts) = empty_scope();
        let (founder_cap, founder_sig) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            founder_scope_root,
            founder_scope_facts,
            now_plus_24h(),
        )
        .expect("founder cap builds");

        let (member_scope_root, member_scope_facts) = empty_scope();
        let (member_cap, member_sig) = build_capability(
            &founder,
            member.verifying_key(),
            Some((founder_cap.clone(), founder_sig.clone())),
            member_scope_root,
            member_scope_facts,
            now_plus_24h(),
        )
        .expect("member cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&member_sig).get_handle();
        let revoked = HashSet::new();
        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            member.verifying_key(),
            &revoked,
            store_for(&[
                &founder_cap,
                &founder_sig,
                &member_cap,
                &member_sig,
            ]),
        );

        let verified = result.expect("chain verifies");
        assert_eq!(verified.subject, member.verifying_key());
        assert_eq!(verified.scope_root, member_scope_root);
    }

    /// Subject mismatch: presenting a cap whose subject doesn't match
    /// the connecting peer's pubkey. Should fail with SubjectMismatch.
    #[test]
    fn reject_subject_mismatch() {
        let team_root = signing_key();
        let founder = signing_key();
        let attacker = signing_key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let revoked = HashSet::new();
        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            attacker.verifying_key(), // wrong subject
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        );
        assert!(matches!(result, Err(VerifyError::SubjectMismatch)));
    }

    /// Wrong team root: presenting a cap signed by some other key as
    /// the team root. Should fail with IssuerMismatch (the leaf's
    /// issuer doesn't match the supplied team root, so chain walk
    /// proceeds, expects a parent, finds none → NonRootMissingParent).
    #[test]
    fn reject_wrong_team_root() {
        let real_root = signing_key();
        let founder = signing_key();
        let bogus_root = signing_key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &real_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let revoked = HashSet::new();
        let result = verify_chain(
            bogus_root.verifying_key(), // wrong team root
            leaf_handle,
            founder.verifying_key(),
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        );
        // The chain has issuer=real_root which != bogus_root, so the
        // verifier tries to walk up but the cap has no parent.
        assert!(matches!(result, Err(VerifyError::NonRootMissingParent)));
    }

    /// Revoked subject: subject pubkey appears in the revocation set.
    /// Should fail with Revoked.
    #[test]
    fn reject_revoked_subject() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let mut revoked = HashSet::new();
        revoked.insert(founder.verifying_key());

        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        );
        assert!(matches!(result, Err(VerifyError::Revoked)));
    }

    /// Revoked transitive issuer: in a length-2 chain, revoking the
    /// intermediate issuer (the founder, in this case) invalidates
    /// the leaf via cascade. Should fail with Revoked at the parent
    /// step.
    #[test]
    fn reject_revoked_intermediate_issuer() {
        let team_root = signing_key();
        let founder = signing_key();
        let member = signing_key();

        let (founder_scope_root, founder_scope_facts) = empty_scope();
        let (founder_cap, founder_sig) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            founder_scope_root,
            founder_scope_facts,
            now_plus_24h(),
        )
        .expect("founder cap builds");

        let (member_scope_root, member_scope_facts) = empty_scope();
        let (member_cap, member_sig) = build_capability(
            &founder,
            member.verifying_key(),
            Some((founder_cap.clone(), founder_sig.clone())),
            member_scope_root,
            member_scope_facts,
            now_plus_24h(),
        )
        .expect("member cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&member_sig).get_handle();
        let mut revoked = HashSet::new();
        // Revoke the founder's pubkey. The leaf cap's issuer is the
        // founder, so it should be rejected at the leaf-level revocation
        // check.
        revoked.insert(founder.verifying_key());

        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            member.verifying_key(),
            &revoked,
            store_for(&[
                &founder_cap,
                &founder_sig,
                &member_cap,
                &member_sig,
            ]),
        );
        assert!(matches!(result, Err(VerifyError::Revoked)));
    }

    // ── Scope subsumption tests ───────────────────────────────────────

    #[test]
    fn admin_subsumes_anything() {
        let (parent_root, parent_set) = scope_with(&[PERM_ADMIN], &[]);
        let (child_root, child_set) = scope_with(&[PERM_WRITE, PERM_READ], &[]);
        assert!(scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn write_implies_read_for_child() {
        let (parent_root, parent_set) = scope_with(&[PERM_WRITE], &[]);
        let (child_root, child_set) = scope_with(&[PERM_READ], &[]);
        assert!(scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn read_does_not_imply_write() {
        let (parent_root, parent_set) = scope_with(&[PERM_READ], &[]);
        let (child_root, child_set) = scope_with(&[PERM_WRITE], &[]);
        assert!(!scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn child_cannot_claim_admin_under_non_admin_parent() {
        let (parent_root, parent_set) = scope_with(&[PERM_WRITE], &[]);
        let (child_root, child_set) = scope_with(&[PERM_ADMIN], &[]);
        assert!(!scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn unrestricted_parent_subsumes_branch_restricted_child() {
        let branch_a = *crate::id::ufoid();
        let (parent_root, parent_set) = scope_with(&[PERM_READ], &[]);
        let (child_root, child_set) = scope_with(&[PERM_READ], &[branch_a]);
        assert!(scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn restricted_parent_rejects_unrestricted_child() {
        let branch_a = *crate::id::ufoid();
        let (parent_root, parent_set) = scope_with(&[PERM_READ], &[branch_a]);
        let (child_root, child_set) = scope_with(&[PERM_READ], &[]);
        assert!(!scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn restricted_parent_subsumes_strict_subset() {
        let branch_a = *crate::id::ufoid();
        let branch_b = *crate::id::ufoid();
        let (parent_root, parent_set) =
            scope_with(&[PERM_READ], &[branch_a, branch_b]);
        let (child_root, child_set) = scope_with(&[PERM_READ], &[branch_a]);
        assert!(scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    #[test]
    fn restricted_parent_rejects_disjoint_child() {
        let branch_a = *crate::id::ufoid();
        let branch_b = *crate::id::ufoid();
        let (parent_root, parent_set) = scope_with(&[PERM_READ], &[branch_a]);
        let (child_root, child_set) = scope_with(&[PERM_READ], &[branch_b]);
        assert!(!scope_subsumes(&parent_set, parent_root, &child_set, child_root));
    }

    // ── Revocation tests ──────────────────────────────────────────────

    #[test]
    fn revocation_round_trip() {
        let revoker = signing_key();
        let target = signing_key();
        let (rev_blob, sig_blob) =
            build_revocation(&revoker, target.verifying_key());

        let (out_revoker, out_target) =
            verify_revocation(rev_blob, sig_blob).expect("verifies");
        assert_eq!(out_revoker, revoker.verifying_key());
        assert_eq!(out_target, target.verifying_key());
    }

    #[test]
    fn revocation_set_filters_unauthorised_revokers() {
        let team_root = signing_key();
        let bystander = signing_key();
        let target_a = signing_key();
        let target_b = signing_key();

        let mut authorised = HashSet::new();
        authorised.insert(team_root.verifying_key());

        let rev_a =
            build_revocation(&team_root, target_a.verifying_key());
        let rev_b =
            build_revocation(&bystander, target_b.verifying_key());

        let revoked =
            build_revocation_set(&authorised, [rev_a, rev_b]);

        assert!(revoked.contains(&target_a.verifying_key()));
        assert!(!revoked.contains(&target_b.verifying_key()));
    }

    /// End-to-end: a valid chain becomes invalid once the leaf's
    /// subject is added to the revocation set built from a team-root-
    /// signed revocation blob.
    #[test]
    fn revocation_set_invalidates_chain() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = empty_scope();

        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");

        // Build a revocation against the founder, signed by the team
        // root.
        let (rev_blob, rev_sig_blob) =
            build_revocation(&team_root, founder.verifying_key());

        let mut authorised = HashSet::new();
        authorised.insert(team_root.verifying_key());
        let revoked = build_revocation_set(
            &authorised,
            [(rev_blob, rev_sig_blob)],
        );

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        );
        assert!(matches!(result, Err(VerifyError::Revoked)));
    }

    /// `extract_revocation_pairs` finds matched (rev, sig) pairs in a
    /// heterogeneous blob set and ignores everything else.
    #[test]
    fn extract_revocation_pairs_finds_matched_pair() {
        let team_root = signing_key();
        let target = signing_key();
        let (rev_blob, sig_blob) =
            build_revocation(&team_root, target.verifying_key());

        // Mix in an unrelated cap blob to confirm the scanner doesn't
        // misclassify it as a revocation.
        let (scope_root, scope_facts) = empty_scope();
        let founder = signing_key();
        let (cap_blob, cap_sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");

        let pairs = extract_revocation_pairs([
            rev_blob.clone(),
            sig_blob.clone(),
            cap_blob,
            cap_sig_blob,
        ]);
        assert_eq!(pairs.len(), 1, "exactly one revocation pair");
        let (out_rev, out_sig) = &pairs[0];
        assert_eq!(out_rev.bytes, rev_blob.bytes);
        assert_eq!(out_sig.bytes, sig_blob.bytes);
    }

    /// A revocation sig blob whose target rev blob is missing from the
    /// input set is treated as incomplete and dropped.
    #[test]
    fn extract_revocation_pairs_drops_orphan_sig() {
        let team_root = signing_key();
        let target = signing_key();
        let (_rev_blob, sig_blob) =
            build_revocation(&team_root, target.verifying_key());
        // sig_blob is included but rev_blob is not.
        let pairs = extract_revocation_pairs([sig_blob]);
        assert!(
            pairs.is_empty(),
            "orphan sig (target blob missing) is not paired"
        );
    }

    /// `extract_revocation_pairs` + `build_revocation_set` compose to
    /// give the relay's bootstrap path: scan a pile, retain only
    /// authorised revocations, get the revoked-key set.
    #[test]
    fn extract_then_build_revocation_set_round_trips() {
        let team_root = signing_key();
        let bystander = signing_key();
        let target_authorised = signing_key();
        let target_rogue = signing_key();

        let (rev_a, sig_a) =
            build_revocation(&team_root, target_authorised.verifying_key());
        let (rev_b, sig_b) =
            build_revocation(&bystander, target_rogue.verifying_key());

        let pairs = extract_revocation_pairs([
            rev_a, sig_a, rev_b, sig_b,
        ]);
        assert_eq!(pairs.len(), 2);

        let mut authorised = HashSet::new();
        authorised.insert(team_root.verifying_key());
        let revoked = build_revocation_set(&authorised, pairs);

        assert!(revoked.contains(&target_authorised.verifying_key()));
        assert!(
            !revoked.contains(&target_rogue.verifying_key()),
            "bystander-signed revocation is dropped",
        );
    }

    /// In a length-2 chain, a child cap claiming a permission the
    /// parent doesn't grant must be rejected by the verifier.
    #[test]
    fn verify_rejects_chain_with_scope_violation() {
        let team_root = signing_key();
        let founder = signing_key();
        let member = signing_key();

        // Founder gets only PERM_READ.
        let (founder_scope_root, founder_scope_facts) =
            scope_with(&[PERM_READ], &[]);
        let (founder_cap, founder_sig) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            founder_scope_root,
            founder_scope_facts,
            now_plus_24h(),
        )
        .expect("founder cap builds");

        // Member tries to claim PERM_WRITE — not authorised by parent.
        let (member_scope_root, member_scope_facts) =
            scope_with(&[PERM_WRITE], &[]);
        let (member_cap, member_sig) = build_capability(
            &founder,
            member.verifying_key(),
            Some((founder_cap.clone(), founder_sig.clone())),
            member_scope_root,
            member_scope_facts,
            now_plus_24h(),
        )
        .expect("member cap builds");

        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&member_sig).get_handle();
        let revoked = HashSet::new();
        let result = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            member.verifying_key(),
            &revoked,
            store_for(&[
                &founder_cap,
                &founder_sig,
                &member_cap,
                &member_sig,
            ]),
        );
        assert!(matches!(result, Err(VerifyError::ScopeNotSubset)));
    }

    /// Length-2 chain: founder signs a member capability with the root cap
    /// as parent. The member's cap blob carries `cap_parent` (handle of
    /// the founder's cap) plus an embedded sub-entity carrying the
    /// founder's signature inline.
    #[test]
    fn build_delegated_capability() {
        let team_root = signing_key();
        let founder = signing_key();
        let member = signing_key();

        // Step 1: root issues founder's cap.
        let (founder_scope_root, founder_scope_facts) = empty_scope();
        let (founder_cap, founder_sig) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            founder_scope_root,
            founder_scope_facts,
            now_plus_24h(),
        )
        .expect("founder cap builds");

        // Step 2: founder issues member's cap, embedding founder_sig.
        let (member_scope_root, member_scope_facts) = empty_scope();
        let (member_cap, _member_sig) = build_capability(
            &founder,
            member.verifying_key(),
            Some((founder_cap.clone(), founder_sig.clone())),
            member_scope_root,
            member_scope_facts,
            now_plus_24h(),
        )
        .expect("member cap builds");

        // Member cap must reference the founder's cap as parent and have
        // an embedded sig sub-entity carrying the founder's signature.
        let member_cap_set: TribleSet =
            <TribleSet as TryFromBlob<SimpleArchive>>::try_from_blob(member_cap)
                .expect("valid cap blob");

        let founder_handle: Inline<Handle<SimpleArchive>> =
            (&founder_cap).get_handle();
        let mut parents = find!(
            (e: Id, h: Inline<Handle<SimpleArchive>>),
            pattern!(&member_cap_set, [{ ?e @ cap_parent: ?h }])
        );
        let (cap_entity_id, parent_handle_v) =
            parents.next().expect("cap_parent present");
        assert!(parents.next().is_none(), "exactly one cap_parent");
        assert_eq!(parent_handle_v, founder_handle);

        // Embedded sig sub-entity present, addressed by cap_entity_id.
        let mut embedded = find!(
            (sig: Id),
            pattern!(&member_cap_set, [{
                cap_entity_id @ cap_embedded_parent_sig: ?sig
            }])
        );
        let (sig_id,) = embedded.next().expect("embedded sig pointer");
        assert!(embedded.next().is_none(), "exactly one embedded sig");

        // The embedded sig sub-entity carries the founder's signature
        // tribles; signature must verify over founder_cap.bytes.
        let mut sig_facts = find!(
            (pubkey: VerifyingKey, r, s),
            pattern!(&member_cap_set, [{
                sig_id @
                crate::repo::signed_by: ?pubkey,
                crate::repo::signature_r: ?r,
                crate::repo::signature_s: ?s,
            }])
        );
        let (parent_issuer_pubkey, r_v, s_v) =
            sig_facts.next().expect("embedded sig has sig fields");
        assert!(sig_facts.next().is_none(), "exactly one sig sub-entity");

        // The embedded parent sig is *the parent's* signature, i.e.
        // whoever signed the founder_cap — which is the team root, not
        // the founder.
        assert_eq!(parent_issuer_pubkey, team_root.verifying_key());

        let signature = ed25519::Signature::from_components(r_v, s_v);
        team_root
            .verifying_key()
            .verify(&founder_cap.bytes, &signature)
            .expect("embedded signature verifies over the parent cap bytes");
    }

    /// `VerifiedCapability::permissions` extracts every `metadata::tag`
    /// hung off the scope root. A read-only cap reports just `PERM_READ`.
    #[test]
    fn verified_capability_permissions_read_only() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = scope_with(&[PERM_READ], &[]);
        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");
        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let revoked = HashSet::new();
        let verified = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &revoked,
            store_for(&[&cap_blob, &sig_blob]),
        )
        .expect("chain valid");

        let perms = verified.permissions();
        assert_eq!(perms.len(), 1);
        assert!(perms.contains(&PERM_READ));
        assert!(verified.grants_read());
    }

    /// Unrestricted cap (no `scope_branch` tribles) reports
    /// `granted_branches() == None` and `grants_read_on(any)` is true.
    #[test]
    fn verified_capability_unrestricted_grants_read_on_any_branch() {
        let team_root = signing_key();
        let founder = signing_key();
        let (scope_root, scope_facts) = scope_with(&[PERM_READ], &[]);
        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");
        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let verified = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &HashSet::new(),
            store_for(&[&cap_blob, &sig_blob]),
        )
        .expect("chain valid");

        assert!(verified.granted_branches().is_none(), "unrestricted");
        let any_branch = crate::id::ufoid();
        assert!(verified.grants_read_on(&any_branch));
    }

    /// Branch-restricted cap reports `granted_branches() == Some(set)`
    /// and `grants_read_on` returns true only for branches in that set.
    #[test]
    fn verified_capability_branch_restricted_gates_correctly() {
        let team_root = signing_key();
        let founder = signing_key();
        let allowed = crate::id::ufoid();
        let blocked = crate::id::ufoid();
        let (scope_root, scope_facts) =
            scope_with(&[PERM_READ], &[*allowed]);
        let (cap_blob, sig_blob) = build_capability(
            &team_root,
            founder.verifying_key(),
            None,
            scope_root,
            scope_facts,
            now_plus_24h(),
        )
        .expect("cap builds");
        let leaf_handle: Inline<Handle<SimpleArchive>> =
            (&sig_blob).get_handle();
        let verified = verify_chain(
            team_root.verifying_key(),
            leaf_handle,
            founder.verifying_key(),
            &HashSet::new(),
            store_for(&[&cap_blob, &sig_blob]),
        )
        .expect("chain valid");

        let granted = verified.granted_branches().expect("restricted set");
        assert!(granted.contains(&*allowed));
        assert!(!granted.contains(&*blocked));
        assert!(verified.grants_read_on(&*allowed));
        assert!(!verified.grants_read_on(&*blocked));
    }

    /// A cap with no read-bearing permission (e.g. a hypothetical empty
    /// scope) does not grant read; `grants_read_on` is false even on an
    /// allowed branch.
    #[test]
    fn verified_capability_without_read_permission_blocks_reads() {
        // Construct directly; we skip `verify_chain` here because a
        // permission-less cap would never pass scope subsumption against
        // a real parent. We just want to exercise the helper logic
        // against a manually-shaped `cap_set`.
        let scope_root = crate::id::ufoid();
        // Empty cap_set with no tags hung off scope_root.
        let cap_set = TribleSet::new();
        let verified = VerifiedCapability {
            subject: signing_key().verifying_key(),
            scope_root: *scope_root,
            cap_set,
        };
        assert!(!verified.grants_read());
        let any_branch = crate::id::ufoid();
        assert!(!verified.grants_read_on(&any_branch));
    }
}
