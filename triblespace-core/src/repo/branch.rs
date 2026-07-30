use crate::macros::entity;
use crate::macros::pattern;
use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
use itertools::Itertools;

use crate::blob::encodings::longstring::LongString;
use crate::blob::Blob;
use crate::find;
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::encodings::time::NsTAIInterval;
use crate::inline::Inline;
use crate::inline::TryToInline;
use crate::metadata;
use crate::prelude::blobencodings::SimpleArchive;
use crate::trible::TribleSet;

/// Why a branch metadata subject could not be identified uniquely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BranchEntityError {
    /// No entity in the metadata identifies itself as the expected branch.
    Missing,
    /// More than one entity identifies itself as the expected branch.
    Ambiguous,
}

/// Resolve the unique entity describing `branch_id` in one metadata blob.
///
/// Branch metadata may carry arbitrary facts on unrelated annotation entities.
/// Consumers must first establish this subject and then read branch fields from
/// that subject only; scanning the whole [`TribleSet`] lets an annotation's
/// `metadata::name`, `repo::head`, or timestamp impersonate branch state.
pub fn branch_entity(meta: &TribleSet, branch_id: Id) -> Result<Id, BranchEntityError> {
    let mut entities = find!(
        entity: Id,
        pattern!(meta, [{ ?entity @ super::branch: branch_id }])
    );
    let Some(entity) = entities.next() else {
        return Err(BranchEntityError::Missing);
    };
    if entities.next().is_some() {
        return Err(BranchEntityError::Ambiguous);
    }
    Ok(entity)
}

/// Return every fact except the branch metadata entity being superseded.
///
/// Unrelated entities are carried byte-for-byte even when they themselves use
/// `repo::branch` to describe another relationship.
pub fn carried_facts(meta: &TribleSet, branch_id: Id) -> Result<TribleSet, BranchEntityError> {
    let replaced = branch_entity(meta, branch_id)?;
    let mut carried = TribleSet::new();
    for fact in meta.iter().filter(|fact| fact.e() != &replaced) {
        carried.insert(fact);
    }
    Ok(carried)
}

/// Current TAI time as a collapsed `NsTAIInterval`. Used as
/// `metadata::updated_at` on every branch metadata blob so that peers can
/// order concurrent HEAD gossips without walking ancestor chains.
///
/// TAI is strictly monotone (no leap-second jumps). Wall-clock regressions
/// still mean subsequent publishes land "in the past" from the publisher's
/// view; receivers simply hold out until the publisher's clock catches up
/// and a fresher timestamp arrives.
fn now_updated_at() -> Inline<NsTAIInterval> {
    let now = crate::clock::epoch_now();
    (now, now)
        .try_to_inline()
        .expect("same epoch is a valid point interval")
}

/// Builds a metadata [`TribleSet`] describing a branch and signs it.
///
/// The metadata records the branch `name` handle, its unique `branch_id`
/// and optionally the handle of the initial commit. The commit handle is
/// signed with `signing_key` allowing the repository to verify its
/// authenticity.
///
/// The metadata entity id is derived intrinsically from the
/// `(attribute, value)` pairs via `entity!`'s content-hash form — no
/// open-coded derivation. Because every publish stamps a fresh
/// `metadata::updated_at`, each publish produces a distinct entity id
/// and a distinct metadata blob hash (which is what lets receivers order
/// concurrent HEAD gossips by timestamp alone).
pub fn branch_metadata(
    signing_key: &SigningKey,
    branch_id: Id,
    name: Inline<Handle<LongString>>,
    commit_head: Option<Blob<SimpleArchive>>,
) -> TribleSet {
    let (head_handle, signed_by, signature) = match commit_head.as_ref() {
        Some(blob) => (
            Some(blob.get_handle()),
            Some(signing_key.verifying_key()),
            Some(signing_key.sign(&blob.bytes)),
        ),
        None => (None, None, None),
    };
    let updated_at = now_updated_at();

    let fragment = entity! {
        super::branch: branch_id,
        super::head?: head_handle,
        super::signed_by?: signed_by,
        super::signature_r?: signature,
        super::signature_s?: signature,
        metadata::name: name,
        metadata::updated_at: updated_at,
    };

    fragment.into()
}

/// Unsigned variant of [`branch_metadata`] used when authenticity is not
/// required. The resulting set omits signature information and can
/// therefore be created without access to a private key.
pub fn branch_unsigned(
    branch_id: Id,
    name: Inline<Handle<LongString>>,
    commit_head: Option<Blob<SimpleArchive>>,
) -> TribleSet {
    let head_handle = commit_head.as_ref().map(|blob| blob.get_handle());
    let updated_at = now_updated_at();

    let fragment = entity! {
        super::branch: branch_id,
        super::head?: head_handle,
        metadata::name: name,
        metadata::updated_at: updated_at,
    };

    fragment.into()
}

/// Error returned when branch signature verification fails.
pub enum ValidationError {
    /// The metadata has no unique entity for the expected branch id.
    InvalidBranchMetadata,
    /// The metadata contains multiple signature entities for the same commit.
    AmbiguousSignature,
    /// No signature information was found in the metadata.
    MissingSignature,
    /// The signature did not match the commit bytes or the public key was invalid.
    FailedValidation,
}

impl From<SignatureError> for ValidationError {
    /// Converts an Ed25519 signature error into a [`ValidationError::FailedValidation`].
    fn from(_: SignatureError) -> Self {
        ValidationError::FailedValidation
    }
}

/// Checks that the metadata signature matches the provided commit blob.
///
/// The function extracts the public key and signature from `metadata` and
/// verifies that it signs the `commit_head` blob. If the metadata is missing a
/// signature or contains multiple signature entities the appropriate
/// `ValidationError` variant is returned.
pub fn verify(
    branch_id: Id,
    commit_head: Blob<SimpleArchive>,
    metadata: TribleSet,
) -> Result<(), ValidationError> {
    let handle = commit_head.get_handle();
    let branch_entity =
        branch_entity(&metadata, branch_id).map_err(|_| ValidationError::InvalidBranchMetadata)?;
    let (pubkey, r, s) = match find!(
    (pubkey: Inline<_>, r, s),
    pattern!(&metadata, [
    {
        branch_entity @ super::head: handle,
        super::signed_by: ?pubkey,
        super::signature_r: ?r,
        super::signature_s: ?s,
    }]))
    .at_most_one()
    {
        Ok(Some(result)) => result,
        Ok(None) => return Err(ValidationError::MissingSignature),
        Err(_) => return Err(ValidationError::AmbiguousSignature),
    };

    let Ok(pubkey): Result<VerifyingKey, _> = pubkey.try_from_inline() else {
        return Err(ValidationError::FailedValidation);
    };
    let signature = Signature::from_components(r, s);
    pubkey.verify(&commit_head.bytes, &signature)?;

    Ok(())
}
