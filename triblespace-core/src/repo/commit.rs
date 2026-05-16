use crate::macros::entity;
use crate::macros::pattern;
use crate::inline::TryToInline;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
use itertools::Itertools;

use ed25519::signature::Signer;

use crate::blob::encodings::longstring::LongString;
use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::Blob;
use crate::prelude::inlineencodings::Handle;
use crate::query::find;
use crate::trible::TribleSet;
use crate::inline::Inline;

use hifitime::Epoch;

/// Error returned when commit signature verification fails.
pub enum ValidationError {
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

/// Constructs commit metadata describing `content`, optional `metadata`, and its parent commits.
///
/// The resulting [`TribleSet`] is signed using `signing_key` when content is
/// present, so that its authenticity can later be verified. If `msg` is
/// provided it is stored as a long commit message via a LongString blob
/// handle. If `metadata` is provided it is stored as a SimpleArchive handle.
///
/// The commit's entity id is derived intrinsically from the
/// `(attribute, value)` pairs present in the metadata — so two commits with
/// identical content, parents, and signatures collide on entity id and blob
/// hash alike. This matters especially for **merge commits**
/// (`content = None`): merges carry no author-specific bits (no signature,
/// no timestamp, no random entity id), so two peers merging the same parent
/// set produce bit-identical merge commits, and parallel-merge scenarios
/// converge in zero extra rounds.
pub fn commit_metadata(
    signing_key: &SigningKey,
    parents: impl IntoIterator<Item = Inline<Handle<SimpleArchive>>>,
    msg: Option<Inline<Handle<LongString>>>,
    content: Option<Blob<SimpleArchive>>,
    metadata: Option<Inline<Handle<SimpleArchive>>>,
) -> TribleSet {
    // Authored commits carry a timestamp and a signature. Merge commits
    // (content = None) carry neither, so they stay content-deterministic.
    let (content_handle, signed_by, signature, created_at) = match content.as_ref() {
        Some(blob) => {
            let now = Epoch::now().expect("system time");
            let timestamp: Inline<_> =
                (now, now).try_to_inline().expect("point interval");
            (
                Some(blob.get_handle()),
                Some(signing_key.verifying_key()),
                Some(signing_key.sign(&blob.bytes)),
                Some(timestamp),
            )
        }
        None => (None, None, None, None),
    };
    let parents: Vec<_> = parents.into_iter().collect();

    // `entity!` without an explicit `id @` prefix derives the entity id
    // by hashing the sorted/deduped (attr_id, value) pairs. The resulting
    // commit is content-addressed at both the blob level (via
    // SimpleArchive) and the entity-id level.
    let fragment = entity! {
        crate::metadata::created_at?: created_at,
        super::content?: content_handle,
        super::signed_by?: signed_by,
        super::signature_r?: signature,
        super::signature_s?: signature,
        super::message?: msg,
        super::metadata?: metadata,
        super::parent*: parents,
    };

    fragment.into()
}

/// Validates that the `metadata` blob genuinely signs the supplied commit
/// `content`.
///
/// Returns an error if the signature information is missing, malformed or does
/// not match the commit bytes.
pub fn verify(content: Blob<SimpleArchive>, metadata: TribleSet) -> Result<(), ValidationError> {
    let handle = content.get_handle();
    let (pubkey, r, s) = match find!(
    (pubkey: Inline<_>, r, s),
    pattern!(&metadata, [
    {
        super::content: handle,
        super::signed_by: ?pubkey,
        super::signature_r: ?r,
        super::signature_s: ?s
    }]))
    .at_most_one()
    {
        Ok(Some(result)) => result,
        Ok(None) => return Err(ValidationError::MissingSignature),
        Err(_) => return Err(ValidationError::AmbiguousSignature),
    };

    let pubkey: VerifyingKey = pubkey.try_from_inline()?;
    let signature = Signature::from_components(r, s);
    pubkey.verify(&content.bytes, &signature)?;
    Ok(())
}
