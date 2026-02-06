use crate::macros::entity;
use crate::macros::pattern;
use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Verifier;
use ed25519_dalek::VerifyingKey;
use itertools::Itertools;

use crate::blob::Blob;
use crate::find;
use crate::id::rngid;
use crate::id::Id;
use crate::metadata;
use crate::prelude::blobschemas::SimpleArchive;
use crate::trible::TribleSet;
use crate::value::Value;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::blob::schemas::longstring::LongString;

/// Builds a metadata [`TribleSet`] describing a branch and signs it.
///
/// The metadata records the branch `name` handle, its unique `branch_id` and
/// optionally the handle of the initial commit. The commit handle is signed with
/// `signing_key` allowing the repository to verify its authenticity.
pub fn branch_metadata(
    signing_key: &SigningKey,
    branch_id: Id,
    name: Value<Handle<Blake3, LongString>>,
    commit_head: Option<Blob<SimpleArchive>>,
) -> TribleSet {
    let mut metadata: TribleSet = Default::default();

    let metadata_entity = rngid();

    metadata += entity! { &metadata_entity @  super::branch: branch_id  };
    if let Some(commit_head) = commit_head {
        let handle = commit_head.get_handle();
        let signature = signing_key.sign(&commit_head.bytes);

        metadata += entity! { &metadata_entity @
           super::head: handle,
           super::signed_by: signing_key.verifying_key(),
           super::signature_r: signature,
           super::signature_s: signature,
        };
    }
    metadata += entity! { &metadata_entity @  metadata::name: name  };

    metadata
}

/// Unsigned variant of [`branch`] used when authenticity is not required.
///
/// The resulting set omits any signature information and can therefore be
/// created without access to a private key.
pub fn branch_unsigned(
    branch_id: Id,
    name: Value<Handle<Blake3, LongString>>,
    commit_head: Option<Blob<SimpleArchive>>,
) -> TribleSet {
    let metadata_entity = rngid();

    let mut metadata: TribleSet = Default::default();

    metadata += entity! { &metadata_entity @  super::branch: branch_id  };

    if let Some(commit_head) = commit_head {
        let handle = commit_head.get_handle();
        metadata += entity! { &metadata_entity @  super::head: handle  };
    }

    metadata += entity! { &metadata_entity @  metadata::name: name  };

    metadata
}

pub enum ValidationError {
    AmbiguousSignature,
    MissingSignature,
    FailedValidation,
}

impl From<SignatureError> for ValidationError {
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
    commit_head: Blob<SimpleArchive>,
    metadata: TribleSet,
) -> Result<(), ValidationError> {
    let handle = commit_head.get_handle();
    let (pubkey, r, s) = match find!(
    (pubkey: Value<_>, r, s),
    pattern!(&metadata, [
    {
        super::head: handle,
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

    let Ok(pubkey): Result<VerifyingKey, _> = pubkey.try_from_value() else {
        return Err(ValidationError::FailedValidation);
    };
    let signature = Signature::from_components(r, s);
    pubkey.verify(&commit_head.bytes, &signature)?;

    Ok(())
}
