use crate::inline::TryToInline;
use crate::macros::entity;
use crate::macros::pattern;
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
use crate::id::Id;
use crate::inline::encodings::ed25519::{ED25519PublicKey, ED25519RComponent, ED25519SComponent};
use crate::inline::encodings::time::NsTAIInterval;
use crate::inline::Inline;
use crate::prelude::inlineencodings::Handle;
use crate::query::find;
use crate::repo::CommitHandle;
use crate::trible::TribleSet;

/// Why a commit metadata archive is not one of the two canonical commit
/// shapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMetadataError {
    /// The archive is empty, incomplete, non-intrinsic, or contains facts not
    /// emitted by the corresponding constructor.
    Malformed,
    /// More than one entity, or more than one value for a singleton field,
    /// could describe the commit.
    Ambiguous,
}

impl std::fmt::Display for CommitMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed => write!(f, "malformed commit metadata"),
            Self::Ambiguous => write!(f, "ambiguous commit metadata"),
        }
    }
}

impl std::error::Error for CommitMetadataError {}

/// Error returned while loading direct commit parents from a blob-store
/// reader.
#[derive(Debug)]
pub enum StoredCommitError<E> {
    /// The requested blob was present but could not be read or decoded.
    Read(E),
    /// The blob decoded as a SimpleArchive but was not canonical commit
    /// metadata.
    Metadata(CommitMetadataError),
}

impl<E: std::fmt::Display> std::fmt::Display for StoredCommitError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(err) => write!(f, "failed to read commit metadata: {err}"),
            Self::Metadata(err) => err.fmt(f),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for StoredCommitError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(err) => Some(err),
            Self::Metadata(err) => Some(err),
        }
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<(), CommitMetadataError> {
    if slot.replace(value).is_some() {
        Err(CommitMetadataError::Ambiguous)
    } else {
        Ok(())
    }
}

/// Parse one canonical commit-metadata entity and return only its direct
/// parents.
///
/// Accepted inputs are exactly authored commits emitted by
/// [`commit_metadata`] and flat authorless merges emitted by
/// [`merge_metadata`]. The exact-set check also verifies the intrinsic entity
/// id and rejects unknown fields. This function deliberately does not load or
/// verify payload content and does not walk any parent.
pub fn direct_parents(metadata_set: &TribleSet) -> Result<Vec<CommitHandle>, CommitMetadataError> {
    let mut entity: Option<Id> = None;
    let mut content: Option<CommitHandle> = None;
    let mut metadata: Option<CommitHandle> = None;
    let mut message: Option<Inline<Handle<LongString>>> = None;
    let mut signed_by: Option<Inline<ED25519PublicKey>> = None;
    let mut signature_r: Option<Inline<ED25519RComponent>> = None;
    let mut signature_s: Option<Inline<ED25519SComponent>> = None;
    let mut created_at: Option<Inline<NsTAIInterval>> = None;
    let mut parents = Vec::new();

    for fact in metadata_set {
        match entity {
            None => entity = Some(*fact.e()),
            Some(current) if current != *fact.e() => return Err(CommitMetadataError::Ambiguous),
            Some(_) => {}
        }

        let attribute = fact.a();
        if attribute == &super::parent.id() {
            parents.push(*fact.v());
        } else if attribute == &super::content.id() {
            set_once(&mut content, *fact.v())?;
        } else if attribute == &super::metadata.id() {
            set_once(&mut metadata, *fact.v())?;
        } else if attribute == &super::message.id() {
            set_once(&mut message, *fact.v())?;
        } else if attribute == &super::signed_by.id() {
            set_once(&mut signed_by, *fact.v())?;
        } else if attribute == &super::signature_r.id() {
            set_once(&mut signature_r, *fact.v())?;
        } else if attribute == &super::signature_s.id() {
            set_once(&mut signature_s, *fact.v())?;
        } else if attribute == &crate::metadata::created_at.id() {
            set_once(&mut created_at, *fact.v())?;
        } else {
            return Err(CommitMetadataError::Malformed);
        }
    }

    if entity.is_none() {
        return Err(CommitMetadataError::Malformed);
    }
    parents.sort_unstable_by_key(|parent| parent.raw);
    parents.dedup();

    match (content, signed_by, signature_r, signature_s, created_at) {
        (
            Some(content),
            Some(signed_by),
            Some(signature_r),
            Some(signature_s),
            Some(created_at),
        ) => {
            // These two schemas have fallible semantic validation. Signature
            // verification itself would require loading content and is
            // intentionally outside this parent parser.
            let _: VerifyingKey = signed_by
                .try_from_inline()
                .map_err(|_| CommitMetadataError::Malformed)?;
            created_at
                .validate()
                .map_err(|_| CommitMetadataError::Malformed)?;

            let canonical: TribleSet = entity! {
                crate::metadata::created_at: created_at,
                super::content: content,
                super::signed_by: signed_by,
                super::signature_r: signature_r,
                super::signature_s: signature_s,
                super::message?: message,
                super::metadata?: metadata,
                super::parent*: parents.iter().copied(),
            }
            .into();
            if &canonical != metadata_set {
                return Err(CommitMetadataError::Malformed);
            }
        }
        (None, None, None, None, None)
            if metadata.is_none() && message.is_none() && parents.len() > 1 =>
        {
            if merge_metadata(parents.iter().copied()) != *metadata_set {
                return Err(CommitMetadataError::Malformed);
            }
        }
        _ => return Err(CommitMetadataError::Malformed),
    }

    Ok(parents)
}

/// Construct the canonical authorless commit for one complete divergent
/// frontier.
///
/// The whole parent set is encoded flat in one intrinsic entity. There is no
/// signer, timestamp, message, content, or metadata, so every replica given
/// the same parent set produces byte-identical commit metadata. This is a
/// derived view; constructing it does not assert or publish it.
pub(crate) fn merge_metadata(
    parents: impl IntoIterator<Item = Inline<Handle<SimpleArchive>>>,
) -> TribleSet {
    let mut parents: Vec<_> = parents.into_iter().collect();
    parents.sort_unstable_by_key(|parent| parent.raw);
    parents.dedup();
    assert!(
        parents.len() > 1,
        "an authorless merge requires at least two distinct parents"
    );
    entity! {
        super::parent*: parents,
    }
    .into()
}

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
            // Through the clock seam (not Epoch::now directly) so
            // simulated executions mint deterministic, virtual-time
            // commit timestamps — bit-identical commits per seed.
            let now = crate::clock::epoch_now();
            let timestamp: Inline<_> = (now, now).try_to_inline().expect("point interval");
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

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;
    use crate::blob::{Bytes, IntoBlob, MemoryBlobStore};
    use crate::repo::branch_frontier::{ParentLookup, PartialCommitDag};
    use crate::repo::pile::{GetBlobError, Pile};
    use crate::repo::{BlobStore, BlobStorePut};

    fn handle(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    #[test]
    fn parses_exact_authored_and_flat_merge_shapes() {
        let authored_parents = [handle(2), handle(1)];
        let content = TribleSet::new().to_blob();
        let authored = commit_metadata(
            &key(),
            authored_parents,
            Some(Inline::new([3; 32])),
            Some(content),
            Some(handle(4)),
        );
        assert_eq!(
            direct_parents(&authored).unwrap(),
            vec![handle(1), handle(2)]
        );

        let merge = merge_metadata([handle(9), handle(7), handle(8), handle(7)]);
        assert_eq!(
            direct_parents(&merge).unwrap(),
            vec![handle(7), handle(8), handle(9)]
        );
    }

    #[test]
    fn rejects_incomplete_noncanonical_and_ambiguous_shapes() {
        let incomplete: TribleSet = entity! {
            crate::repo::parent: handle(1),
        }
        .into();
        assert_eq!(
            direct_parents(&incomplete),
            Err(CommitMetadataError::Malformed)
        );

        let explicit_id = Id::new([0xAA; 16]).unwrap();
        let explicit = crate::id::ExclusiveId::force_ref(&explicit_id);
        let noncanonical: TribleSet = entity! {
            explicit @
            crate::repo::parent*: [handle(1), handle(2)],
        }
        .into();
        assert_eq!(
            direct_parents(&noncanonical),
            Err(CommitMetadataError::Malformed)
        );

        let ambiguous =
            merge_metadata([handle(1), handle(2)]) + merge_metadata([handle(3), handle(4)]);
        assert_eq!(
            direct_parents(&ambiguous),
            Err(CommitMetadataError::Ambiguous)
        );
    }

    #[test]
    fn memory_reader_distinguishes_absence_and_malformed_without_chasing_refs() {
        let parent = handle(21);
        let content = TribleSet::new().to_blob();
        let content_handle = content.get_handle();
        let metadata = commit_metadata(&key(), [parent], None, Some(content), None);
        let mut store = MemoryBlobStore::new();
        let commit = store.insert(metadata.to_blob());
        let malformed_shape: TribleSet = entity! {
            crate::repo::parent: handle(22),
        }
        .into();
        let malformed_shape = store.insert(malformed_shape.to_blob());
        let malformed = store.insert(Blob::<SimpleArchive>::new(Bytes::from(vec![0; 1])));
        let mut reader = store.reader().unwrap();

        // Neither the content blob nor the parent metadata is stored. A
        // successful direct-parent read therefore proves neither reference
        // was followed.
        assert_ne!(content_handle, commit);
        assert_eq!(
            reader.parents(commit).unwrap(),
            ParentLookup::Present(vec![parent])
        );
        assert_eq!(reader.parents(handle(99)).unwrap(), ParentLookup::Missing);
        assert!(matches!(
            reader.parents(malformed_shape),
            Err(StoredCommitError::Metadata(CommitMetadataError::Malformed))
        ));
        assert!(matches!(
            reader.parents(malformed),
            Err(StoredCommitError::Read(_))
        ));
    }

    #[test]
    fn pile_reader_distinguishes_absence_and_malformed_without_chasing_refs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("commits.pile");
        File::create(&path).unwrap();
        let parent = handle(31);
        let content = TribleSet::new().to_blob();
        let metadata = commit_metadata(&key(), [parent], None, Some(content), None);
        let mut pile = Pile::open(&path).unwrap();
        let commit = pile.put(metadata).unwrap();
        let malformed_shape: TribleSet = entity! {
            crate::repo::parent: handle(32),
        }
        .into();
        let malformed_shape = pile.put(malformed_shape).unwrap();
        let malformed = pile
            .put(Blob::<SimpleArchive>::new(Bytes::from(vec![0; 1])))
            .unwrap();
        let mut reader = pile.reader().unwrap();

        assert_eq!(
            reader.parents(commit).unwrap(),
            ParentLookup::Present(vec![parent])
        );
        assert_eq!(reader.parents(handle(98)).unwrap(), ParentLookup::Missing);
        assert!(matches!(
            reader.parents(malformed_shape),
            Err(StoredCommitError::Metadata(CommitMetadataError::Malformed))
        ));
        assert!(matches!(
            reader.parents(malformed),
            Err(StoredCommitError::Read(GetBlobError::ConversionError(_)))
        ));
    }
}
