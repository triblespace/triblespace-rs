//! Reader-side discovery of collection records in heterogeneous blob stores.
//!
//! Discovery uses [`crate::repo::BlobInfo::length`] only as a cheap candidate
//! filter. Candidate bytes are still fetched through [`BlobStoreGet`] and
//! decoded as a canonical [`SimpleArchive`]. Unknown kinds and bytes that do
//! not decode far enough to identify a kind remain ordinary store noise.
//! Consequently, diagnostics cover candidate-sized known kinds; a malformed
//! shape whose reported length is outside the canonical set is never fetched.

use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::repo::{BlobStoreGet, BlobStoreList};

use super::{
    CollectionCommit, CollectionDefinition, CollectionDerive, CollectionMerge, CollectionRecord,
    CommitVerificationError, RecordDecodeError, COLLECTION_COMMIT_ARCHIVE_LEN,
    COLLECTION_DEFINITION_ARCHIVE_LEN, COLLECTION_DERIVE_ARCHIVE_LEN, COLLECTION_MERGE_ARCHIVE_LEN,
};

/// One known collection record with a discovery-time validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionRecordDiagnostic {
    /// Store handle of the record carrying this diagnostic.
    pub handle: Inline<Handle<UnknownBlob>>,
    /// Structural or cryptographic validation failure.
    pub error: CollectionRecordDiagnosticError,
}

/// Observable validation failure for a known collection record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionRecordDiagnosticError {
    /// A known kind did not have that kind's exact canonical shape.
    Malformed(RecordDecodeError),
    /// A structurally canonical commit failed strict Ed25519 verification.
    InvalidCommit(CommitVerificationError),
}

impl fmt::Display for CollectionRecordDiagnosticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(error) => write!(f, "malformed collection record: {error}"),
            Self::InvalidCommit(error) => write!(f, "invalid collection commit: {error}"),
        }
    }
}

impl Error for CollectionRecordDiagnosticError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Malformed(error) => Some(error),
            Self::InvalidCommit(error) => Some(error),
        }
    }
}

/// Structurally canonical records and diagnostics from one store scan.
///
/// Every collection is sorted by intrinsic record id, and diagnostics are
/// sorted by blob handle. The result therefore does not expose enumeration or
/// pile-append order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiscoveredCollectionRecords {
    definitions: Vec<CollectionDefinition>,
    commits: Vec<CollectionCommit>,
    merges: Vec<CollectionMerge>,
    derives: Vec<CollectionDerive>,
    diagnostics: Vec<CollectionRecordDiagnostic>,
}

impl DiscoveredCollectionRecords {
    /// Canonical collection definitions, ordered by intrinsic id.
    pub fn definitions(&self) -> &[CollectionDefinition] {
        &self.definitions
    }

    /// Commits with valid strict self-signatures, ordered by intrinsic id.
    ///
    /// Signature validity does not authorize the signing key. Callers apply
    /// local authorization policy before treating a commit as a membership
    /// root.
    pub fn commits(&self) -> &[CollectionCommit] {
        &self.commits
    }

    /// Structurally canonical merge claims, ordered by intrinsic id.
    pub fn merges(&self) -> &[CollectionMerge] {
        &self.merges
    }

    /// Structurally canonical derive claims, ordered by intrinsic id.
    pub fn derives(&self) -> &[CollectionDerive] {
        &self.derives
    }

    /// Known records with malformed structure or failed commit verification.
    pub fn diagnostics(&self) -> &[CollectionRecordDiagnostic] {
        &self.diagnostics
    }

    fn canonicalize(&mut self) {
        self.definitions
            .sort_unstable_by_key(CollectionDefinition::id);
        self.definitions.dedup_by_key(|record| record.id());
        self.commits.sort_unstable_by_key(CollectionCommit::id);
        self.commits.dedup_by_key(|record| record.id());
        self.merges.sort_unstable_by_key(CollectionMerge::id);
        self.merges.dedup_by_key(|record| record.id());
        self.derives.sort_unstable_by_key(CollectionDerive::id);
        self.derives.dedup_by_key(|record| record.id());
        self.diagnostics
            .sort_unstable_by_key(|entry| entry.handle.raw);
        self.diagnostics.dedup_by_key(|entry| entry.handle.raw);
    }
}

/// A storage failure that prevents a complete collection-record scan.
#[derive(Debug)]
pub enum CollectionDiscoveryError<ListError, GetError> {
    /// Blob enumeration failed.
    List(ListError),
    /// A candidate blob could not be retrieved.
    Get {
        /// Candidate handle whose retrieval failed.
        handle: Inline<Handle<UnknownBlob>>,
        /// Backend error.
        source: GetError,
    },
}

impl<ListError, GetError> fmt::Display for CollectionDiscoveryError<ListError, GetError>
where
    ListError: fmt::Display,
    GetError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::List(error) => write!(f, "failed to enumerate collection records: {error}"),
            Self::Get { handle, source } => {
                write!(
                    f,
                    "failed to retrieve collection-record candidate {handle:?}: {source}"
                )
            }
        }
    }
}

impl<ListError, GetError> Error for CollectionDiscoveryError<ListError, GetError>
where
    ListError: Error + 'static,
    GetError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::List(error) => Some(error),
            Self::Get { source, .. } => Some(source),
        }
    }
}

/// Whether storage-observed metadata has the length of a collection record.
///
/// This is only a candidate test. It is never evidence that the bytes have a
/// particular encoding, kind, canonical structure, or valid signature.
pub const fn is_collection_record_archive_len(length: u64) -> bool {
    length == COLLECTION_DEFINITION_ARCHIVE_LEN
        || length == COLLECTION_COMMIT_ARCHIVE_LEN
        || length == COLLECTION_MERGE_ARCHIVE_LEN
        || length == COLLECTION_DERIVE_ARCHIVE_LEN
}

/// Discover structurally canonical collection records in a heterogeneous blob store.
///
/// Noncandidate lengths are not fetched, even when their raw bytes might
/// contain a known tag. Candidate-sized noise and unknown record kinds are
/// ignored. Once a decoded `metadata::tag` names a known collection kind,
/// malformed structure becomes a diagnostic. Commits are included only after
/// strict self-signature verification. This establishes cryptographic
/// authorship, not authorization; callers decide which signing keys may
/// introduce membership roots. Discovery likewise does not validate
/// representation-specific `MERGE` or `DERIVE` semantics. Listing and
/// retrieval failures abort the scan because returning a partial set as
/// complete would make the result depend on backend failure timing.
pub fn discover_collection_records<R>(
    reader: &R,
) -> Result<
    DiscoveredCollectionRecords,
    CollectionDiscoveryError<<R as BlobStoreList>::Err, <R as BlobStoreGet>::GetError<Infallible>>,
>
where
    R: BlobStoreList + BlobStoreGet,
{
    let mut discovered = DiscoveredCollectionRecords::default();

    for listed in reader.blobs() {
        let info = listed.map_err(CollectionDiscoveryError::List)?;
        if !is_collection_record_archive_len(info.length) {
            continue;
        }

        let archive_handle: Inline<Handle<SimpleArchive>> = info.handle.transmute();
        let blob: Blob<SimpleArchive> =
            reader
                .get(archive_handle)
                .map_err(|source| CollectionDiscoveryError::Get {
                    handle: info.handle,
                    source,
                })?;

        let record = match CollectionRecord::decode(&blob) {
            Ok(Some(record)) => record,
            Ok(None) | Err(RecordDecodeError::Archive(_)) => continue,
            Err(error) => {
                discovered.diagnostics.push(CollectionRecordDiagnostic {
                    handle: info.handle,
                    error: CollectionRecordDiagnosticError::Malformed(error),
                });
                continue;
            }
        };

        match record {
            CollectionRecord::Definition(record) => discovered.definitions.push(record),
            CollectionRecord::Commit(record) => match record.verify_strict() {
                Ok(()) => discovered.commits.push(record),
                Err(error) => discovered.diagnostics.push(CollectionRecordDiagnostic {
                    handle: info.handle,
                    error: CollectionRecordDiagnosticError::InvalidCommit(error),
                }),
            },
            CollectionRecord::Merge(record) => discovered.merges.push(record),
            CollectionRecord::Derive(record) => discovered.derives.push(record),
        }
    }

    discovered.canonicalize();
    Ok(discovered)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use ed25519_dalek::SigningKey;

    use crate::blob::{BlobEncoding, TryFromBlob};
    use crate::collection::{
        collection, data, derive_output, empty_metadata_handle, merge_high, merge_low,
        merge_result, CollectionData, KIND_COLLECTION_COMMIT, KIND_COLLECTION_MERGE,
    };
    use crate::id::Id;
    use crate::inline::InlineEncoding;
    use crate::metadata;
    use crate::prelude::entity;
    use crate::repo::{metadata as commit_metadata, signature_r, signature_s, signed_by, BlobInfo};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ProbeListError;

    impl fmt::Display for ProbeListError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "injected list failure")
        }
    }

    impl Error for ProbeListError {}

    #[derive(Debug)]
    enum ProbeGetError<E: Error> {
        Missing,
        Conversion(E),
    }

    impl<E: Error> fmt::Display for ProbeGetError<E> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Missing => write!(f, "injected missing blob"),
                Self::Conversion(error) => write!(f, "conversion failed: {error}"),
            }
        }
    }

    impl<E: Error> Error for ProbeGetError<E> {}

    #[derive(Clone)]
    struct ProbeReader {
        listed: Vec<Result<BlobInfo, ProbeListError>>,
        blobs: Vec<(Inline<Handle<UnknownBlob>>, Blob<UnknownBlob>)>,
        gets: Arc<AtomicUsize>,
    }

    impl ProbeReader {
        fn from_blobs(blobs: Vec<Blob<UnknownBlob>>) -> Self {
            let blobs: Vec<_> = blobs
                .into_iter()
                .map(|blob| (blob.get_handle(), blob))
                .collect();
            let listed = blobs
                .iter()
                .map(|(handle, blob)| {
                    Ok(BlobInfo {
                        handle: *handle,
                        length: blob.bytes.len() as u64,
                    })
                })
                .collect();
            Self {
                listed,
                blobs,
                gets: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn listed_only(listed: Vec<Result<BlobInfo, ProbeListError>>) -> Self {
            Self {
                listed,
                blobs: Vec::new(),
                gets: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reversed(mut self) -> Self {
            self.listed.reverse();
            self
        }

        fn get_count(&self) -> usize {
            self.gets.load(Ordering::Relaxed)
        }
    }

    impl BlobStoreList for ProbeReader {
        type Iter<'a> = std::vec::IntoIter<Result<BlobInfo, Self::Err>>;
        type Err = ProbeListError;

        fn blobs(&self) -> Self::Iter<'_> {
            self.listed.clone().into_iter()
        }
    }

    impl BlobStoreGet for ProbeReader {
        type GetError<E: Error + Send + Sync + 'static> = ProbeGetError<E>;

        fn get<T, S>(
            &self,
            handle: Inline<Handle<S>>,
        ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
        where
            S: BlobEncoding + 'static,
            T: TryFromBlob<S>,
            Handle<S>: InlineEncoding,
        {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let handle: Inline<Handle<UnknownBlob>> = handle.transmute();
            let blob = self
                .blobs
                .iter()
                .find(|(stored, _)| stored == &handle)
                .map(|(_, blob)| blob.clone())
                .ok_or(ProbeGetError::Missing)?;
            T::try_from_blob(blob.transmute()).map_err(ProbeGetError::Conversion)
        }
    }

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn hash(byte: u8) -> CollectionData {
        Inline::new([byte; 32])
    }

    fn erased<S: BlobEncoding>(blob: Blob<S>) -> Blob<UnknownBlob>
    where
        Handle<S>: InlineEncoding,
    {
        blob.transmute::<UnknownBlob>()
    }

    fn archive(facts: crate::trible::TribleSet) -> Blob<SimpleArchive> {
        <crate::trible::TribleSet as crate::blob::IntoBlob<SimpleArchive>>::to_blob(facts)
    }

    fn fixture_blobs() -> Vec<Blob<UnknownBlob>> {
        let definition = CollectionDefinition::new(id(1), id(2), id(3));
        let commit = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            definition.id(),
            hash(4),
            empty_metadata_handle(),
        );
        let merge = CollectionMerge::new(definition.id(), hash(4), hash(5), hash(6));
        let derive = CollectionDerive::new(definition.id(), id(7), hash(4), hash(8));

        let unknown_kind = archive(
            entity! {
                metadata::tag: id(90),
                collection: definition.id(),
                merge_low: hash(1),
                merge_high: hash(2),
                merge_result: hash(3),
            }
            .into_facts(),
        );

        let malformed_known_kind = archive(
            entity! {
                metadata::tag: KIND_COLLECTION_MERGE,
                collection: definition.id(),
                merge_low: hash(1),
                merge_high: hash(2),
                derive_output: hash(3),
            }
            .into_facts(),
        );

        let (r, mut s) = commit.signature();
        s.raw[0] ^= 1;
        let invalid_commit = archive(
            entity! {
                metadata::tag: KIND_COLLECTION_COMMIT,
                collection: commit.collection(),
                data: commit.data(),
                commit_metadata: commit.metadata(),
                signed_by: commit.public_key(),
                signature_r: r,
                signature_s: s,
            }
            .into_facts(),
        );

        vec![
            erased::<SimpleArchive>(CollectionDefinition::to_blob(&definition)),
            erased::<SimpleArchive>(CollectionCommit::to_blob(&commit)),
            erased::<SimpleArchive>(CollectionMerge::to_blob(&merge)),
            erased::<SimpleArchive>(CollectionDerive::to_blob(&derive)),
            erased::<SimpleArchive>(unknown_kind),
            erased::<SimpleArchive>(malformed_known_kind),
            erased::<SimpleArchive>(invalid_commit),
            Blob::new(vec![0_u8; COLLECTION_MERGE_ARCHIVE_LEN as usize].into()),
            Blob::new(vec![1_u8, 2, 3].into()),
        ]
    }

    #[test]
    fn mixed_store_is_filtered_verified_diagnosed_and_order_independent() {
        let forward = ProbeReader::from_blobs(fixture_blobs());
        let reverse = ProbeReader::from_blobs(fixture_blobs()).reversed();

        let forward_records = discover_collection_records(&forward).unwrap();
        let reverse_records = discover_collection_records(&reverse).unwrap();

        assert_eq!(forward_records, reverse_records);
        assert_eq!(forward_records.definitions().len(), 1);
        assert_eq!(forward_records.commits().len(), 1);
        assert_eq!(forward_records.merges().len(), 1);
        assert_eq!(forward_records.derives().len(), 1);
        assert_eq!(forward_records.diagnostics().len(), 2);
        assert!(forward_records.diagnostics().iter().any(|diagnostic| {
            matches!(
                diagnostic.error,
                CollectionRecordDiagnosticError::Malformed(RecordDecodeError::MissingField(
                    "merge_result"
                ))
            )
        }));
        assert!(forward_records.diagnostics().iter().any(|diagnostic| {
            matches!(
                diagnostic.error,
                CollectionRecordDiagnosticError::InvalidCommit(
                    CommitVerificationError::InvalidSignature
                )
            )
        }));
        assert_eq!(forward.get_count(), 8);
        assert_eq!(reverse.get_count(), 8);
    }

    #[test]
    fn noncandidate_lengths_do_not_issue_gets() {
        let reader = ProbeReader::listed_only(vec![
            Ok(BlobInfo {
                handle: Inline::new([1; 32]),
                length: 0,
            }),
            Ok(BlobInfo {
                handle: Inline::new([2; 32]),
                length: COLLECTION_DEFINITION_ARCHIVE_LEN - 1,
            }),
            Ok(BlobInfo {
                handle: Inline::new([3; 32]),
                length: COLLECTION_COMMIT_ARCHIVE_LEN + 1,
            }),
        ]);

        let discovered = discover_collection_records(&reader).unwrap();
        assert_eq!(discovered, DiscoveredCollectionRecords::default());
        assert_eq!(reader.get_count(), 0);
    }

    #[test]
    fn list_and_candidate_get_failures_are_real_errors() {
        let list_failure = ProbeReader::listed_only(vec![Err(ProbeListError)]);
        assert!(matches!(
            discover_collection_records(&list_failure),
            Err(CollectionDiscoveryError::List(ProbeListError))
        ));

        let missing = ProbeReader::listed_only(vec![Ok(BlobInfo {
            handle: Inline::new([9; 32]),
            length: COLLECTION_DEFINITION_ARCHIVE_LEN,
        })]);
        assert!(matches!(
            discover_collection_records(&missing),
            Err(CollectionDiscoveryError::Get {
                source: ProbeGetError::Missing,
                ..
            })
        ));
        assert_eq!(missing.get_count(), 1);
    }
}
