//! Discovery over native collection-record storage.
//!
//! A [`CollectionRead`](super::CollectionRead) snapshot is responsible for
//! decoding its physical representation into structurally canonical typed
//! records.
//! Discovery therefore performs no blob-store scan and has no malformed-record
//! recovery path: a structural storage failure is fatal. It verifies signed
//! commits, classifies records, and canonicalizes the resulting semantic view.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use ed25519_dalek::VerifyingKey;

use crate::blob::encodings::UnknownBlob;
use crate::inline::encodings::ed25519::ED25519PublicKey;
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::repo::BlobStoreList;

use super::{
    CollectionCommit, CollectionDerive, CollectionEncoding, CollectionHandle, CollectionMerge,
    CollectionRead, CollectionRecord, CollectionRecordSelector, CommitVerificationError, Cover,
};

/// One collection record with a discovery-time validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionRecordDiagnostic {
    /// Exact structurally valid commit carrying this diagnostic.
    pub record: CollectionCommit,
    /// Cryptographic validation failure.
    pub error: CollectionRecordDiagnosticError,
}

/// Observable semantic validation failure for a structurally valid record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionRecordDiagnosticError {
    /// A structurally canonical commit failed strict Ed25519 verification.
    InvalidCommit(CommitVerificationError),
}

impl fmt::Display for CollectionRecordDiagnosticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommit(error) => write!(f, "invalid collection commit: {error}"),
        }
    }
}

impl Error for CollectionRecordDiagnosticError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidCommit(error) => Some(error),
        }
    }
}

/// Structurally canonical records and diagnostics from one store enumeration.
///
/// Every collection is sorted by its exact canonical record value, as are diagnostics. The
/// result therefore does not expose backend enumeration or append order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiscoveredCollectionRecords {
    commits: Vec<CollectionCommit>,
    merges: Vec<CollectionMerge>,
    derives: Vec<CollectionDerive>,
    diagnostics: Vec<CollectionRecordDiagnostic>,
}

impl DiscoveredCollectionRecords {
    /// Commits with valid strict self-signatures, ordered canonically.
    ///
    /// Signature validity does not authorize the signing key. Callers apply
    /// local authorization policy before treating a commit as a membership
    /// root.
    pub fn commits(&self) -> &[CollectionCommit] {
        &self.commits
    }

    /// Structurally canonical merge claims, ordered canonically.
    pub fn merges(&self) -> &[CollectionMerge] {
        &self.merges
    }

    /// Structurally canonical derive claims, ordered canonically.
    pub fn derives(&self) -> &[CollectionDerive] {
        &self.derives
    }

    /// Structurally valid records whose semantic verification failed.
    pub fn diagnostics(&self) -> &[CollectionRecordDiagnostic] {
        &self.diagnostics
    }

    fn canonicalize(&mut self) {
        self.commits.sort_unstable();
        self.commits.dedup();
        self.merges.sort_unstable();
        self.merges.dedup();
        self.derives.sort_unstable();
        self.derives.dedup();
        self.diagnostics.sort_unstable_by_key(|entry| entry.record);
        self.diagnostics.dedup_by_key(|entry| entry.record);
    }
}

/// A native-store failure that prevents one complete record enumeration.
#[derive(Debug)]
pub enum CollectionDiscoveryError<RecordsError> {
    /// The store could not start or complete record enumeration.
    Records(RecordsError),
    /// The frozen blob index could not answer direct-reference residency.
    Residency {
        /// Exact direct reference whose residency was being inspected.
        reference: Inline<Handle<UnknownBlob>>,
        /// Backend observation failure.
        source: Box<dyn Error + Send + Sync + 'static>,
    },
}

impl<RecordsError> fmt::Display for CollectionDiscoveryError<RecordsError>
where
    RecordsError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Records(error) => write!(f, "failed to enumerate collection records: {error}"),
            Self::Residency { reference, source } => write!(
                f,
                "failed to inspect resident collection-record reference {}: {source}",
                hex::encode_upper(reference.raw),
            ),
        }
    }
}

impl<RecordsError> Error for CollectionDiscoveryError<RecordsError>
where
    RecordsError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Records(error) => Some(error),
            Self::Residency { source, .. } => Some(source.as_ref()),
        }
    }
}

/// Discover and cryptographically classify native collection records.
///
/// The store owns physical decoding and returns only structurally canonical
/// [`CollectionRecord`] values. Any failure to begin or continue enumeration
/// aborts discovery rather than turning storage corruption into a partial
/// semantic view. Commits enter the accepted result only after strict
/// self-signature verification. This establishes authorship, not
/// authorization; callers still choose which signing keys may introduce
/// membership roots. Encoding-specific `MERGE` validation and mapping-specific
/// `DERIVE` validation likewise remain the resolver callback's responsibility.
pub fn discover_collection_records<S>(
    snapshot: &S,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: CollectionRead,
{
    let mut discovered = DiscoveredCollectionRecords::default();
    let records = snapshot
        .records()
        .map_err(CollectionDiscoveryError::Records)?;

    for record in records {
        let record = record.map_err(CollectionDiscoveryError::Records)?;
        match record {
            CollectionRecord::Commit(record) => match record.verify_strict() {
                Ok(()) => discovered.commits.push(record),
                Err(error) => discovered.diagnostics.push(CollectionRecordDiagnostic {
                    record,
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

/// Whether every blob named directly by one raw collection record belongs to
/// this immutable blob snapshot.
///
/// Raw record storage deliberately preserves dangling evidence for repair.
/// Semantic discovery crosses the stronger boundary here: an equation or
/// commit cannot affect a snapshot until its complete direct closure was
/// already resident when that snapshot was frozen. Absence is inert; failure
/// to observe the blob index is propagated rather than confused with a
/// partial semantic result. The raw [`CollectionRead`] surface remains
/// available to repair dangling records independently.
fn record_references_are_resident<S>(
    snapshot: &S,
    record: CollectionRecord,
) -> Result<bool, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
{
    for reference in record.blob_references() {
        let resident = snapshot.contains_blob(reference).map_err(|source| {
            CollectionDiscoveryError::Residency {
                reference,
                source: Box::new(source),
            }
        })?;
        if !resident {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Discover every stored equation whose collection belongs to one descriptor
/// lineage.
///
/// A foundational [`Cover`] has already crossed admission before this helper
/// is called, so signed commits are deliberately absent.  `MERGE` equations
/// are selected for every lattice in the lineage and `DERIVE` equations for
/// every derived target.  Resolution can then seed only the foundational
/// support and close the whole chain without treating an intermediate
/// physical cover as a new denotational root.
pub(crate) fn discover_collection_equations_for_lineage<S>(
    snapshot: &S,
    lineage: &BTreeSet<CollectionHandle>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
{
    let raw = discover_collection_equations_for_lineage_raw(snapshot, lineage)?;
    let mut discovered = DiscoveredCollectionRecords::default();
    for record in raw.merges {
        if record_references_are_resident(snapshot, CollectionRecord::Merge(record))? {
            discovered.merges.push(record);
        }
    }
    for record in raw.derives {
        if record_references_are_resident(snapshot, CollectionRecord::Derive(record))? {
            discovered.derives.push(record);
        }
    }
    discovered.canonicalize();
    Ok(discovered)
}

/// Discover the bounded raw equation frontier for active acquisition.
///
/// Unlike semantic discovery this intentionally preserves dangling records;
/// callers use their direct references to acquire exact immutable content,
/// then resnapshot before interpreting the equations.
pub(crate) fn discover_collection_equations_for_lineage_raw<S>(
    snapshot: &S,
    lineage: &BTreeSet<CollectionHandle>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: CollectionRead,
{
    let mut selectors = BTreeSet::new();
    for collection in lineage {
        // Whole-collection selection is the pile's indexed path. Selecting
        // MERGE and DERIVE separately would force a full record scan merely
        // to discard the same COMMITs below.
        selectors.insert(CollectionRecordSelector::Collection(*collection));
    }

    let mut discovered = DiscoveredCollectionRecords::default();
    for record in snapshot
        .select_records(&selectors)
        .map_err(CollectionDiscoveryError::Records)?
    {
        match record {
            CollectionRecord::Commit(_) => {}
            CollectionRecord::Merge(record) if lineage.contains(&record.collection()) => {
                discovered.merges.push(record);
            }
            CollectionRecord::Derive(record) if lineage.contains(&record.collection()) => {
                discovered.derives.push(record);
            }
            CollectionRecord::Merge(_) | CollectionRecord::Derive(_) => {}
        }
    }
    discovered.canonicalize();
    Ok(discovered)
}

/// Discover same-lattice equations that may physically realize one cover.
///
/// The cover is already an opaque constructed value, so this
/// path deliberately does not select or verify provenance commits.
pub(crate) fn discover_collection_equations_for_cover<S, L>(
    snapshot: &S,
    cover: &Cover<L>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
    L: CollectionEncoding,
{
    let selectors = BTreeSet::from([CollectionRecordSelector::MergeCollection(
        cover.collection().handle(),
    )]);
    discover_collection_records_for_cover_selectors(snapshot, cover, &selectors)
}

/// Discover every strictly signed provenance claim currently known for one
/// exact payload cover.
pub(crate) fn discover_collection_claims_for_cover<S, L>(
    snapshot: &S,
    cover: &Cover<L>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
    L: CollectionEncoding,
{
    let selectors: BTreeSet<_> = cover
        .data_members()
        .map(|member| CollectionRecordSelector::CommitMember(cover.collection().handle(), member))
        .collect();
    discover_collection_records_for_cover_selectors(snapshot, cover, &selectors)
}

fn discover_collection_records_for_cover_selectors<S, L>(
    snapshot: &S,
    cover: &Cover<L>,
    selectors: &BTreeSet<CollectionRecordSelector>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
    L: CollectionEncoding,
{
    let mut discovered = DiscoveredCollectionRecords::default();
    let mut matching_commits = Vec::new();
    let records = snapshot
        .select_records(selectors)
        .map_err(CollectionDiscoveryError::Records)?;

    for record in records {
        if !record_references_are_resident(snapshot, record)? {
            continue;
        }
        match record {
            CollectionRecord::Commit(record)
                if record.collection() == cover.collection().handle()
                    && cover.contains_data(record.data()) =>
            {
                matching_commits.push(record);
            }
            CollectionRecord::Commit(_) => {}
            CollectionRecord::Merge(record) => discovered.merges.push(record),
            CollectionRecord::Derive(record) => discovered.derives.push(record),
        }
    }

    let mut verifications =
        verify_matching_commits(&matching_commits, &CollectionCommit::verify_strict).into_iter();
    matching_commits.retain(|record| {
        match verifications
            .next()
            .expect("one verification result per cover claim")
        {
            Ok(()) => true,
            Err(error) => {
                discovered.diagnostics.push(CollectionRecordDiagnostic {
                    record: *record,
                    error: CollectionRecordDiagnosticError::InvalidCommit(error),
                });
                false
            }
        }
    });
    debug_assert!(verifications.next().is_none());
    discovered.commits = matching_commits;
    discovered.canonicalize();
    Ok(discovered)
}

/// Discover records for one exact collection and one exact supplied signer.
///
/// A commit's descriptor and public-key fields are structurally available
/// before its signature is verified. Commits outside this exact scope are
/// therefore discarded without paying for Ed25519 verification. Matching
/// commits still undergo the same strict verification and produce the same
/// diagnostics as [`discover_collection_records`]. With the `parallel`
/// feature, independent matching signatures are verified concurrently; the
/// returned records and diagnostics remain canonically ordered by intrinsic
/// canonical-record order rather than worker completion order.
///
/// `MERGE` and `DERIVE` records are retained in full. They are unsigned
/// equations whose relevance can span collection boundaries, so narrowing
/// them here would change the semantic graph seen by downstream resolution.
/// Physical decoding remains the store's responsibility: a malformed record
/// or iteration failure is fatal even when it appears outside the requested
/// commit scope.
pub fn discover_collection_records_scoped<S>(
    snapshot: &S,
    collection: CollectionHandle,
    signer: Inline<ED25519PublicKey>,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: CollectionRead,
{
    let mut discovered = DiscoveredCollectionRecords::default();
    let mut matching_commits = Vec::new();
    let records = snapshot
        .records()
        .map_err(CollectionDiscoveryError::Records)?;

    for record in records {
        let record = record.map_err(CollectionDiscoveryError::Records)?;
        match record {
            CollectionRecord::Commit(record)
                if record.collection() == collection && record.public_key() == signer =>
            {
                matching_commits.push(record)
            }
            CollectionRecord::Commit(_) => {}
            CollectionRecord::Merge(record) => discovered.merges.push(record),
            CollectionRecord::Derive(record) => discovered.derives.push(record),
        }
    }

    let verifying_key = VerifyingKey::from_bytes(&signer.raw);
    let mut verifications =
        verify_matching_commits(&matching_commits, &move |commit| match &verifying_key {
            Ok(verifying_key) => commit.verify_strict_with_key(verifying_key),
            Err(_) => Err(CommitVerificationError::InvalidPublicKey),
        })
        .into_iter();
    matching_commits.retain(|record| {
        let verification = verifications
            .next()
            .expect("one verification result per matching commit");
        match verification {
            Ok(()) => true,
            Err(error) => {
                discovered.diagnostics.push(CollectionRecordDiagnostic {
                    record: *record,
                    error: CollectionRecordDiagnosticError::InvalidCommit(error),
                });
                false
            }
        }
    });
    debug_assert!(verifications.next().is_none());
    discovered.commits = matching_commits;

    discovered.canonicalize();
    Ok(discovered)
}

/// Discover records for one exact collection across every caller-admitted signer.
///
/// This is the multi-author counterpart of
/// [`discover_collection_records_scoped`]. That function fixes one
/// caller-supplied signer; this one asks `is_member` for each claimed signer.
/// Both narrow to one exact collection, and both hand the result to the same
/// key-agnostic validator, so every admitted author follows one verification
/// path.
///
/// `is_member` sees a commit's *claimed* public key, which -- like the
/// descriptor and public-key fields [`discover_collection_records_scoped`]
/// filters on -- is structurally available before any signature is verified.
/// Narrowing first means a nonmember costs no Ed25519 verification, and
/// claiming membership falsely buys nothing: strict verification afterwards
/// binds that key to the signed bytes, so a forged claim fails there. The
/// predicate supplies the admission scope, while strict verification
/// establishes that the claimed signer actually authored the commit. Ordinary
/// Higher-level collection reads pass either their explicitly verified
/// capability subjects or an open predicate here. This lower-level primitive
/// accepts a callback so protocols can supply any already-decided signer set
/// without coupling discovery to how that decision was made.
///
/// `MERGE` and `DERIVE` records are retained in full, for the same reason
/// [`discover_collection_records_scoped`] retains them: they are unsigned
/// equations whose relevance can span collection boundaries.
pub(crate) fn discover_collection_records_authorized<S, F>(
    snapshot: &S,
    collection: CollectionHandle,
    mut is_member: F,
) -> Result<DiscoveredCollectionRecords, CollectionDiscoveryError<S::RecordsError>>
where
    S: BlobStoreList + CollectionRead,
    F: FnMut(&Inline<ED25519PublicKey>) -> bool,
{
    let mut discovered = DiscoveredCollectionRecords::default();
    let mut matching_commits = Vec::new();
    let records = snapshot
        .records()
        .map_err(CollectionDiscoveryError::Records)?;

    for record in records {
        let record = record.map_err(CollectionDiscoveryError::Records)?;
        if !record_references_are_resident(snapshot, record)? {
            continue;
        }
        match record {
            CollectionRecord::Commit(record)
                if record.collection() == collection && is_member(&record.public_key()) =>
            {
                matching_commits.push(record)
            }
            CollectionRecord::Commit(_) => {}
            CollectionRecord::Merge(record) => discovered.merges.push(record),
            CollectionRecord::Derive(record) => discovered.derives.push(record),
        }
    }

    // Each commit carries its own signer, so verification derives the key from
    // the record rather than from a fixed scope key.
    let mut verifications =
        verify_matching_commits(&matching_commits, &CollectionCommit::verify_strict).into_iter();
    matching_commits.retain(|record| {
        match verifications
            .next()
            .expect("one verification result per matching commit")
        {
            Ok(()) => true,
            Err(error) => {
                discovered.diagnostics.push(CollectionRecordDiagnostic {
                    record: *record,
                    error: CollectionRecordDiagnosticError::InvalidCommit(error),
                });
                false
            }
        }
    });
    debug_assert!(verifications.next().is_none());
    discovered.commits = matching_commits;

    discovered.canonicalize();
    Ok(discovered)
}

#[cfg(feature = "parallel")]
fn verify_matching_commits<V>(
    commits: &[CollectionCommit],
    verify_commit: &V,
) -> Vec<Result<(), CommitVerificationError>>
where
    V: Fn(&CollectionCommit) -> Result<(), CommitVerificationError> + Sync,
{
    use rayon::prelude::*;

    // There is no independent work to schedule for an empty or singleton
    // scope. Besides being the structural boundary of parallelism, this keeps
    // a first singleton read from paying global Rayon-pool initialization.
    if commits.len() < 2 {
        return commits.iter().map(verify_commit).collect();
    }

    // `par_iter` is indexed, so `collect` restores input order even though
    // workers finish out of order. The caller can therefore attribute every
    // diagnostic to the exact record without making scheduling observable.
    commits.par_iter().map(verify_commit).collect()
}

#[cfg(not(feature = "parallel"))]
fn verify_matching_commits<V>(
    commits: &[CollectionCommit],
    verify_commit: &V,
) -> Vec<Result<(), CommitVerificationError>>
where
    V: Fn(&CollectionCommit) -> Result<(), CommitVerificationError> + Sync,
{
    commits.iter().map(verify_commit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use anybytes::Bytes;
    use ed25519_dalek::SigningKey;

    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::blob::encodings::UnknownBlob;
    use crate::blob::Blob;
    use crate::collection::{
        empty_metadata_handle, AdmissionPolicy, Collection, CollectionData, CollectionPolicy,
        CollectionStore, CollectionStoreExt, Support,
    };
    use crate::inline::encodings::hash::Handle;
    use crate::inline::Inline;
    use crate::repo::memoryrepo::MemoryRepo;
    use crate::repo::{BlobStorePut, SnapshotSource};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ProbeRecordsError(&'static str);

    impl fmt::Display for ProbeRecordsError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for ProbeRecordsError {}

    #[derive(Default)]
    struct ProbeStore {
        start_error: Option<ProbeRecordsError>,
        records: Vec<Result<CollectionRecord, ProbeRecordsError>>,
    }

    impl CollectionRead for ProbeStore {
        type RecordsError = ProbeRecordsError;
        type RecordIter<'a> = std::vec::IntoIter<Result<CollectionRecord, Self::RecordsError>>;

        fn records<'a>(&'a self) -> Result<Self::RecordIter<'a>, Self::RecordsError> {
            if let Some(error) = self.start_error {
                return Err(error);
            }
            Ok(self.records.clone().into_iter())
        }
    }

    fn data(byte: u8) -> CollectionData {
        Inline::new([byte; 32])
    }

    fn member(byte: u8) -> Inline<Handle<SimpleArchive>> {
        Inline::new([byte; 32])
    }

    fn collection(byte: u8) -> Collection<SimpleArchive> {
        Collection::from_handle(Inline::new([byte; 32]))
    }

    fn signer(key: &SigningKey) -> Inline<ED25519PublicKey> {
        Inline::new(key.verifying_key().to_bytes())
    }

    fn invalid_signature(commit: CollectionCommit) -> CollectionCommit {
        let (r, mut s) = commit.signature();
        s.raw[0] ^= 1;
        CollectionCommit::from_parts(
            commit.collection(),
            commit.data(),
            commit.metadata(),
            commit.public_key(),
            r,
            s,
        )
    }

    #[test]
    fn cover_additions_are_payload_set_difference() {
        let target = collection(1);
        let previous = Support::from_members(target, [member(2), member(1), member(1)]);
        let current = Support::from_members(target, [member(3), member(1), member(2), member(3)]);

        let additions = current.additions_since(&previous).unwrap();
        assert_eq!(additions.members().collect::<Vec<_>>(), vec![member(3)]);
        assert!(current.additions_since(&current).unwrap().is_empty());
    }

    #[test]
    fn cover_additions_reject_shrink_and_cross_collection() {
        let first = Support::from_members(collection(1), [member(1)]);
        let empty = Support::from_members(collection(1), []);
        assert_eq!(
            empty.additions_since(&first),
            Err(crate::collection::CoverAdvanceError::ResetRequired { missing: data(1) })
        );

        let foreign = Support::from_members(collection(2), [member(1)]);
        assert_eq!(
            foreign.additions_since(&first),
            Err(crate::collection::CoverAdvanceError::DifferentCollection {
                previous: collection(1).handle(),
                current: collection(2).handle(),
            })
        );
    }

    #[test]
    fn cover_algebra_is_checked_and_patch_backed() {
        let target = collection(1);
        let left = Support::from_members(target, [member(1), member(2)]);
        let right = Support::from_members(target, [member(2), member(3)]);

        assert_eq!(
            left.union(&right).unwrap().members().collect::<Vec<_>>(),
            vec![member(1), member(2), member(3)],
        );
        assert_eq!(
            left.intersection(&right)
                .unwrap()
                .members()
                .collect::<Vec<_>>(),
            vec![member(2)],
        );
        assert_eq!(
            left.difference(&right)
                .unwrap()
                .members()
                .collect::<Vec<_>>(),
            vec![member(1)],
        );
        assert!(left.intersection(&right).unwrap().is_subset(&left).unwrap());
        assert!(!left.is_subset(&right).unwrap());

        let foreign = Support::from_members(collection(2), [member(1)]);
        assert_eq!(
            left.union(&foreign),
            Err(crate::collection::CoverAlgebraError::DifferentCollection {
                left: collection(1).handle(),
                right: collection(2).handle(),
            }),
        );
        assert!(left.intersection(&foreign).is_err());
        assert!(left.difference(&foreign).is_err());
        assert!(left.is_subset(&foreign).is_err());
    }

    #[test]
    fn dangling_equations_are_raw_but_semantically_visible_only_later() {
        let mut store = MemoryRepo::default();
        let target = store
            .collection(
                "dangling-equations",
                CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open),
            )
            .unwrap();
        let low = store
            .put::<UnknownBlob, _>(Bytes::from_source(b"low".to_vec()))
            .unwrap();
        let high = store
            .put::<UnknownBlob, _>(Bytes::from_source(b"high".to_vec()))
            .unwrap();
        let output = Bytes::from_source(b"output".to_vec());
        let output_handle = Blob::<UnknownBlob>::new(output.clone()).get_handle();
        let merge = CollectionRecord::Merge(CollectionMerge::new(
            target.handle(),
            Handle::<UnknownBlob>::to_hash(low),
            Handle::<UnknownBlob>::to_hash(high),
            Handle::<UnknownBlob>::to_hash(output_handle),
        ));
        let derive = CollectionRecord::Derive(CollectionDerive::new(
            target.handle(),
            Handle::<UnknownBlob>::to_hash(low),
            Handle::<UnknownBlob>::to_hash(output_handle),
        ));
        store.insert(merge).unwrap();
        store.insert(derive).unwrap();
        let lineage = BTreeSet::from([target.handle()]);

        let before = store.snapshot().unwrap();
        let raw = discover_collection_equations_for_lineage_raw(&before, &lineage).unwrap();
        assert_eq!(raw.merges().len(), 1);
        assert_eq!(raw.derives().len(), 1);
        let semantic = discover_collection_equations_for_lineage(&before, &lineage).unwrap();
        assert!(semantic.merges().is_empty());
        assert!(semantic.derives().is_empty());

        store.put::<UnknownBlob, _>(output).unwrap();
        let after = store.snapshot().unwrap();
        let semantic = discover_collection_equations_for_lineage(&after, &lineage).unwrap();
        assert_eq!(semantic.merges().len(), 1);
        assert_eq!(semantic.derives().len(), 1);
        let still_before = discover_collection_equations_for_lineage(&before, &lineage).unwrap();
        assert!(still_before.merges().is_empty());
        assert!(still_before.derives().is_empty());
    }

    fn fixture_records() -> (Vec<CollectionRecord>, CollectionCommit) {
        let commit = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            collection(1).handle(),
            data(4),
            empty_metadata_handle(),
        );
        let merge = CollectionMerge::new(collection(1).handle(), data(4), data(5), data(6));
        let derive = CollectionDerive::new(collection(7).handle(), data(4), data(8));

        let invalid_commit = invalid_signature(commit);

        (
            vec![
                CollectionRecord::Commit(commit),
                CollectionRecord::Merge(merge),
                CollectionRecord::Derive(derive),
                CollectionRecord::Commit(invalid_commit),
            ],
            invalid_commit,
        )
    }

    #[test]
    fn lineage_discovery_selects_only_equations_in_the_descriptor_lineage() {
        let source = collection(1);
        let target = collection(2);
        let other = collection(3);
        let commit = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            source.handle(),
            data(4),
            empty_metadata_handle(),
        );
        let source_merge = CollectionMerge::new(source.handle(), data(4), data(5), data(6));
        let target_merge = CollectionMerge::new(target.handle(), data(7), data(8), data(9));
        let derive = CollectionDerive::new(target.handle(), data(6), data(7));
        let unrelated_merge = CollectionMerge::new(other.handle(), data(10), data(11), data(12));
        let unrelated_derive = CollectionDerive::new(other.handle(), data(6), data(13));
        let unrelated_commit = invalid_signature(CollectionCommit::sign(
            &SigningKey::from_bytes(&[8; 32]),
            other.handle(),
            data(14),
            empty_metadata_handle(),
        ));
        let mut physical = vec![
            CollectionRecord::Merge(unrelated_merge),
            CollectionRecord::Derive(unrelated_derive),
            CollectionRecord::Commit(unrelated_commit),
            CollectionRecord::Merge(target_merge),
            CollectionRecord::Commit(commit),
            CollectionRecord::Derive(derive),
            CollectionRecord::Merge(source_merge),
        ];
        physical.sort_unstable();
        let store = ProbeStore {
            records: physical.into_iter().map(Ok).collect(),
            ..ProbeStore::default()
        };

        let lineage = BTreeSet::from([source.handle(), target.handle()]);
        let discovered = discover_collection_equations_for_lineage_raw(&store, &lineage).unwrap();

        let mut expected_merges = vec![source_merge, target_merge];
        expected_merges.sort_unstable();
        assert!(discovered.commits().is_empty());
        assert_eq!(discovered.merges(), expected_merges);
        assert_eq!(discovered.derives(), &[derive]);
        assert!(discovered.diagnostics().is_empty());
    }

    #[test]
    fn native_records_are_verified_classified_and_order_independent() {
        let (records, invalid_commit) = fixture_records();
        let forward = ProbeStore {
            records: records.iter().copied().map(Ok).collect(),
            ..ProbeStore::default()
        };
        let reverse = ProbeStore {
            records: records.iter().rev().copied().map(Ok).collect(),
            ..ProbeStore::default()
        };

        let forward_records = discover_collection_records(&forward).unwrap();
        let reverse_records = discover_collection_records(&reverse).unwrap();

        assert_eq!(forward_records, reverse_records);
        assert_eq!(forward_records.commits().len(), 1);
        assert_eq!(forward_records.merges().len(), 1);
        assert_eq!(forward_records.derives().len(), 1);
        assert_eq!(
            forward_records.diagnostics(),
            &[CollectionRecordDiagnostic {
                record: invalid_commit,
                error: CollectionRecordDiagnosticError::InvalidCommit(
                    CommitVerificationError::InvalidSignature,
                ),
            }]
        );
    }

    #[test]
    fn scoped_discovery_ignores_unrelated_invalid_signatures_and_rejects_relevant_ones() {
        let target = collection(1);
        let other = collection(2);
        let authorized_key = SigningKey::from_bytes(&[7; 32]);
        let foreign_key = SigningKey::from_bytes(&[8; 32]);
        let valid = CollectionCommit::sign(
            &authorized_key,
            target.handle(),
            data(1),
            empty_metadata_handle(),
        );
        let relevant_invalid = invalid_signature(CollectionCommit::sign(
            &authorized_key,
            target.handle(),
            data(2),
            empty_metadata_handle(),
        ));
        let wrong_collection = invalid_signature(CollectionCommit::sign(
            &authorized_key,
            other.handle(),
            data(3),
            empty_metadata_handle(),
        ));
        let wrong_signer = invalid_signature(CollectionCommit::sign(
            &foreign_key,
            target.handle(),
            data(4),
            empty_metadata_handle(),
        ));
        let target_merge = CollectionMerge::new(target.handle(), data(1), data(2), data(5));
        let other_merge = CollectionMerge::new(other.handle(), data(3), data(4), data(6));
        let crossing_derive = CollectionDerive::new(other.handle(), data(5), data(6));

        let store = ProbeStore {
            records: [
                CollectionRecord::Commit(wrong_signer),
                CollectionRecord::Merge(other_merge),
                CollectionRecord::Commit(relevant_invalid),
                CollectionRecord::Derive(crossing_derive),
                CollectionRecord::Commit(valid),
                CollectionRecord::Commit(wrong_collection),
                CollectionRecord::Merge(target_merge),
            ]
            .into_iter()
            .map(Ok)
            .collect(),
            ..ProbeStore::default()
        };

        let discovered =
            discover_collection_records_scoped(&store, target.handle(), signer(&authorized_key))
                .unwrap();

        assert_eq!(discovered.commits(), &[valid]);
        assert_eq!(
            discovered.diagnostics(),
            &[CollectionRecordDiagnostic {
                record: relevant_invalid,
                error: CollectionRecordDiagnosticError::InvalidCommit(
                    CommitVerificationError::InvalidSignature,
                ),
            }]
        );
        assert_eq!(discovered.merges().len(), 2);
        assert!(discovered.merges().contains(&target_merge));
        assert!(discovered.merges().contains(&other_merge));
        assert_eq!(discovered.derives(), &[crossing_derive]);
    }

    #[test]
    fn scoped_discovery_attributes_an_invalid_expected_public_key_to_each_match() {
        let target = collection(1);
        let mut invalid_signer_bytes = [0; 32];
        invalid_signer_bytes[0] = 2;
        let invalid_signer = Inline::new(invalid_signer_bytes);
        assert!(VerifyingKey::from_bytes(&invalid_signer.raw).is_err());

        let template = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            target.handle(),
            data(1),
            empty_metadata_handle(),
        );
        let (r, s) = template.signature();
        let invalid_key_commit = CollectionCommit::from_parts(
            target.handle(),
            template.data(),
            template.metadata(),
            invalid_signer,
            r,
            s,
        );
        let store = ProbeStore {
            records: vec![Ok(CollectionRecord::Commit(invalid_key_commit))],
            ..ProbeStore::default()
        };

        let discovered =
            discover_collection_records_scoped(&store, target.handle(), invalid_signer).unwrap();
        assert!(discovered.commits().is_empty());
        assert_eq!(
            discovered.diagnostics(),
            &[CollectionRecordDiagnostic {
                record: invalid_key_commit,
                error: CollectionRecordDiagnosticError::InvalidCommit(
                    CommitVerificationError::InvalidPublicKey,
                ),
            }]
        );
    }

    #[test]
    fn scoped_discovery_equals_full_discovery_projected_to_valid_matching_commits() {
        let target = collection(1);
        let other = collection(2);
        let authorized_key = SigningKey::from_bytes(&[7; 32]);
        let foreign_key = SigningKey::from_bytes(&[8; 32]);
        let matching = [
            CollectionCommit::sign(
                &authorized_key,
                target.handle(),
                data(1),
                empty_metadata_handle(),
            ),
            CollectionCommit::sign(
                &authorized_key,
                target.handle(),
                data(2),
                empty_metadata_handle(),
            ),
        ];
        let records = vec![
            CollectionRecord::Commit(CollectionCommit::sign(
                &foreign_key,
                target.handle(),
                data(3),
                empty_metadata_handle(),
            )),
            CollectionRecord::Merge(CollectionMerge::new(
                other.handle(),
                data(3),
                data(4),
                data(5),
            )),
            CollectionRecord::Commit(matching[1]),
            CollectionRecord::Derive(CollectionDerive::new(target.handle(), data(5), data(2))),
            CollectionRecord::Commit(CollectionCommit::sign(
                &authorized_key,
                other.handle(),
                data(4),
                empty_metadata_handle(),
            )),
            CollectionRecord::Commit(matching[0]),
            CollectionRecord::Merge(CollectionMerge::new(
                target.handle(),
                data(1),
                data(2),
                data(6),
            )),
        ];
        let full_store = ProbeStore {
            records: records.iter().copied().map(Ok).collect(),
            ..ProbeStore::default()
        };
        let scoped_store = ProbeStore {
            records: records.into_iter().map(Ok).collect(),
            ..ProbeStore::default()
        };

        let full = discover_collection_records(&full_store).unwrap();
        let scoped = discover_collection_records_scoped(
            &scoped_store,
            target.handle(),
            signer(&authorized_key),
        )
        .unwrap();
        let expected_commits: Vec<_> = full
            .commits()
            .iter()
            .copied()
            .filter(|commit| {
                commit.collection() == target.handle()
                    && commit.public_key() == signer(&authorized_key)
            })
            .collect();

        assert_eq!(scoped.commits(), expected_commits);
        assert_eq!(scoped.merges(), full.merges());
        assert_eq!(scoped.derives(), full.derives());
        assert_eq!(scoped.diagnostics(), full.diagnostics());
        assert_eq!(expected_commits.len(), matching.len());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn parallel_verification_preserves_canonical_diagnostic_attribution() {
        let target = collection(1);
        let authorized_key = SigningKey::from_bytes(&[7; 32]);
        let mut records: Vec<_> = (0..64)
            .map(|byte| {
                let commit = CollectionCommit::sign(
                    &authorized_key,
                    target.handle(),
                    data(byte),
                    empty_metadata_handle(),
                );
                CollectionRecord::Commit(if byte % 3 == 0 {
                    invalid_signature(commit)
                } else {
                    commit
                })
            })
            .collect();
        records.reverse();
        // Duplicate physical evidence is canonicalized by exact record after
        // verification, independent of worker completion order.
        records.push(records[7]);

        let discover_with = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| {
                let store = ProbeStore {
                    records: records.iter().copied().map(Ok).collect(),
                    ..ProbeStore::default()
                };
                discover_collection_records_scoped(&store, target.handle(), signer(&authorized_key))
                    .unwrap()
            })
        };

        let serial_schedule = discover_with(1);
        let parallel_schedule = discover_with(4);
        assert_eq!(parallel_schedule, serial_schedule);
        assert_eq!(parallel_schedule.commits().len(), 42);
        assert_eq!(parallel_schedule.diagnostics().len(), 22);
        assert!(parallel_schedule
            .commits()
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        assert!(parallel_schedule
            .diagnostics()
            .windows(2)
            .all(|pair| pair[0].record < pair[1].record));
        assert!(parallel_schedule.diagnostics().iter().all(|diagnostic| {
            diagnostic.error
                == CollectionRecordDiagnosticError::InvalidCommit(
                    CommitVerificationError::InvalidSignature,
                )
        }));
    }

    #[test]
    fn start_and_iteration_failures_are_fatal() {
        let start_failure = ProbeStore {
            start_error: Some(ProbeRecordsError("start")),
            ..ProbeStore::default()
        };
        assert!(matches!(
            discover_collection_records(&start_failure),
            Err(CollectionDiscoveryError::Records(ProbeRecordsError(
                "start"
            )))
        ));

        let (records, _) = fixture_records();
        let iteration_failure = ProbeStore {
            records: vec![Ok(records[0]), Err(ProbeRecordsError("iteration"))],
            ..ProbeStore::default()
        };
        assert!(matches!(
            discover_collection_records(&iteration_failure),
            Err(CollectionDiscoveryError::Records(ProbeRecordsError(
                "iteration"
            )))
        ));

        let scoped_failure = ProbeStore {
            records: vec![Ok(records[0]), Err(ProbeRecordsError("scoped iteration"))],
            ..ProbeStore::default()
        };
        assert!(matches!(
            discover_collection_records_scoped(
                &scoped_failure,
                collection(99).handle(),
                signer(&SigningKey::from_bytes(&[9; 32])),
            ),
            Err(CollectionDiscoveryError::Records(ProbeRecordsError(
                "scoped iteration"
            )))
        ));
    }
}
