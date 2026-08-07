//! Read-side materialization for one resolved `SimpleArchive` union collection.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::Blob;
use crate::inline::encodings::hash::Handle;
use crate::repo::{BlobStoreGet, BlobStoreMeta};
use crate::trible::TribleSet;

use super::{validate_definition, SimpleArchiveUnionValidationError};
use crate::collection::{
    collection_physical_cover, CollectionData, CollectionDefinition, CollectionSemantics,
};

/// Failure to materialize one resolved `SimpleArchive` union collection.
#[derive(Debug)]
pub enum MaterializationError<MetadataError, GetError> {
    /// The supplied definition does not name this representation and recipe.
    Definition(SimpleArchiveUnionValidationError),
    /// Residency lookup failed for one semantic member.
    Metadata {
        /// Member whose residency could not be determined.
        data: CollectionData,
        /// Backend metadata failure.
        source: MetadataError,
    },
    /// No resident merge proof covers these maximal semantic obligations.
    Missing {
        /// Uncovered members of the collection's semantic frontier.
        obligations: BTreeSet<CollectionData>,
    },
    /// A member selected by the physical cover could not be fetched.
    Get {
        /// Selected resident member.
        data: CollectionData,
        /// Backend fetch failure.
        source: GetError,
    },
    /// A fetched member was not a canonical `SimpleArchive`.
    InvalidElement {
        /// Selected resident member.
        data: CollectionData,
        /// Canonical archive failure.
        source: UnarchiveError,
    },
}

impl<MetadataError, GetError> fmt::Display for MaterializationError<MetadataError, GetError>
where
    MetadataError: fmt::Display,
    GetError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Definition(source) => write!(f, "invalid collection definition: {source}"),
            Self::Metadata { data, source } => write!(
                f,
                "failed to inspect collection member {}: {source}",
                hex::encode_upper(data.raw),
            ),
            Self::Missing { obligations } => write!(
                f,
                "{} semantic frontier obligation(s) have no resident physical cover",
                obligations.len(),
            ),
            Self::Get { data, source } => write!(
                f,
                "failed to fetch collection member {}: {source}",
                hex::encode_upper(data.raw),
            ),
            Self::InvalidElement { data, source } => write!(
                f,
                "collection member {} is not a canonical SimpleArchive: {source}",
                hex::encode_upper(data.raw),
            ),
        }
    }
}

impl<MetadataError, GetError> Error for MaterializationError<MetadataError, GetError>
where
    MetadataError: Error + 'static,
    GetError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Definition(source) => Some(source),
            Self::Metadata { source, .. } => Some(source),
            Self::Missing { .. } => None,
            Self::Get { source, .. } => Some(source),
            Self::InvalidElement { source, .. } => Some(source),
        }
    }
}

/// Materialize the complete known value of one resolved collection.
///
/// This function inspects only members already present in `semantics`; it does
/// not enumerate the blob store. Metadata lookups determine current residency,
/// then [`collection_physical_cover`] selects a deterministic overlap-aware
/// cover. A resident compacted result may replace its nonresident inputs, while
/// resident exact inputs may reconstruct a nonresident result. If neither is
/// possible, all uncovered semantic-frontier obligations are returned before
/// any blob is fetched.
///
/// The selected `SimpleArchive` blobs are fetched and unioned in content-hash
/// order. Authorization, claim validation, and resolution status belong to the
/// caller that produced `semantics`; pending or rejected equations do not enter
/// this boundary.
pub fn materialize<R>(
    semantics: &CollectionSemantics,
    definition: &CollectionDefinition,
    reader: &R,
) -> Result<
    TribleSet,
    MaterializationError<
        <R as BlobStoreMeta>::MetaError,
        <R as BlobStoreGet>::GetError<Infallible>,
    >,
>
where
    R: BlobStoreMeta + BlobStoreGet + ?Sized,
{
    validate_definition(definition).map_err(MaterializationError::Definition)?;

    let collection = definition.id();
    let mut resident = BTreeSet::new();
    for data in semantics.members(collection).into_iter().flatten().copied() {
        let handle = Handle::<SimpleArchive>::from_hash(data);
        if reader
            .metadata(handle)
            .map_err(|source| MaterializationError::Metadata { data, source })?
            .is_some()
        {
            resident.insert(data);
        }
    }

    let cover = collection_physical_cover(semantics, collection, &resident);
    if !cover.missing.is_empty() {
        return Err(MaterializationError::Missing {
            obligations: cover.missing,
        });
    }

    let mut facts = TribleSet::new();
    for data in cover.cover {
        let handle = Handle::<SimpleArchive>::from_hash(data);
        let blob: Blob<SimpleArchive> = reader
            .get(handle)
            .map_err(|source| MaterializationError::Get { data, source })?;
        let member: TribleSet = blob
            .try_from_blob()
            .map_err(|source| MaterializationError::InvalidElement { data, source })?;
        facts += member;
    }
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::collections::BTreeMap;

    use ed25519_dalek::SigningKey;

    use crate::blob::encodings::UnknownBlob;
    use crate::blob::{BlobEncoding, IntoBlob, MemoryBlobStore, TryFromBlob};
    use crate::collection::{
        discover_collection_records, resolve_collection_semantics, CollectionClaimValidation,
        CollectionCommit, CollectionMerge, CollectionValidationRequest,
    };
    use crate::id::Id;
    use crate::inline::Inline;
    use crate::inline::InlineEncoding;
    use crate::repo::{BlobMetadata, BlobStore, BlobStoreGet, BlobStoreMeta};
    use crate::trible::{Trible, TRIBLE_LEN};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ProbeMetadataError {
        Injected,
    }

    impl fmt::Display for ProbeMetadataError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "injected metadata failure")
        }
    }

    impl Error for ProbeMetadataError {}

    #[derive(Debug)]
    enum ProbeGetError<E: Error> {
        Injected,
        Missing,
        Conversion(E),
    }

    impl<E: Error> fmt::Display for ProbeGetError<E> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Injected => write!(f, "injected get failure"),
                Self::Missing => write!(f, "missing blob"),
                Self::Conversion(source) => fmt::Display::fmt(source, f),
            }
        }
    }

    impl<E: Error + 'static> Error for ProbeGetError<E> {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                Self::Conversion(source) => Some(source),
                _ => None,
            }
        }
    }

    #[derive(Default)]
    struct ProbeReader {
        blobs: BTreeMap<[u8; 32], Blob<UnknownBlob>>,
        metadata_failures: BTreeSet<[u8; 32]>,
        get_failures: BTreeSet<[u8; 32]>,
        metadata_calls: RefCell<Vec<[u8; 32]>>,
        get_calls: RefCell<Vec<[u8; 32]>>,
    }

    impl ProbeReader {
        fn insert(&mut self, blob: Blob<SimpleArchive>) {
            self.blobs
                .insert(blob.get_handle().raw, blob.transmute::<UnknownBlob>());
        }
    }

    impl BlobStoreMeta for ProbeReader {
        type MetaError = ProbeMetadataError;

        fn metadata<S>(
            &self,
            handle: Inline<Handle<S>>,
        ) -> Result<Option<BlobMetadata>, Self::MetaError>
        where
            S: BlobEncoding + 'static,
            Handle<S>: InlineEncoding,
        {
            self.metadata_calls.borrow_mut().push(handle.raw);
            if self.metadata_failures.contains(&handle.raw) {
                return Err(ProbeMetadataError::Injected);
            }
            Ok(self.blobs.get(&handle.raw).map(|blob| BlobMetadata {
                timestamp: 0,
                length: blob.bytes.len() as u64,
            }))
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
            self.get_calls.borrow_mut().push(handle.raw);
            if self.get_failures.contains(&handle.raw) {
                return Err(ProbeGetError::Injected);
            }
            let blob = self
                .blobs
                .get(&handle.raw)
                .cloned()
                .ok_or(ProbeGetError::Missing)?
                .transmute::<S>();
            blob.try_from_blob().map_err(ProbeGetError::Conversion)
        }
    }

    fn id(byte: u8) -> Id {
        Id::new([byte; 16]).unwrap()
    }

    fn row(entity: u8, attribute: u8, value: u8) -> [u8; TRIBLE_LEN] {
        let mut row = [value; TRIBLE_LEN];
        row[..16].fill(entity);
        row[16..32].fill(attribute);
        row
    }

    fn archive(rows: impl IntoIterator<Item = [u8; TRIBLE_LEN]>) -> Blob<SimpleArchive> {
        let mut facts = TribleSet::new();
        for row in rows {
            facts.insert(&Trible::force_raw(row).unwrap());
        }
        facts.to_blob()
    }

    fn data(blob: &Blob<SimpleArchive>) -> CollectionData {
        Handle::<SimpleArchive>::to_hash(blob.get_handle())
    }

    fn semantics(
        definition: &CollectionDefinition,
        roots: &[Blob<SimpleArchive>],
        merges: &[CollectionMerge],
    ) -> CollectionSemantics {
        let commits: Vec<_> = roots
            .iter()
            .enumerate()
            .map(|(index, blob)| {
                CollectionCommit::sign(
                    &SigningKey::from_bytes(&[(index + 1) as u8; 32]),
                    definition.id(),
                    data(blob),
                    crate::collection::empty_metadata_handle(),
                )
            })
            .collect();

        let mut records = MemoryBlobStore::new();
        records.insert(definition.to_blob());
        for commit in &commits {
            records.insert(commit.to_blob());
        }
        for merge in merges {
            records.insert(merge.to_blob());
        }
        let reader = records.reader().unwrap();
        let discovered = discover_collection_records(&reader).unwrap();
        let authorized = commits.iter().map(CollectionCommit::id).collect();
        resolve_collection_semantics(
            &discovered,
            &authorized,
            |_: CollectionValidationRequest<'_>| {
                Ok::<_, Infallible>(CollectionClaimValidation::<()>::Accepted)
            },
        )
        .unwrap()
        .into_semantics()
    }

    fn decode(blob: Blob<SimpleArchive>) -> TribleSet {
        blob.try_from_blob().unwrap()
    }

    #[test]
    fn empty_collection_materializes_without_store_access() {
        let definition = super::super::definition(id(1));
        let reader = ProbeReader::default();

        assert_eq!(
            materialize(&CollectionSemantics::default(), &definition, &reader).unwrap(),
            TribleSet::new(),
        );
        assert!(reader.metadata_calls.borrow().is_empty());
        assert!(reader.get_calls.borrow().is_empty());
    }

    #[test]
    fn direct_resident_leaves_materialize_in_deterministic_handle_order() {
        let definition = super::super::definition(id(1));
        let left = archive([row(1, 1, 1)]);
        let right = archive([row(2, 1, 2)]);
        let semantics = semantics(&definition, &[left.clone(), right.clone()], &[]);
        let mut reader = ProbeReader::default();
        reader.insert(right.clone());
        reader.insert(left.clone());

        let actual = materialize(&semantics, &definition, &reader).unwrap();
        let expected = decode(super::super::join(&left, &right).unwrap());
        assert_eq!(actual, expected);

        let expected_order: Vec<_> =
            BTreeSet::from([left.get_handle().raw, right.get_handle().raw])
                .into_iter()
                .collect();
        assert_eq!(*reader.get_calls.borrow(), expected_order);
    }

    #[test]
    fn resident_compacted_result_replaces_nonresident_inputs() {
        let definition = super::super::definition(id(1));
        let left = archive([row(1, 1, 1)]);
        let right = archive([row(2, 1, 2)]);
        let result = super::super::join(&left, &right).unwrap();
        let merge = CollectionMerge::new(definition.id(), data(&left), data(&right), data(&result));
        let semantics = semantics(&definition, &[left, right], &[merge]);
        let mut reader = ProbeReader::default();
        reader.insert(result.clone());

        assert_eq!(
            materialize(&semantics, &definition, &reader).unwrap(),
            decode(result.clone())
        );
        assert_eq!(*reader.get_calls.borrow(), vec![result.get_handle().raw]);
    }

    #[test]
    fn overlapping_upper_cover_is_fetched_only_once() {
        let definition = super::super::definition(id(1));
        let a = archive([row(1, 1, 1)]);
        let b = archive([row(2, 1, 2)]);
        let c = archive([row(3, 1, 3)]);
        let d = archive([row(4, 1, 4)]);
        let ab = super::super::join(&a, &b).unwrap();
        let bc = super::super::join(&b, &c).unwrap();
        let bcd = super::super::join(&bc, &d).unwrap();
        let merges = [
            CollectionMerge::new(definition.id(), data(&a), data(&b), data(&ab)),
            CollectionMerge::new(definition.id(), data(&b), data(&c), data(&bc)),
            CollectionMerge::new(definition.id(), data(&bc), data(&d), data(&bcd)),
        ];
        let semantics = semantics(&definition, &[a.clone(), b, c, d], &merges);
        let mut reader = ProbeReader::default();
        reader.insert(a.clone());
        reader.insert(bcd.clone());

        let actual = materialize(&semantics, &definition, &reader).unwrap();
        let expected = decode(super::super::join(&a, &bcd).unwrap());
        assert_eq!(actual, expected);
        let mut calls = reader.get_calls.borrow().clone();
        calls.sort_unstable();
        let mut expected_calls = vec![a.get_handle().raw, bcd.get_handle().raw];
        expected_calls.sort_unstable();
        assert_eq!(calls, expected_calls);
    }

    #[test]
    fn missing_frontier_is_reported_before_fetching() {
        let definition = super::super::definition(id(1));
        let left = archive([row(1, 1, 1)]);
        let right = archive([row(2, 1, 2)]);
        let result = super::super::join(&left, &right).unwrap();
        let merge = CollectionMerge::new(definition.id(), data(&left), data(&right), data(&result));
        let semantics = semantics(&definition, &[left.clone(), right], &[merge]);
        let mut reader = ProbeReader::default();
        reader.insert(left);

        assert!(matches!(
            materialize(&semantics, &definition, &reader),
            Err(MaterializationError::Missing { obligations })
                if obligations == BTreeSet::from([data(&result)])
        ));
        assert!(reader.get_calls.borrow().is_empty());
    }

    #[test]
    fn wrong_definition_fails_before_store_access() {
        let wrong =
            CollectionDefinition::new(id(1), id(2), super::super::TRIBLE_SET_UNION_RECIPE_V1);
        let reader = ProbeReader::default();

        assert!(matches!(
            materialize(&CollectionSemantics::default(), &wrong, &reader),
            Err(MaterializationError::Definition(
                SimpleArchiveUnionValidationError::WrongRepresentation { .. }
            ))
        ));
        assert!(reader.metadata_calls.borrow().is_empty());
        assert!(reader.get_calls.borrow().is_empty());
    }

    #[test]
    fn metadata_and_get_failures_remain_distinct() {
        let definition = super::super::definition(id(1));
        let leaf = archive([row(1, 1, 1)]);
        let semantics = semantics(&definition, std::slice::from_ref(&leaf), &[]);

        let mut metadata_failure = ProbeReader::default();
        metadata_failure.insert(leaf.clone());
        metadata_failure
            .metadata_failures
            .insert(leaf.get_handle().raw);
        assert!(matches!(
            materialize(&semantics, &definition, &metadata_failure),
            Err(MaterializationError::Metadata {
                data: failed,
                source: ProbeMetadataError::Injected,
            }) if failed == data(&leaf)
        ));

        let mut get_failure = ProbeReader::default();
        get_failure.insert(leaf.clone());
        get_failure.get_failures.insert(leaf.get_handle().raw);
        assert!(matches!(
            materialize(&semantics, &definition, &get_failure),
            Err(MaterializationError::Get {
                data: failed,
                source: ProbeGetError::Injected,
            }) if failed == data(&leaf)
        ));
    }

    #[test]
    fn malformed_resident_element_is_a_decode_failure() {
        let definition = super::super::definition(id(1));
        let malformed = Blob::new(vec![0_u8; TRIBLE_LEN - 1].into());
        let semantics = semantics(&definition, std::slice::from_ref(&malformed), &[]);
        let mut reader = ProbeReader::default();
        reader.insert(malformed.clone());

        assert!(matches!(
            materialize(&semantics, &definition, &reader),
            Err(MaterializationError::InvalidElement {
                data: failed,
                source: UnarchiveError::BadArchive,
            }) if failed == data(&malformed)
        ));
    }
}
