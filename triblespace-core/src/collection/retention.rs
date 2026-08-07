//! Policy-driven retention roots for resolved collection views.
//!
//! Collection records describe algebraic facts; their embedded hashes are not
//! ownership edges. This module therefore emits a two-sorted
//! [`RetentionRoots`](crate::repo::RetentionRoots): admitted ledger records are
//! direct roots, while selected physical data and commit metadata are recursive
//! roots whose resident descendants (attachments) are owned.
//!
//! Resolution is stateless. A `COMMIT`, `MERGE`, or `DERIVE` which was
//! validated from endpoint bytes cannot be revalidated after those bytes
//! disappear unless the positive verdict is durably available elsewhere. The
//! caller must name exactly those claims through
//! [`ValidationRetentionPolicy::DurableValidationEvidence`], and its future readers must
//! consume that same durable evidence. Every other admitted claim keeps all
//! validation endpoints recursively; if one is already absent, planning fails
//! rather than manufacturing evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::BlobEncoding;
use crate::id::Id;
use crate::inline::encodings::hash::Handle;
use crate::inline::{Inline, InlineEncoding};
use crate::repo::{BlobStoreMeta, RetentionRoots};

use super::{
    collection_physical_cover, CollectionData, CollectionResolution, DiscoveredCollectionRecords,
};

/// How endpoint bytes support positive claim validation after retention.
///
/// The conservative policy is explicit and is also the default. Durable
/// evidence is an external contract: every future resolver and rewrite must
/// consume the same positive verdicts keyed by exact claim id.
#[derive(Clone, Copy, Debug, Default)]
pub enum ValidationRetentionPolicy<'a> {
    /// Keep every admitted claim's resident validation endpoints.
    #[default]
    RetainAllEndpoints,
    /// Permit endpoint collection only for the exact claims named by a
    /// persistent positive-verdict policy.
    DurableValidationEvidence(&'a BTreeSet<Id>),
}

impl ValidationRetentionPolicy<'_> {
    fn has_durable_evidence(self, claim: Id) -> bool {
        match self {
            Self::RetainAllEndpoints => false,
            Self::DurableValidationEvidence(claims) => claims.contains(&claim),
        }
    }
}

/// A collection retention plan could not prove that every required blob stays
/// available.
#[derive(Debug)]
pub enum CollectionRetentionError<MetadataError> {
    /// A requested or derive-supporting collection has no retained definition.
    MissingDefinition {
        /// Intrinsic collection id.
        collection: Id,
    },
    /// Storage metadata lookup failed while establishing residency.
    Metadata {
        /// Handle whose residency could not be established.
        handle: Inline<Handle<UnknownBlob>>,
        /// Backend failure.
        source: MetadataError,
    },
    /// An admitted commit's signed metadata is absent.
    MissingCommitMetadata {
        /// Intrinsic commit-record id.
        commit: Id,
        /// Missing metadata blob.
        metadata: Inline<Handle<SimpleArchive>>,
    },
    /// A claim lacks durable validation evidence and one of the endpoint
    /// blobs needed to reproduce its verdict is absent.
    MissingValidationEndpoint {
        /// Intrinsic collection-record id.
        claim: Id,
        /// Missing endpoint.
        data: CollectionData,
    },
    /// The requested collection's semantic frontier has no complete resident
    /// physical cover.
    MissingPhysicalCover {
        /// Requested collection.
        collection: Id,
        /// Uncovered maximal semantic members.
        obligations: BTreeSet<CollectionData>,
    },
}

impl<MetadataError: fmt::Display> fmt::Display for CollectionRetentionError<MetadataError> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDefinition { collection } => {
                write!(f, "collection {collection:X} has no canonical definition")
            }
            Self::Metadata { handle, source } => write!(
                f,
                "failed to inspect collection retention handle {}: {source}",
                hex::encode_upper(handle.raw),
            ),
            Self::MissingCommitMetadata { commit, metadata } => write!(
                f,
                "admitted commit {commit:X} has missing metadata {}",
                hex::encode_upper(metadata.raw),
            ),
            Self::MissingValidationEndpoint { claim, data } => write!(
                f,
                "claim {claim:X} has no durable validation evidence and endpoint {} is absent",
                hex::encode_upper(data.raw),
            ),
            Self::MissingPhysicalCover {
                collection,
                obligations,
            } => write!(
                f,
                "collection {collection:X} has {} uncovered semantic-frontier obligation(s)",
                obligations.len(),
            ),
        }
    }
}

impl<MetadataError> Error for CollectionRetentionError<MetadataError>
where
    MetadataError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Metadata { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Plan roots for complete views of explicitly selected collections.
///
/// `resolution` must be the result of the caller's explicit authorization and
/// validation policy over `records`. Only claims in
/// [`CollectionResolution::admitted_claims`] can retain anything, so arbitrary
/// validly self-signed append noise does not become a storage root.
///
/// The requested collections are expanded backwards through admitted derives:
/// retaining a derived target also retains the source definitions, admitted
/// roots, metadata, and construction records needed to reconstruct its
/// semantic support. Physical-cover data is selected only for the originally
/// requested views. Claims without caller-supplied durable positive evidence
/// retain every endpoint as validation input.
///
/// [`ValidationRetentionPolicy::DurableValidationEvidence`] is an external, persistent
/// positive-verdict policy keyed by exact claim id. It is safe to name a claim
/// only when every later reader or rewrite will admit that claim from the same
/// durable evidence after its endpoint bytes disappear. The conservative
/// [`ValidationRetentionPolicy::RetainAllEndpoints`] policy keeps every
/// admitted claim reproducible from resident bytes.
///
/// The returned roots are a pure result, not a persisted retained-scope
/// registry. A collector must rediscover, resolve, and plan every locally
/// selected collection again on each later pass. A legacy pin must not be
/// removed until some higher layer durably owns that recurring policy.
pub fn plan_collection_retention<D, R>(
    records: &DiscoveredCollectionRecords,
    resolution: &CollectionResolution<D>,
    requested_collections: &BTreeSet<Id>,
    validation: ValidationRetentionPolicy<'_>,
    reader: &R,
) -> Result<RetentionRoots, CollectionRetentionError<<R as BlobStoreMeta>::MetaError>>
where
    R: BlobStoreMeta + ?Sized,
{
    let admitted = resolution.admitted_claims();

    // A target derived from another collection needs the source's admitted
    // semantic roots after restart. Compute the least backwards closure.
    let mut supporting_collections = requested_collections.clone();
    loop {
        let mut changed = false;
        for claim in records.derives() {
            if admitted.contains(&claim.id()) && supporting_collections.contains(&claim.target()) {
                changed |= supporting_collections.insert(claim.source());
            }
        }
        if !changed {
            break;
        }
    }

    let definitions: BTreeMap<_, _> = records
        .definitions()
        .iter()
        .map(|definition| (definition.id(), definition))
        .collect();
    let mut roots = RetentionRoots::new();
    for collection in &supporting_collections {
        let definition =
            definitions
                .get(collection)
                .ok_or(CollectionRetentionError::MissingDefinition {
                    collection: *collection,
                })?;
        roots.retain_direct(definition.to_blob().get_handle());
    }

    for claim in records.commits() {
        if !admitted.contains(&claim.id()) || !supporting_collections.contains(&claim.collection())
        {
            continue;
        }
        roots.retain_direct(claim.to_blob().get_handle());
        require_resident(reader, claim.metadata(), |source| {
            CollectionRetentionError::Metadata {
                handle: claim.metadata().transmute(),
                source,
            }
        })?
        .then_some(())
        .ok_or(CollectionRetentionError::MissingCommitMetadata {
            commit: claim.id(),
            metadata: claim.metadata(),
        })?;
        roots.retain_recursive(claim.metadata());
        if !validation.has_durable_evidence(claim.id()) {
            retain_validation_endpoint(reader, &mut roots, claim.id(), claim.data())?;
        }
    }

    for claim in records.merges() {
        if !admitted.contains(&claim.id()) || !supporting_collections.contains(&claim.collection())
        {
            continue;
        }
        roots.retain_direct(claim.to_blob().get_handle());
        if !validation.has_durable_evidence(claim.id()) {
            let (low, high) = claim.inputs();
            for endpoint in [low, high, claim.result()] {
                retain_validation_endpoint(reader, &mut roots, claim.id(), endpoint)?;
            }
        }
    }

    for claim in records.derives() {
        if !admitted.contains(&claim.id()) || !supporting_collections.contains(&claim.target()) {
            continue;
        }
        roots.retain_direct(claim.to_blob().get_handle());
        if !validation.has_durable_evidence(claim.id()) {
            let (input, output) = claim.mapping();
            for endpoint in [input, output] {
                retain_validation_endpoint(reader, &mut roots, claim.id(), endpoint)?;
            }
        }
    }

    // Physical cover is a view policy, not ledger admission. Only requested
    // collections need resident bytes; backwards derive support can remain a
    // semantic proof when its positive validation evidence is durable.
    for collection in requested_collections {
        let mut resident = BTreeSet::new();
        for member in resolution
            .semantics()
            .members(*collection)
            .into_iter()
            .flatten()
            .copied()
        {
            let handle = Handle::<UnknownBlob>::from_hash(member);
            if require_resident(reader, handle, |source| {
                CollectionRetentionError::Metadata { handle, source }
            })? {
                resident.insert(member);
            }
        }

        let cover = collection_physical_cover(resolution.semantics(), *collection, &resident);
        if !cover.missing.is_empty() {
            return Err(CollectionRetentionError::MissingPhysicalCover {
                collection: *collection,
                obligations: cover.missing,
            });
        }
        for data in cover.cover {
            roots.retain_recursive(Handle::<UnknownBlob>::from_hash(data));
        }
    }

    Ok(roots)
}

fn retain_validation_endpoint<R>(
    reader: &R,
    roots: &mut RetentionRoots,
    claim: Id,
    data: CollectionData,
) -> Result<(), CollectionRetentionError<<R as BlobStoreMeta>::MetaError>>
where
    R: BlobStoreMeta + ?Sized,
{
    let handle = Handle::<UnknownBlob>::from_hash(data);
    if !require_resident(reader, handle, |source| {
        CollectionRetentionError::Metadata { handle, source }
    })? {
        return Err(CollectionRetentionError::MissingValidationEndpoint { claim, data });
    }
    roots.retain_recursive(handle);
    Ok(())
}

fn require_resident<R, S, F>(
    reader: &R,
    handle: Inline<Handle<S>>,
    map_error: F,
) -> Result<bool, CollectionRetentionError<<R as BlobStoreMeta>::MetaError>>
where
    R: BlobStoreMeta + ?Sized,
    S: BlobEncoding + 'static,
    Handle<S>: InlineEncoding,
    F: FnOnce(<R as BlobStoreMeta>::MetaError) -> CollectionRetentionError<R::MetaError>,
{
    reader
        .metadata(handle)
        .map(|entry| entry.is_some())
        .map_err(map_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::convert::Infallible;

    use ed25519_dalek::SigningKey;

    use crate::blob::encodings::longstring::LongString;
    use crate::blob::{Blob, IntoBlob, MemoryBlobStore};
    use crate::collection::simplearchive_union::{self, SimpleArchiveUnionValidationError};
    use crate::collection::{
        discover_collection_records, resolve_collection_semantics, CollectionClaimValidation,
        CollectionCommit, CollectionMerge, CollectionValidationRequest,
    };
    use crate::inline::encodings::hash::{Blake3, Hash};
    use crate::macros::entity;
    use crate::metadata;
    use crate::repo::{BlobStore, BlobStoreGet};
    use crate::trible::{Trible, TribleSet, TRIBLE_LEN};

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
        Inline::<Hash<Blake3>>::new(Blake3::digest(&blob.bytes))
    }

    fn load_archive<R: BlobStoreGet>(
        reader: &R,
        data: CollectionData,
    ) -> Option<Blob<SimpleArchive>> {
        reader.get(Handle::<SimpleArchive>::from_hash(data)).ok()
    }

    fn validate_union<R: BlobStoreGet>(
        reader: &R,
        durable: &BTreeSet<Id>,
        request: CollectionValidationRequest<'_>,
    ) -> Result<CollectionClaimValidation<SimpleArchiveUnionValidationError>, Infallible> {
        if durable.contains(&request.claim_id()) {
            return Ok(CollectionClaimValidation::Accepted);
        }

        let verdict = match request {
            CollectionValidationRequest::Commit { definition, claim } => {
                let Some(blob) = load_archive(reader, claim.data()) else {
                    return Ok(CollectionClaimValidation::Pending);
                };
                match simplearchive_union::validate_commit(definition, claim, &blob) {
                    Ok(()) => CollectionClaimValidation::Accepted,
                    Err(error) => CollectionClaimValidation::Rejected(error),
                }
            }
            CollectionValidationRequest::Merge { definition, claim } => {
                let (low, high) = claim.inputs();
                let (Some(low), Some(high), Some(result)) = (
                    load_archive(reader, low),
                    load_archive(reader, high),
                    load_archive(reader, claim.result()),
                ) else {
                    return Ok(CollectionClaimValidation::Pending);
                };
                match simplearchive_union::validate_merge(definition, claim, &low, &high, &result) {
                    Ok(()) => CollectionClaimValidation::Accepted,
                    Err(error) => CollectionClaimValidation::Rejected(error),
                }
            }
            CollectionValidationRequest::Derive { .. } => CollectionClaimValidation::Pending,
        };
        Ok(verdict)
    }

    fn insert_record_fixture(
        store: &mut MemoryBlobStore,
        definition: &super::super::CollectionDefinition,
        data: Blob<SimpleArchive>,
        metadata: Blob<SimpleArchive>,
        key: &SigningKey,
    ) -> CollectionCommit {
        let commit = CollectionCommit::sign(
            key,
            definition.id(),
            self::data(&data),
            metadata.get_handle(),
        );
        store.insert(super::super::CollectionDefinition::to_blob(definition));
        store.insert(data);
        store.insert(metadata);
        store.insert(CollectionCommit::to_blob(&commit));
        commit
    }

    #[test]
    fn collection_only_roots_survive_with_owned_attachments() {
        let definition = simplearchive_union::definition(id(1));
        let key = SigningKey::from_bytes(&[7; 32]);
        let content_text: Blob<LongString> = "retained content".to_owned().to_blob();
        let content_text_handle = content_text.get_handle();
        let metadata_text: Blob<LongString> = "retained metadata".to_owned().to_blob();
        let metadata_text_handle = metadata_text.get_handle();
        let orphan: Blob<LongString> = "orphan".to_owned().to_blob();
        let orphan_handle = orphan.get_handle();

        let content = entity! { metadata::name: content_text_handle }
            .into_facts()
            .to_blob();
        let metadata = entity! { metadata::description: metadata_text_handle }
            .into_facts()
            .to_blob();

        let mut store = MemoryBlobStore::new();
        store.insert(content_text);
        store.insert(metadata_text);
        store.insert(orphan);
        let commit = insert_record_fixture(&mut store, &definition, content, metadata, &key);

        let reader = store.reader().unwrap();
        let records = discover_collection_records(&reader).unwrap();
        let authorized = BTreeSet::from([commit.id()]);
        let resolution = resolve_collection_semantics(&records, &authorized, |request| {
            validate_union(&reader, &BTreeSet::new(), request)
        })
        .unwrap();
        let roots = plan_collection_retention(
            &records,
            &resolution,
            &BTreeSet::from([definition.id()]),
            ValidationRetentionPolicy::RetainAllEndpoints,
            &reader,
        )
        .unwrap();
        let keep = roots.expanded(&reader);

        store.keep(keep);
        let reader = store.reader().unwrap();
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(
                super::super::CollectionDefinition::to_blob(&definition).get_handle(),
            )
            .is_ok());
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(CollectionCommit::to_blob(&commit).get_handle())
            .is_ok());
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(Handle::from_hash(commit.data()))
            .is_ok());
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(commit.metadata())
            .is_ok());
        assert!(reader
            .get::<Blob<LongString>, _>(content_text_handle)
            .is_ok());
        assert!(reader
            .get::<Blob<LongString>, _>(metadata_text_handle)
            .is_ok());
        assert!(reader.get::<Blob<LongString>, _>(orphan_handle).is_err());
    }

    #[test]
    fn durable_claim_evidence_can_collect_superseded_physical_inputs() {
        let definition = simplearchive_union::definition(id(1));
        let key = SigningKey::from_bytes(&[7; 32]);
        let left = archive([row(1, 1, 1)]);
        let right = archive([row(2, 1, 2)]);
        let result = simplearchive_union::join(&left, &right).unwrap();
        let empty_metadata = TribleSet::new().to_blob();

        let mut store = MemoryBlobStore::new();
        let first = insert_record_fixture(
            &mut store,
            &definition,
            left.clone(),
            empty_metadata.clone(),
            &key,
        );
        let second =
            insert_record_fixture(&mut store, &definition, right.clone(), empty_metadata, &key);
        let merge = CollectionMerge::new(definition.id(), data(&left), data(&right), data(&result));
        store.insert(result.clone());
        store.insert(CollectionMerge::to_blob(&merge));

        let reader = store.reader().unwrap();
        let records = discover_collection_records(&reader).unwrap();
        let authorized = BTreeSet::from([first.id(), second.id()]);
        let resolution = resolve_collection_semantics(&records, &authorized, |request| {
            validate_union(&reader, &BTreeSet::new(), request)
        })
        .unwrap();
        let requested = BTreeSet::from([definition.id()]);

        let conservative = plan_collection_retention(
            &records,
            &resolution,
            &requested,
            ValidationRetentionPolicy::RetainAllEndpoints,
            &reader,
        )
        .unwrap()
        .expanded(&reader);
        assert!(conservative.contains(&left.get_handle().transmute()));
        assert!(conservative.contains(&right.get_handle().transmute()));
        assert!(conservative.contains(&result.get_handle().transmute()));

        // This set models a persistent verdict store which the next resolver
        // also consumes. Without that shared durable policy, passing an empty
        // set above is the only safe choice.
        let durable = BTreeSet::from([first.id(), second.id(), merge.id()]);
        let keep = plan_collection_retention(
            &records,
            &resolution,
            &requested,
            ValidationRetentionPolicy::DurableValidationEvidence(&durable),
            &reader,
        )
        .unwrap()
        .expanded(&reader);
        store.keep(keep);

        let reader = store.reader().unwrap();
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(left.get_handle())
            .is_err());
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(right.get_handle())
            .is_err());
        assert!(reader
            .get::<Blob<SimpleArchive>, _>(result.get_handle())
            .is_ok());

        let records = discover_collection_records(&reader).unwrap();
        let resumed = resolve_collection_semantics(&records, &authorized, |request| {
            validate_union(&reader, &durable, request)
        })
        .unwrap();
        assert!(matches!(
            plan_collection_retention(
                &records,
                &resumed,
                &requested,
                ValidationRetentionPolicy::RetainAllEndpoints,
                &reader,
            ),
            Err(CollectionRetentionError::MissingValidationEndpoint { claim, .. })
                if claim == first.id() || claim == second.id()
        ));
        let materialized =
            simplearchive_union::materialize(resumed.semantics(), &definition, &reader).unwrap();
        let expected: TribleSet = result.try_from_blob().unwrap();
        assert_eq!(materialized, expected);
    }
}
