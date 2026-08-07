//! Canonical TribleSet set union over
//! [`SimpleArchive`](crate::blob::encodings::simplearchive::SimpleArchive)
//! elements.
//!
//! This is the first concrete production collection kind. A collection pairs
//! an extrinsic scope with the existing `SimpleArchive` representation and the
//! [`TRIBLE_SET_UNION_RECIPE_V1`](crate::collection::simplearchive_union::TRIBLE_SET_UNION_RECIPE_V1)
//! semantic recipe. Every element is an exact, canonical EAV-ordered stream of
//! 64-byte tribles. Its join is ordinary set union, so canonical output bytes
//! and their Blake3 identity are associative, commutative, and idempotent.
//!
//! Validation, joins, and publication operate directly on the canonical byte
//! streams. They deliberately do not construct [`crate::trible::TribleSet`] or
//! PATCH indexes; query-time decoding keeps its independently optimized path.
//! Missing endpoint blobs are likewise outside this module: callers defer an
//! equation until its three blobs are resident, then call
//! [`validate_merge`](crate::collection::simplearchive_union::validate_merge).

use std::error::Error;
use std::fmt;

use anybytes::{Bytes, View};
use ed25519_dalek::SigningKey;

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::Blob;
use crate::id::Id;
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle, Hash};
use crate::inline::Inline;
use crate::metadata::MetaDescribe;
use crate::repo::{BlobStorePut, StorageFlush};
use crate::trible::{Trible, TRIBLE_LEN};

use super::{CollectionCommit, CollectionData, CollectionDefinition, CollectionMerge};

/// Canonical TribleSet set-union recipe, version 1.
///
/// This identifies the semantic law independently of its direct-stream
/// implementation and of the collection's blob representation. Minted with
/// `trible genid` on 2026-08-07.
pub const TRIBLE_SET_UNION_RECIPE_V1: Id = id_hex!("6D64C5F4B9E9B73F57C5F8702AB7FE45");

/// The collection endpoint involved in a validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElementRole {
    /// Data introduced by a signed commit.
    CommitData,
    /// Canonically lower merge input.
    MergeLow,
    /// Canonically higher merge input.
    MergeHigh,
    /// Claimed merge output.
    MergeResult,
}

impl fmt::Display for ElementRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitData => write!(f, "commit data"),
            Self::MergeLow => write!(f, "merge low input"),
            Self::MergeHigh => write!(f, "merge high input"),
            Self::MergeResult => write!(f, "merge result"),
        }
    }
}

/// Failure to validate a commit or merge against this concrete collection kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SimpleArchiveUnionValidationError {
    /// The definition names another blob representation.
    WrongRepresentation { expected: Id, actual: Id },
    /// The definition names another semantic recipe.
    WrongRecipe { expected: Id, actual: Id },
    /// The record belongs to another collection definition.
    WrongCollection { expected: Id, actual: Id },
    /// Supplied bytes do not have the content identity named by the record.
    EndpointMismatch {
        role: ElementRole,
        expected: CollectionData,
        actual: CollectionData,
    },
    /// An endpoint is not a canonical `SimpleArchive` element.
    InvalidElement {
        role: ElementRole,
        source: UnarchiveError,
    },
    /// The claimed result is not the exact canonical union of the two inputs.
    WrongMergeResult,
}

impl fmt::Display for SimpleArchiveUnionValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongRepresentation { expected, actual } => write!(
                f,
                "collection representation {actual:X} does not match SimpleArchive {expected:X}"
            ),
            Self::WrongRecipe { expected, actual } => write!(
                f,
                "collection recipe {actual:X} does not match TribleSet union {expected:X}"
            ),
            Self::WrongCollection { expected, actual } => write!(
                f,
                "record collection {actual:X} does not match definition {expected:X}"
            ),
            Self::EndpointMismatch {
                role,
                expected,
                actual,
            } => write!(
                f,
                "{role} handle {} does not match claimed {}",
                hex::encode_upper(actual.raw),
                hex::encode_upper(expected.raw),
            ),
            Self::InvalidElement { role, source } => {
                write!(f, "{role} is not a canonical SimpleArchive: {source}")
            }
            Self::WrongMergeResult => {
                write!(f, "merge result is not the exact canonical input union")
            }
        }
    }
}

impl Error for SimpleArchiveUnionValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidElement { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Failure to publish a crash-ordered collection record.
///
/// Dependency writes and their durability barrier are distinguished from the
/// record write and its barrier so callers can report which publication phase
/// failed. The ordering guarantee applies at successful operation boundaries:
/// a record write is not attempted until the dependency flush succeeds, and a
/// record is not returned until its own flush succeeds.
///
/// [`BlobStorePut`] and [`StorageFlush`] do not require failed I/O operations to
/// be atomic. A backend error may therefore require backend-specific recovery
/// before retrying. Once the store is usable again, replaying the same logical
/// publication is content-addressed and deterministic.
#[derive(Debug)]
pub enum PublicationError<PutError, FlushError> {
    /// The definition or collection data is invalid for this concrete kind.
    Validation(SimpleArchiveUnionValidationError),
    /// Commit metadata is not a canonical `SimpleArchive`.
    InvalidMetadata(UnarchiveError),
    /// A definition, element, result, or metadata write failed.
    DependencyPut(PutError),
    /// The dependency durability barrier failed; no record write was attempted.
    DependencyFlush(FlushError),
    /// The final commit or merge record write failed.
    RecordPut(PutError),
    /// The final record durability barrier failed.
    RecordFlush(FlushError),
}

impl<PutError, FlushError> fmt::Display for PublicationError<PutError, FlushError>
where
    PutError: fmt::Display,
    FlushError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(error) => write!(f, "invalid collection publication: {error}"),
            Self::InvalidMetadata(error) => {
                write!(
                    f,
                    "commit metadata is not a canonical SimpleArchive: {error}"
                )
            }
            Self::DependencyPut(error) => {
                write!(f, "failed to write a collection dependency: {error}")
            }
            Self::DependencyFlush(error) => {
                write!(f, "failed to flush collection dependencies: {error}")
            }
            Self::RecordPut(error) => write!(f, "failed to write collection record: {error}"),
            Self::RecordFlush(error) => write!(f, "failed to flush collection record: {error}"),
        }
    }
}

impl<PutError, FlushError> Error for PublicationError<PutError, FlushError>
where
    PutError: Error + 'static,
    FlushError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Validation(error) => Some(error),
            Self::InvalidMetadata(error) => Some(error),
            Self::DependencyPut(error) | Self::RecordPut(error) => Some(error),
            Self::DependencyFlush(error) | Self::RecordFlush(error) => Some(error),
        }
    }
}

/// Construct this collection kind for an extrinsic dataset scope.
pub fn definition(scope: Id) -> CollectionDefinition {
    CollectionDefinition::new(
        scope,
        <SimpleArchive as MetaDescribe>::id(),
        TRIBLE_SET_UNION_RECIPE_V1,
    )
}

/// Validate one canonical `SimpleArchive` collection element without decoding
/// it into query indexes.
pub fn validate_element(blob: &Blob<SimpleArchive>) -> Result<(), UnarchiveError> {
    canonical_rows(blob).map(|_| ())
}

/// Compute the exact canonical union of two `SimpleArchive` elements.
///
/// Both inputs are validated before an identity fast path or output allocation
/// is taken. Equal and empty inputs reuse their immutable bytes but recompute
/// the returned handle; every other case performs one lexicographic two-pointer
/// merge and emits shared rows once.
pub fn join(
    left: &Blob<SimpleArchive>,
    right: &Blob<SimpleArchive>,
) -> Result<Blob<SimpleArchive>, UnarchiveError> {
    let left_rows = canonical_rows(left)?;
    let right_rows = canonical_rows(right)?;
    Ok(join_canonical_rows(left, right, &left_rows, &right_rows))
}

/// Validate a discovered commit as one canonical root of this collection.
///
/// This binds the concrete definition, record collection, endpoint identity,
/// and element bytes in one check. The record's strict self-signature and the
/// caller's authorization policy remain separate admission prerequisites.
pub fn validate_commit(
    definition: &CollectionDefinition,
    commit: &CollectionCommit,
    data_blob: &Blob<SimpleArchive>,
) -> Result<(), SimpleArchiveUnionValidationError> {
    validate_definition(definition)?;
    validate_collection(definition, commit.collection())?;
    validate_endpoint(ElementRole::CommitData, commit.data(), data_blob)?;
    Ok(())
}

/// Validate a claimed exact union without materializing another result blob.
///
/// All endpoints are first bound to their record hashes and validated as
/// canonical archives. The expected two-way union is then compared row-for-row
/// with `result`, using constant auxiliary space.
pub fn validate_merge(
    definition: &CollectionDefinition,
    claim: &CollectionMerge,
    low: &Blob<SimpleArchive>,
    high: &Blob<SimpleArchive>,
    result: &Blob<SimpleArchive>,
) -> Result<(), SimpleArchiveUnionValidationError> {
    validate_definition(definition)?;
    validate_collection(definition, claim.collection())?;

    let (expected_low, expected_high) = claim.inputs();
    validate_handle(ElementRole::MergeLow, expected_low, low)?;
    validate_handle(ElementRole::MergeHigh, expected_high, high)?;
    validate_handle(ElementRole::MergeResult, claim.result(), result)?;

    let low_rows = canonical_rows(low).map_err(|source| {
        SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::MergeLow,
            source,
        }
    })?;
    let high_rows = canonical_rows(high).map_err(|source| {
        SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::MergeHigh,
            source,
        }
    })?;
    let result_rows = canonical_rows(result).map_err(|source| {
        SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::MergeResult,
            source,
        }
    })?;

    if !UnionRows::new(&low_rows, &high_rows).eq(result_rows.iter()) {
        return Err(SimpleArchiveUnionValidationError::WrongMergeResult);
    }
    Ok(())
}

/// Publish a signed membership root after its dependencies are crash-durable.
///
/// Supplied data and metadata are normalized from their bytes before either is
/// validated or stored, so a forged [`Blob::with_handle`] cache cannot enter
/// storage or the signed transcript. The exact write order is:
///
/// 1. definition, data, metadata;
/// 2. dependency flush;
/// 3. signed commit record;
/// 4. record flush.
///
/// A completed prefix before the record write leaves only content-addressed
/// dependencies, and this function returns a commit only after both durability
/// barriers succeed. Failed backend I/O may require recovery according to that
/// backend's contract; after recovery, replay with the same arguments is
/// deterministic and idempotent. Signature authorization remains a reader-side
/// policy decision.
pub fn publish_commit<S>(
    store: &mut S,
    definition: &CollectionDefinition,
    data: &Blob<SimpleArchive>,
    metadata: &Blob<SimpleArchive>,
    signing_key: &SigningKey,
) -> Result<CollectionCommit, PublicationError<S::PutError, <S as StorageFlush>::Error>>
where
    S: BlobStorePut + StorageFlush,
{
    validate_definition(definition).map_err(PublicationError::Validation)?;

    let data = normalize_blob(data);
    validate_element(&data).map_err(|source| {
        PublicationError::Validation(SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::CommitData,
            source,
        })
    })?;

    let metadata = normalize_blob(metadata);
    validate_element(&metadata).map_err(PublicationError::InvalidMetadata)?;

    let commit = CollectionCommit::sign(
        signing_key,
        definition.id(),
        normalized_data_identity(&data),
        metadata.get_handle(),
    );

    store
        .put::<SimpleArchive, _>(definition.to_blob())
        .map_err(PublicationError::DependencyPut)?;
    store
        .put::<SimpleArchive, _>(data)
        .map_err(PublicationError::DependencyPut)?;
    store
        .put::<SimpleArchive, _>(metadata)
        .map_err(PublicationError::DependencyPut)?;
    store.flush().map_err(PublicationError::DependencyFlush)?;
    store
        .put::<SimpleArchive, _>(commit.to_blob())
        .map_err(PublicationError::RecordPut)?;
    store.flush().map_err(PublicationError::RecordFlush)?;

    Ok(commit)
}

/// Publish an exact merge after its definition, inputs, and result are durable.
///
/// Input blobs are normalized from their bytes, ordered by their freshly
/// computed Blake3 identities, validated, and joined directly. The exact write
/// order is:
///
/// 1. definition, canonical low input, canonical high input, result;
/// 2. dependency flush;
/// 3. merge record;
/// 4. record flush.
///
/// The returned pair is `(canonical record, canonical result blob)`. A merge
/// record is never attempted before a successful dependency flush. Failed
/// backend I/O may require recovery according to that backend's contract;
/// after recovery, replay with the same arguments is deterministic and
/// idempotent.
pub fn publish_merge<S>(
    store: &mut S,
    definition: &CollectionDefinition,
    low: &Blob<SimpleArchive>,
    high: &Blob<SimpleArchive>,
) -> Result<
    (CollectionMerge, Blob<SimpleArchive>),
    PublicationError<S::PutError, <S as StorageFlush>::Error>,
>
where
    S: BlobStorePut + StorageFlush,
{
    validate_definition(definition).map_err(PublicationError::Validation)?;

    let mut low = normalize_blob(low);
    let mut high = normalize_blob(high);
    let mut low_data = normalized_data_identity(&low);
    let mut high_data = normalized_data_identity(&high);
    if high_data < low_data {
        std::mem::swap(&mut low, &mut high);
        std::mem::swap(&mut low_data, &mut high_data);
    }

    let low_rows = canonical_rows(&low).map_err(|source| {
        PublicationError::Validation(SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::MergeLow,
            source,
        })
    })?;
    let high_rows = canonical_rows(&high).map_err(|source| {
        PublicationError::Validation(SimpleArchiveUnionValidationError::InvalidElement {
            role: ElementRole::MergeHigh,
            source,
        })
    })?;
    let result = join_canonical_rows(&low, &high, &low_rows, &high_rows);
    let merge = CollectionMerge::new(
        definition.id(),
        low_data,
        high_data,
        normalized_data_identity(&result),
    );

    store
        .put::<SimpleArchive, _>(definition.to_blob())
        .map_err(PublicationError::DependencyPut)?;
    store
        .put::<SimpleArchive, _>(low)
        .map_err(PublicationError::DependencyPut)?;
    store
        .put::<SimpleArchive, _>(high)
        .map_err(PublicationError::DependencyPut)?;
    store
        .put::<SimpleArchive, _>(result.clone())
        .map_err(PublicationError::DependencyPut)?;
    store.flush().map_err(PublicationError::DependencyFlush)?;
    store
        .put::<SimpleArchive, _>(merge.to_blob())
        .map_err(PublicationError::RecordPut)?;
    store.flush().map_err(PublicationError::RecordFlush)?;

    Ok((merge, result))
}

fn validate_definition(
    definition: &CollectionDefinition,
) -> Result<(), SimpleArchiveUnionValidationError> {
    let expected_representation = <SimpleArchive as MetaDescribe>::id();
    if definition.representation() != expected_representation {
        return Err(SimpleArchiveUnionValidationError::WrongRepresentation {
            expected: expected_representation,
            actual: definition.representation(),
        });
    }
    if definition.recipe() != TRIBLE_SET_UNION_RECIPE_V1 {
        return Err(SimpleArchiveUnionValidationError::WrongRecipe {
            expected: TRIBLE_SET_UNION_RECIPE_V1,
            actual: definition.recipe(),
        });
    }
    Ok(())
}

fn validate_collection(
    definition: &CollectionDefinition,
    actual: Id,
) -> Result<(), SimpleArchiveUnionValidationError> {
    if actual != definition.id() {
        return Err(SimpleArchiveUnionValidationError::WrongCollection {
            expected: definition.id(),
            actual,
        });
    }
    Ok(())
}

fn validate_endpoint(
    role: ElementRole,
    expected: CollectionData,
    blob: &Blob<SimpleArchive>,
) -> Result<(), SimpleArchiveUnionValidationError> {
    validate_handle(role, expected, blob)?;
    validate_element(blob)
        .map_err(|source| SimpleArchiveUnionValidationError::InvalidElement { role, source })
}

fn validate_handle(
    role: ElementRole,
    expected: CollectionData,
    blob: &Blob<SimpleArchive>,
) -> Result<(), SimpleArchiveUnionValidationError> {
    // `Blob::with_handle` is an explicitly trusted read-path constructor, so
    // an admission boundary must not rely on its cached handle. Recompute the
    // content identity from the supplied bytes before accepting the endpoint.
    let actual = Inline::<Hash<Blake3>>::new(Blake3::digest(&blob.bytes));
    if actual != expected {
        return Err(SimpleArchiveUnionValidationError::EndpointMismatch {
            role,
            expected,
            actual,
        });
    }
    Ok(())
}

fn normalize_blob(blob: &Blob<SimpleArchive>) -> Blob<SimpleArchive> {
    Blob::new(blob.bytes.clone())
}

fn normalized_data_identity(blob: &Blob<SimpleArchive>) -> CollectionData {
    Handle::<SimpleArchive>::to_hash(blob.get_handle())
}

fn join_canonical_rows(
    left: &Blob<SimpleArchive>,
    right: &Blob<SimpleArchive>,
    left_rows: &[[u8; TRIBLE_LEN]],
    right_rows: &[[u8; TRIBLE_LEN]],
) -> Blob<SimpleArchive> {
    if left.bytes == right.bytes || right_rows.is_empty() {
        return Blob::new(left.bytes.clone());
    }
    if left_rows.is_empty() {
        return Blob::new(right.bytes.clone());
    }

    let mut rows = Vec::with_capacity(left_rows.len() + right_rows.len());
    rows.extend(UnionRows::new(left_rows, right_rows).copied());
    Blob::new(Bytes::from(rows))
}

fn canonical_rows(blob: &Blob<SimpleArchive>) -> Result<View<[[u8; TRIBLE_LEN]]>, UnarchiveError> {
    let rows: View<[[u8; TRIBLE_LEN]]> = blob
        .bytes
        .clone()
        .view()
        .map_err(|_| UnarchiveError::BadArchive)?;
    let mut previous: Option<&[u8; TRIBLE_LEN]> = None;
    for row in rows.iter() {
        if Trible::as_transmute_force_raw(row).is_none() {
            return Err(UnarchiveError::BadTrible);
        }
        if let Some(previous) = previous {
            if previous == row {
                return Err(UnarchiveError::BadCanonicalizationRedundancy);
            }
            if previous > row {
                return Err(UnarchiveError::BadCanonicalizationOrdering);
            }
        }
        previous = Some(row);
    }
    Ok(rows)
}

struct UnionRows<'a> {
    left: &'a [[u8; TRIBLE_LEN]],
    right: &'a [[u8; TRIBLE_LEN]],
    left_index: usize,
    right_index: usize,
}

impl<'a> UnionRows<'a> {
    fn new(left: &'a [[u8; TRIBLE_LEN]], right: &'a [[u8; TRIBLE_LEN]]) -> Self {
        Self {
            left,
            right,
            left_index: 0,
            right_index: 0,
        }
    }
}

impl<'a> Iterator for UnionRows<'a> {
    type Item = &'a [u8; TRIBLE_LEN];

    fn next(&mut self) -> Option<Self::Item> {
        match (
            self.left.get(self.left_index),
            self.right.get(self.right_index),
        ) {
            (Some(left), Some(right)) => match left.cmp(right) {
                std::cmp::Ordering::Less => {
                    self.left_index += 1;
                    Some(left)
                }
                std::cmp::Ordering::Equal => {
                    self.left_index += 1;
                    self.right_index += 1;
                    Some(left)
                }
                std::cmp::Ordering::Greater => {
                    self.right_index += 1;
                    Some(right)
                }
            },
            (Some(left), None) => {
                self.left_index += 1;
                Some(left)
            }
            (None, Some(right)) => {
                self.right_index += 1;
                Some(right)
            }
            (None, None) => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.left.len() - self.left_index;
        let right = self.right.len() - self.right_index;
        (left.max(right), left.checked_add(right))
    }
}

impl std::iter::FusedIterator for UnionRows<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use ed25519_dalek::SigningKey;
    use hex_literal::hex;

    use crate::blob::{BlobEncoding, IntoBlob};
    use crate::collection::{discover_collection_records, empty_metadata_handle};
    use crate::inline::InlineEncoding;
    use crate::repo::pile::Pile;
    use crate::repo::{BlobStore, BlobStoreGet};
    use crate::trible::TribleSet;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct ProbeFailure(usize);

    impl fmt::Display for ProbeFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "injected failure at operation {}", self.0)
        }
    }

    impl Error for ProbeFailure {}

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ProbeEvent {
        Put([u8; 32]),
        Flush,
    }

    #[derive(Default)]
    struct ProbeStore {
        events: Vec<ProbeEvent>,
        known: BTreeSet<[u8; 32]>,
        pending: BTreeSet<[u8; 32]>,
        durable: BTreeSet<[u8; 32]>,
        fail_at: Option<usize>,
    }

    impl ProbeStore {
        // This probe fails before an operation takes effect, so it exercises
        // publication ordering at trait-operation boundaries. BlobStorePut
        // does not promise that a real backend cannot leave torn physical I/O.
        fn failing_before_effect_at(operation: usize) -> Self {
            Self {
                fail_at: Some(operation),
                ..Self::default()
            }
        }

        fn attempt(&mut self, event: ProbeEvent) -> Result<(), ProbeFailure> {
            self.events.push(event);
            let operation = self.events.len();
            if self.fail_at == Some(operation) {
                return Err(ProbeFailure(operation));
            }
            Ok(())
        }

        fn recover(&mut self) {
            self.fail_at = None;
        }
    }

    impl BlobStorePut for ProbeStore {
        type PutError = ProbeFailure;

        fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
        where
            S: BlobEncoding + 'static,
            T: IntoBlob<S>,
            Handle<S>: InlineEncoding,
        {
            let blob: Blob<S> = item.to_blob();
            let handle = blob.get_handle();
            self.attempt(ProbeEvent::Put(handle.raw))?;
            self.known.insert(handle.raw);
            self.pending.insert(handle.raw);
            Ok(handle)
        }
    }

    impl StorageFlush for ProbeStore {
        type Error = ProbeFailure;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.attempt(ProbeEvent::Flush)?;
            self.durable.extend(std::mem::take(&mut self.pending));
            Ok(())
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

    fn raw_archive(rows: Vec<[u8; TRIBLE_LEN]>) -> Blob<SimpleArchive> {
        Blob::new(Bytes::from(rows))
    }

    fn data(blob: &Blob<SimpleArchive>) -> CollectionData {
        Inline::<Hash<Blake3>>::new(Blake3::digest(&blob.bytes))
    }

    fn ordered_inputs<'a>(
        left: &'a Blob<SimpleArchive>,
        right: &'a Blob<SimpleArchive>,
    ) -> (&'a Blob<SimpleArchive>, &'a Blob<SimpleArchive>) {
        if data(left) <= data(right) {
            (left, right)
        } else {
            (right, left)
        }
    }

    fn put_event(blob: &Blob<SimpleArchive>) -> ProbeEvent {
        ProbeEvent::Put(blob.get_handle().raw)
    }

    fn commit_fixture() -> (
        CollectionDefinition,
        Blob<SimpleArchive>,
        Blob<SimpleArchive>,
        SigningKey,
        CollectionCommit,
    ) {
        let definition = definition(id(1));
        let data_blob = archive([row(1, 1, 1), row(3, 1, 3)]);
        let metadata = archive([row(9, 1, 9)]);
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let commit = CollectionCommit::sign(
            &signing_key,
            definition.id(),
            data(&data_blob),
            metadata.get_handle(),
        );
        (definition, data_blob, metadata, signing_key, commit)
    }

    #[test]
    fn commit_publication_normalizes_orders_flushes_and_replays_idempotently() {
        let (definition, data_blob, metadata, signing_key, expected) = commit_fixture();
        let bogus = archive([row(14, 1, 14)]);
        let forged_data = Blob::with_handle(data_blob.bytes.clone(), bogus.get_handle());
        let forged_metadata = Blob::with_handle(metadata.bytes.clone(), bogus.get_handle());
        let definition_blob = CollectionDefinition::to_blob(&definition);
        let record_blob = CollectionCommit::to_blob(&expected);
        let sequence = vec![
            put_event(&definition_blob),
            put_event(&data_blob),
            put_event(&metadata),
            ProbeEvent::Flush,
            put_event(&record_blob),
            ProbeEvent::Flush,
        ];

        let mut store = ProbeStore::default();
        let first = publish_commit(
            &mut store,
            &definition,
            &forged_data,
            &forged_metadata,
            &signing_key,
        )
        .unwrap();
        let second = publish_commit(
            &mut store,
            &definition,
            &forged_data,
            &forged_metadata,
            &signing_key,
        )
        .unwrap();

        assert_eq!(first, expected);
        assert_eq!(second, expected);
        assert_eq!(first.data(), data(&data_blob));
        assert_eq!(first.metadata(), metadata.get_handle());
        first.verify_strict().unwrap();
        validate_commit(&definition, &first, &data_blob).unwrap();

        let mut expected_events = sequence.clone();
        expected_events.extend(sequence);
        assert_eq!(store.events, expected_events);
        let expected_handles = BTreeSet::from([
            definition_blob.get_handle().raw,
            data_blob.get_handle().raw,
            metadata.get_handle().raw,
            record_blob.get_handle().raw,
        ]);
        assert_eq!(store.known, expected_handles);
        assert_eq!(store.durable, expected_handles);
        assert!(store.pending.is_empty());
        assert!(!store.known.contains(&bogus.get_handle().raw));
    }

    #[test]
    fn merge_publication_normalizes_canonicalizes_and_replays_idempotently() {
        let definition = definition(id(1));
        let left = archive([row(1, 1, 1), row(3, 1, 3)]);
        let right = archive([row(2, 1, 2), row(3, 1, 3)]);
        let bogus = archive([row(14, 1, 14)]);
        let forged_left = Blob::with_handle(left.bytes.clone(), bogus.get_handle());
        let forged_right = Blob::with_handle(right.bytes.clone(), bogus.get_handle());
        let (low, high) = ordered_inputs(&left, &right);
        let expected_result = join(low, high).unwrap();
        let expected_merge = CollectionMerge::new(
            definition.id(),
            data(low),
            data(high),
            data(&expected_result),
        );
        let definition_blob = CollectionDefinition::to_blob(&definition);
        let record_blob = CollectionMerge::to_blob(&expected_merge);
        let sequence = vec![
            put_event(&definition_blob),
            put_event(low),
            put_event(high),
            put_event(&expected_result),
            ProbeEvent::Flush,
            put_event(&record_blob),
            ProbeEvent::Flush,
        ];

        let mut store = ProbeStore::default();
        let first = publish_merge(&mut store, &definition, &forged_right, &forged_left).unwrap();
        let second = publish_merge(&mut store, &definition, &forged_left, &forged_right).unwrap();

        assert_eq!(first, (expected_merge.clone(), expected_result.clone()));
        assert_eq!(second, (expected_merge.clone(), expected_result.clone()));
        validate_merge(&definition, &first.0, low, high, &first.1).unwrap();

        let mut expected_events = sequence.clone();
        expected_events.extend(sequence);
        assert_eq!(store.events, expected_events);
        let expected_handles = BTreeSet::from([
            definition_blob.get_handle().raw,
            low.get_handle().raw,
            high.get_handle().raw,
            expected_result.get_handle().raw,
            record_blob.get_handle().raw,
        ]);
        assert_eq!(store.known, expected_handles);
        assert_eq!(store.durable, expected_handles);
        assert!(store.pending.is_empty());
        assert!(!store.known.contains(&bogus.get_handle().raw));
    }

    #[test]
    fn commit_publication_orders_completed_prefixes_and_replays_after_recovery() {
        let (definition, data_blob, metadata, signing_key, expected) = commit_fixture();
        let definition_blob = CollectionDefinition::to_blob(&definition);
        let record_blob = CollectionCommit::to_blob(&expected);
        let dependencies = BTreeSet::from([
            definition_blob.get_handle().raw,
            data_blob.get_handle().raw,
            metadata.get_handle().raw,
        ]);

        for fail_at in 1..=6 {
            let mut store = ProbeStore::failing_before_effect_at(fail_at);
            let error =
                publish_commit(&mut store, &definition, &data_blob, &metadata, &signing_key)
                    .unwrap_err();
            match (fail_at, error) {
                (1..=3, PublicationError::DependencyPut(ProbeFailure(at)))
                | (4, PublicationError::DependencyFlush(ProbeFailure(at)))
                | (5, PublicationError::RecordPut(ProbeFailure(at)))
                | (6, PublicationError::RecordFlush(ProbeFailure(at))) => {
                    assert_eq!(at, fail_at)
                }
                (_, error) => panic!("unexpected publication error: {error}"),
            }

            assert!(!store.durable.contains(&record_blob.get_handle().raw));
            if fail_at <= 4 {
                assert!(!store
                    .events
                    .contains(&ProbeEvent::Put(record_blob.get_handle().raw)));
            } else {
                assert_eq!(store.events[3], ProbeEvent::Flush);
                assert!(dependencies.is_subset(&store.durable));
            }

            store.recover();
            let retried =
                publish_commit(&mut store, &definition, &data_blob, &metadata, &signing_key)
                    .unwrap();
            assert_eq!(retried, expected);
            assert!(dependencies.is_subset(&store.durable));
            assert!(store.durable.contains(&record_blob.get_handle().raw));
        }
    }

    #[test]
    fn merge_publication_orders_completed_prefixes_and_replays_after_recovery() {
        let definition = definition(id(1));
        let left = archive([row(1, 1, 1), row(3, 1, 3)]);
        let right = archive([row(2, 1, 2), row(3, 1, 3)]);
        let (low, high) = ordered_inputs(&left, &right);
        let result = join(low, high).unwrap();
        let expected = CollectionMerge::new(definition.id(), data(low), data(high), data(&result));
        let definition_blob = CollectionDefinition::to_blob(&definition);
        let record_blob = CollectionMerge::to_blob(&expected);
        let dependencies = BTreeSet::from([
            definition_blob.get_handle().raw,
            low.get_handle().raw,
            high.get_handle().raw,
            result.get_handle().raw,
        ]);

        for fail_at in 1..=7 {
            let mut store = ProbeStore::failing_before_effect_at(fail_at);
            let error = publish_merge(&mut store, &definition, &left, &right).unwrap_err();
            match (fail_at, error) {
                (1..=4, PublicationError::DependencyPut(ProbeFailure(at)))
                | (5, PublicationError::DependencyFlush(ProbeFailure(at)))
                | (6, PublicationError::RecordPut(ProbeFailure(at)))
                | (7, PublicationError::RecordFlush(ProbeFailure(at))) => {
                    assert_eq!(at, fail_at)
                }
                (_, error) => panic!("unexpected publication error: {error}"),
            }

            assert!(!store.durable.contains(&record_blob.get_handle().raw));
            if fail_at <= 5 {
                assert!(!store
                    .events
                    .contains(&ProbeEvent::Put(record_blob.get_handle().raw)));
            } else {
                assert_eq!(store.events[4], ProbeEvent::Flush);
                assert!(dependencies.is_subset(&store.durable));
            }

            store.recover();
            let retried = publish_merge(&mut store, &definition, &left, &right).unwrap();
            assert_eq!(retried, (expected.clone(), result.clone()));
            assert!(dependencies.is_subset(&store.durable));
            assert!(store.durable.contains(&record_blob.get_handle().raw));
        }
    }

    #[test]
    fn publication_rejects_every_invalid_input_before_writing() {
        let (definition, data_blob, metadata, signing_key, _) = commit_fixture();
        let mut store = ProbeStore::default();
        let wrong_definition =
            CollectionDefinition::new(definition.scope(), id(8), TRIBLE_SET_UNION_RECIPE_V1);
        assert!(matches!(
            publish_commit(
                &mut store,
                &wrong_definition,
                &data_blob,
                &metadata,
                &signing_key,
            ),
            Err(PublicationError::Validation(
                SimpleArchiveUnionValidationError::WrongRepresentation { .. }
            ))
        ));
        assert!(store.events.is_empty());

        let invalid_data = raw_archive(vec![row(2, 1, 2), row(1, 1, 1)]);
        assert!(matches!(
            publish_commit(
                &mut store,
                &definition,
                &invalid_data,
                &metadata,
                &signing_key,
            ),
            Err(PublicationError::Validation(
                SimpleArchiveUnionValidationError::InvalidElement { .. }
            ))
        ));
        assert!(store.events.is_empty());

        let invalid_metadata = raw_archive(vec![row(4, 1, 4), row(3, 1, 3)]);
        assert!(matches!(
            publish_commit(
                &mut store,
                &definition,
                &data_blob,
                &invalid_metadata,
                &signing_key,
            ),
            Err(PublicationError::InvalidMetadata(
                UnarchiveError::BadCanonicalizationOrdering
            ))
        ));
        assert!(store.events.is_empty());

        assert!(matches!(
            publish_merge(&mut store, &definition, &invalid_data, &data_blob,),
            Err(PublicationError::Validation(
                SimpleArchiveUnionValidationError::InvalidElement { .. }
            ))
        ));
        assert!(store.events.is_empty());
    }

    #[test]
    fn pile_publication_roundtrips_through_discovery_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("collections.pile");
        std::fs::File::create(&path).unwrap();

        let definition = definition(id(1));
        let left = archive([row(1, 1, 1), row(3, 1, 3)]);
        let right = archive([row(2, 1, 2), row(3, 1, 3)]);
        let metadata = archive([row(9, 1, 9)]);
        let signing_key = SigningKey::from_bytes(&[7; 32]);

        let (commit, merge, result) = {
            let mut pile = Pile::open(&path).unwrap();
            let commit =
                publish_commit(&mut pile, &definition, &left, &metadata, &signing_key).unwrap();
            let (merge, result) = publish_merge(&mut pile, &definition, &right, &left).unwrap();
            pile.close().unwrap();
            (commit, merge, result)
        };

        let mut reopened = Pile::open(&path).unwrap();
        let reader = reopened.reader().unwrap();
        let discovered = discover_collection_records(&reader).unwrap();
        assert_eq!(discovered.definitions(), &[definition.clone()]);
        assert_eq!(discovered.commits(), &[commit.clone()]);
        assert_eq!(discovered.merges(), &[merge.clone()]);
        assert!(discovered.derives().is_empty());
        assert!(discovered.diagnostics().is_empty());

        let fetched_left: Blob<SimpleArchive> = reader.get(left.get_handle()).unwrap();
        let fetched_right: Blob<SimpleArchive> = reader.get(right.get_handle()).unwrap();
        let fetched_metadata: Blob<SimpleArchive> = reader.get(metadata.get_handle()).unwrap();
        let fetched_result: Blob<SimpleArchive> = reader.get(result.get_handle()).unwrap();
        assert_eq!(fetched_left, left);
        assert_eq!(fetched_right, right);
        assert_eq!(fetched_metadata, metadata);
        assert_eq!(fetched_result, result);
        validate_commit(&definition, &commit, &fetched_left).unwrap();
        let (low, high) = ordered_inputs(&fetched_left, &fetched_right);
        validate_merge(&definition, &merge, low, high, &fetched_result).unwrap();

        drop(reader);
        reopened.close().unwrap();
    }

    #[test]
    fn definition_and_empty_element_are_golden() {
        let definition = definition(id(1));
        assert_eq!(
            <SimpleArchive as MetaDescribe>::id(),
            id_hex!("8F4A27C8581DADCBA1ADA8BA228069B6")
        );
        assert_eq!(
            TRIBLE_SET_UNION_RECIPE_V1,
            id_hex!("6D64C5F4B9E9B73F57C5F8702AB7FE45")
        );
        assert_eq!(definition.scope(), id(1));
        assert_eq!(definition.id(), id_hex!("4B6F24A289B950F2CF20896EAB7A1658"));
        assert_eq!(
            CollectionDefinition::to_blob(&definition).get_handle().raw,
            hex!("A639BFB1D8F4DD5E9AF4667512A23673812866F2CBF01D3F11DEF89850FA65B9")
        );

        let empty: Blob<SimpleArchive> = TribleSet::new().to_blob();
        validate_element(&empty).unwrap();
        assert!(empty.bytes.is_empty());
        assert_eq!(
            empty.get_handle().raw,
            hex!("AF1349B9F5F9A1A6A0404DEA36DCC9499BCB25C9ADC112B7CC9A93CAE41F3262")
        );
    }

    #[test]
    fn element_validation_matches_simplearchive_canonical_rules() {
        let first = row(1, 1, 1);
        let second = row(2, 1, 2);
        validate_element(&raw_archive(vec![first, second])).unwrap();
        assert_eq!(
            validate_element(&Blob::new(vec![0_u8; TRIBLE_LEN - 1].into())),
            Err(UnarchiveError::BadArchive)
        );

        let mut nil_entity = first;
        nil_entity[..16].fill(0);
        assert_eq!(
            validate_element(&raw_archive(vec![nil_entity])),
            Err(UnarchiveError::BadTrible)
        );
        assert_eq!(
            validate_element(&raw_archive(vec![first, first])),
            Err(UnarchiveError::BadCanonicalizationRedundancy)
        );
        assert_eq!(
            validate_element(&raw_archive(vec![second, first])),
            Err(UnarchiveError::BadCanonicalizationOrdering)
        );
    }

    #[test]
    fn join_obeys_empty_idempotent_commutative_and_associative_laws() {
        let empty = archive([]);
        let a = archive([row(1, 1, 1), row(3, 1, 3)]);
        let b = archive([row(2, 1, 2), row(3, 1, 3)]);
        let c = archive([row(1, 2, 4), row(4, 1, 5)]);

        assert_eq!(join(&empty, &a).unwrap(), a);
        assert_eq!(join(&a, &empty).unwrap(), a);
        assert_eq!(join(&a, &a).unwrap(), a);
        assert_eq!(join(&a, &b).unwrap(), join(&b, &a).unwrap());

        let forged = Blob::with_handle(a.bytes.clone(), empty.get_handle());
        assert_ne!(forged.get_handle().raw, data(&forged).raw);
        let normalized = join(&forged, &empty).unwrap();
        assert_eq!(normalized.bytes, a.bytes);
        assert_eq!(normalized.get_handle().raw, data(&normalized).raw);

        let left_associated = join(&join(&a, &b).unwrap(), &c).unwrap();
        let right_associated = join(&a, &join(&b, &c).unwrap()).unwrap();
        assert_eq!(left_associated, right_associated);
        assert_eq!(left_associated.bytes.len(), 5 * TRIBLE_LEN);
    }

    #[test]
    fn commit_validation_binds_definition_collection_handle_and_bytes() {
        let definition = definition(id(1));
        let blob = archive([row(1, 1, 1)]);
        let commit = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            definition.id(),
            data(&blob),
            empty_metadata_handle(),
        );
        validate_commit(&definition, &commit, &blob).unwrap();

        let wrong_representation =
            CollectionDefinition::new(definition.scope(), id(9), TRIBLE_SET_UNION_RECIPE_V1);
        assert!(matches!(
            validate_commit(&wrong_representation, &commit, &blob),
            Err(SimpleArchiveUnionValidationError::WrongRepresentation { .. })
        ));

        let wrong_recipe = CollectionDefinition::new(
            definition.scope(),
            <SimpleArchive as MetaDescribe>::id(),
            id(9),
        );
        assert!(matches!(
            validate_commit(&wrong_recipe, &commit, &blob),
            Err(SimpleArchiveUnionValidationError::WrongRecipe { .. })
        ));

        let other_definition = super::definition(id(2));
        assert_eq!(
            validate_commit(&other_definition, &commit, &blob),
            Err(SimpleArchiveUnionValidationError::WrongCollection {
                expected: other_definition.id(),
                actual: definition.id(),
            })
        );

        let other_blob = archive([row(2, 1, 2)]);
        assert!(matches!(
            validate_commit(&definition, &commit, &other_blob),
            Err(SimpleArchiveUnionValidationError::EndpointMismatch {
                role: ElementRole::CommitData,
                ..
            })
        ));

        let forged = Blob::with_handle(other_blob.bytes.clone(), blob.get_handle());
        assert_eq!(
            validate_commit(&definition, &commit, &forged),
            Err(SimpleArchiveUnionValidationError::EndpointMismatch {
                role: ElementRole::CommitData,
                expected: data(&blob),
                actual: data(&other_blob),
            })
        );

        let invalid = raw_archive(vec![row(2, 1, 2), row(1, 1, 1)]);
        let invalid_commit = CollectionCommit::sign(
            &SigningKey::from_bytes(&[7; 32]),
            definition.id(),
            data(&invalid),
            empty_metadata_handle(),
        );
        assert_eq!(
            validate_commit(&definition, &invalid_commit, &invalid),
            Err(SimpleArchiveUnionValidationError::InvalidElement {
                role: ElementRole::CommitData,
                source: UnarchiveError::BadCanonicalizationOrdering,
            })
        );
    }

    #[test]
    fn merge_validation_is_exact_and_binds_every_endpoint() {
        let definition = definition(id(1));
        let left = archive([row(1, 1, 1), row(3, 1, 3)]);
        let right = archive([row(2, 1, 2), row(3, 1, 3)]);
        let result = join(&left, &right).unwrap();
        let claim = CollectionMerge::new(definition.id(), data(&left), data(&right), data(&result));
        let (low, high) = ordered_inputs(&left, &right);
        validate_merge(&definition, &claim, low, high, &result).unwrap();

        let wrong_collection = CollectionMerge::new(id(9), data(low), data(high), data(&result));
        assert!(matches!(
            validate_merge(&definition, &wrong_collection, low, high, &result),
            Err(SimpleArchiveUnionValidationError::WrongCollection { .. })
        ));

        assert!(matches!(
            validate_merge(&definition, &claim, high, low, &result),
            Err(SimpleArchiveUnionValidationError::EndpointMismatch {
                role: ElementRole::MergeLow,
                ..
            })
        ));

        let forged_high = Blob::with_handle(low.bytes.clone(), high.get_handle());
        assert_eq!(
            validate_merge(&definition, &claim, low, &forged_high, &result),
            Err(SimpleArchiveUnionValidationError::EndpointMismatch {
                role: ElementRole::MergeHigh,
                expected: data(high),
                actual: data(low),
            })
        );

        let other_result = archive([row(4, 1, 4)]);
        assert!(matches!(
            validate_merge(&definition, &claim, low, high, &other_result),
            Err(SimpleArchiveUnionValidationError::EndpointMismatch {
                role: ElementRole::MergeResult,
                ..
            })
        ));

        let wrong_result = archive([row(1, 1, 1), row(2, 1, 2)]);
        let wrong_claim =
            CollectionMerge::new(definition.id(), data(low), data(high), data(&wrong_result));
        assert_eq!(
            validate_merge(&definition, &wrong_claim, low, high, &wrong_result),
            Err(SimpleArchiveUnionValidationError::WrongMergeResult)
        );

        let invalid_result = raw_archive(vec![row(2, 1, 2), row(1, 1, 1)]);
        let invalid_claim = CollectionMerge::new(
            definition.id(),
            data(low),
            data(high),
            data(&invalid_result),
        );
        assert_eq!(
            validate_merge(&definition, &invalid_claim, low, high, &invalid_result),
            Err(SimpleArchiveUnionValidationError::InvalidElement {
                role: ElementRole::MergeResult,
                source: UnarchiveError::BadCanonicalizationOrdering,
            })
        );
    }

    #[cfg(feature = "proptest")]
    mod property_tests {
        use super::*;

        use proptest::collection::vec;
        use proptest::prelude::*;

        fn arb_trible() -> impl Strategy<Value = Trible> {
            (
                prop::array::uniform16(1_u8..=255),
                prop::array::uniform16(1_u8..=255),
                prop::array::uniform32(any::<u8>()),
            )
                .prop_map(|(entity, attribute, value)| {
                    let mut raw = [0; TRIBLE_LEN];
                    raw[..16].copy_from_slice(&entity);
                    raw[16..32].copy_from_slice(&attribute);
                    raw[32..].copy_from_slice(&value);
                    Trible::force_raw(raw).unwrap()
                })
        }

        fn arb_set(max: usize) -> impl Strategy<Value = TribleSet> {
            vec(arb_trible(), 0..max).prop_map(|tribles| {
                let mut set = TribleSet::new();
                for trible in &tribles {
                    set.insert(trible);
                }
                set
            })
        }

        proptest! {
            #[test]
            fn direct_union_matches_the_patch_oracle(
                left in arb_set(64),
                right in arb_set(64),
            ) {
                let expected: Blob<SimpleArchive> = (left.clone() + right.clone()).to_blob();
                let left: Blob<SimpleArchive> = left.to_blob();
                let right: Blob<SimpleArchive> = right.to_blob();
                let actual = join(&left, &right).unwrap();

                prop_assert_eq!(&actual, &expected);
                let collection = definition(id(1));
                let claim = CollectionMerge::new(
                    collection.id(),
                    data(&left),
                    data(&right),
                    data(&actual),
                );
                let (low, high) = ordered_inputs(&left, &right);
                prop_assert!(validate_merge(&collection, &claim, low, high, &actual).is_ok());
                prop_assert_eq!(actual, join(&right, &left).unwrap());
            }

            #[test]
            fn direct_union_obeys_identity_and_aci(
                a in arb_set(32),
                b in arb_set(32),
                c in arb_set(32),
            ) {
                let empty: Blob<SimpleArchive> = TribleSet::new().to_blob();
                let a: Blob<SimpleArchive> = a.to_blob();
                let b: Blob<SimpleArchive> = b.to_blob();
                let c: Blob<SimpleArchive> = c.to_blob();

                prop_assert_eq!(join(&empty, &a).unwrap(), a.clone());
                prop_assert_eq!(join(&a, &empty).unwrap(), a.clone());
                prop_assert_eq!(join(&a, &a).unwrap(), a.clone());
                prop_assert_eq!(join(&a, &b).unwrap(), join(&b, &a).unwrap());

                let left_associated = join(&join(&a, &b).unwrap(), &c).unwrap();
                let right_associated = join(&a, &join(&b, &c).unwrap()).unwrap();
                prop_assert_eq!(left_associated, right_associated);
            }
        }
    }
}
