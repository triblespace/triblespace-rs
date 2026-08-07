//! Canonical TribleSet set union over [`SimpleArchive`] elements.
//!
//! This is the first concrete production collection kind. A collection pairs
//! an extrinsic scope with the existing `SimpleArchive` representation and the
//! [`TRIBLE_SET_UNION_RECIPE_V1`] semantic recipe. Every element is an exact,
//! canonical EAV-ordered stream of 64-byte tribles. Its join is ordinary set
//! union, so canonical output bytes and their Blake3 identity are associative,
//! commutative, and idempotent.
//!
//! Validation and joins operate directly on the canonical byte streams. They
//! deliberately do not construct [`crate::trible::TribleSet`] or PATCH indexes;
//! query-time decoding keeps its independently optimized path. Missing endpoint
//! blobs are likewise outside this module: callers defer an equation until its
//! three blobs are resident, then call [`validate_merge`].

use std::error::Error;
use std::fmt;

use anybytes::{Bytes, View};

use crate::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use crate::blob::Blob;
use crate::id::Id;
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Hash};
use crate::inline::Inline;
use crate::metadata::MetaDescribe;
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

    if left.bytes == right.bytes || right_rows.is_empty() {
        return Ok(Blob::new(left.bytes.clone()));
    }
    if left_rows.is_empty() {
        return Ok(Blob::new(right.bytes.clone()));
    }

    let mut rows = Vec::with_capacity(left_rows.len() + right_rows.len());
    rows.extend(UnionRows::new(&left_rows, &right_rows).copied());
    Ok(Blob::new(Bytes::from(rows)))
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

    use ed25519_dalek::SigningKey;
    use hex_literal::hex;

    use crate::blob::IntoBlob;
    use crate::collection::empty_metadata_handle;
    use crate::trible::TribleSet;

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
