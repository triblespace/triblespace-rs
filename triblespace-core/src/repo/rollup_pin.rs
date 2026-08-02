//! Grow-only branch rollup records as one typed asserted pin.
//!
//! A rollup pin is identified by a source branch and an index recipe. The
//! source branch's author occupies the generic [`PinIdentity`] author slot;
//! the typed descriptor contains the source [`BranchPinDescriptor`] handle and
//! recipe id. Its assertion values are exact standalone range-record
//! [`SimpleArchive`] handles.
//!
//! The typed descriptor is wrapped in [`StrongPinDescriptor`]. This makes the
//! small range-record archives part of hard retention while allowing their
//! aligned artifact handles to become explicit weak-pin boundaries. The pin is
//! a grow-only set: labels are fixed signed padding and have no ordering or
//! subsumption meaning in this module.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use super::branch_pin::{BranchIdentity, BranchPinDescriptor};
use super::pin_assertion::{
    PinAssertion, PinAssertionSnapshot, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
};
use super::strong_pin::StrongPinDescriptor;
use crate::blob::encodings::longstring::LongString;
use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::{Blob, BlobEncoding, TryFromBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::Inline;
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};
use crate::trible::Fragment;

/// Canonical byte length of a V1 rollup-pin descriptor.
pub const ROLLUP_PIN_DESCRIPTOR_LEN: usize = 16 + 16 + 32 + 16;

/// Kind/schema marker for the V1 rollup-pin descriptor.
///
/// Minted with `trible genid` on 2026-08-03.
pub const ROLLUP_PIN_DESCRIPTOR_V1: [u8; 16] = hex!("0E1295F9D56242186CA30D3B7BB010A9");

/// Blob encoding for one branch-and-recipe rollup G-set.
///
/// The canonical bytes are `kind marker [16] | zero padding [16] | source
/// BranchPinDescriptor handle [32] | recipe id [16]`. The padding aligns the
/// source descriptor handle for conservative closure discovery. The generic
/// pin is a [`StrongPinDescriptor`] around this descriptor's content handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollupPinDescriptor;

impl BlobEncoding for RollupPinDescriptor {}

impl MetaDescribe for RollupPinDescriptor {
    fn describe() -> Fragment {
        let id: Id = id_hex!("0E1295F9D56242186CA30D3B7BB010A9");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "rollup-pin-descriptor-v1",
                metadata::description: "Canonical inner descriptor for one asserted branch rollup G-set: a V1 kind marker, sixteen zero padding bytes, the aligned BranchPinDescriptor handle of the source branch, and the index recipe id. A StrongPinDescriptor wraps its content handle so standalone range-record values remain hard while weak-pinned derived artifacts remain evictable.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

/// The typed content decoded from a canonical rollup-pin descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RollupPinKey {
    source_branch: Inline<Handle<BranchPinDescriptor>>,
    recipe: Id,
}

impl RollupPinKey {
    /// Exact inner descriptor handle of the source branch.
    pub const fn source_branch(&self) -> Inline<Handle<BranchPinDescriptor>> {
        self.source_branch
    }

    /// Index recipe whose range records form this rollup set.
    pub const fn recipe(&self) -> Id {
        self.recipe
    }
}

impl RollupPinDescriptor {
    /// Encode one source branch name and recipe into canonical descriptor bytes.
    pub fn encode(
        source_name: Inline<Handle<LongString>>,
        recipe: Id,
    ) -> [u8; ROLLUP_PIN_DESCRIPTOR_LEN] {
        let mut raw = [0u8; ROLLUP_PIN_DESCRIPTOR_LEN];
        raw[..16].copy_from_slice(&ROLLUP_PIN_DESCRIPTOR_V1);
        raw[32..64].copy_from_slice(&BranchPinDescriptor::descriptor_handle(source_name).raw);
        raw[64..].copy_from_slice(&recipe.raw());
        raw
    }

    /// Build the typed descriptor blob staged beside a rollup assertion.
    pub fn blob(source_name: Inline<Handle<LongString>>, recipe: Id) -> Blob<Self> {
        Blob::new(Bytes::from_source(
            Self::encode(source_name, recipe).to_vec(),
        ))
    }

    /// Derive this inner descriptor's exact content handle.
    pub fn descriptor_handle(
        source_name: Inline<Handle<LongString>>,
        recipe: Id,
    ) -> Inline<Handle<Self>> {
        Inline::new(Blake3::digest(&Self::encode(source_name, recipe)))
    }

    /// Build the outer strong-retention descriptor staged before publication.
    pub fn strong_blob(
        source_name: Inline<Handle<LongString>>,
        recipe: Id,
    ) -> Blob<StrongPinDescriptor> {
        StrongPinDescriptor::blob(Self::descriptor_handle(source_name, recipe))
    }

    /// Derive the generic outer pin handle without loading either descriptor.
    pub fn pin_handle(source_name: Inline<Handle<LongString>>, recipe: Id) -> PinHandle {
        StrongPinDescriptor::pin_handle(Self::descriptor_handle(source_name, recipe))
    }

    /// Derive the generic identity for one source branch and recipe.
    ///
    /// The author is deliberately the source branch's author. It stays in the
    /// generic envelope rather than being duplicated in descriptor content.
    pub fn pin_identity(
        author: VerifyingKey,
        source_name: Inline<Handle<LongString>>,
        recipe: Id,
    ) -> PinIdentity {
        PinIdentity::new(author, Self::pin_handle(source_name, recipe))
    }
}

impl TryFromBlob<RollupPinDescriptor> for RollupPinKey {
    type Error = RollupPinDescriptorError;

    fn try_from_blob(blob: Blob<RollupPinDescriptor>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != ROLLUP_PIN_DESCRIPTOR_LEN {
            return Err(RollupPinDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes[..16] != ROLLUP_PIN_DESCRIPTOR_V1 {
            return Err(RollupPinDescriptorError::WrongKind);
        }
        if bytes[16..32].iter().any(|byte| *byte != 0) {
            return Err(RollupPinDescriptorError::NonZeroReserved);
        }
        let source_branch =
            Inline::new(bytes[32..64].try_into().expect("descriptor length checked"));
        let recipe = Id::new(bytes[64..80].try_into().expect("descriptor length checked"))
            .ok_or(RollupPinDescriptorError::NilRecipe)?;
        Ok(Self {
            source_branch,
            recipe,
        })
    }
}

/// A rollup-pin descriptor was not the one exact canonical V1 shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollupPinDescriptorError {
    /// The descriptor was not exactly 80 bytes.
    WrongLength { actual: usize },
    /// The descriptor did not carry the rollup V1 kind marker.
    WrongKind,
    /// Reserved alignment bytes were not all canonical zeroes.
    NonZeroReserved,
    /// The recipe bytes encoded the reserved nil id.
    NilRecipe,
}

impl fmt::Display for RollupPinDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "rollup pin descriptor is {actual} bytes, expected {ROLLUP_PIN_DESCRIPTOR_LEN}"
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 rollup descriptor"),
            Self::NonZeroReserved => {
                write!(f, "rollup pin descriptor has non-zero reserved bytes")
            }
            Self::NilRecipe => write!(f, "rollup pin descriptor has the reserved nil recipe id"),
        }
    }
}

impl Error for RollupPinDescriptorError {}

fn canonical_label() -> SubsumptionLabel {
    // Signed padding, not a neutral element. Rollup projection never compares
    // labels, so the kind takes zero label-based skips.
    SubsumptionLabel::from_raw([0u8; 32])
}

fn value_from_range_record(record: Inline<Handle<SimpleArchive>>) -> ValueHandle {
    ValueHandle::from_raw(record.raw)
}

fn range_record_from_value(value: ValueHandle) -> Inline<Handle<SimpleArchive>> {
    Inline::new(value.raw())
}

/// Sign one grow-only rollup record for the signing author's source branch.
pub fn sign_rollup_record(
    key: &SigningKey,
    source_name: Inline<Handle<LongString>>,
    recipe: Id,
    range_record: Inline<Handle<SimpleArchive>>,
) -> PinAssertion {
    PinAssertion::sign(
        key,
        RollupPinDescriptor::pin_handle(source_name, recipe),
        value_from_range_record(range_record),
        canonical_label(),
    )
}

/// Project one source branch and recipe's exact range-record handle set.
///
/// Values are deduplicated even if redundant valid assertions carry different
/// opaque labels. Other authors, source branches, and recipes remain in the
/// generic snapshot but do not enter this typed view.
pub fn rollup_records_in_snapshot(
    snapshot: &PinAssertionSnapshot,
    source: &BranchIdentity,
    recipe: Id,
) -> BTreeSet<Inline<Handle<SimpleArchive>>> {
    snapshot
        .for_pin(&RollupPinDescriptor::pin_identity(
            source.author(),
            source.name(),
            recipe,
        ))
        .into_iter()
        .map(|assertion| range_record_from_value(assertion.value()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn name(byte: u8) -> Inline<Handle<LongString>> {
        Inline::new([byte; 32])
    }

    fn recipe(byte: u8) -> Id {
        Id::new([byte; 16]).expect("test recipe is nonzero")
    }

    fn record(byte: u8) -> Inline<Handle<SimpleArchive>> {
        Inline::new([byte; 32])
    }

    #[test]
    fn descriptor_is_canonical_typed_content_and_roundtrips() {
        let source_name = name(7);
        let recipe = recipe(11);
        let blob = RollupPinDescriptor::blob(source_name, recipe);
        let inner = blob.get_handle();
        let outer = RollupPinDescriptor::strong_blob(source_name, recipe);

        assert_eq!(blob.bytes.len(), ROLLUP_PIN_DESCRIPTOR_LEN);
        assert_eq!(&blob.bytes[..16], &ROLLUP_PIN_DESCRIPTOR_V1);
        assert_eq!(&blob.bytes[16..32], &[0u8; 16]);
        assert_eq!(
            &blob.bytes[32..64],
            &BranchPinDescriptor::descriptor_handle(source_name).raw
        );
        assert_eq!(&blob.bytes[64..], &recipe.raw());
        assert_eq!(
            RollupPinDescriptor::descriptor_handle(source_name, recipe),
            inner
        );
        assert_eq!(
            RollupPinDescriptor::pin_handle(source_name, recipe).raw(),
            outer.get_handle().raw
        );

        let decoded: RollupPinKey = blob.try_from_blob().unwrap();
        assert_eq!(
            decoded.source_branch(),
            BranchPinDescriptor::descriptor_handle(source_name)
        );
        assert_eq!(decoded.recipe(), recipe);
    }

    #[test]
    fn author_branch_and_recipe_each_separate_pin_identity() {
        let first_author = key(1).verifying_key();
        let second_author = key(2).verifying_key();
        let first_name = name(3);
        let second_name = name(4);
        let first_recipe = recipe(5);
        let second_recipe = recipe(6);

        let base = RollupPinDescriptor::pin_identity(first_author, first_name, first_recipe);
        let other_author =
            RollupPinDescriptor::pin_identity(second_author, first_name, first_recipe);
        let other_branch =
            RollupPinDescriptor::pin_identity(first_author, second_name, first_recipe);
        let other_recipe =
            RollupPinDescriptor::pin_identity(first_author, first_name, second_recipe);

        assert_ne!(base, other_author);
        assert_ne!(base, other_branch);
        assert_ne!(base, other_recipe);
        assert_eq!(
            RollupPinDescriptor::descriptor_handle(first_name, first_recipe),
            RollupPinDescriptor::descriptor_handle(first_name, first_recipe),
            "descriptor derivation is deterministic"
        );
        assert_eq!(
            base.pin(),
            other_author.pin(),
            "the author belongs in generic PinIdentity, not descriptor content"
        );
    }

    #[test]
    fn projection_is_an_idempotent_grow_only_set_for_one_exact_key() {
        let author = key(1);
        let source = BranchIdentity::new(author.verifying_key(), name(3));
        let recipe_id = recipe(5);
        let first = record(7);
        let second = record(8);
        let mut snapshot = PinAssertionSnapshot::new();

        let duplicate = sign_rollup_record(&author, source.name(), recipe_id, first);
        snapshot.insert(duplicate).unwrap();
        snapshot.insert(duplicate).unwrap();
        snapshot
            .insert(sign_rollup_record(
                &author,
                source.name(),
                recipe_id,
                second,
            ))
            .unwrap();

        // Labels are opaque signed padding for this kind. Even a redundant
        // generic witness with another label projects to the same set value.
        snapshot
            .insert(PinAssertion::sign(
                &author,
                RollupPinDescriptor::pin_handle(source.name(), recipe_id),
                value_from_range_record(first),
                SubsumptionLabel::from_raw([9; 32]),
            ))
            .unwrap();

        snapshot
            .insert(sign_rollup_record(&author, name(4), recipe_id, record(10)))
            .unwrap();
        snapshot
            .insert(sign_rollup_record(
                &author,
                source.name(),
                recipe(6),
                record(11),
            ))
            .unwrap();
        snapshot
            .insert(sign_rollup_record(
                &key(2),
                source.name(),
                recipe_id,
                record(12),
            ))
            .unwrap();

        assert_eq!(
            rollup_records_in_snapshot(&snapshot, &source, recipe_id),
            BTreeSet::from([first, second])
        );
    }

    #[test]
    fn descriptor_rejects_every_noncanonical_shape() {
        let source_name = name(3);
        let recipe = recipe(5);

        let err = Blob::<RollupPinDescriptor>::new(Bytes::from_source(vec![0u8; 79]))
            .try_from_blob::<RollupPinKey>()
            .unwrap_err();
        assert_eq!(err, RollupPinDescriptorError::WrongLength { actual: 79 });

        let mut wrong_kind = RollupPinDescriptor::encode(source_name, recipe);
        wrong_kind[0] ^= 1;
        let err = Blob::<RollupPinDescriptor>::new(Bytes::from_source(wrong_kind.to_vec()))
            .try_from_blob::<RollupPinKey>()
            .unwrap_err();
        assert_eq!(err, RollupPinDescriptorError::WrongKind);

        let mut nonzero_reserved = RollupPinDescriptor::encode(source_name, recipe);
        nonzero_reserved[16] = 1;
        let err = Blob::<RollupPinDescriptor>::new(Bytes::from_source(nonzero_reserved.to_vec()))
            .try_from_blob::<RollupPinKey>()
            .unwrap_err();
        assert_eq!(err, RollupPinDescriptorError::NonZeroReserved);

        let mut nil_recipe = RollupPinDescriptor::encode(source_name, recipe);
        nil_recipe[64..].fill(0);
        let err = Blob::<RollupPinDescriptor>::new(Bytes::from_source(nil_recipe.to_vec()))
            .try_from_blob::<RollupPinKey>()
            .unwrap_err();
        assert_eq!(err, RollupPinDescriptorError::NilRecipe);
    }
}
