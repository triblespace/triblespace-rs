//! Grow-only branch rollup nodes as one typed asserted pin.
//!
//! A rollup pin is identified by a source branch and an index recipe. The
//! source branch's author occupies the generic [`PinIdentity`] author slot;
//! the typed descriptor contains the source [`BranchPinDescriptor`] handle and
//! recipe id. Each assertion atomically pairs two exact [`SimpleArchive`]
//! handles:
//!
//! - the assertion value is a core-only range record and stays hard;
//! - the opaque label is one complete artifact node for that range and stays
//!   outside generic hard reachability.
//!
//! The typed descriptor is wrapped in [`StrongPinDescriptor`]. This makes the
//! small coverage records part of hard retention without turning every
//! historical derived artifact into a permanent weak-pin demand. The label is
//! never ordered by this module: its bytes are a content handle, not a
//! subsumption clock.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use super::branch_pin::{BranchIdentity, BranchPinDescriptor};
use super::pin_assertion::{
    PinAssertion, PinAssertionId, PinAssertionSnapshot, PinAssertionStore, PinHandle, PinIdentity,
    SubsumptionLabel, ValueHandle,
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
use crate::repo::{BlobStorePut, StorageFlush};
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
                metadata::description: "Canonical inner descriptor for one asserted branch rollup G-set: a V1 kind marker, sixteen zero padding bytes, the aligned BranchPinDescriptor handle of the source branch, and the index recipe id. A StrongPinDescriptor wraps its content handle so core-only range-record assertion values remain hard; each opaque assertion label names one complete unowned artifact node.",
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

fn value_from_range_record(record: Inline<Handle<SimpleArchive>>) -> ValueHandle {
    ValueHandle::from_raw(record.raw)
}

fn range_record_from_value(value: ValueHandle) -> Inline<Handle<SimpleArchive>> {
    Inline::new(value.raw())
}

fn label_from_node(node: Inline<Handle<SimpleArchive>>) -> SubsumptionLabel {
    SubsumptionLabel::from_raw(node.raw)
}

fn node_from_label(label: SubsumptionLabel) -> Inline<Handle<SimpleArchive>> {
    Inline::new(label.raw())
}

/// One atomic rollup alternative over a fixed logical source range.
///
/// `range_record` names the canonical core-only [`RangeRecord`](super::index_range::RangeRecord)
/// archive. `node` names one complete standalone archive containing only the
/// conjunctive typed artifact components for the alternative, rooted at the
/// core's intrinsic entity; the signed pair supplies their association. The
/// range core is recipe-neutral: the surrounding pin descriptor supplies the
/// sole `(source branch, recipe)` partition. Distinct nodes may share one range
/// record; they remain disjunctive alternatives and must never be fact-unioned
/// merely because their intrinsic range entity is equal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RollupRecord {
    range_record: Inline<Handle<SimpleArchive>>,
    node: Inline<Handle<SimpleArchive>>,
}

impl RollupRecord {
    /// Pair one hard range core with one complete derived artifact node.
    pub const fn new(
        range_record: Inline<Handle<SimpleArchive>>,
        node: Inline<Handle<SimpleArchive>>,
    ) -> Self {
        Self { range_record, node }
    }

    /// Hard-retained core-only range record archive.
    pub const fn range_record(self) -> Inline<Handle<SimpleArchive>> {
        self.range_record
    }

    /// Unowned complete artifact-node archive.
    pub const fn node(self) -> Inline<Handle<SimpleArchive>> {
        self.node
    }
}

/// Sign one grow-only rollup alternative for the signing author's source branch.
///
/// The generic strong wrapper follows `range_record` because it is the
/// assertion value. It deliberately ignores `node`, carried in the opaque
/// label. Typed rollup resolution never compares labels.
pub fn sign_rollup_record(
    key: &SigningKey,
    source_name: Inline<Handle<LongString>>,
    recipe: Id,
    range_record: Inline<Handle<SimpleArchive>>,
    node: Inline<Handle<SimpleArchive>>,
) -> PinAssertion {
    PinAssertion::sign(
        key,
        RollupPinDescriptor::pin_handle(source_name, recipe),
        value_from_range_record(range_record),
        label_from_node(node),
    )
}

/// Failure while publishing one durable rollup alternative.
///
/// Descriptor uploads are distinguished because either may fail before the
/// durability barrier. Once the barrier succeeds, the assertion append is the
/// only remaining fallible operation and is itself durable on success.
#[derive(Debug)]
pub enum PublishRollupRecordError<PutError, FlushError, AssertionError> {
    /// Storing the typed inner [`RollupPinDescriptor`] failed.
    RollupDescriptorPut(PutError),
    /// Storing the outer [`StrongPinDescriptor`] failed.
    StrongDescriptorPut(PutError),
    /// Making both descriptors durable failed.
    StorageFlush(FlushError),
    /// Durably appending the signed rollup assertion failed.
    AssertionStore(AssertionError),
}

impl<PutError, FlushError, AssertionError> fmt::Display
    for PublishRollupRecordError<PutError, FlushError, AssertionError>
where
    PutError: fmt::Display,
    FlushError: fmt::Display,
    AssertionError: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RollupDescriptorPut(error) => {
                write!(f, "failed to store rollup pin descriptor: {error}")
            }
            Self::StrongDescriptorPut(error) => {
                write!(f, "failed to store strong pin descriptor: {error}")
            }
            Self::StorageFlush(error) => {
                write!(f, "failed to make rollup descriptors durable: {error}")
            }
            Self::AssertionStore(error) => {
                write!(f, "failed to append rollup assertion: {error}")
            }
        }
    }
}

impl<PutError, FlushError, AssertionError> Error
    for PublishRollupRecordError<PutError, FlushError, AssertionError>
where
    PutError: Error + 'static,
    FlushError: Error + 'static,
    AssertionError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RollupDescriptorPut(error) | Self::StrongDescriptorPut(error) => Some(error),
            Self::StorageFlush(error) => Some(error),
            Self::AssertionStore(error) => Some(error),
        }
    }
}

/// Publish one rollup alternative after its pin descriptors are durable.
///
/// The range core and artifact node must already have been stored by the
/// caller. Publication stages the deterministic typed descriptor and its
/// strong-retention wrapper, crosses one durability barrier, then appends the
/// single signed `(range core, artifact node)` assertion. It writes no weak pin
/// and maintains no repair or replacement state.
pub fn publish_rollup_record<Storage>(
    storage: &mut Storage,
    key: &SigningKey,
    source_name: Inline<Handle<LongString>>,
    recipe: Id,
    record: RollupRecord,
) -> Result<
    PinAssertionId,
    PublishRollupRecordError<
        <Storage as BlobStorePut>::PutError,
        <Storage as StorageFlush>::Error,
        <Storage as PinAssertionStore>::Error,
    >,
>
where
    Storage: BlobStorePut + StorageFlush + PinAssertionStore,
{
    storage
        .put::<RollupPinDescriptor, _>(RollupPinDescriptor::blob(source_name, recipe))
        .map_err(PublishRollupRecordError::RollupDescriptorPut)?;
    storage
        .put::<StrongPinDescriptor, _>(RollupPinDescriptor::strong_blob(source_name, recipe))
        .map_err(PublishRollupRecordError::StrongDescriptorPut)?;
    storage
        .flush()
        .map_err(PublishRollupRecordError::StorageFlush)?;

    let assertion = sign_rollup_record(
        key,
        source_name,
        recipe,
        record.range_record(),
        record.node(),
    );
    let assertion_id = assertion.id();
    storage
        .append_pin_assertion(assertion)
        .map_err(PublishRollupRecordError::AssertionStore)?;
    Ok(assertion_id)
}

/// Project one source branch and recipe's exact rollup-alternative set.
///
/// Exact duplicate pairs are deduplicated. The same range core paired with two
/// node handles remains two alternatives. Other authors, source branches, and
/// recipes remain in the generic snapshot but do not enter this typed view.
pub fn rollup_records_in_snapshot(
    snapshot: &PinAssertionSnapshot,
    source: &BranchIdentity,
    recipe: Id,
) -> BTreeSet<RollupRecord> {
    snapshot
        .for_pin(&RollupPinDescriptor::pin_identity(
            source.author(),
            source.name(),
            recipe,
        ))
        .into_iter()
        .map(|assertion| {
            RollupRecord::new(
                range_record_from_value(assertion.value()),
                node_from_label(assertion.label()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;
    use std::convert::Infallible;

    use super::*;
    use crate::blob::IntoBlob;
    use crate::inline::InlineEncoding;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PublishEvent {
        PutRollupDescriptor([u8; 32]),
        PutStrongDescriptor([u8; 32]),
        Flush,
        AppendAssertion(PinAssertion),
    }

    struct PublishProbe {
        events: Vec<PublishEvent>,
        assertions: PinAssertionSnapshot,
    }

    impl PublishProbe {
        fn new() -> Self {
            Self {
                events: Vec::new(),
                assertions: PinAssertionSnapshot::new(),
            }
        }
    }

    impl BlobStorePut for PublishProbe {
        type PutError = Infallible;

        fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
        where
            S: BlobEncoding + 'static,
            T: IntoBlob<S>,
            Handle<S>: InlineEncoding,
        {
            let handle = item.to_blob().get_handle();
            let event = if TypeId::of::<S>() == TypeId::of::<RollupPinDescriptor>() {
                PublishEvent::PutRollupDescriptor(handle.raw)
            } else if TypeId::of::<S>() == TypeId::of::<StrongPinDescriptor>() {
                PublishEvent::PutStrongDescriptor(handle.raw)
            } else {
                panic!("publication wrote an unexpected blob encoding")
            };
            self.events.push(event);
            Ok(handle)
        }
    }

    impl StorageFlush for PublishProbe {
        type Error = Infallible;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.events.push(PublishEvent::Flush);
            Ok(())
        }
    }

    impl PinAssertionStore for PublishProbe {
        type Error = Infallible;

        fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
            Ok(self.assertions.clone())
        }

        fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
            self.events.push(PublishEvent::AppendAssertion(assertion));
            self.assertions
                .insert(assertion)
                .expect("one valid test assertion cannot collide");
            Ok(())
        }
    }

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
    fn publication_flushes_both_descriptors_before_the_projectable_assertion() {
        let author = key(1);
        let source_name = name(3);
        let source = BranchIdentity::new(author.verifying_key(), source_name);
        let recipe_id = recipe(5);
        let rollup = RollupRecord::new(record(7), record(11));
        let expected_assertion = sign_rollup_record(
            &author,
            source_name,
            recipe_id,
            rollup.range_record(),
            rollup.node(),
        );
        let mut probe = PublishProbe::new();

        let assertion_id =
            publish_rollup_record(&mut probe, &author, source_name, recipe_id, rollup).unwrap();

        assert_eq!(assertion_id, expected_assertion.id());
        assert_eq!(
            probe.events,
            vec![
                PublishEvent::PutRollupDescriptor(
                    RollupPinDescriptor::descriptor_handle(source_name, recipe_id).raw,
                ),
                PublishEvent::PutStrongDescriptor(
                    RollupPinDescriptor::strong_blob(source_name, recipe_id)
                        .get_handle()
                        .raw,
                ),
                PublishEvent::Flush,
                PublishEvent::AppendAssertion(expected_assertion),
            ]
        );
        assert_eq!(
            rollup_records_in_snapshot(&probe.assertions, &source, recipe_id),
            BTreeSet::from([rollup])
        );
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
        let first = RollupRecord::new(record(7), record(8));
        let alternative = RollupRecord::new(record(7), record(9));
        let second = RollupRecord::new(record(10), record(11));
        let mut snapshot = PinAssertionSnapshot::new();

        let duplicate = sign_rollup_record(
            &author,
            source.name(),
            recipe_id,
            first.range_record(),
            first.node(),
        );
        snapshot.insert(duplicate).unwrap();
        snapshot.insert(duplicate).unwrap();
        snapshot
            .insert(sign_rollup_record(
                &author,
                source.name(),
                recipe_id,
                alternative.range_record(),
                alternative.node(),
            ))
            .unwrap();
        snapshot
            .insert(sign_rollup_record(
                &author,
                source.name(),
                recipe_id,
                second.range_record(),
                second.node(),
            ))
            .unwrap();

        snapshot
            .insert(sign_rollup_record(
                &author,
                name(4),
                recipe_id,
                record(12),
                record(13),
            ))
            .unwrap();
        snapshot
            .insert(sign_rollup_record(
                &author,
                source.name(),
                recipe(6),
                record(14),
                record(15),
            ))
            .unwrap();
        snapshot
            .insert(sign_rollup_record(
                &key(2),
                source.name(),
                recipe_id,
                record(16),
                record(17),
            ))
            .unwrap();

        assert_eq!(
            rollup_records_in_snapshot(&snapshot, &source, recipe_id),
            BTreeSet::from([first, alternative, second])
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
