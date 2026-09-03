//! Reading one collection descriptor.
//!
//! A descriptor is an ordinary [`TribleSet`]: the facts of one
//! [`entity!`](crate::macros::entity), stored as a
//! [`SimpleArchive`](crate::blob::encodings::simplearchive::SimpleArchive)
//! blob whose handle is the collection identity. The descriptor names its
//! encoding directly, and reading one is an ordinary query over ordinary
//! facts.
//!
//! A root carries [`collection_name`] while a derived collection carries
//! [`collection_source`] and one concrete [`collection_mapping`]
//! instance instead. Mapping parameters hang from that mapping entity, not
//! from the collection descriptor, so the conversion remains independently
//! identifiable and queryable. Both kinds carry independent, self-contained
//! READ and WRITE policy fragments. Each action is either open or governed by
//! a quorum over its own canonical root set; there is no privileged collection
//! owner or shared anchor. Policy is never inferred by walking the source
//! chain. Readers first locate the one tagged
//! descriptor entity, then bind
//! every field lookup to that exact entity so embedded descriptions cannot
//! accidentally satisfy descriptor shape.

use itertools::Itertools;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::utf8string::UTF8String;
use crate::blob::encodings::UnknownBlob;
use crate::blob::Blob;
use crate::id::Id;
use crate::inline::encodings::genid::GenId;
use crate::inline::encodings::hash::Handle;
use crate::inline::encodings::iu256::U256;
use crate::inline::{Inline, InlineEncoding, IntoInline, RawInline};
use crate::metadata::{self, MetaDescribe};
use crate::prelude::{entity, find, pattern};
use crate::query::TriblePattern;
use crate::repo::{BlobStorePut, SnapshotSource};
use crate::trible::{Fragment, TribleSet};

use super::policy::{
    AdmissionPolicy, CollectionPolicy, KIND_ADMISSION_POLICY_OPEN, KIND_ADMISSION_POLICY_QUORUM,
};
use super::records::{
    admission_delegate_threshold, admission_invoke_threshold, admission_policy_root,
    collection_mapping, collection_name, collection_read_policy, collection_representation,
    collection_source, collection_write_policy, mapping_algorithm as mapping_algorithm_attribute,
    CollectionHandle, RecordDecodeError, KIND_COLLECTION_DESCRIPTOR, KIND_COLLECTION_MAPPING,
};
use super::{CollectionEncoding, CollectionMapping};

/// Retired `collection_recipe` attribute, minted with `trible genid` on
/// 2026-08-07. It remains only as a rejection marker: accepting an old recipe
/// descriptor as a recipe-free encoding descriptor would silently reinterpret
/// its identity and laws.
const OBSOLETE_COLLECTION_RECIPE: Id = crate::id::id_hex!("5D338C58D897B969BE1AE0956CCFE301");

/// Store one descriptor archive and every blob carried by its self-contained
/// Fragment, returning the canonical descriptor handle.
///
/// The descriptor identity covers only its fact archive. Names and embedded
/// self-descriptions may reference separate blobs, so publishing facts alone
/// would leave a descriptor whose shape validates but whose descriptions
/// cannot be read. Registration therefore stores the complete closure before
/// publishing any later record which names the descriptor handle.
pub(crate) fn put_closure<S>(
    store: &mut S,
    descriptor: &Fragment,
) -> Result<CollectionHandle, S::PutError>
where
    S: BlobStorePut,
{
    let mut blobs = descriptor.blobs().clone();
    let mut embedded: Vec<Blob<UnknownBlob>> = blobs
        .snapshot()
        .expect("MemoryBlobStore::snapshot is infallible")
        .into_iter()
        .map(|(_, blob)| blob)
        .collect();
    embedded.sort_unstable_by_key(|blob| blob.get_handle().raw);
    for blob in embedded {
        store.put::<UnknownBlob, _>(blob)?;
    }
    store.put::<SimpleArchive, _>(descriptor.facts().clone())
}

/// Build a root descriptor that names its encoding without describing it.
///
/// A collection encoding normally writes its own descriptor as a visible
/// `entity!`; see
/// [`simplearchive_union::descriptor`](crate::collection::simplearchive_union::descriptor),
/// which additionally embeds the encoding's self-description so a stranger
/// holding the one blob can say what the collection is. This is the bare
/// generic form, for callers holding only ids.
///
/// Policy is mandatory and contributes to collection identity. Actual
/// delegated principals remain external capability proofs so invitations can
/// grow without renaming the collection.
pub(crate) fn naming<E>(name: &str, policy: CollectionPolicy) -> Fragment
where
    E: CollectionEncoding,
{
    entity! {
        metadata::tag: KIND_COLLECTION_DESCRIPTOR,
        collection_name: name.to_owned(),
        collection_read_policy*: policy.read().fragment(),
        collection_write_policy*: policy.write().fragment(),
        collection_representation*: <E as MetaDescribe>::describe(),
    }
}

/// Build a derived descriptor around one explicit mapping value.
///
/// The mapping Fragment is spread into the same descriptor archive. Its root
/// is linked from the descriptor and all algorithm descriptions, parameters,
/// and attachments therefore travel with the collection identity. This
/// lower-level form also serves downstream mappings between encodings whose
/// crates cannot implement the target-owned
/// [`CollectionDerivation`](super::CollectionDerivation) trait because of
/// Rust's orphan rule.
pub(crate) fn deriving_with<M>(
    source: CollectionHandle,
    mapping: &M,
    policy: CollectionPolicy,
) -> Fragment
where
    M: CollectionMapping,
{
    entity! {
        metadata::tag: KIND_COLLECTION_DESCRIPTOR,
        collection_source: source,
        collection_read_policy*: policy.read().fragment(),
        collection_write_policy*: policy.write().fragment(),
        collection_representation*: <M::Target as MetaDescribe>::describe(),
        collection_mapping*: mapping.fragment(),
    }
}

/// The entity the descriptor's own attributes hang off.
///
/// A descriptor archive holds more than one entity: the descriptor, plus the
/// embedded self-descriptions of its encoding and mapping. The
/// descriptor is the one tagged [`KIND_COLLECTION_DESCRIPTOR`].
///
/// This is not the collection identity. That is the handle of the stored
/// descriptor blob.
///
/// For the decoded path only: a descriptor read back out of a blob is a bare
/// `TribleSet`, so finding its root means looking for the tag. A caller that
/// *built* the descriptor already holds the root and should use
/// [`Fragment::root`](crate::trible::Fragment::root) rather than pay this scan
/// to recover something it never lost.
pub fn entity(facts: &TribleSet) -> Result<Id, RecordDecodeError> {
    exactly_one(
        find!(
            (e: Id),
            pattern!(facts, [{ ?e @ metadata::tag: KIND_COLLECTION_DESCRIPTOR }])
        )
        .map(|(e,)| e),
        "metadata::tag",
    )
}

/// Blob representation carried by the elements of this collection.
pub fn representation(facts: &TribleSet) -> Result<Id, RecordDecodeError> {
    let descriptor = entity(facts)?;
    exactly_one(
        find!(
            (v: Id?),
            pattern!(facts, [{ descriptor @ collection_representation: ?v }])
        )
        .map(|(v,)| v),
        "collection_representation",
    )?
    .map_err(|_| RecordDecodeError::InvalidId("collection_representation"))
}

/// Concrete mapping instance carried by a derived collection.
///
/// Root collections answer `None`. A derived collection names exactly one
/// mapping entity. Canonical builders normally derive its id from its concrete
/// parameters, but readers accept the equivalent extrinsic-id substitution.
pub fn mapping(facts: &TribleSet) -> Result<Option<Id>, RecordDecodeError> {
    let descriptor = entity(facts)?;
    at_most_one(
        find!(
            (v: Id?),
            pattern!(facts, [{ descriptor @ collection_mapping: ?v }])
        )
        .map(|(v,)| v),
        "collection_mapping",
    )?
    .map(|value| value.map_err(|_| RecordDecodeError::InvalidId("collection_mapping")))
    .transpose()
}

/// Algorithm named by the concrete mapping instance, if this is derived.
pub fn mapping_algorithm(facts: &TribleSet) -> Result<Option<Id>, RecordDecodeError> {
    let Some(mapping) = mapping(facts)? else {
        return Ok(None);
    };
    let kind: Inline<GenId> = KIND_COLLECTION_MAPPING.to_inline();
    if !facts.iter().any(|fact| {
        fact.e() == &mapping && fact.a() == &metadata::tag.id() && fact.v::<GenId>() == &kind
    }) {
        return Err(RecordDecodeError::MissingField(
            "mapping metadata::tag KIND_COLLECTION_MAPPING",
        ));
    }
    exactly_one(
        find!(
            (v: Id?),
            pattern!(facts, [{ mapping @ mapping_algorithm_attribute: ?v }])
        )
        .map(|(v,)| v),
        "mapping_algorithm",
    )?
    .map(Some)
    .map_err(|_| RecordDecodeError::InvalidId("mapping_algorithm"))
}

/// The collection this one derives from, if it derives from one.
///
/// A root has no source and answers `None`; that is not a failure, it is what
/// being a root means.
pub fn source(facts: &TribleSet) -> Result<Option<CollectionHandle>, RecordDecodeError> {
    let descriptor = entity(facts)?;
    at_most_one(
        find!(
            (v: CollectionHandle),
            pattern!(facts, [{ descriptor @ collection_source: ?v }])
        )
        .map(|(v,)| v),
        "collection_source",
    )
}

/// Handle of the UTF-8 name carried by a root collection.
///
/// A derived collection has no name of its own and answers `None`: its anchor
/// is its source, and its name is whatever that source is called.
pub fn name(facts: &TribleSet) -> Result<Option<Inline<Handle<UTF8String>>>, RecordDecodeError> {
    let descriptor = entity(facts)?;
    at_most_one(
        find!(
            (v: Inline<Handle<UTF8String>>),
            pattern!(facts, [{ descriptor @ collection_name: ?v }])
        )
        .map(|(v,)| v),
        "collection_name",
    )
}

/// Immutable capability policy declared by this descriptor.
///
/// Both links are required and single-valued. Their linked policy entities are
/// decoded independently; unknown kinds and invalid quorum geometry fail
/// closed as malformed descriptors.
pub fn policy(facts: &TribleSet) -> Result<CollectionPolicy, RecordDecodeError> {
    let read =
        exactly_one_descriptor_inline(facts, &collection_read_policy, "collection_read_policy")?
            .try_from_inline::<Id>()
            .map_err(|_| RecordDecodeError::InvalidId("collection_read_policy"))?;
    let write =
        exactly_one_descriptor_inline(facts, &collection_write_policy, "collection_write_policy")?
            .try_from_inline::<Id>()
            .map_err(|_| RecordDecodeError::InvalidId("collection_write_policy"))?;
    Ok(CollectionPolicy::new(
        decode_admission_policy(facts, read)?,
        decode_admission_policy(facts, write)?,
    ))
}

fn decode_admission_policy(
    facts: &TribleSet,
    policy: Id,
) -> Result<AdmissionPolicy, RecordDecodeError> {
    let kind = exactly_one(
        facts
            .iter()
            .filter(|fact| fact.e() == &policy && fact.a() == &metadata::tag.id())
            .map(|fact| *fact.v::<GenId>()),
        "admission policy metadata::tag",
    )?
    .try_from_inline::<Id>()
    .map_err(|_| RecordDecodeError::InvalidId("admission policy metadata::tag"))?;

    let policy_fields = [
        admission_policy_root.id(),
        admission_invoke_threshold.id(),
        admission_delegate_threshold.id(),
    ];
    if kind == KIND_ADMISSION_POLICY_OPEN {
        if facts.iter().any(|fact| {
            fact.e() == &policy && policy_fields.iter().any(|attribute| fact.a() == attribute)
        }) {
            return Err(RecordDecodeError::InvalidId("open admission policy fields"));
        }
        return Ok(AdmissionPolicy::Open);
    }
    if kind != KIND_ADMISSION_POLICY_QUORUM {
        return Err(RecordDecodeError::InvalidId(
            "admission policy metadata::tag",
        ));
    }

    let roots = facts
        .iter()
        .filter(|fact| fact.e() == &policy && fact.a() == &admission_policy_root.id())
        .map(|fact| {
            (*fact.v::<crate::inline::encodings::ed25519::ED25519PublicKey>())
                .try_from_inline::<ed25519_dalek::VerifyingKey>()
                .map_err(|_| RecordDecodeError::InvalidId("admission_policy_root"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let invoke = exactly_one(
        facts
            .iter()
            .filter(|fact| fact.e() == &policy && fact.a() == &admission_invoke_threshold.id())
            .map(|fact| *fact.v::<U256>()),
        "admission_invoke_threshold",
    )?
    .try_from_inline::<u32>()
    .map_err(|_| RecordDecodeError::InvalidId("admission_invoke_threshold"))?;
    let delegate = at_most_one(
        facts
            .iter()
            .filter(|fact| fact.e() == &policy && fact.a() == &admission_delegate_threshold.id())
            .map(|fact| *fact.v::<U256>()),
        "admission_delegate_threshold",
    )?
    .map(|value| {
        value
            .try_from_inline::<u32>()
            .map_err(|_| RecordDecodeError::InvalidId("admission_delegate_threshold"))
    })
    .transpose()?;
    AdmissionPolicy::quorum(roots, invoke, delegate)
        .map_err(|_| RecordDecodeError::InvalidId("admission policy quorum"))
}

/// Validate the representation-independent shape shared by every collection
/// descriptor and return its local policy.
///
/// A root is named and has no source mapping. A derived collection is unnamed
/// and carries both its source and one concrete mapping. Encoding-specific
/// context is deliberately left to [`CollectionEncoding::validate_descriptor`]
/// at the typed boundary.
pub fn validate(facts: &TribleSet) -> Result<CollectionPolicy, RecordDecodeError> {
    let descriptor = entity(facts)?;
    if facts
        .iter()
        .any(|fact| fact.e() == &descriptor && fact.a() == &OBSOLETE_COLLECTION_RECIPE)
    {
        return Err(RecordDecodeError::ObsoleteField("collection_recipe"));
    }
    representation(facts)?;
    let name = name(facts)?;
    let source = source(facts)?;
    let mapping = mapping(facts)?;
    match (name, source, mapping) {
        (Some(_), None, None) => {}
        (None, Some(_), Some(_)) => {
            mapping_algorithm(facts)?;
        }
        (None, None, None) => {
            return Err(RecordDecodeError::MissingField(
                "collection_name or collection_source with collection_mapping",
            ));
        }
        _ => {
            return Err(RecordDecodeError::RepeatedField(
                "collection shape (root name or derived source/mapping)",
            ));
        }
    }
    policy(facts)
}

/// Look up one descriptor argument by attribute.
///
/// Source-to-target parameters normally use [`mapping_argument`]. This lower
/// level helper remains available for encoding-specific descriptor facts.
pub fn argument(facts: &TribleSet, attribute: Id) -> Result<Option<RawInline>, RecordDecodeError> {
    argument_on(facts, entity(facts)?, attribute, "descriptor argument")
}

/// Look up one concrete mapping parameter by attribute.
pub fn mapping_argument(
    facts: &TribleSet,
    attribute: Id,
) -> Result<Option<RawInline>, RecordDecodeError> {
    let Some(mapping) = mapping(facts)? else {
        return Ok(None);
    };
    argument_on(facts, mapping, attribute, "mapping argument")
}

fn argument_on(
    facts: &TribleSet,
    subject: Id,
    attribute: Id,
    field: &'static str,
) -> Result<Option<RawInline>, RecordDecodeError> {
    let subject: Inline<GenId> = subject.to_inline();
    let attribute: Inline<GenId> = attribute.to_inline();
    at_most_one(
        find!(
            (v: Inline<GenId>),
            facts.pattern::<GenId>(subject, attribute, v)
        )
        .map(|(v,)| v.raw),
        field,
    )
}

/// Decode one required single-valued field and require that it belongs to the
/// exact tagged descriptor entity.
fn exactly_one_descriptor_inline<S: InlineEncoding>(
    facts: &TribleSet,
    attribute: &crate::attribute::Attribute<S>,
    field: &'static str,
) -> Result<Inline<S>, RecordDecodeError> {
    let descriptor = entity(facts)?;
    let fact = exactly_one(
        facts.iter().filter(|fact| fact.a() == &attribute.id()),
        field,
    )?;
    if fact.e() != &descriptor {
        return Err(RecordDecodeError::FieldOnWrongEntity(field));
    }
    Ok(*fact.v::<S>())
}

/// `Itertools::exactly_one`, saying which field the rows came from.
///
/// The question is the same one the rest of the crate asks by that name; only
/// the answer differs, because a decoder has to name the attribute it was
/// reading. `ExactlyOneError` carries the leftover iterator, which is empty
/// exactly when there was no first row at all -- that is how the two failures
/// are told apart here.
fn exactly_one<T>(
    rows: impl Iterator<Item = T>,
    field: &'static str,
) -> Result<T, RecordDecodeError> {
    rows.exactly_one().map_err(|mut leftover| {
        if leftover.next().is_some() {
            RecordDecodeError::RepeatedField(field)
        } else {
            RecordDecodeError::MissingField(field)
        }
    })
}

/// Decode an optional single-valued field without accepting an arbitrary
/// first match from malformed input.
fn at_most_one<T>(
    mut rows: impl Iterator<Item = T>,
    field: &'static str,
) -> Result<Option<T>, RecordDecodeError> {
    let Some(first) = rows.next() else {
        return Ok(None);
    };
    if rows.next().is_some() {
        return Err(RecordDecodeError::RepeatedField(field));
    }
    Ok(Some(first))
}

/// A root descriptor under one fixed test authority, named by `name`.
///
/// Tests overwhelmingly want "some collection, distinct from that other one".
/// Spelling that as a name under a fixed authority keeps the distinction
/// readable in the test itself.
#[cfg(test)]
pub(crate) fn named_for_tests(name: &str, representation: Id) -> Fragment {
    let root = ed25519_dalek::SigningKey::from_bytes(&[0xAA; 32]).verifying_key();
    // Some record-algebra tests deliberately use synthetic encoding ids, so
    // retain the one bare low-level test constructor beside the typed public
    // builders.
    entity! {
        metadata::tag: KIND_COLLECTION_DESCRIPTOR,
        collection_name: name.to_owned(),
        collection_read_policy*: AdmissionPolicy::direct(root).fragment(),
        collection_write_policy*: AdmissionPolicy::direct(root).fragment(),
        collection_representation: representation,
    }
}

/// Content identity of a descriptor, for tests that have no store.
///
/// The record algebra -- which payloads a cover admits, which merges compose,
/// what a derive equates -- is a property of records alone, and its tests
/// build them without a pile. There is no `put` to take a handle from, so
/// there is nothing to forget to write and no phantom to create: the
/// descriptor is an input to the algebra rather than a thing in a store.
///
/// This is why it is `cfg(test)` rather than public. Production code always
/// has somewhere to put the descriptor, and takes the handle from what `put`
/// hands back -- because a handle computed beside a store instead of by it can
/// name a collection whose descriptor was never written, leaving records that
/// reference something nothing can decode.
#[cfg(test)]
pub(crate) fn identity_for_tests(descriptor: &Fragment) -> CollectionHandle {
    crate::blob::IntoBlob::<crate::blob::encodings::simplearchive::SimpleArchive>::to_blob(
        descriptor.facts().clone(),
    )
    .get_handle()
}

#[cfg(test)]
mod policy_tests {
    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::blob::encodings::simplearchive::SimpleArchive;
    use crate::collection::policy::{AdmissionPolicy, CollectionPolicy};
    use crate::inline::encodings::genid::GenId;
    use crate::inline::{Inline, IntoInline};
    use crate::metadata;
    use crate::trible::Trible;

    fn key(byte: u8) -> ed25519_dalek::VerifyingKey {
        SigningKey::from_bytes(&[byte; 32]).verifying_key()
    }

    fn root(name: &str, expected: CollectionPolicy) -> Fragment {
        naming::<SimpleArchive>(name, expected)
    }

    #[test]
    fn policy_round_trips_independent_quorums() {
        let expected = CollectionPolicy::new(
            AdmissionPolicy::quorum([key(4), key(5)], 2, Some(2)).unwrap(),
            AdmissionPolicy::direct(key(4)),
        );
        assert_eq!(
            policy(root("ledger", expected.clone()).facts()),
            Ok(expected)
        );
    }

    #[test]
    fn policy_participates_in_collection_identity() {
        let closed = root(
            "ledger",
            CollectionPolicy::new(
                AdmissionPolicy::direct(key(5)),
                AdmissionPolicy::direct(key(5)),
            ),
        );
        let open_read = root(
            "ledger",
            CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::direct(key(5))),
        );
        assert_ne!(identity_for_tests(&closed), identity_for_tests(&open_read));
    }

    #[test]
    fn missing_policy_link_fails_closed() {
        let fragment = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_write_policy*: AdmissionPolicy::direct(key(6)).fragment(),
        };
        assert_eq!(
            policy(fragment.facts()),
            Err(RecordDecodeError::MissingField("collection_read_policy"))
        );
    }

    #[test]
    fn unknown_policy_kind_fails_closed() {
        let unknown = entity! {
            metadata::tag: crate::id::id_hex!("44444444444444444444444444444444"),
        };
        let fragment = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_read_policy*: unknown,
            collection_write_policy*: AdmissionPolicy::direct(key(7)).fragment(),
        };
        assert_eq!(
            policy(fragment.facts()),
            Err(RecordDecodeError::InvalidId(
                "admission policy metadata::tag"
            ))
        );
    }

    #[test]
    fn repeated_policy_link_fails_closed() {
        let mut fragment = root(
            "ledger",
            CollectionPolicy::new(
                AdmissionPolicy::direct(key(8)),
                AdmissionPolicy::direct(key(8)),
            ),
        );
        let descriptor = fragment.root().expect("descriptor root");
        let second: Inline<GenId> = AdmissionPolicy::Open
            .fragment()
            .root()
            .expect("open policy root")
            .to_inline();
        fragment.facts_mut().insert(&Trible::force(
            &descriptor,
            &collection_read_policy.id(),
            &second,
        ));
        assert_eq!(
            policy(fragment.facts()),
            Err(RecordDecodeError::RepeatedField("collection_read_policy"))
        );
    }

    #[test]
    fn retired_recipe_is_still_rejected() {
        let mut fragment = root(
            "legacy",
            CollectionPolicy::new(
                AdmissionPolicy::direct(key(9)),
                AdmissionPolicy::direct(key(9)),
            ),
        );
        let descriptor = fragment.root().expect("descriptor root");
        let value: Inline<GenId> = KIND_COLLECTION_MAPPING.to_inline();
        fragment.facts_mut().insert(&Trible::force(
            &descriptor,
            &OBSOLETE_COLLECTION_RECIPE,
            &value,
        ));
        assert_eq!(
            validate(fragment.facts()),
            Err(RecordDecodeError::ObsoleteField("collection_recipe"))
        );
    }
}
