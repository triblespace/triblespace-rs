//! Metadata namespace for the `triblespace` crate.
//!
//! This namespace is used to bootstrap the meaning of other namespaces.
//! It defines meta attributes that are used to describe other attributes.

use crate::blob::MemoryBlobStore;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id::RawId;
use crate::id_hex;
use crate::macros::entity;
use crate::prelude::valueschemas;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use core::marker::PhantomData;
use std::collections::HashMap;
use triblespace_core_macros::attributes;

/// Describes metadata that can be emitted for documentation or discovery.
pub trait Metadata {
    /// Returns the root identifier for this metadata description.
    fn id(&self) -> Id;

    fn describe(&self) -> (TribleSet, MemoryBlobStore<Blake3>);
}

/// Helper trait for schema types that want to expose metadata without requiring an instance.
pub trait ConstMetadata {
    /// Returns the root identifier for this metadata description.
    fn id() -> Id;

    fn describe() -> (TribleSet, MemoryBlobStore<Blake3>) {
        (TribleSet::new(), MemoryBlobStore::new())
    }
}

impl<S> Metadata for PhantomData<S>
where
    S: ConstMetadata,
{
    fn id(&self) -> Id {
        <S as ConstMetadata>::id()
    }

    fn describe(&self) -> (TribleSet, MemoryBlobStore<Blake3>) {
        <S as ConstMetadata>::describe()
    }
}

impl<T> Metadata for T
where
    T: ConstMetadata,
{
    fn id(&self) -> Id {
        T::id()
    }

    fn describe(&self) -> (TribleSet, MemoryBlobStore<Blake3>) {
        (TribleSet::new(), MemoryBlobStore::new())
    }
}
// namespace constants

pub const ATTR_NAME: Id = id_hex!("2E26F8BA886495A8DF04ACF0ED3ACBD4");
pub const ATTR_JSON_KIND: Id = id_hex!("A7AFC8C0FAD017CE7EC19587AF682CFF");
pub const ATTR_CARDINALITY: Id = id_hex!("F83E75F635BE17647CDCE616380B1CD2");
pub const VALUE_SCHEMA: Id = id_hex!("213F89E3F49628A105B3830BD3A6612C");
pub const BLOB_SCHEMA: Id = id_hex!("43C134652906547383054B1E31E23DF4");
pub const HASH_SCHEMA: Id = id_hex!("51C08CFABB2C848CE0B4A799F0EFE5EA");

attributes! {
    "2E26F8BA886495A8DF04ACF0ED3ACBD4" as name: valueschemas::ShortString;
    "213F89E3F49628A105B3830BD3A6612C" as value_schema: valueschemas::GenId;
    "43C134652906547383054B1E31E23DF4" as blob_schema: valueschemas::GenId;
    "51C08CFABB2C848CE0B4A799F0EFE5EA" as hash_schema: valueschemas::GenId;
    /// Preferred JSON representation (e.g. string, number, bool, object, ref, blob).
    "A7AFC8C0FAD017CE7EC19587AF682CFF" as json_kind: valueschemas::ShortString;
    /// Cardinality hint for JSON export (`single` and/or `multi`).
    "F83E75F635BE17647CDCE616380B1CD2" as cardinality: valueschemas::ShortString;
    /// Generic tag edge: link any entity to a tag entity (by Id). Reusable across domains.
    "91C50E9FBB1F73E892EBD5FFDE46C251" as tag: valueschemas::GenId;
}

#[derive(Default, Clone, Copy)]
pub struct CardinalityHints {
    pub single: bool,
    pub multi: bool,
}

pub fn cardinality_hints_for(data: &TribleSet) -> HashMap<RawId, CardinalityHints> {
    let mut per_entity_counts: HashMap<(RawId, Id), usize> = HashMap::new();
    for trible in data.iter() {
        let attr: RawId = (*trible.a()).into();
        let entity = *trible.e();
        *per_entity_counts.entry((attr, entity)).or_default() += 1;
    }

    let mut hints: HashMap<RawId, CardinalityHints> = HashMap::new();
    for ((attr, _entity), count) in per_entity_counts {
        let entry = hints.entry(attr).or_default();
        if count == 1 {
            entry.single = true;
        } else {
            entry.multi = true;
        }
    }

    hints
}

pub fn emit_cardinality_metadata(
    attr_id: Id,
    hints: Option<&CardinalityHints>,
    metadata: &mut TribleSet,
) {
    let Some(hints) = hints else {
        return;
    };

    let entity = ExclusiveId::force(attr_id);
    if hints.single {
        *metadata += entity! { &entity @ cardinality: "single" };
    }
    if hints.multi {
        *metadata += entity! { &entity @ cardinality: "multi" };
    }
}
