//! Metadata namespace for the `triblespace` crate.
//!
//! This namespace is used to bootstrap the meaning of other namespaces.
//! It defines meta attributes that are used to describe other attributes.

use crate::repo::BlobStore;
use crate::id::Id;
use crate::id_hex;
use crate::prelude::valueschemas;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use core::marker::PhantomData;
use triblespace_core_macros::attributes;

/// Describes metadata that can be emitted for documentation or discovery.
pub trait Metadata {
    /// Returns the root identifier for this metadata description.
    fn id(&self) -> Id;

    fn describe(&self, blobs: &mut impl BlobStore<Blake3>) -> TribleSet;
}

/// Helper trait for schema types that want to expose metadata without requiring an instance.
pub trait ConstMetadata {
    /// Returns the root identifier for this metadata description.
    fn id() -> Id;

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;
        TribleSet::new()
    }
}

impl<S> Metadata for PhantomData<S>
where
    S: ConstMetadata,
{
    fn id(&self) -> Id {
        <S as ConstMetadata>::id()
    }

    fn describe(&self, blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        <S as ConstMetadata>::describe(blobs)
    }
}

impl<T> Metadata for T
where
    T: ConstMetadata,
{
    fn id(&self) -> Id {
        T::id()
    }

    fn describe(&self, blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;
        TribleSet::new()
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
