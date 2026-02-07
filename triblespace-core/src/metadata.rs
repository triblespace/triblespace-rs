//! Metadata namespace for the `triblespace` crate.
//!
//! This namespace is used to bootstrap the meaning of other namespaces.
//! It defines meta attributes that are used to describe other attributes.

use crate::blob::schemas::longstring::LongString;
use crate::blob::schemas::wasmcode::WasmCode;
use crate::id::Id;
use crate::id_hex;
use crate::prelude::valueschemas;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash;
use crate::value::schemas::hash::Blake3;
use core::marker::PhantomData;
use triblespace_core_macros::attributes;

/// Describes metadata that can be emitted for documentation or discovery.
pub trait Metadata {
    /// Returns the root identifier for this metadata description.
    fn id(&self) -> Id;

    fn describe<B>(&self, blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>;
}

/// Helper trait for schema types that want to expose metadata without requiring an instance.
pub trait ConstMetadata {
    /// Returns the root identifier for this metadata description.
    fn id() -> Id;

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let _ = blobs;
        Ok(TribleSet::new())
    }
}

impl<S> Metadata for PhantomData<S>
where
    S: ConstMetadata,
{
    fn id(&self) -> Id {
        <S as ConstMetadata>::id()
    }

    fn describe<B>(&self, blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
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

    fn describe<B>(&self, blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let _ = blobs;
        Ok(TribleSet::new())
    }
}

// namespace constants
pub const KIND_MULTI: Id = id_hex!("C36D9C16B34729D855BD6C36A624E1BF");
/// Tag for entities that represent value schemas.
pub const KIND_VALUE_SCHEMA: Id = id_hex!("9A169BF2383E7B1A3E019808DFE3C2EB");
/// Tag for entities that represent blob schemas.
pub const KIND_BLOB_SCHEMA: Id = id_hex!("CE488DB0C494C7FDBF3DF1731AED68A6");
/// Tag for entities that describe an attribute usage in some source context.
pub const KIND_ATTRIBUTE_USAGE: Id = id_hex!("45759727A79C28D657EC06D5C6013649");

attributes! {
    /// Optional long-form description stored as a LongString handle.
    ///
    /// This attribute is general-purpose: it can describe any entity. Schema
    /// metadata uses it for documenting value/blob schemas, but it is equally
    /// valid for domain entities.
    "AE94660A55D2EE3C428D2BB299E02EC3" as description: valueschemas::Handle<hash::Blake3, LongString>;
    "213F89E3F49628A105B3830BD3A6612C" as value_schema: valueschemas::GenId;
    "43C134652906547383054B1E31E23DF4" as blob_schema: valueschemas::GenId;
    "51C08CFABB2C848CE0B4A799F0EFE5EA" as hash_schema: valueschemas::GenId;
    /// Optional WebAssembly module for formatting values governed by this schema.
    ///
    /// The value is a `Handle<Blake3, WasmCode>` that points to a sandboxed
    /// formatter module (see `triblespace_core::value_formatter`).
    "1A3D520FEDA9E1A4051EBE96E43ABAC7" as value_formatter: valueschemas::Handle<hash::Blake3, WasmCode>;
    /// Long-form name stored as a LongString handle.
    ///
    /// Names are contextual: multiple usages of the same attribute may carry
    /// different names depending on the codebase or domain. Use attribute
    /// usage entities (tagged with KIND_ATTRIBUTE_USAGE) when you need to
    /// capture multiple names for the same attribute id.
    "7FB28C0B48E1924687857310EE230414" as name: valueschemas::Handle<hash::Blake3, LongString>;
    /// Link a usage annotation entity to the attribute it describes.
    "F10DE6D8E60E0E86013F1B867173A85C" as attribute: valueschemas::GenId;
    /// Optional provenance string for a usage annotation.
    "A56350FD00EC220B4567FE15A5CD68B8" as source: valueschemas::Handle<hash::Blake3, LongString>;
    /// Optional module path for the usage annotation (from `module_path!()`).
    "BCB94C7439215641A3E9760CE3F4F432" as source_module: valueschemas::Handle<hash::Blake3, LongString>;
    /// Preferred JSON representation (e.g. string, number, bool, object, ref, blob).
    "A7AFC8C0FAD017CE7EC19587AF682CFF" as json_kind: valueschemas::ShortString;
    /// Generic tag edge: link any entity to a tag entity (by Id). Reusable across domains.
    "91C50E9FBB1F73E892EBD5FFDE46C251" as tag: valueschemas::GenId;
}
