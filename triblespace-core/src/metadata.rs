//! Metadata namespace for the `triblespace` crate.
//!
//! This namespace is used to bootstrap the meaning of other namespaces.
//! It defines meta attributes that are used to describe other attributes.

use crate::blob::schemas::iri::IRI;
use crate::blob::schemas::longstring::LongString;
use crate::blob::schemas::wasmcode::WasmCode;
use crate::id::Id;
use crate::id_hex;
use crate::prelude::valueschemas;
use crate::trible::Fragment;
use crate::value::schemas::hash;
use core::marker::PhantomData;
use triblespace_core_macros::attributes;

/// Describes a runtime *instance* — emits metadata about a specific value (an
/// `Attribute<S>` with its id+name+usage, etc.). For describing a Rust *type*
/// itself (schema metadata for `ShortString`, `Handle<T>`, …) use
/// [`MetaDescribe`].
///
/// `describe` returns a [`Fragment`] that's self-contained — its
/// embedded [`crate::blob::MemoryBlobStore`] holds any bytes the
/// returned facts reference by handle. Consumers wanting to persist
/// the description hand the fragment to a workspace.
pub trait Describe {
    /// Produces a [`Fragment`] describing this instance, with any
    /// long-form bytes embedded in its local blob store.
    fn describe(&self) -> Fragment;

    /// Returns the id of this instance. Default: derive from
    /// `self.describe().root()`. Override when the id is cheaper to
    /// compute directly (e.g. `Attribute<S>` reads it from its
    /// stored fragment).
    fn id(&self) -> Id {
        self.describe()
            .root()
            .expect("describe returns a rooted fragment")
    }
}

/// Describes a Rust *type* — emits schema metadata about the type itself
/// without needing an instance (`ShortString`, `Handle<T>`, …). For
/// describing a runtime value use [`Describe`].
///
/// Same self-contained Fragment contract as [`Describe`]: the
/// returned Fragment's local blob store holds the bytes for any
/// handles in its facts.
pub trait MetaDescribe {
    /// Produces a [`Fragment`] describing this schema type.
    fn describe() -> Fragment;

    /// Returns the id of this type. Default: derive from
    /// `Self::describe().root()`. Impls choose whether the id is
    /// *explicit* (an `entity!{ &id_hex @ … }` form inside describe)
    /// or *derived* (no `@`, intrinsic id from the facts) — either
    /// way the default reads the root the fragment carries.
    ///
    /// Impls **must not** call `Self::id()` from inside their `describe`
    /// body — that would recurse through this default. Use the literal id
    /// directly in describe() or rely on no-`@` intrinsic derivation.
    ///
    /// No caching: each call re-runs describe + root. If id() becomes a hot
    /// path, layer a `TypeId`-keyed cache on top from the call site.
    fn id() -> Id {
        <Self as MetaDescribe>::describe()
            .root()
            .expect("describe returns a rooted fragment")
    }
}

impl<S> Describe for PhantomData<S>
where
    S: MetaDescribe,
{
    fn describe(&self) -> Fragment {
        <S as MetaDescribe>::describe()
    }

    // id() uses the default (describe + root).
}

// namespace constants
/// Tag for entities that can have multiple simultaneous kinds.
pub const KIND_MULTI: Id = id_hex!("C36D9C16B34729D855BD6C36A624E1BF");
/// Tag for entities that represent value schemas.
pub const KIND_VALUE_SCHEMA: Id = id_hex!("9A169BF2383E7B1A3E019808DFE3C2EB");
/// Tag for entities that represent blob schemas.
pub const KIND_BLOB_SCHEMA: Id = id_hex!("CE488DB0C494C7FDBF3DF1731AED68A6");
/// Tag for entities that describe an attribute usage in some source context.
pub const KIND_ATTRIBUTE_USAGE: Id = id_hex!("45759727A79C28D657EC06D5C6013649");
/// Tag for entities that describe a protocol.
pub const KIND_PROTOCOL: Id = id_hex!("A04AD649FA28DC5904385532E9C8EF74");
/// Tag for entities that are themselves tag/marker constants (e.g. kind discriminants).
pub const KIND_TAG: Id = id_hex!("452584B4C1CAE0B77F44408E6F194A31");

attributes! {
    /// Optional long-form description stored as a LongString handle.
    ///
    /// This attribute is general-purpose: it can describe any entity. Schema
    /// metadata uses it for documenting value/blob schemas, but it is equally
    /// valid for domain entities.
    "AE94660A55D2EE3C428D2BB299E02EC3" as description: valueschemas::Handle<LongString>;
    /// Links an attribute or handle to its value schema identifier.
    "213F89E3F49628A105B3830BD3A6612C" as value_schema: valueschemas::GenId;
    /// Links a handle to its blob schema identifier.
    "43C134652906547383054B1E31E23DF4" as blob_schema: valueschemas::GenId;
    /// Links an `Array<T>` schema entity to its element schema's id. Distinct
    /// from `blob_schema` because element schemas are not themselves
    /// `BlobSchema`s — they only carry an `ArrayElement::Native` byte-layout.
    "56C43BEE48BE99521886D99BE9026A3B" as array_item_schema: valueschemas::GenId;
    /// Links a handle to the hash algorithm used for content addressing.
    "51C08CFABB2C848CE0B4A799F0EFE5EA" as hash_schema: valueschemas::GenId;
    /// Optional WebAssembly module for formatting values governed by this schema.
    ///
    /// The value is a `Handle<WasmCode>` that points to a sandboxed
    /// formatter module (see `triblespace_core::value_formatter`).
    "1A3D520FEDA9E1A4051EBE96E43ABAC7" as value_formatter: valueschemas::Handle<WasmCode>;
    /// Long-form display name stored as a LongString handle.
    ///
    /// Names are *display*-oriented and contextual: multiple usages of the
    /// same attribute may carry different names depending on the codebase
    /// or domain. Use attribute usage entities (tagged with
    /// `KIND_ATTRIBUTE_USAGE`) when you need to capture multiple names for
    /// the same attribute id.
    ///
    /// For *identity*-determining strings (an IRI for RDF, an export
    /// symbol for WASM, …), use a dedicated attribute like
    /// [`iri`](`self::iri`) instead. The id-derivation paths for dynamic
    /// attributes hash from those identity-determining attributes, not
    /// from `name`.
    "7FB28C0B48E1924687857310EE230414" as name: valueschemas::Handle<LongString>;
    /// Internationalized Resource Identifier (IRI) for this entity.
    ///
    /// The canonical identity-determining string for RDF predicate URIs and
    /// RDF entity URIs. Distinct from [`name`] (display) so an IRI-derived
    /// attribute and a same-bytes JSON-field-derived attribute never
    /// collide: the (attr_id, value) pair that participates in
    /// entity-intrinsic-id derivation differs in the attr_id, even when the
    /// raw value bytes are identical.
    "325F05DB88184B4540AAEEFAE1E9667F" as iri: valueschemas::Handle<IRI>;
    /// Link a usage annotation entity to the attribute it describes.
    "F10DE6D8E60E0E86013F1B867173A85C" as attribute: valueschemas::GenId;
    /// Optional provenance string for a usage annotation.
    "A56350FD00EC220B4567FE15A5CD68B8" as source: valueschemas::Handle<LongString>;
    /// Optional module path for the usage annotation (from `module_path!()`).
    "BCB94C7439215641A3E9760CE3F4F432" as source_module: valueschemas::Handle<LongString>;
    /// Preferred JSON representation (e.g. string, number, bool, object, ref, blob).
    /// Preferred JSON representation hint (e.g. `"string"`, `"number"`, `"bool"`, `"object"`).
    "A7AFC8C0FAD017CE7EC19587AF682CFF" as json_kind: valueschemas::ShortString;
    /// Generic tag edge: link any entity to a tag entity (by Id). Reusable across domains.
    "91C50E9FBB1F73E892EBD5FFDE46C251" as tag: valueschemas::GenId;
    /// When an entity was created.
    "9B1E79DFD065F643954141593CD8B9E0" as created_at: valueschemas::NsTAIInterval;
    /// When an entity was last updated.
    "93B7372E3443063392CD801B03A8D390" as updated_at: valueschemas::NsTAIInterval;
    /// When a process or interval started.
    "06973030ACA83A7B2B4FC8BEBB31F77A" as started_at: valueschemas::NsTAIInterval;
    /// When a process or interval finished.
    "9B06AA4060EF9928A923FC7E6A6B6438" as finished_at: valueschemas::NsTAIInterval;
    /// When an entity expires or becomes invalid.
    "89FEC3B560336BA88B10759DECD3155F" as expires_at: valueschemas::NsTAIInterval;
}
