//! Field helper type used by the query macros.
//!
//! The `Field<S>` type is a small, const-friendly wrapper around a 16-byte
//! attribute id (RawId) and a phantom type parameter `S` indicating the value
//! schema for that attribute. We keep construction simple and const-friendly so
//! fields can be declared as `pub const F: Field<ShortString> = Field::from(hex!("..."));`.

use crate::blob::schemas::longstring::LongString;
use crate::blob::ToBlob;
use crate::id::ExclusiveId;
use crate::id::RawId;
use crate::macros::entity;
use crate::metadata::{self, Describe};
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::Blake3;
use crate::value::ValueSchema;
use core::marker::PhantomData;

/// Describes a concrete usage of an attribute in source code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttributeUsage {
    /// Contextual name for this usage (may differ across codebases).
    pub name: &'static str,
    /// Optional human-facing description for this usage.
    pub description: Option<&'static str>,
    /// Optional source location to disambiguate multiple usages.
    pub source: Option<AttributeUsageSource>,
}

/// Source location metadata for attribute usages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttributeUsageSource {
    /// Fully qualified Rust module path (e.g. `"crate::schema::core"`).
    pub module_path: &'static str,
    /// Source file path where the attribute is used.
    pub file: &'static str,
    /// Line number within the source file.
    pub line: u32,
    /// Column number within the source line.
    pub column: u32,
}

impl AttributeUsageSource {}

impl AttributeUsage {
    /// Construct a minimal usage entry with a name.
    pub const fn named(name: &'static str) -> Self {
        Self {
            name,
            description: None,
            source: None,
        }
    }

    /// Set a human-facing description for this usage.
    pub const fn description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    /// Set a source location for this usage.
    pub const fn source(mut self, source: AttributeUsageSource) -> Self {
        self.source = Some(source);
        self
    }

    fn describe<B>(
        &self,
        blobs: &mut B,
        attribute_id: crate::id::Id,
    ) -> Result<Fragment, B::PutError>
    where
        B: crate::repo::BlobStore<Blake3>,
    {
        // Step 1: entity core — the facts that determine this usage's
        // identity. The attribute it describes, plus (optionally) which
        // source module the usage lives in. Module-path bytes go through
        // `blobs.put` here just like in the annotations below; both paths
        // produce the same `Handle<Blake3, LongString>` handle bytes by
        // content addressing, so the core's intrinsic id is stable
        // whether the same usage is computed standalone or as part of a
        // larger description.
        let module_handle = if let Some(src) = self.source {
            Some(blobs.put(src.module_path)?)
        } else {
            None
        };
        let mut fragment = match module_handle {
            Some(handle) => entity! {
                metadata::attribute:     attribute_id,
                metadata::source_module: handle,
            },
            None => entity! {
                metadata::attribute: attribute_id,
            },
        };
        let usage_id = fragment
            .root()
            .expect("entity! without `@` always emits a rooted fragment");
        let usage_entity = ExclusiveId::force_ref(&usage_id);

        // Step 2: annotate the core with descriptive facts.
        let name_handle = blobs.put(self.name)?;
        fragment += entity! { &usage_entity @ metadata::name: name_handle };

        if let Some(description) = self.description {
            let description_handle = blobs.put(description)?;
            fragment += entity! { &usage_entity @ metadata::description: description_handle };
        }

        fragment += entity! { &usage_entity @
            metadata::attribute: GenId::value_from(attribute_id),
            metadata::tag: metadata::KIND_ATTRIBUTE_USAGE,
        };

        Ok(fragment)
    }
}
/// A typed reference to an attribute id together with its value schema.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Attribute<S: ValueSchema> {
    raw: RawId,
    handle: Option<crate::value::Value<crate::value::schemas::hash::Handle<Blake3, LongString>>>,
    usage: Option<AttributeUsage>,
    _schema: PhantomData<S>,
}

impl<S: ValueSchema> Clone for Attribute<S> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw,
            handle: self.handle,
            usage: self.usage,
            _schema: PhantomData,
        }
    }
}

impl<S: ValueSchema> Attribute<S> {
    /// Construct a `Field` from a raw 16-byte id and a fully specified usage.
    pub const fn from_id_with_usage(raw: RawId, usage: AttributeUsage) -> Self {
        Self {
            raw,
            handle: None,
            usage: Some(usage),
            _schema: PhantomData,
        }
    }

    /// Construct a `Field` from a raw 16-byte id without attaching a static name.
    /// Prefer [`Attribute::from_id_with_usage`] when a static usage is available.
    pub const fn from_id(raw: RawId) -> Self {
        Self {
            raw,
            handle: None,
            usage: None,
            _schema: PhantomData,
        }
    }

    /// Return the underlying raw id bytes.
    pub const fn raw(&self) -> RawId {
        self.raw
    }

    /// Convert to a runtime [`Id`](crate::id::Id) value. This performs the nil check and will
    /// panic if the raw id is the nil id (all zeros).
    pub fn id(&self) -> crate::id::Id {
        crate::id::Id::new(self.raw).unwrap()
    }

    /// Convert a host value into a typed `Value<S>` using the Field's schema.
    /// This is a small convenience wrapper around the `ToValue` trait and
    /// simplifies macro expansion: `af.value_from(expr)` preserves the
    /// schema `S` for type inference.
    pub fn value_from<T: crate::value::ToValue<S>>(&self, v: T) -> crate::value::Value<S> {
        crate::value::ToValue::to_value(v)
    }

    /// Coerce an existing variable of any schema into a variable typed with
    /// this field's schema. This is a convenience for macros: they can
    /// allocate an untyped/UnknownValue variable and then annotate it with the
    /// field's schema using `af.as_variable(raw_var)`.
    ///
    /// The operation is a zero-cost conversion as variables are simply small
    /// integer indexes; the implementation uses an unsafe transmute to change
    /// the type parameter without moving the underlying data.
    pub fn as_variable(&self, v: crate::query::Variable<S>) -> crate::query::Variable<S> {
        v
    }

    /// Returns the declared name of this attribute, if any.
    pub fn name(&self) -> Option<&str> {
        self.usage.map(|usage| usage.name)
    }

    /// Attach usage metadata to an attribute.
    pub const fn with_usage(mut self, usage: AttributeUsage) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Derive an attribute id from a dynamic *display name* and this
    /// schema's metadata.
    ///
    /// Hashes from `metadata::name + metadata::value_schema`. Use this
    /// when the *name* itself is the canonical identifier for this
    /// origin — e.g. a JSON field name, a column header, a config
    /// key. The name doubles as display and identity in those
    /// origins.
    ///
    /// For origins where identity is *not* a display name — RDF
    /// (where the IRI is canonical), WASM (where the export symbol
    /// is canonical), etc. — use the origin-specific constructor
    /// instead:
    ///
    /// - [`Attribute::from_iri`] — RDF predicates / IRIs.
    ///
    /// The idiomatic path remains explicit hex via the `attributes!`
    /// macro for any attribute whose id we want to pin (so local
    /// renames don't churn the schema).
    pub fn from_name(name: &str) -> Self {
        let field_handle = String::from(name).to_blob().get_handle::<Blake3>();
        let fragment = entity! {
            metadata::name:         field_handle,
            metadata::value_schema: <S as crate::metadata::MetaDescribe>::id(),
        };
        let id = fragment
            .root()
            .expect("entity! without `@` always emits a rooted fragment");
        let raw: RawId = id.into();
        Self {
            raw,
            handle: Some(field_handle),
            usage: None,
            _schema: PhantomData,
        }
    }

    /// Derive an attribute id from an IRI and this schema's metadata.
    ///
    /// Hashes from `metadata::iri + metadata::value_schema`. The IRI
    /// is wrapped as a `Handle<Blake3, IRI>` (typed distinctly from
    /// the `Handle<Blake3, LongString>` `from_name` produces), so
    /// even if a JSON field name happens to look like an IRI, the
    /// attribute ids never collide — the (attr_id, value) pair that
    /// feeds intrinsic-id derivation differs in the attr_id.
    ///
    /// Use this from the RDF / JSON-LD / SPARQL importers — anywhere
    /// the predicate or property *is* an IRI by spec. Debug builds
    /// validate the IRI predicate at construction; release builds
    /// trust the caller.
    pub fn from_iri(iri: &str) -> Self {
        let iri_handle: crate::value::Value<
            crate::value::schemas::hash::Handle<Blake3, crate::blob::schemas::iri::IRI>,
        > = String::from(iri).to_blob().get_handle::<Blake3>();
        let fragment = entity! {
            metadata::iri:          iri_handle,
            metadata::value_schema: <S as crate::metadata::MetaDescribe>::id(),
        };
        let id = fragment
            .root()
            .expect("entity! without `@` always emits a rooted fragment");
        let raw: RawId = id.into();
        Self {
            raw,
            // We don't currently cache the IRI handle on Attribute —
            // it's a different handle type from `handle: Option<...LongString...>`.
            // describe() emits the IRI fact explicitly via metadata::iri.
            handle: None,
            usage: None,
            _schema: PhantomData,
        }
    }
}

impl<S> Describe for Attribute<S>
where
    S: ValueSchema + crate::metadata::MetaDescribe,
{
    fn describe<B>(&self, blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: crate::repo::BlobStore<Blake3>,
    {
        let id = self.id();
        let mut fragment = Fragment::rooted(id, TribleSet::new());

        if let Some(handle) = self.handle {
            fragment += entity! { ExclusiveId::force_ref(&id) @ metadata::name: handle };
        }

        // Spread S's describe — runs once, S's root becomes the
        // `metadata::value_schema` value, and S's facts fold in.
        fragment += entity! { ExclusiveId::force_ref(&id) @
            metadata::value_schema*: <S as crate::metadata::MetaDescribe>::describe(blobs)?,
        };

        if let Some(usage) = self.usage {
            fragment += usage.describe(blobs, id)?;
        }

        Ok(fragment)
    }
}

/// Re-export of [`RawId`] used by generated macro code.
pub use crate::id::RawId as RawIdAlias;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::schemas::longstring::LongString;
    use crate::value::schemas::hash::{Blake3, Handle};
    use crate::value::schemas::shortstring::ShortString;

    #[test]
    fn dynamic_field_is_deterministic() {
        let a1 = Attribute::<ShortString>::from_name("title");
        let a2 = Attribute::<ShortString>::from_name("title");

        assert_eq!(a1.raw(), a2.raw());
        assert_ne!(a1.raw(), [0; crate::id::ID_LEN]);
    }

    #[test]
    fn dynamic_field_changes_with_name() {
        let title = Attribute::<ShortString>::from_name("title");
        let author = Attribute::<ShortString>::from_name("author");

        assert_ne!(title.raw(), author.raw());
    }

    #[test]
    fn dynamic_field_changes_with_schema() {
        let short = Attribute::<ShortString>::from_name("title");
        let handle = Attribute::<Handle<Blake3, LongString>>::from_name("title");

        assert_ne!(short.raw(), handle.raw());
    }
}
