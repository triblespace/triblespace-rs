//! Typed attribute references with carried identity-determining facts.
//!
//! An [`Attribute<S>`] is a rooted [`Fragment`] plus a phantom value-
//! schema marker. The fragment's `root()` IS the attribute id; its
//! facts carry the identity-determining data (e.g.
//! `metadata::iri: <handle>` or `metadata::name: <handle>` together
//! with `metadata::value_schema: <schema id>`). Optional usage
//! metadata is merged into the same fragment as additional facts (the
//! usage's own root is not exposed — only the attribute root remains
//! visible via `Fragment::root()`).
//!
//! Construct via [`From<Fragment>`]:
//!
//! ```ignore
//! // Display-name origin (JSON fields, config keys, column headers):
//! Attribute::<S>::from(entity! {
//!     metadata::name:         name.to_blob().get_handle::<Blake3>(),
//!     metadata::value_schema: <S as MetaDescribe>::id(),
//! })
//!
//! // RDF / JSON-LD predicate (IRI as canonical identifier):
//! Attribute::<S>::from(entity! {
//!     metadata::iri:          iri.to_blob().get_handle::<Blake3>(),
//!     metadata::value_schema: <S as MetaDescribe>::id(),
//! })
//!
//! // Explicit hex id (schemas, pinned attribute namespace):
//! let id: Id = id_hex!("…");
//! Attribute::<S>::from(entity! { &ExclusiveId::force_ref(&id) @
//!     metadata::value_schema: <S as MetaDescribe>::id(),
//! })
//! ```
//!
//! Add a contextual usage via the [`Attribute::with_usage`] builder.

use crate::id::ExclusiveId;
use crate::id::RawId;
use crate::macros::entity;
use crate::metadata::{self, Describe};
use crate::repo::BlobStore;
use crate::trible::Fragment;
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

    /// Build the usage's own fragment — rooted at the usage's intrinsic id,
    /// with `metadata::attribute: <attr_id>` linking it back to the
    /// described attribute.
    pub(crate) fn describe<B>(
        &self,
        blobs: &mut B,
        attribute_id: crate::id::Id,
    ) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
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

/// A typed reference to an attribute: a rooted [`Fragment`] carrying
/// the identity-determining facts, plus optional usage metadata that
/// `describe()` re-emits on demand using the caller's blob store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute<S: ValueSchema> {
    fragment: Fragment,
    usage: Option<AttributeUsage>,
    _schema: PhantomData<S>,
}

impl<S: ValueSchema> Attribute<S> {
    /// The attribute's id, equal to the wrapped fragment's root.
    pub fn id(&self) -> crate::id::Id {
        self.fragment
            .root()
            .expect("Attribute fragment must be rooted")
    }

    /// Return the underlying raw id bytes.
    pub fn raw(&self) -> RawId {
        self.id().into()
    }

    /// The identity-determining fragment.
    pub fn fragment(&self) -> &Fragment {
        &self.fragment
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

    /// Attach a [`AttributeUsage`]. The usage's facts are re-emitted
    /// by [`Describe::describe`] using the caller's blob store, so
    /// the destination store ends up with the usage's name /
    /// description / source-module bytes — without the attribute
    /// itself ever needing access to a blob store at construction.
    pub const fn with_usage(mut self, usage: AttributeUsage) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Returns the declared name of the attached usage, if any.
    pub fn name(&self) -> Option<&str> {
        self.usage.map(|usage| usage.name)
    }
}

/// Wrap a rooted fragment as a typed attribute.
///
/// The fragment's `root()` is the attribute id; its facts (typically
/// `metadata::iri | metadata::name` together with
/// `metadata::value_schema`) are carried through to [`Describe`] so the
/// attribute remains queryable in the metadata registry by its
/// originating identity attribute.
///
/// Pinning a schema's attribute ids (so local renames don't churn the
/// schema) is what the `attributes!` macro is for — declare them with
/// explicit hex literals there.
impl<S: ValueSchema> From<Fragment> for Attribute<S> {
    fn from(fragment: Fragment) -> Self {
        fragment
            .root()
            .expect("Attribute::from(Fragment) requires a rooted fragment");
        Self {
            fragment,
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
        B: BlobStore<Blake3>,
    {
        let id = self.id();
        // Start from the identity-determining fragment — its facts
        // (metadata::iri / metadata::name / metadata::value_schema)
        // are the attribute's metadata, not just the bytes of their
        // hash.
        let mut fragment = self.fragment.clone();

        // Spread S's describe under the same attribute id so the bare
        // `metadata::value_schema: S::id()` grows into the full schema
        // description. Merge as facts so the spread fragment's root
        // doesn't escape past this attribute.
        let spread = entity! { ExclusiveId::force_ref(&id) @
            metadata::value_schema*: <S as crate::metadata::MetaDescribe>::describe(blobs)?,
        };
        fragment += spread.into_facts();

        // Re-emit the usage on demand against the caller's blobs, so
        // they end up with the usage handle bytes. The usage's root
        // stays internal — we merge facts only.
        if let Some(usage) = self.usage {
            fragment += usage.describe(blobs, id)?.into_facts();
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
    use crate::blob::ToBlob;
    use crate::id::Id;
    use crate::macros::{find, pattern};
    use crate::metadata::MetaDescribe;
    use crate::blob::MemoryBlobStore;
    use crate::value::schemas::hash::{Blake3, Handle};
    use crate::value::schemas::shortstring::ShortString;
    use crate::value::Value;

    #[test]
    fn dynamic_field_is_deterministic() {
        let h1 = "title".to_blob().get_handle::<Blake3>();
        let h2 = "title".to_blob().get_handle::<Blake3>();
        let a1 = Attribute::<ShortString>::from(entity! {
            metadata::name:         h1,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });
        let a2 = Attribute::<ShortString>::from(entity! {
            metadata::name:         h2,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });

        assert_eq!(a1.raw(), a2.raw());
        assert_ne!(a1.raw(), [0; crate::id::ID_LEN]);
    }

    #[test]
    fn dynamic_field_changes_with_name() {
        let h_title = "title".to_blob().get_handle::<Blake3>();
        let h_author = "author".to_blob().get_handle::<Blake3>();
        let title = Attribute::<ShortString>::from(entity! {
            metadata::name:         h_title,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });
        let author = Attribute::<ShortString>::from(entity! {
            metadata::name:         h_author,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });

        assert_ne!(title.raw(), author.raw());
    }

    #[test]
    fn dynamic_field_changes_with_schema() {
        let h = "title".to_blob().get_handle::<Blake3>();
        let short = Attribute::<ShortString>::from(entity! {
            metadata::name:         h,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });
        let handle = Attribute::<Handle<Blake3, LongString>>::from(entity! {
            metadata::name:         h,
            metadata::value_schema: <Handle<Blake3, LongString> as MetaDescribe>::id(),
        });

        assert_ne!(short.raw(), handle.raw());
    }

    #[test]
    fn describe_preserves_identity_iri() {
        use crate::blob::schemas::iri::IRI;

        let iri = "http://example.org/foo";
        let iri_handle: Value<Handle<Blake3, IRI>> = iri.to_blob().get_handle::<Blake3>();
        let attr = Attribute::<ShortString>::from(entity! {
            metadata::iri:          iri_handle,
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        });
        let attr_id = attr.id();

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let meta = attr.describe(&mut blobs).expect("describe");

        // Discovery-by-IRI: the registry must contain
        // `<attr_id> @ metadata::iri: <handle>`.
        let hits: Vec<Id> = find!(
            (a: Id),
            pattern!(&meta, [{ ?a @ metadata::iri: iri_handle }])
        )
        .map(|(a,)| a)
        .collect();
        assert_eq!(hits, vec![attr_id]);

        // The describe output's sole root is the attribute id — the
        // schema spread's root doesn't bubble up.
        assert_eq!(meta.root(), Some(attr_id));
    }

    #[test]
    fn with_usage_keeps_attribute_root_and_populates_blobs() {
        let attr = Attribute::<ShortString>::from(entity! {
            metadata::name:         "answer".to_blob().get_handle::<Blake3>(),
            metadata::value_schema: <ShortString as MetaDescribe>::id(),
        })
        .with_usage(AttributeUsage::named("answer"));
        let attr_id = attr.id();

        let mut blobs = MemoryBlobStore::<Blake3>::new();
        let meta = attr.describe(&mut blobs).expect("describe");
        // Sole exposed root is the attribute id — the usage's own
        // intrinsic root stays internal.
        assert_eq!(meta.root(), Some(attr_id));

        // Usage facts reach the registry: a usage entity links back
        // to this attribute via `metadata::attribute`.
        let usage_count = find!(
            (u: Id),
            pattern!(&meta, [{ ?u @ metadata::attribute: attr_id }])
        )
        .count();
        assert!(usage_count > 0, "usage facts present in describe output");
    }
}
