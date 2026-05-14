//! Typed attribute references with carried identity-determining facts.
//!
//! An [`Attribute<S>`] is a rooted [`Fragment`] plus a phantom value-
//! schema marker. The fragment's `root()` IS the attribute id; its
//! facts are the identity-determining data (e.g.
//! `metadata::iri: <handle>` or `metadata::name: <handle>` together
//! with `metadata::value_schema: <schema id>`). The attribute is the
//! *abstract shared thing* multiple parties agree on; codebase-local
//! annotations (the rust identifier, source location, doc comment)
//! are emitted at the [`attributes!`] call site as usage facts —
//! there is no [`AttributeUsage`] type, the macro inlines them.
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
//! // Explicit hex id (pinned attribute namespace):
//! Attribute::<S>::from(Fragment::rooted(id, TribleSet::new()))
//! ```

use crate::id::RawId;
use crate::trible::Fragment;
use crate::value::ValueSchema;
use core::marker::PhantomData;

/// A typed reference to an attribute: a rooted [`Fragment`] carrying
/// the identity-determining facts, tagged with a phantom value-schema
/// marker.
#[derive(Debug, PartialEq, Eq)]
pub struct Attribute<S: ValueSchema> {
    fragment: Fragment,
    _schema: PhantomData<S>,
}

impl<S: ValueSchema> Clone for Attribute<S> {
    // Manual impl: `PhantomData<S>` doesn't require `S: Clone`, but
    // `#[derive(Clone)]` over a `S: ValueSchema` bound conservatively
    // adds that constraint. Implementing by hand lets callers clone
    // `Attribute<Boolean>` etc. without needing `Boolean: Clone`.
    fn clone(&self) -> Self {
        Self {
            fragment: self.fragment.clone(),
            _schema: PhantomData,
        }
    }
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

    /// Macro-side entry point: produce the `(Value<S>, Option<Bytes>)`
    /// pair the `entity!{}` codegen folds into a Fragment. The bytes
    /// half (if any) get absorbed into the fragment's local blob
    /// store via `MemoryBlobStore::insert_bytes`. Anchored on
    /// `Attribute<S>` so the schema parameter `S` is captured for
    /// trait resolution on the value side.
    pub fn into_field_value<V: crate::value::IntoFieldValue<S>>(
        &self,
        v: V,
    ) -> (crate::value::Value<S>, Option<anybytes::Bytes>) {
        v.into_field_value()
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
/// schema) is what the [`attributes!`] macro is for — declare them with
/// explicit hex literals there.
impl<S: ValueSchema> From<Fragment> for Attribute<S> {
    fn from(fragment: Fragment) -> Self {
        fragment
            .root()
            .expect("Attribute::from(Fragment) requires a rooted fragment");
        Self {
            fragment,
            _schema: PhantomData,
        }
    }
}

impl<S> crate::metadata::Describe for Attribute<S>
where
    S: ValueSchema,
{
    fn describe(&self) -> Fragment {
        // An attribute IS its identity fragment. The wrapped fragment
        // already carries `metadata::iri` / `metadata::name` and
        // `metadata::value_schema: S::id()` from construction —
        // exactly the facts a registry queries on. The schema's own
        // facts (the human-readable name, description, hash protocol,
        // …) belong to the schema, not the attribute; consumers
        // wanting them ask `<S as MetaDescribe>::describe()`
        // separately. Pure accessor.
        self.fragment.clone()
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
    use crate::macros::{entity, find, pattern};
    use crate::metadata::{self, Describe, MetaDescribe};
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
            metadata::value_schema: <ShortString as crate::metadata::MetaDescribe>::id(),
        });
        let attr_id = attr.id();

        let meta = attr.describe();

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
}
