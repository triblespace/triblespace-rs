//! Typed attribute references with carried identity-determining facts.
//!
//! An [`Attribute<S>`] is a rooted [`Fragment`] plus a phantom value-
//! schema marker. The fragment's `root()` IS the attribute id; its
//! facts are the identity-determining data (e.g.
//! `metadata::iri: <handle>` or `metadata::name: <handle>` together
//! with `metadata::value_encoding: <schema id>`). The attribute is the
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
//!     metadata::name:         name.to_blob().get_handle(),
//!     metadata::value_encoding: <S as MetaDescribe>::id(),
//! })
//!
//! // RDF / JSON-LD predicate (IRI as canonical identifier):
//! Attribute::<S>::from(entity! {
//!     metadata::iri:          iri.to_blob().get_handle(),
//!     metadata::value_encoding: <S as MetaDescribe>::id(),
//! })
//!
//! // Explicit hex id (pinned attribute namespace):
//! Attribute::<S>::from(entity! {
//!     ExclusiveId::force_ref(&id) @
//!         metadata::value_encoding: <S as MetaDescribe>::id(),
//! })
//! ```

use crate::id::Id;
use crate::id::RawId;
use crate::trible::Fragment;
use crate::inline::InlineEncoding;
use core::marker::PhantomData;

/// A typed reference to an attribute: a rooted [`Fragment`] carrying
/// the identity-determining facts, tagged with a phantom value-schema
/// marker.
///
/// The root id is cached alongside the fragment so `.id()` is a field
/// read — `entity!{}` codegen calls it once per attribute per fact,
/// and walking the fragment's exports PATCH each time dominated the
/// pre-0.40 entities/union benches.
#[derive(Debug, PartialEq, Eq)]
pub struct Attribute<S: InlineEncoding> {
    id: Id,
    fragment: Fragment,
    _schema: PhantomData<S>,
}

impl<S: InlineEncoding> Clone for Attribute<S> {
    // Manual impl: `PhantomData<S>` doesn't require `S: Clone`, but
    // `#[derive(Clone)]` over a `S: InlineEncoding` bound conservatively
    // adds that constraint. Implementing by hand lets callers clone
    // `Attribute<Boolean>` etc. without needing `Boolean: Clone`.
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            fragment: self.fragment.clone(),
            _schema: PhantomData,
        }
    }
}

impl<S: InlineEncoding> Attribute<S> {
    /// The attribute's id, equal to the wrapped fragment's root.
    pub fn id(&self) -> Id {
        self.id
    }

    /// Return the underlying raw id bytes.
    pub fn raw(&self) -> RawId {
        self.id().into()
    }

    /// The identity-determining fragment.
    pub fn fragment(&self) -> &Fragment {
        &self.fragment
    }

    /// Convert a host value into a typed `Inline<S>` using the Field's schema.
    /// This is a small convenience wrapper around the `IntoInline` trait and
    /// simplifies macro expansion: `af.inline_from(expr)` preserves the
    /// schema `S` for type inference.
    pub fn inline_from<T: crate::inline::IntoInline<S>>(&self, v: T) -> crate::inline::Inline<S> {
        crate::inline::IntoInline::to_inline(v)
    }

    /// Macro-side entry point: produce the [`Encoded<S>`] the
    /// `entity!{}` codegen folds into a Fragment.
    ///
    /// Dispatches via [`IntoEncoded`], parameterised by the schema's
    /// [`Encoding`](crate::inline::InlineEncoding::Encoding) — `S`
    /// itself for inline schemas, the inner `BlobEncoding` for
    /// `Handle<T>`. The resulting `Output` is lifted into a [`Encoded`]
    /// via [`ToEncoded`].
    ///
    /// [`IntoEncoded`]: crate::inline::IntoEncoded
    /// [`ToEncoded`]: crate::inline::ToEncoded
    /// [`Encoded`]: crate::inline::Encoded
    /// [`Encoded<S>`]: crate::inline::Encoded
    pub fn encoded_from<V>(&self, v: V) -> crate::inline::Encoded<S>
    where
        V: crate::inline::IntoEncoded<<S as crate::inline::InlineEncoding>::Encoding>,
        <V as crate::inline::IntoEncoded<
            <S as crate::inline::InlineEncoding>::Encoding,
        >>::Output: crate::inline::ToEncoded<S>,
    {
        use crate::inline::ToEncoded;
        v.into_encoded().to_encoded()
    }

    /// Coerce an existing variable of any schema into a variable typed with
    /// this field's schema. This is a convenience for macros: they can
    /// allocate an untyped/UnknownInline variable and then annotate it with the
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
/// `metadata::value_encoding`) are carried through to [`Describe`] so the
/// attribute remains queryable in the metadata registry by its
/// originating identity attribute.
///
/// Pinning a schema's attribute ids (so local renames don't churn the
/// schema) is what the [`attributes!`] macro is for — declare them with
/// explicit hex literals there.
impl<S: InlineEncoding> From<Fragment> for Attribute<S> {
    fn from(fragment: Fragment) -> Self {
        let id = fragment
            .root()
            .expect("Attribute::from(Fragment) requires a rooted fragment");
        Self {
            id,
            fragment,
            _schema: PhantomData,
        }
    }
}

impl<S> crate::metadata::Describe for Attribute<S>
where
    S: InlineEncoding,
{
    fn describe(&self) -> Fragment {
        // An attribute IS its identity fragment. The wrapped fragment
        // already carries `metadata::iri` / `metadata::name` and
        // `metadata::value_encoding: S::id()` from construction —
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
    use crate::blob::encodings::longstring::LongString;
    use crate::blob::IntoBlob;
    use crate::id::Id;
    use crate::macros::{entity, find, pattern};
    use crate::metadata::{self, Describe, MetaDescribe};
    use crate::inline::encodings::hash::Handle;
    use crate::inline::encodings::shortstring::ShortString;
    use crate::inline::Inline;

    #[test]
    fn dynamic_field_is_deterministic() {
        let h1 = "title".to_blob().get_handle();
        let h2 = "title".to_blob().get_handle();
        let a1 = Attribute::<ShortString>::from(entity! {
            metadata::name:         h1,
            metadata::value_encoding: <ShortString as MetaDescribe>::id(),
        });
        let a2 = Attribute::<ShortString>::from(entity! {
            metadata::name:         h2,
            metadata::value_encoding: <ShortString as MetaDescribe>::id(),
        });

        assert_eq!(a1.raw(), a2.raw());
        assert_ne!(a1.raw(), [0; crate::id::ID_LEN]);
    }

    #[test]
    fn dynamic_field_changes_with_name() {
        let h_title = "title".to_blob().get_handle();
        let h_author = "author".to_blob().get_handle();
        let title = Attribute::<ShortString>::from(entity! {
            metadata::name:         h_title,
            metadata::value_encoding: <ShortString as MetaDescribe>::id(),
        });
        let author = Attribute::<ShortString>::from(entity! {
            metadata::name:         h_author,
            metadata::value_encoding: <ShortString as MetaDescribe>::id(),
        });

        assert_ne!(title.raw(), author.raw());
    }

    #[test]
    fn dynamic_field_changes_with_schema() {
        let h = "title".to_blob().get_handle();
        let short = Attribute::<ShortString>::from(entity! {
            metadata::name:         h,
            metadata::value_encoding: <ShortString as MetaDescribe>::id(),
        });
        let handle = Attribute::<Handle<LongString>>::from(entity! {
            metadata::name:         h,
            metadata::value_encoding: <Handle<LongString> as MetaDescribe>::id(),
        });

        assert_ne!(short.raw(), handle.raw());
    }

    #[test]
    fn describe_preserves_identity_iri() {
        let iri = "http://example.org/foo".to_string();
        let iri_handle: Inline<Handle<LongString>> = iri.to_blob().get_handle();
        let attr = Attribute::<ShortString>::from(entity! {
            metadata::iri:          iri_handle,
            metadata::value_encoding: <ShortString as crate::metadata::MetaDescribe>::id(),
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
