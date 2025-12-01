//! Field helper type used by the query macros.
//!
//! The `Field<S>` type is a small, const-friendly wrapper around a 16-byte
//! attribute id (RawId) and a phantom type parameter `S` indicating the value
//! schema for that attribute. We keep construction simple and const-friendly so
//! fields can be declared as `pub const F: Field<ShortString> = Field::from(hex!("..."));`.

use core::marker::PhantomData;
use std::borrow::Cow;

use crate::blob::ToBlob;
use crate::id::ExclusiveId;
use crate::id::RawId;
use crate::macros::entity;
use crate::metadata::{self, Metadata};
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::Blake3;
use crate::value::ValueSchema;
use blake3::Hasher;
/// A typed reference to an attribute id together with its value schema.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Attribute<S: ValueSchema> {
    raw: RawId,
    name: Option<Cow<'static, str>>,
    _schema: PhantomData<S>,
}

impl<S: ValueSchema> Clone for Attribute<S> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw,
            name: self.name.clone(),
            _schema: PhantomData,
        }
    }
}

impl<S: ValueSchema> Attribute<S> {
    /// Construct a `Field` from a raw 16-byte id and static attribute name.
    pub const fn from_id_with_name(raw: RawId, name: &'static str) -> Self {
        Self {
            raw,
            name: Some(Cow::Borrowed(name)),
            _schema: PhantomData,
        }
    }

    /// Construct a `Field` from a raw 16-byte id without attaching a static name.
    /// Prefer [`Attribute::from_id_with_name`] when the name is known at compile time.
    pub const fn from_id(raw: RawId) -> Self {
        Self {
            raw,
            name: None,
            _schema: PhantomData,
        }
    }

    /// Return the underlying raw id bytes.
    pub const fn raw(&self) -> RawId {
        self.raw
    }

    /// Convert to a runtime `Id` value. This performs the nil check and will
    /// panic if the raw id is the nil id (all zeros).
    pub fn id(&self) -> crate::id::Id {
        crate::id::Id::new(self.raw).unwrap()
    }

    /// Convert a host value into a typed Value<S> using the Field's schema.
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
        self.name.as_deref()
    }

    /// Derive an attribute id from a dynamic name and this schema's metadata.
    ///
    /// The identifier is computed by hashing the field name handle produced as a
    /// `Handle<Blake3, crate::blob::schemas::longstring::LongString>` together with the
    /// schema's [`ConstMetadata::id`].
    /// The resulting 32-byte Blake3 digest uses its rightmost 16 bytes to match the
    /// `RawId` layout used by [`Attribute::from_id`].
    pub fn from_name(name: &str) -> Self {
        let mut hasher = Hasher::new();

        let field_handle = String::from(name).to_blob().get_handle::<Blake3>();
        hasher.update(&field_handle.raw);
        hasher.update(S::id().as_ref());

        let digest = hasher.finalize();
        let mut raw = [0u8; crate::id::ID_LEN];
        let lower_half = &digest.as_bytes()[digest.as_bytes().len() - crate::id::ID_LEN..];
        raw.copy_from_slice(lower_half);
        Self {
            raw,
            name: Some(Cow::Owned(name.to_owned())),
            _schema: PhantomData,
        }
    }
}

impl<S> Metadata for Attribute<S>
where
    S: ValueSchema,
{
    fn id(&self) -> crate::id::Id {
        self.id()
    }

    fn describe(&self, blobs: &mut impl crate::repo::BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;
        let mut tribles = TribleSet::new();

        let entity = ExclusiveId::force(self.id());

        if let Some(name) = self.name() {
            tribles += entity! { &entity @ metadata::name: name };
        }

        tribles += entity! { &entity @ metadata::value_schema: GenId::value_from(S::id()) };

        tribles
    }
}

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
