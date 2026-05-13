use core::marker::PhantomData;

use anybytes::View;

use crate::blob::schemas::longstring::LongString;
use crate::id::{ExclusiveId, Id, RawId};
use crate::macros::entity;
use crate::metadata::{self, Describe};
use crate::repo::BlobStore;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::schemas::genid::GenId;
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::Value;
use crate::value::ValueSchema;

/// Import-only attribute wrapper that keeps field names as metadata without
/// emitting contextual usage annotations.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ImportAttribute<S: ValueSchema> {
    raw: RawId,
    name: Option<View<str>>,
    _schema: PhantomData<S>,
}

impl<S: ValueSchema> Clone for ImportAttribute<S> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw,
            name: self.name.clone(),
            _schema: PhantomData,
        }
    }
}

impl<S: ValueSchema> ImportAttribute<S> {
    /// Construct an import attribute from a raw id with an optional name.
    pub fn from_raw(raw: RawId, name: Option<View<str>>) -> Self {
        Self {
            raw,
            name,
            _schema: PhantomData,
        }
    }

    /// Construct an import attribute from a name handle and the original name bytes.
    ///
    /// The id is derived via the canonical entity-intrinsic-id
    /// mechanism — the attribute IS the entity described by
    /// `metadata::name: <handle>` and
    /// `metadata::value_schema: <S>::ID`. This keeps
    /// `ImportAttribute::from_handle` byte-identical to
    /// [`crate::attribute::Attribute::from_name`] for the same
    /// `(name, S)` inputs, which the cross-engine importers and
    /// test helpers rely on.
    pub fn from_handle(handle: Value<Handle<Blake3, LongString>>, name: View<str>) -> Self {
        let fragment = entity! {
            metadata::name:         handle,
            metadata::value_schema: <S as crate::metadata::MetaDescribe>::id(),
        };
        let id = fragment
            .root()
            .expect("entity! without `@` always emits a rooted fragment");
        let raw: RawId = id.into();
        Self::from_raw(raw, Some(name))
    }

    /// Return the underlying raw id bytes.
    pub const fn raw(&self) -> RawId {
        self.raw
    }

    /// Convert to a runtime [`Id`](crate::id::Id) value.
    pub fn id(&self) -> Id {
        Id::new(self.raw).unwrap()
    }
}

impl<S> Describe for ImportAttribute<S>
where
    S: ValueSchema,
{
    fn describe<B>(&self, blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let mut tribles = TribleSet::new();
        let id = self.id();

        if let Some(name) = &self.name {
            let handle = blobs.put(name.clone())?;
            tribles += entity! { ExclusiveId::force_ref(&id) @ metadata::name: handle };
        }

        tribles += entity! { ExclusiveId::force_ref(&id) @ metadata::value_schema: GenId::value_from(<S as crate::metadata::MetaDescribe>::id()) };

        Ok(Fragment::rooted(id, tribles))
    }
}
