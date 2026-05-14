//! This is a collection of Rust types that can be (de)serialized as [crate::prelude::Blob]s.

/// Flat typed array blob schema.
pub mod array;
/// Raw file bytes blob schema.
pub mod filebytes;
/// Internationalized Resource Identifier blob schema.
pub mod iri;
/// Arbitrary-length UTF-8 text blob schema.
pub mod longstring;
/// Opaque raw bytes blob schema (positive choice, distinct from UnknownBlob).
pub mod rawbytes;
/// Canonical trible sequence blob schema.
pub mod simplearchive;
/// Succinct (Ring-based) compressed trible archive blob schema.
pub mod succinctarchive;
/// WebAssembly bytecode blob schema.
pub mod wasmcode;

use crate::value::IntoSchema;
use anybytes::Bytes;

use crate::blob::BlobSchema;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};

use super::Blob;
use super::TryFromBlob;

/// A blob schema for an unknown blob.
/// This blob schema is used as a fallback when the blob schema is not known.
/// It is not recommended to use this blob schema in practice.
/// Instead, use a specific blob schema.
///
/// Any bit pattern can be a valid blob of this schema.
pub struct UnknownBlob;
impl BlobSchema for UnknownBlob {}

impl MetaDescribe for UnknownBlob {
    fn describe() -> crate::trible::Fragment {
        // Fixed-id fallback schema. Even though it's discouraged in
        // practice, the metadata should still self-describe so a
        // consumer encountering this id can recognise it.
        let mut fragment = crate::trible::Fragment::rooted(
            id_hex!("EAB14005141181B0C10C4B5DD7985F8D"),
            crate::trible::TribleSet::new(),
        );
        let name = fragment.put("UnknownBlob");
        let description = fragment.put(
            "Fallback blob schema for byte payloads with no known type. Discouraged in practice — use a specific blob schema (e.g. `LongString`, `Array<T>`, `SimpleArchive`) instead.",
        );
        fragment.annotated(|id_ref| {
            entity! { id_ref @
                metadata::name:        name,
                metadata::description: description,
                metadata::tag:         metadata::KIND_BLOB_SCHEMA,
            }
        })
    }
}

impl TryFromBlob<UnknownBlob> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<UnknownBlob>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl IntoSchema<UnknownBlob> for Bytes
where crate::value::schemas::hash::Handle<UnknownBlob>: crate::value::InlineSchema,
{
    type Form = Blob<UnknownBlob>;
    fn into_schema(self) -> Blob<UnknownBlob> {
        Blob::new(self)
    }
}
