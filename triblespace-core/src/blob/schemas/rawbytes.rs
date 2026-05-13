use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::repo::BlobStore;
use crate::trible::Fragment;
use crate::value::schemas::hash::Blake3;

use anybytes::Bytes;

/// Opaque raw bytes — no structural interpretation.
///
/// Use this when the payload is intentionally bytes-without-further-
/// structure, e.g. an XSD `hexBinary` / `base64Binary` literal, a
/// digest value carried inline, or any other "the bytes ARE the
/// content" case. Distinct from [`FileBytes`](super::filebytes::FileBytes)
/// (which signals file provenance — attachments, dataset artifacts) and
/// from [`UnknownBlob`](super::UnknownBlob) (the explicit "I don't know
/// what schema this is" fallback): RawBytes is a positive choice
/// meaning "I do know what this is — it's bytes."
pub struct RawBytes;

impl BlobSchema for RawBytes {}

impl MetaDescribe for RawBytes {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id: Id = id_hex!("4C1BA1EB2FDCC637C2F269A46FCA2398");
        let description = blobs.put(
            "Opaque raw bytes with no further structural interpretation. Used for content where the bytes themselves are the payload (XSD hexBinary / base64Binary literals, inline digests, key material). Distinct from FileBytes (file-provenance) and from UnknownBlob (the 'unknown schema' fallback): RawBytes is a positive choice meaning the schema *is* raw bytes.",
        )?;
        Ok(entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("rawbytes")?,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        })
    }
}

impl TryFromBlob<RawBytes> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<RawBytes>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl ToBlob<RawBytes> for Bytes {
    fn to_blob(self) -> Blob<RawBytes> {
        Blob::new(self)
    }
}

impl ToBlob<RawBytes> for Vec<u8> {
    fn to_blob(self) -> Blob<RawBytes> {
        Blob::new(Bytes::from_source(self))
    }
}

impl ToBlob<RawBytes> for &[u8] {
    fn to_blob(self) -> Blob<RawBytes> {
        Blob::new(Bytes::from_source(self.to_vec()))
    }
}
