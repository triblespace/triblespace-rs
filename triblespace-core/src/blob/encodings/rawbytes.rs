use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::Encodes;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;

use anybytes::Bytes;

/// Opaque raw bytes — no structural interpretation.
///
/// Use this for any payload whose decode target is `Bytes`/`Vec<u8>` —
/// XSD `hexBinary` / `base64Binary` literals, file contents, digest
/// values carried inline, attachments, key material. Distinct from
/// [`UnknownBlob`](super::UnknownBlob) (the explicit "I don't know
/// what schema this is" fallback): `RawBytes` is a positive choice
/// meaning "I do know what this is — it's bytes."
///
/// "File-provenance" or "attachment" semantic intent lives at the
/// attribute level (`file::contents`, `request::body`, etc.), not at
/// the encoding level — `RawBytes` is the byte encoding; attributes
/// supply the meaning.
pub struct RawBytes;

impl BlobEncoding for RawBytes {}

impl MetaDescribe for RawBytes {
    fn describe() -> Fragment {
        let id: Id = id_hex!("4C1BA1EB2FDCC637C2F269A46FCA2398");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "rawbytes",
                metadata::description: "Opaque raw bytes with no further structural interpretation. Used for any payload whose decode target is Bytes/Vec<u8>: XSD hexBinary / base64Binary literals, file contents, inline digests, key material. Distinct from UnknownBlob (the 'unknown schema' fallback): RawBytes is a positive choice meaning the schema *is* raw bytes.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl TryFromBlob<RawBytes> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<RawBytes>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl Encodes<Bytes> for RawBytes
where
    crate::inline::encodings::hash::Handle<RawBytes>: crate::inline::InlineEncoding,
{
    type Output = Blob<RawBytes>;
    fn encode(source: Bytes) -> Blob<RawBytes> {
        Blob::new(source)
    }
}

impl Encodes<Vec<u8>> for RawBytes
where
    crate::inline::encodings::hash::Handle<RawBytes>: crate::inline::InlineEncoding,
{
    type Output = Blob<RawBytes>;
    fn encode(source: Vec<u8>) -> Blob<RawBytes> {
        Blob::new(Bytes::from_source(source))
    }
}

impl Encodes<&[u8]> for RawBytes
where
    crate::inline::encodings::hash::Handle<RawBytes>: crate::inline::InlineEncoding,
{
    type Output = Blob<RawBytes>;
    fn encode(source: &[u8]) -> Blob<RawBytes> {
        Blob::new(Bytes::from_source(source.to_vec()))
    }
}
