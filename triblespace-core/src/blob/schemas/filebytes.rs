use crate::value::Encodes;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;

use anybytes::Bytes;

/// Raw file bytes stored as a blob.
///
/// Use this schema when the payload represents a file snapshot and you want to
/// preserve that provenance explicitly instead of falling back to UnknownBlob.
pub struct FileBytes;

impl BlobSchema for FileBytes {}

impl MetaDescribe for FileBytes {
    fn describe() -> Fragment {
        let id: Id = id_hex!("5DE76157AE4FDEA830019916805E80A4");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Opaque file bytes captured as a blob. Use when the payload represents a file snapshot (attachments, dataset artifacts, exported archives) and you want to preserve that provenance in the schema rather than treating it as UnknownBlob. The meaning is given by adjacent metadata attributes such as mime type, filename, and dimensions.",
        );
        let name = tribles.put("filebytes");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        };
        tribles
    }
}

impl TryFromBlob<FileBytes> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<FileBytes>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl Encodes<Bytes> for FileBytes
where crate::value::schemas::hash::Handle<FileBytes>: crate::value::InlineSchema,
{
    type Encoded = Blob<FileBytes>;
    fn encode(source: Bytes) -> Blob<FileBytes> {
        Blob::new(source)
    }
}

impl Encodes<Vec<u8>> for FileBytes
where crate::value::schemas::hash::Handle<FileBytes>: crate::value::InlineSchema,
{
    type Encoded = Blob<FileBytes>;
    fn encode(source: Vec<u8>) -> Blob<FileBytes> {
        Blob::new(Bytes::from_source(source))
    }
}

impl Encodes<&[u8]> for FileBytes
where crate::value::schemas::hash::Handle<FileBytes>: crate::value::InlineSchema,
{
    type Encoded = Blob<FileBytes>;
    fn encode(source: &[u8]) -> Blob<FileBytes> {
        Blob::new(Bytes::from_source(source.to_vec()))
    }
}
