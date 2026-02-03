use crate::blob::schemas::longstring::LongString;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::blob::TryFromBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

use anybytes::Bytes;

/// Raw file bytes stored as a blob.
///
/// Use this schema when the payload represents a file snapshot and you want to
/// preserve that provenance explicitly instead of falling back to UnknownBlob.
pub struct FileBytes;

impl BlobSchema for FileBytes {}

impl ConstMetadata for FileBytes {
    fn id() -> Id {
        id_hex!("5DE76157AE4FDEA830019916805E80A4")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put::<LongString, _>(
            "Opaque file bytes captured as a blob. Use when the payload represents a file snapshot (attachments, dataset artifacts, exported archives) and you want to preserve that provenance in the schema rather than treating it as UnknownBlob. The meaning is given by adjacent metadata attributes such as mime type, filename, and dimensions.",
        )?;
        Ok(entity! {
            ExclusiveId::force_ref(&id) @
                metadata::shortname: "filebytes",
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        })
    }
}

impl TryFromBlob<FileBytes> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<FileBytes>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl ToBlob<FileBytes> for Bytes {
    fn to_blob(self) -> Blob<FileBytes> {
        Blob::new(self)
    }
}

impl ToBlob<FileBytes> for Vec<u8> {
    fn to_blob(self) -> Blob<FileBytes> {
        Blob::new(Bytes::from_source(self))
    }
}

impl ToBlob<FileBytes> for &[u8] {
    fn to_blob(self) -> Blob<FileBytes> {
        Blob::new(Bytes::from_source(self.to_vec()))
    }
}
