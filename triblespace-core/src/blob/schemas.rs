//! This is a collection of Rust types that can be (de)serialized as [crate::prelude::Blob]s.

pub mod filebytes;
pub mod longstring;
pub mod simplearchive;
pub mod succinctarchive;
pub mod wasmcode;

use anybytes::Bytes;

use crate::blob::BlobSchema;
use crate::id::Id;
use crate::id_hex;
use crate::metadata::{ConstDescribe, ConstId};

use super::Blob;
use super::ToBlob;
use super::TryFromBlob;

/// A blob schema for an unknown blob.
/// This blob schema is used as a fallback when the blob schema is not known.
/// It is not recommended to use this blob schema in practice.
/// Instead, use a specific blob schema.
///
/// Any bit pattern can be a valid blob of this schema.
pub struct UnknownBlob;
impl BlobSchema for UnknownBlob {}

impl ConstId for UnknownBlob {
    const ID: Id = id_hex!("EAB14005141181B0C10C4B5DD7985F8D");
}

impl ConstDescribe for UnknownBlob {}

impl TryFromBlob<UnknownBlob> for Bytes {
    type Error = std::convert::Infallible;

    fn try_from_blob(blob: Blob<UnknownBlob>) -> Result<Self, Self::Error> {
        Ok(blob.bytes)
    }
}

impl ToBlob<UnknownBlob> for Bytes {
    fn to_blob(self) -> Blob<UnknownBlob> {
        Blob::new(self)
    }
}
