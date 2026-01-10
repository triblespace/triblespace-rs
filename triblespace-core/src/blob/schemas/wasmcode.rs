use anybytes::Bytes;

use crate::blob::schemas::longstring::LongString;
use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

/// A blob schema for WebAssembly bytecode.
///
/// This schema is intended for sandboxed helper modules such as value formatters
/// (see `metadata::value_formatter`).
pub struct WasmCode;

impl BlobSchema for WasmCode {}

impl ConstMetadata for WasmCode {
    fn id() -> Id {
        id_hex!("DEE50FAD0CFFA4F8FD542DD18D9B7E52")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description =
            blobs.put::<LongString, _>("WebAssembly bytecode blob for formatters.")?;
        Ok(entity! {
            ExclusiveId::force_ref(&id) @
                metadata::shortname: "wasmcode",
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        })
    }
}

impl ToBlob<WasmCode> for Bytes {
    fn to_blob(self) -> Blob<WasmCode> {
        Blob::new(self)
    }
}

impl ToBlob<WasmCode> for Vec<u8> {
    fn to_blob(self) -> Blob<WasmCode> {
        Blob::new(Bytes::from_source(self))
    }
}

impl ToBlob<WasmCode> for &[u8] {
    fn to_blob(self) -> Blob<WasmCode> {
        Blob::new(Bytes::from_source(self.to_vec()))
    }
}

#[cfg(feature = "wasm")]
pub mod runtime;
