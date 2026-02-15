use anybytes::Bytes;

use crate::blob::Blob;
use crate::blob::BlobSchema;
use crate::blob::ToBlob;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstDescribe, ConstId};
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;

/// A blob schema for WebAssembly bytecode.
///
/// This schema is intended for sandboxed helper modules such as value formatters
/// (see `metadata::value_formatter`).
pub struct WasmCode;

impl BlobSchema for WasmCode {}

impl ConstId for WasmCode {
    const ID: Id = id_hex!("DEE50FAD0CFFA4F8FD542DD18D9B7E52");
}

impl ConstDescribe for WasmCode {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "WebAssembly bytecode blob for sandboxed helper modules. The modules are expected to be deterministic and import-free, intended for small utilities such as value formatters.\n\nUse when a schema references a formatter via metadata::value_formatter or similar tooling and you want portable, sandboxed code alongside the data. Avoid large or stateful modules; keep the bytecode focused on pure formatting or validation tasks.",
        )?;
        Ok(entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("wasmcode".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_SCHEMA,
        }
        .into_facts())
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
