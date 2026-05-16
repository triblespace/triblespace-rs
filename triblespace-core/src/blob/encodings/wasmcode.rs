use crate::inline::Encodes;
use anybytes::Bytes;

use crate::blob::Blob;
use crate::blob::BlobEncoding;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;

/// A blob encoding for WebAssembly bytecode.
///
/// This schema is intended for sandboxed helper modules such as value formatters
/// (see `metadata::value_formatter`).
pub struct WasmCode;

impl BlobEncoding for WasmCode {}

impl MetaDescribe for WasmCode {
    fn describe() -> Fragment {
        let id: Id = id_hex!("DEE50FAD0CFFA4F8FD542DD18D9B7E52");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "WebAssembly bytecode blob for sandboxed helper modules. The modules are expected to be deterministic and import-free, intended for small utilities such as value formatters.\n\nUse when a schema references a formatter via metadata::value_formatter or similar tooling and you want portable, sandboxed code alongside the data. Avoid large or stateful modules; keep the bytecode focused on pure formatting or validation tasks.",
        );
        let name = tribles.put("wasmcode");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        };
        tribles
    }
}

impl Encodes<Bytes> for WasmCode
where crate::inline::encodings::hash::Handle<WasmCode>: crate::inline::InlineEncoding,
{
    type Output = Blob<WasmCode>;
    fn encode(source: Bytes) -> Blob<WasmCode> {
        Blob::new(source)
    }
}

impl Encodes<Vec<u8>> for WasmCode
where crate::inline::encodings::hash::Handle<WasmCode>: crate::inline::InlineEncoding,
{
    type Output = Blob<WasmCode>;
    fn encode(source: Vec<u8>) -> Blob<WasmCode> {
        Blob::new(Bytes::from_source(source))
    }
}

impl Encodes<&[u8]> for WasmCode
where crate::inline::encodings::hash::Handle<WasmCode>: crate::inline::InlineEncoding,
{
    type Output = Blob<WasmCode>;
    fn encode(source: &[u8]) -> Blob<WasmCode> {
        Blob::new(Bytes::from_source(source.to_vec()))
    }
}

#[cfg(feature = "wasm")]
/// Runtime support for executing [`WasmCode`] modules.
pub mod runtime;
