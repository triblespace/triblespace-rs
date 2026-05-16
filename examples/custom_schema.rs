use anybytes::Bytes;
use std::convert::Infallible;
use triblespace::core::blob::Blob;
use triblespace::core::blob::BlobEncoding;
use triblespace::core::blob::TryFromBlob;
use triblespace::core::id::id_hex;
use triblespace::core::id::{ExclusiveId, Id};
use triblespace::core::macros::entity;
use triblespace::core::metadata::{self, MetaDescribe};
use triblespace::core::inline::TryFromInline;
use triblespace::core::inline::Inline;
use triblespace::core::inline::InlineEncoding;
use triblespace::core::inline::Encodes;
use triblespace::core::inline::INLINE_LEN;

// ANCHOR: custom_schema

pub struct U64LE;

impl MetaDescribe for U64LE {
    fn describe() -> triblespace::core::trible::Fragment {
        let id: Id = id_hex!("0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A");
        entity! { ExclusiveId::force_ref(&id) @
            metadata::name: "u64le",
            metadata::tag:  metadata::KIND_INLINE_ENCODING,
        }
    }
}

impl InlineEncoding for U64LE {
    type ValidationError = Infallible;
    type Encoding = Self;
}

impl Encodes<u64> for U64LE
{
    type Output = Inline<U64LE>;
    fn encode(source: u64) -> Inline<U64LE> {
        let mut raw = [0u8; INLINE_LEN];
        raw[..8].copy_from_slice(&source.to_le_bytes());
        Inline::new(raw)
    }
}

impl TryFromInline<'_, U64LE> for u64 {
    type Error = std::convert::Infallible;
    fn try_from_inline(v: &Inline<U64LE>) -> Result<Self, std::convert::Infallible> {
        Ok(u64::from_le_bytes(v.raw[..8].try_into().unwrap()))
    }
}

pub struct BytesBlob;

impl MetaDescribe for BytesBlob {
    fn describe() -> triblespace::core::trible::Fragment {
        let id: Id = id_hex!("B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0");
        entity! { ExclusiveId::force_ref(&id) @
            metadata::name: "bytesblob",
            metadata::tag:  metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl BlobEncoding for BytesBlob {}

impl Encodes<Bytes> for BytesBlob
{
    type Output = Blob<BytesBlob>;
    fn encode(source: Bytes) -> Blob<BytesBlob> {
        Blob::new(source)
    }
}

impl TryFromBlob<BytesBlob> for Bytes {
    type Error = Infallible;
    fn try_from_blob(b: Blob<BytesBlob>) -> Result<Self, Self::Error> {
        Ok(b.bytes)
    }
}

// ANCHOR_END: custom_schema

fn main() {}
