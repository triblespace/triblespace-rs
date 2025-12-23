use anybytes::Bytes;
use std::convert::Infallible;
use triblespace::core::blob::Blob;
use triblespace::core::blob::BlobSchema;
use triblespace::core::blob::ToBlob;
use triblespace::core::blob::TryFromBlob;
use triblespace::core::id::id_hex;
use triblespace::core::id::Id;
use triblespace::core::metadata::ConstMetadata;
use triblespace::core::repo::BlobStore;
use triblespace::core::value::schemas::hash::Blake3;
use triblespace::core::value::FromValue;
use triblespace::core::value::ToValue;
use triblespace::core::value::Value;
use triblespace::core::value::ValueSchema;
use triblespace::core::value::VALUE_LEN;

// ANCHOR: custom_schema

pub struct U64LE;

impl ConstMetadata for U64LE {
    fn id() -> Id {
        id_hex!("0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> triblespace::core::trible::TribleSet {
        let _ = blobs;
        triblespace::core::trible::TribleSet::new()
    }
}

impl ValueSchema for U64LE {
    type ValidationError = Infallible;
}

impl ToValue<U64LE> for u64 {
    fn to_value(self) -> Value<U64LE> {
        let mut raw = [0u8; VALUE_LEN];
        raw[..8].copy_from_slice(&self.to_le_bytes());
        Value::new(raw)
    }
}

impl FromValue<'_, U64LE> for u64 {
    fn from_value(v: &Value<U64LE>) -> Self {
        u64::from_le_bytes(v.raw[..8].try_into().unwrap())
    }
}

pub struct BytesBlob;

impl ConstMetadata for BytesBlob {
    fn id() -> Id {
        id_hex!("B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0")
    }
}

impl BlobSchema for BytesBlob {}

impl ToBlob<BytesBlob> for Bytes {
    fn to_blob(self) -> Blob<BytesBlob> {
        Blob::new(self)
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
