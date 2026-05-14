use anybytes::Bytes;
use std::convert::Infallible;
use triblespace::core::blob::Blob;
use triblespace::core::blob::BlobSchema;
use triblespace::core::blob::IntoBlob;
use triblespace::core::blob::TryFromBlob;
use triblespace::core::id::id_hex;
use triblespace::core::metadata::MetaDescribe;
use triblespace::core::value::TryFromValue;
use triblespace::core::value::IntoValue;
use triblespace::core::value::Value;
use triblespace::core::value::ValueSchema;
use triblespace::core::value::VALUE_LEN;

// ANCHOR: custom_schema

pub struct U64LE;

impl MetaDescribe for U64LE {
    fn describe() -> triblespace::core::trible::Fragment {
        triblespace::core::trible::Fragment::rooted(
            id_hex!("0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A"),
            triblespace::core::trible::TribleSet::new(),
        )
    }
}

impl ValueSchema for U64LE {
    type ValidationError = Infallible;
}

impl IntoValue<U64LE> for u64 {
    fn to_value(self) -> Value<U64LE> {
        let mut raw = [0u8; VALUE_LEN];
        raw[..8].copy_from_slice(&self.to_le_bytes());
        Value::new(raw)
    }
}

impl TryFromValue<'_, U64LE> for u64 {
    type Error = std::convert::Infallible;
    fn try_from_value(v: &Value<U64LE>) -> Result<Self, std::convert::Infallible> {
        Ok(u64::from_le_bytes(v.raw[..8].try_into().unwrap()))
    }
}

pub struct BytesBlob;

impl MetaDescribe for BytesBlob {
    fn describe() -> triblespace::core::trible::Fragment {
        triblespace::core::trible::Fragment::rooted(
            id_hex!("B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0"),
            triblespace::core::trible::TribleSet::new(),
        )
    }
}

impl BlobSchema for BytesBlob {}

impl IntoBlob<BytesBlob> for Bytes {
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
