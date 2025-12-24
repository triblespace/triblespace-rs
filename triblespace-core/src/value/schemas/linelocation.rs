use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::FromValue;
use crate::value::RawValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use proc_macro::Span;
use std::convert::Infallible;

/// A value schema for representing a span using explicit line and column
/// coordinates.
#[derive(Debug, Clone, Copy)]
pub struct LineLocation;

impl ConstMetadata for LineLocation {
    fn id() -> Id {
        id_hex!("DFAED173A908498CB893A076EAD3E578")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = super::wasm_formatters::describe_value_formatter(
            blobs,
            Self::id(),
            super::wasm_formatters::LINELOCATION_WASM,
        );
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
        tribles
    }
}

impl ValueSchema for LineLocation {
    type ValidationError = Infallible;
}

fn encode_location(lines: (u64, u64, u64, u64)) -> RawValue {
    let mut raw = [0u8; 32];
    raw[..8].copy_from_slice(&lines.0.to_be_bytes());
    raw[8..16].copy_from_slice(&lines.1.to_be_bytes());
    raw[16..24].copy_from_slice(&lines.2.to_be_bytes());
    raw[24..].copy_from_slice(&lines.3.to_be_bytes());
    raw
}

fn decode_location(raw: &RawValue) -> (u64, u64, u64, u64) {
    let mut first = [0u8; 8];
    let mut second = [0u8; 8];
    let mut third = [0u8; 8];
    let mut fourth = [0u8; 8];
    first.copy_from_slice(&raw[..8]);
    second.copy_from_slice(&raw[8..16]);
    third.copy_from_slice(&raw[16..24]);
    fourth.copy_from_slice(&raw[24..]);
    (
        u64::from_be_bytes(first),
        u64::from_be_bytes(second),
        u64::from_be_bytes(third),
        u64::from_be_bytes(fourth),
    )
}

impl ToValue<LineLocation> for (u64, u64, u64, u64) {
    fn to_value(self) -> Value<LineLocation> {
        Value::new(encode_location(self))
    }
}

impl FromValue<'_, LineLocation> for (u64, u64, u64, u64) {
    fn from_value(v: &Value<LineLocation>) -> Self {
        decode_location(&v.raw)
    }
}

impl ToValue<LineLocation> for Span {
    fn to_value(self) -> Value<LineLocation> {
        (
            self.start().line() as u64,
            self.start().column() as u64,
            self.end().line() as u64,
            self.end().column() as u64,
        )
            .to_value()
    }
}
