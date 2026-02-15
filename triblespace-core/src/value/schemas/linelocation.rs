use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstId, ConstMetadata};
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

impl ConstId for LineLocation {
    const ID: Id = id_hex!("DFAED173A908498CB893A076EAD3E578");
}

impl ConstMetadata for LineLocation {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "Line/column span encoded as four big-endian u64 values (start_line, start_col, end_line, end_col). This captures explicit source positions rather than byte offsets.\n\nUse for editor diagnostics, source maps, or human-facing spans. If you need byte offsets or length-based ranges, use RangeU128 instead.\n\nColumns are raw counts and do not account for variable-width graphemes. Store any display conventions separately if needed.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("line_location".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::LINELOCATION_WASM)?,
            };
            tribles
        };
        Ok(tribles.into_facts())
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn linelocation(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&raw[..8]);
        let start_line = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[8..16]);
        let start_col = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..24]);
        let end_line = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[24..]);
        let end_col = u64::from_be_bytes(buf);

        write!(out, "{start_line}:{start_col}..{end_line}:{end_col}").map_err(|_| 1u32)?;
        Ok(())
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
