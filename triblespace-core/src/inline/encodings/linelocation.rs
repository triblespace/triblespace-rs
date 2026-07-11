use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::Encodes;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::IntoInline;
use crate::inline::RawInline;
use crate::inline::TryFromInline;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use proc_macro::Span;
use std::convert::Infallible;

/// A inline encoding for representing a span using explicit line and column
/// coordinates.
#[derive(Debug, Clone, Copy)]
pub struct LineLocation;

impl MetaDescribe for LineLocation {
    fn describe() -> Fragment {
        let id: Id = id_hex!("DFAED173A908498CB893A076EAD3E578");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "line_location",
                metadata::description: "Line/column span encoded as four big-endian u64 values (start_line, start_col, end_line, end_col). This captures explicit source positions rather than byte offsets.\n\nUse for editor diagnostics, source maps, or human-facing spans. If you need byte offsets or length-based ranges, use RangeU128 instead.\n\nColumns are raw counts and do not account for variable-width graphemes. Store any display conventions separately if needed.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatter::LINELOCATION_WASM,
            };
        }
        tribles
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

impl InlineEncoding for LineLocation {
    type ValidationError = Infallible;
    type Encoding = Self;
}

fn encode_location(lines: (u64, u64, u64, u64)) -> RawInline {
    let mut raw = [0u8; 32];
    raw[..8].copy_from_slice(&lines.0.to_be_bytes());
    raw[8..16].copy_from_slice(&lines.1.to_be_bytes());
    raw[16..24].copy_from_slice(&lines.2.to_be_bytes());
    raw[24..].copy_from_slice(&lines.3.to_be_bytes());
    raw
}

fn decode_location(raw: &RawInline) -> (u64, u64, u64, u64) {
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

impl Encodes<(u64, u64, u64, u64)> for LineLocation {
    type Output = Inline<LineLocation>;
    fn encode(source: (u64, u64, u64, u64)) -> Inline<LineLocation> {
        Inline::new(encode_location(source))
    }
}

impl TryFromInline<'_, LineLocation> for (u64, u64, u64, u64) {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<LineLocation>) -> Result<Self, Infallible> {
        Ok(decode_location(&v.raw))
    }
}

impl Encodes<Span> for LineLocation {
    type Output = Inline<LineLocation>;
    fn encode(source: Span) -> Inline<LineLocation> {
        (
            source.start().line() as u64,
            source.start().column() as u64,
            source.end().line() as u64,
            source.end().column() as u64,
        )
            .to_inline()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::IntoInline;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn tuple_roundtrip(a: u64, b: u64, c: u64, d: u64) {
            let input = (a, b, c, d);
            let value: Inline<LineLocation> = input.to_inline();
            let output: (u64, u64, u64, u64) = value.from_inline();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn validates(a: u64, b: u64, c: u64, d: u64) {
            let value: Inline<LineLocation> = (a, b, c, d).to_inline();
            prop_assert!(LineLocation::validate(value).is_ok());
        }
    }
}
