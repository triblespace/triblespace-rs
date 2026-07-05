use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::Encodes;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::TryFromInline;
use crate::inline::TryToInline;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use std::convert::Infallible;
use std::ops::{Range, RangeInclusive};

/// A inline encoding for representing a pair of `u128` values.
///
/// [`RangeU128`] encodes the pair as a half-open interval while
/// [`RangeInclusiveU128`] represents an inclusive range. Both schemas encode the
/// endpoints by packing the line into the high 64 bits and the column into the
/// low 64 bits of the `u128`.
#[derive(Debug, Clone, Copy)]
pub struct RangeU128;

/// Inclusive range of two `u128` values (`start..=end`), big-endian encoded.
#[derive(Debug, Clone, Copy)]
pub struct RangeInclusiveU128;

impl MetaDescribe for RangeU128 {
    fn describe() -> Fragment {
        let id: Id = id_hex!("A4E25E3B92364FA5AB519C6A77D7CB3A");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "range_u128",
                metadata::description: "Half-open range encoded as two big-endian u128 values (start..end). This mirrors common slice semantics where the end is exclusive.\n\nUse for offsets, byte ranges, and spans where length matters and empty ranges are valid. Use RangeInclusiveU128 when both endpoints should be included.\n\nNo normalization is enforced; callers should ensure start <= end and interpret units consistently.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatters::RANGE_U128_WASM,
            };
        }
        tribles
    }
}

impl InlineEncoding for RangeU128 {
    type ValidationError = Infallible;
    type Encoding = Self;
}

impl MetaDescribe for RangeInclusiveU128 {
    fn describe() -> Fragment {
        let id: Id = id_hex!("1D0D82CA84424CD0A2F98DB37039E152");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "range_u128_inc",
                metadata::description: "Inclusive range encoded as two big-endian u128 values (start..=end). This is convenient when both endpoints are meaningful.\n\nUse for closed intervals such as line/column ranges or inclusive numeric bounds. Prefer RangeU128 for half-open intervals and length-based calculations.\n\nCallers should decide how to handle empty or reversed ranges; the schema only defines the byte layout.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatters::RANGE_INCLUSIVE_U128_WASM,
            };
        }
        tribles
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatters {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn range_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let start = u128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let end = u128::from_be_bytes(buf);
        write!(out, "{start}..{end}").map_err(|_| 1u32)?;
        Ok(())
    }

    #[value_formatter]
    pub(crate) fn range_inclusive_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let start = u128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let end = u128::from_be_bytes(buf);
        write!(out, "{start}..={end}").map_err(|_| 1u32)?;
        Ok(())
    }
}

impl InlineEncoding for RangeInclusiveU128 {
    type ValidationError = Infallible;
    type Encoding = Self;
}

fn encode_pair(range: (u128, u128)) -> RawInline {
    let mut raw = [0u8; 32];
    raw[..16].copy_from_slice(&range.0.to_be_bytes());
    raw[16..].copy_from_slice(&range.1.to_be_bytes());
    raw
}

fn decode_pair(raw: &RawInline) -> (u128, u128) {
    let mut first = [0u8; 16];
    let mut second = [0u8; 16];
    first.copy_from_slice(&raw[..16]);
    second.copy_from_slice(&raw[16..]);
    (u128::from_be_bytes(first), u128::from_be_bytes(second))
}

fn encode_range_value<S: InlineEncoding>(range: (u128, u128)) -> Inline<S> {
    Inline::new(encode_pair(range))
}

fn decode_range_value<S: InlineEncoding>(value: &Inline<S>) -> (u128, u128) {
    decode_pair(&value.raw)
}

impl Encodes<(u128, u128)> for RangeU128 {
    type Output = Inline<RangeU128>;
    fn encode(source: (u128, u128)) -> Inline<RangeU128> {
        encode_range_value(source)
    }
}

impl TryFromInline<'_, RangeU128> for (u128, u128) {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<RangeU128>) -> Result<Self, Infallible> {
        Ok(decode_range_value(v))
    }
}

impl Encodes<(u128, u128)> for RangeInclusiveU128 {
    type Output = Inline<RangeInclusiveU128>;
    fn encode(source: (u128, u128)) -> Inline<RangeInclusiveU128> {
        encode_range_value(source)
    }
}

impl TryFromInline<'_, RangeInclusiveU128> for (u128, u128) {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<RangeInclusiveU128>) -> Result<Self, Infallible> {
        Ok(decode_range_value(v))
    }
}

impl TryToInline<RangeU128> for Range<u128> {
    type Error = Infallible;

    fn try_to_inline(self) -> Result<Inline<RangeU128>, Self::Error> {
        Ok(encode_range_value((self.start, self.end)))
    }
}

impl TryFromInline<'_, RangeU128> for Range<u128> {
    type Error = Infallible;

    fn try_from_inline(v: &Inline<RangeU128>) -> Result<Self, Self::Error> {
        let (start, end) = decode_range_value(v);
        Ok(start..end)
    }
}

impl TryToInline<RangeInclusiveU128> for RangeInclusive<u128> {
    type Error = Infallible;

    fn try_to_inline(self) -> Result<Inline<RangeInclusiveU128>, Self::Error> {
        let (start, end) = self.into_inner();
        Ok(encode_range_value((start, end)))
    }
}

impl TryFromInline<'_, RangeInclusiveU128> for RangeInclusive<u128> {
    type Error = Infallible;

    fn try_from_inline(v: &Inline<RangeInclusiveU128>) -> Result<Self, Self::Error> {
        let (start, end) = decode_range_value(v);
        Ok(start..=end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::{IntoInline, TryFromInline, TryToInline};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn range_u128_tuple_roundtrip(a: u128, b: u128) {
            let input = (a, b);
            let value: Inline<RangeU128> = input.to_inline();
            let output: (u128, u128) = value.from_inline();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn range_u128_range_roundtrip(a: u128, b: u128) {
            let input = a..b;
            let value: Inline<RangeU128> = input.clone().try_to_inline().unwrap();
            let output = Range::<u128>::try_from_inline(&value).unwrap();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn range_inclusive_tuple_roundtrip(a: u128, b: u128) {
            let input = (a, b);
            let value: Inline<RangeInclusiveU128> = input.to_inline();
            let output: (u128, u128) = value.from_inline();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn range_inclusive_range_roundtrip(a: u128, b: u128) {
            let input = a..=b;
            let value: Inline<RangeInclusiveU128> = input.clone().try_to_inline().unwrap();
            let output = RangeInclusive::<u128>::try_from_inline(&value).unwrap();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn range_u128_tuple_and_range_agree(a: u128, b: u128) {
            let tuple_val: Inline<RangeU128> = (a, b).to_inline();
            let range_val: Inline<RangeU128> = (a..b).try_to_inline().unwrap();
            prop_assert_eq!(tuple_val.raw, range_val.raw);
        }

        #[test]
        fn range_inclusive_tuple_and_range_agree(a: u128, b: u128) {
            let tuple_val: Inline<RangeInclusiveU128> = (a, b).to_inline();
            let range_val: Inline<RangeInclusiveU128> = (a..=b).try_to_inline().unwrap();
            prop_assert_eq!(tuple_val.raw, range_val.raw);
        }

        #[test]
        fn range_u128_validates(a: u128, b: u128) {
            let value: Inline<RangeU128> = (a, b).to_inline();
            prop_assert!(RangeU128::validate(value).is_ok());
        }

        #[test]
        fn range_inclusive_validates(a: u128, b: u128) {
            let value: Inline<RangeInclusiveU128> = (a, b).to_inline();
            prop_assert!(RangeInclusiveU128::validate(value).is_ok());
        }
    }
}
