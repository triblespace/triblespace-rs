use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::inline::Encodes;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::IntoInline;
use crate::inline::TryFromInline;
use crate::inline::TryToInline;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use std::convert::Infallible;
use std::fmt;

use f256::f256;
use serde_json::Number as JsonNumber;

/// A inline encoding for a 256-bit floating point number in little-endian byte order.
pub struct F256LE;

/// A inline encoding for a 256-bit floating point number in big-endian byte order.
pub struct F256BE;

/// Type alias for [`F256LE`], the default little-endian 256-bit float schema.
pub type F256 = F256LE;

impl MetaDescribe for F256LE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("D9A419D3CAA0D8E05D8DAB950F5E80F2");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "f256le",
                metadata::description: "High-precision f256 float stored in little-endian byte order. The format preserves far more precision than f64 and can round-trip large JSON numbers.\n\nUse when precision or exact decimal import matters more than storage or compute cost. Choose the big-endian variant if you need lexicographic ordering or network byte order.\n\nF256 values are heavier to parse and compare than f64. If you only need standard double precision, prefer F64 for faster operations.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatter::F256_LE_WASM,
            };
        }
        tribles
    }
}
impl InlineEncoding for F256LE {
    type ValidationError = Infallible;
    type Encoding = Self;
}
impl MetaDescribe for F256BE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("A629176D4656928D96B155038F9F2220");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "f256be",
                metadata::description: "High-precision f256 float stored in big-endian byte order. This variant is convenient for bytewise ordering or wire formats that expect network order.\n\nUse for high-precision metrics or lossless JSON import when ordering matters across systems. For everyday numeric values, F64 is smaller and faster.\n\nAs with all floats, rounding can still occur at the chosen precision. If you need exact fractions, use R256 instead.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatter::F256_BE_WASM,
            };
        }
        tribles
    }
}
impl InlineEncoding for F256BE {
    type ValidationError = Infallible;
    type Encoding = Self;
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter(const_wasm = F256_LE_WASM)]
    pub(crate) fn f256_le(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[0..16]);
        let lo = u128::from_le_bytes(buf);
        buf.copy_from_slice(&raw[16..32]);
        let hi = u128::from_le_bytes(buf);

        const EXP_BITS: u32 = 19;
        const HI_FRACTION_BITS: u32 = 108;
        const EXP_MAX: u32 = (1u32 << EXP_BITS) - 1;
        const EXP_BIAS: i32 = (EXP_MAX >> 1) as i32;

        const HI_SIGN_MASK: u128 = 1u128 << 127;
        const HI_EXP_MASK: u128 = (EXP_MAX as u128) << HI_FRACTION_BITS;
        const HI_FRACTION_MASK: u128 = (1u128 << HI_FRACTION_BITS) - 1;

        let sign = (hi & HI_SIGN_MASK) != 0;
        let exp = ((hi & HI_EXP_MASK) >> HI_FRACTION_BITS) as u32;

        let frac_hi = hi & HI_FRACTION_MASK;
        let frac_lo = lo;
        let fraction_is_zero = frac_hi == 0 && frac_lo == 0;

        if exp == EXP_MAX {
            let text = if fraction_is_zero {
                if sign {
                    "-inf"
                } else {
                    "inf"
                }
            } else {
                "nan"
            };
            out.write_str(text).map_err(|_| 1u32)?;
            return Ok(());
        }

        if exp == 0 && fraction_is_zero {
            let text = if sign { "-0" } else { "0" };
            out.write_str(text).map_err(|_| 1u32)?;
            return Ok(());
        }

        const HEX: &[u8; 16] = b"0123456789ABCDEF";

        if sign {
            out.write_char('-').map_err(|_| 1u32)?;
        }

        let exp2 = if exp == 0 {
            1 - EXP_BIAS
        } else {
            exp as i32 - EXP_BIAS
        };
        if exp == 0 {
            out.write_str("0x0").map_err(|_| 1u32)?;
        } else {
            out.write_str("0x1").map_err(|_| 1u32)?;
        }

        let mut digits = [0u8; 59];
        for i in 0..27 {
            let shift = (26 - i) * 4;
            let nibble = ((frac_hi >> shift) & 0xF) as usize;
            digits[i] = HEX[nibble];
        }
        for i in 0..32 {
            let shift = (31 - i) * 4;
            let nibble = ((frac_lo >> shift) & 0xF) as usize;
            digits[27 + i] = HEX[nibble];
        }

        let mut end = digits.len();
        while end > 0 && digits[end - 1] == b'0' {
            end -= 1;
        }
        if end > 0 {
            out.write_char('.').map_err(|_| 1u32)?;
            for &b in &digits[0..end] {
                out.write_char(b as char).map_err(|_| 1u32)?;
            }
        }

        write!(out, "p{exp2:+}").map_err(|_| 1u32)?;
        Ok(())
    }

    #[value_formatter(const_wasm = F256_BE_WASM)]
    pub(crate) fn f256_be(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[0..16]);
        let hi = u128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..32]);
        let lo = u128::from_be_bytes(buf);

        const EXP_BITS: u32 = 19;
        const HI_FRACTION_BITS: u32 = 108;
        const EXP_MAX: u32 = (1u32 << EXP_BITS) - 1;
        const EXP_BIAS: i32 = (EXP_MAX >> 1) as i32;

        const HI_SIGN_MASK: u128 = 1u128 << 127;
        const HI_EXP_MASK: u128 = (EXP_MAX as u128) << HI_FRACTION_BITS;
        const HI_FRACTION_MASK: u128 = (1u128 << HI_FRACTION_BITS) - 1;

        let sign = (hi & HI_SIGN_MASK) != 0;
        let exp = ((hi & HI_EXP_MASK) >> HI_FRACTION_BITS) as u32;

        let frac_hi = hi & HI_FRACTION_MASK;
        let frac_lo = lo;
        let fraction_is_zero = frac_hi == 0 && frac_lo == 0;

        if exp == EXP_MAX {
            let text = if fraction_is_zero {
                if sign {
                    "-inf"
                } else {
                    "inf"
                }
            } else {
                "nan"
            };
            out.write_str(text).map_err(|_| 1u32)?;
            return Ok(());
        }

        if exp == 0 && fraction_is_zero {
            let text = if sign { "-0" } else { "0" };
            out.write_str(text).map_err(|_| 1u32)?;
            return Ok(());
        }

        const HEX: &[u8; 16] = b"0123456789ABCDEF";

        if sign {
            out.write_char('-').map_err(|_| 1u32)?;
        }

        let exp2 = if exp == 0 {
            1 - EXP_BIAS
        } else {
            exp as i32 - EXP_BIAS
        };
        if exp == 0 {
            out.write_str("0x0").map_err(|_| 1u32)?;
        } else {
            out.write_str("0x1").map_err(|_| 1u32)?;
        }

        let mut digits = [0u8; 59];
        for i in 0..27 {
            let shift = (26 - i) * 4;
            let nibble = ((frac_hi >> shift) & 0xF) as usize;
            digits[i] = HEX[nibble];
        }
        for i in 0..32 {
            let shift = (31 - i) * 4;
            let nibble = ((frac_lo >> shift) & 0xF) as usize;
            digits[27 + i] = HEX[nibble];
        }

        let mut end = digits.len();
        while end > 0 && digits[end - 1] == b'0' {
            end -= 1;
        }
        if end > 0 {
            out.write_char('.').map_err(|_| 1u32)?;
            for &b in &digits[0..end] {
                out.write_char(b as char).map_err(|_| 1u32)?;
            }
        }

        write!(out, "p{exp2:+}").map_err(|_| 1u32)?;
        Ok(())
    }
}

impl TryFromInline<'_, F256BE> for f256 {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<F256BE>) -> Result<Self, Infallible> {
        Ok(f256::from_be_bytes(v.raw))
    }
}

impl Encodes<f256> for F256BE {
    type Output = Inline<F256BE>;
    fn encode(source: f256) -> Inline<F256BE> {
        Inline::new(source.to_be_bytes())
    }
}

impl TryFromInline<'_, F256LE> for f256 {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<F256LE>) -> Result<Self, Infallible> {
        Ok(f256::from_le_bytes(v.raw))
    }
}

impl Encodes<f256> for F256LE {
    type Output = Inline<F256LE>;
    fn encode(source: f256) -> Inline<F256LE> {
        Inline::new(source.to_le_bytes())
    }
}

/// Errors encountered when converting JSON numbers into [`F256`] values.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonNumberToF256Error {
    /// The numeric value could not be represented as an `f256`.
    Unrepresentable,
}

impl fmt::Display for JsonNumberToF256Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonNumberToF256Error::Unrepresentable => {
                write!(f, "number is too large to represent as f256")
            }
        }
    }
}

impl std::error::Error for JsonNumberToF256Error {}

impl TryToInline<F256> for JsonNumber {
    type Error = JsonNumberToF256Error;

    fn try_to_inline(self) -> Result<Inline<F256>, Self::Error> {
        (&self).try_to_inline()
    }
}

impl TryToInline<F256> for &JsonNumber {
    type Error = JsonNumberToF256Error;

    fn try_to_inline(self) -> Result<Inline<F256>, Self::Error> {
        if let Some(value) = self.as_u128() {
            return Ok(f256::from(value).to_inline());
        }
        if let Some(value) = self.as_i128() {
            return Ok(f256::from(value).to_inline());
        }
        if let Some(value) = self.as_f64() {
            return Ok(f256::from(value).to_inline());
        }
        Err(JsonNumberToF256Error::Unrepresentable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::{IntoInline, TryToInline};
    use ::f256::f256;
    use proptest::prelude::*;

    /// Generate an f256 from an f64, filtering out NaN (NaN != NaN).
    fn arb_f256_non_nan() -> impl Strategy<Value = f256> {
        any::<f64>()
            .prop_filter("not NaN", |v| !v.is_nan())
            .prop_map(f256::from)
    }

    proptest! {
        #[test]
        fn f256le_roundtrip(input in arb_f256_non_nan()) {
            let value: Inline<F256LE> = input.to_inline();
            let output: f256 = value.from_inline();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn f256be_roundtrip(input in arb_f256_non_nan()) {
            let value: Inline<F256BE> = input.to_inline();
            let output: f256 = value.from_inline();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn f256le_validates(input in arb_f256_non_nan()) {
            let value: Inline<F256LE> = input.to_inline();
            prop_assert!(F256LE::validate(value).is_ok());
        }

        #[test]
        fn f256be_validates(input in arb_f256_non_nan()) {
            let value: Inline<F256BE> = input.to_inline();
            prop_assert!(F256BE::validate(value).is_ok());
        }

        #[test]
        fn f256_le_and_be_differ(input in arb_f256_non_nan().prop_filter("non-zero", |v| *v != f256::ZERO)) {
            let le_val: Inline<F256LE> = input.to_inline();
            let be_val: Inline<F256BE> = input.to_inline();
            prop_assert_ne!(le_val.raw, be_val.raw);
        }

        #[test]
        fn json_number_u128_roundtrip(input: u64) {
            let s = input.to_string();
            let num: JsonNumber = serde_json::from_str(&s).unwrap();
            let value: Inline<F256> = num.try_to_inline().expect("valid number");
            let output: f256 = value.from_inline();
            prop_assert_eq!(output, f256::from(input as u128));
        }

        #[test]
        fn json_number_negative_roundtrip(input in any::<i64>().prop_filter("negative", |v| *v < 0)) {
            let s = input.to_string();
            let num: JsonNumber = serde_json::from_str(&s).unwrap();
            let value: Inline<F256> = num.try_to_inline().expect("valid number");
            let output: f256 = value.from_inline();
            prop_assert_eq!(output, f256::from(input as i128));
        }

        #[test]
        fn json_number_f64_roundtrip(input in any::<f64>().prop_filter("finite", |v| v.is_finite())) {
            let s = ryu::Buffer::new().format(input).to_string();
            let num: JsonNumber = serde_json::from_str(&s).unwrap();
            // Compare via &JsonNumber so we can also inspect the parsed value.
            let expected = f256::from(num.as_f64().unwrap());
            let value: Inline<F256> = (&num).try_to_inline().expect("valid number");
            let output: f256 = value.from_inline();
            // Compare against what serde_json actually parsed (via as_f64),
            // not the original f64, since JSON string round-tripping can
            // shift the least-significant bit.
            prop_assert_eq!(output, expected);
        }

        #[test]
        fn json_number_ref_roundtrip(input: u64) {
            let s = input.to_string();
            let num: JsonNumber = serde_json::from_str(&s).unwrap();
            let value: Inline<F256> = (&num).try_to_inline().expect("valid ref number");
            let output: f256 = value.from_inline();
            prop_assert_eq!(output, f256::from(input as u128));
        }
    }

    // NaN round-trip must use is_nan() since NaN != NaN.
    #[test]
    fn f256_le_roundtrip_nan() {
        let input = f256::NAN;
        let value: Inline<F256LE> = input.to_inline();
        let output: f256 = value.from_inline();
        assert!(output.is_nan());
    }
}
