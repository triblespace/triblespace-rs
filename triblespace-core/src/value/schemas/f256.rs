use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;
use std::fmt;

use f256::f256;
use serde_json::Number as JsonNumber;

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
/// A value schema for a 256-bit floating point number in little-endian byte order.
pub struct F256LE;

/// A value schema for a 256-bit floating point number in big-endian byte order.
pub struct F256BE;

/// A type alias for the little-endian version of the 256-bit floating point number.
pub type F256 = F256LE;

impl ConstMetadata for F256LE {
    fn id() -> Id {
        id_hex!("D9A419D3CAA0D8E05D8DAB950F5E80F2")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put(
            "High-precision f256 float stored in little-endian byte order. The format preserves far more precision than f64 and can round-trip large JSON numbers.\n\nUse when precision or exact decimal import matters more than storage or compute cost. Choose the big-endian variant if you need lexicographic ordering or network byte order.\n\nF256 values are heavier to parse and compare than f64. If you only need standard double precision, prefer F64 for faster operations.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("f256le".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::F256_LE_WASM)?,
            };
            tribles
        };
        Ok(tribles)
    }
}
impl ValueSchema for F256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for F256BE {
    fn id() -> Id {
        id_hex!("A629176D4656928D96B155038F9F2220")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put(
            "High-precision f256 float stored in big-endian byte order. This variant is convenient for bytewise ordering or wire formats that expect network order.\n\nUse for high-precision metrics or lossless JSON import when ordering matters across systems. For everyday numeric values, F64 is smaller and faster.\n\nAs with all floats, rounding can still occur at the chosen precision. If you need exact fractions, use R256 instead.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("f256be".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::F256_BE_WASM)?,
            };
            tribles
        };
        Ok(tribles)
    }
}
impl ValueSchema for F256BE {
    type ValidationError = Infallible;
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

impl FromValue<'_, F256BE> for f256 {
    fn from_value(v: &Value<F256BE>) -> Self {
        f256::from_be_bytes(v.raw)
    }
}

impl ToValue<F256BE> for f256 {
    fn to_value(self) -> Value<F256BE> {
        Value::new(self.to_be_bytes())
    }
}

impl FromValue<'_, F256LE> for f256 {
    fn from_value(v: &Value<F256LE>) -> Self {
        f256::from_le_bytes(v.raw)
    }
}

impl ToValue<F256LE> for f256 {
    fn to_value(self) -> Value<F256LE> {
        Value::new(self.to_le_bytes())
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

impl TryToValue<F256> for JsonNumber {
    type Error = JsonNumberToF256Error;

    fn try_to_value(self) -> Result<Value<F256>, Self::Error> {
        (&self).try_to_value()
    }
}

impl TryToValue<F256> for &JsonNumber {
    type Error = JsonNumberToF256Error;

    fn try_to_value(self) -> Result<Value<F256>, Self::Error> {
        if let Some(value) = self.as_u128() {
            return Ok(f256::from(value).to_value());
        }
        if let Some(value) = self.as_i128() {
            return Ok(f256::from(value).to_value());
        }
        if let Some(value) = self.as_f64() {
            return Ok(f256::from(value).to_value());
        }
        Err(JsonNumberToF256Error::Unrepresentable)
    }
}
