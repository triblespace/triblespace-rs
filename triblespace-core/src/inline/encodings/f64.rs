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
use serde_json::Number as JsonNumber;
use std::convert::Infallible;
use std::fmt;

/// A inline encoding for an IEEE-754 double in little-endian byte order.
pub struct F64;

impl MetaDescribe for F64 {
    fn describe() -> Fragment {
        let id: Id = id_hex!("C80A60F4A6F2FBA5A8DB2531A923EC70");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "f64",
                metadata::description: "IEEE-754 double stored in the first 8 bytes (little-endian); remaining bytes are zero. This matches the standard host representation while preserving the 32-byte value width.\n\nUse for typical metrics, measurements, and calculations where floating-point rounding is acceptable. Choose F256 for higher precision or lossless JSON number import, and R256 for exact rational values.\n\nNaN and infinity can be represented; decide whether your application accepts them. If you need deterministic ordering or exact comparisons, prefer integer or rational schemas.",
                metadata::tag: metadata::KIND_INLINE_ENCODING,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: wasm_formatter::F64_WASM,
            };
        }
        tribles
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter(const_wasm = F64_WASM)]
    pub(crate) fn float64(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&raw[..8]);
        let value = f64::from_le_bytes(bytes);
        write!(out, "{value}").map_err(|_| 1u32)?;
        Ok(())
    }
}

impl InlineEncoding for F64 {
    type ValidationError = Infallible;
    type Encoding = Self;
}

impl TryFromInline<'_, F64> for f64 {
    type Error = Infallible;
    fn try_from_inline(v: &Inline<F64>) -> Result<Self, Infallible> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.raw[..8]);
        Ok(f64::from_le_bytes(bytes))
    }
}

impl Encodes<f64> for F64 {
    type Output = Inline<F64>;
    fn encode(source: f64) -> Inline<F64> {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&source.to_le_bytes());
        Inline::new(raw)
    }
}

/// Errors encountered when converting JSON numbers into [`F64`] values.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonNumberToF64Error {
    /// The numeric value could not be represented as an `f64` (non-finite or out of range).
    Unrepresentable,
}

impl fmt::Display for JsonNumberToF64Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonNumberToF64Error::Unrepresentable => {
                write!(f, "number is too large to represent as f64")
            }
        }
    }
}

impl std::error::Error for JsonNumberToF64Error {}

impl TryToInline<F64> for JsonNumber {
    type Error = JsonNumberToF64Error;

    fn try_to_inline(self) -> Result<Inline<F64>, Self::Error> {
        (&self).try_to_inline()
    }
}

impl TryToInline<F64> for &JsonNumber {
    type Error = JsonNumberToF64Error;

    fn try_to_inline(self) -> Result<Inline<F64>, Self::Error> {
        if let Some(value) = self.as_f64().filter(|v| v.is_finite()) {
            return Ok(value.to_inline());
        }
        Err(JsonNumberToF64Error::Unrepresentable)
    }
}
