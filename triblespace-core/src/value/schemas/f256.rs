use crate::id::Id;
use crate::id_hex;
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

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "builtin-wasm-formatters")]
        let tribles = super::wasm_formatters::describe_value_formatter(
            blobs,
            Self::id(),
            super::wasm_formatters::HEX32_REV_WASM,
        );
        #[cfg(not(feature = "builtin-wasm-formatters"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for F256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for F256BE {
    fn id() -> Id {
        id_hex!("A629176D4656928D96B155038F9F2220")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "builtin-wasm-formatters")]
        let tribles = super::wasm_formatters::describe_value_formatter(
            blobs,
            Self::id(),
            super::wasm_formatters::HEX32_WASM,
        );
        #[cfg(not(feature = "builtin-wasm-formatters"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for F256BE {
    type ValidationError = Infallible;
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
