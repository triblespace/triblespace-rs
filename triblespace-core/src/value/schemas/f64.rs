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
use serde_json::Number as JsonNumber;
use std::convert::Infallible;
use std::fmt;

/// A value schema for an IEEE-754 double in little-endian byte order.
pub struct F64;

impl ConstMetadata for F64 {
    fn id() -> Id {
        id_hex!("C80A60F4A6F2FBA5A8DB2531A923EC70")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = super::wasm_formatters::describe_value_formatter(
            blobs,
            Self::id(),
            wasm_formatter::F64_WASM,
        );
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
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

impl ValueSchema for F64 {
    type ValidationError = Infallible;
}

impl FromValue<'_, F64> for f64 {
    fn from_value(v: &Value<F64>) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.raw[..8]);
        f64::from_le_bytes(bytes)
    }
}

impl ToValue<F64> for f64 {
    fn to_value(self) -> Value<F64> {
        let mut raw = [0u8; 32];
        raw[..8].copy_from_slice(&self.to_le_bytes());
        Value::new(raw)
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

impl TryToValue<F64> for JsonNumber {
    type Error = JsonNumberToF64Error;

    fn try_to_value(self) -> Result<Value<F64>, Self::Error> {
        (&self).try_to_value()
    }
}

impl TryToValue<F64> for &JsonNumber {
    type Error = JsonNumberToF64Error;

    fn try_to_value(self) -> Result<Value<F64>, Self::Error> {
        if let Some(value) = self.as_f64().filter(|v| v.is_finite()) {
            return Ok(value.to_value());
        }
        Err(JsonNumberToF64Error::Unrepresentable)
    }
}
