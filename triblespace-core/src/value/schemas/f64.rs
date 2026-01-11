use crate::blob::schemas::longstring::LongString;
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
use serde_json::Number as JsonNumber;
use std::convert::Infallible;
use std::fmt;

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
/// A value schema for an IEEE-754 double in little-endian byte order.
pub struct F64;

impl ConstMetadata for F64 {
    fn id() -> Id {
        id_hex!("C80A60F4A6F2FBA5A8DB2531A923EC70")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put::<LongString, _>(
            "IEEE-754 double stored in the first 8 bytes (little-endian); remaining bytes are zero. This matches the standard host representation while preserving the 32-byte value width.\n\nUse for typical metrics, measurements, and calculations where floating-point rounding is acceptable. Choose F256 for higher precision or lossless JSON number import, and R256 for exact rational values.\n\nNaN and infinity can be represented; decide whether your application accepts them. If you need deterministic ordering or exact comparisons, prefer integer or rational schemas.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::shortname: "f64",
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put::<WasmCode, _>(wasm_formatter::F64_WASM)?,
            };
            tribles
        };
        Ok(tribles)
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
