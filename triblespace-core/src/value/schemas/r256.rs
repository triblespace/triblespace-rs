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
use crate::value::TryFromValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use std::convert::TryInto;

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
use num_rational::Ratio;

/// A 256-bit ratio value.
/// It is stored as two 128-bit signed integers, the numerator and the denominator.
/// The ratio is always reduced to its canonical form, which mean that the numerator and the denominator
/// are coprime and the denominator is positive.
/// Both the numerator and the denominator are stored in little-endian byte order,
/// with the numerator in the first 16 bytes and the denominator in the last 16 bytes.
///
/// For a big-endian version, see [R256BE].
pub struct R256LE;

/// A 256-bit ratio value.
/// It is stored as two 128-bit signed integers, the numerator and the denominator.
/// The ratio is always reduced to its canonical form, which mean that the numerator and the denominator
/// are coprime and the denominator is positive.
/// Both the numerator and the denominator are stored in big-endian byte order,
/// with the numerator in the first 16 bytes and the denominator in the last 16 bytes.
///
/// For a little-endian version, see [R256LE].
pub struct R256BE;

pub type R256 = R256LE;

impl ConstMetadata for R256LE {
    fn id() -> Id {
        id_hex!("0A9B43C5C2ECD45B257CDEFC16544358")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @ metadata::tag: metadata::KIND_VALUE_SCHEMA
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put::<WasmCode, _>(wasm_formatter::R256_LE_WASM)?,
            };
        }
        #[cfg(not(feature = "wasm"))]
        let _ = (blobs, &mut tribles);
        Ok(tribles)
    }
}
impl ValueSchema for R256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for R256BE {
    fn id() -> Id {
        id_hex!("CA5EAF567171772C1FFD776E9C7C02D1")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @ metadata::tag: metadata::KIND_VALUE_SCHEMA
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put::<WasmCode, _>(wasm_formatter::R256_BE_WASM)?,
            };
        }
        #[cfg(not(feature = "wasm"))]
        let _ = (blobs, &mut tribles);
        Ok(tribles)
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn r256_le(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let numer = i128::from_le_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let denom = i128::from_le_bytes(buf);

        if denom == 0 {
            return Err(2);
        }

        if denom == 1 {
            write!(out, "{numer}").map_err(|_| 1u32)?;
        } else {
            write!(out, "{numer}/{denom}").map_err(|_| 1u32)?;
        }
        Ok(())
    }

    #[value_formatter]
    pub(crate) fn r256_be(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let numer = i128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let denom = i128::from_be_bytes(buf);

        if denom == 0 {
            return Err(2);
        }

        if denom == 1 {
            write!(out, "{numer}").map_err(|_| 1u32)?;
        } else {
            write!(out, "{numer}/{denom}").map_err(|_| 1u32)?;
        }
        Ok(())
    }
}
impl ValueSchema for R256BE {
    type ValidationError = Infallible;
}

/// An error that can occur when converting a ratio value.
///
/// The error can be caused by a non-canonical ratio, where the numerator and the denominator are not coprime,
/// or by a zero denominator.
#[derive(Debug)]
pub enum RatioError {
    NonCanonical(i128, i128),
    ZeroDenominator,
}

impl TryFromValue<'_, R256BE> for Ratio<i128> {
    type Error = RatioError;

    fn try_from_value(v: &Value<R256BE>) -> Result<Self, Self::Error> {
        let n = i128::from_be_bytes(v.raw[0..16].try_into().unwrap());
        let d = i128::from_be_bytes(v.raw[16..32].try_into().unwrap());

        if d == 0 {
            return Err(RatioError::ZeroDenominator);
        }

        let ratio = Ratio::new_raw(n, d);
        let ratio = ratio.reduced();
        let (reduced_n, reduced_d) = ratio.into_raw();

        if reduced_n != n || reduced_d != d {
            Err(RatioError::NonCanonical(n, d))
        } else {
            Ok(ratio)
        }
    }
}

impl FromValue<'_, R256BE> for Ratio<i128> {
    fn from_value(v: &Value<R256BE>) -> Self {
        match Ratio::try_from_value(v) {
            Ok(ratio) => ratio,
            Err(RatioError::NonCanonical(n, d)) => {
                panic!("Non canonical ratio: {n}/{d}");
            }
            Err(RatioError::ZeroDenominator) => {
                panic!("Zero denominator ratio");
            }
        }
    }
}

impl ToValue<R256BE> for Ratio<i128> {
    fn to_value(self) -> Value<R256BE> {
        let ratio = self.reduced();

        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&ratio.numer().to_be_bytes());
        bytes[16..32].copy_from_slice(&ratio.denom().to_be_bytes());

        Value::new(bytes)
    }
}

impl ToValue<R256BE> for i128 {
    fn to_value(self) -> Value<R256BE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&self.to_be_bytes());
        bytes[16..32].copy_from_slice(&1i128.to_be_bytes());

        Value::new(bytes)
    }
}

impl TryFromValue<'_, R256LE> for Ratio<i128> {
    type Error = RatioError;

    fn try_from_value(v: &Value<R256LE>) -> Result<Self, Self::Error> {
        let n = i128::from_le_bytes(v.raw[0..16].try_into().unwrap());
        let d = i128::from_le_bytes(v.raw[16..32].try_into().unwrap());

        if d == 0 {
            return Err(RatioError::ZeroDenominator);
        }

        let ratio = Ratio::new_raw(n, d);
        let ratio = ratio.reduced();
        let (reduced_n, reduced_d) = ratio.into_raw();

        if reduced_n != n || reduced_d != d {
            Err(RatioError::NonCanonical(n, d))
        } else {
            Ok(ratio)
        }
    }
}

impl FromValue<'_, R256LE> for Ratio<i128> {
    fn from_value(v: &Value<R256LE>) -> Self {
        match Ratio::try_from_value(v) {
            Ok(ratio) => ratio,
            Err(RatioError::NonCanonical(n, d)) => {
                panic!("Non canonical ratio: {n}/{d}");
            }
            Err(RatioError::ZeroDenominator) => {
                panic!("Zero denominator ratio");
            }
        }
    }
}

impl ToValue<R256LE> for Ratio<i128> {
    fn to_value(self) -> Value<R256LE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&self.numer().to_le_bytes());
        bytes[16..32].copy_from_slice(&self.denom().to_le_bytes());

        Value::new(bytes)
    }
}

impl ToValue<R256LE> for i128 {
    fn to_value(self) -> Value<R256LE> {
        let mut bytes = [0; 32];
        bytes[0..16].copy_from_slice(&self.to_le_bytes());
        bytes[16..32].copy_from_slice(&1i128.to_le_bytes());

        Value::new(bytes)
    }
}
