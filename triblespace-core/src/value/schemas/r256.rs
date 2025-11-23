use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use std::convert::TryInto;

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
}
impl ValueSchema for R256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for R256BE {
    fn id() -> Id {
        id_hex!("CA5EAF567171772C1FFD776E9C7C02D1")
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
