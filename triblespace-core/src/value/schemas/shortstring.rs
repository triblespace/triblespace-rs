use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;

use indxvec::Printing;
use std::str::Utf8Error;

/// An error that occurs when converting a string to a short string.
/// This error occurs when the string is too long or contains an interior NUL byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FromStrError {
    TooLong,
    InteriorNul,
}

/// Errors that can occur when validating a [`ShortString`] value.
#[derive(Debug)]
pub enum ValidationError {
    InteriorNul,
    Utf8(Utf8Error),
}

/// A value schema for a short string.
/// A short string is a UTF-8 encoded string with a maximum length of 32 bytes (inclusive)
/// The string is null-terminated.
/// If the string is shorter than 32 bytes, the remaining bytes are zero.
/// If the string is exactly 32 bytes, then there is no zero terminator.
pub struct ShortString;

impl ConstMetadata for ShortString {
    fn id() -> Id {
        id_hex!("2D848DB0AF112DB226A6BF1A3640D019")
    }
}

impl ValueSchema for ShortString {
    type ValidationError = ValidationError;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        let raw = &value.raw;
        let len = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
        // ensure all bytes after first NUL are zero
        if raw[len..].iter().any(|&b| b != 0) {
            return Err(ValidationError::InteriorNul);
        }
        std::str::from_utf8(&raw[..len]).map_err(ValidationError::Utf8)?;
        Ok(value)
    }
}

impl<'a> TryFromValue<'a, ShortString> for &'a str {
    type Error = Utf8Error;

    fn try_from_value(v: &'a Value<ShortString>) -> Result<&'a str, Self::Error> {
        let len = v.raw.iter().position(|&b| b == 0).unwrap_or(v.raw.len());
        #[cfg(kani)]
        {
            // Kani spends significant time unwinding the UTF-8 validation loop.
            // Bounding `len` to 32 keeps the verifier from exploring unrealistic
            // larger values, reducing runtime from minutes to seconds.
            kani::assume(len <= 32);
        }
        std::str::from_utf8(&v.raw[..len])
    }
}

impl<'a> TryFromValue<'a, ShortString> for String {
    type Error = Utf8Error;

    fn try_from_value(v: &Value<ShortString>) -> Result<Self, Self::Error> {
        let s: &str = v.try_from_value()?;
        Ok(s.to_string())
    }
}

impl<'a> FromValue<'a, ShortString> for &'a str {
    fn from_value(v: &'a Value<ShortString>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl<'a> FromValue<'a, ShortString> for String {
    fn from_value(v: &'a Value<ShortString>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl TryToValue<ShortString> for &str {
    type Error = FromStrError;

    fn try_to_value(self) -> Result<Value<ShortString>, Self::Error> {
        let bytes = self.as_bytes();
        if bytes.len() > 32 {
            return Err(FromStrError::TooLong);
        }
        if bytes.contains(&0) {
            return Err(FromStrError::InteriorNul);
        }

        let mut data: [u8; 32] = [0; 32];
        data[..bytes.len()].copy_from_slice(bytes);

        Ok(Value::new(data))
    }
}

impl TryToValue<ShortString> for String {
    type Error = FromStrError;

    fn try_to_value(self) -> Result<Value<ShortString>, Self::Error> {
        (&self[..]).try_to_value()
    }
}

impl ToValue<ShortString> for &str {
    fn to_value(self) -> Value<ShortString> {
        self.try_to_value().unwrap()
    }
}

impl ToValue<ShortString> for String {
    fn to_value(self) -> Value<ShortString> {
        self.try_to_value().unwrap()
    }
}

impl ToValue<ShortString> for &String {
    fn to_value(self) -> Value<ShortString> {
        self.to_str().try_to_value().unwrap()
    }
}
