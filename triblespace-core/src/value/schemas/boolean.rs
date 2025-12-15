use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use crate::value::VALUE_LEN;

use std::convert::Infallible;
/// Error raised when a value does not match the [`Boolean`] encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidBoolean;

/// Value schema that stores boolean flags as either all-zero or all-one bit patterns.
///
/// Storing `false` as `0x00` and `true` as `0xFF` in every byte makes it trivial to
/// distinguish the two cases while leaving room for future SIMD optimisations when
/// scanning large collections of flags.
pub struct Boolean;

impl Boolean {
    fn encode(flag: bool) -> Value<Self> {
        if flag {
            Value::new([u8::MAX; VALUE_LEN])
        } else {
            Value::new([0u8; VALUE_LEN])
        }
    }

    fn decode(value: &Value<Self>) -> Result<bool, InvalidBoolean> {
        if value.raw.iter().all(|&b| b == 0) {
            Ok(false)
        } else if value.raw.iter().all(|&b| b == u8::MAX) {
            Ok(true)
        } else {
            Err(InvalidBoolean)
        }
    }
}

impl ConstMetadata for Boolean {
    fn id() -> Id {
        id_hex!("73B414A3E25B0C0F9E4D6B0694DC33C5")
    }
}

impl ValueSchema for Boolean {
    type ValidationError = InvalidBoolean;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        Self::decode(&value)?;
        Ok(value)
    }
}

impl<'a> TryFromValue<'a, Boolean> for bool {
    type Error = InvalidBoolean;

    fn try_from_value(v: &'a Value<Boolean>) -> Result<Self, Self::Error> {
        Boolean::decode(v)
    }
}

impl<'a> FromValue<'a, Boolean> for bool {
    fn from_value(v: &'a Value<Boolean>) -> Self {
        v.try_from_value()
            .expect("boolean values must be well-formed")
    }
}

impl TryToValue<Boolean> for bool {
    type Error = Infallible;

    fn try_to_value(self) -> Result<Value<Boolean>, Self::Error> {
        Ok(Boolean::encode(self))
    }
}

impl TryToValue<Boolean> for &bool {
    type Error = Infallible;

    fn try_to_value(self) -> Result<Value<Boolean>, Self::Error> {
        Ok(Boolean::encode(*self))
    }
}

impl ToValue<Boolean> for bool {
    fn to_value(self) -> Value<Boolean> {
        Boolean::encode(self)
    }
}

impl ToValue<Boolean> for &bool {
    fn to_value(self) -> Value<Boolean> {
        Boolean::encode(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::Boolean;
    use super::InvalidBoolean;
    use crate::value::Value;
    use crate::value::ValueSchema;

    #[test]
    fn encodes_false_as_zero_bytes() {
        let value = Boolean::value_from(false);
        assert!(value.raw.iter().all(|&b| b == 0));
        assert_eq!(Boolean::validate(value), Ok(Boolean::value_from(false)));
    }

    #[test]
    fn encodes_true_as_all_ones() {
        let value = Boolean::value_from(true);
        assert!(value.raw.iter().all(|&b| b == u8::MAX));
        assert_eq!(Boolean::validate(value), Ok(Boolean::value_from(true)));
    }

    #[test]
    fn rejects_mixed_bit_patterns() {
        let mut mixed = [0u8; crate::value::VALUE_LEN];
        mixed[0] = 1;
        let value = Value::<Boolean>::new(mixed);
        assert_eq!(Boolean::validate(value), Err(InvalidBoolean));
    }
}
