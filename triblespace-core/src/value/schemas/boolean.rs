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
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use crate::value::VALUE_LEN;

use std::convert::Infallible;

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
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

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put(
            "Boolean stored as all-zero bytes for false and all-0xFF bytes for true. The encoding uses the full 32-byte value, making the two states obvious and cheap to test.\n\nUse for simple flags and binary states. Represent unknown or missing data by omitting the trible rather than inventing a third sentinel value.\n\nMixed patterns are invalid and will fail validation. If you need tri-state or richer states, model it explicitly (for example with ShortString or a dedicated entity).",
        )?;
        let name = blobs.put("boolean".to_string())?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::BOOLEAN_WASM)?,
            };
            tribles
        };
        Ok(tribles.into_facts())
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn boolean(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let all_zero = raw.iter().all(|&b| b == 0);
        let all_ones = raw.iter().all(|&b| b == u8::MAX);

        let text = if all_zero {
            "false"
        } else if all_ones {
            "true"
        } else {
            return Err(2);
        };

        out.write_str(text).map_err(|_| 1u32)?;
        Ok(())
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
