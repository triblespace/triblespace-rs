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
use crate::value::RawValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;
use std::ops::{Range, RangeInclusive};

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
/// A value schema for representing a pair of `u128` values.
///
/// [`RangeU128`] encodes the pair as a half-open interval while
/// [`RangeInclusiveU128`] represents an inclusive range. Both schemas encode the
/// endpoints by packing the line into the high 64 bits and the column into the
/// low 64 bits of the `u128`.
#[derive(Debug, Clone, Copy)]
pub struct RangeU128;

#[derive(Debug, Clone, Copy)]
pub struct RangeInclusiveU128;

impl ConstMetadata for RangeU128 {
    fn id() -> Id {
        id_hex!("A4E25E3B92364FA5AB519C6A77D7CB3A")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put(
            "Half-open range encoded as two big-endian u128 values (start..end). This mirrors common slice semantics where the end is exclusive.\n\nUse for offsets, byte ranges, and spans where length matters and empty ranges are valid. Use RangeInclusiveU128 when both endpoints should be included.\n\nNo normalization is enforced; callers should ensure start <= end and interpret units consistently.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("range_u128".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatters::RANGE_U128_WASM)?,
            };
            tribles
        };
        Ok(tribles)
    }
}

impl ValueSchema for RangeU128 {
    type ValidationError = Infallible;
}

impl ConstMetadata for RangeInclusiveU128 {
    fn id() -> Id {
        id_hex!("1D0D82CA84424CD0A2F98DB37039E152")
    }

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::id();
        let description = blobs.put(
            "Inclusive range encoded as two big-endian u128 values (start..=end). This is convenient when both endpoints are meaningful.\n\nUse for closed intervals such as line/column ranges or inclusive numeric bounds. Prefer RangeU128 for half-open intervals and length-based calculations.\n\nCallers should decide how to handle empty or reversed ranges; the schema only defines the byte layout.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("range_u128_inc".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatters::RANGE_INCLUSIVE_U128_WASM)?,
            };
            tribles
        };
        Ok(tribles)
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatters {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn range_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let start = u128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let end = u128::from_be_bytes(buf);
        write!(out, "{start}..{end}").map_err(|_| 1u32)?;
        Ok(())
    }

    #[value_formatter]
    pub(crate) fn range_inclusive_u128(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&raw[..16]);
        let start = u128::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..]);
        let end = u128::from_be_bytes(buf);
        write!(out, "{start}..={end}").map_err(|_| 1u32)?;
        Ok(())
    }
}

impl ValueSchema for RangeInclusiveU128 {
    type ValidationError = Infallible;
}

fn encode_pair(range: (u128, u128)) -> RawValue {
    let mut raw = [0u8; 32];
    raw[..16].copy_from_slice(&range.0.to_be_bytes());
    raw[16..].copy_from_slice(&range.1.to_be_bytes());
    raw
}

fn decode_pair(raw: &RawValue) -> (u128, u128) {
    let mut first = [0u8; 16];
    let mut second = [0u8; 16];
    first.copy_from_slice(&raw[..16]);
    second.copy_from_slice(&raw[16..]);
    (u128::from_be_bytes(first), u128::from_be_bytes(second))
}

fn encode_range_value<S: ValueSchema>(range: (u128, u128)) -> Value<S> {
    Value::new(encode_pair(range))
}

fn decode_range_value<S: ValueSchema>(value: &Value<S>) -> (u128, u128) {
    decode_pair(&value.raw)
}

impl ToValue<RangeU128> for (u128, u128) {
    fn to_value(self) -> Value<RangeU128> {
        encode_range_value(self)
    }
}

impl FromValue<'_, RangeU128> for (u128, u128) {
    fn from_value(v: &Value<RangeU128>) -> Self {
        decode_range_value(v)
    }
}

impl ToValue<RangeInclusiveU128> for (u128, u128) {
    fn to_value(self) -> Value<RangeInclusiveU128> {
        encode_range_value(self)
    }
}

impl FromValue<'_, RangeInclusiveU128> for (u128, u128) {
    fn from_value(v: &Value<RangeInclusiveU128>) -> Self {
        decode_range_value(v)
    }
}

impl TryToValue<RangeU128> for Range<u128> {
    type Error = Infallible;

    fn try_to_value(self) -> Result<Value<RangeU128>, Self::Error> {
        Ok(encode_range_value((self.start, self.end)))
    }
}

impl TryFromValue<'_, RangeU128> for Range<u128> {
    type Error = Infallible;

    fn try_from_value(v: &Value<RangeU128>) -> Result<Self, Self::Error> {
        let (start, end) = decode_range_value(v);
        Ok(start..end)
    }
}

impl TryToValue<RangeInclusiveU128> for RangeInclusive<u128> {
    type Error = Infallible;

    fn try_to_value(self) -> Result<Value<RangeInclusiveU128>, Self::Error> {
        let (start, end) = self.into_inner();
        Ok(encode_range_value((start, end)))
    }
}

impl TryFromValue<'_, RangeInclusiveU128> for RangeInclusive<u128> {
    type Error = Infallible;

    fn try_from_value(v: &Value<RangeInclusiveU128>) -> Result<Self, Self::Error> {
        let (start, end) = decode_range_value(v);
        Ok(start..=end)
    }
}
