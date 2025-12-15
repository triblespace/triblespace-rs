use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use ethnum;

/// A value schema for a 256-bit unsigned integer in little-endian byte order.
pub struct U256LE;

/// A value schema for a 256-bit unsigned integer in big-endian byte order.
pub struct U256BE;

/// A value schema for a 256-bit signed integer in little-endian byte order.
pub struct I256LE;

/// A value schema for a 256-bit signed integer in big-endian byte order.
pub struct I256BE;

/// A type alias for a 256-bit signed integer.
/// This type is an alias for [I256BE].
pub type I256 = I256BE;

/// A type alias for a 256-bit unsigned integer.
/// This type is an alias for [U256BE].
pub type U256 = U256BE;

impl ConstMetadata for U256LE {
    fn id() -> Id {
        id_hex!("49E70B4DBD84DC7A3E0BDDABEC8A8C6E")
    }
}
impl ValueSchema for U256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for U256BE {
    fn id() -> Id {
        id_hex!("DC3CFB719B05F019FB8101A6F471A982")
    }
}
impl ValueSchema for U256BE {
    type ValidationError = Infallible;
}
impl ConstMetadata for I256LE {
    fn id() -> Id {
        id_hex!("DB94325A37D96037CBFC6941A4C3B66D")
    }
}
impl ValueSchema for I256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for I256BE {
    fn id() -> Id {
        id_hex!("CE3A7839231F1EB390E9E8E13DAED782")
    }
}
impl ValueSchema for I256BE {
    type ValidationError = Infallible;
}

impl ToValue<U256BE> for ethnum::U256 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(self.to_be_bytes())
    }
}

impl FromValue<'_, U256BE> for ethnum::U256 {
    fn from_value(v: &Value<U256BE>) -> Self {
        ethnum::U256::from_be_bytes(v.raw)
    }
}

impl ToValue<U256LE> for ethnum::U256 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(self.to_le_bytes())
    }
}

impl FromValue<'_, U256LE> for ethnum::U256 {
    fn from_value(v: &Value<U256LE>) -> Self {
        ethnum::U256::from_le_bytes(v.raw)
    }
}

impl ToValue<I256BE> for ethnum::I256 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(self.to_be_bytes())
    }
}

impl FromValue<'_, I256BE> for ethnum::I256 {
    fn from_value(v: &Value<I256BE>) -> Self {
        ethnum::I256::from_be_bytes(v.raw)
    }
}

impl ToValue<I256LE> for ethnum::I256 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(self.to_le_bytes())
    }
}

impl FromValue<'_, I256LE> for ethnum::I256 {
    fn from_value(v: &Value<I256LE>) -> Self {
        ethnum::I256::from_le_bytes(v.raw)
    }
}

impl ToValue<U256LE> for u8 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(ethnum::U256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<U256LE> for u16 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(ethnum::U256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<U256LE> for u32 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(ethnum::U256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<U256LE> for u64 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(ethnum::U256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<U256LE> for u128 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(ethnum::U256::new(self).to_le_bytes())
    }
}

impl ToValue<U256BE> for u8 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(ethnum::U256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<U256BE> for u16 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(ethnum::U256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<U256BE> for u32 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(ethnum::U256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<U256BE> for u64 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(ethnum::U256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<U256BE> for u128 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(ethnum::U256::new(self).to_be_bytes())
    }
}

impl ToValue<I256LE> for i8 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(ethnum::I256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<I256LE> for i16 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(ethnum::I256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<I256LE> for i32 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(ethnum::I256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<I256LE> for i64 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(ethnum::I256::new(self.into()).to_le_bytes())
    }
}

impl ToValue<I256LE> for i128 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(ethnum::I256::new(self).to_le_bytes())
    }
}

impl ToValue<I256BE> for i8 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(ethnum::I256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<I256BE> for i32 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(ethnum::I256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<I256BE> for i64 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(ethnum::I256::new(self.into()).to_be_bytes())
    }
}

impl ToValue<I256BE> for i128 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(ethnum::I256::new(self).to_be_bytes())
    }
}
