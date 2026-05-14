use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::repo::BlobStore;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;
use std::num::TryFromIntError;

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

impl MetaDescribe for U256LE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("49E70B4DBD84DC7A3E0BDDABEC8A8C6E");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Unsigned 256-bit integer stored in little-endian byte order. The full 32 bytes are dedicated to the magnitude.\n\nUse for large counters, identifiers, or domain-specific fixed-width numbers that exceed u128. Prefer U256BE when bytewise ordering or protocol encoding matters.\n\nIf a smaller width suffices, prefer U64 or U128 in your schema to reduce storage and improve readability.",
        );
        let name = tribles.put("u256le");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::U256_LE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}
impl ValueSchema for U256LE {
    type ValidationError = Infallible;
}
impl MetaDescribe for U256BE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("DC3CFB719B05F019FB8101A6F471A982");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Unsigned 256-bit integer stored in big-endian byte order. Bytewise comparisons align with numeric order.\n\nUse when ordering or network serialization matters. Prefer U256LE for local storage or interop with little-endian APIs.\n\nIf you do not need the full 256-bit range, smaller integer schemas are easier to handle and faster to encode.",
        );
        let name = tribles.put("u256be");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::U256_BE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}
impl ValueSchema for U256BE {
    type ValidationError = Infallible;
}
impl MetaDescribe for I256LE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("DB94325A37D96037CBFC6941A4C3B66D");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Signed 256-bit integer stored in little-endian twos-complement. This enables extremely large signed ranges in a fixed width.\n\nUse for large signed quantities such as balances or offsets beyond i128. Prefer I256BE when bytewise ordering or external protocols require big-endian.\n\nIf values fit within i64 or i128, smaller schemas are more compact and easier to interoperate with.",
        );
        let name = tribles.put("i256le");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::I256_LE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}
impl ValueSchema for I256LE {
    type ValidationError = Infallible;
}
impl MetaDescribe for I256BE {
    fn describe() -> Fragment {
        let id: Id = id_hex!("CE3A7839231F1EB390E9E8E13DAED782");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Signed 256-bit integer stored in big-endian twos-complement. This variant is convenient for protocol encoding and deterministic ordering.\n\nUse for interoperability or stable bytewise comparisons across systems. Prefer I256LE for local storage or when endianness does not matter.\n\nAs with any signed integer, consider whether the sign bit has semantic meaning and avoid mixing signed and unsigned ranges.",
        );
        let name = tribles.put("i256be");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::I256_BE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}
impl ValueSchema for I256BE {
    type ValidationError = Infallible;
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter(const_wasm = U256_LE_WASM)]
    pub(crate) fn u256_le(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        fn div_mod10(limbs: &mut [u64; 4]) -> u8 {
            let mut rem: u128 = 0;
            for limb in limbs.iter_mut() {
                let n = (rem << 64) | (*limb as u128);
                *limb = (n / 10) as u64;
                rem = n % 10;
            }
            rem as u8
        }

        fn is_zero(limbs: &[u64; 4]) -> bool {
            limbs.iter().all(|&limb| limb == 0)
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&raw[0..8]);
        let w0 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[8..16]);
        let w1 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[16..24]);
        let w2 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[24..32]);
        let w3 = u64::from_le_bytes(buf);

        let mut limbs = [w3, w2, w1, w0];
        if is_zero(&limbs) {
            out.write_char('0').map_err(|_| 1u32)?;
            return Ok(());
        }

        let mut digits = [0u8; 78];
        let mut len = 0usize;
        while !is_zero(&limbs) {
            let digit = div_mod10(&mut limbs);
            digits[len] = b'0' + digit;
            len += 1;
        }

        for &digit in digits[..len].iter().rev() {
            out.write_char(digit as char).map_err(|_| 1u32)?;
        }

        Ok(())
    }

    #[value_formatter(const_wasm = U256_BE_WASM)]
    pub(crate) fn u256_be(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        fn div_mod10(limbs: &mut [u64; 4]) -> u8 {
            let mut rem: u128 = 0;
            for limb in limbs.iter_mut() {
                let n = (rem << 64) | (*limb as u128);
                *limb = (n / 10) as u64;
                rem = n % 10;
            }
            rem as u8
        }

        fn is_zero(limbs: &[u64; 4]) -> bool {
            limbs.iter().all(|&limb| limb == 0)
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&raw[0..8]);
        let w0 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[8..16]);
        let w1 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..24]);
        let w2 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[24..32]);
        let w3 = u64::from_be_bytes(buf);

        let mut limbs = [w0, w1, w2, w3];
        if is_zero(&limbs) {
            out.write_char('0').map_err(|_| 1u32)?;
            return Ok(());
        }

        let mut digits = [0u8; 78];
        let mut len = 0usize;
        while !is_zero(&limbs) {
            let digit = div_mod10(&mut limbs);
            digits[len] = b'0' + digit;
            len += 1;
        }

        for &digit in digits[..len].iter().rev() {
            out.write_char(digit as char).map_err(|_| 1u32)?;
        }

        Ok(())
    }

    #[value_formatter(const_wasm = I256_LE_WASM)]
    pub(crate) fn i256_le(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        fn div_mod10(limbs: &mut [u64; 4]) -> u8 {
            let mut rem: u128 = 0;
            for limb in limbs.iter_mut() {
                let n = (rem << 64) | (*limb as u128);
                *limb = (n / 10) as u64;
                rem = n % 10;
            }
            rem as u8
        }

        fn is_zero(limbs: &[u64; 4]) -> bool {
            limbs.iter().all(|&limb| limb == 0)
        }

        fn twos_complement(limbs: &mut [u64; 4]) {
            for limb in limbs.iter_mut() {
                *limb = !*limb;
            }

            let mut carry: u128 = 1;
            for limb in limbs.iter_mut().rev() {
                let sum = (*limb as u128) + carry;
                *limb = sum as u64;
                carry = sum >> 64;
                if carry == 0 {
                    break;
                }
            }
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&raw[0..8]);
        let w0 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[8..16]);
        let w1 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[16..24]);
        let w2 = u64::from_le_bytes(buf);
        buf.copy_from_slice(&raw[24..32]);
        let w3 = u64::from_le_bytes(buf);

        let mut limbs = [w3, w2, w1, w0];
        let negative = (limbs[0] & (1u64 << 63)) != 0;
        if negative {
            twos_complement(&mut limbs);
        }

        if is_zero(&limbs) {
            out.write_char('0').map_err(|_| 1u32)?;
            return Ok(());
        }

        let mut digits = [0u8; 78];
        let mut len = 0usize;
        while !is_zero(&limbs) {
            let digit = div_mod10(&mut limbs);
            digits[len] = b'0' + digit;
            len += 1;
        }

        if negative {
            out.write_char('-').map_err(|_| 1u32)?;
        }

        for &digit in digits[..len].iter().rev() {
            out.write_char(digit as char).map_err(|_| 1u32)?;
        }

        Ok(())
    }

    #[value_formatter(const_wasm = I256_BE_WASM)]
    pub(crate) fn i256_be(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        fn div_mod10(limbs: &mut [u64; 4]) -> u8 {
            let mut rem: u128 = 0;
            for limb in limbs.iter_mut() {
                let n = (rem << 64) | (*limb as u128);
                *limb = (n / 10) as u64;
                rem = n % 10;
            }
            rem as u8
        }

        fn is_zero(limbs: &[u64; 4]) -> bool {
            limbs.iter().all(|&limb| limb == 0)
        }

        fn twos_complement(limbs: &mut [u64; 4]) {
            for limb in limbs.iter_mut() {
                *limb = !*limb;
            }

            let mut carry: u128 = 1;
            for limb in limbs.iter_mut().rev() {
                let sum = (*limb as u128) + carry;
                *limb = sum as u64;
                carry = sum >> 64;
                if carry == 0 {
                    break;
                }
            }
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(&raw[0..8]);
        let w0 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[8..16]);
        let w1 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[16..24]);
        let w2 = u64::from_be_bytes(buf);
        buf.copy_from_slice(&raw[24..32]);
        let w3 = u64::from_be_bytes(buf);

        let mut limbs = [w0, w1, w2, w3];
        let negative = (limbs[0] & (1u64 << 63)) != 0;
        if negative {
            twos_complement(&mut limbs);
        }

        if is_zero(&limbs) {
            out.write_char('0').map_err(|_| 1u32)?;
            return Ok(());
        }

        let mut digits = [0u8; 78];
        let mut len = 0usize;
        while !is_zero(&limbs) {
            let digit = div_mod10(&mut limbs);
            digits[len] = b'0' + digit;
            len += 1;
        }

        if negative {
            out.write_char('-').map_err(|_| 1u32)?;
        }

        for &digit in digits[..len].iter().rev() {
            out.write_char(digit as char).map_err(|_| 1u32)?;
        }

        Ok(())
    }
}

impl ToValue<U256BE> for ethnum::U256 {
    fn to_value(self) -> Value<U256BE> {
        Value::new(self.to_be_bytes())
    }
}

impl TryFromValue<'_, U256BE> for ethnum::U256 {
    type Error = Infallible;
    fn try_from_value(v: &Value<U256BE>) -> Result<Self, Infallible> {
        Ok(ethnum::U256::from_be_bytes(v.raw))
    }
}

impl ToValue<U256LE> for ethnum::U256 {
    fn to_value(self) -> Value<U256LE> {
        Value::new(self.to_le_bytes())
    }
}

impl TryFromValue<'_, U256LE> for ethnum::U256 {
    type Error = Infallible;
    fn try_from_value(v: &Value<U256LE>) -> Result<Self, Infallible> {
        Ok(ethnum::U256::from_le_bytes(v.raw))
    }
}

impl ToValue<I256BE> for ethnum::I256 {
    fn to_value(self) -> Value<I256BE> {
        Value::new(self.to_be_bytes())
    }
}

impl TryFromValue<'_, I256BE> for ethnum::I256 {
    type Error = Infallible;
    fn try_from_value(v: &Value<I256BE>) -> Result<Self, Infallible> {
        Ok(ethnum::I256::from_be_bytes(v.raw))
    }
}

impl ToValue<I256LE> for ethnum::I256 {
    fn to_value(self) -> Value<I256LE> {
        Value::new(self.to_le_bytes())
    }
}

impl TryFromValue<'_, I256LE> for ethnum::I256 {
    type Error = Infallible;
    fn try_from_value(v: &Value<I256LE>) -> Result<Self, Infallible> {
        Ok(ethnum::I256::from_le_bytes(v.raw))
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

// --- Narrowing TryFromValue impls (U256 → native integers) ---

macro_rules! impl_try_from_u256 {
    ($schema:ty, $wide:ty, $($narrow:ty),+) => {
        $(
            impl TryFromValue<'_, $schema> for $narrow {
                type Error = TryFromIntError;
                fn try_from_value(v: &Value<$schema>) -> Result<Self, Self::Error> {
                    let wide: $wide = v.from_value();
                    <$narrow>::try_from(wide)
                }
            }
        )+
    };
}

impl_try_from_u256!(U256BE, ethnum::U256, u8, u16, u32, u64, u128);
impl_try_from_u256!(U256LE, ethnum::U256, u8, u16, u32, u64, u128);
impl_try_from_u256!(I256BE, ethnum::I256, i8, i16, i32, i64, i128);
impl_try_from_u256!(I256LE, ethnum::I256, i8, i16, i32, i64, i128);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{ToValue, TryFromValue};
    use proptest::prelude::*;

    fn arb_u256() -> impl Strategy<Value = ethnum::U256> {
        prop::array::uniform32(any::<u8>()).prop_map(ethnum::U256::from_be_bytes)
    }

    fn arb_i256() -> impl Strategy<Value = ethnum::I256> {
        prop::array::uniform32(any::<u8>()).prop_map(ethnum::I256::from_be_bytes)
    }

    // --- U256BE property tests ---

    proptest! {
        #[test]
        fn u256be_ethnum_roundtrip(input in arb_u256()) {
            let value: Value<U256BE> = input.to_value();
            let output: ethnum::U256 = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_u128_roundtrip(input: u128) {
            let value: Value<U256BE> = input.to_value();
            let output = u128::try_from_value(&value).expect("fits in u128");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_u64_roundtrip(input: u64) {
            let value: Value<U256BE> = input.to_value();
            let output = u64::try_from_value(&value).expect("fits in u64");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_u32_roundtrip(input: u32) {
            let value: Value<U256BE> = input.to_value();
            let output = u32::try_from_value(&value).expect("fits in u32");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_u16_roundtrip(input: u16) {
            let value: Value<U256BE> = input.to_value();
            let output = u16::try_from_value(&value).expect("fits in u16");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_u8_roundtrip(input: u8) {
            let value: Value<U256BE> = input.to_value();
            let output = u8::try_from_value(&value).expect("fits in u8");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256be_validates(input in arb_u256()) {
            let value: Value<U256BE> = input.to_value();
            prop_assert!(U256BE::validate(value).is_ok());
        }

        #[test]
        fn u256be_order_preservation(a in arb_u256(), b in arb_u256()) {
            let va: Value<U256BE> = a.to_value();
            let vb: Value<U256BE> = b.to_value();
            prop_assert_eq!(a.cmp(&b), va.raw.cmp(&vb.raw));
        }

        #[test]
        fn u256be_widening_u64_u128(input: u64) {
            let v64: Value<U256BE> = input.to_value();
            let v128: Value<U256BE> = (input as u128).to_value();
            prop_assert_eq!(v64.raw, v128.raw);
        }

        #[test]
        fn u256be_widening_u32_u128(input: u32) {
            let v32: Value<U256BE> = input.to_value();
            let v128: Value<U256BE> = (input as u128).to_value();
            prop_assert_eq!(v32.raw, v128.raw);
        }

        // --- U256LE property tests ---

        #[test]
        fn u256le_ethnum_roundtrip(input in arb_u256()) {
            let value: Value<U256LE> = input.to_value();
            let output: ethnum::U256 = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256le_u128_roundtrip(input: u128) {
            let value: Value<U256LE> = input.to_value();
            let output = u128::try_from_value(&value).expect("fits in u128");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256le_u64_roundtrip(input: u64) {
            let value: Value<U256LE> = input.to_value();
            let output = u64::try_from_value(&value).expect("fits in u64");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn u256le_validates(input in arb_u256()) {
            let value: Value<U256LE> = input.to_value();
            prop_assert!(U256LE::validate(value).is_ok());
        }

        #[test]
        fn u256_le_and_be_differ(input in arb_u256().prop_filter("non-zero", |v| *v != ethnum::U256::ZERO)) {
            let le_val: Value<U256LE> = input.to_value();
            let be_val: Value<U256BE> = input.to_value();
            prop_assert_ne!(le_val.raw, be_val.raw);
        }

        // --- I256BE property tests ---

        #[test]
        fn i256be_ethnum_roundtrip(input in arb_i256()) {
            let value: Value<I256BE> = input.to_value();
            let output: ethnum::I256 = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256be_i128_roundtrip(input: i128) {
            let value: Value<I256BE> = input.to_value();
            let output = i128::try_from_value(&value).expect("fits in i128");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256be_i64_roundtrip(input: i64) {
            let value: Value<I256BE> = input.to_value();
            let output = i64::try_from_value(&value).expect("fits in i64");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256be_i32_roundtrip(input: i32) {
            let value: Value<I256BE> = input.to_value();
            let output = i32::try_from_value(&value).expect("fits in i32");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256be_i8_roundtrip(input: i8) {
            let value: Value<I256BE> = input.to_value();
            let output = i8::try_from_value(&value).expect("fits in i8");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256be_validates(input in arb_i256()) {
            let value: Value<I256BE> = input.to_value();
            prop_assert!(I256BE::validate(value).is_ok());
        }

        // Note: I256BE uses raw two's complement, so bytewise order does NOT
        // match signed numeric order (negative values sort after positive).

        // --- I256LE property tests ---

        #[test]
        fn i256le_ethnum_roundtrip(input in arb_i256()) {
            let value: Value<I256LE> = input.to_value();
            let output: ethnum::I256 = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256le_i128_roundtrip(input: i128) {
            let value: Value<I256LE> = input.to_value();
            let output = i128::try_from_value(&value).expect("fits in i128");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256le_i64_roundtrip(input: i64) {
            let value: Value<I256LE> = input.to_value();
            let output = i64::try_from_value(&value).expect("fits in i64");
            prop_assert_eq!(input, output);
        }

        #[test]
        fn i256le_validates(input in arb_i256()) {
            let value: Value<I256LE> = input.to_value();
            prop_assert!(I256LE::validate(value).is_ok());
        }

        #[test]
        fn i256_le_and_be_differ(input in arb_i256().prop_filter("non-zero", |v| *v != ethnum::I256::ZERO)) {
            let le_val: Value<I256LE> = input.to_value();
            let be_val: Value<I256BE> = input.to_value();
            prop_assert_ne!(le_val.raw, be_val.raw);
        }
    }

    // --- Narrowing overflow tests (specific invalid inputs) ---

    #[test]
    fn u256be_narrowing_overflow() {
        let input = ethnum::U256::from(u128::MAX) + ethnum::U256::ONE;
        let value: Value<U256BE> = input.to_value();
        assert!(u128::try_from_value(&value).is_err());
    }

    #[test]
    fn i256be_narrowing_overflow_positive() {
        let input = ethnum::I256::from(i128::MAX) + ethnum::I256::ONE;
        let value: Value<I256BE> = input.to_value();
        assert!(i128::try_from_value(&value).is_err());
    }

    #[test]
    fn i256be_narrowing_overflow_negative() {
        let input = ethnum::I256::from(i128::MIN) - ethnum::I256::ONE;
        let value: Value<I256BE> = input.to_value();
        assert!(i128::try_from_value(&value).is_err());
    }
}
