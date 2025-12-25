use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use ethnum;

#[cfg(feature = "wasm")]
use crate::blob::schemas::wasmcode::WasmCode;
#[cfg(feature = "wasm")]
use crate::id::ExclusiveId;
#[cfg(feature = "wasm")]
use crate::macros::entity;
#[cfg(feature = "wasm")]
use crate::metadata;
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

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = match blobs.put::<WasmCode, _>(wasm_formatter::U256_LE_WASM) {
            Ok(handle) => {
                let entity = ExclusiveId::force(Self::id());
                entity! { &entity @ metadata::value_formatter: handle }
            }
            Err(_) => TribleSet::new(),
        };
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for U256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for U256BE {
    fn id() -> Id {
        id_hex!("DC3CFB719B05F019FB8101A6F471A982")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = match blobs.put::<WasmCode, _>(wasm_formatter::U256_BE_WASM) {
            Ok(handle) => {
                let entity = ExclusiveId::force(Self::id());
                entity! { &entity @ metadata::value_formatter: handle }
            }
            Err(_) => TribleSet::new(),
        };
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for U256BE {
    type ValidationError = Infallible;
}
impl ConstMetadata for I256LE {
    fn id() -> Id {
        id_hex!("DB94325A37D96037CBFC6941A4C3B66D")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = match blobs.put::<WasmCode, _>(wasm_formatter::I256_LE_WASM) {
            Ok(handle) => {
                let entity = ExclusiveId::force(Self::id());
                entity! { &entity @ metadata::value_formatter: handle }
            }
            Err(_) => TribleSet::new(),
        };
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for I256LE {
    type ValidationError = Infallible;
}
impl ConstMetadata for I256BE {
    fn id() -> Id {
        id_hex!("CE3A7839231F1EB390E9E8E13DAED782")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = match blobs.put::<WasmCode, _>(wasm_formatter::I256_BE_WASM) {
            Ok(handle) => {
                let entity = ExclusiveId::force(Self::id());
                entity! { &entity @ metadata::value_formatter: handle }
            }
            Err(_) => TribleSet::new(),
        };
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
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
