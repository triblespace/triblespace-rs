//! This is a collection of Rust types that can be (de)serialized as [crate::prelude::Inline]s.

/// Boolean value schema (all-zero / all-one encoding).
pub mod boolean;
/// Ed25519 signature component and public key schemas.
pub mod ed25519;
/// 256-bit IEEE-like floating point schemas (little-endian and big-endian).
pub mod f256;
/// IEEE-754 double-precision floating point schema.
pub mod f64;
/// Opaque 128-bit identifier schema.
pub mod genid;
/// Cryptographic hash and typed blob handle schemas.
pub mod hash;
/// 256-bit signed and unsigned integer schemas (little-endian and big-endian).
pub mod iu256;
/// Line/column source location schema.
pub mod linelocation;
/// 256-bit rational number schemas (little-endian and big-endian).
pub mod r256;
/// Range schemas for pairs of `u128` values.
pub mod range;
/// Inline UTF-8 short string schema (up to 32 bytes).
pub mod shortstring;
/// TAI nanosecond interval schema.
pub mod time;

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::value::Inline;
use crate::value::InlineSchema;
use std::convert::Infallible;

/// A value schema for an unknown value.
/// This value schema is used as a fallback when the value schema is not known.
/// It is not recommended to use this value schema in practice.
/// Instead, use a specific value schema.
///
/// Any bit pattern can be a valid value of this schema.
pub struct UnknownInline {}

impl MetaDescribe for UnknownInline {
    fn describe() -> Fragment {
        let id: Id = id_hex!("4EC697E8599AC79D667C722E2C8BEBF4");
        #[allow(unused_mut)]
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @ metadata::tag: metadata::KIND_VALUE_SCHEMA
        };
        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::UNKNOWN_VALUE_WASM);
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: formatter,
            };
        }
        tribles
    }
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn unknown_value(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        out.write_str("unknown:").map_err(|_| 1u32)?;
        const TABLE: &[u8; 16] = b"0123456789ABCDEF";
        for &byte in raw {
            let hi = (byte >> 4) as usize;
            let lo = (byte & 0x0F) as usize;
            out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
            out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
        }
        Ok(())
    }
}

impl InlineSchema for UnknownInline {
    type ValidationError = Infallible;
    type FieldKind = Self;

    fn validate(value: Inline<Self>) -> Result<Inline<Self>, Self::ValidationError> {
        Ok(value)
    }
}
