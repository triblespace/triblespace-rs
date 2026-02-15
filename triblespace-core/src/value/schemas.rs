//! This is a collection of Rust types that can be (de)serialized as [crate::prelude::Value]s.

pub mod boolean;
pub mod ed25519;
pub mod f256;
pub mod f64;
pub mod genid;
pub mod hash;
pub mod iu256;
pub mod linelocation;
pub mod r256;
pub mod range;
pub mod shortstring;
pub mod time;

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstDescribe, ConstId};
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

/// A value schema for an unknown value.
/// This value schema is used as a fallback when the value schema is not known.
/// It is not recommended to use this value schema in practice.
/// Instead, use a specific value schema.
///
/// Any bit pattern can be a valid value of this schema.
pub struct UnknownValue {}

impl ConstId for UnknownValue {
    const ID: Id = id_hex!("4EC697E8599AC79D667C722E2C8BEBF4");
}

impl ConstDescribe for UnknownValue {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let mut tribles = entity! {
            ExclusiveId::force_ref(&id) @ metadata::tag: metadata::KIND_VALUE_SCHEMA
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::UNKNOWN_VALUE_WASM)?,
            };
        }
        #[cfg(not(feature = "wasm"))]
        let _ = (blobs, &mut tribles);
        Ok(tribles.into_facts())
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

impl ValueSchema for UnknownValue {
    type ValidationError = Infallible;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        Ok(value)
    }
}
