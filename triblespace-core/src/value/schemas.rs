//! This is a collection of Rust types that can be (de)serialized as [crate::prelude::Value]s.

pub mod boolean;
pub mod ed25519;
pub mod f256;
pub mod genid;
pub mod hash;
pub mod iu256;
pub mod linelocation;
pub mod r256;
pub mod range;
pub mod shortstring;
pub mod time;

use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
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
impl ConstMetadata for UnknownValue {
    fn id() -> Id {
        id_hex!("4EC697E8599AC79D667C722E2C8BEBF4")
    }
}
impl ValueSchema for UnknownValue {
    type ValidationError = Infallible;

    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        Ok(value)
    }
}
