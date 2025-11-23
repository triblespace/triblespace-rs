use ed25519::ComponentBytes;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
pub use ed25519_dalek::VerifyingKey;

use crate::id::Id;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

/// A value schema for the R component of an Ed25519 signature.
pub struct ED25519RComponent;

/// A value schema for the S component of an Ed25519 signature.
pub struct ED25519SComponent;

/// A value schema for an Ed25519 public key.
pub struct ED25519PublicKey;

impl ConstMetadata for ED25519RComponent {
    fn id() -> Id {
        id_hex!("995A86FFC83DB95ECEAA17E226208897")
    }
}
impl ValueSchema for ED25519RComponent {
    type ValidationError = Infallible;
}
impl ConstMetadata for ED25519SComponent {
    fn id() -> Id {
        id_hex!("10D35B0B628E9E409C549D8EC1FB3598")
    }
}
impl ValueSchema for ED25519SComponent {
    type ValidationError = Infallible;
}
impl ConstMetadata for ED25519PublicKey {
    fn id() -> Id {
        id_hex!("69A872254E01B4C1ED36E08E40445E93")
    }
}
impl ValueSchema for ED25519PublicKey {
    type ValidationError = Infallible;
}

impl ED25519RComponent {
    pub fn from_signature(s: Signature) -> Value<ED25519RComponent> {
        Value::new(*s.r_bytes())
    }
}

impl ED25519SComponent {
    pub fn from_signature(s: Signature) -> Value<ED25519SComponent> {
        Value::new(*s.s_bytes())
    }
}

impl ToValue<ED25519RComponent> for Signature {
    fn to_value(self) -> Value<ED25519RComponent> {
        ED25519RComponent::from_signature(self)
    }
}

impl ToValue<ED25519SComponent> for Signature {
    fn to_value(self) -> Value<ED25519SComponent> {
        ED25519SComponent::from_signature(self)
    }
}

impl ToValue<ED25519RComponent> for ComponentBytes {
    fn to_value(self) -> Value<ED25519RComponent> {
        Value::new(self)
    }
}

impl FromValue<'_, ED25519RComponent> for ComponentBytes {
    fn from_value(v: &Value<ED25519RComponent>) -> Self {
        v.raw
    }
}

impl ToValue<ED25519SComponent> for ComponentBytes {
    fn to_value(self) -> Value<ED25519SComponent> {
        Value::new(self)
    }
}

impl FromValue<'_, ED25519SComponent> for ComponentBytes {
    fn from_value(v: &Value<ED25519SComponent>) -> Self {
        v.raw
    }
}

impl ToValue<ED25519PublicKey> for VerifyingKey {
    fn to_value(self) -> Value<ED25519PublicKey> {
        Value::new(self.to_bytes())
    }
}

impl TryFromValue<'_, ED25519PublicKey> for VerifyingKey {
    type Error = SignatureError;

    fn try_from_value(v: &Value<ED25519PublicKey>) -> Result<Self, Self::Error> {
        VerifyingKey::from_bytes(&v.raw)
    }
}
