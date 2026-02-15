use ed25519::ComponentBytes;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
pub use ed25519_dalek::VerifyingKey;

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstId, ConstMetadata};
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::FromValue;
use crate::value::ToValue;
use crate::value::TryFromValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

/// A value schema for the R component of an Ed25519 signature.
pub struct ED25519RComponent;

impl ConstId for ED25519RComponent {
    const ID: Id = id_hex!("995A86FFC83DB95ECEAA17E226208897");
}

/// A value schema for the S component of an Ed25519 signature.
pub struct ED25519SComponent;

impl ConstId for ED25519SComponent {
    const ID: Id = id_hex!("10D35B0B628E9E409C549D8EC1FB3598");
}

/// A value schema for an Ed25519 public key.
pub struct ED25519PublicKey;

impl ConstId for ED25519PublicKey {
    const ID: Id = id_hex!("69A872254E01B4C1ED36E08E40445E93");
}

impl ConstMetadata for ED25519RComponent {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "Ed25519 signature R component stored as a 32-byte field. This is one half of the standard 64-byte Ed25519 signature.\n\nUse when you store signatures as structured values or need to index the components separately. Pair with the S component to reconstruct or verify the full signature.\n\nIf you prefer storing the signature as a single binary blob, use a blob schema (for example LongString with base64 or a custom blob schema).",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:r".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::ED25519_R_WASM)?,
            };
            tribles
        };
        Ok(tribles.into_facts())
    }
}
impl ValueSchema for ED25519RComponent {
    type ValidationError = Infallible;
}
impl ConstMetadata for ED25519SComponent {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "Ed25519 signature S component stored as a 32-byte field. This is the second half of the standard Ed25519 signature.\n\nUse when storing or querying signatures in a structured form. Pair with the R component to reconstruct or verify the full signature.\n\nAs with the R component, treat this as public data; private signing keys should be stored separately and securely.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:s".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::ED25519_S_WASM)?,
            };
            tribles
        };
        Ok(tribles.into_facts())
    }
}
impl ValueSchema for ED25519SComponent {
    type ValidationError = Infallible;
}
impl ConstMetadata for ED25519PublicKey {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let description = blobs.put(
            "Ed25519 public key stored as a 32-byte field. Public keys verify signatures and identify signing identities.\n\nUse for signer registries, verification records, or key references associated with signatures. Private keys are not represented by a built-in schema and should be handled separately.\n\nEd25519 is widely supported and deterministic; if you need another scheme, define a custom schema with its own metadata.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:pubkey".to_string())?,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        let tribles = {
            let mut tribles = tribles;
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::ED25519_PUBKEY_WASM)?,
            };
            tribles
        };
        Ok(tribles.into_facts())
    }
}
impl ValueSchema for ED25519PublicKey {
    type ValidationError = Infallible;
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter(const_wasm = ED25519_R_WASM)]
    pub(crate) fn ed25519_r(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        out.write_str("ed25519:r:").map_err(|_| 1u32)?;
        const TABLE: &[u8; 16] = b"0123456789ABCDEF";
        for &byte in raw {
            let hi = (byte >> 4) as usize;
            let lo = (byte & 0x0F) as usize;
            out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
            out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
        }
        Ok(())
    }

    #[value_formatter(const_wasm = ED25519_S_WASM)]
    pub(crate) fn ed25519_s(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        out.write_str("ed25519:s:").map_err(|_| 1u32)?;
        const TABLE: &[u8; 16] = b"0123456789ABCDEF";
        for &byte in raw {
            let hi = (byte >> 4) as usize;
            let lo = (byte & 0x0F) as usize;
            out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
            out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
        }
        Ok(())
    }

    #[value_formatter(const_wasm = ED25519_PUBKEY_WASM)]
    pub(crate) fn ed25519_pubkey(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        out.write_str("ed25519:pubkey:").map_err(|_| 1u32)?;
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
