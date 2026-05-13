use ed25519::ComponentBytes;
use ed25519::Signature;
use ed25519_dalek::SignatureError;
/// Re-export of the Ed25519 verifying (public) key type from [`ed25519_dalek`].
pub use ed25519_dalek::VerifyingKey;

use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::repo::BlobStore;
use crate::trible::Fragment;
use crate::value::schemas::hash::Blake3;
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

impl MetaDescribe for ED25519RComponent {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id: Id = id_hex!("995A86FFC83DB95ECEAA17E226208897");
        let description = blobs.put(
            "Ed25519 signature R component stored as a 32-byte field. This is one half of the standard 64-byte Ed25519 signature.\n\nUse when you store signatures as structured values or need to index the components separately. Pair with the S component to reconstruct or verify the full signature.\n\nIf you prefer storing the signature as a single binary blob, use a blob schema (for example LongString with base64 or a custom blob schema).",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:r")?,
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
        Ok(tribles)
    }
}
impl ValueSchema for ED25519RComponent {
    type ValidationError = Infallible;
}
impl MetaDescribe for ED25519SComponent {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id: Id = id_hex!("10D35B0B628E9E409C549D8EC1FB3598");
        let description = blobs.put(
            "Ed25519 signature S component stored as a 32-byte field. This is the second half of the standard Ed25519 signature.\n\nUse when storing or querying signatures in a structured form. Pair with the R component to reconstruct or verify the full signature.\n\nAs with the R component, treat this as public data; private signing keys should be stored separately and securely.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:s")?,
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
        Ok(tribles)
    }
}
impl ValueSchema for ED25519SComponent {
    type ValidationError = Infallible;
}
impl MetaDescribe for ED25519PublicKey {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id: Id = id_hex!("69A872254E01B4C1ED36E08E40445E93");
        let description = blobs.put(
            "Ed25519 public key stored as a 32-byte field. Public keys verify signatures and identify signing identities.\n\nUse for signer registries, verification records, or key references associated with signatures. Private keys are not represented by a built-in schema and should be handled separately.\n\nEd25519 is widely supported and deterministic; if you need another scheme, define a custom schema with its own metadata.",
        )?;
        let tribles = entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: blobs.put("ed25519:pubkey")?,
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
        Ok(tribles)
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
    /// Extracts the R component from a full Ed25519 signature.
    pub fn from_signature(s: Signature) -> Value<ED25519RComponent> {
        Value::new(*s.r_bytes())
    }
}

impl ED25519SComponent {
    /// Extracts the S component from a full Ed25519 signature.
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

impl TryFromValue<'_, ED25519RComponent> for ComponentBytes {
    type Error = Infallible;
    fn try_from_value(v: &Value<ED25519RComponent>) -> Result<Self, Infallible> {
        Ok(v.raw)
    }
}

impl ToValue<ED25519SComponent> for ComponentBytes {
    fn to_value(self) -> Value<ED25519SComponent> {
        Value::new(self)
    }
}

impl TryFromValue<'_, ED25519SComponent> for ComponentBytes {
    type Error = Infallible;
    fn try_from_value(v: &Value<ED25519SComponent>) -> Result<Self, Infallible> {
        Ok(v.raw)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{ToValue, TryFromValue};
    use ed25519::Signature;
    use ed25519_dalek::SigningKey;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn r_component_bytes_roundtrip(input in prop::array::uniform32(any::<u8>())) {
            let value: Value<ED25519RComponent> = input.to_value();
            let output: ComponentBytes = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn s_component_bytes_roundtrip(input in prop::array::uniform32(any::<u8>())) {
            let value: Value<ED25519SComponent> = input.to_value();
            let output: ComponentBytes = value.from_value();
            prop_assert_eq!(input, output);
        }

        #[test]
        fn r_component_validates(input in prop::array::uniform32(any::<u8>())) {
            let value: Value<ED25519RComponent> = input.to_value();
            prop_assert!(ED25519RComponent::validate(value).is_ok());
        }

        #[test]
        fn s_component_validates(input in prop::array::uniform32(any::<u8>())) {
            let value: Value<ED25519SComponent> = input.to_value();
            prop_assert!(ED25519SComponent::validate(value).is_ok());
        }

        #[test]
        fn pubkey_validates(seed in prop::array::uniform32(any::<u8>())) {
            let key = SigningKey::from_bytes(&seed).verifying_key();
            let value: Value<ED25519PublicKey> = key.to_value();
            prop_assert!(ED25519PublicKey::validate(value).is_ok());
        }

        #[test]
        fn verifying_key_roundtrip(seed in prop::array::uniform32(any::<u8>())) {
            let key = SigningKey::from_bytes(&seed).verifying_key();
            let value: Value<ED25519PublicKey> = key.to_value();
            let recovered = VerifyingKey::try_from_value(&value).expect("valid key");
            prop_assert_eq!(key, recovered);
        }

        #[test]
        fn signature_r_component_roundtrip(seed in prop::array::uniform32(any::<u8>()), msg in prop::collection::vec(any::<u8>(), 0..256)) {
            use ed25519_dalek::Signer;
            let signing_key = SigningKey::from_bytes(&seed);
            let sig = Signature::from_bytes(&signing_key.sign(&msg).to_bytes());
            let value: Value<ED25519RComponent> = sig.to_value();
            let bytes: ComponentBytes = value.from_value();
            prop_assert_eq!(&bytes, sig.r_bytes());
        }

        #[test]
        fn signature_s_component_roundtrip(seed in prop::array::uniform32(any::<u8>()), msg in prop::collection::vec(any::<u8>(), 0..256)) {
            use ed25519_dalek::Signer;
            let signing_key = SigningKey::from_bytes(&seed);
            let sig = Signature::from_bytes(&signing_key.sign(&msg).to_bytes());
            let value: Value<ED25519SComponent> = sig.to_value();
            let bytes: ComponentBytes = value.from_value();
            prop_assert_eq!(&bytes, sig.s_bytes());
        }

        #[test]
        fn signature_r_s_reconstruct(seed in prop::array::uniform32(any::<u8>()), msg in prop::collection::vec(any::<u8>(), 0..256)) {
            use ed25519_dalek::Signer;
            let signing_key = SigningKey::from_bytes(&seed);
            let sig = Signature::from_bytes(&signing_key.sign(&msg).to_bytes());
            let r_val: Value<ED25519RComponent> = sig.to_value();
            let s_val: Value<ED25519SComponent> = sig.to_value();
            let r_bytes: ComponentBytes = r_val.from_value();
            let s_bytes: ComponentBytes = s_val.from_value();
            let mut combined = [0u8; 64];
            combined[..32].copy_from_slice(&r_bytes);
            combined[32..].copy_from_slice(&s_bytes);
            let reconstructed = Signature::from_bytes(&combined);
            prop_assert_eq!(sig, reconstructed);
        }
    }

    // Invalid key bytes: specific error-case test.
    #[test]
    fn verifying_key_invalid_bytes() {
        let mut raw = [0u8; 32];
        raw[0] = 2;
        let value: Value<ED25519PublicKey> = Value::new(raw);
        assert!(VerifyingKey::try_from_value(&value).is_err());
    }
}
