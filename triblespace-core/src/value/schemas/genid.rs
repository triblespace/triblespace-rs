use crate::value::IntoSchema;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id::NilUuidError;
use crate::id::OwnedId;
use crate::id::RawId;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::IntoValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use crate::value::VALUE_LEN;

use std::convert::TryInto;

use hex::FromHex;
use hex::FromHexError;

#[cfg(feature = "proptest")]
use proptest::prelude::RngCore;

/// A value schema for an abstract 128-bit identifier.
/// This identifier is generated with high entropy and is suitable for use as a unique identifier.
///
/// See the [crate::id] module documentation for a discussion on the role of this identifier.
pub struct GenId;

impl MetaDescribe for GenId {
    fn describe() -> Fragment {
        let id: Id = id_hex!("B08EE1D45EB081E8C47618178AFE0D81");
        let mut tribles = Fragment::rooted(id, TribleSet::new());
        let description = tribles.put(
            "Opaque 128-bit identifier stored in the lower 16 bytes; the upper 16 bytes are zero. The value is intended to be high-entropy and stable over time.\n\nUse for entity ids, references, or user-assigned identifiers when the bytes do not carry meaning. If you want content-derived identifiers or deduplication, use a Hash schema instead.\n\nGenId does not imply ordering or integrity. If you need deterministic ids across systems, derive them from agreed inputs (for example by wrapping the inputs in `entity!{}` and taking its `root()`, or by hashing them directly).",
        );
        let name = tribles.put("genid");
        tribles += entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: name,
                metadata::description: description,
                metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            let formatter = tribles.put(wasm_formatter::GENID_WASM);
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
    pub(crate) fn genid(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        const TABLE: &[u8; 16] = b"0123456789ABCDEF";

        let prefix_ok = raw[..16].iter().all(|&b| b == 0);
        let bytes = if prefix_ok { &raw[16..] } else { &raw[..] };
        for &byte in bytes {
            let hi = (byte >> 4) as usize;
            let lo = (byte & 0x0F) as usize;
            out.write_char(TABLE[hi] as char).map_err(|_| 1u32)?;
            out.write_char(TABLE[lo] as char).map_err(|_| 1u32)?;
        }
        Ok(())
    }
}
impl ValueSchema for GenId {
    type ValidationError = ();
    type FieldKind = Self;
    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        if value.raw[0..16] == [0; 16] {
            Ok(value)
        } else {
            Err(())
        }
    }
}

/// Error returned when extracting an identifier from a [`Value<GenId>`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IdParseError {
    /// The identifier is nil (all zeros), which is reserved.
    IsNil,
    /// The upper 16 bytes are not zero, violating the GenId layout.
    BadFormat,
}

//RawId
impl<'a> TryFromValue<'a, GenId> for &'a RawId {
    type Error = IdParseError;

    fn try_from_value(value: &'a Value<GenId>) -> Result<Self, Self::Error> {
        if value.raw[0..16] != [0; 16] {
            return Err(IdParseError::BadFormat);
        }
        Ok(value.raw[16..32].try_into().unwrap())
    }
}

impl TryFromValue<'_, GenId> for RawId {
    type Error = IdParseError;

    fn try_from_value(value: &Value<GenId>) -> Result<Self, Self::Error> {
        let r: Result<&RawId, IdParseError> = value.try_from_value();
        r.copied()
    }
}

impl IntoSchema<GenId> for RawId {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        let mut data = [0; VALUE_LEN];
        data[16..32].copy_from_slice(&self[..]);
        Value::new(data)
    }
}

//Id
impl<'a> TryFromValue<'a, GenId> for &'a Id {
    type Error = IdParseError;

    fn try_from_value(value: &'a Value<GenId>) -> Result<Self, Self::Error> {
        if value.raw[0..16] != [0; 16] {
            return Err(IdParseError::BadFormat);
        }
        if let Some(id) = Id::as_transmute_raw(value.raw[16..32].try_into().unwrap()) {
            Ok(id)
        } else {
            Err(IdParseError::IsNil)
        }
    }
}

impl TryFromValue<'_, GenId> for Id {
    type Error = IdParseError;

    fn try_from_value(value: &Value<GenId>) -> Result<Self, Self::Error> {
        let r: Result<&Id, IdParseError> = value.try_from_value();
        r.copied()
    }
}

impl IntoSchema<GenId> for &Id {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        let mut data = [0; VALUE_LEN];
        data[16..32].copy_from_slice(&self[..]);
        Value::new(data)
    }
}

impl IntoSchema<GenId> for Id {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        (&self).to_value()
    }
}

impl TryFromValue<'_, GenId> for uuid::Uuid {
    type Error = IdParseError;

    fn try_from_value(value: &Value<GenId>) -> Result<Self, Self::Error> {
        if value.raw[0..16] != [0; 16] {
            return Err(IdParseError::BadFormat);
        }
        let bytes: [u8; 16] = value.raw[16..32].try_into().unwrap();
        Ok(uuid::Uuid::from_bytes(bytes))
    }
}

/// Error returned when extracting an [`ExclusiveId`] from a [`Value<GenId>`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExclusiveIdError {
    /// The raw bytes could not be interpreted as an identifier.
    FailedParse(IdParseError),
    /// The identifier is valid but could not be exclusively acquired
    /// (another holder already owns it).
    FailedAcquire(),
}

impl From<IdParseError> for ExclusiveIdError {
    fn from(e: IdParseError) -> Self {
        ExclusiveIdError::FailedParse(e)
    }
}

impl<'a> TryFromValue<'a, GenId> for ExclusiveId {
    type Error = ExclusiveIdError;

    fn try_from_value(value: &'a Value<GenId>) -> Result<Self, Self::Error> {
        let id: Id = value.try_from_value()?;
        id.acquire().ok_or(ExclusiveIdError::FailedAcquire())
    }
}

impl IntoSchema<GenId> for ExclusiveId {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        self.id.to_value()
    }
}

impl IntoSchema<GenId> for &ExclusiveId {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        self.id.to_value()
    }
}

impl TryFromValue<'_, GenId> for String {
    type Error = IdParseError;

    fn try_from_value(v: &'_ Value<GenId>) -> Result<Self, Self::Error> {
        let id: Id = v.try_from_value()?;
        let mut s = String::new();
        s.push_str("genid:");
        s.push_str(&hex::encode(id));
        Ok(s)
    }
}

impl IntoSchema<GenId> for OwnedId<'_> {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        self.id.to_value()
    }
}

impl IntoSchema<GenId> for &OwnedId<'_> {
    type Form = Value<GenId>;
    fn into_schema(self) -> Value<GenId> {
        self.id.to_value()
    }
}

/// Error returned when packing a string into a [`Value<GenId>`].
///
/// The expected format is `"genid:<32 hex chars>"`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PackIdError {
    /// The string does not start with `"genid:"`.
    BadProtocol,
    /// The hex portion could not be decoded.
    BadHex(FromHexError),
}

impl From<FromHexError> for PackIdError {
    fn from(value: FromHexError) -> Self {
        PackIdError::BadHex(value)
    }
}

impl TryToValue<GenId> for &str {
    type Error = PackIdError;

    fn try_to_value(self) -> Result<Value<GenId>, Self::Error> {
        let protocol = "genid:";
        if !self.starts_with(protocol) {
            return Err(PackIdError::BadProtocol);
        }
        let id = RawId::from_hex(&self[protocol.len()..])?;
        Ok(id.to_value())
    }
}

impl TryToValue<GenId> for uuid::Uuid {
    type Error = NilUuidError;

    fn try_to_value(self) -> Result<Value<GenId>, Self::Error> {
        let mut data = [0; VALUE_LEN];
        data[16..32].copy_from_slice(self.as_bytes());
        Ok(Value::new(data))
    }
}

impl TryToValue<GenId> for &uuid::Uuid {
    type Error = NilUuidError;

    fn try_to_value(self) -> Result<Value<GenId>, Self::Error> {
        (*self).try_to_value()
    }
}

#[cfg(feature = "proptest")]
/// Proptest value tree for a random [`GenId`]. Does not shrink.
pub struct IdValueTree(RawId);

#[cfg(feature = "proptest")]
/// Proptest strategy that generates random 128-bit identifiers.
#[derive(Debug)]
pub struct RandomGenId();
#[cfg(feature = "proptest")]
impl proptest::strategy::Strategy for RandomGenId {
    type Tree = IdValueTree;
    type Value = RawId;

    fn new_tree(
        &self,
        runner: &mut proptest::prelude::prop::test_runner::TestRunner,
    ) -> proptest::prelude::prop::strategy::NewTree<Self> {
        let rng = runner.rng();
        let mut id = [0; 16];
        rng.fill_bytes(&mut id[..]);

        Ok(IdValueTree(id))
    }
}

#[cfg(feature = "proptest")]
impl proptest::strategy::ValueTree for IdValueTree {
    type Value = RawId;

    fn simplify(&mut self) -> bool {
        false
    }
    fn complicate(&mut self) -> bool {
        false
    }
    fn current(&self) -> RawId {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::GenId;
    use crate::id::rngid;
    use crate::value::TryFromValue;
    use crate::value::TryToValue;
    use crate::value::ValueSchema;

    #[test]
    fn unique() {
        assert!(rngid() != rngid());
    }

    #[test]
    fn uuid_nil_round_trip() {
        let uuid = uuid::Uuid::nil();
        let value = uuid.try_to_value().expect("uuid packing should succeed");
        GenId::validate(value).expect("schema validation");
        let round_trip = uuid::Uuid::try_from_value(&value).expect("uuid unpacking should succeed");
        assert_eq!(uuid, round_trip);
    }
}
