use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id::NilUuidError;
use crate::id::OwnedId;
use crate::id::RawId;
use crate::id_hex;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::schemas::hash::Blake3;
use crate::value::FromValue;
use crate::value::ToValue;
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

impl ConstMetadata for GenId {
    fn id() -> Id {
        id_hex!("B08EE1D45EB081E8C47618178AFE0D81")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let _ = blobs;

        #[cfg(feature = "wasm")]
        let tribles = super::wasm_formatters::describe_value_formatter(
            blobs,
            Self::id(),
            super::wasm_formatters::GENID_WASM,
        );
        #[cfg(not(feature = "wasm"))]
        let tribles = TribleSet::new();
        tribles
    }
}
impl ValueSchema for GenId {
    type ValidationError = ();
    fn validate(value: Value<Self>) -> Result<Value<Self>, Self::ValidationError> {
        if value.raw[0..16] == [0; 16] {
            Ok(value)
        } else {
            Err(())
        }
    }
}

/// Error that can occur when parsing an identifier from a Value.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IdParseError {
    IsNil,
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

impl FromValue<'_, GenId> for RawId {
    fn from_value(v: &Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl<'a> FromValue<'a, GenId> for &'a RawId {
    fn from_value(v: &'a Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl ToValue<GenId> for RawId {
    fn to_value(self) -> Value<GenId> {
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

impl FromValue<'_, GenId> for Id {
    fn from_value(v: &Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl<'a> FromValue<'a, GenId> for &'a Id {
    fn from_value(v: &'a Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl ToValue<GenId> for &Id {
    fn to_value(self) -> Value<GenId> {
        let mut data = [0; VALUE_LEN];
        data[16..32].copy_from_slice(&self[..]);
        Value::new(data)
    }
}

impl ToValue<GenId> for Id {
    fn to_value(self) -> Value<GenId> {
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

impl FromValue<'_, GenId> for uuid::Uuid {
    fn from_value(v: &Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExclusiveIdError {
    FailedParse(IdParseError),
    FailedAquire(),
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
        id.aquire().ok_or(ExclusiveIdError::FailedAquire())
    }
}

impl FromValue<'_, GenId> for ExclusiveId {
    fn from_value(v: &Value<GenId>) -> Self {
        v.try_from_value().unwrap()
    }
}

impl ToValue<GenId> for ExclusiveId {
    fn to_value(self) -> Value<GenId> {
        self.id.to_value()
    }
}

impl ToValue<GenId> for &ExclusiveId {
    fn to_value(self) -> Value<GenId> {
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

impl ToValue<GenId> for OwnedId<'_> {
    fn to_value(self) -> Value<GenId> {
        self.id.to_value()
    }
}

impl ToValue<GenId> for &OwnedId<'_> {
    fn to_value(self) -> Value<GenId> {
        self.id.to_value()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PackIdError {
    BadProtocol,
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
pub struct IdValueTree(RawId);

#[cfg(feature = "proptest")]
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
