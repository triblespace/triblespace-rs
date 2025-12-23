use crate::blob::BlobSchema;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::ConstMetadata;
use crate::repo::BlobStore;
use crate::trible::TribleSet;
use crate::value::FromValue;
use crate::value::RawValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use anybytes::Bytes;
use digest::typenum::U32;
use digest::Digest;
use hex::FromHex;
use hex::FromHexError;
use std::marker::PhantomData;

/// A trait for hash functions.
/// This trait is implemented by hash functions that can be in a value schema
/// for example via a [struct@Hash] or a [Handle].
pub trait HashProtocol: Digest<OutputSize = U32> + Clone + Send + 'static + ConstMetadata {
    const NAME: &'static str;
}

/// A value schema for a hash.
/// A hash is a fixed-size 256bit digest of a byte sequence.
///
/// See the [crate::id] module documentation for a discussion on the length
/// of the digest and its role as an intrinsic identifier.
pub struct Hash<H> {
    _hasher: PhantomData<fn(H) -> ()>,
}

impl<H> ConstMetadata for Hash<H>
where
    H: HashProtocol,
{
    fn id() -> Id {
        <H as ConstMetadata>::id()
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        H::describe(blobs)
    }
}

impl<H> ValueSchema for Hash<H>
where
    H: HashProtocol,
{
    type ValidationError = Infallible;
}

impl<H> Hash<H>
where
    H: HashProtocol,
{
    pub fn digest(blob: &Bytes) -> Value<Self> {
        Value::new(H::digest(blob).into())
    }

    pub fn from_hex(hex: &str) -> Result<Value<Self>, FromHexError> {
        let digest = RawValue::from_hex(hex)?;
        Ok(Value::new(digest))
    }

    pub fn to_hex(value: &Value<Self>) -> String {
        hex::encode_upper(value.raw)
    }
}

impl<H> FromValue<'_, Hash<H>> for String
where
    H: HashProtocol,
{
    fn from_value(v: &Value<Hash<H>>) -> Self {
        let mut out = String::new();
        out.push_str(<H as HashProtocol>::NAME);
        out.push(':');
        out.push_str(&hex::encode(v.raw));
        out
    }
}

/// An error that can occur when converting a hash value from a string.
/// The error can be caused by a bad protocol or a bad hex encoding.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HashError {
    BadProtocol,
    BadHex(FromHexError),
}

impl From<FromHexError> for HashError {
    fn from(value: FromHexError) -> Self {
        HashError::BadHex(value)
    }
}

impl<H> TryToValue<Hash<H>> for &str
where
    H: HashProtocol,
{
    type Error = HashError;

    fn try_to_value(self) -> Result<Value<Hash<H>>, Self::Error> {
        let protocol = <H as HashProtocol>::NAME;
        if !(self.starts_with(protocol) && &self[protocol.len()..=protocol.len()] == ":") {
            return Err(HashError::BadProtocol);
        }
        let digest = RawValue::from_hex(&self[protocol.len() + 1..])?;

        Ok(Value::new(digest))
    }
}

impl<H> TryToValue<Hash<H>> for String
where
    H: HashProtocol,
{
    type Error = HashError;

    fn try_to_value(self) -> Result<Value<Hash<H>>, Self::Error> {
        (&self[..]).try_to_value()
    }
}

fn describe_hash<H>(blobs: &mut impl BlobStore<Blake3>) -> TribleSet
where
    H: HashProtocol,
{
    let _ = blobs;
    let mut tribles = TribleSet::new();

    let entity = ExclusiveId::force(H::id());
    tribles += entity! { &entity @ metadata::shortname: H::NAME };

    #[cfg(feature = "builtin-wasm-formatters")]
    {
        tribles += super::wasm_formatters::describe_value_formatter(
            blobs,
            H::id(),
            super::wasm_formatters::HEX32_WASM,
        );
    }

    tribles
}

use blake2::Blake2b as Blake2bUnsized;
pub type Blake2b = Blake2bUnsized<U32>;

pub use blake3::Hasher as Blake3;

impl HashProtocol for Blake2b {
    const NAME: &'static str = "blake2";
}

impl HashProtocol for Blake3 {
    const NAME: &'static str = "blake3";
}

impl ConstMetadata for Blake2b {
    fn id() -> Id {
        id_hex!("91F880222412A49F012BE999942E6199")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        describe_hash::<Self>(blobs)
    }
}

impl ConstMetadata for Blake3 {
    fn id() -> Id {
        id_hex!("4160218D6C8F620652ECFBD7FDC7BDB3")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        describe_hash::<Self>(blobs)
    }
}

/// This is a value schema for a handle.
/// A handle to a blob is comprised of a hash of a blob and type level information about the blobs schema.
///
/// The handle can be stored in a Trible, while the blob can be stored in a BlobSet, allowing for a
/// separation of the blob data from the means of identifying and accessing it.
///
/// The handle is generated when a blob is inserted into a BlobSet, and the handle
/// can be used to retrieve the blob from the BlobSet later.
#[repr(transparent)]
pub struct Handle<H: HashProtocol, T: BlobSchema> {
    digest: Hash<H>,
    _type: PhantomData<T>,
}

impl<H: HashProtocol, T: BlobSchema> Handle<H, T> {
    pub fn from_hash(hash: Value<Hash<H>>) -> Value<Self> {
        hash.transmute()
    }

    pub fn to_hash(handle: Value<Self>) -> Value<Hash<H>> {
        handle.transmute()
    }
}

impl<H: HashProtocol, T: BlobSchema> From<Value<Hash<H>>> for Value<Handle<H, T>> {
    fn from(value: Value<Hash<H>>) -> Self {
        value.transmute()
    }
}

impl<H: HashProtocol, T: BlobSchema> From<Value<Handle<H, T>>> for Value<Hash<H>> {
    fn from(value: Value<Handle<H, T>>) -> Self {
        value.transmute()
    }
}

impl<H: HashProtocol, T: BlobSchema> ConstMetadata for Handle<H, T> {
    // NOTE: This can't be a `const fn` while we rely on the runtime `blake3`
    // hasher to derive the identifier. Once a const-friendly hashing API is
    // available we can revisit this.
    fn id() -> Id {
        let mut hasher = blake3::Hasher::new();
        hasher.update(Hash::<H>::id().as_ref());
        hasher.update(T::id().as_ref());
        let digest = hasher.finalize();
        let mut raw = [0u8; 16];
        let bytes: &[u8] = digest.as_ref();
        let lower_half = &bytes[bytes.len() - raw.len()..];
        raw.copy_from_slice(lower_half);
        Id::new(raw).expect("derived handle schema id must be non-nil")
    }

    fn describe(blobs: &mut impl BlobStore<Blake3>) -> TribleSet {
        let mut tribles = TribleSet::new();
        tribles += H::describe(blobs);
        tribles += T::describe(blobs);

        let entity = ExclusiveId::force(Self::id());
        tribles += entity! { &entity @
            metadata::blob_schema: T::id(),
            metadata::hash_schema: H::id(),
        };

        let _ = blobs;
        #[cfg(feature = "builtin-wasm-formatters")]
        {
            tribles += super::wasm_formatters::describe_value_formatter(
                blobs,
                Self::id(),
                super::wasm_formatters::HEX32_WASM,
            );
        }
        tribles
    }
}

impl<H: HashProtocol, T: BlobSchema> ValueSchema for Handle<H, T> {
    type ValidationError = Infallible;
}

#[cfg(test)]
mod tests {
    use super::Blake3;
    use crate::prelude::*;
    use crate::value::schemas::hash::HashError;
    use rand;

    use super::Hash;

    #[test]
    fn value_roundtrip() {
        let v: Value<Hash<Blake3>> = Value::new(rand::random());
        let s: String = v.from_value();
        let _: Value<Hash<Blake3>> = s.try_to_value().expect("roundtrip should succeed");
    }

    #[test]
    fn value_from_known() {
        let s: &str = "blake3:CA98593CB9DC0FA48B2BE01E53D042E22B47862D646F9F19E2889A7961663663";
        let _: Value<Hash<Blake3>> = s
            .try_to_value()
            .expect("packing valid constant should succeed");
    }

    #[test]
    fn to_value_fail_protocol() {
        let s: &str = "bad:CA98593CB9DC0FA48B2BE01E53D042E22B47862D646F9F19E2889A7961663663";
        let err: HashError = <&str as TryToValue<Hash<Blake3>>>::try_to_value(s)
            .expect_err("packing invalid protocol should fail");
        assert_eq!(err, HashError::BadProtocol);
    }

    #[test]
    fn to_value_fail_hex() {
        let s: &str = "blake3:BAD!";
        let err: HashError = <&str as TryToValue<Hash<Blake3>>>::try_to_value(s)
            .expect_err("packing invalid protocol should fail");
        assert!(std::matches!(err, HashError::BadHex(..)));
    }
}
