use crate::blob::BlobSchema;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::{ConstId, ConstMetadata};
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

impl<H> ConstId for Hash<H>
where
    H: HashProtocol,
{
    const ID: Id = H::ID;
}

impl<H> ConstMetadata for Hash<H>
where
    H: HashProtocol,
{
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
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

fn describe_hash<H, B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
where
    H: HashProtocol,
    B: BlobStore<Blake3>,
{
    let id = H::ID;
    let name = H::NAME;
    let description = blobs.put(format!(
        "{name} 256-bit hash digest of raw bytes. The value stores the digest bytes and is stable across systems.\n\nUse for content-addressed identifiers, deduplication, or integrity checks. Use Handle when you need a typed blob reference with schema metadata.\n\nHashes do not carry type information; the meaning comes from the schema that uses them. If you need provenance or typed payloads, combine with handles or additional metadata."
    ))?;
    let name_handle = blobs.put(name.to_string())?;
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&id) @
        metadata::name: name_handle,
        metadata::description: description,
        metadata::tag: metadata::KIND_VALUE_SCHEMA,
    };

    #[cfg(feature = "wasm")]
    {
        tribles += entity! { ExclusiveId::force_ref(&id) @
            metadata::value_formatter: blobs.put(wasm_formatter::HASH_HEX_WASM)?,
        };
    }

    Ok(tribles)
}

#[cfg(feature = "wasm")]
mod wasm_formatter {
    use core::fmt::Write;

    use triblespace_core_macros::value_formatter;

    #[value_formatter]
    pub(crate) fn hash_hex(raw: &[u8; 32], out: &mut impl Write) -> Result<(), u32> {
        out.write_str("hash:").map_err(|_| 1u32)?;
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

use blake2::Blake2b as Blake2bUnsized;
pub type Blake2b = Blake2bUnsized<U32>;

pub use blake3::Hasher as Blake3;

impl HashProtocol for Blake2b {
    const NAME: &'static str = "blake2";
}

impl HashProtocol for Blake3 {
    const NAME: &'static str = "blake3";
}

impl ConstId for Blake2b {
    const ID: Id = id_hex!("91F880222412A49F012BE999942E6199");
}

impl ConstMetadata for Blake2b {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        describe_hash::<Self, B>(blobs)
    }
}

impl ConstId for Blake3 {
    const ID: Id = id_hex!("4160218D6C8F620652ECFBD7FDC7BDB3");
}

impl ConstMetadata for Blake3 {
    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        describe_hash::<Self, B>(blobs)
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

impl<H: HashProtocol, T: BlobSchema> ConstId for Handle<H, T> {
    const ID: Id = {
        let mut hasher = const_blake3::Hasher::new();
        hasher.update(&Hash::<H>::ID.raw());
        hasher.update(&T::ID.raw());
        let mut digest = [0u8; 32];
        hasher.finalize(&mut digest);
        let mut raw = [0u8; 16];
        let mut i = 0;
        while i < raw.len() {
            raw[i] = digest[16 + i];
            i += 1;
        }
        match Id::new(raw) {
            Some(id) => id,
            None => panic!("derived handle schema id must be non-nil"),
        }
    };
}

impl<H, T> ConstMetadata for Handle<H, T>
where
    H: HashProtocol,
    T: BlobSchema + ConstMetadata,
{

    fn describe<B>(blobs: &mut B) -> Result<TribleSet, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        let id = Self::ID;
        let name = H::NAME;
        let schema_id = T::ID;
        let description = blobs.put(format!(
            "Typed handle for blobs hashed with {name}; the value stores the digest and metadata points at blob schema {schema_id:X}. The schema id is derived from the hash and blob schema.\n\nUse when referencing blobs from tribles without embedding data; the blob store holds the payload. For untyped content hashes, use the hash schema directly.\n\nHandles assume the blob store is available and consistent with the digest. If the blob is missing, the handle still validates but dereferencing will fail."
        ))?;
        let name_handle = blobs.put("handle".to_string())?;
        let mut tribles = TribleSet::new();
        tribles += H::describe(blobs)?;
        tribles += T::describe(blobs)?;

        tribles += entity! { ExclusiveId::force_ref(&id) @
            metadata::name: name_handle,
            metadata::description: description,
            metadata::blob_schema: schema_id,
            metadata::hash_schema: H::ID,
            metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            tribles += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::HASH_HEX_WASM)?,
            };
        }
        Ok(tribles)
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
