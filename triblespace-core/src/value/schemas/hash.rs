use crate::blob::BlobSchema;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::trible::TribleSet;
use crate::value::RawValue;
use crate::value::TryFromValue;
use crate::value::TryToValue;
use crate::value::Value;
use crate::value::ValueSchema;
use std::convert::Infallible;

use anybytes::Bytes;
use hex::FromHex;
use hex::FromHexError;
use std::marker::PhantomData;

/// A 32-byte content-addressed hash function.
///
/// triblespace's *storage* layer (handles, blob stores, piles) is
/// fixed to [`Blake3`] — that's the content-addressing hash that
/// produces every [`Handle<T>`]. This trait stays generic so
/// [`Hash<H>`] can carry digests produced by *other* hash functions
/// (e.g. an external system's SHA-256 fingerprints) alongside
/// Blake3 in the same store, distinguished by their schema type at
/// the value layer. Only the storage-side parameter went away; the
/// value-side distinction between digest families is still useful.
pub trait HashProtocol: Sized + 'static + MetaDescribe {
    /// Short lowercase name used in serialised representations (e.g. `"blake3"`).
    const NAME: &'static str;

    /// One-shot convenience: hash `bytes` and return the digest.
    fn digest(bytes: &[u8]) -> RawValue;
}

/// Blake3 hash protocol — the canonical content-addressing hash
/// for triblespace blob storage. The [`MemoryBlobStore`], [`Pile`],
/// and [`Handle`] types are all implicitly Blake3-backed.
///
/// Implements [`HashProtocol`] so [`Hash<Blake3>`] is also a valid
/// "blake3 digest" value schema, parallel to hypothetical
/// `Hash<Sha256>` etc. for foreign-hash fingerprints.
pub struct Blake3 {
    hasher: blake3::Hasher,
}

impl Clone for Blake3 {
    fn clone(&self) -> Self {
        Self {
            hasher: self.hasher.clone(),
        }
    }
}

impl Default for Blake3 {
    fn default() -> Self {
        Self::new()
    }
}

impl Blake3 {
    /// Short lowercase name used in serialised representations.
    pub const NAME: &'static str = <Self as HashProtocol>::NAME;

    /// Create a fresh hasher ready to accept input.
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    /// Feed `bytes` into the streaming state.
    pub fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    /// Return the 32-byte digest of the bytes fed so far.
    pub fn finalize(&self) -> RawValue {
        *self.hasher.finalize().as_bytes()
    }

    /// One-shot convenience: hash `bytes` with Blake3 and return
    /// the 32-byte digest. Mirrors [`HashProtocol::digest`] as an
    /// inherent method so call sites don't need to import the trait.
    pub fn digest(bytes: &[u8]) -> RawValue {
        <Self as HashProtocol>::digest(bytes)
    }
}

impl HashProtocol for Blake3 {
    const NAME: &'static str = "blake3";

    fn digest(bytes: &[u8]) -> RawValue {
        *blake3::hash(bytes).as_bytes()
    }
}

/// A value schema for a 32-byte hash digest.
///
/// `H` selects the hash function — `Hash<Blake3>` for blake3-produced
/// digests, hypothetical `Hash<Sha256>` for foreign 256-bit
/// fingerprints carried alongside. This stays parametric so a store
/// can hold both kinds of digests with type-level distinction; only
/// the storage-side wiring ([`Handle`], [`MemoryBlobStore`], piles)
/// is fixed to Blake3.
///
/// See the [crate::id] module documentation for a discussion on the
/// length of the digest and its role as an intrinsic identifier.
pub struct Hash<H> {
    _hasher: PhantomData<fn(H) -> ()>,
}

impl<H> MetaDescribe for Hash<H>
where
    H: HashProtocol,
{
    fn describe() -> Fragment {
        H::describe()
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
    /// Computes the hash of `blob` and returns it as a value.
    pub fn digest(blob: &Bytes) -> Value<Self> {
        Value::new(H::digest(blob))
    }

    /// Parses a hex-encoded digest string into a hash value.
    pub fn from_hex(hex: &str) -> Result<Value<Self>, FromHexError> {
        let digest = RawValue::from_hex(hex)?;
        Ok(Value::new(digest))
    }

    /// Returns the digest as an uppercase hex string.
    pub fn to_hex(value: &Value<Self>) -> String {
        hex::encode_upper(value.raw)
    }
}

impl<H: HashProtocol> TryFromValue<'_, Hash<H>> for String {
    type Error = std::convert::Infallible;
    fn try_from_value(v: &Value<Hash<H>>) -> Result<Self, std::convert::Infallible> {
        let mut out = String::new();
        out.push_str(H::NAME);
        out.push(':');
        out.push_str(&hex::encode(v.raw));
        Ok(out)
    }
}

/// An error that can occur when converting a hash value from a string.
/// The error can be caused by a bad protocol or a bad hex encoding.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HashError {
    /// The string does not start with the expected protocol prefix
    /// (e.g. `"blake3:"`).
    BadProtocol,
    /// The hex portion could not be decoded.
    BadHex(FromHexError),
}

impl From<FromHexError> for HashError {
    fn from(value: FromHexError) -> Self {
        HashError::BadHex(value)
    }
}

impl<H: HashProtocol> TryToValue<Hash<H>> for &str {
    type Error = HashError;

    fn try_to_value(self) -> Result<Value<Hash<H>>, Self::Error> {
        let protocol = H::NAME;
        if !(self.starts_with(protocol) && &self[protocol.len()..=protocol.len()] == ":") {
            return Err(HashError::BadProtocol);
        }
        let digest = RawValue::from_hex(&self[protocol.len() + 1..])?;

        Ok(Value::new(digest))
    }
}

impl<H: HashProtocol> TryToValue<Hash<H>> for String {
    type Error = HashError;

    fn try_to_value(self) -> Result<Value<Hash<H>>, Self::Error> {
        (&self[..]).try_to_value()
    }
}

fn describe_hash<H: HashProtocol>(id: Id) -> Fragment {
    let name = H::NAME;
    let mut tribles = Fragment::rooted(id, TribleSet::new());
    let description = tribles.put(format!(
        "{name} 256-bit hash digest of raw bytes. The value stores the digest bytes and is stable across systems.\n\nUse for content-addressed identifiers, deduplication, or integrity checks. Use Handle when you need a typed blob reference with schema metadata.\n\nHashes do not carry type information; the meaning comes from the schema that uses them. If you need provenance or typed payloads, combine with handles or additional metadata."
    ));
    let name_handle = tribles.put(name);
    tribles += entity! { ExclusiveId::force_ref(&id) @
        metadata::name: name_handle,
        metadata::description: description,
        metadata::tag: metadata::KIND_VALUE_SCHEMA,
    };
    #[cfg(feature = "wasm")]
    {
        let formatter = tribles.put(wasm_formatter::HASH_HEX_WASM);
        tribles += entity! { ExclusiveId::force_ref(&id) @
            metadata::value_formatter: formatter,
        };
    }
    tribles
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

/// This is a value schema for a handle.
/// A handle to a blob is comprised of a hash of a blob and type level information about the blobs schema.
///
/// The handle can be stored in a Trible, while the blob can be stored in a BlobSet, allowing for a
/// separation of the blob data from the means of identifying and accessing it.
///
/// The handle is generated when a blob is inserted into a BlobSet, and the handle
/// can be used to retrieve the blob from the BlobSet later.
#[repr(transparent)]
pub struct Handle<T: BlobSchema> {
    digest: Hash<Blake3>,
    _type: PhantomData<T>,
}

impl<T: BlobSchema> Handle<T> {
    /// Wraps a Blake3 hash value as a typed handle.
    pub fn from_hash(hash: Value<Hash<Blake3>>) -> Value<Self> {
        hash.transmute()
    }

    /// Extracts the underlying Blake3 hash, discarding the blob schema type.
    pub fn to_hash(handle: Value<Self>) -> Value<Hash<Blake3>> {
        handle.transmute()
    }
}

impl<T: BlobSchema> From<Value<Hash<Blake3>>> for Value<Handle<T>> {
    fn from(value: Value<Hash<Blake3>>) -> Self {
        value.transmute()
    }
}

impl<T: BlobSchema> From<Value<Handle<T>>> for Value<Hash<Blake3>> {
    fn from(value: Value<Handle<T>>) -> Self {
        value.transmute()
    }
}

impl<T> MetaDescribe for Handle<T>
where
    T: BlobSchema + MetaDescribe,
{
    fn describe() -> Fragment {
        // Entity core via `*:` spread. `T::describe()` runs once: its
        // root becomes the value of `metadata::blob_schema` and its
        // facts + blobs fold in automatically. With the hash protocol
        // fixed to Blake3, only the blob schema parameter distinguishes
        // one `Handle<T>` monomorphization from another; `annotated`
        // layers the human-facing annotations under the derived root.
        let mut core = entity! {
            metadata::blob_schema*: T::describe(),
            metadata::hash_schema*: Blake3::describe(),
            metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };
        let name = Blake3::NAME;
        let description_handle = core.put(format!(
            "Typed handle for blobs hashed with {name}; the value stores the digest and metadata points at the referenced blob schema. The schema id is derived from the hash and blob schema.\n\nUse when referencing blobs from tribles without embedding data; the blob store holds the payload. For untyped content hashes, use the hash schema directly.\n\nHandles assume the blob store is available and consistent with the digest. If the blob is missing, the handle still validates but dereferencing will fail."
        ));
        let name_handle = core.put("handle");
        #[cfg(feature = "wasm")]
        let wasm_handle = core.put(wasm_formatter::HASH_HEX_WASM);
        core.annotated(|id_ref| {
            #[allow(unused_mut)]
            let mut annotations = entity! { id_ref @
                metadata::name: name_handle,
                metadata::description: description_handle,
            };
            #[cfg(feature = "wasm")]
            {
                annotations += entity! { id_ref @
                    metadata::value_formatter: wasm_handle,
                };
            }
            annotations
        })
    }
}

impl<T: BlobSchema + MetaDescribe> ValueSchema for Handle<T> {
    type ValidationError = Infallible;
}

impl MetaDescribe for Blake3 {
    fn describe() -> Fragment {
        describe_hash::<Self>(id_hex!("4160218D6C8F620652ECFBD7FDC7BDB3"))
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::value::schemas::hash::HashError;
    use rand;

    use super::{Blake3, Hash};

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
