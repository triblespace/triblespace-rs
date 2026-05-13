use crate::blob::BlobSchema;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::repo::BlobStore;
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

/// A trait for 32-byte content-addressed hash functions.
///
/// Implementors expose the minimal streaming API the rest of triblespace
/// relies on (`new` / `update` / `finalize`) plus a one-shot `digest`
/// helper. The previous version of this trait extended the `digest`
/// crate's [`Digest`](https://docs.rs/digest) trait via blake3's
/// `traits-preview` feature, but `traits-preview` is explicitly
/// pre-stable and its pinned `digest` dependency conflicts with other
/// iroh-ecosystem crates. Defining a minimal trait here keeps
/// triblespace-core dep-independent from the digest crate entirely.
pub trait HashProtocol: Clone + Send + 'static + MetaDescribe {
    /// Short lowercase name used in serialised representations (e.g. `"blake3"`).
    const NAME: &'static str;

    /// Create a fresh hasher ready to accept input.
    fn new() -> Self;

    /// Feed `bytes` into the streaming state.
    fn update(&mut self, bytes: &[u8]);

    /// Return the 32-byte digest of the bytes fed so far. Takes
    /// `&self` (not `self`) so method-resolution on `blake3::Hasher`
    /// still prefers blake3's inherent `finalize` returning a
    /// `blake3::Hash` — that matters for call sites (including the
    /// `entity!` macro expansion) that want the native API.
    fn finalize(&self) -> RawValue;

    /// One-shot convenience: hash `bytes` and return the digest.
    fn digest(bytes: &[u8]) -> RawValue;
}

/// A value schema for a hash.
/// A hash is a fixed-size 256bit digest of a byte sequence.
///
/// See the [crate::id] module documentation for a discussion on the length
/// of the digest and its role as an intrinsic identifier.
pub struct Hash<H> {
    _hasher: PhantomData<fn(H) -> ()>,
}

impl<H> MetaDescribe for Hash<H>
where
    H: HashProtocol,
{
    // Hash<H>'s schema id IS H's schema id — Hash<H> is the value-schema
    // facet of the same conceptual entity. describe delegates to H so the
    // fragment's intrinsic root is H's id.
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
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

impl<H> TryFromValue<'_, Hash<H>> for String
where
    H: HashProtocol,
{
    type Error = std::convert::Infallible;
    fn try_from_value(v: &Value<Hash<H>>) -> Result<Self, std::convert::Infallible> {
        let mut out = String::new();
        out.push_str(<H as HashProtocol>::NAME);
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

fn describe_hash<H, B>(blobs: &mut B, id: Id) -> Result<Fragment, B::PutError>
where
    H: HashProtocol,
    B: BlobStore<Blake3>,
{
    // `id` is passed in by the HashProtocol impl so we don't recurse through
    // `H::id()` (which would call `H::describe` which would call us back).
    let name = H::NAME;
    let description = blobs.put(format!(
        "{name} 256-bit hash digest of raw bytes. The value stores the digest bytes and is stable across systems.\n\nUse for content-addressed identifiers, deduplication, or integrity checks. Use Handle when you need a typed blob reference with schema metadata.\n\nHashes do not carry type information; the meaning comes from the schema that uses them. If you need provenance or typed payloads, combine with handles or additional metadata."
    ))?;
    let name_handle = blobs.put(name)?;
    let tribles = entity! { ExclusiveId::force_ref(&id) @
        metadata::name: name_handle,
        metadata::description: description,
        metadata::tag: metadata::KIND_VALUE_SCHEMA,
    };

    #[cfg(feature = "wasm")]
    let tribles = {
        let mut tribles = tribles;
        tribles += entity! { ExclusiveId::force_ref(&id) @
            metadata::value_formatter: blobs.put(wasm_formatter::HASH_HEX_WASM)?,
        };
        tribles
    };

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

/// Blake3 hasher, usable as a [`HashProtocol`]. This is the default
/// hash function for content-addressed blob storage.
pub use blake3::Hasher as Blake3;

impl HashProtocol for Blake3 {
    const NAME: &'static str = "blake3";

    fn new() -> Self {
        blake3::Hasher::new()
    }

    fn update(&mut self, bytes: &[u8]) {
        blake3::Hasher::update(self, bytes);
    }

    fn finalize(&self) -> RawValue {
        *blake3::Hasher::finalize(self).as_bytes()
    }

    fn digest(bytes: &[u8]) -> RawValue {
        *blake3::hash(bytes).as_bytes()
    }
}

impl MetaDescribe for Blake3 {
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        describe_hash::<Self, B>(blobs, id_hex!("4160218D6C8F620652ECFBD7FDC7BDB3"))
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
    /// Wraps a hash value as a typed handle.
    pub fn from_hash(hash: Value<Hash<H>>) -> Value<Self> {
        hash.transmute()
    }

    /// Extracts the underlying hash, discarding the blob schema type.
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

impl<H, T> MetaDescribe for Handle<H, T>
where
    H: HashProtocol,
    T: BlobSchema + MetaDescribe,
{
    fn describe<B>(blobs: &mut B) -> Result<Fragment, B::PutError>
    where
        B: BlobStore<Blake3>,
    {
        // Step 1: entity core — *only* the facts that determine identity.
        // The hash_schema + blob_schema pair distinguishes one `Handle<H,T>`
        // monomorphization from another; reword the name or description and
        // the id stays stable.
        let mut fragment = entity! {
            metadata::blob_schema: T::id(),
            metadata::hash_schema: H::id(),
        };
        let id = fragment
            .root()
            .expect("entity! without `@` always emits a rooted fragment");

        // Step 2: annotate the core with human-facing facts.
        let name = H::NAME;
        let description_handle = blobs.put(format!(
            "Typed handle for blobs hashed with {name}; the value stores the digest and metadata points at blob schema {schema_id:X}. The schema id is derived from the hash and blob schema.\n\nUse when referencing blobs from tribles without embedding data; the blob store holds the payload. For untyped content hashes, use the hash schema directly.\n\nHandles assume the blob store is available and consistent with the digest. If the blob is missing, the handle still validates but dereferencing will fail.",
            schema_id = T::id()
        ))?;
        let name_handle = blobs.put("handle")?;
        fragment += entity! { ExclusiveId::force_ref(&id) @
            metadata::name: name_handle,
            metadata::description: description_handle,
            metadata::tag: metadata::KIND_VALUE_SCHEMA,
        };

        #[cfg(feature = "wasm")]
        {
            fragment += entity! { ExclusiveId::force_ref(&id) @
                metadata::value_formatter: blobs.put(wasm_formatter::HASH_HEX_WASM)?,
            };
        }

        // Recursive metadata about the underlying hash and blob schemas —
        // attached as auxiliary facts (TribleSet level) so their roots
        // don't expand this fragment's exports.
        fragment += H::describe(blobs)?.into_facts();
        fragment += T::describe(blobs)?.into_facts();

        Ok(fragment)
    }

    // id() uses the describe-based default. Handle's describe builds the
    // core entity first (step 1) and attaches annotations under its root —
    // the fragment's intrinsic root is the core's id, exactly the schema id
    // we want.
}

impl<H: HashProtocol, T: BlobSchema + MetaDescribe> ValueSchema for Handle<H, T> {
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
