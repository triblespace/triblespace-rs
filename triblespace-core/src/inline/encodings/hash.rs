use crate::blob::BlobEncoding;
use crate::id::ExclusiveId;
use crate::id::Id;
use crate::id_hex;
use crate::macros::entity;
use crate::metadata;
use crate::metadata::MetaDescribe;
use crate::trible::Fragment;
use crate::inline::RawInline;
use crate::inline::TryFromInline;
use crate::inline::TryToInline;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
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
    fn digest(bytes: &[u8]) -> RawInline;
}

/// Blake3 hash protocol — the canonical content-addressing hash
/// for triblespace blob storage. The [`MemoryBlobStore`], [`Pile`],
/// and [`Handle`] types are all implicitly Blake3-backed.
///
/// Implements [`HashProtocol`] so [`Hash<Blake3>`] is also a valid
/// "blake3 digest" inline encoding, parallel to hypothetical
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
    pub fn finalize(&self) -> RawInline {
        *self.hasher.finalize().as_bytes()
    }

    /// One-shot convenience: hash `bytes` with Blake3 and return
    /// the 32-byte digest. Mirrors [`HashProtocol::digest`] as an
    /// inherent method so call sites don't need to import the trait.
    pub fn digest(bytes: &[u8]) -> RawInline {
        <Self as HashProtocol>::digest(bytes)
    }
}

impl HashProtocol for Blake3 {
    const NAME: &'static str = "blake3";

    fn digest(bytes: &[u8]) -> RawInline {
        *blake3::hash(bytes).as_bytes()
    }
}

/// A inline encoding for a 32-byte hash digest.
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

impl<H> InlineEncoding for Hash<H>
where
    H: HashProtocol,
{
    type ValidationError = Infallible;
    type Encoding = Self;
}

impl<H> Hash<H>
where
    H: HashProtocol,
{
    /// Computes the hash of `blob` and returns it as a value.
    pub fn digest(blob: &Bytes) -> Inline<Self> {
        Inline::new(H::digest(blob))
    }

    /// Parses a hex-encoded digest string into a hash value.
    pub fn from_hex(hex: &str) -> Result<Inline<Self>, FromHexError> {
        let digest = RawInline::from_hex(hex)?;
        Ok(Inline::new(digest))
    }

    /// Returns the digest as an uppercase hex string.
    pub fn to_hex(value: &Inline<Self>) -> String {
        hex::encode_upper(value.raw)
    }
}

impl<H: HashProtocol> TryFromInline<'_, Hash<H>> for String {
    type Error = std::convert::Infallible;
    fn try_from_inline(v: &Inline<Hash<H>>) -> Result<Self, std::convert::Infallible> {
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

impl<H: HashProtocol> TryToInline<Hash<H>> for &str {
    type Error = HashError;

    fn try_to_inline(self) -> Result<Inline<Hash<H>>, Self::Error> {
        let protocol = H::NAME;
        if !(self.starts_with(protocol) && &self[protocol.len()..=protocol.len()] == ":") {
            return Err(HashError::BadProtocol);
        }
        let digest = RawInline::from_hex(&self[protocol.len() + 1..])?;

        Ok(Inline::new(digest))
    }
}

impl<H: HashProtocol> TryToInline<Hash<H>> for String {
    type Error = HashError;

    fn try_to_inline(self) -> Result<Inline<Hash<H>>, Self::Error> {
        (&self[..]).try_to_inline()
    }
}

fn describe_hash<H: HashProtocol>(id: Id) -> Fragment {
    let name = H::NAME;
    #[allow(unused_mut)]
    let mut tribles = entity! { ExclusiveId::force_ref(&id) @
        metadata::name: name,
        metadata::description: format!(
            "{name} 256-bit hash digest of raw bytes. The value stores the digest bytes and is stable across systems.\n\nUse for content-addressed identifiers, deduplication, or integrity checks. Use Handle when you need a typed blob reference with schema metadata.\n\nHashes do not carry type information; the meaning comes from the schema that uses them. If you need provenance or typed payloads, combine with handles or additional metadata."
        ),
        metadata::tag: metadata::KIND_INLINE_ENCODING,
    };
    #[cfg(feature = "wasm")]
    {
        tribles += entity! { ExclusiveId::force_ref(&id) @
            metadata::value_formatter: wasm_formatter::HASH_HEX_WASM,
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

/// The **lightweight reference form** of a content-addressed blob.
///
/// A `Handle<T>` is a 32-byte Blake3 hash plus a phantom marker for
/// the referenced blob's schema. It's the small, trible-storable,
/// network-sendable counterpart to a [`Blob<T>`][b] — the same
/// content/reference duality as `&[u8]`/`Vec<u8>`, except the
/// reference is hash-based rather than pointer-based and survives
/// crossing process and storage boundaries.
///
/// You store handles in tribles. You store blobs in
/// [`MemoryBlobStore`][m] / [`Pile`][p] / any other [`BlobStore`][bs]
/// backend. Pairing them — `(handle in trible) ↔ (blob in store)` —
/// is the canonical pattern for keeping the entity graph compact
/// while leaving heavy payloads (text, binary data, archived
/// subgraphs) addressable by content rather than by location.
///
/// Handles are produced *by* blobs: [`Blob::new`][bn] hashes the
/// bytes and stores the handle in the blob; [`Blob::get_handle`][bg]
/// returns it. A `&Blob<T>` also `AsRef`s to its handle, so passing
/// "the lightweight reference" through APIs that accept
/// `&Inline<Handle<T>>` is allocation-free.
///
/// [b]: crate::blob::Blob
/// [bn]: crate::blob::Blob::new
/// [bg]: crate::blob::Blob::get_handle
/// [m]: crate::blob::MemoryBlobStore
/// [p]: crate::repo::pile::Pile
/// [bs]: crate::repo::BlobStore
#[repr(transparent)]
pub struct Handle<T: BlobEncoding> {
    digest: Hash<Blake3>,
    _type: PhantomData<T>,
}

impl<T: BlobEncoding> Handle<T> {
    /// Wraps a Blake3 hash value as a typed handle.
    pub fn from_hash(hash: Inline<Hash<Blake3>>) -> Inline<Self> {
        hash.transmute()
    }

    /// Extracts the underlying Blake3 hash, discarding the blob encoding type.
    pub fn to_hash(handle: Inline<Self>) -> Inline<Hash<Blake3>> {
        handle.transmute()
    }
}

impl<T: BlobEncoding> From<Inline<Hash<Blake3>>> for Inline<Handle<T>> {
    fn from(value: Inline<Hash<Blake3>>) -> Self {
        value.transmute()
    }
}

impl<T: BlobEncoding> From<Inline<Handle<T>>> for Inline<Hash<Blake3>> {
    fn from(value: Inline<Handle<T>>) -> Self {
        value.transmute()
    }
}

impl<T> MetaDescribe for Handle<T>
where
    T: BlobEncoding + MetaDescribe,
{
    fn describe() -> Fragment {
        // Entity core via `*:` spread. `T::describe()` runs once: its
        // root becomes the value of `metadata::blob_encoding` and its
        // facts + blobs fold in automatically. With the hash protocol
        // fixed to Blake3, only the blob encoding parameter distinguishes
        // one `Handle<T>` monomorphization from another. Annotations
        // share the derived root, so merging them with `+=` re-unions
        // the same id into exports (idempotent) and folds their facts +
        // auto-put blobs into the core.
        let mut core = entity! {
            metadata::blob_encoding*: T::describe(),
            metadata::hash_schema*: Blake3::describe(),
            metadata::tag: metadata::KIND_INLINE_ENCODING,
        };
        let name = Blake3::NAME;
        let id = core.root().expect("rooted");
        let id_ref = ExclusiveId::force_ref(&id);
        core += entity! { id_ref @
            metadata::name: "handle",
            metadata::description: format!(
                "Typed handle for blobs hashed with {name}; the value stores the digest and metadata points at the referenced blob encoding. The schema id is derived from the hash and blob encoding.\n\nUse when referencing blobs from tribles without embedding data; the blob store holds the payload. For untyped content hashes, use the hash schema directly.\n\nHandles assume the blob store is available and consistent with the digest. If the blob is missing, the handle still validates but dereferencing will fail."
            ),
        };
        #[cfg(feature = "wasm")]
        {
            core += entity! { id_ref @
                metadata::value_formatter: wasm_formatter::HASH_HEX_WASM,
            };
        }
        core
    }
}

impl<T: BlobEncoding + MetaDescribe> InlineEncoding for Handle<T> {
    type ValidationError = Infallible;
    type Encoding = T;
}

impl MetaDescribe for Blake3 {
    fn describe() -> Fragment {
        describe_hash::<Self>(id_hex!("4160218D6C8F620652ECFBD7FDC7BDB3"))
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::inline::encodings::hash::HashError;
    use rand;

    use super::{Blake3, Hash};

    #[test]
    fn value_roundtrip() {
        let v: Inline<Hash<Blake3>> = Inline::new(rand::random());
        let s: String = v.from_inline();
        let _: Inline<Hash<Blake3>> = s.try_to_inline().expect("roundtrip should succeed");
    }

    #[test]
    fn value_from_known() {
        let s: &str = "blake3:CA98593CB9DC0FA48B2BE01E53D042E22B47862D646F9F19E2889A7961663663";
        let _: Inline<Hash<Blake3>> = s
            .try_to_inline()
            .expect("packing valid constant should succeed");
    }

    #[test]
    fn to_value_fail_protocol() {
        let s: &str = "bad:CA98593CB9DC0FA48B2BE01E53D042E22B47862D646F9F19E2889A7961663663";
        let err: HashError = <&str as TryToInline<Hash<Blake3>>>::try_to_inline(s)
            .expect_err("packing invalid protocol should fail");
        assert_eq!(err, HashError::BadProtocol);
    }

    #[test]
    fn to_value_fail_hex() {
        let s: &str = "blake3:BAD!";
        let err: HashError = <&str as TryToInline<Hash<Blake3>>>::try_to_inline(s)
            .expect_err("packing invalid protocol should fail");
        assert!(std::matches!(err, HashError::BadHex(..)));
    }
}
