//! Generic hard-retention semantics for asserted pins.
//!
//! [`StrongPinDescriptor`] is a deliberately small decorator around another
//! content-addressed pin descriptor. The generic asserted-pin envelope uses
//! the decorator's handle as its [`PinHandle`](super::pin_assertion::PinHandle),
//! while the wrapped descriptor remains responsible for every domain-specific
//! meaning: value interpretation, labels, resolution, authorization, and UI.
//!
//! A retention backend that recognizes this exact outer descriptor may keep
//! the outer descriptor, the wrapped descriptor's locally present closure, and
//! every distinct authentic assertion value's locally present closure. Missing
//! or malformed outer content is neutral: the assertion remains durable but
//! acquires no hard-retention semantics until valid descriptor content arrives.
//! This module intentionally defines no resolver, signer, or store trait.

use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::VerifyingKey;
use hex_literal::hex;

use super::pin_assertion::{PinHandle, PinIdentity};
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, TryFromBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::{Inline, InlineEncoding};
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};
use crate::trible::Fragment;

/// Canonical byte length of a V1 strong-pin descriptor.
pub const STRONG_PIN_DESCRIPTOR_LEN: usize = 16 + 32;

/// Kind/schema marker for the V1 strong-pin descriptor.
///
/// Minted with `trible genid` on 2026-08-02.
pub const STRONG_PIN_DESCRIPTOR_V1: [u8; 16] = hex!("D8C90DA77903FBBB84DCBE912AACE43E");

/// Retention-only decorator around an exact inner pin descriptor handle.
///
/// The canonical bytes are `kind marker [16] | inner descriptor handle [32]`.
/// The wrapper says only that locally present content reachable from the inner
/// descriptor and asserted values is hard retention state. It deliberately
/// does not confer any interpretation or authorization on the inner kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StrongPinDescriptor;

impl BlobEncoding for StrongPinDescriptor {}

impl MetaDescribe for StrongPinDescriptor {
    fn describe() -> Fragment {
        let id: Id = id_hex!("D8C90DA77903FBBB84DCBE912AACE43E");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "strong-pin-descriptor-v1",
                metadata::description: "Canonical retention-only wrapper for an asserted pin descriptor: a V1 kind marker followed by the exact content handle of the inner descriptor. Recognized assertions retain the locally present closure of the inner descriptor and every distinct asserted value.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl StrongPinDescriptor {
    /// Encode one inner descriptor handle into canonical wrapper bytes.
    pub fn encode<S>(inner: Inline<Handle<S>>) -> [u8; STRONG_PIN_DESCRIPTOR_LEN]
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        let mut raw = [0u8; STRONG_PIN_DESCRIPTOR_LEN];
        raw[..16].copy_from_slice(&STRONG_PIN_DESCRIPTOR_V1);
        raw[16..].copy_from_slice(&inner.raw);
        raw
    }

    /// Build the outer descriptor blob for an exact inner descriptor handle.
    pub fn blob<S>(inner: Inline<Handle<S>>) -> Blob<Self>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        Blob::<Self>::new(Bytes::from_source(Self::encode(inner).to_vec()))
    }

    /// Derive the generic pin handle without loading either descriptor.
    pub fn pin_handle<S>(inner: Inline<Handle<S>>) -> PinHandle
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        PinHandle::from_raw(Blake3::digest(&Self::encode(inner)))
    }

    /// Derive the exact asserted-pin identity for one author and inner kind.
    pub fn pin_identity<S>(author: VerifyingKey, inner: Inline<Handle<S>>) -> PinIdentity
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        PinIdentity::new(author, Self::pin_handle(inner))
    }

    /// Reinterpret an opaque generic pin handle as its outer descriptor handle.
    pub fn descriptor_handle(pin: PinHandle) -> Inline<Handle<Self>> {
        Inline::new(pin.raw())
    }

    /// Decode one exact canonical outer descriptor.
    pub fn decode(
        blob: Blob<Self>,
    ) -> Result<Inline<Handle<UnknownBlob>>, StrongPinDescriptorError> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != STRONG_PIN_DESCRIPTOR_LEN {
            return Err(StrongPinDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes[..16] != STRONG_PIN_DESCRIPTOR_V1 {
            return Err(StrongPinDescriptorError::WrongKind);
        }
        Ok(Inline::new(
            bytes[16..].try_into().expect("descriptor length checked"),
        ))
    }
}

impl TryFromBlob<StrongPinDescriptor> for Inline<Handle<UnknownBlob>> {
    type Error = StrongPinDescriptorError;

    fn try_from_blob(blob: Blob<StrongPinDescriptor>) -> Result<Self, Self::Error> {
        StrongPinDescriptor::decode(blob)
    }
}

/// A strong-pin descriptor was not the exact canonical V1 shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrongPinDescriptorError {
    /// The descriptor was not exactly 48 bytes.
    WrongLength { actual: usize },
    /// The descriptor did not carry the strong-pin V1 kind marker.
    WrongKind,
}

impl fmt::Display for StrongPinDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "strong pin descriptor is {actual} bytes, expected {STRONG_PIN_DESCRIPTOR_LEN}"
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 strong descriptor"),
        }
    }
}

impl Error for StrongPinDescriptorError {}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn inner(byte: u8) -> Inline<Handle<UnknownBlob>> {
        Inline::new([byte; 32])
    }

    #[test]
    fn descriptor_is_canonical_typed_content_and_roundtrips() {
        let inner = inner(7);
        let blob = StrongPinDescriptor::blob(inner);
        assert_eq!(blob.bytes.len(), STRONG_PIN_DESCRIPTOR_LEN);
        assert_eq!(&blob.bytes[..16], &STRONG_PIN_DESCRIPTOR_V1);
        assert_eq!(&blob.bytes[16..], &inner.raw);
        assert_eq!(
            StrongPinDescriptor::pin_handle(inner).raw(),
            blob.get_handle().raw
        );

        let decoded: Inline<Handle<UnknownBlob>> = blob.try_from_blob().unwrap();
        assert_eq!(decoded.raw, inner.raw);
    }

    #[test]
    fn descriptor_rejects_wrong_kind_and_noncanonical_length() {
        let mut wrong_kind = StrongPinDescriptor::encode(inner(3));
        wrong_kind[0] ^= 1;
        let err = Blob::<StrongPinDescriptor>::new(Bytes::from_source(wrong_kind.to_vec()))
            .try_from_blob::<Inline<Handle<UnknownBlob>>>()
            .unwrap_err();
        assert_eq!(err, StrongPinDescriptorError::WrongKind);

        let err = Blob::<StrongPinDescriptor>::new(Bytes::from_source(vec![0u8; 47]))
            .try_from_blob::<Inline<Handle<UnknownBlob>>>()
            .unwrap_err();
        assert_eq!(err, StrongPinDescriptorError::WrongLength { actual: 47 });
    }

    #[test]
    fn wrapped_descriptor_and_author_are_both_identity() {
        let first_inner = inner(1);
        let second_inner = inner(2);
        assert_ne!(
            StrongPinDescriptor::pin_handle(first_inner),
            StrongPinDescriptor::pin_handle(second_inner)
        );

        let first = StrongPinDescriptor::pin_identity(
            SigningKey::from_bytes(&[1; 32]).verifying_key(),
            first_inner,
        );
        let second = StrongPinDescriptor::pin_identity(
            SigningKey::from_bytes(&[2; 32]).verifying_key(),
            first_inner,
        );
        assert_ne!(first, second);
        assert_ne!(first.digest(), second.digest());
    }
}
