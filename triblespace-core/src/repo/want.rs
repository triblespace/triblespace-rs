//! Grow-only fetch wants as one typed view over asserted pins.
//!
//! A want is not a mutable weak-pin bit. Each author owns one fixed pin
//! identity and asserts exact blob handles as its values. The view is therefore
//! a G-set: duplicate assertions collapse, satisfaction makes an entry inert,
//! and there is no semantic unpin or tombstone. Forgetting is an explicit
//! physical rewrite; concatenating an older pile may harmlessly restore a want.
//!
//! This kind has no ancestry or dominance relation. Its label bytes are fixed
//! solely to make repeated signing canonical; the resolver never compares
//! them. That—not a supposedly neutral label value—is how this kind takes zero
//! label-based skips.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use super::pin_assertion::{
    PinAssertion, PinAssertionSnapshot, PinAssertionStore, PinHandle, PinIdentity,
    SubsumptionLabel, ValueHandle,
};
use crate::blob::encodings::UnknownBlob;
use crate::blob::{Blob, BlobEncoding, TryFromBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::{Inline, InlineEncoding};
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};
use crate::trible::Fragment;

/// Canonical byte length of the one fixed V1 want-set descriptor.
pub const WANT_PIN_DESCRIPTOR_LEN: usize = 16;

/// Kind/schema marker carried by the fixed V1 want-set descriptor.
///
/// Minted with `trible genid` on 2026-08-02.
pub const WANT_PIN_DESCRIPTOR_V1: [u8; WANT_PIN_DESCRIPTOR_LEN] =
    hex!("38AB8FB8FE80F3054AB165A64E322FF1");

/// Blob encoding for the fixed descriptor naming an author's want G-set.
///
/// Every author uses the same descriptor content; the author key in
/// [`PinIdentity`] keeps their sets distinct. The wanted blob handle belongs in
/// the assertion value—not in this descriptor—so one pin can accumulate many
/// values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WantPinDescriptor;

impl BlobEncoding for WantPinDescriptor {}

impl MetaDescribe for WantPinDescriptor {
    fn describe() -> Fragment {
        // The schema id doubles as the exact in-band V1 kind marker. A format
        // change mints a new schema instead of adding hidden version state.
        let id: Id = id_hex!("38AB8FB8FE80F3054AB165A64E322FF1");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "want-pin-descriptor-v1",
                metadata::description: "Fixed descriptor for one author's grow-only set of asserted blob-fetch wants. Wanted handles are assertion values; this descriptor identifies the collection kind.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl WantPinDescriptor {
    /// Materialize the canonical fixed descriptor blob when a store wants to
    /// retain descriptors for inspection. Typed lookup does not require it.
    pub fn blob() -> Blob<Self> {
        Blob::new(Bytes::from_source(WANT_PIN_DESCRIPTOR_V1.to_vec()))
    }

    /// Derive the fixed generic pin handle without loading descriptor content.
    pub fn pin_handle() -> PinHandle {
        PinHandle::from_raw(Blake3::digest(&WANT_PIN_DESCRIPTOR_V1))
    }

    /// Exact identity of one author's want G-set.
    pub fn pin_identity(author: VerifyingKey) -> PinIdentity {
        PinIdentity::new(author, Self::pin_handle())
    }
}

impl TryFromBlob<WantPinDescriptor> for WantPinDescriptor {
    type Error = WantPinDescriptorError;

    fn try_from_blob(blob: Blob<WantPinDescriptor>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != WANT_PIN_DESCRIPTOR_LEN {
            return Err(WantPinDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes != WANT_PIN_DESCRIPTOR_V1 {
            return Err(WantPinDescriptorError::WrongKind);
        }
        Ok(Self)
    }
}

/// A want-set descriptor was not the exact canonical V1 token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WantPinDescriptorError {
    /// The descriptor was not exactly 16 bytes.
    WrongLength { actual: usize },
    /// The descriptor carried another kind marker.
    WrongKind,
}

impl fmt::Display for WantPinDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "want pin descriptor is {actual} bytes, expected {WANT_PIN_DESCRIPTOR_LEN}"
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 want-set descriptor"),
        }
    }
}

impl Error for WantPinDescriptorError {}

fn canonical_label() -> SubsumptionLabel {
    // This is canonical signed padding for this kind, not a neutral element of
    // the generic label order. No want operation compares it.
    SubsumptionLabel::from_raw([0u8; 32])
}

fn value_from_handle<S>(handle: Inline<Handle<S>>) -> ValueHandle
where
    S: BlobEncoding + 'static,
    Handle<S>: InlineEncoding,
{
    ValueHandle::from_raw(handle.raw)
}

fn handle_from_value(value: ValueHandle) -> Inline<Handle<UnknownBlob>> {
    Inline::new(value.raw())
}

/// Sign one durable grow-only want assertion.
pub fn sign_want<S>(key: &SigningKey, handle: Inline<Handle<S>>) -> PinAssertion
where
    S: BlobEncoding + 'static,
    Handle<S>: InlineEncoding,
{
    PinAssertion::sign(
        key,
        WantPinDescriptor::pin_handle(),
        value_from_handle(handle),
        canonical_label(),
    )
}

/// Project one trusted author's exact wanted-handle set from a coherent
/// generic snapshot.
///
/// Values are deduplicated as a set even if the author signed redundant records
/// carrying different opaque labels. Other authors and other pin kinds remain
/// present in the generic snapshot but do not enter this view.
pub fn wants_in_snapshot(
    snapshot: &PinAssertionSnapshot,
    author: VerifyingKey,
) -> BTreeSet<Inline<Handle<UnknownBlob>>> {
    snapshot
        .for_pin(&WantPinDescriptor::pin_identity(author))
        .into_iter()
        .map(|assertion| handle_from_value(assertion.value()))
        .collect()
}

/// Project the union of every author's wanted handles from a coherent generic
/// snapshot.
///
/// This is the storage-policy view used by garbage collection: any authentic
/// want retains the named blob regardless of which author asserted it. Normal
/// consumers should use [`wants_in_snapshot`] or [`WantStore::wants`] instead,
/// so one principal never mistakes another principal's demand for its own.
pub fn all_wants_in_snapshot(
    snapshot: &PinAssertionSnapshot,
) -> BTreeSet<Inline<Handle<UnknownBlob>>> {
    let pin = WantPinDescriptor::pin_handle();
    snapshot
        .iter()
        .filter(|assertion| assertion.identity().pin() == pin)
        .map(|assertion| handle_from_value(assertion.value()))
        .collect()
}

/// Author-scoped durable operations over the generic asserted-want G-set.
///
/// An implementation owns or otherwise has a configured signing identity.
/// Both methods are scoped to that one author, so callers cannot accidentally
/// switch principals by passing a key per operation. Use
/// [`all_wants_in_snapshot`] only for global storage policy such as GC.
pub trait WantStore: PinAssertionStore {
    /// Durably assert one wanted handle as the configured author. Duplicate
    /// assertion is success.
    fn assert_want<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::Error>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding;

    /// Read the configured author's exact wanted-handle set from one coherent
    /// snapshot.
    fn wants(&mut self) -> Result<BTreeSet<Inline<Handle<UnknownBlob>>>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;
    use crate::blob::encodings::rawbytes::RawBytes;

    fn handle(byte: u8) -> Inline<Handle<RawBytes>> {
        Inline::new([byte; 32])
    }

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn fixed_descriptor_is_canonical_content() {
        let blob = WantPinDescriptor::blob();
        assert_eq!(blob.bytes.as_ref(), WANT_PIN_DESCRIPTOR_V1);
        assert_eq!(blob.get_handle().raw, WantPinDescriptor::pin_handle().raw());
        assert_eq!(
            blob.try_from_blob::<WantPinDescriptor>().unwrap(),
            WantPinDescriptor
        );
    }

    #[test]
    fn one_author_projects_an_exact_grow_only_set() {
        let author = key(1);
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(sign_want(&author, handle(3))).unwrap();
        snapshot.insert(sign_want(&author, handle(2))).unwrap();
        snapshot.insert(sign_want(&author, handle(3))).unwrap();

        let wants = wants_in_snapshot(&snapshot, author.verifying_key());
        assert_eq!(
            wants,
            BTreeSet::from([
                handle_from_value(value_from_handle(handle(2))),
                handle_from_value(value_from_handle(handle(3))),
            ])
        );
        assert_eq!(snapshot.len(), 2, "duplicate signing is idempotent");
    }

    #[test]
    fn authors_are_distinct_but_consumers_may_union_their_values() {
        let first = key(1);
        let second = key(2);
        let shared = handle(9);
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(sign_want(&first, shared)).unwrap();
        snapshot.insert(sign_want(&second, shared)).unwrap();

        assert_eq!(snapshot.len(), 2);
        let first_wants = wants_in_snapshot(&snapshot, first.verifying_key());
        let second_wants = wants_in_snapshot(&snapshot, second.verifying_key());
        assert_eq!(first_wants, second_wants);
        assert_eq!(first_wants.len(), 1);
        assert_eq!(all_wants_in_snapshot(&snapshot), first_wants);
    }

    #[test]
    fn the_view_never_interprets_or_compares_labels() {
        let author = key(4);
        let wanted = handle(7);
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(sign_want(&author, wanted)).unwrap();
        snapshot
            .insert(PinAssertion::sign(
                &author,
                WantPinDescriptor::pin_handle(),
                value_from_handle(wanted),
                SubsumptionLabel::from_raw([0xFF; 32]),
            ))
            .unwrap();

        let wants = wants_in_snapshot(&snapshot, author.verifying_key());
        assert_eq!(wants.len(), 1);
        assert!(wants.contains(&handle_from_value(value_from_handle(wanted))));
    }

    struct MemoryPins {
        key: SigningKey,
        assertions: PinAssertionSnapshot,
    }

    impl MemoryPins {
        fn new(key: SigningKey) -> Self {
            Self {
                key,
                assertions: PinAssertionSnapshot::new(),
            }
        }
    }

    impl PinAssertionStore for MemoryPins {
        type Error = Infallible;

        fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
            Ok(self.assertions.clone())
        }

        fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
            self.assertions
                .insert(assertion)
                .expect("cryptographic key collision in test");
            Ok(())
        }
    }

    impl WantStore for MemoryPins {
        fn assert_want<S>(&mut self, handle: Inline<Handle<S>>) -> Result<(), Self::Error>
        where
            S: BlobEncoding + 'static,
            Handle<S>: InlineEncoding,
        {
            let assertion = sign_want(&self.key, handle);
            self.append_pin_assertion(assertion)
        }

        fn wants(&mut self) -> Result<BTreeSet<Inline<Handle<UnknownBlob>>>, Self::Error> {
            let author = self.key.verifying_key();
            self.pin_assertion_snapshot()
                .map(|snapshot| wants_in_snapshot(&snapshot, author))
        }
    }

    #[test]
    fn authored_store_is_append_only_and_author_scoped() {
        let first = key(1);
        let second = key(2);
        let mut store = MemoryPins::new(first);
        store.assert_want(handle(5)).unwrap();
        store.assert_want(handle(6)).unwrap();
        store
            .append_pin_assertion(sign_want(&second, handle(7)))
            .unwrap();
        store
            .append_pin_assertion(PinAssertion::sign(
                &second,
                PinHandle::from_raw([99; 32]),
                value_from_handle(handle(8)),
                SubsumptionLabel::from_raw([0; 32]),
            ))
            .unwrap();

        assert_eq!(store.wants().unwrap().len(), 2);
        assert_eq!(
            all_wants_in_snapshot(&store.pin_assertion_snapshot().unwrap()).len(),
            3,
            "global GC view unions authors but ignores other pin kinds"
        );
    }
}
