//! The generic asserted-pin envelope: one signed record shape for every pin
//! kind.
//!
//! A pin is the primitive and a branch is one pin kind, not a separate
//! mechanism. There is no local state — a copied pile is a replica and `cat` is
//! synchronisation — so no persisted mutable cell is exempt, and this record
//! replaces both halves of the old cell: compare-and-swap writes and
//! tombstone/unpin deletes.
//!
//! The canonical encoding is exactly 192 bytes, four uniform 32-byte slots plus
//! the signature:
//!
//! ```text
//! author key [32] | pin handle [32] | value handle [32] | label [32] | signature [64]
//! ```
//!
//! # The label contract
//!
//! The label is **opaque to this layer and compared bytewise**. The store
//! `memcmp`s it and never learns what it means, which is what keeps replay
//! kind-agnostic: a peer must preserve and merge a kind it cannot interpret.
//!
//! Exactly one inference is sound:
//!
//! > `label(A) >= label(B)` proves **A is not an ancestor of B**.
//!
//! It holds only when the issuing kind's encoding is strictly increasing along
//! causality *under bytewise order*. It buys skipped ancestry traversals, and a
//! traversal skipped is a fetch avoided — which is the whole point, since
//! ancestry needs the chain but a label is already in the record.
//!
//! The converse is **not** sound. Label order never proves subsumption and must
//! never drop a claim: two divergent commits can share a depth, and a deeper
//! branch does not subsume a shallower divergent one. A kind whose label is not
//! provably monotone (wall-clock expiry, replica-local generation counters)
//! gets *zero* skips rather than fewer — degraded, never wrong.
//!
//! Numeric labels must be **big-endian**, or byte order will not agree with
//! numeric order and the encoding will silently stop being monotone in the
//! unsound direction.

use std::error::Error;
use std::fmt;

use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use crate::inline::encodings::hash::Blake3;

/// Number of semantic bytes in a canonical asserted-pin record.
pub const PIN_ASSERTION_LEN: usize = 192;

const AUTHOR_RANGE: std::ops::Range<usize> = 0..32;
const PIN_RANGE: std::ops::Range<usize> = 32..64;
const VALUE_RANGE: std::ops::Range<usize> = 64..96;
const LABEL_RANGE: std::ops::Range<usize> = 96..128;
const SIGNATURE_RANGE: std::ops::Range<usize> = 128..192;

/// Domain separator for the bytes signed by a V1 asserted-pin record.
/// Minted with `trible genid` on 2026-08-02.
const PIN_ASSERTION_V1_SIGNATURE_DOMAIN: [u8; 16] = hex!("D013E0A9C63928DA1431467F4A19C314");
const SIGNED_MESSAGE_LEN: usize = 16 + 32 + 32 + 32 + 32;

/// Index key: 32-byte identity digest, then the 32-byte assertion id.
///
/// The identity component is a full digest rather than a truncated id on
/// purpose — a 16-byte prefix used as if it were a selector is exactly the
/// confusion the branch layer is carrying today.
pub const PIN_INDEX_KEY_LEN: usize = 32 + 32;

/// Content handle of a pin's descriptor. Opaque here; the typed adapter knows
/// what it names. Its *content* may be absent locally without making the
/// identity malformed — the handle is the identity component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PinHandle([u8; 32]);

/// Content handle of a pin's asserted value. Opaque at this layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueHandle([u8; 32]);

/// Opaque 32-byte subsumption label, ordered bytewise.
///
/// `Ord` is the derived lexicographic order over the raw bytes, which is
/// exactly `memcmp`. Kinds encode into it; this layer never interprets it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SubsumptionLabel([u8; 32]);

impl SubsumptionLabel {
    /// A label carrying no ordering information. Every comparison ties, so a
    /// kind with no dominance relation (a grow-only want set, say) gets no
    /// skips and needs none.
    pub const NONE: Self = Self([0u8; 32]);

    /// Encode a depth-like counter: big-endian in the leading 8 bytes, zero
    /// tail. Big-endian is required — little-endian would order bytewise in a
    /// way that disagrees with numeric order, silently breaking monotonicity.
    pub const fn from_depth(depth: u64) -> Self {
        let mut raw = [0u8; 32];
        let be = depth.to_be_bytes();
        let mut i = 0;
        while i < 8 {
            raw[i] = be[i];
            i += 1;
        }
        Self(raw)
    }

    /// Raw bytes, for kinds that need a composite encoding.
    pub const fn from_raw(raw: [u8; 32]) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> [u8; 32] {
        self.0
    }

    /// Sound use of the label, and the only one.
    ///
    /// Returns true when `self >= other` proves the labelled assertion cannot
    /// be an ancestor of `other`'s, so the traversal — and any fetch it would
    /// have required — can be skipped. Callers must only invoke this for kinds
    /// whose encoding is proven causally monotone.
    pub fn proves_not_ancestor_of(self, other: Self) -> bool {
        self >= other
    }
}

macro_rules! raw_handle {
    ($t:ty) => {
        impl $t {
            pub const fn from_raw(raw: [u8; 32]) -> Self {
                Self(raw)
            }
            pub const fn raw(self) -> [u8; 32] {
                self.0
            }
        }
    };
}
raw_handle!(PinHandle);
raw_handle!(ValueHandle);

/// Blake3 content id of one canonical signed assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PinAssertionId([u8; 32]);

impl PinAssertionId {
    pub const fn raw(self) -> [u8; 32] {
        self.0
    }
}

/// The exact identity of one asserted pin: `(author key, pin handle)`.
///
/// Two authors asserting the same pin handle hold *different* registers, for
/// the same reason two authors' `main` are different branches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PinIdentity {
    author: [u8; 32],
    pin: PinHandle,
}

impl PinIdentity {
    pub fn new(author: VerifyingKey, pin: PinHandle) -> Self {
        Self {
            author: author.to_bytes(),
            pin,
        }
    }

    pub fn author(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.author)
            .expect("PinIdentity is constructible only from a checked key")
    }

    pub const fn pin(&self) -> PinHandle {
        self.pin
    }

    /// Full-width identity digest used as the index prefix. Not truncated.
    pub fn digest(&self) -> [u8; 32] {
        let mut buf = [0u8; 64];
        buf[..32].copy_from_slice(&self.author);
        buf[32..].copy_from_slice(&self.pin.0);
        Blake3::digest(&buf)
    }
}

/// A canonical asserted-pin record whose signature has already been verified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinAssertion {
    identity: PinIdentity,
    value: ValueHandle,
    label: SubsumptionLabel,
    signature: [u8; 64],
}

impl PinAssertion {
    /// Sign locally. The result is verified by construction.
    pub fn sign(
        key: &SigningKey,
        pin: PinHandle,
        value: ValueHandle,
        label: SubsumptionLabel,
    ) -> Self {
        let identity = PinIdentity::new(key.verifying_key(), pin);
        let message = signed_message(&identity.author, &pin.0, &value.0, &label.0);
        Self {
            identity,
            value,
            label,
            signature: key.sign(&message).to_bytes(),
        }
    }

    pub fn encode(&self) -> [u8; PIN_ASSERTION_LEN] {
        let mut bytes = [0u8; PIN_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author);
        bytes[PIN_RANGE].copy_from_slice(&self.identity.pin.0);
        bytes[VALUE_RANGE].copy_from_slice(&self.value.0);
        bytes[LABEL_RANGE].copy_from_slice(&self.label.0);
        bytes[SIGNATURE_RANGE].copy_from_slice(&self.signature);
        bytes
    }

    pub const fn identity(&self) -> &PinIdentity {
        &self.identity
    }
    pub const fn value(&self) -> ValueHandle {
        self.value
    }
    pub const fn label(&self) -> SubsumptionLabel {
        self.label
    }

    pub fn id(&self) -> PinAssertionId {
        PinAssertionId(Blake3::digest(&self.encode()))
    }

    pub fn index_key(&self) -> [u8; PIN_INDEX_KEY_LEN] {
        let mut key = [0u8; PIN_INDEX_KEY_LEN];
        key[..32].copy_from_slice(&self.identity.digest());
        key[32..].copy_from_slice(&self.id().0);
        key
    }
}

/// Structurally decoded bytes whose signature has not been checked.
///
/// Every semantic accessor is named `claimed_*`: the fields must be readable to
/// build an optimistic frontier at all, so safety lives in the naming and in
/// [`PinAssertion`] remaining the only verified currency — not in withholding
/// the accessors, which would make the demand-driven design unimplementable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnverifiedPinAssertion {
    identity: PinIdentity,
    value: ValueHandle,
    label: SubsumptionLabel,
    signature: [u8; 64],
}

impl UnverifiedPinAssertion {
    /// Decode without Ed25519 verification. A malformed author key stays a
    /// *structural* error: bad framing means later offsets are untrustworthy,
    /// whereas a bad signature is a datum this layer may carry.
    pub fn decode_structural(
        bytes: [u8; PIN_ASSERTION_LEN],
    ) -> Result<Self, PinAssertionError> {
        let author_bytes: [u8; 32] = bytes[AUTHOR_RANGE].try_into().unwrap();
        let author = VerifyingKey::from_bytes(&author_bytes)
            .map_err(|_| PinAssertionError::InvalidAuthorKey)?;
        Ok(Self {
            identity: PinIdentity::new(
                author,
                PinHandle(bytes[PIN_RANGE].try_into().unwrap()),
            ),
            value: ValueHandle(bytes[VALUE_RANGE].try_into().unwrap()),
            label: SubsumptionLabel(bytes[LABEL_RANGE].try_into().unwrap()),
            signature: bytes[SIGNATURE_RANGE].try_into().unwrap(),
        })
    }

    pub fn claimed_identity(self) -> PinIdentity {
        self.identity
    }
    pub fn claimed_value(self) -> ValueHandle {
        self.value
    }
    /// The label is claimed like everything else, but a *lying* label only
    /// harms its own author: an inflated one skips checks and leaves a
    /// dominated assertion in the author's own frontier (spurious divergence),
    /// and a deflated one merely costs traversals. It is inside the signature,
    /// so nobody else can set it.
    pub fn claimed_label(self) -> SubsumptionLabel {
        self.label
    }

    pub fn encode(self) -> [u8; PIN_ASSERTION_LEN] {
        let mut bytes = [0u8; PIN_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author);
        bytes[PIN_RANGE].copy_from_slice(&self.identity.pin.0);
        bytes[VALUE_RANGE].copy_from_slice(&self.value.0);
        bytes[LABEL_RANGE].copy_from_slice(&self.label.0);
        bytes[SIGNATURE_RANGE].copy_from_slice(&self.signature);
        bytes
    }

    pub fn id(self) -> PinAssertionId {
        PinAssertionId(Blake3::digest(&self.encode()))
    }

    pub fn verify_strict(self) -> Result<PinAssertion, PinAssertionError> {
        let author = self.identity.author();
        let signature = Signature::from_bytes(&self.signature);
        let message = signed_message(
            &self.identity.author,
            &self.identity.pin.0,
            &self.value.0,
            &self.label.0,
        );
        author
            .verify_strict(&message, &signature)
            .map_err(|_| PinAssertionError::InvalidSignature)?;
        Ok(PinAssertion {
            identity: self.identity,
            value: self.value,
            label: self.label,
            signature: self.signature,
        })
    }
}

impl From<PinAssertion> for UnverifiedPinAssertion {
    fn from(a: PinAssertion) -> Self {
        Self {
            identity: a.identity,
            value: a.value,
            label: a.label,
            signature: a.signature,
        }
    }
}

fn signed_message(
    author: &[u8; 32],
    pin: &[u8; 32],
    value: &[u8; 32],
    label: &[u8; 32],
) -> [u8; SIGNED_MESSAGE_LEN] {
    let mut m = [0u8; SIGNED_MESSAGE_LEN];
    m[..16].copy_from_slice(&PIN_ASSERTION_V1_SIGNATURE_DOMAIN);
    m[16..48].copy_from_slice(author);
    m[48..80].copy_from_slice(pin);
    m[80..112].copy_from_slice(value);
    m[112..144].copy_from_slice(label);
    m
}

/// Why raw bytes were not admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinAssertionError {
    /// The claimed Ed25519 public key is not a valid verifying key.
    InvalidAuthorKey,
    /// The signature does not strictly authenticate the record under the V1
    /// domain.
    InvalidSignature,
}

impl fmt::Display for PinAssertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAuthorKey => write!(f, "pin assertion has an invalid author key"),
            Self::InvalidSignature => write!(f, "pin assertion signature is invalid"),
        }
    }
}

impl Error for PinAssertionError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> SigningKey {
        SigningKey::from_bytes(&[b; 32])
    }
    fn pin(b: u8) -> PinHandle {
        PinHandle([b; 32])
    }
    fn val(b: u8) -> ValueHandle {
        ValueHandle([b; 32])
    }

    #[test]
    fn record_is_192_bytes_and_fits_a_256_byte_v3_record() {
        assert_eq!(PIN_ASSERTION_LEN, 32 + 32 + 32 + 32 + 64);
        assert_eq!(SIGNED_MESSAGE_LEN, 16 + 32 + 32 + 32 + 32);
        // marker + semantic must leave room for zeroed reserve.
        assert!(16 + PIN_ASSERTION_LEN <= 256);
        assert_eq!(256 - 16 - PIN_ASSERTION_LEN, 48);
    }

    #[test]
    fn every_one_of_the_192_bytes_is_authenticated_including_the_label() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), SubsumptionLabel::from_depth(3));
        let encoded = a.encode();
        assert_eq!(
            UnverifiedPinAssertion::decode_structural(encoded)
                .unwrap()
                .verify_strict()
                .unwrap(),
            a
        );
        for i in 0..PIN_ASSERTION_LEN {
            let mut bad = encoded;
            bad[i] ^= 1;
            let rejected = match UnverifiedPinAssertion::decode_structural(bad) {
                Err(_) => true,
                Ok(u) => u.verify_strict().is_err(),
            };
            assert!(rejected, "byte {i} was not authenticated");
        }
    }

    #[test]
    fn label_is_inside_the_signature_so_only_its_author_can_inflate_it() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), SubsumptionLabel::from_depth(1));
        let mut forged = a.encode();
        forged[LABEL_RANGE].copy_from_slice(&SubsumptionLabel::from_depth(9999).raw());
        assert!(UnverifiedPinAssertion::decode_structural(forged)
            .unwrap()
            .verify_strict()
            .is_err());
    }

    #[test]
    fn big_endian_depth_is_monotone_under_bytewise_order() {
        let mut prev = SubsumptionLabel::from_depth(0);
        for d in [1u64, 2, 255, 256, 65_535, 65_536, 1 << 40, u64::MAX] {
            let cur = SubsumptionLabel::from_depth(d);
            assert!(cur > prev, "depth {d} did not increase bytewise");
            prev = cur;
        }
    }

    /// NEGATIVE CONTROL. A little-endian encoding still produces a total order,
    /// so a positive-only test would pass — but the order disagrees with
    /// numeric order, which silently breaks monotonicity in the UNSOUND
    /// direction (it licenses skips that are not justified). This test exists
    /// to fail if anyone "simplifies" `from_depth` to native/little-endian.
    #[test]
    fn little_endian_depth_would_not_be_monotone() {
        let le = |d: u64| {
            let mut raw = [0u8; 32];
            raw[..8].copy_from_slice(&d.to_le_bytes());
            SubsumptionLabel(raw)
        };
        assert!(le(256) < le(1), "little-endian must misorder 1 vs 256");
        assert!(
            SubsumptionLabel::from_depth(256) > SubsumptionLabel::from_depth(1),
            "big-endian must order 1 < 256"
        );
    }

    #[test]
    fn label_proves_non_ancestry_only_and_none_ties_every_comparison() {
        let deep = SubsumptionLabel::from_depth(9);
        let shallow = SubsumptionLabel::from_depth(2);
        assert!(deep.proves_not_ancestor_of(shallow));
        assert!(!shallow.proves_not_ancestor_of(deep));
        // A kind with no dominance relation ties everywhere: no skips, and none
        // are needed.
        assert!(SubsumptionLabel::NONE.proves_not_ancestor_of(SubsumptionLabel::NONE));
    }

    #[test]
    fn identity_uses_both_fields_and_its_digest_is_full_width() {
        let a = PinIdentity::new(key(3).verifying_key(), pin(1));
        assert_eq!(a, PinIdentity::new(key(3).verifying_key(), pin(1)));
        assert_ne!(a, PinIdentity::new(key(3).verifying_key(), pin(2)));
        assert_ne!(a, PinIdentity::new(key(4).verifying_key(), pin(1)));
        assert_eq!(a.digest().len(), 32);
        assert_ne!(
            a.digest(),
            PinIdentity::new(key(3).verifying_key(), pin(2)).digest()
        );
    }

    #[test]
    fn two_authors_asserting_one_pin_are_distinct_registers() {
        let p = pin(5);
        let x = PinAssertion::sign(&key(1), p, val(9), SubsumptionLabel::from_depth(1));
        let y = PinAssertion::sign(&key(2), p, val(9), SubsumptionLabel::from_depth(1));
        assert_ne!(x.identity().digest(), y.identity().digest());
        assert_ne!(x.index_key(), y.index_key());
    }

    #[test]
    fn duplicate_signing_is_byte_identical_so_append_is_idempotent() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), SubsumptionLabel::from_depth(4));
        let b = PinAssertion::sign(&key(7), pin(11), val(19), SubsumptionLabel::from_depth(4));
        assert_eq!(a.encode(), b.encode());
        assert_eq!(a.id(), b.id());
        assert_eq!(a.index_key(), b.index_key());
    }
}
