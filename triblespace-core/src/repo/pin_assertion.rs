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
//! Exactly one inference is sound, and **this type deliberately does not offer
//! it**:
//!
//! > For a kind whose encoding is strictly increasing along causality *under
//! > bytewise order*, `label(A) >= label(B)` proves **A is not a strict
//! > ancestor of B**, so that traversal — and any fetch it required — may be
//! > skipped.
//!
//! The proof obligation belongs to the typed resolver that owns the encoding,
//! not to the label. A method here would be a false affordance: the bytes alone
//! prove nothing, and any kind could call it. So this type exposes only opaque
//! storage and bytewise [`Ord`]; a resolver that can discharge monotonicity
//! compares labels itself, and one that cannot simply never does.
//!
//! Two traps the boundary exists to prevent. A **constant** label ties every
//! comparison, and under `>=` a tie *licenses a skip* — so a "neutral" sentinel
//! would silently grant skips on every pair rather than none. And equal labels
//! rule out strict ancestry only between **distinct** values, so identical
//! values must be grouped before any comparison.
//!
//! The converse is never sound. Label order does not prove subsumption and must
//! never drop a claim: two divergent commits can share a depth, and a deeper
//! branch does not subsume a shallower divergent one. A kind whose label is not
//! provably monotone (wall-clock expiry, replica-local generation counters)
//! gets *zero* skips rather than fewer — degraded, never wrong.
//!
//! One obligation falls on every *kind* that encodes a number: it must be
//! **big-endian**, or byte order will not agree with numeric order and the
//! encoding will silently stop being monotone in the unsound direction. This
//! module ships no encoder, precisely so no label model is canonised here; see
//! [`super::branch_pin`] for the branch kind's depth codec and the negative
//! control that pins the endianness requirement.

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
/// Deliberately **not** `Default`: an implicit all-zero value is the same
/// sentinel that was removed as `NONE`, reintroduced somewhere harder to see.
/// Every label is chosen explicitly via [`SubsumptionLabel::from_raw`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubsumptionLabel([u8; 32]);

impl SubsumptionLabel {
    /// Raw bytes. Every encoding decision belongs to the kind; this layer
    /// deliberately ships none, so no particular label model (depth, counter,
    /// clock) is canonised by the generic API.
    pub const fn from_raw(raw: [u8; 32]) -> Self {
        Self(raw)
    }

    pub const fn raw(self) -> [u8; 32] {
        self.0
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
    /// The label is claimed like everything else.
    ///
    /// A dishonest or malformed label may only ever **over-approximate the
    /// frontier**; it can never drop a claim, because labels suppress exact
    /// checks and nothing else. There is no direction-specific promise here:
    /// inflating an ancestor and deflating its descendant are the same event
    /// seen from either end — both make `label(ancestor) >= label(descendant)`
    /// hold, skipping the walk that would have found the domination, so the
    /// ancestor wrongly survives as spurious divergence. Only the *relative*
    /// order matters.
    ///
    /// The label is inside the signature, so nobody but the author can set it,
    /// and the damage is confined to that author's own register.
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
    /// Opaque here on purpose: the generic layer has no label semantics.
    fn label(b: u8) -> SubsumptionLabel {
        SubsumptionLabel::from_raw([b; 32])
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
        let a = PinAssertion::sign(&key(7), pin(11), val(19), label(3));
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
    fn label_is_inside_the_signature_so_only_its_author_can_alter_it() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), label(1));
        let mut forged = a.encode();
        forged[LABEL_RANGE].copy_from_slice(&label(9).raw());
        assert!(UnverifiedPinAssertion::decode_structural(forged)
            .unwrap()
            .verify_strict()
            .is_err());
    }

    /// The label exposes ordering and nothing else. A resolver that has proven
    /// monotonicity for its own encoding reads `a >= b` as "a is not a strict
    /// ancestor of b"; this type neither performs nor sanctions that step.
    #[test]
    fn label_exposes_only_bytewise_order() {
        assert!(label(9) > label(2));
        assert!(!(label(2) > label(9)));
        assert_eq!(label(9).raw(), [9u8; 32]);
    }

    /// A constant label ties every comparison, and a tie satisfies `>=`. Had
    /// the type shipped a "neutral" sentinel with a `proves_not_ancestor_of`
    /// helper, a non-monotone kind would have been granted skips on EVERY pair
    /// while appearing to opt out of skipping entirely. Caught by liora-gpt.
    #[test]
    fn a_constant_label_ties_and_a_tie_would_license_a_skip() {
        let flat = SubsumptionLabel::from_raw([0u8; 32]);
        assert!(flat >= flat, "ties satisfy >=, which is why no sentinel exists");
        // Equal labels rule out STRICT ancestry only between distinct values,
        // so identical values must be grouped before any comparison is made.
        let a = PinAssertion::sign(&key(7), pin(1), val(2), flat);
        let b = PinAssertion::sign(&key(7), pin(1), val(2), flat);
        assert_eq!(a.id(), b.id(), "identical values dedupe rather than compare");
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
        let x = PinAssertion::sign(&key(1), p, val(9), label(1));
        let y = PinAssertion::sign(&key(2), p, val(9), label(1));
        assert_ne!(x.identity().digest(), y.identity().digest());
        assert_ne!(x.index_key(), y.index_key());
    }

    #[test]
    fn duplicate_signing_is_byte_identical_so_append_is_idempotent() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), label(4));
        let b = PinAssertion::sign(&key(7), pin(11), val(19), label(4));
        assert_eq!(a.encode(), b.encode());
        assert_eq!(a.id(), b.id());
        assert_eq!(a.index_key(), b.index_key());
    }
}
