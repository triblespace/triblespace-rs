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
use std::sync::OnceLock;

#[cfg(test)]
use std::cell::Cell;

use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use crate::inline::encodings::hash::Blake3;
use crate::patch::{Entry, IdentitySchema, PATCH};

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

    /// Decode the fixed semantic bytes and verify their signature strictly.
    ///
    /// Persisted replay has a separate crate-private structural path so it can
    /// defer public-key work. Raw bytes entering through the public API become
    /// usable claims only through this verified constructor.
    pub fn decode_verified(bytes: [u8; PIN_ASSERTION_LEN]) -> Result<Self, PinAssertionError> {
        UnverifiedPinAssertion::decode_structural(bytes)?.verify_strict()
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
pub(crate) struct UnverifiedPinAssertion {
    identity: PinIdentity,
    value: ValueHandle,
    label: SubsumptionLabel,
    signature: [u8; 64],
}

impl UnverifiedPinAssertion {
    /// Decode without Ed25519 verification. A malformed author key stays a
    /// *structural* error: bad framing means later offsets are untrustworthy,
    /// whereas a bad signature is a datum this layer may carry.
    pub(crate) fn decode_structural(
        bytes: [u8; PIN_ASSERTION_LEN],
    ) -> Result<Self, PinAssertionError> {
        let author_bytes: [u8; 32] = bytes[AUTHOR_RANGE].try_into().unwrap();
        let author = VerifyingKey::from_bytes(&author_bytes)
            .map_err(|_| PinAssertionError::InvalidAuthorKey)?;
        Ok(Self {
            identity: PinIdentity::new(author, PinHandle(bytes[PIN_RANGE].try_into().unwrap())),
            value: ValueHandle(bytes[VALUE_RANGE].try_into().unwrap()),
            label: SubsumptionLabel(bytes[LABEL_RANGE].try_into().unwrap()),
            signature: bytes[SIGNATURE_RANGE].try_into().unwrap(),
        })
    }

    pub(crate) fn claimed_identity(self) -> PinIdentity {
        self.identity
    }
    pub(crate) fn claimed_value(self) -> ValueHandle {
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
    pub(crate) fn claimed_label(self) -> SubsumptionLabel {
        self.label
    }

    pub(crate) fn encode(self) -> [u8; PIN_ASSERTION_LEN] {
        let mut bytes = [0u8; PIN_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author);
        bytes[PIN_RANGE].copy_from_slice(&self.identity.pin.0);
        bytes[VALUE_RANGE].copy_from_slice(&self.value.0);
        bytes[LABEL_RANGE].copy_from_slice(&self.label.0);
        bytes[SIGNATURE_RANGE].copy_from_slice(&self.signature);
        bytes
    }

    pub(crate) fn id(self) -> PinAssertionId {
        PinAssertionId(Blake3::digest(&self.encode()))
    }

    pub(crate) fn verify_strict(self) -> Result<PinAssertion, PinAssertionError> {
        #[cfg(test)]
        SIGNATURE_VERIFICATIONS.with(|count| count.set(count.get() + 1));

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

#[cfg(test)]
thread_local! {
    static SIGNATURE_VERIFICATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_signature_verification_count() {
    SIGNATURE_VERIFICATIONS.with(|count| count.set(0));
}

#[cfg(test)]
fn signature_verification_count() -> usize {
    SIGNATURE_VERIFICATIONS.with(Cell::get)
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

/// A practically impossible mismatch between a 64-byte snapshot key and its
/// exact canonical witness.
///
/// PATCH equality and hashing deliberately concern keys only, so the wrapper
/// still compares values before accepting a duplicate or union. The identity
/// prefix is a full Blake3 digest and the suffix is the assertion's full
/// Blake3 content id; this error therefore represents a cryptographic
/// collision rather than an ordinary conflict.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinAssertionKeyCollision;

impl fmt::Display for PinAssertionKeyCollision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "two different pin assertions have the same snapshot key")
    }
}

impl Error for PinAssertionKeyCollision {}

/// Opaque grow-only snapshot of asserted-pin witnesses.
///
/// The inner PATCH is private so callers cannot forge a mismatched key, remove
/// a claim, or introduce arrival-order semantics. Replay may insert a
/// structurally decoded witness without checking its signature; public
/// iteration and typed views verify lazily and memoize both success and
/// failure. Descriptor and value content remain opaque and need not be present
/// locally.
#[derive(Clone, Debug, Default)]
pub struct PinAssertionSnapshot {
    assertions: PATCH<PIN_INDEX_KEY_LEN, IdentitySchema, PinAssertionWitness>,
}

/// One structurally valid persisted witness and its memoized semantic result.
///
/// This remains crate-private: only a verified [`PinAssertion`] may cross the
/// public assertion boundary. Typed resolvers may inspect claimed fields while
/// building an optimistic view, but must authenticate every surviving claim
/// before exposing it.
#[derive(Clone, Debug)]
pub(crate) struct PinAssertionWitness {
    unverified: UnverifiedPinAssertion,
    verified: OnceLock<Result<Box<PinAssertion>, PinAssertionError>>,
}

impl PartialEq for PinAssertionWitness {
    fn eq(&self, other: &Self) -> bool {
        self.unverified == other.unverified
    }
}

impl Eq for PinAssertionWitness {}

impl PinAssertionWitness {
    fn from_verified(assertion: PinAssertion) -> Self {
        Self {
            unverified: assertion.into(),
            verified: OnceLock::from(Ok(Box::new(assertion))),
        }
    }

    fn from_unverified(unverified: UnverifiedPinAssertion) -> Self {
        Self {
            unverified,
            verified: OnceLock::new(),
        }
    }

    pub(crate) fn id(&self) -> PinAssertionId {
        self.unverified.id()
    }

    pub(crate) fn claimed_value(&self) -> ValueHandle {
        self.unverified.claimed_value()
    }

    pub(crate) fn claimed_label(&self) -> SubsumptionLabel {
        self.unverified.claimed_label()
    }

    pub(crate) fn verified(&self) -> Result<&PinAssertion, PinAssertionError> {
        match self
            .verified
            .get_or_init(|| self.unverified.verify_strict().map(Box::new))
        {
            Ok(assertion) => Ok(assertion.as_ref()),
            Err(error) => Err(*error),
        }
    }
}

impl PartialEq for PinAssertionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        if self.assertions.len() != other.assertions.len() {
            return false;
        }
        self.assertions
            .iter_ordered()
            .all(|key| self.assertions.get(key) == other.assertions.get(key))
    }
}

impl Eq for PinAssertionSnapshot {}

impl PinAssertionSnapshot {
    /// Create an empty asserted-pin snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert one verified assertion. Re-insertion is an idempotent success.
    pub fn insert(&mut self, assertion: PinAssertion) -> Result<(), PinAssertionKeyCollision> {
        self.insert_witness(PinAssertionWitness::from_verified(assertion))
    }

    /// Insert one structurally decoded persisted witness without verifying its
    /// signature. This path is reserved for exact replay.
    pub(crate) fn insert_unverified(
        &mut self,
        assertion: UnverifiedPinAssertion,
    ) -> Result<(), PinAssertionKeyCollision> {
        self.insert_witness(PinAssertionWitness::from_unverified(assertion))
    }

    fn insert_witness(
        &mut self,
        witness: PinAssertionWitness,
    ) -> Result<(), PinAssertionKeyCollision> {
        let key = witness_index_key(&witness.unverified);
        if let Some(existing) = self.assertions.get(&key) {
            if existing != &witness {
                return Err(PinAssertionKeyCollision);
            }
            return Ok(());
        }
        self.assertions.insert(&Entry::with_value(&key, witness));
        Ok(())
    }

    /// Check exact membership while still surfacing an index-key collision.
    pub(crate) fn contains(
        &self,
        assertion: &PinAssertion,
    ) -> Result<bool, PinAssertionKeyCollision> {
        let witness = PinAssertionWitness::from_verified(*assertion);
        match self.assertions.get(&assertion.index_key()) {
            Some(existing) if existing == &witness => Ok(true),
            Some(_) => Err(PinAssertionKeyCollision),
            None => Ok(false),
        }
    }

    /// Union another grow-only snapshot into this one.
    pub fn union(&mut self, other: Self) -> Result<(), PinAssertionKeyCollision> {
        for key in other.assertions.iter_ordered() {
            let witness = other
                .assertions
                .get(key)
                .expect("a key yielded by PATCH resolves in the same snapshot");
            if let Some(existing) = self.assertions.get(key) {
                if existing != witness {
                    return Err(PinAssertionKeyCollision);
                }
            }
        }
        self.assertions.union(other.assertions);
        Ok(())
    }

    /// Number of distinct canonical assertion witnesses.
    pub fn len(&self) -> usize {
        self.assertions.len() as usize
    }

    /// Whether this snapshot contains no witnesses.
    pub fn is_empty(&self) -> bool {
        self.assertions.is_empty()
    }

    /// Iterate every valid assertion in canonical snapshot-key order.
    ///
    /// Invalid signatures remain stored but are omitted from the public view.
    /// Each verification result is memoized in its witness.
    pub fn iter(&self) -> impl Iterator<Item = &PinAssertion> {
        self.assertions.iter_ordered().filter_map(|key| {
            let witness = self
                .assertions
                .get(key)
                .expect("a key yielded by PATCH resolves in the same snapshot");
            witness.verified().ok()
        })
    }

    /// Return every valid assertion for one exact `(author, descriptor)` pair.
    ///
    /// The full identity digest narrows the prefix scan. The complete identity
    /// is then compared so even a forced digest collision cannot merge pins.
    pub fn for_pin(&self, identity: &PinIdentity) -> Vec<PinAssertion> {
        self.witnesses_for_pin(identity)
            .into_iter()
            .filter_map(|witness| witness.verified().ok().copied())
            .collect()
    }

    /// Return structural witnesses for one exact identity. Typed resolvers use
    /// this to build an optimistic view and authenticate only surviving claims.
    pub(crate) fn witnesses_for_pin(&self, identity: &PinIdentity) -> Vec<&PinAssertionWitness> {
        let digest = identity.digest();
        let mut assertions = Vec::new();
        self.assertions
            .infixes::<32, 32, _>(&digest, |assertion_id| {
                let mut key = [0u8; PIN_INDEX_KEY_LEN];
                key[..32].copy_from_slice(&digest);
                key[32..].copy_from_slice(assertion_id);
                let assertion = self
                    .assertions
                    .get(&key)
                    .expect("a suffix yielded by PATCH resolves in the same snapshot");
                if assertion.unverified.claimed_identity() == *identity {
                    assertions.push(assertion);
                }
            });
        assertions
    }

    #[cfg(test)]
    fn insert_with_identity_digest_for_test(&mut self, digest: [u8; 32], assertion: PinAssertion) {
        let witness = PinAssertionWitness::from_verified(assertion);
        let mut key = [0u8; PIN_INDEX_KEY_LEN];
        key[..32].copy_from_slice(&digest);
        key[32..].copy_from_slice(&assertion.id().raw());
        self.assertions.insert(&Entry::with_value(&key, witness));
    }
}

fn witness_index_key(assertion: &UnverifiedPinAssertion) -> [u8; PIN_INDEX_KEY_LEN] {
    let mut key = [0u8; PIN_INDEX_KEY_LEN];
    key[..32].copy_from_slice(&assertion.claimed_identity().digest());
    key[32..].copy_from_slice(&assertion.id().raw());
    key
}

/// Storage surface for the shared grow-only asserted-pin layer.
///
/// Duplicate append is success. There is deliberately no update, delete,
/// tombstone, compare-and-swap, scalar-head, or kind-specific operation.
/// Implementations preserve every accepted assertion even when its descriptor
/// is absent or its kind is unknown.
///
/// Signature verification identifies an author; it is not authorization. A
/// replication ingest boundary must restrict accepted authors and pin kinds
/// before calling [`Self::append_pin_assertion`], or an attacker can consume
/// unbounded durable storage with perfectly valid signatures. Overload must be
/// an explicit refusal—never silent eviction of an assertion already accepted.
pub trait PinAssertionStore {
    /// Storage or validation error.
    type Error: Error + fmt::Debug + Send + Sync + 'static;

    /// Return one coherent snapshot of all persisted assertion witnesses.
    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error>;

    /// Durably append one verified assertion. Duplicate append is success.
    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error>;
}

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

    fn snapshot(assertions: &[PinAssertion]) -> PinAssertionSnapshot {
        let mut snapshot = PinAssertionSnapshot::new();
        for assertion in assertions {
            snapshot.insert(*assertion).unwrap();
        }
        snapshot
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
        assert_eq!(PinAssertion::decode_verified(encoded).unwrap(), a);
        for i in 0..PIN_ASSERTION_LEN {
            let mut bad = encoded;
            bad[i] ^= 1;
            assert!(
                PinAssertion::decode_verified(bad).is_err(),
                "byte {i} was not authenticated"
            );
        }
    }

    #[test]
    fn label_is_inside_the_signature_so_only_its_author_can_alter_it() {
        let a = PinAssertion::sign(&key(7), pin(11), val(19), label(1));
        let mut forged = a.encode();
        forged[LABEL_RANGE].copy_from_slice(&label(9).raw());
        assert!(PinAssertion::decode_verified(forged).is_err());
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
        assert!(
            flat >= flat,
            "ties satisfy >=, which is why no sentinel exists"
        );
        // Equal labels rule out STRICT ancestry only between distinct values,
        // so identical values must be grouped before any comparison is made.
        let a = PinAssertion::sign(&key(7), pin(1), val(2), flat);
        let b = PinAssertion::sign(&key(7), pin(1), val(2), flat);
        assert_eq!(
            a.id(),
            b.id(),
            "identical values dedupe rather than compare"
        );
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

    #[test]
    fn snapshot_insertion_is_idempotent_and_checks_the_full_witness() {
        let a = PinAssertion::sign(&key(1), pin(2), val(3), label(4));
        let mut assertions = PinAssertionSnapshot::new();

        assertions.insert(a).unwrap();
        assertions.insert(a).unwrap();

        assert_eq!(assertions.len(), 1);
        assert!(assertions.contains(&a).unwrap());

        // The public API cannot construct this state. Force it here to prove
        // that a matching PATCH key is not silently treated as equal when its
        // attached canonical witness differs.
        let b = PinAssertion::sign(&key(5), pin(6), val(7), label(8));
        let mut forged = PinAssertionSnapshot::new();
        forged.assertions.insert(&Entry::with_value(
            &a.index_key(),
            PinAssertionWitness::from_verified(b),
        ));
        assert_eq!(forged.insert(a), Err(PinAssertionKeyCollision));
        assert_eq!(forged.contains(&a), Err(PinAssertionKeyCollision));
    }

    #[test]
    fn snapshot_union_is_commutative_associative_and_idempotent() {
        let a = PinAssertion::sign(&key(1), pin(1), val(1), label(1));
        let b = PinAssertion::sign(&key(2), pin(2), val(2), label(2));
        let c = PinAssertion::sign(&key(3), pin(3), val(3), label(3));

        let mut ab = snapshot(&[a]);
        ab.union(snapshot(&[b])).unwrap();
        let mut ba = snapshot(&[b]);
        ba.union(snapshot(&[a])).unwrap();
        assert_eq!(ab, ba, "union must be commutative");

        let mut a_bc = snapshot(&[b]);
        a_bc.union(snapshot(&[c])).unwrap();
        let mut left = snapshot(&[a]);
        left.union(a_bc).unwrap();

        let mut ab_c = snapshot(&[a]);
        ab_c.union(snapshot(&[b])).unwrap();
        ab_c.union(snapshot(&[c])).unwrap();
        assert_eq!(left, ab_c, "union must be associative");

        let before = left.clone();
        left.union(before.clone()).unwrap();
        assert_eq!(left, before, "union must be idempotent");
    }

    #[test]
    fn exact_pin_selection_uses_author_and_descriptor() {
        let wanted_a = PinAssertion::sign(&key(1), pin(9), val(1), label(1));
        let wanted_b = PinAssertion::sign(&key(1), pin(9), val(2), label(2));
        let other_author = PinAssertion::sign(&key(2), pin(9), val(3), label(3));
        let other_descriptor = PinAssertion::sign(&key(1), pin(8), val(4), label(4));
        let assertions = snapshot(&[wanted_a, wanted_b, other_author, other_descriptor]);

        let identity = PinIdentity::new(key(1).verifying_key(), pin(9));
        let mut selected = assertions
            .for_pin(&identity)
            .into_iter()
            .map(|assertion| assertion.id())
            .collect::<Vec<_>>();
        selected.sort();
        let mut expected = vec![wanted_a.id(), wanted_b.id()];
        expected.sort();

        assert_eq!(selected, expected);
        assert_eq!(assertions.iter().count(), 4);
    }

    #[test]
    fn pin_index_uses_full_identity_digest_and_rechecks_exact_identity() {
        let wanted = PinAssertion::sign(&key(1), pin(9), val(1), label(1));
        let foreign = PinAssertion::sign(&key(2), pin(9), val(2), label(2));
        let identity = *wanted.identity();
        let key = wanted.index_key();
        assert_eq!(&key[..32], &identity.digest());
        assert_eq!(&key[32..], &wanted.id().raw());

        let mut assertions = snapshot(&[wanted]);
        assertions.insert_with_identity_digest_for_test(identity.digest(), foreign);

        assert_eq!(assertions.for_pin(&identity), vec![wanted]);
        assert_eq!(assertions.len(), 2, "the foreign witness is preserved");
    }

    #[test]
    fn invalid_persisted_signature_is_hidden_and_verified_only_once() {
        let valid = PinAssertion::sign(&key(7), pin(11), val(19), label(4));
        let identity = *valid.identity();
        let mut encoded = valid.encode();
        encoded[SIGNATURE_RANGE.start] ^= 1;
        let invalid = UnverifiedPinAssertion::decode_structural(encoded).unwrap();

        reset_signature_verification_count();
        let mut assertions = PinAssertionSnapshot::new();
        assertions.insert_unverified(invalid).unwrap();
        assert_eq!(signature_verification_count(), 0, "replay stays lazy");
        assert_eq!(assertions.len(), 1, "the exact witness is preserved");
        let witnesses = assertions.witnesses_for_pin(&identity);
        assert_eq!(witnesses.len(), 1);
        assert_eq!(witnesses[0].id(), invalid.id());
        assert_eq!(witnesses[0].claimed_value(), valid.value());
        assert_eq!(witnesses[0].claimed_label(), valid.label());
        assert_eq!(signature_verification_count(), 0, "claimed reads stay lazy");

        assert_eq!(assertions.iter().count(), 0);
        assert_eq!(signature_verification_count(), 1);
        assert!(assertions.for_pin(&identity).is_empty());
        assert_eq!(assertions.iter().count(), 0);
        assert_eq!(
            signature_verification_count(),
            1,
            "both failure and success must be memoized"
        );
    }
}
