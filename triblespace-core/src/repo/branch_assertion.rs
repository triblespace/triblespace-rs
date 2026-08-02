//! Immutable signed assertions are the replicated state of a branch.
//!
//! A branch identity is the exact `(author key, name handle)` descriptor. Its
//! 16-byte [`BranchId`] is the intrinsic id of the equivalent
//! two-fact entity, and is therefore only an index prefix: equality always
//! compares the full descriptor. An assertion adds one commit and one Ed25519
//! signature. There is deliberately no timestamp, extension map, replacement,
//! or deletion.
//!
//! The canonical assertion encoding is exactly 160 bytes:
//!
//! ```text
//! author key [32] | name handle [32] | commit handle [32] | signature [64]
//! ```
//!
//! Public [`BranchAssertion`] values have already passed strict signature
//! verification. Persisted snapshots retain a private structural witness and
//! verify only claims that branch resolution may expose; raw bytes become a
//! public assertion only through [`BranchAssertion::decode_verified`].
//!
//! [`BranchId`]: crate::repo::branch_assertion::BranchId
//! [`BranchAssertion`]: crate::repo::branch_assertion::BranchAssertion
//! [`BranchAssertion::decode_verified`]: crate::repo::branch_assertion::BranchAssertion::decode_verified

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

#[cfg(test)]
use std::cell::Cell;

use ed25519::signature::Signer;
use ed25519::Signature;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hex_literal::hex;

use crate::blob::encodings::longstring::LongString;
use crate::id::Id;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::Inline;
use crate::macros::entity;
use crate::metadata;
use crate::patch::{Entry, IdentitySchema, PATCH};
use crate::repo::branch_pin::BranchIdentity;
use crate::repo::CommitHandle;

/// Number of semantic bytes in a canonical branch assertion.
pub const BRANCH_ASSERTION_LEN: usize = 160;

const AUTHOR_RANGE: std::ops::Range<usize> = 0..32;
const NAME_RANGE: std::ops::Range<usize> = 32..64;
const COMMIT_RANGE: std::ops::Range<usize> = 64..96;
const SIGNATURE_RANGE: std::ops::Range<usize> = 96..160;

/// Domain separator for the bytes signed by a V1 branch assertion.
/// Minted with `trible genid` on 2026-08-01.
const ASSERTION_V1_SIGNATURE_DOMAIN: [u8; 16] = hex!("C06E4D18932F6E89A89F9382744AA248");
const SIGNED_MESSAGE_LEN: usize = 16 + 32 + 32 + 32;
const ASSERTION_INDEX_KEY_LEN: usize = 16 + 32;

/// Intrinsic 16-byte index/cache identifier of a [`BranchIdentity`].
///
/// This is intentionally a distinct type rather than a bare [`Id`]. It is a
/// truncated hash and MUST NOT be used as branch identity equality; compare
/// the complete descriptor instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId(Id);

impl BranchId {
    /// Return the underlying intrinsic entity id.
    pub const fn entity(self) -> Id {
        self.0
    }

    /// Return the canonical 16-byte index prefix.
    pub const fn raw(self) -> [u8; 16] {
        self.0.raw()
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

fn branch_id(identity: &BranchIdentity) -> BranchId {
    let author = identity.author();
    let fragment = entity! {
        metadata::name: identity.name(),
        crate::repo::signed_by: author,
    };
    BranchId(
        fragment
            .root()
            .expect("a two-fact intrinsic identity exports one entity"),
    )
}

/// Blake3 content id of one canonical signed assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssertionId([u8; 32]);

impl AssertionId {
    /// Return the canonical digest bytes.
    pub const fn raw(self) -> [u8; 32] {
        self.0
    }
}

/// A canonical branch assertion whose signature has already been verified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BranchAssertion {
    identity: BranchIdentity,
    commit: CommitHandle,
    signature: [u8; 64],
}

impl BranchAssertion {
    /// Sign a branch assertion locally. The returned value is already verified
    /// by construction and can enter a [`BranchAssertionSnapshot`].
    pub fn sign(key: &SigningKey, name: Inline<Handle<LongString>>, commit: CommitHandle) -> Self {
        let identity = BranchIdentity::new(key.verifying_key(), name);
        let message = signed_message(&identity.author().to_bytes(), &name.raw, &commit.raw);
        let signature = key.sign(&message).to_bytes();
        Self {
            identity,
            commit,
            signature,
        }
    }

    /// Decode the fixed semantic bytes and verify the signature strictly.
    ///
    /// `verify_strict` rejects weak keys and non-canonical encodings. Pile
    /// padding and record markers are intentionally outside this semantic
    /// codec and are checked by the pile decoder.
    pub fn decode_verified(bytes: [u8; BRANCH_ASSERTION_LEN]) -> Result<Self, AssertionError> {
        UnverifiedBranchAssertion::decode_structural(bytes)?.verify_strict()
    }

    /// Encode this verified assertion into its canonical 160 semantic bytes.
    pub fn encode(&self) -> [u8; BRANCH_ASSERTION_LEN] {
        let mut bytes = [0u8; BRANCH_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author().to_bytes());
        bytes[NAME_RANGE].copy_from_slice(&self.identity.name().raw);
        bytes[COMMIT_RANGE].copy_from_slice(&self.commit.raw);
        bytes[SIGNATURE_RANGE].copy_from_slice(&self.signature);
        bytes
    }

    /// Return the exact branch identity descriptor.
    pub const fn identity(&self) -> &BranchIdentity {
        &self.identity
    }

    /// Return the asserted commit handle.
    pub const fn commit(&self) -> CommitHandle {
        self.commit
    }

    /// Derive this assertion's Blake3 content id.
    pub fn id(&self) -> AssertionId {
        AssertionId(Blake3::digest(&self.encode()))
    }

    fn index_key(&self) -> [u8; ASSERTION_INDEX_KEY_LEN] {
        index_key(branch_id(&self.identity), self.id())
    }
}

/// Structurally decoded canonical assertion bytes whose signature has not yet
/// been checked.
///
/// This type is deliberately crate-visible only. It may live in a persisted
/// snapshot, but none of its claimed identity/commit accessors are public API;
/// a [`BranchAssertion`] is the sole verified value that can leave the
/// repository layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnverifiedBranchAssertion {
    identity: BranchIdentity,
    commit: CommitHandle,
    signature: [u8; 64],
}

impl UnverifiedBranchAssertion {
    /// Decode fixed semantic fields without performing Ed25519 verification.
    /// A malformed author key remains a structural error.
    pub(crate) fn decode_structural(
        bytes: [u8; BRANCH_ASSERTION_LEN],
    ) -> Result<Self, AssertionError> {
        let author_bytes: [u8; 32] = bytes[AUTHOR_RANGE].try_into().unwrap();
        let author = VerifyingKey::from_bytes(&author_bytes)
            .map_err(|_| AssertionError::InvalidAuthorKey)?;
        let name_raw: [u8; 32] = bytes[NAME_RANGE].try_into().unwrap();
        let commit_raw: [u8; 32] = bytes[COMMIT_RANGE].try_into().unwrap();
        let signature: [u8; 64] = bytes[SIGNATURE_RANGE].try_into().unwrap();
        Ok(Self {
            identity: BranchIdentity::new(author, Inline::new(name_raw)),
            commit: Inline::new(commit_raw),
            signature,
        })
    }

    pub(crate) fn encode(self) -> [u8; BRANCH_ASSERTION_LEN] {
        let mut bytes = [0u8; BRANCH_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author().to_bytes());
        bytes[NAME_RANGE].copy_from_slice(&self.identity.name().raw);
        bytes[COMMIT_RANGE].copy_from_slice(&self.commit.raw);
        bytes[SIGNATURE_RANGE].copy_from_slice(&self.signature);
        bytes
    }

    pub(crate) fn identity(self) -> BranchIdentity {
        self.identity
    }

    pub(crate) fn commit(self) -> CommitHandle {
        self.commit
    }

    pub(crate) fn id(self) -> AssertionId {
        AssertionId(Blake3::digest(&self.encode()))
    }

    fn index_key(self) -> [u8; ASSERTION_INDEX_KEY_LEN] {
        index_key(branch_id(&self.identity), self.id())
    }

    pub(crate) fn verify_strict(self) -> Result<BranchAssertion, AssertionError> {
        #[cfg(test)]
        SIGNATURE_VERIFICATIONS.with(|count| count.set(count.get() + 1));

        let author = self.identity.author();
        let author_bytes = author.to_bytes();
        let name = self.identity.name();
        let signature = Signature::from_bytes(&self.signature);
        let message = signed_message(&author_bytes, &name.raw, &self.commit.raw);
        author
            .verify_strict(&message, &signature)
            .map_err(|_| AssertionError::InvalidSignature)?;
        Ok(BranchAssertion {
            identity: self.identity,
            commit: self.commit,
            signature: self.signature,
        })
    }
}

impl From<BranchAssertion> for UnverifiedBranchAssertion {
    fn from(assertion: BranchAssertion) -> Self {
        Self {
            identity: assertion.identity,
            commit: assertion.commit,
            signature: assertion.signature,
        }
    }
}

#[cfg(test)]
thread_local! {
    static SIGNATURE_VERIFICATIONS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_signature_verification_count() {
    SIGNATURE_VERIFICATIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn signature_verification_count() -> usize {
    SIGNATURE_VERIFICATIONS.with(Cell::get)
}

fn signed_message(
    author: &[u8; 32],
    name: &[u8; 32],
    commit: &[u8; 32],
) -> [u8; SIGNED_MESSAGE_LEN] {
    let mut message = [0u8; SIGNED_MESSAGE_LEN];
    message[..16].copy_from_slice(&ASSERTION_V1_SIGNATURE_DOMAIN);
    message[16..48].copy_from_slice(author);
    message[48..80].copy_from_slice(name);
    message[80..112].copy_from_slice(commit);
    message
}

fn index_key(branch: BranchId, assertion: AssertionId) -> [u8; ASSERTION_INDEX_KEY_LEN] {
    let mut key = [0u8; ASSERTION_INDEX_KEY_LEN];
    key[..16].copy_from_slice(&branch.raw());
    key[16..].copy_from_slice(&assertion.raw());
    key
}

/// Why raw assertion bytes could not become a verified public assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssertionError {
    /// The claimed Ed25519 public key is not a valid verifying key.
    InvalidAuthorKey,
    /// The signature does not strictly authenticate the complete descriptor
    /// and commit under the V1 domain.
    InvalidSignature,
}

impl fmt::Display for AssertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAuthorKey => write!(f, "branch assertion has an invalid author key"),
            Self::InvalidSignature => write!(f, "branch assertion signature is invalid"),
        }
    }
}

impl Error for AssertionError {}

/// A practically impossible mismatch between a 48-byte index key and value.
///
/// The wrapper checks this instead of relying on PATCH values, because PATCH
/// equality and hashing deliberately concern keys only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AssertionKeyCollision;

impl fmt::Display for AssertionKeyCollision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "two different branch assertions have the same index key")
    }
}

impl Error for AssertionKeyCollision {}

/// Opaque grow-only snapshot of branch-assertion witnesses.
///
/// The inner PATCH is deliberately private: every safe insertion derives its
/// key from the exact canonical bytes, and there is no remove, replace,
/// difference, or scalar-head operation. Public iteration yields only
/// successfully verified [`BranchAssertion`] values. Pile replay uses the
/// crate-private structural insertion path so loading a log does no public-key
/// work.
#[derive(Clone, Debug, Default)]
pub struct BranchAssertionSnapshot {
    assertions: PATCH<ASSERTION_INDEX_KEY_LEN, IdentitySchema, AssertionWitness>,
}

#[derive(Clone, Debug)]
pub(crate) struct AssertionWitness {
    unverified: UnverifiedBranchAssertion,
    // Keep the cold persisted representation small. Only witnesses that reach
    // a semantic view allocate the verified public value.
    verified: OnceLock<Result<Box<BranchAssertion>, AssertionError>>,
}

impl PartialEq for AssertionWitness {
    fn eq(&self, other: &Self) -> bool {
        self.unverified == other.unverified
    }
}

impl Eq for AssertionWitness {}

impl AssertionWitness {
    fn from_verified(assertion: BranchAssertion) -> Self {
        Self {
            unverified: assertion.into(),
            verified: OnceLock::from(Ok(Box::new(assertion))),
        }
    }

    fn from_unverified(unverified: UnverifiedBranchAssertion) -> Self {
        Self {
            unverified,
            verified: OnceLock::new(),
        }
    }

    pub(crate) fn id(&self) -> AssertionId {
        self.unverified.id()
    }

    pub(crate) fn commit(&self) -> CommitHandle {
        self.unverified.commit()
    }

    pub(crate) fn verified(&self) -> Result<&BranchAssertion, AssertionError> {
        match self
            .verified
            .get_or_init(|| self.unverified.verify_strict().map(Box::new))
        {
            Ok(assertion) => Ok(assertion.as_ref()),
            Err(err) => Err(*err),
        }
    }
}

impl PartialEq for BranchAssertionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        if self.assertions.len() != other.assertions.len() {
            return false;
        }
        self.assertions
            .iter_ordered()
            .all(|key| self.assertions.get(key) == other.assertions.get(key))
    }
}

impl Eq for BranchAssertionSnapshot {}

impl BranchAssertionSnapshot {
    /// Create an empty assertion snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert one verified assertion. Re-inserting the same assertion is an
    /// idempotent success.
    pub fn insert(&mut self, assertion: BranchAssertion) -> Result<(), AssertionKeyCollision> {
        let witness = AssertionWitness::from_verified(assertion);
        self.insert_witness(witness)
    }

    /// Insert one structurally decoded persisted witness without verifying its
    /// signature. This is reserved for exact pile replay.
    pub(crate) fn insert_unverified(
        &mut self,
        assertion: UnverifiedBranchAssertion,
    ) -> Result<(), AssertionKeyCollision> {
        self.insert_witness(AssertionWitness::from_unverified(assertion))
    }

    fn insert_witness(&mut self, witness: AssertionWitness) -> Result<(), AssertionKeyCollision> {
        let key = witness.unverified.index_key();
        if let Some(existing) = self.assertions.get(&key) {
            if existing != &witness {
                return Err(AssertionKeyCollision);
            }
            return Ok(());
        }
        self.assertions.insert(&Entry::with_value(&key, witness));
        Ok(())
    }

    /// Check exact membership while still surfacing an index-key collision.
    pub(crate) fn contains(
        &self,
        assertion: &BranchAssertion,
    ) -> Result<bool, AssertionKeyCollision> {
        let witness = AssertionWitness::from_verified(*assertion);
        match self.assertions.get(&assertion.index_key()) {
            Some(existing) if existing == &witness => Ok(true),
            Some(_) => Err(AssertionKeyCollision),
            None => Ok(false),
        }
    }

    /// Union another grow-only snapshot into this one.
    pub fn union(&mut self, other: Self) -> Result<(), AssertionKeyCollision> {
        for key in other.assertions.iter_ordered() {
            let witness = other
                .assertions
                .get(key)
                .expect("a key yielded by PATCH resolves in the same snapshot");
            if let Some(existing) = self.assertions.get(&key) {
                if existing != witness {
                    return Err(AssertionKeyCollision);
                }
            }
        }
        self.assertions.union(other.assertions);
        Ok(())
    }

    /// Number of distinct stored canonical assertion witnesses.
    pub fn len(&self) -> usize {
        self.assertions.len() as usize
    }

    /// Whether the snapshot contains no assertions.
    pub fn is_empty(&self) -> bool {
        self.assertions.is_empty()
    }

    /// Iterate every valid assertion in canonical index-key order.
    ///
    /// This explicit semantic view verifies witnesses lazily and permanently
    /// memoizes both success and failure. Invalid signatures are omitted; no
    /// unverified claim can escape as a [`BranchAssertion`].
    pub fn iter(&self) -> impl Iterator<Item = &BranchAssertion> {
        self.assertions.iter_ordered().filter_map(|key| {
            let witness = self
                .assertions
                .get(key)
                .expect("a key yielded by PATCH resolves in the same snapshot");
            witness.verified().ok()
        })
    }

    /// Return every assertion for the exact descriptor.
    ///
    /// The 16-byte BranchId narrows the prefix scan; the full descriptor check
    /// prevents a truncated-id collision from merging two branches.
    pub fn for_branch(&self, identity: &BranchIdentity) -> Vec<BranchAssertion> {
        self.witnesses_for_branch(identity)
            .into_iter()
            .filter_map(|witness| witness.verified().ok().copied())
            .collect()
    }

    /// Return every structural witness for the exact descriptor. The resolver
    /// is the sole consumer: it verifies only optimistic-frontier claims before
    /// exposing any tip or demand.
    pub(crate) fn witnesses_for_branch(&self, identity: &BranchIdentity) -> Vec<&AssertionWitness> {
        let branch = branch_id(identity);
        let mut assertions = Vec::new();
        self.assertions
            .infixes::<16, 32, _>(&branch.raw(), |assertion_id| {
                let mut key = [0u8; ASSERTION_INDEX_KEY_LEN];
                key[..16].copy_from_slice(&branch.raw());
                key[16..].copy_from_slice(assertion_id);
                let assertion = self
                    .assertions
                    .get(&key)
                    .expect("a suffix yielded by PATCH resolves in the same snapshot");
                if assertion.unverified.identity() == *identity {
                    assertions.push(assertion);
                }
            });
        assertions
    }

    #[cfg(test)]
    fn insert_with_branch_prefix_for_test(&mut self, branch: BranchId, assertion: BranchAssertion) {
        let key = index_key(branch, assertion.id());
        self.assertions.insert(&Entry::with_value(
            &key,
            AssertionWitness::from_verified(assertion),
        ));
    }
}

/// Storage surface for the shared grow-only assertion layer.
///
/// Duplicate append is success. The trait deliberately has no update, delete,
/// tombstone, compare-and-swap, or scalar-head method. Implementations must
/// retain every accepted assertion or fail explicitly; storage pressure must
/// never silently evict replicated state.
///
/// Signature verification is not authorization. A replication ingest boundary
/// MUST restrict assertions to its configured identity/key set before calling
/// [`Self::append_assertion`]. The raw store remains policy-agnostic and does
/// not require the asserted commit metadata to be present locally. Skipping
/// that gate does not change the fold's mathematical correctness, but permits
/// unbounded storage and attention consumption; overload must reject explicitly
/// rather than silently drop accepted state.
pub trait BranchAssertionStore {
    /// Storage error.
    type Error: Error + fmt::Debug + Send + Sync + 'static;

    /// Return one coherent snapshot of all persisted assertion witnesses.
    /// Public views and branch resolution never expose an unverified claim.
    fn assertion_snapshot(&mut self) -> Result<BranchAssertionSnapshot, Self::Error>;

    /// Durably append one verified assertion. A duplicate is an idempotent
    /// success.
    fn append_assertion(&mut self, assertion: BranchAssertion) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::branch_pin::BranchIdentityParseError;

    fn name(byte: u8) -> Inline<Handle<LongString>> {
        Inline::new([byte; 32])
    }

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn signing_key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    #[test]
    fn canonical_codec_roundtrips_and_rejects_every_single_byte_change() {
        let assertion = BranchAssertion::sign(&signing_key(7), name(11), commit(19));
        let encoded = assertion.encode();
        assert_eq!(
            encoded,
            hex!(
                "EA4A6C63E29C520ABEF5507B132EC5F9954776AEBEBE7B92421EEA691446D22C
                 0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B
                 1313131313131313131313131313131313131313131313131313131313131313
                 ED9FC8AE03C142D1927C6ADA258D3198B271E5A0107413844DA758C220B01CB
                 2D269F760C1BDC729B23A51067E7011CE78EB05683ACCE6067BACD4A87EFE7E0F"
            )
        );
        assert_eq!(
            branch_id(assertion.identity()).raw(),
            hex!("AFF4BA00CA5D7270149C932647173802")
        );
        assert_eq!(
            assertion.id().raw(),
            hex!("87D84224C432A5E9F93850C35B2AC17FD689960387A777120A255492756FD732")
        );
        assert_eq!(encoded.len(), BRANCH_ASSERTION_LEN);
        assert_eq!(
            BranchAssertion::decode_verified(encoded).unwrap(),
            assertion
        );

        for index in 0..BRANCH_ASSERTION_LEN {
            let mut changed = encoded;
            changed[index] ^= 1;
            assert!(
                BranchAssertion::decode_verified(changed).is_err(),
                "byte {index} was not authenticated"
            );
        }
    }

    #[test]
    fn identity_is_stable_and_uses_both_descriptor_fields() {
        let key = signing_key(3);
        let a = BranchIdentity::new(key.verifying_key(), name(1));
        let same = BranchIdentity::new(key.verifying_key(), name(1));
        let renamed = BranchIdentity::new(key.verifying_key(), name(2));
        let rekeyed = BranchIdentity::new(signing_key(4).verifying_key(), name(1));
        assert_eq!(a, same);
        assert_eq!(branch_id(&a), branch_id(&same));
        assert_ne!(a, renamed);
        assert_ne!(branch_id(&a), branch_id(&renamed));
        assert_ne!(a, rekeyed);
        assert_ne!(branch_id(&a), branch_id(&rekeyed));
    }

    #[test]
    fn exact_identity_selector_has_one_stable_rendering() {
        let identity = BranchIdentity::new(signing_key(3).verifying_key(), name(1));
        let selector = identity.to_string();

        assert_eq!(
            selector,
            concat!(
                "ed25519:ed4928c628d1c2c6eae90338905995612959273a5c63f93636c14614ac8737d1/",
                "blake3:0101010101010101010101010101010101010101010101010101010101010101"
            )
        );
        assert_eq!(selector.parse::<BranchIdentity>().unwrap(), identity);
    }

    #[test]
    fn exact_identity_selector_rejects_ambiguous_or_malformed_inputs() {
        let identity = BranchIdentity::new(signing_key(3).verifying_key(), name(1));
        let selector = identity.to_string();

        assert_eq!(
            branch_id(&identity).to_string().parse::<BranchIdentity>(),
            Err(BranchIdentityParseError::InvalidFormat)
        );
        assert_eq!(
            selector
                .replace("ed25519:", "key:")
                .parse::<BranchIdentity>(),
            Err(BranchIdentityParseError::InvalidFormat)
        );
        assert_eq!(
            selector
                .replace("blake3:01", "blake3:zz")
                .parse::<BranchIdentity>(),
            Err(BranchIdentityParseError::InvalidNameHex)
        );
    }

    #[test]
    fn snapshot_is_idempotent_and_delivery_order_independent() {
        let key = signing_key(9);
        let assertions: Vec<_> = [1, 2, 3]
            .into_iter()
            .map(|c| BranchAssertion::sign(&key, name(5), commit(c)))
            .collect();

        let mut forward = BranchAssertionSnapshot::new();
        for assertion in &assertions {
            forward.insert(assertion.clone()).unwrap();
            forward.insert(assertion.clone()).unwrap();
        }
        let mut reverse = BranchAssertionSnapshot::new();
        for assertion in assertions.iter().rev() {
            reverse.insert(assertion.clone()).unwrap();
        }
        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), assertions.len());
    }

    #[test]
    fn branch_id_prefix_is_never_identity_equality() {
        let wanted = BranchAssertion::sign(&signing_key(1), name(1), commit(1));
        let colliding = BranchAssertion::sign(&signing_key(2), name(2), commit(2));
        let forced_prefix = branch_id(wanted.identity());
        let mut snapshot = BranchAssertionSnapshot::new();
        snapshot.insert(wanted.clone()).unwrap();
        snapshot.insert_with_branch_prefix_for_test(forced_prefix, colliding);

        assert_eq!(snapshot.for_branch(wanted.identity()), vec![wanted]);
    }
}
