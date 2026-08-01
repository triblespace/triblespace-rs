//! Immutable signed assertions are the replicated state of a branch.
//!
//! A branch identity is the exact `(author key, name handle)` descriptor. Its
//! 16-byte [`BranchId`](self::BranchId) is the intrinsic id of the equivalent
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
//! Public [`BranchAssertion`](self::BranchAssertion) values have already passed
//! strict signature verification. Raw bytes can enter the semantic layer only
//! through
//! [`BranchAssertion::decode_verified`](self::BranchAssertion::decode_verified).

use std::error::Error;
use std::fmt;

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

/// Blake3 content id of one canonical signed assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssertionId([u8; 32]);

impl AssertionId {
    /// Return the canonical digest bytes.
    pub const fn raw(self) -> [u8; 32] {
        self.0
    }
}

/// The exact identity descriptor of one branch.
///
/// The name's content may be absent locally without making the identity
/// malformed: its content-addressed handle is the identity component.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchIdentity {
    author: [u8; 32],
    name: Inline<Handle<LongString>>,
}

impl BranchIdentity {
    /// Construct an identity from a checked Ed25519 key and a name handle.
    pub fn new(author: VerifyingKey, name: Inline<Handle<LongString>>) -> Self {
        Self {
            author: author.to_bytes(),
            name,
        }
    }

    /// Return the complete checked author key.
    pub fn author(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.author)
            .expect("BranchIdentity is constructible only from a checked key")
    }

    /// Return the content-addressed branch-name handle.
    pub const fn name(&self) -> Inline<Handle<LongString>> {
        self.name
    }

    /// Derive the intrinsic id of the exact two-fact identity entity.
    pub fn id(&self) -> BranchId {
        let author = self.author();
        let fragment = entity! {
            metadata::name: self.name,
            crate::repo::signed_by: author,
        };
        BranchId(
            fragment
                .root()
                .expect("a two-fact intrinsic identity exports one entity"),
        )
    }
}

/// A canonical branch assertion whose signature has already been verified.
#[derive(Clone, Debug, PartialEq, Eq)]
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
        let message = signed_message(&identity.author, &name.raw, &commit.raw);
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
        let author_bytes: [u8; 32] = bytes[AUTHOR_RANGE].try_into().unwrap();
        let author = VerifyingKey::from_bytes(&author_bytes)
            .map_err(|_| AssertionError::InvalidAuthorKey)?;
        let name_raw: [u8; 32] = bytes[NAME_RANGE].try_into().unwrap();
        let commit_raw: [u8; 32] = bytes[COMMIT_RANGE].try_into().unwrap();
        let signature_bytes: [u8; 64] = bytes[SIGNATURE_RANGE].try_into().unwrap();
        let signature = Signature::from_bytes(&signature_bytes);
        let message = signed_message(&author_bytes, &name_raw, &commit_raw);
        author
            .verify_strict(&message, &signature)
            .map_err(|_| AssertionError::InvalidSignature)?;

        Ok(Self {
            identity: BranchIdentity::new(author, Inline::new(name_raw)),
            commit: Inline::new(commit_raw),
            signature: signature_bytes,
        })
    }

    /// Encode this verified assertion into its canonical 160 semantic bytes.
    pub fn encode(&self) -> [u8; BRANCH_ASSERTION_LEN] {
        let mut bytes = [0u8; BRANCH_ASSERTION_LEN];
        bytes[AUTHOR_RANGE].copy_from_slice(&self.identity.author);
        bytes[NAME_RANGE].copy_from_slice(&self.identity.name.raw);
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
        index_key(self.identity.id(), self.id())
    }
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

/// Why raw assertion bytes were not admitted to replicated state.
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

/// Opaque grow-only snapshot of verified branch assertions.
///
/// The inner PATCH is deliberately private: every safe insertion derives its
/// key from the verified value, and there is no remove, replace, difference,
/// or scalar-head operation.
#[derive(Clone, Debug, Default)]
pub struct BranchAssertionSnapshot {
    assertions: PATCH<ASSERTION_INDEX_KEY_LEN, IdentitySchema, BranchAssertion>,
}

impl PartialEq for BranchAssertionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.assertions == other.assertions
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
        let key = assertion.index_key();
        if let Some(existing) = self.assertions.get(&key) {
            if existing != &assertion {
                return Err(AssertionKeyCollision);
            }
            return Ok(());
        }
        self.assertions.insert(&Entry::with_value(&key, assertion));
        Ok(())
    }

    /// Union another grow-only snapshot into this one.
    pub fn union(&mut self, other: Self) -> Result<(), AssertionKeyCollision> {
        for assertion in other.iter() {
            let key = assertion.index_key();
            if let Some(existing) = self.assertions.get(&key) {
                if existing != assertion {
                    return Err(AssertionKeyCollision);
                }
            }
        }
        self.assertions.union(other.assertions);
        Ok(())
    }

    /// Number of distinct verified assertions.
    pub fn len(&self) -> usize {
        self.assertions.len() as usize
    }

    /// Whether the snapshot contains no assertions.
    pub fn is_empty(&self) -> bool {
        self.assertions.is_empty()
    }

    /// Iterate every assertion in canonical index-key order.
    pub fn iter(&self) -> impl Iterator<Item = &BranchAssertion> {
        self.assertions.iter_ordered().map(|key| {
            self.assertions
                .get(key)
                .expect("a key yielded by PATCH resolves in the same snapshot")
        })
    }

    /// Return every assertion for the exact descriptor.
    ///
    /// The 16-byte BranchId narrows the prefix scan; the full descriptor check
    /// prevents a truncated-id collision from merging two branches.
    pub fn for_branch(&self, identity: &BranchIdentity) -> Vec<BranchAssertion> {
        let branch = identity.id();
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
                if assertion.identity() == identity {
                    assertions.push(assertion.clone());
                }
            });
        assertions
    }

    #[cfg(test)]
    fn insert_with_branch_prefix_for_test(&mut self, branch: BranchId, assertion: BranchAssertion) {
        let key = index_key(branch, assertion.id());
        self.assertions.insert(&Entry::with_value(&key, assertion));
    }
}

/// Storage surface for the shared grow-only assertion layer.
///
/// Duplicate append is success. The trait deliberately has no update, delete,
/// tombstone, compare-and-swap, or scalar-head method.
pub trait BranchAssertionStore {
    /// Storage error.
    type Error: Error + fmt::Debug + Send + Sync + 'static;

    /// Return one coherent snapshot of all verified assertions.
    fn assertion_snapshot(&mut self) -> Result<BranchAssertionSnapshot, Self::Error>;

    /// Durably append one verified assertion. A duplicate is an idempotent
    /// success.
    fn append_assertion(&mut self, assertion: BranchAssertion) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(a.id(), same.id());
        assert_ne!(a, renamed);
        assert_ne!(a.id(), renamed.id());
        assert_ne!(a, rekeyed);
        assert_ne!(a.id(), rekeyed.id());
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
        let forced_prefix = wanted.identity().id();
        let mut snapshot = BranchAssertionSnapshot::new();
        snapshot.insert(wanted.clone()).unwrap();
        snapshot.insert_with_branch_prefix_for_test(forced_prefix, colliding);

        assert_eq!(snapshot.for_branch(wanted.identity()), vec![wanted]);
    }
}
