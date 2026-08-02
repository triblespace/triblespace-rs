//! The branch pin kind: the first typed adapter over the generic envelope.
//!
//! A branch is one pin kind, not a separate mechanism. This module owns the
//! part the generic layer must not: the branch kind's **label encoding** and
//! the proof that it is causally monotone.
//!
//! [`SubsumptionLabel`] is opaque and offers only bytewise `Ord`. It cannot
//! prove anything on its own, so the sound inference —
//!
//! > `label(A) >= label(B)` implies A is not a strict ancestor of B
//!
//! — is licensed *here*, by this kind, and only because honest branch ranks are
//! constructed inductively: a root is zero, an ordinary child is its parent's
//! successor, and a merge is the successor of its greatest parent rank. Thus a
//! strict ancestry edge always increases rank. Equal rank therefore rules out
//! strict ancestry between distinct commits, which is why `>=` rather than `>`
//! is the correct test after identical values have been grouped.
//!
//! Rank is deliberately not “DAG depth”. Publication derives it from the
//! asserted/workspace provenance already in hand and never materialises a
//! remote ancestry chain merely to count it. Ranks from independent histories
//! have no quantitative meaning; the only contract is increase along ancestry.
//! A dishonest rank can suppress exact walks and conservatively retain extra
//! tips, but cannot drop one because labels never decide domination directly.
//!
//! A kind that cannot make an argument of this shape supplies no label
//! comparison at all and takes zero skips — degraded, never wrong.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use anybytes::Bytes;
use hex_literal::hex;

use super::pin_assertion::{PinAssertion, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle};
use super::strong_pin::{StrongPinDescriptor, StrongPinDescriptorError};
use super::BlobStoreGet;
use super::CommitHandle;
use crate::blob::encodings::{longstring::LongString, UnknownBlob};
use crate::blob::{Blob, BlobEncoding, TryFromBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::Inline;
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};
use crate::trible::Fragment;
use ed25519_dalek::{SigningKey, VerifyingKey};

/// The exact human-facing identity of one branch: `(author key, name handle)`.
///
/// The generic asserted-pin layer indexes the corresponding
/// `(author key, outer descriptor handle)` pair. The typed inner descriptor deliberately
/// keeps the name handle because it is what users select and what repositories
/// stage as presentation content. Neither the name nor any truncated digest is
/// a branch selector by itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    /// Return the exact generic identity used by asserted-pin storage.
    pub fn pin_identity(&self) -> PinIdentity {
        BranchPinDescriptor::pin_identity(self.author(), self.name)
    }
}

impl fmt::Display for BranchIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ed25519:{}/blake3:{}",
            hex::encode(self.author),
            hex::encode(self.name.raw)
        )
    }
}

impl FromStr for BranchIdentity {
    type Err = BranchIdentityParseError;

    fn from_str(selector: &str) -> Result<Self, Self::Err> {
        let encoded = selector
            .strip_prefix("ed25519:")
            .ok_or(BranchIdentityParseError::InvalidFormat)?;
        let (author, name) = encoded
            .split_once("/blake3:")
            .ok_or(BranchIdentityParseError::InvalidFormat)?;
        if author.len() != 64 || name.len() != 64 || name.contains('/') {
            return Err(BranchIdentityParseError::InvalidFormat);
        }

        let mut author_bytes = [0u8; 32];
        hex::decode_to_slice(author, &mut author_bytes)
            .map_err(|_| BranchIdentityParseError::InvalidAuthorHex)?;
        let author = VerifyingKey::from_bytes(&author_bytes)
            .map_err(|_| BranchIdentityParseError::InvalidAuthorKey)?;

        let mut name_bytes = [0u8; 32];
        hex::decode_to_slice(name, &mut name_bytes)
            .map_err(|_| BranchIdentityParseError::InvalidNameHex)?;
        Ok(Self::new(author, Inline::new(name_bytes)))
    }
}

/// Why a textual exact branch descriptor could not be parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchIdentityParseError {
    /// The selector is not `ed25519:<64 hex>/blake3:<64 hex>`.
    InvalidFormat,
    /// The author component is not hexadecimal.
    InvalidAuthorHex,
    /// The decoded author is not a valid Ed25519 verifying key.
    InvalidAuthorKey,
    /// The name-handle component is not hexadecimal.
    InvalidNameHex,
}

impl fmt::Display for BranchIdentityParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => write!(
                f,
                "branch selector must be ed25519:<64 hex>/blake3:<64 hex>"
            ),
            Self::InvalidAuthorHex => write!(f, "branch selector author is not hexadecimal"),
            Self::InvalidAuthorKey => {
                write!(f, "branch selector author is not a valid Ed25519 key")
            }
            Self::InvalidNameHex => {
                write!(f, "branch selector name handle is not hexadecimal")
            }
        }
    }
}

impl Error for BranchIdentityParseError {}

/// Canonical byte length of a V2 branch-pin descriptor.
pub const BRANCH_PIN_DESCRIPTOR_LEN: usize = 16 + 16 + 32;

/// Kind/schema marker for the V2 branch-pin descriptor.
///
/// Minted with `trible genid` on 2026-08-02. Keeping the kind inside the
/// content-addressed descriptor prevents a branch name handle from aliasing a
/// different pin kind that happens to use the same 32-byte value.
pub const BRANCH_PIN_DESCRIPTOR_V2: [u8; 16] = hex!("3EB94D2C4719C1A771F31E87749D5909");

/// Blob encoding for the exact inner identity descriptor of a branch pin.
///
/// The bytes are `kind marker [16] | zero padding [16] | LongString name
/// handle [32]`. Aligning the handle on a 32-byte boundary makes conservative
/// generic closure discovery retain a locally present name without teaching
/// the retention layer about branches. The generic assertion pin is a
/// [`StrongPinDescriptor`] wrapping this descriptor's exact content handle.
pub struct BranchPinDescriptor;

impl BlobEncoding for BranchPinDescriptor {}

impl MetaDescribe for BranchPinDescriptor {
    fn describe() -> Fragment {
        // The schema id is also the canonical in-band V2 kind marker. A format
        // change therefore mints a new schema id instead of growing a hidden
        // version switch inside this descriptor.
        let id: Id = id_hex!("3EB94D2C4719C1A771F31E87749D5909");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "branch-pin-descriptor-v2",
                metadata::description: "Canonical inner descriptor for an asserted branch pin: a V2 kind marker, sixteen zero padding bytes, and the aligned LongString handle of the branch name. A StrongPinDescriptor wraps its content handle to confer generic hard-retention semantics.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl BranchPinDescriptor {
    /// Encode one branch name into its canonical typed descriptor bytes.
    pub fn encode(name: Inline<Handle<LongString>>) -> [u8; BRANCH_PIN_DESCRIPTOR_LEN] {
        let mut raw = [0u8; BRANCH_PIN_DESCRIPTOR_LEN];
        raw[..16].copy_from_slice(&BRANCH_PIN_DESCRIPTOR_V2);
        raw[32..].copy_from_slice(&name.raw);
        raw
    }

    /// Build the descriptor blob that a repository stages beside the name.
    pub fn blob(name: Inline<Handle<LongString>>) -> Blob<Self> {
        Blob::new(Bytes::from_source(Self::encode(name).to_vec()))
    }

    /// Derive this inner descriptor's exact content handle.
    pub fn descriptor_handle(name: Inline<Handle<LongString>>) -> Inline<Handle<Self>> {
        Inline::new(Blake3::digest(&Self::encode(name)))
    }

    /// Reinterpret a decoded generic inner handle as a branch descriptor.
    pub fn from_unknown_handle(handle: Inline<Handle<UnknownBlob>>) -> Inline<Handle<Self>> {
        Inline::new(handle.raw)
    }

    /// Build the outer strong-retention descriptor staged before publication.
    pub fn strong_blob(name: Inline<Handle<LongString>>) -> Blob<StrongPinDescriptor> {
        StrongPinDescriptor::blob(Self::descriptor_handle(name))
    }

    /// Derive the generic outer pin handle without loading either descriptor.
    pub fn pin_handle(name: Inline<Handle<LongString>>) -> PinHandle {
        StrongPinDescriptor::pin_handle(Self::descriptor_handle(name))
    }

    /// Derive the generic exact identity for this author's named branch.
    pub fn pin_identity(
        author: ed25519_dalek::VerifyingKey,
        name: Inline<Handle<LongString>>,
    ) -> PinIdentity {
        PinIdentity::new(author, Self::pin_handle(name))
    }
}

impl TryFromBlob<BranchPinDescriptor> for Inline<Handle<LongString>> {
    type Error = BranchPinDescriptorError;

    fn try_from_blob(blob: Blob<BranchPinDescriptor>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != BRANCH_PIN_DESCRIPTOR_LEN {
            return Err(BranchPinDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes[..16] != BRANCH_PIN_DESCRIPTOR_V2 {
            return Err(BranchPinDescriptorError::WrongKind);
        }
        if bytes[16..32].iter().any(|byte| *byte != 0) {
            return Err(BranchPinDescriptorError::NonZeroReserved);
        }
        Ok(Inline::new(bytes[32..].try_into().expect("length checked")))
    }
}

/// A branch-pin descriptor was not the one exact canonical V2 shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchPinDescriptorError {
    /// The descriptor was not exactly 64 bytes.
    WrongLength { actual: usize },
    /// The descriptor did not carry the branch V2 kind marker.
    WrongKind,
    /// Reserved alignment bytes were not all canonical zeroes.
    NonZeroReserved,
}

impl fmt::Display for BranchPinDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "branch pin descriptor is {actual} bytes, expected {BRANCH_PIN_DESCRIPTOR_LEN}"
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V2 branch descriptor"),
            Self::NonZeroReserved => {
                write!(f, "branch pin descriptor has non-zero reserved bytes")
            }
        }
    }
}

impl Error for BranchPinDescriptorError {}

/// A canonical strong branch descriptor loaded from one generic pin handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedBranchPin {
    /// Exact outer descriptor named by the generic pin envelope.
    pub strong: Inline<Handle<StrongPinDescriptor>>,
    /// Exact wrapped branch descriptor.
    pub branch: Inline<Handle<BranchPinDescriptor>>,
    /// Exact content-addressed branch-name handle.
    pub name: Inline<Handle<LongString>>,
}

/// Failure while unwrapping a generic strong pin as a branch descriptor.
#[derive(Debug)]
pub enum LoadBranchPinError<StrongError, BranchError> {
    /// The outer descriptor was missing, malformed, or unreadable.
    Strong(StrongError),
    /// The wrapped descriptor was missing, malformed, or not a branch.
    Branch(BranchError),
}

impl<StrongError: fmt::Display, BranchError: fmt::Display> fmt::Display
    for LoadBranchPinError<StrongError, BranchError>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Strong(err) => write!(f, "could not load strong pin descriptor: {err}"),
            Self::Branch(err) => write!(f, "could not load branch pin descriptor: {err}"),
        }
    }
}

impl<StrongError, BranchError> Error for LoadBranchPinError<StrongError, BranchError>
where
    StrongError: Error + 'static,
    BranchError: Error + 'static,
{
}

/// Unwrap a generic strong pin and decode its branch name in one typed path.
///
/// Callers use this for discovery only. Exact branch resolution already knows
/// the author's name handle and therefore does not depend on descriptor
/// availability.
pub fn load_branch_pin<R>(
    reader: &R,
    pin: PinHandle,
) -> Result<
    DecodedBranchPin,
    LoadBranchPinError<
        R::GetError<StrongPinDescriptorError>,
        R::GetError<BranchPinDescriptorError>,
    >,
>
where
    R: BlobStoreGet,
{
    let strong = StrongPinDescriptor::descriptor_handle(pin);
    let inner = reader
        .get::<Inline<Handle<UnknownBlob>>, StrongPinDescriptor>(strong)
        .map_err(LoadBranchPinError::Strong)?;
    let branch = BranchPinDescriptor::from_unknown_handle(inner);
    let name = reader
        .get::<Inline<Handle<LongString>>, BranchPinDescriptor>(branch)
        .map_err(LoadBranchPinError::Branch)?;
    Ok(DecodedBranchPin {
        strong,
        branch,
        name,
    })
}

/// Reinterpret a generic assertion value as a branch commit handle after its
/// pin identity has been established as a canonical branch descriptor.
pub fn commit_from_value(value: ValueHandle) -> CommitHandle {
    Inline::new(value.raw())
}

/// Reinterpret one exact branch commit as the generic assertion value.
pub(crate) fn value_from_commit(commit: CommitHandle) -> ValueHandle {
    ValueHandle::from_raw(commit.raw)
}

/// Sign one typed branch assertion in the generic envelope.
///
/// The caller carries `rank` through the workspace's asserted provenance;
/// publication never walks remote history merely to derive the label. It
/// stages both descriptor layers before durably appending the record.
pub fn sign_branch_assertion(
    key: &SigningKey,
    name: Inline<Handle<LongString>>,
    commit: CommitHandle,
    rank: BranchRank,
) -> PinAssertion {
    PinAssertion::sign(
        key,
        BranchPinDescriptor::pin_handle(name),
        value_from_commit(commit),
        rank.label(),
    )
}

/// The branch kind's 256-bit, big-endian topological rank.
///
/// This wrapper is the typed proof boundary. The generic label remains opaque;
/// only branch code constructs successors and uses their order to suppress
/// impossible strict-ancestry walks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchRank(SubsumptionLabel);

impl BranchRank {
    /// Rank of a root with no parents.
    pub const ROOT: Self = Self(SubsumptionLabel::from_raw([0u8; 32]));

    /// Recover the rank claimed by a branch assertion.
    pub const fn from_label(label: SubsumptionLabel) -> Self {
        Self(label)
    }

    /// Return the exact generic label bytes carried in the signed assertion.
    pub const fn label(self) -> SubsumptionLabel {
        self.0
    }

    /// The smallest rank greater than this one under bytewise order.
    ///
    /// Treating all 32 bytes as one big-endian integer uses the entire opaque
    /// slot and avoids a realistic cutoff. `None` is still explicit for an
    /// adversarial all-`0xFF` parent label.
    pub fn successor(self) -> Option<Self> {
        let mut raw = self.0.raw();
        for byte in raw.iter_mut().rev() {
            let (next, carry) = byte.overflowing_add(1);
            *byte = next;
            if !carry {
                return Some(Self(SubsumptionLabel::from_raw(raw)));
            }
        }
        None
    }

    /// Preserve an already monotone descendant rank, or raise it just enough
    /// to sit strictly above a proven ancestor.
    ///
    /// A fast-forward may import rank provenance from another branch lineage.
    /// Exact ancestry licenses this local repair without materialising or
    /// relabelling the commits between the two endpoints.
    pub fn raise_above(self, ancestor: Self) -> Option<Self> {
        if self > ancestor {
            Some(self)
        } else {
            ancestor.successor()
        }
    }

    /// Root for an empty parent set; otherwise one past the greatest parent.
    pub fn after<I>(parents: I) -> Option<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        match parents.into_iter().max() {
            None => Some(Self::ROOT),
            Some(parent) => parent.successor(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn name(byte: u8) -> Inline<Handle<LongString>> {
        Inline::new([byte; 32])
    }

    #[test]
    fn descriptor_is_canonical_typed_content_and_roundtrips() {
        let name = name(7);
        let blob = BranchPinDescriptor::blob(name);
        let inner = blob.get_handle();
        let outer = BranchPinDescriptor::strong_blob(name);
        assert_eq!(blob.bytes.len(), BRANCH_PIN_DESCRIPTOR_LEN);
        assert_eq!(&blob.bytes[..16], &BRANCH_PIN_DESCRIPTOR_V2);
        assert_eq!(&blob.bytes[16..32], &[0u8; 16]);
        assert_eq!(&blob.bytes[32..], &name.raw);
        assert_eq!(BranchPinDescriptor::descriptor_handle(name), inner);
        assert_eq!(
            BranchPinDescriptor::pin_handle(name).raw(),
            outer.get_handle().raw,
            "the generic pin identity is the strong wrapper's content handle"
        );
        assert_ne!(outer.get_handle().raw, inner.raw);
        let decoded: Inline<Handle<LongString>> = blob.try_from_blob().unwrap();
        assert_eq!(decoded, name);
    }

    #[test]
    fn descriptor_rejects_wrong_kind_and_noncanonical_length() {
        let mut wrong_kind = BranchPinDescriptor::encode(name(3));
        wrong_kind[0] ^= 1;
        let err = Blob::<BranchPinDescriptor>::new(Bytes::from_source(wrong_kind.to_vec()))
            .try_from_blob::<Inline<Handle<LongString>>>()
            .unwrap_err();
        assert_eq!(err, BranchPinDescriptorError::WrongKind);

        let err = Blob::<BranchPinDescriptor>::new(Bytes::from_source(vec![0u8; 47]))
            .try_from_blob::<Inline<Handle<LongString>>>()
            .unwrap_err();
        assert_eq!(err, BranchPinDescriptorError::WrongLength { actual: 47 });

        let err = Blob::<BranchPinDescriptor>::new(Bytes::from_source(vec![0u8; 48]))
            .try_from_blob::<Inline<Handle<LongString>>>()
            .unwrap_err();
        assert_eq!(err, BranchPinDescriptorError::WrongLength { actual: 48 });

        let mut noncanonical_padding = BranchPinDescriptor::encode(name(3));
        noncanonical_padding[16] = 1;
        let err =
            Blob::<BranchPinDescriptor>::new(Bytes::from_source(noncanonical_padding.to_vec()))
                .try_from_blob::<Inline<Handle<LongString>>>()
                .unwrap_err();
        assert_eq!(err, BranchPinDescriptorError::NonZeroReserved);
    }

    #[test]
    fn aligned_descriptor_discovers_only_the_present_name_child() {
        use crate::blob::MemoryBlobStore;
        use crate::repo::{BlobChildren, BlobStore, BlobStorePut};

        let mut store = MemoryBlobStore::new();
        let name = store
            .put::<LongString, _>("aligned-name".to_owned())
            .unwrap();
        let descriptor = store
            .put::<BranchPinDescriptor, _>(BranchPinDescriptor::blob(name))
            .unwrap();
        let reader = store.reader().unwrap();

        assert_eq!(
            reader.children(descriptor.transmute()),
            vec![name.transmute()],
            "the marker-plus-padding candidate is rejected by existence lookup"
        );
    }

    #[test]
    fn descriptor_namespaces_branch_names_and_author_remains_part_of_identity() {
        let branch_name = name(11);
        let descriptor = BranchPinDescriptor::pin_handle(branch_name);
        assert_ne!(
            descriptor.raw(),
            branch_name.raw,
            "a raw name handle must never double as a generic pin kind"
        );

        let first = BranchPinDescriptor::pin_identity(
            SigningKey::from_bytes(&[1; 32]).verifying_key(),
            branch_name,
        );
        let second = BranchPinDescriptor::pin_identity(
            SigningKey::from_bytes(&[2; 32]).verifying_key(),
            branch_name,
        );
        assert_ne!(first, second);
        assert_ne!(first.digest(), second.digest());
    }

    #[test]
    fn exact_identity_selector_has_one_stable_rendering() {
        let key = SigningKey::from_bytes(&[3; 32]);
        let identity = BranchIdentity::new(key.verifying_key(), name(1));
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
        let key = SigningKey::from_bytes(&[3; 32]);
        let identity = BranchIdentity::new(key.verifying_key(), name(1));
        let selector = identity.to_string();

        assert_eq!(
            "00000000000000000000000000000000".parse::<BranchIdentity>(),
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
    fn branch_values_are_exact_commit_handles() {
        let raw = [19; 32];
        assert_eq!(commit_from_value(ValueHandle::from_raw(raw)).raw, raw);
    }

    #[test]
    fn typed_signing_uses_the_descriptor_commit_and_rank() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let name = name(11);
        let commit = Inline::new([19; 32]);
        let rank = BranchRank::ROOT.successor().unwrap();
        let assertion = sign_branch_assertion(&key, name, commit, rank);
        assert_eq!(
            assertion.identity(),
            &BranchPinDescriptor::pin_identity(key.verifying_key(), name)
        );
        assert_eq!(assertion.value(), value_from_commit(commit));
        assert_eq!(assertion.label(), rank.label());

        let inner = BranchPinDescriptor::descriptor_handle(name);
        let unwrapped = PinAssertion::sign(
            &key,
            PinHandle::from_raw(inner.raw),
            value_from_commit(commit),
            rank.label(),
        );
        assert_ne!(assertion.identity(), unwrapped.identity());
        assert_ne!(
            &assertion.encode()[128..],
            &unwrapped.encode()[128..],
            "wrapping changes the authenticated identity and signature"
        );
    }

    #[test]
    fn rank_is_a_full_width_big_endian_successor() {
        let zero = BranchRank::ROOT;
        let one = zero.successor().unwrap();
        assert_eq!(zero.label().raw(), [0u8; 32]);
        assert_eq!(one.label().raw()[31], 1);
        assert!(one > zero);

        let mut ff = [0u8; 32];
        ff[31] = 0xFF;
        let carried = BranchRank::from_label(SubsumptionLabel::from_raw(ff))
            .successor()
            .unwrap();
        assert_eq!(carried.label().raw()[30], 1);
        assert_eq!(carried.label().raw()[31], 0);

        assert!(
            BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]))
                .successor()
                .is_none()
        );
    }

    #[test]
    fn roots_children_and_merges_are_inductive_not_dag_walks() {
        assert_eq!(BranchRank::after([]).unwrap(), BranchRank::ROOT);
        let left = BranchRank::ROOT.successor().unwrap();
        let right = BranchRank::ROOT.successor().unwrap();
        assert_eq!(left, right, "independent children may share a rank");
        let merge = BranchRank::after([left, right]).unwrap();
        assert!(merge > left);
        assert!(merge > right);

        let child = merge.successor().unwrap();
        assert!(child > merge);
        assert!(child.label() > merge.label());
    }

    #[test]
    fn rank_repair_preserves_good_provenance_and_minimally_raises_stale_provenance() {
        let root = BranchRank::ROOT;
        let one = root.successor().unwrap();
        let two = one.successor().unwrap();

        assert_eq!(two.raise_above(one), Some(two));
        assert_eq!(root.raise_above(one), Some(two));
        assert_eq!(one.raise_above(one), Some(two));
        assert_eq!(
            root.raise_above(BranchRank::from_label(SubsumptionLabel::from_raw(
                [0xFF; 32]
            ))),
            None
        );
    }
}
