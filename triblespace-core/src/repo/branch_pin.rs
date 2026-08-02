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
//! — is licensed *here*, by this kind, and only because commit depth is
//! strictly increasing along ancestry: if A is a strict ancestor of B then
//! every path from A reaches B through at least one edge, so
//! `depth(A) < depth(B)`. Equal depth therefore rules out strict ancestry
//! between distinct commits, which is why `>=` rather than `>` is the correct
//! test after identical values have been grouped.
//!
//! A kind that cannot make an argument of this shape supplies no label
//! comparison at all and takes zero skips — degraded, never wrong.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use hex_literal::hex;

use super::branch_frontier::{ParentLookup, PartialCommitDag};
use super::pin_assertion::{PinAssertion, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle};
use super::CommitHandle;
use crate::blob::encodings::longstring::LongString;
use crate::blob::{Blob, BlobEncoding, TryFromBlob};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::encodings::hash::{Blake3, Handle};
use crate::inline::Inline;
use crate::macros::entity;
use crate::metadata::{self, MetaDescribe};
use crate::trible::Fragment;
use ed25519_dalek::SigningKey;

/// Canonical byte length of a V1 branch-pin descriptor.
pub const BRANCH_PIN_DESCRIPTOR_LEN: usize = 16 + 32;

/// Kind/schema marker for the V1 branch-pin descriptor.
///
/// Minted with `trible genid` on 2026-08-02. Keeping the kind inside the
/// content-addressed descriptor prevents a branch name handle from aliasing a
/// different pin kind that happens to use the same 32-byte value.
pub const BRANCH_PIN_DESCRIPTOR_V1: [u8; 16] = hex!("58DD2D159FA741F73DD0CE5A0E2F161F");

/// Blob encoding for the exact identity descriptor of a branch pin.
///
/// The bytes are `kind marker [16] | LongString name handle [32]`. The generic
/// assertion layer sees only their Blake3 handle; this typed adapter can derive
/// that handle from a known name without loading the descriptor, while branch
/// enumeration can decode the blob when it is present.
pub struct BranchPinDescriptor;

impl BlobEncoding for BranchPinDescriptor {}

impl MetaDescribe for BranchPinDescriptor {
    fn describe() -> Fragment {
        // The schema id is also the canonical in-band V1 kind marker. A format
        // change therefore mints a new schema id instead of growing a hidden
        // version switch inside this descriptor.
        let id: Id = id_hex!("58DD2D159FA741F73DD0CE5A0E2F161F");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "branch-pin-descriptor-v1",
                metadata::description: "Canonical descriptor for an asserted branch pin: a V1 kind marker followed by the LongString handle of the branch name. Its content handle namespaces branch identities away from every other pin kind.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl BranchPinDescriptor {
    /// Encode one branch name into its canonical typed descriptor bytes.
    pub fn encode(name: Inline<Handle<LongString>>) -> [u8; BRANCH_PIN_DESCRIPTOR_LEN] {
        let mut raw = [0u8; BRANCH_PIN_DESCRIPTOR_LEN];
        raw[..16].copy_from_slice(&BRANCH_PIN_DESCRIPTOR_V1);
        raw[16..].copy_from_slice(&name.raw);
        raw
    }

    /// Build the descriptor blob that a repository stages beside the name.
    pub fn blob(name: Inline<Handle<LongString>>) -> Blob<Self> {
        Blob::new(Bytes::from_source(Self::encode(name).to_vec()))
    }

    /// Derive the generic pin handle without requiring descriptor content to be
    /// present in a store.
    pub fn pin_handle(name: Inline<Handle<LongString>>) -> PinHandle {
        PinHandle::from_raw(Blake3::digest(&Self::encode(name)))
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
        if bytes[..16] != BRANCH_PIN_DESCRIPTOR_V1 {
            return Err(BranchPinDescriptorError::WrongKind);
        }
        Ok(Inline::new(bytes[16..].try_into().expect("length checked")))
    }
}

/// A branch-pin descriptor was not the one exact canonical V1 shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BranchPinDescriptorError {
    /// The descriptor was not exactly 48 bytes.
    WrongLength { actual: usize },
    /// The descriptor did not carry the branch V1 kind marker.
    WrongKind,
}

impl fmt::Display for BranchPinDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "branch pin descriptor is {actual} bytes, expected {BRANCH_PIN_DESCRIPTOR_LEN}"
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 branch descriptor"),
        }
    }
}

impl Error for BranchPinDescriptorError {}

/// Reinterpret a generic assertion value as a branch commit handle.
pub(crate) fn commit_from_value(value: ValueHandle) -> CommitHandle {
    Inline::new(value.raw())
}

/// Reinterpret one exact branch commit as the generic assertion value.
pub(crate) fn value_from_commit(commit: CommitHandle) -> ValueHandle {
    ValueHandle::from_raw(commit.raw)
}

/// Sign one typed branch assertion in the generic envelope.
///
/// The caller must have computed `depth` from the complete commit DAG; this
/// adapter only owns its canonical encoding. Publication code stages
/// [`BranchPinDescriptor::blob`] before durably appending the returned record.
pub fn sign_branch_assertion(
    key: &SigningKey,
    name: Inline<Handle<LongString>>,
    commit: CommitHandle,
    depth: u64,
) -> PinAssertion {
    PinAssertion::sign(
        key,
        BranchPinDescriptor::pin_handle(name),
        value_from_commit(commit),
        depth_label(depth),
    )
}

/// Encode a commit depth as this kind's subsumption label.
///
/// Big-endian in the leading 8 bytes, zero tail. Big-endian is **required**:
/// the store compares labels bytewise, so a little-endian encoding would order
/// by low byte first and disagree with numeric order — silently, and in the
/// unsound direction, since it would license skips that ancestry does not
/// justify. The trailing 24 bytes stay zero and are available to a future
/// composite (depth then tiebreaker) that remains totally ordered by the same
/// comparison, with no change to the store.
pub fn depth_label(depth: u64) -> SubsumptionLabel {
    let mut raw = [0u8; 32];
    raw[..8].copy_from_slice(&depth.to_be_bytes());
    SubsumptionLabel::from_raw(raw)
}

/// Compute the canonical longest-path depth of one complete commit DAG.
///
/// Roots have depth zero and every other commit has
/// `1 + max(parent depths)`. This is iterative rather than recursive so an
/// adversarially deep but valid history cannot overflow the Rust call stack.
/// Publication must finish this walk before signing: a workspace counter is
/// insufficient because callers may set or merge arbitrary imported heads.
pub fn commit_depth<D: PartialCommitDag>(
    dag: &mut D,
    tip: CommitHandle,
) -> Result<u64, CommitDepthError<D::Error>> {
    #[derive(Clone, Copy)]
    enum Visit {
        Enter(CommitHandle),
        Exit(CommitHandle),
    }

    let mut stack = vec![Visit::Enter(tip)];
    let mut active = HashSet::new();
    let mut parents = HashMap::<CommitHandle, Vec<CommitHandle>>::new();
    let mut depths = HashMap::<CommitHandle, u64>::new();

    while let Some(visit) = stack.pop() {
        match visit {
            Visit::Enter(commit) => {
                if depths.contains_key(&commit) {
                    continue;
                }
                if !active.insert(commit) {
                    return Err(CommitDepthError::Cycle { commit });
                }
                let ParentLookup::Present(mut direct) =
                    dag.parents(commit).map_err(CommitDepthError::Lookup)?
                else {
                    return Err(CommitDepthError::Missing { commit });
                };
                direct.sort_unstable_by_key(|parent| parent.raw);
                direct.dedup();
                parents.insert(commit, direct.clone());
                stack.push(Visit::Exit(commit));
                for parent in direct.into_iter().rev() {
                    if active.contains(&parent) {
                        return Err(CommitDepthError::Cycle { commit: parent });
                    }
                    if !depths.contains_key(&parent) {
                        stack.push(Visit::Enter(parent));
                    }
                }
            }
            Visit::Exit(commit) => {
                active.remove(&commit);
                let direct = parents
                    .get(&commit)
                    .expect("every exiting commit was entered");
                let depth = match direct
                    .iter()
                    .map(|parent| {
                        depths
                            .get(parent)
                            .copied()
                            .expect("a parent exits before its child")
                    })
                    .max()
                {
                    None => 0,
                    Some(parent_depth) => parent_depth
                        .checked_add(1)
                        .ok_or(CommitDepthError::Overflow { commit })?,
                };
                depths.insert(commit, depth);
            }
        }
    }

    Ok(*depths
        .get(&tip)
        .expect("the requested tip exits before traversal completes"))
}

/// A commit cannot receive a sound branch depth label yet.
#[derive(Debug)]
pub enum CommitDepthError<E> {
    /// Reading local commit metadata failed for a non-absence reason.
    Lookup(E),
    /// A commit needed for the depth proof is not present locally.
    Missing { commit: CommitHandle },
    /// The supplied parent relation contains a cycle.
    Cycle { commit: CommitHandle },
    /// The longest path cannot be represented in the label's leading `u64`.
    Overflow { commit: CommitHandle },
}

impl<E> fmt::Display for CommitDepthError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lookup(_) => write!(f, "commit metadata lookup failed while computing depth"),
            Self::Missing { commit } => {
                write!(f, "commit {commit:?} is missing while computing depth")
            }
            Self::Cycle { commit } => {
                write!(f, "commit parent cycle reaches {commit:?}")
            }
            Self::Overflow { commit } => {
                write!(f, "commit depth overflows u64 at {commit:?}")
            }
        }
    }
}

impl<E> Error for CommitDepthError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lookup(error) => Some(error),
            Self::Missing { .. } | Self::Cycle { .. } | Self::Overflow { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use std::convert::Infallible;

    fn name(byte: u8) -> Inline<Handle<LongString>> {
        Inline::new([byte; 32])
    }

    #[test]
    fn descriptor_is_canonical_typed_content_and_roundtrips() {
        let name = name(7);
        let blob = BranchPinDescriptor::blob(name);
        assert_eq!(blob.bytes.len(), BRANCH_PIN_DESCRIPTOR_LEN);
        assert_eq!(&blob.bytes[..16], &BRANCH_PIN_DESCRIPTOR_V1);
        assert_eq!(&blob.bytes[16..], &name.raw);
        assert_eq!(
            BranchPinDescriptor::pin_handle(name).raw(),
            blob.get_handle().raw,
            "the generic pin identity is the descriptor's exact content handle"
        );
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
    fn branch_values_are_exact_commit_handles() {
        let raw = [19; 32];
        assert_eq!(commit_from_value(ValueHandle::from_raw(raw)).raw, raw);
    }

    #[test]
    fn typed_signing_uses_the_descriptor_commit_and_depth_codec() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let name = name(11);
        let commit = Inline::new([19; 32]);
        let assertion = sign_branch_assertion(&key, name, commit, 23);
        assert_eq!(
            assertion.identity(),
            &BranchPinDescriptor::pin_identity(key.verifying_key(), name)
        );
        assert_eq!(assertion.value(), value_from_commit(commit));
        assert_eq!(assertion.label(), depth_label(23));
    }

    #[derive(Default)]
    struct TestDag(HashMap<CommitHandle, Vec<CommitHandle>>);

    impl PartialCommitDag for TestDag {
        type Error = Infallible;

        fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error> {
            Ok(match self.0.get(&commit) {
                Some(parents) => ParentLookup::Present(parents.clone()),
                None => ParentLookup::Missing,
            })
        }
    }

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    #[test]
    fn commit_depth_is_longest_path_and_merge_is_max_parent_plus_one() {
        let root = commit(1);
        let short = commit(2);
        let long = commit(3);
        let long_tip = commit(4);
        let merge = commit(5);
        let mut dag = TestDag(HashMap::from([
            (root, vec![]),
            (short, vec![root]),
            (long, vec![root]),
            (long_tip, vec![long]),
            // Duplicate parents are semantically one edge and do not alter
            // the longest-path definition.
            (merge, vec![short, long_tip, short]),
        ]));
        assert_eq!(commit_depth(&mut dag, root).unwrap(), 0);
        assert_eq!(commit_depth(&mut dag, short).unwrap(), 1);
        assert_eq!(commit_depth(&mut dag, long_tip).unwrap(), 2);
        assert_eq!(commit_depth(&mut dag, merge).unwrap(), 3);
    }

    #[test]
    fn commit_depth_refuses_missing_ancestry_and_cycles() {
        let tip = commit(9);
        let missing = commit(8);
        let mut incomplete = TestDag(HashMap::from([(tip, vec![missing])]));
        assert!(matches!(
            commit_depth(&mut incomplete, tip),
            Err(CommitDepthError::Missing { commit }) if commit == missing
        ));

        let a = commit(10);
        let b = commit(11);
        let mut cyclic = TestDag(HashMap::from([(a, vec![b]), (b, vec![a])]));
        assert!(matches!(
            commit_depth(&mut cyclic, a),
            Err(CommitDepthError::Cycle { .. })
        ));
    }

    #[test]
    fn depth_label_is_monotone_under_the_stores_bytewise_order() {
        let mut prev = depth_label(0);
        for d in [1u64, 2, 255, 256, 65_535, 65_536, 1 << 40, u64::MAX] {
            let cur = depth_label(d);
            assert!(cur > prev, "depth {d} did not increase bytewise");
            prev = cur;
        }
    }

    /// NEGATIVE CONTROL for the encoding obligation.
    ///
    /// Little-endian still yields a total order, so a positive-only test would
    /// pass — the order merely disagrees with numeric order, which breaks
    /// monotonicity in the UNSOUND direction: it licenses skips ancestry does
    /// not justify. This fails if anyone "simplifies" `depth_label` to native
    /// or little-endian bytes.
    #[test]
    fn a_little_endian_encoding_would_not_be_monotone() {
        let le = |d: u64| {
            let mut raw = [0u8; 32];
            raw[..8].copy_from_slice(&d.to_le_bytes());
            SubsumptionLabel::from_raw(raw)
        };
        assert!(le(256) < le(1), "little-endian must misorder 1 vs 256");
        assert!(
            depth_label(256) > depth_label(1),
            "big-endian must order 1 < 256"
        );
    }

    /// The tail is reserved for a composite that keeps the same total order.
    #[test]
    fn depth_occupies_the_leading_eight_bytes_and_leaves_the_tail_free() {
        let l = depth_label(9);
        assert_eq!(l.raw()[..8], 9u64.to_be_bytes());
        assert!(l.raw()[8..].iter().all(|b| *b == 0));
    }
}
