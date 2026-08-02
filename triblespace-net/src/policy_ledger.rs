//! Monotone issuer policy as one author-scoped asserted event ledger.
//!
//! Incoming capability requests and the issuer's renewal policy are one
//! authority domain: approving a request is the same durable fact as issuing
//! the grant that the renewal daemon later maintains. This module represents
//! that domain as canonical `SimpleArchive` event values under one fixed pin
//! descriptor per assertion author. There is no scalar head, compare-and-swap,
//! mutable status, or negative tombstone.
//!
//! [`StrongPinDescriptor`] wraps the fixed inner descriptor so a retention
//! backend keeps locally present event and proof closure. Event labels are
//! canonical signed padding only; this kind has no ancestry relation and no
//! operation may compare them.

use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};

use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use triblespace_core::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::{Blake3, Handle};
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::repo::pin_assertion::{
    PinAssertion, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
};
use triblespace_core::repo::strong_pin::StrongPinDescriptor;
use triblespace_core::trible::{Fragment, TribleSet};

use crate::policy::{policy_scope, policy_subject, request_partial_cap, request_requester};

triblespace_core::prelude::attributes! {
    /// Team root whose founder anchor terminates a grant's verified proof.
    "CF48B211C9FCF5FAFA1AF2A35AC93799" as pub policy_team_root: triblespace_core::prelude::inlineencodings::ED25519PublicKey;
    /// Exact finite operational capability issued for a grant.
    "3E5D5BF44F5198CC71176A628C06A5C7" as pub policy_credential_cap: Handle<SimpleArchive>;
    /// Exact signature/proof blob accompanying `policy_credential_cap`.
    "1898D3D13786EEDCCDA79008EC2F1205" as pub policy_credential_sig: Handle<SimpleArchive>;
    /// Optional exact `RequestObserved` event used as issuance provenance.
    "6C293DE1077C76992DA7BD5F436A5368" as pub policy_request_event: Handle<SimpleArchive>;
}

/// Fixed V1 inner descriptor marker for one author's policy event ledger.
///
/// Minted with `trible genid` on 2026-08-02.
pub const POLICY_LEDGER_DESCRIPTOR_V1: [u8; 16] = [
    0x66, 0x7D, 0x00, 0xBE, 0x80, 0x4F, 0xEF, 0xC9, 0x7B, 0x16, 0xD3, 0x66, 0x1C, 0x57, 0x8A, 0xA1,
];

/// Canonical event-kind ids, minted with `trible genid` on 2026-08-02.
pub const EVENT_REQUEST_OBSERVED: Id =
    triblespace_core::id::id_hex!("F31E07D0864FAC0DA4834DB7F1D35DA2");
pub const EVENT_REQUEST_REJECTED: Id =
    triblespace_core::id::id_hex!("221E9AB596578983F7A23710FC93CB2E");
pub const EVENT_GRANT_ISSUED: Id =
    triblespace_core::id::id_hex!("54CB40BA2F44FF63D40A90746B516F2A");
pub const EVENT_CREDENTIAL_AUTHENTICATED: Id =
    triblespace_core::id::id_hex!("333A14195FBDE41C755394621D4D875F");
pub const EVENT_GRANT_DISABLED: Id =
    triblespace_core::id::id_hex!("B97B48D5DC12E304303357B6C2126E82");

/// Fixed inner descriptor for one author's complete issuer-policy event set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolicyLedgerDescriptor;

impl BlobEncoding for PolicyLedgerDescriptor {}

impl MetaDescribe for PolicyLedgerDescriptor {
    fn describe() -> Fragment {
        let id = triblespace_core::id::id_hex!("667D00BE804FEFC97B16D3661C578AA1");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "policy-ledger-descriptor-v1",
                metadata::description: "Fixed descriptor for one assertion author's monotone issuer-policy event ledger. Event values are canonical SimpleArchive fragments; a StrongPinDescriptor supplies hard retention.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl PolicyLedgerDescriptor {
    pub fn blob() -> Blob<Self> {
        Blob::new(Bytes::from_source(POLICY_LEDGER_DESCRIPTOR_V1.to_vec()))
    }

    pub fn descriptor_handle() -> Inline<Handle<Self>> {
        Inline::new(Blake3::digest(&POLICY_LEDGER_DESCRIPTOR_V1))
    }

    pub fn from_unknown_handle(handle: Inline<Handle<UnknownBlob>>) -> Inline<Handle<Self>> {
        Inline::new(handle.raw)
    }

    pub fn strong_blob() -> Blob<StrongPinDescriptor> {
        StrongPinDescriptor::blob(Self::descriptor_handle())
    }

    pub fn pin_handle() -> PinHandle {
        StrongPinDescriptor::pin_handle(Self::descriptor_handle())
    }

    pub fn pin_identity(author: VerifyingKey) -> PinIdentity {
        PinIdentity::new(author, Self::pin_handle())
    }
}

impl TryFromBlob<PolicyLedgerDescriptor> for PolicyLedgerDescriptor {
    type Error = PolicyLedgerDescriptorError;

    fn try_from_blob(blob: Blob<PolicyLedgerDescriptor>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != POLICY_LEDGER_DESCRIPTOR_V1.len() {
            return Err(PolicyLedgerDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes != POLICY_LEDGER_DESCRIPTOR_V1 {
            return Err(PolicyLedgerDescriptorError::WrongKind);
        }
        Ok(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyLedgerDescriptorError {
    WrongLength { actual: usize },
    WrongKind,
}

impl fmt::Display for PolicyLedgerDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "policy ledger descriptor is {actual} bytes, expected {}",
                POLICY_LEDGER_DESCRIPTOR_V1.len()
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 policy ledger"),
        }
    }
}

impl Error for PolicyLedgerDescriptorError {}

/// Exact authorization request identity. Wall-clock receipt time is
/// deliberately absent: replay of the same wire content is the same fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestIdentity {
    requester: [u8; 32],
    partial_cap: Inline<Handle<SimpleArchive>>,
}

impl RequestIdentity {
    pub fn new(requester: VerifyingKey, partial_cap: Inline<Handle<SimpleArchive>>) -> Self {
        Self {
            requester: requester.to_bytes(),
            partial_cap,
        }
    }

    pub fn requester(self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.requester)
            .expect("RequestIdentity is constructible only from a checked key")
    }

    pub const fn partial_cap(self) -> Inline<Handle<SimpleArchive>> {
        self.partial_cap
    }

    pub fn observed_blob(self) -> Blob<SimpleArchive> {
        PolicyEvent::RequestObserved(self).to_blob()
    }
}

/// Stable renewal-grant identity within one assertion author's namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GrantIdentity {
    team_root: [u8; 32],
    subject: [u8; 32],
    scope_root: Id,
}

impl GrantIdentity {
    pub fn new(team_root: VerifyingKey, subject: VerifyingKey, scope_root: Id) -> Self {
        Self {
            team_root: team_root.to_bytes(),
            subject: subject.to_bytes(),
            scope_root,
        }
    }

    pub fn team_root(self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.team_root)
            .expect("GrantIdentity is constructible only from a checked key")
    }

    pub fn subject(self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.subject)
            .expect("GrantIdentity is constructible only from a checked key")
    }

    pub const fn scope_root(self) -> Id {
        self.scope_root
    }
}

/// One canonical positive policy effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyEvent {
    RequestObserved(RequestIdentity),
    RequestRejected(RequestIdentity),
    GrantIssued {
        grant: GrantIdentity,
        cap: Inline<Handle<SimpleArchive>>,
        sig: Inline<Handle<SimpleArchive>>,
        request: Option<Inline<Handle<SimpleArchive>>>,
    },
    CredentialAuthenticated {
        grant: GrantIdentity,
        sig: Inline<Handle<SimpleArchive>>,
    },
    GrantDisabled(GrantIdentity),
}

impl PolicyEvent {
    fn fragment(self) -> Fragment {
        match self {
            Self::RequestObserved(request) => entity! {
                metadata::tag: EVENT_REQUEST_OBSERVED,
                request_requester: request.requester(),
                request_partial_cap: request.partial_cap(),
            },
            Self::RequestRejected(request) => entity! {
                metadata::tag: EVENT_REQUEST_REJECTED,
                request_requester: request.requester(),
                request_partial_cap: request.partial_cap(),
            },
            Self::GrantIssued {
                grant,
                cap,
                sig,
                request,
            } => entity! {
                metadata::tag: EVENT_GRANT_ISSUED,
                policy_team_root: grant.team_root(),
                policy_subject: grant.subject(),
                policy_scope: grant.scope_root(),
                policy_credential_cap: cap,
                policy_credential_sig: sig,
                policy_request_event?: request,
            },
            Self::CredentialAuthenticated { grant, sig } => entity! {
                metadata::tag: EVENT_CREDENTIAL_AUTHENTICATED,
                policy_team_root: grant.team_root(),
                policy_subject: grant.subject(),
                policy_scope: grant.scope_root(),
                policy_credential_sig: sig,
            },
            Self::GrantDisabled(grant) => entity! {
                metadata::tag: EVENT_GRANT_DISABLED,
                policy_team_root: grant.team_root(),
                policy_subject: grant.subject(),
                policy_scope: grant.scope_root(),
            },
        }
    }

    pub fn id(self) -> Id {
        self.fragment()
            .root()
            .expect("one policy event fragment exports one intrinsic id")
    }

    pub fn to_blob(self) -> Blob<SimpleArchive> {
        let set: TribleSet = self.fragment().into();
        set.to_blob()
    }

    pub fn handle(self) -> Inline<Handle<SimpleArchive>> {
        self.to_blob().get_handle()
    }

    /// Strictly decode one event and reject semantically equivalent alternate
    /// containers. Reconstructing and byte-comparing the canonical archive
    /// prevents irrelevant extra facts from minting endlessly many values.
    pub fn decode(blob: Blob<SimpleArchive>) -> Result<Self, PolicyEventError> {
        let set: TribleSet = TryFromBlob::try_from_blob(blob.clone())?;
        let mut tags = find!(
            (event: Id, kind: Id),
            pattern!(&set, [{ ?event @ metadata::tag: ?kind }])
        );
        let (event, kind) = exactly_one(&mut tags)?;

        let decoded = if kind == EVENT_REQUEST_OBSERVED || kind == EVENT_REQUEST_REJECTED {
            let requester = one_value(find!(
                requester: VerifyingKey,
                pattern!(&set, [{ event @ request_requester: ?requester }])
            ))?;
            let partial_cap = one_value(find!(
                cap: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ request_partial_cap: ?cap }])
            ))?;
            let request = RequestIdentity::new(requester, partial_cap);
            if kind == EVENT_REQUEST_OBSERVED {
                Self::RequestObserved(request)
            } else {
                Self::RequestRejected(request)
            }
        } else if kind == EVENT_GRANT_ISSUED {
            let grant = decode_grant(&set, event)?;
            let cap = one_value(find!(
                cap: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ policy_credential_cap: ?cap }])
            ))?;
            let sig = one_value(find!(
                sig: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ policy_credential_sig: ?sig }])
            ))?;
            let request = optional_one(find!(
                request: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ policy_request_event: ?request }])
            ))?;
            Self::GrantIssued {
                grant,
                cap,
                sig,
                request,
            }
        } else if kind == EVENT_CREDENTIAL_AUTHENTICATED {
            let grant = decode_grant(&set, event)?;
            let sig = one_value(find!(
                sig: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ policy_credential_sig: ?sig }])
            ))?;
            Self::CredentialAuthenticated { grant, sig }
        } else if kind == EVENT_GRANT_DISABLED {
            Self::GrantDisabled(decode_grant(&set, event)?)
        } else {
            return Err(PolicyEventError::UnknownKind(kind));
        };

        if decoded.to_blob().bytes != blob.bytes {
            return Err(PolicyEventError::NonCanonical);
        }
        Ok(decoded)
    }
}

fn decode_grant(set: &TribleSet, event: Id) -> Result<GrantIdentity, PolicyEventError> {
    let team_root = one_value(find!(
        root: VerifyingKey,
        pattern!(set, [{ event @ policy_team_root: ?root }])
    ))?;
    let subject = one_value(find!(
        subject: VerifyingKey,
        pattern!(set, [{ event @ policy_subject: ?subject }])
    ))?;
    let scope_root = one_value(find!(
        scope: Id,
        pattern!(set, [{ event @ policy_scope: ?scope }])
    ))?;
    Ok(GrantIdentity::new(team_root, subject, scope_root))
}

fn exactly_one<T>(iter: &mut impl Iterator<Item = T>) -> Result<T, PolicyEventError> {
    match (iter.next(), iter.next()) {
        (Some(value), None) => Ok(value),
        _ => Err(PolicyEventError::Malformed),
    }
}

fn one_value<T>(mut iter: impl Iterator<Item = T>) -> Result<T, PolicyEventError> {
    exactly_one(&mut iter)
}

fn optional_one<T>(mut iter: impl Iterator<Item = T>) -> Result<Option<T>, PolicyEventError> {
    match (iter.next(), iter.next()) {
        (None, None) => Ok(None),
        (Some(value), None) => Ok(Some(value)),
        _ => Err(PolicyEventError::Malformed),
    }
}

#[derive(Debug)]
pub enum PolicyEventError {
    Archive(UnarchiveError),
    Malformed,
    UnknownKind(Id),
    NonCanonical,
}

impl From<UnarchiveError> for PolicyEventError {
    fn from(value: UnarchiveError) -> Self {
        Self::Archive(value)
    }
}

impl fmt::Display for PolicyEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive(err) => write!(f, "policy event is not a SimpleArchive: {err}"),
            Self::Malformed => write!(f, "policy event has a missing or repeated field"),
            Self::UnknownKind(kind) => write!(f, "unknown policy event kind {kind:?}"),
            Self::NonCanonical => write!(f, "policy event contains non-canonical facts"),
        }
    }
}

impl Error for PolicyEventError {}

fn canonical_label() -> SubsumptionLabel {
    SubsumptionLabel::from_raw([0u8; 32])
}

/// Sign one canonical event value for the author's fixed policy ledger.
pub fn sign_policy_event(key: &SigningKey, event: PolicyEvent) -> PinAssertion {
    PinAssertion::sign(
        key,
        PolicyLedgerDescriptor::pin_handle(),
        ValueHandle::from_raw(event.handle().raw),
        canonical_label(),
    )
}

/// Reinterpret an asserted policy value as its canonical archive handle.
pub fn event_handle(value: ValueHandle) -> Inline<Handle<SimpleArchive>> {
    Inline::new(value.raw())
}

#[cfg(test)]
mod tests {
    use super::*;
    use triblespace_core::repo::pin_assertion::PinAssertionSnapshot;

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn handle(byte: u8) -> Inline<Handle<SimpleArchive>> {
        Inline::new([byte; 32])
    }

    fn request() -> RequestIdentity {
        RequestIdentity::new(key(2).verifying_key(), handle(3))
    }

    fn grant() -> GrantIdentity {
        GrantIdentity::new(
            key(4).verifying_key(),
            key(5).verifying_key(),
            triblespace_core::id::id_hex!("00112233445566778899AABBCCDDEEFF"),
        )
    }

    #[test]
    fn fixed_inner_and_outer_descriptors_are_canonical_content() {
        let inner = PolicyLedgerDescriptor::blob();
        assert_eq!(
            inner.get_handle(),
            PolicyLedgerDescriptor::descriptor_handle()
        );
        assert_eq!(
            inner
                .clone()
                .try_from_blob::<PolicyLedgerDescriptor>()
                .unwrap(),
            PolicyLedgerDescriptor
        );
        let outer = PolicyLedgerDescriptor::strong_blob();
        assert_eq!(
            outer.get_handle().raw,
            PolicyLedgerDescriptor::pin_handle().raw()
        );
    }

    #[test]
    fn all_event_variants_roundtrip_exact_canonical_archives() {
        let observed = PolicyEvent::RequestObserved(request());
        let events = [
            observed,
            PolicyEvent::RequestRejected(request()),
            PolicyEvent::GrantIssued {
                grant: grant(),
                cap: handle(6),
                sig: handle(7),
                request: Some(observed.handle()),
            },
            PolicyEvent::CredentialAuthenticated {
                grant: grant(),
                sig: handle(7),
            },
            PolicyEvent::GrantDisabled(grant()),
        ];

        for event in events {
            let blob = event.to_blob();
            assert_eq!(PolicyEvent::decode(blob).unwrap(), event);
        }
    }

    #[test]
    fn event_identity_ignores_replay_time_because_time_is_not_encoded() {
        let first = PolicyEvent::RequestObserved(request());
        let replay = PolicyEvent::RequestObserved(request());
        assert_eq!(first.id(), replay.id());
        assert_eq!(first.handle(), replay.handle());
    }

    #[test]
    fn canonical_decoder_rejects_irrelevant_extra_facts() {
        let event = PolicyEvent::RequestObserved(request());
        let mut set: TribleSet = TryFromBlob::try_from_blob(event.to_blob()).unwrap();
        let unrelated = triblespace_core::id::ufoid();
        set += TribleSet::from(entity! {
            ExclusiveId::force_ref(&unrelated) @ metadata::tag: EVENT_REQUEST_REJECTED,
        });
        assert!(matches!(
            PolicyEvent::decode(set.to_blob()),
            Err(PolicyEventError::Malformed | PolicyEventError::NonCanonical)
        ));
    }

    #[test]
    fn signing_uses_one_author_scoped_strong_pin_identity() {
        let author = key(8);
        let other = key(9);
        let first = sign_policy_event(&author, PolicyEvent::RequestObserved(request()));
        let second = sign_policy_event(&author, PolicyEvent::GrantDisabled(grant()));
        let foreign = sign_policy_event(&other, PolicyEvent::RequestObserved(request()));
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(first).unwrap();
        snapshot.insert(second).unwrap();
        snapshot.insert(foreign).unwrap();

        assert_eq!(
            snapshot
                .for_pin(&PolicyLedgerDescriptor::pin_identity(
                    author.verifying_key()
                ))
                .len(),
            2
        );
        assert_eq!(
            snapshot
                .for_pin(&PolicyLedgerDescriptor::pin_identity(other.verifying_key()))
                .len(),
            1
        );
    }
}
