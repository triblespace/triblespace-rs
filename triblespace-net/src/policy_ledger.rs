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

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use anybytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hifitime::Epoch;

use triblespace_core::blob::encodings::UnknownBlob;
use triblespace_core::blob::encodings::simplearchive::{SimpleArchive, UnarchiveError};
use triblespace_core::blob::{Blob, BlobEncoding, IntoBlob, TryFromBlob};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::Inline;
use triblespace_core::inline::encodings::hash::{Blake3, Handle};
use triblespace_core::macros::{entity, find, pattern};
use triblespace_core::metadata::{self, MetaDescribe};
use triblespace_core::repo::capability::{
    VerifiedCapability, VerifyError, decode_operational_capability, scope_subsumes,
    verify_chain_details_allow_expired,
};
use triblespace_core::repo::pin_assertion::{
    PinAssertion, PinAssertionSnapshot, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
};
use triblespace_core::repo::strong_pin::StrongPinDescriptor;
use triblespace_core::trible::{Fragment, TribleSet};

use crate::policy::{policy_scope, policy_subject, request_partial_cap, request_requester};

triblespace_core::prelude::attributes! {
    /// Team root whose founder anchor terminates a grant's verified proof.
    "CF48B211C9FCF5FAFA1AF2A35AC93799" as pub policy_team_root: triblespace_core::prelude::inlineencodings::ED25519PublicKey;
    /// Exact canonical signature/proof blob issued for a grant. The signature
    /// itself names the finite operational capability it authenticates.
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
                sig,
                request,
            } => entity! {
                metadata::tag: EVENT_GRANT_ISSUED,
                policy_team_root: grant.team_root(),
                policy_subject: grant.subject(),
                policy_scope: grant.scope_root(),
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

/// Typed result of reducing one author's complete policy assertion set.
///
/// Only Complete exposes an operational view. Missing content and known-invalid
/// evidence are global fail-closed states for this deliberately coarse first
/// ledger layout.
#[derive(Debug)]
pub enum PolicyLedgerResolution {
    Complete(PolicyLedgerView),
    Incomplete {
        missing: Vec<Inline<Handle<SimpleArchive>>>,
    },
    Invalid {
        diagnostics: Vec<PolicyLedgerDiagnostic>,
    },
}

/// Deterministic diagnostic for one present but invalid policy effect.
#[derive(Debug)]
pub enum PolicyLedgerDiagnostic {
    HandleMismatch {
        expected: Inline<Handle<SimpleArchive>>,
        actual: Inline<Handle<SimpleArchive>>,
    },
    InvalidEvent {
        handle: Inline<Handle<SimpleArchive>>,
        error: PolicyEventError,
    },
    ProvenanceIsNotRequest {
        handle: Inline<Handle<SimpleArchive>>,
    },
    InvalidRequest {
        event: Inline<Handle<SimpleArchive>>,
        request: RequestIdentity,
        reason: InvalidRequestReason,
    },
    InvalidIssuance {
        event: Inline<Handle<SimpleArchive>>,
        grant: GrantIdentity,
        sig: Inline<Handle<SimpleArchive>>,
        reason: InvalidIssuanceReason,
    },
}

#[derive(Debug)]
pub enum InvalidRequestReason {
    Claim(VerifyError),
    SubjectMismatch { claim: VerifyingKey },
    IssuerMismatch { claim: VerifyingKey },
}

#[derive(Debug)]
pub enum InvalidIssuanceReason {
    Proof(VerifyError),
    IssuerMismatch { proof: VerifyingKey },
    ScopeMismatch { proof: Id },
    RequesterMismatch { request: VerifyingKey },
    RequestScopeMismatch { request: Id },
    ExceedsRequestedScope,
    ExceedsRequestedExpiry { requested: Epoch, issued: Epoch },
}

/// Complete deterministic projection of one author's policy effects.
#[derive(Debug)]
pub struct PolicyLedgerView {
    author: VerifyingKey,
    requests: BTreeMap<RequestIdentity, RequestView>,
    grants: BTreeMap<GrantIdentity, GrantView>,
}

impl PolicyLedgerView {
    pub fn author(&self) -> VerifyingKey {
        self.author
    }

    pub fn requests(&self) -> &BTreeMap<RequestIdentity, RequestView> {
        &self.requests
    }

    pub fn grants(&self) -> &BTreeMap<GrantIdentity, GrantView> {
        &self.grants
    }
}

/// Independent positive facts about one exact request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestView {
    observed: bool,
    rejected: bool,
    issued_signatures: BTreeSet<Inline<Handle<SimpleArchive>>>,
}

impl RequestView {
    pub const fn observed(&self) -> bool {
        self.observed
    }

    pub const fn rejected(&self) -> bool {
        self.rejected
    }

    pub fn issued_signatures(&self) -> &BTreeSet<Inline<Handle<SimpleArchive>>> {
        &self.issued_signatures
    }

    pub fn is_pending(&self) -> bool {
        self.observed && !self.rejected && self.issued_signatures.is_empty()
    }
}

/// Derived state for one stable team, subject, and scope grant identity.
#[derive(Debug)]
pub struct GrantView {
    disabled: bool,
    issuance: GrantIssuanceResolution,
}

impl GrantView {
    pub const fn disabled(&self) -> bool {
        self.disabled
    }

    pub fn issuance(&self) -> &GrantIssuanceResolution {
        &self.issuance
    }

    /// Return the selected credential only when this grant remains active.
    ///
    /// Disabled grants retain their historical issuance for inspection, but
    /// must never be dispatched, installed, or renewed by operational callers.
    pub fn active_current(&self) -> Option<&CurrentGrant> {
        if self.disabled {
            return None;
        }
        match &self.issuance {
            GrantIssuanceResolution::Current(current) => Some(current),
            GrantIssuanceResolution::Unissued | GrantIssuanceResolution::Conflicted { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum GrantIssuanceResolution {
    Unissued,
    Current(CurrentGrant),
    Conflicted {
        signatures: BTreeSet<Inline<Handle<SimpleArchive>>>,
    },
}

/// Deterministically selected credential for one semantically coherent grant.
#[derive(Clone, Debug)]
pub struct CurrentGrant {
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
    capability: VerifiedCapability,
    authenticated: bool,
}

impl CurrentGrant {
    pub const fn cap(&self) -> Inline<Handle<SimpleArchive>> {
        self.cap
    }

    pub const fn sig(&self) -> Inline<Handle<SimpleArchive>> {
        self.sig
    }

    pub fn capability(&self) -> &VerifiedCapability {
        &self.capability
    }

    pub fn effective_expiry(&self) -> Epoch {
        self.capability.expires_at()
    }

    pub const fn authenticated(&self) -> bool {
        self.authenticated
    }
}

#[derive(Clone, Copy, Debug)]
struct AssertedIssuance {
    event: Inline<Handle<SimpleArchive>>,
    grant: GrantIdentity,
    sig: Inline<Handle<SimpleArchive>>,
    request: Option<Inline<Handle<SimpleArchive>>>,
}

#[derive(Debug)]
struct ValidIssuance {
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
    request: Option<RequestIdentity>,
    capability: VerifiedCapability,
}

#[derive(Default)]
struct GrantAccumulator {
    disabled: bool,
    authentications: BTreeSet<Inline<Handle<SimpleArchive>>>,
    issuances: Vec<ValidIssuance>,
}

/// Reduce one exact author's monotone policy assertion set.
///
/// The fetch callback is memoized for the duration of the fold, including
/// negative lookups, so one resolution observes a coherent content boundary.
pub fn resolve_policy_ledger<F>(
    snapshot: &PinAssertionSnapshot,
    author: VerifyingKey,
    mut fetch_blob: F,
) -> PolicyLedgerResolution
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let identity = PolicyLedgerDescriptor::pin_identity(author);
    let asserted_handles: BTreeSet<_> = snapshot
        .for_pin(&identity)
        .into_iter()
        .map(|assertion| event_handle(assertion.value()))
        .collect();

    let mut cache = BTreeMap::new();
    let mut missing = BTreeSet::new();
    let mut diagnostics = Vec::new();
    let mut asserted_events = Vec::new();

    for handle in asserted_handles {
        match read_event(
            handle,
            &mut cache,
            &mut fetch_blob,
            &mut missing,
            &mut diagnostics,
        ) {
            Some(event) => asserted_events.push((handle, event)),
            None => {}
        }
    }

    let provenance_handles: BTreeSet<_> = asserted_events
        .iter()
        .filter_map(|(_, event)| match event {
            PolicyEvent::GrantIssued {
                request: Some(handle),
                ..
            } => Some(*handle),
            _ => None,
        })
        .collect();

    let mut request_events = BTreeMap::new();
    for (handle, event) in &asserted_events {
        if let PolicyEvent::RequestObserved(request) = event {
            request_events.insert(*handle, *request);
        }
    }
    for handle in provenance_handles {
        if request_events.contains_key(&handle) {
            continue;
        }
        if let Some(event) = read_event(
            handle,
            &mut cache,
            &mut fetch_blob,
            &mut missing,
            &mut diagnostics,
        ) {
            match event {
                PolicyEvent::RequestObserved(request) => {
                    request_events.insert(handle, request);
                }
                _ => diagnostics.push(PolicyLedgerDiagnostic::ProvenanceIsNotRequest { handle }),
            }
        }
    }

    let mut request_claims = BTreeMap::new();
    for (event, request) in &request_events {
        let Some(blob) = read_blob(
            request.partial_cap(),
            &mut cache,
            &mut fetch_blob,
            &mut missing,
            &mut diagnostics,
        ) else {
            continue;
        };
        match decode_operational_capability(blob) {
            Err(error) => diagnostics.push(PolicyLedgerDiagnostic::InvalidRequest {
                event: *event,
                request: *request,
                reason: InvalidRequestReason::Claim(error),
            }),
            Ok(claim) if claim.subject != request.requester() => {
                diagnostics.push(PolicyLedgerDiagnostic::InvalidRequest {
                    event: *event,
                    request: *request,
                    reason: InvalidRequestReason::SubjectMismatch {
                        claim: claim.subject,
                    },
                });
            }
            Ok(claim) if claim.issuer != author => {
                diagnostics.push(PolicyLedgerDiagnostic::InvalidRequest {
                    event: *event,
                    request: *request,
                    reason: InvalidRequestReason::IssuerMismatch {
                        claim: claim.issuer,
                    },
                });
            }
            Ok(claim) => {
                request_claims.insert(*event, (*request, claim));
            }
        }
    }

    let mut asserted_issuances = Vec::new();
    let mut disabled = BTreeSet::new();
    let mut authentications =
        BTreeMap::<GrantIdentity, BTreeSet<Inline<Handle<SimpleArchive>>>>::new();
    let mut rejected = BTreeSet::new();

    for (event_handle, event) in &asserted_events {
        match *event {
            PolicyEvent::RequestObserved(_) => {}
            PolicyEvent::RequestRejected(request) => {
                rejected.insert(request);
            }
            PolicyEvent::GrantIssued {
                grant,
                sig,
                request,
            } => asserted_issuances.push(AssertedIssuance {
                event: *event_handle,
                grant,
                sig,
                request,
            }),
            PolicyEvent::CredentialAuthenticated { grant, sig } => {
                // Keep exact authentication as a latent positive fact. It is
                // consulted only for a matching selected valid issuance below,
                // so out-of-order replication is harmless and order-neutral.
                authentications.entry(grant).or_default().insert(sig);
            }
            PolicyEvent::GrantDisabled(grant) => {
                disabled.insert(grant);
            }
        }
    }

    let mut valid_issuances = Vec::new();
    for issuance in asserted_issuances {
        let verified = match verify_chain_details_allow_expired(
            issuance.grant.team_root(),
            issuance.sig,
            issuance.grant.subject(),
            |handle| {
                read_blob(
                    handle,
                    &mut cache,
                    &mut fetch_blob,
                    &mut missing,
                    &mut diagnostics,
                )
            },
        ) {
            Err(VerifyError::MissingBlob(handle)) => {
                missing.insert(handle);
                continue;
            }
            Err(error) => {
                diagnostics.push(PolicyLedgerDiagnostic::InvalidIssuance {
                    event: issuance.event,
                    grant: issuance.grant,
                    sig: issuance.sig,
                    reason: InvalidIssuanceReason::Proof(error),
                });
                continue;
            }
            Ok(verified) => verified,
        };

        if verified.leaf_issuer != author {
            diagnostics.push(PolicyLedgerDiagnostic::InvalidIssuance {
                event: issuance.event,
                grant: issuance.grant,
                sig: issuance.sig,
                reason: InvalidIssuanceReason::IssuerMismatch {
                    proof: verified.leaf_issuer,
                },
            });
            continue;
        }
        if verified.capability.scope_root != issuance.grant.scope_root() {
            diagnostics.push(PolicyLedgerDiagnostic::InvalidIssuance {
                event: issuance.event,
                grant: issuance.grant,
                sig: issuance.sig,
                reason: InvalidIssuanceReason::ScopeMismatch {
                    proof: verified.capability.scope_root,
                },
            });
            continue;
        }

        let request = if let Some(request_handle) = issuance.request {
            let Some((request, claim)) = request_claims.get(&request_handle) else {
                continue;
            };
            let invalid_reason = if request.requester() != issuance.grant.subject() {
                Some(InvalidIssuanceReason::RequesterMismatch {
                    request: request.requester(),
                })
            } else if claim.scope_root != issuance.grant.scope_root() {
                Some(InvalidIssuanceReason::RequestScopeMismatch {
                    request: claim.scope_root,
                })
            } else if !scope_subsumes(
                &claim.cap_set,
                claim.scope_root,
                &verified.capability.cap_set,
                verified.capability.scope_root,
            ) {
                Some(InvalidIssuanceReason::ExceedsRequestedScope)
            } else if verified.capability.expires_at() > claim.expires_at {
                Some(InvalidIssuanceReason::ExceedsRequestedExpiry {
                    requested: claim.expires_at,
                    issued: verified.capability.expires_at(),
                })
            } else {
                None
            };
            if let Some(reason) = invalid_reason {
                diagnostics.push(PolicyLedgerDiagnostic::InvalidIssuance {
                    event: issuance.event,
                    grant: issuance.grant,
                    sig: issuance.sig,
                    reason,
                });
                continue;
            }
            Some(*request)
        } else {
            None
        };

        valid_issuances.push((
            issuance.grant,
            ValidIssuance {
                cap: verified.leaf_cap,
                sig: issuance.sig,
                request,
                capability: verified.capability,
            },
        ));
    }

    if !diagnostics.is_empty() {
        return PolicyLedgerResolution::Invalid { diagnostics };
    }
    if !missing.is_empty() {
        return PolicyLedgerResolution::Incomplete {
            missing: missing.into_iter().collect(),
        };
    }

    let mut requests = BTreeMap::<RequestIdentity, RequestView>::new();
    for request in request_events.values() {
        requests.entry(*request).or_default().observed = true;
    }
    for request in rejected {
        requests.entry(request).or_default().rejected = true;
    }

    let mut grants = BTreeMap::<GrantIdentity, GrantAccumulator>::new();
    for grant in disabled {
        grants.entry(grant).or_default().disabled = true;
    }
    for (grant, signatures) in authentications {
        grants.entry(grant).or_default().authentications = signatures;
    }
    for (grant, issuance) in valid_issuances {
        if let Some(request) = issuance.request {
            requests
                .entry(request)
                .or_default()
                .issued_signatures
                .insert(issuance.sig);
        }
        grants.entry(grant).or_default().issuances.push(issuance);
    }

    let grants = grants
        .into_iter()
        .map(|(grant, accumulator)| {
            let valid_signatures = accumulator
                .issuances
                .iter()
                .map(|issuance| issuance.sig)
                .collect::<BTreeSet<_>>();
            let issuance = if accumulator.issuances.is_empty() {
                GrantIssuanceResolution::Unissued
            } else if accumulator.issuances.iter().skip(1).any(|candidate| {
                !same_scope_facts(&accumulator.issuances[0].capability, &candidate.capability)
            }) {
                GrantIssuanceResolution::Conflicted {
                    signatures: valid_signatures,
                }
            } else {
                let current = accumulator
                    .issuances
                    .into_iter()
                    .max_by(compare_issuances)
                    .expect("nonempty issuance set has a maximum");
                let authenticated = accumulator.authentications.contains(&current.sig);
                GrantIssuanceResolution::Current(CurrentGrant {
                    cap: current.cap,
                    sig: current.sig,
                    capability: current.capability,
                    authenticated,
                })
            };
            (
                grant,
                GrantView {
                    disabled: accumulator.disabled,
                    issuance,
                },
            )
        })
        .collect();

    PolicyLedgerResolution::Complete(PolicyLedgerView {
        author,
        requests,
        grants,
    })
}

fn cached_fetch<F>(
    cache: &mut BTreeMap<Inline<Handle<SimpleArchive>>, Option<Blob<SimpleArchive>>>,
    fetch_blob: &mut F,
    handle: Inline<Handle<SimpleArchive>>,
) -> Option<Blob<SimpleArchive>>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    if let Some(blob) = cache.get(&handle) {
        return blob.clone();
    }
    let blob = fetch_blob(handle);
    cache.insert(handle, blob.clone());
    blob
}

fn read_blob<F>(
    handle: Inline<Handle<SimpleArchive>>,
    cache: &mut BTreeMap<Inline<Handle<SimpleArchive>>, Option<Blob<SimpleArchive>>>,
    fetch_blob: &mut F,
    missing: &mut BTreeSet<Inline<Handle<SimpleArchive>>>,
    diagnostics: &mut Vec<PolicyLedgerDiagnostic>,
) -> Option<Blob<SimpleArchive>>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let Some(blob) = cached_fetch(cache, fetch_blob, handle) else {
        missing.insert(handle);
        return None;
    };
    // This callback is a public trust boundary. Do not trust `Blob`'s cached
    // handle: `Blob::with_handle` exists for already-verified store reads and
    // a custom fetcher can otherwise pair arbitrary bytes with `handle`.
    let actual: Inline<Handle<SimpleArchive>> = Inline::new(Blake3::digest(&blob.bytes));
    if actual != handle {
        diagnostics.push(PolicyLedgerDiagnostic::HandleMismatch {
            expected: handle,
            actual,
        });
        return None;
    }
    Some(blob)
}

fn read_event<F>(
    handle: Inline<Handle<SimpleArchive>>,
    cache: &mut BTreeMap<Inline<Handle<SimpleArchive>>, Option<Blob<SimpleArchive>>>,
    fetch_blob: &mut F,
    missing: &mut BTreeSet<Inline<Handle<SimpleArchive>>>,
    diagnostics: &mut Vec<PolicyLedgerDiagnostic>,
) -> Option<PolicyEvent>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let blob = read_blob(handle, cache, fetch_blob, missing, diagnostics)?;
    match PolicyEvent::decode(blob) {
        Ok(event) => Some(event),
        Err(error) => {
            diagnostics.push(PolicyLedgerDiagnostic::InvalidEvent { handle, error });
            None
        }
    }
}

fn same_scope_facts(left: &VerifiedCapability, right: &VerifiedCapability) -> bool {
    left.scope_root == right.scope_root
        && left
            .cap_set
            .iter()
            .filter(|fact| fact.e() == &left.scope_root)
            .eq(right
                .cap_set
                .iter()
                .filter(|fact| fact.e() == &right.scope_root))
}

fn compare_issuances(left: &ValidIssuance, right: &ValidIssuance) -> std::cmp::Ordering {
    let left_expiry = left.capability.expires_at();
    let right_expiry = right.capability.expires_at();
    if left_expiry < right_expiry {
        std::cmp::Ordering::Less
    } else if left_expiry > right_expiry {
        std::cmp::Ordering::Greater
    } else {
        left.sig.cmp(&right.sig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hifitime::Duration;
    use triblespace_core::inline::TryToInline;
    use triblespace_core::repo::capability::{
        self, PERM_ADMIN, PERM_READ, PERM_WRITE, build_capability, build_founder_anchor,
    };

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

    struct LedgerFixture {
        root: SigningKey,
        author: SigningKey,
        subject: SigningKey,
        scope_root: Id,
        now: Epoch,
        anchor_cap: Blob<SimpleArchive>,
        anchor_sig: Blob<SimpleArchive>,
        blobs: BTreeMap<Inline<Handle<SimpleArchive>>, Blob<SimpleArchive>>,
    }

    impl LedgerFixture {
        fn new() -> Self {
            let root = key(21);
            let author = key(22);
            let subject = key(23);
            let scope_root = *triblespace_core::id::ufoid();
            let parent_scope = TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @ metadata::tag: PERM_ADMIN,
            });
            let (anchor_cap, anchor_sig) =
                build_founder_anchor(&root, author.verifying_key(), scope_root, parent_scope)
                    .unwrap();
            let mut blobs = BTreeMap::new();
            blobs.insert(anchor_cap.get_handle(), anchor_cap.clone());
            Self {
                root,
                author,
                subject,
                scope_root,
                now: crate::clock::epoch_now(),
                anchor_cap,
                anchor_sig,
                blobs,
            }
        }

        fn interval(
            &self,
            seconds: f64,
        ) -> Inline<triblespace_core::inline::encodings::time::NsTAIInterval> {
            (self.now, self.now + Duration::from_seconds(seconds))
                .try_to_inline()
                .unwrap()
        }

        fn scope(&self, permission: Id) -> TribleSet {
            TribleSet::from(entity! {
                ExclusiveId::force_ref(&self.scope_root) @ metadata::tag: permission,
            })
        }

        fn issue(
            &mut self,
            permission: Id,
            seconds: f64,
        ) -> (Inline<Handle<SimpleArchive>>, Inline<Handle<SimpleArchive>>) {
            let (cap, sig) = build_capability(
                &self.author,
                self.subject.verifying_key(),
                (self.anchor_cap.clone(), self.anchor_sig.clone()),
                self.scope_root,
                self.scope(permission),
                self.interval(seconds),
            )
            .unwrap();
            let cap_handle = cap.get_handle();
            let sig_handle = sig.get_handle();
            self.blobs.insert(cap_handle, cap);
            self.blobs.insert(sig_handle, sig);
            (cap_handle, sig_handle)
        }

        fn request(&mut self, subject: VerifyingKey, permission: Id) -> RequestIdentity {
            let fragment = entity! {
                capability::cap_subject: subject,
                capability::cap_issuer: self.author.verifying_key(),
                capability::cap_scope_root: self.scope_root,
                metadata::expires_at: self.interval(1_000.0),
            };
            let mut set = TribleSet::from(fragment);
            set += self.scope(permission);
            let blob = set.to_blob();
            let handle = blob.get_handle();
            self.blobs.insert(handle, blob);
            RequestIdentity::new(subject, handle)
        }

        fn store_event(&mut self, event: PolicyEvent) -> Inline<Handle<SimpleArchive>> {
            let blob = event.to_blob();
            let handle = blob.get_handle();
            self.blobs.insert(handle, blob);
            handle
        }

        fn assertion(&mut self, event: PolicyEvent) -> PinAssertion {
            self.store_event(event);
            sign_policy_event(&self.author, event)
        }

        fn resolve(&self, snapshot: &PinAssertionSnapshot) -> PolicyLedgerResolution {
            resolve_policy_ledger(snapshot, self.author.verifying_key(), |handle| {
                self.blobs.get(&handle).cloned()
            })
        }

        fn grant(&self) -> GrantIdentity {
            GrantIdentity::new(
                self.root.verifying_key(),
                self.subject.verifying_key(),
                self.scope_root,
            )
        }
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
    fn provenance_alone_observes_and_approves_the_exact_request() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let observed = PolicyEvent::RequestObserved(request);
        let observed_handle = fixture.store_event(observed);
        let (cap, sig) = fixture.issue(PERM_READ, 200.0);
        let grant = fixture.grant();
        let issued = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig,
            request: Some(observed_handle),
        });
        let authenticated = fixture.assertion(PolicyEvent::CredentialAuthenticated { grant, sig });
        let disabled = fixture.assertion(PolicyEvent::GrantDisabled(grant));
        let rejected = fixture.assertion(PolicyEvent::RequestRejected(request));
        let mut snapshot = PinAssertionSnapshot::new();
        for assertion in [disabled, issued, rejected, authenticated] {
            snapshot.insert(assertion).unwrap();
        }

        let PolicyLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("complete closure must produce an operational view");
        };
        let request_view = view.requests().get(&request).unwrap();
        assert!(request_view.observed());
        assert!(request_view.rejected());
        assert_eq!(request_view.issued_signatures(), &BTreeSet::from([sig]));
        assert!(!request_view.is_pending());

        let grant_view = view.grants().get(&grant).unwrap();
        assert!(grant_view.disabled());
        assert!(grant_view.active_current().is_none());
        let GrantIssuanceResolution::Current(current) = grant_view.issuance() else {
            panic!("one valid issuance must be current");
        };
        assert_eq!(current.cap(), cap);
        assert_eq!(current.sig(), sig);
        assert!(current.authenticated());
    }

    #[test]
    fn current_selection_is_order_independent_and_scope_conflict_stops_selection() {
        let mut fixture = LedgerFixture::new();
        let grant = fixture.grant();
        let (_short_cap, short_sig) = fixture.issue(PERM_READ, 100.0);
        let (long_cap, long_sig) = fixture.issue(PERM_READ, 300.0);
        let short = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: short_sig,
            request: None,
        });
        let long = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: long_sig,
            request: None,
        });

        for assertions in [[short, long], [long, short]] {
            let mut snapshot = PinAssertionSnapshot::new();
            for assertion in assertions {
                snapshot.insert(assertion).unwrap();
            }
            let PolicyLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
                panic!("complete closure must resolve");
            };
            let GrantIssuanceResolution::Current(current) =
                view.grants().get(&grant).unwrap().issuance()
            else {
                panic!("equal-scope siblings must select a current credential");
            };
            assert_eq!(current.cap(), long_cap);
            assert_eq!(current.sig(), long_sig);
            assert!(!current.authenticated());
            assert_eq!(
                view.grants()
                    .get(&grant)
                    .unwrap()
                    .active_current()
                    .unwrap()
                    .sig(),
                long_sig
            );
        }

        let (_write_cap, write_sig) = fixture.issue(PERM_WRITE, 400.0);
        let write = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: write_sig,
            request: None,
        });
        let mut conflicted = PinAssertionSnapshot::new();
        for assertion in [short, long, write] {
            conflicted.insert(assertion).unwrap();
        }
        let PolicyLedgerResolution::Complete(view) = fixture.resolve(&conflicted) else {
            panic!("understood scope disagreement is a local conflict");
        };
        let GrantIssuanceResolution::Conflicted { signatures } =
            view.grants().get(&grant).unwrap().issuance()
        else {
            panic!("different exact scope facts must not be hash-arbitrated");
        };
        assert_eq!(
            signatures,
            &BTreeSet::from([short_sig, long_sig, write_sig])
        );
    }

    #[test]
    fn missing_content_and_invalid_request_fail_the_whole_ledger_closed() {
        let fixture = LedgerFixture::new();
        let missing_event = PolicyEvent::RequestRejected(RequestIdentity::new(
            fixture.subject.verifying_key(),
            handle(31),
        ));
        let mut missing_snapshot = PinAssertionSnapshot::new();
        missing_snapshot
            .insert(sign_policy_event(&fixture.author, missing_event))
            .unwrap();
        let PolicyLedgerResolution::Incomplete { missing } = fixture.resolve(&missing_snapshot)
        else {
            panic!("an absent event value must suppress the whole view");
        };
        assert_eq!(missing, vec![missing_event.handle()]);

        let mut invalid_fixture = LedgerFixture::new();
        let wrong_subject = key(32).verifying_key();
        let request = invalid_fixture.request(wrong_subject, PERM_READ);
        let lied_about_requester = RequestIdentity::new(
            invalid_fixture.subject.verifying_key(),
            request.partial_cap(),
        );
        let assertion =
            invalid_fixture.assertion(PolicyEvent::RequestObserved(lied_about_requester));
        let mut invalid_snapshot = PinAssertionSnapshot::new();
        invalid_snapshot.insert(assertion).unwrap();
        let PolicyLedgerResolution::Invalid { diagnostics } =
            invalid_fixture.resolve(&invalid_snapshot)
        else {
            panic!("a present identity-mismatched request must fail closed");
        };
        assert!(matches!(
            diagnostics.as_slice(),
            [PolicyLedgerDiagnostic::InvalidRequest {
                reason: InvalidRequestReason::SubjectMismatch { .. },
                ..
            }]
        ));
    }

    #[test]
    fn missing_and_invalid_grant_proofs_fail_the_whole_ledger_closed() {
        let mut missing_fixture = LedgerFixture::new();
        let grant = missing_fixture.grant();
        let missing_sig = handle(41);
        let assertion = missing_fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: missing_sig,
            request: None,
        });
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(assertion).unwrap();
        let PolicyLedgerResolution::Incomplete { missing } = missing_fixture.resolve(&snapshot)
        else {
            panic!("an absent named proof must suppress the whole view");
        };
        assert_eq!(missing, vec![missing_sig]);

        let mut invalid_fixture = LedgerFixture::new();
        let grant = invalid_fixture.grant();
        let invalid_sig_blob = PolicyEvent::GrantDisabled(grant).to_blob();
        let invalid_sig = invalid_sig_blob.get_handle();
        invalid_fixture.blobs.insert(invalid_sig, invalid_sig_blob);
        let assertion = invalid_fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: invalid_sig,
            request: None,
        });
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(assertion).unwrap();
        let PolicyLedgerResolution::Invalid { diagnostics } = invalid_fixture.resolve(&snapshot)
        else {
            panic!("a present malformed proof must be invalid, not unavailable");
        };
        assert!(matches!(
            diagnostics.as_slice(),
            [PolicyLedgerDiagnostic::InvalidIssuance {
                reason: InvalidIssuanceReason::Proof(_),
                ..
            }]
        ));
    }

    #[test]
    fn proof_fetch_rejects_valid_content_returned_for_the_wrong_handle() {
        let mut fixture = LedgerFixture::new();
        let grant = fixture.grant();
        let (_cap, real_sig) = fixture.issue(PERM_READ, 200.0);
        let claimed_sig = handle(42);
        let assertion = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig: claimed_sig,
            request: None,
        });
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(assertion).unwrap();

        let result =
            resolve_policy_ledger(&snapshot, fixture.author.verifying_key(), |requested| {
                if requested == claimed_sig {
                    let real = fixture.blobs.get(&real_sig).unwrap();
                    Some(Blob::with_handle(real.bytes.clone(), claimed_sig))
                } else {
                    fixture.blobs.get(&requested).cloned()
                }
            });
        let PolicyLedgerResolution::Invalid { diagnostics } = result else {
            panic!("content returned under the wrong handle must fail closed");
        };
        assert!(matches!(
            diagnostics.as_slice(),
            [PolicyLedgerDiagnostic::HandleMismatch {
                expected,
                actual,
            }] if *expected == claimed_sig && *actual == real_sig
        ));
    }

    #[test]
    fn provenance_cannot_turn_a_read_request_into_a_write_grant() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let request_handle = fixture.store_event(PolicyEvent::RequestObserved(request));
        let (_cap, sig) = fixture.issue(PERM_WRITE, 200.0);
        let assertion = fixture.assertion(PolicyEvent::GrantIssued {
            grant: fixture.grant(),
            sig,
            request: Some(request_handle),
        });
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(assertion).unwrap();

        let PolicyLedgerResolution::Invalid { diagnostics } = fixture.resolve(&snapshot) else {
            panic!("an issuance broader than its cited request must fail closed");
        };
        assert!(matches!(
            diagnostics.as_slice(),
            [PolicyLedgerDiagnostic::InvalidIssuance {
                reason: InvalidIssuanceReason::ExceedsRequestedScope,
                ..
            }]
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
