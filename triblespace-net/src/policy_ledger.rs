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
    PinAssertion, PinAssertionId, PinAssertionKeyCollision, PinAssertionSnapshot,
    PinAssertionStore, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
};
use triblespace_core::repo::strong_pin::StrongPinDescriptor;
use triblespace_core::repo::{BlobStore, BlobStoreGet, StorageFlush};
use triblespace_core::trible::{Fragment, TribleSet};

triblespace_core::prelude::attributes! {
    /// Public key whose request or grant is being described.
    "3583BC29C2155717639FA7E9314CC8B9" as pub request_requester: triblespace_core::prelude::inlineencodings::ED25519PublicKey;
    /// Exact finite capability claim supplied by a requester.
    "42903FA16A2913144A48072F575BB304" as pub request_partial_cap: Handle<SimpleArchive>;
    /// Subject whose credential this grant maintains.
    "384D8A994AF026BBD1329CAD7041E3B8" as pub policy_subject: triblespace_core::prelude::inlineencodings::ED25519PublicKey;
    /// Stable scope-root identity of this grant.
    "D67D3CB1562B27504892BF0ACB55EA8B" as pub policy_scope: triblespace_core::prelude::inlineencodings::GenId;
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

/// Local admission threshold for requests simultaneously pending in one
/// author's operational policy view.
///
/// Historical observed, rejected, and issued request facts remain in the
/// monotone ledger but do not consume this threshold. This is a writer-local
/// resource guard, not a replicated invariant: producers must serialize
/// observation writes for one author when they require the bound locally.
/// Independently mutated copies may each admit requests and later union above
/// this threshold; the reducer preserves every valid fact rather than
/// discarding or invalidating concurrent observations.
pub const MAX_PENDING_REQUESTS: usize = 1024;

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
    /// Stop renewing and redispatching this grant. This is revocation by
    /// attrition, not immediate denial: a credential already delivered to the
    /// subject remains cryptographically valid until its expiry, so the
    /// revocation-latency bound is the credential's remaining lifetime.
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

type PolicyStorageError = Box<dyn Error + Send + Sync>;

/// Durable receipt for one policy event publication.
///
/// This deliberately does not return the prospective reduced view: another
/// writer may append a concurrent positive fact immediately afterwards, so an
/// operational caller must take a fresh snapshot before acting on current
/// policy. Successful return means the complete referenced blob closure was
/// flushed before the assertion crossed its own durable append boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolicyEventReceipt {
    event: Inline<Handle<SimpleArchive>>,
    assertion: PinAssertionId,
}

/// Ordinary, non-storage reason an authenticated capability request was not
/// admitted into the policy ledger.
#[derive(Debug)]
pub enum ObserveRequestRefusal {
    InvalidClaim(VerifyError),
    SubjectMismatch { declared: VerifyingKey },
    IssuerMismatch { declared: VerifyingKey },
    OutstandingRequest { existing: RequestIdentity },
    Capacity,
}

impl fmt::Display for ObserveRequestRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidClaim(error) => {
                write!(f, "invalid operational capability claim: {error:?}")
            }
            Self::SubjectMismatch { declared } => write!(
                f,
                "request claim names a different subject {}",
                hex::encode(declared.to_bytes())
            ),
            Self::IssuerMismatch { declared } => write!(
                f,
                "request claim names a different issuer {}",
                hex::encode(declared.to_bytes())
            ),
            Self::OutstandingRequest { existing } => write!(
                f,
                "requester already has pending request {:?}",
                existing.partial_cap()
            ),
            Self::Capacity => write!(
                f,
                "local prospective policy view is at or above the {MAX_PENDING_REQUESTS}-request admission limit"
            ),
        }
    }
}

/// Result of attempting to durably observe one authenticated request.
#[derive(Debug)]
pub enum ObserveRequestOutcome {
    Observed(PolicyEventReceipt),
    Refused(ObserveRequestRefusal),
}

enum PolicyEventPublication<R> {
    Published(PolicyEventReceipt),
    Refused(R),
}

impl PolicyEventReceipt {
    pub const fn event(&self) -> Inline<Handle<SimpleArchive>> {
        self.event
    }

    pub const fn assertion(&self) -> PinAssertionId {
        self.assertion
    }
}

/// Failure to validate or durably publish one policy event.
#[derive(Debug)]
pub enum PolicyLedgerWriteError {
    Snapshot(PolicyStorageError),
    SnapshotCollision(PinAssertionKeyCollision),
    Reader(PolicyStorageError),
    Read {
        handle: Inline<Handle<SimpleArchive>>,
        source: PolicyStorageError,
    },
    Invalid {
        diagnostics: Vec<PolicyLedgerDiagnostic>,
    },
    PostconditionFailed {
        event: Inline<Handle<SimpleArchive>>,
    },
    Put {
        stage: &'static str,
        source: PolicyStorageError,
    },
    PutHandleMismatch {
        stage: &'static str,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    VerifyStored {
        stage: &'static str,
        source: PolicyStorageError,
    },
    StoredContentMismatch {
        stage: &'static str,
        handle: [u8; 32],
    },
    Flush(PolicyStorageError),
    Append(PolicyStorageError),
}

impl fmt::Display for PolicyLedgerWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => write!(f, "failed to snapshot policy assertions: {error}"),
            Self::SnapshotCollision(error) => {
                write!(f, "failed to overlay prospective policy assertion: {error}")
            }
            Self::Reader(error) => write!(f, "failed to open policy blob reader: {error}"),
            Self::Read { handle, source } => {
                write!(f, "failed to read policy blob {handle:?}: {source}")
            }
            Self::Invalid { diagnostics } => write!(
                f,
                "prospective policy ledger is invalid ({} diagnostics)",
                diagnostics.len()
            ),
            Self::PostconditionFailed { event } => write!(
                f,
                "prospective policy ledger omitted candidate event {event:?}"
            ),
            Self::Put { stage, source } => {
                write!(f, "failed to store policy {stage}: {source}")
            }
            Self::PutHandleMismatch {
                stage,
                expected,
                actual,
            } => write!(
                f,
                "policy {stage} stored under the wrong handle: expected {}, got {}",
                hex::encode_upper(expected),
                hex::encode_upper(actual)
            ),
            Self::VerifyStored { stage, source } => {
                write!(f, "failed to verify stored policy {stage}: {source}")
            }
            Self::StoredContentMismatch { stage, handle } => write!(
                f,
                "stored policy {stage} has wrong bytes under handle {}",
                hex::encode_upper(handle)
            ),
            Self::Flush(error) => write!(f, "failed to flush policy closure: {error}"),
            Self::Append(error) => write!(f, "failed to append policy assertion: {error}"),
        }
    }
}

impl Error for PolicyLedgerWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Snapshot(error)
            | Self::Reader(error)
            | Self::Flush(error)
            | Self::Append(error) => Some(error.as_ref()),
            Self::Read { source, .. }
            | Self::Put { source, .. }
            | Self::VerifyStored { source, .. } => Some(source.as_ref()),
            Self::SnapshotCollision(error) => Some(error),
            Self::Invalid { .. }
            | Self::PostconditionFailed { .. }
            | Self::PutHandleMismatch { .. }
            | Self::StoredContentMismatch { .. } => None,
        }
    }
}

/// Validate one event against the complete prospective author ledger, then
/// publish it with closure-before-assertion crash ordering.
///
/// `closure` contains newly created `SimpleArchive` blobs referenced directly
/// or transitively by the event (for example a request claim, capability, and
/// signature proof). Existing dependencies may be omitted: the prospective
/// reducer reads them through a pinned store reader. No blob is written until
/// the candidate assertion has reduced to [`PolicyLedgerResolution::Complete`].
/// The write order is closure, event, inner descriptor, strong descriptor,
/// flush, then the assertion's durable append. There is intentionally no flush
/// after `append_pin_assertion`; durability on return is that trait's contract.
pub fn append_validated_policy_event<S, I>(
    store: &mut S,
    author: &SigningKey,
    event: PolicyEvent,
    closure: I,
) -> Result<PolicyEventReceipt, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
    I: IntoIterator<Item = Blob<SimpleArchive>>,
{
    match append_validated_policy_event_if(store, author, event, closure, |_, _| {
        Ok::<bool, std::convert::Infallible>(true)
    })? {
        PolicyEventPublication::Published(receipt) => Ok(receipt),
        PolicyEventPublication::Refused(never) => match never {},
    }
}

/// Shared prospective-validation and publication mechanism. `admit` observes
/// the complete prospective view before any mutation. `Ok(true)` admits,
/// `Ok(false)` reports a violated event postcondition, and `Err(reason)` turns
/// a locally valid event into an ordinary typed refusal.
fn append_validated_policy_event_if<S, I, P, R>(
    store: &mut S,
    author: &SigningKey,
    event: PolicyEvent,
    closure: I,
    admit: P,
) -> Result<PolicyEventPublication<R>, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
    I: IntoIterator<Item = Blob<SimpleArchive>>,
    P: FnOnce(&PolicyLedgerView, bool) -> Result<bool, R>,
{
    let event_blob = event.to_blob();
    let event_handle = event_blob.get_handle();
    let assertion = sign_policy_event(author, event);

    // Normalize every supplied blob from bytes rather than trusting a cached
    // handle. This mirrors the reducer's public fetch-boundary check.
    let mut overlay = BTreeMap::new();
    for blob in closure {
        let blob = Blob::new(blob.bytes);
        overlay.insert(blob.get_handle(), blob);
    }
    overlay.insert(event_handle, event_blob.clone());
    let inner = PolicyLedgerDescriptor::blob();
    let inner_handle = inner.get_handle();
    let outer = PolicyLedgerDescriptor::strong_blob();
    let outer_handle = outer.get_handle();

    let mut snapshot = store
        .pin_assertion_snapshot()
        .map_err(|error| PolicyLedgerWriteError::Snapshot(Box::new(error)))?;
    let already_asserted = snapshot
        .for_pin(&PolicyLedgerDescriptor::pin_identity(
            author.verifying_key(),
        ))
        .contains(&assertion);
    snapshot
        .insert(assertion)
        .map_err(PolicyLedgerWriteError::SnapshotCollision)?;
    let reader = store
        .reader()
        .map_err(|error| PolicyLedgerWriteError::Reader(Box::new(error)))?;

    // BlobStoreGet deliberately has no portable NotFound discriminator. At a
    // write boundary every raw read error is therefore preserved as a storage
    // error rather than guessed to be semantic absence.
    let mut read_error = None;
    let resolution = resolve_policy_ledger(&snapshot, author.verifying_key(), |handle| {
        if let Some(blob) = overlay.get(&handle) {
            return Some(blob.clone());
        }
        if read_error.is_some() {
            return None;
        }
        match reader.get::<Blob<SimpleArchive>, SimpleArchive>(handle) {
            Ok(blob) => Some(blob),
            Err(error) => {
                read_error = Some((handle, Box::new(error) as PolicyStorageError));
                None
            }
        }
    });
    // An exact retry can skip rewriting only when the store already contains
    // the exact bytes of every supplied closure member and both retention
    // descriptors. Assertions may legitimately replicate before their
    // content, so the prospective overlay alone proves nothing about storage.
    let closure_present = already_asserted
        && overlay
            .values()
            .all(|expected| verify_stored_blob(&reader, "closure member", expected).is_ok())
        && verify_stored_blob(&reader, "inner descriptor", &inner).is_ok()
        && verify_stored_blob(&reader, "strong descriptor", &outer).is_ok();
    // End the immutable store phase explicitly before any publication write.
    // Some backends own their reader outright today, but this keeps the
    // validation/read and mutation phases distinct in the generic protocol.
    drop(reader);
    if let Some((handle, source)) = read_error {
        return Err(PolicyLedgerWriteError::Read { handle, source });
    }
    let view = match resolution {
        PolicyLedgerResolution::Complete(view) if view.event_handles().contains(&event_handle) => {
            view
        }
        PolicyLedgerResolution::Complete(_) => {
            return Err(PolicyLedgerWriteError::PostconditionFailed {
                event: event_handle,
            });
        }
        PolicyLedgerResolution::Incomplete { .. } => {
            unreachable!("the storage writer records every callback absence as a read error")
        }
        PolicyLedgerResolution::Invalid { diagnostics } => {
            return Err(PolicyLedgerWriteError::Invalid { diagnostics });
        }
    };
    match admit(&view, already_asserted) {
        Ok(true) => {}
        Ok(false) => {
            return Err(PolicyLedgerWriteError::PostconditionFailed {
                event: event_handle,
            });
        }
        Err(reason) => return Ok(PolicyEventPublication::Refused(reason)),
    }
    if closure_present {
        // Visibility through a reader does not prove crash durability. Flush
        // even on an exact retry; only the already-durable assertion append is
        // safely elided through the available generic traits.
        store
            .flush()
            .map_err(|error| PolicyLedgerWriteError::Flush(Box::new(error)))?;
        return Ok(PolicyEventPublication::Published(PolicyEventReceipt {
            event: event_handle,
            assertion: assertion.id(),
        }));
    }

    for (handle, blob) in &overlay {
        if *handle == event_handle {
            continue;
        }
        let actual = store
            .put::<SimpleArchive, _>(blob.clone())
            .map_err(|error| PolicyLedgerWriteError::Put {
                stage: "closure blob",
                source: Box::new(error),
            })?;
        require_stored_handle("closure blob", handle.raw, actual.raw)?;
    }

    let actual_event =
        store
            .put::<SimpleArchive, _>(event_blob)
            .map_err(|error| PolicyLedgerWriteError::Put {
                stage: "event blob",
                source: Box::new(error),
            })?;
    require_stored_handle("event blob", event_handle.raw, actual_event.raw)?;

    let actual_inner = store
        .put::<PolicyLedgerDescriptor, _>(inner.clone())
        .map_err(|error| PolicyLedgerWriteError::Put {
            stage: "inner descriptor",
            source: Box::new(error),
        })?;
    require_stored_handle("inner descriptor", inner_handle.raw, actual_inner.raw)?;

    let actual_outer = store
        .put::<StrongPinDescriptor, _>(outer.clone())
        .map_err(|error| PolicyLedgerWriteError::Put {
            stage: "strong descriptor",
            source: Box::new(error),
        })?;
    require_stored_handle("strong descriptor", outer_handle.raw, actual_outer.raw)?;

    // A backend returning the requested handle from `put` is not sufficient:
    // cached handles can lie, and an idempotent insert may retain corrupt bytes
    // already stored under that key. Re-read every member before crossing the
    // durability boundary, while an absent/corrupt candidate still cannot gain
    // a new assertion.
    let verification_reader = store
        .reader()
        .map_err(|error| PolicyLedgerWriteError::Reader(Box::new(error)))?;
    for blob in overlay.values() {
        let stage = if blob.get_handle() == event_handle {
            "event blob"
        } else {
            "closure blob"
        };
        verify_stored_blob(&verification_reader, stage, blob)?;
    }
    verify_stored_blob(&verification_reader, "inner descriptor", &inner)?;
    verify_stored_blob(&verification_reader, "strong descriptor", &outer)?;
    drop(verification_reader);

    store
        .flush()
        .map_err(|error| PolicyLedgerWriteError::Flush(Box::new(error)))?;
    if !already_asserted {
        store
            .append_pin_assertion(assertion)
            .map_err(|error| PolicyLedgerWriteError::Append(Box::new(error)))?;
    }

    Ok(PolicyEventPublication::Published(PolicyEventReceipt {
        event: event_handle,
        assertion: assertion.id(),
    }))
}

/// Durably observe one exact authenticated request.
///
/// Malformed claims and local admission-policy failures are ordinary typed
/// refusals and mutate nothing. For serialized calls against one store, a fresh
/// request is admitted only when it is the requester's sole pending identity and
/// the prospective local pending count is at most [`MAX_PENDING_REQUESTS`].
/// That guard is deliberately not closed under replica union: concurrent
/// asserted-pin writers may each admit facts, and the reducer preserves all of
/// them as valid concurrent observations.
pub fn observe_request<S>(
    store: &mut S,
    author: &SigningKey,
    requester: VerifyingKey,
    partial_cap: Blob<SimpleArchive>,
) -> Result<ObserveRequestOutcome, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
{
    let partial_cap = Blob::new(partial_cap.bytes);
    let claim = match decode_operational_capability(partial_cap.clone()) {
        Ok(claim) => claim,
        Err(error) => {
            return Ok(ObserveRequestOutcome::Refused(
                ObserveRequestRefusal::InvalidClaim(error),
            ));
        }
    };
    if claim.subject != requester {
        return Ok(ObserveRequestOutcome::Refused(
            ObserveRequestRefusal::SubjectMismatch {
                declared: claim.subject,
            },
        ));
    }
    if claim.issuer != author.verifying_key() {
        return Ok(ObserveRequestOutcome::Refused(
            ObserveRequestRefusal::IssuerMismatch {
                declared: claim.issuer,
            },
        ));
    }

    let request = RequestIdentity::new(requester, partial_cap.get_handle());
    let publication = append_validated_policy_event_if(
        store,
        author,
        PolicyEvent::RequestObserved(request),
        [partial_cap],
        |view, already_asserted| {
            let Some(candidate) = view.requests().get(&request) else {
                return Ok(false);
            };

            // Reassertion is an idempotent durability repair even if a later
            // positive fact has already disposed of the request. Likewise, a
            // rejection or provenance-bearing issuance may arrive before the
            // observed assertion under monotone replication.
            if already_asserted || candidate.rejected() || !candidate.issued_signatures().is_empty()
            {
                return Ok(true);
            }
            if !candidate.is_pending() {
                return Ok(false);
            }

            if let Some((&existing, _)) = view.requests().iter().find(|(identity, state)| {
                **identity != request && identity.requester() == requester && state.is_pending()
            }) {
                return Err(ObserveRequestRefusal::OutstandingRequest { existing });
            }
            let pending = view
                .requests()
                .values()
                .filter(|state| state.is_pending())
                .count();
            if pending > MAX_PENDING_REQUESTS {
                return Err(ObserveRequestRefusal::Capacity);
            }
            Ok(true)
        },
    )?;
    Ok(match publication {
        PolicyEventPublication::Published(receipt) => ObserveRequestOutcome::Observed(receipt),
        PolicyEventPublication::Refused(reason) => ObserveRequestOutcome::Refused(reason),
    })
}

/// Durably reject one exact request identity.
pub fn reject_request<S>(
    store: &mut S,
    author: &SigningKey,
    request: RequestIdentity,
) -> Result<PolicyEventReceipt, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
{
    append_validated_policy_event(
        store,
        author,
        PolicyEvent::RequestRejected(request),
        std::iter::empty(),
    )
}

/// Durably issue one credential, optionally citing exact request provenance.
///
/// The signature blob is mandatory and supplies the event's exact signature
/// handle. `closure` carries the new cap and any other proof members not
/// already present in the store.
pub fn issue_grant<S, I>(
    store: &mut S,
    author: &SigningKey,
    grant: GrantIdentity,
    signature: Blob<SimpleArchive>,
    request: Option<Inline<Handle<SimpleArchive>>>,
    closure: I,
) -> Result<PolicyEventReceipt, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
    I: IntoIterator<Item = Blob<SimpleArchive>>,
{
    let signature = Blob::new(signature.bytes);
    let sig = signature.get_handle();
    append_validated_policy_event(
        store,
        author,
        PolicyEvent::GrantIssued {
            grant,
            sig,
            request,
        },
        std::iter::once(signature).chain(closure),
    )
}

/// Durably record authentication with one exact issued signature.
pub fn authenticate_credential<S>(
    store: &mut S,
    author: &SigningKey,
    grant: GrantIdentity,
    sig: Inline<Handle<SimpleArchive>>,
) -> Result<PolicyEventReceipt, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
{
    append_validated_policy_event(
        store,
        author,
        PolicyEvent::CredentialAuthenticated { grant, sig },
        std::iter::empty(),
    )
}

/// Durably and terminally disable automatic work for one exact grant.
pub fn disable_grant<S>(
    store: &mut S,
    author: &SigningKey,
    grant: GrantIdentity,
) -> Result<PolicyEventReceipt, PolicyLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
{
    append_validated_policy_event(
        store,
        author,
        PolicyEvent::GrantDisabled(grant),
        std::iter::empty(),
    )
}

fn require_stored_handle(
    stage: &'static str,
    expected: [u8; 32],
    actual: [u8; 32],
) -> Result<(), PolicyLedgerWriteError> {
    if expected == actual {
        Ok(())
    } else {
        Err(PolicyLedgerWriteError::PutHandleMismatch {
            stage,
            expected,
            actual,
        })
    }
}

fn verify_stored_blob<R, S>(
    reader: &R,
    stage: &'static str,
    expected: &Blob<S>,
) -> Result<(), PolicyLedgerWriteError>
where
    R: BlobStoreGet,
    S: BlobEncoding + 'static,
    Handle<S>: triblespace_core::inline::InlineEncoding,
{
    let handle = expected.get_handle();
    let stored =
        reader
            .get::<Blob<S>, S>(handle)
            .map_err(|error| PolicyLedgerWriteError::VerifyStored {
                stage,
                source: Box::new(error),
            })?;
    if stored.bytes != expected.bytes {
        return Err(PolicyLedgerWriteError::StoredContentMismatch {
            stage,
            handle: handle.raw,
        });
    }
    Ok(())
}

/// Typed result of reducing one author's complete policy assertion set.
///
/// Only Complete exposes an operational view. Missing content and known-invalid
/// evidence are global fail-closed states for this deliberately coarse first
/// ledger layout. Complete means closure-valid for the supplied assertion
/// snapshot; it does not imply that every independently mutated replica has
/// converged into that snapshot. Fetching missing content for the same snapshot
/// can refine resolution, but adding or merging assertions creates a new input;
/// callers must not cache a complete policy view across that boundary.
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
    event_handles: BTreeSet<Inline<Handle<SimpleArchive>>>,
    requests: BTreeMap<RequestIdentity, RequestView>,
    grants: BTreeMap<GrantIdentity, GrantView>,
}

impl PolicyLedgerView {
    pub fn author(&self) -> VerifyingKey {
        self.author
    }

    /// Exact canonical values admitted from this author's assertion set.
    pub fn event_handles(&self) -> &BTreeSet<Inline<Handle<SimpleArchive>>> {
        &self.event_handles
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

    /// Historical issuance projection, retained even after disablement.
    ///
    /// Inspection and positive-evidence recording may need this exact past
    /// issuance. Credential dispatch must use [`Self::usable_at`] instead so
    /// disabled or expired grants are not sent. Renewal may still inspect the
    /// historical current issuance in order to replace an expired credential.
    pub fn historical_issuance(&self) -> &GrantIssuanceResolution {
        &self.issuance
    }

    /// Return the selected credential only when this grant is usable at `now`.
    ///
    /// Disabled and expired grants retain their historical issuance for
    /// inspection and renewal decisions, but must never be dispatched as
    /// usable credentials.
    pub fn usable_at(&self, now: Epoch) -> Option<&CurrentGrant> {
        if self.disabled {
            return None;
        }
        match &self.issuance {
            GrantIssuanceResolution::Current(current) if !current.capability.is_expired_at(now) => {
                Some(current)
            }
            GrantIssuanceResolution::Unissued | GrantIssuanceResolution::Conflicted { .. } => None,
            GrantIssuanceResolution::Current(_) => None,
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

    for &handle in &asserted_handles {
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
    let mut issued_request_signatures = BTreeSet::new();
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

        if let Some(request) = request {
            issued_request_signatures.insert((request, issuance.sig));
        }
        valid_issuances.push((
            issuance.grant,
            ValidIssuance {
                cap: verified.leaf_cap,
                sig: issuance.sig,
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
    for (request, signature) in issued_request_signatures {
        requests
            .entry(request)
            .or_default()
            .issued_signatures
            .insert(signature);
    }

    let mut grants = BTreeMap::<GrantIdentity, GrantAccumulator>::new();
    for grant in disabled {
        grants.entry(grant).or_default().disabled = true;
    }
    for (grant, signatures) in authentications {
        grants.entry(grant).or_default().authentications = signatures;
    }
    for (grant, issuance) in valid_issuances {
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
                // Equal order means the signature is identical. Signature
                // handles commit to the capability blob, so every field kept
                // in `CurrentGrant` is then identical too. Request provenance
                // was projected separately into commutative `RequestView`
                // signature sets and never enters selection candidates.
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
        event_handles: asserted_handles,
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
    use std::convert::Infallible;
    use triblespace_core::inline::TryToInline;
    use triblespace_core::repo::BlobStorePut;
    use triblespace_core::repo::capability::{
        self, PERM_ADMIN, PERM_READ, PERM_WRITE, build_capability, build_founder_anchor,
    };
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::pile::Pile;

    #[derive(Debug)]
    struct InjectedFailure(&'static str);

    impl fmt::Display for InjectedFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "injected {} failure", self.0)
        }
    }

    impl Error for InjectedFailure {}

    #[derive(Debug)]
    enum RecordingAssertionError {
        Collision(PinAssertionKeyCollision),
        Injected(InjectedFailure),
    }

    impl fmt::Display for RecordingAssertionError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Collision(error) => error.fmt(f),
                Self::Injected(error) => error.fmt(f),
            }
        }
    }

    impl Error for RecordingAssertionError {}

    impl From<PinAssertionKeyCollision> for RecordingAssertionError {
        fn from(value: PinAssertionKeyCollision) -> Self {
            Self::Collision(value)
        }
    }

    #[derive(Default)]
    struct RecordingStore {
        inner: MemoryRepo,
        operations: Vec<&'static str>,
        put_handles: Vec<[u8; 32]>,
        fail_flush: bool,
        fail_append: bool,
    }

    impl triblespace_core::repo::BlobStorePut for RecordingStore {
        type PutError = Infallible;

        fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
        where
            S: BlobEncoding + 'static,
            T: IntoBlob<S>,
            Handle<S>: triblespace_core::inline::InlineEncoding,
        {
            let handle = self.inner.put(item)?;
            self.operations.push("put");
            self.put_handles.push(handle.raw);
            Ok(handle)
        }
    }

    impl BlobStore for RecordingStore {
        type Reader = <MemoryRepo as BlobStore>::Reader;
        type ReaderError = <MemoryRepo as BlobStore>::ReaderError;

        fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
            self.operations.push("reader");
            self.inner.reader()
        }
    }

    impl PinAssertionStore for RecordingStore {
        type Error = RecordingAssertionError;

        fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
            self.operations.push("snapshot");
            self.inner.pin_assertion_snapshot().map_err(Into::into)
        }

        fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
            self.operations.push("append");
            if self.fail_append {
                return Err(RecordingAssertionError::Injected(InjectedFailure("append")));
            }
            self.inner
                .append_pin_assertion(assertion)
                .map_err(Into::into)
        }
    }

    impl StorageFlush for RecordingStore {
        type Error = InjectedFailure;

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.operations.push("flush");
            if self.fail_flush {
                Err(InjectedFailure("flush"))
            } else {
                Ok(())
            }
        }
    }

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn indexed_key(index: u64) -> SigningKey {
        let mut bytes = [0xA5; 32];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        SigningKey::from_bytes(&bytes)
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
            self.request_with_expiry(subject, permission, 1_000.0)
        }

        fn request_with_expiry(
            &mut self,
            subject: VerifyingKey,
            permission: Id,
            seconds: f64,
        ) -> RequestIdentity {
            let fragment = entity! {
                capability::cap_subject: subject,
                capability::cap_issuer: self.author.verifying_key(),
                capability::cap_scope_root: self.scope_root,
                metadata::expires_at: self.interval(seconds),
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
    fn observe_request_refuses_invalid_claims_before_touching_storage() {
        let fixture = LedgerFixture::new();
        let mut store = RecordingStore::default();
        let malformed = Blob::new(Bytes::from_source(b"not a capability".to_vec()));

        let outcome = observe_request(
            &mut store,
            &fixture.author,
            fixture.subject.verifying_key(),
            malformed,
        )
        .expect("invalid remote input is an ordinary refusal");
        assert!(matches!(
            outcome,
            ObserveRequestOutcome::Refused(ObserveRequestRefusal::InvalidClaim(_))
        ));
        assert!(store.operations.is_empty());
        assert_eq!(store.inner.blobs.len(), 0);
        assert!(store.inner.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn observe_request_refuses_a_wrong_issuer_before_touching_storage() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let wrong_author = key(72);
        let mut store = RecordingStore::default();

        let outcome = observe_request(
            &mut store,
            &wrong_author,
            fixture.subject.verifying_key(),
            partial_cap,
        )
        .expect("wrong issuer is an ordinary refusal");
        assert!(matches!(
            outcome,
            ObserveRequestOutcome::Refused(ObserveRequestRefusal::IssuerMismatch { declared })
                if declared == fixture.author.verifying_key()
        ));
        assert!(store.operations.is_empty());
        assert_eq!(store.inner.blobs.len(), 0);
    }

    #[test]
    fn observe_request_allows_one_pending_requester_then_reopens_after_disposition() {
        let mut fixture = LedgerFixture::new();
        let requester = fixture.subject.verifying_key();
        let first = fixture.request(requester, PERM_READ);
        let first_cap = fixture.blobs.get(&first.partial_cap()).unwrap().clone();
        let second = fixture.request(requester, PERM_WRITE);
        let second_cap = fixture.blobs.get(&second.partial_cap()).unwrap().clone();
        let mut store = RecordingStore::default();

        assert!(matches!(
            observe_request(&mut store, &fixture.author, requester, first_cap)
                .expect("first observation"),
            ObserveRequestOutcome::Observed(_)
        ));
        let before_blobs = store.inner.blobs.len();
        store.operations.clear();
        store.put_handles.clear();
        let refused = observe_request(&mut store, &fixture.author, requester, second_cap.clone())
            .expect("admission refusal is not a storage error");
        assert!(matches!(
            refused,
            ObserveRequestOutcome::Refused(ObserveRequestRefusal::OutstandingRequest {
                existing
            }) if existing == first
        ));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert!(store.put_handles.is_empty());
        assert_eq!(store.inner.blobs.len(), before_blobs);

        reject_request(&mut store, &fixture.author, first).expect("dispose first request");
        let reopened = observe_request(&mut store, &fixture.author, requester, second_cap)
            .expect("fresh request after disposition");
        assert!(matches!(reopened, ObserveRequestOutcome::Observed(_)));

        let snapshot = store.inner.pin_assertion_snapshot().unwrap();
        let reader = store.inner.reader().unwrap();
        let PolicyLedgerResolution::Complete(view) =
            resolve_policy_ledger(&snapshot, fixture.author.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            })
        else {
            panic!("admitted requests must leave a complete ledger");
        };
        assert!(view.requests().get(&first).unwrap().rejected());
        assert!(view.requests().get(&second).unwrap().is_pending());
    }

    #[test]
    fn observe_request_refuses_when_the_pending_view_is_at_capacity() {
        let mut fixture = LedgerFixture::new();
        let mut store = RecordingStore::default();
        for index in 0..MAX_PENDING_REQUESTS as u64 {
            let requester = indexed_key(1_000 + index).verifying_key();
            let request = fixture.request(requester, PERM_READ);
            let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
            let event = PolicyEvent::RequestObserved(request);
            store
                .inner
                .put::<SimpleArchive, _>(partial_cap)
                .expect("seed request claim");
            store
                .inner
                .put::<SimpleArchive, _>(event.to_blob())
                .expect("seed request event");
            store
                .inner
                .append_pin_assertion(sign_policy_event(&fixture.author, event))
                .expect("seed request assertion");
        }

        let requester = indexed_key(99_999).verifying_key();
        let request = fixture.request(requester, PERM_WRITE);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let before_blobs = store.inner.blobs.len();
        let before_assertions = store.inner.pin_assertion_snapshot().unwrap().len();
        store.operations.clear();

        let outcome = observe_request(&mut store, &fixture.author, requester, partial_cap)
            .expect("capacity is an ordinary refusal");
        assert!(matches!(
            outcome,
            ObserveRequestOutcome::Refused(ObserveRequestRefusal::Capacity)
        ));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert_eq!(store.inner.blobs.len(), before_blobs);
        assert_eq!(
            store.inner.pin_assertion_snapshot().unwrap().len(),
            before_assertions
        );
    }

    #[test]
    fn independent_local_admissions_union_as_concurrent_pending_facts() {
        let mut fixture = LedgerFixture::new();
        let requester = fixture.subject.verifying_key();
        let first = fixture.request(requester, PERM_READ);
        let first_cap = fixture.blobs.get(&first.partial_cap()).unwrap().clone();
        let second = fixture.request(requester, PERM_WRITE);
        let second_cap = fixture.blobs.get(&second.partial_cap()).unwrap().clone();
        let mut left = MemoryRepo::default();
        let mut right = MemoryRepo::default();

        assert!(matches!(
            observe_request(&mut left, &fixture.author, requester, first_cap)
                .expect("left replica admits against its local view"),
            ObserveRequestOutcome::Observed(_)
        ));
        assert!(matches!(
            observe_request(&mut right, &fixture.author, requester, second_cap)
                .expect("right replica admits against its local view"),
            ObserveRequestOutcome::Observed(_)
        ));

        let pin = PolicyLedgerDescriptor::pin_identity(fixture.author.verifying_key());
        let left_snapshot = left.pin_assertion_snapshot().unwrap();
        let right_snapshot = right.pin_assertion_snapshot().unwrap();
        let mut merged = PinAssertionSnapshot::new();
        for assertion in left_snapshot
            .for_pin(&pin)
            .into_iter()
            .chain(right_snapshot.for_pin(&pin))
        {
            merged.insert(assertion).unwrap();
        }
        let left_reader = left.reader().unwrap();
        let right_reader = right.reader().unwrap();
        let PolicyLedgerResolution::Complete(view) =
            resolve_policy_ledger(&merged, fixture.author.verifying_key(), |handle| {
                left_reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
                    .or_else(|| {
                        right_reader
                            .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                            .ok()
                    })
            })
        else {
            panic!("union of independently valid observations remains complete");
        };

        assert_eq!(view.requests().len(), 2);
        assert!(view.requests().get(&first).unwrap().is_pending());
        assert!(view.requests().get(&second).unwrap().is_pending());
        assert_eq!(
            view.requests()
                .iter()
                .filter(|(request, state)| request.requester() == requester && state.is_pending())
                .count(),
            2,
            "writer-local one-outstanding guards are intentionally not closed under union"
        );
    }

    #[test]
    fn validated_writer_flushes_complete_closure_before_durable_assertion() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);
        let mut store = RecordingStore::default();

        let receipt =
            append_validated_policy_event(&mut store, &fixture.author, event, [partial_cap])
                .expect("valid prospective event publishes");
        assert_eq!(receipt.event(), event.handle());
        assert_eq!(
            receipt.assertion(),
            sign_policy_event(&fixture.author, event).id()
        );
        assert_eq!(
            store.operations,
            [
                "snapshot", "reader", "put", "put", "put", "put", "reader", "flush", "append"
            ]
        );
        assert_eq!(
            store.put_handles,
            [
                request.partial_cap().raw,
                event.handle().raw,
                PolicyLedgerDescriptor::descriptor_handle().raw,
                PolicyLedgerDescriptor::strong_blob().get_handle().raw,
            ]
        );

        let snapshot = store.inner.pin_assertion_snapshot().unwrap();
        let reader = store.inner.reader().unwrap();
        let PolicyLedgerResolution::Complete(view) =
            resolve_policy_ledger(&snapshot, fixture.author.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            })
        else {
            panic!("durably published closure must resolve after the write");
        };
        assert!(view.event_handles().contains(&event.handle()));
        assert!(view.requests().get(&request).unwrap().observed());
    }

    #[test]
    fn validated_writer_mutates_nothing_for_an_invalid_candidate() {
        let mut fixture = LedgerFixture::new();
        let actual = fixture.request(key(71).verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&actual.partial_cap()).unwrap().clone();
        let lied = RequestIdentity::new(fixture.subject.verifying_key(), actual.partial_cap());
        let mut store = RecordingStore::default();

        let error = append_validated_policy_event(
            &mut store,
            &fixture.author,
            PolicyEvent::RequestObserved(lied),
            [partial_cap],
        )
        .unwrap_err();
        assert!(matches!(error, PolicyLedgerWriteError::Invalid { .. }));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert_eq!(store.inner.blobs.len(), 0);
        assert!(store.inner.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn validated_writer_never_appends_before_flush_and_leaves_only_safe_orphans_on_failure() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);

        let mut flush_failure = RecordingStore {
            fail_flush: true,
            ..RecordingStore::default()
        };
        let error = append_validated_policy_event(
            &mut flush_failure,
            &fixture.author,
            event,
            [partial_cap.clone()],
        )
        .unwrap_err();
        assert!(matches!(error, PolicyLedgerWriteError::Flush(_)));
        assert_eq!(
            flush_failure.operations,
            [
                "snapshot", "reader", "put", "put", "put", "put", "reader", "flush"
            ]
        );
        assert_eq!(flush_failure.inner.blobs.len(), 4);
        assert!(
            flush_failure
                .inner
                .pin_assertion_snapshot()
                .unwrap()
                .is_empty()
        );

        let mut append_failure = RecordingStore {
            fail_append: true,
            ..RecordingStore::default()
        };
        let error = append_validated_policy_event(
            &mut append_failure,
            &fixture.author,
            event,
            [partial_cap],
        )
        .unwrap_err();
        assert!(matches!(error, PolicyLedgerWriteError::Append(_)));
        assert_eq!(
            append_failure.operations,
            [
                "snapshot", "reader", "put", "put", "put", "put", "reader", "flush", "append"
            ]
        );
        assert_eq!(append_failure.inner.blobs.len(), 4);
        assert!(
            append_failure
                .inner
                .pin_assertion_snapshot()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn validated_writer_republication_is_idempotent() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);
        let mut store = RecordingStore::default();

        let first = append_validated_policy_event(
            &mut store,
            &fixture.author,
            event,
            [partial_cap.clone()],
        )
        .expect("first publication succeeds");
        assert_eq!(store.inner.blobs.len(), 4);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);

        store.operations.clear();
        store.put_handles.clear();
        let second =
            append_validated_policy_event(&mut store, &fixture.author, event, [partial_cap])
                .expect("exact republication succeeds");

        assert_eq!(second, first);
        assert_eq!(store.inner.blobs.len(), 4);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);
        assert_eq!(store.operations, ["snapshot", "reader", "flush"]);
        assert!(store.put_handles.is_empty());
    }

    #[test]
    fn validated_writer_repairs_an_assertion_whose_content_has_not_arrived() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);
        let assertion = sign_policy_event(&fixture.author, event);
        let mut store = RecordingStore::default();
        store
            .inner
            .append_pin_assertion(assertion)
            .expect("seed assertion without its closure");

        let receipt =
            append_validated_policy_event(&mut store, &fixture.author, event, [partial_cap])
                .expect("exact retry repairs missing content");
        assert_eq!(receipt.assertion(), assertion.id());
        assert_eq!(
            store.operations,
            [
                "snapshot", "reader", "put", "put", "put", "put", "reader", "flush"
            ]
        );
        assert_eq!(store.inner.blobs.len(), 4);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);

        let snapshot = store.inner.pin_assertion_snapshot().unwrap();
        let reader = store.inner.reader().unwrap();
        assert!(matches!(
            resolve_policy_ledger(&snapshot, fixture.author.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            }),
            PolicyLedgerResolution::Complete(_)
        ));
    }

    #[test]
    fn validated_writer_does_not_trust_cached_handles_on_duplicate_presence_check() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);
        let mut store = RecordingStore::default();
        append_validated_policy_event(&mut store, &fixture.author, event, [partial_cap.clone()])
            .expect("seed valid publication");

        let poisoned = Blob::<UnknownBlob>::with_handle(
            Bytes::from_source(b"wrong bytes under the event handle".to_vec()),
            Inline::new(event.handle().raw),
        );
        let existing = store.inner.blobs.reader().unwrap();
        store.inner.blobs = existing
            .iter()
            .map(|(handle, blob)| {
                if handle.raw == event.handle().raw {
                    (handle, poisoned.clone())
                } else {
                    (handle, blob)
                }
            })
            .collect();
        store.operations.clear();
        store.put_handles.clear();

        let error =
            append_validated_policy_event(&mut store, &fixture.author, event, [partial_cap])
                .expect_err("a backend retaining poisoned content must fail closed");
        assert!(matches!(
            error,
            PolicyLedgerWriteError::StoredContentMismatch {
                stage: "event blob",
                ..
            }
        ));
        assert_eq!(
            store.operations,
            ["snapshot", "reader", "put", "put", "put", "put", "reader"]
        );
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);
    }

    #[test]
    fn validated_writer_is_complete_after_pile_reopen_without_post_append_flush() {
        let mut fixture = LedgerFixture::new();
        let request = fixture.request(fixture.subject.verifying_key(), PERM_READ);
        let partial_cap = fixture.blobs.get(&request.partial_cap()).unwrap().clone();
        let event = PolicyEvent::RequestObserved(request);
        let dir = tempfile::tempdir().expect("temporary pile directory");
        let path = dir.path().join("policy-ledger.pile");
        std::fs::File::create(&path).expect("create empty pile");

        {
            let mut pile = Pile::open(&path).expect("open policy pile");
            let ObserveRequestOutcome::Observed(receipt) = observe_request(
                &mut pile,
                &fixture.author,
                fixture.subject.verifying_key(),
                partial_cap,
            )
            .expect("durably publish observed request") else {
                panic!("valid request must be observed");
            };
            assert_eq!(receipt.event(), event.handle());
            // Deliberately drop without another flush: the writer's one blob
            // flush precedes the assertion, whose append is durable itself.
        }

        let mut reopened = Pile::open(&path).expect("reopen policy pile");
        let snapshot = reopened
            .pin_assertion_snapshot()
            .expect("replay durable assertion");
        assert_eq!(snapshot.len(), 1);
        let reader = reopened.reader().expect("open replay reader");
        reader
            .get::<Blob<PolicyLedgerDescriptor>, PolicyLedgerDescriptor>(
                PolicyLedgerDescriptor::descriptor_handle(),
            )
            .expect("replay inner policy descriptor");
        reader
            .get::<Blob<StrongPinDescriptor>, StrongPinDescriptor>(
                PolicyLedgerDescriptor::strong_blob().get_handle(),
            )
            .expect("replay strong policy descriptor");
        let PolicyLedgerResolution::Complete(view) =
            resolve_policy_ledger(&snapshot, fixture.author.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            })
        else {
            panic!("one-flush publication must replay as a complete ledger");
        };
        assert!(view.event_handles().contains(&event.handle()));
        assert!(view.requests().get(&request).unwrap().observed());
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
        assert!(grant_view.usable_at(fixture.now).is_none());
        let GrantIssuanceResolution::Current(current) = grant_view.historical_issuance() else {
            panic!("one valid issuance must be current");
        };
        assert_eq!(current.cap(), cap);
        assert_eq!(current.sig(), sig);
        assert!(current.authenticated());
    }

    #[test]
    fn expiry_selects_current_and_scope_conflict_stops_selection() {
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

        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(short).unwrap();
        snapshot.insert(long).unwrap();
        let PolicyLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("complete closure must resolve");
        };
        let GrantIssuanceResolution::Current(current) =
            view.grants().get(&grant).unwrap().historical_issuance()
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
                .usable_at(fixture.now)
                .unwrap()
                .sig(),
            long_sig
        );

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
            view.grants().get(&grant).unwrap().historical_issuance()
        else {
            panic!("different exact scope facts must not be hash-arbitrated");
        };
        assert_eq!(
            signatures,
            &BTreeSet::from([short_sig, long_sig, write_sig])
        );
    }

    #[test]
    fn identical_issuance_cited_by_two_requests_projects_both_provenances() {
        let mut fixture = LedgerFixture::new();
        let subject = fixture.subject.verifying_key();
        let first = fixture.request_with_expiry(subject, PERM_READ, 800.0);
        let second = fixture.request_with_expiry(subject, PERM_READ, 900.0);
        let first_handle = fixture.store_event(PolicyEvent::RequestObserved(first));
        let second_handle = fixture.store_event(PolicyEvent::RequestObserved(second));
        assert_ne!(first_handle, second_handle);
        let (cap, sig) = fixture.issue(PERM_READ, 200.0);
        let grant = fixture.grant();
        let first_issuance = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig,
            request: Some(first_handle),
        });
        let second_issuance = fixture.assertion(PolicyEvent::GrantIssued {
            grant,
            sig,
            request: Some(second_handle),
        });
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot.insert(first_issuance).unwrap();
        snapshot.insert(second_issuance).unwrap();

        let PolicyLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid duplicate issuance provenance must resolve completely");
        };
        for request in [first, second] {
            let request_view = view.requests().get(&request).unwrap();
            assert!(request_view.observed());
            assert_eq!(request_view.issued_signatures(), &BTreeSet::from([sig]));
        }
        let current = view
            .grants()
            .get(&grant)
            .unwrap()
            .usable_at(fixture.now)
            .unwrap();
        assert_eq!(current.cap(), cap);
        assert_eq!(current.sig(), sig);
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
