//! Recipient-authored effects as one monotone asserted event ledger.
//!
//! This ledger records what one recipient intended and accepted. It is kept
//! separate from [`crate::policy_ledger`], which records an issuer's policy.
//! Every value is a canonical [`SimpleArchive`] asserted under the recipient's
//! fixed author-scoped [`StrongPinDescriptor`] identity. The opaque assertion
//! label is all zero in V1; all ordering comes from explicit event handles.
//!
//! Intent and founder-selection replacement use visible-frontier semantics:
//! an event explicitly supersedes the frontier its writer observed, while an
//! unseen concurrent event remains visible after replica union. Credential
//! successors cite a nonempty homogeneous set of prior acceptances and must
//! product-dominate every cited credential. Consequently a later successor
//! can deliberately join and heal a credential fork without erasing evidence.
//! A cancellation or replacement racing an acceptance of the same visible
//! intent is conservative and order independent: after replica union that
//! acceptance is retained as inert evidence and grants no authority. A fresh
//! post-union intent and acceptance can heal the race without choosing a
//! replay winner.
//! An intent binds both authority axes known before delivery: its partial cap
//! carries the mandatory admin `cap_issuer`, and `IntentDeclared.team_root` is
//! the caller-supplied trust anchor. A delivery must check that root; it must
//! never teach or replace the requested authority domain.
//!
//! The descriptor, attribute, and event-kind ids in this module were freshly
//! minted with `trible genid` on 2026-08-02. The descriptor marker is
//! `F61842DC6DE1737A423C682D96894D41`; the new `effect_parent` attribute is
//! `05DD446F58BEA8C08F57547D57782930`; the four event kinds are recorded beside
//! their constants below.

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
    OperationalCapability, VerifiedCapability, VerifiedCapabilityChain, VerifyError,
    decode_operational_capability, scope_subsumes, verify_chain_details_allow_expired,
};
use triblespace_core::repo::pin_assertion::{
    PinAssertion, PinAssertionId, PinAssertionKeyCollision, PinAssertionSnapshot,
    PinAssertionStore, PinHandle, PinIdentity, SubsumptionLabel, ValueHandle,
};
use triblespace_core::repo::strong_pin::StrongPinDescriptor;
use triblespace_core::repo::{BlobStore, BlobStoreGet, StorageFlush};
use triblespace_core::trible::{Fragment, TribleSet};

use crate::policy_ledger::{
    policy_credential_sig, policy_scope, policy_team_root, request_partial_cap,
};

triblespace_core::prelude::attributes! {
    /// Exact asserted recipient event that this event causally follows.
    "05DD446F58BEA8C08F57547D57782930" as pub effect_parent: Handle<SimpleArchive>;
}

/// Fixed V1 inner descriptor marker for one recipient's effect ledger.
///
/// Minted with `trible genid` on 2026-08-02.
pub const RECIPIENT_LEDGER_DESCRIPTOR_V1: [u8; 16] = [
    0xF6, 0x18, 0x42, 0xDC, 0x6D, 0xE1, 0x73, 0x7A, 0x42, 0x3C, 0x68, 0x2D, 0x96, 0x89, 0x4D, 0x41,
];

/// `IntentDeclared`, minted with `trible genid` on 2026-08-02.
pub const EVENT_INTENT_DECLARED: Id =
    triblespace_core::id::id_hex!("0FD8197C467AB34BE3DFC09295E54ACD");
/// `IntentCanceled`, minted with `trible genid` on 2026-08-02.
pub const EVENT_INTENT_CANCELED: Id =
    triblespace_core::id::id_hex!("B2FAD2BBEBCD0BCA65E703898D0B5098");
/// `CredentialAccepted`, minted with `trible genid` on 2026-08-02.
pub const EVENT_CREDENTIAL_ACCEPTED: Id =
    triblespace_core::id::id_hex!("07D922D8C02B12C2EDA3076BD17EDDDA");
/// `FounderGrantSelected`, minted with `trible genid` on 2026-08-02.
pub const EVENT_FOUNDER_GRANT_SELECTED: Id =
    triblespace_core::id::id_hex!("1CDC2D91150DB87AE9F96234A9E199FB");

/// Fixed inner descriptor for one author's complete recipient-effect set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecipientLedgerDescriptor;

impl BlobEncoding for RecipientLedgerDescriptor {}

impl MetaDescribe for RecipientLedgerDescriptor {
    fn describe() -> Fragment {
        let id = triblespace_core::id::id_hex!("F61842DC6DE1737A423C682D96894D41");
        entity! {
            ExclusiveId::force_ref(&id) @
                metadata::name: "recipient-effect-ledger-v1",
                metadata::description: "Fixed descriptor for one assertion author's monotone recipient-effect event ledger. Values are canonical SimpleArchive events; a StrongPinDescriptor supplies hard retention.",
                metadata::tag: metadata::KIND_BLOB_ENCODING,
        }
    }
}

impl RecipientLedgerDescriptor {
    pub fn blob() -> Blob<Self> {
        Blob::new(Bytes::from_source(RECIPIENT_LEDGER_DESCRIPTOR_V1.to_vec()))
    }

    pub fn descriptor_handle() -> Inline<Handle<Self>> {
        Inline::new(Blake3::digest(&RECIPIENT_LEDGER_DESCRIPTOR_V1))
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

impl TryFromBlob<RecipientLedgerDescriptor> for RecipientLedgerDescriptor {
    type Error = RecipientLedgerDescriptorError;

    fn try_from_blob(blob: Blob<RecipientLedgerDescriptor>) -> Result<Self, Self::Error> {
        let bytes = blob.bytes.as_ref();
        if bytes.len() != RECIPIENT_LEDGER_DESCRIPTOR_V1.len() {
            return Err(RecipientLedgerDescriptorError::WrongLength {
                actual: bytes.len(),
            });
        }
        if bytes != RECIPIENT_LEDGER_DESCRIPTOR_V1 {
            return Err(RecipientLedgerDescriptorError::WrongKind);
        }
        Ok(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecipientLedgerDescriptorError {
    WrongLength { actual: usize },
    WrongKind,
}

impl fmt::Display for RecipientLedgerDescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { actual } => write!(
                f,
                "recipient ledger descriptor is {actual} bytes, expected {}",
                RECIPIENT_LEDGER_DESCRIPTOR_V1.len()
            ),
            Self::WrongKind => write!(f, "pin descriptor is not a V1 recipient ledger"),
        }
    }
}

impl Error for RecipientLedgerDescriptorError {}

pub type RecipientEventHandle = Inline<Handle<SimpleArchive>>;

/// One canonical positive recipient effect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecipientEvent {
    /// Request one issuer under an independently supplied trusted team root.
    IntentDeclared {
        team_root: VerifyingKey,
        partial_cap: Inline<Handle<SimpleArchive>>,
        supersedes: BTreeSet<RecipientEventHandle>,
    },
    IntentCanceled {
        intent: RecipientEventHandle,
    },
    CredentialAccepted {
        team_root: VerifyingKey,
        sig: Inline<Handle<SimpleArchive>>,
        basis: BTreeSet<RecipientEventHandle>,
    },
    FounderGrantSelected {
        team_root: VerifyingKey,
        scope_root: Id,
        supersedes: BTreeSet<RecipientEventHandle>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RecipientEventKind {
    IntentDeclared,
    IntentCanceled,
    CredentialAccepted,
    FounderGrantSelected,
}

impl RecipientEvent {
    pub const fn kind(&self) -> RecipientEventKind {
        match self {
            Self::IntentDeclared { .. } => RecipientEventKind::IntentDeclared,
            Self::IntentCanceled { .. } => RecipientEventKind::IntentCanceled,
            Self::CredentialAccepted { .. } => RecipientEventKind::CredentialAccepted,
            Self::FounderGrantSelected { .. } => RecipientEventKind::FounderGrantSelected,
        }
    }

    fn fragment(&self) -> Fragment {
        match self {
            Self::IntentDeclared {
                team_root,
                partial_cap,
                supersedes,
            } => entity! {
                metadata::tag: EVENT_INTENT_DECLARED,
                policy_team_root: *team_root,
                request_partial_cap: *partial_cap,
                effect_parent*: supersedes.iter().copied(),
            },
            Self::IntentCanceled { intent } => entity! {
                metadata::tag: EVENT_INTENT_CANCELED,
                effect_parent: *intent,
            },
            Self::CredentialAccepted {
                team_root,
                sig,
                basis,
            } => entity! {
                metadata::tag: EVENT_CREDENTIAL_ACCEPTED,
                policy_team_root: *team_root,
                policy_credential_sig: *sig,
                effect_parent*: basis.iter().copied(),
            },
            Self::FounderGrantSelected {
                team_root,
                scope_root,
                supersedes,
            } => entity! {
                metadata::tag: EVENT_FOUNDER_GRANT_SELECTED,
                policy_team_root: *team_root,
                policy_scope: *scope_root,
                effect_parent*: supersedes.iter().copied(),
            },
        }
    }

    pub fn id(&self) -> Id {
        self.fragment()
            .root()
            .expect("one recipient event fragment exports one intrinsic id")
    }

    pub fn to_blob(&self) -> Blob<SimpleArchive> {
        let set: TribleSet = self.fragment().into();
        set.to_blob()
    }

    pub fn handle(&self) -> RecipientEventHandle {
        self.to_blob().get_handle()
    }

    fn parents(&self) -> BTreeSet<RecipientEventHandle> {
        match self {
            Self::IntentDeclared { supersedes, .. }
            | Self::FounderGrantSelected { supersedes, .. } => supersedes.clone(),
            Self::IntentCanceled { intent } => BTreeSet::from([*intent]),
            Self::CredentialAccepted { basis, .. } => basis.clone(),
        }
    }

    /// Strictly decode one event and reject alternate containers.
    ///
    /// Repeated parents are reconstructed into sets, then canonical archive
    /// bytes are compared. Parent order and duplicates therefore cannot alter
    /// an event handle, while unrelated facts and extra fields are rejected.
    pub fn decode(blob: Blob<SimpleArchive>) -> Result<Self, RecipientEventError> {
        let set: TribleSet = TryFromBlob::try_from_blob(blob.clone())?;
        let mut tags = find!(
            (event: Id, kind: Id),
            pattern!(&set, [{ ?event @ metadata::tag: ?kind }])
        );
        let (event, kind) = exactly_one(&mut tags)?;

        let decoded = if kind == EVENT_INTENT_DECLARED {
            let team_root = one_value(find!(
                root: VerifyingKey,
                pattern!(&set, [{ event @ policy_team_root: ?root }])
            ))?;
            let partial_cap = one_value(find!(
                cap: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ request_partial_cap: ?cap }])
            ))?;
            let supersedes = find!(
                parent: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ effect_parent: ?parent }])
            )
            .collect();
            Self::IntentDeclared {
                team_root,
                partial_cap,
                supersedes,
            }
        } else if kind == EVENT_INTENT_CANCELED {
            let intent = one_value(find!(
                parent: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ effect_parent: ?parent }])
            ))?;
            Self::IntentCanceled { intent }
        } else if kind == EVENT_CREDENTIAL_ACCEPTED {
            let team_root = one_value(find!(
                root: VerifyingKey,
                pattern!(&set, [{ event @ policy_team_root: ?root }])
            ))?;
            let sig = one_value(find!(
                sig: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ policy_credential_sig: ?sig }])
            ))?;
            let basis = find!(
                parent: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ effect_parent: ?parent }])
            )
            .collect();
            Self::CredentialAccepted {
                team_root,
                sig,
                basis,
            }
        } else if kind == EVENT_FOUNDER_GRANT_SELECTED {
            let team_root = one_value(find!(
                root: VerifyingKey,
                pattern!(&set, [{ event @ policy_team_root: ?root }])
            ))?;
            let scope_root = one_value(find!(
                scope: Id,
                pattern!(&set, [{ event @ policy_scope: ?scope }])
            ))?;
            let supersedes = find!(
                parent: Inline<Handle<SimpleArchive>>,
                pattern!(&set, [{ event @ effect_parent: ?parent }])
            )
            .collect();
            Self::FounderGrantSelected {
                team_root,
                scope_root,
                supersedes,
            }
        } else {
            return Err(RecipientEventError::UnknownKind(kind));
        };

        if RecipientEvent::to_blob(&decoded).bytes != blob.bytes {
            return Err(RecipientEventError::NonCanonical);
        }
        Ok(decoded)
    }
}

fn exactly_one<T>(iter: &mut impl Iterator<Item = T>) -> Result<T, RecipientEventError> {
    match (iter.next(), iter.next()) {
        (Some(value), None) => Ok(value),
        _ => Err(RecipientEventError::Malformed),
    }
}

fn one_value<T>(mut iter: impl Iterator<Item = T>) -> Result<T, RecipientEventError> {
    exactly_one(&mut iter)
}

#[derive(Debug)]
pub enum RecipientEventError {
    Archive(UnarchiveError),
    Malformed,
    UnknownKind(Id),
    NonCanonical,
}

impl From<UnarchiveError> for RecipientEventError {
    fn from(value: UnarchiveError) -> Self {
        Self::Archive(value)
    }
}

impl fmt::Display for RecipientEventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Archive(error) => write!(f, "recipient event is not a SimpleArchive: {error}"),
            Self::Malformed => write!(f, "recipient event has a missing or repeated field"),
            Self::UnknownKind(kind) => write!(f, "unknown recipient event kind {kind:?}"),
            Self::NonCanonical => write!(f, "recipient event contains non-canonical facts"),
        }
    }
}

impl Error for RecipientEventError {}

fn canonical_label() -> SubsumptionLabel {
    SubsumptionLabel::from_raw([0u8; 32])
}

/// Sign one canonical event value for the author's fixed recipient ledger.
pub fn sign_recipient_event(key: &SigningKey, event: &RecipientEvent) -> PinAssertion {
    PinAssertion::sign(
        key,
        RecipientLedgerDescriptor::pin_handle(),
        ValueHandle::from_raw(event.handle().raw),
        canonical_label(),
    )
}

/// Reinterpret an asserted recipient value as its canonical archive handle.
pub fn recipient_event_handle(value: ValueHandle) -> RecipientEventHandle {
    Inline::new(value.raw())
}

type RecipientStorageError = Box<dyn Error + Send + Sync>;

/// Durable receipt for one recipient event publication.
///
/// A receipt is intentionally not an operational ledger view. Another writer
/// may append a concurrent fact immediately after publication, so callers must
/// take a fresh assertion snapshot and resolve it before causing host effects.
/// Successful return means the complete supplied content closure was flushed
/// before the assertion crossed its durable append boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecipientEventReceipt {
    event: RecipientEventHandle,
    assertion: PinAssertionId,
}

impl RecipientEventReceipt {
    pub const fn event(&self) -> RecipientEventHandle {
        self.event
    }

    pub const fn assertion(&self) -> PinAssertionId {
        self.assertion
    }
}

/// Failure to validate or durably publish one recipient event.
#[derive(Debug)]
pub enum RecipientLedgerWriteError {
    Snapshot(RecipientStorageError),
    SnapshotCollision(PinAssertionKeyCollision),
    Reader(RecipientStorageError),
    Read {
        handle: Inline<Handle<SimpleArchive>>,
        source: RecipientStorageError,
    },
    Incomplete {
        missing: Vec<Inline<Handle<SimpleArchive>>>,
        unknown_parents: Vec<RecipientEventHandle>,
    },
    Invalid {
        diagnostics: Vec<RecipientLedgerDiagnostic>,
    },
    PostconditionFailed {
        event: RecipientEventHandle,
    },
    Put {
        stage: &'static str,
        source: RecipientStorageError,
    },
    PutHandleMismatch {
        stage: &'static str,
        expected: [u8; 32],
        actual: [u8; 32],
    },
    VerifyStored {
        stage: &'static str,
        source: RecipientStorageError,
    },
    StoredContentMismatch {
        stage: &'static str,
        handle: [u8; 32],
    },
    Flush(RecipientStorageError),
    Append(RecipientStorageError),
}

impl fmt::Display for RecipientLedgerWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => {
                write!(f, "failed to snapshot recipient assertions: {error}")
            }
            Self::SnapshotCollision(error) => {
                write!(
                    f,
                    "failed to overlay prospective recipient assertion: {error}"
                )
            }
            Self::Reader(error) => write!(f, "failed to open recipient blob reader: {error}"),
            Self::Read { handle, source } => {
                write!(f, "failed to read recipient blob {handle:?}: {source}")
            }
            Self::Incomplete {
                missing,
                unknown_parents,
            } => write!(
                f,
                "prospective recipient ledger is incomplete ({} missing blobs, {} unknown parents)",
                missing.len(),
                unknown_parents.len()
            ),
            Self::Invalid { diagnostics } => write!(
                f,
                "prospective recipient ledger is invalid ({} diagnostics)",
                diagnostics.len()
            ),
            Self::PostconditionFailed { event } => write!(
                f,
                "prospective recipient ledger omitted candidate event {event:?}"
            ),
            Self::Put { stage, source } => {
                write!(f, "failed to store recipient {stage}: {source}")
            }
            Self::PutHandleMismatch {
                stage,
                expected,
                actual,
            } => write!(
                f,
                "recipient {stage} stored under the wrong handle: expected {}, got {}",
                hex::encode_upper(expected),
                hex::encode_upper(actual)
            ),
            Self::VerifyStored { stage, source } => {
                write!(f, "failed to verify stored recipient {stage}: {source}")
            }
            Self::StoredContentMismatch { stage, handle } => write!(
                f,
                "stored recipient {stage} has wrong bytes under handle {}",
                hex::encode_upper(handle)
            ),
            Self::Flush(error) => write!(f, "failed to flush recipient closure: {error}"),
            Self::Append(error) => write!(f, "failed to append recipient assertion: {error}"),
        }
    }
}

impl Error for RecipientLedgerWriteError {
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
            Self::Incomplete { .. }
            | Self::Invalid { .. }
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
/// or transitively by the event. Existing dependencies may be omitted: the
/// prospective reducer reads them through a pinned store reader. Supplied
/// blobs are normalized from their bytes, never trusted cached handles. No
/// storage mutation occurs until the candidate assertion has reduced to
/// [`RecipientLedgerResolution::Complete`] and that view contains the exact
/// candidate event.
///
/// Publication writes the supplied closure, event, inner descriptor, and
/// strong descriptor; verifies their exact stored handles and bytes; flushes
/// all content; then durably appends the assertion. There is intentionally no
/// flush after `append_pin_assertion`: durability on return is that trait's
/// contract. An exact retry rewrites absent or corrupt content and descriptors
/// before safely eliding an already-durable duplicate assertion.
pub fn append_validated_recipient_event<S, I>(
    store: &mut S,
    author: &SigningKey,
    event: RecipientEvent,
    closure: I,
) -> Result<RecipientEventReceipt, RecipientLedgerWriteError>
where
    S: BlobStore + StorageFlush + PinAssertionStore,
    I: IntoIterator<Item = Blob<SimpleArchive>>,
{
    let event_blob = RecipientEvent::to_blob(&event);
    let event_handle = event_blob.get_handle();
    let assertion = sign_recipient_event(author, &event);

    let mut overlay = BTreeMap::new();
    for blob in closure {
        let blob = Blob::new(blob.bytes);
        overlay.insert(blob.get_handle(), blob);
    }
    overlay.insert(event_handle, event_blob.clone());
    let inner = RecipientLedgerDescriptor::blob();
    let inner_handle = inner.get_handle();
    let outer = RecipientLedgerDescriptor::strong_blob();
    let outer_handle = outer.get_handle();

    let mut snapshot = store
        .pin_assertion_snapshot()
        .map_err(|error| RecipientLedgerWriteError::Snapshot(Box::new(error)))?;
    let already_asserted = snapshot
        .for_pin(&RecipientLedgerDescriptor::pin_identity(
            author.verifying_key(),
        ))
        .contains(&assertion);
    snapshot
        .insert(assertion)
        .map_err(RecipientLedgerWriteError::SnapshotCollision)?;
    let reader = store
        .reader()
        .map_err(|error| RecipientLedgerWriteError::Reader(Box::new(error)))?;

    // BlobStoreGet exposes no portable NotFound discriminator. Preserve the
    // first raw read failure instead of guessing that it means semantic
    // absence; supplied overlay members remain available prospectively.
    let mut read_error = None;
    let resolution = resolve_recipient_ledger(&snapshot, author.verifying_key(), |handle| {
        if let Some(blob) = overlay.get(&handle) {
            return Some(blob.clone());
        }
        if read_error.is_some() {
            return None;
        }
        match reader.get::<Blob<SimpleArchive>, SimpleArchive>(handle) {
            Ok(blob) => Some(blob),
            Err(error) => {
                read_error = Some((handle, Box::new(error) as RecipientStorageError));
                None
            }
        }
    });

    // A replicated assertion may precede its content. Elide all puts only if
    // this exact assertion already exists and every supplied member plus both
    // descriptors is present with the exact expected bytes.
    let closure_present = already_asserted
        && overlay.values().all(|expected| {
            verify_stored_recipient_blob(&reader, "closure member", expected).is_ok()
        })
        && verify_stored_recipient_blob(&reader, "inner descriptor", &inner).is_ok()
        && verify_stored_recipient_blob(&reader, "strong descriptor", &outer).is_ok();
    drop(reader);

    if let Some((handle, source)) = read_error {
        return Err(RecipientLedgerWriteError::Read { handle, source });
    }
    match resolution {
        RecipientLedgerResolution::Complete(view)
            if view.event_handles().contains(&event_handle) => {}
        RecipientLedgerResolution::Complete(_) => {
            return Err(RecipientLedgerWriteError::PostconditionFailed {
                event: event_handle,
            });
        }
        RecipientLedgerResolution::Incomplete {
            missing,
            unknown_parents,
        } => {
            return Err(RecipientLedgerWriteError::Incomplete {
                missing,
                unknown_parents,
            });
        }
        RecipientLedgerResolution::Invalid { diagnostics } => {
            return Err(RecipientLedgerWriteError::Invalid { diagnostics });
        }
    }

    if closure_present {
        // Reader visibility is not proof of crash durability. Flush on exact
        // retries even though the already-durable assertion append is elided.
        store
            .flush()
            .map_err(|error| RecipientLedgerWriteError::Flush(Box::new(error)))?;
        return Ok(RecipientEventReceipt {
            event: event_handle,
            assertion: assertion.id(),
        });
    }

    for (handle, blob) in &overlay {
        if *handle == event_handle {
            continue;
        }
        let actual = store
            .put::<SimpleArchive, _>(blob.clone())
            .map_err(|error| RecipientLedgerWriteError::Put {
                stage: "closure blob",
                source: Box::new(error),
            })?;
        require_recipient_stored_handle("closure blob", handle.raw, actual.raw)?;
    }

    let actual_event = store.put::<SimpleArchive, _>(event_blob).map_err(|error| {
        RecipientLedgerWriteError::Put {
            stage: "event blob",
            source: Box::new(error),
        }
    })?;
    require_recipient_stored_handle("event blob", event_handle.raw, actual_event.raw)?;

    let actual_inner = store
        .put::<RecipientLedgerDescriptor, _>(inner.clone())
        .map_err(|error| RecipientLedgerWriteError::Put {
            stage: "inner descriptor",
            source: Box::new(error),
        })?;
    require_recipient_stored_handle("inner descriptor", inner_handle.raw, actual_inner.raw)?;

    let actual_outer = store
        .put::<StrongPinDescriptor, _>(outer.clone())
        .map_err(|error| RecipientLedgerWriteError::Put {
            stage: "strong descriptor",
            source: Box::new(error),
        })?;
    require_recipient_stored_handle("strong descriptor", outer_handle.raw, actual_outer.raw)?;

    let verification_reader = store
        .reader()
        .map_err(|error| RecipientLedgerWriteError::Reader(Box::new(error)))?;
    for blob in overlay.values() {
        let stage = if blob.get_handle() == event_handle {
            "event blob"
        } else {
            "closure blob"
        };
        verify_stored_recipient_blob(&verification_reader, stage, blob)?;
    }
    verify_stored_recipient_blob(&verification_reader, "inner descriptor", &inner)?;
    verify_stored_recipient_blob(&verification_reader, "strong descriptor", &outer)?;
    drop(verification_reader);

    store
        .flush()
        .map_err(|error| RecipientLedgerWriteError::Flush(Box::new(error)))?;
    if !already_asserted {
        store
            .append_pin_assertion(assertion)
            .map_err(|error| RecipientLedgerWriteError::Append(Box::new(error)))?;
    }

    Ok(RecipientEventReceipt {
        event: event_handle,
        assertion: assertion.id(),
    })
}

fn require_recipient_stored_handle(
    stage: &'static str,
    expected: [u8; 32],
    actual: [u8; 32],
) -> Result<(), RecipientLedgerWriteError> {
    if expected == actual {
        Ok(())
    } else {
        Err(RecipientLedgerWriteError::PutHandleMismatch {
            stage,
            expected,
            actual,
        })
    }
}

fn verify_stored_recipient_blob<R, S>(
    reader: &R,
    stage: &'static str,
    expected: &Blob<S>,
) -> Result<(), RecipientLedgerWriteError>
where
    R: BlobStoreGet,
    S: BlobEncoding + 'static,
    Handle<S>: triblespace_core::inline::InlineEncoding,
{
    let handle = expected.get_handle();
    let stored = reader.get::<Blob<S>, S>(handle).map_err(|error| {
        RecipientLedgerWriteError::VerifyStored {
            stage,
            source: Box::new(error),
        }
    })?;
    if stored.bytes != expected.bytes {
        return Err(RecipientLedgerWriteError::StoredContentMismatch {
            stage,
            handle: handle.raw,
        });
    }
    Ok(())
}

/// Result of reducing one exact recipient author's asserted event set.
#[derive(Debug)]
pub enum RecipientLedgerResolution {
    Complete(RecipientLedgerView),
    Incomplete {
        /// Content handles asserted or transitively required but unavailable.
        missing: Vec<Inline<Handle<SimpleArchive>>>,
        /// Causal parents whose event values may be present but are not yet
        /// asserted by this author. A child-before-parent union lands here.
        unknown_parents: Vec<RecipientEventHandle>,
    },
    Invalid {
        diagnostics: Vec<RecipientLedgerDiagnostic>,
    },
}

#[derive(Debug)]
pub enum RecipientLedgerDiagnostic {
    HandleMismatch {
        expected: Inline<Handle<SimpleArchive>>,
        actual: Inline<Handle<SimpleArchive>>,
    },
    InvalidEvent {
        handle: RecipientEventHandle,
        error: RecipientEventError,
    },
    WrongParentKind {
        event: RecipientEventHandle,
        parent: RecipientEventHandle,
        expected: ParentKind,
        actual: RecipientEventKind,
    },
    CrossTeamFounderSupersession {
        event: RecipientEventHandle,
        parent: RecipientEventHandle,
        event_team: VerifyingKey,
        parent_team: VerifyingKey,
    },
    CrossTeamIntentSupersession {
        event: RecipientEventHandle,
        parent: RecipientEventHandle,
        event_team: VerifyingKey,
        parent_team: VerifyingKey,
    },
    EmptyCredentialBasis {
        event: RecipientEventHandle,
    },
    MultipleIntentBasis {
        event: RecipientEventHandle,
        count: usize,
    },
    InvalidIntent {
        event: RecipientEventHandle,
        partial_cap: Inline<Handle<SimpleArchive>>,
        reason: InvalidIntentReason,
    },
    InvalidCredential {
        event: RecipientEventHandle,
        sig: Inline<Handle<SimpleArchive>>,
        reason: InvalidCredentialReason,
    },
    CausalCycle {
        events: BTreeSet<RecipientEventHandle>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParentKind {
    IntentDeclared,
    CredentialAccepted,
    FounderGrantSelected,
    IntentOrCredential,
}

#[derive(Debug)]
pub enum InvalidIntentReason {
    Claim(VerifyError),
    SubjectMismatch { declared: VerifyingKey },
}

#[derive(Debug)]
pub enum InvalidCredentialReason {
    Proof(VerifyError),
    RequestIssuerMismatch {
        requested: VerifyingKey,
        accepted: VerifyingKey,
    },
    RequestTeamMismatch {
        requested: VerifyingKey,
        accepted: VerifyingKey,
    },
    ExceedsRequestedScope,
    ExceedsRequestedExpiry {
        requested: Epoch,
        accepted: Epoch,
    },
    ParentTeamMismatch,
    ParentIssuerMismatch,
    WeakerThanParentScope,
    EarlierThanParentExpiry {
        parent: Epoch,
        accepted: Epoch,
    },
}

/// Complete deterministic projection of one recipient author's effects.
#[derive(Debug)]
pub struct RecipientLedgerView {
    author: VerifyingKey,
    event_handles: BTreeSet<RecipientEventHandle>,
    intent_frontiers: BTreeMap<[u8; 32], BTreeMap<RecipientEventHandle, IntentFrontierEntry>>,
    pending_intents: BTreeMap<[u8; 32], BTreeMap<RecipientEventHandle, PendingIntent>>,
    credentials: BTreeMap<[u8; 32], RecipientCredentialResolution>,
    founder_grants: BTreeMap<[u8; 32], FounderGrantResolution>,
    inactive_acceptances: BTreeSet<RecipientEventHandle>,
}

impl RecipientLedgerView {
    pub fn author(&self) -> VerifyingKey {
        self.author
    }

    pub fn event_handles(&self) -> &BTreeSet<RecipientEventHandle> {
        &self.event_handles
    }

    /// Unsuperseded declaration frontier, including disposed tips.
    ///
    /// A writer reuses an identical [`IntentDisposition::Pending`] event for an
    /// exact retry. After cancellation it instead declares the same partial
    /// cap while superseding that canceled declaration handle, which changes
    /// the intrinsic event handle without a nonce.
    pub fn intent_frontiers(
        &self,
    ) -> &BTreeMap<[u8; 32], BTreeMap<RecipientEventHandle, IntentFrontierEntry>> {
        &self.intent_frontiers
    }

    pub fn intent_frontier(
        &self,
        team_root: VerifyingKey,
    ) -> Option<&BTreeMap<RecipientEventHandle, IntentFrontierEntry>> {
        self.intent_frontiers.get(&team_root.to_bytes())
    }

    /// Active intent frontier after explicit supersession, cancellation, and
    /// successful acceptance consumption.
    pub fn pending_intents(
        &self,
    ) -> &BTreeMap<[u8; 32], BTreeMap<RecipientEventHandle, PendingIntent>> {
        &self.pending_intents
    }

    pub fn pending_intents_for(
        &self,
        team_root: VerifyingKey,
    ) -> Option<&BTreeMap<RecipientEventHandle, PendingIntent>> {
        self.pending_intents.get(&team_root.to_bytes())
    }

    /// Per-team accepted-credential frontiers, keyed by team-root bytes.
    pub fn credentials(&self) -> &BTreeMap<[u8; 32], RecipientCredentialResolution> {
        &self.credentials
    }

    pub fn credential(&self, team_root: VerifyingKey) -> Option<&RecipientCredentialResolution> {
        self.credentials.get(&team_root.to_bytes())
    }

    /// Per-team founder grant selectors, keyed by team-root bytes.
    pub fn founder_grants(&self) -> &BTreeMap<[u8; 32], FounderGrantResolution> {
        &self.founder_grants
    }

    pub fn founder_grant(&self, team_root: VerifyingKey) -> Option<&FounderGrantResolution> {
        self.founder_grants.get(&team_root.to_bytes())
    }

    /// Validly signed acceptance events made inert because an intent in their
    /// causal roots was later canceled or superseded in the merged view.
    pub fn inactive_acceptances(&self) -> &BTreeSet<RecipientEventHandle> {
        &self.inactive_acceptances
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentDisposition {
    Pending,
    Canceled,
    Accepted,
}

#[derive(Debug, Clone)]
pub struct IntentFrontierEntry {
    event: RecipientEventHandle,
    team_root: [u8; 32],
    partial_cap: Inline<Handle<SimpleArchive>>,
    claim: OperationalCapability,
    disposition: IntentDisposition,
}

impl IntentFrontierEntry {
    pub const fn event(&self) -> RecipientEventHandle {
        self.event
    }

    pub fn team_root(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.team_root)
            .expect("intent frontier entry is constructed from a checked key")
    }

    pub const fn partial_cap(&self) -> Inline<Handle<SimpleArchive>> {
        self.partial_cap
    }

    pub fn claim(&self) -> &OperationalCapability {
        &self.claim
    }

    pub const fn disposition(&self) -> IntentDisposition {
        self.disposition
    }
}

#[derive(Debug, Clone)]
pub struct PendingIntent {
    event: RecipientEventHandle,
    team_root: [u8; 32],
    partial_cap: Inline<Handle<SimpleArchive>>,
    claim: OperationalCapability,
}

impl PendingIntent {
    pub const fn event(&self) -> RecipientEventHandle {
        self.event
    }

    pub fn team_root(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.team_root)
            .expect("pending intent is constructed from a checked key")
    }

    pub const fn partial_cap(&self) -> Inline<Handle<SimpleArchive>> {
        self.partial_cap
    }

    pub fn claim(&self) -> &OperationalCapability {
        &self.claim
    }
}

#[derive(Debug)]
pub enum RecipientCredentialResolution {
    Unaccepted,
    Current {
        credential: CurrentRecipientCredential,
        /// Every causally maximal acceptance event for this team, including
        /// weaker or duplicate-value tips that the next writer must join.
        frontier: BTreeSet<RecipientEventHandle>,
    },
    Conflicted {
        /// Complete causal frontier, never only the semantic maxima.
        frontier: BTreeSet<RecipientEventHandle>,
        /// Distinct incomparable raw credential maxima, keyed by signature.
        candidates: BTreeMap<Inline<Handle<SimpleArchive>>, CurrentRecipientCredential>,
    },
}

impl RecipientCredentialResolution {
    pub fn frontier(&self) -> Option<&BTreeSet<RecipientEventHandle>> {
        match self {
            Self::Unaccepted => None,
            Self::Current { frontier, .. } | Self::Conflicted { frontier, .. } => Some(frontier),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CurrentRecipientCredential {
    team_root: VerifyingKey,
    cap: Inline<Handle<SimpleArchive>>,
    sig: Inline<Handle<SimpleArchive>>,
    issuer: VerifyingKey,
    capability: VerifiedCapability,
}

impl CurrentRecipientCredential {
    pub fn team_root(&self) -> VerifyingKey {
        self.team_root
    }

    pub const fn cap(&self) -> Inline<Handle<SimpleArchive>> {
        self.cap
    }

    pub const fn sig(&self) -> Inline<Handle<SimpleArchive>> {
        self.sig
    }

    pub fn issuer(&self) -> VerifyingKey {
        self.issuer
    }

    pub fn capability(&self) -> &VerifiedCapability {
        &self.capability
    }

    pub fn effective_expiry(&self) -> Epoch {
        self.capability.expires_at()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FounderGrantSelection {
    team_root: [u8; 32],
    scope_root: Id,
    frontier: BTreeSet<RecipientEventHandle>,
}

impl FounderGrantSelection {
    pub fn team_root(&self) -> VerifyingKey {
        VerifyingKey::from_bytes(&self.team_root)
            .expect("founder selection is constructed from a checked key")
    }

    pub const fn scope_root(&self) -> Id {
        self.scope_root
    }

    /// Complete same-team causal selection frontier that a replacement must
    /// supersede, even when every tip selects this same scope.
    pub fn frontier(&self) -> &BTreeSet<RecipientEventHandle> {
        &self.frontier
    }
}

#[derive(Debug)]
pub enum FounderGrantResolution {
    Unselected,
    Current(FounderGrantSelection),
    Conflicted {
        frontier: BTreeSet<RecipientEventHandle>,
        scopes: BTreeSet<Id>,
    },
}

#[derive(Clone, Debug)]
struct VerifiedAcceptance {
    event: RecipientEventHandle,
    team_root: VerifyingKey,
    sig: Inline<Handle<SimpleArchive>>,
    basis: BTreeSet<RecipientEventHandle>,
    proof: VerifiedCapabilityChain,
}

impl VerifiedAcceptance {
    fn current(&self) -> CurrentRecipientCredential {
        CurrentRecipientCredential {
            team_root: self.team_root,
            cap: self.proof.leaf_cap,
            sig: self.sig,
            issuer: self.proof.leaf_issuer,
            capability: self.proof.capability.clone(),
        }
    }
}

/// Reduce one exact author's monotone recipient assertion set.
///
/// Fetches, including misses, are memoized for the fold so one call observes a
/// coherent content boundary. Only [`RecipientLedgerResolution::Complete`]
/// exposes operational state; callers must resolve afresh after assertion
/// union or content arrival.
///
/// V1 deliberately gates completeness at the whole author ledger rather than
/// per team. One missing or invalid event therefore withholds every team's
/// projection. This is safe but creates coarse availability coupling between
/// otherwise independent team partitions.
pub fn resolve_recipient_ledger<F>(
    snapshot: &PinAssertionSnapshot,
    author: VerifyingKey,
    mut fetch_blob: F,
) -> RecipientLedgerResolution
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let identity = RecipientLedgerDescriptor::pin_identity(author);
    let asserted_handles: BTreeSet<_> = snapshot
        .for_pin(&identity)
        .into_iter()
        .map(|assertion| recipient_event_handle(assertion.value()))
        .collect();

    let mut cache = BTreeMap::new();
    let mut missing = BTreeSet::new();
    let mut diagnostics = Vec::new();
    let mut events = BTreeMap::new();

    for &handle in &asserted_handles {
        if let Some(event) = read_event(
            handle,
            &mut cache,
            &mut fetch_blob,
            &mut missing,
            &mut diagnostics,
        ) {
            events.insert(handle, event);
        }
    }

    let unknown_parents: BTreeSet<_> = events
        .values()
        .flat_map(RecipientEvent::parents)
        .filter(|parent| !asserted_handles.contains(parent))
        .collect();

    validate_parent_kinds(&events, &mut diagnostics);

    let mut intent_claims = BTreeMap::new();
    for (&event, value) in &events {
        let RecipientEvent::IntentDeclared { partial_cap, .. } = value else {
            continue;
        };
        let Some(blob) = read_blob(
            *partial_cap,
            &mut cache,
            &mut fetch_blob,
            &mut missing,
            &mut diagnostics,
        ) else {
            continue;
        };
        match decode_operational_capability(blob) {
            Err(error) => diagnostics.push(RecipientLedgerDiagnostic::InvalidIntent {
                event,
                partial_cap: *partial_cap,
                reason: InvalidIntentReason::Claim(error),
            }),
            Ok(claim) if claim.subject != author => {
                diagnostics.push(RecipientLedgerDiagnostic::InvalidIntent {
                    event,
                    partial_cap: *partial_cap,
                    reason: InvalidIntentReason::SubjectMismatch {
                        declared: claim.subject,
                    },
                });
            }
            Ok(claim) => {
                intent_claims.insert(event, claim);
            }
        }
    }

    let mut acceptances = BTreeMap::new();
    for (&event, value) in &events {
        let RecipientEvent::CredentialAccepted {
            team_root,
            sig,
            basis,
        } = value
        else {
            continue;
        };
        match verify_chain_details_allow_expired(*team_root, *sig, author, |handle| {
            read_blob(
                handle,
                &mut cache,
                &mut fetch_blob,
                &mut missing,
                &mut diagnostics,
            )
        }) {
            Ok(proof) => {
                acceptances.insert(
                    event,
                    VerifiedAcceptance {
                        event,
                        team_root: *team_root,
                        sig: *sig,
                        basis: basis.clone(),
                        proof,
                    },
                );
            }
            Err(VerifyError::MissingBlob(handle)) => {
                missing.insert(handle);
            }
            Err(error) => diagnostics.push(RecipientLedgerDiagnostic::InvalidCredential {
                event,
                sig: *sig,
                reason: InvalidCredentialReason::Proof(error),
            }),
        }
    }

    validate_acceptance_semantics(&events, &intent_claims, &acceptances, &mut diagnostics);

    if !diagnostics.is_empty() {
        return RecipientLedgerResolution::Invalid { diagnostics };
    }
    if !missing.is_empty() || !unknown_parents.is_empty() {
        return RecipientLedgerResolution::Incomplete {
            missing: missing.into_iter().collect(),
            unknown_parents: unknown_parents.into_iter().collect(),
        };
    }

    let mut superseded_intents = BTreeSet::<RecipientEventHandle>::new();
    let mut canceled_intents = BTreeSet::<RecipientEventHandle>::new();
    let mut superseded_founder_selections = BTreeSet::<RecipientEventHandle>::new();
    for value in events.values() {
        match value {
            RecipientEvent::IntentDeclared { supersedes, .. } => {
                superseded_intents.extend(supersedes.iter().copied());
            }
            RecipientEvent::IntentCanceled { intent } => {
                canceled_intents.insert(*intent);
            }
            RecipientEvent::FounderGrantSelected { supersedes, .. } => {
                superseded_founder_selections.extend(supersedes.iter().copied());
            }
            RecipientEvent::CredentialAccepted { .. } => {}
        }
    }
    let unsuperseded_intents: BTreeSet<_> = events
        .iter()
        .filter_map(|(handle, event)| {
            matches!(event, RecipientEvent::IntentDeclared { .. })
                .then_some(*handle)
                .filter(|handle| !superseded_intents.contains(handle))
        })
        .collect();
    let active_intents: BTreeSet<_> = unsuperseded_intents
        .difference(&canceled_intents)
        .copied()
        .collect();

    let acceptance_roots = match acceptance_roots(&events, &acceptances) {
        Ok(roots) => roots,
        Err(events) => {
            return RecipientLedgerResolution::Invalid {
                diagnostics: vec![RecipientLedgerDiagnostic::CausalCycle { events }],
            };
        }
    };
    let eligible_acceptances: BTreeSet<_> = acceptance_roots
        .iter()
        .filter_map(|(event, roots)| {
            roots
                .iter()
                .all(|root| active_intents.contains(root))
                .then_some(*event)
        })
        .collect();
    let inactive_acceptances = acceptances
        .keys()
        .copied()
        .filter(|event| !eligible_acceptances.contains(event))
        .collect();

    let consumed_intents: BTreeSet<_> = eligible_acceptances
        .iter()
        .flat_map(|event| acceptance_roots[event].iter().copied())
        .collect();
    let mut intent_frontiers =
        BTreeMap::<[u8; 32], BTreeMap<RecipientEventHandle, IntentFrontierEntry>>::new();
    for event in &unsuperseded_intents {
        let RecipientEvent::IntentDeclared {
            team_root,
            partial_cap,
            ..
        } = &events[event]
        else {
            unreachable!("intent frontier contains declarations only")
        };
        let disposition = if canceled_intents.contains(event) {
            IntentDisposition::Canceled
        } else if consumed_intents.contains(event) {
            IntentDisposition::Accepted
        } else {
            IntentDisposition::Pending
        };
        intent_frontiers
            .entry(team_root.to_bytes())
            .or_default()
            .insert(
                *event,
                IntentFrontierEntry {
                    event: *event,
                    team_root: team_root.to_bytes(),
                    partial_cap: *partial_cap,
                    claim: intent_claims[event].clone(),
                    disposition,
                },
            );
    }
    let mut pending_intents =
        BTreeMap::<[u8; 32], BTreeMap<RecipientEventHandle, PendingIntent>>::new();
    for event in active_intents.difference(&consumed_intents) {
        let RecipientEvent::IntentDeclared {
            team_root,
            partial_cap,
            ..
        } = &events[event]
        else {
            unreachable!("active intent set contains declarations only")
        };
        pending_intents
            .entry(team_root.to_bytes())
            .or_default()
            .insert(
                *event,
                PendingIntent {
                    event: *event,
                    team_root: team_root.to_bytes(),
                    partial_cap: *partial_cap,
                    claim: intent_claims[event].clone(),
                },
            );
    }

    let credentials = project_credentials(&acceptances, &eligible_acceptances);
    let founder_grants = project_founder_grants(&events, &superseded_founder_selections);

    RecipientLedgerResolution::Complete(RecipientLedgerView {
        author,
        event_handles: asserted_handles,
        intent_frontiers,
        pending_intents,
        credentials,
        founder_grants,
        inactive_acceptances,
    })
}

fn validate_parent_kinds(
    events: &BTreeMap<RecipientEventHandle, RecipientEvent>,
    diagnostics: &mut Vec<RecipientLedgerDiagnostic>,
) {
    for (&event_handle, event) in events {
        match event {
            RecipientEvent::IntentDeclared {
                team_root: event_team,
                supersedes,
                ..
            } => {
                for parent in supersedes {
                    require_parent_kind(
                        events,
                        event_handle,
                        *parent,
                        ParentKind::IntentDeclared,
                        RecipientEventKind::IntentDeclared,
                        diagnostics,
                    );
                    let Some(RecipientEvent::IntentDeclared {
                        team_root: parent_team,
                        ..
                    }) = events.get(parent)
                    else {
                        continue;
                    };
                    if event_team != parent_team {
                        diagnostics.push(RecipientLedgerDiagnostic::CrossTeamIntentSupersession {
                            event: event_handle,
                            parent: *parent,
                            event_team: *event_team,
                            parent_team: *parent_team,
                        });
                    }
                }
            }
            RecipientEvent::IntentCanceled { intent } => require_parent_kind(
                events,
                event_handle,
                *intent,
                ParentKind::IntentDeclared,
                RecipientEventKind::IntentDeclared,
                diagnostics,
            ),
            RecipientEvent::FounderGrantSelected { supersedes, .. } => {
                for parent in supersedes {
                    require_parent_kind(
                        events,
                        event_handle,
                        *parent,
                        ParentKind::FounderGrantSelected,
                        RecipientEventKind::FounderGrantSelected,
                        diagnostics,
                    );
                    let Some(RecipientEvent::FounderGrantSelected {
                        team_root: parent_team,
                        ..
                    }) = events.get(parent)
                    else {
                        continue;
                    };
                    let RecipientEvent::FounderGrantSelected {
                        team_root: event_team,
                        ..
                    } = event
                    else {
                        unreachable!()
                    };
                    if event_team != parent_team {
                        diagnostics.push(RecipientLedgerDiagnostic::CrossTeamFounderSupersession {
                            event: event_handle,
                            parent: *parent,
                            event_team: *event_team,
                            parent_team: *parent_team,
                        });
                    }
                }
            }
            RecipientEvent::CredentialAccepted { basis, .. } => {
                if basis.is_empty() {
                    diagnostics.push(RecipientLedgerDiagnostic::EmptyCredentialBasis {
                        event: event_handle,
                    });
                    continue;
                }
                // Absence is monotone-incomplete, not evidence of a malformed
                // basis. Wait until every asserted parent event is readable
                // before deciding whether the set is homogeneous.
                if basis.iter().any(|parent| !events.contains_key(parent)) {
                    continue;
                }
                let kinds: BTreeSet<_> = basis
                    .iter()
                    .filter_map(|parent| events.get(parent).map(RecipientEvent::kind))
                    .collect();
                let expected = if kinds == BTreeSet::from([RecipientEventKind::IntentDeclared]) {
                    if basis.len() != 1 {
                        diagnostics.push(RecipientLedgerDiagnostic::MultipleIntentBasis {
                            event: event_handle,
                            count: basis.len(),
                        });
                    }
                    Some(RecipientEventKind::IntentDeclared)
                } else if kinds == BTreeSet::from([RecipientEventKind::CredentialAccepted]) {
                    Some(RecipientEventKind::CredentialAccepted)
                } else if kinds.is_empty() {
                    None
                } else {
                    for parent in basis {
                        if let Some(actual) = events.get(parent).map(RecipientEvent::kind) {
                            diagnostics.push(RecipientLedgerDiagnostic::WrongParentKind {
                                event: event_handle,
                                parent: *parent,
                                expected: ParentKind::IntentOrCredential,
                                actual,
                            });
                        }
                    }
                    None
                };
                if let Some(expected) = expected {
                    for parent in basis {
                        require_parent_kind(
                            events,
                            event_handle,
                            *parent,
                            if expected == RecipientEventKind::IntentDeclared {
                                ParentKind::IntentDeclared
                            } else {
                                ParentKind::CredentialAccepted
                            },
                            expected,
                            diagnostics,
                        );
                    }
                }
            }
        }
    }
}

fn require_parent_kind(
    events: &BTreeMap<RecipientEventHandle, RecipientEvent>,
    event: RecipientEventHandle,
    parent: RecipientEventHandle,
    expected_label: ParentKind,
    expected: RecipientEventKind,
    diagnostics: &mut Vec<RecipientLedgerDiagnostic>,
) {
    let Some(actual) = events.get(&parent).map(RecipientEvent::kind) else {
        return;
    };
    if actual != expected {
        diagnostics.push(RecipientLedgerDiagnostic::WrongParentKind {
            event,
            parent,
            expected: expected_label,
            actual,
        });
    }
}

fn validate_acceptance_semantics(
    events: &BTreeMap<RecipientEventHandle, RecipientEvent>,
    intents: &BTreeMap<RecipientEventHandle, OperationalCapability>,
    acceptances: &BTreeMap<RecipientEventHandle, VerifiedAcceptance>,
    diagnostics: &mut Vec<RecipientLedgerDiagnostic>,
) {
    for acceptance in acceptances.values() {
        let parent_kinds: BTreeSet<_> = acceptance
            .basis
            .iter()
            .filter_map(|parent| events.get(parent).map(RecipientEvent::kind))
            .collect();
        if parent_kinds == BTreeSet::from([RecipientEventKind::IntentDeclared])
            && acceptance.basis.len() == 1
        {
            let parent = *acceptance.basis.first().expect("one intent basis");
            let Some(request) = intents.get(&parent) else {
                continue;
            };
            let RecipientEvent::IntentDeclared {
                team_root: requested_team,
                ..
            } = &events[&parent]
            else {
                unreachable!("validated first basis is an intent")
            };
            let reason = if *requested_team != acceptance.team_root {
                Some(InvalidCredentialReason::RequestTeamMismatch {
                    requested: *requested_team,
                    accepted: acceptance.team_root,
                })
            } else if request.issuer != acceptance.proof.leaf_issuer {
                Some(InvalidCredentialReason::RequestIssuerMismatch {
                    requested: request.issuer,
                    accepted: acceptance.proof.leaf_issuer,
                })
            } else if !scope_subsumes(
                &request.cap_set,
                request.scope_root,
                &acceptance.proof.capability.cap_set,
                acceptance.proof.capability.scope_root,
            ) {
                Some(InvalidCredentialReason::ExceedsRequestedScope)
            } else if acceptance.proof.capability.expires_at() > request.expires_at {
                Some(InvalidCredentialReason::ExceedsRequestedExpiry {
                    requested: request.expires_at,
                    accepted: acceptance.proof.capability.expires_at(),
                })
            } else {
                None
            };
            if let Some(reason) = reason {
                diagnostics.push(RecipientLedgerDiagnostic::InvalidCredential {
                    event: acceptance.event,
                    sig: acceptance.sig,
                    reason,
                });
            }
        } else if parent_kinds == BTreeSet::from([RecipientEventKind::CredentialAccepted]) {
            for parent in &acceptance.basis {
                let Some(parent) = acceptances.get(parent) else {
                    continue;
                };
                if let Err(reason) = product_dominates(acceptance, parent) {
                    diagnostics.push(RecipientLedgerDiagnostic::InvalidCredential {
                        event: acceptance.event,
                        sig: acceptance.sig,
                        reason,
                    });
                }
            }
        }
    }
}

fn product_dominates(
    candidate: &VerifiedAcceptance,
    parent: &VerifiedAcceptance,
) -> Result<(), InvalidCredentialReason> {
    if candidate.team_root != parent.team_root {
        return Err(InvalidCredentialReason::ParentTeamMismatch);
    }
    if candidate.proof.leaf_issuer != parent.proof.leaf_issuer {
        return Err(InvalidCredentialReason::ParentIssuerMismatch);
    }
    if !scope_subsumes(
        &candidate.proof.capability.cap_set,
        candidate.proof.capability.scope_root,
        &parent.proof.capability.cap_set,
        parent.proof.capability.scope_root,
    ) {
        return Err(InvalidCredentialReason::WeakerThanParentScope);
    }
    let parent_expiry = parent.proof.capability.expires_at();
    let candidate_expiry = candidate.proof.capability.expires_at();
    if candidate_expiry < parent_expiry {
        return Err(InvalidCredentialReason::EarlierThanParentExpiry {
            parent: parent_expiry,
            accepted: candidate_expiry,
        });
    }
    Ok(())
}

fn acceptance_roots(
    events: &BTreeMap<RecipientEventHandle, RecipientEvent>,
    acceptances: &BTreeMap<RecipientEventHandle, VerifiedAcceptance>,
) -> Result<
    BTreeMap<RecipientEventHandle, BTreeSet<RecipientEventHandle>>,
    BTreeSet<RecipientEventHandle>,
> {
    let mut roots = BTreeMap::<RecipientEventHandle, BTreeSet<RecipientEventHandle>>::new();
    let mut unresolved: BTreeSet<_> = acceptances.keys().copied().collect();
    loop {
        let ready: Vec<_> = unresolved
            .iter()
            .copied()
            .filter(|event| {
                let Some(acceptance) = acceptances.get(event) else {
                    return false;
                };
                let first = acceptance.basis.iter().all(|parent| {
                    matches!(
                        events.get(parent),
                        Some(RecipientEvent::IntentDeclared { .. })
                    )
                });
                first
                    || acceptance.basis.iter().all(|parent| {
                        matches!(
                            events.get(parent),
                            Some(RecipientEvent::CredentialAccepted { .. })
                        ) && roots.contains_key(parent)
                    })
            })
            .collect();
        if ready.is_empty() {
            break;
        }
        let mut made_progress = false;
        for event in ready {
            let Some(acceptance) = acceptances.get(&event) else {
                continue;
            };
            let mut event_roots = BTreeSet::<RecipientEventHandle>::new();
            let mut complete = true;
            for parent in &acceptance.basis {
                match events.get(parent) {
                    Some(RecipientEvent::IntentDeclared { .. }) => {
                        event_roots.insert(*parent);
                    }
                    Some(RecipientEvent::CredentialAccepted { .. }) => {
                        let Some(parent_roots) = roots.get(parent) else {
                            complete = false;
                            break;
                        };
                        event_roots.extend(parent_roots.iter().copied());
                    }
                    Some(RecipientEvent::IntentCanceled { .. })
                    | Some(RecipientEvent::FounderGrantSelected { .. })
                    | None => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                roots.insert(event, event_roots);
                unresolved.remove(&event);
                made_progress = true;
            }
        }
        if !made_progress {
            break;
        }
    }
    if unresolved.is_empty() {
        Ok(roots)
    } else {
        Err(unresolved)
    }
}

fn project_credentials(
    acceptances: &BTreeMap<RecipientEventHandle, VerifiedAcceptance>,
    eligible: &BTreeSet<RecipientEventHandle>,
) -> BTreeMap<[u8; 32], RecipientCredentialResolution> {
    let mut teams = BTreeMap::<[u8; 32], BTreeSet<RecipientEventHandle>>::new();
    for acceptance in acceptances.values() {
        teams
            .entry(acceptance.team_root.to_bytes())
            .or_default()
            .insert(acceptance.event);
    }
    teams
        .into_iter()
        .map(|(team, asserted)| {
            let eligible = asserted
                .intersection(eligible)
                .copied()
                .collect::<BTreeSet<_>>();
            (team, project_team_credential(acceptances, &eligible))
        })
        .collect()
}

fn project_team_credential(
    acceptances: &BTreeMap<RecipientEventHandle, VerifiedAcceptance>,
    eligible: &BTreeSet<RecipientEventHandle>,
) -> RecipientCredentialResolution {
    let mut causal_frontier = eligible.clone();
    for event in eligible {
        for parent in &acceptances[event].basis {
            if eligible.contains(parent) {
                causal_frontier.remove(parent);
            }
        }
    }

    let maximal: BTreeSet<_> = causal_frontier
        .iter()
        .copied()
        .filter(|candidate| {
            !causal_frontier.iter().any(|other| {
                candidate != other
                    && semantically_dominates(&acceptances[other], &acceptances[candidate])
                    && !semantically_dominates(&acceptances[candidate], &acceptances[other])
            })
        })
        .collect();
    if maximal.is_empty() {
        return RecipientCredentialResolution::Unaccepted;
    }

    // Collapse only exact raw credentials. Two events carrying the same
    // signature name the same cap/proof tuple, but their causal tips remain
    // distinct and are all retained in `causal_frontier` for the next write.
    let mut candidates = BTreeMap::new();
    for event in maximal {
        let acceptance = &acceptances[&event];
        candidates
            .entry(acceptance.sig)
            .or_insert_with(|| acceptance.current());
    }
    if candidates.len() == 1 {
        let credential = candidates
            .into_values()
            .next()
            .expect("one maximal raw credential");
        RecipientCredentialResolution::Current {
            credential,
            frontier: causal_frontier,
        }
    } else {
        // Semantically equivalent but raw-distinct credentials remain
        // explicit too. A signature/hash tie-break would erase concurrency.
        RecipientCredentialResolution::Conflicted {
            frontier: causal_frontier,
            candidates,
        }
    }
}

fn semantically_dominates(left: &VerifiedAcceptance, right: &VerifiedAcceptance) -> bool {
    product_dominates(left, right).is_ok()
}

fn project_founder_grants(
    events: &BTreeMap<RecipientEventHandle, RecipientEvent>,
    superseded: &BTreeSet<RecipientEventHandle>,
) -> BTreeMap<[u8; 32], FounderGrantResolution> {
    let mut teams = BTreeMap::<[u8; 32], Vec<(RecipientEventHandle, Id)>>::new();
    for (event, team_root, scope_root) in events.iter().filter_map(|(event, value)| {
        let RecipientEvent::FounderGrantSelected {
            team_root,
            scope_root,
            ..
        } = value
        else {
            return None;
        };
        (!superseded.contains(event)).then_some((*event, *team_root, *scope_root))
    }) {
        teams
            .entry(team_root.to_bytes())
            .or_default()
            .push((event, scope_root));
    }
    teams
        .into_iter()
        .map(|(team, selections)| {
            let frontier = selections
                .iter()
                .map(|(event, _)| *event)
                .collect::<BTreeSet<_>>();
            let scopes = selections
                .iter()
                .map(|(_, scope)| *scope)
                .collect::<BTreeSet<_>>();
            let resolution = if scopes.len() == 1 {
                FounderGrantResolution::Current(FounderGrantSelection {
                    team_root: team,
                    scope_root: *scopes.first().expect("one selected scope"),
                    frontier,
                })
            } else {
                FounderGrantResolution::Conflicted { frontier, scopes }
            };
            (team, resolution)
        })
        .collect()
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
    diagnostics: &mut Vec<RecipientLedgerDiagnostic>,
) -> Option<Blob<SimpleArchive>>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let Some(blob) = cached_fetch(cache, fetch_blob, handle) else {
        missing.insert(handle);
        return None;
    };
    let actual = Inline::new(Blake3::digest(&blob.bytes));
    if actual != handle {
        diagnostics.push(RecipientLedgerDiagnostic::HandleMismatch {
            expected: handle,
            actual,
        });
        return None;
    }
    Some(blob)
}

fn read_event<F>(
    handle: RecipientEventHandle,
    cache: &mut BTreeMap<Inline<Handle<SimpleArchive>>, Option<Blob<SimpleArchive>>>,
    fetch_blob: &mut F,
    missing: &mut BTreeSet<Inline<Handle<SimpleArchive>>>,
    diagnostics: &mut Vec<RecipientLedgerDiagnostic>,
) -> Option<RecipientEvent>
where
    F: FnMut(Inline<Handle<SimpleArchive>>) -> Option<Blob<SimpleArchive>>,
{
    let blob = read_blob(handle, cache, fetch_blob, missing, diagnostics)?;
    match RecipientEvent::decode(blob) {
        Ok(event) => Some(event),
        Err(error) => {
            diagnostics.push(RecipientLedgerDiagnostic::InvalidEvent { handle, error });
            None
        }
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
        lie_about_put_handle: bool,
    }

    impl BlobStorePut for RecordingStore {
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
            if self.lie_about_put_handle {
                Ok(Inline::new([0xEE; 32]))
            } else {
                Ok(handle)
            }
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

    fn handle(byte: u8) -> RecipientEventHandle {
        Inline::new([byte; 32])
    }

    struct Fixture {
        root: SigningKey,
        issuer: SigningKey,
        recipient: SigningKey,
        scope_root: Id,
        now: Epoch,
        anchor_cap: Blob<SimpleArchive>,
        anchor_sig: Blob<SimpleArchive>,
        blobs: BTreeMap<RecipientEventHandle, Blob<SimpleArchive>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_key_bytes(21, 22, 23)
        }

        fn with_key_bytes(root_byte: u8, issuer_byte: u8, recipient_byte: u8) -> Self {
            let root = key(root_byte);
            let issuer = key(issuer_byte);
            let recipient = key(recipient_byte);
            let scope_root = *triblespace_core::id::ufoid();
            let scope = TribleSet::from(entity! {
                ExclusiveId::force_ref(&scope_root) @ metadata::tag: PERM_ADMIN,
            });
            let (anchor_cap, anchor_sig) =
                build_founder_anchor(&root, issuer.verifying_key(), scope_root, scope)
                    .expect("founder anchor");
            let mut blobs = BTreeMap::new();
            blobs.insert(anchor_cap.get_handle(), anchor_cap.clone());
            blobs.insert(anchor_sig.get_handle(), anchor_sig.clone());
            Self {
                root,
                issuer,
                recipient,
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
                .expect("valid interval")
        }

        fn scope(&self, permission: Id) -> TribleSet {
            TribleSet::from(entity! {
                ExclusiveId::force_ref(&self.scope_root) @ metadata::tag: permission,
            })
        }

        fn partial_cap(&mut self, permission: Id, seconds: f64) -> RecipientEventHandle {
            let fragment = entity! {
                capability::cap_subject: self.recipient.verifying_key(),
                capability::cap_issuer: self.issuer.verifying_key(),
                capability::cap_scope_root: self.scope_root,
                metadata::expires_at: self.interval(seconds),
            };
            let mut set = TribleSet::from(fragment);
            set += self.scope(permission);
            let blob = set.to_blob();
            let handle = blob.get_handle();
            self.blobs.insert(handle, blob);
            handle
        }

        fn publishable_intent(
            &mut self,
            permission: Id,
            seconds: f64,
        ) -> (RecipientEvent, Blob<SimpleArchive>) {
            let partial_cap = self.partial_cap(permission, seconds);
            let claim = self
                .blobs
                .get(&partial_cap)
                .expect("fixture stored partial capability")
                .clone();
            (
                RecipientEvent::IntentDeclared {
                    team_root: self.root.verifying_key(),
                    partial_cap,
                    supersedes: BTreeSet::new(),
                },
                claim,
            )
        }

        fn credential(
            &mut self,
            permission: Id,
            seconds: f64,
        ) -> (RecipientEventHandle, RecipientEventHandle) {
            let (cap, sig) = build_capability(
                &self.issuer,
                self.recipient.verifying_key(),
                (self.anchor_cap.clone(), self.anchor_sig.clone()),
                self.scope_root,
                self.scope(permission),
                self.interval(seconds),
            )
            .expect("finite recipient credential");
            let cap_handle = cap.get_handle();
            let sig_handle = sig.get_handle();
            self.blobs.insert(cap_handle, cap);
            self.blobs.insert(sig_handle, sig);
            (cap_handle, sig_handle)
        }

        fn store_event(&mut self, event: &RecipientEvent) -> RecipientEventHandle {
            let blob = event.to_blob();
            let handle = blob.get_handle();
            self.blobs.insert(handle, blob);
            handle
        }

        fn insert_event(
            &mut self,
            snapshot: &mut PinAssertionSnapshot,
            event: &RecipientEvent,
        ) -> RecipientEventHandle {
            let handle = self.store_event(event);
            snapshot
                .insert(sign_recipient_event(&self.recipient, event))
                .expect("unique assertion key");
            handle
        }

        fn resolve(&self, snapshot: &PinAssertionSnapshot) -> RecipientLedgerResolution {
            resolve_recipient_ledger(snapshot, self.recipient.verifying_key(), |handle| {
                self.blobs.get(&handle).cloned()
            })
        }
    }

    #[test]
    fn validated_writer_flushes_complete_closure_before_durable_assertion() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let event_handle = event.handle();
        let assertion = sign_recipient_event(&fixture.recipient, &event);
        let mut store = RecordingStore::default();
        let supplied =
            Blob::<SimpleArchive>::with_handle(partial_cap.bytes.clone(), Inline::new([0xA1; 32]));
        assert_ne!(supplied.get_handle(), partial_cap.get_handle());

        let receipt =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, [supplied])
                .expect("valid prospective event publishes");
        assert_eq!(receipt.event(), event_handle);
        assert_eq!(receipt.assertion(), assertion.id());
        assert_eq!(
            store.operations,
            [
                "snapshot", "reader", "put", "put", "put", "put", "reader", "flush", "append"
            ]
        );
        assert_eq!(
            store.put_handles,
            [
                partial_cap.get_handle().raw,
                event_handle.raw,
                RecipientLedgerDescriptor::descriptor_handle().raw,
                RecipientLedgerDescriptor::strong_blob().get_handle().raw,
            ]
        );

        let snapshot = store.inner.pin_assertion_snapshot().unwrap();
        let reader = store.inner.reader().unwrap();
        let RecipientLedgerResolution::Complete(view) =
            resolve_recipient_ledger(&snapshot, fixture.recipient.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            })
        else {
            panic!("durably published closure must resolve after the write")
        };
        assert!(view.event_handles().contains(&event_handle));
    }

    #[test]
    fn validated_writer_mutates_nothing_for_an_invalid_candidate() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let wrong_author = key(71);
        let mut store = RecordingStore::default();

        let error =
            append_validated_recipient_event(&mut store, &wrong_author, event, [partial_cap])
                .unwrap_err();
        assert!(matches!(error, RecipientLedgerWriteError::Invalid { .. }));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert_eq!(store.inner.blobs.len(), 0);
        assert!(store.inner.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn validated_writer_mutates_nothing_for_an_unasserted_causal_parent() {
        let mut fixture = Fixture::new();
        let missing_parent = handle(92);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let event = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([missing_parent]),
        };
        let closure = fixture.blobs.values().cloned().collect::<Vec<_>>();
        let mut store = RecordingStore::default();

        let error =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, closure)
                .unwrap_err();
        assert!(matches!(
            error,
            RecipientLedgerWriteError::Incomplete {
                missing,
                unknown_parents,
            } if missing.is_empty() && unknown_parents == vec![missing_parent]
        ));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert_eq!(store.inner.blobs.len(), 0);
        assert!(store.inner.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn validated_writer_never_appends_before_flush_and_leaves_only_safe_orphans_on_failure() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);

        let mut flush_failure = RecordingStore {
            fail_flush: true,
            ..RecordingStore::default()
        };
        let error = append_validated_recipient_event(
            &mut flush_failure,
            &fixture.recipient,
            event.clone(),
            [partial_cap.clone()],
        )
        .unwrap_err();
        assert!(matches!(error, RecipientLedgerWriteError::Flush(_)));
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
        let error = append_validated_recipient_event(
            &mut append_failure,
            &fixture.recipient,
            event,
            [partial_cap],
        )
        .unwrap_err();
        assert!(matches!(error, RecipientLedgerWriteError::Append(_)));
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
    fn validated_writer_exact_republication_is_idempotent() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let mut store = RecordingStore::default();

        let first = append_validated_recipient_event(
            &mut store,
            &fixture.recipient,
            event.clone(),
            [partial_cap.clone()],
        )
        .expect("first publication succeeds");
        assert_eq!(store.inner.blobs.len(), 4);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);

        store.operations.clear();
        store.put_handles.clear();
        let second =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, [partial_cap])
                .expect("exact republication succeeds");
        assert_eq!(second, first);
        assert_eq!(store.inner.blobs.len(), 4);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);
        assert_eq!(store.operations, ["snapshot", "reader", "flush"]);
        assert!(store.put_handles.is_empty());
    }

    #[test]
    fn validated_writer_repairs_an_assertion_whose_content_has_not_arrived() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let event_handle = event.handle();
        let assertion = sign_recipient_event(&fixture.recipient, &event);
        let mut store = RecordingStore::default();
        store
            .inner
            .append_pin_assertion(assertion)
            .expect("seed assertion without its closure");

        let receipt =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, [partial_cap])
                .expect("exact retry repairs missing content");
        assert_eq!(receipt.event(), event_handle);
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
            resolve_recipient_ledger(&snapshot, fixture.recipient.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            }),
            RecipientLedgerResolution::Complete(_)
        ));
    }

    #[test]
    fn validated_writer_preserves_existing_blob_read_errors() {
        let mut fixture = Fixture::new();
        let (candidate, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let dangling = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: fixture.scope_root,
            supersedes: BTreeSet::new(),
        };
        let dangling_handle = dangling.handle();
        let mut store = RecordingStore::default();
        store
            .inner
            .append_pin_assertion(sign_recipient_event(&fixture.recipient, &dangling))
            .expect("seed assertion without event content");

        let error = append_validated_recipient_event(
            &mut store,
            &fixture.recipient,
            candidate,
            [partial_cap],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            RecipientLedgerWriteError::Read { handle, .. } if handle == dangling_handle
        ));
        assert_eq!(store.operations, ["snapshot", "reader"]);
        assert_eq!(store.inner.blobs.len(), 0);
        assert_eq!(store.inner.pin_assertion_snapshot().unwrap().len(), 1);
    }

    #[test]
    fn validated_writer_rejects_a_backend_returning_the_wrong_put_handle() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let mut store = RecordingStore {
            lie_about_put_handle: true,
            ..RecordingStore::default()
        };

        let error =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, [partial_cap])
                .unwrap_err();
        assert!(matches!(
            error,
            RecipientLedgerWriteError::PutHandleMismatch {
                stage: "closure blob",
                ..
            }
        ));
        assert_eq!(store.operations, ["snapshot", "reader", "put"]);
        assert_eq!(store.inner.blobs.len(), 1);
        assert!(store.inner.pin_assertion_snapshot().unwrap().is_empty());
    }

    #[test]
    fn validated_writer_does_not_trust_cached_handles_on_duplicate_presence_check() {
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let event_handle = event.handle();
        let mut store = RecordingStore::default();
        append_validated_recipient_event(
            &mut store,
            &fixture.recipient,
            event.clone(),
            [partial_cap.clone()],
        )
        .expect("seed valid publication");

        let poisoned = Blob::<UnknownBlob>::with_handle(
            Bytes::from_source(b"wrong bytes under the event handle".to_vec()),
            Inline::new(event_handle.raw),
        );
        let existing = store.inner.blobs.reader().unwrap();
        store.inner.blobs = existing
            .iter()
            .map(|(handle, blob)| {
                if handle.raw == event_handle.raw {
                    (handle, poisoned.clone())
                } else {
                    (handle, blob)
                }
            })
            .collect();
        store.operations.clear();
        store.put_handles.clear();

        let error =
            append_validated_recipient_event(&mut store, &fixture.recipient, event, [partial_cap])
                .expect_err("a backend retaining poisoned content must fail closed");
        assert!(matches!(
            error,
            RecipientLedgerWriteError::StoredContentMismatch {
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
        let mut fixture = Fixture::new();
        let (event, partial_cap) = fixture.publishable_intent(PERM_READ, 1_000.0);
        let event_handle = event.handle();
        let dir = tempfile::tempdir().expect("temporary pile directory");
        let path = dir.path().join("recipient-ledger.pile");
        std::fs::File::create(&path).expect("create empty pile");

        {
            let mut pile = Pile::open(&path).expect("open recipient pile");
            let receipt = append_validated_recipient_event(
                &mut pile,
                &fixture.recipient,
                event,
                [partial_cap],
            )
            .expect("durably publish recipient intent");
            assert_eq!(receipt.event(), event_handle);
            // Deliberately drop without another flush. Content was flushed
            // first; the assertion append is durable on return.
        }

        let mut reopened = Pile::open(&path).expect("reopen recipient pile");
        let snapshot = reopened
            .pin_assertion_snapshot()
            .expect("replay durable assertion");
        assert_eq!(snapshot.len(), 1);
        let reader = reopened.reader().expect("open replay reader");
        reader
            .get::<Blob<RecipientLedgerDescriptor>, RecipientLedgerDescriptor>(
                RecipientLedgerDescriptor::descriptor_handle(),
            )
            .expect("replay inner recipient descriptor");
        reader
            .get::<Blob<StrongPinDescriptor>, StrongPinDescriptor>(
                RecipientLedgerDescriptor::strong_blob().get_handle(),
            )
            .expect("replay strong recipient descriptor");
        let RecipientLedgerResolution::Complete(view) =
            resolve_recipient_ledger(&snapshot, fixture.recipient.verifying_key(), |handle| {
                reader
                    .get::<Blob<SimpleArchive>, SimpleArchive>(handle)
                    .ok()
            })
        else {
            panic!("one-flush publication must replay as a complete ledger")
        };
        assert!(view.event_handles().contains(&event_handle));
    }

    #[test]
    fn descriptor_and_all_event_codecs_are_canonical() {
        let inner = RecipientLedgerDescriptor::blob();
        assert_eq!(
            inner.get_handle(),
            RecipientLedgerDescriptor::descriptor_handle()
        );
        assert_eq!(
            inner
                .clone()
                .try_from_blob::<RecipientLedgerDescriptor>()
                .unwrap(),
            RecipientLedgerDescriptor
        );
        assert_eq!(
            RecipientLedgerDescriptor::strong_blob().get_handle().raw,
            RecipientLedgerDescriptor::pin_handle().raw()
        );

        let author = key(1).verifying_key();
        let scope_root = triblespace_core::id::id_hex!("00112233445566778899AABBCCDDEEFF");
        let events = [
            RecipientEvent::IntentDeclared {
                team_root: author,
                partial_cap: handle(1),
                supersedes: BTreeSet::from([handle(2), handle(3)]),
            },
            RecipientEvent::IntentCanceled { intent: handle(4) },
            RecipientEvent::CredentialAccepted {
                team_root: author,
                sig: handle(5),
                basis: BTreeSet::from([handle(6), handle(7)]),
            },
            RecipientEvent::FounderGrantSelected {
                team_root: author,
                scope_root,
                supersedes: BTreeSet::from([handle(8), handle(9)]),
            },
        ];
        for event in events {
            assert_eq!(
                RecipientEvent::decode(RecipientEvent::to_blob(&event)).unwrap(),
                event
            );
        }

        let forward = RecipientEvent::IntentDeclared {
            team_root: author,
            partial_cap: handle(10),
            supersedes: [handle(11), handle(12), handle(11)].into_iter().collect(),
        };
        let reverse = RecipientEvent::IntentDeclared {
            team_root: author,
            partial_cap: handle(10),
            supersedes: [handle(12), handle(11)].into_iter().collect(),
        };
        assert_eq!(
            RecipientEvent::to_blob(&forward).bytes,
            RecipientEvent::to_blob(&reverse).bytes
        );
        assert_eq!(forward.handle(), reverse.handle());

        let canonical = RecipientEvent::to_blob(&forward);
        let mut set: TribleSet = TryFromBlob::try_from_blob(canonical).unwrap();
        let extra = triblespace_core::id::id_hex!("FFEEDDCCBBAA99887766554433221100");
        set += entity! {
            ExclusiveId::force_ref(&extra) @ policy_scope: extra,
        };
        assert!(matches!(
            RecipientEvent::decode(set.to_blob()),
            Err(RecipientEventError::NonCanonical)
        ));
    }

    #[test]
    fn assertion_union_order_does_not_change_intent_frontier() {
        let mut fixture = Fixture::new();
        let first = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let second = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_WRITE, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let first_handle = fixture.store_event(&first);
        let second_handle = fixture.store_event(&second);
        let first_assertion = sign_recipient_event(&fixture.recipient, &first);
        let second_assertion = sign_recipient_event(&fixture.recipient, &second);

        let mut left = PinAssertionSnapshot::new();
        left.insert(first_assertion).unwrap();
        let mut right = PinAssertionSnapshot::new();
        right.insert(second_assertion).unwrap();
        let mut left_then_right = left;
        left_then_right.union(right).unwrap();

        let mut right = PinAssertionSnapshot::new();
        right.insert(second_assertion).unwrap();
        let mut left = PinAssertionSnapshot::new();
        left.insert(first_assertion).unwrap();
        let mut right_then_left = right;
        right_then_left.union(left).unwrap();

        let pending = |resolution: RecipientLedgerResolution| match resolution {
            RecipientLedgerResolution::Complete(view) => view
                .pending_intents_for(fixture.root.verifying_key())
                .expect("team has pending intents")
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            other => panic!("expected complete view, got {other:?}"),
        };
        let expected = BTreeSet::from([first_handle, second_handle]);
        assert_eq!(pending(fixture.resolve(&left_then_right)), expected);
        assert_eq!(pending(fixture.resolve(&right_then_left)), expected);
    }

    #[test]
    fn missing_asserted_event_content_is_incomplete() {
        let mut fixture = Fixture::new();
        let event = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let event_handle = event.handle();
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot
            .insert(sign_recipient_event(&fixture.recipient, &event))
            .unwrap();

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Incomplete { missing, unknown_parents }
                if missing == vec![event_handle] && unknown_parents.is_empty()
        ));
    }

    #[test]
    fn child_before_parent_assertion_is_incomplete_then_heals() {
        let mut fixture = Fixture::new();
        let parent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let parent_handle = fixture.store_event(&parent);
        let child = RecipientEvent::IntentCanceled {
            intent: parent_handle,
        };
        let mut snapshot = PinAssertionSnapshot::new();
        fixture.insert_event(&mut snapshot, &child);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Incomplete { missing, unknown_parents }
                if missing.is_empty() && unknown_parents == vec![parent_handle]
        ));

        snapshot
            .insert(sign_recipient_event(&fixture.recipient, &parent))
            .unwrap();
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("parent assertion should complete the view")
        };
        assert!(
            view.pending_intents_for(fixture.root.verifying_key())
                .is_none_or(BTreeMap::is_empty)
        );
    }

    #[test]
    fn credential_acceptance_with_an_unasserted_parent_is_incomplete_not_a_panic() {
        let mut fixture = Fixture::new();
        let missing_parent = handle(91);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let acceptance = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([missing_parent]),
        };
        let mut snapshot = PinAssertionSnapshot::new();
        fixture.insert_event(&mut snapshot, &acceptance);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Incomplete {
                missing,
                unknown_parents,
            } if missing.is_empty() && unknown_parents == vec![missing_parent]
        ));
    }

    #[test]
    fn identical_retry_after_cancel_uses_the_canceled_declaration_as_predecessor() {
        let mut fixture = Fixture::new();
        let partial_cap = fixture.partial_cap(PERM_READ, 1_000.0);
        let declaration = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap,
            supersedes: BTreeSet::new(),
        };
        let mut snapshot = PinAssertionSnapshot::new();
        let declaration_handle = fixture.insert_event(&mut snapshot, &declaration);

        // A normal exact retry is the same content-addressed event and the
        // assertion snapshot deduplicates it.
        assert_eq!(declaration.handle(), declaration_handle);
        snapshot
            .insert(sign_recipient_event(&fixture.recipient, &declaration))
            .unwrap();
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("exact pending retry")
        };
        assert_eq!(snapshot.len(), 1);
        assert!(matches!(
            view.intent_frontier(fixture.root.verifying_key())
                .and_then(|frontier| frontier.get(&declaration_handle)),
            Some(entry) if entry.disposition() == IntentDisposition::Pending
        ));

        let canceled = RecipientEvent::IntentCanceled {
            intent: declaration_handle,
        };
        fixture.insert_event(&mut snapshot, &canceled);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("canceled intent")
        };
        assert!(matches!(
            view.intent_frontier(fixture.root.verifying_key())
                .and_then(|frontier| frontier.get(&declaration_handle)),
            Some(entry)
                if entry.partial_cap() == partial_cap
                    && entry.disposition() == IntentDisposition::Canceled
        ));

        let retry = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap,
            supersedes: BTreeSet::from([declaration_handle]),
        };
        let retry_handle = fixture.insert_event(&mut snapshot, &retry);
        assert_ne!(retry_handle, declaration_handle);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("retry after cancellation")
        };
        assert_eq!(
            view.intent_frontier(fixture.root.verifying_key())
                .expect("team intent frontier")
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([retry_handle])
        );
        assert!(matches!(
            view.intent_frontier(fixture.root.verifying_key())
                .and_then(|frontier| frontier.get(&retry_handle)),
            Some(entry) if entry.disposition() == IntentDisposition::Pending
        ));
    }

    #[test]
    fn request_supersession_projects_the_visible_frontier() {
        let mut fixture = Fixture::new();
        let first = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let mut snapshot = PinAssertionSnapshot::new();
        let first_handle = fixture.insert_event(&mut snapshot, &first);
        let replacement = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_WRITE, 1_000.0),
            supersedes: BTreeSet::from([first_handle]),
        };
        let replacement_handle = fixture.insert_event(&mut snapshot, &replacement);
        let concurrent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let concurrent_handle = fixture.insert_event(&mut snapshot, &concurrent);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid intent frontier")
        };
        assert_eq!(
            view.pending_intents_for(fixture.root.verifying_key())
                .expect("team has pending frontier")
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([replacement_handle, concurrent_handle])
        );
    }

    #[test]
    fn intent_cannot_supersede_another_team_frontier() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let first = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let first_handle = fixture.insert_event(&mut snapshot, &first);
        let crossed = RecipientEvent::IntentDeclared {
            team_root: key(31).verifying_key(),
            partial_cap: fixture.partial_cap(PERM_WRITE, 1_000.0),
            supersedes: BTreeSet::from([first_handle]),
        };
        let crossed_handle = fixture.insert_event(&mut snapshot, &crossed);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Invalid { diagnostics }
                if diagnostics.iter().any(|diagnostic| matches!(
                    diagnostic,
                    RecipientLedgerDiagnostic::CrossTeamIntentSupersession {
                        event,
                        parent,
                        ..
                    } if *event == crossed_handle && *parent == first_handle
                ))
        ));
    }

    #[test]
    fn founder_selection_replacement_preserves_unseen_concurrency() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let first = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: fixture.scope_root,
            supersedes: BTreeSet::new(),
        };
        let first_handle = fixture.insert_event(&mut snapshot, &first);
        let replacement_scope = *triblespace_core::id::ufoid();
        let replacement = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: replacement_scope,
            supersedes: BTreeSet::from([first_handle]),
        };
        let replacement_handle = fixture.insert_event(&mut snapshot, &replacement);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid founder selection")
        };
        assert!(matches!(
            view.founder_grant(fixture.root.verifying_key()),
            Some(FounderGrantResolution::Current(selection))
                if selection.frontier() == &BTreeSet::from([replacement_handle])
                    && selection.scope_root() == replacement_scope
        ));

        // Same operational selection through a different causal path remains
        // Current, but both tips are retained for the next replacement.
        let same_scope = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: replacement_scope,
            supersedes: BTreeSet::new(),
        };
        let same_scope_handle = fixture.insert_event(&mut snapshot, &same_scope);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("same founder grant through concurrent paths")
        };
        assert!(matches!(
            view.founder_grant(fixture.root.verifying_key()),
            Some(FounderGrantResolution::Current(selection))
                if selection.scope_root() == replacement_scope
                    && selection.frontier()
                        == &BTreeSet::from([replacement_handle, same_scope_handle])
        ));

        let concurrent = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: *triblespace_core::id::ufoid(),
            supersedes: BTreeSet::new(),
        };
        let concurrent_handle = fixture.insert_event(&mut snapshot, &concurrent);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid concurrent founder selections")
        };
        assert!(matches!(
            view.founder_grant(fixture.root.verifying_key()),
            Some(FounderGrantResolution::Conflicted { frontier, .. })
                if frontier == &BTreeSet::from([
                    replacement_handle,
                    same_scope_handle,
                    concurrent_handle,
                ])
        ));

        let other_team = key(44).verifying_key();
        let unrelated = RecipientEvent::FounderGrantSelected {
            team_root: other_team,
            scope_root: *triblespace_core::id::ufoid(),
            supersedes: BTreeSet::new(),
        };
        let unrelated_handle = fixture.insert_event(&mut snapshot, &unrelated);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("unrelated team selector must remain independent")
        };
        assert!(matches!(
            view.founder_grant(other_team),
            Some(FounderGrantResolution::Current(selection))
                if selection.frontier() == &BTreeSet::from([unrelated_handle])
        ));
        assert_eq!(view.founder_grants().len(), 2);
    }

    #[test]
    fn founder_selection_cannot_supersede_another_team() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let first = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: fixture.scope_root,
            supersedes: BTreeSet::new(),
        };
        let first_handle = fixture.insert_event(&mut snapshot, &first);
        let crossed = RecipientEvent::FounderGrantSelected {
            team_root: key(77).verifying_key(),
            scope_root: *triblespace_core::id::ufoid(),
            supersedes: BTreeSet::from([first_handle]),
        };
        let crossed_handle = fixture.insert_event(&mut snapshot, &crossed);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Invalid { diagnostics }
                if diagnostics.iter().any(|diagnostic| matches!(
                    diagnostic,
                    RecipientLedgerDiagnostic::CrossTeamFounderSupersession {
                        event,
                        parent,
                        ..
                    } if *event == crossed_handle && *parent == first_handle
                ))
        ));
    }

    #[test]
    fn wrong_parent_event_kind_is_invalid() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let selection = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: fixture.scope_root,
            supersedes: BTreeSet::new(),
        };
        let selection_handle = fixture.insert_event(&mut snapshot, &selection);
        let cancel = RecipientEvent::IntentCanceled {
            intent: selection_handle,
        };
        let cancel_handle = fixture.insert_event(&mut snapshot, &cancel);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Invalid { diagnostics }
                if diagnostics.iter().any(|diagnostic| matches!(
                    diagnostic,
                    RecipientLedgerDiagnostic::WrongParentKind {
                        event,
                        parent,
                        expected: ParentKind::IntentDeclared,
                        actual: RecipientEventKind::FounderGrantSelected,
                    } if *event == cancel_handle && *parent == selection_handle
                ))
        ));
    }

    #[test]
    fn accepted_signature_derives_the_exact_capability() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let intent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.insert_event(&mut snapshot, &intent);
        let (cap, sig) = fixture.credential(PERM_READ, 100.0);
        let accepted = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([intent_handle]),
        };
        fixture.insert_event(&mut snapshot, &accepted);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid acceptance")
        };
        let Some(RecipientCredentialResolution::Current {
            credential: current,
            frontier,
        }) = view.credential(fixture.root.verifying_key())
        else {
            panic!("one accepted credential should be current")
        };
        assert_eq!(frontier, &BTreeSet::from([accepted.handle()]));
        assert_eq!(current.cap(), cap);
        assert_eq!(current.sig(), sig);
        assert_eq!(current.issuer(), fixture.issuer.verifying_key());
        assert_eq!(
            current.capability().subject,
            fixture.recipient.verifying_key()
        );
        assert!(view.pending_intents().is_empty());
    }

    #[test]
    fn acceptance_cannot_replace_the_intent_trust_anchor() {
        let mut fixture = Fixture::new();
        let requested_team = key(31).verifying_key();
        let mut snapshot = PinAssertionSnapshot::new();
        let intent = RecipientEvent::IntentDeclared {
            team_root: requested_team,
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.insert_event(&mut snapshot, &intent);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let accepted = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([intent_handle]),
        };
        let accepted_handle = fixture.insert_event(&mut snapshot, &accepted);

        assert!(matches!(
            fixture.resolve(&snapshot),
            RecipientLedgerResolution::Invalid { diagnostics }
                if diagnostics.iter().any(|diagnostic| matches!(
                    diagnostic,
                    RecipientLedgerDiagnostic::InvalidCredential {
                        event,
                        reason: InvalidCredentialReason::RequestTeamMismatch {
                            requested,
                            accepted,
                        },
                        ..
                    } if *event == accepted_handle
                        && *requested == requested_team
                        && *accepted == fixture.root.verifying_key()
                ))
        ));
    }

    #[test]
    fn cancel_accept_race_is_inert_in_every_union_order_and_fresh_intent_heals() {
        let mut fixture = Fixture::new();
        let team_root = fixture.root.verifying_key();
        let partial_cap = fixture.partial_cap(PERM_ADMIN, 1_000.0);
        let intent = RecipientEvent::IntentDeclared {
            team_root,
            partial_cap,
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.store_event(&intent);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let accepted = RecipientEvent::CredentialAccepted {
            team_root,
            sig,
            basis: BTreeSet::from([intent_handle]),
        };
        let accepted_handle = fixture.store_event(&accepted);
        let canceled = RecipientEvent::IntentCanceled {
            intent: intent_handle,
        };
        fixture.store_event(&canceled);

        let intent_assertion = sign_recipient_event(&fixture.recipient, &intent);
        let accepted_assertion = sign_recipient_event(&fixture.recipient, &accepted);
        let canceled_assertion = sign_recipient_event(&fixture.recipient, &canceled);
        let merge = |first: &PinAssertion, second: &PinAssertion| {
            let mut merged = PinAssertionSnapshot::new();
            merged.insert(intent_assertion.clone()).unwrap();
            merged.insert(first.clone()).unwrap();
            let mut other = PinAssertionSnapshot::new();
            other.insert(second.clone()).unwrap();
            merged.union(other).unwrap();
            merged
        };
        let cancel_then_accept = merge(&canceled_assertion, &accepted_assertion);
        let accept_then_cancel = merge(&accepted_assertion, &canceled_assertion);

        let project = |snapshot: &PinAssertionSnapshot| {
            let RecipientLedgerResolution::Complete(view) = fixture.resolve(snapshot) else {
                panic!("cancel/accept union must be complete")
            };
            (
                matches!(
                    view.credential(team_root),
                    Some(RecipientCredentialResolution::Unaccepted)
                ),
                view.inactive_acceptances().clone(),
                view.intent_frontier(team_root)
                    .and_then(|frontier| frontier.get(&intent_handle))
                    .map(IntentFrontierEntry::disposition),
            )
        };
        let expected = (
            true,
            BTreeSet::from([accepted_handle]),
            Some(IntentDisposition::Canceled),
        );
        assert_eq!(project(&cancel_then_accept), expected);
        assert_eq!(project(&accept_then_cancel), expected);

        // Healing is an explicit fresh declaration and acceptance. Reusing
        // the raw proof is safe because the new basis changes the event id.
        let retry = RecipientEvent::IntentDeclared {
            team_root,
            partial_cap,
            supersedes: BTreeSet::from([intent_handle]),
        };
        let mut healed = cancel_then_accept;
        let retry_handle = fixture.insert_event(&mut healed, &retry);
        let fresh_acceptance = RecipientEvent::CredentialAccepted {
            team_root,
            sig,
            basis: BTreeSet::from([retry_handle]),
        };
        let fresh_handle = fixture.insert_event(&mut healed, &fresh_acceptance);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&healed) else {
            panic!("fresh post-race acceptance must heal")
        };
        assert!(matches!(
            view.credential(team_root),
            Some(RecipientCredentialResolution::Current {
                credential,
                frontier,
            }) if credential.sig() == sig && frontier == &BTreeSet::from([fresh_handle])
        ));
        assert_eq!(
            view.inactive_acceptances(),
            &BTreeSet::from([accepted_handle])
        );
    }

    #[test]
    fn replace_accept_race_is_inert_in_every_union_order() {
        let mut fixture = Fixture::new();
        let team_root = fixture.root.verifying_key();
        let original = RecipientEvent::IntentDeclared {
            team_root,
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let original_handle = fixture.store_event(&original);
        let replacement = RecipientEvent::IntentDeclared {
            team_root,
            partial_cap: fixture.partial_cap(PERM_ADMIN, 2_000.0),
            supersedes: BTreeSet::from([original_handle]),
        };
        let replacement_handle = fixture.store_event(&replacement);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let accepted = RecipientEvent::CredentialAccepted {
            team_root,
            sig,
            basis: BTreeSet::from([original_handle]),
        };
        let accepted_handle = fixture.store_event(&accepted);

        let original_assertion = sign_recipient_event(&fixture.recipient, &original);
        let replacement_assertion = sign_recipient_event(&fixture.recipient, &replacement);
        let accepted_assertion = sign_recipient_event(&fixture.recipient, &accepted);
        let merge = |first: &PinAssertion, second: &PinAssertion| {
            let mut merged = PinAssertionSnapshot::new();
            merged.insert(original_assertion.clone()).unwrap();
            merged.insert(first.clone()).unwrap();
            let mut other = PinAssertionSnapshot::new();
            other.insert(second.clone()).unwrap();
            merged.union(other).unwrap();
            merged
        };

        for snapshot in [
            merge(&replacement_assertion, &accepted_assertion),
            merge(&accepted_assertion, &replacement_assertion),
        ] {
            let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
                panic!("replace/accept union must be complete")
            };
            assert!(matches!(
                view.credential(team_root),
                Some(RecipientCredentialResolution::Unaccepted)
            ));
            assert_eq!(
                view.inactive_acceptances(),
                &BTreeSet::from([accepted_handle])
            );
            assert!(matches!(
                view.intent_frontier(team_root),
                Some(frontier)
                    if frontier.keys().copied().collect::<BTreeSet<_>>()
                        == BTreeSet::from([replacement_handle])
                        && frontier[&replacement_handle].disposition()
                            == IntentDisposition::Pending
            ));
        }
    }

    #[test]
    fn identical_credential_on_concurrent_bases_is_current_with_both_tips() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let first_intent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let first_intent_handle = fixture.insert_event(&mut snapshot, &first_intent);
        let second_intent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_WRITE, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let second_intent_handle = fixture.insert_event(&mut snapshot, &second_intent);
        let (_, sig) = fixture.credential(PERM_READ, 100.0);
        let first = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([first_intent_handle]),
        };
        let first_handle = fixture.insert_event(&mut snapshot, &first);
        let second = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig,
            basis: BTreeSet::from([second_intent_handle]),
        };
        let second_handle = fixture.insert_event(&mut snapshot, &second);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("same raw credential on two bases")
        };
        assert!(matches!(
            view.credential(fixture.root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential, frontier })
                if credential.sig() == sig
                    && frontier == &BTreeSet::from([first_handle, second_handle])
        ));
    }

    #[test]
    fn strict_credential_maximum_keeps_weaker_causal_tip() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let intent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.insert_event(&mut snapshot, &intent);
        let (_, initial_sig) = fixture.credential(PERM_READ, 100.0);
        let initial = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: initial_sig,
            basis: BTreeSet::from([intent_handle]),
        };
        let initial_handle = fixture.insert_event(&mut snapshot, &initial);

        let (_, weaker_sig) = fixture.credential(PERM_READ, 150.0);
        let weaker = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: weaker_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let weaker_handle = fixture.insert_event(&mut snapshot, &weaker);
        let (_, stronger_sig) = fixture.credential(PERM_WRITE, 200.0);
        let stronger = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: stronger_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let stronger_handle = fixture.insert_event(&mut snapshot, &stronger);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("strict product maximum")
        };
        assert!(matches!(
            view.credential(fixture.root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential, frontier })
                if credential.sig() == stronger_sig
                    && frontier == &BTreeSet::from([weaker_handle, stronger_handle])
        ));
    }

    #[test]
    fn stale_descendant_cannot_hide_an_unseen_stronger_tip() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let team_root = fixture.root.verifying_key();
        let intent = RecipientEvent::IntentDeclared {
            team_root,
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.insert_event(&mut snapshot, &intent);
        let (_, initial_sig) = fixture.credential(PERM_READ, 100.0);
        let initial = RecipientEvent::CredentialAccepted {
            team_root,
            sig: initial_sig,
            basis: BTreeSet::from([intent_handle]),
        };
        let initial_handle = fixture.insert_event(&mut snapshot, &initial);

        let (_, seen_sig) = fixture.credential(PERM_READ, 150.0);
        let seen = RecipientEvent::CredentialAccepted {
            team_root,
            sig: seen_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let seen_handle = fixture.insert_event(&mut snapshot, &seen);
        let (_, stale_descendant_sig) = fixture.credential(PERM_READ, 200.0);
        let stale_descendant = RecipientEvent::CredentialAccepted {
            team_root,
            sig: stale_descendant_sig,
            basis: BTreeSet::from([seen_handle]),
        };
        let stale_descendant_handle = fixture.insert_event(&mut snapshot, &stale_descendant);

        // This stronger sibling was unseen by the stale writer. It remains a
        // causal tip after union and wins the product order without erasing
        // the stale tip needed by a future all-frontier healing write.
        let (_, unseen_sig) = fixture.credential(PERM_WRITE, 300.0);
        let unseen = RecipientEvent::CredentialAccepted {
            team_root,
            sig: unseen_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let unseen_handle = fixture.insert_event(&mut snapshot, &unseen);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid stale descendant and unseen sibling")
        };
        assert!(matches!(
            view.credential(team_root),
            Some(RecipientCredentialResolution::Current {
                credential,
                frontier,
            }) if credential.sig() == unseen_sig
                && frontier == &BTreeSet::from([stale_descendant_handle, unseen_handle])
        ));
    }

    #[test]
    fn accepted_credentials_for_unrelated_teams_project_independently() {
        let mut first = Fixture::with_key_bytes(21, 22, 23);
        let mut second = Fixture::with_key_bytes(31, 22, 23);
        assert_eq!(
            first.recipient.verifying_key(),
            second.recipient.verifying_key()
        );
        assert_ne!(first.root.verifying_key(), second.root.verifying_key());
        assert_eq!(first.issuer.verifying_key(), second.issuer.verifying_key());

        let mut snapshot = PinAssertionSnapshot::new();
        let first_intent = RecipientEvent::IntentDeclared {
            team_root: first.root.verifying_key(),
            partial_cap: first.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let first_intent_handle = first.insert_event(&mut snapshot, &first_intent);
        let (first_cap, first_sig) = first.credential(PERM_READ, 100.0);
        let first_accepted = RecipientEvent::CredentialAccepted {
            team_root: first.root.verifying_key(),
            sig: first_sig,
            basis: BTreeSet::from([first_intent_handle]),
        };
        first.insert_event(&mut snapshot, &first_accepted);

        let second_intent = RecipientEvent::IntentDeclared {
            team_root: second.root.verifying_key(),
            partial_cap: second.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let second_intent_handle = second.insert_event(&mut snapshot, &second_intent);
        let (second_cap, second_sig) = second.credential(PERM_WRITE, 200.0);
        let second_accepted = RecipientEvent::CredentialAccepted {
            team_root: second.root.verifying_key(),
            sig: second_sig,
            basis: BTreeSet::from([second_intent_handle]),
        };
        second.insert_event(&mut snapshot, &second_accepted);

        let blobs: BTreeMap<_, _> = first
            .blobs
            .iter()
            .chain(second.blobs.iter())
            .map(|(handle, blob)| (*handle, blob.clone()))
            .collect();
        let resolution =
            resolve_recipient_ledger(&snapshot, first.recipient.verifying_key(), |handle| {
                blobs.get(&handle).cloned()
            });
        let RecipientLedgerResolution::Complete(view) = resolution else {
            panic!("two independent team credentials should resolve")
        };
        assert_eq!(view.intent_frontiers().len(), 2);
        assert!(matches!(
            view.intent_frontier(first.root.verifying_key()),
            Some(frontier)
                if frontier.values().all(|intent| intent.disposition() == IntentDisposition::Accepted)
        ));
        assert!(matches!(
            view.intent_frontier(second.root.verifying_key()),
            Some(frontier)
                if frontier.values().all(|intent| intent.disposition() == IntentDisposition::Accepted)
        ));
        assert_eq!(view.credentials().len(), 2);
        assert!(matches!(
            view.credential(first.root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential: current, .. })
                if current.cap() == first_cap && current.sig() == first_sig
        ));
        assert!(matches!(
            view.credential(second.root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential: current, .. })
                if current.cap() == second_cap && current.sig() == second_sig
        ));
    }

    #[test]
    fn incomparable_successors_remain_an_explicit_credential_conflict() {
        let mut fixture = Fixture::new();
        let mut snapshot = PinAssertionSnapshot::new();
        let intent = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_ADMIN, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let intent_handle = fixture.insert_event(&mut snapshot, &intent);
        let (_, initial_sig) = fixture.credential(PERM_READ, 100.0);
        let initial = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: initial_sig,
            basis: BTreeSet::from([intent_handle]),
        };
        let initial_handle = fixture.insert_event(&mut snapshot, &initial);

        // One successor broadens scope but expires earlier than the other;
        // both dominate the initial credential, neither dominates its sibling.
        let (_, wider_sig) = fixture.credential(PERM_WRITE, 150.0);
        let wider = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: wider_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let wider_handle = fixture.insert_event(&mut snapshot, &wider);
        let (_, longer_sig) = fixture.credential(PERM_READ, 250.0);
        let longer = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: longer_sig,
            basis: BTreeSet::from([initial_handle]),
        };
        let longer_handle = fixture.insert_event(&mut snapshot, &longer);

        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid credential fork")
        };
        assert!(matches!(
            view.credential(fixture.root.verifying_key()),
            Some(RecipientCredentialResolution::Conflicted { frontier, .. })
                if frontier == &BTreeSet::from([wider_handle, longer_handle])
        ));

        let (_, healing_sig) = fixture.credential(PERM_WRITE, 300.0);
        let healing = RecipientEvent::CredentialAccepted {
            team_root: fixture.root.verifying_key(),
            sig: healing_sig,
            basis: BTreeSet::from([wider_handle, longer_handle]),
        };
        let healing_handle = fixture.insert_event(&mut snapshot, &healing);
        let RecipientLedgerResolution::Complete(view) = fixture.resolve(&snapshot) else {
            panic!("valid fork-healing successor")
        };
        assert!(matches!(
            view.credential(fixture.root.verifying_key()),
            Some(RecipientCredentialResolution::Current { credential: current, frontier })
                if frontier == &BTreeSet::from([healing_handle]) && current.sig() == healing_sig
        ));
    }

    #[test]
    fn fetch_handle_mismatch_is_invalid() {
        let mut fixture = Fixture::new();
        let asserted = RecipientEvent::IntentDeclared {
            team_root: fixture.root.verifying_key(),
            partial_cap: fixture.partial_cap(PERM_READ, 1_000.0),
            supersedes: BTreeSet::new(),
        };
        let asserted_handle = asserted.handle();
        let substitute_event = RecipientEvent::FounderGrantSelected {
            team_root: fixture.root.verifying_key(),
            scope_root: fixture.scope_root,
            supersedes: BTreeSet::new(),
        };
        let substitute = RecipientEvent::to_blob(&substitute_event);
        let actual = substitute.get_handle();
        let mut snapshot = PinAssertionSnapshot::new();
        snapshot
            .insert(sign_recipient_event(&fixture.recipient, &asserted))
            .unwrap();

        let resolution =
            resolve_recipient_ledger(&snapshot, fixture.recipient.verifying_key(), |handle| {
                (handle == asserted_handle).then(|| substitute.clone())
            });
        assert!(matches!(
            resolution,
            RecipientLedgerResolution::Invalid { diagnostics }
                if diagnostics.iter().any(|diagnostic| matches!(
                    diagnostic,
                    RecipientLedgerDiagnostic::HandleMismatch { expected, actual: found }
                        if *expected == asserted_handle && *found == actual
                ))
        ));
    }
}
