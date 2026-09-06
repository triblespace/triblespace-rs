//! Exact per-collection semantic repair state.
//!
//! Collection records and authorization proofs are independent grow-only
//! sets. A newly arrived proof may activate an old COMMIT or admit a new reader
//! without changing the record PATCH, so a collection wake commits to both.
//! The authorization projection contains only structurally relevant
//! self-contained READ(C) or WRITE(C) proof records.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;

use ed25519_dalek::VerifyingKey;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{Blob, TryFromBlob};
use triblespace_core::capability::{
    CapabilityAction, CapabilityAtom, CapabilityProof, CapabilityProofError, CapabilityProofId,
    CapabilityResource,
};
use triblespace_core::collection::{
    ACTION_READ, ACTION_WRITE, AdmissionPolicy, CollectionDescriptorError, CollectionHandle,
    CollectionPolicy, CollectionRead, CollectionReadAudience, RecordDecodeError,
    collection_read_audience_by_policy_at, collection_read_policy,
    collection_reader_is_admitted_by_policy_at, collection_write_policy, descriptor,
};
use triblespace_core::patch::{Blake3Merkle, Entry as PatchEntry, IdentitySchema, PATCH};
use triblespace_core::repo::{BlobStoreGet, CapabilityProofRead, StoreSnapshot};
use triblespace_core::trible::TribleSet;

use crate::collection_delta::{
    CollectionRecordPatch, CollectionRecordPatchError, collection_record_patch,
};
use crate::patch_repair::PatchSummary;

const COLLECTION_REPAIR_ROOT_DOMAIN: &[u8] = b"triblespace.collection.repair-overlay\0";
const COLLECTION_REPAIR_ROOT_VERSION: u32 = 1;

type AuthorizationEvidencePatch = PATCH<32, IdentitySchema, CapabilityProof, Blake3Merkle>;

/// Canonical collection-scoped set of structurally relevant authorization proofs.
///
/// Keys and values are exact self-contained native proof records. The Merkle
/// root commits to proof ids, which commit to the complete signed paths.
#[derive(Clone, Debug)]
pub struct CollectionAuthorizationEvidencePatch {
    collection: CollectionHandle,
    descriptor: TribleSet,
    proofs: AuthorizationEvidencePatch,
}

impl CollectionAuthorizationEvidencePatch {
    /// Exact collection whose READ and WRITE atoms shaped this evidence set.
    pub const fn collection(&self) -> CollectionHandle {
        self.collection
    }

    /// Inspect one unambiguous scalar policy for diagnostics.
    /// Ordinary admission queries the action-specific alternatives instead.
    pub fn policy(&self) -> Result<CollectionPolicy, RecordDecodeError> {
        descriptor::policy(&self.descriptor)
    }

    /// Explicitly recognized READ policies from the pinned descriptor facts.
    pub fn read_policies(&self) -> impl Iterator<Item = AdmissionPolicy> + '_ {
        descriptor::admission_policies(&self.descriptor, &collection_read_policy, None)
    }

    /// Explicitly recognized WRITE policies from the pinned descriptor facts.
    pub fn write_policies(&self) -> impl Iterator<Item = AdmissionPolicy> + '_ {
        descriptor::admission_policies(&self.descriptor, &collection_write_policy, None)
    }

    pub(crate) fn reader_is_admitted_by_at(
        &self,
        subject: VerifyingKey,
        proofs: &[CapabilityProof],
        instant: hifitime::Epoch,
    ) -> bool {
        self.read_policies().any(|policy| {
            collection_reader_is_admitted_by_policy_at(
                self.collection,
                &policy,
                subject,
                proofs,
                instant,
            )
        })
    }

    /// Root and count of the immutable native-proof PATCH.
    pub fn summary(&self) -> PatchSummary {
        PatchSummary::from_patch(&self.proofs)
    }

    /// Number of distinct self-contained native proofs.
    pub fn len(&self) -> u64 {
        self.proofs.len()
    }

    /// Whether the projection contains no proof evidence.
    pub fn is_empty(&self) -> bool {
        self.proofs.is_empty()
    }

    /// Look up one exact native proof by identity.
    pub fn get(&self, id: CapabilityProofId) -> Option<&CapabilityProof> {
        self.proofs.get(&id.raw)
    }

    /// Enumerate every retained native proof in proof-id order.
    pub fn proofs(&self) -> impl Iterator<Item = &CapabilityProof> {
        self.proofs.iter_ordered().map(|id| {
            self.proofs
                .get(id)
                .expect("an ordered authorization-evidence key retains its proof")
        })
    }

    pub(crate) const fn patch(&self) -> &AuthorizationEvidencePatch {
        &self.proofs
    }

    /// Derive the finite READ(C)-authorized audience at one exact instant.
    ///
    /// Restricted policies return a deterministic, deduplicated list from
    /// independent rooted paths, applying the quorum, mode, and validity rules.
    /// Open READ is explicit because no finite list can enumerate its audience.
    pub fn authorized_readers_at(&self, instant: hifitime::Epoch) -> CollectionReadAudience {
        let proofs = self.proofs().cloned().collect::<Vec<_>>();
        let mut readers = BTreeMap::new();
        for policy in self.read_policies() {
            match collection_read_audience_by_policy_at(self.collection, &policy, &proofs, instant)
            {
                CollectionReadAudience::Open => return CollectionReadAudience::Open,
                CollectionReadAudience::Restricted(subjects) => {
                    readers.extend(
                        subjects
                            .into_iter()
                            .map(|subject| (subject.to_bytes(), subject)),
                    );
                }
            }
        }
        CollectionReadAudience::Restricted(readers.into_values().collect())
    }
}

/// The two immutable components which determine collection repair semantics.
#[derive(Clone, Debug)]
pub struct CollectionRepairOverlay {
    collection: CollectionHandle,
    records: CollectionRecordPatch,
    authorization_evidence: CollectionAuthorizationEvidencePatch,
}

impl CollectionRepairOverlay {
    /// Exact collection represented by both component PATCHes.
    pub const fn collection(&self) -> CollectionHandle {
        self.collection
    }

    /// Inspect one unambiguous scalar policy for diagnostics.
    /// The pinned evidence retains descriptor facts, not a singular policy gate.
    pub fn policy(&self) -> Result<CollectionPolicy, RecordDecodeError> {
        self.authorization_evidence.policy()
    }

    /// Structurally valid collection records naming this collection.
    ///
    /// WRITE admission is deliberately derived by each receiver from this
    /// component and its local authorization evidence, so records and proofs
    /// may arrive in either order.
    pub const fn records(&self) -> &CollectionRecordPatch {
        &self.records
    }

    /// Structurally relevant self-contained READ(C) or WRITE(C) proof projection.
    pub const fn authorization_evidence(&self) -> &CollectionAuthorizationEvidencePatch {
        &self.authorization_evidence
    }

    /// Opaque digest suitable for the collection gossip wake root.
    ///
    /// Counts participate alongside roots so the digest commits to the same
    /// authenticated component summaries used by PATCH repair. Neither a
    /// proof, record, count, nor component root is disclosed by this value.
    pub fn wake_root(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(COLLECTION_REPAIR_ROOT_DOMAIN);
        hasher.update(&COLLECTION_REPAIR_ROOT_VERSION.to_be_bytes());
        hasher.update(&self.collection.raw);
        update_summary(&mut hasher, self.records.summary());
        update_summary(&mut hasher, self.authorization_evidence.summary());
        *hasher.finalize().as_bytes()
    }
}

fn update_summary(hasher: &mut blake3::Hasher, summary: PatchSummary) {
    match summary.root() {
        Some(root) => {
            hasher.update(&[1]);
            hasher.update(&root);
        }
        None => {
            hasher.update(&[0]);
            hasher.update(&[0; 32]);
        }
    }
    hasher.update(&summary.leaf_count().to_be_bytes());
}

/// A proof is not collection-scoped authorization evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionAuthorizationEvidenceError {
    /// Both collection policies are open and need no proof evidence.
    OpenPolicies,
    /// The proof starts outside both descriptor-local root sets.
    WrongRoot,
    /// Signature, path attenuation, or exact atom is invalid or irrelevant.
    Invalid(CapabilityProofError),
    /// Cryptographically distinct proof values share one proof identity.
    ProofIdCollision(CapabilityProofId),
}

impl fmt::Display for CollectionAuthorizationEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenPolicies => {
                formatter.write_str("open READ and WRITE policies need no proof evidence")
            }
            Self::WrongRoot => {
                formatter.write_str("capability proof starts outside the collection policy roots")
            }
            Self::Invalid(source) => {
                write!(
                    formatter,
                    "invalid collection authorization proof: {source}"
                )
            }
            Self::ProofIdCollision(id) => write!(
                formatter,
                "distinct collection authorization proofs share id {}",
                hex::encode_upper(id.raw),
            ),
        }
    }
}

impl Error for CollectionAuthorizationEvidenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Invalid(source) => Some(source),
            _ => None,
        }
    }
}

/// Failure while constructing collection-scoped authorization evidence.
#[derive(Debug)]
pub enum CollectionAuthorizationEvidenceDiscoveryError<ProofsError, GetError> {
    /// The descriptor is absent or structurally invalid.
    Descriptor(CollectionDescriptorError<GetError>),
    /// The coherent proof-store observation failed.
    Proofs(ProofsError),
    /// Canonical evidence construction found a proof-id collision.
    Evidence(CollectionAuthorizationEvidenceError),
}

enum AuthorizationEvidenceBuildError<ProofsError> {
    Proofs(ProofsError),
    Evidence(CollectionAuthorizationEvidenceError),
}

impl<ProofsError, GetError> fmt::Display
    for CollectionAuthorizationEvidenceDiscoveryError<ProofsError, GetError>
where
    ProofsError: fmt::Display,
    GetError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor(source) => source.fmt(formatter),
            Self::Proofs(source) => write!(formatter, "enumerate capability proofs: {source}"),
            Self::Evidence(source) => source.fmt(formatter),
        }
    }
}

impl<ProofsError, GetError> Error
    for CollectionAuthorizationEvidenceDiscoveryError<ProofsError, GetError>
where
    ProofsError: Error + 'static,
    GetError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Descriptor(source) => Some(source),
            Self::Proofs(source) => Some(source),
            Self::Evidence(source) => Some(source),
        }
    }
}

/// Failure while selecting bounded native READ proofs for `C`.
#[derive(Debug)]
pub enum CollectionReadBootstrapError<ProofsError, GetError> {
    /// The descriptor is absent or structurally invalid.
    Descriptor(CollectionDescriptorError<GetError>),
    /// The coherent proof-store observation failed.
    Proofs(ProofsError),
    /// More relevant proofs exist than the caller's transport bound permits.
    TooMany {
        /// Exact number of canonical relevant proofs.
        count: usize,
        /// Caller-supplied maximum.
        limit: usize,
    },
    /// Collection-scoped authorization evidence discovery failed.
    Authorization(CollectionAuthorizationEvidenceError),
}

impl<ProofsError, GetError> fmt::Display for CollectionReadBootstrapError<ProofsError, GetError>
where
    ProofsError: fmt::Display,
    GetError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor(source) => source.fmt(formatter),
            Self::Proofs(source) => write!(formatter, "enumerate capability proofs: {source}"),
            Self::TooMany { count, limit } => write!(
                formatter,
                "collection READ bootstrap has {count} proofs; limit is {limit}",
            ),
            Self::Authorization(source) => source.fmt(formatter),
        }
    }
}

impl<ProofsError, GetError> Error for CollectionReadBootstrapError<ProofsError, GetError>
where
    ProofsError: Error + 'static,
    GetError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Descriptor(source) => Some(source),
            Self::Proofs(source) => Some(source),
            Self::TooMany { .. } => None,
            Self::Authorization(source) => Some(source),
        }
    }
}

/// Failure while freezing the repair overlay of one collection.
#[derive(Debug)]
pub enum CollectionRepairOverlayError<RecordsError, ProofsError, GetError> {
    /// Exact collection-record selection failed.
    Records(CollectionRecordPatchError<RecordsError>),
    /// The descriptor is absent or structurally invalid.
    Descriptor(CollectionDescriptorError<GetError>),
    /// The coherent proof-store observation failed.
    Proofs(ProofsError),
    /// Canonical evidence construction found a proof-id collision.
    Evidence(CollectionAuthorizationEvidenceError),
}

impl<RecordsError, ProofsError, GetError> fmt::Display
    for CollectionRepairOverlayError<RecordsError, ProofsError, GetError>
where
    RecordsError: fmt::Display,
    ProofsError: fmt::Display,
    GetError: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Records(source) => source.fmt(formatter),
            Self::Descriptor(source) => source.fmt(formatter),
            Self::Proofs(source) => write!(formatter, "enumerate capability proofs: {source}"),
            Self::Evidence(source) => source.fmt(formatter),
        }
    }
}

impl<RecordsError, ProofsError, GetError> Error
    for CollectionRepairOverlayError<RecordsError, ProofsError, GetError>
where
    RecordsError: Error + 'static,
    ProofsError: Error + 'static,
    GetError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Records(source) => Some(source),
            Self::Descriptor(source) => Some(source),
            Self::Proofs(source) => Some(source),
            Self::Evidence(source) => Some(source),
        }
    }
}

/// Freeze exact collection records and the structurally relevant
/// READ(C)/WRITE(C) proof PATCH for `C`.
///
/// Missing descriptor bytes or invalid archives fail closed. Unknown policy
/// rows are inert; no recognized READ alternative means no reader is admitted.
/// Invalid or irrelevant ambient proofs are inert. Record inclusion is independent of
/// WRITE admission and time: a receiver derives its admitted view locally
/// after both grow-only components land in either order. Failure to enumerate
/// the coherent proof snapshot is an error.
pub fn collection_repair_overlay<R>(
    snapshot: &R,
    collection: CollectionHandle,
) -> Result<
    CollectionRepairOverlay,
    CollectionRepairOverlayError<R::RecordsError, R::ProofsError, R::GetError<Infallible>>,
>
where
    R: BlobStoreGet + CapabilityProofRead + CollectionRead,
{
    let descriptor = load_collection_descriptor_facts(snapshot, collection)
        .map_err(CollectionRepairOverlayError::Descriptor)?;
    let authorization_evidence =
        collection_authorization_evidence_patch_for_descriptor(snapshot, collection, descriptor)
            .map_err(|error| match error {
                AuthorizationEvidenceBuildError::Proofs(source) => {
                    CollectionRepairOverlayError::Proofs(source)
                }
                AuthorizationEvidenceBuildError::Evidence(source) => {
                    CollectionRepairOverlayError::Evidence(source)
                }
            })?;
    let records = collection_record_patch(snapshot, collection)
        .map_err(CollectionRepairOverlayError::Records)?;
    Ok(CollectionRepairOverlay {
        collection,
        records,
        authorization_evidence,
    })
}

fn load_collection_descriptor_facts<R>(
    snapshot: &R,
    collection: CollectionHandle,
) -> Result<TribleSet, CollectionDescriptorError<R::GetError<Infallible>>>
where
    R: BlobStoreGet,
{
    let descriptor_blob: Blob<SimpleArchive> = snapshot
        .get(collection)
        .map_err(|source| CollectionDescriptorError::Get { collection, source })?;
    TribleSet::try_from_blob(descriptor_blob).map_err(|source| CollectionDescriptorError::Invalid {
        collection,
        source: RecordDecodeError::from(source),
    })
}

/// Freeze all structurally relevant self-contained native READ(C) and WRITE(C)
/// proofs from one coherent store observation.
pub fn collection_authorization_evidence<R>(
    snapshot: &R,
    collection: CollectionHandle,
) -> Result<
    CollectionAuthorizationEvidencePatch,
    CollectionAuthorizationEvidenceDiscoveryError<R::ProofsError, R::GetError<Infallible>>,
>
where
    R: BlobStoreGet + CapabilityProofRead,
{
    let descriptor = load_collection_descriptor_facts(snapshot, collection)
        .map_err(CollectionAuthorizationEvidenceDiscoveryError::Descriptor)?;
    collection_authorization_evidence_patch_for_descriptor(snapshot, collection, descriptor)
        .map_err(|error| match error {
            AuthorizationEvidenceBuildError::Proofs(source) => {
                CollectionAuthorizationEvidenceDiscoveryError::Proofs(source)
            }
            AuthorizationEvidenceBuildError::Evidence(source) => {
                CollectionAuthorizationEvidenceDiscoveryError::Evidence(source)
            }
        })
}

/// Select deterministic bounded native proofs for exact READ(C).
///
/// The descriptor's recognized READ alternatives shape the result. Each returned
/// self-contained proof has a valid signature path and exact READ atom.
/// Selection and deletion minimization use the snapshot's frozen instant; the
/// receiver independently applies its own current instant during admission.
/// Invalid, irrelevant, and duplicate ambient proofs are inert. The caller
/// chooses `max_proofs`; a larger independent-root witness fails rather than
/// silently dropping paths required by quorum.
pub fn collection_read_bootstrap_proofs<R>(
    snapshot: &R,
    collection: CollectionHandle,
    subject: VerifyingKey,
    max_proofs: usize,
) -> Result<
    Vec<CapabilityProof>,
    CollectionReadBootstrapError<R::ProofsError, R::GetError<Infallible>>,
>
where
    R: BlobStoreGet + CapabilityProofRead + StoreSnapshot,
{
    let instant = snapshot.instant();
    let evidence =
        collection_authorization_evidence(snapshot, collection).map_err(|error| match error {
            CollectionAuthorizationEvidenceDiscoveryError::Descriptor(source) => {
                CollectionReadBootstrapError::Descriptor(source)
            }
            CollectionAuthorizationEvidenceDiscoveryError::Proofs(source) => {
                CollectionReadBootstrapError::Proofs(source)
            }
            CollectionAuthorizationEvidenceDiscoveryError::Evidence(source) => {
                CollectionReadBootstrapError::Authorization(source)
            }
        })?;
    if evidence
        .read_policies()
        .any(|policy| matches!(policy, AdmissionPolicy::Open))
    {
        return Ok(Vec::new());
    }

    let atom = collection_atom(ACTION_READ, collection);
    let mut selected = evidence
        .proofs()
        .filter(|proof| {
            evidence
                .read_policies()
                .any(|policy| root_is_relevant(&policy, proof.root_key()))
                && proof.validate_structure_for_atom(atom).is_ok()
        })
        .cloned()
        .collect::<Vec<_>>();
    if !evidence.reader_is_admitted_by_at(subject, &selected, instant) {
        return Ok(Vec::new());
    }
    // Delete every proof not required by the independently rooted quorum
    // witness while withholding unrelated ambient grants from the endpoint.
    let mut index = selected.len();
    while index > 0 {
        index -= 1;
        let removed = selected.remove(index);
        if !evidence.reader_is_admitted_by_at(subject, &selected, instant) {
            selected.insert(index, removed);
        }
    }
    if selected.len() > max_proofs {
        return Err(CollectionReadBootstrapError::TooMany {
            count: selected.len(),
            limit: max_proofs,
        });
    }
    Ok(selected)
}

fn collection_authorization_evidence_patch_for_descriptor<R>(
    snapshot: &R,
    collection: CollectionHandle,
    descriptor: TribleSet,
) -> Result<CollectionAuthorizationEvidencePatch, AuthorizationEvidenceBuildError<R::ProofsError>>
where
    R: CapabilityProofRead,
{
    let has_roots = descriptor::admission_policies(&descriptor, &collection_read_policy, None)
        .chain(descriptor::admission_policies(
            &descriptor,
            &collection_write_policy,
            None,
        ))
        .any(|policy| policy.roots().is_some());
    if !has_roots {
        return Ok(CollectionAuthorizationEvidencePatch {
            collection,
            descriptor,
            proofs: PATCH::new(),
        });
    }

    let proofs = snapshot
        .proofs()
        .map_err(AuthorizationEvidenceBuildError::Proofs)?;
    let mut candidates = Vec::new();
    for proof in proofs {
        let proof = proof.map_err(AuthorizationEvidenceBuildError::Proofs)?;
        if !descriptor::admission_policies(&descriptor, &collection_read_policy, None)
            .chain(descriptor::admission_policies(
                &descriptor,
                &collection_write_policy,
                None,
            ))
            .any(|policy| root_is_relevant(&policy, proof.root_key()))
        {
            continue;
        }
        candidates.push(proof);
    }
    canonical_authorization_evidence(collection, descriptor, candidates)
        .map_err(AuthorizationEvidenceBuildError::Evidence)
}

fn canonical_authorization_evidence(
    collection: CollectionHandle,
    descriptor: TribleSet,
    candidates: impl IntoIterator<Item = CapabilityProof>,
) -> Result<CollectionAuthorizationEvidencePatch, CollectionAuthorizationEvidenceError> {
    let mut proofs = AuthorizationEvidencePatch::new();
    for proof in candidates {
        let policies = descriptor::admission_policies(&descriptor, &collection_read_policy, None)
            .map(|policy| (policy, ACTION_READ))
            .chain(
                descriptor::admission_policies(&descriptor, &collection_write_policy, None)
                    .map(|policy| (policy, ACTION_WRITE)),
            );
        if validate_evidence_for_actions(collection, policies, &proof).is_err() {
            continue;
        }
        let id = proof.id();
        if let Some(existing) = proofs.get(&id.raw) {
            if existing != &proof {
                return Err(CollectionAuthorizationEvidenceError::ProofIdCollision(id));
            }
            continue;
        }
        proofs.insert(&PatchEntry::with_value(&id.raw, proof));
    }
    Ok(CollectionAuthorizationEvidencePatch {
        collection,
        descriptor,
        proofs,
    })
}

fn root_is_relevant(policy: &AdmissionPolicy, root: ed25519_dalek::VerifyingKey) -> bool {
    policy.roots().is_some_and(|roots| {
        roots
            .binary_search_by_key(&root.to_bytes(), ed25519_dalek::VerifyingKey::to_bytes)
            .is_ok()
    })
}

fn collection_atom(
    action: triblespace_core::id::Id,
    collection: CollectionHandle,
) -> CapabilityAtom {
    CapabilityAtom::new(
        CapabilityAction::new(action),
        CapabilityResource::from(collection),
    )
}

#[cfg(test)]
fn write_atom(collection: CollectionHandle) -> CapabilityAtom {
    collection_atom(ACTION_WRITE, collection)
}

/// Strictly validate one self-contained proof as exact READ(C) or WRITE(C) evidence.
pub fn validate_authorization_evidence_proof(
    collection: CollectionHandle,
    policy: &CollectionPolicy,
    proof: &CapabilityProof,
) -> Result<(), CollectionAuthorizationEvidenceError> {
    if matches!(policy.read(), AdmissionPolicy::Open)
        && matches!(policy.write(), AdmissionPolicy::Open)
    {
        return Err(CollectionAuthorizationEvidenceError::OpenPolicies);
    }
    validate_evidence_for_actions(
        collection,
        [
            (policy.read().clone(), ACTION_READ),
            (policy.write().clone(), ACTION_WRITE),
        ],
        proof,
    )
}

fn validate_evidence_for_actions(
    collection: CollectionHandle,
    policies: impl IntoIterator<Item = (AdmissionPolicy, triblespace_core::id::Id)>,
    proof: &CapabilityProof,
) -> Result<(), CollectionAuthorizationEvidenceError> {
    let root = proof.root_key();
    let mut invalid = None;
    for (candidate_policy, action) in policies {
        if !root_is_relevant(&candidate_policy, root) {
            continue;
        }
        match proof.validate_structure_for_atom(collection_atom(action, collection)) {
            Ok(()) => return Ok(()),
            Err(error) => invalid = Some(error),
        }
    }
    match invalid {
        Some(error) => Err(CollectionAuthorizationEvidenceError::Invalid(error)),
        None => Err(CollectionAuthorizationEvidenceError::WrongRoot),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use ed25519_dalek::SigningKey;
    use hifitime::Epoch;
    use triblespace_core::capability::{
        Capability, CapabilityMode, CapabilityRequest, CapabilityValidity,
        capability_quorum_authorizes,
    };
    use triblespace_core::collection::{
        CollectionCommit, CollectionData, CollectionDerive, CollectionMerge, CollectionPolicy,
        CollectionRecord, CollectionStore, CollectionStoreExt, KIND_COLLECTION_DESCRIPTOR,
        empty_metadata_handle,
    };
    use triblespace_core::inline::Inline;
    use triblespace_core::metadata;
    use triblespace_core::prelude::entity;
    use triblespace_core::repo::memoryrepo::MemoryRepo;
    use triblespace_core::repo::{BlobStorePut, CapabilityProofStore, SnapshotSource};

    use super::*;

    fn key(byte: u8) -> SigningKey {
        SigningKey::from_bytes(&[byte; 32])
    }

    fn data(byte: u8) -> CollectionData {
        Inline::new([byte; 32])
    }

    fn policy(roots: &[SigningKey], threshold: u32) -> CollectionPolicy {
        CollectionPolicy::new(
            AdmissionPolicy::Open,
            AdmissionPolicy::quorum(roots.iter().map(SigningKey::verifying_key), threshold, None)
                .unwrap(),
        )
    }

    fn policy_facts(policy: CollectionPolicy) -> TribleSet {
        entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_read_policy*: policy.read().fragment(),
            collection_write_policy*: policy.write().fragment(),
        }
        .facts()
        .clone()
    }

    fn root_proof(
        root: &SigningKey,
        subject: &SigningKey,
        atom: CapabilityAtom,
        mode: CapabilityMode,
        validity: Option<CapabilityValidity>,
    ) -> CapabilityProof {
        CapabilityProof::issue_root(
            root,
            atom.resource(),
            Capability::new(atom.action(), mode),
            validity,
            subject.verifying_key(),
        )
    }

    fn store_proof(store: &mut MemoryRepo, proof: CapabilityProof) {
        store.insert_proof(proof).unwrap();
    }

    #[test]
    fn read_policy_alternatives_shape_evidence_and_bootstrap_independently_of_write() {
        let root_a = key(40);
        let root_b = key(41);
        let reader_a = key(42);
        let reader_b = key(43);
        let descriptor = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_read_policy*: AdmissionPolicy::direct(root_a.verifying_key()).fragment()
                + AdmissionPolicy::direct(root_b.verifying_key()).fragment(),
            collection_write_policy*: entity! { metadata::tag: metadata::KIND_BLOB_ENCODING },
        };
        let mut store = MemoryRepo::default();
        let collection = store
            .put::<SimpleArchive, _>(descriptor.facts().clone())
            .unwrap();
        let proof_a = root_proof(
            &root_a,
            &reader_a,
            collection_atom(ACTION_READ, collection),
            CapabilityMode::Invoke,
            None,
        );
        let proof_b = root_proof(
            &root_b,
            &reader_b,
            collection_atom(ACTION_READ, collection),
            CapabilityMode::Invoke,
            None,
        );
        let wrong_action = root_proof(
            &root_a,
            &reader_a,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );
        for proof in [proof_a.clone(), proof_b.clone(), wrong_action] {
            store_proof(&mut store, proof);
        }
        let snapshot = store.snapshot().unwrap();
        let overlay = collection_repair_overlay(&snapshot, collection).unwrap();
        assert!(
            overlay.policy().is_err(),
            "scalar diagnostics remain explicit"
        );
        let evidence = overlay.authorization_evidence();
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence.write_policies().count(), 0);
        let proofs = evidence.proofs().cloned().collect::<Vec<_>>();
        for reader in [reader_a.verifying_key(), reader_b.verifying_key()] {
            assert!(evidence.reader_is_admitted_by_at(reader, &proofs, snapshot.instant()));
            assert!(
                triblespace_core::collection::collection_reader_is_admitted_by(
                    &snapshot, collection, reader, &proofs,
                )
                .unwrap()
            );
        }
        assert!(!evidence.reader_is_admitted_by_at(
            key(44).verifying_key(),
            &proofs,
            snapshot.instant()
        ));
        let CollectionReadAudience::Restricted(audience) =
            evidence.authorized_readers_at(snapshot.instant())
        else {
            panic!("restricted READ alternatives must not become Open");
        };
        assert!(audience.contains(&reader_a.verifying_key()));
        assert!(audience.contains(&reader_b.verifying_key()));
        assert_eq!(
            collection_read_bootstrap_proofs(&snapshot, collection, reader_b.verifying_key(), 1)
                .unwrap(),
            [proof_b],
        );
    }

    #[test]
    fn absent_or_unknown_read_policy_never_borrows_open_write_or_unrelated_read() {
        let absent = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_write_policy*: AdmissionPolicy::Open.fragment(),
        };
        let unknown = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_read_policy*: entity! { metadata::tag: metadata::KIND_BLOB_ENCODING },
            collection_write_policy*: AdmissionPolicy::Open.fragment(),
        };
        for mut descriptor in [absent, unknown] {
            descriptor += entity! {
                collection_read_policy*: AdmissionPolicy::Open.fragment(),
            };
            let mut store = MemoryRepo::default();
            let collection = store
                .put::<SimpleArchive, _>(descriptor.facts().clone())
                .unwrap();
            let snapshot = store.snapshot().unwrap();
            let overlay = collection_repair_overlay(&snapshot, collection).unwrap();
            let evidence = overlay.authorization_evidence();
            assert_eq!(evidence.read_policies().count(), 0);
            assert!(
                evidence
                    .write_policies()
                    .any(|policy| matches!(policy, AdmissionPolicy::Open))
            );
            assert!(!evidence.reader_is_admitted_by_at(
                key(44).verifying_key(),
                &[],
                snapshot.instant()
            ));
            assert_eq!(
                evidence.authorized_readers_at(snapshot.instant()),
                CollectionReadAudience::Restricted(Vec::new())
            );
            assert!(
                !triblespace_core::collection::collection_reader_is_admitted_by(
                    &snapshot,
                    collection,
                    key(44).verifying_key(),
                    &[],
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn explicit_open_read_does_not_require_a_write_policy() {
        let descriptor = entity! {
            metadata::tag: KIND_COLLECTION_DESCRIPTOR,
            collection_read_policy*: AdmissionPolicy::Open.fragment(),
        };
        let mut store = MemoryRepo::default();
        let collection = store
            .put::<SimpleArchive, _>(descriptor.facts().clone())
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let overlay = collection_repair_overlay(&snapshot, collection).unwrap();
        assert!(overlay.policy().is_err());
        let evidence = overlay.authorization_evidence();
        assert!(evidence.is_empty());
        assert_eq!(evidence.write_policies().count(), 0);
        assert!(evidence.reader_is_admitted_by_at(
            key(44).verifying_key(),
            &[],
            snapshot.instant()
        ));
        assert_eq!(
            evidence.authorized_readers_at(snapshot.instant()),
            CollectionReadAudience::Open
        );
        assert!(
            collection_read_bootstrap_proofs(&snapshot, collection, key(44).verifying_key(), 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn commit_and_later_write_proof_are_independent_repair_components() {
        let root = key(1);
        let writer = key(2);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection("activation", policy(&[root.clone()], 1))
            .unwrap();
        store
            .insert(CollectionRecord::Commit(CollectionCommit::sign(
                &writer,
                collection.handle(),
                data(3),
                empty_metadata_handle(),
            )))
            .unwrap();

        let instant = Epoch::from_tai_seconds(0.0);
        let before_snapshot = store.snapshot_at(instant).unwrap();
        let before = collection_repair_overlay(&before_snapshot, collection.handle()).unwrap();
        assert!(
            !collection
                .writer_is_admitted(&before_snapshot, writer.verifying_key())
                .unwrap()
        );
        let atom = write_atom(collection.handle());
        store_proof(
            &mut store,
            root_proof(&root, &writer, atom, CapabilityMode::Invoke, None),
        );
        let after_snapshot = store.snapshot_at(instant).unwrap();
        let after = collection_repair_overlay(&after_snapshot, collection.handle()).unwrap();
        assert!(
            collection
                .writer_is_admitted(&after_snapshot, writer.verifying_key())
                .unwrap()
        );

        assert_eq!(before.records().summary().leaf_count(), 1);
        assert_eq!(after.records().summary().leaf_count(), 1);
        assert_eq!(before.records().summary(), after.records().summary());
        assert_ne!(
            before.authorization_evidence().summary(),
            after.authorization_evidence().summary()
        );
        assert_ne!(before.wake_root(), after.wake_root());
    }

    #[test]
    fn record_and_authorization_evidence_converge_in_either_arrival_order() {
        let root = key(32);
        let writer = key(33);
        let policy = policy(&[root.clone()], 1);
        let mut record_first = MemoryRepo::default();
        let first_collection = record_first
            .collection("repair-order", policy.clone())
            .unwrap();
        let mut proof_first = MemoryRepo::default();
        let second_collection = proof_first.collection("repair-order", policy).unwrap();
        assert_eq!(first_collection.handle(), second_collection.handle());

        let commit = CollectionRecord::Commit(CollectionCommit::sign(
            &writer,
            first_collection.handle(),
            data(34),
            empty_metadata_handle(),
        ));
        let grant = root_proof(
            &root,
            &writer,
            write_atom(first_collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        record_first.insert(commit).unwrap();
        store_proof(&mut record_first, grant.clone());
        store_proof(&mut proof_first, grant);
        proof_first.insert(commit).unwrap();

        let first =
            collection_repair_overlay(&record_first.snapshot().unwrap(), first_collection.handle())
                .unwrap();
        let second =
            collection_repair_overlay(&proof_first.snapshot().unwrap(), second_collection.handle())
                .unwrap();
        assert_eq!(first.records().summary(), second.records().summary());
        assert_eq!(
            first.authorization_evidence().summary(),
            second.authorization_evidence().summary()
        );
        assert_eq!(first.wake_root(), second.wake_root());
    }

    #[test]
    fn read_proof_changes_wake_root_without_changing_records() {
        let root = key(29);
        let reader = key(30);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "read-repair",
                CollectionPolicy::new(
                    AdmissionPolicy::direct(root.verifying_key()),
                    AdmissionPolicy::Open,
                ),
            )
            .unwrap();
        let before_snapshot = store.snapshot().unwrap();
        let before = collection_repair_overlay(&before_snapshot, collection.handle()).unwrap();
        store_proof(
            &mut store,
            root_proof(
                &root,
                &reader,
                collection_atom(ACTION_READ, collection.handle()),
                CapabilityMode::Invoke,
                None,
            ),
        );
        let after_snapshot = store.snapshot().unwrap();
        let after = collection_repair_overlay(&after_snapshot, collection.handle()).unwrap();

        assert_eq!(before.records().summary(), after.records().summary());
        assert_eq!(before.authorization_evidence().len(), 0);
        assert_eq!(after.authorization_evidence().len(), 1);
        assert_ne!(before.wake_root(), after.wake_root());
    }

    #[test]
    fn merge_and_derive_equations_participate_in_collection_repair() {
        let writer = key(3);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "commit-only-activation",
                CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open),
            )
            .unwrap();
        let commit = CollectionCommit::sign(
            &writer,
            collection.handle(),
            data(31),
            empty_metadata_handle(),
        );
        store.insert(CollectionRecord::Commit(commit)).unwrap();

        let before_snapshot = store.snapshot().unwrap();
        let before = collection_repair_overlay(&before_snapshot, collection.handle()).unwrap();

        store
            .insert(CollectionRecord::Merge(CollectionMerge::new(
                collection.handle(),
                data(31),
                data(32),
                data(33),
            )))
            .unwrap();
        store
            .insert(CollectionRecord::Derive(CollectionDerive::new(
                collection.handle(),
                data(33),
                data(34),
            )))
            .unwrap();

        let after_snapshot = store.snapshot().unwrap();
        let after = collection_repair_overlay(&after_snapshot, collection.handle()).unwrap();

        assert_ne!(before.records().summary(), after.records().summary());
        assert_ne!(before.wake_root(), after.wake_root());
        assert_eq!(after.records().len(), 3);
        assert_eq!(
            after
                .records()
                .get(CollectionRecord::Commit(commit).fingerprint()),
            Some(CollectionRecord::Commit(commit))
        );
        assert!(after.records().records().any(|record| matches!(
            record,
            CollectionRecord::Merge(merge)
                if merge.collection() == collection.handle()
        )));
        assert!(after.records().records().any(|record| matches!(
            record,
            CollectionRecord::Derive(derive)
                if derive.collection() == collection.handle()
        )));
    }

    #[test]
    fn evidence_shape_is_independent_of_the_clock() {
        let root = key(4);
        let other_root = key(31);
        let writer = key(5);
        let collection = Inline::new([6; 32]);
        let atom = write_atom(collection);
        let validity =
            CapabilityValidity::new(Epoch::from_tai_seconds(10.0), Epoch::from_tai_seconds(20.0))
                .unwrap();
        let proof = root_proof(&root, &writer, atom, CapabilityMode::Invoke, Some(validity));
        let write_policy =
            AdmissionPolicy::quorum([root.verifying_key(), other_root.verifying_key()], 2, None)
                .unwrap();
        let evidence = canonical_authorization_evidence(
            collection,
            policy_facts(CollectionPolicy::new(AdmissionPolicy::Open, write_policy)),
            [proof.clone()],
        )
        .unwrap();

        assert_eq!(evidence.len(), 1);
        assert!(proof.validate_structure_for_atom(atom).is_ok());
        let request = CapabilityRequest::new(atom, CapabilityMode::Invoke);
        assert!(
            proof
                .verify(
                    root.verifying_key(),
                    Epoch::from_tai_seconds(0.0),
                    writer.verifying_key(),
                    request,
                )
                .is_err()
        );
        assert!(
            proof
                .verify(
                    root.verifying_key(),
                    Epoch::from_tai_seconds(30.0),
                    writer.verifying_key(),
                    request,
                )
                .is_err()
        );
    }

    #[test]
    fn self_contained_proof_needs_no_blob_residency() {
        let root = key(32);
        let reader = key(33);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "self-contained-proof",
                CollectionPolicy::new(
                    AdmissionPolicy::direct(root.verifying_key()),
                    AdmissionPolicy::Open,
                ),
            )
            .unwrap();
        let proof = root_proof(
            &root,
            &reader,
            collection_atom(ACTION_READ, collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        store.insert_proof(proof.clone()).unwrap();
        let snapshot = store.snapshot().unwrap();
        let evidence = collection_authorization_evidence(&snapshot, collection.handle()).unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence.get(proof.id()), Some(&proof));
    }

    #[test]
    fn read_audience_includes_an_intermediate_without_a_stored_prefix() {
        let root = key(34);
        let intermediate = key(35);
        let leaf = key(36);
        let delegate_only = key(37);
        let future = key(38);
        let collection = Inline::new([39; 32]);
        let atom = collection_atom(ACTION_READ, collection);
        let parent = root_proof(
            &root,
            &intermediate,
            atom,
            CapabilityMode::InvokeAndDelegate,
            None,
        );
        let verified = parent
            .verify(
                root.verifying_key(),
                Epoch::from_tai_seconds(0.0),
                intermediate.verifying_key(),
                CapabilityRequest::new(atom, CapabilityMode::InvokeAndDelegate),
            )
            .unwrap();
        let child = verified
            .delegate(
                &intermediate,
                Capability::new(atom.action(), CapabilityMode::Invoke),
                None,
                leaf.verifying_key(),
            )
            .unwrap();
        let delegate_only = root_proof(&root, &delegate_only, atom, CapabilityMode::Delegate, None);
        let future = root_proof(
            &root,
            &future,
            atom,
            CapabilityMode::Invoke,
            Some(
                CapabilityValidity::new(
                    Epoch::from_tai_seconds(10.0),
                    Epoch::from_tai_seconds(20.0),
                )
                .unwrap(),
            ),
        );
        let evidence = canonical_authorization_evidence(
            collection,
            policy_facts(CollectionPolicy::new(
                AdmissionPolicy::direct(root.verifying_key()),
                AdmissionPolicy::Open,
            )),
            [child, delegate_only, future],
        )
        .unwrap();
        assert_eq!(evidence.len(), 3);

        let CollectionReadAudience::Restricted(readers) =
            evidence.authorized_readers_at(Epoch::from_tai_seconds(0.0))
        else {
            panic!("restricted READ policy returned an open audience");
        };
        assert!(readers.contains(&root.verifying_key()));
        assert!(readers.contains(&intermediate.verifying_key()));
        assert!(readers.contains(&leaf.verifying_key()));
        assert!(!readers.contains(&key(37).verifying_key()));
        assert!(!readers.contains(&key(38).verifying_key()));
    }

    #[test]
    fn authorization_validation_rejects_wrong_scope_root_action_and_signature() {
        let root = key(7);
        let other_root = key(8);
        let writer = key(9);
        let collection = Inline::new([10; 32]);
        let policy = CollectionPolicy::new(
            AdmissionPolicy::direct(root.verifying_key()),
            AdmissionPolicy::direct(root.verifying_key()),
        );
        let proof = root_proof(
            &root,
            &writer,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );
        validate_authorization_evidence_proof(collection, &policy, &proof).unwrap();

        assert!(matches!(
            validate_authorization_evidence_proof(Inline::new([11; 32]), &policy, &proof),
            Err(CollectionAuthorizationEvidenceError::Invalid(
                CapabilityProofError::WrongAtom { .. }
            ))
        ));
        let wrong_action = root_proof(
            &root,
            &writer,
            CapabilityAtom::new(
                CapabilityAction::new(triblespace_core::id::Id::new([12; 16]).unwrap()),
                CapabilityResource::from(collection),
            ),
            CapabilityMode::Invoke,
            None,
        );
        assert!(matches!(
            validate_authorization_evidence_proof(collection, &policy, &wrong_action),
            Err(CollectionAuthorizationEvidenceError::Invalid(
                CapabilityProofError::WrongAtom { .. }
            ))
        ));
        let wrong_root = root_proof(
            &other_root,
            &writer,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );
        assert!(matches!(
            validate_authorization_evidence_proof(collection, &policy, &wrong_root),
            Err(CollectionAuthorizationEvidenceError::WrongRoot)
        ));

        let mut bytes = proof.as_bytes().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let bad_signature = CapabilityProof::from_bytes(&bytes).unwrap();
        assert!(matches!(
            validate_authorization_evidence_proof(collection, &policy, &bad_signature),
            Err(CollectionAuthorizationEvidenceError::Invalid(
                CapabilityProofError::InvalidSignature { .. }
            ))
        ));
    }

    #[test]
    fn canonical_patch_ignores_arrival_order_duplicates_and_irrelevant_proofs() {
        let root = key(13);
        let other_root = key(14);
        let a = key(15);
        let b = key(16);
        let collection = Inline::new([17; 32]);
        let write_policy = AdmissionPolicy::direct(root.verifying_key());
        let first = root_proof(
            &root,
            &a,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );
        let second = root_proof(
            &root,
            &b,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );
        let read = root_proof(
            &root,
            &b,
            collection_atom(ACTION_READ, collection),
            CapabilityMode::Invoke,
            None,
        );
        let irrelevant = root_proof(
            &other_root,
            &b,
            write_atom(collection),
            CapabilityMode::Invoke,
            None,
        );

        let policy =
            CollectionPolicy::new(AdmissionPolicy::direct(root.verifying_key()), write_policy);
        let left = canonical_authorization_evidence(
            collection,
            policy_facts(policy.clone()),
            [
                first.clone(),
                read.clone(),
                second.clone(),
                first.clone(),
                irrelevant,
            ],
        )
        .unwrap();
        let right = canonical_authorization_evidence(
            collection,
            policy_facts(policy),
            [second, first, read],
        )
        .unwrap();
        assert_eq!(left.len(), 3);
        assert_eq!(left.summary(), right.summary());
    }

    #[test]
    fn every_independent_root_path_needed_by_quorum_is_preserved() {
        let root_a = key(18);
        let root_b = key(19);
        let bridge = key(20);
        let writer = key(21);
        let collection = Inline::new([22; 32]);
        let atom = write_atom(collection);
        let write_policy =
            AdmissionPolicy::quorum([root_a.verifying_key(), root_b.verifying_key()], 2, None)
                .unwrap();

        let delegated = |root: &SigningKey| {
            let parent = root_proof(root, &bridge, atom, CapabilityMode::InvokeAndDelegate, None);
            let verified = parent
                .verify(
                    root.verifying_key(),
                    Epoch::from_tai_seconds(0.0),
                    bridge.verifying_key(),
                    CapabilityRequest::new(atom, CapabilityMode::InvokeAndDelegate),
                )
                .unwrap();
            verified
                .delegate(
                    &bridge,
                    Capability::new(atom.action(), CapabilityMode::Invoke),
                    None,
                    writer.verifying_key(),
                )
                .unwrap()
        };
        let evidence = canonical_authorization_evidence(
            collection,
            policy_facts(CollectionPolicy::new(AdmissionPolicy::Open, write_policy)),
            [delegated(&root_a), delegated(&root_b)],
        )
        .unwrap();

        assert_eq!(evidence.len(), 2);
        assert!(capability_quorum_authorizes(
            evidence.proofs(),
            [root_a.verifying_key(), root_b.verifying_key()],
            Epoch::from_tai_seconds(0.0),
            writer.verifying_key(),
            CapabilityRequest::new(atom, CapabilityMode::Invoke),
            NonZeroUsize::new(2).unwrap(),
        ));
    }

    #[test]
    fn missing_descriptor_fails_closed_before_overlay_exists() {
        let mut store = MemoryRepo::default();
        let snapshot = store.snapshot().unwrap();
        let result = collection_repair_overlay(&snapshot, Inline::new([23; 32]));
        assert!(matches!(
            result,
            Err(CollectionRepairOverlayError::Descriptor(_))
        ));
    }

    #[test]
    fn read_bootstrap_is_exact_deterministic_and_transport_bounded() {
        let root = key(24);
        let other_root = key(25);
        let reader = key(26);
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "read-evidence",
                CollectionPolicy::new(
                    AdmissionPolicy::direct(root.verifying_key()),
                    AdmissionPolicy::Open,
                ),
            )
            .unwrap();
        let relevant = root_proof(
            &root,
            &reader,
            collection_atom(ACTION_READ, collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        let wrong_action = root_proof(
            &root,
            &reader,
            write_atom(collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        let wrong_root = root_proof(
            &other_root,
            &reader,
            collection_atom(ACTION_READ, collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        let unrelated_reader = root_proof(
            &root,
            &key(28),
            collection_atom(ACTION_READ, collection.handle()),
            CapabilityMode::Invoke,
            None,
        );
        store_proof(&mut store, wrong_root);
        store_proof(&mut store, unrelated_reader);
        store_proof(&mut store, relevant.clone());
        store_proof(&mut store, wrong_action);

        let snapshot = store.snapshot().unwrap();
        let selected = collection_read_bootstrap_proofs(
            &snapshot,
            collection.handle(),
            reader.verifying_key(),
            1,
        )
        .unwrap();
        assert_eq!(selected, [relevant.clone()]);
        let overlay = collection_repair_overlay(&snapshot, collection.handle()).unwrap();
        assert!(overlay.authorization_evidence().reader_is_admitted_by_at(
            reader.verifying_key(),
            &[relevant],
            Epoch::from_tai_seconds(0.0),
        ));
        assert!(matches!(
            collection_read_bootstrap_proofs(
                &snapshot,
                collection.handle(),
                reader.verifying_key(),
                0
            ),
            Err(CollectionReadBootstrapError::TooMany { count: 1, limit: 0 })
        ));
    }

    #[test]
    fn open_read_policy_needs_no_bootstrap_evidence() {
        let mut store = MemoryRepo::default();
        let collection = store
            .collection(
                "open-read",
                CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open),
            )
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        let selected = collection_read_bootstrap_proofs(
            &snapshot,
            collection.handle(),
            key(27).verifying_key(),
            0,
        )
        .unwrap();
        assert!(selected.is_empty());
        assert!(
            collection_repair_overlay(&snapshot, collection.handle())
                .unwrap()
                .authorization_evidence()
                .reader_is_admitted_by_at(
                    key(27).verifying_key(),
                    &[],
                    Epoch::from_tai_seconds(0.0),
                )
        );
    }
}
