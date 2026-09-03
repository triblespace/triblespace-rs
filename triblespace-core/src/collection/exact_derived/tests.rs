use super::*;

use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anybytes::Bytes;
use ed25519_dalek::SigningKey;
use futures::executor::block_on;

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::encodings::UnknownBlob;
use crate::blob::{BlobEncoding, IntoBlob, TryFromBlob};
use crate::capability::{
    CapabilityAction, CapabilityAtom, CapabilityClaim, CapabilityMode, CapabilityProof,
    CapabilityProofBundle, CapabilityProofId, CapabilityResource,
};
use crate::collection::{
    AdmissionPolicy, CollectionCommit, CollectionMerge, CollectionPolicy, CollectionRead,
    CollectionRecordFingerprint, CollectionRecordSelector, CollectionSnapshotExt, CollectionStore,
    CollectionStoreExt, ACTION_WRITE,
};
use crate::id::{ExclusiveId, Id};
use crate::id_hex;
use crate::inline::{Inline, InlineEncoding};
use crate::metadata::MetaDescribe;
use crate::repo::memoryrepo::{MemoryRepo, MemoryRepoSnapshot};
use crate::repo::{
    async_store::AsyncBlobStoreAcquire, BlobInfo, BlobMetadata, BlobStoreGet, BlobStoreList,
    BlobStoreMeta, BlobStorePut, CapabilityProofRead, CapabilityProofStore, SnapshotSource,
    StoreChanges, StoreSnapshot, WantRead,
};
use crate::trible::{Fragment, Trible, TribleSet, TRIBLE_LEN};

fn policy() -> CollectionPolicy {
    CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open)
}

fn row(entity: u8, value: u8) -> Trible {
    let mut raw = [value; TRIBLE_LEN];
    raw[..16].fill(entity);
    raw[16..32].fill(9);
    Trible::force_raw(raw).unwrap()
}

fn archive(entity: u8, value: u8) -> Blob<SimpleArchive> {
    let mut facts = TribleSet::new();
    facts.insert(&row(entity, value));
    facts.to_blob()
}

fn data<E: BlobEncoding>(blob: &Blob<E>) -> CollectionData
where
    Handle<E>: crate::inline::InlineEncoding,
{
    Handle::<E>::to_hash(blob.get_handle())
}

fn support(collection: Collection<SimpleArchive>, blobs: &[Blob<SimpleArchive>]) -> Support {
    Cover::from_members(collection, blobs.iter().map(Blob::get_handle))
}

fn publish_root(
    store: &mut MemoryRepo,
    collection: Collection<SimpleArchive>,
    blob: &Blob<SimpleArchive>,
    key: u8,
) {
    store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    let metadata = store
        .put::<SimpleArchive, _>(TribleSet::new().to_blob())
        .unwrap();
    store
        .insert(CollectionRecord::Commit(CollectionCommit::sign(
            &SigningKey::from_bytes(&[key; 32]),
            collection.handle(),
            data(blob),
            metadata,
        )))
        .unwrap();
}

/// Test encoding `SimpleArchive || 0xA5`; id originally minted for the old
/// exact-derived tests with `trible genid` on 2026-08-29.
const FIRST_ENCODING: Id = id_hex!("39B18B6D13B2B1872F2394EF6588F1B5");
struct FirstEncoding;

impl BlobEncoding for FirstEncoding {}

impl MetaDescribe for FirstEncoding {
    fn describe() -> Fragment {
        let id = FIRST_ENCODING;
        crate::macros::entity! { ExclusiveId::force_ref(&id) @
            crate::metadata::name: "invariant-support-test-first",
            crate::metadata::tag: crate::metadata::KIND_BLOB_ENCODING,
        }
    }
}

fn decode_first(
    blob: &Blob<FirstEncoding>,
) -> Result<Blob<SimpleArchive>, CollectionOperationError> {
    let bytes = blob.bytes.as_ref().strip_suffix(&[0xA5]).ok_or_else(|| {
        CollectionOperationError::Fatal("first test encoding lacks suffix".to_owned())
    })?;
    let archive = Blob::new(bytes.to_vec().into());
    crate::collection::simplearchive_union::validate_element(&archive)
        .map_err(|error| CollectionOperationError::Fatal(error.to_string()))?;
    Ok(archive)
}

fn join_first(
    low: &Blob<FirstEncoding>,
    high: &Blob<FirstEncoding>,
) -> Result<Blob<FirstEncoding>, CollectionOperationError> {
    let low = decode_first(low)?;
    let high = decode_first(high)?;
    let joined = crate::collection::simplearchive_union::join(&low, &high)
        .map_err(|error| CollectionOperationError::Fatal(error.to_string()))?;
    let mut bytes = joined.bytes.as_ref().to_vec();
    bytes.push(0xA5);
    Ok(Blob::new(bytes.into()))
}

impl CollectionEncoding for FirstEncoding {
    fn validate_member<R>(
        _descriptor: &Fragment,
        member: &Blob<Self>,
        _reader: &R,
    ) -> Result<(), CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        decode_first(member).map(drop)
    }

    fn join_members<R>(
        _descriptor: &Fragment,
        _low: &Blob<Self>,
        _high: &Blob<Self>,
        _reader: &R,
    ) -> Result<Blob<Self>, CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        // A missing optional target-join dependency must leave the exact fine
        // cover intact, not cause construction back in SimpleArchive.
        Err(CollectionOperationError::MissingDependency(
            crate::inline::Inline::new([0xDD; 32]),
        ))
    }
}

/// Test encoding `(SimpleArchive || 0xA5) || 0xB6`; id originally minted for
/// the old exact-derived tests with `trible genid` on 2026-08-29.
const SECOND_ENCODING: Id = id_hex!("9318ADD9A6257CB8973AC8BE806D12EC");
struct SecondEncoding;

impl BlobEncoding for SecondEncoding {}

impl MetaDescribe for SecondEncoding {
    fn describe() -> Fragment {
        let id = SECOND_ENCODING;
        crate::macros::entity! { ExclusiveId::force_ref(&id) @
            crate::metadata::name: "invariant-support-test-second",
            crate::metadata::tag: crate::metadata::KIND_BLOB_ENCODING,
        }
    }
}

fn decode_second(
    blob: &Blob<SecondEncoding>,
) -> Result<Blob<FirstEncoding>, CollectionOperationError> {
    let bytes = blob.bytes.as_ref().strip_suffix(&[0xB6]).ok_or_else(|| {
        CollectionOperationError::Fatal("second test encoding lacks suffix".to_owned())
    })?;
    let first = Blob::new(bytes.to_vec().into());
    decode_first(&first)?;
    Ok(first)
}

impl CollectionEncoding for SecondEncoding {
    fn validate_member<R>(
        _descriptor: &Fragment,
        member: &Blob<Self>,
        _reader: &R,
    ) -> Result<(), CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        decode_second(member).map(drop)
    }

    fn join_members<R>(
        _descriptor: &Fragment,
        low: &Blob<Self>,
        high: &Blob<Self>,
        _reader: &R,
    ) -> Result<Blob<Self>, CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        let low = decode_first(&decode_second(low)?)?;
        let high = decode_first(&decode_second(high)?)?;
        let joined = crate::collection::simplearchive_union::join(&low, &high)
            .map_err(|error| CollectionOperationError::Fatal(error.to_string()))?;
        let mut bytes = joined.bytes.as_ref().to_vec();
        bytes.extend_from_slice(&[0xA5, 0xB6]);
        Ok(Blob::new(bytes.into()))
    }
}

const FIRST_MAPPING: Id = id_hex!("70D406F7483E8A1D384354D0AFD0D717");
struct FirstMappingAlgorithm;

impl MetaDescribe for FirstMappingAlgorithm {
    fn describe() -> Fragment {
        let id = FIRST_MAPPING;
        crate::macros::entity! { ExclusiveId::force_ref(&id) @
            crate::metadata::name: "invariant-support-test-first-mapping",
            crate::metadata::tag: crate::metadata::KIND_COLLECTION_MAPPING_ALGORITHM,
        }
    }
}

#[derive(Clone, Copy)]
struct FirstMapping;

thread_local! {
    static FIRST_MAP_CALLS: Cell<usize> = const { Cell::new(0) };
    static SECOND_MAP_CALLS: Cell<usize> = const { Cell::new(0) };
    static FIRST_MAP_MISSING: Cell<bool> = const { Cell::new(false) };
    static FIRST_MAP_CAPACITY: RefCell<Option<CollectionData>> = const { RefCell::new(None) };
}

fn reset_mapping_calls() {
    FIRST_MAP_CALLS.set(0);
    SECOND_MAP_CALLS.set(0);
    FIRST_MAP_MISSING.set(false);
    FIRST_MAP_CAPACITY.replace(None);
}

impl CollectionMapping for FirstMapping {
    type Source = SimpleArchive;
    type Target = FirstEncoding;

    fn fragment(&self) -> Fragment {
        crate::macros::entity! {
            crate::metadata::tag: crate::collection::KIND_COLLECTION_MAPPING,
            crate::collection::mapping_algorithm*: <FirstMappingAlgorithm as MetaDescribe>::describe(),
        }
    }

    fn bind(_source: &Fragment, target: &Fragment) -> Result<Self, CollectionOperationError> {
        require_mapping(target, FIRST_MAPPING)?;
        Ok(Self)
    }

    fn map<R>(
        &self,
        source: &Blob<Self::Source>,
        _reader: &R,
    ) -> Result<Blob<Self::Target>, CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        FIRST_MAP_CALLS.set(FIRST_MAP_CALLS.get() + 1);
        if FIRST_MAP_MISSING.get() {
            return Err(CollectionOperationError::MissingDependency(
                crate::inline::Inline::new([0xEE; 32]),
            ));
        }
        if FIRST_MAP_CAPACITY.with_borrow(|blocked| *blocked == Some(data(source))) {
            return Err(CollectionOperationError::Capacity(
                "injected source capacity".to_owned(),
            ));
        }
        let mut bytes = source.bytes.as_ref().to_vec();
        bytes.push(0xA5);
        Ok(Blob::new(bytes.into()))
    }
}

const SECOND_MAPPING: Id = id_hex!("4B671CE9A7CF6F2AEC3AD5F9B2A59FBC");
struct SecondMappingAlgorithm;

impl MetaDescribe for SecondMappingAlgorithm {
    fn describe() -> Fragment {
        let id = SECOND_MAPPING;
        crate::macros::entity! { ExclusiveId::force_ref(&id) @
            crate::metadata::name: "invariant-support-test-second-mapping",
            crate::metadata::tag: crate::metadata::KIND_COLLECTION_MAPPING_ALGORITHM,
        }
    }
}

#[derive(Clone, Copy)]
struct SecondMapping;

impl CollectionMapping for SecondMapping {
    type Source = FirstEncoding;
    type Target = SecondEncoding;

    fn fragment(&self) -> Fragment {
        crate::macros::entity! {
            crate::metadata::tag: crate::collection::KIND_COLLECTION_MAPPING,
            crate::collection::mapping_algorithm*: <SecondMappingAlgorithm as MetaDescribe>::describe(),
        }
    }

    fn bind(_source: &Fragment, target: &Fragment) -> Result<Self, CollectionOperationError> {
        require_mapping(target, SECOND_MAPPING)?;
        Ok(Self)
    }

    fn map<R>(
        &self,
        source: &Blob<Self::Source>,
        _reader: &R,
    ) -> Result<Blob<Self::Target>, CollectionOperationError>
    where
        R: BlobStoreGet + BlobStoreMeta,
    {
        SECOND_MAP_CALLS.set(SECOND_MAP_CALLS.get() + 1);
        decode_first(source)?;
        let mut bytes = source.bytes.as_ref().to_vec();
        bytes.push(0xB6);
        Ok(Blob::new(bytes.into()))
    }
}

fn require_mapping(target: &Fragment, expected: Id) -> Result<(), CollectionOperationError> {
    let actual = descriptor::mapping_algorithm(target.facts())
        .map_err(|error| CollectionOperationError::Fatal(error.to_string()))?;
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(CollectionOperationError::Fatal(format!(
            "mapping algorithm {actual:?} does not match {expected:X}"
        )))
    }
}

fn collections() -> (
    MemoryRepo,
    Collection<SimpleArchive>,
    Collection<FirstEncoding>,
    Collection<SecondEncoding>,
) {
    let mut store = MemoryRepo::default();
    let root = store.collection("root", policy()).unwrap();
    let first = store.derive(root, FirstMapping, policy()).unwrap();
    let second = store.derive(first, SecondMapping, policy()).unwrap();
    (store, root, first, second)
}

fn records(store: &mut MemoryRepo) -> Vec<CollectionRecord> {
    store
        .snapshot()
        .unwrap()
        .records()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteEvent {
    Put(CollectionData),
    Insert(CollectionRecord),
}

#[derive(Debug)]
enum GuardStoreError {
    Injected(&'static str),
    Backend(String),
}

impl fmt::Display for GuardStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Injected(operation) => write!(formatter, "injected {operation} failure"),
            Self::Backend(reason) => formatter.write_str(reason),
        }
    }
}

impl Error for GuardStoreError {}

struct GuardSnapshot {
    inner: MemoryRepoSnapshot,
    live: Arc<AtomicUsize>,
    semantic_probes: Arc<AtomicUsize>,
}

impl Clone for GuardSnapshot {
    fn clone(&self) -> Self {
        self.live.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: self.inner.clone(),
            live: Arc::clone(&self.live),
            semantic_probes: Arc::clone(&self.semantic_probes),
        }
    }
}

impl Drop for GuardSnapshot {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl StoreSnapshot for GuardSnapshot {
    fn changes_since(&self, previous: &Self) -> StoreChanges {
        self.inner.changes_since(&previous.inner)
    }
}

impl WantRead for GuardSnapshot {
    type WantsError = <MemoryRepoSnapshot as WantRead>::WantsError;
    type WantIter<'a> = <MemoryRepoSnapshot as WantRead>::WantIter<'a>;

    fn wants<'a>(&'a self) -> Result<Self::WantIter<'a>, Self::WantsError> {
        self.inner.wants()
    }
}

impl BlobStoreMeta for GuardSnapshot {
    type MetaError = <MemoryRepoSnapshot as BlobStoreMeta>::MetaError;

    fn metadata<S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<Option<BlobMetadata>, Self::MetaError>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.inner.metadata(handle)
    }
}

impl BlobStoreGet for GuardSnapshot {
    type GetError<E: Error + Send + Sync + 'static> =
        <MemoryRepoSnapshot as BlobStoreGet>::GetError<E>;

    fn get<T, S>(
        &self,
        handle: Inline<Handle<S>>,
    ) -> Result<T, Self::GetError<<T as TryFromBlob<S>>::Error>>
    where
        S: BlobEncoding + 'static,
        T: TryFromBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.inner.get(handle)
    }
}

impl BlobStoreList for GuardSnapshot {
    type Iter<'a>
        = <MemoryRepoSnapshot as BlobStoreList>::Iter<'a>
    where
        Self: 'a;
    type Err = <MemoryRepoSnapshot as BlobStoreList>::Err;

    fn blobs<'a>(&'a self) -> Self::Iter<'a> {
        self.inner.blobs()
    }

    fn contains_blob<S>(&self, handle: Inline<Handle<S>>) -> Result<bool, Self::Err>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.inner.contains_blob(handle)
    }

    fn blob_info<S>(&self, handle: Inline<Handle<S>>) -> Result<Option<BlobInfo>, Self::Err>
    where
        S: BlobEncoding + 'static,
        Handle<S>: InlineEncoding,
    {
        self.inner.blob_info(handle)
    }

    fn blobs_diff<'a>(&'a self, previous: &Self) -> Self::Iter<'a> {
        self.inner.blobs_diff(&previous.inner)
    }
}

impl CollectionRead for GuardSnapshot {
    type RecordsError = <MemoryRepoSnapshot as CollectionRead>::RecordsError;
    type RecordIter<'a>
        = <MemoryRepoSnapshot as CollectionRead>::RecordIter<'a>
    where
        Self: 'a;

    fn records<'a>(&'a self) -> Result<Self::RecordIter<'a>, Self::RecordsError> {
        self.inner.records()
    }

    fn record(
        &self,
        fingerprint: CollectionRecordFingerprint,
    ) -> Result<Option<CollectionRecord>, Self::RecordsError> {
        self.inner.record(fingerprint)
    }

    fn select_records(
        &self,
        selectors: &std::collections::BTreeSet<CollectionRecordSelector>,
    ) -> Result<Vec<CollectionRecord>, Self::RecordsError> {
        self.semantic_probes.fetch_add(1, Ordering::SeqCst);
        self.inner.select_records(selectors)
    }
}

impl CapabilityProofRead for GuardSnapshot {
    type ProofsError = <MemoryRepoSnapshot as CapabilityProofRead>::ProofsError;
    type ProofIter<'a>
        = <MemoryRepoSnapshot as CapabilityProofRead>::ProofIter<'a>
    where
        Self: 'a;

    fn proofs<'a>(&'a self) -> Result<Self::ProofIter<'a>, Self::ProofsError> {
        self.inner.proofs()
    }

    fn proof(&self, id: CapabilityProofId) -> Result<Option<CapabilityProof>, Self::ProofsError> {
        self.inner.proof(id)
    }
}

struct GuardStore {
    inner: MemoryRepo,
    live: Arc<AtomicUsize>,
    semantic_probes: Arc<AtomicUsize>,
    events: Vec<WriteEvent>,
    put_calls: usize,
    insert_calls: usize,
    reject_put_at: Option<usize>,
    reject_insert_at: Option<usize>,
    acquirable: BTreeMap<CollectionData, Bytes>,
    acquired: Vec<CollectionData>,
    inject_record_on_acquire: Option<CollectionRecord>,
}

impl GuardStore {
    fn new(inner: MemoryRepo) -> Self {
        Self {
            inner,
            live: Arc::new(AtomicUsize::new(0)),
            semantic_probes: Arc::new(AtomicUsize::new(0)),
            events: Vec::new(),
            put_calls: 0,
            insert_calls: 0,
            reject_put_at: None,
            reject_insert_at: None,
            acquirable: BTreeMap::new(),
            acquired: Vec::new(),
            inject_record_on_acquire: None,
        }
    }

    fn offer<E: BlobEncoding>(&mut self, blob: &Blob<E>)
    where
        Handle<E>: InlineEncoding,
    {
        self.acquirable.insert(data(blob), blob.bytes.clone());
    }

    fn assert_only_control_snapshot(&self) {
        assert_eq!(
            self.live.load(Ordering::SeqCst),
            1,
            "write while a residency snapshot is live, or without the frozen control snapshot",
        );
    }
}

impl AsyncBlobStoreAcquire for GuardStore {
    type AcquireError = GuardStoreError;

    fn acquire(
        &mut self,
        handle: Inline<Handle<UnknownBlob>>,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>, Self::AcquireError>> + Send {
        self.assert_only_control_snapshot();
        let member = Handle::<UnknownBlob>::to_hash(handle);
        self.acquired.push(member);
        let result = match self.acquirable.get(&member).cloned() {
            Some(bytes) => self
                .inner
                .put::<UnknownBlob, _>(bytes.clone())
                .map_err(|error| GuardStoreError::Backend(error.to_string()))
                .map(|_| Some(bytes)),
            None => Ok(None),
        };
        let result = result.and_then(|bytes| {
            if let Some(record) = self.inject_record_on_acquire.take() {
                self.inner
                    .insert(record)
                    .map_err(|error| GuardStoreError::Backend(error.to_string()))?;
            }
            Ok(bytes)
        });
        std::future::ready(result)
    }
}

impl BlobStorePut for GuardStore {
    type PutError = GuardStoreError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.assert_only_control_snapshot();
        let blob = item.to_blob();
        self.put_calls += 1;
        self.events
            .push(WriteEvent::Put(Handle::<S>::to_hash(blob.get_handle())));
        if self.reject_put_at == Some(self.put_calls) {
            return Err(GuardStoreError::Injected("put"));
        }
        self.inner
            .put::<S, _>(blob)
            .map_err(|error| GuardStoreError::Backend(error.to_string()))
    }
}

impl SnapshotSource for GuardStore {
    type Snapshot = GuardSnapshot;
    type SnapshotError = <MemoryRepo as SnapshotSource>::SnapshotError;

    fn snapshot(&mut self) -> Result<Self::Snapshot, Self::SnapshotError> {
        let inner = self.inner.snapshot()?;
        self.live.fetch_add(1, Ordering::SeqCst);
        Ok(GuardSnapshot {
            inner,
            live: Arc::clone(&self.live),
            semantic_probes: Arc::clone(&self.semantic_probes),
        })
    }
}

impl CollectionStore for GuardStore {
    type InsertError = GuardStoreError;

    fn insert(&mut self, record: CollectionRecord) -> Result<(), Self::InsertError> {
        self.assert_only_control_snapshot();
        self.insert_calls += 1;
        self.events.push(WriteEvent::Insert(record));
        if self.reject_insert_at == Some(self.insert_calls) {
            return Err(GuardStoreError::Injected("collection insert"));
        }
        self.inner
            .insert(record)
            .map_err(|error| GuardStoreError::Backend(error.to_string()))
    }
}

impl CapabilityProofStore for GuardStore {
    type InsertError = GuardStoreError;

    fn insert_proof(&mut self, proof: CapabilityProof) -> Result<(), Self::InsertError> {
        self.assert_only_control_snapshot();
        self.inner
            .insert_proof(proof)
            .map_err(|error| GuardStoreError::Backend(error.to_string()))
    }
}

#[test]
fn foundation_walks_the_complete_descriptor_ancestry() {
    let (mut store, root, _first, second) = collections();
    let snapshot = store.snapshot().unwrap();
    assert_eq!(foundation(&snapshot, second).unwrap(), root);
}

#[test]
fn downstream_ensure_requires_an_existing_immediate_source_realization() {
    let (mut store, root, first, second) = collections();
    let source = archive(1, 1);
    store.put::<SimpleArchive, _>(source.clone()).unwrap();
    let support = support(root, std::slice::from_ref(&source));

    let result = ensure_exact_resident::<_, SecondMapping>(&mut store, second, &support);
    assert!(matches!(
        result,
        Err(CollectionRealizationError::IncompleteCover { .. })
    ));
    assert!(!records(&mut store).iter().any(|record| matches!(
        record,
        CollectionRecord::Derive(derive)
            if derive.collection() == first.handle() || derive.collection() == second.handle()
    )));
}

#[test]
fn two_hops_reuse_one_invariant_foundational_support() {
    let (mut store, root, first, second) = collections();
    let source = archive(1, 1);
    store.put::<SimpleArchive, _>(source.clone()).unwrap();
    let support = support(root, std::slice::from_ref(&source));

    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    ensure_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();
    let snapshot = store.snapshot().unwrap();
    let (observed_support, cover) = attach_collection_exact(&snapshot, second, &support).unwrap();

    assert_eq!(observed_support, support);
    assert_eq!(cover.len(), 1);
    let output: Blob<SecondEncoding> = snapshot.get(cover.members().next().unwrap()).unwrap();
    assert_eq!(output.bytes.as_ref().last(), Some(&0xB6));

    let second_derive = records(&mut store)
        .into_iter()
        .find_map(|record| match record {
            CollectionRecord::Derive(derive) if derive.collection() == second.handle() => {
                Some(derive)
            }
            _ => None,
        })
        .unwrap();
    assert_ne!(second_derive.input(), data(&source));
}

#[test]
fn ordinary_attachment_reports_only_support_realized_in_its_snapshot() {
    let (mut store, root, first, _second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    publish_root(&mut store, root, &left, 1);
    publish_root(&mut store, root, &right, 2);
    let left_support = support(root, std::slice::from_ref(&left));
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &left_support).unwrap();

    let snapshot = store.snapshot().unwrap();
    let (observed, cover) =
        attach_collection_at(&snapshot, first, hifitime::Epoch::from_tai_seconds(0.0)).unwrap();
    assert_eq!(observed, left_support);
    assert_eq!(cover.len(), 1);
}

#[test]
fn duplicate_commit_fibers_collapse_to_one_support_member() {
    let (mut store, root, _first, _second) = collections();
    let source = archive(1, 1);
    publish_root(&mut store, root, &source, 1);
    publish_root(&mut store, root, &source, 2);

    let snapshot = store.snapshot().unwrap();
    let admitted = root
        .admitted_at(&snapshot, hifitime::Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted.members().next(), Some(source.get_handle()));
    assert_eq!(admitted.commits(&snapshot).unwrap().len(), 2);
}

#[test]
fn ensure_drops_every_residency_snapshot_and_stores_the_blob_before_derive() {
    let (mut inner, root, first, _second) = collections();
    let source = archive(1, 1);
    inner.put::<SimpleArchive, _>(source.clone()).unwrap();
    let support = support(root, &[source]);
    let mut store = GuardStore::new(inner);

    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();

    assert_eq!(store.live.load(Ordering::SeqCst), 0);
    let (insert_position, derive) = store
        .events
        .iter()
        .enumerate()
        .find_map(|(position, event)| match event {
            WriteEvent::Insert(CollectionRecord::Derive(derive)) => Some((position, *derive)),
            _ => None,
        })
        .expect("ensure must publish one DERIVE");
    let put_position = store
        .events
        .iter()
        .position(|event| matches!(event, WriteEvent::Put(data) if *data == derive.output()))
        .expect("ensure must store its target member");
    assert!(put_position < insert_position);
}

#[test]
fn exact_ensure_acquires_explicit_foundational_support_without_want() {
    let (inner, root, first, _second) = collections();
    let source = archive(1, 1);
    let support = support(root, std::slice::from_ref(&source));
    let mut store = GuardStore::new(inner);
    store.offer(&source);

    let snapshot = block_on(store.ensure_exact::<FirstMapping>(first, &support)).unwrap();

    assert_eq!(store.acquired, vec![data(&source)]);
    assert_eq!(snapshot.wants().unwrap().count(), 0);
    let (observed, cover) = attach_collection_exact(&snapshot, first, &support).unwrap();
    assert_eq!(observed, support);
    assert_eq!(cover.len(), 1);
}

#[test]
fn async_ensure_hydrates_only_the_bounded_admitted_commit_frontier() {
    let (mut inner, root, first, _second) = collections();
    let source = archive(1, 1);
    let metadata = inner
        .put::<SimpleArchive, _>(TribleSet::new().to_blob())
        .unwrap();
    inner
        .insert(CollectionRecord::Commit(CollectionCommit::sign(
            &SigningKey::from_bytes(&[42; 32]),
            root.handle(),
            data(&source),
            metadata,
        )))
        .unwrap();
    let mut store = GuardStore::new(inner);
    store.offer(&source);
    let concurrent = archive(9, 9);
    store
        .inner
        .put::<SimpleArchive, _>(concurrent.clone())
        .unwrap();
    store.inject_record_on_acquire = Some(CollectionRecord::Commit(CollectionCommit::sign(
        &SigningKey::from_bytes(&[43; 32]),
        root.handle(),
        data(&concurrent),
        metadata,
    )));

    let snapshot = block_on(store.ensure::<FirstMapping>(first)).unwrap();

    assert_eq!(store.acquired, vec![data(&source)]);
    assert_eq!(snapshot.wants().unwrap().count(), 0);
    let observed = snapshot
        .collection_at(first, hifitime::Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(
        observed.support().data_members().collect::<Vec<_>>(),
        vec![data(&source)]
    );
    assert_eq!(observed.cover().len(), 1);
}

#[test]
fn async_ensure_hydrates_relevant_proof_before_authorized_commit_payload() {
    let authority = SigningKey::from_bytes(&[43; 32]);
    let writer = SigningKey::from_bytes(&[44; 32]);
    let restricted = CollectionPolicy::new(
        AdmissionPolicy::Open,
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let mut inner = MemoryRepo::default();
    let root = inner.collection("restricted-root", restricted).unwrap();
    let first = inner.derive(root, FirstMapping, policy()).unwrap();
    let source = archive(2, 2);
    let metadata = inner
        .put::<SimpleArchive, _>(TribleSet::new().to_blob())
        .unwrap();
    inner
        .insert(CollectionRecord::Commit(CollectionCommit::sign(
            &writer,
            root.handle(),
            data(&source),
            metadata,
        )))
        .unwrap();
    let atom = CapabilityAtom::new(
        CapabilityAction::new(ACTION_WRITE),
        CapabilityResource::from(root.handle()),
    );
    let bundle = CapabilityProofBundle::issue_root(
        &authority,
        CapabilityClaim::root(atom, CapabilityMode::Invoke, None),
        writer.verifying_key(),
    )
    .unwrap();
    let (proof, claims) = bundle.into_parts();
    inner.insert_proof(proof).unwrap();

    let claim_members = claims.iter().map(data).collect::<Vec<_>>();
    let mut store = GuardStore::new(inner);
    for claim in &claims {
        store.offer(claim);
    }
    store.offer(&source);

    let snapshot = block_on(store.ensure::<FirstMapping>(first)).unwrap();

    assert_eq!(
        &store.acquired[..claim_members.len()],
        claim_members.as_slice(),
        "proof closure is acquired before payload authorization",
    );
    assert_eq!(store.acquired.last(), Some(&data(&source)));
    assert_eq!(snapshot.wants().unwrap().count(), 0);
    let observed = snapshot
        .collection_at(first, hifitime::Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(
        observed.support().data_members().collect::<Vec<_>>(),
        vec![data(&source)]
    );
}

#[test]
fn maintenance_drops_every_residency_snapshot_and_stores_the_blob_before_merge() {
    let (mut inner, root, first, second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        inner.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let support = support(root, &[left, right]);
    ensure_exact_resident::<_, FirstMapping>(&mut inner, first, &support).unwrap();
    ensure_exact_resident::<_, SecondMapping>(&mut inner, second, &support).unwrap();
    let mut store = GuardStore::new(inner);

    maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();

    assert_eq!(store.live.load(Ordering::SeqCst), 0);
    let (insert_position, merge) = store
        .events
        .iter()
        .enumerate()
        .find_map(|(position, event)| match event {
            WriteEvent::Insert(CollectionRecord::Merge(merge)) => Some((position, *merge)),
            _ => None,
        })
        .expect("maintenance must publish one MERGE");
    let put_position = store
        .events
        .iter()
        .position(|event| matches!(event, WriteEvent::Put(data) if *data == merge.result()))
        .expect("maintenance must store its joined target member");
    assert!(put_position < insert_position);
}

#[test]
fn target_maintenance_reprobes_once_per_tier_not_per_carry() {
    const MEMBERS: usize = 8;

    let (mut inner, root, first, second) = collections();
    let members: Vec<_> = (0..MEMBERS)
        .map(|member| archive(member as u8 + 1, member as u8 + 1))
        .collect();
    for member in &members {
        inner.put::<SimpleArchive, _>(member.clone()).unwrap();
    }
    let support = support(root, &members);
    ensure_exact_resident::<_, FirstMapping>(&mut inner, first, &support).unwrap();
    ensure_exact_resident::<_, SecondMapping>(&mut inner, second, &support).unwrap();

    let mut store = GuardStore::new(inner);
    maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();

    let merges = store
        .events
        .iter()
        .filter(|event| matches!(event, WriteEvent::Insert(CollectionRecord::Merge(_))))
        .count();
    assert_eq!(merges, MEMBERS - 1);
    assert_eq!(
        store.semantic_probes.load(Ordering::SeqCst),
        5,
        "one ensure probe plus one target-resolution probe for each dyadic tier round",
    );

    let snapshot = store.inner.snapshot().unwrap();
    let (_, cover) = attach_collection_exact(&snapshot, second, &support).unwrap();
    assert_eq!(cover.len(), 1);
}

#[test]
fn failed_target_batch_preserves_every_published_prefix_carry() {
    for fail_insert in [false, true] {
        let (mut inner, root, first, second) = collections();
        let members: Vec<_> = (1..=4).map(|member| archive(member, member)).collect();
        for member in &members {
            inner.put::<SimpleArchive, _>(member.clone()).unwrap();
        }
        let support = support(root, &members);
        ensure_exact_resident::<_, FirstMapping>(&mut inner, first, &support).unwrap();
        ensure_exact_resident::<_, SecondMapping>(&mut inner, second, &support).unwrap();

        let mut store = GuardStore::new(inner);
        if fail_insert {
            store.reject_insert_at = Some(2);
        } else {
            store.reject_put_at = Some(2);
        }

        assert!(matches!(
            maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support),
            Err(CollectionRealizationError::Storage { .. })
        ));
        assert_eq!(store.live.load(Ordering::SeqCst), 0);
        let merges = records(&mut store.inner)
            .into_iter()
            .filter(|record| {
                matches!(record, CollectionRecord::Merge(merge) if merge.collection() == second.handle())
            })
            .count();
        assert_eq!(merges, 1, "the first target carry remains visible");
    }
}

#[test]
fn later_put_or_insert_failure_preserves_the_published_prefix() {
    for fail_insert in [false, true] {
        let (mut inner, root, first, _second) = collections();
        let left = archive(1, 1);
        let right = archive(2, 2);
        for blob in [&left, &right] {
            inner.put::<SimpleArchive, _>(blob.clone()).unwrap();
        }
        let support = support(root, &[left, right]);
        let mut store = GuardStore::new(inner);
        if fail_insert {
            store.reject_insert_at = Some(2);
        } else {
            store.reject_put_at = Some(2);
        }

        assert!(matches!(
            ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support),
            Err(CollectionRealizationError::Storage { .. })
        ));
        let derives = records(&mut store.inner)
            .into_iter()
            .filter(|record| {
                matches!(record, CollectionRecord::Derive(derive) if derive.collection() == first.handle())
            })
            .count();
        assert_eq!(derives, 1, "the first successful mapping remains visible");
    }
}

#[test]
fn missing_mapping_dependency_publishes_nothing() {
    let (mut store, root, first, _second) = collections();
    let source = archive(1, 1);
    store.put::<SimpleArchive, _>(source.clone()).unwrap();
    let support = support(root, &[source]);
    let before = store.snapshot().unwrap();
    FIRST_MAP_MISSING.set(true);

    let result = ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support);
    FIRST_MAP_MISSING.set(false);

    assert!(matches!(
        result,
        Err(CollectionRealizationError::MissingDependency { .. })
    ));
    assert!(before == store.snapshot().unwrap());
}

#[test]
fn capacity_blocked_source_upper_falls_back_to_its_resident_children() {
    let (mut store, root, first, _second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let joined = crate::collection::simplearchive_union::join(&left, &right).unwrap();
    store.put::<SimpleArchive, _>(joined.clone()).unwrap();
    store
        .insert(CollectionRecord::Merge(CollectionMerge::new(
            root.handle(),
            data(&left),
            data(&right),
            data(&joined),
        )))
        .unwrap();
    let support = support(root, &[left.clone(), right.clone()]);
    reset_mapping_calls();
    FIRST_MAP_CAPACITY.replace(Some(data(&joined)));

    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    FIRST_MAP_CAPACITY.replace(None);

    assert_eq!(FIRST_MAP_CALLS.get(), 3);
    let inputs: std::collections::BTreeSet<_> = records(&mut store)
        .into_iter()
        .filter_map(|record| match record {
            CollectionRecord::Derive(derive) if derive.collection() == first.handle() => {
                Some(derive.input())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        inputs,
        std::collections::BTreeSet::from([data(&left), data(&right)])
    );
}

#[test]
fn warm_exact_ensure_is_a_zero_write_zero_algebra_observation() {
    let (mut store, root, first, _second) = collections();
    let source = archive(1, 1);
    store.put::<SimpleArchive, _>(source.clone()).unwrap();
    let support = support(root, &[source]);
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();

    reset_mapping_calls();
    let before = store.snapshot().unwrap();
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    let after = store.snapshot().unwrap();
    assert!(before == after);
    assert_eq!(FIRST_MAP_CALLS.get(), 0);
}

#[test]
fn existing_target_support_is_not_mapped_again_when_support_grows() {
    let (mut store, root, first, _second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let left_support = support(root, std::slice::from_ref(&left));
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &left_support).unwrap();

    reset_mapping_calls();
    let full_support = support(root, &[left, right]);
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &full_support).unwrap();
    assert_eq!(FIRST_MAP_CALLS.get(), 1);
}

#[test]
fn resident_source_upper_is_mapped_instead_of_its_finer_children() {
    let (mut store, root, first, second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let support = support(root, &[left, right]);
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();

    let snapshot = store.snapshot().unwrap();
    let (_, first_cover) = attach_collection_exact(&snapshot, first, &support).unwrap();
    let mut children = first_cover.members().map(|handle| {
        snapshot
            .get::<Blob<FirstEncoding>, FirstEncoding>(handle)
            .unwrap()
    });
    let low = children.next().unwrap();
    let high = children.next().unwrap();
    drop(snapshot);
    let upper = join_first(&low, &high).unwrap();
    store.put::<FirstEncoding, _>(upper.clone()).unwrap();
    store
        .insert(CollectionRecord::Merge(CollectionMerge::new(
            first.handle(),
            data(&low),
            data(&high),
            data(&upper),
        )))
        .unwrap();

    reset_mapping_calls();
    ensure_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();
    assert_eq!(SECOND_MAP_CALLS.get(), 1);
    assert!(records(&mut store).iter().any(|record| matches!(
        record,
        CollectionRecord::Derive(derive)
            if derive.collection() == second.handle() && derive.input() == data(&upper)
    )));
}

#[test]
fn optional_target_dependency_keeps_the_finer_cover() {
    let (mut store, root, first, _second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let support = support(root, &[left, right]);

    maintain_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    let snapshot = store.snapshot().unwrap();
    let (_, cover) = attach_collection_exact(&snapshot, first, &support).unwrap();
    assert_eq!(cover.len(), 2);
    drop(snapshot);
    let first_result = store.snapshot().unwrap();
    maintain_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    assert!(first_result == store.snapshot().unwrap());
    assert!(!records(&mut store).iter().any(|record| matches!(
        record,
        CollectionRecord::Merge(merge) if merge.collection() == root.handle()
    )));
}

#[test]
fn target_maintenance_publishes_only_horizontal_target_merges() {
    let (mut store, root, first, second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let support = support(root, &[left, right]);
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();

    let snapshot = store.snapshot().unwrap();
    let (_, cover) = attach_collection_exact(&snapshot, second, &support).unwrap();
    assert_eq!(cover.len(), 1);
    let all = records(&mut store);
    assert!(all.iter().any(|record| matches!(
        record,
        CollectionRecord::Merge(merge) if merge.collection() == second.handle()
    )));
    assert!(!all.iter().any(|record| matches!(
        record,
        CollectionRecord::Merge(merge) if merge.collection() == root.handle()
    )));
}

#[test]
fn target_maintenance_is_deterministic_and_repeatedly_idempotent() {
    let (mut store, root, first, second) = collections();
    let left = archive(1, 1);
    let right = archive(2, 2);
    for blob in [&left, &right] {
        store.put::<SimpleArchive, _>(blob.clone()).unwrap();
    }
    let support = support(root, &[left, right]);
    ensure_exact_resident::<_, FirstMapping>(&mut store, first, &support).unwrap();
    maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();
    let first_result = store.snapshot().unwrap();

    maintain_exact_resident::<_, SecondMapping>(&mut store, second, &support).unwrap();
    let second_result = store.snapshot().unwrap();
    assert!(first_result == second_result);
}
