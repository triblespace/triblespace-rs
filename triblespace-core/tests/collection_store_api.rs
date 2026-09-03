use std::collections::BTreeMap;

use ed25519_dalek::{SigningKey, VerifyingKey};
use hifitime::Epoch;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::SuccinctArchiveBlob;
use triblespace_core::blob::encodings::utf8string::UTF8String;
use triblespace_core::blob::{Blob, BlobEncoding, IntoBlob};
use triblespace_core::capability::{
    CapabilityAction, CapabilityAtom, CapabilityClaim, CapabilityMode, CapabilityProof,
    CapabilityProofBundle, CapabilityRequest, CapabilityResource,
};
use triblespace_core::collection::descriptor;
use triblespace_core::collection::{
    collection_read_audience_at, grant_collection_read, grant_collection_write, AdmissionPolicy,
    Collection, CollectionDescriptorError, CollectionOpenError, CollectionPolicy,
    CollectionReadAudience, CollectionReadGrantError, CollectionRecord, CollectionStore,
    CollectionStoreExt, CollectionTypeError, CollectionWriteGrantError, PreparedCollectionCommit,
    ACTION_READ, ACTION_WRITE,
};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, InlineEncoding};
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStoreGet, BlobStorePut, CapabilityProofRead, CapabilityProofStore, SnapshotSource,
};
use triblespace_core::trible::{Fragment, Trible, TribleSet, TRIBLE_LEN};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreEvent {
    Put([u8; 32]),
    Insert(triblespace_core::collection::CollectionRecordFingerprint),
    Proof([u8; 32]),
}

#[derive(Default)]
struct CountingRepo {
    inner: MemoryRepo,
    puts: BTreeMap<[u8; 32], usize>,
    events: Vec<StoreEvent>,
}

impl CountingRepo {
    fn puts_for<S>(&self, handle: Inline<Handle<S>>) -> usize
    where
        S: BlobEncoding,
        Handle<S>: InlineEncoding,
    {
        self.puts.get(&handle.raw).copied().unwrap_or_default()
    }
}

impl BlobStorePut for CountingRepo {
    type PutError = <MemoryRepo as BlobStorePut>::PutError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: triblespace_core::blob::IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        let handle = self.inner.put(item)?;
        *self.puts.entry(handle.raw).or_default() += 1;
        self.events.push(StoreEvent::Put(handle.raw));
        Ok(handle)
    }
}

impl CollectionStore for CountingRepo {
    type InsertError = <MemoryRepo as CollectionStore>::InsertError;

    fn insert(&mut self, record: CollectionRecord) -> Result<(), Self::InsertError> {
        self.events.push(StoreEvent::Insert(record.fingerprint()));
        self.inner.insert(record)
    }
}

impl SnapshotSource for CountingRepo {
    type Snapshot = <MemoryRepo as SnapshotSource>::Snapshot;
    type SnapshotError = <MemoryRepo as SnapshotSource>::SnapshotError;

    fn snapshot(&mut self) -> Result<Self::Snapshot, Self::SnapshotError> {
        self.inner.snapshot()
    }
}

impl CapabilityProofStore for CountingRepo {
    type InsertError = <MemoryRepo as CapabilityProofStore>::InsertError;

    fn insert_proof(&mut self, proof: CapabilityProof) -> Result<(), Self::InsertError> {
        self.events.push(StoreEvent::Proof(proof.id().raw));
        self.inner.insert_proof(proof)
    }
}

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

fn policy(root: VerifyingKey) -> CollectionPolicy {
    CollectionPolicy::new(
        AdmissionPolicy::delegable(root),
        AdmissionPolicy::direct(root),
    )
}

fn fragment(entity: u8) -> Fragment {
    let mut row = [entity; TRIBLE_LEN];
    row[16..32].fill(entity.wrapping_add(1));
    row[32..].fill(entity.wrapping_add(2));
    let mut facts = TribleSet::new();
    facts.insert(&Trible::force_raw(row).unwrap());
    Fragment::from(facts)
}

fn atom(action: triblespace_core::id::Id, collection: Collection<SimpleArchive>) -> CapabilityAtom {
    CapabilityAtom::new(
        CapabilityAction::new(action),
        CapabilityResource::from(collection.handle()),
    )
}

fn store_bundle(store: &mut MemoryRepo, bundle: CapabilityProofBundle) {
    let (proof, claims) = bundle.into_parts();
    for claim in claims {
        store.put::<SimpleArchive, _>(claim).unwrap();
    }
    store.insert_proof(proof).unwrap();
}

#[test]
fn root_creation_registers_a_self_contained_descriptor() {
    let root = key(1);
    let expected_policy = policy(root.verifying_key());
    let mut store = MemoryRepo::default();

    let collection = store
        .collection("collection-store-api", expected_policy.clone())
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    let descriptor_blob = snapshot
        .get::<TribleSet, SimpleArchive>(collection.handle())
        .unwrap();

    assert_eq!(descriptor::policy(&descriptor_blob), Ok(expected_policy));
    let name = descriptor::name(&descriptor_blob).unwrap().unwrap();
    let name: anybytes::View<str> = snapshot.get::<_, UTF8String>(name).unwrap();
    assert_eq!(&*name, "collection-store-api");
}

#[test]
fn typed_collection_open_accepts_the_registered_encoding() {
    let root = key(20);
    let mut store = MemoryRepo::default();
    let registered = store
        .collection("typed-open", policy(root.verifying_key()))
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    let opened = Collection::<SimpleArchive>::open(&snapshot, registered.handle()).unwrap();

    assert_eq!(opened, registered);
}

#[test]
fn typed_collection_names_an_exact_coordinate_without_store_access() {
    let root = key(21);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection("durable-exact-cover", policy(root.verifying_key()))
        .unwrap();
    let low = Inline::<Handle<SimpleArchive>>::new([0x31; 32]);
    let high = Inline::<Handle<SimpleArchive>>::new([0x42; 32]);

    let cover = collection.cover([high, low, high]);

    assert_eq!(cover.collection(), collection);
    assert_eq!(cover.members().collect::<Vec<_>>(), vec![low, high]);
}

#[test]
fn typed_collection_reads_its_descriptor_local_policy() {
    let source_root = key(22);
    let target_root = key(23);
    let source_policy = policy(source_root.verifying_key());
    let target_policy = CollectionPolicy::new(
        AdmissionPolicy::Open,
        AdmissionPolicy::direct(target_root.verifying_key()),
    );
    let mut store = MemoryRepo::default();
    let source = store.collection("policy-source", source_policy).unwrap();
    let target = store
        .derive::<SuccinctArchiveBlob>(source, (), target_policy.clone())
        .unwrap();

    let snapshot = store.snapshot().unwrap();

    assert_eq!(target.policy(&snapshot).unwrap(), target_policy);
}

#[test]
fn typed_collection_open_rejects_the_wrong_encoding() {
    let root = key(21);
    let mut store = MemoryRepo::default();
    let source = store
        .collection("source", policy(root.verifying_key()))
        .unwrap();
    let registered = store
        .derive::<SuccinctArchiveBlob>(source, (), policy(root.verifying_key()))
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    let error = Collection::<SimpleArchive>::open(&snapshot, registered.handle()).unwrap_err();

    assert!(matches!(
        error,
        CollectionOpenError::WrongType(CollectionTypeError::WrongEncoding { .. })
    ));
}

#[test]
fn typed_collection_open_rejects_an_invalid_descriptor() {
    let mut store = MemoryRepo::default();
    let invalid = store.put::<SimpleArchive, _>(TribleSet::new()).unwrap();

    let snapshot = store.snapshot().unwrap();
    let error = Collection::<SimpleArchive>::open(&snapshot, invalid).unwrap_err();

    assert!(matches!(
        error,
        CollectionOpenError::Descriptor(CollectionDescriptorError::Invalid {
            collection,
            ..
        }) if collection == invalid
    ));
}

#[test]
fn read_grant_is_root_checked_commit_last_and_replay_deterministic() {
    let root = key(25);
    let reader = key(26);
    let mut store = CountingRepo::default();
    let collection = store
        .collection("root-granted-read", policy(root.verifying_key()))
        .unwrap();
    store.events.clear();

    let first = grant_collection_read(
        &mut store,
        collection.handle(),
        &root,
        reader.verifying_key(),
    )
    .unwrap();
    let claim_handle = first.proof().leaf_claim();
    assert_eq!(
        store.events,
        vec![
            StoreEvent::Put(claim_handle.raw),
            StoreEvent::Proof(first.proof().id().raw),
        ]
    );

    let claim = CapabilityClaim::from_blob(first.claims()[0].clone()).unwrap();
    assert_eq!(claim.parent(), None);
    assert_eq!(claim.mode(), CapabilityMode::Invoke);
    assert_eq!(claim.validity(), None);
    assert_eq!(claim.atom(), atom(ACTION_READ, collection));
    assert_eq!(first.proof().leaf_key(), reader.verifying_key());

    store.events.clear();
    let replay = grant_collection_read(
        &mut store,
        collection.handle(),
        &root,
        reader.verifying_key(),
    )
    .unwrap();
    assert_eq!(replay, first);
    assert_eq!(
        store.events,
        vec![
            StoreEvent::Put(claim_handle.raw),
            StoreEvent::Proof(first.proof().id().raw),
        ]
    );

    let snapshot = store.snapshot().unwrap();
    let proofs = snapshot
        .proofs()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(proofs, vec![first.proof().clone()]);
    let stored_claim: Blob<SimpleArchive> = snapshot.get(claim_handle).unwrap();
    assert_eq!(stored_claim, first.claims()[0]);
    assert!(collection
        .reader_is_admitted_at(
            &snapshot,
            reader.verifying_key(),
            Epoch::from_tai_seconds(0.0),
        )
        .unwrap());
}

#[test]
fn write_grant_is_root_checked_commit_last_and_activates_recipient_commits() {
    let root = key(34);
    let writer = key(35);
    let mut store = CountingRepo::default();
    let collection = store
        .collection(
            "root-granted-write",
            CollectionPolicy::new(
                AdmissionPolicy::Open,
                AdmissionPolicy::direct(root.verifying_key()),
            ),
        )
        .unwrap();
    let commit = store.commit(collection, &writer, fragment(36)).unwrap();
    let snapshot = store.snapshot().unwrap();
    assert!(collection
        .admitted_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap()
        .is_empty());
    drop(snapshot);
    store.events.clear();

    let bundle = grant_collection_write(
        &mut store,
        collection.handle(),
        &root,
        writer.verifying_key(),
    )
    .unwrap();
    let claim_handle = bundle.proof().leaf_claim();
    assert_eq!(
        store.events,
        vec![
            StoreEvent::Put(claim_handle.raw),
            StoreEvent::Proof(bundle.proof().id().raw),
        ]
    );

    let claim = CapabilityClaim::from_blob(bundle.claims()[0].clone()).unwrap();
    assert_eq!(claim.parent(), None);
    assert_eq!(claim.mode(), CapabilityMode::Invoke);
    assert_eq!(claim.validity(), None);
    assert_eq!(claim.atom(), atom(ACTION_WRITE, collection));
    assert_eq!(bundle.proof().leaf_key(), writer.verifying_key());

    let snapshot = store.snapshot().unwrap();
    assert!(collection
        .writer_is_admitted_at(
            &snapshot,
            writer.verifying_key(),
            Epoch::from_tai_seconds(0.0),
        )
        .unwrap());
    let admitted = collection
        .admitted_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(admitted.len(), 1);
    assert!(admitted.contains(Handle::<SimpleArchive>::from_hash(commit.data())));
}

#[test]
fn read_grant_rejects_non_root_without_writing() {
    let root = key(27);
    let stranger = key(28);
    let reader = key(29);
    let mut store = CountingRepo::default();
    let collection = store
        .collection("root-checked-read", policy(root.verifying_key()))
        .unwrap();
    store.events.clear();

    let error = grant_collection_read(
        &mut store,
        collection.handle(),
        &stranger,
        reader.verifying_key(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CollectionReadGrantError::RootNotAuthorized {
            action: ACTION_READ,
            collection: rejected,
            root: rejected_root,
        } if rejected == collection.handle() && rejected_root == stranger.verifying_key()
    ));
    assert!(store.events.is_empty());
}

#[test]
fn read_grant_rejects_an_invalid_descriptor_without_writing() {
    let root = key(32);
    let reader = key(33);
    let mut store = CountingRepo::default();
    let invalid = store.put::<SimpleArchive, _>(TribleSet::new()).unwrap();
    store.events.clear();

    let error =
        grant_collection_read(&mut store, invalid, &root, reader.verifying_key()).unwrap_err();

    assert!(matches!(
        error,
        CollectionReadGrantError::Descriptor(CollectionDescriptorError::Invalid {
            collection,
            ..
        }) if collection == invalid
    ));
    assert!(store.events.is_empty());
}

#[test]
fn read_grant_rejects_redundant_open_policy_without_writing() {
    let root = key(30);
    let reader = key(31);
    let mut store = CountingRepo::default();
    let collection = store
        .collection(
            "already-open-read",
            CollectionPolicy::new(
                AdmissionPolicy::Open,
                AdmissionPolicy::direct(root.verifying_key()),
            ),
        )
        .unwrap();
    store.events.clear();

    let error = grant_collection_read(
        &mut store,
        collection.handle(),
        &root,
        reader.verifying_key(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CollectionReadGrantError::OpenPolicy {
            action: ACTION_READ,
            collection: rejected,
        }
            if rejected == collection.handle()
    ));
    assert!(store.events.is_empty());
}

#[test]
fn write_grant_checks_write_roots_not_read_roots() {
    let read_root = key(37);
    let write_root = key(38);
    let writer = key(39);
    let mut store = CountingRepo::default();
    let collection = store
        .collection(
            "distinct-write-root",
            CollectionPolicy::new(
                AdmissionPolicy::direct(read_root.verifying_key()),
                AdmissionPolicy::direct(write_root.verifying_key()),
            ),
        )
        .unwrap();
    store.events.clear();

    let error = grant_collection_write(
        &mut store,
        collection.handle(),
        &read_root,
        writer.verifying_key(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        CollectionWriteGrantError::RootNotAuthorized {
            action: ACTION_WRITE,
            collection: rejected,
            root: rejected_root,
        } if rejected == collection.handle() && rejected_root == read_root.verifying_key()
    ));
    assert!(store.events.is_empty());
}

#[test]
fn commit_is_local_and_correct_by_construction() {
    let root = key(2);
    let mut store = CountingRepo::default();
    let collection = store
        .collection("registered", policy(root.verifying_key()))
        .unwrap();
    let descriptor_puts = store.puts_for(collection.handle());
    store.events.clear();

    let expected_data = fragment(7).facts().clone().to_blob().get_handle();
    let commit = store.commit(collection, &root, fragment(7)).unwrap();

    assert_eq!(
        Handle::<SimpleArchive>::from_hash(commit.data()),
        expected_data
    );
    assert_eq!(descriptor_puts, 1);
    assert_eq!(store.puts_for(collection.handle()), descriptor_puts);
    assert_eq!(
        store.events.last(),
        Some(&StoreEvent::Insert(
            CollectionRecord::Commit(commit).fingerprint()
        ))
    );
}

#[test]
fn prepared_fragment_preserves_data_and_metadata_identity_until_commit_last() {
    let root = key(22);
    let mut store = CountingRepo::default();
    let collection = store
        .collection("prepared", policy(root.verifying_key()))
        .unwrap();
    store.events.clear();

    let mut candidate = fragment(23);
    candidate.describe_with(fragment(24));
    let expected_data = candidate.facts().clone().to_blob().get_handle();
    let expected_metadata = candidate.metafacts().clone().to_blob().get_handle();

    let prepared = PreparedCollectionCommit::from_fragment(candidate);
    let mut staged = prepared.stage_for(&mut store, collection, &root).unwrap();
    let withheld = *staged.commit();

    assert_eq!(
        Handle::<SimpleArchive>::from_hash(withheld.data()),
        expected_data
    );
    assert_eq!(withheld.metadata(), expected_metadata);
    assert!(staged
        .store_mut()
        .events
        .iter()
        .all(|event| matches!(event, StoreEvent::Put(_))));

    let committed = staged.finalize().unwrap();
    assert_eq!(committed, withheld);
    assert_eq!(
        store.events.last(),
        Some(&StoreEvent::Insert(
            CollectionRecord::Commit(withheld).fingerprint()
        ))
    );
}

#[test]
fn read_and_write_policies_are_independent() {
    let root = key(3);
    let stranger = key(4);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection(
            "independent-actions",
            CollectionPolicy::new(
                AdmissionPolicy::Open,
                AdmissionPolicy::direct(root.verifying_key()),
            ),
        )
        .unwrap();
    store.commit(collection, &root, fragment(1)).unwrap();
    store.commit(collection, &stranger, fragment(2)).unwrap();

    let snapshot = store.snapshot().unwrap();
    assert!(collection
        .reader_is_admitted_at(
            &snapshot,
            stranger.verifying_key(),
            Epoch::from_tai_seconds(0.0)
        )
        .unwrap());
    assert!(!collection
        .writer_is_admitted_at(
            &snapshot,
            stranger.verifying_key(),
            Epoch::from_tai_seconds(0.0)
        )
        .unwrap());
    let (cover, commits) = collection
        .admitted_with_commits_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(cover.len(), 1);
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].public_key().raw, root.verifying_key().to_bytes());
}

#[test]
fn direct_policy_accepts_root_grants_but_blocks_redelegation() {
    let root = key(5);
    let intermediary = key(6);
    let leaf = key(7);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection(
            "direct-only",
            CollectionPolicy::new(
                AdmissionPolicy::direct(root.verifying_key()),
                AdmissionPolicy::direct(root.verifying_key()),
            ),
        )
        .unwrap();
    let write_atom = atom(ACTION_WRITE, collection);
    let parent_bundle = CapabilityProofBundle::issue_root(
        &root,
        CapabilityClaim::root(write_atom, CapabilityMode::InvokeAndDelegate, None),
        intermediary.verifying_key(),
    )
    .unwrap();
    let parent = parent_bundle
        .verify(
            root.verifying_key(),
            Epoch::from_tai_seconds(0.0),
            intermediary.verifying_key(),
            CapabilityRequest::new(write_atom, CapabilityMode::InvokeAndDelegate),
        )
        .unwrap();
    let child_bundle = parent
        .delegate(
            &intermediary,
            CapabilityClaim::delegated(
                parent.claim_handle(),
                write_atom,
                CapabilityMode::Invoke,
                None,
            ),
            leaf.verifying_key(),
        )
        .unwrap();
    store_bundle(&mut store, parent_bundle);
    store_bundle(&mut store, child_bundle);

    let snapshot = store.snapshot().unwrap();
    let instant = Epoch::from_tai_seconds(0.0);
    assert!(collection
        .writer_is_admitted_at(&snapshot, intermediary.verifying_key(), instant)
        .unwrap());
    assert!(!collection
        .writer_is_admitted_at(&snapshot, leaf.verifying_key(), instant)
        .unwrap());
}

#[test]
fn read_grants_use_the_distinct_read_action() {
    let root = key(8);
    let reader = key(9);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection(
            "read-action",
            CollectionPolicy::new(
                AdmissionPolicy::direct(root.verifying_key()),
                AdmissionPolicy::direct(root.verifying_key()),
            ),
        )
        .unwrap();
    let read_atom = atom(ACTION_READ, collection);
    store_bundle(
        &mut store,
        CapabilityProofBundle::issue_root(
            &root,
            CapabilityClaim::root(read_atom, CapabilityMode::Invoke, None),
            reader.verifying_key(),
        )
        .unwrap(),
    );

    let snapshot = store.snapshot().unwrap();
    let instant = Epoch::from_tai_seconds(0.0);
    assert!(collection
        .reader_is_admitted_at(&snapshot, reader.verifying_key(), instant)
        .unwrap());
    assert!(!collection
        .writer_is_admitted_at(&snapshot, reader.verifying_key(), instant)
        .unwrap());
}

#[test]
fn read_audience_uses_complete_snapshot_closure_and_includes_proof_prefixes() {
    let root = key(83);
    let intermediary = key(84);
    let leaf = key(85);
    let incomplete = key(86);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection(
            "read-audience",
            CollectionPolicy::new(
                AdmissionPolicy::delegable(root.verifying_key()),
                AdmissionPolicy::Open,
            ),
        )
        .unwrap();
    let read_atom = atom(ACTION_READ, collection);
    let parent_bundle = CapabilityProofBundle::issue_root(
        &root,
        CapabilityClaim::root(read_atom, CapabilityMode::InvokeAndDelegate, None),
        intermediary.verifying_key(),
    )
    .unwrap();
    let parent = parent_bundle
        .verify(
            root.verifying_key(),
            Epoch::from_tai_seconds(0.0),
            intermediary.verifying_key(),
            CapabilityRequest::new(read_atom, CapabilityMode::InvokeAndDelegate),
        )
        .unwrap();
    let child_bundle = parent
        .delegate(
            &intermediary,
            CapabilityClaim::delegated(
                parent.claim_handle(),
                read_atom,
                CapabilityMode::Invoke,
                None,
            ),
            leaf.verifying_key(),
        )
        .unwrap();
    store_bundle(&mut store, child_bundle);

    let incomplete_proof = CapabilityProofBundle::issue_root(
        &root,
        CapabilityClaim::root(read_atom, CapabilityMode::Invoke, None),
        incomplete.verifying_key(),
    )
    .unwrap()
    .proof()
    .clone();
    store.insert_proof(incomplete_proof).unwrap();

    let CollectionReadAudience::Restricted(readers) = collection_read_audience_at(
        &store.snapshot().unwrap(),
        collection.handle(),
        Epoch::from_tai_seconds(0.0),
    )
    .unwrap() else {
        panic!("delegable READ policy must have a finite audience");
    };
    assert!(readers.contains(&root.verifying_key()));
    assert!(readers.contains(&intermediary.verifying_key()));
    assert!(readers.contains(&leaf.verifying_key()));
    assert!(!readers.contains(&incomplete.verifying_key()));
}

#[test]
fn collection_quorum_needs_support_from_distinct_roots() {
    let first_root = key(10);
    let second_root = key(11);
    let writer = key(12);
    let mut store = MemoryRepo::default();
    let collection = store
        .collection(
            "two-root-write-quorum",
            CollectionPolicy::new(
                AdmissionPolicy::Open,
                AdmissionPolicy::quorum(
                    [first_root.verifying_key(), second_root.verifying_key()],
                    2,
                    None,
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let write_atom = atom(ACTION_WRITE, collection);
    let instant = Epoch::from_tai_seconds(0.0);

    store_bundle(
        &mut store,
        CapabilityProofBundle::issue_root(
            &first_root,
            CapabilityClaim::root(write_atom, CapabilityMode::Invoke, None),
            writer.verifying_key(),
        )
        .unwrap(),
    );
    assert!(!collection
        .writer_is_admitted_at(&store.snapshot().unwrap(), writer.verifying_key(), instant)
        .unwrap());

    store_bundle(
        &mut store,
        CapabilityProofBundle::issue_root(
            &second_root,
            CapabilityClaim::root(write_atom, CapabilityMode::Invoke, None),
            writer.verifying_key(),
        )
        .unwrap(),
    );
    assert!(collection
        .writer_is_admitted_at(&store.snapshot().unwrap(), writer.verifying_key(), instant)
        .unwrap());
}
