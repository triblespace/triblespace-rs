use ed25519_dalek::SigningKey;
use futures::executor::block_on;
use hifitime::Epoch;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, Rank9AcceleratedSuccinctArchiveBlob, SuccinctArchiveBlob, UnionArchive,
};
use triblespace_core::blob::{Blob, IntoBlob};
use triblespace_core::capability::{
    Capability, CapabilityAction, CapabilityMode, CapabilityProof, CapabilityResource,
    CapabilityValidity,
};
use triblespace_core::collection::succinctarchive_union;
use triblespace_core::collection::{
    AdmissionPolicy, CollectionCommit, CollectionPolicy, CollectionRead, CollectionRecord,
    CollectionSnapshotExt, CollectionStore, CollectionStoreExt, ACTION_WRITE,
};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::{
    BlobStorePut, CapabilityProofRead, CapabilityProofStore, SnapshotSource,
};
use triblespace_core::trible::{Fragment, Trible, TribleSet, TRIBLE_LEN};

fn one_fact(seed: u8) -> TribleSet {
    let mut row = [seed; TRIBLE_LEN];
    row[16..32].fill(seed.wrapping_add(1));
    row[32..].fill(seed.wrapping_add(2));
    let mut facts = TribleSet::new();
    facts.insert(&Trible::force_raw(row).unwrap());
    facts
}

fn write_capability() -> Capability {
    Capability::new(CapabilityAction::new(ACTION_WRITE), CapabilityMode::Invoke)
}

#[test]
fn simplearchive_collection_round_trips_typed_views() {
    let authority = SigningKey::from_bytes(&[41; 32]);
    let policy = CollectionPolicy::new(
        AdmissionPolicy::direct(authority.verifying_key()),
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let expected = one_fact(7);
    let expected_member = expected.clone().to_blob().get_handle();
    let mut store = MemoryRepo::default();

    let collection = store.collection("typed-api", policy).unwrap();

    let commit = store
        .commit(collection, &authority, Fragment::from(expected.clone()))
        .unwrap();
    assert_eq!(
        Handle::<SimpleArchive>::from_hash(commit.data()),
        expected_member
    );

    let snapshot = store.snapshot().unwrap();
    let cover = collection
        .admitted_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(cover.collection(), collection);
    assert_eq!(cover.members().collect::<Vec<_>>(), vec![expected_member]);

    let materialized: TribleSet = collection
        .read_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(materialized, expected);
}

#[test]
fn succinct_cover_materializes_as_a_typed_union_archive() {
    let authority = SigningKey::from_bytes(&[42; 32]);
    let expected = one_fact(11);
    let source_blob: Blob<SimpleArchive> = expected.clone().to_blob();
    let raw = succinctarchive_union::derive_element(&source_blob).unwrap();
    let raw_handle = raw.get_handle();
    let mut store = MemoryRepo::default();

    let source_policy = CollectionPolicy::new(
        AdmissionPolicy::direct(authority.verifying_key()),
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let target_policy = source_policy.clone();
    let source = store.collection("typed-api-source", source_policy).unwrap();
    let target = store
        .derive::<SuccinctArchiveBlob>(source, (), target_policy)
        .unwrap();

    store
        .commit(source, &authority, Fragment::from(expected.clone()))
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    let source_cover = source
        .admitted_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    let ensured = block_on(store.ensure(target)).unwrap();
    let collection = ensured.collection_exact(target, &source_cover).unwrap();

    // Later source growth cannot silently change the support paired with the
    // completed target realization.
    store
        .commit(source, &authority, Fragment::from(one_fact(12)))
        .unwrap();
    assert_eq!(collection.support(), &source_cover);
    let cover = collection.cover();
    assert_eq!(cover.collection(), target);
    assert_eq!(cover.members().collect::<Vec<_>>(), vec![raw_handle]);

    let materialized = collection.view::<UnionArchive<OrderedUniverse>>().unwrap();
    assert_eq!(materialized.segment_count(), 1);
    assert_eq!(materialized.iter().collect::<TribleSet>(), expected);

    // The explicit-support ensure and admitted-support maintenance paths share
    // the same immutable snapshot result shape.
    block_on(store.ensure_exact(target, &source_cover)).unwrap();
    block_on(store.maintain(target)).unwrap();
    let maintained = block_on(store.maintain_exact(target, &source_cover)).unwrap();
    let collection = maintained.collection_exact(target, &source_cover).unwrap();
    assert_eq!(collection.support(), &source_cover);
    assert_eq!(
        collection.cover().members().collect::<Vec<_>>(),
        vec![raw_handle]
    );
}

#[test]
fn exact_apis_accept_a_derived_source_encoding() {
    let authority = SigningKey::from_bytes(&[43; 32]);
    let expected = one_fact(13);
    let policy = CollectionPolicy::new(
        AdmissionPolicy::direct(authority.verifying_key()),
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let mut store = MemoryRepo::default();

    let source = store
        .collection("typed-api-exact-source", policy.clone())
        .unwrap();
    let raw = store
        .derive::<SuccinctArchiveBlob>(source, (), policy.clone())
        .unwrap();
    let accelerated = store
        .derive::<Rank9AcceleratedSuccinctArchiveBlob>(raw, (), policy)
        .unwrap();
    store
        .commit(source, &authority, Fragment::from(expected.clone()))
        .unwrap();

    let support = source
        .admitted_at(&store.snapshot().unwrap(), Epoch::from_tai_seconds(0.0))
        .unwrap();
    block_on(store.ensure_exact(raw, &support)).unwrap();
    let ensured = block_on(store.ensure_exact(accelerated, &support)).unwrap();
    let observed = ensured.collection_exact(accelerated, &support).unwrap();
    assert_eq!(observed.support(), &support);
    assert_eq!(observed.cover().len(), 1);

    let maintained = block_on(store.maintain_exact(accelerated, &support)).unwrap();
    let materialized = maintained
        .collection_exact(accelerated, &support)
        .unwrap()
        .view::<UnionArchive<OrderedUniverse>>()
        .unwrap();
    assert_eq!(materialized.iter().collect::<TribleSet>(), expected);
}

#[test]
fn collection_at_uses_the_supplied_authorization_instant() {
    let authority = SigningKey::from_bytes(&[44; 32]);
    let writer = SigningKey::from_bytes(&[45; 32]);
    let policy = CollectionPolicy::new(
        AdmissionPolicy::direct(authority.verifying_key()),
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let expected = one_fact(14);
    let expected_member = expected.clone().to_blob().get_handle();
    let mut store = MemoryRepo::default();
    let collection = store.collection("typed-api-clock", policy).unwrap();
    store
        .commit(collection, &writer, Fragment::from(expected))
        .unwrap();

    let validity =
        CapabilityValidity::new(Epoch::from_tai_seconds(10.0), Epoch::from_tai_seconds(20.0))
            .unwrap();
    store
        .insert_proof(CapabilityProof::issue_root(
            &authority,
            CapabilityResource::from(collection.handle()),
            write_capability(),
            Some(validity),
            writer.verifying_key(),
        ))
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    assert!(snapshot
        .collection_at(collection, Epoch::from_tai_seconds(9.0))
        .unwrap()
        .support()
        .is_empty());

    let admitted = snapshot
        .collection_at(collection, Epoch::from_tai_seconds(15.0))
        .unwrap();
    assert_eq!(
        admitted.support().members().collect::<Vec<_>>(),
        vec![expected_member]
    );
    assert_eq!(
        admitted.cover().members().collect::<Vec<_>>(),
        vec![expected_member]
    );

    assert!(snapshot
        .collection_at(collection, Epoch::from_tai_seconds(21.0))
        .unwrap()
        .support()
        .is_empty());
}

#[test]
fn collection_at_returns_the_maximal_resident_partial_realization() {
    let authority = SigningKey::from_bytes(&[46; 32]);
    let policy = CollectionPolicy::new(
        AdmissionPolicy::direct(authority.verifying_key()),
        AdmissionPolicy::direct(authority.verifying_key()),
    );
    let first = one_fact(15);
    let second = one_fact(16);
    let first_member = first.clone().to_blob().get_handle();
    let second_member = second.clone().to_blob().get_handle();
    let mut store = MemoryRepo::default();
    let source = store
        .collection("typed-api-partial-source", policy.clone())
        .unwrap();
    let target = store
        .derive::<SuccinctArchiveBlob>(source, (), policy)
        .unwrap();

    store
        .commit(source, &authority, Fragment::from(first.clone()))
        .unwrap();
    let first_support = source
        .admitted_at(&store.snapshot().unwrap(), Epoch::from_tai_seconds(0.0))
        .unwrap();
    block_on(store.ensure_exact(target, &first_support)).unwrap();
    store
        .commit(source, &authority, Fragment::from(second))
        .unwrap();

    let snapshot = store.snapshot().unwrap();
    let admitted_source = source
        .admitted_at(&snapshot, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(admitted_source.len(), 2);
    assert!(admitted_source.contains(first_member));
    assert!(admitted_source.contains(second_member));

    let observed = snapshot
        .collection_at(target, Epoch::from_tai_seconds(0.0))
        .unwrap();
    assert_eq!(observed.support(), &first_support);
    assert_eq!(observed.cover().len(), 1);
    assert_eq!(
        observed
            .view::<UnionArchive<OrderedUniverse>>()
            .unwrap()
            .iter()
            .collect::<TribleSet>(),
        first
    );
}

#[test]
fn dangling_commit_is_raw_but_semantically_visible_only_in_a_later_snapshot() {
    let authority = SigningKey::from_bytes(&[47; 32]);
    let policy = CollectionPolicy::new(AdmissionPolicy::Open, AdmissionPolicy::Open);
    let payload = one_fact(17).to_blob();
    let payload_handle = payload.get_handle();
    let mut store = MemoryRepo::default();
    let collection = store.collection("typed-api-dangling", policy).unwrap();
    let metadata = store
        .put::<SimpleArchive, _>(TribleSet::new().to_blob())
        .unwrap();
    let commit = CollectionCommit::sign(
        &authority,
        collection.handle(),
        Handle::<SimpleArchive>::to_hash(payload_handle),
        metadata,
    );
    store.insert(CollectionRecord::Commit(commit)).unwrap();

    let before = store.snapshot().unwrap();
    assert_eq!(
        before
            .records()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![CollectionRecord::Commit(commit)]
    );
    assert!(collection
        .admitted_at(&before, Epoch::from_tai_seconds(0.0))
        .unwrap()
        .is_empty());

    store.put::<SimpleArchive, _>(payload).unwrap();
    let after = store.snapshot().unwrap();
    assert_eq!(
        collection
            .admitted_at(&after, Epoch::from_tai_seconds(0.0))
            .unwrap()
            .members()
            .collect::<Vec<_>>(),
        vec![payload_handle]
    );
    assert!(collection
        .admitted_at(&before, Epoch::from_tai_seconds(0.0))
        .unwrap()
        .is_empty());
}

#[test]
fn self_contained_capability_proof_activates_commit_without_blob_closure() {
    let root = SigningKey::from_bytes(&[48; 32]);
    let writer = SigningKey::from_bytes(&[49; 32]);
    let policy = CollectionPolicy::new(
        AdmissionPolicy::Open,
        AdmissionPolicy::direct(root.verifying_key()),
    );
    let expected = one_fact(18);
    let expected_member = expected.clone().to_blob().get_handle();
    let mut store = MemoryRepo::default();
    let collection = store
        .collection("typed-api-dangling-proof", policy)
        .unwrap();
    store
        .commit(collection, &writer, Fragment::from(expected))
        .unwrap();

    let before = store.snapshot().unwrap();
    assert!(collection
        .admitted_at(&before, Epoch::from_tai_seconds(0.0))
        .unwrap()
        .is_empty());

    let proof = CapabilityProof::issue_root(
        &root,
        CapabilityResource::from(collection.handle()),
        write_capability(),
        None,
        writer.verifying_key(),
    );
    store.insert_proof(proof.clone()).unwrap();

    let after = store.snapshot().unwrap();
    assert_eq!(
        after
            .proofs()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![proof]
    );
    assert_eq!(
        collection
            .admitted_at(&after, Epoch::from_tai_seconds(0.0))
            .unwrap()
            .members()
            .collect::<Vec<_>>(),
        vec![expected_member]
    );
    assert!(collection
        .admitted_at(&before, Epoch::from_tai_seconds(0.0))
        .unwrap()
        .is_empty());
}
