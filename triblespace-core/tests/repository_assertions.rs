use ed25519_dalek::SigningKey;
use std::fs::File;

use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{BlobEncoding, IntoBlob};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, InlineEncoding};
use triblespace_core::repo::branch_assertion::{
    BranchAssertion, BranchAssertionSnapshot, BranchAssertionStore,
};
use triblespace_core::repo::branch_frontier::{BranchResolution, ResolvedHead};
use triblespace_core::repo::commit;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::{
    AssertionPullError, BlobStore, BlobStorePut, PublishError, PublishOutcome, Repository,
    StorageFlush,
};
use triblespace_core::trible::TribleSet;

fn key(byte: u8) -> SigningKey {
    SigningKey::from_bytes(&[byte; 32])
}

fn repository(byte: u8) -> Repository<MemoryRepo> {
    Repository::new(MemoryRepo::default(), key(byte), TribleSet::new()).unwrap()
}

fn absent_commit(byte: u8) -> Inline<Handle<SimpleArchive>> {
    Inline::new([byte; 32])
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct StoreCalls {
    puts: usize,
    readers: usize,
    snapshots: usize,
    flushes: usize,
    appends: usize,
}

#[derive(Debug, Default)]
struct ProbeStore {
    inner: MemoryRepo,
    calls: StoreCalls,
}

impl BlobStorePut for ProbeStore {
    type PutError = <MemoryRepo as BlobStorePut>::PutError;

    fn put<S, T>(&mut self, item: T) -> Result<Inline<Handle<S>>, Self::PutError>
    where
        S: BlobEncoding + 'static,
        T: IntoBlob<S>,
        Handle<S>: InlineEncoding,
    {
        self.calls.puts += 1;
        self.inner.put(item)
    }
}

impl BlobStore for ProbeStore {
    type Reader = <MemoryRepo as BlobStore>::Reader;
    type ReaderError = <MemoryRepo as BlobStore>::ReaderError;

    fn reader(&mut self) -> Result<Self::Reader, Self::ReaderError> {
        self.calls.readers += 1;
        self.inner.reader()
    }
}

impl StorageFlush for ProbeStore {
    type Error = <MemoryRepo as StorageFlush>::Error;

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.calls.flushes += 1;
        self.inner.flush()
    }
}

impl BranchAssertionStore for ProbeStore {
    type Error = <MemoryRepo as BranchAssertionStore>::Error;

    fn assertion_snapshot(&mut self) -> Result<BranchAssertionSnapshot, Self::Error> {
        self.calls.snapshots += 1;
        self.inner.assertion_snapshot()
    }

    fn append_assertion(&mut self, assertion: BranchAssertion) -> Result<(), Self::Error> {
        self.calls.appends += 1;
        self.inner.append_assertion(assertion)
    }
}

#[test]
fn empty_workspace_has_no_assertion() {
    let mut repo = repository(7);
    let identity = repo.branch_identity("main");
    let mut workspace = repo.create_workspace("main").unwrap();

    assert!(matches!(
        repo.resolve(&identity).unwrap(),
        BranchResolution::Absent
    ));
    assert_eq!(repo.push(&mut workspace).unwrap(), PublishOutcome::NoChange);
    assert!(repo.storage_mut().assertion_snapshot().unwrap().is_empty());
}

#[test]
fn first_publish_and_repeat_are_exactly_once() {
    let mut repo = repository(7);
    let mut workspace = repo.create_workspace("main").unwrap();
    let identity = *workspace.identity();
    workspace.commit(TribleSet::new(), "first");
    let proposed = workspace.head().unwrap();

    let published = repo.push(&mut workspace).unwrap();
    let PublishOutcome::Published(assertion_id) = published else {
        panic!("a changed workspace must publish an assertion");
    };
    let snapshot = repo.storage_mut().assertion_snapshot().unwrap();
    let assertions = snapshot.for_branch(&identity);
    assert_eq!(assertions.len(), 1);
    assert_eq!(assertions[0].commit(), proposed);
    assert_eq!(assertions[0].id(), assertion_id);

    assert_eq!(repo.push(&mut workspace).unwrap(), PublishOutcome::NoChange);
    assert_eq!(
        repo.storage_mut()
            .assertion_snapshot()
            .unwrap()
            .for_branch(&identity)
            .len(),
        1
    );
}

#[test]
fn stale_workspaces_publish_a_divergent_complete_frontier() {
    let mut repo = repository(7);
    let mut left = repo.create_workspace("main").unwrap();
    let mut right = repo.create_workspace("main").unwrap();
    let identity = *left.identity();
    left.commit(TribleSet::new(), "left");
    right.commit(TribleSet::new(), "right");
    let mut expected = vec![left.head().unwrap(), right.head().unwrap()];
    expected.sort_unstable_by_key(|commit| commit.raw);

    assert!(matches!(
        repo.push(&mut left).unwrap(),
        PublishOutcome::Published(_)
    ));
    assert!(matches!(
        repo.push(&mut right).unwrap(),
        PublishOutcome::Published(_)
    ));

    let BranchResolution::Complete(frontier) = repo.resolve(&identity).unwrap() else {
        panic!("fully resident incomparable tips must form a complete frontier");
    };
    assert_eq!(frontier.tips(), expected);
    assert_eq!(
        repo.storage_mut()
            .assertion_snapshot()
            .unwrap()
            .for_branch(&identity)
            .len(),
        2
    );
}

#[test]
fn pull_stages_the_flat_synthetic_merge_without_asserting_it() {
    let mut repo = repository(7);
    let mut left = repo.create_workspace("main").unwrap();
    let mut right = repo.create_workspace("main").unwrap();
    let identity = *left.identity();
    left.commit(TribleSet::new(), "left");
    right.commit(TribleSet::new(), "right");
    let mut parents = vec![left.head().unwrap(), right.head().unwrap()];
    parents.sort_unstable_by_key(|commit| commit.raw);
    repo.push(&mut left).unwrap();
    repo.push(&mut right).unwrap();

    let BranchResolution::Complete(frontier) = repo.resolve(&identity).unwrap() else {
        panic!("resident tips must resolve completely");
    };
    assert!(matches!(
        frontier.resolved_head(),
        ResolvedHead::Synthetic(_)
    ));

    let mut pulled = repo.pull(identity).unwrap();
    let merge = pulled.head().unwrap();
    let merge_meta: TribleSet = pulled.get(merge).unwrap();
    assert_eq!(commit::direct_parents(&merge_meta).unwrap(), parents);
    assert_eq!(repo.push(&mut pulled).unwrap(), PublishOutcome::NoChange);
    assert_eq!(
        repo.storage_mut()
            .assertion_snapshot()
            .unwrap()
            .for_branch(&identity)
            .len(),
        2,
        "the derived read view is not itself replicated state"
    );
}

#[test]
fn a_missing_singleton_tip_is_pending_and_not_writable() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let missing = absent_commit(91);
    repo.storage_mut()
        .append_assertion(BranchAssertion::sign(
            &signing_key,
            identity.name(),
            missing,
        ))
        .unwrap();

    let BranchResolution::TipPending(frontier) = repo.resolve(&identity).unwrap() else {
        panic!("an unreadable asserted tip must stay pending");
    };
    assert_eq!(frontier.tips(), &[missing]);
    assert_eq!(frontier.missing_tips(), &[missing]);
    assert!(matches!(
        repo.pull(identity),
        Err(AssertionPullError::TipPending(_))
    ));
}

#[test]
fn missing_ancestry_is_partial_and_not_writable() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let missing_parent = absent_commit(83);
    let left_meta = commit::commit_metadata(
        &signing_key,
        [missing_parent],
        None,
        Some(TribleSet::new().to_blob()),
        None,
    );
    let right_meta = commit::commit_metadata(
        &signing_key,
        [],
        None,
        Some(TribleSet::new().to_blob()),
        None,
    );
    let left = repo.storage_mut().put(left_meta).unwrap();
    let right = repo.storage_mut().put(right_meta).unwrap();
    for tip in [left, right] {
        repo.storage_mut()
            .append_assertion(BranchAssertion::sign(&signing_key, identity.name(), tip))
            .unwrap();
    }

    let BranchResolution::Partial(frontier) = repo.resolve(&identity).unwrap() else {
        panic!("an undecidable relation through missing ancestry must stay partial");
    };
    assert!(frontier.missing_ancestry().contains(&missing_parent));
    assert!(matches!(
        frontier.candidate_root(),
        ResolvedHead::Synthetic(_)
    ));
    assert!(matches!(
        repo.pull(identity),
        Err(AssertionPullError::Partial(_))
    ));
}

#[test]
fn absent_and_malformed_proposed_tips_never_append() {
    let mut absent_repo = repository(7);
    let mut absent = absent_repo.create_workspace("main").unwrap();
    absent.set_head(absent_commit(77));
    assert!(matches!(
        absent_repo.push(&mut absent),
        Err(PublishError::StorageGet(_))
    ));
    assert!(absent_repo
        .storage_mut()
        .assertion_snapshot()
        .unwrap()
        .is_empty());

    let mut malformed_repo = repository(7);
    let mut malformed = malformed_repo.create_workspace("main").unwrap();
    let malformed_tip = malformed.put::<SimpleArchive, _>(TribleSet::new());
    malformed.set_head(malformed_tip);
    assert!(matches!(
        malformed_repo.push(&mut malformed),
        Err(PublishError::BadCommitMetadata(_))
    ));
    assert!(malformed_repo
        .storage_mut()
        .assertion_snapshot()
        .unwrap()
        .is_empty());
}

#[test]
fn foreign_identity_is_refused_and_same_name_assertions_do_not_contaminate() {
    let mut local = repository(7);
    let own = local.branch_identity("main");
    let foreign_key = key(11);
    let mut foreign =
        Repository::new(MemoryRepo::default(), foreign_key.clone(), TribleSet::new()).unwrap();
    let mut foreign_workspace = foreign.create_workspace("main").unwrap();
    let foreign_identity = *foreign_workspace.identity();
    foreign_workspace.commit(TribleSet::new(), "foreign");

    assert!(matches!(
        local.resolve(&foreign_identity),
        Err(triblespace_core::repo::ResolveBranchError::ForeignIdentity(
            _
        ))
    ));
    assert!(matches!(
        local.pull(foreign_identity),
        Err(AssertionPullError::ForeignIdentity(_))
    ));
    assert!(matches!(
        local.push(&mut foreign_workspace),
        Err(PublishError::ForeignIdentity(_))
    ));
    assert!(local.storage_mut().assertion_snapshot().unwrap().is_empty());

    local
        .storage_mut()
        .append_assertion(BranchAssertion::sign(
            &foreign_key,
            foreign_identity.name(),
            absent_commit(99),
        ))
        .unwrap();
    assert!(matches!(
        local.resolve(&own).unwrap(),
        BranchResolution::Absent
    ));
}

#[test]
fn foreign_identity_is_rejected_before_any_store_operation() {
    let mut local = Repository::new(ProbeStore::default(), key(7), TribleSet::new()).unwrap();
    let mut foreign = Repository::new(ProbeStore::default(), key(11), TribleSet::new()).unwrap();
    let mut foreign_workspace = foreign.create_workspace("main").unwrap();
    foreign_workspace.commit(TribleSet::new(), "foreign");
    let foreign_identity = *foreign_workspace.identity();

    local.storage_mut().calls = StoreCalls::default();
    assert!(matches!(
        local.resolve(&foreign_identity),
        Err(triblespace_core::repo::ResolveBranchError::ForeignIdentity(
            _
        ))
    ));
    assert!(matches!(
        local.pull(foreign_identity),
        Err(AssertionPullError::ForeignIdentity(_))
    ));
    assert!(matches!(
        local.push(&mut foreign_workspace),
        Err(PublishError::ForeignIdentity(_))
    ));
    assert_eq!(
        local.storage().calls,
        StoreCalls::default(),
        "foreign refusal must precede reads, uploads, flushes, snapshots, and appends"
    );
}

#[test]
fn memory_and_pile_publish_the_same_assertion() {
    let signing_key = key(7);
    let content_blob = TribleSet::new().to_blob();
    let commit_meta =
        commit::commit_metadata(&signing_key, [], None, Some(content_blob.clone()), None);

    let mut memory =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let pile_path = directory.path().join("repository.pile");
    File::create(&pile_path).unwrap();
    let mut pile = Repository::new(
        Pile::open(&pile_path).unwrap(),
        signing_key,
        TribleSet::new(),
    )
    .unwrap();

    let mut memory_workspace = memory.create_workspace("main").unwrap();
    let mut pile_workspace = pile.create_workspace("main").unwrap();
    let _memory_content = memory_workspace.put::<SimpleArchive, _>(content_blob.clone());
    let memory_proposed = memory_workspace.put::<SimpleArchive, _>(commit_meta.clone());
    memory_workspace.set_head(memory_proposed);
    let _pile_content = pile_workspace.put::<SimpleArchive, _>(content_blob);
    let pile_proposed = pile_workspace.put::<SimpleArchive, _>(commit_meta);
    pile_workspace.set_head(pile_proposed);
    let identity = *memory_workspace.identity();
    assert_eq!(identity, *pile_workspace.identity());
    assert_eq!(memory_workspace.head(), pile_workspace.head());

    let memory_outcome = memory.push(&mut memory_workspace).unwrap();
    let pile_outcome = pile.push(&mut pile_workspace).unwrap();
    assert_eq!(memory_outcome, pile_outcome);

    let memory_assertion = memory
        .storage_mut()
        .assertion_snapshot()
        .unwrap()
        .for_branch(&identity)[0];
    let pile_assertion = pile
        .storage_mut()
        .assertion_snapshot()
        .unwrap()
        .for_branch(&identity)[0];
    assert_eq!(memory_assertion.encode(), pile_assertion.encode());
    assert_eq!(
        memory.resolve(&identity).unwrap(),
        pile.resolve(&identity).unwrap()
    );
    assert_eq!(
        memory.pull(identity).unwrap().head(),
        pile.pull(identity).unwrap().head()
    );

    pile.into_storage().close().unwrap();
}
