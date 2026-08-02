use ed25519_dalek::SigningKey;
use std::fs::File;

use triblespace_core::blob::encodings::longstring::LongString;
use triblespace_core::blob::encodings::simplearchive::SimpleArchive;
use triblespace_core::blob::{BlobEncoding, IntoBlob, MemoryBlobStore};
use triblespace_core::inline::encodings::hash::Handle;
use triblespace_core::inline::{Inline, InlineEncoding};
use triblespace_core::repo::branch_frontier::{BranchResolution, ResolvedHead};
use triblespace_core::repo::branch_pin::{
    sign_branch_assertion, BranchIdentity, BranchPinDescriptor, BranchRank,
};
use triblespace_core::repo::commit;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::pile::Pile;
use triblespace_core::repo::pin_assertion::{
    PinAssertion, PinAssertionSnapshot, PinAssertionStore, PinIdentity, SubsumptionLabel,
};
use triblespace_core::repo::{
    AssertionPullError, BlobStore, BlobStoreGet, BlobStorePut, CommitHandle, PublishError,
    PublishOutcome, Repository, StorageFlush, Workspace,
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

fn pin_identity(identity: &BranchIdentity) -> PinIdentity {
    BranchPinDescriptor::pin_identity(identity.author(), identity.name())
}

fn assertions_for_branch(
    snapshot: &PinAssertionSnapshot,
    identity: &BranchIdentity,
) -> Vec<PinAssertion> {
    snapshot.for_pin(&pin_identity(identity))
}

fn asserted_commit(assertion: &PinAssertion) -> CommitHandle {
    Inline::new(assertion.value().raw())
}

fn rank(byte: u8) -> BranchRank {
    let mut raw = [0u8; 32];
    raw[31] = byte;
    BranchRank::from_label(SubsumptionLabel::from_raw(raw))
}

fn staged_parent_child() -> (Workspace<MemoryRepo>, CommitHandle, CommitHandle) {
    let signing_key = key(7);
    let mut repo = repository(7);
    let mut workspace = repo.create_workspace("main").unwrap();
    let parent = workspace.put(commit::commit_metadata(&signing_key, [], None, None, None));
    let child = workspace.put(commit::commit_metadata(
        &signing_key,
        [parent],
        None,
        None,
        None,
    ));
    (workspace, parent, child)
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

impl PinAssertionStore for ProbeStore {
    type Error = <MemoryRepo as PinAssertionStore>::Error;

    fn pin_assertion_snapshot(&mut self) -> Result<PinAssertionSnapshot, Self::Error> {
        self.calls.snapshots += 1;
        self.inner.pin_assertion_snapshot()
    }

    fn append_pin_assertion(&mut self, assertion: PinAssertion) -> Result<(), Self::Error> {
        self.calls.appends += 1;
        self.inner.append_pin_assertion(assertion)
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
    assert!(repo
        .storage_mut()
        .pin_assertion_snapshot()
        .unwrap()
        .is_empty());
}

#[test]
fn first_publish_and_repeat_are_exactly_once() {
    let mut repo = repository(7);
    let mut workspace = repo.create_workspace("main").unwrap();
    let identity = *workspace.identity();
    workspace.commit(TribleSet::new(), "first").unwrap();
    let proposed = workspace.head().unwrap();
    assert_eq!(workspace.head_rank(), Some(BranchRank::ROOT));

    let published = repo.push(&mut workspace).unwrap();
    let PublishOutcome::Published(assertion_id) = published else {
        panic!("a changed workspace must publish an assertion");
    };
    let snapshot = repo.storage_mut().pin_assertion_snapshot().unwrap();
    let assertions = assertions_for_branch(&snapshot, &identity);
    assert_eq!(assertions.len(), 1);
    assert_eq!(asserted_commit(&assertions[0]), proposed);
    assert_eq!(assertions[0].label(), BranchRank::ROOT.label());
    assert_eq!(assertions[0].id(), assertion_id);

    assert_eq!(repo.push(&mut workspace).unwrap(), PublishOutcome::NoChange);
    assert_eq!(
        repo.storage_mut()
            .pin_assertion_snapshot()
            .unwrap()
            .for_pin(&pin_identity(&identity))
            .len(),
        1
    );
}

#[test]
fn ordinary_authored_commits_advance_the_inductive_rank() {
    let mut repo = repository(7);
    let mut workspace = repo.create_workspace("main").unwrap();
    let identity = *workspace.identity();

    workspace.commit(TribleSet::new(), "root").unwrap();
    assert_eq!(workspace.head_rank(), Some(BranchRank::ROOT));
    repo.push(&mut workspace).unwrap();

    let child_rank = BranchRank::ROOT.successor().unwrap();
    workspace.commit(TribleSet::new(), "child").unwrap();
    assert_eq!(workspace.head_rank(), Some(child_rank));
    repo.push(&mut workspace).unwrap();

    let snapshot = repo.storage_mut().pin_assertion_snapshot().unwrap();
    let assertions = assertions_for_branch(&snapshot, &identity);
    assert_eq!(assertions.len(), 2);
    assert!(assertions
        .iter()
        .any(|assertion| assertion.label() == BranchRank::ROOT.label()));
    assert!(assertions
        .iter()
        .any(|assertion| assertion.label() == child_rank.label()));
}

#[test]
fn stale_workspaces_publish_a_divergent_complete_frontier() {
    let mut repo = repository(7);
    let mut left = repo.create_workspace("main").unwrap();
    let mut right = repo.create_workspace("main").unwrap();
    let identity = *left.identity();
    left.commit(TribleSet::new(), "left").unwrap();
    right.commit(TribleSet::new(), "right").unwrap();
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
            .pin_assertion_snapshot()
            .unwrap()
            .for_pin(&pin_identity(&identity))
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
    left.commit(TribleSet::new(), "left").unwrap();
    right.commit(TribleSet::new(), "right").unwrap();
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
    assert_eq!(
        pulled.head_rank(),
        BranchRank::after([BranchRank::ROOT, BranchRank::ROOT])
    );
    let merge_meta: TribleSet = pulled.get(merge).unwrap();
    assert_eq!(commit::direct_parents(&merge_meta).unwrap(), parents);
    assert_eq!(repo.push(&mut pulled).unwrap(), PublishOutcome::NoChange);
    assert_eq!(
        repo.storage_mut()
            .pin_assertion_snapshot()
            .unwrap()
            .for_pin(&pin_identity(&identity))
            .len(),
        2,
        "the derived read view is not itself replicated state"
    );
}

#[test]
fn pull_repairs_an_assertion_first_descriptor_arrival() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let tip_meta = commit::commit_metadata(
        &signing_key,
        [],
        None,
        Some(TribleSet::new().to_blob()),
        None,
    );
    let tip = repo.storage_mut().put(tip_meta).unwrap();
    repo.storage_mut()
        .append_pin_assertion(sign_branch_assertion(
            &signing_key,
            identity.name(),
            tip,
            BranchRank::ROOT,
        ))
        .unwrap();

    // Deliberately do not put the descriptor: replicated generic records are
    // allowed to arrive before their typed content.
    let descriptor = BranchPinDescriptor::blob(identity.name());
    let descriptor_handle = descriptor.get_handle();
    let mut pulled = repo.pull(identity).unwrap();
    let decoded: Inline<Handle<LongString>> = pulled.get(descriptor_handle).unwrap();
    assert_eq!(decoded, identity.name());

    // Publication reconstructs the descriptor too; constructor staging is a
    // convenience, not the crash-order invariant.
    pulled.staged = MemoryBlobStore::new();
    assert_eq!(repo.push(&mut pulled).unwrap(), PublishOutcome::NoChange);
    let reader = repo.storage_mut().reader().unwrap();
    let decoded: Inline<Handle<LongString>> = reader.get(descriptor_handle).unwrap();
    assert_eq!(decoded, identity.name());
}

#[test]
fn strict_fast_forward_repairs_only_stale_rank_provenance() {
    let (mut stale, parent, child) = staged_parent_child();
    stale.set_head(parent, rank(5)).unwrap();
    assert_eq!(stale.merge_commit(child, rank(1)).unwrap(), child);
    assert_eq!(stale.head(), Some(child));
    assert_eq!(stale.head_rank(), Some(rank(6)));

    let (mut monotone, parent, child) = staged_parent_child();
    monotone.set_head(parent, rank(5)).unwrap();
    assert_eq!(monotone.merge_commit(child, rank(9)).unwrap(), child);
    assert_eq!(monotone.head_rank(), Some(rank(9)));
}

#[test]
fn ancestor_noop_repairs_the_existing_descendant_rank() {
    let (mut workspace, parent, child) = staged_parent_child();
    workspace.set_head(child, rank(1)).unwrap();

    assert_eq!(workspace.merge_commit(parent, rank(5)).unwrap(), child);
    assert_eq!(workspace.head(), Some(child));
    assert_eq!(workspace.head_rank(), Some(rank(6)));
}

#[test]
fn strict_rank_repair_overflow_is_atomic() {
    let (mut workspace, parent, child) = staged_parent_child();
    let full = BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]));
    workspace.set_head(parent, full).unwrap();

    assert!(matches!(
        workspace.merge_commit(child, BranchRank::ROOT),
        Err(triblespace_core::repo::MergeError::RankExhausted)
    ));
    assert_eq!(workspace.head(), Some(parent));
    assert_eq!(workspace.head_rank(), Some(full));

    let (mut reverse, parent, child) = staged_parent_child();
    reverse.set_head(child, BranchRank::ROOT).unwrap();
    assert!(matches!(
        reverse.merge_commit(parent, full),
        Err(triblespace_core::repo::MergeError::RankExhausted)
    ));
    assert_eq!(reverse.head(), Some(child));
    assert_eq!(reverse.head_rank(), Some(BranchRank::ROOT));
}

#[test]
fn identical_commit_rank_provenance_uses_max_without_successor() {
    let (mut workspace, _, child) = staged_parent_child();
    workspace.set_head(child, rank(5)).unwrap();

    assert_eq!(workspace.merge_commit(child, rank(9)).unwrap(), child);
    assert_eq!(workspace.head_rank(), Some(rank(9)));
}

#[test]
fn set_head_preserves_the_workspace_rank_invariant() {
    let (mut workspace, parent, child) = staged_parent_child();
    workspace.set_head(parent, rank(5)).unwrap();
    workspace.set_head(child, rank(1)).unwrap();
    assert_eq!(workspace.head(), Some(child));
    assert_eq!(workspace.head_rank(), Some(rank(6)));

    workspace.set_head(child, rank(2)).unwrap();
    assert_eq!(workspace.head_rank(), Some(rank(6)));

    let (mut exhausted, parent, child) = staged_parent_child();
    let full = BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]));
    exhausted.set_head(parent, full).unwrap();
    assert!(exhausted.set_head(child, BranchRank::ROOT).is_err());
    assert_eq!(exhausted.head(), Some(parent));
    assert_eq!(exhausted.head_rank(), Some(full));
}

#[test]
fn commit_rank_exhaustion_leaves_the_workspace_bitwise_unextended() {
    let mut repo = repository(7);
    let mut workspace = repo.create_workspace("main").unwrap();
    let original = absent_commit(0xA7);
    let full = BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]));
    workspace.set_head(original, full).unwrap();
    let staged_before = workspace.staged.reader().unwrap();

    assert!(workspace
        .commit_with_metadata(TribleSet::new(), TribleSet::new(), "cannot advance")
        .is_err());
    assert_eq!(workspace.head(), Some(original));
    assert_eq!(workspace.head_rank(), Some(full));
    assert_eq!(workspace.staged.reader().unwrap(), staged_before);
}

#[test]
fn pull_surfaces_divergent_rank_exhaustion() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let left = repo
        .storage_mut()
        .put(commit::commit_metadata(
            &signing_key,
            [],
            Some(Inline::new([1; 32])),
            Some(TribleSet::new().to_blob()),
            None,
        ))
        .unwrap();
    let right = repo
        .storage_mut()
        .put(commit::commit_metadata(
            &signing_key,
            [],
            Some(Inline::new([2; 32])),
            Some(TribleSet::new().to_blob()),
            None,
        ))
        .unwrap();
    let full = BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]));
    for (commit, rank) in [(left, full), (right, BranchRank::ROOT)] {
        repo.storage_mut()
            .append_pin_assertion(sign_branch_assertion(
                &signing_key,
                identity.name(),
                commit,
                rank,
            ))
            .unwrap();
    }

    assert!(matches!(
        repo.pull(identity),
        Err(AssertionPullError::RankExhausted)
    ));
}

#[test]
fn publishing_a_rank_repair_collapses_conflicting_exact_value_labels() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let parent = repo
        .storage_mut()
        .put(commit::commit_metadata(
            &signing_key,
            [],
            None,
            Some(TribleSet::new().to_blob()),
            None,
        ))
        .unwrap();
    let child = repo
        .storage_mut()
        .put(commit::commit_metadata(
            &signing_key,
            [parent],
            None,
            Some(TribleSet::new().to_blob()),
            None,
        ))
        .unwrap();
    for (commit, rank) in [(parent, rank(5)), (child, rank(1))] {
        repo.storage_mut()
            .append_pin_assertion(sign_branch_assertion(
                &signing_key,
                identity.name(),
                commit,
                rank,
            ))
            .unwrap();
    }

    let BranchResolution::Complete(before) = repo.resolve(&identity).unwrap() else {
        panic!("fully resident assertions must resolve completely")
    };
    let mut expected_before = vec![parent, child];
    expected_before.sort_unstable_by_key(|commit| commit.raw);
    assert_eq!(before.tips(), expected_before);

    let mut workspace = repo.create_workspace("main").unwrap();
    workspace.set_head(parent, rank(5)).unwrap();
    assert_eq!(workspace.merge_commit(child, rank(1)).unwrap(), child);
    assert_eq!(workspace.head_rank(), Some(rank(6)));
    assert!(matches!(
        repo.push(&mut workspace).unwrap(),
        PublishOutcome::Published(_)
    ));

    let BranchResolution::Complete(after) = repo.resolve(&identity).unwrap() else {
        panic!("the repaired exact claim must resolve completely")
    };
    assert_eq!(after.tips(), &[child]);
    assert_eq!(after.resolved_rank(), Some(rank(6)));
}

#[test]
fn a_missing_singleton_tip_is_pending_and_not_writable() {
    let signing_key = key(7);
    let mut repo =
        Repository::new(MemoryRepo::default(), signing_key.clone(), TribleSet::new()).unwrap();
    let identity = repo.branch_identity("main");
    let missing = absent_commit(91);
    repo.storage_mut()
        .append_pin_assertion(sign_branch_assertion(
            &signing_key,
            identity.name(),
            missing,
            BranchRank::ROOT,
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
    for (tip, rank) in [
        (left, BranchRank::ROOT.successor().unwrap()),
        (right, BranchRank::ROOT),
    ] {
        repo.storage_mut()
            .append_pin_assertion(sign_branch_assertion(
                &signing_key,
                identity.name(),
                tip,
                rank,
            ))
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
    absent
        .set_head(absent_commit(77), BranchRank::ROOT)
        .unwrap();
    assert!(matches!(
        absent_repo.push(&mut absent),
        Err(PublishError::StorageGet(_))
    ));
    assert!(absent_repo
        .storage_mut()
        .pin_assertion_snapshot()
        .unwrap()
        .is_empty());

    let mut malformed_repo = repository(7);
    let mut malformed = malformed_repo.create_workspace("main").unwrap();
    let malformed_tip = malformed.put::<SimpleArchive, _>(TribleSet::new());
    malformed.set_head(malformed_tip, BranchRank::ROOT).unwrap();
    assert!(matches!(
        malformed_repo.push(&mut malformed),
        Err(PublishError::BadCommitMetadata(_))
    ));
    assert!(malformed_repo
        .storage_mut()
        .pin_assertion_snapshot()
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
    foreign_workspace
        .commit(TribleSet::new(), "foreign")
        .unwrap();

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
    assert!(local
        .storage_mut()
        .pin_assertion_snapshot()
        .unwrap()
        .is_empty());

    local
        .storage_mut()
        .append_pin_assertion(sign_branch_assertion(
            &foreign_key,
            foreign_identity.name(),
            absent_commit(99),
            BranchRank::ROOT,
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
    foreign_workspace
        .commit(TribleSet::new(), "foreign")
        .unwrap();
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
    memory_workspace
        .set_head(memory_proposed, BranchRank::ROOT)
        .unwrap();
    let _pile_content = pile_workspace.put::<SimpleArchive, _>(content_blob);
    let pile_proposed = pile_workspace.put::<SimpleArchive, _>(commit_meta);
    pile_workspace
        .set_head(pile_proposed, BranchRank::ROOT)
        .unwrap();
    let identity = *memory_workspace.identity();
    assert_eq!(identity, *pile_workspace.identity());
    assert_eq!(memory_workspace.head(), pile_workspace.head());

    let memory_outcome = memory.push(&mut memory_workspace).unwrap();
    let pile_outcome = pile.push(&mut pile_workspace).unwrap();
    assert_eq!(memory_outcome, pile_outcome);

    let memory_assertion = memory
        .storage_mut()
        .pin_assertion_snapshot()
        .unwrap()
        .for_pin(&pin_identity(&identity))[0];
    let pile_assertion = pile
        .storage_mut()
        .pin_assertion_snapshot()
        .unwrap()
        .for_pin(&pin_identity(&identity))[0];
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
