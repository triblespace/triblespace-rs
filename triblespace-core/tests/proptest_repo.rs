use ed25519_dalek::SigningKey;
use proptest::collection::vec;
use proptest::prelude::*;
use rand::rngs::OsRng;
use std::collections::BTreeSet;
use triblespace_core::id::rngid;
use triblespace_core::inline::encodings::shortstring::ShortString;
use triblespace_core::prelude::*;
use triblespace_core::repo::memoryrepo::MemoryRepo;
use triblespace_core::repo::Repository;

mod test_ns {
    use triblespace_core::prelude::*;
    attributes! {
        "DD00000000000000DD00000000000001" as pub label: inlineencodings::ShortString;
    }
}

proptest! {
    // ── Workspace commit + checkout round-trip ─────────────────────────

    #[test]
    fn commit_checkout_roundtrip(
        labels in vec("[a-z]{1,8}", 1..10),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull");

        // Commit data
        let mut data = TribleSet::new();
        for label in &labels {
            let e = rngid();
            data += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data.clone(), "test commit");

        // Checkout and verify
        let checkout = ws.checkout(..).expect("checkout");
        prop_assert_eq!(checkout.facts().len(), data.len(),
            "checkout should contain all committed tribles");

        // Query should return all labels
        let found: BTreeSet<Inline<ShortString>> = find!(
            label: Inline<ShortString>,
            pattern!(&checkout, [{ test_ns::label: ?label }])
        ).collect();
        let expected: BTreeSet<Inline<ShortString>> = labels
            .iter()
            .map(|label| label.as_str().to_inline())
            .collect();
        prop_assert_eq!(found, expected);
    }

    #[test]
    fn multiple_commits_accumulate(
        batch1 in vec("[a-z]{1,6}", 1..5),
        batch2 in vec("[a-z]{1,6}", 1..5),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull");

        // First commit
        let mut data1 = TribleSet::new();
        for label in &batch1 {
            let e = rngid();
            data1 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data1.clone(), "batch 1");

        // Second commit
        let mut data2 = TribleSet::new();
        for label in &batch2 {
            let e = rngid();
            data2 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data2.clone(), "batch 2");

        // Full checkout should contain both batches
        let checkout = ws.checkout(..).expect("checkout");
        let expected_len = data1.len() + data2.len();
        prop_assert_eq!(checkout.facts().len(), expected_len);

        // All labels from both batches should be queryable
        let found: Vec<String> = find!(
            label: String,
            pattern!(&checkout, [{ test_ns::label: ?label }])
        ).collect();
        for label in batch1.iter().chain(batch2.iter()) {
            prop_assert!(found.contains(label),
                "missing {:?}", label);
        }
    }

    #[test]
    fn push_then_pull_preserves_data(
        labels in vec("[a-z]{1,8}", 1..8),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull");

        let mut data = TribleSet::new();
        for label in &labels {
            let e = rngid();
            data += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data, "commit");
        repo.push(&mut ws).expect("push");

        // Fresh pull should see the same data
        let mut ws2 = repo.pull(*branch_id).expect("pull2");
        let checkout = ws2.checkout(..).expect("checkout");

        let found: BTreeSet<Inline<ShortString>> = find!(
            label: Inline<ShortString>,
            pattern!(&checkout, [{ test_ns::label: ?label }])
        ).collect();
        let expected: BTreeSet<Inline<ShortString>> = labels
            .iter()
            .map(|label| label.as_str().to_inline())
            .collect();
        prop_assert_eq!(found, expected,
            "push then pull should preserve all data");
    }

    // ── Incremental checkout via CommitSet ────────────────────────────

    #[test]
    fn incremental_checkout_excludes_seen(
        batch1 in vec("[a-z]{1,6}", 1..5),
        batch2 in vec("[a-z]{1,6}", 1..5),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("branch");

        // First commit + push
        let mut ws = repo.pull(*branch_id).expect("pull");
        let mut data1 = TribleSet::new();
        for label in &batch1 {
            let e = rngid();
            data1 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data1.clone(), "batch 1");
        repo.push(&mut ws).expect("push");

        // First checkout — sees everything
        let mut full = repo.pull(*branch_id).expect("pull").checkout(..).expect("checkout");

        // Second commit + push
        let mut ws = repo.pull(*branch_id).expect("pull");
        let mut data2 = TribleSet::new();
        for label in &batch2 {
            let e = rngid();
            data2 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data2.clone(), "batch 2");
        repo.push(&mut ws).expect("push");

        // Incremental checkout — should only see batch2
        let mut ws2 = repo.pull(*branch_id).expect("pull");
        let delta = ws2.checkout(full.commits()..).expect("delta");

        let delta_labels: BTreeSet<Inline<ShortString>> = find!(
            label: Inline<ShortString>,
            pattern!(&delta, [{ test_ns::label: ?label }])
        ).collect();
        let expected_delta: BTreeSet<Inline<ShortString>> = batch2
            .iter()
            .map(|label| label.as_str().to_inline())
            .collect();
        prop_assert_eq!(&delta_labels, &expected_delta,
            "the incremental checkout should project batch2's distinct raw labels");

        // Accumulate: full += &delta
        full += &delta;
        let all_labels: BTreeSet<Inline<ShortString>> = find!(
            label: Inline<ShortString>,
            pattern!(&full, [{ test_ns::label: ?label }])
        ).collect();
        let expected_all: BTreeSet<Inline<ShortString>> = batch1
            .iter()
            .chain(batch2.iter())
            .map(|label| label.as_str().to_inline())
            .collect();
        prop_assert_eq!(all_labels, expected_all);
    }

    // ── Workspace merge ────────────────────────────────────────────────

    #[test]
    fn merge_combines_concurrent_commits(
        labels_a in vec("[a-z]{1,6}", 1..4),
        labels_b in vec("[m-z]{1,6}", 1..4),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("branch");

        // Workspace A commits
        let mut ws_a = repo.pull(*branch_id).expect("pull");
        let mut data_a = TribleSet::new();
        for label in &labels_a {
            let e = rngid();
            data_a += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws_a.commit(data_a, "from A");
        repo.push(&mut ws_a).expect("push A");

        // Workspace B commits (on top of A)
        let mut ws_b = repo.pull(*branch_id).expect("pull");
        let mut data_b = TribleSet::new();
        for label in &labels_b {
            let e = rngid();
            data_b += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws_b.commit(data_b, "from B");
        repo.push(&mut ws_b).expect("push B");

        // Checkout should contain both
        let mut ws_final = repo.pull(*branch_id).expect("pull");
        let checkout = ws_final.checkout(..).expect("checkout");

        let found: Vec<String> = find!(
            label: String,
            pattern!(&checkout, [{ test_ns::label: ?label }])
        ).collect();

        for label in labels_a.iter().chain(labels_b.iter()) {
            prop_assert!(found.contains(label),
                "merged checkout missing {:?}", label);
        }
    }

    // ── Checkout union ───────────────────────────────────────────────

    #[test]
    fn checkout_union_accumulates_facts_and_commits(
        batch1 in vec("[a-z]{1,6}", 1..5),
        batch2 in vec("[a-z]{1,6}", 1..5),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("branch");

        // Commit batch1
        let mut ws = repo.pull(*branch_id).expect("pull");
        let mut data1 = TribleSet::new();
        for label in &batch1 {
            let e = rngid();
            data1 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data1.clone(), "batch 1");
        repo.push(&mut ws).expect("push");

        // First checkout
        let mut ws1 = repo.pull(*branch_id).expect("pull");
        let checkout1 = ws1.checkout(..).expect("checkout1");

        // Commit batch2
        let mut ws = repo.pull(*branch_id).expect("pull");
        let mut data2 = TribleSet::new();
        for label in &batch2 {
            let e = rngid();
            data2 += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data2.clone(), "batch 2");
        repo.push(&mut ws).expect("push");

        // Second checkout (only new commits)
        let mut ws2 = repo.pull(*branch_id).expect("pull");
        let checkout2 = ws2.checkout(checkout1.commits()..).expect("checkout2");

        // Union the two checkouts
        let mut combined = checkout1;
        combined += &checkout2;

        // Combined should have all labels
        let found: BTreeSet<Inline<ShortString>> = find!(
            label: Inline<ShortString>,
            pattern!(&combined, [{ test_ns::label: ?label }])
        ).collect();
        let expected: BTreeSet<Inline<ShortString>> = batch1
            .iter()
            .chain(batch2.iter())
            .map(|label| label.as_str().to_inline())
            .collect();
        prop_assert_eq!(found, expected);

        // Combined commits should cover both checkouts
        // A third incremental checkout should yield nothing new
        let mut ws3 = repo.pull(*branch_id).expect("pull");
        let checkout3 = ws3.checkout(combined.commits()..).expect("checkout3");
        prop_assert!(checkout3.facts().is_empty(),
            "combined commits should exclude all seen data, got {} tribles", checkout3.facts().len());
    }

    // ── BlobStore round-trip ───────────────────────────────────────────

    #[test]
    fn blobstore_put_get_roundtrip(
        content in vec(any::<u8>(), 0..200),
    ) {
        use triblespace_core::blob::MemoryBlobStore;
        use triblespace_core::blob::encodings::utf8string::UTF8String;
        use triblespace_core::repo::{BlobStorePut, BlobStore, BlobStoreGet};
        use anybytes::View;

        let text = String::from_utf8_lossy(&content).to_string();
        let mut store: MemoryBlobStore = MemoryBlobStore::default();
        let handle = store.put::<UTF8String, _>(text.clone()).expect("put");

        let reader = store.reader().expect("reader");
        let retrieved: View<str> = reader.get(handle).expect("get");
        prop_assert_eq!(retrieved.as_ref(), text.as_str());
    }

    #[test]
    fn checkout_commits_tracks_seen(
        labels in vec("[a-z]{1,8}", 1..5),
    ) {
        let storage = MemoryRepo::default();
        let mut repo = Repository::new(
            storage,
            SigningKey::generate(&mut OsRng),
            TribleSet::new(),
        ).unwrap();
        let branch_id = repo.create_branch("test", None).expect("create branch");
        let mut ws = repo.pull(*branch_id).expect("pull");

        let mut data = TribleSet::new();
        for label in &labels {
            let e = rngid();
            data += entity! { &e @ test_ns::label: label.as_str() };
        }
        ws.commit(data, "commit");

        let checkout = ws.checkout(..).expect("checkout");
        // commits() should be non-empty after a checkout with data
        prop_assert!(!checkout.commits().is_empty(),
            "checkout should track the commit");
    }
}

// ── Branch-head annotations survive a rebuild ─────────────────────────
//
// Pushing a commit REBUILDS the branch-head metadata: `branch_metadata`
// mints a fresh, content-derived branch entity, so anything else stored
// beside it is dropped unless deliberately carried forward.
//
// That carry used to name exactly one kind (`index_home::manifest_tribles`),
// which made head metadata durable only for annotations core knew about by
// name — a closed-world assumption in a store built on open extension. The
// loss was invisible: an annotation could be written, read back, and then
// vanish at the next unrelated push, with no error at the write and no trace
// afterwards.
//
// This asserts the property that makes head metadata usable by anything
// outside core — a migration record, a downstream faculty's annotation.
mod branch_head_carry {
    use super::*;
    use triblespace_core::repo::branch;

    mod ann {
        use triblespace_core::prelude::*;
        attributes! {
            "DD00000000000000DD00000000000002" as pub note: inlineencodings::ShortString;
        }
    }

    #[test]
    fn an_unknown_fact_on_a_branch_head_survives_a_push() {
        let storage = MemoryRepo::default();
        let mut repo =
            Repository::new(storage, SigningKey::generate(&mut OsRng), TribleSet::new()).unwrap();
        let branch_id = *repo.create_branch("annotated", None).expect("create branch");

        // Attach an annotation core knows nothing about, beside the head.
        let marker = rngid();
        let annotation: TribleSet = entity! { &marker @ ann::note: "kilroy" }.into();
        let base = repo
            .storage_mut()
            .head(branch_id)
            .expect("head")
            .expect("branch exists");
        let mut head_set: TribleSet = repo
            .storage_mut()
            .reader()
            .expect("reader")
            .get(base)
            .expect("read head meta");
        head_set += annotation;
        let handle = repo
            .storage_mut()
            .put(head_set.to_blob())
            .expect("store annotated head");
        repo.storage_mut()
            .update(branch_id, Some(base), Some(handle))
            .expect("update head");

        // An ordinary commit — nothing to do with the annotation.
        let mut ws = repo.pull(branch_id).expect("pull");
        let e = rngid();
        let work: TribleSet = entity! { &e @ test_ns::label: "work" }.into();
        ws.commit(work, "a commit");
        repo.push(&mut ws).expect("push");

        // The annotation must still be there.
        let after_handle = repo
            .storage_mut()
            .head(branch_id)
            .expect("head")
            .expect("branch exists");
        let after: TribleSet = repo
            .storage_mut()
            .reader()
            .expect("reader")
            .get(after_handle)
            .expect("read head meta");
        let notes: Vec<Inline<triblespace_core::inline::encodings::shortstring::ShortString>> =
            find!(n: Inline<ShortString>, pattern!(&after, [{ marker @ ann::note: ?n }])).collect();
        assert_eq!(
            notes.len(),
            1,
            "an annotation beside the branch head must survive a push that rebuilds it"
        );

        // And the head genuinely advanced — otherwise this proves nothing.
        assert_ne!(after_handle, base, "the push must have rebuilt the head");
    }
}
