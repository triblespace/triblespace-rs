use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::ancestors;
use triblespace::core::repo::difference;
use triblespace::core::repo::history_of;
use triblespace::core::repo::intersect;
use triblespace::core::repo::memoryrepo::MemoryRepo;
use triblespace::core::repo::nth_ancestor;
use triblespace::core::repo::parents;
use triblespace::core::repo::symmetric_diff;
use triblespace::core::repo::union;
use triblespace::core::repo::Repository;
use triblespace::prelude::*;

#[test]
fn workspace_commit_updates_head() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    ws.commit(TribleSet::new(), Some("change"));

    repo.push(&mut ws).expect("push");
}

#[test]
fn workspace_checkout_empty_branch_returns_empty_set() {
    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let result = ws.checkout(..).expect("checkout empty branch");

    assert!(result.is_empty());
}

#[test]
fn workspace_checkout_unions_commits() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let e1 = ufoid();
    let a1 = ufoid();
    let v1: Value<R256> = 1i128.to_value();
    let t1 = Trible::new(&e1, &a1, &v1);
    let mut s1 = TribleSet::new();
    s1.insert(&t1);

    ws.commit(s1.clone(), None);
    let c1 = ws.head().unwrap();

    let e2 = ufoid();
    let a2 = ufoid();
    let v2: Value<R256> = 2i128.to_value();
    let t2 = Trible::new(&e2, &a2, &v2);
    let mut s2 = TribleSet::new();
    s2.insert(&t2);

    ws.commit(s2.clone(), None);
    let c2 = ws.head().unwrap();

    let result = ws.checkout(&[c1, c2][..]).expect("checkout");

    let mut expected = s1;
    expected.union(s2);

    assert_eq!(result, expected);
}

#[test]
fn workspace_checkout_single_commit() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let e = ufoid();
    let a = ufoid();
    let v: Value<R256> = 42i128.to_value();
    let t = Trible::new(&e, &a, &v);
    let mut s = TribleSet::new();
    s.insert(&t);

    ws.commit(s.clone(), None);
    let c = ws.head().unwrap();

    let result = ws.checkout(c).expect("checkout single");

    assert_eq!(result, s);
}

#[test]
fn workspace_checkout_vec_commits() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    let mut commits = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
        commits.push(ws.head().unwrap());
    }

    let result = ws.checkout(commits.clone()).expect("checkout vec");

    let mut expected = TribleSet::new();
    for s in sets {
        expected.union(s);
    }

    assert_eq!(result, expected);
}

#[test]
fn workspace_checkout_metadata_unions_commits() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let e1 = ufoid();
    let a1 = ufoid();
    let v1: Value<R256> = 1i128.to_value();
    let t1 = Trible::new(&e1, &a1, &v1);
    let mut m1 = TribleSet::new();
    m1.insert(&t1);

    let e2 = ufoid();
    let a2 = ufoid();
    let v2: Value<R256> = 2i128.to_value();
    let t2 = Trible::new(&e2, &a2, &v2);
    let mut m2 = TribleSet::new();
    m2.insert(&t2);

    ws.commit_with_metadata(TribleSet::new(), m1.clone(), None);
    let c1 = ws.head().unwrap();
    ws.commit_with_metadata(TribleSet::new(), m2.clone(), None);
    let c2 = ws.head().unwrap();
    ws.commit(TribleSet::new(), None);
    let c3 = ws.head().unwrap();

    let result = ws
        .checkout_metadata(&[c1, c2, c3][..])
        .expect("checkout metadata");

    let mut expected = m1;
    expected.union(m2);

    assert_eq!(result, expected);
}

#[test]
fn workspace_checkout_with_metadata_returns_both() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let data_e = ufoid();
    let data_a = ufoid();
    let data_v: Value<R256> = 42i128.to_value();
    let data_t = Trible::new(&data_e, &data_a, &data_v);
    let mut data = TribleSet::new();
    data.insert(&data_t);

    let meta_e = ufoid();
    let meta_a = ufoid();
    let meta_v: Value<R256> = 7i128.to_value();
    let meta_t = Trible::new(&meta_e, &meta_a, &meta_v);
    let mut meta = TribleSet::new();
    meta.insert(&meta_t);

    ws.commit_with_metadata(data.clone(), meta.clone(), None);
    let c = ws.head().unwrap();

    let (data_out, meta_out) = ws
        .checkout_with_metadata(c)
        .expect("checkout with metadata");

    assert_eq!(data_out, data);
    assert_eq!(meta_out, meta);
}

#[test]
fn workspace_commit_uses_default_metadata() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let data_e = ufoid();
    let data_a = ufoid();
    let data_v: Value<R256> = 1i128.to_value();
    let data_t = Trible::new(&data_e, &data_a, &data_v);
    let mut data = TribleSet::new();
    data.insert(&data_t);

    let meta_e = ufoid();
    let meta_a = ufoid();
    let meta_v: Value<R256> = 99i128.to_value();
    let meta_t = Trible::new(&meta_e, &meta_a, &meta_v);
    let mut meta = TribleSet::new();
    meta.insert(&meta_t);

    ws.set_default_metadata(meta.clone());
    ws.commit(data.clone(), None);
    let c = ws.head().unwrap();

    let (data_out, meta_out) = ws
        .checkout_with_metadata(c)
        .expect("checkout with metadata");

    assert_eq!(data_out, data);
    assert_eq!(meta_out, meta);
}

#[test]
fn repository_default_metadata_seeds_workspaces() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));

    let meta_e = ufoid();
    let meta_a = ufoid();
    let meta_v: Value<R256> = 123i128.to_value();
    let meta_t = Trible::new(&meta_e, &meta_a, &meta_v);
    let mut meta = TribleSet::new();
    meta.insert(&meta_t);

    repo.set_default_metadata(meta.clone())
        .expect("set default metadata");

    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");
    ws.commit(TribleSet::new(), None);
    let c = ws.head().unwrap();

    let (_, meta_out) = ws
        .checkout_with_metadata(c)
        .expect("checkout with metadata");

    assert_eq!(meta_out, meta);
}

#[test]
fn commit_with_metadata_does_not_override_default() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let default_e = ufoid();
    let default_a = ufoid();
    let default_v: Value<R256> = 10i128.to_value();
    let default_t = Trible::new(&default_e, &default_a, &default_v);
    let mut default_meta = TribleSet::new();
    default_meta.insert(&default_t);
    ws.set_default_metadata(default_meta.clone());

    let alt_e = ufoid();
    let alt_a = ufoid();
    let alt_v: Value<R256> = 20i128.to_value();
    let alt_t = Trible::new(&alt_e, &alt_a, &alt_v);
    let mut alt_meta = TribleSet::new();
    alt_meta.insert(&alt_t);

    ws.commit(TribleSet::new(), None);
    let c1 = ws.head().unwrap();
    ws.commit_with_metadata(TribleSet::new(), alt_meta.clone(), None);
    let c2 = ws.head().unwrap();
    ws.commit(TribleSet::new(), None);
    let c3 = ws.head().unwrap();

    let (_, meta1) = ws.checkout_with_metadata(c1).expect("checkout meta1");
    let (_, meta2) = ws.checkout_with_metadata(c2).expect("checkout meta2");
    let (_, meta3) = ws.checkout_with_metadata(c3).expect("checkout meta3");

    assert_eq!(meta1, default_meta);
    assert_eq!(meta2, alt_meta);
    assert_eq!(meta3, default_meta);
}

#[test]
fn workspace_checkout_range_variants() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    let mut handles = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
        handles.push(ws.head().unwrap());
    }

    let (c1, c2, c3) = (handles[0], handles[1], handles[2]);

    let mut s1s2 = sets[0].clone();
    s1s2.union(sets[1].clone());
    let mut s2s3 = sets[1].clone();
    s2s3.union(sets[2].clone());
    let mut s1s2s3 = s1s2.clone();
    s1s2s3.union(sets[2].clone());

    assert_eq!(ws.checkout(c1..c3).unwrap(), s2s3.clone());
    assert_eq!(ws.checkout(c2..).unwrap(), sets[2].clone());
    assert_eq!(ws.checkout(..c3).unwrap(), s1s2s3.clone());
    assert_eq!(ws.checkout(..).unwrap(), s1s2s3);
}

#[test]
fn workspace_checkout_range_stops_at_explicit_boundaries() {
    use std::iter;

    use triblespace::core::blob::schemas::simplearchive::SimpleArchive;
    use triblespace::core::blob::Blob;
    use triblespace::core::repo::commit::commit_metadata;
    use triblespace::core::value::schemas::hash::{Blake3, Handle};
    use triblespace::core::value::schemas::r256::R256;
    use triblespace::core::value::Value;

    type CommitHandle = Value<Handle<Blake3, SimpleArchive>>;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let signing = SigningKey::generate(&mut OsRng);

    let root_e = ufoid();
    let root_a = ufoid();
    let root_v: Value<R256> = 0i128.to_value();
    let root_t = Trible::new(&root_e, &root_a, &root_v);
    let mut root_set = TribleSet::new();
    root_set.insert(&root_t);

    let root_blob: Blob<SimpleArchive> = root_set.clone().to_blob();
    let _: CommitHandle = ws.put(root_blob.clone());
    let root_commit = commit_metadata(
        &signing,
        iter::empty::<CommitHandle>(),
        None,
        Some(root_blob),
        None,
    );
    let root_commit_blob: Blob<SimpleArchive> = root_commit.to_blob();
    let c_root: CommitHandle = ws.put(root_commit_blob);

    let child_a_e = ufoid();
    let child_a_a = ufoid();
    let child_a_v: Value<R256> = 1i128.to_value();
    let child_a_t = Trible::new(&child_a_e, &child_a_a, &child_a_v);
    let mut child_a_set = TribleSet::new();
    child_a_set.insert(&child_a_t);

    let child_a_blob: Blob<SimpleArchive> = child_a_set.clone().to_blob();
    let _: CommitHandle = ws.put(child_a_blob.clone());
    let child_a_commit =
        commit_metadata(&signing, iter::once(c_root), None, Some(child_a_blob), None);
    let child_a_commit_blob: Blob<SimpleArchive> = child_a_commit.to_blob();
    let c_a: CommitHandle = ws.put(child_a_commit_blob);

    let child_b_e = ufoid();
    let child_b_a = ufoid();
    let child_b_v: Value<R256> = 2i128.to_value();
    let child_b_t = Trible::new(&child_b_e, &child_b_a, &child_b_v);
    let mut child_b_set = TribleSet::new();
    child_b_set.insert(&child_b_t);

    let child_b_blob: Blob<SimpleArchive> = child_b_set.clone().to_blob();
    let _: CommitHandle = ws.put(child_b_blob.clone());
    let child_b_commit =
        commit_metadata(&signing, iter::once(c_root), None, Some(child_b_blob), None);
    let child_b_commit_blob: Blob<SimpleArchive> = child_b_commit.to_blob();
    let c_b: CommitHandle = ws.put(child_b_commit_blob);

    let result = ws.checkout(c_a..c_b).expect("checkout");

    let mut expected = root_set.clone();
    expected.union(child_b_set);

    assert_eq!(result, expected);
}

#[test]
fn workspace_checkout_symmetric_diff() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    let mut handles = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
        handles.push(ws.head().unwrap());
    }

    let (c1, _c2, c3) = (handles[0], handles[1], handles[2]);
    let mut expected = sets[1].clone();
    expected.union(sets[2].clone());

    assert_eq!(ws.checkout(symmetric_diff(c1, c3)).unwrap(), expected);
}

#[test]
fn workspace_checkout_set_operation_selectors() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    let mut handles = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
        handles.push(ws.head().unwrap());
    }

    let head = ws.head().unwrap();

    let union_result = ws
        .checkout(union(handles[0], handles[2]))
        .expect("checkout union");
    let mut union_expected = sets[0].clone();
    union_expected.union(sets[2].clone());
    assert_eq!(union_result, union_expected);

    let intersect_result = ws
        .checkout(intersect(ancestors(head), ancestors(handles[1])))
        .expect("checkout intersect");
    let mut intersect_expected = sets[0].clone();
    intersect_expected.union(sets[1].clone());
    assert_eq!(intersect_result, intersect_expected);

    let difference_result = ws
        .checkout(difference(ancestors(head), ancestors(handles[1])))
        .expect("checkout difference");
    assert_eq!(difference_result, sets[2]);
}

#[test]
fn workspace_get_local_and_base() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let e = ufoid();
    let a = ufoid();
    let v: Value<R256> = 123i128.to_value();
    let t = Trible::new(&e, &a, &v);
    let mut set = TribleSet::new();
    set.insert(&t);

    let handle = ws.put(set.clone());
    ws.commit(set.clone(), None);

    let local: TribleSet = ws.get(handle).expect("get local");
    assert_eq!(local, set);

    repo.push(&mut ws).expect("push");
    let branch_id = ws.branch_id();
    let mut ws2 = repo.pull(branch_id).expect("pull");

    let base: TribleSet = ws2.get(handle).expect("get base");
    assert_eq!(base, set);
}

#[test]
fn workspace_checkout_head_collects_history() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
    }

    let head = ws.head().unwrap();
    let result = ws.checkout(ancestors(head)).expect("checkout history");

    let mut expected = sets[0].clone();
    expected.union(sets[1].clone());
    expected.union(sets[2].clone());

    assert_eq!(result, expected);
}

#[test]
fn workspace_nth_ancestor_selector() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let mut sets = Vec::new();
    for i in 0..3i128 {
        let e = ufoid();
        let a = ufoid();
        let v: Value<R256> = i.to_value();
        let t = Trible::new(&e, &a, &v);
        let mut s = TribleSet::new();
        s.insert(&t);
        ws.commit(s.clone(), None);
        sets.push(s);
    }

    let head = ws.head().unwrap();

    let result = ws
        .checkout(nth_ancestor(head, 2))
        .expect("checkout nth ancestor");
    assert_eq!(result, sets[0]);

    let empty = ws
        .checkout(nth_ancestor(head, 3))
        .expect("checkout past root");
    assert_eq!(empty.len(), 0);
}

#[test]
fn workspace_parents_selector() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));

    // Base commit so both workspaces share a common ancestor.
    let main_branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws_main = repo.pull(*main_branch_id).expect("pull branch state");
    let e0 = ufoid();
    let a0 = ufoid();
    let v0: Value<R256> = 0i128.to_value();
    let t0 = Trible::new(&e0, &a0, &v0);
    let mut s0 = TribleSet::new();
    s0.insert(&t0);
    ws_main.commit(s0, None);
    repo.push(&mut ws_main).expect("push base");

    // Fork a second workspace from the same base commit.
    let mut ws_feature = repo.pull(ws_main.branch_id()).expect("pull branch state");

    // Divergent commits on both workspaces.
    let e1 = ufoid();
    let a1 = ufoid();
    let v1: Value<R256> = 1i128.to_value();
    let t1 = Trible::new(&e1, &a1, &v1);
    let mut s1 = TribleSet::new();
    s1.insert(&t1);
    ws_main.commit(s1.clone(), None);

    let e2 = ufoid();
    let a2 = ufoid();
    let v2: Value<R256> = 2i128.to_value();
    let t2 = Trible::new(&e2, &a2, &v2);
    let mut s2 = TribleSet::new();
    s2.insert(&t2);
    ws_feature.commit(s2.clone(), None);

    // Merge the feature workspace into main to create a commit with two parents.
    ws_main.merge(&mut ws_feature).expect("merge workspaces");
    let merge_commit = ws_main.head().expect("merge head");

    let result = ws_main
        .checkout(parents(merge_commit))
        .expect("checkout parents");

    let mut expected = s1;
    expected.union(s2);

    assert_eq!(result, expected);
}

#[test]
fn workspace_history_of_entity() {
    use triblespace::core::value::schemas::r256::R256;

    let storage = MemoryRepo::default();
    let mut repo = Repository::new(storage, SigningKey::generate(&mut OsRng));
    let branch_id = repo.create_branch("main", None).expect("create branch");
    let mut ws = repo.pull(*branch_id).expect("pull");

    let entity = ufoid();
    let a1 = ufoid();
    let a2 = ufoid();
    let v1: Value<R256> = 1i128.to_value();
    let v2: Value<R256> = 2i128.to_value();

    let mut s1 = TribleSet::new();
    s1.insert(&Trible::new(&entity, &a1, &v1));
    ws.commit(s1.clone(), None);

    let mut s2 = TribleSet::new();
    s2.insert(&Trible::new(&ufoid(), &a1, &v1));
    ws.commit(s2.clone(), None);

    let mut s3 = TribleSet::new();
    s3.insert(&Trible::new(&entity, &a2, &v2));
    ws.commit(s3.clone(), None);

    let result = ws.checkout(history_of(*entity)).expect("history_of");

    let mut expected = s1;
    expected.union(s3);

    assert_eq!(result, expected);
}
