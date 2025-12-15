#![cfg(kani)]

use arrayvec::ArrayVec;
use ed25519_dalek::{SigningKey, SECRET_KEY_LENGTH};

use crate::blob::schemas::simplearchive::SimpleArchive;
use crate::repo::commit;
use crate::repo::memoryrepo::MemoryRepo;
use crate::repo::{self, Repository};
use crate::value::schemas::hash::{Blake3, Handle};
use crate::value::Value;

use super::util::bounded_commit_dag;

const MAX_COMMITS: usize = 4;

#[kani::proof]
#[kani::unwind(5)]
fn ancestors_respects_bounded_commit_dags() {
    let dag = bounded_commit_dag::<MAX_COMMITS>();

    let secret: [u8; SECRET_KEY_LENGTH] = kani::any();
    let signing_key = SigningKey::from_bytes(&secret);

    let mut storage = MemoryRepo::default();
    let mut commit_handles = ArrayVec::<Value<Handle<Blake3, SimpleArchive>>, MAX_COMMITS>::new();

    for index in dag.indices() {
        let parents = dag.parents(index);
        let parent_handles = parents
            .iter()
            .flatten()
            .map(|&parent_index| commit_handles[parent_index]);

        let metadata = commit::commit_metadata(&signing_key, parent_handles, None, None);
        let handle = storage
            .blobs
            .put(metadata)
            .expect("in-memory blob insertion cannot fail");
        commit_handles.push(handle);
    }

    if commit_handles.is_empty() {
        return;
    }

    let mut repo = Repository::new(storage, signing_key);
    let branch_id = repo
        .create_branch("kani", commit_handles.last().copied())
        .expect("branch creation")
        .release();
    let mut workspace = repo.pull(branch_id).expect("pull workspace");

    for (index, &handle) in commit_handles.iter().enumerate() {
        let patch = repo::ancestors(handle)
            .select(&mut workspace)
            .expect("collect ancestors");

        let mut expected = [false; MAX_COMMITS];
        let mut stack = ArrayVec::<usize, MAX_COMMITS>::new();
        stack.push(index);

        while let Some(node) = stack.pop() {
            if expected[node] {
                continue;
            }
            expected[node] = true;

            for &parent in dag.parents(node).iter().flatten() {
                stack.push(parent);
            }
        }

        let mut observed = [false; MAX_COMMITS];
        for raw in patch.iter() {
            let candidate = Value::<Handle<Blake3, SimpleArchive>>::new(*raw);
            let mut matched = false;
            for (pos, existing) in commit_handles.iter().enumerate() {
                if *existing == candidate {
                    observed[pos] = true;
                    matched = true;
                    break;
                }
            }
            assert!(matched, "ancestors returned unknown commit handle");
        }

        for node in 0..commit_handles.len() {
            assert_eq!(observed[node], expected[node], "ancestor set mismatch");
        }
    }
}
