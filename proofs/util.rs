#![cfg(kani)]

use crate::id::{ExclusiveId, Id, ID_LEN};
use crate::patch::Entry;
use crate::trible::Trible;
use crate::value::schemas::UnknownValue;
use crate::value::{Value, VALUE_LEN};
use kani::BoundedArbitrary;

/// Ensures the generated identifier is never nil by rejecting the sentinel.
fn non_nil_raw_id() -> [u8; ID_LEN] {
    let raw: [u8; ID_LEN] = kani::any();
    // Rule out the nil sentinel without biasing the remaining population.
    kani::assume(raw != [0u8; ID_LEN]);
    raw
}

/// Generate a bounded identifier suitable for use in Kani harnesses.
///
/// The value is guaranteed to be non-nil so it can be promoted to an
/// [`ExclusiveId`] when needed.
pub fn bounded_id() -> Id {
    Id::new(non_nil_raw_id()).expect("non-nil ids are always valid")
}

/// Generate a bounded, writeable identifier for harnesses that require
/// entity ownership.
pub fn bounded_exclusive_id() -> ExclusiveId {
    ExclusiveId::force(bounded_id())
}

/// Produce a value with a reduced search space for harnesses that only care
/// about byte-level behaviour.
pub fn bounded_unknown_value() -> Value<UnknownValue> {
    let raw: [u8; VALUE_LEN] = kani::any();
    // Restrict the value to the lower nibble of each byte to keep the state
    // space manageable for symbolic execution while still covering a wide
    // range of patterns.
    let raw = raw.map(|byte| byte & 0x0F);
    Value::new(raw)
}

/// Construct a single [`Trible`] using bounded identifiers and value bytes.
pub fn bounded_trible() -> Trible {
    let entity = bounded_exclusive_id();
    let attribute = bounded_id();
    let value = bounded_unknown_value();
    Trible::new(&entity, &attribute, &value)
}

/// Generate a bounded key for PATCH based structures by restricting each byte
/// to the lower nibble.
pub fn bounded_patch_key<const KEY_LEN: usize>() -> [u8; KEY_LEN] {
    let raw: [u8; KEY_LEN] = kani::any();
    raw.map(|byte| byte & 0x0F)
}

/// Produce a shareable PATCH entry with an empty payload along with the key
/// bytes used to construct it.
pub fn bounded_patch_entry<const KEY_LEN: usize>() -> ([u8; KEY_LEN], Entry<KEY_LEN>) {
    let key = bounded_patch_key::<KEY_LEN>();
    let entry = Entry::new(&key);
    (key, entry)
}

/// Produce a shareable PATCH entry with a bounded payload generated via the
/// [`BoundedArbitrary`] trait, returning both the key and entry so harnesses can
/// interact with the PATCH APIs that accept raw keys.
pub fn bounded_patch_entry_with_value<const KEY_LEN: usize, V, const MAX: usize>(
) -> ([u8; KEY_LEN], Entry<KEY_LEN, V>)
where
    V: BoundedArbitrary,
{
    let key = bounded_patch_key::<KEY_LEN>();
    let value = V::bounded_any::<MAX>();
    let entry = Entry::with_value(&key, value);
    (key, entry)
}

/// Maximum number of parents assigned to a commit in the generated DAG.
const MAX_COMMIT_PARENTS: usize = 2;

#[derive(Clone, Copy, Debug)]
struct CommitNode {
    parents: [Option<usize>; MAX_COMMIT_PARENTS],
}

impl CommitNode {
    const fn new() -> Self {
        Self {
            parents: [None; MAX_COMMIT_PARENTS],
        }
    }
}

/// A compact commit DAG description used by harnesses to exercise repository
/// algorithms.
#[derive(Clone, Debug)]
pub struct CommitDag<const MAX_COMMITS: usize> {
    len: usize,
    nodes: [CommitNode; MAX_COMMITS],
}

impl<const MAX_COMMITS: usize> CommitDag<MAX_COMMITS> {
    /// Number of commits represented in the DAG.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Iterator over commit indices in insertion order.
    pub fn indices(&self) -> core::ops::Range<usize> {
        0..self.len
    }

    /// Returns the (up to two) parents associated with `index`.
    pub fn parents(&self, index: usize) -> &[Option<usize>; MAX_COMMIT_PARENTS] {
        assert!(index < self.len, "commit index out of bounds");
        &self.nodes[index].parents
    }
}

/// Generate a bounded commit DAG with up to `MAX_COMMITS` nodes.
///
/// The resulting DAG is acyclic by construction because every parent index is
/// constrained to reference an earlier commit.
pub fn bounded_commit_dag<const MAX_COMMITS: usize>() -> CommitDag<MAX_COMMITS> {
    const {
        assert!(MAX_COMMITS > 0);
    }

    let mut nodes = [CommitNode::new(); MAX_COMMITS];

    let bound = MAX_COMMITS.min(4);
    let len: usize = kani::any();
    kani::assume(len <= bound);

    for index in 0..len {
        if index == 0 {
            continue;
        }

        let max_parents = index.min(MAX_COMMIT_PARENTS);
        let parent_count: usize = kani::any();
        kani::assume(parent_count <= max_parents);

        let mut node = CommitNode::new();

        if parent_count >= 1 {
            let first_parent: usize = kani::any();
            kani::assume(first_parent < index);
            node.parents[0] = Some(first_parent);
        }

        if parent_count >= 2 {
            let second_parent: usize = kani::any();
            kani::assume(second_parent < index);
            if let Some(first_parent) = node.parents[0] {
                kani::assume(second_parent != first_parent);
            }
            node.parents[1] = Some(second_parent);
        }

        nodes[index] = node;
    }

    CommitDag { len, nodes }
}
