//! Resolve grow-only branch assertions under partially available ancestry.
//!
//! Only definitely dominated assertions disappear. A missing surviving tip is
//! [`BranchResolution::TipPending`](self::BranchResolution::TipPending);
//! readable tips whose relation is unknown are
//! [`BranchResolution::Partial`](self::BranchResolution::Partial). Malformed
//! metadata and backend failures remain errors. Unrelated history and payload
//! content stay lazy. Complete and partial divergent frontiers can both provide
//! a correct read view, but only a complete frontier may license an authored
//! merge assertion.

use std::collections::{HashMap, HashSet};

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::{Blob, IntoBlob};
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::repo::branch_assertion::{BranchAssertionSnapshot, BranchIdentity};
use crate::repo::commit::merge_metadata;
use crate::repo::CommitHandle;

/// Result of looking up one commit's direct parents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParentLookup {
    /// The commit metadata was present and decoded successfully.
    Present(Vec<CommitHandle>),
    /// The commit metadata is not present locally yet.
    Missing,
}

/// A commit DAG whose content-addressed metadata may be partially available.
///
/// Implementations must return [`ParentLookup::Missing`] only for genuine
/// absence. Corrupt bytes, malformed commit metadata, and backend failures are
/// errors; turning them into `Missing` would make corruption look self-healing.
pub trait PartialCommitDag {
    /// Non-absence lookup failure.
    type Error;

    /// Return the direct parents or report that this commit is not local yet.
    fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error>;
}

/// Canonical branch resolution from a coherent assertion snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BranchResolution {
    /// No assertion exists for the requested exact identity. Empty branches are
    /// intentionally unrepresentable.
    Absent,
    /// Some surviving asserted tip metadata is not present locally, so its
    /// target cannot yet be established as a well-formed commit.
    TipPending(TipPendingFrontier),
    /// Every surviving tip is well-formed, but missing ancestry prevents some
    /// pairwise relation from being decided. Its conservative view is safe to
    /// read and unsafe to assert.
    Partial(PartialFrontier),
    /// The complete maximal antichain is known.
    Complete(CompleteFrontier),
}

/// Conservative candidates containing one or more unreadable asserted tips.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TipPendingFrontier {
    tips: Vec<CommitHandle>,
    missing_tips: Vec<CommitHandle>,
}

impl TipPendingFrontier {
    /// Candidate tips retained until the missing targets arrive.
    pub fn tips(&self) -> &[CommitHandle] {
        &self.tips
    }

    /// Surviving asserted targets whose commit metadata is absent.
    pub fn missing_tips(&self) -> &[CommitHandle] {
        &self.missing_tips
    }
}

/// Well-formed conservative candidates whose exact ancestry relation is not
/// yet known.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartialFrontier {
    tips: Vec<CommitHandle>,
    missing_ancestry: Vec<CommitHandle>,
}

impl PartialFrontier {
    /// Non-definitely-dominated, well-formed candidate tips.
    pub fn tips(&self) -> &[CommitHandle] {
        &self.tips
    }

    /// Commit metadata observed missing during the unresolved comparisons.
    pub fn missing_ancestry(&self) -> &[CommitHandle] {
        &self.missing_ancestry
    }

    /// Build a conservative read view over every candidate.
    ///
    /// Checkout of this view is correct whether the unknown candidates later
    /// prove ordered or incomparable. Its synthetic merge MUST NOT be asserted:
    /// if a later ancestry fetch proves one candidate dominated, that assertion
    /// would leave a permanent redundant merge in grow-only history.
    pub fn conservative_head(&self) -> ResolvedHead {
        resolved_head(&self.tips)
    }
}

/// Complete, sorted, nonempty maximal commit antichain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteFrontier {
    tips: Vec<CommitHandle>,
}

impl CompleteFrontier {
    /// Canonical maximal tips in raw-handle order.
    pub fn tips(&self) -> &[CommitHandle] {
        &self.tips
    }

    /// Return the existing singleton head, or synthesize one flat authorless
    /// merge over the whole divergent frontier.
    pub fn resolved_head(&self) -> ResolvedHead {
        resolved_head(&self.tips)
    }
}

/// Canonical read head for a complete or conservative partial frontier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedHead {
    /// The frontier already has one maximal asserted commit.
    Existing(CommitHandle),
    /// Deterministic derived merge commit. This blob is not an assertion and
    /// never substitutes for signed replicated state.
    Synthetic(Blob<SimpleArchive>),
}

/// Resolve the assertions for one exact branch identity.
///
/// The result is invariant under assertion insertion and physical log order.
/// BranchId is used only by the snapshot's prefix scan; the snapshot rechecks
/// the complete descriptor before returning assertions.
pub fn resolve_branch<D: PartialCommitDag>(
    snapshot: &BranchAssertionSnapshot,
    identity: &BranchIdentity,
    dag: &mut D,
) -> Result<BranchResolution, D::Error> {
    let assertions = snapshot.for_branch(identity);
    if assertions.is_empty() {
        return Ok(BranchResolution::Absent);
    }

    let mut tips: Vec<_> = assertions
        .into_iter()
        .map(|assertion| assertion.commit())
        .collect();
    canonicalize(&mut tips);

    let mut view = PartialDagView::new(dag);
    let mut dominated = vec![false; tips.len()];
    let mut unknown_pairs = Vec::new();

    for left in 0..tips.len() {
        for right in (left + 1)..tips.len() {
            let forward = view.is_ancestor(tips[left], tips[right])?;
            if forward.relation == Ancestry::Yes {
                dominated[left] = true;
                continue;
            }

            let reverse = view.is_ancestor(tips[right], tips[left])?;
            if reverse.relation == Ancestry::Yes {
                dominated[right] = true;
                continue;
            }

            if forward.relation == Ancestry::Unknown || reverse.relation == Ancestry::Unknown {
                let mut missing = forward.missing;
                missing.extend(reverse.missing);
                canonicalize(&mut missing);
                unknown_pairs.push((left, right, missing));
            }
        }
    }

    let unresolved_pair = unknown_pairs
        .iter()
        .any(|(left, right, _)| !dominated[*left] && !dominated[*right]);
    let mut frontier: Vec<_> = tips
        .into_iter()
        .enumerate()
        .filter_map(|(index, commit)| (!dominated[index]).then_some(commit))
        .collect();
    canonicalize(&mut frontier);

    // A branch claim may arrive before its commit metadata. Check each
    // surviving TIP exactly once so Complete never claims an unchecked target.
    // Do not walk its entire closure: deeper history is read only when a
    // pairwise comparison actually needs it, and payload content is never read.
    let mut missing_tips = HashSet::new();
    for tip in &frontier {
        if view.parents(*tip)? == ParentLookup::Missing {
            missing_tips.insert(*tip);
        }
    }
    let mut missing_ancestry = HashSet::new();
    for (left, right, pair_missing) in unknown_pairs {
        if !dominated[left] && !dominated[right] {
            missing_ancestry.extend(pair_missing);
        }
    }
    let mut missing_tips: Vec<_> = missing_tips.into_iter().collect();
    canonicalize(&mut missing_tips);
    let mut missing_ancestry: Vec<_> = missing_ancestry.into_iter().collect();
    canonicalize(&mut missing_ancestry);

    if !missing_tips.is_empty() {
        Ok(BranchResolution::TipPending(TipPendingFrontier {
            tips: frontier,
            missing_tips,
        }))
    } else if unresolved_pair {
        Ok(BranchResolution::Partial(PartialFrontier {
            tips: frontier,
            missing_ancestry,
        }))
    } else {
        Ok(BranchResolution::Complete(CompleteFrontier {
            tips: frontier,
        }))
    }
}

fn resolved_head(tips: &[CommitHandle]) -> ResolvedHead {
    debug_assert!(!tips.is_empty());
    if tips.len() == 1 {
        ResolvedHead::Existing(tips[0])
    } else {
        let metadata = merge_metadata(tips.iter().copied());
        ResolvedHead::Synthetic(metadata.to_blob())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ancestry {
    Yes,
    No,
    Unknown,
}

struct AncestryObservation {
    relation: Ancestry,
    missing: Vec<CommitHandle>,
}

struct PartialDagView<'a, D: PartialCommitDag> {
    dag: &'a mut D,
    parents: HashMap<CommitHandle, ParentLookup>,
}

impl<'a, D: PartialCommitDag> PartialDagView<'a, D> {
    fn new(dag: &'a mut D) -> Self {
        Self {
            dag,
            parents: HashMap::new(),
        }
    }

    fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, D::Error> {
        if let Some(parents) = self.parents.get(&commit) {
            return Ok(parents.clone());
        }
        let mut lookup = self.dag.parents(commit)?;
        if let ParentLookup::Present(parents) = &mut lookup {
            canonicalize(parents);
        }
        self.parents.insert(commit, lookup.clone());
        Ok(lookup)
    }

    fn is_ancestor(
        &mut self,
        ancestor: CommitHandle,
        descendant: CommitHandle,
    ) -> Result<AncestryObservation, D::Error> {
        let mut visited = HashSet::new();
        let mut stack = vec![descendant];
        let mut missing = Vec::new();
        while let Some(current) = stack.pop() {
            if current == ancestor {
                return Ok(AncestryObservation {
                    relation: Ancestry::Yes,
                    missing,
                });
            }
            if !visited.insert(current) {
                continue;
            }
            match self.parents(current)? {
                ParentLookup::Present(parents) => stack.extend(parents),
                ParentLookup::Missing => missing.push(current),
            }
        }
        canonicalize(&mut missing);
        Ok(AncestryObservation {
            relation: if missing.is_empty() {
                Ancestry::No
            } else {
                Ancestry::Unknown
            },
            missing,
        })
    }
}

fn canonicalize(commits: &mut Vec<Inline<Handle<SimpleArchive>>>) {
    commits.sort_unstable_by_key(|commit| commit.raw);
    commits.dedup();
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use ed25519_dalek::SigningKey;
    use proptest::prelude::*;

    use super::*;
    use crate::blob::encodings::longstring::LongString;
    use crate::repo::branch_assertion::BranchAssertion;

    #[derive(Clone, Default)]
    struct TestDag {
        parents: HashMap<CommitHandle, ParentLookup>,
    }

    impl PartialCommitDag for TestDag {
        type Error = Infallible;

        fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error> {
            Ok(self
                .parents
                .get(&commit)
                .cloned()
                .unwrap_or(ParentLookup::Missing))
        }
    }

    fn commit(byte: u8) -> CommitHandle {
        Inline::new([byte; 32])
    }

    fn name(byte: u8) -> Inline<Handle<LongString>> {
        Inline::new([byte; 32])
    }

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    fn snapshot(commits: impl IntoIterator<Item = CommitHandle>) -> BranchAssertionSnapshot {
        let key = key();
        let mut snapshot = BranchAssertionSnapshot::new();
        for commit in commits {
            snapshot
                .insert(BranchAssertion::sign(&key, name(3), commit))
                .unwrap();
        }
        snapshot
    }

    fn identity() -> BranchIdentity {
        BranchIdentity::new(key().verifying_key(), name(3))
    }

    fn complete_tips(resolution: BranchResolution) -> Vec<CommitHandle> {
        match resolution {
            BranchResolution::Absent => Vec::new(),
            BranchResolution::Complete(frontier) => frontier.tips,
            other => panic!("fully resident acyclic DAG must resolve completely: {other:?}"),
        }
    }

    fn present(dag: &mut TestDag, commit: CommitHandle, parents: &[CommitHandle]) {
        dag.parents
            .insert(commit, ParentLookup::Present(parents.to_vec()));
    }

    #[test]
    fn linear_assertions_collapse_to_the_maximal_commit() {
        let (a, b, c) = (commit(1), commit(2), commit(3));
        let mut dag = TestDag::default();
        present(&mut dag, a, &[]);
        present(&mut dag, b, &[a]);
        present(&mut dag, c, &[b]);
        let resolved = resolve_branch(&snapshot([b, a, c]), &identity(), &mut dag).unwrap();
        assert_eq!(
            resolved,
            BranchResolution::Complete(CompleteFrontier { tips: vec![c] })
        );
    }

    #[test]
    fn missing_unneeded_ancestry_does_not_block_a_proven_maximum() {
        let (root, child, missing) = (commit(1), commit(2), commit(9));
        let mut dag = TestDag::default();
        present(&mut dag, root, &[]);
        present(&mut dag, child, &[root, missing]);
        let assertions = snapshot([root, child]);

        let resolved = resolve_branch(&assertions, &identity(), &mut dag).unwrap();
        assert_eq!(
            resolved,
            BranchResolution::Complete(CompleteFrontier { tips: vec![child] })
        );

        present(&mut dag, missing, &[]);
        let complete = resolve_branch(&assertions, &identity(), &mut dag).unwrap();
        assert_eq!(
            complete,
            BranchResolution::Complete(CompleteFrontier { tips: vec![child] })
        );
    }

    #[test]
    fn missing_ancestry_needed_to_compare_surviving_tips_is_partial() {
        let (left, right, missing) = (commit(1), commit(2), commit(9));
        let mut dag = TestDag::default();
        present(&mut dag, left, &[missing]);
        present(&mut dag, right, &[]);
        let assertions = snapshot([left, right]);

        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            BranchResolution::Partial(PartialFrontier {
                tips: vec![left, right],
                missing_ancestry: vec![missing],
            })
        );

        present(&mut dag, missing, &[]);
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            BranchResolution::Complete(CompleteFrontier {
                tips: vec![left, right],
            })
        );
    }

    #[test]
    fn singleton_missing_commit_is_pending_but_payloads_are_not_consulted() {
        let tip = commit(4);
        let assertions = snapshot([tip]);
        let mut dag = TestDag::default();
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            BranchResolution::TipPending(TipPendingFrontier {
                tips: vec![tip],
                missing_tips: vec![tip],
            })
        );

        present(&mut dag, tip, &[]);
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            BranchResolution::Complete(CompleteFrontier { tips: vec![tip] })
        );
    }

    #[test]
    fn divergent_frontier_builds_one_flat_order_independent_merge() {
        let (a, b, c) = (commit(1), commit(2), commit(3));
        let mut dag = TestDag::default();
        present(&mut dag, a, &[]);
        present(&mut dag, b, &[]);
        present(&mut dag, c, &[]);

        let left = resolve_branch(&snapshot([c, a, b]), &identity(), &mut dag).unwrap();
        let right = resolve_branch(&snapshot([b, c, a]), &identity(), &mut dag).unwrap();
        assert_eq!(left, right);

        let BranchResolution::Complete(frontier) = left else {
            panic!("complete ancestry must resolve")
        };
        assert_eq!(frontier.tips(), &[a, b, c]);
        let ResolvedHead::Synthetic(first) = frontier.resolved_head() else {
            panic!("three tips must synthesize a merge")
        };
        let ResolvedHead::Synthetic(second) = CompleteFrontier {
            tips: vec![c, b, a],
        }
        .resolved_head() else {
            panic!("three tips must synthesize a merge")
        };
        assert_eq!(first, second);
    }

    #[test]
    fn maximal_frontier_is_a_sufficient_statistic() {
        let (a, b, c, d) = (commit(1), commit(2), commit(3), commit(4));
        let mut dag = TestDag::default();
        present(&mut dag, a, &[]);
        present(&mut dag, b, &[a]);
        present(&mut dag, c, &[a]);
        present(&mut dag, d, &[b, c]);

        let all = resolve_branch(&snapshot([a, b, c, d]), &identity(), &mut dag).unwrap();
        let first = resolve_branch(&snapshot([a, b, c]), &identity(), &mut dag).unwrap();
        let BranchResolution::Complete(first) = first else {
            panic!("complete ancestry must resolve")
        };
        let compacted_then_extended = resolve_branch(
            &snapshot(first.tips().iter().copied().chain([d])),
            &identity(),
            &mut dag,
        )
        .unwrap();
        assert_eq!(all, compacted_then_extended);
    }

    proptest! {
        #[test]
        fn max_is_a_sufficient_statistic_on_generated_dags(
            node_count in 1usize..9,
            parent_masks in prop::collection::vec(any::<u16>(), 8),
            first_mask in any::<u16>(),
            second_mask in any::<u16>(),
        ) {
            let commits: Vec<_> = (0..node_count)
                .map(|index| commit(index as u8 + 1))
                .collect();
            let mut dag = TestDag::default();
            for index in 0..node_count {
                let lower = if index == 0 { 0 } else { (1u16 << index) - 1 };
                let parents: Vec<_> = (0..index)
                    .filter(|parent| parent_masks[index] & (1u16 << parent) & lower != 0)
                    .map(|parent| commits[parent])
                    .collect();
                present(&mut dag, commits[index], &parents);
            }

            let selected = |mask: u16| {
                commits
                    .iter()
                    .enumerate()
                    .filter_map(|(index, commit)| (mask & (1u16 << index) != 0).then_some(*commit))
                    .collect::<Vec<_>>()
            };
            let first = selected(first_mask);
            let second = selected(second_mask);
            let mut union = first.clone();
            union.extend(second.iter().copied());

            let direct = complete_tips(
                resolve_branch(&snapshot(union), &identity(), &mut dag).unwrap()
            );
            let first_max = complete_tips(
                resolve_branch(&snapshot(first), &identity(), &mut dag).unwrap()
            );
            let mut compacted_then_extended = first_max;
            compacted_then_extended.extend(second);
            let incremental = complete_tips(
                resolve_branch(
                    &snapshot(compacted_then_extended),
                    &identity(),
                    &mut dag,
                )
                .unwrap()
            );
            prop_assert_eq!(direct, incremental);
        }

        #[test]
        fn delivery_order_does_not_change_frontier_or_head(
            node_count in 1usize..9,
            parent_masks in prop::collection::vec(any::<u16>(), 8),
            assertion_mask in 1u16..=u16::MAX,
        ) {
            let commits: Vec<_> = (0..node_count)
                .map(|index| commit(index as u8 + 1))
                .collect();
            let mut dag = TestDag::default();
            for index in 0..node_count {
                let lower = if index == 0 { 0 } else { (1u16 << index) - 1 };
                let parents: Vec<_> = (0..index)
                    .filter(|parent| parent_masks[index] & (1u16 << parent) & lower != 0)
                    .map(|parent| commits[parent])
                    .collect();
                present(&mut dag, commits[index], &parents);
            }
            let forward: Vec<_> = commits
                .iter()
                .enumerate()
                .filter_map(|(index, commit)| {
                    (assertion_mask & (1u16 << index) != 0).then_some(*commit)
                })
                .collect();
            prop_assume!(!forward.is_empty());
            let mut reverse = forward.clone();
            reverse.reverse();

            let left = resolve_branch(&snapshot(forward), &identity(), &mut dag).unwrap();
            let right = resolve_branch(&snapshot(reverse), &identity(), &mut dag).unwrap();
            prop_assert_eq!(&left, &right);

            let BranchResolution::Complete(left) = left else {
                prop_assert!(false, "resident DAG did not complete");
                return Ok(());
            };
            let BranchResolution::Complete(right) = right else {
                prop_assert!(false, "resident DAG did not complete");
                return Ok(());
            };
            prop_assert_eq!(left.resolved_head(), right.resolved_head());
        }
    }
}
