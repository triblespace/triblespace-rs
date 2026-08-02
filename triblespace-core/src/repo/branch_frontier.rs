//! Resolve the typed branch projection of generic asserted pins under partially
//! available ancestry.
//!
//! Only definitely dominated assertions disappear. A missing surviving tip is
//! [`BranchResolution::TipPending`];
//! readable tips whose relation is unknown are
//! [`BranchResolution::Partial`]. Malformed
//! metadata and backend failures remain errors. Unrelated history and payload
//! content stay lazy. A partial divergent frontier exposes a deterministic
//! candidate-root descriptor, but missing ancestry prevents checkout and only a
//! complete frontier may license an authored merge assertion.
//!
//! Persisted assertion signatures are verified lazily. Resolution may perform
//! optimistic ancestry reads for structural claims, but only against the local
//! [`PartialCommitDag`]: those reads MUST NOT initiate network work. Every
//! optimistic frontier claim is authenticated before its target is checked for
//! presence and before any [`TipPendingFrontier`] or [`PartialFrontier`] demand
//! is returned. A forged dominator is discarded and the frontier is recomputed
//! from scratch, allowing a buried real claim to reappear.
//!
//! [`BranchResolution::TipPending`]: crate::repo::branch_frontier::BranchResolution::TipPending
//! [`BranchResolution::Partial`]: crate::repo::branch_frontier::BranchResolution::Partial

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::blob::encodings::simplearchive::SimpleArchive;
use crate::blob::{Blob, IntoBlob};
use crate::inline::encodings::hash::Handle;
use crate::inline::Inline;
use crate::repo::branch_pin::{commit_from_value, BranchIdentity, BranchPinDescriptor, BranchRank};
use crate::repo::commit::merge_metadata;
use crate::repo::pin_assertion::{PinAssertionId, PinAssertionSnapshot, PinAssertionWitness};
use crate::repo::CommitHandle;

/// Result of looking up one commit's direct parents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParentLookup {
    /// The commit metadata was present and decoded successfully.
    Present(Vec<CommitHandle>),
    /// The commit metadata is not present locally yet.
    Missing,
}

/// A local commit DAG whose content-addressed metadata may be partially
/// available.
///
/// Implementations must return [`ParentLookup::Missing`] only for genuine
/// absence. Corrupt bytes, malformed commit metadata, and backend failures are
/// errors; turning them into `Missing` would make corruption look self-healing.
/// This interface is also the resolver's pre-authentication read boundary:
/// `parents` MUST inspect only already-local state and MUST NOT enqueue a fetch,
/// emit a want, or otherwise create externally visible demand. Replication
/// admission must bound the number of stored assertion witnesses independently
/// of this fold.
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
    /// pairwise relation from being decided. Its conservative root is a
    /// descriptor only: it is neither checkout-safe nor assertion-safe.
    Partial(PartialFrontier),
    /// The maximal antichain is known under the branch rank contract.
    /// Dishonest author-signed labels can conservatively retain redundant tips
    /// and make that over-approximation appear complete, but cannot remove a
    /// truly maximal tip.
    Complete(CompleteFrontier),
}

/// Conservative candidates containing one or more unreadable asserted tips.
///
/// Callers recover by fetching [`Self::missing_tips`] and rerunning resolution.
/// Deeper gaps observed during the same pass are deliberately withheld: a
/// fetched tip can prove another candidate dominated and make those wants
/// irrelevant. A fair fetch-and-rerun loop therefore costs at most one extra
/// demand round while avoiding irrelevant ancestry downloads.
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

    /// Build a deterministic root descriptor over every unresolved candidate.
    ///
    /// This retains every non-definitely-dominated candidate, but missing
    /// ancestry still makes high-level checkout unavailable. Its synthetic
    /// merge MUST NOT be asserted: if a later ancestry fetch proves one
    /// candidate dominated, that assertion would leave a permanent redundant
    /// merge in grow-only history.
    pub fn candidate_root(&self) -> ResolvedHead {
        resolved_head(&self.tips)
    }
}

/// Complete, sorted, nonempty maximal commit antichain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteFrontier {
    tips: Vec<CommitHandle>,
    resolved_rank: Option<BranchRank>,
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

    /// Rank carried by the existing singleton head or assigned to the
    /// deterministic synthetic merge.
    ///
    /// `None` means an all-`0xFF` parent rank cannot be advanced. Checkout is
    /// still sound, but publication must fail explicitly rather than wrapping
    /// or inventing a rank.
    pub fn resolved_rank(&self) -> Option<BranchRank> {
        self.resolved_rank
    }
}

/// Canonical root descriptor for a complete or unresolved candidate frontier.
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
/// The generic snapshot narrows by the full pin-identity digest and then
/// rechecks the complete identity before returning witnesses. Persisted witnesses are
/// authenticated only when their semantic `(identity, commit)` claim reaches
/// the optimistic frontier. At least one valid witness licenses a claim; all
/// surviving siblings are checked so publication provenance can take the
/// greatest valid rank. If every witness for a frontier claim is invalid, that
/// claim is removed and domination is recomputed from the remaining claims.
/// Thus every returned tip has a verified witness in this call, and invalid
/// claims never create fetch demand.
pub fn resolve_branch<D: PartialCommitDag>(
    snapshot: &PinAssertionSnapshot,
    identity: &BranchIdentity,
    dag: &mut D,
) -> Result<BranchResolution, D::Error> {
    let pin_identity = BranchPinDescriptor::pin_identity(identity.author(), identity.name());
    let witnesses = snapshot.witnesses_for_pin(&pin_identity);
    if witnesses.is_empty() {
        return Ok(BranchResolution::Absent);
    }

    // Exact values are the semantic claim identity. Group them before looking
    // at labels: equal labels rule out *strict* ancestry only between distinct
    // commits, and repeated assertions of one commit must never compare as two
    // tips.
    let mut claims: BTreeMap<CommitHandle, Vec<&PinAssertionWitness>> = BTreeMap::new();
    for witness in witnesses {
        claims
            .entry(commit_from_value(witness.claimed_value()))
            .or_default()
            .push(witness);
    }
    let initial_distinct_claims = claims.len();
    let mut view = PartialDagView::new(dag);
    let mut verification = HashMap::<PinAssertionId, bool>::new();
    let mut rounds = 0usize;

    loop {
        // Each nonterminal retry removes at least one distinct claim. The
        // extra round is the final resolution (including all-invalid Absent).
        rounds += 1;
        debug_assert!(rounds <= initial_distinct_claims + 1);
        if claims.is_empty() {
            return Ok(BranchResolution::Absent);
        }

        let tips: Vec<_> = claims
            .iter()
            .map(|(commit, witnesses)| (*commit, consistent_rank_hint(witnesses)))
            .collect();
        let optimistic = match optimistic_frontier(&tips, &mut view) {
            Ok(frontier) => frontier,
            Err(error) => {
                // An unauthenticated claim may point at malformed local bytes
                // or a backend failure and trigger that error during the
                // optimistic ancestry fold. On this exceptional path, verify
                // every active exact claim. Removing all-invalid groups and
                // retrying prevents forged witnesses from poisoning a valid
                // branch; if every active group has a valid witness, the
                // storage/metadata error is genuine and remains observable.
                let invalid: Vec<_> = claims
                    .iter()
                    .filter_map(|(commit, witnesses)| {
                        claim_max_valid_rank(witnesses, &mut verification)
                            .is_none()
                            .then_some(*commit)
                    })
                    .collect();
                if invalid.is_empty() {
                    return Err(error);
                }
                for commit in invalid {
                    claims.remove(&commit);
                }
                continue;
            }
        };

        let mut invalid_frontier = Vec::new();
        let mut valid_frontier_ranks = BTreeMap::new();
        for commit in &optimistic.tips {
            let witnesses = claims
                .get(commit)
                .expect("an optimistic tip belongs to the active claim set");
            match claim_max_valid_rank(witnesses, &mut verification) {
                Some(rank) => {
                    valid_frontier_ranks.insert(*commit, rank);
                }
                None => invalid_frontier.push(*commit),
            }
        }
        if !invalid_frontier.is_empty() {
            for commit in invalid_frontier {
                claims.remove(&commit);
            }
            // A forged descendant may have hidden a real ancestor. Never
            // reuse the prior domination result: fold the surviving claims
            // again (the local parent-read cache is safe to retain).
            continue;
        }

        debug_assert!(optimistic.tips.iter().all(|tip| {
            claims
                .get(tip)
                .expect("frontier tip remains active")
                .iter()
                .any(|witness| verification.get(&witness.id()) == Some(&true))
        }));

        // Only now may absence become externally visible demand. No target or
        // missing-ancestry handle returned below originates solely from an
        // unauthenticated surviving tip.
        let mut missing_tips = HashSet::new();
        for tip in &optimistic.tips {
            if view.parents(*tip)? == ParentLookup::Missing {
                missing_tips.insert(*tip);
            }
        }
        let mut missing_tips: Vec<_> = missing_tips.into_iter().collect();
        canonicalize(&mut missing_tips);

        if !missing_tips.is_empty() {
            return Ok(BranchResolution::TipPending(TipPendingFrontier {
                tips: optimistic.tips,
                missing_tips,
            }));
        }
        if optimistic.unresolved_pair {
            return Ok(BranchResolution::Partial(PartialFrontier {
                tips: optimistic.tips,
                missing_ancestry: optimistic.missing_ancestry,
            }));
        }
        let resolved_rank = if optimistic.tips.len() == 1 {
            Some(
                *valid_frontier_ranks
                    .get(&optimistic.tips[0])
                    .expect("every complete tip has an authenticated rank"),
            )
        } else {
            BranchRank::after(optimistic.tips.iter().map(|tip| {
                *valid_frontier_ranks
                    .get(tip)
                    .expect("every complete tip has an authenticated rank")
            }))
        };
        return Ok(BranchResolution::Complete(CompleteFrontier {
            tips: optimistic.tips,
            resolved_rank,
        }));
    }
}

/// A claim receives a label hint only when every structural witness agrees.
/// A forged conflicting sibling can therefore disable an optimisation but can
/// never create one. Verification remains lazy until the claim survives.
fn consistent_rank_hint(witnesses: &[&PinAssertionWitness]) -> Option<BranchRank> {
    let mut labels = witnesses.iter().map(|witness| witness.claimed_label());
    let first = labels.next()?;
    labels
        .all(|label| label == first)
        .then(|| BranchRank::from_label(first))
}

/// Authenticate every surviving sibling and retain the greatest valid rank.
///
/// Taking the maximum means a newly authored child can advance beyond every
/// valid assertion of its parent commit. Invalid siblings remain forensic data
/// but cannot influence publication provenance.
fn claim_max_valid_rank(
    witnesses: &[&PinAssertionWitness],
    memo: &mut HashMap<PinAssertionId, bool>,
) -> Option<BranchRank> {
    witnesses
        .iter()
        .filter_map(|witness| {
            let id = witness.id();
            if let Some(valid) = memo.get(&id) {
                return valid.then(|| BranchRank::from_label(witness.claimed_label()));
            }
            let valid = witness.verified().is_ok();
            memo.insert(id, valid);
            valid.then(|| BranchRank::from_label(witness.claimed_label()))
        })
        .max()
}

struct OptimisticFrontier {
    tips: Vec<CommitHandle>,
    unresolved_pair: bool,
    missing_ancestry: Vec<CommitHandle>,
}

fn optimistic_frontier<D: PartialCommitDag>(
    tips: &[(CommitHandle, Option<BranchRank>)],
    view: &mut PartialDagView<'_, D>,
) -> Result<OptimisticFrontier, D::Error> {
    let mut dominated = vec![false; tips.len()];
    let mut unknown_pairs = Vec::new();

    for left in 0..tips.len() {
        for right in (left + 1)..tips.len() {
            let (left_commit, left_rank) = tips[left];
            let (right_commit, right_rank) = tips[right];

            // A branch rank may suppress only the direction it proves
            // impossible. It never establishes ancestry and therefore never
            // marks a claim dominated. Arbitrary labels can make us retain an
            // extra tip, but cannot make us drop a real one.
            let forward = match (left_rank, right_rank) {
                (Some(left_rank), Some(right_rank)) if left_rank >= right_rank => None,
                _ => Some(view.is_ancestor(left_commit, right_commit)?),
            };
            if forward
                .as_ref()
                .is_some_and(|walk| walk.relation == Ancestry::Yes)
            {
                dominated[left] = true;
                continue;
            }

            let reverse = match (left_rank, right_rank) {
                (Some(left_rank), Some(right_rank)) if right_rank >= left_rank => None,
                _ => Some(view.is_ancestor(right_commit, left_commit)?),
            };
            if reverse
                .as_ref()
                .is_some_and(|walk| walk.relation == Ancestry::Yes)
            {
                dominated[right] = true;
                continue;
            }

            if forward
                .as_ref()
                .is_some_and(|walk| walk.relation == Ancestry::Unknown)
                || reverse
                    .as_ref()
                    .is_some_and(|walk| walk.relation == Ancestry::Unknown)
            {
                let mut missing = forward.map(|walk| walk.missing).unwrap_or_default();
                missing.extend(reverse.map(|walk| walk.missing).unwrap_or_default());
                canonicalize(&mut missing);
                unknown_pairs.push((left, right, missing));
            }
        }
    }

    let unresolved_pair = unknown_pairs
        .iter()
        .any(|(left, right, _)| !dominated[*left] && !dominated[*right]);
    let mut frontier: Vec<_> = tips
        .iter()
        .enumerate()
        .filter_map(|(index, (commit, _))| (!dominated[index]).then_some(*commit))
        .collect();
    canonicalize(&mut frontier);

    let mut missing_ancestry = HashSet::new();
    for (left, right, pair_missing) in unknown_pairs {
        if !dominated[left] && !dominated[right] {
            missing_ancestry.extend(pair_missing);
        }
    }
    let mut missing_ancestry: Vec<_> = missing_ancestry.into_iter().collect();
    canonicalize(&mut missing_ancestry);
    Ok(OptimisticFrontier {
        tips: frontier,
        unresolved_pair,
        missing_ancestry,
    })
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
    use crate::repo::branch_pin::sign_branch_assertion;
    use crate::repo::pin_assertion::{
        reset_signature_verification_count, signature_verification_count, PinAssertion,
        SubsumptionLabel, UnverifiedPinAssertion,
    };

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

    #[derive(Default)]
    struct CountingDag {
        parents: HashMap<CommitHandle, ParentLookup>,
        calls: Vec<CommitHandle>,
    }

    impl PartialCommitDag for CountingDag {
        type Error = Infallible;

        fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error> {
            self.calls.push(commit);
            Ok(self
                .parents
                .get(&commit)
                .cloned()
                .unwrap_or(ParentLookup::Missing))
        }
    }

    struct FailingDag {
        resident: CommitHandle,
        failing: CommitHandle,
    }

    impl PartialCommitDag for FailingDag {
        type Error = ();

        fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error> {
            if commit == self.failing {
                Err(())
            } else if commit == self.resident {
                Ok(ParentLookup::Present(vec![]))
            } else {
                Ok(ParentLookup::Missing)
            }
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

    fn rank(byte: u8) -> BranchRank {
        let mut raw = [0u8; 32];
        raw[31] = byte;
        BranchRank::from_label(SubsumptionLabel::from_raw(raw))
    }

    fn rank_for_commit(commit: CommitHandle) -> BranchRank {
        rank(commit.raw[0])
    }

    fn assertion(commit: CommitHandle, rank: BranchRank) -> PinAssertion {
        sign_branch_assertion(&key(), name(3), commit, rank)
    }

    fn snapshot(commits: impl IntoIterator<Item = CommitHandle>) -> PinAssertionSnapshot {
        let key = key();
        let mut snapshot = PinAssertionSnapshot::new();
        for commit in commits {
            snapshot
                .insert(sign_branch_assertion(
                    &key,
                    name(3),
                    commit,
                    rank_for_commit(commit),
                ))
                .unwrap();
        }
        snapshot
    }

    fn ranked_snapshot(
        claims: impl IntoIterator<Item = (CommitHandle, BranchRank)>,
    ) -> PinAssertionSnapshot {
        let mut snapshot = PinAssertionSnapshot::new();
        for (commit, rank) in claims {
            snapshot.insert(assertion(commit, rank)).unwrap();
        }
        snapshot
    }

    fn structural_witness(
        commit: CommitHandle,
        corrupt_signature_byte: Option<usize>,
    ) -> UnverifiedPinAssertion {
        structural_witness_ranked(commit, rank_for_commit(commit), corrupt_signature_byte)
    }

    fn structural_witness_ranked(
        commit: CommitHandle,
        rank: BranchRank,
        corrupt_signature_byte: Option<usize>,
    ) -> UnverifiedPinAssertion {
        let mut bytes = assertion(commit, rank).encode();
        if let Some(index) = corrupt_signature_byte {
            bytes[128 + index % 64] ^= 1;
        }
        UnverifiedPinAssertion::decode_structural(bytes).unwrap()
    }

    fn structural_snapshot(
        witnesses: impl IntoIterator<Item = UnverifiedPinAssertion>,
    ) -> PinAssertionSnapshot {
        let mut snapshot = PinAssertionSnapshot::new();
        for witness in witnesses {
            snapshot.insert_unverified(witness).unwrap();
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

    fn complete_frontier(tips: impl IntoIterator<Item = CommitHandle>) -> CompleteFrontier {
        let mut tips: Vec<_> = tips.into_iter().collect();
        canonicalize(&mut tips);
        let resolved_rank = if tips.len() == 1 {
            Some(rank_for_commit(tips[0]))
        } else {
            BranchRank::after(tips.iter().copied().map(rank_for_commit))
        };
        CompleteFrontier {
            tips,
            resolved_rank,
        }
    }

    fn complete(tips: impl IntoIterator<Item = CommitHandle>) -> BranchResolution {
        BranchResolution::Complete(complete_frontier(tips))
    }

    fn candidate_tips(resolution: &BranchResolution) -> &[CommitHandle] {
        match resolution {
            BranchResolution::Absent => &[],
            BranchResolution::TipPending(frontier) => frontier.tips(),
            BranchResolution::Partial(frontier) => frontier.tips(),
            BranchResolution::Complete(frontier) => frontier.tips(),
        }
    }

    fn reachable_from(dag: &TestDag, roots: &[CommitHandle]) -> HashSet<CommitHandle> {
        let mut reachable = HashSet::new();
        let mut stack = roots.to_vec();
        while let Some(commit) = stack.pop() {
            if !reachable.insert(commit) {
                continue;
            }
            match dag.parents.get(&commit) {
                Some(ParentLookup::Present(parents)) => stack.extend(parents),
                other => panic!("generated full DAG is missing {commit:?}: {other:?}"),
            }
        }
        reachable
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
        assert_eq!(resolved, complete([c]));
    }

    #[test]
    fn honest_ranks_walk_only_the_causally_possible_direction() {
        let (parent, child) = (commit(1), commit(2));
        let mut dag = CountingDag {
            parents: HashMap::from([
                (parent, ParentLookup::Present(vec![])),
                (child, ParentLookup::Present(vec![parent])),
            ]),
            calls: Vec::new(),
        };
        let mut view = PartialDagView::new(&mut dag);
        let frontier = optimistic_frontier(
            &[(parent, Some(rank(1))), (child, Some(rank(2)))],
            &mut view,
        )
        .unwrap();

        assert_eq!(frontier.tips, vec![child]);
        assert_eq!(
            dag.calls,
            vec![child],
            "the impossible child→parent direction must not be walked"
        );
    }

    #[test]
    fn equal_ranks_skip_both_directions_between_distinct_values() {
        struct ForbiddenDag;
        impl PartialCommitDag for ForbiddenDag {
            type Error = Infallible;

            fn parents(&mut self, _: CommitHandle) -> Result<ParentLookup, Self::Error> {
                panic!("equal ranks should suppress both strict-ancestry walks")
            }
        }

        let (left, right) = (commit(1), commit(2));
        let mut dag = ForbiddenDag;
        let mut view = PartialDagView::new(&mut dag);
        let frontier =
            optimistic_frontier(&[(left, Some(rank(7))), (right, Some(rank(7)))], &mut view)
                .unwrap();
        assert_eq!(frontier.tips, vec![left, right]);
        assert!(!frontier.unresolved_pair);
    }

    #[test]
    fn a_larger_rank_never_prunes_a_divergent_smaller_rank() {
        let (left, right) = (commit(1), commit(2));
        let mut dag = CountingDag {
            parents: HashMap::from([
                (left, ParentLookup::Present(vec![])),
                (right, ParentLookup::Present(vec![])),
            ]),
            calls: Vec::new(),
        };
        let mut view = PartialDagView::new(&mut dag);
        let frontier = optimistic_frontier(
            &[(left, Some(rank(1))), (right, Some(rank(200)))],
            &mut view,
        )
        .unwrap();

        assert_eq!(frontier.tips, vec![left, right]);
        assert_eq!(dag.calls, vec![right]);
    }

    #[test]
    fn exact_values_group_before_labels_and_keep_the_maximum_valid_rank() {
        let tip = commit(4);
        let assertions = ranked_snapshot([(tip, rank(1)), (tip, rank(9))]);
        let mut dag = TestDag::default();
        present(&mut dag, tip, &[]);

        let BranchResolution::Complete(frontier) =
            resolve_branch(&assertions, &identity(), &mut dag).unwrap()
        else {
            panic!("one resident exact value must complete")
        };
        assert_eq!(frontier.tips(), &[tip]);
        assert_eq!(frontier.resolved_rank(), Some(rank(9)));

        let pin_identity =
            BranchPinDescriptor::pin_identity(identity().author(), identity().name());
        let witnesses = assertions.witnesses_for_pin(&pin_identity);
        assert_eq!(witnesses.len(), 2);
        assert_eq!(consistent_rank_hint(&witnesses), None);
    }

    #[test]
    fn invalid_high_rank_siblings_do_not_influence_provenance() {
        let tip = commit(4);
        let assertions = structural_snapshot([
            structural_witness_ranked(tip, rank(3), None),
            structural_witness_ranked(tip, rank(250), Some(17)),
        ]);
        let mut dag = TestDag::default();
        present(&mut dag, tip, &[]);

        reset_signature_verification_count();
        let BranchResolution::Complete(frontier) =
            resolve_branch(&assertions, &identity(), &mut dag).unwrap()
        else {
            panic!("the valid sibling must license the exact claim")
        };
        assert_eq!(frontier.resolved_rank(), Some(rank(3)));
        assert_eq!(signature_verification_count(), 2);
    }

    #[test]
    fn invalid_claims_cannot_turn_optimistic_read_failures_into_branch_errors() {
        let (resident, forged) = (commit(1), commit(2));
        let assertions = structural_snapshot([
            structural_witness_ranked(resident, rank(1), None),
            structural_witness_ranked(forged, rank(2), Some(17)),
        ]);
        let mut dag = FailingDag {
            resident,
            failing: forged,
        };

        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag),
            Ok(complete([resident]))
        );
    }

    #[test]
    fn valid_claims_preserve_optimistic_read_failures() {
        let (resident, failing) = (commit(1), commit(2));
        let assertions = ranked_snapshot([(resident, rank(1)), (failing, rank(2))]);
        let mut dag = FailingDag { resident, failing };

        assert_eq!(resolve_branch(&assertions, &identity(), &mut dag), Err(()));
    }

    #[test]
    fn divergent_rank_overflow_is_explicit() {
        let (left, right) = (commit(1), commit(2));
        let full = BranchRank::from_label(SubsumptionLabel::from_raw([0xFF; 32]));
        let assertions = ranked_snapshot([(left, full), (right, BranchRank::ROOT)]);
        let mut dag = TestDag::default();
        present(&mut dag, left, &[]);
        present(&mut dag, right, &[]);

        let BranchResolution::Complete(frontier) =
            resolve_branch(&assertions, &identity(), &mut dag).unwrap()
        else {
            panic!("resident divergent roots must complete")
        };
        assert_eq!(frontier.tips(), &[left, right]);
        assert_eq!(frontier.resolved_rank(), None);
    }

    #[test]
    fn missing_unneeded_ancestry_does_not_block_a_proven_maximum() {
        let (root, child, missing) = (commit(1), commit(2), commit(9));
        let mut dag = TestDag::default();
        present(&mut dag, root, &[]);
        present(&mut dag, child, &[root, missing]);
        let assertions = snapshot([root, child]);

        let resolved = resolve_branch(&assertions, &identity(), &mut dag).unwrap();
        assert_eq!(resolved, complete([child]));

        present(&mut dag, missing, &[]);
        let completed = resolve_branch(&assertions, &identity(), &mut dag).unwrap();
        assert_eq!(completed, complete([child]));
    }

    #[test]
    fn missing_ancestry_needed_to_compare_surviving_tips_is_partial() {
        let (left, right, missing) = (commit(1), commit(2), commit(9));
        let mut dag = TestDag::default();
        present(&mut dag, left, &[missing]);
        present(&mut dag, right, &[]);
        // Only right→left can still be causal under these ranks, and that
        // exact walk crosses `left`'s missing parent.
        let assertions = ranked_snapshot([(left, rank(2)), (right, rank(1))]);

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
            complete([left, right])
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
            complete([tip])
        );
    }

    #[test]
    fn singleton_resolution_reads_the_tip_but_never_walks_its_closure() {
        struct TipOnlyDag {
            tip: CommitHandle,
            forbidden_parent: CommitHandle,
            calls: usize,
        }

        impl PartialCommitDag for TipOnlyDag {
            type Error = Infallible;

            fn parents(&mut self, commit: CommitHandle) -> Result<ParentLookup, Self::Error> {
                self.calls += 1;
                assert_eq!(
                    commit, self.tip,
                    "singleton resolution resurrected an eager ancestry-closure walk"
                );
                Ok(ParentLookup::Present(vec![self.forbidden_parent]))
            }
        }

        let tip = commit(4);
        let mut dag = TipOnlyDag {
            tip,
            forbidden_parent: commit(5),
            calls: 0,
        };
        assert_eq!(
            resolve_branch(&snapshot([tip]), &identity(), &mut dag).unwrap(),
            complete([tip])
        );
        assert_eq!(dag.calls, 1, "the surviving tip is checked exactly once");
    }

    #[test]
    fn backend_errors_are_never_downgraded_to_missing_metadata() {
        struct FailingDag;

        impl PartialCommitDag for FailingDag {
            type Error = &'static str;

            fn parents(&mut self, _: CommitHandle) -> Result<ParentLookup, Self::Error> {
                Err("backend failure")
            }
        }

        assert_eq!(
            resolve_branch(&snapshot([commit(4)]), &identity(), &mut FailingDag).unwrap_err(),
            "backend failure"
        );
    }

    #[test]
    fn all_invalid_witnesses_resolve_absent_without_dag_reads_or_demand() {
        struct ForbiddenDag;

        impl PartialCommitDag for ForbiddenDag {
            type Error = Infallible;

            fn parents(&mut self, _: CommitHandle) -> Result<ParentLookup, Self::Error> {
                panic!("an unauthenticated singleton created local or fetch demand")
            }
        }

        let tip = commit(9);
        let assertions = structural_snapshot([
            structural_witness(tip, Some(0)),
            structural_witness(tip, Some(1)),
        ]);
        reset_signature_verification_count();
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut ForbiddenDag).unwrap(),
            BranchResolution::Absent
        );
        assert_eq!(signature_verification_count(), 2);
    }

    #[test]
    fn forged_dominator_drops_and_promotes_the_buried_real_tip() {
        let (real, forged) = (commit(1), commit(2));
        let assertions = structural_snapshot([
            structural_witness(real, None),
            structural_witness(forged, Some(7)),
        ]);
        let mut dag = TestDag::default();
        present(&mut dag, real, &[]);
        present(&mut dag, forged, &[real]);

        reset_signature_verification_count();
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            complete([real])
        );
        assert_eq!(
            signature_verification_count(),
            2,
            "each successive optimistic frontier is checked once"
        );
    }

    #[test]
    fn one_valid_witness_licenses_a_claim_among_invalid_siblings() {
        let tip = commit(4);
        let assertions = structural_snapshot([
            structural_witness(tip, Some(3)),
            structural_witness(tip, None),
            structural_witness(tip, Some(19)),
        ]);
        let mut dag = TestDag::default();
        present(&mut dag, tip, &[]);

        reset_signature_verification_count();
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            complete([tip])
        );
        assert_eq!(signature_verification_count(), 3);
        let pin_identity =
            BranchPinDescriptor::pin_identity(identity().author(), identity().name());
        assert_eq!(
            assertions.for_pin(&pin_identity).len(),
            1,
            "the two invalid siblings never escape the snapshot as public assertions"
        );
    }

    #[test]
    fn resolution_verifies_frontier_scale_not_snapshot_scale() {
        let commits: Vec<_> = (1..=64).map(commit).collect();
        let assertions = structural_snapshot(
            commits
                .iter()
                .copied()
                .map(|commit| structural_witness(commit, None)),
        );
        let mut dag = TestDag::default();
        for (index, commit) in commits.iter().copied().enumerate() {
            let parents = index
                .checked_sub(1)
                .map(|parent| vec![commits[parent]])
                .unwrap_or_default();
            present(&mut dag, commit, &parents);
        }

        reset_signature_verification_count();
        assert_eq!(
            resolve_branch(&assertions, &identity(), &mut dag).unwrap(),
            complete([*commits.last().unwrap()])
        );
        assert_eq!(
            signature_verification_count(),
            1,
            "a 64-claim linear history has a one-claim optimistic frontier"
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
        let ResolvedHead::Synthetic(second) = complete_frontier([c, b, a]).resolved_head() else {
            panic!("three tips must synthesize a merge")
        };
        assert_eq!(first, second);

        let pair = merge_metadata([a, b]).to_blob().get_handle();
        let nested = merge_metadata([pair, c]).to_blob();
        assert_ne!(
            first, nested,
            "a nested pairwise merge must not masquerade as the canonical flat frontier"
        );
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
        fn arbitrary_labels_only_overapproximate_the_exact_frontier(
            node_count in 1usize..9,
            parent_masks in prop::collection::vec(any::<u16>(), 8),
            assertion_mask in 1u16..=u16::MAX,
            labels in prop::collection::vec(any::<u8>(), 8),
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

            let selected: Vec<_> = commits
                .iter()
                .enumerate()
                .filter_map(|(index, commit)| {
                    (assertion_mask & (1u16 << index) != 0).then_some((index, *commit))
                })
                .collect();
            prop_assume!(!selected.is_empty());

            let exact_input: Vec<_> = selected
                .iter()
                .map(|(_, commit)| (*commit, None))
                .collect();
            let guided_input: Vec<_> = selected
                .iter()
                .map(|(index, commit)| (*commit, Some(rank(labels[*index]))))
                .collect();
            let mut exact_dag = dag.clone();
            let mut exact_view = PartialDagView::new(&mut exact_dag);
            let exact = optimistic_frontier(&exact_input, &mut exact_view).unwrap();
            let mut guided_view = PartialDagView::new(&mut dag);
            let guided = optimistic_frontier(&guided_input, &mut guided_view).unwrap();

            prop_assert!(
                exact.tips.iter().all(|tip| guided.tips.contains(tip)),
                "label guidance discarded an exact maximal tip"
            );
        }

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

        #[test]
        fn adding_ancestry_only_refines_the_conservative_frontier(
            node_count in 1usize..9,
            parent_masks in prop::collection::vec(any::<u16>(), 8),
            assertion_mask in 1u16..=u16::MAX,
            coarse_missing_mask in any::<u16>(),
            refinement_mask in any::<u16>(),
        ) {
            let commits: Vec<_> = (0..node_count)
                .map(|index| commit(index as u8 + 1))
                .collect();
            let mut full = TestDag::default();
            for index in 0..node_count {
                let lower = if index == 0 { 0 } else { (1u16 << index) - 1 };
                let parents: Vec<_> = (0..index)
                    .filter(|parent| parent_masks[index] & (1u16 << parent) & lower != 0)
                    .map(|parent| commits[parent])
                    .collect();
                present(&mut full, commits[index], &parents);
            }
            let assertions: Vec<_> = commits
                .iter()
                .enumerate()
                .filter_map(|(index, commit)| {
                    (assertion_mask & (1u16 << index) != 0).then_some(*commit)
                })
                .collect();
            prop_assume!(!assertions.is_empty());
            let snapshot = snapshot(assertions.iter().copied());

            let mut coarse = full.clone();
            let mut refined = full.clone();
            let refined_missing_mask = coarse_missing_mask & refinement_mask;
            for (index, commit) in commits.iter().enumerate() {
                if coarse_missing_mask & (1u16 << index) != 0 {
                    coarse.parents.remove(commit);
                }
                if refined_missing_mask & (1u16 << index) != 0 {
                    refined.parents.remove(commit);
                }
            }

            let coarse_resolution = resolve_branch(&snapshot, &identity(), &mut coarse).unwrap();
            let refined_resolution = resolve_branch(&snapshot, &identity(), &mut refined).unwrap();
            let full_resolution = resolve_branch(&snapshot, &identity(), &mut full).unwrap();
            let coarse_tips = candidate_tips(&coarse_resolution);
            let refined_tips = candidate_tips(&refined_resolution);
            let full_tips = candidate_tips(&full_resolution);

            prop_assert!(
                refined_tips.iter().all(|tip| coarse_tips.contains(tip)),
                "adding ancestry introduced a new candidate"
            );
            prop_assert!(
                full_tips.iter().all(|tip| refined_tips.contains(tip)),
                "partial ancestry discarded a truly maximal candidate"
            );
            prop_assert_eq!(
                reachable_from(&full, coarse_tips),
                reachable_from(&full, &assertions),
                "the coarse conservative view changed eventual content"
            );
            prop_assert_eq!(
                reachable_from(&full, refined_tips),
                reachable_from(&full, &assertions),
                "the refined conservative view changed eventual content"
            );
            if matches!(&coarse_resolution, BranchResolution::Complete(_)) {
                prop_assert_eq!(&coarse_resolution, &refined_resolution);
            }
            if matches!(&refined_resolution, BranchResolution::Complete(_)) {
                prop_assert_eq!(&refined_resolution, &full_resolution);
            }
        }

        #[test]
        fn lazy_verification_matches_the_valid_claim_fold_in_any_insertion_order(
            node_count in 1usize..9,
            parent_masks in prop::collection::vec(any::<u16>(), 8),
            assertion_mask in 1u16..=u16::MAX,
            invalid_mask in any::<u16>(),
            missing_mask in any::<u16>(),
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
                if missing_mask & (1u16 << index) == 0 {
                    present(&mut dag, commits[index], &parents);
                }
            }

            let selected: Vec<_> = commits
                .iter()
                .enumerate()
                .filter_map(|(index, commit)| {
                    (assertion_mask & (1u16 << index) != 0).then_some((index, *commit))
                })
                .collect();
            prop_assume!(!selected.is_empty());

            let witnesses: Vec<_> = selected
                .iter()
                .map(|(index, commit)| {
                    structural_witness(
                        *commit,
                        (invalid_mask & (1u16 << index) != 0).then_some(*index),
                    )
                })
                .collect();
            let forward = structural_snapshot(witnesses.iter().copied());
            let reverse = structural_snapshot(witnesses.iter().rev().copied());
            prop_assert_eq!(&forward, &reverse);

            let valid_commits: Vec<_> = selected
                .iter()
                .filter_map(|(index, commit)| {
                    (invalid_mask & (1u16 << index) == 0).then_some(*commit)
                })
                .collect();
            let expected_snapshot = snapshot(valid_commits.clone());

            reset_signature_verification_count();
            let left = resolve_branch(&forward, &identity(), &mut dag.clone()).unwrap();
            prop_assert!(signature_verification_count() <= witnesses.len());
            prop_assert!(candidate_tips(&left)
                .iter()
                .all(|tip| valid_commits.contains(tip)));

            reset_signature_verification_count();
            let right = resolve_branch(&reverse, &identity(), &mut dag.clone()).unwrap();
            prop_assert!(signature_verification_count() <= witnesses.len());

            let eager_valid =
                resolve_branch(&expected_snapshot, &identity(), &mut dag.clone()).unwrap();
            prop_assert_eq!(&left, &right);
            prop_assert_eq!(left, eager_valid);
        }
    }
}
