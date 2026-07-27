use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    Binding, Candidates, Constraint, ProposalBuffer, ProposeCursor, RawTerm, Term, VariableId,
    VariableSet,
};

use crate::PathIndex;

/// A two-endpoint view of a [`PathIndex`] for the classic query solver.
#[derive(Clone, Copy, Debug)]
pub struct PathConstraint<'a> {
    index: &'a PathIndex,
    start: RawTerm,
    end: RawTerm,
}

#[derive(Clone, Copy, Debug)]
enum CandidateKind {
    Diagonal,
    Reaching(RawInline),
    Starts,
    ReachableFrom(RawInline),
    Ends,
}

impl<'a> PathConstraint<'a> {
    /// Constrains `start` and `end` to an accepted pair in `index`.
    pub fn new<S: InlineEncoding, E: InlineEncoding>(
        index: &'a PathIndex,
        start: impl Into<Term<S>>,
        end: impl Into<Term<E>>,
    ) -> Self {
        Self {
            index,
            start: start.into().erase(),
            end: end.into().erase(),
        }
    }

    fn candidate_kind(&self, variable: VariableId, binding: &Binding) -> Option<CandidateKind> {
        let at_start = self.start.is_var(variable);
        let at_end = self.end.is_var(variable);
        match (at_start, at_end) {
            (false, false) => None,
            (true, true) => Some(CandidateKind::Diagonal),
            (true, false) => Some(match self.end.position_value(binding).copied() {
                Some(end) => CandidateKind::Reaching(end),
                None => CandidateKind::Starts,
            }),
            (false, true) => Some(match self.start.position_value(binding).copied() {
                Some(start) => CandidateKind::ReachableFrom(start),
                None => CandidateKind::Ends,
            }),
        }
    }

    fn candidate_count(&self, kind: CandidateKind) -> usize {
        match kind {
            CandidateKind::Diagonal => self.index.diagonal().count(),
            CandidateKind::Reaching(end) => self.index.reaching(&end).count(),
            CandidateKind::Starts => self.index.starts().count(),
            CandidateKind::ReachableFrom(start) => self.index.reachable_from(&start).count(),
            CandidateKind::Ends => self.index.ends().count(),
        }
    }

    fn propose_candidates(&self, kind: CandidateKind, proposals: &mut ProposalBuffer) {
        match kind {
            CandidateKind::Diagonal => proposals.extend(self.index.diagonal()),
            CandidateKind::Reaching(end) => proposals.extend(self.index.reaching(&end)),
            CandidateKind::Starts => proposals.extend(self.index.starts()),
            CandidateKind::ReachableFrom(start) => {
                proposals.extend(self.index.reachable_from(&start));
            }
            CandidateKind::Ends => proposals.extend(self.index.ends()),
        }
    }

    fn candidate_contains(&self, kind: CandidateKind, candidate: &RawInline) -> bool {
        match kind {
            CandidateKind::Diagonal => self.index.contains(candidate, candidate),
            CandidateKind::Reaching(end) => self.index.contains(candidate, &end),
            CandidateKind::Starts => self.index.reachable_from(candidate).next().is_some(),
            CandidateKind::ReachableFrom(start) => self.index.contains(&start, candidate),
            CandidateKind::Ends => self.index.reaching(candidate).next().is_some(),
        }
    }

    fn propose_candidate_chunk(
        &self,
        kind: CandidateKind,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        match kind {
            CandidateKind::Diagonal => {
                propose_chunk_from(self.index.diagonal(), cursor, budget, proposals)
            }
            CandidateKind::Reaching(end) => {
                propose_chunk_from(self.index.reaching(&end), cursor, budget, proposals)
            }
            CandidateKind::Starts => {
                propose_chunk_from(self.index.starts(), cursor, budget, proposals)
            }
            CandidateKind::ReachableFrom(start) => {
                propose_chunk_from(self.index.reachable_from(&start), cursor, budget, proposals)
            }
            CandidateKind::Ends => propose_chunk_from(self.index.ends(), cursor, budget, proposals),
        }
    }

    fn is_same_unbound_variable(&self, binding: &Binding) -> bool {
        match (self.start, self.end) {
            (RawTerm::Var(start), RawTerm::Var(end)) if start == end => {
                binding.get(start).is_none()
            }
            _ => false,
        }
    }
}

impl<'a> Constraint<'a> for PathConstraint<'a> {
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        self.start.add_to(&mut variables);
        self.end.add_to(&mut variables);
        variables
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.candidate_kind(variable, binding)
            .map(|kind| self.candidate_count(kind))
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        if let Some(kind) = self.candidate_kind(variable, binding) {
            self.propose_candidates(kind, proposals);
        }
    }

    fn propose_chunk(
        &self,
        variable: VariableId,
        binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        let Some(kind) = self.candidate_kind(variable, binding) else {
            return false;
        };
        self.propose_candidate_chunk(kind, cursor, budget, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if let Some(kind) = self.candidate_kind(variable, binding) {
            cands.retain(|candidate| self.candidate_contains(kind, candidate));
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let start = self.start.position_value(binding);
        let end = self.end.position_value(binding);
        match (start, end) {
            (Some(start), Some(end)) => self.index.contains(start, end),
            (Some(start), None) => self.index.reachable_from(start).next().is_some(),
            (None, Some(end)) => self.index.reaching(end).next().is_some(),
            (None, None) if self.is_same_unbound_variable(binding) => {
                self.index.diagonal().next().is_some()
            }
            (None, None) => self.index.metrics().accepted_pairs != 0,
        }
    }
}

fn propose_chunk_from(
    values: impl Iterator<Item = RawInline>,
    cursor: &mut ProposeCursor,
    budget: usize,
    proposals: &mut ProposalBuffer,
) -> bool {
    let resume_after = cursor.started.then_some(cursor.key);
    let mut values = values
        .skip_while(move |value| resume_after.is_some_and(|resume_after| value <= &resume_after));
    if budget == 0 {
        return values.next().is_some();
    }
    cursor.started = true;

    for _ in 0..budget {
        let Some(value) = values.next() else {
            return false;
        };
        cursor.key = value;
        proposals.push(value);
    }
    values.next().is_some()
}

impl PathIndex {
    /// Creates a classic query constraint over this path relation.
    pub fn constraint<'a, S: InlineEncoding, E: InlineEncoding>(
        &'a self,
        start: impl Into<Term<S>>,
        end: impl Into<Term<E>>,
    ) -> PathConstraint<'a> {
        PathConstraint::new(self, start, end)
    }
}
