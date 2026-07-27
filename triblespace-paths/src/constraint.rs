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

    fn candidate_ordinals<'b>(
        &'b self,
        variable: VariableId,
        binding: &Binding,
    ) -> Option<&'b [u32]> {
        let at_start = self.start.is_var(variable);
        let at_end = self.end.is_var(variable);
        match (at_start, at_end) {
            (false, false) => None,
            (true, true) => Some(self.index.diagonal_ordinals()),
            (true, false) => Some(match self.end.position_value(binding) {
                Some(end) => self.index.reverse_ordinals(end),
                None => self.index.starts_ordinals(),
            }),
            (false, true) => Some(match self.start.position_value(binding) {
                Some(start) => self.index.forward_ordinals(start),
                None => self.index.ends_ordinals(),
            }),
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
        self.candidate_ordinals(variable, binding).map(<[u32]>::len)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        if let Some(ordinals) = self.candidate_ordinals(variable, binding) {
            proposals.extend(self.index.values(ordinals));
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
        let Some(ordinals) = self.candidate_ordinals(variable, binding) else {
            return false;
        };
        propose_chunk_from(self.index.values(ordinals), cursor, budget, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if let Some(ordinals) = self.candidate_ordinals(variable, binding) {
            cands.retain(|candidate| self.index.ordinals_contain(ordinals, candidate));
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let start = self.start.position_value(binding);
        let end = self.end.position_value(binding);
        match (start, end) {
            (Some(start), Some(end)) => self.index.contains(start, end),
            (Some(start), None) => !self.index.forward_ordinals(start).is_empty(),
            (None, Some(end)) => !self.index.reverse_ordinals(end).is_empty(),
            (None, None) if self.is_same_unbound_variable(binding) => {
                !self.index.diagonal_ordinals().is_empty()
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
