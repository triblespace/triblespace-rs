use triblespace_core::inline::InlineEncoding;
use triblespace_core::query::{
    Binding, Candidates, Constraint, Frontier, ProposalBuffer, RawTerm, Term, VariableId,
    VariableSet,
};

use crate::PathIndex;

/// A two-endpoint view of a [`PathIndex`] for the ordinary query solver.
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

    fn candidates(&self, variable: VariableId, binding: &Binding) -> Option<&[u32]> {
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
        matches!(
            (self.start, self.end),
            (RawTerm::Var(start), RawTerm::Var(end))
                if start == end && binding.get(start).is_none()
        )
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
        self.candidates(variable, binding).map(<[u32]>::len)
    }

    /// Proposes each row's own ordinal run. Which endpoint is bound is
    /// uniform across the batch; the bound *value* is not, so the ordinal
    /// slice is looked up once per row.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        for row in 0..frontier.len() {
            if let Some(ordinals) = self.candidates(variable, &frontier.row(row)) {
                proposals.open(row as u32);
                proposals.extend(self.index.values(ordinals));
            }
        }
    }

    /// Confirms each candidate against its own row's ordinal run — one
    /// lookup per run of equal parent tags rather than one per candidate.
    fn confirm(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        candidates: &mut Candidates<'_>,
    ) {
        candidates.for_each_parent(|row, run| {
            if let Some(ordinals) = self.candidates(variable, &frontier.row(row as usize)) {
                run.retain(|candidate| self.index.ordinal_in(ordinals, candidate));
            }
        });
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
            (None, None) => self.index.accepted_pair_count() != 0,
        }
    }
}

impl PathIndex {
    /// Creates an ordinary query constraint over this path relation.
    pub fn constraint<'a, S: InlineEncoding, E: InlineEncoding>(
        &'a self,
        start: impl Into<Term<S>>,
        end: impl Into<Term<E>>,
    ) -> PathConstraint<'a> {
        PathConstraint::new(self, start, end)
    }
}
