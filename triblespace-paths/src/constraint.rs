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

    fn candidates(&self, variable: VariableId, binding: &Binding) -> Option<&[RawInline]> {
        let at_start = self.start.is_var(variable);
        let at_end = self.end.is_var(variable);
        match (at_start, at_end) {
            (false, false) => None,
            (true, true) => Some(self.index.diagonal()),
            (true, false) => Some(match self.end.position_value(binding) {
                Some(end) => self.index.reaching(end),
                None => self.index.starts(),
            }),
            (false, true) => Some(match self.start.position_value(binding) {
                Some(start) => self.index.reachable_from(start),
                None => self.index.ends(),
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
        self.candidates(variable, binding).map(<[RawInline]>::len)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        if let Some(values) = self.candidates(variable, binding) {
            proposals.extend_from_slice(values);
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
        let Some(values) = self.candidates(variable, binding) else {
            return false;
        };
        let start = if cursor.started {
            values.partition_point(|value| value <= &cursor.key)
        } else {
            0
        };
        if budget == 0 {
            return start < values.len();
        }
        cursor.started = true;

        let end = start.saturating_add(budget).min(values.len());
        proposals.extend_from_slice(&values[start..end]);
        if end > start {
            cursor.key = values[end - 1];
        }
        end < values.len()
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if let Some(values) = self.candidates(variable, binding) {
            cands.retain(|candidate| values.binary_search(candidate).is_ok());
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let start = self.start.position_value(binding);
        let end = self.end.position_value(binding);
        match (start, end) {
            (Some(start), Some(end)) => self.index.contains(start, end),
            (Some(start), None) => !self.index.reachable_from(start).is_empty(),
            (None, Some(end)) => !self.index.reaching(end).is_empty(),
            (None, None) if self.is_same_unbound_variable(binding) => {
                !self.index.diagonal().is_empty()
            }
            (None, None) => self.index.metrics().accepted_pairs != 0,
        }
    }
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
