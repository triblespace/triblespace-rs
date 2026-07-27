use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::query::{
    Binding, Candidates, Constraint, ProposalBuffer, ProposeCursor, RawTerm, Term, VariableId,
    VariableSet,
};

use crate::index::bit_count;
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

    fn candidate_bits<'b>(&'b self, variable: VariableId, binding: &Binding) -> Option<&'b [u64]> {
        let at_start = self.start.is_var(variable);
        let at_end = self.end.is_var(variable);
        match (at_start, at_end) {
            (false, false) => None,
            (true, true) => Some(self.index.diagonal_bits()),
            (true, false) => Some(match self.end.position_value(binding) {
                Some(end) => self.index.reverse_bits(end),
                None => self.index.starts_bits(),
            }),
            (false, true) => Some(match self.start.position_value(binding) {
                Some(start) => self.index.forward_bits(start),
                None => self.index.ends_bits(),
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
        self.candidate_bits(variable, binding).map(bit_count)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        if let Some(bits) = self.candidate_bits(variable, binding) {
            proposals.extend(self.index.values(bits));
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
        let Some(bits) = self.candidate_bits(variable, binding) else {
            return false;
        };
        propose_chunk_from(self.index.values(bits), cursor, budget, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        if let Some(bits) = self.candidate_bits(variable, binding) {
            cands.retain(|candidate| self.index.bits_contain(bits, candidate));
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let start = self.start.position_value(binding);
        let end = self.end.position_value(binding);
        match (start, end) {
            (Some(start), Some(end)) => self.index.contains(start, end),
            (Some(start), None) => bit_count(self.index.forward_bits(start)) != 0,
            (None, Some(end)) => bit_count(self.index.reverse_bits(end)) != 0,
            (None, None) if self.is_same_unbound_variable(binding) => {
                bit_count(self.index.diagonal_bits()) != 0
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
