//! Classic scalar protocol implementations for the common diagnostic wrappers.

use crate::debug::query::{DebugConstraint, EstimateOverrideConstraint};
use crate::inline::RawInline;

use super::{Binding, Constraint, VariableId, VariableSet};

impl<'a, C: Constraint<'a>> Constraint<'a> for DebugConstraint<C> {
    fn variables(&self) -> VariableSet {
        self.constraint.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraint.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.record.borrow_mut().push(variable);
        self.constraint.propose(variable, binding, proposals);
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.constraint.confirm(variable, binding, proposals);
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.constraint.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraint.influence(variable)
    }
}

impl<'a, C: Constraint<'a>> Constraint<'a> for EstimateOverrideConstraint<C> {
    fn variables(&self) -> VariableSet {
        self.constraint.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.estimates[variable].or_else(|| self.constraint.estimate(variable, binding))
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.constraint.propose(variable, binding, proposals);
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.constraint.confirm(variable, binding, proposals);
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        self.constraint.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraint.influence(variable)
    }
}
