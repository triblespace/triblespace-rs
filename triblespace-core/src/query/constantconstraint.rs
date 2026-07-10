use super::*;

/// Pins a variable to a single known value.
///
/// Created by [`Variable::is`]. The estimate is always 1, propose yields
/// exactly the constant, and confirm retains only matching proposals.
/// This is the simplest possible constraint. Note that `pattern!` does
/// not use it for attribute constants or literal values — those are
/// folded into the pattern constraint as constant
/// [`Term`](crate::query::Term)s and never become variables.
pub struct ConstantConstraint {
    variable: VariableId,
    constant: RawInline,
}

impl ConstantConstraint {
    /// Creates a constraint that binds `variable` to `constant`.
    pub fn new<T: InlineEncoding>(variable: Variable<T>, constant: Inline<T>) -> Self {
        ConstantConstraint {
            variable: variable.index,
            constant: constant.raw,
        }
    }
}

impl<'a> Constraint<'a> for ConstantConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    /// Always returns `Some(1)` for the constrained variable.
    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable == variable {
            Some(1)
        } else {
            None
        }
    }

    /// Pushes the single constant value.
    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable == variable {
            proposals.push(self.constant);
        }
    }

    /// Retains only proposals that match the constant exactly.
    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable == variable {
            proposals.retain(|v| *v == self.constant);
        }
    }

    /// Returns `false` when the variable is bound to a different value.
    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable) {
            Some(v) => *v == self.constant,
            None => true,
        }
    }
}
