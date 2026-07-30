use super::*;

/// Pins a variable to a single known value.
///
/// Created by [`Variable::is`]. The estimate is always 1, propose yields
/// exactly the constant, and confirm retains only matching proposals.
/// This is the simplest possible constraint and is used by the macro
/// layer to bind attribute IDs and literal values.
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

    /// Pushes the single constant value, once per frontier row.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        if self.variable == variable {
            for row in 0..frontier.len() {
                proposals.open(row as u32);
                proposals.push(self.constant);
            }
        }
    }

    /// Retains only proposals that match the constant exactly. The verdict
    /// does not depend on the parent binding, so the tags are ignored.
    fn confirm(&self, variable: VariableId, _frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        if self.variable == variable {
            for i in 0..cands.len() {
                let v = &cands.values()[i];
                if cands.is_live(i) && *v != self.constant {
                    cands.kill(i);
                }
            }
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
