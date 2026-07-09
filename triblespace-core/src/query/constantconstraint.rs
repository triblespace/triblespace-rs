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

    /// Always estimates exactly one candidate, for every row.
    fn estimate(&self, variable: VariableId, view: &RowsView<'_>, out: &mut EstimateSink<'_>) -> bool {
        if self.variable != variable {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    /// Proposes the single constant value for every row.
    fn propose(&self, variable: VariableId, view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
        if self.variable == variable {
            for i in 0..view.len() as u32 {
                candidates.push(i, self.constant);
            }
        }
    }

    /// The constant is binding-independent, so confirm is a single retain
    /// over the whole frontier — no per-row work at all.
    fn confirm(&self, variable: VariableId, _view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
        if self.variable == variable {
            candidates.retain(|_, v| *v == self.constant);
        }
    }

    /// Returns `false` when any row binds the variable to another value.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable) {
            Some(col) => view.iter().all(|row| row[col] == self.constant),
            None => true,
        }
    }
}
