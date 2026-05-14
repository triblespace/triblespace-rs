use super::*;

/// Constrains two variables to have the same value.
///
/// Used to express variable equality when two positions in a triple
/// share the same logical variable but need distinct [`VariableId`]s
/// for the `TribleSetConstraint`
/// (which assumes its three positions have distinct ids).
///
/// The macro layer emits this automatically when a `_?var` appears in
/// both the entity and value positions of the same triple.
pub struct EqualityConstraint {
    a: VariableId,
    b: VariableId,
}

impl EqualityConstraint {
    /// Creates a constraint requiring `a` and `b` to be bound to the
    /// same raw value.
    pub fn new(a: VariableId, b: VariableId) -> Self {
        EqualityConstraint { a, b }
    }
}

impl<'c> Constraint<'c> for EqualityConstraint {
    fn variables(&self) -> VariableSet {
        let mut vs = VariableSet::new_empty();
        vs.set(self.a);
        vs.set(self.b);
        vs
    }

    /// Returns `Some(1)` when the peer variable is already bound
    /// (exactly one candidate). Returns `None` when the peer is
    /// unbound — the constraint has no independent opinion about the
    /// variable's cardinality and defers to other constraints in the
    /// intersection. This is safe as long as each variable also appears
    /// in at least one other constraint (which the macro desugaring
    /// guarantees).
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        if variable == self.a {
            if binding.get(self.b).is_some() {
                Some(1)
            } else {
                None
            }
        } else if variable == self.b {
            if binding.get(self.a).is_some() {
                Some(1)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// When the peer variable is bound, proposes its value.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.a {
            if let Some(v) = binding.get(self.b) {
                proposals.push(*v);
            }
        } else if variable == self.b {
            if let Some(v) = binding.get(self.a) {
                proposals.push(*v);
            }
        }
    }

    /// Retains only proposals that match the peer variable's binding.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.a {
            if let Some(peer) = binding.get(self.b) {
                proposals.retain(|v| v == peer);
            }
        } else if variable == self.b {
            if let Some(peer) = binding.get(self.a) {
                proposals.retain(|v| v == peer);
            }
        }
    }

    /// Returns `false` when both variables are bound to different values.
    fn satisfied(&self, binding: &Binding) -> bool {
        match (binding.get(self.a), binding.get(self.b)) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }
}
