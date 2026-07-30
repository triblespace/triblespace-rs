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

    /// Proposes each row's own peer value — the peer is bound across the
    /// whole batch or unbound across the whole batch, but the *value*
    /// differs per row, so this is one push per row.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let peer = if variable == self.a {
            self.b
        } else if variable == self.b {
            self.a
        } else {
            return;
        };
        for row in 0..frontier.len() {
            if let Some(v) = frontier.row(row).get(peer) {
                proposals.open(row as u32);
                proposals.push(*v);
            }
        }
    }

    /// Retains only proposals that match their own row's peer binding.
    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let peer = if variable == self.a {
            self.b
        } else if variable == self.b {
            self.a
        } else {
            return;
        };
        cands.for_each_parent(|row, run| {
            let Some(peer) = frontier.row(row as usize).get(peer) else {
                return;
            };
            run.retain(|value| value == peer);
        });
    }

    /// Returns `false` when both variables are bound to different values.
    fn satisfied(&self, binding: &Binding) -> bool {
        match (binding.get(self.a), binding.get(self.b)) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }
}
