use super::*;

/// Hides variables from the outer query while preserving internal joins.
///
/// Created by the [`ignore!`](crate::ignore) macro. The wrapped constraint
/// still constrains the hidden variables internally — they participate in
/// estimate, propose, and confirm — but the engine does not see them in
/// [`variables`](Constraint::variables), so they are neither bound from
/// outside nor projected into results.
///
/// This is useful when a multi-column constraint (like a triple pattern)
/// should enforce a join condition without exposing one of its positions
/// to the caller.
pub struct IgnoreConstraint<'a> {
    ignored: VariableSet,
    constraint: Box<dyn Constraint<'a> + Send + Sync + 'a>,
}

impl<'a> IgnoreConstraint<'a> {
    /// Wraps `constraint`, hiding every variable in `ignored` from the
    /// outer query.
    pub fn new(ignored: VariableSet, constraint: Box<dyn Constraint<'a> + Send + Sync + 'a>) -> Self {
        IgnoreConstraint {
            ignored,
            constraint,
        }
    }
}

impl<'a> Constraint<'a> for IgnoreConstraint<'a> {
    /// Returns the inner constraint's variables minus the ignored set.
    fn variables(&self) -> VariableSet {
        self.constraint.variables().subtract(self.ignored)
    }

    /// Delegates to the inner constraint. Ignored variables are still
    /// estimated normally — they participate internally, just not in the
    /// outer variable set.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraint.estimate(variable, binding)
    }

    /// Delegates to the inner constraint.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.constraint.propose(variable, binding, proposals);
    }

    /// Delegates to the inner constraint.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        self.constraint.confirm(variable, binding, proposals)
    }
}

/// Wraps a constraint while hiding one or more variables from the outer
/// query.
///
/// Hidden variables still participate in internal joins but are not
/// projected into results. Useful when a multi-column constraint should
/// enforce a join without exposing a helper variable.
///
/// ```rust,ignore
/// ignore!((helper), set.pattern(e, a, helper))
/// ```
#[macro_export]
macro_rules! ignore {
    (($($Var:ident),+), $c:expr) => {{
        let ctx = __local_find_context!();
        let mut ignored = $crate::query::VariableSet::new_empty();
        $(let $Var = ctx.next_variable();
          ignored.set($Var.index);)*
        $crate::query::IgnoreConstraint::new(ignored, Box::new($c))
    }}
}
