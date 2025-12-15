use super::*;

pub struct IgnoreConstraint<'a> {
    ignored: VariableSet,
    constraint: Box<dyn Constraint<'a> + 'a>,
}

impl<'a> IgnoreConstraint<'a> {
    pub fn new(ignored: VariableSet, constraint: Box<dyn Constraint<'a> + 'a>) -> Self {
        IgnoreConstraint {
            ignored,
            constraint,
        }
    }
}

impl<'a> Constraint<'a> for IgnoreConstraint<'a> {
    fn variables(&self) -> VariableSet {
        // Remove ignored variables so they neither join with outer
        // constraints nor appear in the result set. This lets callers use
        // multi-column constraints (like triples) while projecting only the
        // surviving positions.
        self.constraint.variables().subtract(self.ignored)
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraint.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        self.constraint.propose(variable, binding, proposals);
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        self.constraint.confirm(variable, binding, proposals)
    }
}

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
