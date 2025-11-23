use super::*;

pub struct IntersectionConstraint<C> {
    constraints: Vec<C>,
}

impl<'a, C> IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    pub fn new(constraints: Vec<C>) -> Self {
        IntersectionConstraint { constraints }
    }
}

impl<'a, C> Constraint<'a> for IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    fn variables(&self) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |vs, c| vs.union(c.variables()))
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraints
            .iter()
            .filter_map(|c| c.estimate(variable, binding))
            .min()
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        let mut relevant_constraints: Vec<_> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        if relevant_constraints.is_empty() {
            return;
        }
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        relevant_constraints[0]
            .1
            .propose(variable, binding, proposals);

        relevant_constraints[1..]
            .iter()
            .for_each(|(_, c)| c.confirm(variable, binding, proposals));
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        let mut relevant_constraints: Vec<_> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        relevant_constraints
            .iter()
            .for_each(|(_, c)| c.confirm(variable, binding, proposals));
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |acc, c| {
                acc.union(c.influence(variable))
            })
    }
}

#[macro_export]
macro_rules! and {
    ($($c:expr),+ $(,)?) => (
        $crate::query::intersectionconstraint::IntersectionConstraint::new(vec![
            $(Box::new($c) as Box<dyn $crate::query::Constraint>),+
        ])
    )
}

pub use and;
