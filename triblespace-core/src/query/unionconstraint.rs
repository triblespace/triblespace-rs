use std::mem;

use super::*;
use itertools::Itertools;

pub struct UnionConstraint<C> {
    constraints: Vec<C>,
}

impl<'a, C> UnionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    pub fn new(constraints: Vec<C>) -> Self {
        assert!(constraints
            .iter()
            .map(|c| c.variables())
            .tuple_windows()
            .all(|(a, b)| a == b));
        UnionConstraint { constraints }
    }
}

impl<'a, C> Constraint<'a> for UnionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    fn variables(&self) -> VariableSet {
        self.constraints[0].variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraints
            .iter()
            .filter_map(|c| c.estimate(variable, binding))
            .reduce(|acc, e| acc + e)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        self.constraints
            .iter()
            .for_each(|c| c.propose(variable, binding, proposals));
        proposals.sort_unstable();
        proposals.dedup();
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        proposals.sort_unstable();

        let union: Vec<_> = self
            .constraints
            .iter()
            .map(|c| {
                let mut proposals = proposals.clone();
                c.confirm(variable, binding, &mut proposals);
                proposals
            })
            .kmerge()
            .dedup()
            .collect();

        _ = mem::replace(proposals, union);
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
macro_rules! or {
    ($($c:expr),+ $(,)?) => (
        $crate::query::unionconstraint::UnionConstraint::new(vec![
            $(Box::new($c) as Box<dyn $crate::query::Constraint>),+
        ])
    )
}

pub use or;
