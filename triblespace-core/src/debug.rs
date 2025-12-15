pub mod query {
    use crate::query::Binding;
    use crate::query::Constraint;
    use crate::query::VariableId;
    use crate::query::VariableSet;
    use crate::value::RawValue;
    use std::cell::RefCell;
    use std::rc::Rc;

    pub struct DebugConstraint<C> {
        pub constraint: C,
        pub record: Rc<RefCell<Vec<VariableId>>>,
    }

    impl<C> DebugConstraint<C> {
        pub fn new(constraint: C, record: Rc<RefCell<Vec<VariableId>>>) -> Self {
            DebugConstraint { constraint, record }
        }
    }

    impl<'a, C: Constraint<'a>> Constraint<'a> for DebugConstraint<C> {
        fn variables(&self) -> VariableSet {
            self.constraint.variables()
        }

        fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
            self.constraint.estimate(variable, binding)
        }

        fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
            self.record.borrow_mut().push(variable);
            self.constraint.propose(variable, binding, proposals);
        }

        fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
            self.constraint.confirm(variable, binding, proposals);
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }

    pub struct EstimateOverrideConstraint<C> {
        pub constraint: C,
        pub estimates: [Option<usize>; 128],
    }

    impl<C> EstimateOverrideConstraint<C> {
        pub fn new(constraint: C) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates: [None; 128],
            }
        }

        pub fn with_estimates(constraint: C, estimates: [Option<usize>; 128]) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates,
            }
        }

        pub fn set_estimate(&mut self, variable: VariableId, estimate: usize) {
            self.estimates[variable] = Some(estimate);
        }
    }

    impl<'a, C: Constraint<'a>> Constraint<'a> for EstimateOverrideConstraint<C> {
        fn variables(&self) -> VariableSet {
            self.constraint.variables()
        }

        fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
            self.estimates[variable].or_else(|| self.constraint.estimate(variable, binding))
        }

        fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
            self.constraint.propose(variable, binding, proposals);
        }

        fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
            self.constraint.confirm(variable, binding, proposals);
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }
}
