/// Diagnostic wrappers for the query engine used in tests.
pub mod query {
    use crate::query::Binding;
    use crate::query::Constraint;
    use crate::query::VariableId;
    use crate::query::VariableSet;
    use crate::value::RawInline;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// Constraint wrapper that records which variables are proposed during query execution.
    pub struct DebugConstraint<C> {
        /// The underlying constraint being observed.
        pub constraint: C,
        /// Shared log of variable ids in the order they were proposed.
        pub record: Rc<RefCell<Vec<VariableId>>>,
    }

    impl<C> DebugConstraint<C> {
        /// Wraps `constraint` and appends every proposed variable id to `record`.
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

        fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
            self.record.borrow_mut().push(variable);
            self.constraint.propose(variable, binding, proposals);
        }

        fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
            self.constraint.confirm(variable, binding, proposals);
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }

    /// Constraint wrapper that overrides cardinality estimates for selected variables.
    pub struct EstimateOverrideConstraint<C> {
        /// The underlying constraint whose estimates may be overridden.
        pub constraint: C,
        /// Per-variable estimate overrides; `None` falls through to the inner constraint.
        pub estimates: [Option<usize>; 128],
    }

    impl<C> EstimateOverrideConstraint<C> {
        /// Creates a wrapper with no estimate overrides.
        pub fn new(constraint: C) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates: [None; 128],
            }
        }

        /// Creates a wrapper with the given estimate override array.
        pub fn with_estimates(constraint: C, estimates: [Option<usize>; 128]) -> Self {
            EstimateOverrideConstraint {
                constraint,
                estimates,
            }
        }

        /// Overrides the cardinality estimate for `variable`.
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

        fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
            self.constraint.propose(variable, binding, proposals);
        }

        fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
            self.constraint.confirm(variable, binding, proposals);
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }
}
