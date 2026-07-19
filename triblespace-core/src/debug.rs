/// Diagnostic wrappers for the query engine used in tests.
pub mod query {
    use crate::inline::RawInline;
    use crate::query::CandidateSink;
    use crate::query::Constraint;
    use crate::query::EstimateSink;
    use crate::query::ProgramRef;
    use crate::query::ProposalCoverage;
    use crate::query::ResidualDeltaExpandBatch;
    use crate::query::ResidualDeltaExpandCursor;
    use crate::query::ResidualDeltaExpandPage;
    use crate::query::ResidualDeltaNode;
    use crate::query::ResidualDeltaOutput;
    use crate::query::ResidualDeltaSeed;
    use crate::query::ResidualDeltaSourceBatch;
    use crate::query::ResidualDeltaSourceCursor;
    use crate::query::ResidualDeltaSourcePage;
    use crate::query::RowsView;
    use crate::query::VariableId;
    use crate::query::VariableSet;
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

        fn fixed_denotation(&self) -> bool {
            self.constraint.fixed_denotation()
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            self.constraint.proposal_coverage(variable, bound)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.constraint.estimate(variable, view, out)
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.record.borrow_mut().push(variable);
            self.constraint.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.confirm(variable, view, candidates);
        }

        fn estimate_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            self.constraint.estimate_certified(variable, view, out)
        }

        fn propose_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.record.borrow_mut().push(variable);
            self.constraint
                .propose_certified(variable, view, candidates);
        }

        fn confirm_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .confirm_certified(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.constraint.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }
    }

    /// Constraint wrapper that overrides cardinality estimates for selected variables.
    ///
    /// The wrapper stays structurally opaque so residual formula descent cannot
    /// bypass its planner input. Optional execution capabilities remain
    /// transparent because proposal, confirmation, and truth semantics are
    /// delegated unchanged.
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

        fn fixed_denotation(&self) -> bool {
            self.constraint.fixed_denotation()
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            self.constraint.proposal_coverage(variable, bound)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if let Some(estimate) = self.estimates[variable] {
                out.fill(estimate, view.len());
                true
            } else {
                self.constraint.estimate(variable, view, out)
            }
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.propose(variable, view, candidates);
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint.confirm(variable, view, candidates);
        }

        fn estimate_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if let Some(estimate) = self.estimates[variable] {
                out.fill(estimate, view.len());
                true
            } else {
                self.constraint.estimate_certified(variable, view, out)
            }
        }

        fn propose_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .propose_certified(variable, view, candidates);
        }

        fn confirm_certified(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            self.constraint
                .confirm_certified(variable, view, candidates);
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            self.constraint.satisfied(view)
        }

        fn influence(&self, variable: VariableId) -> VariableSet {
            self.constraint.influence(variable)
        }

        // EstimateOverrideConstraint changes only the planner's cardinality
        // input. Keep the wrapper structurally opaque so opening a composite
        // child cannot bypass that override, but forward every optional
        // execution capability whose semantics are identical to the delegated
        // propose/confirm/satisfied verbs above.
        fn residual_confirm_is_page_local(&self) -> bool {
            self.constraint.residual_confirm_is_page_local()
        }

        fn residual_delta_confirm_grouping_requirements(
            &self,
            variable: VariableId,
        ) -> Option<VariableSet> {
            self.constraint
                .residual_delta_confirm_grouping_requirements(variable)
        }

        fn residual_program(&self) -> Option<ProgramRef<'_>> {
            self.constraint.residual_program()
        }

        fn residual_program_proposal_coverage(
            &self,
            variable: VariableId,
            bound: VariableSet,
        ) -> ProposalCoverage {
            self.constraint
                .residual_program_proposal_coverage(variable, bound)
        }

        fn residual_delta_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.constraint
                .residual_delta_source_is_paged(variable, view)
        }

        fn residual_proposal_source_is_paged(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
        ) -> bool {
            self.constraint
                .residual_proposal_source_is_paged(variable, view)
        }

        fn residual_delta_source_page(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: Option<&[RawInline]>,
            cursor: ResidualDeltaSourceCursor,
            limit: usize,
            roots: &mut Vec<ResidualDeltaOutput>,
            accepted: &mut Vec<RawInline>,
        ) -> Option<ResidualDeltaSourcePage> {
            self.constraint.residual_delta_source_page(
                variable, view, candidates, cursor, limit, roots, accepted,
            )
        }

        fn residual_delta_source_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaSourceBatch<'_>,
            pages: &mut Vec<ResidualDeltaSourcePage>,
            roots: &mut Vec<(u32, ResidualDeltaOutput)>,
            accepted: &mut Vec<(u32, RawInline)>,
        ) -> bool {
            self.constraint
                .residual_delta_source_pages(variable, batch, pages, roots, accepted)
        }

        fn residual_delta_seeds(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            seeds: &mut Vec<ResidualDeltaSeed>,
        ) -> bool {
            self.constraint.residual_delta_seeds(variable, view, seeds)
        }

        fn residual_delta_support_seeds(
            &self,
            view: &RowsView<'_>,
            seeds: &mut Vec<ResidualDeltaSeed>,
        ) -> Option<VariableId> {
            self.constraint.residual_delta_support_seeds(view, seeds)
        }

        fn residual_delta_expand_page(
            &self,
            variable: VariableId,
            node: ResidualDeltaNode,
            cursor: ResidualDeltaExpandCursor,
            limit: usize,
            successors: &mut Vec<ResidualDeltaOutput>,
        ) -> Option<ResidualDeltaExpandPage> {
            self.constraint
                .residual_delta_expand_page(variable, node, cursor, limit, successors)
        }

        fn residual_delta_expand_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaExpandBatch<'_>,
            pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) {
            self.constraint
                .residual_delta_expand_pages(variable, batch, pages, successors)
        }

        fn residual_delta_expand(
            &self,
            variable: VariableId,
            nodes: &[ResidualDeltaNode],
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) -> bool {
            self.constraint
                .residual_delta_expand(variable, nodes, successors)
        }
    }
}
