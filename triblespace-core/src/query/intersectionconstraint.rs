use super::*;
use smallvec::SmallVec;

/// Logical conjunction of constraints (AND).
///
/// All children must agree on every variable binding. Built by the
/// [`and!`](crate::and) macro or directly via [`new`](Self::new).
///
/// Only covering children may act as sources; every target-containing child
/// remains a validator whether or not it supplies an estimate. Per row, the
/// source with the lowest raw [`estimate`](Constraint::estimate) proposes
/// candidates, with lower child index breaking ties. An Exact proposer may
/// skip its own refinement; a Covering proposer validates itself. The other
/// relevant validators [`confirm`](Constraint::confirm) through
/// whole-frontier passes, one per child, in the ascending raw-estimate order
/// selected from the frontier's first row. That deferral is what fuses the
/// per-branch confirm trickle into one ragged batch per (child, level), which is
/// what makes batched probe streams and accelerator dispatch possible in the
/// first place.
///
/// Variables from all children are exposed as a single union, so the
/// engine sees one flat set of variables regardless of how many
/// sub-constraints contribute.
pub struct IntersectionConstraint<C> {
    constraints: Vec<C>,
}

impl<'a, C> IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Creates an intersection over the given constraints.
    pub fn new(constraints: Vec<C>) -> Self {
        IntersectionConstraint { constraints }
    }

    fn target_validators(&self, variable: VariableId) -> SmallVec<[usize; 16]> {
        self.constraints
            .iter()
            .enumerate()
            .filter_map(|(index, constraint)| {
                constraint.variables().is_set(variable).then_some(index)
            })
            .collect()
    }

    fn target_sources(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> SmallVec<[(usize, ProposalCoverage); 16]> {
        self.constraints
            .iter()
            .enumerate()
            .filter_map(|(index, constraint)| {
                if !constraint.variables().is_set(variable) {
                    return None;
                }
                let coverage = constraint.proposal_coverage(variable, bound);
                (coverage >= ProposalCoverage::Covering).then_some((index, coverage))
            })
            .collect()
    }

    fn source_estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let sources = self.target_sources(variable, view.bound());
        if sources.is_empty() {
            return false;
        }

        match out {
            EstimateSink::Scalar(slot) => {
                let mut best = usize::MAX;
                for &(index, _) in &sources {
                    let mut estimate = usize::MAX;
                    self.constraints[index].estimate(
                        variable,
                        view,
                        &mut EstimateSink::Scalar(&mut estimate),
                    );
                    best = best.min(estimate);
                }
                **slot = best;
            }
            EstimateSink::Column(out) => {
                let base = out.len();
                out.resize(base + view.len(), usize::MAX);
                let mut scratch = Vec::new();
                for &(index, _) in &sources {
                    scratch.clear();
                    if self.constraints[index].estimate(
                        variable,
                        view,
                        &mut EstimateSink::Column(&mut scratch),
                    ) {
                        debug_assert_eq!(scratch.len(), view.len());
                        for (best, estimate) in out[base..].iter_mut().zip(scratch.iter().copied())
                        {
                            *best = (*best).min(estimate);
                        }
                    }
                }
            }
        }
        true
    }

    fn validator_order(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        skip: Option<usize>,
    ) -> SmallVec<[(usize, usize); 16]> {
        let mut validators = SmallVec::new();
        for index in self.target_validators(variable) {
            if skip == Some(index) {
                continue;
            }
            let mut estimate = usize::MAX;
            self.constraints[index].estimate(
                variable,
                view,
                &mut EstimateSink::Scalar(&mut estimate),
            );
            validators.push((estimate, index));
        }
        validators.sort_unstable_by_key(|&(estimate, index)| (estimate, index));
        validators
    }

    fn propose_intersection(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        let sources = self.target_sources(variable, view.bound());
        if sources.is_empty() || view.is_empty() {
            return ProposalLayout::default();
        }

        if matches!(candidates, CandidateSink::Values(_)) {
            let &(proposer, coverage) = sources
                .iter()
                .min_by_key(|&&(index, _)| {
                    let mut estimate = usize::MAX;
                    self.constraints[index].estimate(
                        variable,
                        view,
                        &mut EstimateSink::Scalar(&mut estimate),
                    );
                    (estimate, index)
                })
                .expect("non-empty covering sources");
            let layout = self.constraints[proposer].propose_with_layout(variable, view, candidates);
            let skip = (coverage == ProposalCoverage::Exact).then_some(proposer);
            for (_, index) in self.validator_order(variable, view, skip) {
                self.constraints[index].confirm(variable, view, candidates);
            }
            return layout;
        }

        let n_rows = view.len();
        let mut columns = Vec::with_capacity(sources.len() * n_rows);
        for &(index, _) in &sources {
            let base = columns.len();
            if !self.constraints[index].estimate(
                variable,
                view,
                &mut EstimateSink::Column(&mut columns),
            ) {
                columns.resize(base + n_rows, usize::MAX);
            } else {
                debug_assert_eq!(columns.len(), base + n_rows);
            }
        }

        let mut propose_counts: SmallVec<[usize; 16]> = SmallVec::from_elem(0, sources.len());
        let mut proposers: SmallVec<[u32; 32]> = SmallVec::with_capacity(n_rows);
        for row in 0..n_rows {
            let source = (0..sources.len())
                .min_by_key(|&source| (columns[source * n_rows + row], sources[source].0))
                .expect("non-empty covering sources");
            propose_counts[source] += 1;
            proposers.push(source as u32);
        }

        let uniform = (0..sources.len()).find(|&source| propose_counts[source] == n_rows);
        let layout = if let Some(source) = uniform {
            self.constraints[sources[source].0].propose_with_layout(variable, view, candidates)
        } else {
            let mut scratch = Vec::new();
            let mut layout = ProposalLayout::grouped_set();
            for (row, &source) in proposers.iter().enumerate() {
                let row_view = view.row_view(row);
                scratch.clear();
                let row_layout = self.constraints[sources[source as usize].0].propose_with_layout(
                    variable,
                    &row_view,
                    &mut CandidateSink::Values(&mut scratch),
                );
                if !row_layout.is_grouped_set() {
                    layout = ProposalLayout::default();
                }
                candidates.extend_row(row as u32, scratch.iter().copied());
            }
            layout
        };

        let skip = uniform.and_then(|source| {
            (sources[source].1 == ProposalCoverage::Exact).then_some(sources[source].0)
        });
        let first = view.row_view(0);
        for (_, index) in self.validator_order(variable, &first, skip) {
            self.constraints[index].confirm(variable, view, candidates);
        }
        layout
    }

    fn confirm_intersection(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if view.is_empty() {
            return;
        }
        let first = view.row_view(0);
        for (_, index) in self.validator_order(variable, &first, None) {
            self.constraints[index].confirm(variable, view, candidates);
        }
    }
}

impl<'a, C> ConstraintChildren<'a> for IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    fn len(&self) -> usize {
        self.constraints.len()
    }

    fn child(&self, index: usize) -> &dyn Constraint<'a> {
        &self.constraints[index]
    }
}

impl<'a, C> Constraint<'a> for IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Returns the union of all children's variable sets.
    fn variables(&self) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |vs, c| vs.union(c.variables()))
    }

    /// Any covering relevant child is a complete source for an intersection:
    /// the joint fiber is a subset of that child's fiber. A multi-child
    /// conjunction is not generally exact even when its source is exact,
    /// because the remaining children can eliminate proposed values.
    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if bound.is_set(variable) || !self.variables().is_set(variable) {
            return ProposalCoverage::None;
        }
        if let [constraint] = self.constraints.as_slice() {
            return constraint.proposal_coverage(variable, bound);
        }
        self.constraints
            .iter()
            .filter(|constraint| constraint.variables().is_set(variable))
            .any(|constraint| {
                constraint.proposal_coverage(variable, bound) >= ProposalCoverage::Covering
            })
            .then_some(ProposalCoverage::Covering)
            .unwrap_or(ProposalCoverage::None)
    }

    /// Pushes the elementwise **minimum** covering-source estimate. A missing
    /// quote is represented by [`usize::MAX`].
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.source_estimate(variable, view, out)
    }

    /// Per row, the tightest covering child proposes and every relevant child
    /// validates the resulting frontier. An Exact uniform source need not
    /// validate its own output; a Covering source must.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        _ = self.propose_intersection(variable, view, candidates);
    }

    /// Confirms a whole frontier through every relevant child in
    /// ascending (first-row) estimate order.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.confirm_intersection(variable, view, candidates)
    }

    fn propose_with_layout(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.propose_intersection(variable, view, candidates)
    }

    /// Returns `true` only when **every** child is satisfied.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.constraints.iter().all(|c| c.satisfied(view))
    }

    /// Returns the union of all children's influence sets for `variable`.
    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |acc, c| {
                acc.union(c.influence(variable))
            })
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        ConstraintShape::And(self)
    }

    fn residual_and_estimate_is_child_minimum(&self) -> bool {
        true
    }
}

/// Combines constraints into an [`IntersectionConstraint`] (logical AND).
///
/// All constraints must agree on every variable binding for a result to
/// be produced. Accepts one or more constraint expressions.
///
/// ```rust,ignore
/// and!(set.pattern(e, a, v), allowed.has(v))
/// ```
#[macro_export]
macro_rules! and {
    // Emits `Arc<IntersectionConstraint<Box<dyn Constraint + Send + Sync>>>`.
    // The outer `Arc` makes the whole tree cheap to `Clone` (single
    // refcount bump) — required by the `parallel` feature's `Query::clone`
    // during rayon split. `Send + Sync` on the trait object lets the tree
    // cross rayon thread boundaries. Every in-tree constraint built via
    // this macro already satisfies Send + Sync; non-thread-safe constraint
    // types (e.g. `Rc`-backed ContainsConstraint variants) can still be
    // used via direct `IntersectionConstraint::new` construction.
    ($($c:expr),+ $(,)?) => (
        ::std::sync::Arc::new(
            $crate::query::intersectionconstraint::IntersectionConstraint::new(vec![
                $(Box::new($c)
                    as Box<dyn $crate::query::Constraint + Send + Sync>),+
            ])
        )
    )
}

/// Re-export of the [`and!`] macro.
pub use and;

#[cfg(test)]
mod tests {
    use super::*;

    const MEMBER: RawInline = [0x31; 32];
    const OTHER: RawInline = [0x72; 32];
    const FIRST_ROW: RawInline = [0x11; 32];
    const SECOND_ROW: RawInline = [0x22; 32];
    const NO_VALUES: &[RawInline] = &[];
    const MEMBER_ONLY: &[RawInline] = &[MEMBER];
    const MEMBER_TWICE: &[RawInline] = &[MEMBER, MEMBER];
    const MEMBER_AND_OTHER: &[RawInline] = &[MEMBER, OTHER];

    #[derive(Clone, Copy)]
    struct RelationalLeaf {
        coverage: ProposalCoverage,
        quote: Option<usize>,
        proposals: &'static [RawInline],
        accepted: &'static [RawInline],
        panic_on_propose: bool,
    }

    impl Constraint<'static> for RelationalLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == 0 && !bound.is_set(variable) {
                self.coverage
            } else {
                ProposalCoverage::None
            }
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            let Some(quote) = self.quote.filter(|_| variable == 0) else {
                return false;
            };
            out.fill(quote, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable != 0 {
                return;
            }
            assert!(!self.panic_on_propose, "validator was used as a source");
            for row in 0..view.len() as u32 {
                candidates.extend_row(row, self.proposals.iter().copied());
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == 0 {
                candidates.retain(|_, value| self.accepted.contains(value));
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.col(0)
                .is_none_or(|column| view.iter().all(|row| self.accepted.contains(&row[column])))
        }
    }

    fn relational_values(constraint: &IntersectionConstraint<RelationalLeaf>) -> Vec<RawInline> {
        let mut values = Vec::new();
        constraint.propose(0, &RowsView::EMPTY, &mut CandidateSink::Values(&mut values));
        values
    }

    #[derive(Clone, Copy)]
    struct RowAdaptiveSource {
        cheap_on: RawInline,
        occurrences: usize,
    }

    impl Constraint<'static> for RowAdaptiveSource {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0).union(VariableSet::new_singleton(1))
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == 0 && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            let column = view.col(1).expect("row discriminator is bound");
            out.extend(
                view.iter()
                    .map(|row| if row[column] == self.cheap_on { 1 } else { 9 }),
            );
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == 0 {
                for row in 0..view.len() as u32 {
                    candidates.extend_row(row, std::iter::repeat_n(MEMBER, self.occurrences));
                }
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == 0 {
                candidates.retain(|_, value| *value == MEMBER);
            }
        }

        fn satisfied(&self, _view: &RowsView<'_>) -> bool {
            true
        }
    }

    #[test]
    fn relational_intersection_never_promotes_a_low_quoted_none_validator() {
        let constraint = IntersectionConstraint::new(vec![
            RelationalLeaf {
                coverage: ProposalCoverage::None,
                quote: Some(0),
                proposals: NO_VALUES,
                accepted: MEMBER_ONLY,
                panic_on_propose: true,
            },
            RelationalLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(9),
                proposals: MEMBER_ONLY,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
        ]);

        let mut estimate = 0;
        assert!(constraint.estimate(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, 9);
        assert_eq!(relational_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn relational_intersection_runs_an_unquoted_target_validator() {
        let constraint = IntersectionConstraint::new(vec![
            RelationalLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(1),
                proposals: MEMBER_AND_OTHER,
                accepted: MEMBER_AND_OTHER,
                panic_on_propose: false,
            },
            RelationalLeaf {
                coverage: ProposalCoverage::None,
                quote: None,
                proposals: NO_VALUES,
                accepted: MEMBER_ONLY,
                panic_on_propose: true,
            },
        ]);

        assert_eq!(relational_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn relational_intersection_self_confirms_a_covering_source() {
        let constraint = IntersectionConstraint::new(vec![RelationalLeaf {
            coverage: ProposalCoverage::Covering,
            quote: Some(1),
            proposals: MEMBER_AND_OTHER,
            accepted: MEMBER_ONLY,
            panic_on_propose: false,
        }]);

        assert_eq!(relational_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn relational_intersection_prices_a_quote_less_source_at_max() {
        let constraint = IntersectionConstraint::new(vec![
            RelationalLeaf {
                coverage: ProposalCoverage::Exact,
                quote: None,
                proposals: MEMBER_TWICE,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
            RelationalLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(usize::MAX - 1),
                proposals: MEMBER_ONLY,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
        ]);

        let mut estimate = 0;
        assert!(constraint.estimate(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, usize::MAX - 1);
        assert_eq!(relational_values(&constraint), vec![MEMBER]);

        let quote_less = IntersectionConstraint::new(vec![RelationalLeaf {
            coverage: ProposalCoverage::Exact,
            quote: None,
            proposals: MEMBER_ONLY,
            accepted: MEMBER_ONLY,
            panic_on_propose: false,
        }]);
        assert!(quote_less.estimate(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, usize::MAX);
    }

    #[test]
    fn relational_intersection_selects_the_cheapest_source_per_row() {
        let constraint = IntersectionConstraint::new(vec![
            RowAdaptiveSource {
                cheap_on: FIRST_ROW,
                occurrences: 1,
            },
            RowAdaptiveSource {
                cheap_on: SECOND_ROW,
                occurrences: 2,
            },
        ]);
        let rows = [FIRST_ROW, SECOND_ROW];
        let view = RowsView::new(&[1], &rows);
        let mut candidates = Vec::new();

        constraint.propose(0, &view, &mut CandidateSink::Tagged(&mut candidates));

        assert_eq!(candidates, vec![(0, MEMBER), (1, MEMBER), (1, MEMBER)]);
    }
}
