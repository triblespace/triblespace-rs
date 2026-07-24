use super::*;
use smallvec::SmallVec;

/// Logical conjunction of constraints (AND).
///
/// All children must agree on every variable binding. Built by the
/// [`and!`](crate::and) macro or directly via [`new`](Self::new).
///
/// A certified fixed-denotation intersection uses semantic receipts to admit
/// only covering children as sources; every target-containing child remains a
/// validator, whether or not it supplies an estimate, and a covering proposer
/// validates itself. An uncertified intersection retains the action-preserving
/// legacy rule: per row, the child with the lowest
/// [`estimate`](Constraint::estimate) proposes candidates. Lower child index
/// breaks equal estimates, and the remaining children
/// [`confirm`](Constraint::confirm) them — not per branch, but in
/// whole-frontier passes, one per child. That deferral is
/// what fuses the per-branch confirm trickle into one ragged batch per
/// (child, level), which is what makes batched probe streams and accelerator
/// dispatch possible in the first place.
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

    fn certified_estimate(
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
                    self.constraints[index].estimate_certified(
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
                    if self.constraints[index].estimate_certified(
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

    fn certified_validator_order(
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
            self.constraints[index].estimate_certified(
                variable,
                view,
                &mut EstimateSink::Scalar(&mut estimate),
            );
            validators.push((estimate, index));
        }
        validators.sort_unstable_by_key(|&(estimate, index)| (estimate, index));
        validators
    }

    fn certified_propose(
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
                    self.constraints[index].estimate_certified(
                        variable,
                        view,
                        &mut EstimateSink::Scalar(&mut estimate),
                    );
                    (estimate, index)
                })
                .expect("non-empty certified sources");
            let layout = self.constraints[proposer]
                .propose_certified_with_receipt(variable, view, candidates);
            let skip = (coverage == ProposalCoverage::Exact).then_some(proposer);
            for (_, index) in self.certified_validator_order(variable, view, skip) {
                self.constraints[index].confirm_certified(variable, view, candidates);
            }
            return layout;
        }

        let n_rows = view.len();
        let mut columns = Vec::with_capacity(sources.len() * n_rows);
        for &(index, _) in &sources {
            let base = columns.len();
            if !self.constraints[index].estimate_certified(
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
                .expect("non-empty certified sources");
            propose_counts[source] += 1;
            proposers.push(source as u32);
        }

        let uniform = (0..sources.len()).find(|&source| propose_counts[source] == n_rows);
        let layout = if let Some(source) = uniform {
            self.constraints[sources[source].0]
                .propose_certified_with_receipt(variable, view, candidates)
        } else {
            let mut scratch = Vec::new();
            let mut layout = ProposalLayout::grouped_set();
            for (row, &source) in proposers.iter().enumerate() {
                let row_view = view.row_view(row);
                scratch.clear();
                let row_layout = self.constraints[sources[source as usize].0]
                    .propose_certified_with_receipt(
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
        for (_, index) in self.certified_validator_order(variable, &first, skip) {
            self.constraints[index].confirm_certified(variable, view, candidates);
        }
        layout
    }

    fn certified_confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if view.is_empty() {
            return;
        }
        let first = view.row_view(0);
        for (_, index) in self.certified_validator_order(variable, &first, None) {
            self.constraints[index].confirm_certified(variable, view, candidates);
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

    /// A conjunction has one fixed relation only when every child does.
    fn fixed_denotation(&self) -> bool {
        self.constraints.iter().all(Constraint::fixed_denotation)
    }

    /// Any covering relevant child is a complete source for an intersection:
    /// the joint fiber is a subset of that child's fiber. A multi-child
    /// conjunction is not generally exact even when its source is exact,
    /// because the remaining children can eliminate proposed values.
    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if !self.fixed_denotation() || bound.is_set(variable) || !self.variables().is_set(variable)
        {
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

    /// Pushes the elementwise **minimum** source estimate. For a certified
    /// intersection, only covering children are sources and a missing quote is
    /// represented by [`usize::MAX`]. The uncertified path retains the legacy
    /// estimate-derived relevance rule.
    ///
    /// The scalar (single-row cursor) arm folds child estimates through
    /// stack slots — no column scratch is ever allocated on the
    /// sequential engine's path.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        match out {
            EstimateSink::Scalar(slot) => {
                let mut any = false;
                let mut acc = usize::MAX;
                for c in &self.constraints {
                    let mut e = 0usize;
                    if c.estimate(variable, view, &mut EstimateSink::Scalar(&mut e)) {
                        any = true;
                        acc = acc.min(e);
                    }
                }
                if any {
                    **slot = acc;
                }
                any
            }
            EstimateSink::Column(out) => {
                let base = out.len();
                let mut any = false;
                let mut scratch: Vec<usize> = Vec::new();
                for c in &self.constraints {
                    if !any {
                        any = c.estimate(variable, view, &mut EstimateSink::Column(out));
                    } else {
                        scratch.clear();
                        if c.estimate(variable, view, &mut EstimateSink::Column(&mut scratch)) {
                            for (o, &s) in out[base..].iter_mut().zip(scratch.iter()) {
                                *o = (*o).min(s);
                            }
                        }
                    }
                }
                any
            }
        }
    }

    /// Frontier expansion: per row the tightest child proposes (one
    /// estimate column per relevant child, argmin per row, lower child index
    /// on an equal estimate), then the sibling confirms run as
    /// **whole-frontier passes** — one per child, cheapest (first-row
    /// estimate) first. The proposer tie break is semantic because a
    /// proposer owns the occurrence multiplicity of its candidate stream.
    ///
    /// When one child proposes for *every* row (the common case —
    /// proposer choice is usually structural), it receives the whole
    /// block so its own batching kicks in, and it skips its confirm pass
    /// (its own proposals are consistent by construction). When proposers
    /// vary across rows each row is proposed through a single-row
    /// borrowed view into an **isolated** scratch sink — a child must
    /// never see candidates owned by another row, because composite
    /// children confirm whatever sink they are handed in its entirety —
    /// and every relevant child then confirms the full frontier.
    /// Re-confirming a child's own pairs is a wasted-work cost, never a
    /// correctness one.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        // The sequential cursor (a Values sink is always a block of 1):
        // scalar child estimates in stack slots, argmin, propose, ordered
        // confirms — no estimate columns, no heap scratch.
        if matches!(candidates, CandidateSink::Values(_)) {
            let mut relevant: SmallVec<[(usize, usize); 16]> = SmallVec::new();
            for (ci, c) in self.constraints.iter().enumerate() {
                let mut e = 0usize;
                if c.estimate(variable, view, &mut EstimateSink::Scalar(&mut e)) {
                    relevant.push((e, ci));
                }
            }
            if relevant.is_empty() {
                return;
            }
            relevant.sort_unstable_by_key(|&(estimate, child)| (estimate, child));
            self.constraints[relevant[0].1].propose(variable, view, candidates);
            for &(_, ci) in &relevant[1..] {
                self.constraints[ci].confirm(variable, view, candidates);
            }
            return;
        }

        let n_rows = view.len();

        // Pass 1: per-child estimate columns (flat, child-major) — the
        // same cardinality data drives proposer choice AND confirm order.
        let mut cols: Vec<usize> = Vec::new();
        let mut relevant: SmallVec<[usize; 16]> = SmallVec::new();
        for (ci, c) in self.constraints.iter().enumerate() {
            if c.estimate(variable, view, &mut EstimateSink::Column(&mut cols)) {
                relevant.push(ci);
            }
        }
        if relevant.is_empty() {
            return;
        }

        // Pass 2: per-row proposer = argmin across the columns.
        let mut propose_counts: SmallVec<[usize; 16]> = SmallVec::from_elem(0, relevant.len());
        let mut proposers: SmallVec<[u32; 32]> = SmallVec::with_capacity(n_rows);
        for i in 0..n_rows {
            let k = (0..relevant.len())
                .min_by_key(|&k| (cols[k * n_rows + i], relevant[k]))
                .expect("non-empty relevant");
            propose_counts[k] += 1;
            proposers.push(k as u32);
        }

        // Pass 3: expand the frontier.
        let uniform = (0..relevant.len()).find(|&k| propose_counts[k] == n_rows);
        if let Some(k) = uniform {
            self.constraints[relevant[k]].propose(variable, view, candidates);
        } else {
            // Non-uniform proposers: each row's child proposes into an
            // isolated, cleared single-row scratch — never into the
            // shared, already-populated frontier sink. `propose` is not
            // append-only for composite children: a nested intersection
            // runs its sibling confirms over the ENTIRE sink it is
            // handed, interpreting every row tag through the one-row
            // view — which deletes other rows' legitimate candidates
            // under the wrong bindings, or indexes past the end of the
            // one-row view for tags ≥ 1. A single-row view with a
            // Values sink is exactly the sequential-cursor contract
            // every constraint already honors, so isolation is free;
            // `extend_row` then applies this row's tag on the way out.
            let mut scratch: Vec<RawInline> = Vec::new();
            for (i, &k) in proposers.iter().enumerate() {
                let row_view = view.row_view(i);
                scratch.clear();
                self.constraints[relevant[k as usize]].propose(
                    variable,
                    &row_view,
                    &mut CandidateSink::Values(&mut scratch),
                );
                candidates.extend_row(i as u32, scratch.iter().copied());
            }
        }

        // Pass 4: whole-frontier confirms, cheapest (first-row estimate)
        // child first. The uniform proposer skips its own pass.
        let mut confirmers: SmallVec<[(usize, usize); 16]> = relevant
            .iter()
            .enumerate()
            .filter(|&(k, _)| uniform != Some(k))
            .map(|(k, &ci)| (cols[k * n_rows], ci))
            .collect();
        confirmers.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in confirmers {
            self.constraints[ci].confirm(variable, view, candidates);
        }
    }

    /// Confirms a whole frontier through every relevant child in
    /// ascending (first-row) estimate order.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let first = view.row_view(0);
        let mut relevant: SmallVec<[(usize, usize); 16]> = SmallVec::new();
        for (ci, c) in self.constraints.iter().enumerate() {
            let mut est = 0usize;
            if c.estimate(variable, &first, &mut EstimateSink::Scalar(&mut est)) {
                relevant.push((est, ci));
            }
        }
        relevant.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in relevant {
            self.constraints[ci].confirm(variable, view, candidates);
        }
    }

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.certified_estimate(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        _ = self.certified_propose(variable, view, candidates);
    }

    fn propose_certified_with_receipt(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.certified_propose(variable, view, candidates)
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.certified_confirm(variable, view, candidates)
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
    struct CertifiedLeaf {
        coverage: ProposalCoverage,
        quote: Option<usize>,
        proposals: &'static [RawInline],
        accepted: &'static [RawInline],
        panic_on_propose: bool,
    }

    impl Constraint<'static> for CertifiedLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn fixed_denotation(&self) -> bool {
            true
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

    fn certified_values(constraint: &IntersectionConstraint<CertifiedLeaf>) -> Vec<RawInline> {
        let mut values = Vec::new();
        constraint.propose_certified(0, &RowsView::EMPTY, &mut CandidateSink::Values(&mut values));
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

        fn fixed_denotation(&self) -> bool {
            true
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

    #[derive(Clone, Copy)]
    struct DirectedMultiplicitySource {
        occurrences: usize,
        classes: ActionUnitClasses,
    }

    impl Constraint<'static> for DirectedMultiplicitySource {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn fixed_denotation(&self) -> bool {
            true
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == 0 && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
        }

        fn action_unit_classes(
            &self,
            variable: VariableId,
            bound: VariableSet,
        ) -> Option<ActionUnitClasses> {
            (variable == 0 && !bound.is_set(variable)).then_some(self.classes)
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
            out.fill(self.occurrences, view.len());
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
    fn certified_intersection_never_promotes_a_low_quoted_none_validator() {
        let constraint = IntersectionConstraint::new(vec![
            CertifiedLeaf {
                coverage: ProposalCoverage::None,
                quote: Some(0),
                proposals: NO_VALUES,
                accepted: MEMBER_ONLY,
                panic_on_propose: true,
            },
            CertifiedLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(9),
                proposals: MEMBER_ONLY,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
        ]);

        let mut estimate = 0;
        assert!(constraint.estimate_certified(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, 9);
        assert_eq!(certified_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn certified_intersection_runs_an_unquoted_target_validator() {
        let constraint = IntersectionConstraint::new(vec![
            CertifiedLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(1),
                proposals: MEMBER_AND_OTHER,
                accepted: MEMBER_AND_OTHER,
                panic_on_propose: false,
            },
            CertifiedLeaf {
                coverage: ProposalCoverage::None,
                quote: None,
                proposals: NO_VALUES,
                accepted: MEMBER_ONLY,
                panic_on_propose: true,
            },
        ]);

        assert_eq!(certified_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn certified_intersection_self_confirms_a_covering_source() {
        let constraint = IntersectionConstraint::new(vec![CertifiedLeaf {
            coverage: ProposalCoverage::Covering,
            quote: Some(1),
            proposals: MEMBER_AND_OTHER,
            accepted: MEMBER_ONLY,
            panic_on_propose: false,
        }]);

        assert_eq!(certified_values(&constraint), vec![MEMBER]);
    }

    #[test]
    fn certified_intersection_prices_a_quote_less_source_at_max() {
        let constraint = IntersectionConstraint::new(vec![
            CertifiedLeaf {
                coverage: ProposalCoverage::Exact,
                quote: None,
                proposals: MEMBER_TWICE,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
            CertifiedLeaf {
                coverage: ProposalCoverage::Exact,
                quote: Some(usize::MAX - 1),
                proposals: MEMBER_ONLY,
                accepted: MEMBER_ONLY,
                panic_on_propose: false,
            },
        ]);

        let mut estimate = 0;
        assert!(constraint.estimate_certified(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, usize::MAX - 1);
        assert_eq!(certified_values(&constraint), vec![MEMBER]);

        let quote_less = IntersectionConstraint::new(vec![CertifiedLeaf {
            coverage: ProposalCoverage::Exact,
            quote: None,
            proposals: MEMBER_ONLY,
            accepted: MEMBER_ONLY,
            panic_on_propose: false,
        }]);
        assert!(quote_less.estimate_certified(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, usize::MAX);
    }

    #[test]
    fn certified_intersection_selects_the_cheapest_source_per_row() {
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

        constraint.propose_certified(0, &view, &mut CandidateSink::Tagged(&mut candidates));

        assert_eq!(candidates, vec![(0, MEMBER), (1, MEMBER), (1, MEMBER)]);
    }

    #[test]
    fn nested_certified_intersection_keeps_raw_scalar_source_order() {
        type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;

        let inner = IntersectionConstraint::new(vec![
            Box::new(DirectedMultiplicitySource {
                occurrences: 8,
                classes: ActionUnitClasses::new(
                    ProposalUnitClass::HASH_TABLE_ENUMERATION,
                    ConfirmationUnitClass::HASH_TABLE_MEMBERSHIP,
                ),
            }) as DynConstraint,
            Box::new(DirectedMultiplicitySource {
                occurrences: 29,
                classes: ActionUnitClasses::new(
                    ProposalUnitClass::SUCCINCT_ORDERED_ENUMERATION,
                    ConfirmationUnitClass::SUCCINCT_RANDOM_MEMBERSHIP,
                ),
            }) as DynConstraint,
        ]);
        let root = IntersectionConstraint::new(vec![Box::new(inner) as DynConstraint]);

        let mut estimate = usize::MAX;
        assert!(root.estimate_certified(
            0,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        assert_eq!(estimate, 8);

        let mut scalar = Vec::new();
        root.propose_certified(0, &RowsView::EMPTY, &mut CandidateSink::Values(&mut scalar));
        assert_eq!(scalar, vec![MEMBER; 8]);
    }

    /// Two lawful intersection leaves with identical support and estimates,
    /// but different proposal multiplicity. Confirm only filters, so child
    /// order is the observable equal-estimate proposer tie break.
    #[derive(Clone, Copy)]
    struct EqualEstimateBagLeaf {
        occurrences: usize,
    }

    impl EqualEstimateBagLeaf {
        const VARIABLE: VariableId = 0;
        const VALUE: RawInline = [7; 32];
    }

    impl Constraint<'static> for EqualEstimateBagLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(Self::VARIABLE)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != Self::VARIABLE {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable != Self::VARIABLE {
                return;
            }
            for (row_index, row) in view.iter().enumerate() {
                if view
                    .col(Self::VARIABLE)
                    .is_none_or(|column| row[column] == Self::VALUE)
                {
                    candidates.extend_row(
                        row_index as u32,
                        std::iter::repeat_n(Self::VALUE, self.occurrences),
                    );
                }
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == Self::VARIABLE {
                candidates.retain(|row, value| {
                    *value == Self::VALUE
                        && view
                            .col(Self::VARIABLE)
                            .is_none_or(|column| view.row(row as usize)[column] == Self::VALUE)
                });
            }
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.iter().all(|row| {
                view.col(Self::VARIABLE)
                    .is_none_or(|column| row[column] == Self::VALUE)
            })
        }
    }

    fn equal_estimate_occurrence_query() -> Query<
        IntersectionConstraint<EqualEstimateBagLeaf>,
        impl Fn(&Binding) -> Option<RawInline>,
        RawInline,
    > {
        Query::new(
            IntersectionConstraint::new(vec![
                EqualEstimateBagLeaf { occurrences: 2 },
                EqualEstimateBagLeaf { occurrences: 1 },
            ]),
            |binding: &Binding| binding.get(EqualEstimateBagLeaf::VARIABLE).copied(),
        )
    }

    #[test]
    fn equal_estimate_tie_preserves_child_occurrences_before_set_projection() {
        let constraint = IntersectionConstraint::new(vec![
            EqualEstimateBagLeaf { occurrences: 2 },
            EqualEstimateBagLeaf { occurrences: 1 },
        ]);
        let mut proposed = Vec::new();
        constraint.propose(
            EqualEstimateBagLeaf::VARIABLE,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut proposed),
        );
        assert_eq!(proposed, vec![EqualEstimateBagLeaf::VALUE; 2]);

        let expected = vec![EqualEstimateBagLeaf::VALUE];
        let sequential: Vec<_> = equal_estimate_occurrence_query().sequential().collect();
        let blocked = equal_estimate_occurrence_query().solve_blocked();
        let residual: Vec<_> = equal_estimate_occurrence_query()
            .solve_residual_state_lazy_with(residual::ResidualLowering::FULL)
            .collect();

        assert_eq!(sequential, expected);
        assert_eq!(blocked, expected);
        assert_eq!(residual, expected);
    }
}
