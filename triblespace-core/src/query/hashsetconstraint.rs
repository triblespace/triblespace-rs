use std::collections::HashSet;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use crate::inline::IntoInline;
use crate::inline::TryFromInline;

use super::*;

/// Constrains a variable to values present in a [`HashSet`].
///
/// Created via the [`ContainsConstraint`] trait (`.has(variable)`).
/// Proposals enumerate every element in the set; confirmations retain
/// only proposals that the set contains. Accepts `&HashSet<T>`,
/// `Rc<HashSet<T>>`, and `Arc<HashSet<T>>` as the backing store.
///
/// The typed residual Program owns pointwise Confirm and Support only.
/// Propose deliberately stays on the ordinary eager path: `std::HashSet`
/// exposes no owned resumable cursor, and materializing one during Program
/// seeding would hide O(n) work outside the affine budget.
pub struct SetConstraint<S: InlineEncoding, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    variable: Variable<S>,
    set: R,
}

impl<S: InlineEncoding, R, T> SetConstraint<S, R, T>
where
    R: Deref<Target = HashSet<T>>,
{
    /// Creates a constraint that restricts `variable` to values in `set`.
    pub fn new(variable: Variable<S>, set: R) -> Self {
        SetConstraint { variable, set }
    }
}

impl<S: InlineEncoding, R, T> SetConstraint<S, R, T>
where
    T: std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    R: Deref<Target = HashSet<T>>,
{
    fn contains_raw(&self, value: &RawInline) -> bool {
        match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(value)) {
            Ok(value) => self.set.contains(&value),
            Err(_) => false,
        }
    }
}

impl<S: InlineEncoding, R, T> TypedProgramSpec for SetConstraint<S, R, T>
where
    T: std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
    R: Deref<Target = HashSet<T>>,
{
    type State = finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        finiteunaryprogram::route_filter_only(self.variable.index, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        if matches!(batch.request.action, ProgramAction::Propose(_)) {
            panic!("filter-only hash-set Program admitted a proposal")
        }
        finiteunaryprogram::seed(self.variable.index, batch, effects);
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        finiteunaryprogram::step(
            self.variable.index,
            states,
            batch,
            effects,
            |_input, _cursor, _limit, _accepted| {
                panic!("filter-only hash-set Program entered an ordered proposal step")
            },
            |_input, value| self.contains_raw(value),
        )
    }
}

impl<'a, S: InlineEncoding, R, T> Constraint<'a> for SetConstraint<S, R, T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
    R: Deref<Target = HashSet<T>>,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if variable == self.variable.index && !bound.is_set(variable) {
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
        if self.variable.index != variable {
            return false;
        }
        // The current set length estimates the proposal count, per row.
        out.fill(self.set.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index != variable {
            return;
        }

        // Eager proposal already materializes the complete mathematical set;
        // raw order therefore costs no streaming opportunity and keeps
        // tail-split residual pages adjacent in storage key space.
        match candidates {
            CandidateSink::Values(values) => {
                values.extend(
                    self.set
                        .iter()
                        .map(|value| IntoInline::to_inline(value).raw),
                );
                values.sort_unstable();
            }
            CandidateSink::Tagged(pairs) => {
                let mut values: Vec<_> = self
                    .set
                    .iter()
                    .map(|value| IntoInline::to_inline(value).raw)
                    .collect();
                values.sort_unstable();
                pairs.reserve(view.len().saturating_mul(values.len()));
                for row in 0..view.len() as u32 {
                    pairs.extend(values.iter().copied().map(|value| (row, value)));
                }
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable.index == variable {
            candidates.retain(|_, value| self.contains_raw(value));
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    /// Exact when the variable is bound: checks whether every row's bound
    /// value is a member of the set. Returns `true` optimistically while
    /// the variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.contains_raw(&row[c])),
            None => true,
        }
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for &'a HashSet<T>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for Rc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for Arc<HashSet<T>>
where
    T: 'a + std::cmp::Eq + std::hash::Hash + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SetConstraint<S, Self, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SetConstraint::new(v, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::encodings::UnknownInline;

    fn value(byte: u8) -> Inline<UnknownInline> {
        Inline::new([byte; 32])
    }

    #[test]
    fn value_proposals_are_raw_ordered_with_unchanged_membership_and_coverage() {
        let variable = Variable::<UnknownInline>::new(0);
        let values: HashSet<_> = [value(3), value(1), value(2)].into_iter().collect();
        let constraint = (&values).has(variable);

        assert_eq!(
            constraint.proposal_coverage(variable.index, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );
        assert_eq!(
            constraint
                .proposal_coverage(variable.index, VariableSet::new_singleton(variable.index)),
            ProposalCoverage::None
        );

        let mut proposed = Vec::new();
        constraint.propose(
            variable.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut proposed),
        );
        assert_eq!(proposed, [value(1).raw, value(2).raw, value(3).raw]);

        let mut candidates = vec![
            value(4).raw,
            value(2).raw,
            value(1).raw,
            value(2).raw,
            value(0).raw,
        ];
        constraint.confirm(
            variable.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut candidates),
        );
        assert_eq!(
            candidates,
            [value(2).raw, value(1).raw, value(2).raw],
            "confirmation keeps the original membership, order, and multiplicity"
        );
    }

    #[test]
    fn tagged_proposals_repeat_one_raw_ordered_run_per_ascending_parent() {
        let variable = Variable::<UnknownInline>::new(1);
        let peer = Variable::<UnknownInline>::new(0);
        let values: HashSet<_> = [value(4), value(1), value(3)].into_iter().collect();
        let constraint = (&values).has(variable);
        let vars = [peer.index];
        let rows = [value(7).raw, value(8).raw, value(9).raw];
        let view = RowsView::new(&vars, &rows);
        let ordered = [value(1).raw, value(3).raw, value(4).raw];

        let mut proposed = Vec::new();
        constraint.propose(
            variable.index,
            &view,
            &mut CandidateSink::Tagged(&mut proposed),
        );

        let expected: Candidates = (0..view.len() as u32)
            .flat_map(|row| ordered.into_iter().map(move |value| (row, value)))
            .collect();
        assert_eq!(proposed, expected);
        assert!(proposed.windows(2).all(|pair| pair[0] <= pair[1]));
        for row in 0..view.len() as u32 {
            let row_values: Vec<_> = proposed
                .iter()
                .filter_map(|(parent, value)| (*parent == row).then_some(*value))
                .collect();
            assert_eq!(row_values, ordered);
        }
    }
}
