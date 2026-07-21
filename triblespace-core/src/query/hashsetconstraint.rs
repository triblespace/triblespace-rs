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
        states: crate::query::TypedProgramStateBatch<Self::State>,
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

    fn action_unit_classes(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> Option<ActionUnitClasses> {
        (variable == self.variable.index && !bound.is_set(variable)).then_some(
            ActionUnitClasses::new(
                ProposalUnitClass::HASH_TABLE_ENUMERATION,
                ConfirmationUnitClass::HASH_TABLE_MEMBERSHIP,
            ),
        )
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
        if self.variable.index == variable {
            for i in 0..view.len() as u32 {
                candidates.extend_row(i, self.set.iter().map(|v| IntoInline::to_inline(v).raw));
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
    use crate::inline::Inline;
    use crate::inline::encodings::UnknownInline;

    #[test]
    fn hash_set_action_classes_cover_exact_proposal_occurrences() {
        let values: HashSet<_> = [
            Inline::<UnknownInline>::new([0x11; 32]),
            Inline::<UnknownInline>::new([0x22; 32]),
        ]
        .into_iter()
        .collect();
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = SetConstraint::new(variable, &values);
        let classes = constraint
            .action_unit_classes(variable.index, VariableSet::new_empty())
            .expect("an unbound HashSet target has exact occurrence counts");

        assert_eq!(classes.proposal, ProposalUnitClass::HASH_TABLE_ENUMERATION);
        assert_eq!(
            classes.confirmation,
            ConfirmationUnitClass::HASH_TABLE_MEMBERSHIP
        );
        let mut estimate = usize::MAX;
        assert!(constraint.estimate(
            variable.index,
            &RowsView::EMPTY,
            &mut EstimateSink::Scalar(&mut estimate),
        ));
        let mut proposed = Vec::new();
        constraint.propose(
            variable.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut proposed),
        );
        assert_eq!(estimate, proposed.len());
        let bound = VariableSet::new_singleton(variable.index);
        assert!(
            constraint
                .action_unit_classes(variable.index, bound)
                .is_none()
        );
    }
}
