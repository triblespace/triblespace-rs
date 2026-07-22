use super::*;

/// Pins a variable to a single known value.
///
/// Created by [`Variable::is`]. The estimate is always 1, propose yields
/// exactly the constant, and confirm retains only matching proposals.
/// This is the simplest possible constraint. Note that `pattern!` does
/// not use it for attribute constants or literal values — those are
/// folded into the pattern constraint as constant
/// [`Term`](crate::query::Term)s and never become variables.
pub struct ConstantConstraint {
    variable: VariableId,
    constant: RawInline,
}

/// Canonical finite continuation for [`ConstantConstraint`].
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantProgramState {
    Propose,
    Confirm { offset: usize },
    Support,
}

const CONSTANT_PROPOSE_ROUTE: ProgramKey = ProgramKey::new(0);
const CONSTANT_CONFIRM_ROUTE: ProgramKey = ProgramKey::new(1);
const CONSTANT_SUPPORT_UNBOUND_ROUTE: ProgramKey = ProgramKey::new(2);
const CONSTANT_SUPPORT_BOUND_ROUTE: ProgramKey = ProgramKey::new(3);

const CONSTANT_PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const CONSTANT_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const CONSTANT_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

impl ConstantConstraint {
    /// Creates a constraint that binds `variable` to `constant`.
    pub fn new<T: InlineEncoding>(variable: Variable<T>, constant: Inline<T>) -> Self {
        ConstantConstraint {
            variable: variable.index,
            constant: constant.raw,
        }
    }
}

impl TypedProgramSpec for ConstantConstraint {
    type State = ConstantProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 2];

    fn exposures(&self) -> crate::query::ProgramExposureSet {
        crate::query::ProgramExposureSet::PRODUCTION
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let (key, variable) = match request.action {
            ProgramAction::Propose(variable) => {
                if variable != self.variable || request.bound.is_set(variable) {
                    return None;
                }
                (CONSTANT_PROPOSE_ROUTE, variable)
            }
            ProgramAction::Confirm(variable) => {
                if variable != self.variable || request.bound.is_set(variable) {
                    return None;
                }
                (CONSTANT_CONFIRM_ROUTE, variable)
            }
            ProgramAction::Support => (
                if request.bound.is_set(self.variable) {
                    CONSTANT_SUPPORT_BOUND_ROUTE
                } else {
                    CONSTANT_SUPPORT_UNBOUND_ROUTE
                },
                self.variable,
            ),
        };
        Some(ProgramRoute {
            key,
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            ConstantProgramState::Propose => CONSTANT_PROPOSE_DISPATCH,
            ConstantProgramState::Confirm { .. } => CONSTANT_CONFIRM_DISPATCH,
            ConstantProgramState::Support => CONSTANT_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        match state {
            ConstantProgramState::Support => [1, 0],
            ConstantProgramState::Confirm { offset } => [
                2,
                u64::MAX
                    - u64::try_from(*offset).expect("constant candidate offset exceeds rank limb"),
            ],
            ConstantProgramState::Propose => [3, 0],
        }
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.route.stratum, ProgramStratum::Finite);
        assert_eq!(batch.route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(batch.route.completion, ProgramCompletion::PageableOnly);
        let state = match batch.request.action {
            ProgramAction::Propose(variable) => {
                assert_eq!(variable, self.variable);
                assert!(!batch.request.bound.is_set(variable));
                ConstantProgramState::Propose
            }
            ProgramAction::Confirm(variable) => {
                assert_eq!(variable, self.variable);
                assert!(!batch.request.bound.is_set(variable));
                ConstantProgramState::Confirm { offset: 0 }
            }
            ProgramAction::Support => ConstantProgramState::Support,
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed constant parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.stratum, ProgramStratum::Finite);
        assert_eq!(states.len(), batch.view.len());
        assert_eq!(states.len(), batch.candidate_sets.len());
        assert_eq!(states.len(), batch.limits.len());
        let Some(first) = states.first() else {
            return;
        };
        match first {
            ConstantProgramState::Propose => {
                for (input, state) in states.drain(..).enumerate() {
                    assert_eq!(state, ConstantProgramState::Propose);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed constant proposal received a candidate group"
                    );
                    assert!(batch.limits[input] > 0, "typed constant proposal has zero demand");
                    effects.direct(
                        u32::try_from(input).expect("too many typed constant inputs"),
                        self.constant,
                    );
                    effects.account_source(1, 0);
                    effects.page(1, None);
                }
            }
            ConstantProgramState::Confirm { .. } => {
                for (input, state) in states.drain(..).enumerate() {
                    let ConstantProgramState::Confirm { offset } = state else {
                        panic!("one typed constant confirmation cohort mixed action variants")
                    };
                    let candidates = batch.candidate_sets[input]
                        .expect("typed constant confirmation lost its candidate group");
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag =
                        u32::try_from(input).expect("too many typed constant inputs in one cohort");
                    for &candidate in &candidates[offset..end] {
                        if candidate == self.constant {
                            effects.accept(input_tag, candidate);
                        }
                    }
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "typed constant confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(ConstantProgramState::Confirm { offset: end })
                    });
                    effects.page(examined, resume);
                }
            }
            ConstantProgramState::Support => {
                let column = batch.view.col(self.variable);
                for (input, state) in states.drain(..).enumerate() {
                    assert_eq!(state, ConstantProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed constant support received a candidate group"
                    );
                    if column.is_none_or(|column| batch.view.row(input)[column] == self.constant) {
                        effects.support(
                            u32::try_from(input).expect("too many typed constant inputs"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'a> Constraint<'a> for ConstantConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if variable == self.variable && !bound.is_set(variable) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    /// Always estimates exactly one candidate, for every row.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if self.variable != variable {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    /// Proposes the single constant value for every row.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable == variable {
            for i in 0..view.len() as u32 {
                candidates.push(i, self.constant);
            }
        }
    }

    /// The constant is binding-independent, so confirm is a single retain
    /// over the whole frontier — no per-row work at all.
    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable == variable {
            candidates.retain(|_, v| *v == self.constant);
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Returns `false` when any row binds the variable to another value.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable) {
            Some(col) => view.iter().all(|row| row[col] == self.constant),
            None => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

#[cfg(test)]
mod typed_program_tests {
    use super::*;
    use crate::inline::encodings::UnknownInline;

    #[test]
    fn constant_program_routes_are_structural_and_ranks_descend() {
        let variable = Variable::<UnknownInline>::new(3);
        let constraint = ConstantConstraint::new(variable, Inline::new([7; 32]));
        let program = constraint.residual_program().unwrap();
        let empty = VariableSet::new_empty();
        let propose = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: empty,
            })
            .unwrap();
        let confirm = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(variable.index),
                bound: empty,
            })
            .unwrap();
        assert_ne!(propose.key, confirm.key);
        assert_eq!(propose.stratum, ProgramStratum::Finite);
        assert_eq!(propose.grouping, ProgramGrouping::PageLocal);
        assert_eq!(propose.completion, ProgramCompletion::PageableOnly);
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: VariableSet::new_singleton(variable.index),
            })
            .is_none());
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(9),
                bound: empty,
            })
            .is_none());
        assert!(
            constraint.progress(&ConstantProgramState::Confirm { offset: 0 })
                > constraint.progress(&ConstantProgramState::Confirm { offset: 1 })
        );
    }
}
