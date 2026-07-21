use super::*;

/// Constrains two variables to have the same value.
///
/// Used to express variable equality when two positions in a triple
/// share the same logical variable but need distinct [`VariableId`]s
/// for the `TribleSetConstraint`
/// (which assumes its three positions have distinct ids).
///
/// The macro layer emits this automatically when a `_?var` appears in
/// both the entity and value positions of the same triple.
pub struct EqualityConstraint {
    a: VariableId,
    b: VariableId,
}

/// Canonical finite continuation for [`EqualityConstraint`].
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EqualityProgramState {
    Propose { variable: VariableId },
    Confirm { variable: VariableId, offset: usize },
    Support,
}

const EQUALITY_PROPOSE_ROUTE: u32 = 1 << 4;
const EQUALITY_CONFIRM_ROUTE: u32 = 2 << 4;
const EQUALITY_SUPPORT_ROUTE: u32 = 3 << 4;

const EQUALITY_PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const EQUALITY_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const EQUALITY_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

impl EqualityConstraint {
    /// Creates a constraint requiring `a` and `b` to be bound to the
    /// same raw value.
    pub fn new(a: VariableId, b: VariableId) -> Self {
        EqualityConstraint { a, b }
    }

    /// Column of the peer of `variable` in `view`, when `variable` is
    /// one of the constrained pair and the peer is bound.
    fn peer_col(&self, variable: VariableId, view: &RowsView<'_>) -> Option<usize> {
        let peer = if variable == self.a {
            self.b
        } else if variable == self.b {
            self.a
        } else {
            return None;
        };
        view.col(peer)
    }

    fn variable_mask(&self, variable: VariableId) -> u32 {
        u32::from(variable == self.a) | (u32::from(variable == self.b) << 1)
    }

    fn bound_mask(&self, bound: VariableSet) -> u32 {
        u32::from(bound.is_set(self.a)) | (u32::from(bound.is_set(self.b)) << 1)
    }

    fn peer(&self, variable: VariableId) -> Option<VariableId> {
        if variable == self.a {
            Some(self.b)
        } else if variable == self.b {
            Some(self.a)
        } else {
            None
        }
    }

    fn support_row(&self, view: &RowsView<'_>, row: &[RawInline]) -> bool {
        match (view.col(self.a), view.col(self.b)) {
            (Some(a), Some(b)) => row[a] == row[b],
            _ => true,
        }
    }
}

impl TypedProgramSpec for EqualityConstraint {
    type State = EqualityProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 2];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let bound_mask = self.bound_mask(request.bound);
        let (action, variable) = match request.action {
            ProgramAction::Propose(variable) => {
                let target_mask = self.variable_mask(variable);
                if target_mask == 0 || request.bound.is_set(variable) {
                    return None;
                }
                let peer = self.peer(variable)?;
                if peer == variable || !request.bound.is_set(peer) {
                    return None;
                }
                (
                    EQUALITY_PROPOSE_ROUTE | (target_mask << 2) | bound_mask,
                    variable,
                )
            }
            ProgramAction::Confirm(variable) => {
                let target_mask = self.variable_mask(variable);
                if target_mask == 0 || request.bound.is_set(variable) {
                    return None;
                }
                (
                    EQUALITY_CONFIRM_ROUTE | (target_mask << 2) | bound_mask,
                    variable,
                )
            }
            ProgramAction::Support => (EQUALITY_SUPPORT_ROUTE | bound_mask, self.a),
        };
        Some(ProgramRoute {
            key: ProgramKey::new(action),
            variable,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Explicit,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            EqualityProgramState::Propose { .. } => EQUALITY_PROPOSE_DISPATCH,
            EqualityProgramState::Confirm { .. } => EQUALITY_CONFIRM_DISPATCH,
            EqualityProgramState::Support => EQUALITY_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        match state {
            EqualityProgramState::Support => [1, 0],
            EqualityProgramState::Confirm { offset, .. } => [
                2,
                u64::MAX
                    - u64::try_from(*offset).expect("equality candidate offset exceeds rank limb"),
            ],
            EqualityProgramState::Propose { .. } => [3, 0],
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
                let peer = self
                    .peer(variable)
                    .expect("typed equality proposal targeted an unrelated variable");
                assert_ne!(peer, variable);
                assert!(!batch.request.bound.is_set(variable));
                assert!(batch.request.bound.is_set(peer));
                assert_eq!(batch.route.variable, variable);
                EqualityProgramState::Propose { variable }
            }
            ProgramAction::Confirm(variable) => {
                assert_ne!(self.variable_mask(variable), 0);
                assert!(!batch.request.bound.is_set(variable));
                assert_eq!(batch.route.variable, variable);
                EqualityProgramState::Confirm {
                    variable,
                    offset: 0,
                }
            }
            ProgramAction::Support => EqualityProgramState::Support,
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed equality parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
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
            EqualityProgramState::Propose { variable } => {
                let variable = *variable;
                let peer = self
                    .peer(variable)
                    .expect("typed equality proposal targeted an unrelated variable");
                assert_ne!(peer, variable);
                let peer_column = batch
                    .view
                    .col(peer)
                    .expect("typed equality proposal lost its bound peer");
                for (input, state) in states.into_iter().enumerate() {
                    assert_eq!(state, EqualityProgramState::Propose { variable });
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed equality proposal received a candidate group"
                    );
                    assert!(batch.limits[input] > 0, "typed equality proposal has zero demand");
                    effects.direct(
                        u32::try_from(input).expect("too many typed equality inputs"),
                        batch.view.row(input)[peer_column],
                    );
                    effects.account_source(1, 0);
                    effects.page(1, None);
                }
            }
            EqualityProgramState::Confirm { variable, .. } => {
                let variable = *variable;
                let peer_column = self.peer(variable).and_then(|peer| batch.view.col(peer));
                for (input, state) in states.into_iter().enumerate() {
                    let EqualityProgramState::Confirm {
                        variable: state_variable,
                        offset,
                    } = state
                    else {
                        panic!("one typed equality confirmation cohort mixed action variants")
                    };
                    assert_eq!(state_variable, variable);
                    let candidates = batch.candidate_sets[input]
                        .expect("typed equality confirmation lost its candidate group");
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag =
                        u32::try_from(input).expect("too many typed equality inputs in one cohort");
                    for &candidate in &candidates[offset..end] {
                        if peer_column
                            .is_none_or(|column| candidate == batch.view.row(input)[column])
                        {
                            effects.accept(input_tag, candidate);
                        }
                    }
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "typed equality confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(EqualityProgramState::Confirm {
                            variable,
                            offset: end,
                        })
                    });
                    effects.page(examined, resume);
                }
            }
            EqualityProgramState::Support => {
                for (input, state) in states.into_iter().enumerate() {
                    assert_eq!(state, EqualityProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed equality support received a candidate group"
                    );
                    if self.support_row(&batch.view, batch.view.row(input)) {
                        effects.support(
                            u32::try_from(input).expect("too many typed equality inputs"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'c> Constraint<'c> for EqualityConstraint {
    fn variables(&self) -> VariableSet {
        let mut vs = VariableSet::new_empty();
        vs.set(self.a);
        vs.set(self.b);
        vs
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    /// Equality becomes an exact finite source only after the peer variable is
    /// bound. With both variables free it remains a validator rather than
    /// pretending to own the universe of raw values.
    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if bound.is_set(variable) {
            return ProposalCoverage::None;
        }
        match self.peer(variable) {
            Some(peer) if peer != variable && bound.is_set(peer) => ProposalCoverage::Exact,
            _ => ProposalCoverage::None,
        }
    }

    /// Estimates exactly one candidate per row when the peer variable is
    /// already bound. Returns `false` when the peer is unbound — the
    /// constraint has no independent opinion about the variable's
    /// cardinality and defers to other constraints in the intersection.
    /// This is safe as long as each variable also appears in at least
    /// one other constraint (which the macro desugaring guarantees).
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if self.peer_col(variable, view).is_none() {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    /// Proposes each row's peer value.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let Some(col) = self.peer_col(variable, view) else {
            return;
        };
        for (i, row) in view.iter().enumerate() {
            candidates.push(i as u32, row[col]);
        }
    }

    /// Retains only candidates matching their row's peer value.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let Some(col) = self.peer_col(variable, view) else {
            return;
        };
        candidates.retain(|row, v| *v == view.row(row as usize)[col]);
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Returns `false` when any row binds the pair to different values.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match (view.col(self.a), view.col(self.b)) {
            (Some(ca), Some(cb)) => view.iter().all(|row| row[ca] == row[cb]),
            _ => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

#[cfg(test)]
mod typed_program_tests {
    use super::*;

    #[test]
    fn equality_program_routes_cover_peer_bound_identity_and_same_variable() {
        let equality = EqualityConstraint::new(2, 5);
        let program = equality.residual_program().unwrap();
        let empty = VariableSet::new_empty();
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(2),
                bound: empty,
            })
            .is_none());
        let unbound_confirmation = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(2),
                bound: empty,
            })
            .unwrap();
        assert_eq!(unbound_confirmation.exposure, ProgramExposure::Explicit);

        let peer_bound = VariableSet::new_singleton(2);
        let proposal = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(5),
                bound: peer_bound,
            })
            .unwrap();
        assert_eq!(proposal.variable, 5);
        assert_eq!(proposal.stratum, ProgramStratum::Finite);
        assert_eq!(proposal.grouping, ProgramGrouping::PageLocal);
        assert_eq!(proposal.exposure, ProgramExposure::Explicit);
        let confirmation = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(5),
                bound: peer_bound,
            })
            .unwrap();
        assert_eq!(confirmation.exposure, ProgramExposure::Explicit);
        let support = program
            .route(ProgramRequest {
                action: ProgramAction::Support,
                bound: peer_bound,
            })
            .unwrap();
        assert_eq!(support.exposure, ProgramExposure::Explicit);
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(2),
                bound: peer_bound,
            })
            .is_none());
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(9),
                bound: empty,
            })
            .is_none());

        let same = EqualityConstraint::new(4, 4);
        let same_program = same.residual_program().unwrap();
        assert!(same_program
            .route(ProgramRequest {
                action: ProgramAction::Propose(4),
                bound: empty,
            })
            .is_none());
        let same_confirmation = same_program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(4),
                bound: empty,
            })
            .unwrap();
        assert_eq!(same_confirmation.exposure, ProgramExposure::Explicit);
        assert!(
            same.progress(&EqualityProgramState::Confirm {
                variable: 4,
                offset: 0,
            }) > same.progress(&EqualityProgramState::Confirm {
                variable: 4,
                offset: 1,
            })
        );
    }
}
