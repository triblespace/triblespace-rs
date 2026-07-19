use super::*;

/// Restricts a variable's raw value to a byte-lexicographic range.
///
/// This constraint only **confirms** — it never proposes candidates.
/// Use it with [`and!`](crate::and) alongside a constraint that does
/// propose (e.g. a [`pattern!`](crate::macros::pattern)):
///
/// ```rust,ignore
/// find!((id: Id, ts: Inline<NsTAIInterval>),
///     and!(
///         pattern!(data, [{ ?id @ exec::requested_at: ?ts }]),
///         value_range(ts, min_ts, max_ts),
///     )
/// )
/// ```
///
/// The estimate returns `usize::MAX` so the intersection sorts this
/// constraint last — the tighter TribleSet constraint proposes first,
/// then this range constraint filters.
pub struct InlineRange {
    variable: VariableId,
    min: RawInline,
    max: RawInline,
}

/// Canonical finite continuation for [`InlineRange`].
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InlineRangeProgramState {
    Confirm { offset: usize },
    Support,
}

const INLINE_RANGE_CONFIRM_ROUTE: ProgramKey = ProgramKey::new(0);
const INLINE_RANGE_SUPPORT_UNBOUND_ROUTE: ProgramKey = ProgramKey::new(1);
const INLINE_RANGE_SUPPORT_BOUND_ROUTE: ProgramKey = ProgramKey::new(2);

const INLINE_RANGE_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(0);
const INLINE_RANGE_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(1);

impl InlineRange {
    /// Create a range constraint on `variable` with inclusive bounds.
    pub fn new<T: InlineEncoding>(variable: Variable<T>, min: Inline<T>, max: Inline<T>) -> Self {
        InlineRange {
            variable: variable.index,
            min: min.raw,
            max: max.raw,
        }
    }

    fn contains(&self, value: &RawInline) -> bool {
        *value >= self.min && *value <= self.max
    }
}

/// Convenience function to create a [`InlineRange`] constraint.
pub fn value_range<T: InlineEncoding>(
    variable: Variable<T>,
    min: Inline<T>,
    max: Inline<T>,
) -> InlineRange {
    InlineRange::new(variable, min, max)
}

impl TypedProgramSpec for InlineRange {
    type State = InlineRangeProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 2];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let (key, variable) = match request.action {
            // InlineRange is intentionally a filter-only atom. Treating the
            // raw byte interval as an enumerable domain would invent a source
            // that the ordinary constraint deliberately does not own.
            ProgramAction::Propose(_) => return None,
            ProgramAction::Confirm(variable) => {
                if variable != self.variable || request.bound.is_set(variable) {
                    return None;
                }
                (INLINE_RANGE_CONFIRM_ROUTE, variable)
            }
            ProgramAction::Support => (
                if request.bound.is_set(self.variable) {
                    INLINE_RANGE_SUPPORT_BOUND_ROUTE
                } else {
                    INLINE_RANGE_SUPPORT_UNBOUND_ROUTE
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
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state {
            InlineRangeProgramState::Confirm { .. } => INLINE_RANGE_CONFIRM_DISPATCH,
            InlineRangeProgramState::Support => INLINE_RANGE_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        match state {
            InlineRangeProgramState::Support => [1, 0],
            InlineRangeProgramState::Confirm { offset } => [
                2,
                u64::MAX
                    - u64::try_from(*offset)
                        .expect("inline-range candidate offset exceeds rank limb"),
            ],
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
            ProgramAction::Propose(_) => {
                panic!("filter-only InlineRange admitted a typed proposal")
            }
            ProgramAction::Confirm(variable) => {
                assert_eq!(variable, self.variable);
                assert!(!batch.request.bound.is_set(variable));
                assert_eq!(batch.route.variable, variable);
                InlineRangeProgramState::Confirm { offset: 0 }
            }
            ProgramAction::Support => InlineRangeProgramState::Support,
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed inline-range parents"),
                state.clone(),
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
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
            InlineRangeProgramState::Confirm { .. } => {
                for (input, state) in states.into_iter().enumerate() {
                    let InlineRangeProgramState::Confirm { offset } = state else {
                        panic!("one typed inline-range cohort mixed action variants")
                    };
                    let candidates = batch.candidate_sets[input]
                        .expect("typed inline-range confirmation lost its candidate group");
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag = u32::try_from(input)
                        .expect("too many typed inline-range inputs in one cohort");
                    for &candidate in &candidates[offset..end] {
                        if self.contains(&candidate) {
                            effects.accept(input_tag, candidate);
                        }
                    }
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "typed inline-range confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(InlineRangeProgramState::Confirm { offset: end })
                    });
                    effects.page(examined, resume);
                }
            }
            InlineRangeProgramState::Support => {
                let column = batch.view.col(self.variable);
                for (input, state) in states.into_iter().enumerate() {
                    assert_eq!(state, InlineRangeProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed inline-range support received a candidate group"
                    );
                    if column.is_none_or(|column| self.contains(&batch.view.row(input)[column])) {
                        effects.support(
                            u32::try_from(input).expect("too many typed inline-range inputs"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'a> Constraint<'a> for InlineRange {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    /// Estimates `usize::MAX` so the intersection never chooses this
    /// constraint as the proposer — it only confirms.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if self.variable != variable {
            return false;
        }
        out.fill(usize::MAX, view.len());
        true
    }

    /// Does not propose — the paired TribleSet constraint handles proposals.
    fn propose(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        // Intentionally empty: this constraint only confirms.
    }

    /// Retains only candidates whose raw bytes fall within [min, max]
    /// inclusive — value-only, so one retain over the whole frontier.
    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if self.variable == variable {
            candidates.retain(|_, value| self.contains(value));
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    /// Returns `false` when any row binds the variable outside the range.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable) {
            Some(col) => view
                .iter()
                .all(|row| row[col] >= self.min && row[col] <= self.max),
            None => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::InlineRangeProgramState;
    use crate::prelude::inlineencodings::R256;
    use crate::prelude::*;
    use crate::query::{
        Constraint, ProgramAction, ProgramCompletion, ProgramGrouping, ProgramRequest,
        ProgramStratum, TypedProgramSpec, VariableSet,
    };

    attributes! {
        "AA00000000000000AA00000000000000" as test_score: R256;
    }

    #[test]
    fn value_range_filters_correctly() {
        let e1 = ufoid();
        let e2 = ufoid();
        let e3 = ufoid();

        let v10: Inline<R256> = 10i128.to_inline();
        let v50: Inline<R256> = 50i128.to_inline();
        let v90: Inline<R256> = 90i128.to_inline();

        let mut data = TribleSet::new();
        data += entity! { &e1 @ test_score: v10 };
        data += entity! { &e2 @ test_score: v50 };
        data += entity! { &e3 @ test_score: v90 };

        // Without range: all 3 results.
        let all: Vec<Inline<R256>> = find!(
            v: Inline<R256>,
            pattern!(&data, [{ test_score: ?v }])
        )
        .collect();
        assert_eq!(all.len(), 3);

        // With range [20..80]: only v50.
        let min: Inline<R256> = 20i128.to_inline();
        let max: Inline<R256> = 80i128.to_inline();
        let filtered: Vec<Inline<R256>> = find!(
            v: Inline<R256>,
            and!(
                pattern!(&data, [{ test_score: ?v }]),
                value_range(v, min, max),
            )
        )
        .collect();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0], v50);
    }

    #[test]
    fn inline_range_program_is_a_finite_filter_but_never_a_source() {
        let variable = Variable::<R256>::new(4);
        let min: Inline<R256> = 20i128.to_inline();
        let max: Inline<R256> = 80i128.to_inline();
        let constraint = value_range(variable, min, max);
        let program = constraint.residual_program().unwrap();
        let empty = VariableSet::new_empty();
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: empty,
            })
            .is_none());
        let confirm = program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(variable.index),
                bound: empty,
            })
            .unwrap();
        assert_eq!(confirm.stratum, ProgramStratum::Finite);
        assert_eq!(confirm.grouping, ProgramGrouping::PageLocal);
        assert_eq!(confirm.completion, ProgramCompletion::PageableOnly);
        assert!(program
            .route(ProgramRequest {
                action: ProgramAction::Confirm(variable.index),
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
            constraint.progress(&InlineRangeProgramState::Confirm { offset: 0 })
                > constraint.progress(&InlineRangeProgramState::Confirm { offset: 1 })
        );
    }
}
