use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::DispatchClass;
use crate::query::EstimateSink;
use crate::query::ProgramPacing;
use crate::query::ProgramRef;
use crate::query::ProgramRequest;
use crate::query::ProgramRoute;
use crate::query::ProgramSeedBatch;
use crate::query::ResidualDeltaOutput;
use crate::query::ResidualDeltaSourceCursor;
use crate::query::ResidualDeltaSourcePage;
use crate::query::RowsView;
use crate::query::TypedEffectSink;
use crate::query::TypedProgramBatch;
use crate::query::TypedProgramSpec;
use crate::query::TypedSeedSink;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;

use super::triblesetconstraint::direct_source_page;
use super::triblesetconstraint::next_inline_source_in_range;

/// A value-range-aware constraint that uses the TribleSet's VEA index
/// to propose only values in a byte-lexicographic range.
///
/// When proposing for the value variable with the attribute bound, it
/// calls `infixes_range` on the AVE index — the trie skips entire
/// subtrees outside the range. This makes range queries O(k + pruned)
/// instead of O(n).
///
/// Create via [`TribleSet::value_in_range`]:
///
/// ```rust,ignore
/// find!((id: Id, ts: Inline<NsTAIInterval>),
///     and!(
///         pattern!(data, [{ ?id @ exec::requested_at: ?ts }]),
///         data.value_in_range(ts, min_ts, max_ts),
///     )
/// )
/// ```
pub struct TribleSetRangeConstraint {
    variable_v: VariableId,
    min: RawInline,
    max: RawInline,
    set: TribleSet,
    // Range bounds are constant and the set's VEA trie does not mutate during
    // query execution, so the estimate is a pure function of construction-time
    // inputs. Cache it to avoid re-walking the trie on every propose/confirm.
    cached_estimate: usize,
}

impl TribleSetRangeConstraint {
    pub fn new<V: InlineEncoding>(
        variable_v: Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
        set: TribleSet,
    ) -> Self {
        let cached_estimate = set
            .vea
            .count_range::<0, INLINE_LEN>(&[0u8; 0], &min.raw, &max.raw)
            .min(usize::MAX as u64) as usize;
        TribleSetRangeConstraint {
            variable_v: variable_v.index,
            min: min.raw,
            max: max.raw,
            set,
            cached_estimate,
        }
    }

    fn contains(&self, value: &RawInline) -> bool {
        *value >= self.min && *value <= self.max
    }
}

impl TypedProgramSpec for TribleSetRangeConstraint {
    type State = crate::query::finiteunaryprogram::FiniteUnaryProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 6];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        crate::query::finiteunaryprogram::route(self.variable_v, request)
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        crate::query::finiteunaryprogram::dispatch(state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        crate::query::finiteunaryprogram::pacing(state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        crate::query::finiteunaryprogram::progress(state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::seed(self.variable_v, batch, effects)
    }

    fn step_typed(
        &self,
        states: Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        crate::query::finiteunaryprogram::step(
            self.variable_v,
            states,
            batch,
            effects,
            |_input, cursor, limit, accepted| {
                direct_source_page(cursor, limit, accepted, |after| {
                    next_inline_source_in_range(
                        &self.set.vea,
                        &[],
                        &self.min,
                        &self.max,
                        after,
                    )
                })
            },
            |_input, value| self.contains(value),
        )
    }
}

impl<'a> Constraint<'a> for TribleSetRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_v)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable_v {
            return false;
        }
        out.fill(self.cached_estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable_v {
            return;
        }
        // Scan the VEA index for all values in range.
        // VEA tree order: V(32) → E(16) → A(16).
        // With empty prefix, infixes_range on V(32 bytes) gives us all
        // values in [min, max]. The trie prunes branches outside the range.
        for i in 0..view.len() as u32 {
            self.set
                .vea
                .infixes_range::<0, INLINE_LEN, _>(&[0u8; 0], &self.min, &self.max, |v| {
                    candidates.push(i, *v);
                });
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable_v {
            candidates.retain(|_, v| *v >= self.min && *v <= self.max);
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.variable_v && view.col(variable).is_none()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable != self.variable_v
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        Some(direct_source_page(cursor, limit, accepted, |after| {
            next_inline_source_in_range(&self.set.vea, &[], &self.min, &self.max, after)
        }))
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable_v) {
            Some(col) => view
                .iter()
                .all(|row| row[col] >= self.min && row[col] <= self.max),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::inline::RawInline;
    use crate::prelude::inlineencodings::R256BE;
    use crate::prelude::*;
    use crate::query::residual::ResidualLowering;
    use crate::query::residual::try_constructed_program_query;
    use crate::query::intersectionconstraint::IntersectionConstraint;
    use crate::query::Binding;
    use crate::query::Constraint;
    use crate::query::ProgramAction;
    use crate::query::ProgramCompletion;
    use crate::query::ProgramGrouping;
    use crate::query::ProgramRequest;
    use crate::query::ProgramStratum;
    use crate::query::Query;
    use crate::query::ResidualDeltaSourceCursor;
    use crate::query::RowsView;
    use crate::query::TypedProgramSpec;
    use crate::query::VariableContext;
    use crate::query::VariableId;
    use crate::query::VariableSet;

    attributes! {
        "BB00000000000000BB00000000000000" as range_test_score: R256BE;
    }

    #[test]
    fn value_in_range_proposes_correctly() {
        let e1 = ufoid();
        let e2 = ufoid();
        let e3 = ufoid();
        let e4 = ufoid();

        let v10: Inline<R256BE> = 10i128.to_inline();
        let v50: Inline<R256BE> = 50i128.to_inline();
        let v90: Inline<R256BE> = 90i128.to_inline();
        let v100: Inline<R256BE> = 100i128.to_inline();

        let mut data = TribleSet::new();
        data += entity! { &e1 @ range_test_score: v10 };
        data += entity! { &e2 @ range_test_score: v50 };
        data += entity! { &e3 @ range_test_score: v90 };
        data += entity! { &e4 @ range_test_score: v100 };

        // Without range: all 4 results.
        let all: Vec<Inline<R256BE>> = find!(
            v: Inline<R256BE>,
            pattern!(&data, [{ range_test_score: ?v }])
        )
        .collect();
        assert_eq!(all.len(), 4);

        // With value_in_range [20..=95]: only v50 and v90.
        let min: Inline<R256BE> = 20i128.to_inline();
        let max: Inline<R256BE> = 95i128.to_inline();
        let mut filtered: Vec<Inline<R256BE>> = find!(
            v: Inline<R256BE>,
            and!(
                pattern!(&data, [{ range_test_score: ?v }]),
                data.value_in_range(v, min, max),
            )
        )
        .collect();
        filtered.sort();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0], v50);
        assert_eq!(filtered[1], v90);

        // Boundary: exact match on min and max.
        let min_exact: Inline<R256BE> = 50i128.to_inline();
        let max_exact: Inline<R256BE> = 90i128.to_inline();
        let mut exact: Vec<Inline<R256BE>> = find!(
            v: Inline<R256BE>,
            and!(
                pattern!(&data, [{ range_test_score: ?v }]),
                data.value_in_range(v, min_exact, max_exact),
            )
        )
        .collect();
        exact.sort();
        assert_eq!(exact.len(), 2);
        assert_eq!(exact[0], v50);
        assert_eq!(exact[1], v90);

        // Empty range: no results.
        let min_empty: Inline<R256BE> = 91i128.to_inline();
        let max_empty: Inline<R256BE> = 99i128.to_inline();
        let empty: Vec<Inline<R256BE>> = find!(
            v: Inline<R256BE>,
            and!(
                pattern!(&data, [{ range_test_score: ?v }]),
                data.value_in_range(v, min_empty, max_empty),
            )
        )
        .collect();
        assert_eq!(empty.len(), 0);
    }

    /// Regression: the range-constraint estimate must report the count of
    /// *distinct values* in range — not the count of tribles. Multiple
    /// entities sharing the same value would otherwise inflate the
    /// estimate and bias intersection ordering.
    #[test]
    fn estimate_counts_distinct_values_not_tribles() {
        // Three distinct scores, but the middle one is shared by four
        // entities. Tribles-in-range would be 6, distinct-values-in-range
        // would be 3.
        let v10: Inline<R256BE> = 10i128.to_inline();
        let v50: Inline<R256BE> = 50i128.to_inline();
        let v90: Inline<R256BE> = 90i128.to_inline();

        let mut data = TribleSet::new();
        data += entity! { &ufoid() @ range_test_score: v10 };
        data += entity! { &ufoid() @ range_test_score: v50 };
        data += entity! { &ufoid() @ range_test_score: v50 };
        data += entity! { &ufoid() @ range_test_score: v50 };
        data += entity! { &ufoid() @ range_test_score: v50 };
        data += entity! { &ufoid() @ range_test_score: v90 };

        use crate::query::Constraint;
        use crate::query::VariableContext;
        let mut ctx = VariableContext::new();
        let v = ctx.next_variable::<R256BE>();

        let min: Inline<R256BE> = 0i128.to_inline();
        let max: Inline<R256BE> = 100i128.to_inline();
        let constraint = data.value_in_range(v, min, max);

        use crate::query::RowsView;
        let mut est = Vec::new();
        assert!(constraint.estimate(
            v.index,
            &RowsView::EMPTY,
            &mut crate::query::EstimateSink::Column(&mut est)
        ));
        // Three distinct values in range. Before the fix this returned 6.
        assert_eq!(est, vec![3], "estimate must count distinct values");
    }

    fn project(variable: VariableId, binding: &Binding) -> Option<RawInline> {
        binding.get(variable).copied()
    }

    #[test]
    fn value_range_pages_are_strict_distinct_and_lazy() {
        let v10: Inline<R256BE> = 10i128.to_inline();
        let v50: Inline<R256BE> = 50i128.to_inline();
        let v90: Inline<R256BE> = 90i128.to_inline();
        let v100: Inline<R256BE> = 100i128.to_inline();
        let mut data = TribleSet::new();
        data += entity! { &ufoid() @ range_test_score: v10 };
        data += entity! { &ufoid() @ range_test_score: v10 };
        data += entity! { &ufoid() @ range_test_score: v50 };
        data += entity! { &ufoid() @ range_test_score: v90 };
        data += entity! { &ufoid() @ range_test_score: v100 };

        let mut context = VariableContext::new();
        let variable = context.next_variable::<R256BE>();
        let constraint = data.value_in_range(variable, v10, v90);
        assert!(constraint.residual_proposal_source_is_paged(variable.index, &RowsView::EMPTY));
        let route = constraint
            .residual_program()
            .expect("value ranges expose a typed Program")
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: VariableSet::new_empty(),
            })
            .expect("the unbound range variable has an ordered source");
        assert_eq!(route.stratum, ProgramStratum::Finite);
        assert_eq!(route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(route.completion, ProgramCompletion::PageableOnly);
        assert!(
            constraint.progress(
                &crate::query::finiteunaryprogram::FiniteUnaryProgramState::Propose {
                    cursor: ResidualDeltaSourceCursor::Start,
                }
            ) > constraint.progress(
                &crate::query::finiteunaryprogram::FiniteUnaryProgramState::Propose {
                    cursor: ResidualDeltaSourceCursor::After(v10.raw),
                }
            )
        );

        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let first = constraint
            .residual_delta_source_page(
                variable.index,
                &RowsView::EMPTY,
                None,
                ResidualDeltaSourceCursor::Start,
                1,
                &mut roots,
                &mut direct,
            )
            .expect("value ranges expose their PATCH frontier directly");
        assert!(roots.is_empty());
        assert_eq!(direct, [v10.raw]);
        assert_eq!(first.examined, 1);
        assert_eq!(first.next, Some(ResidualDeltaSourceCursor::After(v10.raw)));

        let second = constraint
            .residual_delta_source_page(
                variable.index,
                &RowsView::EMPTY,
                None,
                first.next.unwrap(),
                2,
                &mut roots,
                &mut direct,
            )
            .expect("the strict cursor resumes the same immutable range");
        assert_eq!(direct, [v10.raw, v50.raw, v90.raw]);
        assert_eq!(second.examined, 2);
        assert_eq!(second.next, None);

        let mut expected: Vec<_> =
            Query::new(data.value_in_range(variable, v10, v90), move |binding| {
                project(variable.index, binding)
            })
            .sequential()
            .collect();
        let mut query = Query::new(data.value_in_range(variable, v10, v90), move |binding| {
            project(variable.index, binding)
        })
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(actual, [v10.raw, v50.raw, v90.raw]);
        assert_eq!(query.stats().delta_source_candidates_examined, 3);
        assert_eq!(query.stats().delta_source_direct_candidates, 3);
        assert_eq!(query.stats().delta_source_roots, 0);

        let mut constructed: Vec<_> = try_constructed_program_query(
            IntersectionConstraint::new(vec![data.value_in_range(variable, v10, v90)]),
            move |binding| project(variable.index, binding),
        )
        .expect("the range Program constructs without an opaque fallback")
        .cap(1)
        .start_width(1)
        .growth(1)
        .collect();
        constructed.sort_unstable();
        assert_eq!(constructed, expected);

        let mut first_only = Query::new(data.value_in_range(variable, v10, v90), move |binding| {
            project(variable.index, binding)
        })
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        assert_eq!(first_only.next(), Some(v10.raw));
        assert_eq!(first_only.stats().delta_source_candidates_examined, 1);
        assert_eq!(first_only.stats().delta_source_direct_candidates, 1);
        drop(first_only);
    }

    #[test]
    fn value_range_direct_source_preserves_affine_parent_bags_and_growth() {
        let v10: Inline<R256BE> = 10i128.to_inline();
        let v50: Inline<R256BE> = 50i128.to_inline();
        let v70: Inline<R256BE> = 70i128.to_inline();
        let v90: Inline<R256BE> = 90i128.to_inline();
        let mut base = TribleSet::new();
        base += entity! { &ufoid() @ range_test_score: v10 };
        base += entity! { &ufoid() @ range_test_score: v50 };
        base += entity! { &ufoid() @ range_test_score: v90 };
        let mut grown = base.clone();
        grown += entity! { &ufoid() @ range_test_score: v70 };

        let mut context = VariableContext::new();
        let parent = context.next_variable::<R256BE>();
        let value = context.next_variable::<R256BE>();
        let duplicate_parents = [v10, v10];
        let constraint = and!(
            SortedSlice::new(&duplicate_parents).unwrap().has(parent),
            grown.value_in_range(value, v10, v90),
        );
        let mut actual: Vec<_> =
            Query::new(constraint, move |binding| project(value.index, binding))
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .cap(1)
                .start_width(1)
                .collect();
        actual.sort_unstable();
        assert_eq!(
            actual,
            [v10.raw, v10.raw, v50.raw, v50.raw, v70.raw, v70.raw, v90.raw, v90.raw]
        );

        let collect = |set: &TribleSet| {
            let mut values: Vec<_> =
                Query::new(set.value_in_range(value, v10, v90), move |binding| {
                    project(value.index, binding)
                })
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .cap(1)
                .start_width(1)
                .collect();
            values.sort_unstable();
            values
        };
        let before = collect(&base);
        let after = collect(&grown);
        assert_eq!(before, [v10.raw, v50.raw, v90.raw]);
        assert_eq!(after, [v10.raw, v50.raw, v70.raw, v90.raw]);

        let inverted = grown.value_in_range(value, v90, v10);
        let mut roots = Vec::new();
        let mut direct = Vec::new();
        let empty = inverted
            .residual_delta_source_page(
                value.index,
                &RowsView::EMPTY,
                None,
                ResidualDeltaSourceCursor::Start,
                1,
                &mut roots,
                &mut direct,
            )
            .expect("an inverted immutable range is an exact empty frontier");
        assert_eq!(empty.examined, 0);
        assert_eq!(empty.next, None);
        assert!(direct.is_empty());
        assert!(roots.is_empty());
    }
}
