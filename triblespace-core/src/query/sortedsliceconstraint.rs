use crate::inline::IntoInline;
use crate::inline::TryFromInline;

use super::*;

/// A verified-sorted slice of values.
///
/// Use [`SortedSlice::new`] to validate sort order, or
/// [`SortedSlice::new_unchecked`] when the caller guarantees ordering.
/// Implements [`ContainsConstraint`] so it can be used with `.has()`
/// in queries — confirm uses binary search for O(log n) filtering
/// instead of the O(n) linear scan of [`HashSet`](std::collections::HashSet).
///
/// Derefs to `&[T]` for direct access to slice methods.
#[derive(Debug, Clone, Copy)]
pub struct SortedSlice<'a, T>(pub &'a [T]);

/// Error returned by [`SortedSlice::new`] when the input is not sorted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotSortedError;

impl std::fmt::Display for NotSortedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "slice is not sorted")
    }
}

impl std::error::Error for NotSortedError {}

impl<'a, T: Ord> SortedSlice<'a, T> {
    /// Creates a sorted slice, verifying that `data` is sorted.
    pub fn new(data: &'a [T]) -> Result<Self, NotSortedError> {
        if data.windows(2).all(|w| w[0] <= w[1]) {
            Ok(SortedSlice(data))
        } else {
            Err(NotSortedError)
        }
    }

    /// Creates a sorted slice without verifying sort order.
    ///
    /// # Safety (logical)
    ///
    /// The caller must ensure `data` is sorted in ascending order.
    /// Unsorted data will produce incorrect query results.
    pub fn new_unchecked(data: &'a [T]) -> Self {
        SortedSlice(data)
    }

    /// Sorts `data` in place and wraps it. Convenience for callers that
    /// have a mutable slice (e.g. via `&mut Vec<T>`) and don't want to
    /// manage the sort themselves.
    pub fn from_mut(data: &'a mut [T]) -> Self {
        data.sort_unstable();
        SortedSlice(data)
    }
}

impl<T> std::ops::Deref for SortedSlice<'_, T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.0
    }
}

/// Constraint backed by a sorted slice — binary search for confirm.
pub struct SortedSliceConstraint<'a, S: InlineEncoding, T> {
    variable: Variable<S>,
    slice: SortedSlice<'a, T>,
}

/// Canonical finite continuation for [`SortedSliceConstraint`].
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SortedSliceProgramState {
    Propose { offset: usize },
    Confirm { offset: usize },
    Support,
}

const SORTED_SLICE_PROPOSE_ROUTE: ProgramKey = ProgramKey::new(0);
const SORTED_SLICE_CONFIRM_ROUTE: ProgramKey = ProgramKey::new(1);
const SORTED_SLICE_SUPPORT_UNBOUND_ROUTE: ProgramKey = ProgramKey::new(2);
const SORTED_SLICE_SUPPORT_BOUND_ROUTE: ProgramKey = ProgramKey::new(3);

const SORTED_SLICE_PROPOSE_DISPATCH: DispatchClass = DispatchClass::new(0);
const SORTED_SLICE_CONFIRM_DISPATCH: DispatchClass = DispatchClass::new(1);
const SORTED_SLICE_SUPPORT_DISPATCH: DispatchClass = DispatchClass::new(2);

impl<'a, S: InlineEncoding, T> SortedSliceConstraint<'a, S, T> {
    /// Creates a constraint that restricts `variable` to values in `slice`.
    pub fn new(variable: Variable<S>, slice: SortedSlice<'a, T>) -> Self {
        SortedSliceConstraint { variable, slice }
    }
}

impl<S: InlineEncoding, T> SortedSliceConstraint<'_, S, T>
where
    T: Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    fn contains_raw(&self, value: &RawInline) -> bool {
        match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(value)) {
            Ok(value) => self.slice.0.binary_search(&value).is_ok(),
            Err(_) => false,
        }
    }

    fn proposal_page_from_offset(
        &self,
        begin: usize,
        limit: usize,
        accepted: &mut Vec<RawInline>,
    ) -> ResidualDeltaSourcePage {
        assert!(limit > 0, "sorted-slice source pages require positive demand");
        assert!(
            begin <= self.slice.0.len(),
            "sorted-slice source cursor exceeds the immutable frontier"
        );
        let end = begin.saturating_add(limit).min(self.slice.0.len());
        accepted.extend(
            self.slice.0[begin..end]
                .iter()
                .map(|value| IntoInline::to_inline(value).raw),
        );
        ResidualDeltaSourcePage {
            next: (end < self.slice.0.len()).then(|| {
                ResidualDeltaSourceCursor::Offset(
                    u64::try_from(end).expect("sorted-slice source offset exceeds u64"),
                )
            }),
            examined: end - begin,
        }
    }
}

impl<S: InlineEncoding, T> TypedProgramSpec for SortedSliceConstraint<'_, S, T>
where
    T: Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type State = SortedSliceProgramState;
    type NoveltyKey = ();
    type Rank = [u64; 2];

    fn exposures(&self) -> crate::query::ProgramExposureSet {
        crate::query::ProgramExposureSet::PRODUCTION
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        let (key, variable) = match request.action {
            ProgramAction::Propose(variable) => {
                if variable != self.variable.index || request.bound.is_set(variable) {
                    return None;
                }
                (SORTED_SLICE_PROPOSE_ROUTE, variable)
            }
            ProgramAction::Confirm(variable) => {
                if variable != self.variable.index || request.bound.is_set(variable) {
                    return None;
                }
                (SORTED_SLICE_CONFIRM_ROUTE, variable)
            }
            ProgramAction::Support => (
                if request.bound.is_set(self.variable.index) {
                    SORTED_SLICE_SUPPORT_BOUND_ROUTE
                } else {
                    SORTED_SLICE_SUPPORT_UNBOUND_ROUTE
                },
                self.variable.index,
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
            SortedSliceProgramState::Propose { .. } => SORTED_SLICE_PROPOSE_DISPATCH,
            SortedSliceProgramState::Confirm { .. } => SORTED_SLICE_CONFIRM_DISPATCH,
            SortedSliceProgramState::Support => SORTED_SLICE_SUPPORT_DISPATCH,
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        match state {
            SortedSliceProgramState::Support => [1, 0],
            SortedSliceProgramState::Confirm { offset } => [
                2,
                u64::MAX
                    - u64::try_from(*offset)
                        .expect("sorted-slice candidate offset exceeds rank limb"),
            ],
            SortedSliceProgramState::Propose { offset } => [
                3,
                u64::MAX
                    - u64::try_from(*offset).expect("sorted-slice offset exceeds rank limb"),
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
            ProgramAction::Propose(variable) => {
                assert_eq!(variable, self.variable.index);
                assert!(!batch.request.bound.is_set(variable));
                SortedSliceProgramState::Propose { offset: 0 }
            }
            ProgramAction::Confirm(variable) => {
                assert_eq!(variable, self.variable.index);
                assert!(!batch.request.bound.is_set(variable));
                SortedSliceProgramState::Confirm { offset: 0 }
            }
            ProgramAction::Support => SortedSliceProgramState::Support,
        };
        for parent in 0..batch.view.len() {
            effects.finite_root(
                u32::try_from(parent).expect("too many typed sorted-slice parents"),
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
            SortedSliceProgramState::Propose { .. } => {
                for (input, state) in states.drain(..).enumerate() {
                    let SortedSliceProgramState::Propose { offset } = state else {
                        panic!("one typed sorted-slice cohort mixed action variants")
                    };
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed sorted-slice proposal received a candidate group"
                    );
                    let mut direct = Vec::new();
                    let page = self.proposal_page_from_offset(
                        offset,
                        batch.limits[input],
                        &mut direct,
                    );
                    let input_tag = u32::try_from(input)
                        .expect("too many typed sorted-slice inputs in one cohort");
                    for value in direct {
                        effects.direct(input_tag, value);
                    }
                    let resume = page.next.map(|cursor| {
                        let ResidualDeltaSourceCursor::Offset(offset) = cursor else {
                            unreachable!("sorted-slice source changed cursor family")
                        };
                        TypedResume::Immediate(SortedSliceProgramState::Propose {
                            offset: usize::try_from(offset)
                                .expect("sorted-slice source offset exceeds usize"),
                        })
                    });
                    effects.account_source(page.examined, 0);
                    effects.page(page.examined, resume);
                }
            }
            SortedSliceProgramState::Confirm { .. } => {
                for (input, state) in states.drain(..).enumerate() {
                    let SortedSliceProgramState::Confirm { offset } = state else {
                        panic!("one typed sorted-slice cohort mixed action variants")
                    };
                    let candidates = batch.candidate_sets[input]
                        .expect("typed sorted-slice confirmation lost its candidate group");
                    assert!(offset <= candidates.len());
                    let end = offset
                        .saturating_add(batch.limits[input])
                        .min(candidates.len());
                    let input_tag = u32::try_from(input)
                        .expect("too many typed sorted-slice inputs in one cohort");
                    for &candidate in &candidates[offset..end] {
                        if self.contains_raw(&candidate) {
                            effects.accept(input_tag, candidate);
                        }
                    }
                    let examined = end - offset;
                    assert!(
                        end == candidates.len() || examined > 0,
                        "typed sorted-slice confirmation resumed without examining a candidate"
                    );
                    let resume = (end < candidates.len()).then(|| {
                        TypedResume::Immediate(SortedSliceProgramState::Confirm { offset: end })
                    });
                    effects.page(examined, resume);
                }
            }
            SortedSliceProgramState::Support => {
                let column = batch.view.col(self.variable.index);
                for (input, state) in states.drain(..).enumerate() {
                    assert_eq!(state, SortedSliceProgramState::Support);
                    assert!(
                        batch.candidate_sets[input].is_none(),
                        "typed sorted-slice support received a candidate group"
                    );
                    if column
                        .is_none_or(|column| self.contains_raw(&batch.view.row(input)[column]))
                    {
                        effects.support(
                            u32::try_from(input).expect("too many typed sorted-slice inputs"),
                        );
                    }
                    effects.page(1, None);
                }
            }
        }
    }
}

impl<'a, S: InlineEncoding, T> Constraint<'a> for SortedSliceConstraint<'a, S, T>
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
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
        out.fill(self.slice.0.len(), view.len());
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
                candidates.extend_row(i, self.slice.0.iter().map(|v| IntoInline::to_inline(v).raw));
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

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.variable.index == variable && view.col(variable).is_none()
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
        if self.variable.index != variable
            || view.len() != 1
            || view.col(variable).is_some()
            || candidates.is_some()
        {
            return None;
        }
        let begin = match cursor {
            ResidualDeltaSourceCursor::Start => 0,
            ResidualDeltaSourceCursor::Offset(index) => {
                usize::try_from(index).expect("sorted-slice source cursor exceeds usize")
            }
            ResidualDeltaSourceCursor::After(_) => {
                panic!("sorted-slice source received a raw-value cursor")
            }
        };
        Some(self.proposal_page_from_offset(begin, limit, accepted))
    }

    /// Exact when the variable is bound: binary-searches the slice for
    /// every row's bound value. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| {
                self.contains_raw(&row[c])
            }),
            None => true,
        }
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for SortedSlice<'a, T>
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SortedSliceConstraint<'a, S, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SortedSliceConstraint::new(v, self)
    }
}

/// Sort-on-demand impl for any mutable slice borrow. Picks up `&mut [T]`
/// directly, and — via `DerefMut` method-resolution — `&mut Vec<T>`,
/// `&mut [T; N]`, `&mut Box<[T]>`, and anything else that derefs to a slice.
///
/// The borrowed data is sorted in place on construction; afterward the
/// returned [`SortedSliceConstraint`] aliases the same buffer for propose and
/// binary-search confirm. Callers who don't want their container reordered
/// should clone first, or use [`SortedSlice::new`] / [`SortedSlice::new_unchecked`]
/// against data they already guarantee sorted.
///
/// Does not conflict with the pre-sorted [`SortedSlice`] impl above:
/// `SortedSlice<'a, T>` is not a `&mut [T]`.
impl<'a, S: InlineEncoding, T> ContainsConstraint<'a, S> for &'a mut [T]
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SortedSliceConstraint<'a, S, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SortedSliceConstraint::new(v, SortedSlice::from_mut(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inline::encodings::UnknownInline;
    use crate::inline::Encodes;
    use crate::query::residual::ResidualLowering;

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct ReverseRaw(u8);

    impl Encodes<&ReverseRaw> for UnknownInline {
        type Output = Inline<UnknownInline>;

        fn encode(source: &ReverseRaw) -> Self::Output {
            Inline::new([u8::MAX - source.0; 32])
        }
    }

    impl TryFromInline<'_, UnknownInline> for ReverseRaw {
        type Error = std::convert::Infallible;

        fn try_from_inline(value: &Inline<UnknownInline>) -> Result<Self, Self::Error> {
            Ok(Self(u8::MAX - value.raw[0]))
        }
    }

    fn value(byte: u8) -> Inline<UnknownInline> {
        Inline::new([byte; 32])
    }

    fn project(binding: &Binding) -> Option<RawInline> {
        binding.get(0).copied()
    }

    #[test]
    fn ordinal_pages_preserve_occurrences_before_set_projection() {
        let values = [value(1), value(1), value(2), value(3)];
        let slice = SortedSlice::new(&values).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = SortedSliceConstraint::new(variable, slice);
        assert!(constraint.residual_proposal_source_is_paged(variable.index, &RowsView::EMPTY));
        let program = constraint.residual_program().unwrap();
        let route = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(variable.index),
                bound: VariableSet::new_empty(),
            })
            .unwrap();
        assert_eq!(route.stratum, ProgramStratum::Finite);
        assert_eq!(route.grouping, ProgramGrouping::PageLocal);
        assert_eq!(route.completion, ProgramCompletion::PageableOnly);
        assert!(
            constraint.progress(&SortedSliceProgramState::Propose { offset: 0 })
                > constraint.progress(&SortedSliceProgramState::Propose { offset: 1 })
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
            .expect("sorted slices expose an ordinal proposal frontier");
        assert!(roots.is_empty());
        assert_eq!(direct, [value(1).raw]);
        assert_eq!(first.examined, 1);
        assert_eq!(first.next, Some(ResidualDeltaSourceCursor::Offset(1)));

        direct.clear();
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
            .expect("the ordinal cursor resumes without raw-order assumptions");
        assert_eq!(direct, [value(1).raw, value(2).raw]);
        assert_eq!(second.examined, 2);
        assert_eq!(second.next, Some(ResidualDeltaSourceCursor::Offset(3)));

        let mut expected: Vec<_> = Query::new(
            SortedSliceConstraint::new(variable, slice),
            project,
        )
        .sequential()
        .collect();
        let mut query = Query::new(SortedSliceConstraint::new(variable, slice), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(actual, [value(1).raw, value(2).raw, value(3).raw]);
        assert_eq!(query.stats().propose_calls, 1);
        assert_eq!(query.stats().delta_source_pages, values.len());
        assert_eq!(query.stats().delta_source_candidates_examined, values.len());
        assert_eq!(query.stats().delta_source_direct_candidates, values.len());
        assert_eq!(query.stats().delta_source_roots, 0);
        assert_eq!(query.stats().max_propose_candidates, 1);
    }

    #[test]
    fn first_pull_consumes_one_entry_and_monotone_slice_growth_only_adds_rows() {
        let base = [value(1), value(1), value(3)];
        let grown = [value(1), value(1), value(2), value(3)];
        let variable = Variable::<UnknownInline>::new(0);
        let make = |values| {
            Query::new(
                SortedSliceConstraint::new(variable, SortedSlice::new(values).unwrap()),
                project,
            )
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
        };

        let mut first = make(&grown);
        assert_eq!(first.next(), Some(value(1).raw));
        assert_eq!(first.stats().delta_source_candidates_examined, 1);
        assert_eq!(first.stats().delta_source_direct_candidates, 1);
        assert_eq!(first.stats().delta_source_roots, 0);
        drop(first);

        let mut before: Vec<_> = make(&base).collect();
        let mut after: Vec<_> = make(&grown).collect();
        before.sort_unstable();
        after.sort_unstable();
        let mut remaining = after.clone();
        for value in before {
            let position = remaining
                .iter()
                .position(|candidate| *candidate == value)
                .expect("monotone growth removed a prior occurrence");
            remaining.remove(position);
        }
        assert_eq!(remaining, [value(2).raw]);
    }

    #[test]
    fn ordinal_paging_does_not_assume_native_order_matches_raw_order() {
        let values = [ReverseRaw(1), ReverseRaw(2), ReverseRaw(3)];
        let slice = SortedSlice::new(&values).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let encoded: Vec<_> = values
            .iter()
            .map(|value| <UnknownInline as Encodes<&ReverseRaw>>::encode(value).raw)
            .collect();
        assert!(encoded.windows(2).all(|pair| pair[0] > pair[1]));

        let mut expected: Vec<_> =
            Query::new(SortedSliceConstraint::new(variable, slice), project)
                .sequential()
                .collect();
        let mut query = Query::new(SortedSliceConstraint::new(variable, slice), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(query.stats().delta_source_candidates_examined, values.len());
        assert_eq!(query.stats().delta_source_direct_candidates, values.len());
    }

    #[test]
    fn direct_union_program_pages_normalize_occurrences_online() {
        let left = [value(1), value(1), value(2)];
        let right = [value(2), value(3)];
        let left = SortedSlice::new(&left).unwrap();
        let right = SortedSlice::new(&right).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let make = || {
            super::super::unionconstraint::UnionConstraint::new(vec![
                SortedSliceConstraint::new(variable, left),
                SortedSliceConstraint::new(variable, right),
            ])
        };

        let mut expected: Vec<_> = Query::new(make(), project).sequential().collect();
        let mut query = Query::new(make(), project)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        expected.sort_unstable();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(actual, [value(1).raw, value(2).raw, value(3).raw]);
        assert_eq!(query.stats().delta_source_direct_candidates, 5);
        assert_eq!(query.stats().delta_source_roots, 0);
        assert_eq!(query.stats().delta_source_pages, 5);
        // Each typed source page is normalized through the direct Union's
        // master accumulator before publication, so duplicate occurrences
        // never inflate a proposal page.
        assert_eq!(query.stats().max_propose_candidates, 1);
    }
}
