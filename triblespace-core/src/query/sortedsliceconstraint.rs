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

impl<'a, S: InlineEncoding, T> SortedSliceConstraint<'a, S, T> {
    /// Creates a constraint that restricts `variable` to values in `slice`.
    pub fn new(variable: Variable<S>, slice: SortedSlice<'a, T>) -> Self {
        SortedSliceConstraint { variable, slice }
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
            candidates.retain(|_, v| {
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(v)) {
                    Ok(t) => self.slice.0.binary_search(&t).is_ok(),
                    Err(_) => false,
                }
            });
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
        assert!(limit > 0, "residual source pages require positive demand");
        let begin = match cursor {
            ResidualDeltaSourceCursor::Start => 0,
            ResidualDeltaSourceCursor::Offset(index) => {
                usize::try_from(index).expect("sorted-slice source cursor exceeds usize")
            }
            ResidualDeltaSourceCursor::After(_) => {
                panic!("sorted-slice source received a raw-value cursor")
            }
        };
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
        Some(ResidualDeltaSourcePage {
            next: (end < self.slice.0.len()).then(|| {
                ResidualDeltaSourceCursor::Offset(
                    u64::try_from(end).expect("sorted-slice source offset exceeds u64"),
                )
            }),
            examined: end - begin,
        })
    }

    /// Exact when the variable is bound: binary-searches the slice for
    /// every row's bound value. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| {
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(&row[c])) {
                    Ok(t) => self.slice.0.binary_search(&t).is_ok(),
                    Err(_) => false,
                }
            }),
            None => true,
        }
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
    fn direct_pages_use_ordinal_cursors_and_preserve_duplicate_occurrences() {
        let values = [value(1), value(1), value(2), value(3)];
        let slice = SortedSlice::new(&values).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = SortedSliceConstraint::new(variable, slice);
        assert!(constraint.residual_proposal_source_is_paged(variable.index, &RowsView::EMPTY));

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

        let mut expected: Vec<_> = Query::new(SortedSliceConstraint::new(variable, slice), project)
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
        assert_eq!(
            actual,
            [value(1).raw, value(1).raw, value(2).raw, value(3).raw]
        );
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

        let mut expected: Vec<_> = Query::new(SortedSliceConstraint::new(variable, slice), project)
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
    fn quiescent_union_keeps_direct_occurrences_private_until_normalization() {
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
        // Each source page stays private to its Union arm; normalization emits
        // the three distinct values only after both arms reach quiescence.
        assert_eq!(query.stats().max_propose_candidates, 3);
    }
}
