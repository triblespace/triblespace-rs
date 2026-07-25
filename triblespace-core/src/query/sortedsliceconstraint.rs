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
}

impl<'a, S: InlineEncoding, T> Constraint<'a> for SortedSliceConstraint<'a, S, T>
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
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

    /// Exact when the variable is bound: binary-searches the slice for
    /// every row's bound value. Returns `true` optimistically while the
    /// variable is unbound.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        match view.col(self.variable.index) {
            Some(c) => view.iter().all(|row| self.contains_raw(&row[c])),
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
    fn ordinary_proposal_preserves_occurrences_before_set_projection() {
        let values = [value(1), value(1), value(2), value(3)];
        let slice = SortedSlice::new(&values).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let constraint = SortedSliceConstraint::new(variable, slice);
        let mut occurrences = Vec::new();
        constraint.propose(
            variable.index,
            &RowsView::EMPTY,
            &mut CandidateSink::Values(&mut occurrences),
        );
        assert_eq!(
            occurrences,
            [value(1).raw, value(1).raw, value(2).raw, value(3).raw]
        );

        let mut actual: Vec<_> = Query::new(constraint, project)
            .solve_residual_state_lazy()
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, [value(1).raw, value(2).raw, value(3).raw]);
    }

    #[test]
    fn monotone_slice_growth_only_adds_rows() {
        let base = [value(1), value(1), value(3)];
        let grown = [value(1), value(1), value(2), value(3)];
        let variable = Variable::<UnknownInline>::new(0);
        let make = |values| {
            Query::new(
                SortedSliceConstraint::new(variable, SortedSlice::new(values).unwrap()),
                project,
            )
            .solve_residual_state_lazy()
        };

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
    fn ordinary_slice_does_not_assume_native_order_matches_raw_order() {
        let values = [ReverseRaw(1), ReverseRaw(2), ReverseRaw(3)];
        let slice = SortedSlice::new(&values).unwrap();
        let variable = Variable::<UnknownInline>::new(0);
        let encoded: Vec<_> = values
            .iter()
            .map(|value| <UnknownInline as Encodes<&ReverseRaw>>::encode(value).raw)
            .collect();
        assert!(encoded.windows(2).all(|pair| pair[0] > pair[1]));

        let mut actual: Vec<_> = Query::new(SortedSliceConstraint::new(variable, slice), project)
            .solve_residual_state_lazy()
            .collect();
        actual.sort_unstable();
        let mut expected = encoded;
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn union_of_sorted_slices_preserves_set_semantics() {
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

        let mut actual: Vec<_> = Query::new(make(), project)
            .solve_residual_state_lazy()
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, [value(1).raw, value(2).raw, value(3).raw]);
    }
}
