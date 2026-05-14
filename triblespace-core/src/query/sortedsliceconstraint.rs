use crate::value::IntoInline;
use crate::value::TryFromInline;

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
pub struct SortedSliceConstraint<'a, S: InlineSchema, T> {
    variable: Variable<S>,
    slice: SortedSlice<'a, T>,
}

impl<'a, S: InlineSchema, T> SortedSliceConstraint<'a, S, T> {
    /// Creates a constraint that restricts `variable` to values in `slice`.
    pub fn new(variable: Variable<S>, slice: SortedSlice<'a, T>) -> Self {
        SortedSliceConstraint { variable, slice }
    }
}

impl<'a, S: InlineSchema, T> Constraint<'a> for SortedSliceConstraint<'a, S, T>
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable.index)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if self.variable.index == variable {
            Some(self.slice.0.len())
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.extend(self.slice.0.iter().map(|v| IntoInline::to_inline(v).raw));
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if self.variable.index == variable {
            proposals.retain(|v| {
                match TryFromInline::try_from_inline(Inline::<S>::as_transmute_raw(v)) {
                    Ok(t) => self.slice.0.binary_search(&t).is_ok(),
                    Err(_) => false,
                }
            });
        }
    }
}

impl<'a, S: InlineSchema, T> ContainsConstraint<'a, S> for SortedSlice<'a, T>
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
impl<'a, S: InlineSchema, T> ContainsConstraint<'a, S> for &'a mut [T]
where
    T: 'a + Ord + for<'b> TryFromInline<'b, S>,
    for<'b> &'b T: IntoInline<S>,
{
    type Constraint = SortedSliceConstraint<'a, S, T>;

    fn has(self, v: Variable<S>) -> Self::Constraint {
        SortedSliceConstraint::new(v, SortedSlice::from_mut(self))
    }
}
