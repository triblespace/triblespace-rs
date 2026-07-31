//! Exact borrowed selections of rows from an index block.
//!
//! The query engine needs only two forms: a consecutive range of row
//! ordinals, and an arbitrary explicit selection owned elsewhere. Identity
//! selections and their non-zero page slices stay allocation-free; a
//! nonconsecutive selection borrows its dense ordinals.

use std::ops::Range;

/// A consecutive range of row ordinals or an arbitrary borrowed selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowSelection<'a> {
    Consecutive { first: u32, len: usize },
    Explicit(&'a [u32]),
}

impl<'a> RowSelection<'a> {
    /// The identity selection `0, 1, ..., len - 1`.
    pub(super) fn identity(len: usize) -> Self {
        Self::consecutive(0, len)
    }

    /// An arbitrary selection borrowed from its dense representation.
    pub(super) fn explicit(values: &'a [u32]) -> Self {
        Self::Explicit(values)
    }

    /// Number of selected rows.
    pub(super) fn len(self) -> usize {
        match self {
            Self::Consecutive { len, .. } => len,
            Self::Explicit(values) => values.len(),
        }
    }

    /// The underlying row ordinal at `index`, or `None` outside the selection.
    pub(super) fn get(self, index: usize) -> Option<u32> {
        match self {
            Self::Consecutive { first, len } if index < len => {
                Some(first + u32::try_from(index).expect("validated consecutive row index"))
            }
            Self::Consecutive { .. } => None,
            Self::Explicit(values) => values.get(index).copied(),
        }
    }

    /// Restricts the selection to `range`, re-basing its domain at zero.
    ///
    /// A consecutive page remains consecutive with its first ordinal shifted;
    /// an explicit page remains a borrowed subslice.
    ///
    /// # Panics
    ///
    /// Panics when `range` is not contained in `0..self.len()`.
    pub(super) fn slice(self, range: Range<usize>) -> Self {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "row selection slice is out of bounds"
        );
        let len = range.end - range.start;
        match self {
            // An empty selection has no first ordinal. Canonicalize it before
            // converting or adding `range.start`: the valid tail of
            // `0..=u32::MAX` starts one past the largest representable row.
            Self::Consecutive { .. } if len == 0 => Self::consecutive(0, 0),
            Self::Consecutive { first, .. } => {
                let offset = u32::try_from(range.start).expect("validated consecutive row index");
                Self::consecutive(
                    first
                        .checked_add(offset)
                        .expect("consecutive row ordinals overflow u32"),
                    len,
                )
            }
            Self::Explicit(values) => Self::explicit(&values[range]),
        }
    }

    /// Constructs a checked consecutive selection.
    fn consecutive(first: u32, len: usize) -> Self {
        assert!(
            consecutive_last(first, len).is_some(),
            "consecutive row ordinals overflow u32"
        );
        Self::Consecutive { first, len }
    }
}

/// Last selected ordinal, or `None` when the range cannot fit in `u32`.
/// The empty selection has no last ordinal and is valid at every `first`.
fn consecutive_last(first: u32, len: usize) -> Option<u32> {
    if len == 0 {
        return Some(first);
    }
    let offset = u32::try_from(len - 1).ok()?;
    first.checked_add(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn dense(selection: RowSelection<'_>) -> Vec<u32> {
        (0..selection.len())
            .map(|index| selection.get(index).expect("in-domain row"))
            .collect()
    }

    #[test]
    fn nonzero_consecutive_and_explicit_slices_stay_exact() {
        let consecutive = RowSelection::consecutive(41, 12).slice(5..12);
        assert_eq!(consecutive, RowSelection::Consecutive { first: 46, len: 7 });
        assert_eq!(dense(consecutive), (46..53).collect::<Vec<_>>());

        let values = [11, 13, 17, 19, 23, 29, 31];
        let explicit = RowSelection::explicit(&values).slice(2..6);
        assert_eq!(explicit, RowSelection::explicit(&values[2..6]));
        assert_eq!(dense(explicit), vec![17, 19, 23, 29]);
    }

    #[test]
    fn empty_consecutive_tails_are_exact_at_the_u32_boundary() {
        let singleton = RowSelection::consecutive(u32::MAX, 1);
        assert_eq!(singleton.slice(1..1), RowSelection::identity(0));

        // This domain contains every `u32` row ordinal. It exists only when
        // `usize` is wider than `u32`; the conversion cleanly skips 32-bit
        // targets rather than overflowing the test itself.
        if let Ok(len) = usize::try_from(u64::from(u32::MAX) + 1) {
            let all_rows = RowSelection::identity(len);
            assert_eq!(all_rows.slice(len..len), RowSelection::identity(0));
        }
    }

    proptest! {
        #[test]
        fn both_forms_match_their_dense_functions(
            first in 0u32..=u32::MAX - 256,
            len in 0usize..=256,
            explicit in prop::collection::vec(any::<u32>(), 0..=256),
        ) {
            let consecutive = RowSelection::consecutive(first, len);
            let expected: Vec<u32> = (0..len).map(|i| first + i as u32).collect();
            prop_assert_eq!(dense(consecutive), expected);

            let explicit_selection = RowSelection::explicit(&explicit);
            prop_assert_eq!(dense(explicit_selection), explicit);
        }

        #[test]
        fn slicing_both_forms_matches_dense_reference(
            first in 0u32..=u32::MAX - 256,
            values in prop::collection::vec(any::<u32>(), 0..=256),
            a in any::<usize>(),
            b in any::<usize>(),
        ) {
            let len = values.len();
            let lo = a.min(len);
            let hi = b.min(len);
            let (start, end) = if lo <= hi { (lo, hi) } else { (hi, lo) };

            let consecutive = RowSelection::consecutive(first, len);
            let expected: Vec<u32> = (0..len).map(|i| first + i as u32).collect();
            prop_assert_eq!(
                dense(consecutive.slice(start..end)),
                expected[start..end].to_vec(),
            );

            let explicit = RowSelection::explicit(&values);
            prop_assert_eq!(
                dense(explicit.slice(start..end)),
                values[start..end].to_vec(),
            );
        }
    }
}
