//! Exact views over finite maps from row positions to `u32` ordinals.
//!
//! A row map is often not data at all: every row may name the same ordinal,
//! or row `i` may name `base + i`. [`RowOrdinalView`] keeps those two laws
//! implicit and borrows a slice only after the map genuinely diverges from
//! them. The representation never changes semantics; every operation is the
//! corresponding operation on the dense function `i -> view.get(i)`.
//!
//! This module deliberately knows nothing about query partitions, variables,
//! or parent tags. Those consumers can own whatever state they need and expose
//! it through this one small view.

use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Repr<'a> {
    Uniform { value: u32, len: usize },
    Affine { base: u32, len: usize },
    Explicit(&'a [u32]),
}

/// A finite map from row positions `0..len` to `u32` ordinals.
///
/// `Uniform` represents `f(i) = value`; `Affine` represents
/// `f(i) = base + i`; `Explicit` borrows arbitrary values. Slicing an affine
/// view shifts its base rather than allocating, including the non-zero tail
/// produced when Rayon bisects a row range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RowOrdinalView<'a>(Repr<'a>);

impl<'a> RowOrdinalView<'a> {
    /// A map whose every row has `value`.
    pub(crate) fn uniform(value: u32, len: usize) -> Self {
        Self(Repr::Uniform { value, len })
    }

    /// The consecutive map `base, base + 1, ..., base + len - 1`.
    ///
    /// # Panics
    ///
    /// Panics when the final ordinal would exceed `u32::MAX`.
    pub(crate) fn affine(base: u32, len: usize) -> Self {
        assert!(
            affine_last(base, len).is_some(),
            "affine row ordinals overflow u32"
        );
        Self(Repr::Affine { base, len })
    }

    /// An arbitrary map borrowed from its dense representation.
    pub(crate) fn explicit(values: &'a [u32]) -> Self {
        Self(Repr::Explicit(values))
    }

    /// Number of rows in the map's domain.
    pub(crate) fn len(self) -> usize {
        match self.0 {
            Repr::Uniform { len, .. } | Repr::Affine { len, .. } => len,
            Repr::Explicit(values) => values.len(),
        }
    }

    /// The ordinal at `index`, or `None` when it is outside the domain.
    pub(crate) fn get(self, index: usize) -> Option<u32> {
        if index >= self.len() {
            return None;
        }
        Some(match self.0 {
            Repr::Uniform { value, .. } => value,
            Repr::Affine { base, .. } => {
                base + u32::try_from(index).expect("validated affine index")
            }
            Repr::Explicit(values) => values[index],
        })
    }

    /// Iterates the ordinals in row order.
    pub(crate) fn iter(self) -> impl ExactSizeIterator<Item = u32> + DoubleEndedIterator + 'a {
        (0..self.len()).map(move |index| {
            self.get(index)
                .expect("the iterator only visits in-domain rows")
        })
    }

    /// Restricts the map to `range`, re-basing the returned domain at zero.
    ///
    /// # Panics
    ///
    /// Panics when `range` is not contained in `0..self.len()`.
    pub(crate) fn slice(self, range: Range<usize>) -> Self {
        assert!(
            range.start <= range.end && range.end <= self.len(),
            "row ordinal slice is out of bounds"
        );
        let len = range.end - range.start;
        match self.0 {
            Repr::Uniform { value, .. } => Self::uniform(value, len),
            Repr::Affine { base, .. } => Self::affine(
                base + u32::try_from(range.start).expect("validated affine index"),
                len,
            ),
            Repr::Explicit(values) => Self::explicit(&values[range]),
        }
    }

    /// Splits the map at `mid`, preserving implicit forms in both halves.
    ///
    /// # Panics
    ///
    /// Panics when `mid > self.len()`.
    pub(crate) fn split_at(self, mid: usize) -> (Self, Self) {
        assert!(mid <= self.len(), "row ordinal split is out of bounds");
        (self.slice(0..mid), self.slice(mid..self.len()))
    }

    /// Composes `self` after `inner`: result row `i` is
    /// `self[inner[i]]`.
    ///
    /// Uniform and affine compositions stay implicit, while selecting an
    /// affine range from explicit storage stays borrowed. A composition that
    /// cannot be expressed by those laws is written to `scratch`, and the
    /// returned explicit view borrows it. `scratch` is always cleared first.
    ///
    /// # Panics
    ///
    /// Panics when any ordinal produced by `inner` is outside
    /// `0..self.len()`.
    pub(crate) fn compose_into<'s>(
        self,
        inner: RowOrdinalView<'_>,
        scratch: &'s mut Vec<u32>,
    ) -> RowOrdinalView<'s>
    where
        'a: 's,
    {
        scratch.clear();
        assert!(
            inner.iter().all(|index| (index as usize) < self.len()),
            "row ordinal composition index is out of bounds"
        );

        match (self.0, inner.0) {
            (_, Repr::Uniform { len: 0, .. })
            | (_, Repr::Affine { len: 0, .. })
            | (_, Repr::Explicit([])) => RowOrdinalView::affine(0, 0),
            (Repr::Uniform { value, .. }, _) => RowOrdinalView::uniform(value, inner.len()),
            (
                Repr::Affine { base, .. },
                Repr::Uniform {
                    value,
                    len: inner_len,
                },
            ) => RowOrdinalView::uniform(base + value, inner_len),
            (
                Repr::Affine { base, .. },
                Repr::Affine {
                    base: inner_base,
                    len: inner_len,
                },
            ) => RowOrdinalView::affine(base + inner_base, inner_len),
            (
                Repr::Explicit(values),
                Repr::Uniform {
                    value,
                    len: inner_len,
                },
            ) => RowOrdinalView::uniform(values[value as usize], inner_len),
            (
                Repr::Explicit(values),
                Repr::Affine {
                    base,
                    len: inner_len,
                },
            ) => {
                let start = base as usize;
                RowOrdinalView::explicit(&values[start..start + inner_len])
            }
            _ => {
                scratch.extend(inner.iter().map(|index| {
                    self.get(index as usize)
                        .expect("composition indexes were validated")
                }));
                RowOrdinalView::explicit(scratch)
            }
        }
    }
}

/// Last value of an affine map, or `None` when its domain cannot fit in
/// `u32`. The empty map has no last value and is valid at every base.
fn affine_last(base: u32, len: usize) -> Option<u32> {
    if len == 0 {
        return Some(base);
    }
    let offset = u32::try_from(len - 1).ok()?;
    base.checked_add(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn dense(view: RowOrdinalView<'_>) -> Vec<u32> {
        view.iter().collect()
    }

    #[test]
    fn offset_affine_tail_stays_affine() {
        let view = RowOrdinalView::affine(41, 12);
        let (head, tail) = view.split_at(5);

        assert_eq!(head, RowOrdinalView(Repr::Affine { base: 41, len: 5 }));
        assert_eq!(tail, RowOrdinalView(Repr::Affine { base: 46, len: 7 }));
        assert_eq!(dense(head), (41..46).collect::<Vec<_>>());
        assert_eq!(dense(tail), (46..53).collect::<Vec<_>>());
    }

    #[test]
    fn generated_composition_stays_generated() {
        let outer = RowOrdinalView::affine(100, 20);
        let mut scratch = vec![999];

        let uniform = outer.compose_into(RowOrdinalView::uniform(7, 4), &mut scratch);
        assert_eq!(uniform, RowOrdinalView::uniform(107, 4));
        assert!(scratch.is_empty());

        let affine = outer.compose_into(RowOrdinalView::affine(3, 8), &mut scratch);
        assert_eq!(affine, RowOrdinalView::affine(103, 8));
        assert!(scratch.is_empty());
    }

    #[test]
    fn explicit_composition_uses_caller_scratch() {
        let outer_values = [9, 7, 5, 3];
        let inner_values = [3, 1, 3, 0];
        let outer = RowOrdinalView::explicit(&outer_values);
        let inner = RowOrdinalView::explicit(&inner_values);
        let mut scratch = Vec::new();

        let composed = outer.compose_into(inner, &mut scratch);
        assert_eq!(dense(composed), vec![3, 7, 3, 9]);
    }

    #[test]
    fn explicit_after_offset_affine_borrows_contiguous_slice() {
        let outer_values = [11, 13, 17, 19, 23, 29, 31];
        let outer = RowOrdinalView::explicit(&outer_values);
        let mut scratch = vec![999];

        let composed = outer.compose_into(RowOrdinalView::affine(2, 4), &mut scratch);

        assert_eq!(composed, RowOrdinalView::explicit(&outer_values[2..6]));
        assert_eq!(dense(composed), vec![17, 19, 23, 29]);
        assert!(scratch.is_empty());
    }

    proptest! {
        #[test]
        fn every_form_matches_its_dense_function(
            value in any::<u32>(),
            base in 0u32..=u32::MAX - 256,
            len in 0usize..=256,
            explicit in prop::collection::vec(any::<u32>(), 0..=256),
        ) {
            let uniform = RowOrdinalView::uniform(value, len);
            prop_assert_eq!(dense(uniform), vec![value; len]);

            let affine = RowOrdinalView::affine(base, len);
            let affine_dense: Vec<u32> = (0..len).map(|i| base + i as u32).collect();
            prop_assert_eq!(dense(affine), affine_dense);

            let explicit_view = RowOrdinalView::explicit(&explicit);
            prop_assert_eq!(dense(explicit_view), explicit);
        }

        #[test]
        fn slice_and_split_match_dense_reference(
            values in prop::collection::vec(any::<u32>(), 0..=256),
            a in any::<usize>(),
            b in any::<usize>(),
        ) {
            let view = RowOrdinalView::explicit(&values);
            let lo = a.min(values.len());
            let hi = b.min(values.len());
            let (start, end) = if lo <= hi { (lo, hi) } else { (hi, lo) };

            prop_assert_eq!(dense(view.slice(start..end)), values[start..end].to_vec());

            let (left, right) = view.split_at(start);
            prop_assert_eq!(dense(left), values[..start].to_vec());
            prop_assert_eq!(dense(right), values[start..].to_vec());
        }

        #[test]
        fn offset_affine_split_matches_dense_reference(
            base in 0u32..=u32::MAX - 1024,
            len in 0usize..=1024,
            raw_mid in any::<usize>(),
        ) {
            let mid = raw_mid.min(len);
            let view = RowOrdinalView::affine(base, len);
            let expected: Vec<u32> = (0..len).map(|i| base + i as u32).collect();
            let (left, right) = view.split_at(mid);

            prop_assert_eq!(dense(left), expected[..mid].to_vec());
            prop_assert_eq!(dense(right), expected[mid..].to_vec());
            prop_assert_eq!(
                right,
                RowOrdinalView(Repr::Affine {
                    base: base + mid as u32,
                    len: len - mid,
                }),
            );
        }

        #[test]
        fn composition_matches_dense_reference(
            outer in prop::collection::vec(any::<u32>(), 1..=128),
            raw_inner in prop::collection::vec(any::<u32>(), 0..=128),
        ) {
            let inner: Vec<u32> = raw_inner
                .into_iter()
                .map(|index| index % outer.len() as u32)
                .collect();
            let expected: Vec<u32> = inner
                .iter()
                .map(|&index| outer[index as usize])
                .collect();
            let mut scratch = Vec::new();
            let composed = RowOrdinalView::explicit(&outer).compose_into(
                RowOrdinalView::explicit(&inner),
                &mut scratch,
            );

            prop_assert_eq!(dense(composed), expected);
        }

        #[test]
        fn composition_is_associative(
            outer in prop::collection::vec(any::<u32>(), 1..=64),
            raw_middle in prop::collection::vec(any::<u32>(), 1..=64),
            raw_inner in prop::collection::vec(any::<u32>(), 0..=64),
        ) {
            let middle: Vec<u32> = raw_middle
                .into_iter()
                .map(|index| index % outer.len() as u32)
                .collect();
            let inner: Vec<u32> = raw_inner
                .into_iter()
                .map(|index| index % middle.len() as u32)
                .collect();

            let outer_view = RowOrdinalView::explicit(&outer);
            let middle_view = RowOrdinalView::explicit(&middle);
            let inner_view = RowOrdinalView::explicit(&inner);

            let mut outer_middle_scratch = Vec::new();
            let outer_middle =
                outer_view.compose_into(middle_view, &mut outer_middle_scratch);
            let mut left_scratch = Vec::new();
            let left = outer_middle.compose_into(inner_view, &mut left_scratch);

            let mut middle_inner_scratch = Vec::new();
            let middle_inner =
                middle_view.compose_into(inner_view, &mut middle_inner_scratch);
            let mut right_scratch = Vec::new();
            let right = outer_view.compose_into(middle_inner, &mut right_scratch);

            prop_assert_eq!(dense(left), dense(right));
        }
    }
}
