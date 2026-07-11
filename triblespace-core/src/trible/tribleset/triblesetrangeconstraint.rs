use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;
use crate::inline::INLINE_LEN;
use crate::query::CandidateSink;
use crate::query::Constraint;
use crate::query::EstimateSink;
use crate::query::RowsView;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
/// A value-range-aware constraint that uses the TribleSet's AVE index
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
}

impl<'a> Constraint<'a> for TribleSetRangeConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_v)
    }

    fn estimate(&self, variable: VariableId, view: &RowsView<'_>, out: &mut EstimateSink<'_>) -> bool {
        if variable != self.variable_v {
            return false;
        }
        out.fill(self.cached_estimate, view.len());
        true
    }

    fn propose(&self, variable: VariableId, view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
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

    fn confirm(&self, variable: VariableId, _view: &RowsView<'_>, candidates: &mut CandidateSink<'_>) {
        if variable == self.variable_v {
            candidates.retain(|_, v| *v >= self.min && *v <= self.max);
        }
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
    use crate::prelude::inlineencodings::R256BE;
    use crate::prelude::*;

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
}
