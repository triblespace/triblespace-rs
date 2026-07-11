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

impl InlineRange {
    /// Create a range constraint on `variable` with inclusive bounds.
    pub fn new<T: InlineEncoding>(variable: Variable<T>, min: Inline<T>, max: Inline<T>) -> Self {
        InlineRange {
            variable: variable.index,
            min: min.raw,
            max: max.raw,
        }
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
            candidates.retain(|_, v| *v >= self.min && *v <= self.max);
        }
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
}

#[cfg(test)]
mod tests {
    use crate::prelude::inlineencodings::R256;
    use crate::prelude::*;

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
}
