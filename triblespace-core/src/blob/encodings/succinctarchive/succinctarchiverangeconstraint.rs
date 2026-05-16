use super::*;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::value::RawInline;
use crate::value::Inline;
use crate::value::InlineEncoding;

/// Inline-range constraint for [`SuccinctArchive`].
///
/// Mirrors [`TribleSet::value_in_range`](crate::trible::TribleSet::value_in_range).
/// The implementation leans on two archive primitives:
///
/// - [`Universe::search_range`] turns the inclusive value range
///   `[min, max]` into a half-open code range `[lo, hi)` in O(log n).
///   This works because [`Universe`] guarantees value-ordered codes.
/// - [`SuccinctArchive::enumerate_domain_in_range`] walks only the codes
///   that actually appear on the indexed axis (here: V) within `[lo, hi)`,
///   skipping empty groups via the same `select1` stride that powers the
///   unbounded [`enumerate_domain`].
///
/// Total propose cost is O(log n + K) where K is the count of distinct
/// V-position codes whose values fall in the range. The `cached_estimate`
/// is the upper bound `hi - lo` (codes in range, before V-presence
/// filtering) — computed once at construction in O(log n) and returned
/// in O(1) at planning time.
///
/// # Example
///
/// ```rust,ignore
/// find!(ts: Inline<NsTAIInterval>,
///     and!(
///         pattern!(&archive, [{ ?id @ attr: ?ts }]),
///         archive.value_in_range(ts, min_ts, max_ts),
///     )
/// )
/// ```
pub struct SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    variable_v: VariableId,
    min: RawInline,
    max: RawInline,
    archive: &'a SuccinctArchive<U>,
    /// Cached upper-bound estimate: width of the universe code range
    /// covering `[min, max]`. Computed once at construction so
    /// `estimate` is O(1) at planning time.
    cached_estimate: usize,
}

impl<'a, U> SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    pub fn new<V: InlineEncoding>(
        variable_v: Variable<V>,
        min: Inline<V>,
        max: Inline<V>,
        archive: &'a SuccinctArchive<U>,
    ) -> Self {
        // O(log n) range lookup once at construction; query-time estimate
        // is just the cached width.
        let code_range = archive.domain.search_range(&min.raw, &max.raw);
        let cached_estimate = code_range.end.saturating_sub(code_range.start);
        SuccinctArchiveRangeConstraint {
            variable_v: variable_v.index,
            min: min.raw,
            max: max.raw,
            archive,
            cached_estimate,
        }
    }
}

impl<'a, U> Constraint<'a> for SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable_v)
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if variable != self.variable_v {
            return None;
        }
        Some(self.cached_estimate)
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable != self.variable_v {
            return;
        }
        let code_range = self.archive.domain.search_range(&self.min, &self.max);
        proposals.extend(
            self.archive
                .enumerate_domain_in_range(&self.archive.v_a, code_range),
        );
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawInline>) {
        if variable == self.variable_v {
            proposals.retain(|v| *v >= self.min && *v <= self.max);
        }
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        match binding.get(self.variable_v) {
            Some(v) => *v >= self.min && *v <= self.max,
            None => true,
        }
    }
}
