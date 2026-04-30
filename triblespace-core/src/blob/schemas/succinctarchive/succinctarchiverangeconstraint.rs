use super::*;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::value::RawValue;
use crate::value::Value;
use crate::value::ValueSchema;
use jerky::bit_vector::Select;

/// Value-range constraint for [`SuccinctArchive`].
///
/// Mirrors [`TribleSetRangeConstraint`](crate::trible::tribleset::triblesetrangeconstraint::TribleSetRangeConstraint)
/// but operates on the archive's universe + V-axis prefix vector. The
/// universe's [`Universe::search_range`] returns the half-open code
/// range whose values fall in `[min, max]`; for each such code we
/// check the V-axis prefix vector to verify the code actually appears
/// in V position before emitting it as a proposal.
///
/// Selectivity: with an `OrderedUniverse` the universe lookup is
/// O(log n) (two binary searches); the per-code V-position check is
/// O(1) via the bit vector's `select1`. Total propose cost is
/// O(log n + k) where k = codes in the range that appear in V.
///
/// Use via [`SuccinctArchive::value_in_range`]:
///
/// ```rust,ignore
/// find!(ts: Value<NsTAIInterval>,
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
    min: RawValue,
    max: RawValue,
    archive: &'a SuccinctArchive<U>,
    /// Cached estimate: count of distinct universe codes in
    /// `[lo, hi)` that have at least one occurrence in the V column.
    /// Computed at construction so `estimate` is O(1) at query time.
    cached_estimate: usize,
}

impl<'a, U> SuccinctArchiveRangeConstraint<'a, U>
where
    U: Universe,
{
    pub fn new<V: ValueSchema>(
        variable_v: Variable<V>,
        min: Value<V>,
        max: Value<V>,
        archive: &'a SuccinctArchive<U>,
    ) -> Self {
        let cached_estimate = code_range_v_count(archive, &min.raw, &max.raw);
        SuccinctArchiveRangeConstraint {
            variable_v: variable_v.index,
            min: min.raw,
            max: max.raw,
            archive,
            cached_estimate,
        }
    }
}

/// Counts universe codes in `[min, max]` that have at least one
/// trible in V position. Used for both the cached estimate and
/// (logically) the propose enumeration.
fn code_range_v_count<U>(archive: &SuccinctArchive<U>, min: &RawValue, max: &RawValue) -> usize
where
    U: Universe,
{
    let code_range = archive.domain.search_range(min, max);
    let mut count = 0;
    for code in code_range {
        if v_position_count(archive, code) > 0 {
            count += 1;
        }
    }
    count
}

/// Number of tribles whose V-position universe code equals `code`.
/// Returns 0 if the code never appears as a value.
fn v_position_count<U>(archive: &SuccinctArchive<U>, code: usize) -> usize
where
    U: Universe,
{
    // The v_a bit vector is unary-encoded group sizes for the V column:
    // select1(code) marks the boundary between code-1 and code, so
    // (select1(code+1) - (code+1)) - (select1(code) - code) is the
    // count of tribles whose V-code equals `code`.
    let lo = match archive.v_a.select1(code) {
        Some(p) => p - code,
        None => return 0,
    };
    let hi = match archive.v_a.select1(code + 1) {
        Some(p) => p - (code + 1),
        None => return 0,
    };
    hi.saturating_sub(lo)
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

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if variable != self.variable_v {
            return;
        }
        let code_range = self.archive.domain.search_range(&self.min, &self.max);
        for code in code_range {
            if v_position_count(self.archive, code) > 0 {
                proposals.push(self.archive.domain.access(code));
            }
        }
    }

    fn confirm(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
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
