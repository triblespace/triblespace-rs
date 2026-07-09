use std::mem;

use super::*;
use itertools::Itertools;

/// Logical disjunction of constraints (OR).
///
/// A value is accepted if *any* variant accepts it. Built by the
/// [`or!`](crate::or) macro, by [`pattern_changes!`](crate::macros::pattern_changes),
/// or directly via [`new`](Self::new).
///
/// All variants must declare the same [`VariableSet`]; this is asserted at
/// construction time. Estimates are summed across variants, proposals are
/// merged and deduplicated, and confirmations are unioned via
/// [`kmerge`](itertools::Itertools::kmerge).
///
/// Before proposing or confirming, the union checks each variant's
/// [`satisfied`](Constraint::satisfied) status and skips variants that are
/// provably dead. This prevents a value confirmed by a dead variant from
/// leaking into the result set — the fix for spurious results in
/// multi-entity [`pattern_changes!`](crate::macros::pattern_changes) joins.
pub struct UnionConstraint<C> {
    constraints: Vec<C>,
}

impl<'a, C> UnionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Creates a union over the given constraints.
    ///
    /// # Panics
    ///
    /// Panics if `constraints` is empty (a zero-arm union has no
    /// well-defined variable set), or if the variants do not all
    /// declare the same variable set.
    pub fn new(constraints: Vec<C>) -> Self {
        assert!(
            !constraints.is_empty(),
            "UnionConstraint requires at least one variant; \
             use a different constraint type for the empty case"
        );
        assert!(constraints
            .iter()
            .map(|c| c.variables())
            .tuple_windows()
            .all(|(a, b)| a == b));
        UnionConstraint { constraints }
    }
}

impl<'a, C> Constraint<'a> for UnionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Returns the variable set of the first variant (all variants share
    /// the same set, enforced at construction).
    fn variables(&self) -> VariableSet {
        self.constraints[0].variables()
    }

    /// Appends the elementwise **sum** of estimates across all variants.
    /// A union can produce candidates from any branch, so the
    /// cardinalities add.
    fn estimate(&self, variable: VariableId, view: RowsView<'_>, out: &mut Vec<usize>) -> bool {
        let base = out.len();
        let mut any = false;
        let mut scratch: Vec<usize> = Vec::new();
        for c in &self.constraints {
            if !any {
                any = c.estimate(variable, view, out);
            } else {
                scratch.clear();
                if c.estimate(variable, view, &mut scratch) {
                    for (o, &s) in out[base..].iter_mut().zip(scratch.iter()) {
                        *o = o.saturating_add(s);
                    }
                }
            }
        }
        any
    }

    /// Per row: collects proposals from every *satisfied* variant (via a
    /// single-row borrowed view), then sorts and deduplicates the row's
    /// group. Dead variants (where [`satisfied`](Constraint::satisfied)
    /// returns `false` for the row) are skipped so their stale bindings
    /// cannot inject values that no live variant would produce.
    fn propose(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        let mut scratch: Candidates = Vec::new();
        for (i, row) in view.iter().enumerate() {
            let row_view = RowsView::new(view.vars, row);
            scratch.clear();
            self.constraints
                .iter()
                .filter(|c| c.satisfied(row_view))
                .for_each(|c| c.propose(variable, row_view, &mut scratch));
            scratch.sort_unstable();
            scratch.dedup();
            candidates.extend(scratch.iter().map(|&(_, v)| (i as u32, v)));
        }
    }

    /// Confirms each row's candidate group against every *satisfied*
    /// variant independently, then merges the per-variant survivors via
    /// [`kmerge`](itertools::Itertools::kmerge) and deduplicates. A value
    /// passes if *any* live variant confirms it.
    fn confirm(&self, variable: VariableId, view: RowsView<'_>, candidates: &mut Candidates) {
        confirm_per_row(view, candidates, |row, values| {
            let row_view = RowsView::new(view.vars, row);
            values.sort_unstable();

            let union: Vec<RawInline> = self
                .constraints
                .iter()
                .filter(|c| c.satisfied(row_view))
                .map(|c| {
                    let mut vs: Candidates = values.iter().map(|&v| (0, v)).collect();
                    c.confirm(variable, row_view, &mut vs);
                    vs
                })
                .kmerge()
                .dedup()
                .map(|(_, v)| v)
                .collect();

            _ = mem::replace(values, union);
        });
    }

    /// Returns `true` when **at least one** variant is satisfied for
    /// every row.
    fn satisfied(&self, view: RowsView<'_>) -> bool {
        view.iter().all(|row| {
            let row_view = RowsView::new(view.vars, row);
            self.constraints.iter().any(|c| c.satisfied(row_view))
        })
    }

    /// Returns the union of all variants' influence sets for `variable`.
    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |acc, c| {
                acc.union(c.influence(variable))
            })
    }
}

/// Combines constraints into a [`UnionConstraint`] (logical OR).
///
/// A result is produced when *any* of the given constraints is satisfied.
/// All constraints must declare the same variable set.
///
/// ```rust,ignore
/// or!(pattern!(&set_a, [...]), pattern!(&set_b, [...]))
/// ```
#[macro_export]
macro_rules! or {
    ($($c:expr),+ $(,)?) => (
        ::std::sync::Arc::new(
            $crate::query::unionconstraint::UnionConstraint::new(vec![
                $(Box::new($c)
                    as Box<dyn $crate::query::Constraint + Send + Sync>),+
            ])
        )
    )
}

/// Re-export of the [`or!`] macro.
pub use or;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::constantconstraint::ConstantConstraint;

    #[test]
    #[should_panic(expected = "UnionConstraint requires at least one variant")]
    fn empty_union_panics_at_construction() {
        // Without this assert, `variables()` would later panic on
        // `self.constraints[0]` with an unhelpful index-out-of-bounds.
        let _: UnionConstraint<ConstantConstraint> = UnionConstraint::new(vec![]);
    }
}
