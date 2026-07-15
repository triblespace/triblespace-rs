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
/// construction time. Branch-local variables are unsupported because the
/// engine's result schema is flat — every row binds the same variable set
/// exactly once, so a variable that exists only in some alternatives has
/// no representation. (This is a result-model restriction, not a semantic
/// one: the union itself is monotonic.) Since `pattern!` folds attribute
/// constants and literal values into constant [`Term`](crate::query::Term)s
/// (they never become variables), the requirement is about the *query
/// variables the caller wrote*: every arm must mention the same ones.
/// Estimates are summed across variants, proposals are merged and
/// deduplicated, and confirmations are unioned via
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
        if let Some((i, (a, b))) = constraints
            .iter()
            .map(|c| c.variables())
            .tuple_windows()
            .enumerate()
            .find(|(_, (a, b))| a != b)
        {
            panic!(
                "all union (or!) variants must mention the same query \
                 variables: variant {} declares {:?} but variant {} \
                 declares {:?}",
                i,
                a,
                i + 1,
                b
            );
        }
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
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        match out {
            EstimateSink::Scalar(slot) => {
                let mut any = false;
                let mut acc = 0usize;
                for c in &self.constraints {
                    let mut e = 0usize;
                    if c.estimate(variable, view, &mut EstimateSink::Scalar(&mut e)) {
                        any = true;
                        acc = acc.saturating_add(e);
                    }
                }
                if any {
                    **slot = acc;
                }
                any
            }
            EstimateSink::Column(out) => {
                let base = out.len();
                let mut any = false;
                let mut scratch: Vec<usize> = Vec::new();
                for c in &self.constraints {
                    if !any {
                        any = c.estimate(variable, view, &mut EstimateSink::Column(out));
                    } else {
                        scratch.clear();
                        if c.estimate(variable, view, &mut EstimateSink::Column(&mut scratch)) {
                            for (o, &s) in out[base..].iter_mut().zip(scratch.iter()) {
                                *o = o.saturating_add(s);
                            }
                        }
                    }
                }
                any
            }
        }
    }

    /// Per row: collects proposals from every *satisfied* variant (via a
    /// single-row borrowed view), then sorts and deduplicates the row's
    /// group. Dead variants (where [`satisfied`](Constraint::satisfied)
    /// returns `false` for the row) are skipped so their stale bindings
    /// cannot inject values that no live variant would produce.
    ///
    /// Each variant proposes into its **own empty buffer** and the union
    /// merges the independent per-variant outputs. This upholds the
    /// empty-sink law of [`propose`](Constraint::propose): a composite
    /// variant (e.g. an intersection) filters the sink it is handed via its
    /// children's `confirm`, so sharing one buffer across variants would let
    /// a later variant delete candidates an earlier variant produced —
    /// making the result depend on variant order and, worse, letting a
    /// monotonic growth of the underlying data *remove* results (a CALM
    /// violation observed in [`pattern_changes!`](crate::macros::pattern_changes)
    /// joins).
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        debug_assert!(
            candidates.is_empty(),
            "propose expects an empty sink (see the Constraint::propose protocol law)"
        );
        let mut row_values: Vec<RawInline> = Vec::new();
        let mut variant_values: Vec<RawInline> = Vec::new();
        for (i, row) in view.iter().enumerate() {
            let row_view = RowsView::new(view.vars, row);
            row_values.clear();
            self.constraints
                .iter()
                .filter(|c| c.satisfied(&row_view))
                .for_each(|c| {
                    c.propose(
                        variable,
                        &row_view,
                        &mut CandidateSink::Values(&mut variant_values),
                    );
                    // `append` drains the buffer, leaving it empty for the
                    // next variant.
                    row_values.append(&mut variant_values);
                });
            row_values.sort_unstable();
            row_values.dedup();
            candidates.extend_row(i as u32, row_values.iter().copied());
        }
    }

    /// Confirms each row's candidate group against every *satisfied*
    /// variant independently, then merges the per-variant survivors via
    /// [`kmerge`](itertools::Itertools::kmerge) and deduplicates. A value
    /// passes if *any* live variant confirms it.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        confirm_per_row(view, candidates, |row, values| {
            let row_view = RowsView::new(view.vars, row);
            values.sort_unstable();

            let union: Vec<RawInline> = self
                .constraints
                .iter()
                .filter(|c| c.satisfied(&row_view))
                .map(|c| {
                    let mut vs: Vec<RawInline> = values.clone();
                    c.confirm(variable, &row_view, &mut CandidateSink::Values(&mut vs));
                    vs
                })
                .kmerge()
                .dedup()
                .collect();

            _ = mem::replace(values, union);
        });
    }

    /// Returns `true` when **at least one** variant is satisfied for
    /// every row.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.iter().all(|row| {
            let row_view = RowsView::new(view.vars, row);
            self.constraints.iter().any(|c| c.satisfied(&row_view))
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

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        Some(self)
    }
}

impl<'a, C> ConstraintChildren<'a> for UnionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    fn len(&self) -> usize {
        self.constraints.len()
    }

    fn child(&self, index: usize) -> &dyn Constraint<'a> {
        &self.constraints[index]
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
