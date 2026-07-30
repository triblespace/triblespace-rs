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
/// deduplicated, and confirmations are ORed per candidate region.
///
/// Before proposing or confirming, the union checks each variant's
/// [`satisfied`](Constraint::satisfied) status for the row and skips
/// variants that are provably dead. This is not a leftover of the old
/// hidden-variable desugar — folding literals into constant `Term`s did
/// not retire it, because deadness does not come from the constants. It
/// comes from a variant being a *conjunction*: `pattern!` lowers to an
/// [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint) with
/// one clause per triple, and a propose/confirm pass consults only the
/// clauses that return `Some` from `estimate` for the variable at hand.
/// So in an arm like `{ ?p @ nickname: "Ali", city: ?out }` the `nickname`
/// clause takes no part in the `?out` pass at all. Once `?p` is bound to
/// an entity whose nickname is not `"Ali"` that arm is logically dead, yet
/// its `city` clause would still propose the entity's city — and since the
/// union ORs the per-variant survivors, the arm then confirms its own
/// proposal and the row escapes. The liveness gate is what notices: the
/// pinned clause's own `satisfied` is `false`, the intersection conjoins
/// that to kill the arm, and the union drops the arm's contribution for
/// that row. Both call sites are independently load-bearing; the
/// `union_dead_variant_leak` integration test pins one leak per site.
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

    /// Returns the **sum** of estimates across all variants. A union can
    /// produce candidates from any branch, so the cardinalities add.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraints
            .iter()
            .filter_map(|c| c.estimate(variable, binding))
            .reduce(|acc, e| acc + e)
    }

    /// Collects proposals from every variant that is satisfied *for a
    /// given row*, then sorts and deduplicates per row. Dead variants
    /// (where [`satisfied`](Constraint::satisfied) returns `false`) are
    /// skipped so their stale bindings cannot inject values that no live
    /// variant would produce.
    ///
    /// With a batch, "dead" is per row: a variant alive nowhere is skipped
    /// entirely (the single-binding behaviour), a variant alive everywhere
    /// proposes untouched, and a variant alive for only some rows has the
    /// rest of its contribution dropped again before the sort.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let rows = frontier.len();
        let base = proposals.len();
        let mut satisfied = vec![false; rows];
        for c in self.constraints.iter() {
            let mut any = false;
            let mut all = true;
            for (row, slot) in satisfied.iter_mut().enumerate() {
                *slot = c.satisfied(&frontier.row(row));
                any |= *slot;
                all &= *slot;
            }
            if !any {
                continue;
            }
            let variant_base = proposals.len();
            c.propose(variable, frontier, proposals);
            if !all {
                proposals.retain_region(variant_base, |row, _| satisfied[row as usize]);
            }
        }
        // Freshness rule: a proposer may rewrite its own freshly-appended
        // region before returning — indices freeze once the caller can see
        // them. The union's set semantics need the sort-dedup, and the key
        // is `(row, value)`: the set is per parent binding, not across the
        // batch. Sorting by that key also restores contiguous segments.
        // `tagged` yields only live entries, which matters here: a variant
        // may kill inside its own propose (an `and!` arm whose narrow side
        // confirms as it goes), and `rewrite_region` republishes everything
        // it is handed as live, so reading the dead back would resurrect
        // them.
        let mut fresh: Vec<(u32, RawInline)> = proposals.tagged(base).collect();
        fresh.sort_unstable();
        fresh.dedup();
        proposals.rewrite_region(base, fresh);
    }

    /// Confirms proposals against every variant that is satisfied for the
    /// candidate's own row (each on a scratch copy of the region's
    /// liveness) and ors the per-variant survivors together. A value passes
    /// if *any* live variant confirms it.
    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let rows = frontier.len();
        let mut any_live = vec![0u32; cands.len()];
        let mut satisfied = vec![false; rows];
        for c in self.constraints.iter() {
            let mut any = false;
            let mut all = true;
            for (row, slot) in satisfied.iter_mut().enumerate() {
                *slot = c.satisfied(&frontier.row(row));
                any |= *slot;
                all &= *slot;
            }
            if !any {
                continue;
            }
            let mut scratch = cands.live_words();
            if !all {
                // A variant that is dead for a row must not vote for that
                // row's candidates.
                for (i, word) in scratch.iter_mut().enumerate() {
                    if !satisfied[cands.parent(i) as usize] {
                        *word = 0;
                    }
                }
            }
            c.confirm(variable, frontier, &mut cands.scratch(&mut scratch));
            or_words(&mut any_live, &scratch);
        }
        for i in 0..cands.len() {
            if any_live[i] == 0 {
                cands.kill(i);
            }
        }
    }

    /// Returns `true` when **at least one** variant is satisfied.
    fn satisfied(&self, binding: &Binding) -> bool {
        self.constraints.iter().any(|c| c.satisfied(binding))
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
