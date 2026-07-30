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
/// deduplicated, and confirmations are ORed per candidate region.
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

    /// Returns the **sum** of estimates across all variants. A union can
    /// produce candidates from any branch, so the cardinalities add.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraints
            .iter()
            .filter_map(|c| c.estimate(variable, binding))
            .reduce(|acc, e| acc + e)
    }

    /// Collects proposals from every *satisfied* variant, then sorts and
    /// deduplicates. Dead variants (where [`satisfied`](Constraint::satisfied)
    /// returns `false`) are skipped so their stale bindings cannot inject
    /// values that no live variant would produce.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        let base = proposals.len();
        self.constraints
            .iter()
            .filter(|c| c.satisfied(binding))
            .for_each(|c| c.propose(variable, binding, proposals));
        // Freshness rule: a proposer may rewrite its own freshly-appended
        // region before returning — indices freeze once the caller can see
        // them. The union's set semantics need the sort-dedup.
        //
        // Read `live_values`, not the values alone: a variant is free to kill
        // inside its own propose (an `and!` arm whose narrow side confirms as
        // it goes), and `rewrite_region` republishes everything it is handed
        // as live. Collecting through the `Deref` would carry those corpses
        // back in — the buffer is kill-only, so a value that died here must
        // stay dead.
        let mut fresh: Vec<RawInline> = proposals.live_values(base).copied().collect();
        fresh.sort_unstable();
        fresh.dedup();
        proposals.rewrite_region(base, fresh);
    }

    /// Confirms proposals against every *satisfied* variant independently
    /// (each on a scratch copy of the region's liveness) and ors the per-variant
    /// survivors together. A value passes if *any* live variant confirms it.
    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        // `any` accumulates, per candidate, whether *some* live variant kept
        // it. It is sized in liveness **words**, not candidates: under the
        // `liveness-bitmask` representation one word carries 32 candidates and
        // a region that starts mid-word needs one word more than a candidate
        // count implies. `live_word_len` is the only thing that knows.
        let mut any = vec![0u32; cands.live_word_len()];
        let mut scratch;
        for c in self.constraints.iter().filter(|c| c.satisfied(binding)) {
            // Each variant votes on its own copy of the region's liveness, so
            // one variant's kills cannot hide a candidate from the next. The
            // scratch keeps the region's bit alignment, which is what lets the
            // votes be merged word-wise.
            scratch = cands.live_words();
            c.confirm(variable, binding, &mut cands.scratch(&mut scratch));
            or_words(&mut any, &scratch);
        }
        // Kill-only by construction: every `scratch` started as a copy of the
        // liveness on entry and confirmers may only clear, so `any` is a
        // subset of what was already live — writing it back kills exactly the
        // candidates no variant confirmed and revives nothing.
        //
        // Write through `set_live_words` rather than a kill loop because that
        // is the one path that knows about region boundaries: bit-packed, the
        // first and last words of a region carry bits owned by *neighbouring*
        // regions of the same buffer, and it masks them out. Do not
        // "simplify" this into a direct word copy.
        cands.set_live_words(&any);
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
