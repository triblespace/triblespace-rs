use super::*;
use smallvec::SmallVec;

/// Logical conjunction of constraints (AND).
///
/// All children must agree on every variable binding. Built by the
/// [`and!`](crate::and) macro or directly via [`new`](Self::new).
///
/// The intersection delegates to its children using cardinality-aware
/// ordering: the child with the lowest [`estimate`](Constraint::estimate)
/// proposes candidates, and the remaining children
/// [`confirm`](Constraint::confirm) them in order of increasing estimate.
/// This strategy keeps the candidate set small from the start and avoids
/// materialising cross products.
///
/// Variables from all children are exposed as a single union, so the
/// engine sees one flat set of variables regardless of how many
/// sub-constraints contribute.
pub struct IntersectionConstraint<C> {
    constraints: Vec<C>,
}

impl<'a, C> IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Creates an intersection over the given constraints.
    pub fn new(constraints: Vec<C>) -> Self {
        IntersectionConstraint { constraints }
    }
}

impl<'a, C> Constraint<'a> for IntersectionConstraint<C>
where
    C: Constraint<'a> + 'a,
{
    /// Returns the union of all children's variable sets.
    fn variables(&self) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |vs, c| vs.union(c.variables()))
    }

    /// Returns the **minimum** estimate across children that constrain
    /// `variable`. The tightest child bounds the search, reflecting the
    /// intersection semantics: every child must agree, so the smallest
    /// candidate set dominates.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        self.constraints
            .iter()
            .filter_map(|c| c.estimate(variable, binding))
            .min()
    }

    /// Sorts children by estimate, lets the tightest one propose, then
    /// confirms through the rest in ascending estimate order. Children
    /// that return `None` for this variable are skipped entirely.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        if relevant_constraints.is_empty() {
            return;
        }
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        relevant_constraints[0]
            .1
            .propose(variable, binding, proposals);

        relevant_constraints[1..]
            .iter()
            .for_each(|(_, c)| c.confirm(variable, binding, proposals));
    }

    /// Confirms proposals through all children that constrain `variable`,
    /// in order of increasing estimate.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        relevant_constraints
            .iter()
            .for_each(|(_, c)| c.confirm(variable, binding, proposals));
    }

    /// Returns `true` only when **every** child is satisfied.
    fn satisfied(&self, binding: &Binding) -> bool {
        self.constraints.iter().all(|c| c.satisfied(binding))
    }

    /// PROBE: frontier expansion for the blocked solver.
    ///
    /// Per row the tightest child proposes (same cardinality-aware choice
    /// as [`propose`](Self::propose)), but the sibling confirms are **not**
    /// run per row — they are deferred to whole-frontier
    /// [`confirm_blocked`](Constraint::confirm_blocked) passes, one per
    /// child, over all rows' candidates at once. That deferral is the
    /// entire point of the blocked engine: it fuses the per-branch confirm
    /// trickle into one ragged batch per (child, level).
    ///
    /// A child that proposed for *every* row skips its confirm pass (its
    /// own proposals are consistent by construction). When proposers vary
    /// across rows the pass runs over the full frontier, re-confirming the
    /// child's own pairs — a wasted-work cost, never a correctness one.
    fn propose_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let stride = vars.len();
        debug_assert!(stride > 0);
        let n_rows = rows.len() / stride;
        let mut binding = Binding::default();
        let mut propose_counts = vec![0usize; self.constraints.len()];
        let mut rows_proposed = 0usize;

        // Pass 1: per-row proposer choice (cardinality-aware, per-row
        // estimates exactly like the sequential engine).
        let mut proposers: Vec<Option<usize>> = Vec::with_capacity(n_rows);
        for row in rows.chunks_exact(stride) {
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &row[k]);
            }
            let mut best: Option<(usize, usize)> = None;
            for (ci, c) in self.constraints.iter().enumerate() {
                if let Some(estimate) = c.estimate(variable, &binding) {
                    if best.map_or(true, |(be, _)| estimate < be) {
                        best = Some((estimate, ci));
                    }
                }
            }
            if let Some((_, ci)) = best {
                propose_counts[ci] += 1;
                rows_proposed += 1;
            }
            proposers.push(best.map(|(_, ci)| ci));
        }

        // Pass 2: expand the frontier. When one child proposes for every
        // row (the common case — proposer choice is usually structural),
        // hand it the whole block so its own `propose_blocked` batching
        // kicks in; otherwise fall back to per-row proposes. Rows with no
        // relevant child propose nothing — the branch dies, matching the
        // sequential engine's empty proposal set.
        let uniform = proposers.first().copied().flatten().filter(|&ci| {
            rows_proposed == n_rows && propose_counts[ci] == n_rows
        });
        if let Some(ci) = uniform {
            self.constraints[ci].propose_blocked(variable, vars, rows, pairs);
        } else {
            let mut scratch: Vec<RawInline> = Vec::new();
            for (i, row) in rows.chunks_exact(stride).enumerate() {
                let Some(ci) = proposers[i] else { continue };
                for (k, &v) in vars.iter().enumerate() {
                    binding.set(v, &row[k]);
                }
                scratch.clear();
                self.constraints[ci].propose(variable, &binding, &mut scratch);
                pairs.extend(scratch.iter().map(|&val| (i as u32, val)));
            }
        }

        // Whole-frontier confirm passes, cheapest child first (first-row
        // estimates; participation is structural, so the child set is
        // uniform across rows even though the estimate values are not).
        for (k, &v) in vars.iter().enumerate() {
            binding.set(v, &rows[k]);
        }
        let mut confirmers: SmallVec<[(usize, usize); 8]> = self
            .constraints
            .iter()
            .enumerate()
            .filter_map(|(ci, c)| Some((c.estimate(variable, &binding)?, ci)))
            .collect();
        confirmers.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in confirmers {
            if propose_counts[ci] == rows_proposed {
                continue;
            }
            self.constraints[ci].confirm_blocked(variable, vars, rows, pairs);
        }
    }

    /// PROBE: confirms a whole frontier through every relevant child in
    /// ascending (first-row) estimate order.
    fn confirm_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let stride = vars.len();
        debug_assert!(stride > 0);
        let mut binding = Binding::default();
        for (k, &v) in vars.iter().enumerate() {
            binding.set(v, &rows[k]);
        }
        let mut relevant: SmallVec<[(usize, usize); 8]> = self
            .constraints
            .iter()
            .enumerate()
            .filter_map(|(ci, c)| Some((c.estimate(variable, &binding)?, ci)))
            .collect();
        relevant.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in relevant {
            self.constraints[ci].confirm_blocked(variable, vars, rows, pairs);
        }
    }

    /// Returns the union of all children's influence sets for `variable`.
    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |acc, c| {
                acc.union(c.influence(variable))
            })
    }
}

/// Combines constraints into an [`IntersectionConstraint`] (logical AND).
///
/// All constraints must agree on every variable binding for a result to
/// be produced. Accepts one or more constraint expressions.
///
/// ```rust,ignore
/// and!(set.pattern(e, a, v), allowed.has(v))
/// ```
#[macro_export]
macro_rules! and {
    // Emits `Arc<IntersectionConstraint<Box<dyn Constraint + Send + Sync>>>`.
    // The outer `Arc` makes the whole tree cheap to `Clone` (single
    // refcount bump) — required by the `parallel` feature's `Query::clone`
    // during rayon split. `Send + Sync` on the trait object lets the tree
    // cross rayon thread boundaries. Every in-tree constraint built via
    // this macro already satisfies Send + Sync; non-thread-safe constraint
    // types (e.g. `Rc`-backed ContainsConstraint variants) can still be
    // used via direct `IntersectionConstraint::new` construction.
    ($($c:expr),+ $(,)?) => (
        ::std::sync::Arc::new(
            $crate::query::intersectionconstraint::IntersectionConstraint::new(vec![
                $(Box::new($c)
                    as Box<dyn $crate::query::Constraint + Send + Sync>),+
            ])
        )
    )
}

/// Re-export of the [`and!`] macro.
pub use and;
