use super::*;
use smallvec::SmallVec;

/// Logical conjunction of constraints (AND).
///
/// All children must agree on every variable binding. Built by the
/// [`and!`](crate::and) macro or directly via [`new`](Self::new).
///
/// The intersection delegates to its children using cardinality-aware
/// ordering: per row, the child with the lowest
/// [`estimate`](Constraint::estimate) proposes candidates, and the
/// remaining children [`confirm`](Constraint::confirm) them — not per
/// branch, but in whole-frontier passes, one per child. That deferral is
/// what fuses the per-branch confirm trickle into one ragged batch per
/// (child, level), which is what makes batched probe streams and accelerator
/// dispatch possible in the first place.
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

impl<'a, C> ConstraintChildren<'a> for IntersectionConstraint<C>
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

    /// Pushes the elementwise **minimum** estimate across children that
    /// constrain `variable`. The tightest child bounds the search per
    /// row, reflecting the intersection semantics: every child must
    /// agree, so the smallest candidate set dominates.
    ///
    /// The scalar (single-row cursor) arm folds child estimates through
    /// stack slots — no column scratch is ever allocated on the
    /// sequential engine's path.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        match out {
            EstimateSink::Scalar(slot) => {
                let mut any = false;
                let mut acc = usize::MAX;
                for c in &self.constraints {
                    let mut e = 0usize;
                    if c.estimate(variable, view, &mut EstimateSink::Scalar(&mut e)) {
                        any = true;
                        acc = acc.min(e);
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
                                *o = (*o).min(s);
                            }
                        }
                    }
                }
                any
            }
        }
    }

    /// Frontier expansion: per row the tightest child proposes (one
    /// estimate column per relevant child, argmin per row), then the
    /// sibling confirms run as **whole-frontier passes** — one per child,
    /// cheapest (first-row estimate) first.
    ///
    /// When one child proposes for *every* row (the common case —
    /// proposer choice is usually structural), it receives the whole
    /// block so its own batching kicks in, and it skips its confirm pass
    /// (its own proposals are consistent by construction). When proposers
    /// vary across rows each row is proposed through a single-row
    /// borrowed view into an **isolated** scratch sink — a child must
    /// never see candidates owned by another row, because composite
    /// children confirm whatever sink they are handed in its entirety —
    /// and every relevant child then confirms the full frontier.
    /// Re-confirming a child's own pairs is a wasted-work cost, never a
    /// correctness one.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        // The sequential cursor (a Values sink is always a block of 1):
        // scalar child estimates in stack slots, argmin, propose, ordered
        // confirms — no estimate columns, no heap scratch.
        if matches!(candidates, CandidateSink::Values(_)) {
            let mut relevant: SmallVec<[(usize, usize); 16]> = SmallVec::new();
            for (ci, c) in self.constraints.iter().enumerate() {
                let mut e = 0usize;
                if c.estimate(variable, view, &mut EstimateSink::Scalar(&mut e)) {
                    relevant.push((e, ci));
                }
            }
            if relevant.is_empty() {
                return;
            }
            relevant.sort_unstable_by_key(|&(estimate, _)| estimate);
            self.constraints[relevant[0].1].propose(variable, view, candidates);
            for &(_, ci) in &relevant[1..] {
                self.constraints[ci].confirm(variable, view, candidates);
            }
            return;
        }

        let n_rows = view.len();

        // Pass 1: per-child estimate columns (flat, child-major) — the
        // same cardinality data drives proposer choice AND confirm order.
        let mut cols: Vec<usize> = Vec::new();
        let mut relevant: SmallVec<[usize; 16]> = SmallVec::new();
        for (ci, c) in self.constraints.iter().enumerate() {
            if c.estimate(variable, view, &mut EstimateSink::Column(&mut cols)) {
                relevant.push(ci);
            }
        }
        if relevant.is_empty() {
            return;
        }

        // Pass 2: per-row proposer = argmin across the columns.
        let mut propose_counts: SmallVec<[usize; 16]> = SmallVec::from_elem(0, relevant.len());
        let mut proposers: SmallVec<[u32; 32]> = SmallVec::with_capacity(n_rows);
        for i in 0..n_rows {
            let k = (0..relevant.len())
                .min_by_key(|&k| cols[k * n_rows + i])
                .expect("non-empty relevant");
            propose_counts[k] += 1;
            proposers.push(k as u32);
        }

        // Pass 3: expand the frontier.
        let uniform = (0..relevant.len()).find(|&k| propose_counts[k] == n_rows);
        if let Some(k) = uniform {
            self.constraints[relevant[k]].propose(variable, view, candidates);
        } else {
            // Non-uniform proposers: each row's child proposes into an
            // isolated, cleared single-row scratch — never into the
            // shared, already-populated frontier sink. `propose` is not
            // append-only for composite children: a nested intersection
            // runs its sibling confirms over the ENTIRE sink it is
            // handed, interpreting every row tag through the one-row
            // view — which deletes other rows' legitimate candidates
            // under the wrong bindings, or indexes past the end of the
            // one-row view for tags ≥ 1. A single-row view with a
            // Values sink is exactly the sequential-cursor contract
            // every constraint already honors, so isolation is free;
            // `extend_row` then applies this row's tag on the way out.
            let mut scratch: Vec<RawInline> = Vec::new();
            for (i, &k) in proposers.iter().enumerate() {
                let row_view = view.row_view(i);
                scratch.clear();
                self.constraints[relevant[k as usize]].propose(
                    variable,
                    &row_view,
                    &mut CandidateSink::Values(&mut scratch),
                );
                candidates.extend_row(i as u32, scratch.iter().copied());
            }
        }

        // Pass 4: whole-frontier confirms, cheapest (first-row estimate)
        // child first. The uniform proposer skips its own pass.
        let mut confirmers: SmallVec<[(usize, usize); 16]> = relevant
            .iter()
            .enumerate()
            .filter(|&(k, _)| uniform != Some(k))
            .map(|(k, &ci)| (cols[k * n_rows], ci))
            .collect();
        confirmers.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in confirmers {
            self.constraints[ci].confirm(variable, view, candidates);
        }
    }

    /// Confirms a whole frontier through every relevant child in
    /// ascending (first-row) estimate order.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let first = view.row_view(0);
        let mut relevant: SmallVec<[(usize, usize); 16]> = SmallVec::new();
        for (ci, c) in self.constraints.iter().enumerate() {
            let mut est = 0usize;
            if c.estimate(variable, &first, &mut EstimateSink::Scalar(&mut est)) {
                relevant.push((est, ci));
            }
        }
        relevant.sort_unstable_by_key(|&(estimate, _)| estimate);
        for (_, ci) in relevant {
            self.constraints[ci].confirm(variable, view, candidates);
        }
    }

    /// Returns `true` only when **every** child is satisfied.
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.constraints.iter().all(|c| c.satisfied(view))
    }

    /// Returns the union of all children's influence sets for `variable`.
    fn influence(&self, variable: VariableId) -> VariableSet {
        self.constraints
            .iter()
            .fold(VariableSet::new_empty(), |acc, c| {
                acc.union(c.influence(variable))
            })
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        ConstraintShape::And(self)
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
