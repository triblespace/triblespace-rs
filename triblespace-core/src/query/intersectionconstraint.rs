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
    /// confirms through the rest in ascending estimate order — kills land
    /// in the region's liveness words; nothing is compacted. Children that return `None` for this variable
    /// are skipped entirely.
    ///
    /// Only the tail region this call appended (from the incoming buffer
    /// length onward) is confirmed, so proposals appended by
    /// sibling constraints in an enclosing composite are never filtered
    /// through this intersection's children.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        if relevant_constraints.is_empty() {
            return;
        }
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        let base = proposals.len();
        relevant_constraints[0]
            .1
            .propose(variable, binding, proposals);

        let mut region = proposals.region(base);
        for (_, c) in relevant_constraints[1..].iter() {
            c.confirm(variable, binding, &mut region);
        }
    }

    /// Batched propose: the tightest child proposes for the **whole
    /// batch** and the remaining children confirm the whole region before
    /// it reaches the caller — candidates never sit unconfirmed in the
    /// buffer, and every confirmer sees a region as wide as the frontier
    /// rather than one parent's handful.
    ///
    /// The proposer is chosen once per batch from
    /// [`frontier_estimate`](Constraint::frontier_estimate) (a sampled
    /// aggregate) rather than once per row. Which children are *relevant*
    /// is not a sampling question at all: `estimate` returns `None` exactly
    /// outside a constraint's [`VariableSet`], which is binding-independent,
    /// so the relevant set is the same for every row. Only the ordering is
    /// sampled, and ordering is a performance heuristic — confirm is
    /// kill-only, so any order computes the same conjunction.
    fn propose_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.frontier_estimate(variable, frontier)?, c)))
            .collect();
        if relevant_constraints.is_empty() {
            // No child constrains this variable, but the segment-per-row
            // shape is still owed to the caller.
            for _ in 0..frontier.len() {
                proposals.open_row();
            }
            return;
        }
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        let base = proposals.len();
        let segment_base = proposals.segments();
        relevant_constraints[0]
            .1
            .propose_frontier(variable, frontier, proposals);

        let mut region = proposals.region_since(base, segment_base);
        for (_, c) in relevant_constraints[1..].iter() {
            c.confirm_frontier(variable, frontier, &mut region);
        }
    }

    /// Confirms proposals through all children that constrain `variable`,
    /// in order of increasing estimate, all killing into the shared mask.
    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.estimate(variable, binding)?, c)))
            .collect();
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        for (_, c) in relevant_constraints.iter() {
            c.confirm(variable, binding, cands);
        }
    }

    /// Batched confirm: every child that constrains `variable` judges the
    /// whole region, in sampled-estimate order, all killing into the shared
    /// liveness words.
    ///
    /// Passing the region through intact rather than per segment is the
    /// point: it is what lets a batch-aware child (the GPU archive) see a
    /// region wide enough to be worth a device dispatch at *every* level,
    /// not just at the root.
    fn confirm_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        cands: &mut Candidates<'_>,
    ) {
        let mut relevant_constraints: SmallVec<[(usize, &C); 8]> = self
            .constraints
            .iter()
            .filter_map(|c| Some((c.frontier_estimate(variable, frontier)?, c)))
            .collect();
        relevant_constraints.sort_unstable_by_key(|(estimate, _)| *estimate);

        for (_, c) in relevant_constraints.iter() {
            c.confirm_frontier(variable, frontier, cands);
        }
    }

    /// Returns `true` only when **every** child is satisfied.
    fn satisfied(&self, binding: &Binding) -> bool {
        self.constraints.iter().all(|c| c.satisfied(binding))
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
