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
