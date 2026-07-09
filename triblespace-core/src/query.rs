//! Query facilities for matching tribles by declaring patterns of constraints.
//! Build queries with the [`find!`](crate::prelude::find) macro which binds variables and
//! combines constraint expressions:
//!
//! ```
//! # use triblespace_core::prelude::*;
//! # use triblespace_core::prelude::inlineencodings::ShortString;
//! let results = find!((x: Inline<ShortString>), x.is("foo".to_inline())).collect::<Vec<_>>();
//! ```
//!
//! Variables are converted via [`TryFromInline`](crate::inline::TryFromInline). By default,
//! conversion failures silently skip the row (filter semantics). Append `?` to a variable
//! to receive `Result<T, E>` instead, letting the caller handle errors explicitly.
//!
//! For a tour of the language see the "Query Language" chapter in the book.
//! Conceptual background on schemas and join strategy appears in the
//! "Query Engine" and "Atreides Join" chapters.
/// [`ConstantConstraint`] — pins a variable to a single value.
pub mod constantconstraint;
/// [`EqualityConstraint`](equalityconstraint::EqualityConstraint) — constrains two variables to have the same value.
pub mod equalityconstraint;
/// [`KeysConstraint`](hashmapconstraint::KeysConstraint) — constrains a variable to HashMap keys.
pub mod hashmapconstraint;
/// [`SetConstraint`](hashsetconstraint::SetConstraint) — constrains a variable to HashSet members.
pub mod hashsetconstraint;
/// [`IgnoreConstraint`] — hides variables from the outer query.
pub mod ignore;
/// [`IntersectionConstraint`](intersectionconstraint::IntersectionConstraint) — logical AND.
pub mod intersectionconstraint;
/// [`PatchValueConstraint`](patchconstraint::PatchValueConstraint) and [`PatchIdConstraint`](patchconstraint::PatchIdConstraint) — constrains variables to PATCH entries.
pub mod patchconstraint;
/// [`InlineRange`](rangeconstraint::InlineRange) — restricts a variable to a byte-lexicographic range.
pub mod rangeconstraint;
/// [`RegularPathConstraint`] — regular path expressions over graphs.
pub mod regularpathconstraint;
/// [`SortedSliceConstraint`](sortedsliceconstraint::SortedSliceConstraint) — constrains a variable to values in a sorted slice (binary search confirm).
pub mod sortedsliceconstraint;
/// [`UnionConstraint`](unionconstraint::UnionConstraint) — logical OR.
pub mod unionconstraint;
mod variableset;

use std::fmt;
use std::iter::FromIterator;
use std::marker::PhantomData;

use arrayvec::ArrayVec;
use constantconstraint::*;
/// Re-export of [`IgnoreConstraint`].
pub use ignore::IgnoreConstraint;

use crate::inline::encodings::genid::GenId;
use crate::inline::RawInline;
use crate::inline::Inline;
use crate::inline::InlineEncoding;

/// Re-export of [`PathOp`].
pub use regularpathconstraint::PathOp;
/// Re-export of [`RegularPathConstraint`].
pub use regularpathconstraint::RegularPathConstraint;
/// Re-export of [`VariableSet`](variableset::VariableSet).
pub use variableset::VariableSet;

/// Types storing tribles can implement this trait to expose them to queries.
/// The trait provides a method to create a constraint for a given trible pattern.
pub trait TriblePattern {
    /// The type of the constraint created by the pattern method.
    ///
    /// `Send + Sync` is required so the resulting constraint tree can be
    /// used with the `parallel` feature's rayon iterators. Every in-tree
    /// pattern backend (TribleSet, SuccinctArchive) satisfies this; custom
    /// implementations should hold their data behind `Arc` or similar.
    type PatternConstraint<'a>: Constraint<'a> + Send + Sync
    where
        Self: 'a;

    /// Create a constraint for a given trible pattern.
    /// The method takes three variables, one for each part of the trible.
    /// The schemas of the entities and attributes are always [GenId], while the value
    /// schema can be any type implementing [InlineEncoding] and is specified as a type parameter.
    ///
    /// This method is usually not called directly, but rather through typed query language
    /// macros like [pattern!][crate::macros::pattern].
    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: Variable<GenId>,
        a: Variable<GenId>,
        v: Variable<V>,
    ) -> Self::PatternConstraint<'a>;
}

/// Low-level identifier for a variable in a query.
pub type VariableId = usize;

/// Context for creating variables in a query.
/// The context keeps track of the next index to assign to a variable.
/// This allows for the creation of new anonymous variables in higher-level query languages.
#[derive(Debug)]
pub struct VariableContext {
    /// The index that will be assigned to the next variable.
    pub next_index: VariableId,
}

impl Default for VariableContext {
    fn default() -> Self {
        Self::new()
    }
}

impl VariableContext {
    /// Create a new variable context.
    /// The context starts with an index of 0.
    pub fn new() -> Self {
        VariableContext { next_index: 0 }
    }

    /// Create a new variable.
    /// The variable is assigned the next available index.
    ///
    /// Panics if the number of variables exceeds 128.
    ///
    /// This method is usually not called directly, but rather through typed query language
    /// macros like [find!][crate::query].
    pub fn next_variable<T: InlineEncoding>(&mut self) -> Variable<T> {
        assert!(
            self.next_index < 128,
            "currently queries support at most 128 variables"
        );
        let v = Variable::new(self.next_index);
        self.next_index += 1;
        v
    }
}

/// A placeholder for unknowns in a query.
/// Within the query engine each variable is identified by an integer,
/// which can be accessed via the `index` property.
/// Variables also have an associated type which is used to parse the [Inline]s
/// found by the query engine.
#[derive(Debug)]
pub struct Variable<T: InlineEncoding> {
    /// The integer index identifying this variable in the [`Binding`].
    pub index: VariableId,
    typed: PhantomData<T>,
}

impl<T: InlineEncoding> Copy for Variable<T> {}

impl<T: InlineEncoding> Clone for Variable<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: InlineEncoding> Variable<T> {
    /// Creates a variable with the given index.
    pub fn new(index: VariableId) -> Self {
        Variable {
            index,
            typed: PhantomData,
        }
    }

    /// Extracts the bound value for this variable from `binding`.
    ///
    /// # Panics
    ///
    /// Panics if the variable has not been bound.
    pub fn extract(self, binding: &Binding) -> &Inline<T> {
        let raw = binding.get(self.index).unwrap_or_else(|| {
            panic!(
                "query variable (idx {}) was never bound before projection. This usually means the variable was projected in `find!` but never appeared in any constraint. If you intended a pure existence query, use `find!((), ...)` or `exists!(constraint)`.",
                self.index
            )
        });
        Inline::as_transmute_raw(raw)
    }
}

/// Collections can implement this trait so that they can be used in queries.
/// The returned constraint will filter the values assigned to the variable
/// to only those that are contained in the collection.
pub trait ContainsConstraint<'a, T: InlineEncoding> {
    /// The concrete constraint type produced by [`has`](ContainsConstraint::has).
    type Constraint: Constraint<'a>;

    /// Create a constraint that filters the values assigned to the variable
    /// to only those that are contained in the collection.
    ///
    /// The returned constraint will usually perform a conversion between the
    /// concrete rust type stored in the collection a [Inline] of the appropriate schema
    /// type for the variable.
    fn has(self, v: Variable<T>) -> Self::Constraint;
}

impl<T: InlineEncoding> Variable<T> {
    /// Create a constraint so that only a specific value can be assigned to the variable.
    pub fn is(self, constant: Inline<T>) -> ConstantConstraint {
        ConstantConstraint::new(self, constant)
    }
}

/// The binding keeps track of the values assigned to variables in a query.
/// It maps variables to values - by their index - via a simple array,
/// and keeps track of which variables are bound.
/// It is used to store intermediate results and to pass information
/// between different constraints.
/// The binding is mutable, as it is modified by the query engine.
/// It is not thread-safe and should not be shared between threads.
/// The binding is a simple data structure that is cheap to clone.
/// It is not intended to be used as a long-term storage for query results.
#[derive(Clone, Debug)]
pub struct Binding {
    /// Bitset tracking which variables have been assigned a value.
    pub bound: VariableSet,
    values: [RawInline; 128],
}

impl Binding {
    /// Binds `variable` to `value`.
    pub fn set(&mut self, variable: VariableId, value: &RawInline) {
        self.values[variable] = *value;
        self.bound.set(variable);
    }

    /// Unset a variable in the binding.
    /// This is used to backtrack in the query engine.
    pub fn unset(&mut self, variable: VariableId) {
        self.bound.unset(variable);
    }

    /// Check if a variable is bound in the binding.
    pub fn get(&self, variable: VariableId) -> Option<&RawInline> {
        if self.bound.is_set(variable) {
            Some(&self.values[variable])
        } else {
            None
        }
    }
}

impl Default for Binding {
    fn default() -> Self {
        Self {
            bound: VariableSet::new_empty(),
            values: [[0; 32]; 128],
        }
    }
}

/// The cooperative protocol that every query participant implements.
///
/// A constraint restricts the values that can be assigned to query variables.
/// The query engine does not plan joins in advance; instead it consults
/// constraints directly during a depth-first search over partial bindings.
/// Each constraint reports which variables it touches, estimates how many
/// candidates remain, enumerates concrete values on demand, and signals
/// whether its requirements are still satisfiable. This protocol is the
/// sole interface between the engine and the data — whether that data lives
/// in a [`TribleSet`](crate::trible::TribleSet), a [`HashMap`](std::collections::HashMap),
/// or a custom application predicate.
///
/// # The protocol
///
/// The engine drives the search by calling five methods in a fixed rhythm:
///
/// | Method | Role | Called when |
/// |--------|------|------------|
/// | [`variables`](Constraint::variables) | Declares which variables the constraint touches. | Once, at query start. |
/// | [`estimate`](Constraint::estimate) | Predicts the candidate count for a variable. | Before each binding decision. |
/// | [`propose`](Constraint::propose) | Enumerates candidate values for a variable. | On the most selective constraint. |
/// | [`confirm`](Constraint::confirm) | Filters candidates proposed by another constraint. | On all remaining constraints. |
/// | [`satisfied`](Constraint::satisfied) | Checks whether fully-bound sub-constraints still hold. | Before propose/confirm in composite constraints. |
///
/// [`influence`](Constraint::influence) completes the picture by telling the
/// engine which estimates to refresh when a variable is bound or unbound.
///
/// # Statelessness
///
/// Constraints are stateless: every method receives the current [`Binding`]
/// as a parameter rather than maintaining internal bookkeeping. This lets
/// the engine backtrack freely by unsetting variables in the binding
/// without notifying the constraints.
///
/// # Composability
///
/// Constraints combine via [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint)
/// (logical AND — built by [`and!`](crate::and)) and
/// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
/// (logical OR — built by [`or!`](crate::or)). Because every constraint
/// speaks the same protocol, heterogeneous data sources mix freely in a
/// single query.
///
/// # Implementing a custom constraint
///
/// A new constraint only needs to implement [`variables`](Constraint::variables),
/// [`estimate`](Constraint::estimate), [`propose`](Constraint::propose), and
/// [`confirm`](Constraint::confirm). Override [`satisfied`](Constraint::satisfied)
/// when the constraint can detect unsatisfiability before the engine asks
/// about individual variables (e.g. a fully-bound triple lookup that found
/// no match). Override [`influence`](Constraint::influence) when binding one
/// variable changes the estimates for a non-obvious set of others.
pub trait Constraint<'a> {
    /// Returns the set of variables this constraint touches.
    ///
    /// Called once at query start. The engine uses this to build influence
    /// graphs and to determine which constraints participate when a
    /// particular variable is being bound.
    fn variables(&self) -> VariableSet;

    /// Estimates the number of candidate values for `variable` given the
    /// current partial `binding`.
    ///
    /// Returns `None` when `variable` is not constrained by this constraint.
    /// The estimate need not be exact — it guides variable ordering, not
    /// correctness. Tighter estimates lead to better search pruning; see the
    /// [Atreides join](crate) family for how different estimate fidelities
    /// affect performance.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize>;

    /// Enumerates candidate values for `variable` into `proposals`.
    ///
    /// Called on the constraint with the lowest estimate for the variable
    /// being bound. Values are appended to `proposals`; the engine may
    /// already have values in the vector from a previous round.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>);

    /// Filters `proposals` to remove values for `variable` that violate
    /// this constraint.
    ///
    /// Called on every constraint *except* the one that proposed, in order
    /// of increasing estimate. Implementations remove entries from
    /// `proposals` that are inconsistent with the current `binding`.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>);

    /// Returns whether this constraint is consistent with the current
    /// `binding`.
    ///
    /// The default implementation returns `true`. Override this when the
    /// constraint can cheaply detect that no solution exists — for example,
    /// a `TribleSetConstraint`
    /// whose entity, attribute, and value are all bound but the triple is
    /// absent from the dataset.
    ///
    /// Composite constraints propagate this check to their children:
    /// [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint)
    /// requires *all* children to be satisfied, while
    /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
    /// requires *at least one*. The union uses this to skip dead variants
    /// in propose and confirm, preventing values from a satisfied variant
    /// from leaking through a dead one.
    fn satisfied(&self, _binding: &Binding) -> bool {
        true
    }

    /// Returns the set of variables whose estimates may change when
    /// `variable` is bound or unbound.
    ///
    /// The default includes every variable this constraint touches except
    /// `variable` itself. Returns an empty set when `variable` is not part
    /// of this constraint.
    fn influence(&self, variable: VariableId) -> VariableSet {
        let mut vars = self.variables();
        if vars.is_set(variable) {
            vars.unset(variable);
            vars
        } else {
            VariableSet::new_empty()
        }
    }

    // -- PROBE: frontier-batched (block-at-a-time) protocol ---------------
    //
    // The blocked solver ([`Query::solve_blocked`]) carries a *block* of
    // sibling partial bindings per search level instead of descending one
    // binding at a time. A block is a flat row store: `vars` names the
    // bound variables (identical for every row), `rows` holds
    // `rows.len() / vars.len()` rows of `vars.len()` values each — row `i`'s
    // value for `vars[j]` is `rows[i * vars.len() + j]`. Candidates for the
    // variable being solved travel as `(row_index, value)` pairs, grouped
    // by ascending row index. Both methods must preserve that grouping
    // (filtering with `retain`-style order preservation does).
    //
    // The default implementations reconstruct a scratch [`Binding`] per row
    // and delegate to [`propose`](Constraint::propose) /
    // [`confirm`](Constraint::confirm), so every existing constraint is
    // blocked-correct with zero changes; constraints with batchable probe
    // streams (e.g. `SuccinctArchiveConstraint`) override `confirm_blocked`
    // to evaluate the whole frontier in one pass.

    /// Enumerates candidates for `variable` for **every row** of a binding
    /// block, appending `(row_index, value)` pairs in ascending row order.
    ///
    /// Default: per-row scratch binding + [`propose`](Constraint::propose).
    fn propose_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let stride = vars.len();
        debug_assert!(stride > 0, "blocked propose needs at least one bound variable");
        let mut binding = Binding::default();
        let mut scratch = Vec::new();
        for (i, row) in rows.chunks_exact(stride).enumerate() {
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &row[k]);
            }
            scratch.clear();
            self.propose(variable, &binding, &mut scratch);
            pairs.extend(scratch.iter().map(|&val| (i as u32, val)));
        }
    }

    /// Filters a whole frontier of `(row_index, value)` candidates for
    /// `variable` against this constraint, removing violating pairs while
    /// preserving order.
    ///
    /// Default: group pairs by row, rebuild the row's binding, and delegate
    /// to [`confirm`](Constraint::confirm).
    fn confirm_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let stride = vars.len();
        debug_assert!(stride > 0, "blocked confirm needs at least one bound variable");
        let mut binding = Binding::default();
        let mut scratch: Vec<RawInline> = Vec::new();
        let mut out: Vec<(u32, RawInline)> = Vec::with_capacity(pairs.len());
        let mut i = 0;
        while i < pairs.len() {
            let row_idx = pairs[i].0;
            scratch.clear();
            let mut j = i;
            while j < pairs.len() && pairs[j].0 == row_idx {
                scratch.push(pairs[j].1);
                j += 1;
            }
            let row = &rows[row_idx as usize * stride..][..stride];
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &row[k]);
            }
            self.confirm(variable, &binding, &mut scratch);
            out.extend(scratch.iter().map(|&val| (row_idx, val)));
            i = j;
        }
        *pairs = out;
    }

    /// PROBE (group-by-ordering): estimates the candidate count for
    /// `variable` for **every row** of a binding block, appending one
    /// estimate per row to `out` in row order.
    ///
    /// Returns `false` (leaving `out` untouched) when `variable` is not
    /// constrained by this constraint. Relevance is assumed to be
    /// **structural** — a constraint either estimates the variable for
    /// every binding or for none, independent of the bound *values*. Every
    /// in-tree constraint satisfies this (`estimate` returns `None` purely
    /// on variable identity); the blocked intersection already leans on
    /// the same assumption for its confirm passes.
    ///
    /// Default: per-row scratch binding + [`estimate`](Constraint::estimate)
    /// — the correct baseline. Constraints with batchable estimate probes
    /// (the archive's `distinct_in` bitvector ranks / `restrict_len`
    /// wavelet ranks) can override this with a single pass that hoists the
    /// arm dispatch out of the row loop and batches the rank stream.
    fn estimate_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        out: &mut Vec<usize>,
    ) -> bool {
        let stride = vars.len();
        debug_assert!(stride > 0, "blocked estimate needs at least one bound variable");
        let mut binding = Binding::default();
        let mut chunks = rows.chunks_exact(stride);
        let Some(first) = chunks.next() else {
            // Empty block: report structural relevance from variables().
            return self.variables().is_set(variable);
        };
        for (k, &v) in vars.iter().enumerate() {
            binding.set(v, &first[k]);
        }
        let Some(estimate) = self.estimate(variable, &binding) else {
            return false;
        };
        out.push(estimate);
        for row in chunks {
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &row[k]);
            }
            out.push(
                self.estimate(variable, &binding)
                    .expect("estimate relevance must be structural across a block"),
            );
        }
        true
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for Box<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        let inner: &T = self;
        inner.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let inner: &T = self;
        inner.confirm(variable, binding, proposals)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let inner: &T = self;
        inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }

    fn propose_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let inner: &T = self;
        inner.propose_blocked(variable, vars, rows, pairs)
    }

    fn confirm_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let inner: &T = self;
        inner.confirm_blocked(variable, vars, rows, pairs)
    }

    fn estimate_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        out: &mut Vec<usize>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate_blocked(variable, vars, rows, out)
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for std::sync::Arc<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
        let inner: &T = self;
        inner.estimate(variable, binding)
    }

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawInline>) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposal: &mut Vec<RawInline>) {
        let inner: &T = self;
        inner.confirm(variable, binding, proposal)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let inner: &T = self;
        inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }

    fn propose_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let inner: &T = self;
        inner.propose_blocked(variable, vars, rows, pairs)
    }

    fn confirm_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        pairs: &mut Vec<(u32, RawInline)>,
    ) {
        let inner: &T = self;
        inner.confirm_blocked(variable, vars, rows, pairs)
    }

    fn estimate_blocked(
        &self,
        variable: VariableId,
        vars: &[VariableId],
        rows: &[RawInline],
        out: &mut Vec<usize>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate_blocked(variable, vars, rows, out)
    }
}

/// A query is an iterator over the results of a query.
/// It takes a constraint and a post-processing function as input,
/// and returns the results of the query as a stream of values.
/// The query engine uses a depth-first search to find solutions to the query,
/// proposing values for the variables and backtracking when it reaches a dead end.
/// The query engine is designed to be simple and efficient, providing low, consistent,
/// and predictable latency, skew resistance, and no required (or possible) tuning.
/// The query engine is designed to be used in combination with the [Constraint] trait,
/// which provides a simple and flexible way to implement constraints that can be used
/// to filter the results of a query.
///
/// This struct is usually not created directly, but rather through the `find!` macro,
/// which provides a convenient way to declare variables and concrete types for them.
/// And which sets up the nessecairy context for higher-level query languages
/// like the one provided by the [`crate::macros`] module.
pub struct Query<C, P: Fn(&Binding) -> Option<R>, R> {
    constraint: C,
    postprocessing: P,
    mode: Search,
    binding: Binding,
    influences: [VariableSet; 128],
    estimates: [usize; 128],
    /// PROBE (order-key experiment): each variable's estimate against the
    /// **empty** binding, frozen at [`Query::new`] — the static baseline
    /// the `ratio_first` / `influenced_only` keys compare against.
    base_estimates: [usize; 128],
    touched_variables: VariableSet,
    stack: ArrayVec<VariableId, 128>,
    unbound: ArrayVec<VariableId, 128>,
    values: ArrayVec<Option<Vec<RawInline>>, 128>,
}

// Manual `Clone` impl, because `#[derive(Clone)]` would require `R: Clone`
// which isn't actually needed — `R` only appears in `P`'s return type.
#[cfg(feature = "parallel")]
impl<C, P, R> Clone for Query<C, P, R>
where
    C: Clone,
    P: Fn(&Binding) -> Option<R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            constraint: self.constraint.clone(),
            postprocessing: self.postprocessing.clone(),
            mode: self.mode,
            binding: self.binding.clone(),
            influences: self.influences,
            estimates: self.estimates,
            base_estimates: self.base_estimates,
            touched_variables: self.touched_variables,
            stack: self.stack.clone(),
            unbound: self.unbound.clone(),
            values: self.values.clone(),
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Query<C, P, R> {
    /// Picks the next unbound variable, refreshes estimates touched by
    /// the most recent binding, re-sorts `unbound`, pushes the chosen
    /// variable onto the stack, and fills its proposal vector via
    /// [`Constraint::propose`]. Leaves `mode = NextValue`. The caller is
    /// responsible for ensuring `unbound` is non-empty.
    ///
    /// Shared between [`Iterator::next`]'s `NextVariable` branch and the
    /// [`UnindexedProducer::split`](crate::query::QueryParIter) implementation
    /// — the "push + propose" dance is identical in both.
    fn push_next_variable(&mut self) {
        let mut stale_estimates = VariableSet::new_empty();
        while let Some(variable) = self.touched_variables.drain_next_ascending() {
            stale_estimates = stale_estimates.union(self.influences[variable]);
        }
        // Bound variables can't be influenced by the unbound ones, so skip.
        stale_estimates = stale_estimates.subtract(self.binding.bound);

        if !stale_estimates.is_empty() {
            while let Some(v) = stale_estimates.drain_next_ascending() {
                self.estimates[v] = self
                    .constraint
                    .estimate(v, &self.binding)
                    .expect("unconstrained variable in query");
            }
            self.unbound.sort_unstable_by_key(|v| {
                variable_order_key(
                    self.estimates[*v],
                    self.base_estimates[*v],
                    self.influences[*v].count(),
                )
            });
        }

        let variable = self.unbound.pop().expect("non-empty unbound");
        if order_trace::enabled() {
            order_trace::record(self.stack.len(), variable, 1);
        }
        let estimate = self.estimates[variable];
        self.stack.push(variable);
        let values = self.values[variable].get_or_insert(Vec::new());
        values.clear();
        values.reserve_exact(estimate.saturating_sub(values.capacity()));
        self.constraint.propose(variable, &self.binding, values);
    }

    /// PROBE: frontier-batched (block-at-a-time) solver.
    ///
    /// The standard iterator descends one binding at a time, so on star or
    /// filter shapes every sibling branch runs its own tiny
    /// propose/confirm round — the per-branch candidate sets (≤ a few
    /// values) are far below any batching break-even. `solve_blocked`
    /// instead carries a **block** of sibling partial bindings per level
    /// and hands whole frontiers of `(row, candidate)` pairs to the
    /// constraints ([`Constraint::propose_blocked`] /
    /// [`Constraint::confirm_blocked`]): one ragged batch per
    /// (constraint, level) instead of one call per branch. Constraints
    /// with batchable probe streams (wavelet ranks in
    /// `SuccinctArchiveConstraint`) evaluate that batch cache-friendly on
    /// the CPU or as a single GPU dispatch.
    ///
    /// Semantics: yields the same result **multiset** as the iterator;
    /// row order may differ (block order instead of DFS order).
    ///
    /// Costs to be aware of (probe honesty):
    /// - Variable order is chosen **per level per block** from the first
    ///   row's estimates, not per branch; data where sibling branches want
    ///   different orders can explode intermediate blocks.
    /// - Blocks materialize intermediate rows (`depth × 32 B` per row),
    ///   capped at [`BLOCK_ROW_CAP`] rows per descend chunk.
    pub fn solve_blocked(self) -> Vec<R> {
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            ..
        } = self;
        let variables = constraint.variables();
        let mut unbound: Vec<VariableId> = variables.into_iter().collect();
        let mut vars: Vec<VariableId> = Vec::new();
        let mut results = Vec::new();
        let mut binding = Binding::default();
        descend_blocked(
            &constraint,
            &postprocessing,
            &influences,
            &base_estimates,
            &mut vars,
            &mut unbound,
            &[],
            1,
            &mut binding,
            &mut results,
        );
        results
    }

    /// PROBE (group-by-ordering): frontier-batched solver with **per-row**
    /// variable choice.
    ///
    /// [`solve_blocked`](Self::solve_blocked) picks one next variable per
    /// level from the *first row's* estimates — cheap, but wrong for every
    /// row that would have preferred a different variable, and those rows'
    /// branches can explode (the ordering-quality loss JP diagnosed:
    /// smallest-estimate-first minimizes work AND batch size; the
    /// objectives coincide on CPU and diverge under fixed-cost batch
    /// primitives). This solver batches the *estimation* too
    /// ([`Constraint::estimate_blocked`]: one estimate column per unbound
    /// variable over the whole block), computes each row's preferred next
    /// variable (argmin of the same `(log2-magnitude, influence)` key the
    /// sequential engine sorts by), **partitions** the block by preferred
    /// variable (stable counting sort), and descends each group with *its*
    /// variable.
    ///
    /// Properties: per-branch ordering quality is restored while batches
    /// stay level-wide within groups; fragmentation is bounded by
    /// `|unbound|` groups per level (never per-branch collapse); the block
    /// invariant deepens from same-bound-*set* to same-*ordering-history*
    /// (each group's rows chose identically at every ancestor level, by
    /// construction). Degenerate case: uniform preferences ⇒ one group ⇒
    /// identical schedule to `solve_blocked` (and the block itself is
    /// borrowed, not copied).
    ///
    /// Semantics: same result **multiset** as the sequential iterator;
    /// row order may differ.
    pub fn solve_blocked_grouped(self) -> Vec<R> {
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            ..
        } = self;
        let variables = constraint.variables();
        let mut unbound: Vec<VariableId> = variables.into_iter().collect();
        let mut vars: Vec<VariableId> = Vec::new();
        let mut results = Vec::new();
        let mut binding = Binding::default();
        descend_grouped(
            &constraint,
            &postprocessing,
            &influences,
            &base_estimates,
            &mut vars,
            &mut unbound,
            &[],
            1,
            &mut binding,
            &mut results,
        );
        results
    }

    /// Create a new query.
    /// The query takes a constraint and a post-processing function as input,
    /// and returns the results of the query as a stream of values.
    /// The post-processing function returns `Option<R>`: returning `None`
    /// skips the current binding and continues the search.
    ///
    /// This method is usually not called directly, but rather through the [find!] macro,
    pub fn new(constraint: C, postprocessing: P) -> Self {
        let variables = constraint.variables();
        let influences = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint.influence(v)
            } else {
                VariableSet::new_empty()
            }
        });
        let binding = Binding::default();
        let estimates = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint
                    .estimate(v, &binding)
                    .expect("unconstrained variable in query")
            } else {
                usize::MAX
            }
        });
        // The estimates just computed ARE the empty-binding baseline —
        // freeze a copy before iteration refreshes them in place.
        let base_estimates = estimates;
        let mut unbound = ArrayVec::from_iter(variables);
        unbound.sort_unstable_by_key(|v| {
            variable_order_key(estimates[*v], base_estimates[*v], influences[*v].count())
        });

        Query {
            constraint,
            postprocessing,
            mode: Search::NextVariable,
            binding,
            influences,
            estimates,
            base_estimates,
            touched_variables: VariableSet::new_empty(),
            stack: ArrayVec::new(),
            unbound,
            values: ArrayVec::from([const { None }; 128]),
        }
    }
}

/// PROBE (order-key experiment): which variable-order key the engine
/// uses. Selected by `TRIBLES_ORDER_KEY`, read once per process.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OrderKeyMode {
    /// `(inverted log2-magnitude, influence-count)` — smallest estimate
    /// first, influence as tiebreak. The shipped engine key.
    Default,
    /// `(influence-count, inverted log2-magnitude)` — highest influence
    /// first. Measured 2026-07-09: loses decisively (blind to *being*
    /// constrained; binds hubs before neighbors shrink them).
    InfluenceFirst,
    /// Most-constrained-*relative-to-its-domain* first: primary key is the
    /// drop `estimate(v, ∅) / estimate(v, binding)` (descending, i.e. the
    /// ratio `estimate/unconstrained` ascending), current magnitude as
    /// tiebreak. Targets the "estimate DROP not estimate SIZE" signal: a
    /// var that is small *because the binding constrained it* (?e:
    /// 2.9M→14.7k) outranks a var that is small unconditionally (?g: 13).
    RatioFirst,
    /// Default `(magnitude, influence)` key, but the candidate set is
    /// restricted to variables whose estimate has actually dropped below
    /// its unconstrained (empty-binding) value — i.e. vars the bound set
    /// demonstrably constrains — falling back to the full unbound set when
    /// none qualifies (first pick, disconnected components). The cheap
    /// approximation of "don't bind a second small var that shares no
    /// constraint with the bound set".
    InfluencedOnly,
}

/// PROBE (order-key experiment): the active [`OrderKeyMode`].
/// `TRIBLES_ORDER_KEY` ∈ {`influence_first`, `ratio_first`,
/// `influenced_only`}; anything else (or unset) is [`OrderKeyMode::Default`].
pub fn order_key_mode() -> OrderKeyMode {
    static MODE: std::sync::OnceLock<OrderKeyMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(
        || match std::env::var("TRIBLES_ORDER_KEY").as_deref() {
            Ok("influence_first") => OrderKeyMode::InfluenceFirst,
            Ok("ratio_first") => OrderKeyMode::RatioFirst,
            Ok("influenced_only") => OrderKeyMode::InfluencedOnly,
            _ => OrderKeyMode::Default,
        },
    )
}

/// The engine's variable-order key. **Larger key = picked next**: every
/// site either takes `max_by_key` over the unbound set or pops the tail
/// of a key-ascending sort.
///
/// `base_estimate` is the variable's estimate against the **empty**
/// binding, computed once at [`Query::new`] (they are static — the
/// constraint tree doesn't change during a solve) and threaded to every
/// key site.
///
/// Per-mode keys (lexicographic triples):
/// - [`Default`](OrderKeyMode::Default): `(inv_mag, influence, 0)` —
///   identical ordering to the old inline `(Reverse(ilog2+1), influence)`
///   tuples.
/// - [`InfluenceFirst`](OrderKeyMode::InfluenceFirst): `(influence,
///   inv_mag, 0)`.
/// - [`RatioFirst`](OrderKeyMode::RatioFirst): `(drop, inv_mag,
///   influence)` where `drop = mag(base) − mag(estimate)` (saturating).
///   Rationale: the spec key is the raw ratio `estimate/base` ascending;
///   in the engine's ilog2-bucket style `⌊log2(base/estimate)⌋ = mag(base)
///   − mag(estimate)`, so the magnitude *difference* IS the log-bucketed
///   ratio — no division, so `estimate = 0` (mag 0, maximal drop) and
///   `base = 0` need no special-casing, and buckets stay consistent with
///   the default key's granularity. Tiebreak: current magnitude ascending
///   (per spec), then influence for determinism.
/// - [`InfluencedOnly`](OrderKeyMode::InfluencedOnly): `(dropped, inv_mag,
///   influence)` where `dropped = (estimate < base_estimate)`. The
///   candidate-set restriction is implemented *as* the lexicographic key:
///   any dropped var beats every undropped var, ties broken by the exact
///   default key — and when **no** var has dropped (first pick,
///   disconnected components) all primaries are 0 and the key degenerates
///   to precisely the default key, which is the required fallback.
#[inline]
fn variable_order_key(
    estimate: usize,
    base_estimate: usize,
    influence_count: usize,
) -> (u64, u64, u64) {
    let magnitude = estimate.checked_ilog2().map(|m| m + 1).unwrap_or(0) as u64;
    let inv_magnitude = u64::MAX - magnitude;
    let influence = influence_count as u64;
    match order_key_mode() {
        OrderKeyMode::Default => (inv_magnitude, influence, 0),
        OrderKeyMode::InfluenceFirst => (influence, inv_magnitude, 0),
        OrderKeyMode::RatioFirst => {
            let base_magnitude = base_estimate.checked_ilog2().map(|m| m + 1).unwrap_or(0) as u64;
            let drop = base_magnitude.saturating_sub(magnitude);
            (drop, inv_magnitude, influence)
        }
        OrderKeyMode::InfluencedOnly => {
            let dropped = (estimate < base_estimate) as u64;
            (dropped, inv_magnitude, influence)
        }
    }
}

/// PROBE (order-key experiment): cheap realized-variable-order trace. Off
/// by default; harnesses enable it around an untimed run. Each pick of a
/// next variable records `(depth, variable, weight)` — weight 1 per
/// branch in the sequential engine and per block in `solve_blocked`, and
/// the group's row count in `solve_blocked_grouped` (per-row preference
/// mass). [`report`](order_trace::report) aggregates counts per
/// `(depth, variable)` so "which variable did the engine actually bind at
/// each level, how often" is visible per query.
pub mod order_trace {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static COUNTS: Mutex<Vec<((usize, usize), u64)>> = Mutex::new(Vec::new());

    /// Turns recording on/off (off by default).
    pub fn set_enabled(on: bool) {
        ENABLED.store(on, Ordering::Relaxed);
    }

    pub(crate) fn enabled() -> bool {
        ENABLED.load(Ordering::Relaxed)
    }

    /// Clears all recorded picks.
    pub fn reset() {
        COUNTS.lock().unwrap().clear();
    }

    pub(crate) fn record(depth: usize, variable: usize, weight: u64) {
        let mut counts = COUNTS.lock().unwrap();
        if let Some(entry) = counts
            .iter_mut()
            .find(|((d, v), _)| *d == depth && *v == variable)
        {
            entry.1 += weight;
        } else {
            counts.push(((depth, variable), weight));
        }
    }

    /// Terse per-depth pick histogram: `d0: v2 x1; d1: v0 x13056, v3 x2`.
    pub fn report() -> String {
        use std::fmt::Write;
        let mut counts = COUNTS.lock().unwrap().clone();
        counts.sort_by_key(|&((d, _), n)| (d, std::cmp::Reverse(n)));
        let mut out = String::new();
        let mut last_depth = usize::MAX;
        for ((d, v), n) in counts {
            if d != last_depth {
                if !out.is_empty() {
                    let _ = write!(out, "; ");
                }
                let _ = write!(out, "d{d}: v{v} x{n}");
                last_depth = d;
            } else {
                let _ = write!(out, ", v{v} x{n}");
            }
        }
        out
    }
}

/// PROBE: maximum rows per block chunk in [`Query::solve_blocked`]. Bounds
/// peak memory (a chunk of D-deep rows costs `CAP × D × 32 B`) while
/// staying far above every batching break-even.
pub const BLOCK_ROW_CAP: usize = 1 << 20;

/// PROBE: effective block-row cap — [`BLOCK_ROW_CAP`] unless overridden by
/// the `TRIBLES_BLOCK_ROW_CAP` environment variable (read once; for the
/// blocked-vs-sequential convergence experiment, e.g. cap = 1 to measure
/// scalar-as-block-of-1 overhead).
pub fn block_row_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("TRIBLES_BLOCK_ROW_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&c| c > 0)
            .unwrap_or(BLOCK_ROW_CAP)
    })
}

/// PROBE (group-by-ordering): cheap instrumentation for the blocked
/// solvers. Off by default; benches call
/// [`set_enabled`](blocked_stats::set_enabled) and
/// [`reset`](blocked_stats::reset) around a measured run and print
/// [`report`](blocked_stats::report). One mutex lock per *descend level*
/// (not per row), so the enabled overhead is negligible next to the
/// propose/confirm work it describes.
pub mod blocked_stats {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;

    /// One record per `descend_*` call that expanded a frontier.
    #[derive(Clone, Debug)]
    pub struct LevelRecord {
        /// Number of variables bound on entry (search depth).
        pub depth: usize,
        /// Rows in the block this call handled.
        pub rows: usize,
        /// Per-group row counts (the v1 solver always reports one group).
        pub group_sizes: Vec<usize>,
        /// Frontier size (candidate pairs) produced per group's propose.
        pub batch_sizes: Vec<usize>,
    }

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static RECORDS: Mutex<Vec<LevelRecord>> = Mutex::new(Vec::new());
    static MATERIALIZED: AtomicU64 = AtomicU64::new(0);
    static LIVE_CELLS: AtomicU64 = AtomicU64::new(0);
    static PEAK_CELLS: AtomicU64 = AtomicU64::new(0);

    /// Turns recording on/off (off by default).
    pub fn set_enabled(on: bool) {
        ENABLED.store(on, Ordering::Relaxed);
    }

    pub(crate) fn enabled() -> bool {
        ENABLED.load(Ordering::Relaxed)
    }

    /// Clears all recorded data.
    pub fn reset() {
        RECORDS.lock().unwrap().clear();
        MATERIALIZED.store(0, Ordering::Relaxed);
        LIVE_CELLS.store(0, Ordering::Relaxed);
        PEAK_CELLS.store(0, Ordering::Relaxed);
    }

    /// PROBE (dag-frontier): row-store cells (`RawInline` = 32 B units)
    /// coming alive — intermediate blocks in the recursive solvers, bucket
    /// rows in the DAG solver. Tracks the running total and its peak so
    /// the engines' frontier memory is comparable (proposal-pair vectors
    /// are excluded in *all* engines).
    pub(crate) fn cells_add(n: usize) {
        let live = LIVE_CELLS.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
        PEAK_CELLS.fetch_max(live, Ordering::Relaxed);
    }

    /// PROBE (dag-frontier): row-store cells released.
    pub(crate) fn cells_sub(n: usize) {
        LIVE_CELLS.fetch_sub(n as u64, Ordering::Relaxed);
    }

    /// Peak live row-store cells observed since [`reset`] (32 B each).
    pub fn peak_cells() -> u64 {
        PEAK_CELLS.load(Ordering::Relaxed)
    }

    pub(crate) fn record_level(rec: LevelRecord) {
        RECORDS.lock().unwrap().push(rec);
    }

    pub(crate) fn record_materialized(rows: usize) {
        MATERIALIZED.fetch_add(rows as u64, Ordering::Relaxed);
    }

    /// Total intermediate rows materialized into child blocks — the
    /// blocked-v1 vs grouped "intermediate block size" comparison number.
    pub fn materialized_rows() -> u64 {
        MATERIALIZED.load(Ordering::Relaxed)
    }

    /// Raw per-level records, for benches that want full distributions.
    pub fn records() -> Vec<LevelRecord> {
        RECORDS.lock().unwrap().clone()
    }

    /// Terse per-depth aggregate: calls, rows, group count/sizes, batch
    /// size distribution, plus the global materialized-row total.
    pub fn report() -> String {
        use std::fmt::Write;
        let records = RECORDS.lock().unwrap();
        let mut depths: Vec<usize> = records.iter().map(|r| r.depth).collect();
        depths.sort_unstable();
        depths.dedup();
        let mut out = String::new();
        for d in depths {
            let recs: Vec<&LevelRecord> = records.iter().filter(|r| r.depth == d).collect();
            let calls = recs.len();
            let rows: usize = recs.iter().map(|r| r.rows).sum();
            let groups: usize = recs.iter().map(|r| r.group_sizes.len()).sum();
            let max_groups = recs.iter().map(|r| r.group_sizes.len()).max().unwrap_or(0);
            let mut batches: Vec<usize> =
                recs.iter().flat_map(|r| r.batch_sizes.iter().copied()).collect();
            batches.sort_unstable();
            let (bmin, bmed, bmax, btot) = if batches.is_empty() {
                (0, 0, 0, 0)
            } else {
                (
                    batches[0],
                    batches[batches.len() / 2],
                    *batches.last().unwrap(),
                    batches.iter().sum(),
                )
            };
            let _ = write!(
                out,
                "d{d}: {calls} calls / {rows} rows / {groups} groups (max {max_groups}/call), \
                 batches n={} tot={btot} [min {bmin} / med {bmed} / max {bmax}]; ",
                batches.len()
            );
        }
        let _ = write!(
            out,
            "materialized rows: {}; peak cells: {}",
            materialized_rows(),
            peak_cells()
        );
        out
    }
}

/// PROBE: recursive frontier descent for [`Query::solve_blocked`].
///
/// One call = one search level for one block chunk: pick the next
/// variable (first-row estimates, same magnitude/influence key as
/// [`Query::push_next_variable`]), expand the frontier via
/// [`Constraint::propose_blocked`] (level 0 with no bound variables uses a
/// plain [`Constraint::propose`] on the empty binding — a single branch,
/// nothing to batch), then recurse on the extended block in
/// [`BLOCK_ROW_CAP`]-row chunks.
#[allow(clippy::too_many_arguments)]
fn descend_blocked<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R>(
    constraint: &C,
    post: &P,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    vars: &mut Vec<VariableId>,
    unbound: &mut Vec<VariableId>,
    rows: &[RawInline],
    n_rows: usize,
    binding: &mut Binding,
    results: &mut Vec<R>,
) {
    if n_rows == 0 {
        return;
    }
    let stride = vars.len();
    // PROBE (fix experiment): one Binding is threaded through the whole
    // descent instead of `Binding::default()` per call — the fresh
    // construction zeroes a 4 KiB value array, which sampling showed as
    // the single largest block-of-1 overhead. Deeper recursion levels
    // mutate the shared binding, so re-establish this level's bound-set
    // (exactly `vars`; stale *values* of unbound variables are never read).
    let mut bound = VariableSet::new_empty();
    for &v in vars.iter() {
        bound.set(v);
    }
    binding.bound = bound;

    if unbound.is_empty() {
        for i in 0..n_rows {
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &rows[i * stride + k]);
            }
            if let Some(r) = post(&binding) {
                results.push(r);
            }
        }
        return;
    }

    // Choose the next variable from the first row's estimates — the same
    // (magnitude, influence-count) key the sequential engine sorts by,
    // applied once per block instead of once per branch.
    for (k, &v) in vars.iter().enumerate() {
        binding.set(v, &rows[k]);
    }
    let (ui, _) = unbound
        .iter()
        .enumerate()
        .max_by_key(|(_, &v)| {
            let estimate = constraint
                .estimate(v, &binding)
                .expect("unconstrained variable in query");
            variable_order_key(estimate, base_estimates[v], influences[v].count())
        })
        .expect("non-empty unbound");
    let variable = unbound.swap_remove(ui);
    if order_trace::enabled() {
        order_trace::record(stride, variable, 1);
    }

    // Expand the frontier: (parent row, candidate) pairs.
    let mut pairs: Vec<(u32, RawInline)> = Vec::new();
    if stride == 0 {
        let empty = Binding::default();
        let mut values = Vec::new();
        constraint.propose(variable, &empty, &mut values);
        pairs.extend(values.into_iter().map(|v| (0u32, v)));
    } else {
        constraint.propose_blocked(variable, vars, rows, &mut pairs);
    }
    if blocked_stats::enabled() {
        blocked_stats::record_level(blocked_stats::LevelRecord {
            depth: stride,
            rows: n_rows,
            group_sizes: vec![n_rows],
            batch_sizes: vec![pairs.len()],
        });
    }

    // Descend on the extended block, chunked to bound memory.
    let row_cap = block_row_cap();
    let new_stride = stride + 1;
    vars.push(variable);
    let mut next_rows: Vec<RawInline> = Vec::new();
    let mut it = pairs.into_iter().peekable();
    while it.peek().is_some() {
        next_rows.clear();
        let mut count = 0usize;
        while count < row_cap {
            let Some((row_idx, value)) = it.next() else {
                break;
            };
            let base = row_idx as usize * stride;
            next_rows.extend_from_slice(&rows[base..base + stride]);
            next_rows.push(value);
            count += 1;
        }
        debug_assert_eq!(next_rows.len(), count * new_stride);
        if blocked_stats::enabled() {
            blocked_stats::record_materialized(count);
            blocked_stats::cells_add(count * new_stride);
        }
        descend_blocked(
            constraint,
            post,
            influences,
            base_estimates,
            vars,
            unbound,
            &next_rows,
            count,
            binding,
            results,
        );
        if blocked_stats::enabled() {
            blocked_stats::cells_sub(count * new_stride);
        }
    }
    vars.pop();
    unbound.push(variable);
}

/// PROBE (group-by-ordering): recursive grouped frontier descent for
/// [`Query::solve_blocked_grouped`].
///
/// One call = one search level for one block chunk:
/// 1. **Estimate blocked** — one estimate column per unbound variable over
///    the whole block ([`Constraint::estimate_blocked`]).
/// 2. **Prefer per row** — argmax of the sequential engine's
///    [`variable_order_key`] over the row's column entries (identical
///    key, applied per row instead of per level).
/// 3. **Partition** — stable counting sort of rows by preferred variable:
///    up to `|unbound|` groups. A single group borrows the parent block
///    (no copy) — the non-skewed fast path.
/// 4. **Descend each group with its own variable** — propose_blocked over
///    the group's rows, then recurse in [`block_row_cap`]-row chunks.
///
/// STRETCH (designed, not built — the tree-becomes-DAG upgrade): rows in
/// *different* groups that bind the same variable-SET in different orders
/// reach the same lattice node and could coalesce into one block. That
/// needs (a) a frontier keyed by bound-variable-set — a map
/// `VariableSet -> pending row store` instead of this call stack, (b)
/// deepest-first scheduling (pop the largest bound-set first) to keep
/// memory DFS-like, and (c) row provenance no longer implied by the
/// parent chunk, so result emission must not assume contiguous ancestry.
/// Deliberately out of v0: cross-parent merging only pays when sibling
/// groups' orders *reconverge* (bushy joins), and the frontier map's
/// bookkeeping would obscure the grouping measurement this probe is for.
#[allow(clippy::too_many_arguments)]
fn descend_grouped<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R>(
    constraint: &C,
    post: &P,
    influences: &[VariableSet; 128],
    base_estimates: &[usize; 128],
    vars: &mut Vec<VariableId>,
    unbound: &mut Vec<VariableId>,
    rows: &[RawInline],
    n_rows: usize,
    binding: &mut Binding,
    results: &mut Vec<R>,
) {
    if n_rows == 0 {
        return;
    }
    let stride = vars.len();
    // Same shared-binding discipline as `descend_blocked`: re-establish
    // this level's bound-set; stale values of unbound variables are never
    // read.
    let mut bound = VariableSet::new_empty();
    for &v in vars.iter() {
        bound.set(v);
    }
    binding.bound = bound;

    if unbound.is_empty() {
        for i in 0..n_rows {
            for (k, &v) in vars.iter().enumerate() {
                binding.set(v, &rows[i * stride + k]);
            }
            if let Some(r) = post(binding) {
                results.push(r);
            }
        }
        return;
    }

    let row_cap = block_row_cap();

    // Level 0: a single empty branch — grouping is vacuous. Choose by the
    // global estimates and expand with a plain propose, exactly like v1.
    if stride == 0 {
        let (ui, _) = unbound
            .iter()
            .enumerate()
            .max_by_key(|(_, &v)| {
                let estimate = constraint
                    .estimate(v, binding)
                    .expect("unconstrained variable in query");
                variable_order_key(estimate, base_estimates[v], influences[v].count())
            })
            .expect("non-empty unbound");
        let variable = unbound.swap_remove(ui);
        if order_trace::enabled() {
            order_trace::record(0, variable, 1);
        }
        let mut values: Vec<RawInline> = Vec::new();
        constraint.propose(variable, binding, &mut values);
        if blocked_stats::enabled() {
            blocked_stats::record_level(blocked_stats::LevelRecord {
                depth: 0,
                rows: 1,
                group_sizes: vec![1],
                batch_sizes: vec![values.len()],
            });
        }
        vars.push(variable);
        // Child rows are single values — chunk the proposal vec directly.
        for chunk in values.chunks(row_cap) {
            if blocked_stats::enabled() {
                blocked_stats::record_materialized(chunk.len());
                blocked_stats::cells_add(chunk.len());
            }
            descend_grouped(
                constraint,
                post,
                influences,
                base_estimates,
                vars,
                unbound,
                chunk,
                chunk.len(),
                binding,
                results,
            );
            if blocked_stats::enabled() {
                blocked_stats::cells_sub(chunk.len());
            }
        }
        vars.pop();
        unbound.push(variable);
        return;
    }

    // 1. Estimate blocked: one column per unbound variable.
    let n_unbound = unbound.len();
    let mut est_cols: Vec<Vec<usize>> = Vec::with_capacity(n_unbound);
    for &v in unbound.iter() {
        let mut col = Vec::with_capacity(n_rows);
        let relevant = constraint.estimate_blocked(v, vars, rows, &mut col);
        assert!(relevant, "unconstrained variable in query");
        debug_assert_eq!(col.len(), n_rows);
        est_cols.push(col);
    }

    // 2. Per-row preferred variable: argmax of the engine's ordering key.
    let mut preferred: Vec<u32> = Vec::with_capacity(n_rows);
    let mut group_counts: Vec<usize> = vec![0; n_unbound];
    for i in 0..n_rows {
        let j = (0..n_unbound)
            .max_by_key(|&j| {
                variable_order_key(
                    est_cols[j][i],
                    base_estimates[unbound[j]],
                    influences[unbound[j]].count(),
                )
            })
            .expect("non-empty unbound");
        preferred.push(j as u32);
        group_counts[j] += 1;
    }
    drop(est_cols);

    // 3. Partition (stable counting sort by preferred variable). One
    //    group ⇒ borrow the parent block, no copy.
    let n_groups = group_counts.iter().filter(|&&c| c > 0).count();
    let mut starts: Vec<usize> = Vec::with_capacity(n_unbound);
    let mut acc = 0usize;
    for &c in &group_counts {
        starts.push(acc);
        acc += c;
    }
    let mut part: Vec<RawInline> = Vec::new();
    if n_groups > 1 {
        if blocked_stats::enabled() {
            blocked_stats::cells_add(n_rows * stride);
        }
        part = vec![[0u8; 32]; n_rows * stride];
        let mut cursors = starts.clone();
        for i in 0..n_rows {
            let j = preferred[i] as usize;
            let dst = cursors[j];
            cursors[j] += 1;
            part[dst * stride..(dst + 1) * stride]
                .copy_from_slice(&rows[i * stride..(i + 1) * stride]);
        }
    }
    drop(preferred);

    // 4. Descend each non-empty group with its preferred variable.
    // `unbound` mutates (remove/push) per group, so snapshot the
    // group-index → variable mapping first.
    let group_vars: Vec<VariableId> = unbound.clone();
    let stats = blocked_stats::enabled();
    let mut group_sizes_rec: Vec<usize> = Vec::new();
    let mut batch_sizes_rec: Vec<usize> = Vec::new();
    for (j, &variable) in group_vars.iter().enumerate() {
        let g_count = group_counts[j];
        if g_count == 0 {
            continue;
        }
        let g_rows: &[RawInline] = if n_groups == 1 {
            rows
        } else {
            &part[starts[j] * stride..(starts[j] + g_count) * stride]
        };

        if order_trace::enabled() {
            order_trace::record(stride, variable, g_count as u64);
        }
        let mut pairs: Vec<(u32, RawInline)> = Vec::new();
        constraint.propose_blocked(variable, vars, g_rows, &mut pairs);
        if stats {
            group_sizes_rec.push(g_count);
            batch_sizes_rec.push(pairs.len());
        }

        let pos = unbound
            .iter()
            .position(|&x| x == variable)
            .expect("group variable still unbound");
        unbound.swap_remove(pos);
        vars.push(variable);
        let new_stride = stride + 1;
        let mut next_rows: Vec<RawInline> = Vec::new();
        let mut it = pairs.into_iter().peekable();
        while it.peek().is_some() {
            next_rows.clear();
            let mut count = 0usize;
            while count < row_cap {
                let Some((row_idx, value)) = it.next() else {
                    break;
                };
                let base = row_idx as usize * stride;
                next_rows.extend_from_slice(&g_rows[base..base + stride]);
                next_rows.push(value);
                count += 1;
            }
            debug_assert_eq!(next_rows.len(), count * new_stride);
            if stats {
                blocked_stats::record_materialized(count);
                blocked_stats::cells_add(count * new_stride);
            }
            descend_grouped(
                constraint,
                post,
                influences,
                base_estimates,
                vars,
                unbound,
                &next_rows,
                count,
                binding,
                results,
            );
            if stats {
                blocked_stats::cells_sub(count * new_stride);
            }
        }
        vars.pop();
        unbound.push(variable);
    }
    if stats {
        if n_groups > 1 {
            blocked_stats::cells_sub(n_rows * stride);
        }
        blocked_stats::record_level(blocked_stats::LevelRecord {
            depth: stride,
            rows: n_rows,
            group_sizes: group_sizes_rec,
            batch_sizes: batch_sizes_rec,
        });
    }
}

/// PROBE (dag-frontier): counters specific to the bucket-worklist solver
/// ([`Query::solve_dag`]). Off by default; benches enable/reset around a
/// run and print [`report`](dag_stats::report). Complements
/// [`blocked_stats`] (which the DAG solver also feeds): this module holds
/// what only a worklist can have — bucket census and **merge events**,
/// i.e. rows arriving at a non-empty bucket from a different pop than the
/// one that last filed into it (the DAG's raison d'être: co-locating rows
/// whose routes through the variable lattice reconverged).
pub mod dag_stats {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static POPS: AtomicU64 = AtomicU64::new(0);
    static BUCKETS_CREATED: AtomicU64 = AtomicU64::new(0);
    static MAX_LIVE_BUCKETS: AtomicU64 = AtomicU64::new(0);
    static MERGE_EVENTS: AtomicU64 = AtomicU64::new(0);
    static MERGED_ROWS: AtomicU64 = AtomicU64::new(0);

    /// Turns recording on/off (off by default).
    pub fn set_enabled(on: bool) {
        ENABLED.store(on, Ordering::Relaxed);
    }

    pub(crate) fn enabled() -> bool {
        ENABLED.load(Ordering::Relaxed)
    }

    /// Clears all counters.
    pub fn reset() {
        POPS.store(0, Ordering::Relaxed);
        BUCKETS_CREATED.store(0, Ordering::Relaxed);
        MAX_LIVE_BUCKETS.store(0, Ordering::Relaxed);
        MERGE_EVENTS.store(0, Ordering::Relaxed);
        MERGED_ROWS.store(0, Ordering::Relaxed);
    }

    pub(crate) fn record_pop() {
        POPS.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_bucket_created(live: usize) {
        BUCKETS_CREATED.fetch_add(1, Ordering::Relaxed);
        MAX_LIVE_BUCKETS.fetch_max(live as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_merge(rows: usize) {
        MERGE_EVENTS.fetch_add(1, Ordering::Relaxed);
        MERGED_ROWS.fetch_add(rows as u64, Ordering::Relaxed);
    }

    /// Number of merge events (filings that appended to a non-empty
    /// bucket from a different pop).
    pub fn merge_events() -> u64 {
        MERGE_EVENTS.load(Ordering::Relaxed)
    }

    /// Terse counter summary.
    pub fn report() -> String {
        format!(
            "pops {} / buckets created {} / max live {} / merge events {} ({} rows merged)",
            POPS.load(Ordering::Relaxed),
            BUCKETS_CREATED.load(Ordering::Relaxed),
            MAX_LIVE_BUCKETS.load(Ordering::Relaxed),
            MERGE_EVENTS.load(Ordering::Relaxed),
            MERGED_ROWS.load(Ordering::Relaxed),
        )
    }
}

/// PROBE (dag-frontier): one pending row store in the bucket worklist.
///
/// `vars` is the bound-variable set in **ascending `VariableId` order** —
/// the canonical column layout. Canonical order is what makes merging
/// sound: rows arriving from parents that bound the same variable *set*
/// in different *orders* still agree column-for-column. (Every blocked
/// protocol method locates variables by scanning `vars`, so no constraint
/// cares about the order — but rows sharing one store must share one
/// layout.)
struct DagBucket {
    /// Bound-variable set (`vars` as a bitset) — the bucket key.
    set: VariableSet,
    /// Bound variables, ascending — the column layout.
    vars: Vec<VariableId>,
    /// Row store: `rows.len() / vars.len()` rows of `vars.len()` values.
    rows: Vec<RawInline>,
    /// Pop id of the last filing (merge-event detection only).
    writer: u64,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Query<C, P, R> {
    /// PROBE (dag-frontier): bucket-worklist solver — the tree-becomes-DAG
    /// upgrade of [`solve_blocked_grouped`](Self::solve_blocked_grouped).
    ///
    /// Evaluation state is a worklist of **buckets keyed by
    /// bound-variable-set** instead of a recursion stack. Pop a bucket,
    /// partition its rows by preferred next variable (the same
    /// estimate-blocked + per-row-argmax logic as the grouped solver), run
    /// one batched propose+confirm per (group, variable), then **file** the
    /// extended rows into the bucket keyed by `bound ∪ {v}` — creating it
    /// or **appending** to it. The append is the whole point: rows whose
    /// routes through the variable lattice bound the same set in different
    /// orders *reconverge* into one row store and every downstream batch is
    /// correspondingly fatter. Rows are affine — moved on pop, never
    /// copied between buckets (prefix duplication on fan-out is
    /// materialization, not sharing). Full-bound buckets emit.
    ///
    /// Scheduling: **deepest-first among ready buckets**, where a bucket
    /// is *ready* iff no live bucket's set is a strict subset of its set.
    /// The gate is exact — rows only ever gain variables, so any future
    /// contributor to bucket `S` is currently a strict subset of `S`;
    /// once none exists, `S` is complete and safe to pop. Without the
    /// gate, strict deepest-first pops a reconvergent bucket after its
    /// *first* parent files (children out-deepen every pending sibling
    /// route), so cross-parent rows never co-locate and the merge is dead
    /// machinery. The price is that reconvergent buckets are *held* until
    /// all their feeders drain — on a densely reconverging lattice the
    /// schedule degrades toward breadth-first and frontier memory grows
    /// accordingly (measured, not hidden). Where routes never reconverge
    /// the gate never blocks and the schedule is DFS-like.
    ///
    /// Semantics: same result **multiset** as the sequential iterator
    /// (each row still value-partitions its region of the search space;
    /// merging is co-location only). Row order differs.
    pub fn solve_dag(self) -> Vec<R> {
        self.solve_dag_impl(true)
    }

    /// PROBE (dag-frontier): [`solve_dag`](Self::solve_dag) with merging
    /// **disabled** — every filing creates a fresh bucket (lineage-keyed),
    /// so reconvergent routes stay in separate row stores. Identical
    /// scheduling rule and machinery; this is the control that isolates
    /// what the merge itself buys (batch re-fattening) from what the
    /// worklist restructuring costs.
    pub fn solve_dag_unmerged(self) -> Vec<R> {
        self.solve_dag_impl(false)
    }

    fn solve_dag_impl(self, merge: bool) -> Vec<R> {
        let Query {
            constraint,
            postprocessing,
            influences,
            base_estimates,
            ..
        } = self;
        let full = constraint.variables();
        let row_cap = block_row_cap();
        let stats = blocked_stats::enabled();
        let dstats = dag_stats::enabled();
        let mut results: Vec<R> = Vec::new();
        let mut binding = Binding::default();

        // Seed: the empty bound-set with one virtual zero-width row.
        let mut buckets: Vec<DagBucket> = vec![DagBucket {
            set: VariableSet::new_empty(),
            vars: Vec::new(),
            rows: Vec::new(),
            writer: 0,
        }];
        let mut pop_id: u64 = 0;

        while !buckets.is_empty() {
            // Pop the deepest READY bucket. Ready: no live bucket is a
            // strict subset (nothing can ever file into it again). The
            // minimal elements of the live poset are always ready, so a
            // pop always exists. O(buckets²) scan — bucket count is
            // bounded by the number of distinct bound-sets in flight
            // (lattice antichains in practice: a handful).
            let mut best: Option<(usize, usize)> = None;
            for (i, b) in buckets.iter().enumerate() {
                let ready = buckets.iter().enumerate().all(|(j, o)| {
                    j == i || !(o.set != b.set && o.set.is_subset_of(&b.set))
                });
                if !ready {
                    continue;
                }
                let depth = b.set.count();
                if best.map_or(true, |(_, bd)| depth > bd) {
                    best = Some((i, depth));
                }
            }
            let (idx, _) = best.expect("a minimal live bucket is always ready");
            let bucket = buckets.swap_remove(idx);
            pop_id += 1;
            if dstats {
                dag_stats::record_pop();
            }
            let stride = bucket.vars.len();
            let n_rows = if stride == 0 {
                1 // the virtual seed row
            } else {
                bucket.rows.len() / stride
            };
            binding.bound = bucket.set;

            // Full-bound bucket: emit.
            if bucket.set == full {
                if stride == 0 {
                    if let Some(r) = postprocessing(&binding) {
                        results.push(r);
                    }
                } else {
                    for i in 0..n_rows {
                        for (k, &v) in bucket.vars.iter().enumerate() {
                            binding.set(v, &bucket.rows[i * stride + k]);
                        }
                        if let Some(r) = postprocessing(&binding) {
                            results.push(r);
                        }
                    }
                }
                if stats {
                    blocked_stats::cells_sub(bucket.rows.len());
                }
                continue;
            }

            let unbound: Vec<VariableId> = full.subtract(bucket.set).into_iter().collect();
            let n_unbound = unbound.len();

            // Files one group's `(row, value)` pairs into the child bucket
            // for `bucket.set ∪ {variable}`, creating or appending.
            let file =
                |buckets: &mut Vec<DagBucket>,
                 variable: VariableId,
                 g_rows: &[RawInline],
                 pairs: Vec<(u32, RawInline)>| {
                    if pairs.is_empty() {
                        return;
                    }
                    let mut child_set = bucket.set;
                    child_set.set(variable);
                    // Canonical layout: insert the new value at the
                    // variable's ascending position.
                    let vpos = bucket
                        .vars
                        .iter()
                        .position(|&x| x > variable)
                        .unwrap_or(stride);
                    let child_stride = stride + 1;
                    let target = if merge {
                        buckets.iter().position(|b| b.set == child_set)
                    } else {
                        None
                    };
                    let target = match target {
                        Some(t) => {
                            if dstats && !buckets[t].rows.is_empty() && buckets[t].writer != pop_id
                            {
                                dag_stats::record_merge(pairs.len());
                            }
                            buckets[t].writer = pop_id;
                            t
                        }
                        None => {
                            let mut child_vars = bucket.vars.clone();
                            child_vars.insert(vpos, variable);
                            buckets.push(DagBucket {
                                set: child_set,
                                vars: child_vars,
                                rows: Vec::new(),
                                writer: pop_id,
                            });
                            if dstats {
                                dag_stats::record_bucket_created(buckets.len());
                            }
                            buckets.len() - 1
                        }
                    };
                    let store = &mut buckets[target].rows;
                    store.reserve(pairs.len() * child_stride);
                    let mut filed = 0usize;
                    for (row_idx, value) in pairs {
                        let base = row_idx as usize * stride;
                        store.extend_from_slice(&g_rows[base..base + vpos]);
                        store.push(value);
                        store.extend_from_slice(&g_rows[base + vpos..base + stride]);
                        filed += 1;
                    }
                    if stats {
                        blocked_stats::record_materialized(filed);
                        blocked_stats::cells_add(filed * child_stride);
                    }
                };

            // Seed level: a single empty branch — plain propose.
            if stride == 0 {
                let (ui, _) = unbound
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, &v)| {
                        let estimate = constraint
                            .estimate(v, &binding)
                            .expect("unconstrained variable in query");
                        variable_order_key(estimate, base_estimates[v], influences[v].count())
                    })
                    .expect("non-empty unbound");
                let variable = unbound[ui];
                if order_trace::enabled() {
                    order_trace::record(0, variable, 1);
                }
                let mut values: Vec<RawInline> = Vec::new();
                constraint.propose(variable, &binding, &mut values);
                if stats {
                    blocked_stats::record_level(blocked_stats::LevelRecord {
                        depth: 0,
                        rows: 1,
                        group_sizes: vec![1],
                        batch_sizes: vec![values.len()],
                    });
                }
                let pairs: Vec<(u32, RawInline)> =
                    values.into_iter().map(|v| (0u32, v)).collect();
                file(&mut buckets, variable, &[], pairs);
                continue;
            }

            // Process the bucket's rows in cap-sized chunks: estimate,
            // prefer, partition, one batched propose per group, file.
            let mut group_sizes_rec: Vec<usize> = Vec::new();
            let mut batch_sizes_rec: Vec<usize> = Vec::new();
            for chunk in bucket.rows.chunks(row_cap * stride) {
                let c_rows = chunk.len() / stride;

                // 1. Estimate columns.
                let mut est_cols: Vec<Vec<usize>> = Vec::with_capacity(n_unbound);
                for &v in unbound.iter() {
                    let mut col = Vec::with_capacity(c_rows);
                    let relevant = constraint.estimate_blocked(v, &bucket.vars, chunk, &mut col);
                    assert!(relevant, "unconstrained variable in query");
                    debug_assert_eq!(col.len(), c_rows);
                    est_cols.push(col);
                }

                // 2. Per-row preferred variable.
                let mut preferred: Vec<u32> = Vec::with_capacity(c_rows);
                let mut group_counts: Vec<usize> = vec![0; n_unbound];
                for i in 0..c_rows {
                    let j = (0..n_unbound)
                        .max_by_key(|&j| {
                            variable_order_key(
                                est_cols[j][i],
                                base_estimates[unbound[j]],
                                influences[unbound[j]].count(),
                            )
                        })
                        .expect("non-empty unbound");
                    preferred.push(j as u32);
                    group_counts[j] += 1;
                }
                drop(est_cols);

                // 3. Partition (stable counting sort); single group
                //    borrows the chunk.
                let n_groups = group_counts.iter().filter(|&&c| c > 0).count();
                let mut starts: Vec<usize> = Vec::with_capacity(n_unbound);
                let mut acc = 0usize;
                for &c in &group_counts {
                    starts.push(acc);
                    acc += c;
                }
                let mut part: Vec<RawInline> = Vec::new();
                if n_groups > 1 {
                    if stats {
                        blocked_stats::cells_add(chunk.len());
                    }
                    part = vec![[0u8; 32]; chunk.len()];
                    let mut cursors = starts.clone();
                    for i in 0..c_rows {
                        let j = preferred[i] as usize;
                        let dst = cursors[j];
                        cursors[j] += 1;
                        part[dst * stride..(dst + 1) * stride]
                            .copy_from_slice(&chunk[i * stride..(i + 1) * stride]);
                    }
                }
                drop(preferred);

                // 4. One batched propose per group; file into buckets.
                for (j, &variable) in unbound.iter().enumerate() {
                    let g_count = group_counts[j];
                    if g_count == 0 {
                        continue;
                    }
                    let g_rows: &[RawInline] = if n_groups == 1 {
                        chunk
                    } else {
                        &part[starts[j] * stride..(starts[j] + g_count) * stride]
                    };
                    if order_trace::enabled() {
                        order_trace::record(stride, variable, g_count as u64);
                    }
                    let mut pairs: Vec<(u32, RawInline)> = Vec::new();
                    constraint.propose_blocked(variable, &bucket.vars, g_rows, &mut pairs);
                    if stats {
                        group_sizes_rec.push(g_count);
                        batch_sizes_rec.push(pairs.len());
                    }
                    file(&mut buckets, variable, g_rows, pairs);
                }
                if stats && n_groups > 1 {
                    blocked_stats::cells_sub(chunk.len());
                }
            }
            if stats {
                blocked_stats::record_level(blocked_stats::LevelRecord {
                    depth: stride,
                    rows: n_rows,
                    group_sizes: group_sizes_rec,
                    batch_sizes: batch_sizes_rec,
                });
                blocked_stats::cells_sub(bucket.rows.len());
            }
        }
        results
    }
}

/// The search mode of the query engine.
/// The query engine uses a depth-first search to find solutions to the query,
/// proposing values for the variables and backtracking when it reaches a dead end.
/// The search mode is used to keep track of the current state of the search.
/// The search mode can be one of the following:
/// - `NextVariable` - The query engine is looking for the next variable to assign a value to.
/// - `NextValue` - The query engine is looking for the next value to assign to a variable.
/// - `Backtrack` - The query engine is backtracking to try a different value for a variable.
/// - `Done` - The query engine has finished the search and there are no more results.
#[derive(Copy, Clone, Debug)]
enum Search {
    NextVariable,
    NextValue,
    Backtrack,
    Done,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &self.mode {
                Search::NextVariable => {
                    self.mode = Search::NextValue;
                    if self.unbound.is_empty() {
                        if let Some(result) = (self.postprocessing)(&self.binding) {
                            return Some(result);
                        }
                        // Post-processing rejected this binding; continue
                        // searching (mode is already NextValue).
                        continue;
                    }
                    self.push_next_variable();
                }
                Search::NextValue => {
                    if let Some(&variable) = self.stack.last() {
                        if let Some(assignment) = self.values[variable]
                            .as_mut()
                            .expect("values should be initialized")
                            .pop()
                        {
                            self.binding.set(variable, &assignment);
                            self.touched_variables.set(variable);
                            self.mode = Search::NextVariable;
                        } else {
                            self.mode = Search::Backtrack;
                        }
                    } else {
                        self.mode = Search::Done;
                        return None;
                    }
                }
                Search::Backtrack => {
                    if let Some(variable) = self.stack.pop() {
                        self.binding.unset(variable);
                        // Note that we did not update estiamtes for the unbound variables
                        // as we are backtracking, so the estimates are still valid.
                        // Since we choose this variable before, we know that it would
                        // still go last in the unbound list.
                        self.unbound.push(variable);

                        // However, we need to update the touched variables,
                        // as we are backtracking and the variable is no longer bound.
                        // We're essentially restoring the estimate of the touched variables
                        // to the state before we bound this variable.
                        self.touched_variables.set(variable);
                        self.mode = Search::NextValue;
                    } else {
                        self.mode = Search::Done;
                        return None;
                    }
                }
                Search::Done => {
                    return None;
                }
            }
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> fmt::Debug for Query<C, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("constraint", &std::any::type_name::<C>())
            .field("mode", &self.mode)
            .field("binding", &self.binding)
            .field("stack", &self.stack)
            .field("unbound", &self.unbound)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Parallel execution via rayon.
//
// `Query` implements `IntoParallelIterator` with `Iter = QueryParIter`.
// `QueryParIter` is a separate wrapper type implementing `ParallelIterator`
// + `UnindexedProducer`, distinct from `Query` itself to avoid method-name
// ambiguity between `Iterator` and `ParallelIterator` — methods like
// `.count()`, `.collect()`, `.map()` exist on both.
//
// Usage: `find!(...).into_par_iter().map(...).collect::<Vec<_>>()`.
//
// The producer's `split` uses the "split-or-descend" rule: while the current
// top-of-stack has a single remaining proposal, bind it and descend one level;
// when the top has ≥2 remaining, bisect them between two sub-queries. This
// keeps the invariant that every non-top stack level has zero remaining
// proposals, so backtracking out of a sub-search unwinds cleanly to done
// without any re-enumeration across clones.
//
// `fold_with` is the terminal leaf: it just drives the existing sequential
// `Iterator::next()` and feeds results into the folder. No duplicated
// execution logic.
// ---------------------------------------------------------------------------

#[cfg(feature = "parallel")]
pub use parallel::QueryParIter;

#[cfg(feature = "parallel")]
mod parallel {
    use super::*;
    use rayon::iter::plumbing::{
        bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer,
    };
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    /// Parallel iterator over the results of a [`Query`]. Obtained via
    /// [`IntoParallelIterator::into_par_iter`] on a `Query`.
    ///
    /// Drives rayon's work-stealing scheduler through an `UnindexedProducer`
    /// impl on the underlying query state. The sequential `Iterator::next`
    /// on `Query` is reused as the fold leaf — parallel execution is purely
    /// additional, no duplicated engine logic.
    ///
    /// The inner query is stored in a [`Box`] so rayon's work-stealing
    /// `split` (which clones the producer) doesn't memcpy ~15 KB of query
    /// state on every fork — just a Box pointer copy, with the heap alloc
    /// paid only by the child.
    ///
    /// `split_budget` bounds the number of splits this sub-producer will
    /// perform. Rayon's default `Splitter` *resets* its budget on every
    /// stolen task, so on a busy thread pool the split tree could grow
    /// unboundedly deep — the Query always has more proposals to bisect.
    /// A bounded per-producer budget (`num_threads²`) caps the split tree
    /// at ~N² leaves — enough for each worker to have roughly N chunks to
    /// rebalance via stealing — regardless of stealing pressure.
    pub struct QueryParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<Query<C, P, R>>,
        split_budget: usize,
    }

    impl<'a, C, P, R> IntoParallelIterator for Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = QueryParIter<C, P, R>;

        fn into_par_iter(self) -> Self::Iter {
            // num_threads² chunks: intuition is "every worker has one spare
            // chunk for every other worker," giving N²/N = N chunks apiece
            // for rebalancing. log₂(N²) = 2·log₂(N), so depth stays modest
            // (8 on a 16-thread box, 10 on a 32-thread) — well below any
            // stack concern.
            let n = rayon::current_num_threads();
            let split_budget = n.saturating_mul(n).max(2);
            QueryParIter {
                inner: Box::new(self),
                split_budget,
            }
        }
    }

    impl<'a, C, P, R> UnindexedProducer for QueryParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        /// Advance the Query's state machine until either the current
        /// top-of-stack has ≥2 remaining proposals (bisect, return a
        /// right half) or the sub-query is exhausted (return `None`,
        /// leaving `self` as a leaf that `fold_with` will fold
        /// sequentially). Single-value levels are descended through —
        /// see the module doc comment for why this preserves correctness
        /// without re-enumeration.
        fn split(mut self) -> (Self, Option<Self>) {
            if self.split_budget == 0 {
                return (self, None);
            }
            self.split_budget -= 1;
            let q = &mut *self.inner;
            loop {
                // Advance the state machine until we're in NextValue with
                // a populated top — the only state where split-or-descend
                // makes sense.
                while !matches!(q.mode, Search::NextValue) {
                    match q.mode {
                        Search::NextVariable => {
                            q.mode = Search::NextValue;
                            if q.unbound.is_empty() {
                                // All variables bound. Leaf — fold_with
                                // will drive sequential `next()` to yield
                                // the one postprocessed result.
                                q.mode = Search::NextVariable;
                                return (self, None);
                            }
                            q.push_next_variable();
                        }
                        Search::Backtrack => {
                            if let Some(variable) = q.stack.pop() {
                                q.binding.unset(variable);
                                q.unbound.push(variable);
                                q.touched_variables.set(variable);
                                q.mode = Search::NextValue;
                            } else {
                                q.mode = Search::Done;
                                return (self, None);
                            }
                        }
                        Search::Done => return (self, None),
                        Search::NextValue => unreachable!(),
                    }
                }

                // mode == NextValue. Inspect top-of-stack's remaining
                // proposals.
                let Some(&top) = q.stack.last() else {
                    return (self, None);
                };
                let top_len = q.values[top].as_ref().map_or(0, |v| v.len());
                match top_len {
                    0 => q.mode = Search::Backtrack,
                    1 => {
                        // Descend: pop the single value, bind it,
                        // transition to NextVariable so the outer loop
                        // runs propose.
                        let assignment = q.values[top].as_mut().unwrap().pop().unwrap();
                        q.binding.set(top, &assignment);
                        q.touched_variables.set(top);
                        q.mode = Search::NextVariable;
                    }
                    _ => {
                        // Bisect the remaining proposals; clone the rest
                        // of the query state into the right half. Clone
                        // cost is one ~15 KB arraycopy per
                        // rayon-requested split — rayon only asks under
                        // stealing pressure.
                        let vals = q.values[top].as_mut().unwrap();
                        let mid = vals.len() / 2;
                        let right_vals: Vec<RawInline> = vals.drain(mid..).collect();
                        let mut right = q.clone();
                        right.values[top] = Some(right_vals);

                        let left_budget = self.split_budget / 2;
                        let right_budget = self.split_budget - left_budget;
                        self.split_budget = left_budget;
                        return (
                            self,
                            Some(QueryParIter {
                                inner: Box::new(right),
                                split_budget: right_budget,
                            }),
                        );
                    }
                }
            }
        }

        fn fold_with<F: Folder<R>>(self, mut folder: F) -> F {
            let QueryParIter { inner: mut q, .. } = self;
            while !folder.full() {
                match q.next() {
                    Some(item) => folder = folder.consume(item),
                    None => break,
                }
            }
            folder
        }
    }

    impl<'a, C, P, R> ParallelIterator for QueryParIter<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        fn drive_unindexed<Con>(self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            bridge_unindexed(self, consumer)
        }
    }

}

/// Iterate over query results, converting each variable via
/// [`TryFromInline`](crate::inline::TryFromInline).
///
/// The macro takes two arguments: a tuple of variables with optional type
/// annotations, and a constraint expression. It injects a `__local_find_context!`
/// macro that provides the variable context to nested query macros like
/// [`pattern!`](crate::macros::pattern) and [`ignore!`](crate::ignore).
///
/// # Variable syntax
///
/// | Syntax | Meaning |
/// |--------|---------|
/// | `name` | inferred type, filter on conversion failure |
/// | `name: Type` | explicit type, filter on conversion failure |
/// | `name?` | inferred type, yield `Result<T, E>` (no filter) |
/// | `name: Type?` | explicit type, yield `Result<T, E>` (no filter) |
///
/// The unit form `find!((), constraint)` projects no variables and yields one
/// `()` for every matching row. This is useful when you only care about
/// existence, counting, or composing the query without returning values.
///
/// **Filter semantics (default):** when a variable's conversion fails the
/// entire row is silently skipped — like a constraint that doesn't match.
/// For types whose `TryFromInline::Error = Infallible` the error branch is
/// dead code, so no rows can ever be accidentally filtered.
///
/// **`?` pass-through:** appending `?` to a variable makes it yield
/// `Result<T, E>` directly. Both `Ok` and `Err` values pass through with
/// no filtering, matching Rust's `?` semantics of "bubble the error to the
/// caller."
///
/// # Examples
///
/// ```
/// # use triblespace_core::prelude::*;
/// # use triblespace_core::prelude::inlineencodings::ShortString;
/// // Filter semantics — rows where conversion fails are skipped:
/// let results = find!((x: Inline<ShortString>), x.is("foo".to_inline())).collect::<Vec<_>>();
/// ```
#[macro_export]
macro_rules! find {
    ($($tokens:tt)*) => {
        {
            #[allow(unused_mut, unused_variables)]
            let mut ctx = $crate::query::VariableContext::new();

            macro_rules! __local_find_context {
                () => { &mut ctx }
            }

            $crate::macros::__find_impl!($crate, ctx, $($tokens)*)
        }
    };
}
/// Re-export of the [`find!`] macro.
pub use find;

/// Returns `true` when a query produces at least one row.
///
/// This is equivalent to calling `find!(...).next().is_some()`, but reads more
/// directly for existence checks.
///
/// # Forms
///
/// - `exists!(constraint)` checks a pure constraint with no projected
///   variables.
/// - `exists!((vars...), constraint)` uses the same variable/conversion syntax
///   as [`find!`] before checking whether any row survives projection.
///
/// ```rust,ignore
/// exists!(pattern!(&kb, [{ ?person @ social::name: "Alice" }]))
/// ```
///
/// ```rust,ignore
/// exists!(
///     (name: Inline<_>),
///     pattern!(&kb, [{ ?person @ social::name: ?name }])
/// )
/// ```
#[macro_export]
macro_rules! exists {
    (($($vars:tt)*), $Constraint:expr) => {
        $crate::query::find!(($($vars)*), $Constraint).next().is_some()
    };
    ($Constraint:expr) => {
        $crate::query::find!((), $Constraint).next().is_some()
    };
}
/// Re-export of the [`exists!`] macro.
pub use exists;

/// Introduces one or more temporary query variables for a nested constraint.
///
/// `temp!` is only meaningful inside macros that provide a local query context,
/// such as [`find!`], [`exists!`], or macros expanded from them like
/// [`pattern!`](crate::macros::pattern). Each identifier becomes a fresh query
/// variable that is scoped to the wrapped body.
///
/// ```rust,ignore
/// find!(
///     (person: Inline<_>),
///     temp!((friend), and!(
///         pattern!(&kb, [{ ?person @ social::friend: ?friend }]),
///         pattern!(&kb, [{ ?friend @ social::name: "Bob" }])
///     ))
/// )
/// ```
#[macro_export]
macro_rules! temp {
    (($Var:ident), $body:expr) => {{
        let $Var = __local_find_context!().next_variable();
        $body
    }};
    (($Var:ident,), $body:expr) => {
        $crate::temp!(($Var), $body)
    };
    (($Var:ident, $($rest:ident),+ $(,)?), $body:expr) => {{
        $crate::temp!(
            ($Var),
            $crate::temp!(($($rest),+), $body)
        )
    }};
}
/// Re-export of the [`temp!`] macro.
pub use temp;

#[cfg(test)]
mod tests {
    use inlineencodings::ShortString;

    use crate::ignore;
    use crate::prelude::inlineencodings::*;
    use crate::prelude::*;

    use crate::examples::literature;

    use fake::faker::lorem::en::Sentence;
    use fake::faker::lorem::en::Words;
    use fake::faker::name::raw::*;
    use fake::locales::*;
    use fake::Fake;

    use std::collections::HashSet;

    use super::*;

    pub mod knights {
        use crate::prelude::*;

        attributes! {
            "8143F46E812E88C4544E7094080EC523" as loves: inlineencodings::GenId;
            "D6E0F2A6E5214E1330565B4D4138E55C" as name: inlineencodings::ShortString;
        }
    }

    mod social {
        use crate::prelude::*;

        attributes! {
            "A19EC1D9DD534BA9896223A457A6B9C9" as name: inlineencodings::ShortString;
            "C21DE0AA5BA3446AB886C9640BA60244" as friend: inlineencodings::GenId;
        }
    }

    #[test]
    fn and_set() {
        let mut books = HashSet::<String>::new();
        let mut movies = HashSet::<Inline<ShortString>>::new();

        books.insert("LOTR".to_string());
        books.insert("Dragonrider".to_string());
        books.insert("Highlander".to_string());

        movies.insert("LOTR".to_inline());
        movies.insert("Highlander".to_inline());

        let inter: Vec<_> =
            find!((a: Inline<ShortString>), and!(books.has(a), movies.has(a))).collect();

        assert_eq!(inter.len(), 2);

        let cross: Vec<_> =
            find!((a: Inline<ShortString>, b: Inline<ShortString>), and!(books.has(a), movies.has(b))).collect();

        assert_eq!(cross.len(), 6);

        let one: Vec<_> = find!((a: Inline<ShortString>),
            and!(books.has(a), a.is(ShortString::inline_from("LOTR")))
        )
        .collect();

        assert_eq!(one.len(), 1);
    }

    #[test]
    fn pattern() {
        let mut kb = TribleSet::new();
        (0..1000).for_each(|_| {
            let author = fucid();
            let book = fucid();
            kb += entity! { &author @
               literature::firstname: FirstName(EN).fake::<String>(),
               literature::lastname: LastName(EN).fake::<String>(),
            };
            kb += entity! { &book @
               literature::author: &author,
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::quote: Sentence(5..25).fake::<String>().to_blob().get_handle()
            };
        });

        let author = fucid();
        let book = fucid();
        kb += entity! { &author @
           literature::firstname: "Frank",
           literature::lastname: "Herbert",
        };
        kb += entity! { &book @
           literature::author: &author,
           literature::title: "Dune",
           literature::quote: "I must not fear. Fear is the \
                   mind-killer. Fear is the little-death that brings total \
                   obliteration. I will face my fear. I will permit it to \
                   pass over me and through me. And when it has gone past I \
                   will turn the inner eye to see its path. Where the fear \
                   has gone there will be nothing. Only I will remain.".to_blob().get_handle()
        };

        (0..100).for_each(|_| {
            let author = fucid();
            let book = fucid();
            kb += entity! { &author @
               literature::firstname: "Fake",
               literature::lastname: "Herbert",
            };
            kb += entity! { &book @
               literature::author: &author,
               literature::title: Words(1..3).fake::<Vec<String>>().join(" "),
               literature::quote: Sentence(5..25).fake::<String>().to_blob().get_handle()
            };
        });

        let r: Vec<_> = find!(
        (author: Inline<_>, book: Inline<_>, title: Inline<_>, quote: Inline<_>),
        pattern!(&kb, [
        {?author @
            literature::firstname: "Frank",
            literature::lastname: "Herbert"},
        {?book @
          literature::author: ?author,
          literature::title: ?title,
          literature::quote: ?quote
        }]))
        .collect();

        assert_eq!(1, r.len())
    }

    #[test]
    fn constant() {
        let r: Vec<_> = find! {
            (string: Inline<_>, number: Inline<_>),
            and!(
                string.is(ShortString::inline_from("Hello World!")),
                number.is(I256BE::inline_from(42))
            )
        }
        .collect();

        assert_eq!(1, r.len())
    }

    #[test]
    fn exists_true() {
        assert!(exists!((a: Inline<_>), a.is(I256BE::inline_from(42))));
    }

    #[test]
    fn exists_false() {
        assert!(!exists!(
            (a: Inline<_>),
            and!(a.is(I256BE::inline_from(1)), a.is(I256BE::inline_from(2)))
        ));
    }

    #[test]
    fn exists_no_variables_true() {
        let mut ctx = VariableContext::new();
        let a = ctx.next_variable::<I256BE>();
        assert!(exists!(a.is(I256BE::inline_from(42))));
    }

    #[test]
    fn find_no_variables_yields_unit() {
        let mut ctx = VariableContext::new();
        let a = ctx.next_variable::<I256BE>();
        let rows: Vec<()> = find!((), a.is(I256BE::inline_from(42))).collect();
        assert_eq!(rows, vec![()]);
    }

    #[test]
    fn temp_variables_span_patterns() {
        use social::*;

        let mut kb = TribleSet::new();
        let alice = fucid();
        let bob = fucid();

        kb += entity! { &alice @ name: "Alice", friend: &bob };
        kb += entity! { &bob @ name: "Bob" };

        let matches: Vec<_> = find!(
            (person_name: Inline<_>),
            temp!((mutual_friend),
                and!(
                    pattern!(&kb, [{ _?person @ name: ?person_name, friend: ?mutual_friend }]),
                    pattern!(&kb, [{ ?mutual_friend @ name: "Bob" }])
                )
            )
        )
        .collect();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0.try_from_inline::<&str>().unwrap(), "Alice");
    }

    #[test]
    fn ignore_skips_variables() {
        let results: Vec<_> = find!(
            (x: Inline<_>),
            ignore!((y), and!(x.is(I256BE::inline_from(1)), y.is(I256BE::inline_from(2))))
        )
        .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, I256BE::inline_from(1));
    }

    #[test]
    fn estimate_override_debug_order() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let mut ctx = VariableContext::new();
        let a = ctx.next_variable::<ShortString>();
        let b = ctx.next_variable::<ShortString>();

        let base = and!(
            a.is(ShortString::inline_from("A")),
            b.is(ShortString::inline_from("B"))
        );

        let mut wrapper = crate::debug::query::EstimateOverrideConstraint::new(base);
        wrapper.set_estimate(a.index, 10);
        wrapper.set_estimate(b.index, 1);

        let record = Rc::new(RefCell::new(Vec::new()));
        let debug = crate::debug::query::DebugConstraint::new(wrapper, Rc::clone(&record));

        let q: Query<_, _, _> = Query::new(debug, |_| Some(()));
        let r: Vec<_> = q.collect();
        assert_eq!(1, r.len());
        assert_eq!(&*record.borrow(), &[b.index, a.index]);
    }
}
