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
/// Shared finite continuation for immutable, ordered single-variable sources.
#[doc(hidden)]
/// [`KeysConstraint`](hashmapconstraint::KeysConstraint) — constrains a variable to HashMap keys.
pub mod hashmapconstraint;
/// [`SetConstraint`](hashsetconstraint::SetConstraint) — constrains a variable to HashSet members.
pub mod hashsetconstraint;
/// [`IntersectionConstraint`](intersectionconstraint::IntersectionConstraint) — logical AND.
pub mod intersectionconstraint;
/// [`PatchValueConstraint`](patchconstraint::PatchValueConstraint) and [`PatchIdConstraint`](patchconstraint::PatchIdConstraint) — constrains variables to PATCH entries.
pub mod patchconstraint;
#[doc(hidden)]
/// [`InlineRange`](rangeconstraint::InlineRange) — restricts a variable to a byte-lexicographic range.
pub mod rangeconstraint;
/// [`SortedSliceConstraint`](sortedsliceconstraint::SortedSliceConstraint) — constrains a variable to values in a sorted slice (binary search confirm).
pub mod sortedsliceconstraint;
/// [`UnionConstraint`](unionconstraint::UnionConstraint) — logical OR.
pub mod unionconstraint;
mod variableset;

use std::cmp::Reverse;
use std::fmt;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "parallel")]
use std::sync::Mutex;

use arrayvec::ArrayVec;
use constantconstraint::*;

use crate::inline::encodings::genid::GenId;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;

/// Re-export of [`VariableSet`](variableset::VariableSet).
pub use variableset::VariableSet;

impl<T: InlineEncoding> Term<T> {
    /// Erases the schema type, yielding the runtime representation
    /// constraint implementations store.
    pub fn erase(self) -> RawTerm {
        match self {
            Term::Var(v) => RawTerm::Var(v.index),
            Term::Const(c) => RawTerm::Const(c.raw),
        }
    }
}

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
    /// Each position takes a [`Term`]: either a [`Variable`] to solve for
    /// or a constant [`Inline`] value baked into the constraint (a constant
    /// position behaves exactly like a variable the engine has already
    /// bound, but never appears in the constraint's [`VariableSet`]).
    /// The schemas of the entities and attributes are always [GenId], while the value
    /// schema can be any type implementing [InlineEncoding] and is specified as a type parameter.
    ///
    /// This method is usually not called directly, but rather through typed query language
    /// macros like [pattern!][crate::macros::pattern], which pass attribute
    /// constants and literal values as constant terms.
    fn pattern<'a, V: InlineEncoding>(
        &'a self,
        e: impl Into<Term<GenId>>,
        a: impl Into<Term<GenId>>,
        v: impl Into<Term<V>>,
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
    pub fn extract<'b>(self, binding: &Binding<'b>) -> &'b Inline<T> {
        let raw = binding.get(self.index).unwrap_or_else(|| {
            panic!(
                "query variable (idx {}) was never bound before projection. This usually means the variable was projected in `find!` but never appeared in any constraint. If you intended a pure existence query, use `find!((), ...)` or `exists!(constraint)`.",
                self.index
            )
        });
        Inline::as_transmute_raw(raw)
    }
}

/// One position of a triple pattern: either a [`Variable`] the engine
/// solves for, or a constant [`Inline`] value pinned at construction.
///
/// Constants are how the macro layer expresses attribute constants and
/// literal values without allocating hidden helper variables. A constant
/// position behaves exactly like a variable that is already bound — the
/// backends' bound/unbound dispatch handles it with no extra cases — but
/// it never appears in the constraint's [`VariableSet`]. This keeps the
/// visible variable set of a `pattern!` equal to the query variables the
/// user actually wrote, which is what makes
/// [`or!`](crate::or) over patterns with different attributes or literals
/// well-formed (all arms declare the same set).
#[derive(Debug)]
pub enum Term<T: InlineEncoding> {
    /// A variable to solve for.
    Var(Variable<T>),
    /// A constant value pinned at construction.
    Const(Inline<T>),
}

impl<T: InlineEncoding> Copy for Term<T> {}

impl<T: InlineEncoding> Clone for Term<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: InlineEncoding> From<Variable<T>> for Term<T> {
    fn from(v: Variable<T>) -> Self {
        Term::Var(v)
    }
}

impl<T: InlineEncoding> From<Inline<T>> for Term<T> {
    fn from(value: Inline<T>) -> Self {
        Term::Const(value)
    }
}

/// Untyped runtime form of a [`Term`]: a variable slot index or a pinned
/// 32-byte value. Constraint implementations store this and use
/// [`is_var`](RawTerm::is_var) / [`bound`](RawTerm::bound) in place of the
/// raw `VariableId` comparison and `Binding::get` lookup — a constant term
/// then flows through the existing bound-position dispatch for free.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawTerm {
    /// A variable slot index.
    Var(VariableId),
    /// A pinned raw value.
    Const(RawInline),
}

impl RawTerm {
    /// Returns `true` when this term is the given variable.
    #[inline]
    pub fn is_var(&self, variable: VariableId) -> bool {
        matches!(self, RawTerm::Var(v) if *v == variable)
    }

    /// Returns the value this position holds under `binding`: the pinned
    /// value for a constant, the binding's value (if any) for a variable.
    ///
    /// This is the helper that unifies "bound variable" and "constant" —
    /// backend dispatch keyed on "is this position bound?" treats a
    /// constant as a position that is born bound, with no extra match arms.
    #[inline]
    pub fn position_value<'b>(&'b self, binding: &'b Binding) -> Option<&'b RawInline> {
        match self {
            RawTerm::Var(v) => binding.get(*v),
            RawTerm::Const(c) => Some(c),
        }
    }

    /// Adds the term's variable (if it is one) to `set`. Constants stay
    /// below the variable layer and are never added.
    #[inline]
    pub fn add_to(&self, set: &mut VariableSet) {
        if let RawTerm::Var(v) = self {
            set.set(*v);
        }
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

/// The values assigned to the variables of a query — stored as **paths,
/// not copies**.
///
/// A bound variable's value always *originates* from that variable's own
/// level buffer: [`propose`](Constraint::propose) fills the buffer and
/// the engine binds by consuming one of its entries. So a binding does
/// not need to carry the 32 bytes; it carries the `u32` index of the
/// chosen entry and resolves it through the buffers on read. Two
/// properties make the index as good as the value:
///
/// * A level's buffer is cleared and refilled only when its variable is
///   (re-)pushed, and the engine only ever pushes a variable that is
///   currently unbound (deeper levels are unset by backtracking before
///   their level is re-pushed). While a variable is bound, its buffer is
///   stable, so its index stays valid for exactly the lifetime of the
///   binding.
/// * Buffers are write-once: confirmers kill entries by clearing a
///   parallel liveness word, and nothing ever moves or rewrites a stored
///   value once the engine can see it.
///
/// `Binding` is therefore a *view* — the index row plus a borrow of the
/// buffers it indexes into — constructed for the duration of one
/// constraint call. [`BindingStore`] owns both halves.
///
/// The payoff is size: an assignment is one `u32` per variable slot
/// instead of a 32-byte raw value each, a bind is a 4-byte write instead
/// of a 32-byte copy, cloning the search state no longer memcpies the
/// values, and a *batch* of bindings — a [`Frontier`] — is a small
/// integer matrix over shared buffers rather than a pile of value
/// copies. That last property is what makes a wide frontier affordable
/// at all: widening the batch costs `stride` `u32`s per row, not 4 KiB.
#[derive(Clone, Copy)]
pub struct Binding<'a> {
    /// Bitset tracking which variables have been assigned a value.
    pub bound: VariableSet,
    indexes: &'a [u32],
    levels: &'a [ProposalBuffer],
}

impl fmt::Debug for Binding<'_> {
    /// Prints the bound variables and the values they resolve to — the
    /// assignment, not the buffers it is a path through.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.bound.into_iter().map(|v| (v, self.get(v))))
            .finish()
    }
}

impl<'a> Binding<'a> {
    /// Returns the value bound to `variable`, or `None` when it is
    /// unbound — resolved through `variable`'s level buffer.
    pub fn get(&self, variable: VariableId) -> Option<&'a RawInline> {
        if self.bound.is_set(variable) {
            Some(&self.levels[variable][self.indexes[variable] as usize])
        } else {
            None
        }
    }
}

/// Backing for the empty [`Binding`]: nothing is bound, so nothing ever
/// resolves through them, but a view needs something to point at.
static NO_LEVELS: [ProposalBuffer; 128] = [const { ProposalBuffer::empty() }; 128];
static NO_INDEXES: [u32; 128] = [0; 128];

impl Default for Binding<'_> {
    /// The empty binding — no variable is bound.
    fn default() -> Self {
        Binding {
            bound: VariableSet::new_empty(),
            indexes: &NO_INDEXES,
            levels: &NO_LEVELS,
        }
    }
}

/// A **batch of parent bindings** — the collection that
/// [`propose_frontier`](Constraint::propose_frontier) expands and
/// [`confirm_frontier`](Constraint::confirm_frontier) filters against.
///
/// Every row is at the same point in the search — they share
/// [`bound`](Frontier::bound) and differ only in which values they took —
/// so a frontier is an *index matrix*, not a pile of copied assignments:
/// `stride` `u32`s per row over the level buffers those indexes point
/// into. Today's single-binding call sites become a frontier of one (see
/// [`BindingStore::frontier`]) and behave identically.
///
/// This is the whole point of the batched protocol. With a width-1
/// frontier only the root level's propose is wide; every deeper level
/// confirms a handful of candidates for one parent. Expanding `W` parents
/// together makes *every* level's region `W` segments wide, which is what
/// puts the GPU and SIMD confirm tiers in play throughout a query rather
/// than once at the top. Expanding `N` prefixes together is the same
/// total work as expanding them one at a time — the AGM bound is about
/// output, not traversal order — so worst-case optimality is untouched;
/// what it costs is frontier memory, which the engine caps at
/// [`FRONTIER_WIDTH`] rows per level.
#[derive(Clone, Copy)]
pub struct Frontier<'a> {
    bound: VariableSet,
    indexes: &'a [u32],
    stride: usize,
    levels: &'a [ProposalBuffer],
}

impl fmt::Debug for Frontier<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.rows()).finish()
    }
}

impl<'a> Frontier<'a> {
    /// Number of parent bindings in this batch.
    pub fn len(&self) -> usize {
        self.indexes.len() / self.stride
    }

    /// True when the batch has no rows.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// The variables bound in *every* row of this batch.
    pub fn bound(&self) -> VariableSet {
        self.bound
    }

    /// The binding of row `row`.
    pub fn row(&self, row: usize) -> Binding<'a> {
        Binding {
            bound: self.bound,
            indexes: &self.indexes[row * self.stride..(row + 1) * self.stride],
            levels: self.levels,
        }
    }

    /// Iterates the batch's bindings in row order.
    pub fn rows(&self) -> impl Iterator<Item = Binding<'a>> + '_ {
        (0..self.len()).map(|row| self.row(row))
    }

    /// Row numbers of a bounded, evenly-spread sample of this batch.
    ///
    /// Used wherever a batch needs *one* number per variable or per child
    /// constraint — the engine's variable choice, an intersection's
    /// proposer choice — instead of one per row. Asking every row would
    /// make the decision cost proportional to the batch, which is exactly
    /// the cost the batch exists to amortise. [`ESTIMATE_SAMPLE`] rows are
    /// plenty because both decisions consume the estimate through a
    /// *coarse* key (see the note on [`Query`]'s ordering key), so the
    /// sample only has to resolve which power-of-two bucket the batch
    /// sits in, not the exact cardinality.
    pub fn sample(&self) -> impl Iterator<Item = usize> + '_ {
        let rows = self.len();
        let taken = rows.min(ESTIMATE_SAMPLE);
        // Spread the sample across the batch rather than taking a prefix:
        // frontier rows arrive in the proposer's value order, which is
        // often correlated with degree.
        let step = if taken == 0 { 1 } else { rows / taken };
        (0..taken).map(move |i| i * step)
    }
}

/// Owns what a [`Binding`] is a view of: the per-variable level buffers
/// and the stack of frontier index matrices that pick entries out of
/// them.
///
/// This is the query engine's search state. It is also how code outside
/// the engine builds a binding over values it picked itself — see
/// [`bind`](BindingStore::bind) — in which case the store holds a single
/// frontier of a single row.
#[derive(Clone)]
pub struct BindingStore {
    bound: VariableSet,
    /// Index-row width. Row `r` of a frontier occupies
    /// `[r * stride .. (r + 1) * stride)`; slot `v` of a row is
    /// variable `v`'s entry index.
    stride: usize,
    /// One index matrix per search depth. `frontiers[0]` is the root: a
    /// single row binding nothing. `frontiers[d + 1]` is the batch that
    /// results from expanding `frontiers[d]` by one variable, so
    /// backtracking to a shallower batch is a `pop`, not a rebuild.
    frontiers: Vec<Vec<u32>>,
    /// Retired index matrices, kept for their capacity. A deep, narrow
    /// search pushes and pops a batch at every level of every descent, so
    /// without this the engine would allocate once per level per descent.
    /// Cleared on retirement, so cloning the store does not copy them.
    spare: Vec<Vec<u32>>,
    levels: [ProposalBuffer; 128],
}

impl fmt::Debug for BindingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.view(), f)
    }
}

impl Default for BindingStore {
    fn default() -> Self {
        Self::with_stride(128)
    }
}

impl BindingStore {
    /// An empty store: nothing bound, every level empty, one root row
    /// wide enough for any of the 128 variable slots.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store whose index rows carry `stride` slots — the engine
    /// sizes this to the query's variable count so a wide frontier costs
    /// `stride` `u32`s per row instead of the full 128.
    pub(crate) fn with_stride(stride: usize) -> Self {
        BindingStore {
            bound: VariableSet::new_empty(),
            stride,
            frontiers: vec![vec![0u32; stride]],
            spare: Vec::new(),
            levels: std::array::from_fn(|_| ProposalBuffer::new()),
        }
    }

    /// The current assignment, as [`satisfied`](Constraint::satisfied)
    /// and result post-processing see it: row 0 of the current frontier.
    pub fn view(&self) -> Binding<'_> {
        self.frontier().row(0)
    }

    /// The current batch of assignments, as
    /// [`propose_frontier`](Constraint::propose_frontier) and
    /// [`confirm_frontier`](Constraint::confirm_frontier) see it.
    ///
    /// For a store built with [`new`](BindingStore::new) this is a
    /// frontier of one — the bridge that lets every single-binding caller
    /// drive the batched protocol unchanged.
    pub fn frontier(&self) -> Frontier<'_> {
        Frontier {
            bound: self.bound,
            indexes: self.frontiers.last().expect("non-empty frontier stack"),
            stride: self.stride,
            levels: &self.levels,
        }
    }

    /// Binds `variable` to `value` by appending it to that variable's
    /// level buffer and pointing the index row at it.
    ///
    /// The engine never needs this — its candidates are already in the
    /// buffer, so it binds by index. This is the entry point for callers
    /// outside the search (tests, tools) that want a binding over values
    /// of their own choosing.
    pub fn bind(&mut self, variable: VariableId, value: &RawInline) {
        let level = &mut self.levels[variable];
        let index = level.len();
        level.push(*value);
        let stride = self.stride;
        self.frontiers.last_mut().expect("non-empty frontier stack")[variable] = index as u32;
        debug_assert!(variable < stride);
        self.bound.set(variable);
    }

    /// Unbinds `variable` — the engine's backtracking step.
    pub fn unset(&mut self, variable: VariableId) {
        self.bound.unset(variable);
    }

    /// The set of currently bound variables.
    pub fn bound(&self) -> VariableSet {
        self.bound
    }

    /// Entry count of `variable`'s level buffer.
    fn level_len(&self, variable: VariableId) -> usize {
        self.levels[variable].len()
    }

    /// Appends up to `width` live entry indexes of `variable`'s level,
    /// starting at `from`, to `out`. Returns the position to resume from.
    fn draw(&self, variable: VariableId, from: usize, width: usize, out: &mut Vec<u32>) -> usize {
        let level = &self.levels[variable];
        let mut pos = from;
        while out.len() < width {
            match level.next_live(pos) {
                Some(i) => {
                    out.push(i as u32);
                    pos = i + 1;
                }
                None => return level.len(),
            }
        }
        pos
    }

    /// Clears `variable`'s level and refills it by proposing over the
    /// current frontier.
    ///
    /// This is the **only** primitive that writes a level buffer. The
    /// resumable-narrowing path used to be a second one, and it was the
    /// only reason a level could be appended to while its variable was
    /// still bound — which forced a detached-buffer special case and
    /// falsified "a bound variable's buffer is stable for the lifetime of
    /// its binding". With it gone the invariant holds unconditionally,
    /// and the `debug_assert` below is that invariant made checkable.
    ///
    /// The level is moved *out* of the array for the duration of the
    /// call, so `propose` gets `&mut` on it while the remaining levels
    /// lend immutably through the [`Frontier`] view — the borrow split the
    /// search needs, with no `unsafe` and no second copy of the
    /// candidates.
    fn refill(
        &mut self,
        variable: VariableId,
        propose: impl FnOnce(&Frontier<'_>, &mut ProposalBuffer),
    ) {
        debug_assert!(
            !self.bound.is_set(variable),
            "refilling a bound variable's level would strand its binding"
        );
        let mut buffer = std::mem::take(&mut self.levels[variable]);
        buffer.clear();
        let frontier = Frontier {
            bound: self.bound,
            indexes: self.frontiers.last().expect("non-empty frontier stack"),
            stride: self.stride,
            levels: &self.levels,
        };
        propose(&frontier, &mut buffer);
        debug_assert_eq!(
            buffer.segments(),
            frontier.len(),
            "propose_frontier must open exactly one segment per frontier row"
        );
        self.levels[variable] = buffer;
    }

    /// Pushes the batch that results from binding `variable` to each of
    /// `entries` (entry indexes into `variable`'s level buffer).
    ///
    /// Each new row inherits its parent row verbatim — the parent is the
    /// frontier row whose segment the entry sits in — and overwrites one
    /// slot. That is the whole descent step: one `stride`-wide memcpy and
    /// one `u32` store per row, no value ever copied.
    fn push_frontier(&mut self, variable: VariableId, entries: &[u32]) {
        let stride = self.stride;
        let parent = self.frontiers.last().expect("non-empty frontier stack");
        let bounds = self.levels[variable].bounds();
        let mut rows = self.spare.pop().unwrap_or_default();
        rows.reserve(entries.len() * stride);
        for &entry in entries {
            // Segments are ascending and cover the buffer, so the segment
            // holding `entry` is the last one starting at or before it.
            let row = bounds.partition_point(|&start| start <= entry) - 1;
            rows.extend_from_slice(&parent[row * stride..(row + 1) * stride]);
            let last = rows.len() - stride;
            rows[last + variable] = entry;
        }
        self.frontiers.push(rows);
        self.bound.set(variable);
    }

    /// Drops the deepest batch, returning to its parent — the engine's
    /// backtracking step.
    fn pop_frontier(&mut self, variable: VariableId) {
        if let Some(mut retired) = self.frontiers.pop() {
            retired.clear();
            self.spare.push(retired);
        }
        self.bound.unset(variable);
    }

    /// Splits the current batch's rows `[from..]` in half, returning the
    /// tail rows for the right half of a parallel split.
    ///
    /// Indexes stay valid across the split without any re-indexing: both
    /// halves keep element-wise identical copies of the level buffers
    /// (the clone is verbatim), so a row's entry indexes resolve to the
    /// same values in either half. Only *rows* are partitioned.
    #[cfg(feature = "parallel")]
    fn split_frontier(&mut self, from: usize) -> Option<Vec<u32>> {
        let stride = self.stride;
        let top = self.frontiers.last_mut()?;
        let rows = top.len() / stride;
        if rows.saturating_sub(from) < 2 {
            return None;
        }
        let mid = from + (rows - from) / 2;
        Some(top.split_off(mid * stride))
    }

    /// Replaces the current batch's rows (the right half of a split
    /// installing the tail it was handed, or emptying its batch when it
    /// is taking only the undrawn tails).
    #[cfg(feature = "parallel")]
    fn set_frontier(&mut self, rows: Vec<u32>) {
        *self.frontiers.last_mut().expect("non-empty frontier stack") = rows;
    }
}

type ProjectionKey = Box<[RawInline]>;

/// Maximum number of parent bindings the engine expands in one batch.
///
/// The rationale is measured, not aesthetic. A census of confirm-region
/// sizes over real dblp data found the median region to be 1–7 candidates
/// at *every* scale, p95 ~200, with only a handful of 63k–268k regions —
/// all of them at the root. The cause was the width-1 frontier: only the
/// root's propose is wide, and every deeper level confirms a handful of
/// candidates for one parent. Batched-confirm tiers with a fixed dispatch
/// cost therefore engaged exactly once per query.
///
/// 16384 is where a batch becomes worth a device dispatch: it is
/// `triblespace-gpu`'s own `DEFAULT_MIN_CONFIRM_BATCH`, itself a measured
/// CPU/GPU crossover. At that width even a median fan-out of one puts
/// every level's region at or above the routing threshold, which is the
/// whole point of the change. One constant, trivially tunable: peak level
/// memory is `FRONTIER_WIDTH * fan-out * 36` bytes, so halving it halves
/// the engine's high-water mark.
pub const FRONTIER_WIDTH: usize = 16384;

/// Width of the *first* batch at a level; batches then grow by
/// [`FRONTIER_RAMP`] up to [`FRONTIER_WIDTH`].
///
/// Starting at one keeps time-to-first-result identical to plain DFS: a
/// query that is stopped after one row (`exists!`, `.next()`, `.take(n)`)
/// does exactly the work DFS did, because its first batch at every level
/// *is* a single row. A full drain reaches full width after
/// `log2(FRONTIER_WIDTH)` batches, so the ramp costs a logarithmic number
/// of extra propose calls at the top of the tree and no extra per-candidate
/// work at all.
const INITIAL_FRONTIER_WIDTH: usize = 1;

/// Growth factor of the frontier-width ramp.
const FRONTIER_RAMP: usize = 2;

/// Rows sampled by [`Frontier::sample`] when a batch has to be summarised
/// by one number.
///
/// 32 is generous: both consumers (the engine's variable choice, an
/// intersection's proposer choice) reduce the estimate to a coarse
/// power-of-two bucket, so the sample only has to identify the bucket the
/// batch sits in. Sampling rather than polling every row keeps the
/// decision O(1) in the batch width, which is what makes widening the
/// batch free.
pub const ESTIMATE_SAMPLE: usize = 32;

/// Growable, **segmented** buffer of candidate values — the write target of
/// [`Constraint::propose_frontier`] and the engine's per-level candidate
/// store.
///
/// Entries are plain `RawInline` (fixed-stride 32-byte POD), stored
/// contiguously and **write-once**: nothing ever moves or rewrites a stored
/// value. Each entry carries a parallel liveness word (`u32`, nonzero =
/// live): confirmers kill entries instead of removing them, and the engine
/// iterates live entries directly — there is no compaction. The pairing of
/// value and liveness is structural (one type owns both), and the buffer
/// derefs to `[RawInline]` for reading.
///
/// The word-per-entry liveness layout is the deliberate baseline: every
/// lane — CPU or GPU — writes its own word with no read-modify-write
/// contention. A bit-packed representation is a planned alternative behind
/// this same API, to be justified against this baseline.
///
/// # Segments
///
/// One buffer now holds the candidates of a whole [`Frontier`], so it
/// records where each parent row's run begins:
/// [`open_row`](ProposalBuffer::open_row) starts the next row's segment and
/// everything pushed afterwards belongs to it. Segment `s` is parent row
/// `s`. Boundaries are *starts only* — the last segment runs to the end of
/// the buffer — so nothing has to be sealed and a nested composite can take
/// a region of its own freshly-appended segments at any time.
#[derive(Clone, Debug, Default)]
pub struct ProposalBuffer {
    entries: Vec<RawInline>,
    live: Vec<u32>,
    /// Start of each parent row's segment. `bounds.len()` is the number of
    /// rows proposed for so far.
    bounds: Vec<u32>,
}

impl ProposalBuffer {
    /// Creates an empty buffer.
    pub fn new() -> Self {
        Self::empty()
    }

    /// `const` constructor so the empty [`Binding`] can borrow a `static`
    /// array of level buffers.
    pub(crate) const fn empty() -> Self {
        ProposalBuffer {
            entries: Vec::new(),
            live: Vec::new(),
            bounds: Vec::new(),
        }
    }

    /// Starts the segment of the next parent row. Every batched proposer
    /// calls this exactly once per [`Frontier`] row, in row order, before
    /// pushing that row's candidates — including for rows it has no
    /// candidates for, which get an empty segment.
    pub fn open_row(&mut self) {
        self.bounds.push(self.entries.len() as u32);
    }

    /// Number of parent rows proposed for so far.
    pub fn segments(&self) -> usize {
        self.bounds.len()
    }

    /// Segment starts, ascending; segment `s` runs from `bounds()[s]` to
    /// `bounds()[s + 1]`, or to the end of the buffer for the last one.
    pub(crate) fn bounds(&self) -> &[u32] {
        &self.bounds
    }

    /// Appends a candidate, live.
    pub fn push(&mut self, value: RawInline) {
        self.entries.push(value);
        self.live.push(1);
    }

    /// Appends every candidate from `iter`, live.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = RawInline>) {
        for value in iter {
            self.push(value);
        }
    }

    /// Appends every candidate from `slice`, live.
    pub fn extend_from_slice(&mut self, slice: &[RawInline]) {
        self.entries.extend_from_slice(slice);
        self.live.resize(self.entries.len(), 1);
    }

    /// Drops all candidates and segments, keeping capacity.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.live.clear();
        self.bounds.clear();
    }

    /// Current capacity in entries.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Reserves capacity for exactly `additional` further entries.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.entries.reserve_exact(additional);
        self.live.reserve_exact(additional);
    }

    /// Index of the first live entry at or after `from`, if any.
    pub fn next_live(&self, from: usize) -> Option<usize> {
        self.live[from.min(self.live.len())..]
            .iter()
            .position(|w| *w != 0)
            .map(|offset| from + offset)
    }

    /// Number of live entries at or after `from`.
    pub fn count_live(&self, from: usize) -> usize {
        self.live[from.min(self.live.len())..]
            .iter()
            .filter(|w| **w != 0)
            .count()
    }

    /// Whether entry `i` is live.
    pub fn is_live(&self, i: usize) -> bool {
        self.live[i] != 0
    }

    /// Iterates the live entry values at or after `from` — the engine's own
    /// consumption view, also handy for inspecting survivors in tests.
    pub fn live_values(&self, from: usize) -> impl Iterator<Item = &RawInline> {
        self.entries[from..]
            .iter()
            .zip(self.live[from..].iter())
            .filter(|(_, w)| **w != 0)
            .map(|(v, _)| v)
    }

    /// The confirmable region from `base` onward as a **single** segment:
    /// entry values paired with their killable liveness words.
    ///
    /// This is the single-binding shape — what a row-at-a-time proposer
    /// hands its siblings. Use
    /// [`region_since`](ProposalBuffer::region_since) for the batched
    /// shape.
    pub fn region(&mut self, base: usize) -> Candidates<'_> {
        Candidates {
            values: &self.entries[base..],
            live: &mut self.live[base..],
            bounds: None,
            base,
            row0: 0,
        }
    }

    /// The confirmable region a batched proposer just appended: entries
    /// from `base` onward, carved into the segments opened from
    /// `segment_base` onward.
    ///
    /// Segment `s` of the result is frontier row `s`, which is what lets a
    /// confirmer recover the parent binding each candidate belongs to.
    pub fn region_since(&mut self, base: usize, segment_base: usize) -> Candidates<'_> {
        Candidates {
            values: &self.entries[base..],
            live: &mut self.live[base..],
            bounds: Some(&self.bounds[segment_base..]),
            base,
            row0: 0,
        }
    }

    /// Rewrites the freshly-proposed region `[base..]` with `values`, all
    /// live. Only a proposer may call this, and only on the region it
    /// appended in the current call, before returning — after that,
    /// indices are frozen because kills bind to them. Used by
    /// [`UnionConstraint`](unionconstraint::UnionConstraint) for its
    /// sort-dedup.
    pub fn rewrite_region(&mut self, base: usize, values: Vec<RawInline>) {
        self.entries.truncate(base);
        self.live.truncate(base);
        for value in values {
            self.push(value);
        }
    }
}

impl std::ops::Deref for ProposalBuffer {
    type Target = [RawInline];

    fn deref(&self) -> &[RawInline] {
        &self.entries
    }
}

impl<'a> IntoIterator for &'a ProposalBuffer {
    type Item = &'a RawInline;
    type IntoIter = std::slice::Iter<'a, RawInline>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// A confirmable view over one proposal region: entry values read-only,
/// liveness killable — the argument of [`Constraint::confirm`] and
/// [`Constraint::confirm_frontier`].
///
/// Confirmers may only kill entries, never revive them, so any number of
/// confirmers writing into the same region compute their conjunction —
/// sequentially (each skipping already-dead entries) or in parallel over
/// copies merged with [`and_words`]/[`or_words`]. Index `i` refers to
/// `values()[i]`.
///
/// # Parent tags
///
/// A batched region holds the candidates of a whole [`Frontier`], so every
/// candidate carries the parent binding it was proposed for. The tag is
/// carried structurally, as segment boundaries rather than a word per
/// entry: [`segments`](Candidates::segments) counts the runs,
/// [`segment`](Candidates::segment) hands out one run at a time together
/// with its frontier row, and [`parent_row`](Candidates::parent_row)
/// answers the same question per entry for a confirmer that wants to work
/// across segments (a device kernel building per-candidate probe ranges,
/// say). Segment boundaries are `rows + 1` words for the whole region
/// instead of one word per candidate, and they are what every in-tree
/// confirmer actually wants — the parent binding is needed once per run,
/// not once per value.
///
/// A single-segment region (the row-at-a-time shape) carries no bounds at
/// all and reports one segment.
pub struct Candidates<'a> {
    values: &'a [RawInline],
    live: &'a mut [u32],
    /// Absolute segment starts, or `None` for a single-segment region.
    bounds: Option<&'a [u32]>,
    /// Absolute index of `values[0]`, so bounds can stay absolute.
    base: usize,
    /// Frontier row of this region's first segment.
    row0: usize,
}

impl<'a> Candidates<'a> {
    /// The candidate values of this region.
    pub fn values(&self) -> &[RawInline] {
        self.values
    }

    /// Number of parent segments in this region (1 when it is a single
    /// segment).
    pub fn segments(&self) -> usize {
        self.bounds.map_or(1, <[u32]>::len)
    }

    /// Segment `s` as its own single-segment region, together with the
    /// [`Frontier`] row it was proposed for.
    ///
    /// Taking segments one at a time is how a row-at-a-time confirmer is
    /// lifted to a batch — see the default
    /// [`confirm_frontier`](Constraint::confirm_frontier).
    pub fn segment(&mut self, s: usize) -> (usize, Candidates<'_>) {
        let Some(bounds) = self.bounds else {
            debug_assert_eq!(s, 0, "a single-segment region has only segment 0");
            let row = self.row0;
            return (row, self.reborrow());
        };
        let base = self.base;
        let lo = bounds[s] as usize - base;
        let hi = bounds
            .get(s + 1)
            .map_or(self.values.len(), |end| *end as usize - base);
        let row = self.row0 + s;
        (
            row,
            Candidates {
                values: &self.values[lo..hi],
                live: &mut self.live[lo..hi],
                bounds: None,
                base: base + lo,
                row0: row,
            },
        )
    }

    /// The [`Frontier`] row entry `i` was proposed for.
    pub fn parent_row(&self, i: usize) -> usize {
        match self.bounds {
            None => self.row0,
            Some(bounds) => {
                self.row0 + bounds.partition_point(|&start| (start as usize) <= self.base + i) - 1
            }
        }
    }

    /// Number of entries (live and dead) in this region.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// True when the region has no entries.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Whether entry `i` is still live.
    pub fn is_live(&self, i: usize) -> bool {
        self.live[i] != 0
    }

    /// Marks entry `i` dead.
    pub fn kill(&mut self, i: usize) {
        self.live[i] = 0;
    }

    /// Kills every live entry whose value fails `keep` — the region-side
    /// equivalent of `Vec::retain`, skipping entries already dead. Lives on
    /// the pair type deliberately: values and liveness are one object here,
    /// so there is no loose pairing to misalign.
    pub fn retain(&mut self, mut keep: impl FnMut(&RawInline) -> bool) {
        for (i, value) in self.values.iter().enumerate() {
            if self.live[i] != 0 && !keep(value) {
                self.live[i] = 0;
            }
        }
    }

    /// Kills every entry (a confirmer that can prove total inconsistency).
    pub fn kill_all(&mut self) {
        self.live.fill(0);
    }

    /// Reborrows this region mutably (for passing to several confirmers in
    /// sequence).
    pub fn reborrow(&mut self) -> Candidates<'_> {
        Candidates {
            values: self.values,
            live: self.live,
            bounds: self.bounds,
            base: self.base,
            row0: self.row0,
        }
    }

    /// Copies the liveness words out (scratch for OR-composition).
    pub fn live_words(&self) -> Vec<u32> {
        self.live.to_vec()
    }

    /// Replaces the liveness words (merge result of OR-composition).
    pub fn set_live_words(&mut self, words: &[u32]) {
        debug_assert_eq!(words.len(), self.live.len());
        self.live.copy_from_slice(words);
    }

    /// A detached scratch region over `words` with the same values and
    /// segmentation — used by OR-composition to collect per-variant
    /// verdicts.
    pub fn scratch<'b>(&self, words: &'b mut [u32]) -> Candidates<'b>
    where
        'a: 'b,
    {
        Candidates {
            values: self.values,
            live: words,
            bounds: self.bounds,
            base: self.base,
            row0: self.row0,
        }
    }
}

/// Word-wise AND of `other` into `words` (conjunction of live sets).
pub fn and_words(words: &mut [u32], other: &[u32]) {
    debug_assert_eq!(words.len(), other.len());
    for (w, o) in words.iter_mut().zip(other.iter()) {
        *w &= *o;
    }
}

/// Word-wise OR of `other` into `words` (disjunction of live sets).
pub fn or_words(words: &mut [u32], other: &[u32]) {
    debug_assert_eq!(words.len(), other.len());
    for (w, o) in words.iter_mut().zip(other.iter()) {
        *w |= *o;
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
/// # Batching
///
/// The engine does not walk one binding at a time. It maintains a
/// [`Frontier`] — a batch of up to [`FRONTIER_WIDTH`] parent bindings —
/// and drives [`propose_frontier`](Constraint::propose_frontier) and
/// [`confirm_frontier`](Constraint::confirm_frontier), which expand and
/// filter the whole batch in one call.
///
/// Those two are *provided* methods. The obligation a data source takes on
/// is still the single-binding pair [`propose`](Constraint::propose) and
/// [`confirm`](Constraint::confirm), and the defaults lift them to a batch
/// by walking rows and segments. That split is deliberate: it keeps the
/// mandatory protocol at "iterate, and answer a point query", which is why
/// a hash map, a PATCH, a succinct archive and a GPU-resident structure can
/// all be sources. A batch-aware source (one that can ship a whole region
/// to a device, or vectorise a probe across candidates) overrides the
/// frontier methods and gets a region `W` segments wide to work on; a
/// source that cannot loses nothing.
///
/// Note what is *not* required: no seek, no leapfrog, no
/// resume-from-a-key. Requiring those would disqualify half the source
/// types above. (Galloping intersection remains a deliberately deferred
/// option, and it would be an *additional* capability, not a new
/// obligation.)
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
    /// being bound. Values are appended to `proposals`; entries appended by
    /// an enclosing composite's other children may precede them, and must
    /// be left untouched.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer);

    /// Enumerates candidate values for `variable` for **every row** of
    /// `frontier`, one segment per row.
    ///
    /// The contract is exactly [`propose`](Constraint::propose) repeated
    /// across a batch, and the default is that repetition: open row `r`'s
    /// segment, propose for `frontier.row(r)`, in row order. Overriders
    /// must keep that shape — exactly one
    /// [`open_row`](ProposalBuffer::open_row) per frontier row, in order,
    /// including for rows with no candidates — because the engine reads
    /// the segments back as parent tags.
    ///
    /// Override this when a source can enumerate a batch of parents more
    /// cheaply than one at a time. Delivering a value twice within one
    /// row's segment is a semantics error: it would inflate bag
    /// multiplicity.
    fn propose_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        for row in 0..frontier.len() {
            proposals.open_row();
            self.propose(variable, &frontier.row(row), proposals);
        }
    }

    /// Kills [`Candidates`] entries whose values for `variable` violate
    /// this constraint.
    ///
    /// Called on every constraint *except* the one that proposed, in order
    /// of increasing estimate, all killing into the same region —
    /// sequential kills compute the conjunction. Implementations may only
    /// kill entries, never revive them, and may skip entries that are
    /// already dead. Nothing is ever compacted: dead entries stay in place
    /// and the engine iterates live ones.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>);

    /// Kills entries of a **batched** region — one segment per row of
    /// `frontier` — whose values violate this constraint under the parent
    /// binding they were proposed for.
    ///
    /// The default walks the segments and calls
    /// [`confirm`](Constraint::confirm) with each segment's own binding,
    /// which is exactly the single-binding behaviour repeated. Override it
    /// when the whole region can be judged at once (the GPU archive ships
    /// a region to one kernel); the kill-only contract makes that safe —
    /// however the verdicts are computed, merging them can only clear
    /// liveness bits.
    fn confirm_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        cands: &mut Candidates<'_>,
    ) {
        for s in 0..cands.segments() {
            let (row, mut segment) = cands.segment(s);
            self.confirm(variable, &frontier.row(row), &mut segment);
        }
    }

    /// Estimate for `variable` aggregated over a sample of `frontier`'s
    /// rows — the batch-level analogue of
    /// [`estimate`](Constraint::estimate), and the number composites use
    /// to pick a proposer for a whole batch.
    ///
    /// Returns `None` exactly when `estimate` does, i.e. when `variable` is
    /// not one of this constraint's [`variables`](Constraint::variables).
    /// That is a binding-independent property, which is what makes "which
    /// children are relevant" a per-batch question rather than a per-row
    /// one.
    fn frontier_estimate(&self, variable: VariableId, frontier: &Frontier<'_>) -> Option<usize> {
        let mut total: usize = 0;
        let mut sampled = false;
        for row in frontier.sample() {
            total = total.saturating_add(self.estimate(variable, &frontier.row(row))?);
            sampled = true;
        }
        sampled.then_some(total)
    }

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

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn propose_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let inner: &T = self;
        inner.propose_frontier(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, binding, cands)
    }

    fn confirm_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        cands: &mut Candidates<'_>,
    ) {
        let inner: &T = self;
        inner.confirm_frontier(variable, frontier, cands)
    }

    fn frontier_estimate(&self, variable: VariableId, frontier: &Frontier<'_>) -> Option<usize> {
        let inner: &T = self;
        inner.frontier_estimate(variable, frontier)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let inner: &T = self;
        inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
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

    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer) {
        let inner: &T = self;
        inner.propose(variable, binding, proposals)
    }

    fn propose_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let inner: &T = self;
        inner.propose_frontier(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, binding, cands)
    }

    fn confirm_frontier(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        cands: &mut Candidates<'_>,
    ) {
        let inner: &T = self;
        inner.confirm_frontier(variable, frontier, cands)
    }

    fn frontier_estimate(&self, variable: VariableId, frontier: &Frontier<'_>) -> Option<usize> {
        let inner: &T = self;
        inner.frontier_estimate(variable, frontier)
    }

    fn satisfied(&self, binding: &Binding) -> bool {
        let inner: &T = self;
        inner.satisfied(binding)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
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
pub struct Query<C, P: Fn(&Binding<'_>) -> Option<R>, R> {
    constraint: C,
    postprocessing: P,
    mode: Search,
    bindings: BindingStore,
    influences: [VariableSet; 128],
    estimates: [usize; 128],
    touched_variables: VariableSet,
    stack: ArrayVec<Level, 128>,
    unbound: ArrayVec<VariableId, 128>,
    /// Row cursor while the deepest batch is being emitted.
    emit: usize,
    /// Reusable scratch for the entry indexes of one drawn batch.
    drawn: Vec<u32>,
}

/// One search level: the variable it binds, how far its candidate buffer
/// has been drawn into batches, and how wide the next batch may be.
///
/// The buffer itself lives in the [`BindingStore`] (keyed by variable, not
/// by depth) because that is what a [`Binding`]'s indexes resolve through.
#[derive(Clone, Debug)]
struct Level {
    variable: VariableId,
    /// Entries before this position have been drawn into a batch already.
    pos: usize,
    /// Width of the next batch drawn here.
    width: usize,
}

// Manual `Clone` impl, because `#[derive(Clone)]` would require `R: Clone`
// which isn't actually needed — `R` only appears in `P`'s return type.
#[cfg(feature = "parallel")]
impl<C, P, R> Clone for Query<C, P, R>
where
    C: Clone,
    P: Fn(&Binding<'_>) -> Option<R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            constraint: self.constraint.clone(),
            postprocessing: self.postprocessing.clone(),
            mode: self.mode,
            bindings: self.bindings.clone(),
            influences: self.influences,
            estimates: self.estimates,
            touched_variables: self.touched_variables,
            stack: self.stack.clone(),
            unbound: self.unbound.clone(),
            emit: self.emit,
            drawn: Vec::new(),
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Query<C, P, R> {
    /// Picks the next unbound variable, refreshes the estimates the last
    /// batch change invalidated, re-sorts `unbound`, pushes a level, and
    /// proposes for the **whole current batch** via
    /// [`Constraint::propose_frontier`]. Leaves `mode = Draw`. The caller
    /// is responsible for ensuring `unbound` is non-empty.
    ///
    /// The next variable is chosen **once per batch**, not once per row.
    /// That is licensed by how coarse the ordering key is: variables are
    /// ranked by `ilog2(estimate) + 1`, so two rows disagree about the next
    /// variable only when their estimates fall in different powers of two.
    /// Sibling rows under one parent are normally within a factor of two of
    /// each other, so they agree by construction; genuine disagreement
    /// needs genuine skew (a hub row against uniform rows), not ordinary
    /// variation. The coarse key was originally chosen for cheapness, and
    /// it turns out to buy robustness under a design change it predates —
    /// worth knowing before anyone "improves" it into an exact-cardinality
    /// comparison, which would make sibling rows disagree constantly and
    /// force the frontier to be fragmented by shape.
    ///
    /// If that ever does become the bottleneck, the seam is right here:
    /// sample the batch, and when no bucket holds a large majority,
    /// partition the frontier by bucket and expand the parts separately.
    /// Deliberately not built — it is the escape hatch, not the design.
    fn push_next_variable(&mut self) {
        let mut stale_estimates = VariableSet::new_empty();
        while let Some(variable) = self.touched_variables.drain_next_ascending() {
            stale_estimates = stale_estimates.union(self.influences[variable]);
        }
        // Bound variables can't be influenced by the unbound ones, so skip.
        stale_estimates = stale_estimates.subtract(self.bindings.bound());

        if !stale_estimates.is_empty() {
            while let Some(v) = stale_estimates.drain_next_ascending() {
                self.estimates[v] = self.batch_estimate(v);
            }
            self.unbound.sort_unstable_by_key(|v| {
                (
                    Reverse(
                        self.estimates[*v]
                            .checked_ilog2()
                            .map(|magnitude| magnitude + 1)
                            .unwrap_or(0),
                    ),
                    self.influences[*v].count(),
                )
            });
        }

        let variable = self.unbound.pop().expect("non-empty unbound");
        self.stack.push(Level {
            variable,
            pos: 0,
            width: INITIAL_FRONTIER_WIDTH,
        });
        let constraint = &self.constraint;
        self.bindings.refill(variable, |frontier, proposals| {
            constraint.propose_frontier(variable, frontier, proposals)
        });
        self.mode = Search::Draw;
    }

    /// One estimate for `variable` over the whole current batch: the
    /// **modal power-of-two bucket** across a sample of rows.
    ///
    /// The mode rather than the mean or the sum, because the number is only
    /// ever consumed through `ilog2`: what the ordering needs is the bucket
    /// the typical row sits in, and a mean would let one hub row drag the
    /// whole batch's ordering with it. Returned as a representative
    /// estimate from that bucket so the value keeps its usual meaning.
    fn batch_estimate(&self, variable: VariableId) -> usize {
        let frontier = self.bindings.frontier();
        if frontier.len() == 1 {
            // The overwhelmingly common case, and the one every level
            // starts in: one row has nothing to summarise, so answer it
            // exactly and skip the bucket table entirely.
            return self
                .constraint
                .estimate(variable, &frontier.row(0))
                .expect("unconstrained variable in query");
        }
        // Bucket 0 is "estimate 0"; buckets are ilog2(e) + 1 thereafter, so
        // 65 buckets cover every usize.
        let mut counts = [0usize; 65];
        let mut representative = [0usize; 65];
        let mut sampled = false;
        for row in frontier.sample() {
            let estimate = self
                .constraint
                .estimate(variable, &frontier.row(row))
                .expect("unconstrained variable in query");
            let bucket = estimate
                .checked_ilog2()
                .map(|magnitude| magnitude as usize + 1)
                .unwrap_or(0);
            counts[bucket] += 1;
            representative[bucket] = representative[bucket].max(estimate);
            sampled = true;
        }
        if !sampled {
            return 0;
        }
        let modal = counts
            .iter()
            .enumerate()
            .max_by_key(|(bucket, count)| (**count, Reverse(*bucket)))
            .map(|(bucket, _)| bucket)
            .expect("non-empty bucket table");
        representative[modal]
    }

    /// Draws the next batch of live candidates at the top level and
    /// descends into it. Leaves `mode = Expand` on success and
    /// `mode = Pop` when the level's buffer is spent.
    fn draw_batch(&mut self) {
        let level = self.stack.last().expect("non-empty stack");
        let (variable, from, width) = (level.variable, level.pos, level.width);
        let mut drawn = std::mem::take(&mut self.drawn);
        drawn.clear();
        let pos = self.bindings.draw(variable, from, width, &mut drawn);
        let level = self.stack.last_mut().expect("non-empty stack");
        level.pos = pos;
        level.width = width.saturating_mul(FRONTIER_RAMP).min(FRONTIER_WIDTH);
        if drawn.is_empty() {
            self.mode = Search::Pop;
        } else {
            self.bindings.push_frontier(variable, &drawn);
            self.touched_variables.set(variable);
            self.mode = Search::Expand;
        }
        self.drawn = drawn;
    }

    /// Returns to the parent batch after the current one is exhausted, and
    /// asks its level for the next batch.
    fn retreat(&mut self) {
        match self.stack.last() {
            Some(level) => {
                let variable = level.variable;
                self.bindings.pop_frontier(variable);
                self.touched_variables.set(variable);
                self.mode = Search::Draw;
            }
            None => self.mode = Search::Done,
        }
    }

    /// Abandons the top level (its buffer is spent) and hands control back
    /// to the level above.
    fn pop_level(&mut self) {
        match self.stack.pop() {
            Some(level) => {
                self.unbound.push(level.variable);
                // Restore the estimates of everything this level influenced
                // to their pre-binding state.
                self.touched_variables.set(level.variable);
                if self.stack.is_empty() {
                    self.mode = Search::Done;
                } else {
                    self.mode = Search::Retreat;
                }
            }
            None => self.mode = Search::Done,
        }
    }

    /// Whether any level still holds candidates that have not been drawn
    /// into a batch.
    #[cfg(feature = "parallel")]
    fn has_undrawn(&self) -> bool {
        self.stack
            .iter()
            .any(|level| level.pos < self.bindings.level_len(level.variable))
    }

    /// Marks every level's candidates as fully drawn — the left half of a
    /// split explores only the batch it is holding, because the right half
    /// has taken ownership of every undrawn candidate above it.
    #[cfg(feature = "parallel")]
    fn seal_levels(&mut self) {
        for level in self.stack.iter_mut() {
            level.pos = self.bindings.level_len(level.variable);
        }
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
        // Index rows only need a slot per variable the query actually
        // mentions. At `FRONTIER_WIDTH` rows that is the difference between
        // a 0.5 MiB batch for a four-variable query and an 8 MiB one.
        let stride = variables
            .into_iter()
            .max()
            .map(|highest| highest + 1)
            .unwrap_or(1);
        let bindings = BindingStore::with_stride(stride);
        let estimates = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint
                    .estimate(v, &bindings.view())
                    .expect("unconstrained variable in query")
            } else {
                usize::MAX
            }
        });
        let mut unbound = ArrayVec::from_iter(variables);
        unbound.sort_unstable_by_key(|v| {
            (
                Reverse(
                    estimates[*v]
                        .checked_ilog2()
                        .map(|magnitude| magnitude + 1)
                        .unwrap_or(0),
                ),
                influences[*v].count(),
            )
        });

        // Constraints whose positions are all constant [`Term`]s (e.g. a
        // fully-constant `pattern!` used as an existence check) have an
        // empty variable set, so the propose/confirm search never consults
        // them. Their truth is binding-independent and `satisfied` is exact
        // for them from the start (the fully-bound exactness law: zero
        // unbound variables). One check up front settles every such
        // subtree; constraints with unbound variables answer an optimistic
        // `true` here and are validated by the search as usual.
        let mode = if constraint.satisfied(&bindings.view()) {
            Search::Expand
        } else {
            Search::Done
        };

        Query {
            constraint,
            postprocessing,
            mode,
            bindings,
            influences,
            estimates,
            touched_variables: VariableSet::new_empty(),
            stack: ArrayVec::new(),
            unbound,
            emit: 0,
            drawn: Vec::new(),
        }
    }
}

/// The search mode of the query engine.
///
/// The engine is still a depth-first search; what changed is the unit of
/// descent. Instead of binding one value and recursing, it draws a *batch*
/// of up to [`FRONTIER_WIDTH`] candidates and expands all of them together,
/// so each level's propose/confirm sees a region as wide as the batch.
///
/// - `Expand` — a batch is current; choose the next variable and propose
///   for every one of its rows.
/// - `Draw` — a level's candidates are materialised; take the next batch of
///   live ones and descend into it.
/// - `Emit` — every variable is bound; the current batch *is* a block of
///   results, yielded one row at a time.
/// - `Retreat` — the current batch is finished; drop it and ask its level
///   for the next one.
/// - `Pop` — a level's candidates are spent; abandon it.
/// - `Done` — the search is over.
///
/// Bag semantics are unchanged and are what the batching has to preserve:
/// one row per complete binding, no deduplication. Every candidate is drawn
/// into exactly one batch (`pos` only moves forward) and every batch row
/// becomes exactly one emitted row, so batching partitions the same result
/// multiset the one-at-a-time search produced. Only the *order* differs,
/// which the engine has never promised.
#[derive(Copy, Clone, Debug)]
enum Search {
    Expand,
    Draw,
    Emit,
    Retreat,
    Pop,
    Done,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.mode {
                Search::Expand => {
                    if self.unbound.is_empty() {
                        self.emit = 0;
                        self.mode = Search::Emit;
                        continue;
                    }
                    self.push_next_variable();
                }
                Search::Draw => self.draw_batch(),
                Search::Emit => {
                    let frontier = self.bindings.frontier();
                    while self.emit < frontier.len() {
                        let row = frontier.row(self.emit);
                        self.emit += 1;
                        if let Some(result) = (self.postprocessing)(&row) {
                            return Some(result);
                        }
                        // Post-processing rejected this row; try the next.
                    }
                    self.mode = Search::Retreat;
                }
                Search::Retreat => self.retreat(),
                Search::Pop => self.pop_level(),
                Search::Done => return None,
            }
        }
    }
}
impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> fmt::Debug for Query<C, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("constraint", &std::any::type_name::<C>())
            .field("mode", &self.mode)
            .field("frontier", &self.bindings.frontier())
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
// The producer's `split` divides a FRONTIER. It first settles the state
// machine onto a batch (descending through levels whose batch is too
// narrow to divide), then either bisects that batch's rows between two
// sub-queries, or — when the batch is a single row — hands the right half
// every level's *undrawn* candidates and seals the left half so it
// explores only the batch it is holding. Either way the two halves
// partition the remaining search exactly: no candidate is drawn twice and
// none is dropped.
//
// Bindings are indexes into the level buffers, and a split does not
// disturb them. `Query::clone` copies those buffers element for element,
// so a row's indexes resolve to the same values in either half; splitting
// partitions ROWS and never re-indexes an entry. (The one-at-a-time
// splitter had to bisect a level's buffer and re-base the tail, which is
// why it needed a matching `unset`.)
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
    pub struct QueryParIter<C, P: Fn(&Binding<'_>) -> Option<R>, R> {
        inner: Box<Query<C, P, R>>,
        split_budget: usize,
    }

    impl<C, P: Fn(&Binding<'_>) -> Option<R>, R> QueryParIter<C, P, R> {
        /// Splits the remaining split budget between this producer and
        /// `right`, and packages the pair rayon expects.
        fn hand_off(mut self, right: Query<C, P, R>) -> (Self, Option<Self>) {
            self.split_budget -= 1;
            let left_budget = self.split_budget / 2;
            let right_budget = self.split_budget - left_budget;
            self.split_budget = left_budget;
            (
                self,
                Some(QueryParIter {
                    inner: Box::new(right),
                    split_budget: right_budget,
                }),
            )
        }
    }

    impl<'a, C, P, R> IntoParallelIterator for Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding<'_>) -> Option<R> + Clone + Send,
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
        P: Fn(&Binding<'_>) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;

        /// Settle the state machine onto a batch, then divide it.
        ///
        /// Returns a right half whenever the remaining search can be cut in
        /// two, and `(self, None)` when it cannot — leaving `self` as a leaf
        /// that `fold_with` folds sequentially.
        fn split(mut self) -> (Self, Option<Self>) {
            if self.split_budget == 0 {
                return (self, None);
            }
            let q = &mut *self.inner;
            loop {
                // Settle onto a batch: `Expand` and `Emit` are the two modes
                // where the deepest frontier IS the live batch. The others
                // are transitions, and stepping through them is ordinary
                // search work that has to happen anyway.
                match q.mode {
                    Search::Draw => {
                        q.draw_batch();
                        continue;
                    }
                    Search::Retreat => {
                        q.retreat();
                        continue;
                    }
                    Search::Pop => {
                        q.pop_level();
                        continue;
                    }
                    Search::Done => return (self, None),
                    Search::Expand | Search::Emit => {}
                }

                // Rows already emitted from this batch must stay with the
                // left half.
                let from = if matches!(q.mode, Search::Emit) {
                    q.emit
                } else {
                    0
                };

                // Preferred cut: halve the live batch. Both halves keep
                // element-wise identical level buffers, so the tail rows'
                // indexes resolve unchanged — only rows move.
                if let Some(tail) = q.bindings.split_frontier(from) {
                    let mut right = q.clone();
                    right.bindings.set_frontier(tail);
                    right.emit = 0;
                    // The left half explores only the rows it kept.
                    q.seal_levels();
                    return self.hand_off(right);
                }

                // Fallback cut: the batch is one row, but levels above may
                // still hold undrawn candidates. Give those to the right
                // half and let the left finish the single row it holds.
                if q.has_undrawn() {
                    let mut right = q.clone();
                    // The right half owns no part of the current batch; it
                    // retreats straight into the next one.
                    right.bindings.set_frontier(Vec::new());
                    right.emit = 0;
                    right.mode = Search::Retreat;
                    q.seal_levels();
                    return self.hand_off(right);
                }

                // Nothing to divide here. Descend if there is anywhere to
                // descend to, otherwise this producer is a leaf.
                match q.mode {
                    Search::Expand if !q.unbound.is_empty() => q.push_next_variable(),
                    _ => return (self, None),
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
        P: Fn(&Binding<'_>) -> Option<R> + Clone + Send,
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
/// [`pattern!`](crate::macros::pattern) and [`temp!`](crate::temp).
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
/// Query heads have BAG semantics: one row per complete satisfying assignment.
/// Two assignments that agree on every declared head variable but differ in
/// hidden variables therefore produce two rows — hidden-variable multiplicity
/// is visible in the output, and the engine performs no deduplication. That is
/// deliberate: deduplicating would mean carrying a claim table proportional to
/// the distinct result set, sharing it across rayon shards, and deciding what a
/// panicking or filtered row does to a claimed key. Dedup is the consumer's
/// choice instead: collect into a set, or use two queries (an outer enumeration
/// over the values you want distinct, with an inner [`exists!`](crate::exists)
/// for the witness). Every projected variable must be unique; repeating a
/// variable in the head is a compile error because it would not add a column.
///
/// The unit form `find!((), constraint)` projects no variables and yields one
/// `()` per satisfying assignment — it counts witnesses rather than answering a
/// yes/no question. Use [`exists!`](crate::exists) for existence, which stops
/// at the first witness.
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

    fn variable_set(indices: impl IntoIterator<Item = VariableId>) -> VariableSet {
        let mut variables = VariableSet::new_empty();
        for variable in indices {
            variables.set(variable);
        }
        variables
    }


















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





    #[derive(Clone)]
    struct SetAdmissionProbe {
        descendants: std::sync::Arc<std::sync::Mutex<Vec<RawInline>>>,
    }

    impl SetAdmissionProbe {
        const ROOT: VariableId = 0;
        const LEAF: VariableId = 1;
        const A: RawInline = [4; 32];
        const B: RawInline = [5; 32];
        const LEAF_VALUE: RawInline = [6; 32];
    }


}
