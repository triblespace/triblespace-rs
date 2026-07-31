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
/// [`ProposalBuffer`] and [`Candidates`] — candidate storage and bit-packed
/// liveness for one search level.
mod liveness;
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
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrayvec::ArrayVec;
use constantconstraint::*;

use crate::inline::encodings::genid::GenId;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;

/// Re-export of [`VariableSet`](variableset::VariableSet).
pub use variableset::VariableSet;

/// Re-exports of the candidate/liveness pair. The module they come from is
/// private on purpose: the bit-packed representation and the region-boundary
/// invariants that keep it honest are enforced at the boundary of these two
/// types, so nothing outside `query::liveness` can reach them. Its
/// module-level comment is the reference for anyone editing it — or writing a
/// device kernel against [`LIVENESS_WORD_BITS`].
pub use liveness::{and_words, or_words, Candidates, ProposalBuffer, LIVENESS_WORD_BITS};

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
///   binding. Nothing ever appends to a level while its variable is
///   bound, so the stability is unconditional (see
///   [`BindingStore::refill`]).
/// * Buffers are write-once: confirmers kill entries by clearing a
///   parallel liveness word, and nothing ever moves or rewrites a stored
///   value once the engine can see it.
///
/// `Binding` is therefore a *view* — the index row plus a borrow of the
/// buffers it indexes into — constructed for the duration of one
/// constraint call. [`BindingStore`] owns the buffers; the index rows of
/// a whole batch live in a [`Frontier`].
///
/// The payoff is size: an assignment is one `u32` per variable slot
/// instead of a 32-byte raw value each, a bind is a 4-byte write instead
/// of a 32-byte copy, cloning the search state no longer memcpies the
/// values, and a *batch* of bindings is a small integer matrix over
/// shared buffers rather than a pile of value copies.
#[derive(Clone, Copy)]
pub struct Binding<'a> {
    /// Bitset tracking which variables have been assigned a value.
    pub bound: VariableSet,
    indexes: &'a [u32],
    levels: &'a [LevelValues],
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
            Some(&self.levels[variable].buffer[self.indexes[variable] as usize])
        } else {
            None
        }
    }
}

/// Backing for the empty [`Binding`]: nothing is bound, so nothing ever
/// resolves through them, but a view needs something to point at.
static NO_LEVELS: [LevelValues; 128] = [const { LevelValues::empty() }; 128];
static NO_INDEXES: [u32; 128] = [0; 128];
/// The one-row selection a width-1 [`Frontier`] uses.
static SINGLE_ROW: [u32; 1] = [0];

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

/// A **batch of parent bindings** — the collection [`Constraint::propose`]
/// expands and [`Constraint::confirm`] filters against.
///
/// Because bindings are indexes rather than values, a frontier is an
/// *index matrix*, not a pile of copied assignments: one row of `stride`
/// `u32`s per parent binding, over the level buffers those indexes point
/// into. A sub-batch is expressed by a `select` array of row numbers, so
/// narrowing a frontier (the engine splitting it by preferred variable,
/// an intersection splitting it by preferred proposer) costs one `u32`
/// per row and never copies a row.
///
/// Every row of a frontier has the *same* [`bound`](Frontier::bound) set —
/// they are all at the same point in the search, differing only in which
/// values they took. Today's single-binding call sites are a frontier of
/// one; see [`BindingStore::frontier`].
#[derive(Clone, Copy)]
pub struct Frontier<'a> {
    bound: VariableSet,
    block: &'a [u32],
    stride: usize,
    select: &'a [u32],
    levels: &'a [LevelValues],
}

impl fmt::Debug for Frontier<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.rows()).finish()
    }
}

impl Default for Frontier<'_> {
    /// A batch of exactly one empty binding — the root of every search,
    /// and the shape a caller with nothing bound hands a constraint.
    fn default() -> Self {
        Frontier {
            bound: VariableSet::new_empty(),
            block: &NO_INDEXES,
            stride: NO_INDEXES.len(),
            select: &SINGLE_ROW,
            levels: &NO_LEVELS,
        }
    }
}

impl<'a> Frontier<'a> {
    /// Number of parent bindings in this batch.
    pub fn len(&self) -> usize {
        self.select.len()
    }

    /// True when the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.select.is_empty()
    }

    /// The variables bound in *every* row of this batch.
    pub fn bound(&self) -> VariableSet {
        self.bound
    }

    /// The binding of row `i`.
    pub fn row(&self, i: usize) -> Binding<'a> {
        Binding {
            bound: self.bound,
            indexes: self.row_indexes(i),
            levels: self.levels,
        }
    }

    /// Iterates the batch's bindings in row order.
    pub fn rows(&self) -> impl Iterator<Item = Binding<'a>> + '_ {
        (0..self.len()).map(|i| self.row(i))
    }

    fn row_indexes(&self, i: usize) -> &'a [u32] {
        let row = self.select[i] as usize;
        &self.block[row * self.stride..(row + 1) * self.stride]
    }

    /// Translates `positions` (row numbers *in this view*) into row
    /// numbers of the underlying block, appending them to `out`.
    ///
    /// The result is the `select` array of a sub-batch: pair it with
    /// [`with_select`](Frontier::with_select) to hand a subset of this
    /// frontier to a child constraint without copying any row.
    pub fn compose(&self, positions: impl IntoIterator<Item = u32>, out: &mut Vec<u32>) {
        out.extend(positions.into_iter().map(|p| self.select[p as usize]));
    }

    /// This frontier restricted to `select` — row numbers of the
    /// underlying block, as produced by [`compose`](Frontier::compose).
    pub fn with_select<'b>(&self, select: &'b [u32]) -> Frontier<'b>
    where
        'a: 'b,
    {
        Frontier {
            bound: self.bound,
            block: self.block,
            stride: self.stride,
            select,
            levels: self.levels,
        }
    }
}

/// Owns what a [`Binding`] is a view of: the per-variable level buffers,
/// plus the index row that picks one entry out of each for callers
/// outside the search.
///
/// The query engine keeps its frontiers' index matrices separately (they
/// are per search *depth*, while buffers are per *variable*) and reaches
/// the buffers through [`refill`](BindingStore::refill) and
/// [`take_chunk`](BindingStore::take_chunk). Code outside the engine
/// builds a binding over values it picked itself — see
/// [`bind`](BindingStore::bind) — and turns it into a width-1
/// [`Frontier`] with [`frontier`](BindingStore::frontier).
#[derive(Clone)]
pub struct BindingStore {
    bound: VariableSet,
    indexes: [u32; 128],
    levels: [LevelValues; 128],
}

impl fmt::Debug for BindingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.view(), f)
    }
}

impl Default for BindingStore {
    fn default() -> Self {
        BindingStore {
            bound: VariableSet::new_empty(),
            indexes: [0; 128],
            levels: std::array::from_fn(|_| LevelValues::default()),
        }
    }
}

impl BindingStore {
    /// An empty store: nothing bound, every level empty.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current assignment, as [`satisfied`](Constraint::satisfied)
    /// and result post-processing see it.
    pub fn view(&self) -> Binding<'_> {
        Binding {
            bound: self.bound,
            indexes: &self.indexes,
            levels: &self.levels,
        }
    }

    /// The current assignment as a **frontier of one** — the batch shape
    /// [`propose`](Constraint::propose) and [`confirm`](Constraint::confirm)
    /// take. This is the bridge for every caller that has a single
    /// binding: a collection of one behaves exactly like the old
    /// single-binding protocol.
    pub fn frontier(&self) -> Frontier<'_> {
        Frontier {
            bound: self.bound,
            block: &self.indexes,
            stride: self.indexes.len(),
            select: &SINGLE_ROW,
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
        let index = level.buffer.len();
        level.buffer.push(*value);
        self.indexes[variable] = index as u32;
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

    /// Live candidates still pending at `variable`'s level.
    #[cfg(feature = "parallel")]
    fn pending(&self, variable: VariableId) -> usize {
        let level = &self.levels[variable];
        level.buffer.count_live(level.pos)
    }

    /// Clears `variable`'s level and refills it by proposing over the
    /// frontier `block`/`select`/`stride` describe.
    ///
    /// The level is moved *out* of the array for the duration of the
    /// call, so `propose` gets `&mut` on it while the remaining levels
    /// lend immutably through the [`Frontier`] view — the borrow split
    /// the search needs, with no `unsafe` and no second copy of the
    /// candidates. This is sound precisely because the engine only ever
    /// refills the level of a variable it is about to push, which is by
    /// construction unbound: nothing can be resolving through this level
    /// while it is away.
    ///
    /// This is now the *only* way a level's buffer is written. With the
    /// resumable-narrowing path gone, no level is ever appended to while
    /// its variable is bound, so "a bound variable's buffer is stable for
    /// the lifetime of its binding" holds unconditionally — the
    /// `debug_assert` below is that invariant.
    pub(crate) fn refill(
        &mut self,
        variable: VariableId,
        block: &[u32],
        select: &[u32],
        stride: usize,
        propose: impl FnOnce(&Frontier<'_>, &mut ProposalBuffer),
    ) -> usize {
        debug_assert!(
            !self.bound.is_set(variable),
            "refilling a bound variable's level would strand its binding"
        );
        let mut level = std::mem::take(&mut self.levels[variable]);
        level.buffer.clear();
        level.pos = 0;
        propose(
            &Frontier {
                bound: self.bound,
                block,
                stride,
                select,
                levels: &self.levels,
            },
            &mut level.buffer,
        );
        let proposed = level.buffer.len();
        self.levels[variable] = level;
        proposed
    }

    /// A [`Frontier`] over the rows `block`/`select`/`stride` describe,
    /// resolving through this store's level buffers.
    pub(crate) fn batch<'s>(
        &'s self,
        block: &'s [u32],
        select: &'s [u32],
        stride: usize,
    ) -> Frontier<'s> {
        Frontier {
            bound: self.bound,
            block,
            stride,
            select,
            levels: &self.levels,
        }
    }

    /// How far `variable`'s level cursor has run — the count of entries the
    /// level has consumed or skipped. With `Level::proposed` this bounds the
    /// candidates still pending without scanning the liveness words.
    pub(crate) fn consumed(&self, variable: VariableId) -> usize {
        self.levels[variable].pos
    }

    /// Consumes up to `width` further live candidates from `variable`'s
    /// level and writes the child frontier's rows into `out`, recording
    /// each child row's parent row number in `parents_out`.
    ///
    /// Each consumed candidate `i` carries the parent tag its proposer
    /// wrote; the child row is that parent's index row with `variable`'s
    /// slot pointed at `i`. Returns how many rows were produced (zero
    /// means the level is spent).
    ///
    /// This is the fused path: one pass, no intermediate. A descent that
    /// might be 1:1 uses [`draw`](BindingStore::draw) instead, which defers
    /// the write until the shape of the draw is known.
    pub(crate) fn take_chunk(
        &mut self,
        variable: VariableId,
        width: usize,
        parent_block: &[u32],
        parent_select: &[u32],
        stride: usize,
        out: &mut Vec<u32>,
        parents_out: &mut Vec<u32>,
    ) -> usize {
        out.clear();
        parents_out.clear();
        let level = &mut self.levels[variable];
        let mut rows = 0;
        while rows < width {
            let Some(i) = level.buffer.next_live(level.pos) else {
                break;
            };
            level.pos = i + 1;
            let parent = parent_select[level.buffer.parent_of(i) as usize] as usize;
            out.extend_from_slice(&parent_block[parent * stride..(parent + 1) * stride]);
            let base = out.len() - stride;
            out[base + variable] = i as u32;
            parents_out.push(parent as u32);
            rows += 1;
        }
        self.bound.set_value(variable, rows != 0);
        rows
    }

    /// Consumes up to `width` further live candidates from `variable`'s
    /// level *without* materialising any child row: each drawn entry's
    /// index lands in `drawn_out` and the frontier row it was proposed for
    /// in `parents_out`.
    ///
    /// Deferring the write is what lets the engine look at the shape of a
    /// draw before paying for it — a 1:1 draw needs no new rows at all,
    /// see [`Query::next_chunk`]. It is not free (two passes instead of
    /// one), which is exactly why the caller only takes this path when a
    /// 1:1 draw is *possible*.
    pub(crate) fn draw(
        &mut self,
        variable: VariableId,
        width: usize,
        parent_select: &[u32],
        drawn_out: &mut Vec<u32>,
        parents_out: &mut Vec<u32>,
    ) {
        drawn_out.clear();
        parents_out.clear();
        let level = &mut self.levels[variable];
        while drawn_out.len() < width {
            let Some(i) = level.buffer.next_live(level.pos) else {
                break;
            };
            level.pos = i + 1;
            drawn_out.push(i as u32);
            parents_out.push(parent_select[level.buffer.parent_of(i) as usize]);
        }
        self.bound.set_value(variable, !drawn_out.is_empty());
    }

    /// Whether `variable`'s level has no live candidate left beyond what
    /// has already been drawn.
    ///
    /// Deliberately *not* folded into [`draw`](BindingStore::draw): it is a
    /// forward scan over the liveness words, so asking after every draw
    /// would pay a second pass over a level's dead tail that the next draw
    /// pays anyway. The one caller asks only once the cheap `O(1)`
    /// conditions for an in-place descent already hold.
    pub(crate) fn spent(&self, variable: VariableId) -> bool {
        let level = &self.levels[variable];
        level.buffer.next_live(level.pos).is_none()
    }

    /// Bisects `variable`'s materialized region, returning the tail as a
    /// fresh level for the right half of a parallel split.
    ///
    /// The left half keeps entries `[0..mid)`, and every consumed entry
    /// (and hence every index any binding holds for this level) sits
    /// below `pos <= mid` — so the left half's indexes stay valid. The
    /// returned tail re-indexes from zero, which is why the right half
    /// must [`unset`](BindingStore::unset) `variable` when it installs
    /// it. The parent tags travel with the values and still address the
    /// same fenced parent frontier in the right half.
    #[cfg(feature = "parallel")]
    fn bisect(&mut self, variable: VariableId) -> LevelValues {
        let level = &mut self.levels[variable];
        let pending_start = level
            .buffer
            .next_live(level.pos)
            .expect("bisect requires pending candidates");
        let mid = pending_start + (level.buffer.len() - pending_start) / 2;
        LevelValues {
            buffer: level.buffer.split_off(mid),
            pos: 0,
        }
    }

    /// Installs `level` as `variable`'s level and unbinds `variable`:
    /// the incoming buffer has its own coordinates, so any index a
    /// frontier row still holds for it is meaningless.
    #[cfg(feature = "parallel")]
    fn install(&mut self, variable: VariableId, level: LevelValues) {
        self.levels[variable] = level;
        self.bound.unset(variable);
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
/// | [`propose`](Constraint::propose) | Enumerates candidate values for a variable, for a whole batch of bindings. | On the most selective constraint. |
/// | [`confirm`](Constraint::confirm) | Filters candidates proposed by another constraint. | On all remaining constraints. |
/// | [`satisfied`](Constraint::satisfied) | Checks whether fully-bound sub-constraints still hold. | Before propose/confirm in composite constraints. |
///
/// [`influence`](Constraint::influence) completes the picture by telling the
/// engine which estimates to refresh when a variable is bound or unbound.
///
/// # Batching
///
/// `propose` and `confirm` operate on a **[`Frontier`]** — a collection of
/// parent bindings — rather than on one binding. Today's single-binding
/// call sites are a frontier of one ([`BindingStore::frontier`]) and behave
/// exactly as before; the engine drives wider batches so that every level
/// of the search offers a source a *wide* region to work on, not just the
/// root. A source that has nothing to gain from width writes the loop over
/// [`Frontier::rows`] and is done; one that does (a GPU-resident index, a
/// SIMD probe) sees the whole batch in one call. Nothing in the protocol
/// requires a source to seek: iteration and point queries still suffice,
/// which is what keeps hash maps, PATCHes, succinct archives and
/// device-resident structures all admissible.
///
/// A batch is only a physical execution unit. Constraints obey the
/// **row-fiber law**: selecting any subset of a frontier, processing it
/// independently, and lifting its local parent tags back to the original
/// rows produces the same tagged bag of candidates (up to order) as
/// processing those rows inside the full frontier. In particular, another
/// row and the batch width may not change either the candidates proposed for
/// one row or a confirmer's verdict on them. This is what lets the engine
/// partition parent rows for latency and Rayon without changing semantics.
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
    ///
    /// Whether the answer is `Some` or `None` must depend only on which
    /// variables are bound, not on the values they are bound to: every row
    /// of a [`Frontier`] shares one bound set, and composites read
    /// relevance off the batch.
    fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize>;

    /// Enumerates candidate values for `variable`, for every row of
    /// `frontier`, into `proposals`.
    ///
    /// Called on the constraint with the lowest estimate for the variable
    /// being bound. Before appending the candidates of row `r`, the
    /// proposer calls [`ProposalBuffer::open`] with `r`, which tags them so
    /// confirmers and the engine can tell whose they are. A proposer may
    /// visit the rows in any order and interleave them; contiguous segments
    /// are merely the cheaper shape.
    ///
    /// Values are appended to `proposals`; entries appended by an enclosing
    /// composite's other children may precede them, and must be left
    /// untouched.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    );

    /// Kills [`Candidates`] entries whose values for `variable` violate
    /// this constraint under the entry's own frontier row.
    ///
    /// Called on every constraint *except* the one that proposed, in order
    /// of increasing estimate, all killing into the same region —
    /// sequential kills compute the conjunction. Implementations may only
    /// kill entries, never revive them, and may skip entries that are
    /// already dead. Nothing is ever compacted: dead entries stay in place
    /// and the engine iterates live ones.
    ///
    /// The region spans the whole batch: entry `i` belongs to frontier row
    /// [`cands.parent(i)`](Candidates::parent). A confirmer whose verdict
    /// depends on the parent binding walks the region with
    /// [`Candidates::for_each_parent`]; one whose verdict does not simply
    /// ignores the tags.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>);

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

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let inner: &T = self;
        inner.propose(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, frontier, cands)
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

    fn propose(
        &self,
        variable: VariableId,
        frontier: &Frontier<'_>,
        proposals: &mut ProposalBuffer,
    ) {
        let inner: &T = self;
        inner.propose(variable, frontier, proposals)
    }

    fn confirm(&self, variable: VariableId, frontier: &Frontier<'_>, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, frontier, cands)
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

/// Per-variable candidate storage for one search level: the proposals for
/// the whole batch that produced them, plus how far the engine has consumed
/// them. Slots are indexed by [`VariableId`], so siblings — different
/// variables chosen at the same depth — never share one; what a slot is
/// reused for is the *next* binding of its own variable, whose refill keeps
/// the buffer's capacity.
#[derive(Clone, Debug)]
struct LevelValues {
    buffer: ProposalBuffer,
    /// Consumption position: entries before it are consumed, live entries at
    /// or after it are pending. Dead entries are skipped, never moved.
    pos: usize,
}

impl LevelValues {
    /// An empty level, const-constructible so the empty [`Binding`] can
    /// borrow a `static` array of them.
    const fn empty() -> Self {
        LevelValues {
            buffer: ProposalBuffer::new(),
            pos: 0,
        }
    }
}

impl Default for LevelValues {
    fn default() -> Self {
        Self::empty()
    }
}

/// Widest frontier the engine expands at once.
///
/// The search keeps a *frontier* — all the rows sitting at one point of the
/// tree — and expands up to this many of them in a single
/// [`propose`](Constraint::propose)/[`confirm`](Constraint::confirm) pass.
/// Width is what makes a level's candidate region large: with a width-1
/// frontier only the root level ever proposes widely, and every deeper level
/// hands a source a handful of candidates for one parent — measured on real
/// data, a median region of 1–7 candidates at every scale, which is far
/// below any batch-dispatch threshold.
///
/// 16384 is a batching and memory ceiling, independent of accelerator
/// placement. The WGPU succinct adapter selects its own measured floor from
/// the operation it is asked to perform: range confirms amortise earlier
/// than lighter membership probes. A wide frontier makes those tiers
/// reachable below the root without coupling this engine constant to either
/// device crossover.
///
/// It is a *ceiling*, not the first batch: the first parent-source page is
/// [`INITIAL_FRONTIER_WIDTH`], then page width grows by
/// [`FRONTIER_RAMP_BASE`] until it reaches this ceiling. Before the first
/// caller-visible row, every candidate level starts scalar. After that
/// boundary a candidate level reuses its source page's width as already-paid
/// source work; subsequent candidate chunks keep widening by the same
/// schedule. A conservative tail merge may consume the remainder early, but
/// never exceeds the ceiling.
///
/// The cost is frontier memory: one index row plus one estimate row per
/// live row per depth, i.e. `O(width · variables · depth)` — the price of
/// trading depth-first's `O(depth)` frontier for a wide one. Worst-case
/// optimality is untouched: expanding N prefixes together is the same total
/// work as expanding them one at a time, and the AGM bound is a statement
/// about output size, not traversal order.
///
/// Tune per query with [`Query::with_frontier_width`].
pub const DEFAULT_FRONTIER_WIDTH: usize = 16384;

/// Width of the first parent-source page. Later pages grow by
/// [`FRONTIER_RAMP_BASE`] up to [`DEFAULT_FRONTIER_WIDTH`] (or whatever
/// [`Query::with_frontier_width`] set). Once a query has emitted a row, a
/// level opened for a wider page starts its candidate drain at that page's
/// width: `propose`/`confirm` already paid for those parent rows, so restarting
/// a second ramp at one would discard useful amortisation credit. This is an
/// execution policy, not a claim that the caller requested every child.
///
/// Before the first caller-visible row, every new candidate level instead
/// starts at one. Internal source pages may widen while searching, but their
/// width cannot make the eventual first completion materialise an unused
/// child suffix. This is a semantic phase boundary rather than a cardinality
/// threshold: `.next()` and `exists!` retain depth-first granularity through
/// arbitrary failing prefixes, then sustained demand can reuse paid work.
/// Measured on a dense first-row join, a flat full-width engine is **8.8x**
/// slower than the pre-batching engine; the pre-yield fence closes that gap.
///
/// This is the same insight as the `INITIAL_CHUNK`/`WIDEN_FACTOR` pair this
/// branch deleted, and as the deleted residual engine's rule that search
/// width grows geometrically after negative work — recovered at the right
/// layer (the frontier) instead of the wrong one (per-parent chunking).
///
/// The pre-yield fence protects `.next()` and `exists!`; after the first
/// result the ramp trades low-demand granularity against full-drain batch
/// width. The base controls that trade. Doubling was measured and rejected
/// because its final chunk is only about half a drain, but base eight retains
/// seven eighths
/// asymptotically while reaching the ceiling in far fewer steps. See
/// [`FRONTIER_RAMP_BASE`] for the measured rationale.
pub const INITIAL_FRONTIER_WIDTH: usize = 1;

/// Factor a level's chunk width grows by after each chunk, from
/// [`INITIAL_FRONTIER_WIDTH`] up to the query's ceiling.
///
/// # Why a base, and why not two
///
/// A ramp's cost is not its number of steps, it is the size of its LAST
/// chunk. Ramping by `b` consumes `1 + b + b^2 + … + b^k`, of which the
/// final chunk `b^k` is `(b-1)/b` of the total — so the widest frontier a
/// level can build is `N - N/b`, and the peak is what batching exists to
/// produce. At `b = 2` that is `N/2`: doubling throws away half the peak,
/// which is the whole of the measured 2048 -> 512 that got the geometric
/// ramp rejected. The failure was the base, not the ramp.
///
/// At `b = 8` the peak is `7N/8` and a level reaches a 16384 ceiling in
/// `log8(16384) ~ 4.7` chunks rather than doubling's fourteen — the
/// amortised expansion overhead stops mattering well before the peak does.
///
/// A base at or above the ceiling reproduces the one-narrow-chunk-then-
/// ceiling schedule this replaced, which is the control arm to measure
/// against. Base `1` is NOT that control — `width * 1` never grows, so it
/// pins the engine to a width-1 frontier and reproduces the pre-batching
/// engine instead. Both are useful arms; they are not the same arm.
pub const FRONTIER_RAMP_BASE: usize = 8;

/// Advances one geometric width after `remaining` entries have been left.
///
/// The same schedule governs both dimensions of the search: how many
/// confirmed candidates a level hands down, and how many parent rows a
/// preferred-variable group hands to one atomic `propose`/`confirm` pass.
/// Keeping the policy in one place is load-bearing: otherwise a narrow
/// candidate chunk can still hide a full-width proposal at the next depth.
fn widened_width(current: usize, remaining: usize, ceiling: usize) -> usize {
    let next = current.saturating_mul(FRONTIER_RAMP_BASE).min(ceiling);
    if remaining < next.saturating_mul(2) {
        remaining.max(next).min(ceiling)
    } else {
        next
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
    /// Whether this iterator has returned at least one caller-visible row.
    /// Before that semantic boundary candidate drains stay scalar; afterward
    /// they may reuse an already-materialised source page as work credit.
    has_emitted: bool,
    bindings: BindingStore,
    influences: [VariableSet; 128],
    unbound: VariableSet,
    /// Index-row width: one slot per variable the query mentions.
    slots: usize,
    width: usize,
    stack: ArrayVec<Level, 128>,
    /// Frontier stack. `depths[0]` is the root (one empty row); `depths[d]`
    /// holds the rows with `d` variables bound. Entries above `depth` are
    /// retired but keep their allocations for the next descent.
    depths: Vec<Depth>,
    depth: usize,
    /// Per-row preferred variable, rebuilt on every expansion.
    choice: Vec<u32>,
    /// Scratch: the level-buffer entry index of each freshly-drawn child row.
    drawn: Vec<u32>,
    /// Scratch: each freshly-drawn child row's parent row number.
    parents: Vec<u32>,
    stats: Arc<FrontierStats>,
}

/// One pushed search level: the variable it binds, how wide its *next*
/// chunk may be, and how many candidates its proposer produced.
///
/// The width is per level rather than per query because, once the query has
/// emitted a row, it starts with the parent source page that opened this
/// level and then records how far its own drain has widened. Before the first
/// caller-visible row every new level starts scalar, even if internal failures
/// have already widened the source schedule. This is only an execution
/// partition: the row-fiber law preserves the exact tagged bag, while result
/// order is not part of the contract. See [`INITIAL_FRONTIER_WIDTH`].
///
/// `proposed` is the `O(1)` gate on the in-place descent: a level with more
/// candidates than parent rows cannot possibly yield one child per parent,
/// so the engine never pays to check. See [`Query::next_chunk`].
#[derive(Clone, Copy, Debug)]
struct Level {
    variable: VariableId,
    width: usize,
    proposed: usize,
    /// Half-open slice of the parent depth's `order` expanded by this
    /// source. A preferred-variable group may be exposed in several
    /// geometrically growing pages; the level owns exactly one of them.
    source_start: usize,
    source_end: usize,
    source_group: usize,
}

/// Empties one of a [`Depth`]'s `Arc`-shared buffers for rewriting, without
/// copying the contents it is about to discard.
///
/// `Arc::make_mut` deep-clones whenever the `Arc` is shared — which is the
/// normal state right after a rayon split, since splitting hands both halves
/// the same matrices — and every caller here clears the result on the next
/// line. That copy is pure waste, and it is proportional to the frontier:
/// `rows * slots` entries, so it grows with exactly the width the engine
/// exists to make large.
///
/// When we are the sole owner the allocation is reused. When we are not, a
/// fresh empty buffer replaces our handle and the other half keeps the old
/// one untouched, which is the same outcome `make_mut` produces minus the
/// memcpy.
fn reset_shared<T: Clone>(slot: &mut Arc<Vec<T>>, capacity: usize) -> &mut Vec<T> {
    if Arc::get_mut(slot).is_none() {
        *slot = Arc::new(Vec::with_capacity(capacity));
    }
    let buffer = Arc::get_mut(slot).expect("sole owner after replacement");
    buffer.clear();
    buffer.reserve(capacity);
    buffer
}

/// One frontier: the index matrix of the rows sitting at one point of the
/// search, their per-row estimates, and the partition into groups that
/// agree on which variable to bind next.
#[derive(Clone, Debug, Default)]
struct Depth {
    /// Row-major index matrix, `rows * slots` entries.
    block: Arc<Vec<u32>>,
    rows: usize,
    /// Row-major estimate matrix, `rows * slots` entries.
    estimates: Arc<Vec<usize>>,
    /// Row numbers, grouped by preferred variable.
    order: Arc<Vec<u32>>,
    /// `(variable, end offset into order)`, one per group.
    groups: Arc<Vec<(VariableId, usize)>>,
    /// Current group to expand (or the next unstarted group when
    /// `group_row == 0`).
    group: usize,
    /// Rows already exposed from `group` and the width of its next source
    /// page. Unlike candidate chunking, this bounds the work paid *inside*
    /// one atomic `Constraint::propose` call.
    group_row: usize,
    group_width: usize,
    /// Emission cursor, used only when every variable is bound.
    emit: usize,
}

impl Depth {
    /// The half-open slice of `order` belonging to group `g`.
    fn group_range(&self, g: usize) -> std::ops::Range<usize> {
        let start = if g == 0 { 0 } else { self.groups[g - 1].1 };
        start..self.groups[g].1
    }
}

/// Observational counters for the batched search — how wide the frontier
/// actually got, and how often its rows disagreed about which variable to
/// bind next.
///
/// Fragmentation is the interesting number:
/// [`mean_variable_groups`](FrontierStats::mean_variable_groups) is 1.0 when
/// every row of every frontier agreed (the batch stayed whole) and rises as
/// bindings pull rows onto different adaptive choices. It is measured
/// rather than assumed because a row is never moved onto a variable it did
/// not choose — see [`Query::plan`].
///
/// Counters use relaxed atomics and are shared with every rayon clone of
/// the query, so a snapshot taken after the iterator is exhausted covers
/// the whole (possibly parallel) run.
#[derive(Debug, Default)]
pub struct FrontierStats {
    expansions: AtomicU64,
    rows: AtomicU64,
    variable_groups: AtomicU64,
    proposals: AtomicU64,
    widest: AtomicU64,
    inplace_descents: AtomicU64,
    copied_descents: AtomicU64,
}

impl FrontierStats {
    /// Number of frontier expansions (one per batch of parent rows).
    pub fn expansions(&self) -> u64 {
        self.expansions.load(Ordering::Relaxed)
    }

    /// Total parent rows expanded, summed over expansions.
    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Total preferred-variable groups, summed over expansions. Equal to
    /// [`expansions`](Self::expansions) when no frontier ever fragmented.
    pub fn variable_groups(&self) -> u64 {
        self.variable_groups.load(Ordering::Relaxed)
    }

    /// Total candidates proposed across all levels.
    pub fn proposals(&self) -> u64 {
        self.proposals.load(Ordering::Relaxed)
    }

    /// Rows in the widest single expansion — the widest frontier the
    /// search actually reached.
    ///
    /// [`mean_width`](Self::mean_width) says what the typical expansion
    /// looked like; this says whether the ceiling was ever approached at
    /// all. The difference matters when reading a benchmark: a query whose
    /// widest frontier is far below `DEFAULT_FRONTIER_WIDTH` cannot
    /// demonstrate anything about batch-width thresholds, however the
    /// engine behaves — the data simply never filled a batch.
    pub fn widest(&self) -> u64 {
        self.widest.load(Ordering::Relaxed)
    }

    /// Descents that reused the parent frontier's matrices in place — the
    /// 1:1 case, where the child block *is* the parent block with one more
    /// slot filled in and nothing is allocated or copied.
    pub fn inplace_descents(&self) -> u64 {
        self.inplace_descents.load(Ordering::Relaxed)
    }

    /// Descents that had to build a fresh child frontier by copying each
    /// row of the parent.
    pub fn copied_descents(&self) -> u64 {
        self.copied_descents.load(Ordering::Relaxed)
    }

    /// Mean rows per expansion — the width the search actually achieved.
    pub fn mean_width(&self) -> f64 {
        let expansions = self.expansions();
        if expansions == 0 {
            0.0
        } else {
            self.rows() as f64 / expansions as f64
        }
    }

    /// Mean preferred-variable groups per expansion; 1.0 means no frontier
    /// ever fragmented.
    pub fn mean_variable_groups(&self) -> f64 {
        let expansions = self.expansions();
        if expansions == 0 {
            0.0
        } else {
            self.variable_groups() as f64 / expansions as f64
        }
    }
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
            has_emitted: self.has_emitted,
            bindings: self.bindings.clone(),
            influences: self.influences,
            unbound: self.unbound,
            slots: self.slots,
            width: self.width,
            stack: self.stack.clone(),
            // Only the live prefix of the frontier stack matters; the
            // retired tail is scratch. Each `Depth`'s matrices sit behind
            // `Arc`, so a split copies refcounts, not megabytes — the
            // copy-on-write lands on whichever half rewrites a frontier
            // first.
            depths: self.depths[..=self.depth].to_vec(),
            depth: self.depth,
            choice: Vec::new(),
            drawn: Vec::new(),
            parents: Vec::new(),
            stats: Arc::clone(&self.stats),
        }
    }
}

#[cfg(feature = "parallel")]
impl<C, P, R> Query<C, P, R>
where
    C: Clone,
    P: Fn(&Binding<'_>) -> Option<R> + Clone,
{
    /// Bisects the pending candidates of the current source, leaving the
    /// prefix in `self` and returning the suffix as a fenced sibling query.
    ///
    /// Only the left query owns the continuation after this source: later
    /// groups of the parent frontier and every ancestor sibling. The right
    /// query is re-rooted at the current parent frontier and ends when its
    /// source suffix is exhausted. This makes split ownership structural
    /// instead of relying on the splitter having drained every ancestor.
    ///
    /// `BindingStore::clone` deliberately deep-copies its level buffers;
    /// descendants in either half may refill an ancestor variable without
    /// invalidating the other half's row indexes. Frontier matrices remain
    /// cheap to share through their `Arc`s.
    fn split_current_source(&mut self, variable: VariableId) -> Self {
        let right_level = self.bindings.bisect(variable);
        let mut right = self.clone();
        right.bindings.install(variable, right_level);

        let depth = right.depth;
        drop(right.stack.drain(..depth));
        drop(right.depths.drain(..depth));
        right.depth = 0;

        debug_assert_eq!(right.stack[0].variable, variable);
        let source_group = right.stack[0].source_group;
        let root = &mut right.depths[0];
        debug_assert!(source_group < root.groups.len());
        debug_assert_eq!(root.groups[source_group].0, variable);
        Arc::make_mut(&mut root.groups).truncate(source_group + 1);
        // The right half owns only this already-materialised source page.
        // Once its candidate suffix is spent it must not resume later pages
        // of the same semantic group or any following group.
        root.group = root.groups.len();
        root.group_row = 0;
        root.group_width = INITIAL_FRONTIER_WIDTH.min(right.width);
        right
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Query<C, P, R> {
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
        let bindings = BindingStore::new();
        let slots = variables.find_last_set().map(|v| v + 1).unwrap_or(0);
        let estimates: Vec<usize> = (0..slots)
            .map(|v| {
                if variables.is_set(v) {
                    constraint
                        .estimate(v, &bindings.view())
                        .expect("unconstrained variable in query")
                } else {
                    usize::MAX
                }
            })
            .collect();

        // Constraints whose positions are all constant [`Term`]s (e.g. a
        // fully-constant `pattern!` used as an existence check) have an
        // empty variable set, so the propose/confirm search never consults
        // them. Their truth is binding-independent and `satisfied` is exact
        // for them from the start (the fully-bound exactness law: zero
        // unbound variables). One check up front settles every such
        // subtree; constraints with unbound variables answer an optimistic
        // `true` here and are validated by the search as usual.
        let mode = if constraint.satisfied(&bindings.view()) {
            Search::Plan
        } else {
            Search::Done
        };

        let root = Depth {
            block: Arc::new(vec![0u32; slots]),
            rows: 1,
            estimates: Arc::new(estimates),
            order: Arc::new(vec![0u32]),
            groups: Arc::new(Vec::new()),
            group: 0,
            group_row: 0,
            group_width: INITIAL_FRONTIER_WIDTH,
            emit: 0,
        };

        Query {
            constraint,
            postprocessing,
            mode,
            has_emitted: false,
            bindings,
            influences,
            unbound: variables,
            slots,
            width: DEFAULT_FRONTIER_WIDTH,
            stack: ArrayVec::new(),
            depths: vec![root],
            depth: 0,
            choice: Vec::new(),
            drawn: Vec::new(),
            parents: Vec::new(),
            stats: Arc::new(FrontierStats::default()),
        }
    }

    /// Sets the *widest* frontier this query expands at once. See
    /// [`DEFAULT_FRONTIER_WIDTH`] for what the number buys and
    /// [`INITIAL_FRONTIER_WIDTH`] for the ramp that leads up to it.
    ///
    /// A width of 1 reduces the engine to expanding one binding at a time —
    /// the shape the protocol had before frontiers — which is what the
    /// equivalence tests pin. The ramp is a no-op at that ceiling, so the
    /// reduction is exact.
    ///
    /// # Panics
    ///
    /// Panics if `width` is zero.
    pub fn with_frontier_width(mut self, width: usize) -> Self {
        assert!(width > 0, "frontier width must be at least one binding");
        self.width = width;
        self
    }

    /// Observational counters for this query's frontiers. The handle is
    /// shared with every rayon clone, so it stays meaningful under parallel
    /// execution.
    pub fn stats(&self) -> Arc<FrontierStats> {
        Arc::clone(&self.stats)
    }

    /// The frontier currently at the top of the stack, unrestricted.
    fn frontier(&self) -> Frontier<'_> {
        let depth = &self.depths[self.depth];
        self.bindings
            .batch(&depth.block, &depth.order[..depth.rows], self.slots)
    }

    /// Partitions the top frontier by each row's preferred variable.
    ///
    /// The preference is the same adaptive, magnitude-bucketed choice the
    /// engine has always made — smallest estimate magnitude, most
    /// influence — evaluated per row, against that row's own estimates.
    /// Rows are never moved onto a variable they did not choose:
    /// [`propose`](Constraint::propose) owns candidate support and
    /// first-seen order, and the protocol supplies no cross-variable
    /// support-equivalence law, so an estimate-compatible variable is not
    /// an interchangeable action. All the leeway lives in the magnitude
    /// bucketing, which is what makes agreement — and therefore a whole,
    /// unsplit batch — the common case.
    fn plan(&mut self) {
        if self.unbound.is_empty() {
            self.depths[self.depth].emit = 0;
            self.mode = Search::Emit;
            return;
        }

        let slots = self.slots;
        let influences = &self.influences;
        let unbound = self.unbound;
        let depth = &mut self.depths[self.depth];
        let rows = depth.rows;

        self.choice.clear();
        self.choice.reserve(rows);
        let mut single = true;
        let mut first = u32::MAX;
        for row in 0..rows {
            let row_estimates = &depth.estimates[row * slots..(row + 1) * slots];
            let variable = unbound
                .into_iter()
                .max_by_key(|&v| {
                    (
                        Reverse(
                            row_estimates[v]
                                .checked_ilog2()
                                .map(|magnitude| magnitude + 1)
                                .unwrap_or(0),
                        ),
                        influences[v].count(),
                    )
                })
                .expect("non-empty unbound") as u32;
            if row == 0 {
                first = variable;
            } else if variable != first {
                single = false;
            }
            self.choice.push(variable);
        }

        let order = reset_shared(&mut depth.order, 0);
        let groups = reset_shared(&mut depth.groups, 0);
        if single {
            // The whole block travels as one batch: only row numbers are
            // written, never a row.
            order.extend(0..rows as u32);
            groups.push((first as VariableId, rows));
        } else {
            // Stable counting sort by preferred variable, so a group's rows
            // keep their frontier order.
            let mut counts = vec![0usize; slots];
            for &variable in &self.choice {
                counts[variable as usize] += 1;
            }
            let mut offset = 0;
            let mut starts = vec![0usize; slots];
            for variable in 0..slots {
                starts[variable] = offset;
                if counts[variable] != 0 {
                    offset += counts[variable];
                    groups.push((variable, offset));
                }
            }
            order.resize(rows, 0);
            for (row, &variable) in self.choice.iter().enumerate() {
                order[starts[variable as usize]] = row as u32;
                starts[variable as usize] += 1;
            }
        }
        depth.group = 0;
        depth.group_row = 0;
        depth.group_width = INITIAL_FRONTIER_WIDTH.min(self.width);

        self.stats.expansions.fetch_add(1, Ordering::Relaxed);
        self.stats.rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.stats.widest.fetch_max(rows as u64, Ordering::Relaxed);
        self.stats.variable_groups.fetch_add(
            self.depths[self.depth].groups.len() as u64,
            Ordering::Relaxed,
        );
        self.mode = Search::NextGroup;
    }

    /// Expands the next page of the next group of the top frontier: pushes
    /// its variable and proposes candidates for a geometrically growing
    /// prefix of that group's still-unseen rows. When every page of every
    /// group is exhausted the frontier itself is retired.
    ///
    /// Candidate chunking alone cannot bound pull latency: before the first
    /// candidate can be drawn, `propose` has already enumerated and confirmed
    /// every row it was handed. Paging the engine-owned parent selection is
    /// the stateless counterpart. Constraints still see an ordinary
    /// [`Frontier`] and every parent row is expanded exactly once.
    fn next_group(&mut self) {
        loop {
            let depth = &self.depths[self.depth];
            if depth.group >= depth.groups.len() {
                if self.depth == 0 {
                    self.mode = Search::Done;
                } else {
                    self.depth -= 1;
                    self.mode = Search::NextChunk;
                }
                return;
            }

            let group = depth.group;
            let whole = depth.group_range(group);
            let offset = depth.group_row;
            let remaining = whole.len() - offset;
            if remaining == 0 {
                let depth = &mut self.depths[self.depth];
                depth.group += 1;
                depth.group_row = 0;
                depth.group_width = INITIAL_FRONTIER_WIDTH.min(self.width);
                continue;
            }

            let page = depth.group_width.min(remaining);
            let range = whole.start + offset..whole.start + offset + page;
            let variable = depth.groups[group].0;

            let depth = &mut self.depths[self.depth];
            depth.group_row += page;
            let remaining = remaining - page;
            if remaining == 0 {
                depth.group += 1;
                depth.group_row = 0;
                depth.group_width = INITIAL_FRONTIER_WIDTH.min(self.width);
            } else {
                depth.group_width = widened_width(depth.group_width, remaining, self.width);
            }

            self.unbound.unset(variable);

            let constraint = &self.constraint;
            let depth = &self.depths[self.depth];
            let block = Arc::clone(&depth.block);
            let order = Arc::clone(&depth.order);
            let proposed = self.bindings.refill(
                variable,
                &block,
                &order[range.clone()],
                self.slots,
                |frontier, proposals| constraint.propose(variable, frontier, proposals),
            );
            self.stack.push(Level {
                variable,
                // Once the caller has observed a completion,
                // `propose`/`confirm` work already paid for this source page
                // becomes useful batching credit. Before that semantic
                // boundary, keep every new candidate drain scalar: failed
                // prefixes must not make the eventual first result prepay an
                // unused child frontier.
                width: if self.has_emitted {
                    page.min(self.width)
                } else {
                    INITIAL_FRONTIER_WIDTH.min(self.width)
                },
                proposed,
                source_start: range.start,
                source_end: range.end,
                source_group: group,
            });
            self.stats
                .proposals
                .fetch_add(proposed as u64, Ordering::Relaxed);
            self.mode = Search::NextChunk;
            return;
        }
    }

    /// Consumes the next chunk of the top level's candidates into the child
    /// frontier. When the level is spent the group is retired and the next
    /// one gets its turn.
    ///
    /// # In-place 1:1 descent
    ///
    /// The child frontier is normally built by copying each drawn
    /// candidate's parent row and filling in the newly bound variable's
    /// slot. When the draw is **1:1** — one surviving child per parent row,
    /// in order, covering the whole parent frontier, with nothing left over
    /// — no row was gained, lost or reordered, so the child block *is* the
    /// parent block with one more slot written. The engine's two standing
    /// invariants are what make that safe: confirmers may only kill
    /// candidates and never revive them (so a surviving row keeps its
    /// identity), and buffers are write-once (so `variable`'s slot in every
    /// row was previously unwritten and filling it destroys nothing). The
    /// same argument covers the estimate matrix: the rows the child would
    /// have copied are bit-identical to the parent's, and the only slots it
    /// then overwrites are the ones the influence refresh recomputes
    /// anyway.
    ///
    /// A 1:1 draw that leaves the level spent is also the last thing the
    /// parent frontier is ever asked for, so the matrices are handed *down*
    /// (swapped with the child's retired allocations) rather than shared —
    /// which is what lets a whole chain of 1:1 descents run without a
    /// single matrix copy, instead of only the first one.
    ///
    /// Ownership is not tracked separately: the frontier matrices already
    /// sit behind `Arc` so a rayon split copies refcounts, and
    /// [`Arc::get_mut`] therefore succeeds exactly when no split or steal
    /// is holding the other half. Let the `Arc` be the guard; when it says
    /// no, take the copying path.
    ///
    /// **The fast path is gated so it costs nothing when it cannot fire.**
    /// Recognising a 1:1 draw means deferring the child rows until the
    /// draw's shape is known, and that deferral is a second pass — measured
    /// at +10% on F10 and +20% on F6 when charged to every descent. So the
    /// engine asks first, from what it already knows: a level holding
    /// `proposed` candidates for `rows` parents can only yield one child
    /// per parent if `proposed == rows`. Fan-out levels fail that `O(1)`
    /// test and run the fused [`take_chunk`](BindingStore::take_chunk)
    /// exactly as before.
    fn next_chunk(&mut self) {
        let level = *self.stack.last().expect("a level to chunk");
        let variable = level.variable;
        let parent = self.depth;
        let range = level.source_start..level.source_end;
        let slots = self.slots;

        if self.depths.len() <= parent + 1 {
            self.depths.push(Depth::default());
        }

        // Could this draw possibly be 1:1? One child per parent needs
        // exactly as many candidates as there are parent rows, and a chunk
        // wide enough to take them all. Both are known before drawing, and
        // both are `O(1)`.
        let source_rows = self.depths[parent].rows;
        let speculate = level.proposed == source_rows
            && level.width >= source_rows
            && self.depths[parent].group >= self.depths[parent].groups.len();

        let rows = if speculate {
            self.bindings.draw(
                variable,
                level.width,
                &self.depths[parent].order[range],
                &mut self.drawn,
                &mut self.parents,
            );
            self.drawn.len()
        } else {
            let (head, tail) = self.depths.split_at_mut(parent + 1);
            self.bindings.take_chunk(
                variable,
                level.width,
                &head[parent].block,
                &head[parent].order[range],
                slots,
                Arc::make_mut(&mut tail[0].block),
                &mut self.parents,
            )
        };
        // Widen the next chunk this level hands down. Before the first
        // caller-visible row its initial width is one; afterward it is the
        // source-page width that opened it, reusing work already paid by
        // sustained demand. After the first draw the width keeps climbing by
        // `FRONTIER_RAMP_BASE` to the ceiling (see `FRONTIER_RAMP_BASE` for
        // why the base is what decides that).
        let consumed = self.bindings.consumed(variable);
        if let Some(top) = self.stack.last_mut() {
            // Never leave a tail smaller than the chunk that would precede
            // it. `proposed - consumed` over-counts (it cannot see which
            // entries confirm already killed), so this merges conservatively
            // — it can decline a merge it should have made, never force one
            // it should not. Both branches are O(1): the level already knows
            // how many candidates it proposed and how far the cursor ran.
            let remaining = top.proposed.saturating_sub(consumed);
            top.width = widened_width(top.width, remaining, self.width);
        }

        if rows == 0 {
            self.stack.pop();
            self.bindings.unset(variable);
            self.unbound.set(variable);
            self.mode = Search::NextGroup;
            return;
        }

        // The speculation paid off when every parent really did produce
        // exactly one child, in order, and nothing is left pending.
        // `spent` is last because it scans the level's remaining liveness
        // words.
        let reusable = speculate
            && rows == source_rows
            && self
                .parents
                .iter()
                .enumerate()
                .all(|(row, &parent_row)| parent_row as usize == row)
            && self.bindings.spent(variable);

        let (head, tail) = self.depths.split_at_mut(parent + 1);
        let source = &mut head[parent];
        let child = &mut tail[0];

        let inplace = reusable
            && Arc::get_mut(&mut source.block).is_some()
            && Arc::get_mut(&mut source.estimates).is_some();

        if inplace {
            // Hand the matrices down and take the child's retired
            // allocations in exchange, so the next 1:1 descent finds a
            // uniquely-owned block again and the chain stays copy-free.
            std::mem::swap(&mut source.block, &mut child.block);
            std::mem::swap(&mut source.estimates, &mut child.estimates);
            let block = Arc::get_mut(&mut child.block).expect("unique across the swap");
            for (row, &entry) in self.drawn.iter().enumerate() {
                block[row * slots + variable] = entry;
            }
            self.stats.inplace_descents.fetch_add(1, Ordering::Relaxed);
        } else {
            if speculate {
                // Speculated and lost: the rows still have to be written,
                // just from the deferred draw rather than fused with it.
                let block = reset_shared(&mut child.block, rows * slots);
                for (&entry, &parent_row) in self.drawn.iter().zip(self.parents.iter()) {
                    let parent_row = parent_row as usize;
                    block.extend_from_slice(
                        &source.block[parent_row * slots..(parent_row + 1) * slots],
                    );
                    let base = block.len() - slots;
                    block[base + variable] = entry;
                }
            }
            // Inherit each child row's estimates from its parent; the
            // refresh below then updates exactly the ones binding
            // `variable` can have changed — the same influence-driven
            // refresh the single-binding engine did, now once per row of
            // the batch. Nothing else can have gone stale: a row's
            // estimates were computed against its own binding, so
            // backtracking never invalidates them.
            let estimates = reset_shared(&mut child.estimates, rows * slots);
            for &parent_row in self.parents.iter() {
                let parent_row = parent_row as usize;
                estimates.extend_from_slice(
                    &source.estimates[parent_row * slots..(parent_row + 1) * slots],
                );
            }
            self.stats.copied_descents.fetch_add(1, Ordering::Relaxed);
        }
        child.rows = rows;
        child.group = 0;
        child.group_row = 0;
        child.group_width = INITIAL_FRONTIER_WIDTH.min(self.width);
        child.emit = 0;

        let order = Arc::make_mut(&mut child.order);
        order.clear();
        order.extend(0..rows as u32);

        let stale = self.influences[variable].intersect(self.unbound);
        if !stale.is_empty() {
            let child = &mut self.depths[parent + 1];
            let block = Arc::clone(&child.block);
            let order = Arc::clone(&child.order);
            let estimates = Arc::make_mut(&mut child.estimates);
            let batch = self.bindings.batch(&block, &order, slots);
            for row in 0..rows {
                let binding = batch.row(row);
                for v in stale {
                    estimates[row * slots + v] = self
                        .constraint
                        .estimate(v, &binding)
                        .expect("unconstrained variable in query");
                }
            }
        }

        self.depth = parent + 1;
        self.mode = Search::Plan;
    }
}

/// The search mode of the query engine.
///
/// The engine is still a depth-first search; what moved is its unit of
/// work. A step no longer binds one variable to one value — it expands a
/// whole *frontier* of parent bindings at once, so every level's proposal
/// region is as wide as the batch rather than as wide as one parent's
/// candidate list.
///
/// - `Plan` — partition the top frontier by each row's preferred variable.
/// - `NextGroup` — expand the next geometric page of a preferred-variable
///   group: push its variable and propose for that disjoint row slice.
/// - `NextChunk` — turn the next `width` surviving candidates into the
///   child frontier, or retire the group when they run out.
/// - `Emit` — every variable is bound; stream the frontier's rows out.
/// - `Done` — the search is finished.
#[derive(Copy, Clone, Debug)]
enum Search {
    Plan,
    NextGroup,
    NextChunk,
    Emit,
    Done,
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.mode {
                Search::Plan => self.plan(),
                Search::NextGroup => self.next_group(),
                Search::NextChunk => self.next_chunk(),
                Search::Emit => {
                    // One row per complete binding, in frontier order, no
                    // dedup — bag semantics are a property of the rows, not
                    // of how many at a time the engine happened to build.
                    let frontier = self.frontier();
                    let rows = frontier.len();
                    let mut emit = self.depths[self.depth].emit;
                    let mut result = None;
                    while emit < rows {
                        let binding = frontier.row(emit);
                        emit += 1;
                        if let Some(row) = (self.postprocessing)(&binding) {
                            result = Some(row);
                            break;
                        }
                    }
                    self.depths[self.depth].emit = emit;
                    if let Some(result) = result {
                        self.has_emitted = true;
                        return Some(result);
                    }
                    // The batch is spent; hand control back to the level
                    // that produced it for its next chunk.
                    if self.depth == 0 {
                        self.mode = Search::Done;
                    } else {
                        self.depth -= 1;
                        self.mode = Search::NextChunk;
                    }
                }
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
            .field("width", &self.width)
            .field("frontier", &self.frontier())
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
// The producer's `split` bisects a *frontier's source*: the pending
// candidates of the top level — the entries that will become the next
// chunks of the child frontier. While the top has a single pending
// candidate the producer descends through it; with two or more it cuts the
// pending region in half and hands the tail to a new sub-query. That sibling
// is fenced at the current source: it keeps the current parent frontier and
// source level, but not later groups or ancestor continuations. The left half
// is their sole owner, so a sibling cannot replay work when it unwinds.
//
// Why the indexes stay valid across the cut: a candidate's parent tag names
// a row of the *parent frontier*, which the right half keeps verbatim. Its
// matrices are shared `Arc`s, so the clone is refcounts, not copies; level
// buffers are deep-cloned because bound row indexes must remain valid if the
// other half refills a level. The left half keeps entries `[0..mid)` and every
// consumed entry sits below `pos <= mid`, so its own indexes still resolve to
// the same values.
//
// Splitting narrows the frontier — the two halves each expand a slice of
// what one would have expanded together. That is the deliberate trade:
// batch width buys per-level dispatch size, work-stealing buys core
// utilisation, and rayon only asks for a split under stealing pressure.
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
    use rayon::iter::plumbing::{bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer};
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

        /// Advance the Query's state machine until either the current
        /// top level has ≥2 pending candidates (bisect, return a right
        /// half) or the sub-query is exhausted / has reached a batch of
        /// complete rows (return `None`, leaving `self` as a leaf that
        /// `fold_with` will fold sequentially).
        fn split(mut self) -> (Self, Option<Self>) {
            if self.split_budget == 0 {
                return (self, None);
            }
            self.split_budget -= 1;
            let q = &mut *self.inner;
            loop {
                match q.mode {
                    Search::Plan => q.plan(),
                    Search::NextGroup => q.next_group(),
                    // A batch of complete rows, or nothing left: both are
                    // leaves for the sequential folder.
                    Search::Emit | Search::Done => return (self, None),
                    Search::NextChunk => {
                        let top = q.stack.last().expect("a level to chunk").variable;
                        if q.bindings.pending(top) < 2 {
                            // Nothing to cut: either the level is spent
                            // (the step retires it) or a single candidate
                            // remains (the step descends through it).
                            q.next_chunk();
                            continue;
                        }
                        let right = q.split_current_source(top);

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

    #[cfg(feature = "parallel")]
    mod split_fence {
        use super::*;

        #[derive(Clone)]
        struct Fixture(u32);

        impl Fixture {
            fn value(tag: u8, index: u32) -> RawInline {
                let mut value = [0; 32];
                value[0] = tag;
                value[28..].copy_from_slice(&index.to_be_bytes());
                value
            }

            fn count(&self, group: u8, variable: VariableId) -> u32 {
                match (group, variable) {
                    (3 | 0, 1) => 0,
                    (1, 2) => self.0,
                    _ => 1,
                }
            }
        }

        impl<'a> Constraint<'a> for Fixture {
            fn variables(&self) -> VariableSet {
                variable_set(0..4)
            }

            fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
                let preferred = binding
                    .get(0)
                    .map(|anchor| match anchor[31] {
                        0 => 1,
                        1 => 2,
                        2 => 3,
                        _ => 1,
                    })
                    .unwrap_or(0);
                (variable < 4).then_some(if variable == 0 {
                    4
                } else if variable == preferred {
                    1
                } else {
                    8
                })
            }

            fn propose(
                &self,
                variable: VariableId,
                frontier: &Frontier<'_>,
                proposals: &mut ProposalBuffer,
            ) {
                for row in 0..frontier.len() {
                    proposals.open(row as u32);
                    if variable == 0 {
                        proposals.extend([3, 0, 1, 2].map(|group| Self::value(0, group)));
                    } else {
                        let group = frontier.row(row).get(0).expect("anchor bound")[31];
                        proposals.extend(
                            (0..self.count(group, variable))
                                .map(|index| Self::value(variable as u8, index)),
                        );
                    }
                }
            }

            fn confirm(
                &self,
                _variable: VariableId,
                _frontier: &Frontier<'_>,
                _candidates: &mut Candidates<'_>,
            ) {
            }
        }

        #[derive(Clone)]
        struct PagedFixture(u32);

        impl<'a> Constraint<'a> for PagedFixture {
            fn variables(&self) -> VariableSet {
                variable_set(0..2)
            }

            fn estimate(&self, variable: VariableId, binding: &Binding) -> Option<usize> {
                match variable {
                    0 => Some(4),
                    1 => Some(if binding.get(0).is_some() { 1 } else { 8 }),
                    _ => None,
                }
            }

            fn propose(
                &self,
                variable: VariableId,
                frontier: &Frontier<'_>,
                proposals: &mut ProposalBuffer,
            ) {
                for row in 0..frontier.len() {
                    proposals.open(row as u32);
                    match variable {
                        0 => proposals.extend((0..4).map(|i| Fixture::value(0, i))),
                        1 => {
                            let anchor = frontier.row(row).get(0).expect("anchor bound")[31];
                            if anchor != 0 {
                                proposals.extend((0..self.0).map(|i| Fixture::value(anchor, i)));
                            }
                        }
                        _ => {}
                    }
                }
            }

            fn confirm(
                &self,
                _variable: VariableId,
                _frontier: &Frontier<'_>,
                _candidates: &mut Candidates<'_>,
            ) {
            }
        }

        type TestQuery = Query<Fixture, fn(&Binding<'_>) -> Option<RawInline>, RawInline>;
        type PagedQuery = Query<
            PagedFixture,
            fn(&Binding<'_>) -> Option<(RawInline, RawInline)>,
            (RawInline, RawInline),
        >;

        fn project(binding: &Binding<'_>) -> Option<RawInline> {
            binding.get(0).copied()
        }

        fn project_pair(binding: &Binding<'_>) -> Option<(RawInline, RawInline)> {
            Some((*binding.get(0)?, *binding.get(1)?))
        }

        fn query(fanout: u32) -> TestQuery {
            Query::new(
                Fixture(fanout),
                project as fn(&Binding<'_>) -> Option<RawInline>,
            )
            .with_frontier_width(4)
        }

        fn paged_query(fanout: u32) -> PagedQuery {
            Query::new(
                PagedFixture(fanout),
                project_pair as fn(&Binding<'_>) -> Option<(RawInline, RawInline)>,
            )
            .with_frontier_width(4)
        }

        fn advance_to_second_of_three_groups(query: &mut TestQuery) {
            query.plan();
            query.next_group();
            query.next_chunk();

            // Exhaust the empty one-row latency frontier, then widen the
            // root into three anchors with three preferred variables.
            query.plan();
            query.next_group();
            query.next_chunk();
            query.next_group();
            query.next_chunk();
            query.plan();
            assert_eq!(query.depths[query.depth].groups.len(), 3);

            // Retire the empty first group and stop inside the second.
            query.next_group();
            query.next_chunk();
            query.next_group();
            assert_eq!(query.depths[query.depth].group, 2);
            assert_eq!(query.stack.last().unwrap().variable, 2);
        }

        #[test]
        fn split_owns_only_its_current_fragmented_group() {
            const FANOUT: u32 = 64;
            let mut expected: Vec<_> = query(FANOUT).collect();
            expected.sort_unstable();
            assert_eq!(expected.len(), FANOUT as usize + 1);

            let mut left = query(FANOUT);
            advance_to_second_of_three_groups(&mut left);
            let mut right = left.split_current_source(2);

            assert_eq!(
                (right.depth, right.depths.len(), right.stack.len()),
                (0, 1, 1)
            );
            assert_eq!(
                (right.depths[0].group, right.depths[0].groups.len()),
                (2, 2)
            );
            assert!(Arc::ptr_eq(&left.depths[1].block, &right.depths[0].block));
            // `ProposalBuffer` derefs to its entry slice, so this compares
            // the two buffers' storage without naming a private field.
            assert!(!std::ptr::eq(
                left.bindings.levels[0].buffer.as_ptr(),
                right.bindings.levels[0].buffer.as_ptr(),
            ));

            // Fencing an already-fenced sibling cannot restore continuation.
            let rightmost = right.split_current_source(2);
            assert_eq!((right.depth, right.depths[0].groups.len()), (0, 2));
            assert_eq!((rightmost.depth, rightmost.depths[0].groups.len()), (0, 2));

            let mut actual: Vec<_> = left.chain(right).chain(rightmost).collect();
            actual.sort_unstable();
            assert_eq!(actual, expected, "splitting must preserve the exact bag");
            assert_eq!(actual.iter().filter(|row| row[31] == 2).count(), 1);
        }

        #[test]
        fn split_does_not_own_later_pages_of_the_same_group() {
            const FANOUT: u32 = 64;
            let mut expected: Vec<_> = paged_query(FANOUT).collect();
            expected.sort_unstable();
            assert_eq!(expected.len(), 3 * FANOUT as usize);

            let mut left = paged_query(FANOUT);
            left.plan();
            left.next_group();
            left.next_chunk();

            // Anchor 0 deliberately has no child. Retire that one-row
            // latency chunk so the root's next chunk contains anchors 1..3.
            left.plan();
            left.next_group();
            left.next_chunk();
            left.next_group();
            left.next_chunk();

            left.plan();
            assert_eq!(left.depths[left.depth].group_range(0).len(), 3);
            left.next_group();
            let source = *left.stack.last().expect("paged source");
            assert_eq!(source.variable, 1);
            assert_eq!(source.source_group, 0);
            assert_eq!(
                (
                    left.depths[left.depth].group,
                    left.depths[left.depth].group_row,
                ),
                (0, 1),
                "the split point must be inside a non-final page sequence"
            );

            let right = left.split_current_source(1);
            assert_eq!(
                (right.depths[0].group, right.depths[0].groups.len()),
                (1, 1),
                "the sibling must be fenced before later pages of this group"
            );

            let mut left_rows: Vec<_> = left.collect();
            let right_rows: Vec<_> = right.collect();
            assert!(
                right_rows.iter().all(|(anchor, _)| anchor[31] == 1),
                "the sibling escaped its first source page"
            );
            left_rows.extend(right_rows);
            left_rows.sort_unstable();
            assert_eq!(
                left_rows, expected,
                "splitting a non-final source page must preserve the exact bag"
            );
        }
    }
}
