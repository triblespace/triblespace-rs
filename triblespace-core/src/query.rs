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
/// [`ProposalBuffer`]/[`Candidates`] and their bit-packed liveness.
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

/// Re-exports of the candidate buffer and its liveness word helpers. Liveness
/// is bit-packed, 32 candidates per `u32`; see the `liveness` module for the
/// region-boundary trap that layout introduces.
pub use liveness::{and_words, or_words, Candidates, ProposalBuffer};

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
/// * Buffers are write-once: confirmers kill entries by clearing their
///   liveness bit, and nothing ever moves or rewrites a stored value once
///   the engine can see it.
///
/// `Binding` is therefore a *view* — the index row plus a borrow of the
/// buffers it indexes into — constructed for the duration of one
/// constraint call. [`BindingStore`] owns both halves.
///
/// The payoff is size: an assignment is 128 `u32`s (512 bytes) instead of
/// 128 raw inline values (4 KiB), a bind is a 4-byte write instead of a
/// 32-byte copy, cloning the search state no longer memcpies the values,
/// and a *batch* of bindings becomes a small integer matrix over shared
/// buffers rather than a pile of value copies.
#[derive(Clone, Copy)]
pub struct Binding<'a> {
    /// Bitset tracking which variables have been assigned a value.
    pub bound: VariableSet,
    indexes: &'a [u32; 128],
    levels: &'a [LevelValues; 128],
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

/// Owns what a [`Binding`] is a view of: the per-variable level buffers
/// and the index row that picks one entry out of each.
///
/// This is the query engine's search state. It is also how code outside
/// the engine builds a binding over values it picked itself — see
/// [`bind`](BindingStore::bind).
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

    /// The current assignment, as constraints see it.
    pub fn view(&self) -> Binding<'_> {
        Binding {
            bound: self.bound,
            indexes: &self.indexes,
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

    /// Whether `variable`'s source may still deliver candidates beyond
    /// what is materialized.
    fn more(&self, variable: VariableId) -> bool {
        self.levels[variable].more
    }

    /// Consumes the next live candidate at `variable`'s level and binds
    /// `variable` to it. Returns `false` when the materialized region is
    /// spent.
    fn advance(&mut self, variable: VariableId) -> bool {
        let level = &mut self.levels[variable];
        match level.buffer.next_live(level.pos) {
            Some(i) => {
                level.pos = i + 1;
                self.indexes[variable] = i as u32;
                self.bound.set(variable);
                true
            }
            None => false,
        }
    }

    /// Clears `variable`'s level and refills it from `propose`.
    ///
    /// The level is moved *out* of the array for the duration of the
    /// call, so `propose` gets `&mut` on it while the remaining levels
    /// lend immutably through the [`Binding`] view — the borrow split the
    /// search needs, with no `unsafe` and no second copy of the
    /// candidates. This is sound precisely because the engine only ever
    /// refills the level of a variable it is about to push, which is by
    /// construction unbound: nothing can be resolving through this level
    /// while it is away.
    fn refill(
        &mut self,
        variable: VariableId,
        reserve: usize,
        propose: impl FnOnce(&Binding<'_>, &mut ProposeCursor, usize, &mut ProposalBuffer) -> bool,
    ) {
        debug_assert!(
            !self.bound.is_set(variable),
            "refilling a bound variable's level would strand its binding"
        );
        let mut level = std::mem::take(&mut self.levels[variable]);
        level.buffer.clear();
        level.pos = 0;
        level.cursor = ProposeCursor::default();
        level.widen = INITIAL_CHUNK;
        level
            .buffer
            .reserve_exact(reserve.saturating_sub(level.buffer.capacity()));
        level.more = propose(
            &Binding {
                bound: self.bound,
                indexes: &self.indexes,
                levels: &self.levels,
            },
            &mut level.cursor,
            level.widen,
            &mut level.buffer,
        );
        self.levels[variable] = level;
    }

    /// Requests the next geometric chunk for `variable`'s level and
    /// appends it to what is already materialized. Returns whether the
    /// level may still produce more.
    ///
    /// Unlike [`refill`](BindingStore::refill) the level stays in the
    /// array, because widening *can* happen while `variable` is still
    /// bound: the engine reaches here by backtracking into a level whose
    /// materialized region ran dry, and that binding has to keep
    /// resolving while the source is asked for more. The chunk is
    /// therefore proposed into a detached buffer and appended, which
    /// leaves every existing index — including the bound one — pointing
    /// at the same entry.
    fn widen(
        &mut self,
        variable: VariableId,
        propose: impl FnOnce(&Binding<'_>, &mut ProposeCursor, usize, &mut ProposalBuffer) -> bool,
    ) -> bool {
        let widen = self.levels[variable].widen.saturating_mul(WIDEN_FACTOR);
        let mut cursor = self.levels[variable].cursor;
        let mut chunk = ProposalBuffer::new();
        let more = propose(
            &Binding {
                bound: self.bound,
                indexes: &self.indexes,
                levels: &self.levels,
            },
            &mut cursor,
            widen,
            &mut chunk,
        );
        let level = &mut self.levels[variable];
        level.widen = widen;
        level.cursor = cursor;
        level.more = more;
        level.buffer.append(&mut chunk);
        more
    }

    /// Bisects `variable`'s materialized region, returning the tail as a
    /// fresh level for the right half of a parallel split.
    ///
    /// The left half keeps entries `[0..mid)`, and every consumed entry
    /// (and hence every index any binding holds for this level) sits
    /// below `pos <= mid` — so the left half's indexes stay valid. The
    /// returned tail re-indexes from zero, which is why the right half
    /// must [`unset`](BindingStore::unset) `variable` when it installs
    /// it.
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
            cursor: ProposeCursor::default(),
            more: false,
            widen: INITIAL_CHUNK,
        }
    }

    /// Installs `level` as `variable`'s level and unbinds `variable`:
    /// the incoming buffer has its own coordinates, so any index the
    /// index row still holds for it is meaningless.
    #[cfg(feature = "parallel")]
    fn install(&mut self, variable: VariableId, level: LevelValues) {
        self.levels[variable] = level;
        self.bound.unset(variable);
    }
}

type ProjectionKey = Box<[RawInline]>;

/// Resume state for chunked proposing — plain data held by the engine,
/// interpreted by the proposing constraint.
///
/// Constraints stay stateless: instead of retaining a live enumeration (which
/// would borrow the constraint into the engine — the self-referential trap the
/// stateless protocol exists to avoid), the source re-finds its place from
/// this cursor on every [`propose_chunk`](Constraint::propose_chunk) call.
/// `key` is 32 opaque bytes the source owns (a last-delivered value, a rank
/// offset — its choice); `started` distinguishes "not begun" from "resumed at
/// the zero key". Plain data means the cursor survives `Query::clone` and the
/// rayon splitter for free.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProposeCursor {
    /// Whether any chunk has been requested yet.
    pub started: bool,
    /// Source-interpreted resume state.
    pub key: RawInline,
}

/// First chunk size requested from [`Constraint::propose_chunk`] at a fresh
/// level. Small on purpose: time-to-first-result at a wide level is bounded
/// by the first chunk, not the level's cardinality.
const INITIAL_CHUNK: usize = 64;

/// Geometric growth factor between successive chunks at one level. Total
/// enumeration work stays within a constant factor of eager proposing while
/// the number of refill calls stays logarithmic.
const WIDEN_FACTOR: usize = 4;

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
    /// being bound. Values are appended to `proposals`; entries appended by
    /// an enclosing composite's other children may precede them, and must
    /// be left untouched.
    ///
    /// Does nothing when `variable` is not constrained by this constraint.
    fn propose(&self, variable: VariableId, binding: &Binding, proposals: &mut ProposalBuffer);

    /// Chunked, resumable [`propose`](Constraint::propose): appends up to
    /// `budget` further candidates for `variable`, advancing `cursor`, and
    /// returns `true` while more may remain (`false` = exhausted, never call
    /// again). Delivering a value twice across the calls of one enumeration
    /// is a semantics error — it would inflate bag multiplicity — and every
    /// call with nonzero budget must advance the cursor, so a caller looping
    /// on refill always terminates.
    ///
    /// The default delivers everything on the first call and reports
    /// exhaustion. Sources with seekable enumerations override this to bound
    /// time-to-first-result at wide levels; composites forward or compose it
    /// (an intersection chunk-confirms each slice as it arrives).
    fn propose_chunk(
        &self,
        variable: VariableId,
        binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        let _ = budget;
        if !cursor.started {
            cursor.started = true;
            self.propose(variable, binding, proposals);
        }
        false
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

    fn propose_chunk(
        &self,
        variable: VariableId,
        binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        let inner: &T = self;
        inner.propose_chunk(variable, binding, cursor, budget, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, binding, cands)
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

    fn propose_chunk(
        &self,
        variable: VariableId,
        binding: &Binding,
        cursor: &mut ProposeCursor,
        budget: usize,
        proposals: &mut ProposalBuffer,
    ) -> bool {
        let inner: &T = self;
        inner.propose_chunk(variable, binding, cursor, budget, proposals)
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, cands: &mut Candidates<'_>) {
        let inner: &T = self;
        inner.confirm(variable, binding, cands)
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

/// Per-variable enumeration state for one search level: the materialized
/// candidates, the resume cursor for the unmaterialized tail, whether more
/// may remain, and the next chunk budget. Slots are reused across sibling
/// levels for their buffer capacity, exactly as the plain buffers were.
#[derive(Clone, Debug)]
struct LevelValues {
    buffer: ProposalBuffer,
    /// Consumption position: entries before it are consumed, live entries at
    /// or after it are pending. Dead entries are skipped, never moved.
    pos: usize,
    cursor: ProposeCursor,
    more: bool,
    widen: usize,
}

impl LevelValues {
    /// An empty level, const-constructible so the empty [`Binding`] can
    /// borrow a `static` array of them.
    const fn empty() -> Self {
        LevelValues {
            // `ProposalBuffer`'s fields are private to the `liveness` module
            // now that there are two layouts of them; `new` is `const` for
            // exactly this call site.
            buffer: ProposalBuffer::new(),
            pos: 0,
            cursor: ProposeCursor {
                started: false,
                key: [0; 32],
            },
            more: false,
            widen: INITIAL_CHUNK,
        }
    }
}

impl Default for LevelValues {
    fn default() -> Self {
        Self::empty()
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
    stack: ArrayVec<VariableId, 128>,
    unbound: ArrayVec<VariableId, 128>,
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
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Query<C, P, R> {
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
        stale_estimates = stale_estimates.subtract(self.bindings.bound());

        if !stale_estimates.is_empty() {
            while let Some(v) = stale_estimates.drain_next_ascending() {
                self.estimates[v] = self
                    .constraint
                    .estimate(v, &self.bindings.view())
                    .expect("unconstrained variable in query");
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
        let estimate = self.estimates[variable];
        self.stack.push(variable);
        let constraint = &self.constraint;
        self.bindings.refill(
            variable,
            estimate.min(INITIAL_CHUNK),
            |binding, cursor, budget, proposals| {
                constraint.propose_chunk(variable, binding, cursor, budget, proposals)
            },
        );
    }

    /// Requests the next geometric chunk for `variable`'s level. Returns
    /// whether the level may still produce more after this call.
    fn widen_level(&mut self, variable: VariableId) -> bool {
        let constraint = &self.constraint;
        self.bindings
            .widen(variable, |binding, cursor, budget, proposals| {
                constraint.propose_chunk(variable, binding, cursor, budget, proposals)
            })
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
        let bindings = BindingStore::new();
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
            Search::NextVariable
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
        }
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

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &self.mode {
                Search::NextVariable => {
                    self.mode = Search::NextValue;
                    if self.unbound.is_empty() {
                        if let Some(result) = (self.postprocessing)(&self.bindings.view()) {
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
                        if self.bindings.advance(variable) {
                            self.touched_variables.set(variable);
                            self.mode = Search::NextVariable;
                        } else if self.bindings.more(variable) {
                            // Materialized candidates ran dry but the source
                            // has (or may have) more: request the next
                            // geometric chunk and re-enter this arm.
                            self.widen_level(variable);
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
                        self.bindings.unset(variable);
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

impl<'a, C: Constraint<'a>, P: Fn(&Binding<'_>) -> Option<R>, R> fmt::Debug for Query<C, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("constraint", &std::any::type_name::<C>())
            .field("mode", &self.mode)
            .field("binding", &self.bindings.view())
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
                                q.bindings.unset(variable);
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
                // proposals. With chunked proposing, "single remaining
                // value" only licenses a descend when the level is also
                // exhausted — descending through a level with a live
                // cursor would clone that cursor into the right half at a
                // later bisect and double-enumerate its tail. Non-exhausted
                // levels are bisected instead: the right half takes the
                // materialized candidates (marked exhausted), the left
                // half keeps the cursor.
                let Some(&top) = q.stack.last() else {
                    return (self, None);
                };
                let (top_live, top_more) = (q.bindings.pending(top), q.bindings.more(top));
                match (top_live, top_more) {
                    (0, false) => q.mode = Search::Backtrack,
                    (0, true) => {
                        // Nothing pending but the source has more:
                        // pull the next chunk, then re-inspect.
                        q.widen_level(top);
                    }
                    (1, false) => {
                        // Descend: consume the single pending value, bind
                        // it, transition to NextVariable so the outer loop
                        // runs propose.
                        let advanced = q.bindings.advance(top);
                        debug_assert!(advanced, "a pending candidate must bind");
                        q.touched_variables.set(top);
                        q.mode = Search::NextVariable;
                    }
                    _ => {
                        // Bisect the materialized proposals; clone the
                        // rest of the query state into the right half.
                        // Clone cost is one ~11 KB arraycopy per
                        // rayon-requested split — rayon only asks under
                        // stealing pressure. The right half never drives
                        // the cursor (its slice is fixed); the left half
                        // retains it, so every unmaterialized candidate
                        // still has exactly one owner.
                        //
                        // Bindings are indexes into the level buffers, so
                        // the halves' coordinate systems matter here. The
                        // left half keeps entries [0..mid) and every
                        // consumed entry sits below `pos <= mid`, so its
                        // indexes still resolve to the same values. The
                        // right half's buffer re-indexes from zero, which
                        // is why `install` unbinds `top` there — it is
                        // about to be re-bound out of the tail anyway.
                        let right_level = q.bindings.bisect(top);
                        let mut right = q.clone();
                        right.bindings.install(top, right_level);

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
