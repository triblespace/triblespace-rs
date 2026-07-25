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
pub mod program;
/// [`InlineRange`](rangeconstraint::InlineRange) — restricts a variable to a byte-lexicographic range.
pub mod rangeconstraint;
/// [`RegularPathConstraint`] — regular path expressions over graphs.
pub mod regularpathconstraint;
/// Experimental canonical residual-state execution for arbitrary constraints.
pub mod residual;
/// [`SortedSliceConstraint`](sortedsliceconstraint::SortedSliceConstraint) — constrains a variable to values in a sorted slice (binary search confirm).
pub mod sortedsliceconstraint;
/// [`UnionConstraint`](unionconstraint::UnionConstraint) — logical OR.
pub mod unionconstraint;
mod variableset;

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "parallel")]
use std::sync::Mutex;

use ahash::AHashSet;
use constantconstraint::*;

use crate::inline::encodings::genid::GenId;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;

pub(crate) use program::ProgramCompleteAffinity;
#[cfg(test)]
pub use program::ProgramCompleteEffects;
#[doc(hidden)]
pub use program::{
    DispatchClass, ProgramAction, ProgramActivation, ProgramBatch, ProgramBatchEffects,
    ProgramChild, ProgramCompleteBatch, ProgramCompleteWorkEvidence, ProgramCompleteWorkQuote,
    ProgramCompletion, ProgramGrouping, ProgramKey, ProgramPacing, ProgramPage, ProgramRef,
    ProgramRequest, ProgramResume, ProgramRoute, ProgramRuntime, ProgramSeedBatch,
    ProgramSeedEffects, ProgramSeedWork, ProgramStratum, ProgramWork, ProgramWorkHandle,
    TypedCompleteArbiter, TypedCompleteSink, TypedEffectSink, TypedProgramBatch, TypedProgramSpec,
    TypedResume, TypedSeedSink,
};
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
    fn from(c: Inline<T>) -> Self {
        Term::Const(c)
    }
}

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

    /// Returns the term's value under `binding`: the pinned value for a
    /// constant, the binding's value (if any) for a variable.
    #[inline]
    pub fn bound<'b>(&'b self, binding: &'b Binding) -> Option<&'b RawInline> {
        match self {
            RawTerm::Var(v) => binding.get(*v),
            RawTerm::Const(c) => Some(c),
        }
    }

    /// Adds the term's variable (if it is one) to `set`.
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

type ProjectionKey = Box<[RawInline]>;

/// The raw relational identity claimed before result conversion.
///
/// A full head is injective over complete bindings, whose uniqueness is
/// already guaranteed by the engine's SET-admitted actions, so it needs no
/// terminal claim table. Strict projections keep an owned set. Rayon promotes
/// that set to one shared run-owned domain when it creates the first sibling
/// shard; ordinary `Query::clone` deliberately snapshots either stored
/// representation back into an independent owned set.
enum ProjectionClaims {
    Elided,
    Owned(AHashSet<ProjectionKey>),
    #[cfg(feature = "parallel")]
    Shared(Arc<Mutex<AHashSet<ProjectionKey>>>),
}

struct ProjectionGate {
    /// Projected variables in declared head order. This order, together with
    /// the raw inline bytes, is the public row identity; converted Rust values
    /// never participate in distinctness.
    head: Arc<[VariableId]>,
    claims: ProjectionClaims,
    /// Macro-created projections expose only their declared head to the
    /// mapper. Direct `Query::new` uses the original complete binding instead.
    mapper_binding: Option<Box<Binding>>,
}

/// Result of mapping one newly claimed complete raw binding.
///
/// `Done` is distinct from `Skip`: an empty head has exactly one possible raw
/// key. Once that key is claimed, no later hidden witness can produce another
/// public row, even when conversion or mapper code rejected the claimed key.
enum ProjectionStep<R> {
    Yield(R),
    Skip,
    Done,
}

impl ProjectionGate {
    fn new(head: impl IntoIterator<Item = VariableId>, variables: VariableSet) -> Self {
        let mut ordered = Vec::new();
        let mut unique = VariableSet::new_empty();
        for variable in head {
            assert!(
                variables.is_set(variable),
                "projected variable {variable} is not constrained by this query"
            );
            assert!(
                !unique.is_set(variable),
                "projected variable {variable} appears more than once in the query head"
            );
            unique.set(variable);
            ordered.push(variable);
        }
        Self {
            head: ordered.into(),
            claims: if unique == variables {
                ProjectionClaims::Elided
            } else {
                ProjectionClaims::Owned(AHashSet::new())
            },
            mapper_binding: Some(Box::default()),
        }
    }

    fn full(variables: VariableSet) -> Self {
        let mut gate = Self::new(variables, variables);
        gate.mapper_binding = None;
        gate
    }

    fn is_empty_head(&self) -> bool {
        self.head.is_empty()
    }

    /// Whether every possible public key has already been claimed.
    ///
    /// Only a strict empty head needs a finite singleton claim domain. A full
    /// zero-variable head is elided: the engine has one semantic seed and
    /// consumes it before invoking user code. Parallel strict-projection
    /// shards inspect the shared claim set, so one shard's claim also stops its
    /// siblings at their next pull boundary.
    fn is_done(&self) -> bool {
        if !self.is_empty_head() {
            return false;
        }
        match &self.claims {
            ProjectionClaims::Elided => false,
            ProjectionClaims::Owned(claims) => !claims.is_empty(),
            #[cfg(feature = "parallel")]
            ProjectionClaims::Shared(claims) => !claims
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .is_empty(),
        }
    }

    /// Admits a complete raw binding before any user conversion or mapper code
    /// runs. Strict heads claim their projected key here; full heads rely on
    /// the upstream complete-binding uniqueness invariant. A failed
    /// conversion, `None`, or panic therefore cannot cause the same relational
    /// row to be retried through another witness.
    fn claim(&mut self, binding: &Binding) -> bool {
        if matches!(&self.claims, ProjectionClaims::Elided) {
            return true;
        }
        let key: ProjectionKey = self
            .head
            .iter()
            .map(|&variable| {
                *binding
                    .get(variable)
                    .expect("projection attempted before a head variable was bound")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        match &mut self.claims {
            ProjectionClaims::Elided => {
                unreachable!("elided claims returned before key allocation")
            }
            ProjectionClaims::Owned(claims) => claims.insert(key),
            #[cfg(feature = "parallel")]
            ProjectionClaims::Shared(claims) => claims
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(key),
        }
    }

    /// Maps a raw head already admitted by [`Self::claim`] with exactly the
    /// binding scope represented by that head. Hidden witnesses cannot affect
    /// a macro-created result through the doc-hidden constructor seam.
    fn project_claimed<P, R>(&mut self, binding: &Binding, postprocessing: &P) -> ProjectionStep<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        let mapped = if let Some(projected) = &mut self.mapper_binding {
            for &variable in self.head.iter() {
                projected.set(
                    variable,
                    binding
                        .get(variable)
                        .expect("projection attempted before a head variable was bound"),
                );
            }
            postprocessing(projected)
        } else {
            postprocessing(binding)
        };
        match mapped {
            Some(result) => ProjectionStep::Yield(result),
            None if self.is_empty_head() => ProjectionStep::Done,
            None => ProjectionStep::Skip,
        }
    }

    #[cfg(feature = "parallel")]
    fn share_for_parallel(&mut self) -> Option<Arc<Mutex<AHashSet<ProjectionKey>>>> {
        match &mut self.claims {
            ProjectionClaims::Elided => None,
            ProjectionClaims::Owned(claims) => {
                let shared = Arc::new(Mutex::new(std::mem::take(claims)));
                self.claims = ProjectionClaims::Shared(Arc::clone(&shared));
                Some(shared)
            }
            ProjectionClaims::Shared(claims) => Some(Arc::clone(claims)),
        }
    }

    #[cfg(feature = "parallel")]
    fn attach_shared(&mut self, claims: Option<Arc<Mutex<AHashSet<ProjectionKey>>>>) {
        if let Some(claims) = claims {
            self.claims = ProjectionClaims::Shared(claims);
        } else {
            assert!(
                matches!(&self.claims, ProjectionClaims::Elided),
                "parallel projection transfer cannot elide a stored claim domain"
            );
        }
    }
}

impl Clone for ProjectionGate {
    fn clone(&self) -> Self {
        let claims = match &self.claims {
            ProjectionClaims::Elided => ProjectionClaims::Elided,
            ProjectionClaims::Owned(claims) => ProjectionClaims::Owned(claims.clone()),
            #[cfg(feature = "parallel")]
            ProjectionClaims::Shared(claims) => ProjectionClaims::Owned(
                claims
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone(),
            ),
        };
        Self {
            head: self.head.clone(),
            claims,
            mapper_binding: self.mapper_binding.clone(),
        }
    }
}

/// A borrowed, row-major view over a block of partial bindings — the
/// operand of the [`Constraint`] protocol.
///
/// `vars` names the bound variables (one column per entry) and `rows`
/// holds [`len`](Self::len) rows of [`stride`](Self::stride) values each:
/// row `i`'s value for `vars[j]` is `rows[i * stride + j]`. Column order
/// is caller-chosen; residual-state cells use their canonical schema, so
/// constraints locate columns with [`col`](Self::col) and never assume a
/// layout.
///
/// A view constructed publicly with **no columns is the seed block: a single
/// zero-width row** (the empty binding). Batch executors may internally carry
/// several occurrences of that empty binding after splitting and remerging;
/// their explicit row count preserves that multiplicity even though `rows`
/// itself is necessarily empty. This is what makes level 0 an ordinary block
/// instead of a special case in the solver.
///
/// The view is `Copy` and borrows the solver's row storage directly. A
/// single-row view ([`row_view`](Self::row_view)) is a subslice of the
/// parent block, not a copy — the borrowed cursor that lets per-row
/// fallbacks run without any scratch [`Binding`].
#[derive(Clone, Copy, Debug)]
pub struct RowsView<'v> {
    /// The bound variables — the column layout of `rows`.
    pub vars: &'v [VariableId],
    /// Row-major value store: `len() * stride()` entries.
    pub rows: &'v [RawInline],
    /// Optional O(1) variable→column index: `cols[v]` is the column of
    /// variable `v`, [`COL_UNBOUND`] when unbound. Canonical multi-parent
    /// frontiers normally pass `None` and amortize the [`col`](Self::col)
    /// scan over the whole block; callers with a maintained index may supply
    /// it for O(1) lookup.
    cols: Option<&'v [u8; 128]>,
    /// Row count, computed once at construction. Kept as a field so
    /// [`len`](Self::len) — called on every verb of every constraint —
    /// is a load instead of an integer division (`rows.len() / stride`).
    n_rows: usize,
}

/// Sentinel in a [`RowsView`] column index: variable not bound.
pub const COL_UNBOUND: u8 = u8::MAX;

impl<'v> RowsView<'v> {
    /// The seed view: no bound variables, one zero-width row.
    pub const EMPTY: RowsView<'static> = RowsView {
        vars: &[],
        rows: &[],
        cols: None,
        n_rows: 1,
    };

    /// Creates a view over `rows` laid out in `vars` column order.
    pub fn new(vars: &'v [VariableId], rows: &'v [RawInline]) -> Self {
        debug_assert!(vars.is_empty() || rows.len().is_multiple_of(vars.len()));
        let n_rows = match vars.len() {
            0 => 1,
            stride => rows.len() / stride,
        };
        RowsView {
            vars,
            rows,
            cols: None,
            n_rows,
        }
    }

    /// Creates an engine-internal view with an explicit row count.
    ///
    /// Unlike [`new`](Self::new), this can represent zero, one, or several
    /// zero-width rows. That distinction cannot be inferred from `rows.len()`
    /// when `vars` is empty, but it matters when equivalent empty bindings
    /// reconverge in a blocked worklist.
    pub(crate) fn new_with_row_count(
        vars: &'v [VariableId],
        rows: &'v [RawInline],
        n_rows: usize,
    ) -> Self {
        let expected = vars
            .len()
            .checked_mul(n_rows)
            .expect("RowsView dimensions overflow");
        assert_eq!(
            rows.len(),
            expected,
            "RowsView storage disagrees with its explicit dimensions"
        );
        RowsView {
            vars,
            rows,
            cols: None,
            n_rows,
        }
    }

    /// Creates a view with a caller-maintained variable→column index
    /// (`cols[v]` = column of `v`, [`COL_UNBOUND`] otherwise), making
    /// [`col`](Self::col) O(1).
    pub fn new_indexed(vars: &'v [VariableId], rows: &'v [RawInline], cols: &'v [u8; 128]) -> Self {
        debug_assert!(vars.is_empty() || rows.len().is_multiple_of(vars.len()));
        debug_assert!(vars.iter().enumerate().all(|(i, &v)| cols[v] as usize == i));
        let n_rows = match vars.len() {
            0 => 1,
            stride => rows.len() / stride,
        };
        RowsView {
            vars,
            rows,
            cols: Some(cols),
            n_rows,
        }
    }

    /// Number of values per row (= number of bound variables).
    #[inline]
    pub fn stride(&self) -> usize {
        self.vars.len()
    }

    /// Number of rows. Public zero-column views have one virtual seed row;
    /// internal blocked views can preserve multiple empty-row occurrences.
    #[inline]
    pub fn len(&self) -> usize {
        self.n_rows
    }

    /// `true` when the view holds no rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The `i`-th row as a value slice.
    #[inline]
    pub fn row(&self, i: usize) -> &'v [RawInline] {
        let stride = self.vars.len();
        &self.rows[i * stride..(i + 1) * stride]
    }

    /// A single-row view of row `i` — a borrowed cursor, no copy.
    #[inline]
    pub fn row_view(&self, i: usize) -> RowsView<'v> {
        RowsView {
            vars: self.vars,
            rows: self.row(i),
            cols: self.cols,
            n_rows: 1,
        }
    }

    /// The column index of `variable`, or `None` when it is unbound.
    /// O(1) with a column index ([`new_indexed`](Self::new_indexed)),
    /// otherwise a scan of `vars`.
    #[inline]
    pub fn col(&self, variable: VariableId) -> Option<usize> {
        match self.cols {
            Some(cols) => match cols[variable] {
                COL_UNBOUND => None,
                c => Some(c as usize),
            },
            None => self.vars.iter().position(|&v| v == variable),
        }
    }

    /// Iterates the rows as value slices (empty slices for zero-width rows).
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &'v [RawInline]> + use<'v> {
        let stride = self.vars.len();
        let rows = self.rows;
        let len = self.n_rows;
        (0..len).map(move |i| &rows[i * stride..(i + 1) * stride])
    }

    /// Bound-variable schema shared by every row in this block.
    #[inline]
    pub(crate) fn bound(&self) -> VariableSet {
        self.vars
            .iter()
            .copied()
            .fold(VariableSet::new_empty(), |mut bound, variable| {
                bound.set(variable);
                bound
            })
    }
}

/// The ragged candidate matrix of batch execution: `(row, value)` pairs in COO
/// form, **grouped by ascending row index**. The residual executor owns
/// buffers of this type and lends them to the protocol through
/// [`CandidateSink::Tagged`].
pub type Candidates = Vec<(u32, RawInline)>;

/// The output sink of [`Constraint::propose`] / [`Constraint::confirm`] —
/// the representation-generic seam that lets one protocol serve both
/// tagged blocks and compact single-parent frontiers with zero ceremony:
///
/// - [`Tagged`](Self::Tagged) lends a [`Candidates`] pair buffer — the
///   residual solver's ragged COO frontier, `(row, value)` grouped by ascending
///   row index.
/// - [`Values`](Self::Values) lends a plain `Vec<RawInline>` for any
///   single-parent residual frontier. The row index is
///   statically 0 and **no `u32` tag is ever materialized**; callers must pass
///   single-row views (`view.len() == 1`). Storage shape does not select an
///   execution backend: a constraint may batch the values through the same
///   CPU or accelerator operation used for tagged candidates.
///
/// A trait with generic verbs would say the same thing, but the protocol
/// must stay object-safe (`and!`/`or!` compose `Box<dyn Constraint>`
/// trees), so the sink is a concrete two-variant type instead. The
/// closure-taking methods ([`extend_row`](Self::extend_row),
/// [`retain`](Self::retain), [`for_each`](Self::for_each)) match on the
/// variant **once per call** and run a monomorphized loop per arm, so
/// nothing representation-dependent survives into the hot loops.
pub enum CandidateSink<'s> {
    /// `(row, value)` pairs, grouped by ascending row for multi-parent
    /// frontiers.
    Tagged(&'s mut Candidates),
    /// Plain values for any single-parent view, with implicit row index zero.
    Values(&'s mut Vec<RawInline>),
}

impl CandidateSink<'_> {
    /// Appends one candidate for parent row `row`.
    #[inline]
    pub fn push(&mut self, row: u32, value: RawInline) {
        match self {
            Self::Tagged(pairs) => pairs.push((row, value)),
            Self::Values(values) => values.push(value),
        }
    }

    /// Appends a run of candidates for parent row `row`. The variant
    /// match is hoisted out of the iteration.
    #[inline]
    pub fn extend_row(&mut self, row: u32, values: impl IntoIterator<Item = RawInline>) {
        match self {
            Self::Tagged(pairs) => pairs.extend(values.into_iter().map(|v| (row, v))),
            Self::Values(out) => out.extend(values),
        }
    }

    /// Number of candidates currently in the sink.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Tagged(pairs) => pairs.len(),
            Self::Values(values) => values.len(),
        }
    }

    /// `true` when the sink holds no candidates.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reserves capacity for at least `additional` more candidates.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        match self {
            Self::Tagged(pairs) => pairs.reserve(additional),
            Self::Values(values) => values.reserve(additional),
        }
    }

    /// Visits every `(row, value)` candidate in order.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u32, &RawInline)) {
        match self {
            Self::Tagged(pairs) => {
                for (row, value) in pairs.iter() {
                    f(*row, value);
                }
            }
            Self::Values(values) => {
                for value in values.iter() {
                    f(0, value);
                }
            }
        }
    }

    /// Order-preserving retain by `(row, &value)` predicate — the confirm
    /// primitive. Preserves the row grouping by construction.
    #[inline]
    pub fn retain(&mut self, mut f: impl FnMut(u32, &RawInline) -> bool) {
        match self {
            Self::Tagged(pairs) => pairs.retain(|(row, value)| f(*row, value)),
            Self::Values(values) => values.retain(|value| f(0, value)),
        }
    }
}

/// The output sink of [`Constraint::estimate`]: one estimate per row of
/// the block.
///
/// - [`Column`](Self::Column) appends per-row estimates to a column vector
///   for multi-parent frontiers.
/// - [`Scalar`](Self::Scalar) writes a single-row view's estimate
///   straight into a stack slot with no `Vec` round-trip.
pub enum EstimateSink<'s> {
    /// One estimate per row, appended for a multi-parent frontier.
    Column(&'s mut Vec<usize>),
    /// A single-row view's estimate, written in place.
    Scalar(&'s mut usize),
}

impl EstimateSink<'_> {
    /// Appends one row's estimate.
    #[inline]
    pub fn push(&mut self, estimate: usize) {
        match self {
            Self::Column(col) => col.push(estimate),
            Self::Scalar(slot) => **slot = estimate,
        }
    }

    /// Appends one estimate per row from an iterator. The variant match
    /// is hoisted out of the iteration.
    #[inline]
    pub fn extend(&mut self, estimates: impl IntoIterator<Item = usize>) {
        match self {
            Self::Column(col) => col.extend(estimates),
            Self::Scalar(slot) => {
                if let Some(e) = estimates.into_iter().next() {
                    **slot = e;
                }
            }
        }
    }

    /// Appends the same estimate for `n` rows — the uniform
    /// (binding-independent) case.
    #[inline]
    pub fn fill(&mut self, estimate: usize, n: usize) {
        match self {
            Self::Column(col) => col.extend(std::iter::repeat_n(estimate, n)),
            Self::Scalar(slot) => {
                debug_assert_eq!(n, 1, "Scalar sink is single-row");
                **slot = estimate;
            }
        }
    }
}

/// Groups a candidate frontier by row and lets `f` filter each row's
/// value group in place; a single-parent frontier is the untagged special
/// case.
/// `f` receives the row's values and the row's candidate values.
///
/// For a [`CandidateSink::Values`] sink this is a direct call on the borrowed
/// buffer — no grouping, no scratch, no copies.
pub fn confirm_per_row(
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
    mut f: impl FnMut(&[RawInline], &mut Vec<RawInline>),
) {
    match candidates {
        CandidateSink::Values(values) => f(view.row(0), values),
        CandidateSink::Tagged(pairs) => {
            // In-place compaction: survivors of each row group are written
            // back over the already-consumed prefix (confirm only ever
            // filters, so the write cursor can never overtake the read
            // cursor), and one value scratch is reused across groups.
            let mut scratch: Vec<RawInline> = Vec::new();
            let mut write = 0usize;
            let mut i = 0;
            while i < pairs.len() {
                let row_idx = pairs[i].0;
                scratch.clear();
                let mut j = i;
                while j < pairs.len() && pairs[j].0 == row_idx {
                    scratch.push(pairs[j].1);
                    j += 1;
                }
                f(view.row(row_idx as usize), &mut scratch);
                debug_assert!(
                    scratch.len() <= j - i,
                    "confirm must filter candidates, never add them"
                );
                for &val in &scratch {
                    pairs[write] = (row_idx, val);
                    write += 1;
                }
                i = j;
            }
            pairs.truncate(write);
        }
    }
}

/// Structural shape exposed to query-engine lowering.
///
/// This is deliberately not part of the ordinary constraint protocol. It lets
/// shape-aware engines flatten associative conjunctions without teaching them
/// the concrete Rust type of every constraint. Ordinary [`Query`] selection
/// may consume an exposed shape; semantic wrappers and custom constraints
/// remain opaque unless they explicitly opt in to exposing one.
#[doc(hidden)]
#[non_exhaustive]
#[derive(Clone, Copy)]
pub enum ConstraintShape<'s, 'a> {
    /// One indivisible ordinary constraint occurrence.
    Opaque,
    /// An associative logical conjunction whose children may be inspected.
    And(&'s dyn ConstraintChildren<'a>),
}

/// Borrow-free cursor for an immutable typed Program source frontier.
///
/// The cursor is opaque Program payload, never part of canonical residual
/// state identity. A source must choose one cursor family and retain it for
/// the continuation. `After(value)` resumes strictly after `value` in
/// raw-inline lexicographic order. `Offset(index)` resumes at a strictly later
/// ordinal position in an immutable constraint-owned sequence whose native
/// order need not agree with raw-inline order.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResidualDeltaSourceCursor {
    Start,
    After(RawInline),
    Offset(u64),
}

/// Result metadata for one bounded typed Program source page.
///
/// `examined` counts source candidates consumed from the ordered source
/// frontier, including candidates rejected by an exact secondary filter. It
/// must not exceed the requested page limit. `next == None` proves source
/// exhaustion; otherwise the returned cursor resumes strictly after every
/// candidate examined by this page.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidualDeltaSourcePage {
    pub next: Option<ResidualDeltaSourceCursor>,
    pub examined: usize,
}

/// Structural proof carried by a constraint's proposal for one variable.
///
/// For a constraint occurrence `C`, target variable `x`, bound-variable
/// schema `B`, and one row `b`, let `F_C(x | b)` be the set of values that
/// extend `b` to at least one complete solution of `C`. Coverage compares
/// that existential fiber with the **support** of
/// `C.propose(x, b)`; proposal occurrence multiplicity is deliberately not
/// part of the receipt.
///
/// The variants form the proof-strength order
/// [`None`](Self::None) < [`Covering`](Self::Covering) <
/// [`Exact`](Self::Exact). This order is suitable for conservative meets,
/// but it is not a cardinality estimate and must never be inferred from one.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[must_use]
pub enum ProposalCoverage {
    /// No completeness claim is made for this proposal.
    ///
    /// The constraint may still validate candidates proposed elsewhere, but
    /// this occurrence is not a sound source for the target variable.
    #[default]
    None,

    /// The proposal support contains the complete existential fiber.
    ///
    /// False positives are permitted, so the proposing occurrence must still
    /// confirm its own candidates before they are considered semantically
    /// admitted.
    Covering,

    /// The proposal support equals the complete existential fiber.
    ///
    /// Physical duplicate occurrences are permitted; exactness concerns only
    /// which distinct values occur in the proposal.
    Exact,
}

/// Logarithmic unit-work tier for one proposal candidate occurrence.
///
/// Rank `r` in `0..=63` represents the broad capability class `2^r`. Ranks are
/// static backend properties: they may depend on the target axis and
/// bound-variable schema, but never on row values, observed timings, frontier
/// width, or scheduler state. The engine uses only their ordering and integer
/// weights; they do not participate in semantic receipts or action identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalUnitClass(u8);

impl ProposalUnitClass {
    /// Largest public tier. With at most `usize::MAX` occurrences, summing
    /// rank-63 peer weights remains representable in the model's `u128` work
    /// domain, so exact-source subtraction cannot erase other peers.
    pub const MAX_LOG2_RANK: u8 = 63;

    /// Direct iteration through a hash table's occupied entries.
    pub const HASH_TABLE_ENUMERATION: Self = Self(0);

    /// Ordered enumeration from a succinct index range.
    pub const SUCCINCT_ORDERED_ENUMERATION: Self = Self(0);

    /// Defines a backend capability tier by its base-two rank.
    pub const fn from_log2_rank(rank: u8) -> Self {
        assert!(rank <= Self::MAX_LOG2_RANK, "unit-work rank exceeds 63");
        Self(rank)
    }

    /// Returns the base-two rank of this capability tier.
    pub const fn log2_rank(self) -> u8 {
        self.0
    }
}

/// Logarithmic unit-work tier for confirming one candidate occurrence.
///
/// Rank `r` in `0..=63` represents the broad capability class `2^r`. Like
/// [`ProposalUnitClass`], this is immutable planning metadata rather than a
/// runtime measurement or semantic capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfirmationUnitClass(u8);

impl ConfirmationUnitClass {
    /// Largest public tier; see [`ProposalUnitClass::MAX_LOG2_RANK`].
    pub const MAX_LOG2_RANK: u8 = ProposalUnitClass::MAX_LOG2_RANK;

    /// One locality-friendly hash membership probe.
    pub const HASH_TABLE_MEMBERSHIP: Self = Self(0);

    /// A domain search followed by dependent random rank/select probes.
    ///
    /// Rank 5 is the broad 32x capability tier: calibration places this
    /// operation around 35x a sequential/hash unit, with a crossover near
    /// 15x. The tier describes the access pattern, not any particular query.
    pub const SUCCINCT_RANDOM_MEMBERSHIP: Self = Self(5);

    /// Defines a backend capability tier by its base-two rank.
    pub const fn from_log2_rank(rank: u8) -> Self {
        assert!(rank <= Self::MAX_LOG2_RANK, "unit-work rank exceeds 63");
        Self(rank)
    }

    /// Returns the base-two rank of this capability tier.
    pub const fn log2_rank(self) -> u8 {
        self.0
    }
}

/// Static directed unit costs for one constraint occurrence and target.
///
/// Proposal and confirmation are deliberately separate: flattened residual
/// planning prices choosing source `S` as the number of candidate
/// **occurrences** quoted by `S`, multiplied by `S`'s proposal unit, one engine
/// SET-admission unit, and the confirmation units of every occurrence that
/// must validate those candidates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionUnitClasses {
    pub proposal: ProposalUnitClass,
    pub confirmation: ConfirmationUnitClass,
}

impl ActionUnitClasses {
    /// Creates one immutable pair of directed unit-work tiers.
    pub const fn new(proposal: ProposalUnitClass, confirmation: ConfirmationUnitClass) -> Self {
        Self {
            proposal,
            confirmation,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ActionCostPeer {
    pub occurrence: usize,
    pub coverage: ProposalCoverage,
    pub classes: Option<ActionUnitClasses>,
}

fn unit_class_weight(rank: u8) -> u128 {
    1u128.checked_shl(rank.into()).unwrap_or(u128::MAX)
}

/// Every proposal occurrence crosses the engine's SET-admission boundary
/// before descendants observe it. Keep that engine work explicit rather than
/// hiding it inside a backend proposal class.
const SET_ADMISSION_LOG2_RANK: u8 = 0;
const SET_ADMISSION_UNIT_WEIGHT: u128 = 1u128 << SET_ADMISSION_LOG2_RANK;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectedActionModel {
    confirmation_weight: u128,
}

impl DirectedActionModel {
    pub(crate) fn new(peers: &[ActionCostPeer]) -> Option<Self> {
        let mut confirmation_weight = 0u128;
        for peer in peers {
            let classes = peer.classes?;
            confirmation_weight = confirmation_weight
                .saturating_add(unit_class_weight(classes.confirmation.log2_rank()));
        }
        Some(Self {
            confirmation_weight,
        })
    }

    /// Prices one source as `occurrences × directed unit work`.
    ///
    /// An exact source need not confirm its own output; a covering source does.
    /// Saturation stops below `usize::MAX`, which remains the engine's sentinel
    /// for an unknown cardinality estimate. A missing source count conservatively
    /// retains that sentinel.
    pub(crate) fn planning_cost(self, source: ActionCostPeer, candidate_count: usize) -> usize {
        if candidate_count == usize::MAX {
            return usize::MAX;
        }
        let classes = source
            .classes
            .expect("a directed model contains classes for every occurrence");
        let own_confirmation = unit_class_weight(classes.confirmation.log2_rank());
        let confirmation_weight = if source.coverage == ProposalCoverage::Exact {
            self.confirmation_weight.saturating_sub(own_confirmation)
        } else {
            self.confirmation_weight
        };
        let unit_weight = unit_class_weight(classes.proposal.log2_rank())
            .saturating_add(SET_ADMISSION_UNIT_WEIGHT)
            .saturating_add(confirmation_weight);
        (candidate_count as u128)
            .saturating_mul(unit_weight)
            .min((usize::MAX - 1) as u128) as usize
    }
}

/// Object-safe child access for a structural constraint shape.
#[doc(hidden)]
pub trait ConstraintChildren<'a> {
    /// Number of direct child occurrences.
    fn len(&self) -> usize;

    /// Borrows one direct child occurrence.
    ///
    /// Repeated references to the same constraint object at different indices
    /// remain distinct occurrences to a lowering engine.
    fn child(&self, index: usize) -> &dyn Constraint<'a>;
}

/// The cooperative protocol that every query participant implements.
///
/// A constraint restricts the values that can be assigned to query
/// variables. The query engine does not plan joins in advance; instead it
/// consults constraints directly during a search over partial bindings.
/// The protocol is **block-native**: every method operates on a
/// [`RowsView`] — a block of sibling partial bindings that share the same
/// bound-variable set — and candidates travel through a representation-
/// generic [`CandidateSink`]. One binding at a time is simply the one-row
/// special case: one-parent frontiers pass single-row views with a plain-value
/// [`CandidateSink::Values`] sink and pay no row tags, while multi-parent
/// frontiers use a [`CandidateSink::Tagged`] pair sink. Constraints with
/// batchable probe streams may evaluate either representation in one pass —
/// cache-friendly on the CPU and suitable for accelerator backends.
///
/// # The protocol
///
/// | Method | Role | Called |
/// |--------|------|--------|
/// | [`variables`](Constraint::variables) | Declares which variables the constraint touches. | Once, at query start. |
/// | [`estimate`](Constraint::estimate) | Predicts per-row candidate counts for a variable. | Before each binding decision. |
/// | [`propose`](Constraint::propose) | Enumerates candidate values per row. | On the most selective constraint. |
/// | [`confirm`](Constraint::confirm) | Filters candidates proposed by another constraint. | On all remaining constraints. |
/// | [`satisfied`](Constraint::satisfied) | Checks whether fully-bound sub-constraints still hold. | Inside composite constraints. |
///
/// [`proposal_coverage`](Constraint::proposal_coverage) is the structural
/// source-eligibility receipt. A Covering source is confirmed before its
/// candidates cross a relational boundary.
///
/// # Fixed relational semantics
///
/// Every constraint occurrence denotes one fixed raw-inline SET relation over
/// [`variables`](Constraint::variables). Its ordinary, paged, typed-Program,
/// and complete-equivalent routes MUST agree on that relation.
/// Across positive snapshots, data growth is monotone: if `D` is a subset of
/// `D'`, then the relation at `D` is a subset of the relation at `D'`.
/// Fixed-per-solve denotation alone is not enough to establish this substrate
/// monotonicity.
/// Activation-local novelty keys exposed by an accelerated route must be
/// congruent for all future outputs: equal keys may not hide states with
/// different relational futures.
/// [`confirm`](Constraint::confirm) must produce a subbag of its input, retain
/// every occurrence whose value belongs to the existential fiber, and become
/// exact once every occurrence variable other than the target is bound.
/// Conservative false positives may depend on the complete candidate page
/// supplied to one call: confirmation is not required to be a homomorphism in
/// that candidate bag. Engines therefore SET-admit every newly proposed
/// `(parent, value)` before independently paging it; correctness is defined by
/// the final raw SET, not by equality of intermediate payloads or call traces.
/// [`satisfied`](Constraint::satisfied) returning `false` must prove that the
/// row has no completion, and it must be exact once all occurrence variables
/// are bound. Estimates are costs only: they cannot change relevance,
/// coverage, or the denoted relation.
///
/// # Statelessness
///
/// Constraints are stateless: every method receives the current block as
/// a borrowed view rather than maintaining internal bookkeeping. This
/// lets the solver page actions or reorder canonical residual-state cells
/// freely without notifying the constraints.
///
/// # Structural relevance
///
/// Whether a constraint has an opinion about a variable is **structural**: it
/// depends only on the variable's identity and bound-variable schema, never on
/// bound *values*. [`variables`](Constraint::variables) defines validator
/// relevance and
/// [`proposal_coverage`](Constraint::proposal_coverage) defines source
/// eligibility. [`estimate`](Constraint::estimate) is then only an optional
/// cost quote; returning `false` assigns unknown cost rather than erasing the
/// occurrence.
///
/// # Row homomorphism
///
/// Every row-taking protocol verb is row-local. If a block is split into
/// non-empty consecutive sub-blocks, evaluating those sub-blocks independently
/// and concatenating their outputs (with candidate row tags remapped to the
/// original rows) MUST be equivalent to evaluating the original block at once:
///
/// - `estimate` yields the concatenation of the per-sub-block estimate columns;
/// - `propose` yields the concatenation of the per-sub-block candidate groups;
/// - `confirm` keeps exactly the candidates that their own row would keep; and
/// - `satisfied` on the whole block is the conjunction of `satisfied` on the
///   sub-blocks.
///
/// Implementations may fuse scans or accelerator dispatch across many rows,
/// but must not use block-global top-k limits, first-row decisions, or any
/// other operation whose answers change when the engine chunks, reconverges,
/// or parallel-shards a frontier. Violating this law can add or remove query
/// results merely by changing scheduler width.
///
/// Diagnostic side effects may observe those call boundaries, but MUST NOT
/// feed back into any estimate, candidate, confirmation, or satisfaction
/// answer.
///
/// # Action identity and SET admission
///
/// Every constraint supplies one fixed relation. Receipt-aware engines may
/// regroup rows among sound sources and collapse occurrence multiplicity at
/// explicit SET boundaries, provided they preserve the same raw projected
/// tuples. Estimates are costs only and never authorize a semantic rewrite.
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
/// A new constraint needs [`variables`](Constraint::variables),
/// [`estimate`](Constraint::estimate), [`propose`](Constraint::propose),
/// and [`confirm`](Constraint::confirm). Constraints without batch
/// structure loop over [`RowsView::iter`] and push per row (see
/// [`CandidateSink::extend_row`]), or filter per row with the
/// [`confirm_per_row`] adapter. Override
/// [`satisfied`](Constraint::satisfied) when the constraint can detect
/// unsatisfiability early (e.g. a fully-bound triple lookup that found no
/// match). Every occurrence used as a source must publish an appropriate
/// [`proposal_coverage`](Constraint::proposal_coverage) receipt.
pub trait Constraint<'a> {
    /// Returns the set of variables this constraint touches.
    ///
    /// Called once at query start. The engine uses this to determine which
    /// constraints participate when a particular variable is being bound.
    fn variables(&self) -> VariableSet;

    /// Returns the proposal proof for `variable` under bound schema `bound`.
    ///
    /// A non-[`None`](ProposalCoverage::None) result is legal only when
    /// `variable` belongs to [`Self::variables`]. The result is structural for
    /// this occurrence, target, and bound-variable set: it may depend on
    /// `bound`, but never on row values, estimates, route availability, page
    /// size, execution placement, or scheduler width.
    ///
    /// The default makes no source claim. A confirmation-only constraint can
    /// therefore retain this default.
    fn proposal_coverage(&self, _variable: VariableId, _bound: VariableSet) -> ProposalCoverage {
        ProposalCoverage::None
    }

    /// Optionally publishes static directed unit costs for one target action.
    ///
    /// Returning `Some` promises that, whenever
    /// [`proposal_coverage`](Constraint::proposal_coverage) is at least
    /// [`Covering`](ProposalCoverage::Covering), each value emitted by
    /// [`estimate`](Constraint::estimate) for the same target and bound schema
    /// is the number of physical candidate **occurrences** that
    /// [`propose`](Constraint::propose) would produce for that row before
    /// intersection confirmation. It is not the number of distinct values
    /// unless the proposal itself is distinct.
    ///
    /// The classes must describe broad, immutable backend capabilities. They
    /// may depend on `variable` and `bound`, but never on row values, sampled
    /// timings, frontier width, or scheduler state. They affect planning only:
    /// they neither strengthen proposal coverage nor enter canonical state,
    /// route, or action identity. The flattened residual Ready planner uses
    /// directed pricing only when every relevant occurrence opts in;
    /// otherwise the complete action falls back atomically to plain
    /// cardinality-estimate ordering. Directed prices select a source only
    /// within one variable; cross-variable ordering continues to compare raw
    /// source counts. The count promise above means an opted-in source cannot
    /// lawfully return an unknown quote; the planner nevertheless preserves
    /// `usize::MAX` if it does.
    ///
    /// A confirmation-only occurrence may return `Some` without providing a
    /// candidate count because its proposal class is not consulted. The
    /// conservative default declines directed pricing.
    fn action_unit_classes(
        &self,
        _variable: VariableId,
        _bound: VariableSet,
    ) -> Option<ActionUnitClasses> {
        None
    }

    /// Estimates the number of candidate values for `variable` for
    /// **every row** of the block, pushing one estimate per row into
    /// `out`.
    ///
    /// Returns `false` leaving `out` untouched. That means no cost quote is
    /// available: structural relevance comes from
    /// [`variables`](Constraint::variables), source eligibility comes from
    /// [`proposal_coverage`](Constraint::proposal_coverage), and the engine
    /// uses `usize::MAX` as the unknown cost.
    ///
    /// Estimates need not equal the eventual candidate count unless this
    /// occurrence opts into
    /// [`action_unit_classes`](Constraint::action_unit_classes). Tighter
    /// quotes improve adaptive ordering, but cannot change the denoted
    /// relation; see the [Atreides join](crate) family for how estimate
    /// fidelity affects performance.
    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool;

    /// Enumerates candidate values for `variable` for every row of the
    /// block, pushing `(row, value)` candidates into the sink grouped by
    /// ascending row index.
    ///
    /// Called on the constraint with the lowest estimate for the variable
    /// being bound. Does nothing when `variable` is not constrained by
    /// this constraint.
    ///
    /// # Protocol law: the sink is always empty
    ///
    /// `propose` is always handed an **empty** sink. The engine clears the
    /// candidate sink before every call, and composite constraints must
    /// preserve the invariant when delegating: every candidate in the sink
    /// belongs to the callee, which may therefore append, filter, sort, and
    /// deduplicate the sink freely (an
    /// [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint)
    /// lets its tightest child propose and then filters the sink through the
    /// remaining children's [`confirm`](Constraint::confirm)).
    ///
    /// The dual obligation falls on composites that invoke more than one
    /// child `propose` for the same sink:
    /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
    /// hands each variant its own empty buffer and merges the independent
    /// outputs afterwards. Sharing one sink across variants would let a
    /// filtering variant delete candidates another variant produced — the
    /// result would depend on variant order and adding data could remove
    /// results, violating the substrate's monotonicity guarantee.
    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    );

    /// Filters `candidates`, removing `(row, value)` candidates whose
    /// value violates this constraint under that row's bindings, while
    /// preserving the row grouping ([`CandidateSink::retain`] does).
    ///
    /// Called on every constraint *except* the one that proposed, in
    /// order of increasing estimate. Does nothing when `variable` is not
    /// constrained by this constraint.
    ///
    /// This is weak support refinement. The result must be a subbag, must keep
    /// every candidate in this occurrence's existential fiber, and must be
    /// exact when all of the occurrence's other variables are bound. It may
    /// conservatively keep different false positives when the same admitted
    /// candidate SET is presented in different pages; candidate-page
    /// homomorphism is deliberately not part of the protocol.
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    );

    /// Returns whether **every row** of the block is consistent with this
    /// constraint.
    ///
    /// # Protocol law: exact when fully bound
    ///
    /// While at least one of this constraint's variables is unbound,
    /// `satisfied` may answer an optimistic `true` (the default
    /// implementation). Once **all** of the constraint's variables are
    /// bound (in every row of the block) the answer MUST be exact: `true`
    /// if and only if the bound values jointly satisfy the constraint in
    /// every row — for example, a `TribleSetConstraint` whose entity,
    /// attribute, and value are all bound must perform the membership
    /// check rather than defaulting to `true`.
    ///
    /// Exactness is a soundness requirement, not an optimisation:
    /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
    /// relies on `satisfied` to detect dead variants when it propose/confirms
    /// *other* variables of the union. A leaf that leaves the optimistic
    /// default lets a dead variant keep proposing, producing rows that no
    /// single variant would accept.
    ///
    /// Composite constraints propagate this check to their children with
    /// single-row views: [`IntersectionConstraint`](crate::query::intersectionconstraint::IntersectionConstraint)
    /// requires *all* children to be satisfied, while
    /// [`UnionConstraint`](crate::query::unionconstraint::UnionConstraint)
    /// requires *at least one* per row. The union uses this to skip dead
    /// variants in propose and confirm, preventing values from a
    /// satisfied variant from leaking through a dead one.
    fn satisfied(&self, _view: &RowsView<'_>) -> bool {
        true
    }

    /// Exposes associative structure to shape-aware residual lowering.
    ///
    /// The default keeps the constraint opaque. Implementations must expose
    /// only structure whose flattening preserves the ordinary protocol's
    /// semantics. Wrappers that change scope, multiplicity, or evaluation
    /// meaning should retain the default. The exposed shape must be a finite,
    /// acyclic tree. Its variants, child counts, and child order are structural
    /// facts and MUST remain stable for the entire query execution. A path-based
    /// engine may resolve the plan repeatedly, so changing shape through
    /// interior mutability can silently select a different constraint occurrence
    /// even when every individual borrow is memory-safe.
    #[doc(hidden)]
    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        ConstraintShape::Opaque
    }

    /// Exposes the finite arms of an otherwise opaque logical union.
    ///
    /// The production compiler exposes this capability to canonical formula
    /// control. The child count and order are structural facts and must remain
    /// stable for the solve.
    #[doc(hidden)]
    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        None
    }

    /// Exposes one immutable typed residual-program family.
    ///
    /// Occurrence identity and query-local runtime state are owned by the
    /// residual lowering engine; sharing one constraint object at several
    /// structural paths must therefore still produce isolated runtimes.
    #[doc(hidden)]
    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        None
    }

    /// Proposal receipt for a typed residual Program route.
    ///
    /// The default inherits the ordinary proposal receipt. A Program may
    /// only strengthen it: the returned value MUST be at least
    /// [`proposal_coverage`](Constraint::proposal_coverage) in the proof order.
    /// Its accepted support must still contain the complete existential fiber,
    /// and `Exact` must equal that fiber. The accepted stream may therefore be
    /// narrower than a conservative ordinary proposal bag while carrying a
    /// stronger receipt. This is useful for a traversal which exposes eager
    /// covering seeds but publishes only witnessed endpoints from its typed
    /// fixpoint. The receipt is consulted only after
    /// [`Self::residual_program`] accepts the exact `Propose(variable)` request.
    /// It must be structural in `bound` and identical across typed CPU and
    /// physical execution.
    #[doc(hidden)]
    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        self.proposal_coverage(variable, bound)
    }
}

/// Stable diagnostic for a frontier that cannot enumerate any remaining
/// variable. A source may become available after another variable is bound
/// (Equality is the canonical example), so callers must apply this only to the
/// exact bound schema they are about to execute.
pub(super) const SOURCE_FRONTIER_ERROR: &str =
    "a non-full query state has no covering proposal source; filter-only and peer-dependent constraints require an enumerable source";

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for Box<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        let inner: &T = self;
        inner.proposal_coverage(variable, bound)
    }

    fn action_unit_classes(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> Option<ActionUnitClasses> {
        let inner: &T = self;
        inner.action_unit_classes(variable, bound)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.satisfied(view)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        let inner: &T = self;
        inner.residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        let inner: &T = self;
        inner.residual_union_children()
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        let inner: &T = self;
        inner.residual_program()
    }

    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        let inner: &T = self;
        inner.residual_program_proposal_coverage(variable, bound)
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for std::sync::Arc<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        let inner: &T = self;
        inner.proposal_coverage(variable, bound)
    }

    fn action_unit_classes(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> Option<ActionUnitClasses> {
        let inner: &T = self;
        inner.action_unit_classes(variable, bound)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.satisfied(view)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        let inner: &T = self;
        inner.residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        let inner: &T = self;
        inner.residual_union_children()
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        let inner: &T = self;
        inner.residual_program()
    }

    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        let inner: &T = self;
        inner.residual_program_proposal_coverage(variable, bound)
    }
}

/// A query is an iterator over the results of a query.
/// It takes a constraint and a post-processing function as input,
/// and returns the results of the query as a stream of values.
/// Every live fresh ordinary iterator uses canonical
/// residual states. It starts with narrow, depth-first action cohorts and
/// widens as the consumer keeps pulling, while histories with identical future
/// computation can reconverge under one state identity. The production
/// plan flattens exposed associative AND regions, lowers finite Union leaves
/// into continuations, and executes returned regular-path Program routes as
/// heterogeneous state actions. Constraints without a selected typed route
/// use the ordinary constraint action. Seed-rejected queries start no runtime.
/// Strict-projection keys are claimed
/// before Rust conversion, so conversion failure or panic never retries the
/// same raw row through another witness. Full heads need no terminal claim
/// table: engine action admission already makes complete raw bindings unique,
/// and a full projection is injective.
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
    /// Raw strict-projection identity and any keys claimed by this exact
    /// iterator snapshot. Full heads carry an elided marker instead.
    projection: ProjectionGate,
    /// Exact zero-or-one-row seed relation, retained until the residual cursor
    /// is first materialized.
    seed: Option<residual::FrameSeedRow>,
    /// Whether [`Iterator::next`] has ever been called on this query.
    ///
    /// Cursor shape cannot encode freshness: an untouched failed zero-variable
    /// settlement and a successfully drained zero-variable query are both
    /// `Done` with empty cursor state. This bit also records a failed `next()`
    /// call, giving freshness the simple exact meaning "the iterator has never
    /// been pulled."
    iteration_started: bool,
    /// Lazily initialized canonical residual-state cursor. The box owns only
    /// a borrow-free lowering plan plus raw machine state; `constraint` and
    /// `postprocessing` remain owned by this `Query`.
    residual: Option<Box<residual::ResidualQueryState>>,
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
        // The residual cursor contains only raw bindings, never projected
        // `R`s, so a clone snapshots the exact remaining search without
        // requiring the output type itself to implement `Clone`.
        Self {
            constraint: self.constraint.clone(),
            postprocessing: self.postprocessing.clone(),
            projection: self.projection.clone(),
            seed: self.seed.clone(),
            iteration_started: self.iteration_started,
            residual: self.residual.clone(),
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Query<C, P, R> {
    /// Create a new query.
    /// The query takes a constraint and a post-processing function as input,
    /// and returns the results of the query as a stream of values.
    /// The post-processing function returns `Option<R>`: returning `None`
    /// skips the current binding and continues the search. The complete set of
    /// variables named by the constraint is the raw SET projection head, so
    /// each byte-identical full binding reaches post-processing at most once.
    /// Because that complete-binding uniqueness is established inside the
    /// engine, the injective full head needs no terminal claim table. The raw
    /// binding is consumed before post-processing, so `None` or a panic cannot
    /// retry it.
    ///
    /// This method is usually not called directly, but rather through the [find!] macro,
    ///
    /// # Panics
    ///
    /// Panics when a non-empty root survives its exact seed check but no
    /// variable has a covering proposal source at the empty binding.
    /// Confirmation-only and peer-dependent constraints must be paired with
    /// an enumerable source.
    pub fn new(constraint: C, postprocessing: P) -> Self {
        let variables = constraint.variables();
        let projection = ProjectionGate::full(variables);
        Self::new_inner(constraint, postprocessing, variables, projection)
    }

    /// Constructs a query with an explicit relational projection head.
    ///
    /// This is the macro expansion seam for [`find!`](crate::find). It is not
    /// a bag-mode control: every supplied head still has public SET semantics.
    /// The postprocessor sees only variables in `head`; hidden witnesses are
    /// absent from its [`Binding`].
    /// Direct callers should normally use [`Query::new`], whose head is the
    /// complete constraint-variable set.
    #[doc(hidden)]
    pub fn new_projected<const N: usize>(
        constraint: C,
        head: [VariableId; N],
        postprocessing: P,
    ) -> Self {
        let variables = constraint.variables();
        let projection = ProjectionGate::new(head, variables);
        Self::new_inner(constraint, postprocessing, variables, projection)
    }

    fn new_inner(
        constraint: C,
        postprocessing: P,
        variables: VariableSet,
        projection: ProjectionGate,
    ) -> Self {
        // Constraints whose variables are all constant [`Term`]s (e.g. a
        // fully-constant `pattern!` used as an existence check) have an
        // empty variable set, so the propose/confirm search never consults
        // them. Their truth is binding-independent and `satisfied` is exact
        // for them from the start (the fully-bound exactness law: zero
        // unbound variables). One check up front settles every such
        // subtree; constraints with unbound variables answer an optimistic
        // `true` here and are validated by the search as usual.
        // `RowsView::EMPTY` is the seed block (a single zero-width row —
        // the empty binding), so this is the block-native form of the
        // empty-binding probe.
        let seed = residual::seed_survives(&constraint, VariableSet::new_empty(), &RowsView::EMPTY)
            .then(residual::FrameSeedRow::empty);
        if seed.is_some() && !variables.is_empty() {
            let has_initial_source = variables.into_iter().any(|variable| {
                constraint.proposal_coverage(variable, VariableSet::new_empty())
                    >= ProposalCoverage::Covering
            });
            assert!(has_initial_source, "{SOURCE_FRONTIER_ERROR}");
        }
        Query {
            constraint,
            postprocessing,
            projection,
            seed,
            iteration_started: false,
            residual: None,
        }
    }
}

/// Total ordering for a row's adaptive variable action. Lower variable IDs win
/// exact ordering-key ties without relying on unstable-sort tie behavior.
///
/// Smaller candidate-count magnitudes win. Counts in one power-of-two bucket
/// are equally specific, so lower variable IDs break those ties.
/// **Larger key = picked next.**
#[inline]
fn variable_choice_key(
    variable: VariableId,
    estimate: usize,
) -> (std::cmp::Reverse<u64>, std::cmp::Reverse<VariableId>) {
    (
        std::cmp::Reverse(estimate_magnitude(estimate)),
        std::cmp::Reverse(variable),
    )
}

#[inline]
fn estimate_magnitude(estimate: usize) -> u64 {
    estimate.checked_ilog2().map(|m| m + 1).unwrap_or(0) as u64
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Iterator for Query<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<Self::Item> {
        // Freshness is an explicit public-iterator property, not something
        // inferred from the cursor. Record the call before any iterator return
        // path, including a seed-rejected query.
        self.iteration_started = true;

        if self.projection.is_done() {
            return None;
        }

        if self.residual.is_none() {
            let seed = self.seed.take()?;
            self.residual = Some(Box::new(residual::ResidualQueryState::new(
                &self.constraint,
                Some(seed),
            )));
        }
        self.residual
            .as_mut()
            .expect("residual cursor was initialized")
            .pull(&self.constraint, &self.postprocessing, &mut self.projection)
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> fmt::Debug for Query<C, P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("constraint", &std::any::type_name::<C>())
            .field("seed", &self.seed)
            .field("iteration_started", &self.iteration_started)
            .field("residual_started", &self.residual.is_some())
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
// Ordinary `IntoParallelIterator` installs the canonical residual runtime on
// a fresh query and delegates partitioning to its affine splitter.
// A partially consumed query remains one exact-remainder leaf when converted
// through the ordinary `IntoParallelIterator` path.
//
// `fold_with` delegates to the iterator that already owns the exact state. No
// duplicated execution loop.
// ---------------------------------------------------------------------------

#[cfg(feature = "parallel")]
pub use parallel::QueryParIter;

#[cfg(feature = "parallel")]
mod parallel {
    use super::*;
    use rayon::iter::plumbing::{bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer};
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    /// Parallel iterator over the results of a [`Query`], obtained via
    /// ordinary [`IntoParallelIterator::into_par_iter`].
    ///
    /// Fresh ordinary iteration delegates directly to
    /// [`ResidualStateParIter`](residual::ResidualStateParIter), including its
    /// affine splitter and fold loop. The wrapped [`Query`] producer exists
    /// only for already-started exact remainders.
    ///
    /// Rayon clones the constraint tree and postprocessor for each shard.
    /// Clone-local interior state is therefore clone-local by definition;
    /// aggregate observations belong behind shared synchronization such as
    /// `Arc<AtomicU64>` rather than a `Cell` copied with the closure.
    pub struct QueryParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: QueryParInner<C, P, R>,
    }

    enum QueryParInner<C, P: Fn(&Binding) -> Option<R>, R> {
        Residual(residual::ResidualStateParIter<C, P, R>),
        Query(Box<Query<C, P, R>>),
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
            // Move a fresh ordinary query directly into the existing residual
            // iterator and producer. QueryParIter is only a type-level adapter;
            // it adds no residual split or execution path of its own.
            if !self.iteration_started && self.residual.is_none() {
                let residual = self.solve_residual_state_lazy();
                return QueryParIter {
                    inner: QueryParInner::Residual(residual.into_par_iter()),
                };
            }

            QueryParIter {
                // An already-started exact remainder is one leaf.
                inner: QueryParInner::Query(Box::new(self)),
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

        fn split(self) -> (Self, Option<Self>) {
            match self.inner {
                QueryParInner::Residual(residual) => {
                    let (left, right) = residual.split();
                    (
                        QueryParIter {
                            inner: QueryParInner::Residual(left),
                        },
                        right.map(|right| QueryParIter {
                            inner: QueryParInner::Residual(right),
                        }),
                    )
                }
                QueryParInner::Query(inner) => (
                    QueryParIter {
                        inner: QueryParInner::Query(inner),
                    },
                    None,
                ),
            }
        }

        fn fold_with<F: Folder<R>>(self, folder: F) -> F {
            let mut q = match self.inner {
                QueryParInner::Residual(residual) => {
                    return UnindexedProducer::fold_with(residual, folder);
                }
                QueryParInner::Query(inner) => inner,
            };
            let mut folder = folder;
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
            match self.inner {
                QueryParInner::Residual(residual) => residual.drive_unindexed(consumer),
                QueryParInner::Query(inner) => bridge_unindexed(
                    QueryParIter {
                        inner: QueryParInner::Query(inner),
                    },
                    consumer,
                ),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::inline::encodings::iu256::U256BE;

        #[test]
        fn fresh_query_routes_to_residual_parallel_producer() {
            let mut context = VariableContext::new();
            let variable = context.next_variable::<U256BE>();
            let query = Query::new(
                Arc::new(variable.is(U256BE::inline_from(1u64))),
                move |binding: &Binding| binding.get(variable.index).copied(),
            );

            let parallel = query.into_par_iter();
            assert!(matches!(parallel.inner, QueryParInner::Residual(_)));
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
/// Query heads have relational SET semantics. Two satisfying assignments with
/// the same ordered raw inline values for every declared head variable produce
/// one result, even when they differ in hidden variables. Distinctness is
/// decided before [`TryFromInline`](crate::inline::TryFromInline) conversion;
/// two different raw values may therefore still convert to equal Rust values.
/// A raw head is claimed before conversion or mapper code runs, so a conversion
/// failure, filtered row, or panic is not retried through another hidden
/// witness. Every projected variable must be unique; repeating a variable in
/// the head is a compile error because it would not add a projected column.
///
/// The unit form `find!((), constraint)` projects no variables and consequently
/// yields at most one `()`: one if any assignment satisfies the constraint and
/// none otherwise. Claiming that singleton key stops the search without
/// draining additional hidden witnesses, including when mapper code returns
/// `None` or panics. Use an explicitly projected witness when its distinct
/// values need to be counted.
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

    #[test]
    fn fixed_variable_choice_key_uses_magnitude_then_lower_variable_id() {
        // Cardinality magnitude dominates the variable-ID tiebreak, including
        // the special zero-to-one and one-to-two boundaries.
        assert!(variable_choice_key(2, 0) > variable_choice_key(1, 1));
        assert!(variable_choice_key(2, 1) > variable_choice_key(1, 2));

        // Counts within one power-of-two bucket are equally specific and lower
        // VariableId wins deterministically.
        assert!(variable_choice_key(1, 3) > variable_choice_key(2, 2));
        assert!(variable_choice_key(1, 2) > variable_choice_key(2, 3));
    }

    fn action_peer(
        occurrence: usize,
        coverage: ProposalCoverage,
        proposal_rank: u8,
        confirmation_rank: u8,
    ) -> ActionCostPeer {
        ActionCostPeer {
            occurrence,
            coverage,
            classes: Some(ActionUnitClasses::new(
                ProposalUnitClass::from_log2_rank(proposal_rank),
                ConfirmationUnitClass::from_log2_rank(confirmation_rank),
            )),
        }
    }

    #[test]
    fn directed_action_model_requires_every_relevant_occurrence() {
        let mut peers = vec![action_peer(0, ProposalCoverage::Exact, 0, 0)];
        peers.push(ActionCostPeer {
            occurrence: 1,
            coverage: ProposalCoverage::None,
            classes: None,
        });

        assert!(DirectedActionModel::new(&peers).is_none());
    }

    #[test]
    fn directed_action_model_prices_engine_set_admission_explicitly() {
        let source = action_peer(0, ProposalCoverage::Exact, 0, 6);
        let model = DirectedActionModel::new(&[source]).expect("complete classes");

        assert_eq!(model.planning_cost(source, 7), 14);
    }

    #[test]
    fn directed_action_model_prices_proposal_and_confirmation_direction() {
        let expensive_to_confirm = action_peer(0, ProposalCoverage::Exact, 0, 6);
        let cheap_to_confirm = action_peer(1, ProposalCoverage::Exact, 0, 0);
        let model = DirectedActionModel::new(&[expensive_to_confirm, cheap_to_confirm])
            .expect("complete classes");

        assert_eq!(model.planning_cost(expensive_to_confirm, 32), 96);
        assert_eq!(model.planning_cost(cheap_to_confirm, 16), 1_056);
    }

    #[test]
    fn directed_cost_can_choose_a_larger_ordered_source() {
        let hash = ActionCostPeer {
            occurrence: 0,
            coverage: ProposalCoverage::Exact,
            classes: Some(ActionUnitClasses::new(
                ProposalUnitClass::HASH_TABLE_ENUMERATION,
                ConfirmationUnitClass::HASH_TABLE_MEMBERSHIP,
            )),
        };
        let archive = ActionCostPeer {
            occurrence: 1,
            coverage: ProposalCoverage::Exact,
            classes: Some(ActionUnitClasses::new(
                ProposalUnitClass::SUCCINCT_ORDERED_ENUMERATION,
                ConfirmationUnitClass::SUCCINCT_RANDOM_MEMBERSHIP,
            )),
        };
        let model = DirectedActionModel::new(&[hash, archive]).expect("complete classes");
        let hash_cost = model.planning_cost(hash, 8);

        for archive_count in [9, 16, 21, 29] {
            assert!(
                model.planning_cost(archive, archive_count) < hash_cost,
                "ordered source width {archive_count} should avoid random succinct confirmation"
            );
        }
    }

    #[test]
    fn directed_action_model_covering_source_confirms_itself() {
        let covering = action_peer(0, ProposalCoverage::Covering, 0, 6);
        let peer = action_peer(1, ProposalCoverage::Exact, 0, 0);
        let model = DirectedActionModel::new(&[covering, peer]).expect("complete classes");

        assert_eq!(model.planning_cost(covering, 32), 2_144);
    }

    #[test]
    fn directed_action_model_counts_repeated_occurrences() {
        let source = action_peer(0, ProposalCoverage::Exact, 0, 0);
        let validator = action_peer(1, ProposalCoverage::None, 0, 2);
        let repeated_validator = action_peer(2, ProposalCoverage::None, 0, 2);
        let once = DirectedActionModel::new(&[source, validator]).expect("complete classes");
        let twice = DirectedActionModel::new(&[source, validator, repeated_validator])
            .expect("complete classes");

        assert_eq!(once.planning_cost(source, 3), 18);
        assert_eq!(twice.planning_cost(source, 3), 30);
    }

    #[test]
    fn directed_action_model_is_monotone_and_preserves_unknown_sentinel() {
        let source = action_peer(
            0,
            ProposalCoverage::Exact,
            ProposalUnitClass::MAX_LOG2_RANK,
            0,
        );
        let model = DirectedActionModel::new(&[source]).expect("complete classes");

        assert_eq!(model.planning_cost(source, 0), 0);
        let one = ((1u128 << ProposalUnitClass::MAX_LOG2_RANK) + 1).min((usize::MAX - 1) as u128)
            as usize;
        assert_eq!(model.planning_cost(source, 1), one);
        assert_eq!(model.planning_cost(source, 2), usize::MAX - 1);
        assert_eq!(model.planning_cost(source, usize::MAX), usize::MAX);
    }

    #[test]
    #[should_panic(expected = "unit-work rank exceeds 63")]
    fn proposal_unit_class_rejects_unlawful_rank() {
        ProposalUnitClass::from_log2_rank(ProposalUnitClass::MAX_LOG2_RANK + 1);
    }

    #[test]
    #[should_panic(expected = "unit-work rank exceeds 63")]
    fn confirmation_unit_class_rejects_unlawful_rank() {
        ConfirmationUnitClass::from_log2_rank(ConfirmationUnitClass::MAX_LOG2_RANK + 1);
    }

    #[test]
    fn projection_gate_elides_exact_full_head_masks() {
        let variables = variable_set([0, 3, 7]);
        let mut full = ProjectionGate::new([0, 3, 7], variables);
        let reordered = ProjectionGate::new([7, 0, 3], variables);

        assert!(matches!(&full.claims, ProjectionClaims::Elided));
        assert!(matches!(&reordered.claims, ProjectionClaims::Elided));
        assert!(matches!(
            &reordered.clone().claims,
            ProjectionClaims::Elided
        ));

        let mut binding = Binding::default();
        binding.set(0, &[1; 32]);
        binding.set(3, &[2; 32]);
        binding.set(7, &[3; 32]);
        assert!(full.claim(&binding));
        assert!(
            full.claim(&binding),
            "an elided full head must not allocate or consult a terminal key table"
        );

        #[cfg(feature = "parallel")]
        {
            let transfer = full.share_for_parallel();
            assert!(transfer.is_none());
            assert!(matches!(&full.claims, ProjectionClaims::Elided));
            let mut sibling = full.clone();
            sibling.attach_shared(transfer);
            assert!(matches!(&sibling.claims, ProjectionClaims::Elided));
        }
    }

    #[test]
    fn projection_gate_keeps_strict_claims_and_snapshots_clones() {
        let variables = variable_set([0, 1]);
        let mut strict = ProjectionGate::new([0], variables);
        assert!(matches!(&strict.claims, ProjectionClaims::Owned(claims) if claims.is_empty()));

        let mut first = Binding::default();
        first.set(0, &[1; 32]);
        first.set(1, &[10; 32]);
        assert!(strict.claim(&first));
        assert!(!strict.claim(&first));

        let mut snapshot = strict.clone();
        let mut second = Binding::default();
        second.set(0, &[2; 32]);
        second.set(1, &[20; 32]);
        assert!(strict.claim(&second));
        assert!(
            snapshot.claim(&second),
            "an ordinary clone must own an independent strict-projection snapshot"
        );
        assert!(matches!(
            &snapshot.claims,
            ProjectionClaims::Owned(claims) if claims.len() == 2
        ));
    }

    #[test]
    fn projection_gate_distinguishes_full_and_strict_zero_heads() {
        let mut full_zero = ProjectionGate::new([], VariableSet::new_empty());
        assert!(matches!(&full_zero.claims, ProjectionClaims::Elided));
        assert!(full_zero.claim(&Binding::default()));
        assert!(full_zero.claim(&Binding::default()));
        assert!(!full_zero.is_done());

        let mut strict_zero = ProjectionGate::new([], variable_set([0]));
        assert!(matches!(
            &strict_zero.claims,
            ProjectionClaims::Owned(claims) if claims.is_empty()
        ));
        assert!(strict_zero.claim(&Binding::default()));
        assert!(strict_zero.is_done());
        assert!(!strict_zero.claim(&Binding::default()));
        assert!(strict_zero.clone().is_done());
    }

    #[test]
    fn projection_gate_raw_claim_survives_mapper_rejection() {
        let mut gate = ProjectionGate::new([0], variable_set([0, 1]));
        let mut first = Binding::default();
        first.set(0, &[7; 32]);
        first.set(1, &[1; 32]);
        assert!(gate.claim(&first));
        assert!(matches!(
            gate.project_claimed(&first, &|_| None::<()>),
            ProjectionStep::Skip
        ));

        let mut same_head = first;
        same_head.set(1, &[2; 32]);
        assert!(!gate.claim(&same_head));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn projection_gate_snapshots_shared_strict_claims_without_eliding_them() {
        let variables = variable_set([0, 1]);
        let mut strict = ProjectionGate::new([0], variables);
        let mut first = Binding::default();
        first.set(0, &[1; 32]);
        first.set(1, &[10; 32]);
        assert!(strict.claim(&first));

        let transfer = strict
            .share_for_parallel()
            .expect("a strict projection has a shared claim domain");
        assert!(matches!(&strict.claims, ProjectionClaims::Shared(_)));
        let mut snapshot = strict.clone();
        assert!(matches!(&snapshot.claims, ProjectionClaims::Owned(_)));

        let mut second = Binding::default();
        second.set(0, &[2; 32]);
        second.set(1, &[20; 32]);
        assert!(strict.claim(&second));
        assert!(snapshot.claim(&second));

        let mut sibling = snapshot.clone();
        sibling.attach_shared(Some(transfer));
        assert!(matches!(&sibling.claims, ProjectionClaims::Shared(_)));
        assert!(!sibling.claim(&second));
    }

    #[cfg(feature = "parallel")]
    #[test]
    #[should_panic(expected = "parallel projection transfer cannot elide")]
    fn projection_gate_rejects_a_missing_strict_parallel_claim_transfer() {
        let mut strict = ProjectionGate::new([0], variable_set([0, 1]));
        strict.attach_shared(None);
    }

    #[test]
    fn rows_view_preserves_explicit_zero_width_row_multiplicity() {
        assert_eq!(RowsView::EMPTY.len(), 1);
        assert_eq!(RowsView::new(&[], &[]).len(), 1);

        let three = RowsView::new_with_row_count(&[], &[], 3);
        assert_eq!(three.len(), 3);
        assert!(!three.is_empty());
        let empty: &[RawInline] = &[];
        assert_eq!(three.iter().collect::<Vec<_>>(), vec![empty; 3]);
        assert_eq!(three.row(2), empty);
        assert_eq!(three.row_view(2).len(), 1);

        let zero = RowsView::new_with_row_count(&[], &[], 0);
        assert!(zero.is_empty());
        assert_eq!(zero.iter().count(), 0);
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

    /// A lawful row-homomorphic constraint whose occurrence bag depends on
    /// which adaptive variable is proposed first. The support relation is the
    /// same along every path; only duplicate proposal occurrences differ.
    #[derive(Clone, Copy)]
    struct VariableOrderBagConstraint {
        tie_children: bool,
    }

    impl VariableOrderBagConstraint {
        const PARENT: VariableId = 0;
        const LEFT: VariableId = 1;
        const RIGHT: VariableId = 2;
        const P0: RawInline = [0; 32];
        const P1: RawInline = [1; 32];
        const LEFT_VALUE: RawInline = [2; 32];
        const RIGHT_VALUE: RawInline = [3; 32];

        fn allowed(variable: VariableId, value: &RawInline) -> bool {
            match variable {
                Self::PARENT => *value == Self::P0 || *value == Self::P1,
                Self::LEFT => *value == Self::LEFT_VALUE,
                Self::RIGHT => *value == Self::RIGHT_VALUE,
                _ => false,
            }
        }

        fn row_valid_so_far(view: &RowsView<'_>, row: &[RawInline]) -> bool {
            [Self::PARENT, Self::LEFT, Self::RIGHT]
                .into_iter()
                .all(|variable| {
                    view.col(variable)
                        .is_none_or(|column| Self::allowed(variable, &row[column]))
                })
        }
    }

    impl Constraint<'static> for VariableOrderBagConstraint {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(Self::PARENT)
                .union(VariableSet::new_singleton(Self::LEFT))
                .union(VariableSet::new_singleton(Self::RIGHT))
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if self.variables().is_set(variable) && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            match variable {
                Self::PARENT => out.fill(2, view.len()),
                Self::LEFT | Self::RIGHT => {
                    let Some(parent) = view.col(Self::PARENT) else {
                        out.fill(8, view.len());
                        return true;
                    };
                    let other = if variable == Self::LEFT {
                        Self::RIGHT
                    } else {
                        Self::LEFT
                    };
                    if view.col(other).is_some() {
                        out.fill(1, view.len());
                    } else if self.tie_children {
                        out.fill(1, view.len());
                    } else {
                        out.extend(view.iter().map(|row| {
                            let even_parent = row[parent][0] & 1 == 0;
                            usize::from(
                                (variable == Self::RIGHT && even_parent)
                                    || (variable == Self::LEFT && !even_parent),
                            ) + 1
                        }));
                    }
                }
                _ => return false,
            }
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable == Self::PARENT {
                for (row_index, row) in view.iter().enumerate() {
                    if Self::row_valid_so_far(view, row) {
                        candidates.extend_row(row_index as u32, [Self::P0, Self::P1]);
                    }
                }
                return;
            }

            let value = match variable {
                Self::LEFT => Self::LEFT_VALUE,
                Self::RIGHT => Self::RIGHT_VALUE,
                _ => return,
            };
            let Some(parent) = view.col(Self::PARENT) else {
                for (row_index, row) in view.iter().enumerate() {
                    if Self::row_valid_so_far(view, row) {
                        candidates.push(row_index as u32, value);
                    }
                }
                return;
            };
            let other = if variable == Self::LEFT {
                Self::RIGHT
            } else {
                Self::LEFT
            };
            let other_is_bound = view.col(other).is_some();
            for (row_index, row) in view.iter().enumerate() {
                if !Self::row_valid_so_far(view, row) {
                    continue;
                }
                let even_parent = row[parent][0] & 1 == 0;
                let duplicates = !other_is_bound
                    && ((variable == Self::RIGHT && even_parent)
                        || (variable == Self::LEFT && !even_parent));
                candidates.extend_row(
                    row_index as u32,
                    std::iter::repeat_n(value, usize::from(duplicates) + 1),
                );
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            candidates.retain(|row, value| {
                Self::allowed(variable, value)
                    && Self::row_valid_so_far(view, view.row(row as usize))
            });
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.iter().all(|row| Self::row_valid_so_far(view, row))
        }
    }

    fn variable_order_bag_query(
        tie_children: bool,
    ) -> Query<
        VariableOrderBagConstraint,
        impl Fn(&Binding) -> Option<(RawInline, RawInline, RawInline)>,
        (RawInline, RawInline, RawInline),
    > {
        Query::new(
            VariableOrderBagConstraint { tie_children },
            |binding: &Binding| {
                Some((
                    *binding.get(VariableOrderBagConstraint::PARENT)?,
                    *binding.get(VariableOrderBagConstraint::LEFT)?,
                    *binding.get(VariableOrderBagConstraint::RIGHT)?,
                ))
            },
        )
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

    impl Constraint<'static> for SetAdmissionProbe {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(Self::ROOT).union(VariableSet::new_singleton(Self::LEAF))
        }

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if self.variables().is_set(variable) && !bound.is_set(variable) {
                ProposalCoverage::Exact
            } else {
                ProposalCoverage::None
            }
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            match variable {
                Self::ROOT => out.fill(1, view.len()),
                Self::LEAF => out.fill(2, view.len()),
                _ => return false,
            }
            true
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            match variable {
                Self::ROOT => {
                    for row in 0..view.len() {
                        candidates.extend_row(row as u32, [Self::A, Self::B, Self::A]);
                    }
                }
                Self::LEAF => {
                    let root = view.col(Self::ROOT).expect("root is bound first");
                    let mut descendants = self.descendants.lock().unwrap();
                    for (row_index, row) in view.iter().enumerate() {
                        descendants.push(row[root]);
                        candidates.push(row_index as u32, Self::LEAF_VALUE);
                    }
                }
                _ => {}
            }
        }

        fn confirm(
            &self,
            variable: VariableId,
            _view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            candidates.retain(|_, value| match variable {
                Self::ROOT => *value == Self::A || *value == Self::B,
                Self::LEAF => *value == Self::LEAF_VALUE,
                _ => false,
            });
        }

        fn satisfied(&self, view: &RowsView<'_>) -> bool {
            view.iter().all(|row| {
                view.col(Self::ROOT)
                    .is_none_or(|column| row[column] == Self::A || row[column] == Self::B)
                    && view
                        .col(Self::LEAF)
                        .is_none_or(|column| row[column] == Self::LEAF_VALUE)
            })
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn ordinary_parallel_residual_admits_set_before_splitting() {
        use rayon::prelude::*;

        let descendants = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut rows: Vec<_> = Query::new(
            SetAdmissionProbe {
                descendants: descendants.clone(),
            },
            |binding: &Binding| {
                Some((
                    *binding.get(SetAdmissionProbe::ROOT)?,
                    *binding.get(SetAdmissionProbe::LEAF)?,
                ))
            },
        )
        .into_par_iter()
        .collect();
        rows.sort_unstable();

        let mut observed = descendants.lock().unwrap().clone();
        observed.sort_unstable();
        assert_eq!(
            rows,
            [
                (SetAdmissionProbe::A, SetAdmissionProbe::LEAF_VALUE,),
                (SetAdmissionProbe::B, SetAdmissionProbe::LEAF_VALUE,),
            ]
        );
        assert_eq!(
            observed,
            [SetAdmissionProbe::A, SetAdmissionProbe::B],
            "residual shards must inherit SET-admitted proposal rows"
        );
    }
    #[test]
    fn residual_equal_key_ties_preserve_semantic_variable_actions() {
        let constraint = VariableOrderBagConstraint {
            tie_children: false,
        };
        let invalid: RawInline = [9; 32];
        let invalid_vars = [VariableOrderBagConstraint::PARENT];
        let invalid_rows = [invalid];
        let invalid_view = RowsView::new(&invalid_vars, &invalid_rows);
        assert!(!constraint.satisfied(&invalid_view));
        let mut proposed = Vec::new();
        constraint.propose(
            VariableOrderBagConstraint::LEFT,
            &invalid_view,
            &mut CandidateSink::Tagged(&mut proposed),
        );
        assert!(proposed.is_empty());
        let mut confirmed = vec![(0, VariableOrderBagConstraint::RIGHT_VALUE)];
        constraint.confirm(
            VariableOrderBagConstraint::RIGHT,
            &invalid_view,
            &mut CandidateSink::Tagged(&mut confirmed),
        );
        assert!(confirmed.is_empty());

        let row = |parent| {
            (
                parent,
                VariableOrderBagConstraint::LEFT_VALUE,
                VariableOrderBagConstraint::RIGHT_VALUE,
            )
        };
        for (tie_children, mut expected) in [
            (
                false,
                vec![
                    row(VariableOrderBagConstraint::P0),
                    row(VariableOrderBagConstraint::P1),
                ],
            ),
            (
                true,
                vec![
                    row(VariableOrderBagConstraint::P0),
                    row(VariableOrderBagConstraint::P1),
                ],
            ),
        ] {
            expected.sort_unstable();

            let mut residual: Vec<_> = variable_order_bag_query(tie_children)
                .solve_residual_state_lazy()
                .collect();
            residual.sort_unstable();
            assert_eq!(residual, expected);
        }
    }
}
