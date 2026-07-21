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
pub mod finiteunaryprogram;
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
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg(feature = "parallel")]
use std::sync::Mutex;

use ahash::AHashSet;
use arrayvec::ArrayVec;
use constantconstraint::*;

use crate::inline::encodings::genid::GenId;
use crate::inline::Inline;
use crate::inline::InlineEncoding;
use crate::inline::RawInline;

#[cfg(test)]
pub use program::ProgramCompleteEffects;
#[doc(hidden)]
pub use program::{
    DispatchClass, PreferredProgram, ProgramAction, ProgramActivation, ProgramBatch,
    ProgramBatchEffects, ProgramChild, ProgramCompleteBatch, ProgramCompleteWorkEvidence,
    ProgramCompleteWorkQuote, ProgramCompletion, ProgramExposure, ProgramGrouping, ProgramKey,
    ProgramPacing, ProgramPage, ProgramPhysicalReceipt, ProgramRef, ProgramRequest, ProgramResume,
    ProgramRoute, ProgramRuntime, ProgramSeedBatch, ProgramSeedEffects, ProgramSeedWork,
    ProgramStratum, ProgramWork, ProgramWorkHandle, TypedCompleteArbiter, TypedCompleteSink,
    TypedEffectSink, TypedPhysicalStep, TypedProgramBatch, TypedProgramSpec,
    TypedProgramStateBatch, TypedResume, TypedSeedSink,
};
pub(crate) use program::{ProgramCompleteAdmission, ProgramCompleteAffinity};
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

/// Result of offering one complete raw binding to the public projection.
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

    /// Claims the raw head, then invokes the mapper with exactly the binding
    /// scope represented by that head. Hidden witnesses cannot affect a
    /// macro-created result through the doc-hidden constructor seam.
    fn project<P, R>(&mut self, binding: &Binding, postprocessing: &P) -> ProjectionStep<R>
    where
        P: Fn(&Binding) -> Option<R>,
    {
        if !self.claim(binding) {
            return if self.is_empty_head() {
                ProjectionStep::Done
            } else {
                ProjectionStep::Skip
            };
        }
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
/// is caller-chosen — the sequential engine uses binding order, the DAG
/// solver canonical ascending order — so constraints locate their columns
/// with [`col`](Self::col) and never assume a layout.
///
/// A view constructed publicly with **no columns is the seed block: a single
/// zero-width row** (the empty binding). Blocked engines may internally carry
/// several occurrences of that empty binding after splitting and remerging;
/// their explicit row count preserves that multiplicity even though `rows`
/// itself is necessarily empty. This is what makes level 0 an ordinary block
/// instead of a special case in every engine.
///
/// The view is `Copy` and borrows the engine's row storage directly. A
/// single-row view ([`row_view`](Self::row_view)) is a subslice of the
/// parent block, not a copy — the borrowed cursor that lets per-row
/// fallbacks (and the sequential engine, which is literally a block-of-1
/// caller) run without any scratch [`Binding`].
#[derive(Clone, Copy, Debug)]
pub struct RowsView<'v> {
    /// The bound variables — the column layout of `rows`.
    pub vars: &'v [VariableId],
    /// Row-major value store: `len() * stride()` entries.
    pub rows: &'v [RawInline],
    /// Optional O(1) variable→column index: `cols[v]` is the column of
    /// variable `v`, [`COL_UNBOUND`] when unbound. The sequential engine
    /// maintains one incrementally (its cursor changes one variable at a
    /// time); the blocked engines pass `None` — they amortize the
    /// [`col`](Self::col) scan over whole blocks, while the block-of-1
    /// caller pays it per verb call without the index.
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
    /// [`col`](Self::col) O(1). The single-row cursor engine uses this.
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

/// The ragged candidate matrix of the blocked engines: `(row, value)`
/// pairs in COO form, **grouped by ascending row index**. The blocked /
/// grouped / DAG solvers own buffers of this type and lend them to the
/// protocol through [`CandidateSink::Tagged`].
pub type Candidates = Vec<(u32, RawInline)>;

/// Collapses an action's occurrence bag to its SET support while preserving
/// the order in which a tail-popping consumer would first encounter values.
///
/// The returned length is the raw occurrence count. Callers charge proposal
/// work from that count before using the normalized action frontier. `seen` is
/// cleared on return but retains its allocation for the next action.
fn admit_reverse_stable_set<T>(occurrences: &mut Vec<T>, seen: &mut AHashSet<T>) -> usize
where
    T: Copy + Eq + std::hash::Hash,
{
    let raw_len = occurrences.len();
    seen.clear();
    if raw_len > 1 {
        occurrences.reverse();
        occurrences.retain(|occurrence| seen.insert(*occurrence));
        occurrences.reverse();
    }
    seen.clear();
    raw_len
}

/// The output sink of [`Constraint::propose`] / [`Constraint::confirm`] —
/// the representation-generic seam that lets one protocol serve both
/// engine families with zero ceremony on either side:
///
/// - [`Tagged`](Self::Tagged) lends a [`Candidates`] pair buffer — the
///   blocked engines' ragged COO frontier, `(row, value)` grouped by
///   ascending row index.
/// - [`Values`](Self::Values) lends a plain `Vec<RawInline>` for any
///   single-parent frontier. The sequential cursor and one-parent blocked /
///   residual frontiers use this compact representation. The row index is
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
    /// `(row, value)` pairs, grouped by ascending row — blocked engines.
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
/// - [`Column`](Self::Column) appends per-row estimates to a column
///   vector — the blocked engines' shape.
/// - [`Scalar`](Self::Scalar) writes a single-row view's estimate
///   straight into a stack slot — the sequential engine's shape, with no
///   `Vec` round-trip.
pub enum EstimateSink<'s> {
    /// One estimate per row, appended — blocked engines.
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
/// value group in place — the derived (scalar) case of blocked confirm.
/// `f` receives the row's values and the row's candidate values.
///
/// For a [`CandidateSink::Values`] sink (the sequential engine's
/// block-of-1) this is a direct call on the borrowed buffer — no
/// grouping, no scratch, no copies.
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

/// One engine-owned node in a residual transition program.
///
/// `value` is the current data-plane term, `source` is an optional fixed
/// acceptance anchor carried through the traversal, and `continuation` is a
/// constraint-defined program point. A same-variable traversal anchors its
/// speculative root; a fully-bound support traversal may instead anchor its
/// required target. Novelty is over the complete node: the same current term
/// reached with different anchors or under different residual programs may
/// have different future computation.
///
/// None of these fields participates in the scheduler's canonical structural
/// state identifier. Nodes are activation-private payload batched under the
/// structural transition operator selected by that identifier.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ResidualDeltaNode {
    pub source: Option<RawInline>,
    pub value: RawInline,
    pub continuation: u32,
}

/// One transition work item plus its endpoint effect.
///
/// `accepted` is not part of work identity. A well-formed constraint must
/// report it consistently for every occurrence of the same node.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidualDeltaOutput {
    pub node: ResidualDeltaNode,
    pub accepted: bool,
}

/// One affine producer root seeded from a parent row.
///
/// Several seeds may name the same parent. They become distinct affine root
/// credits inside one parent-scoped activation, sharing that activation's
/// novelty and reducer. `parent` selects the immutable outer row copied into
/// the activation. Seeds must be grouped by ascending parent tag.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidualDeltaSeed {
    pub parent: u32,
    pub output: ResidualDeltaOutput,
}

/// Borrow-free cursor for a constraint-owned residual source frontier.
///
/// The cursor is activation payload, never part of the canonical residual or
/// delta state identifier. A source must choose one cursor family and retain it
/// for the activation. `After(value)` resumes strictly after `value` in
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

/// Result metadata for one bounded residual source page.
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

/// One physically compatible cohort of affine residual source activations.
///
/// The rows share one bound-variable schema. `candidate_sets`, `cursors`, and
/// `limits` are row-aligned; candidate sets are either present for every row
/// or absent for every row, and every cursor belongs to the same family. The
/// per-row limits are positive and their sum is bounded by the scheduler's
/// current global geometric width. None of these physical dispatch details is
/// part of canonical residual or delta state identity.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ResidualDeltaSourceBatch<'v> {
    pub view: RowsView<'v>,
    pub candidate_sets: &'v [Option<&'v [RawInline]>],
    pub cursors: &'v [ResidualDeltaSourceCursor],
    pub limits: &'v [usize],
}

/// Borrow-free cursor for one node's ordered transition frontier.
///
/// `branch` identifies one constraint-defined outgoing transition from the
/// node's current program point. `After` resumes strictly after `value` within
/// that branch. Branches are visited in increasing order, so the pair
/// `(branch, value)` advances monotonically even when two branches produce the
/// same value or lead to different program points.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ResidualDeltaExpandCursor {
    Start,
    After { branch: u32, value: RawInline },
}

/// Result metadata for one bounded transition-expansion page.
///
/// `examined` counts constraint-owned transition candidates consumed from the
/// node frontier and must not exceed the requested limit. `next == None`
/// proves that this node has no remaining outgoing transition work.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResidualDeltaExpandPage {
    pub next: Option<ResidualDeltaExpandCursor>,
    pub examined: usize,
}

/// One physical cohort of affine transition-node pages.
///
/// Every node belongs to the same structural transition operator. `nodes`,
/// `cursors`, and `limits` are row-aligned, every limit is positive, and their
/// sum is bounded by the scheduler's current global geometric width. Nodes and
/// cursors remain activation payload; the batch is a dispatch shape, never a
/// canonical state identity.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ResidualDeltaExpandBatch<'v> {
    pub nodes: &'v [ResidualDeltaNode],
    pub cursors: &'v [ResidualDeltaExpandCursor],
    pub limits: &'v [usize],
}

/// Structural proof carried by a constraint's proposal for one variable.
///
/// For a constraint occurrence `C`, target variable `x`, bound-variable
/// schema `B`, and one row `b`, let `F_C(x | b)` be the set of values that
/// extend `b` to at least one complete solution of `C`. Coverage compares
/// that existential fiber with the **support** of
/// `C.propose_certified(x, b)` under a completely certified root; proposal
/// occurrence multiplicity is deliberately not part of the receipt. Legacy
/// action-defined execution ignores this receipt and continues to call
/// `C.propose(x, b)`.
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

/// Every certified proposal occurrence crosses the engine's SET-admission
/// boundary before descendants observe it. Keep that engine work explicit
/// rather than hiding it inside a backend proposal class.
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
    /// for an unknown legacy estimate. A missing source count conservatively
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
/// special case (the sequential engine and one-parent blocked/residual
/// frontiers pass single-row views with a plain-value
/// [`CandidateSink::Values`] sink, paying no row tags); multi-parent frontiers
/// use a [`CandidateSink::Tagged`] pair sink. Constraints with batchable probe
/// streams may evaluate either representation in one pass — cache-friendly on
/// the CPU and suitable for accelerator backends.
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
/// [`influence`](Constraint::influence) completes the picture by telling
/// the engine which estimates to refresh when a variable is bound or
/// unbound.
///
/// [`fixed_denotation`](Constraint::fixed_denotation) and
/// [`proposal_coverage`](Constraint::proposal_coverage) are semantic receipts.
/// An engine activates them only when the complete root occurrence is
/// certified: coverage then owns source eligibility, while a Covering source
/// is confirmed before its candidates cross a relational boundary. A root
/// containing any default-false occurrence retains the legacy action-defined
/// protocol.
///
/// # Statelessness
///
/// Constraints are stateless: every method receives the current block as
/// a borrowed view rather than maintaining internal bookkeeping. This
/// lets the engines backtrack (sequential), chunk (blocked), or reorder
/// work (DAG worklist) freely without notifying the constraints.
///
/// # Structural relevance
///
/// Whether a constraint has an opinion about a variable is **structural**: it
/// depends only on the variable's identity and bound-variable schema, never on
/// bound *values*. For a certified root, [`variables`](Constraint::variables)
/// defines validator relevance and
/// [`proposal_coverage`](Constraint::proposal_coverage) defines source
/// eligibility. [`estimate`](Constraint::estimate) is then only an optional
/// cost quote; returning `false` assigns unknown cost rather than erasing the
/// occurrence. In legacy action-defined mode, `estimate == false` retains its
/// historical second role as the relevance/source signal for the whole block.
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
/// For an uncertified root, once planning selects a variable and proposing
/// constraint occurrence for a row, that pair remains the row's semantic
/// action. [`propose`](Constraint::propose) owns the candidate support and its
/// first-seen order; estimates only order the choice. Physical duplicate
/// occurrences collapse at the engine's per-parent SET-admission boundary
/// before descendants are filed. Row homomorphism permits batching the
/// **same** action across rows, but does not make two proposer occurrences
/// interchangeable: they may still expose different supports.
///
/// A certified root instead supplies one fixed relation. Receipt-aware engines
/// may regroup rows among sound sources and collapse occurrence multiplicity at
/// explicit SET boundaries, provided they preserve the same raw projected
/// tuples. Estimates remain costs in both modes and never authorize a semantic
/// rewrite by themselves.
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
/// match). Override [`influence`](Constraint::influence) when binding one
/// variable changes the estimates for a non-obvious set of others. To opt a
/// complete query tree into relational SET planning, every occurrence must
/// also uphold [`fixed_denotation`](Constraint::fixed_denotation), and every
/// occurrence used as a source must publish an appropriate
/// [`proposal_coverage`](Constraint::proposal_coverage) receipt.
pub trait Constraint<'a> {
    /// Returns the set of variables this constraint touches.
    ///
    /// Called once at query start. The engine uses this to build influence
    /// graphs and to determine which constraints participate when a
    /// particular variable is being bound.
    fn variables(&self) -> VariableSet;

    /// Certifies that this occurrence denotes one fixed set relation.
    ///
    /// Returning `true` is a proof obligation over the occurrence's
    /// relational-SET protocol and every accelerated route it exposes. That
    /// protocol is activated only when **every occurrence in the complete
    /// query root** returns `true`. For the duration of such a solve:
    ///
    /// - `propose_certified`, `confirm_certified`, paged, typed-Program, and
    ///   complete-equivalent routes must agree on the same relation over
    ///   [`Self::variables`];
    /// - `confirm_certified(x, b, input)` must be a subbag of `input`, retain
    ///   every occurrence whose value belongs to the existential fiber, and
    ///   become exact once all occurrence variables other than `x` are bound;
    /// - `satisfied(b) == false` must prove that `b` has no completion, and
    ///   `satisfied` must be exact once all occurrence variables are bound;
    /// - estimates remain costs only and cannot change relevance, coverage,
    ///   or the denoted relation.
    ///
    /// The certified action methods default to the ordinary action methods,
    /// which is the right implementation for leaves. Logical composites may
    /// override them while retaining their historical ordinary behavior below
    /// an uncertified root. A transparent wrapper that forwards
    /// `fixed_denotation` or `proposal_coverage` MUST also forward all three
    /// certified action methods; otherwise it has made a false certificate.
    /// The default `false` preserves action-defined scheduling for custom
    /// constraints until they explicitly certify these laws.
    fn fixed_denotation(&self) -> bool {
        false
    }

    /// Returns the proposal proof for `variable` under bound schema `bound`.
    ///
    /// A non-[`None`](ProposalCoverage::None) result is legal only when
    /// [`Self::fixed_denotation`] is `true` and `variable` belongs to
    /// [`Self::variables`]. The result is structural for this occurrence,
    /// target, and bound-variable set: it may depend on `bound`, but never on
    /// row values, estimates, route availability, page size, execution
    /// placement, or scheduler width.
    ///
    /// The default makes no source claim. A confirmation-only constraint can
    /// therefore certify a fixed denotation while retaining this default.
    fn proposal_coverage(&self, _variable: VariableId, _bound: VariableSet) -> ProposalCoverage {
        ProposalCoverage::None
    }

    /// Optionally publishes static directed unit costs for one target action.
    ///
    /// This capability is consulted only under a completely
    /// [`fixed_denotation`](Constraint::fixed_denotation) root. Returning
    /// `Some` promises that, whenever
    /// [`proposal_coverage`](Constraint::proposal_coverage) is at least
    /// [`Covering`](ProposalCoverage::Covering), each value emitted by
    /// [`estimate_certified`](Constraint::estimate_certified) for the same
    /// target and bound schema is the number of physical candidate
    /// **occurrences** that [`propose_certified`](Constraint::propose_certified)
    /// would produce for that row before intersection confirmation. It is not
    /// the number of distinct values unless the proposal itself is distinct.
    ///
    /// The classes must describe broad, immutable backend capabilities. They
    /// may depend on `variable` and `bound`, but never on row values, sampled
    /// timings, frontier width, or scheduler state. They affect planning only:
    /// they neither strengthen proposal coverage nor enter canonical state,
    /// route, or action identity. The flattened residual Ready planner uses
    /// directed pricing only when every relevant occurrence opts in;
    /// otherwise the complete action falls back atomically to legacy estimate
    /// ordering. Directed prices select a source only within one variable;
    /// cross-variable ordering continues to compare raw source counts. The
    /// count promise above means an opted-in source cannot lawfully return an
    /// unknown quote; the planner nevertheless preserves `usize::MAX` if it
    /// does.
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
    /// Returns `false` leaving `out` untouched. In legacy action-defined mode
    /// that means this occurrence does not constrain `variable`, uniformly
    /// across the block. Under a completely certified root it means only that
    /// no cost quote is available: structural relevance comes from
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
    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    );

    /// Estimates through the relational-SET execution protocol.
    ///
    /// Engines may call this only after proving that the complete query root
    /// has [`fixed_denotation`](Constraint::fixed_denotation). The default is
    /// deliberately identical to [`estimate`](Constraint::estimate): leaves
    /// need no second implementation. Logical composites override this entry
    /// point so receipt-aware planning propagates through nested structure
    /// without changing the ordinary, action-defined protocol when that same
    /// composite appears below an uncertified root.
    #[doc(hidden)]
    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.estimate(variable, view, out)
    }

    /// Proposes through the relational-SET execution protocol.
    ///
    /// This has the same whole-root activation rule as
    /// [`estimate_certified`](Constraint::estimate_certified). The default
    /// delegates to the ordinary leaf action.
    #[doc(hidden)]
    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.propose(variable, view, candidates)
    }

    /// Confirms through the relational-SET execution protocol.
    ///
    /// This has the same whole-root activation rule as
    /// [`estimate_certified`](Constraint::estimate_certified). The default
    /// delegates to the ordinary leaf action.
    #[doc(hidden)]
    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.confirm(variable, view, candidates)
    }

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
    /// [`residual::FormulaScope::OpaqueLeaves`] deliberately ignores this
    /// capability, so a union retains its existing indivisible [`Constraint`]
    /// semantics. `UnionLeaves` and `WholeRoot` expose it to canonical formula
    /// control. The child count and order are structural facts and must remain
    /// stable for the solve.
    #[doc(hidden)]
    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        None
    }

    /// Reports whether residual execution may partition one parent's ordered
    /// candidate sequence into disjoint pages before calling `confirm`.
    ///
    /// This is an opt-in execution capability, not an additional obligation
    /// of the ordinary constraint protocol. Returning `true` promises that,
    /// for fixed row bindings, confirming consecutive candidate pages and
    /// concatenating their survivors preserves exactly the values, order, and
    /// multiplicity of one confirmation over the complete parent group.
    /// Pointwise `CandidateSink::retain` filters have this property. A
    /// group-global operation such as sorting, deduplication, top-k, or
    /// selecting one representative does not.
    ///
    /// An opted-in confirmer may receive several tagged parent rows whose
    /// `RowsView` has zero columns. They are distinct affine occurrences even
    /// though every row slice is empty: candidate tags still identify the
    /// parent group, and reconvergence must preserve their multiplicity. In
    /// particular, page-local implementations must not infer
    /// `view.len() == 1` from `view.vars.is_empty()`.
    ///
    /// The conservative default keeps the complete parent group atomic.
    /// Residual execution consults this only after any unchecked atomic
    /// confirmer has run, so an atomic prefix may safely feed a page-local
    /// suffix. The answer is structural and must remain stable for the solve.
    #[doc(hidden)]
    fn residual_confirm_is_page_local(&self) -> bool {
        false
    }

    /// Bound-variable prerequisites for a grouped transition confirmation.
    ///
    /// `Some(required)` means that confirming `variable` through a supported
    /// residual transition program needs the complete ordered candidate group
    /// exactly when every variable in `required` is already bound. `None`
    /// means that this confirmation never needs a grouped reducer. When the
    /// prerequisites are not met, the constraint must either decline residual
    /// transition seeds or provide a page-local transition confirmation.
    ///
    /// This is separate from `residual_confirm_is_page_local`: the ordinary
    /// confirmation may be elementwise while the lowered implementation
    /// intentionally traverses once and filters the immutable original group.
    /// The conservative default declines grouped transition lowering. The
    /// answer is structural and must remain stable for the solve.
    #[doc(hidden)]
    fn residual_delta_confirm_grouping_requirements(
        &self,
        _variable: VariableId,
    ) -> Option<VariableSet> {
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
    /// [`Self::residual_program`] accepts the exact `Propose(variable)` request
    /// and the selected [`residual::ProgramScope`] admits that route. It must
    /// be structural in `bound` and identical across typed CPU and physical
    /// execution.
    #[doc(hidden)]
    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        self.proposal_coverage(variable, bound)
    }

    /// Whether this action owns an ordered, page-producing source frontier.
    ///
    /// Returning `true` replaces eager [`Self::residual_delta_seeds`] for the
    /// exact action. The residual engine creates one affine activation per
    /// parent row and asks [`Self::residual_delta_source_page`] for bounded
    /// pages only as scheduler demand grows. The answer is structural for the
    /// supplied bound schema and must remain stable for the solve.
    #[doc(hidden)]
    fn residual_delta_source_is_paged(&self, _variable: VariableId, _view: &RowsView<'_>) -> bool {
        false
    }

    /// Whether this proposal can expose an ordered direct-candidate source
    /// frontier without first materializing its complete output.
    ///
    /// Unlike `residual_delta_source_is_paged`, this capability is consulted
    /// only for Propose. A direct candidate returned by the page hook is
    /// already a proposal candidate and owns no transition lineage.
    #[doc(hidden)]
    fn residual_proposal_source_is_paged(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
    ) -> bool {
        false
    }

    /// Whether a paged proposal source emits product-state roots rather than
    /// only finished direct candidates.
    ///
    /// Direct candidate pages may be materialized eagerly when a surrounding
    /// finite-formula reducer is quiescent. A root-producing page must retain
    /// the transition substrate: its source page is merely the beginning of a
    /// resumable automaton traversal. This answer is structural for the
    /// supplied bound schema and must remain stable for the solve.
    #[doc(hidden)]
    fn residual_proposal_source_has_transition_roots(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
    ) -> bool {
        false
    }

    /// Consume at most `limit` entries from one activation's ordered source
    /// frontier.
    ///
    /// `view` contains exactly one immutable parent row. During grouped
    /// confirmation, `candidates` is the sorted, deduplicated set of values in
    /// that parent's immutable original candidate sequence; proposal actions
    /// pass `None`. Appended roots belong to this one activation and therefore
    /// carry no parent tags. Appended `accepted` values are direct candidate
    /// occurrences that need no transition expansion; their order and
    /// multiplicity are preserved exactly, unlike transition witnesses that
    /// reduce to distinct accepted endpoints. Returning `Some` declares
    /// support and must satisfy `page.examined <= limit` plus
    /// `roots_added + accepted_added <= page.examined`.
    /// `page.next` is suspended until every root lineage from this page has
    /// retired. The conservative default is unsupported.
    #[doc(hidden)]
    fn residual_delta_source_page(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: Option<&[RawInline]>,
        _cursor: ResidualDeltaSourceCursor,
        _limit: usize,
        _roots: &mut Vec<ResidualDeltaOutput>,
        _accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        None
    }

    /// Consume one physically compatible cohort of affine source pages.
    ///
    /// `pages` receives exactly one row-aligned page descriptor. Roots and
    /// direct accepted occurrences carry in-range input-row tags grouped in
    /// ascending order, just like [`Self::residual_delta_expand`] successors.
    /// Returning `false` declares the complete cohort unsupported and must
    /// leave all three output vectors unchanged. Once the corresponding source
    /// capability admitted these activations, changing that answer is an
    /// engine error and the scheduler panics rather than falling back after
    /// consuming affine credits. An implementation may override this hook with
    /// a native batched kernel. The default preserves compatibility by invoking
    /// [`Self::residual_delta_source_page`] once per row and rolling back every
    /// output if any row reports unsupported.
    #[doc(hidden)]
    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        let row_count = batch.view.len();
        assert_eq!(batch.candidate_sets.len(), row_count);
        assert_eq!(batch.cursors.len(), row_count);
        assert_eq!(batch.limits.len(), row_count);

        let page_base = pages.len();
        let root_base = roots.len();
        let accepted_base = accepted.len();
        for row in 0..row_count {
            let mut row_roots = Vec::new();
            let mut row_accepted = Vec::new();
            let Some(page) = self.residual_delta_source_page(
                variable,
                &batch.view.row_view(row),
                batch.candidate_sets[row],
                batch.cursors[row],
                batch.limits[row],
                &mut row_roots,
                &mut row_accepted,
            ) else {
                pages.truncate(page_base);
                roots.truncate(root_base);
                accepted.truncate(accepted_base);
                return false;
            };
            let tag = u32::try_from(row).expect("residual source cohort exceeds u32 tags");
            pages.push(page);
            roots.extend(row_roots.into_iter().map(|root| (tag, root)));
            accepted.extend(row_accepted.into_iter().map(|value| (tag, value)));
        }
        true
    }

    /// Seeds zero or more engine-owned transition programs for each parent row.
    ///
    /// Returning `true` opts this exact `(constraint, variable, bound schema)`
    /// proposal or confirm action into residual delta execution. Every
    /// appended seed carries an in-range parent-row tag and tags are grouped in
    /// ascending order. Proposal actions may append zero or more seeds per
    /// parent; repeated tags denote distinct affine producer roots inside one
    /// parent activation. That activation streams proposal effects but does not
    /// reduce a confirmation until every root lineage quiesces. A page-local
    /// finite confirmation owns only its disjoint candidate page; a grouped
    /// confirmation owns the complete parent sequence. In both cases the
    /// immutable sequence supplies exact order and multiplicity. A nullable
    /// program may mark its seed accepted without adding it to work novelty;
    /// the scheduler records that endpoint at activation creation and may
    /// publish a streaming proposal or Support witness before expanding the
    /// seed's independent transition credit. Seed acceptance consumes no
    /// transition-page demand.
    /// Returning `true` with no seeds for a parent is an exact empty result for
    /// that parent. The conservative default retains the ordinary constraint
    /// protocol.
    #[doc(hidden)]
    fn residual_delta_seeds(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        false
    }

    /// Seeds a transition-backed boolean test for fully-bound parent rows.
    ///
    /// Returning `Some(variable)` declares support and selects the structural
    /// route subsequently passed to [`Self::residual_delta_expand`]. Every
    /// constraint variable must already be present in `view`. Appended seeds
    /// follow the same ascending parent-tag law as
    /// [`Self::residual_delta_seeds`], but accepted outputs are boolean
    /// witnesses rather than proposed values. Returning `Some` with no seed
    /// for a parent is an exact false result for that parent. Returning `None`
    /// must leave `seeds` untouched.
    ///
    /// The route must depend only on the constraint and bound schema, never on
    /// row values, so every seeded node remains valid under one canonical
    /// structural transition operator.
    #[doc(hidden)]
    fn residual_delta_support_seeds(
        &self,
        _view: &RowsView<'_>,
        _seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        None
    }

    /// Expands at most `limit` entries from one transition node's ordered
    /// outgoing frontier.
    ///
    /// Returning `Some` opts this exact node into affine transition paging.
    /// Appended outputs belong to the supplied node and therefore carry no
    /// input tags. Their count must not exceed `page.examined`, which in turn
    /// must not exceed `limit`. A nonterminal page resumes strictly after its
    /// previous cursor in the same `(branch, value)` order. Returning `None`
    /// from `Start` retains block-native [`Self::residual_delta_expand`]; a
    /// node that has returned a nonterminal page must continue to support every
    /// cursor it produced.
    #[doc(hidden)]
    fn residual_delta_expand_page(
        &self,
        _variable: VariableId,
        _node: ResidualDeltaNode,
        _cursor: ResidualDeltaExpandCursor,
        _limit: usize,
        _successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        None
    }

    /// Expands one physical cohort of bounded transition-node pages.
    ///
    /// `pages` receives one row-aligned entry per input node. `Some(page)`
    /// follows [`Self::residual_delta_expand_page`]; `None` leaves that row for
    /// the block-native eager fallback and is valid only from `Start`.
    /// Successors from supported pages are tagged by input-node index and
    /// grouped in ascending tag order. The default preserves scalar page
    /// implementations while giving block-native constraints one stable seam
    /// for fused CPU or accelerator execution.
    #[doc(hidden)]
    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        assert_eq!(batch.nodes.len(), batch.cursors.len());
        assert_eq!(batch.nodes.len(), batch.limits.len());
        for (row, ((&node, &cursor), &limit)) in batch
            .nodes
            .iter()
            .zip(batch.cursors)
            .zip(batch.limits)
            .enumerate()
        {
            let mut row_successors = Vec::new();
            let page =
                self.residual_delta_expand_page(variable, node, cursor, limit, &mut row_successors);
            if page.is_none() {
                assert_eq!(
                    cursor,
                    ResidualDeltaExpandCursor::Start,
                    "paged delta expansion became unsupported after suspension"
                );
                assert!(
                    row_successors.is_empty(),
                    "unsupported delta expansion page mutated its output"
                );
            } else {
                let row = u32::try_from(row).expect("too many transition pages in one cohort");
                successors.extend(row_successors.into_iter().map(|output| (row, output)));
            }
            pages.push(page);
        }
    }

    /// Expands one block of engine-owned transition-program nodes.
    ///
    /// Successors are tagged by input-node index and grouped in ascending tag
    /// order. A constraint that returned `true` from `residual_delta_seeds`
    /// for an action must return `true` here for the same action.
    #[doc(hidden)]
    fn residual_delta_expand(
        &self,
        _variable: VariableId,
        _nodes: &[ResidualDeltaNode],
        _successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        false
    }
}

/// Dispatches one constraint action through the root-selected protocol.
fn estimate_constraint<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    certified: bool,
    variable: VariableId,
    view: &RowsView<'_>,
    out: &mut EstimateSink<'_>,
) -> bool {
    if certified {
        constraint.estimate_certified(variable, view, out)
    } else {
        constraint.estimate(variable, view, out)
    }
}

fn propose_constraint<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    certified: bool,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    if certified {
        constraint.propose_certified(variable, view, candidates);
    } else {
        constraint.propose(variable, view, candidates);
    }
}

fn confirm_constraint<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    certified: bool,
    variable: VariableId,
    view: &RowsView<'_>,
    candidates: &mut CandidateSink<'_>,
) {
    if certified {
        constraint.confirm_certified(variable, view, candidates);
    } else {
        constraint.confirm(variable, view, candidates);
    }
}

/// Receipt-aware source quote for one scalar row.
///
/// In legacy action-defined mode an estimate is also the relevance/source
/// signal. Once the complete root is certified, coverage owns source
/// eligibility and a missing estimate is merely an unknown (`MAX`) cost.
fn source_quote_scalar<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    certified: bool,
    variable: VariableId,
    bound: VariableSet,
    view: &RowsView<'_>,
) -> Option<(ProposalCoverage, usize)> {
    let coverage = if certified {
        let coverage = constraint.proposal_coverage(variable, bound);
        if coverage == ProposalCoverage::None {
            return None;
        }
        debug_assert!(constraint.fixed_denotation());
        debug_assert!(constraint.variables().is_set(variable));
        coverage
    } else {
        ProposalCoverage::Exact
    };

    let mut estimate = usize::MAX;
    let quoted = estimate_constraint(
        constraint,
        certified,
        variable,
        view,
        &mut EstimateSink::Scalar(&mut estimate),
    );
    if certified {
        Some((coverage, estimate))
    } else {
        quoted.then_some((coverage, estimate))
    }
}

/// Receipt-aware source quote column for a row block.
///
/// Returns the source coverage and appends exactly one cost per row. Certified
/// sources without an estimate receive `usize::MAX`; legacy irrelevance leaves
/// `out` untouched and returns `None`.
fn source_quote_column<'a, C: Constraint<'a> + ?Sized>(
    constraint: &C,
    certified: bool,
    variable: VariableId,
    bound: VariableSet,
    view: &RowsView<'_>,
    out: &mut Vec<usize>,
) -> Option<ProposalCoverage> {
    let coverage = if certified {
        let coverage = constraint.proposal_coverage(variable, bound);
        if coverage == ProposalCoverage::None {
            return None;
        }
        debug_assert!(constraint.fixed_denotation());
        debug_assert!(constraint.variables().is_set(variable));
        coverage
    } else {
        ProposalCoverage::Exact
    };

    let start = out.len();
    let quoted = estimate_constraint(
        constraint,
        certified,
        variable,
        view,
        &mut EstimateSink::Column(out),
    );
    if quoted {
        assert_eq!(
            out.len() - start,
            view.len(),
            "constraint estimate must append one value per row"
        );
        Some(coverage)
    } else if certified {
        assert_eq!(
            out.len(),
            start,
            "missing constraint estimate must leave its sink untouched"
        );
        out.resize(start + view.len(), usize::MAX);
        Some(coverage)
    } else {
        assert_eq!(
            out.len(),
            start,
            "irrelevant constraint estimate must leave its sink untouched"
        );
        None
    }
}

/// Stable diagnostic for a certified frontier that cannot enumerate any
/// remaining variable. A source may become available after another variable
/// is bound (Equality is the canonical example), so callers must apply this
/// only to the exact bound schema they are about to execute.
pub(super) const CERTIFIED_SOURCE_FRONTIER_ERROR: &str =
    "a non-full fixed-denotation query state has no covering proposal source; filter-only and peer-dependent constraints require an enumerable source";

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for Box<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn fixed_denotation(&self) -> bool {
        let inner: &T = self;
        inner.fixed_denotation()
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

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate_certified(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.propose_certified(variable, view, candidates)
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.confirm_certified(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        let inner: &T = self;
        inner.residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        let inner: &T = self;
        inner.residual_union_children()
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        let inner: &T = self;
        inner.residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_grouping_requirements(
        &self,
        variable: VariableId,
    ) -> Option<VariableSet> {
        let inner: &T = self;
        inner.residual_delta_confirm_grouping_requirements(variable)
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

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.residual_delta_source_is_paged(variable, view)
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_proposal_source_has_transition_roots(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_proposal_source_has_transition_roots(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        let inner: &T = self;
        inner.residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_source_pages(variable, batch, pages, roots, accepted)
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_seeds(variable, view, seeds)
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        let inner: &T = self;
        inner.residual_delta_support_seeds(view, seeds)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        let inner: &T = self;
        inner.residual_delta_expand_page(variable, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        let inner: &T = self;
        inner.residual_delta_expand_pages(variable, batch, pages, successors)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_expand(variable, nodes, successors)
    }
}

impl<'a, T: Constraint<'a> + ?Sized> Constraint<'a> for std::sync::Arc<T> {
    fn variables(&self) -> VariableSet {
        let inner: &T = self;
        inner.variables()
    }

    fn fixed_denotation(&self) -> bool {
        let inner: &T = self;
        inner.fixed_denotation()
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

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.estimate_certified(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.propose_certified(variable, view, candidates)
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let inner: &T = self;
        inner.confirm_certified(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        let inner: &T = self;
        inner.influence(variable)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        let inner: &T = self;
        inner.residual_shape()
    }

    fn residual_union_children(&self) -> Option<&dyn ConstraintChildren<'a>> {
        let inner: &T = self;
        inner.residual_union_children()
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        let inner: &T = self;
        inner.residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_grouping_requirements(
        &self,
        variable: VariableId,
    ) -> Option<VariableSet> {
        let inner: &T = self;
        inner.residual_delta_confirm_grouping_requirements(variable)
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

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.residual_delta_source_is_paged(variable, view)
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        let inner: &T = self;
        inner.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_proposal_source_has_transition_roots(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_proposal_source_has_transition_roots(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        let inner: &T = self;
        inner.residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_source_pages(variable, batch, pages, roots, accepted)
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_seeds(variable, view, seeds)
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        let inner: &T = self;
        inner.residual_delta_support_seeds(view, seeds)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        let inner: &T = self;
        inner.residual_delta_expand_page(variable, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        let inner: &T = self;
        inner.residual_delta_expand_pages(variable, batch, pages, successors)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        let inner: &T = self;
        inner.residual_delta_expand(variable, nodes, successors)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryScheduler {
    LazyDag,
    ResidualState,
    Sequential,
}

/// A query is an iterator over the results of a query.
/// It takes a constraint and a post-processing function as input,
/// and returns the results of the query as a stream of values.
/// Every live fresh ordinary iterator uses canonical
/// residual states. It starts with narrow, depth-first action cohorts and
/// widens as the consumer keeps pulling, while histories with identical future
/// computation can reconverge under one state identity. The production
/// lowering flattens exposed associative AND regions, preserves other finite
/// composites such as Union as fused constraint kernels, and executes
/// production-qualified regular-path Programs as heterogeneous state actions.
/// Explicit routes deferred by policy use the ordinary constraint action. A
/// structurally absent route may instead retain the constraint's legacy pager
/// or seed hooks. Seed-rejected queries start no runtime. Use
/// [`Query::lazy_dag_scheduler`] for the bound-variable-set DAG control and
/// [`Query::sequential`] for the scalar depth-first specialization. The
/// Scheduler selection and structural lowering are independent controls; use
/// [`Query::residual_lowering`] to select a conservative or intermediate
/// lowering without changing the scheduler. Fully drained scheduler results
/// produce the same distinct raw projected-row set; their iteration order may
/// differ. Strict-projection keys are claimed before Rust conversion, so
/// conversion failure or panic never retries the same raw row through another
/// witness. Full heads need no terminal claim table: engine action admission
/// already makes complete raw bindings unique, and a full projection is
/// injective.
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
    scheduler: QueryScheduler,
    /// Structural lowering selected independently from the physical scheduler.
    residual_lowering: residual::ResidualLowering,
    /// The complete root certified one fixed denotation when this query was
    /// constructed. Receipt-aware planning is all-or-nothing at this boundary;
    /// default-false custom roots retain legacy action semantics.
    certified_denotation: bool,
    mode: Search,
    /// Whether [`Iterator::next`] has ever been called on this query.
    ///
    /// Probe solvers restart from the seed block and therefore require this
    /// to remain `false`. Cursor shape cannot encode the same fact: an
    /// untouched failed zero-variable settlement and a successfully drained
    /// zero-variable query are both `Done` with empty cursor state. This bit
    /// also records a failed `next()` call, giving freshness the simple exact
    /// meaning "the iterator has never been pulled."
    iteration_started: bool,
    influences: [VariableSet; 128],
    estimates: [usize; 128],
    /// PROBE (order-key experiment): each variable's estimate against the
    /// **empty** binding, frozen at [`Query::new`] — the static baseline
    /// the `ratio_first` / `influenced_only` keys compare against.
    base_estimates: [usize; 128],
    touched_variables: VariableSet,
    /// The borrowed cursor, half one: bound variables in binding order.
    stack: ArrayVec<VariableId, 128>,
    /// The borrowed cursor, half two: bound values parallel to `stack`.
    /// `RowsView::new_indexed(&stack, &row, &cols)` is the engine's
    /// single-row block — the sequential engine is literally a block-of-1
    /// caller.
    row: ArrayVec<RawInline, 128>,
    /// Variable→column index for the cursor ([`RowsView::new_indexed`]):
    /// `cols[v]` = position of `v` in `stack`, [`COL_UNBOUND`] otherwise.
    /// Maintained incrementally on push/pop, so constraints locate their
    /// columns in O(1) instead of scanning the stack per verb call.
    cols: [u8; 128],
    /// Bitset mirror of `stack` (estimate-staleness bookkeeping).
    bound: VariableSet,
    unbound: ArrayVec<VariableId, 128>,
    /// Per-variable proposal buffers — plain values, no row tags: the
    /// cursor is a block of one, so the row index is statically 0
    /// ([`CandidateSink::Values`]).
    values: ArrayVec<Option<Vec<RawInline>>, 128>,
    /// Reused scratch for admitting one scalar proposal action to SET
    /// support without allocating on every depth-first branch.
    value_admission: AHashSet<RawInline>,
    /// Emit-only scratch: filled from the cursor when a full row is
    /// postprocessed. The only place a [`Binding`] still exists.
    binding: Binding,
    /// Lazily initialized lazy-DAG state for an explicit scheduler override.
    /// Keeping the worklist in a box avoids growing the already-large
    /// sequential cursor copied by rayon's DFS splitter.
    dag: Option<Box<DagState>>,
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
        // Both cursor forms contain only raw bindings, never projected `R`s,
        // so a clone snapshots the exact remaining search without requiring
        // the output type itself to implement `Clone`.
        debug_assert!(self.value_admission.is_empty());
        Self {
            constraint: self.constraint.clone(),
            postprocessing: self.postprocessing.clone(),
            projection: self.projection.clone(),
            scheduler: self.scheduler,
            residual_lowering: self.residual_lowering,
            certified_denotation: self.certified_denotation,
            mode: self.mode,
            iteration_started: self.iteration_started,
            influences: self.influences,
            estimates: self.estimates,
            base_estimates: self.base_estimates,
            touched_variables: self.touched_variables,
            stack: self.stack.clone(),
            row: self.row.clone(),
            cols: self.cols,
            bound: self.bound,
            unbound: self.unbound.clone(),
            values: self.values.clone(),
            // Action admission never spans a cursor boundary. Do not clone an
            // empty scratch table whose retained capacity may match a very
            // large root proposal (especially on Rayon's first split).
            value_admission: AHashSet::new(),
            binding: self.binding.clone(),
            dag: self.dag.clone(),
            residual: self.residual.clone(),
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Query<C, P, R> {
    /// Picks the next unbound variable, refreshes estimates touched by
    /// the most recent binding, re-sorts `unbound`, fills the variable's
    /// proposal vector via [`Constraint::propose`] (on the single-row
    /// cursor view), and pushes it onto the cursor. Leaves
    /// `mode = NextValue`. The caller is responsible for ensuring
    /// `unbound` is non-empty.
    ///
    /// Shared between [`Iterator::next`]'s `NextVariable` branch and the
    /// [`UnindexedProducer::split`](crate::query::QueryParIter) implementation
    /// — the "push + propose" dance is identical in both.
    fn push_next_variable(&mut self) {
        let (variable, coverage) = if self.certified_denotation {
            // Coverage is structural in the complete bound schema and may be
            // enabled by any newly bound peer (Equality is the smallest
            // example). Recompute the at-most-128 source receipts rather than
            // treating an estimate-influence cache as semantic eligibility.
            let view = RowsView::new_indexed(&self.stack, &self.row, &self.cols);
            self.touched_variables = VariableSet::new_empty();
            let mut best = None;
            for (index, &v) in self.unbound.iter().enumerate() {
                let Some((coverage, estimate)) =
                    source_quote_scalar(&self.constraint, true, v, self.bound, &view)
                else {
                    self.estimates[v] = usize::MAX;
                    continue;
                };
                self.estimates[v] = estimate;
                let key = variable_choice_key(
                    v,
                    estimate,
                    self.base_estimates[v],
                    self.influences[v].count(),
                );
                if best.is_none_or(|(_, _, _, best_key)| key > best_key) {
                    best = Some((index, coverage, estimate, key));
                }
            }
            let (index, coverage, _, _) =
                best.unwrap_or_else(|| panic!("{CERTIFIED_SOURCE_FRONTIER_ERROR}"));
            (self.unbound.remove(index), coverage)
        } else {
            let mut stale_estimates = VariableSet::new_empty();
            while let Some(variable) = self.touched_variables.drain_next_ascending() {
                stale_estimates = stale_estimates.union(self.influences[variable]);
            }
            // Bound variables can't be influenced by the unbound ones, so skip.
            stale_estimates = stale_estimates.subtract(self.bound);

            if !stale_estimates.is_empty() {
                let view = RowsView::new_indexed(&self.stack, &self.row, &self.cols);
                while let Some(v) = stale_estimates.drain_next_ascending() {
                    let (_, estimate) =
                        source_quote_scalar(&self.constraint, false, v, self.bound, &view)
                            .expect("unconstrained variable in query");
                    self.estimates[v] = estimate;
                }
                self.unbound.sort_unstable_by_key(|v| {
                    variable_choice_key(
                        *v,
                        self.estimates[*v],
                        self.base_estimates[*v],
                        self.influences[*v].count(),
                    )
                });
            }

            let variable = self.unbound.pop().expect("non-empty unbound");
            (variable, ProposalCoverage::Exact)
        };
        if order_trace::enabled() {
            order_trace::record(self.stack.len(), variable, 1);
        }
        let values = self.values[variable].get_or_insert(Vec::new());
        values.clear();
        // Estimates are ordering costs, not capacity promises. Amortized
        // growth is the only lawful default until the protocol exposes a
        // separate occurrence-count receipt.
        let view = RowsView::new_indexed(&self.stack, &self.row, &self.cols);
        propose_constraint(
            &self.constraint,
            self.certified_denotation,
            variable,
            &view,
            &mut CandidateSink::Values(values),
        );
        if coverage == ProposalCoverage::Covering {
            confirm_constraint(
                &self.constraint,
                true,
                variable,
                &view,
                &mut CandidateSink::Values(values),
            );
        }
        // `values` is a tail-popped action stack. Keep each value's last
        // stored occurrence so the first-seen DFS order remains unchanged:
        // `[a, b, a] -> [b, a]`, then pop yields `a, b`.
        admit_reverse_stable_set(values, &mut self.value_admission);
        self.cols[variable] = self.stack.len() as u8;
        self.stack.push(variable);
        self.row.push([0; 32]);
        self.bound.set(variable);
    }

    /// Fills the emit-only [`Binding`] from the cursor and runs the
    /// postprocessing closure on it.
    fn emit(&mut self) -> ProjectionStep<R> {
        for (k, &v) in self.stack.iter().enumerate() {
            self.binding.set(v, &self.row[k]);
        }
        self.projection.project(&self.binding, &self.postprocessing)
    }

    /// PROBE: frontier-batched (block-at-a-time) solver — the exact-grouped,
    /// unmerged, saturated-width configuration of the worklist core.
    ///
    /// The scalar [`sequential`](Self::sequential) scheduler descends one
    /// binding at a time, so on star or filter shapes every sibling branch
    /// runs its own tiny
    /// propose/confirm round — the per-branch candidate sets (≤ a few
    /// values) are far below any batching break-even. `solve_blocked`
    /// instead carries a **block** of sibling partial bindings per level
    /// and hands whole frontiers of `(row, candidate)` pairs to the
    /// constraints ([`Constraint::propose`] / [`Constraint::confirm`]
    /// over multi-row [`RowsView`]s): one ragged batch per
    /// (constraint, level) instead of one call per branch. Each row retains
    /// the same adaptive next-variable choice as scalar execution, and rows
    /// batch only when that semantic action agrees.
    ///
    /// Semantics: yields the same distinct projected-row set as the iterator;
    /// row order may differ (block order instead of DFS order).
    pub fn solve_blocked(self) -> Vec<R> {
        let mut it = self.solve_dag_lazy().start_width(usize::MAX);
        it.state.merge = false;
        it.collect()
    }

    /// Use the scalar depth-first scheduler for this query.
    ///
    /// Every live fresh ordinary iterator uses residual states. The scalar
    /// scheduler remains useful for tiny queries, strict frontier-memory
    /// bounds, and as the block-of-one specialization of the same block-native
    /// constraint protocol; [`Query::lazy_dag_scheduler`] selects the explicit
    /// bound-variable-set worklist control.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started. Scheduler selection must be
    /// made before the first call to [`Iterator::next`].
    pub fn sequential(mut self) -> Self {
        assert!(
            !self.iteration_started && self.dag.is_none() && self.residual.is_none(),
            "cannot select the sequential query scheduler after iteration has started"
        );
        self.scheduler = QueryScheduler::Sequential;
        self
    }

    /// Force canonical residual-state execution through the ordinary
    /// resumable [`Query`] iterator.
    ///
    /// Ordinary live iteration already selects residual states for every root.
    /// This explicit selector can restore that scheduler after another builder
    /// choice and remains useful as a completeness/comparison control. A
    /// seed-rejected query still starts no worklist. The selector preserves the
    /// query's structural lowering. Use
    /// [`Query::residual_lowering`] before this method to choose another of the
    /// nine canonical lowering forms. The runtime cursor remains behind
    /// `Query::next`, so cloning a started query
    /// snapshots its exact raw remainder. Ordinary Rayon conversion of an
    /// unstarted query still uses the established scalar splitter; use
    /// `Query::into_par_residual_state_iter` (with the `parallel` feature) to
    /// request affine residual sharding explicitly.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started. Scheduler selection must be
    /// made before the first call to [`Iterator::next`].
    pub fn residual_state_scheduler(mut self) -> Self {
        assert!(
            !self.iteration_started && self.dag.is_none() && self.residual.is_none(),
            "cannot select the residual-state query scheduler after iteration has started"
        );
        self.scheduler = QueryScheduler::ResidualState;
        self
    }

    /// Select structural lowering independently from the physical scheduler.
    ///
    /// Ordinary live queries start with [`residual::ResidualLowering::HYBRID`].
    /// Explicit scheduler comparisons can request
    /// [`residual::ResidualLowering::CONSERVATIVE`] or any intermediate form
    /// without changing their scheduler.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started. Lowering must be selected
    /// before the first call to [`Iterator::next`].
    pub fn residual_lowering(mut self, lowering: residual::ResidualLowering) -> Self {
        assert!(
            !self.iteration_started && self.dag.is_none() && self.residual.is_none(),
            "cannot select residual lowering after iteration has started"
        );
        self.residual_lowering = lowering;
        self
    }

    /// Use the lazy bound-variable-set DAG through the ordinary resumable
    /// [`Query`] iterator.
    ///
    /// This is a diagnostic and behavioral control for comparing the DAG
    /// worklist with the residual-default ordinary scheduler. It keeps its raw
    /// resumable worklist behind `Query::next`, so cloning a started query
    /// snapshots the exact remainder. Converting an unstarted selected query
    /// through ordinary Rayon iteration still uses the established scalar
    /// splitter; use `Query::into_par_dag_iter` (with the `parallel` feature)
    /// to request affine DAG sharding explicitly.
    ///
    /// # Panics
    ///
    /// Panics if iteration has already started. Scheduler selection must be
    /// made before the first call to [`Iterator::next`].
    pub fn lazy_dag_scheduler(mut self) -> Self {
        assert!(
            !self.iteration_started && self.dag.is_none() && self.residual.is_none(),
            "cannot select the lazy-DAG query scheduler after iteration has started"
        );
        self.scheduler = QueryScheduler::LazyDag;
        self
    }

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
    /// Panics when a non-empty fixed-denotation root survives its exact seed
    /// check but no variable has a covering proposal source at the empty
    /// binding. Confirmation-only and peer-dependent constraints must be
    /// paired with an enumerable source.
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
        let certified_denotation = constraint.fixed_denotation();
        let influences = std::array::from_fn(|v| {
            if variables.is_set(v) {
                constraint.influence(v)
            } else {
                VariableSet::new_empty()
            }
        });
        let mut has_initial_source = false;
        let estimates = std::array::from_fn(|v| {
            if variables.is_set(v) {
                let quote = source_quote_scalar(
                    &constraint,
                    certified_denotation,
                    v,
                    VariableSet::new_empty(),
                    &RowsView::EMPTY,
                );
                has_initial_source |= quote.is_some();
                quote.map_or_else(
                    || {
                        assert!(certified_denotation, "unconstrained variable in query");
                        usize::MAX
                    },
                    |(_, estimate)| estimate,
                )
            } else {
                usize::MAX
            }
        });
        // The estimates just computed ARE the empty-binding baseline —
        // freeze a copy before iteration refreshes them in place.
        let base_estimates = estimates;
        let mut unbound = ArrayVec::from_iter(variables);
        unbound.sort_unstable_by_key(|v| {
            variable_choice_key(
                *v,
                estimates[*v],
                base_estimates[*v],
                influences[*v].count(),
            )
        });

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
        let mode =
            if residual::seed_survives(&constraint, VariableSet::new_empty(), &RowsView::EMPTY) {
                Search::NextVariable
            } else {
                Search::Done
            };
        if certified_denotation && matches!(mode, Search::NextVariable) && !variables.is_empty() {
            assert!(has_initial_source, "{CERTIFIED_SOURCE_FRONTIER_ERROR}");
        }
        let scheduler = if matches!(mode, Search::NextVariable) {
            QueryScheduler::ResidualState
        } else {
            QueryScheduler::LazyDag
        };
        Query {
            constraint,
            postprocessing,
            projection,
            scheduler,
            residual_lowering: residual::ResidualLowering::HYBRID,
            certified_denotation,
            mode,
            iteration_started: false,
            influences,
            estimates,
            base_estimates,
            touched_variables: VariableSet::new_empty(),
            stack: ArrayVec::new(),
            row: ArrayVec::new(),
            cols: [COL_UNBOUND; 128],
            bound: VariableSet::new_empty(),
            unbound,
            values: ArrayVec::from([const { None }; 128]),
            value_admission: AHashSet::new(),
            binding: Binding::default(),
            dag: None,
            residual: None,
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
    *MODE.get_or_init(|| match std::env::var("TRIBLES_ORDER_KEY").as_deref() {
        Ok("influence_first") => OrderKeyMode::InfluenceFirst,
        Ok("ratio_first") => OrderKeyMode::RatioFirst,
        Ok("influenced_only") => OrderKeyMode::InfluencedOnly,
        _ => OrderKeyMode::Default,
    })
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
    let magnitude = estimate_magnitude(estimate);
    let inv_magnitude = u64::MAX - magnitude;
    let influence = influence_count as u64;
    match order_key_mode() {
        OrderKeyMode::Default => (inv_magnitude, influence, 0),
        OrderKeyMode::InfluenceFirst => (influence, inv_magnitude, 0),
        OrderKeyMode::RatioFirst => {
            let base_magnitude = estimate_magnitude(base_estimate);
            let drop = base_magnitude.saturating_sub(magnitude);
            (drop, inv_magnitude, influence)
        }
        OrderKeyMode::InfluencedOnly => {
            let dropped = (estimate < base_estimate) as u64;
            (dropped, inv_magnitude, influence)
        }
    }
}

/// Total ordering for a row's adaptive variable action. Lower variable IDs win
/// exact ordering-key ties, so scalar sort/pop and block-native scans select the
/// same semantic action without relying on unstable-sort tie behavior.
#[inline]
fn variable_choice_key(
    variable: VariableId,
    estimate: usize,
    base_estimate: usize,
    influence_count: usize,
) -> (u64, u64, u64, std::cmp::Reverse<VariableId>) {
    let (first, second, third) = variable_order_key(estimate, base_estimate, influence_count);
    (first, second, third, std::cmp::Reverse(variable))
}

#[inline]
fn estimate_magnitude(estimate: usize) -> u64 {
    estimate.checked_ilog2().map(|m| m + 1).unwrap_or(0) as u64
}

/// PROBE (order-key experiment): cheap realized-variable-order trace. Off
/// by default; harnesses enable it around an untimed run. Each pick of a
/// next variable records `(depth, variable, weight)` — weight 1 per
/// branch in the sequential engine and the exact group's row count in the
/// block-native engines. [`report`](order_trace::report) aggregates counts per
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
/// [`report`](blocked_stats::report). One mutex lock per *pop* (not per
/// row), so the enabled overhead is negligible next to the
/// propose/confirm work it describes.
pub mod blocked_stats {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;

    /// One record per worklist pop that expanded a frontier.
    #[derive(Clone, Debug)]
    pub struct LevelRecord {
        /// Number of variables bound on entry (search depth).
        pub depth: usize,
        /// Rows in the block this call handled.
        pub rows: usize,
        /// Slow-start chunk width in force for this pop.
        pub chunk_width: usize,
        /// Per-exact-variable-group row counts.
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

    /// Total intermediate rows materialized into child blocks.
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
            let mut batches: Vec<usize> = recs
                .iter()
                .flat_map(|r| r.batch_sizes.iter().copied())
                .collect();
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
                batches.len(),
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
    use std::sync::Mutex;

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static POPS: AtomicU64 = AtomicU64::new(0);
    static BUCKETS_CREATED: AtomicU64 = AtomicU64::new(0);
    static MAX_LIVE_BUCKETS: AtomicU64 = AtomicU64::new(0);
    static MERGE_EVENTS: AtomicU64 = AtomicU64::new(0);
    static MERGED_ROWS: AtomicU64 = AtomicU64::new(0);
    static PARALLEL_SPLITS: AtomicU64 = AtomicU64::new(0);
    /// PROBE (lazy-dag): chunk width per engine resumption, in resumption
    /// order — the slow-start trajectory.
    static WIDTHS: Mutex<Vec<u64>> = Mutex::new(Vec::new());

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
        PARALLEL_SPLITS.store(0, Ordering::Relaxed);
        WIDTHS.lock().unwrap().clear();
    }

    pub(crate) fn record_width(width: usize) {
        WIDTHS.lock().unwrap().push(width as u64);
    }

    /// PROBE (lazy-dag): the chunk-width trajectory — one entry per engine
    /// resumption of a [`DagIter`](super::DagIter), in order.
    pub fn widths() -> Vec<u64> {
        WIDTHS.lock().unwrap().clone()
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

    #[cfg(feature = "parallel")]
    pub(crate) fn record_parallel_split() {
        PARALLEL_SPLITS.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of merge events (filings that appended to a non-empty
    /// bucket from a different pop).
    pub fn merge_events() -> u64 {
        MERGE_EVENTS.load(Ordering::Relaxed)
    }

    /// Number of bucket pops performed by DAG-backed query iteration.
    pub fn pops() -> u64 {
        POPS.load(Ordering::Relaxed)
    }

    /// Number of successful affine-frontier splits performed for fresh
    /// parallel DAG queries while recording was enabled.
    pub fn parallel_splits() -> u64 {
        PARALLEL_SPLITS.load(Ordering::Relaxed)
    }

    /// Terse counter summary.
    pub fn report() -> String {
        let widths = WIDTHS.lock().unwrap();
        let widths_str = if widths.is_empty() {
            String::new()
        } else {
            let shown: Vec<String> = widths.iter().take(24).map(|w| w.to_string()).collect();
            let ellipsis = if widths.len() > 24 { ", …" } else { "" };
            format!(
                " / widths[{}]: {}{}",
                widths.len(),
                shown.join(","),
                ellipsis
            )
        };
        format!(
            "pops {} / buckets created {} / max live {} / merge events {} ({} rows merged) / parallel splits {}{}",
            POPS.load(Ordering::Relaxed),
            BUCKETS_CREATED.load(Ordering::Relaxed),
            MAX_LIVE_BUCKETS.load(Ordering::Relaxed),
            MERGE_EVENTS.load(Ordering::Relaxed),
            MERGED_ROWS.load(Ordering::Relaxed),
            PARALLEL_SPLITS.load(Ordering::Relaxed),
            widths_str,
        )
    }
}

/// PROBE (dag-frontier): scheduling ablation — when
/// `TRIBLES_DAG_STRICT_DEEPEST` is set, [`Query::solve_dag`] pops the
/// globally deepest bucket **without** the readiness gate (the
/// whiteboard's original rule). Prediction, checkable via
/// [`dag_stats`]: cross-parent merge events collapse to ~0, because a
/// reconvergent bucket is popped right after its first parent files —
/// its children out-deepen every pending sibling route.
pub fn dag_strict_deepest() -> bool {
    static STRICT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *STRICT.get_or_init(|| std::env::var("TRIBLES_DAG_STRICT_DEEPEST").is_ok())
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
#[derive(Clone)]
struct DagBucket {
    /// Bound-variable set (`vars` as a bitset) — the bucket key.
    set: VariableSet,
    /// Bound variables, ascending — the column layout.
    vars: Vec<VariableId>,
    /// Row store: `rows.len() / vars.len()` rows of `vars.len()` values.
    rows: Vec<RawInline>,
    /// Pop id of the last filing (merge-event detection only).
    writer: u64,
    /// Pending-contributor count — the number of live buckets whose set
    /// is a **strict subset** of this bucket's set. Rows only ever gain
    /// variables, so every future filing into this bucket must come from
    /// such a contributor; `pending == 0` therefore *is* the readiness
    /// gate, replacing the O(buckets²) subset scan with O(buckets)
    /// incremental maintenance on create/retire. Maintained only in merge
    /// mode (the gate exists to hold buckets *for* merging); invariant
    /// checked against the scan in debug builds ([`dag_gate_check`]).
    pending: u32,
}

/// Counting-gate maintenance — a contributor with set `retired` is gone
/// for good (bucket fully consumed; a pop's filings complete before the
/// next pop decision, so consumption and retirement fuse): every live
/// strict superset loses one pending contributor.
fn dag_gate_retire(buckets: &mut [DagBucket], retired: &VariableSet) {
    for o in buckets.iter_mut() {
        if o.set != *retired && retired.is_subset_of(&o.set) {
            debug_assert!(o.pending > 0, "pending-contributor underflow");
            o.pending -= 1;
        }
    }
}

/// Counting-gate maintenance for a newly created bucket — returns its
/// initial pending count (live strict subsets) and registers it as a new
/// pending contributor with every live strict superset.
fn dag_gate_admit(buckets: &mut [DagBucket], new_set: &VariableSet) -> u32 {
    let mut pending = 0u32;
    for o in buckets.iter_mut() {
        if o.set == *new_set {
            continue;
        }
        if o.set.is_subset_of(new_set) {
            pending += 1;
        }
        if new_set.is_subset_of(&o.set) {
            o.pending += 1;
        }
    }
    pending
}

/// Equivalence assertion — every live bucket's incrementally maintained
/// `pending` must equal the O(n²) strict-subset scan the gate replaced.
/// Debug builds run this at every pop decision in merge mode, so the
/// whole `solve_blocked` parity corpus doubles as the counting-gate ==
/// scan-gate proof.
fn dag_gate_check(buckets: &[DagBucket]) {
    for b in buckets {
        let scan = buckets
            .iter()
            .filter(|o| o.set != b.set && o.set.is_subset_of(&b.set))
            .count();
        assert_eq!(
            b.pending as usize, scan,
            "counting gate diverged from the subset scan on bucket {:?}",
            b.set
        );
    }
}

/// Rebuild the readiness gate after a parallel frontier split.
///
/// A split moves complete affine rows (or whole buckets) into an independent
/// worklist. Cross-shard contributors can no longer reconverge, so each shard
/// must count only the strict-subset buckets it still owns. Recomputing here is
/// deliberately simple: splitting happens at most a bounded number of times
/// at the Rayon boundary, while the hot worklist path keeps using incremental
/// admit/retire maintenance.
#[cfg(feature = "parallel")]
fn dag_gate_rebuild(buckets: &mut [DagBucket]) {
    for i in 0..buckets.len() {
        let set = buckets[i].set;
        let pending = buckets
            .iter()
            .filter(|other| other.set != set && other.set.is_subset_of(&set))
            .count() as u32;
        buckets[i].pending = pending;
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Query<C, P, R> {
    /// PROBE (dag-frontier): bucket-worklist solver — the tree-becomes-DAG
    /// upgrade of [`solve_blocked`](Self::solve_blocked), i.e. the exact-grouped
    /// worklist with merging enabled, drained eagerly at saturated width.
    ///
    /// Evaluation state is a worklist of **buckets keyed by
    /// bound-variable-set** instead of a recursion stack. Pop a bucket,
    /// partition its rows by preferred next variable, run one batched
    /// propose+confirm per (group, variable), then **file** the extended
    /// rows into the bucket keyed by `bound ∪ {v}` — creating it or
    /// **appending** to it. The append is the whole point: rows whose
    /// routes through the variable lattice bound the same set in different
    /// orders *reconverge* into one row store and every downstream batch
    /// is correspondingly fatter. Rows are affine — moved on pop, never
    /// copied between buckets. Full-bound buckets emit.
    ///
    /// Scheduling: **deepest-first among ready buckets**, where a bucket
    /// is *ready* iff no live bucket's set is a strict subset of its set
    /// (tracked incrementally by the counting gate — see
    /// [`DagBucket::pending`]). The gate is exact — rows only ever gain
    /// variables, so any future contributor to bucket `S` is currently a
    /// strict subset of `S`; once none exists, `S` is complete and safe
    /// to pop. Without the gate, strict deepest-first pops a reconvergent
    /// bucket after its *first* parent files, so cross-parent rows never
    /// co-locate and the merge is dead machinery. The price is that
    /// reconvergent buckets are *held* until all their feeders drain — on
    /// a densely reconverging lattice the schedule degrades toward
    /// breadth-first and frontier memory grows accordingly (measured, not
    /// hidden). Where routes never reconverge the gate never blocks and
    /// the schedule is DFS-like.
    ///
    /// Semantics: same distinct projected-row set as the sequential iterator
    /// (each row still value-partitions its region of the search space;
    /// merging is co-location only). Row order differs.
    pub fn solve_dag(self) -> Vec<R> {
        self.solve_dag_lazy().start_width(usize::MAX).collect()
    }

    /// PROBE (dag-frontier): [`solve_dag`](Self::solve_dag) with merging
    /// **disabled** — every filing creates a fresh bucket (lineage-keyed),
    /// so reconvergent routes stay in separate row stores. This is the
    /// control that isolates what the merge itself buys (batch
    /// re-fattening) from what the worklist restructuring costs. With no
    /// merging there is nothing to hold buckets *for*, so the readiness
    /// gate is off and scheduling is strict deepest-first (DFS-like) —
    /// which also makes this configuration identical to
    /// [`solve_blocked`](Self::solve_blocked).
    pub fn solve_dag_unmerged(self) -> Vec<R> {
        let mut it = self.solve_dag_lazy().start_width(usize::MAX);
        it.state.merge = false;
        it.collect()
    }

    /// PROBE (lazy-dag): resumable-iterator form of
    /// [`solve_dag`](Self::solve_dag) with **demand-adaptive chunk width**
    /// (TCP slow start).
    ///
    /// The worklist is explicit state, so there is no recursion to
    /// suspend: [`DagIter`] holds the worklist and postprocessing closure;
    /// `next()` postprocesses staged full rows one at a time, else runs pop →
    /// group → batch → file until a full-bound bucket stages another chunk.
    /// Dropping the iterator drops the worklist — this is the
    /// streaming yield catch-5 called for: `exists!`-class consumers stop
    /// the engine at the first match instead of paying for full
    /// enumeration.
    ///
    /// **Slow start.** A per-iterator chunk width starts tiny
    /// (`TRIBLES_LAZY_START_WIDTH`, default 1) and multiplies by
    /// `TRIBLES_LAZY_GROWTH` (default 2) on each engine *resumption* (a
    /// `next()` call that finds the staged-row buffer empty), saturating at
    /// [`block_row_cap`]. Each pop takes at most `width` rows off the
    /// chosen bucket's tail; the remainder stays live under the same key.
    /// Narrow pops keep first-result latency sequential-class; sustained
    /// pulling widens to full harvest batches.
    ///
    /// **Scheduling — sprint vs harvest.** Width and pop order are physical,
    /// but each row's adaptive next-variable assignment is semantic because
    /// the selected proposer owns its candidate support and order. The engine
    /// therefore preserves exact per-row assignments while width and pop order interact:
    /// a
    /// partially drained bucket's remainder is a strict subset of its own
    /// children, so the eager engine's readiness gate would refuse to
    /// descend past it and partial pops would degenerate to level-drain —
    /// exactly the latency the laziness exists to avoid. The gate is
    /// therefore demand-adaptive too: while `width < cap` (*sprint*) the
    /// scheduler pops strict-deepest-first, which at width 1 is the DFS
    /// dive (the cap=1 isomorphism), at the cost of cross-parent merging
    /// (the ablation showed strict-deepest never merges — an accelerator
    /// throughput loss, not a CPU one); once width saturates (*harvest*) the
    /// strict-subset readiness gate switches on and the residual
    /// computation is the eager [`solve_dag`](Self::solve_dag) algorithm
    /// on the remaining state.
    ///
    /// **Exact grouping.** A popped block is partitioned by each row's exact
    /// preferred variable. The DAG cohorts those exact-variable groups and
    /// delegates row-local proposer choice to the root constraint's
    /// block-native [`Constraint::propose`]. Exposed residual planning instead
    /// cohorts explicit `(variable, proposer occurrence)` actions. Neither path
    /// reassigns either choice: an estimate-similar action is not
    /// interchangeable because [`Constraint`] supplies no cross-action
    /// occurrence-bag equivalence law.
    ///
    /// For `R` rows and `V ≤ 128` unbound variables, planning takes `O(RV)`
    /// time and uses `O(RV + V)` reusable scratch space.
    ///
    /// Semantics: fully drained, the same distinct projected-row set as the
    /// sequential iterator and the eager DAG solver; row order differs.
    ///
    /// # Panics
    ///
    /// Panics once [`Iterator::next`] has been called, whether that call
    /// yielded a row or returned `None`. The probe solvers restart evaluation
    /// from the seed block, so an explicit never-pulled rule prevents both
    /// duplicate emission and ambiguity after exhaustion. An untouched query
    /// is fresh, including one whose zero-variable settlement already failed
    /// in [`Query::new`] (`Search::Done` without a `next()` call): that one
    /// correctly yields the empty set.
    pub fn solve_dag_lazy(self) -> DagIter<C, P, R> {
        assert!(
            !self.iteration_started
                && self.stack.is_empty()
                && self.bound.is_empty()
                && self.touched_variables.is_empty()
                && matches!(self.mode, Search::NextVariable | Search::Done),
            "cannot probe-solve a Query mid-iteration: Iterator::next has already \
             been called; probe solvers (solve_blocked/solve_dag/\
             solve_dag_unmerged/solve_dag_lazy) restart from the seed \
             block and require a fresh query"
        );
        let Query {
            constraint,
            postprocessing,
            projection,
            influences,
            base_estimates,
            certified_denotation,
            mode,
            ..
        } = self;
        let full = constraint.variables();
        let mut state = DagState::new(full, certified_denotation);
        // [`Query::new`] settles zero-variable (fully-constant) constraints
        // with one exact `satisfied` probe against the seed block; when the
        // probe failed the query is already `Done`. The DAG worklist never
        // consults zero-variable constraints (they have no unbound
        // variables to propose for), so honor the settlement here by
        // starting with an empty worklist — the DAG engine then agrees
        // with the sequential engine's empty result set.
        if matches!(mode, Search::Done) {
            state.buckets.clear();
        }
        DagIter {
            constraint,
            postprocessing,
            projection,
            influences,
            base_estimates,
            state,
        }
    }
}

/// PROBE (lazy-dag): initial chunk width for [`Query::solve_dag_lazy`] —
/// `TRIBLES_LAZY_START_WIDTH`, default 1. Read per iterator (not cached),
/// so experiments can vary it within one process.
fn lazy_start_width() -> usize {
    std::env::var("TRIBLES_LAZY_START_WIDTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&w| w > 0)
        .unwrap_or(1)
}

/// PROBE (lazy-dag): width growth factor per engine resumption for
/// [`Query::solve_dag_lazy`] — `TRIBLES_LAZY_GROWTH`, default 2 (1 =
/// fixed width). Read per iterator.
fn lazy_growth() -> usize {
    std::env::var("TRIBLES_LAZY_GROWTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&g| g > 0)
        .unwrap_or(2)
}

/// PROBE (lazy-dag): the resumable bucket-worklist engine behind
/// [`Query::solve_dag_lazy`]. See there for the design; per-instance state
/// is exactly `{worklist buckets, raw staged rows, postprocessing, width}`.
///
/// Builder-style [`start_width`](Self::start_width) /
/// [`growth`](Self::growth) override the env defaults (tests need
/// per-instance settings; env vars are process-global).
pub struct DagIter<C, P: Fn(&Binding) -> Option<R>, R> {
    constraint: C,
    postprocessing: P,
    projection: ProjectionGate,
    influences: [VariableSet; 128],
    base_estimates: [usize; 128],
    state: DagState,
}

impl<C, P: Fn(&Binding) -> Option<R>, R> DagIter<C, P, R> {
    /// Overrides the initial chunk width (clamped to `1..=cap`).
    pub fn start_width(mut self, width: usize) -> Self {
        self.state.width = width.clamp(1, self.state.cap);
        self
    }

    /// Overrides the per-resumption width growth factor (min 1 = fixed).
    pub fn growth(mut self, growth: usize) -> Self {
        self.state.growth = growth.max(1);
        self
    }

    /// Overrides the width saturation cap (default [`block_row_cap`]).
    /// Tests use a tiny cap to force the *harvest* regime (gated
    /// scheduling at saturated width), which real workloads only reach
    /// after ~20 doublings; the env cap is process-global and cached.
    pub fn cap(mut self, cap: usize) -> Self {
        self.state.cap = cap.max(1);
        self.state.width = self.state.width.min(self.state.cap);
        self
    }

    /// The chunk width the *next* engine resumption will use — the
    /// slow-start observable (tests sample this per pull; benches use the
    /// process-global [`dag_stats::widths`] trajectory instead).
    pub fn current_width(&self) -> usize {
        self.state.width
    }
}

/// PROBE (lazy-dag): the constraint-agnostic core of the lazy DAG engine —
/// exactly the resumable state (`{worklist buckets, raw staged rows, binding
/// scratch, slow-start width}`), with the constraint,
/// postprocessing, and the frozen `influences`/`base_estimates` tables
/// passed in per call.
#[derive(Clone)]
pub(crate) struct DagState {
    full: VariableSet,
    /// Whole-root receipt captured when this worklist was constructed.
    certified_denotation: bool,
    buckets: Vec<DagBucket>,
    pop_id: u64,
    binding: Binding,
    /// Fully-bound rows staged for demand-driven postprocessing. Results are
    /// deliberately kept in raw form: storing projected `R`s here would make
    /// `Query`'s auto traits depend on its output type and make an exact
    /// mid-iteration clone require `R: Clone`.
    emit_vars: Vec<VariableId>,
    emit_rows: Vec<RawInline>,
    emit_next: usize,
    /// Row count is explicit because a zero-column block contains one virtual
    /// row even though `emit_rows` is empty.
    emit_count: usize,
    width: usize,
    growth: usize,
    cap: usize,
    /// File by bound-set — reconvergent routes co-locate and downstream
    /// batches re-fatten. Off: lineage-keyed filing (a fresh bucket per
    /// filing), the tree-shaped control.
    merge: bool,
    /// Pooled per-pop scratch — the worklist loop is allocation-free in
    /// steady state (bucket row stores and their `vars` are the only
    /// per-pop allocations left, and those are the product, not scratch).
    scratch: DagScratch,
}

/// Per-pop scratch buffers for [`DagState::pop_once`], pooled across pops
/// (taken with `mem::take`, returned when the pop completes).
#[derive(Clone)]
struct DagScratch {
    /// Unbound variables of the popped bucket.
    unbound: Vec<VariableId>,
    /// Column layout of the popped rows (survives bucket removal).
    parent_vars: Vec<VariableId>,
    /// Flat estimate matrix, variable-major: `est[j * rows + i]` — the
    /// columns land contiguously because [`EstimateSink::Column`] appends.
    est: Vec<usize>,
    /// Per-row preferred-variable index (into `unbound`).
    preferred: Vec<u32>,
    /// Rows per group.
    group_counts: Vec<usize>,
    /// Group start offsets (rows).
    starts: Vec<usize>,
    /// Partition write cursors (counting sort).
    cursors: Vec<usize>,
    /// Partitioned row store (populated only when >1 group).
    part: Vec<RawInline>,
    /// Copied tail rows for partial pops.
    work: Vec<RawInline>,
    /// Candidate frontier, reused across groups and pops.
    pairs: Candidates,
    /// Reused scratch for SET-admitting `(parent, value)` actions. Including
    /// the parent tag preserves equal values belonging to distinct rows.
    pair_admission: AHashSet<(u32, RawInline)>,
    /// Variable→column index for the popped layout
    /// ([`RowsView::new_indexed`]), refilled once per pop — every verb
    /// call of every constraint at this level then locates its columns in
    /// O(1) instead of scanning `vars`.
    cols: [u8; 128],
}

impl Default for DagScratch {
    fn default() -> Self {
        DagScratch {
            unbound: Vec::new(),
            parent_vars: Vec::new(),
            est: Vec::new(),
            preferred: Vec::new(),
            group_counts: Vec::new(),
            starts: Vec::new(),
            cursors: Vec::new(),
            part: Vec::new(),
            work: Vec::new(),
            pairs: Vec::new(),
            pair_admission: AHashSet::new(),
            cols: [COL_UNBOUND; 128],
        }
    }
}

impl DagState {
    fn new(full: VariableSet, certified_denotation: bool) -> Self {
        let cap = block_row_cap();
        DagState {
            full,
            certified_denotation,
            buckets: vec![DagBucket {
                set: VariableSet::new_empty(),
                vars: Vec::new(),
                rows: Vec::new(),
                writer: 0,
                pending: 0,
            }],
            pop_id: 0,
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            width: lazy_start_width().clamp(1, cap),
            growth: lazy_growth(),
            cap,
            merge: true,
            scratch: DagScratch::default(),
        }
    }
}

#[cfg(feature = "parallel")]
impl DagState {
    /// Construct an empty worklist with the same scheduler policy as `self`.
    /// Raw frontier rows are installed by [`split_for_parallel`](Self::split_for_parallel).
    fn parallel_sibling(&self) -> Self {
        DagState {
            full: self.full,
            certified_denotation: self.certified_denotation,
            buckets: Vec::new(),
            pop_id: self.pop_id,
            binding: Binding::default(),
            emit_vars: Vec::new(),
            emit_rows: Vec::new(),
            emit_next: 0,
            emit_count: 0,
            width: self.width,
            growth: self.growth,
            cap: self.cap,
            merge: self.merge,
            scratch: DagScratch::default(),
        }
    }

    /// Partition the current affine frontier into two independent worklists.
    ///
    /// Every active row represents one disjoint remainder of the search. The
    /// worklist consumes parents when filing children, so moving rows between
    /// shards neither duplicates nor loses a possible complete binding. Each
    /// shard rebuilds its readiness gate because only contributors remaining
    /// in that shard can reconverge there.
    ///
    /// If the frontier is still the one-row seed (or another unsplittable
    /// one-row chain), advance it serially until a proposal creates at least
    /// two affine rows. This is planning work only: result projection remains
    /// deferred to the Rayon fold leaves.
    fn split_for_parallel<'a, C: Constraint<'a>>(
        &mut self,
        constraint: &C,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<Self> {
        loop {
            debug_assert_eq!(self.emit_next, 0, "Rayon splits before fold consumption");

            // A full-bound block is already a disjoint result frontier. Split
            // it directly without invoking user postprocessing.
            if self.emit_count >= 2 {
                let right_count = self.emit_count / 2;
                let left_count = self.emit_count - right_count;
                let stride = self.emit_vars.len();
                debug_assert!(stride > 0, "a zero-variable query has one result");

                let mut right = self.parallel_sibling();
                right.emit_vars = self.emit_vars.clone();
                right.emit_rows = self.emit_rows.split_off(left_count * stride);
                right.emit_count = right_count;
                self.emit_count = left_count;
                return Some(right);
            }

            // Keep a staged singleton as one shard while another shard drains
            // the remaining worklist.
            if self.emit_count == 1 && !self.buckets.is_empty() {
                let mut right = self.parallel_sibling();
                right.emit_vars = std::mem::take(&mut self.emit_vars);
                right.emit_rows = std::mem::take(&mut self.emit_rows);
                right.emit_count = 1;
                self.emit_count = 0;
                return Some(right);
            }

            // Prefer splitting rows inside one bucket: this is the common
            // case immediately after the seed proposes its first variable and
            // gives both workers similarly shaped block-native work.
            if let Some(index) = self.buckets.iter().position(|bucket| {
                let stride = bucket.vars.len();
                stride > 0 && bucket.rows.len() / stride >= 2
            }) {
                let bucket = &mut self.buckets[index];
                let stride = bucket.vars.len();
                let rows = bucket.rows.len() / stride;
                let left_rows = rows - rows / 2;
                let right_rows = bucket.rows.split_off(left_rows * stride);
                let right_bucket = DagBucket {
                    set: bucket.set,
                    vars: bucket.vars.clone(),
                    rows: right_rows,
                    writer: bucket.writer,
                    pending: 0,
                };

                let mut right = self.parallel_sibling();
                right.buckets.push(right_bucket);
                if self.merge {
                    dag_gate_rebuild(&mut self.buckets);
                    dag_gate_rebuild(&mut right.buckets);
                }
                return Some(right);
            }

            // Multiple singleton buckets are also independent frontier
            // components. Moving one whole bucket avoids descending either
            // component merely to manufacture a split point.
            if self.buckets.len() >= 2 {
                let mut right = self.parallel_sibling();
                right
                    .buckets
                    .push(self.buckets.pop().expect("at least two buckets"));
                if self.merge {
                    dag_gate_rebuild(&mut self.buckets);
                    dag_gate_rebuild(&mut right.buckets);
                }
                return Some(right);
            }

            if self.buckets.is_empty() {
                return None;
            }

            // One unsplittable row remains. Expand it through the normal DAG
            // negotiation; a branching proposal will create the row frontier
            // split by the next loop iteration.
            self.pop_once(constraint, influences, base_estimates, self.width.max(1));
        }
    }
}

/// Files one group's `(row, value)` pairs into the bucket keyed by
/// `parent_set ∪ {variable}` — finding it (merge mode: reconvergent routes
/// co-locate) or creating it (always in lineage mode). New buckets are
/// admitted to the counting gate in merge mode.
///
/// Note on merge stats: with partial pops, the same parent's remainder
/// files into the same child across *different* pop ids, so [`dag_stats`]
/// merge events count self-refills as merges under narrow widths.
#[allow(clippy::too_many_arguments)]
fn dag_file(
    buckets: &mut Vec<DagBucket>,
    merge: bool,
    pop_id: u64,
    parent_set: VariableSet,
    parent_vars: &[VariableId],
    variable: VariableId,
    g_rows: &[RawInline],
    pairs: &[(u32, RawInline)],
) {
    if pairs.is_empty() {
        return;
    }
    let stats = blocked_stats::enabled();
    let dstats = dag_stats::enabled();
    let stride = parent_vars.len();
    let mut child_set = parent_set;
    child_set.set(variable);
    // Canonical layout: insert the new value at the variable's ascending
    // position.
    let vpos = parent_vars
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
            if dstats && !buckets[t].rows.is_empty() && buckets[t].writer != pop_id {
                dag_stats::record_merge(pairs.len());
            }
            buckets[t].writer = pop_id;
            t
        }
        None => {
            let mut child_vars = Vec::with_capacity(child_stride);
            child_vars.extend_from_slice(&parent_vars[..vpos]);
            child_vars.push(variable);
            child_vars.extend_from_slice(&parent_vars[vpos..]);
            let pending = if merge {
                dag_gate_admit(buckets, &child_set)
            } else {
                0
            };
            buckets.push(DagBucket {
                set: child_set,
                vars: child_vars,
                rows: Vec::new(),
                writer: pop_id,
                pending,
            });
            if dstats {
                dag_stats::record_bucket_created(buckets.len());
            }
            buckets.len() - 1
        }
    };
    let store = &mut buckets[target].rows;
    store.reserve(pairs.len() * child_stride);
    for &(row_idx, value) in pairs {
        let base = row_idx as usize * stride;
        store.extend_from_slice(&g_rows[base..base + vpos]);
        store.push(value);
        store.extend_from_slice(&g_rows[base + vpos..base + stride]);
    }
    if stats {
        blocked_stats::record_materialized(pairs.len());
        blocked_stats::cells_add(pairs.len() * child_stride);
    }
}

impl DagState {
    /// One pop: choose a bucket (sprint: strict deepest; harvest:
    /// deepest-ready per the counting gate), take at most `width` rows off
    /// its tail (the seed bucket is one virtual row, always consumed
    /// whole), and either stage full-bound rows for emission or expand by
    /// estimating every row, stable-partitioning by its exact preferred
    /// variable, proposing once per exact-variable group, and filing the
    /// resulting child rows.
    fn pop_once<'a, C: Constraint<'a>>(
        &mut self,
        constraint: &C,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
        width: usize,
    ) {
        let stats = blocked_stats::enabled();
        let dstats = dag_stats::enabled();
        // Readiness gating exists to hold buckets *for merging*, and only
        // pays once the width has saturated (harvest — see
        // `solve_dag_lazy` docs); in sprint, and always in lineage mode,
        // the scheduler pops strict-deepest (DFS-isomorphic). The
        // strict-deepest env ablation forces sprint scheduling throughout.
        let gated = self.merge && width >= self.cap && !dag_strict_deepest();
        if cfg!(debug_assertions) && self.merge {
            dag_gate_check(&self.buckets);
        }
        let mut best: Option<(usize, usize)> = None;
        for (i, b) in self.buckets.iter().enumerate() {
            if gated && b.pending != 0 {
                continue;
            }
            let depth = b.set.count();
            if best.is_none_or(|(_, bd)| depth > bd) {
                best = Some((i, depth));
            }
        }
        let (idx, _) = best.expect("a minimal live bucket is always ready");
        self.pop_id += 1;
        if dstats {
            dag_stats::record_pop();
        }

        // Full-bound bucket: take at most `width` rows off its tail and stage
        // them in raw form — the remainder stays live under the same key,
        // exactly like a partial expansion pop. `pull` postprocesses staged
        // rows one at a time, so output values never become engine state and
        // a later row's side effects (or panic) happen only when the consumer
        // actually pulls that far.
        if self.buckets[idx].set == self.full {
            let n_rows = RowsView::new(&self.buckets[idx].vars, &self.buckets[idx].rows).len();
            let take = n_rows.min(width.max(1));
            debug_assert!(self.emit_next >= self.emit_count);
            self.emit_vars.clear();
            self.emit_rows.clear();
            self.emit_next = 0;
            self.emit_count = take;
            if take == n_rows {
                let mut bucket = self.buckets.swap_remove(idx);
                if self.merge {
                    dag_gate_retire(&mut self.buckets, &bucket.set);
                }
                self.emit_vars.append(&mut bucket.vars);
                self.emit_rows.append(&mut bucket.rows);
            } else {
                let b = &mut self.buckets[idx];
                let split = (n_rows - take) * b.vars.len();
                self.emit_vars.extend_from_slice(&b.vars);
                self.emit_rows.extend_from_slice(&b.rows[split..]);
                b.rows.truncate(split);
            }
            if stats {
                blocked_stats::cells_sub(self.emit_rows.len());
            }
            return;
        }

        let stride = self.buckets[idx].vars.len();

        // Take up to `width` rows off the tail; a remainder stays live
        // under the same key (it is its own future feeder — in harvest
        // mode the gate holds its children until it drains, in sprint
        // mode its children out-deepen it and dive first). The seed
        // bucket is a single virtual zero-width row, so it is always
        // consumed whole and flows through the generic path.
        let mut scratch = std::mem::take(&mut self.scratch);
        let n_rows = RowsView::new(&self.buckets[idx].vars, &self.buckets[idx].rows).len();
        let take = n_rows.min(width.max(1));
        scratch.parent_vars.clear();
        let owned: Vec<RawInline>;
        let (parent_set, work): (VariableSet, &[RawInline]) = if take == n_rows {
            let b = self.buckets.swap_remove(idx);
            if self.merge {
                dag_gate_retire(&mut self.buckets, &b.set);
            }
            scratch.parent_vars.extend_from_slice(&b.vars);
            owned = b.rows;
            (b.set, &owned)
        } else {
            let b = &mut self.buckets[idx];
            let split = (n_rows - take) * stride;
            scratch.work.clear();
            scratch.work.extend_from_slice(&b.rows[split..]);
            b.rows.truncate(split);
            scratch.parent_vars.extend_from_slice(&b.vars);
            (b.set, &scratch.work)
        };
        scratch.cols = [COL_UNBOUND; 128];
        for (i, &v) in scratch.parent_vars.iter().enumerate() {
            scratch.cols[v] = i as u8;
        }
        let view = RowsView::new_indexed(&scratch.parent_vars, work, &scratch.cols);
        let c_rows = take;
        scratch.unbound.clear();
        scratch.unbound.extend(self.full.subtract(parent_set));
        if self.certified_denotation {
            scratch.unbound.retain(|&variable| {
                constraint.proposal_coverage(variable, parent_set) >= ProposalCoverage::Covering
            });
            assert!(
                !scratch.unbound.is_empty(),
                "{CERTIFIED_SOURCE_FRONTIER_ERROR}"
            );
        }
        let n_unbound = scratch.unbound.len();

        // A single unbound variable means there is no choice to make and
        // no partition to build — skip the estimate pass entirely and
        // propose over the whole block. This is every query's deepest
        // level, which at sprint widths is also the most-popped one.
        if n_unbound == 1 {
            let variable = scratch.unbound[0];
            let coverage = if self.certified_denotation {
                constraint.proposal_coverage(variable, parent_set)
            } else {
                ProposalCoverage::Exact
            };
            if order_trace::enabled() {
                order_trace::record(stride, variable, c_rows as u64);
            }
            scratch.pairs.clear();
            propose_constraint(
                constraint,
                self.certified_denotation,
                variable,
                &view,
                &mut CandidateSink::Tagged(&mut scratch.pairs),
            );
            if coverage == ProposalCoverage::Covering {
                confirm_constraint(
                    constraint,
                    true,
                    variable,
                    &view,
                    &mut CandidateSink::Tagged(&mut scratch.pairs),
                );
            }
            let raw_pairs =
                admit_reverse_stable_set(&mut scratch.pairs, &mut scratch.pair_admission);
            if stats {
                blocked_stats::record_level(blocked_stats::LevelRecord {
                    depth: stride,
                    rows: c_rows,
                    chunk_width: width,
                    group_sizes: vec![c_rows],
                    batch_sizes: vec![raw_pairs],
                });
            }
            dag_file(
                &mut self.buckets,
                self.merge,
                self.pop_id,
                parent_set,
                &scratch.parent_vars,
                variable,
                work,
                &scratch.pairs,
            );
            if stats {
                blocked_stats::cells_sub(work.len());
            }
            self.scratch = scratch;
            return;
        }

        // 1. Estimate: flat variable-major matrix, one column per unbound
        //    variable — columns land contiguously because the sink appends.
        scratch.est.clear();
        for &v in scratch.unbound.iter() {
            let _ = source_quote_column(
                constraint,
                self.certified_denotation,
                v,
                parent_set,
                &view,
                &mut scratch.est,
            )
            .expect("unconstrained variable in query");
        }
        debug_assert_eq!(scratch.est.len(), n_unbound * c_rows);

        // 2. Per-row preferred variable: argmax of the engine's ordering
        //    key over the row's matrix entries. This assignment is semantic:
        //    the chosen proposer owns candidate support and first-seen order.
        scratch.preferred.clear();
        scratch.group_counts.clear();
        scratch.group_counts.resize(n_unbound, 0);
        for i in 0..c_rows {
            let mut preferred = None;
            for j in 0..n_unbound {
                let estimate = scratch.est[j * c_rows + i];
                let key = variable_choice_key(
                    scratch.unbound[j],
                    estimate,
                    base_estimates[scratch.unbound[j]],
                    influences[scratch.unbound[j]].count(),
                );
                if preferred.is_none_or(|(_, best_key)| key > best_key) {
                    preferred = Some((j, key));
                }
            }
            let preferred = preferred.expect("non-empty unbound").0;
            scratch.preferred.push(preferred as u32);
            scratch.group_counts[preferred] += 1;
        }
        let n_groups = scratch
            .group_counts
            .iter()
            .filter(|&&count| count > 0)
            .count();

        // 3. Partition (stable counting sort); a single group borrows the
        //    popped rows directly.
        scratch.starts.clear();
        let mut acc = 0usize;
        for &c in &scratch.group_counts {
            scratch.starts.push(acc);
            acc += c;
        }
        if n_groups > 1 {
            if stats {
                blocked_stats::cells_add(work.len());
            }
            scratch.part.clear();
            scratch.part.resize(work.len(), [0u8; 32]);
            scratch.cursors.clear();
            scratch.cursors.extend_from_slice(&scratch.starts);
            for i in 0..c_rows {
                let j = scratch.preferred[i] as usize;
                let dst = scratch.cursors[j];
                scratch.cursors[j] += 1;
                scratch.part[dst * stride..(dst + 1) * stride]
                    .copy_from_slice(&work[i * stride..(i + 1) * stride]);
            }
        }

        // 4. One batched propose per exact-variable group; file into child
        //    buckets.
        let mut group_sizes_rec: Vec<usize> = Vec::new();
        let mut batch_sizes_rec: Vec<usize> = Vec::new();
        for j in 0..n_unbound {
            let g_count = scratch.group_counts[j];
            if g_count == 0 {
                continue;
            }
            let variable = scratch.unbound[j];
            let g_rows: &[RawInline] = if n_groups == 1 {
                work
            } else {
                &scratch.part[scratch.starts[j] * stride..(scratch.starts[j] + g_count) * stride]
            };
            if order_trace::enabled() {
                order_trace::record(stride, variable, g_count as u64);
            }
            scratch.pairs.clear();
            propose_constraint(
                constraint,
                self.certified_denotation,
                variable,
                &RowsView::new_indexed(&scratch.parent_vars, g_rows, &scratch.cols),
                &mut CandidateSink::Tagged(&mut scratch.pairs),
            );
            if self.certified_denotation
                && constraint.proposal_coverage(variable, parent_set) == ProposalCoverage::Covering
            {
                confirm_constraint(
                    constraint,
                    true,
                    variable,
                    &RowsView::new_indexed(&scratch.parent_vars, g_rows, &scratch.cols),
                    &mut CandidateSink::Tagged(&mut scratch.pairs),
                );
            }
            let raw_pairs =
                admit_reverse_stable_set(&mut scratch.pairs, &mut scratch.pair_admission);
            if stats {
                group_sizes_rec.push(g_count);
                batch_sizes_rec.push(raw_pairs);
            }
            dag_file(
                &mut self.buckets,
                self.merge,
                self.pop_id,
                parent_set,
                &scratch.parent_vars,
                variable,
                g_rows,
                &scratch.pairs,
            );
        }
        if stats {
            if n_groups > 1 {
                blocked_stats::cells_sub(work.len());
            }
            blocked_stats::record_level(blocked_stats::LevelRecord {
                depth: stride,
                rows: c_rows,
                chunk_width: width,
                group_sizes: group_sizes_rec,
                batch_sizes: batch_sizes_rec,
            });
            blocked_stats::cells_sub(work.len());
        }
        self.scratch = scratch;
    }
}

impl DagState {
    /// One consumer pull: postprocess staged full rows one at a time, else
    /// resume the engine — run pops at the current width until something is
    /// staged (or the worklist drains), then grow the width (TCP slow start
    /// on consumer demand).
    fn pull<'a, C, P, R>(
        &mut self,
        constraint: &C,
        postprocessing: &P,
        projection: &mut ProjectionGate,
        influences: &[VariableSet; 128],
        base_estimates: &[usize; 128],
    ) -> Option<R>
    where
        C: Constraint<'a>,
        P: Fn(&Binding) -> Option<R>,
    {
        if projection.is_done() {
            return None;
        }
        loop {
            while self.emit_next < self.emit_count {
                let row_index = self.emit_next;
                // Consume the raw row before invoking user postprocessing. If
                // it panics and the unwind is caught, retrying the iterator
                // must not repeat the same row or its side effects.
                self.emit_next += 1;
                let stride = self.emit_vars.len();
                let start = row_index * stride;
                let row = &self.emit_rows[start..start + stride];
                for (k, &v) in self.emit_vars.iter().enumerate() {
                    self.binding.set(v, &row[k]);
                }
                match projection.project(&self.binding, postprocessing) {
                    ProjectionStep::Yield(result) => return Some(result),
                    ProjectionStep::Skip => {}
                    ProjectionStep::Done => return None,
                }
            }
            if self.buckets.is_empty() {
                return None;
            }
            let width = self.width;
            if dag_stats::enabled() {
                dag_stats::record_width(width);
            }
            while self.emit_next >= self.emit_count && !self.buckets.is_empty() {
                self.pop_once(constraint, influences, base_estimates, width);
            }
            self.width = self.width.saturating_mul(self.growth).clamp(1, self.cap);
        }
    }
}

impl<'a, C: Constraint<'a>, P: Fn(&Binding) -> Option<R>, R> Iterator for DagIter<C, P, R> {
    type Item = R;

    fn next(&mut self) -> Option<R> {
        self.state.pull(
            &self.constraint,
            &self.postprocessing,
            &mut self.projection,
            &self.influences,
            &self.base_estimates,
        )
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
        let fresh = !self.iteration_started;
        // Freshness is an explicit public-iterator property, not something
        // inferred from the cursor. In particular, successful and failed
        // zero-variable queries both have structurally empty `Done` state
        // after a pull. Record the call before any iterator return path.
        self.iteration_started = true;

        if self.projection.is_done() {
            self.mode = Search::Done;
            return None;
        }

        if let Some(state) = &mut self.dag {
            return state.pull(
                &self.constraint,
                &self.postprocessing,
                &mut self.projection,
                &self.influences,
                &self.base_estimates,
            );
        }

        if let Some(state) = &mut self.residual {
            return state.pull(
                &self.constraint,
                &self.postprocessing,
                &mut self.projection,
                &self.influences,
                &self.base_estimates,
            );
        }

        if self.scheduler == QueryScheduler::ResidualState
            && fresh
            && matches!(self.mode, Search::NextVariable | Search::Done)
            && self.stack.is_empty()
            && self.bound.is_empty()
            && self.touched_variables.is_empty()
        {
            let state = self
                .residual
                .insert(Box::new(residual::ResidualQueryState::new(
                    &self.constraint,
                    self.mode,
                    self.residual_lowering,
                )));
            return state.pull(
                &self.constraint,
                &self.postprocessing,
                &mut self.projection,
                &self.influences,
                &self.base_estimates,
            );
        }

        // The lazy DAG is an explicit diagnostic selection. Rayon partitions a
        // fresh ordinary query by advancing the scalar cursor before its leaves
        // call `next`; those partial cursors deliberately stay on the
        // sequential path instead of restarting from a worklist.
        if self.scheduler == QueryScheduler::LazyDag
            && fresh
            && matches!(self.mode, Search::NextVariable)
            && self.stack.is_empty()
            && self.bound.is_empty()
            && self.touched_variables.is_empty()
        {
            let state = self.dag.insert(Box::new(DagState::new(
                self.constraint.variables(),
                self.certified_denotation,
            )));
            return state.pull(
                &self.constraint,
                &self.postprocessing,
                &mut self.projection,
                &self.influences,
                &self.base_estimates,
            );
        }

        loop {
            match &self.mode {
                Search::NextVariable => {
                    self.mode = Search::NextValue;
                    if self.unbound.is_empty() {
                        match self.emit() {
                            ProjectionStep::Yield(result) => return Some(result),
                            ProjectionStep::Skip => {}
                            ProjectionStep::Done => {
                                self.mode = Search::Done;
                                return None;
                            }
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
                            *self.row.last_mut().expect("cursor row parallel to stack") =
                                assignment;
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
                        self.row.pop();
                        self.cols[variable] = COL_UNBOUND;
                        self.bound.unset(variable);
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
            .field("scheduler", &self.scheduler)
            .field("mode", &self.mode)
            .field("iteration_started", &self.iteration_started)
            .field("dag_started", &self.dag.is_some())
            .field("residual_started", &self.residual.is_some())
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
// Ordinary `IntoParallelIterator` retains the established scalar
// split-or-descend path. `Query::into_par_dag_iter` explicitly selects the
// block-native alternative: seed negotiation runs until a bucket contains
// multiple rows, then Rayon bisects those affine rows into independent
// worklists. Each DAG fold leaf therefore keeps batching, per-row variable
// selection, and local route reconvergence. A partially consumed ordinary
// residual or lazy-DAG query remains one exact-remainder leaf when converted
// through the ordinary `IntoParallelIterator` path.
//
// `fold_with` is the terminal leaf: it drives the ordinary `Iterator::next()`
// on whichever scheduler state the producer owns. No duplicated execution
// loop.
// ---------------------------------------------------------------------------

#[cfg(feature = "parallel")]
pub use parallel::QueryParIter;

#[cfg(feature = "parallel")]
mod parallel {
    use super::*;
    use rayon::iter::plumbing::{bridge_unindexed, Folder, UnindexedConsumer, UnindexedProducer};
    use rayon::iter::{IntoParallelIterator, ParallelIterator};

    /// Parallel iterator over the results of a [`Query`]. Obtained either via
    /// ordinary [`IntoParallelIterator::into_par_iter`] (the scalar DFS
    /// splitter) or [`Query::into_par_dag_iter`] (affine DAG-frontier
    /// sharding).
    ///
    /// Drives rayon's work-stealing scheduler through an `UnindexedProducer`
    /// impl on the underlying query state. Ordinary parallel iteration uses
    /// the established scalar cursor splitter. The explicit DAG entry point
    /// partitions the lazy DAG's affine row frontier, so each fold leaf
    /// continues through the block-native scheduler. A started residual-state
    /// cursor is likewise preserved as one unsplittable exact-remainder leaf.
    /// `Query::next` is reused as the fold leaf in every case — parallel
    /// execution adds no duplicate engine loop.
    ///
    /// The inner query is stored in a [`Box`] so rayon's work-stealing
    /// `split` (which clones the producer) doesn't memcpy ~15 KB of query
    /// state on every fork — just a Box pointer copy, with the heap alloc
    /// paid only by the child.
    ///
    /// `split_budget` bounds the number of splits this sub-producer will
    /// perform. It is initialized by [`ParallelIterator::drive_unindexed`]
    /// from the pool that actually consumes the iterator, so moving a prepared
    /// iterator into a custom pool still uses that pool's worker count. Rayon's
    /// default `Splitter` *resets* its budget on every stolen task, so on a
    /// busy pool the split tree could otherwise grow without a query-owned
    /// limit. A fresh DAG receives `N - 1` total splits (at most one shard per
    /// worker); the scalar cursor retains its historical `N²` spare chunks for
    /// finer work stealing.
    ///
    /// Rayon clones the constraint tree and postprocessor for each shard.
    /// Clone-local interior state is therefore clone-local by definition;
    /// aggregate observations belong behind shared synchronization such as
    /// `Arc<AtomicU64>` rather than a `Cell` copied with the closure.
    pub struct QueryParIter<C, P: Fn(&Binding) -> Option<R>, R> {
        inner: Box<Query<C, P, R>>,
        split_budget: usize,
    }

    impl<'a, C, P, R> Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        /// Consume a fresh query as a block-native parallel DAG iterator.
        ///
        /// Unlike ordinary [`IntoParallelIterator::into_par_iter`], which
        /// retains the established scalar DFS splitter, this explicit path
        /// starts the lazy DAG at saturated width and partitions its affine
        /// row frontier into at most one worklist shard per Rayon worker.
        /// Each shard preserves backend batches, per-row variable selection,
        /// and route reconvergence among the rows it owns; reconvergence across
        /// shards is traded for parallelism. Fully drained results preserve the
        /// query's distinct projected-row set, not its iteration order.
        ///
        /// This path is intended for block-oriented or accelerator-backed
        /// constraints. The scalar splitter can remain faster for CPU-only
        /// constraints with inexpensive one-row probes.
        ///
        /// # Panics
        ///
        /// Panics once [`Iterator::next`] has been called. Initializing a new
        /// DAG from the seed after partial consumption would duplicate prior
        /// results; use ordinary `into_par_iter()` to drain a partially
        /// consumed query's exact remaining state as one leaf.
        pub fn into_par_dag_iter(mut self) -> QueryParIter<C, P, R> {
            assert!(
                !self.iteration_started
                    && self.dag.is_none()
                    && self.residual.is_none()
                    && self.stack.is_empty()
                    && self.bound.is_empty()
                    && self.touched_variables.is_empty()
                    && matches!(self.mode, Search::NextVariable | Search::Done),
                "cannot initialize parallel DAG iteration after Iterator::next has been called; \
                 use ordinary into_par_iter() to drain the exact remainder"
            );

            self.scheduler = QueryScheduler::LazyDag;
            let mut state = DagState::new(self.constraint.variables(), self.certified_denotation);
            // Full parallel enumeration is an explicit throughput request, so
            // do not repeat the ordinary iterator's first-result slow start in
            // every shard.
            state.width = state.cap;
            if matches!(self.mode, Search::Done) {
                state.buckets.clear();
            }
            self.dag = Some(Box::new(state));

            QueryParIter {
                inner: Box::new(self),
                // Filled at `drive_unindexed`, inside the consuming pool.
                split_budget: 0,
            }
        }
    }

    impl<'a, C, P, R> IntoParallelIterator for Query<C, P, R>
    where
        C: Constraint<'a> + Clone + Send + 'a,
        P: Fn(&Binding) -> Option<R> + Clone + Send,
        R: Send,
    {
        type Item = R;
        type Iter = QueryParIter<C, P, R>;

        fn into_par_iter(mut self) -> Self::Iter {
            // Ordinary fresh parallel iteration is deliberately the stable
            // scalar DFS path. Marking the scheduler explicitly prevents an
            // unsplittable zero-/one-row leaf from lazily creating a DAG when
            // `fold_with` first calls `Query::next`.
            if !self.iteration_started && self.dag.is_none() && self.residual.is_none() {
                self.scheduler = QueryScheduler::Sequential;
            }

            QueryParIter {
                inner: Box::new(self),
                // Filled at `drive_unindexed`, inside the consuming pool.
                split_budget: 0,
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

        /// Partition whichever scheduler state this producer owns. The
        /// explicit DAG path bisects affine frontier rows; the ordinary path
        /// descends scalar single-value levels until it can bisect a proposal
        /// vector. Exhaustion returns `None`, leaving `self` as a leaf for
        /// `fold_with`. See the module comment for both non-re-enumeration
        /// arguments.
        fn split(mut self) -> (Self, Option<Self>) {
            // The empty projection has one possible public key. Sharding its
            // hidden witnesses can only do redundant work and weakens the
            // existence-query latency guarantee, so keep it in one leaf.
            if self.inner.projection.is_empty_head() {
                self.split_budget = 0;
                return (self, None);
            }
            // A query converted after an ordinary `next()` may own a resumable
            // DAG or residual worklist with projected progress. Keep it as
            // one leaf so the exact remainder is neither restarted nor
            // reordered.
            if (self.inner.dag.is_some() || self.inner.residual.is_some())
                && self.inner.iteration_started
            {
                self.split_budget = 0;
                return (self, None);
            }
            if self.split_budget == 0 {
                return (self, None);
            }
            self.split_budget -= 1;

            // Explicit parallel-DAG query: split the affine frontier. `right`
            // owns disjoint raw rows and receives its own cloned constraint
            // and postprocessor when the surrounding Query is cloned.
            if self.inner.dag.is_some() {
                let right_state = {
                    let q = &mut *self.inner;
                    q.dag.as_mut().expect("checked above").split_for_parallel(
                        &q.constraint,
                        &q.influences,
                        &q.base_estimates,
                    )
                };
                let Some(right_state) = right_state else {
                    self.split_budget = 0;
                    return (self, None);
                };
                if dag_stats::enabled() {
                    dag_stats::record_parallel_split();
                }

                // Clone only the small Query shell plus constraint and
                // postprocessor. Temporarily remove the left worklist so a
                // split does not deep-clone all of its frontier rows merely
                // to overwrite them with `right_state`.
                let left_state = self.inner.dag.take().expect("checked above");
                let projection = self.inner.projection.share_for_parallel();
                let mut right = (*self.inner).clone();
                self.inner.dag = Some(left_state);
                right.dag = Some(Box::new(right_state));
                right.projection.attach_shared(projection);
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

            // Explicit scalar scheduler: historical split-or-descend path.
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
                                q.row.pop();
                                q.cols[variable] = COL_UNBOUND;
                                q.bound.unset(variable);
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
                        *q.row.last_mut().expect("cursor row parallel to stack") = assignment;
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
                        let projection = q.projection.share_for_parallel();
                        let mut right = q.clone();
                        right.values[top] = Some(right_vals);
                        right.projection.attach_shared(projection);

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

        fn drive_unindexed<Con>(mut self, consumer: Con) -> Con::Result
        where
            Con: UnindexedConsumer<Self::Item>,
        {
            let workers = rayon::current_num_threads();
            self.split_budget = match (
                &self.inner.dag,
                &self.inner.residual,
                self.inner.iteration_started,
            ) {
                // Explicit fresh DAG: at most one affine shard per worker.
                (Some(_), None, false) => workers.saturating_sub(1),
                // Partially consumed block-native scheduler: exact remainder,
                // one leaf. Residual sharding is deliberately out of scope.
                (Some(_), _, true) | (_, Some(_), true) => 0,
                // Scalar cursor: retain the established spare work chunks.
                (None, None, _) => workers.saturating_mul(workers).max(2),
                // No public path installs an unstarted residual runtime, but
                // fail closed if an internal caller ever does.
                (None, Some(_), false) | (Some(_), Some(_), false) => 0,
            };
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
    fn scheduler_and_residual_lowering_are_orthogonal_controls() {
        let mut context = VariableContext::new();
        let variable = context.next_variable::<U256BE>();
        let ordinary = Query::new(variable.is(U256BE::inline_from(1u64)), |_| Some(()));
        assert_eq!(ordinary.scheduler, QueryScheduler::ResidualState);
        assert_eq!(
            ordinary.residual_lowering,
            residual::ResidualLowering::HYBRID
        );

        let conservative = ordinary
            .residual_lowering(residual::ResidualLowering::CONSERVATIVE)
            .residual_state_scheduler();
        assert_eq!(conservative.scheduler, QueryScheduler::ResidualState);
        assert_eq!(
            conservative.residual_lowering,
            residual::ResidualLowering::CONSERVATIVE,
            "selecting a scheduler must not rewrite structural lowering"
        );

        let mut context = VariableContext::new();
        let variable = context.next_variable::<U256BE>();
        let intermediate = residual::ResidualLowering::new(
            residual::FormulaScope::UnionLeaves,
            residual::ProgramScope::All,
        );
        let dag = Query::new(variable.is(U256BE::inline_from(1u64)), |_| Some(()))
            .lazy_dag_scheduler()
            .residual_lowering(intermediate);
        assert_eq!(dag.scheduler, QueryScheduler::LazyDag);
        assert_eq!(
            dag.residual_lowering, intermediate,
            "selecting lowering must not rewrite the physical scheduler"
        );
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
    struct ScalarSetAdmissionProbe {
        descendants: std::sync::Arc<std::sync::Mutex<Vec<RawInline>>>,
    }

    impl ScalarSetAdmissionProbe {
        const ROOT: VariableId = 0;
        const LEAF: VariableId = 1;
        const A: RawInline = [4; 32];
        const B: RawInline = [5; 32];
        const LEAF_VALUE: RawInline = [6; 32];
    }

    impl Constraint<'static> for ScalarSetAdmissionProbe {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(Self::ROOT).union(VariableSet::new_singleton(Self::LEAF))
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
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[test]
    fn scalar_action_admission_is_reverse_stable_before_descending() {
        let descendants = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let rows: Vec<_> = Query::new(
            ScalarSetAdmissionProbe {
                descendants: descendants.clone(),
            },
            |binding: &Binding| {
                Some((
                    *binding.get(ScalarSetAdmissionProbe::ROOT)?,
                    *binding.get(ScalarSetAdmissionProbe::LEAF)?,
                ))
            },
        )
        .sequential()
        .collect();

        assert_eq!(
            rows,
            [
                (
                    ScalarSetAdmissionProbe::A,
                    ScalarSetAdmissionProbe::LEAF_VALUE,
                ),
                (
                    ScalarSetAdmissionProbe::B,
                    ScalarSetAdmissionProbe::LEAF_VALUE,
                ),
            ]
        );
        assert_eq!(
            *descendants.lock().unwrap(),
            [ScalarSetAdmissionProbe::A, ScalarSetAdmissionProbe::B],
            "the duplicate tail occurrence must disappear before the next action"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn scalar_action_admission_precedes_rayon_splitting() {
        use rayon::prelude::*;

        let descendants = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut rows: Vec<_> = Query::new(
            ScalarSetAdmissionProbe {
                descendants: descendants.clone(),
            },
            |binding: &Binding| {
                Some((
                    *binding.get(ScalarSetAdmissionProbe::ROOT)?,
                    *binding.get(ScalarSetAdmissionProbe::LEAF)?,
                ))
            },
        )
        .sequential()
        .into_par_iter()
        .collect();
        rows.sort_unstable();

        let mut observed = descendants.lock().unwrap().clone();
        observed.sort_unstable();
        assert_eq!(
            rows,
            [
                (
                    ScalarSetAdmissionProbe::A,
                    ScalarSetAdmissionProbe::LEAF_VALUE,
                ),
                (
                    ScalarSetAdmissionProbe::B,
                    ScalarSetAdmissionProbe::LEAF_VALUE,
                ),
            ]
        );
        assert_eq!(
            observed,
            [ScalarSetAdmissionProbe::A, ScalarSetAdmissionProbe::B],
            "Rayon shards must inherit the already SET-admitted proposal stack"
        );
    }

    #[derive(Clone)]
    struct DagSetAdmissionProbe {
        descendants: std::sync::Arc<std::sync::Mutex<Vec<(RawInline, RawInline)>>>,
    }

    impl DagSetAdmissionProbe {
        const PARENT: VariableId = 0;
        const VALUE: VariableId = 1;
        const LEAF: VariableId = 2;
        const P0: RawInline = [7; 32];
        const P1: RawInline = [8; 32];
        const SHARED_VALUE: RawInline = [9; 32];
        const LEAF_VALUE: RawInline = [10; 32];
    }

    impl Constraint<'static> for DagSetAdmissionProbe {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(Self::PARENT)
                .union(VariableSet::new_singleton(Self::VALUE))
                .union(VariableSet::new_singleton(Self::LEAF))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            match variable {
                Self::PARENT => out.fill(1, view.len()),
                Self::VALUE => out.fill(2, view.len()),
                Self::LEAF => out.fill(4, view.len()),
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
                Self::PARENT => {
                    for row in 0..view.len() {
                        candidates.extend_row(row as u32, [Self::P0, Self::P1]);
                    }
                }
                Self::VALUE => {
                    let parent = view.col(Self::PARENT).expect("parent is bound first");
                    for (row_index, row) in view.iter().enumerate() {
                        let copies = if row[parent] == Self::P0 { 2 } else { 1 };
                        candidates.extend_row(
                            row_index as u32,
                            std::iter::repeat_n(Self::SHARED_VALUE, copies),
                        );
                    }
                }
                Self::LEAF => {
                    let parent = view.col(Self::PARENT).expect("parent is bound");
                    let value = view.col(Self::VALUE).expect("value is bound");
                    let mut descendants = self.descendants.lock().unwrap();
                    for (row_index, row) in view.iter().enumerate() {
                        descendants.push((row[parent], row[value]));
                        candidates.push(row_index as u32, Self::LEAF_VALUE);
                    }
                }
                _ => {}
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[test]
    fn dag_action_admission_is_parent_scoped_before_filing() {
        let descendants = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let rows: Vec<_> = Query::new(
            DagSetAdmissionProbe {
                descendants: descendants.clone(),
            },
            |binding: &Binding| {
                Some((
                    *binding.get(DagSetAdmissionProbe::PARENT)?,
                    *binding.get(DagSetAdmissionProbe::VALUE)?,
                    *binding.get(DagSetAdmissionProbe::LEAF)?,
                ))
            },
        )
        .solve_dag_lazy()
        .start_width(usize::MAX)
        .growth(1)
        .collect();

        assert_eq!(
            rows,
            [
                (
                    DagSetAdmissionProbe::P0,
                    DagSetAdmissionProbe::SHARED_VALUE,
                    DagSetAdmissionProbe::LEAF_VALUE,
                ),
                (
                    DagSetAdmissionProbe::P1,
                    DagSetAdmissionProbe::SHARED_VALUE,
                    DagSetAdmissionProbe::LEAF_VALUE,
                ),
            ]
        );
        assert_eq!(
            *descendants.lock().unwrap(),
            [
                (
                    DagSetAdmissionProbe::P0,
                    DagSetAdmissionProbe::SHARED_VALUE,
                ),
                (
                    DagSetAdmissionProbe::P1,
                    DagSetAdmissionProbe::SHARED_VALUE,
                ),
            ],
            "an intra-parent duplicate must vanish while an equal value under another parent survives"
        );
    }

    #[derive(Clone, Copy)]
    struct DagTerminalSetAdmissionProbe;

    impl Constraint<'static> for DagTerminalSetAdmissionProbe {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(DagSetAdmissionProbe::PARENT)
                .union(VariableSet::new_singleton(DagSetAdmissionProbe::VALUE))
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if self.variables().is_set(variable) {
                out.fill(1, view.len());
                true
            } else {
                false
            }
        }

        fn propose(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            candidates: &mut CandidateSink<'_>,
        ) {
            if variable != DagSetAdmissionProbe::VALUE {
                return;
            }
            let parent = view
                .col(DagSetAdmissionProbe::PARENT)
                .expect("parent is already bound");
            for (row_index, row) in view.iter().enumerate() {
                let copies = if row[parent] == DagSetAdmissionProbe::P0 {
                    2
                } else {
                    1
                };
                candidates.extend_row(
                    row_index as u32,
                    std::iter::repeat_n(DagSetAdmissionProbe::SHARED_VALUE, copies),
                );
            }
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }
    }

    #[test]
    fn dag_single_unbound_fast_path_admits_before_filing() {
        let full = DagTerminalSetAdmissionProbe.variables();
        let mut parent_set = VariableSet::new_empty();
        parent_set.set(DagSetAdmissionProbe::PARENT);
        let mut state = DagState::new(full, false);
        state.buckets = vec![DagBucket {
            set: parent_set,
            vars: vec![DagSetAdmissionProbe::PARENT],
            rows: vec![DagSetAdmissionProbe::P0, DagSetAdmissionProbe::P1],
            writer: 0,
            pending: 0,
        }];

        state.pop_once(
            &DagTerminalSetAdmissionProbe,
            &[VariableSet::new_empty(); 128],
            &[1; 128],
            usize::MAX,
        );

        assert_eq!(state.buckets.len(), 1);
        let child = &state.buckets[0];
        assert_eq!(child.set, full);
        assert_eq!(
            RowsView::new(&child.vars, &child.rows)
                .iter()
                .map(|row| (row[0], row[1]))
                .collect::<Vec<_>>(),
            [
                (DagSetAdmissionProbe::P0, DagSetAdmissionProbe::SHARED_VALUE,),
                (DagSetAdmissionProbe::P1, DagSetAdmissionProbe::SHARED_VALUE,),
            ]
        );
    }

    #[test]
    fn scheduler_width_and_equal_key_ties_preserve_semantic_variable_actions() {
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

            let mut sequential: Vec<_> = variable_order_bag_query(tie_children)
                .sequential()
                .collect();
            let mut residual_narrow: Vec<_> = variable_order_bag_query(tie_children)
                .solve_residual_state_lazy_with(residual::ResidualLowering::FULL)
                .cap(1)
                .start_width(1)
                .growth(1)
                .collect();
            let mut residual_wide: Vec<_> = variable_order_bag_query(tie_children)
                .solve_residual_state_lazy_with(residual::ResidualLowering::FULL)
                .cap(8)
                .start_width(8)
                .growth(1)
                .collect();
            let mut dag_narrow: Vec<_> = variable_order_bag_query(tie_children)
                .solve_dag_lazy()
                .cap(1)
                .start_width(1)
                .growth(1)
                .collect();
            let mut dag_wide: Vec<_> = variable_order_bag_query(tie_children)
                .solve_dag_lazy()
                .cap(8)
                .start_width(8)
                .growth(1)
                .collect();

            for bag in [
                &mut sequential,
                &mut residual_narrow,
                &mut residual_wide,
                &mut dag_narrow,
                &mut dag_wide,
            ] {
                bag.sort_unstable();
                assert_eq!(bag, &expected);
            }
        }
    }
}
