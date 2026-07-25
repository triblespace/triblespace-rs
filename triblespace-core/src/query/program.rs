//! Typed residual-program substrate.
//!
//! The residual engine owns affine scheduling, reducers, and return
//! continuations. A program family owns only its stored typed continuation
//! states and per-activation novelty keys. The erased boundary is crossed once
//! for a physical cohort; individual work items are generational handles into
//! a query-local typed arena rather than boxes or engine-defined opcodes.

use std::any::{type_name, Any, TypeId};
use std::collections::hash_map::Entry;
use std::hash::Hash;

use ahash::{AHashMap, AHashSet};

use super::{RawInline, RowsView, VariableId, VariableSet};

/// Query-local identity supplied to typed novelty admission.
///
/// The numeric value is engine-owned and is never program continuation state.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramActivation(pub(crate) u64);

/// Opaque physical dispatch compatibility class chosen by one program family.
///
/// Classes affect only which handles may share one typed call. They do not
/// participate in logical continuation or novelty identity.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchClass(u32);

impl DispatchClass {
    /// Constructs a family-private physical class.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

/// Physical budget source for one typed continuation.
///
/// This is scheduling metadata, not a semantic opcode: both classes remain in
/// the same program queue and cross the same typed cohort call. `Search`
/// receives the outer geometric width for pageable domain discovery;
/// `Activation` receives the activation-local sparse quantum used for graph
/// product traversal.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramPacing {
    Search,
    Activation,
}

/// Generational reference to one stored, family-private continuation.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProgramWorkHandle {
    slot: u32,
    generation: u32,
}

#[cfg(test)]
impl ProgramWorkHandle {
    pub(crate) const fn test(slot: u32) -> Self {
        Self {
            slot,
            generation: 0,
        }
    }
}

/// One schedulable opaque continuation and its physical compatibility.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProgramWork {
    pub(crate) handle: ProgramWorkHandle,
    pub(crate) dispatch: DispatchClass,
    pub(crate) pacing: ProgramPacing,
}

/// Closed query action offered to a residual program.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramAction {
    Propose(VariableId),
    Confirm(VariableId),
    Support,
}

/// Structurally uniform request used to construct one action route.
///
/// Bound values are deliberately absent. A route selected for one row schema
/// is valid for every row with that schema for the duration of the solve.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramRequest {
    pub action: ProgramAction,
    pub bound: VariableSet,
}

/// Structural route selected by an immutable program spec for one action.
///
/// Returning a route certifies that typed novelty computes a least fixpoint for
/// that exact request. Quiescence additionally relies on the family exposing a
/// finite reachable novelty domain; RPQ keys are finite graph-value ×
/// program-counter products. Returning `None` leaves the action on the ordinary
/// [`super::Constraint`] protocol.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramRoute {
    /// Variable naming the structural graph-product operator.
    pub variable: VariableId,
}

/// Row block used to construct initial typed work handles.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ProgramSeedBatch<'v> {
    pub request: ProgramRequest,
    pub route: ProgramRoute,
    pub view: RowsView<'v>,
    /// One engine-created activation per parent row.
    pub activations: &'v [ProgramActivation],
}

/// One initial affine work root for a tagged parent row.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProgramSeedWork {
    pub parent: u32,
    pub work: ProgramWork,
    /// Nullable roots may observe an endpoint before their independent work
    /// credit is expanded.
    pub accepted: Option<RawInline>,
}

/// Typed seed call output.
#[doc(hidden)]
#[derive(Default)]
pub struct ProgramSeedEffects {
    pub work: Vec<ProgramSeedWork>,
}

/// One opaque work item plus the immutable parent context owned by its
/// activation. All rows in a cohort share the `view` schema.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ProgramBatch<'v> {
    pub view: RowsView<'v>,
    pub candidate_sets: &'v [Option<&'v [RawInline]>],
    pub activations: &'v [ProgramActivation],
    pub work: &'v [ProgramWork],
    pub limits: &'v [usize],
}

/// Replacement metadata for one input work item.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProgramPage {
    pub examined: usize,
    /// Exact same-lineage continuation and its generic affine disposition.
    pub resume: Option<ProgramResume>,
}

/// Engine-owned continuation disposition requested by a typed receipt.
///
/// `AfterChildren` is a receipt-local structured join: unrelated work in the
/// same activation is never included in its barrier.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum ProgramResume {
    Immediate(ProgramWork),
    AfterChildren(ProgramWork),
    /// Retire this input only after its receipt-local children drain.
    /// No continuation is scheduled at the barrier.
    AfterChildrenDone,
}

/// One novel child admitted by the typed runtime.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ProgramChild {
    pub input: u32,
    pub work: ProgramWork,
    pub accepted: Option<RawInline>,
}

/// Effects returned by one typed cohort call.
///
/// The erased adapter publishes this receipt only after validating every tag
/// and static page law across the complete cohort.
#[doc(hidden)]
#[derive(Default)]
pub struct ProgramBatchEffects {
    /// Exactly one page per input handle, in input order.
    pub pages: Vec<ProgramPage>,
    /// Novel work children, grouped by ascending input tag.
    pub children: Vec<ProgramChild>,
    /// Direct proposal occurrences from source pages. Unlike accepted product
    /// endpoints, order and multiplicity are preserved.
    pub direct: Vec<(u32, RawInline)>,
    /// Candidate observations proved by the program without manufacturing a
    /// continuation node solely to carry the value.
    pub accepted: Vec<(u32, RawInline)>,
    /// Boolean support observations. The unit payload keeps these tags in
    /// the same grouped-effect shape as candidate observations while making
    /// it impossible to smuggle a synthetic candidate witness.
    pub supported: Vec<(u32, ())>,
    /// Family-reported telemetry only. These counters never affect dispatch
    /// or affine replacement.
    pub source_pages: usize,
    pub source_examined: usize,
    pub source_roots: usize,
    pub transition_pages: usize,
    pub transition_examined: usize,
    /// Additional typed family calls normalized behind this one erased
    /// receipt. This is scheduler telemetry only.
    pub(crate) normalized_steps: usize,
    pub(crate) source_cohorts: usize,
    pub(crate) max_source_cohort: usize,
    pub(crate) transition_cohorts: usize,
    pub(crate) max_transition_cohort: usize,
    pub(crate) final_source_telemetry_cohort: bool,
}

impl ProgramBatchEffects {
    pub(crate) fn clear(&mut self) {
        self.pages.clear();
        self.children.clear();
        self.direct.clear();
        self.accepted.clear();
        self.supported.clear();
        self.source_pages = 0;
        self.source_examined = 0;
        self.source_roots = 0;
        self.transition_pages = 0;
        self.transition_examined = 0;
        self.normalized_steps = 0;
        self.source_cohorts = 0;
        self.max_source_cohort = 0;
        self.transition_cohorts = 0;
        self.max_transition_cohort = 0;
        self.final_source_telemetry_cohort = false;
    }
}

struct TypedSeedWork<State, NoveltyKey> {
    parent: u32,
    state: State,
    novelty: Option<NoveltyKey>,
    accepted: Option<RawInline>,
}

/// Typed initial-state sink. Program families cannot allocate engine handles.
#[doc(hidden)]
pub struct TypedSeedSink<State, NoveltyKey> {
    work: Vec<TypedSeedWork<State, NoveltyKey>>,
}

impl<State, NoveltyKey> Default for TypedSeedSink<State, NoveltyKey> {
    fn default() -> Self {
        Self { work: Vec::new() }
    }
}

impl<State, NoveltyKey> TypedSeedSink<State, NoveltyKey> {
    pub fn finite_root(&mut self, parent: u32, state: State, accepted: Option<RawInline>) {
        self.work.push(TypedSeedWork {
            parent,
            state,
            novelty: None,
            accepted,
        });
    }

    pub fn fixpoint_root(
        &mut self,
        parent: u32,
        state: State,
        novelty: NoveltyKey,
        accepted: Option<RawInline>,
    ) {
        self.work.push(TypedSeedWork {
            parent,
            state,
            novelty: Some(novelty),
            accepted,
        });
    }
}

/// Handle-free context passed to one typed cohort call.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct TypedProgramBatch<'v> {
    pub view: RowsView<'v>,
    pub candidate_sets: &'v [Option<&'v [RawInline]>],
    pub activations: &'v [ProgramActivation],
    pub limits: &'v [usize],
}

/// Typed exact continuation disposition.
#[doc(hidden)]
pub enum TypedResume<State> {
    Immediate(State),
    AfterChildren(State),
    /// Retire the input after its children drain without scheduling another
    /// family state. This closes a final pageable scope without manufacturing
    /// a zero-work sentinel continuation.
    AfterChildrenDone,
}

struct TypedPage<State> {
    examined: usize,
    resume: Option<TypedResume<State>>,
}

struct TypedChild<State, NoveltyKey> {
    input: u32,
    state: State,
    novelty: Option<NoveltyKey>,
    accepted: Option<RawInline>,
}

/// Typed effect sink. Novelty admission and handle allocation happen only in
/// the blanket erased adapter after the family call returns.
#[doc(hidden)]
pub struct TypedEffectSink<State, NoveltyKey> {
    pages: Vec<TypedPage<State>>,
    children: Vec<TypedChild<State, NoveltyKey>>,
    direct: Vec<(u32, RawInline)>,
    accepted: Vec<(u32, RawInline)>,
    supported: Vec<(u32, ())>,
    source_pages: usize,
    source_examined: usize,
    source_roots: usize,
    transition_pages: usize,
    transition_examined: usize,
}

impl<State, NoveltyKey> Default for TypedEffectSink<State, NoveltyKey> {
    fn default() -> Self {
        Self {
            pages: Vec::new(),
            children: Vec::new(),
            direct: Vec::new(),
            accepted: Vec::new(),
            supported: Vec::new(),
            source_pages: 0,
            source_examined: 0,
            source_roots: 0,
            transition_pages: 0,
            transition_examined: 0,
        }
    }
}

impl<State, NoveltyKey> TypedEffectSink<State, NoveltyKey> {
    fn clear(&mut self) {
        self.pages.clear();
        self.children.clear();
        self.direct.clear();
        self.accepted.clear();
        self.supported.clear();
        self.source_pages = 0;
        self.source_examined = 0;
        self.source_roots = 0;
        self.transition_pages = 0;
        self.transition_examined = 0;
    }

    /// Reserves family-known child capacity without exposing the private
    /// effect representation or committing any receipt prefix.
    pub fn reserve_children(&mut self, additional: usize) {
        self.children.reserve(additional);
    }

    pub fn page(&mut self, examined: usize, resume: Option<TypedResume<State>>) {
        self.pages.push(TypedPage { examined, resume });
    }

    pub fn finite_child(&mut self, input: u32, state: State, accepted: Option<RawInline>) {
        self.children.push(TypedChild {
            input,
            state,
            novelty: None,
            accepted,
        });
    }

    pub fn fixpoint_child(
        &mut self,
        input: u32,
        state: State,
        novelty: NoveltyKey,
        accepted: Option<RawInline>,
    ) {
        self.children.push(TypedChild {
            input,
            state,
            novelty: Some(novelty),
            accepted,
        });
    }

    pub fn direct(&mut self, input: u32, value: RawInline) {
        self.direct.push((input, value));
    }

    /// Records one candidate value proved by this input page.
    pub fn accept(&mut self, input: u32, value: RawInline) {
        self.accepted.push((input, value));
    }

    /// Records a typed Boolean support witness for this input page.
    pub fn support(&mut self, input: u32) {
        self.supported.push((input, ()));
    }

    pub fn account_source(&mut self, examined: usize, roots: usize) {
        self.source_pages += 1;
        self.source_examined += examined;
        self.source_roots += roots;
    }

    pub fn account_transition(&mut self, examined: usize) {
        self.transition_pages += 1;
        self.transition_examined += examined;
    }
}

struct ValidatedTypedState<State> {
    state: State,
    dispatch: DispatchClass,
    pacing: ProgramPacing,
}

enum ValidatedTypedResume<State> {
    Immediate(ValidatedTypedState<State>),
    AfterChildren(ValidatedTypedState<State>),
    AfterChildrenDone,
}

struct ValidatedTypedPage<State> {
    examined: usize,
    resume: Option<ValidatedTypedResume<State>>,
}

struct ValidatedTypedChild<State> {
    input: u32,
    state: ValidatedTypedState<State>,
    accepted: Option<RawInline>,
}

struct ValidatedTypedEffects<State> {
    pages: Vec<ValidatedTypedPage<State>>,
    children: Vec<ValidatedTypedChild<State>>,
    direct: Vec<(u32, RawInline)>,
    accepted: Vec<(u32, RawInline)>,
    supported: Vec<(u32, ())>,
    source_pages: usize,
    source_examined: usize,
    source_roots: usize,
    transition_pages: usize,
    transition_examined: usize,
    source_cohorts: usize,
    max_source_cohort: usize,
    transition_cohorts: usize,
    max_transition_cohort: usize,
    final_source_telemetry_cohort: bool,
}

impl<State> Default for ValidatedTypedEffects<State> {
    fn default() -> Self {
        Self {
            pages: Vec::new(),
            children: Vec::new(),
            direct: Vec::new(),
            accepted: Vec::new(),
            supported: Vec::new(),
            source_pages: 0,
            source_examined: 0,
            source_roots: 0,
            transition_pages: 0,
            transition_examined: 0,
            source_cohorts: 0,
            max_source_cohort: 0,
            transition_cohorts: 0,
            max_transition_cohort: 0,
            final_source_telemetry_cohort: false,
        }
    }
}

impl<State> ValidatedTypedEffects<State> {
    fn clear(&mut self) {
        self.pages.clear();
        self.children.clear();
        self.direct.clear();
        self.accepted.clear();
        self.supported.clear();
        self.source_pages = 0;
        self.source_examined = 0;
        self.source_roots = 0;
        self.transition_pages = 0;
        self.transition_examined = 0;
        self.source_cohorts = 0;
        self.max_source_cohort = 0;
        self.transition_cohorts = 0;
        self.max_transition_cohort = 0;
        self.final_source_telemetry_cohort = false;
    }

    fn absorb_observations_and_telemetry(&mut self, other: &mut Self) {
        self.direct.append(&mut other.direct);
        self.accepted.append(&mut other.accepted);
        self.supported.append(&mut other.supported);
        self.source_pages += other.source_pages;
        self.source_examined += other.source_examined;
        self.source_roots += other.source_roots;
        self.transition_pages += other.transition_pages;
        self.transition_examined += other.transition_examined;
        self.source_cohorts += other.source_cohorts;
        self.max_source_cohort = self.max_source_cohort.max(other.max_source_cohort);
        self.transition_cohorts += other.transition_cohorts;
        self.max_transition_cohort = self.max_transition_cohort.max(other.max_transition_cohort);
        self.final_source_telemetry_cohort = other.final_source_telemetry_cohort;
        other.source_pages = 0;
        other.source_examined = 0;
        other.source_roots = 0;
        other.transition_pages = 0;
        other.transition_examined = 0;
        other.source_cohorts = 0;
        other.max_source_cohort = 0;
        other.transition_cohorts = 0;
        other.max_transition_cohort = 0;
        other.final_source_telemetry_cohort = false;
    }
}

enum TypedFrontierDisposition {
    Immediate,
    Child { accepted: Option<RawInline> },
}

struct TypedFrontierState<State> {
    state: ValidatedTypedState<State>,
    disposition: TypedFrontierDisposition,
}

fn escape_typed_frontier<State>(
    frontier: &mut Vec<TypedFrontierState<State>>,
    receipt: &mut ValidatedTypedEffects<State>,
    examined: usize,
) {
    let mut resume = None;
    for frontier_state in frontier.drain(..) {
        match frontier_state.disposition {
            TypedFrontierDisposition::Immediate => {
                assert!(
                    resume.is_none(),
                    "typed morsel manufactured more than one same-lineage resume"
                );
                resume = Some(ValidatedTypedResume::Immediate(frontier_state.state));
            }
            TypedFrontierDisposition::Child { accepted } => {
                receipt.children.push(ValidatedTypedChild {
                    input: 0,
                    state: frontier_state.state,
                    accepted,
                });
            }
        }
    }
    receipt.pages.push(ValidatedTypedPage { examined, resume });
}

/// Family-typed residual program contract.
///
/// Program code can emit only typed states and novelty keys. It cannot create
/// or inspect engine handles, and therefore cannot bypass affine take or
/// novelty admission. Within one activation, equal novelty keys must identify
/// states with the same possible future outputs; otherwise admission order
/// would change the relation produced by the Program.
///
/// Route selection ends at seeding: every distinction that can change future
/// computation must be lowered into [`TypedProgramSpec::State`]. The scheduler
/// may keep one runtime for several routes of the same structural occurrence
/// and variable, and may co-batch their states whenever the state-derived
/// dispatch metadata and physical input shape are compatible. A route cannot
/// rely on a separate runtime partition to carry hidden semantics.
#[doc(hidden)]
pub trait TypedProgramSpec {
    type State: Clone + Send + 'static;
    type NoveltyKey: Clone + Eq + Hash + Send + 'static;
    /// Family-owned finite-domain measure for non-recurrent edges.
    ///
    /// Every resume and every child without a novelty key must strictly
    /// decrease this rank. Novelty-admitted fixpoint roots and children may
    /// enter at any rank, but their later finite pagination must decrease.
    type Rank: Ord + Send + 'static;

    /// Selects one structural action route.
    ///
    /// A selected `Confirm` route owns the complete SET-admitted candidate
    /// relation for each parent in one activation. Ordinary
    /// [`Constraint`](super::Constraint) confirmation remains independently
    /// pageable; selecting a typed Program is the structural request for
    /// activation-local recurrence and reuse.
    ///
    /// The residual planner discovers confirmation routes by probing with
    /// every other variable owned by the exposing Constraint bound. Once that
    /// family-local schema is present, route presence must be invariant under
    /// adding bound variables outside the exposing
    /// [`Constraint::variables`](super::Constraint::variables) set. Runtime
    /// checks enforce this contract before any candidate page enters a typed
    /// activation.
    ///
    /// The returned route supplies planning and seed metadata, not an enduring
    /// runtime identity.
    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute>;

    /// Certifies that the selected exact Confirm Program physically dominates
    /// its paired fully-bound Support Program for first-positive publication.
    ///
    /// For the Confirm activation's immutable ordered candidate bag `B`, the
    /// certificate is consumed only for `B[0]`, after a real exact Program
    /// replacement spends its affine credit. Returning `true` promises more
    /// than Boolean equivalence:
    ///
    /// - every incremental `accepted(B[0])` Confirm receipt implies that the
    ///   paired Support route is true for the row with `B[0]` bound; and
    /// - for every corresponding cumulative examined-work grant `w`, if the
    ///   Support route could report its first positive receipt within `w`,
    ///   exact Confirm reports `accepted(B[0])` within at most `w`.
    ///
    /// This is a performance-elision receipt, not an equality requirement on
    /// either Program's internal state, page boundaries, ordering, or execution
    /// trace. Exact Confirm acceptance is independently authoritative;
    /// returning `true` says only that retaining the competing one-occurrence
    /// Support feeder cannot improve its first-positive latency. The default is
    /// deliberately conservative: matching route shapes or Boolean
    /// denotations alone do not prove physical dominance.
    fn certifies_confirm_dominates_support_positive_prefix(
        &self,
        _confirm_request: ProgramRequest,
        _confirm_route: ProgramRoute,
        _support_request: ProgramRequest,
        _support_route: ProgramRoute,
    ) -> bool {
        false
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass;

    /// Selects the physical budget source for this continuation.
    ///
    /// This must be a pure function of the canonical typed state. The erased
    /// [`ProgramWork`] copy is only a scheduler cache: the adapter rederives
    /// and validates it when the affine handle is taken.
    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Activation
    }

    /// Returns the well-founded finite-spine measure for an exact state.
    fn progress(&self, state: &Self::State) -> Self::Rank;

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    );

    /// Executes one cohort against affinely taken typed inputs.
    ///
    /// Implementations may drain or otherwise move states when convenient,
    /// but they may also borrow them to construct a complete receipt. The
    /// adapter discards any states left in the vector after this call while
    /// retaining its allocation for the next cohort.
    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    );
}

trait ErasedProgramRuntime: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn clone_box(&self) -> Box<dyn ErasedProgramRuntime>;
}

impl<T> ErasedProgramRuntime for T
where
    T: Any + Clone + Send,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn ErasedProgramRuntime> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ErasedProgramRuntime> {
    fn clone(&self) -> Self {
        // Dispatch through the stored runtime, not the blanket implementation
        // for `Box<dyn ErasedProgramRuntime>` itself. The latter would wrap the
        // trait object in another erased box and lose the concrete downcast.
        (**self).clone_box()
    }
}

/// Cloneable query-local runtime with private type erasure.
#[doc(hidden)]
#[derive(Clone)]
pub struct ProgramRuntime {
    erased: Box<dyn ErasedProgramRuntime>,
    family: TypeId,
    family_name: &'static str,
}

/// Immutable residual-program family specification.
///
/// Implementations downcast `runtime` once at the beginning of each seed or
/// step call, then operate on a dense typed state vector. Returning `None`
/// from `route` declines only that exact structural action, so the ordinary
/// constraint protocol remains eligible. After a route is returned, however,
/// that action is owned by the Program and cannot fall back to a second
/// residual execution path.
trait ErasedProgramSpec {
    fn new_runtime(&self) -> ProgramRuntime;

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute>;

    fn certifies_confirm_dominates_support_positive_prefix(
        &self,
        confirm_request: ProgramRequest,
        confirm_route: ProgramRoute,
        support_request: ProgramRequest,
        support_route: ProgramRoute,
    ) -> bool;

    fn seed_batch(
        &self,
        runtime: &mut ProgramRuntime,
        batch: ProgramSeedBatch<'_>,
        effects: &mut ProgramSeedEffects,
    );

    fn step_batch(
        &self,
        runtime: &mut ProgramRuntime,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    );

    fn discard_work(
        &self,
        runtime: &mut ProgramRuntime,
        activation: ProgramActivation,
        work: &ProgramWork,
    );

    fn retire_activations(&self, runtime: &mut ProgramRuntime, activations: &[ProgramActivation]);
}

/// Borrowed immutable typed program behind a private erased vtable.
///
/// Construction accepts one [`TypedProgramSpec`] and does not let custom
/// constraints bypass typed sinks, activation ownership, or novelty admission.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ProgramRef<'a> {
    erased: &'a dyn ErasedProgramSpec,
}

impl<'a> ProgramRef<'a> {
    pub fn new<T: TypedProgramSpec>(spec: &'a T) -> Self {
        Self { erased: spec }
    }

    pub(crate) fn new_runtime(self) -> ProgramRuntime {
        self.erased.new_runtime()
    }

    pub(crate) fn route(self, request: ProgramRequest) -> Option<ProgramRoute> {
        self.erased.route(request)
    }

    pub(crate) fn same_implementation(self, other: Self) -> bool {
        std::ptr::eq(self.erased, other.erased)
    }

    pub(crate) fn certifies_confirm_dominates_support_positive_prefix(
        self,
        confirm_request: ProgramRequest,
        confirm_route: ProgramRoute,
        support_request: ProgramRequest,
        support_route: ProgramRoute,
    ) -> bool {
        self.erased
            .certifies_confirm_dominates_support_positive_prefix(
                confirm_request,
                confirm_route,
                support_request,
                support_route,
            )
    }

    pub(crate) fn seed_batch(
        self,
        runtime: &mut ProgramRuntime,
        batch: ProgramSeedBatch<'_>,
        effects: &mut ProgramSeedEffects,
    ) {
        self.erased.seed_batch(runtime, batch, effects);
    }

    pub(crate) fn step_batch(
        self,
        runtime: &mut ProgramRuntime,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    ) {
        self.erased.step_batch(runtime, batch, effects);
    }

    /// Affinely discards typed work that policy declines before execution.
    ///
    /// This consumes only the opaque typed handle. A caller that already
    /// allocated a producer credit for the work must consume that separate
    /// affine authority in the same scheduler transaction.
    pub(crate) fn discard_work(
        self,
        runtime: &mut ProgramRuntime,
        activation: ProgramActivation,
        work: &ProgramWork,
    ) {
        self.erased.discard_work(runtime, activation, work);
    }

    pub(crate) fn retire_activations(
        self,
        runtime: &mut ProgramRuntime,
        activations: &[ProgramActivation],
    ) {
        self.erased.retire_activations(runtime, activations);
    }
}

#[derive(Clone)]
struct ArenaSlot<T> {
    generation: u32,
    value: Option<(ProgramActivation, T)>,
}

/// Query-local typed state and novelty storage for one program occurrence.
///
/// `State` is deliberately not constrained by equality or hashing. Only the
/// smaller family-defined `NoveltyKey` participates in per-activation
/// admission.
struct TypedProgramScratch<State, NoveltyKey, Rank> {
    states: Vec<State>,
    input_ranks: Vec<Rank>,
    effects: TypedEffectSink<State, NoveltyKey>,
    layer: ValidatedTypedEffects<State>,
    morsel: ValidatedTypedEffects<State>,
    frontier: Vec<TypedFrontierState<State>>,
    examined: Vec<usize>,
    raw_effects: Vec<usize>,
    resume_physical: Vec<Option<(DispatchClass, ProgramPacing)>>,
    batch_novelty: AHashMap<(ProgramActivation, NoveltyKey), Option<RawInline>>,
    child_admitted: Vec<bool>,
    child_physical: Vec<(DispatchClass, ProgramPacing)>,
}

impl<State, NoveltyKey, Rank> Default for TypedProgramScratch<State, NoveltyKey, Rank> {
    fn default() -> Self {
        Self {
            states: Vec::new(),
            input_ranks: Vec::new(),
            effects: TypedEffectSink::default(),
            layer: ValidatedTypedEffects::default(),
            morsel: ValidatedTypedEffects::default(),
            frontier: Vec::new(),
            examined: Vec::new(),
            raw_effects: Vec::new(),
            resume_physical: Vec::new(),
            batch_novelty: AHashMap::new(),
            child_admitted: Vec::new(),
            child_physical: Vec::new(),
        }
    }
}

impl<State, NoveltyKey, Rank> TypedProgramScratch<State, NoveltyKey, Rank>
where
    State: Clone + Send + 'static,
    NoveltyKey: Clone + Eq + Hash + Send + 'static,
    Rank: Ord + Send + 'static,
{
    /// Validates one complete family call into a handle-free typed receipt.
    ///
    /// Novelty admission is committed only after every receipt law in this
    /// family call validates. Later typed layers observe that committed
    /// admission, while outward effects and arena handles remain buffered until
    /// the complete morsel escapes.
    fn validate_layer<T>(
        &mut self,
        spec: &T,
        runtime: &mut TypedProgramRuntime<State, NoveltyKey, Rank>,
        activations: &[ProgramActivation],
        limits: &[usize],
    ) where
        T: TypedProgramSpec<State = State, NoveltyKey = NoveltyKey, Rank = Rank>,
    {
        let input_count = self.input_ranks.len();
        assert_eq!(activations.len(), input_count);
        assert_eq!(limits.len(), input_count);
        self.batch_novelty.clear();
        let typed = &mut self.effects;
        assert_eq!(
            typed.pages.len(),
            input_count,
            "typed program returned the wrong page count"
        );

        self.examined.clear();
        self.examined
            .extend(typed.pages.iter().map(|page| page.examined));
        assert!(
            self.examined
                .iter()
                .zip(limits)
                .all(|(&spent, &limit)| spent <= limit),
            "typed program exceeded one input's physical work budget"
        );
        self.raw_effects.clear();
        self.raw_effects.resize(input_count, 0);

        self.resume_physical.clear();
        self.resume_physical.reserve(input_count);
        for (input, page) in typed.pages.iter().enumerate() {
            match &page.resume {
                Some(TypedResume::Immediate(state) | TypedResume::AfterChildren(state)) => {
                    assert!(
                        spec.progress(state) < self.input_ranks[input],
                        "typed program resume did not strictly decrease its finite rank"
                    );
                    self.resume_physical
                        .push(Some((spec.dispatch(state), spec.pacing(state))));
                }
                Some(TypedResume::AfterChildrenDone) | None => {
                    self.resume_physical.push(None);
                }
            }
        }

        // This bitmap and the novelty map form a transaction plan. Repeated
        // keys consult the local plan first; the runtime remains immutable
        // until every traversed layer has validated.
        self.child_admitted.clear();
        self.child_admitted.reserve(typed.children.len());
        self.child_physical.clear();
        self.child_physical.reserve(typed.children.len());
        let mut previous = 0u32;
        for (position, child) in typed.children.iter().enumerate() {
            assert!(
                (child.input as usize) < input_count,
                "typed program child tag is out of range"
            );
            assert!(
                position == 0 || child.input >= previous,
                "typed program child tags are not grouped in ascending order"
            );
            previous = child.input;
            self.raw_effects[child.input as usize] += 1;
            if child.novelty.is_none() {
                assert!(
                    spec.progress(&child.state) < self.input_ranks[child.input as usize],
                    "typed program finite child did not strictly decrease its input rank"
                );
            }
            self.child_physical
                .push((spec.dispatch(&child.state), spec.pacing(&child.state)));

            let admitted = if let Some(novelty) = child.novelty.as_ref() {
                let activation = activations[child.input as usize];
                match self.batch_novelty.entry((activation, novelty.clone())) {
                    Entry::Occupied(previous) => {
                        assert_eq!(
                            *previous.get(),
                            child.accepted,
                            "one typed novelty key changed its endpoint observation"
                        );
                        false
                    }
                    Entry::Vacant(first) => {
                        let admitted = match runtime
                            .novelty
                            .get(&activation)
                            .and_then(|seen| seen.get(novelty))
                        {
                            Some(previous) => {
                                assert_eq!(
                                    *previous, child.accepted,
                                    "one typed novelty key changed its endpoint observation"
                                );
                                false
                            }
                            None => true,
                        };
                        first.insert(child.accepted);
                        admitted
                    }
                }
            } else {
                true
            };
            self.child_admitted.push(admitted);
        }

        let mut previous = 0u32;
        for (position, (input, _)) in typed.direct.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed direct observations are not grouped in ascending order"
            );
            previous = *input;
            self.raw_effects[*input as usize] += 1;
        }
        let mut previous = 0u32;
        for (position, (input, _)) in typed.accepted.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed candidate observations are not grouped in ascending order"
            );
            previous = *input;
            self.raw_effects[*input as usize] += 1;
        }
        let mut previous = 0u32;
        for (position, (input, ())) in typed.supported.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed support observations are not grouped in ascending order"
            );
            assert!(
                position == 0 || *input != previous,
                "one typed input page reported Boolean support more than once"
            );
            previous = *input;
            self.raw_effects[*input as usize] += 1;
        }
        assert!(
            self.raw_effects
                .iter()
                .zip(&self.examined)
                .all(|(&outputs, &spent)| outputs <= spent),
            "typed program emitted more raw effects than its examined-work receipt"
        );
        assert!(
            typed
                .pages
                .iter()
                .all(|page| page.examined > 0 || page.resume.is_none()),
            "typed program scheduled zero-examined continuation work without a positive work receipt"
        );

        // Only after the complete layer validates do we move family-owned
        // states into the private transaction receipt.
        self.layer.clear();
        self.layer.pages.extend(
            typed
                .pages
                .drain(..)
                .zip(self.resume_physical.drain(..))
                .map(|(page, physical)| {
                    let resume = match (page.resume, physical) {
                        (Some(TypedResume::Immediate(state)), Some((dispatch, pacing))) => {
                            Some(ValidatedTypedResume::Immediate(ValidatedTypedState {
                                state,
                                dispatch,
                                pacing,
                            }))
                        }
                        (Some(TypedResume::AfterChildren(state)), Some((dispatch, pacing))) => {
                            Some(ValidatedTypedResume::AfterChildren(ValidatedTypedState {
                                state,
                                dispatch,
                                pacing,
                            }))
                        }
                        (Some(TypedResume::AfterChildrenDone), None) => {
                            Some(ValidatedTypedResume::AfterChildrenDone)
                        }
                        (None, None) => None,
                        _ => unreachable!("typed Program resume preflight lost alignment"),
                    };
                    ValidatedTypedPage {
                        examined: page.examined,
                        resume,
                    }
                }),
        );
        for ((child, (dispatch, pacing)), admitted) in typed
            .children
            .drain(..)
            .zip(self.child_physical.drain(..))
            .zip(self.child_admitted.drain(..))
        {
            if admitted {
                if let Some(novelty) = child.novelty {
                    let activation = activations[child.input as usize];
                    let previous = runtime
                        .novelty
                        .entry(activation)
                        .or_default()
                        .insert(novelty, child.accepted);
                    assert!(
                        previous.is_none(),
                        "typed novelty preflight admitted an existing key"
                    );
                }
                self.layer.children.push(ValidatedTypedChild {
                    input: child.input,
                    state: ValidatedTypedState {
                        state: child.state,
                        dispatch,
                        pacing,
                    },
                    accepted: child.accepted,
                });
            }
        }
        self.layer.direct.append(&mut typed.direct);
        self.layer.accepted.append(&mut typed.accepted);
        self.layer.supported.append(&mut typed.supported);
        self.layer.source_pages = typed.source_pages;
        self.layer.source_examined = typed.source_examined;
        self.layer.source_roots = typed.source_roots;
        self.layer.transition_pages = typed.transition_pages;
        self.layer.transition_examined = typed.transition_examined;
        self.layer.source_cohorts = usize::from(typed.source_pages > 0);
        self.layer.max_source_cohort = typed.source_pages;
        self.layer.transition_cohorts = usize::from(typed.transition_pages > 0);
        self.layer.max_transition_cohort = typed.transition_pages;
        self.layer.final_source_telemetry_cohort =
            typed.source_pages > 0 && typed.transition_pages == 0;
        typed.clear();
    }
}

struct TypedProgramRuntime<State, NoveltyKey, Rank> {
    slots: Vec<ArenaSlot<State>>,
    free: Vec<u32>,
    novelty: AHashMap<ProgramActivation, AHashMap<NoveltyKey, Option<RawInline>>>,
    /// Lazily allocated so dormant program occurrences pay only one pointer.
    /// Clones deliberately start cold rather than retaining cohort-sized
    /// buffers from the source query.
    scratch: Option<Box<TypedProgramScratch<State, NoveltyKey, Rank>>>,
    #[cfg(test)]
    retirement_slot_probes: usize,
    #[cfg(test)]
    retirement_membership_builds: usize,
}

impl<State, NoveltyKey, Rank> Clone for TypedProgramRuntime<State, NoveltyKey, Rank>
where
    State: Clone,
    NoveltyKey: Clone + Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            slots: self.slots.clone(),
            free: self.free.clone(),
            novelty: self.novelty.clone(),
            scratch: None,
            #[cfg(test)]
            retirement_slot_probes: self.retirement_slot_probes,
            #[cfg(test)]
            retirement_membership_builds: self.retirement_membership_builds,
        }
    }
}

impl<State, NoveltyKey, Rank> Default for TypedProgramRuntime<State, NoveltyKey, Rank> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            novelty: AHashMap::new(),
            scratch: None,
            #[cfg(test)]
            retirement_slot_probes: 0,
            #[cfg(test)]
            retirement_membership_builds: 0,
        }
    }
}

impl<State, NoveltyKey, Rank> TypedProgramRuntime<State, NoveltyKey, Rank>
where
    State: Clone + Send + 'static,
    NoveltyKey: Clone + Eq + Hash + Send + 'static,
{
    fn insert(&mut self, activation: ProgramActivation, state: State) -> ProgramWorkHandle {
        if let Some(slot) = self.free.pop() {
            let record = &mut self.slots[slot as usize];
            assert!(
                record.value.is_none(),
                "program free list named a live slot"
            );
            record.value = Some((activation, state));
            ProgramWorkHandle {
                slot,
                generation: record.generation,
            }
        } else {
            let slot = u32::try_from(self.slots.len()).expect("program work arena exhausted");
            self.slots.push(ArenaSlot {
                generation: 0,
                value: Some((activation, state)),
            });
            ProgramWorkHandle {
                slot,
                generation: 0,
            }
        }
    }

    /// Affinely takes one continuation. A copied or replayed handle is stale.
    fn take(&mut self, activation: ProgramActivation, handle: ProgramWorkHandle) -> State {
        let record = self
            .slots
            .get_mut(handle.slot as usize)
            .expect("program work handle named an unknown slot");
        assert_eq!(
            record.generation, handle.generation,
            "stale program work handle generation"
        );
        let owner = record
            .value
            .as_ref()
            .map(|(owner, _)| *owner)
            .expect("program work handle was replayed after affine take");
        assert_eq!(
            owner, activation,
            "program work handle crossed activation ownership"
        );
        let (_, value) = record
            .value
            .take()
            .expect("validated program work handle disappeared");
        record.generation = record
            .generation
            .checked_add(1)
            .expect("program work generation exhausted");
        self.free.push(handle.slot);
        value
    }

    /// Takes a cohort into one dense typed vector in scheduler order.
    fn take_batch_into(
        &mut self,
        activations: &[ProgramActivation],
        handles: &[ProgramWork],
        states: &mut Vec<State>,
    ) {
        assert_eq!(activations.len(), handles.len());
        states.clear();
        states.reserve(activations.len());
        for (&activation, work) in activations.iter().zip(handles) {
            states.push(self.take(activation, work.handle.clone()));
        }
    }

    fn discard(&mut self, activation: ProgramActivation, work: &ProgramWork) {
        drop(self.take(activation, work.handle.clone()));
    }

    /// Admits one typed novelty key for an activation.
    ///
    /// The attached Boolean is the key's endpoint observation and must remain
    /// stable if another exact state maps to the same novelty key.
    fn admit(
        &mut self,
        activation: ProgramActivation,
        key: NoveltyKey,
        accepted: Option<RawInline>,
    ) -> bool {
        let seen = self.novelty.entry(activation).or_default();
        if let Some(previous) = seen.get(&key) {
            assert_eq!(
                *previous, accepted,
                "one typed novelty key changed its endpoint observation"
            );
            false
        } else {
            seen.insert(key, accepted);
            true
        }
    }

    fn publish_validated(
        &mut self,
        activations: &[ProgramActivation],
        receipt: &mut ValidatedTypedEffects<State>,
        normalized_steps: usize,
        effects: &mut ProgramBatchEffects,
    ) {
        effects
            .pages
            .extend(receipt.pages.drain(..).enumerate().map(|(input, page)| {
                let activation = activations[input];
                let resume = match page.resume {
                    Some(ValidatedTypedResume::Immediate(state)) => {
                        Some(ProgramResume::Immediate(ProgramWork {
                            handle: self.insert(activation, state.state),
                            dispatch: state.dispatch,
                            pacing: state.pacing,
                        }))
                    }
                    Some(ValidatedTypedResume::AfterChildren(state)) => {
                        Some(ProgramResume::AfterChildren(ProgramWork {
                            handle: self.insert(activation, state.state),
                            dispatch: state.dispatch,
                            pacing: state.pacing,
                        }))
                    }
                    Some(ValidatedTypedResume::AfterChildrenDone) => {
                        Some(ProgramResume::AfterChildrenDone)
                    }
                    None => None,
                };
                ProgramPage {
                    examined: page.examined,
                    resume,
                }
            }));
        for child in receipt.children.drain(..) {
            let activation = activations[child.input as usize];
            effects.children.push(ProgramChild {
                input: child.input,
                work: ProgramWork {
                    handle: self.insert(activation, child.state.state),
                    dispatch: child.state.dispatch,
                    pacing: child.state.pacing,
                },
                accepted: child.accepted,
            });
        }
        effects.direct.append(&mut receipt.direct);
        effects.accepted.append(&mut receipt.accepted);
        effects.supported.append(&mut receipt.supported);
        effects.source_pages += receipt.source_pages;
        effects.source_examined += receipt.source_examined;
        effects.source_roots += receipt.source_roots;
        effects.transition_pages += receipt.transition_pages;
        effects.transition_examined += receipt.transition_examined;
        effects.normalized_steps += normalized_steps;
        effects.source_cohorts += receipt.source_cohorts;
        effects.max_source_cohort = effects.max_source_cohort.max(receipt.max_source_cohort);
        effects.transition_cohorts += receipt.transition_cohorts;
        effects.max_transition_cohort = effects
            .max_transition_cohort
            .max(receipt.max_transition_cohort);
        effects.final_source_telemetry_cohort = receipt.final_source_telemetry_cohort;
        receipt.clear();
    }

    /// Atomically retires a cohort after at most one arena ownership pass.
    ///
    /// Empty and singleton receipts preserve the old allocation-free scalar
    /// path. Wider cohorts build membership once, changing the ownership check
    /// from `O(activations * slots)` to `O(activations + slots)` without adding
    /// bookkeeping to continuation insertion or affine take.
    fn retire_activations(&mut self, activations: &[ProgramActivation]) {
        if activations.is_empty() {
            return;
        }
        if self.free.len() == self.slots.len() {
            for activation in activations {
                self.novelty.remove(activation);
            }
            return;
        }

        #[cfg(test)]
        let mut slot_probes = 0usize;
        let live_owner = if let [activation] = activations {
            self.slots.iter().find_map(|slot| {
                #[cfg(test)]
                {
                    slot_probes += 1;
                }
                slot.value
                    .as_ref()
                    .map(|(owner, _)| *owner)
                    .filter(|owner| owner == activation)
            })
        } else {
            #[cfg(test)]
            {
                self.retirement_membership_builds += 1;
            }
            let retiring: AHashSet<_> = activations.iter().copied().collect();
            self.slots.iter().find_map(|slot| {
                #[cfg(test)]
                {
                    slot_probes += 1;
                }
                slot.value
                    .as_ref()
                    .map(|(owner, _)| *owner)
                    .filter(|owner| retiring.contains(owner))
            })
        };
        #[cfg(test)]
        {
            self.retirement_slot_probes += slot_probes;
        }
        assert!(
            live_owner.is_none(),
            "program activation retired while a live state handle remained"
        );
        for activation in activations {
            self.novelty.remove(activation);
        }
    }

    #[cfg(test)]
    fn contains(&self, handle: &ProgramWorkHandle) -> bool {
        self.slots
            .get(handle.slot as usize)
            .is_some_and(|slot| slot.generation == handle.generation && slot.value.is_some())
    }
}

/// Opens one engine-owned typed continuation in an existing private runtime.
///
/// Ordinary constraints can create states only through [`TypedSeedSink`] and
/// [`TypedEffectSink`].  The residual engine additionally needs to transfer a
/// closed affine reducer into one of its own finite Program states without
/// pretending that the enclosing constraint seeded a second activation.  This
/// crate-private seam preserves the same typed arena, handle generation,
/// dispatch, and pacing checks while keeping that transfer unavailable to
/// public [`TypedProgramSpec`] implementations.
pub(crate) fn insert_engine_program_state<T>(
    spec: &T,
    runtime: &mut ProgramRuntime,
    activation: ProgramActivation,
    state: T::State,
) -> ProgramWork
where
    T: TypedProgramSpec,
{
    assert_eq!(
        runtime.family,
        TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
        "engine Program state expected family {}, received {}",
        type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
        runtime.family_name
    );
    let dispatch = spec.dispatch(&state);
    let pacing = spec.pacing(&state);
    let runtime = runtime
        .erased
        .as_mut()
        .as_any_mut()
        .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>()
        .expect("engine Program state received another family's runtime");
    let handle = runtime.insert(activation, state);
    ProgramWork {
        handle,
        dispatch,
        pacing,
    }
}

impl<T> ErasedProgramSpec for T
where
    T: TypedProgramSpec,
{
    fn new_runtime(&self) -> ProgramRuntime {
        ProgramRuntime {
            erased: Box::new(TypedProgramRuntime::<T::State, T::NoveltyKey, T::Rank>::default()),
            family: TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            family_name: type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
        }
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        TypedProgramSpec::route(self, request)
    }

    fn certifies_confirm_dominates_support_positive_prefix(
        &self,
        confirm_request: ProgramRequest,
        confirm_route: ProgramRoute,
        support_request: ProgramRequest,
        support_route: ProgramRoute,
    ) -> bool {
        TypedProgramSpec::certifies_confirm_dominates_support_positive_prefix(
            self,
            confirm_request,
            confirm_route,
            support_request,
            support_route,
        )
    }

    fn seed_batch(
        &self,
        runtime: &mut ProgramRuntime,
        batch: ProgramSeedBatch<'_>,
        effects: &mut ProgramSeedEffects,
    ) {
        assert_eq!(batch.activations.len(), batch.view.len());
        assert_eq!(
            runtime.family,
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            "residual program seed expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>()
            .expect("residual program seed received another family's runtime");
        let mut typed = TypedSeedSink::default();
        self.seed_typed(batch, &mut typed);

        let mut previous = 0u32;
        for (position, seed) in typed.work.into_iter().enumerate() {
            assert!(
                (seed.parent as usize) < batch.view.len(),
                "typed program seed parent tag is out of range"
            );
            assert!(
                position == 0 || seed.parent > previous,
                "typed program seed emitted more than one unbudgeted root for a parent"
            );
            previous = seed.parent;
            let activation = batch.activations[seed.parent as usize];
            if let Some(novelty) = seed.novelty {
                if !runtime.admit(activation, novelty, seed.accepted) {
                    continue;
                }
            }
            let dispatch = self.dispatch(&seed.state);
            let pacing = self.pacing(&seed.state);
            let handle = runtime.insert(activation, seed.state);
            effects.work.push(ProgramSeedWork {
                parent: seed.parent,
                work: ProgramWork {
                    handle,
                    dispatch,
                    pacing,
                },
                accepted: seed.accepted,
            });
        }
    }

    fn step_batch(
        &self,
        runtime: &mut ProgramRuntime,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    ) {
        let input_count = batch.work.len();
        assert_eq!(batch.view.len(), input_count);
        assert_eq!(batch.candidate_sets.len(), input_count);
        assert_eq!(batch.activations.len(), input_count);
        assert_eq!(batch.limits.len(), input_count);
        assert!(batch.limits.iter().all(|&limit| limit > 0));

        // This is the cohort's sole erased downcast. Every item is then taken
        // affinely into one dense family-typed vector.
        assert_eq!(
            runtime.family,
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            "residual program step expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>()
            .expect("residual program step received another family's runtime");
        let mut scratch = runtime
            .scratch
            .take()
            .unwrap_or_else(|| Box::new(TypedProgramScratch::default()));
        runtime.take_batch_into(batch.activations, batch.work, &mut scratch.states);
        scratch.input_ranks.clear();
        scratch
            .input_ranks
            .extend(scratch.states.iter().map(|state| self.progress(state)));
        for (state, work) in scratch.states.iter().zip(batch.work) {
            assert_eq!(
                self.dispatch(state),
                work.dispatch,
                "typed program work entered an incompatible dispatch cohort"
            );
            assert_eq!(
                self.pacing(state),
                work.pacing,
                "typed program work entered an incompatible pacing cohort"
            );
        }

        scratch.layer.clear();
        scratch.morsel.clear();
        scratch.frontier.clear();

        let normalized_steps;
        if input_count == 1 {
            // The first adapter-local morsel is deliberately a singleton
            // frontier. A branch can contain independent receipt-local joins;
            // flattening two such lineages into one ProgramPage would widen an
            // AfterChildren barrier. Branches therefore escape unchanged.
            let state = scratch
                .states
                .pop()
                .expect("typed singleton cohort lost its affine state");
            scratch.input_ranks.clear();
            scratch.frontier.push(TypedFrontierState {
                state: ValidatedTypedState {
                    state,
                    dispatch: batch.work[0].dispatch,
                    pacing: batch.work[0].pacing,
                },
                disposition: TypedFrontierDisposition::Immediate,
            });

            let dispatch = batch.work[0].dispatch;
            let pacing = batch.work[0].pacing;
            let limit = batch.limits[0];
            let mut total_examined = 0usize;
            let mut typed_calls = 0usize;
            loop {
                if matches!(
                    &scratch
                        .frontier
                        .last()
                        .expect("typed morsel lost its singleton frontier")
                        .disposition,
                    TypedFrontierDisposition::Child { accepted: Some(_) }
                ) {
                    // An accepted child is already a reducer-visible
                    // publication. Traversing behind it could expose a later
                    // AfterChildren barrier to PositiveSupport even though
                    // the unfused reducer would publish and cancel first.
                    escape_typed_frontier(
                        &mut scratch.frontier,
                        &mut scratch.morsel,
                        total_examined,
                    );
                    break;
                }
                let frontier_state = scratch
                    .frontier
                    .pop()
                    .expect("typed morsel lost its singleton frontier");
                if frontier_state.state.dispatch != dispatch
                    || frontier_state.state.pacing != pacing
                {
                    scratch.frontier.push(frontier_state);
                    escape_typed_frontier(
                        &mut scratch.frontier,
                        &mut scratch.morsel,
                        total_examined,
                    );
                    break;
                }

                scratch.input_ranks.clear();
                scratch
                    .input_ranks
                    .push(self.progress(&frontier_state.state.state));
                scratch.states.clear();
                scratch.states.push(frontier_state.state.state);
                let remaining = limit
                    .checked_sub(total_examined)
                    .expect("typed morsel overspent its input grant");
                assert!(remaining > 0, "typed morsel re-entered an exhausted grant");
                let local_limits = [remaining];
                scratch.effects.clear();
                self.step_typed(
                    &mut scratch.states,
                    TypedProgramBatch {
                        view: batch.view,
                        candidate_sets: batch.candidate_sets,
                        activations: batch.activations,
                        limits: &local_limits,
                    },
                    &mut scratch.effects,
                );
                scratch.states.clear();
                scratch.validate_layer(self, runtime, batch.activations, &local_limits);
                scratch.input_ranks.clear();
                typed_calls += 1;

                let page = scratch
                    .layer
                    .pages
                    .pop()
                    .expect("validated singleton layer lost its page");
                assert!(
                    scratch.layer.pages.is_empty(),
                    "validated singleton layer manufactured extra pages"
                );
                total_examined = total_examined
                    .checked_add(page.examined)
                    .expect("typed morsel examined-work count overflow");
                scratch
                    .morsel
                    .absorb_observations_and_telemetry(&mut scratch.layer);

                match page.resume {
                    Some(
                        resume @ (ValidatedTypedResume::AfterChildren(_)
                        | ValidatedTypedResume::AfterChildrenDone),
                    ) => {
                        scratch.morsel.children.append(&mut scratch.layer.children);
                        scratch.morsel.pages.push(ValidatedTypedPage {
                            examined: total_examined,
                            resume: Some(resume),
                        });
                        break;
                    }
                    Some(ValidatedTypedResume::Immediate(state)) => {
                        for child in scratch.layer.children.drain(..) {
                            scratch.frontier.push(TypedFrontierState {
                                state: child.state,
                                disposition: TypedFrontierDisposition::Child {
                                    accepted: child.accepted,
                                },
                            });
                        }
                        scratch.frontier.push(TypedFrontierState {
                            state,
                            disposition: TypedFrontierDisposition::Immediate,
                        });
                    }
                    None => {
                        for child in scratch.layer.children.drain(..) {
                            scratch.frontier.push(TypedFrontierState {
                                state: child.state,
                                disposition: TypedFrontierDisposition::Child {
                                    accepted: child.accepted,
                                },
                            });
                        }
                    }
                }

                if scratch.frontier.is_empty() {
                    scratch.morsel.pages.push(ValidatedTypedPage {
                        examined: total_examined,
                        resume: None,
                    });
                    break;
                }
                // Outward observations are a publication boundary. This keeps
                // their typed page chronology intact and avoids delaying an
                // already available witness behind more local work.
                if !scratch.morsel.direct.is_empty()
                    || !scratch.morsel.accepted.is_empty()
                    || !scratch.morsel.supported.is_empty()
                    || scratch.frontier.len() != 1
                    || total_examined == limit
                {
                    escape_typed_frontier(
                        &mut scratch.frontier,
                        &mut scratch.morsel,
                        total_examined,
                    );
                    break;
                }
            }
            normalized_steps = typed_calls.saturating_sub(1);
        } else {
            scratch.effects.clear();
            self.step_typed(
                &mut scratch.states,
                TypedProgramBatch {
                    view: batch.view,
                    candidate_sets: batch.candidate_sets,
                    activations: batch.activations,
                    limits: batch.limits,
                },
                &mut scratch.effects,
            );
            scratch.states.clear();
            scratch.validate_layer(self, runtime, batch.activations, batch.limits);
            scratch.input_ranks.clear();
            normalized_steps = 0;
        }

        // Every traversed layer has validated and admitted novelty. Allocate
        // handles only for the final escaped frontier.
        if input_count == 1 {
            runtime.publish_validated(
                batch.activations,
                &mut scratch.morsel,
                normalized_steps,
                effects,
            );
        } else {
            runtime.publish_validated(batch.activations, &mut scratch.layer, 0, effects);
        }
        scratch.effects.clear();
        scratch.layer.clear();
        scratch.morsel.clear();
        scratch.frontier.clear();
        scratch.input_ranks.clear();
        scratch.examined.clear();
        scratch.raw_effects.clear();
        runtime.scratch = Some(scratch);
    }

    fn discard_work(
        &self,
        runtime: &mut ProgramRuntime,
        activation: ProgramActivation,
        work: &ProgramWork,
    ) {
        assert_eq!(
            runtime.family,
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            "residual program discard expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>()
            .expect("residual program discard received another family's runtime");
        runtime.discard(activation, work);
    }

    fn retire_activations(&self, runtime: &mut ProgramRuntime, activations: &[ProgramActivation]) {
        assert_eq!(
            runtime.family,
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            "residual program retirement expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey, T::Rank>>()
            .expect("residual program retirement received another family's runtime");
        runtime.retire_activations(activations);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct NonComparableState {
        exact_cursor: usize,
    }

    #[derive(Clone, Eq, Hash, PartialEq)]
    struct Key(u8);

    struct DenseProbe {
        calls: Arc<Mutex<Vec<Vec<usize>>>>,
    }

    impl TypedProgramSpec for DenseProbe {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(9)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            for parent in 0..batch.view.len() {
                effects.finite_root(
                    parent as u32,
                    NonComparableState {
                        exact_cursor: parent + 10,
                    },
                    None,
                );
            }
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.calls
                .lock()
                .unwrap()
                .push(states.iter().map(|state| state.exact_cursor).collect());
            for _ in states {
                effects.page(1, None);
            }
        }
    }

    struct ScratchReuseProbe;

    impl TypedProgramSpec for ScratchReuseProbe {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(12)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            for parent in 0..batch.view.len() {
                effects.finite_root(
                    parent as u32,
                    NonComparableState {
                        exact_cursor: parent + 10,
                    },
                    None,
                );
            }
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            let rich = states.len() > 1;
            for (input, state) in states.iter().enumerate() {
                effects.page(1, None);
                if rich {
                    match input {
                        0 => effects.finite_child(
                            0,
                            NonComparableState {
                                exact_cursor: state.exact_cursor - 1,
                            },
                            None,
                        ),
                        1 => effects.direct(1, RawInline::default()),
                        2 => effects.accept(2, RawInline::default()),
                        3 => effects.support(3),
                        _ => {}
                    }
                }
            }
            if rich {
                effects.account_source(states.len(), 1);
                effects.account_transition(states.len());
            }
        }
    }

    #[derive(Clone, Copy)]
    enum NoveltyBatchMode {
        Stable,
        ExistingConflict,
        LocalConflict,
    }

    struct NoveltyBatchProbe {
        mode: NoveltyBatchMode,
    }

    impl TypedProgramSpec for NoveltyBatchProbe {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(13)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            effects.fixpoint_root(0, NonComparableState { exact_cursor: 10 }, Key(1), None);
        }

        fn step_typed(
            &self,
            _states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            let existing_endpoint =
                matches!(self.mode, NoveltyBatchMode::ExistingConflict).then(RawInline::default);
            let duplicate_endpoint = if matches!(self.mode, NoveltyBatchMode::LocalConflict) {
                None
            } else {
                Some(RawInline::default())
            };
            effects.fixpoint_child(
                0,
                NonComparableState { exact_cursor: 11 },
                Key(1),
                existing_endpoint,
            );
            effects.fixpoint_child(
                0,
                NonComparableState { exact_cursor: 12 },
                Key(2),
                Some(RawInline::default()),
            );
            effects.fixpoint_child(
                0,
                NonComparableState { exact_cursor: 13 },
                Key(2),
                duplicate_endpoint,
            );
            effects.fixpoint_child(0, NonComparableState { exact_cursor: 14 }, Key(3), None);
            effects.page(4, None);
        }
    }

    struct NoveltyScopeProbe {
        endpoints: Vec<Option<RawInline>>,
    }

    impl TypedProgramSpec for NoveltyScopeProbe {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(14)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(batch.view.len(), self.endpoints.len());
            for parent in 0..batch.view.len() {
                effects.fixpoint_root(
                    parent as u32,
                    NonComparableState {
                        exact_cursor: parent + 10,
                    },
                    Key(parent as u8 + 64),
                    None,
                );
            }
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), self.endpoints.len());
            for (input, accepted) in self.endpoints.iter().copied().enumerate() {
                effects.fixpoint_child(
                    input as u32,
                    NonComparableState {
                        exact_cursor: input + 100,
                    },
                    Key(7),
                    accepted,
                );
                effects.page(1, None);
            }
        }
    }

    #[derive(Clone, Copy)]
    enum RankAttack {
        FiniteResume,
        FixpointFiniteChild,
    }

    #[derive(Clone, Copy)]
    enum AmplificationAttack {
        Seed,
        Step,
    }

    fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
            })
            .unwrap_or_default()
    }

    impl TypedProgramSpec for AmplificationAttack {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(0)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            effects.finite_root(0, NonComparableState { exact_cursor: 2 }, None);
            if matches!(self, Self::Seed) {
                effects.finite_root(0, NonComparableState { exact_cursor: 2 }, None);
            }
        }

        fn step_typed(
            &self,
            _states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            if matches!(self, Self::Step) {
                effects.finite_child(0, NonComparableState { exact_cursor: 1 }, None);
                effects.finite_child(0, NonComparableState { exact_cursor: 1 }, None);
                effects.page(1, None);
            }
        }
    }

    impl TypedProgramSpec for RankAttack {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(0)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.exact_cursor as u64
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            let state = NonComparableState { exact_cursor: 1 };
            match self {
                Self::FiniteResume => effects.finite_root(0, state, None),
                Self::FixpointFiniteChild => effects.fixpoint_root(0, state, Key(1), None),
            }
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            let state = states.pop().unwrap();
            match self {
                Self::FiniteResume => {
                    effects.page(1, Some(TypedResume::Immediate(state)));
                }
                Self::FixpointFiniteChild => {
                    effects.finite_child(0, state, None);
                    effects.page(1, None);
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum MorselMode {
        BranchMixedNovelty,
        AcceptedChain,
        ImmediateChain,
        AfterChildren,
        MalformedLater,
    }

    #[derive(Clone, Debug)]
    struct MorselState(u8);

    struct MorselProbe {
        mode: MorselMode,
        calls: Arc<Mutex<Vec<u8>>>,
    }

    impl TypedProgramSpec for MorselProbe {
        type State = MorselState;
        type NoveltyKey = Key;
        type Rank = u8;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute { variable: 0 })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(21)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.0
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            match self.mode {
                MorselMode::BranchMixedNovelty => {
                    effects.fixpoint_root(0, MorselState(9), Key(0), None);
                }
                MorselMode::AcceptedChain
                | MorselMode::ImmediateChain
                | MorselMode::AfterChildren
                | MorselMode::MalformedLater => {
                    effects.finite_root(0, MorselState(3), None);
                }
            }
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), 1);
            let state = states.pop().unwrap();
            self.calls.lock().unwrap().push(state.0);
            match (self.mode, state.0) {
                (MorselMode::BranchMixedNovelty, 9) => {
                    // Existing, fresh, local duplicate, fresh: only the two
                    // first fresh keys may own escaped handles.
                    effects.fixpoint_child(0, MorselState(8), Key(0), None);
                    effects.fixpoint_child(0, MorselState(7), Key(1), Some([1; 32]));
                    effects.fixpoint_child(0, MorselState(6), Key(1), Some([1; 32]));
                    effects.fixpoint_child(0, MorselState(5), Key(2), Some([2; 32]));
                    effects.direct(0, [3; 32]);
                    effects.accept(0, [4; 32]);
                    effects.account_transition(6);
                    effects.page(6, Some(TypedResume::Immediate(MorselState(4))));
                }
                (MorselMode::AcceptedChain, 3) => {
                    effects.fixpoint_child(0, MorselState(2), Key(2), Some([0xA2; 32]));
                    effects.account_transition(1);
                    effects.page(1, None);
                }
                (MorselMode::AcceptedChain, 2) => {
                    effects.fixpoint_child(0, MorselState(1), Key(1), Some([0xA1; 32]));
                    effects.account_transition(1);
                    effects.page(1, None);
                }
                (MorselMode::AcceptedChain, 1) => {
                    effects.accept(0, [0xAF; 32]);
                    effects.account_transition(1);
                    effects.page(1, None);
                }
                (MorselMode::ImmediateChain, 3 | 2) => {
                    effects.account_transition(1);
                    effects.page(1, Some(TypedResume::Immediate(MorselState(state.0 - 1))));
                }
                (MorselMode::ImmediateChain, 1) => {
                    effects.account_transition(1);
                    effects.page(1, None);
                }
                (MorselMode::AfterChildren, 3) => {
                    effects.fixpoint_child(0, MorselState(2), Key(2), Some([0xB2; 32]));
                    effects.account_transition(1);
                    effects.page(1, Some(TypedResume::AfterChildren(MorselState(1))));
                }
                (MorselMode::MalformedLater, 3) => {
                    effects.fixpoint_child(0, MorselState(2), Key(2), None);
                    effects.account_transition(1);
                    effects.page(1, None);
                }
                (MorselMode::MalformedLater, 2) => {
                    effects.accept(0, [0xCF; 32]);
                    effects.account_transition(1);
                    effects.page(1, Some(TypedResume::Immediate(MorselState(2))));
                }
                _ => panic!("morsel probe entered an unexpected state"),
            }
        }
    }

    fn seed_morsel_probe(
        spec: &MorselProbe,
        activation: ProgramActivation,
    ) -> (ProgramRuntime, ProgramWork) {
        let program = ProgramRef::new(spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let activations = [activation];
        let view = RowsView::new_with_row_count(&[], &[], 1);
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        assert_eq!(seeded.work.len(), 1);
        (runtime, seeded.work.pop().unwrap().work)
    }

    fn step_morsel_probe(
        spec: &MorselProbe,
        runtime: &mut ProgramRuntime,
        activation: ProgramActivation,
        work: &ProgramWork,
        limit: usize,
    ) -> ProgramBatchEffects {
        let activations = [activation];
        let work = [work.clone()];
        let candidate_sets = [None];
        let limits = [limit];
        let mut effects = ProgramBatchEffects::default();
        ProgramRef::new(spec).step_batch(
            runtime,
            ProgramBatch {
                view: RowsView::new_with_row_count(&[], &[], 1),
                candidate_sets: &candidate_sets,
                activations: &activations,
                work: &work,
                limits: &limits,
            },
            &mut effects,
        );
        effects
    }

    fn morsel_runtime(
        runtime: &mut ProgramRuntime,
    ) -> &mut TypedProgramRuntime<MorselState, Key, u8> {
        runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<MorselState, Key, u8>>()
            .unwrap()
    }

    #[test]
    fn typed_morsel_escapes_branches_with_mixed_novelty_and_only_frontier_handles() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::BranchMixedNovelty,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(91);
        let (mut runtime, work) = seed_morsel_probe(&spec, activation);
        let effects = step_morsel_probe(&spec, &mut runtime, activation, &work, 8);

        assert_eq!(*calls.lock().unwrap(), [9]);
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, 6);
        assert!(matches!(
            effects.pages[0].resume,
            Some(ProgramResume::Immediate(_))
        ));
        assert_eq!(effects.children.len(), 2);
        assert_eq!(
            effects
                .children
                .iter()
                .map(|child| child.accepted)
                .collect::<Vec<_>>(),
            [Some([1; 32]), Some([2; 32])]
        );
        assert_eq!(effects.direct, [(0, [3; 32])]);
        assert_eq!(effects.accepted, [(0, [4; 32])]);
        assert_eq!(effects.normalized_steps, 0);

        let typed = morsel_runtime(&mut runtime);
        assert_eq!(typed.novelty[&activation].len(), 3);
        assert_eq!(typed.novelty[&activation][&Key(0)], None);
        assert_eq!(typed.novelty[&activation][&Key(1)], Some([1; 32]));
        assert_eq!(typed.novelty[&activation][&Key(2)], Some([2; 32]));
        assert_eq!(
            typed
                .slots
                .iter()
                .filter(|slot| slot.value.is_some())
                .count(),
            3,
            "only two admitted children and the Immediate resume may escape"
        );
        let child_states = effects
            .children
            .iter()
            .map(|child| typed.take(activation, child.work.handle.clone()).0)
            .collect::<Vec<_>>();
        assert_eq!(child_states, [7, 5]);
        let Some(ProgramResume::Immediate(resume)) = &effects.pages[0].resume else {
            unreachable!()
        };
        assert_eq!(typed.take(activation, resume.handle.clone()).0, 4);
    }

    #[test]
    fn typed_morsel_treats_an_accepted_child_as_a_publication_boundary() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::AcceptedChain,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(92);
        let (mut runtime, work) = seed_morsel_probe(&spec, activation);
        let effects = step_morsel_probe(&spec, &mut runtime, activation, &work, 3);

        assert_eq!(*calls.lock().unwrap(), [3]);
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, 1);
        assert!(effects.pages[0].resume.is_none());
        assert_eq!(effects.children.len(), 1);
        assert_eq!(effects.children[0].accepted, Some([0xA2; 32]));
        assert!(effects.accepted.is_empty());
        assert_eq!(effects.normalized_steps, 0);

        let typed = morsel_runtime(&mut runtime);
        assert_eq!(typed.novelty[&activation].len(), 1);
        assert!(typed.contains(&effects.children[0].work.handle));
    }

    #[test]
    fn typed_morsel_consumes_unobserved_immediate_spine_under_one_budget() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::ImmediateChain,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(96);
        let (mut runtime, work) = seed_morsel_probe(&spec, activation);
        let effects = step_morsel_probe(&spec, &mut runtime, activation, &work, 3);

        assert_eq!(*calls.lock().unwrap(), [3, 2, 1]);
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, 3);
        assert!(effects.pages[0].resume.is_none());
        assert!(effects.children.is_empty());
        assert_eq!(effects.normalized_steps, 2);
        assert!(morsel_runtime(&mut runtime)
            .slots
            .iter()
            .all(|slot| slot.value.is_none()));
    }

    #[test]
    fn typed_morsel_stops_at_budget_and_clones_the_escaped_owner_independently() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::ImmediateChain,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(93);
        let (mut runtime, input) = seed_morsel_probe(&spec, activation);
        let first = step_morsel_probe(&spec, &mut runtime, activation, &input, 1);
        assert_eq!(first.pages[0].examined, 1);
        assert!(first.children.is_empty());
        assert_eq!(first.normalized_steps, 0);

        let Some(ProgramResume::Immediate(escaped)) = &first.pages[0].resume else {
            panic!("budget boundary lost the Immediate owner")
        };
        let escaped = escaped.clone();
        let input_handle = input.handle.clone();
        let typed = morsel_runtime(&mut runtime);
        assert!(!typed.contains(&input_handle));
        assert!(typed.contains(&escaped.handle));
        assert_eq!(
            typed
                .slots
                .iter()
                .filter(|slot| slot.value.is_some())
                .count(),
            1
        );

        let mut cloned = runtime.clone();
        let left = step_morsel_probe(&spec, &mut runtime, activation, &escaped, 2);
        let right = step_morsel_probe(&spec, &mut cloned, activation, &escaped, 2);
        for effects in [&left, &right] {
            assert_eq!(effects.pages[0].examined, 2);
            assert!(effects.pages[0].resume.is_none());
            assert!(effects.children.is_empty());
            assert!(effects.accepted.is_empty());
            assert_eq!(effects.normalized_steps, 1);
        }
        assert!(morsel_runtime(&mut runtime)
            .slots
            .iter()
            .all(|slot| slot.value.is_none()));
        assert!(morsel_runtime(&mut cloned)
            .slots
            .iter()
            .all(|slot| slot.value.is_none()));
    }

    #[test]
    fn typed_morsel_treats_after_children_as_a_hard_escape_barrier() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::AfterChildren,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(94);
        let (mut runtime, work) = seed_morsel_probe(&spec, activation);
        let effects = step_morsel_probe(&spec, &mut runtime, activation, &work, 3);

        assert_eq!(*calls.lock().unwrap(), [3]);
        assert_eq!(effects.normalized_steps, 0);
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.pages[0].examined, 1);
        assert!(matches!(
            effects.pages[0].resume,
            Some(ProgramResume::AfterChildren(_))
        ));
        assert_eq!(effects.children.len(), 1);
        assert_eq!(effects.children[0].accepted, Some([0xB2; 32]));
        assert_eq!(
            morsel_runtime(&mut runtime)
                .slots
                .iter()
                .filter(|slot| slot.value.is_some())
                .count(),
            2
        );
    }

    #[test]
    fn typed_morsel_later_malformed_layer_publishes_no_effect_or_handle_prefix() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = MorselProbe {
            mode: MorselMode::MalformedLater,
            calls: Arc::clone(&calls),
        };
        let activation = ProgramActivation(95);
        let (mut runtime, work) = seed_morsel_probe(&spec, activation);
        let mut effects = ProgramBatchEffects::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let activations = [activation];
            let work = [work.clone()];
            ProgramRef::new(&spec).step_batch(
                &mut runtime,
                ProgramBatch {
                    view: RowsView::new_with_row_count(&[], &[], 1),
                    candidate_sets: &[None],
                    activations: &activations,
                    work: &work,
                    limits: &[2],
                },
                &mut effects,
            );
        }));

        assert!(
            panic_text(result.expect_err("later rank violation must fail closed"))
                .contains("resume did not strictly decrease")
        );
        assert_eq!(*calls.lock().unwrap(), [3, 2]);
        assert!(effects.pages.is_empty());
        assert!(effects.children.is_empty());
        assert!(effects.direct.is_empty());
        assert!(effects.accepted.is_empty());
        assert!(effects.supported.is_empty());
        let typed = morsel_runtime(&mut runtime);
        assert_eq!(typed.novelty[&activation].get(&Key(2)), Some(&None));
        assert!(typed.slots.iter().all(|slot| slot.value.is_none()));
    }

    #[test]
    fn exact_state_and_novelty_have_independent_type_laws() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let activation = ProgramActivation(1);
        let handle = runtime.insert(activation, NonComparableState { exact_cursor: 7 });
        assert!(runtime.admit(ProgramActivation(1), Key(3), None));
        assert!(!runtime.admit(ProgramActivation(1), Key(3), None));
        assert!(runtime.admit(ProgramActivation(2), Key(3), None));
        assert_eq!(runtime.take(activation, handle).exact_cursor, 7);
    }

    #[test]
    fn stale_handles_are_rejected_after_slot_reuse() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let activation = ProgramActivation(1);
        let stale = runtime.insert(activation, NonComparableState { exact_cursor: 1 });
        let _ = runtime.take(activation, stale.clone());
        let fresh = runtime.insert(activation, NonComparableState { exact_cursor: 2 });
        assert_eq!(fresh.slot, stale.slot);
        assert_ne!(fresh.generation, stale.generation);
        let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = runtime.take(activation, stale.clone());
        }));
        assert!(replay.is_err());
        assert_eq!(runtime.take(activation, fresh).exact_cursor, 2);
    }

    #[test]
    fn deep_clone_preserves_live_handles_without_sharing_mutation() {
        let mut left = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let activation = ProgramActivation(1);
        let handle = left.insert(activation, NonComparableState { exact_cursor: 11 });
        let mut right = left.clone();
        assert!(left.contains(&handle));
        assert!(right.contains(&handle));
        assert_eq!(left.take(activation, handle.clone()).exact_cursor, 11);
        assert!(!left.contains(&handle));
        assert!(right.contains(&handle));
        left.retire_activations(&[activation]);
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            right.retire_activations(&[activation]);
        }));
        assert!(rejected.is_err());
        assert!(right.contains(&handle));
        assert_eq!(right.take(activation, handle).exact_cursor, 11);
        right.retire_activations(&[activation]);
    }

    #[test]
    fn activation_retirement_keeps_singletons_scalar_and_scans_wide_cohorts_once() {
        const HIGH_WATER: usize = 4_096;
        const RETIRING: usize = 1_024;

        let mut runtime = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let keeper = ProgramActivation(0);
        let handles: Vec<_> = (0..HIGH_WATER)
            .map(|exact_cursor| runtime.insert(keeper, NonComparableState { exact_cursor }))
            .collect();

        let singleton = ProgramActivation(1);
        assert!(runtime.admit(singleton, Key(1), None));
        runtime.retire_activations(&[singleton]);
        assert_eq!(runtime.retirement_slot_probes, HIGH_WATER);
        assert_eq!(runtime.retirement_membership_builds, 0);

        runtime.retirement_slot_probes = 0;
        let retiring: Vec<_> = (2..2 + RETIRING as u64).map(ProgramActivation).collect();
        for &activation in &retiring {
            assert!(runtime.admit(activation, Key(1), None));
        }
        runtime.retire_activations(&retiring);

        assert_eq!(runtime.retirement_slot_probes, HIGH_WATER);
        assert_eq!(runtime.retirement_membership_builds, 1);
        assert!(retiring
            .iter()
            .all(|activation| !runtime.novelty.contains_key(activation)));
        assert!(handles.iter().all(|handle| runtime.contains(handle)));
    }

    #[test]
    fn activation_retirement_skips_membership_and_scans_when_the_arena_is_drained() {
        const HIGH_WATER: usize = 4_096;

        let mut runtime = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let activations: Vec<_> = (0..HIGH_WATER)
            .map(|index| ProgramActivation(index as u64))
            .collect();
        let handles: Vec<_> = activations
            .iter()
            .enumerate()
            .map(|(exact_cursor, &activation)| {
                assert!(runtime.admit(activation, Key(1), None));
                (
                    activation,
                    runtime.insert(activation, NonComparableState { exact_cursor }),
                )
            })
            .collect();
        for (activation, handle) in handles {
            let _ = runtime.take(activation, handle);
        }
        assert_eq!(runtime.free.len(), runtime.slots.len());

        runtime.retire_activations(&activations);

        assert_eq!(runtime.retirement_slot_probes, 0);
        assert_eq!(runtime.retirement_membership_builds, 0);
        assert!(runtime.novelty.is_empty());
    }

    #[test]
    fn activation_retirement_rejection_preserves_the_whole_receipt_cohort() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key, u64>::default();
        let quiescent = ProgramActivation(11);
        let live = ProgramActivation(12);
        assert!(runtime.admit(quiescent, Key(3), None));
        assert!(runtime.admit(live, Key(4), None));
        let handle = runtime.insert(live, NonComparableState { exact_cursor: 9 });

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.retire_activations(&[quiescent, live, quiescent]);
        }));

        assert!(
            panic_text(rejected.expect_err("live cohort retirement must fail"))
                .contains("live state handle remained")
        );
        assert!(runtime.novelty.contains_key(&quiescent));
        assert!(runtime.novelty.contains_key(&live));
        assert!(runtime.contains(&handle));
        assert_eq!(runtime.retirement_membership_builds, 1);

        assert_eq!(runtime.take(live, handle).exact_cursor, 9);
        runtime.retire_activations(&[quiescent, live, quiescent]);
        assert!(!runtime.novelty.contains_key(&quiescent));
        assert!(!runtime.novelty.contains_key(&live));
    }

    fn run_novelty_batch_probe(
        mode: NoveltyBatchMode,
    ) -> (
        std::thread::Result<()>,
        ProgramRuntime,
        ProgramBatchEffects,
        ProgramActivation,
    ) {
        let spec = NoveltyBatchProbe { mode };
        let program = ProgramRef::new(&spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let activation = ProgramActivation(17);
        let activations = [activation];
        let view = RowsView::new_with_row_count(&[], &[], 1);
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        let work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let candidate_sets: [Option<&[RawInline]>; 1] = [None];
        let mut effects = ProgramBatchEffects::default();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    view,
                    candidate_sets: &candidate_sets,
                    activations: &activations,
                    work: &work,
                    limits: &[4],
                },
                &mut effects,
            );
        }));
        (result, runtime, effects, activation)
    }

    fn run_novelty_scope_probe(
        activations: &[ProgramActivation],
        endpoints: Vec<Option<RawInline>>,
    ) -> (ProgramRuntime, ProgramBatchEffects) {
        assert_eq!(activations.len(), endpoints.len());
        let spec = NoveltyScopeProbe { endpoints };
        let program = ProgramRef::new(&spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let view = RowsView::new_with_row_count(&[], &[], activations.len());
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations,
            },
            &mut seeded,
        );
        assert_eq!(seeded.work.len(), activations.len());
        let work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let candidate_sets = vec![None; activations.len()];
        let limits = vec![1; activations.len()];
        let mut effects = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                view,
                candidate_sets: &candidate_sets,
                activations,
                work: &work,
                limits: &limits,
            },
            &mut effects,
        );
        (runtime, effects)
    }

    #[test]
    fn novelty_batch_filters_existing_and_local_duplicates_in_first_admission_order() {
        let (result, mut runtime, effects, activation) =
            run_novelty_batch_probe(NoveltyBatchMode::Stable);
        result.expect("stable novelty observations must commit");
        assert_eq!(effects.pages.len(), 1);
        assert_eq!(effects.children.len(), 2);
        assert_eq!(
            effects
                .children
                .iter()
                .map(|child| child.accepted)
                .collect::<Vec<_>>(),
            [Some(RawInline::default()), None]
        );

        let typed_runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap();
        let cursors = effects
            .children
            .iter()
            .map(|child| {
                typed_runtime
                    .take(activation, child.work.handle.clone())
                    .exact_cursor
            })
            .collect::<Vec<_>>();
        assert_eq!(cursors, [12, 14]);
        let seen = typed_runtime.novelty.get(&activation).unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen.get(&Key(1)), Some(&None));
        assert_eq!(seen.get(&Key(2)), Some(&Some(RawInline::default())));
        assert_eq!(seen.get(&Key(3)), Some(&None));
    }

    #[test]
    fn novelty_batch_endpoint_conflicts_commit_no_novelty_or_output_prefix() {
        for mode in [
            NoveltyBatchMode::ExistingConflict,
            NoveltyBatchMode::LocalConflict,
        ] {
            let (result, mut runtime, effects, activation) = run_novelty_batch_probe(mode);
            let message = panic_text(result.expect_err("endpoint conflicts must fail closed"));
            assert!(message.contains("changed its endpoint observation"));
            assert!(effects.pages.is_empty());
            assert!(effects.children.is_empty());
            assert!(effects.direct.is_empty());
            assert!(effects.accepted.is_empty());
            assert!(effects.supported.is_empty());

            let typed_runtime = runtime
                .erased
                .as_mut()
                .as_any_mut()
                .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
                .unwrap();
            assert!(typed_runtime.slots.iter().all(|slot| slot.value.is_none()));
            let seen = typed_runtime.novelty.get(&activation).unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen.get(&Key(1)), Some(&None));
        }
    }

    #[test]
    fn novelty_transaction_keeps_first_receipt_across_input_tags_of_one_activation() {
        let activation = ProgramActivation(23);
        let activations = [activation, activation];
        let endpoint = Some([0xA1; 32]);
        let (mut runtime, effects) =
            run_novelty_scope_probe(&activations, vec![endpoint, endpoint]);

        assert_eq!(effects.pages.len(), 2);
        assert_eq!(effects.children.len(), 1);
        assert_eq!(effects.children[0].input, 0);
        assert_eq!(effects.children[0].accepted, endpoint);

        let typed_runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap();
        assert_eq!(
            typed_runtime
                .take(activation, effects.children[0].work.handle.clone())
                .exact_cursor,
            100,
            "the first receipt, rather than its later equal input, must own the handle"
        );
        assert_eq!(
            typed_runtime.novelty[&activation].get(&Key(7)),
            Some(&endpoint)
        );
    }

    #[test]
    fn novelty_transaction_scopes_equal_key_bytes_by_activation() {
        let activations = [ProgramActivation(23), ProgramActivation(24)];
        let endpoints = [Some([0xA1; 32]), None];
        let (mut runtime, effects) = run_novelty_scope_probe(&activations, endpoints.to_vec());

        assert_eq!(effects.pages.len(), 2);
        assert_eq!(
            effects
                .children
                .iter()
                .map(|child| (child.input, child.accepted))
                .collect::<Vec<_>>(),
            vec![(0, endpoints[0]), (1, endpoints[1])]
        );

        let typed_runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap();
        for (input, child) in effects.children.iter().enumerate() {
            assert_eq!(
                typed_runtime
                    .take(activations[input], child.work.handle.clone())
                    .exact_cursor,
                input + 100
            );
            assert_eq!(
                typed_runtime.novelty[&activations[input]].get(&Key(7)),
                Some(&endpoints[input])
            );
        }
    }

    #[test]
    fn typed_program_scratch_reuse_clears_a_wide_receipt_before_a_narrow_step() {
        let spec = ScratchReuseProbe;
        let program = ProgramRef::new(&spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let mut runtime = program.new_runtime();

        let wide_activations = [
            ProgramActivation(1),
            ProgramActivation(2),
            ProgramActivation(3),
            ProgramActivation(4),
        ];
        let wide_view = RowsView::new_with_row_count(&[], &[], wide_activations.len());
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view: wide_view,
                activations: &wide_activations,
            },
            &mut seeded,
        );
        let wide_work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let wide_candidates = [None, None, None, None];
        let mut wide = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                view: wide_view,
                candidate_sets: &wide_candidates,
                activations: &wide_activations,
                work: &wide_work,
                limits: &[1, 1, 1, 1],
            },
            &mut wide,
        );
        assert_eq!(wide.pages.len(), 4);
        assert_eq!(wide.children.len(), 1);
        assert_eq!(wide.direct.len(), 1);
        assert_eq!(wide.accepted.len(), 1);
        assert_eq!(wide.supported.len(), 1);
        assert_eq!(wide.source_pages, 1);
        assert_eq!(wide.source_examined, 4);
        assert_eq!(wide.source_roots, 1);
        assert_eq!(wide.transition_pages, 1);
        assert_eq!(wide.transition_examined, 4);

        let wide_capacities = {
            let typed = runtime
                .erased
                .as_mut()
                .as_any_mut()
                .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
                .unwrap();
            let scratch = typed.scratch.as_ref().expect("wide step warmed scratch");
            assert!(scratch.states.is_empty());
            assert!(scratch.input_ranks.is_empty());
            assert!(scratch.effects.pages.is_empty());
            assert!(scratch.effects.children.is_empty());
            assert!(scratch.effects.direct.is_empty());
            assert!(scratch.effects.accepted.is_empty());
            assert!(scratch.effects.supported.is_empty());
            assert!(scratch.examined.is_empty());
            assert!(scratch.raw_effects.is_empty());
            assert!(scratch.resume_physical.is_empty());
            assert!(scratch.batch_novelty.is_empty());
            assert!(scratch.child_admitted.is_empty());
            assert!(scratch.child_physical.is_empty());
            (
                scratch.states.capacity(),
                scratch.effects.pages.capacity(),
                scratch.effects.children.capacity(),
            )
        };

        let narrow_activations = [ProgramActivation(5)];
        let narrow_view = RowsView::new_with_row_count(&[], &[], 1);
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view: narrow_view,
                activations: &narrow_activations,
            },
            &mut seeded,
        );
        let narrow_work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let mut narrow = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                view: narrow_view,
                candidate_sets: &[None],
                activations: &narrow_activations,
                work: &narrow_work,
                limits: &[1],
            },
            &mut narrow,
        );
        assert_eq!(narrow.pages.len(), 1);
        assert!(narrow.children.is_empty());
        assert!(narrow.direct.is_empty());
        assert!(narrow.accepted.is_empty());
        assert!(narrow.supported.is_empty());
        assert_eq!(narrow.source_pages, 0);
        assert_eq!(narrow.source_examined, 0);
        assert_eq!(narrow.source_roots, 0);
        assert_eq!(narrow.transition_pages, 0);
        assert_eq!(narrow.transition_examined, 0);

        let typed = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap();
        let scratch = typed
            .scratch
            .as_ref()
            .expect("narrow step returned scratch");
        assert!(scratch.states.is_empty());
        assert!(scratch.effects.pages.is_empty());
        assert!(scratch.effects.children.is_empty());
        assert!(scratch.effects.direct.is_empty());
        assert!(scratch.effects.accepted.is_empty());
        assert!(scratch.effects.supported.is_empty());
        assert!(scratch.states.capacity() >= wide_capacities.0);
        assert!(scratch.effects.pages.capacity() >= wide_capacities.1);
        assert!(scratch.effects.children.capacity() >= wide_capacities.2);
    }

    #[test]
    fn erased_adapter_clones_live_handles_and_warm_runtime_clones_scratch_cold() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let spec = DenseProbe {
            calls: Arc::clone(&calls),
        };
        let program = ProgramRef::new(&spec);
        let route = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            })
            .unwrap();
        let activations = [
            ProgramActivation(1),
            ProgramActivation(2),
            ProgramActivation(3),
        ];
        let view = RowsView::new_with_row_count(&[], &[], activations.len());
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request: ProgramRequest {
                    action: ProgramAction::Propose(0),
                    bound: VariableSet::new_empty(),
                },
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        assert_eq!(seeded.work.len(), 3);
        let work: Vec<_> = seeded.work.iter().map(|seed| seed.work.clone()).collect();
        let candidates = [None, None, None];
        let limits = [1, 1, 1];
        let mut cloned = runtime.clone();

        for runtime in [&mut runtime, &mut cloned] {
            let mut effects = ProgramBatchEffects::default();
            program.step_batch(
                runtime,
                ProgramBatch {
                    view,
                    candidate_sets: &candidates,
                    activations: &activations,
                    work: &work,
                    limits: &limits,
                },
                &mut effects,
            );
            assert_eq!(effects.pages.len(), 3);
        }
        assert_eq!(*calls.lock().unwrap(), vec![vec![10, 11, 12]; 2]);

        let original_scratch = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap()
            .scratch
            .as_ref()
            .expect("successful step warmed the typed runtime scratch")
            .states
            .capacity();
        assert!(original_scratch >= 3);
        let mut warm_clone = runtime.clone();
        let cloned_scratch = warm_clone
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key, u64>>()
            .unwrap()
            .scratch
            .as_ref();
        assert!(cloned_scratch.is_none());
    }

    #[test]
    fn erased_adapter_rejects_cross_activation_handle_ownership() {
        let spec = DenseProbe {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let program = ProgramRef::new(&spec);
        let route = program
            .route(ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            })
            .unwrap();
        let activations = [ProgramActivation(1), ProgramActivation(2)];
        let view = RowsView::new_with_row_count(&[], &[], 2);
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request: ProgramRequest {
                    action: ProgramAction::Propose(0),
                    bound: VariableSet::new_empty(),
                },
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        let work: Vec<_> = seeded.work.iter().map(|seed| seed.work.clone()).collect();
        let crossed = [ProgramActivation(2), ProgramActivation(1)];
        let candidates = [None, None];
        let limits = [1, 1];
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    view,
                    candidate_sets: &candidates,
                    activations: &crossed,
                    work: &work,
                    limits: &limits,
                },
                &mut ProgramBatchEffects::default(),
            );
        }));
        assert!(rejected.is_err());
    }

    #[test]
    fn erased_adapter_rederives_cached_pacing_from_the_taken_typed_state() {
        let spec = DenseProbe {
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let program = ProgramRef::new(&spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let activations = [ProgramActivation(1)];
        let view = RowsView::new_with_row_count(&[], &[], 1);
        let mut runtime = program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &activations,
            },
            &mut seeded,
        );
        let mut work = [seeded.work.pop().unwrap().work];
        assert_eq!(work[0].pacing, ProgramPacing::Activation);
        work[0].pacing = ProgramPacing::Search;
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    view,
                    candidate_sets: &[None],
                    activations: &activations,
                    work: &work,
                    limits: &[1],
                },
                &mut ProgramBatchEffects::default(),
            );
        }));
        let payload = rejected.expect_err("a forged pacing cache must fail closed");
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains("incompatible pacing cohort"));
    }

    #[test]
    fn rank_rejects_finite_loops_and_fixpoint_novelty_bypasses() {
        for attack in [RankAttack::FiniteResume, RankAttack::FixpointFiniteChild] {
            let program = ProgramRef::new(&attack);
            let request = ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            };
            let route = program.route(request).unwrap();
            let activation = [ProgramActivation(1)];
            let mut runtime = program.new_runtime();
            let mut seeded = ProgramSeedEffects::default();
            program.seed_batch(
                &mut runtime,
                ProgramSeedBatch {
                    request,
                    route,
                    view: RowsView::EMPTY,
                    activations: &activation,
                },
                &mut seeded,
            );
            let work = [seeded.work.pop().unwrap().work];
            let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                program.step_batch(
                    &mut runtime,
                    ProgramBatch {
                        view: RowsView::EMPTY,
                        candidate_sets: &[None],
                        activations: &activation,
                        work: &work,
                        limits: &[1],
                    },
                    &mut ProgramBatchEffects::default(),
                );
            }));
            assert!(rejected.is_err());
        }
    }

    #[test]
    fn adapter_rejects_unbudgeted_seed_and_step_amplification() {
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let seed_attack = AmplificationAttack::Seed;
        let seed_program = ProgramRef::new(&seed_attack);
        let seed_route = seed_program.route(request).unwrap();
        let seed_rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            seed_program.seed_batch(
                &mut seed_program.new_runtime(),
                ProgramSeedBatch {
                    request,
                    route: seed_route,
                    view: RowsView::EMPTY,
                    activations: &[ProgramActivation(1)],
                },
                &mut ProgramSeedEffects::default(),
            );
        }));
        assert!(seed_rejected.is_err());

        let step_attack = AmplificationAttack::Step;
        let step_program = ProgramRef::new(&step_attack);
        let route = step_program.route(request).unwrap();
        let mut runtime = step_program.new_runtime();
        let mut seeded = ProgramSeedEffects::default();
        step_program.seed_batch(
            &mut runtime,
            ProgramSeedBatch {
                request,
                route,
                view: RowsView::EMPTY,
                activations: &[ProgramActivation(1)],
            },
            &mut seeded,
        );
        let work = [seeded.work.pop().unwrap().work];
        let step_rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            step_program.step_batch(
                &mut runtime,
                ProgramBatch {
                    view: RowsView::EMPTY,
                    candidate_sets: &[None],
                    activations: &[ProgramActivation(1)],
                    work: &work,
                    limits: &[1],
                },
                &mut ProgramBatchEffects::default(),
            );
        }));
        assert!(step_rejected.is_err());
    }
}
