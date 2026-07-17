//! Typed residual-program substrate.
//!
//! The residual engine owns affine scheduling, reducers, and return
//! continuations. A program family owns only its stored typed continuation
//! states and per-activation novelty keys. The erased boundary is crossed once
//! for a physical cohort; individual work items are generational handles into
//! a query-local typed arena rather than boxes or engine-defined opcodes.

use std::any::{type_name, Any, TypeId};
use std::hash::Hash;

use ahash::AHashMap;

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

/// Family-local immutable identity within one structural occurrence.
///
/// The occurrence-local address carries this value directly; it is not a
/// query-global catalog or a forwarding lookup key.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramKey(u32);

impl ProgramKey {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

/// Certificate for the recurrence stratum of a constructed program.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProgramStratum {
    /// The typed continuation graph is acyclic for this route.
    Finite,
    /// Per-activation typed novelty computes a least fixpoint. Quiescence
    /// additionally relies on the family exposing a finite reachable novelty
    /// domain; RPQ keys are finite graph-value × program-counter products.
    Fixpoint,
}

/// Action-specific candidate admission law carried by a constructed route.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramGrouping {
    /// Each candidate page is an independent input to the program reducer.
    PageLocal,
    /// The complete ordered candidate bag for one parent remains atomic.
    ///
    /// V1 discovers this requirement by probing `Confirm(variable)` with all
    /// other variables owned by the same constraint family bound. A program
    /// must therefore keep grouping compatible as the ambient bound schema
    /// grows: it must not introduce `ParentAtomic` only at an intermediate or
    /// globally enriched schema after that family-local probe returned
    /// `PageLocal`. RPQ routes satisfy this because their two endpoints make
    /// the probe schema the only opposite-endpoint transition.
    ParentAtomic,
}

/// Structural route selected by an immutable program spec for one action.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramRoute {
    pub key: ProgramKey,
    /// Variable naming the structural graph-product operator.
    pub variable: VariableId,
    pub stratum: ProgramStratum,
    pub grouping: ProgramGrouping,
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
    pub stratum: ProgramStratum,
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
    pub stratum: ProgramStratum,
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

/// Family-typed residual program contract.
///
/// Program code can emit only typed states and novelty keys. It cannot create
/// or inspect engine handles, and therefore cannot bypass affine take or
/// novelty admission.
#[doc(hidden)]
pub trait TypedProgramSpec {
    type State: Clone + Send + 'static;
    type NoveltyKey: Clone + Eq + Hash + Send + 'static;
    /// Family-owned finite-domain measure for non-recurrent edges.
    ///
    /// Every resume and every child without a novelty key must strictly
    /// decrease this rank. Novelty-admitted fixpoint roots and children may
    /// enter at any rank, but their later finite pagination must decrease.
    type Rank: Ord;

    /// Selects one structural action route.
    ///
    /// In addition to being stable for the solve, confirmation routes obey
    /// [`ProgramGrouping`]'s V1 family-local planning contract.
    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute>;

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

    fn step_typed(
        &self,
        states: Vec<Self::State>,
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
/// step call, then operate on a dense typed state vector. An implementation
/// must never fall back to legacy residual hooks after returning a route.
trait ErasedProgramSpec {
    fn new_runtime(&self) -> ProgramRuntime;

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute>;

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

    fn retire_activations(&self, runtime: &mut ProgramRuntime, activations: &[ProgramActivation]);
}

/// Borrowed immutable typed program behind a private erased vtable.
///
/// The only constructor accepts [`TypedProgramSpec`], so custom constraints
/// cannot bypass typed sinks, activation ownership, or novelty admission.
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
#[derive(Clone)]
struct TypedProgramRuntime<State, NoveltyKey> {
    slots: Vec<ArenaSlot<State>>,
    free: Vec<u32>,
    novelty: AHashMap<ProgramActivation, AHashMap<NoveltyKey, Option<RawInline>>>,
}

impl<State, NoveltyKey> Default for TypedProgramRuntime<State, NoveltyKey> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            novelty: AHashMap::new(),
        }
    }
}

impl<State, NoveltyKey> TypedProgramRuntime<State, NoveltyKey>
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
    fn take_batch(
        &mut self,
        activations: &[ProgramActivation],
        handles: &[ProgramWork],
    ) -> Vec<State> {
        assert_eq!(activations.len(), handles.len());
        activations
            .iter()
            .zip(handles)
            .map(|(&activation, work)| self.take(activation, work.handle.clone()))
            .collect()
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

    fn retire(&mut self, activation: ProgramActivation) {
        assert!(
            self.slots.iter().all(|slot| {
                slot.value
                    .as_ref()
                    .is_none_or(|(owner, _)| *owner != activation)
            }),
            "program activation retired while a live state handle remained"
        );
        self.novelty.remove(&activation);
    }

    #[cfg(test)]
    fn contains(&self, handle: &ProgramWorkHandle) -> bool {
        self.slots
            .get(handle.slot as usize)
            .is_some_and(|slot| slot.generation == handle.generation && slot.value.is_some())
    }
}

impl<T> ErasedProgramSpec for T
where
    T: TypedProgramSpec,
{
    fn new_runtime(&self) -> ProgramRuntime {
        ProgramRuntime {
            erased: Box::new(TypedProgramRuntime::<T::State, T::NoveltyKey>::default()),
            family: TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            family_name: type_name::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
        }
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        TypedProgramSpec::route(self, request)
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
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            "residual program seed expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey>>()
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
            assert!(
                batch.route.stratum == ProgramStratum::Fixpoint || seed.novelty.is_none(),
                "a finite typed program emitted a fixpoint root"
            );
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
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            "residual program step expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey>>()
            .expect("residual program step received another family's runtime");
        let states = runtime.take_batch(batch.activations, batch.work);
        let input_ranks: Vec<_> = states.iter().map(|state| self.progress(state)).collect();
        for (state, work) in states.iter().zip(batch.work) {
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

        let typed_batch = TypedProgramBatch {
            stratum: batch.stratum,
            view: batch.view,
            candidate_sets: batch.candidate_sets,
            activations: batch.activations,
            limits: batch.limits,
        };
        let mut typed = TypedEffectSink::default();
        self.step_typed(states, typed_batch, &mut typed);
        assert_eq!(
            typed.pages.len(),
            input_count,
            "typed program returned the wrong page count"
        );
        let examined: Vec<_> = typed.pages.iter().map(|page| page.examined).collect();
        assert!(
            examined
                .iter()
                .zip(batch.limits)
                .all(|(&spent, &limit)| spent <= limit),
            "typed program exceeded one input's physical work budget"
        );
        let mut raw_effects = vec![0usize; input_count];

        effects
            .pages
            .extend(typed.pages.into_iter().enumerate().map(|(input, page)| {
                let activation = batch.activations[input];
                let resume = page.resume.map(|resume| match resume {
                    TypedResume::Immediate(state) => {
                        assert!(
                            self.progress(&state) < input_ranks[input],
                            "typed program resume did not strictly decrease its finite rank"
                        );
                        let dispatch = self.dispatch(&state);
                        let pacing = self.pacing(&state);
                        let work = ProgramWork {
                            handle: runtime.insert(activation, state),
                            dispatch,
                            pacing,
                        };
                        ProgramResume::Immediate(work)
                    }
                    TypedResume::AfterChildren(state) => {
                        assert!(
                            self.progress(&state) < input_ranks[input],
                            "typed program resume did not strictly decrease its finite rank"
                        );
                        let dispatch = self.dispatch(&state);
                        let pacing = self.pacing(&state);
                        let work = ProgramWork {
                            handle: runtime.insert(activation, state),
                            dispatch,
                            pacing,
                        };
                        ProgramResume::AfterChildren(work)
                    }
                    TypedResume::AfterChildrenDone => ProgramResume::AfterChildrenDone,
                });
                ProgramPage {
                    examined: page.examined,
                    resume,
                }
            }));

        let mut previous = 0u32;
        for (position, child) in typed.children.into_iter().enumerate() {
            assert!(
                (child.input as usize) < input_count,
                "typed program child tag is out of range"
            );
            assert!(
                position == 0 || child.input >= previous,
                "typed program child tags are not grouped in ascending order"
            );
            previous = child.input;
            raw_effects[child.input as usize] += 1;
            assert!(
                batch.stratum == ProgramStratum::Fixpoint || child.novelty.is_none(),
                "a finite typed program emitted a fixpoint child"
            );
            if child.novelty.is_none() {
                assert!(
                    self.progress(&child.state) < input_ranks[child.input as usize],
                    "typed program finite child did not strictly decrease its input rank"
                );
            }
            let activation = batch.activations[child.input as usize];
            if let Some(novelty) = child.novelty {
                if !runtime.admit(activation, novelty, child.accepted) {
                    continue;
                }
            }
            let dispatch = self.dispatch(&child.state);
            let pacing = self.pacing(&child.state);
            let work = ProgramWork {
                handle: runtime.insert(activation, child.state),
                dispatch,
                pacing,
            };
            effects.children.push(ProgramChild {
                input: child.input,
                work,
                accepted: child.accepted,
            });
        }

        let mut previous = 0u32;
        for (position, (input, value)) in typed.direct.into_iter().enumerate() {
            assert!((input as usize) < input_count);
            assert!(
                position == 0 || input >= previous,
                "typed direct observations are not grouped in ascending order"
            );
            previous = input;
            raw_effects[input as usize] += 1;
            effects.direct.push((input, value));
        }
        let mut previous = 0u32;
        for (position, (input, value)) in typed.accepted.into_iter().enumerate() {
            assert!((input as usize) < input_count);
            assert!(
                position == 0 || input >= previous,
                "typed candidate observations are not grouped in ascending order"
            );
            previous = input;
            raw_effects[input as usize] += 1;
            effects.accepted.push((input, value));
        }
        let mut previous = 0u32;
        for (position, (input, ())) in typed.supported.into_iter().enumerate() {
            assert!((input as usize) < input_count);
            assert!(
                position == 0 || input >= previous,
                "typed support observations are not grouped in ascending order"
            );
            previous = input;
            raw_effects[input as usize] += 1;
            effects.supported.push((input, ()));
        }
        assert!(
            raw_effects
                .iter()
                .zip(&examined)
                .all(|(&outputs, &spent)| outputs <= spent),
            "typed program emitted more raw effects than its examined-work receipt"
        );
        effects.source_pages += typed.source_pages;
        effects.source_examined += typed.source_examined;
        effects.source_roots += typed.source_roots;
        effects.transition_pages += typed.transition_pages;
        effects.transition_examined += typed.transition_examined;
    }

    fn retire_activations(&self, runtime: &mut ProgramRuntime, activations: &[ProgramActivation]) {
        assert_eq!(
            runtime.family,
            TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            "residual program retirement expected family {}, received {}",
            type_name::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
            runtime.family_name
        );
        let runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey>>()
            .expect("residual program retirement received another family's runtime");
        for &activation in activations {
            runtime.retire(activation);
        }
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
            Some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
            })
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
            states: Vec<Self::State>,
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

    struct FiniteNovelty;

    impl TypedProgramSpec for FiniteNovelty {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            None
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
            effects.fixpoint_root(0, NonComparableState { exact_cursor: 0 }, Key(0), None);
        }

        fn step_typed(
            &self,
            _states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            _effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
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

    impl TypedProgramSpec for AmplificationAttack {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
            })
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
            _states: Vec<Self::State>,
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
            Some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: match self {
                    Self::FiniteResume => ProgramStratum::Finite,
                    Self::FixpointFiniteChild => ProgramStratum::Fixpoint,
                },
                grouping: ProgramGrouping::PageLocal,
            })
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
            mut states: Vec<Self::State>,
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

    #[test]
    fn exact_state_and_novelty_have_independent_type_laws() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
        let activation = ProgramActivation(1);
        let handle = runtime.insert(activation, NonComparableState { exact_cursor: 7 });
        assert!(runtime.admit(ProgramActivation(1), Key(3), None));
        assert!(!runtime.admit(ProgramActivation(1), Key(3), None));
        assert!(runtime.admit(ProgramActivation(2), Key(3), None));
        assert_eq!(runtime.take(activation, handle).exact_cursor, 7);
    }

    #[test]
    fn stale_handles_are_rejected_after_slot_reuse() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
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
        let mut left = TypedProgramRuntime::<NonComparableState, Key>::default();
        let activation = ProgramActivation(1);
        let handle = left.insert(activation, NonComparableState { exact_cursor: 11 });
        let mut right = left.clone();
        assert!(left.contains(&handle));
        assert!(right.contains(&handle));
        assert_eq!(left.take(activation, handle.clone()).exact_cursor, 11);
        assert!(!left.contains(&handle));
        assert!(right.contains(&handle));
        assert_eq!(right.take(activation, handle).exact_cursor, 11);
    }

    #[test]
    fn erased_adapter_clones_live_handles_and_steps_one_dense_typed_cohort() {
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
                    stratum: ProgramStratum::Finite,
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
                    stratum: ProgramStratum::Finite,
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
                    stratum: route.stratum,
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
    fn finite_route_rejects_fixpoint_novelty_at_the_adapter_boundary() {
        let spec = FiniteNovelty;
        let program = ProgramRef::new(&spec);
        let mut runtime = program.new_runtime();
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
        };
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.seed_batch(
                &mut runtime,
                ProgramSeedBatch {
                    request: ProgramRequest {
                        action: ProgramAction::Propose(0),
                        bound: VariableSet::new_empty(),
                    },
                    route,
                    view: RowsView::EMPTY,
                    activations: &[ProgramActivation(1)],
                },
                &mut ProgramSeedEffects::default(),
            );
        }));
        assert!(rejected.is_err());
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
                        stratum: route.stratum,
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
                    stratum: ProgramStratum::Finite,
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
