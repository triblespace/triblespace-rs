//! Typed residual-program substrate.
//!
//! The residual engine owns affine scheduling, reducers, and return
//! continuations. A program family owns only its serialized continuation
//! states and per-activation novelty keys. The erased boundary is crossed once
//! for a physical cohort; individual work items are generational handles into
//! a query-local typed arena rather than boxes or engine-defined opcodes.

use std::any::Any;
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

/// Generational reference to one serialized, family-private continuation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramWorkHandle {
    slot: u32,
    generation: u32,
}

/// Engine-visible paging category for one opaque continuation.
///
/// This controls affine source-page joins and accounting only. The program's
/// exact source/transition state remains behind [`ProgramWorkHandle`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProgramWorkKind {
    Source,
    Transition,
}

/// One schedulable opaque continuation and its physical compatibility.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProgramWork {
    pub handle: ProgramWorkHandle,
    pub dispatch: DispatchClass,
    pub kind: ProgramWorkKind,
}

/// Closed query action offered to a residual program.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramAction {
    Propose(VariableId),
    Confirm(VariableId),
    Support,
}

/// Structural route selected by an immutable program spec for one action.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramRoute {
    /// Variable naming the structural graph-product operator.
    pub variable: VariableId,
    /// Kind of the initial opaque continuation.
    pub initial_kind: ProgramWorkKind,
    /// Whether draining the route can require product-state transitions.
    pub has_transition_work: bool,
}

/// Row block used to construct initial typed work handles.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ProgramSeedBatch<'v> {
    pub action: ProgramAction,
    pub route: ProgramRoute,
    pub view: RowsView<'v>,
}

/// One initial affine work root for a tagged parent row.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramPage {
    pub examined: usize,
    /// Exact same-lineage continuation. It bypasses novelty admission.
    pub resume: Option<ProgramWork>,
}

/// One novel child admitted by the typed runtime.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

/// Cloneable erased query-local runtime.
#[doc(hidden)]
pub trait ResidualProgramRuntime: Any + Send {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn clone_box(&self) -> Box<dyn ResidualProgramRuntime>;
}

impl<T> ResidualProgramRuntime for T
where
    T: Any + Clone + Send,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn ResidualProgramRuntime> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn ResidualProgramRuntime> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Immutable residual-program family specification.
///
/// Implementations downcast `runtime` once at the beginning of each seed or
/// step call, then operate on a dense typed state vector. An implementation
/// must never fall back to legacy residual hooks after returning a route.
#[doc(hidden)]
pub trait ResidualProgramSpec {
    fn new_runtime(&self) -> Box<dyn ResidualProgramRuntime>;

    fn route(&self, action: ProgramAction, view: &RowsView<'_>) -> Option<ProgramRoute>;

    fn confirm_grouping_requirements(&self, _variable: VariableId) -> Option<VariableSet> {
        None
    }

    fn seed_batch(
        &self,
        runtime: &mut dyn ResidualProgramRuntime,
        batch: ProgramSeedBatch<'_>,
        effects: &mut ProgramSeedEffects,
    );

    fn step_batch(
        &self,
        runtime: &mut dyn ResidualProgramRuntime,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    );
}

#[derive(Clone)]
struct ArenaSlot<T> {
    generation: u32,
    value: Option<T>,
}

/// Query-local typed state and novelty storage for one program occurrence.
///
/// `State` is deliberately not constrained by equality or hashing. Only the
/// smaller family-defined `NoveltyKey` participates in per-activation
/// admission.
#[doc(hidden)]
#[derive(Clone)]
pub struct TypedProgramRuntime<State, NoveltyKey> {
    slots: Vec<ArenaSlot<State>>,
    free: Vec<u32>,
    novelty: AHashMap<ProgramActivation, AHashMap<NoveltyKey, bool>>,
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
    pub fn insert(&mut self, state: State) -> ProgramWorkHandle {
        if let Some(slot) = self.free.pop() {
            let record = &mut self.slots[slot as usize];
            assert!(
                record.value.is_none(),
                "program free list named a live slot"
            );
            record.value = Some(state);
            ProgramWorkHandle {
                slot,
                generation: record.generation,
            }
        } else {
            let slot = u32::try_from(self.slots.len()).expect("program work arena exhausted");
            self.slots.push(ArenaSlot {
                generation: 0,
                value: Some(state),
            });
            ProgramWorkHandle {
                slot,
                generation: 0,
            }
        }
    }

    /// Affinely takes one continuation. A copied or replayed handle is stale.
    pub fn take(&mut self, handle: ProgramWorkHandle) -> State {
        let record = self
            .slots
            .get_mut(handle.slot as usize)
            .expect("program work handle named an unknown slot");
        assert_eq!(
            record.generation, handle.generation,
            "stale program work handle generation"
        );
        let value = record
            .value
            .take()
            .expect("program work handle was replayed after affine take");
        record.generation = record
            .generation
            .checked_add(1)
            .expect("program work generation exhausted");
        self.free.push(handle.slot);
        value
    }

    /// Takes a cohort into one dense typed vector in scheduler order.
    pub fn take_batch(&mut self, handles: &[ProgramWork]) -> Vec<State> {
        handles.iter().map(|work| self.take(work.handle)).collect()
    }

    /// Admits one typed novelty key for an activation.
    ///
    /// The attached Boolean is the key's endpoint observation and must remain
    /// stable if another exact state maps to the same novelty key.
    pub fn admit(
        &mut self,
        activation: ProgramActivation,
        key: NoveltyKey,
        accepted: bool,
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

    #[cfg(test)]
    fn contains(&self, handle: ProgramWorkHandle) -> bool {
        self.slots
            .get(handle.slot as usize)
            .is_some_and(|slot| slot.generation == handle.generation && slot.value.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct NonComparableState {
        exact_cursor: usize,
    }

    #[derive(Clone, Eq, Hash, PartialEq)]
    struct Key(u8);

    #[test]
    fn exact_state_and_novelty_have_independent_type_laws() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
        let handle = runtime.insert(NonComparableState { exact_cursor: 7 });
        assert!(runtime.admit(ProgramActivation(1), Key(3), false));
        assert!(!runtime.admit(ProgramActivation(1), Key(3), false));
        assert!(runtime.admit(ProgramActivation(2), Key(3), false));
        assert_eq!(runtime.take(handle).exact_cursor, 7);
    }

    #[test]
    fn stale_handles_are_rejected_after_slot_reuse() {
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
        let stale = runtime.insert(NonComparableState { exact_cursor: 1 });
        let _ = runtime.take(stale);
        let fresh = runtime.insert(NonComparableState { exact_cursor: 2 });
        assert_eq!(fresh.slot, stale.slot);
        assert_ne!(fresh.generation, stale.generation);
        let replay = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = runtime.take(stale);
        }));
        assert!(replay.is_err());
        assert_eq!(runtime.take(fresh).exact_cursor, 2);
    }

    #[test]
    fn deep_clone_preserves_live_handles_without_sharing_mutation() {
        let mut left = TypedProgramRuntime::<NonComparableState, Key>::default();
        let handle = left.insert(NonComparableState { exact_cursor: 11 });
        let mut right = left.clone();
        assert!(left.contains(handle));
        assert!(right.contains(handle));
        assert_eq!(left.take(handle).exact_cursor, 11);
        assert!(!left.contains(handle));
        assert!(right.contains(handle));
        assert_eq!(right.take(handle).exact_cursor, 11);
    }
}
