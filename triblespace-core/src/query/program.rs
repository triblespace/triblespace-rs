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
use std::marker::PhantomData;

use ahash::{AHashMap, AHashSet};
use smallvec::SmallVec;

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

/// Private semantic arm of one family-local route key.
///
/// `Preferred` and `Fallback` are introduced only by [`PreferredProgram`].
/// The arm is carried by the immutable occurrence-local address, so it is
/// selected once before runtime construction and never rides an affine work
/// handle or a family state. It is not a physical placement tag: Native and
/// accelerated execution remain interchangeable inside the selected arm.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ProgramRouteArm {
    Direct,
    Preferred,
    Fallback,
}

/// Family-local immutable identity within one structural occurrence.
///
/// The occurrence-local address carries both the route's local value and its
/// private composition arm directly; it is not a query-global catalog or a
/// forwarding lookup key.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramKey {
    local: u32,
    arm: ProgramRouteArm,
}

impl ProgramKey {
    pub const fn new(value: u32) -> Self {
        Self {
            local: value,
            arm: ProgramRouteArm::Direct,
        }
    }

    fn in_arm(self, arm: ProgramRouteArm) -> Self {
        assert_eq!(
            self.arm,
            ProgramRouteArm::Direct,
            "a composed Program route attempted to wrap an already-armed key"
        );
        Self {
            local: self.local,
            arm,
        }
    }

    fn direct(self) -> Self {
        Self::new(self.local)
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

/// Action-specific certificate for replacing pageable execution with one
/// complete family-owned batch.
///
/// This is a semantic equivalence claim for the exact [`ProgramRequest`] and
/// bound schema carried by a route. It does not select the physical phase:
/// terminality, demand, and cohort width remain scheduler evidence.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramCompletion {
    /// The route must be evaluated through its budgeted affine continuation.
    PageableOnly,
    /// [`TypedProgramSpec::complete_typed`] returns the exact complete raw
    /// per-parent Propose occurrence bag produced by draining this route. The
    /// erased adapter validates that whole bag before admitting each distinct
    /// `(parent, value)` once at the completed action boundary.
    CompleteActionEquivalent,
}

/// Policy tier required before the scheduler may select a constructed route.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramExposure {
    /// The route is part of the ordinary production execution policy.
    Production,
    /// The route is available only when explicitly requested by policy.
    Explicit,
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
    pub completion: ProgramCompletion,
    pub exposure: ProgramExposure,
}

/// Runtime-free complete-action call for one certified route.
///
/// No activation or work handle appears here: successful completion retires
/// the fresh parent cohort without ever opening sparse Program state.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ProgramCompleteBatch<'v> {
    pub request: ProgramRequest,
    pub route: ProgramRoute,
    pub view: RowsView<'v>,
}

/// Exact parent-local admission evidence for one complete action.
///
/// Quotes are physical evidence only. They never name Program work handles,
/// enter canonical identity, or grant execution; the erased adapter consumes
/// them in the same call that either completes one bounded tail or declines.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramCompleteWorkQuote {
    /// Exact physical units examined by an unbounded drain for this parent.
    pub drain_work_units: usize,
    /// Exact raw occurrences emitted before parent-local SET admission.
    pub raw_occurrences: usize,
}

/// Physical admission policy returned by a typed complete-action family.
#[doc(hidden)]
#[derive(Debug, Eq, PartialEq)]
pub enum ProgramCompleteWorkEvidence {
    /// This family predates exact quoting. Preserve its established semantic
    /// whole-batch completion policy after independent scheduler admission.
    Unquoted,
    /// One exact quote per parent, in batch order.
    Quoted(Vec<ProgramCompleteWorkQuote>),
    /// Exact evidence is unavailable for this batch; use pageable execution.
    Declined,
}

#[cfg(test)]
#[derive(Default)]
pub struct ProgramCompleteEffects {
    pub occurrences: Vec<(u32, RawInline)>,
    pub raw_occurrence_count: usize,
}

/// Admission receipt retained only until the originating batch is consumed.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ProgramCompleteAdmission {
    LegacyUnquoted,
    Exact {
        drain_work_units: usize,
        raw_occurrences: usize,
    },
}

/// Unique non-zero-sized identity for one cold completion transaction.
///
/// Slice identity alone cannot distinguish zero-column row batches because
/// their backing storage is empty. The token therefore borrows and records
/// the sized owner of those slices, in addition to carrying its own identity.
pub(crate) struct ProgramCompleteAffinity<'owner> {
    owner: *const (),
    _owner: PhantomData<&'owner u8>,
    _identity: u8,
}

impl<'owner> ProgramCompleteAffinity<'owner> {
    pub(crate) fn new<T: Sized>(owner: &'owner T) -> Self {
        assert!(
            std::mem::size_of_val(owner) > 0,
            "bounded Program completion owner must have an address identity"
        );
        Self {
            owner: std::ptr::from_ref(owner).cast(),
            _owner: PhantomData,
            _identity: 0,
        }
    }

    fn is_owner<T: Sized>(&self, owner: &T) -> bool {
        std::ptr::eq(self.owner, std::ptr::from_ref(owner).cast())
    }
}

/// Non-cloneable completion result affined to its exact borrowed input batch.
///
/// The fields stay private so quote evidence cannot become scheduler state.
/// `into_parts_for` checks both the transaction token and original slice
/// identity before releasing owned effects to the one cold residual owner.
pub(crate) struct ProgramBoundedCompletion<'v, 'affinity, 'owner> {
    batch: ProgramCompleteBatch<'v>,
    affinity: &'affinity ProgramCompleteAffinity<'owner>,
    first_parent: usize,
    admission: ProgramCompleteAdmission,
    raw_occurrence_count: usize,
    occurrences: Vec<(u32, RawInline)>,
}

impl<'v> ProgramBoundedCompletion<'v, '_, '_> {
    pub(crate) fn into_parts_for<T: Sized>(
        self,
        batch: ProgramCompleteBatch<'v>,
        affinity: &ProgramCompleteAffinity<'_>,
        owner: &T,
    ) -> (
        usize,
        ProgramCompleteAdmission,
        usize,
        Vec<(u32, RawInline)>,
    ) {
        assert!(
            std::ptr::eq(self.affinity, affinity),
            "bounded Program completion was paired with another transaction"
        );
        assert!(
            affinity.is_owner(owner),
            "bounded Program completion was paired with another owner"
        );
        assert_eq!(self.batch.request, batch.request);
        assert_eq!(self.batch.route, batch.route);
        assert_eq!(self.batch.view.len(), batch.view.len());
        assert!(
            std::ptr::eq(self.batch.view.vars, batch.view.vars)
                && std::ptr::eq(self.batch.view.rows, batch.view.rows),
            "bounded Program completion was paired with another row batch"
        );
        (
            self.first_parent,
            self.admission,
            self.raw_occurrence_count,
            self.occurrences,
        )
    }
}

/// Family-facing complete-action sink.
///
/// Parent bounds and grouping are intentionally checked by the erased adapter
/// after the family call, rather than trusted at each public push site.
#[doc(hidden)]
pub struct TypedCompleteSink {
    occurrences: Vec<(u32, RawInline)>,
}

impl TypedCompleteSink {
    pub fn push(&mut self, parent: u32, value: RawInline) {
        self.occurrences.push((parent, value));
    }

    pub fn extend_parent(&mut self, parent: u32, values: impl IntoIterator<Item = RawInline>) {
        self.occurrences
            .extend(values.into_iter().map(|value| (parent, value)));
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TypedCompleteSelection {
    first_parent: usize,
    parent_count: usize,
}

impl TypedCompleteSelection {
    pub(crate) fn first_parent(self) -> usize {
        self.first_parent
    }

    fn tail(self, batch: ProgramCompleteBatch<'_>) -> ProgramCompleteBatch<'_> {
        assert_eq!(
            batch.view.len(),
            self.parent_count,
            "typed complete selection was applied to another parent cohort"
        );
        let completed_parent_count = self.parent_count - self.first_parent;
        let row_offset = self
            .first_parent
            .checked_mul(batch.view.vars.len())
            .expect("typed complete-action row offset overflow");
        ProgramCompleteBatch {
            view: RowsView::new_with_row_count(
                batch.view.vars,
                &batch.view.rows[row_offset..],
                completed_parent_count,
            ),
            ..batch
        }
    }
}

struct TypedCompleteTail {
    next_parent: Option<usize>,
    stopped_at_boundary: bool,
    quotes_reversed: Vec<ProgramCompleteWorkQuote>,
    drain_work_units: usize,
    raw_occurrences: usize,
}

enum TypedCompleteArbiterState {
    Pending,
    Tail(TypedCompleteTail),
    Declined,
    LegacyUnquoted,
    Exact {
        first_parent: usize,
        drain_work_units: usize,
        raw_occurrences: usize,
        quotes: Vec<ProgramCompleteWorkQuote>,
    },
}

/// Engine-owned one-shot admission and validation transaction for a complete
/// action.
///
/// The constructor, admission operations, selected suffix, and retained quote
/// state are crate-private. Public typed families inherit the legacy default
/// path; a crate-owned physical family may prepare work tail-first without
/// exposing quotes to scheduler state.
#[doc(hidden)]
pub struct TypedCompleteArbiter {
    parent_count: usize,
    capacity: usize,
    state: TypedCompleteArbiterState,
    effects: TypedCompleteSink,
}

impl TypedCompleteArbiter {
    fn new(parent_count: usize, capacity: usize) -> Self {
        Self {
            parent_count,
            capacity,
            state: TypedCompleteArbiterState::Pending,
            effects: TypedCompleteSink {
                occurrences: Vec::new(),
            },
        }
    }

    fn begin_tail(&mut self) -> &mut TypedCompleteTail {
        if matches!(self.state, TypedCompleteArbiterState::Pending) {
            self.state = TypedCompleteArbiterState::Tail(TypedCompleteTail {
                next_parent: self.parent_count.checked_sub(1),
                stopped_at_boundary: false,
                quotes_reversed: Vec::new(),
                drain_work_units: 0,
                raw_occurrences: 0,
            });
        }
        let TypedCompleteArbiterState::Tail(tail) = &mut self.state else {
            panic!("typed complete arbiter admission was not one-shot")
        };
        assert!(
            !tail.stopped_at_boundary,
            "typed complete tail continued after its first rejected boundary"
        );
        tail
    }

    fn check_tail_parent(tail: &TypedCompleteTail, parent: usize) {
        assert_eq!(
            tail.next_parent,
            Some(parent),
            "typed complete tail parents were not submitted in strict descending order"
        );
    }

    /// Attempts exactly the next descending parent against both physical
    /// bounds. A rejection closes the tail at that first boundary.
    pub(crate) fn try_admit_tail_parent(
        &mut self,
        parent: usize,
        quote: ProgramCompleteWorkQuote,
    ) -> bool {
        let capacity = self.capacity;
        let tail = self.begin_tail();
        Self::check_tail_parent(tail, parent);
        let Some(next_drain) = tail.drain_work_units.checked_add(quote.drain_work_units) else {
            tail.stopped_at_boundary = true;
            return false;
        };
        let Some(next_raw) = tail.raw_occurrences.checked_add(quote.raw_occurrences) else {
            tail.stopped_at_boundary = true;
            return false;
        };
        if next_drain > capacity || next_raw > capacity {
            tail.stopped_at_boundary = true;
            return false;
        }
        tail.drain_work_units = next_drain;
        tail.raw_occurrences = next_raw;
        tail.quotes_reversed.push(quote);
        tail.next_parent = parent.checked_sub(1);
        true
    }

    /// Closes an unrepresentable exact parent at the same first-boundary seam.
    pub(crate) fn reject_tail_parent(&mut self, parent: usize) {
        let tail = self.begin_tail();
        Self::check_tail_parent(tail, parent);
        tail.stopped_at_boundary = true;
    }

    /// Seals a maximal tail after every parent through either zero or the
    /// first rejected boundary has been visited.
    pub(crate) fn seal_tail_admission(&mut self) -> Option<TypedCompleteSelection> {
        if matches!(self.state, TypedCompleteArbiterState::Pending) {
            self.begin_tail();
        }
        let state = std::mem::replace(&mut self.state, TypedCompleteArbiterState::Declined);
        let TypedCompleteArbiterState::Tail(mut tail) = state else {
            panic!("typed complete tail admission was sealed more than once")
        };
        assert!(
            tail.stopped_at_boundary || tail.next_parent.is_none(),
            "typed complete tail stopped before reaching a rejected boundary"
        );
        if tail.quotes_reversed.len() < 2 {
            return None;
        }
        let first_parent = self.parent_count - tail.quotes_reversed.len();
        tail.quotes_reversed.reverse();
        self.state = TypedCompleteArbiterState::Exact {
            first_parent,
            drain_work_units: tail.drain_work_units,
            raw_occurrences: tail.raw_occurrences,
            quotes: tail.quotes_reversed,
        };
        Some(TypedCompleteSelection {
            first_parent,
            parent_count: self.parent_count,
        })
    }

    fn try_admit_evidence(
        &mut self,
        evidence: ProgramCompleteWorkEvidence,
    ) -> Option<TypedCompleteSelection> {
        match evidence {
            ProgramCompleteWorkEvidence::Unquoted => {
                assert!(
                    matches!(self.state, TypedCompleteArbiterState::Pending),
                    "typed complete arbiter admission was not one-shot"
                );
                self.state = TypedCompleteArbiterState::LegacyUnquoted;
                Some(TypedCompleteSelection {
                    first_parent: 0,
                    parent_count: self.parent_count,
                })
            }
            ProgramCompleteWorkEvidence::Declined => {
                assert!(
                    matches!(self.state, TypedCompleteArbiterState::Pending),
                    "typed complete arbiter admission was not one-shot"
                );
                self.state = TypedCompleteArbiterState::Declined;
                None
            }
            ProgramCompleteWorkEvidence::Quoted(quotes) => {
                assert_eq!(
                    quotes.len(),
                    self.parent_count,
                    "typed complete-action quote did not cover every parent exactly once"
                );
                for (parent, quote) in quotes.into_iter().enumerate().rev() {
                    if !self.try_admit_tail_parent(parent, quote) {
                        break;
                    }
                }
                self.seal_tail_admission()
            }
        }
    }

    pub(crate) fn effects_mut(&mut self) -> &mut TypedCompleteSink {
        assert!(
            matches!(
                self.state,
                TypedCompleteArbiterState::LegacyUnquoted | TypedCompleteArbiterState::Exact { .. }
            ),
            "typed complete effects were requested before admission"
        );
        &mut self.effects
    }

    fn finish(mut self) -> Option<BoundedCompleteEffects> {
        let (first_parent, admission, quotes) = match self.state {
            TypedCompleteArbiterState::Pending | TypedCompleteArbiterState::Tail(_) => {
                panic!("typed complete action returned without sealing admission")
            }
            TypedCompleteArbiterState::Declined => return None,
            TypedCompleteArbiterState::LegacyUnquoted => {
                (0, ProgramCompleteAdmission::LegacyUnquoted, None)
            }
            TypedCompleteArbiterState::Exact {
                first_parent,
                drain_work_units,
                raw_occurrences,
                quotes,
            } => (
                first_parent,
                ProgramCompleteAdmission::Exact {
                    drain_work_units,
                    raw_occurrences,
                },
                Some(quotes),
            ),
        };
        let completed_parent_count = self.parent_count - first_parent;
        let mut previous = 0u32;
        for (position, &(parent, _)) in self.effects.occurrences.iter().enumerate() {
            assert!(
                (parent as usize) < completed_parent_count,
                "typed complete action parent tag is out of range"
            );
            assert!(
                position == 0 || parent >= previous,
                "typed complete action parent tags are not grouped in ascending order"
            );
            previous = parent;
        }

        let raw_occurrence_count = self.effects.occurrences.len();
        if let Some(quotes) = &quotes {
            assert_eq!(quotes.len(), completed_parent_count);
            let mut occurrence = 0usize;
            for (parent, quote) in quotes.iter().enumerate() {
                let begin = occurrence;
                while occurrence < raw_occurrence_count
                    && self.effects.occurrences[occurrence].0 as usize == parent
                {
                    occurrence += 1;
                }
                assert_eq!(
                    occurrence - begin,
                    quote.raw_occurrences,
                    "typed complete action disagreed with parent {parent}'s exact raw-occurrence quote"
                );
            }
            assert_eq!(
                occurrence, raw_occurrence_count,
                "typed complete action emitted an unquoted parent occurrence"
            );
        }

        let mut admitted = AHashSet::with_capacity(raw_occurrence_count);
        self.effects
            .occurrences
            .retain(|occurrence| admitted.insert(*occurrence));
        Some(BoundedCompleteEffects {
            first_parent,
            admission,
            raw_occurrence_count,
            occurrences: self.effects.occurrences,
        })
    }
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

/// One opaque work item plus its immutable parent context.
///
/// This is the scalar transaction form of [`ProgramBatch`]. It exists so the
/// latency path does not have to manufacture six one-element heap vectors
/// before crossing the same typed family boundary. Wider cohorts continue to
/// use the unchanged batch contract.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct ProgramOneBatch<'v> {
    pub stratum: ProgramStratum,
    pub view: RowsView<'v>,
    pub candidate_set: Option<&'v [RawInline]>,
    pub activation: ProgramActivation,
    pub work: &'v ProgramWork,
    pub limit: usize,
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

/// Diagnostic receipt naming the physical executor selected for one complete
/// typed Program cohort.
///
/// Placement is never semantic input. The scheduler may aggregate these
/// static labels in statistics, but route selection, novelty, affine
/// replacement, and future cohort identity must not consult them.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramPhysicalReceipt {
    pub executor: &'static str,
    pub operation: &'static str,
}

impl ProgramPhysicalReceipt {
    pub const fn new(executor: &'static str, operation: &'static str) -> Self {
        Self {
            executor,
            operation,
        }
    }
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
    /// Successful non-Native placement for this exact cohort. `None` denotes
    /// the ordinary typed implementation, including immediate fallback after
    /// a physical attempt declined or failed before effect commit.
    pub placement: Option<ProgramPhysicalReceipt>,
}

/// Effects returned by one scalar typed Program transaction.
///
/// The common zero/one cardinalities stay inline. A genuinely branching page
/// may spill without weakening the one-input transaction or changing the
/// existing many-input representation.
#[doc(hidden)]
#[derive(Default)]
pub struct ProgramOneEffects {
    pub page: Option<ProgramPage>,
    pub children: SmallVec<[ProgramChild; 1]>,
    pub direct: SmallVec<[RawInline; 1]>,
    pub accepted: SmallVec<[RawInline; 1]>,
    pub supported: bool,
    pub source_pages: usize,
    pub source_examined: usize,
    pub source_roots: usize,
    pub transition_pages: usize,
    pub transition_examined: usize,
    pub placement: Option<ProgramPhysicalReceipt>,
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

/// Handle-free context for one scalar typed Program transaction.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct TypedProgramOneBatch<'v> {
    pub stratum: ProgramStratum,
    pub view: RowsView<'v>,
    pub candidate_set: Option<&'v [RawInline]>,
    pub activation: ProgramActivation,
    pub limit: usize,
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

/// Typed scalar effect sink. Input tags are absent because the transaction
/// has exactly one input; the common RPQ corridor therefore remains entirely
/// stack-backed while a real branch can still spill.
#[doc(hidden)]
pub struct TypedOneEffectSink<State, NoveltyKey> {
    page: Option<TypedPage<State>>,
    children: SmallVec<[TypedChild<State, NoveltyKey>; 1]>,
    direct: SmallVec<[RawInline; 1]>,
    accepted: SmallVec<[RawInline; 1]>,
    supported: bool,
    source_pages: usize,
    source_examined: usize,
    source_roots: usize,
    transition_pages: usize,
    transition_examined: usize,
}

impl<State, NoveltyKey> Default for TypedOneEffectSink<State, NoveltyKey> {
    fn default() -> Self {
        Self {
            page: None,
            children: SmallVec::new(),
            direct: SmallVec::new(),
            accepted: SmallVec::new(),
            supported: false,
            source_pages: 0,
            source_examined: 0,
            source_roots: 0,
            transition_pages: 0,
            transition_examined: 0,
        }
    }
}

impl<State, NoveltyKey> TypedOneEffectSink<State, NoveltyKey> {
    pub fn reserve_children(&mut self, additional: usize) {
        self.children.reserve(additional);
    }

    pub fn page(&mut self, examined: usize, resume: Option<TypedResume<State>>) {
        assert!(
            self.page.replace(TypedPage { examined, resume }).is_none(),
            "one typed Program input returned more than one page"
        );
    }

    pub fn finite_child(&mut self, state: State, accepted: Option<RawInline>) {
        self.children.push(TypedChild {
            input: 0,
            state,
            novelty: None,
            accepted,
        });
    }

    pub fn fixpoint_child(
        &mut self,
        state: State,
        novelty: NoveltyKey,
        accepted: Option<RawInline>,
    ) {
        self.children.push(TypedChild {
            input: 0,
            state,
            novelty: Some(novelty),
            accepted,
        });
    }

    pub fn direct(&mut self, value: RawInline) {
        self.direct.push(value);
    }

    pub fn accept(&mut self, value: RawInline) {
        self.accepted.push(value);
    }

    pub fn support(&mut self) {
        assert!(
            !self.supported,
            "one typed input reported Boolean support twice"
        );
        self.supported = true;
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

    fn absorb_batch(&mut self, batch: TypedEffectSink<State, NoveltyKey>) {
        let TypedEffectSink {
            pages,
            children,
            direct,
            accepted,
            supported,
            source_pages,
            source_examined,
            source_roots,
            transition_pages,
            transition_examined,
        } = batch;
        assert_eq!(
            pages.len(),
            1,
            "typed scalar bridge returned the wrong page count"
        );
        self.page = pages.into_iter().next();
        for child in children {
            assert_eq!(
                child.input, 0,
                "typed scalar bridge returned a foreign child tag"
            );
            self.children.push(child);
        }
        for (input, value) in direct {
            assert_eq!(
                input, 0,
                "typed scalar bridge returned a foreign direct tag"
            );
            self.direct.push(value);
        }
        for (input, value) in accepted {
            assert_eq!(
                input, 0,
                "typed scalar bridge returned a foreign accepted tag"
            );
            self.accepted.push(value);
        }
        for (input, ()) in supported {
            assert_eq!(
                input, 0,
                "typed scalar bridge returned a foreign support tag"
            );
            assert!(
                !self.supported,
                "one typed input reported Boolean support twice"
            );
            self.supported = true;
        }
        self.source_pages += source_pages;
        self.source_examined += source_examined;
        self.source_roots += source_roots;
        self.transition_pages += transition_pages;
        self.transition_examined += transition_examined;
    }
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

/// Complete, still-uncommitted result of an optional physical Program step.
///
/// The owning family builds effects in this private transaction. Only after
/// this value is returned does the erased adapter apply the same page, budget,
/// rank, tag, novelty, and affine-handle checks as Native execution.
#[doc(hidden)]
#[must_use]
pub struct TypedPhysicalStep<State, NoveltyKey> {
    effects: TypedEffectSink<State, NoveltyKey>,
    placement: ProgramPhysicalReceipt,
}

impl<State, NoveltyKey> TypedPhysicalStep<State, NoveltyKey> {
    pub fn new(placement: ProgramPhysicalReceipt) -> Self {
        Self {
            effects: TypedEffectSink::default(),
            placement,
        }
    }

    pub fn effects_mut(&mut self) -> &mut TypedEffectSink<State, NoveltyKey> {
        &mut self.effects
    }

    fn into_parts(self) -> (TypedEffectSink<State, NoveltyKey>, ProgramPhysicalReceipt) {
        (self.effects, self.placement)
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

    /// Executes one typed state without requiring the engine to manufacture a
    /// one-element batch. Families may override this latency seam while the
    /// default bridge preserves the existing typed contract exactly.
    fn step_one_typed(
        &self,
        state: Self::State,
        batch: TypedProgramOneBatch<'_>,
        effects: &mut TypedOneEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        let candidate_sets = [batch.candidate_set];
        let activations = [batch.activation];
        let limits = [batch.limit];
        let mut bridged = TypedEffectSink::default();
        self.step_typed(
            vec![state],
            TypedProgramBatch {
                stratum: batch.stratum,
                view: batch.view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                limits: &limits,
            },
            &mut bridged,
        );
        effects.absorb_batch(bridged);
    }

    /// Attempts one already-formed cohort on a family-owned physical backend.
    ///
    /// The adapter calls this only after affinely taking and revalidating every
    /// canonical state. Inputs remain borrowed so `None` can immediately move
    /// the exact retained states into [`Self::step_typed`]. A successful result
    /// is still uncommitted and passes through the ordinary adapter checks.
    /// Implementations must return `None` rather than wait when their backend
    /// is unsupported, unavailable, still preparing, or fails recoverably.
    fn try_step_physical(
        &self,
        _states: &[Self::State],
        _batch: TypedProgramBatch<'_>,
    ) -> Option<TypedPhysicalStep<Self::State, Self::NoveltyKey>> {
        None
    }

    /// Executes one complete action certified by the selected route.
    ///
    /// V1 deliberately supports only complete Propose occurrence bags. A
    /// family that never returns [`ProgramCompletion::CompleteActionEquivalent`]
    /// need not implement this method.
    fn complete_typed(&self, _batch: ProgramCompleteBatch<'_>, _effects: &mut TypedCompleteSink) {
        panic!("typed Program certified a complete action without implementing it")
    }

    /// Returns exact parent-local physical admission evidence for `batch`.
    ///
    /// A quoted family must return exactly one quote per parent. The evidence
    /// is consumed privately by the erased adapter and is deliberately not
    /// passed back to [`Self::complete_typed`]. `Declined` performs no complete
    /// action work; `Unquoted` retains the pre-quote whole-batch policy.
    fn quote_complete_typed(
        &self,
        _batch: ProgramCompleteBatch<'_>,
    ) -> ProgramCompleteWorkEvidence {
        ProgramCompleteWorkEvidence::Unquoted
    }

    /// Runs the engine-owned bounded complete-action transaction.
    ///
    /// The default keeps legacy quote and completion hooks distinct. A
    /// crate-owned physical family may override this hidden seam to retain
    /// prepared work while the arbiter alone selects the admissible tail.
    #[doc(hidden)]
    fn complete_bounded_typed(
        &self,
        batch: ProgramCompleteBatch<'_>,
        arbiter: &mut TypedCompleteArbiter,
    ) {
        let Some(selection) = arbiter.try_admit_evidence(self.quote_complete_typed(batch)) else {
            return;
        };
        self.complete_typed(selection.tail(batch), arbiter.effects_mut());
    }
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

/// Left-biased composition of two typed Program capabilities.
///
/// The preferred family owns an action whenever its `route` returns `Some`;
/// only a structural decline consults the fallback family. The selected arm
/// is sealed into [`ProgramKey`] before the occurrence-local runtime exists.
/// Consequently each address constructs exactly one child-native runtime and
/// every affine resume remains in that child's unchanged typed state arena.
/// A physical decline therefore returns to the selected family's Native step,
/// never across the composition boundary.
///
/// V1 deliberately composes two direct [`TypedProgramSpec`] implementations,
/// not another `PreferredProgram`. A one-level arm tag is sufficient for this
/// law and cannot silently truncate a nested composition path.
#[doc(hidden)]
#[derive(Clone)]
pub struct PreferredProgram<Preferred, Fallback> {
    preferred: Preferred,
    fallback: Fallback,
}

impl<Preferred, Fallback> PreferredProgram<Preferred, Fallback> {
    pub fn new(preferred: Preferred, fallback: Fallback) -> Self {
        Self {
            preferred,
            fallback,
        }
    }

    pub fn preferred(&self) -> &Preferred {
        &self.preferred
    }

    pub fn fallback(&self) -> &Fallback {
        &self.fallback
    }
}

/// Immutable residual-program family specification.
///
/// Implementations downcast `runtime` once at the beginning of each seed or
/// step call, then operate on a dense typed state vector. Returning `None`
/// from `route` declines only that exact structural action, so the engine may
/// still consult the constraint's legacy residual capabilities. After a route
/// is returned, however, that action is owned by the Program and must never
/// fall back to legacy residual hooks.
trait ErasedProgramSpec {
    fn new_runtime(&self, key: ProgramKey) -> ProgramRuntime;

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
        key: ProgramKey,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    );

    fn step_one(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramOneBatch<'_>,
        effects: &mut ProgramOneEffects,
    );

    fn try_complete_bounded(
        &self,
        batch: ProgramCompleteBatch<'_>,
        capacity: usize,
    ) -> Option<BoundedCompleteEffects>;

    fn retire_activations(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        activations: &[ProgramActivation],
    );
}

struct BoundedCompleteEffects {
    first_parent: usize,
    admission: ProgramCompleteAdmission,
    raw_occurrence_count: usize,
    occurrences: Vec<(u32, RawInline)>,
}

/// Borrowed immutable typed program behind a private erased vtable.
///
/// Direct construction accepts [`TypedProgramSpec`]. Left-biased construction
/// accepts a [`PreferredProgram`] whose two children are themselves direct
/// typed specs. Neither path lets custom constraints bypass typed sinks,
/// activation ownership, or novelty admission.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ProgramRef<'a> {
    erased: &'a dyn ErasedProgramSpec,
}

impl<'a> ProgramRef<'a> {
    pub fn new<T: TypedProgramSpec>(spec: &'a T) -> Self {
        Self { erased: spec }
    }

    pub fn preferred<Preferred, Fallback>(spec: &'a PreferredProgram<Preferred, Fallback>) -> Self
    where
        Preferred: TypedProgramSpec,
        Fallback: TypedProgramSpec,
    {
        Self { erased: spec }
    }

    pub(crate) fn new_runtime(self) -> ProgramRuntime {
        self.new_runtime_for(ProgramKey::new(0))
    }

    pub(crate) fn new_runtime_for(self, key: ProgramKey) -> ProgramRuntime {
        self.erased.new_runtime(key)
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
        self.step_batch_for(runtime, ProgramKey::new(0), batch, effects);
    }

    pub(crate) fn step_batch_for(
        self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    ) {
        self.erased.step_batch(runtime, key, batch, effects);
    }

    pub(crate) fn step_one_for(
        self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramOneBatch<'_>,
        effects: &mut ProgramOneEffects,
    ) {
        self.erased.step_one(runtime, key, batch, effects);
    }

    #[cold]
    #[inline(never)]
    pub(crate) fn try_complete_bounded<'v, 'affinity, 'owner>(
        self,
        batch: ProgramCompleteBatch<'v>,
        capacity: usize,
        affinity: &'affinity ProgramCompleteAffinity<'owner>,
    ) -> Option<ProgramBoundedCompletion<'v, 'affinity, 'owner>> {
        self.erased
            .try_complete_bounded(batch, capacity)
            .map(|effects| ProgramBoundedCompletion {
                batch,
                affinity,
                first_parent: effects.first_parent,
                admission: effects.admission,
                raw_occurrence_count: effects.raw_occurrence_count,
                occurrences: effects.occurrences,
            })
    }

    #[cfg(test)]
    pub(crate) fn complete_batch(
        self,
        batch: ProgramCompleteBatch<'_>,
        effects: &mut ProgramCompleteEffects,
    ) {
        let owner = batch;
        let affinity = ProgramCompleteAffinity::new(&owner);
        let completion = self
            .try_complete_bounded(batch, usize::MAX, &affinity)
            .expect("test complete action declined");
        let (_, _, raw_occurrence_count, occurrences) =
            completion.into_parts_for(batch, &affinity, &owner);
        effects.raw_occurrence_count = effects
            .raw_occurrence_count
            .checked_add(raw_occurrence_count)
            .expect("typed complete action occurrence count overflow");
        effects.occurrences.extend(occurrences);
    }

    pub(crate) fn retire_activations(
        self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        activations: &[ProgramActivation],
    ) {
        self.erased.retire_activations(runtime, key, activations);
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
    #[cfg(test)]
    retirement_slot_probes: usize,
    #[cfg(test)]
    retirement_membership_builds: usize,
}

impl<State, NoveltyKey> Default for TypedProgramRuntime<State, NoveltyKey> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            novelty: AHashMap::new(),
            #[cfg(test)]
            retirement_slot_probes: 0,
            #[cfg(test)]
            retirement_membership_builds: 0,
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
        TypeId::of::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
        "engine Program state expected family {}, received {}",
        type_name::<TypedProgramRuntime<T::State, T::NoveltyKey>>(),
        runtime.family_name
    );
    let dispatch = spec.dispatch(&state);
    let pacing = spec.pacing(&state);
    let runtime = runtime
        .erased
        .as_mut()
        .as_any_mut()
        .downcast_mut::<TypedProgramRuntime<T::State, T::NoveltyKey>>()
        .expect("engine Program state received another family's runtime");
    let handle = runtime.insert(activation, state);
    ProgramWork {
        handle,
        dispatch,
        pacing,
    }
}

impl<Preferred, Fallback> PreferredProgram<Preferred, Fallback>
where
    Preferred: TypedProgramSpec,
    Fallback: TypedProgramSpec,
{
    fn selected(&self, key: ProgramKey) -> (&dyn ErasedProgramSpec, ProgramKey) {
        match key.arm {
            ProgramRouteArm::Preferred => (&self.preferred, key.direct()),
            ProgramRouteArm::Fallback => (&self.fallback, key.direct()),
            ProgramRouteArm::Direct => {
                panic!("composed Program address lost its selected semantic arm")
            }
        }
    }
}

impl<Preferred, Fallback> ErasedProgramSpec for PreferredProgram<Preferred, Fallback>
where
    Preferred: TypedProgramSpec,
    Fallback: TypedProgramSpec,
{
    fn new_runtime(&self, key: ProgramKey) -> ProgramRuntime {
        let (selected, child_key) = self.selected(key);
        selected.new_runtime(child_key)
    }

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        if let Some(mut route) = TypedProgramSpec::route(&self.preferred, request) {
            route.key = route.key.in_arm(ProgramRouteArm::Preferred);
            Some(route)
        } else {
            TypedProgramSpec::route(&self.fallback, request).map(|mut route| {
                route.key = route.key.in_arm(ProgramRouteArm::Fallback);
                route
            })
        }
    }

    fn seed_batch(
        &self,
        runtime: &mut ProgramRuntime,
        batch: ProgramSeedBatch<'_>,
        effects: &mut ProgramSeedEffects,
    ) {
        let (selected, child_key) = self.selected(batch.route.key);
        selected.seed_batch(
            runtime,
            ProgramSeedBatch {
                route: ProgramRoute {
                    key: child_key,
                    ..batch.route
                },
                ..batch
            },
            effects,
        );
    }

    fn step_batch(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    ) {
        let (selected, child_key) = self.selected(key);
        selected.step_batch(runtime, child_key, batch, effects);
    }

    fn step_one(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramOneBatch<'_>,
        effects: &mut ProgramOneEffects,
    ) {
        let (selected, child_key) = self.selected(key);
        selected.step_one(runtime, child_key, batch, effects);
    }

    #[cold]
    #[inline(never)]
    fn try_complete_bounded(
        &self,
        batch: ProgramCompleteBatch<'_>,
        capacity: usize,
    ) -> Option<BoundedCompleteEffects> {
        let (selected, child_key) = self.selected(batch.route.key);
        selected.try_complete_bounded(
            ProgramCompleteBatch {
                route: ProgramRoute {
                    key: child_key,
                    ..batch.route
                },
                ..batch
            },
            capacity,
        )
    }

    fn retire_activations(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        activations: &[ProgramActivation],
    ) {
        let (selected, child_key) = self.selected(key);
        selected.retire_activations(runtime, child_key, activations);
    }
}

impl<T> ErasedProgramSpec for T
where
    T: TypedProgramSpec,
{
    fn new_runtime(&self, key: ProgramKey) -> ProgramRuntime {
        assert_eq!(
            key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program received a composed route arm"
        );
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
        assert_eq!(
            batch.route.key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program seed received a composed route arm"
        );
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

    fn step_one(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramOneBatch<'_>,
        effects: &mut ProgramOneEffects,
    ) {
        assert_eq!(
            key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program step received a composed route arm"
        );
        assert!(effects.page.is_none(), "one Program effect sink was reused");
        assert!(effects.children.is_empty());
        assert!(effects.direct.is_empty());
        assert!(effects.accepted.is_empty());
        assert!(!effects.supported);
        assert!(effects.placement.is_none());
        assert_eq!(batch.view.len(), 1);
        assert!(batch.limit > 0);

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
        let state = runtime.take(batch.activation, batch.work.handle.clone());
        let input_rank = self.progress(&state);
        assert_eq!(
            self.dispatch(&state),
            batch.work.dispatch,
            "typed program work entered an incompatible dispatch cohort"
        );
        assert_eq!(
            self.pacing(&state),
            batch.work.pacing,
            "typed program work entered an incompatible pacing cohort"
        );

        let candidate_sets = [batch.candidate_set];
        let activations = [batch.activation];
        let limits = [batch.limit];
        let typed_batch = TypedProgramOneBatch {
            stratum: batch.stratum,
            view: batch.view,
            candidate_set: batch.candidate_set,
            activation: batch.activation,
            limit: batch.limit,
        };
        let mut typed = TypedOneEffectSink::default();
        let placement = if let Some(physical) = self.try_step_physical(
            std::slice::from_ref(&state),
            TypedProgramBatch {
                stratum: batch.stratum,
                view: batch.view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                limits: &limits,
            },
        ) {
            let (physical, placement) = physical.into_parts();
            typed.absorb_batch(physical);
            Some(placement)
        } else {
            self.step_one_typed(state, typed_batch, &mut typed);
            None
        };

        let page = typed
            .page
            .as_ref()
            .expect("typed scalar program returned no page");
        assert!(
            page.examined <= batch.limit,
            "typed program exceeded one input's physical work budget"
        );
        let resume_physical = match &page.resume {
            Some(TypedResume::Immediate(state) | TypedResume::AfterChildren(state)) => {
                assert!(
                    self.progress(state) < input_rank,
                    "typed program resume did not strictly decrease its finite rank"
                );
                Some((self.dispatch(state), self.pacing(state)))
            }
            Some(TypedResume::AfterChildrenDone) | None => None,
        };

        let raw_effects = typed
            .children
            .len()
            .checked_add(typed.direct.len())
            .and_then(|count| count.checked_add(typed.accepted.len()))
            .and_then(|count| count.checked_add(usize::from(typed.supported)))
            .expect("typed scalar effect count overflow");
        assert!(
            raw_effects <= page.examined,
            "typed program emitted more raw effects than its examined-work receipt"
        );

        // Validate the complete scalar receipt before publishing novelty,
        // replacement handles, or outward effects. Linear duplicate probing
        // is allocation-free and appropriate for a one-input page; a real
        // wide fanout has already crossed the SmallVec spill boundary.
        let mut child_admitted: SmallVec<[bool; 1]> = SmallVec::new();
        for (position, child) in typed.children.iter().enumerate() {
            assert_eq!(child.input, 0);
            assert!(
                batch.stratum == ProgramStratum::Fixpoint || child.novelty.is_none(),
                "a finite typed program emitted a fixpoint child"
            );
            if child.novelty.is_none() {
                assert!(
                    self.progress(&child.state) < input_rank,
                    "typed program finite child did not strictly decrease its input rank"
                );
            }
            let _ = self.dispatch(&child.state);
            let _ = self.pacing(&child.state);

            let admitted = if let Some(novelty) = child.novelty.as_ref() {
                let prior = typed.children[..position]
                    .iter()
                    .find(|prior| prior.novelty.as_ref() == Some(novelty));
                if let Some(prior) = prior {
                    assert_eq!(
                        prior.accepted, child.accepted,
                        "one typed novelty key changed its endpoint observation"
                    );
                    false
                } else {
                    match runtime
                        .novelty
                        .get(&batch.activation)
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
                    }
                }
            } else {
                true
            };
            child_admitted.push(admitted);
        }

        let TypedOneEffectSink {
            page,
            children,
            direct,
            accepted,
            supported,
            source_pages,
            source_examined,
            source_roots,
            transition_pages,
            transition_examined,
        } = typed;
        let page = page.expect("typed scalar program page vanished after validation");
        let resume = match (page.resume, resume_physical) {
            (Some(TypedResume::Immediate(state)), Some((dispatch, pacing))) => {
                Some(ProgramResume::Immediate(ProgramWork {
                    handle: runtime.insert(batch.activation, state),
                    dispatch,
                    pacing,
                }))
            }
            (Some(TypedResume::AfterChildren(state)), Some((dispatch, pacing))) => {
                Some(ProgramResume::AfterChildren(ProgramWork {
                    handle: runtime.insert(batch.activation, state),
                    dispatch,
                    pacing,
                }))
            }
            (Some(TypedResume::AfterChildrenDone), None) => Some(ProgramResume::AfterChildrenDone),
            (None, None) => None,
            _ => unreachable!("typed Program resume preflight lost alignment"),
        };
        effects.page = Some(ProgramPage {
            examined: page.examined,
            resume,
        });

        for (child, admitted) in children.into_iter().zip(child_admitted) {
            if !admitted {
                continue;
            }
            if let Some(novelty) = child.novelty {
                let previous = runtime
                    .novelty
                    .entry(batch.activation)
                    .or_default()
                    .insert(novelty, child.accepted);
                assert!(
                    previous.is_none(),
                    "typed novelty preflight admitted an existing key"
                );
            }
            let dispatch = self.dispatch(&child.state);
            let pacing = self.pacing(&child.state);
            effects.children.push(ProgramChild {
                input: 0,
                work: ProgramWork {
                    handle: runtime.insert(batch.activation, child.state),
                    dispatch,
                    pacing,
                },
                accepted: child.accepted,
            });
        }
        effects.direct.extend(direct);
        effects.accepted.extend(accepted);
        effects.supported = supported;
        effects.source_pages += source_pages;
        effects.source_examined += source_examined;
        effects.source_roots += source_roots;
        effects.transition_pages += transition_pages;
        effects.transition_examined += transition_examined;
        effects.placement = placement;
    }

    fn step_batch(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        batch: ProgramBatch<'_>,
        effects: &mut ProgramBatchEffects,
    ) {
        assert_eq!(
            key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program step received a composed route arm"
        );
        assert!(
            effects.placement.is_none(),
            "one ProgramBatchEffects sink received more than one physical placement"
        );
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
        let (typed, placement) = match self.try_step_physical(&states, typed_batch) {
            Some(physical) => {
                let (effects, placement) = physical.into_parts();
                (effects, Some(placement))
            }
            None => {
                let mut typed = TypedEffectSink::default();
                self.step_typed(states, typed_batch, &mut typed);
                (typed, None)
            }
        };
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

        // Validate the entire typed receipt before publishing any replacement
        // handle, novelty admission, or outward effect. A physical `Some`
        // result is a transaction candidate, not permission to commit a valid
        // prefix before a later malformed effect is discovered.
        let mut resume_physical = Vec::with_capacity(input_count);
        for (input, page) in typed.pages.iter().enumerate() {
            match &page.resume {
                Some(TypedResume::Immediate(state) | TypedResume::AfterChildren(state)) => {
                    assert!(
                        self.progress(state) < input_ranks[input],
                        "typed program resume did not strictly decrease its finite rank"
                    );
                    resume_physical.push(Some((self.dispatch(state), self.pacing(state))));
                }
                Some(TypedResume::AfterChildrenDone) | None => resume_physical.push(None),
            }
        }

        let mut batch_novelty: AHashMap<(ProgramActivation, &T::NoveltyKey), Option<RawInline>> =
            AHashMap::new();
        // The bitmap is a receipt-local transaction plan. Repetitions consult
        // the batch observation first, so only the first exact key reads the
        // runtime. Neither map nor handles are mutated until every receipt law
        // below has validated.
        let mut child_admitted = Vec::with_capacity(typed.children.len());
        let mut child_physical = Vec::with_capacity(typed.children.len());
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
            child_physical.push((self.dispatch(&child.state), self.pacing(&child.state)));

            let admitted = if let Some(novelty) = child.novelty.as_ref() {
                let activation = batch.activations[child.input as usize];
                match batch_novelty.entry((activation, novelty)) {
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
            child_admitted.push(admitted);
        }

        let mut previous = 0u32;
        for (position, (input, _)) in typed.direct.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed direct observations are not grouped in ascending order"
            );
            previous = *input;
            raw_effects[*input as usize] += 1;
        }
        let mut previous = 0u32;
        for (position, (input, _)) in typed.accepted.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed candidate observations are not grouped in ascending order"
            );
            previous = *input;
            raw_effects[*input as usize] += 1;
        }
        let mut previous = 0u32;
        for (position, (input, ())) in typed.supported.iter().enumerate() {
            assert!((*input as usize) < input_count);
            assert!(
                position == 0 || *input >= previous,
                "typed support observations are not grouped in ascending order"
            );
            previous = *input;
            raw_effects[*input as usize] += 1;
        }
        assert!(
            raw_effects
                .iter()
                .zip(&examined)
                .all(|(&outputs, &spent)| outputs <= spent),
            "typed program emitted more raw effects than its examined-work receipt"
        );

        drop(batch_novelty);
        let TypedEffectSink {
            pages,
            children,
            direct,
            accepted,
            supported,
            source_pages,
            source_examined,
            source_roots,
            transition_pages,
            transition_examined,
        } = typed;

        // From here onward every family-derived value and every static receipt
        // law has already been checked. Allocation failure, generation
        // exhaustion, or another panic remains fatal and non-rollback, exactly
        // like the affine input take above; recoverable backends return `None`
        // before reaching this commit phase.
        effects
            .pages
            .extend(pages.into_iter().zip(resume_physical).enumerate().map(
                |(input, (page, physical))| {
                    let activation = batch.activations[input];
                    let resume = match (page.resume, physical) {
                        (Some(TypedResume::Immediate(state)), Some((dispatch, pacing))) => {
                            let work = ProgramWork {
                                handle: runtime.insert(activation, state),
                                dispatch,
                                pacing,
                            };
                            Some(ProgramResume::Immediate(work))
                        }
                        (Some(TypedResume::AfterChildren(state)), Some((dispatch, pacing))) => {
                            let work = ProgramWork {
                                handle: runtime.insert(activation, state),
                                dispatch,
                                pacing,
                            };
                            Some(ProgramResume::AfterChildren(work))
                        }
                        (Some(TypedResume::AfterChildrenDone), None) => {
                            Some(ProgramResume::AfterChildrenDone)
                        }
                        (None, None) => None,
                        _ => unreachable!("typed Program resume preflight lost alignment"),
                    };
                    ProgramPage {
                        examined: page.examined,
                        resume,
                    }
                },
            ));

        for ((child, (dispatch, pacing)), admitted) in
            children.into_iter().zip(child_physical).zip(child_admitted)
        {
            if !admitted {
                continue;
            }
            let activation = batch.activations[child.input as usize];
            if let Some(novelty) = child.novelty {
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

        effects.direct.extend(direct);
        effects.accepted.extend(accepted);
        effects.supported.extend(supported);
        effects.source_pages += source_pages;
        effects.source_examined += source_examined;
        effects.source_roots += source_roots;
        effects.transition_pages += transition_pages;
        effects.transition_examined += transition_examined;
        effects.placement = placement;
    }

    #[cold]
    #[inline(never)]
    fn try_complete_bounded(
        &self,
        batch: ProgramCompleteBatch<'_>,
        capacity: usize,
    ) -> Option<BoundedCompleteEffects> {
        assert_eq!(
            batch.route.key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program completion received a composed route arm"
        );
        assert!(
            matches!(batch.request.action, ProgramAction::Propose(_)),
            "typed complete actions currently support only Propose"
        );
        let ProgramAction::Propose(variable) = batch.request.action else {
            unreachable!()
        };
        assert_eq!(
            variable, batch.route.variable,
            "typed complete action route changed its proposal variable"
        );
        assert_eq!(
            batch.route.completion,
            ProgramCompletion::CompleteActionEquivalent,
            "typed complete action lacked an equivalence certificate"
        );
        assert_eq!(
            TypedProgramSpec::route(self, batch.request),
            Some(batch.route),
            "typed complete action route was not pure for its request"
        );
        let mut view_bound = VariableSet::new_empty();
        for &bound in batch.view.vars {
            assert!(
                !view_bound.is_set(bound),
                "typed complete action view repeated a bound variable"
            );
            view_bound.set(bound);
        }
        assert_eq!(
            view_bound, batch.request.bound,
            "typed complete action view disagreed with its bound schema"
        );
        assert!(
            batch.view.col(variable).is_none(),
            "typed complete action proposal variable was already bound"
        );

        let mut arbiter = TypedCompleteArbiter::new(batch.view.len(), capacity);
        self.complete_bounded_typed(batch, &mut arbiter);
        arbiter.finish()
    }

    fn retire_activations(
        &self,
        runtime: &mut ProgramRuntime,
        key: ProgramKey,
        activations: &[ProgramActivation],
    ) {
        assert_eq!(
            key.arm,
            ProgramRouteArm::Direct,
            "a direct typed Program retirement received a composed route arm"
        );
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
        runtime.retire_activations(activations);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn raw(byte: u8) -> RawInline {
        let mut value = RawInline::default();
        value[0] = byte;
        value
    }

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
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
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

    #[derive(Clone, Copy)]
    enum PhysicalProbeMode {
        Decline,
        Complete,
        OverBudget,
        LateRawAmplification,
    }

    struct PhysicalProbe {
        mode: PhysicalProbeMode,
        physical_states: Arc<Mutex<Vec<Vec<usize>>>>,
        native_states: Arc<Mutex<Vec<Vec<usize>>>>,
    }

    impl PhysicalProbe {
        fn new(mode: PhysicalProbeMode) -> Self {
            Self {
                mode,
                physical_states: Arc::new(Mutex::new(Vec::new())),
                native_states: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl TypedProgramSpec for PhysicalProbe {
        type State = NonComparableState;
        type NoveltyKey = Key;
        type Rank = u64;

        fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
            Some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: if matches!(self.mode, PhysicalProbeMode::LateRawAmplification) {
                    ProgramStratum::Fixpoint
                } else {
                    ProgramStratum::Finite
                },
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
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
                let state = NonComparableState {
                    exact_cursor: parent + 10,
                };
                if matches!(self.mode, PhysicalProbeMode::LateRawAmplification) {
                    effects.fixpoint_root(parent as u32, state, Key(parent as u8), None);
                } else {
                    effects.finite_root(parent as u32, state, None);
                }
            }
        }

        fn step_typed(
            &self,
            states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.native_states
                .lock()
                .unwrap()
                .push(states.iter().map(|state| state.exact_cursor).collect());
            for _ in states {
                effects.page(1, None);
            }
        }

        fn try_step_physical(
            &self,
            states: &[Self::State],
            batch: TypedProgramBatch<'_>,
        ) -> Option<TypedPhysicalStep<Self::State, Self::NoveltyKey>> {
            self.physical_states
                .lock()
                .unwrap()
                .push(states.iter().map(|state| state.exact_cursor).collect());
            match self.mode {
                PhysicalProbeMode::Decline => None,
                PhysicalProbeMode::Complete
                | PhysicalProbeMode::OverBudget
                | PhysicalProbeMode::LateRawAmplification => {
                    let mut step = TypedPhysicalStep::new(ProgramPhysicalReceipt::new(
                        "test-physical",
                        "dense-page",
                    ));
                    for (input, state) in states.iter().enumerate() {
                        let examined = match self.mode {
                            PhysicalProbeMode::Complete => 1,
                            PhysicalProbeMode::OverBudget => batch.limits[input] + 1,
                            PhysicalProbeMode::LateRawAmplification => 1,
                            PhysicalProbeMode::Decline => unreachable!(),
                        };
                        let resume = matches!(self.mode, PhysicalProbeMode::LateRawAmplification)
                            .then(|| {
                                TypedResume::Immediate(NonComparableState {
                                    exact_cursor: state.exact_cursor - 1,
                                })
                            });
                        step.effects_mut().page(examined, resume);
                        if matches!(self.mode, PhysicalProbeMode::LateRawAmplification) {
                            step.effects_mut().fixpoint_child(
                                input as u32,
                                NonComparableState {
                                    exact_cursor: state.exact_cursor - 2,
                                },
                                Key(input as u8 + 64),
                                None,
                            );
                            step.effects_mut()
                                .direct(input as u32, RawInline::default());
                        }
                    }
                    Some(step)
                }
            }
        }
    }

    #[derive(Clone)]
    struct PreferredChoiceState;

    #[derive(Clone)]
    struct FallbackChoiceState;

    #[derive(Clone, Eq, Hash, PartialEq)]
    struct PreferredChoiceKey;

    #[derive(Clone, Eq, Hash, PartialEq)]
    struct FallbackChoiceKey;

    #[derive(Default)]
    struct ChoiceCalls {
        preferred_physical: usize,
        preferred_native: usize,
        fallback_native: usize,
        preferred_quote: usize,
        preferred_complete: usize,
    }

    struct PreferredChoiceProbe {
        calls: Arc<Mutex<ChoiceCalls>>,
    }

    impl TypedProgramSpec for PreferredChoiceProbe {
        type State = PreferredChoiceState;
        type NoveltyKey = PreferredChoiceKey;
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            let ProgramAction::Propose(variable) = request.action else {
                return None;
            };
            if variable > 1 {
                return None;
            }
            Some(ProgramRoute {
                key: ProgramKey::new(7),
                variable,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
                completion: if variable == 1 {
                    ProgramCompletion::CompleteActionEquivalent
                } else {
                    ProgramCompletion::PageableOnly
                },
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(70)
        }

        fn progress(&self, _state: &Self::State) -> Self::Rank {
            1
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(batch.route.key, ProgramKey::new(7));
            for parent in 0..batch.view.len() {
                effects.finite_root(parent as u32, PreferredChoiceState, None);
            }
        }

        fn step_typed(
            &self,
            states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.calls.lock().unwrap().preferred_native += 1;
            for _ in states {
                effects.page(1, None);
            }
        }

        fn try_step_physical(
            &self,
            _states: &[Self::State],
            _batch: TypedProgramBatch<'_>,
        ) -> Option<TypedPhysicalStep<Self::State, Self::NoveltyKey>> {
            self.calls.lock().unwrap().preferred_physical += 1;
            None
        }

        fn complete_typed(&self, batch: ProgramCompleteBatch<'_>, effects: &mut TypedCompleteSink) {
            assert_eq!(batch.route.key, ProgramKey::new(7));
            self.calls.lock().unwrap().preferred_complete += 1;
            for parent in 0..batch.view.len() {
                effects.push(parent as u32, RawInline::default());
            }
        }

        fn quote_complete_typed(
            &self,
            batch: ProgramCompleteBatch<'_>,
        ) -> ProgramCompleteWorkEvidence {
            assert_eq!(batch.route.key, ProgramKey::new(7));
            self.calls.lock().unwrap().preferred_quote += 1;
            ProgramCompleteWorkEvidence::Quoted(vec![
                ProgramCompleteWorkQuote {
                    drain_work_units: 1,
                    raw_occurrences: 1,
                };
                batch.view.len()
            ])
        }
    }

    struct FallbackChoiceProbe {
        calls: Arc<Mutex<ChoiceCalls>>,
    }

    impl TypedProgramSpec for FallbackChoiceProbe {
        type State = FallbackChoiceState;
        type NoveltyKey = FallbackChoiceKey;
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            let ProgramAction::Confirm(variable) = request.action else {
                return None;
            };
            Some(ProgramRoute {
                // Deliberately collides with the preferred family's local
                // key. The private semantic arm keeps the addresses distinct.
                key: ProgramKey::new(7),
                variable,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(71)
        }

        fn progress(&self, _state: &Self::State) -> Self::Rank {
            1
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(batch.route.key, ProgramKey::new(7));
            for parent in 0..batch.view.len() {
                effects.finite_root(parent as u32, FallbackChoiceState, None);
            }
        }

        fn step_typed(
            &self,
            states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.calls.lock().unwrap().fallback_native += 1;
            for _ in states {
                effects.page(1, None);
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
            Some(ProgramRoute {
                key: ProgramKey::new(13),
                variable: 0,
                stratum: ProgramStratum::Fixpoint,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
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
            _states: Vec<Self::State>,
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
            Some(ProgramRoute {
                key: ProgramKey::new(14),
                variable: 0,
                stratum: ProgramStratum::Fixpoint,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
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
            states: Vec<Self::State>,
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

    #[derive(Clone, Copy)]
    enum CompleteTagProbe {
        RepeatedInOrder,
        MisattributedQuote,
        OutOfRange,
        Descending,
    }

    impl TypedProgramSpec for CompleteTagProbe {
        type State = ();
        type NoveltyKey = ();
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Propose(1)).then_some(ProgramRoute {
                key: ProgramKey::new(7),
                variable: 1,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::CompleteActionEquivalent,
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(0)
        }

        fn progress(&self, _state: &Self::State) -> Self::Rank {
            0
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
        }

        fn step_typed(
            &self,
            _states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            _effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
        }

        fn complete_typed(
            &self,
            _batch: ProgramCompleteBatch<'_>,
            effects: &mut TypedCompleteSink,
        ) {
            match self {
                Self::RepeatedInOrder | Self::MisattributedQuote => {
                    effects.push(0, raw(1));
                    effects.push(0, raw(2));
                    effects.push(0, raw(1));
                    effects.push(1, raw(1));
                    effects.push(1, raw(1));
                }
                Self::OutOfRange => effects.push(2, raw(1)),
                Self::Descending => {
                    effects.push(1, raw(1));
                    effects.push(0, raw(2));
                }
            }
        }

        fn quote_complete_typed(
            &self,
            _batch: ProgramCompleteBatch<'_>,
        ) -> ProgramCompleteWorkEvidence {
            match self {
                Self::MisattributedQuote => ProgramCompleteWorkEvidence::Quoted(vec![
                    ProgramCompleteWorkQuote {
                        drain_work_units: 2,
                        raw_occurrences: 2,
                    },
                    ProgramCompleteWorkQuote {
                        drain_work_units: 3,
                        raw_occurrences: 3,
                    },
                ]),
                Self::RepeatedInOrder | Self::OutOfRange | Self::Descending => {
                    ProgramCompleteWorkEvidence::Unquoted
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum BoundedQuoteMode {
        Quoted(&'static [(usize, usize)]),
        Declined,
    }

    #[derive(Default)]
    struct BoundedQuoteCalls {
        quotes: usize,
        completions: usize,
        completed_rows: Vec<u8>,
    }

    struct BoundedQuoteProbe {
        mode: BoundedQuoteMode,
        calls: Arc<Mutex<BoundedQuoteCalls>>,
    }

    impl TypedProgramSpec for BoundedQuoteProbe {
        type State = ();
        type NoveltyKey = ();
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Propose(1)).then_some(ProgramRoute {
                key: ProgramKey::new(11),
                variable: 1,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::CompleteActionEquivalent,
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(0)
        }

        fn progress(&self, _state: &Self::State) -> Self::Rank {
            0
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
        }

        fn step_typed(
            &self,
            _states: Vec<Self::State>,
            _batch: TypedProgramBatch<'_>,
            _effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
        }

        fn complete_typed(&self, batch: ProgramCompleteBatch<'_>, effects: &mut TypedCompleteSink) {
            let mut calls = self.calls.lock().unwrap();
            calls.completions += 1;
            calls.completed_rows = batch
                .view
                .iter()
                .map(|row| row.first().map_or(0, |value| value[0]))
                .collect();
            drop(calls);
            for (parent, row) in batch.view.iter().enumerate() {
                effects.push(parent as u32, row.first().copied().unwrap_or_default());
            }
        }

        fn quote_complete_typed(
            &self,
            batch: ProgramCompleteBatch<'_>,
        ) -> ProgramCompleteWorkEvidence {
            self.calls.lock().unwrap().quotes += 1;
            match self.mode {
                BoundedQuoteMode::Quoted(quotes) => {
                    assert_eq!(quotes.len(), batch.view.len());
                    ProgramCompleteWorkEvidence::Quoted(
                        quotes
                            .iter()
                            .map(
                                |&(drain_work_units, raw_occurrences)| ProgramCompleteWorkQuote {
                                    drain_work_units,
                                    raw_occurrences,
                                },
                            )
                            .collect(),
                    )
                }
                BoundedQuoteMode::Declined => ProgramCompleteWorkEvidence::Declined,
            }
        }
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
            Some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: ProgramStratum::Finite,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
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
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
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
    fn complete_adapter_set_admits_after_validating_the_raw_occurrence_bag() {
        let request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_singleton(0),
        };
        let vars = [0];
        let rows = [RawInline::default(), RawInline::default()];
        let view = RowsView::new(&vars, &rows);

        let valid = CompleteTagProbe::RepeatedInOrder;
        let program = ProgramRef::new(&valid);
        let route = program.route(request).unwrap();
        let mut effects = ProgramCompleteEffects::default();
        program.complete_batch(
            ProgramCompleteBatch {
                request,
                route,
                view,
            },
            &mut effects,
        );
        assert_eq!(effects.occurrences, [(0, raw(1)), (0, raw(2)), (1, raw(1))]);
        assert_eq!(effects.raw_occurrence_count, 5);

        let misattributed = CompleteTagProbe::MisattributedQuote;
        let program = ProgramRef::new(&misattributed);
        let route = program.route(request).unwrap();
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.complete_batch(
                ProgramCompleteBatch {
                    request,
                    route,
                    view,
                },
                &mut ProgramCompleteEffects::default(),
            );
        }));
        assert!(
            panic_text(rejected.expect_err("misattributed parent quotes must fail closed"))
                .contains("parent 0's exact raw-occurrence quote")
        );

        for attack in [CompleteTagProbe::OutOfRange, CompleteTagProbe::Descending] {
            let program = ProgramRef::new(&attack);
            let route = program.route(request).unwrap();
            let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                program.complete_batch(
                    ProgramCompleteBatch {
                        request,
                        route,
                        view,
                    },
                    &mut ProgramCompleteEffects::default(),
                );
            }));
            let message = panic_text(rejected.expect_err("bad parent tags must fail closed"));
            match attack {
                CompleteTagProbe::OutOfRange => assert!(message.contains("out of range")),
                CompleteTagProbe::Descending => {
                    assert!(message.contains("not grouped in ascending order"))
                }
                CompleteTagProbe::RepeatedInOrder | CompleteTagProbe::MisattributedQuote => {
                    unreachable!()
                }
            }
        }
    }

    #[test]
    fn bounded_completion_selects_the_maximal_tail_under_both_exact_bounds() {
        static DRAIN_LIMITED: &[(usize, usize)] = &[(9, 1), (2, 1), (1, 1), (1, 1)];
        static RAW_LIMITED: &[(usize, usize)] = &[(1, 1), (1, 3), (1, 1), (1, 1)];
        static OVERFLOW_BOUNDARY: &[(usize, usize)] = &[(usize::MAX, 1), (1, 1), (1, 1)];
        static RAW_OVERFLOW_BOUNDARY: &[(usize, usize)] = &[(1, usize::MAX), (1, 1), (1, 1)];

        for (quotes, capacity, expected_first, expected_drain) in [
            (DRAIN_LIMITED, 4, 1, 4),
            (RAW_LIMITED, 3, 2, 2),
            (OVERFLOW_BOUNDARY, 2, 1, 2),
            (RAW_OVERFLOW_BOUNDARY, 2, 1, 2),
        ] {
            let calls = Arc::new(Mutex::new(BoundedQuoteCalls::default()));
            let probe = BoundedQuoteProbe {
                mode: BoundedQuoteMode::Quoted(quotes),
                calls: Arc::clone(&calls),
            };
            let program = ProgramRef::new(&probe);
            let request = ProgramRequest {
                action: ProgramAction::Propose(1),
                bound: VariableSet::new_singleton(0),
            };
            let vars = [0];
            let rows: Vec<_> = (0..quotes.len())
                .map(|offset| raw(10 + offset as u8))
                .collect();
            let batch = ProgramCompleteBatch {
                request,
                route: program.route(request).unwrap(),
                view: RowsView::new(&vars, &rows),
            };
            let affinity = ProgramCompleteAffinity::new(&rows);
            let completion = program
                .try_complete_bounded(batch, capacity, &affinity)
                .expect("a multi-parent exact tail fits");
            let (first, admission, raw_occurrence_count, occurrences) =
                completion.into_parts_for(batch, &affinity, &rows);
            assert_eq!(first, expected_first);
            assert_eq!(
                admission,
                ProgramCompleteAdmission::Exact {
                    drain_work_units: expected_drain,
                    raw_occurrences: quotes.len() - expected_first,
                }
            );
            assert_eq!(raw_occurrence_count, quotes.len() - expected_first);
            assert_eq!(
                occurrences,
                rows[expected_first..]
                    .iter()
                    .enumerate()
                    .map(|(parent, value)| (parent as u32, *value))
                    .collect::<Vec<_>>()
            );
            let calls = calls.lock().unwrap();
            assert_eq!((calls.quotes, calls.completions), (1, 1));
            assert_eq!(
                calls.completed_rows,
                rows[expected_first..]
                    .iter()
                    .map(|value| value[0])
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn bounded_completion_decline_and_singleton_do_no_complete_work() {
        static SINGLETON: &[(usize, usize)] = &[(1, 1), (5, 1), (1, 1)];
        static OVERFLOW_SINGLETON: &[(usize, usize)] = &[(1, 1), (usize::MAX, 1), (1, 1)];

        for (mode, capacity) in [
            (BoundedQuoteMode::Declined, 8),
            (BoundedQuoteMode::Quoted(SINGLETON), 1),
            (BoundedQuoteMode::Quoted(OVERFLOW_SINGLETON), 2),
        ] {
            let calls = Arc::new(Mutex::new(BoundedQuoteCalls::default()));
            let probe = BoundedQuoteProbe {
                mode,
                calls: Arc::clone(&calls),
            };
            let program = ProgramRef::new(&probe);
            let request = ProgramRequest {
                action: ProgramAction::Propose(1),
                bound: VariableSet::new_singleton(0),
            };
            let vars = [0];
            let rows = [raw(10), raw(11), raw(12)];
            let batch = ProgramCompleteBatch {
                request,
                route: program.route(request).unwrap(),
                view: RowsView::new(&vars, &rows),
            };
            let affinity = ProgramCompleteAffinity::new(&rows);
            assert!(program
                .try_complete_bounded(batch, capacity, &affinity)
                .is_none());
            let calls = calls.lock().unwrap();
            assert_eq!((calls.quotes, calls.completions), (1, 0));
            assert!(calls.completed_rows.is_empty());
        }
    }

    #[test]
    fn bounded_completion_result_rejects_an_equal_width_foreign_batch() {
        static QUOTES: &[(usize, usize)] = &[(1, 1), (1, 1)];
        let probe = BoundedQuoteProbe {
            mode: BoundedQuoteMode::Quoted(QUOTES),
            calls: Arc::new(Mutex::new(BoundedQuoteCalls::default())),
        };
        let program = ProgramRef::new(&probe);
        let request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_singleton(0),
        };
        let vars = [0];
        let original_rows = [raw(10), raw(11)];
        let foreign_rows = [raw(20), raw(21)];
        let route = program.route(request).unwrap();
        let original = ProgramCompleteBatch {
            request,
            route,
            view: RowsView::new(&vars, &original_rows),
        };
        let foreign = ProgramCompleteBatch {
            request,
            route,
            view: RowsView::new(&vars, &foreign_rows),
        };
        let affinity = ProgramCompleteAffinity::new(&original_rows);
        let completion = program
            .try_complete_bounded(original, 2, &affinity)
            .unwrap();
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            completion.into_parts_for(foreign, &affinity, &original_rows)
        }));
        assert!(
            panic_text(rejected.expect_err("completion affinity must reject another batch"))
                .contains("paired with another row batch")
        );
    }

    #[test]
    fn bounded_completion_result_rejects_a_same_token_zero_column_foreign_owner() {
        struct ZeroColumnOwner {
            rows: Vec<RawInline>,
            row_count: usize,
        }

        static QUOTES: &[(usize, usize)] = &[(1, 1), (1, 1)];
        let probe = BoundedQuoteProbe {
            mode: BoundedQuoteMode::Quoted(QUOTES),
            calls: Arc::new(Mutex::new(BoundedQuoteCalls::default())),
        };
        let program = ProgramRef::new(&probe);
        let request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let original_owner = ZeroColumnOwner {
            rows: Vec::new(),
            row_count: 2,
        };
        let foreign_owner = ZeroColumnOwner {
            rows: Vec::new(),
            row_count: 2,
        };
        let original = ProgramCompleteBatch {
            request,
            route,
            view: RowsView::new_with_row_count(&[], &original_owner.rows, original_owner.row_count),
        };
        let foreign = ProgramCompleteBatch {
            request,
            route,
            view: RowsView::new_with_row_count(&[], &foreign_owner.rows, foreign_owner.row_count),
        };
        assert!(std::ptr::eq(original.view.vars, foreign.view.vars));
        assert!(std::ptr::eq(original.view.rows, foreign.view.rows));
        let affinity = ProgramCompleteAffinity::new(&original_owner);
        let completion = program
            .try_complete_bounded(original, 2, &affinity)
            .unwrap();
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            completion.into_parts_for(foreign, &affinity, &foreign_owner)
        }));
        assert!(
            panic_text(rejected.expect_err("completion affinity must reject another owner"))
                .contains("paired with another owner")
        );
    }

    #[test]
    fn complete_adapter_rejects_forged_route_and_completion_before_engine_dispatch() {
        let spec = CompleteTagProbe::RepeatedInOrder;
        let program = ProgramRef::new(&spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_singleton(0),
        };
        let vars = [0];
        let rows = [RawInline::default(), RawInline::default()];
        let view = RowsView::new(&vars, &rows);
        let canonical = program.route(request).unwrap();

        let mut wrong_route = canonical;
        wrong_route.key = ProgramKey::new(8);
        let rejected_route = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.complete_batch(
                ProgramCompleteBatch {
                    request,
                    route: wrong_route,
                    view,
                },
                &mut ProgramCompleteEffects::default(),
            );
        }));
        assert!(
            panic_text(rejected_route.expect_err("forged route must fail"))
                .contains("route was not pure")
        );

        let mut wrong_completion = canonical;
        wrong_completion.completion = ProgramCompletion::PageableOnly;
        let rejected_completion = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.complete_batch(
                ProgramCompleteBatch {
                    request,
                    route: wrong_completion,
                    view,
                },
                &mut ProgramCompleteEffects::default(),
            );
        }));
        assert!(
            panic_text(rejected_completion.expect_err("forged completion must fail"))
                .contains("lacked an equivalence certificate")
        );
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

        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
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

        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
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
        let mut runtime = TypedProgramRuntime::<NonComparableState, Key>::default();
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

    fn step_physical_probe(spec: &PhysicalProbe, limits: &[usize]) -> ProgramBatchEffects {
        let program = ProgramRef::new(spec);
        let request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let route = program.route(request).unwrap();
        let activations: Vec<_> = (0..limits.len())
            .map(|index| ProgramActivation(index as u64 + 1))
            .collect();
        let view = RowsView::new_with_row_count(&[], &[], activations.len());
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
        let candidate_sets: Vec<Option<&[RawInline]>> = vec![None; limits.len()];
        let mut effects = ProgramBatchEffects::default();
        program.step_batch(
            &mut runtime,
            ProgramBatch {
                stratum: route.stratum,
                view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                work: &work,
                limits,
            },
            &mut effects,
        );
        effects
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
                    stratum: route.stratum,
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
                stratum: route.stratum,
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
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
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
            assert_eq!(effects.placement, None);

            let typed_runtime = runtime
                .erased
                .as_mut()
                .as_any_mut()
                .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
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
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
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
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
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
    fn declined_physical_step_falls_back_with_the_exact_retained_states() {
        let spec = PhysicalProbe::new(PhysicalProbeMode::Decline);
        let effects = step_physical_probe(&spec, &[1, 1]);

        assert_eq!(*spec.physical_states.lock().unwrap(), vec![vec![10, 11]]);
        assert_eq!(*spec.native_states.lock().unwrap(), vec![vec![10, 11]]);
        assert_eq!(effects.pages.len(), 2);
        assert_eq!(effects.placement, None);
    }

    #[test]
    fn preferred_program_seals_route_arm_and_keeps_child_native_state() {
        assert_eq!(
            std::mem::size_of::<ProgramRef<'static>>(),
            std::mem::size_of::<&'static dyn ErasedProgramSpec>(),
            "Program composition must not widen every direct ProgramRef"
        );
        let calls = Arc::new(Mutex::new(ChoiceCalls::default()));
        let choice = PreferredProgram::new(
            PreferredChoiceProbe {
                calls: Arc::clone(&calls),
            },
            FallbackChoiceProbe {
                calls: Arc::clone(&calls),
            },
        );
        let program = ProgramRef::preferred(&choice);
        let view = RowsView::new_with_row_count(&[], &[], 1);
        let activations = [ProgramActivation(41)];
        let candidate = [RawInline::default()];

        let preferred_request = ProgramRequest {
            action: ProgramAction::Propose(0),
            bound: VariableSet::new_empty(),
        };
        let preferred_route = program.route(preferred_request).unwrap();
        assert_eq!(preferred_route.key.arm, ProgramRouteArm::Preferred);
        let mut preferred_runtime = program.new_runtime_for(preferred_route.key);
        let mut preferred_seed = ProgramSeedEffects::default();
        program.seed_batch(
            &mut preferred_runtime,
            ProgramSeedBatch {
                request: preferred_request,
                route: preferred_route,
                view,
                activations: &activations,
            },
            &mut preferred_seed,
        );
        let preferred_work: Vec<_> = preferred_seed
            .work
            .into_iter()
            .map(|seed| seed.work)
            .collect();
        let mut preferred_effects = ProgramBatchEffects::default();
        program.step_batch_for(
            &mut preferred_runtime,
            preferred_route.key,
            ProgramBatch {
                stratum: preferred_route.stratum,
                view,
                candidate_sets: &[None],
                activations: &activations,
                work: &preferred_work,
                limits: &[1],
            },
            &mut preferred_effects,
        );
        assert_eq!(preferred_effects.pages.len(), 1);
        {
            let calls = calls.lock().unwrap();
            assert_eq!(calls.preferred_physical, 1);
            assert_eq!(calls.preferred_native, 1);
            assert_eq!(calls.fallback_native, 0);
        }
        program.retire_activations(&mut preferred_runtime, preferred_route.key, &activations);

        let fallback_request = ProgramRequest {
            action: ProgramAction::Confirm(0),
            bound: VariableSet::new_empty(),
        };
        let fallback_route = program.route(fallback_request).unwrap();
        assert_eq!(fallback_route.key.arm, ProgramRouteArm::Fallback);
        assert_eq!(preferred_route.key.local, fallback_route.key.local);
        assert_ne!(preferred_route.key, fallback_route.key);
        let mut fallback_runtime = program.new_runtime_for(fallback_route.key);
        let mut fallback_seed = ProgramSeedEffects::default();
        program.seed_batch(
            &mut fallback_runtime,
            ProgramSeedBatch {
                request: fallback_request,
                route: fallback_route,
                view,
                activations: &activations,
            },
            &mut fallback_seed,
        );
        let fallback_work: Vec<_> = fallback_seed
            .work
            .into_iter()
            .map(|seed| seed.work)
            .collect();
        let mut fallback_effects = ProgramBatchEffects::default();
        program.step_batch_for(
            &mut fallback_runtime,
            fallback_route.key,
            ProgramBatch {
                stratum: fallback_route.stratum,
                view,
                candidate_sets: &[Some(&candidate)],
                activations: &activations,
                work: &fallback_work,
                limits: &[1],
            },
            &mut fallback_effects,
        );
        assert_eq!(fallback_effects.pages.len(), 1);
        assert_eq!(calls.lock().unwrap().fallback_native, 1);
        program.retire_activations(&mut fallback_runtime, fallback_route.key, &activations);

        let complete_request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_empty(),
        };
        let complete_route = program.route(complete_request).unwrap();
        assert_eq!(complete_route.key.arm, ProgramRouteArm::Preferred);
        let complete_view = RowsView::new_with_row_count(&[], &[], 2);
        let mut complete = ProgramCompleteEffects::default();
        program.complete_batch(
            ProgramCompleteBatch {
                request: complete_request,
                route: complete_route,
                view: complete_view,
            },
            &mut complete,
        );
        assert_eq!(
            complete.occurrences,
            [(0, RawInline::default()), (1, RawInline::default())]
        );
        assert_eq!(complete.raw_occurrence_count, 2);
        let calls = calls.lock().unwrap();
        assert_eq!(calls.preferred_quote, 1);
        assert_eq!(calls.preferred_complete, 1);
    }

    #[test]
    fn preferred_bounded_completion_remaps_once_without_touching_fallback() {
        let calls = Arc::new(Mutex::new(ChoiceCalls::default()));
        let choice = PreferredProgram::new(
            PreferredChoiceProbe {
                calls: Arc::clone(&calls),
            },
            FallbackChoiceProbe {
                calls: Arc::clone(&calls),
            },
        );
        let program = ProgramRef::preferred(&choice);
        let request = ProgramRequest {
            action: ProgramAction::Propose(1),
            bound: VariableSet::new_empty(),
        };
        let batch = ProgramCompleteBatch {
            request,
            route: program.route(request).unwrap(),
            view: RowsView::new_with_row_count(&[], &[], 2),
        };
        assert_eq!(batch.route.key.arm, ProgramRouteArm::Preferred);
        let affinity = ProgramCompleteAffinity::new(&batch);
        let completion = program
            .try_complete_bounded(batch, 2, &affinity)
            .expect("preferred child has an exact two-parent completion");
        let (first, admission, raw_occurrences, occurrences) =
            completion.into_parts_for(batch, &affinity, &batch);
        assert_eq!(first, 0);
        assert_eq!(
            admission,
            ProgramCompleteAdmission::Exact {
                drain_work_units: 2,
                raw_occurrences: 2,
            }
        );
        assert_eq!(raw_occurrences, 2);
        assert_eq!(
            occurrences,
            [(0, RawInline::default()), (1, RawInline::default())]
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.preferred_quote, 1);
        assert_eq!(calls.preferred_complete, 1);
        assert_eq!(calls.preferred_physical, 0);
        assert_eq!(calls.preferred_native, 0);
        assert_eq!(calls.fallback_native, 0);
    }

    #[test]
    fn completed_physical_step_uses_the_shared_adapter_and_records_placement() {
        let spec = PhysicalProbe::new(PhysicalProbeMode::Complete);
        let effects = step_physical_probe(&spec, &[1, 1]);

        assert_eq!(*spec.physical_states.lock().unwrap(), vec![vec![10, 11]]);
        assert!(spec.native_states.lock().unwrap().is_empty());
        assert_eq!(effects.pages.len(), 2);
        assert_eq!(
            effects.placement,
            Some(ProgramPhysicalReceipt::new("test-physical", "dense-page"))
        );
    }

    #[test]
    fn physical_step_cannot_bypass_the_shared_budget_validation() {
        let spec = PhysicalProbe::new(PhysicalProbeMode::OverBudget);
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = step_physical_probe(&spec, &[1]);
        }));
        let message = panic_text(rejected.expect_err("over-budget physical receipt must fail"));
        assert!(message.contains("exceeded one input's physical work budget"));
        assert!(spec.native_states.lock().unwrap().is_empty());
    }

    #[test]
    fn late_invalid_physical_receipt_commits_no_output_prefix() {
        let spec = PhysicalProbe::new(PhysicalProbeMode::LateRawAmplification);
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
        let work: Vec<_> = seeded.work.into_iter().map(|seed| seed.work).collect();
        let novelty_before = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
            .unwrap()
            .novelty
            .clone();
        let candidate_sets: [Option<&[RawInline]>; 1] = [None];
        let mut effects = ProgramBatchEffects::default();

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            program.step_batch(
                &mut runtime,
                ProgramBatch {
                    stratum: route.stratum,
                    view,
                    candidate_sets: &candidate_sets,
                    activations: &activations,
                    work: &work,
                    limits: &[1],
                },
                &mut effects,
            )
        }));
        let message = panic_text(rejected.expect_err("amplified physical receipt must fail"));
        assert!(message.contains("more raw effects than its examined-work receipt"));

        assert!(effects.pages.is_empty());
        assert!(effects.children.is_empty());
        assert!(effects.direct.is_empty());
        assert!(effects.accepted.is_empty());
        assert!(effects.supported.is_empty());
        assert_eq!(effects.source_pages, 0);
        assert_eq!(effects.source_examined, 0);
        assert_eq!(effects.source_roots, 0);
        assert_eq!(effects.transition_pages, 0);
        assert_eq!(effects.transition_examined, 0);
        assert_eq!(effects.placement, None);

        let typed_runtime = runtime
            .erased
            .as_mut()
            .as_any_mut()
            .downcast_mut::<TypedProgramRuntime<NonComparableState, Key>>()
            .unwrap();
        assert!(
            typed_runtime.slots.iter().all(|slot| slot.value.is_none()),
            "late validation committed a resume or child handle"
        );
        assert!(
            typed_runtime.novelty == novelty_before,
            "late validation admitted an output novelty key"
        );
        assert!(spec.native_states.lock().unwrap().is_empty());
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
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
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
