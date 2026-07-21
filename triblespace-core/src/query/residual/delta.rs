//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::{AHashMap, AHashSet};
use smallvec::SmallVec;

use crate::query::program::insert_engine_program_state;

use super::materialize::{
    ProposalMaterializePhaseKind, ProposalMaterializerState,
};
use super::set_admit::{SetAdmissionPhaseKind, SetAdmissionState};
use super::*;

static NEXT_REGISTRY_BRAND: AtomicU64 = AtomicU64::new(1);

/// Structural constraint occurrence that owns one cyclic expansion kernel.
/// The exact finite or outer continuation deliberately remains activation
/// payload, so histories with different return addresses can still batch the
/// same graph-product operation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DeltaSite {
    Leaf {
        occurrence: usize,
    },
    Formula {
        occurrence: usize,
        node: FormulaNodeId,
    },
}

/// Canonical cyclic work key. Activation-specific state, reducer policy, and
/// return continuation are deliberately absent.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct DeltaDesc {
    variable: VariableId,
    site: DeltaSite,
}

impl DeltaDesc {
    pub(super) fn leaf(variable: VariableId, occurrence: usize) -> Self {
        Self {
            variable,
            site: DeltaSite::Leaf { occurrence },
        }
    }

    pub(super) fn formula(variable: VariableId, occurrence: usize, node: FormulaNodeId) -> Self {
        Self {
            variable,
            site: DeltaSite::Formula { occurrence, node },
        }
    }

    fn resolve<'r, 'a>(
        &self,
        root: &'r dyn Constraint<'a>,
        plan: &ResidualPlan,
    ) -> &'r dyn Constraint<'a> {
        match self.site {
            DeltaSite::Leaf { occurrence } => plan.resolve(root, occurrence),
            DeltaSite::Formula { occurrence, node } => {
                plan.resolve_formula_node(root, occurrence, node)
            }
        }
    }
}

/// Immutable occurrence-local address of one constructed typed program.
///
/// The structural site distinguishes repeated references to the same `Arc`;
/// the family-local key distinguishes routes of that occurrence without a
/// query-global program catalog.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum ProgramAddress {
    /// A route owned by one structural constraint occurrence.
    Constraint {
        desc: DeltaDesc,
        key: ProgramKey,
        stratum: ProgramStratum,
    },
    /// One engine-owned finite reducer family. Bound schema, return PC,
    /// cursors, accumulators, and phase remain affine typed payload, so the
    /// address names only the static operation.
    Engine(EngineProgramKind),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum EngineProgramKind {
    ConfirmFinalize,
    FormulaOrAdmit,
    FormulaOrEmit,
    ProposalMaterialize,
    SetAdmit,
}

impl EngineProgramKind {
    fn resolve(self) -> ProgramRef<'static> {
        match self {
            Self::ConfirmFinalize => ProgramRef::new(&CONFIRM_FINALIZER_PROGRAM),
            Self::FormulaOrAdmit => ProgramRef::new(&FORMULA_OR_ADMISSION_PROGRAM),
            Self::FormulaOrEmit => ProgramRef::new(&FORMULA_OR_EMISSION_PROGRAM),
            Self::ProposalMaterialize => ProgramRef::new(&PROPOSAL_MATERIALIZER_PROGRAM),
            Self::SetAdmit => ProgramRef::new(&SET_ADMISSION_PROGRAM),
        }
    }
}

impl ProgramAddress {
    fn new(desc: DeltaDesc, route: ProgramRoute) -> Self {
        assert_eq!(
            desc.variable, route.variable,
            "constructed program route changed its structural variable"
        );
        Self::Constraint {
            desc,
            key: route.key,
            stratum: route.stratum,
        }
    }

    fn resolve<'r, 'a>(&self, root: &'r dyn Constraint<'a>, plan: &ResidualPlan) -> ProgramRef<'r> {
        match self {
            Self::Constraint { desc, .. } => desc
                .resolve(root, plan)
                .residual_program()
                .expect("constructed typed program disappeared during execution"),
            Self::Engine(kind) => kind.resolve(),
        }
    }

    fn stratum(&self) -> ProgramStratum {
        match self {
            Self::Constraint { stratum, .. } => *stratum,
            Self::Engine(_) => ProgramStratum::Finite,
        }
    }

    fn key(&self) -> ProgramKey {
        match self {
            Self::Constraint { key, .. } => *key,
            Self::Engine(_) => ProgramKey::new(0),
        }
    }

    fn has_private_direct_effects(&self) -> bool {
        matches!(self, Self::Engine(_))
    }
}

#[derive(Clone)]
struct ConfirmFinalizerState {
    original: DeferredCandidateCursor,
    accepted: Arc<AHashSet<RawInline>>,
}

struct ConfirmFinalizerProgram;

static CONFIRM_FINALIZER_PROGRAM: ConfirmFinalizerProgram = ConfirmFinalizerProgram;

impl TypedProgramSpec for ConfirmFinalizerProgram {
    type State = ConfirmFinalizerState;
    type NoveltyKey = ();
    type Rank = usize;

    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        // Engine-owned reducer states are opened only through the private
        // runtime seam; they are never routes offered by a Constraint.
        None
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        DispatchClass::new(0)
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.original.remaining
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        panic!("engine Confirm finalizer was seeded through a Constraint route")
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(
            batch.candidate_sets.iter().all(|candidates| candidates.is_none()),
            "Confirm finalizer unexpectedly borrowed a graph candidate slice"
        );
        for (input, (mut state, &limit)) in states.into_iter().zip(batch.limits).enumerate() {
            let mut examined = 0usize;
            while examined < limit {
                let Some((parent, candidate)) = state.original.next() else {
                    break;
                };
                assert_eq!(parent, 0, "one-parent finalizer cursor changed domains");
                examined += 1;
                if state.accepted.contains(&candidate) {
                    effects.direct(
                        u32::try_from(input).expect("too many Confirm finalizer inputs"),
                        candidate,
                    );
                }
            }
            assert!(examined > 0, "a nonempty Confirm finalizer made no progress");
            let resume = (state.original.remaining > 0).then_some(TypedResume::Immediate(state));
            effects.page(examined, resume);
        }
    }
}

struct SetAdmissionProgram;

static SET_ADMISSION_PROGRAM: SetAdmissionProgram = SetAdmissionProgram;

impl TypedProgramSpec for SetAdmissionProgram {
    type State = SetAdmissionState;
    type NoveltyKey = ();
    type Rank = u128;

    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        None
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        DispatchClass::new(match state.phase_kind() {
            SetAdmissionPhaseKind::Scan => 0,
            SetAdmissionPhaseKind::Emit => 1,
        })
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.rank()
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        panic!("engine SET admission was seeded through a Constraint route")
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (state, &limit)) in states.into_iter().zip(batch.limits).enumerate() {
            let page = state.advance(limit);
            for value in page.emitted {
                effects.direct(
                    u32::try_from(input).expect("too many SET-admission inputs"),
                    value,
                );
            }
            effects.page(page.examined, page.next.map(TypedResume::Immediate));
        }
    }
}

#[derive(Clone)]
struct FormulaOrAdmissionState {
    input: DeferredCandidateCursor,
}

struct FormulaOrAdmissionProgram;

static FORMULA_OR_ADMISSION_PROGRAM: FormulaOrAdmissionProgram = FormulaOrAdmissionProgram;

impl TypedProgramSpec for FormulaOrAdmissionProgram {
    type State = FormulaOrAdmissionState;
    type NoveltyKey = ();
    type Rank = usize;

    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        None
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        DispatchClass::new(0)
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.input.remaining
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        panic!("engine Formula OR admission was seeded through a Constraint route")
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (mut state, &limit)) in states.into_iter().zip(batch.limits).enumerate() {
            let mut examined = 0usize;
            while examined < limit {
                let Some((parent, value)) = state.input.next() else {
                    break;
                };
                assert_eq!(parent, 0, "one-parent Formula admission changed domains");
                examined += 1;
                effects.direct(
                    u32::try_from(input).expect("too many Formula admission inputs"),
                    value,
                );
            }
            assert!(examined > 0, "a nonempty Formula admission made no progress");
            let resume =
                (state.input.remaining > 0).then_some(TypedResume::Immediate(state));
            effects.page(examined, resume);
        }
    }
}

#[derive(Clone)]
struct FormulaOrEmissionState {
    set: OrdSet<RawInline>,
    emitted_count: usize,
    last_emitted: Option<RawInline>,
}

struct FormulaOrEmissionProgram;

static FORMULA_OR_EMISSION_PROGRAM: FormulaOrEmissionProgram = FormulaOrEmissionProgram;

impl TypedProgramSpec for FormulaOrEmissionProgram {
    type State = FormulaOrEmissionState;
    type NoveltyKey = ();
    type Rank = usize;

    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        None
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        DispatchClass::new(0)
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state
            .set
            .len()
            .checked_sub(state.emitted_count)
            .expect("Formula emission count exceeded its ordered set")
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        panic!("engine Formula OR emission was seeded through a Constraint route")
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        use std::ops::Bound::{Excluded, Unbounded};

        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (mut state, &limit)) in states.into_iter().zip(batch.limits).enumerate() {
            // "Singleton emission" is one affine parent/credit, not one
            // value per receipt.  One Search page may move at most its grant
            // into one new rope leaf.
            let values: Vec<_> = match state.last_emitted {
                Some(last) => state
                    .set
                    .range((Excluded(last), Unbounded))
                    .take(limit)
                    .copied()
                    .collect(),
                None => state.set.iter().take(limit).copied().collect(),
            };
            assert!(!values.is_empty(), "a nonempty Formula emission made no progress");
            for &value in &values {
                effects.direct(
                    u32::try_from(input).expect("too many Formula emission inputs"),
                    value,
                );
            }
            state.emitted_count = state
                .emitted_count
                .checked_add(values.len())
                .expect("Formula emission count overflow");
            state.last_emitted = values.last().copied();
            let remaining = state
                .set
                .len()
                .checked_sub(state.emitted_count)
                .expect("Formula emission count exceeded its ordered set");
            let resume = (remaining > 0).then_some(TypedResume::Immediate(state));
            effects.page(values.len(), resume);
        }
    }
}

struct ProposalMaterializerProgram;

static PROPOSAL_MATERIALIZER_PROGRAM: ProposalMaterializerProgram = ProposalMaterializerProgram;

impl TypedProgramSpec for ProposalMaterializerProgram {
    type State = ProposalMaterializerState;
    type NoveltyKey = ();
    type Rank = u128;

    fn route(&self, _request: ProgramRequest) -> Option<ProgramRoute> {
        None
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        DispatchClass::new(match state.phase_kind() {
            ProposalMaterializePhaseKind::Seal => 0,
            ProposalMaterializePhaseKind::Merge => 1,
            ProposalMaterializePhaseKind::Emit => 2,
        })
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Search
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        state.rank()
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        panic!("engine proposal materializer was seeded through a Constraint route")
    }

    fn step_typed(
        &self,
        states: crate::query::TypedProgramStateBatch<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (state, &limit)) in states.into_iter().zip(batch.limits).enumerate() {
            let page = state.advance(limit);
            for value in page.emitted {
                effects.direct(
                    u32::try_from(input).expect("too many proposal materializer inputs"),
                    value,
                );
            }
            effects.page(
                page.examined,
                page.next.map(TypedResume::Immediate),
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct DeltaStateId(u32);

/// Physical preference for one newly filed cyclic activation.
///
/// The structural state remains the canonical batching key. Activation
/// identity is deliberately payload-only: this token merely lets the outer
/// latency scheduler follow the affine lineage it just created before cold
/// stable work harvests a wider cohort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ActiveDeltaContinuation {
    state: DeltaStateId,
    activation: ActivationId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeltaStateEntry {
    Legacy(DeltaDesc),
    Program(ProgramAddress),
}

#[derive(Clone, Default)]
struct DeltaInterner {
    by_desc: AHashMap<DeltaDesc, DeltaStateId>,
    by_program: AHashMap<ProgramAddress, DeltaStateId>,
    entries: Vec<DeltaStateEntry>,
}

impl DeltaInterner {
    fn intern(&mut self, desc: DeltaDesc) -> DeltaStateId {
        if let Some(&id) = self.by_desc.get(&desc) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.entries.len()).expect("too many delta states"));
        self.entries.push(DeltaStateEntry::Legacy(desc.clone()));
        self.by_desc.insert(desc, id);
        id
    }

    fn intern_program(&mut self, address: ProgramAddress) -> DeltaStateId {
        if let Some(&id) = self.by_program.get(&address) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.entries.len()).expect("too many program states"));
        self.entries.push(DeltaStateEntry::Program(address.clone()));
        self.by_program.insert(address, id);
        id
    }

    fn get(&self, id: DeltaStateId) -> &DeltaDesc {
        match &self.entries[id.0 as usize] {
            DeltaStateEntry::Legacy(desc) => desc,
            DeltaStateEntry::Program(_) => {
                panic!("typed Program state was resolved as a legacy delta descriptor")
            }
        }
    }

    fn program(&self, id: DeltaStateId) -> Option<&ProgramAddress> {
        match &self.entries[id.0 as usize] {
            DeltaStateEntry::Legacy(_) => None,
            DeltaStateEntry::Program(address) => Some(address),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RegistryBrand(u64);

impl RegistryBrand {
    fn fresh() -> Self {
        let value = NEXT_REGISTRY_BRAND
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .expect("delta registry brand space exhausted");
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ActivationId(u64);

impl ActivationId {
    pub(super) fn index(self) -> usize {
        usize::try_from(self.0).expect("delta activation index exceeds usize")
    }
}

#[cfg(test)]
impl ActivationId {
    pub(super) const fn test(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditNonce(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditKey {
    activation: ActivationId,
    nonce: CreditNonce,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ProgramJoinId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreditKind {
    Generator,
    Traversal,
    Program { join: Option<ProgramJoinId> },
}

/// Affine authority to replace one cyclic producer with its novel successors.
#[derive(Debug)]
pub(super) struct ProducerCredit {
    brand: RegistryBrand,
    key: CreditKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivationStatus {
    Open,
    Quiescent,
}

/// Physical publication class for one affine activation. This is payload
/// metadata only: it never participates in [`DeltaDesc`] or interner identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeltaPhysicalClass {
    General,
    TerminalStreaming,
}

#[derive(Clone)]
enum DeltaReducer {
    /// Accepted values may immediately enter an ordinary Candidate state.
    StreamProposal,
    /// Accepted values may immediately resume an activation-local formula
    /// continuation whose exact PC has been proved linear and page-local.
    /// Emission inherits the ordinary streaming proposal's discovery order;
    /// only bag equality with the sorted quiescent formula result is promised.
    StreamFormulaProposal,
    /// Accepted values remain private until the enclosing formula action has
    /// proved quiescence. Every direct occurrence and newly accepted endpoint
    /// is appended as it is discovered, so the quiescence handoff performs no
    /// hidden whole-bag scan or conversion.
    QuiescentProposal { occurrences: CandidatePayload },
    /// Accepted endpoints are Boolean witnesses, not candidate values. The
    /// first witness releases `true` exactly once; only producer quiescence can
    /// release `false`.
    Support {
        published: bool,
    },
    Confirm {
        /// Immutable one-parent occurrence bag frozen at action opening.
        /// During graph execution this remains one contiguous Deferred leaf,
        /// so typed constraint Programs may borrow it through their existing
        /// slice API. Finalization always switches to its structural cursor.
        original: CandidatePayload,
    },
    /// Graph discovery has quiesced and transferred its sole affine credit to
    /// the engine-owned Search-paced occurrence scanner.
    FinalizingConfirm {
        output: CandidatePayload,
    },
    /// Proposal discovery has quiesced and transferred its sole affine credit
    /// to the engine-owned Seal/Merge/Emit normalizer.
    FinalizingProposal {
        output: CandidatePayload,
    },
    /// A segmented candidate relation is being admitted by the engine-owned
    /// bounded scan/emit Program before it re-enters the stable machine.
    SetAdmit {
        output: CandidatePayload,
    },
    /// One Search-paced pass inserts occurrence values into the persistent OR
    /// accumulator stored in the activation's exact Formula return payload.
    FormulaOrAdmit,
    /// One Search-paced pass emits an ordered set into the persistent
    /// candidate rope consumed by the ordinary Formula continuation.
    FormulaOrEmit { output: CandidatePayload },
}

impl DeltaReducer {
    fn quiescent_proposal() -> Self {
        Self::QuiescentProposal {
            occurrences: empty_one_parent_rope(),
        }
    }

    fn streams(&self) -> bool {
        matches!(self, Self::StreamProposal | Self::StreamFormulaProposal)
    }

    fn retain_quiescent_proposal_page(&mut self, values: Vec<RawInline>) {
        if let Self::QuiescentProposal { occurrences } = self {
            append_one_parent_page(occurrences, values);
        }
    }
}

fn empty_one_parent_rope() -> CandidatePayload {
    let mut output = CandidatePayload::empty(1);
    output.defer_for_shared_activation(1);
    output
}

fn append_one_parent_page(output: &mut CandidatePayload, values: Vec<RawInline>) {
    if values.is_empty() {
        return;
    }
    let mut page = CandidatePayload::Values(values);
    page.defer_for_shared_activation(1);
    output.extend_same_domain(page, 1);
}

/// Exact affine continuation owned by one reducer activation.
///
/// Stable formula PC IDs intentionally live here rather than in [`DeltaDesc`]:
/// two activations may expand the same RPQ product kernel while returning to
/// different arena-interned ancestor states and payload-frame stacks.
#[derive(Clone)]
enum DeltaReturn {
    Stable {
        desc: StateDesc,
        parent: Box<[RawInline]>,
        /// The complete action result crosses from an occurrence bag into a
        /// candidate continuation that may split or commit independently.
        /// Cyclic Confirm computes this receipt with the shared stable-state
        /// boundary predicate before opening graph traversal.
        set_admit_result: bool,
    },
    Formula {
        bound: VariableSet,
        counter: FormulaPcId,
        batch: FormulaBatch,
    },
    FormulaOrAdmit {
        bound: VariableSet,
        batch: FormulaBatch,
        continuation: FormulaReducerContinuation,
    },
    FormulaOrEmit {
        bound: VariableSet,
        batch: FormulaBatch,
        counter: FormulaPcId,
    },
    SetAdmission {
        successor: StateDesc,
        destination: SetAdmissionDestination,
    },
}

#[derive(Clone)]
struct SuspendedSourcePage {
    next: Option<ResidualDeltaSourceCursor>,
    /// Only an effect already filed into the stable acyclic machine keeps a
    /// page from being scheduler-negative. Formula proposals and grouped
    /// confirmations remain private until the complete action quiesces.
    had_stable_effect: bool,
}

/// Receipt-local structured join for an opaque typed continuation.
///
/// Child credits drain independently. Only this join's final child releases
/// the stored exact resume, and the resume then inherits the parent lineage's
/// join without involving unrelated work in the activation.
#[derive(Clone)]
struct ProgramJoin {
    remaining: usize,
    resume: Option<ProgramWork>,
    state: DeltaStateId,
    parent: Option<ProgramJoinId>,
    /// A Search-paced page owns this receipt-local child barrier. Its children
    /// may perform many Activation-paced steps, but the page contributes at
    /// most one geometric negative receipt when the barrier drains.
    search_page: bool,
    /// Independently records whether family telemetry classified the page as
    /// a source page. This affects counters only, never join semantics.
    source_telemetry: bool,
    had_stable_effect: bool,
}

struct ProgramJoinCompletion {
    scheduled: Option<(DeltaStateId, ProgramWork, ProducerCredit)>,
    dead_search_pages: usize,
    dead_source_telemetry_pages: usize,
}

/// One affine parent reducer scope. Several speculative source roots may own
/// live credits inside it; they share novelty and Accepted, while source stays
/// in each node so their product states cannot suppress one another.
#[derive(Clone)]
struct Activation {
    reducer: DeltaReducer,
    return_to: DeltaReturn,
    physical_class: DeltaPhysicalClass,
    /// Physical grant quantum for a terminal activation whose current sparse
    /// dispatch did not publish. This is engine-owned activation-local search evidence:
    /// publication resets it to one, while the independent search width
    /// supplies only the hard cap.
    terminal_sparse_quantum: usize,
    /// Sorted distinct source scope for grouped confirmation. Proposals own a
    /// constraint-generated graph frontier and therefore store `None`.
    source_candidates: Option<Box<[RawInline]>>,
    /// The continuation cursor is suspended while every traversal lineage
    /// rooted in the current page owns the activation's affine credits.
    suspended_source_page: Option<SuspendedSourcePage>,
    program_joins: AHashMap<ProgramJoinId, ProgramJoin>,
    seen: AHashMap<ResidualDeltaNode, bool>,
    accepted: AHashSet<RawInline>,
    /// The complete affine producer ledger for this activation. Presence
    /// proves that the nonce is live; the value distinguishes generator and
    /// traversal replacement authority without a second global owner map.
    live: AHashMap<CreditNonce, CreditKind>,
    status: ActivationStatus,
}

#[derive(Clone)]
struct RegistryState {
    next_activation: u64,
    next_credit: u64,
    next_program_join: u64,
    activations: AHashMap<ActivationId, Activation>,
}

struct ProducerRegistry {
    brand: RegistryBrand,
    state: RegistryState,
}

#[derive(Debug)]
struct QuiescenceProof {
    activation: ActivationId,
}

#[derive(Debug)]
struct ReplaceOutcome {
    children: Vec<(ResidualDeltaNode, ProducerCredit)>,
    accepted: Vec<RawInline>,
    resumed_traversal: Option<(ResidualDeltaExpandCursor, ProducerCredit)>,
    resumed_source: Option<(ResidualDeltaSourceCursor, ProducerCredit)>,
    retired_source_page: Option<RetiredSourcePage>,
    quiescence: Option<QuiescenceProof>,
}

#[derive(Clone, Copy, Debug)]
struct RetiredSourcePage {
    had_stable_effect: bool,
}

#[derive(Debug)]
struct SourcePageOutcome {
    roots: Vec<(ResidualDeltaNode, ProducerCredit)>,
    /// Raw direct and accepting-root proposal occurrences before
    /// activation-local SET admission. Non-streaming reducers report zero.
    raw_proposal_occurrences: usize,
    accepted: Vec<RawInline>,
    resumed_source: Option<(ResidualDeltaSourceCursor, ProducerCredit)>,
    retired_source_page: Option<RetiredSourcePage>,
    quiescence: Option<QuiescenceProof>,
}

struct StartOutcome {
    activation: ActivationId,
    roots: Vec<(ResidualDeltaNode, ProducerCredit)>,
    /// Distinct accepting seed endpoints in constraint order. The registry
    /// records them before issuing this receipt, so reducers that cannot
    /// stream may simply retain them until quiescence.
    initial_accepted: Vec<RawInline>,
    quiescence: Option<QuiescenceProof>,
}

struct ProgramInstallOutcome {
    roots: Vec<(ProgramWork, ProducerCredit)>,
    initial_accepted: Vec<RawInline>,
    quiescence: Option<QuiescenceProof>,
}

struct ProgramReplaceOutcome {
    scheduled: SmallVec<[(DeltaStateId, ProgramWork, ProducerCredit); 2]>,
    /// Raw proposal occurrences reported by this typed page before
    /// activation-local SET admission. This remains telemetry only.
    raw_proposal_occurrences: usize,
    accepted: SmallVec<[RawInline; 1]>,
    dead_search_pages: usize,
    dead_source_telemetry_pages: usize,
    quiescence: Option<QuiescenceProof>,
}

struct CompletedActivation {
    activation: ActivationId,
    return_to: DeltaReturn,
    effect: DeltaCompletion,
}

struct ConfirmFinalizerSeed {
    activation: ActivationId,
    state: ConfirmFinalizerState,
    credit: ProducerCredit,
}

struct ProposalMaterializerSeed {
    activation: ActivationId,
    state: ProposalMaterializerState,
    credit: ProducerCredit,
}

enum RegistrySettlement {
    Completed(CompletedActivation),
    ConfirmFinalizer(ConfirmFinalizerSeed),
    ProposalMaterializer(ProposalMaterializerSeed),
}

#[derive(Debug)]
enum DeltaCompletion {
    /// Every semantic effect was released before quiescence.
    Cleanup,
    /// Complete quiescent candidate action result.
    Candidates(CandidatePayload),
    /// Boolean support proved only at the reducer boundary.
    Support(bool),
    /// Admission mutated the private persistent accumulator directly; EOF
    /// releases only its exact saved Formula control.
    FormulaOrAdmitted,
}

impl PartialEq for DeltaCompletion {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Cleanup, Self::Cleanup) => true,
            (Self::Support(left), Self::Support(right)) => left == right,
            (Self::FormulaOrAdmitted, Self::FormulaOrAdmitted) => true,
            (Self::Candidates(left), Self::Candidates(right)) => {
                left.iter().eq(right.iter())
            }
            _ => false,
        }
    }
}

impl Eq for DeltaCompletion {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeltaStreamingEffect {
    Candidates,
    Support,
}

struct DeltaStreamingReturn {
    return_to: DeltaReturn,
    effect: DeltaStreamingEffect,
}

/// Full-bound rows published by terminal streaming activations, with one
/// exact affine origin per row.  Origin is physical evidence for the outer
/// projected-yield ledger; it never enters canonical residual identity.
#[derive(Debug)]
pub(super) struct TerminalPublicationBatch {
    pub(super) rows: RowBatch,
    /// Terminal sparse search overwhelmingly publishes one row at a time.
    /// Keep that exact origin inline; wider/mixed cohorts spill only when
    /// they actually need more storage.
    pub(super) origins: SmallVec<[ActivationId; 1]>,
}

impl TerminalPublicationBatch {
    fn new(activation: ActivationId, rows: RowBatch) -> Self {
        let mut origins = SmallVec::new();
        origins.resize(rows.row_count, activation);
        Self { rows, origins }
    }

    fn append(&mut self, mut other: Self) {
        self.rows.append(other.rows);
        self.origins.extend(other.origins.drain(..));
        debug_assert_eq!(self.origins.len(), self.rows.row_count);
    }
}

#[derive(Default)]
struct DeltaStableEffects {
    continuation: Option<ContinuationToken>,
    /// Full-bound raw rows ready for the outer iterator's ordinary staging
    /// buffer. This is a physical publication receipt, never a canonical
    /// delta or stable state.
    publication: Option<TerminalPublicationBatch>,
}

#[derive(Default)]
struct FormulaReducerDrain {
    continuation: Option<ContinuationToken>,
    active: Option<ActiveDeltaContinuation>,
}

struct DeltaStreamingRelease {
    stable: DeltaStableEffects,
    /// A first streaming support witness may release Formula control into a
    /// fresh private reducer while the old activation remains globally live
    /// only to retire its cleanup credits.
    active: Option<ActiveDeltaContinuation>,
}

impl DeltaStableEffects {
    fn absorb(&mut self, mut other: Self) {
        prefer_continuation(&mut self.continuation, other.continuation);
        if let Some(rows) = other.publication.take() {
            if let Some(existing) = &mut self.publication {
                existing.append(rows);
            } else {
                self.publication = Some(rows);
            }
        }
    }

    fn has_effect(&self) -> bool {
        self.continuation.is_some() || self.publication.is_some()
    }
}

impl ProducerRegistry {
    fn new() -> Self {
        Self {
            brand: RegistryBrand::fresh(),
            state: RegistryState {
                next_activation: 0,
                next_credit: 0,
                next_program_join: 0,
                activations: AHashMap::new(),
            },
        }
    }

    /// Reserves query-local activation identities for a terminal cohort that
    /// is evaluated eagerly instead of entering the sparse fixpoint machine.
    ///
    /// These are receipts, not activations: they deliberately have no
    /// registry entry and mint no affine producer credit. Sharing the same
    /// monotone namespace lets the outer projected-yield ledger treat eager
    /// and sparse parents uniformly without manufacturing fake cyclic work.
    fn reserve_terminal_receipts(&mut self, count: usize) -> Vec<ActivationId> {
        let mut receipts = Vec::with_capacity(count);
        for _ in 0..count {
            let receipt = ActivationId(take_monotonic(
                &mut self.state.next_activation,
                "activation",
            ));
            debug_assert!(
                !self.state.activations.contains_key(&receipt),
                "eager terminal receipt unexpectedly owns a live activation"
            );
            receipts.push(receipt);
        }
        receipts
    }

    /// Starts one parent-scoped activation with one affine credit per root.
    fn start_many(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        seeds: impl IntoIterator<Item = ResidualDeltaOutput>,
    ) -> StartOutcome {
        self.start_many_classified(reducer, return_to, seeds, None)
    }

    fn start_many_terminal(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        seeds: impl IntoIterator<Item = ResidualDeltaOutput>,
        full: VariableSet,
    ) -> StartOutcome {
        self.start_many_classified(reducer, return_to, seeds, Some(full))
    }

    fn start_many_classified(
        &mut self,
        mut reducer: DeltaReducer,
        return_to: DeltaReturn,
        seeds: impl IntoIterator<Item = ResidualDeltaOutput>,
        terminal_full: Option<VariableSet>,
    ) -> StartOutcome {
        debug_assert!(
            matches!(
                &return_to,
                DeltaReturn::Formula { batch, .. } if batch.parents.row_count == 1
            ) || !matches!(&return_to, DeltaReturn::Formula { .. })
        );
        let physical_class = Self::physical_class(&reducer, &return_to, terminal_full);
        let seeds = seeds.into_iter();
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        let mut live = AHashMap::new();
        let mut accepted = AHashSet::new();
        let mut initial_accepted = Vec::new();
        let mut roots = Vec::with_capacity(seeds.size_hint().0);
        for seed in seeds {
            let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
            assert!(live.insert(nonce, CreditKind::Traversal).is_none());
            if seed.accepted && accepted.insert(seed.node.value) {
                initial_accepted.push(seed.node.value);
            }
            roots.push((
                seed.node,
                ProducerCredit {
                    brand: self.brand,
                    key: CreditKey { activation, nonce },
                },
            ));
        }
        reducer.retain_quiescent_proposal_page(initial_accepted.clone());
        let status = if live.is_empty() {
            ActivationStatus::Quiescent
        } else {
            ActivationStatus::Open
        };
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        reducer,
                        return_to,
                        physical_class,
                        terminal_sparse_quantum: 1,
                        source_candidates: None,
                        suspended_source_page: None,
                        program_joins: AHashMap::new(),
                        seen: AHashMap::new(),
                        accepted,
                        live,
                        status,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        StartOutcome {
            activation,
            roots,
            initial_accepted,
            quiescence: (status == ActivationStatus::Quiescent)
                .then_some(QuiescenceProof { activation }),
        }
    }

    /// Starts one activation with a single affine generator credit. The
    /// generator is replaced by one bounded source page; its continuation is
    /// not reissued until every traversal rooted in that page retires.
    fn start_source(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        source_candidates: Option<Box<[RawInline]>>,
    ) -> (ActivationId, ProducerCredit) {
        self.start_source_classified(reducer, return_to, source_candidates, None)
    }

    fn start_source_terminal(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        source_candidates: Option<Box<[RawInline]>>,
        full: VariableSet,
    ) -> (ActivationId, ProducerCredit) {
        self.start_source_classified(reducer, return_to, source_candidates, Some(full))
    }

    fn start_source_classified(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        source_candidates: Option<Box<[RawInline]>>,
        terminal_full: Option<VariableSet>,
    ) -> (ActivationId, ProducerCredit) {
        debug_assert!(
            matches!(
                &return_to,
                DeltaReturn::Formula { batch, .. } if batch.parents.row_count == 1
            ) || !matches!(&return_to, DeltaReturn::Formula { .. })
        );
        let physical_class = Self::physical_class(&reducer, &return_to, terminal_full);
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        reducer,
                        return_to,
                        physical_class,
                        terminal_sparse_quantum: 1,
                        source_candidates,
                        suspended_source_page: None,
                        program_joins: AHashMap::new(),
                        seen: AHashMap::new(),
                        accepted: AHashSet::new(),
                        live: AHashMap::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        let credit = self.issue_credit(activation, CreditKind::Generator);
        (activation, credit)
    }

    /// Creates one reducer activation before typed seed states are installed.
    /// The engine-created identity is passed into the typed adapter so every
    /// arena slot is owned from birth by its affine parent.
    fn open_program_activation(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        source_candidates: Option<Box<[RawInline]>>,
        terminal_full: Option<VariableSet>,
    ) -> ActivationId {
        let physical_class = Self::physical_class(&reducer, &return_to, terminal_full);
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        reducer,
                        return_to,
                        physical_class,
                        terminal_sparse_quantum: 1,
                        source_candidates,
                        suspended_source_page: None,
                        program_joins: AHashMap::new(),
                        seen: AHashMap::new(),
                        accepted: AHashSet::new(),
                        live: AHashMap::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "program activation identifier was reused"
        );
        activation
    }

    fn install_program_roots(
        &mut self,
        activation_id: ActivationId,
        seeds: impl IntoIterator<Item = ProgramSeedWork>,
    ) -> ProgramInstallOutcome {
        {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            assert_eq!(activation.status, ActivationStatus::Open);
            assert!(activation.live.is_empty());
            assert!(activation.program_joins.is_empty());
        }

        let mut roots = Vec::new();
        let mut initial_accepted = Vec::new();
        for seed in seeds {
            if let Some(value) = seed.accepted {
                if self
                    .state
                    .activations
                    .get_mut(&activation_id)
                    .expect("unknown program activation")
                    .accepted
                    .insert(value)
                {
                    initial_accepted.push(value);
                }
            }
            let credit = self.issue_credit(activation_id, CreditKind::Program { join: None });
            roots.push((seed.work, credit));
        }
        self.state
            .activations
            .get_mut(&activation_id)
            .expect("unknown program activation")
            .reducer
            .retain_quiescent_proposal_page(initial_accepted.clone());
        let status = if roots.is_empty() {
            ActivationStatus::Quiescent
        } else {
            ActivationStatus::Open
        };
        self.state
            .activations
            .get_mut(&activation_id)
            .expect("unknown program activation")
            .status = status;
        ProgramInstallOutcome {
            roots,
            initial_accepted,
            quiescence: (status == ActivationStatus::Quiescent).then_some(QuiescenceProof {
                activation: activation_id,
            }),
        }
    }

    fn physical_class(
        reducer: &DeltaReducer,
        return_to: &DeltaReturn,
        terminal_full: Option<VariableSet>,
    ) -> DeltaPhysicalClass {
        let Some(full) = terminal_full else {
            return DeltaPhysicalClass::General;
        };
        let (DeltaReducer::StreamProposal, DeltaReturn::Stable { desc, .. }) = (reducer, return_to)
        else {
            return DeltaPhysicalClass::General;
        };
        if commits_final_checked_candidate(desc, full) {
            DeltaPhysicalClass::TerminalStreaming
        } else {
            DeltaPhysicalClass::General
        }
    }

    fn issue_credit(&mut self, activation: ActivationId, kind: CreditKind) -> ProducerCredit {
        let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
        assert!(
            self.state
                .activations
                .get_mut(&activation)
                .expect("unknown delta activation")
                .live
                .insert(nonce, kind)
                .is_none(),
            "delta credit nonce was reused"
        );
        ProducerCredit {
            brand: self.brand,
            key: CreditKey { activation, nonce },
        }
    }

    #[cfg(test)]
    fn replace_traversal(
        &mut self,
        parent: ProducerCredit,
        successors: impl IntoIterator<Item = ResidualDeltaOutput>,
    ) -> ReplaceOutcome {
        self.replace_traversal_page(parent, successors, None)
    }

    fn replace_traversal_page(
        &mut self,
        parent: ProducerCredit,
        successors: impl IntoIterator<Item = ResidualDeltaOutput>,
        next: Option<ResidualDeltaExpandCursor>,
    ) -> ReplaceOutcome {
        assert_eq!(parent.brand, self.brand, "delta credit crossed registries");
        let activation = self
            .state
            .activations
            .get_mut(&parent.key.activation)
            .expect("unknown delta activation");
        assert_eq!(activation.status, ActivationStatus::Open);
        assert_eq!(
            activation.live.get(&parent.key.nonce),
            Some(&CreditKind::Traversal),
            "unknown, replayed, or wrong-kind delta traversal credit"
        );

        let mut novel = Vec::new();
        let mut accepted = Vec::new();
        for successor in successors {
            if let Some(&previous) = activation.seen.get(&successor.node) {
                assert_eq!(
                    previous, successor.accepted,
                    "one delta node changed its endpoint effect"
                );
                continue;
            }
            assert!(activation
                .seen
                .insert(successor.node, successor.accepted)
                .is_none());
            novel.push(successor.node);
            if successor.accepted && activation.accepted.insert(successor.node.value) {
                accepted.push(successor.node.value);
            }
        }

        // Insert every child before retiring the parent. A 1 -> N replacement
        // therefore cannot transiently advertise zero live producers.
        let mut children = Vec::with_capacity(novel.len());
        for successor in novel {
            children.push((
                successor,
                self.issue_credit(parent.key.activation, CreditKind::Traversal),
            ));
        }
        let resumed_traversal = next.map(|cursor| {
            (
                cursor,
                self.issue_credit(parent.key.activation, CreditKind::Traversal),
            )
        });

        let activation = self
            .state
            .activations
            .get_mut(&parent.key.activation)
            .expect("unknown delta activation");
        activation
            .reducer
            .retain_quiescent_proposal_page(accepted.clone());
        assert_eq!(
            activation.live.remove(&parent.key.nonce),
            Some(CreditKind::Traversal)
        );
        if !accepted.is_empty() && activation.reducer.streams() {
            if let Some(page) = &mut activation.suspended_source_page {
                page.had_stable_effect = true;
            }
        }
        let page_finished = activation.live.is_empty();
        let (resumed_source, retired_source_page, quiescence) = if page_finished {
            self.finish_source_page_or_activation(parent.key.activation)
        } else {
            (None, None, None)
        };
        ReplaceOutcome {
            children,
            accepted,
            resumed_traversal,
            resumed_source,
            retired_source_page,
            quiescence,
        }
    }

    fn replace_source(
        &mut self,
        generator: ProducerCredit,
        roots: impl IntoIterator<Item = ResidualDeltaOutput>,
        direct: impl IntoIterator<Item = RawInline>,
        next: Option<ResidualDeltaSourceCursor>,
    ) -> SourcePageOutcome {
        assert_eq!(
            generator.brand, self.brand,
            "delta credit crossed registries"
        );
        let activation = self
            .state
            .activations
            .get(&generator.key.activation)
            .expect("unknown delta activation");
        assert_eq!(activation.status, ActivationStatus::Open);
        assert_eq!(
            activation.live.get(&generator.key.nonce),
            Some(&CreditKind::Generator),
            "unknown, replayed, or wrong-kind delta generator credit"
        );

        let roots: Vec<_> = roots.into_iter().collect();
        let direct: Vec<_> = direct.into_iter().collect();
        let mut distinct_nodes = AHashSet::with_capacity(roots.len());
        assert!(
            roots
                .iter()
                .all(|output| distinct_nodes.insert(output.node)),
            "one residual source page repeated a root node"
        );

        // Direct proposal effects and accepting roots share one affine SET
        // lifetime for streaming reducers. Quiescent proposals deliberately
        // retain their raw direct bag until their later bounded boundary.
        let raw_stream_occurrences = {
            let activation = self
                .state
                .activations
                .get(&generator.key.activation)
                .expect("unknown delta activation");
            if activation.reducer.streams() {
                direct
                    .len()
                    .checked_add(roots.iter().filter(|root| root.accepted).count())
                    .expect("source proposal occurrence count overflow")
            } else {
                0
            }
        };
        let mut accepted = Vec::new();
        let had_stable_effect;
        {
            let activation = self
                .state
                .activations
                .get_mut(&generator.key.activation)
                .expect("unknown delta activation");
            assert_eq!(activation.status, ActivationStatus::Open);
            assert_eq!(activation.live.len(), 1);
            assert_eq!(
                activation.live.get(&generator.key.nonce),
                Some(&CreditKind::Generator)
            );
            assert!(activation.suspended_source_page.is_none());
            match &activation.reducer {
                DeltaReducer::QuiescentProposal { .. }
                | DeltaReducer::StreamProposal
                | DeltaReducer::StreamFormulaProposal => {}
                DeltaReducer::Support { .. }
                | DeltaReducer::Confirm { .. }
                | DeltaReducer::FinalizingConfirm { .. }
                | DeltaReducer::FinalizingProposal { .. }
                | DeltaReducer::SetAdmit { .. }
                | DeltaReducer::FormulaOrAdmit
                | DeltaReducer::FormulaOrEmit { .. } => assert!(
                    direct.is_empty(),
                    "a non-proposal reducer received direct source candidates"
                ),
            }
            if activation.reducer.streams() {
                for value in direct {
                    if activation.accepted.insert(value) {
                        accepted.push(value);
                    }
                }
            } else {
                accepted = direct;
            }
            for value in roots
                .iter()
                .filter(|output| output.accepted)
                .map(|output| output.node.value)
            {
                if activation.accepted.insert(value) {
                    accepted.push(value);
                }
            }
            activation
                .reducer
                .retain_quiescent_proposal_page(accepted.clone());
            had_stable_effect = activation.reducer.streams() && !accepted.is_empty();
            activation.suspended_source_page = Some(SuspendedSourcePage {
                next,
                had_stable_effect,
            });
        }

        let mut issued_roots = Vec::with_capacity(roots.len());
        for output in roots {
            issued_roots.push((
                output.node,
                self.issue_credit(generator.key.activation, CreditKind::Traversal),
            ));
        }
        let page_finished = {
            let activation = self
                .state
                .activations
                .get_mut(&generator.key.activation)
                .expect("unknown delta activation");
            assert_eq!(
                activation.live.remove(&generator.key.nonce),
                Some(CreditKind::Generator)
            );
            activation.live.is_empty()
        };
        let (resumed_source, retired_source_page, quiescence) = if page_finished {
            self.finish_source_page_or_activation(generator.key.activation)
        } else {
            (None, None, None)
        };
        SourcePageOutcome {
            roots: issued_roots,
            raw_proposal_occurrences: raw_stream_occurrences,
            accepted,
            resumed_source,
            retired_source_page,
            quiescence,
        }
    }

    fn new_program_join(
        &mut self,
        activation: ActivationId,
        remaining: usize,
        resume: Option<ProgramWork>,
        state: DeltaStateId,
        parent: Option<ProgramJoinId>,
        search_page: bool,
        source_telemetry: bool,
        had_stable_effect: bool,
    ) -> ProgramJoinId {
        assert!(remaining > 0, "program join requires at least one child");
        let join = ProgramJoinId(take_monotonic(
            &mut self.state.next_program_join,
            "program join",
        ));
        assert!(
            self.state
                .activations
                .get_mut(&activation)
                .expect("unknown program activation")
                .program_joins
                .insert(
                    join,
                    ProgramJoin {
                        remaining,
                        resume,
                        state,
                        parent,
                        search_page,
                        source_telemetry,
                        had_stable_effect,
                    },
                )
                .is_none(),
            "program join identifier was reused"
        );
        join
    }

    fn finish_program_join_member(
        &mut self,
        activation: ActivationId,
        mut join: ProgramJoinId,
    ) -> ProgramJoinCompletion {
        let mut dead_search_pages = 0usize;
        let mut dead_source_telemetry_pages = 0usize;
        loop {
            let completed = {
                let joins = &mut self
                    .state
                    .activations
                    .get_mut(&activation)
                    .expect("unknown program activation")
                    .program_joins;
                let record = joins.get_mut(&join).expect("unknown program join");
                record.remaining = record
                    .remaining
                    .checked_sub(1)
                    .expect("program join child retired twice");
                (record.remaining == 0).then(|| {
                    joins
                        .remove(&join)
                        .expect("completed program join disappeared")
                })
            };
            let Some(record) = completed else {
                return ProgramJoinCompletion {
                    scheduled: None,
                    dead_search_pages,
                    dead_source_telemetry_pages,
                };
            };
            dead_search_pages += usize::from(record.search_page && !record.had_stable_effect);
            dead_source_telemetry_pages +=
                usize::from(record.source_telemetry && !record.had_stable_effect);
            if let Some(resume) = record.resume {
                let credit = self.issue_credit(
                    activation,
                    CreditKind::Program {
                        join: record.parent,
                    },
                );
                return ProgramJoinCompletion {
                    scheduled: Some((record.state, resume, credit)),
                    dead_search_pages,
                    dead_source_telemetry_pages,
                };
            }
            let Some(parent) = record.parent else {
                return ProgramJoinCompletion {
                    scheduled: None,
                    dead_search_pages,
                    dead_source_telemetry_pages,
                };
            };
            // A barrier without a resume retires as one member of its parent
            // barrier. Continue iteratively so a final source page can close
            // an arbitrarily nested receipt tree without a sentinel task.
            join = parent;
        }
    }

    fn mark_program_join_stable_effect(
        &mut self,
        activation: ActivationId,
        mut join: Option<ProgramJoinId>,
    ) {
        while let Some(id) = join {
            let record = self
                .state
                .activations
                .get_mut(&activation)
                .expect("unknown program activation")
                .program_joins
                .get_mut(&id)
                .expect("program effect named an unknown join");
            record.had_stable_effect = true;
            join = record.parent;
        }
    }

    fn program_credit_within_search_page(&self, credit: &ProducerCredit) -> bool {
        let activation = self
            .state
            .activations
            .get(&credit.key.activation)
            .expect("unknown program activation");
        let mut join = match activation.live.get(&credit.key.nonce) {
            Some(CreditKind::Program { join }) => *join,
            _ => panic!("unknown, replayed, or wrong-kind program credit"),
        };
        while let Some(id) = join {
            let record = activation
                .program_joins
                .get(&id)
                .expect("program credit named an unknown join");
            if record.search_page {
                return true;
            }
            join = record.parent;
        }
        false
    }

    /// Whether this credit is the activation's sole live producer and is not
    /// nested below a receipt-local Program join. This is diagnostic evidence
    /// for an affine tail call; it does not itself transfer or replace credit.
    fn program_credit_is_unjoined_unique(&self, credit: &ProducerCredit) -> bool {
        let activation = self
            .state
            .activations
            .get(&credit.key.activation)
            .expect("unknown program activation");
        activation.live.len() == 1
            && matches!(
                activation.live.get(&credit.key.nonce),
                Some(CreditKind::Program { join: None })
            )
    }

    /// Replaces one opaque typed producer through the single affine law.
    ///
    /// Immediate resumes are siblings of admitted children. `AfterChildren`
    /// creates a receipt-local join whose final descendant releases exactly
    /// that resume; the engine never inspects the typed state that requested
    /// either disposition.
    fn replace_program(
        &mut self,
        parent: ProducerCredit,
        state: DeltaStateId,
        children: &[ProgramChild],
        observed: impl IntoIterator<Item = RawInline>,
        direct: impl IntoIterator<Item = RawInline>,
        reported_support: bool,
        search_page: bool,
        source_telemetry: bool,
        resume: Option<ProgramResume>,
    ) -> ProgramReplaceOutcome {
        assert_eq!(
            parent.brand, self.brand,
            "program credit crossed registries"
        );
        let activation_id = parent.key.activation;
        let parent_join = {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            assert_eq!(activation.status, ActivationStatus::Open);
            match activation.live.get(&parent.key.nonce) {
                Some(CreditKind::Program { join }) => *join,
                _ => panic!("unknown, replayed, or wrong-kind program credit"),
            }
        };

        let observed: Vec<_> = observed.into_iter().collect();
        let mut direct: Vec<_> = direct.into_iter().collect();
        let raw_stream_occurrences = {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            if activation.reducer.streams() {
                direct
                    .len()
                    .checked_add(observed.len())
                    .and_then(|count| {
                        count.checked_add(
                            children
                                .iter()
                                .filter(|child| child.accepted.is_some())
                                .count(),
                        )
                    })
                    .expect("typed proposal occurrence count overflow")
            } else {
                0
            }
        };
        let mut accepted: SmallVec<[RawInline; 1]> = SmallVec::new();
        {
            let activation = self
                .state
                .activations
                .get_mut(&activation_id)
                .expect("unknown program activation");
            match (&mut activation.reducer, &mut activation.return_to) {
                (DeltaReducer::QuiescentProposal { .. }, _) => {}
                (
                    DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal,
                    _,
                ) => {
                    for value in direct.drain(..) {
                        if activation.accepted.insert(value) {
                            accepted.push(value);
                        }
                    }
                }
                (DeltaReducer::FinalizingConfirm { output }, _) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine Confirm finalizer reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "Confirm finalizer reacquired mutable graph Accepted state"
                    );
                    if !direct.is_empty() {
                        let mut page = CandidatePayload::Values(std::mem::take(&mut direct));
                        page.defer_for_shared_activation(1);
                        output.extend_same_domain(page, 1);
                    }
                }
                (DeltaReducer::FinalizingProposal { output }, _) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine proposal materializer reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "proposal materializer reacquired mutable graph Accepted state"
                    );
                    append_one_parent_page(output, std::mem::take(&mut direct));
                }
                (
                    DeltaReducer::SetAdmit { output },
                    DeltaReturn::SetAdmission { .. },
                ) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine SET admission reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "SET admission acquired graph Accepted state"
                    );
                    append_one_parent_page(output, std::mem::take(&mut direct));
                }
                (
                    DeltaReducer::FormulaOrAdmit,
                    DeltaReturn::FormulaOrAdmit { batch, .. },
                ) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine Formula OR admission reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "Formula OR admission acquired graph Accepted state"
                    );
                    for value in direct.drain(..) {
                        batch.admit_current_or_value(0, value);
                    }
                }
                (
                    DeltaReducer::FormulaOrEmit { output },
                    DeltaReturn::FormulaOrEmit { .. },
                ) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine Formula OR emission reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "Formula OR emission acquired graph Accepted state"
                    );
                    if !direct.is_empty() {
                        let mut page = CandidatePayload::Values(std::mem::take(&mut direct));
                        page.defer_for_shared_activation(1);
                        output.extend_same_domain(page, 1);
                    }
                }
                (DeltaReducer::FormulaOrAdmit, _)
                | (DeltaReducer::FormulaOrEmit { .. }, _)
                | (DeltaReducer::SetAdmit { .. }, _) => {
                    panic!("engine reducer lost its exact affine return payload")
                }
                (DeltaReducer::Support { .. } | DeltaReducer::Confirm { .. }, _) => {
                    assert!(
                        direct.is_empty(),
                        "a non-proposal program reducer observed direct candidates"
                    )
                }
            }
            for value in observed
                .into_iter()
                .chain(children.iter().filter_map(|child| child.accepted))
            {
                if activation.accepted.insert(value) {
                    accepted.push(value);
                }
            }
            if matches!(activation.reducer, DeltaReducer::QuiescentProposal { .. }) {
                let mut retained = Vec::with_capacity(direct.len() + accepted.len());
                retained.extend(direct.iter().copied());
                retained.extend(accepted.iter().copied());
                activation
                    .reducer
                    .retain_quiescent_proposal_page(retained);
            }
        }

        let publishes_stable_effect = {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            match &activation.reducer {
                DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {
                    !accepted.is_empty()
                }
                DeltaReducer::Support { published } => {
                    !*published && (reported_support || !accepted.is_empty())
                }
                DeltaReducer::QuiescentProposal { .. }
                | DeltaReducer::Confirm { .. }
                | DeltaReducer::FinalizingConfirm { .. }
                | DeltaReducer::FinalizingProposal { .. }
                | DeltaReducer::SetAdmit { .. }
                | DeltaReducer::FormulaOrAdmit
                | DeltaReducer::FormulaOrEmit { .. } => false,
            }
        };
        if publishes_stable_effect {
            self.mark_program_join_stable_effect(activation_id, parent_join);
        }

        let no_replacement =
            children.is_empty() && matches!(&resume, None | Some(ProgramResume::AfterChildrenDone));
        let mut scheduled: SmallVec<[(DeltaStateId, ProgramWork, ProducerCredit); 2]> =
            SmallVec::new();
        match resume {
            Some(ProgramResume::AfterChildren(resume)) if !children.is_empty() => {
                let join = self.new_program_join(
                    activation_id,
                    children.len(),
                    Some(resume),
                    state,
                    parent_join,
                    search_page,
                    source_telemetry,
                    publishes_stable_effect,
                );
                for child in children {
                    let credit =
                        self.issue_credit(activation_id, CreditKind::Program { join: Some(join) });
                    scheduled.push((state, child.work.clone(), credit));
                }
            }
            Some(ProgramResume::AfterChildrenDone) if !children.is_empty() => {
                let join = self.new_program_join(
                    activation_id,
                    children.len(),
                    None,
                    state,
                    parent_join,
                    search_page,
                    source_telemetry,
                    publishes_stable_effect,
                );
                for child in children {
                    let credit =
                        self.issue_credit(activation_id, CreditKind::Program { join: Some(join) });
                    scheduled.push((state, child.work.clone(), credit));
                }
            }
            resume => {
                let immediate_resume = match resume {
                    Some(ProgramResume::Immediate(work) | ProgramResume::AfterChildren(work)) => {
                        Some(work)
                    }
                    Some(ProgramResume::AfterChildrenDone) | None => None,
                };
                let replacement_count = children.len() + usize::from(immediate_resume.is_some());
                if let Some(join) = parent_join {
                    let record = self
                        .state
                        .activations
                        .get_mut(&activation_id)
                        .expect("unknown program activation")
                        .program_joins
                        .get_mut(&join)
                        .expect("program parent named an unknown join");
                    if replacement_count > 0 {
                        record.remaining = record
                            .remaining
                            .checked_add(replacement_count - 1)
                            .expect("program join width overflow");
                    }
                }
                for child in children {
                    let credit =
                        self.issue_credit(activation_id, CreditKind::Program { join: parent_join });
                    scheduled.push((state, child.work.clone(), credit));
                }
                if let Some(work) = immediate_resume {
                    let credit =
                        self.issue_credit(activation_id, CreditKind::Program { join: parent_join });
                    scheduled.push((state, work, credit));
                }
            }
        }

        assert_eq!(
            self.state
                .activations
                .get_mut(&activation_id)
                .expect("unknown program activation")
                .live
                .remove(&parent.key.nonce),
            Some(CreditKind::Program { join: parent_join })
        );

        let mut dead_search_pages = 0usize;
        let mut dead_source_telemetry_pages = 0usize;
        if no_replacement {
            if let Some(join) = parent_join {
                let completed = self.finish_program_join_member(activation_id, join);
                dead_search_pages += completed.dead_search_pages;
                dead_source_telemetry_pages += completed.dead_source_telemetry_pages;
                if let Some(resumed) = completed.scheduled {
                    scheduled.push(resumed);
                }
            }
        }

        let quiescence = {
            let activation = self
                .state
                .activations
                .get_mut(&activation_id)
                .expect("unknown program activation");
            if activation.live.is_empty() {
                assert!(
                    activation.program_joins.is_empty(),
                    "program activation lost every credit behind a live join"
                );
                activation.status = ActivationStatus::Quiescent;
                Some(QuiescenceProof {
                    activation: activation_id,
                })
            } else {
                None
            }
        };
        ProgramReplaceOutcome {
            scheduled,
            raw_proposal_occurrences: raw_stream_occurrences,
            accepted,
            dead_search_pages,
            dead_source_telemetry_pages,
            quiescence,
        }
    }

    /// Complete either one suspended page or one eager activation. When a
    /// source cursor remains, the page's N traversal lineages join back into
    /// exactly one fresh affine generator credit.
    fn finish_source_page_or_activation(
        &mut self,
        activation_id: ActivationId,
    ) -> (
        Option<(ResidualDeltaSourceCursor, ProducerCredit)>,
        Option<RetiredSourcePage>,
        Option<QuiescenceProof>,
    ) {
        let page = {
            let activation = self
                .state
                .activations
                .get_mut(&activation_id)
                .expect("unknown delta activation");
            assert_eq!(activation.status, ActivationStatus::Open);
            assert!(activation.live.is_empty());
            activation.suspended_source_page.take()
        };
        let Some(page) = page else {
            self.state
                .activations
                .get_mut(&activation_id)
                .expect("unknown delta activation")
                .status = ActivationStatus::Quiescent;
            return (
                None,
                None,
                Some(QuiescenceProof {
                    activation: activation_id,
                }),
            );
        };

        let retired = Some(RetiredSourcePage {
            had_stable_effect: page.had_stable_effect,
        });
        if let Some(cursor) = page.next {
            let credit = self.issue_credit(activation_id, CreditKind::Generator);
            (Some((cursor, credit)), retired, None)
        } else {
            self.state
                .activations
                .get_mut(&activation_id)
                .expect("unknown delta activation")
                .status = ActivationStatus::Quiescent;
            (
                None,
                retired,
                Some(QuiescenceProof {
                    activation: activation_id,
                }),
            )
        }
    }

    fn source_context(
        &self,
        activation: ActivationId,
    ) -> (VariableSet, &[RawInline], Option<&[RawInline]>) {
        let activation = self
            .state
            .activations
            .get(&activation)
            .expect("unknown delta activation");
        let (bound, parent) = match &activation.return_to {
            DeltaReturn::Stable { desc, parent, .. } => (desc.bound, parent.as_ref()),
            DeltaReturn::Formula { bound, batch, .. }
            | DeltaReturn::FormulaOrAdmit { bound, batch, .. }
            | DeltaReturn::FormulaOrEmit { bound, batch, .. } => {
                assert_eq!(batch.parents.row_count, 1);
                (*bound, batch.parents.rows.as_slice())
            }
            DeltaReturn::SetAdmission {
                successor,
                destination,
            } => {
                assert_eq!(destination.parent_count(), 1);
                (successor.bound, destination.parent_rows())
            }
        };
        (bound, parent, Self::activation_candidates(activation))
    }

    fn activation_candidates(activation: &Activation) -> Option<&[RawInline]> {
        activation.source_candidates.as_deref().or_else(|| {
            match &activation.reducer {
                DeltaReducer::Confirm { original } => Some(original.one_parent_values()),
                _ => None,
            }
        })
    }

    fn source_dispatch_shape(&self, activation: ActivationId) -> (VariableSet, bool) {
        let activation = self
            .state
            .activations
            .get(&activation)
            .expect("unknown delta activation");
        let bound = match &activation.return_to {
            DeltaReturn::Stable { desc, .. } => desc.bound,
            DeltaReturn::Formula { bound, .. }
            | DeltaReturn::FormulaOrAdmit { bound, .. }
            | DeltaReturn::FormulaOrEmit { bound, .. } => *bound,
            DeltaReturn::SetAdmission { successor, .. } => successor.bound,
        };
        (bound, Self::activation_candidates(activation).is_some())
    }

    fn activation_streams(&self, activation: ActivationId) -> bool {
        self.state
            .activations
            .get(&activation)
            .expect("unknown delta activation")
            .reducer
            .streams()
    }

    fn physical_activation_class(&self, activation: ActivationId) -> DeltaPhysicalClass {
        self.state
            .activations
            .get(&activation)
            .expect("unknown delta activation")
            .physical_class
    }

    /// Transition expansion is activation-local sparse search. Confirmed
    /// demand may raise the outer search ceiling, but cannot itself make one
    /// traversal spend more of that ceiling.
    fn transition_dispatch_width(&self, activation: ActivationId, search_width: usize) -> usize {
        let activation = self
            .state
            .activations
            .get(&activation)
            .expect("unknown delta activation");
        if activation.physical_class == DeltaPhysicalClass::TerminalStreaming {
            activation
                .terminal_sparse_quantum
                .min(search_width.max(1))
                .max(1)
        } else {
            search_width.max(1)
        }
    }

    /// Source paging discovers independent roots for one admitted parent. It
    /// is search work, rather than graph traversal effort, so it receives the
    /// outer search width (which confirmed projected demand may floor).
    fn source_dispatch_width(&self, activation: ActivationId, search_width: usize) -> usize {
        assert!(
            self.state.activations.contains_key(&activation),
            "unknown delta activation"
        );
        search_width.max(1)
    }

    /// Updates only physical sparse-search effort. Publication from either
    /// layer resets to one; a live transition no-publication step doubles
    /// toward `search_width`, while a source miss leaves traversal effort
    /// unchanged. Confirmed result demand may widen source/nonterminal search,
    /// but is not itself evidence that one traversal should become broader.
    fn finish_dispatch(
        &mut self,
        activation: ActivationId,
        search_width: usize,
        kind: PhysicalDispatchKind,
        published: bool,
    ) -> (bool, bool) {
        let Some(activation) = self.state.activations.get_mut(&activation) else {
            return (false, false);
        };
        if activation.physical_class != DeltaPhysicalClass::TerminalStreaming {
            return (false, false);
        }
        let before = activation.terminal_sparse_quantum;
        if published {
            activation.terminal_sparse_quantum = 1;
        } else if matches!(
            kind,
            PhysicalDispatchKind::Transition | PhysicalDispatchKind::Program
        ) {
            activation.terminal_sparse_quantum =
                before.saturating_mul(2).min(search_width.max(1)).max(1);
        }
        (
            published && activation.terminal_sparse_quantum != before,
            !published && activation.terminal_sparse_quantum > before,
        )
    }

    fn is_live(&self, activation: ActivationId) -> bool {
        self.state.activations.contains_key(&activation)
    }

    /// Takes one activation-local early effect. Support mutates its reducer at
    /// this exact boundary so duplicate witnesses and later expansion cohorts
    /// cannot replay `true`.
    fn take_streaming_return(&mut self, activation: ActivationId) -> Option<DeltaStreamingReturn> {
        let activation = self
            .state
            .activations
            .get_mut(&activation)
            .expect("unknown delta activation");
        let effect = match &mut activation.reducer {
            DeltaReducer::StreamProposal => {
                assert!(matches!(&activation.return_to, DeltaReturn::Stable { .. }));
                DeltaStreamingEffect::Candidates
            }
            DeltaReducer::StreamFormulaProposal => {
                assert!(matches!(&activation.return_to, DeltaReturn::Formula { .. }));
                DeltaStreamingEffect::Candidates
            }
            DeltaReducer::Support { published } if !*published => {
                assert!(matches!(&activation.return_to, DeltaReturn::Formula { .. }));
                *published = true;
                DeltaStreamingEffect::Support
            }
            DeltaReducer::Support { .. }
            | DeltaReducer::QuiescentProposal { .. }
            | DeltaReducer::Confirm { .. }
            | DeltaReducer::FinalizingConfirm { .. }
            | DeltaReducer::FinalizingProposal { .. }
            | DeltaReducer::SetAdmit { .. }
            | DeltaReducer::FormulaOrAdmit
            | DeltaReducer::FormulaOrEmit { .. } => return None,
        };
        Some(DeltaStreamingReturn {
            return_to: activation.return_to.clone(),
            effect,
        })
    }

    /// Observes an idempotent typed Boolean support effect.
    ///
    /// The first witness publishes `true`; later witnesses from independent
    /// pages in the same activation are harmless. Reporting this effect to a
    /// non-support reducer remains a contract violation.
    fn take_program_support_return(
        &mut self,
        activation: ActivationId,
    ) -> Option<DeltaStreamingReturn> {
        let activation = self
            .state
            .activations
            .get_mut(&activation)
            .expect("unknown delta activation");
        let DeltaReducer::Support { published } = &mut activation.reducer else {
            panic!("typed support observation reached a non-support reducer")
        };
        if *published {
            return None;
        }
        *published = true;
        Some(DeltaStreamingReturn {
            return_to: activation.return_to.clone(),
            effect: DeltaStreamingEffect::Support,
        })
    }

    /// Consumes a synchronous zero-rank or already-finalized quiescence proof.
    /// Graph callers enter through [`Self::settle_quiescence`], which delegates
    /// here only when no private finite Program remains to be opened.
    fn finish(&mut self, proof: QuiescenceProof) -> CompletedActivation {
        let activation = self
            .state
            .activations
            .remove(&proof.activation)
            .expect("unknown delta activation");
        assert_eq!(activation.status, ActivationStatus::Quiescent);
        assert!(activation.live.is_empty());

        let effect = match activation.reducer {
            DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {
                DeltaCompletion::Cleanup
            }
            DeltaReducer::QuiescentProposal { occurrences } => {
                assert!(
                    occurrences.is_empty() && activation.accepted.is_empty(),
                    "nonempty proposal bypassed its pageable materializer"
                );
                DeltaCompletion::Candidates(occurrences)
            }
            DeltaReducer::Support { published: true } => {
                DeltaCompletion::Cleanup
            }
            DeltaReducer::Support { published: false } => {
                assert!(
                    activation.accepted.is_empty(),
                    "an unpublished support reducer quiesced with a witness"
                );
                DeltaCompletion::Support(false)
            }
            DeltaReducer::Confirm { original } => {
                let result = original
                    .iter()
                    .filter_map(|(parent, candidate)| {
                        assert_eq!(parent, 0, "one-parent Confirm changed domains");
                        activation.accepted.contains(&candidate).then_some(candidate)
                    })
                    .collect();
                DeltaCompletion::Candidates(CandidatePayload::Values(result))
            }
            DeltaReducer::FinalizingConfirm { output } => {
                assert!(
                    activation.accepted.is_empty(),
                    "Confirm finalizer retained the mutable graph accepted set"
                );
                DeltaCompletion::Candidates(output)
            }
            DeltaReducer::FinalizingProposal { output } => {
                assert!(
                    activation.accepted.is_empty(),
                    "proposal materializer retained graph Accepted state"
                );
                DeltaCompletion::Candidates(output)
            }
            DeltaReducer::SetAdmit { output } => {
                assert!(
                    activation.accepted.is_empty(),
                    "SET admission retained graph Accepted state"
                );
                assert!(matches!(
                    &activation.return_to,
                    DeltaReturn::SetAdmission { .. }
                ));
                DeltaCompletion::Candidates(output)
            }
            DeltaReducer::FormulaOrAdmit => {
                assert!(
                    activation.accepted.is_empty(),
                    "Formula OR admission retained graph Accepted state"
                );
                assert!(matches!(
                    &activation.return_to,
                    DeltaReturn::FormulaOrAdmit { .. }
                ));
                DeltaCompletion::FormulaOrAdmitted
            }
            DeltaReducer::FormulaOrEmit { output } => {
                assert!(
                    activation.accepted.is_empty(),
                    "Formula OR emission retained graph Accepted state"
                );
                assert!(matches!(
                    &activation.return_to,
                    DeltaReturn::FormulaOrEmit { .. }
                ));
                DeltaCompletion::Candidates(output)
            }
        };
        CompletedActivation {
            activation: proof.activation,
            return_to: activation.return_to,
            effect,
        }
    }

    /// Settles one graph/reducer quiescence boundary without hiding whole-bag
    /// work inside the affine receipt.
    ///
    /// Nonempty quiescent proposals always transfer to the private
    /// Seal/Merge/Emit Program. Stable and finite-formula Confirm activations
    /// transfer whenever their live candidate frame can be finalized
    /// independently. Engine-owned Formula OR reducers settle directly
    /// through their own pageable Program families.
    fn settle_quiescence(&mut self, proof: QuiescenceProof) -> RegistrySettlement {
        enum Handoff {
            Complete,
            Confirm,
            Proposal,
        }

        let handoff = {
            let activation = self
                .state
                .activations
                .get(&proof.activation)
                .expect("unknown delta activation");
            assert_eq!(activation.status, ActivationStatus::Quiescent);
            assert!(activation.live.is_empty());
            let eligible_return = match &activation.return_to {
                DeltaReturn::Stable { .. } => true,
                DeltaReturn::Formula { batch, .. } => batch.confirm_finalizer_capable(),
                DeltaReturn::FormulaOrAdmit { .. }
                | DeltaReturn::FormulaOrEmit { .. }
                | DeltaReturn::SetAdmission { .. } => false,
            };
            match &activation.reducer {
                DeltaReducer::QuiescentProposal { occurrences }
                    if !occurrences.is_empty() => Handoff::Proposal,
                DeltaReducer::Confirm { original }
                    if eligible_return && !original.is_empty() => Handoff::Confirm,
                _ => Handoff::Complete,
            }
        };

        match handoff {
            Handoff::Complete => RegistrySettlement::Completed(self.finish(proof)),
            Handoff::Proposal => {
                let state = {
                    let activation = self
                        .state
                        .activations
                        .get_mut(&proof.activation)
                        .expect("unknown delta activation");
                    assert!(
                        activation.program_joins.is_empty(),
                        "proposal graph quiesced behind a live Program join"
                    );
                    assert!(
                        activation.suspended_source_page.is_none(),
                        "proposal graph quiesced with a suspended source page"
                    );
                    let reducer = std::mem::replace(
                        &mut activation.reducer,
                        DeltaReducer::FinalizingProposal {
                            output: empty_one_parent_rope(),
                        },
                    );
                    let DeltaReducer::QuiescentProposal { occurrences } = reducer else {
                        unreachable!("proposal materializer settlement lost its reducer")
                    };
                    let state = ProposalMaterializerState::start(occurrences)
                        .expect("nonempty proposal failed to open its materializer");
                    activation.accepted = AHashSet::new();
                    activation.seen = AHashMap::new();
                    activation.program_joins = AHashMap::new();
                    activation.source_candidates = None;
                    activation.status = ActivationStatus::Open;
                    state
                };
                let credit = self.issue_credit(
                    proof.activation,
                    CreditKind::Program { join: None },
                );
                RegistrySettlement::ProposalMaterializer(ProposalMaterializerSeed {
                    activation: proof.activation,
                    state,
                    credit,
                })
            }
            Handoff::Confirm => {
                let state = {
                    let activation = self
                        .state
                        .activations
                        .get_mut(&proof.activation)
                        .expect("unknown delta activation");
                    assert!(
                        activation.program_joins.is_empty(),
                        "Confirm graph quiesced behind a live Program join"
                    );
                    assert!(
                        activation.suspended_source_page.is_none(),
                        "Confirm graph quiesced with a suspended source page"
                    );
                    let reducer = std::mem::replace(
                        &mut activation.reducer,
                        DeltaReducer::FinalizingConfirm {
                            output: empty_one_parent_rope(),
                        },
                    );
                    let DeltaReducer::Confirm { original } = reducer else {
                        unreachable!("Confirm finalizer settlement lost its reducer")
                    };
                    let original = original.shared_one_parent_cursor();
                    let accepted = Arc::new(std::mem::take(&mut activation.accepted));
                    // Drop graph-only allocation capacity at the handoff,
                    // rather than merely making it unreachable behind the
                    // finalizer state.
                    activation.seen = AHashMap::new();
                    activation.program_joins = AHashMap::new();
                    activation.source_candidates = None;
                    activation.status = ActivationStatus::Open;
                    ConfirmFinalizerState { original, accepted }
                };
                let credit = self.issue_credit(
                    proof.activation,
                    CreditKind::Program { join: None },
                );
                RegistrySettlement::ConfirmFinalizer(ConfirmFinalizerSeed {
                    activation: proof.activation,
                    state,
                    credit,
                })
            }
        }
    }

    fn deep_clone(&self) -> (Self, BTreeMap<CreditKey, ProducerCredit>) {
        let state = self.state.clone();
        let brand = RegistryBrand::fresh();
        let mut remap = BTreeMap::new();
        for (&activation, state) in &state.activations {
            for &nonce in state.live.keys() {
                let key = CreditKey { activation, nonce };
                assert!(
                    remap.insert(key, ProducerCredit { brand, key }).is_none(),
                    "live delta credit appeared twice"
                );
            }
        }
        (Self { brand, state }, remap)
    }
}

fn take_monotonic(counter: &mut u64, kind: &str) -> u64 {
    let current = *counter;
    *counter = current
        .checked_add(1)
        .unwrap_or_else(|| panic!("delta {kind} identifier space exhausted"));
    current
}

fn shared_one_parent_candidates(values: Vec<RawInline>) -> CandidatePayload {
    let mut payload = CandidatePayload::Values(values);
    payload.defer_for_shared_activation(1);
    payload
}

fn validate_seed_tags(seeds: &[ResidualDeltaSeed], parent_count: usize) {
    let mut previous = 0u32;
    for (index, seed) in seeds.iter().enumerate() {
        assert!(
            (seed.parent as usize) < parent_count,
            "delta seed parent tag out of range"
        );
        assert!(
            index == 0 || seed.parent >= previous,
            "delta seed parent tags are not grouped in ascending order"
        );
        previous = seed.parent;
    }
}

fn seed_ranges(seeds: &[ResidualDeltaSeed], parent_count: usize) -> Vec<std::ops::Range<usize>> {
    validate_seed_tags(seeds, parent_count);
    let mut ranges = Vec::with_capacity(parent_count);
    let mut cursor = 0usize;
    for parent in 0..parent_count {
        let begin = cursor;
        while cursor < seeds.len() && seeds[cursor].parent as usize == parent {
            cursor += 1;
        }
        ranges.push(begin..cursor);
    }
    assert_eq!(
        cursor,
        seeds.len(),
        "delta seed parent tags skipped a range"
    );
    ranges
}

fn program_seed_ranges(
    seeds: &[ProgramSeedWork],
    parent_count: usize,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::with_capacity(parent_count);
    let mut cursor = 0usize;
    for parent in 0..parent_count {
        let begin = cursor;
        while cursor < seeds.len() && seeds[cursor].parent as usize == parent {
            cursor += 1;
        }
        ranges.push(begin..cursor);
    }
    assert_eq!(
        cursor,
        seeds.len(),
        "typed program seed tags skipped a parent range"
    );
    ranges
}

fn program_child_ranges(
    children: &[ProgramChild],
    input_count: usize,
) -> SmallVec<[std::ops::Range<usize>; 1]> {
    let mut ranges = SmallVec::with_capacity(input_count);
    let mut cursor = 0usize;
    for input in 0..input_count {
        let begin = cursor;
        while cursor < children.len() && children[cursor].input as usize == input {
            cursor += 1;
        }
        ranges.push(begin..cursor);
    }
    assert_eq!(
        cursor,
        children.len(),
        "typed program child tags skipped an input range"
    );
    ranges
}

fn tagged_ranges<T>(
    values: &[(u32, T)],
    parent_count: usize,
    kind: &str,
) -> SmallVec<[std::ops::Range<usize>; 1]> {
    let mut ranges = SmallVec::with_capacity(parent_count);
    let mut cursor = 0usize;
    for parent in 0..parent_count {
        let begin = cursor;
        while cursor < values.len() && values[cursor].0 as usize == parent {
            cursor += 1;
        }
        ranges.push(begin..cursor);
    }
    assert_eq!(
        cursor,
        values.len(),
        "residual {kind} tags are out of range or not grouped in ascending order"
    );
    ranges
}

#[derive(Debug)]
struct DeltaTask {
    activation: ActivationId,
    credit: ProducerCredit,
    node: ResidualDeltaNode,
    cursor: ResidualDeltaExpandCursor,
}

/// Physical transition cohort compatible with one publication boundary.
///
/// Streaming activations may share a block because every accepted endpoint is
/// immediately visible to the stable machine. A quiescent reducer must finish
/// its own fixpoint before it can publish, so mixing independent activations
/// would turn geometric width into breadth and postpone every first result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransitionDispatchKey {
    /// Terminal streams share one physical class while retaining independent
    /// affine reducers and sparse-search quanta as activation payload.
    TerminalStreaming,
    Streaming,
    Quiescent(ActivationId),
}

impl TransitionDispatchKey {
    fn for_activation(registry: &ProducerRegistry, activation: ActivationId) -> Self {
        if registry.physical_activation_class(activation) == DeltaPhysicalClass::TerminalStreaming {
            Self::TerminalStreaming
        } else if registry.activation_streams(activation) {
            Self::Streaming
        } else {
            Self::Quiescent(activation)
        }
    }

    fn of(registry: &ProducerRegistry, task: &DeltaTask) -> Self {
        Self::for_activation(registry, task.activation)
    }
}

#[derive(Debug)]
struct ProgramTask {
    activation: ActivationId,
    credit: ProducerCredit,
    work: ProgramWork,
}

/// Physical Program-call class after removing activation-local reducer state.
///
/// Search pages may mix streaming and quiescent reducers because reducer
/// finalization happens after the typed call and does not change its physical
/// source shape. Activation-paced work retains the publication distinction:
/// streaming work may use every compatible activation, while quiescent work
/// admits only a bounded number of independent reducers. Terminal streaming
/// remains its own physical feedback class at either pacing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramCohortClass {
    Search { physical: DeltaPhysicalClass },
    ActivationStreaming,
    ActivationQuiescent,
    ActivationTerminalStreaming,
}

impl ProgramCohortClass {
    fn of(registry: &ProducerRegistry, task: &ProgramTask) -> Self {
        let physical = registry.physical_activation_class(task.activation);
        match task.work.pacing {
            ProgramPacing::Search => Self::Search { physical },
            ProgramPacing::Activation if physical == DeltaPhysicalClass::TerminalStreaming => {
                Self::ActivationTerminalStreaming
            }
            ProgramPacing::Activation if registry.activation_streams(task.activation) => {
                Self::ActivationStreaming
            }
            ProgramPacing::Activation => Self::ActivationQuiescent,
        }
    }

    fn pacing(self) -> ProgramPacing {
        match self {
            Self::Search { .. } => ProgramPacing::Search,
            Self::ActivationStreaming
            | Self::ActivationQuiescent
            | Self::ActivationTerminalStreaming => ProgramPacing::Activation,
        }
    }
}

/// Exact compatibility key for one erased typed Program call.
///
/// Activation identity deliberately remains on [`ProgramTask`]. It is reducer
/// payload and affine feedback authority, not a property of the physical call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProgramCohortKey {
    dispatch: DispatchClass,
    bound: VariableSet,
    has_candidates: bool,
    class: ProgramCohortClass,
}

impl ProgramCohortKey {
    fn of(registry: &ProducerRegistry, task: &ProgramTask) -> Self {
        let (bound, has_candidates) = registry.source_dispatch_shape(task.activation);
        Self {
            dispatch: task.work.dispatch,
            bound,
            has_candidates,
            class: ProgramCohortClass::of(registry, task),
        }
    }
}

#[derive(Clone, Copy)]
enum ProgramSelectionOrder {
    Lifo,
    Append,
}

struct ProgramSelection {
    key: ProgramCohortKey,
    tasks: Vec<ProgramTask>,
    limits: Vec<usize>,
}

#[derive(Default)]
struct ProgramBucket {
    tasks: Vec<ProgramTask>,
}

impl ProgramBucket {
    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn last(&self) -> Option<&ProgramTask> {
        self.tasks.last()
    }

    fn append(&mut self, tasks: &mut Vec<ProgramTask>) {
        self.tasks.append(tasks);
    }

    fn contains_activation(&self, activation: ActivationId) -> bool {
        self.tasks.iter().any(|task| task.activation == activation)
    }

    /// Takes the newest matching tasks, preserving either physical LIFO order
    /// or their original append order. Retained tasks always keep append
    /// order, so selection is a stable partition of the stored bucket.
    fn take_matching(
        &mut self,
        width: usize,
        order: ProgramSelectionOrder,
        mut matches: impl FnMut(&ProgramTask) -> bool,
    ) -> Vec<ProgramTask> {
        let width = width.max(1);
        let mut selected = Vec::with_capacity(width.min(self.tasks.len()));
        let mut retained = Vec::with_capacity(self.tasks.len());
        for task in std::mem::take(&mut self.tasks).into_iter().rev() {
            if selected.len() < width && matches(&task) {
                selected.push(task);
            } else {
                retained.push(task);
            }
        }
        retained.reverse();
        self.tasks = retained;
        if matches!(order, ProgramSelectionOrder::Append) {
            selected.reverse();
        }
        selected
    }

    /// Directed latency selection remains activation-affine. Search pages use
    /// their established LIFO cursor order; transition-like Activation pages
    /// return the selected suffix in append order so geometric feedback sees
    /// the same progression as the legacy delta queue.
    fn take_active(
        &mut self,
        registry: &ProducerRegistry,
        activation: ActivationId,
        search_width: usize,
    ) -> ProgramSelection {
        let key = self
            .tasks
            .iter()
            .rev()
            .find(|task| task.activation == activation)
            .map(|task| ProgramCohortKey::of(registry, task))
            .expect("active typed program lost its affine task");
        let width = match key.class.pacing() {
            ProgramPacing::Search => registry.source_dispatch_width(activation, search_width),
            ProgramPacing::Activation => {
                registry.transition_dispatch_width(activation, search_width)
            }
        };
        let order = match key.class.pacing() {
            ProgramPacing::Search => ProgramSelectionOrder::Lifo,
            ProgramPacing::Activation => ProgramSelectionOrder::Append,
        };
        let tasks = self.take_matching(width, order, |task| {
            task.activation == activation && ProgramCohortKey::of(registry, task) == key
        });
        assert!(!tasks.is_empty(), "active typed program pop was empty");
        let limits = even_limits(width, tasks.len());
        ProgramSelection { key, tasks, limits }
    }

    /// Selects one global physical Program cohort. The hot tail chooses the
    /// exact normalized call key; class-specific policy controls only order,
    /// activation breadth, and terminal per-activation budgets.
    fn take_global(
        &mut self,
        registry: &ProducerRegistry,
        search_width: usize,
        activation_width: usize,
        terminal_selection_slots: &mut AHashMap<ActivationId, usize>,
        terminal_selections: &mut Vec<TerminalActivationSelection>,
    ) -> ProgramSelection {
        let hot = self.last().expect("typed program bucket is nonempty");
        let key = ProgramCohortKey::of(registry, hot);
        match key.class {
            ProgramCohortClass::Search { .. } => {
                let width = registry.source_dispatch_width(hot.activation, search_width);
                let tasks = self.take_matching(width, ProgramSelectionOrder::Lifo, |task| {
                    ProgramCohortKey::of(registry, task) == key
                });
                let limits = even_limits(width, tasks.len());
                ProgramSelection { key, tasks, limits }
            }
            ProgramCohortClass::ActivationStreaming => {
                let width = registry.transition_dispatch_width(hot.activation, search_width);
                let tasks = self.take_matching(width, ProgramSelectionOrder::Append, |task| {
                    ProgramCohortKey::of(registry, task) == key
                });
                let limits = even_limits(width, tasks.len());
                ProgramSelection { key, tasks, limits }
            }
            ProgramCohortClass::ActivationQuiescent => {
                let width = registry.transition_dispatch_width(hot.activation, search_width);
                let activation_width = activation_width.max(1);
                let mut activations = AHashSet::new();
                let tasks = self.take_matching(width, ProgramSelectionOrder::Append, |task| {
                    if ProgramCohortKey::of(registry, task) != key {
                        return false;
                    }
                    activations.contains(&task.activation)
                        || (activations.len() < activation_width
                            && activations.insert(task.activation))
                });
                let limits = even_limits(width, tasks.len());
                ProgramSelection { key, tasks, limits }
            }
            ProgramCohortClass::ActivationTerminalStreaming => self.take_terminal(
                registry,
                key,
                search_width,
                terminal_selection_slots,
                terminal_selections,
            ),
        }
    }

    /// Assigns each admitted terminal activation its independent sparse
    /// quantum, selects the newest tasks covered by those grants, then returns
    /// `(task, limit)` pairs in original append order. Ordering the pair—not
    /// merely the task—keeps budgets aligned when activations are interleaved.
    fn take_terminal(
        &mut self,
        registry: &ProducerRegistry,
        key: ProgramCohortKey,
        search_width: usize,
        terminal_selection_slots: &mut AHashMap<ActivationId, usize>,
        terminal_selections: &mut Vec<TerminalActivationSelection>,
    ) -> ProgramSelection {
        let width = search_width.max(1);
        let tasks = std::mem::take(&mut self.tasks);
        let mut remaining = width;
        terminal_selection_slots.clear();
        terminal_selections.clear();
        for task in tasks.iter().rev() {
            if ProgramCohortKey::of(registry, task) != key
                || terminal_selection_slots.contains_key(&task.activation)
            {
                continue;
            }
            let budget = registry
                .transition_dispatch_width(task.activation, search_width)
                .min(remaining);
            let slot = terminal_selections.len();
            terminal_selections.push(TerminalActivationSelection {
                activation: task.activation,
                budget,
                selected: 0,
                ordinal: 0,
            });
            terminal_selection_slots.insert(task.activation, slot);
            remaining -= budget;
            if remaining == 0 {
                break;
            }
        }

        let mut selected = Vec::new();
        let mut retained = Vec::with_capacity(tasks.len());
        for task in tasks.into_iter().rev() {
            let selection = (ProgramCohortKey::of(registry, &task) == key)
                .then(|| terminal_selection_slots.get(&task.activation).copied())
                .flatten();
            if let Some(slot) = selection.filter(|&slot| {
                terminal_selections[slot].selected < terminal_selections[slot].budget
            }) {
                terminal_selections[slot].selected += 1;
                selected.push(task);
            } else {
                retained.push(task);
            }
        }
        selected.reverse();
        retained.reverse();
        self.tasks = retained;

        let mut limits = Vec::with_capacity(selected.len());
        for task in &selected {
            let selection = &mut terminal_selections[terminal_selection_slots[&task.activation]];
            debug_assert!(selection.selected > 0);
            let quotient = selection.budget / selection.selected;
            let remainder = selection.budget % selection.selected;
            limits.push(quotient + usize::from(selection.ordinal < remainder));
            selection.ordinal += 1;
        }
        debug_assert!(limits.iter().all(|&limit| limit > 0));
        ProgramSelection {
            key,
            tasks: selected,
            limits,
        }
    }
}

#[derive(Debug)]
struct SourceTask {
    activation: ActivationId,
    credit: ProducerCredit,
    cursor: ResidualDeltaSourceCursor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceCursorFamily {
    Start,
    After,
    Offset,
}

impl SourceCursorFamily {
    fn of(cursor: ResidualDeltaSourceCursor) -> Self {
        match cursor {
            ResidualDeltaSourceCursor::Start => Self::Start,
            ResidualDeltaSourceCursor::After(_) => Self::After,
            ResidualDeltaSourceCursor::Offset(_) => Self::Offset,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SourceDispatchKey {
    bound: VariableSet,
    has_candidates: bool,
    cursor_family: SourceCursorFamily,
    /// Physical latency class is payload-only, but source calls must not mix
    /// terminal demand feedback with ordinary cyclic work.
    physical_class: DeltaPhysicalClass,
}

impl SourceDispatchKey {
    fn of(registry: &ProducerRegistry, task: &SourceTask) -> Self {
        let (bound, has_candidates) = registry.source_dispatch_shape(task.activation);
        Self {
            bound,
            has_candidates,
            cursor_family: SourceCursorFamily::of(task.cursor),
            physical_class: registry.physical_activation_class(task.activation),
        }
    }
}

fn validate_source_cursor(
    current: ResidualDeltaSourceCursor,
    next: Option<ResidualDeltaSourceCursor>,
) {
    let Some(next) = next else {
        return;
    };
    match (current, next) {
        (
            ResidualDeltaSourceCursor::Start,
            ResidualDeltaSourceCursor::After(_) | ResidualDeltaSourceCursor::Offset(1..),
        ) => {}
        (ResidualDeltaSourceCursor::After(previous), ResidualDeltaSourceCursor::After(next)) => {
            assert!(next > previous, "residual source cursor did not advance")
        }
        (ResidualDeltaSourceCursor::Offset(previous), ResidualDeltaSourceCursor::Offset(next)) => {
            assert!(next > previous, "residual source cursor did not advance")
        }
        (_, ResidualDeltaSourceCursor::Start) => {
            panic!("residual source page restarted its cursor")
        }
        _ => panic!("residual source page changed cursor families"),
    }
}

enum DeltaBucket {
    /// The ordinary representation keeps small, nonterminal, and formula
    /// buckets on the original contiguous-vector path.
    Plain(Vec<DeltaTask>),
    /// Terminal activation-affine extraction promotes a bucket once mixed
    /// work would otherwise require repeated whole-vector partitions.
    Indexed(IndexedDeltaBucket),
}

impl Default for DeltaBucket {
    fn default() -> Self {
        Self::Plain(Vec::new())
    }
}

#[derive(Default)]
struct IndexedDeltaBucket {
    /// Append-order task arena. Removed tasks become tombstones until dead
    /// slots exceed the live payload.
    tasks: Vec<Option<DeltaTask>>,
    /// Live arena slots for each affine activation, in append order.
    activation_slots: AHashMap<ActivationId, Vec<usize>>,
    /// The latest live slot for every activation. Ordering these tails is
    /// equivalent to discovering activations during the old reverse scan.
    activation_tails: BTreeMap<usize, ActivationId>,
    live_tasks: usize,
}

enum DeltaBucketIter<'a> {
    Plain(std::slice::Iter<'a, DeltaTask>),
    Indexed(std::slice::Iter<'a, Option<DeltaTask>>),
}

impl<'a> Iterator for DeltaBucketIter<'a> {
    type Item = &'a DeltaTask;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(tasks) => tasks.next(),
            Self::Indexed(tasks) => tasks.find_map(Option::as_ref),
        }
    }
}

#[derive(Default)]
struct SourceBucket {
    tasks: Vec<SourceTask>,
}

#[derive(Debug)]
struct PhysicalDispatch {
    terminal_activations: OrderedActivationSet,
    /// Assigned work and the activation-local quantum in force before this
    /// dispatch. Cohort totals are never evidence that one affine activation
    /// saturated its own sparse search budget.
    terminal_budgets: Vec<TerminalActivationBudget>,
    kind: PhysicalDispatchKind,
    task_limits: Vec<usize>,
    remainder_tasks: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TerminalActivationBudget {
    activation: ActivationId,
    assigned: usize,
    quantum: usize,
}

impl PhysicalDispatch {
    fn new(
        registry: &ProducerRegistry,
        kind: PhysicalDispatchKind,
        search_width: usize,
        task_activations: impl IntoIterator<Item = ActivationId>,
        task_limits: Vec<usize>,
        remainder_tasks: usize,
    ) -> Self {
        let mut terminal_budgets: Vec<TerminalActivationBudget> = Vec::new();
        let mut slots = AHashMap::new();
        let activations: Vec<_> = task_activations.into_iter().collect();
        assert_eq!(activations.len(), task_limits.len());
        for (activation, &assigned) in activations.iter().zip(&task_limits) {
            if registry.physical_activation_class(*activation)
                != DeltaPhysicalClass::TerminalStreaming
            {
                continue;
            }
            let slot = *slots.entry(*activation).or_insert_with(|| {
                let quantum = match kind {
                    PhysicalDispatchKind::Source => {
                        registry.source_dispatch_width(*activation, search_width)
                    }
                    PhysicalDispatchKind::Transition | PhysicalDispatchKind::Program => {
                        registry.transition_dispatch_width(*activation, search_width)
                    }
                };
                terminal_budgets.push(TerminalActivationBudget {
                    activation: *activation,
                    assigned: 0,
                    quantum,
                });
                terminal_budgets.len() - 1
            });
            terminal_budgets[slot].assigned = terminal_budgets[slot]
                .assigned
                .checked_add(assigned)
                .expect("terminal activation work budget overflow");
        }
        let terminal_activations = terminal_budgets
            .iter()
            .map(|receipt| receipt.activation)
            .collect();
        assert!(
            terminal_budgets
                .iter()
                .all(|receipt| receipt.assigned <= receipt.quantum),
            "one terminal activation was assigned beyond its local physical quantum"
        );
        Self {
            terminal_activations,
            terminal_budgets,
            kind,
            task_limits,
            remainder_tasks,
        }
    }

    fn work_budget(&self) -> usize {
        self.task_limits.iter().sum()
    }

    fn task_count(&self) -> usize {
        self.task_limits.len()
    }
}

struct DeltaPhysicalOutcome {
    outcome: DeltaStepOutcome,
    terminal_publications: OrderedActivationSet,
    /// A Search-paced receipt completed under a descendant's physical
    /// Activation dispatch. It still owns outer geometric feedback.
    retired_search_receipt: bool,
}

enum DeltaSettlement {
    Completed(CompletedActivation),
    Retargeted(ActiveDeltaContinuation),
}

/// Insertion-ordered activation membership with an allocation-free singleton
/// lookup. Physical cohorts observe activation order while repeated feedback
/// checks need set rather than quadratic vector membership.
#[derive(Debug, Default)]
struct OrderedActivationSet {
    values: Vec<ActivationId>,
    membership: Option<AHashSet<ActivationId>>,
}

impl OrderedActivationSet {
    fn insert(&mut self, activation: ActivationId) -> bool {
        match self.values.as_slice() {
            [] => {
                self.values.push(activation);
                true
            }
            [only] if *only == activation => false,
            [only] => {
                let mut membership = AHashSet::with_capacity(2);
                assert!(membership.insert(*only));
                assert!(membership.insert(activation));
                self.membership = Some(membership);
                self.values.push(activation);
                true
            }
            _ => {
                if self
                    .membership
                    .as_mut()
                    .expect("multi-activation set lost its membership index")
                    .insert(activation)
                {
                    self.values.push(activation);
                    true
                } else {
                    false
                }
            }
        }
    }

    fn contains(&self, activation: &ActivationId) -> bool {
        match self.values.as_slice() {
            [] => false,
            [only] => only == activation,
            _ => self
                .membership
                .as_ref()
                .expect("multi-activation set lost its membership index")
                .contains(activation),
        }
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> impl Iterator<Item = &ActivationId> {
        self.values.iter()
    }
}

impl From<Vec<ActivationId>> for OrderedActivationSet {
    fn from(activations: Vec<ActivationId>) -> Self {
        activations.into_iter().collect()
    }
}

impl FromIterator<ActivationId> for OrderedActivationSet {
    fn from_iter<T: IntoIterator<Item = ActivationId>>(iter: T) -> Self {
        let mut activations = Self::default();
        for activation in iter {
            let _ = activations.insert(activation);
        }
        activations
    }
}

#[derive(Debug)]
struct TerminalActivationSelection {
    activation: ActivationId,
    budget: usize,
    selected: usize,
    ordinal: usize,
}

fn even_limits(work_budget: usize, task_count: usize) -> Vec<usize> {
    assert!(
        task_count > 0,
        "a physical dispatch requires at least one task"
    );
    assert!(
        task_count <= work_budget,
        "every physical task requires at least one work unit"
    );
    let quotient = work_budget / task_count;
    let remainder = work_budget % task_count;
    let limits: Vec<_> = (0..task_count)
        .map(|task| quotient + usize::from(task < remainder))
        .collect();
    debug_assert!(limits.iter().all(|&limit| limit > 0));
    debug_assert_eq!(limits.iter().sum::<usize>(), work_budget);
    limits
}

/// Which physical layer consumed one bounded backend call. Source misses are
/// evidence about root discovery, not about the sparse graph traversal credit
/// retained by the activation; only transition misses widen that credit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhysicalDispatchKind {
    Source,
    Transition,
    Program,
}

/// One delta scheduler step as observed by the outer geometric policy.
///
/// Stable progress and dead pages are deliberately independent: one batched
/// expansion can retire an ineffective page for one activation while another
/// activation files a stable continuation.
#[derive(Debug)]
pub(super) struct DeltaStepOutcome {
    pub(super) continuation: Option<ContinuationToken>,
    pub(super) publication: Option<TerminalPublicationBatch>,
    pub(super) completed_activation_ids: Vec<ActivationId>,
    /// Exact Program continuation installed by quiescence settlement, keyed
    /// by the affine activation because one physical cohort may transfer more
    /// than one reducer. Queue layout is not a continuation receipt.
    pub(super) retargeted: AHashMap<ActivationId, ActiveDeltaContinuation>,
    pub(super) dead_pages: usize,
    pub(super) source_dead_pages: usize,
    pub(super) transition_dead_pages: usize,
    pub(super) completed_activations: usize,
    /// More than one activation from the scheduler's deliberately bounded
    /// transition cohort completed in this step. Source paging batches rows
    /// for storage efficiency, not as a latency/throughput activation choice.
    pub(super) completed_transition_cohort: bool,
    /// Whether a globally negative physical step is mature evidence for
    /// widening outer search `S`. Terminal traversal first exhausts its local
    /// geometric quantum; only a saturated still-live miss reaches this tier.
    pub(super) allows_global_width_growth: bool,
}

impl DeltaStepOutcome {
    fn has_stable_effect(&self) -> bool {
        self.continuation.is_some() || self.publication.is_some()
    }
}

/// Result of seeding an ordinary action into the cyclic scheduler.
///
/// Stable seed effects and deferred traversal are independent. An accepting
/// seed may file both at once, while an empty seed range may file neither.
#[derive(Debug)]
pub(super) struct DeltaSeedOutcome {
    pub(super) continuation: Option<ContinuationToken>,
    pub(super) publication: Option<TerminalPublicationBatch>,
    pub(super) active: Option<ActiveDeltaContinuation>,
    /// Every terminal-streaming activation created by this seed, in parent
    /// order, including activations that quiesced immediately.
    pub(super) terminal_activations: Vec<ActivationId>,
    /// Seed activations whose complete lineage quiesced before returning.
    pub(super) completed_activation_ids: Vec<ActivationId>,
    /// Canonical stable proposer family assigned by the outer machine.
    pub(super) terminal_family: Option<StateId>,
    /// Exact affine parents transferred after any physical admission split.
    pub(super) seeded_parents: usize,
}

/// Exact liveness classification after a directed cyclic step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActiveDeltaStatus {
    /// The activation filed stable work; the ordinary continuation takes over.
    Yielded,
    /// The activation still owns a scheduled source or transition credit.
    Pending,
    /// The activation reached quiescence and was removed from the registry.
    Quiescent,
}

#[derive(Debug)]
pub(super) struct ActiveDeltaStepOutcome {
    pub(super) outcome: DeltaStepOutcome,
    pub(super) status: ActiveDeltaStatus,
    /// Exact canonical state to install whenever the affine activation stays
    /// live. Most steps retain their input state; a graph-quiescence handoff
    /// carries the explicitly settled engine-Program state instead.
    pub(super) resume: Option<ActiveDeltaContinuation>,
}

impl IndexedDeltaBucket {
    fn last(&self) -> Option<&DeltaTask> {
        let (&slot, _) = self.activation_tails.last_key_value()?;
        self.tasks[slot].as_ref()
    }

    fn iter(&self) -> DeltaBucketIter<'_> {
        DeltaBucketIter::Indexed(self.tasks.iter())
    }

    fn extend(&mut self, tasks: impl IntoIterator<Item = DeltaTask>) {
        // Removing the old tail marks an activation as touched. We defer the
        // replacement until the whole append finishes, so one activation
        // costs one ordered-index update even when it contributes a run.
        let mut touched = Vec::new();
        for task in tasks {
            let activation = task.activation;
            let slot = self.tasks.len();
            let slots = self.activation_slots.entry(activation).or_default();
            if let Some(&old_tail) = slots.last() {
                if self.activation_tails.remove(&old_tail).is_some() {
                    touched.push(activation);
                }
            } else {
                touched.push(activation);
            }
            slots.push(slot);
            self.tasks.push(Some(task));
            self.live_tasks += 1;
        }
        for activation in touched {
            let tail = *self.activation_slots[&activation]
                .last()
                .expect("touched activation has one live task");
            assert_eq!(self.activation_tails.insert(tail, activation), None);
        }
    }

    /// Removes newest tasks from one activation and returns their arena slots
    /// in descending order. The caller may combine several activations and
    /// restore global append order by sorting on the slot.
    fn take_activation_slots(
        &mut self,
        activation: ActivationId,
        width: usize,
    ) -> Vec<(usize, DeltaTask)> {
        let width = width.max(1);
        let slots = self
            .activation_slots
            .get_mut(&activation)
            .expect("indexed activation has live tasks");
        let old_tail = *slots.last().expect("indexed activation is nonempty");
        assert_eq!(self.activation_tails.remove(&old_tail), Some(activation));

        let take = width.min(slots.len());
        let selected_slots = slots.split_off(slots.len() - take);
        let new_tail = slots.last().copied();
        if let Some(new_tail) = new_tail {
            assert_eq!(self.activation_tails.insert(new_tail, activation), None);
        } else {
            self.activation_slots.remove(&activation);
        }

        self.live_tasks -= selected_slots.len();
        selected_slots
            .into_iter()
            .rev()
            .map(|slot| {
                let task = self.tasks[slot]
                    .take()
                    .expect("activation index referenced a tombstone");
                debug_assert_eq!(task.activation, activation);
                (slot, task)
            })
            .collect()
    }

    fn drain_live(&mut self) -> Vec<DeltaTask> {
        let tasks = std::mem::take(&mut self.tasks)
            .into_iter()
            .flatten()
            .collect();
        self.activation_slots.clear();
        self.activation_tails.clear();
        self.live_tasks = 0;
        tasks
    }

    fn take_terminal_tail(
        &mut self,
        registry: &ProducerRegistry,
        width: usize,
        terminal_selection_slots: &mut AHashMap<ActivationId, usize>,
        terminal_selections: &mut Vec<TerminalActivationSelection>,
    ) -> (Vec<DeltaTask>, Vec<usize>) {
        let hot_activation = self
            .last()
            .expect("live delta bucket is nonempty")
            .activation;
        if registry.transition_dispatch_width(hot_activation, width) == width {
            // The hot activation alone exhausts the shared budget, so no
            // sibling can enter this physical cohort.
            let mut selected: Vec<_> = self
                .take_activation_slots(hot_activation, width)
                .into_iter()
                .map(|(_, task)| task)
                .collect();
            selected.reverse();
            let limits = even_limits(width, selected.len());
            return (selected, limits);
        }

        let mut remaining = width;
        terminal_selection_slots.clear();
        terminal_selections.clear();
        // An activation's latest slot is exactly where the old reverse scan
        // first encountered it. Selecting ordered activation tails therefore
        // preserves physical cohort and budget order without walking rows.
        for (_, &activation) in self.activation_tails.iter().rev() {
            if registry.physical_activation_class(activation)
                != DeltaPhysicalClass::TerminalStreaming
            {
                continue;
            }
            let budget = registry
                .transition_dispatch_width(activation, width)
                .min(remaining);
            debug_assert!(budget > 0);
            remaining -= budget;
            let slot = terminal_selections.len();
            terminal_selections.push(TerminalActivationSelection {
                activation,
                budget,
                selected: 0,
                ordinal: 0,
            });
            terminal_selection_slots.insert(activation, slot);
            if remaining == 0 {
                break;
            }
        }

        let mut selected_with_slots = Vec::with_capacity(width.min(self.live_tasks));
        for selection in terminal_selections.iter_mut() {
            let tasks = self.take_activation_slots(selection.activation, selection.budget);
            selection.selected = tasks.len();
            selected_with_slots.extend(tasks);
        }
        selected_with_slots.sort_unstable_by_key(|(slot, _)| *slot);

        let mut selected = Vec::with_capacity(selected_with_slots.len());
        let mut limits = Vec::with_capacity(selected_with_slots.len());
        for (_, task) in selected_with_slots {
            let selection = &mut terminal_selections[terminal_selection_slots[&task.activation]];
            let quotient = selection.budget / selection.selected;
            let remainder = selection.budget % selection.selected;
            limits.push(quotient + usize::from(selection.ordinal < remainder));
            selection.ordinal += 1;
            selected.push(task);
        }
        debug_assert!(limits.iter().all(|&limit| limit > 0));
        debug_assert_eq!(
            limits.iter().sum::<usize>(),
            terminal_selections
                .iter()
                .map(|selection| selection.budget)
                .sum::<usize>()
        );
        (selected, limits)
    }
}

impl DeltaBucket {
    fn len(&self) -> usize {
        match self {
            Self::Plain(tasks) => tasks.len(),
            Self::Indexed(bucket) => bucket.live_tasks,
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn last(&self) -> Option<&DeltaTask> {
        match self {
            Self::Plain(tasks) => tasks.last(),
            Self::Indexed(bucket) => bucket.last(),
        }
    }

    fn iter(&self) -> DeltaBucketIter<'_> {
        match self {
            Self::Plain(tasks) => DeltaBucketIter::Plain(tasks.iter()),
            Self::Indexed(bucket) => bucket.iter(),
        }
    }

    fn contains_activation(&self, activation: ActivationId) -> bool {
        match self {
            Self::Plain(tasks) => tasks.iter().any(|task| task.activation == activation),
            Self::Indexed(bucket) => bucket
                .activation_slots
                .get(&activation)
                .is_some_and(|slots| !slots.is_empty()),
        }
    }

    fn extend(&mut self, tasks: impl IntoIterator<Item = DeltaTask>) {
        match self {
            Self::Plain(current) => current.extend(tasks),
            Self::Indexed(bucket) => bucket.extend(tasks),
        }
    }

    fn promote(&mut self) {
        if matches!(self, Self::Indexed(_)) {
            return;
        }
        let Self::Plain(tasks) = std::mem::take(self) else {
            unreachable!()
        };
        let mut indexed = IndexedDeltaBucket::default();
        indexed.extend(tasks);
        *self = Self::Indexed(indexed);
    }

    fn drain_live(&mut self) -> Vec<DeltaTask> {
        match std::mem::take(self) {
            Self::Plain(tasks) => tasks,
            Self::Indexed(mut bucket) => bucket.drain_live(),
        }
    }

    fn compact_if_sparse(&mut self) {
        let compact = matches!(
            self,
            Self::Indexed(bucket)
                if bucket.tasks.len() > bucket.live_tasks.saturating_mul(2)
        );
        if compact {
            let tasks = self.drain_live();
            *self = Self::Plain(tasks);
        }
    }

    #[cfg(test)]
    fn arena_len(&self) -> usize {
        match self {
            Self::Plain(tasks) => tasks.len(),
            Self::Indexed(bucket) => bucket.tasks.len(),
        }
    }

    #[cfg(test)]
    fn assert_index_consistent(&self) {
        match self {
            Self::Plain(tasks) => assert_eq!(self.iter().count(), tasks.len()),
            Self::Indexed(bucket) => {
                assert_eq!(self.iter().count(), bucket.live_tasks);
                assert!(bucket.tasks.len() <= bucket.live_tasks.saturating_mul(2));
                let indexed_tasks: usize = bucket.activation_slots.values().map(Vec::len).sum();
                assert_eq!(indexed_tasks, bucket.live_tasks);
                assert_eq!(bucket.activation_tails.len(), bucket.activation_slots.len());
                for (&activation, slots) in &bucket.activation_slots {
                    assert!(!slots.is_empty());
                    for &slot in slots {
                        assert_eq!(bucket.tasks[slot].as_ref().unwrap().activation, activation);
                    }
                    assert_eq!(
                        bucket.activation_tails.get(slots.last().unwrap()),
                        Some(&activation)
                    );
                }
            }
        }
    }

    /// Removes at most `width` tasks owned by one exact activation while
    /// preserving the relative order of both the selected and retained
    /// subsequences.
    fn take_activation(&mut self, activation: ActivationId, width: usize) -> Vec<DeltaTask> {
        let width = width.max(1);
        let selected = match self {
            Self::Plain(tasks) => {
                let mut selected = Vec::with_capacity(width.min(tasks.len()));
                let mut retained = Vec::with_capacity(tasks.len());
                for task in std::mem::take(tasks).into_iter().rev() {
                    if task.activation == activation && selected.len() < width {
                        selected.push(task);
                    } else {
                        retained.push(task);
                    }
                }
                selected.reverse();
                retained.reverse();
                *tasks = retained;
                selected
            }
            Self::Indexed(bucket) => {
                let mut selected: Vec<_> = bucket
                    .take_activation_slots(activation, width)
                    .into_iter()
                    .map(|(_, task)| task)
                    .collect();
                selected.reverse();
                selected
            }
        };
        self.compact_if_sparse();
        selected
    }

    #[cfg(test)]
    fn take_activation_indexed(
        &mut self,
        activation: ActivationId,
        width: usize,
    ) -> Vec<DeltaTask> {
        self.promote();
        self.take_activation(activation, width)
    }

    fn take_tail(
        &mut self,
        registry: &ProducerRegistry,
        width: usize,
        activation_width: usize,
        terminal_selection_slots: &mut AHashMap<ActivationId, usize>,
        terminal_selections: &mut Vec<TerminalActivationSelection>,
    ) -> (Vec<DeltaTask>, Vec<usize>) {
        let width = width.max(1);
        let activation_width = activation_width.max(1);
        let key = TransitionDispatchKey::of(
            registry,
            self.last().expect("live delta bucket is nonempty"),
        );
        if key == TransitionDispatchKey::TerminalStreaming {
            // Admit terminal activations from the hot tail until their local
            // sparse quanta fill the one shared global budget. Promotion is
            // terminal-only, leaving ordinary and formula buckets on Vec.
            self.promote();
            let Self::Indexed(bucket) = self else {
                unreachable!()
            };
            let result = bucket.take_terminal_tail(
                registry,
                width,
                terminal_selection_slots,
                terminal_selections,
            );
            self.compact_if_sparse();
            return result;
        }

        let mut activations = BTreeSet::new();
        let tasks = self.drain_live();
        let mut selected = Vec::with_capacity(width.min(tasks.len()));
        let mut retained = Vec::with_capacity(tasks.len());
        for task in tasks.into_iter().rev() {
            let compatible = match (key, TransitionDispatchKey::of(registry, &task)) {
                (TransitionDispatchKey::Streaming, TransitionDispatchKey::Streaming) => true,
                (
                    TransitionDispatchKey::Quiescent(_),
                    TransitionDispatchKey::Quiescent(activation),
                ) => {
                    activations.contains(&activation)
                        || (activations.len() < activation_width && {
                            activations.insert(activation);
                            true
                        })
                }
                _ => false,
            };
            if selected.len() < width && compatible {
                selected.push(task);
            } else {
                retained.push(task);
            }
        }
        selected.reverse();
        retained.reverse();
        self.extend(retained);
        let limits = even_limits(width, selected.len());
        (selected, limits)
    }
}

impl SourceBucket {
    /// Source credits are activation-affine just like transition credits. A
    /// directed latency step must not absorb compatible generators from cold
    /// sibling activations merely because they share one canonical state. The
    /// selected tasks retain the global source pop's LIFO dispatch order;
    /// retained cold tasks keep their storage order.
    fn take_activation(&mut self, activation: ActivationId, width: usize) -> Vec<SourceTask> {
        let width = width.max(1);
        let mut selected = Vec::with_capacity(width.min(self.tasks.len()));
        let mut retained = Vec::with_capacity(self.tasks.len());
        for task in std::mem::take(&mut self.tasks).into_iter().rev() {
            if task.activation == activation && selected.len() < width {
                selected.push(task);
            } else {
                retained.push(task);
            }
        }
        retained.reverse();
        self.tasks = retained;
        selected
    }
}

/// Reopenable cyclic work kept outside the strict-rank stable worklist.
pub(super) struct DeltaScheduler {
    registry: ProducerRegistry,
    interner: DeltaInterner,
    worklist: BTreeMap<DeltaStateId, DeltaBucket>,
    source_worklist: BTreeMap<DeltaStateId, SourceBucket>,
    /// One unified queue of opaque typed continuations. Source generation and
    /// product expansion are family-private states distinguished only by
    /// opaque physical dispatch classes.
    program_worklist: BTreeMap<DeltaStateId, ProgramBucket>,
    program_runtimes: AHashMap<DeltaStateId, ProgramRuntime>,
    /// Number of independent quiescent activations that may share one
    /// transition cohort. This grows only when activations complete; `width`
    /// remains the separate intra-activation page/work budget.
    activation_width: usize,
    /// Query-local scratch for the exact terminal cohort partition. Keeping
    /// it beside the scheduler amortizes hash-table and record allocation
    /// without making scratch state part of canonical delta identity.
    terminal_selection_slots: AHashMap<ActivationId, usize>,
    terminal_selections: Vec<TerminalActivationSelection>,
}

impl DeltaScheduler {
    pub(super) fn new() -> Self {
        Self {
            registry: ProducerRegistry::new(),
            interner: DeltaInterner::default(),
            worklist: BTreeMap::new(),
            source_worklist: BTreeMap::new(),
            program_worklist: BTreeMap::new(),
            program_runtimes: AHashMap::new(),
            activation_width: 1,
            terminal_selection_slots: AHashMap::new(),
            terminal_selections: Vec::new(),
        }
    }

    /// Mints exact per-parent terminal receipts without filing sparse source
    /// or transition work. The caller returns each identity as both an
    /// admission registration and an immediate completion receipt.
    pub(super) fn reserve_terminal_receipts(&mut self, count: usize) -> Vec<ActivationId> {
        self.registry.reserve_terminal_receipts(count)
    }

    pub(super) fn receipt_has_live_activation(&self, receipt: ActivationId) -> bool {
        self.registry.state.activations.contains_key(&receipt)
    }

    pub(super) fn grow_activation_width(&mut self, growth: usize, cap: usize) -> bool {
        let next = self
            .activation_width
            .saturating_mul(growth.max(1))
            .clamp(1, cap.max(1));
        let grew = next > self.activation_width;
        self.activation_width = next;
        grew
    }

    #[cfg(test)]
    pub(super) fn activation_width(&self) -> usize {
        self.activation_width
    }

    pub(super) fn is_empty(&self) -> bool {
        self.worklist.is_empty()
            && self.source_worklist.is_empty()
            && self.program_worklist.is_empty()
    }

    fn prepare_program(
        &mut self,
        desc: DeltaDesc,
        route: ProgramRoute,
        spec: ProgramRef<'_>,
    ) -> DeltaStateId {
        let state = self
            .interner
            .intern_program(ProgramAddress::new(desc, route));
        self.program_runtimes
            .entry(state)
            .or_insert_with(|| spec.new_runtime_for(route.key));
        state
    }

    fn prepare_engine_program(&mut self, kind: EngineProgramKind) -> DeltaStateId {
        let address = ProgramAddress::Engine(kind);
        let state = self.interner.intern_program(address);
        self.program_runtimes
            .entry(state)
            .or_insert_with(|| kind.resolve().new_runtime());
        state
    }

    /// Central scheduler half of graph quiescence settlement.
    ///
    /// Registry settlement owns the reducer's semantic capability gate and
    /// same-activation Q->Open transition. This half installs the one private
    /// typed handle and files its one affine Program task, returning the exact
    /// canonical state as an explicit continuation receipt.
    fn settle_quiescence(&mut self, proof: QuiescenceProof) -> DeltaSettlement {
        match self.registry.settle_quiescence(proof) {
            RegistrySettlement::Completed(completed) => DeltaSettlement::Completed(completed),
            RegistrySettlement::ConfirmFinalizer(seed) => {
                let state = self.prepare_engine_program(EngineProgramKind::ConfirmFinalize);
                let work = insert_engine_program_state(
                    &CONFIRM_FINALIZER_PROGRAM,
                    self.program_runtimes
                        .get_mut(&state)
                        .expect("prepared Confirm finalizer lost its runtime"),
                    ProgramActivation(seed.activation.0),
                    seed.state,
                );
                let active = self
                    .file_program_state(
                        state,
                        vec![ProgramTask {
                            activation: seed.activation,
                            credit: seed.credit,
                            work,
                        }],
                    )
                    .expect("Confirm finalizer filed one affine task");
                DeltaSettlement::Retargeted(active)
            }
            RegistrySettlement::ProposalMaterializer(seed) => {
                let state = self.prepare_engine_program(EngineProgramKind::ProposalMaterialize);
                let work = insert_engine_program_state(
                    &PROPOSAL_MATERIALIZER_PROGRAM,
                    self.program_runtimes
                        .get_mut(&state)
                        .expect("prepared proposal materializer lost its runtime"),
                    ProgramActivation(seed.activation.0),
                    seed.state,
                );
                let active = self
                    .file_program_state(
                        state,
                        vec![ProgramTask {
                            activation: seed.activation,
                            credit: seed.credit,
                            work,
                        }],
                    )
                    .expect("proposal materializer filed one affine task");
                DeltaSettlement::Retargeted(active)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_program_proposals_with_full(
        &mut self,
        spec: ProgramRef<'_>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        successor: StateDesc,
        parents: RowBatch,
        full: VariableSet,
        direct_terminal_publication_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let seeded_parents = parents.row_count;
        let state = self.prepare_program(desc, route, spec);
        let stride = successor.bound.count();
        let mut activations = Vec::with_capacity(parents.row_count);
        let mut terminal_activations = Vec::with_capacity(parents.row_count);
        for row in 0..parents.row_count {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let activation = self.registry.open_program_activation(
                DeltaReducer::StreamProposal,
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result: false,
                },
                None,
                Some(full),
            );
            if self.registry.physical_activation_class(activation)
                == DeltaPhysicalClass::TerminalStreaming
            {
                terminal_activations.push(activation);
            }
            activations.push(activation);
        }
        let program_activations: Vec<_> = activations
            .iter()
            .map(|activation| ProgramActivation(activation.0))
            .collect();
        let vars: Vec<_> = successor.bound.into_iter().collect();
        let view = rows_view(&vars, &parents.rows, parents.row_count);
        let mut seeded = ProgramSeedEffects::default();
        spec.seed_batch(
            self.program_runtimes
                .get_mut(&state)
                .expect("prepared program lost its runtime"),
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &program_activations,
            },
            &mut seeded,
        );
        let ranges = program_seed_ranges(&seeded.work, parents.row_count);
        let mut tasks = Vec::with_capacity(seeded.work.len());
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        for (activation, range) in activations.iter().copied().zip(ranges) {
            let installed = self
                .registry
                .install_program_roots(activation, seeded.work[range].iter().cloned());
            if !installed.initial_accepted.is_empty() {
                let direct_terminal = direct_terminal_publication_full.filter(|_| {
                    self.registry.physical_activation_class(activation)
                        == DeltaPhysicalClass::TerminalStreaming
                });
                let streamed = self
                    .registry
                    .take_streaming_return(activation)
                    .expect("typed streaming proposal rejected accepting seed effects");
                let released = self.release_streaming(
                    activation,
                    streamed,
                    installed.initial_accepted,
                    direct_terminal,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
                assert!(
                    released.active.is_none(),
                    "ordinary typed proposal seed opened a Formula reducer"
                );
                effects.absorb(released.stable);
            }
            tasks.extend(
                installed
                    .roots
                    .into_iter()
                    .map(|(work, credit)| ProgramTask {
                        activation,
                        credit,
                        work,
                    }),
            );
            if let Some(proof) = installed.quiescence {
                let completed = self.registry.finish(proof);
                assert_eq!(completed.effect, DeltaCompletion::Cleanup);
                completed_activation_ids.push(completed.activation);
            }
        }
        if !completed_activation_ids.is_empty() {
            let retired: Vec<_> = completed_activation_ids
                .iter()
                .map(|activation| ProgramActivation(activation.0))
                .collect();
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("prepared program lost its runtime"),
                route.key,
                &retired,
            );
        }
        let active = self.file_program_state(state, tasks);
        DeltaSeedOutcome {
            continuation: effects.continuation,
            publication: effects.publication,
            active,
            terminal_activations,
            completed_activation_ids,
            terminal_family: None,
            seeded_parents,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_program_confirms(
        &mut self,
        spec: ProgramRef<'_>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        successor: StateDesc,
        set_admit_result: bool,
        batch: CandidateBatch,
    ) -> Option<ActiveDeltaContinuation> {
        let state = self.prepare_program(desc, route, spec);
        let stride = successor.bound.count();
        let parent_count = batch.parents.row_count;
        let (parents, candidate_groups) = batch.into_parent_candidates();
        let mut activations = Vec::with_capacity(parent_count);
        for (row, original) in candidate_groups.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let original = shared_one_parent_candidates(original);
            activations.push(self.registry.open_program_activation(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result,
                },
                None,
                None,
            ));
        }
        let program_activations: Vec<_> = activations
            .iter()
            .map(|activation| ProgramActivation(activation.0))
            .collect();
        let vars: Vec<_> = successor.bound.into_iter().collect();
        let view = rows_view(&vars, &parents.rows, parent_count);
        let mut seeded = ProgramSeedEffects::default();
        spec.seed_batch(
            self.program_runtimes
                .get_mut(&state)
                .expect("prepared program lost its runtime"),
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &program_activations,
            },
            &mut seeded,
        );
        let ranges = program_seed_ranges(&seeded.work, parent_count);
        let mut tasks = Vec::with_capacity(seeded.work.len());
        let mut retired = Vec::new();
        let mut finalizer_active = None;
        for (activation, range) in activations.into_iter().zip(ranges) {
            let installed = self
                .registry
                .install_program_roots(activation, seeded.work[range].iter().cloned());
            tasks.extend(
                installed
                    .roots
                    .into_iter()
                    .map(|(work, credit)| ProgramTask {
                        activation,
                        credit,
                        work,
                    }),
            );
            if let Some(proof) = installed.quiescence {
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Completed(completed) => assert!(matches!(
                        completed.effect,
                        DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
                    )),
                    DeltaSettlement::Retargeted(active) => finalizer_active = Some(active),
                }
                retired.push(ProgramActivation(activation.0));
            }
        }
        if !retired.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("prepared program lost its runtime"),
                route.key,
                &retired,
            );
        }
        let graph_active = self.file_program_state(state, tasks);
        finalizer_active.or(graph_active)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_program_formula(
        &mut self,
        spec: ProgramRef<'_>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        bound: VariableSet,
        counter: FormulaPcId,
        stage: FormulaStage,
        batch: FormulaBatch,
        stream_proposal: bool,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let seeded_parents = batch.parents.row_count;
        let parent_rows = batch.parents.rows.clone();
        let singletons = batch.into_singletons(bound.count());
        let state = self.prepare_program(desc, route, spec);
        let mut activations = Vec::with_capacity(singletons.len());
        for mut batch in singletons {
            let reducer = match stage {
                FormulaStage::Support => DeltaReducer::Support { published: false },
                FormulaStage::Propose if stream_proposal => DeltaReducer::StreamFormulaProposal,
                FormulaStage::Propose => DeltaReducer::quiescent_proposal(),
                FormulaStage::Confirm => DeltaReducer::Confirm {
                    original: batch.shared_contiguous_confirm_original(),
                },
            };
            activations.push(self.registry.open_program_activation(
                reducer,
                DeltaReturn::Formula {
                    bound,
                    counter,
                    batch,
                },
                None,
                None,
            ));
        }
        let program_activations: Vec<_> = activations
            .iter()
            .map(|activation| ProgramActivation(activation.0))
            .collect();
        let vars: Vec<_> = bound.into_iter().collect();
        let view = rows_view(&vars, &parent_rows, seeded_parents);
        let mut seeded = ProgramSeedEffects::default();
        spec.seed_batch(
            self.program_runtimes
                .get_mut(&state)
                .expect("prepared program lost its runtime"),
            ProgramSeedBatch {
                request,
                route,
                view,
                activations: &program_activations,
            },
            &mut seeded,
        );
        let ranges = program_seed_ranges(&seeded.work, seeded_parents);
        let mut tasks = Vec::with_capacity(seeded.work.len());
        let mut completed = Vec::new();
        let mut retired = Vec::new();
        let mut continuation = None;
        let mut finalizer_active = None;
        for (activation, range) in activations.into_iter().zip(ranges) {
            let installed = self
                .registry
                .install_program_roots(activation, seeded.work[range].iter().cloned());
            if !installed.initial_accepted.is_empty() {
                if let Some(streamed) = self.registry.take_streaming_return(activation) {
                    let released = self.release_streaming(
                        activation,
                        streamed,
                        installed.initial_accepted,
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    debug_assert!(released.stable.publication.is_none());
                    prefer_continuation(
                        &mut continuation,
                        released.stable.continuation,
                    );
                    if released.active.is_some() {
                        finalizer_active = released.active;
                    }
                }
            }
            tasks.extend(
                installed
                    .roots
                    .into_iter()
                    .map(|(work, credit)| ProgramTask {
                        activation,
                        credit,
                        work,
                    }),
            );
            if let Some(proof) = installed.quiescence {
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Completed(completed_activation) => {
                        completed.push(completed_activation)
                    }
                    DeltaSettlement::Retargeted(active) => finalizer_active = Some(active),
                }
                retired.push(ProgramActivation(activation.0));
            }
        }
        let graph_active = self.file_program_state(state, tasks);
        let mut active = finalizer_active.or(graph_active);
        for completed in completed {
            let released = self.release_completion(
                completed,
                plan,
                stable,
                stable_interner,
                stats,
            );
            prefer_continuation(
                &mut continuation,
                released.continuation,
            );
            if released.active.is_some() {
                active = released.active;
            }
        }
        if !retired.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("prepared program lost its runtime"),
                route.key,
                &retired,
            );
        }
        DeltaSeedOutcome {
            continuation,
            publication: None,
            active,
            terminal_activations: Vec::new(),
            completed_activation_ids: Vec::new(),
            terminal_family: None,
            seeded_parents,
        }
    }

    #[cfg(test)]
    pub(super) fn seed_proposals(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
        seeds: Vec<ResidualDeltaSeed>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        self.seed_proposals_with_full(
            desc,
            successor,
            parents,
            seeds,
            VariableSet::new_empty(),
            None,
            plan,
            stable,
            stable_interner,
            stats,
        )
    }

    pub(super) fn seed_proposals_with_full(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
        seeds: Vec<ResidualDeltaSeed>,
        full: VariableSet,
        direct_terminal_publication_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let seeded_parents = parents.row_count;
        let ranges = seed_ranges(&seeds, parents.row_count);
        let stride = successor.bound.count();
        let mut tasks = Vec::with_capacity(seeds.len());
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut terminal_activations = Vec::with_capacity(parents.row_count);
        for (row, range) in ranges.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let started = self.registry.start_many_terminal(
                DeltaReducer::StreamProposal,
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result: false,
                },
                seeds[range].iter().map(|seed| seed.output),
                full,
            );
            if self.registry.physical_activation_class(started.activation)
                == DeltaPhysicalClass::TerminalStreaming
            {
                terminal_activations.push(started.activation);
            }
            if !started.initial_accepted.is_empty() {
                let direct_terminal = direct_terminal_publication_full.filter(|_| {
                    self.registry.physical_activation_class(started.activation)
                        == DeltaPhysicalClass::TerminalStreaming
                });
                let streamed = self
                    .registry
                    .take_streaming_return(started.activation)
                    .expect("a streaming proposal rejected its accepting seed receipt");
                let released = self.release_streaming(
                    started.activation,
                    streamed,
                    started.initial_accepted,
                    direct_terminal,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
                assert!(
                    released.active.is_none(),
                    "ordinary proposal seed opened a Formula reducer"
                );
                effects.absorb(released.stable);
            }
            tasks.extend(started.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }));
            if let Some(proof) = started.quiescence {
                let completed = self.registry.finish(proof);
                assert_eq!(completed.effect, DeltaCompletion::Cleanup);
                assert!(matches!(completed.return_to, DeltaReturn::Stable { .. }));
                completed_activation_ids.push(completed.activation);
            }
        }
        let active = self.file(desc, tasks);
        DeltaSeedOutcome {
            continuation: effects.continuation,
            publication: effects.publication,
            active,
            terminal_activations,
            completed_activation_ids,
            terminal_family: None,
            seeded_parents,
        }
    }

    #[cfg(test)]
    pub(super) fn seed_source_proposals(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
    ) -> Option<ActiveDeltaContinuation> {
        self.seed_source_proposals_with_full_receipt(
            desc,
            successor,
            parents,
            VariableSet::new_empty(),
        )
        .active
    }

    pub(super) fn seed_source_proposals_with_full_receipt(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
        full: VariableSet,
    ) -> DeltaSeedOutcome {
        let seeded_parents = parents.row_count;
        let stride = successor.bound.count();
        let mut tasks = Vec::with_capacity(parents.row_count);
        let mut terminal_activations = Vec::with_capacity(parents.row_count);
        for row in 0..parents.row_count {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let (activation, credit) = self.registry.start_source_terminal(
                DeltaReducer::StreamProposal,
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result: false,
                },
                None,
                full,
            );
            if self.registry.physical_activation_class(activation)
                == DeltaPhysicalClass::TerminalStreaming
            {
                terminal_activations.push(activation);
            }
            tasks.push(SourceTask {
                activation,
                credit,
                cursor: ResidualDeltaSourceCursor::Start,
            });
        }
        DeltaSeedOutcome {
            continuation: None,
            publication: None,
            active: self.file_source(desc, tasks),
            terminal_activations,
            completed_activation_ids: Vec::new(),
            terminal_family: None,
            seeded_parents,
        }
    }

    pub(super) fn seed_confirms(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        set_admit_result: bool,
        batch: CandidateBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) -> Option<ActiveDeltaContinuation> {
        let seed_ranges = seed_ranges(&seeds, batch.parents.row_count);
        let stride = successor.bound.count();
        let (parents, candidate_groups) = batch.into_parent_candidates();

        let mut tasks = Vec::with_capacity(seeds.len());
        let mut finalizer_active = None;
        for ((row, seed_range), original) in
            seed_ranges.into_iter().enumerate().zip(candidate_groups)
        {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let original = shared_one_parent_candidates(original);
            let started = self.registry.start_many(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result,
                },
                seeds[seed_range].iter().map(|seed| seed.output),
            );
            tasks.extend(started.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }));
            if let Some(proof) = started.quiescence {
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Completed(completed) => {
                        assert!(matches!(completed.return_to, DeltaReturn::Stable { .. }));
                        assert!(matches!(
                            completed.effect,
                            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
                        ));
                    }
                    DeltaSettlement::Retargeted(active) => finalizer_active = Some(active),
                }
            }
        }
        let graph_active = self.file(desc, tasks);
        finalizer_active.or(graph_active)
    }

    pub(super) fn seed_source_confirms(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        set_admit_result: bool,
        batch: CandidateBatch,
    ) -> Option<ActiveDeltaContinuation> {
        let stride = successor.bound.count();
        let parent_count = batch.parents.row_count;
        let (parents, candidate_groups) = batch.into_parent_candidates();

        let mut tasks = Vec::with_capacity(parent_count);
        for (row, original) in candidate_groups.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let mut source_candidates = original.clone();
            source_candidates.sort_unstable();
            source_candidates.dedup();
            let original = shared_one_parent_candidates(original);
            let (activation, credit) = self.registry.start_source(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result,
                },
                Some(source_candidates.into_boxed_slice()),
            );
            tasks.push(SourceTask {
                activation,
                credit,
                cursor: ResidualDeltaSourceCursor::Start,
            });
        }
        self.file_source(desc, tasks)
    }

    /// Suspends each affine formula parent behind one activation-local reducer.
    /// Empty seed ranges complete immediately with an empty action result, so
    /// an empty RPQ arm can still return through AND/OR frames.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_formula(
        &mut self,
        desc: DeltaDesc,
        bound: VariableSet,
        counter: FormulaPcId,
        stage: FormulaStage,
        batch: FormulaBatch,
        seeds: Vec<ResidualDeltaSeed>,
        stream_proposal: bool,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let seeded_parents = batch.parents.row_count;
        let ranges = seed_ranges(&seeds, batch.parents.row_count);
        let singletons = batch.into_singletons(bound.count());
        assert_eq!(singletons.len(), ranges.len());

        let mut tasks = Vec::with_capacity(seeds.len());
        let mut completed = Vec::new();
        let mut continuation = None;
        let mut finalizer_active = None;
        for (mut batch, range) in singletons.into_iter().zip(ranges) {
            let reducer = match stage {
                FormulaStage::Support => DeltaReducer::Support { published: false },
                FormulaStage::Propose if stream_proposal => DeltaReducer::StreamFormulaProposal,
                FormulaStage::Propose => DeltaReducer::quiescent_proposal(),
                FormulaStage::Confirm => DeltaReducer::Confirm {
                    original: batch.shared_confirm_original(),
                },
            };
            let started = self.registry.start_many(
                reducer,
                DeltaReturn::Formula {
                    bound,
                    counter,
                    batch,
                },
                seeds[range].iter().map(|seed| seed.output),
            );
            if !started.initial_accepted.is_empty() {
                if let Some(streamed) = self.registry.take_streaming_return(started.activation) {
                    let released = self.release_streaming(
                        started.activation,
                        streamed,
                        started.initial_accepted,
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    debug_assert!(released.stable.publication.is_none());
                    prefer_continuation(
                        &mut continuation,
                        released.stable.continuation,
                    );
                    if released.active.is_some() {
                        finalizer_active = released.active;
                    }
                }
            }
            tasks.extend(started.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }));
            if let Some(proof) = started.quiescence {
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Completed(completed_activation) => {
                        completed.push(completed_activation)
                    }
                    DeltaSettlement::Retargeted(active) => finalizer_active = Some(active),
                }
            }
        }
        let graph_active = self.file(desc, tasks);
        let mut active = finalizer_active.or(graph_active);

        for completed in completed {
            let released = self.release_completion(
                completed,
                plan,
                stable,
                stable_interner,
                stats,
            );
            prefer_continuation(
                &mut continuation,
                released.continuation,
            );
            if released.active.is_some() {
                active = released.active;
            }
        }
        DeltaSeedOutcome {
            continuation,
            publication: None,
            active,
            terminal_activations: Vec::new(),
            completed_activation_ids: Vec::new(),
            terminal_family: None,
            seeded_parents,
        }
    }

    /// Suspends one bounded source generator per affine formula parent. The
    /// exact Action PC and reducer frames remain activation payload; the
    /// structural descriptor names only the shared expansion kernel.
    pub(super) fn seed_source_formula(
        &mut self,
        desc: DeltaDesc,
        bound: VariableSet,
        counter: FormulaPcId,
        stage: FormulaStage,
        batch: FormulaBatch,
        stream_proposal: bool,
    ) -> Option<ActiveDeltaContinuation> {
        let singletons = batch.into_singletons(bound.count());
        let mut tasks = Vec::with_capacity(singletons.len());
        for mut batch in singletons {
            let (reducer, source_candidates) = match stage {
                FormulaStage::Support => {
                    unreachable!("support has no delta source reducer")
                }
                FormulaStage::Propose if stream_proposal => {
                    (DeltaReducer::StreamFormulaProposal, None)
                }
                FormulaStage::Propose => (DeltaReducer::quiescent_proposal(), None),
                FormulaStage::Confirm => {
                    let original = batch.shared_contiguous_confirm_original();
                    let mut source_candidates = original.one_parent_values().to_vec();
                    source_candidates.sort_unstable();
                    source_candidates.dedup();
                    (
                        DeltaReducer::Confirm { original },
                        Some(source_candidates.into_boxed_slice()),
                    )
                }
            };
            let (activation, credit) = self.registry.start_source(
                reducer,
                DeltaReturn::Formula {
                    bound,
                    counter,
                    batch,
                },
                source_candidates,
            );
            tasks.push(SourceTask {
                activation,
                credit,
                cursor: ResidualDeltaSourceCursor::Start,
            });
        }
        self.file_source(desc, tasks)
    }

    /// Drains engine-owned Formula reducer seeds without manufacturing a
    /// graph descriptor. Multi-parent payloads are first split by persistent
    /// parent-domain cuts; each nonempty singleton then owns exactly one
    /// affine Program credit. Zero-rank reducers advance synchronously and
    /// put any recursively generated reducer at the front of this same queue,
    /// preserving the hot depth-first lineage without a sentinel task.
    #[allow(clippy::too_many_arguments)]
    fn drain_formula_reducer_seeds(
        &mut self,
        seeds: Vec<FormulaReducerSeed>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> FormulaReducerDrain {
        let mut queue: VecDeque<_> = seeds.into();
        let mut drained = FormulaReducerDrain::default();
        while let Some(seed) = queue.pop_front() {
            match seed {
                FormulaReducerSeed::SetAdmit(seed)
                    if seed.destination.parent_count() > 1 =>
                {
                    let singletons = seed
                        .destination
                        .into_structural_singletons(seed.successor.bound.count());
                    for destination in singletons.into_iter().rev() {
                        queue.push_front(FormulaReducerSeed::SetAdmit(SetAdmissionSeed {
                            successor: seed.successor.clone(),
                            destination,
                        }));
                    }
                }
                FormulaReducerSeed::SetAdmit(mut seed) => {
                    assert_eq!(
                        seed.destination.parent_count(),
                        1,
                        "SET admission requires one affine parent"
                    );
                    let input = seed.destination.take_candidates();
                    input.debug_assert_valid_for(1);
                    let Some(program_state) = SetAdmissionState::start(input) else {
                        if let Some(bucket) = seed
                            .destination
                            .into_live_bucket(seed.successor.bound.count())
                        {
                            prefer_continuation(
                                &mut drained.continuation,
                                file_with_plan(
                                    stable,
                                    stable_interner,
                                    plan,
                                    seed.successor,
                                    bucket,
                                    stats,
                                ),
                            );
                        }
                        continue;
                    };

                    let mut output = CandidatePayload::empty(1);
                    output.defer_for_shared_activation(1);
                    let state = self.prepare_engine_program(EngineProgramKind::SetAdmit);
                    let activation = self.registry.open_program_activation(
                        DeltaReducer::SetAdmit { output },
                        DeltaReturn::SetAdmission {
                            successor: seed.successor,
                            destination: seed.destination,
                        },
                        None,
                        None,
                    );
                    let credit = self
                        .registry
                        .issue_credit(activation, CreditKind::Program { join: None });
                    let work = insert_engine_program_state(
                        &SET_ADMISSION_PROGRAM,
                        self.program_runtimes
                            .get_mut(&state)
                            .expect("prepared SET admission lost its runtime"),
                        ProgramActivation(activation.0),
                        program_state,
                    );
                    drained.active = self.file_program_state(
                        state,
                        vec![ProgramTask {
                            activation,
                            credit,
                            work,
                        }],
                    );
                }
                FormulaReducerSeed::Admit(seed) if seed.batch.parents.row_count > 1 => {
                    let singletons = seed.batch.into_structural_singletons_with_input(
                        seed.bound.count(),
                        seed.input,
                    );
                    for (batch, input) in singletons.into_iter().rev() {
                        queue.push_front(FormulaReducerSeed::Admit(FormulaOrAdmissionSeed {
                            bound: seed.bound,
                            batch,
                            input,
                            continuation: seed.continuation,
                        }));
                    }
                }
                FormulaReducerSeed::Admit(mut seed) => {
                    assert_eq!(
                        seed.batch.parents.row_count, 1,
                        "Formula OR admission requires one affine parent"
                    );
                    seed.input.debug_assert_valid_for(1);
                    if seed.input.is_empty() {
                        let mut generated = Vec::new();
                        prefer_continuation(
                            &mut drained.continuation,
                            finish_formula_or_admission(
                                plan,
                                seed.bound,
                                seed.batch,
                                seed.continuation,
                                stable,
                                stable_interner,
                                stats,
                                &mut generated,
                            ),
                        );
                        for seed in generated.into_iter().rev() {
                            queue.push_front(seed);
                        }
                        continue;
                    }

                    seed.input.defer_for_shared_activation(1);
                    let input = seed.input.shared_one_parent_cursor();
                    let state =
                        self.prepare_engine_program(EngineProgramKind::FormulaOrAdmit);
                    let activation = self.registry.open_program_activation(
                        DeltaReducer::FormulaOrAdmit,
                        DeltaReturn::FormulaOrAdmit {
                            bound: seed.bound,
                            batch: seed.batch,
                            continuation: seed.continuation,
                        },
                        None,
                        None,
                    );
                    let credit = self
                        .registry
                        .issue_credit(activation, CreditKind::Program { join: None });
                    let work = insert_engine_program_state(
                        &FORMULA_OR_ADMISSION_PROGRAM,
                        self.program_runtimes
                            .get_mut(&state)
                            .expect("prepared Formula OR admission lost its runtime"),
                        ProgramActivation(activation.0),
                        FormulaOrAdmissionState { input },
                    );
                    drained.active = self.file_program_state(
                        state,
                        vec![ProgramTask {
                            activation,
                            credit,
                            work,
                        }],
                    );
                }
                FormulaReducerSeed::Emit(seed) if seed.batch.parents.row_count > 1 => {
                    let singletons = seed
                        .batch
                        .into_structural_singletons(seed.bound.count());
                    for batch in singletons.into_iter().rev() {
                        queue.push_front(FormulaReducerSeed::Emit(FormulaOrEmissionSeed {
                            bound: seed.bound,
                            batch,
                            counter: seed.counter,
                        }));
                    }
                }
                FormulaReducerSeed::Emit(seed) => {
                    assert_eq!(
                        seed.batch.parents.row_count, 1,
                        "Formula OR emission requires one affine parent"
                    );
                    let set = seed.batch.current_or_set();
                    if set.is_empty() {
                        let mut result = CandidatePayload::empty(1);
                        result.defer_for_shared_activation(1);
                        let mut generated = Vec::new();
                        prefer_continuation(
                            &mut drained.continuation,
                            finish_formula_or_emission(
                                plan,
                                seed.bound,
                                seed.counter,
                                seed.batch,
                                result,
                                stable,
                                stable_interner,
                                stats,
                                &mut generated,
                            ),
                        );
                        for seed in generated.into_iter().rev() {
                            queue.push_front(seed);
                        }
                        continue;
                    }

                    let mut output = CandidatePayload::empty(1);
                    output.defer_for_shared_activation(1);
                    let state =
                        self.prepare_engine_program(EngineProgramKind::FormulaOrEmit);
                    let activation = self.registry.open_program_activation(
                        DeltaReducer::FormulaOrEmit { output },
                        DeltaReturn::FormulaOrEmit {
                            bound: seed.bound,
                            batch: seed.batch,
                            counter: seed.counter,
                        },
                        None,
                        None,
                    );
                    let credit = self
                        .registry
                        .issue_credit(activation, CreditKind::Program { join: None });
                    let work = insert_engine_program_state(
                        &FORMULA_OR_EMISSION_PROGRAM,
                        self.program_runtimes
                            .get_mut(&state)
                            .expect("prepared Formula OR emission lost its runtime"),
                        ProgramActivation(activation.0),
                        FormulaOrEmissionState {
                            set,
                            emitted_count: 0,
                            last_emitted: None,
                        },
                    );
                    drained.active = self.file_program_state(
                        state,
                        vec![ProgramTask {
                            activation,
                            credit,
                            work,
                        }],
                    );
                }
            }
        }
        drained
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_formula_reducers(
        &mut self,
        seeds: Vec<FormulaReducerSeed>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let seeded_parents = seeds
            .iter()
            .map(|seed| match seed {
                FormulaReducerSeed::Admit(seed) => seed.batch.parents.row_count,
                FormulaReducerSeed::Emit(seed) => seed.batch.parents.row_count,
                FormulaReducerSeed::SetAdmit(seed) => seed.destination.parent_count(),
            })
            .sum();
        let drained = self.drain_formula_reducer_seeds(
            seeds,
            plan,
            stable,
            stable_interner,
            stats,
        );
        DeltaSeedOutcome {
            continuation: drained.continuation,
            publication: None,
            active: drained.active,
            terminal_activations: Vec::new(),
            completed_activation_ids: Vec::new(),
            terminal_family: None,
            seeded_parents,
        }
    }

    fn release_completion(
        &mut self,
        completed: CompletedActivation,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> FormulaReducerDrain {
        let CompletedActivation {
            activation: _,
            return_to,
            effect,
        } = completed;
        match (return_to, effect) {
            (_, DeltaCompletion::Cleanup) => {
                // A streaming activation has already resumed one affine copy
                // of its continuation per semantic effect. Quiescence only
                // retires producer credits; replaying the template here would
                // duplicate publication.
                FormulaReducerDrain::default()
            }
            (return_to, DeltaCompletion::Support(truth)) => self.release_support(
                return_to,
                truth,
                plan,
                stable,
                stable_interner,
                stats,
            ),
            (
                DeltaReturn::Stable {
                    desc,
                    parent,
                    set_admit_result,
                },
                DeltaCompletion::Candidates(mut result),
            ) => {
                let continuation = if result.is_empty() {
                    None
                } else if set_admit_result && !result.admit_set_tail_stable(1) {
                    return self.drain_formula_reducer_seeds(
                        vec![FormulaReducerSeed::SetAdmit(SetAdmissionSeed {
                            successor: desc,
                            destination: SetAdmissionDestination::Candidate(CandidateBatch {
                                parents: RowBatch {
                                    rows: parent.into_vec(),
                                    row_count: 1,
                                },
                                candidates: result,
                            }),
                        })],
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                } else {
                    file_with_plan(
                        stable,
                        stable_interner,
                        plan,
                        desc,
                        StateBucket::Candidates(CandidateBatch {
                            parents: RowBatch {
                                rows: parent.into_vec(),
                                row_count: 1,
                            },
                            candidates: result,
                        }),
                        stats,
                    )
                };
                FormulaReducerDrain {
                    continuation,
                    active: None,
                }
            }
            (
                DeltaReturn::SetAdmission {
                    successor,
                    mut destination,
                },
                DeltaCompletion::Candidates(result),
            ) => {
                destination.install_candidates(result);
                let continuation = destination
                    .into_live_bucket(successor.bound.count())
                    .and_then(|bucket| {
                        file_with_plan(
                            stable,
                            stable_interner,
                            plan,
                            successor,
                            bucket,
                            stats,
                        )
                    });
                FormulaReducerDrain {
                    continuation,
                    active: None,
                }
            }
            (
                DeltaReturn::Formula {
                    bound,
                    counter,
                    batch,
                },
                DeltaCompletion::Candidates(result),
            ) => {
                if matches!(
                    &stable_interner.formula(counter).focus,
                    FormulaFocus::Action {
                        stage: FormulaStage::Propose,
                        ..
                    }
                ) {
                    stats.candidates_proposed += result.len();
                    stats.max_propose_candidates = stats.max_propose_candidates.max(result.len());
                }
                let mut seeds = Vec::new();
                let continuation = finish_formula_action_result(
                    plan,
                    bound,
                    counter,
                    batch,
                    result,
                    stable,
                    stable_interner,
                    stats,
                    &mut seeds,
                );
                let mut drained = self.drain_formula_reducer_seeds(
                    seeds,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
                prefer_continuation(&mut drained.continuation, continuation);
                drained
            }
            (
                DeltaReturn::FormulaOrAdmit {
                    bound,
                    batch,
                    continuation: formula_continuation,
                },
                DeltaCompletion::FormulaOrAdmitted,
            ) => {
                let mut seeds = Vec::new();
                let continuation = finish_formula_or_admission(
                    plan,
                    bound,
                    batch,
                    formula_continuation,
                    stable,
                    stable_interner,
                    stats,
                    &mut seeds,
                );
                let mut drained = self.drain_formula_reducer_seeds(
                    seeds,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
                prefer_continuation(&mut drained.continuation, continuation);
                drained
            }
            (
                DeltaReturn::FormulaOrEmit {
                    bound,
                    batch,
                    counter,
                },
                DeltaCompletion::Candidates(result),
            ) => {
                let mut seeds = Vec::new();
                let continuation = finish_formula_or_emission(
                    plan,
                    bound,
                    counter,
                    batch,
                    result,
                    stable,
                    stable_interner,
                    stats,
                    &mut seeds,
                );
                let mut drained = self.drain_formula_reducer_seeds(
                    seeds,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
                prefer_continuation(&mut drained.continuation, continuation);
                drained
            }
            (DeltaReturn::FormulaOrAdmit { .. }, effect)
            | (DeltaReturn::FormulaOrEmit { .. }, effect)
            | (DeltaReturn::SetAdmission { .. }, effect) => {
                panic!("engine reducer completed with incompatible effect: {effect:?}")
            }
            (DeltaReturn::Stable { .. } | DeltaReturn::Formula { .. }, effect) => {
                panic!("ordinary delta reducer completed with incompatible effect: {effect:?}")
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_streaming<Accepted>(
        &mut self,
        activation: ActivationId,
        streamed: DeltaStreamingReturn,
        accepted: Accepted,
        direct_terminal_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStreamingRelease
    where
        Accepted: IntoIterator<Item = RawInline>,
        Accepted::IntoIter: ExactSizeIterator,
    {
        if streamed.effect == DeltaStreamingEffect::Support {
            let released = self.release_support(
                streamed.return_to,
                true,
                plan,
                stable,
                stable_interner,
                stats,
            );
            return DeltaStreamingRelease {
                stable: DeltaStableEffects {
                    continuation: released.continuation,
                    publication: None,
                },
                active: released.active,
            };
        }
        let accepted = accepted.into_iter();
        let accepted_len = accepted.len();
        debug_assert!(accepted_len > 0);
        stats.candidates_proposed += accepted_len;
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted_len);
        if let Some(full) = direct_terminal_full {
            let DeltaReturn::Stable { desc, parent, .. } = streamed.return_to else {
                panic!("a direct-terminal publication returned through a formula")
            };
            let ResidualPhase::Candidate {
                variable,
                relevant,
                checked,
            } = &desc.phase
            else {
                panic!("a direct-terminal publication did not return to candidates")
            };
            assert_eq!(
                relevant, checked,
                "a direct-terminal publication retained unchecked confirmers"
            );
            let (committed, rows) = committed_candidate_rows_with(
                desc.bound,
                *variable,
                RowBatch {
                    rows: parent.into_vec(),
                    row_count: 1,
                },
                accepted_len,
                |commit_one| {
                    for candidate in accepted {
                        commit_one(0, candidate);
                    }
                },
                |_| {},
            );
            assert_eq!(
                committed, full,
                "a direct-terminal publication did not bind the full result schema"
            );
            return DeltaStreamingRelease {
                stable: DeltaStableEffects {
                    continuation: None,
                    publication: Some(TerminalPublicationBatch::new(activation, rows)),
                },
                active: None,
            };
        }
        let candidates = CandidatePayload::Values(accepted.collect());
        let mut reducer_seeds = Vec::new();
        let continuation = match streamed.return_to {
            DeltaReturn::Stable { desc, parent, .. } => file_with_plan(
                stable,
                stable_interner,
                plan,
                desc,
                StateBucket::Candidates(CandidateBatch {
                    parents: RowBatch {
                        rows: parent.into_vec(),
                        row_count: 1,
                    },
                    candidates,
                }),
                stats,
            ),
            DeltaReturn::Formula {
                bound,
                counter,
                batch,
            } => finish_formula_action_result(
                plan,
                bound,
                counter,
                batch,
                candidates,
                stable,
                stable_interner,
                stats,
                &mut reducer_seeds,
            ),
            DeltaReturn::FormulaOrAdmit { .. }
            | DeltaReturn::FormulaOrEmit { .. }
            | DeltaReturn::SetAdmission { .. } => {
                panic!("a private engine reducer attempted streaming publication")
            }
        };
        assert!(
            reducer_seeds.is_empty(),
            "a certified streaming Formula proposal reached an OR reducer"
        );
        DeltaStreamingRelease {
            stable: DeltaStableEffects {
                continuation,
                publication: None,
            },
            active: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_support(
        &mut self,
        return_to: DeltaReturn,
        truth: bool,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> FormulaReducerDrain {
        let DeltaReturn::Formula {
            bound,
            counter,
            batch,
        } = return_to
        else {
            panic!("delta support returned to a candidate continuation")
        };
        assert!(matches!(
            &stable_interner.formula(counter).focus,
            FormulaFocus::Action {
                stage: FormulaStage::Support,
                ..
            }
        ));
        let completed = stable_interner
            .formula_pcs
            .complete(&plan.finite_formula, counter);
        let desc = StateDesc {
            bound,
            phase: ResidualPhase::Formula { counter },
        };
        let mut reducer_seeds = Vec::new();
        let continuation = propagate_formula_support(
            plan,
            &desc,
            completed,
            truth,
            batch,
            stable,
            stable_interner,
            stats,
            &mut reducer_seeds,
        );
        let mut drained = self.drain_formula_reducer_seeds(
            reducer_seeds,
            plan,
            stable,
            stable_interner,
            stats,
        );
        prefer_continuation(&mut drained.continuation, continuation);
        drained
    }

    fn file(&mut self, desc: DeltaDesc, tasks: Vec<DeltaTask>) -> Option<ActiveDeltaContinuation> {
        let activation = tasks.last()?.activation;
        let id = self.interner.intern(desc);
        self.worklist.entry(id).or_default().extend(tasks);
        Some(ActiveDeltaContinuation {
            state: id,
            activation,
        })
    }

    fn file_source(
        &mut self,
        desc: DeltaDesc,
        mut tasks: Vec<SourceTask>,
    ) -> Option<ActiveDeltaContinuation> {
        let activation = tasks.last()?.activation;
        let id = self.interner.intern(desc);
        self.source_worklist
            .entry(id)
            .or_default()
            .tasks
            .append(&mut tasks);
        Some(ActiveDeltaContinuation {
            state: id,
            activation,
        })
    }

    fn file_program_state(
        &mut self,
        state: DeltaStateId,
        mut tasks: Vec<ProgramTask>,
    ) -> Option<ActiveDeltaContinuation> {
        let activation = tasks.last()?.activation;
        assert!(
            self.interner.program(state).is_some(),
            "typed program task was filed under a legacy delta state"
        );
        self.program_worklist
            .entry(state)
            .or_default()
            .append(&mut tasks);
        Some(ActiveDeltaContinuation { state, activation })
    }

    fn has_active_source(&self, active: ActiveDeltaContinuation) -> bool {
        self.source_worklist
            .get(&active.state)
            .is_some_and(|bucket| {
                bucket
                    .tasks
                    .iter()
                    .any(|task| task.activation == active.activation)
            })
    }

    fn has_active_transition(&self, active: ActiveDeltaContinuation) -> bool {
        self.worklist
            .get(&active.state)
            .is_some_and(|bucket| bucket.contains_activation(active.activation))
    }

    fn has_active_program(&self, active: ActiveDeltaContinuation) -> bool {
        self.program_worklist
            .get(&active.state)
            .is_some_and(|bucket| bucket.contains_activation(active.activation))
    }

    fn allows_global_width_growth(
        &self,
        dispatch: &PhysicalDispatch,
        search_width: usize,
        terminal_publications: &OrderedActivationSet,
    ) -> bool {
        if dispatch.terminal_activations.is_empty() {
            return true;
        }
        dispatch.kind == PhysicalDispatchKind::Source
            || dispatch.terminal_budgets.iter().any(|receipt| {
                receipt.assigned == search_width.max(1)
                    && receipt.quantum == search_width.max(1)
                    && self.registry.is_live(receipt.activation)
                    && !terminal_publications.contains(&receipt.activation)
            })
    }

    fn account_physical_dispatch(
        &mut self,
        dispatch: PhysicalDispatch,
        search_width: usize,
        examined_before: usize,
        terminal_publications: &OrderedActivationSet,
        stats: &mut ResidualStateStats,
    ) -> bool {
        if dispatch.terminal_activations.is_empty() {
            stats.delta_nonterminal_calls += 1;
            return true;
        }
        let work_budget = dispatch.work_budget();
        let task_count = dispatch.task_count();
        let published = dispatch
            .terminal_activations
            .iter()
            .any(|activation| terminal_publications.contains(activation));
        stats.delta_terminal_calls += 1;
        stats.delta_terminal_work_budget += work_budget;
        stats.max_delta_terminal_work_budget =
            stats.max_delta_terminal_work_budget.max(work_budget);
        stats.delta_terminal_tasks += task_count;
        stats.max_delta_terminal_task_cohort = stats.max_delta_terminal_task_cohort.max(task_count);
        stats.delta_terminal_remainder_tasks += dispatch.remainder_tasks;
        let examined_after = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
        stats.delta_terminal_candidates_examined += examined_after.saturating_sub(examined_before);
        stats.delta_terminal_publications += usize::from(published);
        for receipt in &dispatch.terminal_budgets {
            let published = terminal_publications.contains(&receipt.activation);
            // Publication is always activation-local reset evidence. A miss
            // advances sparse effort only after this activation, rather than
            // the physical cohort in aggregate, received its complete
            // pre-dispatch quantum.
            let (reset, widened) = if published || receipt.assigned >= receipt.quantum {
                self.registry.finish_dispatch(
                    receipt.activation,
                    search_width,
                    dispatch.kind,
                    published,
                )
            } else {
                (false, false)
            };
            stats.delta_terminal_sparse_resets += usize::from(reset);
            stats.delta_terminal_sparse_widenings += usize::from(widened);
        }
        self.allows_global_width_growth(&dispatch, search_width, terminal_publications)
    }

    fn pop_active_transition(
        &mut self,
        active: ActiveDeltaContinuation,
        width: usize,
    ) -> (DeltaDesc, Vec<DeltaTask>) {
        let (tasks, empty) = {
            let bucket = self
                .worklist
                .get_mut(&active.state)
                .expect("active delta transition state remains live");
            let tasks = bucket.take_activation(active.activation, width);
            (tasks, bucket.is_empty())
        };
        assert!(
            !tasks.is_empty(),
            "active delta transition lost its affine task"
        );
        if empty {
            self.worklist.remove(&active.state);
        }
        (self.interner.get(active.state).clone(), tasks)
    }

    fn pop_active_source(
        &mut self,
        active: ActiveDeltaContinuation,
        width: usize,
    ) -> (DeltaDesc, Vec<SourceTask>) {
        let (tasks, empty) = {
            let bucket = self
                .source_worklist
                .get_mut(&active.state)
                .expect("active delta source state remains live");
            let tasks = bucket.take_activation(active.activation, width);
            (tasks, bucket.tasks.is_empty())
        };
        assert!(
            !tasks.is_empty(),
            "active delta source lost its affine task"
        );
        if empty {
            self.source_worklist.remove(&active.state);
        }
        (self.interner.get(active.state).clone(), tasks)
    }

    fn pop_active_program(
        &mut self,
        active: ActiveDeltaContinuation,
        search_width: usize,
    ) -> (DeltaStateId, Vec<ProgramTask>, PhysicalDispatch) {
        let (selection, empty, remainder_tasks) = {
            let bucket = self
                .program_worklist
                .get_mut(&active.state)
                .expect("active typed program state remains live");
            let selection = bucket.take_active(&self.registry, active.activation, search_width);
            (selection, bucket.is_empty(), bucket.len())
        };
        if empty {
            self.program_worklist.remove(&active.state);
        }
        let ProgramSelection { key, tasks, limits } = selection;
        let kind = match key.class.pacing() {
            ProgramPacing::Search => PhysicalDispatchKind::Source,
            ProgramPacing::Activation => PhysicalDispatchKind::Program,
        };
        let dispatch = PhysicalDispatch::new(
            &self.registry,
            kind,
            search_width,
            tasks.iter().map(|task| task.activation),
            limits,
            remainder_tasks,
        );
        (active.state, tasks, dispatch)
    }

    fn pop_program_bounded(
        &mut self,
        search_width: usize,
    ) -> (DeltaStateId, Vec<ProgramTask>, PhysicalDispatch) {
        let id = *self
            .program_worklist
            .last_key_value()
            .expect("typed program pop requires live work")
            .0;
        let (selection, empty, remainder_tasks) = {
            let bucket = self
                .program_worklist
                .get_mut(&id)
                .expect("selected typed program state");
            let selection = bucket.take_global(
                &self.registry,
                search_width,
                self.activation_width,
                &mut self.terminal_selection_slots,
                &mut self.terminal_selections,
            );
            (selection, bucket.is_empty(), bucket.len())
        };
        if empty {
            self.program_worklist.remove(&id);
        }
        let ProgramSelection { key, tasks, limits } = selection;
        let kind = match key.class.pacing() {
            ProgramPacing::Search => PhysicalDispatchKind::Source,
            ProgramPacing::Activation => PhysicalDispatchKind::Program,
        };
        let dispatch = PhysicalDispatch::new(
            &self.registry,
            kind,
            search_width,
            tasks.iter().map(|task| task.activation),
            limits,
            remainder_tasks,
        );
        (id, tasks, dispatch)
    }

    #[cfg(test)]
    fn pop(&mut self, width: usize) -> (DeltaDesc, Vec<DeltaTask>) {
        let (desc, tasks, _) = self.pop_bounded(width);
        (desc, tasks)
    }

    fn pop_bounded(
        &mut self,
        search_width: usize,
    ) -> (DeltaDesc, Vec<DeltaTask>, PhysicalDispatch) {
        let full = self.worklist.iter().rev().find_map(|(&id, bucket)| {
            let activation = bucket.last()?.activation;
            let width = self
                .registry
                .transition_dispatch_width(activation, search_width);
            (bucket.len() >= width).then_some(id)
        });
        let id = full.unwrap_or_else(|| {
            *self
                .worklist
                .last_key_value()
                .expect("delta pop requires live work")
                .0
        });
        let (tasks, task_limits, empty, remainder_tasks) = {
            let registry = &self.registry;
            let activation_width = self.activation_width;
            let terminal_selection_slots = &mut self.terminal_selection_slots;
            let terminal_selections = &mut self.terminal_selections;
            let bucket = self.worklist.get_mut(&id).expect("selected delta state");
            let (tasks, task_limits) = bucket.take_tail(
                registry,
                search_width,
                activation_width,
                terminal_selection_slots,
                terminal_selections,
            );
            let remainder_tasks = bucket.len();
            (tasks, task_limits, bucket.is_empty(), remainder_tasks)
        };
        if empty {
            self.worklist.remove(&id);
        }
        let dispatch = PhysicalDispatch::new(
            &self.registry,
            PhysicalDispatchKind::Transition,
            search_width,
            tasks.iter().map(|task| task.activation),
            task_limits,
            remainder_tasks,
        );
        (self.interner.get(id).clone(), tasks, dispatch)
    }

    #[cfg(test)]
    fn pop_source(&mut self, width: usize) -> (DeltaDesc, Vec<SourceTask>) {
        let (desc, tasks, _) = self.pop_source_bounded(width);
        (desc, tasks)
    }

    fn pop_source_bounded(
        &mut self,
        search_width: usize,
    ) -> (DeltaDesc, Vec<SourceTask>, PhysicalDispatch) {
        let id = *self
            .source_worklist
            .last_key_value()
            .expect("source pop requires live work")
            .0;
        let activation = self
            .source_worklist
            .get(&id)
            .and_then(|bucket| bucket.tasks.last())
            .expect("selected source state has live work")
            .activation;
        let width = self
            .registry
            .source_dispatch_width(activation, search_width);
        let (tasks, empty, remainder_tasks) = {
            let registry = &self.registry;
            let bucket = self
                .source_worklist
                .get_mut(&id)
                .expect("selected source state");
            let key = SourceDispatchKey::of(
                registry,
                bucket.tasks.last().expect("live source bucket is nonempty"),
            );
            let mut selected = Vec::with_capacity(width.min(bucket.tasks.len()));
            let mut retained = Vec::with_capacity(bucket.tasks.len());
            for task in std::mem::take(&mut bucket.tasks).into_iter().rev() {
                if selected.len() < width && SourceDispatchKey::of(registry, &task) == key {
                    selected.push(task);
                } else {
                    retained.push(task);
                }
            }
            retained.reverse();
            bucket.tasks = retained;
            let remainder_tasks = bucket.tasks.len();
            (selected, bucket.tasks.is_empty(), remainder_tasks)
        };
        if empty {
            self.source_worklist.remove(&id);
        }
        let dispatch = PhysicalDispatch::new(
            &self.registry,
            PhysicalDispatchKind::Source,
            search_width,
            tasks.iter().map(|task| task.activation),
            even_limits(width, tasks.len()),
            remainder_tasks,
        );
        (self.interner.get(id).clone(), tasks, dispatch)
    }

    /// Advances only the affine activation named by a physical continuation.
    ///
    /// A suspended source generator wins over unrelated transition work. At
    /// scheduler boundaries one activation cannot own both kinds at once, but
    /// checking source first makes the intended source -> transition -> source
    /// cycle explicit and prevents a cold global bucket from preempting it.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(super) fn step_active<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        active: ActiveDeltaContinuation,
        width: usize,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> ActiveDeltaStepOutcome {
        self.step_active_bounded(
            root,
            plan,
            active,
            width,
            None,
            stable,
            stable_interner,
            stats,
        )
    }

    pub(super) fn step_active_bounded<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        active: ActiveDeltaContinuation,
        search_width: usize,
        direct_terminal_publication_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> ActiveDeltaStepOutcome {
        let has_source = self.has_active_source(active);
        let has_transition = self.has_active_transition(active);
        let has_program = self.has_active_program(active);
        assert!(
            has_source || has_transition || has_program,
            "active delta continuation has no scheduled affine task"
        );
        debug_assert!(
            usize::from(has_source) + usize::from(has_transition) + usize::from(has_program) == 1,
            "one delta activation owns incompatible scheduler queue kinds simultaneously"
        );

        let terminal = self.registry.physical_activation_class(active.activation)
            == DeltaPhysicalClass::TerminalStreaming;
        let direct_terminal_full = direct_terminal_publication_full.filter(|_| terminal);
        let examined_before = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
        let outcome = if has_program {
            let (state, tasks, dispatch) = self.pop_active_program(active, search_width);
            let physical = self.step_program(
                root,
                plan,
                state,
                tasks,
                &dispatch.task_limits,
                true,
                direct_terminal_full,
                stable,
                stable_interner,
                stats,
            );
            let retired_search_receipt = physical.retired_search_receipt;
            let mut outcome = physical.outcome;
            let physical_allows_global_width_growth = self.account_physical_dispatch(
                dispatch,
                search_width,
                examined_before,
                &physical.terminal_publications,
                stats,
            );
            outcome.allows_global_width_growth =
                retired_search_receipt || physical_allows_global_width_growth;
            outcome
        } else if has_source {
            let width = self
                .registry
                .source_dispatch_width(active.activation, search_width);
            let (desc, tasks) = self.pop_active_source(active, width);
            let task_count = tasks.len();
            let remainder_tasks = self
                .source_worklist
                .get(&active.state)
                .map_or(0, |bucket| bucket.tasks.len());
            let dispatch = PhysicalDispatch::new(
                &self.registry,
                PhysicalDispatchKind::Source,
                search_width,
                tasks.iter().map(|task| task.activation),
                even_limits(width, task_count),
                remainder_tasks,
            );
            let physical = self.step_sources(
                root,
                plan,
                desc,
                tasks,
                &dispatch.task_limits,
                direct_terminal_full,
                stable,
                stable_interner,
                stats,
            );
            let mut outcome = physical.outcome;
            outcome.allows_global_width_growth = self.account_physical_dispatch(
                dispatch,
                search_width,
                examined_before,
                &physical.terminal_publications,
                stats,
            );
            outcome
        } else {
            let width = self
                .registry
                .transition_dispatch_width(active.activation, search_width);
            let (desc, tasks) = self.pop_active_transition(active, width);
            let task_count = tasks.len();
            let remainder_tasks = self.worklist.get(&active.state).map_or(0, DeltaBucket::len);
            let dispatch = PhysicalDispatch::new(
                &self.registry,
                PhysicalDispatchKind::Transition,
                search_width,
                tasks.iter().map(|task| task.activation),
                even_limits(width, task_count),
                remainder_tasks,
            );
            let physical = self.step_transitions(
                root,
                plan,
                desc,
                tasks,
                &dispatch.task_limits,
                direct_terminal_full,
                stable,
                stable_interner,
                stats,
            );
            let mut outcome = physical.outcome;
            outcome.allows_global_width_growth = self.account_physical_dispatch(
                dispatch,
                search_width,
                examined_before,
                &physical.terminal_publications,
                stats,
            );
            outcome
        };
        let yielded = outcome.has_stable_effect();
        let live = self.registry.is_live(active.activation);
        let settled = outcome.retargeted.get(&active.activation).copied();
        // A completed graph/action activation may transfer its affine lineage
        // to a fresh engine reducer activation.  The explicit old -> new
        // receipt remains authoritative even though the old registry entry
        // has already been removed; queue liveness must not be used to infer
        // or discard that handoff.
        let resume = settled.or_else(|| live.then_some(active));
        let status = if yielded {
            ActiveDeltaStatus::Yielded
        } else if resume.is_some() {
            ActiveDeltaStatus::Pending
        } else {
            ActiveDeltaStatus::Quiescent
        };
        ActiveDeltaStepOutcome {
            outcome,
            status,
            resume,
        }
    }

    /// Executes one structural product-state cohort and files accepted
    /// proposal endpoints or quiescent confirmation reductions back into the
    /// ordinary acyclic Candidate continuation.
    #[cfg(test)]
    pub(super) fn step<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        width: usize,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStepOutcome {
        self.step_bounded(root, plan, width, None, stable, stable_interner, stats)
    }

    pub(super) fn step_bounded<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        search_width: usize,
        direct_terminal_publication_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStepOutcome {
        if !self.program_worklist.is_empty() {
            let (state, tasks, dispatch) = self.pop_program_bounded(search_width);
            let examined_before = stats
                .delta_source_candidates_examined
                .saturating_add(stats.delta_transition_candidates_examined);
            let physical = self.step_program(
                root,
                plan,
                state,
                tasks,
                &dispatch.task_limits,
                false,
                direct_terminal_publication_full,
                stable,
                stable_interner,
                stats,
            );
            let retired_search_receipt = physical.retired_search_receipt;
            let mut outcome = physical.outcome;
            let physical_allows_global_width_growth = self.account_physical_dispatch(
                dispatch,
                search_width,
                examined_before,
                &physical.terminal_publications,
                stats,
            );
            outcome.allows_global_width_growth =
                retired_search_receipt || physical_allows_global_width_growth;
            return outcome;
        }
        if self.worklist.is_empty() {
            return self.step_source(
                root,
                plan,
                search_width,
                direct_terminal_publication_full,
                stable,
                stable_interner,
                stats,
            );
        }

        let (desc, tasks, dispatch) = self.pop_bounded(search_width);
        let examined_before = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
        let physical = self.step_transitions(
            root,
            plan,
            desc,
            tasks,
            &dispatch.task_limits,
            direct_terminal_publication_full,
            stable,
            stable_interner,
            stats,
        );
        let mut outcome = physical.outcome;
        outcome.allows_global_width_growth = self.account_physical_dispatch(
            dispatch,
            search_width,
            examined_before,
            &physical.terminal_publications,
            stats,
        );
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn step_transitions<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        desc: DeltaDesc,
        tasks: Vec<DeltaTask>,
        limits: &[usize],
        direct_terminal_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaPhysicalOutcome {
        let constraint = desc.resolve(root, plan);
        let task_count = tasks.len();
        assert_eq!(limits.len(), task_count);
        assert!(limits.iter().all(|&limit| limit > 0));
        let nodes: Vec<_> = tasks.iter().map(|task| task.node).collect();
        let cursors: Vec<_> = tasks.iter().map(|task| task.cursor).collect();
        let batch = ResidualDeltaExpandBatch {
            nodes: &nodes,
            cursors: &cursors,
            limits,
        };
        let mut pages = Vec::with_capacity(task_count);
        let mut tagged_successors = Vec::new();
        constraint.residual_delta_expand_pages(
            desc.variable,
            batch,
            &mut pages,
            &mut tagged_successors,
        );
        assert_eq!(
            pages.len(),
            task_count,
            "delta transition cohort returned the wrong page count"
        );
        let successor_ranges =
            tagged_ranges(&tagged_successors, task_count, "transition successor");
        let mut next_cursors = vec![None; task_count];
        let mut paged = vec![false; task_count];
        let mut legacy_indices = Vec::new();
        let mut legacy_nodes = Vec::new();
        let mut paged_count = 0usize;
        for (index, (((task, page), range), &limit)) in tasks
            .iter()
            .zip(pages)
            .zip(&successor_ranges)
            .zip(limits)
            .enumerate()
        {
            let Some(page) = page else {
                assert_eq!(
                    task.cursor,
                    ResidualDeltaExpandCursor::Start,
                    "paged delta expansion became unsupported after suspension"
                );
                assert!(
                    range.is_empty(),
                    "unsupported delta expansion page mutated its output"
                );
                legacy_indices.push(index);
                legacy_nodes.push(task.node);
                continue;
            };
            paged_count += 1;
            assert!(page.examined <= limit);
            assert!(range.len() <= page.examined);
            if let Some(next) = page.next {
                assert!(
                    page.examined > 0,
                    "a delta expansion cursor made no progress"
                );
                match (task.cursor, next) {
                    (ResidualDeltaExpandCursor::Start, ResidualDeltaExpandCursor::After { .. }) => {
                    }
                    (
                        ResidualDeltaExpandCursor::After { .. },
                        ResidualDeltaExpandCursor::After { .. },
                    ) => assert!(next > task.cursor, "delta expansion cursor did not advance"),
                    (_, ResidualDeltaExpandCursor::Start) => {
                        panic!("delta expansion page restarted its cursor")
                    }
                }
            }
            stats.delta_transition_pages += 1;
            stats.delta_transition_candidates_examined += page.examined;
            next_cursors[index] = page.next;
            paged[index] = true;
        }
        if paged_count > 0 {
            stats.delta_transition_cohorts += 1;
            stats.max_delta_transition_cohort = stats.max_delta_transition_cohort.max(paged_count);
        }

        let mut legacy_successors = vec![Vec::new(); legacy_nodes.len()];
        if !legacy_nodes.is_empty() {
            let mut tagged = Vec::new();
            assert!(
                constraint.residual_delta_expand(desc.variable, &legacy_nodes, &mut tagged),
                "delta expansion became unsupported after seeding"
            );
            let mut previous = 0u32;
            for (position, (tag, output)) in tagged.into_iter().enumerate() {
                assert!(
                    (tag as usize) < legacy_nodes.len(),
                    "delta successor tag out of range"
                );
                assert!(
                    position == 0 || tag >= previous,
                    "delta successor tags are not grouped in ascending order"
                );
                previous = tag;
                legacy_successors[tag as usize].push(output);
            }
        }

        let mut next_tasks = Vec::new();
        let mut resumed_sources = Vec::new();
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut retargeted = AHashMap::new();
        let mut dead_pages = 0usize;
        let mut source_dead_pages = 0usize;
        let mut transition_dead_pages = 0usize;
        let mut completed_activations = 0usize;
        let mut terminal_publications = OrderedActivationSet::default();
        for (task_index, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.activation, task.credit.key.activation);
            let terminal = self.registry.physical_activation_class(task.activation)
                == DeltaPhysicalClass::TerminalStreaming;
            let outcome = if paged[task_index] {
                self.registry.replace_traversal_page(
                    task.credit,
                    tagged_successors[successor_ranges[task_index].clone()]
                        .iter()
                        .map(|(_, output)| *output),
                    next_cursors[task_index],
                )
            } else {
                let legacy_index = legacy_indices
                    .binary_search(&task_index)
                    .expect("unsupported transition task lost its legacy result slot");
                self.registry.replace_traversal_page(
                    task.credit,
                    legacy_successors[legacy_index].iter().copied(),
                    None,
                )
            };
            let retired_source_page = outcome.retired_source_page;
            let transition_page_had_effect =
                !outcome.children.is_empty() || !outcome.accepted.is_empty();
            let mut task_effects = DeltaStableEffects::default();
            if let Some((cursor, credit)) = outcome.resumed_traversal {
                next_tasks.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node: task.node,
                    cursor,
                });
            }
            for (node, credit) in outcome.children {
                next_tasks.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node,
                    cursor: ResidualDeltaExpandCursor::Start,
                });
            }
            if let Some((source_cursor, credit)) = outcome.resumed_source {
                resumed_sources.push(SourceTask {
                    activation: task.activation,
                    credit,
                    cursor: source_cursor,
                });
            }
            if !outcome.accepted.is_empty() {
                let direct_terminal = direct_terminal_full.filter(|_| {
                    self.registry.physical_activation_class(task.activation)
                        == DeltaPhysicalClass::TerminalStreaming
                });
                if let Some(streamed) = self.registry.take_streaming_return(task.activation) {
                    let released = self.release_streaming(
                        task.activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    task_effects.absorb(released.stable);
                    if let Some(active) = released.active {
                        assert!(retargeted.insert(task.activation, active).is_none());
                    }
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Retargeted(active) => {
                        assert_eq!(active.activation, task.activation);
                        assert!(retargeted.insert(task.activation, active).is_none());
                    }
                    DeltaSettlement::Completed(completed) => {
                        let old_activation = completed.activation;
                        let released = self.release_completion(
                            completed,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        );
                        prefer_continuation(
                            &mut task_effects.continuation,
                            released.continuation,
                        );
                        if let Some(active) = released.active {
                            assert!(retargeted.insert(old_activation, active).is_none());
                        } else if !retargeted.contains_key(&old_activation) {
                            completed_activations += 1;
                            completed_activation_ids.push(old_activation);
                        }
                    }
                }
            }
            let source_page_dead = retired_source_page.is_some_and(|page| !page.had_stable_effect)
                && !task_effects.has_effect();
            let transition_page_dead =
                paged[task_index] && !transition_page_had_effect && !task_effects.has_effect();
            if source_page_dead || transition_page_dead {
                dead_pages += 1;
            }
            source_dead_pages += usize::from(source_page_dead);
            transition_dead_pages += usize::from(transition_page_dead);
            if terminal && task_effects.has_effect() {
                let _ = terminal_publications.insert(task.activation);
            }
            effects.absorb(task_effects);
        }
        let _ = self.file(desc.clone(), next_tasks);
        let _ = self.file_source(desc, resumed_sources);
        stats.delta_source_dead_pages += source_dead_pages;
        stats.delta_transition_dead_pages += transition_dead_pages;
        DeltaPhysicalOutcome {
            outcome: DeltaStepOutcome {
                continuation: effects.continuation,
                publication: effects.publication,
                completed_activation_ids,
                retargeted,
                dead_pages,
                source_dead_pages,
                transition_dead_pages,
                completed_activations,
                completed_transition_cohort: completed_activations > 1,
                allows_global_width_growth: true,
            },
            terminal_publications,
            retired_search_receipt: false,
        }
    }

    /// Executes one physically compatible cohort of opaque typed
    /// continuations. The erased family boundary is crossed once: handles are
    /// affinely taken into a dense typed vector, and the adapter returns one
    /// replacement receipt per input in scheduler order.
    #[allow(clippy::too_many_arguments)]
    fn step_program<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        state: DeltaStateId,
        tasks: Vec<ProgramTask>,
        limits: &[usize],
        active_pop: bool,
        direct_terminal_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaPhysicalOutcome {
        #[cfg(feature = "allocation-probe")]
        let _allocation_probe =
            super::allocation_probe::enter(super::allocation_probe::Phase::Program);
        assert!(!tasks.is_empty());
        assert_eq!(tasks.len(), limits.len());
        assert!(limits.iter().all(|&limit| limit > 0));
        if active_pop {
            stats.delta_program_active_pops += 1;
        } else {
            stats.delta_program_global_pops += 1;
        }

        let address = self
            .interner
            .program(state)
            .cloned()
            .expect("typed program task was scheduled under a legacy delta state");
        let address_key = address.key();
        let spec = address.resolve(root, plan);
        let private_direct = address.has_private_direct_effects();
        let cohort_key = ProgramCohortKey::of(&self.registry, &tasks[0]);
        assert!(
            tasks
                .iter()
                .all(|task| ProgramCohortKey::of(&self.registry, task) == cohort_key),
            "one typed program cohort mixed incompatible physical dispatch shapes"
        );

        let row_count = tasks.len();
        let mut parents: SmallVec<[RawInline; 8]> = SmallVec::new();
        let mut candidate_sets: SmallVec<[Option<&[RawInline]>; 1]> =
            SmallVec::with_capacity(row_count);
        for task in &tasks {
            assert_eq!(task.activation, task.credit.key.activation);
            let (bound, parent, candidates) = self.registry.source_context(task.activation);
            assert_eq!(bound, cohort_key.bound);
            assert_eq!(candidates.is_some(), cohort_key.has_candidates);
            parents.extend_from_slice(parent);
            candidate_sets.push(candidates);
        }
        let vars: SmallVec<[VariableId; 8]> = cohort_key.bound.into_iter().collect();
        let view = rows_view(&vars, &parents, row_count);
        let activations: SmallVec<[ProgramActivation; 1]> = tasks
            .iter()
            .map(|task| ProgramActivation(task.activation.0))
            .collect();
        let mut task_receipts: SmallVec<
            [(
                ActivationId,
                ProducerCredit,
                DispatchClass,
                ProgramPacing,
                bool,
            ); 1],
        > = SmallVec::with_capacity(row_count);
        let mut work: SmallVec<[ProgramWork; 1]> = SmallVec::with_capacity(row_count);
        for task in tasks {
            let unique_unjoined = self
                .registry
                .program_credit_is_unjoined_unique(&task.credit);
            task_receipts.push((
                task.activation,
                task.credit,
                task.work.dispatch,
                task.work.pacing,
                unique_unjoined,
            ));
            work.push(task.work);
        }
        let mut receipt = ProgramBatchEffects::default();
        spec.step_batch_for(
            self.program_runtimes
                .get_mut(&state)
                .expect("typed program state lost its runtime"),
            address_key,
            ProgramBatch {
                stratum: address.stratum(),
                view,
                candidate_sets: &candidate_sets,
                activations: &activations,
                work: &work,
                limits,
            },
            &mut receipt,
        );
        drop(candidate_sets);
        assert_eq!(
            receipt.pages.len(),
            row_count,
            "typed program returned the wrong page count"
        );
        for (page, &limit) in receipt.pages.iter().zip(limits) {
            assert!(
                page.examined <= limit,
                "typed program exceeded one input's physical work budget"
            );
        }
        let child_ranges = program_child_ranges(&receipt.children, row_count);
        let direct_ranges = tagged_ranges(&receipt.direct, row_count, "program direct effect");
        let accepted_ranges = tagged_ranges(
            &receipt.accepted,
            row_count,
            "program candidate observation",
        );
        let supported_ranges =
            tagged_ranges(&receipt.supported, row_count, "program support observation");

        // Placement is observation only. Static executor labels deliberately
        // stay out of the ordinary hot-path aggregate and never feed dispatch.
        if receipt.placement.is_some() {
            let granted_work = limits.iter().sum();
            stats.delta_program_physical_cohorts += 1;
            stats.delta_program_physical_rows += row_count;
            stats.delta_program_physical_granted_work += granted_work;
            stats.max_delta_program_physical_cohort =
                stats.max_delta_program_physical_cohort.max(row_count);
            stats.max_delta_program_physical_granted_work = stats
                .max_delta_program_physical_granted_work
                .max(granted_work);
        }

        // Source/transition naming remains family-reported telemetry; it is
        // never consulted for dispatch, novelty, or replacement semantics.
        stats.delta_source_pages += receipt.source_pages;
        stats.delta_source_candidates_examined += receipt.source_examined;
        stats.delta_source_roots += receipt.source_roots;
        if !private_direct {
            stats.delta_source_direct_candidates += receipt.direct.len();
        }
        if receipt.source_pages > 0 {
            stats.delta_source_cohorts += 1;
            stats.max_delta_source_cohort = stats.max_delta_source_cohort.max(receipt.source_pages);
        }
        stats.delta_transition_pages += receipt.transition_pages;
        stats.delta_transition_candidates_examined += receipt.transition_examined;
        if receipt.transition_pages > 0 {
            stats.delta_transition_cohorts += 1;
            stats.max_delta_transition_cohort = stats
                .max_delta_transition_cohort
                .max(receipt.transition_pages);
        }

        // Physical pacing is revalidated by the typed adapter from canonical
        // state before this receipt is produced. Family-reported source and
        // transition counts remain telemetry only.
        let search_cohort = cohort_key.class.pacing() == ProgramPacing::Search;
        let source_telemetry_cohort = receipt.source_pages > 0 && receipt.transition_pages == 0;
        let mut scheduled = Vec::new();
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut retargeted = AHashMap::new();
        let mut retired_activations = Vec::new();
        let mut dead_pages = 0usize;
        let mut source_dead_pages = 0usize;
        let mut transition_dead_pages = 0usize;
        let mut retired_search_receipts = 0usize;
        let mut completed_activations = 0usize;
        let mut terminal_publications = OrderedActivationSet::default();

        for (
            input,
            (
                ((((
                    (activation, credit, input_dispatch, input_pacing, unique_unjoined),
                    page,
                ), child_range), direct_range), accepted_range),
                supported_range,
            ),
        ) in task_receipts
            .into_iter()
            .zip(receipt.pages)
            .zip(child_ranges)
            .zip(direct_ranges)
            .zip(accepted_ranges)
            .zip(supported_ranges)
            .enumerate()
        {
            let terminal = self.registry.physical_activation_class(activation)
                == DeltaPhysicalClass::TerminalStreaming;
            let within_search_page = self.registry.program_credit_within_search_page(&credit);
            assert!(
                page.examined > 0 || (page.resume.is_none() && child_range.is_empty()),
                "typed program scheduled zero-examined continuation work without a positive work receipt"
            );
            assert!(
                supported_range.len() <= 1,
                "one typed input page reported Boolean support more than once"
            );
            let page_had_program_effect = !child_range.is_empty()
                || (!private_direct && !direct_range.is_empty())
                || !accepted_range.is_empty()
                || !supported_range.is_empty();
            let single_child_no_barrier = child_range.len() == 1 && page.resume.is_none();
            stats.delta_program_single_child_no_barrier +=
                usize::from(single_child_no_barrier);
            if single_child_no_barrier {
                let child = &receipt.children[child_range.clone()][0];
                let compatible = child.work.dispatch == input_dispatch
                    && child.work.pacing == input_pacing;
                stats.delta_program_affine_tail_opportunities +=
                    usize::from(unique_unjoined && compatible);
            }
            let outcome = self.registry.replace_program(
                credit,
                state,
                &receipt.children[child_range],
                receipt.accepted[accepted_range]
                    .iter()
                    .map(|(_, value)| *value),
                receipt.direct[direct_range].iter().map(|(_, value)| *value),
                !supported_range.is_empty(),
                search_cohort,
                source_telemetry_cohort,
                page.resume,
            );
            if outcome.raw_proposal_occurrences != 0 {
                assert!(
                    outcome.raw_proposal_occurrences >= outcome.accepted.len(),
                    "typed SET admission manufactured proposal occurrences"
                );
                stats.candidates_proposed +=
                    outcome.raw_proposal_occurrences - outcome.accepted.len();
                stats.max_propose_candidates = stats
                    .max_propose_candidates
                    .max(outcome.raw_proposal_occurrences);
            }
            for (scheduled_state, work, credit) in outcome.scheduled {
                assert_eq!(
                    scheduled_state, state,
                    "typed program continuation crossed occurrence-local runtime state"
                );
                scheduled.push(ProgramTask {
                    activation,
                    credit,
                    work,
                });
            }

            let mut task_effects = DeltaStableEffects::default();
            if !supported_range.is_empty() {
                assert!(
                    outcome.accepted.is_empty(),
                    "one typed page mixed Boolean support with candidate acceptance"
                );
                if let Some(streamed) = self.registry.take_program_support_return(activation) {
                    let released = self.release_streaming(
                        activation,
                        streamed,
                        Vec::new(),
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    task_effects.absorb(released.stable);
                    if let Some(active) = released.active {
                        assert!(retargeted.insert(activation, active).is_none());
                    }
                }
            }
            if !outcome.accepted.is_empty() {
                let direct_terminal = direct_terminal_full.filter(|_| terminal);
                if let Some(streamed) = self.registry.take_streaming_return(activation) {
                    let released = self.release_streaming(
                        activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    task_effects.absorb(released.stable);
                    if let Some(active) = released.active {
                        assert!(retargeted.insert(activation, active).is_none());
                    }
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, activation);
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Retargeted(active) => {
                        assert_eq!(active.activation, activation);
                        assert!(retargeted.insert(activation, active).is_none());
                    }
                    DeltaSettlement::Completed(completed) => {
                        let old_activation = completed.activation;
                        let released = self.release_completion(
                            completed,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        );
                        prefer_continuation(
                            &mut task_effects.continuation,
                            released.continuation,
                        );
                        if let Some(active) = released.active {
                            assert!(retargeted.insert(old_activation, active).is_none());
                        } else if !retargeted.contains_key(&old_activation) {
                            completed_activations += 1;
                            completed_activation_ids.push(old_activation);
                        }
                    }
                }
                // Whether this proof removed the activation or transferred it
                // to the engine finalizer, the just-drained Program family's
                // activation-local arena is dead at this boundary.
                retired_activations.push(ProgramActivation(activation.0));
            }

            let page_dead = !page_had_program_effect && !task_effects.has_effect();
            if page_dead {
                // A child nested below an AfterChildren source receipt is
                // local work for that one source page. Preserve its exact
                // transition telemetry, but defer geometric feedback until
                // the receipt-local barrier knows whether any descendant
                // produced a stable effect.
                dead_pages += usize::from(!within_search_page);
                if source_telemetry_cohort {
                    source_dead_pages += 1;
                } else if !private_direct {
                    transition_dead_pages += 1;
                }
            }
            let retired_search_dead_pages = if task_effects.has_effect() {
                0
            } else {
                outcome.dead_search_pages
            };
            let retired_source_telemetry_dead_pages = if task_effects.has_effect() {
                0
            } else {
                outcome.dead_source_telemetry_pages
            };
            dead_pages += retired_search_dead_pages;
            retired_search_receipts += retired_search_dead_pages;
            source_dead_pages += retired_source_telemetry_dead_pages;
            if terminal && task_effects.has_effect() {
                let _ = terminal_publications.insert(activation);
            }
            effects.absorb(task_effects);
            debug_assert!(input < row_count);
        }

        if !scheduled.is_empty() {
            stats.delta_program_continuation_files += 1;
            stats.delta_program_continuation_tasks_filed += scheduled.len();
            stats.delta_program_continuation_reentries +=
                usize::from(!self.program_worklist.contains_key(&state));
        }
        let _ = self.file_program_state(state, scheduled);
        if !retired_activations.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("typed program state lost its runtime during retirement"),
                address_key,
                &retired_activations,
            );
        }
        stats.delta_source_dead_pages += source_dead_pages;
        stats.delta_transition_dead_pages += transition_dead_pages;
        DeltaPhysicalOutcome {
            outcome: DeltaStepOutcome {
                continuation: effects.continuation,
                publication: effects.publication,
                completed_activation_ids,
                retargeted,
                dead_pages,
                source_dead_pages,
                transition_dead_pages,
                completed_activations,
                completed_transition_cohort: !search_cohort && completed_activations > 1,
                allows_global_width_growth: true,
            },
            terminal_publications,
            retired_search_receipt: retired_search_receipts > 0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn step_source<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        search_width: usize,
        direct_terminal_publication_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStepOutcome {
        let (desc, tasks, dispatch) = self.pop_source_bounded(search_width);
        let examined_before = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
        let physical = self.step_sources(
            root,
            plan,
            desc,
            tasks,
            &dispatch.task_limits,
            direct_terminal_publication_full,
            stable,
            stable_interner,
            stats,
        );
        let mut outcome = physical.outcome;
        outcome.allows_global_width_growth = self.account_physical_dispatch(
            dispatch,
            search_width,
            examined_before,
            &physical.terminal_publications,
            stats,
        );
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn step_sources<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        desc: DeltaDesc,
        tasks: Vec<SourceTask>,
        limits: &[usize],
        direct_terminal_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaPhysicalOutcome {
        assert!(!tasks.is_empty());
        assert_eq!(tasks.len(), limits.len());
        assert!(limits.iter().all(|&limit| limit > 0));
        let dispatch_key = SourceDispatchKey::of(&self.registry, &tasks[0]);
        assert!(
            tasks
                .iter()
                .all(|task| SourceDispatchKey::of(&self.registry, task) == dispatch_key),
            "one residual source cohort mixed incompatible physical dispatch shapes"
        );

        let row_count = tasks.len();

        let mut parents = Vec::new();
        let mut candidate_sets = Vec::with_capacity(row_count);
        for task in &tasks {
            assert_eq!(task.activation, task.credit.key.activation);
            let (bound, parent, candidates) = self.registry.source_context(task.activation);
            assert_eq!(bound, dispatch_key.bound);
            assert_eq!(candidates.is_some(), dispatch_key.has_candidates);
            parents.extend_from_slice(parent);
            candidate_sets.push(candidates);
        }
        let vars: Vec<VariableId> = dispatch_key.bound.into_iter().collect();
        let view = rows_view(&vars, &parents, row_count);
        let cursors: Vec<_> = tasks.iter().map(|task| task.cursor).collect();
        let batch = ResidualDeltaSourceBatch {
            view,
            candidate_sets: &candidate_sets,
            cursors: &cursors,
            limits,
        };
        let mut pages = Vec::with_capacity(row_count);
        let mut roots = Vec::new();
        let mut direct = Vec::new();
        assert!(
            desc.resolve(root, plan).residual_delta_source_pages(
                desc.variable,
                batch,
                &mut pages,
                &mut roots,
                &mut direct,
            ),
            "paged delta source became unsupported after seeding"
        );
        drop(candidate_sets);
        assert_eq!(pages.len(), row_count);
        let root_ranges = tagged_ranges(&roots, row_count, "root");
        let direct_ranges = tagged_ranges(&direct, row_count, "direct candidate");

        stats.delta_source_cohorts += 1;
        stats.max_delta_source_cohort = stats.max_delta_source_cohort.max(row_count);
        stats.delta_source_pages += row_count;
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut retargeted = AHashMap::new();
        let mut traversal = Vec::new();
        let mut resumed_sources = Vec::new();
        let mut dead_pages = 0usize;
        let mut completed_activations = 0usize;
        let mut terminal_publications = OrderedActivationSet::default();
        for (row, (((task, page), root_range), direct_range)) in tasks
            .into_iter()
            .zip(pages)
            .zip(root_ranges)
            .zip(direct_ranges)
            .enumerate()
        {
            let terminal = self.registry.physical_activation_class(task.activation)
                == DeltaPhysicalClass::TerminalStreaming;
            let row_roots = &roots[root_range];
            let row_direct = &direct[direct_range];
            assert!(page.examined <= limits[row]);
            assert!(row_roots.len() + row_direct.len() <= page.examined);
            validate_source_cursor(task.cursor, page.next);
            stats.delta_source_candidates_examined += page.examined;
            stats.delta_source_roots += row_roots.len();
            stats.delta_source_direct_candidates += row_direct.len();

            let outcome = self.registry.replace_source(
                task.credit,
                row_roots.iter().map(|(_, output)| *output),
                row_direct.iter().map(|(_, value)| *value),
                page.next,
            );
            if outcome.raw_proposal_occurrences != 0 {
                assert!(
                    outcome.raw_proposal_occurrences >= outcome.accepted.len(),
                    "source SET admission manufactured proposal occurrences"
                );
                stats.candidates_proposed +=
                    outcome.raw_proposal_occurrences - outcome.accepted.len();
                stats.max_propose_candidates = stats
                    .max_propose_candidates
                    .max(outcome.raw_proposal_occurrences);
            }
            for (node, credit) in outcome.roots {
                traversal.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node,
                    cursor: ResidualDeltaExpandCursor::Start,
                });
            }
            if let Some((cursor, credit)) = outcome.resumed_source {
                resumed_sources.push(SourceTask {
                    activation: task.activation,
                    credit,
                    cursor,
                });
            }
            let retired_source_page = outcome.retired_source_page;
            let mut task_effects = DeltaStableEffects::default();
            if !outcome.accepted.is_empty() {
                let direct_terminal = direct_terminal_full.filter(|_| {
                    self.registry.physical_activation_class(task.activation)
                        == DeltaPhysicalClass::TerminalStreaming
                });
                if let Some(streamed) = self.registry.take_streaming_return(task.activation) {
                    let released = self.release_streaming(
                        task.activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    task_effects.absorb(released.stable);
                    if let Some(active) = released.active {
                        assert!(retargeted.insert(task.activation, active).is_none());
                    }
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                match self.settle_quiescence(proof) {
                    DeltaSettlement::Retargeted(active) => {
                        assert_eq!(active.activation, task.activation);
                        assert!(retargeted.insert(task.activation, active).is_none());
                    }
                    DeltaSettlement::Completed(completed) => {
                        let old_activation = completed.activation;
                        let released = self.release_completion(
                            completed,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        );
                        prefer_continuation(
                            &mut task_effects.continuation,
                            released.continuation,
                        );
                        if let Some(active) = released.active {
                            assert!(retargeted.insert(old_activation, active).is_none());
                        } else if !retargeted.contains_key(&old_activation) {
                            completed_activations += 1;
                            completed_activation_ids.push(old_activation);
                        }
                    }
                }
            }
            if retired_source_page.is_some_and(|page| !page.had_stable_effect)
                && !task_effects.has_effect()
            {
                dead_pages += 1;
            }
            if terminal && task_effects.has_effect() {
                let _ = terminal_publications.insert(task.activation);
            }
            effects.absorb(task_effects);
        }
        let _ = self.file(desc.clone(), traversal);
        let _ = self.file_source(desc, resumed_sources);
        stats.delta_source_dead_pages += dead_pages;
        DeltaPhysicalOutcome {
            outcome: DeltaStepOutcome {
                continuation: effects.continuation,
                publication: effects.publication,
                completed_activation_ids,
                retargeted,
                dead_pages,
                source_dead_pages: dead_pages,
                transition_dead_pages: 0,
                completed_activations,
                completed_transition_cohort: false,
                allows_global_width_growth: true,
            },
            terminal_publications,
            retired_search_receipt: false,
        }
    }

    fn deep_clone(&self) -> Self {
        let (registry, mut remap) = self.registry.deep_clone();
        let mut worklist = BTreeMap::new();
        for (&id, bucket) in &self.worklist {
            let mut tasks = Vec::with_capacity(bucket.len());
            for task in bucket.iter() {
                let credit = remap
                    .remove(&task.credit.key)
                    .expect("delta clone omitted one live credit");
                tasks.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node: task.node,
                    cursor: task.cursor,
                });
            }
            let mut cloned_bucket = DeltaBucket::default();
            cloned_bucket.extend(tasks);
            worklist.insert(id, cloned_bucket);
        }
        let mut source_worklist = BTreeMap::new();
        for (&id, bucket) in &self.source_worklist {
            let mut tasks = Vec::with_capacity(bucket.tasks.len());
            for task in &bucket.tasks {
                let credit = remap
                    .remove(&task.credit.key)
                    .expect("delta clone omitted one live source credit");
                tasks.push(SourceTask {
                    activation: task.activation,
                    credit,
                    cursor: task.cursor,
                });
            }
            source_worklist.insert(id, SourceBucket { tasks });
        }
        let mut program_worklist = BTreeMap::new();
        for (&id, bucket) in &self.program_worklist {
            let mut tasks = Vec::with_capacity(bucket.tasks.len());
            for task in &bucket.tasks {
                let credit = remap
                    .remove(&task.credit.key)
                    .expect("delta clone omitted one live program credit");
                tasks.push(ProgramTask {
                    activation: task.activation,
                    credit,
                    work: task.work.clone(),
                });
            }
            program_worklist.insert(id, ProgramBucket { tasks });
        }
        assert!(
            remap.is_empty(),
            "delta registry held a live credit without a scheduled task"
        );
        Self {
            registry,
            interner: self.interner.clone(),
            worklist,
            source_worklist,
            program_worklist,
            program_runtimes: self.program_runtimes.clone(),
            activation_width: self.activation_width,
            terminal_selection_slots: AHashMap::new(),
            terminal_selections: Vec::new(),
        }
    }
}

impl Clone for DeltaScheduler {
    fn clone(&self) -> Self {
        self.deep_clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use rand::{rngs::StdRng, Rng, SeedableRng};

    use crate::query::{
        intersectionconstraint::IntersectionConstraint, unionconstraint::UnionConstraint,
    };

    use super::*;

    #[derive(Clone)]
    struct ZeroProgressState(u64);

    struct ZeroProgressProgram;

    impl TypedProgramSpec for ZeroProgressProgram {
        type State = ZeroProgressState;
        type NoveltyKey = u8;
        type Rank = u64;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Propose(0)).then_some(ProgramRoute {
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
            state.0
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            for parent in 0..batch.view.len() {
                effects.finite_root(parent as u32, ZeroProgressState(1), None);
            }
        }

        fn step_typed(
            &self,
            states: crate::query::TypedProgramStateBatch<Self::State>,
            _batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            for _ in states {
                effects.page(0, Some(TypedResume::Immediate(ZeroProgressState(0))));
            }
        }
    }

    impl Constraint<'static> for ZeroProgressProgram {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            panic!("typed program unexpectedly fell back to ordinary propose")
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_program(&self) -> Option<ProgramRef<'_>> {
            Some(ProgramRef::new(self))
        }
    }

    #[derive(Clone, Copy)]
    struct OneShotSupportState {
        keep_cleanup_live: bool,
    }

    #[derive(Clone, Copy)]
    struct OneShotSupportProgram;

    impl TypedProgramSpec for OneShotSupportProgram {
        type State = OneShotSupportState;
        type NoveltyKey = ();
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Support).then_some(ProgramRoute {
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

        fn pacing(&self, _state: &Self::State) -> ProgramPacing {
            ProgramPacing::Search
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            1 + u8::from(state.keep_cleanup_live)
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            panic!("test support Program is installed through the private runtime seam")
        }

        fn step_typed(
            &self,
            states: crate::query::TypedProgramStateBatch<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), batch.limits.len());
            for (input, state) in states.into_iter().enumerate() {
                effects.support(u32::try_from(input).unwrap());
                effects.page(
                    1,
                    state.keep_cleanup_live.then_some(TypedResume::Immediate(
                        OneShotSupportState {
                            keep_cleanup_live: false,
                        },
                    )),
                );
            }
        }
    }

    impl Constraint<'static> for OneShotSupportProgram {
        fn variables(&self) -> VariableSet {
            VariableSet::new_empty()
        }

        fn estimate(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _out: &mut EstimateSink<'_>,
        ) -> bool {
            false
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_program(&self) -> Option<ProgramRef<'_>> {
            Some(ProgramRef::new(self))
        }
    }

    #[test]
    fn scheduler_rejects_zero_examined_program_recurrence() {
        let mut query = Query::new(ZeroProgressProgram, |binding: &crate::query::Binding| {
            binding.get(0).copied()
        })
        .solve_residual_state_lazy_with(ResidualLowering::FULL);
        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| query.next()));
        let payload = rejected.expect_err("zero-cost recurrence must fail closed");
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("zero-examined continuation work"),
            "unexpected panic: {message}"
        );
    }

    #[derive(Clone, Copy)]
    struct MixedExpansion;

    impl Constraint<'static> for MixedExpansion {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_expand(
            &self,
            _variable: VariableId,
            nodes: &[ResidualDeltaNode],
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) -> bool {
            for (tag, node) in nodes.iter().enumerate() {
                if node.value == value(2) {
                    successors.push((
                        u32::try_from(tag).unwrap(),
                        output(3, node.continuation + 1, true),
                    ));
                }
            }
            true
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TransitionBatchTrace {
        nodes: Vec<ResidualDeltaNode>,
        cursors: Vec<ResidualDeltaExpandCursor>,
        limits: Vec<usize>,
    }

    struct BatchedPagedExpansion {
        trace: Arc<Mutex<Option<TransitionBatchTrace>>>,
        scalar_page_calls: Arc<AtomicUsize>,
        eager_nodes: Arc<AtomicUsize>,
    }

    impl Constraint<'static> for BatchedPagedExpansion {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_expand_page(
            &self,
            _variable: VariableId,
            _node: ResidualDeltaNode,
            _cursor: ResidualDeltaExpandCursor,
            _limit: usize,
            _successors: &mut Vec<ResidualDeltaOutput>,
        ) -> Option<ResidualDeltaExpandPage> {
            self.scalar_page_calls.fetch_add(1, Ordering::Relaxed);
            panic!("native transition cohort was scalarized")
        }

        fn residual_delta_expand_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaExpandBatch<'_>,
            pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) {
            assert_eq!(variable, 0);
            assert!(pages.is_empty());
            assert!(successors.is_empty());
            *self.trace.lock().expect("transition batch trace poisoned") =
                Some(TransitionBatchTrace {
                    nodes: batch.nodes.to_vec(),
                    cursors: batch.cursors.to_vec(),
                    limits: batch.limits.to_vec(),
                });
            for (row, node) in batch.nodes.iter().enumerate() {
                if node.value == value(2) {
                    pages.push(None);
                    continue;
                }
                assert_eq!(batch.cursors[row], ResidualDeltaExpandCursor::Start);
                assert_eq!(batch.limits[row], 1);
                pages.push(Some(ResidualDeltaExpandPage {
                    next: None,
                    examined: 1,
                }));
                successors.push((
                    u32::try_from(row).expect("too many native transition rows"),
                    output(node.value[0] + 10, node.continuation + 1, false),
                ));
            }
        }

        fn residual_delta_expand(
            &self,
            _variable: VariableId,
            nodes: &[ResidualDeltaNode],
            successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) -> bool {
            self.eager_nodes.fetch_add(nodes.len(), Ordering::Relaxed);
            for (tag, node) in nodes.iter().enumerate() {
                assert_eq!(node.value, value(2));
                successors.push((
                    u32::try_from(tag).expect("too many eager transition rows"),
                    output(12, node.continuation + 1, false),
                ));
            }
            true
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ZeroColumnBatchTrace {
        row_count: usize,
        vars: Vec<VariableId>,
        candidate_modes: Vec<bool>,
        cursors: Vec<ResidualDeltaSourceCursor>,
        limits: Vec<usize>,
    }

    struct ZeroColumnSource {
        trace: Arc<Mutex<Option<ZeroColumnBatchTrace>>>,
    }

    impl Constraint<'static> for ZeroColumnSource {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_source_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaSourceBatch<'_>,
            pages: &mut Vec<ResidualDeltaSourcePage>,
            roots: &mut Vec<(u32, ResidualDeltaOutput)>,
            accepted: &mut Vec<(u32, RawInline)>,
        ) -> bool {
            assert_eq!(variable, 0);
            assert!(pages.is_empty());
            assert!(roots.is_empty());
            assert!(accepted.is_empty());
            *self.trace.lock().expect("zero-column trace poisoned") = Some(ZeroColumnBatchTrace {
                row_count: batch.view.len(),
                vars: batch.view.vars.to_vec(),
                candidate_modes: batch
                    .candidate_sets
                    .iter()
                    .map(|candidates| candidates.is_some())
                    .collect(),
                cursors: batch.cursors.to_vec(),
                limits: batch.limits.to_vec(),
            });
            pages.extend((0..batch.view.len()).map(|_| ResidualDeltaSourcePage {
                next: None,
                examined: 0,
            }));
            true
        }
    }

    #[derive(Clone, Copy)]
    struct DirectTerminalSource;

    impl Constraint<'static> for DirectTerminalSource {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_source_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaSourceBatch<'_>,
            pages: &mut Vec<ResidualDeltaSourcePage>,
            roots: &mut Vec<(u32, ResidualDeltaOutput)>,
            accepted: &mut Vec<(u32, RawInline)>,
        ) -> bool {
            assert_eq!(variable, 0);
            assert!(pages.is_empty());
            assert!(roots.is_empty());
            assert!(accepted.is_empty());
            for row in 0..batch.view.len() {
                assert!(batch.limits[row] >= 1);
                pages.push(ResidualDeltaSourcePage {
                    next: None,
                    examined: 1,
                });
                accepted.push((
                    u32::try_from(row).expect("too many direct terminal rows"),
                    value(10 + u8::try_from(row).expect("too many direct terminal rows")),
                ));
            }
            true
        }
    }

    #[derive(Clone, Copy)]
    struct SourceTransitionCycle;

    impl Constraint<'static> for SourceTransitionCycle {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
        }

        fn residual_delta_source_pages(
            &self,
            variable: VariableId,
            batch: ResidualDeltaSourceBatch<'_>,
            pages: &mut Vec<ResidualDeltaSourcePage>,
            roots: &mut Vec<(u32, ResidualDeltaOutput)>,
            accepted: &mut Vec<(u32, RawInline)>,
        ) -> bool {
            assert_eq!(variable, 0);
            assert!(pages.is_empty());
            assert!(roots.is_empty());
            assert!(accepted.is_empty());
            for (row, cursor) in batch.cursors.iter().copied().enumerate() {
                let row = u32::try_from(row).expect("too many cycle rows");
                match cursor {
                    ResidualDeltaSourceCursor::Start => {
                        roots.push((row, output(1, 0, false)));
                        pages.push(ResidualDeltaSourcePage {
                            next: Some(ResidualDeltaSourceCursor::Offset(1)),
                            examined: 1,
                        });
                    }
                    ResidualDeltaSourceCursor::Offset(1) => {
                        pages.push(ResidualDeltaSourcePage {
                            next: None,
                            examined: 0,
                        });
                    }
                    _ => panic!("cycle source received an unexpected cursor"),
                }
            }
            true
        }

        fn residual_delta_expand(
            &self,
            _variable: VariableId,
            _nodes: &[ResidualDeltaNode],
            _successors: &mut Vec<(u32, ResidualDeltaOutput)>,
        ) -> bool {
            true
        }
    }

    fn value(byte: u8) -> RawInline {
        [byte; 32]
    }

    fn output(byte: u8, continuation: u32, accepted: bool) -> ResidualDeltaOutput {
        ResidualDeltaOutput {
            node: ResidualDeltaNode {
                source: None,
                value: value(byte),
                continuation,
            },
            accepted,
        }
    }

    fn finish_registry_proposal(
        registry: &mut ProducerRegistry,
        proof: QuiescenceProof,
    ) -> CompletedActivation {
        let RegistrySettlement::ProposalMaterializer(seed) =
            registry.settle_quiescence(proof)
        else {
            panic!("nonempty proposal did not open its materializer")
        };
        let mut state = seed.state;
        let mut emitted = Vec::new();
        loop {
            let page = state.advance(1);
            emitted.extend(page.emitted);
            let Some(next) = page.next else {
                break;
            };
            state = next;
        }
        let retired = registry.replace_program(
            seed.credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            emitted,
            false,
            true,
            false,
            None,
        );
        let proof = retired
            .quiescence
            .expect("proposal materializer retired its sole credit");
        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(proof) else {
            panic!("completed proposal materializer reopened engine work")
        };
        completed
    }

    fn sourced_output(
        source: u8,
        current: u8,
        continuation: u32,
        accepted: bool,
    ) -> ResidualDeltaOutput {
        ResidualDeltaOutput {
            node: ResidualDeltaNode {
                source: Some(value(source)),
                value: value(current),
                continuation,
            },
            accepted,
        }
    }

    fn stable_return(parent: Vec<RawInline>) -> DeltaReturn {
        DeltaReturn::Stable {
            desc: StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Ready,
            },
            parent: parent.into_boxed_slice(),
            set_admit_result: false,
        }
    }

    fn candidate_return(parent: Vec<RawInline>) -> DeltaReturn {
        let relevant = ChildSet::empty(1).with_inserted(0);
        DeltaReturn::Stable {
            desc: StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Candidate {
                    variable: 0,
                    relevant: relevant.clone(),
                    checked: relevant,
                },
            },
            parent: parent.into_boxed_slice(),
            set_admit_result: false,
        }
    }

    fn formula_or_reducer_batch(values: &[u8]) -> FormulaBatch {
        let mut batch = FormulaBatch::from_proposal(
            RowBatch::seed(),
            vec![super::super::ActivationId(11)],
            &FiniteFormulaNodeKind::Or {
                children: Box::new([]),
            },
        );
        for &candidate in values {
            batch.admit_current_or_value(0, value(candidate));
        }
        batch
    }

    #[test]
    fn formula_or_admission_is_pageable_duplicate_safe_and_clone_independent() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let seeded = scheduler.seed_formula_reducers(
            vec![FormulaReducerSeed::Admit(FormulaOrAdmissionSeed {
                bound: VariableSet::new_empty(),
                batch: formula_or_reducer_batch(&[]),
                input: CandidatePayload::Values(vec![
                    value(2),
                    value(2),
                    value(1),
                    value(3),
                ]),
                // The test deliberately stops before EOF; this exact saved
                // PC must remain opaque to every intermediate page.
                continuation: FormulaReducerContinuation::Complete(FormulaPcId(u32::MAX)),
            })],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let active = seeded.active.expect("nonempty admission opened one Program");
        assert_eq!(seeded.seeded_parents, 1);
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::FormulaOrAdmit))
        );

        let admitted = |scheduler: &DeltaScheduler| {
            let activation = scheduler
                .registry
                .state
                .activations
                .get(&active.activation)
                .expect("live Formula admission activation");
            let DeltaReturn::FormulaOrAdmit { batch, .. } = &activation.return_to else {
                panic!("Formula admission lost its accumulator payload")
            };
            batch.current_or_set().iter().copied().collect::<Vec<_>>()
        };

        let first = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(first.status, ActiveDeltaStatus::Pending);
        assert_eq!(first.outcome.dead_pages, 1);
        assert_eq!(admitted(&scheduler), [value(2)]);
        let resume = first.resume.unwrap();
        let cloned = scheduler.clone();

        let duplicate = scheduler.step_active(
            &root,
            &plan,
            resume,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(duplicate.status, ActiveDeltaStatus::Pending);
        assert_eq!(duplicate.outcome.dead_pages, 1);
        assert_eq!(admitted(&scheduler), [value(2)]);
        assert_eq!(admitted(&cloned), [value(2)]);

        let third = scheduler.step_active(
            &root,
            &plan,
            duplicate.resume.unwrap(),
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(third.status, ActiveDeltaStatus::Pending);
        assert_eq!(admitted(&scheduler), [value(1), value(2)]);
        assert_eq!(admitted(&cloned), [value(2)]);
        assert!(stable.is_empty());
    }

    #[test]
    fn formula_or_emission_moves_grant_sized_ordered_pages_without_graph_telemetry() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let seeded = scheduler.seed_formula_reducers(
            vec![FormulaReducerSeed::Emit(FormulaOrEmissionSeed {
                bound: VariableSet::new_empty(),
                batch: formula_or_reducer_batch(&[7, 3, 1, 6, 2, 5, 4, 8]),
                // Seven values are emitted by the tested 1 -> 2 -> 4 pages,
                // so EOF never observes this deliberately opaque PC.
                counter: FormulaPcId(u32::MAX),
            })],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let active = seeded.active.expect("nonempty emission opened one Program");
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::FormulaOrEmit))
        );

        let output = |scheduler: &DeltaScheduler| {
            let activation = scheduler
                .registry
                .state
                .activations
                .get(&active.activation)
                .expect("live Formula emission activation");
            let DeltaReducer::FormulaOrEmit { output } = &activation.reducer else {
                panic!("Formula emission lost its output rope")
            };
            output.iter().collect::<Vec<_>>()
        };
        let output_root = |scheduler: &DeltaScheduler| {
            let activation = scheduler
                .registry
                .state
                .activations
                .get(&active.activation)
                .expect("live Formula emission activation");
            let DeltaReducer::FormulaOrEmit {
                output: CandidatePayload::Deferred(output),
            } = &activation.reducer
            else {
                panic!("Formula emission output was not deferred")
            };
            output.root.as_ref().unwrap().node.clone()
        };

        let first = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(first.status, ActiveDeltaStatus::Pending);
        assert_eq!(first.outcome.dead_pages, 1);
        assert!(first.outcome.allows_global_width_growth);
        assert_eq!(output(&scheduler), [(0, value(1))]);
        let cloned = scheduler.clone();
        assert!(Arc::ptr_eq(
            &output_root(&scheduler),
            &output_root(&cloned),
        ));

        let second = scheduler.step_active(
            &root,
            &plan,
            first.resume.unwrap(),
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(second.status, ActiveDeltaStatus::Pending);
        assert_eq!(second.outcome.dead_pages, 1);
        assert!(second.outcome.allows_global_width_growth);
        assert_eq!(
            output(&scheduler),
            [(0, value(1)), (0, value(2)), (0, value(3))]
        );
        assert_eq!(output(&cloned), [(0, value(1))]);

        let third = scheduler.step_active(
            &root,
            &plan,
            second.resume.unwrap(),
            4,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(third.status, ActiveDeltaStatus::Pending);
        assert_eq!(third.outcome.dead_pages, 1);
        assert!(third.outcome.allows_global_width_growth);
        assert_eq!(
            output(&scheduler),
            (1..=7).map(|byte| (0, value(byte))).collect::<Vec<_>>()
        );
        assert_eq!(output(&cloned), [(0, value(1))]);
        assert!(stable.is_empty());
        assert_eq!(stats.delta_source_direct_candidates, 0);
        assert_eq!(stats.delta_source_candidates_examined, 0);
        assert_eq!(stats.delta_transition_candidates_examined, 0);
        assert_eq!(stats.delta_source_pages, 0);
        assert_eq!(stats.delta_transition_pages, 0);
        assert_eq!(stats.delta_program_physical_cohorts, 0);
    }

    #[test]
    fn proposal_materializer_drains_typed_and_direct_occurrences_without_graph_telemetry() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            None,
            None,
        );
        let work = |slot| ProgramWork {
            handle: ProgramWorkHandle::test(slot),
            dispatch: DispatchClass::new(0),
            pacing: ProgramPacing::Activation,
        };
        let mut installed = scheduler.registry.install_program_roots(
            activation,
            [ProgramSeedWork {
                parent: 0,
                work: work(0),
                accepted: Some(value(4)),
            }],
        );
        assert_eq!(installed.initial_accepted, [value(4)]);
        let (_, root_credit) = installed.roots.pop().expect("one typed proposal root");
        let first = scheduler.registry.replace_program(
            root_credit,
            DeltaStateId(0),
            &[ProgramChild {
                input: 0,
                work: work(1),
                accepted: Some(value(2)),
            }],
            [value(3), value(2)],
            [value(3), value(3), value(1)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.accepted.as_slice(), &[value(3), value(2)]);
        assert!(first.quiescence.is_none());
        let (_, _, child_credit) = first
            .scheduled
            .into_iter()
            .next()
            .expect("typed proposal child retained one affine credit");
        let last = scheduler.registry.replace_program(
            child_credit,
            DeltaStateId(0),
            &[],
            [value(4), value(5), value(5)],
            [value(2)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(last.accepted.as_slice(), &[value(5)]);
        let DeltaSettlement::Retargeted(mut active) = scheduler.settle_quiescence(
            last.quiescence
                .expect("the typed proposal graph proved quiescence"),
        ) else {
            panic!("nonempty proposal did not open its materializer")
        };
        assert_eq!(active.activation, activation);
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(
                EngineProgramKind::ProposalMaterialize,
            ))
        );

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let graph_telemetry = |stats: &ResidualStateStats| {
            [
                stats.delta_source_pages,
                stats.delta_source_cohorts,
                stats.delta_source_candidates_examined,
                stats.delta_source_roots,
                stats.delta_source_direct_candidates,
                stats.delta_source_dead_pages,
                stats.delta_transition_pages,
                stats.delta_transition_cohorts,
                stats.delta_transition_candidates_examined,
                stats.delta_transition_dead_pages,
            ]
        };
        let telemetry_before = graph_telemetry(&stats);
        let mut yielded = None;
        for _ in 0..256 {
            let stepped = scheduler.step_active(
                &root,
                &plan,
                active,
                1,
                &mut stable,
                &mut stable_interner,
                &mut stats,
            );
            assert_eq!(graph_telemetry(&stats), telemetry_before);
            match stepped.status {
                ActiveDeltaStatus::Pending => {
                    let resume = stepped
                        .resume
                        .expect("live materializer returned no directed continuation");
                    assert_eq!(resume.activation, activation);
                    assert_eq!(resume.state, active.state);
                    assert!(scheduler.registry.is_live(activation));
                    assert!(scheduler.has_active_program(resume));
                    active = resume;
                }
                ActiveDeltaStatus::Yielded => {
                    assert!(stepped.resume.is_none());
                    assert_eq!(stepped.outcome.completed_activation_ids, [activation]);
                    yielded = Some(stepped.outcome);
                    break;
                }
                ActiveDeltaStatus::Quiescent => {
                    panic!("nonempty materializer orphaned its candidate result")
                }
            }
        }
        assert!(yielded.is_some(), "unit-grant materializer failed to terminate");
        assert!(!scheduler.registry.is_live(activation));
        assert!(scheduler.is_empty());
        let batches: Vec<_> = stable.values().flat_map(|level| level.values()).collect();
        assert_eq!(batches.len(), 1);
        let StateBucket::Candidates(batch) = batches[0] else {
            panic!("proposal materializer returned the wrong stable payload")
        };
        assert!(matches!(&batch.candidates, CandidatePayload::Deferred(_)));
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            [1, 2, 2, 3, 3, 3, 4, 5]
                .map(value)
                .map(|value| (0, value))
        );
    }

    #[test]
    fn empty_quiescent_proposal_completes_without_engine_work() {
        let mut scheduler = DeltaScheduler::new();
        let started = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [],
        );
        let activation = started.activation;
        let DeltaSettlement::Completed(completed) = scheduler.settle_quiescence(
            started
                .quiescence
                .expect("empty proposal is synchronously quiescent"),
        ) else {
            panic!("empty proposal manufactured engine work")
        };
        assert_eq!(completed.activation, activation);
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
        assert!(!scheduler.registry.is_live(activation));
        assert!(scheduler.interner.entries.is_empty());
        assert!(scheduler.program_runtimes.is_empty());
        assert!(scheduler.is_empty());
    }

    #[test]
    fn proposal_materializer_eof_retargets_nested_formula_into_or_admission() {
        let root = UnionConstraint::new(vec![MixedExpansion]);
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let formula_root = plan
            .finite_formula
            .root(0)
            .expect("the union root has a formula program");
        let FiniteFormulaNodeKind::Or { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("the union root did not compile as OR")
        };
        assert_eq!(children.len(), 1);

        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let parent = stable_interner.start_formula(
            &plan.finite_formula,
            0,
            0,
            UnionVerb::Propose { relevant },
        );
        let action = stable_interner.formula_pcs.select_child_with(
            &plan.finite_formula,
            parent,
            0,
            FormulaReturnKind::Child,
            FormulaStage::Propose,
            true,
        );
        let batch = FormulaBatch::from_proposal(
            RowBatch::seed(),
            vec![super::super::ActivationId(11)],
            &plan.finite_formula.node(formula_root).kind,
        );
        let started = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                counter: action,
                batch,
            },
            [output(7, 0, true)],
        );
        let old_activation = started.activation;
        let (_, root_credit) = started.roots.into_iter().next().expect("one formula root");
        let retired = scheduler.registry.replace_traversal(root_credit, []);
        let DeltaSettlement::Retargeted(active) = scheduler.settle_quiescence(
            retired
                .quiescence
                .expect("the singleton formula proposal quiesced"),
        ) else {
            panic!("formula proposal did not open its materializer")
        };
        assert_eq!(active.activation, old_activation);
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(
                EngineProgramKind::ProposalMaterialize,
            ))
        );

        let sealed = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(sealed.status, ActiveDeltaStatus::Pending);
        assert_eq!(sealed.resume, Some(active));
        let emitted = scheduler.step_active(
            &root,
            &plan,
            sealed.resume.unwrap(),
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let fresh = emitted
            .resume
            .expect("materializer EOF lost the nested Formula reducer");
        assert_eq!(emitted.status, ActiveDeltaStatus::Pending);
        assert_ne!(fresh.activation, old_activation);
        assert_eq!(
            scheduler.interner.program(fresh.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::FormulaOrAdmit))
        );
        assert_eq!(emitted.outcome.retargeted.get(&old_activation), Some(&fresh));
        assert!(emitted.outcome.completed_activation_ids.is_empty());
        assert!(!scheduler.registry.is_live(old_activation));
        assert!(scheduler.registry.is_live(fresh.activation));
        assert!(scheduler.has_active_program(fresh));
        assert!(stable.is_empty());
    }

    #[test]
    fn empty_formula_or_reducers_drain_admission_and_emission_synchronously() {
        // A one-arm union retains a real OR frame but lets one empty admission
        // complete its child and immediately discover the empty root emission.
        // Neither zero-rank reducer may manufacture a sentinel Program task.
        let root = UnionConstraint::new(vec![MixedExpansion]);
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let formula_root = plan
            .finite_formula
            .root(0)
            .expect("the union root has a formula program");
        let FiniteFormulaNodeKind::Or { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("the union root did not compile as OR")
        };

        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let parent = stable_interner.start_formula(
            &plan.finite_formula,
            0,
            0,
            UnionVerb::Propose { relevant },
        );
        let action = stable_interner.formula_pcs.select_child_with(
            &plan.finite_formula,
            parent,
            0,
            FormulaReturnKind::Child,
            FormulaStage::Propose,
            true,
        );
        assert_eq!(children.len(), 1);

        let seeded = scheduler.seed_formula_reducers(
            vec![FormulaReducerSeed::Admit(FormulaOrAdmissionSeed {
                bound: VariableSet::new_empty(),
                batch: FormulaBatch::from_proposal(
                    RowBatch::seed(),
                    vec![super::super::ActivationId(11)],
                    &plan.finite_formula.node(formula_root).kind,
                ),
                input: CandidatePayload::Values(Vec::new()),
                continuation: FormulaReducerContinuation::Complete(action),
            })],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(seeded.seeded_parents, 1);
        assert!(seeded.active.is_none());
        assert!(seeded.continuation.is_none());
        assert!(seeded.completed_activation_ids.is_empty());
        assert!(scheduler.is_empty());
        assert!(scheduler.registry.state.activations.is_empty());
        assert!(stable.is_empty());
    }

    #[test]
    fn streaming_support_retargets_to_fresh_formula_or_admission_before_old_cleanup() {
        type AnyConstraint = Box<dyn Constraint<'static>>;

        let root = UnionConstraint::new(vec![
            Box::new(
                crate::query::intersectionconstraint::IntersectionConstraint::<AnyConstraint>::new(
                    Vec::new(),
                ),
            ) as AnyConstraint,
            Box::new(OneShotSupportProgram) as AnyConstraint,
        ]);
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let formula_root = plan
            .finite_formula
            .root(0)
            .expect("the union root has a formula program");
        let FiniteFormulaNodeKind::Or { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("the union root did not compile as OR")
        };
        assert_eq!(children.len(), 2);

        for keep_cleanup_live in [false, true] {
            let mut scheduler = DeltaScheduler::new();
            let mut stable = Worklist::new();
            let mut stable_interner = StateInterner::default();
            let mut stats = ResidualStateStats::default();
            let relevant = ChildSet::empty(plan.len()).with_inserted(0);
            let parent = stable_interner.start_formula(
                &plan.finite_formula,
                0,
                0,
                UnionVerb::Confirm {
                    relevant,
                    checked: ChildSet::empty(plan.len()),
                },
            );
            // Force the empty AND arm's Support action. A true witness selects
            // that arm, whose complete Confirm frame immediately contributes
            // the immutable root candidate to a fresh OR admission reducer.
            let support_action = stable_interner.formula_pcs.select_child_with(
                &plan.finite_formula,
                parent,
                0,
                FormulaReturnKind::Guard,
                FormulaStage::Support,
                true,
            );
            let batch = FormulaBatch::from_confirmation(
                CandidateBatch {
                    parents: RowBatch::seed(),
                    candidates: CandidatePayload::Values(vec![value(7)]),
                },
                vec![super::super::ActivationId(11)],
                &plan.finite_formula.node(formula_root).kind,
            );

            let request = ProgramRequest {
                action: ProgramAction::Support,
                bound: VariableSet::new_empty(),
            };
            // This is deliberately a white-box lineage fixture: the Program
            // address supplies one finite Support receipt, while the affine
            // return payload names the empty composite arm above. Production
            // descendants use the same address/return separation; the
            // scheduler must not recover semantic control from the address.
            let desc = DeltaDesc::formula(0, 0, children[1]);
            let spec = plan
                .resolve_formula_node(&root, 0, children[1])
                .residual_program()
                .expect("the support arm exposes its typed Program");
            let route = spec
                .route(request)
                .expect("the support arm accepts a Support route");
            let state = scheduler.prepare_program(desc, route, spec);
            let old_activation = scheduler.registry.open_program_activation(
                DeltaReducer::Support { published: false },
                DeltaReturn::Formula {
                    bound: VariableSet::new_empty(),
                    counter: support_action,
                    batch,
                },
                None,
                None,
            );
            let credit = scheduler
                .registry
                .issue_credit(old_activation, CreditKind::Program { join: None });
            let work = insert_engine_program_state(
                &OneShotSupportProgram,
                scheduler
                    .program_runtimes
                    .get_mut(&state)
                    .expect("prepared support Program lost its runtime"),
                ProgramActivation(old_activation.0),
                OneShotSupportState { keep_cleanup_live },
            );
            let active = scheduler
                .file_program_state(
                    state,
                    vec![ProgramTask {
                        activation: old_activation,
                        credit,
                        work,
                    }],
                )
                .expect("the support Program filed one affine task");

            let stepped = scheduler.step_active(
                &root,
                &plan,
                active,
                1,
                &mut stable,
                &mut stable_interner,
                &mut stats,
            );
            let fresh = stepped
                .resume
                .expect("the support witness returned its fresh reducer lineage");
            assert_eq!(stepped.status, ActiveDeltaStatus::Pending);
            assert_ne!(fresh.activation, old_activation);
            assert_eq!(
                scheduler.interner.program(fresh.state),
                Some(&ProgramAddress::Engine(EngineProgramKind::FormulaOrAdmit))
            );
            assert_eq!(
                stepped.outcome.retargeted.get(&old_activation),
                Some(&fresh)
            );
            assert!(stepped.outcome.completed_activation_ids.is_empty());
            assert_eq!(scheduler.registry.is_live(old_activation), keep_cleanup_live);
            assert_eq!(
                scheduler.has_active_program(ActiveDeltaContinuation {
                    state,
                    activation: old_activation,
                }),
                keep_cleanup_live
            );
            assert!(scheduler.registry.is_live(fresh.activation));
            assert!(scheduler.has_active_program(fresh));
            assert!(stable.is_empty());
        }
    }

    #[test]
    fn terminal_streaming_is_activation_payload_not_delta_state_identity() {
        let mut scheduler = DeltaScheduler::new();
        let full = VariableSet::new_singleton(0);
        let terminal = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false)],
            full,
        );
        let ordinary = scheduler.registry.start_many(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(2, 0, false)],
        );
        let wrong_full = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(3, 0, false)],
            VariableSet::new_empty(),
        );
        let wrong_reducer = scheduler.registry.start_many_terminal(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [output(4, 0, false)],
            full,
        );
        assert_eq!(
            scheduler
                .registry
                .physical_activation_class(terminal.activation),
            DeltaPhysicalClass::TerminalStreaming
        );
        assert_eq!(
            scheduler
                .registry
                .physical_activation_class(ordinary.activation),
            DeltaPhysicalClass::General
        );
        assert_eq!(
            scheduler
                .registry
                .physical_activation_class(wrong_full.activation),
            DeltaPhysicalClass::General
        );
        assert_eq!(
            scheduler
                .registry
                .physical_activation_class(wrong_reducer.activation),
            DeltaPhysicalClass::General
        );

        let desc = DeltaDesc::leaf(0, 0);
        let tasks = terminal
            .roots
            .into_iter()
            .map(|(node, credit)| DeltaTask {
                activation: terminal.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            })
            .chain(ordinary.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: ordinary.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }))
            .collect();
        let _ = scheduler.file(desc.clone(), tasks);
        assert_eq!(scheduler.interner.entries, [DeltaStateEntry::Legacy(desc)]);
        assert_eq!(scheduler.worklist.len(), 1);
    }

    #[test]
    fn eager_receipts_and_sparse_activations_share_a_nonreusing_namespace() {
        let mut registry = ProducerRegistry::new();

        let receipts = registry.reserve_terminal_receipts(3);
        assert_eq!(
            receipts,
            [
                ActivationId::test(0),
                ActivationId::test(1),
                ActivationId::test(2),
            ]
        );
        assert!(
            receipts
                .iter()
                .all(|receipt| !registry.state.activations.contains_key(receipt)),
            "eager receipts must not manufacture sparse registry state"
        );

        let sparse = registry.start_many(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            [output(9, 0, false)],
        );
        assert_eq!(sparse.activation, ActivationId::test(3));
        assert!(registry.state.activations.contains_key(&sparse.activation));
        assert!(receipts
            .iter()
            .all(|receipt| !registry.state.activations.contains_key(receipt)));

        assert_eq!(
            registry.reserve_terminal_receipts(1),
            [ActivationId::test(4)]
        );
    }

    #[test]
    fn terminal_source_search_and_transition_effort_are_independent() {
        let mut registry = ProducerRegistry::new();
        let full = VariableSet::new_singleton(0);
        let started = registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false)],
            full,
        );

        assert_eq!(registry.source_dispatch_width(started.activation, 64), 64);
        assert_eq!(
            registry.transition_dispatch_width(started.activation, 64),
            1
        );
        assert_eq!(
            registry.finish_dispatch(started.activation, 64, PhysicalDispatchKind::Source, false,),
            (false, false)
        );
        assert_eq!(
            registry.transition_dispatch_width(started.activation, 64),
            1
        );
        assert_eq!(
            registry.finish_dispatch(
                started.activation,
                64,
                PhysicalDispatchKind::Transition,
                false,
            ),
            (false, true)
        );
        assert_eq!(
            registry.transition_dispatch_width(started.activation, 64),
            2
        );
        assert_eq!(
            registry.finish_dispatch(
                started.activation,
                64,
                PhysicalDispatchKind::Transition,
                false,
            ),
            (false, true)
        );
        assert_eq!(registry.transition_dispatch_width(started.activation, 3), 3);
        assert_eq!(
            registry.finish_dispatch(started.activation, 64, PhysicalDispatchKind::Source, true,),
            (true, false)
        );
        assert_eq!(
            registry.transition_dispatch_width(started.activation, 64),
            1
        );
    }

    #[test]
    fn terminal_global_feedback_waits_for_live_local_saturation() {
        let mut scheduler = DeltaScheduler::new();
        let started = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false)],
            VariableSet::new_singleton(0),
        );
        let activation = started.activation;

        assert!(scheduler.allows_global_width_growth(
            &PhysicalDispatch::new(
                &scheduler.registry,
                PhysicalDispatchKind::Source,
                64,
                [activation],
                vec![1],
                0,
            ),
            64,
            &OrderedActivationSet::default(),
        ));
        for expected in [2, 4, 8, 16, 32] {
            let _ = scheduler.registry.finish_dispatch(
                activation,
                64,
                PhysicalDispatchKind::Transition,
                false,
            );
            assert_eq!(
                scheduler.registry.transition_dispatch_width(activation, 64),
                expected
            );
        }
        assert!(!scheduler.allows_global_width_growth(
            &PhysicalDispatch::new(
                &scheduler.registry,
                PhysicalDispatchKind::Transition,
                64,
                [activation],
                vec![32],
                0,
            ),
            64,
            &OrderedActivationSet::default(),
        ));
        let _ = scheduler.registry.finish_dispatch(
            activation,
            64,
            PhysicalDispatchKind::Transition,
            false,
        );
        let saturated = PhysicalDispatch::new(
            &scheduler.registry,
            PhysicalDispatchKind::Transition,
            64,
            [activation],
            vec![64],
            0,
        );
        assert!(scheduler.allows_global_width_growth(
            &saturated,
            64,
            &OrderedActivationSet::default()
        ));
        assert!(!scheduler.allows_global_width_growth(
            &saturated,
            64,
            &OrderedActivationSet::from(vec![activation])
        ));

        let (_, credit) = started.roots.into_iter().next().unwrap();
        let replaced = scheduler
            .registry
            .replace_traversal(credit, std::iter::empty());
        let proof = replaced.quiescence.expect("empty traversal quiesces");
        let _ = scheduler.registry.finish(proof);
        assert!(!scheduler.allows_global_width_growth(
            &saturated,
            64,
            &OrderedActivationSet::default()
        ));
    }

    #[test]
    fn terminal_transition_cohort_shares_one_budget_across_local_quanta() {
        let mut scheduler = DeltaScheduler::new();
        let full = VariableSet::new_singleton(0);
        let first = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false), output(2, 0, false)],
            full,
        );
        let second = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(3, 0, false), output(4, 0, false)],
            full,
        );
        assert_eq!(
            scheduler.registry.finish_dispatch(
                first.activation,
                8,
                PhysicalDispatchKind::Transition,
                false,
            ),
            (false, true)
        );
        for expected in [2, 4] {
            let _ = scheduler.registry.finish_dispatch(
                second.activation,
                8,
                PhysicalDispatchKind::Transition,
                false,
            );
            assert_eq!(
                scheduler
                    .registry
                    .transition_dispatch_width(second.activation, 8),
                expected
            );
        }

        let tasks = first
            .roots
            .into_iter()
            .map(|(node, credit)| DeltaTask {
                activation: first.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            })
            .chain(second.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: second.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }))
            .collect();
        let _ = scheduler.file(DeltaDesc::leaf(0, 0), tasks);

        let (_, tasks, dispatch) = scheduler.pop_bounded(5);
        assert_eq!(dispatch.work_budget(), 5);
        assert_eq!(dispatch.task_limits, [1, 2, 2]);
        assert_eq!(dispatch.remainder_tasks, 1);
        assert_eq!(dispatch.terminal_activations.len(), 2);
        assert_eq!(
            tasks.iter().map(|task| task.node.value).collect::<Vec<_>>(),
            [value(2), value(3), value(4)]
        );

        let mut activation_work = BTreeMap::new();
        for (task, limit) in tasks.iter().zip(&dispatch.task_limits) {
            *activation_work.entry(task.activation).or_insert(0usize) += limit;
        }
        assert_eq!(activation_work[&first.activation], 1);
        assert_eq!(activation_work[&second.activation], 4);
        assert!(activation_work.iter().all(|(&activation, &work)| {
            work <= scheduler.registry.transition_dispatch_width(activation, 5)
        }));
        assert_eq!(
            dispatch.terminal_budgets,
            [
                TerminalActivationBudget {
                    activation: first.activation,
                    assigned: 1,
                    quantum: 2,
                },
                TerminalActivationBudget {
                    activation: second.activation,
                    assigned: 4,
                    quantum: 4,
                },
            ]
        );
        let mut stats = ResidualStateStats::default();
        assert!(!scheduler.account_physical_dispatch(
            dispatch,
            5,
            0,
            &OrderedActivationSet::default(),
            &mut stats,
        ));
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(first.activation, 5),
            2,
            "the truncated activation did not spend its complete local quantum"
        );
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(second.activation, 5),
            5
        );
        assert_eq!(stats.delta_terminal_sparse_widenings, 1);
    }

    #[test]
    fn terminal_cohort_feedback_is_activation_local() {
        let mut scheduler = DeltaScheduler::new();
        let full = VariableSet::new_singleton(0);
        let first = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false)],
            full,
        );
        let second = scheduler.registry.start_many_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(2, 0, false)],
            full,
        );
        for activation in [first.activation, second.activation] {
            let _ = scheduler.registry.finish_dispatch(
                activation,
                8,
                PhysicalDispatchKind::Transition,
                false,
            );
        }
        let mut stats = ResidualStateStats::default();
        let published = OrderedActivationSet::from(vec![first.activation]);
        assert!(!scheduler.account_physical_dispatch(
            PhysicalDispatch::new(
                &scheduler.registry,
                PhysicalDispatchKind::Transition,
                8,
                [first.activation, second.activation],
                vec![2, 2],
                0,
            ),
            8,
            0,
            &published,
            &mut stats,
        ));
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(first.activation, 8),
            1
        );
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(second.activation, 8),
            4
        );
        assert_eq!(stats.delta_terminal_publications, 1);
        assert_eq!(stats.delta_terminal_sparse_resets, 1);
        assert_eq!(stats.delta_terminal_sparse_widenings, 1);

        assert!(scheduler.account_physical_dispatch(
            PhysicalDispatch::new(
                &scheduler.registry,
                PhysicalDispatchKind::Source,
                8,
                [first.activation, second.activation],
                vec![4, 4],
                0,
            ),
            8,
            0,
            &OrderedActivationSet::default(),
            &mut stats,
        ));
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(first.activation, 8),
            1
        );
        assert_eq!(
            scheduler
                .registry
                .transition_dispatch_width(second.activation, 8),
            4
        );
        assert_eq!(stats.delta_terminal_sparse_widenings, 1);
    }

    #[test]
    fn terminal_sources_share_s_without_mixing_physical_classes() {
        let mut scheduler = DeltaScheduler::new();
        let full = VariableSet::new_singleton(0);
        let (first_activation, first_credit) = scheduler.registry.start_source_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            full,
        );
        let (ordinary_activation, ordinary_credit) = scheduler.registry.start_source(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
        );
        let (second_activation, second_credit) = scheduler.registry.start_source_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            full,
        );
        let desc = DeltaDesc::leaf(0, 0);
        let _ = scheduler.file_source(
            desc,
            vec![
                SourceTask {
                    activation: first_activation,
                    credit: first_credit,
                    cursor: ResidualDeltaSourceCursor::Start,
                },
                SourceTask {
                    activation: ordinary_activation,
                    credit: ordinary_credit,
                    cursor: ResidualDeltaSourceCursor::Start,
                },
                SourceTask {
                    activation: second_activation,
                    credit: second_credit,
                    cursor: ResidualDeltaSourceCursor::Start,
                },
            ],
        );

        let (_, terminal, dispatch) = scheduler.pop_source_bounded(8);
        assert_eq!(
            terminal
                .iter()
                .map(|task| task.activation)
                .collect::<Vec<_>>(),
            [second_activation, first_activation]
        );
        assert_eq!(dispatch.terminal_activations.len(), 2);
        assert_eq!(dispatch.task_limits, [4, 4]);
        assert_eq!(dispatch.work_budget(), 8);
        assert_eq!(dispatch.remainder_tasks, 1);
        assert!(terminal.iter().all(|task| {
            SourceDispatchKey::of(&scheduler.registry, task).physical_class
                == DeltaPhysicalClass::TerminalStreaming
        }));

        let (_, ordinary, dispatch) = scheduler.pop_source_bounded(8);
        assert_eq!(ordinary.len(), 1);
        assert_eq!(ordinary[0].activation, ordinary_activation);
        assert!(dispatch.terminal_activations.is_empty());
        assert_eq!(dispatch.task_limits, [8]);
    }

    #[test]
    fn terminal_source_cohort_preserves_origins_and_completion_receipts() {
        let root = DirectTerminalSource;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let full = VariableSet::new_singleton(0);
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let (first_activation, first_credit) = scheduler.registry.start_source_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            full,
        );
        let (second_activation, second_credit) = scheduler.registry.start_source_terminal(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            full,
        );
        let _ = scheduler.file_source(
            DeltaDesc::leaf(0, 0),
            vec![
                SourceTask {
                    activation: first_activation,
                    credit: first_credit,
                    cursor: ResidualDeltaSourceCursor::Start,
                },
                SourceTask {
                    activation: second_activation,
                    credit: second_credit,
                    cursor: ResidualDeltaSourceCursor::Start,
                },
            ],
        );

        let outcome = scheduler.step_bounded(
            &root,
            &plan,
            4,
            Some(full),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let publication = outcome
            .publication
            .expect("the terminal cohort publishes both affine rows directly");
        assert_eq!(publication.rows.row_count, 2);
        assert_eq!(publication.rows.rows, [value(10), value(11)]);
        assert_eq!(
            publication.origins.as_slice(),
            [second_activation, first_activation],
            "cohort batching must not collapse projected-yield origins"
        );
        assert_eq!(
            outcome.completed_activation_ids,
            [second_activation, first_activation],
            "completion remains an exact per-activation ledger receipt"
        );
        assert!(!outcome.completed_transition_cohort);
        assert!(stable.is_empty());
        assert_eq!(stats.delta_terminal_calls, 1);
        assert_eq!(stats.max_delta_terminal_task_cohort, 2);
    }

    #[test]
    fn proven_terminal_accepting_seed_publishes_rows_and_retains_its_credit() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let full = VariableSet::new_singleton(0);
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let seeded = scheduler.seed_proposals_with_full(
            DeltaDesc::leaf(0, 0),
            successor,
            RowBatch::seed(),
            vec![ResidualDeltaSeed {
                parent: 0,
                output: output(7, 0, true),
            }],
            full,
            Some(full),
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert!(seeded.continuation.is_none());
        let publication = seeded
            .publication
            .expect("the proven terminal accepting seed published directly");
        assert_eq!(
            (publication.rows.row_count, publication.rows.rows.as_slice(),),
            (1, &[value(7)][..])
        );
        assert_eq!(publication.origins.len(), 1);
        assert!(stable.is_empty());
        assert_eq!(
            (stats.candidates_proposed, stats.max_propose_candidates),
            (1, 1)
        );
        let active = seeded
            .active
            .expect("the accepted seed retained its independent traversal root");
        assert_eq!(
            scheduler
                .registry
                .physical_activation_class(active.activation),
            DeltaPhysicalClass::TerminalStreaming
        );
        assert!(scheduler.registry.is_live(active.activation));
    }

    fn streaming_formula_return(
        plan: &ResidualPlan,
        stable_interner: &mut StateInterner,
    ) -> DeltaReturn {
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let counter = stable_interner.start_formula(
            &plan.finite_formula,
            0,
            0,
            UnionVerb::Propose { relevant },
        );
        let root = plan
            .finite_formula
            .root(0)
            .expect("the synthetic root has a formula program");
        let FiniteFormulaNodeKind::And { children } = &plan.finite_formula.node(root).kind
        else {
            panic!("the streaming fixture requires a linear AND root")
        };
        assert_eq!(children.len(), 2);
        let counter = stable_interner
            .formula_pcs
            .skip_child(&plan.finite_formula, counter, 1);
        let counter =
            stable_interner
                .formula_pcs
                .select_child_as_action(&plan.finite_formula, counter, 0);
        assert_eq!(
            plan.interned_formula_proposal_streamability(
                &stable_interner.formula_pcs,
                counter,
                VariableSet::new_empty(),
            ),
            FormulaProposalStreamability::Linear,
            "the streaming fixture constructed an impossible production state"
        );
        DeltaReturn::Formula {
            bound: VariableSet::new_empty(),
            counter,
            batch: FormulaBatch::from_proposal(
                RowBatch {
                    rows: Vec::new(),
                    row_count: 1,
                },
                vec![super::super::ActivationId(11)],
                &plan.finite_formula.node(root).kind,
            ),
        }
    }

    fn support_formula_return() -> DeltaReturn {
        DeltaReturn::Formula {
            bound: VariableSet::new_empty(),
            counter: FormulaPcId(7),
            batch: FormulaBatch::from_proposal(
                RowBatch {
                    rows: Vec::new(),
                    row_count: 1,
                },
                vec![super::super::ActivationId(11)],
                &FiniteFormulaNodeKind::Or {
                    children: Box::new([]),
                },
            ),
        }
    }

    #[test]
    fn streaming_program_set_admission_is_activation_local_and_charges_raw_pages() {
        let work = |slot| ProgramWork {
            handle: ProgramWorkHandle::test(slot),
            dispatch: DispatchClass::new(0),
            pacing: ProgramPacing::Activation,
        };
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            None,
        );
        let installed = registry.install_program_roots(
            activation,
            [0, 1].map(|slot| ProgramSeedWork {
                parent: 0,
                work: work(slot),
                accepted: None,
            }),
        );
        let mut roots = installed.roots.into_iter();
        let (_, first_credit) = roots.next().unwrap();
        let (_, second_credit) = roots.next().unwrap();

        let first = registry.replace_program(
            first_credit,
            DeltaStateId(0),
            &[],
            [value(8), value(9), value(9)],
            [value(7), value(7), value(8)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.raw_proposal_occurrences, 6);
        assert_eq!(first.accepted.as_slice(), &[value(7), value(8), value(9)]);
        assert!(first.quiescence.is_none());

        let second = registry.replace_program(
            second_credit,
            DeltaStateId(0),
            &[],
            [value(7), value(11), value(11)],
            [value(8), value(10), value(10)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(second.raw_proposal_occurrences, 6);
        assert_eq!(second.accepted.as_slice(), &[value(10), value(11)]);
        assert!(second.quiescence.is_some());

        let sibling = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            None,
        );
        let (_, sibling_credit) = registry
            .install_program_roots(
                sibling,
                [ProgramSeedWork {
                    parent: 0,
                    work: work(2),
                    accepted: None,
                }],
            )
            .roots
            .pop()
            .unwrap();
        let sibling = registry.replace_program(
            sibling_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty::<RawInline>(),
            [value(7)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(sibling.raw_proposal_occurrences, 1);
        assert_eq!(sibling.accepted.as_slice(), &[value(7)]);
    }

    #[test]
    fn duplicate_typed_support_is_idempotent_across_sibling_receipts_and_clone() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            None,
            None,
        );
        let roots = [0, 1].map(|slot| ProgramSeedWork {
            parent: 0,
            work: ProgramWork {
                handle: ProgramWorkHandle::test(slot),
                dispatch: DispatchClass::new(0),
                pacing: ProgramPacing::Activation,
            },
            accepted: None,
        });
        let installed = registry.install_program_roots(activation, roots);
        assert_eq!(installed.roots.len(), 2);
        let mut roots = installed.roots.into_iter();
        let (_, first_credit) = roots.next().unwrap();
        let (_, second_credit) = roots.next().unwrap();

        let first = registry.replace_program(
            first_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            false,
            false,
            false,
            None,
        );
        assert!(first.quiescence.is_none());
        assert!(registry.take_program_support_return(activation).is_some());

        let second_key = second_credit.key;
        let (mut cloned, mut rebranded) = registry.deep_clone();
        let cloned_second = rebranded
            .remove(&second_key)
            .expect("deep clone omitted the live sibling credit");

        for (registry, credit) in [(&mut registry, second_credit), (&mut cloned, cloned_second)] {
            let second = registry.replace_program(
                credit,
                DeltaStateId(0),
                &[],
                std::iter::empty::<RawInline>(),
                std::iter::empty::<RawInline>(),
                false,
                false,
                false,
                None,
            );
            assert!(
                registry.take_program_support_return(activation).is_none(),
                "the cloned published reducer must suppress a later true witness"
            );
            let completed = registry.finish(second.quiescence.unwrap());
            assert_eq!(completed.effect, DeltaCompletion::Cleanup);
        }
    }

    #[test]
    fn after_children_resume_remains_credit_backed_and_preserves_sparse_quantum() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(VariableSet::new_singleton(0)),
        );
        assert_eq!(
            registry.finish_dispatch(activation, 8, PhysicalDispatchKind::Program, false,),
            (false, true)
        );
        assert_eq!(registry.transition_dispatch_width(activation, 8), 2);
        let work = |slot| ProgramWork {
            handle: ProgramWorkHandle::test(slot),
            dispatch: DispatchClass::new(0),
            pacing: ProgramPacing::Activation,
        };
        let (_, root) = registry
            .install_program_roots(
                activation,
                [ProgramSeedWork {
                    parent: 0,
                    work: work(0),
                    accepted: None,
                }],
            )
            .roots
            .pop()
            .unwrap();
        let parent = registry.replace_program(
            root,
            DeltaStateId(0),
            &[ProgramChild {
                input: 0,
                work: work(1),
                accepted: None,
            }],
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            false,
            false,
            false,
            Some(ProgramResume::AfterChildren(work(2))),
        );
        assert!(parent.quiescence.is_none());
        assert_eq!(parent.scheduled.len(), 1);
        assert_eq!(registry.transition_dispatch_width(activation, 8), 2);

        let (_, _, child) = parent.scheduled.into_iter().next().unwrap();
        let child = registry.replace_program(
            child,
            DeltaStateId(0),
            &[],
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            false,
            false,
            false,
            None,
        );
        assert!(child.quiescence.is_none());
        assert_eq!(child.scheduled.len(), 1);
        assert_eq!(registry.transition_dispatch_width(activation, 8), 2);

        assert_eq!(
            registry.finish_dispatch(activation, 8, PhysicalDispatchKind::Program, true,),
            (true, false),
            "only an activation-local publication resets the sparse grant"
        );
        assert_eq!(registry.transition_dispatch_width(activation, 8), 1);
        let (_, _, resume) = child.scheduled.into_iter().next().unwrap();
        let resume = registry.replace_program(
            resume,
            DeltaStateId(0),
            &[],
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            false,
            false,
            false,
            None,
        );
        let completed = registry.finish(
            resume
                .quiescence
                .expect("the delayed resume retained the final live credit"),
        );
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
    }

    #[test]
    fn search_after_children_dead_receipt_is_atomic_and_private_publication_suppresses_it() {
        let run = |publishes: bool| {
            let mut registry = ProducerRegistry::new();
            let activation = registry.open_program_activation(
                DeltaReducer::StreamProposal,
                stable_return(Vec::new()),
                None,
                None,
            );
            let work = |slot| ProgramWork {
                handle: ProgramWorkHandle::test(slot),
                dispatch: DispatchClass::new(0),
                pacing: ProgramPacing::Activation,
            };
            let (_, root) = registry
                .install_program_roots(
                    activation,
                    [ProgramSeedWork {
                        parent: 0,
                        work: work(0),
                        accepted: None,
                    }],
                )
                .roots
                .pop()
                .unwrap();
            let parent = registry.replace_program(
                root,
                DeltaStateId(0),
                &[ProgramChild {
                    input: 0,
                    work: work(1),
                    accepted: None,
                }],
                std::iter::empty::<RawInline>(),
                std::iter::empty::<RawInline>(),
                false,
                true,
                true,
                Some(ProgramResume::AfterChildrenDone),
            );
            assert_eq!(parent.dead_search_pages, 0);
            assert_eq!(parent.dead_source_telemetry_pages, 0);
            assert!(parent.quiescence.is_none());
            let (_, _, child) = parent.scheduled.into_iter().next().unwrap();
            let observed = publishes.then_some(value(7));
            let child = registry.replace_program(
                child,
                DeltaStateId(0),
                &[],
                observed,
                std::iter::empty::<RawInline>(),
                false,
                false,
                false,
                None,
            );
            assert_eq!(child.dead_search_pages, usize::from(!publishes));
            assert_eq!(child.dead_source_telemetry_pages, usize::from(!publishes));
            if publishes {
                assert_eq!(child.accepted.as_slice(), &[value(7)]);
                assert!(registry.take_streaming_return(activation).is_some());
            }
            let completed = registry.finish(
                child
                    .quiescence
                    .expect("the barrier verdict and quiescence are one receipt"),
            );
            assert_eq!(completed.effect, DeltaCompletion::Cleanup);
        };
        run(false);
        run(true);
    }

    #[test]
    fn program_source_context_borrows_large_candidate_sets_across_pages() {
        let mut registry = ProducerRegistry::new();
        let candidates: Vec<_> = (0..4096)
            .map(|ordinal| value((ordinal % 251) as u8))
            .collect();
        let original_ptr = candidates.as_ptr();
        let original = shared_one_parent_candidates(candidates);
        let activation = registry.open_program_activation(
            DeltaReducer::Confirm { original },
            stable_return(Vec::new()),
            None,
            None,
        );
        let (_, _, first) = registry.source_context(activation);
        let first = first.unwrap();
        let (_, _, second) = registry.source_context(activation);
        let second = second.unwrap();
        assert_eq!(first.len(), 4096);
        assert_eq!(first.as_ptr(), original_ptr);
        assert_eq!(first.as_ptr(), second.as_ptr());

        let formula_values: Vec<_> = (0..2048)
            .map(|ordinal| value((ordinal % 239) as u8))
            .collect();
        let formula_ptr = formula_values.as_ptr();
        let mut formula_batch = FormulaBatch::from_confirmation(
            CandidateBatch {
                parents: RowBatch::seed(),
                candidates: CandidatePayload::Values(formula_values),
            },
            vec![super::super::ActivationId(9)],
            &FiniteFormulaNodeKind::Atom,
        );
        let formula_original = formula_batch.shared_contiguous_confirm_original();
        let formula = registry.open_program_activation(
            DeltaReducer::Confirm {
                original: formula_original,
            },
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                counter: FormulaPcId(0),
                batch: formula_batch,
            },
            None,
            None,
        );
        let (_, _, formula_candidates) = registry.source_context(formula);
        let formula_candidates = formula_candidates.unwrap();
        assert_eq!(formula_candidates.len(), 2048);
        assert_eq!(formula_candidates.as_ptr(), formula_ptr);
    }

    #[test]
    fn singleton_program_lease_selects_one_lineage_inside_the_canonical_bucket() {
        let mut scheduler = DeltaScheduler::new();
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        };
        let state = scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 0), route));
        let first = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let second = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let install = |registry: &mut ProducerRegistry, activation, slot| {
            registry.install_program_roots(
                activation,
                [ProgramSeedWork {
                    parent: 0,
                    work: ProgramWork {
                        handle: ProgramWorkHandle::test(slot),
                        dispatch: DispatchClass::new(0),
                        pacing: ProgramPacing::Activation,
                    },
                    accepted: None,
                }],
            )
        };
        let first_root = install(&mut scheduler.registry, first, 0)
            .roots
            .pop()
            .unwrap();
        let active = scheduler
            .file_program_state(
                state,
                vec![ProgramTask {
                    activation: first,
                    work: first_root.0,
                    credit: first_root.1,
                }],
            )
            .unwrap();
        let second_root = install(&mut scheduler.registry, second, 1)
            .roots
            .pop()
            .unwrap();
        let _ = scheduler.file_program_state(
            state,
            vec![ProgramTask {
                activation: second,
                work: second_root.0,
                credit: second_root.1,
            }],
        );

        assert_eq!(scheduler.program_worklist.len(), 1);
        assert_eq!(scheduler.program_worklist[&state].tasks.len(), 2);
        let (popped_state, hot, dispatch) = scheduler.pop_active_program(active, 1);
        assert_eq!(popped_state, state);
        assert_eq!(dispatch.kind, PhysicalDispatchKind::Program);
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].activation, first);
        assert_eq!(scheduler.program_worklist[&state].tasks.len(), 1);
        assert_eq!(
            scheduler.program_worklist[&state].tasks[0].activation,
            second
        );
    }

    fn test_program_state(scheduler: &mut DeltaScheduler) -> DeltaStateId {
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        };
        scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 0), route))
    }

    fn install_program_tasks(
        registry: &mut ProducerRegistry,
        activation: ActivationId,
        slots: impl IntoIterator<Item = u32>,
        dispatch: DispatchClass,
        pacing: ProgramPacing,
    ) -> Vec<ProgramTask> {
        registry
            .install_program_roots(
                activation,
                slots.into_iter().map(|slot| ProgramSeedWork {
                    parent: 0,
                    work: ProgramWork {
                        handle: ProgramWorkHandle::test(slot),
                        dispatch,
                        pacing,
                    },
                    accepted: None,
                }),
            )
            .roots
            .into_iter()
            .map(|(work, credit)| ProgramTask {
                activation,
                work,
                credit,
            })
            .collect()
    }

    fn program_order_trace(
        pacing: ProgramPacing,
        active_pop: bool,
    ) -> (Vec<CreditNonce>, Vec<CreditNonce>, Vec<CreditNonce>) {
        let mut scheduler = DeltaScheduler::new();
        let state = test_program_state(&mut scheduler);
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let tasks = install_program_tasks(
            &mut scheduler.registry,
            activation,
            0..4,
            DispatchClass::new(0),
            pacing,
        );
        let storage_nonces: Vec<_> = tasks.iter().map(|task| task.credit.key.nonce).collect();
        let active = scheduler.file_program_state(state, tasks).unwrap();
        let (popped_state, selected, dispatch) = if active_pop {
            scheduler.pop_active_program(active, 3)
        } else {
            scheduler.pop_program_bounded(3)
        };
        assert_eq!(popped_state, state);
        assert_eq!(
            dispatch.kind,
            match pacing {
                ProgramPacing::Search => PhysicalDispatchKind::Source,
                ProgramPacing::Activation => PhysicalDispatchKind::Program,
            }
        );
        let selected_nonces = selected.iter().map(|task| task.credit.key.nonce).collect();
        let retained_nonces = scheduler.program_worklist[&state]
            .tasks
            .iter()
            .map(|task| task.credit.key.nonce)
            .collect();
        (storage_nonces, selected_nonces, retained_nonces)
    }

    #[test]
    fn active_program_order_is_lifo_for_search_and_append_for_activation() {
        for pacing in [ProgramPacing::Search, ProgramPacing::Activation] {
            let (storage, selected, retained) = program_order_trace(pacing, true);
            let expected = match pacing {
                ProgramPacing::Search => storage[1..].iter().copied().rev().collect(),
                ProgramPacing::Activation => storage[1..].to_vec(),
            };
            assert_eq!(selected, expected);
            assert_eq!(retained, storage[..1]);
        }
    }

    #[test]
    fn global_program_order_is_lifo_for_search_and_append_for_activation() {
        for pacing in [ProgramPacing::Search, ProgramPacing::Activation] {
            let (storage, selected, retained) = program_order_trace(pacing, false);
            let expected = match pacing {
                ProgramPacing::Search => storage[1..].iter().copied().rev().collect(),
                ProgramPacing::Activation => storage[1..].to_vec(),
            };
            assert_eq!(selected, expected);
            assert_eq!(retained, storage[..1]);
        }
    }

    #[test]
    fn global_search_program_cohorts_mix_reducers_without_an_activation_cap() {
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 1;
        let state = test_program_state(&mut scheduler);
        let streaming = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let quiescent = scheduler.registry.open_program_activation(
            DeltaReducer::quiescent_proposal(),
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut streaming_tasks = install_program_tasks(
            &mut scheduler.registry,
            streaming,
            0..2,
            DispatchClass::new(0),
            ProgramPacing::Search,
        )
        .into_iter();
        let mut quiescent_tasks = install_program_tasks(
            &mut scheduler.registry,
            quiescent,
            2..4,
            DispatchClass::new(0),
            ProgramPacing::Search,
        )
        .into_iter();
        let s0 = streaming_tasks.next().unwrap();
        let s1 = streaming_tasks.next().unwrap();
        let q0 = quiescent_tasks.next().unwrap();
        let q1 = quiescent_tasks.next().unwrap();
        let expected = [
            q1.credit.key.nonce,
            s1.credit.key.nonce,
            q0.credit.key.nonce,
            s0.credit.key.nonce,
        ];
        assert_eq!(
            ProgramCohortKey::of(&scheduler.registry, &s0),
            ProgramCohortKey::of(&scheduler.registry, &q0),
            "Search call compatibility must not encode reducer publication policy"
        );
        let _ = scheduler.file_program_state(state, vec![s0, q0, s1, q1]);

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(4);
        assert_eq!(popped_state, state);
        assert_eq!(dispatch.kind, PhysicalDispatchKind::Source);
        assert_eq!(dispatch.task_limits, [1, 1, 1, 1]);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(scheduler.program_worklist.is_empty());
    }

    #[test]
    fn global_streaming_activation_program_cohort_crosses_activations_in_append_order() {
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 1;
        let state = test_program_state(&mut scheduler);
        let first = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let second = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut first_tasks = install_program_tasks(
            &mut scheduler.registry,
            first,
            0..3,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut second_tasks = install_program_tasks(
            &mut scheduler.registry,
            second,
            3..5,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let a0 = first_tasks.next().unwrap();
        let a1 = first_tasks.next().unwrap();
        let a2 = first_tasks.next().unwrap();
        let b0 = second_tasks.next().unwrap();
        let b1 = second_tasks.next().unwrap();
        let retained = a0.credit.key.nonce;
        let expected = [
            b0.credit.key.nonce,
            a1.credit.key.nonce,
            b1.credit.key.nonce,
            a2.credit.key.nonce,
        ];
        assert_eq!(
            ProgramCohortKey::of(&scheduler.registry, &a0),
            ProgramCohortKey::of(&scheduler.registry, &b0),
            "streaming activation identity must remain task payload"
        );
        let _ = scheduler.file_program_state(state, vec![a0, b0, a1, b1, a2]);

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(4);
        assert_eq!(popped_state, state);
        assert_eq!(dispatch.kind, PhysicalDispatchKind::Program);
        assert_eq!(dispatch.task_limits, [1, 1, 1, 1]);
        assert!(dispatch.terminal_activations.is_empty());
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(scheduler.program_worklist[&state].tasks.len(), 1);
        assert_eq!(
            scheduler.program_worklist[&state].tasks[0]
                .credit
                .key
                .nonce,
            retained
        );
    }

    #[test]
    fn global_quiescent_program_cohort_uses_append_order_and_activation_cap() {
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 2;
        let state = test_program_state(&mut scheduler);
        let activations: Vec<_> = (0..4)
            .map(|_| {
                scheduler.registry.open_program_activation(
                    DeltaReducer::quiescent_proposal(),
                    stable_return(Vec::new()),
                    None,
                    None,
                )
            })
            .collect();
        let mut a = install_program_tasks(
            &mut scheduler.registry,
            activations[0],
            0..2,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut b = install_program_tasks(
            &mut scheduler.registry,
            activations[1],
            2..4,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut c = install_program_tasks(
            &mut scheduler.registry,
            activations[2],
            4..6,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut incompatible = install_program_tasks(
            &mut scheduler.registry,
            activations[3],
            6..7,
            DispatchClass::new(1),
            ProgramPacing::Activation,
        )
        .into_iter();
        let a0 = a.next().unwrap();
        let a1 = a.next().unwrap();
        let b0 = b.next().unwrap();
        let b1 = b.next().unwrap();
        let c0 = c.next().unwrap();
        let c1 = c.next().unwrap();
        let incompatible = incompatible.next().unwrap();
        let a_nonces = [a0.credit.key.nonce, a1.credit.key.nonce];
        let expected = [
            b0.credit.key.nonce,
            c0.credit.key.nonce,
            b1.credit.key.nonce,
            c1.credit.key.nonce,
        ];
        let incompatible_nonce = incompatible.credit.key.nonce;
        assert_eq!(
            ProgramCohortKey::of(&scheduler.registry, &a0),
            ProgramCohortKey::of(&scheduler.registry, &b0),
            "quiescent activation identity must remain task payload"
        );
        let _ = scheduler.file_program_state(state, vec![a0, b0, c0, a1, incompatible, b1, c1]);

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(8);
        assert_eq!(popped_state, state);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.activation)
                .collect::<BTreeSet<_>>(),
            activations[1..3].iter().copied().collect()
        );
        assert_eq!(dispatch.task_limits, [2, 2, 2, 2]);
        assert_eq!(dispatch.remainder_tasks, 3);
        let retained = &scheduler.program_worklist[&state].tasks;
        assert!(a_nonces
            .iter()
            .all(|nonce| retained.iter().any(|task| task.credit.key.nonce == *nonce)));
        assert!(retained
            .iter()
            .any(|task| task.credit.key.nonce == incompatible_nonce));
    }

    #[derive(Clone)]
    struct CoCompletionNovelty {
        parent: u32,
        drops: Arc<AtomicUsize>,
    }

    impl PartialEq for CoCompletionNovelty {
        fn eq(&self, other: &Self) -> bool {
            self.parent == other.parent
        }
    }

    impl Eq for CoCompletionNovelty {}

    impl std::hash::Hash for CoCompletionNovelty {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.parent.hash(state);
        }
    }

    impl Drop for CoCompletionNovelty {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Clone)]
    struct OneShotConfirmProgram {
        novelty_drops: Arc<AtomicUsize>,
        physical: bool,
    }

    impl OneShotConfirmProgram {
        fn fill_step(
            &self,
            states: &[u8],
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<u8, CoCompletionNovelty>,
        ) {
            assert_eq!(states.len(), batch.candidate_sets.len());
            for (input, (&state, candidates)) in states
                .iter()
                .zip(batch.candidate_sets.iter().copied())
                .enumerate()
            {
                assert_eq!(state, 1);
                let candidates = candidates.expect("Confirm activation lost its source set");
                assert_eq!(candidates.len(), 1);
                effects.page(1, None);
                effects.accept(input as u32, candidates[0]);
                effects.account_transition(1);
            }
        }
    }

    impl TypedProgramSpec for OneShotConfirmProgram {
        type State = u8;
        type NoveltyKey = CoCompletionNovelty;
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Confirm(0)).then_some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: ProgramStratum::Fixpoint,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, _state: &Self::State) -> DispatchClass {
            DispatchClass::new(0)
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            *state
        }

        fn seed_typed(
            &self,
            batch: ProgramSeedBatch<'_>,
            effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            for parent in 0..batch.view.len() {
                effects.fixpoint_root(
                    parent as u32,
                    1,
                    CoCompletionNovelty {
                        parent: parent as u32,
                        drops: Arc::clone(&self.novelty_drops),
                    },
                    None,
                );
            }
        }

        fn step_typed(
            &self,
            states: crate::query::TypedProgramStateBatch<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.fill_step(&states, batch, effects);
        }

        fn try_step_physical(
            &self,
            states: &[Self::State],
            batch: TypedProgramBatch<'_>,
        ) -> Option<TypedPhysicalStep<Self::State, Self::NoveltyKey>> {
            if !self.physical {
                return None;
            }
            let mut step = TypedPhysicalStep::new(ProgramPhysicalReceipt::new(
                "test-physical",
                "one-shot-confirm",
            ));
            self.fill_step(states, batch, step.effects_mut());
            Some(step)
        }
    }

    impl Constraint<'static> for OneShotConfirmProgram {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(0)
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != 0 {
                return false;
            }
            out.fill(1, view.len());
            true
        }

        fn propose(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            panic!("one-shot Confirm Program unexpectedly proposed")
        }

        fn confirm(
            &self,
            _variable: VariableId,
            _view: &RowsView<'_>,
            _candidates: &mut CandidateSink<'_>,
        ) {
            panic!("one-shot Confirm Program fell back to ordinary confirm")
        }

        fn residual_program(&self) -> Option<ProgramRef<'_>> {
            Some(ProgramRef::new(self))
        }
    }

    #[test]
    fn global_quiescent_program_co_transfers_then_finalizes_real_confirm_activations() {
        let novelty_drops = Arc::new(AtomicUsize::new(0));
        let root = OneShotConfirmProgram {
            novelty_drops: Arc::clone(&novelty_drops),
            physical: false,
        };
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let request = ProgramRequest {
            action: ProgramAction::Confirm(0),
            bound: VariableSet::new_empty(),
        };
        let spec = root.residual_program().unwrap();
        let route = spec.route(request).unwrap();
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 2;
        let active = scheduler
            .seed_program_confirms(
                spec,
                DeltaDesc::leaf(0, 0),
                request,
                route,
                successor,
                false,
                CandidateBatch {
                    parents: RowBatch {
                        rows: Vec::new(),
                        row_count: 2,
                    },
                    candidates: CandidatePayload::Tagged(vec![(0, value(7)), (1, value(8))]),
                },
            )
            .expect("both Confirm parents must seed one live Program root");
        let stored_tasks = &scheduler.program_worklist[&active.state].tasks;
        let mut activation_ids: Vec<_> = stored_tasks.iter().map(|task| task.activation).collect();
        activation_ids.sort_unstable();
        activation_ids.dedup();
        assert_eq!(
            activation_ids.len(),
            2,
            "the seed must open distinct activations"
        );
        let drops_before_step = novelty_drops.load(Ordering::Relaxed);

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let graph = scheduler.step_bounded(
            &root,
            &plan,
            8,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert!(graph.completed_activation_ids.is_empty());
        assert_eq!(graph.completed_activations, 0);
        assert!(!graph.completed_transition_cohort);
        assert!(graph.continuation.is_none());
        assert_eq!(graph.retargeted.len(), 2);
        assert!(activation_ids.iter().all(|activation| {
            graph
                .retargeted
                .get(activation)
                .is_some_and(|active| active.activation == *activation)
        }));
        let finalizer_states: BTreeSet<_> = graph
            .retargeted
            .values()
            .map(|active| active.state)
            .collect();
        assert_eq!(finalizer_states.len(), 1);
        let finalizer_state = *finalizer_states.iter().next().unwrap();
        assert_eq!(
            scheduler.interner.program(finalizer_state),
            Some(&ProgramAddress::Engine(EngineProgramKind::ConfirmFinalize))
        );
        assert!(activation_ids
            .iter()
            .all(|activation| scheduler.registry.is_live(*activation)));
        assert_eq!(
            novelty_drops.load(Ordering::Relaxed),
            drops_before_step + 2,
            "graph-family retirement must precede finalizer execution"
        );

        let outcome = scheduler.step_bounded(
            &root,
            &plan,
            8,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let mut completed_ids = outcome.completed_activation_ids.clone();
        completed_ids.sort_unstable();
        assert_eq!(completed_ids, activation_ids);
        assert_eq!(outcome.completed_activations, 2);
        assert!(!outcome.completed_transition_cohort);
        assert!(outcome.continuation.is_some());
        assert!(scheduler.program_worklist.is_empty());
        assert!(activation_ids
            .iter()
            .all(|activation| !scheduler.registry.is_live(*activation)));
        assert_eq!(
            novelty_drops.load(Ordering::Relaxed),
            drops_before_step + 2,
            "retiring the cohort must drop each activation-local novelty table"
        );

        let stable_batches: Vec<_> = stable.values().flat_map(|level| level.values()).collect();
        assert_eq!(stable_batches.len(), 1);
        let StateBucket::Candidates(batch) = stable_batches[0] else {
            panic!("Confirm completions returned the wrong stable payload")
        };
        assert_eq!(batch.parents.row_count, 2);
        assert_eq!(batch.candidate_count(), 2);
        let snapshot = batch.candidates.tagged_snapshot();
        assert_eq!(
            snapshot
                .iter()
                .map(|(parent, _)| *parent)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let mut returned_values: Vec<_> = snapshot.into_iter().map(|(_, value)| value).collect();
        returned_values.sort_unstable();
        assert_eq!(returned_values, [value(7), value(8)]);
        assert_eq!(stats.delta_transition_pages, 2);
        assert_eq!(stats.delta_transition_candidates_examined, 2);
        assert_eq!(stats.delta_transition_cohorts, 1);
        assert_eq!(stats.max_delta_transition_cohort, 2);
        assert_eq!(stats.delta_transition_dead_pages, 0);
        assert_eq!(stats.delta_program_physical_cohorts, 0);
        assert_eq!(stats.delta_program_physical_rows, 0);
        assert_eq!(stats.delta_program_physical_granted_work, 0);
        assert_eq!(stats.max_delta_program_physical_cohort, 0);
        assert_eq!(stats.max_delta_program_physical_granted_work, 0);
    }

    #[test]
    fn stable_confirm_finalizer_pages_occurrences_without_graph_telemetry() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let a = value(1);
        let b = value(2);
        let rejected = value(9);

        let mut original = shared_one_parent_candidates(vec![b, a]);
        original.extend_same_domain(
            shared_one_parent_candidates(vec![rejected, a, b]),
            1,
        );
        let mut scheduler = DeltaScheduler::new();
        let started = scheduler.registry.start_many(
            DeltaReducer::Confirm { original },
            candidate_return(Vec::new()),
            [output(1, 0, true), output(2, 0, true)],
        );
        let activation = started.activation;
        {
            let graph = scheduler
                .registry
                .state
                .activations
                .get_mut(&activation)
                .expect("live Confirm graph activation");
            graph.seen.insert(
                ResidualDeltaNode {
                    source: None,
                    value: value(42),
                    continuation: 7,
                },
                false,
            );
            graph.program_joins.reserve(8);
            graph.source_candidates = Some(vec![a, b, rejected].into_boxed_slice());
        }
        let mut proof = None;
        for (_, credit) in started.roots {
            if let Some(quiescence) = scheduler.registry.replace_traversal(credit, []).quiescence {
                assert!(proof.replace(quiescence).is_none());
            }
        }
        let DeltaSettlement::Retargeted(active) =
            scheduler.settle_quiescence(proof.expect("Confirm graph quiesced"))
        else {
            panic!("nonempty Confirm did not open its finalizer")
        };
        assert_eq!(active.activation, activation);
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::ConfirmFinalize))
        );
        {
            let finalizing = scheduler
                .registry
                .state
                .activations
                .get(&activation)
                .expect("Confirm finalizer retained its activation");
            assert_eq!(finalizing.status, ActivationStatus::Open);
            assert!(finalizing.seen.is_empty());
            assert_eq!(finalizing.seen.capacity(), 0);
            assert!(finalizing.program_joins.is_empty());
            assert_eq!(finalizing.program_joins.capacity(), 0);
            assert!(finalizing.source_candidates.is_none());
            assert!(finalizing.accepted.is_empty());
            assert_eq!(finalizing.accepted.capacity(), 0);
            assert_eq!(
                finalizing.live.values().copied().collect::<Vec<_>>(),
                [CreditKind::Program { join: None }]
            );
            assert!(matches!(
                &finalizing.reducer,
                DeltaReducer::FinalizingConfirm { .. }
            ));
        }

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let graph_telemetry = |stats: &ResidualStateStats| {
            [
                stats.delta_source_pages,
                stats.delta_source_cohorts,
                stats.delta_source_candidates_examined,
                stats.delta_source_roots,
                stats.delta_source_direct_candidates,
                stats.delta_source_dead_pages,
                stats.delta_transition_pages,
                stats.delta_transition_cohorts,
                stats.delta_transition_candidates_examined,
                stats.delta_transition_dead_pages,
            ]
        };
        let telemetry_before = graph_telemetry(&stats);

        let first = scheduler.step_active(
            &root,
            &plan,
            active,
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(first.status, ActiveDeltaStatus::Pending);
        assert_eq!(first.resume, Some(active));
        assert_eq!(first.outcome.dead_pages, 1);
        assert_eq!(first.outcome.source_dead_pages, 0);
        assert_eq!(first.outcome.transition_dead_pages, 0);
        assert!(first.outcome.continuation.is_none());
        assert!(first.outcome.publication.is_none());
        assert_eq!(graph_telemetry(&stats), telemetry_before);
        assert!(stable.is_empty());

        let resume = first.resume.unwrap();
        let mut cloned = scheduler.clone();
        let output_root = |scheduler: &DeltaScheduler| {
            let activation = scheduler
                .registry
                .state
                .activations
                .get(&active.activation)
                .expect("live cloned Confirm finalizer");
            let DeltaReducer::FinalizingConfirm {
                output: CandidatePayload::Deferred(output),
            } = &activation.reducer
            else {
                panic!("live Confirm clone lost its deferred reducer output")
            };
            output.root.as_ref().unwrap().node.clone()
        };
        assert!(Arc::ptr_eq(&output_root(&scheduler), &output_root(&cloned)));
        let original_brand = scheduler.program_worklist[&resume.state]
            .tasks
            .iter()
            .find(|task| task.activation == activation)
            .unwrap()
            .credit
            .brand;
        let cloned_brand = cloned.program_worklist[&resume.state]
            .tasks
            .iter()
            .find(|task| task.activation == activation)
            .unwrap()
            .credit
            .brand;
        assert_ne!(original_brand, cloned_brand);

        let second = scheduler.step_active(
            &root,
            &plan,
            resume,
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(second.status, ActiveDeltaStatus::Pending);
        assert_eq!(second.resume, Some(active));
        assert_eq!(second.outcome.dead_pages, 1);
        assert_eq!(second.outcome.source_dead_pages, 0);
        assert_eq!(second.outcome.transition_dead_pages, 0);
        assert!(second.outcome.continuation.is_none());
        assert_eq!(graph_telemetry(&stats), telemetry_before);
        assert!(stable.is_empty());

        let eof = scheduler.step_active(
            &root,
            &plan,
            second.resume.unwrap(),
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(eof.status, ActiveDeltaStatus::Yielded);
        assert!(eof.resume.is_none());
        assert_eq!(eof.outcome.completed_activation_ids, [activation]);
        assert_eq!(eof.outcome.completed_activations, 1);
        assert_eq!(eof.outcome.dead_pages, 0);
        assert!(eof.outcome.continuation.is_some());
        assert_eq!(graph_telemetry(&stats), telemetry_before);
        assert!(!scheduler.registry.is_live(activation));
        assert!(cloned.registry.is_live(activation));
        let cloned_partial_len = match &cloned
            .registry
            .state
            .activations
            .get(&activation)
            .unwrap()
            .reducer
        {
            DeltaReducer::FinalizingConfirm { output } => output.len(),
            _ => panic!("cloned finalizer changed reducer state"),
        };
        assert_eq!(
            cloned_partial_len, 2,
            "finishing the original mutated the clone's shared prefix"
        );

        let stable_batches: Vec<_> = stable.values().flat_map(|level| level.values()).collect();
        assert_eq!(stable_batches.len(), 1);
        let StateBucket::Candidates(batch) = stable_batches[0] else {
            panic!("Confirm finalizer returned the wrong stable payload")
        };
        let CandidatePayload::Deferred(deferred) = &batch.candidates else {
            panic!("Confirm finalizer materialized its pageable output at EOF")
        };
        assert!(matches!(
            deferred.root.as_ref().map(|root| &root.node.kind),
            Some(DeferredCandidateNodeKind::Concat { .. })
        ));
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            [(0, b), (0, a), (0, a), (0, b)]
        );

        let mut cloned_stable = Worklist::new();
        let mut cloned_interner = StateInterner::default();
        let mut cloned_stats = ResidualStateStats::default();
        let cloned_second = cloned.step_active(
            &root,
            &plan,
            resume,
            2,
            &mut cloned_stable,
            &mut cloned_interner,
            &mut cloned_stats,
        );
        assert_eq!(cloned_second.status, ActiveDeltaStatus::Pending);
        let cloned_eof = cloned.step_active(
            &root,
            &plan,
            cloned_second.resume.unwrap(),
            2,
            &mut cloned_stable,
            &mut cloned_interner,
            &mut cloned_stats,
        );
        assert_eq!(cloned_eof.status, ActiveDeltaStatus::Yielded);
        let cloned_batches: Vec<_> = cloned_stable
            .values()
            .flat_map(|level| level.values())
            .collect();
        assert_eq!(cloned_batches.len(), 1);
        let StateBucket::Candidates(cloned_batch) = cloned_batches[0] else {
            panic!("cloned finalizer returned the wrong stable payload")
        };
        assert_eq!(
            cloned_batch.candidates.iter().collect::<Vec<_>>(),
            [(0, b), (0, a), (0, a), (0, b)]
        );
    }

    #[test]
    fn confirm_finalizer_keeps_empty_original_eager_and_all_rejected_pageable() {
        let mut empty = DeltaScheduler::new();
        let started = empty.registry.start_many(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(Vec::new()),
            },
            candidate_return(Vec::new()),
            std::iter::empty::<ResidualDeltaOutput>(),
        );
        let DeltaSettlement::Completed(completed) =
            empty.settle_quiescence(started.quiescence.unwrap())
        else {
            panic!("empty Confirm opened a finalizer task")
        };
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
        assert!(empty.program_worklist.is_empty());
        assert!(!empty.registry.is_live(started.activation));

        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut rejected = DeltaScheduler::new();
        let started = rejected.registry.start_many(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![value(1), value(2)]),
            },
            candidate_return(Vec::new()),
            std::iter::empty::<ResidualDeltaOutput>(),
        );
        let DeltaSettlement::Retargeted(active) =
            rejected.settle_quiescence(started.quiescence.unwrap())
        else {
            panic!("nonempty rejected bag was not scheduled for scanning")
        };
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let eof = rejected.step_active(
            &root,
            &plan,
            active,
            8,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(eof.status, ActiveDeltaStatus::Quiescent);
        assert!(eof.resume.is_none());
        assert_eq!(eof.outcome.completed_activation_ids, [started.activation]);
        assert_eq!(eof.outcome.dead_pages, 1);
        assert!(eof.outcome.continuation.is_none());
        assert!(stable.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn formula_confirm_finalizer_accepts_or_ancestry_now_that_admission_is_pageable() {
        fn formula_batch(original: &CandidatePayload, with_or: bool) -> FormulaBatch {
            let mut frames = vec![FormulaPayloadFrame::And {
                current: original.clone(),
            }];
            if with_or {
                frames.push(FormulaPayloadFrame::Or {
                    source: original.clone(),
                    accumulator: FormulaOrAccumulator::empty(1),
                });
            } else {
                frames.push(FormulaPayloadFrame::And {
                    current: original.clone(),
                });
            }
            FormulaBatch {
                activations: vec![super::super::ActivationId(11)],
                parents: RowBatch::seed(),
                frames,
            }
        }

        fn quiescent_formula(
            registry: &mut ProducerRegistry,
            original: CandidatePayload,
            with_or: bool,
        ) -> QuiescenceProof {
            let batch = formula_batch(&original, with_or);
            let started = registry.start_many(
                DeltaReducer::Confirm { original },
                DeltaReturn::Formula {
                    bound: VariableSet::new_empty(),
                    counter: FormulaPcId(0),
                    batch,
                },
                [output(7, 0, true)],
            );
            let (_, credit) = started.roots.into_iter().next().unwrap();
            registry
                .replace_traversal(credit, [])
                .quiescence
                .expect("one-root Formula Confirm quiesced")
        }

        let original = shared_one_parent_candidates(vec![value(7), value(8), value(7)]);
        let mut all_and = ProducerRegistry::new();
        let proof = quiescent_formula(&mut all_and, original.clone(), false);
        let activation = proof.activation;
        let RegistrySettlement::ConfirmFinalizer(seed) = all_and.settle_quiescence(proof) else {
            panic!("all-AND Formula Confirm did not enter the pageable finalizer")
        };
        assert_eq!(seed.activation, activation);
        assert!(all_and.is_live(activation));

        let mut with_or = ProducerRegistry::new();
        let proof = quiescent_formula(&mut with_or, original, true);
        let activation = proof.activation;
        let RegistrySettlement::ConfirmFinalizer(seed) = with_or.settle_quiescence(proof) else {
            panic!("Formula OR ancestry did not enter the pageable Confirm finalizer")
        };
        assert_eq!(seed.activation, activation);
        assert!(with_or.is_live(activation));

        let empty_original = shared_one_parent_candidates(Vec::new());
        let empty_batch = formula_batch(&empty_original, false);
        let mut empty = ProducerRegistry::new();
        let started = empty.start_many(
            DeltaReducer::Confirm {
                original: empty_original,
            },
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                counter: FormulaPcId(0),
                batch: empty_batch,
            },
            std::iter::empty::<ResidualDeltaOutput>(),
        );
        let RegistrySettlement::Completed(completed) =
            empty.settle_quiescence(started.quiescence.unwrap())
        else {
            panic!("empty all-AND Formula Confirm opened a finalizer task")
        };
        assert!(matches!(completed.return_to, DeltaReturn::Formula { .. }));
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
    }

    #[test]
    fn active_program_confirm_retargets_exactly_to_the_engine_finalizer() {
        let novelty_drops = Arc::new(AtomicUsize::new(0));
        let root = OneShotConfirmProgram {
            novelty_drops,
            physical: false,
        };
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let request = ProgramRequest {
            action: ProgramAction::Confirm(0),
            bound: VariableSet::new_empty(),
        };
        let spec = root.residual_program().unwrap();
        let route = spec.route(request).unwrap();
        let mut scheduler = DeltaScheduler::new();
        let old = scheduler
            .seed_program_confirms(
                spec,
                DeltaDesc::leaf(0, 0),
                request,
                route,
                successor,
                false,
                CandidateBatch {
                    parents: RowBatch::seed(),
                    candidates: CandidatePayload::Values(vec![value(7)]),
                },
            )
            .expect("Confirm Program seeded one graph task");

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let graph = scheduler.step_active(
            &root,
            &plan,
            old,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(graph.status, ActiveDeltaStatus::Pending);
        assert!(graph.outcome.continuation.is_none());
        let retargeted = graph.resume.expect("live Confirm has an exact new state");
        assert_eq!(retargeted.activation, old.activation);
        assert_ne!(retargeted.state, old.state);
        assert_eq!(
            scheduler.interner.program(retargeted.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::ConfirmFinalize))
        );
        assert!(!scheduler.has_active_program(old));
        assert!(scheduler.has_active_program(retargeted));
        assert!(stable.is_empty());
        let graph_telemetry = (
            stats.delta_source_pages,
            stats.delta_source_candidates_examined,
            stats.delta_transition_pages,
            stats.delta_transition_candidates_examined,
        );

        let finalized = scheduler.step_active(
            &root,
            &plan,
            retargeted,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(finalized.status, ActiveDeltaStatus::Yielded);
        assert!(finalized.resume.is_none());
        assert_eq!(finalized.outcome.completed_activation_ids, [old.activation]);
        assert_eq!(
            (
                stats.delta_source_pages,
                stats.delta_source_candidates_examined,
                stats.delta_transition_pages,
                stats.delta_transition_candidates_examined,
            ),
            graph_telemetry
        );
        assert!(!scheduler.registry.is_live(old.activation));
    }

    #[test]
    fn physical_program_placement_stats_count_exact_cohort_geometry() {
        let novelty_drops = Arc::new(AtomicUsize::new(0));
        let root = OneShotConfirmProgram {
            novelty_drops,
            physical: true,
        };
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let request = ProgramRequest {
            action: ProgramAction::Confirm(0),
            bound: VariableSet::new_empty(),
        };
        let spec = root.residual_program().unwrap();
        let route = spec.route(request).unwrap();
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 2;
        scheduler
            .seed_program_confirms(
                spec,
                DeltaDesc::leaf(0, 0),
                request,
                route,
                successor,
                false,
                CandidateBatch {
                    parents: RowBatch {
                        rows: Vec::new(),
                        row_count: 2,
                    },
                    candidates: CandidatePayload::Tagged(vec![(0, value(7)), (1, value(8))]),
                },
            )
            .expect("both Confirm parents must seed one live Program root");

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let graph = scheduler.step_bounded(
            &root,
            &plan,
            8,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(graph.completed_activations, 0);
        assert_eq!(stats.delta_program_physical_cohorts, 1);
        assert_eq!(stats.delta_program_physical_rows, 2);
        assert_eq!(stats.delta_program_physical_granted_work, 8);
        assert_eq!(stats.max_delta_program_physical_cohort, 2);
        assert_eq!(stats.max_delta_program_physical_granted_work, 8);

        let finalized = scheduler.step_bounded(
            &root,
            &plan,
            8,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(finalized.completed_activations, 2);
        assert_eq!(stats.delta_program_physical_cohorts, 1);
        assert_eq!(stats.delta_program_physical_rows, 2);
        assert_eq!(stats.delta_program_physical_granted_work, 8);
    }

    #[test]
    fn terminal_program_returns_each_task_with_its_limit_in_append_order() {
        let mut scheduler = DeltaScheduler::new();
        let state = test_program_state(&mut scheduler);
        let full = VariableSet::new_singleton(0);
        let narrow = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        let wide = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        for expected in [2, 4] {
            let (_, widened) =
                scheduler
                    .registry
                    .finish_dispatch(wide, 8, PhysicalDispatchKind::Program, false);
            assert!(widened);
            assert_eq!(
                scheduler.registry.transition_dispatch_width(wide, 8),
                expected
            );
        }
        let mut narrow_tasks = install_program_tasks(
            &mut scheduler.registry,
            narrow,
            0..2,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut wide_tasks = install_program_tasks(
            &mut scheduler.registry,
            wide,
            2..5,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let n0 = narrow_tasks.next().unwrap();
        let n1 = narrow_tasks.next().unwrap();
        let w0 = wide_tasks.next().unwrap();
        let w1 = wide_tasks.next().unwrap();
        let w2 = wide_tasks.next().unwrap();
        let expected = [
            w0.credit.key.nonce,
            n1.credit.key.nonce,
            w1.credit.key.nonce,
            w2.credit.key.nonce,
        ];
        let retained = n0.credit.key.nonce;
        let _ = scheduler.file_program_state(state, vec![n0, w0, n1, w1, w2]);

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(8);
        assert_eq!(popped_state, state);
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(dispatch.task_limits, [2, 1, 1, 1]);
        assert_eq!(dispatch.remainder_tasks, 1);
        assert_eq!(scheduler.program_worklist[&state].tasks.len(), 1);
        assert_eq!(
            scheduler.program_worklist[&state].tasks[0].credit.key.nonce,
            retained
        );
        assert_eq!(
            dispatch.terminal_budgets,
            [
                TerminalActivationBudget {
                    activation: wide,
                    assigned: 4,
                    quantum: 4,
                },
                TerminalActivationBudget {
                    activation: narrow,
                    assigned: 1,
                    quantum: 1,
                },
            ]
        );
    }

    #[test]
    fn terminal_program_pop_funds_the_hot_activation_without_cross_quantum_averaging() {
        let mut scheduler = DeltaScheduler::new();
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        };
        let state = scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 0), route));
        let full = VariableSet::new_singleton(0);
        let narrow = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        let wide = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        for expected in [2, 4, 8] {
            let (_, widened) =
                scheduler
                    .registry
                    .finish_dispatch(wide, 8, PhysicalDispatchKind::Program, false);
            assert!(widened);
            assert_eq!(
                scheduler.registry.transition_dispatch_width(wide, 8),
                expected
            );
        }

        let install = |registry: &mut ProducerRegistry, activation, slot| {
            registry
                .install_program_roots(
                    activation,
                    [ProgramSeedWork {
                        parent: 0,
                        work: ProgramWork {
                            handle: ProgramWorkHandle::test(slot),
                            dispatch: DispatchClass::new(0),
                            pacing: ProgramPacing::Activation,
                        },
                        accepted: None,
                    }],
                )
                .roots
                .pop()
                .unwrap()
        };
        let narrow_root = install(&mut scheduler.registry, narrow, 0);
        let wide_root = install(&mut scheduler.registry, wide, 1);
        let _ = scheduler.file_program_state(
            state,
            vec![
                ProgramTask {
                    activation: narrow,
                    work: narrow_root.0,
                    credit: narrow_root.1,
                },
                ProgramTask {
                    activation: wide,
                    work: wide_root.0,
                    credit: wide_root.1,
                },
            ],
        );

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(8);
        assert_eq!(popped_state, state);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].activation, wide);
        assert_eq!(dispatch.task_limits, [8]);
        assert_eq!(dispatch.remainder_tasks, 1);
        assert_eq!(
            dispatch.terminal_budgets,
            [TerminalActivationBudget {
                activation: wide,
                assigned: 8,
                quantum: 8,
            }]
        );

        let mut stats = ResidualStateStats::default();
        assert!(scheduler.account_physical_dispatch(
            dispatch,
            8,
            0,
            &OrderedActivationSet::default(),
            &mut stats,
        ));
        assert_eq!(scheduler.registry.transition_dispatch_width(narrow, 8), 1);
        assert_eq!(scheduler.registry.transition_dispatch_width(wide, 8), 8);
        assert_eq!(stats.delta_terminal_sparse_widenings, 0);
    }

    #[test]
    fn empty_support_roots_prove_false_only_at_quiescence() {
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            [],
        );

        let completed = registry.finish(
            started
                .quiescence
                .expect("an empty support frontier is immediately quiescent"),
        );
        assert_eq!(completed.effect, DeltaCompletion::Support(false));
        assert!(matches!(completed.return_to, DeltaReturn::Formula { .. }));
    }

    #[test]
    fn accepting_seed_is_an_immediate_effect_receipt_not_an_expansion_side_effect() {
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            [output(7, 0, true)],
        );
        assert_eq!(started.initial_accepted, [value(7)]);
        assert_eq!(
            registry
                .take_streaming_return(started.activation)
                .expect("the accepting seed has a streaming return")
                .effect,
            DeltaStreamingEffect::Candidates
        );
        let (_, root) = started.roots.into_iter().next().expect("one seed root");

        let expanded = registry.replace_traversal(root, []);
        assert!(
            expanded.accepted.is_empty(),
            "the first adjacency expansion replayed seed acceptance"
        );
        let completed = registry.finish(expanded.quiescence.expect("the root quiesces"));
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
    }

    #[test]
    fn support_reducer_publishes_only_the_first_distinct_witness() {
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            [output(1, 0, false), output(2, 0, false)],
        );
        let activation = started.activation;
        let mut roots = started.roots.into_iter();
        let (_, first_root) = roots.next().unwrap();
        let (_, second_root) = roots.next().unwrap();

        let first = registry.replace_traversal(first_root, [output(7, 1, true)]);
        assert_eq!(first.accepted, [value(7)]);
        let streamed = registry
            .take_streaming_return(activation)
            .expect("the first witness publishes support");
        assert_eq!(streamed.effect, DeltaStreamingEffect::Support);
        assert!(matches!(streamed.return_to, DeltaReturn::Formula { .. }));

        let second =
            registry.replace_traversal(second_root, [output(7, 2, true), output(8, 2, true)]);
        assert_eq!(second.accepted, [value(8)]);
        assert!(registry.take_streaming_return(activation).is_none());

        let mut proof = None;
        for (_, child) in first.children.into_iter().chain(second.children) {
            let retired = registry.replace_traversal(child, []);
            if let Some(quiescence) = retired.quiescence {
                assert!(proof.replace(quiescence).is_none());
            }
        }
        let completed = registry.finish(proof.expect("the last witness lineage quiesces"));
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
    }

    #[test]
    fn support_publication_state_is_preserved_across_deep_clone() {
        let mut original = ProducerRegistry::new();
        let started = original.start_many(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            [output(1, 0, false), output(2, 0, false)],
        );
        let activation = started.activation;
        let mut roots = started.roots.into_iter();
        let (_, witness_root) = roots.next().unwrap();
        let (_, remaining_root) = roots.next().unwrap();
        let remaining_key = remaining_root.key;

        let first = original.replace_traversal(witness_root, [output(7, 1, true)]);
        assert_eq!(first.accepted, [value(7)]);
        assert_eq!(
            original
                .take_streaming_return(activation)
                .expect("the original publishes its witness")
                .effect,
            DeltaStreamingEffect::Support
        );

        let (mut cloned, mut remap) = original.deep_clone();
        let cloned_remaining = remap
            .remove(&remaining_key)
            .expect("the clone remapped the still-live root");
        let original_second = original.replace_traversal(remaining_root, [output(8, 2, true)]);
        let cloned_second = cloned.replace_traversal(cloned_remaining, [output(8, 2, true)]);
        assert_eq!(original_second.accepted, [value(8)]);
        assert_eq!(cloned_second.accepted, [value(8)]);
        assert!(original.take_streaming_return(activation).is_none());
        assert!(cloned.take_streaming_return(activation).is_none());
    }

    #[test]
    fn transition_pop_keeps_quiescent_activations_coherent() {
        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::leaf(0, 0);

        let mut first = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [output(1, 0, false), output(2, 0, false)],
        );
        let first_activation = first.activation;
        let (first_node, first_credit) = first.roots.remove(0);
        let (last_node, last_credit) = first.roots.remove(0);

        let mut second = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [output(3, 0, false)],
        );
        let second_activation = second.activation;
        let (second_node, second_credit) = second.roots.remove(0);

        let _ = scheduler.file(
            desc,
            vec![
                DeltaTask {
                    activation: first_activation,
                    credit: first_credit,
                    node: first_node,
                    cursor: ResidualDeltaExpandCursor::Start,
                },
                DeltaTask {
                    activation: second_activation,
                    credit: second_credit,
                    node: second_node,
                    cursor: ResidualDeltaExpandCursor::Start,
                },
                DeltaTask {
                    activation: first_activation,
                    credit: last_credit,
                    node: last_node,
                    cursor: ResidualDeltaExpandCursor::Start,
                },
            ],
        );

        let (_, selected) = scheduler.pop(usize::MAX);
        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|task| task.activation == first_activation),
            "one quiescent fixpoint must consume width internally"
        );
        let retained = scheduler
            .worklist
            .values()
            .next()
            .expect("the independent activation remains queued");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained.last().unwrap().activation, second_activation);
    }

    #[test]
    fn active_transition_pop_is_exact_and_preserves_global_task_order() {
        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::leaf(0, 0);
        let mut first = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [
                output(1, 0, false),
                output(2, 0, false),
                output(3, 0, false),
            ],
        );
        let first_activation = first.activation;
        let mut second = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            [output(4, 0, false), output(5, 0, false)],
        );
        let second_activation = second.activation;
        let task = |started: &mut StartOutcome| {
            let (node, credit) = started.roots.remove(0);
            DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }
        };
        let tasks = vec![
            task(&mut first),
            task(&mut second),
            task(&mut first),
            task(&mut second),
            task(&mut first),
        ];
        let active = scheduler
            .file(desc.clone(), tasks)
            .expect("the interleaved bucket is live");
        assert_eq!(active.activation, first_activation);
        assert_eq!(scheduler.interner.get(active.state), &desc);
        assert!(scheduler.file(desc, Vec::new()).is_none());
        assert_eq!(scheduler.interner.entries.len(), 1);

        let (_, selected) = scheduler.pop_active_transition(active, 2);
        assert_eq!(
            selected
                .iter()
                .map(|task| (task.activation, task.node.value))
                .collect::<Vec<_>>(),
            [(first_activation, value(2)), (first_activation, value(3))]
        );
        let retained = scheduler
            .worklist
            .get(&active.state)
            .expect("cold tasks remain");
        assert_eq!(
            retained
                .iter()
                .map(|task| (task.activation, task.node.value))
                .collect::<Vec<_>>(),
            [
                (first_activation, value(1)),
                (second_activation, value(4)),
                (second_activation, value(5)),
            ]
        );
        retained.assert_index_consistent();
    }

    #[test]
    fn activation_index_compacts_geometrically_without_reordering_survivors() {
        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::leaf(0, 0);
        let mut first = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            (1..=4).map(|value| output(value, 0, false)),
        );
        let first_activation = first.activation;
        let mut second = scheduler.registry.start_many(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            (5..=8).map(|value| output(value, 0, false)),
        );
        let task = |started: &mut StartOutcome| {
            let (node, credit) = started.roots.remove(0);
            DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }
        };
        let tasks = vec![
            task(&mut first),
            task(&mut second),
            task(&mut first),
            task(&mut second),
            task(&mut first),
            task(&mut second),
            task(&mut first),
            task(&mut second),
        ];
        let active = scheduler.file(desc, tasks).unwrap();
        let bucket = scheduler.worklist.get_mut(&active.state).unwrap();

        let selected = bucket.take_activation_indexed(first_activation, 3);
        assert_eq!(
            selected
                .iter()
                .map(|task| task.node.value)
                .collect::<Vec<_>>(),
            [value(2), value(3), value(4)]
        );
        assert_eq!((bucket.arena_len(), bucket.len()), (8, 5));
        bucket.assert_index_consistent();

        let selected = bucket.take_activation(second.activation, 3);
        assert_eq!(
            selected
                .iter()
                .map(|task| task.node.value)
                .collect::<Vec<_>>(),
            [value(6), value(7), value(8)]
        );
        assert_eq!((bucket.arena_len(), bucket.len()), (2, 2));
        assert_eq!(
            bucket
                .iter()
                .map(|task| (task.activation, task.node.value))
                .collect::<Vec<_>>(),
            [(first_activation, value(1)), (second.activation, value(5))]
        );
        bucket.assert_index_consistent();
    }

    #[test]
    fn indexed_terminal_selection_matches_literal_reverse_scan_under_churn() {
        type TaskKey = (ActivationId, ResidualDeltaNode, ResidualDeltaExpandCursor);

        fn key(task: &DeltaTask) -> TaskKey {
            (task.activation, task.node, task.cursor)
        }

        fn literal_take_activation(
            tasks: &mut Vec<DeltaTask>,
            activation: ActivationId,
            width: usize,
        ) -> Vec<DeltaTask> {
            let width = width.max(1);
            let mut selected = Vec::with_capacity(width.min(tasks.len()));
            let mut retained = Vec::with_capacity(tasks.len());
            for task in std::mem::take(tasks).into_iter().rev() {
                if task.activation == activation && selected.len() < width {
                    selected.push(task);
                } else {
                    retained.push(task);
                }
            }
            selected.reverse();
            retained.reverse();
            *tasks = retained;
            selected
        }

        fn literal_take_tail(
            tasks: &mut Vec<DeltaTask>,
            registry: &ProducerRegistry,
            width: usize,
            activation_width: usize,
        ) -> (Vec<DeltaTask>, Vec<usize>) {
            let width = width.max(1);
            let activation_width = activation_width.max(1);
            let key = TransitionDispatchKey::of(
                registry,
                tasks.last().expect("literal bucket is nonempty"),
            );
            if key == TransitionDispatchKey::TerminalStreaming {
                let hot_activation = tasks.last().unwrap().activation;
                if registry.transition_dispatch_width(hot_activation, width) == width {
                    let selected = literal_take_activation(tasks, hot_activation, width);
                    let limits = even_limits(width, selected.len());
                    return (selected, limits);
                }

                #[derive(Debug)]
                struct Selection {
                    budget: usize,
                    selected: usize,
                    ordinal: usize,
                }

                let mut remaining = width;
                let mut slots = AHashMap::new();
                let mut selections = Vec::new();
                let mut selected = Vec::with_capacity(width.min(tasks.len()));
                let mut limits = Vec::with_capacity(width.min(tasks.len()));
                let mut retained = Vec::with_capacity(tasks.len());
                for task in std::mem::take(tasks).into_iter().rev() {
                    if TransitionDispatchKey::of(registry, &task) == key {
                        let selection_slot = if let Some(&slot) = slots.get(&task.activation) {
                            Some(slot)
                        } else if remaining > 0 {
                            let budget = registry
                                .transition_dispatch_width(task.activation, width)
                                .min(remaining);
                            remaining -= budget;
                            let slot = selections.len();
                            selections.push(Selection {
                                budget,
                                selected: 0,
                                ordinal: 0,
                            });
                            slots.insert(task.activation, slot);
                            Some(slot)
                        } else {
                            None
                        };
                        if let Some((slot, selection)) = selection_slot
                            .map(|slot| (slot, &mut selections[slot]))
                            .filter(|(_, selection)| selection.selected < selection.budget)
                        {
                            selection.selected += 1;
                            limits.push(slot);
                            selected.push(task);
                            continue;
                        }
                    }
                    retained.push(task);
                }
                selected.reverse();
                limits.reverse();
                retained.reverse();
                *tasks = retained;
                for slot_or_limit in &mut limits {
                    let selection = &mut selections[*slot_or_limit];
                    let quotient = selection.budget / selection.selected;
                    let remainder = selection.budget % selection.selected;
                    *slot_or_limit = quotient + usize::from(selection.ordinal < remainder);
                    selection.ordinal += 1;
                }
                return (selected, limits);
            }

            let mut activations = BTreeSet::new();
            let mut selected = Vec::with_capacity(width.min(tasks.len()));
            let mut retained = Vec::with_capacity(tasks.len());
            for task in std::mem::take(tasks).into_iter().rev() {
                let compatible = match (key, TransitionDispatchKey::of(registry, &task)) {
                    (TransitionDispatchKey::Streaming, TransitionDispatchKey::Streaming) => true,
                    (
                        TransitionDispatchKey::Quiescent(_),
                        TransitionDispatchKey::Quiescent(activation),
                    ) => {
                        activations.contains(&activation)
                            || (activations.len() < activation_width && {
                                activations.insert(activation);
                                true
                            })
                    }
                    _ => false,
                };
                if selected.len() < width && compatible {
                    selected.push(task);
                } else {
                    retained.push(task);
                }
            }
            selected.reverse();
            retained.reverse();
            *tasks = retained;
            let limits = even_limits(width, selected.len());
            (selected, limits)
        }

        let mut rng = StdRng::seed_from_u64(0x5eed_1ade_7a11_2026);
        for case in 0..32 {
            let mut scheduler = DeltaScheduler::new();
            let mut starts = Vec::new();
            let mut terminal = Vec::new();
            let mut next_value = 0u8;
            for activation_index in 0..8 {
                let outputs: Vec<_> = (0..8)
                    .map(|_| {
                        let current = next_value;
                        next_value += 1;
                        output(current, activation_index, false)
                    })
                    .collect();
                let started = if activation_index % 3 == 0 {
                    scheduler.registry.start_many(
                        DeltaReducer::StreamProposal,
                        candidate_return(Vec::new()),
                        outputs,
                    )
                } else {
                    let started = scheduler.registry.start_many_terminal(
                        DeltaReducer::StreamProposal,
                        candidate_return(Vec::new()),
                        outputs,
                        VariableSet::new_singleton(0),
                    );
                    terminal.push(started.activation);
                    started
                };
                starts.push(started);
            }
            for &activation in &terminal {
                for _ in 0..rng.gen_range(0..=5) {
                    let _ = scheduler.registry.finish_dispatch(
                        activation,
                        64,
                        PhysicalDispatchKind::Transition,
                        false,
                    );
                }
            }

            let mut tasks = Vec::with_capacity(64);
            while starts.iter().any(|started| !started.roots.is_empty()) {
                let live: Vec<_> = starts
                    .iter()
                    .enumerate()
                    .filter_map(|(index, started)| (!started.roots.is_empty()).then_some(index))
                    .collect();
                let index = live[rng.gen_range(0..live.len())];
                let (node, credit) = starts[index].roots.remove(0);
                tasks.push(DeltaTask {
                    activation: starts[index].activation,
                    credit,
                    node,
                    cursor: ResidualDeltaExpandCursor::Start,
                });
            }
            let terminal_tail = tasks
                .iter()
                .rposition(|task| terminal.contains(&task.activation))
                .unwrap();
            let tail = tasks.remove(terminal_tail);
            tasks.push(tail);

            let active = scheduler
                .file(DeltaDesc::leaf(0, 0), tasks)
                .expect("random bucket is live");
            let churn_activation = terminal[0];
            let churned = scheduler
                .worklist
                .get_mut(&active.state)
                .unwrap()
                .take_activation_indexed(churn_activation, 2);
            scheduler
                .worklist
                .get_mut(&active.state)
                .unwrap()
                .extend(churned);

            // Cloning an indexed arena with tombstones must produce an
            // independent, append-order-equivalent plain sibling.
            let mut oracle_scheduler = scheduler.deep_clone();
            let mut indexed = scheduler.worklist.remove(&active.state).unwrap();
            let mut oracle = oracle_scheduler
                .worklist
                .remove(&active.state)
                .unwrap()
                .drain_live();
            let mut indexed_stash = Vec::new();
            let mut oracle_stash = Vec::new();
            let mut selection_slots = AHashMap::new();
            let mut selections = Vec::new();

            for step in 0..400 {
                if !indexed_stash.is_empty() && (indexed.is_empty() || rng.gen_bool(0.2)) {
                    let count = rng.gen_range(1..=indexed_stash.len());
                    let indexed_append = indexed_stash.split_off(indexed_stash.len() - count);
                    let oracle_append = oracle_stash.split_off(oracle_stash.len() - count);
                    indexed.extend(indexed_append);
                    oracle.extend(oracle_append);
                } else {
                    let widths = [0usize, 1, 2, 3, 5, 8, 13, 64, usize::MAX];
                    let width = widths[rng.gen_range(0..widths.len())];
                    let activation_width = rng.gen_range(0..=8);
                    let exact = rng.gen_bool(0.35);
                    let (indexed_selected, indexed_limits, oracle_selected, oracle_limits) =
                        if exact {
                            let live: Vec<_> = oracle.iter().map(|task| task.activation).collect();
                            let activation = live[rng.gen_range(0..live.len())];
                            (
                                indexed.take_activation(activation, width),
                                None,
                                literal_take_activation(&mut oracle, activation, width),
                                None,
                            )
                        } else {
                            let (selected, limits) = indexed.take_tail(
                                &scheduler.registry,
                                width,
                                activation_width,
                                &mut selection_slots,
                                &mut selections,
                            );
                            let (oracle_selected, oracle_limits) = literal_take_tail(
                                &mut oracle,
                                &oracle_scheduler.registry,
                                width,
                                activation_width,
                            );
                            (selected, Some(limits), oracle_selected, Some(oracle_limits))
                        };
                    assert_eq!(
                        indexed_selected.iter().map(key).collect::<Vec<_>>(),
                        oracle_selected.iter().map(key).collect::<Vec<_>>(),
                        "selected order diverged in case {case}, step {step}"
                    );
                    assert_eq!(
                        indexed_limits, oracle_limits,
                        "budget allocation diverged in case {case}, step {step}"
                    );
                    if rng.gen_bool(0.45) {
                        indexed.extend(indexed_selected);
                        oracle.extend(oracle_selected);
                    } else {
                        indexed_stash.extend(indexed_selected);
                        oracle_stash.extend(oracle_selected);
                    }
                }

                assert_eq!(
                    indexed.iter().map(key).collect::<Vec<_>>(),
                    oracle.iter().map(key).collect::<Vec<_>>(),
                    "retained order diverged in case {case}, step {step}"
                );
                assert_eq!(
                    indexed_stash.iter().map(key).collect::<Vec<_>>(),
                    oracle_stash.iter().map(key).collect::<Vec<_>>(),
                    "stashed order diverged in case {case}, step {step}"
                );
                indexed.assert_index_consistent();
            }
        }
    }

    #[test]
    fn seed_token_names_last_filed_live_activation_not_last_created_parent() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant: relevant.clone(),
                checked: relevant,
            },
        };
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let seeded = scheduler.seed_proposals(
            DeltaDesc::leaf(0, 0),
            successor.clone(),
            RowBatch {
                rows: Vec::new(),
                row_count: 2,
            },
            vec![ResidualDeltaSeed {
                parent: 0,
                output: output(1, 0, false),
            }],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let active = seeded.active.expect("the first parent filed one root");
        assert_eq!(active.activation, ActivationId(0));
        assert!(scheduler.registry.is_live(active.activation));
        assert_eq!(
            scheduler.registry.state.next_activation, 2,
            "the later empty parent was created and immediately retired"
        );
        assert!(seeded.continuation.is_none());

        let empty = scheduler.seed_proposals(
            DeltaDesc::leaf(0, 0),
            successor,
            RowBatch {
                rows: Vec::new(),
                row_count: 1,
            },
            Vec::new(),
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(empty.active.is_none());
        assert!(empty.continuation.is_none());
    }

    #[test]
    fn active_source_cycles_through_transition_before_unrelated_work() {
        let root = SourceTransitionCycle;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let desc = DeltaDesc::leaf(0, 0);
        let mut scheduler = DeltaScheduler::new();
        let active = scheduler
            .seed_source_proposals(
                desc.clone(),
                StateDesc {
                    bound: VariableSet::new_empty(),
                    phase: ResidualPhase::Ready,
                },
                RowBatch {
                    rows: Vec::new(),
                    row_count: 1,
                },
            )
            .expect("the source generator was filed");

        let mut cloned = scheduler.clone();
        let mut clone_stable = Worklist::new();
        let mut clone_interner = StateInterner::default();
        let mut clone_stats = ResidualStateStats::default();
        assert_eq!(
            cloned
                .step_active(
                    &root,
                    &plan,
                    active,
                    1,
                    &mut clone_stable,
                    &mut clone_interner,
                    &mut clone_stats,
                )
                .status,
            ActiveDeltaStatus::Pending,
            "deep clone must preserve state and activation identities named by the token"
        );

        let mut unrelated = scheduler.registry.start_many(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(9, 0, false)],
        );
        let unrelated_activation = unrelated.activation;
        let (node, credit) = unrelated.roots.pop().expect("one unrelated root");
        let _ = scheduler.file(
            desc,
            vec![DeltaTask {
                activation: unrelated_activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }],
        );

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let source = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(source.status, ActiveDeltaStatus::Pending);
        assert_eq!(source.resume, Some(active));
        assert_eq!(stats.delta_source_pages, 1);
        assert!(scheduler.has_active_transition(active));

        let transition = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(transition.status, ActiveDeltaStatus::Pending);
        assert_eq!(transition.resume, Some(active));
        assert!(scheduler.has_active_source(active));

        let terminal_source = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(terminal_source.status, ActiveDeltaStatus::Quiescent);
        assert!(terminal_source.resume.is_none());
        assert_eq!(stats.delta_source_pages, 2);
        assert!(stable.is_empty());
        let retained = scheduler
            .worklist
            .get(&active.state)
            .expect("unrelated transition remains cold");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained.last().unwrap().activation, unrelated_activation);
    }

    #[test]
    fn active_transition_yield_is_classified_before_registry_liveness() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let desc = DeltaDesc::leaf(0, 0);
        let mut scheduler = DeltaScheduler::new();
        let mut started = scheduler.registry.start_many(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            [output(2, 0, false)],
        );
        let activation = started.activation;
        let (node, credit) = started.roots.pop().expect("one accepting lineage");
        let active = scheduler
            .file(
                desc,
                vec![DeltaTask {
                    activation,
                    credit,
                    node,
                    cursor: ResidualDeltaExpandCursor::Start,
                }],
            )
            .unwrap();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let yielded = scheduler.step_active(
            &root,
            &plan,
            active,
            1,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(yielded.status, ActiveDeltaStatus::Yielded);
        assert!(yielded.outcome.continuation.is_some());
        assert_eq!(yielded.resume, Some(active));
        assert!(scheduler.registry.is_live(activation));
        assert!(scheduler.has_active_transition(active));
    }

    #[test]
    fn transition_pages_dispatch_as_one_affine_cohort_with_mixed_eager_fallback() {
        let trace = Arc::new(Mutex::new(None));
        let scalar_page_calls = Arc::new(AtomicUsize::new(0));
        let eager_nodes = Arc::new(AtomicUsize::new(0));
        let root = BatchedPagedExpansion {
            trace: Arc::clone(&trace),
            scalar_page_calls: Arc::clone(&scalar_page_calls),
            eager_nodes: Arc::clone(&eager_nodes),
        };
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let desc = DeltaDesc::leaf(0, 0);
        let mut scheduler = DeltaScheduler::new();
        let mut tasks = Vec::new();
        let mut activations = Vec::new();
        for byte in [1, 3, 2] {
            let mut started = scheduler.registry.start_many(
                DeltaReducer::StreamProposal,
                candidate_return(Vec::new()),
                [output(byte, 0, false)],
            );
            activations.push(started.activation);
            let (node, credit) = started.roots.pop().expect("one transition root");
            assert!(started.quiescence.is_none());
            tasks.push(DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            });
        }
        let _ = scheduler.file(desc.clone(), tasks);

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let outcome = scheduler.step(
            &root,
            &plan,
            3,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert!(outcome.continuation.is_none());
        assert_eq!(outcome.dead_pages, 0);
        assert!(stable.is_empty());
        assert_eq!(scalar_page_calls.load(Ordering::Relaxed), 0);
        assert_eq!(eager_nodes.load(Ordering::Relaxed), 1);
        assert_eq!(stats.delta_transition_pages, 2);
        assert_eq!(stats.delta_transition_cohorts, 1);
        assert_eq!(stats.max_delta_transition_cohort, 2);
        assert_eq!(stats.delta_transition_candidates_examined, 2);
        assert_eq!(
            *trace.lock().expect("transition batch trace poisoned"),
            Some(TransitionBatchTrace {
                nodes: vec![
                    output(1, 0, false).node,
                    output(3, 0, false).node,
                    output(2, 0, false).node
                ],
                cursors: vec![ResidualDeltaExpandCursor::Start; 3],
                limits: vec![1; 3],
            })
        );

        let bucket = scheduler
            .worklist
            .values()
            .next()
            .expect("transition children were refiled");
        let actual: Vec<_> = bucket
            .iter()
            .map(|task| (task.activation, task.node.value, task.node.continuation))
            .collect();
        assert_eq!(
            actual,
            vec![
                (activations[0], value(11), 1),
                (activations[1], value(13), 1),
                (activations[2], value(12), 1),
            ],
            "tagged native and eager successors crossed affine activations"
        );
    }

    #[test]
    fn batched_delta_step_keeps_dead_page_and_seeded_formula_handoff_independent() {
        // Keep a real, streamable formula boundary in this white-box fixture.
        // A lone opaque root is deliberately normalized to the flat action
        // plan, while an OR frame is a streaming barrier by construction.
        let root = IntersectionConstraint::new(vec![MixedExpansion, MixedExpansion]);
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let formula_root = plan
            .finite_formula
            .root(0)
            .expect("the intersection root has a formula program");
        let FiniteFormulaNodeKind::And { children } = &plan.finite_formula.node(formula_root).kind
        else {
            panic!("the intersection root did not compile as AND")
        };
        let desc = DeltaDesc::formula(0, 0, children[0]);
        let mut scheduler = DeltaScheduler::new();

        let (dead_activation, dead_generator) = scheduler.registry.start_source(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
        );
        let dead_page = scheduler.registry.replace_source(
            dead_generator,
            [output(1, 0, false)],
            [],
            Some(ResidualDeltaSourceCursor::After(value(1))),
        );
        let (dead_node, dead_credit) = dead_page.roots.into_iter().next().unwrap();

        // The accepting seed publishes before this scheduler step, while its
        // independent traversal credit proves quiescence during the step.
        // Cleanup must not replay the activation-local return template.
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut live = scheduler.registry.start_many(
            DeltaReducer::StreamFormulaProposal,
            streaming_formula_return(&plan, &mut stable_interner),
            [output(3, 0, true)],
        );
        let live_activation = live.activation;
        let (live_node, live_credit) = live.roots.pop().expect("one live formula root");
        assert!(live.quiescence.is_none());

        let mut stats = ResidualStateStats::default();
        let streamed = scheduler
            .registry
            .take_streaming_return(live_activation)
            .expect("an accepting formula seed has a streaming return");
        let seed_continuation = scheduler.release_streaming(
            live_activation,
            streamed,
            live.initial_accepted,
            None,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(seed_continuation.stable.continuation.is_some());
        assert!(seed_continuation.stable.publication.is_none());
        assert!(seed_continuation.active.is_none());

        let _ = scheduler.file(
            desc,
            vec![
                DeltaTask {
                    activation: dead_activation,
                    credit: dead_credit,
                    node: dead_node,
                    cursor: ResidualDeltaExpandCursor::Start,
                },
                DeltaTask {
                    activation: live_activation,
                    credit: live_credit,
                    node: live_node,
                    cursor: ResidualDeltaExpandCursor::Start,
                },
            ],
        );

        let outcome = scheduler.step(
            &root,
            &plan,
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(outcome.dead_pages, 1);
        assert!(outcome.continuation.is_none());
        assert_eq!(stats.delta_source_dead_pages, 1);
        assert_eq!(stats.candidates_proposed, 1);
        let mut stable_buckets = stable.values().flat_map(BTreeMap::values);
        let StateBucket::Candidates(batch) = stable_buckets.next().expect("one stable handoff")
        else {
            panic!("streamed formula handoff changed payload shape")
        };
        assert!(stable_buckets.next().is_none());
        assert_eq!(batch.parents.row_count, 1);
        assert!(batch.parents.rows.is_empty());
        assert!(batch.candidates.is_values());
        assert_eq!(batch.candidates.one_parent_values(), [value(3)]);
        assert!(
            !scheduler
                .registry
                .state
                .activations
                .contains_key(&live_activation),
            "formula quiescence must retire the streamed activation"
        );
        assert!(scheduler.worklist.is_empty());
        assert_eq!(
            scheduler
                .source_worklist
                .values()
                .next()
                .unwrap()
                .tasks
                .len(),
            1
        );

        let mut machine = ResidualStateMachine::new(root.variables(), plan.len(), Search::Done);
        machine.width = 4;
        machine.cap = 64;
        machine.accept_delta_step(outcome);
        assert_eq!(
            machine.width, 8,
            "the earlier seed receipt must not hide a later dead source page"
        );
        assert_eq!(machine.stats.delta_source_negative_steps, 1);
        assert!(machine.continuation.is_none());
    }

    #[test]
    fn source_is_part_of_activation_local_novelty() {
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            [
                sourced_output(1, 1, 0, false),
                sourced_output(2, 2, 0, false),
            ],
        );
        assert!(started.quiescence.is_none());
        let activation = started.activation;
        let roots = started.roots;

        let mut children = Vec::new();
        for ((_, root), successor) in roots.into_iter().zip([
            sourced_output(1, 3, 1, false),
            sourced_output(2, 3, 1, false),
        ]) {
            children.extend(registry.replace_traversal(root, [successor]).children);
        }
        assert_eq!(
            children.len(),
            2,
            "one source suppressed the other's C state"
        );
        assert_eq!(
            registry
                .state
                .activations
                .get(&activation)
                .expect("live activation")
                .seen
                .len(),
            2
        );
    }

    #[test]
    fn duplicate_accepted_roots_filter_one_original_confirm_sequence() {
        let candidate = value(7);
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![candidate, candidate]),
            },
            stable_return(Vec::new()),
            [output(7, 0, true), output(7, 0, true)],
        );
        assert!(started.quiescence.is_none());
        let activation = started.activation;
        let roots = started.roots;
        let mut proof = None;
        for (_, root) in roots {
            let outcome = registry.replace_traversal(root, []);
            if let Some(quiescence) = outcome.quiescence {
                assert!(proof.replace(quiescence).is_none());
            }
        }

        let proof = proof.expect("all root credits quiesced");
        assert_eq!(proof.activation, activation);
        let completed = registry.finish(proof);
        assert_eq!(
            completed.effect,
            DeltaCompletion::Candidates(CandidatePayload::Values(vec![candidate, candidate]))
        );
    }

    fn confirm_boundary_descs(plan: &ResidualPlan) -> (StateDesc, StateDesc) {
        assert_eq!(plan.len(), 1);
        let relevant = ChildSet::empty(1).with_inserted(0);
        (
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Confirm {
                    variable: 0,
                    relevant: relevant.clone(),
                    checked: ChildSet::empty(1),
                    confirmer: 0,
                },
            },
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Candidate {
                    variable: 0,
                    relevant: relevant.clone(),
                    checked: relevant,
                },
            },
        )
    }

    #[test]
    fn cyclic_confirm_set_admits_each_affine_parent_at_its_candidate_boundary() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let (previous, successor) = confirm_boundary_descs(&plan);
        let formula_pcs = FormulaPcInterner::default();
        let set_admit_result =
            crosses_candidate_set_boundary(&previous, &successor, &plan, &formula_pcs);
        assert!(set_admit_result);

        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats {
            candidates_confirmed: 5,
            ..ResidualStateStats::default()
        };
        for (activation, candidates) in [
            (1, vec![value(1), value(2), value(1)]),
            (2, vec![value(1), value(1)]),
        ] {
            let released = scheduler.release_completion(
                CompletedActivation {
                    activation: ActivationId(activation),
                    return_to: DeltaReturn::Stable {
                        desc: successor.clone(),
                        parent: Vec::new().into_boxed_slice(),
                        set_admit_result,
                    },
                    effect: DeltaCompletion::Candidates(CandidatePayload::Values(candidates)),
                },
                &plan,
                &mut stable,
                &mut stable_interner,
                &mut stats,
            );
            assert!(released.continuation.is_some());
            assert!(released.active.is_none());
        }

        let StateBucket::Candidates(batch) = stable
            .values()
            .flat_map(BTreeMap::values)
            .next()
            .expect("both affine parents rejoined one candidate state")
        else {
            panic!("cyclic Confirm returned a non-candidate payload")
        };
        assert_eq!(batch.parents.row_count, 2);
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            vec![(0, value(2)), (0, value(1)), (1, value(1))]
        );
        assert_eq!(
            stats.candidates_confirmed, 5,
            "SET admission must not rewrite raw Confirm telemetry"
        );
    }

    #[test]
    fn cyclic_confirm_keeps_a_nonboundary_internal_result_as_an_occurrence_bag() {
        let root = IntersectionConstraint::new(vec![MixedExpansion; 3]);
        let plan = ResidualPlan::compile(&root);
        assert_eq!(plan.len(), 3);
        let relevant = (0..3).fold(ChildSet::empty(3), |set, leaf| {
            set.with_inserted(leaf)
        });
        let previous = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Confirm {
                variable: 0,
                relevant: relevant.clone(),
                checked: ChildSet::empty(3).with_inserted(0),
                confirmer: 1,
            },
        };
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked: ChildSet::empty(3).with_inserted(0).with_inserted(1),
            },
        };
        let formula_pcs = FormulaPcInterner::default();
        let set_admit_result =
            crosses_candidate_set_boundary(&previous, &successor, &plan, &formula_pcs);
        assert!(!set_admit_result);

        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let released = scheduler.release_completion(
            CompletedActivation {
                activation: ActivationId(1),
                return_to: DeltaReturn::Stable {
                    desc: successor,
                    parent: Vec::new().into_boxed_slice(),
                    set_admit_result,
                },
                effect: DeltaCompletion::Candidates(CandidatePayload::Values(vec![
                    value(1),
                    value(2),
                    value(1),
                ])),
            },
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(released.continuation.is_some());
        assert!(released.active.is_none());
        let StateBucket::Candidates(batch) = stable
            .values()
            .flat_map(BTreeMap::values)
            .next()
            .unwrap()
        else {
            panic!("internal Confirm returned a non-candidate payload")
        };
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            vec![(0, value(1)), (0, value(2)), (0, value(1))]
        );
    }

    #[test]
    fn deferred_cyclic_confirm_routes_to_bounded_set_admission_without_materializing() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let (previous, successor) = confirm_boundary_descs(&plan);
        let set_admit_result = crosses_candidate_set_boundary(
            &previous,
            &successor,
            &plan,
            &FormulaPcInterner::default(),
        );
        let mut result = CandidatePayload::Values(vec![value(1), value(2), value(1)]);
        result.defer_for_shared_activation(1);
        assert!(matches!(result, CandidatePayload::Deferred(_)));

        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats {
            candidates_confirmed: 3,
            ..ResidualStateStats::default()
        };
        let released = scheduler.release_completion(
            CompletedActivation {
                activation: ActivationId(1),
                return_to: DeltaReturn::Stable {
                    desc: successor,
                    parent: Vec::new().into_boxed_slice(),
                    set_admit_result,
                },
                effect: DeltaCompletion::Candidates(result),
            },
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(released.continuation.is_none());
        let mut active = released
            .active
            .expect("segmented result opened bounded SET admission");
        assert!(stable.is_empty(), "no partial relation may re-enter Candidate");
        assert_eq!(
            scheduler.interner.program(active.state),
            Some(&ProgramAddress::Engine(EngineProgramKind::SetAdmit))
        );
        let activation = scheduler
            .registry
            .state
            .activations
            .get(&active.activation)
            .expect("SET-admission activation remains live");
        assert!(matches!(
            &activation.reducer,
            DeltaReducer::SetAdmit {
                output: CandidatePayload::Deferred(_)
            }
        ));
        let DeltaReturn::SetAdmission { destination, .. } = &activation.return_to else {
            panic!("SET admission lost its candidate destination")
        };
        let SetAdmissionDestination::Candidate(destination) = destination else {
            panic!("cyclic Confirm routed to a Formula destination")
        };
        assert!(destination.candidates.is_empty());

        for _ in 0..16 {
            let step = scheduler.step_active(
                &root,
                &plan,
                active,
                1,
                &mut stable,
                &mut stable_interner,
                &mut stats,
            );
            if let Some(resume) = step.resume {
                active = resume;
                continue;
            }
            assert_eq!(step.status, ActiveDeltaStatus::Yielded);
            break;
        }
        assert!(!stable.is_empty(), "SET admission failed to reach EOF");
        let StateBucket::Candidates(batch) = stable
            .values()
            .flat_map(BTreeMap::values)
            .next()
            .unwrap()
        else {
            panic!("SET admission returned a non-candidate payload")
        };
        assert!(matches!(batch.candidates, CandidatePayload::Deferred(_)));
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            vec![(0, value(2)), (0, value(1))]
        );
        assert_eq!(stats.candidates_confirmed, 3);
    }

    #[test]
    fn bounded_set_admission_structurally_splits_multi_parent_deferred_payload() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let (_, mut successor) = confirm_boundary_descs(&plan);
        successor.bound = VariableSet::new_singleton(1);
        let mut candidates = CandidatePayload::Tagged(vec![
            (0, value(1)),
            (0, value(2)),
            (0, value(1)),
            (1, value(1)),
            (1, value(1)),
        ]);
        candidates.defer_for_shared_activation(2);
        let mut scheduler = DeltaScheduler::new();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats {
            candidates_confirmed: 5,
            ..ResidualStateStats::default()
        };
        let seeded = scheduler.seed_formula_reducers(
            vec![FormulaReducerSeed::SetAdmit(SetAdmissionSeed {
                successor,
                destination: SetAdmissionDestination::Candidate(CandidateBatch {
                    parents: RowBatch {
                        rows: vec![value(10), value(11)],
                        row_count: 2,
                    },
                    candidates,
                }),
            })],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let active = seeded.active.expect("SET admission opened a Program cohort");
        let tasks = &scheduler.program_worklist[&active.state].tasks;
        assert_eq!(tasks.len(), 2);
        assert_ne!(tasks[0].activation, tasks[1].activation);
        for task in tasks {
            let activation = &scheduler.registry.state.activations[&task.activation];
            let DeltaReturn::SetAdmission { destination, .. } = &activation.return_to else {
                panic!("SET admission lost its saved destination")
            };
            assert_eq!(destination.parent_count(), 1);
        }

        for _ in 0..32 {
            if scheduler.is_empty() {
                break;
            }
            let _ = scheduler.step(
                &root,
                &plan,
                1,
                &mut stable,
                &mut stable_interner,
                &mut stats,
            );
        }
        assert!(scheduler.is_empty(), "unit grants did not drain SET admission");
        let StateBucket::Candidates(batch) = stable
            .values()
            .flat_map(BTreeMap::values)
            .next()
            .expect("admitted parents rejoined their saved successor")
        else {
            panic!("SET admission returned a non-candidate payload")
        };
        assert_eq!(batch.parents.row_count, 2);
        let mut by_parent: BTreeMap<RawInline, Vec<RawInline>> = BTreeMap::new();
        for (parent, candidate) in batch.candidates.iter() {
            by_parent
                .entry(batch.parents.rows[parent as usize])
                .or_default()
                .push(candidate);
        }
        assert_eq!(by_parent[&value(10)], vec![value(2), value(1)]);
        assert_eq!(by_parent[&value(11)], vec![value(1)]);
        assert_eq!(stats.candidates_confirmed, 5);
    }

    #[test]
    fn confirm_reducer_joins_multiple_roots_before_filtering_the_sequence() {
        let seed = value(1);
        let first = value(2);
        let second = value(3);
        let rejected = value(4);
        let mut registry = ProducerRegistry::new();
        let started = registry.start_many(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![
                    second, seed, first, rejected, second,
                ]),
            },
            stable_return(vec![value(9)]),
            [output(1, 0, false), output(5, 0, false)],
        );
        assert!(started.quiescence.is_none());
        let activation = started.activation;
        let roots = started.roots;
        assert_eq!(roots.len(), 2);

        let mut children = Vec::new();
        for ((_, root), successor) in roots
            .into_iter()
            .zip([output(2, 1, true), output(3, 1, true)])
        {
            assert_eq!(root.key.activation, activation);
            let outcome = registry.replace_traversal(root, [successor]);
            assert!(outcome.quiescence.is_none());
            children.extend(outcome.children);
        }
        let mut proof = None;
        for (_, child) in children {
            let outcome = registry.replace_traversal(child, []);
            assert!(outcome.accepted.is_empty());
            if let Some(quiescence) = outcome.quiescence {
                assert!(proof.replace(quiescence).is_none());
            }
        }

        let proof = proof.expect("last producer must prove quiescence");
        assert_eq!(proof.activation, activation);
        let completed = registry.finish(proof);
        let DeltaReturn::Stable { parent, .. } = completed.return_to else {
            panic!("confirm returned to a formula continuation")
        };
        assert_eq!(parent.as_ref(), &[value(9)]);
        assert_eq!(
            completed.effect,
            DeltaCompletion::Candidates(CandidatePayload::Values(vec![second, first, second]))
        );
    }

    #[test]
    fn source_cursor_resumes_only_after_every_page_root_retires() {
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let next = ResidualDeltaSourceCursor::After(value(9));
        let page = registry.replace_source(
            generator,
            [
                sourced_output(1, 1, 0, false),
                sourced_output(2, 2, 0, false),
            ],
            [],
            Some(next),
        );
        assert_eq!(page.roots.len(), 2);
        assert!(page.resumed_source.is_none());
        assert!(page.retired_source_page.is_none());
        assert!(page.quiescence.is_none());

        let mut roots = page.roots.into_iter();
        let (_, first) = roots.next().expect("first page root");
        let (_, second) = roots.next().expect("second page root");
        let first = registry.replace_traversal(first, []);
        assert!(first.resumed_source.is_none());
        assert!(first.retired_source_page.is_none());
        assert!(first.quiescence.is_none());

        let second = registry.replace_traversal(second, []);
        let (resumed, credit) = second
            .resumed_source
            .expect("the last root rejoins into one generator");
        assert_eq!(resumed, next);
        assert_eq!(credit.key.activation, activation);
        assert!(second.retired_source_page.is_some());
        assert!(second.quiescence.is_none());
        let state = registry
            .state
            .activations
            .get(&activation)
            .expect("live source activation");
        assert!(state.suspended_source_page.is_none());
        assert_eq!(state.live.len(), 1);
        assert_eq!(
            state.live.get(&credit.key.nonce),
            Some(&CreditKind::Generator)
        );
    }

    #[test]
    fn direct_source_effect_resumes_without_a_fake_traversal_credit() {
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let next = ResidualDeltaSourceCursor::After(value(9));
        let first = registry.replace_source(generator, [], [value(1), value(1)], Some(next));
        assert!(first.roots.is_empty());
        assert_eq!(first.raw_proposal_occurrences, 2);
        assert_eq!(first.accepted, [value(1)]);
        assert_eq!(
            first
                .retired_source_page
                .expect("the rootless page retires immediately")
                .had_stable_effect,
            true
        );
        let (cursor, generator) = first
            .resumed_source
            .expect("the next direct page resumes immediately");
        assert_eq!(cursor, next);
        assert!(first.quiescence.is_none());
        assert_eq!(
            registry
                .take_streaming_return(activation)
                .expect("the direct candidate has a streaming return")
                .effect,
            DeltaStreamingEffect::Candidates
        );

        let last = registry.replace_source(generator, [], [value(1), value(2), value(2)], None);
        assert!(last.roots.is_empty());
        assert_eq!(last.raw_proposal_occurrences, 3);
        assert_eq!(last.accepted, [value(2)]);
        assert!(last.resumed_source.is_none());
        assert!(
            last.retired_source_page
                .expect("the terminal direct page retires immediately")
                .had_stable_effect
        );
        assert_eq!(
            registry
                .take_streaming_return(activation)
                .expect("the terminal candidate has a streaming return")
                .effect,
            DeltaStreamingEffect::Candidates
        );
        assert_eq!(
            registry
                .finish(last.quiescence.expect("the last page quiesces"))
                .effect,
            DeltaCompletion::Cleanup
        );
    }

    #[test]
    fn streaming_source_set_admission_unifies_direct_and_accepting_roots_per_activation() {
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let page = registry.replace_source(
            generator,
            [sourced_output(7, 7, 0, true)],
            [value(7), value(7), value(8)],
            None,
        );
        assert_eq!(page.raw_proposal_occurrences, 4);
        assert_eq!(page.accepted, [value(7), value(8)]);
        let (_, root) = page.roots.into_iter().next().expect("one accepting root");
        let retired = registry.replace_traversal(root, []);
        assert!(retired.accepted.is_empty());
        assert_eq!(retired.quiescence.unwrap().activation, activation);

        let (sibling, sibling_generator) = registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let sibling_page = registry.replace_source(sibling_generator, [], [value(7)], None);
        assert_eq!(sibling_page.raw_proposal_occurrences, 1);
        assert_eq!(sibling_page.accepted, [value(7)]);
        assert_eq!(sibling_page.quiescence.unwrap().activation, sibling);
    }

    #[test]
    fn grouped_confirm_waits_for_all_source_pages_before_reducing() {
        let first = value(1);
        let second = value(2);
        let rejected = value(3);
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![second, first, rejected, first]),
            },
            stable_return(Vec::new()),
            Some(vec![first, second, rejected].into_boxed_slice()),
        );
        let first_page = registry.replace_source(
            generator,
            [sourced_output(1, 1, 0, true)],
            [],
            Some(ResidualDeltaSourceCursor::After(first)),
        );
        assert_eq!(first_page.accepted, vec![first]);
        let (_, first_root) = first_page.roots.into_iter().next().expect("first root");
        let first_retired = registry.replace_traversal(first_root, []);
        assert!(first_retired.quiescence.is_none());
        assert!(
            !first_retired
                .retired_source_page
                .expect("first page retired")
                .had_stable_effect,
            "grouped membership is not stable before the reducer quiesces"
        );
        let (_, generator) = first_retired
            .resumed_source
            .expect("second page cursor resumed");

        let second_page =
            registry.replace_source(generator, [sourced_output(2, 2, 0, true)], [], None);
        assert!(second_page.quiescence.is_none());
        let (_, second_root) = second_page.roots.into_iter().next().expect("second root");
        let second_retired = registry.replace_traversal(second_root, []);
        let proof = second_retired
            .quiescence
            .expect("the terminal page proves reducer quiescence");
        assert_eq!(proof.activation, activation);
        assert_eq!(
            registry.finish(proof).effect,
            DeltaCompletion::Candidates(CandidatePayload::Values(vec![second, first, first]))
        );
    }

    #[test]
    fn different_activation_cursors_share_one_canonical_source_bucket() {
        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::leaf(0, 0);
        let (first_activation, first_credit) = scheduler.registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let (second_activation, second_credit) = scheduler.registry.start_source(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
        );
        let _ = scheduler.file_source(
            desc.clone(),
            vec![SourceTask {
                activation: first_activation,
                credit: first_credit,
                cursor: ResidualDeltaSourceCursor::Start,
            }],
        );
        let _ = scheduler.file_source(
            desc,
            vec![SourceTask {
                activation: second_activation,
                credit: second_credit,
                cursor: ResidualDeltaSourceCursor::After(value(7)),
            }],
        );

        assert_eq!(scheduler.interner.entries.len(), 1);
        assert_eq!(scheduler.source_worklist.len(), 1);
        let tasks = &scheduler
            .source_worklist
            .first_key_value()
            .expect("one canonical source bucket")
            .1
            .tasks;
        assert_eq!(tasks.len(), 2);
        assert_ne!(tasks[0].activation, tasks[1].activation);
        assert_eq!(tasks[0].cursor, ResidualDeltaSourceCursor::Start);
        assert_eq!(tasks[1].cursor, ResidualDeltaSourceCursor::After(value(7)));
    }

    #[test]
    fn source_cohort_preserves_multiple_zero_column_affine_rows() {
        let trace = Arc::new(Mutex::new(None));
        let root = ZeroColumnSource {
            trace: Arc::clone(&trace),
        };
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.seed_source_proposals(
            DeltaDesc::leaf(0, 0),
            StateDesc {
                bound: VariableSet::new_empty(),
                phase: ResidualPhase::Ready,
            },
            RowBatch {
                rows: Vec::new(),
                row_count: 2,
            },
        );
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let outcome = scheduler.step(
            &root,
            &plan,
            2,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(
            *trace.lock().expect("zero-column trace poisoned"),
            Some(ZeroColumnBatchTrace {
                row_count: 2,
                vars: Vec::new(),
                candidate_modes: vec![false, false],
                cursors: vec![
                    ResidualDeltaSourceCursor::Start,
                    ResidualDeltaSourceCursor::Start,
                ],
                limits: vec![1, 1],
            })
        );
        assert_eq!(outcome.dead_pages, 2);
        assert!(outcome.continuation.is_none());
        assert_eq!(stats.delta_source_cohorts, 1);
        assert_eq!(stats.max_delta_source_cohort, 2);
        assert_eq!(stats.delta_source_pages, 2);
        assert_eq!(stats.delta_source_candidates_examined, 0);
        assert!(scheduler.is_empty());
        assert!(stable.is_empty());
    }

    #[test]
    fn source_pop_cohorts_by_physical_shape_without_refining_delta_identity() {
        fn return_with_bound(bound: VariableSet, parent: RawInline) -> DeltaReturn {
            DeltaReturn::Stable {
                desc: StateDesc {
                    bound,
                    phase: ResidualPhase::Ready,
                },
                parent: vec![parent].into_boxed_slice(),
                set_admit_result: false,
            }
        }

        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::leaf(0, 0);
        let schema_a = VariableSet::new_singleton(0);
        let schema_b = VariableSet::new_singleton(1);
        let mut source_task = |bound: VariableSet,
                               candidates: Option<Box<[RawInline]>>,
                               cursor: ResidualDeltaSourceCursor,
                               parent: RawInline| {
            let reducer = if candidates.is_some() {
                DeltaReducer::Confirm {
                    original: shared_one_parent_candidates(Vec::new()),
                }
            } else {
                DeltaReducer::StreamProposal
            };
            let (activation, credit) = scheduler.registry.start_source(
                reducer,
                return_with_bound(bound, parent),
                candidates,
            );
            SourceTask {
                activation,
                credit,
                cursor,
            }
        };

        // All five activations retain one canonical DeltaDesc bucket. The two
        // final tasks alone share schema, candidate mode, and cursor family.
        let tasks = vec![
            source_task(schema_b, None, ResidualDeltaSourceCursor::Start, value(1)),
            source_task(
                schema_a,
                Some(vec![value(9)].into_boxed_slice()),
                ResidualDeltaSourceCursor::Start,
                value(2),
            ),
            source_task(
                schema_a,
                None,
                ResidualDeltaSourceCursor::After(value(3)),
                value(3),
            ),
            source_task(schema_a, None, ResidualDeltaSourceCursor::Start, value(4)),
            source_task(schema_a, None, ResidualDeltaSourceCursor::Start, value(5)),
        ];
        let _ = scheduler.file_source(desc.clone(), tasks);
        assert_eq!(scheduler.interner.entries, [DeltaStateEntry::Legacy(desc)]);
        assert_eq!(scheduler.source_worklist.len(), 1);

        let (_, compatible) = scheduler.pop_source(8);
        assert_eq!(compatible.len(), 2);
        assert!(compatible.iter().all(|task| {
            SourceDispatchKey::of(&scheduler.registry, task)
                == SourceDispatchKey {
                    bound: schema_a,
                    has_candidates: false,
                    cursor_family: SourceCursorFamily::Start,
                    physical_class: DeltaPhysicalClass::General,
                }
        }));

        let (_, after) = scheduler.pop_source(8);
        assert_eq!(after.len(), 1);
        assert_eq!(
            SourceDispatchKey::of(&scheduler.registry, &after[0]).cursor_family,
            SourceCursorFamily::After
        );
        let (_, candidate) = scheduler.pop_source(8);
        assert_eq!(candidate.len(), 1);
        assert!(SourceDispatchKey::of(&scheduler.registry, &candidate[0]).has_candidates);
        let (_, other_schema) = scheduler.pop_source(8);
        assert_eq!(other_schema.len(), 1);
        assert_eq!(
            SourceDispatchKey::of(&scheduler.registry, &other_schema[0]).bound,
            schema_b
        );
        assert!(scheduler.source_worklist.is_empty());
        assert_eq!(scheduler.interner.entries.len(), 1);
    }

    #[test]
    fn cloning_live_formula_activation_remaps_credit_and_preserves_return_payload() {
        let counter = FormulaPcId(7);
        let bound = VariableSet::new_singleton(0);
        let batch = FormulaBatch::from_proposal(
            RowBatch {
                rows: vec![value(9)],
                row_count: 1,
            },
            vec![super::super::ActivationId(11)],
            &FiniteFormulaNodeKind::Or {
                children: Box::new([]),
            },
        );
        let mut original = ProducerRegistry::new();
        let mut started = original.start_many(
            DeltaReducer::quiescent_proposal(),
            DeltaReturn::Formula {
                bound,
                counter,
                batch,
            },
            [output(7, 0, true)],
        );
        let (_, original_credit) = started.roots.pop().expect("one live root");
        let key = original_credit.key;

        let (mut cloned, mut remap) = original.deep_clone();
        let cloned_credit = remap.remove(&key).expect("clone remapped live credit");
        assert!(remap.is_empty());

        let original_proof = original
            .replace_traversal(original_credit, [])
            .quiescence
            .expect("original quiesced independently");
        let cloned_proof = cloned
            .replace_traversal(cloned_credit, [])
            .quiescence
            .expect("clone quiesced independently");
        for completed in [
            finish_registry_proposal(&mut original, original_proof),
            finish_registry_proposal(&mut cloned, cloned_proof),
        ] {
            assert_eq!(
                completed.effect,
                DeltaCompletion::Candidates(CandidatePayload::Values(vec![value(7)]))
            );
            let DeltaReturn::Formula {
                bound: returned_bound,
                counter: returned_counter,
                batch: returned_batch,
            } = completed.return_to
            else {
                panic!("formula activation returned to a stable continuation")
            };
            assert_eq!(returned_bound, bound);
            assert_eq!(returned_counter, counter);
            assert_eq!(returned_batch.parents.rows, [value(9)]);
            assert_eq!(returned_batch.parents.row_count, 1);
        }
    }

    #[test]
    fn cloning_suspended_formula_source_preserves_cursor_and_return_payload() {
        fn complete(
            registry: &mut ProducerRegistry,
            traversal: ProducerCredit,
        ) -> CompletedActivation {
            let retired = registry.replace_traversal(traversal, []);
            let (cursor, generator) = retired
                .resumed_source
                .expect("retiring the page root resumes its generator");
            assert_eq!(cursor, ResidualDeltaSourceCursor::After(value(9)));
            let terminal = registry.replace_source(generator, [], [], None);
            assert!(terminal.roots.is_empty());
            let proof = terminal
                .quiescence
                .expect("the zero-root terminal page resumes the formula return");
            finish_registry_proposal(registry, proof)
        }

        let counter = FormulaPcId(7);
        let bound = VariableSet::new_singleton(0);
        let batch = FormulaBatch::from_proposal(
            RowBatch {
                rows: vec![value(9)],
                row_count: 1,
            },
            vec![super::super::ActivationId(11)],
            &FiniteFormulaNodeKind::Or {
                children: Box::new([]),
            },
        );
        let mut original = ProducerRegistry::new();
        let (activation, generator) = original.start_source(
            DeltaReducer::quiescent_proposal(),
            DeltaReturn::Formula {
                bound,
                counter,
                batch,
            },
            None,
        );
        let page = original.replace_source(
            generator,
            [output(7, 0, true)],
            [value(3), value(3)],
            Some(ResidualDeltaSourceCursor::After(value(9))),
        );
        assert_eq!(page.roots.len(), 1);
        assert!(page.resumed_source.is_none());
        assert!(original
            .state
            .activations
            .get(&activation)
            .expect("live formula source activation")
            .suspended_source_page
            .is_some());
        let (_, original_credit) = page.roots.into_iter().next().expect("one page root");
        let key = original_credit.key;

        let (mut cloned, mut remap) = original.deep_clone();
        let cloned_credit = remap
            .remove(&key)
            .expect("clone remapped the suspended page root");
        assert!(remap.is_empty());

        for completed in [
            complete(&mut original, original_credit),
            complete(&mut cloned, cloned_credit),
        ] {
            assert_eq!(
                completed.effect,
                DeltaCompletion::Candidates(CandidatePayload::Values(vec![
                    value(3),
                    value(3),
                    value(7),
                ]))
            );
            let DeltaReturn::Formula {
                bound: returned_bound,
                counter: returned_counter,
                batch: returned_batch,
            } = completed.return_to
            else {
                panic!("formula source returned to a stable continuation")
            };
            assert_eq!(returned_bound, bound);
            assert_eq!(returned_counter, counter);
            assert_eq!(returned_batch.parents.rows, [value(9)]);
            assert_eq!(returned_batch.parents.row_count, 1);
        }
    }

    #[test]
    fn distinct_formula_return_masks_share_one_structural_delta_bucket() {
        let relevant = ChildSet::empty(1).with_inserted(0);
        let resume = FormulaOuterResume {
            variable: 0,
            occurrence: 3,
            verb: UnionVerb::Propose { relevant },
            proposer_checked: true,
        };
        let mut formula_pcs = FormulaPcInterner::default();
        let resume = formula_pcs.intern_resume(resume);
        let focus = FormulaFocus::Action {
            node: FormulaNodeId(7),
            stage: FormulaStage::Propose,
        };
        let first = formula_pcs.intern_record(
            FormulaPcRecord {
                focus: focus.clone(),
                return_to: None,
                resume,
            },
            1,
        );
        let parent = formula_pcs.intern_record(
            FormulaPcRecord {
                focus: FormulaFocus::Plan {
                    node: FormulaNodeId(5),
                    stage: FormulaStage::Propose,
                    done: ChildSet::empty(2).with_inserted(0),
                },
                return_to: None,
                resume,
            },
            1,
        );
        let return_to = formula_pcs.intern_return(FormulaReturnRecord {
            kind: FormulaReturnKind::Child,
            parent,
            child: 1,
        });
        let second = formula_pcs.intern_record(
            FormulaPcRecord {
                focus,
                return_to: Some(return_to),
                resume,
            },
            3,
        );
        assert_ne!(first, second);

        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::formula(0, 3, FormulaNodeId(7));
        for (index, counter) in [first, second].into_iter().enumerate() {
            let batch = FormulaBatch::from_proposal(
                RowBatch {
                    rows: vec![value(index as u8)],
                    row_count: 1,
                },
                vec![super::super::ActivationId(index as u64)],
                &FiniteFormulaNodeKind::Or {
                    children: Box::new([]),
                },
            );
            let started = scheduler.registry.start_many(
                DeltaReducer::quiescent_proposal(),
                DeltaReturn::Formula {
                    bound: VariableSet::new_singleton(0),
                    counter,
                    batch,
                },
                [output(index as u8, 0, false)],
            );
            let tasks = started
                .roots
                .into_iter()
                .map(|(node, credit)| DeltaTask {
                    activation: started.activation,
                    credit,
                    node,
                    cursor: ResidualDeltaExpandCursor::Start,
                })
                .collect();
            let _ = scheduler.file(desc.clone(), tasks);
        }

        assert_eq!(scheduler.interner.entries, vec![DeltaStateEntry::Legacy(desc)]);
        assert_eq!(scheduler.worklist.len(), 1);
        let bucket = scheduler.worklist.values().next().unwrap();
        assert_eq!(bucket.len(), 2);
        let counters: Vec<_> = bucket
            .iter()
            .map(|task| {
                let activation = scheduler
                    .registry
                    .state
                    .activations
                    .get(&task.activation)
                    .unwrap();
                let DeltaReturn::Formula { counter, .. } = &activation.return_to else {
                    panic!("formula task lost its formula continuation")
                };
                *counter
            })
            .collect();
        assert_eq!(counters, [first, second]);
    }
}
