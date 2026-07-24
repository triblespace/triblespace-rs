//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};

use ahash::{AHashMap, AHashSet};

use crate::query::program::insert_engine_program_state;

use super::materialize::{ProposalMaterializePhaseKind, ProposalMaterializerState};
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
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(
            batch
                .candidate_sets
                .iter()
                .all(|candidates| candidates.is_none()),
            "Confirm finalizer unexpectedly borrowed a graph candidate slice"
        );
        for (input, (mut state, &limit)) in states.drain(..).zip(batch.limits).enumerate() {
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
            assert!(
                examined > 0,
                "a nonempty Confirm finalizer made no progress"
            );
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
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (state, &limit)) in states.drain(..).zip(batch.limits).enumerate() {
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
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (mut state, &limit)) in states.drain(..).zip(batch.limits).enumerate() {
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
            assert!(
                examined > 0,
                "a nonempty Formula admission made no progress"
            );
            let resume = (state.input.remaining > 0).then_some(TypedResume::Immediate(state));
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
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        use std::ops::Bound::{Excluded, Unbounded};

        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (mut state, &limit)) in states.drain(..).zip(batch.limits).enumerate() {
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
            assert!(
                !values.is_empty(),
                "a nonempty Formula emission made no progress"
            );
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
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.limits.len());
        assert_eq!(states.len(), batch.view.len());
        assert!(batch.candidate_sets.iter().all(Option::is_none));
        for (input, (state, &limit)) in states.drain(..).zip(batch.limits).enumerate() {
            let page = state.advance(limit);
            for value in page.emitted {
                effects.direct(
                    u32::try_from(input).expect("too many proposal materializer inputs"),
                    value,
                );
            }
            effects.page(page.examined, page.next.map(TypedResume::Immediate));
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

/// Affine semantic identity of one Confirm parent.
///
/// [`StateId`] identifies a canonical reducer shape and may cohort many parent
/// rows, so it is validation metadata rather than publication identity. The
/// registry brand makes pre-clone addresses inert in a cloned query, while the
/// activation identifies the exact semantic parent without naming any
/// physical candidate occurrence.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PositiveConfirmParentId {
    brand: RegistryBrand,
    activation: ActivationId,
}

/// Semantic continuation evidence owned once by a Confirm activation.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PositivePublicationCertificate {
    fixed_denotation: bool,
    continuation: ContinuationPublicationReceipt,
    crosses_set_boundary: bool,
}

#[cfg_attr(not(test), allow(dead_code))]
impl PositivePublicationCertificate {
    fn from_confirm_transition(
        previous: &StateDesc,
        successor: &StateDesc,
        full: VariableSet,
        plan: &ResidualPlan,
        formula_pcs: &FormulaPcInterner,
    ) -> Self {
        Self {
            fixed_denotation: plan.certified_denotation,
            continuation: continuation_publication_receipt(
                previous,
                successor,
                full,
                plan,
                formula_pcs,
            ),
            crosses_set_boundary: crosses_candidate_set_boundary(
                previous,
                successor,
                plan,
                formula_pcs,
            ),
        }
    }

    fn eligible(self) -> bool {
        self.fixed_denotation
            && match self.continuation {
                ContinuationPublicationReceipt::Terminal => true,
                ContinuationPublicationReceipt::ChunkHomomorphic => self.crosses_set_boundary,
                ContinuationPublicationReceipt::Barrier => false,
            }
    }
}

/// Physical fallback feeder for one exact positive Support hedge.
struct PositiveSupportSeed<'a> {
    spec: ProgramRef<'a>,
    desc: DeltaDesc,
    request: ProgramRequest,
    route: ProgramRoute,
    support_variables: VariableSet,
    direct_terminal_full: Option<VariableSet>,
}

/// Optional positive publication attached to an exact Confirm seed.
///
/// Every eligible parent may tap its own authoritative acceptance at a real
/// replacement boundary. A separately authorized fully-bound Support hedge
/// may race that tap when the Confirm Program has not proved that retaining
/// the hedge is physically redundant.
pub(super) struct PositivePublicationSeed<'a> {
    confirm_state: StateId,
    certificate: PositivePublicationCertificate,
    support_hedge: Option<PositiveSupportSeed<'a>>,
}

impl<'a> PositivePublicationSeed<'a> {
    fn certificate(
        previous: &StateDesc,
        successor: &StateDesc,
        full: VariableSet,
        plan: &ResidualPlan,
        formula_pcs: &FormulaPcInterner,
    ) -> Option<PositivePublicationCertificate> {
        let certificate = PositivePublicationCertificate::from_confirm_transition(
            previous,
            successor,
            full,
            plan,
            formula_pcs,
        );
        certificate.eligible().then_some(certificate)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn exact_confirm_tap(
        confirm_state: StateId,
        previous: &StateDesc,
        successor: &StateDesc,
        full: VariableSet,
        plan: &ResidualPlan,
        formula_pcs: &FormulaPcInterner,
    ) -> Option<Self> {
        let certificate = Self::certificate(previous, successor, full, plan, formula_pcs)?;
        Some(Self {
            confirm_state,
            certificate,
            support_hedge: None,
        })
    }

    pub(super) fn with_support_hedge(
        mut self,
        spec: ProgramRef<'a>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        support_variables: VariableSet,
        direct_terminal_full: Option<VariableSet>,
    ) -> Self {
        assert!(
            self.support_hedge.is_none(),
            "one positive publication seed acquired two Support hedges"
        );
        self.support_hedge = Some(PositiveSupportSeed {
            spec,
            desc,
            request,
            route,
            support_variables,
            direct_terminal_full,
        });
        self
    }
}

/// Clone-safe physical custody for one exact Support child.
///
/// This payload deliberately carries no [`RegistryBrand`]: it lives inside
/// cloneable registry state, while [`ProducerRegistry::deep_clone`] rebrands
/// every live producer credit. Authority enters only when the current
/// registry consumes one of those credits and mints a
/// [`PositiveSupportWitness`].
///
/// `occurrence` identifies one member of the semantic Confirm parent's
/// immutable original bag. Publication remains value-keyed, so duplicate
/// occurrences may own distinct links while racing for one `(parent, value)`
/// ledger entry.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct PositiveSupportLink {
    child: ActivationId,
    parent: ActivationId,
    generation: u64,
    occurrence: usize,
    value: RawInline,
}

/// Affine proof that one current-registry typed Support producer reported its
/// first exact success and spent the credit carrying that receipt.
///
/// The cloneable link supplies structural provenance; the brand supplies
/// branch-local authority. This type intentionally implements neither
/// [`Clone`] nor [`Copy`] and has no constructor outside the registry.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
#[must_use = "a positive Support witness must be committed or deliberately discarded"]
struct PositiveSupportWitness {
    brand: RegistryBrand,
    link: PositiveSupportLink,
}

/// Affine proof that one exact Confirm Program replacement consumed a real
/// current-registry credit and newly accepted its frozen first candidate.
///
/// The semantic parent carries branch-local authority; the occurrence is
/// implicitly zero because no other member of the immutable bag is eligible
/// for this feeder.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
#[must_use = "an exact Confirm witness must be committed or deliberately discarded"]
struct PositiveConfirmWitness {
    parent: PositiveConfirmParentId,
    generation: u64,
    value: RawInline,
}

/// One semantic Terminal origin that must be introduced to the outer
/// projected-yield ledger before a batch carrying it is staged.
///
/// This is affine propagation metadata, so it is intentionally neither
/// [`Clone`] nor [`Copy`].
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
#[must_use = "a terminal origin registration must reach the yield ledger"]
pub(super) struct TerminalOriginRegistration {
    pub(super) family: StateId,
    pub(super) origin: ActivationId,
}

#[cfg_attr(not(test), allow(dead_code))]
enum PositivePublicationRoute {
    Terminal {
        origin: ActivationId,
        full: VariableSet,
        registration: Option<TerminalOriginRegistration>,
    },
    ChunkHomomorphic,
}

/// Affine authority to release one value whose positive publication has
/// already won its semantic parent's SET ledger.
#[cfg_attr(not(test), allow(dead_code))]
#[must_use = "a committed positive publication must be released exactly once"]
struct PositivePublicationGrant {
    value: RawInline,
    /// Cloned only from the authoritative Confirm activation after every
    /// release precondition has passed.
    return_to: DeltaReturn,
    route: PositivePublicationRoute,
    source: PositivePublicationSource,
}

/// Parent-local conservation ledger for demand-bounded Support speculation.
///
/// Demand (`D`) starts the hedge and exact Confirm work (`C`) may add credit
/// only after that point. Runnable Support work reserves credit before its
/// opaque Program handle crosses the dispatch boundary, then settles the
/// reservation against the validated examined count (`S`). Every other unit
/// is eventually retired when the hedge or its semantic parent closes.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PositiveSupportWorkBudget {
    started: bool,
    demand_minted: usize,
    exact_minted: usize,
    reserved: usize,
    spent: usize,
    retired: usize,
}

impl PositiveSupportWorkBudget {
    fn minted(&self) -> usize {
        self.demand_minted
            .checked_add(self.exact_minted)
            .expect("positive Support minted-work count overflow")
    }

    fn available(&self) -> usize {
        let accounted = self
            .reserved
            .checked_add(self.spent)
            .and_then(|value| value.checked_add(self.retired))
            .expect("positive Support accounted-work count overflow");
        self.minted()
            .checked_sub(accounted)
            .expect("positive Support work ledger overspent its minted credit")
    }

    fn assert_conservation(&self) {
        assert_eq!(
            self.minted(),
            self.available()
                .checked_add(self.reserved)
                .and_then(|value| value.checked_add(self.spent))
                .and_then(|value| value.checked_add(self.retired))
                .expect("positive Support conservation count overflow"),
            "positive Support work ledger violated D + C = available + reserved + S + retired"
        );
    }

    fn mint_demand(&mut self) {
        self.started = true;
        self.demand_minted = self
            .demand_minted
            .checked_add(1)
            .expect("positive Support demand credit overflow");
        self.assert_conservation();
    }

    fn mint_exact(&mut self, examined: usize) -> usize {
        if !self.started || examined == 0 {
            return 0;
        }
        self.exact_minted = self
            .exact_minted
            .checked_add(examined)
            .expect("positive Support exact-work credit overflow");
        self.assert_conservation();
        examined
    }

    fn reserve(&mut self, requested: usize) -> usize {
        let granted = self.available().min(requested);
        self.reserved = self
            .reserved
            .checked_add(granted)
            .expect("positive Support reserved-work count overflow");
        self.assert_conservation();
        granted
    }

    fn settle(&mut self, granted: usize, examined: usize) {
        assert!(
            examined <= granted,
            "positive Support Program examined beyond its affine work grant"
        );
        self.reserved = self
            .reserved
            .checked_sub(granted)
            .expect("positive Support settled an unknown work reservation");
        self.spent = self
            .spent
            .checked_add(examined)
            .expect("positive Support spent-work count overflow");
        self.assert_conservation();
    }

    fn retire_available(&mut self) -> usize {
        assert_eq!(
            self.reserved, 0,
            "positive Support allowance retired across a live dispatch reservation"
        );
        let retired = self.available();
        self.retired = self
            .retired
            .checked_add(retired)
            .expect("positive Support retired-work count overflow");
        self.assert_conservation();
        retired
    }
}

/// Affine reservation carried beside one selected PositiveSupport Program
/// task. It intentionally implements neither [`Clone`] nor [`Copy`].
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
#[must_use = "a positive Support work grant must be settled exactly once"]
struct PositiveSupportWorkGrant {
    brand: RegistryBrand,
    parent: ActivationId,
    child: ActivationId,
    generation: u64,
    granted: usize,
    accounting: PositiveSupportWorkAccounting,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositiveSupportWorkAccounting {
    CountCredit,
    GlobalServiceDebt,
}

/// Query-global custody of the experimental PositiveSupport service lease.
///
/// This remains separate from [`PositiveSupportWorkBudget`]: the production
/// count-credit policy and the experimental service-debt policy never share
/// admission currency.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositiveSupportServiceLease {
    Dormant,
    /// The query-global ledger has started, but no semantic parent is live.
    Idle,
    Parked,
    Reserved,
}

/// Non-cloneable authority for one selected service packet part.
///
/// `nonce` identifies the one coalesced query-global packet while
/// `part_nonce` distinguishes overlapping shard receipts within it. Shared
/// reservations live in a runtime guard that abandons the entire packet when
/// any part is dropped unsettled. A dropped Direct reservation instead leaves
/// its ledger permanently `Reserved`: both modes fail closed, so no later
/// packet, split, or deep clone can cross a missing charge.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
#[must_use = "a service packet part must be settled exactly once"]
struct PositiveSupportPacketReservation {
    brand: RegistryBrand,
    nonce: u64,
    lane: ProgramServiceLane,
    part_nonce: u64,
}

/// Experimental query-global service-debt account for PositiveSupport.
///
/// The first demand starts one epoch and authorizes one initial Support
/// packet. Empty parent turnover parks the same brand and cumulative debt in
/// [`PositiveSupportServiceLease::Idle`]; later parents resume that ledger
/// without another epoch or bypass. Once the first packet settles, Support is
/// admissible exactly while its attributed service is strictly behind Exact
/// service; Exact therefore owns every tie.
///
/// One scalar account proves an aggregate one-Support-packet overshoot, but it
/// deliberately cannot represent opposite parent-local debts.  A future
/// runtime would need a separate parent-fairness policy if local latency
/// matters. This opt-in policy changes only physical scheduling and has no
/// effect on raw projected tuple SET semantics.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Eq, PartialEq)]
struct PositiveSupportServiceDebtLedger {
    brand: RegistryBrand,
    lease: PositiveSupportServiceLease,
    initial_bypass_spent: bool,
    exact_service: u64,
    support_service: u64,
    max_exact_packet: u64,
    max_support_packet: u64,
    next_packet_nonce: u64,
    active_packet_nonce: Option<u64>,
    attribution_tainted: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum PositiveSupportScheduling {
    #[default]
    CountCredit,
    GlobalServiceDebt,
}

#[cfg_attr(not(test), allow(dead_code))]
impl PositiveSupportServiceDebtLedger {
    fn dormant() -> Self {
        let ledger = Self {
            brand: RegistryBrand::fresh(),
            lease: PositiveSupportServiceLease::Dormant,
            initial_bypass_spent: false,
            exact_service: 0,
            support_service: 0,
            max_exact_packet: 0,
            max_support_packet: 0,
            next_packet_nonce: 0,
            active_packet_nonce: None,
            attribution_tainted: false,
        };
        ledger.assert_affine_custody();
        ledger
    }

    /// Starts the one query-global epoch if it has not started already.
    ///
    /// The Boolean result is telemetry only: admission depends on the
    /// persistent epoch state, not on the number of demand arrivals.
    fn demand_arrived(&mut self) -> bool {
        let started = match self.lease {
            PositiveSupportServiceLease::Dormant => true,
            PositiveSupportServiceLease::Idle | PositiveSupportServiceLease::Parked => false,
            // A different residual shard may discover a new service parent
            // while this query's one attributable packet is in flight. The
            // packet retains the affine lease; its eventual settlement will
            // observe query-global shard liveness and return to Parked.
            PositiveSupportServiceLease::Reserved => return false,
        };
        self.lease = PositiveSupportServiceLease::Parked;
        self.assert_affine_custody();
        started
    }

    fn support_is_admissible(&self, support_ready: bool) -> bool {
        self.assert_affine_custody();
        support_ready
            && self.lease == PositiveSupportServiceLease::Parked
            && !self.attribution_tainted
            && (!self.initial_bypass_spent || self.support_service < self.exact_service)
    }

    fn reserve_support(&mut self, support_ready: bool) -> Option<PositiveSupportPacketReservation> {
        if !self.support_is_admissible(support_ready) {
            return None;
        }
        self.reserve_lane(ProgramServiceLane::Support)
    }

    fn reserve_exact(&mut self) -> Option<PositiveSupportPacketReservation> {
        if self.next_lane() != Some(ProgramServiceLane::Exact) {
            return None;
        }
        self.reserve_lane(ProgramServiceLane::Exact)
    }

    fn next_lane(&self) -> Option<ProgramServiceLane> {
        self.assert_affine_custody();
        if self.lease != PositiveSupportServiceLease::Parked {
            return None;
        }
        if !self.attribution_tainted
            && (!self.initial_bypass_spent || self.support_service < self.exact_service)
        {
            Some(ProgramServiceLane::Support)
        } else {
            Some(ProgramServiceLane::Exact)
        }
    }

    fn reserve_lane(
        &mut self,
        lane: ProgramServiceLane,
    ) -> Option<PositiveSupportPacketReservation> {
        if self.next_lane() != Some(lane) {
            return None;
        }
        self.initial_bypass_spent = true;
        let nonce = self.next_packet_nonce;
        self.next_packet_nonce = self
            .next_packet_nonce
            .checked_add(1)
            .expect("positive Support service packet nonce overflow");
        self.active_packet_nonce = Some(nonce);
        self.lease = PositiveSupportServiceLease::Reserved;
        self.assert_affine_custody();
        Some(PositiveSupportPacketReservation {
            brand: self.brand,
            nonce,
            lane,
            part_nonce: 0,
        })
    }

    fn validate_settlement(
        &self,
        reservation: &PositiveSupportPacketReservation,
        lane: ProgramServiceLane,
    ) {
        assert_eq!(
            reservation.brand, self.brand,
            "service reservation belongs to another query branch"
        );
        assert_eq!(
            reservation.lane, lane,
            "service reservation crossed attributable lanes"
        );
        assert_eq!(
            self.active_packet_nonce,
            Some(reservation.nonce),
            "service settled an unknown packet reservation"
        );
        assert!(
            matches!(self.lease, PositiveSupportServiceLease::Reserved),
            "service settled without affine packet custody"
        );
    }

    fn finish_settlement(&mut self, remains_live: bool) {
        self.active_packet_nonce = None;
        self.lease = if remains_live {
            PositiveSupportServiceLease::Parked
        } else {
            PositiveSupportServiceLease::Idle
        };
        self.assert_affine_custody();
        self.assert_packet_bounds_if_attributed();
    }

    fn settle_support(
        &mut self,
        reservation: &PositiveSupportPacketReservation,
        service: u64,
        remains_live: bool,
    ) {
        assert!(
            service > 0,
            "Support service packets must have positive cost"
        );
        self.validate_settlement(reservation, ProgramServiceLane::Support);

        self.support_service = self
            .support_service
            .checked_add(service)
            .expect("positive Support cumulative service overflow");
        self.max_support_packet = self.max_support_packet.max(service);
        self.finish_settlement(remains_live);
    }

    fn settle_exact(
        &mut self,
        reservation: &PositiveSupportPacketReservation,
        service: u64,
        remains_live: bool,
    ) {
        assert!(service > 0, "Exact service packets must have positive cost");
        self.validate_settlement(reservation, ProgramServiceLane::Exact);

        self.exact_service = self
            .exact_service
            .checked_add(service)
            .expect("positive Exact cumulative service overflow");
        self.max_exact_packet = self.max_exact_packet.max(service);
        self.finish_settlement(remains_live);
    }

    /// Abandons an unsettled packet without ever reopening attributable
    /// admission. This is the unwind-only fail-closed transition used by the
    /// shared parallel coordinator.
    fn abandon_packet(
        &mut self,
        reservation: &PositiveSupportPacketReservation,
        remains_live: bool,
    ) {
        self.validate_settlement(reservation, reservation.lane);
        self.active_packet_nonce = None;
        self.attribution_tainted = true;
        self.lease = if remains_live {
            PositiveSupportServiceLease::Parked
        } else {
            PositiveSupportServiceLease::Idle
        };
        self.assert_affine_custody();
    }

    /// Permanently invalidates this epoch's service proof.
    ///
    /// The missing charge may reverse which lane is behind, so demand cannot
    /// reset this flag and no further Support packet is admitted from these
    /// totals.  A weaker scheduler may still complete the exact spine.
    fn taint_unattributed(&mut self) {
        assert!(
            !matches!(self.lease, PositiveSupportServiceLease::Reserved),
            "service attribution cannot be lost across an unsettled Support receipt"
        );
        assert_ne!(
            self.lease,
            PositiveSupportServiceLease::Dormant,
            "an unstarted PositiveSupport epoch cannot lose attribution"
        );
        self.attribution_tainted = true;
        self.assert_affine_custody();
    }

    /// Deep-clones only a quiescent affine component and rebrands it.
    fn try_deep_clone(&self) -> Option<Self> {
        self.assert_affine_custody();
        if self.active_packet_nonce.is_some() {
            return None;
        }
        Some(Self {
            brand: RegistryBrand::fresh(),
            lease: self.lease,
            initial_bypass_spent: self.initial_bypass_spent,
            exact_service: self.exact_service,
            support_service: self.support_service,
            max_exact_packet: self.max_exact_packet,
            max_support_packet: self.max_support_packet,
            next_packet_nonce: self.next_packet_nonce,
            active_packet_nonce: None,
            attribution_tainted: self.attribution_tainted,
        })
    }

    /// Reconciles a quiescent affine ledger with the live custody owned by one
    /// independent scheduler.
    ///
    /// This is used both when a semantic clone receives a fresh brand and when
    /// the first real Rayon split promotes the direct ledger into a shared
    /// coordinator. Neither boundary may cross an in-flight packet.
    fn reconcile_quiescent_liveness(&mut self, live: bool) -> bool {
        self.assert_affine_custody();
        if self.active_packet_nonce.is_some() {
            return false;
        }
        self.lease = match (self.lease, live) {
            (PositiveSupportServiceLease::Dormant, false) => PositiveSupportServiceLease::Dormant,
            (PositiveSupportServiceLease::Dormant, true) => return false,
            (_, true) => PositiveSupportServiceLease::Parked,
            (_, false) => PositiveSupportServiceLease::Idle,
        };
        self.assert_affine_custody();
        true
    }

    fn try_deep_clone_for_liveness(&self, live: bool) -> Option<Self> {
        let mut ledger = self.try_deep_clone()?;
        ledger.reconcile_quiescent_liveness(live).then_some(ledger)
    }

    fn assert_affine_custody(&self) {
        let reservation_live = matches!(self.lease, PositiveSupportServiceLease::Reserved);
        assert_eq!(
            reservation_live,
            self.active_packet_nonce.is_some(),
            "PositiveSupport lease and packet reservation custody diverged"
        );
        if self.lease == PositiveSupportServiceLease::Dormant {
            assert!(
                !self.initial_bypass_spent
                    && self.exact_service == 0
                    && self.support_service == 0
                    && self.max_exact_packet == 0
                    && self.max_support_packet == 0
                    && !self.attribution_tainted,
                "dormant PositiveSupport epoch retained service history"
            );
        }
        assert!(
            self.support_service == 0 || self.initial_bypass_spent,
            "Support service appeared before the initial affine bypass"
        );
        assert_eq!(
            self.exact_service == 0,
            self.max_exact_packet == 0,
            "Exact service total and maximum packet presence diverged"
        );
        assert_eq!(
            self.support_service == 0,
            self.max_support_packet == 0,
            "Support service total and maximum packet presence diverged"
        );
    }

    fn assert_packet_bounds_if_attributed(&self) {
        if self.attribution_tainted {
            return;
        }
        self.assert_packet_bounds();
    }

    fn assert_packet_bounds(&self) {
        assert!(
            !self.attribution_tainted,
            "an unattributed packet permanently tainted this service epoch"
        );
        assert!(
            self.support_service
                <= self
                    .exact_service
                    .checked_add(self.max_support_packet)
                    .expect("Support service packet bound overflow"),
            "Support crossed the query-global one-packet overshoot"
        );
        if self.lease != PositiveSupportServiceLease::Dormant {
            assert!(
                self.exact_service
                    <= self
                        .support_service
                        .checked_add(self.max_exact_packet)
                        .expect("Exact service packet bound overflow"),
                "Exact crossed one packet ahead while Support remained live"
            );
        }
    }
}

/// One query-run service account shared by every residual Rayon shard.
///
/// The scalar debt ledger and packet are global. Overlapping shards join one
/// coalesced packet in the selected lane, contribute one affine part receipt,
/// and charge the ledger only when its final part settles. Each scheduler
/// keeps its own registry, Program runtimes, and queues, and owns one exact
/// registration. Packet guards retain that registration through settlement,
/// while its final [`Arc`] drop removes live custody under the coordinator
/// mutex. Quiescent liveness is therefore an exact count rather than a scan of
/// weak slots.
struct PositiveSupportGlobalServiceCoordinator {
    ledger: PositiveSupportServiceDebtLedger,
    active_packet: Option<PositiveSupportActiveGlobalPacket>,
    next_shard_id: u64,
    live_shards: usize,
    abandoned: bool,
}

struct PositiveSupportActiveGlobalPacket {
    nonce: u64,
    lane: ProgramServiceLane,
    admission_horizon: u64,
    admitted_shards: AHashSet<u64>,
    next_part_nonce: u64,
    live_parts: AHashSet<u64>,
    service: u64,
}

enum PositiveSupportGlobalPartReservation {
    Deferred(u64),
    Reserved(PositiveSupportPacketReservation),
}

impl PositiveSupportGlobalServiceCoordinator {
    fn global_live(&self) -> bool {
        self.live_shards > 0
    }

    fn update_liveness(
        &mut self,
        registration: &PositiveSupportGlobalShardRegistration,
        live: bool,
    ) {
        let was_live = registration.live.load(Ordering::Acquire);
        if was_live == live {
            // The coordinator already serializes this registration. Avoid
            // turning steady-state packet traffic into an atomic RMW stream.
            self.assert_liveness_bound();
            return;
        }
        registration.live.store(live, Ordering::Release);
        match (was_live, live) {
            (false, true) => {
                self.live_shards = self
                    .live_shards
                    .checked_add(1)
                    .expect("PositiveSupport live shard count overflow");
            }
            (true, false) => {
                self.live_shards = self
                    .live_shards
                    .checked_sub(1)
                    .expect("PositiveSupport live shard count underflow");
            }
            _ => {}
        }
        self.assert_liveness_bound();
    }

    fn assert_liveness_bound(&self) {
        assert!(
            u64::try_from(self.live_shards)
                .is_ok_and(|live_shards| live_shards <= self.next_shard_id),
            "PositiveSupport live shard count crossed its monotone admission horizon"
        );
    }

    fn assert_global_state(&self) {
        self.assert_liveness_bound();
        assert_eq!(
            self.active_packet.is_some(),
            self.ledger.active_packet_nonce.is_some(),
            "coalesced packet and affine ledger custody diverged"
        );
        if self.active_packet.is_some() {
            assert_eq!(
                self.ledger.lease,
                PositiveSupportServiceLease::Reserved,
                "an active coalesced packet lost its reserved lease"
            );
            return;
        }
        match self.ledger.lease {
            PositiveSupportServiceLease::Dormant => {
                assert_eq!(
                    self.live_shards, 0,
                    "a dormant PositiveSupport epoch retained live shard custody"
                );
            }
            PositiveSupportServiceLease::Idle => {
                assert_eq!(
                    self.live_shards, 0,
                    "an idle PositiveSupport lease retained live shard custody"
                );
            }
            PositiveSupportServiceLease::Parked => {
                assert!(
                    self.live_shards > 0,
                    "a parked PositiveSupport lease lost all live shard custody"
                );
            }
            PositiveSupportServiceLease::Reserved => {
                panic!("a reserved PositiveSupport lease lost its active packet")
            }
        }
    }

    fn reconcile_idle_lease(&mut self) -> bool {
        assert_eq!(
            self.active_packet.is_some(),
            self.ledger.active_packet_nonce.is_some(),
            "coalesced packet and affine ledger custody diverged"
        );
        if self.active_packet.is_some() {
            assert_eq!(
                self.ledger.lease,
                PositiveSupportServiceLease::Reserved,
                "an active coalesced packet lost its reserved lease"
            );
            // Liveness cannot change an already-reserved lease. The exact
            // count is nevertheless maintained so its final settlement or
            // abandonment chooses Parked versus Idle without a read-side
            // registry scan.
            self.assert_global_state();
            return self.global_live();
        }

        let live = self.global_live();
        match (self.ledger.lease, live) {
            (PositiveSupportServiceLease::Parked, false) => {
                self.ledger.lease = PositiveSupportServiceLease::Idle;
            }
            (PositiveSupportServiceLease::Idle, true) => {
                self.ledger.lease = PositiveSupportServiceLease::Parked;
            }
            _ => {}
        }
        self.ledger.assert_affine_custody();
        self.assert_global_state();
        live
    }

    fn reserve_part(&mut self, shard_id: u64) -> PositiveSupportGlobalPartReservation {
        if let Some(packet) = self.active_packet.as_mut() {
            assert_eq!(
                self.ledger.active_packet_nonce,
                Some(packet.nonce),
                "coalesced packet lost its affine ledger reservation"
            );
            if shard_id >= packet.admission_horizon || !packet.admitted_shards.insert(shard_id) {
                return PositiveSupportGlobalPartReservation::Deferred(packet.nonce);
            }
            let part_nonce = packet.next_part_nonce;
            packet.next_part_nonce = packet
                .next_part_nonce
                .checked_add(1)
                .expect("positive Support service packet part nonce overflow");
            assert!(
                packet.live_parts.insert(part_nonce),
                "coalesced packet reused a live part nonce"
            );
            return PositiveSupportGlobalPartReservation::Reserved(
                PositiveSupportPacketReservation {
                    brand: self.ledger.brand,
                    nonce: packet.nonce,
                    lane: packet.lane,
                    part_nonce,
                },
            );
        }

        let lane = self
            .ledger
            .next_lane()
            .expect("a live service query had no attributable lane");
        let reservation = match lane {
            ProgramServiceLane::Exact => self.ledger.reserve_exact(),
            ProgramServiceLane::Support => self.ledger.reserve_support(true),
            ProgramServiceLane::Neutral => unreachable!(),
        }
        .expect("selected global service lane failed to reserve its affine packet");
        let admission_horizon = self.next_shard_id;
        assert!(
            shard_id < admission_horizon,
            "global service shard was not registered before packet admission"
        );
        let mut admitted_shards = AHashSet::new();
        assert!(admitted_shards.insert(shard_id));
        let mut live_parts = AHashSet::new();
        assert!(live_parts.insert(reservation.part_nonce));
        self.active_packet = Some(PositiveSupportActiveGlobalPacket {
            nonce: reservation.nonce,
            lane,
            admission_horizon,
            admitted_shards,
            next_part_nonce: reservation
                .part_nonce
                .checked_add(1)
                .expect("positive Support service packet part nonce overflow"),
            live_parts,
            service: 0,
        });
        PositiveSupportGlobalPartReservation::Reserved(reservation)
    }

    fn settle_part(
        &mut self,
        reservation: &PositiveSupportPacketReservation,
        service: u64,
    ) -> PositiveSupportGlobalSettlement {
        assert!(
            service > 0,
            "coalesced global service packet parts must have positive cost"
        );
        assert!(
            !self.abandoned,
            "PositiveSupport global service coordinator was abandoned"
        );
        assert_eq!(
            reservation.brand, self.ledger.brand,
            "service packet part belongs to another query branch"
        );

        let packet = self
            .active_packet
            .as_mut()
            .expect("service packet part settled without an active packet");
        assert_eq!(
            (reservation.nonce, reservation.lane),
            (packet.nonce, packet.lane),
            "service packet part crossed its coalesced packet"
        );
        assert!(
            packet.live_parts.remove(&reservation.part_nonce),
            "service packet part settled an unknown affine receipt"
        );
        packet.service = packet
            .service
            .checked_add(service)
            .expect("coalesced positive Support packet service overflow");
        let lane = packet.lane;
        if !packet.live_parts.is_empty() {
            return PositiveSupportGlobalSettlement {
                lane,
                packet_nonce: packet.nonce,
                closed_packet: None,
            };
        }

        let packet = self
            .active_packet
            .take()
            .expect("last service packet part lost its coalesced packet");
        let remains_live = self.global_live();
        let prior_max_packet = match packet.lane {
            ProgramServiceLane::Exact => {
                let prior = self.ledger.max_exact_packet;
                self.ledger
                    .settle_exact(reservation, packet.service, remains_live);
                prior
            }
            ProgramServiceLane::Support => {
                let prior = self.ledger.max_support_packet;
                self.ledger
                    .settle_support(reservation, packet.service, remains_live);
                prior
            }
            ProgramServiceLane::Neutral => unreachable!(),
        };
        PositiveSupportGlobalSettlement {
            lane,
            packet_nonce: packet.nonce,
            closed_packet: Some(PositiveSupportGlobalPacketClosure {
                service: packet.service,
                prior_max_packet,
            }),
        }
    }

    fn abandon_part(&mut self, reservation: &PositiveSupportPacketReservation) {
        if self.abandoned {
            return;
        }
        assert_eq!(
            reservation.brand, self.ledger.brand,
            "abandoned service packet part belongs to another query branch"
        );
        let packet = self
            .active_packet
            .as_ref()
            .expect("abandoned service packet part lost its coalesced packet");
        assert_eq!(
            (reservation.nonce, reservation.lane),
            (packet.nonce, packet.lane),
            "abandoned service packet part crossed its coalesced packet"
        );
        assert!(
            packet.live_parts.contains(&reservation.part_nonce),
            "abandoned an unknown service packet part"
        );

        self.active_packet = None;
        let remains_live = self.global_live();
        self.ledger.abandon_packet(reservation, remains_live);
        self.abandoned = true;
        self.assert_global_state();
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositiveSupportGlobalPublishedPhase {
    Dormant,
    Idle,
    Parked,
    Reserved,
    Abandoned,
}

impl PositiveSupportGlobalPublishedPhase {
    fn from_ledger(lease: PositiveSupportServiceLease) -> Self {
        match lease {
            PositiveSupportServiceLease::Dormant => Self::Dormant,
            PositiveSupportServiceLease::Idle => Self::Idle,
            PositiveSupportServiceLease::Parked => Self::Parked,
            PositiveSupportServiceLease::Reserved => Self::Reserved,
        }
    }

    fn from_raw(raw: u8) -> Self {
        match raw {
            raw if raw == Self::Dormant as u8 => Self::Dormant,
            raw if raw == Self::Idle as u8 => Self::Idle,
            raw if raw == Self::Parked as u8 => Self::Parked,
            raw if raw == Self::Reserved as u8 => Self::Reserved,
            raw if raw == Self::Abandoned as u8 => Self::Abandoned,
            _ => panic!("PositiveSupport published an unknown global service phase"),
        }
    }
}

/// Shared phase of one query-run service account.
///
/// It exists only after a real Rayon split. `parallel_sibling` registers a new
/// exact shard lifetime in the same coordinator; semantic cloning extracts a
/// fresh direct ledger instead of retaining this sharing topology.
struct PositiveSupportGlobalServiceShared {
    coordinator: Mutex<PositiveSupportGlobalServiceCoordinator>,
    /// Lock-free executor wake hint. Packet authority remains entirely under
    /// `coordinator`; this mirror only lets waiters observe that their exact
    /// monotonically increasing packet nonce is no longer active.
    active_packet_nonce: AtomicU64,
    /// Lock-free quiescent scheduling hint. `Reserved` deliberately sends
    /// readers through `coordinator`, preserving the V1 contention window in
    /// which overlapping shards coalesce into the active packet. Every store
    /// occurs while that mutex is held.
    published_phase: AtomicU8,
}

impl PositiveSupportGlobalServiceShared {
    fn authoritative_phase(
        coordinator: &PositiveSupportGlobalServiceCoordinator,
    ) -> PositiveSupportGlobalPublishedPhase {
        if coordinator.abandoned {
            PositiveSupportGlobalPublishedPhase::Abandoned
        } else {
            PositiveSupportGlobalPublishedPhase::from_ledger(coordinator.ledger.lease)
        }
    }

    fn publish_phase(&self, coordinator: &PositiveSupportGlobalServiceCoordinator) {
        coordinator.assert_global_state();
        let phase = Self::authoritative_phase(coordinator) as u8;
        if self.published_phase.load(Ordering::Relaxed) != phase {
            // Writers are serialized by `coordinator`, and same-phase stores
            // convey no new lock-free authority. Publish only real phase
            // transitions so Reserved packet traffic does not bounce the
            // cache line read by quiescent probes.
            self.published_phase.store(phase, Ordering::Release);
        }
    }

    fn load_phase(&self) -> PositiveSupportGlobalPublishedPhase {
        PositiveSupportGlobalPublishedPhase::from_raw(self.published_phase.load(Ordering::Acquire))
    }
}

/// Exact lifetime and liveness registration for one residual scheduler shard.
///
/// A runtime and every packet guard it mints share this allocation. Its final
/// [`Arc`] drop is the precise shard-retirement boundary. The weak back-link
/// avoids making the shared coordinator own its registrations.
struct PositiveSupportGlobalShardRegistration {
    shared: Weak<PositiveSupportGlobalServiceShared>,
    shard_id: u64,
    /// Read and written only while `shared.coordinator` is held. Atomic
    /// storage lets an unchanged-false reservation attempt return without an
    /// inner lock; real transitions still serialize through the coordinator.
    live: AtomicBool,
}

impl Drop for PositiveSupportGlobalShardRegistration {
    fn drop(&mut self) {
        if !self.live.load(Ordering::Acquire) {
            // Final Arc ownership proves that no concurrent transition can
            // make this false registration live. It owns no exact count and
            // therefore needs no coordinator retirement boundary.
            return;
        }
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let Ok(mut coordinator) = shared.coordinator.lock() else {
            return;
        };
        coordinator.update_liveness(self, false);
        coordinator.reconcile_idle_lease();
        shared.publish_phase(&coordinator);
    }
}

/// `PositiveSupportServiceDebtLedger::reserve_lane` checks the increment before
/// publishing a reservation, so `u64::MAX` can never be an active packet nonce.
const NO_ACTIVE_GLOBAL_SERVICE_PACKET: u64 = u64::MAX;

struct PositiveSupportGlobalServiceRuntime {
    // Drop the registration while this runtime still keeps `shared` alive, so
    // its exact last-Arc retirement can always serialize through the mutex.
    registration: Arc<PositiveSupportGlobalShardRegistration>,
    shared: Arc<PositiveSupportGlobalServiceShared>,
}

#[cfg(all(test, feature = "parallel"))]
pub(super) type GlobalServiceDispatchTestProbe = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// One scheduler's service-debt account.
///
/// Serial and not-yet-split iterators stay in `Direct`, where every transition
/// is an ordinary mutable ledger operation. The first proven affine Rayon
/// split consumes that ledger into `Shared`; later siblings only register
/// another liveness slot. This keeps synchronization out of the latency path
/// without creating a second accounting epoch.
struct PositiveSupportServiceDebtRuntime {
    mode: PositiveSupportServiceDebtMode,
    #[cfg(all(test, feature = "parallel"))]
    dispatch_probe: Option<GlobalServiceDispatchTestProbe>,
}

#[cfg_attr(not(feature = "parallel"), allow(dead_code))]
enum PositiveSupportServiceDebtMode {
    Direct(PositiveSupportServiceDebtLedger),
    Shared(PositiveSupportGlobalServiceRuntime),
}

enum PositiveSupportGlobalTurn {
    Inactive,
    Deferred(u64),
    Reserved(PositiveSupportGlobalPacketGuard),
}

enum PositiveSupportServiceTurn {
    Inactive,
    Deferred(u64),
    Reserved(PositiveSupportServicePacketReservation),
}

enum PositiveSupportServicePacketReservation {
    Direct(PositiveSupportPacketReservation),
    Shared(PositiveSupportGlobalPacketGuard),
}

struct PositiveSupportGlobalPacketGuard {
    // Keep shard custody alive until settlement or abandonment has crossed
    // the coordinator boundary.
    registration: Arc<PositiveSupportGlobalShardRegistration>,
    shared: Arc<PositiveSupportGlobalServiceShared>,
    reservation: Option<PositiveSupportPacketReservation>,
}

struct PositiveSupportGlobalSettlement {
    lane: ProgramServiceLane,
    packet_nonce: u64,
    closed_packet: Option<PositiveSupportGlobalPacketClosure>,
}

struct PositiveSupportGlobalPacketClosure {
    service: u64,
    prior_max_packet: u64,
}

impl PositiveSupportGlobalServiceRuntime {
    #[cfg(test)]
    fn dormant() -> Self {
        Self::from_ledger(PositiveSupportServiceDebtLedger::dormant(), false)
    }

    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    fn from_ledger(ledger: PositiveSupportServiceDebtLedger, live: bool) -> Self {
        assert!(
            ledger.active_packet_nonce.is_none(),
            "a shared service coordinator cannot inherit an in-flight direct packet"
        );
        assert_eq!(
            matches!(ledger.lease, PositiveSupportServiceLease::Parked),
            live,
            "shared service liveness disagreed with its promoted affine lease"
        );
        let published_phase = PositiveSupportGlobalPublishedPhase::from_ledger(ledger.lease) as u8;
        let shared = Arc::new(PositiveSupportGlobalServiceShared {
            coordinator: Mutex::new(PositiveSupportGlobalServiceCoordinator {
                ledger,
                active_packet: None,
                next_shard_id: 1,
                live_shards: usize::from(live),
                abandoned: false,
            }),
            active_packet_nonce: AtomicU64::new(NO_ACTIVE_GLOBAL_SERVICE_PACKET),
            published_phase: AtomicU8::new(published_phase),
        });
        let registration = Arc::new(PositiveSupportGlobalShardRegistration {
            shared: Arc::downgrade(&shared),
            shard_id: 0,
            live: AtomicBool::new(live),
        });
        shared
            .coordinator
            .lock()
            .expect("PositiveSupport global service coordinator was poisoned")
            .assert_global_state();
        Self {
            registration,
            shared,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PositiveSupportGlobalServiceCoordinator> {
        self.shared
            .coordinator
            .lock()
            .expect("PositiveSupport global service coordinator was poisoned")
    }

    fn demand_arrived(&self) -> bool {
        let mut coordinator = self.lock();
        assert!(
            !coordinator.abandoned,
            "PositiveSupport global service coordinator was abandoned"
        );
        coordinator.update_liveness(&self.registration, true);
        let started = coordinator.ledger.demand_arrived();
        coordinator.reconcile_idle_lease();
        self.shared.publish_phase(&coordinator);
        started
    }

    fn global_lane_is_active(&self) -> bool {
        match self.shared.load_phase() {
            PositiveSupportGlobalPublishedPhase::Dormant
            | PositiveSupportGlobalPublishedPhase::Idle
            | PositiveSupportGlobalPublishedPhase::Abandoned => false,
            PositiveSupportGlobalPublishedPhase::Parked => true,
            PositiveSupportGlobalPublishedPhase::Reserved => {
                // Deliberately retain V1's active-packet mutex path. Besides
                // validating current authority, its contention window gives
                // sibling reservers time to join the finite packet cohort.
                let coordinator = self.lock();
                if coordinator.abandoned {
                    return false;
                }
                coordinator.assert_global_state();
                matches!(
                    coordinator.ledger.lease,
                    PositiveSupportServiceLease::Parked | PositiveSupportServiceLease::Reserved
                )
            }
        }
    }

    #[cfg(feature = "parallel")]
    fn active_packet_nonce(&self) -> Option<u64> {
        match self.shared.active_packet_nonce.load(Ordering::Acquire) {
            NO_ACTIVE_GLOBAL_SERVICE_PACKET => None,
            nonce => Some(nonce),
        }
    }

    fn try_reserve_turn(&self, local_live: bool) -> PositiveSupportGlobalTurn {
        if !local_live && !self.registration.live.load(Ordering::Acquire) {
            // An unchanged false registration owns no global custody to
            // reconcile. Preserve the V1 no-work fast return; only a real
            // true-to-false transition must serialize and decrement the exact
            // count.
            return PositiveSupportGlobalTurn::Inactive;
        }
        let mut coordinator = self.lock();
        assert!(
            !coordinator.abandoned,
            "PositiveSupport global service coordinator was abandoned"
        );
        coordinator.update_liveness(&self.registration, local_live);
        if !local_live {
            coordinator.reconcile_idle_lease();
            self.shared.publish_phase(&coordinator);
            return PositiveSupportGlobalTurn::Inactive;
        }
        assert!(
            coordinator.reconcile_idle_lease(),
            "a live service shard disappeared from query-global liveness"
        );
        let had_active_packet = coordinator.active_packet.is_some();
        match coordinator.reserve_part(self.registration.shard_id) {
            PositiveSupportGlobalPartReservation::Deferred(packet_nonce) => {
                self.shared.publish_phase(&coordinator);
                PositiveSupportGlobalTurn::Deferred(packet_nonce)
            }
            PositiveSupportGlobalPartReservation::Reserved(reservation) => {
                if !had_active_packet {
                    // Publish only after the authoritative packet and ledger
                    // reservation exist, and before releasing the coordinator
                    // lock. Acquire readers may then wait on this exact nonce
                    // without entering the mutex.
                    self.shared
                        .active_packet_nonce
                        .store(reservation.nonce, Ordering::Release);
                } else {
                    debug_assert_eq!(
                        self.shared.active_packet_nonce.load(Ordering::Acquire),
                        reservation.nonce,
                        "active packet mirror diverged while another shard joined"
                    );
                }
                // The Release phase store follows the nonce publication:
                // observing Reserved therefore also observes its wake nonce.
                self.shared.publish_phase(&coordinator);
                drop(coordinator);
                PositiveSupportGlobalTurn::Reserved(PositiveSupportGlobalPacketGuard {
                    registration: Arc::clone(&self.registration),
                    shared: Arc::clone(&self.shared),
                    reservation: Some(reservation),
                })
            }
        }
    }

    #[cfg_attr(not(feature = "parallel"), allow(dead_code))]
    fn synchronize_local_live(&self, local_live: bool) {
        let mut coordinator = self.lock();
        assert!(
            !coordinator.abandoned,
            "PositiveSupport global service coordinator was abandoned"
        );
        coordinator.update_liveness(&self.registration, local_live);
        coordinator.reconcile_idle_lease();
        self.shared.publish_phase(&coordinator);
    }

    #[cfg_attr(not(any(test, feature = "parallel")), allow(dead_code))]
    fn parallel_sibling(&self) -> Self {
        let mut coordinator = self.lock();
        let shard_id = coordinator.next_shard_id;
        coordinator.next_shard_id = coordinator
            .next_shard_id
            .checked_add(1)
            .expect("PositiveSupport global service shard id overflow");
        coordinator.assert_liveness_bound();
        self.shared.publish_phase(&coordinator);
        drop(coordinator);
        let registration = Arc::new(PositiveSupportGlobalShardRegistration {
            shared: Arc::downgrade(&self.shared),
            shard_id,
            live: AtomicBool::new(false),
        });
        Self {
            registration,
            shared: Arc::clone(&self.shared),
        }
    }

    fn try_deep_clone_ledger(&self, local_live: bool) -> Option<PositiveSupportServiceDebtLedger> {
        let mut coordinator = self.lock();
        if coordinator.abandoned {
            return None;
        }
        coordinator.update_liveness(&self.registration, local_live);
        coordinator.reconcile_idle_lease();
        self.shared.publish_phase(&coordinator);
        assert_eq!(
            coordinator.active_packet.is_some(),
            coordinator.ledger.active_packet_nonce.is_some(),
            "coalesced packet and affine ledger custody diverged"
        );
        coordinator.ledger.try_deep_clone_for_liveness(local_live)
    }

    #[cfg(test)]
    fn snapshot(&self) -> PositiveSupportServiceDebtSnapshot {
        let coordinator = self.lock();
        PositiveSupportServiceDebtSnapshot::from(&coordinator.ledger)
    }

    #[cfg(all(test, feature = "parallel"))]
    fn live_shards(&self) -> usize {
        self.lock().live_shards
    }

    #[cfg(all(test, feature = "parallel"))]
    fn published_phase(&self) -> PositiveSupportGlobalPublishedPhase {
        self.shared.load_phase()
    }

    #[cfg(all(test, feature = "parallel"))]
    fn shares_coordinator_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    #[cfg(all(test, feature = "parallel"))]
    fn shares_liveness_slot_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registration, &other.registration)
    }
}

impl PositiveSupportServiceDebtRuntime {
    fn dormant() -> Self {
        Self {
            mode: PositiveSupportServiceDebtMode::Direct(
                PositiveSupportServiceDebtLedger::dormant(),
            ),
            #[cfg(all(test, feature = "parallel"))]
            dispatch_probe: None,
        }
    }

    fn demand_arrived(&mut self) -> bool {
        match &mut self.mode {
            PositiveSupportServiceDebtMode::Direct(ledger) => ledger.demand_arrived(),
            PositiveSupportServiceDebtMode::Shared(runtime) => runtime.demand_arrived(),
        }
    }

    fn global_lane_is_active(&self) -> bool {
        match &self.mode {
            PositiveSupportServiceDebtMode::Direct(ledger) => matches!(
                ledger.lease,
                PositiveSupportServiceLease::Parked | PositiveSupportServiceLease::Reserved
            ),
            PositiveSupportServiceDebtMode::Shared(runtime) => runtime.global_lane_is_active(),
        }
    }

    #[cfg(feature = "parallel")]
    fn active_packet_nonce(&self) -> Option<u64> {
        match &self.mode {
            // This is an executor wake/park signal, not an introspection API.
            // A Direct reservation has no sibling owner that could close it.
            PositiveSupportServiceDebtMode::Direct(_) => None,
            PositiveSupportServiceDebtMode::Shared(runtime) => runtime.active_packet_nonce(),
        }
    }

    fn try_reserve_turn(&mut self, local_live: bool) -> PositiveSupportServiceTurn {
        match &mut self.mode {
            PositiveSupportServiceDebtMode::Direct(ledger) => {
                assert!(
                    ledger.reconcile_quiescent_liveness(local_live),
                    "direct service attribution was abandoned with a packet in flight"
                );
                if !local_live {
                    return PositiveSupportServiceTurn::Inactive;
                }
                let lane = ledger
                    .next_lane()
                    .expect("a live direct service query had no attributable lane");
                let reservation = match lane {
                    ProgramServiceLane::Exact => ledger.reserve_exact(),
                    ProgramServiceLane::Support => ledger.reserve_support(true),
                    ProgramServiceLane::Neutral => unreachable!(),
                }
                .expect("selected direct service lane failed to reserve its affine packet");
                PositiveSupportServiceTurn::Reserved(
                    PositiveSupportServicePacketReservation::Direct(reservation),
                )
            }
            PositiveSupportServiceDebtMode::Shared(runtime) => {
                match runtime.try_reserve_turn(local_live) {
                    PositiveSupportGlobalTurn::Inactive => PositiveSupportServiceTurn::Inactive,
                    PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                        PositiveSupportServiceTurn::Deferred(packet_nonce)
                    }
                    PositiveSupportGlobalTurn::Reserved(guard) => {
                        PositiveSupportServiceTurn::Reserved(
                            PositiveSupportServicePacketReservation::Shared(guard),
                        )
                    }
                }
            }
        }
    }

    fn settle(
        &mut self,
        reservation: PositiveSupportServicePacketReservation,
        service: u64,
        local_live: bool,
    ) -> PositiveSupportGlobalSettlement {
        match (&mut self.mode, reservation) {
            (
                PositiveSupportServiceDebtMode::Direct(ledger),
                PositiveSupportServicePacketReservation::Direct(reservation),
            ) => {
                let prior_max_packet = match reservation.lane {
                    ProgramServiceLane::Exact => {
                        let prior = ledger.max_exact_packet;
                        ledger.settle_exact(&reservation, service, local_live);
                        prior
                    }
                    ProgramServiceLane::Support => {
                        let prior = ledger.max_support_packet;
                        ledger.settle_support(&reservation, service, local_live);
                        prior
                    }
                    ProgramServiceLane::Neutral => unreachable!(),
                };
                PositiveSupportGlobalSettlement {
                    lane: reservation.lane,
                    packet_nonce: reservation.nonce,
                    closed_packet: Some(PositiveSupportGlobalPacketClosure {
                        service,
                        prior_max_packet,
                    }),
                }
            }
            (
                PositiveSupportServiceDebtMode::Shared(_),
                PositiveSupportServicePacketReservation::Shared(guard),
            ) => guard.settle(service, local_live),
            (PositiveSupportServiceDebtMode::Direct(_), _) => {
                panic!("shared service receipt returned to a direct debt ledger")
            }
            (PositiveSupportServiceDebtMode::Shared(_), _) => {
                panic!("direct service receipt crossed the first parallel split")
            }
        }
    }

    /// Consumes one scheduler runtime into the two runtimes created by a
    /// successful affine split.
    ///
    /// Direct promotion is possible only while quiescent. The split caller has
    /// already proved that a right affine payload exists, so failed split
    /// negotiation never allocates or synchronizes this account.
    #[cfg(feature = "parallel")]
    fn into_parallel_pair(mut self, local_live: bool) -> (Self, Self) {
        let shared = match self.mode {
            PositiveSupportServiceDebtMode::Direct(mut ledger) => {
                assert!(
                    ledger.reconcile_quiescent_liveness(local_live),
                    "cannot promote a direct ledger across an in-flight service packet"
                );
                PositiveSupportGlobalServiceRuntime::from_ledger(ledger, local_live)
            }
            PositiveSupportServiceDebtMode::Shared(runtime) => {
                // The split caller has the authoritative registry view for
                // this already-shared shard. Publish it before minting the
                // new false sibling so exact global liveness cannot retain a
                // stale pre-split state.
                runtime.synchronize_local_live(local_live);
                runtime
            }
        };
        let right_shared = shared.parallel_sibling();
        #[cfg(all(test, feature = "parallel"))]
        let right_probe = self.dispatch_probe.clone();
        self.mode = PositiveSupportServiceDebtMode::Shared(shared);
        let right = Self {
            mode: PositiveSupportServiceDebtMode::Shared(right_shared),
            #[cfg(all(test, feature = "parallel"))]
            dispatch_probe: right_probe,
        };
        (self, right)
    }

    fn try_deep_clone(&self, local_live: bool) -> Option<Self> {
        let ledger = match &self.mode {
            PositiveSupportServiceDebtMode::Direct(ledger) => {
                ledger.try_deep_clone_for_liveness(local_live)?
            }
            PositiveSupportServiceDebtMode::Shared(runtime) => {
                runtime.try_deep_clone_ledger(local_live)?
            }
        };
        Some(Self {
            mode: PositiveSupportServiceDebtMode::Direct(ledger),
            #[cfg(all(test, feature = "parallel"))]
            dispatch_probe: self.dispatch_probe.clone(),
        })
    }

    #[cfg(all(test, feature = "parallel"))]
    fn install_dispatch_probe(&mut self, probe: GlobalServiceDispatchTestProbe) {
        self.dispatch_probe = Some(probe);
    }

    #[cfg(all(test, feature = "parallel"))]
    fn notify_dispatch_started(&self, reservation: &PositiveSupportServicePacketReservation) {
        let Some(probe) = &self.dispatch_probe else {
            return;
        };
        let reservation = match reservation {
            PositiveSupportServicePacketReservation::Direct(reservation) => reservation,
            PositiveSupportServicePacketReservation::Shared(guard) => guard
                .reservation
                .as_ref()
                .expect("dispatch probe observed a settled global service part"),
        };
        probe(reservation.nonce, reservation.part_nonce);
    }

    #[cfg(test)]
    fn snapshot(&self) -> PositiveSupportServiceDebtSnapshot {
        match &self.mode {
            PositiveSupportServiceDebtMode::Direct(ledger) => {
                PositiveSupportServiceDebtSnapshot::from(ledger)
            }
            PositiveSupportServiceDebtMode::Shared(runtime) => runtime.snapshot(),
        }
    }

    #[cfg(all(test, feature = "parallel"))]
    fn shares_coordinator_with(&self, other: &Self) -> bool {
        match (&self.mode, &other.mode) {
            (
                PositiveSupportServiceDebtMode::Shared(left),
                PositiveSupportServiceDebtMode::Shared(right),
            ) => left.shares_coordinator_with(right),
            _ => false,
        }
    }

    #[cfg(all(test, feature = "parallel"))]
    fn shares_liveness_slot_with(&self, other: &Self) -> bool {
        match (&self.mode, &other.mode) {
            (
                PositiveSupportServiceDebtMode::Shared(left),
                PositiveSupportServiceDebtMode::Shared(right),
            ) => left.shares_liveness_slot_with(right),
            _ => false,
        }
    }
}

impl PositiveSupportServicePacketReservation {
    fn lane(&self) -> ProgramServiceLane {
        match self {
            Self::Direct(reservation) => reservation.lane,
            Self::Shared(guard) => guard.lane(),
        }
    }
}

impl PositiveSupportGlobalPacketGuard {
    fn lane(&self) -> ProgramServiceLane {
        self.reservation
            .as_ref()
            .expect("settled global service packet lost its lane")
            .lane
    }

    fn settle(mut self, service: u64, local_live: bool) -> PositiveSupportGlobalSettlement {
        let reservation = self
            .reservation
            .as_ref()
            .expect("global service packet settled twice");
        let mut coordinator = self
            .shared
            .coordinator
            .lock()
            .expect("PositiveSupport global service coordinator was poisoned");
        assert!(
            !coordinator.abandoned,
            "PositiveSupport global service coordinator was abandoned"
        );
        coordinator.update_liveness(&self.registration, local_live);
        let settlement = coordinator.settle_part(reservation, service);
        if settlement.closed_packet.is_some() {
            // The final part has already settled the authoritative ledger
            // against the exact shard count. Publish that quiescent phase,
            // then clear the nonce before unlocking: pending-leaf handoff
            // still cannot re-park on the retired packet, while a phase
            // observer never mistakes the packet for authoritative custody.
            self.shared.publish_phase(&coordinator);
            self.shared
                .active_packet_nonce
                .store(NO_ACTIVE_GLOBAL_SERVICE_PACKET, Ordering::Release);
        } else {
            self.shared.publish_phase(&coordinator);
            debug_assert_eq!(
                self.shared.active_packet_nonce.load(Ordering::Acquire),
                reservation.nonce,
                "partial settlement cleared the active packet mirror"
            );
        }
        self.reservation.take();
        drop(coordinator);
        settlement
    }
}

impl Drop for PositiveSupportGlobalPacketGuard {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.as_ref() else {
            return;
        };
        let Ok(mut coordinator) = self.shared.coordinator.lock() else {
            return;
        };
        coordinator.abandon_part(reservation);
        // Abandonment has already removed the authoritative packet, reconciled
        // exact liveness, tainted the ledger, and marked the coordinator
        // fail-closed. Publish the terminal phase before clearing the wake
        // nonce. A poisoned lock deliberately leaves both mirrors untouched:
        // the query abort path, not a false wake, owns that failure mode.
        self.shared.publish_phase(&coordinator);
        self.shared
            .active_packet_nonce
            .store(NO_ACTIVE_GLOBAL_SERVICE_PACKET, Ordering::Release);
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PositiveSupportServiceDebtSnapshot {
    brand: RegistryBrand,
    lease: PositiveSupportServiceLease,
    initial_bypass_spent: bool,
    exact_service: u64,
    support_service: u64,
    max_exact_packet: u64,
    max_support_packet: u64,
    next_packet_nonce: u64,
    active_packet_nonce: Option<u64>,
    attribution_tainted: bool,
}

#[cfg(test)]
impl From<&PositiveSupportServiceDebtLedger> for PositiveSupportServiceDebtSnapshot {
    fn from(ledger: &PositiveSupportServiceDebtLedger) -> Self {
        Self {
            brand: ledger.brand,
            lease: ledger.lease,
            initial_bypass_spent: ledger.initial_bypass_spent,
            exact_service: ledger.exact_service,
            support_service: ledger.support_service,
            max_exact_packet: ledger.max_exact_packet,
            max_support_packet: ledger.max_support_packet,
            next_packet_nonce: ledger.next_packet_nonce,
            active_packet_nonce: ledger.active_packet_nonce,
            attribution_tainted: ledger.attribution_tainted,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PositiveExactWorkAccounting {
    paired: bool,
    credited: usize,
}

/// Dormant scheduler-owned publication state attached to the authoritative
/// Confirm activation.
///
/// The immutable original occurrence bag remains in [`DeltaReducer::Confirm`].
/// This ledger therefore records only semantic evidence, lifecycle, and the
/// relational values that won publication.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct PositivePublicationLedger {
    generation: u64,
    open: bool,
    authorization: PositivePublicationAuthorization,
    /// Canonical state is retained only to validate which Confirm reducer
    /// opened this affine parent; it is never a publication key.
    confirm_state: StateId,
    certificate: PositivePublicationCertificate,
    published: BTreeSet<RawInline>,
    /// Physical hedges linked to this semantic parent. These identities are
    /// cancellation custody only: they never participate in publication SET
    /// identity or exact Confirm completeness.
    support_children: SmallVec<[ActivationId; 1]>,
    /// Demand-bounded physical allowance shared by every linked Support child.
    support_work: PositiveSupportWorkBudget,
    /// Independent start bit for the experimental query-global service
    /// scheduler. It never mints or consumes production `D + C` credit.
    service_support_started: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositivePublicationSource {
    ExactConfirmTap,
    SupportHedge,
}

/// Source-specific authority admitted when the semantic Confirm parent opens.
///
/// Exact acceptance is inherently authoritative. Support is a separate
/// opt-in proof source; admitting it never lets either witness borrow the
/// other's provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PositivePublicationAuthorization {
    ExactOnly,
    ExactAndSupport,
}

impl PositivePublicationAuthorization {
    fn authorizes(self, source: PositivePublicationSource) -> bool {
        match (self, source) {
            (_, PositivePublicationSource::ExactConfirmTap)
            | (Self::ExactAndSupport, PositivePublicationSource::SupportHedge) => true,
            (Self::ExactOnly, PositivePublicationSource::SupportHedge) => false,
        }
    }
}

/// Boxed registration keeps the dormant activation tax to one nullable
/// pointer while retaining semantic evidence for parents that correctly own
/// no ledger.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Debug, Eq, PartialEq)]
enum PositivePublicationRegistration {
    Private {
        confirm_state: StateId,
        certificate: PositivePublicationCertificate,
    },
    Eligible(PositivePublicationLedger),
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
    Support { published: bool },
    /// A fully-bound typed Support child proving one occurrence owned by an
    /// authoritative semantic Confirm parent.
    ///
    /// The first production feeder deliberately supports only unjoined
    /// Program credits. RPQ's exact fully-bound Support routes satisfy that
    /// law; generic `AfterChildren` propagation remains outside this substrate
    /// until it can carry semantic-commit evidence through the receipt-local
    /// join.
    PositiveSupport {
        link: Box<PositiveSupportLink>,
        witnessed: bool,
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
    FinalizingConfirm { output: CandidatePayload },
    /// Proposal discovery has quiesced and transferred its sole affine credit
    /// to the engine-owned Seal/Merge/Emit normalizer.
    FinalizingProposal { output: CandidatePayload },
    /// A segmented candidate relation is being admitted by the engine-owned
    /// bounded scan/emit Program before it re-enters the stable machine.
    SetAdmit { output: CandidatePayload },
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
    /// Minimal full-bound row retained by a physical PositiveSupport child.
    ///
    /// It is source context only. The semantic Confirm parent remains the
    /// exclusive owner of B, G, P, and the ordinary Stable continuation.
    PositiveSupport {
        bound: VariableSet,
        row: Box<[RawInline]>,
    },
    SetAdmission {
        successor: StateDesc,
        destination: SetAdmissionDestination,
    },
}

/// Constructs the one authoritative physical row for a positive Support
/// child from its semantic Confirm parent's Stable Candidate return.
///
/// Reusing the ordinary candidate-commit layout law here makes a mismatched
/// `(bound, row, value)` tuple unrepresentable at the specialized opener.
fn positive_support_child_context(
    return_to: &DeltaReturn,
    value: RawInline,
) -> Option<(VariableSet, Box<[RawInline]>)> {
    let DeltaReturn::Stable { desc, parent, .. } = return_to else {
        return None;
    };
    let ResidualPhase::Candidate { variable, .. } = &desc.phase else {
        return None;
    };
    if parent.len() != desc.bound.count() {
        return None;
    }
    let (bound, rows) = committed_candidate_rows(
        desc.bound,
        *variable,
        CandidateBatch {
            parents: RowBatch {
                rows: parent.to_vec(),
                row_count: 1,
            },
            candidates: CandidatePayload::Values(vec![value]),
        },
    );
    (rows.row_count == 1 && rows.rows.len() == bound.count())
        .then(|| (bound, rows.rows.into_boxed_slice()))
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
    /// Optional boxed registration for this exact Confirm parent. Ordinary
    /// execution leaves it absent; every dormant activation pays one nullable
    /// pointer rather than an inline tree-set-bearing ledger.
    #[cfg_attr(not(test), allow(dead_code))]
    positive_publication: Option<Box<PositivePublicationRegistration>>,
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
    #[cfg_attr(not(test), allow(dead_code))]
    next_positive_generation: u64,
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
    /// Identifies the distinct PositiveSupport reducer even after its affine
    /// first-witness slot has already been spent.
    positive_support_reducer: bool,
    /// Present only for the first accepted-or-supported receipt, and only
    /// after `replace_program` consumed the current registry's real credit.
    positive_support: Option<Box<PositiveSupportWitness>>,
    /// Present only when an exact-tap Confirm replacement newly accepted its
    /// frozen B[0] and consumed the current registry's real credit.
    positive_confirm: Option<Box<PositiveConfirmWitness>>,
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
            (Self::Candidates(left), Self::Candidates(right)) => left.iter().eq(right.iter()),
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
/// exact affine origin per row. Origin is the semantic publication identity
/// for the outer projected-yield ledger; it never enters canonical residual
/// identity. Ordinary terminal streaming happens to use its physical
/// activation as that semantic origin.
#[derive(Debug)]
pub(super) struct TerminalPublicationBatch {
    pub(super) rows: RowBatch,
    /// Terminal sparse search overwhelmingly publishes one row at a time.
    /// Keep that exact origin inline; wider/mixed cohorts spill only when
    /// they actually need more storage.
    pub(super) origins: SmallVec<[ActivationId; 1]>,
    /// Semantic origins that must be registered before any row in this batch
    /// is staged. Ordinary terminal batches carry none; the first positive
    /// winner for one Confirm parent carries exactly one.
    pub(super) registrations: SmallVec<[TerminalOriginRegistration; 1]>,
}

impl TerminalPublicationBatch {
    fn new(activation: ActivationId, rows: RowBatch) -> Self {
        Self::new_with_registration(activation, rows, None)
    }

    fn new_with_registration(
        activation: ActivationId,
        rows: RowBatch,
        registration: Option<TerminalOriginRegistration>,
    ) -> Self {
        let mut origins = SmallVec::new();
        origins.resize(rows.row_count, activation);
        let mut registrations = SmallVec::new();
        registrations.extend(registration);
        Self {
            rows,
            origins,
            registrations,
        }
    }

    fn append(&mut self, mut other: Self) {
        self.rows.append(other.rows);
        self.origins.extend(other.origins.drain(..));
        self.registrations.extend(other.registrations.drain(..));
        debug_assert_eq!(self.origins.len(), self.rows.row_count);
    }
}

#[derive(Default)]
struct DeltaStableEffects {
    continuation: Option<ContinuationToken>,
    /// Full-bound raw rows ready for the outer iterator's ordinary staging
    /// buffer. This is a semantic-origin-bearing publication receipt, never
    /// a canonical delta or stable state.
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
                next_positive_generation: 0,
                activations: AHashMap::new(),
            },
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn positive_parent(&self, activation: ActivationId) -> Option<PositiveConfirmParentId> {
        self.state
            .activations
            .contains_key(&activation)
            .then_some(PositiveConfirmParentId {
                brand: self.brand,
                activation,
            })
    }

    /// Opens the optional ledger on one authoritative Confirm activation.
    ///
    /// One activation is one semantic parent and may register exactly once;
    /// a duplicate open is inert rather than erasing published obligations.
    /// Unfixed and Barrier parents retain their semantic certificate but
    /// acquire no ledger.
    #[cfg_attr(not(test), allow(dead_code))]
    fn open_exact_and_support_publication(
        &mut self,
        activation: ActivationId,
        confirm_state: StateId,
        certificate: PositivePublicationCertificate,
    ) -> Option<PositiveConfirmParentId> {
        self.open_positive_publication_from(
            activation,
            confirm_state,
            certificate,
            PositivePublicationAuthorization::ExactAndSupport,
        )
    }

    fn open_exact_only_publication(
        &mut self,
        activation: ActivationId,
        confirm_state: StateId,
        certificate: PositivePublicationCertificate,
    ) -> Option<PositiveConfirmParentId> {
        self.open_positive_publication_from(
            activation,
            confirm_state,
            certificate,
            PositivePublicationAuthorization::ExactOnly,
        )
    }

    fn open_positive_publication_from(
        &mut self,
        activation: ActivationId,
        confirm_state: StateId,
        certificate: PositivePublicationCertificate,
        authorization: PositivePublicationAuthorization,
    ) -> Option<PositiveConfirmParentId> {
        let parent = self.positive_parent(activation)?;
        if !matches!(
            &self.state.activations.get(&activation)?.reducer,
            DeltaReducer::Confirm { .. }
        ) {
            return None;
        }
        if self
            .state
            .activations
            .get(&activation)?
            .positive_publication
            .is_some()
        {
            return None;
        }
        let registration = if certificate.eligible() {
            let generation = take_monotonic(
                &mut self.state.next_positive_generation,
                "positive-publication generation",
            );
            PositivePublicationRegistration::Eligible(PositivePublicationLedger {
                generation,
                open: true,
                authorization,
                confirm_state,
                certificate,
                published: BTreeSet::new(),
                support_children: SmallVec::new(),
                support_work: PositiveSupportWorkBudget::default(),
                service_support_started: false,
            })
        } else {
            PositivePublicationRegistration::Private {
                confirm_state,
                certificate,
            }
        };
        let activation = self
            .state
            .activations
            .get_mut(&activation)
            .expect("validated Confirm activation disappeared");
        activation.positive_publication = Some(Box::new(registration));
        Some(parent)
    }

    /// Opens one physical Support child and constructs its link only after the
    /// child's activation identity has been allocated.
    ///
    /// The production feeder enters this transaction only after selecting an
    /// exact fully-bound Support Program for an eligible live Confirm.
    #[cfg_attr(not(test), allow(dead_code))]
    fn open_positive_support_activation(
        &mut self,
        parent: PositiveConfirmParentId,
        occurrence: usize,
        value: RawInline,
        support_variables: VariableSet,
        terminal_full: Option<VariableSet>,
    ) -> Option<ActivationId> {
        if parent.brand != self.brand {
            return None;
        }
        let (generation, bound, row, terminal) = {
            let activation = self.state.activations.get(&parent.activation)?;
            let DeltaReducer::Confirm { original } = &activation.reducer else {
                return None;
            };
            if original.one_parent_values().get(occurrence) != Some(&value) {
                return None;
            }
            let (bound, row) = positive_support_child_context(&activation.return_to, value)?;
            if !support_variables.is_subset_of(&bound) {
                return None;
            }
            let PositivePublicationRegistration::Eligible(ledger) =
                activation.positive_publication.as_deref()?
            else {
                return None;
            };
            if !ledger.open
                || !ledger
                    .authorization
                    .authorizes(PositivePublicationSource::SupportHedge)
                || !ledger.certificate.eligible()
            {
                return None;
            }
            let terminal = terminal_full.is_some_and(|full| {
                ledger.certificate.continuation == ContinuationPublicationReceipt::Terminal
                    && bound == full
                    && matches!(
                        &activation.return_to,
                        DeltaReturn::Stable { desc, .. }
                            if commits_final_checked_candidate(desc, full)
                    )
            });
            (ledger.generation, bound, row, terminal)
        };
        let child = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        let link = PositiveSupportLink {
            child,
            parent: parent.activation,
            generation,
            occurrence,
            value,
        };
        assert!(
            self.state
                .activations
                .insert(
                    child,
                    Activation {
                        reducer: DeltaReducer::PositiveSupport {
                            link: Box::new(link),
                            witnessed: false,
                        },
                        return_to: DeltaReturn::PositiveSupport { bound, row },
                        positive_publication: None,
                        physical_class: if terminal {
                            DeltaPhysicalClass::TerminalStreaming
                        } else {
                            DeltaPhysicalClass::General
                        },
                        terminal_sparse_quantum: 1,
                        source_candidates: None,
                        suspended_source_page: None,
                        program_joins: AHashMap::new(),
                        seen: AHashMap::new(),
                        accepted: AHashSet::new(),
                        live: AHashMap::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "positive Support activation identifier was reused"
        );
        let parent_activation = self
            .state
            .activations
            .get_mut(&parent.activation)
            .expect("positive Support parent disappeared during child creation");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            parent_activation.positive_publication.as_deref_mut()
        else {
            unreachable!("validated positive Support parent lost its eligible ledger")
        };
        assert!(
            ledger.open && ledger.generation == generation,
            "positive Support parent changed generation during child creation"
        );
        ledger.support_children.push(child);
        Some(child)
    }

    /// Returns the physical Support hedges currently owned by one semantic
    /// Confirm parent. The copy is an affine-retirement target list, not
    /// relational evidence.
    fn positive_support_children(
        &self,
        parent: PositiveConfirmParentId,
    ) -> SmallVec<[ActivationId; 1]> {
        if parent.brand != self.brand {
            return SmallVec::new();
        }
        let Some(activation) = self.state.activations.get(&parent.activation) else {
            return SmallVec::new();
        };
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref()
        else {
            return SmallVec::new();
        };
        ledger.support_children.clone()
    }

    fn positive_support_parent_for_child(
        &self,
        child: ActivationId,
    ) -> Option<PositiveConfirmParentId> {
        let activation = self.state.activations.get(&child)?;
        let DeltaReducer::PositiveSupport { link, .. } = &activation.reducer else {
            return None;
        };
        (link.child == child).then_some(PositiveConfirmParentId {
            brand: self.brand,
            activation: link.parent,
        })
    }

    fn positive_publication_parent(
        &self,
        activation: ActivationId,
    ) -> Option<PositiveConfirmParentId> {
        let activation_state = self.state.activations.get(&activation)?;
        matches!(
            activation_state.positive_publication.as_deref(),
            Some(PositivePublicationRegistration::Eligible(_))
        )
        .then_some(PositiveConfirmParentId {
            brand: self.brand,
            activation,
        })
    }

    fn live_positive_support_child(
        &self,
        parent: ActivationId,
        generation: u64,
        children: &[ActivationId],
    ) -> bool {
        children.iter().copied().any(|child| {
            self.state
                .activations
                .get(&child)
                .is_some_and(|activation| {
                    matches!(
                        &activation.reducer,
                        DeltaReducer::PositiveSupport { link, .. }
                            if link.child == child
                                && link.parent == parent
                                && link.generation == generation
                    )
                })
        })
    }

    fn positive_support_budget_available(&self, parent: PositiveConfirmParentId) -> usize {
        if parent.brand != self.brand {
            return 0;
        }
        let Some(activation) = self.state.activations.get(&parent.activation) else {
            return 0;
        };
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref()
        else {
            return 0;
        };
        if !ledger.open
            || !ledger
                .authorization
                .authorizes(PositivePublicationSource::SupportHedge)
            || !self.live_positive_support_child(
                parent.activation,
                ledger.generation,
                &ledger.support_children,
            )
        {
            return 0;
        }
        ledger.support_work.available()
    }

    fn positive_support_service_is_started(&self, parent: PositiveConfirmParentId) -> bool {
        if parent.brand != self.brand {
            return false;
        }
        let Some(activation) = self.state.activations.get(&parent.activation) else {
            return false;
        };
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref()
        else {
            return false;
        };
        ledger.open
            && ledger
                .authorization
                .authorizes(PositivePublicationSource::SupportHedge)
            && ledger.service_support_started
            && self.live_positive_support_child(
                parent.activation,
                ledger.generation,
                &ledger.support_children,
            )
    }

    fn has_live_started_positive_support(&self) -> bool {
        self.state.activations.iter().any(|(&activation, state)| {
            matches!(&state.reducer, DeltaReducer::Confirm { .. })
                && self.positive_support_service_is_started(PositiveConfirmParentId {
                    brand: self.brand,
                    activation,
                })
        })
    }

    /// Assigns one public-pull demand token to one exact semantic parent.
    ///
    /// The scheduler selects a concrete parked child before entering this
    /// transaction. Revalidation here ensures demand can neither start an
    /// orphan hedge nor cross a closed publication generation.
    fn mint_positive_support_demand(&mut self, parent: PositiveConfirmParentId) -> bool {
        if parent.brand != self.brand {
            return false;
        }
        let (generation, children) = {
            let Some(activation) = self.state.activations.get(&parent.activation) else {
                return false;
            };
            let Some(PositivePublicationRegistration::Eligible(ledger)) =
                activation.positive_publication.as_deref()
            else {
                return false;
            };
            if !ledger.open
                || !ledger
                    .authorization
                    .authorizes(PositivePublicationSource::SupportHedge)
                || !ledger.certificate.eligible()
            {
                return false;
            }
            (ledger.generation, ledger.support_children.clone())
        };
        if !self.live_positive_support_child(parent.activation, generation, &children) {
            return false;
        }
        let activation = self
            .state
            .activations
            .get_mut(&parent.activation)
            .expect("validated positive Support parent disappeared");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref_mut()
        else {
            unreachable!("validated positive Support parent lost its ledger")
        };
        assert!(ledger.open && ledger.generation == generation);
        ledger.support_work.mint_demand();
        true
    }

    /// Starts one parent in the experimental service scheduler without
    /// minting production count credit.
    fn start_positive_support_service(&mut self, parent: PositiveConfirmParentId) -> bool {
        if parent.brand != self.brand {
            return false;
        }
        let (generation, children) = {
            let Some(activation) = self.state.activations.get(&parent.activation) else {
                return false;
            };
            let Some(PositivePublicationRegistration::Eligible(ledger)) =
                activation.positive_publication.as_deref()
            else {
                return false;
            };
            if !ledger.open
                || !ledger
                    .authorization
                    .authorizes(PositivePublicationSource::SupportHedge)
                || ledger.service_support_started
                || !ledger.certificate.eligible()
            {
                return false;
            }
            (ledger.generation, ledger.support_children.clone())
        };
        if !self.live_positive_support_child(parent.activation, generation, &children) {
            return false;
        }
        let activation = self
            .state
            .activations
            .get_mut(&parent.activation)
            .expect("validated positive Support parent disappeared");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref_mut()
        else {
            unreachable!("validated positive Support parent lost its ledger")
        };
        assert!(ledger.open && ledger.generation == generation);
        ledger.service_support_started = true;
        true
    }

    /// Reserves at most `requested` units immediately before physical Program
    /// dispatch. The returned grant is affine and must accompany this exact
    /// selected task until its validated receipt settles.
    fn reserve_positive_support_work(
        &mut self,
        child: ActivationId,
        requested: usize,
    ) -> Option<PositiveSupportWorkGrant> {
        let link = {
            let activation = self.state.activations.get(&child)?;
            let DeltaReducer::PositiveSupport { link, .. } = &activation.reducer else {
                return None;
            };
            if link.child != child {
                return None;
            }
            link.as_ref().clone()
        };
        let activation = self.state.activations.get_mut(&link.parent)?;
        let PositivePublicationRegistration::Eligible(ledger) =
            activation.positive_publication.as_deref_mut()?
        else {
            return None;
        };
        if !ledger.open
            || ledger.generation != link.generation
            || !ledger.support_work.started
            || !ledger.support_children.contains(&child)
        {
            return None;
        }
        let granted = ledger.support_work.reserve(requested);
        (granted > 0).then_some(PositiveSupportWorkGrant {
            brand: self.brand,
            parent: link.parent,
            child,
            generation: link.generation,
            granted,
            accounting: PositiveSupportWorkAccounting::CountCredit,
        })
    }

    /// Validates one service-debt Support dispatch without touching the
    /// production `D + C` count ledger.
    fn reserve_positive_support_service(
        &self,
        child: ActivationId,
        requested: usize,
    ) -> Option<PositiveSupportWorkGrant> {
        if requested == 0 {
            return None;
        }
        let activation = self.state.activations.get(&child)?;
        let DeltaReducer::PositiveSupport { link, .. } = &activation.reducer else {
            return None;
        };
        if link.child != child {
            return None;
        }
        let parent = self.state.activations.get(&link.parent)?;
        let PositivePublicationRegistration::Eligible(ledger) =
            parent.positive_publication.as_deref()?
        else {
            return None;
        };
        if !ledger.open
            || ledger.generation != link.generation
            || !ledger.service_support_started
            || !ledger.support_children.contains(&child)
        {
            return None;
        }
        Some(PositiveSupportWorkGrant {
            brand: self.brand,
            parent: link.parent,
            child,
            generation: link.generation,
            granted: requested,
            accounting: PositiveSupportWorkAccounting::GlobalServiceDebt,
        })
    }

    fn settle_positive_support_work(
        &mut self,
        grant: PositiveSupportWorkGrant,
        child: ActivationId,
        examined: usize,
    ) -> usize {
        assert_eq!(
            grant.brand, self.brand,
            "positive Support work grant crossed registries"
        );
        assert_eq!(
            grant.child, child,
            "positive Support work grant crossed physical children"
        );
        let child_activation = self
            .state
            .activations
            .get(&child)
            .expect("positive Support work settled after its child disappeared");
        assert!(matches!(
            &child_activation.reducer,
            DeltaReducer::PositiveSupport { link, .. }
                if link.child == child
                    && link.parent == grant.parent
                    && link.generation == grant.generation
        ));
        let parent = self
            .state
            .activations
            .get_mut(&grant.parent)
            .expect("positive Support work settled after its parent disappeared");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            parent.positive_publication.as_deref_mut()
        else {
            panic!("positive Support work settled after its ledger disappeared")
        };
        assert_eq!(
            ledger.generation, grant.generation,
            "positive Support work settled across ledger generations"
        );
        match grant.accounting {
            PositiveSupportWorkAccounting::CountCredit => {
                ledger.support_work.settle(grant.granted, examined);
            }
            PositiveSupportWorkAccounting::GlobalServiceDebt => {
                assert!(
                    examined <= grant.granted,
                    "positive Support service receipt exceeded its physical packet"
                );
            }
        }
        examined
    }

    /// Accounts one validated exact Confirm Program replacement. The paired
    /// total is diagnostic; only a started, open parent with a live linked
    /// Support child mints usable `C`.
    fn account_positive_exact_work(
        &mut self,
        parent: ActivationId,
        examined: usize,
    ) -> PositiveExactWorkAccounting {
        let (generation, children, paired) = {
            let Some(activation) = self.state.activations.get(&parent) else {
                return PositiveExactWorkAccounting::default();
            };
            if !matches!(&activation.reducer, DeltaReducer::Confirm { .. }) {
                return PositiveExactWorkAccounting::default();
            }
            let Some(PositivePublicationRegistration::Eligible(ledger)) =
                activation.positive_publication.as_deref()
            else {
                return PositiveExactWorkAccounting::default();
            };
            (
                ledger.generation,
                ledger.support_children.clone(),
                ledger.authorization == PositivePublicationAuthorization::ExactAndSupport,
            )
        };
        if !paired {
            return PositiveExactWorkAccounting::default();
        }
        let live_child = self.live_positive_support_child(parent, generation, &children);
        let activation = self
            .state
            .activations
            .get_mut(&parent)
            .expect("paired exact Confirm disappeared during work accounting");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref_mut()
        else {
            unreachable!("paired exact Confirm lost its positive ledger")
        };
        let credited = if ledger.open && live_child {
            ledger.support_work.mint_exact(examined)
        } else {
            0
        };
        PositiveExactWorkAccounting {
            paired: true,
            credited,
        }
    }

    /// Burns every unspent unit after the last linked Support child leaves
    /// live registry custody.
    fn retire_orphaned_positive_support_work(&mut self, parent: PositiveConfirmParentId) -> usize {
        if parent.brand != self.brand {
            return 0;
        }
        let (generation, children) = {
            let Some(activation) = self.state.activations.get(&parent.activation) else {
                return 0;
            };
            let Some(PositivePublicationRegistration::Eligible(ledger)) =
                activation.positive_publication.as_deref()
            else {
                return 0;
            };
            (ledger.generation, ledger.support_children.clone())
        };
        if self.live_positive_support_child(parent.activation, generation, &children) {
            return 0;
        }
        let activation = self
            .state
            .activations
            .get_mut(&parent.activation)
            .expect("positive Support parent disappeared during allowance retirement");
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref_mut()
        else {
            unreachable!("positive Support parent lost its allowance ledger")
        };
        ledger.support_work.retire_available()
    }

    fn retire_positive_support_work(&mut self, parent: PositiveConfirmParentId) -> usize {
        if parent.brand != self.brand {
            return 0;
        }
        let Some(activation) = self.state.activations.get_mut(&parent.activation) else {
            return 0;
        };
        let Some(PositivePublicationRegistration::Eligible(ledger)) =
            activation.positive_publication.as_deref_mut()
        else {
            return 0;
        };
        ledger.support_work.retire_available()
    }

    fn assert_no_positive_support_reservations(&self) {
        for activation in self.state.activations.values() {
            if let Some(PositivePublicationRegistration::Eligible(ledger)) =
                activation.positive_publication.as_deref()
            {
                ledger.support_work.assert_conservation();
                assert_eq!(
                    ledger.support_work.reserved, 0,
                    "positive Support reservation crossed a scheduler boundary"
                );
            }
        }
    }

    /// Commits a linked physical Support witness through the semantic parent's
    /// sole positive-publication linearization point.
    #[cfg_attr(not(test), allow(dead_code))]
    fn commit_positive_publication(
        &mut self,
        witness: PositiveSupportWitness,
        direct_terminal_full: Option<VariableSet>,
    ) -> Option<PositivePublicationGrant> {
        if witness.brand != self.brand {
            return None;
        }
        let link = witness.link;
        let Some(child) = self.state.activations.get(&link.child) else {
            return None;
        };
        let DeltaReducer::PositiveSupport {
            link: current_link,
            witnessed: true,
        } = &child.reducer
        else {
            return None;
        };
        if current_link.as_ref() != &link {
            return None;
        }
        let child_context = match &child.return_to {
            DeltaReturn::PositiveSupport { bound, row } => (*bound, row.clone()),
            _ => return None,
        };
        self.commit_positive_value(
            link.parent,
            link.generation,
            link.occurrence,
            link.value,
            PositivePublicationSource::SupportHedge,
            Some(child_context),
            direct_terminal_full,
        )
    }

    /// Commits one branch-local exact Confirm replacement witness.
    ///
    /// Witness construction already required a newly accepted B[0] after a
    /// real Program credit was consumed. This preflight deliberately repeats
    /// the authoritative acceptance check before entering the shared SET
    /// linearization point.
    fn commit_confirm_positive_publication(
        &mut self,
        witness: PositiveConfirmWitness,
        direct_terminal_full: Option<VariableSet>,
    ) -> Option<PositivePublicationGrant> {
        if witness.parent.brand != self.brand {
            return None;
        }
        let Some(activation) = self.state.activations.get(&witness.parent.activation) else {
            return None;
        };
        if !activation.accepted.contains(&witness.value) {
            return None;
        }
        self.commit_positive_value(
            witness.parent.activation,
            witness.generation,
            0,
            witness.value,
            PositivePublicationSource::ExactConfirmTap,
            None,
            direct_terminal_full,
        )
    }

    /// Mints exact-tap authority only from a newly accepted B[0] after the
    /// replacement that observed it has consumed its real Program credit.
    fn exact_confirm_positive_witness(
        &self,
        parent: ActivationId,
        newly_accepted: &[RawInline],
    ) -> Option<PositiveConfirmWitness> {
        let activation = self.state.activations.get(&parent)?;
        let DeltaReducer::Confirm { original } = &activation.reducer else {
            return None;
        };
        let value = *original.one_parent_values().first()?;
        if !newly_accepted.contains(&value) || !activation.accepted.contains(&value) {
            return None;
        }
        let PositivePublicationRegistration::Eligible(ledger) =
            activation.positive_publication.as_deref()?
        else {
            return None;
        };
        if !ledger.open
            || !ledger
                .authorization
                .authorizes(PositivePublicationSource::ExactConfirmTap)
            || !ledger.certificate.eligible()
            || ledger.published.contains(&value)
        {
            return None;
        }
        Some(PositiveConfirmWitness {
            parent: PositiveConfirmParentId {
                brand: self.brand,
                activation: parent,
            },
            generation: ledger.generation,
            value,
        })
    }

    /// Sole positive-publication SET linearization point.
    ///
    /// This exclusive mutable registry borrow is the scheduler's CAS law.
    /// Current semantic source, generation, open Confirm reducer, indexed
    /// original occurrence, and continuation certificate are revalidated
    /// before the first `(parent, value)` insertion wins.
    ///
    /// The returned private grant must be consumed immediately with
    /// `release_positive_publication`, before fallible work. The outer
    /// `direct_terminal_full` arrives unfiltered by any feeder's physical
    /// activation class.
    #[allow(clippy::too_many_arguments)]
    fn commit_positive_value(
        &mut self,
        parent_activation: ActivationId,
        generation: u64,
        occurrence: usize,
        value: RawInline,
        source: PositivePublicationSource,
        support_child_context: Option<(VariableSet, Box<[RawInline]>)>,
        direct_terminal_full: Option<VariableSet>,
    ) -> Option<PositivePublicationGrant> {
        let Some(activation) = self.state.activations.get_mut(&parent_activation) else {
            return None;
        };
        let DeltaReducer::Confirm { original } = &activation.reducer else {
            return None;
        };
        if let Some(child_context) = support_child_context {
            if positive_support_child_context(&activation.return_to, value) != Some(child_context) {
                return None;
            }
        }
        let DeltaReturn::Stable {
            desc,
            parent,
            set_admit_result,
        } = &activation.return_to
        else {
            return None;
        };
        let Some(registration) = activation.positive_publication.as_deref_mut() else {
            return None;
        };
        let PositivePublicationRegistration::Eligible(ledger) = registration else {
            return None;
        };
        if !ledger.open
            || ledger.generation != generation
            || !ledger.authorization.authorizes(source)
            || !ledger.certificate.eligible()
            || original.one_parent_values().get(occurrence) != Some(&value)
            || ledger.published.contains(&value)
            || parent.len() != desc.bound.count()
            || *set_admit_result != ledger.certificate.crosses_set_boundary
        {
            return None;
        }
        let route = match ledger.certificate.continuation {
            ContinuationPublicationReceipt::Terminal => {
                let full = direct_terminal_full?;
                if !commits_final_checked_candidate(desc, full) {
                    return None;
                }
                let registration =
                    ledger
                        .published
                        .is_empty()
                        .then_some(TerminalOriginRegistration {
                            family: ledger.confirm_state,
                            origin: parent_activation,
                        });
                PositivePublicationRoute::Terminal {
                    origin: parent_activation,
                    full,
                    registration,
                }
            }
            ContinuationPublicationReceipt::ChunkHomomorphic => {
                if !ledger.certificate.crosses_set_boundary
                    || !matches!(&desc.phase, ResidualPhase::Candidate { .. })
                {
                    return None;
                }
                PositivePublicationRoute::ChunkHomomorphic
            }
            ContinuationPublicationReceipt::Barrier => return None,
        };
        let return_to = activation.return_to.clone();
        assert!(
            ledger.published.insert(value),
            "preflighted positive publication lost its first-winner race"
        );
        Some(PositivePublicationGrant {
            value,
            return_to,
            route,
            source,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn close_and_snapshot_positive_publication(
        &mut self,
        parent: PositiveConfirmParentId,
        generation: u64,
    ) -> Option<PositivePublicationLedger> {
        if parent.brand != self.brand {
            return None;
        }
        let Some(activation) = self.state.activations.get(&parent.activation) else {
            return None;
        };
        if !matches!(&activation.reducer, DeltaReducer::Confirm { .. }) {
            return None;
        }
        let Some(registration) = activation.positive_publication.as_deref() else {
            return None;
        };
        let PositivePublicationRegistration::Eligible(ledger) = registration else {
            return None;
        };
        if !ledger.open || ledger.generation != generation {
            return None;
        }
        let closed_generation = take_monotonic(
            &mut self.state.next_positive_generation,
            "positive-publication generation",
        );
        let ledger = self
            .state
            .activations
            .get_mut(&parent.activation)
            .expect("validated Confirm activation disappeared")
            .positive_publication
            .as_deref_mut()
            .and_then(|registration| match registration {
                PositivePublicationRegistration::Eligible(ledger) => Some(ledger),
                PositivePublicationRegistration::Private { .. } => None,
            })
            .expect("validated positive-publication ledger disappeared");
        ledger.support_work.retire_available();
        ledger.open = false;
        ledger.generation = closed_generation;
        Some(ledger.clone())
    }

    /// Test-only diagnostic snapshot. Production settlement uses the atomic
    /// close-and-snapshot transaction above.
    #[cfg(test)]
    fn positive_publication_snapshot(
        &self,
        parent: PositiveConfirmParentId,
    ) -> Option<PositivePublicationLedger> {
        if parent.brand != self.brand {
            return None;
        }
        self.state
            .activations
            .get(&parent.activation)?
            .positive_publication
            .as_deref()
            .and_then(|registration| match registration {
                PositivePublicationRegistration::Eligible(ledger) => Some(ledger.clone()),
                PositivePublicationRegistration::Private { .. } => None,
            })
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
                        positive_publication: None,
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
                        positive_publication: None,
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
                        positive_publication: None,
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
        let positive_support_reducer = matches!(
            &self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation")
                .reducer,
            DeltaReducer::PositiveSupport { .. }
        );
        for seed in seeds {
            assert!(
                !positive_support_reducer || seed.accepted.is_none(),
                "PositiveSupport cannot turn initial acceptance into publication authority"
            );
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
                | DeltaReducer::PositiveSupport { .. }
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

    /// Affinely retires one queued Program producer owned by a cancelled
    /// PositiveSupport hedge.
    ///
    /// The scheduler must discard the corresponding typed [`ProgramWork`]
    /// before entering this transaction. PositiveSupport deliberately admits
    /// only unjoined Program credits, so removing the last queued producer is
    /// sufficient to prove physical child quiescence without manufacturing a
    /// semantic false result.
    fn retire_positive_support_program_credit(
        &mut self,
        credit: ProducerCredit,
    ) -> Option<QuiescenceProof> {
        assert_eq!(
            credit.brand, self.brand,
            "positive Support cancellation credit crossed registries"
        );
        let activation_id = credit.key.activation;
        let activation = self
            .state
            .activations
            .get_mut(&activation_id)
            .expect("positive Support cancellation named an unknown activation");
        assert_eq!(activation.status, ActivationStatus::Open);
        assert!(
            matches!(&activation.reducer, DeltaReducer::PositiveSupport { .. }),
            "only a PositiveSupport child may consume hedge-cancellation credit"
        );
        assert_eq!(
            activation.live.remove(&credit.key.nonce),
            Some(CreditKind::Program { join: None }),
            "positive Support cancellation received an unknown, replayed, or joined credit"
        );
        assert!(
            activation.program_joins.is_empty(),
            "PositiveSupport cancellation crossed a receipt-local Program join"
        );
        if activation.live.is_empty() {
            activation.status = ActivationStatus::Quiescent;
            Some(QuiescenceProof {
                activation: activation_id,
            })
        } else {
            None
        }
    }

    /// Replaces one opaque typed producer through the single affine law.
    ///
    /// `prior_observed` contains accepting child endpoints from earlier pages
    /// fused into this receipt. Streaming SET admission visits that prefix
    /// before this page's direct and observed values, preserving the exact
    /// page chronology without classifying those endpoints as source-direct
    /// effects.
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
        prior_observed: impl IntoIterator<Item = RawInline>,
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
        let (parent_join, positive_support_reducer) = {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            assert_eq!(activation.status, ActivationStatus::Open);
            let join = match activation.live.get(&parent.key.nonce) {
                Some(CreditKind::Program { join }) => *join,
                _ => panic!("unknown, replayed, or wrong-kind program credit"),
            };
            if let DeltaReducer::PositiveSupport { link, .. } = &activation.reducer {
                assert_eq!(
                    link.child, activation_id,
                    "PositiveSupport activation retained a link to a different physical child"
                );
            }
            (
                join,
                matches!(&activation.reducer, DeltaReducer::PositiveSupport { .. }),
            )
        };
        if positive_support_reducer {
            assert!(
                parent_join.is_none(),
                "PositiveSupport cannot consume a receipt-local joined Program credit"
            );
            assert!(
                !matches!(
                    &resume,
                    Some(ProgramResume::AfterChildren(_) | ProgramResume::AfterChildrenDone)
                ),
                "PositiveSupport does not yet propagate commit evidence through AfterChildren"
            );
        }

        let mut prior_observed: SmallVec<[RawInline; 1]> = prior_observed.into_iter().collect();
        let observed: SmallVec<[RawInline; 1]> = observed.into_iter().collect();
        let mut direct: SmallVec<[RawInline; 1]> = direct.into_iter().collect();
        let raw_stream_occurrences = {
            let activation = self
                .state
                .activations
                .get(&activation_id)
                .expect("unknown program activation");
            if activation.reducer.streams() {
                direct
                    .len()
                    .checked_add(prior_observed.len())
                    .and_then(|count| count.checked_add(observed.len()))
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
                (DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal, _) => {
                    for value in prior_observed.drain(..).chain(direct.drain(..)) {
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
                        let mut page =
                            CandidatePayload::Values(std::mem::take(&mut direct).into_vec());
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
                    append_one_parent_page(output, std::mem::take(&mut direct).into_vec());
                }
                (DeltaReducer::SetAdmit { output }, DeltaReturn::SetAdmission { .. }) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine SET admission reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "SET admission acquired graph Accepted state"
                    );
                    append_one_parent_page(output, std::mem::take(&mut direct).into_vec());
                }
                (DeltaReducer::FormulaOrAdmit, DeltaReturn::FormulaOrAdmit { batch, .. }) => {
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
                (DeltaReducer::FormulaOrEmit { output }, DeltaReturn::FormulaOrEmit { .. }) => {
                    assert!(
                        observed.is_empty() && children.is_empty() && !reported_support,
                        "engine Formula OR emission reported graph effects"
                    );
                    assert!(
                        activation.accepted.is_empty(),
                        "Formula OR emission acquired graph Accepted state"
                    );
                    if !direct.is_empty() {
                        let mut page =
                            CandidatePayload::Values(std::mem::take(&mut direct).into_vec());
                        page.defer_for_shared_activation(1);
                        output.extend_same_domain(page, 1);
                    }
                }
                (DeltaReducer::FormulaOrAdmit, _)
                | (DeltaReducer::FormulaOrEmit { .. }, _)
                | (DeltaReducer::SetAdmit { .. }, _) => {
                    panic!("engine reducer lost its exact affine return payload")
                }
                (
                    DeltaReducer::Support { .. }
                    | DeltaReducer::PositiveSupport { .. }
                    | DeltaReducer::Confirm { .. },
                    _,
                ) => {
                    assert!(
                        prior_observed.is_empty() && direct.is_empty(),
                        "a non-proposal program reducer observed proposal candidates"
                    )
                }
            }
            assert!(
                prior_observed.is_empty(),
                "only streaming proposal reducers may receive prior observations"
            );
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
                activation.reducer.retain_quiescent_proposal_page(retained);
            }
        }

        let reported_positive =
            positive_support_reducer && (reported_support || !accepted.is_empty());
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
                DeltaReducer::PositiveSupport { .. } => false,
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
        let positive_confirm =
            self.exact_confirm_positive_witness(activation_id, accepted.as_slice());

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
        let positive_support = if reported_positive {
            let activation = self
                .state
                .activations
                .get_mut(&activation_id)
                .expect("positive Support activation disappeared after replacement");
            let DeltaReducer::PositiveSupport { link, witnessed } = &mut activation.reducer else {
                unreachable!("positive Support replacement changed its reducer")
            };
            assert_eq!(
                link.child, activation_id,
                "PositiveSupport activation retained a link to a different physical child"
            );
            if std::mem::replace(witnessed, true) {
                None
            } else {
                Some(PositiveSupportWitness {
                    brand: self.brand,
                    link: link.as_ref().clone(),
                })
            }
        } else {
            None
        };
        ProgramReplaceOutcome {
            scheduled,
            raw_proposal_occurrences: raw_stream_occurrences,
            accepted: if positive_support_reducer {
                SmallVec::new()
            } else {
                accepted
            },
            positive_support_reducer,
            positive_support: positive_support.map(Box::new),
            positive_confirm: positive_confirm.map(Box::new),
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
            DeltaReturn::PositiveSupport { bound, row } => (*bound, row.as_ref()),
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
        activation
            .source_candidates
            .as_deref()
            .or_else(|| match &activation.reducer {
                DeltaReducer::Confirm { original } => Some(original.one_parent_values()),
                _ => None,
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
            DeltaReturn::PositiveSupport { bound, .. } => *bound,
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

    /// Whether one Program credit is the activation's complete unjoined
    /// producer frontier. A receipt-local child may reuse this ownership only
    /// while no sibling or structured join can observe an intermediate
    /// replacement boundary.
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

    fn is_live_positive_support(&self, activation: ActivationId) -> bool {
        self.state
            .activations
            .get(&activation)
            .is_some_and(|activation| {
                matches!(&activation.reducer, DeltaReducer::PositiveSupport { .. })
            })
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
            | DeltaReducer::PositiveSupport { .. }
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
            DeltaReducer::Support { published: true } => DeltaCompletion::Cleanup,
            DeltaReducer::Support { published: false } => {
                assert!(
                    activation.accepted.is_empty(),
                    "an unpublished support reducer quiesced with a witness"
                );
                DeltaCompletion::Support(false)
            }
            DeltaReducer::PositiveSupport { .. } => {
                // Truth or exhaustion belongs solely to the semantic positive
                // publication transaction. Physical child completion never
                // releases Formula false and never owns the Confirm result.
                DeltaCompletion::Cleanup
            }
            DeltaReducer::Confirm { original } => {
                let result = original
                    .iter()
                    .filter_map(|(parent, candidate)| {
                        assert_eq!(parent, 0, "one-parent Confirm changed domains");
                        activation
                            .accepted
                            .contains(&candidate)
                            .then_some(candidate)
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

        let (handoff, positive_authority) = {
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
                | DeltaReturn::PositiveSupport { .. }
                | DeltaReturn::SetAdmission { .. } => false,
            };
            let handoff = match &activation.reducer {
                DeltaReducer::QuiescentProposal { occurrences } if !occurrences.is_empty() => {
                    Handoff::Proposal
                }
                DeltaReducer::Confirm { original } if eligible_return && !original.is_empty() => {
                    Handoff::Confirm
                }
                _ => Handoff::Complete,
            };
            let positive_authority = match (
                &activation.reducer,
                activation.positive_publication.as_deref(),
            ) {
                (
                    DeltaReducer::Confirm { .. },
                    Some(PositivePublicationRegistration::Eligible(ledger)),
                ) => {
                    assert!(
                        ledger.open,
                        "authoritative Confirm reached settlement behind a closed positive ledger"
                    );
                    assert!(
                        ledger.certificate.eligible(),
                        "eligible registration retained an ineligible publication certificate"
                    );
                    Some((
                        PositiveConfirmParentId {
                            brand: self.brand,
                            activation: proof.activation,
                        },
                        ledger.generation,
                    ))
                }
                _ => None,
            };
            (handoff, positive_authority)
        };

        let positive = positive_authority.map(|(parent, generation)| {
            self.close_and_snapshot_positive_publication(parent, generation)
                .expect("live positive settlement authority failed to close")
        });

        match handoff {
            Handoff::Complete => {
                if let Some(ledger) = positive {
                    assert!(!ledger.open, "positive settlement snapshot remained open");
                    assert!(
                        ledger.published.is_empty(),
                        "positive Confirm without a finalizer retained publications"
                    );
                }
                RegistrySettlement::Completed(self.finish(proof))
            }
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
                let credit =
                    self.issue_credit(proof.activation, CreditKind::Program { join: None });
                RegistrySettlement::ProposalMaterializer(ProposalMaterializerSeed {
                    activation: proof.activation,
                    state,
                    credit,
                })
            }
            Handoff::Confirm => {
                // Publication owns the relational values in P. Close and
                // freeze that set before replacing the authoritative Confirm,
                // then transfer only the residual G \ P to the unchanged
                // finalizer. Multiplicity in raw B is an internal
                // representation detail: every occurrence of a published
                // value must disappear from the late path.
                let published = positive.map(|ledger| {
                    assert!(!ledger.open, "positive settlement snapshot remained open");
                    ledger.published
                });
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
                    if let Some(published) = published {
                        assert!(
                            published
                                .iter()
                                .all(|value| activation.accepted.contains(value)),
                            "positive Confirm publication contradicted authoritative acceptance"
                        );
                        let registration = activation
                            .positive_publication
                            .take()
                            .expect("closed positive registration disappeared at handoff");
                        assert!(matches!(
                            registration.as_ref(),
                            PositivePublicationRegistration::Eligible(PositivePublicationLedger {
                                open: false,
                                ..
                            })
                        ));
                        for value in &published {
                            assert!(
                                activation.accepted.remove(value),
                                "validated positive publication disappeared from acceptance"
                            );
                        }
                    }
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
                let credit =
                    self.issue_credit(proof.activation, CreditKind::Program { join: None });
                RegistrySettlement::ConfirmFinalizer(ConfirmFinalizerSeed {
                    activation: proof.activation,
                    state,
                    credit,
                })
            }
        }
    }

    fn deep_clone(&self) -> (Self, BTreeMap<CreditKey, ProducerCredit>) {
        self.assert_no_positive_support_reservations();
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

fn program_child_ranges_into(
    children: &[ProgramChild],
    input_count: usize,
    ranges: &mut Vec<std::ops::Range<usize>>,
) {
    ranges.clear();
    ranges.reserve(input_count);
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
}

fn tagged_ranges<T>(
    values: &[(u32, T)],
    parent_count: usize,
    kind: &str,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::with_capacity(parent_count);
    tagged_ranges_into(values, parent_count, kind, &mut ranges);
    ranges
}

fn tagged_ranges_into<T>(
    values: &[(u32, T)],
    parent_count: usize,
    kind: &str,
    ranges: &mut Vec<std::ops::Range<usize>>,
) {
    ranges.clear();
    ranges.reserve(parent_count);
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

struct ProgramTaskReceipt {
    activation: ActivationId,
    credit: ProducerCredit,
    support_grant: Option<PositiveSupportWorkGrant>,
}

#[derive(Default)]
struct ProgramSchedulerScratch {
    parents: Vec<RawInline>,
    vars: Vec<VariableId>,
    activations: Vec<ProgramActivation>,
    task_receipts: Vec<ProgramTaskReceipt>,
    work: Vec<ProgramWork>,
    receipt: ProgramBatchEffects,
    fused_receipt: ProgramBatchEffects,
    receipt_local_observed_prefix: Vec<RawInline>,
    child_ranges: Vec<std::ops::Range<usize>>,
    direct_ranges: Vec<std::ops::Range<usize>>,
    accepted_ranges: Vec<std::ops::Range<usize>>,
    supported_ranges: Vec<std::ops::Range<usize>>,
    retired_activations: Vec<ProgramActivation>,
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
    fn of_work(
        registry: &ProducerRegistry,
        activation: ActivationId,
        pacing: ProgramPacing,
    ) -> Self {
        let physical = registry.physical_activation_class(activation);
        match pacing {
            ProgramPacing::Search => Self::Search { physical },
            ProgramPacing::Activation if physical == DeltaPhysicalClass::TerminalStreaming => {
                Self::ActivationTerminalStreaming
            }
            ProgramPacing::Activation if registry.activation_streams(activation) => {
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
        Self::of_work(registry, task.activation, &task.work)
    }

    fn of_work(registry: &ProducerRegistry, activation: ActivationId, work: &ProgramWork) -> Self {
        let (bound, has_candidates) = registry.source_dispatch_shape(activation);
        let class = ProgramCohortClass::of_work(registry, activation, work.pacing);
        Self {
            dispatch: work.dispatch,
            bound,
            has_candidates,
            class,
        }
    }
}

/// Scheduler-only service attribution for one queued Program activation.
///
/// This lane is deliberately absent from [`ProgramCohortKey`]. The latter
/// remains the exact semantic/physical call identity, while this label is a
/// noncanonical fence used only when an experimental query-global service
/// scheduler needs an attributable packet. A PositiveSupport race starts when
/// its parent consumes public demand; before that point both its dormant child
/// and its exact parent remain ordinary neutral work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramServiceLane {
    Neutral,
    Exact,
    Support,
}

impl ProgramServiceLane {
    fn of(registry: &ProducerRegistry, activation: ActivationId) -> Self {
        let Some(state) = registry.state.activations.get(&activation) else {
            return Self::Neutral;
        };
        if let DeltaReducer::PositiveSupport { link, .. } = &state.reducer {
            let started = registry
                .state
                .activations
                .get(&link.parent)
                .and_then(|parent| parent.positive_publication.as_deref())
                .is_some_and(|registration| {
                    matches!(
                        registration,
                        PositivePublicationRegistration::Eligible(ledger)
                            if ledger.generation == link.generation
                                && ledger.support_children.contains(&activation)
                                && (ledger.support_work.started
                                    || ledger.service_support_started)
                    )
                });
            return if started {
                Self::Support
            } else {
                Self::Neutral
            };
        }
        let started_live_support =
            state
                .positive_publication
                .as_deref()
                .is_some_and(|registration| {
                    matches!(
                        registration,
                        PositivePublicationRegistration::Eligible(ledger)
                            if matches!(&state.reducer, DeltaReducer::Confirm { .. })
                                && (ledger.support_work.started
                                    || ledger.service_support_started)
                                && registry.live_positive_support_child(
                                    activation,
                                    ledger.generation,
                                    &ledger.support_children,
                                )
                    )
                });
        if started_live_support {
            Self::Exact
        } else {
            Self::Neutral
        }
    }
}

/// Opt-in physical selection policy for attributable Program packets.
///
/// `Unrestricted` is the production default and exactly preserves the
/// established cohort selection. `LanePure` adds only a scheduler fence:
/// compatible work in another service lane stays queued for a later pop.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ProgramLaneSelection {
    #[default]
    Unrestricted,
    LanePure,
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

/// Dense membership index for typed Program work.
///
/// [`DeltaStateId`] values are allocated monotonically from one shared dense
/// interner. Keeping one reusable bucket per observed ID therefore lets an
/// affine pop deactivate a state without destroying the bucket's allocation.
/// Refiling the same state only flips its membership bit and appends into the
/// retained storage. The active bitset also preserves the `BTreeMap` policy of
/// selecting the greatest live state ID for global work, without allocating a
/// tree node on every remove/reinsert cycle.
#[derive(Default)]
struct ProgramWorklist {
    buckets: Vec<ProgramBucket>,
    active: Vec<u64>,
    len: usize,
}

impl ProgramWorklist {
    const WORD_BITS: usize = u64::BITS as usize;

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.len
    }

    fn contains_key(&self, state: &DeltaStateId) -> bool {
        let index = state.0 as usize;
        let word = index / Self::WORD_BITS;
        let bit = index % Self::WORD_BITS;
        self.active
            .get(word)
            .is_some_and(|active| active & (1u64 << bit) != 0)
    }

    fn get(&self, state: &DeltaStateId) -> Option<&ProgramBucket> {
        self.contains_key(state)
            .then(|| &self.buckets[state.0 as usize])
    }

    fn get_mut(&mut self, state: &DeltaStateId) -> Option<&mut ProgramBucket> {
        self.contains_key(state)
            .then(|| &mut self.buckets[state.0 as usize])
    }

    fn append(&mut self, state: DeltaStateId, tasks: &mut Vec<ProgramTask>) {
        if tasks.is_empty() {
            return;
        }
        let index = state.0 as usize;
        if self.buckets.len() <= index {
            self.buckets.resize_with(index + 1, ProgramBucket::default);
        }
        let word = index / Self::WORD_BITS;
        if self.active.len() <= word {
            self.active.resize(word + 1, 0);
        }
        let bit = 1u64 << (index % Self::WORD_BITS);
        if self.active[word] & bit == 0 {
            assert!(
                self.buckets[index].is_empty(),
                "inactive typed Program bucket retained live work"
            );
            self.active[word] |= bit;
            self.len += 1;
        } else {
            assert!(
                !self.buckets[index].is_empty(),
                "active typed Program bucket lost its live work"
            );
        }
        self.buckets[index].append(tasks);
    }

    fn deactivate(&mut self, state: DeltaStateId) {
        let index = state.0 as usize;
        let word = index / Self::WORD_BITS;
        let bit = 1u64 << (index % Self::WORD_BITS);
        let active = self
            .active
            .get_mut(word)
            .expect("typed Program state was never activated");
        assert_ne!(*active & bit, 0, "typed Program state was not active");
        assert!(
            self.buckets[index].is_empty(),
            "nonempty typed Program state was deactivated"
        );
        *active &= !bit;
        self.len -= 1;
    }

    fn last_id(&self) -> Option<DeltaStateId> {
        for (word_index, &word) in self.active.iter().enumerate().rev() {
            if word != 0 {
                let bit = (u64::BITS - 1 - word.leading_zeros()) as usize;
                let index = word_index * Self::WORD_BITS + bit;
                return Some(DeltaStateId(
                    u32::try_from(index).expect("typed Program state id overflow"),
                ));
            }
        }
        None
    }

    fn last_id_in_lane(
        &self,
        registry: &ProducerRegistry,
        lane: ProgramServiceLane,
    ) -> Option<DeltaStateId> {
        self.iter()
            .filter_map(|(state, bucket)| {
                bucket
                    .tasks
                    .iter()
                    .any(|task| ProgramServiceLane::of(registry, task.activation) == lane)
                    .then_some(state)
            })
            .last()
    }

    fn iter(&self) -> impl Iterator<Item = (DeltaStateId, &ProgramBucket)> {
        self.buckets
            .iter()
            .enumerate()
            .filter_map(|(index, bucket)| {
                let state =
                    DeltaStateId(u32::try_from(index).expect("typed Program state id overflow"));
                self.contains_key(&state).then_some((state, bucket))
            })
    }

    /// Removes every queued task owned by the selected physical activations,
    /// retaining state grouping so each opaque handle can be discarded through
    /// the exact Program runtime that created it.
    fn take_activations(
        &mut self,
        activations: &AHashSet<ActivationId>,
    ) -> Vec<(DeltaStateId, Vec<ProgramTask>)> {
        if activations.is_empty() {
            return Vec::new();
        }
        let states: Vec<_> = self
            .iter()
            .filter_map(|(state, bucket)| {
                bucket
                    .tasks
                    .iter()
                    .any(|task| activations.contains(&task.activation))
                    .then_some(state)
            })
            .collect();
        let mut removed = Vec::with_capacity(states.len());
        for state in states {
            let (tasks, empty) = {
                let bucket = self
                    .get_mut(&state)
                    .expect("selected typed Program cancellation state disappeared");
                let tasks =
                    bucket.take_matching(usize::MAX, ProgramSelectionOrder::Append, |task| {
                        activations.contains(&task.activation)
                    });
                (tasks, bucket.is_empty())
            };
            assert!(
                !tasks.is_empty(),
                "typed Program cancellation selected an empty state"
            );
            if empty {
                self.deactivate(state);
            }
            removed.push((state, tasks));
        }
        removed
    }

    /// Removes the newest task matching one scheduler-owned predicate while
    /// preserving every retained bucket's append order.
    fn take_one_matching(
        &mut self,
        mut matches: impl FnMut(&ProgramTask) -> bool,
    ) -> Option<(DeltaStateId, ProgramTask)> {
        let state = self
            .iter()
            .filter_map(|(state, bucket)| bucket.tasks.iter().any(&mut matches).then_some(state))
            .last()?;
        let (mut selected, empty) = {
            let bucket = self
                .get_mut(&state)
                .expect("selected typed Program wake state disappeared");
            let selected =
                bucket.take_matching(1, ProgramSelectionOrder::Lifo, |task| matches(task));
            (selected, bucket.is_empty())
        };
        if empty {
            self.deactivate(state);
        }
        let task = selected
            .pop()
            .expect("selected typed Program wake predicate became false");
        Some((state, task))
    }
}

#[cfg(test)]
impl std::ops::Index<&DeltaStateId> for ProgramWorklist {
    type Output = ProgramBucket;

    fn index(&self, state: &DeltaStateId) -> &Self::Output {
        self.get(state)
            .expect("typed Program worklist has no active state")
    }
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
        lane: Option<ProgramServiceLane>,
        terminal_selection_slots: &mut AHashMap<ActivationId, usize>,
        terminal_selections: &mut Vec<TerminalActivationSelection>,
    ) -> ProgramSelection {
        let hot = match lane {
            Some(lane) => self
                .tasks
                .iter()
                .rev()
                .find(|task| ProgramServiceLane::of(registry, task.activation) == lane)
                .expect("selected typed Program state lost its preferred service lane"),
            None => self.last().expect("typed program bucket is nonempty"),
        };
        let key = ProgramCohortKey::of(registry, hot);
        let matches = |task: &ProgramTask| {
            ProgramCohortKey::of(registry, task) == key
                && lane.is_none_or(|lane| ProgramServiceLane::of(registry, task.activation) == lane)
        };
        match key.class {
            ProgramCohortClass::Search { .. } => {
                let width = registry.source_dispatch_width(hot.activation, search_width);
                let tasks = self.take_matching(width, ProgramSelectionOrder::Lifo, matches);
                let limits = even_limits(width, tasks.len());
                ProgramSelection { key, tasks, limits }
            }
            ProgramCohortClass::ActivationStreaming => {
                let width = registry.transition_dispatch_width(hot.activation, search_width);
                let tasks = self.take_matching(width, ProgramSelectionOrder::Append, matches);
                let limits = even_limits(width, tasks.len());
                ProgramSelection { key, tasks, limits }
            }
            ProgramCohortClass::ActivationQuiescent => {
                let width = registry.transition_dispatch_width(hot.activation, search_width);
                let activation_width = activation_width.max(1);
                let mut activations = AHashSet::new();
                let tasks = self.take_matching(width, ProgramSelectionOrder::Append, |task| {
                    if !matches(task) {
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
                lane,
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
        lane: Option<ProgramServiceLane>,
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
                || lane
                    .is_some_and(|lane| ProgramServiceLane::of(registry, task.activation) != lane)
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
            let selection = (ProgramCohortKey::of(registry, &task) == key
                && lane
                    .is_none_or(|lane| ProgramServiceLane::of(registry, task.activation) == lane))
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
    /// Attribution chosen by the Program scheduler that formed this packet.
    /// Carrying it with the physical receipt avoids reclassifying affine
    /// reducers after selection.
    program_service_lane: Option<ProgramServiceLane>,
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
            program_service_lane: None,
        }
    }

    fn with_program_service_lane(mut self, lane: Option<ProgramServiceLane>) -> Self {
        self.program_service_lane = lane;
        self
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
    /// Authoritative per-input Program work validated from typed page
    /// receipts. Family source/transition counters are telemetry only.
    validated_program_examined: usize,
}

enum DeltaSettlement {
    Completed(CompletedActivation),
    Retargeted(ActiveDeltaContinuation),
}

/// Exact affine handoffs emitted by one physical delta step.
///
/// Directed chain execution overwhelmingly transfers a single activation at
/// a time. Keep that receipt inline while preserving expected constant-time
/// lookup for the genuinely wider reducer cohorts.
#[derive(Debug, Default)]
pub(super) enum RetargetedActivations {
    #[default]
    Empty,
    One(ActivationId, ActiveDeltaContinuation),
    Many(AHashMap<ActivationId, ActiveDeltaContinuation>),
}

impl RetargetedActivations {
    fn insert(
        &mut self,
        activation: ActivationId,
        continuation: ActiveDeltaContinuation,
    ) -> Option<ActiveDeltaContinuation> {
        match self {
            Self::Empty => {
                *self = Self::One(activation, continuation);
                None
            }
            Self::One(existing, previous) if *existing == activation => {
                Some(std::mem::replace(previous, continuation))
            }
            Self::One(existing, previous) => {
                let mut entries = AHashMap::with_capacity(2);
                assert!(entries.insert(*existing, *previous).is_none());
                assert!(entries.insert(activation, continuation).is_none());
                *self = Self::Many(entries);
                None
            }
            Self::Many(entries) => entries.insert(activation, continuation),
        }
    }

    fn get(&self, activation: &ActivationId) -> Option<&ActiveDeltaContinuation> {
        match self {
            Self::Empty => None,
            Self::One(existing, continuation) => (existing == activation).then_some(continuation),
            Self::Many(entries) => entries.get(activation),
        }
    }

    fn contains_key(&self, activation: &ActivationId) -> bool {
        self.get(activation).is_some()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_, _) => 1,
            Self::Many(entries) => entries.len(),
        }
    }

    #[cfg(test)]
    fn values(&self) -> Box<dyn Iterator<Item = &ActiveDeltaContinuation> + '_> {
        match self {
            Self::Empty => Box::new(std::iter::empty()),
            Self::One(_, continuation) => Box::new(std::iter::once(continuation)),
            Self::Many(entries) => Box::new(entries.values()),
        }
    }
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

fn validated_program_examined(
    pages: &[ProgramPage],
    receipt_local_fused_total: Option<usize>,
) -> Vec<usize> {
    let mut examined: Vec<_> = pages.iter().map(|page| page.examined).collect();
    if let Some(total) = receipt_local_fused_total {
        assert_eq!(
            examined.len(),
            1,
            "receipt-local fusion must retain one affine input"
        );
        assert!(
            total >= examined[0],
            "receipt-local fused total fell below its final validated page"
        );
        examined[0] = total;
    }
    examined
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
    pub(super) retargeted: RetargetedActivations,
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
    /// A newly runnable Support sibling invalidated the directed Exact lease.
    /// The Exact activation remains live and runnable, but must return to
    /// global arbitration rather than retaining scalar priority.
    pub(super) release_directed_lease: bool,
    /// A public-pull demand token assigned during this step. Unlike Exact
    /// credit wakeups, D owns an explicit latency preference.
    pub(super) demand_preference: Option<ActiveDeltaContinuation>,
}

/// One attempt to advance the global cyclic frontier.
///
/// `Deferred` is an executor signal, not semantic exhaustion: this shard has
/// already contributed to the still-open query-global service packet and must
/// be rescheduled after that packet closes.
pub(super) enum DeltaSchedulerStep {
    Deferred {
        packet_nonce: u64,
    },
    Completed {
        outcome: DeltaStepOutcome,
        global_packet_event: Option<GlobalServicePacketEvent>,
    },
}

pub(super) enum GlobalServicePacketEvent {
    StillOpen { packet_nonce: u64 },
    Closed,
}

impl DeltaStepOutcome {
    pub(super) fn has_stable_effect(&self) -> bool {
        self.continuation.is_some() || self.publication.is_some()
    }

    /// Releases a directed lease whose affine work remains scheduler-owned
    /// but is deliberately ineligible for physical dispatch.
    fn parked_lease_release() -> Self {
        Self {
            continuation: None,
            publication: None,
            completed_activation_ids: Vec::new(),
            retargeted: RetargetedActivations::default(),
            dead_pages: 0,
            source_dead_pages: 0,
            transition_dead_pages: 0,
            completed_activations: 0,
            completed_transition_cohort: false,
            allows_global_width_growth: false,
            release_directed_lease: false,
            demand_preference: None,
        }
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
    /// The activation remains live, but all of its affine work is deliberately
    /// parked outside the runnable scheduler frontier.
    Parked,
    /// The activation remains ordinary runnable custody, but another lineage
    /// must receive global arbitration before it may resume.
    Released,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicPullDemandState {
    Closed,
    Unassigned,
    Assigned,
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
    program_worklist: ProgramWorklist,
    /// Exact physical count of runnable PositiveSupport tasks per semantic
    /// parent.
    ///
    /// This is a noncanonical index over `program_worklist`, maintained for
    /// both scheduling policies. It owns no admission or semantic authority;
    /// it only answers whether ordinary runnable custody already exists
    /// without repeatedly scanning every Program bucket.
    runnable_positive_support_tasks: AHashMap<PositiveConfirmParentId, usize>,
    /// Affine PositiveSupport custody that is live but not runnable.
    ///
    /// Parked hedges do not own semantic completeness: their exact Confirm
    /// parents remain on the ordinary runnable frontier. Keeping the opaque
    /// handles in the same state-indexed shape lets cancellation and cloning
    /// preserve their exact typed runtime without exposing them to global pop.
    parked_positive_support_worklist: ProgramWorklist,
    /// Noncanonical packet-attribution fence. The default is unrestricted;
    /// the experimental query-global scheduler can opt in without refining a
    /// canonical Program state or physical call key.
    program_lane_selection: ProgramLaneSelection,
    /// One-shot global lane preference armed when parked PositiveSupport work
    /// becomes runnable. It is consumed only by a global Program pop, which
    /// may select a different canonical state from the ordinary hot tail.
    next_program_lane: Option<ProgramServiceLane>,
    /// Exact state selected at the same scheduler boundary as
    /// `next_program_lane`. Manual test policies may omit it and use the
    /// ordinary lane lookup fallback.
    next_program_state: Option<DeltaStateId>,
    /// Support tasks moved out of parked custody for the current global
    /// packet but not yet selected. Zero is the allocation-free common path;
    /// a positive remainder is reparked after the selected receipt settles.
    global_service_woken_support_tasks: usize,
    /// Reused parent scratch for service-locator filing and global Support
    /// wakes. Both operations are serialized scheduler boundaries, so one
    /// allocation can be amortized across them without becoming custody.
    positive_support_parent_scratch: Vec<PositiveConfirmParentId>,
    /// Newly parked service parents that have not yet consumed public demand.
    ///
    /// This is noncanonical locator custody, not scheduling currency. Filing a
    /// parked Support task registers its parent once; a public pull pops and
    /// revalidates one candidate. Stale entries from cancellation are harmless,
    /// while the ordinary no-candidate pull remains O(1) instead of rescanning
    /// every parked task.
    positive_support_service_demand_queue: Vec<PositiveConfirmParentId>,
    positive_support_service_demand_queued: AHashSet<PositiveConfirmParentId>,
    /// Boundary probes performed while filing service-locator parents.
    ///
    /// Test-only because this observes a physical optimization boundary, not
    /// query semantics or production scheduling currency.
    #[cfg(test)]
    positive_support_service_locator_boundary_probes: usize,
    /// Mutually exclusive PositiveSupport admission currency.
    positive_support_scheduling: PositiveSupportScheduling,
    /// One aggregate affine query-run lease, direct until the first real
    /// parallel split and shared only between the resulting siblings.
    positive_support_service_debt: Option<PositiveSupportServiceDebtRuntime>,
    /// One public pull may carry one demand token while the machine searches
    /// for a concrete parked Support parent. Assignment consumes the token
    /// permanently into that parent's conservation ledger, while retaining
    /// the `Assigned` state until the public pull closes so repeated internal
    /// begin calls cannot mint another D.
    public_pull_demand: PublicPullDemandState,
    program_runtimes: AHashMap<DeltaStateId, ProgramRuntime>,
    /// Program-only cohort scratch is lazy so non-Program queries retain the
    /// baseline scheduler footprint. One allocation is amortized across all
    /// Program steps in the query.
    program_scratch: Option<Box<ProgramSchedulerScratch>>,
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
            program_worklist: ProgramWorklist::default(),
            runnable_positive_support_tasks: AHashMap::new(),
            parked_positive_support_worklist: ProgramWorklist::default(),
            program_lane_selection: ProgramLaneSelection::Unrestricted,
            next_program_lane: None,
            next_program_state: None,
            global_service_woken_support_tasks: 0,
            positive_support_parent_scratch: Vec::new(),
            positive_support_service_demand_queue: Vec::new(),
            positive_support_service_demand_queued: AHashSet::new(),
            #[cfg(test)]
            positive_support_service_locator_boundary_probes: 0,
            positive_support_scheduling: PositiveSupportScheduling::CountCredit,
            positive_support_service_debt: None,
            public_pull_demand: PublicPullDemandState::Closed,
            program_runtimes: AHashMap::new(),
            program_scratch: None,
            activation_width: 1,
            terminal_selection_slots: AHashMap::new(),
            terminal_selections: Vec::new(),
        }
    }

    pub(super) fn enable_positive_support_global_service_debt(&mut self) {
        assert_eq!(
            self.positive_support_scheduling,
            PositiveSupportScheduling::CountCredit,
            "PositiveSupport scheduling policy was already configured"
        );
        assert!(
            self.registry.state.activations.is_empty()
                && self.worklist.is_empty()
                && self.source_worklist.is_empty()
                && self.program_worklist.is_empty()
                && self.parked_positive_support_worklist.is_empty(),
            "PositiveSupport scheduling policy must be selected before execution"
        );
        self.positive_support_scheduling = PositiveSupportScheduling::GlobalServiceDebt;
        self.program_lane_selection = ProgramLaneSelection::LanePure;
        self.positive_support_service_debt = Some(PositiveSupportServiceDebtRuntime::dormant());
    }

    pub(super) fn global_service_lane_is_active(&self) -> bool {
        self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt
            && self
                .positive_support_service_debt
                .as_ref()
                .expect("global service policy lost its query-run coordinator")
                .global_lane_is_active()
    }

    #[cfg(feature = "parallel")]
    pub(super) fn uses_global_service_debt(&self) -> bool {
        self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt
    }

    #[cfg(feature = "parallel")]
    pub(super) fn global_service_active_packet_nonce(&self) -> Option<u64> {
        self.positive_support_service_debt
            .as_ref()
            .and_then(PositiveSupportServiceDebtRuntime::active_packet_nonce)
    }

    #[cfg(all(test, feature = "parallel"))]
    pub(super) fn install_global_service_dispatch_probe(
        &mut self,
        probe: GlobalServiceDispatchTestProbe,
    ) {
        self.positive_support_service_debt
            .as_mut()
            .expect("global service scheduler lost its coordinator")
            .install_dispatch_probe(probe);
    }

    fn lane_selection_is_active(&self) -> bool {
        self.program_lane_selection == ProgramLaneSelection::LanePure
            && (self.positive_support_scheduling == PositiveSupportScheduling::CountCredit
                || self.global_service_lane_is_active())
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
        // PositiveSupport is a latency hedge, never a completeness owner.
        // Parked custody therefore cannot keep the semantic scheduler alive;
        // every live hedge has an exact Confirm parent on the runnable path.
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
    pub(super) fn seed_program_confirms<'a>(
        &mut self,
        spec: ProgramRef<'a>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        successor: StateDesc,
        set_admit_result: bool,
        batch: CandidateBatch,
        positive_publication: Option<PositivePublicationSeed<'a>>,
        stats: &mut ResidualStateStats,
    ) -> DeltaSeedOutcome {
        let confirm_state = self.prepare_program(desc, route, spec);
        let stride = successor.bound.count();
        let parent_count = batch.parents.row_count;
        let (parents, candidate_groups) = batch.into_parent_candidates();
        let mut activations = Vec::with_capacity(parent_count);
        for (row, original) in candidate_groups.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let first_candidate = original.first().copied();
            let original = shared_one_parent_candidates(original);
            let activation = self.registry.open_program_activation(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                    set_admit_result,
                },
                None,
                None,
            );
            activations.push((activation, first_candidate));
        }
        let program_activations: Vec<_> = activations
            .iter()
            .map(|(activation, _)| ProgramActivation(activation.0))
            .collect();
        let vars: Vec<_> = successor.bound.into_iter().collect();
        let view = rows_view(&vars, &parents.rows, parent_count);
        let mut seeded = ProgramSeedEffects::default();
        spec.seed_batch(
            self.program_runtimes
                .get_mut(&confirm_state)
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
        let mut support_activations = Vec::new();
        for ((activation, first_candidate), range) in activations.into_iter().zip(ranges) {
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
            } else if let (Some(publication), Some(value)) =
                (positive_publication.as_ref(), first_candidate)
            {
                let parent = if publication.support_hedge.is_some() {
                    self.registry.open_exact_and_support_publication(
                        activation,
                        publication.confirm_state,
                        publication.certificate,
                    )
                } else {
                    self.registry.open_exact_only_publication(
                        activation,
                        publication.confirm_state,
                        publication.certificate,
                    )
                }
                .expect("eligible live Confirm rejected its positive publication ledger");
                if let Some(support) = publication.support_hedge.as_ref() {
                    let child = self
                        .registry
                        .open_positive_support_activation(
                            parent,
                            0,
                            value,
                            support.support_variables,
                            support.direct_terminal_full,
                        )
                        .expect("eligible Confirm occurrence rejected its positive Support child");
                    support_activations.push(child);
                }
            }
        }
        if !retired.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&confirm_state)
                    .expect("prepared program lost its runtime"),
                route.key,
                &retired,
            );
        }
        // Exact Confirm work is never displaced by the hedge. Filing it first
        // preserves the complete fallback while the subsequently prepared
        // Support family receives the hot state/tail.
        let graph_active = self.file_program_state(confirm_state, tasks);

        let mut support_active = None;
        let mut completed_activation_ids = Vec::new();
        if !support_activations.is_empty() {
            let Some(PositivePublicationSeed {
                support_hedge: Some(support),
                ..
            }) = positive_publication
            else {
                unreachable!("positive Support activations lost their physical feeder")
            };
            let PositiveSupportSeed {
                spec,
                desc,
                request,
                route,
                ..
            } = support;
            let support_state = self.prepare_program(desc, route, spec);
            let mut rows = Vec::with_capacity(
                support_activations
                    .len()
                    .checked_mul(request.bound.count())
                    .expect("positive Support seed row capacity overflow"),
            );
            for &activation in &support_activations {
                let (bound, row, candidates) = self.registry.source_context(activation);
                assert_eq!(bound, request.bound);
                assert!(
                    candidates.is_none(),
                    "positive Support child exposed a candidate set"
                );
                rows.extend_from_slice(row);
            }
            let vars: Vec<_> = request.bound.into_iter().collect();
            let program_activations: Vec<_> = support_activations
                .iter()
                .map(|activation| ProgramActivation(activation.0))
                .collect();
            let view = rows_view(&vars, &rows, support_activations.len());
            let mut seeded = ProgramSeedEffects::default();
            spec.seed_batch(
                self.program_runtimes
                    .get_mut(&support_state)
                    .expect("prepared positive Support program lost its runtime"),
                ProgramSeedBatch {
                    request,
                    route,
                    view,
                    activations: &program_activations,
                },
                &mut seeded,
            );
            let ranges = program_seed_ranges(&seeded.work, support_activations.len());
            let mut tasks = Vec::with_capacity(seeded.work.len());
            let mut retired = Vec::new();
            for (activation, range) in support_activations.into_iter().zip(ranges) {
                let seeds = &seeded.work[range];
                if seeds.iter().any(|seed| seed.accepted.is_some()) {
                    // Nullable seed acceptance is not a runtime Support
                    // witness. Affinely discard its uninstalled typed work,
                    // then retire the creditless physical child through
                    // ordinary quiescence.
                    let program_activation = ProgramActivation(activation.0);
                    let runtime = self
                        .program_runtimes
                        .get_mut(&support_state)
                        .expect("positive Support program lost its runtime");
                    for seed in seeds {
                        spec.discard_work(runtime, route.key, program_activation, &seed.work);
                    }
                    let installed = self
                        .registry
                        .install_program_roots(activation, std::iter::empty());
                    let proof = installed
                        .quiescence
                        .expect("empty positive Support install remained live");
                    let completed = self.registry.finish(proof);
                    assert_eq!(completed.effect, DeltaCompletion::Cleanup);
                    completed_activation_ids.push(completed.activation);
                    retired.push(ProgramActivation(activation.0));
                    continue;
                }
                let installed = self
                    .registry
                    .install_program_roots(activation, seeds.iter().cloned());
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
                    retired.push(ProgramActivation(activation.0));
                }
            }
            if !retired.is_empty() {
                spec.retire_activations(
                    self.program_runtimes
                        .get_mut(&support_state)
                        .expect("positive Support program lost its runtime"),
                    route.key,
                    &retired,
                );
            }
            let _ = self.file_parked_positive_support_state(support_state, tasks);
            support_active = self.assign_public_pull_demand(stats);
        }

        DeltaSeedOutcome {
            continuation: None,
            publication: None,
            // A newly assigned public demand token explicitly prefers the
            // Support hedge. Without demand the exact Confirm remains the
            // directed latency lineage and Support stays parked.
            active: (!self.global_service_lane_is_active())
                .then(|| support_active.or(finalizer_active).or(graph_active))
                .flatten(),
            terminal_activations: Vec::new(),
            completed_activation_ids,
            terminal_family: None,
            seeded_parents: parent_count,
        }
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
                    prefer_continuation(&mut continuation, released.stable.continuation);
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
            let released = self.release_completion(completed, plan, stable, stable_interner, stats);
            prefer_continuation(&mut continuation, released.continuation);
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
                    prefer_continuation(&mut continuation, released.stable.continuation);
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
            let released = self.release_completion(completed, plan, stable, stable_interner, stats);
            prefer_continuation(&mut continuation, released.continuation);
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
                FormulaReducerSeed::SetAdmit(seed) if seed.destination.parent_count() > 1 => {
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
                    let singletons = seed
                        .batch
                        .into_structural_singletons_with_input(seed.bound.count(), seed.input);
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
                    let state = self.prepare_engine_program(EngineProgramKind::FormulaOrAdmit);
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
                    let singletons = seed.batch.into_structural_singletons(seed.bound.count());
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
                    let state = self.prepare_engine_program(EngineProgramKind::FormulaOrEmit);
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
        let drained = self.drain_formula_reducer_seeds(seeds, plan, stable, stable_interner, stats);
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
            (DeltaReturn::PositiveSupport { .. }, DeltaCompletion::Cleanup) => {
                // Positive truth is released only by the semantic Confirm
                // parent's affine publication grant. Its physical Support
                // child always retires as an inert cleanup.
                FormulaReducerDrain::default()
            }
            (_, DeltaCompletion::Cleanup) => {
                // A streaming activation has already resumed one affine copy
                // of its continuation per semantic effect. Quiescence only
                // retires producer credits; replaying the template here would
                // duplicate publication.
                FormulaReducerDrain::default()
            }
            (return_to, DeltaCompletion::Support(truth)) => {
                self.release_support(return_to, truth, plan, stable, stable_interner, stats)
            }
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
                        file_with_plan(stable, stable_interner, plan, successor, bucket, stats)
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
                let mut drained =
                    self.drain_formula_reducer_seeds(seeds, plan, stable, stable_interner, stats);
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
                let mut drained =
                    self.drain_formula_reducer_seeds(seeds, plan, stable, stable_interner, stats);
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
                let mut drained =
                    self.drain_formula_reducer_seeds(seeds, plan, stable, stable_interner, stats);
                prefer_continuation(&mut drained.continuation, continuation);
                drained
            }
            (DeltaReturn::FormulaOrAdmit { .. }, effect)
            | (DeltaReturn::FormulaOrEmit { .. }, effect)
            | (DeltaReturn::PositiveSupport { .. }, effect)
            | (DeltaReturn::SetAdmission { .. }, effect) => {
                panic!("engine reducer completed with incompatible effect: {effect:?}")
            }
            (DeltaReturn::Stable { .. } | DeltaReturn::Formula { .. }, effect) => {
                panic!("ordinary delta reducer completed with incompatible effect: {effect:?}")
            }
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn release_positive_publication(
        grant: PositivePublicationGrant,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStableEffects {
        let PositivePublicationGrant {
            value,
            return_to,
            route,
            source,
        } = grant;
        match source {
            PositivePublicationSource::ExactConfirmTap => {
                stats.delta_positive_publication_exact_wins += 1;
            }
            PositivePublicationSource::SupportHedge => {
                stats.delta_positive_publication_support_wins += 1;
            }
        }
        let DeltaReturn::Stable { desc, parent, .. } = return_to else {
            unreachable!("a preflighted positive publication lost its Stable return")
        };
        let batch = CandidateBatch {
            parents: RowBatch {
                rows: parent.into_vec(),
                row_count: 1,
            },
            candidates: CandidatePayload::Values(vec![value]),
        };
        match route {
            PositivePublicationRoute::Terminal {
                origin,
                full,
                registration,
            } => {
                stats.delta_positive_publication_terminal_commits += 1;
                let ResidualPhase::Candidate { variable, .. } = &desc.phase else {
                    unreachable!("a preflighted Terminal publication lost its Candidate return")
                };
                let (committed, rows) = committed_candidate_rows(desc.bound, *variable, batch);
                debug_assert_eq!(
                    committed, full,
                    "a preflighted Terminal publication changed its full schema"
                );
                debug_assert_eq!(
                    rows.row_count, 1,
                    "a positive Terminal singleton did not commit exactly one row"
                );
                DeltaStableEffects {
                    continuation: None,
                    publication: Some(TerminalPublicationBatch::new_with_registration(
                        origin,
                        rows,
                        registration,
                    )),
                }
            }
            PositivePublicationRoute::ChunkHomomorphic => {
                stats.delta_positive_publication_chunk_homomorphic_commits += 1;
                // Preflight proved a Stable Candidate descriptor, and this
                // function constructs exactly one parent with one candidate.
                // `file_with_plan` can therefore return `None` only if that
                // nonempty invariant was broken internally.
                let continuation = file_with_plan(
                    stable,
                    stable_interner,
                    plan,
                    desc,
                    StateBucket::Candidates(batch),
                    stats,
                )
                .expect("a positive ChunkHomomorphic singleton filed no continuation");
                DeltaStableEffects {
                    continuation: Some(continuation),
                    publication: None,
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_streaming(
        &mut self,
        activation: ActivationId,
        streamed: DeltaStreamingReturn,
        accepted: Vec<RawInline>,
        direct_terminal_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStreamingRelease {
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
        debug_assert!(!accepted.is_empty());
        stats.candidates_proposed += accepted.len();
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted.len());
        let candidates = CandidatePayload::Values(accepted);
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
            let (committed, rows) = committed_candidate_rows(
                desc.bound,
                *variable,
                CandidateBatch {
                    parents: RowBatch {
                        rows: parent.into_vec(),
                        row_count: 1,
                    },
                    candidates,
                },
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
            | DeltaReturn::PositiveSupport { .. }
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
        let mut drained =
            self.drain_formula_reducer_seeds(reducer_seeds, plan, stable, stable_interner, stats);
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

    fn index_runnable_positive_support_tasks(&mut self, tasks: &[ProgramTask]) {
        for task in tasks {
            let Some(parent) = self
                .registry
                .positive_support_parent_for_child(task.activation)
            else {
                continue;
            };
            let count = self
                .runnable_positive_support_tasks
                .entry(parent)
                .or_default();
            *count = count
                .checked_add(1)
                .expect("runnable PositiveSupport task count overflow");
        }
    }

    fn unindex_runnable_positive_support_tasks(&mut self, tasks: &[ProgramTask]) {
        for task in tasks {
            let Some(parent) = self
                .registry
                .positive_support_parent_for_child(task.activation)
            else {
                continue;
            };
            let remove = {
                let count = self
                    .runnable_positive_support_tasks
                    .get_mut(&parent)
                    .expect("runnable PositiveSupport task was absent from its parent index");
                *count = count
                    .checked_sub(1)
                    .expect("runnable PositiveSupport task count underflow");
                *count == 0
            };
            if remove {
                assert_eq!(
                    self.runnable_positive_support_tasks.remove(&parent),
                    Some(0),
                    "empty runnable PositiveSupport parent index disappeared"
                );
            }
        }
    }

    fn take_runnable_program_activations(
        &mut self,
        activations: &AHashSet<ActivationId>,
    ) -> Vec<(DeltaStateId, Vec<ProgramTask>)> {
        let groups = self.program_worklist.take_activations(activations);
        for (_, tasks) in &groups {
            self.unindex_runnable_positive_support_tasks(tasks);
        }
        groups
    }

    #[cfg(test)]
    fn assert_runnable_positive_support_task_index(&self) {
        let mut actual = AHashMap::<PositiveConfirmParentId, usize>::new();
        for (_, bucket) in self.program_worklist.iter() {
            for task in &bucket.tasks {
                let Some(parent) = self
                    .registry
                    .positive_support_parent_for_child(task.activation)
                else {
                    continue;
                };
                *actual.entry(parent).or_default() += 1;
            }
        }
        assert_eq!(
            self.runnable_positive_support_tasks, actual,
            "runnable PositiveSupport parent index diverged from Program custody"
        );
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
        self.index_runnable_positive_support_tasks(&tasks);
        self.program_worklist.append(state, &mut tasks);
        Some(ActiveDeltaContinuation { state, activation })
    }

    /// Files typed PositiveSupport custody without making it globally
    /// runnable. Credits and opaque handles remain untouched and affine.
    fn file_parked_positive_support_state(
        &mut self,
        state: DeltaStateId,
        mut tasks: Vec<ProgramTask>,
    ) -> Option<ActiveDeltaContinuation> {
        let activation = tasks.last()?.activation;
        assert!(
            self.interner.program(state).is_some(),
            "parked positive Support task was filed under a legacy delta state"
        );
        assert!(
            tasks.iter().all(|task| {
                task.activation == task.credit.key.activation
                    && self.registry.is_live_positive_support(task.activation)
            }),
            "only live PositiveSupport Program tasks may enter the parked lane"
        );
        if self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt {
            self.positive_support_parent_scratch.clear();
            for task in &tasks {
                let parent = self
                    .registry
                    .positive_support_parent_for_child(task.activation)
                    .expect("live PositiveSupport task lost its semantic parent");
                if !self.registry.positive_support_service_is_started(parent)
                    && !self
                        .positive_support_service_demand_queued
                        .contains(&parent)
                {
                    self.positive_support_parent_scratch.push(parent);
                }
            }
            self.positive_support_parent_scratch.sort_unstable();
            self.positive_support_parent_scratch.dedup();
            for index in 0..self.positive_support_parent_scratch.len() {
                let parent = self.positive_support_parent_scratch[index];
                #[cfg(test)]
                {
                    self.positive_support_service_locator_boundary_probes += 1;
                }
                // Preserve the old discovery precondition at the custody
                // transition: one parent is offered for demand only after
                // its final runnable sibling has joined parked custody.
                if self.has_runnable_positive_support_parent(parent) {
                    continue;
                }
                if self.positive_support_service_demand_queued.insert(parent) {
                    self.positive_support_service_demand_queue.push(parent);
                }
            }
        }
        self.parked_positive_support_worklist
            .append(state, &mut tasks);
        Some(ActiveDeltaContinuation { state, activation })
    }

    fn has_runnable_positive_support_parent(&self, parent: PositiveConfirmParentId) -> bool {
        self.runnable_positive_support_tasks.contains_key(&parent)
    }

    /// Moves at most one parked task for this semantic parent onto the
    /// runnable frontier. Parent-local available credit and the absence of an
    /// existing runnable sibling are revalidated at the move boundary.
    fn wake_one_positive_support_parent(
        &mut self,
        parent: PositiveConfirmParentId,
    ) -> Option<ActiveDeltaContinuation> {
        let admitted = match self.positive_support_scheduling {
            PositiveSupportScheduling::CountCredit => {
                self.registry.positive_support_budget_available(parent) > 0
            }
            PositiveSupportScheduling::GlobalServiceDebt => {
                self.registry.positive_support_service_is_started(parent)
            }
        };
        if !admitted || self.has_runnable_positive_support_parent(parent) {
            return None;
        }
        let registry = &self.registry;
        let (state, task) = self
            .parked_positive_support_worklist
            .take_one_matching(|task| {
                registry.positive_support_parent_for_child(task.activation) == Some(parent)
            })?;
        let activation = task.activation;
        let active = self
            .file_program_state(state, vec![task])
            .expect("one parked PositiveSupport task failed to become runnable");
        if self.lane_selection_is_active() {
            debug_assert_eq!(
                ProgramServiceLane::of(&self.registry, activation),
                ProgramServiceLane::Support
            );
            self.next_program_lane = Some(ProgramServiceLane::Support);
            self.next_program_state = Some(state);
        }
        debug_assert_eq!(active.activation, activation);
        Some(active)
    }

    /// Consumes the query's one unassigned pull token into one concrete
    /// parked parent and immediately prefers that newly runnable Support
    /// lineage.
    fn assign_public_pull_demand(
        &mut self,
        stats: &mut ResidualStateStats,
    ) -> Option<ActiveDeltaContinuation> {
        if self.public_pull_demand != PublicPullDemandState::Unassigned {
            return None;
        }
        if self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt {
            while let Some(parent) = self.positive_support_service_demand_queue.pop() {
                assert!(
                    self.positive_support_service_demand_queued.remove(&parent),
                    "service-demand locator queue lost its unique membership"
                );
                if !self.registry.start_positive_support_service(parent) {
                    continue;
                }
                self.public_pull_demand = PublicPullDemandState::Assigned;
                stats.delta_positive_support_service_parents_started += 1;
                let epoch_started = self
                    .positive_support_service_debt
                    .as_mut()
                    .expect("global service demand lost its query-run coordinator")
                    .demand_arrived();
                if epoch_started {
                    stats.delta_positive_support_service_epochs += 1;
                }
                return None;
            }
            return None;
        }

        let mut parents = Vec::new();
        let mut seen = AHashSet::new();
        for (_, bucket) in self.parked_positive_support_worklist.iter() {
            for task in &bucket.tasks {
                stats.delta_positive_support_demand_discovery_task_visits += 1;
                let Some(parent) = self
                    .registry
                    .positive_support_parent_for_child(task.activation)
                else {
                    continue;
                };
                if !self.has_runnable_positive_support_parent(parent) && seen.insert(parent) {
                    parents.push(parent);
                }
            }
        }
        for parent in parents.into_iter().rev() {
            let started = self.registry.mint_positive_support_demand(parent);
            if !started {
                continue;
            }
            self.public_pull_demand = PublicPullDemandState::Assigned;
            stats.delta_positive_support_demand_assigned += 1;
            return Some(
                self.wake_one_positive_support_parent(parent)
                    .expect("assigned positive Support demand failed to wake parked custody"),
            );
        }
        None
    }

    /// Opens one idempotent public-pull demand token. It may remain
    /// unassigned while stable work runs and be consumed by a Support child
    /// seeded later in the same pull.
    pub(super) fn begin_public_pull_demand(
        &mut self,
        stats: &mut ResidualStateStats,
    ) -> Option<ActiveDeltaContinuation> {
        if self.public_pull_demand == PublicPullDemandState::Closed {
            self.public_pull_demand = PublicPullDemandState::Unassigned;
        }
        self.assign_public_pull_demand(stats)
    }

    /// Closes the current public-pull lifecycle. An unassigned token retires
    /// here; an assigned token remains D in its parent ledger but cannot cause
    /// a second assignment before this boundary.
    pub(super) fn retire_unassigned_public_pull_demand(&mut self) {
        self.public_pull_demand = PublicPullDemandState::Closed;
    }

    #[cfg(test)]
    pub(super) fn has_unassigned_public_pull_demand(&self) -> bool {
        self.public_pull_demand == PublicPullDemandState::Unassigned
    }

    fn wake_positive_support_parents(
        &mut self,
        parents: impl IntoIterator<Item = PositiveConfirmParentId>,
    ) -> bool {
        let mut woke = false;
        for parent in parents {
            woke |= self.wake_one_positive_support_parent(parent).is_some();
        }
        woke
    }

    /// Makes one queued task per started parent runnable so a global Support
    /// packet can batch across parents without admitting a second task from
    /// any one semantic race.
    fn wake_global_service_support_lane(&mut self) -> usize {
        self.positive_support_parent_scratch.clear();
        for (_, bucket) in self.parked_positive_support_worklist.iter() {
            for task in &bucket.tasks {
                let Some(parent) = self
                    .registry
                    .positive_support_parent_for_child(task.activation)
                else {
                    continue;
                };
                if self.registry.positive_support_service_is_started(parent) {
                    self.positive_support_parent_scratch.push(parent);
                }
            }
        }
        self.positive_support_parent_scratch.sort_unstable();
        self.positive_support_parent_scratch.dedup();
        let mut woke = 0;
        for index in 0..self.positive_support_parent_scratch.len() {
            let parent = self.positive_support_parent_scratch[index];
            woke += usize::from(self.wake_one_positive_support_parent(parent).is_some());
        }
        woke
    }

    fn assert_local_global_service_pair(&self) {
        // Production reachability makes this pair structural. A positive
        // publication is opened only after nonquiescent Exact roots exist,
        // those roots are filed before Support is parked, and every Exact
        // quiescence/positive path retires the linked Support children before
        // returning. Conversely, losing the final Support child makes the
        // parent ineligible for service start. Therefore a service-started
        // live parent can expose neither lane without the other.
        let exact = self
            .program_worklist
            .last_id_in_lane(&self.registry, ProgramServiceLane::Exact)
            .is_some();
        let support = self
            .program_worklist
            .last_id_in_lane(&self.registry, ProgramServiceLane::Support)
            .is_some()
            || self
                .parked_positive_support_worklist
                .last_id_in_lane(&self.registry, ProgramServiceLane::Support)
                .is_some();
        assert!(
            exact && support,
            "an active global-service shard must furnish both Exact and Support lanes"
        );
    }

    /// Arms the already-reserved query-global Program lane in this shard.
    ///
    /// Reservation precedes local wake/pop. Overlapping shards receive
    /// distinct affine parts of the same coalesced packet and therefore move
    /// only work from its one query-global selected lane.
    fn arm_global_service_lane(&mut self, lane: Option<ProgramServiceLane>) {
        self.next_program_lane = None;
        self.next_program_state = None;
        self.global_service_woken_support_tasks = 0;
        match lane {
            Some(ProgramServiceLane::Support) => {
                self.global_service_woken_support_tasks = self.wake_global_service_support_lane();
                assert!(
                    self.global_service_woken_support_tasks > 0,
                    "reserved Support service lane had no parked task"
                );
                let state = self
                    .program_worklist
                    .last_id_in_lane(&self.registry, ProgramServiceLane::Support)
                    .expect("reserved Support service lane lost its queued packet");
                self.next_program_lane = Some(ProgramServiceLane::Support);
                self.next_program_state = Some(state);
            }
            Some(ProgramServiceLane::Exact) => {
                let state = self
                    .program_worklist
                    .last_id_in_lane(&self.registry, ProgramServiceLane::Exact)
                    .expect("reserved Exact service lane lost its queued packet");
                self.next_program_lane = Some(ProgramServiceLane::Exact);
                self.next_program_state = Some(state);
            }
            Some(ProgramServiceLane::Neutral) => unreachable!(),
            None => {}
        }
    }

    fn reserve_global_service_packet(&mut self) -> PositiveSupportServiceTurn {
        if self.positive_support_scheduling != PositiveSupportScheduling::GlobalServiceDebt {
            return PositiveSupportServiceTurn::Inactive;
        }
        let local_live = self.registry.has_live_started_positive_support();
        match self
            .positive_support_service_debt
            .as_mut()
            .expect("global service policy lost its query-run coordinator")
            .try_reserve_turn(local_live)
        {
            PositiveSupportServiceTurn::Inactive => {
                self.arm_global_service_lane(None);
                PositiveSupportServiceTurn::Inactive
            }
            PositiveSupportServiceTurn::Deferred(packet_nonce) => {
                self.arm_global_service_lane(None);
                PositiveSupportServiceTurn::Deferred(packet_nonce)
            }
            PositiveSupportServiceTurn::Reserved(turn) => {
                self.assert_local_global_service_pair();
                self.arm_global_service_lane(Some(turn.lane()));
                PositiveSupportServiceTurn::Reserved(turn)
            }
        }
    }

    fn settle_global_service_packet(
        &mut self,
        lane: ProgramServiceLane,
        reservation: Option<PositiveSupportServicePacketReservation>,
        examined: usize,
        stats: &mut ResidualStateStats,
    ) -> Option<GlobalServicePacketEvent> {
        if self.positive_support_scheduling != PositiveSupportScheduling::GlobalServiceDebt
            || lane == ProgramServiceLane::Neutral
        {
            assert!(
                reservation.is_none(),
                "neutral/count dispatch retained a global service packet part"
            );
            return None;
        }
        // A quiescent typed receipt may validate zero candidates while still
        // consuming one physical dispatch. Service ticks are therefore
        // `max(1, validated examined)`; zero-cost packets are forbidden by the
        // debt theorem rather than silently becoming free.
        let charged = examined.max(1);
        let service = u64::try_from(charged).expect("Program service does not fit u64");
        let reservation = reservation.expect("attributable service packet lost its global turn");
        let shared = matches!(
            &reservation,
            PositiveSupportServicePacketReservation::Shared(_)
        );
        let settlement = self
            .positive_support_service_debt
            .as_mut()
            .expect("service settlement lost its query-run account")
            .settle(
                reservation,
                service,
                self.registry.has_live_started_positive_support(),
            );
        assert_eq!(
            settlement.lane, lane,
            "global service packet part crossed its selected Program lane"
        );
        let PositiveSupportGlobalSettlement {
            lane: _,
            packet_nonce,
            closed_packet,
        } = settlement;
        let packet_closed = closed_packet.is_some();
        match lane {
            ProgramServiceLane::Exact => {
                stats.delta_positive_support_service_exact_examined += charged;
                if let Some(closed) = closed_packet {
                    let packet = usize::try_from(closed.service)
                        .expect("coalesced Exact service packet does not fit usize");
                    let prior_max = usize::try_from(closed.prior_max_packet)
                        .expect("prior Exact service packet maximum does not fit usize");
                    stats.delta_positive_support_service_exact_packets += 1;
                    stats.max_delta_positive_support_service_exact_packet = stats
                        .max_delta_positive_support_service_exact_packet
                        .max(packet);
                    stats.delta_positive_support_service_exact_packet_allowance +=
                        packet.saturating_sub(prior_max);
                }
            }
            ProgramServiceLane::Support => {
                stats.delta_positive_support_service_support_examined += charged;
                if let Some(closed) = closed_packet {
                    let packet = usize::try_from(closed.service)
                        .expect("coalesced Support service packet does not fit usize");
                    let prior_max = usize::try_from(closed.prior_max_packet)
                        .expect("prior Support service packet maximum does not fit usize");
                    stats.delta_positive_support_service_support_packets += 1;
                    stats.max_delta_positive_support_service_support_packet = stats
                        .max_delta_positive_support_service_support_packet
                        .max(packet);
                    stats.delta_positive_support_service_support_packet_allowance +=
                        packet.saturating_sub(prior_max);
                }
                // `step_program` files every live PositiveSupport recurrence
                // directly into parked custody. Only tasks woken for another
                // parent/state but excluded from this physical packet need a
                // repark pass; the one-parent common path stays scan-free.
                if self.global_service_woken_support_tasks > 0 {
                    let runnable: AHashSet<_> = self
                        .program_worklist
                        .iter()
                        .flat_map(|(_, bucket)| bucket.tasks.iter())
                        .filter(|task| {
                            ProgramServiceLane::of(&self.registry, task.activation)
                                == ProgramServiceLane::Support
                        })
                        .map(|task| task.activation)
                        .collect();
                    assert_eq!(
                        runnable.len(),
                        self.global_service_woken_support_tasks,
                        "global Support wake remainder lost affine custody"
                    );
                    self.park_positive_support_activations(&runnable);
                }
                self.global_service_woken_support_tasks = 0;
                self.next_program_lane = None;
                self.next_program_state = None;
            }
            ProgramServiceLane::Neutral => unreachable!(),
        }
        shared.then_some(if packet_closed {
            GlobalServicePacketEvent::Closed
        } else {
            GlobalServicePacketEvent::StillOpen { packet_nonce }
        })
    }

    /// Moves already queued PositiveSupport tasks out of runnable selection
    /// while retaining their exact state grouping and affine credits.
    #[cfg_attr(not(test), allow(dead_code))]
    fn park_positive_support_activations(&mut self, activations: &AHashSet<ActivationId>) {
        for &activation in activations {
            assert!(
                self.registry.is_live_positive_support(activation),
                "only live PositiveSupport activations may be parked"
            );
        }
        let groups = self.take_runnable_program_activations(activations);
        for (state, tasks) in groups {
            let _ = self.file_parked_positive_support_state(state, tasks);
        }
    }

    /// Discards every queued opaque handle for the selected PositiveSupport
    /// hedges, consumes their issued producer credits, and retires each child
    /// through ordinary cleanup quiescence.
    ///
    /// Exact Confirm work is intentionally unreachable from this path. The
    /// caller chooses children from a parent-owned link list, and both the
    /// scheduler and registry revalidate the distinct PositiveSupport reducer
    /// before consuming any physical custody.
    fn retire_positive_support_activations<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        requested: &AHashSet<ActivationId>,
    ) -> (Vec<ActivationId>, usize) {
        let live: AHashSet<_> = requested
            .iter()
            .copied()
            .filter(|&activation| self.registry.is_live_positive_support(activation))
            .collect();
        if live.is_empty() {
            return (Vec::new(), 0);
        }
        let parents: BTreeSet<_> = live
            .iter()
            .filter_map(|&child| self.registry.positive_support_parent_for_child(child))
            .collect();

        let mut groups = BTreeMap::<DeltaStateId, Vec<ProgramTask>>::new();
        let runnable = self.take_runnable_program_activations(&live);
        let parked = self
            .parked_positive_support_worklist
            .take_activations(&live);
        for (state, mut tasks) in runnable.into_iter().chain(parked) {
            groups.entry(state).or_default().append(&mut tasks);
        }
        let mut completed = Vec::with_capacity(live.len());
        for (state, tasks) in groups {
            let address = self
                .interner
                .program(state)
                .cloned()
                .expect("cancelled PositiveSupport work occupied a legacy delta state");
            let spec = address.resolve(root, plan);
            let key = address.key();
            let mut retired = Vec::new();
            let mut seen = AHashSet::new();
            let mut proofs = Vec::new();
            {
                let runtime = self
                    .program_runtimes
                    .get_mut(&state)
                    .expect("cancelled PositiveSupport work lost its typed runtime");
                for task in tasks {
                    assert!(
                        live.contains(&task.activation),
                        "PositiveSupport cancellation removed an untargeted task"
                    );
                    let program_activation = ProgramActivation(task.activation.0);
                    spec.discard_work(runtime, key, program_activation, &task.work);
                    if seen.insert(task.activation) {
                        retired.push(program_activation);
                    }
                    if let Some(proof) = self
                        .registry
                        .retire_positive_support_program_credit(task.credit)
                    {
                        proofs.push(proof);
                    }
                }
                spec.retire_activations(runtime, key, &retired);
            }
            for proof in proofs {
                let completed_activation = self.registry.finish(proof);
                assert!(
                    matches!(
                        (
                            &completed_activation.return_to,
                            &completed_activation.effect
                        ),
                        (
                            DeltaReturn::PositiveSupport { .. },
                            DeltaCompletion::Cleanup
                        )
                    ),
                    "cancelled PositiveSupport child released a semantic completion"
                );
                completed.push(completed_activation.activation);
            }
        }

        for activation in live {
            assert!(
                !self.registry.is_live(activation),
                "PositiveSupport cancellation left affine work outside its queued or parked custody"
            );
        }
        let retired = parents
            .into_iter()
            .map(|parent| self.registry.retire_orphaned_positive_support_work(parent))
            .sum();
        (completed, retired)
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

    fn has_active_parked_positive_support(&self, active: ActiveDeltaContinuation) -> bool {
        self.parked_positive_support_worklist
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
    ) -> (
        DeltaStateId,
        Vec<ProgramTask>,
        Vec<Option<PositiveSupportWorkGrant>>,
        PhysicalDispatch,
    ) {
        let (selection, empty, remainder_tasks) = {
            let bucket = self
                .program_worklist
                .get_mut(&active.state)
                .expect("active typed program state remains live");
            let selection = bucket.take_active(&self.registry, active.activation, search_width);
            (selection, bucket.is_empty(), bucket.len())
        };
        if empty {
            self.program_worklist.deactivate(active.state);
        }
        let ProgramSelection {
            key,
            tasks,
            mut limits,
        } = selection;
        self.unindex_runnable_positive_support_tasks(&tasks);
        let support_grants = self.reserve_positive_support_selection(&tasks, &mut limits);
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
        (active.state, tasks, support_grants, dispatch)
    }

    fn pop_program_bounded(
        &mut self,
        search_width: usize,
    ) -> (
        DeltaStateId,
        Vec<ProgramTask>,
        Vec<Option<PositiveSupportWorkGrant>>,
        PhysicalDispatch,
    ) {
        let preferred_lane = self
            .lane_selection_is_active()
            .then(|| self.next_program_lane.take())
            .flatten();
        let planned_state = self.next_program_state.take();
        let preferred_state = preferred_lane.and_then(|lane| {
            planned_state.or_else(|| self.program_worklist.last_id_in_lane(&self.registry, lane))
        });
        assert!(
            preferred_lane.is_none() || preferred_state.is_some(),
            "preferred Program service lane disappeared before its global pop"
        );
        let id = preferred_state
            .or_else(|| self.program_worklist.last_id())
            .expect("typed program pop requires live work");
        let lane = if self.lane_selection_is_active() {
            preferred_state
                .zip(preferred_lane)
                .map(|(_, lane)| lane)
                .or_else(|| {
                    self.program_worklist
                        .get(&id)
                        .and_then(ProgramBucket::last)
                        .map(|task| ProgramServiceLane::of(&self.registry, task.activation))
                })
        } else {
            None
        };
        let (selection, empty, remainder_tasks) = {
            let bucket = self
                .program_worklist
                .get_mut(&id)
                .expect("selected typed program state");
            let selection = bucket.take_global(
                &self.registry,
                search_width,
                self.activation_width,
                lane,
                &mut self.terminal_selection_slots,
                &mut self.terminal_selections,
            );
            (selection, bucket.is_empty(), bucket.len())
        };
        if empty {
            self.program_worklist.deactivate(id);
        }
        let ProgramSelection {
            key,
            tasks,
            mut limits,
        } = selection;
        self.unindex_runnable_positive_support_tasks(&tasks);
        let support_grants = self.reserve_positive_support_selection(&tasks, &mut limits);
        if self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt
            && lane == Some(ProgramServiceLane::Support)
        {
            self.global_service_woken_support_tasks = self
                .global_service_woken_support_tasks
                .checked_sub(tasks.len())
                .expect("global Support packet selected work outside the current wake");
        }
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
        )
        .with_program_service_lane(lane);
        (id, tasks, support_grants, dispatch)
    }

    fn reserve_positive_support_selection(
        &mut self,
        tasks: &[ProgramTask],
        limits: &mut [usize],
    ) -> Vec<Option<PositiveSupportWorkGrant>> {
        assert_eq!(tasks.len(), limits.len());
        let mut selected_parents = AHashSet::new();
        let mut grants = Vec::with_capacity(tasks.len());
        for (task, limit) in tasks.iter().zip(limits) {
            let Some(parent) = self
                .registry
                .positive_support_parent_for_child(task.activation)
            else {
                grants.push(None);
                continue;
            };
            assert!(
                selected_parents.insert(parent),
                "one physical cohort selected two runnable Support tasks for one semantic parent"
            );
            let grant = match self.positive_support_scheduling {
                PositiveSupportScheduling::CountCredit => {
                    let grant = self
                        .registry
                        .reserve_positive_support_work(task.activation, *limit)
                        .expect("runnable PositiveSupport task had no available work allowance");
                    *limit = grant.granted;
                    grant
                }
                PositiveSupportScheduling::GlobalServiceDebt => self
                    .registry
                    .reserve_positive_support_service(task.activation, *limit)
                    .expect("runnable PositiveSupport task was outside the service epoch"),
            };
            grants.push(Some(grant));
        }
        grants
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
        let has_parked_program = self.has_active_parked_positive_support(active);
        if has_parked_program {
            assert!(
                !has_source && !has_transition && !has_program,
                "parked PositiveSupport activation remained runnable"
            );
            assert!(
                self.registry.is_live_positive_support(active.activation),
                "parked PositiveSupport lease lost its live affine activation"
            );
            return ActiveDeltaStepOutcome {
                outcome: DeltaStepOutcome::parked_lease_release(),
                status: ActiveDeltaStatus::Parked,
                resume: None,
            };
        }
        assert!(
            has_source || has_transition || has_program,
            "active delta continuation has no scheduled affine task"
        );
        debug_assert!(
            usize::from(has_source) + usize::from(has_transition) + usize::from(has_program) == 1,
            "one delta activation owns incompatible scheduler queue kinds simultaneously"
        );

        // Keep the raw semantic full-result receipt alive through dispatch.
        // Ordinary paths still gate it by their physical activation class at
        // the exact release site; Stage 2's positive publication transaction
        // will instead validate its semantic Confirm parent.
        let direct_terminal_full = direct_terminal_publication_full;
        let examined_before = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
        let outcome = if has_program {
            let (state, tasks, support_grants, dispatch) =
                self.pop_active_program(active, search_width);
            let physical = self.step_program(
                root,
                plan,
                state,
                tasks,
                support_grants,
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
        let runnable = self.has_active_source(active)
            || self.has_active_transition(active)
            || self.has_active_program(active);
        let parked = self.has_active_parked_positive_support(active);
        debug_assert!(
            !runnable || !parked,
            "one activation remained both runnable and parked after a directed step"
        );
        // A query-global service epoch may remain active solely because a
        // sibling shard still owns work. That does not manufacture ordinary
        // runnable custody for a locally exhausted directed activation.
        // Release is meaningful only when this shard can actually return the
        // exact continuation to its ordinary queue.
        let release_directed_lease = outcome.release_directed_lease && runnable;
        let resume = (!release_directed_lease)
            .then(|| settled.or_else(|| runnable.then_some(active)))
            .flatten();
        let status = if yielded {
            ActiveDeltaStatus::Yielded
        } else if release_directed_lease {
            debug_assert!(
                runnable,
                "a released Exact lease lost its ordinary runnable custody"
            );
            ActiveDeltaStatus::Released
        } else if resume.is_some() {
            ActiveDeltaStatus::Pending
        } else if live && parked {
            ActiveDeltaStatus::Parked
        } else {
            debug_assert!(
                !live,
                "live delta activation lost both runnable and parked affine custody"
            );
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

    #[cfg(test)]
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
        match self.try_step_bounded(
            root,
            plan,
            search_width,
            direct_terminal_publication_full,
            stable,
            stable_interner,
            stats,
        ) {
            DeltaSchedulerStep::Completed { outcome, .. } => outcome,
            DeltaSchedulerStep::Deferred { .. } => {
                panic!("serial delta step was deferred behind a parallel service packet")
            }
        }
    }

    pub(super) fn try_step_bounded<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        search_width: usize,
        direct_terminal_publication_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaSchedulerStep {
        let service_reservation = match self.reserve_global_service_packet() {
            PositiveSupportServiceTurn::Inactive => None,
            PositiveSupportServiceTurn::Deferred(packet_nonce) => {
                return DeltaSchedulerStep::Deferred { packet_nonce };
            }
            PositiveSupportServiceTurn::Reserved(turn) => Some(turn),
        };
        if !self.program_worklist.is_empty() {
            let (state, tasks, support_grants, dispatch) = self.pop_program_bounded(search_width);
            let service_lane = if self.positive_support_scheduling
                == PositiveSupportScheduling::GlobalServiceDebt
            {
                let lane = dispatch
                    .program_service_lane
                    .unwrap_or(ProgramServiceLane::Neutral);
                assert!(
                    match lane {
                        ProgramServiceLane::Support => support_grants.iter().all(Option::is_some),
                        ProgramServiceLane::Exact | ProgramServiceLane::Neutral => {
                            support_grants.iter().all(Option::is_none)
                        }
                    },
                    "global service packet crossed its selected lane"
                );
                lane
            } else {
                ProgramServiceLane::Neutral
            };
            assert_eq!(
                service_reservation.as_ref().map(|turn| turn.lane()),
                (service_lane != ProgramServiceLane::Neutral).then_some(service_lane),
                "Program service lane diverged from its reserved global turn"
            );
            #[cfg(all(test, feature = "parallel"))]
            if let Some(turn) = service_reservation.as_ref() {
                self.positive_support_service_debt
                    .as_ref()
                    .expect("global service dispatch lost its coordinator")
                    .notify_dispatch_started(turn);
            }
            let examined_before = stats
                .delta_source_candidates_examined
                .saturating_add(stats.delta_transition_candidates_examined);
            let physical = self.step_program(
                root,
                plan,
                state,
                tasks,
                support_grants,
                &dispatch.task_limits,
                false,
                direct_terminal_publication_full,
                stable,
                stable_interner,
                stats,
            );
            let global_packet_event = self.settle_global_service_packet(
                service_lane,
                service_reservation,
                physical.validated_program_examined,
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
            return DeltaSchedulerStep::Completed {
                outcome,
                global_packet_event,
            };
        }
        if self.worklist.is_empty() {
            return DeltaSchedulerStep::Completed {
                outcome: self.step_source(
                    root,
                    plan,
                    search_width,
                    direct_terminal_publication_full,
                    stable,
                    stable_interner,
                    stats,
                ),
                global_packet_event: None,
            };
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
        DeltaSchedulerStep::Completed {
            outcome,
            global_packet_event: None,
        }
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
        let mut retargeted = RetargetedActivations::default();
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
                        prefer_continuation(&mut task_effects.continuation, released.continuation);
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
                release_directed_lease: false,
                demand_preference: None,
            },
            terminal_publications,
            retired_search_receipt: false,
            validated_program_examined: 0,
        }
    }

    /// Executes one physically compatible cohort of opaque typed
    /// continuations. Handles are affinely taken into a dense typed vector,
    /// and the adapter returns one replacement receipt per input in scheduler
    /// order. A directed singleton may cross the same erased family boundary
    /// again for exact sole children while spending the original grant; the
    /// registry still observes one final replacement receipt.
    #[allow(clippy::too_many_arguments)]
    fn step_program<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        state: DeltaStateId,
        mut tasks: Vec<ProgramTask>,
        support_grants: Vec<Option<PositiveSupportWorkGrant>>,
        limits: &[usize],
        directed_active: bool,
        direct_terminal_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaPhysicalOutcome {
        assert!(!tasks.is_empty());
        assert_eq!(tasks.len(), limits.len());
        assert_eq!(tasks.len(), support_grants.len());
        assert!(limits.iter().all(|&limit| limit > 0));

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
        let directed_positive_support =
            directed_active && self.registry.is_live_positive_support(tasks[0].activation);
        let mut scratch = self
            .program_scratch
            .take()
            .unwrap_or_else(|| Box::new(ProgramSchedulerScratch::default()));
        scratch.parents.clear();
        let mut candidate_sets: SmallVec<[Option<&[RawInline]>; 1]> = SmallVec::new();
        candidate_sets.reserve(row_count);
        for task in &tasks {
            assert_eq!(task.activation, task.credit.key.activation);
            let (bound, parent, candidates) = self.registry.source_context(task.activation);
            assert_eq!(bound, cohort_key.bound);
            assert_eq!(candidates.is_some(), cohort_key.has_candidates);
            scratch.parents.extend_from_slice(parent);
            candidate_sets.push(candidates);
        }
        scratch.vars.clear();
        scratch.vars.extend(cohort_key.bound.into_iter());
        let view = rows_view(&scratch.vars, &scratch.parents, row_count);
        scratch.activations.clear();
        scratch.activations.extend(
            tasks
                .iter()
                .map(|task| ProgramActivation(task.activation.0)),
        );
        scratch.task_receipts.clear();
        scratch.work.clear();
        for (task, support_grant) in tasks.drain(..).zip(support_grants) {
            scratch.task_receipts.push(ProgramTaskReceipt {
                activation: task.activation,
                credit: task.credit,
                support_grant,
            });
            scratch.work.push(task.work);
        }
        scratch.receipt.clear();
        spec.step_batch_for(
            self.program_runtimes
                .get_mut(&state)
                .expect("typed program state lost its runtime"),
            address_key,
            ProgramBatch {
                stratum: address.stratum(),
                view,
                candidate_sets: &candidate_sets,
                activations: &scratch.activations,
                work: &scratch.work,
                limits,
            },
            &mut scratch.receipt,
        );
        assert_eq!(
            scratch.receipt.pages.len(),
            row_count,
            "typed program returned the wrong page count"
        );
        for (page, &limit) in scratch.receipt.pages.iter().zip(limits) {
            assert!(
                page.examined <= limit,
                "typed program exceeded one input's physical work budget"
            );
        }

        // A directed streaming activation with one unjoined producer may
        // consume an exact sole child before publishing the replacement
        // receipt. This is not parked work: every additional typed call spends
        // the original input's still-unspent grant, and the original registry
        // credit remains authoritative until the final receipt commits once.
        // The child handle has already passed ordinary typed validation and
        // novelty admission, so taking it here preserves fixpoint semantics.
        let receipt_local_fusion = directed_active
            && row_count == 1
            && matches!(&address, ProgramAddress::Constraint { .. })
            && cohort_key.class == ProgramCohortClass::ActivationStreaming
            && self
                .registry
                .program_credit_is_unjoined_unique(&scratch.task_receipts[0].credit);
        scratch.receipt_local_observed_prefix.clear();
        scratch.fused_receipt.clear();
        let mut total_examined = scratch.receipt.pages[0].examined;
        let mut fused_steps = 0usize;
        let mut source_cohorts = usize::from(scratch.receipt.source_pages > 0);
        let mut max_source_cohort = scratch.receipt.source_pages;
        let mut source_pages = scratch.receipt.source_pages;
        let mut source_examined = scratch.receipt.source_examined;
        let mut source_roots = scratch.receipt.source_roots;
        let mut transition_cohorts = usize::from(scratch.receipt.transition_pages > 0);
        let mut max_transition_cohort = scratch.receipt.transition_pages;
        let mut transition_pages = scratch.receipt.transition_pages;
        let mut transition_examined = scratch.receipt.transition_examined;
        let mut placement_granted_work = scratch
            .receipt
            .placement
            .map(|_| limits.iter().sum::<usize>());
        while receipt_local_fusion && total_examined < limits[0] {
            let exact_tail = scratch.receipt.placement.is_none()
                && scratch.receipt.pages.len() == 1
                && scratch.receipt.pages[0].examined > 0
                && scratch.receipt.pages[0].resume.is_none()
                && scratch.receipt.children.len() == 1
                && scratch.receipt.children[0].input == 0
                && scratch.receipt.direct.is_empty()
                && scratch.receipt.accepted.is_empty()
                && scratch.receipt.supported.is_empty()
                && ProgramCohortKey::of_work(
                    &self.registry,
                    scratch.task_receipts[0].activation,
                    &scratch.receipt.children[0].work,
                ) == cohort_key;
            if !exact_tail {
                break;
            }

            let child = scratch
                .receipt
                .children
                .pop()
                .expect("receipt-local Program tail lost its sole child");
            if let Some(accepted) = child.accepted {
                scratch.receipt_local_observed_prefix.push(accepted);
            }
            let remaining = limits[0]
                .checked_sub(total_examined)
                .expect("receipt-local Program chain overspent its grant");
            assert!(remaining > 0);
            scratch.work.clear();
            scratch.work.push(child.work);
            let fused_limits = [remaining];
            spec.step_batch_for(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("typed program state lost its runtime during receipt-local fusion"),
                address_key,
                ProgramBatch {
                    stratum: address.stratum(),
                    view,
                    candidate_sets: &candidate_sets,
                    activations: &scratch.activations,
                    work: &scratch.work,
                    limits: &fused_limits,
                },
                &mut scratch.fused_receipt,
            );
            assert_eq!(
                scratch.fused_receipt.pages.len(),
                1,
                "receipt-local typed Program returned the wrong page count"
            );
            let examined = scratch.fused_receipt.pages[0].examined;
            assert!(
                examined <= remaining,
                "receipt-local typed Program exceeded its remaining work budget"
            );
            total_examined = total_examined
                .checked_add(examined)
                .expect("receipt-local Program examined-work count overflow");
            source_cohorts += usize::from(scratch.fused_receipt.source_pages > 0);
            max_source_cohort = max_source_cohort.max(scratch.fused_receipt.source_pages);
            source_pages += scratch.fused_receipt.source_pages;
            source_examined += scratch.fused_receipt.source_examined;
            source_roots += scratch.fused_receipt.source_roots;
            transition_cohorts += usize::from(scratch.fused_receipt.transition_pages > 0);
            max_transition_cohort =
                max_transition_cohort.max(scratch.fused_receipt.transition_pages);
            transition_pages += scratch.fused_receipt.transition_pages;
            transition_examined += scratch.fused_receipt.transition_examined;
            if scratch.fused_receipt.placement.is_some() {
                placement_granted_work = Some(remaining);
            }
            std::mem::swap(&mut scratch.receipt, &mut scratch.fused_receipt);
            scratch.fused_receipt.clear();
            fused_steps += 1;
        }
        let validated_examined = validated_program_examined(
            &scratch.receipt.pages,
            receipt_local_fusion.then_some(total_examined),
        );
        let validated_program_examined = validated_examined
            .iter()
            .copied()
            .try_fold(0usize, usize::checked_add)
            .expect("validated Program examined-work count overflow");
        let final_source_telemetry_cohort =
            scratch.receipt.source_pages > 0 && scratch.receipt.transition_pages == 0;
        scratch.receipt.source_pages = source_pages;
        scratch.receipt.source_examined = source_examined;
        scratch.receipt.source_roots = source_roots;
        scratch.receipt.transition_pages = transition_pages;
        scratch.receipt.transition_examined = transition_examined;
        if fused_steps > 0 {
            stats.delta_program_receipt_local_fused_steps += fused_steps;
            stats.delta_program_receipt_local_refiles_avoided += fused_steps;
            stats.max_delta_program_receipt_local_chain = stats
                .max_delta_program_receipt_local_chain
                .max(fused_steps + 1);
        }
        drop(candidate_sets);
        program_child_ranges_into(
            &scratch.receipt.children,
            row_count,
            &mut scratch.child_ranges,
        );
        tagged_ranges_into(
            &scratch.receipt.direct,
            row_count,
            "program direct effect",
            &mut scratch.direct_ranges,
        );
        tagged_ranges_into(
            &scratch.receipt.accepted,
            row_count,
            "program candidate observation",
            &mut scratch.accepted_ranges,
        );
        tagged_ranges_into(
            &scratch.receipt.supported,
            row_count,
            "program support observation",
            &mut scratch.supported_ranges,
        );

        // Placement is observation only. Static executor labels deliberately
        // stay out of the ordinary hot-path aggregate and never feed dispatch.
        if scratch.receipt.placement.is_some() {
            let granted_work =
                placement_granted_work.expect("non-Native Program placement lost its exact grant");
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
        stats.delta_source_pages += scratch.receipt.source_pages;
        stats.delta_source_candidates_examined += scratch.receipt.source_examined;
        stats.delta_source_roots += scratch.receipt.source_roots;
        if !private_direct {
            stats.delta_source_direct_candidates += scratch.receipt.direct.len();
        }
        if scratch.receipt.source_pages > 0 {
            stats.delta_source_cohorts += source_cohorts;
            stats.max_delta_source_cohort = stats.max_delta_source_cohort.max(max_source_cohort);
        }
        stats.delta_transition_pages += scratch.receipt.transition_pages;
        stats.delta_transition_candidates_examined += scratch.receipt.transition_examined;
        if scratch.receipt.transition_pages > 0 {
            stats.delta_transition_cohorts += transition_cohorts;
            stats.max_delta_transition_cohort =
                stats.max_delta_transition_cohort.max(max_transition_cohort);
        }

        // Physical pacing is revalidated by the typed adapter from canonical
        // state before this receipt is produced. Family-reported source and
        // transition counts remain telemetry only.
        let search_cohort = cohort_key.class.pacing() == ProgramPacing::Search;
        let source_telemetry_cohort = final_source_telemetry_cohort;
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut retargeted = RetargetedActivations::default();
        let mut dead_pages = 0usize;
        let mut source_dead_pages = 0usize;
        let mut transition_dead_pages = 0usize;
        let mut retired_search_receipts = 0usize;
        let mut completed_activations = 0usize;
        let mut terminal_publications = OrderedActivationSet::default();
        let mut positive_support_retirements = AHashSet::new();
        let mut credited_support_parents = BTreeSet::new();
        let mut exhausted_support_parents = BTreeSet::new();

        scratch.retired_activations.clear();
        let ProgramSchedulerScratch {
            task_receipts,
            receipt,
            child_ranges,
            direct_ranges,
            accepted_ranges,
            supported_ranges,
            receipt_local_observed_prefix,
            retired_activations,
            ..
        } = &mut *scratch;
        let ProgramBatchEffects {
            pages,
            children,
            direct,
            accepted,
            supported,
            ..
        } = receipt;
        for (
            input,
            (((((task, page), child_range), direct_range), accepted_range), supported_range),
        ) in task_receipts
            .drain(..)
            .zip(pages.drain(..))
            .zip(child_ranges.drain(..))
            .zip(direct_ranges.drain(..))
            .zip(accepted_ranges.drain(..))
            .zip(supported_ranges.drain(..))
            .enumerate()
        {
            let ProgramTaskReceipt {
                activation,
                credit,
                support_grant,
            } = task;
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
            let had_child = !child_range.is_empty();
            let raw_ordinary_program_effect = (!private_direct && !direct_range.is_empty())
                || !accepted_range.is_empty()
                || !supported_range.is_empty();
            let mut outcome = self.registry.replace_program(
                credit,
                state,
                &children[child_range],
                receipt_local_observed_prefix.iter().copied(),
                accepted[accepted_range].iter().map(|(_, value)| *value),
                direct[direct_range].iter().map(|(_, value)| *value),
                !supported_range.is_empty(),
                search_cohort,
                source_telemetry_cohort,
                page.resume,
            );
            let positive_support_reducer = outcome.positive_support_reducer;
            assert_eq!(
                positive_support_reducer,
                support_grant.is_some(),
                "PositiveSupport dispatch lost or manufactured its affine work grant"
            );
            let examined = validated_examined[input];
            if let Some(grant) = support_grant {
                let accounting = grant.accounting;
                let parent = PositiveConfirmParentId {
                    brand: self.registry.brand,
                    activation: grant.parent,
                };
                let settled = self
                    .registry
                    .settle_positive_support_work(grant, activation, examined);
                if accounting == PositiveSupportWorkAccounting::CountCredit {
                    stats.delta_positive_support_examined += settled;
                }
                // A short physical page refunds the unexamined reservation.
                // Reconsider this parent only after every selected receipt has
                // been replaced, refiled, and cancelled.
                if accounting == PositiveSupportWorkAccounting::CountCredit {
                    credited_support_parents.insert(parent);
                }
            } else {
                if self.positive_support_scheduling == PositiveSupportScheduling::CountCredit {
                    let accounting = self
                        .registry
                        .account_positive_exact_work(activation, examined);
                    if accounting.paired {
                        stats.delta_positive_support_exact_paired_examined += examined;
                    }
                    if accounting.credited > 0 {
                        stats.delta_positive_support_exact_credited += accounting.credited;
                        credited_support_parents.insert(PositiveConfirmParentId {
                            brand: self.registry.brand,
                            activation,
                        });
                    }
                }
            }
            // A real Program receipt has already spent the child's affine
            // credit. Commit its witness and consume any resulting grant
            // immediately, before scheduling, settlement, or other fallible
            // work can separate the semantic SET insertion from release.
            let mut task_effects = DeltaStableEffects::default();
            if let Some(witness) = outcome.positive_support.take() {
                let child = witness.link.child;
                if let Some(grant) = self
                    .registry
                    .commit_positive_publication(*witness, direct_terminal_full)
                {
                    task_effects.absorb(Self::release_positive_publication(
                        grant,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
                // A real positive receipt exhausts this fully-bound hedge
                // regardless of whether it won the parent/value SET race.
                // Exact Confirm remains live and solely owns completeness.
                positive_support_retirements.insert(child);
            }
            if let Some(witness) = outcome.positive_confirm.take() {
                let parent = witness.parent;
                if let Some(grant) = self
                    .registry
                    .commit_confirm_positive_publication(*witness, direct_terminal_full)
                {
                    positive_support_retirements
                        .extend(self.registry.positive_support_children(parent));
                    task_effects.absorb(Self::release_positive_publication(
                        grant,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
            }
            // Raw accepted/supported reports from a PositiveSupport child are
            // merely witness material. Only a successful semantic
            // commit/release above is stable progress; independently retained
            // child work remains ordinary physical progress.
            let page_had_program_effect =
                had_child || (!positive_support_reducer && raw_ordinary_program_effect);
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
                tasks.push(ProgramTask {
                    activation,
                    credit,
                    work,
                });
            }

            if !positive_support_reducer && !supported_range.is_empty() {
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
                        outcome.accepted.into_vec(),
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
                let exhausted_support_parent = self
                    .registry
                    .positive_support_parent_for_child(proof.activation);
                if let Some(parent) = self.registry.positive_publication_parent(proof.activation) {
                    positive_support_retirements
                        .extend(self.registry.positive_support_children(parent));
                    stats.delta_positive_support_credit_retired +=
                        self.registry.retire_positive_support_work(parent);
                }
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
                        prefer_continuation(&mut task_effects.continuation, released.continuation);
                        if let Some(active) = released.active {
                            assert!(retargeted.insert(old_activation, active).is_none());
                        } else if !retargeted.contains_key(&old_activation) {
                            completed_activations += 1;
                            completed_activation_ids.push(old_activation);
                        }
                    }
                }
                if let Some(parent) = exhausted_support_parent {
                    exhausted_support_parents.insert(parent);
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

        let mut runnable = Vec::with_capacity(tasks.len());
        let mut parked = Vec::new();
        for task in tasks {
            if self.registry.is_live_positive_support(task.activation) {
                parked.push(task);
            } else {
                runnable.push(task);
            }
        }
        let _ = self.file_program_state(state, runnable);
        let _ = self.file_parked_positive_support_state(state, parked);
        let (cancelled, cancellation_retired) =
            self.retire_positive_support_activations(root, plan, &positive_support_retirements);
        stats.delta_positive_support_credit_retired += cancellation_retired;
        completed_activations += cancelled.len();
        completed_activation_ids.extend(cancelled);
        if !retired_activations.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("typed program state lost its runtime during retirement"),
                address_key,
                &retired_activations,
            );
        }
        for parent in exhausted_support_parents {
            stats.delta_positive_support_credit_retired +=
                self.registry.retire_orphaned_positive_support_work(parent);
        }
        let demand_preference = self.assign_public_pull_demand(stats);
        let global_service_active = self.global_service_lane_is_active();
        let exact_credit_wake = self.positive_support_scheduling
            == PositiveSupportScheduling::CountCredit
            && self.wake_positive_support_parents(credited_support_parents);
        let release_directed_lease = directed_active
            && !directed_positive_support
            && (demand_preference.is_some() || global_service_active || exact_credit_wake);
        self.registry.assert_no_positive_support_reservations();
        children.clear();
        direct.clear();
        accepted.clear();
        supported.clear();
        receipt_local_observed_prefix.clear();
        scratch.parents.clear();
        scratch.vars.clear();
        scratch.activations.clear();
        scratch.work.clear();
        scratch.receipt.clear();
        scratch.fused_receipt.clear();
        scratch.retired_activations.clear();
        self.program_scratch = Some(scratch);
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
                release_directed_lease,
                demand_preference,
            },
            terminal_publications,
            retired_search_receipt: retired_search_receipts > 0,
            validated_program_examined,
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
        let mut retargeted = RetargetedActivations::default();
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
                        prefer_continuation(&mut task_effects.continuation, released.continuation);
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
                release_directed_lease: false,
                demand_preference: None,
            },
            terminal_publications,
            retired_search_receipt: false,
            validated_program_examined: 0,
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
        let mut program_worklist = ProgramWorklist::default();
        let mut runnable_positive_support_tasks = AHashMap::new();
        for (id, bucket) in self.program_worklist.iter() {
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
                if let Some(parent) = registry.positive_support_parent_for_child(task.activation) {
                    *runnable_positive_support_tasks.entry(parent).or_default() += 1;
                }
            }
            program_worklist.append(id, &mut tasks);
        }
        let mut parked_positive_support_worklist = ProgramWorklist::default();
        for (id, bucket) in self.parked_positive_support_worklist.iter() {
            let mut tasks = Vec::with_capacity(bucket.tasks.len());
            for task in &bucket.tasks {
                let credit = remap
                    .remove(&task.credit.key)
                    .expect("delta clone omitted one parked positive Support credit");
                tasks.push(ProgramTask {
                    activation: task.activation,
                    credit,
                    work: task.work.clone(),
                });
            }
            parked_positive_support_worklist.append(id, &mut tasks);
        }
        assert!(
            remap.is_empty(),
            "delta registry held a live credit without a scheduled task"
        );
        let positive_support_service_demand_queue = self
            .positive_support_service_demand_queue
            .iter()
            .map(|parent| PositiveConfirmParentId {
                brand: registry.brand,
                activation: parent.activation,
            })
            .collect::<Vec<_>>();
        let positive_support_service_demand_queued = self
            .positive_support_service_demand_queued
            .iter()
            .map(|parent| PositiveConfirmParentId {
                brand: registry.brand,
                activation: parent.activation,
            })
            .collect::<AHashSet<_>>();
        assert_eq!(
            positive_support_service_demand_queue.len(),
            positive_support_service_demand_queued.len(),
            "service-demand locator clone found duplicate or missing membership"
        );
        let positive_support_service_live = registry.has_live_started_positive_support();
        let positive_support_service_debt =
            self.positive_support_service_debt.as_ref().map(|service| {
                service
                    .try_deep_clone(positive_support_service_live)
                    .expect("cannot clone a scheduler across an in-flight service packet")
            });
        Self {
            registry,
            interner: self.interner.clone(),
            worklist,
            source_worklist,
            program_worklist,
            runnable_positive_support_tasks,
            parked_positive_support_worklist,
            program_lane_selection: self.program_lane_selection,
            next_program_lane: self.next_program_lane,
            next_program_state: self.next_program_state,
            global_service_woken_support_tasks: self.global_service_woken_support_tasks,
            positive_support_parent_scratch: Vec::new(),
            positive_support_service_demand_queue,
            positive_support_service_demand_queued,
            #[cfg(test)]
            positive_support_service_locator_boundary_probes: self
                .positive_support_service_locator_boundary_probes,
            positive_support_scheduling: self.positive_support_scheduling,
            positive_support_service_debt,
            public_pull_demand: self.public_pull_demand,
            program_runtimes: self.program_runtimes.clone(),
            program_scratch: None,
            activation_width: self.activation_width,
            terminal_selection_slots: AHashMap::new(),
            terminal_selections: Vec::new(),
        }
    }

    #[cfg(feature = "parallel")]
    pub(super) fn empty_parallel_sibling(&mut self) -> Self {
        let mut sibling = Self::new();
        sibling.positive_support_scheduling = self.positive_support_scheduling;
        sibling.program_lane_selection = self.program_lane_selection;
        assert_eq!(
            self.positive_support_service_debt.is_some(),
            self.positive_support_scheduling == PositiveSupportScheduling::GlobalServiceDebt,
            "PositiveSupport scheduling mode diverged from its service-debt account"
        );
        if let Some(service) = self.positive_support_service_debt.take() {
            let local_live = self.registry.has_live_started_positive_support();
            let (left, right) = service.into_parallel_pair(local_live);
            self.positive_support_service_debt = Some(left);
            sibling.positive_support_service_debt = Some(right);
        }
        sibling
    }

    #[cfg(all(test, feature = "parallel"))]
    pub(super) fn shares_global_service_coordinator_with(&self, other: &Self) -> bool {
        self.positive_support_service_debt
            .as_ref()
            .expect("left scheduler lost its global service coordinator")
            .shares_coordinator_with(
                other
                    .positive_support_service_debt
                    .as_ref()
                    .expect("right scheduler lost its global service coordinator"),
            )
    }

    #[cfg(all(test, feature = "parallel"))]
    pub(super) fn shares_global_service_liveness_slot_with(&self, other: &Self) -> bool {
        self.positive_support_service_debt
            .as_ref()
            .expect("left scheduler lost its global service coordinator")
            .shares_liveness_slot_with(
                other
                    .positive_support_service_debt
                    .as_ref()
                    .expect("right scheduler lost its global service coordinator"),
            )
    }

    #[cfg(all(test, feature = "parallel"))]
    pub(super) fn global_service_debt_is_direct(&self) -> bool {
        matches!(
            &self
                .positive_support_service_debt
                .as_ref()
                .expect("global service scheduler lost its debt account")
                .mode,
            PositiveSupportServiceDebtMode::Direct(_)
        )
    }

    #[cfg(all(test, feature = "parallel"))]
    pub(super) fn global_service_debt_is_shared(&self) -> bool {
        matches!(
            &self
                .positive_support_service_debt
                .as_ref()
                .expect("global service scheduler lost its debt account")
                .mode,
            PositiveSupportServiceDebtMode::Shared(_)
        )
    }

    #[cfg(test)]
    fn global_service_debt_snapshot(&self) -> PositiveSupportServiceDebtSnapshot {
        self.positive_support_service_debt
            .as_ref()
            .expect("global service scheduler lost its coordinator")
            .snapshot()
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

    #[test]
    fn retargeted_activations_preserve_map_semantics_across_storage_shapes() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<RetargetedActivations>();

        let first_key = ActivationId::test(11);
        let second_key = ActivationId::test(22);
        let third_key = ActivationId::test(33);
        let missing_key = ActivationId::test(44);
        let continuation = |state, activation| ActiveDeltaContinuation {
            state: DeltaStateId(state),
            activation: ActivationId::test(activation),
        };
        let first = continuation(1, 101);
        let first_replacement = continuation(2, 102);
        let first_many_replacement = continuation(3, 103);
        let second = continuation(4, 104);
        let second_replacement = continuation(5, 105);
        let third = continuation(6, 106);

        let mut retargeted = RetargetedActivations::default();
        assert!(matches!(&retargeted, RetargetedActivations::Empty));
        assert_eq!(retargeted.len(), 0);
        assert_eq!(retargeted.get(&first_key), None);
        assert!(!retargeted.contains_key(&missing_key));

        assert_eq!(retargeted.insert(first_key, first), None);
        assert!(matches!(
            &retargeted,
            RetargetedActivations::One(activation, continuation)
                if *activation == first_key && *continuation == first
        ));
        assert_eq!(retargeted.len(), 1);
        assert_eq!(retargeted.get(&first_key), Some(&first));

        assert_eq!(retargeted.insert(first_key, first_replacement), Some(first));
        assert_eq!(retargeted.len(), 1);
        assert_eq!(retargeted.get(&first_key), Some(&first_replacement));

        assert_eq!(retargeted.insert(second_key, second), None);
        assert!(matches!(
            &retargeted,
            RetargetedActivations::Many(entries) if entries.len() == 2
        ));
        assert_eq!(retargeted.get(&first_key), Some(&first_replacement));
        assert_eq!(retargeted.get(&second_key), Some(&second));
        assert_eq!(retargeted.get(&missing_key), None);

        assert_eq!(
            retargeted.insert(first_key, first_many_replacement),
            Some(first_replacement)
        );
        assert_eq!(
            retargeted.insert(second_key, second_replacement),
            Some(second)
        );
        assert_eq!(retargeted.insert(third_key, third), None);
        assert_eq!(retargeted.len(), 3);
        assert_eq!(retargeted.get(&first_key), Some(&first_many_replacement));
        assert_eq!(retargeted.get(&second_key), Some(&second_replacement));
        assert_eq!(retargeted.get(&third_key), Some(&third));
        assert!(retargeted.contains_key(&first_key));
        assert!(!retargeted.contains_key(&missing_key));
    }

    #[test]
    fn positive_support_budget_conserves_demand_exact_reservations_and_refunds() {
        let mut budget = PositiveSupportWorkBudget::default();
        assert_eq!(
            budget.mint_exact(7),
            0,
            "exact work before external demand must not become retroactive credit"
        );
        budget.mint_demand();
        assert_eq!(budget.mint_exact(4), 4);
        assert_eq!(budget.minted(), 5);

        // Model two physical roots reserving from the same semantic parent.
        // Sequential reservation, rather than per-row limits, is the
        // authoritative aggregate cap.
        let first = budget.reserve(4);
        let second = budget.reserve(4);
        assert_eq!((first, second), (4, 1));
        assert_eq!(budget.available(), 0);
        budget.settle(first, 1);
        budget.settle(second, 1);
        assert_eq!(budget.spent, 2);
        assert_eq!(
            budget.available(),
            3,
            "unexamined reservation must refund through the same ledger"
        );

        let cloned = budget.clone();
        assert_eq!(cloned, budget);
        cloned.assert_conservation();
        assert_eq!(budget.retire_available(), 3);
        assert_eq!(budget.retired, 3);
        assert_eq!(budget.available(), 0);
        budget.assert_conservation();
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestServiceLane {
        Exact,
        Support,
    }

    fn next_service_lane(ledger: &PositiveSupportServiceDebtLedger) -> TestServiceLane {
        if ledger.support_is_admissible(true) {
            TestServiceLane::Support
        } else {
            TestServiceLane::Exact
        }
    }

    fn run_support_service_packet(
        ledger: &mut PositiveSupportServiceDebtLedger,
        service: u64,
        remains_live: bool,
        trace: &mut Vec<TestServiceLane>,
    ) {
        assert_eq!(next_service_lane(ledger), TestServiceLane::Support);
        let reservation = ledger
            .reserve_support(true)
            .expect("selected Support packet must reserve the global lease");
        ledger.settle_support(&reservation, service, remains_live);
        trace.push(TestServiceLane::Support);
    }

    fn run_exact_service_packet(
        ledger: &mut PositiveSupportServiceDebtLedger,
        service: u64,
        trace: &mut Vec<TestServiceLane>,
    ) {
        assert_eq!(next_service_lane(ledger), TestServiceLane::Exact);
        let reservation = ledger
            .reserve_exact()
            .expect("selected Exact packet must reserve the global lease");
        ledger.settle_exact(&reservation, service, true);
        trace.push(TestServiceLane::Exact);
    }

    #[test]
    fn positive_support_service_debt_recycles_after_three_exact_packets() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        let mut trace = Vec::new();
        assert!(ledger.demand_arrived());

        run_support_service_packet(&mut ledger, 5, true, &mut trace);
        for _ in 0..3 {
            run_exact_service_packet(&mut ledger, 2, &mut trace);
        }
        run_support_service_packet(&mut ledger, 1, true, &mut trace);

        assert_eq!(
            trace,
            [
                TestServiceLane::Support,
                TestServiceLane::Exact,
                TestServiceLane::Exact,
                TestServiceLane::Exact,
                TestServiceLane::Support,
            ],
            "H=5 / E=2,2,2 / H=1 is the mandatory aggregate-debt trace"
        );
        assert_eq!((ledger.support_service, ledger.exact_service), (6, 6));
        assert_eq!((ledger.max_support_packet, ledger.max_exact_packet), (5, 2));
        assert_eq!(
            next_service_lane(&ledger),
            TestServiceLane::Exact,
            "Exact must own the restored service tie"
        );
        ledger.assert_packet_bounds();
    }

    #[test]
    fn positive_support_service_debt_recycles_unit_support_until_exact_tie() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        let mut trace = Vec::new();
        assert!(ledger.demand_arrived());

        run_support_service_packet(&mut ledger, 1, true, &mut trace);
        run_exact_service_packet(&mut ledger, 8, &mut trace);
        for _ in 0..7 {
            run_support_service_packet(&mut ledger, 1, true, &mut trace);
        }

        assert_eq!((ledger.support_service, ledger.exact_service), (8, 8));
        assert_eq!(trace.len(), 9);
        assert_eq!(
            trace[0..2],
            [TestServiceLane::Support, TestServiceLane::Exact]
        );
        assert!(
            trace[2..]
                .iter()
                .all(|lane| *lane == TestServiceLane::Support),
            "one E=8 packet must recycle seven unit Support packets"
        );
        assert_eq!(next_service_lane(&ledger), TestServiceLane::Exact);
        ledger.assert_packet_bounds();
    }

    #[test]
    fn positive_support_service_debt_pays_one_large_initial_packet_only() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        let mut trace = Vec::new();
        assert!(ledger.demand_arrived());

        run_support_service_packet(&mut ledger, 100, true, &mut trace);
        for _ in 0..101 {
            run_exact_service_packet(&mut ledger, 1, &mut trace);
        }
        run_support_service_packet(&mut ledger, 1, true, &mut trace);

        assert_eq!((ledger.support_service, ledger.exact_service), (101, 101));
        assert_eq!(trace.first(), Some(&TestServiceLane::Support));
        assert_eq!(trace.last(), Some(&TestServiceLane::Support));
        assert!(
            trace[1..102]
                .iter()
                .all(|lane| *lane == TestServiceLane::Exact),
            "H=100 must let 101 unit Exact packets cross the tie before H recycles"
        );
        assert_eq!(
            (ledger.max_support_packet, ledger.max_exact_packet),
            (100, 1)
        );
        ledger.assert_packet_bounds();
    }

    #[test]
    fn positive_support_service_debt_has_one_bypass_across_demand_arrivals() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        assert!(!ledger.demand_arrived());
        assert!(!ledger.demand_arrived());

        let initial = ledger
            .reserve_support(true)
            .expect("the first epoch demand authorizes one initial packet");
        ledger.settle_support(&initial, 5, true);
        assert!(!ledger.demand_arrived());
        assert!(!ledger.support_is_admissible(true));
        assert!(
            ledger.reserve_support(true).is_none(),
            "later public pulls must not mint another query-global bypass"
        );

        let exact = ledger.reserve_exact().expect("Exact owns the debt tie");
        ledger.settle_exact(&exact, 6, true);
        assert!(ledger.support_is_admissible(true));
    }

    #[test]
    fn positive_support_service_debt_inflight_settlement_enters_idle() {
        trait AmbiguousIfClone<Marker> {
            fn marker() {}
        }
        impl<T: ?Sized> AmbiguousIfClone<()> for T {}
        struct CloneMarker;
        impl<T: Clone> AmbiguousIfClone<CloneMarker> for T {}
        let _ = <PositiveSupportPacketReservation as AmbiguousIfClone<_>>::marker;

        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        let original_brand = ledger.brand;
        let reservation = ledger
            .reserve_support(true)
            .expect("initial Support packet reservation");

        assert!(
            ledger.reserve_support(true).is_none(),
            "one affine lease cannot reserve two Support packets"
        );
        assert!(
            ledger.try_deep_clone().is_none(),
            "a query clone must not cross an unsettled Support receipt"
        );
        assert!(ledger.active_packet_nonce.is_some());
        assert!(ledger.try_deep_clone().is_none());

        ledger.settle_support(&reservation, 3, false);
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Idle);
        assert_eq!(ledger.support_service, 3);
        assert!(ledger.active_packet_nonce.is_none());

        let clone = ledger
            .try_deep_clone()
            .expect("settled affine custody may be deep-cloned");
        assert_ne!(clone.brand, original_brand);
        assert_eq!(clone.lease, PositiveSupportServiceLease::Idle);
        assert_eq!(clone.support_service, ledger.support_service);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn positive_support_service_runtime_promotes_once_without_rebranding() {
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let service = scheduler
            .positive_support_service_debt
            .as_mut()
            .expect("enabled scheduler lost its direct debt account");
        assert!(matches!(
            &service.mode,
            PositiveSupportServiceDebtMode::Direct(_)
        ));
        assert!(service.demand_arrived());
        let reservation = match service.try_reserve_turn(true) {
            PositiveSupportServiceTurn::Reserved(reservation) => reservation,
            PositiveSupportServiceTurn::Inactive => panic!("live direct ledger was inactive"),
            PositiveSupportServiceTurn::Deferred(packet_nonce) => {
                panic!("direct ledger deferred on packet {packet_nonce}")
            }
        };
        let settlement = service.settle(reservation, 7, false);
        assert!(settlement.closed_packet.is_some());
        let before = scheduler.global_service_debt_snapshot();

        let mut right = scheduler.empty_parallel_sibling();
        assert!(scheduler.global_service_debt_is_shared());
        assert!(right.global_service_debt_is_shared());
        assert_eq!(scheduler.global_service_debt_snapshot(), before);
        assert_eq!(right.global_service_debt_snapshot(), before);
        assert!(scheduler.shares_global_service_coordinator_with(&right));
        assert!(!scheduler.shares_global_service_liveness_slot_with(&right));

        let third = right.empty_parallel_sibling();
        assert!(scheduler.shares_global_service_coordinator_with(&right));
        assert!(scheduler.shares_global_service_coordinator_with(&third));
        assert!(right.shares_global_service_coordinator_with(&third));
        assert!(!scheduler.shares_global_service_liveness_slot_with(&right));
        assert!(!scheduler.shares_global_service_liveness_slot_with(&third));
        assert!(!right.shares_global_service_liveness_slot_with(&third));
        assert_eq!(third.global_service_debt_snapshot(), before);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn already_shared_split_synchronizes_existing_registration_before_new_sibling() {
        let mut service = PositiveSupportServiceDebtRuntime::dormant();
        assert!(service.demand_arrived());
        let (left, first_false_sibling) = service.into_parallel_pair(true);
        assert!(left.global_lane_is_active());

        let (left, second_false_sibling) = left.into_parallel_pair(false);
        let PositiveSupportServiceDebtMode::Shared(left_runtime) = &left.mode else {
            panic!("an already-shared split returned to a direct ledger");
        };
        assert_eq!(left_runtime.live_shards(), 0);
        assert_eq!(
            left_runtime.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
        assert_eq!(
            left.snapshot().lease,
            PositiveSupportServiceLease::Idle,
            "the already-shared branch ignored the caller's current registry liveness"
        );
        assert!(!left.global_lane_is_active());
        assert!(left.shares_coordinator_with(&first_false_sibling));
        assert!(left.shares_coordinator_with(&second_false_sibling));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn positive_support_shared_clone_returns_to_an_independent_direct_ledger() {
        let mut source = DeltaScheduler::new();
        source.enable_positive_support_global_service_debt();
        let source_sibling = source.empty_parallel_sibling();
        assert!(source.global_service_debt_is_shared());
        assert!(source.shares_global_service_coordinator_with(&source_sibling));

        let mut cloned = source.deep_clone();
        assert!(cloned.global_service_debt_is_direct());
        assert_ne!(
            cloned.global_service_debt_snapshot().brand,
            source.global_service_debt_snapshot().brand
        );
        assert!(!source.shares_global_service_coordinator_with(&cloned));

        let cloned_sibling = cloned.empty_parallel_sibling();
        assert!(cloned.global_service_debt_is_shared());
        assert!(cloned.shares_global_service_coordinator_with(&cloned_sibling));
        assert!(!source.shares_global_service_coordinator_with(&cloned));
        assert!(!source_sibling.shares_global_service_coordinator_with(&cloned_sibling));
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn abandoned_direct_packet_fails_closed_without_parking_the_executor() {
        let mut service = PositiveSupportServiceDebtRuntime::dormant();
        assert!(service.demand_arrived());
        let reservation = match service.try_reserve_turn(true) {
            PositiveSupportServiceTurn::Reserved(reservation) => reservation,
            PositiveSupportServiceTurn::Inactive => panic!("live direct ledger was inactive"),
            PositiveSupportServiceTurn::Deferred(packet_nonce) => {
                panic!("direct ledger deferred on packet {packet_nonce}")
            }
        };
        drop(reservation);

        assert!(service.global_lane_is_active());
        assert_eq!(
            service.active_packet_nonce(),
            None,
            "a direct receipt has no sibling that could wake a parked executor"
        );
        assert!(
            service.try_deep_clone(true).is_none(),
            "a semantic clone crossed an abandoned direct receipt"
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = service.try_reserve_turn(true);
            }))
            .is_err(),
            "an abandoned direct receipt reopened attributable scheduling"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn shared_active_packet_mirror_tracks_partial_close_and_nonce_advance() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let left = PositiveSupportGlobalServiceRuntime::dormant();
        let right = left.parallel_sibling();
        assert!(left.demand_arrived());
        assert_eq!(left.live_shards(), 1);
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );

        let support_left = reserved(left.try_reserve_turn(true));
        assert_eq!(support_left.lane(), ProgramServiceLane::Support);
        let support_nonce = left
            .active_packet_nonce()
            .expect("opening Support did not publish its packet nonce");
        assert_eq!(left.active_packet_nonce(), Some(support_nonce));
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );

        let support_right = reserved(right.try_reserve_turn(true));
        assert_eq!(right.active_packet_nonce(), Some(support_nonce));
        assert_eq!(left.live_shards(), 2);
        assert!(left.global_lane_is_active());
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );

        assert!(support_left.settle(2, true).closed_packet.is_none());
        assert_eq!(
            left.active_packet_nonce(),
            Some(support_nonce),
            "a partial settlement cleared the packet mirror"
        );
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );

        assert!(support_right.settle(3, true).closed_packet.is_some());
        assert_eq!(
            left.active_packet_nonce(),
            None,
            "the final settlement left a retired packet visible to waiters"
        );
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );

        let exact = reserved(left.try_reserve_turn(true));
        let exact_nonce = left
            .active_packet_nonce()
            .expect("the next Exact packet was not published");
        assert_ne!(
            exact_nonce, support_nonce,
            "packet nonces must advance so stale waiters cannot observe ABA"
        );
        assert_ne!(
            left.active_packet_nonce(),
            Some(support_nonce),
            "a waiter for the retired Support nonce followed the reopened Exact packet"
        );
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );
        assert!(exact.settle(5, true).closed_packet.is_some());
        assert_eq!(left.active_packet_nonce(), None);
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn quiescent_phase_probes_bypass_mutex_but_reserved_probe_serializes() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let owner = PositiveSupportGlobalServiceRuntime::dormant();

        let dormant_probe = owner.parallel_sibling();
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Dormant
        );
        let coordinator = owner.lock();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let dormant_thread = std::thread::spawn(move || {
            result_tx
                .send(dormant_probe.global_lane_is_active())
                .unwrap();
        });
        assert!(!result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Dormant phase probe entered the coordinator mutex"));
        dormant_thread.join().unwrap();
        drop(coordinator);

        let parked_probe = owner.parallel_sibling();
        assert!(owner.demand_arrived());
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );
        let coordinator = owner.lock();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let parked_thread = std::thread::spawn(move || {
            result_tx
                .send(parked_probe.global_lane_is_active())
                .unwrap();
        });
        assert!(result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Parked phase probe entered the coordinator mutex"));
        parked_thread.join().unwrap();
        drop(coordinator);

        owner.synchronize_local_live(false);
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
        let idle_probe = owner.parallel_sibling();
        let coordinator = owner.lock();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let idle_thread = std::thread::spawn(move || {
            let active = idle_probe.global_lane_is_active();
            let unchanged_false_is_inactive = matches!(
                idle_probe.try_reserve_turn(false),
                PositiveSupportGlobalTurn::Inactive
            );
            result_tx
                .send((active, unchanged_false_is_inactive))
                .unwrap();
        });
        assert_eq!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("Idle phase probe entered the coordinator mutex"),
            (false, true)
        );
        idle_thread.join().unwrap();
        drop(coordinator);

        assert!(!owner.demand_arrived());
        let packet = reserved(owner.try_reserve_turn(true));
        let reserved_probe = owner.parallel_sibling();
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );
        let coordinator = owner.lock();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let reserved_thread = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            result_tx
                .send(reserved_probe.global_lane_is_active())
                .unwrap();
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Reserved phase probe thread did not start");
        assert_eq!(
            result_rx.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "Reserved phase probe bypassed the coordinator mutex"
        );
        drop(coordinator);
        assert!(result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Reserved phase probe did not resume after mutex release"));
        reserved_thread.join().unwrap();
        assert!(packet.settle(1, false).closed_packet.is_some());
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn last_live_runtime_drop_reconciles_the_quiescent_lease_to_idle() {
        let owner = PositiveSupportGlobalServiceRuntime::dormant();
        let observer = owner.parallel_sibling();
        assert!(owner.demand_arrived());
        assert_eq!(observer.live_shards(), 1);
        assert_eq!(
            observer.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );

        drop(owner);

        assert_eq!(observer.live_shards(), 0);
        assert_eq!(observer.snapshot().lease, PositiveSupportServiceLease::Idle);
        assert_eq!(
            observer.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn packet_guard_retains_exact_registration_after_runtime_drop() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let owner = PositiveSupportGlobalServiceRuntime::dormant();
        let observer = owner.parallel_sibling();
        assert!(owner.demand_arrived());
        let packet = reserved(owner.try_reserve_turn(true));
        drop(owner);

        assert_eq!(
            observer.live_shards(),
            1,
            "runtime drop retired registration custody still held by its packet guard"
        );
        assert_eq!(
            observer.published_phase(),
            PositiveSupportGlobalPublishedPhase::Reserved
        );

        assert!(packet.settle(1, false).closed_packet.is_some());
        assert_eq!(observer.live_shards(), 0);
        assert_eq!(
            observer.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn final_close_observes_late_live_sibling_outside_admission_horizon() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let owner = PositiveSupportGlobalServiceRuntime::dormant();
        assert!(owner.demand_arrived());
        let packet = reserved(owner.try_reserve_turn(true));
        let packet_nonce = packet
            .reservation
            .as_ref()
            .expect("active packet guard lost its reservation")
            .nonce;

        let late = owner.parallel_sibling();
        assert_eq!(
            match late.try_reserve_turn(true) {
                PositiveSupportGlobalTurn::Deferred(nonce) => nonce,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("late live sibling unexpectedly became inactive")
                }
                PositiveSupportGlobalTurn::Reserved(_) => {
                    panic!("late sibling crossed the packet admission horizon")
                }
            },
            packet_nonce
        );
        assert_eq!(owner.live_shards(), 2);

        assert!(packet.settle(1, false).closed_packet.is_some());
        assert_eq!(late.live_shards(), 1);
        assert_eq!(late.snapshot().lease, PositiveSupportServiceLease::Parked);
        assert_eq!(
            late.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );

        drop(late);
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn exact_registration_count_is_idempotent_and_one_of_two_live_drops_stays_parked() {
        let owner = PositiveSupportGlobalServiceRuntime::dormant();
        let sibling = owner.parallel_sibling();
        let observer = owner.parallel_sibling();

        assert!(owner.demand_arrived());
        assert!(!owner.demand_arrived());
        owner.synchronize_local_live(true);
        assert_eq!(
            owner.live_shards(),
            1,
            "same-state true publications changed the exact live count"
        );

        assert!(!sibling.demand_arrived());
        assert!(!sibling.demand_arrived());
        sibling.synchronize_local_live(true);
        assert_eq!(
            owner.live_shards(),
            2,
            "same-state sibling publications changed the exact live count"
        );
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked
        );

        drop(sibling);
        assert_eq!(owner.live_shards(), 1);
        assert_eq!(
            owner.published_phase(),
            PositiveSupportGlobalPublishedPhase::Parked,
            "dropping one of two live shards idled surviving custody"
        );

        owner.synchronize_local_live(false);
        owner.synchronize_local_live(false);
        assert_eq!(
            observer.live_shards(),
            0,
            "same-state false publications changed the exact live count"
        );
        assert_eq!(
            observer.published_phase(),
            PositiveSupportGlobalPublishedPhase::Idle
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn final_settlement_liveness_boundaries_converge_in_both_mutex_orders() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        // Final settle(false) commutes with retirement of the other last-live
        // shard. Exercise both possible mutex serializations deterministically.
        for drop_first in [false, true] {
            let owner = PositiveSupportGlobalServiceRuntime::dormant();
            let sibling = owner.parallel_sibling();
            let observer = owner.parallel_sibling();
            assert!(owner.demand_arrived());
            assert!(!sibling.demand_arrived());
            let packet = reserved(owner.try_reserve_turn(true));

            if drop_first {
                drop(sibling);
                assert_eq!(owner.live_shards(), 1);
                assert!(packet.settle(1, false).closed_packet.is_some());
            } else {
                assert!(packet.settle(1, false).closed_packet.is_some());
                assert_eq!(
                    owner.published_phase(),
                    PositiveSupportGlobalPublishedPhase::Parked
                );
                drop(sibling);
            }

            assert_eq!(observer.live_shards(), 0);
            assert_eq!(observer.snapshot().lease, PositiveSupportServiceLease::Idle);
            assert_eq!(
                observer.published_phase(),
                PositiveSupportGlobalPublishedPhase::Idle
            );
        }

        // Final settle(false) also commutes with a false sibling becoming
        // live. In both serializations the demand owns Parked custody until
        // that sibling's exact registration retires.
        for demand_first in [false, true] {
            let owner = PositiveSupportGlobalServiceRuntime::dormant();
            let sibling = owner.parallel_sibling();
            assert!(owner.demand_arrived());
            let packet = reserved(owner.try_reserve_turn(true));

            if demand_first {
                assert!(!sibling.demand_arrived());
                assert!(packet.settle(1, false).closed_packet.is_some());
            } else {
                assert!(packet.settle(1, false).closed_packet.is_some());
                assert_eq!(
                    owner.published_phase(),
                    PositiveSupportGlobalPublishedPhase::Idle
                );
                assert!(!sibling.demand_arrived());
            }

            assert_eq!(owner.live_shards(), 1);
            assert_eq!(owner.snapshot().lease, PositiveSupportServiceLease::Parked);
            assert_eq!(
                owner.published_phase(),
                PositiveSupportGlobalPublishedPhase::Parked
            );
            drop(sibling);
            assert_eq!(owner.live_shards(), 0);
            assert_eq!(
                owner.published_phase(),
                PositiveSupportGlobalPublishedPhase::Idle
            );
        }
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn abandoned_shared_packet_clears_mirror_after_fail_closed_transition() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let left = PositiveSupportGlobalServiceRuntime::dormant();
        let right = left.parallel_sibling();
        let abandoned_probe = left.parallel_sibling();
        assert!(left.demand_arrived());
        let left_part = reserved(left.try_reserve_turn(true));
        let right_part = reserved(right.try_reserve_turn(true));
        assert!(left.active_packet_nonce().is_some());

        drop(left_part);
        assert_eq!(
            left.active_packet_nonce(),
            None,
            "abandonment left waiters parked on a packet that can never close"
        );
        let snapshot = left.snapshot();
        assert!(snapshot.attribution_tainted);
        assert!(snapshot.active_packet_nonce.is_none());
        assert_eq!(
            left.published_phase(),
            PositiveSupportGlobalPublishedPhase::Abandoned
        );
        assert!(
            !left.global_lane_is_active(),
            "an abandoned shared coordinator reopened attributable scheduling"
        );
        assert!(left.try_deep_clone_ledger(true).is_none());

        let coordinator = left.lock();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let abandoned_thread = std::thread::spawn(move || {
            result_tx
                .send(abandoned_probe.global_lane_is_active())
                .unwrap();
        });
        assert!(!result_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("Abandoned phase probe entered the coordinator mutex"));
        abandoned_thread.join().unwrap();
        drop(coordinator);

        // A second outstanding guard observes the already-abandoned
        // coordinator and must neither panic nor republish the old nonce.
        drop(right_part);
        assert_eq!(right.active_packet_nonce(), None);

        // This deliberate fail-closed assertion poisons the coordinator, so
        // keep it last: the normal idempotent abandonment path above must run
        // against an unpoisoned mutex.
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = left.try_reserve_turn(true);
            }))
            .is_err(),
            "an abandoned shared packet admitted another turn"
        );
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn active_packet_mirror_and_pending_mutex_close_both_lost_wake_windows() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        // Parker-before-drain: the second Acquire still sees the packet, so
        // the leaf enters the queue and the closer subsequently drains it.
        {
            let owner = PositiveSupportGlobalServiceRuntime::dormant();
            let waiter = owner.parallel_sibling();
            assert!(owner.demand_arrived());
            let packet = reserved(owner.try_reserve_turn(true));
            let nonce = owner.active_packet_nonce().unwrap();
            let pending = Arc::new(Mutex::new(Vec::new()));
            let waiter_pending = Arc::clone(&pending);
            let (parked_tx, parked_rx) = std::sync::mpsc::channel();
            let waiter_thread = std::thread::spawn(move || {
                assert_eq!(waiter.active_packet_nonce(), Some(nonce));
                let mut parked = waiter_pending.lock().unwrap();
                if waiter.active_packet_nonce().is_some() {
                    parked.push(nonce);
                }
                parked_tx.send(()).unwrap();
            });

            parked_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("waiter did not enter the pending queue");
            assert!(packet.settle(1, false).closed_packet.is_some());
            let drained = std::mem::take(&mut *pending.lock().unwrap());
            waiter_thread.join().unwrap();
            assert_eq!(drained, [nonce]);
            assert_eq!(owner.active_packet_nonce(), None);
        }

        // Drain-before-parker: hold the pending mutex while the closer clears
        // the mirror and drains the still-empty queue. The waiter's second
        // Acquire occurs after that mutex handoff and therefore refuses to
        // orphan itself on the retired nonce.
        {
            let owner = PositiveSupportGlobalServiceRuntime::dormant();
            let waiter = owner.parallel_sibling();
            assert!(owner.demand_arrived());
            let packet = reserved(owner.try_reserve_turn(true));
            let nonce = owner.active_packet_nonce().unwrap();
            let pending = Arc::new(Mutex::new(Vec::new()));
            let mut closer_pending = pending.lock().unwrap();
            let waiter_pending = Arc::clone(&pending);
            let (observed_tx, observed_rx) = std::sync::mpsc::channel();
            let waiter_thread = std::thread::spawn(move || {
                assert_eq!(waiter.active_packet_nonce(), Some(nonce));
                observed_tx.send(()).unwrap();
                let mut parked = waiter_pending.lock().unwrap();
                if waiter.active_packet_nonce().is_some() {
                    parked.push(nonce);
                }
            });

            observed_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("waiter did not observe the open packet");
            assert!(packet.settle(1, false).closed_packet.is_some());
            assert!(closer_pending.is_empty());
            closer_pending.clear();
            drop(closer_pending);
            waiter_thread.join().unwrap();
            assert!(pending.lock().unwrap().is_empty());
            assert_eq!(owner.active_packet_nonce(), None);
        }
    }

    #[test]
    fn positive_support_global_service_coalesces_overlapping_parts() {
        fn reserved(turn: PositiveSupportGlobalTurn) -> PositiveSupportGlobalPacketGuard {
            match turn {
                PositiveSupportGlobalTurn::Reserved(guard) => guard,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live global-service shard failed to reserve a packet part")
                }
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => {
                    panic!("live global-service shard was deferred on packet {packet_nonce}")
                }
            }
        }

        let left = PositiveSupportGlobalServiceRuntime::dormant();
        let right = left.parallel_sibling();
        assert!(left.demand_arrived());
        assert!(!right.demand_arrived());

        let support_left = reserved(left.try_reserve_turn(true));
        let support_right = reserved(right.try_reserve_turn(true));
        let left_receipt = support_left
            .reservation
            .as_ref()
            .expect("left Support part retained its receipt");
        let right_receipt = support_right
            .reservation
            .as_ref()
            .expect("right Support part retained its receipt");
        assert_eq!(
            (left_receipt.lane, right_receipt.lane),
            (ProgramServiceLane::Support, ProgramServiceLane::Support)
        );
        assert_eq!(
            left_receipt.nonce, right_receipt.nonce,
            "overlapping shards must join one global packet nonce"
        );
        assert_ne!(
            left_receipt.part_nonce, right_receipt.part_nonce,
            "overlapping shards still require distinct affine part receipts"
        );
        let support_nonce = left_receipt.nonce;
        let opened = left.snapshot();
        assert!(opened.initial_bypass_spent);
        assert_eq!(opened.next_packet_nonce, 1);
        assert_eq!(opened.active_packet_nonce, Some(support_nonce));
        assert_eq!((opened.support_service, opened.max_support_packet), (0, 0));

        let first = support_left.settle(2, true);
        assert_eq!(first.lane, ProgramServiceLane::Support);
        assert!(
            first.closed_packet.is_none(),
            "a non-final part must not charge the global ledger"
        );
        let half_settled = left.snapshot();
        assert_eq!(half_settled.active_packet_nonce, Some(support_nonce));
        assert_eq!(
            (
                half_settled.next_packet_nonce,
                half_settled.support_service,
                half_settled.max_support_packet,
            ),
            (1, 0, 0),
            "partial settlement must neither mint nor charge a packet"
        );
        assert_eq!(
            match left.try_reserve_turn(true) {
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => packet_nonce,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("settled shard unexpectedly became inactive")
                }
                PositiveSupportGlobalTurn::Reserved(_) => {
                    panic!("one shard re-entered an open coalesced packet")
                }
            },
            support_nonce
        );
        assert_eq!(
            match right.try_reserve_turn(true) {
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => packet_nonce,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("live part owner unexpectedly became inactive")
                }
                PositiveSupportGlobalTurn::Reserved(_) => {
                    panic!("one shard minted a second live part in one packet")
                }
            },
            support_nonce
        );
        let late = left.parallel_sibling();
        assert_eq!(
            match late.try_reserve_turn(true) {
                PositiveSupportGlobalTurn::Deferred(packet_nonce) => packet_nonce,
                PositiveSupportGlobalTurn::Inactive => {
                    panic!("late live shard unexpectedly became inactive")
                }
                PositiveSupportGlobalTurn::Reserved(_) => {
                    panic!("a shard created after packet admission extended its finite cohort")
                }
            },
            support_nonce
        );
        drop(late);

        let closed = support_right.settle(3, true);
        let support_packet = closed
            .closed_packet
            .expect("the final Support part must close the coalesced packet");
        assert_eq!(closed.lane, ProgramServiceLane::Support);
        assert_eq!(
            (support_packet.service, support_packet.prior_max_packet),
            (5, 0),
            "the packet charge is the sum of its parts, not their maximum"
        );
        let support_snapshot = left.snapshot();
        assert_eq!(
            (
                support_snapshot.next_packet_nonce,
                support_snapshot.support_service,
                support_snapshot.max_support_packet,
            ),
            (1, 5, 5)
        );
        left.lock().ledger.assert_packet_bounds();

        let exact_left = reserved(left.try_reserve_turn(true));
        let exact_right = reserved(right.try_reserve_turn(true));
        assert_eq!(exact_left.lane(), ProgramServiceLane::Exact);
        assert_eq!(exact_right.lane(), ProgramServiceLane::Exact);
        assert_eq!(
            exact_left
                .reservation
                .as_ref()
                .expect("left Exact part retained its receipt")
                .nonce,
            exact_right
                .reservation
                .as_ref()
                .expect("right Exact part retained its receipt")
                .nonce
        );
        assert!(exact_left.settle(1, true).closed_packet.is_none());
        let exact_packet = exact_right
            .settle(5, true)
            .closed_packet
            .expect("the final Exact part must close the coalesced packet");
        assert_eq!(
            (exact_packet.service, exact_packet.prior_max_packet),
            (6, 0)
        );
        let exact_snapshot = left.snapshot();
        assert_eq!(
            (
                exact_snapshot.next_packet_nonce,
                exact_snapshot.support_service,
                exact_snapshot.exact_service,
                exact_snapshot.max_support_packet,
                exact_snapshot.max_exact_packet,
            ),
            (2, 5, 6, 5, 6)
        );

        let later_support = reserved(left.try_reserve_turn(true));
        assert_eq!(later_support.lane(), ProgramServiceLane::Support);
        let later_support_packet = later_support
            .settle(4, true)
            .closed_packet
            .expect("one-part later Support packet must close immediately");
        assert_eq!(
            (
                later_support_packet.service,
                later_support_packet.prior_max_packet,
            ),
            (4, 5)
        );
        let after_smaller_support = left.snapshot();
        assert_eq!(
            (
                after_smaller_support.next_packet_nonce,
                after_smaller_support.support_service,
                after_smaller_support.exact_service,
                after_smaller_support.max_support_packet,
                after_smaller_support.max_exact_packet,
            ),
            (3, 9, 6, 5, 6),
            "a later H=4 packet raises total H but must leave qH=max(5, 4)=5"
        );
        {
            let coordinator = left.lock();
            assert_eq!(
                coordinator.ledger.next_lane(),
                Some(ProgramServiceLane::Exact),
                "Exact must run after Support crosses the cumulative service tie"
            );
            coordinator.ledger.assert_packet_bounds();
        }
    }

    #[test]
    fn positive_support_idle_clone_rebrands_without_refunding_debt() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        let initial = ledger
            .reserve_support(true)
            .expect("the iterator owns one initial Support bypass");
        ledger.settle_support(&initial, 3, false);
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Idle);

        let mut cloned = ledger
            .try_deep_clone()
            .expect("idle affine custody may be cloned");
        assert_ne!(cloned.brand, ledger.brand);
        assert_eq!(
            (
                cloned.lease,
                cloned.initial_bypass_spent,
                cloned.exact_service,
                cloned.support_service,
                cloned.max_exact_packet,
                cloned.max_support_packet,
                cloned.next_packet_nonce,
                cloned.active_packet_nonce,
                cloned.attribution_tainted,
            ),
            (
                ledger.lease,
                ledger.initial_bypass_spent,
                ledger.exact_service,
                ledger.support_service,
                ledger.max_exact_packet,
                ledger.max_support_packet,
                ledger.next_packet_nonce,
                ledger.active_packet_nonce,
                ledger.attribution_tainted,
            )
        );

        assert!(!cloned.demand_arrived());
        let exact = cloned.reserve_exact().expect("Exact owns the debt tie");
        cloned.settle_exact(&exact, 4, true);
        let next = cloned
            .reserve_support(true)
            .expect("the clone may spend only debt-authorized Support");
        cloned.settle_support(&next, 1, false);
        assert_eq!((cloned.exact_service, cloned.support_service), (4, 4));
        assert_eq!(
            (ledger.lease, ledger.exact_service, ledger.support_service),
            (PositiveSupportServiceLease::Idle, 0, 3),
            "stepping the clone mutated original idle debt"
        );
    }

    #[test]
    fn positive_support_service_debt_taint_is_permanent_and_fail_closed() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        let mut trace = Vec::new();
        assert!(ledger.demand_arrived());
        run_support_service_packet(&mut ledger, 1, true, &mut trace);
        run_exact_service_packet(&mut ledger, 3, &mut trace);
        assert!(ledger.support_is_admissible(true));

        ledger.taint_unattributed();
        assert!(ledger.attribution_tainted);
        assert!(!ledger.support_is_admissible(true));
        assert!(!ledger.demand_arrived());
        assert!(
            ledger.reserve_support(true).is_none(),
            "demand cannot revive a poisoned service epoch"
        );

        let exact = ledger
            .reserve_exact()
            .expect("tainted service admits Exact");
        ledger.settle_exact(&exact, 100, false);
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Idle);
        assert!(
            !ledger.demand_arrived(),
            "turnover must not replace a tainted query-global ledger"
        );
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Parked);
        assert!(!ledger.support_is_admissible(true));
        assert!(
            std::panic::catch_unwind(|| ledger.assert_packet_bounds()).is_err(),
            "a tainted ledger must refuse to claim the packet theorem"
        );
        let clone = ledger
            .try_deep_clone()
            .expect("taint alone does not strand affine custody");
        assert!(clone.attribution_tainted);
        assert!(!clone.support_is_admissible(true));
    }

    #[test]
    #[should_panic(expected = "Support service packets must have positive cost")]
    fn positive_support_service_debt_rejects_zero_cost_packets() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        let reservation = ledger.reserve_support(true).unwrap();
        ledger.settle_support(&reservation, 0, true);
    }

    #[test]
    #[should_panic(expected = "Exact service packets must have positive cost")]
    fn positive_support_service_debt_rejects_zero_cost_exact_packets() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        let initial = ledger.reserve_support(true).unwrap();
        ledger.settle_support(&initial, 1, true);
        let exact = ledger.reserve_exact().unwrap();
        ledger.settle_exact(&exact, 0, true);
    }

    #[test]
    fn positive_exact_credit_uses_cumulative_receipt_local_fusion_work() {
        let final_page = ProgramPage {
            examined: 1,
            resume: None,
        };
        let examined = validated_program_examined(&[final_page], Some(4));
        assert_eq!(examined, [4]);

        let mut budget = PositiveSupportWorkBudget::default();
        budget.mint_demand();
        assert_eq!(budget.mint_exact(examined[0]), 4);
        assert_eq!(
            (
                budget.demand_minted,
                budget.exact_minted,
                budget.available()
            ),
            (1, 4, 5),
            "C must use cumulative validated work, not the final fused page"
        );
        budget.assert_conservation();
    }

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
            states: &mut Vec<Self::State>,
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
        accept_candidate: bool,
        report_support: bool,
        examined: usize,
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
            states: &mut Vec<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), batch.limits.len());
            for (input, state) in states.drain(..).enumerate() {
                let input = u32::try_from(input).unwrap();
                if state.accept_candidate {
                    let candidate = batch.candidate_sets[input as usize]
                        .and_then(|candidates| candidates.first())
                        .copied()
                        .expect("accepting one-shot Confirm lost its candidate");
                    effects.accept(input, candidate);
                } else if state.report_support {
                    effects.support(input);
                }
                effects.page(
                    state.examined,
                    state.keep_cleanup_live.then_some(TypedResume::Immediate(
                        OneShotSupportState {
                            keep_cleanup_live: false,
                            accept_candidate: state.accept_candidate,
                            report_support: state.report_support,
                            examined: state.examined,
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

    fn queue_one_shot_positive_support(
        scheduler: &mut DeltaScheduler,
        root: &OneShotSupportProgram,
        parent: PositiveConfirmParentId,
        candidate: RawInline,
        keep_cleanup_live: bool,
    ) -> (ActivationId, ActiveDeltaContinuation) {
        queue_one_shot_positive_support_with_result(
            scheduler,
            root,
            parent,
            candidate,
            keep_cleanup_live,
            true,
        )
    }

    fn queue_one_shot_positive_support_with_result(
        scheduler: &mut DeltaScheduler,
        root: &OneShotSupportProgram,
        parent: PositiveConfirmParentId,
        candidate: RawInline,
        keep_cleanup_live: bool,
        report_support: bool,
    ) -> (ActivationId, ActiveDeltaContinuation) {
        queue_positive_support_with_examined(
            scheduler,
            root,
            parent,
            candidate,
            keep_cleanup_live,
            report_support,
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn queue_positive_support_with_examined(
        scheduler: &mut DeltaScheduler,
        root: &OneShotSupportProgram,
        parent: PositiveConfirmParentId,
        candidate: RawInline,
        keep_cleanup_live: bool,
        report_support: bool,
        examined: usize,
    ) -> (ActivationId, ActiveDeltaContinuation) {
        let child = scheduler
            .registry
            .open_positive_support_activation(
                parent,
                0,
                candidate,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .expect("the exact-and-Support parent should open its hedge");
        let request = ProgramRequest {
            action: ProgramAction::Support,
            bound: VariableSet::new_singleton(0),
        };
        let spec = ProgramRef::new(root);
        let route = spec
            .route(request)
            .expect("the one-shot Support Program should accept its route");
        let state = scheduler.prepare_program(DeltaDesc::leaf(0, 0), route, spec);
        let credit = scheduler
            .registry
            .issue_credit(child, CreditKind::Program { join: None });
        let work = insert_engine_program_state(
            root,
            scheduler
                .program_runtimes
                .get_mut(&state)
                .expect("prepared one-shot Support lost its runtime"),
            ProgramActivation(child.0),
            OneShotSupportState {
                keep_cleanup_live,
                accept_candidate: false,
                report_support,
                examined,
            },
        );
        let active = scheduler
            .file_program_state(
                state,
                vec![ProgramTask {
                    activation: child,
                    credit,
                    work,
                }],
            )
            .expect("the positive Support child filed one affine task");
        (child, active)
    }

    fn queue_exact_confirm_credit(
        scheduler: &mut DeltaScheduler,
        state: DeltaStateId,
        activation: ActivationId,
        credit: ProducerCredit,
    ) -> ActiveDeltaContinuation {
        queue_exact_confirm_program(scheduler, state, activation, credit, false, false)
    }

    fn queue_exact_confirm_program(
        scheduler: &mut DeltaScheduler,
        state: DeltaStateId,
        activation: ActivationId,
        credit: ProducerCredit,
        keep_cleanup_live: bool,
        accept_candidate: bool,
    ) -> ActiveDeltaContinuation {
        queue_exact_confirm_with_examined(
            scheduler,
            state,
            activation,
            credit,
            keep_cleanup_live,
            accept_candidate,
            1,
        )
    }

    fn queue_exact_confirm_with_examined(
        scheduler: &mut DeltaScheduler,
        state: DeltaStateId,
        activation: ActivationId,
        credit: ProducerCredit,
        keep_cleanup_live: bool,
        accept_candidate: bool,
        examined: usize,
    ) -> ActiveDeltaContinuation {
        let work = insert_engine_program_state(
            &OneShotSupportProgram,
            scheduler
                .program_runtimes
                .get_mut(&state)
                .expect("prepared one-shot Support lost its runtime"),
            ProgramActivation(activation.0),
            OneShotSupportState {
                keep_cleanup_live,
                accept_candidate,
                report_support: false,
                examined,
            },
        );
        scheduler
            .file_program_state(
                state,
                vec![ProgramTask {
                    activation,
                    credit,
                    work,
                }],
            )
            .expect("the exact Confirm parent filed one affine task")
    }

    fn queue_neutral_confirm_program(
        scheduler: &mut DeltaScheduler,
        state: DeltaStateId,
        candidate: RawInline,
        slot: u32,
    ) -> (ActivationId, ActiveDeltaContinuation) {
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![candidate]),
            },
            terminal_positive_return(Vec::new()),
            None,
            None,
        );
        let task = install_program_tasks(
            &mut scheduler.registry,
            activation,
            [slot],
            DispatchClass::new(0),
            ProgramPacing::Search,
        )
        .pop()
        .expect("the neutral Confirm fixture installed one Program task");
        let active = scheduler
            .file_program_state(state, vec![task])
            .expect("the neutral Confirm fixture filed one Program task");
        (activation, active)
    }

    #[test]
    fn program_service_lane_starts_at_demand_without_refining_the_cohort_key() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        assert_eq!(
            scheduler.program_lane_selection,
            ProgramLaneSelection::Unrestricted
        );
        let candidate = value(60);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);

        assert_eq!(
            ProgramServiceLane::of(&scheduler.registry, exact),
            ProgramServiceLane::Neutral
        );
        assert_eq!(
            ProgramServiceLane::of(&scheduler.registry, support),
            ProgramServiceLane::Neutral
        );
        assert!(scheduler.registry.mint_positive_support_demand(parent));
        assert_eq!(
            ProgramServiceLane::of(&scheduler.registry, exact),
            ProgramServiceLane::Exact
        );
        assert_eq!(
            ProgramServiceLane::of(&scheduler.registry, support),
            ProgramServiceLane::Support
        );
    }

    #[test]
    fn lane_pure_global_program_pop_batches_exact_parents_but_retains_neutral_peer() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.program_lane_selection = ProgramLaneSelection::LanePure;

        let first_value = value(61);
        let (first_exact, first_parent, first_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [first_value], None, true);
        let (_, first_support) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            first_parent,
            first_value,
            false,
        );
        assert!(scheduler
            .registry
            .mint_positive_support_demand(first_parent));
        let (neutral, _) =
            queue_neutral_confirm_program(&mut scheduler, first_support.state, value(62), 91);
        let _ = queue_exact_confirm_credit(
            &mut scheduler,
            first_support.state,
            first_exact,
            first_credit,
        );

        let second_value = value(63);
        let (second_exact, second_parent, second_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [second_value], None, true);
        let (_, second_support) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            second_parent,
            second_value,
            false,
        );
        assert_eq!(second_support.state, first_support.state);
        assert!(scheduler
            .registry
            .mint_positive_support_demand(second_parent));
        let _ = queue_exact_confirm_credit(
            &mut scheduler,
            first_support.state,
            second_exact,
            second_credit,
        );

        let bucket = &scheduler.program_worklist[&first_support.state];
        let neutral_task = bucket
            .tasks
            .iter()
            .find(|task| task.activation == neutral)
            .expect("the neutral Confirm peer remained queued");
        let exact_task = bucket
            .tasks
            .iter()
            .find(|task| task.activation == first_exact)
            .expect("the first exact parent remained queued");
        assert_eq!(
            ProgramCohortKey::of(&scheduler.registry, neutral_task),
            ProgramCohortKey::of(&scheduler.registry, exact_task),
            "the lane fence must not refine ProgramCohortKey"
        );

        let (state, selected, grants, _) = scheduler.pop_program_bounded(8);
        assert_eq!(state, first_support.state);
        assert!(grants.iter().all(Option::is_none));
        assert_eq!(
            selected
                .iter()
                .map(|task| task.activation)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([first_exact, second_exact])
        );
        assert!(selected.iter().all(|task| {
            ProgramServiceLane::of(&scheduler.registry, task.activation)
                == ProgramServiceLane::Exact
        }));
        assert!(scheduler.program_worklist[&state]
            .tasks
            .iter()
            .any(|task| task.activation == neutral));
    }

    #[test]
    fn lane_pure_global_program_pop_batches_support_parents_but_retains_dormant_peer() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.program_lane_selection = ProgramLaneSelection::LanePure;

        let first_value = value(64);
        let (_, first_parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [first_value], None, true);
        let (first_support, first_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            first_parent,
            first_value,
            false,
        );
        assert!(scheduler
            .registry
            .mint_positive_support_demand(first_parent));

        let dormant_value = value(65);
        let (_, dormant_parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [dormant_value], None, true);
        let (dormant_support, dormant_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            dormant_parent,
            dormant_value,
            false,
        );
        assert_eq!(dormant_active.state, first_active.state);

        let second_value = value(66);
        let (_, second_parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [second_value], None, true);
        let (second_support, second_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            second_parent,
            second_value,
            false,
        );
        assert_eq!(second_active.state, first_active.state);
        assert!(scheduler
            .registry
            .mint_positive_support_demand(second_parent));

        let bucket = &scheduler.program_worklist[&first_active.state];
        let dormant_task = bucket
            .tasks
            .iter()
            .find(|task| task.activation == dormant_support)
            .expect("the dormant Support peer remained queued");
        let started_task = bucket
            .tasks
            .iter()
            .find(|task| task.activation == first_support)
            .expect("the started Support peer remained queued");
        assert_eq!(
            ProgramCohortKey::of(&scheduler.registry, dormant_task),
            ProgramCohortKey::of(&scheduler.registry, started_task),
            "the lane fence must not refine ProgramCohortKey"
        );

        scheduler.next_program_lane = Some(ProgramServiceLane::Support);
        let (state, selected, grants, _) = scheduler.pop_program_bounded(8);
        assert_eq!(state, first_active.state);
        assert_eq!(
            selected
                .iter()
                .map(|task| task.activation)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([first_support, second_support])
        );
        assert!(selected.iter().all(|task| {
            ProgramServiceLane::of(&scheduler.registry, task.activation)
                == ProgramServiceLane::Support
        }));
        assert!(grants.iter().all(Option::is_some));
        assert!(scheduler.program_worklist[&state]
            .tasks
            .iter()
            .any(|task| task.activation == dormant_support));
    }

    #[test]
    fn support_lane_preference_selects_a_lower_global_program_state_once() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.program_lane_selection = ProgramLaneSelection::LanePure;

        let candidate = value(67);
        let (_, parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));

        let higher_state = test_program_state(&mut scheduler);
        assert!(higher_state > support_active.state);
        let (_, neutral_active) =
            queue_neutral_confirm_program(&mut scheduler, higher_state, value(68), 92);
        assert_eq!(neutral_active.state, higher_state);
        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        assert_eq!(
            scheduler.next_program_lane,
            Some(ProgramServiceLane::Support)
        );

        let (preferred_state, preferred, grants, _) = scheduler.pop_program_bounded(1);
        assert_eq!(preferred_state, support_active.state);
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].activation, support);
        assert!(grants[0].is_some());
        assert_eq!(scheduler.next_program_lane, None);

        let (ordinary_state, ordinary, grants, _) = scheduler.pop_program_bounded(1);
        assert_eq!(ordinary_state, higher_state);
        assert_eq!(ordinary.len(), 1);
        assert_eq!(ordinary[0].activation, neutral_active.activation);
        assert!(grants[0].is_none());
    }

    #[test]
    fn count_lane_preference_survives_global_service_arming_hook() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.program_lane_selection = ProgramLaneSelection::LanePure;

        let support_candidate = value(75);
        let (_, support_parent, _, _) = open_tapped_confirm_with_support(
            &mut scheduler.registry,
            [support_candidate],
            None,
            true,
        );
        let (support, support_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            support_parent,
            support_candidate,
            false,
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));

        let request = ProgramRequest {
            action: ProgramAction::Support,
            bound: VariableSet::new_singleton(0),
        };
        let spec = ProgramRef::new(&root);
        let route = spec
            .route(request)
            .expect("one-shot Program accepts Support");
        let higher_state = scheduler.prepare_program(DeltaDesc::leaf(0, 1), route, spec);
        assert!(higher_state > support_active.state);
        let neutral_candidate = value(76);
        let (neutral, _, neutral_credit, _) = open_tapped_confirm_with_support(
            &mut scheduler.registry,
            [neutral_candidate],
            None,
            true,
        );
        let _ = queue_exact_confirm_program(
            &mut scheduler,
            higher_state,
            neutral,
            neutral_credit,
            false,
            false,
        );

        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let outcome = scheduler.step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(outcome.has_stable_effect());
        assert_eq!(stats.delta_positive_publication_support_wins, 1);
        assert!(
            scheduler.program_worklist.contains_key(&higher_state),
            "the higher neutral state ran after the count preference was erased"
        );
    }

    #[test]
    fn global_service_demand_never_mints_count_credit_and_arms_a_lane() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(69);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        scheduler.assert_runnable_positive_support_task_index();
        assert!(!scheduler
            .runnable_positive_support_tasks
            .contains_key(&parent));

        let mut stats = ResidualStateStats::default();
        assert!(
            scheduler.begin_public_pull_demand(&mut stats).is_none(),
            "global service demand must not create an activation-local sprint"
        );
        let ledger = scheduler
            .registry
            .positive_publication_snapshot(parent)
            .expect("service parent retained its publication ledger");
        assert_eq!(ledger.support_work, PositiveSupportWorkBudget::default());
        assert!(ledger.service_support_started);
        assert_eq!(
            (
                stats.delta_positive_support_demand_assigned,
                stats.delta_positive_support_examined,
                stats.delta_positive_support_exact_credited,
                stats.delta_positive_support_credit_retired,
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(stats.delta_positive_support_service_parents_started, 1);
        assert_eq!(stats.delta_positive_support_service_epochs, 1);
        assert_eq!(
            stats.delta_positive_support_demand_discovery_task_visits, 0,
            "service demand must use its incremental parent locator"
        );

        scheduler.retire_unassigned_public_pull_demand();
        assert!(
            scheduler.begin_public_pull_demand(&mut stats).is_none(),
            "an already-started parked parent must not consume later demand"
        );
        assert_eq!(
            stats.delta_positive_support_demand_discovery_task_visits, 0,
            "a no-candidate service pull must not rediscover parked tasks"
        );

        let _turn = match scheduler.reserve_global_service_packet() {
            PositiveSupportServiceTurn::Reserved(turn) => turn,
            PositiveSupportServiceTurn::Inactive => {
                panic!("started service parent failed to activate the initial Support turn")
            }
            PositiveSupportServiceTurn::Deferred(packet_nonce) => {
                panic!("single scheduler was deferred on packet {packet_nonce}")
            }
        };
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(
            scheduler.next_program_lane,
            Some(ProgramServiceLane::Support)
        );
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);
        assert!(scheduler.has_runnable_positive_support_parent(parent));
    }

    #[test]
    fn global_service_locator_waits_for_the_last_runnable_sibling() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(83);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (first, first_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let (second, second_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        assert_eq!(first_active.state, second_active.state);
        let _ = queue_exact_confirm_credit(&mut scheduler, first_active.state, exact, exact_credit);

        scheduler.park_positive_support_activations(&AHashSet::from_iter([first]));
        assert!(
            scheduler.positive_support_service_demand_queue.is_empty(),
            "one parked sibling cannot expose a parent with runnable Support custody"
        );
        assert!(scheduler.has_runnable_positive_support_parent(parent));

        scheduler.park_positive_support_activations(&AHashSet::from_iter([second]));
        assert_eq!(scheduler.positive_support_service_demand_queue, [parent]);
        assert!(!scheduler.has_runnable_positive_support_parent(parent));

        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert!(scheduler
            .registry
            .positive_support_service_is_started(parent));
        assert_eq!(stats.delta_positive_support_service_parents_started, 1);
        assert_eq!(stats.delta_positive_support_demand_discovery_task_visits, 0);
    }

    #[test]
    fn global_service_locator_probes_same_parent_once_per_filing_batch() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(84);
        let (_, parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let mut children = (0..65)
            .map(|_| {
                queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false).0
            })
            .collect::<Vec<_>>();
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 65);
        let final_runnable = children
            .pop()
            .expect("the fixture retained one runnable sibling");

        scheduler.park_positive_support_activations(&children.into_iter().collect());
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);
        assert_eq!(
            scheduler.positive_support_service_locator_boundary_probes, 1,
            "one filing batch must probe a shared semantic parent once"
        );
        assert!(scheduler.positive_support_service_demand_queue.is_empty());
        assert!(scheduler.has_runnable_positive_support_parent(parent));

        scheduler.park_positive_support_activations(&AHashSet::from_iter([final_runnable]));
        scheduler.assert_runnable_positive_support_task_index();
        assert!(!scheduler
            .runnable_positive_support_tasks
            .contains_key(&parent));
        assert_eq!(
            scheduler.positive_support_service_locator_boundary_probes, 2,
            "filing the final sibling must retry exactly one parent probe"
        );
        assert_eq!(scheduler.positive_support_service_demand_queue, [parent]);
        assert!(!scheduler.has_runnable_positive_support_parent(parent));
    }

    #[test]
    fn global_service_mode_keeps_pre_epoch_program_work_neutral() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(74);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let _ = scheduler.step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(
            (
                stats.delta_positive_support_service_exact_packets,
                stats.delta_positive_support_service_support_packets,
            ),
            (0, 0)
        );
        assert_eq!(
            scheduler.global_service_debt_snapshot().lease,
            PositiveSupportServiceLease::Dormant
        );
    }

    #[test]
    fn global_service_runtime_settles_one_support_packet_and_preserves_count_zeros() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(70);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, true);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);
        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let step = scheduler.try_step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let outcome = match step {
            DeltaSchedulerStep::Completed {
                outcome,
                global_packet_event,
            } => {
                assert!(
                    global_packet_event.is_none(),
                    "a direct packet emitted a shared-executor wake boundary"
                );
                outcome
            }
            DeltaSchedulerStep::Deferred { packet_nonce } => {
                panic!("direct scheduler deferred on packet {packet_nonce}")
            }
        };
        assert!(outcome.has_stable_effect());
        assert_eq!(stats.delta_positive_publication_support_wins, 1);
        assert_eq!(
            (
                stats.delta_positive_support_demand_assigned,
                stats.delta_positive_support_examined,
                stats.delta_positive_support_exact_paired_examined,
                stats.delta_positive_support_exact_credited,
                stats.delta_positive_support_credit_retired,
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(stats.delta_positive_support_service_support_examined, 1);
        assert_eq!(stats.delta_positive_support_service_support_packets, 1);
        assert_eq!(
            stats.delta_positive_support_service_support_packet_allowance,
            1
        );
        assert_eq!(
            scheduler.global_service_debt_snapshot().lease,
            PositiveSupportServiceLease::Idle
        );
        scheduler.assert_runnable_positive_support_task_index();
        assert!(scheduler.runnable_positive_support_tasks.is_empty());
    }

    #[test]
    fn global_service_charges_validated_pages_not_family_telemetry() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(71);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) = queue_positive_support_with_examined(
            &mut scheduler,
            &root,
            parent,
            candidate,
            false,
            true,
            7,
        );
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let outcome = scheduler.step_bounded(
            &root,
            &plan,
            7,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(outcome.has_stable_effect());
        assert_eq!(
            (
                stats.delta_source_candidates_examined,
                stats.delta_transition_candidates_examined,
            ),
            (0, 0),
            "family counters remain optional placement telemetry"
        );
        assert_eq!(stats.delta_positive_support_service_support_examined, 7);
        assert_eq!(
            stats.delta_positive_support_service_support_packet_allowance,
            7
        );
        assert_eq!(scheduler.global_service_debt_snapshot().support_service, 7);
    }

    #[test]
    fn later_global_service_demand_starts_a_distinct_parked_parent() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let mut parents = Vec::new();
        let mut children = AHashSet::new();
        for candidate in [value(72), value(73)] {
            let (_, parent, _, _) =
                open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
            let (child, _) =
                queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
            parents.push(parent);
            children.insert(child);
        }
        scheduler.park_positive_support_activations(&children);

        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert_eq!(
            parents
                .iter()
                .filter(|&&parent| scheduler
                    .registry
                    .positive_support_service_is_started(parent))
                .count(),
            1
        );

        scheduler.retire_unassigned_public_pull_demand();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert!(
            parents.iter().all(|&parent| scheduler
                .registry
                .positive_support_service_is_started(parent)),
            "a later pull must not be consumed by the already-started parent"
        );
        assert_eq!(stats.delta_positive_support_service_parents_started, 2);
        assert_eq!(
            stats.delta_positive_support_service_epochs, 1,
            "adding a parent to a live service epoch must not mint a bypass"
        );
        scheduler.retire_unassigned_public_pull_demand();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert_eq!(
            stats.delta_positive_support_demand_discovery_task_visits, 0,
            "exhausted service-parent discovery must stay O(1)"
        );
    }

    #[test]
    fn global_service_reparks_support_wake_remainders_after_a_narrow_packet() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let mut children = AHashSet::new();
        for candidate in [value(77), value(78)] {
            let (exact, parent, exact_credit, _) =
                open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
            let (child, support_active) = queue_one_shot_positive_support_with_result(
                &mut scheduler,
                &root,
                parent,
                candidate,
                false,
                false,
            );
            let _ = queue_exact_confirm_credit(
                &mut scheduler,
                support_active.state,
                exact,
                exact_credit,
            );
            children.insert(child);
        }
        scheduler.park_positive_support_activations(&children);
        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        scheduler.retire_unassigned_public_pull_demand();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let _ = scheduler.step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(stats.delta_positive_support_service_support_packets, 1);
        assert_eq!(scheduler.global_service_woken_support_tasks, 0);
        assert!(
            scheduler.program_worklist.iter().all(|(_, bucket)| {
                bucket.tasks.iter().all(|task| {
                    ProgramServiceLane::of(&scheduler.registry, task.activation)
                        != ProgramServiceLane::Support
                })
            }),
            "an unselected Support wake remainder stayed globally runnable"
        );
        assert_eq!(
            scheduler.parked_positive_support_worklist.len(),
            1,
            "exactly one unselected parent must return to parked custody"
        );
    }

    #[test]
    fn global_service_debt_survives_empty_parent_turnover() {
        let mut ledger = PositiveSupportServiceDebtLedger::dormant();
        assert!(ledger.demand_arrived());
        let first_brand = ledger.brand;
        let first = ledger
            .reserve_support(true)
            .expect("the first epoch owns one initial Support bypass");
        ledger.settle_support(&first, 7, false);
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Idle);
        assert!(ledger.initial_bypass_spent);
        assert_eq!(
            (
                ledger.exact_service,
                ledger.support_service,
                ledger.max_exact_packet,
                ledger.max_support_packet,
                ledger.next_packet_nonce,
            ),
            (0, 7, 0, 7, 1)
        );

        assert!(
            !ledger.demand_arrived(),
            "later parents must join the existing query-global epoch"
        );
        assert_eq!(ledger.brand, first_brand);
        assert_eq!((ledger.exact_service, ledger.support_service), (0, 7));
        assert!(
            ledger.reserve_support(true).is_none(),
            "empty turnover minted a second initial Support bypass"
        );
        let exact = ledger
            .reserve_exact()
            .expect("Exact must repay carried debt");
        ledger.settle_exact(&exact, 8, true);
        let second = ledger
            .reserve_support(true)
            .expect("carried Exact service eventually repays prior Support debt");
        ledger.settle_support(&second, 1, false);
        assert_eq!(ledger.lease, PositiveSupportServiceLease::Idle);
        assert_eq!(
            (
                ledger.exact_service,
                ledger.support_service,
                ledger.max_exact_packet,
                ledger.max_support_packet,
                ledger.next_packet_nonce,
            ),
            (8, 8, 8, 7, 3)
        );
        assert_eq!(ledger.brand, first_brand);
        ledger.assert_packet_bounds();
    }

    #[test]
    fn global_service_runtime_carries_debt_across_parent_turnover() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let mut stats = ResidualStateStats::default();
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();

        let first_candidate = value(79);
        let (first_exact, first_parent, first_credit, _) = open_tapped_confirm_with_support(
            &mut scheduler.registry,
            [first_candidate],
            None,
            true,
        );
        let (first_child, first_active) = queue_positive_support_with_examined(
            &mut scheduler,
            &root,
            first_parent,
            first_candidate,
            false,
            true,
            7,
        );
        let _ = queue_exact_confirm_credit(
            &mut scheduler,
            first_active.state,
            first_exact,
            first_credit,
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([first_child]));
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        let first_brand = scheduler.global_service_debt_snapshot().brand;
        let _ = scheduler.step_bounded(
            &root,
            &plan,
            8,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let debt = scheduler.global_service_debt_snapshot();
        assert_eq!(debt.lease, PositiveSupportServiceLease::Idle);
        assert_eq!(
            (
                debt.exact_service,
                debt.support_service,
                debt.max_exact_packet,
                debt.max_support_packet,
            ),
            (0, 7, 0, 7)
        );

        scheduler.retire_unassigned_public_pull_demand();
        let second_candidate = value(80);
        let (second_exact, second_parent, second_credit, _) = open_tapped_confirm_with_support(
            &mut scheduler.registry,
            [second_candidate],
            None,
            true,
        );
        let (second_child, second_active) = queue_positive_support_with_examined(
            &mut scheduler,
            &root,
            second_parent,
            second_candidate,
            false,
            true,
            1,
        );
        let _ = queue_exact_confirm_with_examined(
            &mut scheduler,
            second_active.state,
            second_exact,
            second_credit,
            true,
            false,
            8,
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([second_child]));
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        let debt = scheduler.global_service_debt_snapshot();
        assert_eq!(debt.brand, first_brand);
        assert_eq!((debt.exact_service, debt.support_service), (0, 7));
        let _ = scheduler.step_bounded(
            &root,
            &plan,
            8,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let debt = scheduler.global_service_debt_snapshot();
        assert_eq!(
            (debt.exact_service, debt.support_service),
            (8, 7),
            "the second parent must pay the carried Support debt with Exact"
        );
        let _ = scheduler.step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let debt = scheduler.global_service_debt_snapshot();
        assert_eq!(
            (
                debt.exact_service,
                debt.support_service,
                debt.max_exact_packet,
                debt.max_support_packet,
            ),
            (8, 8, 8, 7)
        );
        assert_eq!(debt.lease, PositiveSupportServiceLease::Idle);
        assert_eq!(debt.next_packet_nonce, 3);
        assert_eq!(stats.delta_positive_support_service_parents_started, 2);
        assert_eq!(stats.delta_positive_support_service_epochs, 1);
        assert_eq!(
            (
                stats.delta_positive_support_service_exact_packets,
                stats.delta_positive_support_service_support_packets,
                stats.delta_positive_support_service_exact_examined,
                stats.delta_positive_support_service_support_examined,
                stats.delta_positive_publication_support_wins,
            ),
            (1, 2, 8, 8, 2)
        );
        assert_eq!(
            (
                stats.delta_positive_support_service_exact_packet_allowance,
                stats.delta_positive_support_service_support_packet_allowance,
            ),
            (8, 7),
            "allowance telemetry must retain one lineage-wide maximum per lane"
        );
    }

    #[test]
    fn global_service_started_scheduler_clone_rebrands_and_diverges() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(81);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);
        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());

        let mut cloned = scheduler.deep_clone();
        #[cfg(feature = "parallel")]
        {
            assert!(scheduler.global_service_debt_is_direct());
            assert!(cloned.global_service_debt_is_direct());
        }
        assert_ne!(cloned.registry.brand, scheduler.registry.brand);
        let cloned_debt = cloned.global_service_debt_snapshot();
        let original_debt = scheduler.global_service_debt_snapshot();
        assert_ne!(cloned_debt.brand, original_debt.brand);
        assert_eq!(cloned_debt.lease, PositiveSupportServiceLease::Parked);
        let cloned_parent = PositiveConfirmParentId {
            brand: cloned.registry.brand,
            activation: parent.activation,
        };
        assert!(cloned
            .registry
            .positive_support_service_is_started(cloned_parent));

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut clone_stats = ResidualStateStats::default();
        let _ = cloned.step_bounded(
            &root,
            &plan,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut clone_stats,
        );
        assert_eq!(clone_stats.delta_positive_publication_support_wins, 1);
        assert_eq!(
            scheduler.global_service_debt_snapshot().lease,
            PositiveSupportServiceLease::Parked,
            "stepping the clone mutated the original service lease"
        );
        assert!(scheduler
            .registry
            .positive_support_service_is_started(parent));
        assert_eq!(scheduler.parked_positive_support_worklist.len(), 1);
    }

    #[test]
    fn global_service_pending_demand_locator_rebrands_across_clone() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(82);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (support, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let _ =
            queue_exact_confirm_credit(&mut scheduler, support_active.state, exact, exact_credit);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([support]));
        assert_eq!(scheduler.positive_support_service_demand_queue, [parent]);

        let mut cloned = scheduler.deep_clone();
        let cloned_parent = PositiveConfirmParentId {
            brand: cloned.registry.brand,
            activation: parent.activation,
        };
        assert_eq!(
            cloned.positive_support_service_demand_queue,
            [cloned_parent],
            "clone-local demand custody must carry the cloned registry brand"
        );

        for (current, current_parent) in [(&mut scheduler, parent), (&mut cloned, cloned_parent)] {
            let mut stats = ResidualStateStats::default();
            assert!(current.begin_public_pull_demand(&mut stats).is_none());
            assert!(current
                .registry
                .positive_support_service_is_started(current_parent));
            assert_eq!(stats.delta_positive_support_service_parents_started, 1);
            assert_eq!(stats.delta_positive_support_service_epochs, 1);
            assert_eq!(stats.delta_positive_support_demand_discovery_task_visits, 0);
            assert!(current.positive_support_service_demand_queue.is_empty());
            assert!(current.positive_support_service_demand_queued.is_empty());
        }
    }

    #[test]
    fn runnable_positive_support_index_rebrands_and_diverges_across_clone() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        scheduler.enable_positive_support_global_service_debt();
        let candidate = value(85);
        let (exact, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (first, active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let (second, _) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let _ = queue_exact_confirm_credit(&mut scheduler, active.state, exact, exact_credit);
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 2);

        let mut cloned = scheduler.deep_clone();
        let cloned_parent = PositiveConfirmParentId {
            brand: cloned.registry.brand,
            activation: parent.activation,
        };
        cloned.assert_runnable_positive_support_task_index();
        assert_eq!(cloned.runnable_positive_support_tasks[&cloned_parent], 2);
        assert!(!cloned.runnable_positive_support_tasks.contains_key(&parent));

        cloned.park_positive_support_activations(&AHashSet::from_iter([first]));
        cloned.assert_runnable_positive_support_task_index();
        assert_eq!(cloned.runnable_positive_support_tasks[&cloned_parent], 1);
        assert!(cloned.has_runnable_positive_support_parent(cloned_parent));
        assert!(cloned.registry.is_live_positive_support(second));

        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 2);
        assert!(
            scheduler.has_active_program(active),
            "parking clone-local custody mutated the original worklist"
        );
    }

    #[test]
    fn parked_positive_support_releases_its_lease_while_exact_remains_runnable() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(6);
        let (parent_activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let exact_active = queue_exact_confirm_credit(
            &mut scheduler,
            support_active.state,
            parent_activation,
            exact_credit,
        );

        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        assert!(scheduler.has_active_program(exact_active));
        assert!(!scheduler.has_active_program(support_active));
        assert!(scheduler.has_active_parked_positive_support(support_active));
        assert!(
            !scheduler.is_empty(),
            "parked Support must not hide its runnable exact parent"
        );

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let released = scheduler.step_active_bounded(
            &root,
            &plan,
            support_active,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(released.status, ActiveDeltaStatus::Parked);
        assert!(released.resume.is_none());
        assert!(!released.outcome.has_stable_effect());
        assert!(released.outcome.completed_activation_ids.is_empty());
        assert!(scheduler.registry.is_live(child));
        assert!(scheduler.has_active_parked_positive_support(support_active));
        assert!(scheduler.has_active_program(exact_active));
        assert!(stable.is_empty());
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .support_work,
            PositiveSupportWorkBudget::default(),
            "parked Support must not cold-start without public demand"
        );
    }

    #[test]
    fn parked_positive_support_survives_deep_clone_with_rebranded_credit() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(7);
        let (parent_activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        let exact_active = queue_exact_confirm_credit(
            &mut scheduler,
            support_active.state,
            parent_activation,
            exact_credit,
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        assert!(
            scheduler.registry.mint_positive_support_demand(parent),
            "the parked parent should accept clone-test demand"
        );
        let original_budget = scheduler
            .registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .support_work;

        let original_key = scheduler
            .parked_positive_support_worklist
            .get(&support_active.state)
            .unwrap()
            .tasks
            .iter()
            .find(|task| task.activation == child)
            .unwrap()
            .credit
            .key;
        let original_brand = scheduler.registry.brand;
        let mut cloned = scheduler.deep_clone();
        let cloned_task = cloned
            .parked_positive_support_worklist
            .get(&support_active.state)
            .unwrap()
            .tasks
            .iter()
            .find(|task| task.activation == child)
            .expect("the cloned scheduler retained parked Support custody");
        assert_eq!(cloned_task.credit.key, original_key);
        assert_ne!(cloned_task.credit.brand, original_brand);
        assert_eq!(cloned_task.credit.brand, cloned.registry.brand);
        assert!(cloned.has_active_parked_positive_support(support_active));
        assert!(cloned.has_active_program(exact_active));
        let cloned_parent = PositiveConfirmParentId {
            brand: cloned.registry.brand,
            activation: parent.activation,
        };
        assert_eq!(
            cloned
                .registry
                .positive_publication_snapshot(cloned_parent)
                .unwrap()
                .support_work,
            original_budget,
            "deep clone must preserve the started parent budget exactly"
        );

        let (completed, retired) =
            cloned.retire_positive_support_activations(&root, &plan, &AHashSet::from_iter([child]));
        assert_eq!(completed, [child]);
        assert_eq!(retired, 1);
        assert!(!cloned.registry.is_live(child));
        assert!(!cloned.has_active_parked_positive_support(support_active));
        assert!(
            scheduler.registry.is_live(child)
                && scheduler.has_active_parked_positive_support(support_active),
            "cancelling the clone mutated original parked custody"
        );
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .support_work
                .available(),
            1,
            "clone-local retirement mutated the original started budget"
        );
    }

    #[test]
    fn one_public_pull_assigns_once_and_a_later_pull_may_assign_another_parent() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        let first_value = value(31);
        let second_value = value(32);
        let (_, first_parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [first_value], None, true);
        let (first_child, first_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            first_parent,
            first_value,
            false,
        );
        let (_, second_parent, _, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [second_value], None, true);
        let (second_child, second_active) = queue_one_shot_positive_support(
            &mut scheduler,
            &root,
            second_parent,
            second_value,
            false,
        );
        scheduler
            .park_positive_support_activations(&AHashSet::from_iter([first_child, second_child]));

        let mut stats = ResidualStateStats::default();
        let preferred = scheduler
            .begin_public_pull_demand(&mut stats)
            .expect("one pull should assign one parked parent");
        let assigned_parent = scheduler
            .registry
            .positive_support_parent_for_child(preferred.activation)
            .unwrap();
        let unassigned_parent = if assigned_parent == first_parent {
            second_parent
        } else {
            first_parent
        };
        assert_eq!(stats.delta_positive_support_demand_assigned, 1);
        assert!(!scheduler.has_unassigned_public_pull_demand());
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(assigned_parent)
                .unwrap()
                .support_work
                .demand_minted,
            1
        );
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(unassigned_parent)
                .unwrap()
                .support_work,
            PositiveSupportWorkBudget::default()
        );
        assert!(
            scheduler.has_active_program(preferred),
            "assigned D should prefer its newly runnable Support task"
        );
        assert_eq!(
            usize::from(scheduler.has_active_parked_positive_support(first_active))
                + usize::from(scheduler.has_active_parked_positive_support(second_active)),
            1,
            "the other parent's Support task must remain parked"
        );

        assert!(
            scheduler.begin_public_pull_demand(&mut stats).is_none(),
            "reopening an assigned token in the same pull must not mint another D"
        );
        assert_eq!(stats.delta_positive_support_demand_assigned, 1);
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(unassigned_parent)
                .unwrap()
                .support_work,
            PositiveSupportWorkBudget::default(),
            "the second parent must remain creditless until a later public pull"
        );

        scheduler.retire_unassigned_public_pull_demand();
        let second_preference = scheduler
            .begin_public_pull_demand(&mut stats)
            .expect("a later public pull may assign the remaining parked parent");
        assert_eq!(
            scheduler
                .registry
                .positive_support_parent_for_child(second_preference.activation),
            Some(unassigned_parent)
        );
        assert_eq!(stats.delta_positive_support_demand_assigned, 2);
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(unassigned_parent)
                .unwrap()
                .support_work
                .demand_minted,
            1
        );
        assert!(
            !scheduler.has_active_parked_positive_support(first_active)
                && !scheduler.has_active_parked_positive_support(second_active),
            "the second pull should wake the sole remaining parked Support task"
        );
    }

    #[test]
    fn pending_public_demand_is_idempotent_retirable_and_clone_local() {
        let root = OneShotSupportProgram;
        let mut scheduler = DeltaScheduler::new();
        let mut stats = ResidualStateStats::default();
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert!(scheduler.begin_public_pull_demand(&mut stats).is_none());
        assert_eq!(stats.delta_positive_support_demand_assigned, 0);

        let mut cloned = scheduler.deep_clone();
        for (index, current) in [&mut scheduler, &mut cloned].into_iter().enumerate() {
            let candidate = value(50 + index as u8);
            let (_, parent, _, _) =
                open_tapped_confirm_with_support(&mut current.registry, [candidate], None, true);
            let (child, _) =
                queue_one_shot_positive_support(current, &root, parent, candidate, false);
            current.park_positive_support_activations(&AHashSet::from_iter([child]));
            let mut branch_stats = ResidualStateStats::default();
            assert!(
                current
                    .assign_public_pull_demand(&mut branch_stats)
                    .is_some(),
                "each clone should observably consume its copied pending demand"
            );
            assert_eq!(branch_stats.delta_positive_support_demand_assigned, 1);
        }

        let mut retired = DeltaScheduler::new();
        assert!(retired.begin_public_pull_demand(&mut stats).is_none());
        retired.retire_unassigned_public_pull_demand();
        let candidate = value(59);
        let (_, parent, _, _) =
            open_tapped_confirm_with_support(&mut retired.registry, [candidate], None, true);
        let (child, _) =
            queue_one_shot_positive_support(&mut retired, &root, parent, candidate, false);
        retired.park_positive_support_activations(&AHashSet::from_iter([child]));
        assert!(
            retired.assign_public_pull_demand(&mut stats).is_none(),
            "retired pending demand must not assign to later parked work"
        );
    }

    #[test]
    fn exact_credit_wakes_support_and_releases_the_directed_exact_lease() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(41);
        let (parent_activation, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));

        let before_d = scheduler
            .registry
            .account_positive_exact_work(parent_activation, 3);
        assert_eq!(
            before_d,
            PositiveExactWorkAccounting {
                paired: true,
                credited: 0,
            },
            "paired exact work must not become retroactive C before D"
        );

        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let spent_d = scheduler
            .registry
            .reserve_positive_support_work(child, 1)
            .expect("assigned D should reserve one Support work unit");
        assert_eq!(
            scheduler
                .registry
                .settle_positive_support_work(spent_d, child, 1),
            1
        );
        let exact_active = queue_exact_confirm_program(
            &mut scheduler,
            support_active.state,
            parent_activation,
            exact_credit,
            true,
            false,
        );

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let stepped = scheduler.step_active_bounded(
            &root,
            &plan,
            exact_active,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(stepped.status, ActiveDeltaStatus::Released);
        assert!(stepped.resume.is_none());
        assert!(!stepped.outcome.has_stable_effect());
        assert!(scheduler.has_active_program(exact_active));
        assert!(scheduler.has_active_program(support_active));
        assert!(!scheduler.has_active_parked_positive_support(support_active));
        assert_eq!(stats.delta_positive_support_exact_paired_examined, 1);
        assert_eq!(stats.delta_positive_support_exact_credited, 1);
        let budget = scheduler
            .registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .support_work;
        assert_eq!(
            (
                budget.demand_minted,
                budget.exact_minted,
                budget.spent,
                budget.available()
            ),
            (1, 1, 1, 1)
        );
        budget.assert_conservation();
    }

    #[test]
    fn same_batch_exact_win_cancels_support_instead_of_waking_refunded_credit() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(42);
        let (parent_activation, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        let exact_active = queue_exact_confirm_program(
            &mut scheduler,
            support_active.state,
            parent_activation,
            exact_credit,
            true,
            true,
        );

        let stepped = scheduler.step_active_bounded(
            &root,
            &plan,
            exact_active,
            1,
            Some(terminal_positive_full()),
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut stats,
        );

        assert_eq!(stepped.status, ActiveDeltaStatus::Yielded);
        assert!(
            stepped.outcome.publication.is_some(),
            "the exact receipt should win publication"
        );
        assert!(!scheduler.registry.is_live(child));
        assert!(!scheduler.has_active_program(support_active));
        assert!(!scheduler.has_active_parked_positive_support(support_active));
        assert_eq!(stats.delta_positive_support_exact_credited, 1);
        assert_eq!(stats.delta_positive_publication_exact_wins, 1);
        assert_eq!(stats.delta_positive_publication_support_wins, 0);
        assert_eq!(stats.delta_positive_support_credit_retired, 2);
        let budget = scheduler
            .registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .support_work;
        assert_eq!(
            (
                budget.demand_minted,
                budget.exact_minted,
                budget.spent,
                budget.retired,
                budget.available()
            ),
            (1, 1, 0, 2, 0)
        );
        budget.assert_conservation();
    }

    #[test]
    fn exact_negative_quiescence_retires_the_started_support_budget() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(44);
        let (parent_activation, parent, exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let exact_active = queue_exact_confirm_program(
            &mut scheduler,
            support_active.state,
            parent_activation,
            exact_credit,
            false,
            false,
        );

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let stepped = scheduler.step_active_bounded(
            &root,
            &plan,
            exact_active,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );

        assert_eq!(stepped.status, ActiveDeltaStatus::Pending);
        assert!(!stepped.outcome.has_stable_effect());
        assert_eq!(
            stats.delta_positive_support_demand_assigned
                + stats.delta_positive_support_exact_credited,
            stats.delta_positive_support_examined + stats.delta_positive_support_credit_retired,
            "closed exact-negative custody must conserve D + C = S + retired"
        );
        let finalizer = stepped
            .resume
            .expect("exact-negative settlement should retain its ordinary finalizer");
        let drained = scheduler.step_active_bounded(
            &root,
            &plan,
            finalizer,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(drained.status, ActiveDeltaStatus::Quiescent);
        assert!(!drained.outcome.has_stable_effect());
        assert!(stable.is_empty());
        assert!(!scheduler.registry.is_live(parent_activation));
        assert!(!scheduler.registry.is_live(child));
        assert!(!scheduler.has_active_program(exact_active));
        assert!(!scheduler.has_active_program(support_active));
        assert!(!scheduler.has_active_parked_positive_support(support_active));
        assert_eq!(stats.delta_positive_support_demand_assigned, 1);
        assert_eq!(stats.delta_positive_support_exact_paired_examined, 1);
        assert_eq!(stats.delta_positive_support_exact_credited, 1);
        assert_eq!(stats.delta_positive_support_credit_retired, 2);
        assert_eq!(stats.delta_positive_support_examined, 0);
        assert!(
            scheduler
                .registry
                .positive_publication_snapshot(parent)
                .is_none(),
            "the drained exact finalizer retained its closed publication ledger"
        );
    }

    #[test]
    fn natural_support_miss_retires_refunded_allowance() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(43);
        let (parent_activation, parent, _exact_credit, _) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        let (child, support_active) = queue_one_shot_positive_support_with_result(
            &mut scheduler,
            &root,
            parent,
            candidate,
            false,
            false,
        );
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let mut stats = ResidualStateStats::default();
        assert_eq!(
            scheduler.begin_public_pull_demand(&mut stats),
            Some(support_active)
        );
        assert_eq!(
            scheduler
                .registry
                .account_positive_exact_work(parent_activation, 3)
                .credited,
            3
        );

        let stepped = scheduler.step_active_bounded(
            &root,
            &plan,
            support_active,
            4,
            Some(terminal_positive_full()),
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut stats,
        );

        assert_eq!(stepped.status, ActiveDeltaStatus::Quiescent);
        assert!(!scheduler.registry.is_live(child));
        assert_eq!(stats.delta_positive_support_examined, 1);
        assert_eq!(stats.delta_positive_support_credit_retired, 3);
        let budget = scheduler
            .registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .support_work;
        assert_eq!(
            (
                budget.demand_minted,
                budget.exact_minted,
                budget.spent,
                budget.retired,
                budget.available()
            ),
            (1, 3, 1, 3, 0)
        );
        budget.assert_conservation();
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum ReceiptProbeMode {
        Linear,
        EffectBoundary,
        ImmediateBoundary,
        AlternatingDispatch,
        AfterChildrenBoundary,
        DuplicateChronology,
        DuplicateDeadTail,
        TransitionThenSourceDead,
        PhysicalBoundary,
        ZeroExaminedChild,
    }

    #[derive(Clone, Copy, Debug)]
    struct ReceiptProbeState(u8);

    #[derive(Clone)]
    struct ReceiptProbeProgram {
        mode: ReceiptProbeMode,
        calls: Arc<AtomicUsize>,
    }

    impl TypedProgramSpec for ReceiptProbeProgram {
        type State = ReceiptProbeState;
        type NoveltyKey = u8;
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Propose(0)).then_some(ProgramRoute {
                key: ProgramKey::new(0),
                variable: 0,
                stratum: ProgramStratum::Fixpoint,
                grouping: ProgramGrouping::PageLocal,
                completion: ProgramCompletion::PageableOnly,
                exposure: ProgramExposure::Production,
            })
        }

        fn dispatch(&self, state: &Self::State) -> DispatchClass {
            let class = match self.mode {
                ReceiptProbeMode::AlternatingDispatch => u32::from(state.0 & 1),
                ReceiptProbeMode::Linear
                | ReceiptProbeMode::EffectBoundary
                | ReceiptProbeMode::ImmediateBoundary
                | ReceiptProbeMode::AfterChildrenBoundary
                | ReceiptProbeMode::DuplicateChronology
                | ReceiptProbeMode::DuplicateDeadTail
                | ReceiptProbeMode::TransitionThenSourceDead
                | ReceiptProbeMode::PhysicalBoundary
                | ReceiptProbeMode::ZeroExaminedChild => 0,
            };
            DispatchClass::new(class)
        }

        fn pacing(&self, _state: &Self::State) -> ProgramPacing {
            ProgramPacing::Activation
        }

        fn progress(&self, state: &Self::State) -> Self::Rank {
            state.0
        }

        fn seed_typed(
            &self,
            _batch: ProgramSeedBatch<'_>,
            _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
        ) {
            panic!("receipt probe is installed through its private runtime")
        }

        fn step_typed(
            &self,
            states: &mut Vec<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), batch.limits.len());
            for (input, state) in states.drain(..).enumerate() {
                self.calls.fetch_add(1, Ordering::Relaxed);
                let input = u32::try_from(input).expect("too many receipt probe inputs");
                match (self.mode, state.0) {
                    (_, 0) => effects.page(0, None),
                    (ReceiptProbeMode::EffectBoundary, 1) => {
                        effects.accept(input, value(1));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::ImmediateBoundary, 1) => {
                        effects.accept(input, value(1));
                        effects.account_transition(1);
                        effects.page(1, Some(TypedResume::Immediate(ReceiptProbeState(0))));
                    }
                    (ReceiptProbeMode::AfterChildrenBoundary, 4) => {
                        effects.fixpoint_child(input, ReceiptProbeState(3), 3, Some(value(4)));
                        effects.account_transition(1);
                        effects.page(1, Some(TypedResume::AfterChildren(ReceiptProbeState(1))));
                    }
                    (ReceiptProbeMode::AfterChildrenBoundary, 3) => {
                        effects.fixpoint_child(input, ReceiptProbeState(2), 2, Some(value(3)));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::AfterChildrenBoundary, 2) => effects.page(0, None),
                    (ReceiptProbeMode::AfterChildrenBoundary, 1) => {
                        effects.accept(input, value(1));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::DuplicateChronology, 4) => {
                        effects.fixpoint_child(input, ReceiptProbeState(3), 3, Some(value(2)));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::DuplicateChronology, 3) => {
                        effects.fixpoint_child(input, ReceiptProbeState(2), 2, Some(value(1)));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::DuplicateChronology, 2) => {
                        effects.fixpoint_child(input, ReceiptProbeState(1), 1, Some(value(2)));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::DuplicateChronology, 1) => {
                        effects.direct(input, value(3));
                        effects.direct(input, value(1));
                        effects.accept(input, value(4));
                        effects.accept(input, value(3));
                        effects.account_transition(4);
                        effects.page(4, None);
                    }
                    (ReceiptProbeMode::DuplicateDeadTail, 2) => {
                        effects.fixpoint_child(input, ReceiptProbeState(1), 1, Some(value(2)));
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::DuplicateDeadTail, 1) => {
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::TransitionThenSourceDead, 2) => {
                        effects.fixpoint_child(input, ReceiptProbeState(1), 1, None);
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::TransitionThenSourceDead, 1) => {
                        effects.account_source(1, 0);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::ZeroExaminedChild, 2) => {
                        effects.fixpoint_child(input, ReceiptProbeState(1), 1, None);
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::ZeroExaminedChild, 1) => {
                        effects.fixpoint_child(input, ReceiptProbeState(0), 0, None);
                        effects.page(0, None);
                    }
                    (
                        ReceiptProbeMode::Linear
                        | ReceiptProbeMode::EffectBoundary
                        | ReceiptProbeMode::ImmediateBoundary
                        | ReceiptProbeMode::AlternatingDispatch
                        | ReceiptProbeMode::PhysicalBoundary,
                        remaining,
                    ) => {
                        let next = remaining - 1;
                        effects.fixpoint_child(
                            input,
                            ReceiptProbeState(next),
                            next,
                            Some(value(remaining)),
                        );
                        effects.account_transition(1);
                        effects.page(1, None);
                    }
                    (ReceiptProbeMode::AfterChildrenBoundary, _) => {
                        panic!("invalid AfterChildren receipt probe state")
                    }
                    (ReceiptProbeMode::DuplicateChronology, _) => {
                        panic!("invalid duplicate-chronology receipt probe state")
                    }
                    (ReceiptProbeMode::DuplicateDeadTail, _) => {
                        panic!("invalid duplicate-dead-tail receipt probe state")
                    }
                    (ReceiptProbeMode::TransitionThenSourceDead, _) => {
                        panic!("invalid transition-source receipt probe state")
                    }
                    (ReceiptProbeMode::ZeroExaminedChild, _) => {
                        panic!("invalid zero-examined receipt probe state")
                    }
                }
            }
        }

        fn try_step_physical(
            &self,
            states: &[Self::State],
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) -> Option<ProgramPhysicalReceipt> {
            if self.mode != ReceiptProbeMode::PhysicalBoundary
                || !matches!(states, [ReceiptProbeState(1)])
            {
                return None;
            }
            assert_eq!(batch.limits.len(), 1);
            assert!(batch.limits[0] >= 1);
            self.calls.fetch_add(1, Ordering::Relaxed);
            effects.fixpoint_child(0, ReceiptProbeState(0), 0, Some(value(1)));
            effects.account_transition(1);
            effects.page(1, None);
            Some(ProgramPhysicalReceipt::new(
                "receipt-probe-physical",
                "placement-boundary",
            ))
        }
    }

    impl Constraint<'static> for ReceiptProbeProgram {
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
            panic!("receipt probe unexpectedly used ordinary proposal")
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

    struct ReceiptProbeHarness {
        root: ReceiptProbeProgram,
        plan: ResidualPlan,
        scheduler: DeltaScheduler,
        stable: Worklist,
        stable_interner: StateInterner,
        stats: ResidualStateStats,
        active: Option<ActiveDeltaContinuation>,
    }

    impl ReceiptProbeHarness {
        fn new(mode: ReceiptProbeMode, initial: u8) -> Self {
            let root = ReceiptProbeProgram {
                mode,
                calls: Arc::new(AtomicUsize::new(0)),
            };
            let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
            let mut scheduler = DeltaScheduler::new();
            let request = ProgramRequest {
                action: ProgramAction::Propose(0),
                bound: VariableSet::new_empty(),
            };
            let spec = ProgramRef::new(&root);
            let route = spec.route(request).expect("receipt probe route declined");
            let state = scheduler.prepare_program(DeltaDesc::leaf(0, 0), route, spec);
            let activation = scheduler.registry.open_program_activation(
                DeltaReducer::StreamProposal,
                candidate_return(Vec::new()),
                None,
                None,
            );
            let credit = scheduler
                .registry
                .issue_credit(activation, CreditKind::Program { join: None });
            let work = insert_engine_program_state(
                &root,
                scheduler
                    .program_runtimes
                    .get_mut(&state)
                    .expect("prepared receipt probe lost its runtime"),
                ProgramActivation(activation.0),
                ReceiptProbeState(initial),
            );
            let active = scheduler
                .file_program_state(
                    state,
                    vec![ProgramTask {
                        activation,
                        credit,
                        work,
                    }],
                )
                .expect("receipt probe filed no active continuation");
            Self {
                root,
                plan,
                scheduler,
                stable: Worklist::new(),
                stable_interner: StateInterner::default(),
                stats: ResidualStateStats::default(),
                active: Some(active),
            }
        }

        fn step(&mut self, width: usize) -> ActiveDeltaStepOutcome {
            let active = self.active.take().expect("receipt probe lost its lease");
            let outcome = self.scheduler.step_active(
                &self.root,
                &self.plan,
                active,
                width,
                &mut self.stable,
                &mut self.stable_interner,
                &mut self.stats,
            );
            self.active = outcome.resume;
            outcome
        }

        fn add_unjoined_sibling(&mut self, initial: u8) {
            let active = self.active.expect("receipt probe lost its active lineage");
            let credit = self
                .scheduler
                .registry
                .issue_credit(active.activation, CreditKind::Program { join: None });
            let work = insert_engine_program_state(
                &self.root,
                self.scheduler
                    .program_runtimes
                    .get_mut(&active.state)
                    .expect("prepared receipt probe lost its runtime"),
                ProgramActivation(active.activation.0),
                ReceiptProbeState(initial),
            );
            let sibling = self
                .scheduler
                .file_program_state(
                    active.state,
                    vec![ProgramTask {
                        activation: active.activation,
                        credit,
                        work,
                    }],
                )
                .expect("unjoined receipt probe sibling was not filed");
            assert_eq!(sibling, active);
        }

        fn preaccept(&mut self, value: RawInline) {
            let active = self.active.expect("receipt probe lost its active lineage");
            assert!(self
                .scheduler
                .registry
                .state
                .activations
                .get_mut(&active.activation)
                .expect("receipt probe lost its activation")
                .accepted
                .insert(value));
        }

        fn step_global(&mut self, width: usize) -> DeltaStepOutcome {
            self.active = None;
            self.scheduler.step_bounded(
                &self.root,
                &self.plan,
                width,
                None,
                &mut self.stable,
                &mut self.stable_interner,
                &mut self.stats,
            )
        }

        fn stable_candidate_values(&self) -> Vec<RawInline> {
            let mut values = Vec::new();
            for bucket in self
                .stable
                .values()
                .flat_map(std::collections::BTreeMap::values)
            {
                let StateBucket::Candidates(batch) = bucket else {
                    panic!("receipt probe returned the wrong stable payload")
                };
                values.extend(batch.candidates.iter().map(|(_, value)| value));
            }
            values
        }
    }

    #[test]
    fn receipt_local_program_chain_spends_one_grant_and_keeps_one_final_credit() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::Linear, 4);
        let outcome = probe.step(4);

        assert_eq!(outcome.status, ActiveDeltaStatus::Yielded);
        let active = outcome.resume.expect("linear chain lost its final child");
        assert!(probe.scheduler.has_active_program(active));
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 4);
        assert_eq!(probe.stats.delta_transition_pages, 4);
        assert_eq!(probe.stats.delta_transition_candidates_examined, 4);
        assert_eq!(probe.stats.delta_transition_cohorts, 4);
        assert_eq!(probe.stats.max_delta_transition_cohort, 1);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 3);
        assert_eq!(probe.stats.delta_program_receipt_local_refiles_avoided, 3);
        assert_eq!(probe.stats.max_delta_program_receipt_local_chain, 4);

        let mut cloned = probe.scheduler.clone();
        assert!(cloned.has_active_program(active));
        drop(probe.scheduler);
        assert!(cloned.has_active_program(active));
        let finished = cloned.step_active(
            &probe.root,
            &probe.plan,
            active,
            1,
            &mut probe.stable,
            &mut probe.stable_interner,
            &mut probe.stats,
        );
        assert_eq!(finished.status, ActiveDeltaStatus::Quiescent);
        assert!(finished.resume.is_none());
        assert!(cloned.is_empty());
    }

    #[test]
    fn receipt_local_program_chain_commits_the_first_outward_effect_as_its_boundary() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::EffectBoundary, 2);
        let outcome = probe.step(4);

        assert_eq!(outcome.status, ActiveDeltaStatus::Yielded);
        assert!(outcome.resume.is_none());
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 2);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 1);
        assert_eq!(probe.stats.max_delta_program_receipt_local_chain, 2);
        assert!(probe.scheduler.is_empty());
        assert!(!probe.stable.is_empty());
    }

    #[test]
    fn receipt_local_program_chain_refiles_immediate_and_cross_dispatch_boundaries() {
        let mut immediate = ReceiptProbeHarness::new(ReceiptProbeMode::ImmediateBoundary, 2);
        let immediate_outcome = immediate.step(4);
        assert_eq!(immediate.stats.delta_program_receipt_local_fused_steps, 1);
        assert!(immediate_outcome.resume.is_some());
        assert!(immediate
            .scheduler
            .has_active_program(immediate_outcome.resume.unwrap()));

        let mut cross_dispatch = ReceiptProbeHarness::new(ReceiptProbeMode::AlternatingDispatch, 4);
        let cross_outcome = cross_dispatch.step(4);
        assert_eq!(
            cross_dispatch.stats.delta_program_receipt_local_fused_steps,
            0
        );
        assert_eq!(cross_dispatch.root.calls.load(Ordering::Relaxed), 1);
        assert!(cross_outcome.resume.is_some());
    }

    #[test]
    fn receipt_local_program_chain_leaves_global_cohorts_unchanged() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::Linear, 4);
        let outcome = probe.step_global(4);

        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 1);
        assert!(outcome.has_stable_effect());
        assert!(!probe.scheduler.is_empty());
    }

    #[test]
    fn receipt_local_program_chain_requires_the_only_live_unjoined_credit() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::Linear, 4);
        probe.add_unjoined_sibling(2);
        let outcome = probe.step(1);

        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 1);
        assert_eq!(outcome.status, ActiveDeltaStatus::Yielded);
        assert!(outcome.resume.is_some());
        assert!(probe.scheduler.has_active_program(outcome.resume.unwrap()));
    }

    #[test]
    fn receipt_local_program_chain_never_crosses_after_children_join() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::AfterChildrenBoundary, 4);
        let first = probe.step(4);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(first.status, ActiveDeltaStatus::Yielded);
        assert!(first.resume.is_some());

        let child = probe.step(4);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(child.status, ActiveDeltaStatus::Yielded);
        assert!(child.resume.is_some());

        let join = probe.step(4);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(join.status, ActiveDeltaStatus::Pending);
        assert!(join.resume.is_some());

        let resume = probe.step(4);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 0);
        assert_eq!(resume.status, ActiveDeltaStatus::Yielded);
        assert!(resume.resume.is_none());
        assert!(probe.scheduler.is_empty());
    }

    #[test]
    fn receipt_local_program_chain_preserves_observation_order_set_admission_and_feedback() {
        let mut fused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateChronology, 4);
        let fused_outcome = fused.step(8);
        assert_eq!(fused_outcome.status, ActiveDeltaStatus::Yielded);
        assert!(fused_outcome.resume.is_none());
        assert!(fused.scheduler.is_empty());

        let mut fused_feedback =
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), Search::Done);
        fused_feedback.width = 8;
        fused_feedback.cap = 64;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateChronology, 4);
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), Search::Done);
        unfused_feedback.width = 8;
        unfused_feedback.cap = 64;
        for _ in 0..8 {
            if unfused.scheduler.is_empty() {
                break;
            }
            let outcome = unfused.step_global(8);
            unfused_feedback.account_delta_feedback(&outcome);
        }
        assert!(unfused.scheduler.is_empty());

        let expected = [value(2), value(1), value(3), value(4)];
        assert_eq!(fused.stable_candidate_values(), expected);
        assert_eq!(unfused.stable_candidate_values(), expected);
        assert_eq!(fused.stats.candidates_proposed, 7);
        assert_eq!(unfused.stats.candidates_proposed, 7);
        assert_eq!(fused.stats.delta_transition_pages, 4);
        assert_eq!(unfused.stats.delta_transition_pages, 4);
        assert_eq!(fused.stats.delta_transition_candidates_examined, 7);
        assert_eq!(unfused.stats.delta_transition_candidates_examined, 7);
        assert_eq!(fused.stats.delta_transition_cohorts, 4);
        assert_eq!(unfused.stats.delta_transition_cohorts, 4);
        assert_eq!(fused_feedback.width, unfused_feedback.width);
        assert_eq!(fused_feedback.width, 8);
        assert_eq!(
            fused_feedback.stats.delta_transition_negative_steps,
            unfused_feedback.stats.delta_transition_negative_steps
        );
        assert_eq!(
            fused_feedback.stats.delta_source_negative_steps,
            unfused_feedback.stats.delta_source_negative_steps
        );
    }

    #[test]
    fn receipt_local_program_chain_clone_preserves_committed_prefix_bag_and_chronology() {
        let mut original = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateChronology, 4);
        let prefix = original.step(3);
        let active = prefix
            .resume
            .expect("the grant boundary must leave the final child live");

        assert_eq!(prefix.status, ActiveDeltaStatus::Yielded);
        assert_eq!(original.stable_candidate_values(), [value(2), value(1)]);
        assert_eq!(original.root.calls.load(Ordering::Relaxed), 3);
        assert_eq!(original.stats.delta_program_receipt_local_fused_steps, 2);
        assert_eq!(
            original.stats.delta_program_receipt_local_refiles_avoided,
            2
        );
        assert_eq!(original.stats.max_delta_program_receipt_local_chain, 3);
        let tasks = &original.scheduler.program_worklist[&active.state].tasks;
        assert_eq!(tasks.len(), 1);
        assert!(original
            .scheduler
            .registry
            .program_credit_is_unjoined_unique(&tasks[0].credit));

        let mut cloned = ReceiptProbeHarness {
            root: original.root.clone(),
            plan: original.plan.clone(),
            scheduler: original.scheduler.clone(),
            stable: original.stable.clone(),
            stable_interner: original.stable_interner.clone(),
            stats: original.stats.clone(),
            active: original.active,
        };
        let cloned_tasks = &cloned.scheduler.program_worklist[&active.state].tasks;
        assert_eq!(cloned_tasks.len(), 1);
        assert!(cloned
            .scheduler
            .registry
            .program_credit_is_unjoined_unique(&cloned_tasks[0].credit));

        let original_tail = original.step(8);
        let cloned_tail = cloned.step(8);
        assert_eq!(original_tail.status, ActiveDeltaStatus::Yielded);
        assert_eq!(cloned_tail.status, ActiveDeltaStatus::Yielded);
        assert!(original_tail.resume.is_none());
        assert!(cloned_tail.resume.is_none());
        assert!(original.scheduler.is_empty());
        assert!(cloned.scheduler.is_empty());

        let expected = [value(2), value(1), value(3), value(4)];
        assert_eq!(original.stable_candidate_values(), expected);
        assert_eq!(cloned.stable_candidate_values(), expected);
        assert_eq!(original.stats, cloned.stats);
        assert_eq!(original.stats.candidates_proposed, 7);
        assert_eq!(original.stats.delta_transition_candidates_examined, 7);
    }

    #[test]
    fn receipt_local_program_chain_stops_at_a_physical_placement_boundary() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::PhysicalBoundary, 3);
        let placement = probe.step(4);
        let active = placement
            .resume
            .expect("the placement page's child must remain scheduled");

        assert_eq!(placement.status, ActiveDeltaStatus::Yielded);
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 3);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 2);
        assert_eq!(probe.stats.delta_program_receipt_local_refiles_avoided, 2);
        assert_eq!(probe.stats.max_delta_program_receipt_local_chain, 3);
        assert_eq!(probe.stats.delta_program_physical_cohorts, 1);
        assert_eq!(probe.stats.delta_program_physical_rows, 1);
        assert_eq!(probe.stats.delta_program_physical_granted_work, 2);
        assert_eq!(probe.stats.max_delta_program_physical_cohort, 1);
        assert_eq!(probe.stats.max_delta_program_physical_granted_work, 2);
        assert_eq!(probe.stats.delta_transition_pages, 3);
        assert_eq!(probe.stats.delta_transition_cohorts, 3);
        assert_eq!(probe.stats.delta_transition_candidates_examined, 3);
        assert!(probe.scheduler.has_active_program(active));
        let tasks = &probe.scheduler.program_worklist[&active.state].tasks;
        assert_eq!(tasks.len(), 1);
        assert!(probe
            .scheduler
            .registry
            .program_credit_is_unjoined_unique(&tasks[0].credit));
        assert_eq!(
            probe.stable_candidate_values(),
            [value(3), value(2), value(1)]
        );

        let finished = probe.step(1);
        assert_eq!(finished.status, ActiveDeltaStatus::Quiescent);
        assert!(finished.resume.is_none());
        assert!(probe.scheduler.is_empty());
        assert_eq!(probe.root.calls.load(Ordering::Relaxed), 4);
        assert_eq!(probe.stats.delta_program_receipt_local_fused_steps, 2);
        assert_eq!(probe.stats.delta_program_physical_cohorts, 1);
        assert_eq!(probe.stats.delta_program_physical_granted_work, 2);
        assert_eq!(probe.stats.delta_transition_pages, 3);
        assert_eq!(probe.stats.delta_transition_candidates_examined, 3);
        assert_eq!(probe.stats.candidates_proposed, 3);
        assert_eq!(
            probe.stable_candidate_values(),
            [value(3), value(2), value(1)]
        );
    }

    #[test]
    fn receipt_local_program_chain_keeps_a_duplicate_prefix_final_dead_page_visible() {
        let mut fused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateDeadTail, 2);
        fused.preaccept(value(2));
        let fused_outcome = fused.step(2);
        assert_eq!(fused.stats.delta_program_receipt_local_fused_steps, 1);
        assert_eq!(fused_outcome.outcome.dead_pages, 1);
        assert_eq!(fused_outcome.outcome.transition_dead_pages, 1);

        let mut fused_feedback =
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), Search::Done);
        fused_feedback.width = 2;
        fused_feedback.cap = 64;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateDeadTail, 2);
        unfused.preaccept(value(2));
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), Search::Done);
        unfused_feedback.width = 2;
        unfused_feedback.cap = 64;
        for _ in 0..4 {
            if unfused.scheduler.is_empty() {
                break;
            }
            let outcome = unfused.step_global(2);
            unfused_feedback.account_delta_feedback(&outcome);
        }
        assert!(unfused.scheduler.is_empty());

        assert_eq!(fused.stats.delta_transition_dead_pages, 1);
        assert_eq!(unfused.stats.delta_transition_dead_pages, 1);
        assert_eq!(fused.stats.candidates_proposed, 1);
        assert_eq!(unfused.stats.candidates_proposed, 1);
        assert_eq!(fused_feedback.width, unfused_feedback.width);
        assert_eq!(fused_feedback.width, 4);
        assert_eq!(
            fused_feedback.stats.delta_transition_negative_steps,
            unfused_feedback.stats.delta_transition_negative_steps
        );
        assert_eq!(fused_feedback.stats.delta_transition_negative_steps, 1);
    }

    #[test]
    fn receipt_local_program_chain_classifies_deadness_from_the_final_page() {
        let mut fused = ReceiptProbeHarness::new(ReceiptProbeMode::TransitionThenSourceDead, 2);
        let fused_outcome = fused.step(2);
        assert_eq!(fused.stats.delta_program_receipt_local_fused_steps, 1);
        assert_eq!(fused_outcome.outcome.dead_pages, 1);
        assert_eq!(fused_outcome.outcome.source_dead_pages, 1);
        assert_eq!(fused_outcome.outcome.transition_dead_pages, 0);

        let mut fused_feedback =
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), Search::Done);
        fused_feedback.width = 2;
        fused_feedback.cap = 64;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::TransitionThenSourceDead, 2);
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), Search::Done);
        unfused_feedback.width = 2;
        unfused_feedback.cap = 64;
        for _ in 0..4 {
            if unfused.scheduler.is_empty() {
                break;
            }
            let outcome = unfused.step_global(2);
            unfused_feedback.account_delta_feedback(&outcome);
        }
        assert!(unfused.scheduler.is_empty());

        assert_eq!(fused.stats.delta_source_dead_pages, 1);
        assert_eq!(unfused.stats.delta_source_dead_pages, 1);
        assert_eq!(fused.stats.delta_transition_dead_pages, 0);
        assert_eq!(unfused.stats.delta_transition_dead_pages, 0);
        assert_eq!(fused.stats.delta_source_pages, 1);
        assert_eq!(unfused.stats.delta_source_pages, 1);
        assert_eq!(fused.stats.delta_transition_pages, 1);
        assert_eq!(unfused.stats.delta_transition_pages, 1);
        assert_eq!(fused_feedback.width, unfused_feedback.width);
        assert_eq!(fused_feedback.width, 4);
        assert_eq!(
            fused_feedback.stats.delta_source_negative_steps,
            unfused_feedback.stats.delta_source_negative_steps
        );
        assert_eq!(fused_feedback.stats.delta_source_negative_steps, 1);
    }

    #[test]
    #[should_panic(
        expected = "typed program emitted more raw effects than its examined-work receipt"
    )]
    fn receipt_local_program_chain_does_not_mask_a_zero_examined_child_page() {
        let mut probe = ReceiptProbeHarness::new(ReceiptProbeMode::ZeroExaminedChild, 2);
        let _ = probe.step(8);
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
        let RegistrySettlement::ProposalMaterializer(seed) = registry.settle_quiescence(proof)
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

    #[derive(Clone, Copy)]
    struct PositiveCertificateLeaf {
        variable: VariableId,
        page_local: bool,
    }

    impl Constraint<'static> for PositiveCertificateLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
        }

        fn fixed_denotation(&self) -> bool {
            true
        }

        fn estimate(
            &self,
            variable: VariableId,
            view: &RowsView<'_>,
            out: &mut EstimateSink<'_>,
        ) -> bool {
            if variable != self.variable {
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

        fn residual_confirm_is_page_local(&self) -> bool {
            self.page_local
        }
    }

    fn positive_candidate_return(
        parent: Vec<RawInline>,
        bound: VariableSet,
        variable: VariableId,
        relevant: ChildSet,
        checked: ChildSet,
        set_admit_result: bool,
    ) -> DeltaReturn {
        DeltaReturn::Stable {
            desc: StateDesc {
                bound,
                phase: ResidualPhase::Candidate {
                    variable,
                    relevant,
                    checked,
                },
            },
            parent: parent.into_boxed_slice(),
            set_admit_result,
        }
    }

    fn terminal_positive_return(parent: Vec<RawInline>) -> DeltaReturn {
        let relevant = ChildSet::empty(1).with_inserted(0);
        positive_candidate_return(
            parent,
            VariableSet::new_empty(),
            0,
            relevant.clone(),
            relevant,
            true,
        )
    }

    fn terminal_positive_full() -> VariableSet {
        VariableSet::new_singleton(0)
    }

    fn commit_terminal_positive(
        registry: &mut ProducerRegistry,
        witness: PositiveSupportWitness,
    ) -> bool {
        registry
            .commit_positive_publication(witness, Some(terminal_positive_full()))
            .is_some()
    }

    fn positive_test_work(slot: u32) -> ProgramWork {
        ProgramWork {
            handle: ProgramWorkHandle::test(slot),
            dispatch: DispatchClass::new(0),
            pacing: ProgramPacing::Activation,
        }
    }

    fn open_positive_support_credit(
        registry: &mut ProducerRegistry,
        parent: PositiveConfirmParentId,
        occurrence: usize,
        value: RawInline,
        support_variables: VariableSet,
        terminal_full: Option<VariableSet>,
    ) -> (ActivationId, ProducerCredit) {
        let child = registry
            .open_positive_support_activation(
                parent,
                occurrence,
                value,
                support_variables,
                terminal_full,
            )
            .expect("valid positive Support child should open");
        let mut installed = registry.install_program_roots(
            child,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(0),
                accepted: None,
            }],
        );
        assert!(installed.initial_accepted.is_empty());
        assert!(installed.quiescence.is_none());
        let (_, credit) = installed.roots.pop().expect("one positive Support root");
        (child, credit)
    }

    fn replace_positive_support_credit(
        registry: &mut ProducerRegistry,
        credit: ProducerCredit,
        accepted: Option<RawInline>,
        reported_support: bool,
    ) -> (PositiveSupportWitness, QuiescenceProof) {
        let mut outcome = registry.replace_program(
            credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            accepted,
            std::iter::empty(),
            reported_support,
            false,
            false,
            None,
        );
        assert!(outcome.positive_support_reducer);
        assert_eq!(outcome.raw_proposal_occurrences, 0);
        assert!(
            outcome.accepted.is_empty(),
            "positive Support must not leak proposal effects"
        );
        assert!(outcome.scheduled.is_empty());
        (
            *outcome
                .positive_support
                .take()
                .expect("first real positive success should mint a witness"),
            outcome
                .quiescence
                .expect("the sole positive Support credit should quiesce"),
        )
    }

    fn terminal_positive_witness(
        registry: &mut ProducerRegistry,
        parent: PositiveConfirmParentId,
        occurrence: usize,
        value: RawInline,
        reported_support: bool,
    ) -> (ActivationId, PositiveSupportWitness, QuiescenceProof) {
        let (child, credit) = open_positive_support_credit(
            registry,
            parent,
            occurrence,
            value,
            VariableSet::new_singleton(0),
            Some(terminal_positive_full()),
        );
        let (witness, proof) = replace_positive_support_credit(
            registry,
            credit,
            (!reported_support).then_some(value),
            reported_support,
        );
        (child, witness, proof)
    }

    fn terminal_positive_certificate() -> PositivePublicationCertificate {
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let checked = ChildSet::empty(plan.len());
        let previous = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Confirm {
                variable: 0,
                relevant: relevant.clone(),
                checked: checked.clone(),
                confirmer: 0,
            },
        };
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked: checked.with_inserted(0),
            },
        };
        PositivePublicationCertificate::from_confirm_transition(
            &previous,
            &successor,
            root.variables(),
            &plan,
            &FormulaPcInterner::default(),
        )
    }

    fn chunk_positive_fixture() -> (
        ResidualPlan,
        StateDesc,
        PositivePublicationCertificate,
        VariableSet,
    ) {
        let root = IntersectionConstraint::new(vec![
            PositiveCertificateLeaf {
                variable: 0,
                page_local: false,
            },
            PositiveCertificateLeaf {
                variable: 0,
                page_local: true,
            },
            PositiveCertificateLeaf {
                variable: 1,
                page_local: false,
            },
        ]);
        let plan = ResidualPlan::compile(&root);
        let relevant = ChildSet::empty(plan.len())
            .with_inserted(0)
            .with_inserted(1);
        let checked = ChildSet::empty(plan.len());
        let previous = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Confirm {
                variable: 0,
                relevant: relevant.clone(),
                checked: checked.clone(),
                confirmer: 0,
            },
        };
        let successor = StateDesc {
            bound: VariableSet::new_empty(),
            phase: ResidualPhase::Candidate {
                variable: 0,
                relevant,
                checked: checked.with_inserted(0),
            },
        };
        let full = root.variables();
        let certificate = PositivePublicationCertificate::from_confirm_transition(
            &previous,
            &successor,
            full,
            &plan,
            &FormulaPcInterner::default(),
        );
        assert_eq!(
            certificate.continuation,
            ContinuationPublicationReceipt::ChunkHomomorphic
        );
        assert!(certificate.crosses_set_boundary);
        (plan, successor, certificate, full)
    }

    fn open_positive_confirm(
        registry: &mut ProducerRegistry,
        values: impl IntoIterator<Item = RawInline>,
        certificate: PositivePublicationCertificate,
    ) -> (ActivationId, PositiveConfirmParentId) {
        open_positive_confirm_with_return(
            registry,
            values,
            certificate,
            terminal_positive_return(Vec::new()),
        )
    }

    fn open_positive_confirm_with_return(
        registry: &mut ProducerRegistry,
        values: impl IntoIterator<Item = RawInline>,
        certificate: PositivePublicationCertificate,
        return_to: DeltaReturn,
    ) -> (ActivationId, PositiveConfirmParentId) {
        let original = shared_one_parent_candidates(values.into_iter().collect());
        let activation = registry.open_program_activation(
            DeltaReducer::Confirm { original },
            return_to,
            None,
            None,
        );
        let parent = registry
            .open_exact_and_support_publication(activation, StateId(17), certificate)
            .expect("Confirm activation should register a semantic parent");
        (activation, parent)
    }

    fn open_tapped_confirm(
        registry: &mut ProducerRegistry,
        values: impl IntoIterator<Item = RawInline>,
        initial_accepted: Option<RawInline>,
    ) -> (
        ActivationId,
        PositiveConfirmParentId,
        ProducerCredit,
        Vec<RawInline>,
    ) {
        open_tapped_confirm_with_support(registry, values, initial_accepted, false)
    }

    fn open_tapped_confirm_with_support(
        registry: &mut ProducerRegistry,
        values: impl IntoIterator<Item = RawInline>,
        initial_accepted: Option<RawInline>,
        support_authorized: bool,
    ) -> (
        ActivationId,
        PositiveConfirmParentId,
        ProducerCredit,
        Vec<RawInline>,
    ) {
        let original = shared_one_parent_candidates(values.into_iter().collect());
        let activation = registry.open_program_activation(
            DeltaReducer::Confirm { original },
            terminal_positive_return(Vec::new()),
            None,
            None,
        );
        let mut installed = registry.install_program_roots(
            activation,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(0),
                accepted: initial_accepted,
            }],
        );
        let parent = if support_authorized {
            registry.open_exact_and_support_publication(
                activation,
                StateId(17),
                terminal_positive_certificate(),
            )
        } else {
            registry.open_exact_only_publication(
                activation,
                StateId(17),
                terminal_positive_certificate(),
            )
        }
        .expect("Confirm activation should register an exact-tap parent");
        let (_, credit) = installed
            .roots
            .pop()
            .expect("one exact Confirm Program root");
        (activation, parent, credit, installed.initial_accepted)
    }

    fn terminal_positive_commit_fixture() -> (
        ProducerRegistry,
        ActivationId,
        PositiveConfirmParentId,
        ActivationId,
        PositiveSupportWitness,
    ) {
        let candidate = value(6);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
        let (child, witness, _) =
            terminal_positive_witness(&mut registry, parent, 0, candidate, false);
        (registry, activation, parent, child, witness)
    }

    fn assert_terminal_positive_preflight_rejected(
        direct_terminal_full: Option<VariableSet>,
        mutate: impl FnOnce(&mut ProducerRegistry, ActivationId),
    ) {
        let (mut registry, activation, parent, _child, witness) =
            terminal_positive_commit_fixture();
        mutate(&mut registry, activation);
        assert!(registry
            .commit_positive_publication(witness, direct_terminal_full)
            .is_none());
        assert!(
            registry
                .positive_publication_snapshot(parent)
                .expect("eligible parent retained its ledger")
                .published
                .is_empty(),
            "a rejected preflight mutated P"
        );
    }

    fn quiesce_confirm_with_accepted(
        registry: &mut ProducerRegistry,
        activation: ActivationId,
        accepted: impl IntoIterator<Item = RawInline>,
    ) -> QuiescenceProof {
        let activation_state = registry
            .state
            .activations
            .get_mut(&activation)
            .expect("test Confirm activation disappeared");
        assert!(matches!(
            &activation_state.reducer,
            DeltaReducer::Confirm { .. }
        ));
        assert!(activation_state.live.is_empty());
        activation_state.accepted = accepted.into_iter().collect();
        activation_state.status = ActivationStatus::Quiescent;
        QuiescenceProof { activation }
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
                input: CandidatePayload::Values(vec![value(2), value(2), value(1), value(3)]),
                // The test deliberately stops before EOF; this exact saved
                // PC must remain opaque to every intermediate page.
                continuation: FormulaReducerContinuation::Complete(FormulaPcId(u32::MAX)),
            })],
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let active = seeded
            .active
            .expect("nonempty admission opened one Program");
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
        assert!(Arc::ptr_eq(&output_root(&scheduler), &output_root(&cloned),));

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
            std::iter::empty(),
            [value(3), value(2)],
            [value(3), value(3), value(1)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.accepted.as_slice(), [value(3), value(2)]);
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
            std::iter::empty(),
            [value(4), value(5), value(5)],
            [value(2)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(last.accepted.as_slice(), [value(5)]);
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
                ActiveDeltaStatus::Parked => {
                    panic!("non-Support materializer entered the parked hedge lane")
                }
                ActiveDeltaStatus::Released => {
                    panic!("non-Confirm materializer released its directed lease")
                }
            }
        }
        assert!(
            yielded.is_some(),
            "unit-grant materializer failed to terminate"
        );
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
            [1, 2, 2, 3, 3, 3, 4, 5].map(value).map(|value| (0, value))
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
        assert_eq!(
            emitted.outcome.retargeted.get(&old_activation),
            Some(&fresh)
        );
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
                OneShotSupportState {
                    keep_cleanup_live,
                    accept_candidate: false,
                    report_support: true,
                    examined: 1,
                },
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
            assert_eq!(
                scheduler.registry.is_live(old_activation),
                keep_cleanup_live
            );
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
    fn positive_support_first_success_discards_its_queued_continuation_affinely() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(6);
        let (_parent_activation, parent, _exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, initially_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, true);
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        scheduler.assert_runnable_positive_support_task_index();
        assert!(scheduler.runnable_positive_support_tasks.is_empty());
        let active = scheduler
            .begin_public_pull_demand(&mut stats)
            .expect("public demand should wake the parked Support child");
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);
        assert_eq!(active, initially_active);
        let state = active.state;
        let stepped = scheduler.step_active_bounded(
            &root,
            &plan,
            active,
            1,
            Some(terminal_positive_full()),
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        scheduler.assert_runnable_positive_support_task_index();
        assert!(scheduler.runnable_positive_support_tasks.is_empty());

        assert_eq!(stepped.status, ActiveDeltaStatus::Yielded);
        assert!(stepped.resume.is_none());
        assert_eq!(stepped.outcome.completed_activation_ids, [child]);
        assert_eq!(
            stepped
                .outcome
                .publication
                .expect("the winning Support receipt should publish")
                .rows
                .rows,
            [candidate]
        );
        assert!(
            !scheduler.registry.is_live(child),
            "the winning hedge retained its cleanup continuation"
        );
        assert!(
            !scheduler.program_worklist.contains_key(&state),
            "the discarded continuation remained runnable"
        );
        assert_eq!(
            scheduler
                .registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate])
        );
        assert!(stable.is_empty());
        assert_eq!(stats.delta_positive_support_demand_assigned, 1);
        assert_eq!(stats.delta_positive_support_examined, 1);
        assert_eq!(stats.delta_positive_publication_support_wins, 1);
    }

    #[test]
    fn exact_quiescence_retires_queued_support_but_preserves_exact_finalization() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(5);
        let (parent_activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        assert!(!scheduler.has_active_program(support_active));
        assert!(scheduler.has_active_parked_positive_support(support_active));

        let exact_page = scheduler.registry.replace_program(
            exact_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert!(exact_page.positive_confirm.is_none());
        let targets: AHashSet<_> = scheduler
            .registry
            .positive_support_children(parent)
            .into_iter()
            .collect();
        assert_eq!(targets, AHashSet::from_iter([child]));

        let DeltaSettlement::Retargeted(finalizer) = scheduler.settle_quiescence(
            exact_page
                .quiescence
                .expect("the exact miss should quiesce"),
        ) else {
            panic!("the exact Confirm must retain its pageable finalizer")
        };
        let (completed, _) = scheduler.retire_positive_support_activations(&root, &plan, &targets);

        assert_eq!(completed, [child]);
        assert!(
            !scheduler.registry.is_live(child),
            "exact quiescence left its Support hedge live"
        );
        assert!(
            !scheduler.has_active_program(support_active),
            "exact quiescence left the Support hedge runnable"
        );
        assert!(
            !scheduler.has_active_parked_positive_support(support_active),
            "exact quiescence left the Support hedge parked"
        );
        assert_eq!(finalizer.activation, parent_activation);
        assert!(
            scheduler.registry.is_live(parent_activation)
                && scheduler.has_active_program(finalizer),
            "Support retirement cancelled the exact Confirm finalizer"
        );
    }

    #[test]
    fn exact_positive_win_retires_queued_support_without_cancelling_exact() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(4);
        let (parent_activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);
        scheduler.assert_runnable_positive_support_task_index();
        assert_eq!(scheduler.runnable_positive_support_tasks[&parent], 1);

        let mut exact_page = scheduler.registry.replace_program(
            exact_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [candidate],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        let witness = *exact_page
            .positive_confirm
            .take()
            .expect("the exact replacement should mint its authoritative witness");
        let grant = scheduler
            .registry
            .commit_confirm_positive_publication(witness, Some(terminal_positive_full()))
            .expect("the exact replacement should win the parent/value SET");
        let targets: AHashSet<_> = scheduler
            .registry
            .positive_support_children(parent)
            .into_iter()
            .collect();
        let (completed, _) = scheduler.retire_positive_support_activations(&root, &plan, &targets);
        scheduler.assert_runnable_positive_support_task_index();
        assert!(scheduler.runnable_positive_support_tasks.is_empty());

        assert_eq!(completed, [child]);
        assert!(
            !scheduler.has_active_program(support_active),
            "the exact winner left its Support hedge runnable"
        );
        assert!(
            scheduler.registry.is_live(parent_activation),
            "Support retirement cancelled the exact Confirm parent"
        );
        let released = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut ResidualStateStats::default(),
        );
        assert_eq!(
            released
                .publication
                .expect("the exact winner should publish")
                .rows
                .rows,
            [candidate]
        );
        let RegistrySettlement::ConfirmFinalizer(seed) = scheduler.registry.settle_quiescence(
            exact_page
                .quiescence
                .expect("the exact page should quiesce"),
        ) else {
            panic!("the exact parent must retain finalization ownership")
        };
        assert_eq!(seed.activation, parent_activation);
        assert!(
            seed.state.accepted.is_empty(),
            "the exact-published value must be removed from late G"
        );
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
    fn exact_confirm_replacement_publishes_b0_once_and_finalizes_g_minus_p() {
        let first = value(41);
        let later = value(42);
        let mut registry = ProducerRegistry::new();
        let (activation, parent, credit, initial) =
            open_tapped_confirm(&mut registry, [first, later, first], None);
        assert!(initial.is_empty());
        assert!(
            registry
                .open_positive_support_activation(
                    parent,
                    0,
                    first,
                    VariableSet::new_singleton(0),
                    Some(terminal_positive_full()),
                )
                .is_none(),
            "an exact-tap ledger must not lend authority to a Support child"
        );

        let mut first_page = registry.replace_program(
            credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [first],
            std::iter::empty(),
            false,
            false,
            false,
            Some(ProgramResume::Immediate(positive_test_work(1))),
        );
        assert!(!first_page.positive_support_reducer);
        assert!(first_page.positive_support.is_none());
        assert_eq!(first_page.accepted.as_slice(), [first]);
        let witness = *first_page
            .positive_confirm
            .take()
            .expect("a real exact replacement accepting B[0] must mint one witness");
        let (_, _, continuation_credit) = first_page
            .scheduled
            .pop()
            .expect("the exact Confirm continuation should remain live");
        assert!(first_page.quiescence.is_none());

        let grant = registry
            .commit_confirm_positive_publication(witness, Some(terminal_positive_full()))
            .expect("the exact B[0] witness should win its parent SET ledger");
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let released = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut ResidualStateStats::default(),
        );
        assert_eq!(
            released
                .publication
                .expect("Terminal exact tap must publish")
                .rows
                .rows,
            [first]
        );
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([first])
        );

        let second_page = registry.replace_program(
            continuation_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [first, later],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert!(
            second_page.positive_confirm.is_none(),
            "an already accepted B[0] must not mint a second receipt"
        );
        assert_eq!(second_page.accepted.as_slice(), [later]);
        let proof = second_page
            .quiescence
            .expect("the final exact page should quiesce");
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(proof) else {
            panic!("nonempty B must enter the exact Confirm finalizer")
        };
        assert!(!seed.state.accepted.contains(&first));
        assert!(seed.state.accepted.contains(&later));
        assert_eq!(seed.activation, activation);
    }

    #[test]
    fn exact_and_support_witnesses_race_on_one_source_distinct_set_ledger() {
        let candidate = value(43);
        let mut registry = ProducerRegistry::new();
        let (activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (support_child, support_witness, support_proof) =
            terminal_positive_witness(&mut registry, parent, 0, candidate, true);

        let mut exact_page = registry.replace_program(
            exact_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [candidate],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        let exact_witness = *exact_page
            .positive_confirm
            .take()
            .expect("the authoritative exact receipt should mint its own witness");
        let grant = registry
            .commit_confirm_positive_publication(exact_witness, Some(terminal_positive_full()))
            .expect("the exact source should win the shared value claim");
        assert!(
            registry
                .commit_positive_publication(support_witness, Some(terminal_positive_full()),)
                .is_none(),
            "a valid later Support witness must not replay the exact winner"
        );
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate])
        );

        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let released = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut ResidualStateStats::default(),
        );
        assert_eq!(
            released
                .publication
                .expect("the exact winner must retain Terminal authority")
                .rows
                .rows,
            [candidate]
        );

        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(support_proof)
        else {
            panic!("the losing Support child must retire as physical cleanup")
        };
        assert_eq!(completed.activation, support_child);
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(
            exact_page
                .quiescence
                .expect("the exact page should quiesce"),
        ) else {
            panic!("the exact parent must retain completeness ownership")
        };
        assert_eq!(seed.activation, activation);
        assert!(
            seed.state.accepted.is_empty(),
            "G minus the exact-published candidate must be empty"
        );
    }

    #[test]
    fn support_wins_first_then_exact_settles_the_same_g_minus_p() {
        let candidate = value(44);
        let mut registry = ProducerRegistry::new();
        let (activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut registry, [candidate, candidate], None, true);
        assert!(initial.is_empty());
        let (support_child, support_witness, support_proof) =
            terminal_positive_witness(&mut registry, parent, 0, candidate, true);

        let grant = registry
            .commit_positive_publication(support_witness, Some(terminal_positive_full()))
            .expect("the Support source should win the shared value claim");
        let exact_page = registry.replace_program(
            exact_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [candidate],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert!(
            exact_page.positive_confirm.is_none(),
            "a later exact acceptance must observe the existing SET winner rather than mint replay authority"
        );
        assert_eq!(exact_page.accepted.as_slice(), [candidate]);
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate])
        );

        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let released = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut ResidualStateStats::default(),
        );
        assert_eq!(
            released
                .publication
                .expect("the Support winner must publish one Terminal row")
                .rows
                .rows,
            [candidate]
        );

        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(support_proof)
        else {
            panic!("the winning Support child must still retire as physical cleanup")
        };
        assert_eq!(completed.activation, support_child);
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(
            exact_page
                .quiescence
                .expect("the exact page should quiesce"),
        ) else {
            panic!("the exact parent must retain completeness ownership")
        };
        assert_eq!(seed.activation, activation);
        assert!(
            seed.state.accepted.is_empty(),
            "G minus the Support-published raw value must remove every duplicate occurrence"
        );
    }

    #[test]
    fn exact_confirm_tap_requires_b0_to_be_newly_accepted_by_a_replacement() {
        let first = value(45);
        let later = value(46);

        let mut wrong_value = ProducerRegistry::new();
        let (_, parent, credit, _) = open_tapped_confirm(&mut wrong_value, [first, later], None);
        let wrong_page = wrong_value.replace_program(
            credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [later],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert!(wrong_page.positive_confirm.is_none());
        assert!(wrong_value
            .positive_publication_snapshot(parent)
            .unwrap()
            .published
            .is_empty());
        let RegistrySettlement::ConfirmFinalizer(seed) = wrong_value.settle_quiescence(
            wrong_page
                .quiescence
                .expect("the wrong-value page should quiesce"),
        ) else {
            panic!("the exact Confirm result should still finalize")
        };
        assert!(!seed.state.accepted.contains(&first));
        assert!(seed.state.accepted.contains(&later));

        let mut nullable = ProducerRegistry::new();
        let (_, parent, credit, initial) =
            open_tapped_confirm(&mut nullable, [first, first], Some(first));
        assert_eq!(initial, [first]);
        let nullable_page = nullable.replace_program(
            credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert!(
            nullable_page.positive_confirm.is_none(),
            "seed-time nullable acceptance is not replacement authority"
        );
        assert!(nullable
            .positive_publication_snapshot(parent)
            .unwrap()
            .published
            .is_empty());
        let RegistrySettlement::ConfirmFinalizer(seed) = nullable.settle_quiescence(
            nullable_page
                .quiescence
                .expect("the nullable root should quiesce after its real page"),
        ) else {
            panic!("nullable acceptance must remain on the exact late path")
        };
        assert!(seed.state.accepted.contains(&first));
    }

    #[test]
    fn positive_publication_certificate_keeps_terminal_boundary_evidence() {
        assert_eq!(
            std::mem::size_of::<Option<Box<PositivePublicationRegistration>>>(),
            std::mem::size_of::<usize>(),
            "dormant positive-publication state must cost one nullable pointer"
        );
        let certificate = terminal_positive_certificate();
        assert!(certificate.fixed_denotation);
        assert_eq!(
            certificate.continuation,
            ContinuationPublicationReceipt::Terminal
        );
        assert!(
            certificate.crosses_set_boundary,
            "Terminal precedence must not erase the semantic parent's SET-boundary fact"
        );
        assert!(certificate.eligible());
    }

    #[test]
    fn positive_publication_ledger_requires_fixed_nonbarrier_parent() {
        let eligible = terminal_positive_certificate();
        let mut registry = ProducerRegistry::new();

        let mut unfixed = eligible;
        unfixed.fixed_denotation = false;
        let (unfixed_activation, unfixed_parent) =
            open_positive_confirm(&mut registry, [value(1)], unfixed);
        assert!(matches!(
            registry.state.activations[&unfixed_activation]
                .positive_publication
                .as_deref(),
            Some(PositivePublicationRegistration::Private {
                confirm_state: StateId(17),
                certificate,
            }) if *certificate == unfixed
        ));
        assert!(
            registry
                .positive_publication_snapshot(unfixed_parent)
                .is_none(),
            "an unfixed parent must not own a ledger"
        );
        assert!(registry
            .open_positive_support_activation(
                unfixed_parent,
                0,
                value(1),
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());

        let mut barrier = eligible;
        barrier.continuation = ContinuationPublicationReceipt::Barrier;
        let (barrier_activation, barrier_parent) =
            open_positive_confirm(&mut registry, [value(2)], barrier);
        assert!(matches!(
            registry.state.activations[&barrier_activation]
                .positive_publication
                .as_deref(),
            Some(PositivePublicationRegistration::Private {
                confirm_state: StateId(17),
                certificate,
            }) if *certificate == barrier
        ));
        assert!(
            registry
                .positive_publication_snapshot(barrier_parent)
                .is_none(),
            "a Barrier parent must not own a ledger"
        );
        assert!(registry
            .open_positive_support_activation(
                barrier_parent,
                0,
                value(2),
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());

        let malformed_chunk = PositivePublicationCertificate {
            fixed_denotation: true,
            continuation: ContinuationPublicationReceipt::ChunkHomomorphic,
            crosses_set_boundary: false,
        };
        let (_, malformed_parent) =
            open_positive_confirm(&mut registry, [value(3)], malformed_chunk);
        assert!(
            registry
                .positive_publication_snapshot(malformed_parent)
                .is_none(),
            "ChunkHomomorphic requires the semantic parent to cross the SET boundary"
        );
        assert!(registry
            .open_positive_support_activation(
                malformed_parent,
                0,
                value(3),
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());
    }

    #[test]
    fn positive_support_opener_derives_exact_row_and_requires_every_support_variable() {
        let parent_value = value(80);
        let candidate = value(81);
        let parent_bound = VariableSet::new_singleton(0);
        let candidate_bound = parent_bound.union(VariableSet::new_singleton(1));
        let relevant = ChildSet::empty(1).with_inserted(0);
        let return_to = positive_candidate_return(
            vec![parent_value],
            parent_bound,
            1,
            relevant.clone(),
            relevant,
            true,
        );
        let mut registry = ProducerRegistry::new();
        let (_, parent) = open_positive_confirm_with_return(
            &mut registry,
            [candidate],
            terminal_positive_certificate(),
            return_to,
        );
        let missing_required = candidate_bound.union(VariableSet::new_singleton(2));
        assert!(
            registry
                .open_positive_support_activation(
                    parent,
                    0,
                    candidate,
                    missing_required,
                    Some(candidate_bound),
                )
                .is_none(),
            "a partially bound Support constraint must not become positive authority"
        );

        let child = registry
            .open_positive_support_activation(
                parent,
                0,
                candidate,
                candidate_bound,
                Some(candidate_bound),
            )
            .expect("a fully bound Support constraint should open");
        let activation = &registry.state.activations[&child];
        let DeltaReducer::PositiveSupport { link, witnessed } = &activation.reducer else {
            panic!("specialized opener installed the wrong reducer")
        };
        assert_eq!(link.child, child);
        assert_eq!(link.parent, parent.activation);
        assert_eq!(link.occurrence, 0);
        assert_eq!(link.value, candidate);
        assert!(!witnessed);
        let DeltaReturn::PositiveSupport { bound, row } = &activation.return_to else {
            panic!("specialized opener installed the wrong return")
        };
        assert_eq!(*bound, candidate_bound);
        assert_eq!(row.as_ref(), &[parent_value, candidate]);
        let (source_bound, source_row, source_candidates) = registry.source_context(child);
        assert_eq!(source_bound, candidate_bound);
        assert_eq!(source_row, &[parent_value, candidate]);
        assert!(source_candidates.is_none());
        assert_eq!(
            registry.source_dispatch_shape(child),
            (candidate_bound, false)
        );
    }

    #[test]
    fn positive_support_opener_checks_the_exact_original_occurrence() {
        let a = value(82);
        let b = value(83);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [a, b, a], terminal_positive_certificate());
        assert!(
            registry
                .open_positive_support_activation(
                    parent,
                    1,
                    a,
                    VariableSet::new_singleton(0),
                    Some(terminal_positive_full()),
                )
                .is_none(),
            "membership elsewhere in B must not validate the indexed occurrence"
        );
        assert!(registry
            .open_positive_support_activation(
                parent,
                2,
                a,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_some());
    }

    #[test]
    fn positive_support_commit_revalidates_the_linked_original_occurrence() {
        let a = value(94);
        let b = value(95);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut registry, [a, b, a], terminal_positive_certificate());
        let (_, witness, _) = terminal_positive_witness(&mut registry, parent, 2, a, false);
        let DeltaReducer::Confirm { original } = &mut registry
            .state
            .activations
            .get_mut(&activation)
            .unwrap()
            .reducer
        else {
            unreachable!()
        };
        *original = shared_one_parent_candidates(vec![a, b, b]);

        assert!(
            registry
                .commit_positive_publication(witness, Some(terminal_positive_full()))
                .is_none(),
            "open-time membership must not replace commit-time indexed revalidation"
        );
        assert!(registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .published
            .is_empty());
    }

    #[test]
    fn positive_support_commit_requires_the_current_physical_child() {
        let candidate = value(96);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
        let (child, witness, proof) =
            terminal_positive_witness(&mut registry, parent, 0, candidate, true);
        assert_eq!(proof.activation, child);
        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(proof) else {
            panic!("positive Support child must complete physically")
        };
        assert!(matches!(completed.effect, DeltaCompletion::Cleanup));
        assert!(!registry.is_live(child));
        assert!(
            registry
                .commit_positive_publication(witness, Some(terminal_positive_full()))
                .is_none(),
            "a witness cannot outlive its current physical child"
        );
        assert!(registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .published
            .is_empty());
    }

    #[test]
    fn positive_support_commit_rejects_corruption_in_each_child_row_region() {
        let parent_value = value(84);
        let candidate = value(85);
        let parent_bound = VariableSet::new_singleton(0);
        let full = parent_bound.union(VariableSet::new_singleton(1));
        let relevant = ChildSet::empty(1).with_inserted(0);
        let return_to = positive_candidate_return(
            vec![parent_value],
            parent_bound,
            1,
            relevant.clone(),
            relevant,
            true,
        );
        let mut registry = ProducerRegistry::new();
        let (_, parent) = open_positive_confirm_with_return(
            &mut registry,
            [candidate],
            terminal_positive_certificate(),
            return_to,
        );
        let (candidate_child, candidate_credit) =
            open_positive_support_credit(&mut registry, parent, 0, candidate, full, Some(full));
        let (candidate_witness, _) = replace_positive_support_credit(
            &mut registry,
            candidate_credit,
            Some(candidate),
            false,
        );
        let (parent_child, parent_credit) =
            open_positive_support_credit(&mut registry, parent, 0, candidate, full, Some(full));
        let (parent_witness, _) =
            replace_positive_support_credit(&mut registry, parent_credit, None, true);

        let DeltaReturn::PositiveSupport { row, .. } = &mut registry
            .state
            .activations
            .get_mut(&candidate_child)
            .unwrap()
            .return_to
        else {
            unreachable!()
        };
        row[1] = value(86);
        assert!(
            registry
                .commit_positive_publication(candidate_witness, Some(full))
                .is_none(),
            "a corrupted candidate column must not publish"
        );

        let DeltaReturn::PositiveSupport { row, .. } = &mut registry
            .state
            .activations
            .get_mut(&parent_child)
            .unwrap()
            .return_to
        else {
            unreachable!()
        };
        row[0] = value(87);
        assert!(
            registry
                .commit_positive_publication(parent_witness, Some(full))
                .is_none(),
            "a corrupted preserved parent column must not publish"
        );
        assert!(registry
            .positive_publication_snapshot(parent)
            .unwrap()
            .published
            .is_empty());
    }

    #[test]
    fn positive_support_link_child_corruption_panics_before_witness_minting() {
        let candidate = value(88);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
        let (child, credit) = open_positive_support_credit(
            &mut registry,
            parent,
            0,
            candidate,
            VariableSet::new_singleton(0),
            Some(terminal_positive_full()),
        );
        let DeltaReducer::PositiveSupport { link, .. } =
            &mut registry.state.activations.get_mut(&child).unwrap().reducer
        else {
            unreachable!()
        };
        link.child = ActivationId::test(u64::MAX);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.replace_program(
                    credit,
                    DeltaStateId(0),
                    &[],
                    std::iter::empty(),
                    [candidate],
                    std::iter::empty(),
                    false,
                    false,
                    false,
                    None,
                );
            }))
            .is_err(),
            "registry-owned physical custody corruption must fail loudly"
        );
    }

    #[test]
    fn positive_support_first_real_success_mints_once_and_replay_is_rejected() {
        for supported_first in [false, true] {
            let candidate = value(if supported_first { 89 } else { 90 });
            let mut registry = ProducerRegistry::new();
            let (_, parent) =
                open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
            let child = registry
                .open_positive_support_activation(
                    parent,
                    0,
                    candidate,
                    VariableSet::new_singleton(0),
                    Some(terminal_positive_full()),
                )
                .unwrap();
            let installed = registry.install_program_roots(
                child,
                [0, 1].map(|slot| ProgramSeedWork {
                    parent: 0,
                    work: positive_test_work(slot),
                    accepted: None,
                }),
            );
            assert!(matches!(
                &registry.state.activations[&child].reducer,
                DeltaReducer::PositiveSupport {
                    witnessed: false,
                    ..
                }
            ));
            let mut roots = installed.roots.into_iter();
            let (_, first_credit) = roots.next().unwrap();
            let (_, later_credit) = roots.next().unwrap();
            let replay_brand = first_credit.brand;
            let replay_key = first_credit.key;
            let mut first = registry.replace_program(
                first_credit,
                DeltaStateId(0),
                &[],
                std::iter::empty(),
                (!supported_first).then_some(candidate),
                std::iter::empty(),
                supported_first,
                false,
                false,
                None,
            );
            assert!(first.positive_support_reducer);
            assert!(first.accepted.is_empty());
            assert_eq!(first.raw_proposal_occurrences, 0);
            assert!(first.quiescence.is_none());
            let witness = *first
                .positive_support
                .take()
                .expect("the first real success must mint exactly one witness");
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    registry.replace_program(
                        ProducerCredit {
                            brand: replay_brand,
                            key: replay_key,
                        },
                        DeltaStateId(0),
                        &[],
                        std::iter::empty(),
                        std::iter::empty(),
                        std::iter::empty(),
                        true,
                        false,
                        false,
                        None,
                    );
                }))
                .is_err(),
                "a consumed producer credit must not replay"
            );

            let later = registry.replace_program(
                later_credit,
                DeltaStateId(0),
                &[],
                std::iter::empty(),
                (!supported_first).then_some(candidate),
                std::iter::empty(),
                !supported_first,
                false,
                false,
                None,
            );
            assert!(later.positive_support_reducer);
            assert!(later.positive_support.is_none());
            assert!(later.quiescence.is_some());
            assert!(commit_terminal_positive(&mut registry, witness));
        }
    }

    #[test]
    fn positive_support_witnessed_and_unwitnessed_quiescence_release_cleanup() {
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        for witnessed in [false, true] {
            let candidate = value(if witnessed { 91 } else { 92 });
            let mut registry = ProducerRegistry::new();
            let (_, parent) =
                open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
            let (child, credit) = open_positive_support_credit(
                &mut registry,
                parent,
                0,
                candidate,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            );
            let mut outcome = registry.replace_program(
                credit,
                DeltaStateId(0),
                &[],
                std::iter::empty(),
                witnessed.then_some(candidate),
                std::iter::empty(),
                false,
                false,
                false,
                None,
            );
            assert_eq!(outcome.positive_support.is_some(), witnessed);
            if let Some(witness) = outcome.positive_support.take() {
                assert!(commit_terminal_positive(&mut registry, *witness));
            }
            let proof = outcome.quiescence.expect("the sole credit should quiesce");
            assert_eq!(proof.activation, child);
            let RegistrySettlement::Completed(completed) = registry.settle_quiescence(proof) else {
                panic!("positive Support child must complete physically")
            };
            assert!(matches!(
                completed.return_to,
                DeltaReturn::PositiveSupport { .. }
            ));
            assert!(matches!(completed.effect, DeltaCompletion::Cleanup));

            let mut scheduler = DeltaScheduler::new();
            let drained = scheduler.release_completion(
                completed,
                &plan,
                &mut Worklist::new(),
                &mut StateInterner::default(),
                &mut ResidualStateStats::default(),
            );
            assert!(drained.continuation.is_none());
            assert!(drained.active.is_none());
        }
    }

    #[test]
    fn positive_support_rejects_seed_success_and_after_children() {
        let candidate = value(93);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
        let child = registry
            .open_positive_support_activation(
                parent,
                0,
                candidate,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.install_program_roots(
                    child,
                    [ProgramSeedWork {
                        parent: 0,
                        work: positive_test_work(0),
                        accepted: Some(candidate),
                    }],
                );
            }))
            .is_err(),
            "PositiveSupport must not counterfeit a replacement witness from initial acceptance"
        );
        let mut installed = registry.install_program_roots(
            child,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(1),
                accepted: None,
            }],
        );
        let (_, credit) = installed.roots.pop().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.replace_program(
                    credit,
                    DeltaStateId(0),
                    &[],
                    std::iter::empty(),
                    std::iter::empty(),
                    std::iter::empty(),
                    false,
                    false,
                    false,
                    Some(ProgramResume::AfterChildrenDone),
                );
            }))
            .is_err(),
            "PositiveSupport must not claim unsupported generic AfterChildren propagation"
        );
    }

    #[test]
    fn positive_commit_preflights_release_before_first_winner() {
        assert_terminal_positive_preflight_rejected(None, |_, _| {});
        assert_terminal_positive_preflight_rejected(Some(VariableSet::new_empty()), |_, _| {});
        assert_terminal_positive_preflight_rejected(
            Some(terminal_positive_full()),
            |registry, activation| {
                let DeltaReturn::Stable {
                    set_admit_result, ..
                } = &mut registry
                    .state
                    .activations
                    .get_mut(&activation)
                    .unwrap()
                    .return_to
                else {
                    unreachable!()
                };
                *set_admit_result = false;
            },
        );
        assert_terminal_positive_preflight_rejected(
            Some(terminal_positive_full()),
            |registry, activation| {
                let DeltaReturn::Stable { parent, .. } = &mut registry
                    .state
                    .activations
                    .get_mut(&activation)
                    .unwrap()
                    .return_to
                else {
                    unreachable!()
                };
                *parent = vec![value(99)].into_boxed_slice();
            },
        );
        assert_terminal_positive_preflight_rejected(
            Some(terminal_positive_full()),
            |registry, activation| {
                let DeltaReturn::Stable { desc, .. } = &mut registry
                    .state
                    .activations
                    .get_mut(&activation)
                    .unwrap()
                    .return_to
                else {
                    unreachable!()
                };
                let ResidualPhase::Candidate { checked, .. } = &mut desc.phase else {
                    unreachable!()
                };
                *checked = ChildSet::empty(1);
            },
        );
        assert_terminal_positive_preflight_rejected(
            Some(terminal_positive_full()),
            |registry, activation| {
                registry
                    .state
                    .activations
                    .get_mut(&activation)
                    .unwrap()
                    .return_to = DeltaReturn::Formula {
                    bound: VariableSet::new_empty(),
                    counter: FormulaPcId(0),
                    batch: formula_or_reducer_batch(&[]),
                };
            },
        );
        assert_terminal_positive_preflight_rejected(
            Some(terminal_positive_full()),
            |registry, activation| {
                let Some(PositivePublicationRegistration::Eligible(ledger)) = registry
                    .state
                    .activations
                    .get_mut(&activation)
                    .unwrap()
                    .positive_publication
                    .as_deref_mut()
                else {
                    unreachable!()
                };
                ledger.certificate.continuation = ContinuationPublicationReceipt::Barrier;
            },
        );

        let (mut registry, _, parent, _child, witness) = terminal_positive_commit_fixture();
        let candidate = witness.link.value;
        let grant = registry
            .commit_positive_publication(witness, Some(terminal_positive_full()))
            .expect("the completely preflighted first value must win");
        assert!(matches!(
            &grant.route,
            PositivePublicationRoute::Terminal {
                registration: Some(_),
                ..
            }
        ));
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let release = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(release.publication.is_some());
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate])
        );
        let (_, hedge, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, true);
        assert!(
            registry
                .commit_positive_publication(hedge, Some(terminal_positive_full()))
                .is_none(),
            "a later real hedge cannot mint the same grant twice"
        );
    }

    #[test]
    fn positive_terminal_release_registers_semantic_confirm_before_real_staging() {
        let first = value(7);
        let second = value(8);
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let mut registry = ProducerRegistry::new();
        let (_, parent) = open_positive_confirm(
            &mut registry,
            [first, second],
            terminal_positive_certificate(),
        );
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();

        let (_, first_witness, _) =
            terminal_positive_witness(&mut registry, parent, 0, first, false);
        let first_grant = registry
            .commit_positive_publication(first_witness, Some(terminal_positive_full()))
            .expect("the first Terminal value should commit");
        let first_release = DeltaScheduler::release_positive_publication(
            first_grant,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(first_release.continuation.is_none());
        let mut publication = first_release
            .publication
            .expect("Terminal release must publish a complete row");
        assert_eq!(publication.rows.rows, [first]);
        assert_eq!(publication.rows.row_count, 1);
        assert_eq!(publication.origins.as_slice(), [parent.activation]);
        assert!(
            !publication.origins.contains(&ActivationId::test(u64::MAX)),
            "physical Support identity must not leak into semantic publication"
        );
        assert_eq!(publication.registrations.len(), 1);
        let registration = &publication.registrations[0];
        assert_eq!(registration.family, StateId(17));
        assert_eq!(registration.origin, parent.activation);

        let (_, second_witness, _) =
            terminal_positive_witness(&mut registry, parent, 1, second, true);
        let second_grant = registry
            .commit_positive_publication(second_witness, Some(terminal_positive_full()))
            .expect("a distinct later Terminal value should commit");
        let second_release = DeltaScheduler::release_positive_publication(
            second_grant,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(
            second_release
                .publication
                .as_ref()
                .is_some_and(|batch| batch.registrations.is_empty()),
            "one semantic parent may register its Terminal origin only once"
        );
        let second_publication = second_release
            .publication
            .expect("later Terminal values still publish complete rows");
        assert_eq!(second_publication.rows.rows, [second]);
        assert_eq!(second_publication.origins.as_slice(), [parent.activation]);
        publication.append(second_publication);

        let mut machine = ResidualStateMachine::new(terminal_positive_full(), 1, Search::Done);
        machine.stage_direct_terminal_publication(publication);
        assert_eq!(machine.emit_rows, [first, second]);
        assert_eq!(
            machine.emit_origins.as_deref(),
            Some([parent.activation, parent.activation].as_slice())
        );
        machine.terminal_yield.complete(parent.activation);
        let origins = machine.emit_origins.clone().unwrap();
        for origin in origins {
            let mut attempt = machine.terminal_yield.begin_projection(origin);
            attempt.mark_successful();
        }
        machine.emit_next = machine.emit_count;
        let family = machine
            .terminal_yield
            .families
            .get(&StateId(17))
            .expect("semantic Confirm family was registered");
        assert_eq!(family.admitted, 1);
        assert_eq!(family.live, 0);
        assert_eq!(family.completed, 1);
        assert_eq!(family.projected, 2);
        assert!(machine.terminal_yield.samples[parent.activation.index()].is_none());
        assert!(stable.is_empty());
        assert_eq!(stats.delta_positive_publication_terminal_commits, 2);
        assert_eq!(
            stats.delta_positive_publication_chunk_homomorphic_commits,
            0
        );
    }

    #[test]
    fn positive_terminal_release_accepts_an_already_set_admitted_input() {
        let candidate = value(9);
        let mut certificate = terminal_positive_certificate();
        certificate.crosses_set_boundary = false;
        assert!(
            certificate.eligible(),
            "Terminal precedence must not require a fresh SET crossing"
        );
        let mut return_to = terminal_positive_return(Vec::new());
        let DeltaReturn::Stable {
            set_admit_result, ..
        } = &mut return_to
        else {
            unreachable!()
        };
        *set_admit_result = false;

        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm_with_return(&mut registry, [candidate], certificate, return_to);
        let (_, witness, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, false);
        let grant = registry
            .commit_positive_publication(witness, Some(terminal_positive_full()))
            .expect("an already-SET Terminal successor should still publish");
        let root = PositiveCertificateLeaf {
            variable: 0,
            page_local: false,
        };
        let plan = ResidualPlan::compile(&root);
        let release = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut Worklist::new(),
            &mut StateInterner::default(),
            &mut ResidualStateStats::default(),
        );
        assert_eq!(
            release
                .publication
                .expect("Terminal grant lost its publication")
                .rows
                .rows,
            [candidate]
        );
    }

    #[test]
    fn positive_chunk_release_files_singleton_into_exact_saved_k() {
        let candidate = value(11);
        let (plan, successor, certificate, _full) = chunk_positive_fixture();
        let mut registry = ProducerRegistry::new();
        let return_to = DeltaReturn::Stable {
            desc: successor.clone(),
            parent: Vec::new().into_boxed_slice(),
            set_admit_result: true,
        };
        let (activation, parent) =
            open_positive_confirm_with_return(&mut registry, [candidate], certificate, return_to);
        let (_, credit) = open_positive_support_credit(
            &mut registry,
            parent,
            0,
            candidate,
            VariableSet::new_singleton(0),
            None,
        );
        let (witness, _) =
            replace_positive_support_credit(&mut registry, credit, Some(candidate), false);
        let grant = registry
            .commit_positive_publication(witness, None)
            .expect("ChunkHomomorphic publication needs no Terminal capability");
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        let release = DeltaScheduler::release_positive_publication(
            grant,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(release.publication.is_none());
        let token = release
            .continuation
            .expect("one parent and one value make Chunk filing nonempty");
        assert_eq!(stable_interner.get(token.state), &successor);
        let StateBucket::Candidates(batch) = &stable[&token.rank][&token.state] else {
            panic!("Chunk release filed the wrong payload kind")
        };
        assert_eq!(batch.parents.row_count, 1);
        assert!(batch.parents.rows.is_empty());
        assert_eq!(
            batch.candidates.iter().collect::<Vec<_>>(),
            [(0, candidate)]
        );
        assert_eq!(token.rows, 1);
        assert_eq!(token.candidates, 1);
        assert_eq!(stats.delta_positive_publication_terminal_commits, 0);
        assert_eq!(
            stats.delta_positive_publication_chunk_homomorphic_commits,
            1
        );
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate]),
            "filing K is not a rollback point for P"
        );

        let proof = quiesce_confirm_with_accepted(&mut registry, activation, [candidate]);
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(proof) else {
            panic!("nonempty original B should retain its ordinary finalizer")
        };
        assert!(
            seed.state.accepted.is_empty(),
            "the finalizer must receive G minus P independently of K's later result"
        );
    }

    #[test]
    fn positive_publication_commit_has_one_winner_and_duplicate_open_is_inert() {
        let candidate = value(7);
        let certificate = terminal_positive_certificate();
        let mut registry = ProducerRegistry::new();
        let (activation, parent) = open_positive_confirm(&mut registry, [candidate], certificate);
        let (_, first, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, false);
        let (_, hedge, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, true);

        assert!(commit_terminal_positive(&mut registry, first));
        assert!(
            !commit_terminal_positive(&mut registry, hedge),
            "a later hedge for the same (parent, value) must be inert"
        );
        let before_reopen = registry.positive_publication_snapshot(parent).unwrap();
        assert_eq!(before_reopen.published, BTreeSet::from([candidate]));
        assert!(
            registry
                .open_exact_and_support_publication(activation, StateId(99), certificate)
                .is_none(),
            "one semantic parent must not reopen and erase committed obligations"
        );
        assert_eq!(
            registry.positive_publication_snapshot(parent),
            Some(before_reopen)
        );
    }

    #[test]
    fn positive_publication_commit_accepts_distinct_values_independently() {
        let one = value(1);
        let two = value(2);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [one, two], terminal_positive_certificate());
        let (_, one_witness, _) = terminal_positive_witness(&mut registry, parent, 0, one, false);
        let (_, two_witness, _) = terminal_positive_witness(&mut registry, parent, 1, two, true);

        assert!(commit_terminal_positive(&mut registry, two_witness));
        assert!(commit_terminal_positive(&mut registry, one_witness));
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([one, two])
        );
    }

    #[test]
    fn positive_publication_commit_revalidates_frozen_original_b_and_confirm_reducer() {
        let member = value(3);
        let absent = value(4);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut registry, [member], terminal_positive_certificate());
        assert!(
            registry
                .open_positive_support_activation(
                    parent,
                    0,
                    absent,
                    VariableSet::new_singleton(0),
                    Some(terminal_positive_full()),
                )
                .is_none(),
            "the registry must not open a physical child outside original B"
        );
        assert!(registry
            .open_positive_support_activation(
                parent,
                1,
                member,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());
        let (_, witness, _) = terminal_positive_witness(&mut registry, parent, 0, member, false);
        let before = registry.positive_publication_snapshot(parent).unwrap();
        registry
            .state
            .activations
            .get_mut(&activation)
            .unwrap()
            .reducer = DeltaReducer::Support { published: false };
        assert!(
            !commit_terminal_positive(&mut registry, witness),
            "a parent no longer owning a Confirm reducer must be inert"
        );
        assert_eq!(registry.positive_publication_snapshot(parent), Some(before));
    }

    #[test]
    fn positive_publication_generation_and_close_snapshot_fence_stale_witnesses() {
        let candidate = value(5);
        let mut registry = ProducerRegistry::new();
        let (_, parent) =
            open_positive_confirm(&mut registry, [candidate], terminal_positive_certificate());
        let (_, witness, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, false);
        let generation = witness.link.generation;
        let wrong_generation = generation
            .checked_add(1)
            .expect("test generation should have a successor");
        let open = registry.positive_publication_snapshot(parent).unwrap();
        assert!(registry
            .close_and_snapshot_positive_publication(parent, wrong_generation)
            .is_none());
        assert_eq!(
            registry.positive_publication_snapshot(parent),
            Some(open.clone())
        );

        let closed = registry
            .close_and_snapshot_positive_publication(parent, generation)
            .expect("matching settlement authority should close and snapshot");
        assert!(!closed.open);
        assert_ne!(
            closed.generation, generation,
            "settlement must advance the generation before freezing its snapshot"
        );
        assert!(
            !commit_terminal_positive(&mut registry, witness),
            "all outstanding physical children must be inert after close"
        );
        assert!(
            registry
                .close_and_snapshot_positive_publication(parent, generation)
                .is_none(),
            "close-and-snapshot authority is affine"
        );
        assert_eq!(registry.positive_publication_snapshot(parent), Some(closed));
    }

    #[test]
    fn positive_support_links_are_parent_local_and_unknown_safe() {
        let candidate = value(9);
        let certificate = terminal_positive_certificate();
        let mut registry = ProducerRegistry::new();
        let (_, left) = open_positive_confirm(&mut registry, [candidate], certificate);
        let (_, right) = open_positive_confirm(&mut registry, [candidate], certificate);
        let (_, left_witness, _) =
            terminal_positive_witness(&mut registry, left, 0, candidate, false);
        let (_, right_witness, _) =
            terminal_positive_witness(&mut registry, right, 0, candidate, true);

        assert_eq!(left_witness.link.parent, left.activation);
        assert_eq!(right_witness.link.parent, right.activation);
        assert_ne!(left_witness.link.child, right_witness.link.child);
        assert!(commit_terminal_positive(&mut registry, left_witness));
        assert!(
            commit_terminal_positive(&mut registry, right_witness),
            "independent parents may each publish the same value once"
        );

        let (gone_activation, gone_parent) =
            open_positive_confirm(&mut registry, [candidate], certificate);
        let (_, gone_witness, _) =
            terminal_positive_witness(&mut registry, gone_parent, 0, candidate, false);
        registry.state.activations.remove(&gone_activation).unwrap();
        assert!(!commit_terminal_positive(&mut registry, gone_witness));
    }

    #[test]
    fn positive_support_registry_clone_keeps_links_rebrands_credits_and_diverges() {
        let one = value(13);
        let two = value(14);
        let mut original = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut original, [one, two], terminal_positive_certificate());
        let (child, credit) = open_positive_support_credit(
            &mut original,
            parent,
            0,
            one,
            VariableSet::new_singleton(0),
            Some(terminal_positive_full()),
        );
        let credit_key = credit.key;
        let original_link = match &original.state.activations[&child].reducer {
            DeltaReducer::PositiveSupport { link, .. } => link.clone(),
            _ => panic!("positive Support child lost its reducer"),
        };
        let (mut cloned, mut remap) = original.deep_clone();
        let cloned_credit = remap
            .remove(&credit_key)
            .expect("deep clone rebranded the live positive Support credit");
        assert!(remap.is_empty());
        let cloned_parent = cloned
            .positive_parent(activation)
            .expect("deep clone retained the Confirm activation");
        assert_ne!(parent, cloned_parent);
        let cloned_link = match &cloned.state.activations[&child].reducer {
            DeltaReducer::PositiveSupport { link, .. } => link.clone(),
            _ => panic!("cloned positive Support child lost its reducer"),
        };
        assert_eq!(original_link, cloned_link);
        assert_eq!(original_link.child, child);

        let (original_witness, _) =
            replace_positive_support_credit(&mut original, credit, Some(one), false);
        let (cloned_witness, _) =
            replace_positive_support_credit(&mut cloned, cloned_credit, None, true);
        assert_eq!(original_witness.brand, original.brand);
        assert_eq!(cloned_witness.brand, cloned.brand);
        assert!(
            !commit_terminal_positive(&mut cloned, original_witness),
            "a post-replacement witness must not cross registry branches"
        );
        assert!(commit_terminal_positive(&mut cloned, cloned_witness));
        let (_, original_two, _) = terminal_positive_witness(&mut original, parent, 1, two, false);
        assert!(commit_terminal_positive(&mut original, original_two));
        assert_eq!(
            original
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([two])
        );
        assert_eq!(
            cloned
                .positive_publication_snapshot(cloned_parent)
                .unwrap()
                .published,
            BTreeSet::from([one])
        );
    }

    #[test]
    fn positive_support_links_index_duplicate_occurrences_but_publication_is_a_set() {
        let candidate = value(21);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) = open_positive_confirm(
            &mut registry,
            [candidate, candidate],
            terminal_positive_certificate(),
        );
        let (_, first, _) = terminal_positive_witness(&mut registry, parent, 0, candidate, false);
        let (_, second, _) = terminal_positive_witness(&mut registry, parent, 1, candidate, true);
        assert_eq!(first.link.parent, parent.activation);
        assert_eq!(first.link.occurrence, 0);
        assert_eq!(second.link.occurrence, 1);
        assert_eq!(first.link.value, candidate);
        assert_eq!(second.link.value, candidate);
        let DeltaReducer::Confirm { original } = &registry.state.activations[&activation].reducer
        else {
            panic!("positive parent lost its Confirm reducer")
        };
        assert_eq!(
            original.one_parent_values(),
            &[candidate, candidate],
            "the authoritative original B preserves bag multiplicity"
        );

        assert!(commit_terminal_positive(&mut registry, first));
        assert!(!commit_terminal_positive(&mut registry, second));
        assert_eq!(
            registry
                .positive_publication_snapshot(parent)
                .unwrap()
                .published,
            BTreeSet::from([candidate])
        );
    }

    #[test]
    fn positive_settlement_partitions_the_accepted_set_before_finalization() {
        let published_one = value(31);
        let published_two = value(32);
        let remainder = value(33);
        let rejected = value(34);
        let mut registry = ProducerRegistry::new();
        let original = shared_one_parent_candidates(vec![
            published_one,
            remainder,
            published_two,
            published_one,
            rejected,
            published_two,
            remainder,
        ]);
        let started = registry.start_many(
            DeltaReducer::Confirm { original },
            terminal_positive_return(Vec::new()),
            [
                output(31, 0, true),
                output(32, 1, true),
                output(33, 2, true),
            ],
        );
        let activation = started.activation;
        let parent = registry
            .open_exact_and_support_publication(
                activation,
                StateId(17),
                terminal_positive_certificate(),
            )
            .expect("Confirm activation should register a semantic parent");
        for (occurrence, published) in [(0, published_one), (2, published_two)] {
            let (_, witness, _) =
                terminal_positive_witness(&mut registry, parent, occurrence, published, false);
            assert!(commit_terminal_positive(&mut registry, witness));
        }

        let mut proof = None;
        for (_, credit) in started.roots {
            if let Some(quiescence) = registry.replace_traversal(credit, []).quiescence {
                assert!(
                    proof.replace(quiescence).is_none(),
                    "one affine activation produced two quiescence receipts"
                );
            }
        }
        let proof = proof.expect("retiring every real Confirm credit must prove quiescence");
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(proof) else {
            panic!("nonempty positive Confirm did not open its ordinary finalizer")
        };

        assert_eq!(
            seed.state.accepted.as_ref(),
            &AHashSet::from_iter([remainder]),
            "the unchanged finalizer must own exactly G minus P"
        );
        let cloned_finalizer = seed.state.clone();
        assert!(
            Arc::ptr_eq(&seed.state.accepted, &cloned_finalizer.accepted),
            "post-handoff clones may share only immutable residual G minus P"
        );
        assert!(
            registry.state.activations[&activation]
                .positive_publication
                .is_none(),
            "graph-dead publication evidence must not survive finalizer handoff"
        );
        assert!(
            registry
                .open_positive_support_activation(
                    parent,
                    0,
                    published_one,
                    VariableSet::new_singleton(0),
                    Some(terminal_positive_full()),
                )
                .is_none(),
            "the finalizer handoff must fence every stale positive child"
        );
    }

    #[test]
    fn positive_empty_partition_preserves_ordinary_acceptance() {
        let accepted = value(41);
        let rejected = value(42);
        let mut registry = ProducerRegistry::new();
        let (activation, _parent) = open_positive_confirm(
            &mut registry,
            [accepted, rejected, accepted],
            terminal_positive_certificate(),
        );
        let proof = quiesce_confirm_with_accepted(&mut registry, activation, [accepted]);
        let RegistrySettlement::ConfirmFinalizer(seed) = registry.settle_quiescence(proof) else {
            panic!("nonempty positive Confirm did not open its ordinary finalizer")
        };

        assert_eq!(
            seed.state.accepted.as_ref(),
            &AHashSet::from_iter([accepted])
        );
        assert!(
            registry.state.activations[&activation]
                .positive_publication
                .is_none(),
            "an empty closed ledger must not burden the ordinary finalizer"
        );
    }

    #[test]
    fn positive_settlement_rejects_contradictory_publication_before_handoff() {
        let published = value(51);
        let accepted = value(52);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) = open_positive_confirm(
            &mut registry,
            [published, accepted],
            terminal_positive_certificate(),
        );
        let (_, witness, _) = terminal_positive_witness(&mut registry, parent, 0, published, false);
        assert!(commit_terminal_positive(&mut registry, witness));
        let proof = quiesce_confirm_with_accepted(&mut registry, activation, [accepted]);

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.settle_quiescence(proof);
            }))
            .is_err(),
            "P outside authoritative G must fail before opening finalizer work"
        );
        let activation = registry
            .state
            .activations
            .get(&activation)
            .expect("contradictory settlement removed its authoritative parent");
        assert!(matches!(&activation.reducer, DeltaReducer::Confirm { .. }));
        assert_eq!(activation.status, ActivationStatus::Quiescent);
        assert!(activation.live.is_empty());
        assert_eq!(activation.accepted, AHashSet::from_iter([accepted]));
        let closed = registry
            .positive_publication_snapshot(parent)
            .expect("contradictory publication lost its frozen evidence");
        assert!(!closed.open);
        assert_eq!(closed.published, BTreeSet::from([published]));
    }

    #[test]
    fn positive_partition_is_clone_local_before_handoff() {
        let one = value(61);
        let two = value(62);
        let mut original = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut original, [one, two], terminal_positive_certificate());
        let (mut cloned, remap) = original.deep_clone();
        assert!(remap.is_empty());
        let cloned_parent = cloned
            .positive_parent(activation)
            .expect("cloned Confirm retained its affine parent");

        let (_, original_witness, _) =
            terminal_positive_witness(&mut original, parent, 0, one, false);
        let (_, cloned_witness, _) =
            terminal_positive_witness(&mut cloned, cloned_parent, 1, two, true);
        assert!(commit_terminal_positive(&mut original, original_witness));
        assert!(commit_terminal_positive(&mut cloned, cloned_witness));

        let original_proof = quiesce_confirm_with_accepted(&mut original, activation, [one, two]);
        let cloned_proof = quiesce_confirm_with_accepted(&mut cloned, activation, [one, two]);
        let RegistrySettlement::ConfirmFinalizer(original_seed) =
            original.settle_quiescence(original_proof)
        else {
            panic!("original positive parent did not open its finalizer")
        };
        let RegistrySettlement::ConfirmFinalizer(cloned_seed) =
            cloned.settle_quiescence(cloned_proof)
        else {
            panic!("cloned positive parent did not open its finalizer")
        };

        assert_eq!(
            original_seed.state.accepted.as_ref(),
            &AHashSet::from_iter([two])
        );
        assert_eq!(
            cloned_seed.state.accepted.as_ref(),
            &AHashSet::from_iter([one])
        );
        assert!(original.state.activations[&activation]
            .positive_publication
            .is_none());
        assert!(cloned.state.activations[&activation]
            .positive_publication
            .is_none());
    }

    #[test]
    fn positive_empty_confirm_fences_before_eager_completion() {
        let candidate = value(71);
        let mut registry = ProducerRegistry::new();
        let (activation, parent) =
            open_positive_confirm(&mut registry, [], terminal_positive_certificate());
        let open = registry
            .positive_publication_snapshot(parent)
            .expect("empty eligible Confirm should still own an open ledger");
        assert!(registry
            .open_positive_support_activation(
                parent,
                0,
                candidate,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());
        let proof = quiesce_confirm_with_accepted(&mut registry, activation, []);

        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(proof) else {
            panic!("empty Confirm unexpectedly opened finalizer work")
        };
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
        assert!(!registry.is_live(activation));
        assert!(
            registry.state.next_positive_generation > open.generation,
            "eager completion must generation-fence its empty publication domain"
        );
        assert!(registry
            .open_positive_support_activation(
                parent,
                0,
                candidate,
                VariableSet::new_singleton(0),
                Some(terminal_positive_full()),
            )
            .is_none());
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
        let FiniteFormulaNodeKind::And { children } = &plan.finite_formula.node(root).kind else {
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
            std::iter::empty(),
            [value(8), value(9), value(9)],
            [value(7), value(7), value(8)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.raw_proposal_occurrences, 6);
        assert_eq!(first.accepted.as_slice(), [value(7), value(8), value(9)]);
        assert!(first.quiescence.is_none());

        let second = registry.replace_program(
            second_credit,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [value(7), value(11), value(11)],
            [value(8), value(10), value(10)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(second.raw_proposal_occurrences, 6);
        assert_eq!(second.accepted.as_slice(), [value(10), value(11)]);
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
            std::iter::empty::<RawInline>(),
            [value(7)],
            false,
            false,
            false,
            None,
        );
        assert_eq!(sibling.raw_proposal_occurrences, 1);
        assert_eq!(sibling.accepted.as_slice(), [value(7)]);
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
                std::iter::empty::<RawInline>(),
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
                assert_eq!(child.accepted.as_slice(), [value(7)]);
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
        let (popped_state, hot, _support_grants, dispatch) =
            scheduler.pop_active_program(active, 1);
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

    #[test]
    fn dense_program_worklist_tracks_live_states_across_bitset_holes() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut tasks = install_program_tasks(
            &mut registry,
            activation,
            0..6,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let low = DeltaStateId(1);
        let middle = DeltaStateId(65);
        let high = DeltaStateId(130);
        let mut worklist = ProgramWorklist::default();

        let mut empty = Vec::new();
        worklist.append(high, &mut empty);
        assert!(worklist.is_empty());
        assert!(worklist.buckets.is_empty());

        for state in [middle, low, high] {
            let mut filed = vec![tasks.next().unwrap()];
            worklist.append(state, &mut filed);
            assert!(filed.is_empty());
        }
        assert_eq!(worklist.len(), 3);
        assert_eq!(worklist.last_id(), Some(high));
        assert_eq!(
            worklist.iter().map(|(state, _)| state).collect::<Vec<_>>(),
            [low, middle, high]
        );

        let high_capacity = worklist[&high].tasks.capacity();
        worklist.get_mut(&high).unwrap().tasks.clear();
        worklist.deactivate(high);
        assert_eq!(worklist.len(), 2);
        assert!(!worklist.contains_key(&high));
        assert_eq!(worklist.last_id(), Some(middle));

        let mut refiled = vec![tasks.next().unwrap()];
        worklist.append(high, &mut refiled);
        assert_eq!(worklist[&high].tasks.capacity(), high_capacity);
        assert_eq!(worklist.last_id(), Some(high));

        worklist.get_mut(&middle).unwrap().tasks.clear();
        worklist.deactivate(middle);
        assert_eq!(worklist.last_id(), Some(high));
        assert_eq!(
            worklist.iter().map(|(state, _)| state).collect::<Vec<_>>(),
            [low, high]
        );

        for state in [high, low] {
            worklist.get_mut(&state).unwrap().tasks.clear();
            worklist.deactivate(state);
        }
        assert!(worklist.is_empty());
        assert_eq!(worklist.last_id(), None);
        assert!(worklist.iter().next().is_none());
    }

    #[test]
    fn dense_program_worklist_membership_matches_ordered_reference() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut tasks = install_program_tasks(
            &mut registry,
            activation,
            0..2048,
            DispatchClass::new(0),
            ProgramPacing::Activation,
        )
        .into_iter();
        let mut worklist = ProgramWorklist::default();
        let mut reference = BTreeSet::new();
        let mut random = 0xA076_1D64_78BD_642Fu64;

        for _ in 0..1024 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let state = DeltaStateId(((random >> 24) % 257) as u32);
            if random & 3 == 0 && reference.remove(&state) {
                worklist.get_mut(&state).unwrap().tasks.clear();
                worklist.deactivate(state);
            } else {
                let mut filed = vec![tasks.next().expect("fixture exhausted Program tasks")];
                worklist.append(state, &mut filed);
                reference.insert(state);
            }

            assert_eq!(worklist.len(), reference.len());
            assert_eq!(worklist.is_empty(), reference.is_empty());
            assert_eq!(worklist.last_id(), reference.last().copied());
            assert_eq!(
                worklist.iter().map(|(state, _)| state).collect::<Vec<_>>(),
                reference.iter().copied().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn exhausted_program_bucket_reactivates_nested_work_before_local_replacements() {
        let mut scheduler = DeltaScheduler::new();
        let state = test_program_state(&mut scheduler);
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut tasks = install_program_tasks(
            &mut scheduler.registry,
            activation,
            0..4,
            DispatchClass::new(0),
            ProgramPacing::Search,
        )
        .into_iter();
        let initial = vec![tasks.next().unwrap(), tasks.next().unwrap()];
        let nested = tasks.next().unwrap();
        let local = tasks.next().unwrap();
        let nested_nonce = nested.credit.key.nonce;
        let local_nonce = local.credit.key.nonce;
        let active = scheduler.file_program_state(state, initial).unwrap();

        let (_, selected, _, _) = scheduler.pop_active_program(active, 2);
        assert_eq!(selected.len(), 2);
        assert!(!scheduler.program_worklist.contains_key(&state));
        let parked_capacity = scheduler.program_worklist.buckets[state.0 as usize]
            .tasks
            .capacity();
        assert!(parked_capacity >= 2);

        let _ = scheduler.file_program_state(state, vec![nested]);
        let _ = scheduler.file_program_state(state, vec![local]);

        assert_eq!(scheduler.program_worklist.len(), 1);
        assert_eq!(
            scheduler.program_worklist[&state]
                .tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            [nested_nonce, local_nonce]
        );
        assert_eq!(
            scheduler.program_worklist[&state].tasks.capacity(),
            parked_capacity,
            "reactivation should reuse the exhausted bucket allocation"
        );
    }

    #[test]
    fn scheduler_clone_does_not_resurrect_inactive_program_bucket_capacity() {
        let mut scheduler = DeltaScheduler::new();
        let dormant_state = test_program_state(&mut scheduler);
        let active_route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        };
        let active_state = scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 1), active_route));

        let dormant_activation = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let dormant_tasks = install_program_tasks(
            &mut scheduler.registry,
            dormant_activation,
            [0],
            DispatchClass::new(0),
            ProgramPacing::Activation,
        );
        let dormant_active = scheduler
            .file_program_state(dormant_state, dormant_tasks)
            .unwrap();
        let (_, mut popped, _, _) = scheduler.pop_active_program(dormant_active, 1);
        let dormant_capacity = scheduler.program_worklist.buckets[dormant_state.0 as usize]
            .tasks
            .capacity();
        assert!(dormant_capacity > 0);
        let dormant_task = popped.pop().unwrap();
        let retired = scheduler.registry.replace_program(
            dormant_task.credit,
            dormant_state,
            &[],
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            std::iter::empty::<RawInline>(),
            false,
            false,
            false,
            None,
        );
        let completed = scheduler
            .registry
            .finish(retired.quiescence.expect("dormant activation must retire"));
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);

        let active_activation = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let active_tasks = install_program_tasks(
            &mut scheduler.registry,
            active_activation,
            [1],
            DispatchClass::new(0),
            ProgramPacing::Activation,
        );
        let _ = scheduler.file_program_state(active_state, active_tasks);
        let cloned = scheduler.clone();

        assert_eq!(scheduler.program_worklist.len(), 1);
        assert_eq!(cloned.program_worklist.len(), 1);
        assert!(!scheduler.program_worklist.contains_key(&dormant_state));
        assert!(!cloned.program_worklist.contains_key(&dormant_state));
        assert!(scheduler.program_worklist.contains_key(&active_state));
        assert!(cloned.program_worklist.contains_key(&active_state));
        assert_eq!(
            scheduler.program_worklist.buckets[dormant_state.0 as usize]
                .tasks
                .capacity(),
            dormant_capacity
        );
        assert_eq!(
            cloned.program_worklist.buckets[dormant_state.0 as usize]
                .tasks
                .capacity(),
            0,
            "clone should not copy dormant high-water capacity"
        );
        assert_ne!(
            scheduler.program_worklist[&active_state].tasks[0]
                .credit
                .brand,
            cloned.program_worklist[&active_state].tasks[0].credit.brand,
            "clone must still rebrand the one live Program credit"
        );
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
        let (popped_state, selected, _support_grants, dispatch) = if active_pop {
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

        let (popped_state, tasks, _support_grants, dispatch) = scheduler.pop_program_bounded(4);
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

        let (popped_state, tasks, _support_grants, dispatch) = scheduler.pop_program_bounded(4);
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
            scheduler.program_worklist[&state].tasks[0].credit.key.nonce,
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

        let (popped_state, tasks, _support_grants, dispatch) = scheduler.pop_program_bounded(8);
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
            states: &mut Vec<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            self.fill_step(&states, batch, effects);
        }

        fn try_step_physical(
            &self,
            states: &[Self::State],
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) -> Option<ProgramPhysicalReceipt> {
            if !self.physical {
                return None;
            }
            let placement = ProgramPhysicalReceipt::new("test-physical", "one-shot-confirm");
            self.fill_step(states, batch, effects);
            Some(placement)
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
                None,
                &mut ResidualStateStats::default(),
            )
            .active
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

        let wide_capacities = {
            let scratch = scheduler
                .program_scratch
                .as_ref()
                .expect("wide graph cohort warmed scheduler scratch");
            assert!(scratch.parents.is_empty());
            assert!(scratch.vars.is_empty());
            assert!(scratch.activations.is_empty());
            assert!(scratch.task_receipts.is_empty());
            assert!(scratch.work.is_empty());
            assert!(scratch.receipt.pages.is_empty());
            assert!(scratch.receipt.children.is_empty());
            assert!(scratch.receipt.direct.is_empty());
            assert!(scratch.receipt.accepted.is_empty());
            assert!(scratch.receipt.supported.is_empty());
            assert!(scratch.child_ranges.is_empty());
            assert!(scratch.direct_ranges.is_empty());
            assert!(scratch.accepted_ranges.is_empty());
            assert!(scratch.supported_ranges.is_empty());
            assert!(scratch.retired_activations.is_empty());
            (
                scratch.receipt.pages.capacity(),
                scratch.child_ranges.capacity(),
                scratch.direct_ranges.capacity(),
                scratch.accepted_ranges.capacity(),
                scratch.supported_ranges.capacity(),
            )
        };
        let cold_clone = scheduler.clone();
        assert!(scheduler.program_scratch.is_some());
        assert!(cold_clone.program_scratch.is_none());

        // The graph cohort above was two rows wide. Limit the first finalizer
        // pop to one row so every retained receipt and tag-range buffer is
        // exercised wide -> narrow on the same scratch allocation.
        let first_finalized = scheduler.step_bounded(
            &root,
            &plan,
            1,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert_eq!(first_finalized.completed_activation_ids.len(), 1);
        assert_eq!(first_finalized.completed_activations, 1);
        assert!(!first_finalized.completed_transition_cohort);
        assert!(first_finalized.continuation.is_some());
        assert_eq!(scheduler.program_worklist.len(), 1);
        let scratch = scheduler.program_scratch.as_ref().unwrap();
        assert!(scratch.parents.is_empty());
        assert!(scratch.vars.is_empty());
        assert!(scratch.activations.is_empty());
        assert!(scratch.task_receipts.is_empty());
        assert!(scratch.work.is_empty());
        assert!(scratch.receipt.pages.is_empty());
        assert!(scratch.receipt.children.is_empty());
        assert!(scratch.receipt.direct.is_empty());
        assert!(scratch.receipt.accepted.is_empty());
        assert!(scratch.receipt.supported.is_empty());
        assert!(scratch.child_ranges.is_empty());
        assert!(scratch.direct_ranges.is_empty());
        assert!(scratch.accepted_ranges.is_empty());
        assert!(scratch.supported_ranges.is_empty());
        assert!(scratch.retired_activations.is_empty());
        assert!(scratch.receipt.pages.capacity() >= wide_capacities.0);
        assert!(scratch.child_ranges.capacity() >= wide_capacities.1);
        assert!(scratch.direct_ranges.capacity() >= wide_capacities.2);
        assert!(scratch.accepted_ranges.capacity() >= wide_capacities.3);
        assert!(scratch.supported_ranges.capacity() >= wide_capacities.4);

        let second_finalized = scheduler.step_bounded(
            &root,
            &plan,
            1,
            None,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        let mut completed_ids = first_finalized.completed_activation_ids.clone();
        completed_ids.extend(second_finalized.completed_activation_ids.iter().copied());
        completed_ids.sort_unstable();
        assert_eq!(completed_ids, activation_ids);
        assert_eq!(second_finalized.completed_activations, 1);
        assert!(!second_finalized.completed_transition_cohort);
        assert!(second_finalized.continuation.is_some());
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
        original.extend_same_domain(shared_one_parent_candidates(vec![rejected, a, b]), 1);
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
                None,
                &mut ResidualStateStats::default(),
            )
            .active
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
                None,
                &mut ResidualStateStats::default(),
            )
            .active
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

        let (popped_state, tasks, _support_grants, dispatch) = scheduler.pop_program_bounded(8);
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

        let (popped_state, tasks, _support_grants, dispatch) = scheduler.pop_program_bounded(8);
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
        let relevant = (0..3).fold(ChildSet::empty(3), |set, leaf| set.with_inserted(leaf));
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
        let StateBucket::Candidates(batch) =
            stable.values().flat_map(BTreeMap::values).next().unwrap()
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
        assert!(
            stable.is_empty(),
            "no partial relation may re-enter Candidate"
        );
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
        let StateBucket::Candidates(batch) =
            stable.values().flat_map(BTreeMap::values).next().unwrap()
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
        let active = seeded
            .active
            .expect("SET admission opened a Program cohort");
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
        assert!(
            scheduler.is_empty(),
            "unit grants did not drain SET admission"
        );
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
                original: shared_one_parent_candidates(vec![second, seed, first, rejected, second]),
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

        assert_eq!(
            scheduler.interner.entries,
            vec![DeltaStateEntry::Legacy(desc)]
        );
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
