//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

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
/// The structural site and semantic variable distinguish repeated references
/// and orientations. Route-specific future computation lives in typed state,
/// so compatible routes of this occurrence deliberately share one runtime.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum ProgramAddress {
    /// A route owned by one structural constraint occurrence.
    Constraint(DeltaDesc),
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
        Self::Constraint(desc)
    }

    fn resolve<'r, 'a>(&self, root: &'r dyn Constraint<'a>, plan: &ResidualPlan) -> ProgramRef<'r> {
        match self {
            Self::Constraint(desc) => desc
                .resolve(root, plan)
                .residual_program()
                .expect("constructed typed program disappeared during execution"),
            Self::Engine(kind) => kind.resolve(),
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
    type NoveltyKey = std::convert::Infallible;
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
    type NoveltyKey = std::convert::Infallible;
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
    type NoveltyKey = std::convert::Infallible;
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
    type NoveltyKey = std::convert::Infallible;
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
    type NoveltyKey = std::convert::Infallible;
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

#[derive(Clone, Default)]
struct DeltaInterner {
    by_program: AHashMap<ProgramAddress, DeltaStateId>,
    entries: Vec<ProgramAddress>,
}

impl DeltaInterner {
    fn intern_program(&mut self, address: ProgramAddress) -> DeltaStateId {
        if let Some(&id) = self.by_program.get(&address) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.entries.len()).expect("too many program states"));
        self.entries.push(address.clone());
        self.by_program.insert(address, id);
        id
    }

    fn program(&self, id: DeltaStateId) -> Option<&ProgramAddress> {
        self.entries.get(id.0 as usize)
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
    continuation: ContinuationPublicationReceipt,
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
            continuation: continuation_publication_receipt(
                previous,
                successor,
                full,
                plan,
                formula_pcs,
            ),
        }
    }

    fn eligible(self) -> bool {
        match self.continuation {
            ContinuationPublicationReceipt::Terminal
            | ContinuationPublicationReceipt::RelationalPrefix => true,
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
    RelationalPrefix,
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
    /// continuation whose exact PC has been proved linear and free of a live
    /// OR-frame or activation-reuse barrier.
    /// Emission inherits the ordinary streaming proposal's discovery order;
    /// only bag equality with the sorted quiescent formula result is promised.
    StreamFormulaProposal,
    /// Accepted values first enter the master OR accumulator, then novel
    /// admissions may leave through this effect-only completed OR Plan. The
    /// master action itself advances exactly once when the producer reaches
    /// EOF.
    StreamFormulaOrProposal { exit: FormulaPcId },
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
        matches!(
            self,
            Self::StreamProposal
                | Self::StreamFormulaProposal
                | Self::StreamFormulaOrProposal { .. }
        )
    }

    fn formula_proposal(streaming: FormulaProposalStreaming) -> Self {
        match streaming {
            FormulaProposalStreaming::Quiescent => Self::quiescent_proposal(),
            FormulaProposalStreaming::Linear => Self::StreamFormulaProposal,
            FormulaProposalStreaming::OnlineDirectOr { exit } => {
                Self::StreamFormulaOrProposal { exit }
            }
        }
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
/// Stable formula cursors intentionally live here rather than in [`DeltaDesc`]:
/// two activations may expand the same RPQ product kernel while returning to
/// different arena-interned ancestor states and OR-reducer payloads.
#[derive(Clone)]
enum DeltaReturn {
    Stable {
        desc: StateDesc,
        parent: Box<[RawInline]>,
        /// The cyclic action result crosses from an occurrence bag into a
        /// candidate continuation that may split or commit independently.
        /// Cyclic Confirm computes this receipt with the shared stable-state
        /// boundary predicate before opening graph traversal.
        set_admit_result: bool,
    },
    Formula {
        bound: VariableSet,
        cursor: FormulaCursor,
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
        cursor: FormulaCursor,
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
    /// Sorted distinct input relation retained by a confirmation activation.
    /// Proposals own a constraint-generated graph frontier and therefore store
    /// `None`.
    source_candidates: Option<Box<[RawInline]>>,
    program_joins: AHashMap<ProgramJoinId, ProgramJoin>,
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
    FormulaOrCandidates { exit: FormulaPcId },
    Support,
}

struct DeltaStreamingReturn {
    /// Online OR effects intentionally carry no pre-admission payload clone;
    /// their exact return is cloned from the master only after first
    /// admission succeeds.
    return_to: Option<DeltaReturn>,
    effect: DeltaStreamingEffect,
}

/// Affine sparse-pacing authority delayed until one activation's staged rows
/// either claim a novel raw projection or exhaust their physical suffix.
#[derive(Clone, Debug)]
pub(super) struct TerminalProjectionFeedback {
    pub(super) activation: ActivationId,
    pub(super) last_row: usize,
    /// Original transition search ceiling when a saturated miss may widen;
    /// `None` makes a duplicate exhaustion a consumed but neutral receipt.
    widen_to: Option<usize>,
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
    /// Physical row origins. Positive Confirm/Support rows may carry a
    /// different semantic activation in `origins`.
    pub(super) physical_origins: SmallVec<[ActivationId; 1]>,
    pub(super) projection_feedback: SmallVec<[TerminalProjectionFeedback; 1]>,
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
        let row_count = rows.row_count;
        let mut origins = SmallVec::new();
        origins.resize(row_count, activation);
        let mut registrations = SmallVec::new();
        registrations.extend(registration);
        let physical_origins = origins.clone();
        Self {
            rows,
            origins,
            registrations,
            physical_origins,
            projection_feedback: SmallVec::new(),
        }
    }

    fn append(&mut self, mut other: Self) {
        assert!(
            self.projection_feedback.is_empty() && other.projection_feedback.is_empty(),
            "terminal batches append before dispatch feedback is attached"
        );
        self.rows.append(other.rows);
        self.origins.extend(other.origins.drain(..));
        self.registrations.extend(other.registrations.drain(..));
        self.physical_origins
            .extend(other.physical_origins.drain(..));
        debug_assert_eq!(self.origins.len(), self.rows.row_count);
        debug_assert_eq!(self.physical_origins.len(), self.rows.row_count);
    }

    /// Reattributes a just-created positive publication from its semantic
    /// Confirm parent to the child activation that physically produced it.
    fn set_physical_origin(&mut self, activation: ActivationId) {
        self.physical_origins.fill(activation);
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

    fn with_physical_origin(mut self, activation: ActivationId) -> Self {
        if let Some(publication) = &mut self.publication {
            publication.set_physical_origin(activation);
        }
        self
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
                        program_joins: AHashMap::new(),
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
        ledger.support_work.settle(grant.granted, examined);
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
        let DeltaReturn::Stable { desc, parent, .. } = &activation.return_to else {
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
            ContinuationPublicationReceipt::RelationalPrefix => {
                if !matches!(&desc.phase, ResidualPhase::Candidate { .. }) {
                    return None;
                }
                PositivePublicationRoute::RelationalPrefix
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
                        program_joins: AHashMap::new(),
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
                (
                    DeltaReducer::StreamProposal
                    | DeltaReducer::StreamFormulaProposal
                    | DeltaReducer::StreamFormulaOrProposal { .. },
                    _,
                ) => {
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
                DeltaReducer::StreamProposal
                | DeltaReducer::StreamFormulaProposal
                | DeltaReducer::StreamFormulaOrProposal { .. } => !accepted.is_empty(),
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
        } else if matches!(kind, PhysicalDispatchKind::Program) {
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
            DeltaReducer::StreamFormulaOrProposal { exit } => {
                assert!(matches!(&activation.return_to, DeltaReturn::Formula { .. }));
                DeltaStreamingEffect::FormulaOrCandidates { exit: *exit }
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
        let return_to = (!matches!(effect, DeltaStreamingEffect::FormulaOrCandidates { .. }))
            .then(|| activation.return_to.clone());
        Some(DeltaStreamingReturn { return_to, effect })
    }

    /// Filters one accepted endpoint page through the live master OR frame
    /// before any effect clone can observe it. The returned payload clone is
    /// taken only after mutation, so later arms and the EOF continuation share
    /// the same first-admission history without mutable aliases.
    fn publish_formula_or_candidates(
        &mut self,
        activation: ActivationId,
        accepted: &mut Vec<RawInline>,
    ) -> Option<DeltaReturn> {
        let activation = self
            .state
            .activations
            .get_mut(&activation)
            .expect("unknown delta activation");
        assert!(matches!(
            activation.reducer,
            DeltaReducer::StreamFormulaOrProposal { .. }
        ));
        let DeltaReturn::Formula { batch, .. } = &mut activation.return_to else {
            panic!("an online Formula OR reducer lost its Formula return")
        };
        assert_eq!(
            batch.parents.row_count, 1,
            "an online Formula OR reducer must own exactly one affine parent"
        );
        accepted.retain(|&value| batch.publish_current_or_value(0, value));
        (!accepted.is_empty()).then(|| activation.return_to.clone())
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
            return_to: Some(activation.return_to.clone()),
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
            DeltaReducer::StreamFormulaOrProposal { .. } => {
                DeltaCompletion::Candidates(CandidatePayload::empty(1))
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
    /// transfer whenever their live candidate stream can be finalized
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
    /// the same progression as the typed activation frontier.
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
                    PhysicalDispatchKind::Program => {
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
    /// The activation still owns scheduled typed Program work.
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
    /// live. Most steps retain their input state; a Program-quiescence handoff
    /// carries the explicitly settled engine-Program state instead.
    pub(super) resume: Option<ActiveDeltaContinuation>,
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
    /// One unified queue of opaque typed continuations. Source generation and
    /// product expansion are family-private states distinguished only by
    /// opaque physical dispatch classes.
    program_worklist: ProgramWorklist,
    /// Affine PositiveSupport custody that is live but not runnable.
    ///
    /// Parked hedges do not own semantic completeness: their exact Confirm
    /// parents remain on the ordinary runnable frontier. Keeping the opaque
    /// handles in the same state-indexed shape lets cancellation and cloning
    /// preserve their exact typed runtime without exposing them to global pop.
    parked_positive_support_worklist: ProgramWorklist,
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
    /// Number of independent quiescent activations that may share one Program
    /// cohort. This grows only when activations complete; `width` remains the
    /// separate intra-activation page/work budget.
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
            program_worklist: ProgramWorklist::default(),
            parked_positive_support_worklist: ProgramWorklist::default(),
            public_pull_demand: PublicPullDemandState::Closed,
            program_runtimes: AHashMap::new(),
            program_scratch: None,
            activation_width: INITIAL_RESIDUAL_WIDTH,
            terminal_selection_slots: AHashMap::new(),
            terminal_selections: Vec::new(),
        }
    }

    pub(super) fn grow_activation_width(&mut self) -> bool {
        let next = next_residual_width(self.activation_width);
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
        self.program_worklist.is_empty()
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
            .or_insert_with(|| spec.new_runtime());
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
                        spec.discard_work(runtime, program_activation, &seed.work);
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
            active: support_active.or(finalizer_active).or(graph_active),
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
        cursor: FormulaCursor,
        stage: FormulaStage,
        batch: FormulaBatch,
        proposal_streaming: FormulaProposalStreaming,
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
                FormulaStage::Propose => DeltaReducer::formula_proposal(proposal_streaming),
                FormulaStage::Confirm => DeltaReducer::Confirm {
                    original: batch.take_contiguous_confirm_original(),
                },
            };
            activations.push(self.registry.open_program_activation(
                reducer,
                DeltaReturn::Formula {
                    bound,
                    cursor,
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
                    if input.is_empty() {
                        seed.destination.install_candidates(input);
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
                    }
                    let program_state = SetAdmissionState::start(input)
                        .expect("nonempty SET-admission input starts a Program");

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
                    assert!(
                        !seed.batch.has_current(),
                        "Formula OR admission retained its input in current"
                    );
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
                    assert!(
                        !seed.batch.has_current(),
                        "Formula OR admission retained its input in current"
                    );
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
                            cursor: seed.cursor,
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
                                seed.cursor,
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
                            cursor: seed.cursor,
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
                    cursor,
                    batch,
                },
                DeltaCompletion::Candidates(result),
            ) => {
                if matches!(
                    &stable_interner.formula(cursor).focus,
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
                    cursor,
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
                    cursor,
                },
                DeltaCompletion::Candidates(result),
            ) => {
                let mut seeds = Vec::new();
                let continuation = finish_formula_or_emission(
                    plan,
                    bound,
                    cursor,
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
            PositivePublicationRoute::RelationalPrefix => {
                stats.delta_positive_publication_relational_prefix_commits += 1;
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
                .expect("a positive RelationalPrefix singleton filed no continuation");
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
        mut accepted: Vec<RawInline>,
        direct_terminal_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStreamingRelease {
        if streamed.effect == DeltaStreamingEffect::Support {
            let released = self.release_support(
                streamed
                    .return_to
                    .expect("a streaming support effect lost its exact return"),
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
        // Callers account raw occurrences minus activation-local Accepted;
        // preserve that identity even when master OR admission suppresses an
        // endpoint already published by another arm.
        stats.candidates_proposed += accepted.len();
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted.len());
        let effect = streamed.effect;
        let return_to = if matches!(effect, DeltaStreamingEffect::FormulaOrCandidates { .. }) {
            let Some(return_to) = self
                .registry
                .publish_formula_or_candidates(activation, &mut accepted)
            else {
                return DeltaStreamingRelease {
                    stable: DeltaStableEffects::default(),
                    active: None,
                };
            };
            return_to
        } else {
            streamed
                .return_to
                .expect("an ordinary streaming effect lost its exact return")
        };
        debug_assert!(!accepted.is_empty());
        let candidates = CandidatePayload::Values(accepted);
        if let Some(full) = direct_terminal_full {
            let DeltaReturn::Stable { desc, parent, .. } = return_to else {
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
        if let DeltaStreamingEffect::FormulaOrCandidates { exit } = effect {
            let DeltaReturn::Formula {
                bound,
                cursor,
                batch,
            } = return_to
            else {
                panic!("an online Formula OR effect lost its Formula return")
            };
            let mut reducer_seeds = Vec::new();
            let continuation = finish_formula_or_emission(
                plan,
                bound,
                cursor.with_pc(exit),
                batch,
                candidates,
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
            return DeltaStreamingRelease {
                stable: DeltaStableEffects {
                    continuation: drained.continuation,
                    publication: None,
                },
                active: drained.active,
            };
        }
        let mut reducer_seeds = Vec::new();
        let continuation = match return_to {
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
                cursor,
                batch,
            } => finish_formula_action_result(
                plan,
                bound,
                cursor,
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
            "a relational streaming Formula proposal reached an OR reducer"
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
            cursor,
            batch,
        } = return_to
        else {
            panic!("delta support returned to a candidate continuation")
        };
        assert!(matches!(
            &stable_interner.formula(cursor).focus,
            FormulaFocus::Action {
                stage: FormulaStage::Support,
                ..
            }
        ));
        let completed = cursor.with_pc(
            stable_interner
                .formula_pcs
                .complete(&plan.finite_formula, cursor.pc),
        );
        let desc = StateDesc {
            bound,
            phase: ResidualPhase::Formula { cursor },
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

    fn file_program_state(
        &mut self,
        state: DeltaStateId,
        mut tasks: Vec<ProgramTask>,
    ) -> Option<ActiveDeltaContinuation> {
        let activation = tasks.last()?.activation;
        assert!(
            self.interner.program(state).is_some(),
            "typed program task was filed under an unknown state"
        );
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
            "parked positive Support task was filed under an unknown state"
        );
        assert!(
            tasks.iter().all(|task| {
                task.activation == task.credit.key.activation
                    && self.registry.is_live_positive_support(task.activation)
            }),
            "only live PositiveSupport Program tasks may enter the parked lane"
        );
        self.parked_positive_support_worklist
            .append(state, &mut tasks);
        Some(ActiveDeltaContinuation { state, activation })
    }

    fn has_runnable_positive_support_parent(&self, parent: PositiveConfirmParentId) -> bool {
        self.program_worklist.iter().any(|(_, bucket)| {
            bucket.tasks.iter().any(|task| {
                self.registry
                    .positive_support_parent_for_child(task.activation)
                    == Some(parent)
            })
        })
    }

    /// Moves at most one parked task for this semantic parent onto the
    /// runnable frontier. Parent-local available credit and the absence of an
    /// existing runnable sibling are revalidated at the move boundary.
    fn wake_one_positive_support_parent(
        &mut self,
        parent: PositiveConfirmParentId,
    ) -> Option<ActiveDeltaContinuation> {
        if self.registry.positive_support_budget_available(parent) == 0
            || self.has_runnable_positive_support_parent(parent)
        {
            return None;
        }
        let registry = &self.registry;
        let (state, task) = self
            .parked_positive_support_worklist
            .take_one_matching(|task| {
                registry.positive_support_parent_for_child(task.activation) == Some(parent)
            })?;
        let activation = task.activation;
        let mut tasks = vec![task];
        self.program_worklist.append(state, &mut tasks);
        Some(ActiveDeltaContinuation { state, activation })
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
        let mut parents = Vec::new();
        let mut seen = AHashSet::new();
        for (_, bucket) in self.parked_positive_support_worklist.iter() {
            for task in &bucket.tasks {
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
            if !self.registry.mint_positive_support_demand(parent) {
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
        let groups = self.program_worklist.take_activations(activations);
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
        for (state, mut tasks) in self
            .program_worklist
            .take_activations(&live)
            .into_iter()
            .chain(
                self.parked_positive_support_worklist
                    .take_activations(&live),
            )
        {
            groups.entry(state).or_default().append(&mut tasks);
        }
        let mut completed = Vec::with_capacity(live.len());
        for (state, tasks) in groups {
            let address = self
                .interner
                .program(state)
                .cloned()
                .expect("cancelled PositiveSupport work occupied an unknown state");
            let spec = address.resolve(root, plan);
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
                    spec.discard_work(runtime, program_activation, &task.work);
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
                spec.retire_activations(runtime, &retired);
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
        mut publication: Option<&mut TerminalPublicationBatch>,
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
            // A direct terminal row crosses one more semantic boundary before
            // it can prove novelty: the public raw ProjectionGate. Transfer
            // reset/miss authority affinely with the staged batch.
            let delayed = published
                && publication.as_ref().is_some_and(|publication| {
                    publication.physical_origins.contains(&receipt.activation)
                });
            let (reset, widened) = if delayed {
                let publication = publication
                    .as_mut()
                    .expect("delayed feedback lost its terminal publication");
                let last_row = publication
                    .physical_origins
                    .iter()
                    .rposition(|&origin| origin == receipt.activation)
                    .unwrap();
                assert!(
                    !publication
                        .projection_feedback
                        .iter()
                        .any(|feedback| feedback.activation == receipt.activation),
                    "one dispatch attached two projection receipts to one activation"
                );
                publication
                    .projection_feedback
                    .push(TerminalProjectionFeedback {
                        activation: receipt.activation,
                        last_row,
                        widen_to: (dispatch.kind == PhysicalDispatchKind::Program
                            && receipt.assigned >= receipt.quantum)
                            .then_some(search_width),
                    });
                (false, false)
            } else if published || receipt.assigned >= receipt.quantum {
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

    pub(super) fn settle_terminal_projection_feedback(
        &mut self,
        receipt: TerminalProjectionFeedback,
        claimed: bool,
        stats: &mut ResidualStateStats,
    ) {
        let (search_width, kind) = if claimed {
            (1, PhysicalDispatchKind::Source)
        } else {
            let Some(search_width) = receipt.widen_to else {
                return;
            };
            (search_width, PhysicalDispatchKind::Program)
        };
        let (reset, widened) =
            self.registry
                .finish_dispatch(receipt.activation, search_width, kind, claimed);
        stats.delta_terminal_sparse_resets += usize::from(reset);
        stats.delta_terminal_sparse_widenings += usize::from(widened);
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
        let id = self
            .program_worklist
            .last_id()
            .expect("typed program pop requires live work");
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
            self.program_worklist.deactivate(id);
        }
        let ProgramSelection {
            key,
            tasks,
            mut limits,
        } = selection;
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
            let grant = self
                .registry
                .reserve_positive_support_work(task.activation, *limit)
                .expect("runnable PositiveSupport task had no available work allowance");
            *limit = grant.granted;
            grants.push(Some(grant));
        }
        grants
    }

    /// Advances only the affine typed Program activation named by a physical
    /// continuation.
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
        let has_program = self.has_active_program(active);
        let has_parked_program = self.has_active_parked_positive_support(active);
        if has_parked_program {
            assert!(
                !has_program,
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
            has_program,
            "active Program continuation has no scheduled affine task"
        );

        // Keep the raw semantic full-result receipt alive through dispatch.
        // Ordinary paths still gate it by their physical activation class at
        // the exact release site; Stage 2's positive publication transaction
        // will instead validate its semantic Confirm parent.
        let direct_terminal_full = direct_terminal_publication_full;
        let examined_before = stats
            .delta_source_candidates_examined
            .saturating_add(stats.delta_transition_candidates_examined);
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
            outcome.publication.as_mut(),
            stats,
        );
        outcome.allows_global_width_growth =
            retired_search_receipt || physical_allows_global_width_growth;
        let yielded = outcome.has_stable_effect();
        let live = self.registry.is_live(active.activation);
        let settled = outcome.retargeted.get(&active.activation).copied();
        // A completed graph/action activation may transfer its affine lineage
        // to a fresh engine reducer activation.  The explicit old -> new
        // receipt remains authoritative even though the old registry entry
        // has already been removed; queue liveness must not be used to infer
        // or discard that handoff.
        let runnable = self.has_active_program(active);
        let parked = self.has_active_parked_positive_support(active);
        debug_assert!(
            !runnable || !parked,
            "one activation remained both runnable and parked after a directed step"
        );
        let release_directed_lease = outcome.release_directed_lease;
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
        assert!(
            !self.program_worklist.is_empty(),
            "Program scheduler stepped without runnable work"
        );
        let (state, tasks, support_grants, dispatch) = self.pop_program_bounded(search_width);
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
        let retired_search_receipt = physical.retired_search_receipt;
        let mut outcome = physical.outcome;
        let physical_allows_global_width_growth = self.account_physical_dispatch(
            dispatch,
            search_width,
            examined_before,
            &physical.terminal_publications,
            outcome.publication.as_mut(),
            stats,
        );
        outcome.allows_global_width_growth =
            retired_search_receipt || physical_allows_global_width_growth;
        outcome
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
            .expect("typed program task was scheduled under an unknown state");
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
        spec.step_batch(
            self.program_runtimes
                .get_mut(&state)
                .expect("typed program state lost its runtime"),
            ProgramBatch {
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
            && matches!(&address, ProgramAddress::Constraint(_))
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
        while receipt_local_fusion && total_examined < limits[0] {
            let exact_tail = scratch.receipt.pages.len() == 1
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
            spec.step_batch(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("typed program state lost its runtime during receipt-local fusion"),
                ProgramBatch {
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
            std::mem::swap(&mut scratch.receipt, &mut scratch.fused_receipt);
            scratch.fused_receipt.clear();
            fused_steps += 1;
        }
        let validated_examined = validated_program_examined(
            &scratch.receipt.pages,
            receipt_local_fusion.then_some(total_examined),
        );
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
                let parent = PositiveConfirmParentId {
                    brand: self.registry.brand,
                    activation: grant.parent,
                };
                stats.delta_positive_support_examined += self
                    .registry
                    .settle_positive_support_work(grant, activation, examined);
                // A short physical page refunds the unexamined reservation.
                // Reconsider this parent only after every selected receipt has
                // been replaced, refiled, and cancelled.
                credited_support_parents.insert(parent);
            } else {
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
                    task_effects.absorb(
                        Self::release_positive_publication(
                            grant,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        )
                        .with_physical_origin(activation),
                    );
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
                    task_effects.absorb(
                        Self::release_positive_publication(
                            grant,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        )
                        .with_physical_origin(activation),
                    );
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
                &retired_activations,
            );
        }
        for parent in exhausted_support_parents {
            stats.delta_positive_support_credit_retired +=
                self.registry.retire_orphaned_positive_support_work(parent);
        }
        let demand_preference = self.assign_public_pull_demand(stats);
        let exact_credit_wake = self.wake_positive_support_parents(credited_support_parents);
        let release_directed_lease = directed_active
            && !directed_positive_support
            && (demand_preference.is_some() || exact_credit_wake);
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
        }
    }

    fn deep_clone(&self) -> Self {
        let (registry, mut remap) = self.registry.deep_clone();
        let mut program_worklist = ProgramWorklist::default();
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
        Self {
            registry,
            interner: self.interner.clone(),
            program_worklist,
            parked_positive_support_worklist,
            public_pull_demand: self.public_pull_demand,
            program_runtimes: self.program_runtimes.clone(),
            program_scratch: None,
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

    use crate::query::{
        intersectionconstraint::IntersectionConstraint, unionconstraint::UnionConstraint,
    };

    use super::*;

    fn test_formula_cursor(pc: u32) -> FormulaCursor {
        FormulaCursor {
            pc: FormulaPcId(pc),
            exit: CandidateExitId(0),
        }
    }

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
                variable: 0,
                grouping: ProgramGrouping::PageLocal,
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
                effects.fixpoint_root(parent as u32, ZeroProgressState(1), 0, None);
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

        fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
            if variable == 0 && !bound.is_set(variable) {
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

    #[test]
    fn one_occurrence_variable_has_one_runtime_across_route_groupings() {
        let root = ZeroProgressProgram;
        let spec = ProgramRef::new(&root);
        let desc = DeltaDesc::leaf(0, 7);
        let mut scheduler = DeltaScheduler::new();
        let page_state = scheduler.prepare_program(
            desc.clone(),
            ProgramRoute {
                variable: 0,
                grouping: ProgramGrouping::PageLocal,
            },
            spec,
        );
        let atomic_state = scheduler.prepare_program(
            desc.clone(),
            ProgramRoute {
                variable: 0,
                grouping: ProgramGrouping::ParentAtomic,
            },
            spec,
        );

        assert_eq!(atomic_state, page_state);
        assert_eq!(scheduler.program_runtimes.len(), 1);
        assert_eq!(
            scheduler.interner.program(page_state),
            Some(&ProgramAddress::Constraint(desc))
        );
    }

    #[derive(Clone, Copy)]
    struct OneShotSupportState {
        keep_cleanup_live: bool,
        accept_candidate: bool,
        report_support: bool,
    }

    #[derive(Clone, Copy)]
    struct OneShotSupportProgram;

    impl TypedProgramSpec for OneShotSupportProgram {
        type State = OneShotSupportState;
        type NoveltyKey = ();
        type Rank = u8;

        fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
            matches!(request.action, ProgramAction::Support).then_some(ProgramRoute {
                variable: 0,
                grouping: ProgramGrouping::PageLocal,
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
                    1,
                    state.keep_cleanup_live.then_some(TypedResume::Immediate(
                        OneShotSupportState {
                            keep_cleanup_live: false,
                            accept_candidate: state.accept_candidate,
                            report_support: state.report_support,
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

    #[test]
    fn parked_positive_support_releases_its_lease_while_exact_remains_runnable() {
        let root = OneShotSupportProgram;
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
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
        .solve_residual_state_lazy();
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
                variable: 0,
                grouping: ProgramGrouping::PageLocal,
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
                        | ReceiptProbeMode::AlternatingDispatch,
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
            let plan = ResidualPlan::compile_production(&root);
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
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), None);
        fused_feedback.width = 8;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateChronology, 4);
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), None);
        unfused_feedback.width = 8;
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
    fn receipt_local_program_chain_keeps_a_duplicate_prefix_final_dead_page_visible() {
        let mut fused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateDeadTail, 2);
        fused.preaccept(value(2));
        let fused_outcome = fused.step(2);
        assert_eq!(fused.stats.delta_program_receipt_local_fused_steps, 1);
        assert_eq!(fused_outcome.outcome.dead_pages, 1);
        assert_eq!(fused_outcome.outcome.transition_dead_pages, 1);

        let mut fused_feedback =
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), None);
        fused_feedback.width = 2;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::DuplicateDeadTail, 2);
        unfused.preaccept(value(2));
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), None);
        unfused_feedback.width = 2;
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
            ResidualStateMachine::new(fused.root.variables(), fused.plan.len(), None);
        fused_feedback.width = 2;
        fused_feedback.account_delta_feedback(&fused_outcome.outcome);

        let mut unfused = ReceiptProbeHarness::new(ReceiptProbeMode::TransitionThenSourceDead, 2);
        let mut unfused_feedback =
            ResidualStateMachine::new(unfused.root.variables(), unfused.plan.len(), None);
        unfused_feedback.width = 2;
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
    }

    fn value(byte: u8) -> RawInline {
        [byte; 32]
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
    }

    impl Constraint<'static> for PositiveCertificateLeaf {
        fn variables(&self) -> VariableSet {
            VariableSet::new_singleton(self.variable)
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

    fn open_terminal_projection_feedback_activation(
        machine: &mut ResidualStateMachine,
        full: VariableSet,
        leaf_count: usize,
    ) -> ActivationId {
        let checked = ChildSet::empty(leaf_count);
        machine.delta.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            positive_candidate_return(
                vec![value(0)],
                VariableSet::new_singleton(0),
                1,
                checked.clone(),
                checked,
                false,
            ),
            None,
            Some(full),
        )
    }

    fn terminal_projection_feedback_fixture() -> (
        IntersectionConstraint<PositiveCertificateLeaf>,
        ResidualPlan,
        ResidualStateMachine,
        VariableSet,
    ) {
        let root = IntersectionConstraint::new(vec![
            PositiveCertificateLeaf { variable: 0 },
            PositiveCertificateLeaf { variable: 1 },
        ]);
        let plan = ResidualPlan::compile_production(&root);
        let full = root.variables();
        let machine = ResidualStateMachine::new_for_plan(full, &plan, None);
        (root, plan, machine, full)
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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

    fn relational_prefix_positive_fixture() -> (
        ResidualPlan,
        StateDesc,
        PositivePublicationCertificate,
        VariableSet,
    ) {
        let root = IntersectionConstraint::new(vec![
            PositiveCertificateLeaf { variable: 0 },
            PositiveCertificateLeaf { variable: 0 },
            PositiveCertificateLeaf { variable: 1 },
        ]);
        let plan = ResidualPlan::compile_production(&root);
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
            ContinuationPublicationReceipt::RelationalPrefix
        );
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
        let plan = ResidualPlan::compile_production(&root);
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
                continuation: FormulaReducerContinuation::Complete(test_formula_cursor(u32::MAX)),
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
        let plan = ResidualPlan::compile_production(&root);
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
                cursor: test_formula_cursor(u32::MAX),
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
    }

    #[test]
    fn proposal_materializer_drains_typed_and_direct_occurrences_without_graph_telemetry() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_production(&root);
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
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            None,
            None,
        );
        let installed = scheduler
            .registry
            .install_program_roots(activation, std::iter::empty::<ProgramSeedWork>());
        let DeltaSettlement::Completed(completed) = scheduler.settle_quiescence(
            installed
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
        let plan = ResidualPlan::compile_production(&root);
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
        let action = parent.with_pc(stable_interner.formula_pcs.select_child_with(
            &plan.finite_formula,
            parent.pc,
            0,
            FormulaReturnKind::Child,
            FormulaStage::Propose,
            true,
        ));
        let batch = FormulaBatch::from_proposal(
            RowBatch::seed(),
            vec![super::super::ActivationId(11)],
            &plan.finite_formula.node(formula_root).kind,
        );
        let old_activation = scheduler.registry.open_program_activation(
            DeltaReducer::quiescent_proposal(),
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                cursor: action,
                batch,
            },
            None,
            None,
        );
        let mut installed = scheduler.registry.install_program_roots(
            old_activation,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(0),
                accepted: Some(value(7)),
            }],
        );
        let (_, root_credit) = installed.roots.pop().expect("one typed formula root");
        let retired = scheduler.registry.replace_program(
            root_credit,
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
        // A one-arm union retains a real OR reducer cell but lets one empty
        // admission complete its child and discover the empty root emission.
        // Neither zero-rank reducer may manufacture a sentinel Program task.
        let root = UnionConstraint::new(vec![MixedExpansion]);
        let plan = ResidualPlan::compile_production(&root);
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
        let action = parent.with_pc(stable_interner.formula_pcs.select_child_with(
            &plan.finite_formula,
            parent.pc,
            0,
            FormulaReturnKind::Child,
            FormulaStage::Propose,
            true,
        ));
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
        let plan = ResidualPlan::compile_production(&root);
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
            // that arm, whose complete Confirm result immediately contributes
            // the immutable root candidate to a fresh OR admission reducer.
            let support_action = parent.with_pc(stable_interner.formula_pcs.select_child_with(
                &plan.finite_formula,
                parent.pc,
                0,
                FormulaReturnKind::Guard,
                FormulaStage::Support,
                true,
            ));
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
                    cursor: support_action,
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
        let plan = ResidualPlan::compile_production(&root);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(6);
        let (_parent_activation, parent, _exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, initially_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, true);
        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
        scheduler.park_positive_support_activations(&AHashSet::from_iter([child]));
        let active = scheduler
            .begin_public_pull_demand(&mut stats)
            .expect("public demand should wake the parked Support child");
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
        let plan = ResidualPlan::compile_production(&root);
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
        let plan = ResidualPlan::compile_production(&root);
        let mut scheduler = DeltaScheduler::new();
        let candidate = value(4);
        let (parent_activation, parent, exact_credit, initial) =
            open_tapped_confirm_with_support(&mut scheduler.registry, [candidate], None, true);
        assert!(initial.is_empty());
        let (child, support_active) =
            queue_one_shot_positive_support(&mut scheduler, &root, parent, candidate, false);

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
        let terminal = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        let ordinary = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            None,
        );
        let wrong_full = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            candidate_return(Vec::new()),
            None,
            Some(VariableSet::new_empty()),
        );
        let wrong_reducer = scheduler.registry.open_program_activation(
            DeltaReducer::quiescent_proposal(),
            candidate_return(Vec::new()),
            None,
            Some(full),
        );
        assert_eq!(
            scheduler.registry.physical_activation_class(terminal),
            DeltaPhysicalClass::TerminalStreaming
        );
        assert_eq!(
            scheduler.registry.physical_activation_class(ordinary),
            DeltaPhysicalClass::General
        );
        assert_eq!(
            scheduler.registry.physical_activation_class(wrong_full),
            DeltaPhysicalClass::General
        );
        assert_eq!(
            scheduler.registry.physical_activation_class(wrong_reducer),
            DeltaPhysicalClass::General
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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

        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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

        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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
    fn positive_publication_certificate_keeps_terminal_precedence() {
        assert_eq!(
            std::mem::size_of::<Option<Box<PositivePublicationRegistration>>>(),
            std::mem::size_of::<usize>(),
            "dormant positive-publication state must cost one nullable pointer"
        );
        let certificate = terminal_positive_certificate();
        assert_eq!(
            certificate.continuation,
            ContinuationPublicationReceipt::Terminal
        );
        assert!(certificate.eligible());
    }

    #[test]
    fn positive_publication_ledger_requires_nonbarrier_parent() {
        let eligible = terminal_positive_certificate();
        let mut registry = ProducerRegistry::new();

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

        let relational_prefix = PositivePublicationCertificate {
            continuation: ContinuationPublicationReceipt::RelationalPrefix,
        };
        let (_, relational_parent) =
            open_positive_confirm(&mut registry, [value(3)], relational_prefix);
        assert!(
            registry
                .positive_publication_snapshot(relational_parent)
                .is_some(),
            "a relational successor needs no historical boundary bit"
        );
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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
                    cursor: test_formula_cursor(0),
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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

        let mut machine = ResidualStateMachine::new(terminal_positive_full(), 1, None);
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
            stats.delta_positive_publication_relational_prefix_commits,
            0
        );
    }

    #[test]
    fn positive_terminal_release_accepts_an_already_set_admitted_input() {
        let candidate = value(9);
        let certificate = terminal_positive_certificate();
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
        let root = PositiveCertificateLeaf { variable: 0 };
        let plan = ResidualPlan::compile_production(&root);
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
    fn positive_relational_prefix_release_files_singleton_into_exact_saved_k() {
        let candidate = value(11);
        let (plan, successor, certificate, _full) = relational_prefix_positive_fixture();
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
            .expect("RelationalPrefix publication needs no Terminal capability");
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
            .expect("one parent and one value make relational filing nonempty");
        assert_eq!(stable_interner.get(token.state), &successor);
        let StateBucket::Candidates(batch) = &stable[&token.rank][&token.state] else {
            panic!("relational-prefix release filed the wrong payload kind")
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
            stats.delta_positive_publication_relational_prefix_commits,
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
        let activation = registry.open_program_activation(
            DeltaReducer::Confirm { original },
            terminal_positive_return(Vec::new()),
            None,
            None,
        );
        let installed = registry.install_program_roots(
            activation,
            [published_one, published_two, remainder]
                .into_iter()
                .enumerate()
                .map(|(slot, accepted)| ProgramSeedWork {
                    parent: 0,
                    work: positive_test_work(u32::try_from(slot).unwrap()),
                    accepted: Some(accepted),
                }),
        );
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
        for (_, credit) in installed.roots {
            let retired = registry.replace_program(
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
            if let Some(quiescence) = retired.quiescence {
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
    fn support_formula_return() -> DeltaReturn {
        DeltaReturn::Formula {
            bound: VariableSet::new_empty(),
            cursor: test_formula_cursor(7),
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
        let formula_original = formula_batch.take_contiguous_confirm_original();
        assert!(!formula_batch.has_current());
        let formula = registry.open_program_activation(
            DeltaReducer::Confirm {
                original: formula_original,
            },
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                cursor: test_formula_cursor(0),
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
            variable: 0,
            grouping: ProgramGrouping::PageLocal,
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
            variable: 0,
            grouping: ProgramGrouping::PageLocal,
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
            variable: 0,
            grouping: ProgramGrouping::PageLocal,
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
                variable: 0,
                grouping: ProgramGrouping::PageLocal,
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
        };
        let plan = ResidualPlan::compile_production(&root);
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
    }

    #[test]
    fn stable_confirm_finalizer_pages_occurrences_without_graph_telemetry() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_production(&root);
        let a = value(1);
        let b = value(2);
        let rejected = value(9);

        let original = shared_one_parent_candidates(vec![b, a, rejected, a, b]);
        let mut scheduler = DeltaScheduler::new();
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::Confirm { original },
            candidate_return(Vec::new()),
            Some(vec![a, b, rejected].into_boxed_slice()),
            None,
        );
        let installed = scheduler.registry.install_program_roots(
            activation,
            [a, b]
                .into_iter()
                .enumerate()
                .map(|(slot, accepted)| ProgramSeedWork {
                    parent: 0,
                    work: positive_test_work(u32::try_from(slot).unwrap()),
                    accepted: Some(accepted),
                }),
        );
        {
            let graph = scheduler
                .registry
                .state
                .activations
                .get_mut(&activation)
                .expect("live Confirm graph activation");
            graph.program_joins.reserve(8);
        }
        let mut proof = None;
        for (_, credit) in installed.roots {
            let retired = scheduler.registry.replace_program(
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
            if let Some(quiescence) = retired.quiescence {
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
        let empty_activation = empty.registry.open_program_activation(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(Vec::new()),
            },
            candidate_return(Vec::new()),
            None,
            None,
        );
        let installed = empty
            .registry
            .install_program_roots(empty_activation, std::iter::empty::<ProgramSeedWork>());
        let DeltaSettlement::Completed(completed) =
            empty.settle_quiescence(installed.quiescence.unwrap())
        else {
            panic!("empty Confirm opened a finalizer task")
        };
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
        assert!(empty.program_worklist.is_empty());
        assert!(!empty.registry.is_live(empty_activation));

        let root = MixedExpansion;
        let plan = ResidualPlan::compile_production(&root);
        let mut rejected = DeltaScheduler::new();
        let rejected_activation = rejected.registry.open_program_activation(
            DeltaReducer::Confirm {
                original: shared_one_parent_candidates(vec![value(1), value(2)]),
            },
            candidate_return(Vec::new()),
            None,
            None,
        );
        let installed = rejected
            .registry
            .install_program_roots(rejected_activation, std::iter::empty::<ProgramSeedWork>());
        let DeltaSettlement::Retargeted(active) =
            rejected.settle_quiescence(installed.quiescence.unwrap())
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
        assert_eq!(eof.outcome.completed_activation_ids, [rejected_activation]);
        assert_eq!(eof.outcome.dead_pages, 1);
        assert!(eof.outcome.continuation.is_none());
        assert!(stable.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn formula_confirm_finalizer_accepts_or_ancestry_now_that_admission_is_pageable() {
        fn formula_batch(original: &CandidatePayload, with_or: bool) -> FormulaBatch {
            let cells = if with_or {
                vec![FormulaLiveCell::Or {
                    source: original.clone(),
                    accumulator: FormulaOrAccumulator::empty(1),
                }]
            } else {
                Vec::new()
            };
            FormulaBatch {
                activations: vec![super::super::ActivationId(11)],
                parents: RowBatch::seed(),
                cells,
            }
        }

        fn quiescent_formula(
            registry: &mut ProducerRegistry,
            original: CandidatePayload,
            with_or: bool,
        ) -> QuiescenceProof {
            let batch = formula_batch(&original, with_or);
            let activation = registry.open_program_activation(
                DeltaReducer::Confirm { original },
                DeltaReturn::Formula {
                    bound: VariableSet::new_empty(),
                    cursor: test_formula_cursor(0),
                    batch,
                },
                None,
                None,
            );
            let mut installed = registry.install_program_roots(
                activation,
                [ProgramSeedWork {
                    parent: 0,
                    work: positive_test_work(0),
                    accepted: Some(value(7)),
                }],
            );
            let (_, credit) = installed.roots.pop().unwrap();
            registry
                .replace_program(
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
                )
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
        let activation = empty.open_program_activation(
            DeltaReducer::Confirm {
                original: empty_original,
            },
            DeltaReturn::Formula {
                bound: VariableSet::new_empty(),
                cursor: test_formula_cursor(0),
                batch: empty_batch,
            },
            None,
            None,
        );
        let installed =
            empty.install_program_roots(activation, std::iter::empty::<ProgramSeedWork>());
        let RegistrySettlement::Completed(completed) =
            empty.settle_quiescence(installed.quiescence.unwrap())
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
        let root = OneShotConfirmProgram { novelty_drops };
        let plan = ResidualPlan::compile_production(&root);
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
            variable: 0,
            grouping: ProgramGrouping::PageLocal,
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
            None,
            &mut stats,
        ));
        assert_eq!(scheduler.registry.transition_dispatch_width(narrow, 8), 1);
        assert_eq!(scheduler.registry.transition_dispatch_width(wide, 8), 8);
        assert_eq!(stats.delta_terminal_sparse_widenings, 0);
    }

    #[test]
    fn terminal_projection_feedback_is_one_receipt_per_activation_batch() {
        let (root, plan, mut machine, full) = terminal_projection_feedback_fixture();
        let first = open_terminal_projection_feedback_activation(&mut machine, full, plan.len());
        let second = open_terminal_projection_feedback_activation(&mut machine, full, plan.len());
        assert_eq!(
            machine
                .delta
                .registry
                .finish_dispatch(first, 8, PhysicalDispatchKind::Program, false),
            (false, true)
        );
        for expected in [2, 4] {
            let (_, widened) = machine.delta.registry.finish_dispatch(
                second,
                8,
                PhysicalDispatchKind::Program,
                false,
            );
            assert!(widened);
            assert_eq!(
                machine.delta.registry.transition_dispatch_width(second, 8),
                expected
            );
        }

        let mut publication = TerminalPublicationBatch::new(
            first,
            RowBatch {
                rows: vec![value(7), value(1), value(7), value(2), value(7), value(3)],
                row_count: 3,
            },
        );
        publication.append(TerminalPublicationBatch::new(
            second,
            RowBatch {
                rows: vec![value(8), value(4)],
                row_count: 1,
            },
        ));
        let dispatch = PhysicalDispatch::new(
            &machine.delta.registry,
            PhysicalDispatchKind::Program,
            8,
            [first, second],
            vec![2, 4],
            0,
        );
        let terminal_publications = OrderedActivationSet::from(vec![first, second]);
        machine.delta.account_physical_dispatch(
            dispatch,
            8,
            0,
            &terminal_publications,
            Some(&mut publication),
            &mut machine.stats,
        );
        assert_eq!(
            publication
                .projection_feedback
                .iter()
                .map(|receipt| (receipt.activation, receipt.last_row, receipt.widen_to))
                .collect::<Vec<_>>(),
            [(first, 2, Some(8)), (second, 3, Some(8))]
        );
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(first, 8),
            2
        );
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(second, 8),
            4
        );

        machine
            .terminal_yield
            .register(StateId(99), &[first, second]);
        machine.stage_direct_terminal_publication(publication);
        machine.width = 8;
        let mut projection = ProjectionGate::new([0], full);
        let mut preclaimed = Binding::default();
        preclaimed.set(0, &value(7));
        assert!(projection.claim(&preclaimed));
        let mapper_calls = AtomicUsize::new(0);
        assert_eq!(
            machine.pull(
                &root,
                &plan,
                &|_| {
                    mapper_calls.fetch_add(1, Ordering::Relaxed);
                    None::<()>
                },
                &mut projection,
            ),
            None
        );

        assert_eq!(mapper_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(first, 8),
            4,
            "three duplicate rows are one negative activation receipt"
        );
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(second, 8),
            1,
            "a fresh raw claim resets before a rejecting mapper runs"
        );
        assert_eq!(machine.stats.delta_terminal_sparse_widenings, 1);
        assert_eq!(machine.stats.delta_terminal_sparse_resets, 1);
        assert!(machine.emit_projection_feedback.is_empty());
    }

    #[test]
    fn empty_projected_head_retires_unvisited_feedback_neutrally() {
        let (root, plan, mut machine, full) = terminal_projection_feedback_fixture();
        let first = open_terminal_projection_feedback_activation(&mut machine, full, plan.len());
        let second = open_terminal_projection_feedback_activation(&mut machine, full, plan.len());
        machine
            .delta
            .registry
            .finish_dispatch(first, 8, PhysicalDispatchKind::Program, false);
        for _ in 0..2 {
            machine
                .delta
                .registry
                .finish_dispatch(second, 8, PhysicalDispatchKind::Program, false);
        }
        let mut publication = TerminalPublicationBatch::new(
            first,
            RowBatch {
                rows: vec![value(7), value(1)],
                row_count: 1,
            },
        );
        publication.append(TerminalPublicationBatch::new(
            second,
            RowBatch {
                rows: vec![value(8), value(2)],
                row_count: 1,
            },
        ));
        let dispatch = PhysicalDispatch::new(
            &machine.delta.registry,
            PhysicalDispatchKind::Program,
            8,
            [first, second],
            vec![2, 4],
            0,
        );
        machine.delta.account_physical_dispatch(
            dispatch,
            8,
            0,
            &OrderedActivationSet::from(vec![first, second]),
            Some(&mut publication),
            &mut machine.stats,
        );
        machine
            .terminal_yield
            .register(StateId(100), &[first, second]);
        machine.stage_direct_terminal_publication(publication);
        let mapper_calls = AtomicUsize::new(0);
        assert_eq!(
            machine.pull(
                &root,
                &plan,
                &|_| {
                    mapper_calls.fetch_add(1, Ordering::Relaxed);
                    None::<()>
                },
                &mut ProjectionGate::new([], full),
            ),
            None
        );

        assert_eq!(mapper_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(first, 8),
            1
        );
        assert_eq!(
            machine.delta.registry.transition_dispatch_width(second, 8),
            4,
            "an exhausted empty head is not negative evidence about its suffix"
        );
        assert_eq!(machine.stats.delta_terminal_sparse_resets, 1);
        assert_eq!(machine.stats.delta_terminal_sparse_widenings, 0);
        assert!(machine.emit_projection_feedback.is_empty());
    }

    #[test]
    fn terminal_projection_miss_uses_the_dispatch_search_ceiling() {
        let (_, plan, mut machine, full) = terminal_projection_feedback_fixture();
        let activation =
            open_terminal_projection_feedback_activation(&mut machine, full, plan.len());
        machine
            .delta
            .registry
            .finish_dispatch(activation, 8, PhysicalDispatchKind::Program, false);
        let mut publication = TerminalPublicationBatch::new(
            activation,
            RowBatch {
                rows: vec![value(7), value(1)],
                row_count: 1,
            },
        );
        let dispatch = PhysicalDispatch::new(
            &machine.delta.registry,
            PhysicalDispatchKind::Program,
            2,
            [activation],
            vec![2],
            0,
        );
        machine.delta.account_physical_dispatch(
            dispatch,
            2,
            0,
            &OrderedActivationSet::from(vec![activation]),
            Some(&mut publication),
            &mut machine.stats,
        );
        let receipt = publication.projection_feedback.pop().unwrap();
        assert_eq!(receipt.widen_to, Some(2));
        machine
            .delta
            .settle_terminal_projection_feedback(receipt, false, &mut machine.stats);
        assert_eq!(
            machine
                .delta
                .registry
                .transition_dispatch_width(activation, 8),
            2
        );
        assert_eq!(machine.stats.delta_terminal_sparse_widenings, 0);
    }

    #[test]
    fn empty_support_roots_prove_false_only_at_quiescence() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            None,
            None,
        );
        let installed =
            registry.install_program_roots(activation, std::iter::empty::<ProgramSeedWork>());

        let completed = registry.finish(
            installed
                .quiescence
                .expect("an empty support frontier is immediately quiescent"),
        );
        assert_eq!(completed.effect, DeltaCompletion::Support(false));
        assert!(matches!(completed.return_to, DeltaReturn::Formula { .. }));
    }

    #[test]
    fn accepting_seed_is_an_immediate_effect_receipt_not_an_expansion_side_effect() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let mut installed = registry.install_program_roots(
            activation,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(0),
                accepted: Some(value(7)),
            }],
        );
        assert_eq!(installed.initial_accepted, [value(7)]);
        assert_eq!(
            registry
                .take_streaming_return(activation)
                .expect("the accepting seed has a streaming return")
                .effect,
            DeltaStreamingEffect::Candidates
        );
        let (_, root) = installed.roots.pop().expect("one typed seed root");

        let expanded = registry.replace_program(
            root,
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
            expanded.accepted.is_empty(),
            "the first typed page replayed seed acceptance"
        );
        let completed = registry.finish(expanded.quiescence.expect("the root quiesces"));
        assert_eq!(completed.effect, DeltaCompletion::Cleanup);
    }

    #[test]
    fn online_or_accepting_seed_publishes_before_program_quiescence() {
        let exit = FormulaPcId(13);
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::StreamFormulaOrProposal { exit },
            support_formula_return(),
            None,
            None,
        );
        let mut installed = registry.install_program_roots(
            activation,
            [ProgramSeedWork {
                parent: 0,
                work: positive_test_work(0),
                accepted: Some(value(7)),
            }],
        );
        assert_eq!(installed.initial_accepted, [value(7)]);

        let streamed = registry
            .take_streaming_return(activation)
            .expect("seed acceptance lost its online effect receipt");
        assert_eq!(
            streamed.effect,
            DeltaStreamingEffect::FormulaOrCandidates { exit }
        );
        assert!(
            streamed.return_to.is_none(),
            "online receipt cloned its Formula payload before master admission"
        );
        let mut accepted = installed.initial_accepted;
        let admitted = registry
            .publish_formula_or_candidates(activation, &mut accepted)
            .expect("the first endpoint was not admitted");
        assert_eq!(accepted, [value(7)]);
        let DeltaReturn::Formula { batch, .. } = admitted else {
            panic!("online admission returned the wrong continuation shape")
        };
        let (_, accumulator) = batch.last_or().expect("online admission lost its OR cell");
        assert_eq!(accumulator.sets[0].len(), 1);
        assert_eq!(accumulator.pending_len(), 0);

        let (_, root) = installed.roots.pop().expect("one typed seed root");
        let outcome = registry.replace_program(
            root,
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
            outcome.accepted.is_empty(),
            "the typed page replayed seed acceptance"
        );
        let proof = outcome
            .quiescence
            .expect("the accepting Program seed reached quiescence");
        let RegistrySettlement::Completed(completed) = registry.settle_quiescence(proof) else {
            panic!("online OR EOF manufactured finalizer work")
        };
        assert!(matches!(
            completed.effect,
            DeltaCompletion::Candidates(ref candidates) if candidates.is_empty()
        ));
        let DeltaReturn::Formula { batch, .. } = completed.return_to else {
            panic!("online OR EOF lost its master Formula return")
        };
        let (_, accumulator) = batch.last_or().expect("online EOF lost its OR cell");
        assert_eq!(accumulator.sets[0].len(), 1);
        assert_eq!(accumulator.pending_len(), 0);
    }

    #[test]
    fn support_reducer_publishes_only_the_first_distinct_witness() {
        let mut registry = ProducerRegistry::new();
        let activation = registry.open_program_activation(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            None,
            None,
        );
        let installed = registry.install_program_roots(
            activation,
            [0, 1].map(|slot| ProgramSeedWork {
                parent: 0,
                work: positive_test_work(slot),
                accepted: None,
            }),
        );
        let mut roots = installed.roots.into_iter();
        let (_, first_root) = roots.next().unwrap();
        let (_, second_root) = roots.next().unwrap();

        let first_children = [ProgramChild {
            input: 0,
            work: positive_test_work(2),
            accepted: Some(value(7)),
        }];
        let first = registry.replace_program(
            first_root,
            DeltaStateId(0),
            &first_children,
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.accepted.as_slice(), [value(7)]);
        let streamed = registry
            .take_streaming_return(activation)
            .expect("the first witness publishes support");
        assert_eq!(streamed.effect, DeltaStreamingEffect::Support);
        assert!(matches!(
            streamed.return_to,
            Some(DeltaReturn::Formula { .. })
        ));

        let second_children = [
            ProgramChild {
                input: 0,
                work: positive_test_work(3),
                accepted: Some(value(7)),
            },
            ProgramChild {
                input: 0,
                work: positive_test_work(4),
                accepted: Some(value(8)),
            },
        ];
        let second = registry.replace_program(
            second_root,
            DeltaStateId(0),
            &second_children,
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert_eq!(second.accepted.as_slice(), [value(8)]);
        assert!(registry.take_streaming_return(activation).is_none());

        let mut proof = None;
        for (_, _, child) in first.scheduled.into_iter().chain(second.scheduled) {
            let retired = registry.replace_program(
                child,
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
        let activation = original.open_program_activation(
            DeltaReducer::Support { published: false },
            support_formula_return(),
            None,
            None,
        );
        let installed = original.install_program_roots(
            activation,
            [0, 1].map(|slot| ProgramSeedWork {
                parent: 0,
                work: positive_test_work(slot),
                accepted: None,
            }),
        );
        let mut roots = installed.roots.into_iter();
        let (_, witness_root) = roots.next().unwrap();
        let (_, remaining_root) = roots.next().unwrap();
        let remaining_key = remaining_root.key;

        let first = original.replace_program(
            witness_root,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [value(7)],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert_eq!(first.accepted.as_slice(), [value(7)]);
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
        let original_second = original.replace_program(
            remaining_root,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [value(8)],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        let cloned_second = cloned.replace_program(
            cloned_remaining,
            DeltaStateId(0),
            &[],
            std::iter::empty(),
            [value(8)],
            std::iter::empty(),
            false,
            false,
            false,
            None,
        );
        assert_eq!(original_second.accepted.as_slice(), [value(8)]);
        assert_eq!(cloned_second.accepted.as_slice(), [value(8)]);
        assert!(original.take_streaming_return(activation).is_none());
        assert!(cloned.take_streaming_return(activation).is_none());
    }
}
