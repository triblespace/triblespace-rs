//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::{AHashMap, AHashSet};

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
pub(super) struct ProgramAddress {
    desc: DeltaDesc,
    key: ProgramKey,
    stratum: ProgramStratum,
}

impl ProgramAddress {
    fn new(desc: DeltaDesc, route: ProgramRoute) -> Self {
        assert_eq!(
            desc.variable, route.variable,
            "constructed program route changed its structural variable"
        );
        Self {
            desc,
            key: route.key,
            stratum: route.stratum,
        }
    }

    fn resolve<'r, 'a>(&self, root: &'r dyn Constraint<'a>, plan: &ResidualPlan) -> ProgramRef<'r> {
        self.desc
            .resolve(root, plan)
            .residual_program()
            .expect("constructed typed program disappeared during execution")
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
    by_desc: AHashMap<DeltaDesc, DeltaStateId>,
    by_program: AHashMap<ProgramAddress, DeltaStateId>,
    descs: Vec<DeltaDesc>,
    programs: Vec<Option<ProgramAddress>>,
}

impl DeltaInterner {
    fn intern(&mut self, desc: DeltaDesc) -> DeltaStateId {
        if let Some(&id) = self.by_desc.get(&desc) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.descs.len()).expect("too many delta states"));
        self.descs.push(desc.clone());
        self.programs.push(None);
        self.by_desc.insert(desc, id);
        id
    }

    fn intern_program(&mut self, address: ProgramAddress) -> DeltaStateId {
        if let Some(&id) = self.by_program.get(&address) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.descs.len()).expect("too many program states"));
        self.descs.push(address.desc.clone());
        self.programs.push(Some(address.clone()));
        self.by_program.insert(address, id);
        id
    }

    fn get(&self, id: DeltaStateId) -> &DeltaDesc {
        &self.descs[id.0 as usize]
    }

    fn program(&self, id: DeltaStateId) -> Option<&ProgramAddress> {
        self.programs[id.0 as usize].as_ref()
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
    /// proved quiescence.
    QuiescentProposal,
    /// Accepted endpoints are Boolean witnesses, not candidate values. The
    /// first witness releases `true` exactly once; only producer quiescence can
    /// release `false`.
    Support {
        published: bool,
    },
    Confirm {
        original: Box<[RawInline]>,
    },
    /// Formula confirmation reads its immutable original stream directly
    /// from the activation's owned return frame, avoiding a second candidate
    /// allocation solely for reducer finalization.
    FormulaConfirm,
}

impl DeltaReducer {
    fn streams(&self) -> bool {
        matches!(self, Self::StreamProposal | Self::StreamFormulaProposal)
    }
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
    },
    Formula {
        bound: VariableSet,
        counter: FormulaPcId,
        batch: FormulaBatch,
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
    /// Occurrence-preserving direct proposal effects retained only by a
    /// quiescent formula reducer. Streaming reducers file these immediately;
    /// transition acceptance continues to use the distinct `accepted` set.
    direct_candidates: Vec<RawInline>,
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
    scheduled: Vec<(DeltaStateId, ProgramWork, ProducerCredit)>,
    accepted: Vec<RawInline>,
    dead_search_pages: usize,
    dead_source_telemetry_pages: usize,
    quiescence: Option<QuiescenceProof>,
}

struct CompletedActivation {
    activation: ActivationId,
    return_to: DeltaReturn,
    effect: DeltaCompletion,
}

#[derive(Debug, Eq, PartialEq)]
enum DeltaCompletion {
    /// Every semantic effect was released before quiescence.
    Cleanup,
    /// Complete quiescent candidate action result.
    Candidates(Vec<RawInline>),
    /// Boolean support proved only at the reducer boundary.
    Support(bool),
}

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
        reducer: DeltaReducer,
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
                        direct_candidates: Vec::new(),
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
                        direct_candidates: Vec::new(),
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
                        direct_candidates: Vec::new(),
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

        // Direct proposal effects are candidate occurrences, not transition
        // witnesses. Preserve their order and multiplicity exactly as an
        // ordinary `propose` call would. Product-state acceptance remains a
        // set operation: several traversal witnesses for one endpoint emit
        // that endpoint once per activation.
        let mut accepted = direct;
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
                DeltaReducer::QuiescentProposal => activation
                    .direct_candidates
                    .extend(accepted.iter().copied()),
                DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {}
                DeltaReducer::Support { .. }
                | DeltaReducer::Confirm { .. }
                | DeltaReducer::FormulaConfirm => assert!(
                    accepted.is_empty(),
                    "a non-proposal reducer received direct source candidates"
                ),
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
        let direct: Vec<_> = direct.into_iter().collect();
        let mut accepted = Vec::new();
        {
            let activation = self
                .state
                .activations
                .get_mut(&activation_id)
                .expect("unknown program activation");
            match &activation.reducer {
                DeltaReducer::QuiescentProposal => {
                    activation.direct_candidates.extend_from_slice(&direct)
                }
                DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {
                    accepted.extend_from_slice(&direct)
                }
                DeltaReducer::Support { .. }
                | DeltaReducer::Confirm { .. }
                | DeltaReducer::FormulaConfirm => assert!(
                    direct.is_empty(),
                    "a non-proposal program reducer observed direct candidates"
                ),
            }
            for value in observed
                .into_iter()
                .chain(children.iter().filter_map(|child| child.accepted))
            {
                if activation.accepted.insert(value) {
                    accepted.push(value);
                }
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
                DeltaReducer::QuiescentProposal
                | DeltaReducer::Confirm { .. }
                | DeltaReducer::FormulaConfirm => false,
            }
        };
        if publishes_stable_effect {
            self.mark_program_join_stable_effect(activation_id, parent_join);
        }

        let no_replacement =
            children.is_empty() && matches!(&resume, None | Some(ProgramResume::AfterChildrenDone));
        let mut scheduled = Vec::new();
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
            DeltaReturn::Stable { desc, parent } => (desc.bound, parent.as_ref()),
            DeltaReturn::Formula { bound, batch, .. } => {
                assert_eq!(batch.parents.row_count, 1);
                (*bound, batch.parents.rows.as_slice())
            }
        };
        (bound, parent, Self::activation_candidates(activation))
    }

    fn activation_candidates(activation: &Activation) -> Option<&[RawInline]> {
        activation.source_candidates.as_deref().or_else(|| {
            match (&activation.reducer, &activation.return_to) {
                (DeltaReducer::Confirm { original }, _) => Some(original.as_ref()),
                (DeltaReducer::FormulaConfirm, DeltaReturn::Formula { batch, .. }) => {
                    Some(batch.input().one_parent_values())
                }
                (DeltaReducer::FormulaConfirm, DeltaReturn::Stable { .. }) => {
                    panic!("formula confirmation returned to a stable continuation")
                }
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
            DeltaReturn::Formula { bound, .. } => *bound,
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
            | DeltaReducer::QuiescentProposal
            | DeltaReducer::Confirm { .. }
            | DeltaReducer::FormulaConfirm => return None,
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

    /// Consumes the unique quiescence proof and releases the exact affine
    /// continuation that was suspended when this activation was seeded.
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
                assert!(activation.direct_candidates.is_empty());
                DeltaCompletion::Cleanup
            }
            DeltaReducer::QuiescentProposal => {
                let mut result = activation.accepted.into_iter().collect::<Vec<_>>();
                result.extend(activation.direct_candidates);
                result.sort_unstable();
                DeltaCompletion::Candidates(result)
            }
            DeltaReducer::Support { published: true } => {
                assert!(activation.direct_candidates.is_empty());
                DeltaCompletion::Cleanup
            }
            DeltaReducer::Support { published: false } => {
                assert!(activation.direct_candidates.is_empty());
                assert!(
                    activation.accepted.is_empty(),
                    "an unpublished support reducer quiesced with a witness"
                );
                DeltaCompletion::Support(false)
            }
            DeltaReducer::Confirm { original } => {
                assert!(activation.direct_candidates.is_empty());
                DeltaCompletion::Candidates(
                    original
                        .iter()
                        .filter(|candidate| activation.accepted.contains(*candidate))
                        .copied()
                        .collect(),
                )
            }
            DeltaReducer::FormulaConfirm => {
                assert!(activation.direct_candidates.is_empty());
                let DeltaReturn::Formula { batch, .. } = &activation.return_to else {
                    panic!("formula confirmation returned to a stable continuation")
                };
                DeltaCompletion::Candidates(
                    batch
                        .input()
                        .one_parent_values()
                        .iter()
                        .filter(|candidate| activation.accepted.contains(*candidate))
                        .copied()
                        .collect(),
                )
            }
        };
        CompletedActivation {
            activation: proof.activation,
            return_to: activation.return_to,
            effect,
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
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::with_capacity(input_count);
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
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::with_capacity(parent_count);
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProgramDispatchKey {
    dispatch: DispatchClass,
    pacing: ProgramPacing,
    bound: VariableSet,
    has_candidates: bool,
    publication: TransitionDispatchKey,
}

impl ProgramDispatchKey {
    fn of(registry: &ProducerRegistry, task: &ProgramTask) -> Self {
        let (bound, has_candidates) = registry.source_dispatch_shape(task.activation);
        Self {
            dispatch: task.work.dispatch,
            pacing: task.work.pacing,
            bound,
            has_candidates,
            publication: TransitionDispatchKey::for_activation(registry, task.activation),
        }
    }

    fn quiescent_peer_compatible(self, other: Self) -> bool {
        self.pacing == ProgramPacing::Activation
            && self.dispatch == other.dispatch
            && self.pacing == other.pacing
            && self.bound == other.bound
            && self.has_candidates == other.has_candidates
            && matches!(
                (self.publication, other.publication),
                (
                    TransitionDispatchKey::Quiescent(_),
                    TransitionDispatchKey::Quiescent(_)
                )
            )
    }
}

#[derive(Default)]
struct ProgramBucket {
    tasks: Vec<ProgramTask>,
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
    /// The same affine activation may be resumed after the yielded stable
    /// continuation runs. Publishing one accepted value and retaining cyclic
    /// traversal work are independent facts.
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
            .or_insert_with(|| spec.new_runtime());
        state
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
                effects.absorb(Self::release_streaming(
                    activation,
                    streamed,
                    installed.initial_accepted,
                    direct_terminal,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                ));
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
    pub(super) fn seed_program_confirms(
        &mut self,
        spec: ProgramRef<'_>,
        desc: DeltaDesc,
        request: ProgramRequest,
        route: ProgramRoute,
        successor: StateDesc,
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
            let original = original.into_boxed_slice();
            activations.push(self.registry.open_program_activation(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
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
                let completed = self.registry.finish(proof);
                assert_eq!(completed.effect, DeltaCompletion::Candidates(Vec::new()));
                retired.push(ProgramActivation(completed.activation.0));
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
        self.file_program_state(state, tasks)
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
        for batch in singletons {
            let reducer = match stage {
                FormulaStage::Support => DeltaReducer::Support { published: false },
                FormulaStage::Propose if stream_proposal => DeltaReducer::StreamFormulaProposal,
                FormulaStage::Propose => DeltaReducer::QuiescentProposal,
                FormulaStage::Confirm => DeltaReducer::FormulaConfirm,
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
        for (activation, range) in activations.into_iter().zip(ranges) {
            let installed = self
                .registry
                .install_program_roots(activation, seeded.work[range].iter().cloned());
            if !installed.initial_accepted.is_empty() {
                if let Some(streamed) = self.registry.take_streaming_return(activation) {
                    let released = Self::release_streaming(
                        activation,
                        streamed,
                        installed.initial_accepted,
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    debug_assert!(released.publication.is_none());
                    prefer_continuation(&mut continuation, released.continuation);
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
                let completed_activation = self.registry.finish(proof);
                retired.push(ProgramActivation(completed_activation.activation.0));
                completed.push(completed_activation);
            }
        }
        let active = self.file_program_state(state, tasks);
        for completed in completed {
            prefer_continuation(
                &mut continuation,
                Self::release_completion(completed, plan, stable, stable_interner, stats),
            );
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
                effects.absorb(Self::release_streaming(
                    started.activation,
                    streamed,
                    started.initial_accepted,
                    direct_terminal,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                ));
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
        batch: CandidateBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) -> Option<ActiveDeltaContinuation> {
        let seed_ranges = seed_ranges(&seeds, batch.parents.row_count);
        let stride = successor.bound.count();
        let (parents, candidate_groups) = batch.into_parent_candidates();

        let mut tasks = Vec::with_capacity(seeds.len());
        for ((row, seed_range), original) in
            seed_ranges.into_iter().enumerate().zip(candidate_groups)
        {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let original = original.into_boxed_slice();
            let started = self.registry.start_many(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
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
                let completed = self.registry.finish(proof);
                assert_eq!(completed.effect, DeltaCompletion::Candidates(Vec::new()));
                assert!(matches!(completed.return_to, DeltaReturn::Stable { .. }));
            }
        }
        self.file(desc, tasks)
    }

    pub(super) fn seed_source_confirms(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
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
            let original = original.into_boxed_slice();
            let mut source_candidates = original.to_vec();
            source_candidates.sort_unstable();
            source_candidates.dedup();
            let (activation, credit) = self.registry.start_source(
                DeltaReducer::Confirm { original },
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
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
        for (batch, range) in singletons.into_iter().zip(ranges) {
            let reducer = match stage {
                FormulaStage::Support => DeltaReducer::Support { published: false },
                FormulaStage::Propose if stream_proposal => DeltaReducer::StreamFormulaProposal,
                FormulaStage::Propose => DeltaReducer::QuiescentProposal,
                FormulaStage::Confirm => DeltaReducer::Confirm {
                    original: batch
                        .input()
                        .one_parent_values()
                        .to_vec()
                        .into_boxed_slice(),
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
                    let released = Self::release_streaming(
                        started.activation,
                        streamed,
                        started.initial_accepted,
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                    debug_assert!(released.publication.is_none());
                    prefer_continuation(&mut continuation, released.continuation);
                }
            }
            tasks.extend(started.roots.into_iter().map(|(node, credit)| DeltaTask {
                activation: started.activation,
                credit,
                node,
                cursor: ResidualDeltaExpandCursor::Start,
            }));
            if let Some(proof) = started.quiescence {
                completed.push(self.registry.finish(proof));
            }
        }
        let active = self.file(desc, tasks);

        for completed in completed {
            prefer_continuation(
                &mut continuation,
                Self::release_completion(completed, plan, stable, stable_interner, stats),
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
        for batch in singletons {
            let (reducer, source_candidates) = match stage {
                FormulaStage::Support => {
                    unreachable!("support has no delta source reducer")
                }
                FormulaStage::Propose if stream_proposal => {
                    (DeltaReducer::StreamFormulaProposal, None)
                }
                FormulaStage::Propose => (DeltaReducer::QuiescentProposal, None),
                FormulaStage::Confirm => {
                    let original = batch
                        .input()
                        .one_parent_values()
                        .to_vec()
                        .into_boxed_slice();
                    let mut source_candidates = original.to_vec();
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

    fn release_completion(
        completed: CompletedActivation,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
        let result = match completed.effect {
            DeltaCompletion::Cleanup => {
                // A streaming activation has already resumed one affine copy
                // of its continuation per semantic effect. Quiescence only
                // retires producer credits; replaying the template here would
                // duplicate publication.
                return None;
            }
            DeltaCompletion::Support(truth) => {
                return Self::release_support(
                    completed.return_to,
                    truth,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
            }
            DeltaCompletion::Candidates(result) => result,
        };
        match completed.return_to {
            DeltaReturn::Stable { desc, parent } => {
                if result.is_empty() {
                    return None;
                }
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
                        candidates: CandidatePayload::Values(result),
                    }),
                    stats,
                )
            }
            DeltaReturn::Formula {
                bound,
                counter,
                batch,
            } => {
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
                finish_formula_action_result(
                    plan,
                    bound,
                    counter,
                    batch,
                    CandidatePayload::Values(result),
                    stable,
                    stable_interner,
                    stats,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_streaming(
        activation: ActivationId,
        streamed: DeltaStreamingReturn,
        accepted: Vec<RawInline>,
        direct_terminal_full: Option<VariableSet>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStableEffects {
        if streamed.effect == DeltaStreamingEffect::Support {
            return DeltaStableEffects {
                continuation: Self::release_support(
                    streamed.return_to,
                    true,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                ),
                publication: None,
            };
        }
        debug_assert!(!accepted.is_empty());
        stats.candidates_proposed += accepted.len();
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted.len());
        let candidates = CandidatePayload::Values(accepted);
        if let Some(full) = direct_terminal_full {
            let DeltaReturn::Stable { desc, parent } = streamed.return_to else {
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
            return DeltaStableEffects {
                continuation: None,
                publication: Some(TerminalPublicationBatch::new(activation, rows)),
            };
        }
        let continuation = match streamed.return_to {
            DeltaReturn::Stable { desc, parent } => file_with_plan(
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
            ),
        };
        DeltaStableEffects {
            continuation,
            publication: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_support(
        return_to: DeltaReturn,
        truth: bool,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
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
        propagate_formula_support(
            plan,
            &desc,
            completed,
            truth,
            batch,
            stable,
            stable_interner,
            stats,
        )
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
            .tasks
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
            .is_some_and(|bucket| {
                bucket
                    .tasks
                    .iter()
                    .any(|task| task.activation == active.activation)
            })
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

    #[allow(unexpected_cfgs)]
    fn pop_active_program(
        &mut self,
        active: ActiveDeltaContinuation,
        search_width: usize,
    ) -> (DeltaStateId, Vec<ProgramTask>, ProgramPacing) {
        let (tasks, empty, pacing) = {
            let registry = &self.registry;
            let bucket = self
                .program_worklist
                .get_mut(&active.state)
                .expect("active typed program state remains live");
            let key = bucket
                .tasks
                .iter()
                .rev()
                .find(|task| task.activation == active.activation)
                .map(|task| ProgramDispatchKey::of(registry, task))
                .expect("active typed program lost its affine task");
            let width = match key.pacing {
                ProgramPacing::Search => {
                    registry.source_dispatch_width(active.activation, search_width)
                }
                ProgramPacing::Activation => {
                    registry.transition_dispatch_width(active.activation, search_width)
                }
            };
            let mut selected = Vec::new();
            let mut retained = Vec::with_capacity(bucket.tasks.len());
            for task in std::mem::take(&mut bucket.tasks).into_iter().rev() {
                if selected.len() < width
                    && task.activation == active.activation
                    && ProgramDispatchKey::of(registry, &task) == key
                {
                    selected.push(task);
                } else {
                    retained.push(task);
                }
            }
            if cfg!(engine_program_restore_storage_order) && key.pacing == ProgramPacing::Activation
            {
                selected.reverse();
            }
            retained.reverse();
            bucket.tasks = retained;
            (selected, bucket.tasks.is_empty(), key.pacing)
        };
        assert!(!tasks.is_empty(), "active typed program pop was empty");
        if empty {
            self.program_worklist.remove(&active.state);
        }
        (active.state, tasks, pacing)
    }

    #[allow(unexpected_cfgs)]
    fn pop_program_bounded(
        &mut self,
        search_width: usize,
    ) -> (DeltaStateId, Vec<ProgramTask>, PhysicalDispatch) {
        let id = *self
            .program_worklist
            .last_key_value()
            .expect("typed program pop requires live work")
            .0;
        let hot = self
            .program_worklist
            .get(&id)
            .and_then(|bucket| bucket.tasks.last())
            .expect("typed program bucket is nonempty");
        let hot_key = ProgramDispatchKey::of(&self.registry, hot);
        let terminal_activation_cohort = hot_key.pacing == ProgramPacing::Activation
            && hot_key.publication == TransitionDispatchKey::TerminalStreaming;
        let width = if terminal_activation_cohort {
            search_width.max(1)
        } else {
            match hot_key.pacing {
                ProgramPacing::Search => self
                    .registry
                    .source_dispatch_width(hot.activation, search_width),
                ProgramPacing::Activation => self
                    .registry
                    .transition_dispatch_width(hot.activation, search_width),
            }
        };
        let activation_width = self.activation_width.max(1);
        let (tasks, task_limits, empty, remainder_tasks) = {
            let registry = &self.registry;
            let selection_slots = &mut self.terminal_selection_slots;
            let selections = &mut self.terminal_selections;
            let bucket = self
                .program_worklist
                .get_mut(&id)
                .expect("selected typed program state");
            let key = ProgramDispatchKey::of(
                registry,
                bucket
                    .tasks
                    .last()
                    .expect("typed program bucket is nonempty"),
            );
            let tasks = std::mem::take(&mut bucket.tasks);
            let (selected, limits, retained) = if terminal_activation_cohort {
                let mut remaining = width;
                selection_slots.clear();
                selections.clear();
                for task in tasks.iter().rev() {
                    if ProgramDispatchKey::of(registry, task) != key
                        || selection_slots.contains_key(&task.activation)
                    {
                        continue;
                    }
                    let budget = registry
                        .transition_dispatch_width(task.activation, search_width)
                        .min(remaining);
                    let slot = selections.len();
                    selections.push(TerminalActivationSelection {
                        activation: task.activation,
                        budget,
                        selected: 0,
                        ordinal: 0,
                    });
                    selection_slots.insert(task.activation, slot);
                    remaining -= budget;
                    if remaining == 0 {
                        break;
                    }
                }

                let mut selected = Vec::new();
                let mut retained = Vec::with_capacity(tasks.len());
                for task in tasks.into_iter().rev() {
                    let selection = (ProgramDispatchKey::of(registry, &task) == key)
                        .then(|| selection_slots.get(&task.activation).copied())
                        .flatten();
                    if let Some(slot) = selection
                        .filter(|&slot| selections[slot].selected < selections[slot].budget)
                    {
                        selections[slot].selected += 1;
                        selected.push(task);
                    } else {
                        retained.push(task);
                    }
                }
                retained.reverse();
                let mut limits = Vec::with_capacity(selected.len());
                for task in &selected {
                    let selection = &mut selections[selection_slots[&task.activation]];
                    let quotient = selection.budget / selection.selected;
                    let remainder = selection.budget % selection.selected;
                    limits.push(quotient + usize::from(selection.ordinal < remainder));
                    selection.ordinal += 1;
                }
                (selected, limits, retained)
            } else {
                let mut activations = BTreeSet::new();
                let mut selected = Vec::new();
                let mut retained = Vec::with_capacity(tasks.len());
                for task in tasks.into_iter().rev() {
                    let task_key = ProgramDispatchKey::of(registry, &task);
                    let compatible = if cfg!(engine_program_restore_quiescent_compatibility)
                        && key.quiescent_peer_compatible(task_key)
                    {
                        activations.contains(&task.activation)
                            || (activations.len() < activation_width && {
                                activations.insert(task.activation);
                                true
                            })
                    } else {
                        task_key == key
                    };
                    if selected.len() < width && compatible {
                        selected.push(task);
                    } else {
                        retained.push(task);
                    }
                }
                if cfg!(engine_program_restore_storage_order)
                    && key.pacing == ProgramPacing::Activation
                {
                    selected.reverse();
                }
                retained.reverse();
                let limits = even_limits(width, selected.len());
                (selected, limits, retained)
            };
            bucket.tasks = retained;
            (
                selected,
                limits,
                bucket.tasks.is_empty(),
                bucket.tasks.len(),
            )
        };
        if empty {
            self.program_worklist.remove(&id);
        }
        let kind = match hot_key.pacing {
            ProgramPacing::Search => PhysicalDispatchKind::Source,
            ProgramPacing::Activation => PhysicalDispatchKind::Program,
        };
        let dispatch = PhysicalDispatch::new(
            &self.registry,
            kind,
            search_width,
            tasks.iter().map(|task| task.activation),
            task_limits,
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
            let (state, tasks, pacing) = self.pop_active_program(active, search_width);
            let task_count = tasks.len();
            let remainder_tasks = self
                .program_worklist
                .get(&active.state)
                .map_or(0, |bucket| bucket.tasks.len());
            let kind = match pacing {
                ProgramPacing::Search => PhysicalDispatchKind::Source,
                ProgramPacing::Activation => PhysicalDispatchKind::Program,
            };
            let task_limits = even_limits(
                match pacing {
                    ProgramPacing::Search => self
                        .registry
                        .source_dispatch_width(active.activation, search_width),
                    ProgramPacing::Activation => self
                        .registry
                        .transition_dispatch_width(active.activation, search_width),
                },
                task_count,
            );
            let dispatch = PhysicalDispatch::new(
                &self.registry,
                kind,
                search_width,
                tasks.iter().map(|task| task.activation),
                task_limits,
                remainder_tasks,
            );
            let physical = self.step_program(
                root,
                plan,
                state,
                tasks,
                &dispatch.task_limits,
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
        let resume = (yielded && self.registry.is_live(active.activation)).then_some(active);
        let status = if yielded {
            ActiveDeltaStatus::Yielded
        } else if self.registry.is_live(active.activation) {
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
                    task_effects.absorb(Self::release_streaming(
                        task.activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                completed_activations += 1;
                let completed = self.registry.finish(proof);
                completed_activation_ids.push(completed.activation);
                prefer_continuation(
                    &mut task_effects.continuation,
                    Self::release_completion(completed, plan, stable, stable_interner, stats),
                );
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
    #[allow(clippy::too_many_arguments, unexpected_cfgs)]
    fn step_program<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        state: DeltaStateId,
        tasks: Vec<ProgramTask>,
        limits: &[usize],
        direct_terminal_full: Option<VariableSet>,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaPhysicalOutcome {
        assert!(!tasks.is_empty());
        assert_eq!(tasks.len(), limits.len());
        assert!(limits.iter().all(|&limit| limit > 0));

        let address = self
            .interner
            .program(state)
            .cloned()
            .expect("typed program task was scheduled under a legacy delta state");
        let spec = address.resolve(root, plan);
        let dispatch_key = ProgramDispatchKey::of(&self.registry, &tasks[0]);
        assert!(
            tasks.iter().all(|task| {
                let task_key = ProgramDispatchKey::of(&self.registry, task);
                task_key == dispatch_key
                    || (cfg!(engine_program_restore_quiescent_compatibility)
                        && dispatch_key.quiescent_peer_compatible(task_key))
            }),
            "one typed program cohort mixed incompatible physical dispatch shapes"
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
        let vars: Vec<_> = dispatch_key.bound.into_iter().collect();
        let view = rows_view(&vars, &parents, row_count);
        let activations: Vec<_> = tasks
            .iter()
            .map(|task| ProgramActivation(task.activation.0))
            .collect();
        let mut task_receipts = Vec::with_capacity(row_count);
        let mut work = Vec::with_capacity(row_count);
        for task in tasks {
            task_receipts.push((task.activation, task.credit));
            work.push(task.work);
        }
        let mut receipt = ProgramBatchEffects::default();
        spec.step_batch(
            self.program_runtimes
                .get_mut(&state)
                .expect("typed program state lost its runtime"),
            ProgramBatch {
                stratum: address.stratum,
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
        #[cfg(engine_program_effect_probe)]
        let probe_zero_telemetry = receipt.source_pages == 0 && receipt.transition_pages == 0;
        #[cfg(engine_program_effect_probe)]
        let probe_finite = address.stratum == ProgramStratum::Finite;
        #[cfg(engine_program_effect_probe)]
        let probe_cohort_raw_effect = !receipt.children.is_empty()
            || !receipt.direct.is_empty()
            || !receipt.accepted.is_empty()
            || !receipt.supported.is_empty();
        #[cfg(engine_program_effect_probe)]
        {
            let probe = &mut stats.program_effect_probe;
            probe.cohorts += 1;
            probe.finite_cohorts += usize::from(probe_finite);
            probe.zero_telemetry_cohorts += usize::from(probe_zero_telemetry);
            probe.zero_telemetry_finite_cohorts +=
                usize::from(probe_zero_telemetry && probe_finite);
            probe.cohorts_with_raw_effect += usize::from(probe_cohort_raw_effect);
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

        // Source/transition naming remains family-reported telemetry; it is
        // never consulted for dispatch, novelty, or replacement semantics.
        stats.delta_source_pages += receipt.source_pages;
        stats.delta_source_candidates_examined += receipt.source_examined;
        stats.delta_source_roots += receipt.source_roots;
        stats.delta_source_direct_candidates += receipt.direct.len();
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
        let search_cohort = dispatch_key.pacing == ProgramPacing::Search;
        let source_telemetry_cohort = receipt.source_pages > 0 && receipt.transition_pages == 0;
        let mut scheduled = Vec::new();
        let mut effects = DeltaStableEffects::default();
        let mut completed_activation_ids = Vec::new();
        let mut retired_activations = Vec::new();
        let mut dead_pages = 0usize;
        let mut source_dead_pages = 0usize;
        let mut transition_dead_pages = 0usize;
        let mut retired_search_receipts = 0usize;
        let mut completed_activations = 0usize;
        let mut terminal_publications = OrderedActivationSet::default();
        #[cfg(engine_program_effect_probe)]
        let mut probe_cohort_quiescence = false;
        #[cfg(engine_program_effect_probe)]
        let mut probe_cohort_local_dead = false;
        #[cfg(engine_program_effect_probe)]
        let mut probe_cohort_dead_search_receipt = false;

        for (
            input,
            (
                (((((activation, credit), page), child_range), direct_range), accepted_range),
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
                || !direct_range.is_empty()
                || !accepted_range.is_empty()
                || !supported_range.is_empty();
            #[cfg(engine_program_effect_probe)]
            {
                let zero_examined_no_raw_no_resume =
                    page.examined == 0 && !page_had_program_effect && page.resume.is_none();
                let probe = &mut stats.program_effect_probe;
                probe.inputs += 1;
                probe.inputs_with_raw_effect += usize::from(page_had_program_effect);
                probe.inputs_within_search_page += usize::from(within_search_page);
                probe.zero_examined_no_raw_no_resume_inputs +=
                    usize::from(zero_examined_no_raw_no_resume);
                if probe_zero_telemetry && probe_finite {
                    probe.zero_telemetry_finite_inputs += 1;
                    probe.zero_telemetry_finite_inputs_with_raw_effect +=
                        usize::from(page_had_program_effect);
                    probe.zero_telemetry_finite_zero_examined_no_raw_no_resume_inputs +=
                        usize::from(zero_examined_no_raw_no_resume);
                }
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
                    task_effects.absorb(Self::release_streaming(
                        activation,
                        streamed,
                        Vec::new(),
                        None,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
            }
            if !outcome.accepted.is_empty() {
                let direct_terminal = direct_terminal_full.filter(|_| terminal);
                if let Some(streamed) = self.registry.take_streaming_return(activation) {
                    task_effects.absorb(Self::release_streaming(
                        activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, activation);
                completed_activations += 1;
                let completed = self.registry.finish(proof);
                #[cfg(engine_program_effect_probe)]
                {
                    probe_cohort_quiescence = true;
                    let probe = &mut stats.program_effect_probe;
                    probe.inputs_with_quiescence += 1;
                    if probe_zero_telemetry && probe_finite {
                        probe.zero_telemetry_finite_inputs_with_quiescence += 1;
                    }
                    match &completed.effect {
                        DeltaCompletion::Cleanup => probe.completions_cleanup += 1,
                        DeltaCompletion::Candidates(values) if values.is_empty() => {
                            probe.completions_candidates_empty += 1
                        }
                        DeltaCompletion::Candidates(_) => {
                            probe.completions_candidates_nonempty += 1
                        }
                        DeltaCompletion::Support(false) => probe.completions_support_false += 1,
                        DeltaCompletion::Support(true) => {
                            unreachable!("support true is released before Program quiescence")
                        }
                    }
                }
                completed_activation_ids.push(completed.activation);
                retired_activations.push(ProgramActivation(completed.activation.0));
                prefer_continuation(
                    &mut task_effects.continuation,
                    Self::release_completion(completed, plan, stable, stable_interner, stats),
                );
            }

            let page_dead = !page_had_program_effect && !task_effects.has_effect();
            #[cfg(engine_program_effect_probe)]
            {
                let task_had_stable_effect = task_effects.has_effect();
                let probe = &mut stats.program_effect_probe;
                probe.inputs_with_stable_effect += usize::from(task_had_stable_effect);
                if probe_zero_telemetry && probe_finite {
                    probe.zero_telemetry_finite_inputs_with_stable_effect +=
                        usize::from(task_had_stable_effect);
                }
                if page_dead {
                    probe_cohort_local_dead = true;
                    probe.inputs_local_dead += 1;
                    probe.inputs_local_dead_counted_global += usize::from(!within_search_page);
                    if probe_zero_telemetry && probe_finite {
                        probe.zero_telemetry_finite_inputs_local_dead += 1;
                        probe.zero_telemetry_finite_inputs_local_dead_counted_global +=
                            usize::from(!within_search_page);
                    }
                }
                if outcome.dead_search_pages > 0 {
                    probe_cohort_dead_search_receipt = true;
                    probe.inputs_with_dead_search_receipt += 1;
                    probe.dead_search_pages_reported += outcome.dead_search_pages;
                    if task_had_stable_effect {
                        probe.dead_search_pages_rescued_by_stable_effect +=
                            outcome.dead_search_pages;
                    } else {
                        probe.dead_search_pages_applied += outcome.dead_search_pages;
                    }
                }
            }
            if page_dead {
                // A child nested below an AfterChildren source receipt is
                // local work for that one source page. Preserve its exact
                // transition telemetry, but defer geometric feedback until
                // the receipt-local barrier knows whether any descendant
                // produced a stable effect.
                dead_pages += usize::from(!within_search_page);
                if source_telemetry_cohort {
                    source_dead_pages += 1;
                } else {
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

        let _ = self.file_program_state(state, scheduled);
        if !retired_activations.is_empty() {
            spec.retire_activations(
                self.program_runtimes
                    .get_mut(&state)
                    .expect("typed program state lost its runtime during retirement"),
                &retired_activations,
            );
        }
        #[cfg(engine_program_effect_probe)]
        {
            let probe = &mut stats.program_effect_probe;
            probe.cohorts_with_stable_effect += usize::from(effects.has_effect());
            probe.cohorts_with_quiescence += usize::from(probe_cohort_quiescence);
            probe.cohorts_with_local_dead += usize::from(probe_cohort_local_dead);
            probe.cohorts_with_dead_search_receipt += usize::from(probe_cohort_dead_search_receipt);
        }
        stats.delta_source_dead_pages += source_dead_pages;
        stats.delta_transition_dead_pages += transition_dead_pages;
        DeltaPhysicalOutcome {
            outcome: DeltaStepOutcome {
                continuation: effects.continuation,
                publication: effects.publication,
                completed_activation_ids,
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
                    task_effects.absorb(Self::release_streaming(
                        task.activation,
                        streamed,
                        outcome.accepted,
                        direct_terminal,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    ));
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                completed_activations += 1;
                let completed = self.registry.finish(proof);
                completed_activation_ids.push(completed.activation);
                prefer_continuation(
                    &mut task_effects.continuation,
                    Self::release_completion(completed, plan, stable, stable_interner, stats),
                );
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

    use crate::query::unionconstraint::UnionConstraint;

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
            states: Vec<Self::State>,
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
            DeltaReducer::QuiescentProposal,
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
        assert_eq!(scheduler.interner.descs, [desc]);
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
        let counter =
            stable_interner
                .formula_pcs
                .select_child_as_action(&plan.finite_formula, counter, 0);
        let root = plan
            .finite_formula
            .root(0)
            .expect("the synthetic root has a formula program");
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
                assert_eq!(child.accepted, [value(7)]);
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
        let original = candidates.into_boxed_slice();
        let original_ptr = original.as_ptr();
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
        let formula_batch = FormulaBatch::from_confirmation(
            CandidateBatch {
                parents: RowBatch::seed(),
                candidates: CandidatePayload::Values(formula_values),
            },
            vec![super::super::ActivationId(9)],
            &FiniteFormulaNodeKind::Atom,
        );
        let formula = registry.open_program_activation(
            DeltaReducer::FormulaConfirm,
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
        let (popped_state, hot, pacing) = scheduler.pop_active_program(active, 1);
        assert_eq!(popped_state, state);
        assert_eq!(pacing, ProgramPacing::Activation);
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].activation, first);
        assert_eq!(scheduler.program_worklist[&state].tasks.len(), 1);
        assert_eq!(
            scheduler.program_worklist[&state].tasks[0].activation,
            second
        );
    }

    fn program_order_trace(
        pacing: ProgramPacing,
        active_pop: bool,
    ) -> (Vec<CreditNonce>, Vec<CreditNonce>, Vec<CreditNonce>) {
        let mut scheduler = DeltaScheduler::new();
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
        };
        let state = scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 0), route));
        let activation = scheduler.registry.open_program_activation(
            DeltaReducer::StreamProposal,
            stable_return(Vec::new()),
            None,
            None,
        );
        let tasks: Vec<_> = scheduler
            .registry
            .install_program_roots(
                activation,
                (0..4).map(|slot| ProgramSeedWork {
                    parent: 0,
                    work: ProgramWork {
                        handle: ProgramWorkHandle::test(slot),
                        dispatch: DispatchClass::new(0),
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
            .collect();
        let storage_nonces: Vec<_> = tasks.iter().map(|task| task.credit.key.nonce).collect();
        let active = scheduler.file_program_state(state, tasks).unwrap();
        let (popped_state, selected) = if active_pop {
            let (popped_state, selected, popped_pacing) = scheduler.pop_active_program(active, 3);
            assert_eq!(popped_pacing, pacing);
            (popped_state, selected)
        } else {
            let (popped_state, selected, _) = scheduler.pop_program_bounded(3);
            (popped_state, selected)
        };
        assert_eq!(popped_state, state);
        let selected_nonces = selected.iter().map(|task| task.credit.key.nonce).collect();
        let retained_nonces = scheduler.program_worklist[&state]
            .tasks
            .iter()
            .map(|task| task.credit.key.nonce)
            .collect();
        (storage_nonces, selected_nonces, retained_nonces)
    }

    #[allow(unexpected_cfgs)]
    #[test]
    fn active_program_ablation_restores_order_only_for_activation_pacing() {
        for pacing in [ProgramPacing::Activation, ProgramPacing::Search] {
            let (storage, selected, retained) = program_order_trace(pacing, true);
            let expected = if cfg!(engine_program_restore_storage_order)
                && pacing == ProgramPacing::Activation
            {
                storage[1..].to_vec()
            } else {
                storage[1..].iter().copied().rev().collect()
            };
            assert_eq!(selected, expected);
            assert_eq!(retained, storage[..1]);
        }
    }

    #[allow(unexpected_cfgs)]
    #[test]
    fn global_program_ablation_restores_order_only_for_activation_pacing() {
        for pacing in [ProgramPacing::Activation, ProgramPacing::Search] {
            let (storage, selected, retained) = program_order_trace(pacing, false);
            let expected = if cfg!(engine_program_restore_storage_order)
                && pacing == ProgramPacing::Activation
            {
                storage[1..].to_vec()
            } else {
                storage[1..].iter().copied().rev().collect()
            };
            assert_eq!(selected, expected);
            assert_eq!(retained, storage[..1]);
        }
    }

    #[test]
    fn quiescent_program_peer_compatibility_is_activation_paced_and_structural() {
        let key = ProgramDispatchKey {
            dispatch: DispatchClass::new(0),
            pacing: ProgramPacing::Activation,
            bound: VariableSet::new_empty(),
            has_candidates: false,
            publication: TransitionDispatchKey::Quiescent(ActivationId::test(0)),
        };
        let peer = ProgramDispatchKey {
            publication: TransitionDispatchKey::Quiescent(ActivationId::test(1)),
            ..key
        };
        assert!(key.quiescent_peer_compatible(peer));
        assert!(!key.quiescent_peer_compatible(ProgramDispatchKey {
            pacing: ProgramPacing::Search,
            ..peer
        }));
        assert!(!key.quiescent_peer_compatible(ProgramDispatchKey {
            dispatch: DispatchClass::new(1),
            ..peer
        }));
        assert!(!key.quiescent_peer_compatible(ProgramDispatchKey {
            publication: TransitionDispatchKey::Streaming,
            ..peer
        }));
    }

    #[allow(unexpected_cfgs)]
    #[test]
    fn global_program_ablation_restores_quiescent_activation_cap_and_storage_order() {
        let mut scheduler = DeltaScheduler::new();
        scheduler.activation_width = 2;
        let route = ProgramRoute {
            key: ProgramKey::new(0),
            variable: 0,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
        };
        let state = scheduler
            .interner
            .intern_program(ProgramAddress::new(DeltaDesc::leaf(0, 0), route));
        let activations: Vec<_> = (0..4)
            .map(|_| {
                scheduler.registry.open_program_activation(
                    DeltaReducer::QuiescentProposal,
                    stable_return(Vec::new()),
                    None,
                    None,
                )
            })
            .collect();
        let install =
            |registry: &mut ProducerRegistry, activation, slots: std::ops::Range<u32>, dispatch| {
                registry
                    .install_program_roots(
                        activation,
                        slots.map(|slot| ProgramSeedWork {
                            parent: 0,
                            work: ProgramWork {
                                handle: ProgramWorkHandle::test(slot),
                                dispatch,
                                pacing: ProgramPacing::Activation,
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
                    .collect::<Vec<_>>()
            };
        let mut a = install(
            &mut scheduler.registry,
            activations[0],
            0..2,
            DispatchClass::new(0),
        )
        .into_iter();
        let mut b = install(
            &mut scheduler.registry,
            activations[1],
            2..4,
            DispatchClass::new(0),
        )
        .into_iter();
        let mut c = install(
            &mut scheduler.registry,
            activations[2],
            4..6,
            DispatchClass::new(0),
        )
        .into_iter();
        let mut incompatible = install(
            &mut scheduler.registry,
            activations[3],
            6..7,
            DispatchClass::new(1),
        )
        .into_iter();
        let a0 = a.next().unwrap();
        let a1 = a.next().unwrap();
        let b0 = b.next().unwrap();
        let b1 = b.next().unwrap();
        let c0 = c.next().unwrap();
        let c1 = c.next().unwrap();
        let incompatible = incompatible.next().unwrap();
        assert!(a.next().is_none() && b.next().is_none() && c.next().is_none());
        let a_nonces = [a0.credit.key.nonce, a1.credit.key.nonce];
        let b_nonces = [b0.credit.key.nonce, b1.credit.key.nonce];
        let c_nonces = [c0.credit.key.nonce, c1.credit.key.nonce];
        let incompatible_nonce = incompatible.credit.key.nonce;
        let stored = vec![a0, b0, c0, a1, incompatible, b1, c1];
        let _ = scheduler.file_program_state(state, stored);

        let (popped_state, tasks, dispatch) = scheduler.pop_program_bounded(8);
        assert_eq!(popped_state, state);
        let compatibility = cfg!(engine_program_restore_quiescent_compatibility);
        let expected_activations = if compatibility {
            &activations[1..3]
        } else {
            &activations[2..3]
        };
        assert_eq!(tasks.len(), expected_activations.len() * 2);
        assert!(tasks
            .iter()
            .all(|task| expected_activations.contains(&task.activation)));
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.activation)
                .collect::<BTreeSet<_>>()
                .len(),
            expected_activations.len(),
            "global quiescent batching must respect activation_width"
        );
        let mut expected_nonces = if compatibility {
            vec![b_nonces[0], c_nonces[0], b_nonces[1], c_nonces[1]]
        } else {
            c_nonces.to_vec()
        };
        if !cfg!(engine_program_restore_storage_order) {
            expected_nonces.reverse();
        }
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.credit.key.nonce)
                .collect::<Vec<_>>(),
            expected_nonces
        );
        let retained = &scheduler.program_worklist[&state].tasks;
        assert!(retained.iter().any(|task| {
            task.credit.key.nonce == incompatible_nonce
                && task.work.dispatch == DispatchClass::new(1)
        }));
        assert!(a_nonces
            .iter()
            .all(|nonce| { retained.iter().any(|task| task.credit.key.nonce == *nonce) }));
        if compatibility {
            assert_eq!(dispatch.task_limits, [2, 2, 2, 2]);
        } else {
            assert_eq!(dispatch.task_limits, [4, 4]);
        }
        assert_eq!(dispatch.remainder_tasks, 7 - tasks.len());
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
            states: Vec<Self::State>,
            batch: TypedProgramBatch<'_>,
            effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
        ) {
            assert_eq!(states.len(), batch.candidate_sets.len());
            for (input, (state, candidates)) in states
                .into_iter()
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

    #[allow(unexpected_cfgs)]
    #[test]
    fn global_program_compatibility_co_completes_and_retires_real_confirm_activations() {
        let compatibility = cfg!(engine_program_restore_quiescent_compatibility);
        let novelty_drops = Arc::new(AtomicUsize::new(0));
        let root = OneShotConfirmProgram {
            novelty_drops: Arc::clone(&novelty_drops),
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
        let hot_id = stored_tasks.last().unwrap().activation;
        let mut all_ids: Vec<_> = stored_tasks.iter().map(|task| task.activation).collect();
        all_ids.sort_unstable();
        all_ids.dedup();
        assert_eq!(all_ids.len(), 2, "the seed must open distinct activations");
        let mut expected_completed_ids = if compatibility {
            all_ids.clone()
        } else {
            vec![hot_id]
        };
        expected_completed_ids.sort_unstable();
        let expected_completions = expected_completed_ids.len();
        let drops_before_step = novelty_drops.load(Ordering::Relaxed);

        let mut stable = Worklist::new();
        let mut stable_interner = StateInterner::default();
        let mut stats = ResidualStateStats::default();
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
        assert_eq!(completed_ids, expected_completed_ids);
        assert_eq!(outcome.completed_activations, expected_completions);
        assert_eq!(outcome.completed_transition_cohort, compatibility);
        assert!(outcome.continuation.is_some());
        assert_eq!(scheduler.program_worklist.is_empty(), compatibility);
        assert!(expected_completed_ids
            .iter()
            .all(|activation| !scheduler.registry.is_live(*activation)));
        if !compatibility {
            let retained = &scheduler.program_worklist[&active.state].tasks;
            assert_eq!(retained.len(), 1);
            assert_ne!(retained[0].activation, hot_id);
            assert!(scheduler.registry.is_live(retained[0].activation));
        }
        assert_eq!(
            novelty_drops.load(Ordering::Relaxed),
            drops_before_step + expected_completions,
            "retiring the cohort must drop each activation-local novelty table"
        );

        let stable_batches: Vec<_> = stable.values().flat_map(|level| level.values()).collect();
        assert_eq!(stable_batches.len(), 1);
        let StateBucket::Candidates(batch) = stable_batches[0] else {
            panic!("Confirm completions returned the wrong stable payload")
        };
        assert_eq!(batch.parents.row_count, expected_completions);
        assert_eq!(batch.candidate_count(), expected_completions);
        let snapshot = batch.candidates.tagged_snapshot();
        assert_eq!(
            snapshot
                .iter()
                .map(|(parent, _)| *parent)
                .collect::<Vec<_>>(),
            (0..expected_completions as u32).collect::<Vec<_>>()
        );
        let mut returned_values: Vec<_> = snapshot.into_iter().map(|(_, value)| value).collect();
        returned_values.sort_unstable();
        let expected_values = if compatibility {
            vec![value(7), value(8)]
        } else {
            vec![value(8)]
        };
        assert_eq!(returned_values, expected_values);
        assert_eq!(stats.delta_transition_pages, expected_completions);
        assert_eq!(
            stats.delta_transition_candidates_examined,
            expected_completions
        );
        assert_eq!(stats.delta_transition_cohorts, 1);
        assert_eq!(stats.max_delta_transition_cohort, expected_completions);
        assert_eq!(stats.delta_transition_dead_pages, 0);
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
            DeltaReducer::QuiescentProposal,
            candidate_return(Vec::new()),
            [output(1, 0, false), output(2, 0, false)],
        );
        let first_activation = first.activation;
        let (first_node, first_credit) = first.roots.remove(0);
        let (last_node, last_credit) = first.roots.remove(0);

        let mut second = scheduler.registry.start_many(
            DeltaReducer::QuiescentProposal,
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
            DeltaReducer::QuiescentProposal,
            candidate_return(Vec::new()),
            [
                output(1, 0, false),
                output(2, 0, false),
                output(3, 0, false),
            ],
        );
        let first_activation = first.activation;
        let mut second = scheduler.registry.start_many(
            DeltaReducer::QuiescentProposal,
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
        assert_eq!(scheduler.interner.descs.len(), 1);

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
            DeltaReducer::QuiescentProposal,
            candidate_return(Vec::new()),
            (1..=4).map(|value| output(value, 0, false)),
        );
        let first_activation = first.activation;
        let mut second = scheduler.registry.start_many(
            DeltaReducer::QuiescentProposal,
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
        assert!(source.resume.is_none());
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
        assert!(transition.resume.is_none());
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
        // Keep a real formula boundary in this white-box fixture. A lone
        // opaque root is deliberately normalized to the flat action plan.
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
        let seed_continuation = DeltaScheduler::release_streaming(
            live_activation,
            streamed,
            live.initial_accepted,
            None,
            &plan,
            &mut stable,
            &mut stable_interner,
            &mut stats,
        );
        assert!(seed_continuation.continuation.is_some());
        assert!(seed_continuation.publication.is_none());

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
                original: vec![candidate, candidate].into_boxed_slice(),
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
            DeltaCompletion::Candidates(vec![candidate, candidate])
        );
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
                original: vec![second, seed, first, rejected, second].into_boxed_slice(),
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
            DeltaCompletion::Candidates(vec![second, first, second])
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
        assert_eq!(first.accepted, [value(1), value(1)]);
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

        let last = registry.replace_source(generator, [], [value(2)], None);
        assert!(last.roots.is_empty());
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
    fn grouped_confirm_waits_for_all_source_pages_before_reducing() {
        let first = value(1);
        let second = value(2);
        let rejected = value(3);
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            DeltaReducer::Confirm {
                original: vec![second, first, rejected, first].into_boxed_slice(),
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
            DeltaCompletion::Candidates(vec![second, first, first])
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

        assert_eq!(scheduler.interner.descs.len(), 1);
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
                    original: Box::new([]),
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
        assert_eq!(scheduler.interner.descs, [desc]);
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
        assert_eq!(scheduler.interner.descs.len(), 1);
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
            DeltaReducer::QuiescentProposal,
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
        for completed in [original.finish(original_proof), cloned.finish(cloned_proof)] {
            assert_eq!(
                completed.effect,
                DeltaCompletion::Candidates(vec![value(7)])
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
            registry.finish(proof)
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
            DeltaReducer::QuiescentProposal,
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
                DeltaCompletion::Candidates(vec![value(3), value(3), value(7)])
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
                DeltaReducer::QuiescentProposal,
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

        assert_eq!(scheduler.interner.descs, vec![desc]);
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
