//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct DeltaStateId(u32);

#[derive(Clone, Default)]
struct DeltaInterner {
    by_desc: HashMap<DeltaDesc, DeltaStateId>,
    descs: Vec<DeltaDesc>,
}

impl DeltaInterner {
    fn intern(&mut self, desc: DeltaDesc) -> DeltaStateId {
        if let Some(&id) = self.by_desc.get(&desc) {
            return id;
        }
        let id = DeltaStateId(u32::try_from(self.descs.len()).expect("too many delta states"));
        self.descs.push(desc.clone());
        self.by_desc.insert(desc, id);
        id
    }

    fn get(&self, id: DeltaStateId) -> &DeltaDesc {
        &self.descs[id.0 as usize]
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditNonce(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditKey {
    activation: ActivationId,
    nonce: CreditNonce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreditKind {
    Generator,
    Traversal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CreditOwner {
    activation: ActivationId,
    kind: CreditKind,
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
}

impl DeltaReducer {
    fn streams(&self) -> bool {
        matches!(self, Self::StreamProposal | Self::StreamFormulaProposal)
    }
}

/// Exact affine continuation owned by one reducer activation.
///
/// Stable formula PCs intentionally live here rather than in [`DeltaDesc`]:
/// two activations may expand the same RPQ product kernel while returning to
/// different ancestor done masks and payload-frame stacks.
#[derive(Clone)]
enum DeltaReturn {
    Stable {
        desc: StateDesc,
        parent: Box<[RawInline]>,
    },
    Formula {
        bound: VariableSet,
        counter: FormulaProgramCounter,
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

/// One affine parent reducer scope. Several speculative source roots may own
/// live credits inside it; they share novelty and Accepted, while source stays
/// in each node so their product states cannot suppress one another.
#[derive(Clone)]
struct Activation {
    reducer: DeltaReducer,
    return_to: DeltaReturn,
    /// Sorted distinct source scope for grouped confirmation. Proposals own a
    /// constraint-generated graph frontier and therefore store `None`.
    source_candidates: Option<Box<[RawInline]>>,
    /// The continuation cursor is suspended while every traversal lineage
    /// rooted in the current page owns the activation's affine credits.
    suspended_source_page: Option<SuspendedSourcePage>,
    seen: HashMap<ResidualDeltaNode, bool>,
    accepted: HashSet<RawInline>,
    /// Occurrence-preserving direct proposal effects retained only by a
    /// quiescent formula reducer. Streaming reducers file these immediately;
    /// transition acceptance continues to use the distinct `accepted` set.
    direct_candidates: Vec<RawInline>,
    pending_accepted: Vec<RawInline>,
    live: BTreeSet<CreditNonce>,
    retired: BTreeSet<CreditNonce>,
    status: ActivationStatus,
}

#[derive(Clone)]
struct RegistryState {
    next_activation: u64,
    next_credit: u64,
    credit_owner: BTreeMap<CreditNonce, CreditOwner>,
    activations: BTreeMap<ActivationId, Activation>,
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
    quiescence: Option<QuiescenceProof>,
}

struct CompletedActivation {
    return_to: DeltaReturn,
    effect: DeltaCompletion,
}

#[derive(Debug, Eq, PartialEq)]
enum DeltaCompletion {
    /// Every semantic effect was released before quiescence.
    Cleanup,
    /// Complete quiescent candidate action result.
    Candidates(Candidates),
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

impl ProducerRegistry {
    fn new() -> Self {
        Self {
            brand: RegistryBrand::fresh(),
            state: RegistryState {
                next_activation: 0,
                next_credit: 0,
                credit_owner: BTreeMap::new(),
                activations: BTreeMap::new(),
            },
        }
    }

    /// Starts one parent-scoped activation with one affine credit per root.
    fn start_many(
        &mut self,
        reducer: DeltaReducer,
        return_to: DeltaReturn,
        seeds: impl IntoIterator<Item = ResidualDeltaOutput>,
    ) -> StartOutcome {
        let seeds = seeds.into_iter();
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        let mut live = BTreeSet::new();
        let mut accepted = HashSet::new();
        let mut pending_accepted = Vec::new();
        let mut roots = Vec::with_capacity(seeds.size_hint().0);
        for seed in seeds {
            let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
            assert!(
                self.state
                    .credit_owner
                    .insert(
                        nonce,
                        CreditOwner {
                            activation,
                            kind: CreditKind::Traversal,
                        },
                    )
                    .is_none(),
                "delta credit nonce was reused"
            );
            assert!(live.insert(nonce));
            if seed.accepted && accepted.insert(seed.node.value) {
                pending_accepted.push(seed.node.value);
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
                        source_candidates: None,
                        suspended_source_page: None,
                        seen: HashMap::new(),
                        accepted,
                        direct_candidates: Vec::new(),
                        pending_accepted,
                        live,
                        retired: BTreeSet::new(),
                        status,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        StartOutcome {
            activation,
            roots,
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
                        source_candidates,
                        suspended_source_page: None,
                        seen: HashMap::new(),
                        accepted: HashSet::new(),
                        direct_candidates: Vec::new(),
                        pending_accepted: Vec::new(),
                        live: BTreeSet::new(),
                        retired: BTreeSet::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        let credit = self.issue_credit(activation, CreditKind::Generator);
        (activation, credit)
    }

    fn issue_credit(&mut self, activation: ActivationId, kind: CreditKind) -> ProducerCredit {
        let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
        assert!(
            self.state
                .credit_owner
                .insert(nonce, CreditOwner { activation, kind })
                .is_none(),
            "delta credit nonce was reused"
        );
        assert!(self
            .state
            .activations
            .get_mut(&activation)
            .expect("unknown delta activation")
            .live
            .insert(nonce));
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
        let owner = self
            .state
            .credit_owner
            .get(&parent.key.nonce)
            .copied()
            .expect("unknown delta credit");
        assert_eq!(
            owner.activation, parent.key.activation,
            "delta credit changed activation"
        );
        assert_eq!(owner.kind, CreditKind::Traversal);

        let activation = self
            .state
            .activations
            .get_mut(&parent.key.activation)
            .expect("unknown delta activation");
        assert_eq!(activation.status, ActivationStatus::Open);
        assert!(
            activation.live.contains(&parent.key.nonce),
            "delta credit was replayed"
        );

        let mut novel = Vec::new();
        let mut accepted = std::mem::take(&mut activation.pending_accepted);
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
        assert!(activation.live.remove(&parent.key.nonce));
        assert!(activation.retired.insert(parent.key.nonce));
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
        let owner = self
            .state
            .credit_owner
            .get(&generator.key.nonce)
            .copied()
            .expect("unknown delta credit");
        assert_eq!(owner.activation, generator.key.activation);
        assert_eq!(owner.kind, CreditKind::Generator);

        let roots: Vec<_> = roots.into_iter().collect();
        let direct: Vec<_> = direct.into_iter().collect();
        let mut distinct_nodes = HashSet::with_capacity(roots.len());
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
            assert!(activation.live.contains(&generator.key.nonce));
            assert!(activation.suspended_source_page.is_none());
            match &activation.reducer {
                DeltaReducer::QuiescentProposal => activation
                    .direct_candidates
                    .extend(accepted.iter().copied()),
                DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {}
                DeltaReducer::Support { .. } | DeltaReducer::Confirm { .. } => assert!(
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
            assert!(activation.live.remove(&generator.key.nonce));
            assert!(activation.retired.insert(generator.key.nonce));
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
    ) -> (VariableSet, Vec<RawInline>, Option<Vec<RawInline>>) {
        let activation = self
            .state
            .activations
            .get(&activation)
            .expect("unknown delta activation");
        let (bound, parent) = match &activation.return_to {
            DeltaReturn::Stable { desc, parent } => (desc.bound, parent.to_vec()),
            DeltaReturn::Formula { bound, batch, .. } => {
                assert_eq!(batch.parents.row_count, 1);
                (*bound, batch.parents.rows.clone())
            }
        };
        (
            bound,
            parent,
            activation
                .source_candidates
                .as_deref()
                .map(<[RawInline]>::to_vec),
        )
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
        (bound, activation.source_candidates.is_some())
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
            | DeltaReducer::Confirm { .. } => return None,
        };
        Some(DeltaStreamingReturn {
            return_to: activation.return_to.clone(),
            effect,
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
        for nonce in &activation.retired {
            assert_eq!(
                self.state
                    .credit_owner
                    .remove(nonce)
                    .map(|owner| owner.activation),
                Some(proof.activation),
                "retired delta credit lost its owner"
            );
        }

        let effect = match activation.reducer {
            DeltaReducer::StreamProposal | DeltaReducer::StreamFormulaProposal => {
                assert!(activation.direct_candidates.is_empty());
                DeltaCompletion::Cleanup
            }
            DeltaReducer::QuiescentProposal => {
                let mut result: Candidates = activation
                    .accepted
                    .into_iter()
                    .map(|value| (0, value))
                    .collect();
                result.extend(
                    activation
                        .direct_candidates
                        .into_iter()
                        .map(|value| (0, value)),
                );
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
                        .map(|candidate| (0, candidate))
                        .collect(),
                )
            }
        };
        CompletedActivation {
            return_to: activation.return_to,
            effect,
        }
    }

    fn deep_clone(&self) -> (Self, BTreeMap<CreditKey, ProducerCredit>) {
        let state = self.state.clone();
        let brand = RegistryBrand::fresh();
        let mut remap = BTreeMap::new();
        for (&activation, state) in &state.activations {
            for &nonce in &state.live {
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
}

impl SourceDispatchKey {
    fn of(registry: &ProducerRegistry, task: &SourceTask) -> Self {
        let (bound, has_candidates) = registry.source_dispatch_shape(task.activation);
        Self {
            bound,
            has_candidates,
            cursor_family: SourceCursorFamily::of(task.cursor),
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
        (
            ResidualDeltaSourceCursor::After(previous),
            ResidualDeltaSourceCursor::After(next),
        ) => assert!(next > previous, "residual source cursor did not advance"),
        (
            ResidualDeltaSourceCursor::Offset(previous),
            ResidualDeltaSourceCursor::Offset(next),
        ) => assert!(next > previous, "residual source cursor did not advance"),
        (_, ResidualDeltaSourceCursor::Start) => {
            panic!("residual source page restarted its cursor")
        }
        _ => panic!("residual source page changed cursor families"),
    }
}

#[derive(Default)]
struct DeltaBucket {
    tasks: Vec<DeltaTask>,
}

#[derive(Default)]
struct SourceBucket {
    tasks: Vec<SourceTask>,
}

/// One delta scheduler step as observed by the outer geometric policy.
///
/// Stable progress and dead pages are deliberately independent: one batched
/// expansion can retire an ineffective page for one activation while another
/// activation files a stable continuation.
pub(super) struct DeltaStepOutcome {
    pub(super) continuation: Option<ContinuationToken>,
    pub(super) dead_pages: usize,
}

impl DeltaBucket {
    fn take_tail(&mut self, width: usize) -> Vec<DeltaTask> {
        let first = self.tasks.len().saturating_sub(width.max(1));
        self.tasks.split_off(first)
    }
}

/// Reopenable cyclic work kept outside the strict-rank stable worklist.
pub(super) struct DeltaScheduler {
    registry: ProducerRegistry,
    interner: DeltaInterner,
    worklist: BTreeMap<DeltaStateId, DeltaBucket>,
    source_worklist: BTreeMap<DeltaStateId, SourceBucket>,
}

impl DeltaScheduler {
    pub(super) fn new() -> Self {
        Self {
            registry: ProducerRegistry::new(),
            interner: DeltaInterner::default(),
            worklist: BTreeMap::new(),
            source_worklist: BTreeMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.worklist.is_empty() && self.source_worklist.is_empty()
    }

    pub(super) fn seed_proposals(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) {
        let ranges = seed_ranges(&seeds, parents.row_count);
        let stride = successor.bound.count();
        let mut tasks = Vec::with_capacity(seeds.len());
        for (row, range) in ranges.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let started = self.registry.start_many(
                DeltaReducer::StreamProposal,
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                },
                seeds[range].iter().map(|seed| seed.output),
            );
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
            }
        }
        self.file(desc, tasks);
    }

    pub(super) fn seed_source_proposals(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        parents: RowBatch,
    ) {
        let stride = successor.bound.count();
        let mut tasks = Vec::with_capacity(parents.row_count);
        for row in 0..parents.row_count {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let (activation, credit) = self.registry.start_source(
                DeltaReducer::StreamProposal,
                DeltaReturn::Stable {
                    desc: successor.clone(),
                    parent,
                },
                None,
            );
            tasks.push(SourceTask {
                activation,
                credit,
                cursor: ResidualDeltaSourceCursor::Start,
            });
        }
        self.file_source(desc, tasks);
    }

    pub(super) fn seed_confirms(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        batch: CandidateBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) {
        let seed_ranges = seed_ranges(&seeds, batch.parents.row_count);
        let stride = successor.bound.count();
        let mut candidate_ranges = Vec::with_capacity(batch.parents.row_count);
        let mut cursor = 0usize;
        for row in 0..batch.parents.row_count {
            let begin = cursor;
            while cursor < batch.candidates.len() && batch.candidates[cursor].0 as usize == row {
                cursor += 1;
            }
            assert!(
                begin < cursor,
                "compacted delta confirmation parent has no candidates"
            );
            candidate_ranges.push(begin..cursor);
        }
        assert_eq!(
            cursor,
            batch.candidates.len(),
            "delta confirmation candidate tags are invalid or ungrouped"
        );

        let mut tasks = Vec::with_capacity(seeds.len());
        for (row, seed_range) in seed_ranges.into_iter().enumerate() {
            let start = row * stride;
            let parent = batch.parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let original = batch.candidates[candidate_ranges[row].clone()]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice();
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
        self.file(desc, tasks);
    }

    pub(super) fn seed_source_confirms(
        &mut self,
        desc: DeltaDesc,
        successor: StateDesc,
        batch: CandidateBatch,
    ) {
        let stride = successor.bound.count();
        let mut candidate_ranges = Vec::with_capacity(batch.parents.row_count);
        let mut cursor = 0usize;
        for row in 0..batch.parents.row_count {
            let begin = cursor;
            while cursor < batch.candidates.len() && batch.candidates[cursor].0 as usize == row {
                cursor += 1;
            }
            assert!(
                begin < cursor,
                "compacted delta confirmation parent has no candidates"
            );
            candidate_ranges.push(begin..cursor);
        }
        assert_eq!(
            cursor,
            batch.candidates.len(),
            "delta confirmation candidate tags are invalid or ungrouped"
        );

        let mut tasks = Vec::with_capacity(batch.parents.row_count);
        for (row, candidate_range) in candidate_ranges.into_iter().enumerate() {
            let start = row * stride;
            let parent = batch.parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let original = batch.candidates[candidate_range]
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice();
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
        self.file_source(desc, tasks);
    }

    /// Suspends each affine formula parent behind one activation-local reducer.
    /// Empty seed ranges complete immediately with an empty action result, so
    /// an empty RPQ arm can still return through AND/OR frames.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn seed_formula(
        &mut self,
        desc: DeltaDesc,
        bound: VariableSet,
        counter: FormulaProgramCounter,
        batch: FormulaBatch,
        seeds: Vec<ResidualDeltaSeed>,
        stream_proposal: bool,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
        let ranges = seed_ranges(&seeds, batch.parents.row_count);
        let stage = match counter.focus {
            FormulaFocus::Action { stage, .. } => stage,
            _ => panic!("delta formula seeding requires an Action PC"),
        };
        let singletons = batch.into_singletons(bound.count());
        assert_eq!(singletons.len(), ranges.len());

        let mut tasks = Vec::with_capacity(seeds.len());
        let mut completed = Vec::new();
        for (batch, range) in singletons.into_iter().zip(ranges) {
            let reducer = match stage {
                FormulaStage::Support => DeltaReducer::Support { published: false },
                FormulaStage::Propose if stream_proposal => DeltaReducer::StreamFormulaProposal,
                FormulaStage::Propose => DeltaReducer::QuiescentProposal,
                FormulaStage::Confirm => DeltaReducer::Confirm {
                    original: batch
                        .input()
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                },
            };
            let started = self.registry.start_many(
                reducer,
                DeltaReturn::Formula {
                    bound,
                    counter: counter.clone(),
                    batch,
                },
                seeds[range].iter().map(|seed| seed.output),
            );
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
        self.file(desc, tasks);

        let mut continuation = None;
        for completed in completed {
            prefer_continuation(
                &mut continuation,
                Self::release_completion(completed, plan, stable, stable_interner, stats),
            );
        }
        continuation
    }

    /// Suspends one bounded source generator per affine formula parent. The
    /// exact Action PC and reducer frames remain activation payload; the
    /// structural descriptor names only the shared expansion kernel.
    pub(super) fn seed_source_formula(
        &mut self,
        desc: DeltaDesc,
        bound: VariableSet,
        counter: FormulaProgramCounter,
        batch: FormulaBatch,
        stream_proposal: bool,
    ) {
        let stage = match counter.focus {
            FormulaFocus::Action { stage, .. } => stage,
            _ => panic!("delta formula seeding requires an Action PC"),
        };
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
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>()
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
                    counter: counter.clone(),
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
        self.file_source(desc, tasks);
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
                        candidates: result,
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
                    counter.focus,
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
                    &counter,
                    batch,
                    result,
                    stable,
                    stable_interner,
                    stats,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn release_streaming(
        streamed: DeltaStreamingReturn,
        accepted: Vec<RawInline>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
        debug_assert!(!accepted.is_empty());
        if streamed.effect == DeltaStreamingEffect::Support {
            return Self::release_support(
                streamed.return_to,
                true,
                plan,
                stable,
                stable_interner,
                stats,
            );
        }
        stats.candidates_proposed += accepted.len();
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted.len());
        let candidates = accepted.into_iter().map(|value| (0, value)).collect();
        match streamed.return_to {
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
                &counter,
                batch,
                candidates,
                stable,
                stable_interner,
                stats,
            ),
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
            &counter.focus,
            FormulaFocus::Action {
                stage: FormulaStage::Support,
                ..
            }
        ));
        let completed = plan.finite_formula.complete(&counter);
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

    fn file(&mut self, desc: DeltaDesc, mut tasks: Vec<DeltaTask>) {
        if tasks.is_empty() {
            return;
        }
        let id = self.interner.intern(desc);
        self.worklist
            .entry(id)
            .or_default()
            .tasks
            .append(&mut tasks);
    }

    fn file_source(&mut self, desc: DeltaDesc, mut tasks: Vec<SourceTask>) {
        if tasks.is_empty() {
            return;
        }
        let id = self.interner.intern(desc);
        self.source_worklist
            .entry(id)
            .or_default()
            .tasks
            .append(&mut tasks);
    }

    fn pop(&mut self, width: usize) -> (DeltaDesc, Vec<DeltaTask>) {
        let full = self
            .worklist
            .iter()
            .rev()
            .find_map(|(&id, bucket)| (bucket.tasks.len() >= width.max(1)).then_some(id));
        let id = full.unwrap_or_else(|| {
            *self
                .worklist
                .last_key_value()
                .expect("delta pop requires live work")
                .0
        });
        let (tasks, empty) = {
            let bucket = self.worklist.get_mut(&id).expect("selected delta state");
            let tasks = bucket.take_tail(width);
            (tasks, bucket.tasks.is_empty())
        };
        if empty {
            self.worklist.remove(&id);
        }
        (self.interner.get(id).clone(), tasks)
    }

    fn pop_source(&mut self, width: usize) -> (DeltaDesc, Vec<SourceTask>) {
        let width = width.max(1);
        let id = *self
            .source_worklist
            .last_key_value()
            .expect("source pop requires live work")
            .0;
        let (tasks, empty) = {
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
            (selected, bucket.tasks.is_empty())
        };
        if empty {
            self.source_worklist.remove(&id);
        }
        (self.interner.get(id).clone(), tasks)
    }

    /// Executes one structural product-state cohort and files accepted
    /// proposal endpoints or quiescent confirmation reductions back into the
    /// ordinary acyclic Candidate continuation.
    pub(super) fn step<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        width: usize,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStepOutcome {
        if self.worklist.is_empty() {
            return self.step_source(root, plan, width, stable, stable_interner, stats);
        }

        let (desc, tasks) = self.pop(width);
        let constraint = desc.resolve(root, plan);
        let task_count = tasks.len();
        let page_budget = width.max(1);
        let page_base = page_budget / task_count;
        let page_remainder = page_budget % task_count;
        debug_assert!(page_base > 0);
        let nodes: Vec<_> = tasks.iter().map(|task| task.node).collect();
        let cursors: Vec<_> = tasks.iter().map(|task| task.cursor).collect();
        let limits: Vec<_> = (0..task_count)
            .map(|index| page_base + usize::from(index < page_remainder))
            .collect();
        debug_assert_eq!(limits.iter().sum::<usize>(), page_budget);
        let batch = ResidualDeltaExpandBatch {
            nodes: &nodes,
            cursors: &cursors,
            limits: &limits,
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
        let mut successors = vec![Vec::new(); task_count];
        let mut next_cursors = vec![None; task_count];
        let mut legacy_indices = Vec::new();
        let mut legacy_nodes = Vec::new();
        let mut paged_count = 0usize;
        for (index, (((task, page), range), &limit)) in tasks
            .iter()
            .zip(pages)
            .zip(successor_ranges)
            .zip(&limits)
            .enumerate()
        {
            successors[index].extend(tagged_successors[range].iter().map(|(_, output)| *output));
            let Some(page) = page else {
                assert_eq!(
                    task.cursor,
                    ResidualDeltaExpandCursor::Start,
                    "paged delta expansion became unsupported after suspension"
                );
                assert!(
                    successors[index].is_empty(),
                    "unsupported delta expansion page mutated its output"
                );
                legacy_indices.push(index);
                legacy_nodes.push(task.node);
                continue;
            };
            paged_count += 1;
            assert!(page.examined <= limit);
            assert!(successors[index].len() <= page.examined);
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
        }
        if paged_count > 0 {
            stats.delta_transition_cohorts += 1;
            stats.max_delta_transition_cohort = stats.max_delta_transition_cohort.max(paged_count);
        }

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
                successors[legacy_indices[tag as usize]].push(output);
            }
        }

        let mut next_tasks = Vec::new();
        let mut resumed_sources = Vec::new();
        let mut continuation = None;
        let mut dead_pages = 0usize;
        for (task_index, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.activation, task.credit.key.activation);
            let outcome = self.registry.replace_traversal_page(
                task.credit,
                successors[task_index].iter().copied(),
                next_cursors[task_index],
            );
            let retired_source_page = outcome.retired_source_page;
            let mut task_continuation = None;
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
                if let Some(streamed) = self.registry.take_streaming_return(task.activation) {
                    prefer_continuation(
                        &mut task_continuation,
                        Self::release_streaming(
                            streamed,
                            outcome.accepted,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        ),
                    );
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                let completed = self.registry.finish(proof);
                prefer_continuation(
                    &mut task_continuation,
                    Self::release_completion(completed, plan, stable, stable_interner, stats),
                );
            }
            if retired_source_page.is_some_and(|page| !page.had_stable_effect)
                && task_continuation.is_none()
            {
                dead_pages += 1;
            }
            prefer_continuation(&mut continuation, task_continuation);
        }
        self.file(desc.clone(), next_tasks);
        self.file_source(desc, resumed_sources);
        stats.delta_source_dead_pages += dead_pages;
        DeltaStepOutcome {
            continuation,
            dead_pages,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn step_source<'a>(
        &mut self,
        root: &dyn Constraint<'a>,
        plan: &ResidualPlan,
        width: usize,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> DeltaStepOutcome {
        let budget = width.max(1);
        let (desc, tasks) = self.pop_source(budget);
        assert!(!tasks.is_empty());
        assert!(tasks.len() <= budget);
        let dispatch_key = SourceDispatchKey::of(&self.registry, &tasks[0]);
        assert!(
            tasks
                .iter()
                .all(|task| SourceDispatchKey::of(&self.registry, task) == dispatch_key),
            "one residual source cohort mixed incompatible physical dispatch shapes"
        );

        let row_count = tasks.len();
        let quotient = budget / row_count;
        let remainder = budget % row_count;
        let limits: Vec<_> = (0..row_count)
            .map(|row| quotient + usize::from(row < remainder))
            .collect();
        assert!(limits.iter().all(|&limit| limit > 0));
        assert_eq!(limits.iter().sum::<usize>(), budget);

        let mut parents = Vec::new();
        let mut candidate_storage = Vec::with_capacity(row_count);
        for task in &tasks {
            assert_eq!(task.activation, task.credit.key.activation);
            let (bound, parent, candidates) = self.registry.source_context(task.activation);
            assert_eq!(bound, dispatch_key.bound);
            assert_eq!(candidates.is_some(), dispatch_key.has_candidates);
            parents.extend(parent);
            candidate_storage.push(candidates);
        }
        let vars: Vec<VariableId> = dispatch_key.bound.into_iter().collect();
        let view = rows_view(&vars, &parents, row_count);
        let candidate_sets: Vec<Option<&[RawInline]>> = candidate_storage
            .iter()
            .map(|candidates| candidates.as_deref())
            .collect();
        let cursors: Vec<_> = tasks.iter().map(|task| task.cursor).collect();
        let batch = ResidualDeltaSourceBatch {
            view,
            candidate_sets: &candidate_sets,
            cursors: &cursors,
            limits: &limits,
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
        assert_eq!(pages.len(), row_count);
        let root_ranges = tagged_ranges(&roots, row_count, "root");
        let direct_ranges = tagged_ranges(&direct, row_count, "direct candidate");

        stats.delta_source_cohorts += 1;
        stats.max_delta_source_cohort = stats.max_delta_source_cohort.max(row_count);
        stats.delta_source_pages += row_count;
        let mut continuation = None;
        let mut traversal = Vec::new();
        let mut resumed_sources = Vec::new();
        let mut dead_pages = 0usize;
        for (row, (((task, page), root_range), direct_range)) in tasks
            .into_iter()
            .zip(pages)
            .zip(root_ranges)
            .zip(direct_ranges)
            .enumerate()
        {
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
            let mut task_continuation = None;
            if !outcome.accepted.is_empty() {
                if let Some(streamed) = self.registry.take_streaming_return(task.activation) {
                    prefer_continuation(
                        &mut task_continuation,
                        Self::release_streaming(
                            streamed,
                            outcome.accepted,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        ),
                    );
                }
            }
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
                let completed = self.registry.finish(proof);
                prefer_continuation(
                    &mut task_continuation,
                    Self::release_completion(completed, plan, stable, stable_interner, stats),
                );
            }
            if retired_source_page.is_some_and(|page| !page.had_stable_effect)
                && task_continuation.is_none()
            {
                dead_pages += 1;
            }
            prefer_continuation(&mut continuation, task_continuation);
        }
        self.file(desc.clone(), traversal);
        self.file_source(desc, resumed_sources);
        stats.delta_source_dead_pages += dead_pages;
        DeltaStepOutcome {
            continuation,
            dead_pages,
        }
    }

    fn deep_clone(&self) -> Self {
        let (registry, mut remap) = self.registry.deep_clone();
        let mut worklist = BTreeMap::new();
        for (&id, bucket) in &self.worklist {
            let mut tasks = Vec::with_capacity(bucket.tasks.len());
            for task in &bucket.tasks {
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
            worklist.insert(id, DeltaBucket { tasks });
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
        assert!(
            remap.is_empty(),
            "delta registry held a live credit without a scheduled task"
        );
        Self {
            registry,
            interner: self.interner.clone(),
            worklist,
            source_worklist,
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

    use super::*;

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
            *self.trace.lock().expect("zero-column trace poisoned") =
                Some(ZeroColumnBatchTrace {
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

    fn streaming_formula_return(plan: &ResidualPlan) -> DeltaReturn {
        let relevant = ChildSet::empty(plan.len()).with_inserted(0);
        let counter = plan
            .finite_formula
            .start(0, 0, UnionVerb::Propose { relevant });
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
        let relevant = ChildSet::empty(1).with_inserted(0);
        DeltaReturn::Formula {
            bound: VariableSet::new_empty(),
            counter: FormulaProgramCounter {
                focus: FormulaFocus::Action {
                    node: FormulaNodeId(7),
                    stage: FormulaStage::Support,
                },
                returns: Vec::new().into_boxed_slice(),
                resume: FormulaOuterResume {
                    variable: 0,
                    occurrence: 0,
                    verb: UnionVerb::Propose { relevant },
                },
            },
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
        scheduler.file(desc.clone(), tasks);

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
            .tasks
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
    fn batched_delta_step_keeps_dead_page_and_streamed_formula_handoff_independent() {
        let root = MixedExpansion;
        let plan = ResidualPlan::compile_lowering(&root, ResidualLowering::FULL);
        let desc = DeltaDesc::leaf(0, 0);
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

        // This accepted seed publishes and proves quiescence in the same
        // scheduler step. StreamFormulaProposal must file its affine return
        // exactly once; cleanup must not replay the activation-local template.
        let mut live = scheduler.registry.start_many(
            DeltaReducer::StreamFormulaProposal,
            streaming_formula_return(&plan),
            [output(3, 0, true)],
        );
        let live_activation = live.activation;
        let (live_node, live_credit) = live.roots.pop().expect("one live formula root");
        assert!(live.quiescence.is_none());

        scheduler.file(
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

        assert_eq!(outcome.dead_pages, 1);
        assert!(outcome.continuation.is_some());
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
        assert_eq!(batch.candidates, [(0, value(3))]);
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
        assert_eq!(machine.width, 4, "a mixed positive step must not widen");
        assert_eq!(machine.stats.delta_source_negative_steps, 0);
        assert!(matches!(
            machine.continuation,
            Some(ActiveContinuation {
                mode: ContinuationMode::ProbeOne,
                ..
            })
        ));
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
            DeltaCompletion::Candidates(vec![(0, candidate), (0, candidate)])
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
            DeltaCompletion::Candidates(vec![(0, second), (0, first), (0, second)])
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
            registry
                .state
                .credit_owner
                .get(&credit.key.nonce)
                .expect("resumed credit owner")
                .kind,
            CreditKind::Generator
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

        let second_page = registry.replace_source(
            generator,
            [sourced_output(2, 2, 0, true)],
            [],
            None,
        );
        assert!(second_page.quiescence.is_none());
        let (_, second_root) = second_page.roots.into_iter().next().expect("second root");
        let second_retired = registry.replace_traversal(second_root, []);
        let proof = second_retired
            .quiescence
            .expect("the terminal page proves reducer quiescence");
        assert_eq!(proof.activation, activation);
        assert_eq!(
            registry.finish(proof).effect,
            DeltaCompletion::Candidates(vec![(0, second), (0, first), (0, first)])
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
        scheduler.file_source(
            desc.clone(),
            vec![SourceTask {
                activation: first_activation,
                credit: first_credit,
                cursor: ResidualDeltaSourceCursor::Start,
            }],
        );
        scheduler.file_source(
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
        let mut source_task = |
            bound: VariableSet,
            candidates: Option<Box<[RawInline]>>,
            cursor: ResidualDeltaSourceCursor,
            parent: RawInline,
        | {
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
        scheduler.file_source(desc.clone(), tasks);
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
        let relevant = ChildSet::empty(1).with_inserted(0);
        let counter = FormulaProgramCounter {
            focus: FormulaFocus::Action {
                node: FormulaNodeId(7),
                stage: FormulaStage::Propose,
            },
            returns: Vec::new().into_boxed_slice(),
            resume: FormulaOuterResume {
                variable: 0,
                occurrence: 0,
                verb: UnionVerb::Propose { relevant },
            },
        };
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
                counter: counter.clone(),
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
                DeltaCompletion::Candidates(vec![(0, value(7))])
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

        let relevant = ChildSet::empty(1).with_inserted(0);
        let counter = FormulaProgramCounter {
            focus: FormulaFocus::Action {
                node: FormulaNodeId(7),
                stage: FormulaStage::Propose,
            },
            returns: Vec::new().into_boxed_slice(),
            resume: FormulaOuterResume {
                variable: 0,
                occurrence: 0,
                verb: UnionVerb::Propose { relevant },
            },
        };
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
                counter: counter.clone(),
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
                DeltaCompletion::Candidates(vec![(0, value(3)), (0, value(3)), (0, value(7))])
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
        let first = FormulaProgramCounter {
            focus: FormulaFocus::Action {
                node: FormulaNodeId(7),
                stage: FormulaStage::Propose,
            },
            returns: Vec::new().into_boxed_slice(),
            resume: resume.clone(),
        };
        let second = FormulaProgramCounter {
            focus: first.focus.clone(),
            returns: vec![FormulaReturnSite {
                kind: FormulaReturnKind::Child,
                parent: FormulaNodeId(5),
                parent_stage: FormulaStage::Propose,
                child: 1,
                done: ChildSet::empty(2).with_inserted(0),
            }]
            .into_boxed_slice(),
            resume,
        };
        assert_ne!(first, second);

        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::formula(0, 3, FormulaNodeId(7));
        for (index, counter) in [first.clone(), second.clone()].into_iter().enumerate() {
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
            scheduler.file(desc.clone(), tasks);
        }

        assert_eq!(scheduler.interner.descs, vec![desc]);
        assert_eq!(scheduler.worklist.len(), 1);
        let tasks = &scheduler.worklist.values().next().unwrap().tasks;
        assert_eq!(tasks.len(), 2);
        let counters: Vec<_> = tasks
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
                counter.clone()
            })
            .collect();
        assert_eq!(counters, [first, second]);
    }
}
