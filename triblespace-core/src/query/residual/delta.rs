//! Cyclic fixpoint stratum for the canonical residual scheduler.
//!
//! Delta state identity is structural. Activation identity, novelty, affine
//! producer credits, and parent rows remain payload, so unrelated traversals
//! can share one expansion cohort without becoming semantically conflated.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

static NEXT_REGISTRY_BRAND: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DeltaVerb {
    Propose,
    Confirm,
}

/// Canonical cyclic work key. Activation-specific state is deliberately absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DeltaDesc {
    bound: VariableSet,
    variable: VariableId,
    leaf: usize,
    verb: DeltaVerb,
    relevant: ChildSet,
    checked: ChildSet,
}

impl DeltaDesc {
    pub(super) fn propose(
        bound: VariableSet,
        variable: VariableId,
        leaf: usize,
        relevant: ChildSet,
        checked: ChildSet,
    ) -> Self {
        Self {
            bound,
            variable,
            leaf,
            verb: DeltaVerb::Propose,
            relevant,
            checked,
        }
    }

    pub(super) fn confirm(
        bound: VariableSet,
        variable: VariableId,
        leaf: usize,
        relevant: ChildSet,
        checked: ChildSet,
    ) -> Self {
        Self {
            bound,
            variable,
            leaf,
            verb: DeltaVerb::Confirm,
            relevant,
            checked,
        }
    }
}

impl Hash for DeltaDesc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bound.count().hash(state);
        for variable in self.bound {
            variable.hash(state);
        }
        self.variable.hash(state);
        self.leaf.hash(state);
        self.verb.hash(state);
        self.relevant.hash(state);
        self.checked.hash(state);
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
    Propose,
    Confirm { original: Box<[RawInline]> },
}

#[derive(Clone)]
struct SuspendedSourcePage {
    next: Option<ResidualDeltaSourceCursor>,
    /// Only an effect already filed into the stable acyclic machine keeps a
    /// page from being scheduler-negative. Grouped confirmation acceptance is
    /// intentionally not stable until the complete reducer quiesces.
    had_stable_effect: bool,
}

/// One affine parent reducer scope. Several speculative source roots may own
/// live credits inside it; they share novelty and Accepted, while source stays
/// in each node so their product states cannot suppress one another.
#[derive(Clone)]
struct Activation {
    parent: Box<[RawInline]>,
    reducer: DeltaReducer,
    /// Sorted distinct source scope for grouped confirmation. Proposals own a
    /// constraint-generated graph frontier and therefore store `None`.
    source_candidates: Option<Box<[RawInline]>>,
    /// The continuation cursor is suspended while every traversal lineage
    /// rooted in the current page owns the activation's affine credits.
    suspended_source_page: Option<SuspendedSourcePage>,
    seen: HashMap<ResidualDeltaNode, bool>,
    accepted: HashSet<RawInline>,
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

struct ReducedConfirm {
    parent: Vec<RawInline>,
    candidates: Vec<(u32, RawInline)>,
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
        parent: Box<[RawInline]>,
        reducer: DeltaReducer,
        seeds: impl IntoIterator<Item = ResidualDeltaOutput>,
    ) -> Option<(ActivationId, Vec<(ResidualDeltaNode, ProducerCredit)>)> {
        let mut seeds = seeds.into_iter().peekable();
        if seeds.peek().is_none() {
            return None;
        }
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        let mut accepted = HashSet::new();
        let mut pending_accepted = Vec::new();
        let seeds: Vec<_> = seeds.collect();
        for seed in &seeds {
            if seed.accepted && accepted.insert(seed.node.value) {
                pending_accepted.push(seed.node.value);
            }
        }
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        parent,
                        reducer,
                        source_candidates: None,
                        suspended_source_page: None,
                        seen: HashMap::new(),
                        accepted,
                        pending_accepted,
                        live: BTreeSet::new(),
                        retired: BTreeSet::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        let mut roots = Vec::with_capacity(seeds.len());
        for seed in seeds {
            roots.push((
                seed.node,
                self.issue_credit(activation, CreditKind::Traversal),
            ));
        }
        Some((activation, roots))
    }

    /// Starts one activation with a single affine generator credit. The
    /// generator is replaced by one bounded source page; its continuation is
    /// not reissued until every traversal rooted in that page retires.
    fn start_source(
        &mut self,
        parent: Box<[RawInline]>,
        reducer: DeltaReducer,
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
                        parent,
                        reducer,
                        source_candidates,
                        suspended_source_page: None,
                        seen: HashMap::new(),
                        accepted: HashSet::new(),
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

    fn replace_traversal(
        &mut self,
        parent: ProducerCredit,
        successors: impl IntoIterator<Item = ResidualDeltaOutput>,
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

        let activation = self
            .state
            .activations
            .get_mut(&parent.key.activation)
            .expect("unknown delta activation");
        assert!(activation.live.remove(&parent.key.nonce));
        assert!(activation.retired.insert(parent.key.nonce));
        if !accepted.is_empty() && matches!(activation.reducer, DeltaReducer::Propose) {
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
            resumed_source,
            retired_source_page,
            quiescence,
        }
    }

    fn replace_source(
        &mut self,
        generator: ProducerCredit,
        roots: impl IntoIterator<Item = ResidualDeltaOutput>,
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
        let mut distinct_nodes = HashSet::with_capacity(roots.len());
        assert!(
            roots
                .iter()
                .all(|output| distinct_nodes.insert(output.node)),
            "one residual source page repeated a root node"
        );

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
            assert!(activation.live.contains(&generator.key.nonce));
            assert!(activation.suspended_source_page.is_none());
            for output in &roots {
                if output.accepted && activation.accepted.insert(output.node.value) {
                    accepted.push(output.node.value);
                }
            }
            had_stable_effect =
                matches!(activation.reducer, DeltaReducer::Propose) && !accepted.is_empty();
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

    fn parent(&self, activation: ActivationId) -> &[RawInline] {
        &self
            .state
            .activations
            .get(&activation)
            .expect("unknown delta activation")
            .parent
    }

    fn source_candidates(&self, activation: ActivationId) -> Option<&[RawInline]> {
        self.state
            .activations
            .get(&activation)
            .expect("unknown delta activation")
            .source_candidates
            .as_deref()
    }

    fn reduce_confirm(&self, proof: QuiescenceProof) -> ReducedConfirm {
        let activation = self
            .state
            .activations
            .get(&proof.activation)
            .expect("unknown delta activation");
        assert_eq!(activation.status, ActivationStatus::Quiescent);
        let DeltaReducer::Confirm { original } = &activation.reducer else {
            panic!("proposal activation received a confirmation proof");
        };
        // The reducer scans the immutable input sequence after quiescence.
        // Membership comes from Accepted, while order and multiplicity come
        // exclusively from the original candidates.
        let candidates = original
            .iter()
            .filter(|candidate| activation.accepted.contains(*candidate))
            .copied()
            .map(|candidate| (0, candidate))
            .collect();
        ReducedConfirm {
            parent: activation.parent.to_vec(),
            candidates,
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

#[derive(Debug)]
struct DeltaTask {
    activation: ActivationId,
    credit: ProducerCredit,
    node: ResidualDeltaNode,
}

#[derive(Debug)]
struct SourceTask {
    activation: ActivationId,
    credit: ProducerCredit,
    cursor: ResidualDeltaSourceCursor,
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
pub(super) enum DeltaStepOutcome {
    /// Cyclic work advanced without a stable acyclic effect and without
    /// retiring a scheduler-negative source page.
    Progress,
    /// A stable Candidate continuation was filed. It may be consumed
    /// immediately for first-result latency.
    Stable(ContinuationToken),
    /// At least one bounded source page retired without filing a stable
    /// effect. The outer scheduler must grow its width geometrically.
    DeadPage,
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
        parents: RowBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) {
        assert_eq!(desc.verb, DeltaVerb::Propose);
        let ranges = seed_ranges(&seeds, parents.row_count);
        let stride = desc.bound.count();
        let mut tasks = Vec::with_capacity(seeds.len());
        for (row, range) in ranges.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let Some((activation, roots)) = self.registry.start_many(
                parent,
                DeltaReducer::Propose,
                seeds[range].iter().map(|seed| seed.output),
            ) else {
                continue;
            };
            tasks.extend(roots.into_iter().map(|(node, credit)| DeltaTask {
                activation,
                credit,
                node,
            }));
        }
        self.file(desc, tasks);
    }

    pub(super) fn seed_source_proposals(&mut self, desc: DeltaDesc, parents: RowBatch) {
        assert_eq!(desc.verb, DeltaVerb::Propose);
        let stride = desc.bound.count();
        let mut tasks = Vec::with_capacity(parents.row_count);
        for row in 0..parents.row_count {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let (activation, credit) =
                self.registry
                    .start_source(parent, DeltaReducer::Propose, None);
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
        batch: CandidateBatch,
        seeds: Vec<ResidualDeltaSeed>,
    ) {
        assert_eq!(desc.verb, DeltaVerb::Confirm);
        let seed_ranges = seed_ranges(&seeds, batch.parents.row_count);
        let stride = desc.bound.count();
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
            let Some((activation, roots)) = self.registry.start_many(
                parent,
                DeltaReducer::Confirm { original },
                seeds[seed_range].iter().map(|seed| seed.output),
            ) else {
                continue;
            };
            tasks.extend(roots.into_iter().map(|(node, credit)| DeltaTask {
                activation,
                credit,
                node,
            }));
        }
        self.file(desc, tasks);
    }

    pub(super) fn seed_source_confirms(&mut self, desc: DeltaDesc, batch: CandidateBatch) {
        assert_eq!(desc.verb, DeltaVerb::Confirm);
        let stride = desc.bound.count();
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
                parent,
                DeltaReducer::Confirm { original },
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

    fn pop_source(&mut self) -> (DeltaDesc, SourceTask) {
        let id = *self
            .source_worklist
            .last_key_value()
            .expect("source pop requires live work")
            .0;
        let (task, empty) = {
            let bucket = self
                .source_worklist
                .get_mut(&id)
                .expect("selected source state");
            let task = bucket.tasks.pop().expect("live source bucket is nonempty");
            (task, bucket.tasks.is_empty())
        };
        if empty {
            self.source_worklist.remove(&id);
        }
        (self.interner.get(id).clone(), task)
    }

    #[allow(clippy::too_many_arguments)]
    fn file_proposal_effects(
        &self,
        desc: &DeltaDesc,
        activation: ActivationId,
        accepted: Vec<RawInline>,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
        if accepted.is_empty() {
            return None;
        }
        stats.candidates_proposed += accepted.len();
        stats.max_propose_candidates = stats.max_propose_candidates.max(accepted.len());
        let parent = self.registry.parent(activation).to_vec();
        let candidates = accepted.into_iter().map(|value| (0, value)).collect();
        file_with_span(
            stable,
            stable_interner,
            plan.len(),
            plan.action_span(),
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable: desc.variable,
                    relevant: desc.relevant.clone(),
                    checked: desc.checked.clone(),
                },
            },
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: parent,
                    row_count: 1,
                },
                candidates,
            }),
            stats,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn file_confirm_reduction(
        &self,
        desc: &DeltaDesc,
        proof: QuiescenceProof,
        plan: &ResidualPlan,
        stable: &mut Worklist,
        stable_interner: &mut StateInterner,
        stats: &mut ResidualStateStats,
    ) -> Option<ContinuationToken> {
        let reduced = self.registry.reduce_confirm(proof);
        if reduced.candidates.is_empty() {
            return None;
        }
        file_with_span(
            stable,
            stable_interner,
            plan.len(),
            plan.action_span(),
            StateDesc {
                bound: desc.bound,
                phase: ResidualPhase::Candidate {
                    variable: desc.variable,
                    relevant: desc.relevant.clone(),
                    checked: desc.checked.with_inserted(desc.leaf),
                },
            },
            StateBucket::Candidates(CandidateBatch {
                parents: RowBatch {
                    rows: reduced.parent,
                    row_count: 1,
                },
                candidates: reduced.candidates,
            }),
            stats,
        )
    }

    /// Executes one bounded source page or one structural product-state
    /// cohort. Traversal work has priority, so a page's root lineages retire
    /// before its suspended generator cursor can run again.
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
        let nodes: Vec<_> = tasks.iter().map(|task| task.node).collect();
        let mut successors = Vec::new();
        assert!(
            plan.resolve(root, desc.leaf).residual_delta_expand(
                desc.variable,
                &nodes,
                &mut successors,
            ),
            "delta expansion became unsupported after seeding"
        );
        let mut previous = 0u32;
        for (index, &(tag, _)) in successors.iter().enumerate() {
            assert!(tag < tasks.len() as u32, "delta successor tag out of range");
            assert!(
                index == 0 || tag >= previous,
                "delta successor tags are not grouped in ascending order"
            );
            previous = tag;
        }

        let mut next_tasks = Vec::new();
        let mut resumed_sources = Vec::new();
        let mut continuation = None;
        let mut dead_page = false;
        let mut cursor = 0usize;
        for (task_index, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.activation, task.credit.key.activation);
            let begin = cursor;
            while cursor < successors.len() && successors[cursor].0 as usize == task_index {
                cursor += 1;
            }
            let outcome = self.registry.replace_traversal(
                task.credit,
                successors[begin..cursor].iter().map(|(_, value)| *value),
            );
            for (node, credit) in outcome.children {
                next_tasks.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node,
                });
            }
            if let Some((source_cursor, credit)) = outcome.resumed_source {
                resumed_sources.push(SourceTask {
                    activation: task.activation,
                    credit,
                    cursor: source_cursor,
                });
            }
            if outcome
                .retired_source_page
                .is_some_and(|page| !page.had_stable_effect)
            {
                dead_page = true;
            }
            match desc.verb {
                DeltaVerb::Propose => {
                    if let Some(proof) = outcome.quiescence {
                        assert_eq!(proof.activation, task.activation);
                    }
                    prefer_continuation(
                        &mut continuation,
                        self.file_proposal_effects(
                            &desc,
                            task.activation,
                            outcome.accepted,
                            plan,
                            stable,
                            stable_interner,
                            stats,
                        ),
                    );
                }
                DeltaVerb::Confirm => {
                    if let Some(proof) = outcome.quiescence {
                        assert_eq!(proof.activation, task.activation);
                        prefer_continuation(
                            &mut continuation,
                            self.file_confirm_reduction(
                                &desc,
                                proof,
                                plan,
                                stable,
                                stable_interner,
                                stats,
                            ),
                        );
                    }
                }
            }
        }
        assert_eq!(cursor, successors.len());
        self.file(desc.clone(), next_tasks);
        self.file_source(desc, resumed_sources);
        if let Some(continuation) = continuation {
            DeltaStepOutcome::Stable(continuation)
        } else if dead_page {
            stats.delta_source_dead_pages += 1;
            DeltaStepOutcome::DeadPage
        } else {
            DeltaStepOutcome::Progress
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
        let (desc, task) = self.pop_source();
        assert_eq!(task.activation, task.credit.key.activation);
        let parent = self.registry.parent(task.activation).to_vec();
        let candidates = self
            .registry
            .source_candidates(task.activation)
            .map(<[RawInline]>::to_vec);
        let vars: Vec<VariableId> = desc.bound.into_iter().collect();
        let view = rows_view(&vars, &parent, 1);
        let mut roots = Vec::new();
        let page = plan
            .resolve(root, desc.leaf)
            .residual_delta_source_page(
                desc.variable,
                &view,
                candidates.as_deref(),
                task.cursor,
                width.max(1),
                &mut roots,
            )
            .expect("paged delta source became unsupported after seeding");
        assert!(page.examined <= width.max(1));
        assert!(roots.len() <= page.examined);
        if let Some(next) = page.next {
            match (task.cursor, next) {
                (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
                (
                    ResidualDeltaSourceCursor::After(previous),
                    ResidualDeltaSourceCursor::After(next),
                ) => assert!(next > previous, "residual source cursor did not advance"),
                (_, ResidualDeltaSourceCursor::Start) => {
                    panic!("residual source page restarted its cursor")
                }
            }
        }
        stats.delta_source_pages += 1;
        stats.delta_source_candidates_examined += page.examined;
        stats.delta_source_roots += roots.len();

        let outcome = self.registry.replace_source(task.credit, roots, page.next);
        let mut traversal = Vec::with_capacity(outcome.roots.len());
        for (node, credit) in outcome.roots {
            traversal.push(DeltaTask {
                activation: task.activation,
                credit,
                node,
            });
        }
        self.file(desc.clone(), traversal);
        if let Some((cursor, credit)) = outcome.resumed_source {
            self.file_source(
                desc.clone(),
                vec![SourceTask {
                    activation: task.activation,
                    credit,
                    cursor,
                }],
            );
        }

        let mut continuation = None;
        match desc.verb {
            DeltaVerb::Propose => {
                if let Some(proof) = outcome.quiescence {
                    assert_eq!(proof.activation, task.activation);
                }
                continuation = self.file_proposal_effects(
                    &desc,
                    task.activation,
                    outcome.accepted,
                    plan,
                    stable,
                    stable_interner,
                    stats,
                );
            }
            DeltaVerb::Confirm => {
                if let Some(proof) = outcome.quiescence {
                    assert_eq!(proof.activation, task.activation);
                    continuation = self.file_confirm_reduction(
                        &desc,
                        proof,
                        plan,
                        stable,
                        stable_interner,
                        stats,
                    );
                }
            }
        }
        if let Some(continuation) = continuation {
            DeltaStepOutcome::Stable(continuation)
        } else if outcome
            .retired_source_page
            .is_some_and(|page| !page.had_stable_effect)
        {
            stats.delta_source_dead_pages += 1;
            DeltaStepOutcome::DeadPage
        } else {
            DeltaStepOutcome::Progress
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
    use super::*;

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

    #[test]
    fn source_is_part_of_activation_local_novelty() {
        let mut registry = ProducerRegistry::new();
        let (activation, roots) = registry
            .start_many(
                Vec::new().into_boxed_slice(),
                DeltaReducer::Propose,
                [
                    sourced_output(1, 1, 0, false),
                    sourced_output(2, 2, 0, false),
                ],
            )
            .expect("two sources create one parent activation");

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
        let (activation, roots) = registry
            .start_many(
                Vec::new().into_boxed_slice(),
                DeltaReducer::Confirm {
                    original: vec![candidate, candidate].into_boxed_slice(),
                },
                [output(7, 0, true), output(7, 0, true)],
            )
            .expect("duplicate roots still share one reducer activation");
        let mut proof = None;
        for (_, root) in roots {
            let outcome = registry.replace_traversal(root, []);
            if outcome.quiescence.is_some() {
                assert!(proof.replace(outcome.quiescence.unwrap()).is_none());
            }
        }

        let proof = proof.expect("all root credits quiesced");
        assert_eq!(proof.activation, activation);
        assert_eq!(
            registry.reduce_confirm(proof).candidates,
            vec![(0, candidate), (0, candidate)]
        );
    }

    #[test]
    fn confirm_reducer_joins_multiple_roots_before_filtering_the_sequence() {
        let seed = value(1);
        let first = value(2);
        let second = value(3);
        let rejected = value(4);
        let mut registry = ProducerRegistry::new();
        let (activation, roots) = registry
            .start_many(
                vec![value(9)].into_boxed_slice(),
                DeltaReducer::Confirm {
                    original: vec![second, seed, first, rejected, second].into_boxed_slice(),
                },
                [output(1, 0, false), output(5, 0, false)],
            )
            .expect("two roots create one activation");
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
            if outcome.quiescence.is_some() {
                assert!(proof.replace(outcome.quiescence.unwrap()).is_none());
            }
        }

        let proof = proof.expect("last producer must prove quiescence");
        assert_eq!(proof.activation, activation);
        let reduced = registry.reduce_confirm(proof);
        assert_eq!(reduced.parent, vec![value(9)]);
        assert_eq!(
            reduced.candidates,
            vec![(0, second), (0, first), (0, second)]
        );
    }

    #[test]
    fn source_cursor_resumes_only_after_every_page_root_retires() {
        let mut registry = ProducerRegistry::new();
        let (activation, generator) =
            registry.start_source(Vec::new().into_boxed_slice(), DeltaReducer::Propose, None);
        let next = ResidualDeltaSourceCursor::After(value(9));
        let page = registry.replace_source(
            generator,
            [
                sourced_output(1, 1, 0, false),
                sourced_output(2, 2, 0, false),
            ],
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
    fn grouped_confirm_waits_for_all_source_pages_before_reducing() {
        let first = value(1);
        let second = value(2);
        let rejected = value(3);
        let mut registry = ProducerRegistry::new();
        let (activation, generator) = registry.start_source(
            Vec::new().into_boxed_slice(),
            DeltaReducer::Confirm {
                original: vec![second, first, rejected, first].into_boxed_slice(),
            },
            Some(vec![first, second, rejected].into_boxed_slice()),
        );
        let first_page = registry.replace_source(
            generator,
            [sourced_output(1, 1, 0, true)],
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

        let second_page = registry.replace_source(generator, [sourced_output(2, 2, 0, true)], None);
        assert!(second_page.quiescence.is_none());
        let (_, second_root) = second_page.roots.into_iter().next().expect("second root");
        let second_retired = registry.replace_traversal(second_root, []);
        let proof = second_retired
            .quiescence
            .expect("the terminal page proves reducer quiescence");
        assert_eq!(proof.activation, activation);
        assert_eq!(
            registry.reduce_confirm(proof).candidates,
            vec![(0, second), (0, first), (0, first)]
        );
    }

    #[test]
    fn different_activation_cursors_share_one_canonical_source_bucket() {
        let mut scheduler = DeltaScheduler::new();
        let desc = DeltaDesc::propose(
            VariableSet::new_empty(),
            0,
            0,
            ChildSet::empty(1),
            ChildSet::empty(1),
        );
        let (first_activation, first_credit) = scheduler.registry.start_source(
            Vec::new().into_boxed_slice(),
            DeltaReducer::Propose,
            None,
        );
        let (second_activation, second_credit) = scheduler.registry.start_source(
            Vec::new().into_boxed_slice(),
            DeltaReducer::Propose,
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
}
