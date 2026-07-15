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

/// One affine parent reducer scope. Several speculative source roots may own
/// live credits inside it; they share novelty and Accepted, while source stays
/// in each node so their product states cannot suppress one another.
#[derive(Clone)]
struct Activation {
    parent: Box<[RawInline]>,
    reducer: DeltaReducer,
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
    credit_owner: BTreeMap<CreditNonce, ActivationId>,
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
        let mut live = BTreeSet::new();
        let mut accepted = HashSet::new();
        let mut pending_accepted = Vec::new();
        let mut roots = Vec::with_capacity(seeds.size_hint().0);
        for seed in seeds {
            let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
            assert!(
                self.state.credit_owner.insert(nonce, activation).is_none(),
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
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        parent,
                        reducer,
                        seen: HashMap::new(),
                        accepted,
                        pending_accepted,
                        live,
                        retired: BTreeSet::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        Some((activation, roots))
    }

    fn replace(
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
            owner, parent.key.activation,
            "delta credit changed activation"
        );

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
            let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
            assert!(
                self.state
                    .credit_owner
                    .insert(nonce, parent.key.activation)
                    .is_none(),
                "delta credit nonce was reused"
            );
            let activation = self
                .state
                .activations
                .get_mut(&parent.key.activation)
                .expect("unknown delta activation");
            assert!(activation.live.insert(nonce));
            children.push((
                successor,
                ProducerCredit {
                    brand: self.brand,
                    key: CreditKey {
                        activation: parent.key.activation,
                        nonce,
                    },
                },
            ));
        }

        let activation = self
            .state
            .activations
            .get_mut(&parent.key.activation)
            .expect("unknown delta activation");
        assert!(activation.live.remove(&parent.key.nonce));
        assert!(activation.retired.insert(parent.key.nonce));
        let quiescence = if activation.live.is_empty() {
            activation.status = ActivationStatus::Quiescent;
            Some(QuiescenceProof {
                activation: parent.key.activation,
            })
        } else {
            None
        };
        ReplaceOutcome {
            children,
            accepted,
            quiescence,
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

#[derive(Default)]
struct DeltaBucket {
    tasks: Vec<DeltaTask>,
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
}

impl DeltaScheduler {
    pub(super) fn new() -> Self {
        Self {
            registry: ProducerRegistry::new(),
            interner: DeltaInterner::default(),
            worklist: BTreeMap::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.worklist.is_empty()
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
    ) -> Option<ContinuationToken> {
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
        let mut continuation = None;
        let mut cursor = 0usize;
        for (task_index, task) in tasks.into_iter().enumerate() {
            assert_eq!(task.activation, task.credit.key.activation);
            let begin = cursor;
            while cursor < successors.len() && successors[cursor].0 as usize == task_index {
                cursor += 1;
            }
            let outcome = self.registry.replace(
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
            match desc.verb {
                DeltaVerb::Propose => {
                    if let Some(proof) = outcome.quiescence {
                        assert_eq!(proof.activation, task.activation);
                    }
                    if !outcome.accepted.is_empty() {
                        stats.candidates_proposed += outcome.accepted.len();
                        stats.max_propose_candidates =
                            stats.max_propose_candidates.max(outcome.accepted.len());
                        let parent = self.registry.parent(task.activation).to_vec();
                        let candidates = outcome
                            .accepted
                            .into_iter()
                            .map(|value| (0, value))
                            .collect();
                        prefer_continuation(
                            &mut continuation,
                            file_with_plan(
                                stable,
                                stable_interner,
                                plan,
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
                            ),
                        );
                    }
                }
                DeltaVerb::Confirm => {
                    if let Some(proof) = outcome.quiescence {
                        assert_eq!(proof.activation, task.activation);
                        let reduced = self.registry.reduce_confirm(proof);
                        if !reduced.candidates.is_empty() {
                            prefer_continuation(
                                &mut continuation,
                                file_with_plan(
                                    stable,
                                    stable_interner,
                                    plan,
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
                                ),
                            );
                        }
                    }
                }
            }
        }
        assert_eq!(cursor, successors.len());
        self.file(desc, next_tasks);
        continuation
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
        assert!(
            remap.is_empty(),
            "delta registry held a live credit without a scheduled task"
        );
        Self {
            registry,
            interner: self.interner.clone(),
            worklist,
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
            children.extend(registry.replace(root, [successor]).children);
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
            let outcome = registry.replace(root, []);
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
            let outcome = registry.replace(root, [successor]);
            assert!(outcome.quiescence.is_none());
            children.extend(outcome.children);
        }
        let mut proof = None;
        for (_, child) in children {
            let outcome = registry.replace(child, []);
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
}
