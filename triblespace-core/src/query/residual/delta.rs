//! Cyclic proposal stratum for the canonical residual scheduler.
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
struct Activation {
    parent: Box<[RawInline]>,
    seen: HashSet<RawInline>,
    accepted: HashSet<RawInline>,
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
    children: Vec<(RawInline, ProducerCredit)>,
    accepted: Vec<RawInline>,
    quiescence: Option<QuiescenceProof>,
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

    fn start(&mut self, parent: Box<[RawInline]>) -> (ActivationId, ProducerCredit) {
        let activation = ActivationId(take_monotonic(
            &mut self.state.next_activation,
            "activation",
        ));
        let nonce = CreditNonce(take_monotonic(&mut self.state.next_credit, "credit"));
        assert!(
            self.state.credit_owner.insert(nonce, activation).is_none(),
            "delta credit nonce was reused"
        );
        let mut live = BTreeSet::new();
        assert!(live.insert(nonce));
        assert!(
            self.state
                .activations
                .insert(
                    activation,
                    Activation {
                        parent,
                        seen: HashSet::new(),
                        accepted: HashSet::new(),
                        live,
                        retired: BTreeSet::new(),
                        status: ActivationStatus::Open,
                    },
                )
                .is_none(),
            "delta activation identifier was reused"
        );
        (
            activation,
            ProducerCredit {
                brand: self.brand,
                key: CreditKey { activation, nonce },
            },
        )
    }

    fn replace(
        &mut self,
        parent: ProducerCredit,
        successors: impl IntoIterator<Item = RawInline>,
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
        let mut accepted = Vec::new();
        for successor in successors {
            if activation.seen.insert(successor) {
                novel.push(successor);
                assert!(activation.accepted.insert(successor));
                accepted.push(successor);
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

#[derive(Debug)]
struct DeltaTask {
    activation: ActivationId,
    credit: ProducerCredit,
    node: RawInline,
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
        seeds: Vec<RawInline>,
    ) {
        assert_eq!(
            seeds.len(),
            parents.row_count,
            "delta seed hook must emit one node per parent"
        );
        let stride = desc.bound.count();
        let mut tasks = Vec::with_capacity(parents.row_count);
        for (row, seed) in seeds.into_iter().enumerate() {
            let start = row * stride;
            let parent = parents.rows[start..start + stride]
                .to_vec()
                .into_boxed_slice();
            let (activation, credit) = self.registry.start(parent);
            tasks.push(DeltaTask {
                activation,
                credit,
                node: seed,
            });
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

    /// Executes one structural cohort and files accepted endpoints back into
    /// the ordinary acyclic Candidate continuation.
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
        assert_eq!(desc.verb, DeltaVerb::Propose);
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
            if let Some(proof) = outcome.quiescence {
                assert_eq!(proof.activation, task.activation);
            }
            for (node, credit) in outcome.children {
                next_tasks.push(DeltaTask {
                    activation: task.activation,
                    credit,
                    node,
                });
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
                    file(
                        stable,
                        stable_interner,
                        plan.len(),
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
