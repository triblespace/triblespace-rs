//! Executable design probe for activation-scoped RPQ producer accounting.
//!
//! This deliberately lives in an integration test: it pins the affine API and
//! its concurrency laws without changing the production query engine. A lost
//! live credit fails closed by leaking liveness; it can delay quiescence, but
//! can never manufacture a premature negative result.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

static NEXT_REGISTRY_BRAND: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RegistryBrand(u64);

impl RegistryBrand {
    fn fresh() -> Self {
        let value = NEXT_REGISTRY_BRAND
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .expect("producer-registry brand space exhausted");
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ActivationId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditNonce(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditKey {
    activation: ActivationId,
    nonce: CreditNonce,
}

/// One affine authority to replace a producer with zero or more successors.
///
/// This type intentionally does not implement `Clone`. The only duplication
/// below is the explicit registry-wide deep-clone/remap operation. Tests use a
/// private adversarial constructor to exercise misuse rejection.
#[derive(Debug)]
struct ProducerCredit {
    brand: RegistryBrand,
    key: CreditKey,
}

#[derive(Debug)]
struct Discovery<K, O> {
    successor: K,
    accepted: Option<O>,
}

impl<K, O> Discovery<K, O> {
    fn successor(successor: K) -> Self {
        Self {
            successor,
            accepted: None,
        }
    }

    fn accepted(successor: K, accepted: O) -> Self {
        Self {
            successor,
            accepted: Some(accepted),
        }
    }
}

#[derive(Debug)]
struct ChildWork<K> {
    successor: K,
    credit: ProducerCredit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QuiescenceKind {
    EnumerationComplete,
    TargetFalse,
}

/// The unique negative/enumeration terminal proof for one activation.
///
/// This proof intentionally does not implement `Clone`. Activation identifiers
/// are never reused, so no generation field is necessary.
#[derive(Debug, Eq, PartialEq)]
struct QuiescenceProof {
    activation: ActivationId,
    kind: QuiescenceKind,
}

/// The unique positive terminal proof for one targeted activation.
#[derive(Debug, Eq, PartialEq)]
struct TargetTrueProof {
    activation: ActivationId,
}

#[derive(Debug, Eq, PartialEq)]
enum TerminalProof {
    Quiescent(QuiescenceProof),
    TargetTrue(TargetTrueProof),
}

#[derive(Debug)]
struct ReplaceOutcome<K, O> {
    children: Vec<ChildWork<K>>,
    accepted: Vec<O>,
    terminal: Option<TerminalProof>,
    cancelled: bool,
}

impl<K, O> ReplaceOutcome<K, O> {
    fn cancelled() -> Self {
        Self {
            children: Vec::new(),
            accepted: Vec::new(),
            terminal: None,
            cancelled: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryError {
    WrongRegistry,
    UnknownActivation,
    WrongActivation,
    UnknownCredit,
    ReplayedCredit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivationStatus {
    Open,
    Quiescent,
    TargetTrue,
}

#[derive(Clone, Debug)]
struct Activation<K, O> {
    target: Option<O>,
    seen: HashSet<K>,
    accepted: HashSet<O>,
    live: BTreeSet<CreditNonce>,
    cancelled: BTreeSet<CreditNonce>,
    retired: BTreeSet<CreditNonce>,
    status: ActivationStatus,
}

#[derive(Clone, Debug)]
struct RegistryState<K, O> {
    next_activation: u64,
    next_credit: u64,
    credit_owner: BTreeMap<CreditNonce, ActivationId>,
    activations: BTreeMap<ActivationId, Activation<K, O>>,
}

/// Registry identity is shared with `Arc`, never duplicated with `Clone`.
struct ProducerRegistry<K, O> {
    brand: RegistryBrand,
    state: Mutex<RegistryState<K, O>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActivationSnapshot {
    status: ActivationStatus,
    target: bool,
    seen: usize,
    accepted: usize,
    live: usize,
    cancelled: usize,
    retired: usize,
    next_activation: u64,
    next_credit: u64,
    known_credits: usize,
}

/// A deep-cloned registry plus a one-shot remap for every outstanding credit.
///
/// The cloned registry has a fresh brand but preserves activation and credit
/// numbers. Remapped credits can therefore drive it independently without any
/// ordinary `Clone` implementation on affine authority.
struct DeepClone<K, O> {
    registry: ProducerRegistry<K, O>,
    credits: BTreeMap<CreditKey, ProducerCredit>,
}

impl<K, O> DeepClone<K, O> {
    fn take(&mut self, key: CreditKey) -> Option<ProducerCredit> {
        self.credits.remove(&key)
    }

    fn remaining(&self) -> usize {
        self.credits.len()
    }
}

impl<K, O> ProducerRegistry<K, O>
where
    K: Clone + Eq + Hash,
    O: Clone + Eq + Hash,
{
    fn new() -> Self {
        Self {
            brand: RegistryBrand::fresh(),
            state: Mutex::new(RegistryState {
                next_activation: 0,
                next_credit: 0,
                credit_owner: BTreeMap::new(),
                activations: BTreeMap::new(),
            }),
        }
    }

    fn start(&self, target: Option<O>) -> (ActivationId, ProducerCredit) {
        let mut state = self.state.lock().expect("producer registry poisoned");
        let activation = ActivationId(take_monotonic(&mut state.next_activation, "activation"));
        let nonce = CreditNonce(take_monotonic(&mut state.next_credit, "credit"));
        let inserted_owner = state.credit_owner.insert(nonce, activation);
        assert!(inserted_owner.is_none(), "credit nonce was reused");

        let mut live = BTreeSet::new();
        assert!(live.insert(nonce));
        let previous = state.activations.insert(
            activation,
            Activation {
                target,
                seen: HashSet::new(),
                accepted: HashSet::new(),
                live,
                cancelled: BTreeSet::new(),
                retired: BTreeSet::new(),
                status: ActivationStatus::Open,
            },
        );
        assert!(previous.is_none(), "activation identifier was reused");

        (
            activation,
            ProducerCredit {
                brand: self.brand,
                key: CreditKey { activation, nonce },
            },
        )
    }

    /// Atomically consumes one live parent and replaces it with novel children.
    ///
    /// Novelty admission, exactly-once acceptance, child minting, and terminal
    /// detection share one mutex linearization point. Children enter the live
    /// set before their parent leaves, so a `1 -> N` replacement never even
    /// transiently reaches zero. Accepted output may accompany children;
    /// quiescence and children are mutually exclusive.
    fn replace<I>(
        &self,
        parent: ProducerCredit,
        discoveries: I,
    ) -> Result<ReplaceOutcome<K, O>, RegistryError>
    where
        I: IntoIterator<Item = Discovery<K, O>>,
    {
        if parent.brand != self.brand {
            return Err(RegistryError::WrongRegistry);
        }
        let discoveries: Vec<_> = discoveries.into_iter().collect();
        let mut state = self.state.lock().expect("producer registry poisoned");

        if !state.activations.contains_key(&parent.key.activation) {
            return Err(RegistryError::UnknownActivation);
        }
        let Some(owner) = state.credit_owner.get(&parent.key.nonce) else {
            return Err(RegistryError::UnknownCredit);
        };
        if *owner != parent.key.activation {
            return Err(RegistryError::WrongActivation);
        }

        let activation = state
            .activations
            .get_mut(&parent.key.activation)
            .expect("activation existence was checked");
        if activation.cancelled.remove(&parent.key.nonce) {
            assert_eq!(activation.status, ActivationStatus::TargetTrue);
            assert!(activation.retired.insert(parent.key.nonce));
            return Ok(ReplaceOutcome::cancelled());
        }
        if activation.retired.contains(&parent.key.nonce) {
            return Err(RegistryError::ReplayedCredit);
        }
        if !activation.live.contains(&parent.key.nonce) {
            return Err(RegistryError::UnknownCredit);
        }
        assert_eq!(activation.status, ActivationStatus::Open);

        let mut novel = Vec::new();
        let mut emitted = Vec::new();
        let mut target_hit = false;
        for discovery in discoveries {
            if !activation.seen.insert(discovery.successor.clone()) {
                continue;
            }
            novel.push(discovery.successor);
            if let Some(output) = discovery.accepted {
                let is_target = activation.target.as_ref() == Some(&output);
                if activation.accepted.insert(output.clone()) {
                    emitted.push(output);
                }
                if is_target {
                    target_hit = true;
                    break;
                }
            }
        }

        if target_hit {
            assert!(activation.live.remove(&parent.key.nonce));
            assert!(activation.retired.insert(parent.key.nonce));
            let siblings = std::mem::take(&mut activation.live);
            activation.cancelled.extend(siblings);
            activation.status = ActivationStatus::TargetTrue;
            return Ok(ReplaceOutcome {
                children: Vec::new(),
                accepted: emitted,
                terminal: Some(TerminalProof::TargetTrue(TargetTrueProof {
                    activation: parent.key.activation,
                })),
                cancelled: false,
            });
        }

        let mut children = Vec::with_capacity(novel.len());
        for successor in novel {
            let nonce = CreditNonce(take_monotonic(&mut state.next_credit, "credit"));
            let inserted_owner = state.credit_owner.insert(nonce, parent.key.activation);
            assert!(inserted_owner.is_none(), "credit nonce was reused");
            let activation = state
                .activations
                .get_mut(&parent.key.activation)
                .expect("activation existence was checked");
            assert!(activation.live.insert(nonce));
            children.push(ChildWork {
                successor,
                credit: ProducerCredit {
                    brand: self.brand,
                    key: CreditKey {
                        activation: parent.key.activation,
                        nonce,
                    },
                },
            });
        }

        let activation = state
            .activations
            .get_mut(&parent.key.activation)
            .expect("activation existence was checked");
        assert!(activation.live.remove(&parent.key.nonce));
        assert!(activation.retired.insert(parent.key.nonce));
        let terminal = if activation.live.is_empty() {
            activation.status = ActivationStatus::Quiescent;
            Some(TerminalProof::Quiescent(QuiescenceProof {
                activation: parent.key.activation,
                kind: if activation.target.is_some() {
                    QuiescenceKind::TargetFalse
                } else {
                    QuiescenceKind::EnumerationComplete
                },
            }))
        } else {
            None
        };

        Ok(ReplaceOutcome {
            children,
            accepted: emitted,
            terminal,
            cancelled: false,
        })
    }

    fn deep_clone(&self) -> DeepClone<K, O> {
        let snapshot = self
            .state
            .lock()
            .expect("producer registry poisoned")
            .clone();
        let brand = RegistryBrand::fresh();
        let mut credits = BTreeMap::new();
        for (&activation_id, activation) in &snapshot.activations {
            for &nonce in activation.live.iter().chain(&activation.cancelled) {
                let key = CreditKey {
                    activation: activation_id,
                    nonce,
                };
                let previous = credits.insert(key, ProducerCredit { brand, key });
                assert!(previous.is_none(), "outstanding credit sets overlapped");
            }
        }
        DeepClone {
            registry: Self {
                brand,
                state: Mutex::new(snapshot),
            },
            credits,
        }
    }

    fn snapshot(&self, activation: ActivationId) -> ActivationSnapshot {
        let state = self.state.lock().expect("producer registry poisoned");
        let activation = state
            .activations
            .get(&activation)
            .expect("unknown activation in test snapshot");
        ActivationSnapshot {
            status: activation.status,
            target: activation.target.is_some(),
            seen: activation.seen.len(),
            accepted: activation.accepted.len(),
            live: activation.live.len(),
            cancelled: activation.cancelled.len(),
            retired: activation.retired.len(),
            next_activation: state.next_activation,
            next_credit: state.next_credit,
            known_credits: state.credit_owner.len(),
        }
    }
}

fn take_monotonic(counter: &mut u64, kind: &str) -> u64 {
    let current = *counter;
    *counter = current
        .checked_add(1)
        .unwrap_or_else(|| panic!("{kind} identifier space exhausted"));
    current
}

/// Deliberately forges authority so misuse paths can be tested. This is not
/// part of the production-shaped API above.
fn adversarial_credit(
    brand: RegistryBrand,
    activation: ActivationId,
    nonce: CreditNonce,
) -> ProducerCredit {
    ProducerCredit {
        brand,
        key: CreditKey { activation, nonce },
    }
}

fn split_two<K, O>(outcome: ReplaceOutcome<K, O>) -> [ChildWork<K>; 2] {
    assert!(outcome.accepted.is_empty());
    assert!(outcome.terminal.is_none());
    assert!(!outcome.cancelled);
    outcome
        .children
        .try_into()
        .unwrap_or_else(|children: Vec<_>| panic!("expected two children, got {}", children.len()))
}

#[test]
fn one_to_two_replacement_never_closes_and_may_emit() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(None);
    let outcome = registry
        .replace(
            root,
            [Discovery::accepted(10, 90), Discovery::successor(11)],
        )
        .unwrap();

    assert_eq!(outcome.accepted, vec![90]);
    assert_eq!(
        outcome
            .children
            .iter()
            .map(|child| child.successor)
            .collect::<Vec<_>>(),
        vec![10, 11]
    );
    assert!(outcome.terminal.is_none());
    assert!(!outcome.cancelled);
    let snapshot = registry.snapshot(activation);
    assert_eq!(snapshot.status, ActivationStatus::Open);
    assert_eq!(snapshot.live, 2);
    assert_eq!(snapshot.seen, 2);
    assert_eq!(snapshot.accepted, 1);
}

#[test]
fn concurrent_last_retire_closes_exactly_once() {
    use std::sync::{Arc, Barrier};

    let registry = Arc::new(ProducerRegistry::<u8, u8>::new());
    let (activation, root) = registry.start(None);
    let [left, right] = split_two(
        registry
            .replace(root, [Discovery::successor(1), Discovery::successor(2)])
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(3));
    let handles = [left, right].map(|child| {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            registry.replace(child.credit, []).unwrap()
        })
    });
    barrier.wait();
    let outcomes = handles.map(|handle| handle.join().unwrap());

    let proofs: Vec<_> = outcomes
        .into_iter()
        .filter_map(|outcome| outcome.terminal)
        .collect();
    assert_eq!(proofs.len(), 1);
    assert_eq!(
        proofs[0],
        TerminalProof::Quiescent(QuiescenceProof {
            activation,
            kind: QuiescenceKind::EnumerationComplete,
        })
    );
    let snapshot = registry.snapshot(activation);
    assert_eq!(snapshot.status, ActivationStatus::Quiescent);
    assert_eq!(snapshot.live, 0);
}

#[test]
fn duplicate_successor_mints_one_credit() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(None);
    let outcome = registry
        .replace(
            root,
            [
                Discovery::successor(7),
                Discovery::accepted(7, 99),
                Discovery::successor(7),
            ],
        )
        .unwrap();

    assert_eq!(outcome.children.len(), 1);
    assert_eq!(outcome.children[0].successor, 7);
    assert!(outcome.accepted.is_empty());
    let snapshot = registry.snapshot(activation);
    assert_eq!(snapshot.seen, 1);
    assert_eq!(snapshot.accepted, 0);
    assert_eq!(snapshot.live, 1);
}

#[test]
fn late_duplicate_acceptance_emits_once_per_activation() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(None);
    let [left, right] = split_two(
        registry
            .replace(root, [Discovery::successor(1), Discovery::successor(2)])
            .unwrap(),
    );

    let first = registry
        .replace(left.credit, [Discovery::accepted(3, 42)])
        .unwrap();
    let second = registry
        .replace(right.credit, [Discovery::accepted(4, 42)])
        .unwrap();
    assert_eq!(first.accepted, vec![42]);
    assert!(second.accepted.is_empty());
    assert_eq!(first.children.len(), 1);
    assert_eq!(second.children.len(), 1);
    assert_eq!(registry.snapshot(activation).accepted, 1);
}

#[test]
fn closed_activation_rejects_replay_without_mutation() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(None);
    let replay = adversarial_credit(root.brand, root.key.activation, root.key.nonce);
    let outcome = registry.replace(root, []).unwrap();
    assert_eq!(
        outcome.terminal,
        Some(TerminalProof::Quiescent(QuiescenceProof {
            activation,
            kind: QuiescenceKind::EnumerationComplete,
        }))
    );
    let before = registry.snapshot(activation);

    assert_eq!(
        registry
            .replace(replay, [Discovery::accepted(9, 9)])
            .unwrap_err(),
        RegistryError::ReplayedCredit
    );
    assert_eq!(registry.snapshot(activation), before);
}

#[test]
fn target_true_never_degrades_to_false() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(Some(99));
    let [winner, sibling] = split_two(
        registry
            .replace(root, [Discovery::successor(1), Discovery::successor(2)])
            .unwrap(),
    );

    let positive = registry
        .replace(winner.credit, [Discovery::accepted(3, 99)])
        .unwrap();
    assert_eq!(positive.accepted, vec![99]);
    assert_eq!(
        positive.terminal,
        Some(TerminalProof::TargetTrue(TargetTrueProof { activation }))
    );
    assert_eq!(registry.snapshot(activation).cancelled, 1);

    let cancelled = registry
        .replace(sibling.credit, [Discovery::accepted(4, 5)])
        .unwrap();
    assert!(cancelled.cancelled);
    assert!(cancelled.children.is_empty());
    assert!(cancelled.accepted.is_empty());
    assert!(cancelled.terminal.is_none());
    let snapshot = registry.snapshot(activation);
    assert_eq!(snapshot.status, ActivationStatus::TargetTrue);
    assert_eq!(snapshot.live, 0);
    assert_eq!(snapshot.cancelled, 0);
    assert_eq!(snapshot.accepted, 1);
}

#[test]
fn target_negative_proof_appears_only_at_zero_live_credits() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(Some(99));
    let [left, right] = split_two(
        registry
            .replace(root, [Discovery::successor(1), Discovery::successor(2)])
            .unwrap(),
    );

    let first = registry.replace(left.credit, []).unwrap();
    assert!(first.terminal.is_none());
    assert_eq!(registry.snapshot(activation).live, 1);
    let last = registry.replace(right.credit, []).unwrap();
    assert_eq!(
        last.terminal,
        Some(TerminalProof::Quiescent(QuiescenceProof {
            activation,
            kind: QuiescenceKind::TargetFalse,
        }))
    );
    let snapshot = registry.snapshot(activation);
    assert_eq!(snapshot.status, ActivationStatus::Quiescent);
    assert_eq!(snapshot.live, 0);
}

#[test]
fn cancellation_tombstones_make_all_outstanding_work_inert() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(Some(99));
    let forked = registry
        .replace(
            root,
            [
                Discovery::successor(1),
                Discovery::successor(2),
                Discovery::successor(3),
            ],
        )
        .unwrap();
    let mut children = forked.children.into_iter();
    let winner = children.next().unwrap();
    let siblings: Vec<_> = children.collect();
    registry
        .replace(winner.credit, [Discovery::accepted(4, 99)])
        .unwrap();
    let before = registry.snapshot(activation);
    assert_eq!(before.cancelled, 2);

    for sibling in siblings {
        let outcome = registry
            .replace(sibling.credit, [Discovery::accepted(200, 17)])
            .unwrap();
        assert!(outcome.cancelled);
        assert!(outcome.children.is_empty());
        assert!(outcome.accepted.is_empty());
        assert!(outcome.terminal.is_none());
    }
    let after = registry.snapshot(activation);
    assert_eq!(after.status, ActivationStatus::TargetTrue);
    assert_eq!(after.seen, before.seen);
    assert_eq!(after.accepted, before.accepted);
    assert_eq!(after.next_credit, before.next_credit);
    assert_eq!(after.cancelled, 0);
}

#[test]
fn deep_clone_remaps_every_live_credit_and_closes_independently() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (activation, root) = registry.start(None);
    let children = split_two(
        registry
            .replace(root, [Discovery::successor(1), Discovery::successor(2)])
            .unwrap(),
    );
    let keys = children.each_ref().map(|child| child.credit.key);
    let original_brand = registry.brand;
    let mut cloned = registry.deep_clone();
    assert_ne!(cloned.registry.brand, original_brand);
    assert_eq!(cloned.remaining(), 2);
    assert_eq!(
        cloned.registry.snapshot(activation),
        registry.snapshot(activation)
    );

    let wrong_brand = adversarial_credit(original_brand, keys[0].activation, keys[0].nonce);
    assert_eq!(
        cloned.registry.replace(wrong_brand, []).unwrap_err(),
        RegistryError::WrongRegistry
    );

    let [original_left, original_right] = children;
    assert!(registry
        .replace(original_left.credit, [])
        .unwrap()
        .terminal
        .is_none());
    let original_terminal = registry
        .replace(original_right.credit, [])
        .unwrap()
        .terminal;

    let clone_left = cloned.take(keys[0]).unwrap();
    assert!(cloned.take(keys[0]).is_none());
    assert!(cloned
        .registry
        .replace(clone_left, [])
        .unwrap()
        .terminal
        .is_none());
    let clone_right = cloned.take(keys[1]).unwrap();
    let clone_terminal = cloned.registry.replace(clone_right, []).unwrap().terminal;

    let expected = Some(TerminalProof::Quiescent(QuiescenceProof {
        activation,
        kind: QuiescenceKind::EnumerationComplete,
    }));
    assert_eq!(original_terminal, expected);
    assert_eq!(clone_terminal, expected);
    assert_eq!(cloned.remaining(), 0);
    assert_eq!(
        registry.snapshot(activation).status,
        ActivationStatus::Quiescent
    );
    assert_eq!(
        cloned.registry.snapshot(activation).status,
        ActivationStatus::Quiescent
    );
}

#[test]
fn wrong_registry_activation_credit_and_replay_are_rejected() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let other_registry = ProducerRegistry::<u8, u8>::new();
    let (first_id, first) = registry.start(None);
    let (second_id, second) = registry.start(None);
    let replay = adversarial_credit(first.brand, first.key.activation, first.key.nonce);

    let wrong_registry = adversarial_credit(first.brand, first.key.activation, first.key.nonce);
    assert_eq!(
        other_registry.replace(wrong_registry, []).unwrap_err(),
        RegistryError::WrongRegistry
    );
    let unknown_activation = adversarial_credit(
        registry.brand,
        ActivationId(u64::MAX),
        CreditNonce(u64::MAX),
    );
    assert_eq!(
        registry.replace(unknown_activation, []).unwrap_err(),
        RegistryError::UnknownActivation
    );
    let wrong_activation = adversarial_credit(registry.brand, second_id, first.key.nonce);
    assert_eq!(
        registry.replace(wrong_activation, []).unwrap_err(),
        RegistryError::WrongActivation
    );
    let unknown_credit = adversarial_credit(registry.brand, first_id, CreditNonce(u64::MAX));
    assert_eq!(
        registry.replace(unknown_credit, []).unwrap_err(),
        RegistryError::UnknownCredit
    );

    assert!(registry.replace(first, []).unwrap().terminal.is_some());
    assert_eq!(
        registry.replace(replay, []).unwrap_err(),
        RegistryError::ReplayedCredit
    );
    assert!(registry.replace(second, []).unwrap().terminal.is_some());
}

#[test]
fn activation_and_credit_identifiers_are_never_reused() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (first_id, first) = registry.start(None);
    let first_nonce = first.key.nonce;
    registry.replace(first, []).unwrap();

    let (second_id, second) = registry.start(None);
    let second_nonce = second.key.nonce;
    assert!(second_id > first_id);
    assert!(second_nonce > first_nonce);
    registry.replace(second, []).unwrap();

    let (third_id, third) = registry.start(None);
    assert!(third_id > second_id);
    assert!(third.key.nonce > second_nonce);
}

#[test]
fn dropping_a_live_credit_fails_closed_by_leaking_liveness() {
    let registry = ProducerRegistry::<u8, u8>::new();
    let (abandoned_id, abandoned) = registry.start(Some(99));
    drop(abandoned);
    let before = registry.snapshot(abandoned_id);
    assert_eq!(before.status, ActivationStatus::Open);
    assert_eq!(before.live, 1);

    let (independent_id, independent) = registry.start(Some(99));
    let independent_terminal = registry.replace(independent, []).unwrap().terminal;
    assert_eq!(
        independent_terminal,
        Some(TerminalProof::Quiescent(QuiescenceProof {
            activation: independent_id,
            kind: QuiescenceKind::TargetFalse,
        }))
    );
    let after = registry.snapshot(abandoned_id);
    assert_eq!(after.status, ActivationStatus::Open);
    assert_eq!(after.live, 1);
    assert_eq!(after.seen, before.seen);
    assert_eq!(after.accepted, before.accepted);
}
