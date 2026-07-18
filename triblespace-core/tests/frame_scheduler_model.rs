//! Executable architecture model for cohosted residual frames.
//!
//! This deliberately does not execute `Constraint`s or model dynamic frame
//! calls. It pins the affine credit, activation-local reducer, plan-local
//! readiness, and outer selection laws that production wiring must preserve.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_BRAND: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RegistryBrand(u64);

impl RegistryBrand {
    fn fresh() -> Self {
        Self(NEXT_BRAND.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PlanId(u16);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ActivationId(u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditNonce(u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CreditKey {
    activation: ActivationId,
    nonce: CreditNonce,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TicketId(u32);

/// An affine queued authority. It is intentionally neither `Clone` nor `Copy`.
#[derive(Debug)]
struct ProducerCredit {
    brand: RegistryBrand,
    key: CreditKey,
}

/// An affine completion authority. Dispatch consumes a [`ProducerCredit`].
#[derive(Debug)]
struct FlightTicket {
    brand: RegistryBrand,
    key: CreditKey,
    ticket: TicketId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ReturnExecutionClass {
    BagExhaustive,
    DistinctExhaustive,
    ExistsCancel,
}

#[derive(Clone, Debug)]
enum Reducer {
    Bag,
    Distinct { seen: BTreeSet<i32> },
    Exists,
}

impl Reducer {
    fn execution_class(&self) -> ReturnExecutionClass {
        match self {
            Self::Bag => ReturnExecutionClass::BagExhaustive,
            Self::Distinct { .. } => ReturnExecutionClass::DistinctExhaustive,
            Self::Exists => ReturnExecutionClass::ExistsCancel,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivationStatus {
    Open,
    Quiescent,
    ResolvedTrue,
    Cancelled,
}

#[derive(Clone, Debug)]
struct Activation {
    plan: PlanId,
    caller: u32,
    imports: Box<[i32]>,
    reducer: Reducer,
    status: ActivationStatus,
    quiescence_proofs: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreditLocation {
    Queued,
    InFlight(TicketId),
    Tombstoned(Option<TicketId>),
}

#[derive(Clone, Copy, Debug)]
struct CreditRecord {
    rank: u16,
    location: CreditLocation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Publication {
    Value { caller: u32, value: i32 },
    Exists { caller: u32, value: bool },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FrameCohortKey {
    plan: PlanId,
    rank: u16,
    return_class: ReturnExecutionClass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QuiescenceProof {
    activation: ActivationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompletionDisposition {
    Applied,
    ResolvedTrue,
    DiscardedLate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplaceEvent {
    ChildIssued { live: usize },
    ParentRetired { live: usize },
}

#[derive(Debug)]
struct CompletionReceipt {
    disposition: CompletionDisposition,
    children: Vec<ProducerCredit>,
    events: Vec<ReplaceEvent>,
    quiescence: Option<QuiescenceProof>,
}

struct CloneHandles {
    queued: Vec<ProducerCredit>,
    in_flight: Vec<FlightTicket>,
    remapped: Vec<((RegistryBrand, CreditKey), (RegistryBrand, CreditKey))>,
}

struct Registry {
    brand: RegistryBrand,
    next_activation: u32,
    next_credit: u32,
    next_ticket: u32,
    activations: BTreeMap<ActivationId, Activation>,
    credits: BTreeMap<CreditKey, CreditRecord>,
    published: Vec<Publication>,
    publication_probe: Arc<AtomicUsize>,
}

impl Registry {
    fn new(publication_probe: Arc<AtomicUsize>) -> Self {
        Self {
            brand: RegistryBrand::fresh(),
            next_activation: 0,
            next_credit: 0,
            next_ticket: 0,
            activations: BTreeMap::new(),
            credits: BTreeMap::new(),
            published: Vec::new(),
            publication_probe,
        }
    }

    fn start(
        &mut self,
        plan: PlanId,
        rank: u16,
        caller: u32,
        imports: impl Into<Box<[i32]>>,
        reducer: Reducer,
    ) -> (ActivationId, ProducerCredit) {
        let activation = ActivationId(self.next_activation);
        self.next_activation += 1;
        self.activations.insert(
            activation,
            Activation {
                plan,
                caller,
                imports: imports.into(),
                reducer,
                status: ActivationStatus::Open,
                quiescence_proofs: 0,
            },
        );
        let credit = self.issue(activation, rank);
        (activation, credit)
    }

    fn issue(&mut self, activation: ActivationId, rank: u16) -> ProducerCredit {
        assert_eq!(
            self.activations[&activation].status,
            ActivationStatus::Open,
            "only an open activation may mint children"
        );
        let key = CreditKey {
            activation,
            nonce: CreditNonce(self.next_credit),
        };
        self.next_credit += 1;
        assert!(
            self.credits
                .insert(
                    key,
                    CreditRecord {
                        rank,
                        location: CreditLocation::Queued,
                    },
                )
                .is_none(),
            "credit nonce reused"
        );
        ProducerCredit {
            brand: self.brand,
            key,
        }
    }

    fn dispatch(&mut self, credit: ProducerCredit) -> FlightTicket {
        assert_eq!(credit.brand, self.brand, "foreign registry brand");
        let record = self.credits.get_mut(&credit.key).expect("unknown credit");
        assert_eq!(record.location, CreditLocation::Queued, "credit not queued");
        let ticket = TicketId(self.next_ticket);
        self.next_ticket += 1;
        record.location = CreditLocation::InFlight(ticket);
        FlightTicket {
            brand: self.brand,
            key: credit.key,
            ticket,
        }
    }

    fn complete(
        &mut self,
        ticket: FlightTicket,
        witnesses: &[i32],
        child_ranks: &[u16],
    ) -> CompletionReceipt {
        assert_eq!(ticket.brand, self.brand, "foreign registry brand");
        let record = *self.credits.get(&ticket.key).expect("unknown ticket");
        match record.location {
            CreditLocation::Tombstoned(Some(expected)) if expected == ticket.ticket => {
                self.credits.remove(&ticket.key);
                return CompletionReceipt {
                    disposition: CompletionDisposition::DiscardedLate,
                    children: Vec::new(),
                    events: Vec::new(),
                    quiescence: None,
                };
            }
            CreditLocation::InFlight(expected) if expected == ticket.ticket => {}
            _ => panic!("ticket does not own this credit"),
        }

        let publications = self.reduce(ticket.key.activation, witnesses);
        self.publish(publications);
        if self.activations[&ticket.key.activation].status == ActivationStatus::ResolvedTrue {
            self.tombstone_activation(ticket.key.activation);
            self.credits.remove(&ticket.key);
            return CompletionReceipt {
                disposition: CompletionDisposition::ResolvedTrue,
                children: Vec::new(),
                events: Vec::new(),
                quiescence: None,
            };
        }

        let mut children = Vec::with_capacity(child_ranks.len());
        let mut events = Vec::with_capacity(child_ranks.len() + 1);
        for &rank in child_ranks {
            assert!(
                rank > record.rank,
                "a child must advance its plan-local rank"
            );
            children.push(self.issue(ticket.key.activation, rank));
            events.push(ReplaceEvent::ChildIssued {
                live: self.live_count(ticket.key.activation),
            });
        }

        self.credits.remove(&ticket.key);
        let live = self.live_count(ticket.key.activation);
        events.push(ReplaceEvent::ParentRetired { live });
        let quiescence = (live == 0).then(|| self.close(ticket.key.activation));

        CompletionReceipt {
            disposition: CompletionDisposition::Applied,
            children,
            events,
            quiescence,
        }
    }

    fn reduce(&mut self, activation: ActivationId, witnesses: &[i32]) -> Vec<Publication> {
        let activation = self
            .activations
            .get_mut(&activation)
            .expect("unknown activation");
        assert_eq!(activation.status, ActivationStatus::Open);
        let caller = activation.caller;
        match &mut activation.reducer {
            Reducer::Bag => witnesses
                .iter()
                .map(|&value| Publication::Value { caller, value })
                .collect(),
            Reducer::Distinct { seen } => witnesses
                .iter()
                .filter_map(|&value| {
                    seen.insert(value)
                        .then_some(Publication::Value { caller, value })
                })
                .collect(),
            Reducer::Exists => {
                if witnesses.is_empty() {
                    Vec::new()
                } else {
                    activation.status = ActivationStatus::ResolvedTrue;
                    vec![Publication::Exists {
                        caller,
                        value: true,
                    }]
                }
            }
        }
    }

    fn publish(&mut self, publications: Vec<Publication>) {
        self.publication_probe
            .fetch_add(publications.len(), Ordering::Relaxed);
        self.published.extend(publications);
    }

    fn close(&mut self, activation: ActivationId) -> QuiescenceProof {
        let activation_state = self
            .activations
            .get_mut(&activation)
            .expect("unknown activation");
        assert_eq!(activation_state.status, ActivationStatus::Open);
        activation_state.status = ActivationStatus::Quiescent;
        activation_state.quiescence_proofs += 1;
        let publication =
            matches!(activation_state.reducer, Reducer::Exists).then_some(Publication::Exists {
                caller: activation_state.caller,
                value: false,
            });
        if let Some(publication) = publication {
            self.publish(vec![publication]);
        }
        QuiescenceProof { activation }
    }

    fn tombstone_activation(&mut self, activation: ActivationId) {
        for (key, record) in &mut self.credits {
            if key.activation == activation {
                record.location = match record.location {
                    CreditLocation::Queued => CreditLocation::Tombstoned(None),
                    CreditLocation::InFlight(ticket) => CreditLocation::Tombstoned(Some(ticket)),
                    tombstone @ CreditLocation::Tombstoned(_) => tombstone,
                };
            }
        }
    }

    fn live_count(&self, activation: ActivationId) -> usize {
        self.credits
            .iter()
            .filter(|(key, record)| {
                key.activation == activation
                    && matches!(
                        record.location,
                        CreditLocation::Queued | CreditLocation::InFlight(_)
                    )
            })
            .count()
    }

    fn may_feed_outer(&self) -> bool {
        let mut unresolved = false;
        for (&id, activation) in &self.activations {
            if activation.status != ActivationStatus::Open {
                continue;
            }
            assert!(
                self.live_count(id) > 0,
                "an open activation without a queued or in-flight credit is corrupt"
            );
            unresolved = true;
        }
        unresolved
    }

    fn queued_cohorts(&self) -> BTreeMap<FrameCohortKey, usize> {
        let mut cohorts = BTreeMap::new();
        for (key, record) in &self.credits {
            if record.location != CreditLocation::Queued {
                continue;
            }
            let activation = &self.activations[&key.activation];
            if activation.status != ActivationStatus::Open {
                continue;
            }
            let cohort = FrameCohortKey {
                plan: activation.plan,
                rank: record.rank,
                return_class: activation.reducer.execution_class(),
            };
            *cohorts.entry(cohort).or_insert(0) += 1;
        }
        cohorts
    }

    fn plan_floors(&self) -> BTreeMap<PlanId, u16> {
        let mut floors: BTreeMap<PlanId, u16> = BTreeMap::new();
        for (key, record) in &self.credits {
            if !matches!(
                record.location,
                CreditLocation::Queued | CreditLocation::InFlight(_)
            ) {
                continue;
            }
            let activation = &self.activations[&key.activation];
            if activation.status != ActivationStatus::Open {
                continue;
            }
            floors
                .entry(activation.plan)
                .and_modify(|floor| *floor = (*floor).min(record.rank))
                .or_insert(record.rank);
        }
        floors
    }

    fn locally_ready_underfilled(&self, width: usize) -> Vec<FrameCohortKey> {
        let width = width.max(1);
        let floors = self.plan_floors();
        self.queued_cohorts()
            .into_iter()
            .filter_map(|(key, occupancy)| {
                (occupancy < width && floors.get(&key.plan) == Some(&key.rank)).then_some(key)
            })
            .collect()
    }

    fn deep_clone(&self) -> (Self, CloneHandles) {
        let mut clone = Self {
            brand: RegistryBrand::fresh(),
            next_activation: self.next_activation,
            next_credit: 0,
            next_ticket: 0,
            activations: self.activations.clone(),
            credits: BTreeMap::new(),
            published: self.published.clone(),
            publication_probe: Arc::clone(&self.publication_probe),
        };
        let mut handles = CloneHandles {
            queued: Vec::new(),
            in_flight: Vec::new(),
            remapped: Vec::new(),
        };

        for (&old_key, record) in &self.credits {
            if !matches!(
                record.location,
                CreditLocation::Queued | CreditLocation::InFlight(_)
            ) {
                continue;
            }
            let new_key = CreditKey {
                activation: old_key.activation,
                nonce: CreditNonce(clone.next_credit),
            };
            clone.next_credit += 1;
            let location = match record.location {
                CreditLocation::Queued => {
                    handles.queued.push(ProducerCredit {
                        brand: clone.brand,
                        key: new_key,
                    });
                    CreditLocation::Queued
                }
                CreditLocation::InFlight(_) => {
                    let ticket = TicketId(clone.next_ticket);
                    clone.next_ticket += 1;
                    handles.in_flight.push(FlightTicket {
                        brand: clone.brand,
                        key: new_key,
                        ticket,
                    });
                    CreditLocation::InFlight(ticket)
                }
                CreditLocation::Tombstoned(_) => unreachable!(),
            };
            clone.credits.insert(
                new_key,
                CreditRecord {
                    rank: record.rank,
                    location,
                },
            );
            handles
                .remapped
                .push(((self.brand, old_key), (clone.brand, new_key)));
        }
        (clone, handles)
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        for activation in self.activations.values_mut() {
            if activation.status == ActivationStatus::Open {
                activation.status = ActivationStatus::Cancelled;
            }
        }
        self.credits.clear();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OuterBucket {
    rank: u16,
    occupancy: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Selection {
    HotFrame(FrameCohortKey),
    HotOuter(u16),
    FullOuter(u16),
    FullFrame(FrameCohortKey),
    UnderfilledFrame(FrameCohortKey),
    AwaitFrame,
    UnderfilledOuter(u16),
    Idle,
}

struct SchedulerModel {
    registry: Registry,
    outer: Vec<OuterBucket>,
    hot: Option<Selection>,
    last_frame_plan: Option<PlanId>,
}

impl SchedulerModel {
    fn select(&mut self, width: usize) -> Selection {
        let width = width.max(1);
        if let Some(hot) = self.hot {
            assert!(matches!(
                hot,
                Selection::HotFrame(_) | Selection::HotOuter(_)
            ));
            return hot;
        }

        if let Some(bucket) = self
            .outer
            .iter()
            .filter(|bucket| bucket.occupancy >= width)
            .max_by_key(|bucket| bucket.rank)
        {
            return Selection::FullOuter(bucket.rank);
        }

        let full_frames: Vec<_> = self
            .registry
            .queued_cohorts()
            .into_iter()
            .filter_map(|(key, occupancy)| (occupancy >= width).then_some(key))
            .collect();
        if let Some(key) = self.choose_frame(full_frames) {
            return Selection::FullFrame(key);
        }

        if self.registry.may_feed_outer() {
            let ready = self.registry.locally_ready_underfilled(width);
            return self
                .choose_frame(ready)
                .map(Selection::UnderfilledFrame)
                .unwrap_or(Selection::AwaitFrame);
        }

        self.outer
            .iter()
            .min_by_key(|bucket| bucket.rank)
            .map(|bucket| Selection::UnderfilledOuter(bucket.rank))
            .unwrap_or(Selection::Idle)
    }

    fn choose_frame(&mut self, keys: Vec<FrameCohortKey>) -> Option<FrameCohortKey> {
        let mut deepest_by_plan = BTreeMap::new();
        for key in keys {
            deepest_by_plan
                .entry(key.plan)
                .and_modify(|current: &mut FrameCohortKey| {
                    if key.rank > current.rank
                        || (key.rank == current.rank && key.return_class < current.return_class)
                    {
                        *current = key;
                    }
                })
                .or_insert(key);
        }
        let selected_plan = match self.last_frame_plan {
            Some(last) => deepest_by_plan
                .range((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded))
                .next()
                .map(|(&plan, _)| plan)
                .or_else(|| deepest_by_plan.keys().next().copied()),
            None => deepest_by_plan.keys().next().copied(),
        }?;
        self.last_frame_plan = Some(selected_plan);
        deepest_by_plan.get(&selected_plan).copied()
    }
}

fn ragged_limits(width: usize, atoms: usize) -> Vec<usize> {
    assert!(atoms > 0 && atoms <= width.max(1));
    let width = width.max(1);
    let base = width / atoms;
    let remainder = width % atoms;
    (0..atoms)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

fn transition_bag(rows: &[i32]) -> Vec<i32> {
    rows.iter()
        .flat_map(|&row| [row * 10, row * 10 + (row & 1)])
        .collect()
}

fn bag_counts(values: impl IntoIterator<Item = i32>) -> BTreeMap<i32, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_insert(0) += 1;
    }
    counts
}

fn specialized_return(class: ReturnExecutionClass, witnesses: &[i32]) -> Vec<i32> {
    match class {
        ReturnExecutionClass::ExistsCancel => witnesses.first().copied().into_iter().collect(),
        ReturnExecutionClass::BagExhaustive => witnesses.to_vec(),
        ReturnExecutionClass::DistinctExhaustive => witnesses
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    }
}

#[test]
fn inflight_predecessor_is_not_quiescence() {
    let probe = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new(probe);
    let (_, credit) = registry.start(PlanId(1), 7, 0, vec![5], Reducer::Exists);
    let ticket = registry.dispatch(credit);

    assert!(registry.may_feed_outer());
    assert!(registry.queued_cohorts().is_empty());
    assert!(
        registry.published.is_empty(),
        "in flight is not false Exists"
    );
    assert_eq!(registry.plan_floors(), BTreeMap::from([(PlanId(1), 7)]));

    let mut scheduler = SchedulerModel {
        registry,
        outer: vec![OuterBucket {
            rank: 3,
            occupancy: 1,
        }],
        hot: None,
        last_frame_plan: None,
    };
    assert_eq!(scheduler.select(4), Selection::AwaitFrame);

    let receipt = scheduler.registry.complete(ticket, &[], &[]);
    assert!(receipt.quiescence.is_some());
    assert_eq!(
        scheduler.registry.published,
        vec![Publication::Exists {
            caller: 0,
            value: false
        }]
    );
    assert_eq!(scheduler.select(4), Selection::UnderfilledOuter(3));
}

#[test]
fn replace_one_with_two_has_no_transient_close() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    let (activation, root) = registry.start(PlanId(1), 0, 0, [], Reducer::Bag);
    let root_ticket = registry.dispatch(root);
    let fork = registry.complete(root_ticket, &[], &[1, 1]);

    assert_eq!(
        fork.events,
        vec![
            ReplaceEvent::ChildIssued { live: 2 },
            ReplaceEvent::ChildIssued { live: 3 },
            ReplaceEvent::ParentRetired { live: 2 },
        ]
    );
    assert_eq!(fork.quiescence, None);

    let mut children = fork.children.into_iter();
    let first = registry.dispatch(children.next().unwrap());
    let second = registry.dispatch(children.next().unwrap());
    assert_eq!(registry.complete(first, &[], &[]).quiescence, None);
    let close = registry.complete(second, &[], &[]).quiescence;
    assert_eq!(close, Some(QuiescenceProof { activation }));
    assert_eq!(registry.activations[&activation].quiescence_proofs, 1);
}

#[test]
fn exists_cancel_is_activation_local_and_late_tickets_are_inert() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    let (first, root) = registry.start(PlanId(1), 0, 9, vec![42], Reducer::Exists);
    let root_ticket = registry.dispatch(root);
    let fork = registry.complete(root_ticket, &[], &[1, 1]);
    let mut children = fork.children.into_iter();
    let winner = registry.dispatch(children.next().unwrap());
    let late = registry.dispatch(children.next().unwrap());
    let (sibling, _sibling_credit) = registry.start(PlanId(1), 0, 9, vec![42], Reducer::Exists);

    let resolved = registry.complete(winner, &[10], &[2]);
    assert_eq!(resolved.disposition, CompletionDisposition::ResolvedTrue);
    assert_eq!(
        registry.activations[&first].status,
        ActivationStatus::ResolvedTrue
    );
    assert_eq!(
        registry.activations[&sibling].status,
        ActivationStatus::Open
    );
    assert!(registry.may_feed_outer(), "the sibling still owns a feeder");

    let before = registry.published.clone();
    let discarded = registry.complete(late, &[20], &[2]);
    assert_eq!(discarded.disposition, CompletionDisposition::DiscardedLate);
    assert_eq!(registry.published, before);
    assert_eq!(
        registry
            .published
            .iter()
            .filter(|publication| matches!(publication, Publication::Exists { value: true, .. }))
            .count(),
        1
    );
}

#[test]
fn duplicate_imports_keep_distinct_activation_bags() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    let (left, left_credit) = registry.start(
        PlanId(4),
        0,
        7,
        vec![11],
        Reducer::Distinct {
            seen: BTreeSet::new(),
        },
    );
    let (right, right_credit) = registry.start(
        PlanId(4),
        0,
        7,
        vec![11],
        Reducer::Distinct {
            seen: BTreeSet::new(),
        },
    );
    let (_, bag_credit) = registry.start(PlanId(4), 0, 8, vec![11], Reducer::Bag);

    assert_eq!(
        registry.activations[&left].imports,
        registry.activations[&right].imports
    );
    let left_ticket = registry.dispatch(left_credit);
    registry.complete(left_ticket, &[20, 20], &[]);
    let right_ticket = registry.dispatch(right_credit);
    registry.complete(right_ticket, &[20, 20], &[]);
    let bag_ticket = registry.dispatch(bag_credit);
    registry.complete(bag_ticket, &[10, 20], &[]);

    assert_eq!(
        registry
            .published
            .iter()
            .filter(|publication| {
                **publication
                    == Publication::Value {
                        caller: 7,
                        value: 20,
                    }
            })
            .count(),
        2,
        "Distinct collapses below each activation, never across activations"
    );
    assert!(registry.published.contains(&Publication::Value {
        caller: 8,
        value: 10
    }));
    assert!(registry.published.contains(&Publication::Value {
        caller: 8,
        value: 20
    }));
}

#[test]
fn plan_local_floors_do_not_compare_numeric_ranks_across_plans() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    let _ = registry.start(PlanId(1), 900, 0, [], Reducer::Bag);
    let _ = registry.start(PlanId(1), 901, 0, [], Reducer::Bag);
    let _ = registry.start(PlanId(2), 1, 0, [], Reducer::Bag);

    let ready = registry.locally_ready_underfilled(8);
    assert!(ready
        .iter()
        .any(|key| key.plan == PlanId(1) && key.rank == 900));
    assert!(ready
        .iter()
        .any(|key| key.plan == PlanId(2) && key.rank == 1));
    assert!(!ready
        .iter()
        .any(|key| key.plan == PlanId(1) && key.rank == 901));

    let mut scheduler = SchedulerModel {
        registry,
        outer: Vec::new(),
        hot: None,
        last_frame_plan: None,
    };
    assert!(matches!(
        scheduler.select(8),
        Selection::UnderfilledFrame(FrameCohortKey {
            plan: PlanId(1),
            rank: 900,
            ..
        })
    ));
    assert!(matches!(
        scheduler.select(8),
        Selection::UnderfilledFrame(FrameCohortKey {
            plan: PlanId(2),
            rank: 1,
            ..
        })
    ));
}

#[test]
fn cold_frames_gate_only_underfilled_outer_work() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    let _ = registry.start(PlanId(1), 0, 0, [], Reducer::Bag);
    let mut scheduler = SchedulerModel {
        registry,
        outer: vec![OuterBucket {
            rank: 10,
            occupancy: 4,
        }],
        hot: None,
        last_frame_plan: None,
    };

    assert_eq!(scheduler.select(4), Selection::FullOuter(10));
    scheduler.outer[0].occupancy = 3;
    assert!(matches!(
        scheduler.select(4),
        Selection::UnderfilledFrame(_)
    ));
}

#[test]
fn hot_full_frame_and_underfilled_outer_follow_declared_order() {
    let mut registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    for _ in 0..4 {
        let _ = registry.start(PlanId(1), 5, 0, [], Reducer::Bag);
    }
    let frame = FrameCohortKey {
        plan: PlanId(1),
        rank: 5,
        return_class: ReturnExecutionClass::BagExhaustive,
    };
    let mut scheduler = SchedulerModel {
        registry,
        outer: vec![OuterBucket {
            rank: 9,
            occupancy: 4,
        }],
        hot: Some(Selection::HotFrame(frame)),
        last_frame_plan: None,
    };

    assert_eq!(scheduler.select(4), Selection::HotFrame(frame));
    scheduler.hot = Some(Selection::HotOuter(9));
    assert_eq!(scheduler.select(4), Selection::HotOuter(9));
    scheduler.hot = None;
    assert_eq!(scheduler.select(4), Selection::FullOuter(9));
    scheduler.outer[0].occupancy = 3;
    assert_eq!(scheduler.select(4), Selection::FullFrame(frame));

    scheduler.registry = Registry::new(Arc::new(AtomicUsize::new(0)));
    assert_eq!(scheduler.select(4), Selection::UnderfilledOuter(9));
}

#[test]
fn late_reentry_is_a_bag_homomorphism() {
    let first = [1, 2, 2];
    let late = [2, 3];

    let mut reopened = transition_bag(&first);
    reopened.extend(transition_bag(&late));
    let combined = transition_bag(&first.into_iter().chain(late).collect::<Vec<_>>());

    assert_eq!(bag_counts(reopened), bag_counts(combined));
}

#[test]
fn mixed_return_specialization_requires_separate_physical_cohorts() {
    let witnesses = [10, 20];
    let leader_policy = ReturnExecutionClass::ExistsCancel;
    let incorrectly_fused_bag = specialized_return(leader_policy, &witnesses);
    let correctly_grouped_exists =
        specialized_return(ReturnExecutionClass::ExistsCancel, &witnesses);
    let correctly_grouped_bag = specialized_return(ReturnExecutionClass::BagExhaustive, &witnesses);

    assert_eq!(incorrectly_fused_bag, vec![10]);
    assert_eq!(correctly_grouped_exists, vec![10]);
    assert_eq!(correctly_grouped_bag, vec![10, 20]);
}

#[test]
fn ragged_limits_share_one_global_width_budget() {
    let limits = ragged_limits(11, 4);
    assert_eq!(limits, vec![3, 3, 3, 2]);
    assert_eq!(limits.iter().sum::<usize>(), 11);
    assert!(limits.iter().all(|&limit| limit > 0));
    assert_ne!(limits, vec![11; 4]);
}

#[test]
fn deep_clone_rebrands_live_credits_and_drop_publishes_nothing() {
    let probe = Arc::new(AtomicUsize::new(0));
    let mut original = Registry::new(Arc::clone(&probe));
    let (activation, credit) = original.start(PlanId(1), 0, 4, [], Reducer::Bag);
    let original_ticket = original.dispatch(credit);
    let (mut clone, mut handles) = original.deep_clone();

    assert_ne!(original.brand, clone.brand);
    assert_eq!(handles.in_flight.len(), 1);
    assert!(handles.queued.is_empty());
    assert!(handles.remapped.iter().all(|(old, new)| old != new));

    let original_close = original.complete(original_ticket, &[42], &[]);
    let clone_close = clone.complete(handles.in_flight.pop().unwrap(), &[42], &[]);
    assert_eq!(
        original_close.quiescence,
        Some(QuiescenceProof { activation })
    );
    assert_eq!(clone_close.quiescence, Some(QuiescenceProof { activation }));
    assert_eq!(original.published, clone.published);

    let before_drop = probe.load(Ordering::Relaxed);
    drop(clone);
    assert_eq!(probe.load(Ordering::Relaxed), before_drop);

    let mut unresolved = Registry::new(Arc::clone(&probe));
    let _ = unresolved.start(PlanId(2), 0, 5, [], Reducer::Exists);
    drop(unresolved);
    assert_eq!(
        probe.load(Ordering::Relaxed),
        before_drop,
        "drop must not turn an unresolved Exists into a false publication"
    );
}
