//! Executable Stage-0 model of a lazy-domain monotone evidence join.
//!
//! This is deliberately a `cfg(test)` model, not a production scheduler route.
//! One parent contributes an immutable raw occurrence bag `B`. A budgeted
//! cursor reveals that bag lazily: `S` is the distinct seen set, `M` and `N`
//! are sound positive and negative predicate evidence, `P` is the set of seen
//! candidates whose local membership work is unresolved, and an implicit tail
//! `T` remains possible until the cursor has actually observed EOF.
//!
//! Candidate-local and mixed evidence can close the join only after `T` is
//! gone and `P` is empty. A quiescent shared executor may instead publish its
//! complete predicate `G` and close before EOF. `G` is intentionally not
//! restricted to `SET(B)`: the result is `B ∩ G`, so values outside this
//! parent's candidate domain are harmless.
//!
//! [`EvidenceAddress`] is `Copy` because it is only a generation-stamped
//! routing capability for positive observations. Negative and exact evidence
//! require the affine [`QuiescentAuthority`] produced at the trusted
//! child-quiescence boundary. Candidate decisions fence only that member;
//! whole closure advances one join generation without walking outstanding
//! routes. These are logical fences. The model deliberately retains routing
//! records and makes no claim that already-dispatched physical work was
//! cancelled or removed from an activation arena.

use std::collections::{BTreeMap, BTreeSet};

use crate::inline::RawInline;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Arm(u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EvidenceRoute {
    Shared { nonce: u64, arm: Arm },
    Member { value: RawInline },
}

/// A copyable destination for streaming positive evidence.
///
/// Copyability does not confer exclusion or completeness authority. Those
/// claims require consuming an [`EvidenceWork`] through [`child_quiesced`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EvidenceAddress {
    generation: u64,
    route: EvidenceRoute,
}

/// One physically issued unit of work.
///
/// The model never clones this token. Its address may be copied for streaming,
/// while the token itself can cross the trusted quiescence boundary once.
#[derive(Debug)]
struct EvidenceWork {
    address: EvidenceAddress,
}

impl EvidenceWork {
    fn address(&self) -> EvidenceAddress {
        self.address
    }
}

/// Authority to make one quiescence-dependent claim.
#[derive(Debug)]
struct QuiescentAuthority(EvidenceWork);

#[derive(Debug)]
enum QuiescentEvidence {
    /// A sound proof that one value is outside the shared predicate.
    Negative(RawInline),
    /// One shared executor's complete predicate, not merely its projection on
    /// this parent's candidate domain.
    Exact(Vec<RawInline>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealReason {
    Candidate,
    Mixed,
    SharedExact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Progress {
    Open,
    MemberFenced,
    Settled(SealReason),
    Stale,
}

#[derive(Debug)]
struct CursorAdvance {
    members: Vec<EvidenceWork>,
    progress: Progress,
}

/// Lazy-domain evidence state for one frozen parent occurrence bag.
///
/// The fields named in the Stage-0 algebra are:
///
/// - `bag`: immutable raw occurrence bag `B`;
/// - `seen`: distinct cursor prefix `S`;
/// - `positive`: sound membership evidence `M`;
/// - `negative`: sound exclusion evidence `N`;
/// - `pending`: unresolved seen candidates `P`;
/// - `!eof`: the implicit, unmaterialized tail `T`.
#[derive(Debug)]
struct EvidenceJoin {
    bag: Vec<RawInline>,
    cursor: usize,
    eof: bool,
    seen: BTreeSet<RawInline>,
    positive: BTreeSet<RawInline>,
    negative: BTreeSet<RawInline>,
    pending: BTreeSet<RawInline>,
    shared_contributed: bool,
    generation: u64,
    next_nonce: u64,
    live_shared: BTreeMap<u64, Arm>,
    predicate: Option<BTreeSet<RawInline>>,
    seal_reason: Option<SealReason>,
}

impl EvidenceJoin {
    fn new(bag: impl IntoIterator<Item = RawInline>) -> Self {
        Self {
            bag: bag.into_iter().collect(),
            cursor: 0,
            eof: false,
            seen: BTreeSet::new(),
            positive: BTreeSet::new(),
            negative: BTreeSet::new(),
            pending: BTreeSet::new(),
            shared_contributed: false,
            generation: 0,
            next_nonce: 0,
            live_shared: BTreeMap::new(),
            predicate: None,
            seal_reason: None,
        }
    }

    fn issue_shared(&mut self, arm: Arm) -> EvidenceWork {
        assert!(!self.settled(), "settled evidence join issued new work");
        let nonce = self.next_nonce;
        self.next_nonce = self
            .next_nonce
            .checked_add(1)
            .expect("evidence nonce overflow");
        assert!(
            self.live_shared.insert(nonce, arm).is_none(),
            "evidence nonce was reused"
        );
        EvidenceWork {
            address: EvidenceAddress {
                generation: self.generation,
                route: EvidenceRoute::Shared { nonce, arm },
            },
        }
    }

    /// Spend at most `budget` cursor observations.
    ///
    /// Reading the final value does not imply EOF. The implicit tail disappears
    /// only when a later observation returns `None`, just as it would for an
    /// opaque external cursor.
    fn advance_cursor(&mut self, budget: usize) -> CursorAdvance {
        if self.settled() {
            return CursorAdvance {
                members: Vec::new(),
                progress: Progress::Stale,
            };
        }

        let mut members = Vec::new();
        for _ in 0..budget {
            if self.eof {
                break;
            }
            let Some(&value) = self.bag.get(self.cursor) else {
                self.eof = true;
                break;
            };
            self.cursor += 1;

            if self.seen.insert(value)
                && !self.positive.contains(&value)
                && !self.negative.contains(&value)
            {
                assert!(
                    self.pending.insert(value),
                    "newly seen candidate was already pending"
                );
                members.push(EvidenceWork {
                    address: EvidenceAddress {
                        generation: self.generation,
                        route: EvidenceRoute::Member { value },
                    },
                });
            }
        }

        let progress = self
            .try_lazy_seal()
            .map_or(Progress::Open, Progress::Settled);
        self.assert_invariants();
        CursorAdvance { members, progress }
    }

    /// Publish sound positive evidence through a copyable routing address.
    ///
    /// A member address resolves and fences only its own candidate. A shared
    /// address may publish before that candidate is discovered; later cursor
    /// discovery then observes the already-known decision and issues no local
    /// work.
    fn publish_positive(&mut self, address: EvidenceAddress, value: RawInline) -> Progress {
        if !self.address_is_current(address) {
            return Progress::Stale;
        }

        let member_fenced = match address.route {
            EvidenceRoute::Member { value: candidate } => {
                assert_eq!(
                    value, candidate,
                    "member evidence was routed to a different candidate"
                );
                assert!(
                    self.pending.contains(&candidate),
                    "current member route had no pending candidate"
                );
                self.record_positive(value)
            }
            EvidenceRoute::Shared { .. } => {
                self.shared_contributed = true;
                self.record_positive(value)
            }
        };

        let progress = self.try_lazy_seal().map_or_else(
            || {
                if member_fenced {
                    Progress::MemberFenced
                } else {
                    Progress::Open
                }
            },
            Progress::Settled,
        );
        self.assert_invariants();
        progress
    }

    /// Apply evidence whose soundness depends on child quiescence.
    fn complete(&mut self, authority: QuiescentAuthority, evidence: QuiescentEvidence) -> Progress {
        let address = authority.0.address;
        if !self.address_is_current(address) {
            return Progress::Stale;
        }

        match evidence {
            QuiescentEvidence::Negative(value) => {
                let member_fenced = match address.route {
                    EvidenceRoute::Member { value: candidate } => {
                        assert_eq!(
                            value, candidate,
                            "member exclusion was routed to a different candidate"
                        );
                        assert!(
                            self.pending.contains(&candidate),
                            "current member route had no pending candidate"
                        );
                        self.record_negative(value)
                    }
                    EvidenceRoute::Shared { nonce, arm } => {
                        self.assert_shared_route(nonce, arm);
                        self.assert_negative_consistent(value);
                        self.live_shared.remove(&nonce);
                        self.shared_contributed = true;
                        self.negative.insert(value);
                        self.pending.remove(&value)
                    }
                };
                let progress = self.try_lazy_seal().map_or_else(
                    || {
                        if member_fenced {
                            Progress::MemberFenced
                        } else {
                            Progress::Open
                        }
                    },
                    Progress::Settled,
                );
                self.assert_invariants();
                progress
            }
            QuiescentEvidence::Exact(values) => {
                let EvidenceRoute::Shared { nonce, arm } = address.route else {
                    panic!("candidate-local work cannot certify a shared exact predicate");
                };
                self.assert_shared_route(nonce, arm);
                let exact: BTreeSet<_> = values.into_iter().collect();
                assert!(
                    self.positive.is_subset(&exact),
                    "exact predicate omitted earlier sound positive evidence"
                );
                assert!(
                    self.negative.is_disjoint(&exact),
                    "exact predicate included earlier sound negative evidence"
                );
                self.live_shared.remove(&nonce);
                self.whole_seal(SealReason::SharedExact, exact);
                self.assert_invariants();
                Progress::Settled(SealReason::SharedExact)
            }
        }
    }

    fn record_positive(&mut self, value: RawInline) -> bool {
        assert!(
            !self.negative.contains(&value),
            "positive evidence contradicted an earlier negative proof"
        );
        self.positive.insert(value);
        self.pending.remove(&value)
    }

    fn assert_negative_consistent(&self, value: RawInline) {
        assert!(
            !self.positive.contains(&value),
            "negative evidence contradicted an earlier positive proof"
        );
    }

    fn record_negative(&mut self, value: RawInline) -> bool {
        self.assert_negative_consistent(value);
        self.negative.insert(value);
        self.pending.remove(&value)
    }

    fn try_lazy_seal(&mut self) -> Option<SealReason> {
        if !self.eof || !self.pending.is_empty() {
            return None;
        }
        let reason = if self.shared_contributed {
            SealReason::Mixed
        } else {
            SealReason::Candidate
        };
        self.whole_seal(reason, self.positive.clone());
        Some(reason)
    }

    /// Fence every outstanding route in O(1) with respect to work count.
    ///
    /// Building the final predicate is separate from this operation. In
    /// particular, this method neither scans nor clears `pending` or
    /// `live_shared`; old physical work may still arrive, but its generation
    /// makes it logically stale.
    fn whole_seal(&mut self, reason: SealReason, predicate: BTreeSet<RawInline>) {
        assert!(!self.settled(), "evidence join sealed twice");
        self.predicate = Some(predicate);
        self.seal_reason = Some(reason);
        self.generation = self
            .generation
            .checked_add(1)
            .expect("evidence generation overflow");
    }

    fn address_is_current(&self, address: EvidenceAddress) -> bool {
        if self.settled() || address.generation != self.generation {
            return false;
        }
        match address.route {
            EvidenceRoute::Shared { nonce, arm } => {
                self.live_shared.get(&nonce).copied() == Some(arm)
            }
            EvidenceRoute::Member { value } => self.pending.contains(&value),
        }
    }

    fn assert_shared_route(&self, nonce: u64, arm: Arm) {
        assert_eq!(
            self.live_shared.get(&nonce).copied(),
            Some(arm),
            "unknown, replayed, or cross-arm shared evidence route"
        );
    }

    fn settled(&self) -> bool {
        self.predicate.is_some()
    }

    fn tail_open(&self) -> bool {
        !self.eof
    }

    /// Interval over the prefix known to the candidate cursor.
    fn known_interval(&self) -> (BTreeSet<RawInline>, BTreeSet<RawInline>) {
        let must = self.seen.intersection(&self.positive).copied().collect();
        let may = self.seen.difference(&self.negative).copied().collect();
        (must, may)
    }

    fn relation(&self) -> Vec<RawInline> {
        let predicate = self
            .predicate
            .as_ref()
            .expect("read an unsettled evidence relation");
        self.bag
            .iter()
            .copied()
            .filter(|value| predicate.contains(value))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn result_bag(&self) -> Vec<RawInline> {
        let predicate = self
            .predicate
            .as_ref()
            .expect("published an unsettled evidence relation");
        self.bag
            .iter()
            .copied()
            .filter(|value| predicate.contains(value))
            .collect()
    }

    fn assert_invariants(&self) {
        assert!(self.cursor <= self.bag.len(), "cursor escaped B");
        let exposed: BTreeSet<_> = self.bag[..self.cursor].iter().copied().collect();
        assert_eq!(self.seen, exposed, "S was not exactly the exposed prefix");
        assert!(
            self.positive.is_disjoint(&self.negative),
            "M and N overlapped"
        );
        assert!(self.pending.is_subset(&self.seen), "P escaped S");
        assert!(
            self.pending.is_disjoint(&self.positive),
            "positive candidate remained pending"
        );
        assert!(
            self.pending.is_disjoint(&self.negative),
            "negative candidate remained pending"
        );
        if self.eof {
            let domain: BTreeSet<_> = self.bag.iter().copied().collect();
            assert_eq!(self.cursor, self.bag.len(), "EOF preceded the end of B");
            assert_eq!(self.seen, domain, "EOF did not expose exactly SET(B)");
        }
        if let Some(predicate) = &self.predicate {
            let relation: BTreeSet<_> = self.relation().into_iter().collect();
            let domain: BTreeSet<_> = self.bag.iter().copied().collect();
            assert_eq!(
                relation,
                domain.intersection(predicate).copied().collect(),
                "published relation was not SET(B) ∩ predicate"
            );
            match self.seal_reason {
                Some(SealReason::Candidate) => {
                    assert!(self.eof, "candidate evidence closed before EOF");
                    assert!(self.pending.is_empty(), "candidate closure retained P");
                    assert!(
                        !self.shared_contributed,
                        "candidate closure contained shared evidence"
                    );
                    assert_eq!(
                        predicate, &self.positive,
                        "candidate predicate differed from M"
                    );
                }
                Some(SealReason::Mixed) => {
                    assert!(self.eof, "mixed evidence closed before EOF");
                    assert!(self.pending.is_empty(), "mixed closure retained P");
                    assert!(
                        self.shared_contributed,
                        "mixed closure contained no shared evidence"
                    );
                    assert_eq!(predicate, &self.positive, "mixed predicate differed from M");
                }
                Some(SealReason::SharedExact) => {}
                None => panic!("settled evidence join had no seal reason"),
            }
        } else {
            assert!(
                self.seal_reason.is_none(),
                "unsettled evidence join had a seal reason"
            );
        }
    }
}

/// Trusted adapter standing in for the scheduler's child-quiescence callback.
///
/// There is intentionally no conversion from [`EvidenceAddress`].
fn child_quiesced(work: EvidenceWork) -> QuiescentAuthority {
    QuiescentAuthority(work)
}

fn raw(byte: u8) -> RawInline {
    let mut value = RawInline::default();
    value[0] = byte;
    value
}

fn member_value(work: &EvidenceWork) -> RawInline {
    let EvidenceRoute::Member { value } = work.address.route else {
        panic!("expected candidate-local work")
    };
    value
}

fn only_member(advance: CursorAdvance) -> EvidenceWork {
    assert_eq!(advance.progress, Progress::Open);
    let mut members = advance.members;
    assert_eq!(members.len(), 1);
    members.pop().unwrap()
}

fn bag_counts(values: impl IntoIterator<Item = RawInline>) -> BTreeMap<RawInline, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
}

#[test]
fn open_cursor_cannot_settle_when_currently_known_must_equals_may() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);

    let first = only_member(join.advance_cursor(1));
    assert_eq!(
        join.publish_positive(first.address(), raw(1)),
        Progress::MemberFenced
    );
    let (must, may) = join.known_interval();
    assert_eq!(must, may, "the currently known interval is exact");
    assert!(join.tail_open(), "T still represents an unseen suffix");
    assert!(!join.settled(), "M = U over S cannot close an open cursor");

    let second = only_member(join.advance_cursor(1));
    assert_eq!(
        join.publish_positive(second.address(), raw(2)),
        Progress::MemberFenced
    );
    let (must, may) = join.known_interval();
    assert_eq!(must, may);
    assert!(
        !join.settled(),
        "even consuming the last value does not prove EOF"
    );

    let eof = join.advance_cursor(1);
    assert!(eof.members.is_empty());
    assert_eq!(eof.progress, Progress::Settled(SealReason::Candidate));
    assert_eq!(join.result_bag(), vec![raw(1), raw(2)]);
}

#[test]
fn late_unseen_candidate_gets_its_own_member_work() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let first = only_member(join.advance_cursor(1));
    assert_eq!(
        join.complete(child_quiesced(first), QuiescentEvidence::Negative(raw(1))),
        Progress::MemberFenced
    );
    assert!(join.tail_open());
    assert!(!join.settled());

    let second = only_member(join.advance_cursor(1));
    assert_eq!(member_value(&second), raw(2));
    assert!(join.pending.contains(&raw(2)));
    assert_eq!(
        join.publish_positive(second.address(), raw(2)),
        Progress::MemberFenced
    );
    assert_eq!(
        join.advance_cursor(1).progress,
        Progress::Settled(SealReason::Candidate)
    );
    assert_eq!(join.relation(), vec![raw(2)]);
}

#[test]
fn shared_positive_before_discovery_suppresses_redundant_member_work() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let shared = join.issue_shared(Arm(0));
    let shared_address = shared.address();

    assert_eq!(
        join.publish_positive(shared_address, raw(2)),
        Progress::Open
    );
    assert!(join.seen.is_empty());
    assert!(join.positive.contains(&raw(2)));

    let first = only_member(join.advance_cursor(1));
    assert_eq!(member_value(&first), raw(1));
    let second = join.advance_cursor(1);
    assert!(second.members.is_empty());
    assert!(
        !join.pending.contains(&raw(2)),
        "pre-discovery positive already decided the member"
    );

    assert_eq!(
        join.complete(child_quiesced(first), QuiescentEvidence::Negative(raw(1))),
        Progress::MemberFenced
    );
    assert_eq!(
        join.advance_cursor(1).progress,
        Progress::Settled(SealReason::Mixed)
    );
    assert_eq!(join.result_bag(), vec![raw(2)]);
    assert!(!join.address_is_current(shared_address));
}

#[test]
fn quiescent_shared_negative_before_discovery_suppresses_member_work() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let shared = join.issue_shared(Arm(0));
    let shared_address = shared.address();

    assert_eq!(
        join.complete(child_quiesced(shared), QuiescentEvidence::Negative(raw(2))),
        Progress::Open
    );
    assert!(!join.address_is_current(shared_address));
    assert!(join.negative.contains(&raw(2)));

    let advance = join.advance_cursor(3);
    assert!(!join.tail_open());
    assert_eq!(
        advance.members.len(),
        1,
        "the prior exclusion already decided candidate 2"
    );
    let first = advance.members.into_iter().next().unwrap();
    assert_eq!(member_value(&first), raw(1));
    assert_eq!(
        join.publish_positive(first.address(), raw(1)),
        Progress::Settled(SealReason::Mixed)
    );
    assert_eq!(join.result_bag(), vec![raw(1)]);
}

#[test]
fn mixed_closure_waits_for_eof_and_every_pending_member() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let shared = join.issue_shared(Arm(0));
    let shared_address = shared.address();
    let mut members = join.advance_cursor(3).members;
    assert!(!join.tail_open());
    assert_eq!(members.len(), 2);

    let second = members.pop().unwrap();
    let first = members.pop().unwrap();
    assert_eq!(
        join.publish_positive(shared_address, raw(1)),
        Progress::MemberFenced
    );
    assert!(!join.settled(), "candidate 2 is still in P");
    assert_eq!(
        join.publish_positive(first.address(), raw(1)),
        Progress::Stale,
        "shared evidence fenced candidate 1 logically"
    );
    assert_eq!(
        join.complete(child_quiesced(second), QuiescentEvidence::Negative(raw(2))),
        Progress::Settled(SealReason::Mixed)
    );
    assert_eq!(join.relation(), vec![raw(1)]);
}

#[test]
fn empty_bag_closes_only_after_cursor_observes_eof() {
    let mut join = EvidenceJoin::new([]);
    assert!(join.tail_open());
    assert!(!join.settled());
    assert_eq!(
        join.advance_cursor(1).progress,
        Progress::Settled(SealReason::Candidate)
    );
    assert!(join.relation().is_empty());
    assert!(join.result_bag().is_empty());
}

#[test]
fn duplicates_and_permutations_preserve_parent_multiset_and_projected_set() {
    let parent_bags = [
        vec![raw(1), raw(1), raw(2), raw(3)],
        vec![raw(3), raw(1), raw(2), raw(1)],
        vec![raw(1), raw(3), raw(1), raw(2)],
    ];
    let expected_counts = bag_counts([raw(1), raw(1), raw(3)]);
    let mut ordered_results = Vec::new();

    for parent_bag in parent_bags {
        let expected_order: Vec<_> = parent_bag
            .iter()
            .copied()
            .filter(|value| *value == raw(1) || *value == raw(3))
            .collect();
        let mut join = EvidenceJoin::new(parent_bag);
        let exact = join.issue_shared(Arm(0));
        assert_eq!(
            join.complete(
                child_quiesced(exact),
                QuiescentEvidence::Exact(vec![raw(1), raw(3)])
            ),
            Progress::Settled(SealReason::SharedExact)
        );

        assert_eq!(join.result_bag(), expected_order);
        assert_eq!(bag_counts(join.result_bag()), expected_counts);
        assert_eq!(join.relation(), vec![raw(1), raw(3)]);
        ordered_results.push(join.result_bag());
    }

    assert_ne!(
        ordered_results[0], ordered_results[1],
        "parent order is deliberately preserved; no cross-order digest gate applies"
    );
}

#[test]
fn shared_exact_predicate_may_extend_outside_candidate_domain() {
    let mut join = EvidenceJoin::new([raw(1), raw(2), raw(2)]);
    let exact = join.issue_shared(Arm(0));
    assert_eq!(
        join.complete(
            child_quiesced(exact),
            QuiescentEvidence::Exact(vec![raw(2), raw(9)])
        ),
        Progress::Settled(SealReason::SharedExact)
    );
    assert!(join.tail_open(), "shared exact is allowed to close pre-EOF");
    assert_eq!(join.relation(), vec![raw(2)]);
    assert_eq!(join.result_bag(), vec![raw(2), raw(2)]);
}

#[test]
fn contradictory_evidence_is_rejected() {
    let positive_then_negative = std::panic::catch_unwind(|| {
        let mut join = EvidenceJoin::new([raw(1)]);
        let positive = join.issue_shared(Arm(0));
        let negative = join.issue_shared(Arm(1));
        assert_eq!(
            join.publish_positive(positive.address(), raw(1)),
            Progress::Open
        );
        join.complete(
            child_quiesced(negative),
            QuiescentEvidence::Negative(raw(1)),
        );
    });
    assert!(positive_then_negative.is_err());

    let negative_then_exact = std::panic::catch_unwind(|| {
        let mut join = EvidenceJoin::new([raw(1)]);
        let negative = join.issue_shared(Arm(0));
        let exact = join.issue_shared(Arm(1));
        assert_eq!(
            join.complete(
                child_quiesced(negative),
                QuiescentEvidence::Negative(raw(1))
            ),
            Progress::Open
        );
        join.complete(
            child_quiesced(exact),
            QuiescentEvidence::Exact(vec![raw(1)]),
        );
    });
    assert!(negative_then_exact.is_err());
}

#[test]
fn whole_seal_generation_makes_late_events_inert_without_cancellation_claim() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let mut members = join.advance_cursor(2).members;
    let member = members.pop().unwrap();
    let winner = join.issue_shared(Arm(0));
    let loser = join.issue_shared(Arm(1));
    let loser_address = loser.address();
    let generation = join.generation;
    let retained_pending = join.pending.len();
    let retained_shared_before = join.live_shared.len();

    assert_eq!(
        join.complete(
            child_quiesced(winner),
            QuiescentEvidence::Exact(vec![raw(2)])
        ),
        Progress::Settled(SealReason::SharedExact)
    );
    assert_eq!(join.generation, generation + 1);
    assert_eq!(
        join.pending.len(),
        retained_pending,
        "whole seal did not walk candidate routing records"
    );
    assert_eq!(
        join.live_shared.len(),
        retained_shared_before - 1,
        "only the quiescent winning source was retired"
    );

    let before = (join.result_bag(), join.relation(), join.seal_reason);
    assert_eq!(
        join.publish_positive(loser_address, raw(1)),
        Progress::Stale
    );
    assert_eq!(
        join.complete(child_quiesced(loser), QuiescentEvidence::Negative(raw(2))),
        Progress::Stale
    );
    assert_eq!(
        join.complete(child_quiesced(member), QuiescentEvidence::Negative(raw(1))),
        Progress::Stale
    );
    assert_eq!(join.advance_cursor(8).progress, Progress::Stale);
    assert_eq!(
        (join.result_bag(), join.relation(), join.seal_reason),
        before
    );
}

#[test]
fn one_candidate_fence_leaves_siblings_and_shared_routes_live() {
    let mut join = EvidenceJoin::new([raw(1), raw(2)]);
    let shared = join.issue_shared(Arm(7));
    let shared_address = shared.address();
    let mut members = join.advance_cursor(2).members;
    let second = members.pop().unwrap();
    let first = members.pop().unwrap();
    let first_address = first.address();
    let second_address = second.address();
    let generation = join.generation;

    assert_eq!(
        join.publish_positive(first_address, raw(1)),
        Progress::MemberFenced
    );
    assert_eq!(
        join.generation, generation,
        "member fence must not advance the whole-join generation"
    );
    assert!(!join.address_is_current(first_address));
    assert!(join.address_is_current(second_address));
    assert!(join.address_is_current(shared_address));
    assert_eq!(join.pending, BTreeSet::from([raw(2)]));

    assert_eq!(
        join.complete(child_quiesced(second), QuiescentEvidence::Negative(raw(2))),
        Progress::MemberFenced
    );
    assert_eq!(
        join.advance_cursor(1).progress,
        Progress::Settled(SealReason::Candidate)
    );
    assert_eq!(join.result_bag(), vec![raw(1)]);
}

struct RaceOutcome {
    result: Vec<RawInline>,
    relation: Vec<RawInline>,
    retained_loser_route: bool,
}

fn run_whole_arm_race(winner: Arm) -> RaceOutcome {
    let mut join = EvidenceJoin::new([raw(3), raw(1), raw(2), raw(1), raw(4)]);
    let left = join.issue_shared(Arm(0));
    let right = join.issue_shared(Arm(1));
    let left_address = left.address();
    let right_address = right.address();

    assert_eq!(join.publish_positive(left_address, raw(1)), Progress::Open);
    assert_eq!(join.publish_positive(right_address, raw(3)), Progress::Open);
    let (winning, losing) = if winner == Arm(0) {
        (left, right)
    } else {
        (right, left)
    };
    let losing_address = losing.address();
    assert_eq!(
        join.complete(
            child_quiesced(winning),
            QuiescentEvidence::Exact(vec![raw(1), raw(3)])
        ),
        Progress::Settled(SealReason::SharedExact)
    );
    assert_eq!(join.result_bag(), vec![raw(3), raw(1), raw(1)]);
    assert_eq!(join.relation(), vec![raw(1), raw(3)]);
    assert_eq!(
        join.complete(child_quiesced(losing), QuiescentEvidence::Negative(raw(4))),
        Progress::Stale
    );

    RaceOutcome {
        result: join.result_bag(),
        relation: join.relation(),
        retained_loser_route: join.live_shared.values().any(|&arm| arm != winner)
            && !join.address_is_current(losing_address),
    }
}

#[test]
fn either_shared_exact_arm_can_win_with_the_same_parent_local_result() {
    let left = run_whole_arm_race(Arm(0));
    let right = run_whole_arm_race(Arm(1));

    assert_eq!(left.result, right.result);
    assert_eq!(left.relation, right.relation);
    assert!(left.retained_loser_route);
    assert!(right.retained_loser_route);
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ModelEvent {
    Cursor,
    SharedPositive(u8),
    Member(u8),
}

struct Replay {
    join: EvidenceJoin,
    streaming_address: EvidenceAddress,
    _streaming_work: EvidenceWork,
    exact_work: Option<EvidenceWork>,
    member_work: BTreeMap<u8, EvidenceWork>,
    shared_delivered: BTreeSet<u8>,
    member_delivered: BTreeSet<u8>,
}

fn replay_lazy_events(order: &[u8], truth: &BTreeSet<u8>, trace: &[ModelEvent]) -> Replay {
    let mut join = EvidenceJoin::new(order.iter().copied().map(raw));
    let streaming_work = join.issue_shared(Arm(9));
    let streaming_address = streaming_work.address();
    let exact_work = join.issue_shared(Arm(10));
    let mut replay = Replay {
        join,
        streaming_address,
        _streaming_work: streaming_work,
        exact_work: Some(exact_work),
        member_work: BTreeMap::new(),
        shared_delivered: BTreeSet::new(),
        member_delivered: BTreeSet::new(),
    };

    for &event in trace {
        assert!(!replay.join.settled(), "trace continued after settlement");
        match event {
            ModelEvent::Cursor => {
                let advance = replay.join.advance_cursor(1);
                assert_ne!(advance.progress, Progress::Stale);
                for work in advance.members {
                    let value = member_value(&work)[0];
                    assert!(
                        replay.member_work.insert(value, work).is_none(),
                        "cursor issued duplicate member work"
                    );
                }
            }
            ModelEvent::SharedPositive(value) => {
                assert!(truth.contains(&value));
                assert!(replay.shared_delivered.insert(value));
                let progress = replay
                    .join
                    .publish_positive(replay.streaming_address, raw(value));
                assert_ne!(progress, Progress::Stale);
            }
            ModelEvent::Member(value) => {
                assert!(replay.member_delivered.insert(value));
                let work = replay
                    .member_work
                    .remove(&value)
                    .expect("member event preceded discovery");
                let progress = if truth.contains(&value) {
                    replay.join.publish_positive(work.address(), raw(value))
                } else {
                    replay.join.complete(
                        child_quiesced(work),
                        QuiescentEvidence::Negative(raw(value)),
                    )
                };
                assert!(
                    matches!(
                        progress,
                        Progress::MemberFenced
                            | Progress::Settled(SealReason::Candidate)
                            | Progress::Settled(SealReason::Mixed)
                            | Progress::Stale
                    ),
                    "unexpected member transition: {progress:?}"
                );
            }
        }
        replay.join.assert_invariants();
    }
    replay
}

fn permutations(values: &[u8]) -> Vec<Vec<u8>> {
    if values.is_empty() {
        return vec![Vec::new()];
    }
    let mut result = Vec::new();
    for index in 0..values.len() {
        let mut rest = values.to_vec();
        let value = rest.remove(index);
        for mut suffix in permutations(&rest) {
            let mut permutation = vec![value];
            permutation.append(&mut suffix);
            result.push(permutation);
        }
    }
    result
}

fn explore_lazy_interleavings(
    order: &[u8],
    truth: &BTreeSet<u8>,
    trace: &mut Vec<ModelEvent>,
    terminal_schedules: &mut usize,
    exact_prefixes: &mut usize,
) {
    let replay = replay_lazy_events(order, truth, trace);
    if replay.join.settled() {
        let expected_relation: Vec<_> = truth.iter().copied().map(raw).collect();
        let expected_bag: Vec<_> = order
            .iter()
            .copied()
            .filter(|value| truth.contains(value))
            .map(raw)
            .collect();
        assert_eq!(replay.join.relation(), expected_relation);
        assert_eq!(replay.join.result_bag(), expected_bag);
        assert!(!replay.join.tail_open());
        assert!(matches!(
            replay.join.seal_reason,
            Some(SealReason::Candidate | SealReason::Mixed)
        ));
        *terminal_schedules += 1;
        return;
    }

    // Shared Exact is a legal cut at every still-open prefix, including before
    // discovery, after mixed evidence, and after EOF with members pending.
    // Include one value outside D to exercise predicate rather than
    // candidate-domain exactness.
    let mut exact_replay = replay_lazy_events(order, truth, trace);
    let exact_work = exact_replay.exact_work.take().unwrap();
    let mut exact_predicate: Vec<_> = truth.iter().copied().map(raw).collect();
    exact_predicate.push(raw(255));
    assert_eq!(
        exact_replay.join.complete(
            child_quiesced(exact_work),
            QuiescentEvidence::Exact(exact_predicate)
        ),
        Progress::Settled(SealReason::SharedExact)
    );
    assert_eq!(
        exact_replay.join.relation(),
        truth.iter().copied().map(raw).collect::<Vec<_>>()
    );
    assert_eq!(
        exact_replay.join.result_bag(),
        order
            .iter()
            .copied()
            .filter(|value| truth.contains(value))
            .map(raw)
            .collect::<Vec<_>>()
    );
    *exact_prefixes += 1;

    let mut enabled = Vec::new();
    if replay.join.tail_open() {
        enabled.push(ModelEvent::Cursor);
    }
    for &value in truth {
        if !replay.shared_delivered.contains(&value) {
            enabled.push(ModelEvent::SharedPositive(value));
        }
    }
    for &value in replay.member_work.keys() {
        if !replay.member_delivered.contains(&value) {
            enabled.push(ModelEvent::Member(value));
        }
    }
    assert!(
        !enabled.is_empty(),
        "legal lazy state had no route to closure: {trace:?}"
    );

    for event in enabled {
        trace.push(event);
        explore_lazy_interleavings(order, truth, trace, terminal_schedules, exact_prefixes);
        trace.pop();
    }
}

#[test]
fn exhaustive_legal_lazy_interleavings_converge_for_domains_up_to_three() {
    let mut terminal_schedules = 0;
    let mut exact_prefixes = 0;
    for size in 0..=3u8 {
        let domain: Vec<_> = (1..=size).collect();
        for order in permutations(&domain) {
            for mask in 0..(1u8 << size) {
                let truth: BTreeSet<_> = domain
                    .iter()
                    .copied()
                    .filter(|value| mask & (1 << (value - 1)) != 0)
                    .collect();
                explore_lazy_interleavings(
                    &order,
                    &truth,
                    &mut Vec::new(),
                    &mut terminal_schedules,
                    &mut exact_prefixes,
                );
            }
        }
    }
    assert!(
        terminal_schedules > 10_000,
        "the bounded model should cover a substantial interleaving space"
    );
    assert!(
        exact_prefixes > 10_000,
        "shared Exact should be checked at every open interleaving prefix"
    );
}
