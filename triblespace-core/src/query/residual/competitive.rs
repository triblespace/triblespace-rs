//! Executable model of a monotone first-completeness join.
//!
//! This is deliberately a test-harness prototype rather than a production
//! scheduler route.  [`TypedCompleteArbiter`](crate::query::TypedCompleteArbiter)
//! admits one already-complete physical action before it runs; it has no
//! online partial-result or loser-cancellation lifetime.  `EvidenceJoin`
//! instead models the missing runtime coordinator: equivalent executors may
//! monotonically tighten one finite answer interval until exactness logically
//! fences every outstanding affine receipt.

use std::collections::{BTreeMap, BTreeSet};

use crate::inline::RawInline;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Arm(u8);

#[derive(Debug)]
struct Ticket {
    generation: u64,
    nonce: u64,
    arm: Arm,
}

/// A receipt that crossed the trusted child-completion boundary.
///
/// Only the scheduler path that observes a child activation with no live
/// descendants may construct this wrapper. Streaming output never has one, so
/// it can publish positives but cannot claim exclusion or whole-arm exactness.
#[derive(Debug)]
struct QuiescentTicket(Ticket);

#[derive(Debug)]
enum QuiescentEvidence {
    /// A sound proof that one universe member is absent.
    Negative(RawInline),
    /// One executor's complete answer relation.
    Exact(Vec<RawInline>),
}

#[derive(Debug)]
enum Replace {
    Continue(Vec<Ticket>),
    Settled { fenced: usize },
    Stale,
}

/// Finite interval `must ⊆ answer ⊆ may` with affine physical work tickets.
///
/// `original` remains an occurrence bag.  Evidence is collected over its
/// distinct value universe, while publication filters the immutable bag so
/// order and multiplicity survive exactly until the engine's later SET
/// boundary.
#[derive(Debug)]
struct EvidenceJoin {
    original: Vec<RawInline>,
    universe: BTreeSet<RawInline>,
    must: BTreeSet<RawInline>,
    may: BTreeSet<RawInline>,
    generation: u64,
    next_nonce: u64,
    live: BTreeMap<u64, Arm>,
    settled: bool,
}

impl EvidenceJoin {
    fn new(original: impl IntoIterator<Item = RawInline>) -> Self {
        let original: Vec<_> = original.into_iter().collect();
        let universe: BTreeSet<_> = original.iter().copied().collect();
        assert!(
            !universe.is_empty(),
            "the prototype gives empty domains an immediate outer completion"
        );
        Self {
            original,
            may: universe.clone(),
            universe,
            must: BTreeSet::new(),
            generation: 0,
            next_nonce: 0,
            live: BTreeMap::new(),
            settled: false,
        }
    }

    fn issue(&mut self, arm: Arm) -> Ticket {
        assert!(!self.settled, "settled evidence join issued new work");
        let nonce = self.next_nonce;
        self.next_nonce = self
            .next_nonce
            .checked_add(1)
            .expect("evidence ticket nonce overflow");
        assert!(
            self.live.insert(nonce, arm).is_none(),
            "evidence ticket nonce was reused"
        );
        Ticket {
            generation: self.generation,
            nonce,
            arm,
        }
    }

    /// Affinely replaces one work ticket after publishing a sound positive.
    ///
    /// Positive evidence may stream before its child activation is quiescent.
    fn publish_positive(&mut self, ticket: Ticket, value: RawInline, successors: usize) -> Replace {
        if !self.consume(&ticket) {
            return Replace::Stale;
        }
        assert!(
            self.universe.contains(&value),
            "positive evidence escaped the candidate universe"
        );
        assert!(
            self.may.contains(&value),
            "positive evidence contradicted an earlier negative proof"
        );
        self.must.insert(value);
        self.finish(ticket.arm, successors)
    }

    /// Applies exclusion or exactness only after child work has quiesced.
    ///
    /// Clearing `live` below is logical receipt fencing: already-dispatched
    /// loser results become `Stale`. This prototype does not remove physical
    /// queue entries or retire activation-arena custody; production wiring
    /// needs that separate cancellation path.
    fn complete(&mut self, ticket: QuiescentTicket, evidence: QuiescentEvidence) -> Replace {
        let ticket = ticket.0;
        if !self.consume(&ticket) {
            return Replace::Stale;
        }
        self.apply_quiescent(evidence);
        self.finish(ticket.arm, 0)
    }

    fn consume(&mut self, ticket: &Ticket) -> bool {
        if ticket.generation != self.generation {
            return false;
        }
        assert!(!self.settled, "settled join retained its live generation");
        assert_eq!(
            self.live.remove(&ticket.nonce),
            Some(ticket.arm),
            "unknown, replayed, or cross-arm evidence ticket"
        );
        true
    }

    fn finish(&mut self, arm: Arm, successors: usize) -> Replace {
        assert!(
            self.must.is_subset(&self.may),
            "inconsistent executor evidence broke must ⊆ may"
        );

        if self.must == self.may {
            let fenced = self.live.len();
            self.live.clear();
            self.settled = true;
            self.generation = self
                .generation
                .checked_add(1)
                .expect("evidence generation overflow");
            return Replace::Settled { fenced };
        }

        Replace::Continue((0..successors).map(|_| self.issue(arm)).collect())
    }

    fn apply_quiescent(&mut self, evidence: QuiescentEvidence) {
        match evidence {
            QuiescentEvidence::Negative(value) => {
                assert!(
                    self.universe.contains(&value),
                    "negative evidence escaped the candidate universe"
                );
                assert!(
                    !self.must.contains(&value),
                    "negative evidence contradicted an earlier witness"
                );
                self.may.remove(&value);
            }
            QuiescentEvidence::Exact(values) => {
                let exact: BTreeSet<_> = values.into_iter().collect();
                assert!(
                    exact.is_subset(&self.universe),
                    "exact executor result escaped the candidate universe"
                );
                assert!(
                    self.must.is_subset(&exact),
                    "exact executor omitted an earlier sound positive"
                );
                assert!(
                    exact.is_subset(&self.may),
                    "exact executor included an earlier sound negative"
                );
                self.must.extend(exact.iter().copied());
                self.may.retain(|value| exact.contains(value));
            }
        }
    }

    fn relation(&self) -> Vec<RawInline> {
        assert!(self.settled, "read an unsettled evidence relation");
        self.must.iter().copied().collect()
    }

    fn result_bag(&self) -> Vec<RawInline> {
        assert!(self.settled, "published an unsettled evidence relation");
        self.original
            .iter()
            .copied()
            .filter(|value| self.must.contains(value))
            .collect()
    }

    fn result_digest(&self) -> [u8; 32] {
        let mut digest = blake3::Hasher::new();
        for value in self.result_bag() {
            digest.update(&value);
        }
        *digest.finalize().as_bytes()
    }

    fn live_tickets(&self) -> usize {
        self.live.len()
    }
}

/// Trusted adapter standing in for the scheduler's child-quiescence callback.
///
/// Keeping this boundary explicit makes it impossible to submit negative or
/// exact evidence with an ordinary streaming receipt.
fn child_quiesced(ticket: Ticket) -> QuiescentTicket {
    QuiescentTicket(ticket)
}

fn raw(byte: u8) -> RawInline {
    let mut value = RawInline::default();
    value[0] = byte;
    value
}

fn one_successor(replaced: Replace) -> Ticket {
    let Replace::Continue(mut tickets) = replaced else {
        panic!("expected one live replacement")
    };
    assert_eq!(tickets.len(), 1);
    tickets.pop().unwrap()
}

struct RaceOutcome {
    result: Vec<RawInline>,
    digest: [u8; 32],
    fenced: usize,
}

fn run_whole_arm_race(winner: Arm) -> RaceOutcome {
    let relation = vec![raw(1), raw(3)];
    let mut join = EvidenceJoin::new([raw(3), raw(1), raw(2), raw(1), raw(4)]);
    let left = join.issue(Arm(0));
    let right = join.issue(Arm(1));

    let (winning, losing) = if winner == Arm(0) {
        (left, right)
    } else {
        (right, left)
    };
    let winning = one_successor(join.publish_positive(winning, raw(1), 1));
    let losing = one_successor(join.publish_positive(losing, raw(3), 1));
    let Replace::Settled { fenced } =
        join.complete(child_quiesced(winning), QuiescentEvidence::Exact(relation))
    else {
        panic!("the first exact executor did not settle the race")
    };

    let result = join.result_bag();
    let digest = join.result_digest();
    assert_eq!(result, vec![raw(3), raw(1), raw(1)]);
    assert_eq!(fenced, 1);
    assert_eq!(join.live_tickets(), 0);

    let before = (result.clone(), digest);
    assert!(matches!(
        join.complete(child_quiesced(losing), QuiescentEvidence::Negative(raw(4))),
        Replace::Stale
    ));
    assert_eq!((join.result_bag(), join.result_digest()), before);
    assert_eq!(join.live_tickets(), 0);

    RaceOutcome {
        result,
        digest,
        fenced,
    }
}

#[test]
fn either_exact_arm_can_win_and_late_loser_work_is_fenced() {
    let left = run_whole_arm_race(Arm(0));
    let right = run_whole_arm_race(Arm(1));

    assert_eq!(left.result, right.result);
    assert_eq!(left.digest, right.digest);
    assert_eq!(left.fenced, right.fenced);
}

#[test]
fn loser_partial_positives_survive_the_winner_certificate() {
    let mut join = EvidenceJoin::new([raw(1), raw(2), raw(3)]);
    let winner = join.issue(Arm(0));
    let loser = join.issue(Arm(1));

    let loser = one_successor(join.publish_positive(loser, raw(3), 1));
    assert!(join.must.contains(&raw(3)));
    assert!(matches!(
        join.complete(
            child_quiesced(winner),
            QuiescentEvidence::Exact(vec![raw(1), raw(3)])
        ),
        Replace::Settled { fenced: 1 }
    ));
    assert_eq!(join.relation(), vec![raw(1), raw(3)]);
    assert!(matches!(
        join.publish_positive(loser, raw(3), 0),
        Replace::Stale
    ));
}

fn settle_singleton_candidate(value: RawInline, present: bool) -> EvidenceJoin {
    let mut candidate = EvidenceJoin::new([value]);
    let proof = candidate.issue(Arm(0));
    let cleanup = candidate.issue(Arm(1));
    let result = if present {
        candidate.publish_positive(proof, value, 0)
    } else {
        candidate.complete(child_quiesced(proof), QuiescentEvidence::Negative(value))
    };
    assert!(matches!(result, Replace::Settled { fenced: 1 }));
    assert!(matches!(
        candidate.complete(
            child_quiesced(cleanup),
            QuiescentEvidence::Exact(Vec::new())
        ),
        Replace::Stale
    ));
    assert_eq!(candidate.live_tickets(), 0);
    candidate
}

#[test]
fn singleton_existentials_compose_into_a_whole_arm_completion() {
    let first = settle_singleton_candidate(raw(1), true);
    let second = settle_singleton_candidate(raw(2), false);
    let third = settle_singleton_candidate(raw(3), true);

    // The candidate arm is an ordinary evidence join too. Each settled
    // singleton supplies one exact bit; the arm becomes exact only after all
    // candidate children have reported.
    let mut candidate_arm = EvidenceJoin::new([raw(1), raw(2), raw(3)]);
    let first_ticket = candidate_arm.issue(Arm(0));
    let second_ticket = candidate_arm.issue(Arm(0));
    let third_ticket = candidate_arm.issue(Arm(0));
    assert!(matches!(
        candidate_arm.publish_positive(first_ticket, raw(1), 0),
        Replace::Continue(tickets) if tickets.is_empty()
    ));
    assert!(matches!(
        candidate_arm.complete(
            child_quiesced(second_ticket),
            QuiescentEvidence::Negative(raw(2))
        ),
        Replace::Continue(tickets) if tickets.is_empty()
    ));
    assert!(matches!(
        candidate_arm.publish_positive(third_ticket, raw(3), 0),
        Replace::Settled { fenced: 0 }
    ));
    assert_eq!(first.relation(), vec![raw(1)]);
    assert!(second.relation().is_empty());
    assert_eq!(third.relation(), vec![raw(3)]);
    assert_eq!(candidate_arm.relation(), vec![raw(1), raw(3)]);
    assert_eq!(candidate_arm.live_tickets(), 0);

    // Only the full candidate-arm relation is a completeness certificate for
    // the outer equivalent-executor race.
    let mut outer = EvidenceJoin::new([raw(3), raw(1), raw(2), raw(1)]);
    let anchored_support = outer.issue(Arm(0));
    let ordinary_confirm = outer.issue(Arm(1));
    assert!(matches!(
        outer.complete(
            child_quiesced(anchored_support),
            QuiescentEvidence::Exact(candidate_arm.relation())
        ),
        Replace::Settled { fenced: 1 }
    ));
    assert_eq!(outer.result_bag(), vec![raw(3), raw(1), raw(1)]);
    assert_eq!(outer.live_tickets(), 0);
    assert!(matches!(
        outer.complete(
            child_quiesced(ordinary_confirm),
            QuiescentEvidence::Exact(vec![raw(1), raw(3)])
        ),
        Replace::Stale
    ));
}
