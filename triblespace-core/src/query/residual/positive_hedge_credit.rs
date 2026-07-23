//! Executable scheduling model for a demand-credit-bounded positive hedge.
//!
//! This is a `cfg(test)` proof model, not a production scheduler route. It
//! deliberately gives Exact/Confirm and fully-bound Support unrelated physical
//! traces. Their only rendezvous is a parent-local SET publication CAS on one
//! raw value. Exact acceptance and Support success carry different affine
//! witness types; neither can be converted into the other.
//!
//! Exact/Confirm is asymmetric: it remains live after either source wins early
//! publication and is the only computation allowed to settle the complete
//! accepted set `G`. Support is speculation. It stops after its first positive,
//! after exhausting its own trace, when Exact wins, or when Exact quiesces.
//!
//! The work law is physical rather than historical:
//!
//! `support_examined <= live_demand_grants + exact_examined_while_support_live`.
//!
//! A live demand grant sponsors the first lazy Support unit. Each validated
//! Exact work unit observed while Support remains live sponsors one additional
//! Support unit. Support asks geometrically (1, 2, 4, ...), but a page spends
//! the minimum of its requested quantum, its unspent credits, and its remaining
//! work. Credits denominate examined units, never pages, chunks, elapsed time,
//! estimates, or matching internal states. Closing Support affinely retires
//! every unspent credit.
//!
//! For a live hedge with demand `D`, Exact work `C`, and speculative work `S`,
//! the invariant gives `S <= D + C` and therefore total duplicated physical
//! work `C + S <= D + 2C`, independent of page boundaries and scheduling
//! order. That is a work bound, not an internal trace-equivalence claim.

use std::collections::BTreeSet;

type Value = u8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactAtom {
    Scan(u8),
    Accept(Value),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupportAtom {
    Scan(u8),
    Prove(Value),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Suffix {
    Identity,
    KeepEven,
}

impl Suffix {
    fn apply(self, input: &BTreeSet<Value>) -> BTreeSet<Value> {
        input
            .iter()
            .copied()
            .filter(|value| match self {
                Self::Identity => true,
                Self::KeepEven => value % 2 == 0,
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationSource {
    ExactTap,
    Support,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Publication {
    source: PublicationSource,
    value: Value,
}

/// Query demand is owned once by the hedge budget.
///
/// The type intentionally implements neither `Clone` nor `Copy`.
#[derive(Debug)]
struct DemandGrant;

/// Source-specific affine evidence minted by an incremental exact receipt.
///
/// Its generation belongs only to the publication race. Exact completion has
/// a separate handle and remains valid after this witness becomes stale.
#[derive(Debug)]
#[must_use = "an exact positive witness must be committed or retired stale"]
struct ExactWitness {
    generation: u64,
    value: Value,
}

/// Source-specific affine evidence minted by a whole validated Support receipt.
#[derive(Debug)]
#[must_use = "a Support positive witness must be committed or retired stale"]
struct SupportWitness {
    generation: u64,
    value: Value,
}

/// Sole authority to settle the complete exact accepted set.
///
/// This handle is not tied to the publication generation: an early Support win
/// must never invalidate or cancel it.
#[derive(Debug)]
#[must_use = "Exact/Confirm completion must settle or be dropped with the query"]
struct ExactCompletion {
    spine: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitOutcome {
    Committed(Publication),
    Stale(PublicationSource),
}

#[derive(Clone, Debug)]
struct Config {
    bag: Vec<Value>,
    accepted: BTreeSet<Value>,
    exact_trace: Vec<ExactAtom>,
    support_trace: Vec<SupportAtom>,
    demand_grants: usize,
    suffix: Suffix,
}

impl Config {
    fn target(&self) -> Value {
        *self.bag.first().expect("positive hedge requires B[0]")
    }

    fn assert_well_formed(&self) {
        let target = self.target();
        let bag_set: BTreeSet<_> = self.bag.iter().copied().collect();
        assert!(
            self.accepted.is_subset(&bag_set),
            "exact accepted set escaped the frozen candidate bag"
        );
        let exact_accepts = self
            .exact_trace
            .iter()
            .filter(|atom| **atom == ExactAtom::Accept(target))
            .count();
        let support_proofs = self
            .support_trace
            .iter()
            .filter(|atom| **atom == SupportAtom::Prove(target))
            .count();
        let target_is_true = self.accepted.contains(&target);
        assert_eq!(
            exact_accepts,
            usize::from(target_is_true),
            "incremental exact acceptance disagreed with final G"
        );
        assert_eq!(
            support_proofs,
            usize::from(target_is_true),
            "Support positivity was not sound and complete for its own trace"
        );
        assert!(
            self.exact_trace
                .iter()
                .all(|atom| { !matches!(atom, ExactAtom::Accept(value) if *value != target) }),
            "the compact model exposes only B[0]'s incremental exact tap"
        );
        assert!(
            self.support_trace
                .iter()
                .all(|atom| { !matches!(atom, SupportAtom::Prove(value) if *value != target) }),
            "the compact model exposes only B[0]'s Support hedge"
        );
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct WorkBudget {
    demand_minted: usize,
    matched_minted: usize,
    support_spent: usize,
    retired: usize,
}

impl WorkBudget {
    fn new(grants: Vec<DemandGrant>) -> Self {
        Self {
            demand_minted: grants.len(),
            ..Self::default()
        }
    }

    fn available(&self) -> usize {
        self.demand_minted
            .checked_add(self.matched_minted)
            .and_then(|minted| minted.checked_sub(self.support_spent))
            .and_then(|unspent| unspent.checked_sub(self.retired))
            .expect("demand-credit conservation underflow")
    }

    fn mint_matched(&mut self, examined: usize) {
        self.matched_minted = self
            .matched_minted
            .checked_add(examined)
            .expect("matched-work credit overflow");
        self.assert_conserved();
    }

    fn spend(&mut self, examined: usize) {
        assert!(
            examined <= self.available(),
            "Support spent work without demand/matched-work credit"
        );
        self.support_spent = self
            .support_spent
            .checked_add(examined)
            .expect("Support work counter overflow");
        self.assert_conserved();
    }

    fn retire_available(&mut self) {
        self.retired = self
            .retired
            .checked_add(self.available())
            .expect("retired-credit counter overflow");
        self.assert_conserved();
    }

    fn assert_conserved(&self) {
        assert_eq!(
            self.demand_minted + self.matched_minted,
            self.support_spent + self.retired + self.available(),
            "affine demand-credit custody was not conserved"
        );
        assert!(
            self.support_spent <= self.demand_minted + self.matched_minted,
            "Support exceeded its algebraic physical-work bound"
        );
    }
}

#[derive(Debug)]
struct ExactPageReceipt {
    witness: Option<ExactWitness>,
    completion: Option<ExactCompletion>,
}

#[derive(Debug)]
struct SupportPageReceipt {
    witness: Option<SupportWitness>,
    examined: usize,
}

#[derive(Debug)]
struct Hedge {
    config: Config,
    publication_generation: u64,
    publication_open: bool,
    support_running: bool,
    exact_cursor: usize,
    support_cursor: usize,
    exact_examined: usize,
    support_quantum: usize,
    exact_witness_issued: bool,
    exact_completion_issued: bool,
    support_witness_issued: bool,
    budget: WorkBudget,
    publication: Option<Publication>,
    early_output: BTreeSet<Value>,
    final_output: Option<BTreeSet<Value>>,
    stale_exact: usize,
    stale_support: usize,
    settled: bool,
}

impl Hedge {
    fn new(config: Config) -> Self {
        config.assert_well_formed();
        let grants = std::iter::repeat_with(|| DemandGrant)
            .take(config.demand_grants)
            .collect();
        let support_running = config.demand_grants > 0 && !config.support_trace.is_empty();
        let mut hedge = Self {
            config,
            publication_generation: 0,
            publication_open: true,
            support_running,
            exact_cursor: 0,
            support_cursor: 0,
            exact_examined: 0,
            support_quantum: 1,
            exact_witness_issued: false,
            exact_completion_issued: false,
            support_witness_issued: false,
            budget: WorkBudget::new(grants),
            publication: None,
            early_output: BTreeSet::new(),
            final_output: None,
            stale_exact: 0,
            stale_support: 0,
            settled: false,
        };
        if !hedge.support_running {
            hedge.budget.retire_available();
        }
        hedge.assert_invariants();
        hedge
    }

    fn target(&self) -> Value {
        self.config.target()
    }

    fn exact_remaining(&self) -> usize {
        self.config.exact_trace.len() - self.exact_cursor
    }

    fn support_remaining(&self) -> usize {
        self.config.support_trace.len() - self.support_cursor
    }

    fn can_step_support(&self) -> bool {
        self.support_running && self.support_remaining() > 0 && self.budget.available() > 0
    }

    fn exact_page(&mut self, limit: usize) -> ExactPageReceipt {
        assert!(!self.settled, "settled Exact spine was stepped");
        let examined = limit.min(self.exact_remaining());
        assert!(examined > 0, "Exact page made no physical progress");
        let end = self.exact_cursor + examined;
        let accepted_target = self.config.exact_trace[self.exact_cursor..end]
            .iter()
            .any(|atom| *atom == ExactAtom::Accept(self.target()));
        self.exact_cursor = end;
        self.exact_examined = self
            .exact_examined
            .checked_add(examined)
            .expect("Exact examined-work counter overflow");
        if self.support_running {
            self.budget.mint_matched(examined);
        }

        let witness = if accepted_target {
            assert!(
                !self.exact_witness_issued,
                "Exact trace issued B[0] acceptance twice"
            );
            self.exact_witness_issued = true;
            Some(ExactWitness {
                generation: 0,
                value: self.target(),
            })
        } else {
            None
        };
        let completion = if self.exact_cursor == self.config.exact_trace.len() {
            assert!(
                !self.exact_completion_issued,
                "Exact completion authority was issued twice"
            );
            self.exact_completion_issued = true;
            Some(ExactCompletion { spine: 1 })
        } else {
            None
        };
        if completion.is_some() {
            // The validated receipt has already observed graph quiescence.
            // Closing here (rather than when the outer continuation later
            // consumes ExactCompletion) prevents physical Support work or a
            // delayed positive claim from crossing that semantic boundary.
            // A target first accepted in this final receipt needs no early
            // tap: Exact settlement will emit it from G.
            self.close_publication();
        }
        self.assert_invariants();
        ExactPageReceipt {
            witness,
            completion,
        }
    }

    fn support_page(&mut self) -> SupportPageReceipt {
        assert!(
            self.can_step_support(),
            "Support page lacked live demand/matched-work credit"
        );
        let limit = self
            .support_quantum
            .min(self.budget.available())
            .min(self.support_remaining());
        assert!(limit > 0);
        let mut examined = 0;
        let mut proved = false;
        while examined < limit {
            let atom = self.config.support_trace[self.support_cursor];
            self.support_cursor += 1;
            examined += 1;
            if atom == SupportAtom::Prove(self.target()) {
                proved = true;
                break;
            }
        }
        self.budget.spend(examined);

        let witness = if proved {
            assert!(
                !self.support_witness_issued,
                "Support issued more than one positive witness"
            );
            self.support_witness_issued = true;
            self.stop_support();
            Some(SupportWitness {
                generation: self.publication_generation,
                value: self.target(),
            })
        } else {
            if self.support_cursor == self.config.support_trace.len() {
                self.stop_support();
            } else {
                self.support_quantum = self
                    .support_quantum
                    .checked_mul(2)
                    .expect("Support geometric quantum overflow");
            }
            None
        };
        self.assert_invariants();
        SupportPageReceipt { witness, examined }
    }

    fn stop_support(&mut self) {
        if self.support_running {
            self.support_running = false;
            self.budget.retire_available();
        }
    }

    fn close_publication(&mut self) {
        if self.publication_open {
            self.publication_open = false;
            self.publication_generation = self
                .publication_generation
                .checked_add(1)
                .expect("publication generation overflow");
        }
        self.stop_support();
    }

    fn commit_exact(&mut self, witness: ExactWitness) -> CommitOutcome {
        let outcome = self.commit(
            witness.generation,
            witness.value,
            PublicationSource::ExactTap,
        );
        if matches!(outcome, CommitOutcome::Stale(_)) {
            self.stale_exact += 1;
        }
        outcome
    }

    fn commit_support(&mut self, witness: SupportWitness) -> CommitOutcome {
        let outcome = self.commit(
            witness.generation,
            witness.value,
            PublicationSource::Support,
        );
        if matches!(outcome, CommitOutcome::Stale(_)) {
            self.stale_support += 1;
        }
        outcome
    }

    fn commit(
        &mut self,
        generation: u64,
        value: Value,
        source: PublicationSource,
    ) -> CommitOutcome {
        if self.settled
            || !self.publication_open
            || generation != self.publication_generation
            || self.publication.is_some()
        {
            self.assert_invariants();
            return CommitOutcome::Stale(source);
        }
        assert_eq!(value, self.target(), "positive witness changed B[0]");
        assert!(
            self.config.accepted.contains(&value),
            "positive witness contradicted exact denotation"
        );
        let publication = Publication { source, value };
        self.publication = Some(publication);
        self.early_output = self
            .config
            .suffix
            .apply(&BTreeSet::from([publication.value]));
        self.close_publication();
        self.assert_invariants();
        CommitOutcome::Committed(publication)
    }

    fn settle_exact(&mut self, completion: ExactCompletion) {
        assert_eq!(completion.spine, 1, "foreign Exact spine settled parent");
        assert!(
            self.exact_cursor == self.config.exact_trace.len(),
            "nonquiescent Exact spine attempted settlement"
        );
        assert!(!self.settled, "Exact spine settled twice");

        let bag_set: BTreeSet<_> = self.config.bag.iter().copied().collect();
        let accepted: BTreeSet<_> = self
            .config
            .accepted
            .intersection(&bag_set)
            .copied()
            .collect();
        let mut remainder = accepted.clone();
        if let Some(publication) = self.publication {
            assert!(
                accepted.contains(&publication.value),
                "early positive disappeared from exact G"
            );
            remainder.remove(&publication.value);
        }
        self.final_output = Some(self.config.suffix.apply(&remainder));
        self.settled = true;
        self.close_publication();

        let mut observed = self.early_output.clone();
        observed.extend(
            self.final_output
                .as_ref()
                .expect("Exact settlement omitted final continuation"),
        );
        assert_eq!(
            observed,
            self.config.suffix.apply(&accepted),
            "early SET publication plus exact remainder changed continuation semantics"
        );
        self.assert_invariants();
    }

    fn observed_output(&self) -> BTreeSet<Value> {
        assert!(
            self.settled,
            "observed output requested before Exact settlement"
        );
        let mut observed = self.early_output.clone();
        observed.extend(
            self.final_output
                .as_ref()
                .expect("settled hedge omitted final continuation"),
        );
        observed
    }

    fn expected_output(&self) -> BTreeSet<Value> {
        let bag_set: BTreeSet<_> = self.config.bag.iter().copied().collect();
        let accepted: BTreeSet<_> = self
            .config
            .accepted
            .intersection(&bag_set)
            .copied()
            .collect();
        self.config.suffix.apply(&accepted)
    }

    fn assert_invariants(&self) {
        self.budget.assert_conserved();
        assert!(
            self.budget.support_spent <= self.config.demand_grants + self.exact_examined,
            "Support work exceeded demand plus all observed Exact work"
        );
        if !self.support_running {
            assert_eq!(
                self.budget.available(),
                0,
                "stopped Support retained spendable affine credit"
            );
        }
        if self.publication.is_some() {
            assert!(
                !self.publication_open && !self.support_running,
                "publication winner left the hedge runnable"
            );
        }
        if self.settled {
            assert!(
                !self.publication_open && !self.support_running,
                "Exact settlement left speculative state live"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Event {
    ExactPage(usize),
    SupportPage,
    CommitExact,
    CommitSupport,
    SettleExact,
}

struct Replay {
    hedge: Hedge,
    exact_witness: Option<ExactWitness>,
    support_witness: Option<SupportWitness>,
    completion: Option<ExactCompletion>,
    max_support_page: usize,
}

fn replay(config: &Config, trace: &[Event]) -> Replay {
    let mut replay = Replay {
        hedge: Hedge::new(config.clone()),
        exact_witness: None,
        support_witness: None,
        completion: None,
        max_support_page: 0,
    };
    for event in trace {
        match *event {
            Event::ExactPage(limit) => {
                let receipt = replay.hedge.exact_page(limit);
                if let Some(witness) = receipt.witness {
                    assert!(
                        replay.exact_witness.replace(witness).is_none(),
                        "trace overwrote a live Exact witness"
                    );
                }
                if let Some(completion) = receipt.completion {
                    assert!(
                        replay.completion.replace(completion).is_none(),
                        "trace overwrote live Exact completion"
                    );
                }
            }
            Event::SupportPage => {
                let receipt = replay.hedge.support_page();
                replay.max_support_page = replay.max_support_page.max(receipt.examined);
                if let Some(witness) = receipt.witness {
                    assert!(
                        replay.support_witness.replace(witness).is_none(),
                        "trace overwrote a live Support witness"
                    );
                }
            }
            Event::CommitExact => {
                let witness = replay
                    .exact_witness
                    .take()
                    .expect("Exact commit preceded its receipt");
                let _ = replay.hedge.commit_exact(witness);
            }
            Event::CommitSupport => {
                let witness = replay
                    .support_witness
                    .take()
                    .expect("Support commit preceded its receipt");
                let _ = replay.hedge.commit_support(witness);
            }
            Event::SettleExact => {
                let completion = replay
                    .completion
                    .take()
                    .expect("Exact settlement preceded quiescence");
                replay.hedge.settle_exact(completion);
            }
        }
        replay.hedge.assert_invariants();
    }
    replay
}

#[derive(Default)]
struct Coverage {
    terminal_schedules: usize,
    exact_wins: usize,
    support_wins: usize,
    exact_only: usize,
    stale_exact: usize,
    stale_support: usize,
    geometric_pages: usize,
    distinct_chunkings: BTreeSet<Vec<usize>>,
}

fn explore(
    config: &Config,
    trace: &mut Vec<Event>,
    exact_chunks: &mut Vec<usize>,
    coverage: &mut Coverage,
) {
    let replayed = replay(config, trace);
    let terminal = replayed.hedge.settled
        && replayed.exact_witness.is_none()
        && replayed.support_witness.is_none()
        && replayed.completion.is_none();
    if terminal {
        assert_eq!(
            replayed.hedge.observed_output(),
            replayed.hedge.expected_output()
        );
        assert_eq!(
            replayed.hedge.exact_cursor,
            replayed.hedge.config.exact_trace.len(),
            "Support winner cancelled the Exact completeness spine"
        );
        coverage.terminal_schedules += 1;
        match replayed
            .hedge
            .publication
            .map(|publication| publication.source)
        {
            Some(PublicationSource::ExactTap) => coverage.exact_wins += 1,
            Some(PublicationSource::Support) => coverage.support_wins += 1,
            None => coverage.exact_only += 1,
        }
        coverage.stale_exact += replayed.hedge.stale_exact;
        coverage.stale_support += replayed.hedge.stale_support;
        if replayed.max_support_page > 1 {
            coverage.geometric_pages += 1;
        }
        coverage.distinct_chunkings.insert(exact_chunks.clone());
        return;
    }

    let mut enabled = Vec::new();
    if !replayed.hedge.settled && replayed.hedge.exact_remaining() > 0 {
        for width in 1..=replayed.hedge.exact_remaining() {
            enabled.push(Event::ExactPage(width));
        }
    }
    if replayed.hedge.can_step_support() {
        enabled.push(Event::SupportPage);
    }
    if replayed.exact_witness.is_some() {
        enabled.push(Event::CommitExact);
    }
    if replayed.support_witness.is_some() {
        enabled.push(Event::CommitSupport);
    }
    if replayed.completion.is_some() && !replayed.hedge.settled {
        enabled.push(Event::SettleExact);
    }
    assert!(!enabled.is_empty(), "demand-credit model deadlocked");

    for event in enabled {
        trace.push(event);
        if let Event::ExactPage(width) = event {
            exact_chunks.push(width);
        }
        explore(config, trace, exact_chunks, coverage);
        if matches!(event, Event::ExactPage(_)) {
            exact_chunks.pop();
        }
        trace.pop();
    }
}

fn true_config(exact_trace: Vec<ExactAtom>, support_trace: Vec<SupportAtom>) -> Config {
    Config {
        bag: vec![1, 1, 2],
        accepted: BTreeSet::from([1, 2]),
        exact_trace,
        support_trace,
        demand_grants: 1,
        suffix: Suffix::Identity,
    }
}

#[test]
fn support_winner_never_cancels_the_exact_completeness_spine() {
    let config = true_config(
        vec![
            ExactAtom::Scan(90),
            ExactAtom::Accept(1),
            ExactAtom::Scan(91),
        ],
        vec![SupportAtom::Prove(1)],
    );
    let mut hedge = Hedge::new(config);
    let support = hedge
        .support_page()
        .witness
        .expect("first demand credit should reach the Support proof");
    assert_eq!(
        hedge.commit_support(support),
        CommitOutcome::Committed(Publication {
            source: PublicationSource::Support,
            value: 1,
        })
    );
    assert_eq!(hedge.exact_cursor, 0);
    assert!(!hedge.support_running);

    let exact = hedge.exact_page(3);
    let stale_exact = exact
        .witness
        .expect("Exact still reports its own acceptance");
    assert_eq!(
        hedge.commit_exact(stale_exact),
        CommitOutcome::Stale(PublicationSource::ExactTap)
    );
    hedge.settle_exact(exact.completion.expect("Exact must still quiesce"));
    assert_eq!(hedge.exact_examined, 3);
    assert_eq!(hedge.observed_output(), BTreeSet::from([1, 2]));
}

#[test]
fn exact_and_support_internal_orders_need_not_match() {
    let config = Config {
        bag: vec![1, 2],
        accepted: BTreeSet::from([1, 2]),
        exact_trace: vec![
            ExactAtom::Scan(99),
            ExactAtom::Accept(1),
            ExactAtom::Scan(7),
        ],
        support_trace: vec![
            SupportAtom::Scan(7),
            SupportAtom::Scan(42),
            SupportAtom::Prove(1),
        ],
        demand_grants: 1,
        suffix: Suffix::KeepEven,
    };
    let mut hedge = Hedge::new(config);

    assert_eq!(hedge.support_page().examined, 1);
    let exact = hedge.exact_page(2);
    let support = hedge.support_page();
    assert_eq!(support.examined, 2);
    assert!(matches!(
        hedge.commit_support(support.witness.unwrap()),
        CommitOutcome::Committed(_)
    ));
    assert!(matches!(
        hedge.commit_exact(exact.witness.unwrap()),
        CommitOutcome::Stale(PublicationSource::ExactTap)
    ));
    let exact = hedge.exact_page(1);
    hedge.settle_exact(exact.completion.unwrap());
    assert_eq!(
        hedge.observed_output(),
        BTreeSet::from([2]),
        "the continuation observes SET semantics, not either internal order"
    );
}

#[test]
fn geometric_pages_spend_examined_units_not_page_receipts() {
    let mut hedge = Hedge::new(true_config(
        vec![ExactAtom::Scan(1), ExactAtom::Scan(2), ExactAtom::Accept(1)],
        vec![
            SupportAtom::Scan(8),
            SupportAtom::Scan(9),
            SupportAtom::Prove(1),
        ],
    ));
    assert_eq!(hedge.support_page().examined, 1);
    let exact = hedge.exact_page(2);
    assert!(exact.witness.is_none());
    let support = hedge.support_page();
    assert_eq!(
        support.examined, 2,
        "a two-unit geometric request should spend two matched-work credits"
    );
    assert_eq!(hedge.budget.support_spent, 3);
    assert_eq!(hedge.budget.demand_minted + hedge.budget.matched_minted, 3);

    // The rejected alternative is intentionally executable as a falsifier:
    // treating one page receipt as authority for a whole geometric page would
    // permit 1 + 2 + 4 Support units after only three one-unit Exact pages.
    let bad_support_examined = 1 + 2 + 4;
    let lawful_authority = 1 + 3;
    assert!(
        bad_support_examined > lawful_authority,
        "page-count credits accidentally satisfied the physical-work law"
    );
}

#[test]
fn no_live_demand_creates_no_support_work() {
    let mut config = true_config(vec![ExactAtom::Accept(1)], vec![SupportAtom::Prove(1)]);
    config.demand_grants = 0;
    let mut hedge = Hedge::new(config);
    assert!(!hedge.can_step_support());
    assert_eq!(hedge.budget.support_spent, 0);
    let exact = hedge.exact_page(1);
    hedge.settle_exact(exact.completion.unwrap());
    assert_eq!(hedge.observed_output(), BTreeSet::from([1, 2]));
}

#[test]
fn exhaustive_divergent_schedules_preserve_set_and_affine_work_laws() {
    let true_configs = [
        true_config(
            vec![
                ExactAtom::Accept(1),
                ExactAtom::Scan(10),
                ExactAtom::Scan(11),
            ],
            vec![
                SupportAtom::Scan(20),
                SupportAtom::Scan(21),
                SupportAtom::Prove(1),
            ],
        ),
        true_config(
            vec![
                ExactAtom::Scan(12),
                ExactAtom::Scan(13),
                ExactAtom::Accept(1),
            ],
            vec![
                SupportAtom::Prove(1),
                SupportAtom::Scan(22),
                SupportAtom::Scan(23),
            ],
        ),
        true_config(
            vec![
                ExactAtom::Scan(14),
                ExactAtom::Accept(1),
                ExactAtom::Scan(15),
            ],
            vec![
                SupportAtom::Scan(24),
                SupportAtom::Prove(1),
                SupportAtom::Scan(25),
            ],
        ),
    ];
    let false_config = Config {
        bag: vec![1, 1, 2],
        accepted: BTreeSet::from([2]),
        exact_trace: vec![
            ExactAtom::Scan(30),
            ExactAtom::Scan(31),
            ExactAtom::Scan(32),
        ],
        support_trace: vec![
            SupportAtom::Scan(40),
            SupportAtom::Scan(41),
            SupportAtom::Scan(42),
        ],
        demand_grants: 1,
        suffix: Suffix::Identity,
    };

    let mut coverage = Coverage::default();
    for mut config in true_configs.into_iter().chain([false_config]) {
        for suffix in [Suffix::Identity, Suffix::KeepEven] {
            config.suffix = suffix;
            explore(&config, &mut Vec::new(), &mut Vec::new(), &mut coverage);
        }
    }

    assert!(
        coverage.terminal_schedules > 500,
        "only {} terminal schedules were explored",
        coverage.terminal_schedules
    );
    assert!(coverage.exact_wins > 0);
    assert!(coverage.support_wins > 0);
    assert!(coverage.exact_only > 0);
    assert!(coverage.stale_exact > 0);
    assert!(coverage.stale_support > 0);
    assert!(coverage.geometric_pages > 0);
    assert!(
        coverage.distinct_chunkings.len() > 3,
        "exhaustive model did not vary Exact page boundaries"
    );
}
