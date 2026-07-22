//! Probe-only 2×2 causal screen for two scalar action receipts.
//!
//! The four arms independently retain or discharge the redundant outer
//! intersection confirmation and retain or discharge SET hash admission:
//! A=(confirm, hash), B=(no confirm, hash), C=(confirm, no hash),
//! D=(no confirm, no hash). The fixture is deliberately unary so exactly one
//! concrete `push_next_variable` action is observed. Static source coverage,
//! concrete result layout, and validation discharge remain separate facts.
//! Arm C is lawful because confirmation is an order-preserving subbag filter:
//! deleting values from a `GroupedSet` result cannot create a duplicate.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::intersectionconstraint::IntersectionConstraint;
use super::*;

const VARIABLE: VariableId = 0;

#[derive(Clone, Copy, Debug)]
struct Fixture {
    name: &'static str,
    n: usize,
    stride: usize,
}

impl Fixture {
    fn kept(self) -> usize {
        assert!(self.stride > 0);
        assert_eq!(self.n % self.stride, 0);
        self.n / self.stride
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Variant {
    A,
    B,
    C,
    D,
}

impl Variant {
    const ALL: [Self; 4] = [Self::A, Self::B, Self::C, Self::D];

    fn label(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
        }
    }

    fn repeats_outer_confirm(self) -> bool {
        matches!(self, Self::A | Self::C)
    }

    fn runs_set_hash(self) -> bool {
        matches!(self, Self::A | Self::B)
    }

    fn receipt(self) -> ScalarActionResultReceipt {
        ScalarActionResultReceipt {
            layout: if self.runs_set_hash() {
                ScalarResultLayout::GroupedBag
            } else {
                ScalarResultLayout::GroupedSet
            },
            validation: if self.repeats_outer_confirm() {
                ScalarValidation::Pending
            } else {
                ScalarValidation::Discharged
            },
        }
    }
}

fn raw_ordinal(ordinal: usize) -> RawInline {
    let mut raw = [0; 32];
    raw[24..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    raw
}

fn ordinal(raw: &RawInline) -> usize {
    usize::try_from(u64::from_be_bytes(raw[24..].try_into().unwrap()))
        .expect("probe ordinal exceeds usize")
}

fn is_canonical_ordinal(raw: &RawInline, n: usize) -> bool {
    raw[..24].iter().all(|byte| *byte == 0) && ordinal(raw) < n
}

#[derive(Debug, Default)]
struct ActionCounters {
    source_propose_calls: AtomicUsize,
    source_proposed_occurrences: AtomicUsize,
    source_confirm_calls: AtomicUsize,
    source_confirm_occurrences: AtomicUsize,
    validator_confirm_calls: AtomicUsize,
    validator_confirm_occurrences: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActionCounterSnapshot {
    source_propose_calls: usize,
    source_proposed_occurrences: usize,
    source_confirm_calls: usize,
    source_confirm_occurrences: usize,
    validator_confirm_calls: usize,
    validator_confirm_occurrences: usize,
}

impl ActionCounters {
    fn snapshot(&self) -> ActionCounterSnapshot {
        ActionCounterSnapshot {
            source_propose_calls: self.source_propose_calls.load(Ordering::Relaxed),
            source_proposed_occurrences: self.source_proposed_occurrences.load(Ordering::Relaxed),
            source_confirm_calls: self.source_confirm_calls.load(Ordering::Relaxed),
            source_confirm_occurrences: self.source_confirm_occurrences.load(Ordering::Relaxed),
            validator_confirm_calls: self.validator_confirm_calls.load(Ordering::Relaxed),
            validator_confirm_occurrences: self
                .validator_confirm_occurrences
                .load(Ordering::Relaxed),
        }
    }
}

struct UniqueSource {
    n: usize,
    counters: Option<Arc<ActionCounters>>,
}

impl Constraint<'static> for UniqueSource {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(VARIABLE)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == VARIABLE && !bound.is_set(VARIABLE) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != VARIABLE {
            return false;
        }
        out.fill(self.n, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != VARIABLE {
            return;
        }
        if let Some(counters) = &self.counters {
            counters
                .source_propose_calls
                .fetch_add(1, Ordering::Relaxed);
            counters
                .source_proposed_occurrences
                .fetch_add(self.n * view.len(), Ordering::Relaxed);
        }
        for row in 0..view.len() as u32 {
            candidates.extend_row(row, (0..self.n).map(raw_ordinal));
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != VARIABLE {
            return;
        }
        if let Some(counters) = &self.counters {
            counters
                .source_confirm_calls
                .fetch_add(1, Ordering::Relaxed);
            counters
                .source_confirm_occurrences
                .fetch_add(candidates.len(), Ordering::Relaxed);
        }
        candidates.retain(|_, value| is_canonical_ordinal(value, self.n));
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(VARIABLE).is_none_or(|column| {
            view.iter()
                .all(|row| is_canonical_ordinal(&row[column], self.n))
        })
    }
}

struct CountingValidator {
    stride: usize,
    kept: usize,
    counters: Option<Arc<ActionCounters>>,
}

impl Constraint<'static> for CountingValidator {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(VARIABLE)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != VARIABLE {
            return false;
        }
        out.fill(self.kept, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        assert_ne!(variable, VARIABLE, "confirm-only validator became a source");
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != VARIABLE {
            return;
        }
        if let Some(counters) = &self.counters {
            counters
                .validator_confirm_calls
                .fetch_add(1, Ordering::Relaxed);
            counters
                .validator_confirm_occurrences
                .fetch_add(candidates.len(), Ordering::Relaxed);
        }
        candidates.retain(|_, value| ordinal(value).is_multiple_of(self.stride));
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(VARIABLE).is_none_or(|column| {
            view.iter()
                .all(|row| ordinal(&row[column]).is_multiple_of(self.stride))
        })
    }
}

type Root = IntersectionConstraint<Box<dyn Constraint<'static> + Send + Sync>>;

fn root(fixture: Fixture, counters: Option<Arc<ActionCounters>>) -> Root {
    IntersectionConstraint::new(vec![
        Box::new(UniqueSource {
            n: fixture.n,
            counters: counters.clone(),
        }),
        Box::new(CountingValidator {
            stride: fixture.stride,
            kept: fixture.kept(),
            counters,
        }),
    ])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HashCounterSnapshot {
    hash_calls: usize,
    hash_occurrences: usize,
    grouped_set_skips: usize,
}

fn hash_snapshot(telemetry: &ScalarActionProbeTelemetry) -> HashCounterSnapshot {
    HashCounterSnapshot {
        hash_calls: telemetry.hash_calls.load(Ordering::Relaxed),
        hash_occurrences: telemetry.hash_occurrences.load(Ordering::Relaxed),
        grouped_set_skips: telemetry.grouped_set_skips.load(Ordering::Relaxed),
    }
}

struct RunEvidence {
    rows: Vec<[RawInline; 1]>,
    actions: ActionCounterSnapshot,
    hashes: HashCounterSnapshot,
}

fn observed_run(variant: Variant, fixture: Fixture) -> RunEvidence {
    let counters = Arc::new(ActionCounters::default());
    let telemetry = Arc::new(ScalarActionProbeTelemetry::default());
    let root = root(fixture, Some(Arc::clone(&counters)));
    assert_eq!(
        root.proposal_coverage(VARIABLE, VariableSet::new_empty()),
        ProposalCoverage::Covering,
        "multi-child root coverage must remain separate from the action receipt"
    );
    let rows = Query::new(root, |binding: &Binding| {
        Some([*binding.get(VARIABLE).expect("unary binding")])
    })
    .with_scalar_action_probe(variant.receipt(), Some(Arc::clone(&telemetry)))
    .sequential()
    .collect();
    RunEvidence {
        rows,
        actions: counters.snapshot(),
        hashes: hash_snapshot(&telemetry),
    }
}

fn assert_structural_evidence(variant: Variant, fixture: Fixture, evidence: &RunEvidence) {
    let kept = fixture.kept();
    assert_eq!(evidence.actions.source_propose_calls, 1);
    assert_eq!(evidence.actions.source_proposed_occurrences, fixture.n);
    if variant.repeats_outer_confirm() {
        assert_eq!(evidence.actions.source_confirm_calls, 1);
        assert_eq!(evidence.actions.source_confirm_occurrences, kept);
        assert_eq!(evidence.actions.validator_confirm_calls, 2);
        assert_eq!(
            evidence.actions.validator_confirm_occurrences,
            fixture.n + kept
        );
    } else {
        assert_eq!(evidence.actions.source_confirm_calls, 0);
        assert_eq!(evidence.actions.source_confirm_occurrences, 0);
        assert_eq!(evidence.actions.validator_confirm_calls, 1);
        assert_eq!(evidence.actions.validator_confirm_occurrences, fixture.n);
    }
    if variant.runs_set_hash() {
        assert_eq!(
            evidence.hashes,
            HashCounterSnapshot {
                hash_calls: 1,
                hash_occurrences: kept,
                grouped_set_skips: 0,
            }
        );
    } else {
        assert_eq!(
            evidence.hashes,
            HashCounterSnapshot {
                hash_calls: 0,
                hash_occurrences: 0,
                grouped_set_skips: 1,
            }
        );
    }
}

fn assert_raw_set_oracle(fixture: Fixture, rows: &[[RawInline; 1]]) {
    let actual: BTreeSet<[RawInline; 1]> = rows.iter().copied().collect();
    let expected: BTreeSet<[RawInline; 1]> = (0..fixture.n)
        .filter(|ordinal| ordinal.is_multiple_of(fixture.stride))
        .map(|ordinal| [raw_ordinal(ordinal)])
        .collect();
    assert_eq!(actual, expected, "raw projected-tuple SET changed");
    assert_eq!(rows.len(), actual.len(), "duplicate raw tuple escaped");
}

#[test]
fn scalar_receipt_factorial_preserves_raw_set_and_counts() {
    let fixture = Fixture {
        name: "semantic",
        n: 64,
        stride: 4,
    };
    let mut baseline = None;
    for variant in Variant::ALL {
        let evidence = observed_run(variant, fixture);
        assert_structural_evidence(variant, fixture, &evidence);
        assert_raw_set_oracle(fixture, &evidence.rows);
        if let Some(baseline) = &baseline {
            assert_eq!(
                &evidence.rows,
                baseline,
                "variant {} changed raw bytes or traversal order",
                variant.label()
            );
        } else {
            baseline = Some(evidence.rows);
        }
    }
}

fn folded_run(variant: Variant, fixture: Fixture) -> (usize, u64) {
    let root = root(fixture, None);
    debug_assert_eq!(
        root.proposal_coverage(VARIABLE, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    let query = Query::new(root, |binding: &Binding| {
        Some([*binding.get(VARIABLE).expect("unary binding")])
    })
    .with_scalar_action_probe(variant.receipt(), None)
    .sequential();
    let mut count = 0usize;
    let mut checksum = 0u64;
    for [value] in query {
        count += 1;
        checksum = checksum.rotate_left(7) ^ ordinal(&value) as u64;
    }
    black_box((count, checksum))
}

fn timed_batch(variant: Variant, fixture: Fixture, repetitions: usize) -> u128 {
    let started = Instant::now();
    let mut digest = 0u64;
    for repetition in 0..repetitions {
        let (count, checksum) = folded_run(variant, fixture);
        digest ^= checksum.rotate_left((repetition % 64) as u32) ^ count as u64;
    }
    black_box(digest);
    started.elapsed().as_nanos() / repetitions as u128
}

#[derive(Default)]
struct Samples {
    a: Vec<u128>,
    b: Vec<u128>,
    c: Vec<u128>,
    d: Vec<u128>,
}

impl Samples {
    fn push(&mut self, variant: Variant, sample: u128) {
        match variant {
            Variant::A => self.a.push(sample),
            Variant::B => self.b.push(sample),
            Variant::C => self.c.push(sample),
            Variant::D => self.d.push(sample),
        }
    }

    fn medians(&self) -> [u128; 4] {
        [
            median(&self.a),
            median(&self.b),
            median(&self.c),
            median(&self.d),
        ]
    }
}

fn median(samples: &[u128]) -> u128 {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[test]
#[ignore = "release-only ABBA timing probe; run explicitly with --ignored --nocapture"]
fn scalar_receipt_factorial_abba() {
    const FIXTURES: [Fixture; 6] = [
        Fixture {
            name: "n256_k256",
            n: 256,
            stride: 1,
        },
        Fixture {
            name: "n4096_k4096",
            n: 4096,
            stride: 1,
        },
        Fixture {
            name: "n4096_k256",
            n: 4096,
            stride: 16,
        },
        Fixture {
            name: "n65536_k65536",
            n: 65_536,
            stride: 1,
        },
        Fixture {
            name: "n65536_k4096",
            n: 65_536,
            stride: 16,
        },
        Fixture {
            name: "n65536_k256",
            n: 65_536,
            stride: 256,
        },
    ];
    const ABBA: [Variant; 16] = [
        Variant::A,
        Variant::B,
        Variant::B,
        Variant::A,
        Variant::A,
        Variant::C,
        Variant::C,
        Variant::A,
        Variant::C,
        Variant::D,
        Variant::D,
        Variant::C,
        Variant::B,
        Variant::D,
        Variant::D,
        Variant::B,
    ];
    const CYCLES: usize = 8;
    const TARGET_SOURCE_OCCURRENCES: usize = 1_048_576;

    println!("receipt_factorial_2x2 format=v1 unit=ns_per_query cycles={CYCLES}");
    for fixture in FIXTURES {
        let evidence = observed_run(Variant::A, fixture);
        assert_structural_evidence(Variant::A, fixture, &evidence);
        assert_raw_set_oracle(fixture, &evidence.rows);

        for variant in Variant::ALL {
            let (count, _) = folded_run(variant, fixture);
            assert_eq!(count, fixture.kept());
        }
        let repetitions = TARGET_SOURCE_OCCURRENCES.div_ceil(fixture.n).clamp(2, 2048);
        let mut samples = Samples::default();
        for cycle in 0..CYCLES {
            if cycle.is_multiple_of(2) {
                for variant in ABBA {
                    samples.push(variant, timed_batch(variant, fixture, repetitions));
                }
            } else {
                for variant in ABBA.into_iter().rev() {
                    samples.push(variant, timed_batch(variant, fixture, repetitions));
                }
            }
        }
        let [a, b, c, d] = samples.medians();
        println!(
            "fixture={} n={} stride={} kept={} repetitions={} samples_per_variant={}",
            fixture.name,
            fixture.n,
            fixture.stride,
            fixture.kept(),
            repetitions,
            samples.a.len()
        );
        println!("variant=A confirm=1 hash=1 median_ns={a}");
        println!("variant=B confirm=0 hash=1 median_ns={b}");
        println!("variant=C confirm=1 hash=0 median_ns={c}");
        println!("variant=D confirm=0 hash=0 median_ns={d}");
        println!(
            "contrasts confirm_hash_on_ns={} confirm_hash_off_ns={} hash_confirm_on_ns={} hash_confirm_off_ns={} interaction_ns={}",
            a as i128 - b as i128,
            c as i128 - d as i128,
            a as i128 - c as i128,
            b as i128 - d as i128,
            d as i128 - b as i128 - c as i128 + a as i128,
        );
    }
}
