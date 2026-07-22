//! Focused adoption gate for selective Production formula regions.
//!
//! The fixture hides one production RPQ behind an OR whose dead sibling has
//! a configurable number of equality atoms. `ProductionRegions` should pay
//! for the three-node OR skeleton, while `WholeRoot` deliberately exposes the
//! entire off-path subtree. Five diagnostic cells separate construction,
//! width-one pull latency, fresh-query latency, cancellation, and a 65,536-row
//! full drain.
//!
//! Fixture construction, an independently generated SET oracle, and one hot
//! warm-up pass per lowering mode stay outside the reported samples. Each cell
//! runs the three modes as a rotated pair-set, and cell order rotates too.
//! Reported p50/p95 values and paired ratios are diagnostic: this example does
//! not supply a preregistered A/A noise bound, epsilon, confidence interval,
//! or GO/STOP acceptance rule.
//!
//! Usage:
//!     cargo run --release --example production_formula_regions_bench -- [repetitions=11] [equalities=32]

use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::{Duration, Instant};

use triblespace::core::id::{ExclusiveId, Id};
use triblespace::core::inline::RawInline;
use triblespace::core::query::equalityconstraint::EqualityConstraint;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace::core::query::residual::{
    FormulaScope, ProgramScope, ResidualLowering, ResidualStateIter, ResidualStateStats,
};
use triblespace::core::query::unionconstraint::UnionConstraint;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, Variable, VariableId,
    VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

const ROWS: usize = 65_536;

type OwnedConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = IntersectionConstraint<OwnedConstraint>;
type BenchIter = ResidualStateIter<Root, fn(&Binding) -> Option<RawInline>, RawInline>;

#[derive(Clone, Copy)]
struct FalseConstraint;

impl Constraint<'static> for FalseConstraint {
    fn variables(&self) -> VariableSet {
        VariableSet::new_empty()
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn estimate(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _out: &mut EstimateSink<'_>,
    ) -> bool {
        false
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

    fn satisfied(&self, _view: &RowsView<'_>) -> bool {
        false
    }
}

struct Fixture {
    graph: TribleSet,
    attribute: Id,
    source: Id,
    expected: BTreeSet<RawInline>,
    expected_signature: Signature,
}

fn fixture_id(domain: u32, index: usize) -> Id {
    let ordinal = u64::try_from(index).expect("fixture ordinal fits u64") + 1;
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&domain.to_be_bytes());
    raw[4..12].copy_from_slice(&ordinal.to_be_bytes());
    raw[12..].copy_from_slice(&(ordinal as u32).rotate_left(13).to_be_bytes());
    Id::new(raw).expect("fixture IDs are non-nil")
}

fn build_fixture(rows: usize) -> Fixture {
    let attribute = fixture_id(1, 0);
    let source = fixture_id(2, 0);
    let mut graph = TribleSet::new();
    let mut expected = BTreeSet::new();
    for index in 0..rows {
        let target = fixture_id(3, index);
        let target_inline = target.to_inline();
        graph.insert(&Trible::new::<GenId>(
            ExclusiveId::force_ref(&source),
            &attribute,
            &target_inline,
        ));
        assert!(expected.insert(target_inline.raw));
    }
    let expected_signature = tally(expected.iter().copied());
    Fixture {
        graph,
        attribute,
        source,
        expected,
        expected_signature,
    }
}

fn make_root(fixture: &Fixture, equality_count: usize) -> Root {
    assert!(
        equality_count > 0,
        "the dead arm must retain the RPQ schema"
    );
    let start = Variable::<GenId>::new(0);
    let end = Variable::<GenId>::new(1);
    let path = RegularPathConstraint::new(
        fixture.graph.clone(),
        start,
        end,
        &[PathOp::Attr(fixture.attribute.raw())],
    );

    let mut dead_arm = Vec::with_capacity(equality_count + 1);
    for _ in 0..equality_count {
        dead_arm.push(Box::new(EqualityConstraint::new(start.index, end.index)) as OwnedConstraint);
    }
    dead_arm.push(Box::new(FalseConstraint) as OwnedConstraint);

    IntersectionConstraint::new(vec![
        Box::new(start.is(fixture.source.to_inline())) as OwnedConstraint,
        Box::new(UnionConstraint::new(vec![
            Box::new(IntersectionConstraint::new(dead_arm)) as OwnedConstraint,
            Box::new(path) as OwnedConstraint,
        ])) as OwnedConstraint,
    ])
}

#[derive(Clone, Copy)]
struct Mode {
    label: &'static str,
    lowering: ResidualLowering,
}

const MODES: [Mode; 3] = [
    Mode {
        label: "opaque-production",
        lowering: ResidualLowering::OPAQUE_PRODUCTION,
    },
    Mode {
        label: "production-regions",
        lowering: ResidualLowering::PRODUCTION,
    },
    Mode {
        label: "whole-root",
        lowering: ResidualLowering::new(FormulaScope::WholeRoot, ProgramScope::Production),
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Signature {
    rows: usize,
    checksum: u64,
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn row_checksum(value: &RawInline) -> u64 {
    value
        .chunks_exact(8)
        .enumerate()
        .fold(0, |checksum, (index, word)| {
            let word = u64::from_be_bytes(word.try_into().expect("eight-byte chunk"));
            checksum.wrapping_add(mix64(word.rotate_left((index * 13) as u32)))
        })
}

fn tally(rows: impl IntoIterator<Item = RawInline>) -> Signature {
    let mut signature = Signature {
        rows: 0,
        checksum: 0,
    };
    for row in rows {
        signature.rows += 1;
        signature.checksum = signature.checksum.wrapping_add(row_checksum(&row));
    }
    signature
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(1).copied()
}

fn make_iter(fixture: &Fixture, equality_count: usize, mode: Mode) -> BenchIter {
    Query::new(
        make_root(fixture, equality_count),
        project_end as fn(&Binding) -> Option<RawInline>,
    )
    .solve_residual_state_lazy_with(mode.lowering)
    .cap(fixture.expected.len().max(1))
    .start_width(1)
}

struct Measurement {
    elapsed: Duration,
    signature: Signature,
    first: Option<RawInline>,
    stats: ResidualStateStats,
    final_width: usize,
}

fn finish_first(mut query: BenchIter, start: Instant) -> Measurement {
    let first = black_box(query.next());
    let elapsed = start.elapsed();
    let signature = tally(first);
    let stats = query.stats().clone();
    let final_width = query.current_width();
    Measurement {
        elapsed,
        signature,
        first,
        stats,
        final_width,
    }
}

fn measure_construct_drop(fixture: &Fixture, equality_count: usize, mode: Mode) -> Duration {
    let start = Instant::now();
    drop(black_box(make_iter(fixture, equality_count, mode)));
    start.elapsed()
}

fn measure_pull_first(fixture: &Fixture, equality_count: usize, mode: Mode) -> Measurement {
    let query = make_iter(fixture, equality_count, mode);
    finish_first(query, Instant::now())
}

fn measure_fresh_first(fixture: &Fixture, equality_count: usize, mode: Mode) -> Measurement {
    let start = Instant::now();
    finish_first(make_iter(fixture, equality_count, mode), start)
}

fn measure_drop_first(
    fixture: &Fixture,
    equality_count: usize,
    mode: Mode,
) -> (Duration, Option<RawInline>) {
    let start = Instant::now();
    let mut query = make_iter(fixture, equality_count, mode);
    let first = black_box(query.next());
    drop(query);
    (start.elapsed(), first)
}

fn measure_full(fixture: &Fixture, equality_count: usize, mode: Mode) -> Measurement {
    let start = Instant::now();
    let mut query = make_iter(fixture, equality_count, mode);
    let signature = black_box(tally(query.by_ref()));
    let elapsed = start.elapsed();
    let stats = query.stats().clone();
    let final_width = query.current_width();
    Measurement {
        elapsed,
        signature,
        first: None,
        stats,
        final_width,
    }
}

fn percentile(samples: &[Duration], quantile: f64) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

fn percentile_f64(samples: &[f64], quantile: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn validate_measurement(measurement: &Measurement, fixture: &Fixture, full: bool, label: &str) {
    if full {
        assert_eq!(
            measurement.signature, fixture.expected_signature,
            "{label}: full-drain signature mismatch"
        );
    } else {
        assert_eq!(measurement.signature.rows, 1, "{label}: no first row");
        assert!(
            fixture
                .expected
                .contains(measurement.first.as_ref().expect("one first row")),
            "{label}: first row is outside the independent SET oracle"
        );
    }
}

fn validate_first(first: Option<RawInline>, fixture: &Fixture, label: &str) {
    assert!(
        first.is_some_and(|row| fixture.expected.contains(&row)),
        "{label}: first row is absent or outside the independent SET oracle"
    );
}

fn print_stats(prefix: &str, measurement: &Measurement) {
    let stats = &measurement.stats;
    println!(
        "    {prefix:<5} pops(full/ready/continuation)={}/{}/{} total={} partial={} underfilled_continuation={} actions(p/c/s)={}/{}/{} calls={}/{}/{}",
        stats.full_pops,
        stats.readiness_pops,
        stats.continuation_pops,
        stats.state_pops,
        stats.partial_pops,
        stats.underfilled_continuation_pops,
        stats.propose_action_pops,
        stats.confirm_action_pops,
        stats.support_action_pops,
        stats.propose_calls,
        stats.confirm_calls,
        stats.support_calls,
    );
    println!(
        "          delta pages(source/transition)={}/{} cohorts={}/{} max_cohort={}/{} examined={}/{} terminal_windows={} promotions={} final_width={}",
        stats.delta_source_pages,
        stats.delta_transition_pages,
        stats.delta_source_cohorts,
        stats.delta_transition_cohorts,
        stats.max_delta_source_cohort,
        stats.max_delta_transition_cohort,
        stats.delta_source_candidates_examined,
        stats.delta_transition_candidates_examined,
        stats.terminal_demand_windows_opened,
        stats.terminal_demand_width_promotions,
        measurement.final_width,
    );
}

#[derive(Clone, Copy)]
enum Cell {
    ConstructDrop,
    PullFirst,
    FreshFirst,
    DropFirst,
    Full,
}

const CELLS: [Cell; 5] = [
    Cell::ConstructDrop,
    Cell::PullFirst,
    Cell::FreshFirst,
    Cell::DropFirst,
    Cell::Full,
];

struct Samples {
    construct_drop: Vec<Duration>,
    pull_first: Vec<Duration>,
    fresh_first: Vec<Duration>,
    drop_first: Vec<Duration>,
    full: Vec<Duration>,
}

impl Samples {
    fn new(capacity: usize) -> Self {
        Self {
            construct_drop: Vec::with_capacity(capacity),
            pull_first: Vec::with_capacity(capacity),
            fresh_first: Vec::with_capacity(capacity),
            drop_first: Vec::with_capacity(capacity),
            full: Vec::with_capacity(capacity),
        }
    }

    fn get(&self, cell: Cell) -> &[Duration] {
        match cell {
            Cell::ConstructDrop => &self.construct_drop,
            Cell::PullFirst => &self.pull_first,
            Cell::FreshFirst => &self.fresh_first,
            Cell::DropFirst => &self.drop_first,
            Cell::Full => &self.full,
        }
    }

    fn push(&mut self, cell: Cell, elapsed: Duration) {
        match cell {
            Cell::ConstructDrop => self.construct_drop.push(elapsed),
            Cell::PullFirst => self.pull_first.push(elapsed),
            Cell::FreshFirst => self.fresh_first.push(elapsed),
            Cell::DropFirst => self.drop_first.push(elapsed),
            Cell::Full => self.full.push(elapsed),
        }
    }
}

fn cell_label(cell: Cell) -> &'static str {
    match cell {
        Cell::ConstructDrop => "construct+drop",
        Cell::PullFirst => "prebuilt pull->first",
        Cell::FreshFirst => "fresh end-to-end-1",
        Cell::DropFirst => "fresh drop-at-1",
        Cell::Full => "fresh full drain",
    }
}

fn paired_ratios(numerator: &[Duration], denominator: &[Duration]) -> Vec<f64> {
    assert_eq!(numerator.len(), denominator.len());
    numerator
        .iter()
        .zip(denominator)
        .map(|(numerator, denominator)| {
            numerator.as_secs_f64() / denominator.as_secs_f64().max(f64::MIN_POSITIVE)
        })
        .collect()
}

fn main() {
    let repetitions = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(11usize);
    let equality_count = std::env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(32usize);
    assert!(repetitions > 0);
    assert!(equality_count > 0);

    let fixture = build_fixture(ROWS);
    assert_eq!(fixture.expected.len(), ROWS);

    // Exact preflight is intentionally outside all reported timings. The SET
    // is generated from fixture IDs, not by another query-engine path.
    for mode in MODES {
        let actual: BTreeSet<_> = make_iter(&fixture, equality_count, mode).collect();
        assert_eq!(
            actual, fixture.expected,
            "{}: exact SET oracle mismatch",
            mode.label
        );
    }

    // One hot pass per mode warms query code, graph indexes, and allocator
    // paths without contributing a sample.
    for mode in MODES {
        black_box(measure_construct_drop(&fixture, equality_count, mode));
        let pull = measure_pull_first(&fixture, equality_count, mode);
        validate_measurement(&pull, &fixture, false, mode.label);
        let fresh = measure_fresh_first(&fixture, equality_count, mode);
        validate_measurement(&fresh, &fixture, false, mode.label);
        let (_, dropped) = measure_drop_first(&fixture, equality_count, mode);
        validate_first(dropped, &fixture, mode.label);
        let full = measure_full(&fixture, equality_count, mode);
        validate_measurement(&full, &fixture, true, mode.label);
    }

    let mut samples: [Samples; 3] = std::array::from_fn(|_| Samples::new(repetitions));
    let mut pull_stats: [Option<Measurement>; 3] = std::array::from_fn(|_| None);
    let mut fresh_stats: [Option<Measurement>; 3] = std::array::from_fn(|_| None);
    let mut full_profiles: [Option<Measurement>; 3] = std::array::from_fn(|_| None);

    for repetition in 0..repetitions {
        for cell_offset in 0..CELLS.len() {
            let cell = CELLS[(repetition + cell_offset) % CELLS.len()];
            for mode_offset in 0..MODES.len() {
                let index = (repetition + mode_offset) % MODES.len();
                let mode = MODES[index];
                match cell {
                    Cell::ConstructDrop => samples[index]
                        .push(cell, measure_construct_drop(&fixture, equality_count, mode)),
                    Cell::PullFirst => {
                        let measured = measure_pull_first(&fixture, equality_count, mode);
                        validate_measurement(&measured, &fixture, false, mode.label);
                        samples[index].push(cell, measured.elapsed);
                        pull_stats[index] = Some(measured);
                    }
                    Cell::FreshFirst => {
                        let measured = measure_fresh_first(&fixture, equality_count, mode);
                        validate_measurement(&measured, &fixture, false, mode.label);
                        samples[index].push(cell, measured.elapsed);
                        fresh_stats[index] = Some(measured);
                    }
                    Cell::DropFirst => {
                        let (elapsed, first) = measure_drop_first(&fixture, equality_count, mode);
                        validate_first(first, &fixture, mode.label);
                        samples[index].push(cell, elapsed);
                    }
                    Cell::Full => {
                        let measured = measure_full(&fixture, equality_count, mode);
                        validate_measurement(&measured, &fixture, true, mode.label);
                        samples[index].push(cell, measured.elapsed);
                        full_profiles[index] = Some(measured);
                    }
                }
            }
        }
    }

    println!(
        "production formula regions diagnostic: rows={ROWS} equalities={equality_count} repetitions={repetitions} start_width=1 treatment_order=cell+mode_rotating paired_by_repetition formal_acceptance=false"
    );
    for (index, mode) in MODES.iter().enumerate() {
        println!("  {}", mode.label);
        for cell in CELLS {
            let cell_samples = samples[index].get(cell);
            let p50 = percentile(cell_samples, 0.50);
            let p95 = percentile(cell_samples, 0.95);
            if matches!(cell, Cell::Full) {
                println!(
                    "    {:<22} p50/p95 {:>10.3}/{:>10.3} ms  {:>10.2} q/s  {:>12.0} rows/s  checksum={:#018x}",
                    cell_label(cell),
                    millis(p50),
                    millis(p95),
                    1.0 / p50.as_secs_f64(),
                    ROWS as f64 / p50.as_secs_f64(),
                    fixture.expected_signature.checksum,
                );
            } else {
                println!(
                    "    {:<22} p50/p95 {:>10.3}/{:>10.3} us  {:>10.2} q/s",
                    cell_label(cell),
                    micros(p50),
                    micros(p95),
                    1.0 / p50.as_secs_f64(),
                );
            }
        }
        print_stats("pull", pull_stats[index].as_ref().expect("pull stats"));
        print_stats("e2e1", fresh_stats[index].as_ref().expect("fresh stats"));
        print_stats("full", full_profiles[index].as_ref().expect("full stats"));
    }

    println!("  paired mode/baseline ratios (p50/p95; lower is faster)");
    for index in 1..MODES.len() {
        println!("    {} / {}", MODES[index].label, MODES[0].label);
        for cell in CELLS {
            let ratios = paired_ratios(samples[index].get(cell), samples[0].get(cell));
            println!(
                "      {:<22} {:>8.4}/{:>8.4}x",
                cell_label(cell),
                percentile_f64(&ratios, 0.50),
                percentile_f64(&ratios, 0.95),
            );
        }
    }
    println!(
        "  independent oracle rows={} checksum={:#018x}; timing is diagnostic only (no preregistered A/A epsilon or confidence interval)",
        fixture.expected_signature.rows, fixture.expected_signature.checksum
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_lowering_mode_matches_the_independent_set_oracle() {
        let fixture = build_fixture(257);
        for mode in MODES {
            let actual: BTreeSet<_> = make_iter(&fixture, 5, mode).collect();
            assert_eq!(actual, fixture.expected, "{} SET parity", mode.label);
            assert_eq!(tally(actual), fixture.expected_signature);

            let first = measure_fresh_first(&fixture, 5, mode);
            validate_measurement(&first, &fixture, false, mode.label);
            let full = measure_full(&fixture, 5, mode);
            validate_measurement(&full, &fixture, true, mode.label);
        }
    }
}
