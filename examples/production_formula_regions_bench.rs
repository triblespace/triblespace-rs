//! Preregistered adoption gate for selective Production formula regions.
//!
//! This binary compares the existing opaque-production lowering (`B`) with
//! `ProductionRegions + Production` (`C`) inside one process. It deliberately
//! separates exact semantic and mechanism receipts from five timed cells:
//! construct-and-drop, prebuilt pull-to-first, fresh pull-to-first,
//! fresh drop-at-first, and fresh full drain.
//!
//! Before timing, both modes must match fixture-generated SET oracles. The
//! preflight also checks guarded-region action-count invariance, publication
//! before cyclic EOF, a non-page-local barrier, exact clone/drop remainders,
//! geometric width traces, and identical ordinary-control plans and action
//! traces at both scales. The guarded-region shape and ordinary-control plan
//! identity are enforced by the adjacent private plan tests
//! `production_regions_adoption_gate_keeps_off_path_formula_size_constant`
//! and
//! `production_regions_adoption_gate_ordinary_control_has_identical_plans`;
//! canceled-iterator destruction is separately pinned by
//! `production_regions_adoption_gate_residual_iter_drop_is_structurally_inert`.
//! Keeping those structural assertions inside `residual.rs` avoids adding a
//! benchmark-only public plan-introspection API.
//!
//! Every timed workload runs exactly twenty paired rounds. Odd rounds use
//! `B-C-C-B`; even rounds use `C-B-B-C`. Adjacent rounds form ten
//! order-balanced superblocks in log-ratio space. The binary prints every raw
//! sample, every superblock, and a geometric ratio with its one-sided 95%
//! Student-t upper bound (`df=9`).
//! Raw sample rows carry `stats_observed=false` for construct-and-drop and
//! fresh drop-at-first: those cells stop their clocks only after destruction,
//! so their diagnostic counter columns are deliberately zero sentinels.
//! Guarded small/large and ordinary small/large arms are adjacent within every
//! round, with their scale order reversed on even rounds; the drain-growth
//! contrast therefore uses genuinely paired superblocks.
//!
//! `GO` requires both positive streaming fixtures to have `C/B upper95 < 1`
//! in the three first/drop cells. Guarded full-drain ratios are measured at
//! two power-of-two scales. Their scale growth may not exceed the ordinary
//! plan-identical A/A noise envelope. Absolute guarded drain ratios are always
//! printed and are never hidden by that relative-growth criterion.
//! Concretely, each scale-growth sample is `large_y[j] - small_y[j]`. The
//! guarded one-sided upper log bound is compared with
//! `abs(mean(ordinary_growth)) + t95 * se(ordinary_growth)`; equality passes.
//! This definition is fixed here before any formal timing is observed.
//!
//! Build and semantic preflight (no formal timing):
//!
//! ```text
//! cargo test -p triblespace-core --lib production_regions_adoption_gate
//! cargo test --example production_formula_regions_bench
//! ```
//!
//! Formal run, on an otherwise idle host:
//!
//! ```text
//! cargo run --release --example production_formula_regions_bench
//! ```

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use triblespace::core::id::{ExclusiveId, Id};
use triblespace::core::inline::{Inline, RawInline};
use triblespace::core::query::equalityconstraint::EqualityConstraint;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace::core::query::residual::{
    ActionGeometry, ActionOutcome, ActionSite, ResidualLowering, ResidualShadowEpoch,
    ResidualShadowStatus, ResidualStateIter, ResidualStateStats,
};
use triblespace::core::query::unionconstraint::UnionConstraint;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, ProposalCoverage, Query, RowsView, Variable,
    VariableId, VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

const START: VariableId = 0;
const END: VariableId = 1;
const FORMAL_ROUNDS: usize = 20;
const SUPERBLOCKS: usize = FORMAL_ROUNDS / 2;
const ONE_SIDED_T95_DF9: f64 = 1.833_112_932_653_633_5;
const GUARDED_SMALL: usize = 16_384;
const GUARDED_LARGE: usize = 65_536;
const RECURSIVE_SCALE: usize = 4_096;
const GUARD_WIDTH: usize = 32;
const GUARDED_FORMULA_NODES: usize = 3;

type OwnedConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = Arc<IntersectionConstraint<OwnedConstraint>>;
type BenchIter = ResidualStateIter<Root, fn(&Binding) -> Option<RawInline>, RawInline>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    B,
    C,
}

impl Mode {
    const ALL: [Self; 2] = [Self::B, Self::C];

    fn label(self) -> &'static str {
        match self {
            Self::B => "B",
            Self::C => "C",
        }
    }

    fn lowering(self) -> ResidualLowering {
        match self {
            Self::B => ResidualLowering::OPAQUE_PRODUCTION,
            Self::C => ResidualLowering::PRODUCTION,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Cell {
    ConstructDrop,
    PullFirst,
    FreshFirst,
    DropFirst,
    Full,
}

impl Cell {
    const ALL: [Self; 5] = [
        Self::ConstructDrop,
        Self::PullFirst,
        Self::FreshFirst,
        Self::DropFirst,
        Self::Full,
    ];
    const FIRST_DROP: [Self; 3] = [Self::PullFirst, Self::FreshFirst, Self::DropFirst];

    fn label(self) -> &'static str {
        match self {
            Self::ConstructDrop => "construct_drop",
            Self::PullFirst => "prebuilt_first",
            Self::FreshFirst => "fresh_first",
            Self::DropFirst => "drop_first",
            Self::Full => "full_drain",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::ConstructDrop => 0,
            Self::PullFirst => 1,
            Self::FreshFirst => 2,
            Self::DropFirst => 3,
            Self::Full => 4,
        }
    }
}

fn round_order(round: usize) -> [Mode; 4] {
    assert!((1..=FORMAL_ROUNDS).contains(&round));
    if round % 2 == 1 {
        [Mode::B, Mode::C, Mode::C, Mode::B]
    } else {
        [Mode::C, Mode::B, Mode::B, Mode::C]
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
    let mut signature = Signature::default();
    for row in rows {
        signature.rows += 1;
        signature.checksum = signature.checksum.wrapping_add(row_checksum(&row));
    }
    signature
}

fn raw_hex(value: &RawInline) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        write!(&mut out, "{byte:02x}").expect("writing to a String is infallible");
    }
    out
}

fn fixture_id(domain: u32, index: usize) -> Id {
    let ordinal = u64::try_from(index).expect("fixture ordinal fits u64") + 1;
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&domain.to_be_bytes());
    raw[4..12].copy_from_slice(&ordinal.to_be_bytes());
    raw[12..].copy_from_slice(&(ordinal as u32).rotate_left(13).to_be_bytes());
    Id::new(raw).expect("fixture IDs are non-nil")
}

fn genid_inline(id: &Id) -> Inline<GenId> {
    id.to_inline()
}

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

#[derive(Clone)]
struct CertifiedFilter {
    accepted: Arc<Vec<RawInline>>,
    page_local: bool,
    calls: Arc<Mutex<Vec<usize>>>,
}

impl Constraint<'static> for CertifiedFilter {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(END)
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
        if variable != END {
            return false;
        }
        out.fill(usize::MAX, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        assert_ne!(variable, END, "validation-only filter became proposer");
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, END);
        self.calls
            .lock()
            .expect("filter trace poisoned")
            .push(candidates.len());
        candidates.retain(|_, value| self.accepted.binary_search(value).is_ok());
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(END).is_none_or(|column| {
            view.iter()
                .all(|row| self.accepted.binary_search(&row[column]).is_ok())
        })
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.page_local
    }
}

#[derive(Clone)]
struct ExactDomain {
    values: Arc<Vec<RawInline>>,
}

impl Constraint<'static> for ExactDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(END)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == END && !bound.is_set(END) {
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
        if variable != END {
            return false;
        }
        out.fill(self.values.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == END {
            for row in 0..view.len() {
                candidates.extend_row(row as u32, self.values.iter().copied());
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == END {
            candidates.retain(|_, value| self.values.binary_search(value).is_ok());
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(END).is_none_or(|column| {
            view.iter()
                .all(|row| self.values.binary_search(&row[column]).is_ok())
        })
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

enum FixtureKind {
    GuardedStar {
        graph: TribleSet,
        attribute: Id,
        source: Id,
        guard_width: usize,
    },
    RecursiveFilter {
        graph: TribleSet,
        attribute: Id,
        source: Id,
        prefix: Vec<RawInline>,
        accepted: Arc<Vec<RawInline>>,
        page_local: bool,
    },
    Ordinary {
        source: Id,
        values: Arc<Vec<RawInline>>,
    },
}

struct Fixture {
    label: &'static str,
    scale: usize,
    kind: FixtureKind,
    expected: BTreeSet<RawInline>,
    expected_signature: Signature,
}

impl Fixture {
    fn guarded_star(label: &'static str, scale: usize, guard_width: usize, domain: u32) -> Self {
        assert!(scale > 0);
        assert!(guard_width > 0);
        let attribute = fixture_id(domain, 0);
        let source = fixture_id(domain + 1, 0);
        let mut graph = TribleSet::new();
        let mut expected = BTreeSet::new();
        for index in 0..scale {
            let target = fixture_id(domain + 2, index);
            let value = genid_inline(&target).raw;
            graph.insert(&Trible::new::<GenId>(
                ExclusiveId::force_ref(&source),
                &attribute,
                &Inline::<GenId>::new(value),
            ));
            assert!(expected.insert(value));
        }
        let expected_signature = tally(expected.iter().copied());
        Self {
            label,
            scale,
            kind: FixtureKind::GuardedStar {
                graph,
                attribute,
                source,
                guard_width,
            },
            expected,
            expected_signature,
        }
    }

    fn recursive_filter(
        label: &'static str,
        scale: usize,
        page_local: bool,
        include_prefix: bool,
        domain: u32,
    ) -> Self {
        assert!(scale >= 4);
        let attribute = fixture_id(domain, 0);
        let nodes: Vec<_> = (0..=scale)
            .map(|index| fixture_id(domain + 1, index))
            .collect();
        let mut graph = TribleSet::new();
        for index in 0..scale {
            graph.insert(&Trible::new::<GenId>(
                ExclusiveId::force_ref(&nodes[index]),
                &attribute,
                &genid_inline(&nodes[index + 1]),
            ));
        }

        let mut accepted = vec![
            genid_inline(&nodes[1]).raw,
            genid_inline(&nodes[3]).raw,
            genid_inline(&nodes[scale]).raw,
        ];
        accepted.sort_unstable();
        accepted.dedup();
        let prefix = include_prefix
            .then(|| vec![genid_inline(&nodes[1]).raw])
            .unwrap_or_default();
        let expected: BTreeSet<_> = accepted.iter().copied().collect();
        let expected_signature = tally(expected.iter().copied());
        Self {
            label,
            scale,
            kind: FixtureKind::RecursiveFilter {
                graph,
                attribute,
                source: nodes[0],
                prefix,
                accepted: Arc::new(accepted),
                page_local,
            },
            expected,
            expected_signature,
        }
    }

    fn ordinary(label: &'static str, scale: usize, domain: u32) -> Self {
        assert!(scale > 0);
        let source = fixture_id(domain, 0);
        let mut values: Vec<_> = (0..scale)
            .map(|index| genid_inline(&fixture_id(domain + 1, index)).raw)
            .collect();
        values.sort_unstable();
        let expected: BTreeSet<_> = values.iter().copied().collect();
        let expected_signature = tally(expected.iter().copied());
        Self {
            label,
            scale,
            kind: FixtureKind::Ordinary {
                source,
                values: Arc::new(values),
            },
            expected,
            expected_signature,
        }
    }

    fn root(&self, calls: Option<Arc<Mutex<Vec<usize>>>>) -> Root {
        let start = Variable::<GenId>::new(START);
        let end = Variable::<GenId>::new(END);
        match &self.kind {
            FixtureKind::GuardedStar {
                graph,
                attribute,
                source,
                guard_width,
            } => {
                let path = Box::new(RegularPathConstraint::new(
                    graph.clone(),
                    start,
                    end,
                    &[PathOp::Attr(attribute.raw())],
                )) as OwnedConstraint;
                let mut dead = Vec::with_capacity(*guard_width + 1);
                for _ in 0..*guard_width {
                    dead.push(Box::new(EqualityConstraint::new(START, END)) as OwnedConstraint);
                }
                dead.push(Box::new(FalseConstraint) as OwnedConstraint);
                Arc::new(IntersectionConstraint::new(vec![
                    Box::new(start.is(source.to_inline())) as OwnedConstraint,
                    Box::new(UnionConstraint::new(vec![
                        Box::new(IntersectionConstraint::new(dead)) as OwnedConstraint,
                        path,
                    ])) as OwnedConstraint,
                ]))
            }
            FixtureKind::RecursiveFilter {
                graph,
                attribute,
                source,
                prefix,
                accepted,
                page_local,
            } => {
                let cyclic = Box::new(RegularPathConstraint::new(
                    graph.clone(),
                    start,
                    end,
                    &[PathOp::Attr(attribute.raw()), PathOp::Plus],
                )) as OwnedConstraint;
                let mut arms: Vec<OwnedConstraint> = prefix
                    .iter()
                    .copied()
                    .map(|value| {
                        Box::new(IntersectionConstraint::new(vec![
                            Box::new(start.is(source.to_inline())) as OwnedConstraint,
                            Box::new(end.is(Inline::<GenId>::new(value))) as OwnedConstraint,
                        ])) as OwnedConstraint
                    })
                    .collect();
                arms.push(cyclic);
                Arc::new(IntersectionConstraint::new(vec![
                    Box::new(start.is(source.to_inline())) as OwnedConstraint,
                    Box::new(UnionConstraint::new(arms)) as OwnedConstraint,
                    Box::new(CertifiedFilter {
                        accepted: Arc::clone(accepted),
                        page_local: *page_local,
                        calls: calls.unwrap_or_default(),
                    }) as OwnedConstraint,
                ]))
            }
            FixtureKind::Ordinary { source, values } => {
                Arc::new(IntersectionConstraint::new(vec![
                    Box::new(start.is(source.to_inline())) as OwnedConstraint,
                    Box::new(ExactDomain {
                        values: Arc::clone(values),
                    }) as OwnedConstraint,
                ]))
            }
        }
    }

    fn make_iter(&self, mode: Mode) -> BenchIter {
        self.make_iter_with_calls(mode, None)
    }

    fn make_iter_with_calls(&self, mode: Mode, calls: Option<Arc<Mutex<Vec<usize>>>>) -> BenchIter {
        Query::new(
            self.root(calls),
            project_end as fn(&Binding) -> Option<RawInline>,
        )
        .solve_residual_state_lazy_with(mode.lowering())
        .cap(self.scale.max(1))
        .start_width(1)
    }
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

struct Measurement {
    elapsed: Duration,
    signature: Signature,
    first: Option<RawInline>,
    stats_observed: bool,
    stats: ResidualStateStats,
    final_width: usize,
}

fn first_measurement(mut query: BenchIter, started: Instant) -> Measurement {
    let first = black_box(query.next());
    let elapsed = started.elapsed();
    Measurement {
        elapsed,
        signature: tally(first),
        first,
        stats_observed: true,
        stats: query.stats().clone(),
        final_width: query.current_width(),
    }
}

fn run_cell(fixture: &Fixture, cell: Cell, mode: Mode) -> Measurement {
    match cell {
        Cell::ConstructDrop => {
            let started = Instant::now();
            let query = black_box(fixture.make_iter(mode));
            drop(query);
            let elapsed = started.elapsed();
            Measurement {
                elapsed,
                signature: Signature::default(),
                first: None,
                stats_observed: false,
                stats: ResidualStateStats::default(),
                final_width: 0,
            }
        }
        Cell::PullFirst => {
            let query = fixture.make_iter(mode);
            first_measurement(query, Instant::now())
        }
        Cell::FreshFirst => {
            let started = Instant::now();
            first_measurement(fixture.make_iter(mode), started)
        }
        Cell::DropFirst => {
            let started = Instant::now();
            let mut query = fixture.make_iter(mode);
            let first = black_box(query.next());
            drop(query);
            let elapsed = started.elapsed();
            Measurement {
                elapsed,
                signature: tally(first),
                first,
                stats_observed: false,
                stats: ResidualStateStats::default(),
                final_width: 0,
            }
        }
        Cell::Full => {
            let started = Instant::now();
            let mut query = fixture.make_iter(mode);
            let signature = black_box(tally(query.by_ref()));
            let elapsed = started.elapsed();
            Measurement {
                elapsed,
                signature,
                first: None,
                stats_observed: true,
                stats: query.stats().clone(),
                final_width: query.current_width(),
            }
        }
    }
}

fn validate_measurement(fixture: &Fixture, cell: Cell, measurement: &Measurement) {
    match cell {
        Cell::ConstructDrop => assert_eq!(measurement.signature.rows, 0),
        Cell::PullFirst | Cell::FreshFirst | Cell::DropFirst => {
            assert_eq!(
                measurement.signature.rows, 1,
                "{} no first row",
                fixture.label
            );
            assert!(
                fixture
                    .expected
                    .contains(measurement.first.as_ref().expect("one first row")),
                "{} first row is outside the independent oracle",
                fixture.label
            );
        }
        Cell::Full => assert_eq!(
            measurement.signature, fixture.expected_signature,
            "{} full signature mismatch",
            fixture.label
        ),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActionTrace {
    site: ActionSite,
    geometry: ActionGeometry,
    outcome: ActionOutcome,
}

fn ordinary_trace(
    fixture: &Fixture,
    mode: Mode,
) -> (Vec<RawInline>, Vec<ActionTrace>, ResidualStateStats) {
    let epoch = ResidualShadowEpoch::new();
    let solve = fixture.make_iter(mode).shadow(epoch).collect_profiled();
    assert_eq!(solve.shadow.status, ResidualShadowStatus::Closed);
    let trace = solve
        .shadow
        .events
        .into_iter()
        .map(|event| ActionTrace {
            site: event.site,
            geometry: event.geometry,
            outcome: event
                .completion
                .expect("closed trace action has completion")
                .outcome,
        })
        .collect();
    (solve.results, trace, solve.stats)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FirstActionCounts {
    support_actions: usize,
    propose_actions: usize,
    confirm_actions: usize,
    support_calls: usize,
    propose_calls: usize,
    confirm_calls: usize,
}

impl From<&ResidualStateStats> for FirstActionCounts {
    fn from(stats: &ResidualStateStats) -> Self {
        Self {
            support_actions: stats.support_action_pops,
            propose_actions: stats.propose_action_pops,
            confirm_actions: stats.confirm_action_pops,
            support_calls: stats.support_calls,
            propose_calls: stats.propose_calls,
            confirm_calls: stats.confirm_calls,
        }
    }
}

fn record_width_point(trace: &mut Vec<(usize, usize)>, query: &BenchIter) {
    let point = (query.current_width(), query.stats().width_increases);
    if trace.last().copied() != Some(point) {
        trace.push(point);
    }
}

fn check_exact_clone_drop_and_geometry(fixture: &Fixture) -> Vec<(usize, usize)> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut query = fixture.make_iter_with_calls(Mode::C, Some(Arc::clone(&calls)));
    let mut widths = Vec::new();
    record_width_point(&mut widths, &query);
    let first = query.next().expect("positive fixture has a first row");
    record_width_point(&mut widths, &query);
    let clone_start = widths.len() - 1;
    let mut exact_clone = query.clone();
    let cancelled = query.clone();
    assert_eq!(query.stats(), exact_clone.stats());
    assert_eq!(query.current_width(), exact_clone.current_width());
    let survivor_stats = query.stats().clone();
    let cloned_stats = exact_clone.stats().clone();
    let calls_before_drop = calls.lock().expect("filter trace poisoned").len();
    drop(cancelled);
    assert_eq!(query.stats(), &survivor_stats, "drop changed survivor work");
    assert_eq!(
        exact_clone.stats(),
        &cloned_stats,
        "drop changed cloned survivor work"
    );
    assert_eq!(
        calls.lock().expect("filter trace poisoned").len(),
        calls_before_drop,
        "dropping a clone performed constraint work"
    );

    let mut original_remainder = Vec::new();
    while let Some(row) = query.next() {
        original_remainder.push(row);
        record_width_point(&mut widths, &query);
    }
    record_width_point(&mut widths, &query);
    let mut cloned_widths = Vec::new();
    record_width_point(&mut cloned_widths, &exact_clone);
    let mut cloned_remainder = Vec::new();
    while let Some(row) = exact_clone.next() {
        cloned_remainder.push(row);
        record_width_point(&mut cloned_widths, &exact_clone);
    }
    record_width_point(&mut cloned_widths, &exact_clone);
    assert_eq!(
        original_remainder, cloned_remainder,
        "exact cloned remainder"
    );
    assert_eq!(
        &widths[clone_start..],
        cloned_widths,
        "exact clone did not preserve geometric continuation state"
    );

    let mut complete = vec![first];
    complete.extend(original_remainder);
    assert_eq!(
        complete.len(),
        fixture.expected.len(),
        "clone/drop trace changed raw result cardinality"
    );
    assert_eq!(
        complete.iter().copied().collect::<BTreeSet<_>>(),
        fixture.expected,
        "clone/drop trace changed the exact SET"
    );
    assert!(
        widths.iter().all(|(width, _)| width.is_power_of_two()),
        "formal fixture caps are powers of two"
    );
    assert!(widths.len() > 1, "geometric growth receipt is vacuous");
    assert!(
        widths.last().expect("nonempty width trace").1 > 0,
        "the positive fixture never widened"
    );
    assert_eq!(
        widths.last().expect("nonempty width trace").0,
        fixture.scale,
        "a fully drained power-of-two fixture did not reach its exact width cap"
    );
    for pair in widths.windows(2) {
        let (previous_width, previous_increases) = pair[0];
        let (width, increases) = pair[1];
        assert!(increases > previous_increases);
        let mut expected = previous_width;
        for _ in previous_increases..increases {
            expected = expected.saturating_mul(2).min(fixture.scale);
        }
        assert_eq!(
            width, expected,
            "geometric trace skipped a doubling or its exact cap"
        );
    }
    widths
}

fn print_mechanism(name: &str, detail: impl std::fmt::Display) {
    println!("mechanism\t{name}\tPASS\t{detail}");
}

fn semantic_and_mechanism_preflight(fixtures: &[Fixture]) {
    for fixture in fixtures {
        for mode in Mode::ALL {
            let raw: Vec<_> = fixture.make_iter(mode).collect();
            assert_eq!(
                raw.len(),
                fixture.expected.len(),
                "{} {} raw result cardinality",
                fixture.label,
                mode.label()
            );
            let actual: BTreeSet<_> = raw.into_iter().collect();
            assert_eq!(
                actual,
                fixture.expected,
                "{} {} independent SET oracle",
                fixture.label,
                mode.label()
            );
            println!(
                "oracle\t{}\t{}\t{}\t{}\t{:#018x}\tPASS",
                fixture.label,
                fixture.scale,
                mode.label(),
                fixture.expected_signature.rows,
                fixture.expected_signature.checksum
            );
        }
    }

    // The public action receipt complements the private structural plan test:
    // changing fanout or the opaque dead-arm width must not change the work
    // required to reach the first live Program page.
    let narrow = Fixture::guarded_star("guarded_probe_narrow", 64, 1, 70);
    let wide = Fixture::guarded_star("guarded_probe_wide", 256, 64, 80);
    let narrow_first = run_cell(&narrow, Cell::PullFirst, Mode::C);
    let wide_first = run_cell(&wide, Cell::PullFirst, Mode::C);
    validate_measurement(&narrow, Cell::PullFirst, &narrow_first);
    validate_measurement(&wide, Cell::PullFirst, &wide_first);
    let narrow_counts = FirstActionCounts::from(&narrow_first.stats);
    let wide_counts = FirstActionCounts::from(&wide_first.stats);
    assert_eq!(narrow_counts, wide_counts);
    assert_eq!(narrow_first.stats.delta_transition_pages, 1);
    assert_eq!(wide_first.stats.delta_transition_pages, 1);
    assert!(narrow_first.stats.delta_transition_candidates_examined < narrow.scale);
    assert!(wide_first.stats.delta_transition_candidates_examined < wide.scale);
    assert_eq!(
        narrow_first.stats.delta_transition_candidates_examined,
        wide_first.stats.delta_transition_candidates_examined,
        "first-page work must be independent of total guarded fanout"
    );
    print_mechanism(
        "guarded_region_shape",
        format_args!(
            "formula_nodes={GUARDED_FORMULA_NODES};private_test=production_regions_adoption_gate_keeps_off_path_formula_size_constant;counts={narrow_counts:?};examined={}",
            narrow_first.stats.delta_transition_candidates_examined
        ),
    );

    for guarded in fixtures
        .iter()
        .filter(|fixture| matches!(fixture.label, "guarded_small" | "guarded_large"))
    {
        let full = run_cell(guarded, Cell::Full, Mode::C);
        validate_measurement(guarded, Cell::Full, &full);
        assert_eq!(
            full.stats.delta_transition_candidates_examined, guarded.scale,
            "full guarded work must follow result cardinality"
        );
        print_mechanism(
            "guarded_full_cardinality",
            format_args!(
                "fixture={};examined={};rows={}",
                guarded.label, full.stats.delta_transition_candidates_examined, guarded.scale
            ),
        );
    }

    let streaming = fixtures
        .iter()
        .find(|fixture| fixture.label == "streaming_filter")
        .expect("streaming fixture");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut query = streaming.make_iter_with_calls(Mode::C, Some(Arc::clone(&calls)));
    let first = query.next().expect("streaming fixture has a result");
    assert!(streaming.expected.contains(&first));
    assert_eq!(query.stats().delta_transition_pages, 1);
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    assert_eq!(*calls.lock().expect("filter trace poisoned"), [1]);
    print_mechanism(
        "positive_pre_eof_publication",
        format_args!(
            "fixture={};examined=1;total={};filter_pages=1",
            streaming.label, streaming.scale
        ),
    );

    let barrier = fixtures
        .iter()
        .find(|fixture| fixture.label == "barrier_control")
        .expect("barrier fixture");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut query = barrier.make_iter_with_calls(Mode::C, Some(Arc::clone(&calls)));
    let first = query.next().expect("barrier fixture has a result");
    assert!(barrier.expected.contains(&first));
    assert_eq!(
        query.stats().delta_transition_candidates_examined,
        barrier.scale,
        "non-page-local barrier published before cyclic EOF"
    );
    assert!(!calls.lock().expect("filter trace poisoned").is_empty());
    print_mechanism(
        "barrier_quiescence",
        format_args!(
            "fixture={};examined={};eof={}",
            barrier.label, barrier.scale, barrier.scale
        ),
    );

    for ordinary in fixtures
        .iter()
        .filter(|fixture| matches!(fixture.label, "ordinary_small" | "ordinary_large"))
    {
        let (b_results, b_trace, b_stats) = ordinary_trace(ordinary, Mode::B);
        let (c_results, c_trace, c_stats) = ordinary_trace(ordinary, Mode::C);
        assert_eq!(b_results, c_results);
        assert_eq!(b_trace, c_trace, "ordinary B/C action traces diverged");
        assert_eq!(b_stats, c_stats, "ordinary B/C scheduler stats diverged");
        print_mechanism(
            "ordinary_plan_action_identity",
            format_args!(
                "fixture={};scale={};events={};stats_equal=true;private_test=production_regions_adoption_gate_ordinary_control_has_identical_plans",
                ordinary.label,
                ordinary.scale,
                b_trace.len()
            ),
        );
    }

    for fixture in fixtures
        .iter()
        .filter(|fixture| matches!(fixture.label, "guarded_large" | "streaming_filter"))
    {
        let widths = check_exact_clone_drop_and_geometry(fixture);
        let trace = widths
            .iter()
            .map(|(width, increases)| format!("{width}@{increases}"))
            .collect::<Vec<_>>()
            .join(",");
        print_mechanism(
            "clone_drop_geometric_trace",
            format_args!(
                "fixture={};widths={trace};exact_remainder=true;private_test=production_regions_adoption_gate_residual_iter_drop_is_structurally_inert",
                fixture.label
            ),
        );
    }
}

fn warm(fixtures: &[Fixture]) {
    for fixture in fixtures {
        for cell in Cell::ALL {
            for mode in Mode::ALL {
                let measured = black_box(run_cell(fixture, cell, mode));
                validate_measurement(fixture, cell, &measured);
            }
        }
    }
}

struct RoundSample {
    b: [f64; 2],
    c: [f64; 2],
}

struct TimingSeries {
    fixture: &'static str,
    scale: usize,
    cell: Cell,
    rounds: Vec<RoundSample>,
}

fn print_sample(
    fixture: &Fixture,
    cell: Cell,
    round: usize,
    arm: usize,
    mode: Mode,
    measured: &Measurement,
) {
    let stats = &measured.stats;
    let first = measured
        .first
        .as_ref()
        .map(raw_hex)
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "sample\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:#018x}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        fixture.label,
        fixture.scale,
        cell.label(),
        round,
        arm,
        mode.label(),
        measured.elapsed.as_nanos(),
        measured.signature.rows,
        measured.signature.checksum,
        first,
        measured.final_width,
        measured.stats_observed,
        stats.support_action_pops,
        stats.propose_action_pops,
        stats.confirm_action_pops,
        stats.support_calls,
        stats.propose_calls,
        stats.confirm_calls,
        stats.delta_source_pages,
        stats.delta_transition_pages,
        stats.delta_source_candidates_examined,
        stats.delta_transition_candidates_examined,
        stats.width_increases,
        stats.terminal_demand_windows_opened,
        stats.terminal_demand_width_promotions,
        stats.terminal_demand_projected_rows,
    );
}

fn measure_series(fixtures: &[Fixture]) -> Vec<TimingSeries> {
    println!(
        "columns\tsample\tfixture\tscale\tcell\tround\tarm\tmode\telapsed_ns\trows\tchecksum\tfirst_hex\tfinal_width\tstats_observed\tsupport_actions\tpropose_actions\tconfirm_actions\tsupport_calls\tpropose_calls\tconfirm_calls\tsource_pages\ttransition_pages\tsource_examined\ttransition_examined\twidth_increases\tterminal_windows\tterminal_promotions\tprojected_rows"
    );
    assert_eq!(
        fixtures
            .iter()
            .map(|fixture| fixture.label)
            .collect::<Vec<_>>(),
        [
            "guarded_small",
            "guarded_large",
            "streaming_filter",
            "barrier_control",
            "ordinary_small",
            "ordinary_large",
        ],
        "fixture order is part of the scale-pairing protocol"
    );
    let mut series = fixtures
        .iter()
        .flat_map(|fixture| {
            Cell::ALL.map(|cell| TimingSeries {
                fixture: fixture.label,
                scale: fixture.scale,
                cell,
                rounds: Vec::with_capacity(FORMAL_ROUNDS),
            })
        })
        .collect::<Vec<_>>();

    for cell in Cell::ALL {
        for round in 1..=FORMAL_ROUNDS {
            // The two guarded scales and the two ordinary A/A scales are
            // adjacent inside every logical round. Their order reverses on
            // even rounds, so same-index superblock subtraction is a real
            // temporally paired contrast rather than a comparison of distant
            // fixture blocks. The two non-scale fixtures reverse as well.
            let fixture_order = if round % 2 == 1 {
                [0, 1, 2, 3, 4, 5]
            } else {
                [1, 0, 3, 2, 5, 4]
            };
            for fixture_index in fixture_order {
                let fixture = &fixtures[fixture_index];
                let mut b = Vec::with_capacity(2);
                let mut c = Vec::with_capacity(2);
                for (arm, mode) in round_order(round).into_iter().enumerate() {
                    let measured = run_cell(fixture, cell, mode);
                    validate_measurement(fixture, cell, &measured);
                    print_sample(fixture, cell, round, arm + 1, mode, &measured);
                    let seconds = measured.elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
                    match mode {
                        Mode::B => b.push(seconds),
                        Mode::C => c.push(seconds),
                    }
                }
                let index = fixture_index * Cell::ALL.len() + cell.index();
                series[index].rounds.push(RoundSample {
                    b: b.try_into().expect("two B arms"),
                    c: c.try_into().expect("two C arms"),
                });
            }
        }
    }
    assert!(
        series
            .iter()
            .all(|series| series.rounds.len() == FORMAL_ROUNDS),
        "every workload receives exactly twenty paired rounds"
    );
    series
}

#[derive(Clone)]
struct Analysis {
    fixture: &'static str,
    scale: usize,
    cell: Cell,
    superblocks: [f64; SUPERBLOCKS],
    geometric_ratio: f64,
    upper95: f64,
}

fn sample_mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn sample_stddev(values: &[f64], mean: f64) -> f64 {
    assert!(values.len() > 1);
    (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64)
        .sqrt()
}

fn upper95_log(values: &[f64]) -> (f64, f64) {
    assert_eq!(values.len(), SUPERBLOCKS);
    let mean = sample_mean(values);
    let standard_error = sample_stddev(values, mean) / (values.len() as f64).sqrt();
    (mean, mean + ONE_SIDED_T95_DF9 * standard_error)
}

fn analyze(series: TimingSeries) -> Analysis {
    assert_eq!(series.rounds.len(), FORMAL_ROUNDS);
    let round_ratios: Vec<_> = series
        .rounds
        .iter()
        .map(|round| {
            (round.c.iter().map(|value| value.ln()).sum::<f64>()
                - round.b.iter().map(|value| value.ln()).sum::<f64>())
                / 2.0
        })
        .collect();
    let superblocks: [f64; SUPERBLOCKS] = round_ratios
        .chunks_exact(2)
        .map(|pair| (pair[0] + pair[1]) / 2.0)
        .collect::<Vec<_>>()
        .try_into()
        .expect("ten superblocks");
    let (mean, upper) = upper95_log(&superblocks);
    Analysis {
        fixture: series.fixture,
        scale: series.scale,
        cell: series.cell,
        superblocks,
        geometric_ratio: mean.exp(),
        upper95: upper.exp(),
    }
}

fn emit_analysis(analysis: &Analysis) {
    for (index, &log_ratio) in analysis.superblocks.iter().enumerate() {
        println!(
            "superblock\t{}\t{}\t{}\t{}\t{:.12}\t{:.12}",
            analysis.fixture,
            analysis.scale,
            analysis.cell.label(),
            index + 1,
            log_ratio,
            log_ratio.exp(),
        );
    }
    println!(
        "summary\t{}\t{}\t{}\t{:.12}\t{:.12}",
        analysis.fixture,
        analysis.scale,
        analysis.cell.label(),
        analysis.geometric_ratio,
        analysis.upper95,
    );
}

fn lookup<'a>(analyses: &'a [Analysis], fixture: &str, scale: usize, cell: Cell) -> &'a Analysis {
    analyses
        .iter()
        .find(|analysis| {
            analysis.fixture == fixture && analysis.scale == scale && analysis.cell == cell
        })
        .expect("registered analysis")
}

fn decide(analyses: &[Analysis]) -> bool {
    let mut go = true;
    for (fixture, scale) in [
        ("guarded_large", GUARDED_LARGE),
        ("streaming_filter", RECURSIVE_SCALE),
    ] {
        for cell in Cell::FIRST_DROP {
            let analysis = lookup(analyses, fixture, scale, cell);
            let pass = analysis.upper95 < 1.0;
            go &= pass;
            println!(
                "decision\tstreaming_upper95\t{}\t{}\t{}\t{:.12}\t{}",
                fixture,
                scale,
                cell.label(),
                analysis.upper95,
                if pass { "PASS" } else { "STOP" },
            );
        }
    }

    let guarded_small = lookup(analyses, "guarded_small", GUARDED_SMALL, Cell::Full);
    let guarded_large = lookup(analyses, "guarded_large", GUARDED_LARGE, Cell::Full);
    let ordinary_small = lookup(analyses, "ordinary_small", GUARDED_SMALL, Cell::Full);
    let ordinary_large = lookup(analyses, "ordinary_large", GUARDED_LARGE, Cell::Full);
    println!(
        "drain_absolute\tguarded_small\t{}\t{:.12}\t{:.12}",
        GUARDED_SMALL, guarded_small.geometric_ratio, guarded_small.upper95
    );
    println!(
        "drain_absolute\tguarded_large\t{}\t{:.12}\t{:.12}",
        GUARDED_LARGE, guarded_large.geometric_ratio, guarded_large.upper95
    );

    let guarded_growth: Vec<_> = guarded_large
        .superblocks
        .iter()
        .zip(guarded_small.superblocks)
        .map(|(large, small)| large - small)
        .collect();
    let ordinary_growth: Vec<_> = ordinary_large
        .superblocks
        .iter()
        .zip(ordinary_small.superblocks)
        .map(|(large, small)| large - small)
        .collect();
    let (_, guarded_growth_upper_log) = upper95_log(&guarded_growth);
    let ordinary_mean = sample_mean(&ordinary_growth);
    let ordinary_noise_log = ordinary_mean.abs()
        + ONE_SIDED_T95_DF9 * sample_stddev(&ordinary_growth, ordinary_mean)
            / (ordinary_growth.len() as f64).sqrt();
    let guarded_growth_upper = guarded_growth_upper_log.exp();
    let ordinary_noise_envelope = ordinary_noise_log.exp();
    let pass = guarded_growth_upper <= ordinary_noise_envelope;
    go &= pass;
    println!(
        "decision\tdrain_scale_growth\tguarded_upper95={:.12}\tordinary_aa_envelope={:.12}\t{}",
        guarded_growth_upper,
        ordinary_noise_envelope,
        if pass { "PASS" } else { "STOP" },
    );
    go
}

fn formal_fixtures() -> Vec<Fixture> {
    assert!(GUARDED_SMALL.is_power_of_two());
    assert!(GUARDED_LARGE.is_power_of_two());
    assert!(GUARDED_SMALL < GUARDED_LARGE);
    vec![
        Fixture::guarded_star("guarded_small", GUARDED_SMALL, GUARD_WIDTH, 100),
        Fixture::guarded_star("guarded_large", GUARDED_LARGE, GUARD_WIDTH, 110),
        Fixture::recursive_filter("streaming_filter", RECURSIVE_SCALE, true, true, 120),
        Fixture::recursive_filter("barrier_control", RECURSIVE_SCALE, false, false, 130),
        Fixture::ordinary("ordinary_small", GUARDED_SMALL, 140),
        Fixture::ordinary("ordinary_large", GUARDED_LARGE, 150),
    ]
}

fn main() {
    let fixtures = formal_fixtures();
    println!(
        "meta\tformat=production-regions-adoption-gate-v2\trounds={FORMAL_ROUNDS}\tsuperblocks={SUPERBLOCKS}\todd=B-C-C-B\teven=C-B-B-C\tt95_df9={ONE_SIDED_T95_DF9}\tguarded_formula_nodes={GUARDED_FORMULA_NODES}"
    );
    semantic_and_mechanism_preflight(&fixtures);
    warm(&fixtures);
    let analyses: Vec<_> = measure_series(&fixtures).into_iter().map(analyze).collect();
    for analysis in &analyses {
        emit_analysis(analysis);
    }
    let go = decide(&analyses);
    println!("verdict\t{}", if go { "GO" } else { "STOP" });
    if !go {
        std::process::exit(3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_order_is_exactly_preregistered() {
        assert_eq!(round_order(1), [Mode::B, Mode::C, Mode::C, Mode::B]);
        assert_eq!(round_order(2), [Mode::C, Mode::B, Mode::B, Mode::C]);
        assert_eq!(round_order(19), [Mode::B, Mode::C, Mode::C, Mode::B]);
        assert_eq!(round_order(20), [Mode::C, Mode::B, Mode::B, Mode::C]);
    }

    #[test]
    fn log_superblocks_and_upper_bound_are_exact() {
        let rounds = (0..FORMAL_ROUNDS)
            .map(|_| RoundSample {
                b: [2.0, 8.0],
                c: [1.0, 4.0],
            })
            .collect();
        let analysis = analyze(TimingSeries {
            fixture: "synthetic",
            scale: 1,
            cell: Cell::Full,
            rounds,
        });
        assert!((analysis.geometric_ratio - 0.5).abs() < 1e-12);
        assert!((analysis.upper95 - 0.5).abs() < 1e-12);
        assert!(analysis
            .superblocks
            .iter()
            .all(|value| (*value - 0.5f64.ln()).abs() < 1e-12));
    }

    #[test]
    fn reduced_fixtures_match_oracles_and_mechanism_contracts() {
        let fixtures = vec![
            Fixture::guarded_star("guarded_small", 32, 5, 200),
            Fixture::guarded_star("guarded_large", 64, 5, 210),
            Fixture::recursive_filter("streaming_filter", 32, true, true, 220),
            Fixture::recursive_filter("barrier_control", 32, false, false, 230),
            Fixture::ordinary("ordinary_small", 32, 240),
            Fixture::ordinary("ordinary_large", 64, 250),
        ];
        semantic_and_mechanism_preflight(&fixtures);
    }

    #[test]
    fn drop_cell_samples_mark_counters_as_unobserved() {
        let fixture = Fixture::ordinary("ordinary_observation_boundary", 8, 260);
        for mode in Mode::ALL {
            for cell in [Cell::ConstructDrop, Cell::DropFirst] {
                let measured = run_cell(&fixture, cell, mode);
                validate_measurement(&fixture, cell, &measured);
                assert!(!measured.stats_observed);
                assert_eq!(measured.stats, ResidualStateStats::default());
                assert_eq!(measured.final_width, 0);
            }
            for cell in [Cell::PullFirst, Cell::FreshFirst, Cell::Full] {
                let measured = run_cell(&fixture, cell, mode);
                validate_measurement(&fixture, cell, &measured);
                assert!(measured.stats_observed);
            }
        }
    }
}
