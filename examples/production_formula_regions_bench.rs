//! Preregistered V3 held-out adoption gate for selective Production formula
//! regions.
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
//! `B-C-C-B`; even rounds use `C-B-B-C`. The odd fixture order is guarded
//! small, guarded large, recursive streaming, non-page-local barrier, its
//! matched ordinary A/A control, ordinary small, ordinary large; even rounds
//! use the exact reverse. Adjacent rounds form ten order-balanced superblocks.
//! The binary prints every raw sample, every superblock, every point ratio,
//! bound, envelope, individual decision, and overall verdict.
//! Raw sample rows carry `stats_observed=false` for construct-and-drop and
//! fresh drop-at-first: those cells stop their clocks only after destruction,
//! so their diagnostic counter columns are deliberately zero sentinels.
//! Guarded small/large, barrier/A-A, and ordinary small/large arms are adjacent
//! within every round, with their pair order reversed on even rounds.
//!
//! `GO` requires both positive streaming fixtures to have `C/B upper95 < 1`
//! in the three first/drop cells and guarded-large full drain to have
//! `C/B upper95 < 1`. Guarded marginal drain cost is the ratio of paired
//! arithmetic time increments between the two formal scales; its one-sided
//! upper bound may not exceed the corresponding plan-identical A/A envelope.
//! Every scale increment must be positive. The non-page-local barrier's four
//! execution cells may likewise not exceed their matched ordinary A/A noise
//! envelopes. Absolute guarded drain ratios remain mandatory disclosure, not
//! an added percentage cap.
//! Here "matched" means an ordinary no-region fixture at the barrier's scale,
//! timed adjacent to it with reversed pair order; the barrier envelope uses
//! that fixture's ordinary point log-ratio superblocks. A nonpositive B or C
//! increment in either the guarded or ordinary marginal pair forces `STOP`.
//!
//! Warmup remains V2 exactly and is outside timed ordering: visit fixtures in
//! their stored order, then cells in construct/prebuilt/fresh/drop/full order,
//! and run B once followed by C once. Formal timing uses that same outer cell
//! order before applying the round and fixture reversals described above.
//!
//! The two formal guarded scales are complemented by a diagnostic-only phase
//! profile on at least four non-held-out powers of two (recommended `2^12`,
//! `2^14`, `2^16`, and `2^18`) and separate domains. That profile may guide
//! implementation before candidate freeze but never contributes to this
//! verdict and must not use the reserved formal pairs below.
//!
//! Freeze this protocol, the candidate commit, and the release-binary hash
//! before formal timing. Exactly one formal invocation is allowed on an idle,
//! plugged-in host. Preserve its complete raw output regardless of verdict.
//! There is no peeking, adaptive stopping, extension, outlier deletion, or
//! rerun for noise or `STOP`. A host failure before the first sample may be
//! rescheduled. After the first sample, interruption leaves the candidate
//! unadmitted and forbids a same-candidate retry; another attempt needs a new
//! preregistration and a materially changed candidate. No revised rule
//! retroactively reinterprets an earlier receipt.
//!
//! Build and semantic preflight (no formal timing):
//!
//! ```text
//! cargo test -p triblespace-core --lib production_regions_adoption_gate
//! cargo test --example production_formula_regions_bench
//! ```
//!
//! Formal run, once, on an otherwise idle host with stdout redirected so the
//! complete receipt is not inspected adaptively:
//!
//! ```text
//! cargo build --release --example production_formula_regions_bench
//! shasum -a 256 target/release/examples/production_formula_regions_bench
//! target/release/examples/production_formula_regions_bench > "$FROZEN_RECEIPT" 2>&1
//! ```

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::hint::black_box;
use std::panic::{catch_unwind, AssertUnwindSafe};
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
const PROTOCOL_VERSION: &str = "BE6594AF7634F74ACD46A83CF6E67283";
const FORMAL_ROUNDS: usize = 20;
const SUPERBLOCKS: usize = FORMAL_ROUNDS / 2;
const ONE_SIDED_T95_DF9: f64 = 1.833_112_932_653_633_5;
const GUARDED_SMALL: usize = 1 << 15;
const GUARDED_LARGE: usize = 1 << 17;
const STREAMING_SCALE: usize = 1 << 11;
const BARRIER_SCALE: usize = 1 << 13;
const GUARD_WIDTH: usize = 32;
const GUARDED_FORMULA_NODES: usize = 3;
const GUARDED_SMALL_DOMAIN: u32 = 300;
const GUARDED_LARGE_DOMAIN: u32 = 310;
const STREAMING_DOMAIN: u32 = 320;
const BARRIER_DOMAIN: u32 = 330;
const ORDINARY_BARRIER_DOMAIN: u32 = 340;
const ORDINARY_SMALL_DOMAIN: u32 = 350;
const ORDINARY_LARGE_DOMAIN: u32 = 360;
const FORMAL_LABELS: [&str; 7] = [
    "guarded_small",
    "guarded_large",
    "streaming_filter",
    "barrier_control",
    "ordinary_barrier",
    "ordinary_small",
    "ordinary_large",
];

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
    const BARRIER_GUARD: [Self; 4] = [
        Self::PullFirst,
        Self::FreshFirst,
        Self::DropFirst,
        Self::Full,
    ];

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

fn fixture_order(round: usize) -> [usize; 7] {
    assert!((1..=FORMAL_ROUNDS).contains(&round));
    if round % 2 == 1 {
        [0, 1, 2, 3, 4, 5, 6]
    } else {
        [6, 5, 4, 3, 2, 1, 0]
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
        .filter(|fixture| matches!(&fixture.kind, FixtureKind::Ordinary { .. }))
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

#[derive(Clone, Copy, Debug, PartialEq)]
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
        FORMAL_LABELS,
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
            // The exact fixture reversal keeps both scale pairs and the
            // barrier/A-A pair adjacent while reversing their temporal order
            // in the even half of every superblock.
            for fixture_index in fixture_order(round) {
                let fixture = &fixtures[fixture_index];
                let mut b = Vec::with_capacity(2);
                let mut c = Vec::with_capacity(2);
                for (arm, mode) in round_order(round).into_iter().enumerate() {
                    let measured = run_cell(fixture, cell, mode);
                    validate_measurement(fixture, cell, &measured);
                    print_sample(fixture, cell, round, arm + 1, mode, &measured);
                    // Zero is retained as invalid evidence rather than
                    // silently clamped: a completed nonpositive/nonfinite
                    // sample must force STOP under the frozen protocol.
                    let seconds = measured.elapsed.as_secs_f64();
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
    valid: bool,
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

fn analyze(series: &TimingSeries) -> Analysis {
    assert_eq!(series.rounds.len(), FORMAL_ROUNDS);
    let round_ratios: Vec<_> = series
        .rounds
        .iter()
        .map(|round| {
            let valid = round
                .b
                .iter()
                .chain(&round.c)
                .all(|value| value.is_finite() && *value > 0.0);
            if valid {
                (round.c.iter().map(|value| value.ln()).sum::<f64>()
                    - round.b.iter().map(|value| value.ln()).sum::<f64>())
                    / 2.0
            } else {
                f64::NAN
            }
        })
        .collect();
    let superblocks: [f64; SUPERBLOCKS] = round_ratios
        .chunks_exact(2)
        .map(|pair| (pair[0] + pair[1]) / 2.0)
        .collect::<Vec<_>>()
        .try_into()
        .expect("ten superblocks");
    let valid = superblocks.iter().all(|value| value.is_finite());
    let (mean, upper) = if valid {
        upper95_log(&superblocks)
    } else {
        (f64::NAN, f64::NAN)
    };
    Analysis {
        fixture: series.fixture,
        scale: series.scale,
        cell: series.cell,
        superblocks,
        valid,
        geometric_ratio: mean.exp(),
        upper95: upper.exp(),
    }
}

fn metric(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.12}")
    } else {
        "INVALID".to_owned()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct SuperblockModeMeans {
    b: f64,
    c: f64,
}

fn superblock_mode_means(series: &TimingSeries) -> [SuperblockModeMeans; SUPERBLOCKS] {
    assert_eq!(series.rounds.len(), FORMAL_ROUNDS);
    series
        .rounds
        .chunks_exact(2)
        .map(|pair| SuperblockModeMeans {
            b: pair.iter().flat_map(|round| round.b).sum::<f64>() / 4.0,
            c: pair.iter().flat_map(|round| round.c).sum::<f64>() / 4.0,
        })
        .collect::<Vec<_>>()
        .try_into()
        .expect("ten superblock mode means")
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct MarginalPoint {
    b_small: f64,
    b_large: f64,
    b_delta: f64,
    c_small: f64,
    c_large: f64,
    c_delta: f64,
    log_ratio: Option<f64>,
}

struct MarginalAnalysis {
    label: &'static str,
    points: [MarginalPoint; SUPERBLOCKS],
    valid: bool,
    geometric_ratio: Option<f64>,
    upper95: Option<f64>,
}

fn analyze_marginal(
    label: &'static str,
    small: &TimingSeries,
    large: &TimingSeries,
) -> MarginalAnalysis {
    assert_eq!(small.cell, Cell::Full);
    assert_eq!(large.cell, Cell::Full);
    assert!(small.scale < large.scale);
    let points: [MarginalPoint; SUPERBLOCKS] = superblock_mode_means(small)
        .into_iter()
        .zip(superblock_mode_means(large))
        .map(|(small, large)| {
            let b_delta = large.b - small.b;
            let c_delta = large.c - small.c;
            let valid = [small.b, large.b, b_delta, small.c, large.c, c_delta]
                .into_iter()
                .all(f64::is_finite)
                && b_delta > 0.0
                && c_delta > 0.0;
            let log_ratio = if valid {
                let log_ratio = (c_delta / b_delta).ln();
                log_ratio.is_finite().then_some(log_ratio)
            } else {
                None
            };
            MarginalPoint {
                b_small: small.b,
                b_large: large.b,
                b_delta,
                c_small: small.c,
                c_large: large.c,
                c_delta,
                log_ratio,
            }
        })
        .collect::<Vec<_>>()
        .try_into()
        .expect("ten marginal superblocks");
    let valid = points.iter().all(|point| point.log_ratio.is_some());
    let (geometric_ratio, upper95) = if valid {
        let logs: Vec<_> = points
            .iter()
            .map(|point| point.log_ratio.expect("validated marginal point"))
            .collect();
        let (mean, upper) = upper95_log(&logs);
        (Some(mean.exp()), Some(upper.exp()))
    } else {
        (None, None)
    };
    MarginalAnalysis {
        label,
        points,
        valid,
        geometric_ratio,
        upper95,
    }
}

fn noise_envelope(superblocks: &[f64]) -> f64 {
    assert_eq!(superblocks.len(), SUPERBLOCKS);
    let mean = sample_mean(superblocks);
    (mean.abs()
        + ONE_SIDED_T95_DF9 * sample_stddev(superblocks, mean) / (superblocks.len() as f64).sqrt())
    .exp()
}

fn strict_upper_bound_pass(valid: bool, upper95: f64) -> bool {
    valid && upper95.is_finite() && upper95 < 1.0
}

fn envelope_pass(valid: bool, upper95: f64, envelope: f64) -> bool {
    valid && upper95.is_finite() && envelope.is_finite() && upper95 <= envelope
}

fn emit_marginal(analysis: &MarginalAnalysis) {
    println!(
        "columns\tmarginal_superblock\tseries\tsuperblock\tB_small\tB_large\tdB\tC_small\tC_large\tdC\tlog_ratio\tratio"
    );
    for (index, point) in analysis.points.iter().enumerate() {
        let log_ratio = point
            .log_ratio
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned());
        let ratio = point
            .log_ratio
            .map(|value| metric(value.exp()))
            .unwrap_or_else(|| "INVALID".to_owned());
        println!(
            "marginal_superblock\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            analysis.label,
            index + 1,
            metric(point.b_small),
            metric(point.b_large),
            metric(point.b_delta),
            metric(point.c_small),
            metric(point.c_large),
            metric(point.c_delta),
            log_ratio,
            ratio,
        );
    }
    println!(
        "marginal_summary\t{}\t{}\t{}\t{}",
        analysis.label,
        analysis
            .geometric_ratio
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned()),
        analysis
            .upper95
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned()),
        if analysis.valid { "VALID" } else { "STOP" },
    );
}

fn emit_analysis(analysis: &Analysis) {
    for (index, &log_ratio) in analysis.superblocks.iter().enumerate() {
        println!(
            "superblock\t{}\t{}\t{}\t{}\t{}\t{}",
            analysis.fixture,
            analysis.scale,
            analysis.cell.label(),
            index + 1,
            metric(log_ratio),
            metric(log_ratio.exp()),
        );
    }
    println!(
        "summary\t{}\t{}\t{}\t{}\t{}\t{}",
        analysis.fixture,
        analysis.scale,
        analysis.cell.label(),
        metric(analysis.geometric_ratio),
        metric(analysis.upper95),
        if analysis.valid { "VALID" } else { "STOP" },
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

fn lookup_series<'a>(
    series: &'a [TimingSeries],
    fixture: &str,
    scale: usize,
    cell: Cell,
) -> &'a TimingSeries {
    series
        .iter()
        .find(|series| series.fixture == fixture && series.scale == scale && series.cell == cell)
        .expect("registered timing series")
}

fn all_completed_samples_valid(analyses: &[Analysis]) -> bool {
    analyses.len() == FORMAL_LABELS.len() * Cell::ALL.len()
        && analyses.iter().all(|analysis| analysis.valid)
}

fn decide(analyses: &[Analysis], series: &[TimingSeries]) -> bool {
    let all_completed_samples_valid = all_completed_samples_valid(analyses);
    println!(
        "decision\tcompleted_samples\tanalyses={}\texpected={}\t{}",
        analyses.len(),
        FORMAL_LABELS.len() * Cell::ALL.len(),
        if all_completed_samples_valid {
            "PASS"
        } else {
            "STOP"
        },
    );
    let mut go = all_completed_samples_valid;
    for (fixture, scale) in [
        ("guarded_large", GUARDED_LARGE),
        ("streaming_filter", STREAMING_SCALE),
    ] {
        for cell in Cell::FIRST_DROP {
            let analysis = lookup(analyses, fixture, scale, cell);
            let pass = strict_upper_bound_pass(analysis.valid, analysis.upper95);
            go &= pass;
            println!(
                "decision\tstreaming_upper95\t{}\t{}\t{}\t{}\t{}",
                fixture,
                scale,
                cell.label(),
                metric(analysis.upper95),
                if pass { "PASS" } else { "STOP" },
            );
        }
    }

    let guarded_small = lookup(analyses, "guarded_small", GUARDED_SMALL, Cell::Full);
    let guarded_large = lookup(analyses, "guarded_large", GUARDED_LARGE, Cell::Full);
    println!(
        "drain_absolute\tguarded_small\t{}\t{}\t{}",
        GUARDED_SMALL,
        metric(guarded_small.geometric_ratio),
        metric(guarded_small.upper95)
    );
    println!(
        "drain_absolute\tguarded_large\t{}\t{}\t{}",
        GUARDED_LARGE,
        metric(guarded_large.geometric_ratio),
        metric(guarded_large.upper95)
    );

    let pass = strict_upper_bound_pass(guarded_large.valid, guarded_large.upper95);
    go &= pass;
    println!(
        "decision\tlargest_drain_upper95\tguarded_large\t{}\t{}\t{}",
        GUARDED_LARGE,
        metric(guarded_large.upper95),
        if pass { "PASS" } else { "STOP" },
    );

    let guarded_marginal = analyze_marginal(
        "guarded",
        lookup_series(series, "guarded_small", GUARDED_SMALL, Cell::Full),
        lookup_series(series, "guarded_large", GUARDED_LARGE, Cell::Full),
    );
    let ordinary_marginal = analyze_marginal(
        "ordinary_aa",
        lookup_series(series, "ordinary_small", GUARDED_SMALL, Cell::Full),
        lookup_series(series, "ordinary_large", GUARDED_LARGE, Cell::Full),
    );
    emit_marginal(&guarded_marginal);
    emit_marginal(&ordinary_marginal);
    let ordinary_marginal_logs: Vec<_> = ordinary_marginal
        .points
        .iter()
        .filter_map(|point| point.log_ratio)
        .collect();
    let ordinary_marginal_envelope = if ordinary_marginal.valid {
        Some(noise_envelope(&ordinary_marginal_logs))
    } else {
        None
    };
    let pass = guarded_marginal.valid
        && ordinary_marginal.valid
        && guarded_marginal
            .upper95
            .zip(ordinary_marginal_envelope)
            .is_some_and(|(upper, envelope)| envelope_pass(true, upper, envelope));
    go &= pass;
    println!(
        "decision\tmarginal_drain_cost\tguarded_ratio={}\tguarded_upper95={}\tordinary_aa_envelope={}\t{}",
        guarded_marginal
            .geometric_ratio
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned()),
        guarded_marginal
            .upper95
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned()),
        ordinary_marginal_envelope
            .map(metric)
            .unwrap_or_else(|| "INVALID".to_owned()),
        if pass { "PASS" } else { "STOP" },
    );

    for cell in Cell::BARRIER_GUARD {
        let barrier = lookup(analyses, "barrier_control", BARRIER_SCALE, cell);
        let ordinary = lookup(analyses, "ordinary_barrier", BARRIER_SCALE, cell);
        let ordinary_envelope = if ordinary.valid {
            noise_envelope(&ordinary.superblocks)
        } else {
            f64::NAN
        };
        let pass = envelope_pass(
            barrier.valid && ordinary.valid,
            barrier.upper95,
            ordinary_envelope,
        );
        go &= pass;
        println!(
            "decision\tbarrier_upper95\tbarrier_control\t{}\t{}\tbarrier_ratio={}\tbarrier_upper95={}\tordinary_aa_envelope={}\t{}",
            BARRIER_SCALE,
            cell.label(),
            metric(barrier.geometric_ratio),
            metric(barrier.upper95),
            metric(ordinary_envelope),
            if pass { "PASS" } else { "STOP" },
        );
    }
    go
}

fn formal_fixtures() -> Vec<Fixture> {
    assert!(GUARDED_SMALL.is_power_of_two());
    assert!(GUARDED_LARGE.is_power_of_two());
    assert!(STREAMING_SCALE.is_power_of_two());
    assert!(BARRIER_SCALE.is_power_of_two());
    assert!(GUARDED_SMALL < GUARDED_LARGE);
    vec![
        Fixture::guarded_star(
            "guarded_small",
            GUARDED_SMALL,
            GUARD_WIDTH,
            GUARDED_SMALL_DOMAIN,
        ),
        Fixture::guarded_star(
            "guarded_large",
            GUARDED_LARGE,
            GUARD_WIDTH,
            GUARDED_LARGE_DOMAIN,
        ),
        Fixture::recursive_filter(
            "streaming_filter",
            STREAMING_SCALE,
            true,
            true,
            STREAMING_DOMAIN,
        ),
        Fixture::recursive_filter(
            "barrier_control",
            BARRIER_SCALE,
            false,
            false,
            BARRIER_DOMAIN,
        ),
        Fixture::ordinary("ordinary_barrier", BARRIER_SCALE, ORDINARY_BARRIER_DOMAIN),
        Fixture::ordinary("ordinary_small", GUARDED_SMALL, ORDINARY_SMALL_DOMAIN),
        Fixture::ordinary("ordinary_large", GUARDED_LARGE, ORDINARY_LARGE_DOMAIN),
    ]
}

fn main() {
    let fixtures = formal_fixtures();
    println!(
        "meta\tformat=production-regions-adoption-gate-v3\tprotocol_version={PROTOCOL_VERSION}\tformal_holdout=true\trounds={FORMAL_ROUNDS}\tsuperblocks={SUPERBLOCKS}\tcells=construct_drop,prebuilt_first,fresh_first,drop_first,full_drain\twarm=fixture_order_then_cells_then_B,C_once\todd=B-C-C-B\teven=C-B-B-C\todd_fixtures=Gs,Gl,S,H,Ah,As,Al\teven_fixtures=Al,As,Ah,H,S,Gl,Gs\tt95_df9={ONE_SIDED_T95_DF9}\tguarded_formula_nodes={GUARDED_FORMULA_NODES}\tanti_adaptation=one_formal_invocation"
    );
    println!(
        "holdouts\tGs={GUARDED_SMALL}@{GUARDED_SMALL_DOMAIN}\tGl={GUARDED_LARGE}@{GUARDED_LARGE_DOMAIN}\tS={STREAMING_SCALE}@{STREAMING_DOMAIN}\tH={BARRIER_SCALE}@{BARRIER_DOMAIN}\tAh={BARRIER_SCALE}@{ORDINARY_BARRIER_DOMAIN}\tAs={GUARDED_SMALL}@{ORDINARY_SMALL_DOMAIN}\tAl={GUARDED_LARGE}@{ORDINARY_LARGE_DOMAIN}\tguard_width={GUARD_WIDTH}"
    );
    if catch_unwind(AssertUnwindSafe(|| {
        semantic_and_mechanism_preflight(&fixtures)
    }))
    .is_err()
    {
        println!("decision\tsemantic_mechanism_preflight\tSTOP");
        println!("verdict\tSTOP");
        std::process::exit(3);
    }
    println!("decision\tsemantic_mechanism_preflight\tPASS");
    if catch_unwind(AssertUnwindSafe(|| warm(&fixtures))).is_err() {
        println!("decision\tfixed_warmup\tSTOP");
        println!("verdict\tSTOP");
        std::process::exit(3);
    }
    println!("decision\tfixed_warmup\tPASS");
    let series = measure_series(&fixtures);
    let analyses: Vec<_> = series.iter().map(analyze).collect();
    for analysis in &analyses {
        emit_analysis(analysis);
    }
    let go = decide(&analyses, &series);
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
        assert_eq!(Mode::ALL, [Mode::B, Mode::C]);
        assert_eq!(
            Cell::ALL,
            [
                Cell::ConstructDrop,
                Cell::PullFirst,
                Cell::FreshFirst,
                Cell::DropFirst,
                Cell::Full,
            ]
        );
        assert_eq!(round_order(1), [Mode::B, Mode::C, Mode::C, Mode::B]);
        assert_eq!(round_order(2), [Mode::C, Mode::B, Mode::B, Mode::C]);
        assert_eq!(round_order(19), [Mode::B, Mode::C, Mode::C, Mode::B]);
        assert_eq!(round_order(20), [Mode::C, Mode::B, Mode::B, Mode::C]);
        assert_eq!(fixture_order(1), [0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(fixture_order(2), [6, 5, 4, 3, 2, 1, 0]);
        assert_eq!(fixture_order(19), [0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(fixture_order(20), [6, 5, 4, 3, 2, 1, 0]);
        assert_eq!(
            Cell::FIRST_DROP,
            [Cell::PullFirst, Cell::FreshFirst, Cell::DropFirst]
        );
        assert_eq!(
            Cell::BARRIER_GUARD,
            [
                Cell::PullFirst,
                Cell::FreshFirst,
                Cell::DropFirst,
                Cell::Full,
            ]
        );
        assert!(!Cell::BARRIER_GUARD.contains(&Cell::ConstructDrop));
    }

    #[test]
    fn log_superblocks_and_upper_bound_are_exact() {
        let rounds = (0..FORMAL_ROUNDS)
            .map(|_| RoundSample {
                b: [2.0, 8.0],
                c: [1.0, 4.0],
            })
            .collect();
        let analysis = analyze(&TimingSeries {
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
    fn zero_or_nonfinite_point_samples_are_explicit_stop_evidence() {
        let zero = constant_series("zero", 1, Cell::Full, 0.0, 1.0);
        let zero_analysis = analyze(&zero);
        assert!(!zero_analysis.valid);
        assert_eq!(metric(zero_analysis.upper95), "INVALID");
        assert!(!strict_upper_bound_pass(
            zero_analysis.valid,
            zero_analysis.upper95
        ));

        let infinite = constant_series("infinite", 1, Cell::Full, 1.0, f64::INFINITY);
        assert!(!analyze(&infinite).valid);
    }

    #[test]
    fn every_completed_cell_must_be_valid_even_when_it_has_no_performance_gate() {
        let mut analyses: Vec<_> = FORMAL_LABELS
            .into_iter()
            .flat_map(|fixture| {
                Cell::ALL
                    .into_iter()
                    .map(move |cell| analyze(&constant_series(fixture, 1, cell, 1.0, 0.5)))
            })
            .collect();
        assert!(all_completed_samples_valid(&analyses));

        analyses[0].valid = false;
        assert_eq!(analyses[0].cell, Cell::ConstructDrop);
        assert!(!all_completed_samples_valid(&analyses));
        analyses[0].valid = true;

        analyses.pop();
        assert!(!all_completed_samples_valid(&analyses));
    }

    #[test]
    fn nonzero_variance_upper_bound_uses_sample_sd_and_df9_t() {
        let values: Vec<_> = (0..SUPERBLOCKS).map(|index| index as f64 / 100.0).collect();
        let (mean, upper) = upper95_log(&values);
        let expected_mean = 0.045;
        let known_sample_sd = (82.5f64 / 9.0).sqrt() / 100.0;
        let expected_upper =
            expected_mean + ONE_SIDED_T95_DF9 * known_sample_sd / (SUPERBLOCKS as f64).sqrt();
        assert!((mean - expected_mean).abs() < 1e-12);
        assert!((upper - expected_upper).abs() < 1e-12);
    }

    fn constant_series(
        fixture: &'static str,
        scale: usize,
        cell: Cell,
        b: f64,
        c: f64,
    ) -> TimingSeries {
        TimingSeries {
            fixture,
            scale,
            cell,
            rounds: (0..FORMAL_ROUNDS)
                .map(|_| RoundSample {
                    b: [b, b],
                    c: [c, c],
                })
                .collect(),
        }
    }

    #[test]
    fn marginal_estimator_uses_arithmetic_scale_increments() {
        let small = constant_series("small", 1, Cell::Full, 10.0, 8.0);
        let large = constant_series("large", 4, Cell::Full, 30.0, 18.0);
        let analysis = analyze_marginal("synthetic", &small, &large);
        assert!(analysis.valid);
        assert!((analysis.geometric_ratio.expect("valid ratio") - 0.5).abs() < 1e-12);
        assert!((analysis.upper95.expect("valid bound") - 0.5).abs() < 1e-12);
        assert!(analysis.points.iter().all(|point| {
            (point.b_delta - 20.0).abs() < 1e-12
                && (point.c_delta - 10.0).abs() < 1e-12
                && (point.log_ratio.expect("valid point") - 0.5f64.ln()).abs() < 1e-12
        }));
    }

    #[test]
    fn superblock_mode_means_pair_only_adjacent_odd_even_rounds() {
        let series = TimingSeries {
            fixture: "tagged",
            scale: 1,
            cell: Cell::Full,
            rounds: (0..FORMAL_ROUNDS)
                .map(|round| {
                    let base = round as f64 * 100.0;
                    RoundSample {
                        b: [base + 1.0, base + 3.0],
                        c: [base + 5.0, base + 7.0],
                    }
                })
                .collect(),
        };
        let means = superblock_mode_means(&series);
        for (index, mean) in means.into_iter().enumerate() {
            assert_eq!(mean.b, index as f64 * 200.0 + 52.0);
            assert_eq!(mean.c, index as f64 * 200.0 + 56.0);
        }
    }

    #[test]
    fn marginal_estimator_stops_on_nonpositive_or_nonfinite_increments() {
        let small = constant_series("small", 1, Cell::Full, 10.0, 8.0);
        let flat_b = constant_series("large", 4, Cell::Full, 10.0, 18.0);
        assert!(!analyze_marginal("flat_b", &small, &flat_b).valid);

        let negative_b = constant_series("large", 4, Cell::Full, 9.0, 18.0);
        assert!(!analyze_marginal("negative_b", &small, &negative_b).valid);

        let flat_c = constant_series("large", 4, Cell::Full, 30.0, 8.0);
        assert!(!analyze_marginal("flat_c", &small, &flat_c).valid);

        let negative_c = constant_series("large", 4, Cell::Full, 30.0, 7.0);
        assert!(!analyze_marginal("negative_c", &small, &negative_c).valid);

        let nan_c = constant_series("large", 4, Cell::Full, 30.0, f64::NAN);
        assert!(!analyze_marginal("nan_c", &small, &nan_c).valid);

        let zero = constant_series("zero", 1, Cell::Full, 0.0, 0.0);
        let overflow = constant_series("overflow", 4, Cell::Full, f64::MIN_POSITIVE, f64::MAX);
        assert!(!analyze_marginal("overflow", &zero, &overflow).valid);
    }

    #[test]
    fn aa_envelope_is_absolute_mean_plus_one_sided_error() {
        let logs = [1.25f64.ln(); SUPERBLOCKS];
        assert!((noise_envelope(&logs) - 1.25).abs() < 1e-12);
        let negative = [0.8f64.ln(); SUPERBLOCKS];
        assert!((noise_envelope(&negative) - 1.25).abs() < 1e-12);
        assert!(envelope_pass(true, 1.25, 1.25));
        assert!(!envelope_pass(true, 1.250_000_000_001, 1.25));
        assert!(!strict_upper_bound_pass(true, 1.0));
        assert!(strict_upper_bound_pass(true, 0.999_999_999_999));
        assert!(!strict_upper_bound_pass(false, 0.5));
    }

    #[test]
    fn formal_constants_pin_reserved_holdout_without_building_it() {
        assert_eq!(PROTOCOL_VERSION, "BE6594AF7634F74ACD46A83CF6E67283");
        assert_eq!(GUARDED_SMALL, 1 << 15);
        assert_eq!(GUARDED_LARGE, 1 << 17);
        assert_eq!(STREAMING_SCALE, 1 << 11);
        assert_eq!(BARRIER_SCALE, 1 << 13);
        assert_eq!(GUARD_WIDTH, 32);
        assert_eq!(
            FORMAL_LABELS,
            [
                "guarded_small",
                "guarded_large",
                "streaming_filter",
                "barrier_control",
                "ordinary_barrier",
                "ordinary_small",
                "ordinary_large",
            ]
        );
        assert_eq!(
            [
                GUARDED_SMALL_DOMAIN,
                GUARDED_LARGE_DOMAIN,
                STREAMING_DOMAIN,
                BARRIER_DOMAIN,
                ORDINARY_BARRIER_DOMAIN,
                ORDINARY_SMALL_DOMAIN,
                ORDINARY_LARGE_DOMAIN,
            ],
            [300, 310, 320, 330, 340, 350, 360]
        );
    }

    #[test]
    fn reduced_fixtures_match_oracles_and_mechanism_contracts() {
        let fixtures = vec![
            Fixture::guarded_star("guarded_small", 32, 5, 200),
            Fixture::guarded_star("guarded_large", 64, 5, 210),
            Fixture::recursive_filter("streaming_filter", 32, true, true, 220),
            Fixture::recursive_filter("barrier_control", 32, false, false, 230),
            Fixture::ordinary("ordinary_barrier", 32, 240),
            Fixture::ordinary("ordinary_small", 32, 250),
            Fixture::ordinary("ordinary_large", 64, 260),
        ];
        semantic_and_mechanism_preflight(&fixtures);
    }

    #[test]
    fn drop_cell_samples_mark_counters_as_unobserved() {
        let fixture = Fixture::ordinary("ordinary_observation_boundary", 8, 270);
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
