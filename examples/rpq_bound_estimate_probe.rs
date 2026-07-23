#![allow(unexpected_cfgs)]

//! Deterministic latency probe for PositiveSupport on target-confirm RPQs.
//!
//! The candidate source is deliberately smaller than the bound-source RPQ
//! estimate, so it proposes `end` and the RPQ is the exact confirmer. The two
//! fixtures have the same reachable candidate set and differ only in candidate
//! occurrence zero:
//!
//! - `positive-first`: the first candidate is the one-hop reachable endpoint;
//! - `negative-first`: an unreachable low sentinel precedes that endpoint.
//!
//! `hybrid-production` exercises production Programs, including the
//! PositiveSupport hedge on revisions that contain it. The identical source
//! can measure a pre-caller revision with
//! `RUSTFLAGS='--cfg baseline_without_positive_support_stats'`; there HYBRID's
//! typed Confirm is the cross-revision control and the unavailable attribution
//! fields print as `n/a`. `programs-disabled` is a secondary exact semantic
//! control that keeps the residual scheduler and formula scope but disables
//! all typed Programs; it is intentionally not presented as a
//! PositiveSupport-only ablation.
//!
//! Run with:
//! `cargo run --release --example rpq_bound_estimate_probe -- [nodes=4096] [reps=9]`

use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::time::{Duration, Instant};

use triblespace::core::id::Id;
use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::residual::{
    FormulaScope, ProgramScope, ResidualLowering, ResidualStateIter, ResidualStateStats,
};
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, PathOp, ProposalCoverage, Query,
    RegularPathConstraint, RowsView, Variable, VariableId, VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::{Inline, IntoInline};

const START: usize = 0;
const END: usize = 1;
const DISTINCT_HITS: usize = 8;
const WIDTHS: [usize; 4] = [1, 4, 16, 64];

type DynConstraint<'a> = Box<dyn Constraint<'a> + 'a>;
type Root<'a> = IntersectionConstraint<DynConstraint<'a>>;
type Project = fn(&Binding) -> Option<RawInline>;
type ProbeIter<'a> = ResidualStateIter<Root<'a>, Project, RawInline>;

#[derive(Clone, Copy)]
struct Mode {
    label: &'static str,
    lowering: ResidualLowering,
}

const MODES: [Mode; 2] = [
    Mode {
        label: "hybrid-production",
        lowering: ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::Production),
    },
    Mode {
        label: "programs-disabled",
        lowering: ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::Disabled),
    },
];

struct Graph {
    set: TribleSet,
    source: Inline<GenId>,
    operations: Vec<PathOp>,
    hits: Vec<Inline<GenId>>,
}

struct Fixture {
    label: &'static str,
    set: TribleSet,
    source: Inline<GenId>,
    candidates: Vec<RawInline>,
    operations: Vec<PathOp>,
    expected: Vec<RawInline>,
}

struct OrderedCandidates<'a> {
    variable: VariableId,
    gate: VariableId,
    values: &'a [RawInline],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Signature {
    count: usize,
    hash: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Attribution {
    positive_terminal: usize,
    positive_chunk_homomorphic: usize,
    direct_terminal_rows: usize,
    support_calls: usize,
    transition_candidates_examined: usize,
}

struct Profile {
    first: RawInline,
    first_stats: Attribution,
    full_stats: Attribution,
}

#[derive(Default)]
struct Samples {
    construct: Vec<Duration>,
    first: Vec<Duration>,
    full: Vec<Duration>,
}

fn id(domain: u32, index: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&domain.to_be_bytes());
    raw[4..12].copy_from_slice(&(index as u64).to_be_bytes());
    raw[12..].copy_from_slice(&(index as u32).rotate_left(13).to_be_bytes());
    Id::new(raw).expect("fixture IDs are non-nil")
}

fn value(id: Id) -> Inline<GenId> {
    id.to_inline()
}

fn insert_edge(set: &mut TribleSet, from: Id, attribute: Id, to: Id) {
    set.insert(&Trible::force(&from, &attribute, &value(to)));
}

fn build_graph(node_count: usize) -> Graph {
    assert!(
        node_count >= DISTINCT_HITS * 2,
        "nodes must leave room for distinct sampled endpoints"
    );
    let source = id(10, 0);
    let attribute = id(40, 0);
    let nodes: Vec<_> = (0..node_count).map(|index| id(20, index)).collect();
    let mut set = TribleSet::new();
    insert_edge(&mut set, source, attribute, nodes[0]);
    for edge in nodes.windows(2) {
        insert_edge(&mut set, edge[0], attribute, edge[1]);
    }

    let hits = (0..DISTINCT_HITS)
        .map(|sample| {
            let index = sample * (node_count - 1) / (DISTINCT_HITS - 1);
            value(nodes[index])
        })
        .collect();
    Graph {
        set,
        source: value(source),
        operations: vec![PathOp::Attr(attribute.raw()), PathOp::Plus],
        hits,
    }
}

fn build_fixture(graph: &Graph, first_is_positive: bool) -> Fixture {
    let mut candidates: Vec<_> = graph.hits.iter().map(|hit| hit.raw).collect();
    // Preserve candidate-bag multiplicity while the projected result remains
    // a set. This also exercises subtraction of an already-published witness
    // when the exact confirmer later drains.
    candidates.push(graph.hits[0].raw);
    candidates.push(
        value(if first_is_positive {
            id(30, 0)
        } else {
            id(5, 0)
        })
        .raw,
    );
    candidates.sort_unstable();

    let mut expected: Vec<_> = graph.hits.iter().map(|hit| hit.raw).collect();
    expected.sort_unstable();
    expected.dedup();
    assert_eq!(expected.len(), DISTINCT_HITS);
    assert_eq!(
        expected.binary_search(&candidates[0]).is_ok(),
        first_is_positive,
        "fixture label must agree with candidate occurrence zero"
    );

    Fixture {
        label: if first_is_positive {
            "positive-first"
        } else {
            "negative-first"
        },
        set: graph.set.clone(),
        source: graph.source,
        candidates,
        operations: graph.operations.clone(),
        expected,
    }
}

impl<'a> Constraint<'a> for OrderedCandidates<'a> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.variable && !bound.is_set(variable) {
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
        if variable != self.variable {
            return false;
        }
        // Let the source endpoint bind first. Once it is present, deliberately
        // tie the RPQ's one-hop bound estimate; lower constraint occurrence
        // order then makes this exact domain the proposer and the RPQ the
        // grouped target confirmer.
        out.fill(
            if view.col(self.gate).is_some() {
                1
            } else {
                self.values.len()
            },
            view.len(),
        );
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
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
        if variable == self.variable {
            candidates.retain(|_, candidate| self.values.binary_search(candidate).is_ok());
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable).is_none_or(|column| {
            view.iter()
                .all(|row| self.values.binary_search(&row[column]).is_ok())
        })
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

fn make_query<'a>(fixture: &'a Fixture) -> Query<Root<'a>, Project, RawInline> {
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let constraints: Vec<DynConstraint<'a>> = vec![
        Box::new(start.is(fixture.source)),
        Box::new(OrderedCandidates {
            variable: end.index,
            gate: start.index,
            values: &fixture.candidates,
        }),
        Box::new(RegularPathConstraint::new(
            fixture.set.clone(),
            start,
            end,
            &fixture.operations,
        )),
    ];
    Query::new(
        IntersectionConstraint::new(constraints),
        project_end as Project,
    )
}

fn make_iter<'a>(fixture: &'a Fixture, mode: Mode, width: usize) -> ProbeIter<'a> {
    make_query(fixture)
        .solve_residual_state_lazy_with(mode.lowering)
        // Hold each rung at one physical width so the comparison does not
        // conflate a cap with history-dependent geometric widening.
        .cap(width)
        .start_width(width)
        .growth(2)
}

fn signature(items: impl IntoIterator<Item = RawInline>) -> Signature {
    let mut count = 0usize;
    let mut hash = 0u64;
    for item in items {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        hash = hash.wrapping_add(hasher.finish());
        count += 1;
    }
    Signature { count, hash }
}

fn attribution(stats: &ResidualStateStats) -> Attribution {
    Attribution {
        #[cfg(not(baseline_without_positive_support_stats))]
        positive_terminal: stats.delta_positive_support_terminal_commits,
        #[cfg(baseline_without_positive_support_stats)]
        positive_terminal: 0,
        #[cfg(not(baseline_without_positive_support_stats))]
        positive_chunk_homomorphic: stats.delta_positive_support_chunk_homomorphic_commits,
        #[cfg(baseline_without_positive_support_stats)]
        positive_chunk_homomorphic: 0,
        direct_terminal_rows: stats.delta_direct_terminal_publication_rows,
        support_calls: stats.support_calls,
        transition_candidates_examined: stats.delta_transition_candidates_examined,
    }
}

fn oracle(fixture: &Fixture) -> (Vec<RawInline>, Signature) {
    let mut results: Vec<_> = make_query(fixture).sequential().collect();
    results.sort_unstable();
    assert_eq!(
        results, fixture.expected,
        "{}: sequential oracle disagrees with fixture",
        fixture.label
    );
    let oracle_signature = signature(results.iter().copied());
    (results, oracle_signature)
}

fn profile(
    fixture: &Fixture,
    mode: Mode,
    width: usize,
    oracle: &[RawInline],
    oracle_signature: Signature,
) -> Profile {
    let mut first_iter = make_iter(fixture, mode, width);
    let first = first_iter
        .next()
        .unwrap_or_else(|| panic!("{} {} returned no first row", fixture.label, mode.label));
    let first_stats = attribution(first_iter.stats());

    let mut full_iter = make_iter(fixture, mode, width);
    let mut full: Vec<_> = full_iter.by_ref().collect();
    let full_stats = attribution(full_iter.stats());
    assert_eq!(
        signature(full.iter().copied()),
        oracle_signature,
        "{} {} width {width}: full signature disagrees with oracle",
        fixture.label,
        mode.label
    );
    full.sort_unstable();
    assert_eq!(
        full, oracle,
        "{} {} width {width}: full result set disagrees with oracle",
        fixture.label, mode.label
    );

    let positive_commits = first_stats.positive_terminal + first_stats.positive_chunk_homomorphic;
    #[cfg(not(baseline_without_positive_support_stats))]
    let first_is_positive = oracle.binary_search(&fixture.candidates[0]).is_ok();
    if mode.lowering.program_scope() == ProgramScope::Disabled {
        assert_eq!(
            positive_commits, 0,
            "programs-disabled control attributed a PositiveSupport commit"
        );
        assert_eq!(
            full_stats.positive_terminal + full_stats.positive_chunk_homomorphic,
            0,
            "programs-disabled full drain attributed a PositiveSupport commit"
        );
    }
    #[cfg(not(baseline_without_positive_support_stats))]
    if mode.lowering.program_scope() != ProgramScope::Disabled && first_is_positive {
        assert_eq!(
            positive_commits, 1,
            "positive-first HYBRID did not attribute its first row to PositiveSupport"
        );
        assert_eq!(
            first, fixture.candidates[0],
            "PositiveSupport did not publish candidate occurrence zero"
        );
        assert!(
            full_stats.transition_candidates_examined > first_stats.transition_candidates_examined,
            "positive publication unexpectedly drained the exact RPQ remainder"
        );
    } else if mode.lowering.program_scope() != ProgramScope::Disabled {
        assert_eq!(
            positive_commits, 0,
            "negative occurrence zero must not publish through PositiveSupport"
        );
        assert_eq!(
            full_stats.positive_terminal + full_stats.positive_chunk_homomorphic,
            0,
            "negative-first full drain must not feed later candidates into PositiveSupport"
        );
    }

    Profile {
        first,
        first_stats,
        full_stats,
    }
}

#[cfg(not(baseline_without_positive_support_stats))]
fn positive_attribution(stats: Attribution) -> String {
    format!(
        "{}/{}",
        stats.positive_terminal, stats.positive_chunk_homomorphic
    )
}

#[cfg(baseline_without_positive_support_stats)]
fn positive_attribution(_stats: Attribution) -> String {
    "n/a".to_owned()
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile + 50) / 100;
    sorted[index]
}

fn measure(fixture: &Fixture, width: usize, reps: usize) {
    let (oracle, oracle_signature) = oracle(fixture);
    let profiles: Vec<_> = MODES
        .iter()
        .copied()
        .map(|mode| profile(fixture, mode, width, &oracle, oracle_signature))
        .collect();
    let mut samples: [Samples; MODES.len()] = std::array::from_fn(|_| Samples {
        construct: Vec::with_capacity(reps),
        first: Vec::with_capacity(reps),
        full: Vec::with_capacity(reps),
    });

    // The profile pass above is also one untimed warmup per mode. Rotate mode
    // order on every repetition to avoid consistently favoring one arm.
    for repetition in 0..reps {
        for offset in 0..MODES.len() {
            let index = (repetition + offset) % MODES.len();
            let mode = MODES[index];

            let began = Instant::now();
            let constructed = black_box(make_iter(fixture, mode, width));
            samples[index].construct.push(began.elapsed());
            drop(constructed);

            let mut first_iter = make_iter(fixture, mode, width);
            let began = Instant::now();
            let first = black_box(first_iter.next());
            samples[index].first.push(began.elapsed());
            assert_eq!(first, Some(profiles[index].first));
            drop(first_iter);

            let mut full_iter = make_iter(fixture, mode, width);
            let began = Instant::now();
            let full_signature = black_box(signature(full_iter.by_ref()));
            samples[index].full.push(began.elapsed());
            assert_eq!(full_signature, oracle_signature);
        }
    }

    println!(
        "\n{} width={width} candidates={} oracle=count:{} hash:{:016x}",
        fixture.label,
        fixture.candidates.len(),
        oracle_signature.count,
        oracle_signature.hash,
    );
    for (index, mode) in MODES.iter().enumerate() {
        let profile = &profiles[index];
        let samples = &samples[index];
        println!(
            "  {:<18} construct p50/p95 {:>10?}/{:>10?}  \
             ttfr p50/p95 {:>10?}/{:>10?}  full p50/p95 {:>10?}/{:>10?}",
            mode.label,
            percentile(&samples.construct, 50),
            percentile(&samples.construct, 95),
            percentile(&samples.first, 50),
            percentile(&samples.first, 95),
            percentile(&samples.full, 50),
            percentile(&samples.full, 95),
        );
        println!(
            "    attribution first: positive terminal/chunk {} direct_rows {} \
             support_calls {} transition_candidates {}; \
             full: positive terminal/chunk {} direct_rows {} support_calls {} \
             transition_candidates {}",
            positive_attribution(profile.first_stats),
            profile.first_stats.direct_terminal_rows,
            profile.first_stats.support_calls,
            profile.first_stats.transition_candidates_examined,
            positive_attribution(profile.full_stats),
            profile.full_stats.direct_terminal_rows,
            profile.full_stats.support_calls,
            profile.full_stats.transition_candidates_examined,
        );
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let node_count = args
        .get(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(4_096);
    let reps = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(9);
    assert!(reps > 0, "reps must be positive");

    let graph = build_graph(node_count);
    let fixtures = [build_fixture(&graph, true), build_fixture(&graph, false)];
    println!(
        "RPQ PositiveSupport probe: nodes={node_count} reps={reps} \
         distinct_hits={DISTINCT_HITS} fixed_widths={WIDTHS:?}"
    );
    println!(
        "HYBRID is explicit OpaqueLeaves+Production; programs-disabled is \
         OpaqueLeaves+Disabled and disables every typed Program."
    );
    #[cfg(baseline_without_positive_support_stats)]
    println!(
        "baseline_without_positive_support_stats: PositiveSupport attribution \
         is unavailable; HYBRID is the typed-Confirm cross-revision control."
    );
    for fixture in &fixtures {
        for width in WIDTHS {
            measure(fixture, width, reps);
        }
    }
}
