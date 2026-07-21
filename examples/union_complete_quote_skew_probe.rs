//! Deterministic counter probe for exact complete-work admission quotes.
//!
//! A width-one query first publishes one singleton parent. The next pull opens
//! `S = 2` and exposes two fresh terminal parents together. They look like a
//! cheap two-parent complete cohort to demand-only admission, but each parent
//! owns `fanout` SuccinctArchive occurrences. Exact quote admission must keep
//! that `2 * fanout` raw drain pageable when it exceeds `S`; a fitting
//! one-occurrence-per-parent control must still use the complete phase.
//!
//! This probe intentionally measures no time. It verifies the complete result
//! set and prints deterministic semantic hashes plus the phase/work counters
//! that distinguish quoted from unquoted admission.
//!
//! ```text
//! cargo run --release --example union_complete_quote_skew_probe -- [fanout=4096] [quoted|unquoted]
//! ```

use blake3::Hasher;

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::debug::query::EstimateOverrideConstraint;
use triblespace::core::inline::RawInline;
use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
use triblespace::core::query::sortedsliceconstraint::SortedSlice;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, TriblePattern,
    VariableContext, VariableId, VariableSet,
};
use triblespace::core::repo::index_home::UnionArchive;
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::*;

type Pair = (RawInline, RawInline);

#[derive(Clone, Copy)]
enum ExpectedPolicy {
    Quoted,
    Unquoted,
}

impl ExpectedPolicy {
    fn parse(value: Option<String>) -> Self {
        match value.as_deref() {
            None | Some("quoted") => Self::Quoted,
            Some("unquoted") => Self::Unquoted,
            Some(other) => panic!("expected policy must be quoted or unquoted, got {other}"),
        }
    }
}

/// Preserve only the ordinary constraint protocol for the tiny parent source.
/// This makes all three parents one deterministic proposal bag, rather than
/// letting the source's own pageable Program hide the terminal-cohort shape
/// that this probe is meant to isolate.
#[derive(Clone, Copy)]
struct Ordinary<C>(C);

impl<'a, C: Constraint<'a>> Constraint<'a> for Ordinary<C> {
    fn variables(&self) -> VariableSet {
        self.0.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.0.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.0.influence(variable)
    }
}

#[derive(Debug)]
struct Observation {
    name: &'static str,
    fanout: usize,
    result_count: usize,
    semantic_hash: String,
    search_width: usize,
    exact_complete_work: usize,
    second_pull_proposed: usize,
    second_pull_eager_admissions: usize,
    second_pull_eager_parents: usize,
    second_pull_eager_rows: usize,
    final_stats: ResidualStateStats,
}

fn id(domain: u32, index: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[..4].copy_from_slice(&domain.to_be_bytes());
    raw[4..12].copy_from_slice(&(index as u64).to_be_bytes());
    raw[12..].copy_from_slice(&(index as u32).rotate_left(11).to_be_bytes());
    Id::new(raw).expect("fixture IDs are non-nil")
}

fn digest(rows: &[Pair]) -> String {
    let mut hasher = Hasher::new();
    for (entity, value) in rows {
        hasher.update(entity);
        hasher.update(value);
    }
    hasher.finalize().to_hex().to_string()
}

fn run_case(name: &'static str, fanout: usize) -> Observation {
    assert!(fanout > 0);

    // The ordinary proposal bag is ascending and residual candidate selection
    // is tail-first. Therefore the highest parent is the singleton that must
    // publish first; the two lower parents become the next width-two cohort.
    let wide_a = id(1, 0);
    let wide_b = id(1, 1);
    let singleton = id(1, 2);
    let attribute = id(2, 0);
    let parents = [wide_a, wide_b, singleton];

    let mut facts = TribleSet::new();
    let mut expected = Vec::with_capacity(2 * fanout + 1);
    for (parent, value_domain, count) in [
        (&wide_a, 10, fanout),
        (&wide_b, 11, fanout),
        (&singleton, 12, 1),
    ] {
        for index in 0..count {
            let value = id(value_domain, index);
            let value = inlineencodings::GenId::inline_from(value);
            facts.insert(&Trible::force(parent, &attribute, &value));
            expected.push((inlineencodings::GenId::inline_from(*parent).raw, value.raw));
        }
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&facts).into();
    let archives = [archive];
    let union = UnionArchive::new(&archives);

    let mut context = VariableContext::new();
    let entity = context.next_variable::<inlineencodings::GenId>();
    let value = context.next_variable::<inlineencodings::GenId>();
    let attribute = inlineencodings::GenId::inline_from(attribute);
    let parent_values = parents.map(inlineencodings::GenId::inline_from);
    let parent_values = SortedSlice::new(&parent_values).unwrap();
    let mut parent_source = EstimateOverrideConstraint::new(Ordinary(parent_values.has(entity)));
    parent_source.set_estimate(entity.index, 0);

    let root = and!(parent_source, union.pattern(entity, attribute, value));
    let project =
        move |binding: &Binding| Some((*binding.get(entity.index)?, *binding.get(value.index)?));
    let mut query = Query::new(root, project)
        .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
        .cap(64)
        .start_width(1)
        .growth(2);

    let first = query.next().expect("singleton parent must publish first");
    let singleton_pair = *expected.last().unwrap();
    assert_eq!(
        first, singleton_pair,
        "fixture no longer publishes singleton first"
    );
    assert_eq!(query.current_width(), 1);
    assert_eq!(query.stats().delta_terminal_eager_cohort_admissions, 0);

    let proposed_before = query.stats().candidates_proposed;
    let eager_before = query.stats().delta_terminal_eager_cohort_admissions;
    let eager_parents_before = query.stats().delta_terminal_eager_cohort_parents;
    let eager_rows_before = query.stats().delta_terminal_eager_cohort_rows;
    let second = query
        .next()
        .expect("continued demand must reach one of the wide parents");
    let search_width = query.current_width();
    assert_eq!(search_width, 2, "second demand window must be S=2");

    let second_pull_proposed = query.stats().candidates_proposed - proposed_before;
    let second_pull_eager_admissions =
        query.stats().delta_terminal_eager_cohort_admissions - eager_before;
    let second_pull_eager_parents =
        query.stats().delta_terminal_eager_cohort_parents - eager_parents_before;
    let second_pull_eager_rows = query.stats().delta_terminal_eager_cohort_rows - eager_rows_before;

    let mut actual = vec![first, second];
    actual.extend(&mut query);
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "quote policy changed the exact result set"
    );

    Observation {
        name,
        fanout,
        result_count: actual.len(),
        semantic_hash: digest(&actual),
        search_width,
        exact_complete_work: 2 * fanout,
        second_pull_proposed,
        second_pull_eager_admissions,
        second_pull_eager_parents,
        second_pull_eager_rows,
        final_stats: query.stats().clone(),
    }
}

fn print(observation: &Observation) {
    let bounded = observation.second_pull_eager_admissions == 0
        && observation.second_pull_proposed <= observation.search_width;
    let completed = observation.second_pull_eager_admissions == 1
        && observation.second_pull_eager_parents == 2
        && observation.second_pull_eager_rows == observation.exact_complete_work;
    println!(
        "case={} fanout={} results={} hash={} S={} exact_complete_work={} second_proposed={} second_eager_admissions={} second_eager_parents={} second_eager_rows={} signal_bounded={} signal_completed={}",
        observation.name,
        observation.fanout,
        observation.result_count,
        observation.semantic_hash,
        observation.search_width,
        observation.exact_complete_work,
        observation.second_pull_proposed,
        observation.second_pull_eager_admissions,
        observation.second_pull_eager_parents,
        observation.second_pull_eager_rows,
        bounded,
        completed,
    );
    println!(
        "phase={} terminal_admissions={} demand_wide={} eager_admissions={} eager_parents={} eager_rows={} admission_remainders={} source_pages={} source_examined={} candidates_proposed={} max_terminal_budget={}",
        observation.name,
        observation.final_stats.delta_terminal_admissions,
        observation
            .final_stats
            .delta_terminal_demand_wide_admissions,
        observation
            .final_stats
            .delta_terminal_eager_cohort_admissions,
        observation
            .final_stats
            .delta_terminal_eager_cohort_parents,
        observation.final_stats.delta_terminal_eager_cohort_rows,
        observation
            .final_stats
            .delta_terminal_admission_remainders,
        observation.final_stats.delta_source_pages,
        observation
            .final_stats
            .delta_source_candidates_examined,
        observation.final_stats.candidates_proposed,
        observation.final_stats.max_delta_terminal_work_budget,
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let fanout = args
        .next()
        .map(|arg| arg.parse::<usize>().expect("fanout must be an integer"))
        .unwrap_or(4096);
    let expected_policy = ExpectedPolicy::parse(args.next());
    assert!(args.next().is_none(), "unexpected extra argument");
    assert!(fanout > 1, "skew fanout must exceed S/parent = 1");

    let skew = run_case("skew", fanout);
    assert!(skew.exact_complete_work > skew.search_width);
    match expected_policy {
        ExpectedPolicy::Quoted => {
            assert_eq!(skew.second_pull_eager_admissions, 0);
            assert!(skew.second_pull_proposed <= skew.search_width);
        }
        ExpectedPolicy::Unquoted => {
            assert_eq!(skew.second_pull_eager_admissions, 1);
            assert_eq!(skew.second_pull_eager_parents, 2);
            assert_eq!(skew.second_pull_eager_rows, skew.exact_complete_work);
            assert_eq!(skew.second_pull_proposed, skew.exact_complete_work);
        }
    }

    let fitting = run_case("fitting", 1);
    assert_eq!(fitting.second_pull_eager_admissions, 1);
    assert_eq!(fitting.second_pull_eager_parents, 2);
    assert_eq!(fitting.second_pull_eager_rows, 2);
    assert_eq!(fitting.second_pull_proposed, 2);

    print(&skew);
    print(&fitting);
}
