//! Region-attribution audit probe.
//!
//! Reconstructs the prior audit's mode-B (`OPAQUE_PRODUCTION`) vs mode-C
//! (`ProductionRegions + Production`) comparison over the `guarded_star`
//! fixture and dumps the raw [`ResidualStateStats`] counters relevant to the
//! region-attribution question: the confirmation populations
//! (`candidates_confirmed`, `confirm_calls`, `confirm_action_pops`,
//! `candidate_confirmation_groups`), the re-entry population
//! (`state_reentries`, `rows_reentered`, `bucket_merges`, `rows_merged`), the
//! interner population (`states_interned`, `interner_hits`), and the physical
//! delta activation/transition populations.
//!
//! The fixture is the exact `guarded_star` shape used by
//! `production_formula_regions_bench`: a fixed `start`, and a Union of a dead
//! guard arm (a width-`guard_width` intersection of `EqualityConstraint`s plus
//! one `FalseConstraint`) with a single `Attr` regular-path constraint that
//! fans `scale` distinct endpoints out of `start`.
//!
//! Run: `cargo run --release --example region_audit_probe`

use triblespace::core::id::{ExclusiveId, Id};
use triblespace::core::inline::{Inline, RawInline};
use triblespace::core::query::equalityconstraint::EqualityConstraint;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::regularpathconstraint::{PathOp, RegularPathConstraint};
use triblespace::core::query::residual::{ResidualLowering, ResidualStateStats};
use triblespace::core::query::unionconstraint::UnionConstraint;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, Variable, VariableId,
    VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

const START: VariableId = 0;
const END: VariableId = 1;

type OwnedConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = std::sync::Arc<IntersectionConstraint<OwnedConstraint>>;

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

/// Shared, route-independent tally of the REAL leaf action callbacks the
/// engine performs, regardless of whether a mode routes the confirm through the
/// composite Union verb (mode B: one `confirm_leaf` fans internally to each
/// satisfied arm) or through per-atom exposure (mode C). These counters are
/// what `candidates_confirmed` cannot see: they are incremented inside the
/// constraint's own `confirm`/`propose` bodies, once per actual invocation,
/// charged by the true candidate population each call processes.
#[derive(Default)]
struct CallbackTally {
    confirm_calls: std::sync::atomic::AtomicUsize,
    confirm_candidates_in: std::sync::atomic::AtomicUsize,
    confirm_candidates_out: std::sync::atomic::AtomicUsize,
    propose_calls: std::sync::atomic::AtomicUsize,
    propose_candidates_out: std::sync::atomic::AtomicUsize,
    satisfied_calls: std::sync::atomic::AtomicUsize,
}

impl CallbackTally {
    fn add(counter: &std::sync::atomic::AtomicUsize, delta: usize) {
        counter.fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
    }
    fn get(counter: &std::sync::atomic::AtomicUsize) -> usize {
        counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// A countable star leaf: proposes and confirms a fixed END domain of `scale`
/// distinct values fanning out of one `start`. It deliberately exposes NO typed
/// `residual_program`, so BOTH lowerings confirm it through the ordinary
/// `confirm`/`confirm_certified` verb — the exact per-arm callback that
/// `UnionConstraint::confirm_with_mode` performs and that residual stats never
/// see. Every action body records its real invocation into the shared tally.
#[derive(Clone)]
struct CountingStar {
    values: std::sync::Arc<Vec<RawInline>>,
    tally: std::sync::Arc<CallbackTally>,
}

impl Constraint<'static> for CountingStar {
    fn variables(&self) -> VariableSet {
        // Mention both endpoints so this leaf shares the Union arm variable set
        // with the guard arm (which equates START and END). START is bound by
        // the ambient `start.is(source)`; only END is ever proposed/confirmed.
        VariableSet::new_singleton(START).union(VariableSet::new_singleton(END))
    }
    fn fixed_denotation(&self) -> bool {
        true
    }
    fn proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> triblespace::core::query::ProposalCoverage {
        if variable == END && !bound.is_set(END) {
            triblespace::core::query::ProposalCoverage::Exact
        } else {
            triblespace::core::query::ProposalCoverage::None
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
        if variable != END {
            return;
        }
        CallbackTally::add(&self.tally.propose_calls, 1);
        for row in 0..view.len() {
            candidates.extend_row(row as u32, self.values.iter().copied());
        }
        CallbackTally::add(
            &self.tally.propose_candidates_out,
            self.values.len() * view.len(),
        );
    }
    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != END {
            return;
        }
        CallbackTally::add(&self.tally.confirm_calls, 1);
        CallbackTally::add(&self.tally.confirm_candidates_in, candidates.len());
        candidates.retain(|_, value| self.values.binary_search(value).is_ok());
        CallbackTally::add(&self.tally.confirm_candidates_out, candidates.len());
    }
    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        CallbackTally::add(&self.tally.satisfied_calls, 1);
        view.col(END).is_none_or(|column| {
            view.iter()
                .all(|row| self.values.binary_search(&row[column]).is_ok())
        })
    }
    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
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

/// Builds one `guarded_star` root exactly as the adoption-gate bench does.
fn guarded_star_root(scale: usize, guard_width: usize, domain: u32) -> (Root, usize) {
    let attribute = fixture_id(domain, 0);
    let source = fixture_id(domain + 1, 0);
    let mut graph = TribleSet::new();
    let mut expected = 0usize;
    for index in 0..scale {
        let target = fixture_id(domain + 2, index);
        let value = genid_inline(&target).raw;
        graph.insert(&Trible::new::<GenId>(
            ExclusiveId::force_ref(&source),
            &attribute,
            &Inline::<GenId>::new(value),
        ));
        expected += 1;
    }

    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let path = Box::new(RegularPathConstraint::new(
        graph,
        start,
        end,
        &[PathOp::Attr(attribute.raw())],
    )) as OwnedConstraint;
    let mut dead = Vec::with_capacity(guard_width + 1);
    for _ in 0..guard_width {
        dead.push(Box::new(EqualityConstraint::new(START, END)) as OwnedConstraint);
    }
    dead.push(Box::new(FalseConstraint) as OwnedConstraint);
    let root = std::sync::Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(source.to_inline())) as OwnedConstraint,
        Box::new(UnionConstraint::new(vec![
            Box::new(IntersectionConstraint::new(dead)) as OwnedConstraint,
            path,
        ])) as OwnedConstraint,
    ]));
    (root, expected)
}

/// Builds a `guarded_counting_star` root: same disjunctive dead guard as
/// `guarded_star`, but the live arm is a [`CountingStar`] domain leaf (no typed
/// program) instead of the RPQ. Because the live leaf exposes no
/// `residual_program`, BOTH modes confirm it through the ordinary
/// `confirm`/`confirm_certified` verb, so the shared [`CallbackTally`] records
/// the REAL per-arm action work identically-measurable across B and C.
fn counting_star_root(
    scale: usize,
    guard_width: usize,
    domain: u32,
    tally: std::sync::Arc<CallbackTally>,
) -> (Root, usize) {
    let source = fixture_id(domain + 1, 0);
    let mut values: Vec<RawInline> = (0..scale)
        .map(|index| genid_inline(&fixture_id(domain + 2, index)).raw)
        .collect();
    values.sort_unstable();
    values.dedup();
    let expected = values.len();

    let start = Variable::<GenId>::new(START);
    let live = Box::new(CountingStar {
        values: std::sync::Arc::new(values),
        tally,
    }) as OwnedConstraint;
    let mut dead = Vec::with_capacity(guard_width + 1);
    for _ in 0..guard_width {
        dead.push(Box::new(EqualityConstraint::new(START, END)) as OwnedConstraint);
    }
    dead.push(Box::new(FalseConstraint) as OwnedConstraint);
    let root = std::sync::Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(source.to_inline())) as OwnedConstraint,
        Box::new(UnionConstraint::new(vec![
            Box::new(IntersectionConstraint::new(dead)) as OwnedConstraint,
            live,
        ])) as OwnedConstraint,
    ]));
    (root, expected)
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

/// Full-drains the fixture under one lowering and returns (row_count, stats).
fn drain(root: Root, scale: usize, lowering: ResidualLowering) -> (usize, ResidualStateStats) {
    let mut query = Query::new(root, project_end as fn(&Binding) -> Option<RawInline>)
        .solve_residual_state_lazy_with(lowering)
        .cap(scale.max(1))
        .start_width(1);
    let mut rows = 0usize;
    while let Some(_row) = query.next() {
        rows += 1;
    }
    (rows, query.stats().clone())
}

fn header() {
    // One tab-separated row per (mode, guard_width, scale). The columns are the
    // raw counters that decide the region-attribution question.
    println!(
        "row\tmode\tguard_width\tscale\trows\t\
         states_interned\tinterner_hits\tbucket_merges\trows_merged\t\
         state_reentries\trows_reentered\t\
         confirm_action_pops\tconfirm_calls\tconfirm_rows\tcandidates_confirmed\t\
         max_confirm_candidates\tcandidate_confirmation_groups\tcandidate_plan_pops\t\
         propose_action_pops\tpropose_calls\tcandidates_proposed\t\
         delta_transition_pages\tdelta_transition_candidates_examined\t\
         delta_activations_completed\tdelta_program_physical_cohorts\tdelta_program_physical_rows\t\
         delta_source_pages\tdelta_source_candidates_examined"
    );
}

fn emit(mode: &str, guard_width: usize, scale: usize, rows: usize, s: &ResidualStateStats) {
    println!(
        "row\t{mode}\t{guard_width}\t{scale}\t{rows}\t\
         {}\t{}\t{}\t{}\t\
         {}\t{}\t\
         {}\t{}\t{}\t{}\t\
         {}\t{}\t{}\t\
         {}\t{}\t{}\t\
         {}\t{}\t\
         {}\t{}\t{}\t\
         {}\t{}",
        s.states_interned,
        s.interner_hits,
        s.bucket_merges,
        s.rows_merged,
        s.state_reentries,
        s.rows_reentered,
        s.confirm_action_pops,
        s.confirm_calls,
        s.confirm_rows,
        s.candidates_confirmed,
        s.max_confirm_candidates,
        s.candidate_confirmation_groups,
        s.candidate_plan_pops,
        s.propose_action_pops,
        s.propose_calls,
        s.candidates_proposed,
        s.delta_transition_pages,
        s.delta_transition_candidates_examined,
        s.delta_activations_completed,
        s.delta_program_physical_cohorts,
        s.delta_program_physical_rows,
        s.delta_source_pages,
        s.delta_source_candidates_examined,
    );
}

/// Full-drains a counting-star fixture and returns (rows, residual stats, the
/// real callback tally). A fresh tally is threaded into the root so the counts
/// are exactly the invocations of this one solve.
fn drain_counting(
    scale: usize,
    guard_width: usize,
    domain: u32,
    lowering: ResidualLowering,
) -> (usize, ResidualStateStats, std::sync::Arc<CallbackTally>) {
    let tally = std::sync::Arc::new(CallbackTally::default());
    let (root, expected) = counting_star_root(scale, guard_width, domain, std::sync::Arc::clone(&tally));
    let (rows, stats) = drain(root, scale, lowering);
    assert_eq!(rows, expected, "counting-star row count mismatch");
    (rows, stats, tally)
}

fn callback_header() {
    println!(
        "cb\tmode\tguard_width\tscale\trows\t\
         REAL_confirm_calls\tREAL_confirm_candidates_in\tREAL_confirm_candidates_out\t\
         REAL_propose_calls\tREAL_propose_candidates_out\tREAL_satisfied_calls\t\
         stat_candidates_confirmed\tstat_confirm_calls\tstat_confirm_action_pops\t\
         stat_candidates_proposed\tstat_propose_calls\tstat_rows_reentered"
    );
}

fn callback_emit(
    mode: &str,
    guard_width: usize,
    scale: usize,
    rows: usize,
    tally: &CallbackTally,
    s: &ResidualStateStats,
) {
    println!(
        "cb\t{mode}\t{guard_width}\t{scale}\t{rows}\t\
         {}\t{}\t{}\t\
         {}\t{}\t{}\t\
         {}\t{}\t{}\t\
         {}\t{}\t{}",
        CallbackTally::get(&tally.confirm_calls),
        CallbackTally::get(&tally.confirm_candidates_in),
        CallbackTally::get(&tally.confirm_candidates_out),
        CallbackTally::get(&tally.propose_calls),
        CallbackTally::get(&tally.propose_candidates_out),
        CallbackTally::get(&tally.satisfied_calls),
        s.candidates_confirmed,
        s.confirm_calls,
        s.confirm_action_pops,
        s.candidates_proposed,
        s.propose_calls,
        s.rows_reentered,
    );
}

fn main() {
    let guard_widths = [1usize, 8, 64];
    let scales = [64usize, 1024, 4096];

    // --- Section 1: original guarded_star residual-stat table (RPQ live arm). ---
    println!("== SECTION 1: guarded_star (RPQ live arm) residual stats ==");
    header();
    let mut domain: u32 = 500;
    for &scale in &scales {
        for &guard_width in &guard_widths {
            // Mode B: OPAQUE_PRODUCTION (OpaqueLeaves + Production).
            let (root_b, expected_b) = guarded_star_root(scale, guard_width, domain);
            domain += 10;
            let (rows_b, stats_b) = drain(root_b, scale, ResidualLowering::OPAQUE_PRODUCTION);
            assert_eq!(rows_b, expected_b, "mode B row count mismatch");
            emit("B", guard_width, scale, rows_b, &stats_b);

            // Mode C: PRODUCTION (ProductionRegions + Production).
            let (root_c, expected_c) = guarded_star_root(scale, guard_width, domain);
            domain += 10;
            let (rows_c, stats_c) = drain(root_c, scale, ResidualLowering::PRODUCTION);
            assert_eq!(rows_c, expected_c, "mode C row count mismatch");
            emit("C", guard_width, scale, rows_c, &stats_c);
        }
    }

    // --- Section 2: counting_star (ordinary-verb live arm), REAL callbacks. ---
    // The live arm exposes no typed program, so both modes confirm it through
    // the ordinary verb. The REAL_* columns are the actual per-arm action work
    // (invisible to `candidates_confirmed`); the stat_* columns are what the
    // residual scheduler records. Comparing REAL_* across B and C answers
    // "does adopting a route change the real work?" — as opposed to the
    // accounting granularity of the stat columns.
    println!("== SECTION 2: counting_star (ordinary-verb live arm) REAL callbacks vs stats ==");
    callback_header();
    domain = 900;
    for &scale in &scales {
        for &guard_width in &guard_widths {
            let (rows_b, stats_b, tally_b) =
                drain_counting(scale, guard_width, domain, ResidualLowering::OPAQUE_PRODUCTION);
            domain += 10;
            callback_emit("B", guard_width, scale, rows_b, &tally_b, &stats_b);

            let (rows_c, stats_c, tally_c) =
                drain_counting(scale, guard_width, domain, ResidualLowering::PRODUCTION);
            domain += 10;
            callback_emit("C", guard_width, scale, rows_c, &tally_c, &stats_c);
        }
    }
}
