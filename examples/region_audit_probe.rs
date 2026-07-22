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

fn main() {
    header();
    let guard_widths = [1usize, 8, 64];
    let scales = [64usize, 1024, 4096];
    // Fresh disjoint fixture-id domains per (guard_width, scale, mode) so no two
    // roots ever share a graph or a source id.
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
}
