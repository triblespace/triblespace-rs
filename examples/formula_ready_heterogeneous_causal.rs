//! Untimed causal probe for heterogeneous row-local Formula Ready choices.
//!
//! The query has one exact parent domain and two exact copies of the same
//! `(parent, target)` relation.  Once the parent is bound, A quotes target
//! work as `1/9` on even/odd parents while B quotes `9/1`.  A and B therefore
//! win exactly half of one 64-row Ready cohort each, without changing the
//! denoted raw SET.  Only deterministic protocol, scheduler, and shadow
//! lifecycle observations are reported; this example never measures time.
//!
//! The public Ready counters intentionally count preferred-variable groups,
//! not physical Formula-child partitions.  Consequently their equality is an
//! invariant, not a discriminator.  With identical downstream leaf actions,
//! the remaining public state geometry is a differential witness for the
//! number of control shells crossed before those actions.  It is not a direct
//! observation of a private carrier representation.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use triblespace::core::inline::encodings::iu256::U256BE;
use triblespace::core::inline::{Inline, RawInline};
use triblespace::core::query::residual::{
    ActionOutcome, ActionVerb, ResidualLowering, ResidualShadowEpoch, ResidualShadowStatus,
};
use triblespace::core::query::{
    CandidateSink, Constraint, EstimateSink, ProposalCoverage, RowsView, VariableId, VariableSet,
};
use triblespace::prelude::*;

const PARENTS: usize = 64;
const LOW_QUOTE: usize = 1;
const HIGH_QUOTE: usize = 9;
const UNBOUND_QUOTE: usize = 1_024;

const REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};

fn value(ordinal: usize) -> RawInline {
    U256BE::inline_from(ordinal as u64).raw
}

fn ordinal(raw: &RawInline) -> Option<usize> {
    (0..PARENTS).find(|&candidate| value(candidate) == *raw)
}

#[derive(Default)]
struct LeafTelemetry {
    estimate_parent_calls: AtomicUsize,
    estimate_parent_rows: AtomicUsize,
    estimate_target_calls: AtomicUsize,
    estimate_target_rows: AtomicUsize,
    estimate_target_bound_rows: AtomicUsize,
    low_quotes: AtomicUsize,
    high_quotes: AtomicUsize,
    propose_parent_calls: AtomicUsize,
    propose_parent_rows: AtomicUsize,
    propose_target_calls: AtomicUsize,
    propose_target_rows: AtomicUsize,
    propose_candidates: AtomicUsize,
    confirm_parent_calls: AtomicUsize,
    confirm_parent_rows: AtomicUsize,
    confirm_target_calls: AtomicUsize,
    confirm_target_rows: AtomicUsize,
    confirm_candidates: AtomicUsize,
}

impl LeafTelemetry {
    fn add(counter: &AtomicUsize, amount: usize) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    fn print(&self, name: &str) {
        let load = |counter: &AtomicUsize| counter.load(Ordering::Relaxed);
        println!(
            "leaf name={name} estimate_parent_calls={} estimate_parent_rows={} \
             estimate_target_calls={} estimate_target_rows={} \
             estimate_target_bound_rows={} low_quotes={} high_quotes={} \
             propose_parent_calls={} propose_parent_rows={} \
             propose_target_calls={} propose_target_rows={} propose_candidates={} \
             confirm_parent_calls={} confirm_parent_rows={} \
             confirm_target_calls={} confirm_target_rows={} confirm_candidates={}",
            load(&self.estimate_parent_calls),
            load(&self.estimate_parent_rows),
            load(&self.estimate_target_calls),
            load(&self.estimate_target_rows),
            load(&self.estimate_target_bound_rows),
            load(&self.low_quotes),
            load(&self.high_quotes),
            load(&self.propose_parent_calls),
            load(&self.propose_parent_rows),
            load(&self.propose_target_calls),
            load(&self.propose_target_rows),
            load(&self.propose_candidates),
            load(&self.confirm_parent_calls),
            load(&self.confirm_parent_rows),
            load(&self.confirm_target_calls),
            load(&self.confirm_target_rows),
            load(&self.confirm_candidates),
        );
    }
}

#[derive(Clone)]
struct ParentDomain {
    parent: VariableId,
    telemetry: Arc<LeafTelemetry>,
}

impl Constraint<'static> for ParentDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.parent)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.parent && !bound.is_set(variable) {
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
        if variable != self.parent {
            return false;
        }
        LeafTelemetry::add(&self.telemetry.estimate_parent_calls, 1);
        LeafTelemetry::add(&self.telemetry.estimate_parent_rows, view.len());
        out.fill(PARENTS, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.parent {
            return;
        }
        LeafTelemetry::add(&self.telemetry.propose_parent_calls, 1);
        LeafTelemetry::add(&self.telemetry.propose_parent_rows, view.len());
        let before = candidates.len();
        for row in 0..view.len() {
            candidates.extend_row(row as u32, (0..PARENTS).map(value));
        }
        LeafTelemetry::add(
            &self.telemetry.propose_candidates,
            candidates.len() - before,
        );
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.parent {
            return;
        }
        LeafTelemetry::add(&self.telemetry.confirm_parent_calls, 1);
        LeafTelemetry::add(&self.telemetry.confirm_parent_rows, view.len());
        LeafTelemetry::add(&self.telemetry.confirm_candidates, candidates.len());
        candidates
            .retain(|row, candidate| (row as usize) < view.len() && ordinal(candidate).is_some());
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.parent)
            .is_none_or(|column| view.iter().all(|row| ordinal(&row[column]).is_some()))
    }
}

#[derive(Clone, Copy)]
enum RelationSide {
    A,
    B,
}

#[derive(Clone)]
struct ExactRelation {
    side: RelationSide,
    parent: VariableId,
    target: VariableId,
    telemetry: Arc<LeafTelemetry>,
}

impl ExactRelation {
    fn quote(&self, parent: usize) -> usize {
        let a_wins = parent.is_multiple_of(2);
        match (self.side, a_wins) {
            (RelationSide::A, true) | (RelationSide::B, false) => LOW_QUOTE,
            (RelationSide::A, false) | (RelationSide::B, true) => HIGH_QUOTE,
        }
    }

    fn matching_value(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        row: &[RawInline],
    ) -> Option<RawInline> {
        let peer = if variable == self.target {
            self.parent
        } else if variable == self.parent {
            self.target
        } else {
            return None;
        };
        view.col(peer)
            .and_then(|column| ordinal(&row[column]))
            .map(value)
    }
}

impl Constraint<'static> for ExactRelation {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.parent).union(VariableSet::new_singleton(self.target))
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if self.variables().is_set(variable) && !bound.is_set(variable) {
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
        if variable != self.parent && variable != self.target {
            return false;
        }
        let (calls, rows) = if variable == self.parent {
            (
                &self.telemetry.estimate_parent_calls,
                &self.telemetry.estimate_parent_rows,
            )
        } else {
            (
                &self.telemetry.estimate_target_calls,
                &self.telemetry.estimate_target_rows,
            )
        };
        LeafTelemetry::add(calls, 1);
        LeafTelemetry::add(rows, view.len());

        if variable == self.target {
            if let Some(parent_column) = view.col(self.parent) {
                LeafTelemetry::add(&self.telemetry.estimate_target_bound_rows, view.len());
                out.extend(view.iter().map(|row| {
                    let parent = ordinal(&row[parent_column]).expect("oracle parent value");
                    let quote = self.quote(parent);
                    if quote == LOW_QUOTE {
                        LeafTelemetry::add(&self.telemetry.low_quotes, 1);
                    } else {
                        LeafTelemetry::add(&self.telemetry.high_quotes, 1);
                    }
                    quote
                }));
                return true;
            }
        } else if view.col(self.target).is_some() {
            out.fill(1, view.len());
            return true;
        }

        out.fill(UNBOUND_QUOTE, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.parent && variable != self.target {
            return;
        }
        let (calls, rows) = if variable == self.parent {
            (
                &self.telemetry.propose_parent_calls,
                &self.telemetry.propose_parent_rows,
            )
        } else {
            (
                &self.telemetry.propose_target_calls,
                &self.telemetry.propose_target_rows,
            )
        };
        LeafTelemetry::add(calls, 1);
        LeafTelemetry::add(rows, view.len());
        let before = candidates.len();
        for (row_index, row) in view.iter().enumerate() {
            if let Some(matching) = self.matching_value(variable, view, row) {
                candidates.push(row_index as u32, matching);
            } else {
                candidates.extend_row(row_index as u32, (0..PARENTS).map(value));
            }
        }
        LeafTelemetry::add(
            &self.telemetry.propose_candidates,
            candidates.len() - before,
        );
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.parent && variable != self.target {
            return;
        }
        let (calls, rows) = if variable == self.parent {
            (
                &self.telemetry.confirm_parent_calls,
                &self.telemetry.confirm_parent_rows,
            )
        } else {
            (
                &self.telemetry.confirm_target_calls,
                &self.telemetry.confirm_target_rows,
            )
        };
        LeafTelemetry::add(calls, 1);
        LeafTelemetry::add(rows, view.len());
        LeafTelemetry::add(&self.telemetry.confirm_candidates, candidates.len());
        candidates.retain(|row_index, candidate| {
            let row = view.row(row_index as usize);
            ordinal(candidate).is_some()
                && self
                    .matching_value(variable, view, row)
                    .is_none_or(|matching| matching == *candidate)
        });
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let parent = view.col(self.parent);
        let target = view.col(self.target);
        view.iter().all(|row| match (parent, target) {
            (Some(parent), Some(target)) => {
                ordinal(&row[parent]).is_some() && row[parent] == row[target]
            }
            (Some(parent), None) => ordinal(&row[parent]).is_some(),
            (None, Some(target)) => ordinal(&row[target]).is_some(),
            (None, None) => true,
        })
    }
}

fn main() {
    let domain = Arc::new(LeafTelemetry::default());
    let a = Arc::new(LeafTelemetry::default());
    let b = Arc::new(LeafTelemetry::default());

    let solved = find!(
        (parent: Inline<U256BE>, target: Inline<U256BE>),
        and!(
            ParentDomain {
                parent: parent.index,
                telemetry: Arc::clone(&domain),
            },
            ExactRelation {
                side: RelationSide::A,
                parent: parent.index,
                target: target.index,
                telemetry: Arc::clone(&a),
            },
            ExactRelation {
                side: RelationSide::B,
                parent: parent.index,
                target: target.index,
                telemetry: Arc::clone(&b),
            },
        )
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(PARENTS)
    .start_width(PARENTS)
    .growth(1)
    .shadow(ResidualShadowEpoch::new())
    .collect_profiled();

    let mut raw_rows: Vec<_> = solved
        .results
        .into_iter()
        .map(|(parent, target)| (parent.raw, target.raw))
        .collect();
    raw_rows.sort_unstable();
    let expected: Vec<_> = (0..PARENTS)
        .map(|parent| (value(parent), value(parent)))
        .collect();
    assert_eq!(raw_rows, expected, "raw projected SET differs from oracle");

    assert_eq!(
        solved.shadow.status,
        ResidualShadowStatus::Closed,
        "fully drained affine observation did not close"
    );
    let mut propose_events = 0;
    let mut confirm_events = 0;
    let mut support_events = 0;
    let mut completed_events = 0;
    let mut stale_events = 0;
    let mut aborted_events = 0;
    let mut action_parent_rows = 0;
    let mut action_candidate_occurrences = 0;
    let mut action_atoms = 0;
    for event in &solved.shadow.events {
        match event.site.verb {
            ActionVerb::Propose => propose_events += 1,
            ActionVerb::Confirm => confirm_events += 1,
            ActionVerb::Support => support_events += 1,
        }
        action_parent_rows += event.geometry.parent_rows;
        action_candidate_occurrences += event.geometry.candidate_occurrences;
        action_atoms += event.geometry.action_atoms;
        let completion = event.completion.expect("drained action must complete");
        completed_events += 1;
        stale_events += usize::from(completion.stale);
        aborted_events += usize::from(matches!(completion.outcome, ActionOutcome::Aborted));
    }
    assert_eq!(stale_events, 0);
    assert_eq!(aborted_events, 0);

    let stats = solved.stats;
    println!("revision={REVISION}");
    println!(
        "quote_certificate rows={PARENTS} a_low={} a_high={} b_low={} b_high={} \
         argmin_a={} argmin_b={} ties=0",
        PARENTS / 2,
        PARENTS / 2,
        PARENTS / 2,
        PARENTS / 2,
        PARENTS / 2,
        PARENTS / 2,
    );
    println!(
        "oracle raw_set_exact=1 raw_set_rows={} first={} last={}",
        raw_rows.len(),
        ordinal(&raw_rows.first().unwrap().0).unwrap(),
        ordinal(&raw_rows.last().unwrap().0).unwrap(),
    );
    println!(
        "affine status={:?} events={} completed={} stale={} aborted={} \
         propose_events={} confirm_events={} support_events={} \
         action_parent_rows={} action_candidate_occurrences={} action_atoms={}",
        solved.shadow.status,
        solved.shadow.events.len(),
        completed_events,
        stale_events,
        aborted_events,
        propose_events,
        confirm_events,
        support_events,
        action_parent_rows,
        action_candidate_occurrences,
        action_atoms,
    );
    println!(
        "ready ready_plan_pops={} preferred_variable_groups={} proposal_groups={}",
        stats.ready_plan_pops, stats.ready_preferred_variable_groups, stats.ready_proposal_groups,
    );
    println!(
        "geometry states_interned={} interner_hits={} bucket_merges={} rows_merged={} \
         state_pops={} state_reentries={} rows_reentered={} full_pops={} \
         readiness_pops={} continuation_pops={} partial_pops={} emit_pops={}",
        stats.states_interned,
        stats.interner_hits,
        stats.bucket_merges,
        stats.rows_merged,
        stats.state_pops,
        stats.state_reentries,
        stats.rows_reentered,
        stats.full_pops,
        stats.readiness_pops,
        stats.continuation_pops,
        stats.partial_pops,
        stats.emit_pops,
    );
    println!(
        "protocol propose_action_pops={} confirm_action_pops={} support_action_pops={} \
         dead_action_pops={} propose_calls={} propose_rows={} candidates_proposed={} \
         max_propose_rows={} max_propose_candidates={} confirm_calls={} confirm_rows={} \
         candidates_confirmed={} max_confirm_rows={} max_confirm_candidates={}",
        stats.propose_action_pops,
        stats.confirm_action_pops,
        stats.support_action_pops,
        stats.dead_action_pops,
        stats.propose_calls,
        stats.propose_rows,
        stats.candidates_proposed,
        stats.max_propose_rows,
        stats.max_propose_candidates,
        stats.confirm_calls,
        stats.confirm_rows,
        stats.candidates_confirmed,
        stats.max_confirm_rows,
        stats.max_confirm_candidates,
    );
    domain.print("domain");
    a.print("a");
    b.print("b");
}
