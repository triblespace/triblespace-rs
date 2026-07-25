//! Measures the recurrent RPQ Machine path under a demand-wide parent cohort.
//!
//! The fixture is the production regression shape reduced to one 16-node
//! component: eight distinct hidden parents carry the same bound path source
//! through `(p | q)+`. Projecting only `(source, target)` must collapse those
//! witnesses to the 16 reachable endpoints. The workload is deliberately
//! small enough to preserve TTFR sensitivity while still forcing a
//! demand-wide admission (`max_admission_parents > 1`). It reports both
//! first-result latency and full-drain latency, plus the sparse activation and
//! paging profile that explains either result.
//!
//! ```text
//! cargo run --release --example rpq_bounded_completion_bench -- 31
//! ```
//!
//! Set `ENGINE_REVISION` while compiling to put the exact revision in the
//! emitted TSV.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use triblespace::core::debug::query::EstimateOverrideConstraint;
use triblespace::core::id::{ExclusiveId, Id, ID_LEN};
use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::inline::{Inline, IntoInline, RawInline};
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::residual::ResidualStateIter;
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, PathOp, ProposalCoverage, Query,
    RegularPathConstraint, RowsView, Variable, VariableId, VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};

const REVISION: &str = match option_env!("ENGINE_REVISION") {
    Some(revision) => revision,
    None => "unknown",
};
const RING_SIZE: usize = 16;
const HIDDEN_PARENTS: usize = 8;

type Pair = (RawInline, RawInline);
type ShapeConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = IntersectionConstraint<ShapeConstraint>;
type Project = fn(&Binding) -> Option<Pair>;
type Solver = ResidualStateIter<Root, Project, Pair>;

#[derive(Clone)]
struct FanoutLeaf {
    variable: VariableId,
    values: Arc<Vec<RawInline>>,
}

impl Constraint<'static> for FanoutLeaf {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
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
        out.fill(self.values.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, self.variable);
        for row in 0..view.len() {
            candidates.extend_row(row as u32, self.values.iter().copied());
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|_, value| self.values.contains(value));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

struct Fixture {
    graph: TribleSet,
    nodes: Vec<Id>,
    p: Id,
    q: Id,
}

impl Fixture {
    fn new() -> Self {
        let p = Id::new([221; ID_LEN]).unwrap();
        let q = Id::new([222; ID_LEN]).unwrap();
        let nodes = (1..=RING_SIZE)
            .map(|ordinal| Id::new([u8::try_from(ordinal).unwrap(); ID_LEN]).unwrap())
            .collect::<Vec<_>>();
        let mut graph = TribleSet::new();

        for (position, node) in nodes.iter().enumerate() {
            for offset in 1..=2 {
                insert_edge(
                    &mut graph,
                    node,
                    &p,
                    &nodes[(position + offset) % RING_SIZE],
                );
                insert_edge(
                    &mut graph,
                    node,
                    &q,
                    &nodes[(position + 2 + offset) % RING_SIZE],
                );
            }
        }

        Self { graph, nodes, p, q }
    }

    fn root(&self) -> Root {
        let source = id_value(&self.nodes[0]);
        let mut discriminators = EstimateOverrideConstraint::new(FanoutLeaf {
            variable: 2,
            values: Arc::new((0..HIDDEN_PARENTS).map(raw).collect()),
        });
        // Bind every hidden witness before opening the repeated path while
        // preserving the production planner and scheduler unchanged.
        discriminators.set_estimate(2, 1);
        let path = RegularPathConstraint::new(
            self.graph.clone(),
            Variable::<GenId>::new(0),
            Variable::<GenId>::new(1),
            &[
                PathOp::Attr(self.p.raw()),
                PathOp::Attr(self.q.raw()),
                PathOp::Union,
                PathOp::Plus,
            ],
        );

        IntersectionConstraint::new(vec![
            Box::new(FanoutLeaf {
                variable: 0,
                values: Arc::new(vec![source]),
            }) as ShapeConstraint,
            Box::new(discriminators) as ShapeConstraint,
            Box::new(path) as ShapeConstraint,
        ])
    }

    fn oracle(&self) -> Vec<Pair> {
        let source = id_value(&self.nodes[0]);
        self.nodes
            .iter()
            .map(|target| (source, id_value(target)))
            .collect()
    }
}

fn insert_edge(graph: &mut TribleSet, from: &Id, attribute: &Id, to: &Id) {
    graph.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        attribute,
        &to.to_inline(),
    ));
}

fn raw(ordinal: usize) -> RawInline {
    let mut value = [0; 32];
    value[0] = u8::try_from(ordinal + 128).unwrap();
    value
}

fn id_value(id: &Id) -> RawInline {
    let inline: Inline<GenId> = id.to_inline();
    inline.raw
}

fn project(binding: &Binding) -> Option<Pair> {
    Some((binding.get(0).copied()?, binding.get(1).copied()?))
}

fn solver(fixture: &Fixture) -> Solver {
    Query::new_projected(fixture.root(), [0, 1], project as Project).solve_residual_state_lazy()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Signature {
    rows: usize,
    digest: [u8; 32],
}

fn signature(rows: impl IntoIterator<Item = Pair>) -> Signature {
    let mut records = rows
        .into_iter()
        .map(|(source, target)| {
            let mut record = [0; 64];
            record[..32].copy_from_slice(&source);
            record[32..].copy_from_slice(&target);
            record
        })
        .collect::<Vec<_>>();
    records.sort_unstable();
    records.dedup();

    let mut hasher = blake3::Hasher::new();
    for record in &records {
        hasher.update(record);
    }
    Signature {
        rows: records.len(),
        digest: *hasher.finalize().as_bytes(),
    }
}

fn digest_prefix(digest: &[u8; 32]) -> String {
    use std::fmt::Write;

    let mut result = String::with_capacity(32);
    for byte in &digest[..16] {
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

fn benchmark_ttfr(fixture: &Fixture, reps: usize) -> f64 {
    black_box(solver(fixture).next().expect("fixture must yield a path"));
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let mut query = solver(fixture);
        black_box(query.next().expect("fixture must yield a path"));
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    }
    median(&samples)
}

fn benchmark_drain(fixture: &Fixture, expected_rows: usize, reps: usize) -> f64 {
    black_box(solver(fixture).count());
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let start = Instant::now();
        let rows = solver(fixture).inspect(|row| {
            black_box(row);
        });
        assert_eq!(rows.count(), expected_rows);
        samples.push(start.elapsed().as_secs_f64() * 1e6);
    }
    median(&samples)
}

fn parse_reps() -> usize {
    std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(31)
}

fn main() {
    let reps = parse_reps();
    assert!(reps > 0 && reps % 2 == 1, "reps must be positive and odd");

    let fixture = Fixture::new();
    let oracle = signature(fixture.oracle());
    let profiled = solver(&fixture).collect_profiled();
    let observed = signature(profiled.results.iter().copied());
    assert_eq!(observed, oracle, "RPQ SET result differs from ring oracle");
    assert_eq!(
        profiled.results.len(),
        oracle.rows,
        "hidden parent witnesses did not collapse under SET projection"
    );
    assert!(
        profiled.stats.delta_terminal_demand_wide_admissions > 0
            && profiled.stats.max_delta_terminal_admission_parents > 1,
        "fixture failed to admit multiple RPQ parents: {:#?}",
        profiled.stats
    );
    let ttfr_us = benchmark_ttfr(&fixture, reps);
    let drain_us = benchmark_drain(&fixture, oracle.rows, reps);
    let stats = &profiled.stats;

    println!(
        "revision\tring\thidden_parents\treps\trows\tdigest16\t\
         ttfr_p50_us\tdrain_p50_us\twide_admissions\tmax_admission_parents\t\
         terminal_calls\tterminal_tasks\tactivations_completed\tactive_lease_steps\t\
         source_pages\tsource_candidates\ttransition_pages\ttransition_candidates"
    );
    println!(
        "{REVISION}\t{RING_SIZE}\t{HIDDEN_PARENTS}\t{reps}\t{}\t{}\t\
         {ttfr_us:.3}\t{drain_us:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        oracle.rows,
        digest_prefix(&oracle.digest),
        stats.delta_terminal_demand_wide_admissions,
        stats.max_delta_terminal_admission_parents,
        stats.delta_terminal_calls,
        stats.delta_terminal_tasks,
        stats.delta_activations_completed,
        stats.delta_active_lease_steps,
        stats.delta_source_pages,
        stats.delta_source_candidates_examined,
        stats.delta_transition_pages,
        stats.delta_transition_candidates_examined,
    );
}
