//! Deterministic positive-publication probe for bound-inverse and
//! same-variable RPQ Confirm routes.
//!
//! Each route is exercised with candidate occurrence zero (`B[0]`) at a near
//! positive witness, a far positive witness, or a miss. Two additional
//! bound-inverse fanout fixtures independently oppose predecessor order and
//! forward-destination order: one makes inverse Exact expensive and forward
//! Support cheap, while the other makes Exact cheap and Support expensive.
//! The production lane is compared with a harness-only Confirm-only control:
//! both use the same fallback `RegularPathConstraint` Confirm Program, while
//! the control masks only the fully-bound Support route behind
//! `ProgramExposure::Explicit`.
//!
//! Run with:
//! `cargo run --release --example rpq_inverse_samevar_positive_probe -- [nodes=4096] [reps=51] [warmups=5] [run-id] [revision] [suite=all|fanout]`

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
    Binding, CandidateSink, Constraint, DispatchClass, EstimateSink, PathOp, PreferredProgram,
    ProgramAction, ProgramExposure, ProgramRef, ProgramRequest, ProgramRoute, ProgramSeedBatch,
    ProposalCoverage, Query, RegularPathConstraint, RowsView, TypedEffectSink, TypedProgramBatch,
    TypedProgramSpec, TypedSeedSink, Variable, VariableId, VariableSet,
};
use triblespace::core::trible::{Trible, TribleSet};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::{Inline, IntoInline};

const START: VariableId = 0;
const END: VariableId = 1;
const DISTINCT_HITS: usize = 8;
const WIDTHS: [usize; 4] = [1, 4, 16, 64];
const LOWERING: ResidualLowering =
    ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::Production);

type DynConstraint<'a> = Box<dyn Constraint<'a> + 'a>;
type Root<'a> = IntersectionConstraint<DynConstraint<'a>>;
type Project = fn(&Binding) -> Option<RawInline>;
type ProbeIter<'a> = ResidualStateIter<Root<'a>, Project, RawInline>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Shape {
    BoundInverse,
    SameVariable,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Self::BoundInverse => "bound-inverse",
            Self::SameVariable => "same-variable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    PositiveNear,
    PositiveFar,
    MissFirst,
    FanoutSupportCheap,
    FanoutExactCheap,
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Self::PositiveNear => "positive-near",
            Self::PositiveFar => "positive-far",
            Self::MissFirst => "miss-first",
            Self::FanoutSupportCheap => "fanout-support-cheap",
            Self::FanoutExactCheap => "fanout-exact-cheap",
        }
    }

    fn is_positive(self) -> bool {
        !matches!(self, Self::MissFirst)
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Production,
    ExactOnly,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::ExactOnly => "exact-only",
        }
    }
}

const MODES: [Mode; 2] = [Mode::Production, Mode::ExactOnly];

struct Fixture {
    shape: Shape,
    case: Case,
    set: TribleSet,
    bound_end: Option<Inline<GenId>>,
    candidates: Vec<RawInline>,
    operations: Vec<PathOp>,
    expected: Vec<RawInline>,
    b0: RawInline,
    b0_distance: Option<usize>,
}

impl Fixture {
    fn label(&self) -> String {
        format!("{}/{}", self.shape.label(), self.case.label())
    }
}

struct OrderedCandidates<'a> {
    values: &'a [RawInline],
    gate: Option<VariableId>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Signature {
    count: usize,
    hash: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Attribution {
    positive_terminal: usize,
    positive_chunk_homomorphic: usize,
    exact_wins: usize,
    support_wins: usize,
    demand_assigned: usize,
    support_examined: usize,
    exact_examined_total: usize,
    exact_credited: usize,
    credit_retired: usize,
    direct_terminal_rows: usize,
    support_action_pops: usize,
    support_calls: usize,
    support_rows: usize,
    confirm_calls: usize,
    confirm_rows: usize,
    source_pages: usize,
    source_candidates_examined: usize,
    transition_pages: usize,
    transition_candidates_examined: usize,
    terminal_calls: usize,
    nonterminal_calls: usize,
}

impl Attribution {
    fn positive_commits(self) -> usize {
        self.positive_terminal + self.positive_chunk_homomorphic
    }

    fn credited_work(self) -> usize {
        self.demand_assigned + self.exact_credited
    }

    fn bound_slack(self) -> usize {
        self.credited_work()
            .checked_sub(self.support_examined)
            .expect("Support examined beyond D + C")
    }
}

struct Profile {
    first: RawInline,
    b0_position: Option<usize>,
    first_stats: Attribution,
    b0_stats: Option<Attribution>,
    full_stats: Attribution,
}

#[derive(Default)]
struct Samples {
    first: Vec<Duration>,
    b0: Vec<Duration>,
    full: Vec<Duration>,
}

struct BenchmarkContext<'a> {
    nodes: usize,
    reps: usize,
    warmups: usize,
    run_id: &'a str,
    revision: &'a str,
}

/// Structurally present Support route that production policy deliberately
/// defers, leaving the fallback arm's exact Confirm Program unchanged.
struct MaskedSupportRpq {
    inner: RegularPathConstraint,
}

impl TypedProgramSpec for MaskedSupportRpq {
    type State = ();
    type NoveltyKey = ();
    type Rank = ();

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        if request.action != ProgramAction::Support {
            return None;
        }
        TypedProgramSpec::route(&self.inner, request).map(|mut route| {
            route.exposure = ProgramExposure::Explicit;
            route
        })
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        unreachable!("production policy never constructs masked Support work")
    }

    fn progress(&self, _state: &Self::State) -> Self::Rank {
        unreachable!("production policy never constructs masked Support work")
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        unreachable!("production policy never seeds masked Support work")
    }

    fn step_typed(
        &self,
        _states: &mut Vec<Self::State>,
        _batch: TypedProgramBatch<'_>,
        _effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        unreachable!("production policy never steps masked Support work")
    }
}

/// Transparent Constraint wrapper around a preferred Support-only arm and the
/// ordinary RPQ fallback used for every other action.
struct PreferredSupportRpq<Preferred> {
    program: PreferredProgram<Preferred, RegularPathConstraint>,
}

impl<'a, Preferred> Constraint<'a> for PreferredSupportRpq<Preferred>
where
    Preferred: TypedProgramSpec,
{
    fn variables(&self) -> VariableSet {
        self.program.fallback().variables()
    }

    fn fixed_denotation(&self) -> bool {
        self.program.fallback().fixed_denotation()
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        self.program.fallback().proposal_coverage(variable, bound)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.program.fallback().estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program.fallback().propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program.fallback().confirm(variable, view, candidates);
    }

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.program
            .fallback()
            .estimate_certified(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program
            .fallback()
            .propose_certified(variable, view, candidates);
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.program
            .fallback()
            .confirm_certified(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.program.fallback().satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.program.fallback().influence(variable)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.program.fallback().residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_grouping_requirements(
        &self,
        variable: VariableId,
    ) -> Option<VariableSet> {
        self.program
            .fallback()
            .residual_delta_confirm_grouping_requirements(variable)
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::preferred(&self.program))
    }

    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        self.program
            .fallback()
            .residual_program_proposal_coverage(variable, bound)
    }
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

fn ordered_candidates(
    case: Case,
    near: RawInline,
    far: RawInline,
    hits: &[RawInline],
) -> Vec<RawInline> {
    let miss = value(id(250, 0)).raw;
    let first = match case {
        Case::PositiveNear => near,
        Case::PositiveFar => far,
        Case::MissFirst => miss,
        Case::FanoutSupportCheap | Case::FanoutExactCheap => {
            unreachable!("fanout fixtures construct their candidate bags directly")
        }
    };
    let mut candidates = Vec::with_capacity(hits.len() + 1);
    candidates.push(first);
    candidates.extend(hits.iter().copied().filter(|candidate| *candidate != first));
    if first != miss {
        candidates.push(miss);
    }
    candidates
}

fn build_inverse_fixtures(node_count: usize) -> Vec<Fixture> {
    let attribute = id(140, 0);
    let nodes: Vec<_> = (0..node_count).map(|index| id(120, index)).collect();
    let mut set = TribleSet::new();
    for edge in nodes.windows(2) {
        insert_edge(&mut set, edge[0], attribute, edge[1]);
    }

    let mut hits: Vec<_> = (0..DISTINCT_HITS)
        .map(|sample| {
            let index = sample * (node_count - 2) / (DISTINCT_HITS - 1);
            value(nodes[index]).raw
        })
        .collect();
    hits.sort_unstable();
    hits.dedup();
    assert_eq!(hits.len(), DISTINCT_HITS);
    let far = value(nodes[0]).raw;
    let near = value(nodes[node_count - 2]).raw;
    assert!(hits.contains(&far) && hits.contains(&near));
    let expected = hits.clone();

    [
        (Case::PositiveNear, Some(1)),
        (Case::PositiveFar, Some(node_count - 1)),
        (Case::MissFirst, None),
    ]
    .into_iter()
    .map(|(case, distance)| {
        let candidates = ordered_candidates(case, near, far, &hits);
        Fixture {
            shape: Shape::BoundInverse,
            case,
            set: set.clone(),
            bound_end: Some(value(nodes[node_count - 1])),
            b0: candidates[0],
            candidates,
            operations: vec![PathOp::Attr(attribute.raw()), PathOp::Plus],
            expected: expected.clone(),
            b0_distance: distance,
        }
    })
    .collect()
}

/// Builds a pair of direct-edge fanout fixtures whose two physical search
/// orders are deliberately independent:
///
/// - `fanout-support-cheap`: `candidate -> target` is the candidate's only
///   forward edge, but `candidate` sorts after every other predecessor of
///   `target`, so inverse Exact must scan the whole fan-in.
/// - `fanout-exact-cheap`: `candidate` sorts before every other predecessor of
///   `target`, but `target` sorts after every other destination of `candidate`,
///   so forward Support must scan the whole fan-out.
///
/// Both candidate bags put the sole positive at occurrence zero and follow it
/// with graph-absent misses. The misses force the bound-end constant to plan
/// before the candidate proposer, so the RPQ really executes its inverse
/// Confirm route rather than binding the candidate first and confirming the
/// target forward. The raw SET oracle is still exactly `{candidate}` and
/// imposes no encounter order.
fn build_inverse_fanout_fixtures(fanout: usize) -> Vec<Fixture> {
    fn candidates(positive: RawInline, miss_domain: u32) -> Vec<RawInline> {
        std::iter::once(positive)
            .chain((0..DISTINCT_HITS - 1).map(|index| value(id(miss_domain, index)).raw))
            .collect()
    }

    let attribute = id(330, 0);

    let support_target = id(334, 0);
    let support_candidate = id(333, 0);
    let support_predecessors: Vec<_> = (0..fanout).map(|index| id(332, index)).collect();
    assert!(support_predecessors
        .iter()
        .all(|predecessor| value(*predecessor).raw < value(support_candidate).raw));
    let mut support_set = TribleSet::new();
    for predecessor in support_predecessors {
        insert_edge(&mut support_set, predecessor, attribute, support_target);
    }
    insert_edge(
        &mut support_set,
        support_candidate,
        attribute,
        support_target,
    );
    let support_candidate_raw = value(support_candidate).raw;
    let support_candidates = candidates(support_candidate_raw, 390);

    let exact_candidate = id(335, 0);
    let exact_predecessors: Vec<_> = (0..fanout).map(|index| id(336, index)).collect();
    let exact_decoys: Vec<_> = (0..fanout).map(|index| id(337, index)).collect();
    let exact_target = id(338, 0);
    assert!(exact_predecessors
        .iter()
        .all(|predecessor| value(exact_candidate).raw < value(*predecessor).raw));
    assert!(exact_decoys
        .iter()
        .all(|decoy| value(*decoy).raw < value(exact_target).raw));
    let mut exact_set = TribleSet::new();
    for predecessor in exact_predecessors {
        insert_edge(&mut exact_set, predecessor, attribute, exact_target);
    }
    for decoy in exact_decoys {
        insert_edge(&mut exact_set, exact_candidate, attribute, decoy);
    }
    insert_edge(&mut exact_set, exact_candidate, attribute, exact_target);
    let exact_candidate_raw = value(exact_candidate).raw;
    let exact_candidates = candidates(exact_candidate_raw, 391);

    vec![
        Fixture {
            shape: Shape::BoundInverse,
            case: Case::FanoutSupportCheap,
            set: support_set,
            bound_end: Some(value(support_target)),
            candidates: support_candidates,
            operations: vec![PathOp::Attr(attribute.raw()), PathOp::Plus],
            expected: vec![support_candidate_raw],
            b0: support_candidate_raw,
            b0_distance: Some(1),
        },
        Fixture {
            shape: Shape::BoundInverse,
            case: Case::FanoutExactCheap,
            set: exact_set,
            bound_end: Some(value(exact_target)),
            candidates: exact_candidates,
            operations: vec![PathOp::Attr(attribute.raw()), PathOp::Plus],
            expected: vec![exact_candidate_raw],
            b0: exact_candidate_raw,
            b0_distance: Some(1),
        },
    ]
}

fn build_same_variable_fixtures(node_count: usize) -> Vec<Fixture> {
    let attribute = id(240, 0);
    let nodes: Vec<_> = (0..node_count).map(|index| id(220, index)).collect();
    let mut set = TribleSet::new();
    insert_edge(&mut set, nodes[0], attribute, nodes[0]);
    for index in 1..node_count - 1 {
        insert_edge(&mut set, nodes[index], attribute, nodes[index + 1]);
    }
    insert_edge(&mut set, nodes[node_count - 1], attribute, nodes[1]);

    let mut hits = vec![value(nodes[0]).raw];
    hits.extend((0..DISTINCT_HITS - 1).map(|sample| {
        let index = 1 + sample * (node_count - 2) / (DISTINCT_HITS - 2);
        value(nodes[index]).raw
    }));
    hits.sort_unstable();
    hits.dedup();
    assert_eq!(hits.len(), DISTINCT_HITS);
    let near = value(nodes[0]).raw;
    let far = value(nodes[1]).raw;
    assert!(hits.contains(&near) && hits.contains(&far));
    let expected = hits.clone();

    [
        (Case::PositiveNear, Some(1)),
        (Case::PositiveFar, Some(node_count - 1)),
        (Case::MissFirst, None),
    ]
    .into_iter()
    .map(|(case, distance)| {
        let candidates = ordered_candidates(case, near, far, &hits);
        Fixture {
            shape: Shape::SameVariable,
            case,
            set: set.clone(),
            bound_end: None,
            b0: candidates[0],
            candidates,
            operations: vec![PathOp::Attr(attribute.raw()), PathOp::Plus],
            expected: expected.clone(),
            b0_distance: distance,
        }
    })
    .collect()
}

impl<'a> Constraint<'a> for OrderedCandidates<'a> {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(START)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == START && !bound.is_set(START) {
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
        if variable != START {
            return false;
        }
        out.fill(
            if self.gate.is_none_or(|gate| view.col(gate).is_some()) {
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
        if variable == START {
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
        if variable == START {
            candidates.retain(|_, candidate| self.values.contains(candidate));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(START)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

fn project_start(binding: &Binding) -> Option<RawInline> {
    binding.get(START).copied()
}

fn regular_path(fixture: &Fixture) -> RegularPathConstraint {
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(match fixture.shape {
        Shape::BoundInverse => END,
        Shape::SameVariable => START,
    });
    RegularPathConstraint::new(fixture.set.clone(), start, end, &fixture.operations)
}

fn make_root<'a>(fixture: &'a Fixture, path: DynConstraint<'a>) -> Root<'a> {
    let mut constraints: Vec<DynConstraint<'a>> = Vec::new();
    if let Some(bound_end) = fixture.bound_end {
        constraints.push(Box::new(Variable::<GenId>::new(END).is(bound_end)));
    }
    constraints.push(Box::new(OrderedCandidates {
        values: &fixture.candidates,
        gate: fixture.bound_end.map(|_| END),
    }));
    constraints.push(path);
    IntersectionConstraint::new(constraints)
}

fn make_query(fixture: &Fixture, mode: Mode) -> Query<Root<'_>, Project, RawInline> {
    let fallback = regular_path(fixture);
    let path: DynConstraint<'_> = match mode {
        Mode::Production => Box::new(fallback),
        Mode::ExactOnly => {
            let preferred = MaskedSupportRpq {
                inner: regular_path(fixture),
            };
            Box::new(PreferredSupportRpq {
                program: PreferredProgram::new(preferred, fallback),
            })
        }
    };
    Query::new(make_root(fixture, path), project_start as Project)
}

fn make_iter(fixture: &Fixture, mode: Mode, width: usize) -> ProbeIter<'_> {
    make_query(fixture, mode)
        .solve_residual_state_lazy_with(LOWERING)
        .cap(width)
        .start_width(width)
        .growth(2)
}

fn signature(items: impl IntoIterator<Item = RawInline>) -> Signature {
    let mut signature = Signature::default();
    for item in items {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        signature.hash = signature.hash.wrapping_add(hasher.finish());
        signature.count += 1;
    }
    signature
}

fn attribution(stats: &ResidualStateStats) -> Attribution {
    Attribution {
        positive_terminal: stats.delta_positive_publication_terminal_commits,
        positive_chunk_homomorphic: stats.delta_positive_publication_chunk_homomorphic_commits,
        exact_wins: stats.delta_positive_publication_exact_wins,
        support_wins: stats.delta_positive_publication_support_wins,
        demand_assigned: stats.delta_positive_support_demand_assigned,
        support_examined: stats.delta_positive_support_examined,
        exact_examined_total: stats.delta_positive_support_exact_paired_examined,
        exact_credited: stats.delta_positive_support_exact_credited,
        credit_retired: stats.delta_positive_support_credit_retired,
        direct_terminal_rows: stats.delta_direct_terminal_publication_rows,
        support_action_pops: stats.support_action_pops,
        support_calls: stats.support_calls,
        support_rows: stats.support_rows,
        confirm_calls: stats.confirm_calls,
        confirm_rows: stats.confirm_rows,
        source_pages: stats.delta_source_pages,
        source_candidates_examined: stats.delta_source_candidates_examined,
        transition_pages: stats.delta_transition_pages,
        transition_candidates_examined: stats.delta_transition_candidates_examined,
        terminal_calls: stats.delta_terminal_calls,
        nonterminal_calls: stats.delta_nonterminal_calls,
    }
}

fn oracle(fixture: &Fixture) -> (Vec<RawInline>, Signature) {
    let mut actual: Vec<_> = make_query(fixture, Mode::Production).sequential().collect();
    actual.sort_unstable();
    assert_eq!(
        actual,
        fixture.expected,
        "{}: sequential oracle disagrees with fixture",
        fixture.label()
    );
    let oracle_signature = signature(actual.iter().copied());
    (actual, oracle_signature)
}

fn assert_accounting(
    fixture: &Fixture,
    mode: Mode,
    width: usize,
    phase: &str,
    stats: Attribution,
    completed: bool,
) {
    assert!(
        stats.support_examined <= stats.credited_work(),
        "{} {} width {width} {phase}: S={} exceeded D+C={}",
        fixture.label(),
        mode.label(),
        stats.support_examined,
        stats.credited_work()
    );
    assert!(
        stats.exact_credited <= stats.exact_examined_total,
        "{} {} width {width} {phase}: C={} exceeded paired Exact={}",
        fixture.label(),
        mode.label(),
        stats.exact_credited,
        stats.exact_examined_total
    );
    if completed {
        assert_eq!(
            stats.credited_work(),
            stats.support_examined + stats.credit_retired,
            "{} {} width {width}: completed hedge violated D+C=S+retired",
            fixture.label(),
            mode.label()
        );
    }
    if matches!(mode, Mode::ExactOnly) {
        assert_eq!(
            (
                stats.demand_assigned,
                stats.support_examined,
                stats.exact_examined_total,
                stats.exact_credited,
                stats.credit_retired,
                stats.support_wins,
            ),
            (0, 0, 0, 0, 0, 0),
            "{} exact-only width {width} {phase}: masked Support accrued hedge accounting",
            fixture.label()
        );
    }
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
        .unwrap_or_else(|| panic!("{} {} returned no first row", fixture.label(), mode.label()));
    let first_stats = attribution(first_iter.stats());
    assert_accounting(fixture, mode, width, "first", first_stats, false);

    let (b0_position, b0_stats) = if fixture.case.is_positive() {
        let mut b0_iter = make_iter(fixture, mode, width);
        let position = b0_iter
            .by_ref()
            .position(|value| value == fixture.b0)
            .unwrap_or_else(|| {
                panic!(
                    "{} {} width {width}: B[0] is absent",
                    fixture.label(),
                    mode.label()
                )
            });
        let stats = attribution(b0_iter.stats());
        assert_accounting(fixture, mode, width, "B0", stats, false);
        (Some(position), Some(stats))
    } else {
        (None, None)
    };

    let mut full_iter = make_iter(fixture, mode, width);
    let mut full: Vec<_> = full_iter.by_ref().collect();
    let full_stats = attribution(full_iter.stats());
    assert_accounting(fixture, mode, width, "full", full_stats, true);
    assert_eq!(
        signature(full.iter().copied()),
        oracle_signature,
        "{} {} width {width}: full signature disagrees with oracle",
        fixture.label(),
        mode.label()
    );
    full.sort_unstable();
    assert_eq!(
        full,
        oracle,
        "{} {} width {width}: full set disagrees with oracle",
        fixture.label(),
        mode.label()
    );

    match mode {
        Mode::Production if fixture.case.is_positive() => {
            assert_eq!(
                (
                    full_stats.positive_commits(),
                    full_stats.exact_wins + full_stats.support_wins,
                ),
                (1, 1),
                "{} width {width}: neither Exact nor Support published positive B[0]",
                fixture.label()
            );
            assert!(
                b0_position.is_some(),
                "{} width {width}: B[0] is absent",
                fixture.label()
            );
        }
        Mode::Production => {
            assert_eq!(
                (
                    full_stats.positive_commits(),
                    full_stats.exact_wins,
                    full_stats.support_wins,
                ),
                (0, 0, 0),
                "{} width {width}: miss B[0] fed a later candidate",
                fixture.label()
            );
        }
        Mode::ExactOnly if fixture.case.is_positive() => {
            assert_eq!(
                (
                    full_stats.positive_commits(),
                    full_stats.exact_wins,
                    full_stats.support_wins,
                ),
                (1, 1, 0),
                "{} width {width}: authoritative Exact tap did not publish positive B[0]",
                fixture.label()
            );
            assert!(
                b0_position.is_some(),
                "{} width {width}: B[0] is absent",
                fixture.label()
            );
        }
        Mode::ExactOnly => {
            assert_eq!(
                (
                    full_stats.positive_commits(),
                    full_stats.exact_wins,
                    full_stats.support_wins,
                ),
                (0, 0, 0),
                "{} width {width}: miss B[0] acquired a positive publication",
                fixture.label()
            );
        }
    }

    Profile {
        first,
        b0_position,
        first_stats,
        b0_stats,
        full_stats,
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * percentile + 50) / 100;
    sorted[index]
}

fn signed_delta(lhs: usize, rhs: usize) -> i128 {
    lhs as i128 - rhs as i128
}

fn total_nanoseconds(samples: &[Duration]) -> u128 {
    samples.iter().map(Duration::as_nanos).sum()
}

fn print_tsv_header() {
    println!(
        "schema\trun_id\trevision\tnodes\tshape\tcase\tdistance\twidth\tmode\tphase\t\
         warmups\treps\trows\tp50_ns\tp95_ns\ttotal_sample_ns\tops_per_sec\t\
         rows_per_sec\tfirst_is_b0\tpositive_total\texact_wins\tsupport_wins\t\
         demand_assigned\texact_examined_total\texact_credited\tsupport_examined\t\
         credit_retired\tbound_slack\tsource_examined\ttransition_examined\t\
         terminal_calls\tnonterminal_calls"
    );
}

#[allow(clippy::too_many_arguments)]
fn print_tsv_row(
    context: &BenchmarkContext<'_>,
    fixture: &Fixture,
    width: usize,
    mode: Mode,
    phase: &str,
    samples: &[Duration],
    rows: usize,
    first_is_b0: bool,
    stats: Attribution,
) {
    let total_ns = total_nanoseconds(samples);
    let operations_per_second = context.reps as f64 * 1_000_000_000.0 / total_ns as f64;
    let rows_per_second = context.reps as f64 * rows as f64 * 1_000_000_000.0 / total_ns as f64;
    println!(
        "rpq-hedge-v1\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t\
         {:.3}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        context.run_id,
        context.revision,
        context.nodes,
        fixture.shape.label(),
        fixture.case.label(),
        fixture
            .b0_distance
            .map_or_else(|| "n/a".to_owned(), |distance| distance.to_string()),
        width,
        mode.label(),
        phase,
        context.warmups,
        context.reps,
        rows,
        percentile(samples, 50).as_nanos(),
        percentile(samples, 95).as_nanos(),
        total_ns,
        operations_per_second,
        rows_per_second,
        first_is_b0,
        stats.positive_commits(),
        stats.exact_wins,
        stats.support_wins,
        stats.demand_assigned,
        stats.exact_examined_total,
        stats.exact_credited,
        stats.support_examined,
        stats.credit_retired,
        stats.bound_slack(),
        stats.source_candidates_examined,
        stats.transition_candidates_examined,
        stats.terminal_calls,
        stats.nonterminal_calls,
    );
}

fn measure(fixture: &Fixture, width: usize, context: &BenchmarkContext<'_>) {
    let reps = context.reps;
    let warmups = context.warmups;
    let (oracle, oracle_signature) = oracle(fixture);
    let profiles: Vec<_> = MODES
        .iter()
        .copied()
        .map(|mode| profile(fixture, mode, width, &oracle, oracle_signature))
        .collect();

    for repetition in 0..warmups {
        for offset in 0..MODES.len() {
            let index = (repetition + offset) % MODES.len();
            let mode = MODES[index];
            let mut first_iter = make_iter(fixture, mode, width);
            assert!(black_box(first_iter.next()).is_some());
            if fixture.case.is_positive() {
                let mut b0_iter = make_iter(fixture, mode, width);
                assert!(
                    black_box(b0_iter.by_ref().position(|value| value == fixture.b0)).is_some()
                );
            }
            let mut full_iter = make_iter(fixture, mode, width);
            assert_eq!(
                black_box(signature(full_iter.by_ref())),
                oracle_signature,
                "{} {} width {width}: warmup disagrees with oracle",
                fixture.label(),
                mode.label()
            );
        }
    }

    let mut samples: [Samples; MODES.len()] = std::array::from_fn(|_| Samples {
        first: Vec::with_capacity(reps),
        b0: Vec::with_capacity(reps),
        full: Vec::with_capacity(reps),
    });
    for repetition in 0..reps {
        for offset in 0..MODES.len() {
            let index = (repetition + offset) % MODES.len();
            let mode = MODES[index];

            let mut first_iter = make_iter(fixture, mode, width);
            let began = Instant::now();
            let first = black_box(first_iter.next());
            samples[index].first.push(began.elapsed());
            assert!(
                first.is_some_and(|value| oracle.contains(&value)),
                "{} {} width {width}: first row escaped the result SET",
                fixture.label(),
                mode.label()
            );

            if fixture.case.is_positive() {
                let mut b0_iter = make_iter(fixture, mode, width);
                let began = Instant::now();
                let position = black_box(b0_iter.by_ref().position(|value| value == fixture.b0));
                samples[index].b0.push(began.elapsed());
                assert!(
                    position.is_some(),
                    "{} {} width {width}: timed run never emitted B[0]",
                    fixture.label(),
                    mode.label()
                );
            }

            let mut full_iter = make_iter(fixture, mode, width);
            let began = Instant::now();
            let full_signature = black_box(signature(full_iter.by_ref()));
            samples[index].full.push(began.elapsed());
            assert_eq!(full_signature, oracle_signature);
        }
    }

    eprintln!(
        "\n{} width={width} B0={} distance={} candidates={} oracle=count:{} hash:{:016x}",
        fixture.label(),
        if fixture.case.is_positive() {
            "positive"
        } else {
            "miss"
        },
        fixture
            .b0_distance
            .map_or_else(|| "n/a".to_owned(), |distance| distance.to_string()),
        fixture.candidates.len(),
        oracle_signature.count,
        oracle_signature.hash,
    );
    for (index, mode) in MODES.iter().enumerate() {
        let profile = &profiles[index];
        eprintln!(
            "  {:<13} ttfr p50/p95 {:>10?}/{:>10?}  full p50/p95 {:>10?}/{:>10?}",
            mode.label(),
            percentile(&samples[index].first, 50),
            percentile(&samples[index].first, 95),
            percentile(&samples[index].full, 50),
            percentile(&samples[index].full, 95),
        );
        if fixture.case.is_positive() {
            eprintln!(
                "    B0 position={} first_is_B0={} time-to-B0 p50/p95 {:>10?}/{:>10?}",
                profile
                    .b0_position
                    .expect("positive fixture profile omitted B[0]"),
                profile.first == fixture.b0,
                percentile(&samples[index].b0, 50),
                percentile(&samples[index].b0, 95),
            );
        }
        eprintln!(
            "    first: positive {}/{} direct_rows {} ordinary_support calls/rows {}/{} \
             confirm calls/rows {}/{} source pages/examined {}/{} \
             transition pages/examined {}/{} dispatches terminal/nonterminal {}/{} \
             D/C/S/retired {}/{}/{}/{} wins exact/support {}/{}",
            profile.first_stats.positive_terminal,
            profile.first_stats.positive_chunk_homomorphic,
            profile.first_stats.direct_terminal_rows,
            profile.first_stats.support_calls,
            profile.first_stats.support_rows,
            profile.first_stats.confirm_calls,
            profile.first_stats.confirm_rows,
            profile.first_stats.source_pages,
            profile.first_stats.source_candidates_examined,
            profile.first_stats.transition_pages,
            profile.first_stats.transition_candidates_examined,
            profile.first_stats.terminal_calls,
            profile.first_stats.nonterminal_calls,
            profile.first_stats.demand_assigned,
            profile.first_stats.exact_credited,
            profile.first_stats.support_examined,
            profile.first_stats.credit_retired,
            profile.first_stats.exact_wins,
            profile.first_stats.support_wins,
        );
        eprintln!(
            "    full:  positive {}/{} direct_rows {} ordinary_support calls/rows {}/{} \
             confirm calls/rows {}/{} source pages/examined {}/{} \
             transition pages/examined {}/{} dispatches terminal/nonterminal {}/{} \
             D/C/S/retired {}/{}/{}/{} wins exact/support {}/{}",
            profile.full_stats.positive_terminal,
            profile.full_stats.positive_chunk_homomorphic,
            profile.full_stats.direct_terminal_rows,
            profile.full_stats.support_calls,
            profile.full_stats.support_rows,
            profile.full_stats.confirm_calls,
            profile.full_stats.confirm_rows,
            profile.full_stats.source_pages,
            profile.full_stats.source_candidates_examined,
            profile.full_stats.transition_pages,
            profile.full_stats.transition_candidates_examined,
            profile.full_stats.terminal_calls,
            profile.full_stats.nonterminal_calls,
            profile.full_stats.demand_assigned,
            profile.full_stats.exact_credited,
            profile.full_stats.support_examined,
            profile.full_stats.credit_retired,
            profile.full_stats.exact_wins,
            profile.full_stats.support_wins,
        );

        let first_is_b0 = profile.first == fixture.b0;
        print_tsv_row(
            context,
            fixture,
            width,
            *mode,
            "first",
            &samples[index].first,
            1,
            first_is_b0,
            profile.first_stats,
        );
        if fixture.case.is_positive() {
            print_tsv_row(
                context,
                fixture,
                width,
                *mode,
                "b0",
                &samples[index].b0,
                profile
                    .b0_position
                    .expect("positive fixture profile omitted B[0]")
                    + 1,
                first_is_b0,
                profile
                    .b0_stats
                    .expect("positive fixture profile omitted B[0] accounting"),
            );
        }
        print_tsv_row(
            context,
            fixture,
            width,
            *mode,
            "full",
            &samples[index].full,
            oracle_signature.count,
            first_is_b0,
            profile.full_stats,
        );
    }

    let production = &profiles[0];
    let confirm_only = &profiles[1];
    eprintln!(
        "  production - exact-only full actual-work delta: \
         source_pages {:+} source_examined {:+} transition_pages {:+} \
         transition_examined {:+} terminal_dispatches {:+} nonterminal_dispatches {:+}",
        signed_delta(
            production.full_stats.source_pages,
            confirm_only.full_stats.source_pages
        ),
        signed_delta(
            production.full_stats.source_candidates_examined,
            confirm_only.full_stats.source_candidates_examined
        ),
        signed_delta(
            production.full_stats.transition_pages,
            confirm_only.full_stats.transition_pages
        ),
        signed_delta(
            production.full_stats.transition_candidates_examined,
            confirm_only.full_stats.transition_candidates_examined
        ),
        signed_delta(
            production.full_stats.terminal_calls,
            confirm_only.full_stats.terminal_calls
        ),
        signed_delta(
            production.full_stats.nonterminal_calls,
            confirm_only.full_stats.nonterminal_calls
        ),
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let node_count = args
        .get(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(4_096);
    let reps = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(51);
    let warmups = args.get(3).and_then(|arg| arg.parse().ok()).unwrap_or(5);
    let run_id = args.get(4).map(String::as_str).unwrap_or("run-1");
    let revision = args.get(5).map(String::as_str).unwrap_or("unknown");
    let suite = args.get(6).map(String::as_str).unwrap_or("all");
    assert!(
        node_count >= DISTINCT_HITS * 2,
        "nodes must leave room for distinct sampled witnesses"
    );
    assert!(reps > 0, "reps must be positive");

    let fixtures = match suite {
        "all" => {
            let mut fixtures = build_inverse_fixtures(node_count);
            fixtures.extend(build_inverse_fanout_fixtures(node_count));
            fixtures.extend(build_same_variable_fixtures(node_count));
            fixtures
        }
        "fanout" => build_inverse_fanout_fixtures(node_count),
        _ => panic!("unknown suite {suite:?}; expected all or fanout"),
    };
    let context = BenchmarkContext {
        nodes: node_count,
        reps,
        warmups,
        run_id,
        revision,
    };
    print_tsv_header();
    eprintln!(
        "RPQ inverse/same-variable positive-publication probe: \
         nodes={node_count} reps={reps} warmups={warmups} \
         distinct_hits={DISTINCT_HITS} widths={WIDTHS:?} suite={suite} \
         run={run_id} revision={revision}"
    );
    eprintln!(
        "production is the unwrapped OpaqueLeaves+Production lane; exact-only keeps \
         the same fallback exact Confirm Program but policy-defers only Support."
    );
    for fixture in &fixtures {
        for width in WIDTHS {
            measure(fixture, width, &context);
        }
    }
}
