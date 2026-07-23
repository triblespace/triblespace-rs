//! Deterministic positive-publication probe for bound-inverse and
//! same-variable RPQ Confirm routes.
//!
//! Each route is exercised with candidate occurrence zero (`B[0]`) at a near
//! positive witness, a far positive witness, or a miss. The production lane
//! is compared with a harness-only Confirm-only control: both use the same
//! fallback `RegularPathConstraint` Confirm Program, while the control masks
//! only the fully-bound Support route behind `ProgramExposure::Explicit`.
//!
//! A separate untimed support-trace lane delegates Support to an otherwise
//! identical `RegularPathConstraint` and counts its real typed seed/step
//! calls, input states, and granted work. This establishes whether Support
//! does any work after the first published positive without changing engine
//! code or the timed production lane.
//!
//! Run with:
//! `cargo run --release --example rpq_inverse_samevar_positive_probe -- [nodes=4096] [reps=9]`

use std::hash::{DefaultHasher, Hash, Hasher};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use triblespace::core::id::Id;
use triblespace::core::inline::RawInline;
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::regularpathconstraint::{RpqNoveltyKey, RpqState};
use triblespace::core::query::residual::{
    FormulaScope, ProgramScope, ResidualLowering, ResidualStateIter, ResidualStateStats,
};
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, DispatchClass, EstimateSink, PathOp, PreferredProgram,
    ProgramAction, ProgramExposure, ProgramPacing, ProgramRef, ProgramRequest, ProgramRoute,
    ProgramSeedBatch, ProposalCoverage, Query, RegularPathConstraint, RowsView, TypedEffectSink,
    TypedProgramBatch, TypedProgramSpec, TypedSeedSink, Variable, VariableId, VariableSet,
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
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Self::PositiveNear => "positive-near",
            Self::PositiveFar => "positive-far",
            Self::MissFirst => "miss-first",
        }
    }

    fn is_positive(self) -> bool {
        !matches!(self, Self::MissFirst)
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Production,
    ConfirmOnly,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::ConfirmOnly => "confirm-only",
        }
    }
}

const MODES: [Mode; 2] = [Mode::Production, Mode::ConfirmOnly];

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
}

struct Profile {
    first: RawInline,
    first_stats: Attribution,
    full_stats: Attribution,
}

#[derive(Default)]
struct Samples {
    first: Vec<Duration>,
    full: Vec<Duration>,
}

#[derive(Default)]
struct SupportTrace {
    seed_calls: AtomicUsize,
    seed_rows: AtomicUsize,
    step_calls: AtomicUsize,
    step_inputs: AtomicUsize,
    granted_work: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SupportTraceSnapshot {
    seed_calls: usize,
    seed_rows: usize,
    step_calls: usize,
    step_inputs: usize,
    granted_work: usize,
}

impl SupportTrace {
    fn snapshot(&self) -> SupportTraceSnapshot {
        SupportTraceSnapshot {
            seed_calls: self.seed_calls.load(Ordering::Relaxed),
            seed_rows: self.seed_rows.load(Ordering::Relaxed),
            step_calls: self.step_calls.load(Ordering::Relaxed),
            step_inputs: self.step_inputs.load(Ordering::Relaxed),
            granted_work: self.granted_work.load(Ordering::Relaxed),
        }
    }
}

impl std::ops::Sub for SupportTraceSnapshot {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            seed_calls: self.seed_calls - rhs.seed_calls,
            seed_rows: self.seed_rows - rhs.seed_rows,
            step_calls: self.step_calls - rhs.step_calls,
            step_inputs: self.step_inputs - rhs.step_inputs,
            granted_work: self.granted_work - rhs.granted_work,
        }
    }
}

/// Production-equivalent RPQ Support arm with harness-only counters.
struct TracedSupportRpq {
    inner: RegularPathConstraint,
    trace: Arc<SupportTrace>,
}

impl TypedProgramSpec for TracedSupportRpq {
    type State = RpqState;
    type NoveltyKey = RpqNoveltyKey;
    type Rank = [u64; 8];

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        if request.action == ProgramAction::Support {
            TypedProgramSpec::route(&self.inner, request)
        } else {
            None
        }
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        TypedProgramSpec::dispatch(&self.inner, state)
    }

    fn pacing(&self, state: &Self::State) -> ProgramPacing {
        TypedProgramSpec::pacing(&self.inner, state)
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        TypedProgramSpec::progress(&self.inner, state)
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.request.action, ProgramAction::Support);
        self.trace.seed_calls.fetch_add(1, Ordering::Relaxed);
        self.trace
            .seed_rows
            .fetch_add(batch.view.len(), Ordering::Relaxed);
        TypedProgramSpec::seed_typed(&self.inner, batch, effects);
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        self.trace.step_calls.fetch_add(1, Ordering::Relaxed);
        self.trace
            .step_inputs
            .fetch_add(states.len(), Ordering::Relaxed);
        self.trace
            .granted_work
            .fetch_add(batch.limits.iter().sum::<usize>(), Ordering::Relaxed);
        TypedProgramSpec::step_typed(&self.inner, states, batch, effects);
    }
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
        Mode::ConfirmOnly => {
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

fn make_traced_query(
    fixture: &Fixture,
    trace: Arc<SupportTrace>,
) -> Query<Root<'_>, Project, RawInline> {
    let fallback = regular_path(fixture);
    let preferred = TracedSupportRpq {
        inner: regular_path(fixture),
        trace,
    };
    let path: DynConstraint<'_> = Box::new(PreferredSupportRpq {
        program: PreferredProgram::new(preferred, fallback),
    });
    Query::new(make_root(fixture, path), project_start as Project)
}

fn make_iter(fixture: &Fixture, mode: Mode, width: usize) -> ProbeIter<'_> {
    make_query(fixture, mode)
        .solve_residual_state_lazy_with(LOWERING)
        .cap(width)
        .start_width(width)
        .growth(2)
}

fn make_traced_iter(fixture: &Fixture, trace: Arc<SupportTrace>, width: usize) -> ProbeIter<'_> {
    make_traced_query(fixture, trace)
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

    let mut full_iter = make_iter(fixture, mode, width);
    let mut full: Vec<_> = full_iter.by_ref().collect();
    let full_stats = attribution(full_iter.stats());
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
                first_stats.positive_commits(),
                1,
                "{} width {width}: Support did not publish positive B[0]",
                fixture.label()
            );
            assert_eq!(
                first,
                fixture.b0,
                "{} width {width}: positive publication did not emit B[0]",
                fixture.label()
            );
        }
        Mode::Production => {
            assert_eq!(
                full_stats.positive_commits(),
                0,
                "{} width {width}: miss B[0] fed a later candidate",
                fixture.label()
            );
        }
        Mode::ConfirmOnly => {
            assert_eq!(
                full_stats.positive_commits(),
                0,
                "{} width {width}: masked Support still published",
                fixture.label()
            );
        }
    }

    Profile {
        first,
        first_stats,
        full_stats,
    }
}

struct SupportAudit {
    first: SupportTraceSnapshot,
    full_after_first: SupportTraceSnapshot,
    first_stats: Attribution,
    full_stats: Attribution,
}

fn support_audit(
    fixture: &Fixture,
    width: usize,
    oracle: &[RawInline],
    oracle_signature: Signature,
) -> SupportAudit {
    let trace = Arc::new(SupportTrace::default());
    let mut query = make_traced_iter(fixture, Arc::clone(&trace), width);
    let first = query
        .next()
        .unwrap_or_else(|| panic!("{} traced Support returned no row", fixture.label()));
    let first_trace = trace.snapshot();
    let first_stats = attribution(query.stats());
    let mut full = vec![first];
    full.extend(query.by_ref());
    let full_trace = trace.snapshot();
    let full_stats = attribution(query.stats());
    assert_eq!(signature(full.iter().copied()), oracle_signature);
    full.sort_unstable();
    assert_eq!(full, oracle);
    assert_eq!(
        first_trace.seed_calls,
        1,
        "{} width {width}: expected one occurrence-zero Support seed",
        fixture.label()
    );
    assert_eq!(
        full_trace.seed_calls,
        1,
        "{} width {width}: Support fed beyond occurrence zero",
        fixture.label()
    );
    SupportAudit {
        first: first_trace,
        full_after_first: full_trace - first_trace,
        first_stats,
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

fn measure(fixture: &Fixture, width: usize, reps: usize) {
    let (oracle, oracle_signature) = oracle(fixture);
    let profiles: Vec<_> = MODES
        .iter()
        .copied()
        .map(|mode| profile(fixture, mode, width, &oracle, oracle_signature))
        .collect();
    let support = support_audit(fixture, width, &oracle, oracle_signature);
    assert_eq!(
        support.first_stats,
        profiles[0].first_stats,
        "{} width {width}: traced Support changed first-result engine statistics",
        fixture.label()
    );
    assert_eq!(
        support.full_stats,
        profiles[0].full_stats,
        "{} width {width}: traced Support changed full-drain engine statistics",
        fixture.label()
    );

    let mut samples: [Samples; MODES.len()] = std::array::from_fn(|_| Samples {
        first: Vec::with_capacity(reps),
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
            assert_eq!(first, Some(profiles[index].first));

            let mut full_iter = make_iter(fixture, mode, width);
            let began = Instant::now();
            let full_signature = black_box(signature(full_iter.by_ref()));
            samples[index].full.push(began.elapsed());
            assert_eq!(full_signature, oracle_signature);
        }
    }

    println!(
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
        println!(
            "  {:<13} ttfr p50/p95 {:>10?}/{:>10?}  full p50/p95 {:>10?}/{:>10?}",
            mode.label(),
            percentile(&samples[index].first, 50),
            percentile(&samples[index].first, 95),
            percentile(&samples[index].full, 50),
            percentile(&samples[index].full, 95),
        );
        println!(
            "    first: positive {}/{} direct_rows {} ordinary_support calls/rows {}/{} \
             confirm calls/rows {}/{} source pages/examined {}/{} \
             transition pages/examined {}/{} dispatches terminal/nonterminal {}/{}",
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
        );
        println!(
            "    full:  positive {}/{} direct_rows {} ordinary_support calls/rows {}/{} \
             confirm calls/rows {}/{} source pages/examined {}/{} \
             transition pages/examined {}/{} dispatches terminal/nonterminal {}/{}",
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
        );
    }

    let production = &profiles[0];
    let confirm_only = &profiles[1];
    println!(
        "  support trace at first: seeds/rows {}/{} steps/inputs {} / {} granted_work {}; \
         after first: seeds/rows {}/{} steps/inputs {} / {} granted_work {}",
        support.first.seed_calls,
        support.first.seed_rows,
        support.first.step_calls,
        support.first.step_inputs,
        support.first.granted_work,
        support.full_after_first.seed_calls,
        support.full_after_first.seed_rows,
        support.full_after_first.step_calls,
        support.full_after_first.step_inputs,
        support.full_after_first.granted_work,
    );
    println!(
        "  production - confirm-only full actual-work delta: \
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
    let reps = args.get(2).and_then(|arg| arg.parse().ok()).unwrap_or(9);
    assert!(
        node_count >= DISTINCT_HITS * 2,
        "nodes must leave room for distinct sampled witnesses"
    );
    assert!(reps > 0, "reps must be positive");

    let mut fixtures = build_inverse_fixtures(node_count);
    fixtures.extend(build_same_variable_fixtures(node_count));
    println!(
        "RPQ inverse/same-variable positive-publication probe: \
         nodes={node_count} reps={reps} distinct_hits={DISTINCT_HITS} widths={WIDTHS:?}"
    );
    println!(
        "production is the unwrapped OpaqueLeaves+Production lane; confirm-only keeps \
         the same fallback exact Confirm Program but policy-defers only Support."
    );
    println!(
        "ordinary support_calls excludes the internal occurrence-zero Support feeder; \
         the support-trace line counts that real typed RPQ arm directly."
    );
    for fixture in &fixtures {
        for width in WIDTHS {
            measure(fixture, width, reps);
        }
    }
}
