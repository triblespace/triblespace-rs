use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace_core::id::{rngid, ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::{
    ActionVerb, FormulaScope, ProgramScope, ResidualLowering, ResidualShadowEpoch,
};
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, DispatchClass, EstimateSink, PathOp, PreferredProgram,
    ProgramAction, ProgramRef, ProgramRequest, ProgramRoute, ProgramSeedBatch, ProposalCoverage,
    Query, RegularPathConstraint, RowsView, TypedEffectSink, TypedProgramBatch, TypedProgramSpec,
    TypedSeedSink, Variable, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const START: VariableId = 0;
const END: VariableId = 1;
const OUTER: VariableId = 2;
const PARENT: VariableId = 3;

type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = Arc<IntersectionConstraint<DynConstraint>>;

struct Graph {
    set: TribleSet,
    nodes: Vec<ExclusiveId>,
    attribute: Id,
}

impl Graph {
    fn new(node_count: usize, edges: &[(usize, usize)]) -> Self {
        let nodes: Vec<_> = (0..node_count).map(|_| rngid()).collect();
        let attribute = path_attribute();
        let mut set = TribleSet::new();
        for &(from, to) in edges {
            set.insert(&Trible::new(
                &nodes[from],
                &attribute,
                &genid(&nodes[to].id),
            ));
        }
        Self {
            set,
            nodes,
            attribute,
        }
    }

    fn value(&self, node: usize) -> Inline<GenId> {
        genid(&self.nodes[node].id)
    }
}

/// A fixed-ID graph family whose levels form an inclusion chain.
///
/// Keeping both the node universe and insertion order stable makes failures in
/// the generated scheduler differential exactly reproducible. Every level
/// retains a secondary-attribute cycle so negated-attribute paths remain
/// non-vacuous, while the primary relation grows by one fact at a time.
struct GeneratedGraph {
    set: TribleSet,
    nodes: [Id; 4],
    primary: Id,
    secondary: Id,
}

impl GeneratedGraph {
    fn new(level: usize) -> Self {
        assert!(level <= 4);
        // Minted specifically for this fixture with `trible genid`.
        let nodes = [
            Id::from_hex("40A990B5D501DC534BDA07DB6E778241").expect("minted fixture ID"),
            Id::from_hex("0D277C88E14918C895DEB43BFE63D17D").expect("minted fixture ID"),
            Id::from_hex("2421654939E557C23EB17CD1ACCFE299").expect("minted fixture ID"),
            Id::from_hex("E5DAB28D2715F85707D3DE7ADA584384").expect("minted fixture ID"),
        ];
        let primary = path_attribute();
        let secondary = other_attribute();
        let mut set = TribleSet::new();
        let mut insert = |from: usize, attribute: &Id, to: usize| {
            set.insert(&Trible::new(
                ExclusiveId::force_ref(&nodes[from]),
                attribute,
                &genid(&nodes[to]),
            ));
        };

        // A stable secondary cycle fixes NODES(G) and gives !primary a
        // meaningful repeated path at every generated level.
        for from in 0..nodes.len() {
            insert(from, &secondary, (from + 1) % nodes.len());
        }
        insert(0, &primary, 1);
        let additions = [
            (1, primary, 0),
            (1, primary, 2),
            (0, secondary, 2),
            (2, primary, 3),
        ];
        for &(from, attribute, to) in additions.iter().take(level) {
            insert(from, &attribute, to);
        }

        Self {
            set,
            nodes,
            primary,
            secondary,
        }
    }

    fn value(&self, node: usize) -> Inline<GenId> {
        genid(&self.nodes[node])
    }
}

fn genid(id: &Id) -> Inline<GenId> {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&id[..]);
    Inline::new(value)
}

fn path_attribute() -> Id {
    Id::new([
        0xD6, 0x5F, 0xF7, 0xBC, 0x33, 0x6E, 0x47, 0x33, 0xD2, 0xEF, 0xA0, 0x9F, 0x38, 0x09, 0x6E,
        0x31,
    ])
    .expect("minted nonzero attribute")
}

fn other_attribute() -> Id {
    Id::new([
        0x4C, 0xEC, 0x06, 0xD5, 0x51, 0xFA, 0xCF, 0x4B, 0xAF, 0xBA, 0x7A, 0x59, 0xA3, 0x50, 0x49,
        0xCE,
    ])
    .expect("minted nonzero attribute")
}

fn later_attribute() -> Id {
    // Minted specifically for the negated-page suffix-order receipt with
    // `trible genid`; it sorts after `path_attribute`.
    Id::from_hex("D70F75EC4AFF404F5FFE681EF0F0D5A4").expect("minted fixture attribute")
}

fn third_attribute() -> Id {
    // Minted specifically for the distinct-destination receipt with
    // `trible genid`.
    Id::from_hex("17D7A00087D912B14F8BC28AFC31474F").expect("minted fixture attribute")
}

struct SatisfiedCountingPath {
    inner: RegularPathConstraint,
    fully_bound_satisfied_calls: Arc<AtomicUsize>,
}

impl<'a> Constraint<'a> for SatisfiedCountingPath {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.propose(variable, view, candidates)
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        if self
            .inner
            .variables()
            .into_iter()
            .all(|variable| view.col(variable).is_some())
        {
            self.fully_bound_satisfied_calls
                .fetch_add(view.len(), Ordering::Relaxed);
        }
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        self.inner.residual_program()
    }
}

#[derive(Clone)]
struct DuplicateParents {
    outer_values: [RawInline; 2],
    start: RawInline,
}

impl<'a> Constraint<'a> for DuplicateParents {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(OUTER).union(VariableSet::new_singleton(START))
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        match variable {
            OUTER => out.fill(1, view.len()),
            // Force OUTER first, then create one identical START occurrence
            // for each distinct outer row. This is a bag-multiplicity oracle,
            // not a duplicate candidate-set oracle.
            START => out.fill(if view.col(OUTER).is_some() { 1 } else { 4 }, view.len()),
            _ => return false,
        }
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        for row in 0..view.len() {
            match variable {
                OUTER => candidates.extend_row(row as u32, self.outer_values),
                START => candidates.push(row as u32, self.start),
                _ => {}
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        match variable {
            OUTER => candidates.retain(|_, value| self.outer_values.contains(value)),
            START => candidates.retain(|_, value| *value == self.start),
            _ => {}
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let outer_ok = view.col(OUTER).is_none_or(|column| {
            view.iter()
                .all(|row| self.outer_values.contains(&row[column]))
        });
        let start_ok = view
            .col(START)
            .is_none_or(|column| view.iter().all(|row| row[column] == self.start));
        outer_ok && start_ok
    }
}

#[derive(Clone)]
struct OrderedDomain {
    variable: VariableId,
    gate: VariableId,
    unbound_estimate: usize,
    values: Vec<RawInline>,
}

impl<'a> Constraint<'a> for OrderedDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
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
        // Let the opposite endpoint bind first, then deliberately win the
        // proposer choice so the RPQ is exercised as a grouped confirmer.
        out.fill(
            if view.col(self.gate).is_some() {
                1
            } else {
                self.unbound_estimate
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
            candidates.retain(|_, candidate| self.values.contains(candidate));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

#[derive(Clone)]
struct CertifiedOrderedDomain(OrderedDomain);

impl<'a> Constraint<'a> for CertifiedOrderedDomain {
    fn variables(&self) -> VariableSet {
        self.0.variables()
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.0.variable && !bound.is_set(variable) {
            ProposalCoverage::Covering
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
        self.0.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates);
    }

    fn estimate_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.estimate(variable, view, out)
    }

    fn propose_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates);
    }

    fn confirm_certified(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct SupportRouteProbe {
    calls: Arc<AtomicUsize>,
}

impl TypedProgramSpec for SupportRouteProbe {
    type State = ();
    type NoveltyKey = ();
    type Rank = ();

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        if request.action == ProgramAction::Support {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        unreachable!("the route-only Support probe never owns work")
    }

    fn progress(&self, _state: &Self::State) -> Self::Rank {
        unreachable!("the route-only Support probe never owns work")
    }

    fn seed_typed(
        &self,
        _batch: ProgramSeedBatch<'_>,
        _effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        unreachable!("the route-only Support probe is never seeded")
    }

    fn step_typed(
        &self,
        _states: &mut Vec<Self::State>,
        _batch: TypedProgramBatch<'_>,
        _effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        unreachable!("the route-only Support probe is never stepped")
    }
}

/// Observes Support route selection without changing the selected RPQ arm.
///
/// The preferred arm always declines after recording the request, so the real
/// RPQ remains the exact typed executor. Most target-Confirm fixtures suppress
/// its covering proposal certificate to isolate confirmation; the partial
/// fixture retains it so the remaining endpoint can still be enumerated.
struct ProbedConfirmRpq {
    program: PreferredProgram<SupportRouteProbe, RegularPathConstraint>,
    covering_proposals: bool,
}

impl<'a> Constraint<'a> for ProbedConfirmRpq {
    fn variables(&self) -> VariableSet {
        self.program.fallback().variables()
    }

    fn fixed_denotation(&self) -> bool {
        self.program.fallback().fixed_denotation()
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if self.covering_proposals {
            self.program.fallback().proposal_coverage(variable, bound)
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
}

#[derive(Clone)]
struct PageTraceFilter {
    variable: VariableId,
    estimate: usize,
    accepted: Option<RawInline>,
    page_local: bool,
    calls: Arc<Mutex<Vec<usize>>>,
}

impl<'a> Constraint<'a> for PageTraceFilter {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
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
        out.fill(self.estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        assert_ne!(
            variable, self.variable,
            "the trace-only suffix unexpectedly became the proposer"
        );
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, self.variable);
        self.calls
            .lock()
            .expect("page-trace recorder poisoned")
            .push(candidates.len());
        if let Some(accepted) = self.accepted {
            candidates.retain(|_, value| *value == accepted);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.accepted.is_none_or(|accepted| {
            view.col(self.variable)
                .is_none_or(|column| view.iter().all(|row| row[column] == accepted))
        })
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.page_local
    }
}

#[derive(Clone)]
struct SupportPageTraceFilter {
    trace: Arc<Mutex<Vec<usize>>>,
}

impl<'a> Constraint<'a> for SupportPageTraceFilter {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(OUTER)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != OUTER {
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
        assert_ne!(
            variable, OUTER,
            "the trace-only Support suffix unexpectedly became the proposer"
        );
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, OUTER);
        self.trace
            .lock()
            .expect("support trace poisoned")
            .push(candidates.len());
    }

    fn satisfied(&self, _view: &RowsView<'_>) -> bool {
        true
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PageLocalDomain(OrderedDomain);

impl<'a> Constraint<'a> for PageLocalDomain {
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
        self.0.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
enum Scheduler {
    Ordinary,
    Residual,
    Dag,
    Sequential,
}

fn combined_effects() -> ResidualLowering {
    ResidualLowering::new(FormulaScope::UnionLeaves, ProgramScope::All)
}

fn root_formula_effects() -> ResidualLowering {
    ResidualLowering::FULL
}

#[cfg(feature = "parallel")]
fn all_formula_effects() -> ResidualLowering {
    ResidualLowering::FULL
}

fn repeated(attribute: Id, inverse: bool) -> Vec<PathOp> {
    if inverse {
        vec![PathOp::Attr(attribute.raw()), PathOp::Inverse, PathOp::Plus]
    } else {
        vec![PathOp::Attr(attribute.raw()), PathOp::Plus]
    }
}

fn bound_start_root(set: TribleSet, start: Inline<GenId>, ops: &[PathOp]) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint,
    ]))
}

fn formula_bound_start_root(set: TribleSet, start: Inline<GenId>, ops: &[PathOp]) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint;
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]))
}

fn bound_end_root(set: TribleSet, end: Inline<GenId>, ops: &[PathOp]) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(end_var.is(end)) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint,
    ]))
}

fn two_free_root(set: TribleSet, ops: &[PathOp]) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint,
    ]))
}

fn target_confirm_root(
    set: TribleSet,
    candidate_variable: VariableId,
    bound: Inline<GenId>,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let (fixed, gate): (DynConstraint, VariableId) = if candidate_variable == END {
        (Box::new(start_var.is(bound)), START)
    } else {
        assert_eq!(candidate_variable, START);
        (Box::new(end_var.is(bound)), END)
    };
    Arc::new(IntersectionConstraint::new(vec![
        fixed,
        Box::new(OrderedDomain {
            variable: candidate_variable,
            gate,
            unbound_estimate: 4,
            values: candidates,
        }) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint,
    ]))
}

fn certified_target_confirm_root(
    set: TribleSet,
    bound: Inline<GenId>,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
    support_routes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let rpq = RegularPathConstraint::new(set, start_var, end_var, ops);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(bound)) as DynConstraint,
        Box::new(CertifiedOrderedDomain(OrderedDomain {
            variable: END,
            gate: START,
            unbound_estimate: 4,
            values: candidates,
        })) as DynConstraint,
        Box::new(ProbedConfirmRpq {
            program: PreferredProgram::new(
                SupportRouteProbe {
                    calls: support_routes,
                },
                rpq,
            ),
            covering_proposals: false,
        }) as DynConstraint,
    ]))
}

fn formula_target_confirm_root(
    set: TribleSet,
    bound: Inline<GenId>,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint;
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(bound)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: END,
            gate: START,
            unbound_estimate: 4,
            values: candidates,
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]))
}

fn formula_and_bound_start_root(
    set: TribleSet,
    start: Inline<GenId>,
    candidates: Vec<RawInline>,
    path_estimate_wins: bool,
    ops: &[PathOp],
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let path = Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint;
    let domain = Box::new(OrderedDomain {
        variable: END,
        // END is unbound while planning this action, so this selects whether
        // the finite AND uses the RPQ as proposer or grouped confirmer.
        gate: END,
        unbound_estimate: if path_estimate_wins { 100 } else { 0 },
        values: candidates,
    }) as DynConstraint;
    let arm = if path_estimate_wins {
        IntersectionConstraint::new(vec![path, domain])
    } else {
        IntersectionConstraint::new(vec![domain, path])
    };
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![Box::new(arm) as DynConstraint])) as DynConstraint,
    ]))
}

fn linear_formula_bound_start_filter_root(
    set: TribleSet,
    start: Inline<GenId>,
    allowed: Vec<RawInline>,
    nested_and: bool,
    ops: &[PathOp],
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let path = Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint;
    let filter = Box::new(PageLocalDomain(OrderedDomain {
        variable: END,
        gate: END,
        unbound_estimate: 100,
        values: allowed,
    })) as DynConstraint;
    let mut children = vec![Box::new(start_var.is(start)) as DynConstraint];
    if nested_and {
        children.push(Box::new(IntersectionConstraint::new(vec![path, filter])) as DynConstraint);
    } else {
        children.extend([path, filter]);
    }
    Arc::new(IntersectionConstraint::new(children))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedFormulaCase {
    Atom,
    PageLocalAnd,
    BarrierAnd,
    RepeatedRecursive,
}

fn generated_formula_root(
    graph: &GeneratedGraph,
    ops: &[PathOp],
    case: GeneratedFormulaCase,
) -> Root {
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let path = Arc::new(RegularPathConstraint::new(
        graph.set.clone(),
        start,
        end,
        ops,
    ));
    let atom = || Box::new(Arc::clone(&path)) as DynConstraint;
    let suffix = |page_local| {
        Box::new(PageTraceFilter {
            variable: END,
            estimate: usize::MAX,
            accepted: Some(graph.value(1).raw),
            page_local,
            calls: Arc::new(Mutex::new(Vec::new())),
        }) as DynConstraint
    };

    let formula = match case {
        GeneratedFormulaCase::Atom => atom(),
        GeneratedFormulaCase::PageLocalAnd => {
            Box::new(IntersectionConstraint::new(vec![atom(), suffix(true)])) as DynConstraint
        }
        GeneratedFormulaCase::BarrierAnd => {
            Box::new(IntersectionConstraint::new(vec![atom(), suffix(false)])) as DynConstraint
        }
        GeneratedFormulaCase::RepeatedRecursive => {
            // Both visits deliberately reference the same Arc. Compilation
            // must still allocate two formula occurrences: object identity is
            // reusable structure, not occurrence identity.
            let local =
                Box::new(IntersectionConstraint::new(vec![atom(), suffix(true)])) as DynConstraint;
            let barrier =
                Box::new(IntersectionConstraint::new(vec![atom(), suffix(false)])) as DynConstraint;
            Box::new(UnionConstraint::new(vec![local, barrier])) as DynConstraint
        }
    };
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(graph.value(0))) as DynConstraint,
        formula,
    ]))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedPathProgram {
    Attr,
    Optional,
    Inverse,
    Concat,
    UnionInverse,
    Negated,
    Plus,
    Star,
    InversePlus,
    ConcatPlus,
    UnionInversePlus,
    NegatedPlus,
}

impl GeneratedPathProgram {
    fn ops(self, primary: Id, secondary: Id) -> Vec<PathOp> {
        let primary = primary.raw();
        let secondary = secondary.raw();
        match self {
            Self::Attr => vec![PathOp::Attr(primary)],
            Self::Optional => vec![PathOp::Attr(primary), PathOp::Optional],
            Self::Inverse => vec![PathOp::Attr(primary), PathOp::Inverse],
            Self::Concat => vec![
                PathOp::Attr(primary),
                PathOp::Attr(secondary),
                PathOp::Concat,
            ],
            Self::UnionInverse => vec![
                PathOp::Attr(primary),
                PathOp::Attr(secondary),
                PathOp::Inverse,
                PathOp::Union,
            ],
            Self::Negated => vec![PathOp::NotAttr(primary)],
            Self::Plus => vec![PathOp::Attr(primary), PathOp::Plus],
            Self::Star => vec![PathOp::Attr(primary), PathOp::Star],
            Self::InversePlus => {
                vec![PathOp::Attr(primary), PathOp::Inverse, PathOp::Plus]
            }
            Self::ConcatPlus => vec![
                PathOp::Attr(primary),
                PathOp::Attr(primary),
                PathOp::Concat,
                PathOp::Plus,
            ],
            Self::UnionInversePlus => vec![
                PathOp::Attr(primary),
                PathOp::Attr(secondary),
                PathOp::Inverse,
                PathOp::Union,
                PathOp::Plus,
            ],
            Self::NegatedPlus => vec![PathOp::NotAttr(primary), PathOp::Plus],
        }
    }
}

fn pair_end_arm(start: Inline<GenId>, values: Vec<RawInline>, estimate: usize) -> DynConstraint {
    let start_var = Variable::<GenId>::new(START);
    Box::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: END,
            gate: END,
            unbound_estimate: estimate,
            values,
        }) as DynConstraint,
    ]))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupportArmOrder {
    FalseFirst,
    TrueFirst,
}

fn support_probe_path(
    set: &TribleSet,
    start: Variable<GenId>,
    end: Variable<GenId>,
    ops: &[PathOp],
    fully_bound_satisfied_calls: &Arc<AtomicUsize>,
) -> DynConstraint {
    Box::new(SatisfiedCountingPath {
        inner: RegularPathConstraint::new(set.clone(), start, end, ops),
        fully_bound_satisfied_calls: Arc::clone(fully_bound_satisfied_calls),
    })
}

#[allow(clippy::too_many_arguments)]
fn nested_affine_support_root(
    set: TribleSet,
    source: Inline<GenId>,
    target: Inline<GenId>,
    primary: Id,
    secondary: Id,
    parent_values: Vec<RawInline>,
    guarded_values: Vec<RawInline>,
    sibling_value: RawInline,
    arm_order: SupportArmOrder,
    trace: Option<Arc<Mutex<Vec<usize>>>>,
) -> (Root, Arc<AtomicUsize>) {
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let fully_bound_satisfied_calls = Arc::new(AtomicUsize::new(0));
    let false_path = support_probe_path(
        &set,
        start,
        end,
        &[PathOp::Attr(secondary.raw())],
        &fully_bound_satisfied_calls,
    );
    let true_path = Box::new(IntersectionConstraint::new(vec![
        support_probe_path(
            &set,
            start,
            end,
            &repeated(primary, false),
            &fully_bound_satisfied_calls,
        ),
        support_probe_path(
            &set,
            start,
            end,
            &[PathOp::Attr(primary.raw()), PathOp::Star],
            &fully_bound_satisfied_calls,
        ),
    ])) as DynConstraint;
    let guard_arms = match arm_order {
        SupportArmOrder::FalseFirst => vec![false_path, true_path],
        SupportArmOrder::TrueFirst => vec![true_path, false_path],
    };
    let guarded_children = vec![
        Box::new(UnionConstraint::new(guard_arms)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 8,
            values: guarded_values,
        }) as DynConstraint,
    ];
    let guarded = Box::new(IntersectionConstraint::new(guarded_children)) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(source)) as DynConstraint,
        Box::new(end.is(target)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 9,
            values: vec![sibling_value],
        }) as DynConstraint,
    ])) as DynConstraint;

    let mut root_children = vec![
        Box::new(OrderedDomain {
            variable: PARENT,
            gate: PARENT,
            unbound_estimate: 0,
            values: parent_values,
        }) as DynConstraint,
        Box::new(start.is(source)) as DynConstraint,
        Box::new(end.is(target)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![guarded, sibling])) as DynConstraint,
    ];
    if let Some(trace) = &trace {
        root_children.push(Box::new(SupportPageTraceFilter {
            trace: Arc::clone(trace),
        }) as DynConstraint);
    }
    let root = Arc::new(IntersectionConstraint::new(root_children));
    (root, fully_bound_satisfied_calls)
}

#[allow(clippy::too_many_arguments)]
fn fully_bound_support_root(
    set: TribleSet,
    source: Inline<GenId>,
    target: Inline<GenId>,
    ops: &[PathOp],
    guarded_value: RawInline,
    sibling_value: RawInline,
    guarded_estimate: usize,
    sibling_estimate: usize,
    fully_bound_satisfied_calls: Arc<AtomicUsize>,
) -> Root {
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let guarded = Box::new(IntersectionConstraint::new(vec![
        Box::new(SatisfiedCountingPath {
            inner: RegularPathConstraint::new(set, start, end, ops),
            fully_bound_satisfied_calls,
        }) as DynConstraint,
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: guarded_estimate,
            values: vec![guarded_value],
        }) as DynConstraint,
    ])) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(source)) as DynConstraint,
        Box::new(end.is(target)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: sibling_estimate,
            values: vec![sibling_value],
        }) as DynConstraint,
    ])) as DynConstraint;

    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(source)) as DynConstraint,
        Box::new(end.is(target)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![guarded, sibling])) as DynConstraint,
    ]))
}

fn fully_bound_same_variable_support_root(
    set: TribleSet,
    source: Inline<GenId>,
    ops: &[PathOp],
    guarded_value: RawInline,
    sibling_value: RawInline,
    fully_bound_satisfied_calls: &Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<GenId>::new(START);
    let guarded = Box::new(IntersectionConstraint::new(vec![
        support_probe_path(&set, node, node, ops, fully_bound_satisfied_calls),
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 1,
            values: vec![guarded_value],
        }) as DynConstraint,
    ])) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(node.is(source)) as DynConstraint,
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 8,
            values: vec![sibling_value],
        }) as DynConstraint,
    ])) as DynConstraint;

    Arc::new(IntersectionConstraint::new(vec![
        Box::new(node.is(source)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![guarded, sibling])) as DynConstraint,
    ]))
}

fn duplicate_parent_root(
    set: TribleSet,
    start: RawInline,
    outer_values: [RawInline; 2],
    ops: &[PathOp],
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(DuplicateParents {
            outer_values,
            start,
        }) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, start_var, end_var, ops)) as DynConstraint,
    ]))
}

fn same_variable_root(set: TribleSet, ops: &[PathOp]) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(RegularPathConstraint::new(set, node, node, ops)) as DynConstraint,
    ]))
}

fn same_variable_confirm_root(set: TribleSet, candidates: Vec<RawInline>, ops: &[PathOp]) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: candidates,
        }) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, node, node, ops)) as DynConstraint,
    ]))
}

fn same_variable_unknown_root(set: TribleSet, ops: &[PathOp]) -> Root {
    let node = Variable::<UnknownInline>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(RegularPathConstraint::new(set, node, node, ops)) as DynConstraint,
    ]))
}

fn same_variable_formula_confirm_root(
    set: TribleSet,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
) -> Root {
    let node = Variable::<GenId>::new(START);
    let arm = Box::new(RegularPathConstraint::new(set, node, node, ops)) as DynConstraint;
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: candidates,
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]))
}

fn same_variable_outer_root(set: TribleSet, outer_values: [RawInline; 2], ops: &[PathOp]) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 0,
            values: outer_values.to_vec(),
        }) as DynConstraint,
        Box::new(RegularPathConstraint::new(set, node, node, ops)) as DynConstraint,
    ]))
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

fn project_start(binding: &Binding) -> Option<RawInline> {
    binding.get(START).copied()
}

fn project_pair(binding: &Binding) -> Option<(RawInline, RawInline)> {
    Some((binding.get(START).copied()?, binding.get(END).copied()?))
}

fn project_outer(binding: &Binding) -> Option<RawInline> {
    binding.get(OUTER).copied()
}

fn run(
    root: Root,
    scheduler: Scheduler,
    project: fn(&Binding) -> Option<RawInline>,
) -> Vec<RawInline> {
    let query = Query::new(root, project);
    let mut results: Vec<_> = match scheduler {
        Scheduler::Ordinary => query.collect(),
        Scheduler::Residual => query
            .solve_residual_state_lazy_with(combined_effects())
            .collect(),
        Scheduler::Dag => query.lazy_dag_scheduler().collect(),
        Scheduler::Sequential => query.sequential().collect(),
    };
    results.sort_unstable();
    results
}

fn assert_all_schedulers(
    make_root: impl Fn() -> Root,
    project: fn(&Binding) -> Option<RawInline>,
    mut expected: Vec<RawInline>,
) {
    expected.sort_unstable();
    for scheduler in [
        Scheduler::Ordinary,
        Scheduler::Residual,
        Scheduler::Dag,
        Scheduler::Sequential,
    ] {
        assert_eq!(run(make_root(), scheduler, project), expected);
    }
}

fn sorted_bag_is_subset(subset: &[RawInline], superset: &[RawInline]) -> bool {
    let mut candidate = 0;
    for expected in subset {
        while candidate < superset.len() && superset[candidate] < *expected {
            candidate += 1;
        }
        if candidate == superset.len() || superset[candidate] != *expected {
            return false;
        }
        candidate += 1;
    }
    true
}

#[test]
fn synthetic_root_atom_same_variable_rpq_composes_capabilities() {
    let graph = Graph::new(4, &[(0, 0), (1, 1), (2, 2), (3, 3)]);
    let ops = repeated(graph.attribute, false);
    let make = || {
        RegularPathConstraint::new(
            graph.set.clone(),
            Variable::<GenId>::new(START),
            Variable::<GenId>::new(START),
            &ops,
        )
    };

    let mut expected: Vec<_> = Query::new(make(), project_start).sequential().collect();
    expected.sort_unstable();

    let cases = [
        (
            "root-only",
            ResidualLowering::new(FormulaScope::WholeRoot, ProgramScope::Disabled),
            false,
        ),
        (
            "cyclic-only",
            ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::All),
            true,
        ),
        ("whole-root-transitions", ResidualLowering::FULL, true),
    ];
    for (name, lowering, should_page_sources) in cases {
        let mut query = Query::new(make(), project_start)
            .solve_residual_state_lazy_with(lowering)
            .cap(4)
            .start_width(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "capability case {name}");
        assert_eq!(
            query.stats().delta_source_pages > 0,
            should_page_sources,
            "capability case {name}"
        );
    }
}

#[test]
fn synthetic_root_grouped_rpq_precedes_page_local_suffix_atomically() {
    let graph = Graph::new(3, &[(0, 0)]);
    let accepted = graph.value(0).raw;
    let candidates = vec![accepted, graph.value(1).raw, accepted, graph.value(2).raw];
    let suffix_calls = Arc::new(Mutex::new(Vec::new()));
    let node = Variable::<GenId>::new(START);
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: candidates,
        }) as DynConstraint,
        Box::new(RegularPathConstraint::new(
            graph.set.clone(),
            node,
            node,
            &repeated(graph.attribute, false),
        )) as DynConstraint,
        Box::new(PageTraceFilter {
            variable: START,
            estimate: usize::MAX,
            accepted: None,
            page_local: true,
            calls: Arc::clone(&suffix_calls),
        }) as DynConstraint,
    ]));
    let mut query = Query::new(root, project_start)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);

    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![accepted]);
    assert_eq!(query.stats().delta_source_pages, 4);
    assert_eq!(query.stats().delta_source_candidates_examined, 4);
    assert_eq!(query.stats().delta_source_roots, 2);
    assert_eq!(
        *suffix_calls.lock().expect("suffix recorder poisoned"),
        [1],
        "the SET-admitted parent reaches the suffix only after the grouped RPQ quiesces"
    );
    assert_eq!(query.stats().max_confirm_candidates, 4);
}

#[test]
fn synthetic_root_cyclic_proposer_respects_the_streamability_latency_boundary() {
    for page_local in [true, false] {
        let edges: Vec<_> = (0..8).map(|node| (node, node)).collect();
        let graph = Graph::new(8, &edges);
        let suffix_calls = Arc::new(Mutex::new(Vec::new()));
        let node = Variable::<GenId>::new(START);
        let accepted = (0..8)
            .map(|node| graph.value(node).raw)
            .max()
            .expect("nonempty source frontier");
        let root = Arc::new(IntersectionConstraint::new(vec![
            Box::new(RegularPathConstraint::new(
                graph.set.clone(),
                node,
                node,
                &repeated(graph.attribute, false),
            )) as DynConstraint,
            Box::new(PageTraceFilter {
                variable: START,
                estimate: usize::MAX,
                accepted: Some(accepted),
                page_local,
                calls: Arc::clone(&suffix_calls),
            }) as DynConstraint,
        ]));
        let mut query = Query::new(root, project_start)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(8)
            .start_width(1);

        assert_eq!(query.next(), Some(accepted));
        assert_eq!(query.stats().delta_source_pages, 4);
        assert_eq!(query.stats().delta_source_candidates_examined, 8);
        assert_eq!(query.stats().delta_source_roots, 8);
        if page_local {
            assert_eq!(
                *suffix_calls.lock().expect("suffix recorder poisoned"),
                [1, 1, 1, 1],
                "each ProbeOne handoff must isolate one streamed atom"
            );
            assert_eq!(query.stats().delta_handoff_probe_pops, 4);
            assert_eq!(query.stats().delta_source_dead_pages, 0);
            assert_eq!(query.stats().delta_source_negative_steps, 0);
            assert_eq!(query.stats().width_increases, 3);
            assert_eq!(query.current_width(), 8);
        } else {
            assert_eq!(
                *suffix_calls.lock().expect("suffix recorder poisoned"),
                [8],
                "an ineligible suffix must retain the frozen global-width trace"
            );
            assert_eq!(query.stats().delta_handoff_probe_pops, 1);
            assert_eq!(query.stats().delta_source_dead_pages, 4);
            assert_eq!(query.stats().delta_source_negative_steps, 4);
            assert_eq!(query.stats().width_increases, 3);
            assert_eq!(query.current_width(), 8);
        }
    }
}

#[test]
fn nested_repeated_root_rpqs_keep_distinct_action_occurrences() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let make = || {
        let start = Variable::<GenId>::new(START);
        let end = Variable::<GenId>::new(END);
        let arm = || {
            Box::new(RegularPathConstraint::new(
                graph.set.clone(),
                start,
                end,
                &ops,
            )) as DynConstraint
        };
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(start.is(graph.value(0))) as DynConstraint,
            Box::new(UnionConstraint::new(vec![arm(), arm()])) as DynConstraint,
        ]))
    };
    let lowering = ResidualLowering::FULL;
    let direct = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(lowering)
        .collect_profiled();
    let mut actual = direct.results;
    actual.sort_unstable();
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    let observed = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(lowering)
        .shadow(ResidualShadowEpoch::new())
        .collect_profiled();
    let mut observed_results = observed.results.clone();
    observed_results.sort_unstable();
    assert_eq!(observed_results, expected);
    let mut occurrences: Vec<_> = observed
        .shadow
        .events
        .iter()
        .filter(|event| event.site.verb == ActionVerb::Propose && event.site.variable == END)
        .map(|event| event.site.leaf_occurrence)
        .collect();
    occurrences.sort_unstable();
    occurrences.dedup();
    assert_eq!(occurrences.len(), 2);
    assert_eq!(observed.stats, direct.stats);
}

#[test]
fn cyclic_rpq_runs_as_a_direct_finite_or_proposal_action() {
    let graph = Graph::new(4, &[(0, 1), (1, 2), (2, 3)]);
    let ops = repeated(graph.attribute, false);
    let root = formula_bound_start_root(graph.set.clone(), graph.value(0), &ops);

    let mut lowered_query = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);
    let mut lowered: Vec<_> = lowered_query.by_ref().collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected: Vec<_> = (1..4).map(|node| graph.value(node).raw).collect();
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert!(lowered_query.stats().delta_transition_pages > 0);
}

#[test]
fn cyclic_rpq_runs_as_a_direct_finite_or_grouped_confirm_action() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let absent = genid(&rngid().id).raw;
    let root = formula_target_confirm_root(
        graph.set.clone(),
        graph.value(0),
        vec![
            graph.value(2).raw,
            absent,
            graph.value(2).raw,
            graph.value(1).raw,
        ],
        &ops,
    );

    let mut lowered_query = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);
    let mut lowered: Vec<_> = lowered_query.by_ref().collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert!(lowered_query.stats().delta_transition_pages > 0);
}

#[test]
fn cyclic_or_confirm_keeps_the_original_group_for_a_later_sibling() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let cyclic = Box::new(RegularPathConstraint::new(
        graph.set.clone(),
        start_var,
        end_var,
        &ops,
    )) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(graph.value(0))) as DynConstraint,
        Box::new(OrderedDomain {
            variable: END,
            gate: START,
            unbound_estimate: 10,
            values: vec![graph.value(3).raw],
        }) as DynConstraint,
    ])) as DynConstraint;
    let union = Box::new(UnionConstraint::new(vec![cyclic, sibling])) as DynConstraint;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(graph.value(0))) as DynConstraint,
        Box::new(OrderedDomain {
            variable: END,
            gate: START,
            unbound_estimate: 4,
            values: vec![graph.value(2).raw, graph.value(3).raw, graph.value(1).raw],
        }) as DynConstraint,
        union,
    ]));

    let mut lowered_query = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);
    let mut lowered: Vec<_> = lowered_query.by_ref().collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw, graph.value(3).raw];
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert!(lowered_query.stats().delta_transition_pages > 0);
}

#[test]
fn cyclic_rpq_runs_in_a_finite_and_as_proposer_and_grouped_confirmer() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let absent = genid(&rngid().id).raw;

    for (path_estimate_wins, candidates, mut expected) in [
        (
            true,
            vec![graph.value(2).raw, absent],
            vec![graph.value(2).raw],
        ),
        (
            false,
            vec![graph.value(2).raw, absent, graph.value(1).raw],
            vec![graph.value(1).raw, graph.value(2).raw],
        ),
    ] {
        let root = formula_and_bound_start_root(
            graph.set.clone(),
            graph.value(0),
            candidates,
            path_estimate_wins,
            &ops,
        );
        let mut lowered_query = Query::new(Arc::clone(&root), project_end)
            .solve_residual_state_lazy_with(combined_effects());
        let mut lowered: Vec<_> = lowered_query.by_ref().collect();
        let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
        lowered.sort_unstable();
        sequential.sort_unstable();
        expected.sort_unstable();
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, expected);
        assert!(lowered_query.stats().delta_transition_pages > 0);
    }
}

#[test]
fn cyclic_rpq_resumes_through_recursive_or_and_or_frames() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);

    for outer_confirmation in [false, true] {
        let start = graph.value(0);
        let start_var = Variable::<GenId>::new(START);
        let end_var = Variable::<GenId>::new(END);
        let cyclic = Box::new(RegularPathConstraint::new(
            graph.set.clone(),
            start_var,
            end_var,
            &ops,
        )) as DynConstraint;
        let inner_or = Box::new(UnionConstraint::new(vec![
            cyclic,
            pair_end_arm(start, vec![graph.value(3).raw], 10),
        ])) as DynConstraint;
        let guarded = Box::new(IntersectionConstraint::new(vec![
            inner_or,
            pair_end_arm(start, vec![graph.value(2).raw, graph.value(3).raw], 100),
        ])) as DynConstraint;
        let outer_or = Box::new(UnionConstraint::new(vec![
            guarded,
            pair_end_arm(start, vec![graph.value(0).raw], 20),
        ])) as DynConstraint;
        let mut constraints = vec![Box::new(start_var.is(start)) as DynConstraint];
        if outer_confirmation {
            constraints.push(Box::new(OrderedDomain {
                variable: END,
                gate: START,
                unbound_estimate: 4,
                values: (0..4).map(|node| graph.value(node).raw).collect(),
            }) as DynConstraint);
        }
        constraints.push(outer_or);
        let root = Arc::new(IntersectionConstraint::new(constraints));

        let mut lowered_query = Query::new(Arc::clone(&root), project_end)
            .solve_residual_state_lazy_with(combined_effects())
            .cap(1)
            .start_width(1);
        let mut lowered: Vec<_> = lowered_query.by_ref().collect();
        let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
        let mut expected = vec![graph.value(0).raw, graph.value(2).raw, graph.value(3).raw];
        lowered.sort_unstable();
        sequential.sort_unstable();
        expected.sort_unstable();
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, expected);
        assert!(lowered_query.stats().delta_transition_pages > 0);
    }
}

#[test]
fn synthetic_root_atom_streams_a_cycle_before_fixpoint_cleanup() {
    let graph = Graph::new(3, &[(0, 0), (1, 2)]);
    let node = Variable::<GenId>::new(START);
    let root = Arc::new(RegularPathConstraint::new(
        graph.set.clone(),
        node,
        node,
        &repeated(graph.attribute, false),
    ));
    let mut query = Query::new(root, project_start)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    assert_eq!(query.next(), Some(graph.value(0).raw));
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    assert_eq!(
        query.stats().delta_handoff_probe_pops,
        0,
        "a fully checked final candidate tail is already exact output"
    );
    assert_eq!(query.next(), None);
    assert_eq!(query.stats().delta_transition_candidates_examined, 2);
}

#[test]
fn synthetic_root_and_streams_early_and_late_page_local_survivors() {
    for (accepted_node, expected_before_emit, nested_and) in [(1, 1, false), (4, 4, true)] {
        let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let ops = repeated(graph.attribute, false);
        let expected = graph.value(accepted_node).raw;
        let root = linear_formula_bound_start_filter_root(
            graph.set.clone(),
            graph.value(0),
            vec![expected],
            nested_and,
            &ops,
        );
        let mut query = Query::new(root, project_end)
            .solve_residual_state_lazy_with(root_formula_effects())
            .cap(1)
            .start_width(1);

        assert_eq!(query.next(), Some(expected));
        assert_eq!(
            query.stats().delta_transition_candidates_examined,
            expected_before_emit,
            "accepted_node={accepted_node}, nested_and={nested_and}"
        );
        assert_eq!(
            query.stats().delta_handoff_probe_pops,
            expected_before_emit + 2,
            "each adjacency plus the SET and typed program/formula handoffs are probed once"
        );
        assert_eq!(query.next(), None);
        assert_eq!(query.stats().delta_transition_pages, 5);
        assert_eq!(query.stats().delta_transition_candidates_examined, 4);
    }
}

#[test]
fn synthetic_root_and_empty_filter_waits_for_cleanup_without_replay() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = repeated(graph.attribute, false);
    let root = linear_formula_bound_start_filter_root(
        graph.set.clone(),
        graph.value(0),
        vec![genid(&rngid().id).raw],
        false,
        &ops,
    );
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    assert_eq!(query.next(), None);
    assert_eq!(query.stats().delta_transition_pages, 5);
    assert_eq!(
        query.stats().delta_transition_candidates_examined,
        4,
        "the initial bound candidate is not adjacency work"
    );
    assert_eq!(
        query.stats().candidates_proposed,
        5,
        "one bound-start candidate plus four streamed RPQ endpoints"
    );
}

#[test]
fn linear_formula_streaming_matches_the_always_quiescent_union_bag() {
    let graph = Graph::new(7, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
    let ops = repeated(graph.attribute, false);
    let allowed = vec![graph.value(1).raw, graph.value(3).raw, graph.value(6).raw];
    let streaming = linear_formula_bound_start_filter_root(
        graph.set.clone(),
        graph.value(0),
        allowed.clone(),
        true,
        &ops,
    );
    let quiescent =
        formula_and_bound_start_root(graph.set.clone(), graph.value(0), allowed, true, &ops);

    let mut streamed: Vec<_> = Query::new(streaming, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1)
        .collect();
    let mut always_quiescent: Vec<_> = Query::new(quiescent, project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1)
        .collect();
    streamed.sort_unstable();
    always_quiescent.sort_unstable();
    assert_eq!(streamed, always_quiescent);
    assert_eq!(streamed.len(), 3);
}

#[test]
fn linear_formula_streaming_collapses_byte_identical_semantic_parents() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let outer = genid(&rngid().id).raw;
    let root = duplicate_parent_root(
        graph.set.clone(),
        graph.value(0).raw,
        [outer, outer],
        &repeated(graph.attribute, false),
    );
    let mut actual: Vec<_> = Query::new(root, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1)
        .collect();
    actual.sort_unstable();
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn clone_and_drop_preserve_a_live_linear_formula_stream() {
    let graph = Graph::new(4, &[(0, 1), (1, 2), (2, 3)]);
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
    );
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    let first = query.next().expect("the first endpoint streamed");
    assert_eq!(first, graph.value(1).raw);
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    assert_eq!(
        query.stats().delta_handoff_probe_pops,
        2,
        "the accepted adjacency and typed program/formula boundary are each probed once"
    );
    let exact_clone = query.clone();
    let cancelled = query.clone();
    drop(cancelled);

    let mut original = vec![first];
    original.extend(query);
    let mut cloned = vec![first];
    cloned.extend(exact_clone);
    original.sort_unstable();
    cloned.sort_unstable();
    assert_eq!(cloned, original);
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw, graph.value(3).raw];
    expected.sort_unstable();
    assert_eq!(original, expected);
}

#[test]
fn ordinary_shape_selected_query_composes_root_formula_union_and_cyclic_rpq() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let start = graph.value(0);

    let mut expected: Vec<_> = Query::new(
        formula_bound_start_root(graph.set.clone(), start, &ops),
        project_end,
    )
    .sequential()
    .collect();

    let mut ordinary_query = Query::new(
        formula_bound_start_root(graph.set, start, &ops),
        project_end,
    );
    let mut ordinary: Vec<_> = ordinary_query.by_ref().collect();

    expected.sort_unstable();
    ordinary.sort_unstable();
    assert_eq!(ordinary, expected);
}

#[test]
fn finite_or_keeps_cyclic_proposals_private_until_fixpoint_quiescence() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let ops = repeated(graph.attribute, false);
    let root = formula_bound_start_root(graph.set.clone(), graph.value(0), &ops);
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);

    assert!(query.next().is_some());
    assert_eq!(
        query.stats().delta_transition_pages,
        8,
        "the finite OR waits for the complete cyclic arm"
    );
    assert_eq!(
        query.stats().delta_transition_candidates_examined,
        7,
        "an OR arm must not publish a partial cyclic proposal"
    );
    drop(query);
}

#[test]
fn clone_and_drop_preserve_a_live_formula_cyclic_remainder() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (4, 5), (5, 6), (6, 7)]);
    let ops = repeated(graph.attribute, false);
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let cyclic = Box::new(RegularPathConstraint::new(
        graph.set.clone(),
        start_var,
        end_var,
        &ops,
    )) as DynConstraint;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: vec![graph.value(0).raw, graph.value(4).raw],
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![cyclic])) as DynConstraint,
    ]));
    let mut query = Query::new(root, project_pair)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);

    let first = query.next().expect("one source activation quiesced");
    assert_eq!(query.stats().delta_transition_pages, 4);
    assert_eq!(
        query.stats().delta_transition_candidates_examined,
        3,
        "the other source activation must remain live"
    );
    let exact_clone = query.clone();
    let cancelled = query.clone();
    drop(cancelled);

    let mut original = vec![first];
    original.extend(query);
    let mut cloned = vec![first];
    cloned.extend(exact_clone);
    original.sort_unstable();
    cloned.sort_unstable();
    assert_eq!(cloned, original);
    assert_eq!(original.len(), 6);
}

#[test]
fn formula_transition_lowering_remains_capability_and_shape_gated() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let plus = repeated(graph.attribute, false);
    let expected = {
        let mut values = vec![graph.value(1).raw, graph.value(2).raw];
        values.sort_unstable();
        values
    };
    for lowering in [
        ResidualLowering::new(FormulaScope::UnionLeaves, ProgramScope::Disabled),
        ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::All),
    ] {
        let root = formula_bound_start_root(graph.set.clone(), graph.value(0), &plus);
        let mut query = Query::new(root, project_end).solve_residual_state_lazy_with(lowering);
        let mut actual: Vec<_> = query.by_ref().collect();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(query.stats().delta_transition_pages, 0);
    }

    let root = formula_bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &[PathOp::Attr(graph.attribute.raw())],
    );
    let mut query =
        Query::new(root, project_end).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(1).raw]);
    assert!(query.stats().delta_transition_pages > 0);
}

#[test]
fn zero_root_cyclic_and_returns_empty_without_erasing_its_or_sibling() {
    let graph = Graph::new(1, &[]);
    let node = Variable::<GenId>::new(START);
    let ops = repeated(graph.attribute, false);
    let survivor = graph.value(0).raw;
    let cyclic = Box::new(RegularPathConstraint::new(
        graph.set.clone(),
        node,
        node,
        &ops,
    )) as DynConstraint;
    let dead_and = Box::new(IntersectionConstraint::new(vec![
        cyclic,
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 100,
            values: vec![survivor],
        }) as DynConstraint,
    ])) as DynConstraint;
    let sibling = Box::new(OrderedDomain {
        variable: START,
        gate: START,
        unbound_estimate: 10,
        values: vec![survivor],
    }) as DynConstraint;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(UnionConstraint::new(vec![dead_and, sibling])) as DynConstraint,
    ]));

    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![survivor]);
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_dead_pages, 0);
    assert_eq!(query.stats().delta_source_negative_steps, 0);
    assert!(query.stats().delta_handoff_probe_pops > 0);
}

#[test]
fn formula_cyclic_activations_preserve_duplicate_outer_parents() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(RegularPathConstraint::new(
        graph.set.clone(),
        start_var,
        end_var,
        &ops,
    )) as DynConstraint;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(DuplicateParents {
            outer_values,
            start: graph.value(0).raw,
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]));

    let mut lowered_query = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects());
    let mut lowered: Vec<_> = lowered_query.by_ref().collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected = vec![
        graph.value(1).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        graph.value(2).raw,
    ];
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert!(lowered_query.stats().delta_transition_pages > 0);
}

#[cfg(feature = "parallel")]
#[test]
fn all_capability_formula_cyclic_plan_survives_clone_and_parallel_split() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make = || {
        let start_var = Variable::<GenId>::new(START);
        let end_var = Variable::<GenId>::new(END);
        let arm = Box::new(RegularPathConstraint::new(
            graph.set.clone(),
            start_var,
            end_var,
            &ops,
        )) as DynConstraint;
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(DuplicateParents {
                outer_values,
                start: graph.value(0).raw,
            }) as DynConstraint,
            Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
        ]))
    };
    let configured = |capabilities| {
        Query::new(make(), project_end)
            .solve_residual_state_lazy_with(capabilities)
            .cap(2)
            .start_width(2)
    };
    let sorted = |mut rows: Vec<RawInline>| {
        rows.sort_unstable();
        rows
    };

    let scalar = sorted(Query::new(make(), project_end).sequential().collect());
    let expected = sorted(vec![
        graph.value(1).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        graph.value(2).raw,
    ]);
    assert_eq!(scalar, expected);

    // `root_formula` recursively owns finite AND/OR structure, so enabling the
    // narrower `finite_unions` capability as well must not change the exact
    // result bag or activation multiplicity.
    let root_formula = configured(root_formula_effects());
    let root_formula = sorted(root_formula.collect());
    assert_eq!(root_formula, scalar);

    let all_capabilities = configured(all_formula_effects());
    let exact_clone = all_capabilities.clone();
    let original = all_capabilities.collect::<Vec<_>>();
    let cloned = exact_clone.collect::<Vec<_>>();
    assert_eq!(cloned, original, "a fresh clone changed the exact stream");
    assert_eq!(sorted(original), root_formula);

    let mut started = configured(all_formula_effects());
    let first = started
        .next()
        .expect("the configured query has four results");
    let exact_remainder = started.clone();
    let remainder = started.collect::<Vec<_>>();
    let cloned_remainder = exact_remainder.collect::<Vec<_>>();
    assert_eq!(
        cloned_remainder, remainder,
        "a started clone changed the exact remainder"
    );
    assert_eq!(
        sorted(std::iter::once(first).chain(remainder).collect()),
        scalar
    );

    for workers in [1, 4] {
        let parallel = configured(all_formula_effects());
        let parallel = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap()
            .install(|| parallel.into_par_iter().collect::<Vec<_>>());
        assert_eq!(sorted(parallel), scalar, "workers={workers}");
    }
}

#[test]
fn formula_same_variable_sources_keep_novelty_separate_at_shared_terms() {
    let graph = Graph::new(4, &[(0, 2), (1, 2), (2, 1), (3, 0)]);
    let root = same_variable_formula_confirm_root(
        graph.set.clone(),
        vec![graph.value(0).raw, graph.value(1).raw],
        &repeated(graph.attribute, false),
    );

    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(1).raw]);
    assert_eq!(query.stats().delta_source_roots, 2);
    assert_eq!(query.stats().delta_source_pages, 2);
    assert_eq!(
        query.stats().delta_source_dead_pages,
        2,
        "a page whose only accepted endpoint was admitted earlier is scheduler-negative"
    );
}

#[test]
fn formula_same_variable_fixpoint_keeps_inverse_program_direction() {
    let graph = Graph::new(3, &[(0, 1), (2, 1)]);
    let attribute = PathOp::Attr(graph.attribute.raw());
    let cases = [
        (
            vec![
                attribute.clone(),
                attribute.clone(),
                PathOp::Inverse,
                PathOp::Concat,
                PathOp::Plus,
            ],
            vec![graph.value(0).raw, graph.value(2).raw],
        ),
        (
            vec![
                attribute.clone(),
                PathOp::Inverse,
                attribute,
                PathOp::Concat,
                PathOp::Plus,
            ],
            vec![graph.value(1).raw],
        ),
    ];
    for (ops, mut expected) in cases {
        let node = Variable::<GenId>::new(START);
        let root = Arc::new(IntersectionConstraint::new(vec![
            Box::new(UnionConstraint::new(vec![
                Box::new(RegularPathConstraint::new(
                    graph.set.clone(),
                    node,
                    node,
                    &ops,
                )) as DynConstraint,
            ])) as DynConstraint,
        ]));
        let mut lowered: Vec<_> = Query::new(root, project_start)
            .solve_residual_state_lazy_with(combined_effects())
            .collect();
        lowered.sort_unstable();
        expected.sort_unstable();
        assert_eq!(lowered, expected);
    }
}

#[test]
fn plus_attr_handles_chain_diamond_self_loop_and_long_cycle() {
    let cases = [
        (3, vec![(0, 1), (1, 2)], vec![1, 2]),
        (4, vec![(0, 1), (0, 2), (1, 3), (2, 3)], vec![1, 2, 3]),
        (1, vec![(0, 0)], vec![0]),
        (3, vec![(0, 1), (1, 2), (2, 0)], vec![0, 1, 2]),
    ];
    for (node_count, edges, reachable) in cases {
        let graph = Graph::new(node_count, &edges);
        let ops = repeated(graph.attribute, false);
        let expected = reachable
            .into_iter()
            .map(|node| graph.value(node).raw)
            .collect();
        assert_all_schedulers(
            || bound_start_root(graph.set.clone(), graph.value(0), &ops),
            project_end,
            expected,
        );
    }
}

#[test]
fn same_variable_plus_denotes_nonempty_cycles_not_general_reachability() {
    let cases = [
        (3, vec![(0, 1), (1, 2)], vec![]),
        (3, vec![(0, 0), (0, 1), (1, 2)], vec![0]),
        (3, vec![(0, 1), (1, 2), (2, 0)], vec![0, 1, 2]),
        (4, vec![(0, 2), (1, 2), (2, 1), (3, 0)], vec![1, 2]),
    ];
    for (node_count, edges, cyclic) in cases {
        let graph = Graph::new(node_count, &edges);
        for inverse in [false, true] {
            let ops = repeated(graph.attribute, inverse);
            let expected = cyclic.iter().map(|&node| graph.value(node).raw).collect();
            assert_all_schedulers(
                || same_variable_root(graph.set.clone(), &ops),
                project_start,
                expected,
            );
        }
    }
}

#[test]
fn same_variable_product_program_keeps_inverse_direction_inside_the_fixpoint() {
    let graph = Graph::new(3, &[(0, 1), (2, 1)]);
    let attribute = PathOp::Attr(graph.attribute.raw());
    let cases = [
        (
            vec![
                attribute.clone(),
                attribute.clone(),
                PathOp::Inverse,
                PathOp::Concat,
                PathOp::Plus,
            ],
            vec![graph.value(0).raw, graph.value(2).raw],
        ),
        (
            vec![
                attribute.clone(),
                PathOp::Inverse,
                attribute,
                PathOp::Concat,
                PathOp::Plus,
            ],
            vec![graph.value(1).raw],
        ),
    ];
    for (ops, expected) in cases {
        assert_all_schedulers(
            || same_variable_root(graph.set.clone(), &ops),
            project_start,
            expected,
        );
    }
}

#[test]
fn same_variable_star_admits_exactly_the_graph_term_universe() {
    let mut graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let other = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[3], &other, &graph.value(3)));
    let expected: Vec<_> = (0..4).map(|node| graph.value(node).raw).collect();
    for inverse in [false, true] {
        let mut ops = if inverse {
            vec![PathOp::Attr(graph.attribute.raw()), PathOp::Inverse]
        } else {
            vec![PathOp::Attr(graph.attribute.raw())]
        };
        ops.push(PathOp::Star);
        assert_all_schedulers(
            || same_variable_root(graph.set.clone(), &ops),
            project_start,
            expected.clone(),
        );
    }
}

#[test]
fn nullable_source_pages_are_the_sorted_nodes_union_without_absent_terms() {
    let mut graph = Graph::new(3, &[]);
    let literal = Inline::<UnknownInline>::new([0xA5; 32]);
    graph.set.insert(&Trible::new(
        &graph.nodes[0],
        &graph.attribute,
        &graph.value(1),
    ));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[1], &other_attribute(), &literal));
    let ops = vec![PathOp::Attr(graph.attribute.raw()), PathOp::Star];
    let mut expected = vec![graph.value(0).raw, graph.value(1).raw, literal.raw];
    expected.sort_unstable();

    assert_all_schedulers(
        || same_variable_unknown_root(graph.set.clone(), &ops),
        project_start,
        expected,
    );
}

#[test]
fn first_union_pages_deduplicate_arms_and_match_candidate_last_filtering() {
    let mut graph = Graph::new(7, &[(0, 1)]);
    let literal = Inline::<UnknownInline>::new([0xA5; 32]);
    graph
        .set
        .insert(&Trible::new(&graph.nodes[2], &graph.attribute, &literal));
    let excluded = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[3], &excluded, &graph.value(4)));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[4], &excluded, &graph.value(3)));
    let included = rngid().id;
    graph
        .set
        .insert(&Trible::new(&graph.nodes[5], &included, &graph.value(6)));
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Union,
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Inverse,
        PathOp::Union,
        PathOp::NotAttr(excluded.raw()),
        PathOp::Union,
        PathOp::Plus,
    ];
    let mut expected = vec![
        graph.value(0).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        literal.raw,
    ];
    expected.sort_unstable();

    assert_all_schedulers(
        || same_variable_unknown_root(graph.set.clone(), &ops),
        project_start,
        expected,
    );
}

#[test]
fn same_variable_confirm_preserves_order_duplicates_and_graph_term_scope() {
    let mut graph = Graph::new(5, &[(0, 0), (1, 2), (2, 1)]);
    let other = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[3], &other, &graph.value(4)));
    let absent = genid(&rngid().id).raw;
    let original = vec![
        (0, graph.value(3).raw),
        (0, absent),
        (0, graph.value(0).raw),
        (0, graph.value(3).raw),
        (0, graph.value(1).raw),
        (0, absent),
    ];
    let node = Variable::<GenId>::new(START);

    let plus = RegularPathConstraint::new(
        graph.set.clone(),
        node,
        node,
        &repeated(graph.attribute, false),
    );
    let mut plus_candidates = original.clone();
    plus.confirm(
        START,
        &RowsView::EMPTY,
        &mut CandidateSink::Tagged(&mut plus_candidates),
    );
    assert_eq!(
        plus_candidates,
        vec![(0, graph.value(0).raw), (0, graph.value(1).raw)]
    );

    let star = RegularPathConstraint::new(
        graph.set.clone(),
        node,
        node,
        &[PathOp::Attr(graph.attribute.raw()), PathOp::Star],
    );
    let mut star_candidates = original;
    star.confirm(
        START,
        &RowsView::EMPTY,
        &mut CandidateSink::Tagged(&mut star_candidates),
    );
    assert_eq!(
        star_candidates,
        vec![
            (0, graph.value(3).raw),
            (0, graph.value(0).raw),
            (0, graph.value(3).raw),
            (0, graph.value(1).raw),
        ]
    );
}

#[test]
fn same_variable_grouped_delta_confirm_filters_one_immutable_sequence() {
    let mut graph = Graph::new(5, &[(0, 0), (1, 2), (2, 1)]);
    let other = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[3], &other, &graph.value(4)));
    let absent = genid(&rngid().id).raw;
    let candidates = vec![
        graph.value(3).raw,
        absent,
        graph.value(0).raw,
        graph.value(3).raw,
        graph.value(1).raw,
        absent,
    ];
    let cases = [
        (
            repeated(graph.attribute, false),
            vec![graph.value(0).raw, graph.value(1).raw],
            2,
        ),
        (
            vec![PathOp::Attr(graph.attribute.raw()), PathOp::Star],
            vec![graph.value(0).raw, graph.value(3).raw, graph.value(1).raw],
            4,
        ),
    ];
    for (ops, expected, expected_roots) in cases {
        let root = same_variable_confirm_root(graph.set.clone(), candidates.clone(), &ops);
        let mut query =
            Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
        let actual: Vec<_> = query.by_ref().collect();
        assert_eq!(actual, expected);
        assert_eq!(query.stats().delta_source_pages, 3);
        assert_eq!(query.stats().delta_source_roots, expected_roots);
        assert_eq!(
            query.stats().delta_source_candidates_examined,
            candidates.len()
        );
        assert_eq!(query.current_width(), 16);
        assert_eq!(query.stats().width_increases, 4);
    }
}

#[test]
fn same_variable_sources_do_not_share_seen_at_a_common_term() {
    // A -> C, B -> C, C -> B is the collision: A rejects after reaching C,
    // while B must continue through the same C and return to B. D -> A only
    // makes A survive the exact FIRST/last-source restriction.
    let graph = Graph::new(4, &[(0, 2), (1, 2), (2, 1), (3, 0)]);
    let root = same_variable_confirm_root(
        graph.set.clone(),
        vec![graph.value(0).raw, graph.value(1).raw],
        &repeated(graph.attribute, false),
    );

    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(1).raw]);
    assert_eq!(query.stats().delta_source_roots, 2);
    assert!(query.stats().delta_transition_pages > 0);
}

#[test]
fn same_variable_fixpoint_preserves_duplicate_outer_activations() {
    let graph = Graph::new(2, &[(0, 1), (1, 0)]);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let ops = repeated(graph.attribute, false);
    assert_all_schedulers(
        || same_variable_outer_root(graph.set.clone(), outer_values, &ops),
        project_start,
        vec![
            graph.value(0).raw,
            graph.value(0).raw,
            graph.value(1).raw,
            graph.value(1).raw,
        ],
    );
}

#[test]
fn same_variable_delta_streams_after_one_lazy_source_and_one_expansion() {
    let graph = Graph::new(
        8,
        &[
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),
            (5, 5),
            (6, 6),
            (7, 7),
        ],
    );
    let root = same_variable_root(graph.set.clone(), &repeated(graph.attribute, false));
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    let first = query.next().expect("every source has a self-loop");
    assert!((0..8).any(|node| first == graph.value(node).raw));
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_roots, 1);
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    drop(query);
}

#[test]
fn same_variable_negative_source_pages_grow_one_two_four() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let root = same_variable_root(graph.set.clone(), &repeated(graph.attribute, false));
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), None);
    assert_eq!(query.current_width(), 8);
    assert_eq!(query.stats().width_increases, 3);
    assert_eq!(query.stats().delta_source_pages, 3);
    assert_eq!(query.stats().delta_source_candidates_examined, 7);
    assert_eq!(query.stats().delta_source_roots, 6);
    assert_eq!(query.stats().delta_source_dead_pages, 3);
    assert_eq!(query.stats().delta_source_negative_steps, 3);
    assert_eq!(query.stats().delta_handoff_probe_pops, 0);
}

#[test]
fn same_variable_late_hit_keeps_the_geometric_negative_prefix() {
    let mut graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let target = (1..=6)
        .max_by_key(|&node| graph.value(node).raw)
        .expect("nonempty middle source set");
    graph.set.insert(&Trible::new(
        &graph.nodes[target],
        &graph.attribute,
        &graph.value(target),
    ));
    let root = same_variable_root(graph.set.clone(), &repeated(graph.attribute, false));
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), Some(graph.value(target).raw));
    assert_eq!(query.stats().delta_source_pages, 3);
    assert_eq!(query.stats().delta_source_candidates_examined, 7);
    assert_eq!(query.stats().delta_source_roots, 6);
    assert_eq!(query.current_width(), 4);
    assert_eq!(query.stats().width_increases, 2);
    assert_eq!(query.stats().delta_source_dead_pages, 2);
    assert_eq!(query.stats().delta_source_negative_steps, 2);
    assert_eq!(
        query.stats().delta_handoff_probe_pops,
        0,
        "the late hit is terminal and needs no selective probe"
    );
    drop(query);
}

#[test]
fn same_variable_delta_remains_opt_in() {
    let graph = Graph::new(1, &[(0, 0)]);
    let root = same_variable_root(graph.set.clone(), &repeated(graph.attribute, false));

    let mut query = Query::new(root, project_start).solve_residual_state_lazy();
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(0).raw]);
    assert_eq!(query.stats().delta_source_pages, 0);
    assert_eq!(query.stats().delta_transition_pages, 0);
}

#[test]
fn star_and_optional_epsilon_acceptance_obey_the_graph_term_gate() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let star = vec![PathOp::Attr(graph.attribute.raw()), PathOp::Star];
    let optional_or_plus = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Optional,
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Plus,
        PathOp::Union,
    ];
    let expected = vec![graph.value(0).raw, graph.value(1).raw, graph.value(2).raw];
    for ops in [&star, &optional_or_plus] {
        assert_all_schedulers(
            || bound_start_root(graph.set.clone(), graph.value(0), ops),
            project_end,
            expected.clone(),
        );

        let absent = genid(&rngid().id);
        assert_all_schedulers(
            || bound_start_root(graph.set.clone(), absent, ops),
            project_end,
            Vec::new(),
        );
    }

    let _ = run(
        bound_start_root(graph.set.clone(), graph.value(0), &star),
        Scheduler::Residual,
        project_end,
    );
}

#[test]
fn nullable_support_uses_native_program_for_distinct_same_variable_and_absent_terms() {
    let graph = Graph::new(6, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = [PathOp::Attr(graph.attribute.raw()), PathOp::Star];
    let guarded_value = genid(&rngid().id).raw;
    let sibling_value = genid(&rngid().id).raw;
    let fully_bound_satisfied_calls = Arc::new(AtomicUsize::new(0));
    let make = || {
        fully_bound_support_root(
            graph.set.clone(),
            graph.value(0),
            graph.value(0),
            &ops,
            guarded_value,
            sibling_value,
            1,
            8,
            Arc::clone(&fully_bound_satisfied_calls),
        )
    };
    let mut query = Query::new(make(), project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);

    let first = query.next().expect("one nullable Support result");
    assert!(first == guarded_value || first == sibling_value);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);
    assert!(query.stats().support_action_pops > 0);
    assert!(query.stats().support_calls > 0);
    let mut cloned = query.clone();
    let remainder = if first == guarded_value {
        sibling_value
    } else {
        guarded_value
    };
    assert_eq!(query.next(), Some(remainder));
    assert_eq!(cloned.next(), Some(remainder));
    drop(query);
    drop(cloned);

    let same_variable_satisfied_calls = Arc::new(AtomicUsize::new(0));
    let mut same_variable = Query::new(
        fully_bound_same_variable_support_root(
            graph.set.clone(),
            graph.value(0),
            &ops,
            guarded_value,
            sibling_value,
            &same_variable_satisfied_calls,
        ),
        project_outer,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .cap(1);
    let same_first = same_variable
        .next()
        .expect("one same-variable nullable Support result");
    assert!(same_first == guarded_value || same_first == sibling_value);
    assert_eq!(same_variable_satisfied_calls.load(Ordering::Relaxed), 0);
    assert!(same_variable.stats().support_action_pops > 0);
    let same_remainder = if same_first == guarded_value {
        sibling_value
    } else {
        guarded_value
    };
    assert_eq!(same_variable.next(), Some(same_remainder));
    drop(same_variable);

    let absent_guard = genid(&rngid().id).raw;
    let absent_sibling = genid(&rngid().id).raw;
    let absent = fully_bound_support_root(
        graph.set.clone(),
        graph.value(5),
        graph.value(5),
        &ops,
        absent_guard,
        absent_sibling,
        1,
        8,
        Arc::new(AtomicUsize::new(0)),
    );
    let mut absent_query = Query::new(absent, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    assert_eq!(
        absent_query.by_ref().collect::<Vec<_>>(),
        vec![absent_sibling],
        "nullable acceptance escaped the NODES(G) gate"
    );
    assert!(absent_query.stats().delta_transition_pages > 0);
}

#[test]
fn fully_bound_formula_guard_uses_native_support_instead_of_legacy_reachability() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let guarded_value = genid(&rngid().id).raw;
    let sibling_value = genid(&rngid().id).raw;
    let fully_bound_satisfied_calls = Arc::new(AtomicUsize::new(0));
    let root = fully_bound_support_root(
        graph.set.clone(),
        graph.value(0),
        graph.value(3),
        &repeated(graph.attribute, false),
        guarded_value,
        sibling_value,
        1,
        8,
        Arc::clone(&fully_bound_satisfied_calls),
    );

    let mut query = Query::new(root, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let actual: Vec<_> = query.by_ref().collect();

    assert_eq!(actual, vec![sibling_value]);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);
    assert!(query.stats().support_action_pops > 0);
    assert!(query.stats().support_calls > 0);
    assert!(query.stats().delta_transition_pages > 0);
}

#[test]
fn first_support_witness_resumes_formula_via_the_native_program() {
    let graph = Graph::new(8, &[(0, 1), (0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let guarded_value = genid(&rngid().id).raw;
    let sibling_value = genid(&rngid().id).raw;
    let fully_bound_satisfied_calls = Arc::new(AtomicUsize::new(0));
    let root = fully_bound_support_root(
        graph.set.clone(),
        graph.value(0),
        graph.value(1),
        &repeated(graph.attribute, false),
        guarded_value,
        sibling_value,
        1,
        8,
        Arc::clone(&fully_bound_satisfied_calls),
    );
    let mut query = Query::new(root, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);

    let first = query.next().expect("one guarded Union result");
    assert!(first == guarded_value || first == sibling_value);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);
    assert!(query.stats().support_action_pops > 0);
    drop(query);
}

#[test]
fn affine_nested_support_is_permutation_invariant_and_monotone() {
    let parent_values = vec![genid(&rngid().id).raw, genid(&rngid().id).raw];
    let guarded_value = genid(&rngid().id).raw;
    let sibling_value = genid(&rngid().id).raw;
    let mut permutation_bags = Vec::new();

    for arm_order in [SupportArmOrder::FalseFirst, SupportArmOrder::TrueFirst] {
        let mut previous = Vec::new();
        let mut level_bags = Vec::new();
        for level in 0..=4 {
            let graph = GeneratedGraph::new(level);
            let (root, fully_bound_satisfied_calls) = nested_affine_support_root(
                graph.set.clone(),
                graph.value(0),
                graph.value(2),
                graph.primary,
                graph.secondary,
                parent_values.clone(),
                vec![guarded_value],
                sibling_value,
                arm_order,
                None,
            );
            let mut query = Query::new(root, project_outer)
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .cap(1)
                .start_width(1);
            let mut actual: Vec<_> = query.by_ref().collect();
            actual.sort_unstable();

            let mut expected = vec![sibling_value, sibling_value];
            if level >= 2 {
                expected.extend([guarded_value, guarded_value]);
            }
            expected.sort_unstable();
            assert_eq!(actual, expected, "level={level}, order={arm_order:?}");
            assert!(
                sorted_bag_is_subset(&previous, &actual),
                "graph growth retracted an affine Support result at level={level}, order={arm_order:?}"
            );
            assert_eq!(
                fully_bound_satisfied_calls.load(Ordering::Relaxed),
                0,
                "nested Support fell back to legacy reachability at level={level}, order={arm_order:?}"
            );
            assert!(query.stats().support_action_pops > 0);
            assert!(query.stats().delta_transition_pages > 0);
            previous = actual.clone();
            level_bags.push(actual);
        }
        permutation_bags.push(level_bags);
    }

    assert_eq!(
        permutation_bags[0], permutation_bags[1],
        "reordering the false arm and nested true AND changed Boolean Support semantics"
    );
}

#[test]
fn live_affine_support_clones_exactly_and_matches_rayon_worker_counts() {
    let graph = Graph::new(8, &[(0, 1), (0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let parent_values = vec![genid(&rngid().id).raw, genid(&rngid().id).raw];
    let guarded_value = genid(&rngid().id).raw;
    let sibling_value = genid(&rngid().id).raw;
    let make = || {
        nested_affine_support_root(
            graph.set.clone(),
            graph.value(0),
            graph.value(1),
            graph.attribute,
            other_attribute(),
            parent_values.clone(),
            vec![guarded_value],
            sibling_value,
            SupportArmOrder::FalseFirst,
            None,
        )
    };
    let mut expected = vec![guarded_value, guarded_value, sibling_value, sibling_value];
    expected.sort_unstable();

    let (root, fully_bound_satisfied_calls) = make();
    let mut query = Query::new(root, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let first = query.next().expect("the affine formula has four results");
    let examined_at_first = query.stats().delta_transition_candidates_examined;
    assert!(examined_at_first > 0);
    assert!(query.stats().support_action_pops > 0);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);

    let clone = query.clone();
    let remainder: Vec<_> = query.collect();
    let cloned_remainder: Vec<_> = clone.collect();
    assert_eq!(
        cloned_remainder, remainder,
        "a live Support clone changed the exact affine remainder"
    );
    let mut reconstructed: Vec<_> = std::iter::once(first).chain(remainder).collect();
    reconstructed.sort_unstable();
    assert_eq!(reconstructed, expected);

    #[cfg(feature = "parallel")]
    for workers in [1, 4] {
        let (root, fully_bound_satisfied_calls) = make();
        let query = Query::new(root, project_outer)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1);
        let mut actual = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap()
            .install(|| query.into_par_iter().collect::<Vec<_>>());
        actual.sort_unstable();
        assert_eq!(actual, expected, "workers={workers}");
        assert_eq!(
            fully_bound_satisfied_calls.load(Ordering::Relaxed),
            0,
            "workers={workers}"
        );
    }
}

#[test]
fn support_is_parent_atomic_before_candidate_pages() {
    let parent = genid(&rngid().id).raw;
    let guarded_values: Vec<_> = (0..4).map(|_| genid(&rngid().id).raw).collect();
    let sibling_value = genid(&rngid().id).raw;
    let reachable = Graph::new(8, &[(0, 1), (0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (root, fully_bound_satisfied_calls) = nested_affine_support_root(
        reachable.set.clone(),
        reachable.value(0),
        reachable.value(1),
        reachable.attribute,
        other_attribute(),
        vec![parent],
        guarded_values.clone(),
        sibling_value,
        SupportArmOrder::FalseFirst,
        Some(Arc::clone(&trace)),
    );
    let mut query = Query::new(root, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let mut actual: Vec<_> = query.by_ref().collect();
    actual.sort_unstable();
    let mut expected = guarded_values.clone();
    expected.push(sibling_value);
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);
    assert!(query.stats().support_action_pops > 0);

    let trace = trace.lock().expect("support trace poisoned").clone();
    assert_eq!(trace, vec![1, 1, 1, 1, 1]);

    let unreachable = Graph::new(8, &[(0, 1), (0, 2), (2, 3), (3, 4), (4, 5), (5, 6), (7, 7)]);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let (root, fully_bound_satisfied_calls) = nested_affine_support_root(
        unreachable.set.clone(),
        unreachable.value(0),
        unreachable.value(7),
        unreachable.attribute,
        other_attribute(),
        vec![parent],
        guarded_values,
        sibling_value,
        SupportArmOrder::FalseFirst,
        Some(Arc::clone(&trace)),
    );
    let mut query = Query::new(root, project_outer)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![sibling_value]);
    assert_eq!(
        trace.lock().expect("support trace poisoned").as_slice(),
        vec![1],
        "a false Support guard leaked its four guarded candidates into paging"
    );
    assert!(query.stats().support_action_pops > 0);
    assert!(query.stats().delta_transition_pages > 0);
    assert_eq!(fully_bound_satisfied_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn one_term_at_two_program_counters_keeps_both_futures() {
    let graph = Graph::new(2, &[(0, 1)]);
    // ((p / p) | (p / ^p))+. Both arms reach node 1 after their first
    // transition. The left continuation dies there; the right continuation
    // walks back to node 0 and accepts it. Novelty by term alone loses the
    // result, while novelty by (term, program counter) preserves it.
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Concat,
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Inverse,
        PathOp::Concat,
        PathOp::Union,
        PathOp::Plus,
    ];
    assert_all_schedulers(
        || bound_start_root(graph.set.clone(), graph.value(0), &ops),
        project_end,
        vec![graph.value(0).raw],
    );
}

#[test]
fn compound_concat_fixpoint_runs_in_both_endpoint_orientations() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Concat,
        PathOp::Plus,
    ];
    assert_all_schedulers(
        || bound_start_root(graph.set.clone(), graph.value(0), &ops),
        project_end,
        vec![graph.value(2).raw, graph.value(4).raw],
    );
    assert_all_schedulers(
        || bound_end_root(graph.set.clone(), graph.value(4), &ops),
        project_start,
        vec![graph.value(0).raw, graph.value(2).raw],
    );
}

#[test]
fn repeated_negated_attribute_uses_the_same_product_fixpoint() {
    let mut graph = Graph::new(3, &[]);
    let other = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[0], &other, &graph.value(1)));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[1], &other, &graph.value(2)));
    let ops = vec![PathOp::NotAttr(graph.attribute.raw()), PathOp::Plus];
    assert_all_schedulers(
        || bound_start_root(graph.set.clone(), graph.value(0), &ops),
        project_end,
        vec![graph.value(1).raw, graph.value(2).raw],
    );
    assert_all_schedulers(
        || bound_end_root(graph.set.clone(), graph.value(2), &ops),
        project_start,
        vec![graph.value(0).raw, graph.value(1).raw],
    );
}

#[test]
fn all_attr_inverse_and_bound_endpoint_routes_match_oracles() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let forward = repeated(graph.attribute, false);
    let inverse = repeated(graph.attribute, true);
    let cases: Vec<(Root, fn(&Binding) -> Option<RawInline>, Vec<RawInline>)> = vec![
        (
            bound_start_root(graph.set.clone(), graph.value(0), &forward),
            project_end,
            vec![graph.value(1).raw, graph.value(2).raw],
        ),
        (
            bound_start_root(graph.set.clone(), graph.value(2), &inverse),
            project_end,
            vec![graph.value(0).raw, graph.value(1).raw],
        ),
        (
            bound_end_root(graph.set.clone(), graph.value(2), &forward),
            project_start,
            vec![graph.value(0).raw, graph.value(1).raw],
        ),
        (
            bound_end_root(graph.set.clone(), graph.value(0), &inverse),
            project_start,
            vec![graph.value(1).raw, graph.value(2).raw],
        ),
    ];
    for (root, project, mut expected) in cases {
        expected.sort_unstable();
        let residual = run(Arc::clone(&root), Scheduler::Residual, project);
        assert_eq!(residual, expected);
        assert_eq!(run(Arc::clone(&root), Scheduler::Dag, project), expected);
        assert_eq!(run(root, Scheduler::Sequential, project), expected);
    }
}

#[test]
fn target_confirm_traverses_once_and_set_admits_reachable_candidates() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let forward = repeated(graph.attribute, false);
    let inverse = repeated(graph.attribute, true);
    let cases = vec![
        (
            END,
            graph.value(0),
            forward.clone(),
            vec![
                graph.value(2).raw,
                graph.value(3).raw,
                graph.value(2).raw,
                graph.value(1).raw,
            ],
            vec![graph.value(2).raw, graph.value(1).raw],
            project_end as fn(&Binding) -> Option<RawInline>,
        ),
        (
            END,
            graph.value(2),
            inverse.clone(),
            vec![
                graph.value(0).raw,
                graph.value(3).raw,
                graph.value(0).raw,
                graph.value(1).raw,
            ],
            vec![graph.value(0).raw, graph.value(1).raw],
            project_end,
        ),
        (
            START,
            graph.value(2),
            forward,
            vec![
                graph.value(0).raw,
                graph.value(3).raw,
                graph.value(0).raw,
                graph.value(1).raw,
            ],
            vec![graph.value(0).raw, graph.value(1).raw],
            project_start,
        ),
        (
            START,
            graph.value(0),
            inverse,
            vec![
                graph.value(2).raw,
                graph.value(3).raw,
                graph.value(2).raw,
                graph.value(1).raw,
            ],
            vec![graph.value(2).raw, graph.value(1).raw],
            project_start,
        ),
    ];

    for (candidate_variable, bound, ops, candidates, mut expected, project) in cases {
        let root = target_confirm_root(
            graph.set.clone(),
            candidate_variable,
            bound,
            candidates,
            &ops,
        );
        expected.sort_unstable();
        assert_eq!(run(root, Scheduler::Residual, project), expected);
    }
}

#[test]
fn target_confirm_positive_support_yields_occurrence_zero_then_exactly_drains() {
    let mut graph = Graph::new(5, &[]);
    let mut reachable: Vec<_> = (1..5).collect();
    reachable.sort_unstable_by_key(|&node| graph.value(node).raw);
    let mut chain = vec![0];
    chain.extend(reachable.iter().copied());
    for edge in chain.windows(2) {
        graph.set.insert(&Trible::new(
            &graph.nodes[edge[0]],
            &graph.attribute,
            &graph.value(edge[1]),
        ));
    }
    let absent = [u8::MAX; 32];
    let candidates = vec![
        graph.value(reachable[2]).raw,
        graph.value(reachable[3]).raw,
        absent,
        graph.value(reachable[1]).raw,
        graph.value(reachable[0]).raw,
        graph.value(reachable[0]).raw,
    ];
    let support_routes = Arc::new(AtomicUsize::new(0));
    let make = || {
        certified_target_confirm_root(
            graph.set.clone(),
            graph.value(0),
            candidates.clone(),
            &repeated(graph.attribute, false),
            Arc::clone(&support_routes),
        )
    };
    let mut control: Vec<_> = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::Disabled,
        ))
        .start_width(1)
        .cap(1)
        .collect();
    control.sort_unstable();

    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::All,
        ))
        .start_width(1)
        .cap(1);
    let first = query
        .next()
        .expect("occurrence-zero Support should publish its exact witness");
    assert_eq!(
        query.stats().delta_direct_terminal_publication_batches,
        1,
        "the first result must come from positive direct publication"
    );
    assert_eq!(
        query.stats().delta_direct_terminal_publication_rows,
        1,
        "one positive witness must publish exactly one row"
    );
    assert_eq!(support_routes.load(Ordering::Relaxed), 1);
    let early_examined = query.stats().delta_transition_candidates_examined;

    let mut actual = vec![first];
    actual.extend(query.by_ref());
    assert!(
        query.stats().delta_transition_candidates_examined > early_examined,
        "exact Confirm work must remain queued after the positive yield"
    );
    actual.sort_unstable();
    assert_eq!(actual, control);
    assert_eq!(
        actual.iter().filter(|&&value| value == first).count(),
        1,
        "exact Confirm settlement must subtract the already-published value"
    );
    assert!(actual.contains(&graph.value(reachable[1]).raw));
    assert!(actual.contains(&graph.value(reachable[3]).raw));
}

#[test]
fn target_confirm_positive_support_does_not_feed_past_false_occurrence_zero() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let candidates = vec![[0; 32], graph.value(1).raw, [u8::MAX; 32]];
    let support_routes = Arc::new(AtomicUsize::new(0));
    let make = || {
        certified_target_confirm_root(
            graph.set.clone(),
            graph.value(0),
            candidates.clone(),
            &repeated(graph.attribute, false),
            Arc::clone(&support_routes),
        )
    };
    let mut control: Vec<_> = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::Disabled,
        ))
        .start_width(3)
        .cap(3)
        .collect();
    control.sort_unstable();

    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::All,
        ))
        .start_width(3)
        .cap(3);
    let mut actual: Vec<_> = query.by_ref().collect();
    actual.sort_unstable();
    assert_eq!(actual, control);
    assert_eq!(
        support_routes.load(Ordering::Relaxed),
        1,
        "v1 must select exactly one occurrence-zero Support route"
    );
    assert_eq!(
        query.stats().delta_direct_terminal_publication_batches,
        0,
        "v1 must not feed occurrence one after occurrence-zero Support is false"
    );
    assert!(actual.contains(&graph.value(1).raw));
    assert!(query.stats().delta_transition_candidates_examined > 0);
}

#[test]
fn target_confirm_nullable_support_seed_is_not_publication_authority() {
    let graph = Graph::new(2, &[(0, 1)]);
    let start = graph.value(0).raw;
    let candidates = vec![start, start, start];
    let support_routes = Arc::new(AtomicUsize::new(0));
    let make = || {
        certified_target_confirm_root(
            graph.set.clone(),
            graph.value(0),
            candidates.clone(),
            &[PathOp::Attr(graph.attribute.raw()), PathOp::Star],
            Arc::clone(&support_routes),
        )
    };
    let control: Vec<_> = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::Disabled,
        ))
        .start_width(1)
        .cap(1)
        .collect();

    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::All,
        ))
        .start_width(1)
        .cap(1);
    let actual: Vec<_> = query.by_ref().collect();
    assert_eq!(actual, control);
    assert_eq!(actual, [start]);
    assert_eq!(
        support_routes.load(Ordering::Relaxed),
        1,
        "v1 must select only occurrence-zero Support"
    );
    assert_eq!(
        query.stats().delta_direct_terminal_publication_batches,
        0,
        "nullable seed acceptance is not a runtime Support receipt"
    );
    assert!(query.stats().delta_transition_candidates_examined > 0);
}

#[test]
fn positive_support_gate_precedes_partial_rpq_optimistic_support_selection() {
    let graph = Graph::new(5, &[(4, 0), (1, 2), (2, 3)]);
    let candidates = vec![graph.value(0).raw, graph.value(1).raw];
    let support_routes = Arc::new(AtomicUsize::new(0));
    let make = || {
        let start = Variable::<GenId>::new(START);
        let end = Variable::<GenId>::new(END);
        let rpq = RegularPathConstraint::new(
            graph.set.clone(),
            start,
            end,
            &repeated(graph.attribute, false),
        );
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(CertifiedOrderedDomain(OrderedDomain {
                variable: START,
                gate: END,
                unbound_estimate: 0,
                values: candidates.clone(),
            })) as DynConstraint,
            Box::new(ProbedConfirmRpq {
                program: PreferredProgram::new(
                    SupportRouteProbe {
                        calls: Arc::clone(&support_routes),
                    },
                    rpq,
                ),
                covering_proposals: true,
            }) as DynConstraint,
        ]))
    };
    let mut sequential: Vec<_> = Query::new(make(), project_pair).sequential().collect();
    let mut disabled: Vec<_> = Query::new(make(), project_pair)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::Disabled,
        ))
        .collect();
    sequential.sort_unstable();
    disabled.sort_unstable();
    assert_eq!(disabled, sequential);

    let mut query = Query::new(make(), project_pair)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::All,
        ))
        .start_width(1)
        .cap(1);
    let mut actual: Vec<_> = query.by_ref().collect();
    actual.sort_unstable();
    assert_eq!(actual, disabled);
    let mut expected = vec![
        (graph.value(1).raw, graph.value(2).raw),
        (graph.value(1).raw, graph.value(3).raw),
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(
        support_routes.load(Ordering::Relaxed),
        0,
        "the full-bound gate must precede optimistic partial Support selection"
    );
    assert!(
        query.stats().confirm_calls > 0
            && query.stats().candidates_confirmed >= candidates.len()
            && query.stats().delta_transition_candidates_examined > 0,
        "the exact partial Confirm must still execute"
    );
}

#[test]
fn automaton_target_confirm_filters_then_set_admits_the_sequence() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Optional,
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Plus,
        PathOp::Union,
    ];
    let absent = genid(&rngid().id);
    let candidates = vec![
        graph.value(2).raw,
        graph.value(0).raw,
        graph.value(2).raw,
        absent.raw,
        graph.value(1).raw,
    ];
    let expected = vec![graph.value(2).raw, graph.value(0).raw, graph.value(1).raw];
    let residual = run(
        target_confirm_root(
            graph.set.clone(),
            END,
            graph.value(0),
            candidates.clone(),
            &ops,
        ),
        Scheduler::Residual,
        project_end,
    );
    let dag = run(
        target_confirm_root(graph.set.clone(), END, graph.value(0), candidates, &ops),
        Scheduler::Dag,
        project_end,
    );
    let mut expected = expected;
    expected.sort_unstable();
    assert_eq!(residual, expected);
    assert_eq!(dag, expected);
}

#[test]
fn bound_literal_endpoint_uses_the_inverse_delta_route() {
    let mut graph = Graph::new(2, &[]);
    let literal = Inline::<UnknownInline>::new([0xA5; 32]);
    graph
        .set
        .insert(&Trible::new(&graph.nodes[0], &graph.attribute, &literal));
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<UnknownInline>::new(END);
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(end_var.is(literal)) as DynConstraint,
        Box::new(RegularPathConstraint::new(
            graph.set.clone(),
            start_var,
            end_var,
            &repeated(graph.attribute, false),
        )) as DynConstraint,
    ]));

    assert_eq!(
        run(root, Scheduler::Residual, project_start),
        vec![graph.value(0).raw]
    );
}

#[test]
fn two_free_distinct_endpoints_page_the_first_binding_before_traversal() {
    let graph = Graph::new(
        16,
        &[
            (0, 1),
            (2, 3),
            (4, 5),
            (6, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
        ],
    );
    let root = two_free_root(graph.set.clone(), &repeated(graph.attribute, false));
    let expected: Vec<_> = (0..8)
        .map(|edge| (graph.value(edge * 2).raw, graph.value(edge * 2 + 1).raw))
        .collect();

    let mut query = Query::new(root, project_pair)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let first = query.next().expect("one two-free RPQ edge");
    assert!(expected.contains(&first));
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    drop(query);
}

#[test]
fn two_free_path_end_pages_the_inverse_first_frontier() {
    let graph = Graph::new(5, &[(0, 1), (2, 1), (3, 4)]);
    let path_end = Variable::<GenId>::new(START);
    let path_start = Variable::<GenId>::new(END);
    let ops = vec![PathOp::Attr(graph.attribute.raw())];

    let make_root = || {
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(RegularPathConstraint::new(
                graph.set.clone(),
                path_start,
                path_end,
                &ops,
            )) as DynConstraint,
        ]))
    };
    let mut sequential: Vec<_> = Query::new(make_root(), project_pair).sequential().collect();
    let mut residual: Vec<_> = Query::new(make_root(), project_pair)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
    sequential.sort_unstable();
    residual.sort_unstable();
    assert_eq!(residual, sequential);
    let mut expected = vec![
        (graph.value(1).raw, graph.value(0).raw),
        (graph.value(1).raw, graph.value(2).raw),
        (graph.value(4).raw, graph.value(3).raw),
    ];
    expected.sort_unstable();
    assert_eq!(residual, expected);
}

#[test]
fn nullable_two_free_first_frontier_is_exactly_the_graph_term_union() {
    let graph = Graph::new(3, &[(0, 1)]);
    let ops = vec![PathOp::Attr(graph.attribute.raw()), PathOp::Optional];
    let make_root = || two_free_root(graph.set.clone(), &ops);
    let mut sequential: Vec<_> = Query::new(make_root(), project_pair).sequential().collect();
    let mut residual: Vec<_> = Query::new(make_root(), project_pair)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
    sequential.sort_unstable();
    residual.sort_unstable();
    assert_eq!(residual, sequential);
    let mut expected = vec![
        (graph.value(0).raw, graph.value(0).raw),
        (graph.value(0).raw, graph.value(1).raw),
        (graph.value(1).raw, graph.value(1).raw),
    ];
    expected.sort_unstable();
    assert_eq!(residual, expected);
}

#[test]
fn duplicate_outer_parents_preserve_endpoint_bag_multiplicity() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make_root =
        || duplicate_parent_root(graph.set.clone(), graph.value(0).raw, outer_values, &ops);
    assert_all_schedulers(
        make_root,
        project_end,
        vec![
            graph.value(1).raw,
            graph.value(1).raw,
            graph.value(2).raw,
            graph.value(2).raw,
        ],
    );
}

#[test]
fn conservative_residual_lowering_keeps_plus_opaque() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
    );
    let mut query = Query::new(root, project_end).solve_residual_state_lazy();
    let mut actual: Vec<_> = query.by_ref().collect();
    actual.sort_unstable();
    let mut expected = [graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(
        query.stats().delta_transition_pages,
        0,
        "cyclic RPQ proposal lowering must remain explicitly opt-in"
    );

    let root = target_confirm_root(
        graph.set.clone(),
        END,
        graph.value(0),
        vec![
            graph.value(2).raw,
            graph.value(0).raw,
            graph.value(2).raw,
            graph.value(1).raw,
        ],
        &repeated(graph.attribute, false),
    );
    let mut query = Query::new(root, project_end).solve_residual_state_lazy();
    let mut actual: Vec<_> = query.by_ref().collect();
    actual.sort_unstable();
    let mut expected = [graph.value(2).raw, graph.value(1).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(
        query.stats().delta_transition_pages,
        0,
        "cyclic RPQ confirmation lowering must remain explicitly opt-in"
    );
}

#[test]
fn first_result_requires_one_expansion_and_drop_cancels_the_remainder() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
    );
    let mut query =
        Query::new(root, project_end).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), Some(graph.value(1).raw));
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    drop(query);
}

#[test]
fn nullable_seed_is_first_result_without_transition_work_and_keeps_affine_bags() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = [PathOp::Attr(graph.attribute.raw()), PathOp::Star];
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make = || duplicate_parent_root(graph.set.clone(), graph.value(0).raw, outer_values, &ops);
    let mut expected: Vec<_> = Query::new(make(), project_end).sequential().collect();
    expected.sort_unstable();
    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);

    let first = query.next().expect("nullable seed endpoint");
    assert_eq!(first, graph.value(0).raw);
    assert_eq!(query.stats().delta_transition_pages, 0);
    assert_eq!(query.stats().delta_transition_cohorts, 0);
    assert_eq!(query.stats().delta_transition_candidates_examined, 0);
    assert_eq!(query.current_width(), 1);
    assert_eq!(query.stats().width_increases, 0);

    let cloned = query.clone();
    let mut original = vec![first];
    original.extend(query);
    let mut clone_results = vec![first];
    clone_results.extend(cloned);
    original.sort_unstable();
    clone_results.sort_unstable();
    assert_eq!(original, expected);
    assert_eq!(clone_results, expected);
    for node in 0..5 {
        assert_eq!(
            original
                .iter()
                .filter(|&&value| value == graph.value(node).raw)
                .count(),
            2,
            "one affine parent copy was lost for node {node}"
        );
    }

    let mut dropped = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    assert_eq!(dropped.next(), Some(graph.value(0).raw));
    assert_eq!(dropped.stats().delta_transition_pages, 0);
    drop(dropped);

    let absent = genid(&rngid().id);
    assert!(
        Query::new(
            bound_start_root(graph.set.clone(), absent, &ops),
            project_end,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1)
        .next()
        .is_none(),
        "nullable seed publication admitted a non-graph term"
    );
}

#[test]
fn clone_after_first_result_has_two_independent_exact_remainders() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
    );
    let mut query =
        Query::new(root, project_end).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.next(), Some(graph.value(1).raw));
    let clone = query.clone();

    let mut left: Vec<_> = query.collect();
    let mut right: Vec<_> = clone.collect();
    left.sort_unstable();
    right.sort_unstable();
    assert_eq!(left, right);
    let mut expected = vec![graph.value(2).raw, graph.value(3).raw, graph.value(4).raw];
    expected.sort_unstable();
    assert_eq!(left, expected);
}

#[test]
fn clone_with_a_suspended_same_variable_cursor_has_two_exact_remainders() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = vec![PathOp::Attr(graph.attribute.raw()), PathOp::Star];
    let root = same_variable_root(graph.set.clone(), &ops);
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    let first = query
        .next()
        .expect("nullable first source is immediately stable");
    let clone = query.clone();

    let mut left: Vec<_> = query.collect();
    let mut right: Vec<_> = clone.collect();
    left.sort_unstable();
    right.sort_unstable();
    assert_eq!(left, right);
    let mut expected: Vec<_> = (0..5).map(|node| graph.value(node).raw).collect();
    let removed = expected
        .iter()
        .position(|value| *value == first)
        .expect("the first result belongs to NODES(G)");
    expected.remove(removed);
    expected.sort_unstable();
    assert_eq!(left, expected);
}

#[test]
fn generated_product_programs_match_sequential_and_dag_bags() {
    let edge_universe = [(0, 0), (0, 1), (0, 2), (1, 2), (2, 3), (3, 0)];
    for mask in 0u16..64 {
        let edges: Vec<_> = edge_universe
            .iter()
            .enumerate()
            .filter_map(|(bit, &edge)| (mask & (1 << bit) != 0).then_some(edge))
            .collect();
        let graph = Graph::new(4, &edges);
        let attribute = graph.attribute.raw();
        let expressions = [
            vec![PathOp::Attr(attribute), PathOp::Plus],
            vec![PathOp::Attr(attribute), PathOp::Star],
            vec![
                PathOp::Attr(attribute),
                PathOp::Attr(attribute),
                PathOp::Concat,
                PathOp::Plus,
            ],
            vec![
                PathOp::Attr(attribute),
                PathOp::Optional,
                PathOp::Attr(attribute),
                PathOp::Plus,
                PathOp::Union,
            ],
            vec![
                PathOp::Attr(attribute),
                PathOp::Attr(attribute),
                PathOp::Concat,
                PathOp::Attr(attribute),
                PathOp::Attr(attribute),
                PathOp::Inverse,
                PathOp::Concat,
                PathOp::Union,
                PathOp::Plus,
            ],
        ];
        for ops in expressions {
            let make_root = || bound_start_root(graph.set.clone(), graph.value(0), &ops);
            let residual = run(make_root(), Scheduler::Residual, project_end);
            assert_eq!(residual, run(make_root(), Scheduler::Dag, project_end));
            assert_eq!(
                residual,
                run(make_root(), Scheduler::Sequential, project_end)
            );

            let make_same_root = || same_variable_root(graph.set.clone(), &ops);
            let ordinary = run(make_same_root(), Scheduler::Ordinary, project_start);
            assert_eq!(
                ordinary,
                run(make_same_root(), Scheduler::Residual, project_start)
            );
            assert_eq!(
                ordinary,
                run(make_same_root(), Scheduler::Dag, project_start)
            );
            assert_eq!(
                ordinary,
                run(make_same_root(), Scheduler::Sequential, project_start)
            );
        }
    }
}

#[test]
fn generated_combined_formula_rpq_matrix_matches_frozen_schedulers_and_is_monotone() {
    let programs = [
        GeneratedPathProgram::Attr,
        GeneratedPathProgram::Optional,
        GeneratedPathProgram::Inverse,
        GeneratedPathProgram::Concat,
        GeneratedPathProgram::UnionInverse,
        GeneratedPathProgram::Negated,
        GeneratedPathProgram::Plus,
        GeneratedPathProgram::Star,
        GeneratedPathProgram::InversePlus,
        GeneratedPathProgram::ConcatPlus,
        GeneratedPathProgram::UnionInversePlus,
        GeneratedPathProgram::NegatedPlus,
    ];
    let formulas = [
        GeneratedFormulaCase::Atom,
        GeneratedFormulaCase::PageLocalAnd,
        GeneratedFormulaCase::BarrierAnd,
        GeneratedFormulaCase::RepeatedRecursive,
    ];
    let lowering_cases = [
        ("opaque", ResidualLowering::CONSERVATIVE),
        (
            "union-leaves",
            ResidualLowering::new(FormulaScope::UnionLeaves, ProgramScope::Disabled),
        ),
        (
            "whole-root",
            ResidualLowering::new(FormulaScope::WholeRoot, ProgramScope::Disabled),
        ),
        (
            "opaque-transitions",
            ResidualLowering::new(FormulaScope::OpaqueLeaves, ProgramScope::All),
        ),
        (
            "union-leaves-transitions",
            ResidualLowering::new(FormulaScope::UnionLeaves, ProgramScope::All),
        ),
        ("whole-root-transitions", ResidualLowering::FULL),
    ];
    let mut saw_root_cyclic_probe_one = false;

    for program in programs {
        let mut bags_by_formula = Vec::new();
        for formula in formulas {
            let mut bags_by_level = Vec::new();
            for level in 0..=4 {
                let graph = GeneratedGraph::new(level);
                let ops = program.ops(graph.primary, graph.secondary);
                let make_root = || generated_formula_root(&graph, &ops, formula);
                let expected = run(make_root(), Scheduler::Sequential, project_end);

                assert_eq!(
                    run(make_root(), Scheduler::Ordinary, project_end),
                    expected,
                    "level={level} program={program:?} formula={formula:?} ordinary"
                );
                assert_eq!(
                    run(make_root(), Scheduler::Dag, project_end),
                    expected,
                    "level={level} program={program:?} formula={formula:?} LazyDag"
                );

                for &(capability, lowering) in &lowering_cases {
                    let mut query = Query::new(make_root(), project_end)
                        .solve_residual_state_lazy_with(lowering)
                        .cap(1)
                        .start_width(1);
                    let mut actual: Vec<_> = query.by_ref().collect();
                    actual.sort_unstable();
                    assert_eq!(
                        actual, expected,
                        "level={level} program={program:?} formula={formula:?} capability={capability}"
                    );
                    if capability == "whole-root-transitions"
                        && program == GeneratedPathProgram::Plus
                        && formula == GeneratedFormulaCase::PageLocalAnd
                        && query.stats().delta_handoff_probe_pops > 0
                    {
                        saw_root_cyclic_probe_one = true;
                    }
                }
                bags_by_level.push(expected);
            }

            for (level, pair) in bags_by_level.windows(2).enumerate() {
                assert!(
                    sorted_bag_is_subset(&pair[0], &pair[1]),
                    "adding graph facts retracted results: level={level}->{} program={program:?} formula={formula:?} before={:?} after={:?}",
                    level + 1,
                    pair[0],
                    pair[1]
                );
            }
            bags_by_formula.push(bags_by_level);
        }

        assert_eq!(
            bags_by_formula[1], bags_by_formula[2],
            "page-locality changed semantics for program={program:?}"
        );
    }

    assert!(
        saw_root_cyclic_probe_one,
        "the generated width-one root+cyclic lane never exercised a streamed handoff probe"
    );

    // The recursive bag cannot reveal accidental occurrence collapse because
    // its two arms denote the same relation. Pin the structural invariant once
    // through the diagnostic-only shadow surface.
    let graph = GeneratedGraph::new(4);
    let ops = GeneratedPathProgram::Plus.ops(graph.primary, graph.secondary);
    let observed = Query::new(
        generated_formula_root(&graph, &ops, GeneratedFormulaCase::RepeatedRecursive),
        project_end,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .shadow(ResidualShadowEpoch::new())
    .collect_profiled();
    let mut occurrences: Vec<_> = observed
        .shadow
        .events
        .iter()
        .filter(|event| event.site.verb == ActionVerb::Propose && event.site.variable == END)
        .map(|event| event.site.leaf_occurrence)
        .collect();
    occurrences.sort_unstable();
    occurrences.dedup();
    assert_eq!(occurrences.len(), 2);
}

#[test]
fn finite_path_families_use_native_transition_programs() {
    let mut graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let other = other_attribute();
    graph
        .set
        .insert(&Trible::new(&graph.nodes[0], &other, &graph.value(2)));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[2], &other, &graph.value(1)));
    let primary = graph.attribute.raw();
    let cases = [
        (
            "attr",
            vec![PathOp::Attr(primary)],
            graph.value(0),
            vec![graph.value(1).raw],
        ),
        (
            "concat",
            vec![PathOp::Attr(primary), PathOp::Attr(primary), PathOp::Concat],
            graph.value(0),
            vec![graph.value(2).raw],
        ),
        (
            "union",
            vec![
                PathOp::Attr(primary),
                PathOp::NotAttr(primary),
                PathOp::Union,
            ],
            graph.value(0),
            vec![graph.value(1).raw, graph.value(2).raw],
        ),
        (
            "optional",
            vec![PathOp::Attr(primary), PathOp::Optional],
            graph.value(0),
            vec![graph.value(0).raw, graph.value(1).raw],
        ),
        (
            "inverse",
            vec![PathOp::Attr(primary), PathOp::Inverse],
            graph.value(2),
            vec![graph.value(1).raw],
        ),
        (
            "negated",
            vec![PathOp::NotAttr(primary)],
            graph.value(0),
            vec![graph.value(2).raw],
        ),
        (
            "inverse-negated",
            vec![PathOp::NotAttr(primary), PathOp::Inverse],
            graph.value(1),
            vec![graph.value(2).raw],
        ),
    ];

    for (name, ops, start, mut expected) in cases {
        let root = bound_start_root(graph.set.clone(), start, &ops);
        expected.sort_unstable();
        let mut query =
            Query::new(root, project_end).solve_residual_state_lazy_with(combined_effects());
        let mut residual: Vec<_> = query.by_ref().collect();
        residual.sort_unstable();
        assert_eq!(residual, expected, "{name}");
        assert_eq!(
            run(
                bound_start_root(graph.set.clone(), start, &ops),
                Scheduler::Dag,
                project_end,
            ),
            expected,
            "{name} DAG oracle"
        );
        assert!(query.stats().delta_transition_pages > 0, "{name}");
    }
}

#[test]
fn finite_bound_end_uses_the_inverse_transition_program() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = vec![PathOp::Attr(graph.attribute.raw())];
    let root = bound_end_root(graph.set.clone(), graph.value(2), &ops);

    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(1).raw]);
    assert!(query.stats().delta_transition_pages > 0);
}

#[test]
fn finite_concat_first_result_takes_only_its_two_transition_steps() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Concat,
    ];
    let root = bound_start_root(graph.set.clone(), graph.value(0), &ops);
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .start_width(1);

    assert_eq!(query.next(), Some(graph.value(2).raw));
    assert_eq!(query.stats().delta_transition_candidates_examined, 2);
    drop(query);
}

#[test]
fn finite_confirm_keeps_geometric_pages_then_set_admits() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let absent = genid(&rngid().id).raw;
    let candidates = vec![
        graph.value(2).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        absent,
        graph.value(0).raw,
    ];
    let ops = vec![
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Attr(graph.attribute.raw()),
        PathOp::Concat,
    ];
    let expected = {
        let mut values = vec![graph.value(2).raw];
        values.sort_unstable();
        values
    };

    for (name, capabilities) in [
        ("leaf", combined_effects()),
        ("formula", root_formula_effects()),
    ] {
        let root = target_confirm_root(
            graph.set.clone(),
            END,
            graph.value(0),
            candidates.clone(),
            &ops,
        );
        let mut query = Query::new(root, project_end)
            .solve_residual_state_lazy_with(capabilities)
            .start_width(1)
            .cap(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{name}");
        assert!(query.stats().delta_transition_pages > 0, "{name}");
        assert_eq!(query.stats().max_confirm_candidates, 1, "{name}");
    }
}

#[test]
fn finite_same_variable_optional_pages_preserve_epsilon_scope_then_set_admit() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let absent = genid(&rngid().id).raw;
    let candidates = vec![
        graph.value(0).raw,
        graph.value(0).raw,
        graph.value(1).raw,
        absent,
    ];
    let ops = vec![PathOp::Attr(graph.attribute.raw()), PathOp::Optional];
    let expected = {
        let mut values = vec![graph.value(0).raw, graph.value(1).raw];
        values.sort_unstable();
        values
    };

    for (name, capabilities, source_work, source_roots) in [
        ("leaf", combined_effects(), 3, 2),
        ("formula", root_formula_effects(), 3, 2),
    ] {
        let root = same_variable_confirm_root(graph.set.clone(), candidates.clone(), &ops);
        let mut query = Query::new(root, project_start)
            .solve_residual_state_lazy_with(capabilities)
            .start_width(1)
            .cap(1);
        let mut actual: Vec<_> = query.by_ref().collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "{name}");
        assert_eq!(query.stats().delta_source_pages, source_work, "{name}");
        assert_eq!(
            query.stats().delta_source_candidates_examined,
            source_work,
            "{name}"
        );
        assert_eq!(query.stats().delta_source_roots, source_roots, "{name}");
        assert!(query.stats().delta_transition_pages > 0, "{name}");
        assert_eq!(query.stats().max_confirm_candidates, 1, "{name}");
    }
}

#[test]
fn positive_transition_frontiers_page_by_automaton_branch_and_value() {
    let graph = Graph::new(6, &[(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)]);
    let make = || {
        bound_start_root(
            graph.set.clone(),
            graph.value(0),
            &[PathOp::Attr(graph.attribute.raw())],
        )
    };
    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(2)
        .cap(2);
    let mut actual: Vec<_> = query.by_ref().collect();
    let mut expected: Vec<_> = (1..6).map(|index| graph.value(index).raw).collect();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(query.stats().delta_transition_candidates_examined, 5);
    assert!(query.stats().delta_transition_pages >= 3);
}

#[test]
fn first_fanout_result_scans_one_transition_and_clone_keeps_the_exact_cursor() {
    let edges: Vec<_> = (1..17).map(|target| (0, target)).collect();
    let graph = Graph::new(17, &edges);
    let ops = [PathOp::Attr(graph.attribute.raw())];
    let make = || bound_start_root(graph.set.clone(), graph.value(0), &ops);
    let mut query = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(16);

    let first = query.next().expect("one fanout endpoint");
    assert_eq!(query.stats().delta_transition_pages, 1);
    assert_eq!(query.stats().delta_transition_cohorts, 1);
    assert_eq!(query.stats().max_delta_transition_cohort, 1);
    assert_eq!(query.stats().delta_transition_candidates_examined, 1);
    let cloned = query.clone();

    let mut original = vec![first];
    original.extend(query);
    let mut clone_results = vec![first];
    clone_results.extend(cloned);
    original.sort_unstable();
    clone_results.sort_unstable();
    let mut expected: Vec<_> = (1..17).map(|target| graph.value(target).raw).collect();
    expected.sort_unstable();
    assert_eq!(original, expected);
    assert_eq!(clone_results, expected);
}

#[test]
fn paged_transitions_preserve_affine_parent_bags_and_storage_monotonicity() {
    let graph = Graph::new(5, &[(0, 1), (0, 2), (0, 3), (0, 4)]);
    let ops = [PathOp::Attr(graph.attribute.raw())];
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make = || duplicate_parent_root(graph.set.clone(), graph.value(0).raw, outer_values, &ops);
    let mut expected: Vec<_> = Query::new(make(), project_end).sequential().collect();
    let mut dag: Vec<_> = Query::new(make(), project_end)
        .lazy_dag_scheduler()
        .collect();
    let mut residual = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(2);
    let mut actual: Vec<_> = residual.by_ref().collect();
    expected.sort_unstable();
    dag.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(dag, expected);
    for target in 1..5 {
        assert_eq!(
            actual
                .iter()
                .filter(|&&value| value == graph.value(target).raw)
                .count(),
            2
        );
    }
    assert!(residual.stats().delta_transition_pages > 0);

    let mut previous = Vec::new();
    for level in 0..=4 {
        let graph = GeneratedGraph::new(level);
        let ops = [PathOp::Attr(graph.primary.raw()), PathOp::Star];
        let mut query = Query::new(
            bound_start_root(graph.set.clone(), graph.value(0), &ops),
            project_end,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(2);
        let mut current: Vec<_> = query.by_ref().collect();
        current.sort_unstable();
        current.dedup();
        assert!(
            previous.iter().all(|value| current.contains(value)),
            "adding graph facts removed a nullable/cyclic endpoint at level {level}"
        );
        assert!(query.stats().delta_transition_pages > 0);
        previous = current;
    }
}

#[test]
fn negated_transition_pages_count_rejections_and_emit_each_destination_once() {
    let mut graph = Graph::new(5, &[]);
    let excluded = graph.attribute;
    let other = other_attribute();
    let later = later_attribute();
    let another = third_attribute();
    let literal = Inline::<UnknownInline>::new([0xA5; 32]);
    let mut destinations = vec![
        graph.value(1).raw,
        graph.value(2).raw,
        graph.value(3).raw,
        graph.value(4).raw,
        literal.raw,
    ];
    destinations.sort_unstable();
    let start = graph.nodes[0].id;
    let mut insert = |attribute: &Id, value: RawInline| {
        graph.set.insert(&Trible::new(
            ExclusiveId::force_ref(&start),
            attribute,
            &Inline::<UnknownInline>::new(value),
        ));
    };

    // The first and fourth destinations are rejected after examination. The
    // second proves the strict-successor case because `excluded` sorts before
    // `later`; the second and third occur under multiple attributes but emit
    // only once. The fifth keeps untouched work behind the first query result.
    insert(&excluded, destinations[0]);
    insert(&excluded, destinations[1]);
    insert(&later, destinations[1]);
    insert(&other, destinations[2]);
    insert(&another, destinations[2]);
    insert(&excluded, destinations[3]);
    insert(&other, destinations[4]);

    let ops = [PathOp::NotAttr(excluded.raw())];
    let mut query = Query::new(
        bound_start_root(graph.set.clone(), genid(&start), &ops),
        project_end,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .start_width(1)
    .cap(2);
    assert!(query.next().is_some());
    assert_eq!(query.stats().delta_transition_dead_pages, 1);
    assert_eq!(query.stats().delta_transition_negative_steps, 1);
    assert_eq!(query.stats().delta_source_negative_steps, 0);
    assert_eq!(query.stats().width_increases, 1);
    assert_eq!(query.stats().delta_transition_candidates_examined, 3);
    drop(query);
}

#[test]
fn inverse_negated_transition_pages_from_literals_are_exact_and_distinct() {
    let mut graph = Graph::new(3, &[]);
    let excluded = graph.attribute;
    let other = other_attribute();
    let later = later_attribute();
    let another = third_attribute();
    let literal = Inline::<UnknownInline>::new([0xA5; 32]);
    let mut subjects = vec![0usize, 1, 2];
    subjects.sort_unstable_by_key(|&subject| graph.value(subject).raw);

    graph
        .set
        .insert(&Trible::new(&graph.nodes[subjects[0]], &excluded, &literal));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[subjects[1]], &excluded, &literal));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[subjects[1]], &later, &literal));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[subjects[2]], &other, &literal));
    graph
        .set
        .insert(&Trible::new(&graph.nodes[subjects[2]], &another, &literal));

    let start_var = Variable::<UnknownInline>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(literal)) as DynConstraint,
        Box::new(RegularPathConstraint::new(
            graph.set.clone(),
            start_var,
            end_var,
            &[PathOp::NotAttr(excluded.raw()), PathOp::Inverse],
        )) as DynConstraint,
    ]));
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    let actual: Vec<_> = query.by_ref().collect();
    assert_eq!(
        actual,
        subjects[1..]
            .iter()
            .map(|&subject| graph.value(subject).raw)
            .collect::<Vec<_>>()
    );
    assert_eq!(query.stats().delta_transition_candidates_examined, 3);
}

#[test]
fn mixed_transition_pages_preserve_cycles_clone_drop_affine_bags_and_monotonicity() {
    let graph = GeneratedGraph::new(4);
    let ops = [
        PathOp::Attr(graph.primary.raw()),
        PathOp::NotAttr(graph.primary.raw()),
        PathOp::Union,
        PathOp::Plus,
    ];
    let make = || bound_start_root(graph.set.clone(), graph.value(0), &ops);
    let mut expected: Vec<_> = Query::new(make(), project_end).sequential().collect();
    expected.sort_unstable();

    let mut residual = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    let first = residual.next().expect("the first mixed-path endpoint");
    assert_eq!(residual.stats().delta_transition_pages, 1);
    assert_eq!(residual.stats().delta_transition_candidates_examined, 1);
    let cloned = residual.clone();
    let mut original = vec![first];
    original.extend(residual);
    let mut clone_results = vec![first];
    clone_results.extend(cloned);
    original.sort_unstable();
    clone_results.sort_unstable();
    assert_eq!(original, expected);
    assert_eq!(clone_results, expected);

    let mut dropped = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    assert!(dropped.next().is_some());
    assert_eq!(dropped.stats().delta_transition_candidates_examined, 1);
    drop(dropped);

    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make_affine =
        || duplicate_parent_root(graph.set.clone(), graph.value(0).raw, outer_values, &ops);
    let mut sequential: Vec<_> = Query::new(make_affine(), project_end)
        .sequential()
        .collect();
    let mut dag: Vec<_> = Query::new(make_affine(), project_end)
        .lazy_dag_scheduler()
        .collect();
    let mut affine = Query::new(make_affine(), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(2);
    let mut actual: Vec<_> = affine.by_ref().collect();
    sequential.sort_unstable();
    dag.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, sequential);
    assert_eq!(actual, dag);
    assert!(affine.stats().delta_transition_pages > 0);

    let mut previous = Vec::new();
    for level in 0..=4 {
        let graph = GeneratedGraph::new(level);
        let ops = [
            PathOp::Attr(graph.primary.raw()),
            PathOp::NotAttr(graph.primary.raw()),
            PathOp::Union,
        ];
        let mut query = Query::new(
            bound_start_root(graph.set.clone(), graph.value(0), &ops),
            project_end,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(2);
        let mut current: Vec<_> = query.by_ref().collect();
        current.sort_unstable();
        current.dedup();
        assert!(
            previous.iter().all(|value| current.contains(value)),
            "adding graph facts removed a mixed-path endpoint at level {level}"
        );
        assert!(query.stats().delta_transition_pages > 0);
        previous = current;
    }
}
