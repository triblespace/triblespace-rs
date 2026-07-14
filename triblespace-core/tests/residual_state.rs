use std::sync::{Arc, Mutex};

use triblespace_core::inline::RawInline;
use triblespace_core::query::equalityconstraint::EqualityConstraint;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, VariableId, VariableSet,
};

// Deliberately put x before p. The residual solver first binds p, then has to
// insert x before the existing p column when it commits x's candidates.
const X: VariableId = 0;
const P: VariableId = 1;
const P_MARKER: u8 = b'p';
const X_MARKER: u8 = b'x';

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Child {
    Domain,
    A,
    B,
    C,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verb {
    Propose,
    Confirm,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Call {
    child: Child,
    verb: Verb,
    variable: VariableId,
    rows: usize,
    candidates_before: usize,
    candidates_after: usize,
    tagged: bool,
    active_unbound: bool,
    tags_valid: bool,
}

#[derive(Default)]
struct Trace {
    calls: Vec<Call>,
}

#[derive(Clone)]
struct TableChild {
    child: Child,
    parents: usize,
    domain_estimate: usize,
    trace: Arc<Mutex<Trace>>,
}

impl TableChild {
    fn domain(parents: usize, estimate: usize, trace: Arc<Mutex<Trace>>) -> Self {
        Self {
            child: Child::Domain,
            parents,
            domain_estimate: estimate,
            trace,
        }
    }

    fn relation(child: Child, parents: usize, trace: Arc<Mutex<Trace>>) -> Self {
        assert!(matches!(child, Child::A | Child::B | Child::C));
        Self {
            child,
            parents,
            domain_estimate: usize::MAX,
            trace,
        }
    }

    fn record(
        &self,
        verb: Verb,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &CandidateSink<'_>,
        before: usize,
    ) {
        let active_unbound = view.col(variable).is_none();
        let tagged = matches!(candidates, CandidateSink::Tagged(_));
        let mut tags_valid = true;
        candidates.for_each(|row, _| tags_valid &= (row as usize) < view.len());

        // These are protocol invariants as well as observations. In
        // particular, x must remain speculative while A/B/C inspect it.
        assert!(active_unbound, "active variable leaked into RowsView");
        assert!(
            tags_valid,
            "constraint received an invalid candidate row tag"
        );
        self.trace.lock().unwrap().calls.push(Call {
            child: self.child,
            verb,
            variable,
            rows: view.len(),
            candidates_before: before,
            candidates_after: candidates.len(),
            tagged,
            active_unbound,
            tags_valid,
        });
    }

    fn estimate_relation(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        row: &[RawInline],
    ) -> usize {
        match variable {
            P => {
                if view.col(X).is_some() {
                    1
                } else {
                    1_024
                }
            }
            X => match view.col(P) {
                None => 1_024,
                Some(column) => {
                    let parent = decode(&row[column], P_MARKER).expect("fixture parent encoding");
                    match self.child {
                        Child::A if parent.is_multiple_of(2) => 1,
                        Child::B if !parent.is_multiple_of(2) => 1,
                        Child::A | Child::B => 8,
                        Child::C => 64,
                        Child::Domain => unreachable!(),
                    }
                }
            },
            _ => unreachable!(),
        }
    }

    fn propose_relation(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        for (row_index, row) in view.iter().enumerate() {
            let row_index = u32::try_from(row_index).expect("fixture row count fits u32");
            match variable {
                X => match view.col(P) {
                    Some(column) => {
                        if let Some(parent) =
                            decode(&row[column], P_MARKER).filter(|&parent| parent < self.parents)
                        {
                            candidates.push(row_index, encoded(X_MARKER, parent));
                        }
                    }
                    None => candidates.extend_row(
                        row_index,
                        (0..self.parents).map(|parent| encoded(X_MARKER, parent)),
                    ),
                },
                P => match view.col(X) {
                    Some(column) => {
                        if let Some(parent) =
                            decode(&row[column], X_MARKER).filter(|&parent| parent < self.parents)
                        {
                            candidates.push(row_index, encoded(P_MARKER, parent));
                        }
                    }
                    None => candidates.extend_row(
                        row_index,
                        (0..self.parents).map(|parent| encoded(P_MARKER, parent)),
                    ),
                },
                _ => unreachable!(),
            }
        }
    }

    fn confirm_relation(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        candidates.retain(|row_index, candidate| {
            let row = view.row(row_index as usize);
            match variable {
                X => {
                    let candidate = decode(candidate, X_MARKER);
                    match view.col(P) {
                        Some(column) => {
                            candidate == decode(&row[column], P_MARKER)
                                && candidate.is_some_and(|parent| parent < self.parents)
                        }
                        None => candidate.is_some_and(|parent| parent < self.parents),
                    }
                }
                P => {
                    let candidate = decode(candidate, P_MARKER);
                    match view.col(X) {
                        Some(column) => {
                            candidate == decode(&row[column], X_MARKER)
                                && candidate.is_some_and(|parent| parent < self.parents)
                        }
                        None => candidate.is_some_and(|parent| parent < self.parents),
                    }
                }
                _ => unreachable!(),
            }
        });
    }
}

impl Constraint<'_> for TableChild {
    fn variables(&self) -> VariableSet {
        let mut variables = VariableSet::new_singleton(P);
        if self.child != Child::Domain {
            variables.set(X);
        }
        variables
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        match self.child {
            Child::Domain if variable == P => {
                out.fill(self.domain_estimate, view.len());
                true
            }
            Child::Domain => false,
            Child::A | Child::B | Child::C if variable == P || variable == X => {
                out.extend(
                    view.iter()
                        .map(|row| self.estimate_relation(variable, view, row)),
                );
                true
            }
            Child::A | Child::B | Child::C => false,
        }
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert!(candidates.is_empty(), "propose sink must start empty");
        let before = candidates.len();
        match self.child {
            Child::Domain if variable == P => {
                for row in 0..view.len() {
                    candidates.extend_row(
                        u32::try_from(row).expect("fixture row count fits u32"),
                        (0..self.parents).map(|parent| encoded(P_MARKER, parent)),
                    );
                }
            }
            Child::Domain => {}
            Child::A | Child::B | Child::C => self.propose_relation(variable, view, candidates),
        }
        self.record(Verb::Propose, variable, view, candidates, before);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let before = candidates.len();
        match self.child {
            Child::Domain if variable == P => candidates.retain(|row, candidate| {
                (row as usize) < view.len()
                    && decode(candidate, P_MARKER).is_some_and(|parent| parent < self.parents)
            }),
            Child::Domain => {}
            Child::A | Child::B | Child::C => self.confirm_relation(variable, view, candidates),
        }
        self.record(Verb::Confirm, variable, view, candidates, before);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.iter().all(|row| match self.child {
            Child::Domain => view.col(P).is_none_or(|column| {
                decode(&row[column], P_MARKER).is_some_and(|parent| parent < self.parents)
            }),
            Child::A | Child::B | Child::C => match (view.col(P), view.col(X)) {
                (Some(p), Some(x)) => {
                    let parent = decode(&row[p], P_MARKER);
                    parent == decode(&row[x], X_MARKER)
                        && parent.is_some_and(|parent| parent < self.parents)
                }
                (Some(p), None) => {
                    decode(&row[p], P_MARKER).is_some_and(|parent| parent < self.parents)
                }
                (None, Some(x)) => {
                    decode(&row[x], X_MARKER).is_some_and(|parent| parent < self.parents)
                }
                (None, None) => true,
            },
        })
    }
}

fn encoded(marker: u8, index: usize) -> RawInline {
    let mut value = [0; 32];
    value[0] = marker;
    value[24..].copy_from_slice(&(index as u64).to_be_bytes());
    value
}

fn decode(value: &RawInline, marker: u8) -> Option<usize> {
    (value[0] == marker)
        .then(|| u64::from_be_bytes(value[24..].try_into().expect("eight-byte suffix")) as usize)
}

#[derive(Clone, Copy)]
struct Truth(bool);

impl Constraint<'_> for Truth {
    fn variables(&self) -> VariableSet {
        VariableSet::new_empty()
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
        self.0
    }
}

#[derive(Default)]
struct VerbCounts {
    propose: usize,
    confirm: usize,
}

#[derive(Clone)]
struct FiniteDomain {
    variable: VariableId,
    values: Vec<RawInline>,
    estimate: usize,
    calls: Arc<Mutex<VerbCounts>>,
}

impl FiniteDomain {
    fn new(
        variable: VariableId,
        values: Vec<RawInline>,
        estimate: usize,
        calls: Arc<Mutex<VerbCounts>>,
    ) -> Self {
        Self {
            variable,
            values,
            estimate,
            calls,
        }
    }
}

impl Constraint<'_> for FiniteDomain {
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
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable {
            return;
        }
        self.calls.lock().unwrap().propose += 1;
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
        if variable != self.variable {
            return;
        }
        self.calls.lock().unwrap().confirm += 1;
        candidates.retain(|_, candidate| self.values.contains(candidate));
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

type OwnedConstraint = Box<dyn Constraint<'static> + Send + Sync>;

fn finite_domain(
    variable: VariableId,
    values: Vec<RawInline>,
    estimate: usize,
) -> (FiniteDomain, Arc<Mutex<VerbCounts>>) {
    let calls = Arc::new(Mutex::new(VerbCounts::default()));
    (
        FiniteDomain::new(variable, values, estimate, Arc::clone(&calls)),
        calls,
    )
}

fn fixture(parents: usize) -> (IntersectionConstraint<TableChild>, Arc<Mutex<Trace>>) {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let root = IntersectionConstraint::new(vec![
        // A zero estimate is a cardinality hint, not proof of emptiness.
        TableChild::domain(parents, 0, Arc::clone(&trace)),
        TableChild::relation(Child::A, parents, Arc::clone(&trace)),
        TableChild::relation(Child::B, parents, Arc::clone(&trace)),
        TableChild::relation(Child::C, parents, Arc::clone(&trace)),
    ]);
    (root, trace)
}

fn project_pair(binding: &Binding) -> Option<(RawInline, RawInline)> {
    Some((*binding.get(P)?, *binding.get(X)?))
}

fn project_parent(binding: &Binding) -> Option<RawInline> {
    binding.get(P).copied()
}

fn project_same(_: &Binding) -> Option<()> {
    Some(())
}

fn matching_calls(trace: &Trace, child: Child, verb: Verb, variable: VariableId) -> Vec<Call> {
    trace
        .calls
        .iter()
        .copied()
        .filter(|call| call.child == child && call.verb == verb && call.variable == variable)
        .collect()
}

#[test]
fn flipped_proposers_remerge_before_the_last_confirmation() {
    const N: usize = 12;
    assert!(
        X < P,
        "fixture must exercise insertion before an existing column"
    );

    let (root, trace) = fixture(N);
    let mut residual = Query::new(root, project_pair).solve_residual_state_profiled();

    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();

    let mut oracle: Vec<_> = (0..N)
        .map(|parent| (encoded(P_MARKER, parent), encoded(X_MARKER, parent)))
        .collect();
    residual.results.sort_unstable();
    sequential.sort_unstable();
    oracle.sort_unstable();
    assert_eq!(residual.results, oracle, "residual result bag vs oracle");
    assert_eq!(
        residual.results, sequential,
        "residual result bag vs scalar DFS"
    );

    let trace = trace.lock().unwrap();

    // Domain's estimate was zero, but the non-empty proposal must survive.
    let domain = matching_calls(&trace, Child::Domain, Verb::Propose, P);
    assert_eq!(domain.len(), 1);
    assert_eq!((domain[0].rows, domain[0].candidates_after), (1, N));

    // A proposes for even parents and B for odd parents. Each first
    // confirms the other half, after which both histories denote exactly
    // checked={A,B}. C must therefore see one remerged full-width batch.
    for child in [Child::A, Child::B] {
        let proposals = matching_calls(&trace, child, Verb::Propose, X);
        assert_eq!(proposals.len(), 1, "{child:?} x proposal calls");
        assert_eq!(proposals[0].rows, N / 2, "{child:?} proposal rows");
        assert_eq!(proposals[0].candidates_after, N / 2);

        let confirmations = matching_calls(&trace, child, Verb::Confirm, X);
        assert_eq!(confirmations.len(), 1, "{child:?} x confirmation calls");
        assert_eq!(confirmations[0].rows, N / 2, "{child:?} confirmation rows");
    }

    assert!(matching_calls(&trace, Child::C, Verb::Propose, X).is_empty());
    let c = matching_calls(&trace, Child::C, Verb::Confirm, X);
    assert_eq!(c.len(), 1, "C should run once after checked-set remerge");
    assert_eq!(
        (c[0].rows, c[0].candidates_before, c[0].candidates_after),
        (N, N, N)
    );

    let x_calls: Vec<_> = trace
        .calls
        .iter()
        .filter(|call| call.variable == X)
        .collect();
    assert!(!x_calls.is_empty());
    assert!(x_calls
        .iter()
        .all(|call| { call.tagged && call.active_unbound && call.tags_valid }));

    assert_eq!(residual.stats.bucket_merges, 1);
    assert_eq!(residual.stats.rows_merged, N / 2);
    assert!(residual.stats.interner_hits >= 1);
    assert_eq!(residual.stats.max_confirm_rows, N);
}

#[test]
fn reconvergence_preserves_duplicate_projection_multiplicity() {
    const N: usize = 12;
    let (root, _) = fixture(N);
    let residual = Query::new(root, project_same).solve_residual_state_profiled();
    let (sequential_root, _) = fixture(N);
    let sequential: Vec<_> = Query::new(sequential_root, project_same)
        .sequential()
        .collect();

    assert_eq!(residual.results, vec![(); N]);
    assert_eq!(residual.results, sequential);
    assert_eq!(residual.stats.bucket_merges, 1);
    assert_eq!(residual.stats.rows_merged, N / 2);
}

#[test]
fn an_empty_zero_estimate_route_finishes_without_a_result() {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let root = IntersectionConstraint::new(vec![TableChild::domain(0, 0, Arc::clone(&trace))]);
    let residual = Query::new(root, project_parent).solve_residual_state_profiled();
    assert!(residual.results.is_empty());
    assert_eq!(residual.stats.propose_calls, 1);

    let calls = &trace.lock().unwrap().calls;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].verb, Verb::Propose);
    assert_eq!(calls[0].candidates_after, 0);

    let sequential_root = IntersectionConstraint::new(vec![TableChild::domain(
        0,
        0,
        Arc::new(Mutex::new(Trace::default())),
    )]);
    assert!(Query::new(sequential_root, project_parent)
        .sequential()
        .next()
        .is_none());
}

#[test]
fn zero_variable_intersections_emit_the_empty_binding_iff_true() {
    let project = |_: &Binding| Some("empty binding");

    let residual_true = Query::new(IntersectionConstraint::<Truth>::new(Vec::new()), project)
        .solve_residual_state_profiled();
    let sequential_true: Vec<_> =
        Query::new(IntersectionConstraint::<Truth>::new(Vec::new()), project)
            .sequential()
            .collect();
    assert_eq!(residual_true.results, ["empty binding"]);
    assert_eq!(residual_true.results, sequential_true);
    assert_eq!(residual_true.stats.state_pops, 1);

    let residual_false = Query::new(IntersectionConstraint::new(vec![Truth(false)]), project)
        .solve_residual_state_profiled();
    let sequential_false: Vec<_> =
        Query::new(IntersectionConstraint::new(vec![Truth(false)]), project)
            .sequential()
            .collect();
    assert!(residual_false.results.is_empty());
    assert_eq!(residual_false.results, sequential_false);
    assert_eq!(residual_false.stats.state_pops, 0);
}

fn equality_fixture() -> (
    IntersectionConstraint<OwnedConstraint>,
    Arc<Mutex<VerbCounts>>,
    Arc<Mutex<VerbCounts>>,
    Vec<RawInline>,
) {
    let values: Vec<_> = (0..4).map(|i| encoded(b'=', i)).collect();
    let (parents, parent_calls) = finite_domain(P, values.clone(), 1);
    let (xs, x_calls) = finite_domain(
        X,
        values.iter().copied().chain([encoded(b'=', 99)]).collect(),
        64,
    );
    let root = IntersectionConstraint::new(vec![
        Box::new(parents) as OwnedConstraint,
        Box::new(xs),
        Box::new(EqualityConstraint::new(P, X)),
    ]);
    (root, parent_calls, x_calls, values)
}

#[test]
fn equality_becomes_relevant_after_its_peer_is_bound() {
    let (root, parent_calls, x_calls, values) = equality_fixture();
    let mut residual = Query::new(root, project_pair).solve_residual_state_profiled();
    let (sequential_root, _, _, _) = equality_fixture();
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    let mut oracle: Vec<_> = values.iter().copied().map(|value| (value, value)).collect();
    residual.results.sort_unstable();
    sequential.sort_unstable();
    oracle.sort_unstable();
    assert_eq!(residual.results, oracle);
    assert_eq!(residual.results, sequential);

    let parent_calls = parent_calls.lock().unwrap();
    let x_calls = x_calls.lock().unwrap();
    assert_eq!((parent_calls.propose, parent_calls.confirm), (1, 0));
    assert_eq!(
        (x_calls.propose, x_calls.confirm),
        (0, 1),
        "once P is bound, EqualityConstraint must propose X's peer value"
    );
    assert_eq!(
        (residual.stats.propose_calls, residual.stats.confirm_calls),
        (2, 1)
    );
}

#[derive(Clone, Copy)]
enum UnionRole {
    Proposer,
    Confirmer,
}

fn union_fixture(
    role: UnionRole,
) -> (
    IntersectionConstraint<OwnedConstraint>,
    [Arc<Mutex<VerbCounts>>; 2],
    RawInline,
) {
    let value = encoded(b'|', 0);
    let arm_estimate = match role {
        UnionRole::Proposer => 1,
        UnionRole::Confirmer => 8,
    };
    let (left, left_calls) = finite_domain(P, vec![value], arm_estimate);
    let (right, right_calls) = finite_domain(P, vec![value], arm_estimate);
    let union = UnionConstraint::new(vec![left, right]);
    let peer_estimate = match role {
        UnionRole::Proposer => 64,
        UnionRole::Confirmer => 1,
    };
    let (peer, _) = finite_domain(P, vec![value], peer_estimate);
    let children = match role {
        UnionRole::Proposer => vec![
            Box::new(union) as OwnedConstraint,
            Box::new(peer) as OwnedConstraint,
        ],
        UnionRole::Confirmer => vec![
            Box::new(peer) as OwnedConstraint,
            Box::new(union) as OwnedConstraint,
        ],
    };
    (
        IntersectionConstraint::new(children),
        [left_calls, right_calls],
        value,
    )
}

#[test]
fn opaque_union_deduplicates_identical_arms_when_proposing_and_confirming() {
    for role in [UnionRole::Proposer, UnionRole::Confirmer] {
        let (root, arm_calls, value) = union_fixture(role);
        let residual = Query::new(root, project_parent).solve_residual_state();
        let (sequential_root, _, _) = union_fixture(role);
        let sequential: Vec<_> = Query::new(sequential_root, project_parent)
            .sequential()
            .collect();

        assert_eq!(residual, [value], "union must preserve set semantics");
        assert_eq!(residual, sequential, "residual vs scalar DFS");
        for calls in arm_calls {
            let calls = calls.lock().unwrap();
            match role {
                UnionRole::Proposer => assert_eq!((calls.propose, calls.confirm), (1, 0)),
                UnionRole::Confirmer => assert_eq!((calls.propose, calls.confirm), (0, 1)),
            }
        }
    }
}
