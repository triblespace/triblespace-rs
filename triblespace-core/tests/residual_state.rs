use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use triblespace_core::id::{rngid, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::Inline;
use triblespace_core::inline::RawInline;
use triblespace_core::query::equalityconstraint::EqualityConstraint;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::rangeconstraint::InlineRange;
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, PathOp, Query, RegularPathConstraint,
    RowsView, Variable, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

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

fn genid_inline(id: &Id) -> Inline<GenId> {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&id[..]);
    Inline::new(value)
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

#[derive(Clone, Copy)]
struct BorrowedDomain<'v> {
    variable: VariableId,
    values: &'v [RawInline],
}

/// Hides an ordinary constraint's optional lowering shape while preserving
/// every semantic protocol operation. This models custom semantic wrappers:
/// the residual engine must treat the whole value as one opaque root leaf.
struct Opaque<C>(C);

impl<'a, C: Constraint<'a>> Constraint<'a> for Opaque<C> {
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

impl<'v> Constraint<'v> for BorrowedDomain<'v> {
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
        out.fill(self.values.len(), view.len());
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

fn negative_fixture(
    parents: usize,
) -> (
    IntersectionConstraint<OwnedConstraint>,
    Arc<Mutex<Trace>>,
    Arc<Mutex<VerbCounts>>,
) {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let (empty, empty_calls) = finite_domain(X, Vec::new(), 16);
    let root = IntersectionConstraint::new(vec![
        Box::new(TableChild::domain(parents, 0, Arc::clone(&trace))) as OwnedConstraint,
        Box::new(TableChild::relation(Child::A, parents, Arc::clone(&trace))) as OwnedConstraint,
        Box::new(empty) as OwnedConstraint,
    ]);
    (root, trace, empty_calls)
}

fn nested_fixture(parents: usize) -> (IntersectionConstraint<OwnedConstraint>, Arc<Mutex<Trace>>) {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let tail = IntersectionConstraint::new(vec![
        Box::new(TableChild::relation(Child::B, parents, Arc::clone(&trace))) as OwnedConstraint,
        Box::new(TableChild::relation(Child::C, parents, Arc::clone(&trace))) as OwnedConstraint,
    ]);
    let relations = IntersectionConstraint::new(vec![
        Box::new(TableChild::relation(Child::A, parents, Arc::clone(&trace))) as OwnedConstraint,
        Box::new(tail) as OwnedConstraint,
    ]);
    let root = IntersectionConstraint::new(vec![
        Box::new(TableChild::domain(parents, 0, Arc::clone(&trace))) as OwnedConstraint,
        Box::new(relations) as OwnedConstraint,
    ]);
    (root, trace)
}

#[derive(Clone, Copy)]
enum AndNesting {
    Flat,
    Left,
    Right,
}

fn and_nesting_fixture(nesting: AndNesting) -> IntersectionConstraint<OwnedConstraint> {
    let values = [encoded(b'n', 0), encoded(b'n', 0), encoded(b'n', 1)];
    let (parents, _) = finite_domain(P, values.to_vec(), 1);
    let (xs, _) = finite_domain(X, vec![values[0], values[2]], 64);
    let parent = Box::new(parents) as OwnedConstraint;
    let x = Box::new(xs) as OwnedConstraint;
    let equality = Box::new(EqualityConstraint::new(P, X)) as OwnedConstraint;

    match nesting {
        AndNesting::Flat => IntersectionConstraint::new(vec![parent, x, equality]),
        AndNesting::Left => {
            IntersectionConstraint::new(vec![Box::new(IntersectionConstraint::new(vec![
                Box::new(IntersectionConstraint::new(vec![parent, x])) as OwnedConstraint,
                equality,
            ])) as OwnedConstraint])
        }
        AndNesting::Right => {
            IntersectionConstraint::new(vec![Box::new(IntersectionConstraint::new(vec![
                parent,
                Box::new(IntersectionConstraint::new(vec![x, equality])) as OwnedConstraint,
            ])) as OwnedConstraint])
        }
    }
}

fn nested_dead_union_fixture() -> IntersectionConstraint<OwnedConstraint> {
    let p_live = encoded(b'u', 0);
    let p_dead = encoded(b'u', 1);
    let x_live = encoded(b'v', 0);
    let x_dead = encoded(b'v', 1);
    let (outer_p, _) = finite_domain(P, vec![p_live], 1);
    let (arm_live_p, _) = finite_domain(P, vec![p_live], 8);
    let (arm_live_x, _) = finite_domain(X, vec![x_live], 1);
    let (arm_dead_p, _) = finite_domain(P, vec![p_dead], 8);
    let (arm_dead_x, _) = finite_domain(X, vec![x_dead], 1);
    let live = IntersectionConstraint::new(vec![
        Box::new(arm_live_p) as OwnedConstraint,
        Box::new(arm_live_x) as OwnedConstraint,
    ]);
    let dead = IntersectionConstraint::new(vec![
        Box::new(arm_dead_p) as OwnedConstraint,
        Box::new(arm_dead_x) as OwnedConstraint,
    ]);
    let union = UnionConstraint::new(vec![live, dead]);
    let nested = IntersectionConstraint::new(vec![
        Box::new(outer_p) as OwnedConstraint,
        Box::new(union) as OwnedConstraint,
    ]);
    IntersectionConstraint::new(vec![Box::new(nested) as OwnedConstraint])
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

fn assert_arbitrary_root_equivalent<'a, C, F, Pj, R>(make: F, project: Pj, mut expected: Vec<R>)
where
    C: Constraint<'a> + 'a,
    F: Fn() -> C,
    Pj: Fn(&Binding) -> Option<R> + Copy,
    R: Ord + std::fmt::Debug,
{
    let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
    let mut eager = Query::new(make(), project).solve_residual_state();
    let mut lazy_default: Vec<_> = Query::new(make(), project)
        .solve_residual_state_lazy()
        .collect();
    let mut lazy_cap_one: Vec<_> = Query::new(make(), project)
        .solve_residual_state_lazy()
        .cap(1)
        .collect();
    let mut ordinary: Vec<_> = Query::new(make(), project)
        .residual_state_scheduler()
        .collect();

    expected.sort_unstable();
    sequential.sort_unstable();
    eager.sort_unstable();
    lazy_default.sort_unstable();
    lazy_cap_one.sort_unstable();
    ordinary.sort_unstable();
    assert_eq!(sequential, expected, "scalar result bag vs fixture oracle");
    assert_eq!(eager, expected, "eager residual result bag");
    assert_eq!(lazy_default, expected, "default lazy residual result bag");
    assert_eq!(lazy_cap_one, expected, "cap=1 lazy residual result bag");
    assert_eq!(ordinary, expected, "ordinary Query residual result bag");
}

#[test]
fn top_level_union_root_preserves_set_semantics() {
    let left = vec![encoded(b'u', 0), encoded(b'u', 1)];
    let right = vec![encoded(b'u', 1), encoded(b'u', 2)];
    let expected = vec![encoded(b'u', 0), encoded(b'u', 1), encoded(b'u', 2)];

    assert_arbitrary_root_equivalent(
        || {
            let (left, _) = finite_domain(P, left.clone(), 2);
            let (right, _) = finite_domain(P, right.clone(), 2);
            UnionConstraint::new(vec![left, right])
        },
        project_parent,
        expected,
    );
}

#[test]
fn top_level_regular_path_root_preserves_graph_semantics() {
    let a = rngid();
    let b = rngid();
    let c = rngid();
    let attribute = Id::new([0x52; 16]).expect("non-zero test attribute");
    let a_value = genid_inline(&a);
    let b_value = genid_inline(&b);
    let c_value = genid_inline(&c);
    let mut set = TribleSet::new();
    set.insert(&Trible::new(&a, &attribute, &b_value));
    set.insert(&Trible::new(&a, &attribute, &c_value));
    set.insert(&Trible::new(&b, &attribute, &c_value));
    let expected = vec![
        (a_value.raw, b_value.raw),
        (a_value.raw, c_value.raw),
        (b_value.raw, c_value.raw),
    ];

    assert_arbitrary_root_equivalent(
        || {
            RegularPathConstraint::new(
                set.clone(),
                Variable::<GenId>::new(P),
                Variable::<GenId>::new(X),
                &[PathOp::Attr(attribute.raw())],
            )
        },
        project_pair,
        expected,
    );
}

#[test]
fn top_level_constant_root_and_confirm_only_range_api_are_supported() {
    let constant = Inline::<UnknownInline>::new(encoded(b'k', 7));
    assert_arbitrary_root_equivalent(
        || Variable::<UnknownInline>::new(P).is(constant),
        project_parent,
        vec![constant.raw],
    );

    let min = Inline::<UnknownInline>::new(encoded(b'r', 1));
    let max = Inline::<UnknownInline>::new(encoded(b'r', 3));
    // InlineRange deliberately cannot propose, so by itself it is not a
    // complete executable query. It can nevertheless be the API's concrete
    // root type; the following test exercises its semantics when paired with
    // a proposer behind one opaque root boundary.
    let range = InlineRange::new(Variable::<UnknownInline>::new(P), min, max);
    drop(Query::new(range, project_parent).solve_residual_state_lazy());
}

#[test]
fn opaque_root_preserves_internal_equality_and_range_semantics() {
    let equality_values: Vec<_> = (0..4).map(|i| encoded(b'=', i)).collect();
    let equality_expected: Vec<_> = equality_values
        .iter()
        .copied()
        .map(|value| (value, value))
        .collect();
    assert_arbitrary_root_equivalent(
        || Opaque(equality_fixture().0),
        project_pair,
        equality_expected,
    );

    let range_values: Vec<_> = (0..5).map(|i| encoded(b'r', i)).collect();
    let min = Inline::<UnknownInline>::new(encoded(b'r', 1));
    let max = Inline::<UnknownInline>::new(encoded(b'r', 3));
    assert_arbitrary_root_equivalent(
        || {
            let (domain, _) = finite_domain(P, range_values.clone(), 5);
            Opaque(IntersectionConstraint::new(vec![
                Box::new(domain) as OwnedConstraint,
                Box::new(InlineRange::new(
                    Variable::<UnknownInline>::new(P),
                    min,
                    max,
                )),
            ]))
        },
        project_parent,
        range_values[1..=3].to_vec(),
    );
}

#[test]
fn top_level_custom_and_borrowed_roots_need_no_wrapper() {
    let owned_values: Vec<_> = (0..3).map(|i| encoded(b'c', i)).collect();
    assert_arbitrary_root_equivalent(
        || finite_domain(P, owned_values.clone(), 3).0,
        project_parent,
        owned_values.clone(),
    );

    let borrowed_values: Vec<_> = (0..3).map(|i| encoded(b'b', i)).collect();
    assert_arbitrary_root_equivalent(
        || BorrowedDomain {
            variable: P,
            values: &borrowed_values,
        },
        project_parent,
        borrowed_values.clone(),
    );
}

#[test]
fn top_level_zero_variable_roots_settle_before_planning() {
    assert_arbitrary_root_equivalent(|| Truth(true), project_same, vec![()]);
    assert_arbitrary_root_equivalent(|| Truth(false), project_same, Vec::new());
}

#[test]
fn failed_ordinary_residual_pull_is_not_fresh_for_probe_restart() {
    let mut query = Query::new(Truth(false), project_same).residual_state_scheduler();
    assert_eq!(query.next(), None);

    let restart = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(query.solve_residual_state_lazy());
    }));
    assert!(restart.is_err());
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
    assert_eq!(residual.stats.full_pops, 0);
    assert_eq!(residual.stats.readiness_pops, residual.stats.state_pops);
    assert_eq!(
        residual.stats.interner_hits,
        residual.stats.bucket_merges + residual.stats.state_reentries
    );
}

#[test]
fn nested_and_flipped_proposers_remerge_before_the_last_confirmation() {
    const N: usize = 12;
    let (root, trace) = nested_fixture(N);
    let mut residual = Query::new(root, project_pair).solve_residual_state_profiled();
    let (sequential_root, _) = nested_fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    residual.results.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(residual.results, sequential);

    let trace = trace.lock().unwrap();
    for leaf in [Child::A, Child::B] {
        let proposals = matching_calls(&trace, leaf, Verb::Propose, X);
        let confirmations = matching_calls(&trace, leaf, Verb::Confirm, X);
        assert_eq!(proposals.len(), 1, "{leaf:?} nested proposal calls");
        assert_eq!(confirmations.len(), 1, "{leaf:?} nested confirm calls");
        assert_eq!(proposals[0].rows, N / 2);
        assert_eq!(confirmations[0].rows, N / 2);
    }
    let c = matching_calls(&trace, Child::C, Verb::Confirm, X);
    assert_eq!(c.len(), 1, "nested C must run after checked-set remerge");
    assert_eq!((c[0].rows, c[0].candidates_after), (N, N));
    assert_eq!(residual.stats.bucket_merges, 1);
    assert_eq!(residual.stats.rows_merged, N / 2);
    assert_eq!(residual.stats.max_confirm_rows, N);
}

#[test]
fn flat_left_and_right_nested_forms_preserve_bags_and_outer_duplicates() {
    let expected = vec![
        (encoded(b'n', 0), encoded(b'n', 0)),
        (encoded(b'n', 0), encoded(b'n', 0)),
        (encoded(b'n', 1), encoded(b'n', 1)),
    ];

    for nesting in [AndNesting::Flat, AndNesting::Left, AndNesting::Right] {
        let mut eager =
            Query::new(and_nesting_fixture(nesting), project_pair).solve_residual_state();
        let mut lazy: Vec<_> = Query::new(and_nesting_fixture(nesting), project_pair)
            .solve_residual_state_lazy()
            .cap(usize::MAX)
            .start_width(1)
            .growth(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(and_nesting_fixture(nesting), project_pair)
            .sequential()
            .collect();
        eager.sort_unstable();
        lazy.sort_unstable();
        sequential.sort_unstable();
        assert_eq!(eager, expected);
        assert_eq!(lazy, expected);
        assert_eq!(sequential, expected);
    }
}

#[test]
fn nested_and_width_one_yields_before_draining_sibling_parents() {
    const N: usize = 12;
    let (root, trace) = nested_fixture(N);
    let mut lazy = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1);
    let first = lazy.next().expect("nested fixture has a first result");
    assert_eq!(decode(&first.0, P_MARKER), decode(&first.1, X_MARKER));
    assert_eq!(lazy.stats().readiness_pops, 0);
    assert!(lazy.stats().full_pops > 0);
    assert!(lazy.stats().partial_pops > 0);
    assert_eq!(lazy.stats().max_propose_rows, 1);
    assert_eq!(lazy.stats().max_confirm_rows, 1);

    let trace = trace.lock().unwrap();
    assert!(trace
        .calls
        .iter()
        .filter(|call| call.variable == X)
        .all(|call| call.rows == 1));
}

#[test]
fn union_with_and_arms_stays_opaque_inside_a_nested_and_and_skips_dead_arms() {
    let expected = vec![(encoded(b'u', 0), encoded(b'v', 0))];
    let eager = Query::new(nested_dead_union_fixture(), project_pair).solve_residual_state();
    let lazy: Vec<_> = Query::new(nested_dead_union_fixture(), project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1)
        .collect();
    let sequential: Vec<_> = Query::new(nested_dead_union_fixture(), project_pair)
        .sequential()
        .collect();
    assert_eq!(eager, expected);
    assert_eq!(lazy, expected);
    assert_eq!(sequential, expected);
}

#[test]
fn nested_zero_variable_ands_settle_true_and_false_like_sequential() {
    let project = |_: &Binding| Some("empty binding");
    let nested_true = || {
        IntersectionConstraint::new(vec![
            Box::new(IntersectionConstraint::<Truth>::new(Vec::new())) as OwnedConstraint,
        ])
    };
    let nested_false = || {
        IntersectionConstraint::new(vec![
            Box::new(IntersectionConstraint::new(vec![Truth(false)])) as OwnedConstraint,
        ])
    };

    let eager_true = Query::new(nested_true(), project).solve_residual_state();
    let lazy_true: Vec<_> = Query::new(nested_true(), project)
        .solve_residual_state_lazy()
        .collect();
    let sequential_true: Vec<_> = Query::new(nested_true(), project).sequential().collect();
    assert_eq!(eager_true, ["empty binding"]);
    assert_eq!(lazy_true, eager_true);
    assert_eq!(sequential_true, eager_true);

    let eager_false = Query::new(nested_false(), project).solve_residual_state();
    let lazy_false: Vec<_> = Query::new(nested_false(), project)
        .solve_residual_state_lazy()
        .collect();
    let sequential_false: Vec<_> = Query::new(nested_false(), project).sequential().collect();
    assert!(eager_false.is_empty());
    assert_eq!(lazy_false, eager_false);
    assert_eq!(sequential_false, eager_false);
}

#[test]
fn repeated_shared_arc_is_executed_once_per_and_occurrence() {
    let values = vec![encoded(b'r', 0), encoded(b'r', 1)];
    let calls = Arc::new(Mutex::new(VerbCounts::default()));
    let shared = Arc::new(FiniteDomain::new(P, values.clone(), 1, Arc::clone(&calls)));
    let root = IntersectionConstraint::new(vec![Arc::clone(&shared), Arc::clone(&shared)]);

    let mut residual = Query::new(root, project_parent).solve_residual_state();
    residual.sort_unstable();
    assert_eq!(residual, values);
    let calls = calls.lock().unwrap();
    assert_eq!(
        (calls.propose, calls.confirm),
        (1, 1),
        "the first shared occurrence proposes and the second independently confirms"
    );
}

#[test]
fn lazy_nested_plan_owns_paths_without_requiring_static_constraints() {
    let values = vec![encoded(b'b', 0), encoded(b'b', 1)];
    let make = || {
        let inner = IntersectionConstraint::new(vec![
            BorrowedDomain {
                variable: P,
                values: &values,
            },
            BorrowedDomain {
                variable: P,
                values: &values,
            },
        ]);
        IntersectionConstraint::new(vec![inner])
    };

    let mut lazy: Vec<_> = Query::new(make(), project_parent)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1)
        .collect();
    let mut sequential: Vec<_> = Query::new(make(), project_parent).sequential().collect();
    lazy.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lazy, values);
    assert_eq!(lazy, sequential);
}

#[test]
fn lazy_width_one_reaches_a_result_before_draining_sibling_parents() {
    const N: usize = 12;
    let (root, trace) = fixture(N);
    let mut lazy = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(2);

    assert_eq!(lazy.current_width(), 1);
    let first = lazy.next().expect("fixture has a first result");
    assert_eq!(decode(&first.0, P_MARKER), decode(&first.1, X_MARKER));
    assert_eq!(
        lazy.current_width(),
        2,
        "the first resumption must prepare a geometrically wider next chunk"
    );
    assert_eq!(lazy.stats().readiness_pops, 0);
    assert!(lazy.stats().full_pops > 0);
    assert!(lazy.stats().partial_pops > 0);
    assert_eq!(lazy.stats().max_propose_rows, 1);
    assert_eq!(lazy.stats().max_confirm_rows, 1);

    let trace = trace.lock().unwrap();
    let x_calls: Vec<_> = trace
        .calls
        .iter()
        .filter(|call| call.variable == X)
        .collect();
    assert!(!x_calls.is_empty());
    assert!(
        x_calls.iter().all(|call| call.rows == 1),
        "the first result must not evaluate x for sibling parent rows: {x_calls:?}"
    );
    drop(trace);

    // Dropping here must discard the unconsumed affine frontier without
    // evaluating the remaining parents.
    drop(lazy);
}

#[test]
fn successful_first_path_has_the_same_ordered_trace_before_growth() {
    const N: usize = 12;
    let run = |growth| {
        let (root, trace) = fixture(N);
        let mut lazy = Query::new(root, project_pair)
            .solve_residual_state_lazy()
            .cap(8)
            .start_width(1)
            .growth(growth);
        let first = lazy.next().expect("fixture has a first result");
        let calls = trace.lock().unwrap().calls.clone();
        let stats = lazy.stats().clone();
        (first, calls, stats, lazy.current_width())
    };

    let (fixed_first, fixed_calls, fixed_stats, fixed_width) = run(1);
    let (grown_first, grown_calls, grown_stats, grown_width) = run(2);

    assert_eq!(fixed_first, grown_first);
    assert_eq!(fixed_calls, grown_calls);
    assert_eq!(fixed_stats.state_pops, grown_stats.state_pops);
    assert_eq!(fixed_stats.propose_calls, grown_stats.propose_calls);
    assert_eq!(fixed_stats.confirm_calls, grown_stats.confirm_calls);
    assert_eq!(fixed_stats.max_propose_rows, 1);
    assert_eq!(fixed_stats.max_confirm_rows, 1);
    assert_eq!(grown_stats.max_propose_rows, 1);
    assert_eq!(grown_stats.max_confirm_rows, 1);
    assert_eq!(fixed_stats.dead_action_pops, 0);
    assert_eq!(grown_stats.dead_action_pops, 0);
    assert_eq!((fixed_width, fixed_stats.width_increases), (1, 0));
    assert_eq!((grown_width, grown_stats.width_increases), (2, 1));
}

#[test]
fn dead_paths_ramp_within_one_negative_pull() {
    const N: usize = 16;
    const CAP: usize = 8;
    let run = |growth| {
        let (root, trace, empty_calls) = negative_fixture(N);
        let mut lazy = Query::new(root, project_same)
            .solve_residual_state_lazy()
            .cap(CAP)
            .start_width(1)
            .growth(growth);
        assert_eq!(lazy.next(), None);
        let calls = trace.lock().unwrap().calls.clone();
        let empty_confirms = empty_calls.lock().unwrap().confirm;
        let stats = lazy.stats().clone();
        (calls, empty_confirms, stats, lazy.current_width())
    };

    let (fixed_calls, fixed_confirms, fixed, fixed_width) = run(1);
    let (grown_calls, grown_confirms, grown, grown_width) = run(2);
    let x_proposal_rows = |calls: &[Call]| {
        calls
            .iter()
            .filter(|call| {
                call.child == Child::A && call.verb == Verb::Propose && call.variable == X
            })
            .map(|call| call.rows)
            .collect::<Vec<_>>()
    };

    assert_eq!(x_proposal_rows(&fixed_calls), vec![1; N]);
    assert_eq!(x_proposal_rows(&grown_calls), [1, 2, 4, 8, 1]);
    assert_eq!((fixed_confirms, fixed.dead_action_pops), (N, N));
    assert_eq!((grown_confirms, grown.dead_action_pops), (5, 5));
    assert_eq!((fixed.max_propose_rows, fixed.max_confirm_rows), (1, 1));
    assert_eq!((grown.max_propose_rows, grown.max_confirm_rows), (8, 8));
    assert_eq!((fixed_width, fixed.width_increases), (1, 0));
    assert_eq!((grown_width, grown.width_increases), (CAP, 3));
    assert_eq!(fixed.emit_pops, 0);
    assert_eq!(grown.emit_pops, 0);

    assert_eq!(fixed.state_pops, 69);
    assert_eq!(
        (
            fixed.ready_plan_pops,
            fixed.propose_action_pops,
            fixed.candidate_plan_pops,
            fixed.confirm_action_pops,
        ),
        (17, 17, 18, 17)
    );
    assert_eq!((fixed.states_interned, fixed.state_reentries), (9, 45));
    assert_eq!((fixed.rows_reentered, fixed.bucket_merges), (45, 0));
    assert_eq!(
        (fixed.full_pops, fixed.readiness_pops, fixed.partial_pops),
        (69, 0, 15)
    );
    assert_eq!(
        (
            fixed.propose_calls,
            fixed.confirm_calls,
            fixed.propose_rows,
            fixed.confirm_rows,
        ),
        (17, 17, 17, 17)
    );

    assert_eq!(grown.state_pops, 25);
    assert_eq!(
        (
            grown.ready_plan_pops,
            grown.propose_action_pops,
            grown.candidate_plan_pops,
            grown.confirm_action_pops,
        ),
        (6, 6, 7, 6)
    );
    assert_eq!((grown.states_interned, grown.state_reentries), (9, 12));
    assert_eq!((grown.rows_reentered, grown.bucket_merges), (45, 0));
    assert_eq!(
        (grown.full_pops, grown.readiness_pops, grown.partial_pops),
        (21, 4, 4)
    );
    assert_eq!(
        (
            grown.propose_calls,
            grown.confirm_calls,
            grown.propose_rows,
            grown.confirm_rows,
        ),
        (6, 6, 17, 17)
    );

    let (eager_root, _, _) = negative_fixture(N);
    let eager = Query::new(eager_root, project_same).solve_residual_state_profiled();
    assert!(eager.results.is_empty());
    let eager = eager.stats;
    assert_eq!((eager.state_pops, eager.readiness_pops), (9, 9));
    assert_eq!(eager.full_pops, 0);
    assert_eq!(
        (
            eager.ready_plan_pops,
            eager.propose_action_pops,
            eager.candidate_plan_pops,
            eager.confirm_action_pops,
        ),
        (2, 2, 3, 2)
    );
    assert_eq!((eager.dead_action_pops, eager.width_increases), (1, 0));
    assert_eq!(
        (
            eager.propose_calls,
            eager.confirm_calls,
            eager.propose_rows,
            eager.confirm_rows,
            eager.max_propose_rows,
            eager.max_confirm_rows,
        ),
        (2, 2, 17, 17, N, N)
    );
}

#[test]
fn rejected_projection_still_grows_after_raw_emit() {
    let values: Vec<_> = (0..7).map(|index| encoded(b'e', index)).collect();
    let (domain, _) = finite_domain(P, values, 1);
    let root = IntersectionConstraint::new(vec![domain]);
    let mut lazy = Query::new(root, |_: &Binding| None::<()>)
        .solve_residual_state_lazy()
        .cap(4)
        .start_width(1)
        .growth(2);

    assert_eq!(lazy.next(), None);
    assert_eq!(lazy.current_width(), 4);
    assert_eq!(lazy.stats().emit_pops, 3);
    assert_eq!(lazy.stats().width_increases, 2);
    assert_eq!(lazy.stats().dead_action_pops, 0);
}

#[test]
fn lazy_fixed_width_reopens_states_without_changing_the_result_bag() {
    const N: usize = 12;
    let (root, _) = fixture(N);
    let mut lazy = Query::new(Arc::new(root), project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1)
        .collect_profiled();

    let (eager_root, _) = fixture(N);
    let mut eager = Query::new(eager_root, project_pair).solve_residual_state();
    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    lazy.results.sort_unstable();
    eager.sort_unstable();
    sequential.sort_unstable();

    assert_eq!(lazy.results, eager);
    assert_eq!(lazy.results, sequential);
    assert_eq!(lazy.stats.readiness_pops, 0);
    assert!(lazy.stats.full_pops > 0);
    assert!(lazy.stats.state_reentries > 0);
    assert!(lazy.stats.rows_reentered > 0);
    assert_eq!(
        lazy.stats.state_pops,
        lazy.stats.full_pops + lazy.stats.readiness_pops + lazy.stats.continuation_pops
    );
    assert_eq!(
        lazy.stats.interner_hits,
        lazy.stats.bucket_merges + lazy.stats.state_reentries
    );
}

#[test]
fn lazy_geometric_width_uses_both_full_and_underfilled_choices() {
    const N: usize = 12;
    let (root, _) = fixture(N);
    let mut crossed = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(4)
        .start_width(1)
        .growth(2)
        .collect_profiled();

    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    crossed.results.sort_unstable();
    sequential.sort_unstable();

    assert_eq!(crossed.results, sequential);
    assert!(crossed.stats.full_pops > 0);
    assert!(crossed.stats.readiness_pops > 0);
    assert_eq!(
        crossed.stats.state_pops,
        crossed.stats.full_pops + crossed.stats.readiness_pops + crossed.stats.continuation_pops
    );
    assert_eq!(
        crossed.stats.interner_hits,
        crossed.stats.bucket_merges + crossed.stats.state_reentries
    );
}

#[test]
fn lazy_width_above_the_frontier_reconverges_before_states_are_popped() {
    const N: usize = 12;
    let (root, _) = fixture(N);
    let mut readiness = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(usize::MAX)
        .growth(1)
        .collect_profiled();

    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    readiness.results.sort_unstable();
    sequential.sort_unstable();

    assert_eq!(readiness.results, sequential);
    assert_eq!(readiness.stats.full_pops, 0);
    assert!(readiness.stats.readiness_pops > 0);
    assert_eq!(readiness.stats.partial_pops, 0);
    assert_eq!(
        readiness.stats.state_reentries, 0,
        "an underfilled minimum-rank drain must consume every feeder before its target"
    );
    assert!(readiness.stats.bucket_merges > 0);
    assert!(readiness.stats.rows_merged > 0);
    assert_eq!(readiness.stats.max_confirm_rows, N);
    assert_eq!(
        readiness.stats.state_pops,
        readiness.stats.full_pops
            + readiness.stats.readiness_pops
            + readiness.stats.continuation_pops
    );
    assert_eq!(
        readiness.stats.interner_hits,
        readiness.stats.bucket_merges + readiness.stats.state_reentries
    );
}

#[test]
fn occupancy_plans_striped_ready_chunks_before_invoking_uniform_actions() {
    const N: usize = 4;
    const W: usize = 2;
    let (root, trace) = fixture(N);
    let mut filled = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(W)
        .growth(1)
        .collect_profiled();
    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    filled.results.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(filled.results, sequential);

    // Each Ready(P) chunk is striped: [2, 3] and [0, 1] independently
    // choose one A and one B row. Each one-row Propose action is underfilled,
    // while the Ready remainder can still fill W. The scheduler therefore
    // plans the remainder first and merges both filings into one width-W call.
    let trace = trace.lock().unwrap();
    for child in [Child::A, Child::B] {
        let calls = matching_calls(&trace, child, Verb::Propose, X);
        assert_eq!(calls.len(), 1, "{child:?} proposal action calls");
        assert_eq!((calls[0].rows, calls[0].candidates_after), (W, W));
    }

    let stats = filled.stats;
    assert_eq!(
        stats.state_pops,
        stats.full_pops + stats.readiness_pops + stats.continuation_pops
    );
    assert_eq!(stats.propose_action_pops, stats.propose_calls);
    assert_eq!(stats.confirm_action_pops, stats.confirm_calls);
    assert_eq!((stats.propose_calls, stats.propose_rows), (3, 5));
    assert_eq!(stats.max_propose_rows, W);
    assert!(stats.max_confirm_rows <= W);
    assert_eq!(
        stats.state_pops,
        stats.ready_plan_pops
            + stats.candidate_plan_pops
            + stats.propose_action_pops
            + stats.confirm_action_pops
            + stats.emit_pops,
        "every pop must have one explicit planning, action, or emission phase"
    );
}

#[test]
fn occupancy_shape_is_independent_of_whether_width_equals_the_cap() {
    const N: usize = 4;
    const W: usize = 2;
    let run = |cap| {
        let (root, trace) = fixture(N);
        let solved = Query::new(root, project_pair)
            .solve_residual_state_lazy()
            .cap(cap)
            .start_width(W)
            .growth(1)
            .collect_profiled();
        let calls = trace.lock().unwrap().calls.clone();
        (solved, calls)
    };
    let (capped, capped_calls) = run(W);
    let (uncapped, uncapped_calls) = run(usize::MAX);

    assert_eq!(capped.results, uncapped.results);
    assert_eq!(capped.stats, uncapped.stats);
    assert_eq!(capped_calls, uncapped_calls);

    let (sequential_root, _) = fixture(N);
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    let mut capped_results = capped.results.clone();
    capped_results.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(capped_results, sequential);

    // The cap only bounds width growth. It cannot switch scheduling policy:
    // the width-W action shape must match the cap-above-width case.
    let trace = Trace {
        calls: capped_calls,
    };
    for child in [Child::A, Child::B] {
        let calls = matching_calls(&trace, child, Verb::Propose, X);
        assert_eq!(calls.len(), 1, "{child:?} proposal action calls");
        assert_eq!((calls[0].rows, calls[0].candidates_after), (W, W));
    }
    assert_eq!(capped.stats.propose_calls, 3);
    assert_eq!(capped.stats.propose_rows, 5);
    assert_eq!(capped.stats.max_propose_rows, W);
    assert_eq!(
        capped.stats.state_pops,
        capped.stats.full_pops + capped.stats.readiness_pops + capped.stats.continuation_pops
    );
}

#[test]
fn lazy_projection_panic_consumes_the_row_before_resume() {
    const N: usize = 4;
    let (root, _) = fixture(N);
    let seen = RefCell::new(Vec::new());
    let project = |binding: &Binding| {
        let parent = *binding.get(P)?;
        let seen_count = {
            let mut seen = seen.borrow_mut();
            seen.push(parent);
            seen.len()
        };
        assert_ne!(seen_count, 1, "first projected row panics");
        Some(parent)
    };
    let mut lazy = Query::new(root, project)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1);

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| lazy.next()));
    assert!(panic.is_err());
    let resumed = lazy.next().expect("a later row remains after the panic");
    let seen = seen.borrow();
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0], seen[1], "the panicking row was projected twice");
    assert_eq!(resumed, seen[1]);
}

#[test]
fn lazy_cap_builder_does_not_raise_an_already_clamped_start_width() {
    let (root, _) = fixture(1);
    let start_then_cap = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(2)
        .start_width(8)
        .cap(8);
    assert_eq!(start_then_cap.current_width(), 2);

    let (root, _) = fixture(1);
    let cap_then_start = Query::new(root, project_pair)
        .solve_residual_state_lazy()
        .cap(2)
        .cap(8)
        .start_width(8);
    assert_eq!(cap_then_start.current_width(), 8);
}

#[test]
fn reconvergence_preserves_duplicate_projection_multiplicity() {
    const N: usize = 12;
    let (root, _) = fixture(N);
    let residual = Query::new(root, project_same).solve_residual_state_profiled();
    let (lazy_root, _) = fixture(N);
    let lazy: Vec<_> = Query::new(lazy_root, project_same)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1)
        .collect();
    let (sequential_root, _) = fixture(N);
    let sequential: Vec<_> = Query::new(sequential_root, project_same)
        .sequential()
        .collect();

    assert_eq!(residual.results, vec![(); N]);
    assert_eq!(lazy, vec![(); N]);
    assert_eq!(residual.results, sequential);
    assert_eq!(lazy, sequential);
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
    assert_eq!(residual.stats.dead_action_pops, 1);

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
    let mut lazy_true = Query::new(IntersectionConstraint::<Truth>::new(Vec::new()), project)
        .solve_residual_state_lazy()
        .cap(4)
        .start_width(1)
        .growth(2);
    assert_eq!(lazy_true.next(), Some("empty binding"));
    assert_eq!(lazy_true.current_width(), 2);
    assert_eq!(lazy_true.stats().emit_pops, 1);
    assert_eq!(lazy_true.stats().width_increases, 1);
    assert_eq!(lazy_true.next(), None);
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

    let mut lazy_false = Query::new(IntersectionConstraint::new(vec![Truth(false)]), project)
        .solve_residual_state_lazy()
        .cap(4)
        .start_width(1)
        .growth(2);
    assert!(lazy_false.next().is_none());
    assert!(lazy_false.next().is_none());
    assert_eq!(lazy_false.current_width(), 1);
    assert_eq!(lazy_false.stats().width_increases, 0);
    assert_eq!(lazy_false.stats().emit_pops, 0);
    assert_eq!(lazy_false.stats().dead_action_pops, 0);
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
    let (lazy_root, _, _, _) = equality_fixture();
    let mut lazy: Vec<_> = Query::new(lazy_root, project_pair)
        .solve_residual_state_lazy()
        .cap(usize::MAX)
        .start_width(1)
        .growth(1)
        .collect();
    let (sequential_root, _, _, _) = equality_fixture();
    let mut sequential: Vec<_> = Query::new(sequential_root, project_pair)
        .sequential()
        .collect();
    let mut oracle: Vec<_> = values.iter().copied().map(|value| (value, value)).collect();
    residual.results.sort_unstable();
    lazy.sort_unstable();
    sequential.sort_unstable();
    oracle.sort_unstable();
    assert_eq!(residual.results, oracle);
    assert_eq!(residual.results, sequential);
    assert_eq!(lazy, sequential);

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
        let (lazy_root, _, _) = union_fixture(role);
        let lazy: Vec<_> = Query::new(lazy_root, project_parent)
            .solve_residual_state_lazy()
            .cap(usize::MAX)
            .start_width(1)
            .growth(1)
            .collect();
        let (sequential_root, _, _) = union_fixture(role);
        let sequential: Vec<_> = Query::new(sequential_root, project_parent)
            .sequential()
            .collect();

        assert_eq!(residual, [value], "union must preserve set semantics");
        assert_eq!(lazy, [value], "lazy union must preserve set semantics");
        assert_eq!(residual, sequential, "residual vs scalar DFS");
        assert_eq!(lazy, sequential, "lazy residual vs scalar DFS");
        for calls in arm_calls {
            let calls = calls.lock().unwrap();
            match role {
                UnionRole::Proposer => assert_eq!((calls.propose, calls.confirm), (1, 0)),
                UnionRole::Confirmer => assert_eq!((calls.propose, calls.confirm), (0, 1)),
            }
        }
    }
}
