use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace_core::id::{rngid, ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::{ActionVerb, ResidualCapabilities, ResidualShadowEpoch};
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, ConstraintShape, EstimateSink, PathOp, Query,
    RegularPathConstraint, ResidualDeltaNode, ResidualDeltaOutput, ResidualDeltaSeed,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, Variable, VariableId,
    VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const START: VariableId = 0;
const END: VariableId = 1;
const OUTER: VariableId = 2;

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
        let attribute = Id::new([
            0xD6, 0x5F, 0xF7, 0xBC, 0x33, 0x6E, 0x47, 0x33, 0xD2, 0xEF, 0xA0, 0x9F, 0x38, 0x09,
            0x6E, 0x31,
        ])
        .expect("minted nonzero attribute");
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

fn genid(id: &Id) -> Inline<GenId> {
    let mut value = [0; 32];
    value[16..].copy_from_slice(&id[..]);
    Inline::new(value)
}

fn other_attribute() -> Id {
    Id::new([
        0x4C, 0xEC, 0x06, 0xD5, 0x51, 0xFA, 0xCF, 0x4B, 0xAF, 0xBA, 0x7A, 0x59, 0xA3, 0x50, 0x49,
        0xCE,
    ])
    .expect("minted nonzero attribute")
}

struct CountingPath {
    inner: RegularPathConstraint,
    seeded_roots: Option<Arc<AtomicUsize>>,
    source_pages: Option<Arc<Mutex<Vec<(usize, usize, usize)>>>>,
    expanded_nodes: Arc<AtomicUsize>,
}

impl<'a> Constraint<'a> for CountingPath {
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
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'a> {
        self.inner.residual_shape()
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_is_grouped(&self) -> bool {
        self.inner.residual_delta_confirm_is_grouped()
    }

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.inner.residual_delta_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaSourcePage> {
        let before = roots.len();
        let page = self
            .inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots);
        if page.is_some() {
            if let Some(counter) = &self.seeded_roots {
                counter.fetch_add(roots.len() - before, Ordering::Relaxed);
            }
        }
        if let (Some(page), Some(pages)) = (page, &self.source_pages) {
            pages.lock().expect("source-page recorder poisoned").push((
                limit,
                page.examined,
                roots.len() - before,
            ));
        }
        page
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        let before = seeds.len();
        let supported = self.inner.residual_delta_seeds(variable, view, seeds);
        if supported {
            if let Some(counter) = &self.seeded_roots {
                counter.fetch_add(seeds.len() - before, Ordering::Relaxed);
            }
        }
        supported
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        self.expanded_nodes
            .fetch_add(nodes.len(), Ordering::Relaxed);
        self.inner
            .residual_delta_expand(variable, nodes, successors)
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
struct PageTraceFilter {
    variable: VariableId,
    estimate: usize,
    accepted: Option<RawInline>,
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

fn combined_effects() -> ResidualCapabilities {
    ResidualCapabilities::default().finite_unions().cyclic_rpq()
}

fn root_formula_effects() -> ResidualCapabilities {
    ResidualCapabilities::default().root_formula().cyclic_rpq()
}

fn repeated(attribute: Id, inverse: bool) -> Vec<PathOp> {
    if inverse {
        vec![PathOp::Attr(attribute.raw()), PathOp::Inverse, PathOp::Plus]
    } else {
        vec![PathOp::Attr(attribute.raw()), PathOp::Plus]
    }
}

fn bound_start_root(
    set: TribleSet,
    start: Inline<GenId>,
    ops: &[PathOp],
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, start_var, end_var, ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes,
        }) as DynConstraint,
    ]))
}

fn formula_bound_start_root(
    set: TribleSet,
    start: Inline<GenId>,
    ops: &[PathOp],
    seeded_roots: Option<Arc<AtomicUsize>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, start_var, end_var, ops),
        seeded_roots,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint;
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start_var.is(start)) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]))
}

fn bound_end_root(
    set: TribleSet,
    end: Inline<GenId>,
    ops: &[PathOp],
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(end_var.is(end)) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, start_var, end_var, ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes,
        }) as DynConstraint,
    ]))
}

fn target_confirm_root(
    set: TribleSet,
    candidate_variable: VariableId,
    bound: Inline<GenId>,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
    expanded_nodes: Arc<AtomicUsize>,
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
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, start_var, end_var, ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes,
        }) as DynConstraint,
    ]))
}

fn formula_target_confirm_root(
    set: TribleSet,
    bound: Inline<GenId>,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
    seeded_roots: Option<Arc<AtomicUsize>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, start_var, end_var, ops),
        seeded_roots,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint;
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
    seeded_roots: Option<Arc<AtomicUsize>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let path = Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, start_var, end_var, ops),
        seeded_roots,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint;
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
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let path = Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, start_var, end_var, ops),
        seeded_roots: None,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint;
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
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, start_var, end_var, ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes: Arc::new(AtomicUsize::new(0)),
        }) as DynConstraint,
    ]))
}

fn same_variable_root(set: TribleSet, ops: &[PathOp], expanded_nodes: Arc<AtomicUsize>) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, node, node, ops),
        seeded_roots: None,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint]))
}

fn counted_same_variable_root(
    set: TribleSet,
    ops: &[PathOp],
    seeded_roots: Arc<AtomicUsize>,
    source_pages: Option<Arc<Mutex<Vec<(usize, usize, usize)>>>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, node, node, ops),
        seeded_roots: Some(seeded_roots),
        source_pages,
        expanded_nodes,
    }) as DynConstraint]))
}

fn same_variable_confirm_root(
    set: TribleSet,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
    seeded_roots: Arc<AtomicUsize>,
    source_pages: Option<Arc<Mutex<Vec<(usize, usize, usize)>>>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: candidates,
        }) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, node, node, ops),
            seeded_roots: Some(seeded_roots),
            source_pages,
            expanded_nodes,
        }) as DynConstraint,
    ]))
}

fn same_variable_unknown_root(
    set: TribleSet,
    ops: &[PathOp],
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<UnknownInline>::new(START);
    Arc::new(IntersectionConstraint::new(vec![Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, node, node, ops),
        seeded_roots: None,
        source_pages: None,
        expanded_nodes,
    }) as DynConstraint]))
}

fn collect_same_variable_source_frontier(
    path: &RegularPathConstraint,
    candidates: Option<&[RawInline]>,
    limit: usize,
) -> (Vec<RawInline>, Vec<(usize, usize)>) {
    assert!(limit > 0);
    let mut cursor = ResidualDeltaSourceCursor::Start;
    let mut sources = Vec::new();
    let mut pages = Vec::new();
    loop {
        let mut roots = Vec::new();
        let page = path
            .residual_delta_source_page(
                START,
                &RowsView::EMPTY,
                candidates,
                cursor,
                limit,
                &mut roots,
            )
            .expect("same-variable repeated path exposes a source frontier");
        pages.push((page.examined, roots.len()));
        sources.extend(roots.into_iter().map(|root| {
            assert_eq!(root.node.source, Some(root.node.value));
            root.node.value
        }));
        let Some(next) = page.next else {
            break;
        };
        assert!(page.examined > 0, "a live cursor must consume its page");
        cursor = next;
    }
    (sources, pages)
}

fn same_variable_formula_confirm_root(
    set: TribleSet,
    candidates: Vec<RawInline>,
    ops: &[PathOp],
    seeded_roots: Arc<AtomicUsize>,
    source_pages: Option<Arc<Mutex<Vec<(usize, usize, usize)>>>>,
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<GenId>::new(START);
    let arm = Box::new(CountingPath {
        inner: RegularPathConstraint::new(set, node, node, ops),
        seeded_roots: Some(seeded_roots),
        source_pages,
        expanded_nodes,
    }) as DynConstraint;
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

fn same_variable_outer_root(
    set: TribleSet,
    outer_values: [RawInline; 2],
    ops: &[PathOp],
    expanded_nodes: Arc<AtomicUsize>,
) -> Root {
    let node = Variable::<GenId>::new(START);
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: OUTER,
            gate: OUTER,
            unbound_estimate: 0,
            values: outer_values.to_vec(),
        }) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(set, node, node, ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes,
        }) as DynConstraint,
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

#[test]
fn synthetic_root_atom_same_variable_rpq_composes_capabilities() {
    let graph = Graph::new(4, &[(0, 0), (1, 1), (2, 2), (3, 3)]);
    let ops = repeated(graph.attribute, false);
    let make = |seeded_roots, source_pages| CountingPath {
        inner: RegularPathConstraint::new(
            graph.set.clone(),
            Variable::<GenId>::new(START),
            Variable::<GenId>::new(START),
            &ops,
        ),
        seeded_roots: Some(seeded_roots),
        source_pages: Some(source_pages),
        expanded_nodes: Arc::new(AtomicUsize::new(0)),
    };

    let mut expected: Vec<_> = Query::new(
        make(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        ),
        project_start,
    )
    .sequential()
    .collect();
    expected.sort_unstable();

    let cases = [
        (
            "root-only",
            ResidualCapabilities::default().root_formula(),
            false,
        ),
        (
            "cyclic-only",
            ResidualCapabilities::default().cyclic_rpq(),
            true,
        ),
        (
            "root-cyclic",
            ResidualCapabilities::default().root_formula().cyclic_rpq(),
            true,
        ),
        (
            "root-finite-cyclic",
            ResidualCapabilities::default()
                .root_formula()
                .finite_unions()
                .cyclic_rpq(),
            true,
        ),
    ];
    for (name, capabilities, should_page_sources) in cases {
        let seeded = Arc::new(AtomicUsize::new(0));
        let pages = Arc::new(Mutex::new(Vec::new()));
        let mut actual: Vec<_> =
            Query::new(make(Arc::clone(&seeded), Arc::clone(&pages)), project_start)
                .solve_residual_state_lazy_with(capabilities)
                .cap(4)
                .start_width(1)
                .collect();
        actual.sort_unstable();
        assert_eq!(actual, expected, "capability case {name}");
        assert_eq!(
            !pages
                .lock()
                .expect("source-page recorder poisoned")
                .is_empty(),
            should_page_sources,
            "capability case {name}"
        );
        assert_eq!(
            seeded.load(Ordering::Relaxed) > 0,
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
    let source_pages = Arc::new(Mutex::new(Vec::new()));
    let suffix_calls = Arc::new(Mutex::new(Vec::new()));
    let seeded = Arc::new(AtomicUsize::new(0));
    let node = Variable::<GenId>::new(START);
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(OrderedDomain {
            variable: START,
            gate: START,
            unbound_estimate: 0,
            values: candidates,
        }) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(
                graph.set.clone(),
                node,
                node,
                &repeated(graph.attribute, false),
            ),
            seeded_roots: Some(Arc::clone(&seeded)),
            source_pages: Some(Arc::clone(&source_pages)),
            expanded_nodes: Arc::new(AtomicUsize::new(0)),
        }) as DynConstraint,
        Box::new(PageTraceFilter {
            variable: START,
            estimate: usize::MAX,
            accepted: None,
            calls: Arc::clone(&suffix_calls),
        }) as DynConstraint,
    ]));
    let mut query = Query::new(root, project_start)
        .solve_residual_state_lazy_with(ResidualCapabilities::default().root_formula().cyclic_rpq())
        .cap(1)
        .start_width(1);

    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![accepted, accepted]);
    let pages = source_pages
        .lock()
        .expect("source-page recorder poisoned")
        .clone();
    assert_eq!(
        pages
            .iter()
            .map(|&(limit, examined, _)| (limit, examined))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 1), (1, 1)]
    );
    assert_eq!(
        pages.iter().map(|&(_, _, roots)| roots).sum::<usize>(),
        1,
        "the duplicate accepted value must remain one grouped source root"
    );
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert_eq!(
        *suffix_calls.lock().expect("suffix recorder poisoned"),
        [1, 1],
        "candidate paging may begin only after the grouped RPQ quiesces"
    );
    assert_eq!(query.stats().max_confirm_candidates, 4);
}

#[test]
fn synthetic_root_cyclic_proposer_pins_global_width_coupling() {
    let edges: Vec<_> = (0..8).map(|node| (node, node)).collect();
    let graph = Graph::new(8, &edges);
    let source_pages = Arc::new(Mutex::new(Vec::new()));
    let suffix_calls = Arc::new(Mutex::new(Vec::new()));
    let seeded = Arc::new(AtomicUsize::new(0));
    let node = Variable::<GenId>::new(START);
    let accepted = graph.value(0).raw;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(
                graph.set.clone(),
                node,
                node,
                &repeated(graph.attribute, false),
            ),
            seeded_roots: Some(Arc::clone(&seeded)),
            source_pages: Some(Arc::clone(&source_pages)),
            expanded_nodes: Arc::new(AtomicUsize::new(0)),
        }) as DynConstraint,
        Box::new(PageTraceFilter {
            variable: START,
            estimate: usize::MAX,
            accepted: Some(accepted),
            calls: Arc::clone(&suffix_calls),
        }) as DynConstraint,
    ]));
    let mut query = Query::new(root, project_start)
        .solve_residual_state_lazy_with(ResidualCapabilities::default().root_formula().cyclic_rpq())
        .cap(8)
        .start_width(1);

    assert_eq!(query.next(), Some(accepted));
    assert_eq!(
        *source_pages.lock().expect("source-page recorder poisoned"),
        [(1, 1, 1), (2, 2, 2), (4, 4, 4), (8, 1, 1)]
    );
    assert_eq!(seeded.load(Ordering::Relaxed), 8);
    assert_eq!(
        *suffix_calls.lock().expect("suffix recorder poisoned"),
        [8],
        "the page-local suffix currently inherits the source frontier's width"
    );
    assert_eq!(query.stats().width_increases, 3);
    assert_eq!(query.current_width(), 8);
}

#[test]
fn nested_repeated_root_rpqs_keep_distinct_action_occurrences() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let make = |left: Option<Arc<AtomicUsize>>, right: Option<Arc<AtomicUsize>>| {
        let start = Variable::<GenId>::new(START);
        let end = Variable::<GenId>::new(END);
        let arm = |seeded_roots| {
            Box::new(CountingPath {
                inner: RegularPathConstraint::new(graph.set.clone(), start, end, &ops),
                seeded_roots,
                source_pages: None,
                expanded_nodes: Arc::new(AtomicUsize::new(0)),
            }) as DynConstraint
        };
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(start.is(graph.value(0))) as DynConstraint,
            Box::new(UnionConstraint::new(vec![arm(left), arm(right)])) as DynConstraint,
        ]))
    };
    let capabilities = ResidualCapabilities::default().root_formula().cyclic_rpq();
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let mut actual: Vec<_> = Query::new(
        make(Some(Arc::clone(&left)), Some(Arc::clone(&right))),
        project_end,
    )
    .solve_residual_state_lazy_with(capabilities)
    .collect();
    actual.sort_unstable();
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(left.load(Ordering::Relaxed), 1);
    assert_eq!(right.load(Ordering::Relaxed), 1);

    let observed = Query::new(make(None, None), project_end)
        .solve_residual_state_lazy_with(capabilities)
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
}

#[test]
fn cyclic_rpq_runs_as_a_direct_finite_or_proposal_action() {
    let graph = Graph::new(4, &[(0, 1), (1, 2), (2, 3)]);
    let ops = repeated(graph.attribute, false);
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = formula_bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &ops,
        Some(Arc::clone(&seeded)),
        Arc::clone(&expanded),
    );

    let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1)
        .collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected: Vec<_> = (1..4).map(|node| graph.value(node).raw).collect();
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert!(expanded.load(Ordering::Relaxed) >= 3);
}

#[test]
fn cyclic_rpq_runs_as_a_direct_finite_or_grouped_confirm_action() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let absent = genid(&rngid().id).raw;
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
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
        Some(Arc::clone(&seeded)),
        Arc::clone(&expanded),
    );

    let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1)
        .collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert!(expanded.load(Ordering::Relaxed) >= 3);
}

#[test]
fn cyclic_or_confirm_keeps_the_original_group_for_a_later_sibling() {
    let graph = Graph::new(4, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let cyclic = Box::new(CountingPath {
        inner: RegularPathConstraint::new(graph.set.clone(), start_var, end_var, &ops),
        seeded_roots: Some(Arc::clone(&seeded)),
        source_pages: None,
        expanded_nodes: Arc::clone(&expanded),
    }) as DynConstraint;
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

    let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1)
        .collect();
    let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
    lowered.sort_unstable();
    sequential.sort_unstable();
    assert_eq!(lowered, sequential);
    let mut expected = vec![graph.value(1).raw, graph.value(2).raw, graph.value(3).raw];
    expected.sort_unstable();
    assert_eq!(lowered, expected);
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert!(expanded.load(Ordering::Relaxed) >= 3);
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
        let seeded = Arc::new(AtomicUsize::new(0));
        let expanded = Arc::new(AtomicUsize::new(0));
        let root = formula_and_bound_start_root(
            graph.set.clone(),
            graph.value(0),
            candidates,
            path_estimate_wins,
            &ops,
            Some(Arc::clone(&seeded)),
            Arc::clone(&expanded),
        );
        let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
            .solve_residual_state_lazy_with(combined_effects())
            .collect();
        let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
        lowered.sort_unstable();
        sequential.sort_unstable();
        expected.sort_unstable();
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, expected);
        assert!(seeded.load(Ordering::Relaxed) > 0);
        assert!(expanded.load(Ordering::Relaxed) > 0);
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
        let seeded = Arc::new(AtomicUsize::new(0));
        let expanded = Arc::new(AtomicUsize::new(0));
        let cyclic = Box::new(CountingPath {
            inner: RegularPathConstraint::new(graph.set.clone(), start_var, end_var, &ops),
            seeded_roots: Some(Arc::clone(&seeded)),
            source_pages: None,
            expanded_nodes: Arc::clone(&expanded),
        }) as DynConstraint;
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

        let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
            .solve_residual_state_lazy_with(combined_effects())
            .cap(1)
            .start_width(1)
            .collect();
        let mut sequential: Vec<_> = Query::new(root, project_end).sequential().collect();
        let mut expected = vec![graph.value(0).raw, graph.value(2).raw, graph.value(3).raw];
        lowered.sort_unstable();
        sequential.sort_unstable();
        expected.sort_unstable();
        assert_eq!(lowered, sequential);
        assert_eq!(lowered, expected);
        assert_eq!(
            seeded.load(Ordering::Relaxed),
            1,
            "outer_confirmation={outer_confirmation}"
        );
        assert!(expanded.load(Ordering::Relaxed) >= 3);
    }
}

#[test]
fn synthetic_root_atom_streams_a_cycle_before_fixpoint_cleanup() {
    let graph = Graph::new(3, &[(0, 0), (1, 2)]);
    let node = Variable::<GenId>::new(START);
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = Arc::new(CountingPath {
        inner: RegularPathConstraint::new(
            graph.set.clone(),
            node,
            node,
            &repeated(graph.attribute, false),
        ),
        seeded_roots: None,
        source_pages: None,
        expanded_nodes: Arc::clone(&expanded),
    });
    let mut query = Query::new(root, project_start)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    assert_eq!(query.next(), Some(graph.value(0).raw));
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
    assert_eq!(query.next(), None);
    assert_eq!(expanded.load(Ordering::Relaxed), 2);
}

#[test]
fn synthetic_root_and_streams_early_and_late_page_local_survivors() {
    for (accepted_node, expected_before_emit, nested_and) in [(1, 1, false), (4, 4, true)] {
        let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let ops = repeated(graph.attribute, false);
        let expanded = Arc::new(AtomicUsize::new(0));
        let expected = graph.value(accepted_node).raw;
        let root = linear_formula_bound_start_filter_root(
            graph.set.clone(),
            graph.value(0),
            vec![expected],
            nested_and,
            &ops,
            Arc::clone(&expanded),
        );
        let mut query = Query::new(root, project_end)
            .solve_residual_state_lazy_with(root_formula_effects())
            .cap(1)
            .start_width(1);

        assert_eq!(query.next(), Some(expected));
        assert_eq!(
            expanded.load(Ordering::Relaxed),
            expected_before_emit,
            "accepted_node={accepted_node}, nested_and={nested_and}"
        );
        assert_eq!(query.next(), None);
        assert_eq!(expanded.load(Ordering::Relaxed), 5);
    }
}

#[test]
fn synthetic_root_and_empty_filter_waits_for_cleanup_without_replay() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let ops = repeated(graph.attribute, false);
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = linear_formula_bound_start_filter_root(
        graph.set.clone(),
        graph.value(0),
        vec![genid(&rngid().id).raw],
        false,
        &ops,
        Arc::clone(&expanded),
    );
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    assert_eq!(query.next(), None);
    assert_eq!(expanded.load(Ordering::Relaxed), 5);
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
        Arc::new(AtomicUsize::new(0)),
    );
    let quiescent = formula_and_bound_start_root(
        graph.set.clone(),
        graph.value(0),
        allowed,
        true,
        &ops,
        None,
        Arc::new(AtomicUsize::new(0)),
    );

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
fn linear_formula_streaming_keeps_byte_identical_parent_activations_distinct() {
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
    let mut expected = vec![
        graph.value(1).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        graph.value(2).raw,
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn clone_and_drop_preserve_a_live_linear_formula_stream() {
    let graph = Graph::new(4, &[(0, 1), (1, 2), (2, 3)]);
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
        Arc::clone(&expanded),
    );
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(root_formula_effects())
        .cap(1)
        .start_width(1);

    let first = query.next().expect("the first endpoint streamed");
    assert_eq!(first, graph.value(1).raw);
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
    let exact_clone = query.clone();
    let cancelled = query.clone();
    drop(cancelled);
    assert_eq!(expanded.load(Ordering::Relaxed), 1);

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
    assert_eq!(expanded.load(Ordering::Relaxed), 7);
}

#[test]
fn finite_or_keeps_cyclic_proposals_private_until_fixpoint_quiescence() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let ops = repeated(graph.attribute, false);
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = formula_bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &ops,
        None,
        Arc::clone(&expanded),
    );
    let mut query = Query::new(root, project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(1)
        .start_width(1);

    assert!(query.next().is_some());
    assert_eq!(
        expanded.load(Ordering::Relaxed),
        8,
        "an OR arm must not publish a partial cyclic proposal"
    );
    drop(query);
    assert_eq!(expanded.load(Ordering::Relaxed), 8);
}

#[test]
fn clone_and_drop_preserve_a_live_formula_cyclic_remainder() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (4, 5), (5, 6), (6, 7)]);
    let ops = repeated(graph.attribute, false);
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let expanded = Arc::new(AtomicUsize::new(0));
    let cyclic = Box::new(CountingPath {
        inner: RegularPathConstraint::new(graph.set.clone(), start_var, end_var, &ops),
        seeded_roots: None,
        source_pages: None,
        expanded_nodes: Arc::clone(&expanded),
    }) as DynConstraint;
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
    assert_eq!(
        expanded.load(Ordering::Relaxed),
        4,
        "the other source activation must remain live"
    );
    let exact_clone = query.clone();
    let cancelled = query.clone();
    drop(cancelled);
    assert_eq!(expanded.load(Ordering::Relaxed), 4);

    let mut original = vec![first];
    original.extend(query);
    let mut cloned = vec![first];
    cloned.extend(exact_clone);
    original.sort_unstable();
    cloned.sort_unstable();
    assert_eq!(cloned, original);
    assert_eq!(original.len(), 6);
    assert_eq!(expanded.load(Ordering::Relaxed), 12);
}

#[test]
fn formula_cyclic_lowering_remains_capability_and_program_gated() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let plus = repeated(graph.attribute, false);
    let expected = {
        let mut values = vec![graph.value(1).raw, graph.value(2).raw];
        values.sort_unstable();
        values
    };
    for capabilities in [
        ResidualCapabilities::default().finite_unions(),
        ResidualCapabilities::default().cyclic_rpq(),
    ] {
        let seeded = Arc::new(AtomicUsize::new(0));
        let expanded = Arc::new(AtomicUsize::new(0));
        let root = formula_bound_start_root(
            graph.set.clone(),
            graph.value(0),
            &plus,
            Some(Arc::clone(&seeded)),
            Arc::clone(&expanded),
        );
        let mut actual: Vec<_> = Query::new(root, project_end)
            .solve_residual_state_lazy_with(capabilities)
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(seeded.load(Ordering::Relaxed), 0);
        assert_eq!(expanded.load(Ordering::Relaxed), 0);
    }

    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = formula_bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &[PathOp::Attr(graph.attribute.raw())],
        Some(Arc::clone(&seeded)),
        Arc::clone(&expanded),
    );
    assert_eq!(
        Query::new(root, project_end)
            .solve_residual_state_lazy_with(combined_effects())
            .collect::<Vec<_>>(),
        vec![graph.value(1).raw]
    );
    assert_eq!(seeded.load(Ordering::Relaxed), 0);
    assert_eq!(expanded.load(Ordering::Relaxed), 0);
}

#[test]
fn zero_root_cyclic_and_returns_empty_without_erasing_its_or_sibling() {
    let graph = Graph::new(1, &[]);
    let node = Variable::<GenId>::new(START);
    let ops = repeated(graph.attribute, false);
    let seeded = Arc::new(AtomicUsize::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let expanded = Arc::new(AtomicUsize::new(0));
    let survivor = graph.value(0).raw;
    let cyclic = Box::new(CountingPath {
        inner: RegularPathConstraint::new(graph.set.clone(), node, node, &ops),
        seeded_roots: Some(Arc::clone(&seeded)),
        source_pages: Some(Arc::clone(&pages)),
        expanded_nodes: Arc::clone(&expanded),
    }) as DynConstraint;
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
    assert_eq!(seeded.load(Ordering::Relaxed), 0);
    assert_eq!(expanded.load(Ordering::Relaxed), 0);
    assert_eq!(
        *pages.lock().expect("source-page recorder poisoned"),
        vec![(1, 0, 0)]
    );
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
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let start_var = Variable::<GenId>::new(START);
    let end_var = Variable::<GenId>::new(END);
    let arm = Box::new(CountingPath {
        inner: RegularPathConstraint::new(graph.set.clone(), start_var, end_var, &ops),
        seeded_roots: Some(Arc::clone(&seeded)),
        source_pages: None,
        expanded_nodes: Arc::clone(&expanded),
    }) as DynConstraint;
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(DuplicateParents {
            outer_values,
            start: graph.value(0).raw,
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]));

    let mut lowered: Vec<_> = Query::new(Arc::clone(&root), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .collect();
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
    assert_eq!(seeded.load(Ordering::Relaxed), 2);
    assert!(expanded.load(Ordering::Relaxed) >= 6);
}

#[cfg(feature = "parallel")]
#[test]
fn formula_cyclic_parallel_split_preserves_affine_activations() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let ops = repeated(graph.attribute, false);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let make = || {
        let start_var = Variable::<GenId>::new(START);
        let end_var = Variable::<GenId>::new(END);
        let arm = Box::new(CountingPath {
            inner: RegularPathConstraint::new(graph.set.clone(), start_var, end_var, &ops),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes: Arc::new(AtomicUsize::new(0)),
        }) as DynConstraint;
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(DuplicateParents {
                outer_values,
                start: graph.value(0).raw,
            }) as DynConstraint,
            Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
        ]))
    };

    let mut serial: Vec<_> = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(2)
        .start_width(2)
        .collect();
    let mut parallel: Vec<_> = Query::new(make(), project_end)
        .solve_residual_state_lazy_with(combined_effects())
        .cap(2)
        .start_width(2)
        .into_par_iter()
        .collect();
    serial.sort_unstable();
    parallel.sort_unstable();
    assert_eq!(parallel, serial);
    assert_eq!(parallel.len(), 4);
}

#[test]
fn formula_same_variable_sources_keep_novelty_separate_at_shared_terms() {
    let graph = Graph::new(4, &[(0, 2), (1, 2), (2, 1), (3, 0)]);
    let seeded = Arc::new(AtomicUsize::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = same_variable_formula_confirm_root(
        graph.set.clone(),
        vec![graph.value(0).raw, graph.value(1).raw],
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        Some(Arc::clone(&pages)),
        Arc::clone(&expanded),
    );

    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
    assert_eq!(query.by_ref().collect::<Vec<_>>(), vec![graph.value(1).raw]);
    assert_eq!(seeded.load(Ordering::Relaxed), 2);
    assert!(expanded.load(Ordering::Relaxed) > 3);
    assert_eq!(
        *pages.lock().expect("source-page recorder poisoned"),
        vec![(1, 1, 1), (2, 1, 1)]
    );
    assert_eq!(query.stats().delta_source_dead_pages, 1);
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
            Box::new(UnionConstraint::new(vec![Box::new(CountingPath {
                inner: RegularPathConstraint::new(graph.set.clone(), node, node, &ops),
                seeded_roots: None,
                source_pages: None,
                expanded_nodes: Arc::new(AtomicUsize::new(0)),
            }) as DynConstraint])) as DynConstraint,
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
            || {
                bound_start_root(
                    graph.set.clone(),
                    graph.value(0),
                    &ops,
                    Arc::new(AtomicUsize::new(0)),
                )
            },
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
                || same_variable_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0))),
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
            || same_variable_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0))),
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
            || same_variable_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0))),
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

    let node = Variable::<UnknownInline>::new(START);
    let path = RegularPathConstraint::new(graph.set.clone(), node, node, &ops);
    let (sources, pages) = collect_same_variable_source_frontier(&path, None, 1);
    assert_eq!(sources, expected);
    assert_eq!(pages, vec![(1, 1), (1, 1), (1, 1)]);
    assert!(!sources.contains(&graph.value(2).raw));

    assert_all_schedulers(
        || same_variable_unknown_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0))),
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
    let node = Variable::<UnknownInline>::new(START);
    let path = RegularPathConstraint::new(graph.set.clone(), node, node, &ops);
    let mut expected = vec![
        graph.value(0).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        literal.raw,
    ];
    expected.sort_unstable();

    let (proposal_sources, proposal_pages) = collect_same_variable_source_frontier(&path, None, 2);
    assert_eq!(proposal_sources, expected);
    assert_eq!(
        proposal_pages
            .iter()
            .map(|&(examined, _)| examined)
            .collect::<Vec<_>>(),
        vec![2, 2, 2, 1]
    );
    assert_eq!(
        proposal_pages
            .iter()
            .map(|&(_, roots)| roots)
            .sum::<usize>(),
        expected.len()
    );

    let absent = [0xE7; 32];
    let mut candidates = vec![
        graph.value(0).raw,
        graph.value(1).raw,
        graph.value(2).raw,
        graph.value(3).raw,
        graph.value(4).raw,
        graph.value(5).raw,
        graph.value(6).raw,
        literal.raw,
        absent,
    ];
    candidates.sort_unstable();
    let (confirm_sources, confirm_pages) =
        collect_same_variable_source_frontier(&path, Some(&candidates), 3);
    assert_eq!(confirm_sources, proposal_sources);
    assert_eq!(
        confirm_pages
            .iter()
            .map(|&(examined, _)| examined)
            .collect::<Vec<_>>(),
        vec![3, 3, 3]
    );

    assert_all_schedulers(
        || same_variable_unknown_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0))),
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
            vec![
                graph.value(3).raw,
                graph.value(0).raw,
                graph.value(3).raw,
                graph.value(1).raw,
            ],
            3,
        ),
    ];
    for (ops, expected, expected_roots) in cases {
        let seeded = Arc::new(AtomicUsize::new(0));
        let pages = Arc::new(Mutex::new(Vec::new()));
        let expanded = Arc::new(AtomicUsize::new(0));
        let root = same_variable_confirm_root(
            graph.set.clone(),
            candidates.clone(),
            &ops,
            Arc::clone(&seeded),
            Some(Arc::clone(&pages)),
            Arc::clone(&expanded),
        );
        let mut query =
            Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());
        let actual: Vec<_> = query.by_ref().collect();
        assert_eq!(actual, expected);
        assert_eq!(seeded.load(Ordering::Relaxed), expected_roots);
        assert!(expanded.load(Ordering::Relaxed) >= expected_roots);
        let pages = pages.lock().expect("source-page recorder poisoned");
        assert_eq!(
            pages
                .iter()
                .map(|&(limit, examined, _)| (limit, examined))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (4, 1)]
        );
        assert_eq!(
            pages.iter().map(|&(_, _, roots)| roots).sum::<usize>(),
            expected_roots
        );
        drop(pages);
        assert_eq!(query.current_width(), 8);
        assert_eq!(query.stats().width_increases, 3);
        assert_eq!(query.stats().delta_source_dead_pages, 2);
    }
}

#[test]
fn same_variable_sources_do_not_share_seen_at_a_common_term() {
    // A -> C, B -> C, C -> B is the collision: A rejects after reaching C,
    // while B must continue through the same C and return to B. D -> A only
    // makes A survive the exact FIRST/last-source restriction.
    let graph = Graph::new(4, &[(0, 2), (1, 2), (2, 1), (3, 0)]);
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = same_variable_confirm_root(
        graph.set.clone(),
        vec![graph.value(0).raw, graph.value(1).raw],
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        None,
        Arc::clone(&expanded),
    );

    assert_eq!(
        Query::new(root, project_start)
            .solve_residual_state_lazy_with(combined_effects())
            .collect::<Vec<_>>(),
        vec![graph.value(1).raw]
    );
    assert_eq!(seeded.load(Ordering::Relaxed), 2);
    assert!(expanded.load(Ordering::Relaxed) > 3);
}

#[test]
fn same_variable_fixpoint_preserves_duplicate_outer_activations() {
    let graph = Graph::new(2, &[(0, 1), (1, 0)]);
    let outer_values = [genid(&rngid().id).raw, genid(&rngid().id).raw];
    let ops = repeated(graph.attribute, false);
    assert_all_schedulers(
        || {
            same_variable_outer_root(
                graph.set.clone(),
                outer_values,
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
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
    let seeded = Arc::new(AtomicUsize::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = counted_same_variable_root(
        graph.set.clone(),
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        Some(Arc::clone(&pages)),
        Arc::clone(&expanded),
    );
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    let first = query.next().expect("every source has a self-loop");
    assert!((0..8).any(|node| first == graph.value(node).raw));
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
    assert_eq!(
        *pages.lock().expect("source-page recorder poisoned"),
        vec![(1, 1, 1)]
    );
    drop(query);
    assert_eq!(seeded.load(Ordering::Relaxed), 1);
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
    assert_eq!(
        *pages.lock().expect("source-page recorder poisoned"),
        vec![(1, 1, 1)]
    );
}

#[test]
fn same_variable_negative_source_pages_grow_one_two_four() {
    let graph = Graph::new(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]);
    let seeded = Arc::new(AtomicUsize::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = counted_same_variable_root(
        graph.set.clone(),
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        Some(Arc::clone(&pages)),
        Arc::clone(&expanded),
    );
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), None);
    let pages = pages.lock().expect("source-page recorder poisoned");
    assert_eq!(
        pages
            .iter()
            .map(|&(limit, examined, _)| (limit, examined))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (4, 4)]
    );
    assert_eq!(pages.iter().map(|&(_, _, roots)| roots).sum::<usize>(), 6);
    drop(pages);
    assert_eq!(seeded.load(Ordering::Relaxed), 6);
    assert_eq!(query.current_width(), 8);
    assert_eq!(query.stats().width_increases, 3);
    assert_eq!(query.stats().delta_source_pages, 3);
    assert_eq!(query.stats().delta_source_candidates_examined, 7);
    assert_eq!(query.stats().delta_source_roots, 6);
    assert_eq!(query.stats().delta_source_dead_pages, 3);
    assert_eq!(query.stats().delta_source_negative_steps, 3);
    assert_eq!(query.stats().delta_handoff_probe_pops, 0);
    assert!(expanded.load(Ordering::Relaxed) >= 6);
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
    let seeded = Arc::new(AtomicUsize::new(0));
    let pages = Arc::new(Mutex::new(Vec::new()));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = counted_same_variable_root(
        graph.set.clone(),
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        Some(Arc::clone(&pages)),
        Arc::clone(&expanded),
    );
    let mut query =
        Query::new(root, project_start).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), Some(graph.value(target).raw));
    let pages = pages.lock().expect("source-page recorder poisoned");
    assert_eq!(
        pages
            .iter()
            .map(|&(limit, examined, _)| (limit, examined))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (4, 4)]
    );
    assert_eq!(pages.iter().map(|&(_, _, roots)| roots).sum::<usize>(), 6);
    drop(pages);
    assert_eq!(seeded.load(Ordering::Relaxed), 6);
    assert_eq!(query.current_width(), 8);
    assert_eq!(query.stats().width_increases, 3);
    assert_eq!(query.stats().delta_source_dead_pages, 2);
    assert_eq!(query.stats().delta_source_negative_steps, 2);
    assert_eq!(query.stats().delta_handoff_probe_pops, 1);
    drop(query);
}

#[test]
fn same_variable_delta_remains_opt_in() {
    let graph = Graph::new(1, &[(0, 0)]);
    let seeded = Arc::new(AtomicUsize::new(0));
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = counted_same_variable_root(
        graph.set.clone(),
        &repeated(graph.attribute, false),
        Arc::clone(&seeded),
        None,
        Arc::clone(&expanded),
    );

    assert_eq!(
        Query::new(root, project_start)
            .solve_residual_state_lazy()
            .collect::<Vec<_>>(),
        vec![graph.value(0).raw]
    );
    assert_eq!(seeded.load(Ordering::Relaxed), 0);
    assert_eq!(expanded.load(Ordering::Relaxed), 0);
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
            || {
                bound_start_root(
                    graph.set.clone(),
                    graph.value(0),
                    ops,
                    Arc::new(AtomicUsize::new(0)),
                )
            },
            project_end,
            expected.clone(),
        );

        let absent = genid(&rngid().id);
        assert_all_schedulers(
            || {
                bound_start_root(
                    graph.set.clone(),
                    absent,
                    ops,
                    Arc::new(AtomicUsize::new(0)),
                )
            },
            project_end,
            Vec::new(),
        );
    }

    let expanded = Arc::new(AtomicUsize::new(0));
    let _ = run(
        bound_start_root(
            graph.set.clone(),
            graph.value(0),
            &star,
            Arc::clone(&expanded),
        ),
        Scheduler::Residual,
        project_end,
    );
    assert!(expanded.load(Ordering::Relaxed) > 0);
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
        || {
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
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
        || {
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
        project_end,
        vec![graph.value(2).raw, graph.value(4).raw],
    );
    assert_all_schedulers(
        || {
            bound_end_root(
                graph.set.clone(),
                graph.value(4),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
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
        || {
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
        project_end,
        vec![graph.value(1).raw, graph.value(2).raw],
    );
    assert_all_schedulers(
        || {
            bound_end_root(
                graph.set.clone(),
                graph.value(2),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            )
        },
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
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &forward,
                Arc::new(AtomicUsize::new(0)),
            ),
            project_end,
            vec![graph.value(1).raw, graph.value(2).raw],
        ),
        (
            bound_start_root(
                graph.set.clone(),
                graph.value(2),
                &inverse,
                Arc::new(AtomicUsize::new(0)),
            ),
            project_end,
            vec![graph.value(0).raw, graph.value(1).raw],
        ),
        (
            bound_end_root(
                graph.set.clone(),
                graph.value(2),
                &forward,
                Arc::new(AtomicUsize::new(0)),
            ),
            project_start,
            vec![graph.value(0).raw, graph.value(1).raw],
        ),
        (
            bound_end_root(
                graph.set.clone(),
                graph.value(0),
                &inverse,
                Arc::new(AtomicUsize::new(0)),
            ),
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
fn target_confirm_traverses_once_and_preserves_reachable_duplicate_candidates() {
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
            vec![graph.value(2).raw, graph.value(2).raw, graph.value(1).raw],
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
            vec![graph.value(0).raw, graph.value(0).raw, graph.value(1).raw],
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
            vec![graph.value(0).raw, graph.value(0).raw, graph.value(1).raw],
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
            vec![graph.value(2).raw, graph.value(2).raw, graph.value(1).raw],
            project_start,
        ),
    ];

    for (candidate_variable, bound, ops, candidates, mut expected, project) in cases {
        let expanded = Arc::new(AtomicUsize::new(0));
        let root = target_confirm_root(
            graph.set.clone(),
            candidate_variable,
            bound,
            candidates,
            &ops,
            Arc::clone(&expanded),
        );
        expected.sort_unstable();
        assert_eq!(run(root, Scheduler::Residual, project), expected);
        assert_eq!(
            expanded.load(Ordering::Relaxed),
            3,
            "one traversal should expand each reachable frontier node once"
        );
    }
}

#[test]
fn automaton_target_confirm_filters_the_original_duplicate_sequence() {
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
    let expected = vec![
        graph.value(2).raw,
        graph.value(0).raw,
        graph.value(2).raw,
        graph.value(1).raw,
    ];
    let expanded = Arc::new(AtomicUsize::new(0));
    let residual = run(
        target_confirm_root(
            graph.set.clone(),
            END,
            graph.value(0),
            candidates.clone(),
            &ops,
            Arc::clone(&expanded),
        ),
        Scheduler::Residual,
        project_end,
    );
    let dag = run(
        target_confirm_root(
            graph.set.clone(),
            END,
            graph.value(0),
            candidates,
            &ops,
            Arc::new(AtomicUsize::new(0)),
        ),
        Scheduler::Dag,
        project_end,
    );
    let mut expected = expected;
    expected.sort_unstable();
    assert_eq!(residual, expected);
    assert_eq!(dag, expected);
    assert!(expanded.load(Ordering::Relaxed) > 0);
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
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(end_var.is(literal)) as DynConstraint,
        Box::new(CountingPath {
            inner: RegularPathConstraint::new(
                graph.set.clone(),
                start_var,
                end_var,
                &repeated(graph.attribute, false),
            ),
            seeded_roots: None,
            source_pages: None,
            expanded_nodes: Arc::clone(&expanded),
        }) as DynConstraint,
    ]));

    assert_eq!(
        run(root, Scheduler::Residual, project_start),
        vec![graph.value(0).raw]
    );
    assert_eq!(expanded.load(Ordering::Relaxed), 2);
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
fn default_residual_capabilities_keep_plus_opaque() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let proposed = Arc::new(AtomicUsize::new(0));
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
        Arc::clone(&proposed),
    );
    let mut actual: Vec<_> = Query::new(root, project_end)
        .solve_residual_state_lazy()
        .collect();
    actual.sort_unstable();
    let mut expected = [graph.value(1).raw, graph.value(2).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(
        proposed.load(Ordering::Relaxed),
        0,
        "cyclic RPQ proposal lowering must remain explicitly opt-in"
    );

    let confirmed = Arc::new(AtomicUsize::new(0));
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
        Arc::clone(&confirmed),
    );
    let mut actual: Vec<_> = Query::new(root, project_end)
        .solve_residual_state_lazy()
        .collect();
    actual.sort_unstable();
    let mut expected = [graph.value(2).raw, graph.value(2).raw, graph.value(1).raw];
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert_eq!(
        confirmed.load(Ordering::Relaxed),
        0,
        "cyclic RPQ confirmation lowering must remain explicitly opt-in"
    );
}

#[test]
fn first_result_requires_one_expansion_and_drop_cancels_the_remainder() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let expanded = Arc::new(AtomicUsize::new(0));
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
        Arc::clone(&expanded),
    );
    let mut query =
        Query::new(root, project_end).solve_residual_state_lazy_with(combined_effects());

    assert_eq!(query.next(), Some(graph.value(1).raw));
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
    drop(query);
    assert_eq!(expanded.load(Ordering::Relaxed), 1);
}

#[test]
fn clone_after_first_result_has_two_independent_exact_remainders() {
    let graph = Graph::new(5, &[(0, 1), (1, 2), (2, 3), (3, 4)]);
    let root = bound_start_root(
        graph.set.clone(),
        graph.value(0),
        &repeated(graph.attribute, false),
        Arc::new(AtomicUsize::new(0)),
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
    let root = same_variable_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0)));
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
            let make_root = || {
                bound_start_root(
                    graph.set.clone(),
                    graph.value(0),
                    &ops,
                    Arc::new(AtomicUsize::new(0)),
                )
            };
            let residual = run(make_root(), Scheduler::Residual, project_end);
            assert_eq!(residual, run(make_root(), Scheduler::Dag, project_end));
            assert_eq!(
                residual,
                run(make_root(), Scheduler::Sequential, project_end)
            );

            let make_same_root =
                || same_variable_root(graph.set.clone(), &ops, Arc::new(AtomicUsize::new(0)));
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
fn finite_concat_union_and_not_attr_stay_on_the_opaque_fallback() {
    let graph = Graph::new(3, &[(0, 1), (1, 2)]);
    let cases = [
        vec![
            PathOp::Attr(graph.attribute.raw()),
            PathOp::Attr(graph.attribute.raw()),
            PathOp::Concat,
        ],
        vec![
            PathOp::Attr(graph.attribute.raw()),
            PathOp::Attr(graph.attribute.raw()),
            PathOp::Union,
        ],
        vec![PathOp::NotAttr(graph.attribute.raw())],
    ];
    for ops in cases {
        let expanded = Arc::new(AtomicUsize::new(0));
        let residual = run(
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &ops,
                Arc::clone(&expanded),
            ),
            Scheduler::Residual,
            project_end,
        );
        let dag = run(
            bound_start_root(
                graph.set.clone(),
                graph.value(0),
                &ops,
                Arc::new(AtomicUsize::new(0)),
            ),
            Scheduler::Dag,
            project_end,
        );
        assert_eq!(residual, dag);
        assert_eq!(expanded.load(Ordering::Relaxed), 0);
    }
}
