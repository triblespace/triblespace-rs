//! Direct proposal paging receipts for the opaque `ignore!` scope boundary.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, IgnoreConstraint, PathOp, Query,
    RegularPathConstraint, ResidualDeltaExpandBatch, ResidualDeltaExpandCursor,
    ResidualDeltaExpandPage, ResidualDeltaNode, ResidualDeltaOutput, ResidualDeltaSeed,
    ResidualDeltaSourceBatch, ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView,
    TriblePattern, Variable, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const PARENT: VariableId = 0;
const ENTITY: VariableId = 1;
const HIDDEN_VALUE: VariableId = 2;
const START: VariableId = 3;
const END: VariableId = 4;
const HIDDEN_PATH_STATE: VariableId = 5;

fn raw(byte: u8) -> RawInline {
    [byte; 32]
}

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).expect("test ids are nonzero")
}

fn id_raw(id: Id) -> RawInline {
    let inline: Inline<GenId> = id.to_inline();
    inline.raw
}

fn relation(attribute: Id, entities: impl IntoIterator<Item = u8>) -> TribleSet {
    let mut set = TribleSet::new();
    for byte in entities {
        let entity = id(byte);
        let value = Inline::<UnknownInline>::new(raw(byte.wrapping_add(0x40)));
        set.insert(&Trible::force(&entity, &attribute, &value));
    }
    set
}

fn ignored_entity_source<'a>(set: &'a TribleSet, attribute: Id) -> IgnoreConstraint<'a> {
    let entity = Variable::<GenId>::new(ENTITY);
    let hidden = Variable::<UnknownInline>::new(HIDDEN_VALUE);
    let attribute: Inline<GenId> = attribute.to_inline();
    IgnoreConstraint::new(
        VariableSet::new_singleton(hidden.index),
        Box::new(set.pattern(entity, attribute, hidden)),
    )
}

fn project_entity(binding: &Binding) -> Option<RawInline> {
    binding.get(ENTITY).copied()
}

fn eager_proposal<'a, C: Constraint<'a>>(constraint: &C) -> Vec<RawInline> {
    let mut values = Vec::new();
    constraint.propose(
        ENTITY,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut values),
    );
    values
}

fn paged_proposal<'a, C: Constraint<'a>>(constraint: &C) -> Vec<RawInline> {
    assert!(constraint.residual_proposal_source_is_paged(ENTITY, &RowsView::EMPTY));
    let mut values = Vec::new();
    let mut cursor = ResidualDeltaSourceCursor::Start;
    loop {
        let before = values.len();
        let mut roots = Vec::new();
        let page = constraint
            .residual_delta_source_page(
                ENTITY,
                &RowsView::EMPTY,
                None,
                cursor,
                2,
                &mut roots,
                &mut values,
            )
            .expect("Ignore keeps its outward direct source available");
        assert!(roots.is_empty());
        assert_eq!(values.len() - before, page.examined);
        assert!(page.examined <= 2);
        let Some(next) = page.next else {
            break;
        };
        match (cursor, next) {
            (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
            (
                ResidualDeltaSourceCursor::After(previous),
                ResidualDeltaSourceCursor::After(next),
            ) => assert!(next > previous),
            _ => panic!("ignored direct source changed cursor families or restarted"),
        }
        cursor = next;
    }
    values
}

#[test]
fn ignored_wildcard_pages_match_eager_and_all_residual_entry_paths() {
    let attribute = id(0xf0);
    let set = relation(attribute, 1..=5);
    let source = ignored_entity_source(&set, attribute);
    assert_eq!(
        source.variables(),
        VariableSet::new_singleton(ENTITY),
        "the hidden value remains outside the canonical row schema"
    );
    assert!(source.residual_confirm_is_page_local());

    let mut eager_proposal = eager_proposal(&source);
    let mut direct = paged_proposal(&source);
    eager_proposal.sort_unstable();
    direct.sort_unstable();
    assert_eq!(direct, eager_proposal);

    let mut sequential: Vec<_> = Query::new(ignored_entity_source(&set, attribute), project_entity)
        .sequential()
        .collect();
    let mut ordinary: Vec<_> =
        Query::new(ignored_entity_source(&set, attribute), project_entity).collect();
    let mut eager =
        Query::new(ignored_entity_source(&set, attribute), project_entity).solve_residual_state();
    let mut full_query = Query::new(ignored_entity_source(&set, attribute), project_entity)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let mut full: Vec<_> = full_query.by_ref().collect();
    for bag in [&mut sequential, &mut ordinary, &mut eager, &mut full] {
        bag.sort_unstable();
    }
    assert_eq!(sequential, direct);
    assert_eq!(ordinary, sequential);
    assert_eq!(eager, sequential);
    assert_eq!(full, sequential);
    assert_eq!(full_query.stats().delta_source_direct_candidates, set.len());
    assert_eq!(full_query.stats().delta_source_roots, 0);
    assert_eq!(full_query.stats().max_propose_candidates, 1);
}

#[derive(Clone, Copy)]
struct DuplicateDomain {
    value: RawInline,
}

impl Constraint<'_> for DuplicateDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(PARENT)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != PARENT {
            return false;
        }
        out.fill(2, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == PARENT {
            for row in 0..view.len() as u32 {
                candidates.extend_row(row, [self.value, self.value]);
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == PARENT {
            candidates.retain(|_, value| *value == self.value);
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(PARENT)
            .is_none_or(|column| view.iter().all(|row| row[column] == self.value))
    }
}

fn affine_root<'a>(
    set: &'a TribleSet,
    attribute: Id,
) -> IntersectionConstraint<Box<dyn Constraint<'a> + 'a>> {
    IntersectionConstraint::new(vec![
        Box::new(DuplicateDomain { value: raw(0x22) }) as Box<dyn Constraint<'a>>,
        Box::new(ignored_entity_source(set, attribute)) as Box<dyn Constraint<'a>>,
    ])
}

#[test]
fn ignored_direct_source_preserves_duplicate_affine_parents() {
    let attribute = id(0xf1);
    let set = relation(attribute, 1..=4);
    let project = |binding: &Binding| Some((*binding.get(PARENT)?, *binding.get(ENTITY)?));

    let mut sequential: Vec<_> = Query::new(affine_root(&set, attribute), project)
        .sequential()
        .collect();
    let mut ordinary: Vec<_> = Query::new(affine_root(&set, attribute), project).collect();
    let mut eager = Query::new(affine_root(&set, attribute), project).solve_residual_state();
    let mut full_query = Query::new(affine_root(&set, attribute), project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let mut full: Vec<_> = full_query.by_ref().collect();
    for bag in [&mut sequential, &mut ordinary, &mut eager, &mut full] {
        bag.sort_unstable();
    }
    assert_eq!(sequential.len(), 2 * set.len());
    assert!(sequential.iter().all(|(parent, _)| *parent == raw(0x22)));
    assert_eq!(ordinary, sequential);
    assert_eq!(eager, sequential);
    assert_eq!(full, sequential);
    assert_eq!(
        full_query.stats().delta_source_direct_candidates,
        2 * set.len()
    );
    assert_eq!(full_query.stats().delta_source_roots, 0);
}

#[derive(Clone, Default)]
struct SourceCounters {
    propose_calls: Arc<AtomicUsize>,
    batch_calls: Arc<AtomicUsize>,
    page_calls: Arc<AtomicUsize>,
    examined: Arc<AtomicUsize>,
}

struct CountedSource<C> {
    inner: C,
    counters: SourceCounters,
}

impl<'a, C: Constraint<'a>> Constraint<'a> for CountedSource<C> {
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
        self.counters.propose_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn influence(&self, variable: VariableId) -> VariableSet {
        self.inner.influence(variable)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.inner.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        let page = self
            .inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted);
        if let Some(page) = page {
            self.counters.page_calls.fetch_add(1, Ordering::Relaxed);
            self.counters
                .examined
                .fetch_add(page.examined, Ordering::Relaxed);
        }
        page
    }

    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        let page_base = pages.len();
        let supported = self
            .inner
            .residual_delta_source_pages(variable, batch, pages, roots, accepted);
        if supported {
            self.counters.batch_calls.fetch_add(1, Ordering::Relaxed);
            self.counters
                .page_calls
                .fetch_add(pages.len() - page_base, Ordering::Relaxed);
            self.counters.examined.fetch_add(
                pages[page_base..].iter().map(|page| page.examined).sum(),
                Ordering::Relaxed,
            );
        }
        supported
    }
}

#[test]
fn width_one_forwards_native_batch_and_drop_cancels_the_frontier() {
    let attribute = id(0xf2);
    let set = relation(attribute, 1..=64);
    let entity = Variable::<GenId>::new(ENTITY);
    let hidden = Variable::<UnknownInline>::new(HIDDEN_VALUE);
    let attribute_inline: Inline<GenId> = attribute.to_inline();
    let counters = SourceCounters::default();
    let counted = CountedSource {
        inner: set.pattern(entity, attribute_inline, hidden),
        counters: counters.clone(),
    };
    let ignored =
        IgnoreConstraint::new(VariableSet::new_singleton(hidden.index), Box::new(counted));
    let mut query = Query::new(ignored, project_entity)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);

    assert!(query.next().is_some());
    assert_eq!(counters.propose_calls.load(Ordering::Relaxed), 0);
    assert_eq!(counters.batch_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    drop(query);
    assert_eq!(counters.batch_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
}

#[test]
fn hidden_and_unsupported_confirmation_shapes_keep_fallback() {
    let attribute = id(0xf3);
    let set = relation(attribute, 1..=2);
    let source = ignored_entity_source(&set, attribute);
    assert!(!source.residual_proposal_source_is_paged(HIDDEN_VALUE, &RowsView::EMPTY));

    let mut roots = Vec::new();
    let mut accepted = Vec::new();
    assert!(source
        .residual_delta_source_page(
            HIDDEN_VALUE,
            &RowsView::EMPTY,
            None,
            ResidualDeltaSourceCursor::Start,
            1,
            &mut roots,
            &mut accepted,
        )
        .is_none());
    assert!(source
        .residual_delta_source_page(
            ENTITY,
            &RowsView::EMPTY,
            Some(&[]),
            ResidualDeltaSourceCursor::Start,
            1,
            &mut roots,
            &mut accepted,
        )
        .is_none());
    assert!(roots.is_empty());
    assert!(accepted.is_empty());
}

#[test]
#[should_panic(expected = "ordinal cursor crossed into a TribleSet source frontier")]
fn ignored_tribleset_frontier_rejects_ordinal_cursors() {
    let attribute = id(0xf4);
    let set = relation(attribute, 1..=2);
    ignored_entity_source(&set, attribute).residual_delta_source_page(
        ENTITY,
        &RowsView::EMPTY,
        None,
        ResidualDeltaSourceCursor::Offset(1),
        1,
        &mut Vec::new(),
        &mut Vec::new(),
    );
}

#[test]
fn monotone_relation_growth_only_adds_visible_rows() {
    let attribute = id(0xf5);
    let base = relation(attribute, 1..=3);
    let mut grown = base.clone();
    let added = id(4);
    let value = Inline::<UnknownInline>::new(raw(0x77));
    grown.insert(&Trible::force(&added, &attribute, &value));
    let solve = |set: &TribleSet| {
        Query::new(ignored_entity_source(set, attribute), project_entity)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .collect::<Vec<_>>()
    };

    let before = solve(&base);
    let mut after = solve(&grown);
    for old in before {
        let position = after
            .iter()
            .position(|candidate| *candidate == old)
            .expect("monotone TribleSet growth removed a visible wildcard row");
        after.remove(position);
    }
    let added: Inline<GenId> = added.to_inline();
    assert_eq!(after, [added.raw]);
}

struct PathWithHidden {
    path: RegularPathConstraint,
    hidden_value: RawInline,
    support_calls: Arc<AtomicUsize>,
}

impl Constraint<'static> for PathWithHidden {
    fn variables(&self) -> VariableSet {
        let mut variables = self.path.variables();
        variables.set(HIDDEN_PATH_STATE);
        variables
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable == HIDDEN_PATH_STATE {
            out.fill(1, view.len());
            true
        } else {
            self.path.estimate(variable, view, out)
        }
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == HIDDEN_PATH_STATE {
            for row in 0..view.len() as u32 {
                candidates.push(row, self.hidden_value);
            }
        } else {
            self.path.propose(variable, view, candidates);
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == HIDDEN_PATH_STATE {
            candidates.retain(|_, value| *value == self.hidden_value);
        } else {
            self.path.confirm(variable, view, candidates);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.path.satisfied(view)
            && view
                .col(HIDDEN_PATH_STATE)
                .is_none_or(|column| view.iter().all(|row| row[column] == self.hidden_value))
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.path.residual_confirm_is_page_local()
    }

    fn residual_delta_confirm_is_grouped(&self) -> bool {
        self.path.residual_delta_confirm_is_grouped()
    }

    fn residual_delta_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable != HIDDEN_PATH_STATE && self.path.residual_delta_source_is_paged(variable, view)
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable != HIDDEN_PATH_STATE && self.path.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if variable == HIDDEN_PATH_STATE {
            return None;
        }
        self.path
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        variable != HIDDEN_PATH_STATE && self.path.residual_delta_seeds(variable, view, seeds)
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        self.support_calls.fetch_add(1, Ordering::Relaxed);
        if view.col(HIDDEN_PATH_STATE).is_none() {
            return None;
        }
        self.path.residual_delta_support_seeds(view, seeds)
    }

    fn residual_delta_expand_page(
        &self,
        variable: VariableId,
        node: ResidualDeltaNode,
        cursor: ResidualDeltaExpandCursor,
        limit: usize,
        successors: &mut Vec<ResidualDeltaOutput>,
    ) -> Option<ResidualDeltaExpandPage> {
        if variable == HIDDEN_PATH_STATE {
            return None;
        }
        self.path
            .residual_delta_expand_page(variable, node, cursor, limit, successors)
    }

    fn residual_delta_expand_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaExpandBatch<'_>,
        pages: &mut Vec<Option<ResidualDeltaExpandPage>>,
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) {
        if variable == HIDDEN_PATH_STATE {
            pages.resize(pages.len() + batch.nodes.len(), None);
            return;
        }
        self.path
            .residual_delta_expand_pages(variable, batch, pages, successors);
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        variable != HIDDEN_PATH_STATE
            && self.path.residual_delta_expand(variable, nodes, successors)
    }
}

fn insert_edge(set: &mut TribleSet, from: Id, attribute: Id, to: Id) {
    let to: Inline<GenId> = to.to_inline();
    set.insert(&Trible::force(&from, &attribute, &to));
}

fn path_graph(attribute: Id) -> (TribleSet, [Id; 4]) {
    let nodes = [id(0x11), id(0x12), id(0x13), id(0x14)];
    let mut set = TribleSet::new();
    insert_edge(&mut set, nodes[0], attribute, nodes[1]);
    insert_edge(&mut set, nodes[1], attribute, nodes[0]);
    insert_edge(&mut set, nodes[2], attribute, nodes[3]);
    (set, nodes)
}

fn ignored_path(
    set: TribleSet,
    attribute: Id,
    start: Variable<GenId>,
    end: Variable<GenId>,
    support_calls: Arc<AtomicUsize>,
) -> IgnoreConstraint<'static> {
    let path = RegularPathConstraint::new(
        set,
        start,
        end,
        &[PathOp::Attr(attribute.into()), PathOp::Plus],
    );
    IgnoreConstraint::new(
        VariableSet::new_singleton(HIDDEN_PATH_STATE),
        Box::new(PathWithHidden {
            path,
            hidden_value: raw(0x99),
            support_calls,
        }),
    )
}

#[test]
fn ignored_same_variable_path_keeps_paged_roots_grouping_and_expansion() {
    let attribute = id(0xe0);
    let (set, _nodes) = path_graph(attribute);
    let node = Variable::<GenId>::new(START);
    let make = || {
        ignored_path(
            set.clone(),
            attribute,
            node,
            node,
            Arc::new(AtomicUsize::new(0)),
        )
    };

    let source = make();
    assert_eq!(source.variables(), VariableSet::new_singleton(START));
    assert!(source.residual_delta_confirm_is_grouped());
    assert!(source.residual_delta_source_is_paged(START, &RowsView::EMPTY));
    let mut roots = Vec::new();
    let page = source
        .residual_delta_source_page(
            START,
            &RowsView::EMPTY,
            None,
            ResidualDeltaSourceCursor::Start,
            1,
            &mut roots,
            &mut Vec::new(),
        )
        .expect("the visible same-variable source crosses Ignore");
    assert_eq!(page.examined, 1);
    assert_eq!(roots.len(), 1);
    let mut paged_successors = Vec::new();
    let transition_page = source
        .residual_delta_expand_page(
            START,
            roots[0].node,
            ResidualDeltaExpandCursor::Start,
            1,
            &mut paged_successors,
        )
        .expect("the visible RPQ transition page crosses Ignore");
    assert_eq!(transition_page.examined, 1);
    assert_eq!(paged_successors.len(), 1);
    let mut successors = Vec::new();
    assert!(source.residual_delta_expand(START, &[roots[0].node], &mut successors));
    assert!(!successors.is_empty());

    let mut sequential: Vec<_> =
        Query::new(make(), |binding: &Binding| binding.get(START).copied())
            .sequential()
            .collect();
    let mut full_query = Query::new(make(), |binding: &Binding| binding.get(START).copied())
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let mut full: Vec<_> = full_query.by_ref().collect();
    sequential.sort_unstable();
    full.sort_unstable();
    assert_eq!(full, sequential);
    assert_eq!(full.len(), 2);
    assert!(full_query.stats().delta_source_pages > 0);
    assert!(full_query.stats().delta_source_roots > 0);
    assert!(full_query.stats().delta_transition_pages > 0);
    assert!(full_query.stats().delta_transition_cohorts > 0);
    assert_eq!(full_query.stats().delta_source_direct_candidates, 0);
}

#[derive(Clone)]
struct OrderedDomain {
    values: Vec<RawInline>,
}

impl Constraint<'static> for OrderedDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(START)
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
        out.fill(1, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == START {
            for row in 0..view.len() as u32 {
                candidates.extend_row(row, self.values.iter().copied());
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
            candidates.retain(|_, value| self.values.contains(value));
        }
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(START)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

type SendConstraint = Box<dyn Constraint<'static> + Send + Sync>;

fn grouped_ignore_root(
    set: TribleSet,
    attribute: Id,
    values: Vec<RawInline>,
) -> IntersectionConstraint<SendConstraint> {
    let node = Variable::<GenId>::new(START);
    IntersectionConstraint::new(vec![
        Box::new(OrderedDomain { values }) as SendConstraint,
        Box::new(ignored_path(
            set,
            attribute,
            node,
            node,
            Arc::new(AtomicUsize::new(0)),
        )) as SendConstraint,
    ])
}

#[test]
fn ignored_grouped_path_confirmation_restores_duplicate_candidate_occurrences() {
    let attribute = id(0xe1);
    let (set, nodes) = path_graph(attribute);
    let values = vec![
        id_raw(nodes[0]),
        id_raw(nodes[0]),
        id_raw(nodes[1]),
        id_raw(nodes[2]),
    ];
    let make = || grouped_ignore_root(set.clone(), attribute, values.clone());
    let project = |binding: &Binding| binding.get(START).copied();

    let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
    let mut ordinary: Vec<_> = Query::new(make(), project).collect();
    let mut full_query = Query::new(make(), project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
    let mut full: Vec<_> = full_query.by_ref().collect();
    for bag in [&mut sequential, &mut ordinary, &mut full] {
        bag.sort_unstable();
    }
    assert_eq!(ordinary, sequential);
    assert_eq!(full, sequential);
    assert_eq!(full.len(), 3);
    assert_eq!(full[0], full[1], "the repeated cycle candidate is affine");
    assert!(full_query.stats().delta_source_pages > 0);
    assert!(full_query.stats().delta_source_roots > 0);
    assert_eq!(full_query.stats().delta_source_direct_candidates, 0);
}

#[test]
fn ignored_bound_endpoint_delegates_seeds_but_never_child_support() {
    let attribute = id(0xe2);
    let (set, nodes) = path_graph(attribute);
    let start = Variable::<GenId>::new(START);
    let end = Variable::<GenId>::new(END);
    let support_calls = Arc::new(AtomicUsize::new(0));
    let path = ignored_path(
        set.clone(),
        attribute,
        start,
        end,
        Arc::clone(&support_calls),
    );
    let start_value: Inline<GenId> = nodes[0].to_inline();
    let end_value: Inline<GenId> = nodes[1].to_inline();
    let start_vars = [START];
    let start_row = [start_value.raw];
    let mut seeds = Vec::new();
    assert!(path.residual_delta_seeds(END, &RowsView::new(&start_vars, &start_row), &mut seeds));
    assert_eq!(seeds.len(), 1);
    let mut successors = Vec::new();
    assert!(path.residual_delta_expand(END, &[seeds[0].output.node], &mut successors));
    assert!(!successors.is_empty());

    let bound_vars = [START, END];
    let bound_row = [start_value.raw, end_value.raw];
    let mut support_seeds = Vec::new();
    assert_eq!(
        path.residual_delta_support_seeds(
            &RowsView::new(&bound_vars, &bound_row),
            &mut support_seeds,
        ),
        None
    );
    assert!(support_seeds.is_empty());
    assert_eq!(
        support_calls.load(Ordering::Relaxed),
        0,
        "Ignore Support must not ask a child whose hidden variable is absent"
    );

    let make = || {
        IntersectionConstraint::new(vec![
            Box::new(start.is(start_value)) as SendConstraint,
            Box::new(ignored_path(
                set.clone(),
                attribute,
                start,
                end,
                Arc::new(AtomicUsize::new(0)),
            )) as SendConstraint,
        ])
    };
    let project = |binding: &Binding| binding.get(END).copied();
    let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
    let mut full: Vec<_> = Query::new(make(), project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1)
        .collect();
    sequential.sort_unstable();
    full.sort_unstable();
    assert_eq!(full, sequential);
    assert_eq!(full.len(), 2);
}
