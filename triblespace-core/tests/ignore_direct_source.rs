//! Direct proposal paging receipts for the opaque `ignore!` scope boundary.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, IgnoreConstraint, Query, ResidualDeltaOutput,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, TriblePattern, Variable,
    VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const PARENT: VariableId = 0;
const ENTITY: VariableId = 1;
const HIDDEN_VALUE: VariableId = 2;

fn raw(byte: u8) -> RawInline {
    [byte; 32]
}

fn id(byte: u8) -> Id {
    Id::new([byte; 16]).expect("test ids are nonzero")
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
}

#[test]
fn width_one_yields_after_one_ignored_entry_and_drop_cancels_the_frontier() {
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
    assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    drop(query);
    assert_eq!(counters.page_calls.load(Ordering::Relaxed), 1);
    assert_eq!(counters.examined.load(Ordering::Relaxed), 1);
}

#[test]
fn hidden_and_confirmation_shapes_do_not_cross_the_scope_boundary() {
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
