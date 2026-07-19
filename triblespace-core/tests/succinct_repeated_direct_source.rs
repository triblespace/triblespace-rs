//! Receipts for bounded repeated-position proposal sources in SuccinctArchive.
//!
//! A page advances over a strict distinct Ring driver. Equality misses consume
//! demand exactly like hits and the public cursor resumes after the last driver
//! value examined, so a negative prefix participates in geometric widening.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, ResidualDeltaOutput,
    ResidualDeltaSourceCursor, ResidualDeltaSourcePage, RowsView, Variable, VariableId,
    VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

fn id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are nonzero")
}

fn inline_id(value: Id) -> Inline<GenId> {
    value.to_inline()
}

fn raw_id(value: Id) -> RawInline {
    inline_id(value).raw
}

fn repeated_fixture() -> (TribleSet, [Id; 6]) {
    let ids = [id(1), id(2), id(3), id(4), id(5), id(6)];
    let mut set = TribleSet::new();
    let mut insert = |e: usize, a: usize, v: usize| {
        set.insert(&Trible::force(&ids[e], &ids[a], &inline_id(ids[v])));
    };

    insert(0, 0, 0);
    insert(1, 1, 1);
    insert(2, 4, 2);
    insert(3, 4, 3);
    insert(2, 5, 2);
    insert(5, 4, 0);
    insert(2, 2, 5);
    insert(3, 3, 4);
    insert(2, 2, 4);
    insert(4, 3, 5);
    insert(5, 2, 2);
    insert(4, 3, 3);
    insert(0, 2, 2);
    (set, ids)
}

struct CpuRing<'a>(&'a SuccinctArchive<OrderedUniverse>);

impl RingBatchQuery for CpuRing<'_> {
    fn rank_batch(
        &self,
        rotation: SuccinctRotation,
        positions: &[usize],
        values: &[usize],
    ) -> Vec<usize> {
        positions
            .iter()
            .zip(values)
            .map(|(&position, &value)| self.0.ring_col(rotation).rank(position, value).unwrap())
            .collect()
    }
}

fn assert_pages_equal_eager<'a, C>(
    name: &str,
    constraint: &C,
    variable: VariableId,
    view: &RowsView<'_>,
) where
    C: Constraint<'a> + ?Sized,
{
    assert!(
        constraint.residual_proposal_source_is_paged(variable, view),
        "{name}: repeated source was not admitted",
    );
    let mut eager = Vec::new();
    constraint.propose(variable, view, &mut CandidateSink::Values(&mut eager));

    let limits = [1, 2, 4];
    let mut actual = Vec::new();
    let mut cursor = ResidualDeltaSourceCursor::Start;
    let mut calls = 0usize;
    loop {
        let limit = limits[calls % limits.len()];
        let before = actual.len();
        let mut roots = Vec::new();
        let page = constraint
            .residual_delta_source_page(
                variable,
                view,
                None,
                cursor,
                limit,
                &mut roots,
                &mut actual,
            )
            .unwrap_or_else(|| panic!("{name}: advertised source became unsupported"));
        assert!(roots.is_empty(), "{name}: proposal source invented roots");
        assert!(page.examined <= limit, "{name}: page exceeded demand");
        assert!(
            actual.len() - before <= page.examined,
            "{name}: accepted more values than it examined",
        );
        calls += 1;
        assert!(calls <= 64, "{name}: source failed to terminate");

        let Some(next) = page.next else {
            break;
        };
        assert!(page.examined > 0, "{name}: hidden continuation did no work");
        match (cursor, next) {
            (ResidualDeltaSourceCursor::Start, ResidualDeltaSourceCursor::After(_)) => {}
            (
                ResidualDeltaSourceCursor::After(previous),
                ResidualDeltaSourceCursor::After(next),
            ) => assert!(next > previous, "{name}: cursor failed strict progress"),
            _ => panic!("{name}: source changed cursor families"),
        }
        cursor = next;
    }

    assert_eq!(actual, eager, "{name}: paging changed eager proposal order");
}

#[test]
fn all_repeated_bound_schemas_match_eager_on_cpu_and_ring_backends() {
    let (set, ids) = repeated_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let ring = CpuRing(&archive);
    let x = Variable::<GenId>::new(0);
    let a = Variable::<GenId>::new(1);
    let v = Variable::<UnknownInline>::new(2);
    let e = Variable::<GenId>::new(3);

    for ring_backed in [false, true] {
        let backend = if ring_backed { "ring" } else { "cpu" };
        let check = |name: &str,
                     constraint: SuccinctArchiveConstraint<'_, OrderedUniverse>,
                     vars: &[VariableId],
                     row: &[RawInline]| {
            let view = if vars.is_empty() {
                RowsView::EMPTY
            } else {
                RowsView::new(vars, row)
            };
            assert_pages_equal_eager(&format!("{backend}/{name}"), &constraint, x.index, &view);
        };
        let ev = |attribute| {
            if ring_backed {
                SuccinctArchiveConstraint::with_ring_batch(x, attribute, x, &archive, &ring)
            } else {
                SuccinctArchiveConstraint::new(x, attribute, x, &archive)
            }
        };
        let ea = |value| {
            if ring_backed {
                SuccinctArchiveConstraint::with_ring_batch(x, x, value, &archive, &ring)
            } else {
                SuccinctArchiveConstraint::new(x, x, value, &archive)
            }
        };
        let av = |entity| {
            if ring_backed {
                SuccinctArchiveConstraint::with_ring_batch(entity, x, x, &archive, &ring)
            } else {
                SuccinctArchiveConstraint::new(entity, x, x, &archive)
            }
        };

        check("E=V/free-A", ev(a), &[], &[]);
        check("E=V/bound-A", ev(a), &[a.index], &[raw_id(ids[4])]);
        check("E=A/free-V", ea(v), &[], &[]);
        check("E=A/bound-V", ea(v), &[v.index], &[raw_id(ids[5])]);
        check("A=V/free-E", av(e), &[], &[]);
        check("A=V/bound-E", av(e), &[e.index], &[raw_id(ids[5])]);
        let all = if ring_backed {
            SuccinctArchiveConstraint::with_ring_batch(x, x, x, &archive, &ring)
        } else {
            SuccinctArchiveConstraint::new(x, x, x, &archive)
        };
        check("E=A=V", all, &[], &[]);
    }
}

#[test]
fn invalid_bound_encodings_exhaust_and_invalid_source_modes_are_rejected() {
    let (set, _) = repeated_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    let a = Variable::<GenId>::new(1);
    let v = Variable::<UnknownInline>::new(2);
    let e = Variable::<GenId>::new(3);
    let invalid_id = [0xee; 32];
    let absent_value = [0xdd; 32];

    for (name, constraint, vars, row) in [
        (
            "E=V/invalid-A",
            SuccinctArchiveConstraint::new(x, a, x, &archive),
            vec![a.index],
            vec![invalid_id],
        ),
        (
            "E=A/absent-V",
            SuccinctArchiveConstraint::new(x, x, v, &archive),
            vec![v.index],
            vec![absent_value],
        ),
        (
            "A=V/invalid-E",
            SuccinctArchiveConstraint::new(e, x, x, &archive),
            vec![e.index],
            vec![invalid_id],
        ),
    ] {
        let view = RowsView::new(&vars, &row);
        assert_pages_equal_eager(name, &constraint, x.index, &view);
    }

    let constraint = SuccinctArchiveConstraint::new(x, a, x, &archive);
    let bound_vars = [x.index];
    let bound_row = [raw_id(id(1))];
    let bound_view = RowsView::new(&bound_vars, &bound_row);
    assert!(!constraint.residual_proposal_source_is_paged(x.index, &bound_view));
    assert!(constraint
        .residual_delta_source_page(
            x.index,
            &RowsView::EMPTY,
            Some(&[]),
            ResidualDeltaSourceCursor::Start,
            1,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .is_none());

    let two_rows = [raw_id(id(4)), raw_id(id(5))];
    let a_vars = [a.index];
    let two_row_view = RowsView::new(&a_vars, &two_rows);
    assert!(constraint
        .residual_delta_source_page(
            x.index,
            &two_row_view,
            None,
            ResidualDeltaSourceCursor::Start,
            1,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .is_none());
}

#[derive(Clone, Default)]
struct SourceTrace {
    propose_calls: Arc<AtomicUsize>,
    pages: Arc<Mutex<Vec<(usize, usize, usize)>>>,
}

struct TracedSource<C> {
    inner: C,
    trace: SourceTrace,
}

impl<'a, C> Constraint<'a> for TracedSource<C>
where
    C: Constraint<'a>,
{
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
        self.trace.propose_calls.fetch_add(1, Ordering::Relaxed);
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
        let before = accepted.len();
        let page = self
            .inner
            .residual_delta_source_page(variable, view, candidates, cursor, limit, roots, accepted);
        if let Some(page) = page {
            self.trace
                .pages
                .lock()
                .unwrap()
                .push((limit, page.examined, accepted.len() - before));
        }
        page
    }
}

fn negative_prefix_set(attribute: Id, count: u8, witnesses: impl Fn(u8) -> bool) -> TribleSet {
    let other = id(0xf0);
    let mut set = TribleSet::new();
    for tag in 1..=count {
        let entity = id(tag);
        let target = if witnesses(tag) { entity } else { other };
        set.insert(&Trible::force(&entity, &attribute, &inline_id(target)));
    }
    set
}

fn project_zero(binding: &Binding) -> Option<RawInline> {
    binding.get(0).copied()
}

fn assert_negative_growth<'a, C>(root: C, trace: &SourceTrace, expected: RawInline)
where
    C: Constraint<'a> + 'a,
{
    let root = TracedSource {
        inner: root,
        trace: trace.clone(),
    };
    let mut query = Query::new(root, project_zero)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .growth(2)
        .cap(16);

    assert_eq!(query.next(), Some(expected));
    assert_eq!(
        trace.pages.lock().unwrap().as_slice(),
        [(1, 1, 0), (2, 2, 0), (4, 4, 1)],
    );
    assert_eq!(trace.propose_calls.load(Ordering::Relaxed), 0);
    assert_eq!(query.stats().delta_source_pages, 3);
    assert_eq!(query.stats().delta_source_candidates_examined, 7);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
    drop(query);
    assert_eq!(trace.pages.lock().unwrap().len(), 3);
}

#[test]
fn negative_prefixes_grow_one_two_four_on_middle_and_domain_drivers() {
    let attribute = id(0xa0);
    let set = negative_prefix_set(attribute, 7, |tag| tag == 7);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    let middle_trace = SourceTrace::default();
    assert_negative_growth(
        SuccinctArchiveConstraint::new(x, inline_id(attribute), x, &archive),
        &middle_trace,
        raw_id(id(7)),
    );

    let other = id(0xf0);
    let mut all_same = TribleSet::new();
    for tag in 1..=7 {
        let entity = id(tag);
        let value = if tag == 7 { entity } else { other };
        all_same.insert(&Trible::force(&entity, &entity, &inline_id(value)));
    }
    let all_same_archive: SuccinctArchive<OrderedUniverse> = (&all_same).into();
    let domain_trace = SourceTrace::default();
    assert_negative_growth(
        SuccinctArchiveConstraint::new(x, x, x, &all_same_archive),
        &domain_trace,
        raw_id(id(7)),
    );
}

#[test]
fn clone_drop_and_duplicate_affine_parents_preserve_exact_sets() {
    let attribute = id(0xa1);
    let set = negative_prefix_set(attribute, 8, |tag| tag % 2 == 0);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    let root = Arc::new(SuccinctArchiveConstraint::new(
        x,
        inline_id(attribute),
        x,
        &archive,
    ));
    let mut expected: Vec<_> = Query::new(Arc::clone(&root), project_zero)
        .sequential()
        .collect();
    expected.sort_unstable();

    let mut query = Query::new(root, project_zero)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1);
    let first = query
        .next()
        .expect("fixture has repeated-position witnesses");
    let dropped = query.clone();
    drop(dropped);
    let mirror = query.clone();
    let mut remainder: Vec<_> = query.collect();
    let mut mirrored: Vec<_> = mirror.collect();
    remainder.sort_unstable();
    mirrored.sort_unstable();
    assert_eq!(mirrored, remainder);
    let mut complete = vec![first];
    complete.extend(remainder);
    complete.sort_unstable();
    assert_eq!(complete, expected);

    const PARENT: VariableId = 0;
    const TARGET: VariableId = 1;
    let parent_value = [0x44; 32];
    let parent = Variable::<UnknownInline>::new(PARENT);
    let target = Variable::<GenId>::new(TARGET);
    let make = || {
        IntersectionConstraint::new(vec![
            Box::new(DuplicateDomain {
                variable: parent.index,
                value: parent_value,
            }) as Box<dyn Constraint<'_>>,
            Box::new(SuccinctArchiveConstraint::new(
                target,
                inline_id(attribute),
                target,
                &archive,
            )) as Box<dyn Constraint<'_>>,
        ])
    };
    let project = |binding: &Binding| Some((*binding.get(PARENT)?, *binding.get(TARGET)?));
    let mut sequential: Vec<_> = Query::new(make(), project).sequential().collect();
    let mut residual: Vec<_> = Query::new(make(), project)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .start_width(1)
        .cap(1)
        .collect();
    sequential.sort_unstable();
    residual.sort_unstable();
    assert_eq!(sequential.len(), expected.len());
    assert!(sequential.iter().all(|(value, _)| *value == parent_value));
    assert_eq!(residual, sequential);
}

#[derive(Clone, Copy)]
struct DuplicateDomain {
    variable: VariableId,
    value: RawInline,
}

impl<'a> Constraint<'a> for DuplicateDomain {
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
        out.fill(2, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
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
        if variable == self.variable {
            candidates.retain(|_, value| *value == self.value);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| row[column] == self.value))
    }
}

fn paged_snapshot(archive: &SuccinctArchive<OrderedUniverse>, attribute: Id) -> Vec<RawInline> {
    let x = Variable::<GenId>::new(0);
    let constraint = SuccinctArchiveConstraint::new(x, inline_id(attribute), x, &archive);
    let mut eager = Vec::new();
    constraint.propose(
        x.index,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut eager),
    );
    assert_pages_equal_eager("snapshot/E=V", &constraint, x.index, &RowsView::EMPTY);

    let mut cursor = ResidualDeltaSourceCursor::Start;
    let mut paged = Vec::new();
    loop {
        let page = constraint
            .residual_delta_source_page(
                x.index,
                &RowsView::EMPTY,
                None,
                cursor,
                1,
                &mut Vec::new(),
                &mut paged,
            )
            .unwrap();
        let Some(next) = page.next else {
            break;
        };
        cursor = next;
    }
    assert_eq!(paged, eager);
    paged
}

#[test]
fn archive_growth_only_adds_answers_and_old_snapshot_stays_exact() {
    let attribute = id(0xa2);
    let base = negative_prefix_set(attribute, 3, |tag| tag == 2);
    let base_archive: SuccinctArchive<OrderedUniverse> = (&base).into();
    let before = paged_snapshot(&base_archive, attribute);

    let mut grown = base.clone();
    grown.insert(&Trible::force(&id(4), &attribute, &inline_id(id(4))));
    grown.insert(&Trible::force(&id(5), &attribute, &inline_id(id(0xf0))));
    let grown_archive: SuccinctArchive<OrderedUniverse> = (&grown).into();
    let after = paged_snapshot(&grown_archive, attribute);
    let old_snapshot_again = paged_snapshot(&base_archive, attribute);

    assert_eq!(before, vec![raw_id(id(2))]);
    assert_eq!(old_snapshot_again, before);
    assert_eq!(after, vec![raw_id(id(2)), raw_id(id(4))]);
    assert!(before.iter().all(|value| after.contains(value)));
}
