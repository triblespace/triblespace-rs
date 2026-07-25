//! Receipts for bounded repeated-position typed Programs in SuccinctArchive.
//!
//! Equality misses consume demand exactly like hits, so a negative prefix
//! participates in geometric widening while the Production Program preserves
//! the ordinary repeated-position result set.

use std::sync::Arc;

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, RingBatchQuery, SuccinctArchive, SuccinctArchiveConstraint, SuccinctRotation,
};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, ProposalCoverage, Query, RowsView, Variable,
    VariableId, VariableSet,
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

fn assert_typed_program_family<'a, C>(
    name: &str,
    constraint: &C,
    _variable: VariableId,
    _view: &RowsView<'_>,
) where
    C: Constraint<'a> + ?Sized,
{
    assert!(
        constraint.residual_program().is_some(),
        "{name}: missing typed Program family",
    );
}

#[test]
fn all_repeated_bound_schemas_use_production_program_on_cpu_and_ring_backends() {
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
            assert_typed_program_family(&format!("{backend}/{name}"), &constraint, x.index, &view);
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
fn invalid_bound_encodings_keep_the_typed_program_family() {
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
        assert_typed_program_family(name, &constraint, x.index, &view);
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

fn assert_negative_growth<'a, C>(root: C, expected: RawInline)
where
    C: Constraint<'a> + 'a,
{
    let mut query = Query::new(root, project_zero).solve_residual_state_lazy();

    assert_eq!(query.next(), Some(expected));
    assert_eq!(query.stats().delta_source_pages, 3);
    assert_eq!(query.stats().delta_source_candidates_examined, 7);
    assert_eq!(query.stats().delta_source_direct_candidates, 1);
}

#[test]
fn negative_prefixes_grow_one_two_four_on_middle_and_domain_drivers() {
    let attribute = id(0xa0);
    let set = negative_prefix_set(attribute, 7, |tag| tag == 7);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let x = Variable::<GenId>::new(0);
    assert_negative_growth(
        SuccinctArchiveConstraint::new(x, inline_id(attribute), x, &archive),
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
    assert_negative_growth(
        SuccinctArchiveConstraint::new(x, x, x, &all_same_archive),
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
        .solve_residual_state_lazy()
        .collect();
    expected.sort_unstable();

    let mut query = Query::new(root, project_zero).solve_residual_state_lazy();
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
    let mut residual: Vec<_> = Query::new(make(), project)
        .solve_residual_state_lazy()
        .collect();
    let mut affine_expected: Vec<_> = expected
        .iter()
        .copied()
        .map(|target| (parent_value, target))
        .collect();
    affine_expected.sort_unstable();
    residual.sort_unstable();
    assert_eq!(residual, affine_expected);
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

fn program_snapshot(archive: &SuccinctArchive<OrderedUniverse>, attribute: Id) -> Vec<RawInline> {
    let x = Variable::<GenId>::new(0);
    let constraint = SuccinctArchiveConstraint::new(x, inline_id(attribute), x, &archive);
    assert_typed_program_family("snapshot/E=V", &constraint, x.index, &RowsView::EMPTY);
    let mut values: Vec<_> = Query::new(constraint, project_zero)
        .solve_residual_state_lazy()
        .collect();
    values.sort_unstable();
    values
}

#[test]
fn archive_growth_only_adds_answers_and_old_snapshot_stays_exact() {
    let attribute = id(0xa2);
    let base = negative_prefix_set(attribute, 3, |tag| tag == 2);
    let base_archive: SuccinctArchive<OrderedUniverse> = (&base).into();
    let before = program_snapshot(&base_archive, attribute);

    let mut grown = base.clone();
    grown.insert(&Trible::force(&id(4), &attribute, &inline_id(id(4))));
    grown.insert(&Trible::force(&id(5), &attribute, &inline_id(id(0xf0))));
    let grown_archive: SuccinctArchive<OrderedUniverse> = (&grown).into();
    let after = program_snapshot(&grown_archive, attribute);
    let old_snapshot_again = program_snapshot(&base_archive, attribute);

    assert_eq!(before, vec![raw_id(id(2))]);
    assert_eq!(old_snapshot_again, before);
    assert_eq!(after, vec![raw_id(id(2)), raw_id(id(4))]);
    assert!(before.iter().all(|value| after.contains(value)));
}
