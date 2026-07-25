//! Solver receipts for repeated-position SuccinctArchive constraints.

use std::sync::Arc;

use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctArchiveConstraint,
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

fn snapshot(archive: &SuccinctArchive<OrderedUniverse>, attribute: Id) -> Vec<RawInline> {
    let x = Variable::<GenId>::new(0);
    let constraint = SuccinctArchiveConstraint::new(x, inline_id(attribute), x, &archive);
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
    let before = snapshot(&base_archive, attribute);

    let mut grown = base.clone();
    grown.insert(&Trible::force(&id(4), &attribute, &inline_id(id(4))));
    grown.insert(&Trible::force(&id(5), &attribute, &inline_id(id(0xf0))));
    let grown_archive: SuccinctArchive<OrderedUniverse> = (&grown).into();
    let after = snapshot(&grown_archive, attribute);
    let old_snapshot_again = snapshot(&base_archive, attribute);

    assert_eq!(before, vec![raw_id(id(2))]);
    assert_eq!(old_snapshot_again, before);
    assert_eq!(after, vec![raw_id(id(2)), raw_id(id(4))]);
    assert!(before.iter().all(|value| after.contains(value)));
}
