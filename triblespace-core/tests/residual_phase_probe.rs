//! Focused receipts for the opt-in residual phase probe.

use std::collections::HashSet;
use std::sync::Arc;

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::debug::query::RESIDUAL_PHASE_PROBE_QUERY_LIMIT;
use triblespace_core::debug::query::{arm_residual_phase_probe, take_residual_phase_probe};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::constantconstraint::ConstantConstraint;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::ResidualLowering;
use triblespace_core::query::TriblePattern;
use triblespace_core::query::{Binding, Constraint, ContainsConstraint, Query, Variable};
use triblespace_core::repo::index_home::UnionArchive;
use triblespace_core::trible::{Trible, TribleSet};

fn id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are nonzero")
}

fn run_intersection<'a>(
    archive: &'a UnionArchive<'a, OrderedUniverse>,
    allowed: Arc<HashSet<Id>>,
    attribute: Inline<GenId>,
    value: Inline<UnknownInline>,
) -> Vec<RawInline> {
    let entity = Variable::<GenId>::new(0);
    let root: IntersectionConstraint<Box<dyn Constraint<'a>>> = IntersectionConstraint::new(vec![
        Box::new(allowed.has(entity)),
        Box::new(archive.pattern(entity, attribute, value)),
    ]);
    Query::new(root, move |binding: &Binding| {
        binding.get(entity.index).copied()
    })
    .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
    .collect()
}

#[test]
fn probe_separates_set_to_archive_and_archive_to_set_work() {
    let attribute = id(32);
    let attribute_inline: Inline<GenId> = attribute.to_inline();
    let value = Inline::<UnknownInline>::new([99; 32]);
    let entities: Vec<_> = (1..=8).map(id).collect();
    let mut tribles = TribleSet::new();
    for entity in &entities {
        tribles.insert(&Trible::force(entity, &attribute, &value));
    }
    let segments: [SuccinctArchive<OrderedUniverse>; 1] = [(&tribles).into()];
    let archive = UnionArchive::new(&segments);

    arm_residual_phase_probe();

    // Two set values beat the archive's eight-value fiber: Set proposes and
    // SuccinctArchive confirms.
    let narrow: Arc<HashSet<_>> = Arc::new(entities[..2].iter().copied().collect());
    let mut narrow_results = run_intersection(&archive, narrow, attribute_inline, value);
    narrow_results.sort_unstable();

    // Ten set values lose to the archive's eight-value fiber: the archive
    // proposes and the set confirms, including two absent set members.
    let mut wide: HashSet<_> = entities.iter().copied().collect();
    wide.insert(id(9));
    wide.insert(id(10));
    let mut wide_results = run_intersection(&archive, Arc::new(wide), attribute_inline, value);
    wide_results.sort_unstable();

    let snapshot = take_residual_phase_probe();
    assert_eq!(narrow_results.len(), 2);
    assert_eq!(wide_results.len(), 8);
    assert_eq!(snapshot.dropped_queries, 0);
    assert_eq!(snapshot.queries.len(), 2, "{snapshot:#?}");

    let set_to_archive = &snapshot.queries[0];
    assert!(set_to_archive.completed);
    assert_eq!(set_to_archive.variables, 1);
    assert_eq!(set_to_archive.hashset_source.examined, 2);
    assert_eq!(set_to_archive.hashset_source.output, 2);
    assert_eq!(set_to_archive.hashset_confirm.input, 0);
    assert_eq!(set_to_archive.succinct_source.output, 0);
    assert_eq!(set_to_archive.union_archive_source.output, 0);
    assert_eq!(set_to_archive.union_archive_confirm.input, 2);
    assert_eq!(set_to_archive.union_archive_confirm.output, 2);
    assert_eq!(set_to_archive.succinct_confirm.input, 2);
    assert_eq!(set_to_archive.succinct_confirm.output, 2);
    assert_eq!(set_to_archive.succinct_confirm.domain_searches, 2);
    assert_eq!(set_to_archive.terminal.projected_rows, 2);

    let archive_to_set = &snapshot.queries[1];
    assert!(archive_to_set.completed);
    assert_eq!(archive_to_set.variables, 1);
    assert_eq!(archive_to_set.hashset_source.output, 0);
    assert_eq!(archive_to_set.succinct_confirm.input, 0);
    assert_eq!(archive_to_set.succinct_source.output, 8);
    assert_eq!(archive_to_set.union_archive_source.output, 8);
    assert_eq!(archive_to_set.union_archive_confirm.input, 0);
    assert_eq!(archive_to_set.hashset_confirm.input, 8);
    assert_eq!(archive_to_set.hashset_confirm.output, 8);
    assert_eq!(archive_to_set.terminal.projected_rows, 8);

    assert_eq!(set_to_archive.set_admission.tail_calls, 1);
    assert_eq!(set_to_archive.set_admission.inline_input, 2);
    assert_eq!(set_to_archive.set_admission.inline_output, 2);
    assert_eq!(set_to_archive.set_admission.hashset_inline.input, 2);
    assert_eq!(set_to_archive.set_admission.union_archive_inline.input, 0);
    assert_eq!(archive_to_set.set_admission.inline_input, 8);
    assert_eq!(archive_to_set.set_admission.pageable_input, 0);
    assert_eq!(archive_to_set.set_admission.hashset_inline.input, 0);
    assert_eq!(archive_to_set.set_admission.union_archive_inline.input, 8);
}

#[test]
fn probe_is_off_by_default_and_take_disarms_it() {
    assert!(take_residual_phase_probe().queries.is_empty());
    arm_residual_phase_probe();
    assert!(take_residual_phase_probe().queries.is_empty());
    assert!(take_residual_phase_probe().queries.is_empty());
}

#[test]
fn probe_marks_a_consumer_truncated_solve_incomplete() {
    let entity = Variable::<GenId>::new(0);
    let allowed: Arc<HashSet<_>> = Arc::new([id(1), id(2)].into_iter().collect());

    arm_residual_phase_probe();
    let mut query = Query::new(allowed.has(entity), move |binding: &Binding| {
        binding.get(entity.index).copied()
    })
    .solve_residual_state_lazy_with(ResidualLowering::HYBRID);
    assert!(query.next().is_some());

    let snapshot = take_residual_phase_probe();
    assert_eq!(snapshot.queries.len(), 1);
    assert!(!snapshot.queries[0].completed);
    assert_eq!(snapshot.queries[0].total_wall, std::time::Duration::ZERO);
}

#[test]
fn probe_retains_a_fixed_number_of_query_records() {
    arm_residual_phase_probe();
    for _ in 0..RESIDUAL_PHASE_PROBE_QUERY_LIMIT + 2 {
        let entity = Variable::<GenId>::new(0);
        let constant: Inline<GenId> = id(1).to_inline();
        let root = ConstantConstraint::new(entity, constant);
        let _: Vec<_> = Query::new(root, move |binding: &Binding| {
            binding.get(entity.index).copied()
        })
        .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
        .collect();
    }
    let snapshot = take_residual_phase_probe();
    assert_eq!(snapshot.queries.len(), RESIDUAL_PHASE_PROBE_QUERY_LIMIT);
    assert_eq!(snapshot.dropped_queries, 2);
}
