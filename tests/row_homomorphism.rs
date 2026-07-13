//! Direct protocol-law checks for row-wise constraint evaluation.
//!
//! A multi-row call must equal the concatenation of calls over non-empty
//! consecutive partitions, with local candidate row tags remapped into the
//! original block. The same fixture is exercised through both pattern
//! backends so the succinct backend's fused confirmation path is covered.

use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::inline::encodings::{genid::GenId, UnknownInline};
use triblespace::core::inline::RawInline;
use triblespace::core::query::{
    CandidateSink, Candidates, Constraint, EstimateSink, RowsView, TriblePattern, Variable,
    VariableContext,
};
use triblespace::prelude::*;

struct Fixture {
    set: TribleSet,
    entities: [Id; 3],
    attribute: Id,
    values: [RawInline; 3],
}

fn fixture() -> Fixture {
    let entities = [
        Id::new([1; 16]).unwrap(),
        Id::new([2; 16]).unwrap(),
        Id::new([3; 16]).unwrap(),
    ];
    let attribute = Id::new([10; 16]).unwrap();
    let other_attribute = Id::new([20; 16]).unwrap();
    let values = [[1; 32], [2; 32], [3; 32]];

    let mut set = TribleSet::new();
    set.insert(&Trible::force(
        &entities[0],
        &attribute,
        &Inline::<UnknownInline>::new(values[0]),
    ));
    set.insert(&Trible::force(
        &entities[0],
        &attribute,
        &Inline::<UnknownInline>::new(values[1]),
    ));
    set.insert(&Trible::force(
        &entities[1],
        &attribute,
        &Inline::<UnknownInline>::new(values[1]),
    ));
    // Keep the third entity in the archive domain while leaving it with no
    // value for `attribute`; this gives the protocol an empty candidate row.
    set.insert(&Trible::force(
        &entities[2],
        &other_attribute,
        &Inline::<UnknownInline>::new(values[2]),
    ));

    Fixture {
        set,
        entities,
        attribute,
        values,
    }
}

fn assert_row_homomorphism<'a, C: Constraint<'a>>(
    backend: &str,
    constraint: &C,
    entity: Variable<GenId>,
    value: Variable<UnknownInline>,
    fixture: &Fixture,
) {
    let entity_vars = [entity.index];
    let entity_rows: Vec<RawInline> = fixture
        .entities
        .iter()
        .map(|id| GenId::inline_from(*id).raw)
        .collect();
    let entity_view = RowsView::new(&entity_vars, &entity_rows);

    let mut full_estimates = Vec::new();
    assert!(constraint.estimate(
        value.index,
        &entity_view,
        &mut EstimateSink::Column(&mut full_estimates),
    ));
    assert_eq!(
        full_estimates,
        vec![2, 1, 0],
        "{backend}: fixture must exercise wide, singleton, and empty rows"
    );

    let mut full_proposals: Candidates = Vec::new();
    constraint.propose(
        value.index,
        &entity_view,
        &mut CandidateSink::Tagged(&mut full_proposals),
    );
    let mut sorted_proposals = full_proposals.clone();
    sorted_proposals.sort_unstable();
    assert_eq!(
        sorted_proposals,
        vec![
            (0, fixture.values[0]),
            (0, fixture.values[1]),
            (1, fixture.values[1]),
        ],
        "{backend}: unexpected fixture proposals"
    );

    let initial_candidates: Candidates = vec![
        (0, fixture.values[0]),
        (0, fixture.values[2]),
        (1, fixture.values[0]),
        (1, fixture.values[1]),
        (2, fixture.values[1]),
        (2, fixture.values[2]),
    ];
    let mut full_confirmed = initial_candidates.clone();
    constraint.confirm(
        value.index,
        &entity_view,
        &mut CandidateSink::Tagged(&mut full_confirmed),
    );
    assert_eq!(
        full_confirmed,
        vec![(0, fixture.values[0]), (1, fixture.values[1])],
        "{backend}: unexpected fixture confirmation"
    );

    let bound_vars = [entity.index, value.index];
    let bound_rows = vec![
        entity_rows[0],
        fixture.values[0],
        entity_rows[1],
        fixture.values[1],
        entity_rows[2],
        fixture.values[2],
    ];
    let bound_view = RowsView::new(&bound_vars, &bound_rows);
    let full_satisfied = constraint.satisfied(&bound_view);
    assert!(
        !full_satisfied,
        "{backend}: the third fully-bound row is deliberately absent"
    );

    for split in 1..entity_view.len() {
        let partitions = [(0, split), (split, entity_view.len())];

        let mut partitioned_estimates = Vec::new();
        let mut partitioned_proposals: Candidates = Vec::new();
        let mut partitioned_confirmed: Candidates = Vec::new();
        let mut partitioned_satisfied = true;

        for (start, end) in partitions {
            let shard_view = RowsView::new(&entity_vars, &entity_rows[start..end]);

            assert!(constraint.estimate(
                value.index,
                &shard_view,
                &mut EstimateSink::Column(&mut partitioned_estimates),
            ));

            let mut shard_proposals: Candidates = Vec::new();
            constraint.propose(
                value.index,
                &shard_view,
                &mut CandidateSink::Tagged(&mut shard_proposals),
            );
            partitioned_proposals.extend(
                shard_proposals
                    .into_iter()
                    .map(|(row, candidate)| (row + start as u32, candidate)),
            );

            let mut shard_candidates: Candidates = initial_candidates
                .iter()
                .filter(|(row, _)| (*row as usize) >= start && (*row as usize) < end)
                .map(|(row, candidate)| (row - start as u32, *candidate))
                .collect();
            constraint.confirm(
                value.index,
                &shard_view,
                &mut CandidateSink::Tagged(&mut shard_candidates),
            );
            partitioned_confirmed.extend(
                shard_candidates
                    .into_iter()
                    .map(|(row, candidate)| (row + start as u32, candidate)),
            );

            let bound_shard = RowsView::new(
                &bound_vars,
                &bound_rows[start * bound_vars.len()..end * bound_vars.len()],
            );
            partitioned_satisfied &= constraint.satisfied(&bound_shard);
        }

        assert_eq!(
            partitioned_estimates, full_estimates,
            "{backend}: estimate violated row homomorphism at split {split}"
        );
        assert_eq!(
            partitioned_proposals, full_proposals,
            "{backend}: propose violated row homomorphism at split {split}"
        );
        assert_eq!(
            partitioned_confirmed, full_confirmed,
            "{backend}: confirm violated row homomorphism at split {split}"
        );
        assert_eq!(
            partitioned_satisfied, full_satisfied,
            "{backend}: satisfied violated row homomorphism at split {split}"
        );
    }
}

fn check_backend<S: TriblePattern>(backend: &str, store: &S, fixture: &Fixture) {
    let mut context = VariableContext::new();
    let entity = context.next_variable::<GenId>();
    let value = context.next_variable::<UnknownInline>();
    let constraint = store.pattern(entity, GenId::inline_from(fixture.attribute), value);
    assert_row_homomorphism(backend, &constraint, entity, value, fixture);
}

#[test]
fn tribleset_pattern_is_row_homomorphic() {
    let fixture = fixture();
    check_backend("TribleSet", &fixture.set, &fixture);
}

#[test]
fn succinctarchive_pattern_is_row_homomorphic() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    check_backend("SuccinctArchive", &archive, &fixture);
}

#[test]
fn tribleset_adjacent_prefix_run_is_row_homomorphic() {
    let fixture = fixture();
    let mut context = VariableContext::new();
    let entity = context.next_variable::<GenId>();
    let value = context.next_variable::<UnknownInline>();
    let unrelated = context.next_variable::<UnknownInline>();
    let constraint = fixture
        .set
        .pattern(entity, GenId::inline_from(fixture.attribute), value);

    // Three copies of one entity followed by two copies of another. The
    // unrelated column changes on every row, proving that replay is keyed to
    // this pattern's bound prefix rather than accidental whole-row equality.
    // Every split exercises the same semantics; splits 1, 2, and 4 cut through
    // an adjacent prefix run and force one shard to recompute the PATCH answer
    // that the whole block may replay.
    let entity_vars = [entity.index, unrelated.index];
    let entities = [
        fixture.entities[0],
        fixture.entities[0],
        fixture.entities[0],
        fixture.entities[1],
        fixture.entities[1],
    ];
    let entity_rows: Vec<RawInline> = entities
        .into_iter()
        .enumerate()
        .flat_map(|(row, id)| [GenId::inline_from(id).raw, [row as u8 + 20; 32]])
        .collect();
    let view = RowsView::new(&entity_vars, &entity_rows);

    let mut full_estimates = Vec::new();
    assert!(constraint.estimate(
        value.index,
        &view,
        &mut EstimateSink::Column(&mut full_estimates),
    ));

    let mut full_proposals = Candidates::new();
    constraint.propose(
        value.index,
        &view,
        &mut CandidateSink::Tagged(&mut full_proposals),
    );

    let mut initial = Candidates::new();
    for row in 0..view.len() as u32 {
        initial.extend(
            fixture
                .values
                .iter()
                .copied()
                .map(|candidate| (row, candidate)),
        );
    }
    let mut full_confirmed = initial.clone();
    constraint.confirm(
        value.index,
        &view,
        &mut CandidateSink::Tagged(&mut full_confirmed),
    );

    for split in 1..view.len() {
        let mut split_estimates = Vec::new();
        let mut split_proposals = Candidates::new();
        let mut split_confirmed = Candidates::new();

        for (start, end) in [(0, split), (split, view.len())] {
            let shard = RowsView::new(
                &entity_vars,
                &entity_rows[start * entity_vars.len()..end * entity_vars.len()],
            );
            assert!(constraint.estimate(
                value.index,
                &shard,
                &mut EstimateSink::Column(&mut split_estimates),
            ));

            let mut proposals = Candidates::new();
            constraint.propose(
                value.index,
                &shard,
                &mut CandidateSink::Tagged(&mut proposals),
            );
            split_proposals.extend(
                proposals
                    .into_iter()
                    .map(|(row, candidate)| (row + start as u32, candidate)),
            );

            let mut confirmed: Candidates = initial
                .iter()
                .filter(|(row, _)| (*row as usize) >= start && (*row as usize) < end)
                .map(|(row, candidate)| (row - start as u32, *candidate))
                .collect();
            constraint.confirm(
                value.index,
                &shard,
                &mut CandidateSink::Tagged(&mut confirmed),
            );
            split_confirmed.extend(
                confirmed
                    .into_iter()
                    .map(|(row, candidate)| (row + start as u32, candidate)),
            );
        }

        assert_eq!(split_estimates, full_estimates, "estimate split {split}");
        assert_eq!(split_proposals, full_proposals, "propose split {split}");
        assert_eq!(split_confirmed, full_confirmed, "confirm split {split}");
    }
}
