use std::collections::{BTreeMap, HashSet};

use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ArchiveCode, ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryProgramError,
    QueryTerm,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::RawInline;
use triblespace_core::prelude::*;
use triblespace_gpu::{ResidentTransitionError, WgpuQueryProgram, WgpuSuccinctArchive};

const BLOCK: usize = 64;

fn fixture_id(prefix: u8, ordinal: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    raw[8..].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
}

fn raw(id: Id) -> RawInline {
    GenId::inline_from(id).raw
}

fn insert(set: &mut TribleSet, entity: Id, attribute: Id, value: Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    ));
}

fn fixture() -> (SuccinctArchive<OrderedUniverse>, Vec<Id>, [Id; 2], Vec<Id>) {
    let entities: Vec<_> = (0..70).map(|i| fixture_id(1, i)).collect();
    let attributes = [fixture_id(2, 0), fixture_id(2, 1)];
    let values: Vec<_> = (0..8).map(|i| fixture_id(3, i)).collect();
    let mut set = TribleSet::new();

    for (row, &entity) in entities.iter().enumerate() {
        // A unit-fanout arm makes output geometry land exactly at B-1/B/B+1.
        insert(&mut set, entity, attributes[1], values[row % values.len()]);

        // The other arm exercises zero through five candidates per parent.
        let fanout = row % 6;
        for &value in values.iter().take(fanout) {
            insert(&mut set, entity, attributes[0], value);
        }
    }

    ((&set).into(), entities, attributes, values)
}

fn frontier(
    program: &QueryProgram<'_, OrderedUniverse>,
    variables: Vec<ProgramVariable>,
    rows: impl IntoIterator<Item = Vec<Id>>,
) -> ProgramFrontier {
    let rows: Vec<Vec<Id>> = rows.into_iter().collect();
    let values = rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|id| {
                program
                    .encode(&raw(*id))
                    .expect("fixture value is in domain")
            })
        })
        .collect();
    ProgramFrontier::new(variables, values, rows.len()).expect("fixture frontier is canonical")
}

fn repeated_ea_rows(entities: &[Id], attribute: Id, count: usize) -> Vec<Vec<Id>> {
    (0..count)
        .map(|row| {
            // Repeat parents deliberately; the transition is a multiset row
            // homomorphism and must not globally deduplicate them.
            let entity = entities[(row * 17 + row / 3) % entities.len()];
            vec![entity, attribute]
        })
        .collect()
}

fn concatenate(left: &ProgramFrontier, right: &ProgramFrontier) -> ProgramFrontier {
    assert_eq!(left.variables(), right.variables());
    let mut values = left.values().to_vec();
    values.extend_from_slice(right.values());
    ProgramFrontier::new(left.variables().to_vec(), values, left.len() + right.len()).unwrap()
}

fn assert_exact_parity(
    resident_program: &WgpuQueryProgram<'_, '_, OrderedUniverse>,
    program: &QueryProgram<'_, OrderedUniverse>,
    target: ProgramVariable,
    parent: &ProgramFrontier,
) -> ProgramFrontier {
    let expected = program.transition_on(target, parent).unwrap();
    let actual = resident_program.transition_on(target, parent).unwrap();
    assert_eq!(actual, expected);
    actual
}

fn resident_values_for_pair(
    archive: SuccinctArchive<OrderedUniverse>,
    entity: Id,
    attribute: Id,
) -> Vec<RawInline> {
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(e, a, v)]).unwrap();
    let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();
    let parent = frontier(&program, vec![e, a], [vec![entity, attribute]]);
    program
        .decode_frontier(&gpu.transition_on(v, &parent).unwrap())
        .unwrap()
        .into_iter()
        .map(|row| row[2])
        .collect()
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn resident_two_bound_transition_preserves_exact_cpu_order_and_capacity_contract() {
    let (archive, entities, attributes, values) = fixture();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(e, a, v)]).unwrap();
    let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();
    assert_eq!(gpu.max_ea_fanout(), 5);

    // Empty, singleton, and both sides of the resident scan block boundary.
    for rows in [0, 1, BLOCK - 1, BLOCK, BLOCK + 1] {
        let parent = frontier(
            &program,
            vec![e, a],
            repeated_ea_rows(&entities, attributes[1], rows),
        );
        let actual = assert_exact_parity(&gpu, &program, v, &parent);
        assert_eq!(actual.len(), rows, "unit fanout at {rows} parent rows");
    }

    // Exact row homomorphism at every edge around the block split. This is an
    // order assertion, not a sorted-set comparison.
    let parent = frontier(
        &program,
        vec![e, a],
        repeated_ea_rows(&entities, attributes[1], BLOCK + 1),
    );
    let whole = gpu.transition_on(v, &parent).unwrap();
    for split in 0..=parent.len() {
        let left = gpu
            .transition_on(v, &parent.slice(0..split).unwrap())
            .unwrap();
        let right = gpu
            .transition_on(v, &parent.slice(split..parent.len()).unwrap())
            .unwrap();
        assert_eq!(whole, concatenate(&left, &right), "split at {split}");
    }

    // Variable insertion is checked at the beginning, middle, and end of the
    // canonical child row, with duplicate parents and non-unit fanout.
    for target_index in 0..3 {
        let target = ProgramVariable::new(target_index);
        let (entity, attribute, parent_variables) = match target_index {
            0 => (
                ProgramVariable::new(1),
                ProgramVariable::new(2),
                vec![ProgramVariable::new(1), ProgramVariable::new(2)],
            ),
            1 => (
                ProgramVariable::new(0),
                ProgramVariable::new(2),
                vec![ProgramVariable::new(0), ProgramVariable::new(2)],
            ),
            2 => (
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                vec![ProgramVariable::new(0), ProgramVariable::new(1)],
            ),
            _ => unreachable!(),
        };
        let insertion_program = QueryProgram::compile(
            resident.archive(),
            3,
            [QueryPattern::new(entity, attribute, target)],
        )
        .unwrap();
        let insertion_gpu = WgpuQueryProgram::new(&insertion_program, &resident).unwrap();
        let rows = vec![
            vec![entities[5], attributes[0]],
            vec![entities[5], attributes[0]],
            vec![entities[2], attributes[0]],
            vec![entities[0], attributes[0]],
        ];
        let parent = frontier(&insertion_program, parent_variables, rows);
        assert_exact_parity(&insertion_gpu, &insertion_program, target, &parent);
    }

    // In a canonical TribleSet, AEV contains each (A,E,V) tuple once, so the
    // CPU arm's `.unique()` is a no-op. Prove that invariant per duplicate
    // parent rather than accidentally relying on a global deduplication.
    let multi = frontier(
        &program,
        vec![e, a],
        vec![
            vec![entities[5], attributes[0]],
            vec![entities[5], attributes[0]],
        ],
    );
    let children = assert_exact_parity(&gpu, &program, v, &multi);
    assert_eq!(children.len(), 10);
    for row_group in children
        .values()
        .chunks_exact(3)
        .collect::<Vec<_>>()
        .chunks(5)
    {
        let distinct: HashSet<ArchiveCode> = row_group.iter().map(|row| row[2]).collect();
        assert_eq!(distinct.len(), 5);
    }

    // The exact output size succeeds, while one word-short capacity reports
    // the exact required row count after the same sole final readback.
    let varying = frontier(
        &program,
        vec![e, a],
        repeated_ea_rows(&entities, attributes[0], BLOCK + 1),
    );
    let expected = program.transition_on(v, &varying).unwrap();
    assert!(expected.len() > 1);
    assert_eq!(
        gpu.transition_on_with_capacity(v, &varying, expected.len())
            .unwrap(),
        expected
    );
    match gpu.transition_on_with_capacity(v, &varying, expected.len() - 1) {
        Err(ResidentTransitionError::OutputCapacityExceeded { required, supplied }) => {
            assert_eq!(required, expected.len());
            assert_eq!(supplied, expected.len() - 1);
        }
        other => panic!("one-short capacity did not fail closed: {other:?}"),
    }
    match gpu.transition_on_with_capacity(v, &varying, 0) {
        Err(ResidentTransitionError::OutputCapacityExceeded { required, supplied }) => {
            assert_eq!(required, expected.len());
            assert_eq!(supplied, 0);
        }
        other => panic!("zero capacity did not fail closed: {other:?}"),
    }

    // Domain-valid values on the wrong E/A axes navigate to empty ranges;
    // defensive select/range guards must not manufacture candidates.
    let wrong_axis = frontier(
        &program,
        vec![e, a],
        [vec![values[7], attributes[0]], vec![entities[5], values[7]]],
    );
    assert_exact_parity(&gpu, &program, v, &wrong_axis);

    // Constants use the same two-bound formulas, including zero-width parent
    // rows and duplicate virtual parents.
    let constant_program = QueryProgram::compile(
        resident.archive(),
        1,
        [QueryPattern::new(
            QueryTerm::Constant(raw(entities[5])),
            QueryTerm::Constant(raw(attributes[0])),
            ProgramVariable::new(0),
        )],
    )
    .unwrap();
    let constant_gpu = WgpuQueryProgram::new(&constant_program, &resident).unwrap();
    let virtual_parents = ProgramFrontier::new(Vec::new(), Vec::new(), 3).unwrap();
    assert_exact_parity(
        &constant_gpu,
        &constant_program,
        ProgramVariable::new(0),
        &virtual_parents,
    );

    let missing_program = QueryProgram::compile(
        resident.archive(),
        1,
        [QueryPattern::new(
            QueryTerm::Constant(raw(fixture_id(9, 9))),
            QueryTerm::Constant(raw(attributes[0])),
            ProgramVariable::new(0),
        )],
    )
    .unwrap();
    let missing_gpu = WgpuQueryProgram::new(&missing_program, &resident).unwrap();
    assert_exact_parity(
        &missing_gpu,
        &missing_program,
        ProgramVariable::new(0),
        &ProgramFrontier::seed(),
    );

    // Compact codes are archive-local. Even byte-identical archive clones are
    // rejected unless the program borrows this exact resident snapshot.
    let detached = resident.archive().clone();
    let detached_program =
        QueryProgram::compile(&detached, 3, [QueryPattern::new(e, a, v)]).unwrap();
    assert!(matches!(
        WgpuQueryProgram::new(&detached_program, &resident),
        Err(ResidentTransitionError::ArchiveMismatch)
    ));

    // Admission is deliberately narrow: no unrelated pattern may be skipped,
    // the kernel never chooses a variable, and both E/A peers must be bound.
    let multi_pattern = QueryProgram::compile(
        resident.archive(),
        3,
        [QueryPattern::new(e, a, v), QueryPattern::new(e, a, v)],
    )
    .unwrap();
    assert!(matches!(
        WgpuQueryProgram::new(&multi_pattern, &resident),
        Err(ResidentTransitionError::UnsupportedArm(_))
    ));

    let no_patterns =
        QueryProgram::compile(resident.archive(), 0, std::iter::empty::<QueryPattern>()).unwrap();
    assert!(matches!(
        WgpuQueryProgram::new(&no_patterns, &resident),
        Err(ResidentTransitionError::UnsupportedArm(_))
    ));

    let constant_value = QueryProgram::compile(
        resident.archive(),
        2,
        [QueryPattern::new(
            ProgramVariable::new(0),
            ProgramVariable::new(1),
            QueryTerm::Constant(raw(values[0])),
        )],
    )
    .unwrap();
    assert!(matches!(
        WgpuQueryProgram::new(&constant_value, &resident),
        Err(ResidentTransitionError::UnsupportedArm(_))
    ));

    let complete_row = frontier(
        &program,
        vec![e, a, v],
        [vec![entities[5], attributes[0], values[0]]],
    );
    let non_target_parent = ProgramFrontier::new(
        vec![a, v],
        vec![complete_row.values()[1], complete_row.values()[2]],
        1,
    )
    .unwrap();
    assert!(matches!(
        gpu.transition_on(e, &non_target_parent),
        Err(ResidentTransitionError::UnsupportedArm(_))
    ));
    assert!(matches!(
        gpu.transition_on(v, &complete_row),
        Err(ResidentTransitionError::Program(
            QueryProgramError::VariableAlreadyBound(bound)
        )) if bound == v
    ));

    let peer_unbound = ProgramFrontier::new(vec![e], vec![complete_row.values()[0]], 1).unwrap();
    assert!(matches!(
        gpu.transition_on(v, &peer_unbound),
        Err(ResidentTransitionError::UnsupportedArm(_))
    ));
}

#[test]
#[ignore = "requires a native WGPU adapter"]
fn resident_archive_extension_is_monotone_in_decoded_value_space() {
    let entity = fixture_id(4, 0);
    let attribute = fixture_id(5, 0);
    // Insert a lexicographically earlier value into the extended snapshot so
    // the old value's OrderedUniverse code shifts. Comparing decoded values is
    // therefore essential rather than merely stylistic.
    let old_value = fixture_id(6, 1);
    let new_value = fixture_id(6, 0);
    let mut base = TribleSet::new();
    insert(&mut base, entity, attribute, old_value);
    let mut extended = base.clone();
    insert(&mut extended, entity, attribute, new_value);

    // Compare codes only to prove the fixture remaps them; semantic inclusion
    // is checked after decoding into the stable raw value space.
    let base_archive: SuccinctArchive<OrderedUniverse> = (&base).into();
    let extended_archive: SuccinctArchive<OrderedUniverse> = (&extended).into();
    let code_for_old_value = |archive: &SuccinctArchive<OrderedUniverse>| {
        QueryProgram::compile(archive, 0, std::iter::empty::<QueryPattern>())
            .unwrap()
            .encode(&raw(old_value))
            .unwrap()
            .get()
    };
    assert_ne!(
        code_for_old_value(&base_archive),
        code_for_old_value(&extended_archive),
        "the fixture must actually remap an existing archive-local code"
    );
    let before = resident_values_for_pair(base_archive, entity, attribute);
    let after = resident_values_for_pair(extended_archive, entity, attribute);
    let histogram = |values: Vec<RawInline>| {
        let mut counts = BTreeMap::new();
        for value in values {
            *counts.entry(value).or_insert(0usize) += 1;
        }
        counts
    };
    let before = histogram(before);
    let after = histogram(after);
    assert!(before
        .iter()
        .all(|(value, count)| after.get(value).copied().unwrap_or(0) >= *count));
    assert!(after.values().sum::<usize>() > before.values().sum());
}
