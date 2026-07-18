use std::collections::{BTreeMap, HashSet};

use jerky::bit_vector::NumBits;
use triblespace_gpu::query_program::{
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

fn boundary_chain_fixture() -> SuccinctArchive<OrderedUniverse> {
    let entities: Vec<_> = (0..65).map(|i| fixture_id(4, i)).collect();
    let attributes: Vec<_> = (0..65).map(|i| fixture_id(5, i)).collect();
    let values: Vec<_> = (0..65).map(|i| fixture_id(6, i)).collect();
    let mut set = TribleSet::new();

    // The hot entity owns every attribute; its first pair owns every value.
    // The remaining entities each add one pair. The resulting stage sizes are
    // deliberately one past successive 64-row scan boundaries:
    // E = 65, E/A = 65 + 64 = 129, E/A/V = 65 + 64 + 64 = 193.
    for &value in &values {
        insert(&mut set, entities[0], attributes[0], value);
    }
    for index in 1..attributes.len() {
        insert(&mut set, entities[0], attributes[index], values[index]);
    }
    for index in 1..entities.len() {
        insert(&mut set, entities[index], attributes[0], values[index]);
    }

    (&set).into()
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

fn forced_eav(
    program: &QueryProgram<'_, OrderedUniverse>,
    [entity, attribute, value]: [ProgramVariable; 3],
    seed_rows: usize,
) -> ProgramFrontier {
    let seed = ProgramFrontier::new(Vec::new(), Vec::new(), seed_rows).unwrap();
    let entities = program.transition_on(entity, &seed).unwrap();
    let pairs = program.transition_on(attribute, &entities).unwrap();
    program.transition_on(value, &pairs).unwrap()
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
fn resident_eav_chain_matches_forced_cpu_for_every_axis_permutation() {
    let (archive, _, _, _) = fixture();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    for permutation in permutations {
        let axes = permutation.map(ProgramVariable::new);
        let program = QueryProgram::compile(
            resident.archive(),
            3,
            [QueryPattern::new(axes[0], axes[1], axes[2])],
        )
        .unwrap();
        let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();

        // Zero seeds exercises the still-exactly-one-read empty path. Two
        // indistinguishable seeds prove that the chain preserves multiplicity
        // instead of globally deduplicating complete rows.
        for seed_rows in [0, 1, 2] {
            assert_eq!(
                gpu.execute_eav(seed_rows).unwrap(),
                forced_eav(&program, axes, seed_rows),
                "axis permutation {permutation:?}, seed rows {seed_rows}",
            );
        }
    }

    // Exercise every row-homomorphism split across the first scan-block
    // boundary. Output equality is exact and ordered, not a sorted-set check.
    let axes = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];
    let program = QueryProgram::compile(
        resident.archive(),
        3,
        [QueryPattern::new(axes[0], axes[1], axes[2])],
    )
    .unwrap();
    let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();
    let whole = gpu.execute_eav(BLOCK + 1).unwrap();
    assert_eq!(whole, forced_eav(&program, axes, BLOCK + 1));
    for split in 0..=BLOCK + 1 {
        let left = gpu.execute_eav(split).unwrap();
        let right = gpu.execute_eav(BLOCK + 1 - split).unwrap();
        assert_eq!(whole, concatenate(&left, &right), "seed split {split}");
    }

    // Cross a different scan boundary at every physical depth, with enough
    // skew to expose accidentally uniform-fanout arithmetic.
    let boundary_resident = WgpuSuccinctArchive::new(boundary_chain_fixture()).unwrap();
    assert_eq!(boundary_resident.archive().entity_count, 65);
    assert_eq!(boundary_resident.archive().changed_e_a.num_ones(), 129);
    let boundary_program = QueryProgram::compile(
        boundary_resident.archive(),
        3,
        [QueryPattern::new(axes[0], axes[1], axes[2])],
    )
    .unwrap();
    let boundary_gpu = WgpuQueryProgram::new(&boundary_program, &boundary_resident).unwrap();
    let boundary_expected = forced_eav(&boundary_program, axes, 1);
    assert_eq!(boundary_expected.len(), 193);
    assert_eq!(boundary_gpu.execute_eav(1).unwrap(), boundary_expected);

    // A truly empty resident archive still returns the complete output schema
    // and performs the method's sole packed final read for both zero and
    // nonzero virtual seed multiplicity.
    let empty_set = TribleSet::new();
    let empty_archive: SuccinctArchive<OrderedUniverse> = (&empty_set).into();
    let empty_resident = WgpuSuccinctArchive::new(empty_archive).unwrap();
    let empty_program = QueryProgram::compile(
        empty_resident.archive(),
        3,
        [QueryPattern::new(axes[0], axes[1], axes[2])],
    )
    .unwrap();
    let empty_gpu = WgpuQueryProgram::new(&empty_program, &empty_resident).unwrap();
    for seed_rows in [0, 1, BLOCK + 1] {
        assert_eq!(
            empty_gpu.execute_eav(seed_rows).unwrap(),
            forced_eav(&empty_program, axes, seed_rows),
        );
    }

    // The general constructor also serves the proven two-bound arm, so some
    // programs it admits are intentionally ineligible for this whole-chain
    // specialization. execute_eav must reject them instead of silently
    // skipping constants.
    let constant_entity = QueryProgram::compile(
        resident.archive(),
        2,
        [QueryPattern::new(
            QueryTerm::Constant(raw(fixture_id(1, 0))),
            ProgramVariable::new(0),
            ProgramVariable::new(1),
        )],
    )
    .unwrap();
    let constant_gpu = WgpuQueryProgram::new(&constant_entity, &resident).unwrap();
    assert!(matches!(
        constant_gpu.execute_eav(1),
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

#[test]
#[ignore = "requires a native WGPU adapter"]
fn budgeted_transition_yields_stable_prefixes_and_lawful_receipts() {
    use triblespace_gpu::budgeted::{BudgetContractError, CohortGrants};

    let (archive, entities, attributes, _values) = fixture();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(e, a, v)]).unwrap();
    let gpu = WgpuQueryProgram::new(&program, &resident).unwrap();
    let rows = BLOCK + 1;
    let parent = frontier(
        &program,
        vec![e, a],
        repeated_ea_rows(&entities, attributes[0], rows),
    );
    let parent_stride = parent.variables().len();

    // Per-parent CPU child blocks: the transition is a row homomorphism, so
    // singleton slices are the exact per-input oracle.
    let child_blocks: Vec<ProgramFrontier> = (0..rows)
        .map(|row| {
            let single = ProgramFrontier::new(
                parent.variables().to_vec(),
                parent.values()[row * parent_stride..(row + 1) * parent_stride].to_vec(),
                1,
            )
            .unwrap();
            program.transition_on(v, &single).unwrap()
        })
        .collect();
    let full_counts: Vec<usize> = child_blocks.iter().map(|block| block.len()).collect();
    assert!(
        full_counts.iter().any(|&count| count == 0),
        "the fixture must exercise a genuinely empty interval under a positive grant"
    );
    assert!(
        full_counts.iter().any(|&count| count > 2),
        "the fixture must exercise a clamped interval"
    );
    let child_variables = child_blocks[0].variables().to_vec();
    let child_stride = child_variables.len();
    let clamped_expectation = |limits: &[usize]| {
        let mut values = Vec::new();
        let mut row_count = 0usize;
        for (block, &limit) in child_blocks.iter().zip(limits) {
            let keep = block.len().min(limit);
            values.extend_from_slice(&block.values()[..keep * child_stride]);
            row_count += keep;
        }
        ProgramFrontier::new(child_variables.clone(), values, row_count).unwrap()
    };

    // Generous grants reproduce the exact whole-frontier transition, with
    // every interval exhausted and no cursor.
    let generous_limits = vec![64usize; rows];
    let generous = CohortGrants::from_task_limits(&generous_limits).unwrap();
    let (child, receipts) = gpu.transition_on_budgeted(v, &parent, &generous).unwrap();
    assert_eq!(child, program.transition_on(v, &parent).unwrap());
    assert_eq!(child, clamped_expectation(&generous_limits));
    assert_eq!(receipts.archive(), resident.identity());
    let receipts = receipts.into_receipts();
    assert_eq!(receipts.len(), rows);
    for (receipt, &full) in receipts.iter().zip(&full_counts) {
        assert_eq!(receipt.examined as usize, full);
        assert_eq!(receipt.produced, receipt.examined);
        assert!(receipt.physical_cursor.is_none());
    }

    // Mixed clamping grants return the stable per-input prefix, exact
    // element-wise receipts, and a physical cursor exactly where the grant
    // was the binding constraint.
    let limits: Vec<usize> = (0..rows).map(|row| (row % 3) + 1).collect();
    let grants = CohortGrants::from_task_limits(&limits).unwrap();
    let (child, receipts) = gpu.transition_on_budgeted(v, &parent, &grants).unwrap();
    assert_eq!(child, clamped_expectation(&limits));
    let receipts = receipts.into_receipts();
    assert_eq!(receipts.len(), rows);
    let mut observed_clamped = 0usize;
    for (receipt, (&full, &limit)) in receipts.into_iter().zip(full_counts.iter().zip(&limits)) {
        let expected_examined = full.min(limit);
        assert_eq!(receipt.examined as usize, expected_examined);
        assert_eq!(receipt.produced, receipt.examined);
        match receipt.physical_cursor {
            Some(cursor) => {
                assert!(full > limit, "a cursor requires a clamped interval");
                observed_clamped += 1;
                assert_eq!(cursor.into_typed_conversion_offset() as usize, expected_examined);
            }
            None => assert!(full <= limit, "an exhausted interval carries no cursor"),
        }
    }
    assert!(observed_clamped > 0, "the fixture must observe real clamping");

    // Grant-shape violations fail closed before any kernel launch.
    let short = CohortGrants::from_task_limits(&vec![1usize; rows - 1]).unwrap();
    assert!(matches!(
        gpu.transition_on_budgeted(v, &parent, &short).unwrap_err(),
        ResidentTransitionError::Budget(BudgetContractError::GrantCountMismatch {
            inputs,
            grants,
        }) if inputs == rows && grants == rows - 1
    ));
    let mut with_zero = vec![1usize; rows];
    with_zero[7] = 0;
    let zeroed = CohortGrants::from_task_limits(&with_zero).unwrap();
    assert!(matches!(
        gpu.transition_on_budgeted(v, &parent, &zeroed).unwrap_err(),
        ResidentTransitionError::Budget(BudgetContractError::ZeroGrant { input: 7 })
    ));
}
