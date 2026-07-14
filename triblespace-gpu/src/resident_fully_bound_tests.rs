use super::*;

use std::collections::HashSet;
use std::sync::Arc;

use triblespace_core::blob::encodings::succinctarchive::query_program::{QueryPattern, QueryTerm};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::trible::{Trible, TribleSet};

fn ordered_id(prefix: u8) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    Id::new(raw).expect("fixture IDs are non-zero")
}

fn role_trible(entity: Id, attribute: Id, value: Id) -> Trible {
    Trible::new::<GenId>(
        ExclusiveId::force_ref(&entity),
        &attribute,
        &GenId::inline_from(value),
    )
}

#[derive(Clone)]
struct FixtureIds {
    entities: [Id; 4],
    attributes: [Id; 3],
    values: [Id; 3],
}

fn fixture_set() -> (TribleSet, FixtureIds) {
    let ids = FixtureIds {
        entities: [ordered_id(1), ordered_id(2), ordered_id(3), ordered_id(4)],
        attributes: [ordered_id(20), ordered_id(21), ordered_id(22)],
        values: [ordered_id(40), ordered_id(41), ordered_id(42)],
    };
    let [e1, e2, e3, e4] = ids.entities;
    let [a1, a2, a3] = ids.attributes;
    let [x, y, z] = ids.values;
    let mut set = TribleSet::new();
    for trible in [
        role_trible(e1, a1, x),
        role_trible(e1, a1, y),
        role_trible(e1, a1, z),
        role_trible(e1, a2, x),
        role_trible(e1, a3, x),
        role_trible(e2, a1, x),
        role_trible(e2, a2, y),
        role_trible(e3, a2, x),
        role_trible(e3, a2, y),
        role_trible(e3, a3, z),
        role_trible(e4, a3, x),
    ] {
        set.insert(&trible);
    }
    (set, ids)
}

fn v(index: u8) -> ProgramVariable {
    ProgramVariable::new(index)
}

fn constant(id: Id) -> QueryTerm {
    QueryTerm::Constant(GenId::inline_from(id).raw)
}

fn code(program: &QueryProgram<'_, OrderedUniverse>, id: Id) -> u32 {
    program
        .encode(&GenId::inline_from(id).raw)
        .expect("fixture value belongs to the archive domain")
        .get()
}

fn run_viability(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
) -> Vec<u32> {
    let resident = round.upload_frontier(frontier).unwrap();
    let inputs = round.initialize_inputs(&resident).unwrap();
    inputs.read_producer_outputs().0
}

fn complete_rows(program: &QueryProgram<'_, OrderedUniverse>) -> HashSet<Vec<u32>> {
    let complete = program.execute().unwrap();
    complete
        .values()
        .chunks(complete.variables().len())
        .map(|row| row.iter().map(|code| code.get()).collect())
        .collect()
}

#[test]
fn every_constant_column_source_shape_including_constant_seed_is_exact() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let axis_ids = [ids.entities[0], ids.attributes[0], ids.values[0]];

    for column_mask in 0u8..8 {
        let mut next_variable = 0u8;
        let mut terms = Vec::with_capacity(3);
        let mut bound = Vec::new();
        let mut row = Vec::new();
        for (axis, &id) in axis_ids.iter().enumerate() {
            if column_mask & (1 << axis) != 0 {
                let variable = v(next_variable);
                next_variable += 1;
                terms.push(QueryTerm::Variable(variable));
                bound.push(variable);
            } else {
                terms.push(constant(id));
            }
        }
        let program = QueryProgram::compile(
            resident.archive(),
            bound.len(),
            [QueryPattern::new(terms[0], terms[1], terms[2])],
        )
        .unwrap();
        for (axis, &id) in axis_ids.iter().enumerate() {
            if column_mask & (1 << axis) != 0 {
                row.push(code(&program, id));
            }
        }
        let round = WgpuResidentRound::new(&resident, &program, &bound).unwrap();
        assert_eq!(round.plan.fully_bound_supports().len(), 1);
        assert_eq!(round.fully_bound_group.as_ref().unwrap().support_count, 1);
        let support = round.plan.fully_bound_supports()[0];
        for (axis, source) in [support.entity(), support.attribute(), support.value()]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                matches!(source, CodeSource::Column(_)),
                column_mask & (1 << axis) != 0,
                "source mask {column_mask:03b}, axis {axis}"
            );
        }
        let frontier = program
            .frontier_from_indices(bound, row, 1)
            .expect("shape row belongs to this exact snapshot");
        assert_eq!(run_viability(&round, &frontier), vec![1]);
    }
}

#[test]
fn constant_only_seed_support_distinguishes_exact_triples_from_present_pairs() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let [e1, e2, ..] = ids.entities;
    let [a1, a2, ..] = ids.attributes;
    let [x, y, ..] = ids.values;

    for (pattern, expected) in [
        (
            QueryPattern::new(constant(e1), constant(a1), constant(x)),
            1,
        ),
        // e2-a1, e2-y, and a1-y all occur somewhere, but this exact
        // conjunction does not.
        (
            QueryPattern::new(constant(e2), constant(a1), constant(y)),
            0,
        ),
        (
            QueryPattern::new(constant(e2), constant(a2), constant(y)),
            1,
        ),
    ] {
        let program = QueryProgram::compile(resident.archive(), 0, [pattern]).unwrap();
        let round = WgpuResidentRound::new(&resident, &program, &[]).unwrap();
        assert_eq!(
            run_viability(&round, &ProgramFrontier::seed()),
            vec![expected]
        );
        let empty = ProgramFrontier::new(Vec::new(), Vec::new(), 0).unwrap();
        assert!(run_viability(&round, &empty).is_empty());
    }
}

#[test]
fn exact_support_matches_cpu_execute_over_present_and_absent_triples() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let cpu = complete_rows(&program);
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    for entity in ids.entities.map(|id| code(&program, id)) {
        for attribute in ids.attributes.map(|id| code(&program, id)) {
            for value in ids.values.map(|id| code(&program, id)) {
                let row = vec![entity, attribute, value];
                expected.push(u32::from(cpu.contains(&row)));
                rows.extend(row);
            }
        }
    }
    assert!(expected.contains(&0));
    assert!(expected.contains(&1));
    let frontier = program
        .frontier_from_indices(vec![v(0), v(1), v(2)], rows, expected.len())
        .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, frontier.variables()).unwrap();
    assert_eq!(run_viability(&round, &frontier), expected);
}

fn conjunction_program<'a>(
    archive: &'a SuccinctArchive<OrderedUniverse>,
    ids: &FixtureIds,
    permuted: bool,
) -> QueryProgram<'a, OrderedUniverse> {
    let first = QueryPattern::new(v(0), constant(ids.attributes[0]), v(1));
    let second = QueryPattern::new(v(0), constant(ids.attributes[1]), v(2));
    let patterns = if permuted {
        [second, first, first]
    } else {
        [first, first, second]
    };
    QueryProgram::compile(archive, 3, patterns).unwrap()
}

fn heterogeneous_conjunction_frontier(
    program: &QueryProgram<'_, OrderedUniverse>,
    ids: &FixtureIds,
    rows: usize,
) -> ProgramFrontier {
    let representatives = [
        (ids.entities[0], ids.values[0], ids.values[0]),
        (ids.entities[0], ids.values[1], ids.values[0]),
        (ids.entities[1], ids.values[0], ids.values[1]),
        (ids.entities[1], ids.values[1], ids.values[1]),
        (ids.entities[2], ids.values[0], ids.values[1]),
        (ids.entities[2], ids.values[1], ids.values[2]),
    ];
    let mut values = Vec::with_capacity(rows * 3);
    for row in 0..rows {
        let (entity, first_value, second_value) = representatives[row % representatives.len()];
        values.extend([
            code(program, entity),
            code(program, first_value),
            code(program, second_value),
        ]);
    }
    program
        .frontier_from_indices(vec![v(0), v(1), v(2)], values, rows)
        .unwrap()
}

#[test]
fn duplicate_permuted_supports_conjoin_without_cross_row_races_at_every_split() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program = conjunction_program(resident.archive(), &ids, false);
    let permuted = conjunction_program(resident.archive(), &ids, true);
    let frontier = heterogeneous_conjunction_frontier(&program, &ids, 65);
    let permuted_frontier = heterogeneous_conjunction_frontier(&permuted, &ids, 65);
    let cpu = complete_rows(&program);
    let expected = frontier
        .values()
        .chunks(3)
        .map(|row| {
            let row = row.iter().map(|code| code.get()).collect::<Vec<_>>();
            u32::from(cpu.contains(&row))
        })
        .collect::<Vec<_>>();
    assert!(expected.contains(&0));
    assert!(expected.contains(&1));

    let round = WgpuResidentRound::new(&resident, &program, frontier.variables()).unwrap();
    let permuted_round =
        WgpuResidentRound::new(&resident, &permuted, permuted_frontier.variables()).unwrap();
    assert_eq!(round.plan.fully_bound_supports().len(), 3);
    assert_eq!(run_viability(&round, &frontier), expected);
    assert_eq!(run_viability(&permuted_round, &permuted_frontier), expected);

    for split in 0..=65 {
        let left = frontier.slice(0..split).unwrap();
        let right = frontier.slice(split..65).unwrap();
        let mut actual = run_viability(&round, &left);
        actual.extend(run_viability(&round, &right));
        assert_eq!(actual, expected, "split {split}");
    }

    for rows in [0usize, 1, 63, 64, 65] {
        let frontier = heterogeneous_conjunction_frontier(&program, &ids, rows);
        let expected = frontier
            .values()
            .chunks(3)
            .map(|row| {
                let row = row.iter().map(|code| code.get()).collect::<Vec<_>>();
                u32::from(cpu.contains(&row))
            })
            .collect::<Vec<_>>();
        assert_eq!(run_viability(&round, &frontier), expected, "rows={rows}");
    }
}

#[test]
fn fully_bound_viability_matches_cpu_transition_row_filtering() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let support = QueryPattern::new(v(0), constant(ids.attributes[0]), v(1));
    let proposal = QueryPattern::new(v(0), constant(ids.attributes[1]), v(2));
    let program = QueryProgram::compile(resident.archive(), 3, [support, proposal]).unwrap();
    let parent_ids = [
        (ids.entities[0], ids.values[0]),
        (ids.entities[0], ids.values[1]),
        (ids.entities[1], ids.values[0]),
        (ids.entities[1], ids.values[1]),
        (ids.entities[2], ids.values[0]),
    ];
    let parent_codes =
        parent_ids.map(|(entity, value)| (code(&program, entity), code(&program, value)));
    let values = parent_codes
        .into_iter()
        .flat_map(|(entity, value)| [entity, value])
        .collect();
    let frontier = program
        .frontier_from_indices(vec![v(0), v(1)], values, parent_ids.len())
        .unwrap();
    let cpu_children = program.transition(&frontier).unwrap();
    assert_eq!(cpu_children.len(), 1);
    let cpu_supported = cpu_children[0]
        .values()
        .chunks(3)
        .map(|row| (row[0].get(), row[1].get()))
        .collect::<HashSet<_>>();
    let expected = parent_codes
        .map(|parent| u32::from(cpu_supported.contains(&parent)))
        .to_vec();
    assert_eq!(expected, vec![1, 1, 1, 0, 0]);

    let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();
    assert_eq!(run_viability(&round, &frontier), expected);
}

#[test]
fn partial_patterns_stay_optimistic_and_missing_constants_stay_globally_dead() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let pattern = QueryPattern::new(v(0), v(1), v(2));
    let partial_program = QueryProgram::compile(resident.archive(), 3, [pattern]).unwrap();
    let partial_round = WgpuResidentRound::new(&resident, &partial_program, &[v(0), v(1)]).unwrap();
    assert!(partial_round.plan.fully_bound_supports().is_empty());
    assert!(partial_round.fully_bound_group.is_none());
    let mut values = Vec::new();
    for row in 0..65 {
        let (entity, attribute) = if row % 2 == 0 {
            (ids.entities[0], ids.attributes[0])
        } else {
            // Both codes occur on their axes, while this E/A pair does not.
            (ids.entities[3], ids.attributes[0])
        };
        values.extend([
            code(&partial_program, entity),
            code(&partial_program, attribute),
        ]);
    }
    let partial = partial_program
        .frontier_from_indices(vec![v(0), v(1)], values, 65)
        .unwrap();
    assert_eq!(run_viability(&partial_round, &partial), vec![1; 65]);

    let missing_program = QueryProgram::compile(
        resident.archive(),
        3,
        [
            pattern,
            QueryPattern::new(
                constant(ids.entities[0]),
                constant(ordered_id(250)),
                constant(ids.values[0]),
            ),
        ],
    )
    .unwrap();
    let dead_round =
        WgpuResidentRound::new(&resident, &missing_program, &[v(0), v(1), v(2)]).unwrap();
    assert!(dead_round.plan.is_global_dead());
    assert!(dead_round.fully_bound_group.is_none());
    let values = (0..65)
        .flat_map(|_| {
            [
                code(&missing_program, ids.entities[0]),
                code(&missing_program, ids.attributes[0]),
                code(&missing_program, ids.values[0]),
            ]
        })
        .collect();
    let dead = missing_program
        .frontier_from_indices(vec![v(0), v(1), v(2)], values, 65)
        .unwrap();
    assert_eq!(run_viability(&dead_round, &dead), vec![0; 65]);
}

#[test]
fn fully_bound_frontiers_preserve_archive_round_and_schema_ownership() {
    let (set, ids) = fixture_set();
    let first_archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let second_archive = first_archive.clone();
    let first = WgpuSuccinctArchive::new(first_archive).unwrap();
    let second = WgpuSuccinctArchive::new(second_archive).unwrap();
    let pattern = QueryPattern::new(v(0), v(1), v(2));
    let first_program = QueryProgram::compile(first.archive(), 3, [pattern]).unwrap();
    let second_program = QueryProgram::compile(second.archive(), 3, [pattern]).unwrap();
    let first_round = WgpuResidentRound::new(&first, &first_program, &[v(0), v(1), v(2)]).unwrap();
    let other_round = WgpuResidentRound::new(&first, &first_program, &[v(0), v(1), v(2)]).unwrap();
    let second_round =
        WgpuResidentRound::new(&second, &second_program, &[v(0), v(1), v(2)]).unwrap();
    let host = first_program
        .frontier_from_indices(
            vec![v(0), v(1), v(2)],
            vec![
                code(&first_program, ids.entities[0]),
                code(&first_program, ids.attributes[0]),
                code(&first_program, ids.values[0]),
            ],
            1,
        )
        .unwrap();
    let frontier = first_round.upload_frontier(&host).unwrap();
    assert!(matches!(
        other_round.initialize_inputs(&frontier),
        Err(ResidentSupportError::FrontierOwnership)
    ));
    assert!(matches!(
        second_round.initialize_inputs(&frontier),
        Err(ResidentSupportError::FrontierOwnership)
    ));

    let wrong_schema = ProgramFrontier::new(vec![v(0), v(1)], Vec::new(), 0).unwrap();
    assert!(matches!(
        first_round.upload_frontier(&wrong_schema),
        Err(ResidentSupportError::MalformedFrontier)
    ));
}

#[test]
fn absent_support_becomes_present_only_after_recompile_and_reencode() {
    let (mut set, ids) = fixture_set();
    let absent = (ids.entities[1], ids.attributes[0], ids.values[1]);
    let pattern = QueryPattern::new(v(0), v(1), v(2));

    let before_archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let before_resident = WgpuSuccinctArchive::new(before_archive).unwrap();
    let before_program = QueryProgram::compile(before_resident.archive(), 3, [pattern]).unwrap();
    let before_round =
        WgpuResidentRound::new(&before_resident, &before_program, &[v(0), v(1), v(2)]).unwrap();
    let before = before_program
        .frontier_from_indices(
            vec![v(0), v(1), v(2)],
            vec![
                code(&before_program, absent.0),
                code(&before_program, absent.1),
                code(&before_program, absent.2),
            ],
            1,
        )
        .unwrap();
    assert_eq!(run_viability(&before_round, &before), vec![0]);

    set.insert(&role_trible(absent.0, absent.1, absent.2));
    let after_archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let after_resident = WgpuSuccinctArchive::new(after_archive).unwrap();
    let after_program = QueryProgram::compile(after_resident.archive(), 3, [pattern]).unwrap();
    let after_round =
        WgpuResidentRound::new(&after_resident, &after_program, &[v(0), v(1), v(2)]).unwrap();
    let after = after_program
        .frontier_from_indices(
            vec![v(0), v(1), v(2)],
            vec![
                code(&after_program, absent.0),
                code(&after_program, absent.1),
                code(&after_program, absent.2),
            ],
            1,
        )
        .unwrap();
    assert_eq!(run_viability(&after_round, &after), vec![1]);
}

#[test]
fn fully_bound_pipeline_preserves_present_absent_and_poison_states() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1), v(2)]).unwrap();
    let present = [
        code(&program, ids.entities[0]),
        code(&program, ids.attributes[0]),
        code(&program, ids.values[0]),
    ];
    let absent = [
        code(&program, ids.entities[1]),
        code(&program, ids.attributes[0]),
        code(&program, ids.values[1]),
    ];
    let domain = resident.archive().domain.len() as u32;
    let frontier = WgpuResidentFrontier {
        archive: &resident,
        owner: round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: resident
            .context()
            .upload_u32(&[
                present[0], present[1], present[2], absent[0], absent[1], absent[2], present[0],
                present[1], domain,
            ])
            .unwrap()
            .into(),
        variables: vec![v(0), v(1), v(2)].into_boxed_slice(),
        rows: 3,
        stride: 3,
    };
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let (viability, estimates) = inputs.read_producer_outputs();
    assert_eq!(viability, vec![1, 0, RESIDENT_U32_SENTINEL],);
    assert!(estimates.is_empty());
    assert_eq!(
        choices.read_words_for_test(),
        vec![
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            0,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            0,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
        ]
    );
    assert!(matches!(
        choices.read(),
        Err(ResidentRoundError::PoisonedDeviceChoice { row: 2 })
    ));
}

#[test]
fn pre_jerky_prepare_jointly_poisons_every_invalid_eav_source() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let valid = [
        code(&program, ids.entities[0]),
        code(&program, ids.attributes[0]),
        code(&program, ids.values[0]),
    ];
    let domain = resident.archive().domain.len() as u32;
    let frontier_words = [
        valid,
        [domain, valid[1], valid[2]],
        [valid[0], domain, valid[2]],
        [valid[0], valid[1], domain],
        [RESIDENT_U32_SENTINEL, valid[1], valid[2]],
        [valid[0], RESIDENT_U32_SENTINEL, valid[2]],
        [valid[0], valid[1], RESIDENT_U32_SENTINEL],
        [valid[0], u32::MAX - 1, valid[2]],
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let rows = frontier_words.len() / 3;
    let descriptors = resident
        .context()
        .upload_u32(&[COLUMN_SOURCE, 0, COLUMN_SOURCE, 1, COLUMN_SOURCE, 2])
        .unwrap();
    let frontier = resident.context().upload_u32(&frontier_words).unwrap();
    let mut entity_queries = resident.context().empty_u32(rows * 2).unwrap();
    let mut attribute_queries = resident.context().empty_u32(rows).unwrap();
    let mut eva_values = resident.context().empty_u32(rows * 2).unwrap();
    let mut aev_values = resident.context().empty_u32(rows * 2).unwrap();
    let dispatch = resident
        .context()
        .static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        prepare_fully_bound_support::launch_unchecked::<WgpuRuntime>(
            resident.context().client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            descriptors.input_arg(),
            frontier.input_arg(),
            entity_queries.output_arg(),
            attribute_queries.output_arg(),
            eva_values.output_arg(),
            aev_values.output_arg(),
            rows as u32,
            3,
            1,
            domain,
            RESIDENT_U32_SENTINEL,
        );
    }
    let entity_queries = entity_queries.read();
    let attribute_queries = attribute_queries.read();
    let eva_values = eva_values.read();
    let aev_values = aev_values.read();
    assert_eq!(&entity_queries[0..2], &[valid[0], valid[0] + 1]);
    assert_eq!(attribute_queries[0], valid[1]);
    assert_eq!(&eva_values[0..2], &[valid[1]; 2]);
    assert_eq!(&aev_values[0..2], &[valid[2]; 2]);
    for row in 1..rows {
        assert_eq!(
            &entity_queries[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2]
        );
        assert_eq!(attribute_queries[row], RESIDENT_U32_SENTINEL);
        assert_eq!(
            &eva_values[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2]
        );
        assert_eq!(
            &aev_values[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2]
        );
    }

    // Malformed source kinds, out-of-stride columns, and sentinel constants
    // are rejected by the same joint trust boundary before any Jerky call.
    let malformed = resident
        .context()
        .upload_u32(&[
            9,
            0,
            CONSTANT_SOURCE,
            valid[1],
            CONSTANT_SOURCE,
            valid[2],
            COLUMN_SOURCE,
            3,
            CONSTANT_SOURCE,
            valid[1],
            CONSTANT_SOURCE,
            valid[2],
            CONSTANT_SOURCE,
            valid[0],
            CONSTANT_SOURCE,
            valid[1],
            CONSTANT_SOURCE,
            RESIDENT_U32_SENTINEL,
        ])
        .unwrap();
    let frontier = resident.context().upload_u32(&valid).unwrap();
    let malformed_lanes = 3usize;
    let mut entity_queries = resident.context().empty_u32(malformed_lanes * 2).unwrap();
    let mut attribute_queries = resident.context().empty_u32(malformed_lanes).unwrap();
    let mut eva_values = resident.context().empty_u32(malformed_lanes * 2).unwrap();
    let mut aev_values = resident.context().empty_u32(malformed_lanes * 2).unwrap();
    let dispatch = resident
        .context()
        .static_batch_dispatch(malformed_lanes, malformed_lanes, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        prepare_fully_bound_support::launch_unchecked::<WgpuRuntime>(
            resident.context().client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            malformed.input_arg(),
            frontier.input_arg(),
            entity_queries.output_arg(),
            attribute_queries.output_arg(),
            eva_values.output_arg(),
            aev_values.output_arg(),
            1,
            3,
            malformed_lanes as u32,
            domain,
            RESIDENT_U32_SENTINEL,
        );
    }
    assert_eq!(
        entity_queries.read(),
        vec![RESIDENT_U32_SENTINEL; malformed_lanes * 2]
    );
    assert_eq!(
        attribute_queries.read(),
        vec![RESIDENT_U32_SENTINEL; malformed_lanes]
    );
    assert_eq!(
        eva_values.read(),
        vec![RESIDENT_U32_SENTINEL; malformed_lanes * 2]
    );
    assert_eq!(
        aev_values.read(),
        vec![RESIDENT_U32_SENTINEL; malformed_lanes * 2]
    );
}

#[test]
fn normalization_poisons_only_derived_outputs_and_preserves_predecessors() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let context = resident.context();
    let lanes = 10usize;
    let entity_query_words = [1, 2, 1, 2, 1, 2, 5, 6, 1, 2, 1, 2, 10, 11, 1, 2, 1, 2, 1, 2];
    let entity_queries = context.upload_u32(&entity_query_words).unwrap();
    let mut eva_positions = context
        .upload_u32(&[
            3,
            5,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            8,
            3,
            4,
            7,
            2,
            20,
            3,
            5,
            12,
            13,
            3,
            5,
            3,
            5,
            3,
            5,
        ])
        .unwrap();
    let attribute_query_words = [1, 1, 1, 1, 1, 5, 1, 1, 1, 1];
    let attribute_queries = context.upload_u32(&attribute_query_words).unwrap();
    let mut attribute_bases = context.upload_u32(&[4; 10]).unwrap();
    let eva_value_words = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 5, 1, 1, 2, 2, 1, 1, 1, 1];
    let eva_values = context.upload_u32(&eva_value_words).unwrap();
    let aev_value_words = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 10, 10, 2, 3];
    let aev_values = context.upload_u32(&aev_value_words).unwrap();
    let dispatch = context
        .static_batch_dispatch(lanes, lanes, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        normalize_prepare_eva::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            entity_queries.input_arg(),
            eva_positions.output_arg(),
            attribute_queries.input_arg(),
            attribute_bases.output_arg(),
            eva_values.input_arg(),
            aev_values.input_arg(),
            lanes as u32,
            10,
            10,
            10,
            RESIDENT_U32_SENTINEL,
        );
    }
    let mut eva_ranks = context.empty_u32(lanes * 2).unwrap();
    resident
        .ring_col(SuccinctRotation::Eva)
        .rank_batch_into(&eva_positions, &eva_values, &mut eva_ranks)
        .unwrap();
    let positions = eva_positions.read();
    let bases = attribute_bases.read();
    let ranks = eva_ranks.read();
    assert_eq!(&positions[0..2], &[2, 3]);
    assert_eq!(bases[0], 3);
    assert!(ranks[0..2]
        .iter()
        .all(|&rank| rank != RESIDENT_U32_SENTINEL));
    for lane in 1..lanes {
        assert_eq!(
            &positions[lane * 2..lane * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2]
        );
        assert_eq!(bases[lane], RESIDENT_U32_SENTINEL);
        assert_eq!(&ranks[lane * 2..lane * 2 + 2], &[RESIDENT_U32_SENTINEL; 2]);
    }
    assert_eq!(entity_queries.read(), entity_query_words);
    assert_eq!(attribute_queries.read(), attribute_query_words);
    assert_eq!(eva_values.read(), eva_value_words);
    assert_eq!(aev_values.read(), aev_value_words);
}

#[test]
fn anchor_rejects_malformed_ranks_positions_values_and_addition_overflow() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let context = resident.context();
    let lanes = 15usize;
    let eva_positions = context
        .upload_u32(&[
            2,
            5,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            8,
            3,
            2,
            21,
            2,
            5,
            2,
            5,
            2,
            5,
            2,
            5,
            2,
            5,
            15,
            20,
            2,
            5,
            2,
            5,
            2,
            5,
            3,
            3,
            3,
            4,
        ])
        .unwrap();
    let eva_ranks = context
        .upload_u32(&[
            1,
            3,
            1,
            3,
            1,
            3,
            1,
            3,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            4,
            2,
            1,
            21,
            3,
            4,
            1,
            3,
            1,
            10,
            1,
            3,
            1,
            3,
            1,
            3,
            1,
            2,
            1,
            3,
        ])
        .unwrap();
    let bases = context
        .upload_u32(&[
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            4,
            RESIDENT_U32_SENTINEL,
            u32::MAX - 2,
            4,
            4,
            4,
            4,
            4,
        ])
        .unwrap();
    let mut values = context
        .upload_u32(&[
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            10,
            10,
            2,
            3,
            2,
            2,
            2,
            2,
        ])
        .unwrap();
    let mut positions = context.empty_u32(lanes * 2).unwrap();
    let dispatch = context
        .static_batch_dispatch(lanes, lanes, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        anchor_prepare_aev::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            eva_positions.input_arg(),
            eva_ranks.input_arg(),
            bases.input_arg(),
            values.output_arg(),
            positions.output_arg(),
            lanes as u32,
            20,
            u32::MAX - 1,
            10,
            RESIDENT_U32_SENTINEL,
        );
    }
    let positions = positions.read();
    assert_eq!(&positions[0..2], &[5, 7]);
    for lane in 1..lanes {
        assert_eq!(
            &positions[lane * 2..lane * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2]
        );
    }
    let values = values.read();
    for lane in 1..lanes {
        assert_eq!(&values[lane * 2..lane * 2 + 2], &[RESIDENT_U32_SENTINEL; 2]);
    }
}

#[test]
fn final_support_reduction_has_one_row_writer_and_fails_closed() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let context = resident.context();
    let present = ([1, 3], [0, 1]);
    let absent = ([1, 3], [1, 1]);
    let poisoned = ([RESIDENT_U32_SENTINEL; 2], [0, 1]);
    let reversed = ([3, 1], [0, 1]);
    let out_of_range = ([1, 3], [0, 11]);
    let duplicate = ([2, 4], [0, 2]);
    let first = [
        present, present, present, present, present, present, present, present, absent, poisoned,
        duplicate,
    ];
    let second = [
        present,
        absent,
        poisoned,
        reversed,
        out_of_range,
        poisoned,
        present,
        present,
        poisoned,
        absent,
        present,
    ];
    let rows = first.len();
    let supports = 2usize;
    let mut positions = Vec::new();
    let mut ranks = Vec::new();
    for lanes in [&first, &second] {
        for &(position, rank) in lanes {
            positions.extend(position);
            ranks.extend(rank);
        }
    }
    let positions = context.upload_u32(&positions).unwrap();
    let ranks = context.upload_u32(&ranks).unwrap();
    let mut viable = context
        .upload_u32(&[1, 1, 1, 1, 1, 0, 2, 0, 1, 1, 1])
        .unwrap();
    let dispatch = context
        .static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        reduce_fully_bound_support::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            positions.input_arg(),
            ranks.input_arg(),
            viable.output_arg(),
            rows as u32,
            supports as u32,
            10,
            RESIDENT_U32_SENTINEL,
        );
    }
    assert_eq!(
        viable.read(),
        vec![
            1,
            0,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            0,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
        ]
    );
}

#[test]
fn fully_bound_geometry_excludes_the_reserved_sentinel() {
    let limit = RESIDENT_U32_SENTINEL as usize;
    let largest = (limit - 1) / 2;
    assert_eq!(
        fully_bound_group_geometry(1, largest).unwrap(),
        (largest, largest * 2)
    );
    assert!(matches!(
        fully_bound_group_geometry(1, largest + 1),
        Err(ResidentRoundError::GeometryOverflow(
            "resident fully-bound support endpoints"
        ))
    ));
    assert!(matches!(
        fully_bound_group_geometry(usize::MAX, 2),
        Err(ResidentRoundError::GeometryOverflow(
            "resident fully-bound support lanes"
        ))
    ));
    assert!(checked_device_product((limit - 1) / 6, 6, "descriptors").is_ok());
    assert!(matches!(
        checked_device_product((limit - 1) / 6 + 1, 6, "descriptors"),
        Err(ResidentRoundError::GeometryOverflow("descriptors"))
    ));
}
