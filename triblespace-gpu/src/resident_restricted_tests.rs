use super::*;

use crate::resident_round::ResidentRowChoice;
use jerky::bit_vector::Select;
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

fn cpu_pair_count(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    peer: u32,
) -> u32 {
    let prefix = match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Eva => &archive.e_a,
        SuccinctRotation::Aev | SuccinctRotation::Ave => &archive.a_a,
        SuccinctRotation::Vea | SuccinctRotation::Vae => &archive.v_a,
    };
    let peer = peer as usize;
    let lo = prefix.select1(peer).unwrap() - peer;
    let hi = prefix.select1(peer + 1).unwrap() - (peer + 1);
    archive.distinct_in(archive.pair_changes(rotation), &(lo..hi)) as u32
}

fn cpu_restricted_count(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    first: u32,
    last: u32,
) -> u32 {
    let witness = cpu_restricted_witness(archive, rotation, first, last);
    witness[3] - witness[2]
}

fn cpu_restricted_witness(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    first: u32,
    last: u32,
) -> [u32; PROPOSAL_WITNESS_WORDS] {
    let prefix = match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Eva => &archive.e_a,
        SuccinctRotation::Aev | SuccinctRotation::Ave => &archive.a_a,
        SuccinctRotation::Vea | SuccinctRotation::Vae => &archive.v_a,
    };
    let first = first as usize;
    let last = last as usize;
    let lo = prefix.select1(first).unwrap() - first;
    let hi = prefix.select1(first + 1).unwrap() - (first + 1);
    let ring = archive.ring_col(rotation);
    let lo_rank = ring.rank(lo, last).unwrap();
    let hi_rank = ring.rank(hi, last).unwrap();
    [lo as u32, hi as u32, lo_rank as u32, hi_rank as u32]
}

fn source_code(source: CodeSource, frontier: &ProgramFrontier, row: usize) -> u32 {
    match source {
        CodeSource::Constant(code) => code,
        CodeSource::Column(column) => {
            frontier.values()[row * frontier.variables().len() + column as usize].get()
        }
    }
}

fn expected_matrix(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
) -> Vec<u32> {
    let mut expected = Vec::with_capacity(round.plan.arm_specs().len() * frontier.len());
    for &spec in round.plan.arm_specs() {
        for row in 0..frontier.len() {
            expected.push(match spec {
                ArmSpec::Present { count, .. } => count,
                ArmSpec::PairDistinct { rotation, peer, .. } => cpu_pair_count(
                    round.archive.archive(),
                    rotation,
                    source_code(peer, frontier, row),
                ),
                ArmSpec::Restricted {
                    rotation,
                    first,
                    last,
                    ..
                } => cpu_restricted_count(
                    round.archive.archive(),
                    rotation,
                    source_code(first, frontier, row),
                    source_code(last, frontier, row),
                ),
            });
        }
    }
    expected
}

fn run_outputs(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
) -> (Vec<u32>, Vec<u32>, Vec<ResidentRowChoice>) {
    let resident = round.upload_frontier(frontier).unwrap();
    run_resident_outputs(round, &resident)
}

fn run_resident_outputs(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    frontier: &WgpuResidentFrontier<'_, OrderedUniverse>,
) -> (Vec<u32>, Vec<u32>, Vec<ResidentRowChoice>) {
    let inputs = round.initialize_inputs(frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let (viable, estimates) = inputs.read_producer_outputs();
    (viable, estimates, choices.read().unwrap())
}

fn reconfigure_restricted_round(
    round: &mut WgpuResidentRound<'_, OrderedUniverse>,
    specs: Vec<ArmSpec>,
) {
    assert_eq!(specs.len(), round.plan.metadata().arms().len());
    for (arm, spec) in specs.iter().copied().enumerate() {
        assert_eq!(spec.arm(), arm as u32);
        assert!(matches!(spec, ArmSpec::Restricted { .. }));
    }
    round.plan.arm_specs = specs.into_boxed_slice();
    round.plan.arm_groups = group_arms(&round.plan.arm_specs);
    round.initial_witnesses = round
        .archive
        .context()
        .upload_u32(&initial_witnesses(&round.plan).unwrap())
        .unwrap();
    round.pair_groups = build_pair_groups(round.archive, &round.plan).unwrap();
    round.restricted_groups = build_restricted_groups(round.archive, &round.plan).unwrap();
}

fn representative_pairs(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
) -> [(u32, u32); 3] {
    let domain = u32::try_from(archive.domain.len()).unwrap();
    let mut zero = None;
    let mut one = None;
    let mut many = None;
    for first in 0..domain {
        for last in 0..domain {
            let count = cpu_restricted_count(archive, rotation, first, last);
            if count == 0 {
                zero.get_or_insert((first, last));
            } else if count == 1 {
                one.get_or_insert((first, last));
            } else {
                many.get_or_insert((first, last));
            }
        }
    }
    [zero.unwrap(), one.unwrap(), many.unwrap()]
}

#[test]
fn restricted_producer_retains_exact_interval_witnesses_including_zero_width() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();
    let ArmSpec::Restricted { rotation, .. } = round.plan.arm_specs()[0] else {
        panic!("two-peer round lowered a non-Restricted witness")
    };
    let peers = representative_pairs(round.archive.archive(), rotation);
    let values = repeated_pairs(&peers, peers.len());
    let host = program
        .frontier_from_indices(vec![v(0), v(1)], values, peers.len())
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let expected = peers
        .into_iter()
        .flat_map(|(first, last)| {
            cpu_restricted_witness(round.archive.archive(), rotation, first, last)
        })
        .collect::<Vec<_>>();
    assert_eq!(inputs.read_proposal_witnesses_for_test(), expected);
    assert_eq!(expected[3] - expected[2], 0);
}

fn repeated_pairs(pairs: &[(u32, u32)], rows: usize) -> Vec<u32> {
    let mut values = Vec::with_capacity(rows * 2);
    for row in 0..rows {
        let (first, last) = pairs[row % pairs.len()];
        values.extend([first, last]);
    }
    values
}

fn append_arm_major(
    destination: &mut Vec<u32>,
    left: &[u32],
    right: &[u32],
    arms: usize,
    left_rows: usize,
    right_rows: usize,
) {
    destination.clear();
    destination.reserve(left.len() + right.len());
    for arm in 0..arms {
        destination.extend_from_slice(&left[arm * left_rows..(arm + 1) * left_rows]);
        destination.extend_from_slice(&right[arm * right_rows..(arm + 1) * right_rows]);
    }
}

#[test]
fn heterogeneous_arm_major_groups_cover_all_rotations_and_source_kinds() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let pattern = QueryPattern::new(v(0), v(1), v(2));
    let program = QueryProgram::compile(resident.archive(), 3, [pattern; 24]).unwrap();
    let mut round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();

    let representatives =
        SuccinctRotation::ALL.map(|rotation| representative_pairs(resident.archive(), rotation));
    let mut specs = Vec::with_capacity(24);
    for source_shape in 0..4usize {
        for rotation_offset in 0..SuccinctRotation::ALL.len() {
            // Reverse each pass so every physical group owns non-contiguous
            // global arms and descriptor scatter cannot rely on local order.
            let rotation = SuccinctRotation::ALL[SuccinctRotation::ALL.len() - 1 - rotation_offset];
            let category = (source_shape + rotation.index()) % 3;
            let (constant_first, constant_last) = representatives[rotation.index()][category];
            let (first, last) = match source_shape {
                0 => (
                    CodeSource::Constant(constant_first),
                    CodeSource::Constant(constant_last),
                ),
                1 => (CodeSource::Constant(constant_first), CodeSource::Column(1)),
                2 => (CodeSource::Column(0), CodeSource::Constant(constant_last)),
                3 => (CodeSource::Column(0), CodeSource::Column(1)),
                _ => unreachable!(),
            };
            specs.push(ArmSpec::Restricted {
                arm: specs.len() as u32,
                rotation,
                first,
                last,
            });
        }
    }
    reconfigure_restricted_round(&mut round, specs);

    assert!(round.pair_groups.is_empty());
    assert_eq!(round.restricted_groups.len(), SuccinctRotation::ALL.len());
    for group in &round.restricted_groups {
        assert_eq!(group.arm_count, 4);
        let arm_ids = round
            .plan
            .arm_groups
            .iter()
            .find(|candidate| candidate.kind == ArmGroupKind::Restricted(group.rotation))
            .unwrap()
            .arm_ids();
        assert!(arm_ids.windows(2).all(|pair| pair[1] - pair[0] == 6));
    }

    let mut row_pairs = Vec::new();
    for rotation_pairs in representatives {
        row_pairs.extend(rotation_pairs);
    }
    for rows in [0usize, 1, 63, 64, 65] {
        let frontier = program
            .frontier_from_indices(vec![v(0), v(1)], repeated_pairs(&row_pairs, rows), rows)
            .unwrap();
        let expected = expected_matrix(&round, &frontier);
        let (viable, estimates, choices) = run_outputs(&round, &frontier);
        assert_eq!(viable, vec![1; rows], "rows={rows}");
        assert_eq!(estimates, expected, "rows={rows}");
        for (row, choice) in choices.into_iter().enumerate() {
            let best = (0..round.metadata().arms().len())
                .map(|arm| (expected[arm * rows + row], arm))
                .min()
                .unwrap();
            assert_eq!(choice.variable, Some(v(2)), "rows={rows}, row={row}");
            assert_eq!(choice.proposal_count, best.0, "rows={rows}, row={row}");
            assert_eq!(choice.proposer_arm, Some(best.1), "rows={rows}, row={row}");
        }
        if rows == 65 {
            assert!(estimates.contains(&0));
            assert!(estimates.contains(&1));
            assert!(estimates
                .iter()
                .any(|&count| count > 1 && count != RESIDENT_U32_SENTINEL));
        }
    }
}

#[test]
fn all_six_rotation_frontiers_commute_with_every_split() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let pattern = QueryPattern::new(v(0), v(1), v(2));
    let program = QueryProgram::compile(resident.archive(), 3, [pattern; 6]).unwrap();
    let mut round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();
    let mut specs = Vec::new();
    let mut row_pairs = Vec::new();
    for rotation in SuccinctRotation::ALL {
        let representatives = representative_pairs(resident.archive(), rotation);
        row_pairs.extend(representatives);
        specs.push(ArmSpec::Restricted {
            arm: specs.len() as u32,
            rotation,
            first: CodeSource::Column(0),
            last: CodeSource::Column(1),
        });
    }
    reconfigure_restricted_round(&mut round, specs);
    let frontier = program
        .frontier_from_indices(vec![v(0), v(1)], repeated_pairs(&row_pairs, 65), 65)
        .unwrap();
    let (whole_viable, whole_estimates, whole_choices) = run_outputs(&round, &frontier);

    for split in 0..=65 {
        let left = frontier.slice(0..split).unwrap();
        let right = frontier.slice(split..65).unwrap();
        let (mut viable, left_estimates, mut choices) = run_outputs(&round, &left);
        let (right_viable, right_estimates, right_choices) = run_outputs(&round, &right);
        viable.extend(right_viable);
        choices.extend(right_choices);
        let mut estimates = Vec::new();
        append_arm_major(
            &mut estimates,
            &left_estimates,
            &right_estimates,
            round.metadata().arms().len(),
            split,
            65 - split,
        );
        assert_eq!(viable, whole_viable, "split {split}");
        assert_eq!(estimates, whole_estimates, "split {split}");
        assert_eq!(choices, whole_choices, "split {split}");
    }
}

#[test]
fn canonical_restricted_estimates_match_cpu_transition_for_each_target_axis() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();

    let entity_codes = ids
        .entities
        .iter()
        .copied()
        .map(|id| code(&program, id))
        .collect::<Vec<_>>();
    let attribute_codes = ids
        .attributes
        .iter()
        .copied()
        .map(|id| code(&program, id))
        .collect::<Vec<_>>();
    let value_codes = ids
        .values
        .iter()
        .copied()
        .map(|id| code(&program, id))
        .collect::<Vec<_>>();

    for (bound, pairs, expected_rotation, target) in [
        (
            vec![v(1), v(2)],
            attribute_codes
                .iter()
                .copied()
                .flat_map(|attribute| {
                    value_codes
                        .iter()
                        .copied()
                        .map(move |value| (attribute, value))
                })
                .collect::<Vec<_>>(),
            SuccinctRotation::Aev,
            v(0),
        ),
        (
            vec![v(0), v(2)],
            entity_codes
                .iter()
                .copied()
                .flat_map(|entity| {
                    value_codes
                        .iter()
                        .copied()
                        .map(move |value| (entity, value))
                })
                .collect::<Vec<_>>(),
            SuccinctRotation::Eav,
            v(1),
        ),
        (
            vec![v(0), v(1)],
            entity_codes
                .iter()
                .copied()
                .flat_map(|entity| {
                    attribute_codes
                        .iter()
                        .copied()
                        .map(move |attribute| (entity, attribute))
                })
                .collect::<Vec<_>>(),
            SuccinctRotation::Eva,
            v(2),
        ),
    ] {
        let round = WgpuResidentRound::new(&resident, &program, &bound).unwrap();
        assert!(matches!(
            round.plan.arm_specs(),
            [ArmSpec::Restricted { rotation, .. }] if *rotation == expected_rotation
        ));
        let rows = 65;
        let frontier = program
            .frontier_from_indices(bound, repeated_pairs(&pairs, rows), rows)
            .unwrap();
        let expected = expected_matrix(&round, &frontier);
        let (viable, estimates, choices) = run_outputs(&round, &frontier);
        assert_eq!(viable, vec![1; rows]);
        assert_eq!(estimates, expected);
        for row in 0..rows {
            let single = frontier.slice(row..row + 1).unwrap();
            let cpu = program.transition(&single).unwrap();
            let choice = choices[row];
            assert_eq!(choice.variable, Some(target));
            assert_eq!(choice.proposal_count, expected[row]);
            if expected[row] == 0 {
                assert!(cpu.is_empty());
                assert_ne!(choice, ResidentRowChoice::dead());
            } else {
                assert_eq!(cpu.len(), 1);
                assert!(cpu[0].variables().contains(&target));
                assert_eq!(cpu[0].len(), expected[row] as usize);
            }
        }
    }
}

#[test]
fn duplicate_and_interleaved_restricted_groups_scatter_by_global_arm() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let pattern = QueryPattern::new(v(0), v(1), v(2));
    let program = QueryProgram::compile(resident.archive(), 3, [pattern; 9]).unwrap();
    let mut round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();
    let rotations = [
        SuccinctRotation::Eav,
        SuccinctRotation::Aev,
        SuccinctRotation::Eav,
        SuccinctRotation::Vea,
        SuccinctRotation::Aev,
        SuccinctRotation::Eav,
        SuccinctRotation::Vae,
        SuccinctRotation::Ave,
        SuccinctRotation::Eva,
    ];
    let specs = rotations
        .into_iter()
        .enumerate()
        .map(|(arm, rotation)| ArmSpec::Restricted {
            arm: arm as u32,
            rotation,
            first: CodeSource::Column(0),
            last: CodeSource::Column(1),
        })
        .collect();
    reconfigure_restricted_round(&mut round, specs);
    let representatives = SuccinctRotation::ALL
        .into_iter()
        .flat_map(|rotation| representative_pairs(resident.archive(), rotation))
        .collect::<Vec<_>>();
    let frontier = program
        .frontier_from_indices(vec![v(0), v(1)], repeated_pairs(&representatives, 65), 65)
        .unwrap();
    let expected = expected_matrix(&round, &frontier);
    let (_, estimates, _) = run_outputs(&round, &frontier);
    assert_eq!(estimates, expected);
    assert_eq!(&estimates[0..65], &estimates[2 * 65..3 * 65]);
    assert_eq!(&estimates[0..65], &estimates[5 * 65..6 * 65]);
}

#[test]
fn invalid_last_aliases_poison_queries_values_and_rank_positions_before_wavelet() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let valid_first = code(&program, ids.entities[0]);
    let valid_last = code(&program, ids.attributes[0]);
    let domain = u32::try_from(resident.archive().domain.len()).unwrap();
    let rows = 4usize;
    let probes = rows;
    let endpoints = probes * 2;
    let descriptors = resident
        .context()
        .upload_u32(&[0, COLUMN_SOURCE, 0, COLUMN_SOURCE, 1])
        .unwrap();
    let frontier = resident
        .context()
        .upload_u32(&[
            valid_first,
            valid_last,
            valid_first,
            domain,
            valid_first,
            u32::MAX - 1,
            domain,
            valid_last,
        ])
        .unwrap();
    let mut positions = resident.context().empty_u32(endpoints).unwrap();
    let mut values = resident.context().empty_u32(endpoints).unwrap();
    let dispatch = resident
        .context()
        .static_batch_dispatch(probes, probes, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        prepare_restricted::launch_unchecked::<WgpuRuntime>(
            resident.context().client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            descriptors.input_arg(),
            frontier.input_arg(),
            positions.output_arg(),
            values.output_arg(),
            rows as u32,
            2,
            1,
            domain,
            RESIDENT_U32_SENTINEL,
        );
    }
    let prepared_positions = positions.read();
    let prepared_values = values.read();
    assert_eq!(&prepared_positions[0..2], &[valid_first, valid_first + 1]);
    assert_eq!(&prepared_values[0..2], &[valid_last, valid_last]);
    for row in 1..rows {
        assert_eq!(
            &prepared_positions[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2],
            "invalid row {row} leaked a prefix query"
        );
        assert_eq!(
            &prepared_values[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2],
            "invalid row {row} leaked a wavelet symbol"
        );
    }

    let mut selected = resident.context().empty_u32(endpoints).unwrap();
    resident
        .entity_prefix()
        .select1_batch_into(&positions, &mut selected)
        .unwrap();
    unsafe {
        normalize_pair_range::launch_unchecked::<WgpuRuntime>(
            resident.context().client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            selected.input_arg(),
            positions.output_arg(),
            probes as u32,
            resident.ring_col(SuccinctRotation::Eva).len() as u32,
            RESIDENT_U32_SENTINEL,
        );
    }
    let rank_positions = positions.read();
    for row in 1..rows {
        assert_eq!(
            &rank_positions[row * 2..row * 2 + 2],
            &[RESIDENT_U32_SENTINEL; 2],
            "invalid row {row} reached wavelet rank"
        );
    }
}

#[test]
fn invalid_last_aliases_fail_closed_in_the_complete_restricted_pipeline() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1)]).unwrap();
    let valid_first = code(&program, ids.entities[0]);
    let valid_last = code(&program, ids.attributes[0]);
    let domain = u32::try_from(resident.archive().domain.len()).unwrap();
    let frontier = WgpuResidentFrontier {
        archive: &resident,
        owner: round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: resident
            .context()
            .upload_u32(&[
                valid_first,
                valid_last,
                valid_first,
                domain,
                valid_first,
                u32::MAX - 1,
                valid_first,
                u32::MAX,
                domain,
                valid_last,
            ])
            .unwrap(),
        variables: vec![v(0), v(1)].into_boxed_slice(),
        rows: 5,
        stride: 2,
    };
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let (viable, estimates) = inputs.read_producer_outputs();
    assert_eq!(viable, vec![1; 5]);
    assert_ne!(estimates[0], RESIDENT_U32_SENTINEL);
    assert!(estimates[1..5]
        .iter()
        .all(|&estimate| estimate == RESIDENT_U32_SENTINEL));
    assert!(matches!(
        choices.read(),
        Err(ResidentRoundError::PoisonedDeviceChoice { row: 1 })
    ));

    let valid_frontier = WgpuResidentFrontier {
        archive: &resident,
        owner: round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: resident
            .context()
            .upload_u32(&[valid_first, valid_last])
            .unwrap(),
        variables: vec![v(0), v(1)].into_boxed_slice(),
        rows: 1,
        stride: 2,
    };
    let (_, valid_estimates, valid_choices) = run_resident_outputs(&round, &valid_frontier);
    assert_ne!(valid_estimates[0], RESIDENT_U32_SENTINEL);
    assert_ne!(valid_choices[0], ResidentRowChoice::dead());
}

#[test]
fn normalization_and_scatter_independently_reject_poison_reversal_and_range_errors() {
    let (set, _) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let context = resident.context();
    let probes = 5usize;
    let mut positions = context.upload_u32(&[1, 2, 1, 2, 1, 2, 1, 2, 1, 2]).unwrap();
    let selected = context
        .upload_u32(&[
            5,
            8,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            6,
            3,
            0,
            2,
            4,
            20,
        ])
        .unwrap();
    let dispatch = context
        .static_batch_dispatch(probes, probes, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        normalize_pair_range::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            selected.input_arg(),
            positions.output_arg(),
            probes as u32,
            10,
            RESIDENT_U32_SENTINEL,
        );
    }
    assert_eq!(
        positions.read(),
        vec![
            4,
            6,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
        ]
    );

    let rows = 8usize;
    let descriptors = context
        .upload_u32(&[0, CONSTANT_SOURCE, 0, CONSTANT_SOURCE, 0])
        .unwrap();
    let ranks = context
        .upload_u32(&[
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            5,
            4,
            0,
            11,
            u32::MAX - 1,
            u32::MAX - 1,
            2,
            2,
            2,
            5,
            3,
            4,
            1,
            3,
        ])
        .unwrap();
    let positions = context
        .upload_u32(&[
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            1,
            2,
            1,
            2,
            u32::MAX - 1,
            u32::MAX - 1,
            2,
            2,
            2,
            5,
            4,
            4,
            4,
            5,
        ])
        .unwrap();
    let mut witnesses = context
        .upload_u32(&[77; 8 * PROPOSAL_WITNESS_WORDS])
        .unwrap();
    let dispatch = context
        .static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        scatter_restricted::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            descriptors.input_arg(),
            positions.input_arg(),
            ranks.input_arg(),
            witnesses.output_arg(),
            rows as u32,
            1,
            1,
            10,
            RESIDENT_U32_SENTINEL,
        );
    }
    assert_eq!(
        witnesses.read(),
        vec![
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            2,
            2,
            2,
            2,
            2,
            5,
            2,
            5,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
        ]
    );
}

#[test]
fn restricted_geometry_excludes_the_reserved_sentinel() {
    let limit = RESIDENT_U32_SENTINEL as usize;
    let largest = (limit - 1) / 2;
    assert_eq!(
        restricted_group_geometry(1, largest).unwrap(),
        (largest, largest * 2)
    );
    assert!(matches!(
        restricted_group_geometry(1, largest + 1),
        Err(ResidentRoundError::GeometryOverflow(
            "resident restricted endpoints"
        ))
    ));
    assert!(matches!(
        restricted_group_geometry(usize::MAX, 2),
        Err(ResidentRoundError::GeometryOverflow(
            "resident restricted probes"
        ))
    ));
    assert!(checked_device_product((limit - 1) / 5, 5, "descriptors").is_ok());
    assert!(matches!(
        checked_device_product((limit - 1) / 5 + 1, 5, "descriptors"),
        Err(ResidentRoundError::GeometryOverflow("descriptors"))
    ));
}

#[test]
fn restricted_frontiers_preserve_round_ownership_and_global_dead_rows() {
    let (set, ids) = fixture_set();
    let first_archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let second_archive = first_archive.clone();
    let first = WgpuSuccinctArchive::new(first_archive).unwrap();
    let second = WgpuSuccinctArchive::new(second_archive).unwrap();
    let first_program =
        QueryProgram::compile(first.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let second_program =
        QueryProgram::compile(second.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let first_round = WgpuResidentRound::new(&first, &first_program, &[v(0), v(1)]).unwrap();
    let second_round = WgpuResidentRound::new(&second, &second_program, &[v(0), v(1)]).unwrap();
    let host = first_program
        .frontier_from_indices(
            vec![v(0), v(1)],
            vec![
                code(&first_program, ids.entities[0]),
                code(&first_program, ids.attributes[0]),
            ],
            1,
        )
        .unwrap();
    let frontier = first_round.upload_frontier(&host).unwrap();
    assert!(matches!(
        second_round.initialize_inputs(&frontier),
        Err(ResidentSupportError::FrontierOwnership)
    ));

    let missing = ordered_id(250);
    let dead_program = QueryProgram::compile(
        first.archive(),
        2,
        [QueryPattern::new(v(0), constant(missing), v(1))],
    )
    .unwrap();
    let dead_round = WgpuResidentRound::new(&first, &dead_program, &[v(0)]).unwrap();
    assert!(dead_round.plan.is_global_dead());
    let dead_host = dead_program
        .frontier_from_indices(
            vec![v(0)],
            vec![code(&dead_program, ids.entities[0]); 65],
            65,
        )
        .unwrap();
    let (viable, estimates, choices) = run_outputs(&dead_round, &dead_host);
    assert_eq!(viable, vec![0; 65]);
    assert_eq!(estimates, vec![0; 65]);
    assert_eq!(choices, vec![ResidentRowChoice::dead(); 65]);
}

#[test]
fn exact_zero_is_viable_and_restricted_counts_grow_after_snapshot_recompile() {
    let e = ordered_id(1);
    let absent_e = ordered_id(2);
    let a1 = ordered_id(20);
    let a2 = ordered_id(21);
    let x = ordered_id(40);
    let y = ordered_id(41);
    let mut base = TribleSet::new();
    base.insert(&role_trible(e, a1, x));
    // Keep every semantic code present in the base universe while the
    // queried `(absent_e, x)` pair itself remains empty.
    base.insert(&role_trible(absent_e, a1, y));
    base.insert(&role_trible(e, a2, y));
    let mut extended = base.clone();
    extended.insert(&role_trible(e, a2, x));

    let run = |set: &TribleSet, entity: Id| {
        let archive: SuccinctArchive<OrderedUniverse> = set.into();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        let program =
            QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
                .unwrap();
        let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(2)]).unwrap();
        let frontier = program
            .frontier_from_indices(
                vec![v(0), v(2)],
                vec![code(&program, entity), code(&program, x)],
                1,
            )
            .unwrap();
        let (viable, estimates, choices) = run_outputs(&round, &frontier);
        assert_eq!(viable, vec![1]);
        assert_ne!(choices[0], ResidentRowChoice::dead());
        assert_eq!(choices[0].proposal_count, estimates[0]);
        estimates[0]
    };

    assert_eq!(run(&base, absent_e), 0);
    assert_eq!(run(&base, e), 1);
    assert_eq!(run(&extended, e), 2);
}

#[test]
fn fully_bound_support_follows_the_restricted_stage_without_relabeling() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0), v(1), v(2)]).unwrap();
    let host = program
        .frontier_from_indices(
            vec![v(0), v(1), v(2)],
            vec![
                code(&program, ids.entities[0]),
                code(&program, ids.attributes[0]),
                code(&program, ids.values[0]),
            ],
            1,
        )
        .unwrap();
    let (viable, estimates, choices) = run_outputs(&round, &host);
    assert_eq!(viable, vec![1]);
    assert!(estimates.is_empty());
    assert_eq!(choices, vec![ResidentRowChoice::dead()]);
}
