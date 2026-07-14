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
    entities: [Id; 3],
    attributes: [Id; 3],
    values: [Id; 3],
}

fn fixture_set() -> (TribleSet, FixtureIds) {
    let ids = FixtureIds {
        entities: [ordered_id(1), ordered_id(2), ordered_id(3)],
        attributes: [ordered_id(20), ordered_id(21), ordered_id(22)],
        values: [ordered_id(40), ordered_id(41), ordered_id(42)],
    };
    let [e1, e2, e3] = ids.entities;
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
        role_trible(e3, a2, x),
        role_trible(e3, a2, y),
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
    let next = peer + 1;
    let lo = prefix.select1(peer).unwrap() - peer;
    let hi = prefix.select1(next).unwrap() - next;
    archive.distinct_in(archive.pair_changes(rotation), &(lo..hi)) as u32
}

fn expected_matrix(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
) -> Vec<u32> {
    let rows = frontier.len();
    let stride = frontier.variables().len();
    let mut result = Vec::with_capacity(round.plan.arm_specs().len() * rows);
    for &spec in round.plan.arm_specs() {
        for row in 0..rows {
            let estimate = match spec {
                ArmSpec::Present { count, .. } => count,
                ArmSpec::PairDistinct { rotation, peer, .. } => {
                    let peer = match peer {
                        CodeSource::Constant(code) => code,
                        CodeSource::Column(column) => {
                            frontier.values()[row * stride + column as usize].get()
                        }
                    };
                    cpu_pair_count(round.archive.archive(), rotation, peer)
                }
                ArmSpec::Restricted { .. } => DEAD_ROW_SENTINEL,
            };
            result.push(estimate);
        }
    }
    result
}

fn planner_oracle(
    metadata: &ResidentRoundMetadata,
    viable: &[u32],
    estimates: &[u32],
) -> Vec<ResidentRowChoice> {
    let rows = viable.len();
    (0..rows)
        .map(|row| {
            if viable[row] != 1
                || (0..metadata.arms().len())
                    .any(|arm| estimates[arm * rows + row] == DEAD_ROW_SENTINEL)
            {
                return ResidentRowChoice::dead();
            }
            let mut best: Option<(ProgramVariable, usize, u32, u32, u32)> = None;
            for variable_index in 0..metadata.variable_count() {
                let variable = v(variable_index as u8);
                let relevant = metadata.relevant_arm_ids(variable).unwrap();
                let Some((&first, rest)) = relevant.split_first() else {
                    continue;
                };
                let mut proposer = first as usize;
                for &candidate in rest {
                    let candidate = candidate as usize;
                    let candidate_key = (
                        estimates[candidate * rows + row],
                        metadata.arms()[candidate].source_pattern_index(),
                    );
                    let proposer_key = (
                        estimates[proposer * rows + row],
                        metadata.arms()[proposer].source_pattern_index(),
                    );
                    if candidate_key < proposer_key {
                        proposer = candidate;
                    }
                }
                let count = estimates[proposer * rows + row];
                let magnitude = u32::BITS - count.leading_zeros();
                let influence = metadata.influence_count(variable).unwrap();
                if best.is_none_or(|(_, _, _, best_magnitude, best_influence)| {
                    magnitude < best_magnitude
                        || (magnitude == best_magnitude && influence > best_influence)
                }) {
                    best = Some((variable, proposer, count, magnitude, influence));
                }
            }
            best.map_or_else(
                ResidentRowChoice::dead,
                |(variable, proposer, count, _, _)| ResidentRowChoice {
                    variable: Some(variable),
                    proposer_arm: Some(proposer),
                    proposal_count: count,
                },
            )
        })
        .collect()
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
    let choices = choices.read().unwrap();
    (viable, estimates, choices)
}

fn repeated_codes(codes: &[u32], rows: usize) -> Vec<u32> {
    (0..rows).map(|row| codes[row % codes.len()]).collect()
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
fn constant_pair_groups_cover_all_rotations_and_block_edges() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let [entity, _, _] = ids.entities;
    let [attribute, _, _] = ids.attributes;
    let [value, _, _] = ids.values;

    for pattern in [
        QueryPattern::new(v(0), constant(attribute), v(1)),
        QueryPattern::new(constant(entity), v(0), v(1)),
        QueryPattern::new(v(0), v(1), constant(value)),
    ] {
        let program = QueryProgram::compile(resident.archive(), 2, [pattern]).unwrap();
        let round = WgpuResidentRound::new(&resident, &program, &[]).unwrap();
        assert_eq!(round.pair_groups.len(), 2);
        for rows in [0usize, 1, 63, 64, 65] {
            let frontier = ProgramFrontier::new(Vec::new(), Vec::new(), rows).unwrap();
            let expected = expected_matrix(&round, &frontier);
            let (viable, estimates, choices) = run_outputs(&round, &frontier);
            assert_eq!(viable, vec![1; rows]);
            assert_eq!(estimates, expected, "constant rows={rows}");
            assert_eq!(
                choices,
                planner_oracle(round.metadata(), &viable, &expected),
                "constant rows={rows}"
            );
        }
    }
}

#[test]
fn column_pair_groups_match_cpu_for_every_rotation_and_shape() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();

    for (bound, peers) in [
        (
            v(0),
            vec![
                code(&program, ids.entities[0]),
                code(&program, ids.entities[1]),
                code(&program, ids.entities[2]),
                code(&program, ids.values[2]),
            ],
        ),
        (
            v(1),
            vec![
                code(&program, ids.attributes[0]),
                code(&program, ids.attributes[1]),
                code(&program, ids.attributes[2]),
                code(&program, ids.entities[0]),
            ],
        ),
        (
            v(2),
            vec![
                code(&program, ids.values[0]),
                code(&program, ids.values[1]),
                code(&program, ids.values[2]),
                code(&program, ids.attributes[0]),
            ],
        ),
    ] {
        let round = WgpuResidentRound::new(&resident, &program, &[bound]).unwrap();
        assert_eq!(round.pair_groups.len(), 2);
        for rows in [0usize, 1, 63, 64, 65] {
            let frontier = program
                .frontier_from_indices(vec![bound], repeated_codes(&peers, rows), rows)
                .unwrap();
            let expected = expected_matrix(&round, &frontier);
            let (viable, estimates, choices) = run_outputs(&round, &frontier);
            assert_eq!(viable, vec![1; rows]);
            assert_eq!(estimates, expected, "bound={bound:?}, rows={rows}");
            assert_eq!(
                choices,
                planner_oracle(round.metadata(), &viable, &expected),
                "bound={bound:?}, rows={rows}"
            );

            for (row, choice) in choices.iter().copied().enumerate() {
                let single = frontier.slice(row..row + 1).unwrap();
                let cpu = program.transition(&single).unwrap();
                if choice.proposal_count == 0 {
                    assert!(cpu.is_empty());
                    assert_ne!(choice, ResidentRowChoice::dead());
                } else {
                    assert_eq!(cpu.len(), 1);
                    assert!(cpu[0].variables().contains(&choice.variable.unwrap()));
                    assert_eq!(cpu[0].len(), choice.proposal_count as usize);
                }
            }
        }
    }
}

#[test]
fn heterogeneous_column_rows_commute_with_every_split() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0)]).unwrap();
    let peer_codes = [
        code(&program, ids.entities[0]),
        code(&program, ids.entities[2]),
        code(&program, ids.values[2]),
        code(&program, ids.entities[1]),
    ];
    let frontier = program
        .frontier_from_indices(vec![v(0)], repeated_codes(&peer_codes, 65), 65)
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
fn duplicate_arms_and_interleaved_rotation_groups_scatter_by_global_arm() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let pattern_a = QueryPattern::new(v(0), constant(ids.attributes[0]), v(1));
    let pattern_e = QueryPattern::new(constant(ids.entities[0]), v(0), v(1));
    let program =
        QueryProgram::compile(resident.archive(), 2, [pattern_a, pattern_e, pattern_a]).unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[]).unwrap();
    assert_eq!(
        round
            .pair_groups
            .iter()
            .map(|group| group.rotation)
            .collect::<Vec<_>>(),
        vec![
            SuccinctRotation::Eav,
            SuccinctRotation::Ave,
            SuccinctRotation::Eva,
            SuccinctRotation::Aev,
        ]
    );

    let frontier = ProgramFrontier::new(Vec::new(), Vec::new(), 65).unwrap();
    let expected = expected_matrix(&round, &frontier);
    let (viable, estimates, choices) = run_outputs(&round, &frontier);
    assert_eq!(estimates, expected);
    assert_eq!(
        choices,
        planner_oracle(round.metadata(), &viable, &expected)
    );
    assert!(choices
        .iter()
        .all(|choice| !matches!(choice.proposer_arm, Some(4 | 5))));
}

#[test]
fn opaque_frontiers_reject_wrong_schema_archive_and_shape() {
    let (set, ids) = fixture_set();
    let first_archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let second_archive = first_archive.clone();
    let first = WgpuSuccinctArchive::new(first_archive).unwrap();
    let second = WgpuSuccinctArchive::new(second_archive).unwrap();
    let first_program =
        QueryProgram::compile(first.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let second_program =
        QueryProgram::compile(second.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let first_round = WgpuResidentRound::new(&first, &first_program, &[v(0)]).unwrap();
    let second_round = WgpuResidentRound::new(&second, &second_program, &[v(0)]).unwrap();
    let host = first_program
        .frontier_from_indices(vec![v(0)], vec![code(&first_program, ids.entities[0])], 1)
        .unwrap();
    let foreign = first_round.upload_frontier(&host).unwrap();
    assert!(matches!(
        second_round.initialize_inputs(&foreign),
        Err(ResidentSupportError::FrontierOwnership)
    ));

    let wrong_schema = WgpuResidentFrontier {
        archive: &first,
        owner: first_round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: first
            .context()
            .upload_u32(&[code(&first_program, ids.attributes[0])])
            .unwrap(),
        variables: vec![v(1)].into_boxed_slice(),
        rows: 1,
        stride: 1,
    };
    assert!(matches!(
        first_round.initialize_inputs(&wrong_schema),
        Err(ResidentSupportError::MalformedFrontier)
    ));

    let malformed_shape = WgpuResidentFrontier {
        archive: &first,
        owner: first_round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: first.context().upload_u32(&[]).unwrap(),
        variables: vec![v(0)].into_boxed_slice(),
        rows: 1,
        stride: 1,
    };
    assert!(matches!(
        first_round.initialize_inputs(&malformed_shape),
        Err(ResidentSupportError::MalformedFrontier)
    ));
}

#[test]
fn malformed_device_codes_poison_estimates_but_valid_zero_stays_viable() {
    let (set, ids) = fixture_set();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let resident = WgpuSuccinctArchive::new(archive).unwrap();
    let program =
        QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))])
            .unwrap();
    let round = WgpuResidentRound::new(&resident, &program, &[v(0)]).unwrap();
    let highest = code(&program, ids.values[2]);
    let domain = resident.archive().domain.len() as u32;
    assert_eq!(highest + 1, domain);
    let frontier = WgpuResidentFrontier {
        archive: &resident,
        owner: round.frontier_owner.clone(),
        lineage: Arc::new(()),
        values: resident
            .context()
            .upload_u32(&[highest, domain, u32::MAX - 1, u32::MAX])
            .unwrap(),
        variables: vec![v(0)].into_boxed_slice(),
        rows: 4,
        stride: 1,
    };
    let (viable, estimates, choices) = run_resident_outputs(&round, &frontier);
    assert_eq!(viable, vec![1; 4]);
    assert_eq!(estimates[0], 0);
    assert_eq!(estimates[4], 0);
    assert_ne!(choices[0], ResidentRowChoice::dead());
    assert_eq!(choices[0].proposal_count, 0);
    for row in 1..4 {
        assert!((0..round.metadata().arms().len())
            .all(|arm| estimates[arm * 4 + row] == DEAD_ROW_SENTINEL));
        assert_eq!(choices[row], ResidentRowChoice::dead());
    }
}

#[test]
fn pair_geometry_excludes_the_reserved_sentinel() {
    let limit = DEAD_ROW_SENTINEL as usize;
    let largest = (limit - 1) / 2;
    assert_eq!(
        pair_group_geometry(1, largest).unwrap(),
        (largest, largest * 2)
    );
    assert!(matches!(
        pair_group_geometry(1, largest + 1),
        Err(ResidentRoundError::GeometryOverflow(
            "resident pair-distinct endpoints"
        ))
    ));
    assert!(matches!(
        pair_group_geometry(usize::MAX, 2),
        Err(ResidentRoundError::GeometryOverflow(
            "resident pair-distinct probes"
        ))
    ));
    assert!(checked_device_product((limit - 1) / 3, 3, "descriptors").is_ok());
    assert!(matches!(
        checked_device_product((limit - 1) / 3 + 1, 3, "descriptors"),
        Err(ResidentRoundError::GeometryOverflow("descriptors"))
    ));
}

#[test]
fn pair_estimates_are_monotonic_after_rebuild_recompile_and_reencode() {
    let e = ordered_id(1);
    let a1 = ordered_id(20);
    let a2 = ordered_id(21);
    let [x, y, z] = [ordered_id(40), ordered_id(41), ordered_id(42)];
    let mut base = TribleSet::new();
    for trible in [
        role_trible(e, a1, x),
        role_trible(e, a1, y),
        role_trible(e, a1, z),
    ] {
        base.insert(&trible);
    }
    let mut extended = base.clone();
    extended.insert(&role_trible(e, a2, x));

    let run = |set: &TribleSet| {
        let archive: SuccinctArchive<OrderedUniverse> = set.into();
        let resident = WgpuSuccinctArchive::new(archive).unwrap();
        // Attribute is v0, value is v1, and the entity peer v2 is bound.
        let program =
            QueryProgram::compile(resident.archive(), 3, [QueryPattern::new(v(2), v(0), v(1))])
                .unwrap();
        let round = WgpuResidentRound::new(&resident, &program, &[v(2)]).unwrap();
        let frontier = program
            .frontier_from_indices(vec![v(2)], vec![code(&program, e)], 1)
            .unwrap();
        let (_, _, choices) = run_outputs(&round, &frontier);
        choices[0]
    };

    let before = run(&base);
    let after = run(&extended);
    assert_eq!(before.variable, Some(v(0)));
    assert_eq!(after.variable, Some(v(0)));
    assert_eq!(before.proposal_count, 1);
    assert_eq!(after.proposal_count, 2);
}
