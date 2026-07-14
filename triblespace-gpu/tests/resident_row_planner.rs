use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::trible::TribleSet;
use triblespace_gpu::{
    ResidentRoundError, ResidentRoundMetadata, ResidentRowChoice, ResidentRowPlanner, WgpuContext,
    WgpuResidentRowPlanner,
};

fn variables() -> [ProgramVariable; 5] {
    std::array::from_fn(|index| ProgramVariable::new(index as u8))
}

fn patterns() -> [QueryPattern; 3] {
    let [v0, v1, v2, v3, v4] = variables();
    [
        QueryPattern::new(v0, v1, v2),
        QueryPattern::new(v0, v3, v4),
        QueryPattern::new(v1, v3, v4),
    ]
}

fn with_program<T>(run: impl FnOnce(&QueryProgram<'_, OrderedUniverse>) -> T) -> T {
    let archive: SuccinctArchive<OrderedUniverse> = (&TribleSet::new()).into();
    let program = QueryProgram::compile(&archive, 5, patterns()).unwrap();
    run(&program)
}

fn planner(bound: &[ProgramVariable]) -> WgpuResidentRowPlanner {
    with_program(|program| {
        ResidentRowPlanner::with_context(program, bound, WgpuContext::on_wgpu()).unwrap()
    })
}

fn magnitude(count: u32) -> u32 {
    if count == 0 {
        0
    } else {
        u32::BITS - count.leading_zeros()
    }
}

fn oracle(
    metadata: &ResidentRoundMetadata,
    viable: &[bool],
    estimates: &[u32],
) -> Vec<ResidentRowChoice> {
    let rows = viable.len();
    assert_eq!(estimates.len(), metadata.arms().len() * rows);
    viable
        .iter()
        .enumerate()
        .map(|(row, &is_viable)| {
            if !is_viable {
                return ResidentRowChoice::dead();
            }
            let mut best: Option<(ProgramVariable, usize, u32, u32, u32)> = None;
            for variable_index in 0..metadata.variable_count() {
                let variable = ProgramVariable::new(variable_index as u8);
                let relevant = metadata.relevant_arm_ids(variable).unwrap();
                let Some((&first, rest)) = relevant.split_first() else {
                    continue;
                };
                let mut proposer = first as usize;
                for &arm in rest {
                    let arm = arm as usize;
                    let candidate = (
                        estimates[arm * rows + row],
                        metadata.arms()[arm].source_pattern_index(),
                    );
                    let incumbent = (
                        estimates[proposer * rows + row],
                        metadata.arms()[proposer].source_pattern_index(),
                    );
                    if candidate < incumbent {
                        proposer = arm;
                    }
                }
                let count = estimates[proposer * rows + row];
                let candidate_magnitude = magnitude(count);
                let influence = metadata.influence_count(variable).unwrap();
                if best.is_none_or(|(_, _, _, best_magnitude, best_influence)| {
                    candidate_magnitude < best_magnitude
                        || (candidate_magnitude == best_magnitude && influence > best_influence)
                }) {
                    best = Some((variable, proposer, count, candidate_magnitude, influence));
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

fn run(
    planner: &WgpuResidentRowPlanner,
    viable: &[bool],
    estimates: &[u32],
) -> Vec<ResidentRowChoice> {
    let inputs = planner.upload_inputs(viable, estimates).unwrap();
    planner.enqueue(&inputs).unwrap().read().unwrap()
}

fn set_estimate(matrix: &mut [u32], rows: usize, arm: usize, row: usize, value: u32) {
    matrix[arm * rows + row] = value;
}

fn slice_matrix(matrix: &[u32], arms: usize, rows: usize, start: usize, end: usize) -> Vec<u32> {
    let mut result = Vec::with_capacity(arms * (end - start));
    for arm in 0..arms {
        result.extend_from_slice(&matrix[arm * rows + start..arm * rows + end]);
    }
    result
}

fn generated_matrix(metadata: &ResidentRoundMetadata, rows: usize) -> Vec<u32> {
    let boundary = [0u32, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64];
    let mut matrix = vec![0; metadata.arms().len() * rows];
    for arm in 0..metadata.arms().len() {
        for row in 0..rows {
            matrix[arm * rows + row] = boundary[(arm * 5 + row * 7 + row / 3) % boundary.len()];
        }
    }
    matrix
}

#[test]
fn lowering_is_stable_csr_and_exact_full_influence_union() {
    let v = variables();
    with_program(|program| {
        let metadata = ResidentRoundMetadata::lower(program, &[v[4]]).unwrap();
        let identities: Vec<_> = metadata
            .arms()
            .iter()
            .map(|arm| (arm.source_pattern_index(), arm.target_variable().index()))
            .collect();
        assert_eq!(
            identities,
            vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 3), (2, 1), (2, 3)]
        );
        assert_eq!(metadata.relevant_arm_ids(v[0]).unwrap(), &[0, 3]);
        assert_eq!(metadata.relevant_arm_ids(v[1]).unwrap(), &[1, 5]);
        assert_eq!(metadata.relevant_arm_ids(v[2]).unwrap(), &[2]);
        assert_eq!(metadata.relevant_arm_ids(v[3]).unwrap(), &[4, 6]);
        assert!(metadata.relevant_arm_ids(v[4]).unwrap().is_empty());
        assert_eq!(
            (0..5)
                .map(|index| metadata.influence_count(v[index]).unwrap())
                .collect::<Vec<_>>(),
            vec![4, 4, 2, 3, 3]
        );
    });
}

#[test]
fn native_kernel_matches_oracle_for_mixed_choices_flips_ties_and_boundaries() {
    let v = variables();
    let planner = planner(&[v[4]]);
    let metadata = planner.metadata();
    let rows = 9;
    let mut estimates = vec![32; metadata.arms().len() * rows];
    let viable = [true, true, true, true, true, true, true, false, true];

    // row 0: all exact ties -> source pattern zero proposes; v0 wins the
    // equal magnitude/influence variable tie by lower ID.
    for arm in 0..metadata.arms().len() {
        set_estimate(&mut estimates, rows, arm, 0, 8);
    }
    // row 1: v0's proposer flips from pattern 0 (arm 0) to pattern 1 (arm 3).
    set_estimate(&mut estimates, rows, 0, 1, 9);
    set_estimate(&mut estimates, rows, 3, 1, 3);
    // row 2: v1's proposer flips to pattern 2, and v1 is the next variable.
    set_estimate(&mut estimates, rows, 1, 2, 10);
    set_estimate(&mut estimates, rows, 5, 2, 2);
    // row 3: v2 and v3 have equal magnitude; v3 wins by larger influence.
    for arm in [0, 1, 3, 5] {
        set_estimate(&mut estimates, rows, arm, 3, 4);
    }
    set_estimate(&mut estimates, rows, 2, 3, 2);
    set_estimate(&mut estimates, rows, 4, 3, 2);
    set_estimate(&mut estimates, rows, 6, 3, 3);
    // row 4: exact zero has magnitude zero and wins despite lower influence.
    set_estimate(&mut estimates, rows, 2, 4, 0);
    for arm in [0, 1, 3, 4, 5, 6] {
        set_estimate(&mut estimates, rows, arm, 4, 1);
    }
    // row 5: 3 and 4 straddle a power-of-two magnitude boundary.
    for arm in [0, 3] {
        set_estimate(&mut estimates, rows, arm, 5, 4);
    }
    for arm in [1, 5] {
        set_estimate(&mut estimates, rows, arm, 5, 3);
    }
    // row 6: exact counts 2 and 3 share one magnitude. Equal influence keeps
    // v0 even though v1 has a different exact count.
    for arm in [0, 3] {
        set_estimate(&mut estimates, rows, arm, 6, 2);
    }
    for arm in [1, 5] {
        set_estimate(&mut estimates, rows, arm, 6, 3);
    }
    // row 8 duplicates row 2 exactly; duplicate affine rows stay duplicated.
    for arm in 0..metadata.arms().len() {
        let value = estimates[arm * rows + 2];
        set_estimate(&mut estimates, rows, arm, 8, value);
    }

    let expected = oracle(metadata, &viable, &estimates);
    let actual = run(&planner, &viable, &estimates);
    assert_eq!(actual, expected);
    assert_eq!(actual[0].variable, Some(v[0]));
    assert_eq!(actual[0].proposer_arm, Some(0));
    assert_eq!(actual[1].proposer_arm, Some(3));
    assert_eq!(actual[2].variable, Some(v[1]));
    assert_eq!(actual[2].proposer_arm, Some(5));
    assert_eq!(actual[3].variable, Some(v[3]));
    assert_eq!(actual[4].variable, Some(v[2]));
    assert_eq!(actual[4].proposal_count, 0);
    assert_eq!(actual[5].variable, Some(v[1]));
    assert_eq!(actual[6].variable, Some(v[0]));
    assert_eq!(actual[7], ResidentRowChoice::dead());
    assert_eq!(actual[8], actual[2]);
}

#[test]
fn reserved_estimate_in_a_proposer_or_non_proposer_arm_kills_the_row() {
    let planner = planner(&[variables()[4]]);
    let rows = 3;
    let mut estimates = vec![8; planner.metadata().arms().len() * rows];

    // Arm 2 is v2's sole arm, hence necessarily that variable's proposer.
    set_estimate(&mut estimates, rows, 2, 0, u32::MAX);
    // For v0, arm 0 remains the proposer while the looser arm 3 is poisoned.
    set_estimate(&mut estimates, rows, 0, 1, 1);
    set_estimate(&mut estimates, rows, 3, 1, u32::MAX);

    let choices = run(&planner, &[true, true, true], &estimates);
    assert_eq!(choices[0], ResidentRowChoice::dead());
    assert_eq!(choices[1], ResidentRowChoice::dead());
    assert_ne!(choices[2], ResidentRowChoice::dead());
}

#[test]
fn native_block_edges_and_every_split_are_row_homomorphic() {
    let planner = planner(&[variables()[4]]);
    let arms = planner.metadata().arms().len();
    for rows in [0usize, 1, 63, 64, 65] {
        let viable: Vec<_> = (0..rows).map(|row| row % 11 != 3).collect();
        let estimates = generated_matrix(planner.metadata(), rows);
        assert_eq!(
            run(&planner, &viable, &estimates),
            oracle(planner.metadata(), &viable, &estimates),
            "row count {rows}"
        );
    }

    let rows = 65;
    let viable: Vec<_> = (0..rows).map(|row| row % 11 != 3).collect();
    let estimates = generated_matrix(planner.metadata(), rows);
    let whole = run(&planner, &viable, &estimates);
    for split in 1..rows {
        let left_matrix = slice_matrix(&estimates, arms, rows, 0, split);
        let right_matrix = slice_matrix(&estimates, arms, rows, split, rows);
        let mut split_result = run(&planner, &viable[..split], &left_matrix);
        split_result.extend(run(&planner, &viable[split..], &right_matrix));
        assert_eq!(split_result, whole, "split at row {split}");
    }
}

#[test]
fn bound_and_complete_schemas_never_reselect_bound_variables() {
    let v = variables();
    let partly_bound = planner(&[v[0], v[4]]);
    let rows = 65;
    let viable = vec![true; rows];
    let estimates = generated_matrix(partly_bound.metadata(), rows);
    let choices = run(&partly_bound, &viable, &estimates);
    assert!(choices.iter().all(|choice| choice.variable != Some(v[0])));
    assert!(choices.iter().all(|choice| choice.variable != Some(v[4])));

    let complete = planner(&v);
    assert!(complete.metadata().arms().is_empty());
    assert_eq!(
        run(&complete, &[true, false, true], &[]),
        vec![ResidentRowChoice::dead(); 3]
    );
}

#[test]
fn malformed_schema_dimensions_and_cross_planner_inputs_fail_closed() {
    let v = variables();
    with_program(|program| {
        assert!(matches!(
            ResidentRoundMetadata::lower(program, &[v[1], v[0]]),
            Err(ResidentRoundError::NonCanonicalFrontierSchema)
        ));
        assert!(matches!(
            ResidentRoundMetadata::lower(program, &[v[1], v[1]]),
            Err(ResidentRoundError::NonCanonicalFrontierSchema)
        ));
        assert!(matches!(
            ResidentRoundMetadata::lower(program, &[ProgramVariable::new(5)]),
            Err(ResidentRoundError::VariableOutOfBounds { .. })
        ));

        let context = WgpuContext::on_wgpu();
        let first = ResidentRowPlanner::with_context(program, &[v[4]], context.clone()).unwrap();
        let second = ResidentRowPlanner::with_context(program, &[v[4]], context).unwrap();
        assert!(matches!(
            first.upload_inputs(&[true, true], &[0; 13]),
            Err(ResidentRoundError::EstimateMatrixShape {
                rows: 2,
                arms: 7,
                estimates: 13
            })
        ));
        let estimates = generated_matrix(first.metadata(), 2);
        let inputs = first.upload_inputs(&[true, true], &estimates).unwrap();
        assert!(matches!(
            second.enqueue(&inputs),
            Err(ResidentRoundError::InputOwnership)
        ));
    });
}
