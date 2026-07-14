use super::*;

use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::trible::TribleSet;

fn v(index: u8) -> ProgramVariable {
    ProgramVariable::new(index)
}

fn empty_archive() -> SuccinctArchive<OrderedUniverse> {
    (&TribleSet::new()).into()
}

fn compile_program<'a>(
    archive: &'a WgpuSuccinctArchive<OrderedUniverse>,
) -> QueryProgram<'a, OrderedUniverse> {
    QueryProgram::compile(archive.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap()
}

fn empty_frontier(rows: usize) -> ProgramFrontier {
    ProgramFrontier::new(Vec::new(), Vec::new(), rows).unwrap()
}

#[test]
fn choices_keep_exact_frontier_lineage_and_low_level_choices_fail_closed() {
    let archive = WgpuSuccinctArchive::new(empty_archive()).unwrap();
    let program = compile_program(&archive);
    let round = WgpuResidentRound::new(&archive, &program, &[]).unwrap();

    let host = empty_frontier(2);
    let frontier_a = round.upload_frontier(&host).unwrap();
    let frontier_b = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier_a).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let forced = round
        .force_choice_words_from_inputs_for_test(
            &frontier_a,
            &inputs,
            &choices.read_words_for_test(),
        )
        .unwrap();

    assert!(round.choice_input_arg(&frontier_a, &choices).is_ok());
    assert!(round.proposal_inputs(&frontier_a, &forced).is_ok());
    assert_eq!(choices.read().unwrap().len(), 2);
    assert!(matches!(
        round.choice_input_arg(&frontier_b, &choices),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));
    assert!(matches!(
        round.proposal_inputs(&frontier_b, &forced),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));

    // The same exact planner can still serve its public low-level reference
    // API, but those inputs deliberately have no frontier lineage and cannot
    // be relabelled as proposal-stage choices.
    let estimates = vec![0; round.metadata().arms().len() * 2];
    let low_inputs = round
        .planner
        .upload_inputs(&[true, true], &estimates)
        .unwrap();
    let low_choices = round.planner.enqueue(&low_inputs).unwrap();
    assert_eq!(low_choices.read().unwrap().len(), 2);
    assert!(matches!(
        round.choice_input_arg(&frontier_a, &low_choices),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));

    let empty_host = empty_frontier(0);
    let empty_resident = round.upload_frontier(&empty_host).unwrap();
    let allocated = round.planner.allocate_inputs(0).unwrap();
    let allocated_choices = round.planner.enqueue(&allocated).unwrap();
    assert!(allocated_choices.read().unwrap().is_empty());
    assert!(matches!(
        round.choice_input_arg(&empty_resident, &allocated_choices),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));
}

#[test]
fn cross_round_and_cross_archive_choices_never_reach_a_device_argument() {
    let archive = WgpuSuccinctArchive::new(empty_archive()).unwrap();
    let program = compile_program(&archive);
    let first = WgpuResidentRound::new(&archive, &program, &[]).unwrap();
    let second = WgpuResidentRound::new(&archive, &program, &[]).unwrap();
    let host = empty_frontier(1);
    let first_frontier = first.upload_frontier(&host).unwrap();
    let second_frontier = second.upload_frontier(&host).unwrap();
    let second_inputs = second.initialize_inputs(&second_frontier).unwrap();
    let second_choices = second.enqueue(&second_inputs).unwrap();

    assert!(matches!(
        first.planner.choice_input_arg(&second_choices),
        Err(ResidentRoundError::ChoiceOwnership)
    ));
    assert!(matches!(
        first.choice_input_arg(&first_frontier, &second_choices),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));
    assert!(matches!(
        first.choice_input_arg(&second_frontier, &second_choices),
        Err(ResidentSupportError::FrontierOwnership)
    ));

    // Equal-looking CPU archives still produce distinct archive/context and
    // planner capabilities. Neither their frontier nor their choices can be
    // consumed by the other resident archive.
    let foreign_archive = WgpuSuccinctArchive::new(empty_archive()).unwrap();
    let foreign_program = compile_program(&foreign_archive);
    let foreign = WgpuResidentRound::new(&foreign_archive, &foreign_program, &[]).unwrap();
    let foreign_frontier = foreign.upload_frontier(&host).unwrap();
    let foreign_inputs = foreign.initialize_inputs(&foreign_frontier).unwrap();
    let foreign_choices = foreign.enqueue(&foreign_inputs).unwrap();

    assert!(matches!(
        first.planner.choice_input_arg(&foreign_choices),
        Err(ResidentRoundError::ChoiceOwnership)
    ));
    assert!(matches!(
        first.choice_input_arg(&first_frontier, &foreign_choices),
        Err(ResidentSupportError::ChoiceFrontierOwnership)
    ));
    assert!(matches!(
        first.choice_input_arg(&foreign_frontier, &foreign_choices),
        Err(ResidentSupportError::FrontierOwnership)
    ));
}
