use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryTerm,
};
use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, SuccinctRotation,
};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::InlineEncoding;
use triblespace_core::trible::{Trible, TribleSet};
use triblespace_gpu::{
    ResidentRoundError, ResidentRowChoice, ResidentRowPlanner, ResidentSupportError,
    WgpuResidentRound, WgpuSuccinctArchive,
};

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

struct Fixture {
    archive: SuccinctArchive<OrderedUniverse>,
    entities: [Id; 5],
    values: [Id; 3],
}

fn fixture() -> Fixture {
    let entities = [
        ordered_id(1),
        ordered_id(2),
        ordered_id(3),
        ordered_id(4),
        ordered_id(5),
    ];
    let attributes = [ordered_id(20), ordered_id(21)];
    let values = [ordered_id(40), ordered_id(41), ordered_id(42)];
    let mut set = TribleSet::new();
    for (index, entity) in entities.iter().copied().enumerate() {
        set.insert(&role_trible(
            entity,
            attributes[index % attributes.len()],
            values[index % values.len()],
        ));
    }
    Fixture {
        archive: (&set).into(),
        entities,
        values,
    }
}

fn v(index: u8) -> ProgramVariable {
    ProgramVariable::new(index)
}

fn constant(id: Id) -> QueryTerm {
    QueryTerm::Constant(GenId::inline_from(id).raw)
}

#[test]
fn facade_rejects_foreign_archive_and_cross_round_inputs() {
    let source = fixture();
    let first_archive = WgpuSuccinctArchive::new(source.archive.clone()).unwrap();
    let second_archive = WgpuSuccinctArchive::new(source.archive).unwrap();
    let program = QueryProgram::compile(
        first_archive.archive(),
        3,
        [QueryPattern::new(v(0), v(1), v(2))],
    )
    .unwrap();
    assert!(matches!(
        WgpuResidentRound::new(&second_archive, &program, &[]),
        Err(ResidentSupportError::ArchiveOwnership)
    ));

    let first = WgpuResidentRound::new(&first_archive, &program, &[]).unwrap();
    let second = WgpuResidentRound::new(&first_archive, &program, &[]).unwrap();
    let inputs = first.initialize_inputs(1).unwrap();
    assert!(matches!(
        second.enqueue(&inputs),
        Err(ResidentRoundError::InputOwnership)
    ));

    let low_level =
        ResidentRowPlanner::with_context(&program, &[], first_archive.context().clone()).unwrap();
    let uninitialized = low_level.allocate_inputs(1).unwrap();
    assert!(matches!(
        first.enqueue(&uninitialized),
        Err(ResidentRoundError::InputOwnership)
    ));
}

#[test]
fn unsupported_physical_specs_fail_before_fabricating_inputs() {
    let source = fixture();
    let archive = WgpuSuccinctArchive::new(source.archive).unwrap();
    let program =
        QueryProgram::compile(archive.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();

    let pair = WgpuResidentRound::new(&archive, &program, &[v(0)]).unwrap();
    assert!(matches!(
        pair.initialize_inputs(1),
        Err(ResidentSupportError::UnsupportedPairDistinctEstimate {
            arm: 0,
            rotation: SuccinctRotation::Eav,
        })
    ));

    let restricted = WgpuResidentRound::new(&archive, &program, &[v(0), v(1)]).unwrap();
    assert!(matches!(
        restricted.initialize_inputs(1),
        Err(ResidentSupportError::UnsupportedRestrictedEstimate {
            arm: 0,
            rotation: SuccinctRotation::Eva,
        })
    ));

    let support = WgpuResidentRound::new(&archive, &program, &[v(0), v(1), v(2)]).unwrap();
    assert!(matches!(
        support.initialize_inputs(1),
        Err(ResidentSupportError::UnsupportedFullyBoundSupport {
            source_pattern_index: 0,
        })
    ));
}

fn cpu_zero_peer_choice(
    program: &QueryProgram<'_, OrderedUniverse>,
    rows: usize,
) -> Option<(ProgramVariable, u32)> {
    let frontier = ProgramFrontier::new(Vec::new(), Vec::new(), rows).unwrap();
    let transitions = program.transition(&frontier).unwrap();
    if rows == 0 {
        assert!(transitions.is_empty());
        return None;
    }
    assert_eq!(transitions.len(), 1);
    let child = &transitions[0];
    assert_eq!(child.variables().len(), 1);
    let count = u32::try_from(child.len() / rows).unwrap();
    assert_eq!(child.len(), count as usize * rows);
    Some((child.variables()[0], count))
}

fn native_choices(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    rows: usize,
) -> Vec<ResidentRowChoice> {
    let inputs = round.initialize_inputs(rows).unwrap();
    round.enqueue(&inputs).unwrap().read().unwrap()
}

#[test]
fn native_zero_peer_initialization_matches_cpu_at_block_boundaries() {
    let source = fixture();
    let archive = WgpuSuccinctArchive::new(source.archive).unwrap();
    let program =
        QueryProgram::compile(archive.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let round = WgpuResidentRound::new(&archive, &program, &[]).unwrap();

    for rows in [0usize, 1, 63, 64, 65] {
        let cpu = cpu_zero_peer_choice(&program, rows);
        let actual = native_choices(&round, rows);
        let expected = cpu.map_or_else(Vec::new, |(variable, count)| {
            let arm = round
                .metadata()
                .arms()
                .iter()
                .position(|candidate| candidate.target_variable() == variable)
                .unwrap();
            vec![
                ResidentRowChoice {
                    variable: Some(variable),
                    proposer_arm: Some(arm),
                    proposal_count: count,
                };
                rows
            ]
        });
        assert_eq!(actual, expected, "row count {rows}");
    }
}

#[test]
fn native_zero_peer_initialization_is_identical_under_every_split() {
    let source = fixture();
    let archive = WgpuSuccinctArchive::new(source.archive).unwrap();
    let program =
        QueryProgram::compile(archive.archive(), 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();
    let round = WgpuResidentRound::new(&archive, &program, &[]).unwrap();
    let whole = native_choices(&round, 65);
    for split in 0..=65 {
        let mut split_choices = native_choices(&round, split);
        split_choices.extend(native_choices(&round, 65 - split));
        assert_eq!(split_choices, whole, "split at {split}");
    }
}

#[test]
fn native_global_dead_initialization_matches_cpu_without_estimates() {
    let source = fixture();
    let archive = WgpuSuccinctArchive::new(source.archive).unwrap();
    let program = QueryProgram::compile(
        archive.archive(),
        3,
        [
            QueryPattern::new(v(0), v(1), v(2)),
            QueryPattern::new(
                constant(source.entities[0]),
                constant(ordered_id(250)),
                constant(source.values[0]),
            ),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&archive, &program, &[]).unwrap();
    for rows in [0usize, 1, 63, 64, 65] {
        let frontier = ProgramFrontier::new(Vec::new(), Vec::new(), rows).unwrap();
        assert!(program.transition(&frontier).unwrap().is_empty());
        assert_eq!(
            native_choices(&round, rows),
            vec![ResidentRowChoice::dead(); rows]
        );
    }
}
