use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::resident_ordered_oracle::{witnessed_transition, OrderedSegment};
use triblespace_core::blob::encodings::succinctarchive::query_program::{QueryPattern, QueryTerm};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::trible::{Trible, TribleSet};

fn ordered_id(axis: u8, ordinal: u16) -> Id {
    let mut raw = [0u8; 16];
    raw[..2].copy_from_slice(&ordinal.to_be_bytes());
    raw[2] = axis + 1;
    Id::new(raw).expect("fixture IDs are non-zero")
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

struct Fixture {
    set: TribleSet,
    entities: [Id; 3],
    attributes: [Id; 2],
    values: [Id; 3],
}

fn fixture() -> Fixture {
    // Axis code spaces are deliberately interleaved in OrderedUniverse.
    let entities = [ordered_id(0, 1), ordered_id(0, 5), ordered_id(0, 9)];
    let attributes = [ordered_id(1, 2), ordered_id(1, 6)];
    let values = [ordered_id(2, 3), ordered_id(2, 7), ordered_id(2, 10)];
    let mut set = TribleSet::new();
    insert(&mut set, entities[0], attributes[0], values[0]);
    insert(&mut set, entities[1], attributes[0], values[1]);
    insert(&mut set, entities[2], attributes[1], values[2]);
    Fixture {
        set,
        entities,
        attributes,
        values,
    }
}

fn required(segments: &[OrderedSegment]) -> usize {
    segments.iter().map(|segment| segment.frontier.len()).sum()
}

fn inspect_wired(
    wired: &WgpuResidentWiredRound<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
    capacity: usize,
) -> ProposalInspection {
    let resident = wired.upload_frontier(frontier).unwrap();
    wired.enqueue(&resident, capacity).unwrap().inspect()
}

fn assert_success(inspection: &ProposalInspection, expected: usize) {
    assert_eq!(inspection.status, STATUS_OK);
    assert_eq!(inspection.required, expected as u32);
    assert_eq!(inspection.logical_len, expected as u32);
    assert_eq!(inspection.capacity, inspection.candidate_codes.len() as u32);
    if expected == 0 {
        assert_eq!((inspection.dispatch_x, inspection.dispatch_y), (0, 1));
    } else {
        assert_ne!(inspection.dispatch_x, 0);
        assert_ne!(inspection.dispatch_y, 0);
    }
}

fn assert_matches_witnessed(
    wired: &WgpuResidentWiredRound<'_, OrderedUniverse>,
    program: &QueryProgram<'_, OrderedUniverse>,
    parent: &ProgramFrontier,
    inspection: &ProposalInspection,
) {
    let expected = witnessed_transition(program, 0, parent, 0).unwrap();
    assert_success(inspection, required(&expected));

    let live_records = inspection
        .segments
        .iter()
        .filter(|record| record.count != 0)
        .count();
    assert_eq!(live_records, expected.len());
    for segment in expected {
        let variable = segment.variable.expect("child segment variable");
        let record = inspection
            .segments
            .iter()
            .find(|record| record.variable == variable.index() as u32)
            .expect("wired arena contains every unbound-variable segment");
        let start = record.base as usize;
        let end = start + record.count as usize;
        let expected_body = segment
            .frontier
            .values()
            .iter()
            .map(|code| code.get())
            .collect::<Vec<_>>();
        let body_start = start * inspection.child_stride as usize;
        let body_end = end * inspection.child_stride as usize;
        assert_eq!(&inspection.child_body[body_start..body_end], expected_body);
        assert_eq!(
            &inspection.candidate_codes[start..end],
            &segment
                .candidates
                .iter()
                .map(|witness| witness.code.get())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            &inspection.candidate_owners[start..end],
            &segment
                .candidates
                .iter()
                .map(|witness| witness.parent_row as u32)
                .collect::<Vec<_>>()
        );

        let relevant = wired
            .staged_round()
            .metadata()
            .relevant_arm_ids(variable)
            .unwrap();
        assert_eq!(
            relevant.len(),
            1,
            "exact pre-confirmation parity requires one relevant arm"
        );
        assert_eq!(
            &inspection.proposer_arms[start..end],
            &vec![relevant[0]; end - start]
        );
    }
}

#[test]
fn wired_single_pattern_matches_manual_stages_and_all_axis_permutations() {
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [2, 0, 1],
        [1, 2, 0],
        [2, 1, 0],
    ];

    for (case, axes) in permutations.into_iter().enumerate() {
        let fixture = fixture();
        let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
        let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
        let variables = axes.map(ProgramVariable::new);
        let program = QueryProgram::compile(
            gpu.archive(),
            3,
            [QueryPattern::new(variables[0], variables[1], variables[2])],
        )
        .unwrap();
        let wired = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
        let seed = ProgramFrontier::seed();
        let expected = witnessed_transition(&program, 0, &seed, 0).unwrap();
        let capacity = required(&expected);
        let resident = wired.upload_frontier(&seed).unwrap();
        let fused = wired.enqueue(&resident, capacity).unwrap().inspect();
        assert_matches_witnessed(&wired, &program, &seed, &fused);

        // The first permutation additionally freezes exact byte parity against
        // the formerly manual chain, including records, owner tags and tails.
        if case == 0 {
            let round = wired.staged_round();
            let inputs = round.initialize_inputs(&resident).unwrap();
            let choices = round.enqueue(&inputs).unwrap();
            let staged = round
                .enqueue_present_proposals(&resident, &choices, capacity)
                .unwrap()
                .inspect();
            assert_eq!(fused, staged);
        }
    }
}

fn support_program<'a>(
    gpu: &'a crate::WgpuSuccinctArchive<OrderedUniverse>,
) -> QueryProgram<'a, OrderedUniverse> {
    QueryProgram::compile(
        gpu.archive(),
        6,
        [
            QueryPattern::new(
                ProgramVariable::new(1),
                ProgramVariable::new(3),
                ProgramVariable::new(5),
            ),
            QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(2),
                ProgramVariable::new(4),
            ),
        ],
    )
    .unwrap()
}

fn support_frontier(
    program: &QueryProgram<'_, OrderedUniverse>,
    fixture: &Fixture,
    rows: usize,
) -> ProgramFrontier {
    let sources = [
        (
            fixture.entities[0],
            fixture.attributes[0],
            fixture.values[0],
        ),
        // Every code is valid, but this exact triple is absent.
        (
            fixture.entities[0],
            fixture.attributes[1],
            fixture.values[0],
        ),
        (
            fixture.entities[1],
            fixture.attributes[0],
            fixture.values[1],
        ),
        // Duplicate the first live parent after a dead gap.
        (
            fixture.entities[0],
            fixture.attributes[0],
            fixture.values[0],
        ),
    ];
    let values = (0..rows)
        .flat_map(|row| {
            let (entity, attribute, value) = sources[row % sources.len()];
            [entity, attribute, value].map(|id| program.encode(&raw(id)).unwrap().get())
        })
        .collect();
    program
        .frontier_from_indices(
            vec![
                ProgramVariable::new(1),
                ProgramVariable::new(3),
                ProgramVariable::new(5),
            ],
            values,
            rows,
        )
        .unwrap()
}

type SegmentContents = (Vec<u32>, Vec<u32>, Vec<u32>, Vec<Vec<u32>>);
type SegmentMap = BTreeMap<u32, SegmentContents>;

fn live_segments(inspection: &ProposalInspection) -> SegmentMap {
    let mut result = BTreeMap::new();
    for record in inspection
        .segments
        .iter()
        .filter(|record| record.count != 0)
    {
        let start = record.base as usize;
        let end = start + record.count as usize;
        let body_start = start * inspection.child_stride as usize;
        let body_end = end * inspection.child_stride as usize;
        result.insert(
            record.variable,
            (
                inspection.candidate_codes[start..end].to_vec(),
                inspection.candidate_owners[start..end].to_vec(),
                inspection.proposer_arms[start..end].to_vec(),
                inspection.child_body[body_start..body_end]
                    .chunks_exact(inspection.child_stride as usize)
                    .map(<[u32]>::to_vec)
                    .collect(),
            ),
        );
    }
    result
}

fn concatenate_segments(
    left: &ProposalInspection,
    right: &ProposalInspection,
    right_owner_base: u32,
) -> SegmentMap {
    let mut combined = live_segments(left);
    for (variable, (codes, owners, arms, bodies)) in live_segments(right) {
        let entry = combined.entry(variable).or_default();
        entry.0.extend(codes);
        entry
            .1
            .extend(owners.into_iter().map(|owner| owner + right_owner_base));
        entry.2.extend(arms);
        entry.3.extend(bodies);
    }
    combined
}

fn inspect_exact(
    wired: &WgpuResidentWiredRound<'_, OrderedUniverse>,
    program: &QueryProgram<'_, OrderedUniverse>,
    frontier: &ProgramFrontier,
) -> ProposalInspection {
    let expected = witnessed_transition(program, 0, frontier, 0).unwrap();
    let inspection = inspect_wired(wired, frontier, required(&expected));
    assert_matches_witnessed(wired, program, frontier, &inspection);
    inspection
}

#[test]
fn wired_real_support_is_row_homomorphic_across_all_block_edges_and_gaps() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let program = support_program(&gpu);
    let bound = [
        ProgramVariable::new(1),
        ProgramVariable::new(3),
        ProgramVariable::new(5),
    ];
    let wired = WgpuResidentWiredRound::new(&gpu, &program, &bound).unwrap();
    let full = support_frontier(&program, &fixture, 65);

    for rows in [0usize, 1, 63, 64, 65] {
        let prefix = full.slice(0..rows).unwrap();
        inspect_exact(&wired, &program, &prefix);
    }

    let whole = inspect_exact(&wired, &program, &full);
    let whole_segments = live_segments(&whole);
    for split in 0..=full.len() {
        let left_frontier = full.slice(0..split).unwrap();
        let right_frontier = full.slice(split..full.len()).unwrap();
        let left = inspect_exact(&wired, &program, &left_frontier);
        let right = inspect_exact(&wired, &program, &right_frontier);
        assert_eq!(
            concatenate_segments(&left, &right, split as u32),
            whole_segments,
            "consecutive split {split}"
        );
    }

    // The absent support rows are precisely the 1 mod 4 gaps; duplicate live
    // parents on rows 0 and 3 remain distinct owners rather than deduplicating.
    for (_, owners, _, _) in whole_segments.values() {
        assert!(owners.iter().all(|owner| owner % 4 != 1));
        assert!(owners.contains(&0));
        assert!(owners.contains(&3));
    }
}

#[test]
fn wired_capacity_is_atomic_and_preflight_rejects_unrepresentable_geometry() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let variables = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(variables[0], variables[1], variables[2])],
    )
    .unwrap();
    let wired = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
    let seed = ProgramFrontier::seed();
    let expected = witnessed_transition(&program, 0, &seed, 0).unwrap();
    let needed = required(&expected);
    assert!(needed > 0);

    let exact = inspect_wired(&wired, &seed, needed);
    assert_matches_witnessed(&wired, &program, &seed, &exact);

    for capacity in [needed - 1, 0] {
        let failed = inspect_wired(&wired, &seed, capacity);
        assert_eq!(failed.status, STATUS_CAPACITY);
        assert_eq!(failed.required, needed as u32);
        assert_eq!(failed.logical_len, 0);
        assert!(failed.segments.iter().all(|record| {
            record.base == RESIDENT_U32_SENTINEL
                && record.count == RESIDENT_U32_SENTINEL
                && record.variable == RESIDENT_U32_SENTINEL
                && record.insertion == RESIDENT_U32_SENTINEL
        }));
        assert!(failed
            .candidate_codes
            .iter()
            .chain(&failed.candidate_owners)
            .chain(&failed.proposer_arms)
            .chain(&failed.child_body)
            .all(|word| *word == RESIDENT_U32_SENTINEL));
    }

    let resident = wired.upload_frontier(&seed).unwrap();
    assert!(matches!(
        wired.enqueue(&resident, RESIDENT_U32_SENTINEL as usize),
        Err(ResidentProposalError::GeometryOverflow(
            "candidate capacity"
        ))
    ));
}

#[test]
fn wired_admission_rejects_terminal_and_non_present_but_keeps_semantic_death() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let [entity, attribute, value] = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];

    let q0 = QueryProgram::compile(
        gpu.archive(),
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(fixture.entities[0])),
            QueryTerm::Constant(raw(fixture.attributes[0])),
            QueryTerm::Constant(raw(fixture.values[0])),
        )],
    )
    .unwrap();
    assert!(matches!(
        WgpuResidentWiredRound::new(&gpu, &q0, &[]),
        Err(ResidentProposalError::TerminalSchema)
    ));
    let q0_absent = QueryProgram::compile(
        gpu.archive(),
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(fixture.entities[0])),
            QueryTerm::Constant(raw(fixture.attributes[1])),
            QueryTerm::Constant(raw(fixture.values[0])),
        )],
    )
    .unwrap();
    assert!(matches!(
        WgpuResidentWiredRound::new(&gpu, &q0_absent, &[]),
        Err(ResidentProposalError::TerminalSchema)
    ));

    let complete = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    assert!(matches!(
        WgpuResidentWiredRound::new(&gpu, &complete, &[entity, attribute, value]),
        Err(ResidentProposalError::TerminalSchema)
    ));
    assert!(matches!(
        WgpuResidentWiredRound::new(&gpu, &complete, &[entity]),
        Err(ResidentProposalError::UnsupportedProposer { .. })
    ));

    let missing = ordered_id(9, 999);
    let dead = QueryProgram::compile(
        gpu.archive(),
        2,
        [QueryPattern::new(
            ProgramVariable::new(0),
            QueryTerm::Constant(raw(missing)),
            ProgramVariable::new(1),
        )],
    )
    .unwrap();
    let dead_wired = WgpuResidentWiredRound::new(&gpu, &dead, &[]).unwrap();
    let dead_result = inspect_wired(&dead_wired, &ProgramFrontier::seed(), 0);
    assert_success(&dead_result, 0);
    assert!(dead_result
        .segments
        .iter()
        .all(|record| record.base == 0 && record.count == 0));

    let empty_archive: SuccinctArchive<OrderedUniverse> = (&TribleSet::new()).into();
    let empty_gpu = crate::WgpuSuccinctArchive::new(empty_archive).unwrap();
    let empty_program = QueryProgram::compile(
        empty_gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    let empty_wired = WgpuResidentWiredRound::new(&empty_gpu, &empty_program, &[]).unwrap();
    let empty = inspect_wired(&empty_wired, &ProgramFrontier::seed(), 0);
    assert_success(&empty, 0);
    assert!(empty
        .segments
        .iter()
        .all(|record| record.base == 0 && record.count == 0));
}

#[test]
fn wired_capabilities_reject_foreign_frontiers_and_reuse_one_schema_safely() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let variables = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(variables[0], variables[1], variables[2])],
    )
    .unwrap();
    let wired_a = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
    let wired_b = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
    let seed = ProgramFrontier::seed();
    let capacity = required(&witnessed_transition(&program, 0, &seed, 0).unwrap());
    let frontier_a0 = wired_a.upload_frontier(&seed).unwrap();
    let frontier_a1 = wired_a.upload_frontier(&seed).unwrap();
    let frontier_b = wired_b.upload_frontier(&seed).unwrap();

    assert!(matches!(
        wired_a.enqueue(&frontier_b, capacity),
        Err(ResidentProposalError::Support(
            ResidentSupportError::FrontierOwnership
        ))
    ));

    let arena_a0 = wired_a.enqueue(&frontier_a0, capacity).unwrap();
    let arena_a1 = wired_a.enqueue(&frontier_a1, capacity).unwrap();
    assert!(!Arc::ptr_eq(
        &arena_a0.frontier_lineage,
        &arena_a1.frontier_lineage
    ));
    assert!(!Arc::ptr_eq(
        &arena_a0.arena_lineage,
        &arena_a1.arena_lineage
    ));
    assert_success(&arena_a0.inspect(), capacity);
    assert_success(&arena_a1.inspect(), capacity);
}

#[test]
fn shared_variable_present_arms_are_explicitly_a_preconfirmation_superset() {
    let entity = ordered_id(0, 1);
    let value = ordered_id(2, 1);
    let attributes = [ordered_id(1, 1), ordered_id(1, 2)];
    let mut set = TribleSet::new();
    insert(&mut set, entity, attributes[0], value);
    insert(&mut set, entity, attributes[1], value);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let variables = (0..5).map(ProgramVariable::new).collect::<Vec<_>>();
    let program = QueryProgram::compile(
        gpu.archive(),
        5,
        [
            QueryPattern::new(variables[0], variables[1], variables[2]),
            QueryPattern::new(variables[3], variables[4], variables[0]),
        ],
    )
    .unwrap();
    let wired = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
    let seed = ProgramFrontier::seed();
    let resident = wired.upload_frontier(&seed).unwrap();
    let provisional = wired.enqueue(&resident, 4).unwrap().inspect();
    assert_success(&provisional, 1);

    let confirmed = program.transition(&seed).unwrap();
    assert!(confirmed.is_empty());
    assert_eq!(
        provisional.segments[0].variable,
        variables[0].index() as u32
    );
    assert_eq!(provisional.segments[0].count, 1);
}

fn extension_set(include_second_entity: bool) -> (TribleSet, Id, Id) {
    let first = ordered_id(0, 1);
    let second = ordered_id(0, 2);
    let mut set = TribleSet::new();
    for ordinal in 0..10u16 {
        insert(
            &mut set,
            first,
            ordered_id(1, ordinal + 1),
            ordered_id(2, ordinal + 1),
        );
    }
    if include_second_entity {
        insert(&mut set, second, ordered_id(1, 1), ordered_id(2, 1));
    }
    (set, first, second)
}

fn decoded_entity_candidates(set: &TribleSet) -> BTreeSet<RawInline> {
    let archive: SuccinctArchive<OrderedUniverse> = set.into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let variables = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(variables[0], variables[1], variables[2])],
    )
    .unwrap();
    let wired = WgpuResidentWiredRound::new(&gpu, &program, &[]).unwrap();
    let seed = ProgramFrontier::seed();
    let expected = witnessed_transition(&program, 0, &seed, 0).unwrap();
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0].variable, Some(variables[0]));
    let inspection = inspect_wired(&wired, &seed, required(&expected));
    assert_matches_witnessed(&wired, &program, &seed, &inspection);
    let record = inspection
        .segments
        .iter()
        .find(|record| record.variable == variables[0].index() as u32)
        .unwrap();
    let start = record.base as usize;
    let end = start + record.count as usize;
    let frontier = program
        .frontier_from_indices(
            vec![variables[0]],
            inspection.candidate_codes[start..end].to_vec(),
            end - start,
        )
        .unwrap();
    program
        .decode_frontier(&frontier)
        .unwrap()
        .into_iter()
        .map(|row| row[0])
        .collect()
}

#[test]
fn confirmation_free_extension_is_monotone_after_decoding_snapshot_local_codes() {
    let (before, first, second) = extension_set(false);
    let (after, _, _) = extension_set(true);
    let before_candidates = decoded_entity_candidates(&before);
    let after_candidates = decoded_entity_candidates(&after);
    assert!(before_candidates.is_subset(&after_candidates));
    assert_eq!(before_candidates, BTreeSet::from([raw(first)]));
    assert_eq!(after_candidates, BTreeSet::from([raw(first), raw(second)]));
}
