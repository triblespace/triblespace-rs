use std::collections::BTreeMap;

use super::*;
use triblespace_core::blob::encodings::succinctarchive::query_program::{
    ProgramFrontier, QueryPattern, QueryProgram, QueryTerm,
};
use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::trible::{Trible, TribleSet};

fn ordered_id(prefix: u8) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    Id::new(raw).expect("fixture IDs are non-zero")
}

fn benchmark_id(axis: u8, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = axis + 1;
    raw[8..].copy_from_slice(&ordinal.to_be_bytes());
    Id::new(raw).expect("benchmark IDs are non-zero")
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

type SegmentContents = (Vec<u32>, Vec<u32>, Vec<u32>, Vec<Vec<u32>>);
type SegmentMap = BTreeMap<u32, SegmentContents>;

fn fixture() -> Fixture {
    // Interleave the three semantic axes in the ordered universe. Their
    // resident present lists are E=[0,3,6], A=[1,4], V=[2,5,7], proving that
    // Present generation cannot enumerate 0..axis_count.
    let entities = [ordered_id(1), ordered_id(5), ordered_id(9)];
    let attributes = [ordered_id(2), ordered_id(6)];
    let values = [ordered_id(3), ordered_id(7), ordered_id(10)];
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

fn codes<U: Universe>(program: &QueryProgram<'_, U>, ids: &[Id]) -> Vec<u32> {
    ids.iter()
        .map(|&id| program.encode(&raw(id)).unwrap().get())
        .collect()
}

fn live_segments(inspection: &ProposalInspection) -> SegmentMap {
    let mut result = BTreeMap::new();
    for segment in &inspection.segments {
        let start = segment.base as usize;
        let end = start + segment.count as usize;
        let bodies = inspection.child_body
            [start * inspection.child_stride as usize..end * inspection.child_stride as usize]
            .chunks_exact(inspection.child_stride as usize)
            .map(<[u32]>::to_vec)
            .collect();
        result.insert(
            segment.variable,
            (
                inspection.candidate_codes[start..end].to_vec(),
                inspection.candidate_owners[start..end].to_vec(),
                inspection.proposer_arms[start..end].to_vec(),
                bodies,
            ),
        );
    }
    result
}

fn assert_success(inspection: &ProposalInspection, expected: usize) {
    assert_eq!(inspection.status, STATUS_OK);
    assert_eq!(inspection.required, expected as u32);
    assert_eq!(inspection.logical_len, expected as u32);
    if expected == 0 {
        assert_eq!((inspection.dispatch_x, inspection.dispatch_y), (0, 1));
    } else {
        assert_ne!(inspection.dispatch_x, 0);
        assert_ne!(inspection.dispatch_y, 0);
    }
    assert_eq!(
        inspection.capacity as usize,
        inspection.candidate_codes.len()
    );
}

#[test]
fn indirect_child_materialization_covers_zero_one_block_edge_and_forced_2d_fold() {
    for (entities, expected_dispatch) in [(0usize, (0, 1)), (1, (1, 1)), (64, (1, 1)), (65, (1, 2))]
    {
        let attribute = ordered_id(200);
        let value = ordered_id(201);
        let mut set = TribleSet::new();
        for prefix in 1..=entities {
            insert(&mut set, ordered_id(prefix as u8), attribute, value);
        }
        let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
        let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
        let v0 = ProgramVariable::new(0);
        let v1 = ProgramVariable::new(1);
        let v2 = ProgramVariable::new(2);
        let program =
            QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
        let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
        let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
        let entity_arm = round
            .metadata()
            .arms()
            .iter()
            .position(|identity| identity.target_variable() == v0)
            .unwrap() as u32;
        let choices = round
            .upload_choice_words_for_test(
                &frontier,
                &[v0.index() as u32, entity_arm, entities as u32],
            )
            .unwrap();
        let capacity = if entities == 65 { 128 } else { entities };
        let arena = if entities == 65 {
            round
                .enqueue_present_proposals_with_dispatch_limits_for_test(
                    &frontier, &choices, capacity, 1, 2,
                )
                .unwrap()
        } else {
            round
                .enqueue_present_proposals(&frontier, &choices, capacity)
                .unwrap()
        };
        let inspection = arena.inspect();
        assert_success(&inspection, entities);
        assert_eq!(
            (inspection.dispatch_x, inspection.dispatch_y),
            expected_dispatch
        );
        assert_eq!(inspection.child_stride, 1);
        let expected_codes: Vec<_> = (1..=entities)
            .map(|ordinal| {
                program
                    .encode(&raw(ordered_id(ordinal as u8)))
                    .unwrap()
                    .get()
            })
            .collect();
        assert_eq!(&inspection.candidate_codes[..entities], expected_codes);
        assert_eq!(&inspection.candidate_owners[..entities], &vec![0; entities]);
        assert_eq!(
            &inspection.proposer_arms[..entities],
            &vec![entity_arm; entities]
        );
        assert_eq!(&inspection.child_body[..entities], expected_codes);
        assert!(inspection.candidate_codes[entities..]
            .iter()
            .chain(&inspection.candidate_owners[entities..])
            .chain(&inspection.proposer_arms[entities..])
            .chain(&inspection.child_body[entities..])
            .all(|&word| word == RESIDENT_U32_SENTINEL));
    }
}

#[test]
fn destination_generator_forced_3x3_flattens_513_unique_destinations() {
    const TOTAL: usize = 513;
    const CAPACITY: usize = 576;
    let attribute = benchmark_id(1, 0);
    let value = benchmark_id(2, 0);
    let mut set = TribleSet::new();
    for ordinal in 0..TOTAL {
        insert(&mut set, benchmark_id(0, ordinal as u64), attribute, value);
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == v0)
        .unwrap() as u32;
    let choices = round
        .upload_choice_words_for_test(&frontier, &[v0.index() as u32, arm, TOTAL as u32])
        .unwrap();
    let inspection = round
        .enqueue_present_proposals_with_dispatch_limits_for_test(
            &frontier, &choices, CAPACITY, 3, 3,
        )
        .unwrap()
        .inspect();
    assert_success(&inspection, TOTAL);
    assert_eq!((inspection.dispatch_x, inspection.dispatch_y), (3, 3));
    let expected: Vec<_> = (0..TOTAL)
        .map(|ordinal| {
            program
                .encode(&raw(benchmark_id(0, ordinal as u64)))
                .unwrap()
                .get()
        })
        .collect();
    assert_eq!(&inspection.candidate_codes[..TOTAL], expected);
    assert_eq!(&inspection.candidate_owners[..TOTAL], &vec![0; TOTAL]);
    assert_eq!(&inspection.proposer_arms[..TOTAL], &vec![arm; TOTAL]);
    assert_eq!(&inspection.child_body[..TOTAL], expected);
    assert!(inspection.candidate_codes[TOTAL..]
        .iter()
        .chain(&inspection.candidate_owners[TOTAL..])
        .chain(&inspection.proposer_arms[TOTAL..])
        .chain(&inspection.child_body[TOTAL..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn destination_row_inversion_skips_long_zero_runs_across_cells_63_64_65() {
    const ROWS: usize = 192;
    const CAPACITY: usize = 8;
    const LIVE_ROWS: [usize; 4] = [63, 64, 65, 130];
    let entity = benchmark_id(0, 0);
    let attribute = benchmark_id(1, 0);
    let value = benchmark_id(2, 0);
    let mut set = TribleSet::new();
    insert(&mut set, entity, attribute, value);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let bound = vec![
        ProgramVariable::new(1),
        ProgramVariable::new(3),
        ProgramVariable::new(5),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        6,
        [
            QueryPattern::new(bound[0], bound[1], bound[2]),
            QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(2),
                ProgramVariable::new(4),
            ),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let parent = [
        program.encode(&raw(entity)).unwrap().get(),
        program.encode(&raw(attribute)).unwrap().get(),
        program.encode(&raw(value)).unwrap().get(),
    ];
    let host_frontier = program
        .frontier_from_indices(bound, (0..ROWS).flat_map(|_| parent).collect(), ROWS)
        .unwrap();
    let frontier = round.upload_frontier(&host_frontier).unwrap();
    let arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == ProgramVariable::new(0))
        .unwrap() as u32;
    let words: Vec<_> = (0..ROWS)
        .flat_map(|row| {
            if LIVE_ROWS.contains(&row) {
                [0, arm, 1]
            } else {
                [RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 0]
            }
        })
        .collect();
    let choices = round
        .upload_choice_words_for_test(&frontier, &words)
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, CAPACITY)
        .unwrap()
        .inspect();
    assert_success(&inspection, LIVE_ROWS.len());
    let entity_code = program.encode(&raw(entity)).unwrap().get();
    assert_eq!(
        &inspection.candidate_codes[..LIVE_ROWS.len()],
        &vec![entity_code; LIVE_ROWS.len()]
    );
    assert_eq!(
        &inspection.candidate_owners[..LIVE_ROWS.len()],
        &LIVE_ROWS.map(|row| row as u32)
    );
    assert_eq!(
        &inspection.proposer_arms[..LIVE_ROWS.len()],
        &vec![arm; LIVE_ROWS.len()]
    );
    let expected_body: Vec<_> = (0..LIVE_ROWS.len())
        .flat_map(|_| [entity_code, parent[0], parent[1], parent[2]])
        .collect();
    assert_eq!(
        &inspection.child_body[..LIVE_ROWS.len() * inspection.child_stride as usize],
        expected_body
    );
    assert!(inspection.candidate_codes[LIVE_ROWS.len()..]
        .iter()
        .chain(&inspection.candidate_owners[LIVE_ROWS.len()..])
        .chain(&inspection.proposer_arms[LIVE_ROWS.len()..])
        .chain(&inspection.child_body[LIVE_ROWS.len() * inspection.child_stride as usize..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn destination_segment_inversion_skips_leading_internal_and_trailing_zero_segments() {
    let entity = benchmark_id(0, 0);
    let attribute = benchmark_id(1, 0);
    let value = benchmark_id(2, 0);
    let mut set = TribleSet::new();
    insert(&mut set, entity, attribute, value);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let bound = vec![
        ProgramVariable::new(9),
        ProgramVariable::new(10),
        ProgramVariable::new(11),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        12,
        [
            QueryPattern::new(bound[0], bound[1], bound[2]),
            QueryPattern::new(
                ProgramVariable::new(0),
                ProgramVariable::new(1),
                ProgramVariable::new(2),
            ),
            QueryPattern::new(
                ProgramVariable::new(3),
                ProgramVariable::new(4),
                ProgramVariable::new(5),
            ),
            QueryPattern::new(
                ProgramVariable::new(6),
                ProgramVariable::new(7),
                ProgramVariable::new(8),
            ),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let parent = [
        program.encode(&raw(entity)).unwrap().get(),
        program.encode(&raw(attribute)).unwrap().get(),
        program.encode(&raw(value)).unwrap().get(),
    ];
    let host_frontier = program
        .frontier_from_indices(bound, [parent, parent].concat(), 2)
        .unwrap();
    let frontier = round.upload_frontier(&host_frontier).unwrap();
    let arm_for = |variable| {
        round
            .metadata()
            .arms()
            .iter()
            .position(|identity| identity.target_variable() == ProgramVariable::new(variable))
            .unwrap() as u32
    };
    let value_arm = arm_for(2);
    let entity_arm = arm_for(6);
    let choices = round
        .upload_choice_words_for_test(&frontier, &[2, value_arm, 1, 6, entity_arm, 1])
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, 4)
        .unwrap()
        .inspect();
    assert_success(&inspection, 2);
    assert_eq!(inspection.segments.len(), 9);
    assert_eq!(
        inspection
            .segments
            .iter()
            .map(|segment| segment.count)
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 0, 0, 0, 1, 0, 0]
    );
    assert_eq!(
        &inspection.candidate_codes[..2],
        &[
            program.encode(&raw(value)).unwrap().get(),
            program.encode(&raw(entity)).unwrap().get(),
        ]
    );
    assert_eq!(&inspection.candidate_owners[..2], &[0, 1]);
    assert_eq!(&inspection.proposer_arms[..2], &[value_arm, entity_arm]);
    assert!(inspection.candidate_codes[2..]
        .iter()
        .chain(&inspection.candidate_owners[2..])
        .chain(&inspection.proposer_arms[2..])
        .chain(&inspection.child_body[2 * inspection.child_stride as usize..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn destination_segment_inversion_reaches_only_live_final_segment_near_program_limit() {
    let entity = benchmark_id(0, 0);
    let attribute = benchmark_id(1, 0);
    let value = benchmark_id(2, 0);
    let mut set = TribleSet::new();
    insert(&mut set, entity, attribute, value);
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    // QueryProgram admits at most 128 variables; 126 is the largest multiple
    // of three that keeps every disjoint arm a zero-peer Present primitive.
    let patterns: Vec<_> = (0..42)
        .map(|pattern| {
            let base = pattern * 3;
            QueryPattern::new(
                ProgramVariable::new(base),
                ProgramVariable::new(base + 1),
                ProgramVariable::new(base + 2),
            )
        })
        .collect();
    let program = QueryProgram::compile(gpu.archive(), 126, patterns).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let final_variable = ProgramVariable::new(125);
    let final_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == final_variable)
        .unwrap() as u32;
    let choices = round
        .upload_choice_words_for_test(&frontier, &[final_variable.index() as u32, final_arm, 1])
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, 4)
        .unwrap()
        .inspect();
    assert_success(&inspection, 1);
    assert_eq!(inspection.segments.len(), 126);
    assert!(inspection.segments[..125]
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));
    assert_eq!(inspection.segments[125].base, 0);
    assert_eq!(inspection.segments[125].count, 1);
    assert_eq!(inspection.segments[125].variable, 125);
    assert_eq!(
        inspection.candidate_codes[0],
        program.encode(&raw(value)).unwrap().get()
    );
    assert_eq!(inspection.candidate_owners[0], 0);
    assert_eq!(inspection.proposer_arms[0], final_arm);
    assert!(inspection.candidate_codes[1..]
        .iter()
        .chain(&inspection.candidate_owners[1..])
        .chain(&inspection.proposer_arms[1..])
        .chain(&inspection.child_body[1..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn destination_generator_preserves_mixed_axis_order_under_1_64_4096_skew() {
    const ATTRIBUTE_COUNT: usize = 64;
    const VALUE_COUNT: usize = 4096;
    const TOTAL: usize = 1 + ATTRIBUTE_COUNT + VALUE_COUNT;
    const CAPACITY: usize = TOTAL + 7;
    let entity = benchmark_id(0, 0);
    let attributes: Vec<_> = (0..ATTRIBUTE_COUNT)
        .map(|ordinal| benchmark_id(1, ordinal as u64))
        .collect();
    let values: Vec<_> = (0..VALUE_COUNT)
        .map(|ordinal| benchmark_id(2, ordinal as u64))
        .collect();
    let mut set = TribleSet::new();
    for (ordinal, &value) in values.iter().enumerate() {
        insert(
            &mut set,
            entity,
            attributes[ordinal % ATTRIBUTE_COUNT],
            value,
        );
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let bound = vec![
        ProgramVariable::new(1),
        ProgramVariable::new(3),
        ProgramVariable::new(5),
    ];
    let targets = [
        ProgramVariable::new(0),
        ProgramVariable::new(2),
        ProgramVariable::new(4),
    ];
    let program = QueryProgram::compile(
        gpu.archive(),
        6,
        [
            QueryPattern::new(bound[0], bound[1], bound[2]),
            QueryPattern::new(targets[0], targets[1], targets[2]),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let parent = [
        program.encode(&raw(entity)).unwrap().get(),
        program.encode(&raw(attributes[0])).unwrap().get(),
        program.encode(&raw(values[0])).unwrap().get(),
    ];
    let host_frontier = program
        .frontier_from_indices(bound, [parent, parent, parent].concat(), 3)
        .unwrap();
    let frontier = round.upload_frontier(&host_frontier).unwrap();
    let arms: Vec<_> = targets
        .iter()
        .map(|&target| {
            round
                .metadata()
                .arms()
                .iter()
                .position(|identity| identity.target_variable() == target)
                .unwrap() as u32
        })
        .collect();
    let choices = round
        .upload_choice_words_for_test(
            &frontier,
            &[
                targets[0].index() as u32,
                arms[0],
                1,
                targets[1].index() as u32,
                arms[1],
                ATTRIBUTE_COUNT as u32,
                targets[2].index() as u32,
                arms[2],
                VALUE_COUNT as u32,
            ],
        )
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, CAPACITY)
        .unwrap()
        .inspect();
    assert_success(&inspection, TOTAL);
    let mut expected_codes = vec![program.encode(&raw(entity)).unwrap().get()];
    expected_codes.extend(
        attributes
            .iter()
            .map(|&id| program.encode(&raw(id)).unwrap().get()),
    );
    expected_codes.extend(
        values
            .iter()
            .map(|&id| program.encode(&raw(id)).unwrap().get()),
    );
    let mut expected_owners = vec![0];
    expected_owners.extend(std::iter::repeat_n(1, ATTRIBUTE_COUNT));
    expected_owners.extend(std::iter::repeat_n(2, VALUE_COUNT));
    let mut expected_arms = vec![arms[0]];
    expected_arms.extend(std::iter::repeat_n(arms[1], ATTRIBUTE_COUNT));
    expected_arms.extend(std::iter::repeat_n(arms[2], VALUE_COUNT));
    assert_eq!(&inspection.candidate_codes[..TOTAL], expected_codes);
    assert_eq!(&inspection.candidate_owners[..TOTAL], expected_owners);
    assert_eq!(&inspection.proposer_arms[..TOTAL], expected_arms);
    assert!(inspection.candidate_codes[TOTAL..]
        .iter()
        .chain(&inspection.candidate_owners[TOTAL..])
        .chain(&inspection.proposer_arms[TOTAL..])
        .chain(&inspection.child_body[TOTAL * inspection.child_stride as usize..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn destination_generator_uses_each_rows_axis_for_two_arms_in_one_segment() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let program = QueryProgram::compile(
        gpu.archive(),
        5,
        [
            QueryPattern::new(v0, ProgramVariable::new(1), ProgramVariable::new(2)),
            QueryPattern::new(ProgramVariable::new(3), ProgramVariable::new(4), v0),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let host_frontier = program
        .frontier_from_indices(Vec::new(), Vec::new(), 2)
        .unwrap();
    let frontier = round.upload_frontier(&host_frontier).unwrap();
    let target_arms: Vec<_> = round
        .metadata()
        .arms()
        .iter()
        .enumerate()
        .filter(|(_, identity)| identity.target_variable() == v0)
        .map(|(arm, _)| arm as u32)
        .collect();
    assert_eq!(target_arms.len(), 2);
    let entity_arm = target_arms
        .iter()
        .copied()
        .find(|&arm| round.proposal_arm_axis(arm as usize) == Some(ResidentAxis::Entity))
        .unwrap();
    let value_arm = target_arms
        .iter()
        .copied()
        .find(|&arm| round.proposal_arm_axis(arm as usize) == Some(ResidentAxis::Value))
        .unwrap();
    let entity_count = fixture.entities.len() as u32;
    let value_count = fixture.values.len() as u32;
    let choices = round
        .upload_choice_words_for_test(
            &frontier,
            &[
                v0.index() as u32,
                entity_arm,
                entity_count,
                v0.index() as u32,
                value_arm,
                value_count,
            ],
        )
        .unwrap();
    let total = entity_count as usize + value_count as usize;
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, total + 2)
        .unwrap()
        .inspect();
    assert_success(&inspection, total);
    let mut expected_codes = codes(&program, &fixture.entities);
    expected_codes.extend(codes(&program, &fixture.values));
    assert_eq!(&inspection.candidate_codes[..total], expected_codes);
    assert_eq!(
        &inspection.candidate_owners[..total],
        &[
            vec![0; entity_count as usize],
            vec![1; value_count as usize]
        ]
        .concat()
    );
    assert_eq!(
        &inspection.proposer_arms[..total],
        &[
            vec![entity_arm; entity_count as usize],
            vec![value_arm; value_count as usize],
        ]
        .concat()
    );
    assert_eq!(&inspection.child_body[..total], expected_codes);
    assert!(inspection.candidate_codes[total..]
        .iter()
        .chain(&inspection.candidate_owners[total..])
        .chain(&inspection.proposer_arms[total..])
        .chain(&inspection.child_body[total..])
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

fn invert_destination_strict_ends(
    segments: usize,
    rows: usize,
    counts: &[u32],
    destination: u32,
) -> (usize, usize, u32) {
    let segment_ends: Vec<_> = counts
        .chunks_exact(rows)
        .scan(0u32, |prefix, segment| {
            *prefix += segment.iter().sum::<u32>();
            Some(*prefix)
        })
        .collect();
    let mut segment_lo = 0usize;
    let mut segment_hi = segments;
    while segment_lo < segment_hi {
        let mid = segment_lo + (segment_hi - segment_lo) / 2;
        if segment_ends[mid] <= destination {
            segment_lo = mid + 1;
        } else {
            segment_hi = mid;
        }
    }
    let segment = segment_lo;
    let segment_base = if segment == 0 {
        0
    } else {
        segment_ends[segment - 1]
    };
    let segment_counts = &counts[segment * rows..(segment + 1) * rows];
    let mut row_ends = Vec::with_capacity(rows);
    let mut prefix = segment_base;
    for &count in segment_counts {
        prefix += count;
        row_ends.push(prefix);
    }
    let mut row_lo = 0usize;
    let mut row_hi = rows;
    while row_lo < row_hi {
        let mid = row_lo + (row_hi - row_lo) / 2;
        if row_ends[mid] <= destination {
            row_lo = mid + 1;
        } else {
            row_hi = mid;
        }
    }
    let row = row_lo;
    let row_start = if row == 0 {
        segment_base
    } else {
        row_ends[row - 1]
    };
    (segment, row, destination - row_start)
}

fn assert_host_inversion_matches_expansion(segments: usize, rows: usize, counts: &[u32]) {
    let expanded: Vec<_> = (0..segments)
        .flat_map(|segment| {
            (0..rows).flat_map(move |row| {
                (0..counts[segment * rows + row]).map(move |ordinal| (segment, row, ordinal))
            })
        })
        .collect();
    for (destination, expected) in expanded.iter().copied().enumerate() {
        assert_eq!(
            invert_destination_strict_ends(segments, rows, counts, destination as u32),
            expected,
            "segments={segments}, rows={rows}, counts={counts:?}, d={destination}"
        );
    }
}

#[test]
fn host_inversion_exhausts_small_counts_and_all_large_zero_run_topologies() {
    for segments in 1usize..=4 {
        for rows in 1usize..=4 {
            let cells = segments * rows;
            if cells <= 9 {
                for encoded in 0usize..4usize.pow(cells as u32) {
                    let mut digits = encoded;
                    let mut counts = vec![0u32; cells];
                    for count in &mut counts {
                        *count = (digits & 3) as u32;
                        digits >>= 2;
                    }
                    assert_host_inversion_matches_expansion(segments, rows, &counts);
                }
            } else {
                // Exhaust every zero/nonzero topology at the larger shapes and
                // instantiate every permitted positive count. Full mixed-width
                // enumeration at 4x4 would be 4^16 cases; the <=9-cell shapes
                // above exhaust those interactions without a multi-billion-case
                // unit test.
                for positive in 1u32..=3 {
                    for mask in 0usize..1usize << cells {
                        let counts: Vec<_> = (0..cells)
                            .map(|cell| {
                                if mask & (1usize << cell) == 0 {
                                    0
                                } else {
                                    positive
                                }
                            })
                            .collect();
                        assert_host_inversion_matches_expansion(segments, rows, &counts);
                    }
                }
                for phase in 0..4 {
                    let counts: Vec<_> =
                        (0..cells).map(|cell| ((cell + phase) % 4) as u32).collect();
                    assert_host_inversion_matches_expansion(segments, rows, &counts);
                }
            }
        }
    }
}

#[test]
fn every_present_axis_and_variable_permutation_matches_cpu_transition_order() {
    let fixture = fixture();
    let permutations = [
        [
            ProgramVariable::new(0),
            ProgramVariable::new(1),
            ProgramVariable::new(2),
        ],
        [
            ProgramVariable::new(0),
            ProgramVariable::new(2),
            ProgramVariable::new(1),
        ],
        [
            ProgramVariable::new(1),
            ProgramVariable::new(0),
            ProgramVariable::new(2),
        ],
        [
            ProgramVariable::new(2),
            ProgramVariable::new(0),
            ProgramVariable::new(1),
        ],
        [
            ProgramVariable::new(1),
            ProgramVariable::new(2),
            ProgramVariable::new(0),
        ],
        [
            ProgramVariable::new(2),
            ProgramVariable::new(1),
            ProgramVariable::new(0),
        ],
    ];

    for axes in permutations {
        let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
        let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
        let program = QueryProgram::compile(
            gpu.archive(),
            3,
            [QueryPattern::new(axes[0], axes[1], axes[2])],
        )
        .unwrap();
        let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
        let seed = ProgramFrontier::seed();
        let frontier = round.upload_frontier(&seed).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let choices = round.enqueue(&inputs).unwrap();

        let cpu = program.transition(&seed).unwrap();
        assert_eq!(cpu.len(), 1);
        assert_eq!(cpu[0].variables(), &[ProgramVariable::new(0)]);
        let expected_body: Vec<_> = cpu[0].values().iter().map(|code| code.get()).collect();
        let expected_rows = cpu[0].len();

        let arena = round
            .enqueue_present_proposals(&frontier, &choices, expected_rows)
            .unwrap();
        let inspection = arena.inspect();
        assert_success(&inspection, expected_rows);
        assert_eq!(inspection.child_body, expected_body);

        let segments = live_segments(&inspection);
        let (candidate_codes, owners, proposers, bodies) = &segments[&0];
        assert_eq!(candidate_codes, &expected_body);
        assert_eq!(owners, &vec![0; expected_rows]);
        assert_eq!(proposers, &vec![0; expected_rows]);
        assert_eq!(
            bodies,
            &expected_body
                .iter()
                .copied()
                .map(|code| vec![code])
                .collect::<Vec<_>>()
        );

        let selected_axis = axes
            .iter()
            .position(|&variable| variable.index() == 0)
            .unwrap();
        let expected_present = match selected_axis {
            0 => codes(&program, &fixture.entities),
            1 => codes(&program, &fixture.attributes),
            2 => codes(&program, &fixture.values),
            _ => unreachable!(),
        };
        assert_eq!(candidate_codes, &expected_present);
        if selected_axis != 1 {
            assert_ne!(
                candidate_codes,
                &(0..expected_rows as u32).collect::<Vec<_>>()
            );
        }
    }
}

struct MixedSetup {
    gpu: crate::WgpuSuccinctArchive<OrderedUniverse>,
    fixture: Fixture,
}

impl MixedSetup {
    fn new() -> Self {
        let fixture = fixture();
        let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
        Self {
            gpu: crate::WgpuSuccinctArchive::new(archive).unwrap(),
            fixture,
        }
    }
}

fn mixed_program<'a>(setup: &'a MixedSetup) -> QueryProgram<'a, OrderedUniverse> {
    // v1/v3/v5 are an unrelated fully-bound support pattern. v0/v2/v4
    // remain zero-peer Present arms with insertion positions 0/1/2.
    QueryProgram::compile(
        setup.gpu.archive(),
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

fn bound_frontier(
    program: &QueryProgram<'_, OrderedUniverse>,
    fixture: &Fixture,
    rows: usize,
) -> ProgramFrontier {
    let triples = [
        (
            fixture.entities[0],
            fixture.attributes[0],
            fixture.values[0],
        ),
        (
            fixture.entities[1],
            fixture.attributes[0],
            fixture.values[1],
        ),
        (
            fixture.entities[2],
            fixture.attributes[1],
            fixture.values[2],
        ),
    ];
    let mut values = Vec::with_capacity(rows * 3);
    for row in 0..rows {
        let (entity, attribute, value) = triples[row % triples.len()];
        values.extend([
            program.encode(&raw(entity)).unwrap().get(),
            program.encode(&raw(attribute)).unwrap().get(),
            program.encode(&raw(value)).unwrap().get(),
        ]);
    }
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

fn mixed_choice_words(
    rows: usize,
    entity_count: u32,
    attribute_count: u32,
    value_count: u32,
) -> Vec<u32> {
    let choices = [
        (0u32, 0u32, entity_count),
        (2u32, 1u32, attribute_count),
        (4u32, 2u32, value_count),
    ];
    (0..rows)
        .flat_map(|row| {
            let (variable, arm, count) = choices[row % choices.len()];
            [variable, arm, count]
        })
        .collect()
}

fn mixed_choice_words_range(
    range: std::ops::Range<usize>,
    entity_count: u32,
    attribute_count: u32,
    value_count: u32,
) -> Vec<u32> {
    let choices = [
        (0u32, 0u32, entity_count),
        (2u32, 1u32, attribute_count),
        (4u32, 2u32, value_count),
    ];
    range
        .map(|row| choices[row % choices.len()])
        .flat_map(|(variable, arm, count)| [variable, arm, count])
        .collect()
}

fn inspect_mixed_range(
    round: &WgpuResidentRound<'_, OrderedUniverse>,
    full: &ProgramFrontier,
    range: std::ops::Range<usize>,
    counts: [u32; 3],
) -> ProposalInspection {
    let frontier = full.slice(range.clone()).unwrap();
    let resident = round.upload_frontier(&frontier).unwrap();
    let words = mixed_choice_words_range(range, counts[0], counts[1], counts[2]);
    let capacity = words
        .chunks_exact(CHOICE_WORDS)
        .map(|choice| choice[2] as usize)
        .sum();
    let choices = round
        .upload_choice_words_for_test(&resident, &words)
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&resident, &choices, capacity)
        .unwrap()
        .inspect();
    assert_success(&inspection, capacity);
    inspection
}

fn concatenate_segment_maps(
    left: &ProposalInspection,
    right: &ProposalInspection,
    right_owner_offset: u32,
) -> SegmentMap {
    let mut left = live_segments(left);
    let right = live_segments(right);
    for (variable, (codes, owners, proposers, bodies)) in right {
        let entry = left.entry(variable).or_default();
        entry.0.extend(codes);
        entry
            .1
            .extend(owners.into_iter().map(|owner| owner + right_owner_offset));
        entry.2.extend(proposers);
        entry.3.extend(bodies);
    }
    left
}

#[test]
fn mixed_variables_form_stable_segments_and_insert_canonically() {
    let setup = MixedSetup::new();
    let program = mixed_program(&setup);
    let round = WgpuResidentRound::new(
        &setup.gpu,
        &program,
        &[
            ProgramVariable::new(1),
            ProgramVariable::new(3),
            ProgramVariable::new(5),
        ],
    )
    .unwrap();
    let cpu_frontier = bound_frontier(&program, &setup.fixture, 4);
    let frontier = round.upload_frontier(&cpu_frontier).unwrap();

    // The actual resident planner chooses v0 for every row. This is a complete
    // ordered CPU-transition parity check with an unrelated fully-bound
    // support and a duplicate parent row (rows 0 and 3).
    let planned_inputs = round.initialize_inputs(&frontier).unwrap();
    let planned_choices = round.enqueue(&planned_inputs).unwrap();
    let cpu_children = program.transition(&cpu_frontier).unwrap();
    assert_eq!(cpu_children.len(), 1);
    let cpu_body: Vec<_> = cpu_children[0]
        .values()
        .iter()
        .map(|code| code.get())
        .collect();
    let planned = round
        .enqueue_present_proposals(&frontier, &planned_choices, cpu_children[0].len())
        .unwrap()
        .inspect();
    assert_success(&planned, cpu_children[0].len());
    assert_eq!(planned.child_body, cpu_body);

    let words = mixed_choice_words(
        4,
        setup.gpu.present_entity_codes().len() as u32,
        setup.gpu.present_attribute_codes().len() as u32,
        setup.gpu.present_value_codes().len() as u32,
    );
    let choices = round
        .upload_choice_words_for_test(&frontier, &words)
        .unwrap();
    let expected = setup.gpu.present_entity_codes().len() * 2
        + setup.gpu.present_attribute_codes().len()
        + setup.gpu.present_value_codes().len();
    let arena = round
        .enqueue_present_proposals(&frontier, &choices, expected + 3)
        .unwrap();
    assert!(!Arc::ptr_eq(&arena.frontier_lineage, &arena.arena_lineage));
    let inspection = arena.inspect();
    assert_success(&inspection, expected);
    assert_eq!(
        inspection.segments,
        vec![
            ProposalSegmentInspection {
                base: 0,
                count: (setup.gpu.present_entity_codes().len() * 2) as u32,
                variable: 0,
                insertion: 0,
            },
            ProposalSegmentInspection {
                base: (setup.gpu.present_entity_codes().len() * 2) as u32,
                count: setup.gpu.present_attribute_codes().len() as u32,
                variable: 2,
                insertion: 1,
            },
            ProposalSegmentInspection {
                base: (setup.gpu.present_entity_codes().len() * 2
                    + setup.gpu.present_attribute_codes().len()) as u32,
                count: setup.gpu.present_value_codes().len() as u32,
                variable: 4,
                insertion: 2,
            },
        ]
    );

    let segments = live_segments(&inspection);
    let entity_codes = codes(&program, &setup.fixture.entities);
    let attribute_codes = codes(&program, &setup.fixture.attributes);
    let value_codes = codes(&program, &setup.fixture.values);
    assert_eq!(
        segments[&0].0,
        [entity_codes.clone(), entity_codes].concat()
    );
    assert_eq!(segments[&0].1, vec![0, 0, 0, 3, 3, 3]);
    assert_eq!(segments[&0].2, vec![0; 6]);
    assert_eq!(segments[&2].0, attribute_codes);
    assert_eq!(segments[&2].1, vec![1, 1]);
    assert_eq!(segments[&2].2, vec![1, 1]);
    assert_eq!(segments[&4].0, value_codes);
    assert_eq!(segments[&4].1, vec![2, 2, 2]);
    assert_eq!(segments[&4].2, vec![2, 2, 2]);

    for (&variable, (_, owners, _, bodies)) in &segments {
        let insertion = (variable / 2) as usize;
        for (&owner, body) in owners.iter().zip(bodies) {
            let parent = cpu_frontier.row(owner as usize);
            assert_eq!(body.len(), parent.len() + 1);
            assert_eq!(
                &body[..insertion],
                &parent[..insertion]
                    .iter()
                    .map(|c| c.get())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                &body[insertion + 1..],
                &parent[insertion..]
                    .iter()
                    .map(|c| c.get())
                    .collect::<Vec<_>>()
            );
        }
    }

    assert!(inspection.candidate_codes[expected..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(inspection.candidate_owners[expected..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(inspection.proposer_arms[expected..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(
        inspection.child_body[expected * inspection.child_stride as usize..]
            .iter()
            .all(|&word| word == RESIDENT_U32_SENTINEL)
    );
}

#[test]
fn sixty_five_rows_match_every_consecutive_split_in_variable_major_order() {
    let setup = MixedSetup::new();
    let program = mixed_program(&setup);
    let round = WgpuResidentRound::new(
        &setup.gpu,
        &program,
        &[
            ProgramVariable::new(1),
            ProgramVariable::new(3),
            ProgramVariable::new(5),
        ],
    )
    .unwrap();
    let full = bound_frontier(&program, &setup.fixture, 65);
    let counts = [
        setup.gpu.present_entity_codes().len() as u32,
        setup.gpu.present_attribute_codes().len() as u32,
        setup.gpu.present_value_codes().len() as u32,
    ];
    let whole = inspect_mixed_range(&round, &full, 0..65, counts);
    let expected = live_segments(&whole);

    // The complete frontier and these partitions jointly exercise rows
    // 0/1/63/64/65, every split of 65, and the 64-cell scan-block boundary.
    let empty = inspect_mixed_range(&round, &full, 0..0, counts);
    assert_success(&empty, 0);
    for split in 1..65 {
        let left = inspect_mixed_range(&round, &full, 0..split, counts);
        let right = inspect_mixed_range(&round, &full, split..65, counts);
        assert_eq!(
            concatenate_segment_maps(&left, &right, split as u32),
            expected,
            "row-homomorphism split {split}"
        );
    }
}

#[test]
fn capacity_exact_one_short_and_zero_never_publish_a_partial_prefix() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let required = gpu.present_entity_codes().len();

    let exact = round
        .enqueue_present_proposals(&frontier, &choices, required)
        .unwrap()
        .inspect();
    assert_success(&exact, required);

    for capacity in [required - 1, 0] {
        let failed = round
            .enqueue_present_proposals(&frontier, &choices, capacity)
            .unwrap()
            .inspect();
        assert_eq!(failed.status, STATUS_CAPACITY);
        assert_eq!(failed.required, required as u32);
        assert_eq!(failed.logical_len, 0);
        assert!(failed.segments.iter().all(|segment| {
            segment.base == RESIDENT_U32_SENTINEL
                && segment.count == RESIDENT_U32_SENTINEL
                && segment.variable == RESIDENT_U32_SENTINEL
                && segment.insertion == RESIDENT_U32_SENTINEL
        }));
        assert!(failed
            .candidate_codes
            .iter()
            .chain(&failed.candidate_owners)
            .chain(&failed.proposer_arms)
            .chain(&failed.child_body)
            .all(|&word| word == RESIDENT_U32_SENTINEL));
    }
}

#[test]
fn malformed_private_choices_and_axis_descriptors_fail_before_capacity() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let entity_count = gpu.present_entity_codes().len() as u32;

    let malformed = [
        [3, 0, entity_count],
        [0, 1, entity_count],
        [0, 0, entity_count - 1],
        [0, 0, RESIDENT_U32_SENTINEL],
        [RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 1],
        [
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
            RESIDENT_U32_SENTINEL,
        ],
    ];
    for words in malformed {
        let choices = round
            .upload_choice_words_for_test(&frontier, &words)
            .unwrap();
        let inspection = round
            .enqueue_present_proposals(&frontier, &choices, 0)
            .unwrap()
            .inspect();
        assert_eq!(inspection.status, STATUS_DEVICE_INVARIANT, "{words:?}");
        assert_eq!(inspection.required, RESIDENT_U32_SENTINEL);
        assert_eq!(inspection.logical_len, 0);
        assert!(inspection.segments.iter().all(|segment| {
            segment.base == RESIDENT_U32_SENTINEL
                && segment.count == RESIDENT_U32_SENTINEL
                && segment.variable == RESIDENT_U32_SENTINEL
                && segment.insertion == RESIDENT_U32_SENTINEL
        }));
    }

    // A valid row still contributes real work beside the malformed row. The
    // sticky invariant must win over the simultaneous zero-capacity miss.
    let two_host = ProgramFrontier::new(Vec::new(), Vec::new(), 2).unwrap();
    let two_frontier = round.upload_frontier(&two_host).unwrap();
    let two_words = [0, 0, entity_count, 0, 0, entity_count - 1];
    let two_choices = round
        .upload_choice_words_for_test(&two_frontier, &two_words)
        .unwrap();
    let sticky = round
        .enqueue_present_proposals(&two_frontier, &two_choices, 0)
        .unwrap()
        .inspect();
    assert_eq!(sticky.status, STATUS_DEVICE_INVARIANT);
    assert_eq!(sticky.required, RESIDENT_U32_SENTINEL);
    assert_eq!(sticky.logical_len, 0);

    let valid = [0, 0, entity_count];
    let choices = round
        .upload_choice_words_for_test(&frontier, &valid)
        .unwrap();
    let mut descriptors = lower_present_admission(&round).unwrap().arm_descriptors;
    descriptors[1] = ResidentAxis::Attribute.code();
    let inspection = round
        .enqueue_present_proposals_with_trusted_descriptors_for_test(
            &frontier,
            &choices,
            entity_count as usize,
            &descriptors,
        )
        .unwrap()
        .inspect();
    assert_eq!(inspection.status, STATUS_DEVICE_INVARIANT);
    assert_eq!(inspection.logical_len, 0);
    assert!(inspection
        .candidate_codes
        .iter()
        .chain(&inspection.candidate_owners)
        .chain(&inspection.proposer_arms)
        .chain(&inspection.child_body)
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn trusted_descriptor_override_is_not_a_device_axis_authenticator() {
    // Entity and value cardinalities are deliberately equal. Production
    // lowering independently derives and checks the entity axis before upload;
    // the private override below bypasses that trust boundary, so the device
    // cannot distinguish this internally consistent E -> V substitution.
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let entity_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == v0)
        .unwrap();
    let count = gpu.present_entity_codes().len() as u32;
    assert_eq!(count as usize, gpu.present_value_codes().len());
    let choices = round
        .upload_choice_words_for_test(&frontier, &[v0.index() as u32, entity_arm as u32, count])
        .unwrap();

    let production = round
        .enqueue_present_proposals(&frontier, &choices, count as usize)
        .unwrap()
        .inspect();
    let expected_entities = codes(&program, &fixture.entities);
    assert_eq!(
        &production.candidate_codes[..count as usize],
        expected_entities
    );

    let mut trusted = lower_present_admission(&round).unwrap().arm_descriptors;
    trusted[entity_arm * ARM_DESCRIPTOR_WORDS + 1] = ResidentAxis::Value.code();
    let overridden = round
        .enqueue_present_proposals_with_trusted_descriptors_for_test(
            &frontier,
            &choices,
            count as usize,
            &trusted,
        )
        .unwrap()
        .inspect();
    assert_success(&overridden, count as usize);
    let expected_values = codes(&program, &fixture.values);
    assert_eq!(
        &overridden.candidate_codes[..count as usize],
        expected_values
    );
}

#[test]
fn two_present_arms_for_one_variable_retain_exact_proposer_ids() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let v3 = ProgramVariable::new(3);
    let v4 = ProgramVariable::new(4);
    let program = QueryProgram::compile(
        gpu.archive(),
        5,
        [QueryPattern::new(v0, v1, v2), QueryPattern::new(v0, v3, v4)],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let arms: Vec<u32> = round
        .metadata()
        .arms()
        .iter()
        .enumerate()
        .filter_map(|(arm, identity)| (identity.target_variable() == v0).then_some(arm as u32))
        .collect();
    assert_eq!(arms.len(), 2);
    let parents = ProgramFrontier::new(Vec::new(), Vec::new(), 2).unwrap();
    let frontier = round.upload_frontier(&parents).unwrap();
    let count = gpu.present_entity_codes().len() as u32;
    let choices = round
        .upload_choice_words_for_test(
            &frontier,
            &[
                v0.index() as u32,
                arms[0],
                count,
                v0.index() as u32,
                arms[1],
                count,
            ],
        )
        .unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, count as usize * 2)
        .unwrap()
        .inspect();
    assert_success(&inspection, count as usize * 2);
    let segment = live_segments(&inspection)
        .remove(&(v0.index() as u32))
        .unwrap();
    let mut expected_codes = codes(&program, &fixture.entities);
    expected_codes.extend(codes(&program, &fixture.entities));
    assert_eq!(segment.0, expected_codes);
    assert_eq!(
        segment.1,
        [vec![0; count as usize], vec![1; count as usize],].concat()
    );
    assert_eq!(
        segment.2,
        [vec![arms[0]; count as usize], vec![arms[1]; count as usize],].concat()
    );
}

#[test]
fn unrepresentable_scan_total_has_a_distinct_geometry_status() {
    let context = crate::WgpuContext::on_wgpu();
    let workspace_layout = workspace_layout(1, 1, 0, 2).unwrap();
    let mut workspace_words = vec![0; workspace_layout.words];
    // The second block would make the exact total equal the reserved sentinel.
    workspace_words[workspace_layout.block_sums..workspace_layout.block_sums + 2]
        .copy_from_slice(&[RESIDENT_U32_SENTINEL - 1, 1]);
    workspace_words[workspace_layout.block_errors..workspace_layout.block_errors + 2]
        .copy_from_slice(&[STATUS_OK, STATUS_OK]);
    let mut workspace = context.upload_u32(&workspace_words).unwrap();
    let plan = context.upload_u32(&[0, 0]).unwrap();
    let mut segment_records = context
        .upload_u32(&[RESIDENT_U32_SENTINEL; SEGMENT_RECORD_WORDS])
        .unwrap();
    let mut control = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    let meta = context.batch_meta(0, 1).unwrap();
    let dispatch = context
        .batch_dispatch(0, 1, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        finalize_present_scan::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.output_arg(),
            plan.input_arg(),
            segment_records.output_arg(),
            control.output_arg(),
            1,
            1,
            2,
            0,
            1,
            0,
            1,
            1,
            dispatch.max_groups_x(),
            dispatch.max_groups_y(),
            THREADS,
            workspace_layout.counts as u32,
            workspace_layout.validation_errors as u32,
            workspace_layout.local_offsets as u32,
            workspace_layout.block_sums as u32,
            workspace_layout.block_errors as u32,
            workspace_layout.block_offsets as u32,
            0,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    let poison = [RESIDENT_U32_SENTINEL];
    let arena = WgpuResidentProposals {
        context: context.clone(),
        round_owner: Arc::new(()),
        frontier_lineage: Arc::new(()),
        arena_lineage: Arc::new(()),
        rows: 1,
        parent_stride: 0,
        child_stride: 1,
        segment_count: 1,
        capacity: 1,
        control,
        meta,
        dispatch,
        segment_records,
        candidate_records: context
            .upload_u32(&[RESIDENT_U32_SENTINEL; CANDIDATE_RECORD_FIELDS])
            .unwrap(),
        child_body: context.upload_u32(&poison).unwrap(),
        stage_profiles: None,
    };
    let inspection = arena.inspect();
    assert_eq!(inspection.status, STATUS_GEOMETRY);
    assert_eq!(inspection.required, RESIDENT_U32_SENTINEL);
    assert_eq!(inspection.logical_len, 0);
    assert!(inspection.segments.iter().all(|segment| {
        segment.base == RESIDENT_U32_SENTINEL
            && segment.count == RESIDENT_U32_SENTINEL
            && segment.variable == RESIDENT_U32_SENTINEL
            && segment.insertion == RESIDENT_U32_SENTINEL
    }));
}

#[test]
fn largest_representable_total_uses_overflow_safe_dispatch_geometry() {
    let context = crate::WgpuContext::on_wgpu();
    let workspace_layout = workspace_layout(1, 1, 0, 1).unwrap();
    let mut workspace_words = vec![0; workspace_layout.words];
    workspace_words[workspace_layout.block_sums] = RESIDENT_U32_SENTINEL - 1;
    workspace_words[workspace_layout.block_errors] = STATUS_OK;
    let mut workspace = context.upload_u32(&workspace_words).unwrap();
    let plan = context.upload_u32(&[0, 0]).unwrap();
    let mut segment_records = context
        .upload_u32(&[RESIDENT_U32_SENTINEL; SEGMENT_RECORD_WORDS])
        .unwrap();
    let mut control = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    let hardware = &context.client().properties().hardware;
    let max_x = hardware.max_cube_count.0;
    let max_y = hardware.max_cube_count.1;
    unsafe {
        finalize_present_scan::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.output_arg(),
            plan.input_arg(),
            segment_records.output_arg(),
            control.output_arg(),
            1,
            1,
            1,
            0,
            1,
            0,
            1,
            RESIDENT_U32_SENTINEL - 1,
            max_x,
            max_y,
            THREADS,
            workspace_layout.counts as u32,
            workspace_layout.validation_errors as u32,
            workspace_layout.local_offsets as u32,
            workspace_layout.block_sums as u32,
            workspace_layout.block_errors as u32,
            workspace_layout.block_offsets as u32,
            0,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    let control = control.read();
    let groups = 1 + (RESIDENT_U32_SENTINEL - 2) / THREADS;
    let expected_y = 1 + (groups - 1) / max_x;
    let expected_x = 1 + (groups - 1) / expected_y;
    assert_eq!(control[CONTROL_STATUS], STATUS_OK);
    assert_eq!(control[CONTROL_REQUIRED], RESIDENT_U32_SENTINEL - 1);
    assert_eq!(control[CONTROL_DISPATCH_X], expected_x);
    assert_eq!(control[CONTROL_DISPATCH_Y], expected_y);
    assert!((1..=max_x).contains(&expected_x));
    assert!((1..=max_y).contains(&expected_y));
}

#[test]
fn cross_frontier_and_cross_round_choices_are_rejected_before_launch() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round_a = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let round_b = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let seed = ProgramFrontier::seed();
    let frontier_a = round_a.upload_frontier(&seed).unwrap();
    let frontier_b = round_a.upload_frontier(&seed).unwrap();
    let inputs = round_a.initialize_inputs(&frontier_a).unwrap();
    let choices = round_a.enqueue(&inputs).unwrap();

    assert!(matches!(
        round_a.enqueue_present_proposals(&frontier_b, &choices, 3),
        Err(ResidentProposalError::Support(
            ResidentSupportError::ChoiceFrontierOwnership
        ))
    ));
    assert!(matches!(
        round_b.enqueue_present_proposals(&frontier_a, &choices, 3),
        Err(ResidentProposalError::Support(
            ResidentSupportError::FrontierOwnership
        ))
    ));
}

#[test]
fn global_dead_all_dead_all_zero_and_empty_frontiers_are_successful_empty_arenas() {
    // Missing constant: global death with semantic arms but no physical specs.
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let missing = [250u8; 32];
    let dead_program = QueryProgram::compile(
        gpu.archive(),
        2,
        [QueryPattern::new(v0, QueryTerm::Constant(missing), v1)],
    )
    .unwrap();
    let dead_round = WgpuResidentRound::new(&gpu, &dead_program, &[]).unwrap();
    let dead_frontier = dead_round
        .upload_frontier(&ProgramFrontier::seed())
        .unwrap();
    let dead_inputs = dead_round.initialize_inputs(&dead_frontier).unwrap();
    let dead_choices = dead_round.enqueue(&dead_inputs).unwrap();
    let dead = dead_round
        .enqueue_present_proposals(&dead_frontier, &dead_choices, 7)
        .unwrap()
        .inspect();
    assert_success(&dead, 0);
    assert!(dead
        .segments
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));
    assert!(dead
        .candidate_codes
        .iter()
        .chain(&dead.candidate_owners)
        .chain(&dead.proposer_arms)
        .chain(&dead.child_body)
        .all(|&word| word == RESIDENT_U32_SENTINEL));

    // Fully-bound support rejects the row, so ordinary (non-global) planning
    // emits one canonical dead choice and zero proposals.
    let support_program = QueryProgram::compile(
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
    .unwrap();
    let support_round = WgpuResidentRound::new(
        &gpu,
        &support_program,
        &[
            ProgramVariable::new(1),
            ProgramVariable::new(3),
            ProgramVariable::new(5),
        ],
    )
    .unwrap();
    let absent = support_program
        .frontier_from_indices(
            vec![
                ProgramVariable::new(1),
                ProgramVariable::new(3),
                ProgramVariable::new(5),
            ],
            vec![
                support_program
                    .encode(&raw(fixture.entities[0]))
                    .unwrap()
                    .get(),
                support_program
                    .encode(&raw(fixture.attributes[1]))
                    .unwrap()
                    .get(),
                support_program
                    .encode(&raw(fixture.values[0]))
                    .unwrap()
                    .get(),
            ],
            1,
        )
        .unwrap();
    let absent_frontier = support_round.upload_frontier(&absent).unwrap();
    let absent_inputs = support_round.initialize_inputs(&absent_frontier).unwrap();
    let absent_choices = support_round.enqueue(&absent_inputs).unwrap();
    let all_dead = support_round
        .enqueue_present_proposals(&absent_frontier, &absent_choices, 7)
        .unwrap()
        .inspect();
    assert_success(&all_dead, 0);
    assert!(all_dead
        .segments
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));

    // Empty archive: every selected Present width is exact zero, not dead.
    let empty_archive: SuccinctArchive<OrderedUniverse> = (&TribleSet::new()).into();
    let empty_gpu = crate::WgpuSuccinctArchive::new(empty_archive).unwrap();
    let zero_program = QueryProgram::compile(
        empty_gpu.archive(),
        3,
        [QueryPattern::new(
            ProgramVariable::new(0),
            ProgramVariable::new(1),
            ProgramVariable::new(2),
        )],
    )
    .unwrap();
    let zero_round = WgpuResidentRound::new(&empty_gpu, &zero_program, &[]).unwrap();
    let zero_frontier = zero_round
        .upload_frontier(&ProgramFrontier::seed())
        .unwrap();
    let zero_inputs = zero_round.initialize_inputs(&zero_frontier).unwrap();
    let zero_choices = zero_round.enqueue(&zero_inputs).unwrap();
    let zero = zero_round
        .enqueue_present_proposals(&zero_frontier, &zero_choices, 0)
        .unwrap()
        .inspect();
    assert_success(&zero, 0);
    assert!(zero
        .segments
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));

    // Zero-row frontier still publishes canonical zero-count segment records.
    let empty_frontier = zero_program
        .frontier_from_indices(Vec::new(), Vec::new(), 0)
        .unwrap();
    let empty_resident = zero_round.upload_frontier(&empty_frontier).unwrap();
    let empty_inputs = zero_round.initialize_inputs(&empty_resident).unwrap();
    let empty_choices = zero_round.enqueue(&empty_inputs).unwrap();
    let empty = zero_round
        .enqueue_present_proposals(&empty_resident, &empty_choices, 0)
        .unwrap()
        .inspect();
    assert_success(&empty, 0);
    assert!(empty
        .segments
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));
}

#[test]
fn fully_bound_round_has_an_exact_zero_segment_arena() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[v0, v1, v2]).unwrap();
    let parent = program
        .frontier_from_indices(
            vec![v0, v1, v2],
            vec![
                program.encode(&raw(fixture.entities[0])).unwrap().get(),
                program.encode(&raw(fixture.attributes[0])).unwrap().get(),
                program.encode(&raw(fixture.values[0])).unwrap().get(),
            ],
            1,
        )
        .unwrap();
    let frontier = round.upload_frontier(&parent).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    let inspection = round
        .enqueue_present_proposals(&frontier, &choices, 0)
        .unwrap()
        .inspect();
    assert_success(&inspection, 0);
    assert!(inspection.segments.is_empty());
    assert!(inspection.candidate_codes.is_empty());
    assert!(inspection.child_body.is_empty());
}

#[test]
fn non_present_arms_fail_host_admission_without_a_device_launch() {
    let fixture = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let program = QueryProgram::compile(gpu.archive(), 3, [QueryPattern::new(v0, v1, v2)]).unwrap();
    let bound = program
        .frontier_from_indices(
            vec![v0],
            vec![program.encode(&raw(fixture.entities[0])).unwrap().get()],
            1,
        )
        .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[v0]).unwrap();
    let frontier = round.upload_frontier(&bound).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let choices = round.enqueue(&inputs).unwrap();
    assert!(matches!(
        round.enqueue_present_proposals(&frontier, &choices, 8),
        Err(ResidentProposalError::UnsupportedProposer { .. })
    ));
}

struct ProposalBenchmarkSample {
    candidate_method: cubecl::profile::TimingMethod,
    candidate_seconds: f64,
    child_body_method: cubecl::profile::TimingMethod,
    child_body_seconds: f64,
    wall_seconds: f64,
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn measure_present_proposal_case(
    entity_count: usize,
    rows: usize,
    seed: bool,
    validate_contents: bool,
) -> ProposalBenchmarkSample {
    assert!(!seed || rows == 1);
    let attribute = benchmark_id(1, 0);
    let value = benchmark_id(2, 0);
    let mut set = TribleSet::new();
    for ordinal in 0..entity_count {
        insert(&mut set, benchmark_id(0, ordinal as u64), attribute, value);
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let v0 = ProgramVariable::new(0);
    let v1 = ProgramVariable::new(1);
    let v2 = ProgramVariable::new(2);
    let (patterns, bound) = if seed {
        (vec![QueryPattern::new(v0, v1, v2)], Vec::new())
    } else {
        (
            vec![
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
            vec![
                ProgramVariable::new(1),
                ProgramVariable::new(3),
                ProgramVariable::new(5),
            ],
        )
    };
    let variable_count = if seed { 3 } else { 6 };
    let program = QueryProgram::compile(gpu.archive(), variable_count, patterns).unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let host_frontier = if seed {
        ProgramFrontier::seed()
    } else {
        let parent = [
            program.encode(&raw(benchmark_id(0, 0))).unwrap().get(),
            program.encode(&raw(attribute)).unwrap().get(),
            program.encode(&raw(value)).unwrap().get(),
        ];
        program
            .frontier_from_indices(
                bound.clone(),
                (0..rows).flat_map(|_| parent).collect(),
                rows,
            )
            .unwrap()
    };
    let frontier = round.upload_frontier(&host_frontier).unwrap();
    let entity_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == v0)
        .unwrap() as u32;
    let choice_words: Vec<_> = (0..rows)
        .flat_map(|_| [v0.index() as u32, entity_arm, entity_count as u32])
        .collect();
    let choices = round
        .upload_choice_words_for_test(&frontier, &choice_words)
        .unwrap();
    let total = rows.checked_mul(entity_count).unwrap();

    // Shader compilation and allocator warmup stay outside every sample.
    let warm = round
        .enqueue_present_proposals(&frontier, &choices, total)
        .unwrap();
    assert_eq!(warm.completion_fence(), total as u32);

    const SAMPLES: usize = 5;
    let mut candidate_seconds = Vec::with_capacity(SAMPLES);
    let mut child_body_seconds = Vec::with_capacity(SAMPLES);
    let mut candidate_method = None;
    let mut child_body_method = None;
    for sample in 0..SAMPLES {
        let mut arena = round
            .enqueue_present_proposals_profiled_for_benchmark(&frontier, &choices, total)
            .unwrap();
        let profiles = arena.resolve_stage_profiles();
        assert_eq!(arena.completion_fence(), total as u32);
        candidate_method = Some(profiles.candidate_method);
        child_body_method = Some(profiles.child_body_method);
        candidate_seconds.push(profiles.candidate_duration.as_secs_f64());
        child_body_seconds.push(profiles.child_body_duration.as_secs_f64());

        if validate_contents && sample == 0 {
            let inspection = arena.inspect();
            assert_success(&inspection, total);
            let expected_codes: Vec<_> = (0..rows)
                .flat_map(|_| {
                    (0..entity_count).map(|ordinal| {
                        program
                            .encode(&raw(benchmark_id(0, ordinal as u64)))
                            .unwrap()
                            .get()
                    })
                })
                .collect();
            assert_eq!(&inspection.candidate_codes[..total], expected_codes);
            assert_eq!(
                &inspection.candidate_owners[..total],
                &(0..rows as u32)
                    .flat_map(|row| std::iter::repeat_n(row, entity_count))
                    .collect::<Vec<_>>()
            );
            assert_eq!(&inspection.proposer_arms[..total], &vec![entity_arm; total]);
        }
    }

    let mut wall_seconds = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = std::time::Instant::now();
        let arena = round
            .enqueue_present_proposals(&frontier, &choices, total)
            .unwrap();
        assert_eq!(arena.completion_fence(), total as u32);
        wall_seconds.push(start.elapsed().as_secs_f64());
    }

    ProposalBenchmarkSample {
        candidate_method: candidate_method.unwrap(),
        candidate_seconds: median(candidate_seconds),
        child_body_method: child_body_method.unwrap(),
        child_body_seconds: median(child_body_seconds),
        wall_seconds: median(wall_seconds),
    }
}

#[test]
#[ignore = "manual release-mode GPU benchmark"]
fn benchmark_present_proposal_candidate_geometry() {
    println!(
        "case,rows,width,total,candidate_timing,candidate_us,candidate_mproposal_s,body_timing,body_us,body_mword_s,wall_us,wall_mproposal_s"
    );
    let mut validated = false;
    let mut run = |label: &str, rows: usize, width: usize, seed: bool| {
        let total = rows * width;
        let sample = measure_present_proposal_case(width, rows, seed, !validated);
        validated = true;
        println!(
            "{label},{rows},{width},{total},{},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3}",
            sample.candidate_method,
            sample.candidate_seconds * 1e6,
            total as f64 / sample.candidate_seconds / 1e6,
            sample.child_body_method,
            sample.child_body_seconds * 1e6,
            total as f64 * if seed { 1.0 } else { 4.0 } / sample.child_body_seconds / 1e6,
            sample.wall_seconds * 1e6,
            total as f64 / sample.wall_seconds / 1e6,
        );
    };

    for exponent in 10..=18 {
        run("seed_width", 1, 1usize << exponent, true);
    }
    const FIXED_TOTAL: usize = 1 << 18;
    for rows in [1usize, 32, 64, 1024] {
        run("fixed_total", rows, FIXED_TOTAL / rows, false);
    }
}
