use std::collections::{BTreeMap, BTreeSet};

use super::*;
use jerky::bit_vector::{Rank, Select};
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

struct PairFixture {
    set: TribleSet,
    entities: [Id; 3],
    attributes: [Id; 3],
    values: [Id; 3],
    triples: Vec<(Id, Id, Id)>,
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

fn pair_fixture() -> PairFixture {
    // The semantic axes are deliberately globally interleaved, so compact
    // snapshot codes are E=[0,3,6], A=[1,4,7], V=[2,5,8]. Every expected
    // Pair result below is derived from these source IDs, never from a
    // Succinct rank/select oracle.
    let entities = [ordered_id(1), ordered_id(4), ordered_id(7)];
    let attributes = [ordered_id(2), ordered_id(5), ordered_id(8)];
    let values = [ordered_id(3), ordered_id(6), ordered_id(9)];
    let triples = vec![
        (entities[0], attributes[0], values[0]),
        (entities[0], attributes[0], values[1]),
        (entities[0], attributes[0], values[2]),
        (entities[0], attributes[1], values[0]),
        (entities[0], attributes[2], values[0]),
        (entities[1], attributes[0], values[0]),
        (entities[2], attributes[1], values[0]),
        (entities[2], attributes[1], values[1]),
    ];
    let mut set = TribleSet::new();
    for &(entity, attribute, value) in &triples {
        insert(&mut set, entity, attribute, value);
    }
    PairFixture {
        set,
        entities,
        attributes,
        values,
        triples,
    }
}

fn restricted_fixture() -> PairFixture {
    let mut fixture = pair_fixture();
    for triple in [
        (
            fixture.entities[1],
            fixture.attributes[1],
            fixture.values[1],
        ),
        (
            fixture.entities[2],
            fixture.attributes[0],
            fixture.values[2],
        ),
    ] {
        insert(&mut fixture.set, triple.0, triple.1, triple.2);
        fixture.triples.push(triple);
    }
    fixture
}

fn expected_pair_ids(fixture: &PairFixture, rotation: SuccinctRotation, peer: Id) -> Vec<Id> {
    let mut expected: Vec<_> = fixture
        .triples
        .iter()
        .filter_map(|&(entity, attribute, value)| match rotation {
            SuccinctRotation::Eav if entity == peer => Some(attribute),
            SuccinctRotation::Vea if value == peer => Some(entity),
            SuccinctRotation::Ave if attribute == peer => Some(value),
            SuccinctRotation::Vae if value == peer => Some(attribute),
            SuccinctRotation::Eva if entity == peer => Some(value),
            SuccinctRotation::Aev if attribute == peer => Some(entity),
            _ => None,
        })
        .collect();
    expected.sort_unstable();
    expected.dedup();
    expected
}

fn expected_restricted_ids(
    fixture: &PairFixture,
    rotation: SuccinctRotation,
    first: Id,
    last: Id,
) -> Vec<Id> {
    let mut expected: Vec<_> = fixture
        .triples
        .iter()
        .filter_map(|&(entity, attribute, value)| match rotation {
            SuccinctRotation::Eav if entity == first && value == last => Some(attribute),
            SuccinctRotation::Vea if value == first && attribute == last => Some(entity),
            SuccinctRotation::Ave if attribute == first && entity == last => Some(value),
            SuccinctRotation::Vae if value == first && entity == last => Some(attribute),
            SuccinctRotation::Eva if entity == first && attribute == last => Some(value),
            SuccinctRotation::Aev if attribute == first && value == last => Some(entity),
            _ => None,
        })
        .collect();
    expected.sort_unstable();
    expected.dedup();
    expected
}

fn restricted_source_ids(fixture: &PairFixture, rotation: SuccinctRotation) -> (&[Id], &[Id]) {
    match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Eva => (
            &fixture.entities,
            match rotation {
                SuccinctRotation::Eav => &fixture.values,
                SuccinctRotation::Eva => &fixture.attributes,
                _ => unreachable!(),
            },
        ),
        SuccinctRotation::Vea | SuccinctRotation::Vae => (
            &fixture.values,
            match rotation {
                SuccinctRotation::Vea => &fixture.attributes,
                SuccinctRotation::Vae => &fixture.entities,
                _ => unreachable!(),
            },
        ),
        SuccinctRotation::Ave | SuccinctRotation::Aev => (
            &fixture.attributes,
            match rotation {
                SuccinctRotation::Ave => &fixture.entities,
                SuccinctRotation::Aev => &fixture.values,
                _ => unreachable!(),
            },
        ),
    }
}

fn representative_restricted_pairs(
    fixture: &PairFixture,
    rotation: SuccinctRotation,
) -> [(Id, Id); 3] {
    let (first_ids, last_ids) = restricted_source_ids(fixture, rotation);
    let mut zero = None;
    let mut one = None;
    let mut many = None;
    for &first in first_ids {
        for &last in last_ids {
            match expected_restricted_ids(fixture, rotation, first, last).len() {
                0 => {
                    zero.get_or_insert((first, last));
                }
                1 => {
                    one.get_or_insert((first, last));
                }
                _ => {
                    many.get_or_insert((first, last));
                }
            }
        }
    }
    [
        zero.expect("fixture has a zero-width Restricted row"),
        one.expect("fixture has a singleton Restricted row"),
        many.expect("fixture has a multi-value Restricted row"),
    ]
}

fn prefix_select(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    code: usize,
) -> usize {
    match rotation {
        SuccinctRotation::Eav | SuccinctRotation::Eva => archive.e_a.select1(code).unwrap(),
        SuccinctRotation::Aev | SuccinctRotation::Ave => archive.a_a.select1(code).unwrap(),
        SuccinctRotation::Vea | SuccinctRotation::Vae => archive.v_a.select1(code).unwrap(),
    }
}

fn restricted_geometry(
    archive: &SuccinctArchive<OrderedUniverse>,
    rotation: SuccinctRotation,
    first: usize,
    last: usize,
) -> ([usize; PROPOSAL_WITNESS_WORDS], usize) {
    let lo = prefix_select(archive, rotation, first) - first;
    let hi = prefix_select(archive, rotation, first + 1) - (first + 1);
    let ring = archive.ring_col(rotation);
    let r0 = ring.rank(lo, last).unwrap();
    let r1 = ring.rank(hi, last).unwrap();
    let successor = successor_rotation(rotation);
    let base = prefix_select(archive, successor, last) - last;
    ([lo, hi, r0, r1], base)
}

fn restricted_anchor_pair(
    fixture: &PairFixture,
    program: &QueryProgram<'_, OrderedUniverse>,
    rotation: SuccinctRotation,
) -> (Id, Id) {
    let archive = program.archive();
    let successor = archive.ring_col(successor_rotation(rotation));
    let (first_ids, last_ids) = restricted_source_ids(fixture, rotation);
    for &first in first_ids {
        for &last in last_ids {
            let first_code = program.encode(&raw(first)).unwrap().index();
            let last_code = program.encode(&raw(last)).unwrap().index();
            let ([_, _, r0, r1], base) =
                restricted_geometry(archive, rotation, first_code, last_code);
            let start = base + r0;
            if base == 0 || r0 == 0 || r0 == r1 || start >= successor.len() {
                continue;
            }
            let previous_differs = start > 0
                && successor.access(start - 1).unwrap() != successor.access(start).unwrap();
            let next_differs = start + 1 < successor.len()
                && successor.access(start + 1).unwrap() != successor.access(start).unwrap();
            if previous_differs || next_differs {
                return (first, last);
            }
        }
    }
    panic!("fixture lacks a nonzero-base/nonzero-rank Restricted anchor for {rotation:?}")
}

fn restricted_bound_row(
    fixture: &PairFixture,
    rotation: SuccinctRotation,
    first: Id,
    last: Id,
) -> [Id; 3] {
    let mut row = [
        fixture.entities[0],
        fixture.attributes[0],
        fixture.values[0],
    ];
    match rotation {
        SuccinctRotation::Eav => {
            row[0] = first;
            row[2] = last;
        }
        SuccinctRotation::Vea => {
            row[2] = first;
            row[1] = last;
        }
        SuccinctRotation::Ave => {
            row[1] = first;
            row[0] = last;
        }
        SuccinctRotation::Vae => {
            row[2] = first;
            row[0] = last;
        }
        SuccinctRotation::Eva => {
            row[0] = first;
            row[1] = last;
        }
        SuccinctRotation::Aev => {
            row[1] = first;
            row[2] = last;
        }
    }
    row
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

fn pair_axes(rotation: SuccinctRotation) -> (ProgramVariable, ProgramVariable) {
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    match rotation {
        SuccinctRotation::Eav => (entity, attribute),
        SuccinctRotation::Vea => (value, entity),
        SuccinctRotation::Ave => (attribute, value),
        SuccinctRotation::Vae => (value, attribute),
        SuccinctRotation::Eva => (entity, value),
        SuccinctRotation::Aev => (attribute, entity),
    }
}

fn pair_cpu_pattern(
    rotation: SuccinctRotation,
    target: ProgramVariable,
    other: ProgramVariable,
    bound: ProgramVariable,
) -> QueryPattern {
    match rotation {
        SuccinctRotation::Eav => QueryPattern::new(bound, target, other),
        SuccinctRotation::Vea => QueryPattern::new(target, other, bound),
        SuccinctRotation::Ave => QueryPattern::new(other, bound, target),
        SuccinctRotation::Vae => QueryPattern::new(other, target, bound),
        SuccinctRotation::Eva => QueryPattern::new(bound, other, target),
        SuccinctRotation::Aev => QueryPattern::new(target, bound, other),
    }
}

fn pair_cpu_peer_rows(fixture: &PairFixture, rotation: SuccinctRotation) -> [Id; 3] {
    match rotation {
        // Every selected target width is no larger than the remaining
        // unbound width. With target v0, the CPU planner therefore chooses the
        // same Pair arm on every row, including the repeated final parent.
        SuccinctRotation::Eav => [
            fixture.entities[0],
            fixture.entities[2],
            fixture.entities[0],
        ],
        SuccinctRotation::Eva => [
            fixture.entities[0],
            fixture.entities[1],
            fixture.entities[0],
        ],
        SuccinctRotation::Vea | SuccinctRotation::Vae => {
            [fixture.values[0], fixture.values[1], fixture.values[0]]
        }
        SuccinctRotation::Ave => [
            fixture.attributes[1],
            fixture.attributes[2],
            fixture.attributes[1],
        ],
        SuccinctRotation::Aev => [
            fixture.attributes[0],
            fixture.attributes[1],
            fixture.attributes[0],
        ],
    }
}

fn restricted_cpu_pattern(
    rotation: SuccinctRotation,
    target: ProgramVariable,
    first: ProgramVariable,
    last: ProgramVariable,
) -> QueryPattern {
    match rotation {
        SuccinctRotation::Eav => QueryPattern::new(first, target, last),
        SuccinctRotation::Vea => QueryPattern::new(target, last, first),
        SuccinctRotation::Ave => QueryPattern::new(last, first, target),
        SuccinctRotation::Vae => QueryPattern::new(last, target, first),
        SuccinctRotation::Eva => QueryPattern::new(first, last, target),
        SuccinctRotation::Aev => QueryPattern::new(target, first, last),
    }
}

fn enqueue_pair_case<U: Universe>(
    gpu: &crate::WgpuSuccinctArchive<U>,
    program: &QueryProgram<'_, U>,
    rotation: SuccinctRotation,
    peer: Id,
    capacity: usize,
    confirmed: bool,
) -> (ProposalInspection, u32, u32, u32) {
    let (bound, target) = pair_axes(rotation);
    let round = WgpuResidentRound::new(gpu, program, &[bound]).unwrap();
    let arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::PairDistinct {
                    rotation: candidate,
                    ..
                } if *candidate == rotation
            )
        })
        .expect("requested Pair rotation is admitted");
    assert_eq!(round.proposal_arm_pair_rotation(arm), Some(rotation));
    let peer_code = program.encode(&raw(peer)).unwrap().get();
    let host = program
        .frontier_from_indices(vec![bound], vec![peer_code], 1)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let witness = arm * PROPOSAL_WITNESS_WORDS;
    let count = witnesses[witness + 3] - witnesses[witness + 2];
    let choices = round
        .force_choice_words_from_inputs_for_test(
            &frontier,
            &inputs,
            &[target.index() as u32, arm as u32, count],
        )
        .unwrap();
    let arena = if confirmed {
        round.enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, capacity)
    } else {
        round.enqueue_generic_proposals_for_test(&frontier, &choices, capacity)
    }
    .unwrap();
    let inspection = arena.inspect();
    (inspection, arm as u32, peer_code, target.index() as u32)
}

#[test]
fn generic_restricted_generation_matches_cpu_order_for_natural_rotation() {
    let fixture = pair_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[entity, attribute]).unwrap();
    let (arm, rotation) = round
        .proposal_arm_specs()
        .iter()
        .enumerate()
        .find_map(|(arm, spec)| match spec {
            ArmSpec::Restricted { rotation, .. } => Some((arm, *rotation)),
            ArmSpec::Present { .. } | ArmSpec::PairDistinct { .. } => None,
        })
        .unwrap();
    assert_eq!(rotation, SuccinctRotation::Eva);
    let first = fixture.entities[0];
    let last = fixture.attributes[0];
    let first_code = program.encode(&raw(first)).unwrap().get();
    let last_code = program.encode(&raw(last)).unwrap().get();
    let host = program
        .frontier_from_indices(vec![entity, attribute], vec![first_code, last_code], 1)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let witness = arm * PROPOSAL_WITNESS_WORDS;
    let count = witnesses[witness + 3] - witnesses[witness + 2];
    let choices = round
        .force_choice_words_from_inputs_for_test(
            &frontier,
            &inputs,
            &[value.index() as u32, arm as u32, count],
        )
        .unwrap();
    let expected = codes(
        &program,
        &expected_restricted_ids(&fixture, rotation, first, last),
    );
    assert_eq!(count as usize, expected.len());
    let inspection = round
        .enqueue_generic_proposals_for_test(&frontier, &choices, expected.len() + 3)
        .unwrap()
        .inspect();
    let confirmed = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, expected.len() + 3)
        .unwrap()
        .inspect();
    assert_eq!(confirmed, inspection);
    assert_success(&inspection, expected.len());
    assert_eq!(&inspection.candidate_codes[..expected.len()], expected);
    assert_eq!(
        &inspection.candidate_owners[..expected.len()],
        vec![0; expected.len()]
    );
    assert_eq!(
        &inspection.proposer_arms[..expected.len()],
        vec![arm as u32; expected.len()]
    );
    let expected_body = expected
        .iter()
        .flat_map(|&candidate| [first_code, last_code, candidate])
        .collect::<Vec<_>>();
    // Canonical child columns are E,A,V; target V is inserted last.
    assert_eq!(&inspection.child_body[..expected_body.len()], expected_body);
}

#[test]
fn physical_restricted_generation_covers_all_six_rotations_and_capacity_edges() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let targets = std::array::from_fn::<_, 6, _>(|index| ProgramVariable::new(index as u8));
    let entity_peer = ProgramVariable::new(6);
    let attribute_peer = ProgramVariable::new(7);
    let value_peer = ProgramVariable::new(8);
    let program = QueryProgram::compile(
        gpu.archive(),
        9,
        [
            QueryPattern::new(entity_peer, targets[0], value_peer),
            QueryPattern::new(targets[1], attribute_peer, value_peer),
            QueryPattern::new(entity_peer, attribute_peer, targets[2]),
            QueryPattern::new(entity_peer, targets[3], value_peer),
            QueryPattern::new(entity_peer, attribute_peer, targets[4]),
            QueryPattern::new(targets[5], attribute_peer, value_peer),
        ],
    )
    .unwrap();
    let mut round =
        WgpuResidentRound::new(&gpu, &program, &[entity_peer, attribute_peer, value_peer]).unwrap();
    let rotations = [
        SuccinctRotation::Eav,
        SuccinctRotation::Vea,
        SuccinctRotation::Ave,
        SuccinctRotation::Vae,
        SuccinctRotation::Eva,
        SuccinctRotation::Aev,
    ];
    let physical_specs = round
        .proposal_arm_specs()
        .iter()
        .copied()
        .enumerate()
        .map(|(arm, spec)| {
            let ArmSpec::Restricted {
                arm: spec_arm,
                rotation,
                first,
                last,
            } = spec
            else {
                panic!("all six fixture arms must lower as Restricted")
            };
            assert_eq!(spec_arm as usize, arm);
            let desired = rotations[arm];
            if desired == rotation {
                spec
            } else {
                assert_eq!(desired, inverse_restricted_rotation(rotation));
                ArmSpec::Restricted {
                    arm: spec_arm,
                    rotation: desired,
                    first: last,
                    last: first,
                }
            }
        })
        .collect();
    round
        .reconfigure_restricted_proposal_arms_for_test(physical_specs)
        .unwrap();
    assert_eq!(
        round
            .proposal_arm_specs()
            .iter()
            .map(|spec| match spec {
                ArmSpec::Restricted { rotation, .. } => *rotation,
                ArmSpec::Present { .. } | ArmSpec::PairDistinct { .. } => unreachable!(),
            })
            .collect::<Vec<_>>(),
        rotations
    );

    struct Case {
        target: ProgramVariable,
        arm: u32,
        rotation: SuccinctRotation,
        first: u32,
        last: u32,
        parent: [u32; 3],
        expected: Vec<u32>,
    }

    let source_pairs = rotations.map(|rotation| {
        let [zero, one, many] = representative_restricted_pairs(&fixture, rotation);
        [
            zero,
            one,
            many,
            restricted_anchor_pair(&fixture, &program, rotation),
        ]
    });
    let mut cases = Vec::with_capacity(rotations.len() * 4);
    // Interleave variables in the affine input. Publication must nevertheless
    // be variable-major and stable by original row within each variable.
    let category_pairs = std::array::from_fn::<_, 4, _>(|category| {
        std::array::from_fn::<_, 6, _>(|arm| source_pairs[arm][category])
    });
    for pairs in category_pairs {
        for (arm, (&rotation, (first, last))) in rotations.iter().zip(pairs).enumerate() {
            let parent_ids = restricted_bound_row(&fixture, rotation, first, last);
            let parent = parent_ids.map(|id| program.encode(&raw(id)).unwrap().get());
            cases.push(Case {
                target: targets[arm],
                arm: arm as u32,
                rotation,
                first: program.encode(&raw(first)).unwrap().get(),
                last: program.encode(&raw(last)).unwrap().get(),
                parent,
                expected: codes(
                    &program,
                    &expected_restricted_ids(&fixture, rotation, first, last),
                ),
            });
        }
    }
    let parent_values = cases
        .iter()
        .flat_map(|case| case.parent)
        .collect::<Vec<_>>();
    let host = program
        .frontier_from_indices(
            vec![entity_peer, attribute_peer, value_peer],
            parent_values,
            cases.len(),
        )
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let mut choice_words = Vec::with_capacity(cases.len() * CHOICE_WORDS);
    for (row, case) in cases.iter().enumerate() {
        let witness = (case.arm as usize * cases.len() + row) * PROPOSAL_WITNESS_WORDS;
        let (expected_witness, _) = restricted_geometry(
            program.archive(),
            case.rotation,
            case.first as usize,
            case.last as usize,
        );
        assert_eq!(
            &witnesses[witness..witness + PROPOSAL_WITNESS_WORDS],
            &expected_witness.map(|word| word as u32),
            "rotation {:?}, row {row}",
            case.rotation
        );
        assert_eq!(
            witnesses[witness + 3] - witnesses[witness + 2],
            case.expected.len() as u32
        );
        choice_words.extend([
            case.target.index() as u32,
            case.arm,
            case.expected.len() as u32,
        ]);
    }
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
        .unwrap();
    let required = cases.iter().map(|case| case.expected.len()).sum::<usize>();
    assert!(required > 1);
    let exact = round
        .enqueue_physical_restricted_proposals_for_test(&frontier, &choices, required)
        .unwrap()
        .inspect();
    let confirmed_arena = round
        .enqueue_confirmed_physical_restricted_proposals_for_test(&frontier, &choices, required)
        .unwrap();
    let confirmation_trace = confirmed_arena.read_semantic_confirmation_work_for_test();
    for arm in 0..rotations.len() {
        let target_count = cases
            .iter()
            .filter(|case| case.arm == arm as u32)
            .map(|case| case.expected.len() as u32)
            .sum::<u32>();
        assert_eq!(
            confirmation_trace[arm],
            [target_count * 2, cases.len() as u32],
            "Restricted confirmation work for {:?}",
            rotations[arm]
        );
    }
    let confirmed = confirmed_arena.inspect();
    assert_eq!(confirmed, exact);
    assert_success(&exact, required);

    let mut expected_codes = Vec::with_capacity(required);
    let mut expected_owners = Vec::with_capacity(required);
    let mut expected_arms = Vec::with_capacity(required);
    let mut expected_body = Vec::with_capacity(required * 4);
    let mut expected_base = 0usize;
    for &target in &targets {
        let segment = &exact.segments[target.index()];
        let segment_count = cases
            .iter()
            .filter(|case| case.target == target)
            .map(|case| case.expected.len())
            .sum::<usize>();
        assert_eq!(segment.variable, target.index() as u32);
        assert_eq!(segment.insertion, 0);
        assert_eq!(segment.base, expected_base as u32);
        assert_eq!(segment.count, segment_count as u32);
        expected_base += segment_count;
        for (row, case) in cases
            .iter()
            .enumerate()
            .filter(|(_, case)| case.target == target)
        {
            for &candidate in &case.expected {
                expected_codes.push(candidate);
                expected_owners.push(row as u32);
                expected_arms.push(case.arm);
                expected_body.extend([candidate, case.parent[0], case.parent[1], case.parent[2]]);
            }
        }
    }
    assert_eq!(expected_base, required);
    assert_eq!(exact.candidate_codes, expected_codes);
    assert_eq!(exact.candidate_owners, expected_owners);
    assert_eq!(exact.proposer_arms, expected_arms);
    assert_eq!(exact.child_body, expected_body);

    for capacity in [required - 1, 0] {
        let failed = round
            .enqueue_physical_restricted_proposals_for_test(&frontier, &choices, capacity)
            .unwrap()
            .inspect();
        assert_eq!(failed.status, STATUS_CAPACITY);
        assert_eq!(failed.required, required as u32);
        assert_eq!(failed.logical_len, 0);
        assert_eq!((failed.dispatch_x, failed.dispatch_y), (0, 1));
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
fn confirmed_physical_restricted_all_six_rotations_match_cpu_selected_rows() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let target = ProgramVariable::new(0);
    let first_variable = ProgramVariable::new(1);
    let last_variable = ProgramVariable::new(2);

    for rotation in SuccinctRotation::ALL {
        let program = QueryProgram::compile(
            gpu.archive(),
            3,
            [restricted_cpu_pattern(
                rotation,
                target,
                first_variable,
                last_variable,
            )],
        )
        .unwrap();
        let mut round =
            WgpuResidentRound::new(&gpu, &program, &[first_variable, last_variable]).unwrap();
        let physical_specs = round
            .proposal_arm_specs()
            .iter()
            .copied()
            .map(|spec| {
                let ArmSpec::Restricted {
                    arm,
                    rotation: canonical,
                    first,
                    last,
                } = spec
                else {
                    panic!("two-bound single-pattern round must be Restricted")
                };
                if canonical == rotation {
                    spec
                } else {
                    assert_eq!(rotation, inverse_restricted_rotation(canonical));
                    ArmSpec::Restricted {
                        arm,
                        rotation,
                        first: last,
                        last: first,
                    }
                }
            })
            .collect();
        round
            .reconfigure_restricted_proposal_arms_for_test(physical_specs)
            .unwrap();
        let arm = round
            .proposal_arm_specs()
            .iter()
            .position(|spec| {
                matches!(
                    spec,
                    ArmSpec::Restricted {
                        rotation: candidate,
                        ..
                    } if *candidate == rotation
                )
            })
            .expect("physical Restricted rotation is installed");

        let pairs = representative_restricted_pairs(&fixture, rotation);
        let mut parent_values = Vec::with_capacity(pairs.len() * 2);
        for &(first, last) in &pairs {
            parent_values.extend([
                program.encode(&raw(first)).unwrap().get(),
                program.encode(&raw(last)).unwrap().get(),
            ]);
        }
        let host = program
            .frontier_from_indices(
                vec![first_variable, last_variable],
                parent_values,
                pairs.len(),
            )
            .unwrap();
        let cpu = program.transition(&host).unwrap();
        assert_eq!(cpu.len(), 1, "CPU transition group for {rotation:?}");
        assert_eq!(
            cpu[0].variables(),
            &[target, first_variable, last_variable],
            "CPU transition variable for {rotation:?}"
        );
        let cpu_body = cpu[0]
            .values()
            .iter()
            .map(|code| code.get())
            .collect::<Vec<_>>();

        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();
        let mut choice_words = Vec::with_capacity(pairs.len() * CHOICE_WORDS);
        let mut expected_owners = Vec::new();
        for (row, &(first, last)) in pairs.iter().enumerate() {
            let witness = (arm * pairs.len() + row) * PROPOSAL_WITNESS_WORDS;
            let count = witnesses[witness + 3] - witnesses[witness + 2];
            assert_eq!(
                count as usize,
                expected_restricted_ids(&fixture, rotation, first, last).len(),
                "raw selected-row width for {rotation:?}, row {row}"
            );
            expected_owners.extend(std::iter::repeat_n(row as u32, count as usize));
            choice_words.extend([target.index() as u32, arm as u32, count]);
        }
        let choices = round
            .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
            .unwrap();
        let expected = cpu[0].len();
        assert_eq!(expected_owners.len(), expected);
        let arena = round
            .enqueue_confirmed_physical_restricted_proposals_for_test(&frontier, &choices, expected)
            .unwrap();
        let trace = arena.read_semantic_confirmation_work_for_test();
        assert_eq!(trace[arm], [(expected * 2) as u32, pairs.len() as u32]);
        let inspection = arena.inspect();
        assert_success(&inspection, expected);
        assert_eq!(inspection.child_body, cpu_body, "{rotation:?}");
        assert_eq!(inspection.candidate_owners, expected_owners, "{rotation:?}");
        assert!(inspection
            .proposer_arms
            .iter()
            .all(|&proposer| proposer == arm as u32));
        let cpu_candidates = cpu_body
            .chunks_exact(inspection.child_stride as usize)
            .map(|row| row[0])
            .collect::<Vec<_>>();
        assert_eq!(inspection.candidate_codes, cpu_candidates, "{rotation:?}");
    }
}

#[test]
fn restricted_zero_width_accepts_successor_end_at_zero_capacity() {
    let mut fixture = restricted_fixture();
    // This code belongs to the archive domain only as an entity and sorts
    // after every attribute. Used as an attribute peer below, its successor
    // prefix base is exactly N: the valid empty interval starts at the end.
    let absent_attribute = ordered_id(11);
    let domain_triple = (absent_attribute, fixture.attributes[2], fixture.values[2]);
    insert(
        &mut fixture.set,
        domain_triple.0,
        domain_triple.1,
        domain_triple.2,
    );
    fixture.triples.push(domain_triple);
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[entity, attribute]).unwrap();
    let ArmSpec::Restricted { arm, rotation, .. } = round.proposal_arm_specs()[0] else {
        panic!("two-peer fixture must lower as Restricted")
    };
    assert_eq!(rotation, SuccinctRotation::Eva);
    let first = fixture.entities[0];
    assert!(expected_restricted_ids(&fixture, rotation, first, absent_attribute).is_empty());
    let first_code = program.encode(&raw(first)).unwrap().get();
    let last_code = program.encode(&raw(absent_attribute)).unwrap().get();
    let (witness, base) = restricted_geometry(
        program.archive(),
        rotation,
        first_code as usize,
        last_code as usize,
    );
    assert_eq!(
        base,
        program
            .archive()
            .ring_col(successor_rotation(rotation))
            .len()
    );
    assert_eq!(witness[2], witness[3]);
    assert_eq!(
        base + witness[2],
        program.archive().ring_col(rotation).len()
    );

    let host = program
        .frontier_from_indices(vec![entity, attribute], vec![first_code, last_code], 1)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    assert_eq!(
        inputs.read_proposal_witnesses_for_test(),
        witness.map(|word| word as u32)
    );
    let choices = round
        .force_choice_words_from_inputs_for_test(
            &frontier,
            &inputs,
            &[value.index() as u32, arm, 0],
        )
        .unwrap();
    let inspection = round
        .enqueue_generic_proposals_for_test(&frontier, &choices, 0)
        .unwrap()
        .inspect();
    let confirmed_arena = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, 0)
        .unwrap();
    assert_eq!(
        confirmed_arena.read_semantic_confirmation_work_for_test(),
        vec![[0, 1]],
        "N..N performs one owner-row prefix translation and no rank probes"
    );
    let confirmed = confirmed_arena.inspect();
    assert_eq!(confirmed, inspection);
    assert_success(&inspection, 0);
    assert_eq!(inspection.segments.len(), 1);
    assert_eq!(inspection.segments[0].variable, value.index() as u32);
    assert_eq!(inspection.segments[0].base, 0);
    assert_eq!(inspection.segments[0].count, 0);
    assert_eq!(inspection.segments[0].insertion, 2);
    assert!(inspection.candidate_codes.is_empty());
    assert!(inspection.candidate_owners.is_empty());
    assert!(inspection.proposer_arms.is_empty());
    assert!(inspection.child_body.is_empty());
}

#[test]
fn restricted_source_shape_matrix_allows_only_canonical_or_exact_inverse() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let targets = std::array::from_fn::<_, 12, _>(|index| ProgramVariable::new(index as u8));
    let entity_peer = ProgramVariable::new(12);
    let attribute_peer = ProgramVariable::new(13);
    let value_peer = ProgramVariable::new(14);
    let canonical_rotations = [
        SuccinctRotation::Eav,
        SuccinctRotation::Aev,
        SuccinctRotation::Eva,
    ];
    let source_shapes = [(true, true), (true, false), (false, true), (false, false)];
    let canonical_pairs =
        canonical_rotations.map(|rotation| representative_restricted_pairs(&fixture, rotation)[2]);
    assert_eq!(canonical_pairs[0], (fixture.entities[0], fixture.values[0]));
    assert_eq!(
        canonical_pairs[1],
        (fixture.attributes[0], fixture.values[0])
    );
    assert_eq!(
        canonical_pairs[2],
        (fixture.entities[0], fixture.attributes[0])
    );

    let mut patterns = Vec::with_capacity(targets.len());
    for (rotation_index, &rotation) in canonical_rotations.iter().enumerate() {
        let (first_id, last_id) = canonical_pairs[rotation_index];
        for (shape, &(constant_first, constant_last)) in source_shapes.iter().enumerate() {
            let target = targets[rotation_index * source_shapes.len() + shape];
            let first_variable = match rotation {
                SuccinctRotation::Eav | SuccinctRotation::Eva => entity_peer,
                SuccinctRotation::Aev => attribute_peer,
                SuccinctRotation::Vea | SuccinctRotation::Ave | SuccinctRotation::Vae => {
                    unreachable!()
                }
            };
            let last_variable = match rotation {
                SuccinctRotation::Eav | SuccinctRotation::Aev => value_peer,
                SuccinctRotation::Eva => attribute_peer,
                SuccinctRotation::Vea | SuccinctRotation::Ave | SuccinctRotation::Vae => {
                    unreachable!()
                }
            };
            let first = if constant_first {
                QueryTerm::Constant(raw(first_id))
            } else {
                QueryTerm::Variable(first_variable)
            };
            let last = if constant_last {
                QueryTerm::Constant(raw(last_id))
            } else {
                QueryTerm::Variable(last_variable)
            };
            patterns.push(match rotation {
                SuccinctRotation::Eav => QueryPattern::new(first, target, last),
                SuccinctRotation::Aev => QueryPattern::new(target, first, last),
                SuccinctRotation::Eva => QueryPattern::new(first, last, target),
                SuccinctRotation::Vea | SuccinctRotation::Ave | SuccinctRotation::Vae => {
                    unreachable!()
                }
            });
        }
    }
    let program = QueryProgram::compile(gpu.archive(), 15, patterns).unwrap();
    let mut round =
        WgpuResidentRound::new(&gpu, &program, &[entity_peer, attribute_peer, value_peer]).unwrap();
    let canonical_specs = round.proposal_arm_specs().to_vec();
    assert_eq!(canonical_specs.len(), targets.len());

    let source_words = |source: CodeSource| match source {
        CodeSource::Constant(code) => [CONSTANT_SOURCE, code],
        CodeSource::Column(column) => [COLUMN_SOURCE, u32::from(column)],
    };
    let assert_source_table = |admission: &ProposalAdmission, specs: &[ArmSpec]| {
        for (arm, &spec) in specs.iter().enumerate() {
            let ArmSpec::Restricted { first, last, .. } = spec else {
                panic!("source-shape fixture lowered a non-Restricted arm")
            };
            let mut expected = Vec::with_capacity(RESTRICTED_SOURCE_WORDS_PER_ARM);
            expected.extend(source_words(first));
            expected.extend(source_words(last));
            let base = arm * RESTRICTED_SOURCE_WORDS_PER_ARM;
            assert_eq!(
                &admission.restricted_sources[base..base + RESTRICTED_SOURCE_WORDS_PER_ARM],
                expected,
                "arm {arm}"
            );
        }
    };
    let canonical_admission =
        lower_proposal_admission(&round, ProposerPolicy::RestrictedNatural).unwrap();
    assert_source_table(&canonical_admission, &canonical_specs);
    for (arm, &spec) in canonical_specs.iter().enumerate() {
        let ArmSpec::Restricted {
            rotation,
            first,
            last,
            ..
        } = spec
        else {
            unreachable!()
        };
        assert_eq!(rotation, canonical_rotations[arm / source_shapes.len()]);
        let (constant_first, constant_last) = source_shapes[arm % source_shapes.len()];
        assert_eq!(matches!(first, CodeSource::Constant(_)), constant_first);
        assert_eq!(matches!(last, CodeSource::Constant(_)), constant_last);
    }

    let parent = [
        fixture.entities[0],
        fixture.attributes[0],
        fixture.values[0],
    ]
    .map(|id| program.encode(&raw(id)).unwrap().get());
    let parent_values = (0..targets.len()).flat_map(|_| parent).collect::<Vec<_>>();
    let host = program
        .frontier_from_indices(
            vec![entity_peer, attribute_peer, value_peer],
            parent_values,
            targets.len(),
        )
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let expected_by_arm = canonical_rotations
        .iter()
        .enumerate()
        .flat_map(|(rotation_index, &rotation)| {
            let (first, last) = canonical_pairs[rotation_index];
            let expected = codes(
                &program,
                &expected_restricted_ids(&fixture, rotation, first, last),
            );
            (0..source_shapes.len()).map(move |_| expected.clone())
        })
        .collect::<Vec<_>>();
    let choice_words = targets
        .iter()
        .enumerate()
        .flat_map(|(arm, target)| {
            [
                target.index() as u32,
                arm as u32,
                expected_by_arm[arm].len() as u32,
            ]
        })
        .collect::<Vec<_>>();
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
        .unwrap();
    let required = expected_by_arm.iter().map(Vec::len).sum::<usize>();

    // A same-rotation source swap remains type/width coherent and would pass
    // the old axis-only gate, but it is neither the semantic arm nor its
    // physical inverse. Failed validation must leave the live round untouched.
    let mut relabelled = canonical_specs.clone();
    let ArmSpec::Restricted {
        arm,
        rotation,
        first,
        last,
    } = relabelled[3]
    else {
        unreachable!()
    };
    relabelled[3] = ArmSpec::Restricted {
        arm,
        rotation,
        first: last,
        last: first,
    };
    assert!(matches!(
        round.reconfigure_restricted_proposal_arms_for_test(relabelled),
        Err(ResidentSupportError::MalformedResidentPlan)
    ));
    assert_eq!(round.proposal_arm_specs(), canonical_specs);
    assert!(round.initialize_inputs(&frontier).is_ok());

    let mut unswapped_inverse = canonical_specs.clone();
    let ArmSpec::Restricted {
        arm,
        rotation,
        first,
        last,
    } = unswapped_inverse[3]
    else {
        unreachable!()
    };
    unswapped_inverse[3] = ArmSpec::Restricted {
        arm,
        rotation: inverse_restricted_rotation(rotation),
        first,
        last,
    };
    assert!(matches!(
        round.reconfigure_restricted_proposal_arms_for_test(unswapped_inverse),
        Err(ResidentSupportError::MalformedResidentPlan)
    ));
    assert_eq!(round.proposal_arm_specs(), canonical_specs);

    let canonical = round
        .enqueue_generic_proposals_for_test(&frontier, &choices, required)
        .unwrap()
        .inspect();
    assert_success(&canonical, required);
    let confirmed_canonical = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, required)
        .unwrap()
        .inspect();
    assert_eq!(confirmed_canonical, canonical);

    let inverse_specs = canonical_specs
        .iter()
        .copied()
        .map(|spec| match spec {
            ArmSpec::Restricted {
                arm,
                rotation,
                first,
                last,
            } => ArmSpec::Restricted {
                arm,
                rotation: inverse_restricted_rotation(rotation),
                first: last,
                last: first,
            },
            ArmSpec::Present { .. } | ArmSpec::PairDistinct { .. } => unreachable!(),
        })
        .collect::<Vec<_>>();
    round
        .reconfigure_restricted_proposal_arms_for_test(inverse_specs.clone())
        .unwrap();
    assert!(matches!(
        round.initialize_inputs(&frontier),
        Err(ResidentSupportError::FrontierOwnership)
    ));
    assert!(matches!(
        round.enqueue(&inputs),
        Err(ResidentRoundError::InputOwnership)
    ));
    assert!(matches!(
        lower_proposal_admission(&round, ProposerPolicy::RestrictedNatural),
        Err(ResidentProposalError::MalformedPlan)
    ));
    let inverse_admission =
        lower_proposal_admission(&round, ProposerPolicy::RestrictedPhysical).unwrap();
    assert_source_table(&inverse_admission, &inverse_specs);
    for (arm, &spec) in inverse_specs.iter().enumerate() {
        let ArmSpec::Restricted {
            rotation,
            first,
            last,
            ..
        } = spec
        else {
            unreachable!()
        };
        assert_eq!(
            rotation,
            inverse_restricted_rotation(canonical_rotations[arm / source_shapes.len()])
        );
        let (canonical_first, canonical_last) = source_shapes[arm % source_shapes.len()];
        assert_eq!(matches!(first, CodeSource::Constant(_)), canonical_last);
        assert_eq!(matches!(last, CodeSource::Constant(_)), canonical_first);
    }

    let inverse_frontier = round.upload_frontier(&host).unwrap();
    let inverse_inputs = round.initialize_inputs(&inverse_frontier).unwrap();
    let inverse_choices = round
        .force_choice_words_from_inputs_for_test(&inverse_frontier, &inverse_inputs, &choice_words)
        .unwrap();
    let inverse = round
        .enqueue_physical_restricted_proposals_for_test(
            &inverse_frontier,
            &inverse_choices,
            required,
        )
        .unwrap()
        .inspect();
    assert_eq!(inverse, canonical);
}

#[test]
fn confirmed_mixed_present_pair_restricted_is_stable_and_row_homomorphic() {
    const ROWS: usize = 65;
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let targets = std::array::from_fn::<_, 6, _>(|index| ProgramVariable::new(index as u8));
    let entity_peer = ProgramVariable::new(6);
    let attribute_peer = ProgramVariable::new(7);
    let program = QueryProgram::compile(
        gpu.archive(),
        8,
        [
            QueryPattern::new(targets[0], targets[1], targets[2]),
            QueryPattern::new(entity_peer, targets[3], targets[4]),
            QueryPattern::new(entity_peer, attribute_peer, targets[5]),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[entity_peer, attribute_peer]).unwrap();
    assert_eq!(round.proposal_arm_specs().len(), targets.len());
    assert!(matches!(
        round.proposal_arm_specs()[0],
        ArmSpec::Present { .. }
    ));
    assert!(matches!(
        round.proposal_arm_specs()[1],
        ArmSpec::Present { .. }
    ));
    assert!(matches!(
        round.proposal_arm_specs()[2],
        ArmSpec::Present { .. }
    ));
    assert!(matches!(
        round.proposal_arm_specs()[3],
        ArmSpec::PairDistinct { .. }
    ));
    assert!(matches!(
        round.proposal_arm_specs()[4],
        ArmSpec::PairDistinct { .. }
    ));
    assert!(matches!(
        round.proposal_arm_specs()[5],
        ArmSpec::Restricted { .. }
    ));

    let parent_cycle = [
        [fixture.entities[0], fixture.attributes[0]],
        [fixture.entities[0], fixture.attributes[1]],
        [fixture.entities[2], fixture.attributes[0]],
        [fixture.entities[1], fixture.attributes[2]],
    ];
    let arm_cycle = [0usize, 3, 5, 1, 4, 2, 5, 3, 0, 4, 5];
    let parent_ids = (0..ROWS)
        .map(|row| parent_cycle[row % parent_cycle.len()])
        .collect::<Vec<_>>();
    let parent_values = parent_ids
        .iter()
        .flat_map(|row| row.map(|id| program.encode(&raw(id)).unwrap().get()))
        .collect::<Vec<_>>();
    let host = program
        .frontier_from_indices(vec![entity_peer, attribute_peer], parent_values, ROWS)
        .unwrap();

    let source_id = |source: CodeSource, parent: [Id; 2]| match source {
        CodeSource::Column(column) => parent[usize::from(column)],
        CodeSource::Constant(_) => panic!("mixed fixture has no constant source"),
    };
    let ids_for = |spec: ArmSpec, parent: [Id; 2]| -> Vec<Id> {
        match spec {
            ArmSpec::Present { axis, .. } => match axis {
                ResidentAxis::Entity => fixture.entities.to_vec(),
                ResidentAxis::Attribute => fixture.attributes.to_vec(),
                ResidentAxis::Value => fixture.values.to_vec(),
            },
            ArmSpec::PairDistinct { rotation, peer, .. } => {
                expected_pair_ids(&fixture, rotation, source_id(peer, parent))
            }
            ArmSpec::Restricted {
                rotation,
                first,
                last,
                ..
            } => expected_restricted_ids(
                &fixture,
                rotation,
                source_id(first, parent),
                source_id(last, parent),
            ),
        }
    };
    let mut row_arms = Vec::with_capacity(ROWS);
    let mut row_candidates = Vec::with_capacity(ROWS);
    let mut choice_words = Vec::with_capacity(ROWS * CHOICE_WORDS);
    for row in 0..ROWS {
        // Keep a live Restricted row in the second 64-lane block. This is the
        // boundary where a source-index/dispatch-lane mix-up previously hid.
        let arm = if row == 64 {
            5
        } else {
            arm_cycle[row % arm_cycle.len()]
        };
        let target = round.metadata().arms()[arm].target_variable();
        let candidates = codes(
            &program,
            &ids_for(round.proposal_arm_specs()[arm], parent_ids[row]),
        );
        choice_words.extend([target.index() as u32, arm as u32, candidates.len() as u32]);
        row_arms.push(arm);
        row_candidates.push(candidates);
    }
    assert_eq!(row_arms[64], 5);
    assert!(
        !row_candidates[64].is_empty(),
        "block-one Restricted row must exercise a nonzero interval"
    );
    assert!(row_arms
        .windows(arm_cycle.len())
        .any(|window| window == arm_cycle));
    assert!(parent_ids
        .windows(parent_cycle.len())
        .any(|window| window == parent_cycle));

    let inspect_range = |range: std::ops::Range<usize>| {
        let sliced = host.slice(range.clone()).unwrap();
        let frontier = round.upload_frontier(&sliced).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let words = &choice_words[range.start * CHOICE_WORDS..range.end * CHOICE_WORDS];
        let capacity = words
            .chunks_exact(CHOICE_WORDS)
            .map(|choice| choice[2] as usize)
            .sum();
        let choices = round
            .force_choice_words_from_inputs_for_test(&frontier, &inputs, words)
            .unwrap();
        let arena = round
            .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, capacity)
            .unwrap();
        let trace = arena.read_semantic_confirmation_work_for_test();
        for (arm, spec) in round.proposal_arm_specs().iter().enumerate() {
            let target = round.metadata().arms()[arm].target_variable();
            let provisional_target = range
                .clone()
                .filter(|&row| round.metadata().arms()[row_arms[row]].target_variable() == target)
                .map(|row| row_candidates[row].len() as u32)
                .sum::<u32>();
            let expected = match spec {
                ArmSpec::Present { .. } => [0, 0],
                ArmSpec::PairDistinct { .. } => [provisional_target * 2, 0],
                ArmSpec::Restricted { .. } => [provisional_target * 2, range.len() as u32],
            };
            assert_eq!(trace[arm], expected, "arm {arm}, rows {range:?}");
        }
        let inspection = arena.inspect();
        assert_success(&inspection, capacity);
        inspection
    };

    let whole = inspect_range(0..ROWS);
    let repeated = inspect_range(0..ROWS);
    assert_eq!(
        repeated, whole,
        "identical immutable inputs changed output bytes"
    );

    let required = row_candidates.iter().map(Vec::len).sum::<usize>();
    assert_success(&whole, required);
    let mut expected_codes = Vec::with_capacity(required);
    let mut expected_owners = Vec::with_capacity(required);
    let mut expected_arms = Vec::with_capacity(required);
    let mut expected_body = Vec::with_capacity(required * 3);
    for &target in &targets {
        for row in 0..ROWS {
            let arm = row_arms[row];
            if round.metadata().arms()[arm].target_variable() != target {
                continue;
            }
            for &candidate in &row_candidates[row] {
                expected_codes.push(candidate);
                expected_owners.push(row as u32);
                expected_arms.push(arm as u32);
                expected_body.extend([
                    candidate,
                    program.encode(&raw(parent_ids[row][0])).unwrap().get(),
                    program.encode(&raw(parent_ids[row][1])).unwrap().get(),
                ]);
            }
        }
    }
    assert_eq!(whole.candidate_codes, expected_codes);
    assert_eq!(whole.candidate_owners, expected_owners);
    assert_eq!(whole.proposer_arms, expected_arms);
    assert_eq!(whole.child_body, expected_body);

    let restricted_segment = whole
        .segments
        .iter()
        .find(|segment| segment.variable == targets[5].index() as u32)
        .unwrap();
    let start = restricted_segment.base as usize;
    let end = start + restricted_segment.count as usize;
    let lane_64_destinations = (start..end)
        .filter(|&destination| whole.candidate_owners[destination] == 64)
        .collect::<Vec<_>>();
    assert_eq!(
        lane_64_destinations
            .iter()
            .map(|&destination| whole.candidate_codes[destination])
            .collect::<Vec<_>>(),
        row_candidates[64],
        "block-one Restricted destinations must match the raw triple oracle"
    );
    assert!(lane_64_destinations.iter().all(|&destination| {
        whole.proposer_arms[destination] == 5
            && whole.child_body[destination * whole.child_stride as usize
                ..(destination + 1) * whole.child_stride as usize]
                == expected_body[destination * whole.child_stride as usize
                    ..(destination + 1) * whole.child_stride as usize]
    }));

    let expected_segments = live_segments(&whole);
    for split in 0..=ROWS {
        let left = inspect_range(0..split);
        let right = inspect_range(split..ROWS);
        assert_eq!(
            concatenate_segment_maps(&left, &right, split as u32),
            expected_segments,
            "Present+Pair+Restricted row homomorphism split {split}"
        );
    }
}

#[test]
fn confirmed_shared_target_filters_cross_family_sibling_misses_stably() {
    const ROWS: usize = 3;
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let target = ProgramVariable::new(0);
    let open_attribute = ProgramVariable::new(1);
    let open_value = ProgramVariable::new(2);
    let bound_pair_attribute = ProgramVariable::new(3);
    let open_pair_value = ProgramVariable::new(4);
    let bound_restricted_attribute = ProgramVariable::new(5);
    let bound_value = ProgramVariable::new(6);
    let program = QueryProgram::compile(
        gpu.archive(),
        7,
        [
            QueryPattern::new(target, open_attribute, open_value),
            QueryPattern::new(target, bound_pair_attribute, open_pair_value),
            QueryPattern::new(target, bound_restricted_attribute, bound_value),
        ],
    )
    .unwrap();
    let bound = [
        bound_pair_attribute,
        bound_restricted_attribute,
        bound_value,
    ];
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let find_target_arm = |predicate: fn(ArmSpec) -> bool| {
        round
            .proposal_arm_specs()
            .iter()
            .copied()
            .enumerate()
            .find(|(arm, spec)| {
                round.metadata().arms()[*arm].target_variable() == target && predicate(*spec)
            })
            .map(|(arm, _)| arm)
            .unwrap()
    };
    let present_arm = find_target_arm(|spec| {
        matches!(
            spec,
            ArmSpec::Present {
                axis: ResidentAxis::Entity,
                ..
            }
        )
    });
    let pair_arm = find_target_arm(|spec| {
        matches!(
            spec,
            ArmSpec::PairDistinct {
                rotation: SuccinctRotation::Aev,
                ..
            }
        )
    });
    let restricted_arm = find_target_arm(|spec| {
        matches!(
            spec,
            ArmSpec::Restricted {
                rotation: SuccinctRotation::Aev,
                ..
            }
        )
    });
    let proposer_arms = [present_arm, pair_arm, restricted_arm];
    let pair_attributes = [
        fixture.attributes[2],
        fixture.attributes[0],
        fixture.attributes[2],
    ];
    let restricted_attributes = [fixture.attributes[0]; ROWS];
    let values = [fixture.values[0]; ROWS];
    let parent_ids = (0..ROWS)
        .flat_map(|row| {
            [
                pair_attributes[row],
                restricted_attributes[row],
                values[row],
            ]
        })
        .collect::<Vec<_>>();
    let parent_codes = parent_ids
        .iter()
        .map(|&id| program.encode(&raw(id)).unwrap().get())
        .collect::<Vec<_>>();
    let host = program
        .frontier_from_indices(bound.to_vec(), parent_codes.clone(), ROWS)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();

    let pair_ids =
        |row: usize| expected_pair_ids(&fixture, SuccinctRotation::Aev, pair_attributes[row]);
    let restricted_ids = |row: usize| {
        expected_restricted_ids(
            &fixture,
            SuccinctRotation::Aev,
            restricted_attributes[row],
            values[row],
        )
    };
    let confirmed_ids = |row: usize| {
        let pair = pair_ids(row);
        let restricted = restricted_ids(row);
        fixture
            .entities
            .iter()
            .copied()
            .filter(|entity| pair.contains(entity) && restricted.contains(entity))
            .collect::<Vec<_>>()
    };
    assert_eq!(confirmed_ids(0), vec![fixture.entities[0]]);
    assert_eq!(
        confirmed_ids(1),
        vec![fixture.entities[0], fixture.entities[1]]
    );
    assert_eq!(confirmed_ids(2), vec![fixture.entities[0]]);

    let provisional_ids = (0..ROWS)
        .map(|row| match proposer_arms[row] {
            arm if arm == present_arm => fixture.entities.to_vec(),
            arm if arm == pair_arm => pair_ids(row),
            arm if arm == restricted_arm => restricted_ids(row),
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    let mut choice_words = Vec::with_capacity(ROWS * CHOICE_WORDS);
    for row in 0..ROWS {
        let arm = proposer_arms[row];
        let witness = (arm * ROWS + row) * PROPOSAL_WITNESS_WORDS;
        let count = witnesses[witness + 3] - witnesses[witness + 2];
        assert_eq!(count as usize, provisional_ids[row].len());
        choice_words.extend([target.index() as u32, arm as u32, count]);
    }
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
        .unwrap();
    let provisional_count = provisional_ids.iter().map(Vec::len).sum::<usize>();
    let capacity = provisional_count + 5;
    let provisional = round
        .enqueue_generic_proposals_for_test(&frontier, &choices, capacity)
        .unwrap()
        .inspect();
    assert_success(&provisional, provisional_count);

    let confirmed_arena = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, capacity)
        .unwrap();
    let trace = confirmed_arena.read_semantic_confirmation_work_for_test();
    for (arm, spec) in round.proposal_arm_specs().iter().enumerate() {
        let target_count = if round.metadata().arms()[arm].target_variable() == target {
            provisional_count as u32
        } else {
            0
        };
        let expected = match spec {
            ArmSpec::Present { .. } => [0, 0],
            ArmSpec::PairDistinct { .. } => [target_count * 2, 0],
            ArmSpec::Restricted { .. } => [target_count * 2, ROWS as u32],
        };
        assert_eq!(trace[arm], expected, "arm {arm}");
    }
    let confirmed = confirmed_arena.inspect();
    let expected_count = (0..ROWS).map(|row| confirmed_ids(row).len()).sum::<usize>();
    assert_success(&confirmed, expected_count);
    assert!(
        expected_count < provisional_count,
        "fixture must leave stable holes"
    );

    let mut expected_codes = Vec::new();
    let mut expected_owners = Vec::new();
    let mut expected_proposers = Vec::new();
    let mut expected_body = Vec::new();
    for row in 0..ROWS {
        for candidate in codes(&program, &confirmed_ids(row)) {
            expected_codes.push(candidate);
            expected_owners.push(row as u32);
            expected_proposers.push(proposer_arms[row] as u32);
            expected_body.extend([
                candidate,
                parent_codes[row * 3],
                parent_codes[row * 3 + 1],
                parent_codes[row * 3 + 2],
            ]);
        }
    }
    assert_eq!(&confirmed.candidate_codes[..expected_count], expected_codes);
    assert_eq!(
        &confirmed.candidate_owners[..expected_count],
        expected_owners
    );
    assert_eq!(
        &confirmed.proposer_arms[..expected_count],
        expected_proposers
    );
    assert_eq!(&confirmed.child_body[..expected_body.len()], expected_body);
    assert!(confirmed.candidate_codes[expected_count..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(confirmed.child_body[expected_body.len()..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn confirmed_restricted_anchor_at_source_lane_64_survives_dropped_capabilities() {
    const ROWS: usize = 65;
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let target = ProgramVariable::new(0);
    let bound_attribute = ProgramVariable::new(1);
    let open_value = ProgramVariable::new(2);
    let bound_value = ProgramVariable::new(3);
    let program = QueryProgram::compile(
        gpu.archive(),
        4,
        [
            QueryPattern::new(target, bound_attribute, open_value),
            QueryPattern::new(target, bound_attribute, bound_value),
        ],
    )
    .unwrap();
    let bound = [bound_attribute, bound_value];
    let round = WgpuResidentRound::new(&gpu, &program, &bound).unwrap();
    let pair_arm = round
        .proposal_arm_specs()
        .iter()
        .copied()
        .enumerate()
        .find(|(arm, spec)| {
            round.metadata().arms()[*arm].target_variable() == target
                && matches!(
                    spec,
                    ArmSpec::PairDistinct {
                        rotation: SuccinctRotation::Aev,
                        ..
                    }
                )
        })
        .map(|(arm, _)| arm)
        .unwrap();
    let restricted_arm = round
        .proposal_arm_specs()
        .iter()
        .copied()
        .enumerate()
        .find(|(arm, spec)| {
            round.metadata().arms()[*arm].target_variable() == target
                && matches!(
                    spec,
                    ArmSpec::Restricted {
                        rotation: SuccinctRotation::Aev,
                        ..
                    }
                )
        })
        .map(|(arm, _)| arm)
        .unwrap();
    let singleton = representative_restricted_pairs(&fixture, SuccinctRotation::Aev)[1];
    assert_eq!(
        expected_restricted_ids(&fixture, SuccinctRotation::Aev, singleton.0, singleton.1).len(),
        1
    );
    let anchor = restricted_anchor_pair(&fixture, &program, SuccinctRotation::Aev);
    let anchor_restricted =
        expected_restricted_ids(&fixture, SuccinctRotation::Aev, anchor.0, anchor.1);
    let anchor_pair = expected_pair_ids(&fixture, SuccinctRotation::Aev, anchor.0);
    assert!(!anchor_restricted.is_empty());
    assert!(anchor_restricted
        .iter()
        .all(|entity| anchor_pair.contains(entity)));

    let row_pairs = (0..ROWS)
        .map(|row| if row == 64 { anchor } else { singleton })
        .collect::<Vec<_>>();
    let parent_codes = row_pairs
        .iter()
        .flat_map(|&(attribute, value)| {
            [
                program.encode(&raw(attribute)).unwrap().get(),
                program.encode(&raw(value)).unwrap().get(),
            ]
        })
        .collect::<Vec<_>>();
    let host = program
        .frontier_from_indices(bound.to_vec(), parent_codes.clone(), ROWS)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let restricted_witness = (restricted_arm * ROWS + 64) * PROPOSAL_WITNESS_WORDS;
    let (anchor_witness, anchor_base) = restricted_geometry(
        program.archive(),
        SuccinctRotation::Aev,
        program.encode(&raw(anchor.0)).unwrap().get() as usize,
        program.encode(&raw(anchor.1)).unwrap().get() as usize,
    );
    assert!(anchor_base > 0);
    assert!(anchor_witness[2] > 0);
    assert!(anchor_witness[3] > anchor_witness[2]);
    assert_eq!(
        &witnesses[restricted_witness..restricted_witness + PROPOSAL_WITNESS_WORDS],
        &anchor_witness.map(|word| word as u32)
    );

    let mut choice_words = Vec::with_capacity(ROWS * CHOICE_WORDS);
    for row in 0..ROWS {
        let arm = if row == 64 { pair_arm } else { restricted_arm };
        let witness = (arm * ROWS + row) * PROPOSAL_WITNESS_WORDS;
        let count = witnesses[witness + 3] - witnesses[witness + 2];
        let expected = if row == 64 { anchor_pair.len() } else { 1 };
        assert_eq!(count as usize, expected, "row {row}");
        choice_words.extend([target.index() as u32, arm as u32, count]);
    }
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
        .unwrap();
    let provisional_count = 64 + anchor_pair.len();
    let capacity = provisional_count + 4;
    let provisional = round
        .enqueue_generic_proposals_for_test(&frontier, &choices, capacity)
        .unwrap()
        .inspect();
    assert_success(&provisional, provisional_count);
    assert_eq!(provisional.candidate_owners[64], 64);
    assert_eq!(provisional.proposer_arms[64], pair_arm as u32);
    assert_eq!(
        &provisional.candidate_codes[64..64 + anchor_pair.len()],
        codes(&program, &anchor_pair)
    );

    let confirmed_arena = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, capacity)
        .unwrap();
    let short_arena = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, provisional_count - 1)
        .unwrap();
    let expected_trace = round
        .proposal_arm_specs()
        .iter()
        .enumerate()
        .map(|(arm, spec)| {
            let target_count = if round.metadata().arms()[arm].target_variable() == target {
                provisional_count as u32
            } else {
                0
            };
            match spec {
                ArmSpec::Present { .. } => [0, 0],
                ArmSpec::PairDistinct { .. } => [target_count * 2, 0],
                ArmSpec::Restricted { .. } => [target_count * 2, ROWS as u32],
            }
        })
        .collect::<Vec<_>>();

    // The arena owns every queued source. No round/frontier/input/choice
    // capability remains when either trace or semantic publication is read.
    drop(choices);
    drop(inputs);
    drop(frontier);
    drop(round);

    assert_eq!(
        confirmed_arena.read_semantic_confirmation_work_for_test(),
        expected_trace
    );
    let confirmed = confirmed_arena.inspect();
    let expected_count = 64 + anchor_restricted.len();
    assert_success(&confirmed, expected_count);
    assert_eq!(
        &confirmed.candidate_codes[64..64 + anchor_restricted.len()],
        codes(&program, &anchor_restricted)
    );
    assert!(confirmed.candidate_owners[64..64 + anchor_restricted.len()]
        .iter()
        .all(|&owner| owner == 64));
    assert!(confirmed.proposer_arms[64..64 + anchor_restricted.len()]
        .iter()
        .all(|&arm| arm == pair_arm as u32));
    for (ordinal, &candidate) in codes(&program, &anchor_restricted).iter().enumerate() {
        let destination = 64 + ordinal;
        assert_eq!(
            &confirmed.child_body[destination * confirmed.child_stride as usize
                ..(destination + 1) * confirmed.child_stride as usize],
            &[candidate, parent_codes[128], parent_codes[129]]
        );
    }
    assert!(confirmed.candidate_codes[expected_count..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));

    let short = short_arena.inspect();
    assert_eq!(short.status, STATUS_CAPACITY);
    assert_eq!(short.required, provisional_count as u32);
    assert_eq!(short.logical_len, 0);
    assert_eq!((short.dispatch_x, short.dispatch_y), (0, 1));
    assert!(short
        .candidate_codes
        .iter()
        .chain(&short.candidate_owners)
        .chain(&short.proposer_arms)
        .chain(&short.child_body)
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn semantic_fold_distinguishes_proposer_and_sibling_misses_and_pending_faults() {
    let context = crate::WgpuContext::on_wgpu();
    let layout = confirmation_workspace_layout(1).unwrap();
    let run_fold = |proposer: u32,
                    restricted: u32,
                    lo: u32,
                    hi: u32,
                    rank_lo: u32,
                    rank_hi: u32,
                    keep: u32,
                    pending: u32,
                    passes: usize| {
        let candidates = context.upload_u32(&[0, 0, proposer]).unwrap();
        let control = context.upload_u32(&[STATUS_OK, 1, 1, 1, 0]).unwrap();
        let lo_positions = context.upload_u32(&[lo]).unwrap();
        let hi_positions = context.upload_u32(&[hi]).unwrap();
        let lo_ranks = context.upload_u32(&[rank_lo]).unwrap();
        let hi_ranks = context.upload_u32(&[rank_hi]).unwrap();
        let mut words = vec![0; layout.words];
        words[layout.keep] = keep;
        words[layout.pending] = pending;
        let mut workspace = context.upload_u32(&words).unwrap();
        for _ in 0..passes {
            unsafe {
                fold_semantic_confirmation_arm::launch_unchecked::<WgpuRuntime>(
                    context.client(),
                    CubeCount::new_single(),
                    CubeDim::new_single(),
                    candidates.input_arg(),
                    control.input_arg(),
                    lo_positions.input_arg(),
                    hi_positions.input_arg(),
                    lo_ranks.input_arg(),
                    hi_ranks.input_arg(),
                    workspace.output_arg(),
                    1,
                    2,
                    2,
                    2,
                    0,
                    restricted,
                    layout.keep as u32,
                    layout.pending as u32,
                    RESIDENT_U32_SENTINEL,
                    STATUS_OK,
                );
            }
        }
        workspace.read()
    };

    let proposer_miss = run_fold(0, 0, 0, 0, 0, 0, 1, 1, 1);
    assert_eq!(proposer_miss[layout.keep], RESIDENT_U32_SENTINEL);
    assert_eq!(proposer_miss[layout.pending], 0);

    let sibling_miss = run_fold(1, 0, 0, 0, 0, 0, 1, 1, 1);
    assert_eq!(sibling_miss[layout.keep], 0);
    assert_eq!(sibling_miss[layout.pending], 0);

    let restricted_multi_hit = run_fold(1, 1, 0, 2, 0, 2, 1, 1, 1);
    assert_eq!(
        restricted_multi_hit[layout.keep], RESIDENT_U32_SENTINEL,
        "a Restricted rank difference greater than one is poison, not a miss"
    );
    assert_eq!(restricted_multi_hit[layout.pending], 0);

    let reversed_interval = run_fold(1, 0, 1, 0, 0, 0, 1, 1, 1);
    assert_eq!(reversed_interval[layout.keep], RESIDENT_U32_SENTINEL);
    let out_of_range_rank = run_fold(1, 0, 0, 1, 0, 2, 1, 1, 1);
    assert_eq!(out_of_range_rank[layout.keep], RESIDENT_U32_SENTINEL);
    let sticky_poison = run_fold(1, 0, 0, 1, 0, 1, RESIDENT_U32_SENTINEL, 1, 1);
    assert_eq!(sticky_poison[layout.keep], RESIDENT_U32_SENTINEL);

    let duplicate = run_fold(1, 0, 0, 1, 0, 1, 1, 1, 2);
    assert_eq!(duplicate[layout.keep], RESIDENT_U32_SENTINEL);
    assert_eq!(duplicate[layout.pending], 0);

    let leftover = run_fold(1, 0, 0, 1, 0, 1, 1, 2, 1);
    assert_eq!(leftover[layout.keep], 1);
    assert_eq!(leftover[layout.pending], 1);

    let scan = |words: Vec<u32>| {
        let mut workspace = context.upload_u32(&words).unwrap();
        unsafe {
            scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                workspace.output_arg(),
                1,
                layout.block_count as u32,
                layout.keep as u32,
                layout.pending as u32,
                layout.semantic_status as u32,
                layout.local_offsets as u32,
                layout.block_sums as u32,
                layout.block_errors as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_DEVICE_INVARIANT,
            );
        }
        workspace.read()
    };
    assert_eq!(
        scan(leftover)[layout.block_errors],
        STATUS_DEVICE_INVARIANT,
        "a skipped deferred pass must be detected by pending"
    );
    let mut never_folded = vec![0; layout.words];
    never_folded[layout.keep] = 1;
    never_folded[layout.pending] = 1;
    assert_eq!(
        scan(never_folded)[layout.block_errors],
        STATUS_DEVICE_INVARIANT
    );
}

#[test]
fn semantic_arm_publisher_rechecks_retained_lengths_and_segment_range() {
    let context = crate::WgpuContext::on_wgpu();
    let layout = confirmation_workspace_layout(1).unwrap();
    let rotation = SuccinctRotation::Aev.index() as u32;
    let enum_limit = 8u32;
    let canonical_plan = vec![0, FAMILY_RESTRICTED, rotation, enum_limit, 0];
    let canonical_segments = vec![0, 1, 0, 0];
    let run = |plan_words: &[u32], segment_words: &[u32]| {
        let plan = context.upload_u32(plan_words).unwrap();
        let segments = context.upload_u32(segment_words).unwrap();
        let provisional = context.upload_u32(&[STATUS_OK, 1, 1, 1]).unwrap();
        let mut workspace = context.upload_u32(&vec![0; layout.words]).unwrap();
        let mut arm_control = context
            .upload_u32(&[
                STATUS_DEVICE_INVARIANT,
                RESIDENT_U32_SENTINEL,
                0,
                1,
                RESIDENT_U32_SENTINEL,
            ])
            .unwrap();
        unsafe {
            publish_semantic_confirmation_arm_work::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                plan.input_arg(),
                segments.input_arg(),
                provisional.input_arg(),
                workspace.output_arg(),
                arm_control.output_arg(),
                0,
                0,
                FAMILY_RESTRICTED,
                rotation,
                enum_limit,
                1,
                1,
                1,
                1,
                1,
                THREADS,
                0,
                ARM_DESCRIPTOR_WORDS as u32,
                layout.semantic_status as u32,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
                STATUS_DEVICE_INVARIANT,
            );
        }
        (arm_control.read(), workspace.read())
    };

    let (control, workspace) = run(&canonical_plan, &canonical_segments);
    assert_eq!(&control[..5], &[STATUS_OK, 1, 1, 1, 0]);
    assert_eq!(workspace[layout.semantic_status], STATUS_OK);

    let mut out_of_range_segment = canonical_plan.clone();
    out_of_range_segment[ARM_DESCRIPTOR_WORDS] = 1;
    let mut faults = vec![
        (
            "out-of-range segment",
            out_of_range_segment,
            canonical_segments.clone(),
        ),
        (
            "truncated plan",
            canonical_plan[..4].to_vec(),
            canonical_segments.clone(),
        ),
        (
            "truncated segment records",
            canonical_plan.clone(),
            canonical_segments[..3].to_vec(),
        ),
    ];
    let mut oversized_segment = canonical_segments.clone();
    oversized_segment[1] = 2;
    faults.push((
        "segment exceeds retained total",
        canonical_plan,
        oversized_segment,
    ));
    for (name, plan, segments) in faults {
        let (control, workspace) = run(&plan, &segments);
        assert_eq!(
            &control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "{name}"
        );
        assert_eq!(
            control[CONTROL_SEGMENT_BASE], RESIDENT_U32_SENTINEL,
            "{name}"
        );
        assert_eq!(
            workspace[layout.semantic_status], STATUS_DEVICE_INVARIANT,
            "{name}"
        );
    }
}

#[test]
fn restricted_confirmation_normalization_rejects_bad_select_results() {
    let context = crate::WgpuContext::on_wgpu();
    let run = |source: [u32; 4], query: u32, selected: u32| {
        let plan = context.upload_u32(&source).unwrap();
        let frontier = context.upload_u32(&[2]).unwrap();
        let queries = context.upload_u32(&[query]).unwrap();
        let mut bases = context.upload_u32(&[selected]).unwrap();
        let control = context.upload_u32(&[STATUS_OK, 1, 1, 1, 0]).unwrap();
        unsafe {
            normalize_restricted_confirmation_bases::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                plan.input_arg(),
                frontier.input_arg(),
                queries.input_arg(),
                bases.output_arg(),
                control.input_arg(),
                1,
                1,
                4,
                0,
                5,
                0,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
            );
        }
        bases.read()[0]
    };

    let constant = [0, 0, CONSTANT_SOURCE, 2];
    assert_eq!(run(constant, 2, 4), 2);
    for selected in [1, 8, RESIDENT_U32_SENTINEL] {
        assert_eq!(run(constant, 2, selected), RESIDENT_U32_SENTINEL);
    }
    assert_eq!(
        run([0, 0, COLUMN_SOURCE, 0], 2, 4),
        2,
        "Column and Constant last sources normalize identically"
    );
    assert_eq!(
        run([0, 0, CONSTANT_SOURCE, 4], 0, 0),
        RESIDENT_U32_SENTINEL,
        "an out-of-domain source cannot borrow the safe query zero"
    );
}

#[test]
fn semantic_initializer_rejects_csr_proposer_descriptor_and_pending_corruption() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let target = ProgramVariable::new(0);
    let open_attribute = ProgramVariable::new(1);
    let open_value = ProgramVariable::new(2);
    let bound_pair_attribute = ProgramVariable::new(3);
    let open_pair_value = ProgramVariable::new(4);
    let bound_restricted_attribute = ProgramVariable::new(5);
    let bound_value = ProgramVariable::new(6);
    let program = QueryProgram::compile(
        gpu.archive(),
        7,
        [
            QueryPattern::new(target, open_attribute, open_value),
            QueryPattern::new(target, bound_pair_attribute, open_pair_value),
            QueryPattern::new(target, bound_restricted_attribute, bound_value),
        ],
    )
    .unwrap();
    let round = WgpuResidentRound::new(
        &gpu,
        &program,
        &[
            bound_pair_attribute,
            bound_restricted_attribute,
            bound_value,
        ],
    )
    .unwrap();
    let admission = lower_proposal_admission(&round, ProposerPolicy::RestrictedNatural).unwrap();
    let (canonical_plan, plan_layout) =
        packed_plan(&admission, &admission.arm_descriptors).unwrap();
    let relevant = round.metadata().relevant_arm_ids(target).unwrap();
    assert_eq!(relevant.len(), 3);
    let proposer = relevant
        .iter()
        .copied()
        .find(|&arm| {
            matches!(
                round.proposal_arm_specs()[arm as usize],
                ArmSpec::Present {
                    axis: ResidentAxis::Entity,
                    ..
                }
            )
        })
        .unwrap();
    let candidate = program.encode(&raw(fixture.entities[0])).unwrap().get();
    let mut segment_records = Vec::with_capacity(admission.segment_count * SEGMENT_RECORD_WORDS);
    let mut cursor = 0u32;
    for spec in admission.segment_specs.chunks_exact(SEGMENT_SPEC_WORDS) {
        let count = u32::from(spec[0] == target.index() as u32);
        segment_records.extend([cursor, count, spec[0], spec[1]]);
        cursor += count;
    }
    assert_eq!(cursor, 1);
    let candidate_records = [candidate, 0, proposer];
    let confirmation = confirmation_workspace_layout(1).unwrap();
    let (ring_len, pair_counts) = checked_proposal_physical_limits(&round).unwrap();
    let context = round.archive().context();
    let run = |plan_words: &[u32]| {
        let plan = context.upload_u32(plan_words).unwrap();
        let segments = context.upload_u32(&segment_records).unwrap();
        let candidates = context.upload_u32(&candidate_records).unwrap();
        let mut workspace = context.upload_u32(&vec![0; confirmation.words]).unwrap();
        unsafe {
            initialize_semantic_confirmation::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                plan.input_arg(),
                segments.input_arg(),
                candidates.input_arg(),
                round.archive().present_entity_codes().input_arg(),
                round.archive().present_attribute_codes().input_arg(),
                round.archive().present_value_codes().input_arg(),
                workspace.output_arg(),
                1,
                admission.segment_count as u32,
                1,
                program.archive().domain.len() as u32,
                ring_len,
                pair_counts[0],
                pair_counts[1],
                pair_counts[2],
                pair_counts[3],
                pair_counts[4],
                pair_counts[5],
                round.metadata().variable_count() as u32,
                round.metadata().arms().len() as u32,
                plan_layout.arm_descriptors as u32,
                plan_layout.variable_offsets as u32,
                plan_layout.variable_arms as u32,
                plan_layout.deferred_arm_counts as u32,
                plan_layout.variable_to_segment as u32,
                confirmation.keep as u32,
                confirmation.pending as u32,
                RESIDENT_U32_SENTINEL,
            );
        }
        let words = workspace.read();
        [words[confirmation.keep], words[confirmation.pending]]
    };
    assert_eq!(run(&canonical_plan), [1, 2]);

    let start = admission.variable_offsets[target.index()] as usize;
    let end = admission.variable_offsets[target.index() + 1] as usize;
    assert_eq!(end - start, 3);
    let arms_base = plan_layout.variable_arms + start;
    let mut faults = Vec::new();
    let mut duplicate = canonical_plan.clone();
    duplicate[arms_base + 1] = duplicate[arms_base];
    faults.push(("duplicate/missing proposer", duplicate));
    let mut out_of_order = canonical_plan.clone();
    out_of_order.swap(arms_base, arms_base + 1);
    faults.push(("out-of-order CSR", out_of_order));
    let mut empty_range = canonical_plan.clone();
    empty_range[plan_layout.variable_offsets + target.index() + 1] = start as u32;
    faults.push(("empty relevant range", empty_range));
    let mut bad_descriptor = canonical_plan.clone();
    bad_descriptor[plan_layout.arm_descriptors + proposer as usize * ARM_DESCRIPTOR_WORDS + 3] += 1;
    faults.push(("malformed proposer descriptor", bad_descriptor));
    let mut bad_pending = canonical_plan.clone();
    bad_pending[plan_layout.deferred_arm_counts + target.index()] = 1;
    faults.push(("wrong deferred count", bad_pending));

    for (name, plan) in faults {
        let result = run(&plan);
        assert_eq!(result[0], RESIDENT_U32_SENTINEL, "{name}");
        assert_ne!(result[1], RESIDENT_U32_SENTINEL, "{name}");
    }
}

#[test]
fn restricted_faults_fail_closed_and_outrank_capacity() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[entity, attribute]).unwrap();
    let admission = lower_proposal_admission(&round, ProposerPolicy::RestrictedNatural).unwrap();
    let arm = 0usize;
    let descriptor = arm * ARM_DESCRIPTOR_WORDS;
    let first_code = program.encode(&raw(fixture.entities[0])).unwrap().get();
    let last_code = program.encode(&raw(fixture.attributes[0])).unwrap().get();
    let host = program
        .frontier_from_indices(vec![entity, attribute], vec![first_code, last_code], 1)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let count = witnesses[3] - witnesses[2];
    assert!(count > 1);
    let choice = [value.index() as u32, arm as u32, count];
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice)
        .unwrap();
    let ring_len = round.archive().ring_col(SuccinctRotation::Eav).len() as u32;

    for (word, replacement) in [
        (0usize, attribute.index() as u32),
        (1, 99),
        (2, SuccinctRotation::ALL.len() as u32),
        (3, ring_len - 1),
    ] {
        let mut descriptors = admission.arm_descriptors.clone();
        descriptors[descriptor + word] = replacement;
        let classified =
            classify_direct(&round, &admission, &descriptors, 2, &choice, &witnesses, 0);
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "descriptor word {word}"
        );
    }

    for replacement in [
        [0, ring_len + 1, 0, count],
        [0, ring_len, 0, ring_len + 1],
        [1, 2, 2, 2],
        [0, 1, 0, 2],
        [RESIDENT_U32_SENTINEL; PROPOSAL_WITNESS_WORDS],
    ] {
        let classified = classify_direct(
            &round,
            &admission,
            &admission.arm_descriptors,
            2,
            &choice,
            &replacement,
            0,
        );
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "witness {replacement:?}"
        );
    }

    let source_base = arm * RESTRICTED_SOURCE_WORDS_PER_ARM;
    let domain = program.archive().domain.len() as u32;
    let mut source_faults = Vec::new();
    let mut unknown_kind = admission.restricted_sources.clone();
    unknown_kind[source_base] = 99;
    source_faults.push(("unknown source kind", unknown_kind));
    let mut coherent_wrong_range = admission.restricted_sources.clone();
    coherent_wrong_range[source_base + 1] = 1;
    source_faults.push(("coherent wrong source range", coherent_wrong_range));
    let mut out_of_domain = admission.restricted_sources.clone();
    out_of_domain[source_base + 2] = CONSTANT_SOURCE;
    out_of_domain[source_base + 3] = domain;
    source_faults.push(("out-of-domain source", out_of_domain));

    for (name, sources) in source_faults {
        let mut corrupted = ProposalAdmission {
            pair_capable: admission.pair_capable,
            restricted_capable: admission.restricted_capable,
            arm_descriptors: admission.arm_descriptors.clone(),
            restricted_sources: sources,
            variable_offsets: admission.variable_offsets.clone(),
            variable_arms: admission.variable_arms.clone(),
            segment_specs: admission.segment_specs.clone(),
            variable_to_segment: admission.variable_to_segment.clone(),
            deferred_arm_counts: admission.deferred_arm_counts.clone(),
            segment_count: admission.segment_count,
        };
        let proposal_inputs = round.proposal_inputs(&frontier, &choices).unwrap();
        let geometry = proposal_geometry(
            proposal_inputs.rows,
            proposal_inputs.parent_stride,
            count as usize - 1,
            &corrupted,
        )
        .unwrap();
        let failed = round
            .enqueue_present_proposals_with_inputs(
                proposal_inputs,
                ProposalPreflight {
                    admission: &corrupted,
                    geometry,
                },
                None,
                None,
                false,
                PresentPublication::Confirmed,
            )
            .unwrap()
            .inspect();
        assert_eq!(failed.status, STATUS_DEVICE_INVARIANT, "{name}");
        assert_eq!(failed.required, RESIDENT_U32_SENTINEL, "{name}");
        assert_eq!(failed.logical_len, 0, "{name}");
        assert_eq!((failed.dispatch_x, failed.dispatch_y), (0, 1), "{name}");
        assert!(!failed.candidate_codes.is_empty(), "{name}");
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
        // Keep the admission live across the complete enqueue above while
        // making it clear no later iteration can accidentally reuse it.
        corrupted.restricted_sources.clear();
    }
}

#[test]
fn restricted_arena_retains_inputs_after_round_capabilities_drop() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program = QueryProgram::compile(
        gpu.archive(),
        3,
        [QueryPattern::new(entity, attribute, value)],
    )
    .unwrap();
    let round = WgpuResidentRound::new(&gpu, &program, &[entity, attribute]).unwrap();
    let expected = expected_restricted_ids(
        &fixture,
        SuccinctRotation::Eva,
        fixture.entities[0],
        fixture.attributes[0],
    );
    let arena = {
        let host = program
            .frontier_from_indices(
                vec![entity, attribute],
                vec![
                    program.encode(&raw(fixture.entities[0])).unwrap().get(),
                    program.encode(&raw(fixture.attributes[0])).unwrap().get(),
                ],
                1,
            )
            .unwrap();
        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();
        let count = witnesses[3] - witnesses[2];
        let choices = round
            .force_choice_words_from_inputs_for_test(
                &frontier,
                &inputs,
                &[value.index() as u32, 0, count],
            )
            .unwrap();
        round
            .enqueue_generic_proposals_for_test(&frontier, &choices, expected.len())
            .unwrap()
    };
    drop(round);
    let inspection = arena.inspect();
    assert_success(&inspection, expected.len());
    assert_eq!(inspection.candidate_codes, codes(&program, &expected));
}

#[test]
fn pair_and_restricted_confirmation_is_monotone_after_decoding_rebuilt_archive_codes() {
    let entity_id = ordered_id(1);
    let inserted_attribute = ordered_id(4);
    let existing_attribute = ordered_id(8);
    let value_id = ordered_id(12);
    let mut before = TribleSet::new();
    insert(&mut before, entity_id, existing_attribute, value_id);
    let mut after = before.clone();
    insert(&mut after, entity_id, inserted_attribute, value_id);

    let run = |set: &TribleSet, restricted: bool| {
        let archive: SuccinctArchive<OrderedUniverse> = set.into();
        let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
        let entity = ProgramVariable::new(0);
        let attribute = ProgramVariable::new(1);
        let value = ProgramVariable::new(2);
        let program = QueryProgram::compile(
            gpu.archive(),
            3,
            [QueryPattern::new(entity, attribute, value)],
        )
        .unwrap();
        let bound_variables = if restricted {
            vec![entity, value]
        } else {
            vec![entity]
        };
        let round = WgpuResidentRound::new(&gpu, &program, &bound_variables).unwrap();
        let arm = round
            .proposal_arm_specs()
            .iter()
            .copied()
            .enumerate()
            .find(|(arm, spec)| {
                round.metadata().arms()[*arm].target_variable() == attribute
                    && if restricted {
                        matches!(
                            spec,
                            ArmSpec::Restricted {
                                rotation: SuccinctRotation::Eav,
                                ..
                            }
                        )
                    } else {
                        matches!(
                            spec,
                            ArmSpec::PairDistinct {
                                rotation: SuccinctRotation::Eav,
                                ..
                            }
                        )
                    }
            })
            .map(|(arm, _)| arm)
            .unwrap();
        let bound_codes = if restricted {
            vec![
                program.encode(&raw(entity_id)).unwrap().get(),
                program.encode(&raw(value_id)).unwrap().get(),
            ]
        } else {
            vec![program.encode(&raw(entity_id)).unwrap().get()]
        };
        let host = program
            .frontier_from_indices(bound_variables, bound_codes, 1)
            .unwrap();
        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();
        let witness = arm * PROPOSAL_WITNESS_WORDS;
        let count = witnesses[witness + 3] - witnesses[witness + 2];
        let choices = round
            .force_choice_words_from_inputs_for_test(
                &frontier,
                &inputs,
                &[attribute.index() as u32, arm as u32, count],
            )
            .unwrap();
        let inspection = round
            .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, count as usize)
            .unwrap()
            .inspect();
        assert_success(&inspection, count as usize);
        let encoded = inspection.candidate_codes;
        let candidate_frontier = program
            .frontier_from_indices(vec![attribute], encoded.clone(), encoded.len())
            .unwrap();
        let decoded = program
            .decode_frontier(&candidate_frontier)
            .unwrap()
            .into_iter()
            .map(|row| row[0])
            .collect::<Vec<_>>();
        (encoded, decoded)
    };

    let (before_codes, before_decoded) = run(&before, false);
    let (after_codes, after_decoded) = run(&after, false);
    assert_eq!(before_decoded, vec![raw(existing_attribute)]);
    assert_eq!(
        after_decoded,
        vec![raw(inserted_attribute), raw(existing_attribute)]
    );
    let before_set = before_decoded.iter().copied().collect::<BTreeSet<_>>();
    let after_set = after_decoded.iter().copied().collect::<BTreeSet<_>>();
    assert!(before_set.is_subset(&after_set));
    assert_eq!(before_codes.len(), 1);
    assert_eq!(after_codes.len(), 2);
    assert_ne!(
        before_codes[0], after_codes[1],
        "new ordered-universe member must shift the old snapshot-local code"
    );

    let (restricted_before_codes, restricted_before_decoded) = run(&before, true);
    let (restricted_after_codes, restricted_after_decoded) = run(&after, true);
    assert_eq!(restricted_before_decoded, before_decoded);
    assert_eq!(restricted_after_decoded, after_decoded);
    assert_ne!(
        restricted_before_codes[0], restricted_after_codes[1],
        "Restricted survivors must be compared after decoding shifted codes"
    );
    let restricted_before = restricted_before_decoded
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let restricted_after = restricted_after_decoded
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(restricted_before.is_subset(&restricted_after));
}

#[test]
fn restricted_group_finalizer_preserves_the_closed_status_lattice() {
    let fixture = restricted_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let context = gpu.context();
    let layout = restricted_workspace_layout(1).unwrap();
    assert_eq!(layout.max_block_count, 1);

    let run = |upstream_status: u32, validation_error: u32, scan_error: u32| {
        let mut words = vec![0; layout.words];
        words[layout.block_sums] = 1;
        words[layout.validation_errors] = validation_error;
        words[layout.block_errors] = scan_error;
        let mut workspace = context.upload_u32(&words).unwrap();
        let mut planning = context.upload_u32(&[upstream_status, 5, 0, 1]).unwrap();
        let mut group = context
            .upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 0, 1])
            .unwrap();
        unsafe {
            finalize_restricted_group_scan::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                workspace.output_arg(),
                planning.output_arg(),
                group.output_arg(),
                1,
                1,
                8,
                8,
                8,
                THREADS,
                layout.validation_errors as u32,
                layout.block_sums as u32,
                layout.block_errors as u32,
                layout.block_offsets as u32,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
                STATUS_CAPACITY,
                STATUS_DEVICE_INVARIANT,
                STATUS_GEOMETRY,
            );
        }
        (planning.read(), group.read())
    };

    let (planning, group) = run(STATUS_CAPACITY, STATUS_DEVICE_INVARIANT, 0);
    assert_eq!(
        &planning[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(&group[..4], &[STATUS_DEVICE_INVARIANT, 0, 0, 1]);

    let (planning, group) = run(STATUS_DEVICE_INVARIANT, 0, 0);
    assert_eq!(
        &planning[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(&group[..4], &[STATUS_DEVICE_INVARIANT, 0, 0, 1]);

    let (planning, group) = run(STATUS_GEOMETRY, STATUS_DEVICE_INVARIANT, 0);
    assert_eq!(&planning[..2], &[STATUS_GEOMETRY, RESIDENT_U32_SENTINEL]);
    assert_eq!(&group[..4], &[STATUS_GEOMETRY, 0, 0, 1]);

    let (planning, group) = run(99, 0, 0);
    assert_eq!(
        &planning[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(&group[..4], &[STATUS_DEVICE_INVARIANT, 0, 0, 1]);

    let (planning, group) = run(STATUS_CAPACITY, 0, 0);
    assert_eq!(&planning[..2], &[STATUS_CAPACITY, 5]);
    assert_eq!(&group[..4], &[STATUS_CAPACITY, 0, 0, 1]);

    for upstream in [STATUS_OK, STATUS_CAPACITY] {
        let (planning, group) = run(upstream, 0, STATUS_GEOMETRY);
        assert_eq!(&planning[..2], &[STATUS_GEOMETRY, RESIDENT_U32_SENTINEL]);
        assert_eq!(&group[..4], &[STATUS_GEOMETRY, 0, 0, 1]);
    }
}

struct DirectClassification {
    control: Vec<u32>,
    workspace: Vec<u32>,
    segments: Vec<u32>,
    layout: WorkspaceLayout,
}

fn classify_direct<U: Universe>(
    round: &WgpuResidentRound<'_, U>,
    admission: &ProposalAdmission,
    descriptors: &[u32],
    parent_stride: usize,
    choices: &[u32],
    witnesses: &[u32],
    capacity: usize,
) -> DirectClassification {
    let rows = choices.len() / CHOICE_WORDS;
    assert_eq!(choices.len(), rows * CHOICE_WORDS);
    assert_eq!(
        witnesses.len(),
        rows * round.metadata().arms().len() * PROPOSAL_WITNESS_WORDS
    );
    let geometry = proposal_geometry(rows, parent_stride, capacity, admission).unwrap();
    let (plan_words, plan_layout) = packed_plan(admission, descriptors).unwrap();
    let context = round.archive().context();
    let choices = context.upload_u32(choices).unwrap();
    let witnesses = context.upload_u32(witnesses).unwrap();
    let plan = context.upload_u32(&plan_words).unwrap();
    let mut workspace = context.empty_u32(geometry.workspace_layout.words).unwrap();
    let (ring_len, pair_counts) = checked_proposal_physical_limits(round).unwrap();
    if rows != 0 {
        let dispatch = context
            .static_batch_dispatch(rows, rows, CubeDim::new_1d(THREADS))
            .unwrap();
        unsafe {
            classify_proposal_choices::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                choices.input_arg(),
                witnesses.input_arg(),
                plan.input_arg(),
                workspace.output_arg(),
                rows as u32,
                admission.segment_count as u32,
                round.metadata().variable_count() as u32,
                round.metadata().arms().len() as u32,
                u32::from(round.proposal_global_dead()),
                round.archive().present_entity_codes().len() as u32,
                round.archive().present_attribute_codes().len() as u32,
                round.archive().present_value_codes().len() as u32,
                ring_len,
                pair_counts[0],
                pair_counts[1],
                pair_counts[2],
                pair_counts[3],
                pair_counts[4],
                pair_counts[5],
                plan_layout.arm_descriptors as u32,
                plan_layout.variable_to_segment as u32,
                geometry.workspace_layout.counts as u32,
                geometry.workspace_layout.row_arms as u32,
                geometry.workspace_layout.row_families as u32,
                geometry.workspace_layout.row_physicals as u32,
                geometry.workspace_layout.row_segments as u32,
                geometry.workspace_layout.row_counts as u32,
                geometry.workspace_layout.row_enum_los as u32,
                geometry.workspace_layout.choice_errors as u32,
                RESIDENT_U32_SENTINEL,
            );
        }
    }
    if geometry.choice_error_blocks != 0 {
        let dispatch = context
            .static_batch_dispatch(
                geometry.choice_error_blocks,
                geometry.choice_error_blocks,
                CubeDim::new_1d(1),
            )
            .unwrap();
        unsafe {
            reduce_validation_errors::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                workspace.output_arg(),
                rows as u32,
                geometry.choice_error_blocks as u32,
                geometry.workspace_layout.choice_errors as u32,
                geometry.workspace_layout.validation_errors as u32,
                BLOCK_ITEMS,
            );
        }
    }
    if geometry.block_count != 0 {
        let dispatch = context
            .static_batch_dispatch(
                geometry.block_count,
                geometry.block_count,
                CubeDim::new_1d(1),
            )
            .unwrap();
        unsafe {
            scan_present_blocks::launch_unchecked::<WgpuRuntime>(
                context.client(),
                dispatch.cube_count(),
                dispatch.cube_dim(),
                workspace.output_arg(),
                geometry.cells as u32,
                geometry.block_count as u32,
                geometry.workspace_layout.counts as u32,
                geometry.workspace_layout.local_offsets as u32,
                geometry.workspace_layout.block_sums as u32,
                geometry.workspace_layout.block_errors as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
            );
        }
    }
    let mut segments = context
        .upload_u32(&vec![RESIDENT_U32_SENTINEL; geometry.segment_record_words])
        .unwrap();
    let mut control = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    let dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        finalize_present_scan::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.output_arg(),
            plan.input_arg(),
            segments.output_arg(),
            control.output_arg(),
            rows as u32,
            geometry.cells as u32,
            geometry.block_count as u32,
            geometry.choice_error_blocks as u32,
            admission.segment_count as u32,
            parent_stride as u32,
            round.metadata().variable_count() as u32,
            capacity as u32,
            dispatch.max_groups_x(),
            dispatch.max_groups_y(),
            THREADS,
            geometry.workspace_layout.counts as u32,
            geometry.workspace_layout.validation_errors as u32,
            geometry.workspace_layout.local_offsets as u32,
            geometry.workspace_layout.block_sums as u32,
            geometry.workspace_layout.block_errors as u32,
            geometry.workspace_layout.block_offsets as u32,
            plan_layout.segment_specs as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    DirectClassification {
        control: control.read(),
        workspace: workspace.read(),
        segments: segments.read(),
        layout: geometry.workspace_layout,
    }
}

struct DirectGate {
    control: Vec<u32>,
    logical_len: u32,
    segments: Vec<u32>,
    candidates: Vec<u32>,
    child: Vec<u32>,
    verdicts: Vec<u32>,
    indirect_marker: u32,
    failure_indirect_marker: u32,
}

#[allow(clippy::too_many_arguments)]
fn gate_direct(
    planning_words: [u32; 4],
    workspace_words: &[u32],
    workspace_layout: WorkspaceLayout,
    rows: usize,
    segment_count: usize,
    capacity: usize,
    domain: u32,
    segment_words: &[u32],
    candidate_words: &[u32],
    child_words: &[u32],
) -> DirectGate {
    let context = crate::WgpuContext::on_wgpu();
    let workspace = context.upload_u32(workspace_words).unwrap();
    let mut segments = context.upload_u32(segment_words).unwrap();
    let mut candidates = context.upload_u32(candidate_words).unwrap();
    let mut child = context.upload_u32(child_words).unwrap();
    let planning = context.upload_u32(&planning_words).unwrap();
    let mut control = context
        .upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 0, 1])
        .unwrap();
    let mut meta = context.batch_meta(0, capacity).unwrap();
    let mut dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    let semantic_poison_len = segment_words.len().max(capacity).max(child_words.len());
    let mut failure_dispatch = context
        .batch_dispatch(0, semantic_poison_len, CubeDim::new_1d(THREADS))
        .unwrap();
    let confirmation_layout = confirmation_workspace_layout(capacity).unwrap();
    let mut confirmation = context
        .upload_u32(&vec![0; confirmation_layout.words])
        .unwrap();

    if capacity != 0 {
        let launch = context
            .static_batch_dispatch(capacity, capacity, CubeDim::new_1d(THREADS))
            .unwrap();
        unsafe {
            validate_proposal_destinations::launch_unchecked::<WgpuRuntime>(
                context.client(),
                launch.cube_count(),
                launch.cube_dim(),
                workspace.input_arg(),
                candidates.input_arg(),
                planning.input_arg(),
                confirmation.output_arg(),
                rows as u32,
                segment_count as u32,
                capacity as u32,
                domain,
                10,
                workspace_layout.row_arms as u32,
                workspace_layout.row_segments as u32,
                workspace_layout.row_counts as u32,
                workspace_layout.counts as u32,
                workspace_layout.local_offsets as u32,
                workspace_layout.block_offsets as u32,
                confirmation_layout.keep as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
            );
        }
    }
    if confirmation_layout.block_count != 0 {
        let launch = context
            .static_batch_dispatch(
                confirmation_layout.block_count,
                confirmation_layout.block_count,
                CubeDim::new_1d(1),
            )
            .unwrap();
        unsafe {
            scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
                context.client(),
                launch.cube_count(),
                launch.cube_dim(),
                confirmation.output_arg(),
                capacity as u32,
                confirmation_layout.block_count as u32,
                confirmation_layout.keep as u32,
                confirmation_layout.pending as u32,
                confirmation_layout.semantic_status as u32,
                confirmation_layout.local_offsets as u32,
                confirmation_layout.block_sums as u32,
                confirmation_layout.block_errors as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_DEVICE_INVARIANT,
            );
        }
    }
    let failure_groups_x = failure_dispatch.max_groups_x();
    let failure_groups_y = failure_dispatch.max_groups_y();
    unsafe {
        finalize_proposal_destinations::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            confirmation.output_arg(),
            planning.input_arg(),
            control.output_arg(),
            capacity as u32,
            confirmation_layout.block_count as u32,
            dispatch.max_groups_x(),
            dispatch.max_groups_y(),
            THREADS,
            confirmation_layout.local_offsets as u32,
            confirmation_layout.block_sums as u32,
            confirmation_layout.block_errors as u32,
            confirmation_layout.block_offsets as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
        publish_proposal_and_failure_dispatch::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            control.input_arg(),
            dispatch.output_arg(),
            failure_dispatch.output_arg(),
            failure_groups_x,
            failure_groups_y,
            STATUS_OK,
        );
    }
    unsafe {
        poison_failed_proposal_outputs::launch_unchecked::<WgpuRuntime>(
            context.client(),
            failure_dispatch.cube_count(),
            failure_dispatch.cube_dim(),
            control.input_arg(),
            segments.output_arg(),
            candidates.output_arg(),
            child.output_arg(),
            segment_words.len() as u32,
            capacity as u32,
            child_words.len() as u32,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
        );
    }
    unsafe {
        publish_proposal_meta::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            control.input_arg(),
            meta.output_arg(),
            capacity as u32,
            STATUS_OK,
        );
    }
    let mut indirect_marker = context.upload_u32(&[0]).unwrap();
    let mut failure_indirect_marker = context.upload_u32(&[0]).unwrap();
    unsafe {
        mark_indirect_dispatch::launch_unchecked::<WgpuRuntime>(
            context.client(),
            dispatch.cube_count(),
            dispatch.cube_dim(),
            indirect_marker.output_arg(),
        );
        mark_indirect_dispatch::launch_unchecked::<WgpuRuntime>(
            context.client(),
            failure_dispatch.cube_count(),
            failure_dispatch.cube_dim(),
            failure_indirect_marker.output_arg(),
        );
    }
    let mut meta_marker = context.empty_u32(1).unwrap();
    unsafe {
        pack_proposal_completion::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            meta.input_arg(),
            meta_marker.output_arg(),
        );
    }
    let confirmation = confirmation.read();
    DirectGate {
        control: control.read(),
        logical_len: meta_marker.read()[0],
        segments: segments.read(),
        candidates: candidates.read(),
        child: child.read(),
        verdicts: confirmation[confirmation_layout.keep..confirmation_layout.keep + capacity]
            .to_vec(),
        indirect_marker: indirect_marker.read()[0],
        failure_indirect_marker: failure_indirect_marker.read()[0],
    }
}

fn canonical_gate_fixture() -> (WorkspaceLayout, Vec<u32>, Vec<u32>, Vec<u32>) {
    let layout = workspace_layout(3, 3, 0, 1).unwrap();
    let mut workspace = vec![0; layout.words];
    workspace[layout.counts..layout.counts + 3].copy_from_slice(&[1, 1, 1]);
    workspace[layout.row_arms..layout.row_arms + 3].copy_from_slice(&[7, 8, 9]);
    workspace[layout.row_segments..layout.row_segments + 3].copy_from_slice(&[0, 0, 0]);
    workspace[layout.row_counts..layout.row_counts + 3].copy_from_slice(&[1, 1, 1]);
    workspace[layout.local_offsets..layout.local_offsets + 3].copy_from_slice(&[0, 1, 2]);
    workspace[layout.block_offsets] = 0;
    let segments = vec![0, 3, 0, 0];
    let dead = RESIDENT_U32_SENTINEL;
    let candidates = vec![0, 1, 2, dead, 0, 1, 2, dead, 7, 8, 9, dead];
    (layout, workspace, segments, candidates)
}

#[test]
fn dual_dispatch_publication_is_mutually_exclusive_and_reuse_safe() {
    let context = crate::WgpuContext::on_wgpu();
    let capacity = 65usize;
    let mut public_dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    let mut failure_dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();

    // Alternate both directions through the same two persistent records. The
    // forced 1x2 rectangles make stale Y/Z words observable instead of letting
    // the ordinary 2x1 host geometry mask an incomplete overwrite.
    let control_words = [
        [STATUS_OK, capacity as u32, 1, 2],
        [STATUS_CAPACITY, capacity as u32 + 1, 99, 99],
        [STATUS_OK, capacity as u32, 1, 2],
        [STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 99, 99],
        [STATUS_OK, capacity as u32, 1, 2],
        [STATUS_GEOMETRY, RESIDENT_U32_SENTINEL, 99, 99],
        [STATUS_OK, capacity as u32, 1, 2],
        [99, RESIDENT_U32_SENTINEL, 99, 99],
        [STATUS_OK, 0, 0, 1],
    ];
    let controls = control_words.map(|words| context.upload_u32(&words).unwrap());
    let mut packed = context.empty_u32(controls.len() * 6).unwrap();

    for (case, control) in controls.iter().enumerate() {
        unsafe {
            publish_proposal_and_failure_dispatch::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                control.input_arg(),
                public_dispatch.output_arg(),
                failure_dispatch.output_arg(),
                1,
                2,
                STATUS_OK,
            );
            pack_dispatch_pair::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                public_dispatch.output_arg(),
                failure_dispatch.output_arg(),
                packed.output_arg(),
                (case * 6) as u32,
            );
        }
    }

    let packed = packed.read();
    for case in 0..control_words.len() {
        let words = &packed[case * 6..case * 6 + 6];
        if control_words[case][CONTROL_STATUS] == STATUS_OK {
            let expected_public = if case + 1 == controls.len() {
                [0, 1, 1]
            } else {
                [1, 2, 1]
            };
            assert_eq!(&words[..3], &expected_public, "success case {case}");
            assert_eq!(&words[3..], &[0, 1, 1], "success case {case}");
        } else {
            assert_eq!(&words[..3], &[0, 1, 1], "failure case {case}");
            assert_eq!(&words[3..], &[1, 2, 1], "failure case {case}");
        }
    }
}

#[test]
fn forced_two_dimensional_failure_cleanup_reaches_lane_64() {
    let context = crate::WgpuContext::on_wgpu();
    let capacity = 65usize;
    let mut public_dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    let mut failure_dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    let control = context
        .upload_u32(&[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL, 99, 99])
        .unwrap();
    let mut segments = context.upload_u32(&[17; SEGMENT_RECORD_WORDS]).unwrap();
    let mut candidates = context
        .upload_u32(&vec![17; capacity * CANDIDATE_RECORD_FIELDS])
        .unwrap();
    let mut children = context.upload_u32(&vec![17; capacity]).unwrap();

    unsafe {
        publish_proposal_and_failure_dispatch::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            control.input_arg(),
            public_dispatch.output_arg(),
            failure_dispatch.output_arg(),
            1,
            2,
            STATUS_OK,
        );
        poison_failed_proposal_outputs::launch_unchecked::<WgpuRuntime>(
            context.client(),
            failure_dispatch.cube_count(),
            failure_dispatch.cube_dim(),
            control.input_arg(),
            segments.output_arg(),
            candidates.output_arg(),
            children.output_arg(),
            SEGMENT_RECORD_WORDS as u32,
            capacity as u32,
            capacity as u32,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
        );
    }

    let candidates = candidates.read();
    assert_eq!(candidates[64], RESIDENT_U32_SENTINEL);
    assert_eq!(candidates[capacity + 64], RESIDENT_U32_SENTINEL);
    assert_eq!(candidates[capacity * 2 + 64], RESIDENT_U32_SENTINEL);
    assert_eq!(children.read()[64], RESIDENT_U32_SENTINEL);
    assert!(segments
        .read()
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn confirmation_workspace_does_not_inflate_failure_cleanup_geometry() {
    let capacity = 65usize;
    let confirmation = confirmation_workspace_layout(capacity).unwrap();
    let (initial_poison_len, semantic_poison_len) =
        proposal_poison_lengths(SEGMENT_RECORD_WORDS, capacity, capacity, confirmation.words);
    assert!(confirmation.words > semantic_poison_len);
    assert_eq!(initial_poison_len, confirmation.words);
    assert_eq!(semantic_poison_len, capacity);
}

#[derive(Clone, Debug)]
struct CanonicalGateModel {
    rows: usize,
    segments: usize,
    counts: Vec<u32>,
    prefixes: Vec<u32>,
    row_arms: Vec<u32>,
    row_segments: Vec<u32>,
    row_counts: Vec<u32>,
}

impl CanonicalGateModel {
    fn from_rows(
        segments: usize,
        row_arms: Vec<u32>,
        row_segments: Vec<u32>,
        row_counts: Vec<u32>,
    ) -> Self {
        let rows = row_arms.len();
        assert_eq!(row_segments.len(), rows);
        assert_eq!(row_counts.len(), rows);
        assert!(segments != 0 || rows == 0);
        let mut counts = vec![0; segments * rows];
        for row in 0..rows {
            let segment = row_segments[row] as usize;
            assert!(segment < segments);
            counts[segment * rows + row] = row_counts[row];
        }
        let mut prefixes: Vec<u32> = Vec::with_capacity(counts.len() + 1);
        prefixes.push(0);
        for &count in &counts {
            prefixes.push(
                prefixes
                    .last()
                    .copied()
                    .unwrap()
                    .checked_add(count)
                    .unwrap(),
            );
        }
        Self {
            rows,
            segments,
            counts,
            prefixes,
            row_arms,
            row_segments,
            row_counts,
        }
    }

    fn total(&self) -> u32 {
        self.prefixes.last().copied().unwrap()
    }

    fn canonical_record(&self, destination: u32, domain: u32) -> [u32; 3] {
        assert!(destination < self.total());
        let cell = self
            .prefixes
            .windows(2)
            .position(|window| destination < window[1])
            .unwrap();
        let row = cell % self.rows;
        [destination % domain, row as u32, self.row_arms[row]]
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Self {
        Self::from_rows(
            self.segments,
            self.row_arms[range.clone()].to_vec(),
            self.row_segments[range.clone()].to_vec(),
            self.row_counts[range].to_vec(),
        )
    }

    fn segment_route_shape(&self, owner_bias: u32) -> Vec<Vec<(u32, u32)>> {
        (0..self.segments)
            .map(|segment| {
                (0..self.rows)
                    .flat_map(|row| {
                        std::iter::repeat_n(
                            (owner_bias + row as u32, self.row_arms[row]),
                            self.counts[segment * self.rows + row] as usize,
                        )
                    })
                    .collect()
            })
            .collect()
    }
}

fn old_lower_bound_gate_verdict(
    model: &CanonicalGateModel,
    status: u32,
    total: u32,
    capacity: u32,
    domain: u32,
    destination: u32,
    record: [u32; 3],
) -> u32 {
    let dead = RESIDENT_U32_SENTINEL;
    if destination >= capacity {
        return dead;
    }
    if status != STATUS_OK || destination >= total {
        return if record == [dead; 3] { 0 } else { dead };
    }
    if model.rows == 0 || record[0] >= domain {
        return dead;
    }

    let destination = destination as usize;
    let segment = (0..model.segments)
        .find(|segment| model.prefixes[(segment + 1) * model.rows] as usize > destination);
    let Some(segment) = segment else {
        return dead;
    };
    let row = (0..model.rows)
        .find(|row| model.prefixes[segment * model.rows + row + 1] as usize > destination);
    let Some(row) = row else {
        return dead;
    };
    let cell = segment * model.rows + row;
    let start = model.prefixes[cell] as usize;
    let end = model.prefixes[cell + 1] as usize;
    if destination >= start
        && destination < end
        && record[1] == row as u32
        && record[2] == model.row_arms[row]
        && model.row_segments[row] == segment as u32
        && model.row_counts[row] == model.counts[cell]
    {
        1
    } else {
        dead
    }
}

#[allow(clippy::too_many_arguments)]
fn owner_indexed_gate_verdict(
    model: &CanonicalGateModel,
    status: u32,
    total: u32,
    capacity: u32,
    domain: u32,
    arm_count: u32,
    destination: u32,
    record: [u32; 3],
) -> u32 {
    let dead = RESIDENT_U32_SENTINEL;
    if destination >= capacity {
        return dead;
    }
    if status != STATUS_OK || destination >= total {
        return if record == [dead; 3] { 0 } else { dead };
    }
    let row = record[1] as usize;
    if record[0] >= domain || row >= model.rows {
        return dead;
    }
    let arm = model.row_arms[row];
    let segment = model.row_segments[row] as usize;
    if arm >= arm_count
        || record[2] != arm
        || segment >= model.segments
        || model.row_counts[row] == dead
    {
        return dead;
    }
    let Some(cell) = segment
        .checked_mul(model.rows)
        .and_then(|cell| cell.checked_add(row))
    else {
        return dead;
    };
    let Some((&count, prefix)) = model
        .counts
        .get(cell)
        .zip(model.prefixes.get(cell..=cell + 1))
    else {
        return dead;
    };
    let start = prefix[0];
    let end = prefix[1];
    if model.row_counts[row] == count && destination >= start && destination < end {
        1
    } else {
        dead
    }
}

fn deterministic_gate_model(rows: usize, segments: usize, mut state: u64) -> CanonicalGateModel {
    let mut row_arms = Vec::with_capacity(rows);
    let mut row_segments = Vec::with_capacity(rows);
    let mut row_counts = Vec::with_capacity(rows);
    for row in 0..rows {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        row_arms.push(((state + row as u64) % 10) as u32);
        row_segments.push(((state.rotate_left(19) + row as u64) % segments as u64) as u32);
        // Include empty cells and duplicate-width runs around every scan edge.
        row_counts.push(((state.rotate_left(41) + row as u64) % 4) as u32);
    }
    CanonicalGateModel::from_rows(segments, row_arms, row_segments, row_counts)
}

#[test]
fn owner_indexed_gate_matches_old_lower_bound_on_canonical_matrices() {
    const DOMAIN: u32 = 4096;
    const ARM_COUNT: u32 = 10;
    for rows in [0usize, 1, 2, 63, 64, 65] {
        for segments in 1usize..=5 {
            for seed in 0u64..32 {
                let model = deterministic_gate_model(rows, segments, seed + 1);
                let total = model.total();
                let capacity = total + 1;
                for destination in 0..total {
                    let record = model.canonical_record(destination, DOMAIN);
                    assert_eq!(
                        owner_indexed_gate_verdict(
                            &model,
                            STATUS_OK,
                            total,
                            capacity,
                            DOMAIN,
                            ARM_COUNT,
                            destination,
                            record,
                        ),
                        old_lower_bound_gate_verdict(
                            &model,
                            STATUS_OK,
                            total,
                            capacity,
                            DOMAIN,
                            destination,
                            record,
                        ),
                        "rows={rows} segments={segments} seed={seed} destination={destination}"
                    );
                }
                for (status, destination) in [(STATUS_OK, total), (STATUS_CAPACITY, 0)] {
                    assert_eq!(
                        owner_indexed_gate_verdict(
                            &model,
                            status,
                            total,
                            capacity,
                            DOMAIN,
                            ARM_COUNT,
                            destination,
                            [RESIDENT_U32_SENTINEL; 3],
                        ),
                        old_lower_bound_gate_verdict(
                            &model,
                            status,
                            total,
                            capacity,
                            DOMAIN,
                            destination,
                            [RESIDENT_U32_SENTINEL; 3],
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn owner_indexed_gate_faults_and_row_splits_preserve_the_proof_boundary() {
    const DOMAIN: u32 = 4096;
    const ARM_COUNT: u32 = 10;
    let model = deterministic_gate_model(65, 4, 0x5eed);
    assert!(model.row_counts.contains(&0));
    assert!(model.row_counts.iter().any(|&count| count != 0));
    let total = model.total();
    let capacity = total + 1;
    for destination in 0..total {
        let record = model.canonical_record(destination, DOMAIN);
        let owner = record[1] as usize;
        let mut faults = Vec::new();
        let mut wrong_owner = record;
        wrong_owner[1] = ((owner + 1) % model.rows) as u32;
        faults.push(("wrong owner", model.clone(), wrong_owner));
        let owner_segment = model.row_segments[owner];
        let same_segment_owner = (0..model.rows)
            .find(|&row| row != owner && model.row_segments[row] == owner_segment)
            .unwrap();
        let mut wrong_owner_same_segment = record;
        wrong_owner_same_segment[1] = same_segment_owner as u32;
        faults.push((
            "wrong owner in the same segment",
            model.clone(),
            wrong_owner_same_segment,
        ));
        let other_segment_owner = (0..model.rows)
            .find(|&row| model.row_segments[row] != owner_segment)
            .unwrap();
        let mut wrong_owner_other_segment = record;
        wrong_owner_other_segment[1] = other_segment_owner as u32;
        faults.push((
            "wrong owner in another segment",
            model.clone(),
            wrong_owner_other_segment,
        ));
        let mut out_of_range_owner = record;
        out_of_range_owner[1] = model.rows as u32;
        faults.push(("out-of-range owner", model.clone(), out_of_range_owner));
        let mut wrong_proposer = record;
        wrong_proposer[2] = (record[2] + 1) % ARM_COUNT;
        faults.push(("wrong proposer", model.clone(), wrong_proposer));
        let mut wrong_segment = model.clone();
        wrong_segment.row_segments[owner] = (wrong_segment.row_segments[owner] + 1) % 4;
        faults.push(("wrong retained segment", wrong_segment, record));
        let mut wrong_count = model.clone();
        wrong_count.row_counts[owner] += 1;
        faults.push(("wrong retained count", wrong_count, record));

        for (name, faulty_model, faulty_record) in faults {
            let old = old_lower_bound_gate_verdict(
                &faulty_model,
                STATUS_OK,
                total,
                capacity,
                DOMAIN,
                destination,
                faulty_record,
            );
            let owner_indexed = owner_indexed_gate_verdict(
                &faulty_model,
                STATUS_OK,
                total,
                capacity,
                DOMAIN,
                ARM_COUNT,
                destination,
                faulty_record,
            );
            assert_eq!(owner_indexed, old, "{name} at destination {destination}");
            assert_eq!(owner_indexed, RESIDENT_U32_SENTINEL, "{name}");
        }
    }

    // Explicit arm-range authentication is a strengthening over the old
    // lower-bound kernel on otherwise coherent retained words.
    let destination = 0;
    let record = model.canonical_record(destination, DOMAIN);
    let owner = record[1] as usize;
    let mut out_of_range_arm = model.clone();
    out_of_range_arm.row_arms[owner] = ARM_COUNT;
    let coherent_record = [record[0], record[1], ARM_COUNT];
    assert_eq!(
        old_lower_bound_gate_verdict(
            &out_of_range_arm,
            STATUS_OK,
            total,
            capacity,
            DOMAIN,
            destination,
            coherent_record,
        ),
        1
    );
    assert_eq!(
        owner_indexed_gate_verdict(
            &out_of_range_arm,
            STATUS_OK,
            total,
            capacity,
            DOMAIN,
            ARM_COUNT,
            destination,
            coherent_record,
        ),
        RESIDENT_U32_SENTINEL
    );

    for split in [0usize, 1, 32, 63, 64, 65] {
        let left = model.slice(0..split);
        let right = model.slice(split..model.rows);
        let mut joined = left.segment_route_shape(0);
        for (segment, right_routes) in right
            .segment_route_shape(split as u32)
            .into_iter()
            .enumerate()
        {
            joined[segment].extend(right_routes);
        }
        assert_eq!(joined, model.segment_route_shape(0), "split {split}");

        for half in [&left, &right] {
            let half_total = half.total();
            for half_destination in 0..half_total {
                let half_record = half.canonical_record(half_destination, DOMAIN);
                assert_eq!(
                    owner_indexed_gate_verdict(
                        half,
                        STATUS_OK,
                        half_total,
                        half_total,
                        DOMAIN,
                        ARM_COUNT,
                        half_destination,
                        half_record,
                    ),
                    1,
                    "split {split}, local destination {half_destination}"
                );
            }
        }
    }
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
    descriptors[2] = ResidentAxis::Attribute.code();
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
    trusted[entity_arm * ARM_DESCRIPTOR_WORDS + 2] = ResidentAxis::Value.code();
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
    let confirmation_layout = confirmation_workspace_layout(1).unwrap();
    let arena = WgpuResidentProposals {
        context: context.clone(),
        round_owner: Arc::new(()),
        frontier_lineage: Arc::new(()),
        _frontier_values: Arc::new(context.upload_u32(&[]).unwrap()),
        _proposal_witness: Arc::new(context.upload_u32(&[]).unwrap()),
        arena_lineage: Arc::new(()),
        rows: 1,
        parent_stride: 0,
        child_stride: 1,
        segment_count: 1,
        capacity: 1,
        _planning_control: context
            .upload_u32(&[STATUS_GEOMETRY, RESIDENT_U32_SENTINEL, 0, 1])
            .unwrap(),
        _workspace: workspace,
        _plan: context.upload_u32(&[]).unwrap(),
        _semantic_confirmation: None,
        _generation_dispatch: context
            .batch_dispatch(0, 1, CubeDim::new_1d(THREADS))
            .unwrap(),
        _pair_generation: None,
        _restricted_generation: None,
        control,
        meta,
        dispatch,
        _failure_dispatch: None,
        segment_records,
        candidate_records: context
            .upload_u32(&[RESIDENT_U32_SENTINEL; CANDIDATE_RECORD_FIELDS])
            .unwrap(),
        child_body: context.upload_u32(&poison).unwrap(),
        confirmation_workspace: context
            .upload_u32(&vec![0; confirmation_layout.words])
            .unwrap(),
        confirmation_layout,
        provisional_backing: None,
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
    assert!(inspection
        .candidate_codes
        .iter()
        .chain(&inspection.candidate_owners)
        .chain(&inspection.proposer_arms)
        .chain(&inspection.child_body)
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn confirmed_finalizer_closes_the_upstream_status_required_lattice() {
    let context = crate::WgpuContext::on_wgpu();
    let capacity = 1usize;
    let layout = confirmation_workspace_layout(capacity).unwrap();
    let dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    let run = |upstream_status: u32, upstream_required: u32| {
        let mut workspace = context.upload_u32(&vec![0; layout.words]).unwrap();
        let provisional_control = context
            .upload_u32(&[upstream_status, upstream_required, 0, 1])
            .unwrap();
        let provisional_segments = context.upload_u32(&[0, 0, 0, 0]).unwrap();
        let mut final_control = context
            .upload_u32(&[STATUS_OK, RESIDENT_U32_SENTINEL, 0, 1])
            .unwrap();
        let mut final_segments = context
            .upload_u32(&[RESIDENT_U32_SENTINEL; SEGMENT_RECORD_WORDS])
            .unwrap();
        unsafe {
            finalize_confirmed_publication::launch_unchecked::<WgpuRuntime>(
                context.client(),
                CubeCount::new_single(),
                CubeDim::new_single(),
                workspace.output_arg(),
                provisional_control.input_arg(),
                provisional_segments.input_arg(),
                final_control.output_arg(),
                final_segments.output_arg(),
                capacity as u32,
                layout.block_count as u32,
                1,
                0,
                1,
                dispatch.max_groups_x(),
                dispatch.max_groups_y(),
                THREADS,
                layout.local_offsets as u32,
                layout.block_sums as u32,
                layout.block_errors as u32,
                layout.block_offsets as u32,
                layout.semantic_status as u32,
                layout.final_status as u32,
                layout.final_total as u32,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
                STATUS_CAPACITY,
                STATUS_DEVICE_INVARIANT,
                STATUS_GEOMETRY,
            );
        }
        final_control.read()
    };

    assert_eq!(&run(STATUS_CAPACITY, 2)[..2], &[STATUS_CAPACITY, 2]);
    for required in [1, RESIDENT_U32_SENTINEL] {
        assert_eq!(
            &run(STATUS_CAPACITY, required)[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }
    assert_eq!(
        &run(STATUS_OK, 2)[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(
        &run(STATUS_DEVICE_INVARIANT, 0)[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(
        &run(STATUS_GEOMETRY, 0)[..2],
        &[STATUS_GEOMETRY, RESIDENT_U32_SENTINEL]
    );
    assert_eq!(
        &run(99, RESIDENT_U32_SENTINEL)[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );

    // No block scan launches at capacity zero, so the scalar finalizer must
    // still consume the sticky semantic status directly.
    let zero = confirmation_workspace_layout(0).unwrap();
    let mut zero_words = vec![0; zero.words];
    zero_words[zero.semantic_status] = STATUS_DEVICE_INVARIANT;
    let mut workspace = context.upload_u32(&zero_words).unwrap();
    let provisional_control = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    let provisional_segments = context.upload_u32(&[0, 0, 0, 0]).unwrap();
    let mut final_control = context
        .upload_u32(&[STATUS_OK, RESIDENT_U32_SENTINEL, 0, 1])
        .unwrap();
    let mut final_segments = context
        .upload_u32(&[RESIDENT_U32_SENTINEL; SEGMENT_RECORD_WORDS])
        .unwrap();
    let zero_dispatch = context
        .batch_dispatch(0, 0, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        finalize_confirmed_publication::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.output_arg(),
            provisional_control.input_arg(),
            provisional_segments.input_arg(),
            final_control.output_arg(),
            final_segments.output_arg(),
            0,
            0,
            1,
            0,
            1,
            zero_dispatch.max_groups_x(),
            zero_dispatch.max_groups_y(),
            THREADS,
            zero.local_offsets as u32,
            zero.block_sums as u32,
            zero.block_errors as u32,
            zero.block_offsets as u32,
            zero.semantic_status as u32,
            zero.final_status as u32,
            zero.final_total as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    assert_eq!(
        &final_control.read()[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );
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

    // Confirmed generic publication must preserve global semantic death as a
    // successful empty arena. Its intentionally sentinel descriptors name no
    // work, and every retained per-arm work counter therefore stays zero.
    let confirmed_dead_arena = dead_round
        .enqueue_confirmed_generic_proposals_for_test(&dead_frontier, &dead_choices, 7)
        .unwrap();
    assert!(confirmed_dead_arena
        .read_semantic_confirmation_work_for_test()
        .iter()
        .all(|&entry| entry == [0, 0]));
    let confirmed_dead = confirmed_dead_arena.inspect();
    assert_success(&confirmed_dead, 0);
    assert!(confirmed_dead
        .segments
        .iter()
        .all(|segment| segment.base == 0 && segment.count == 0));
    assert!(confirmed_dead
        .candidate_codes
        .iter()
        .chain(&confirmed_dead.candidate_owners)
        .chain(&confirmed_dead.proposer_arms)
        .chain(&confirmed_dead.child_body)
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

#[test]
fn pair_lf_all_six_rotations_match_independent_ordered_id_oracle() {
    let fixture = pair_fixture();
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
    let cases = [
        (SuccinctRotation::Eav, fixture.entities[0]),
        (SuccinctRotation::Vea, fixture.values[0]),
        (SuccinctRotation::Ave, fixture.attributes[2]),
        (SuccinctRotation::Vae, fixture.values[0]),
        (SuccinctRotation::Eva, fixture.entities[0]),
        (SuccinctRotation::Aev, fixture.attributes[0]),
    ];
    let mut seen = [false; 6];

    for (rotation, peer) in cases {
        let expected_ids = expected_pair_ids(&fixture, rotation, peer);
        let expected_codes = codes(&program, &expected_ids);
        let capacity = expected_codes.len() + 3;
        let (inspection, arm, peer_code, target) =
            enqueue_pair_case(&gpu, &program, rotation, peer, capacity, false);
        let (confirmed, confirmed_arm, confirmed_peer, confirmed_target) =
            enqueue_pair_case(&gpu, &program, rotation, peer, capacity, true);
        assert_eq!(confirmed_arm, arm);
        assert_eq!(confirmed_peer, peer_code);
        assert_eq!(confirmed_target, target);
        assert_eq!(confirmed, inspection, "confirmed {rotation:?}");
        assert_success(&inspection, expected_codes.len());
        seen[rotation.index()] = true;

        let segment = inspection
            .segments
            .iter()
            .find(|segment| segment.variable == target)
            .unwrap();
        assert_eq!(segment.count as usize, expected_codes.len());
        let start = segment.base as usize;
        let end = start + segment.count as usize;
        assert_eq!(&inspection.candidate_codes[start..end], expected_codes);
        assert!(inspection.candidate_owners[start..end]
            .iter()
            .all(|&owner| owner == 0));
        assert!(inspection.proposer_arms[start..end]
            .iter()
            .all(|&proposer| proposer == arm));

        let (bound, target_variable) = pair_axes(rotation);
        assert_eq!(target_variable.index() as u32, target);
        let expected_children: Vec<Vec<u32>> = expected_codes
            .iter()
            .map(|&candidate| {
                if target_variable < bound {
                    vec![candidate, peer_code]
                } else {
                    vec![peer_code, candidate]
                }
            })
            .collect();
        let actual_children: Vec<Vec<u32>> = inspection.child_body
            [start * inspection.child_stride as usize..end * inspection.child_stride as usize]
            .chunks_exact(inspection.child_stride as usize)
            .map(<[u32]>::to_vec)
            .collect();
        assert_eq!(actual_children, expected_children, "{rotation:?}");

        assert!(inspection.candidate_codes[end..]
            .iter()
            .all(|&word| word == RESIDENT_U32_SENTINEL));
        assert!(inspection.candidate_owners[end..]
            .iter()
            .all(|&word| word == RESIDENT_U32_SENTINEL));
        assert!(inspection.proposer_arms[end..]
            .iter()
            .all(|&word| word == RESIDENT_U32_SENTINEL));
        assert!(
            inspection.child_body[end * inspection.child_stride as usize..]
                .iter()
                .all(|&word| word == RESIDENT_U32_SENTINEL)
        );
    }
    assert!(seen.into_iter().all(|seen| seen));
}

#[test]
fn confirmed_pair_all_six_rotations_match_cpu_order_and_parent_multiplicity() {
    let fixture = pair_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let target = ProgramVariable::new(0);
    let other = ProgramVariable::new(1);
    let bound = ProgramVariable::new(2);

    for rotation in SuccinctRotation::ALL {
        let program = QueryProgram::compile(
            gpu.archive(),
            3,
            [pair_cpu_pattern(rotation, target, other, bound)],
        )
        .unwrap();
        let round = WgpuResidentRound::new(&gpu, &program, &[bound]).unwrap();
        let arm = round
            .proposal_arm_specs()
            .iter()
            .position(|spec| {
                matches!(
                    spec,
                    ArmSpec::PairDistinct {
                        rotation: candidate,
                        ..
                    } if *candidate == rotation
                )
            })
            .expect("requested Pair rotation is admitted");
        let peers = pair_cpu_peer_rows(&fixture, rotation);
        let peer_codes = peers
            .iter()
            .map(|&peer| program.encode(&raw(peer)).unwrap().get())
            .collect::<Vec<_>>();
        let host = program
            .frontier_from_indices(vec![bound], peer_codes, peers.len())
            .unwrap();
        let cpu = program.transition(&host).unwrap();
        assert_eq!(cpu.len(), 1, "CPU transition group for {rotation:?}");
        assert_eq!(
            cpu[0].variables(),
            &[target, bound],
            "CPU transition variable for {rotation:?}"
        );
        let cpu_body = cpu[0]
            .values()
            .iter()
            .map(|code| code.get())
            .collect::<Vec<_>>();

        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();
        let mut choice_words = Vec::with_capacity(peers.len() * CHOICE_WORDS);
        let mut expected_owners = Vec::new();
        for (row, &peer) in peers.iter().enumerate() {
            let witness = (arm * peers.len() + row) * PROPOSAL_WITNESS_WORDS;
            let count = witnesses[witness + 3] - witnesses[witness + 2];
            assert_eq!(
                count as usize,
                expected_pair_ids(&fixture, rotation, peer).len(),
                "raw selected-row width for {rotation:?}, row {row}"
            );
            expected_owners.extend(std::iter::repeat_n(row as u32, count as usize));
            choice_words.extend([target.index() as u32, arm as u32, count]);
        }
        let choices = round
            .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
            .unwrap();
        let expected = cpu[0].len();
        assert_eq!(expected_owners.len(), expected);
        let inspection = round
            .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, expected)
            .unwrap()
            .inspect();
        assert_success(&inspection, expected);
        assert_eq!(inspection.child_body, cpu_body, "{rotation:?}");
        assert_eq!(inspection.candidate_owners, expected_owners, "{rotation:?}");
        assert!(inspection
            .proposer_arms
            .iter()
            .all(|&proposer| proposer == arm as u32));
        let cpu_candidates = cpu_body
            .chunks_exact(inspection.child_stride as usize)
            .map(|row| row[0])
            .collect::<Vec<_>>();
        assert_eq!(inspection.candidate_codes, cpu_candidates, "{rotation:?}");
    }
}

#[test]
fn pair_lf_zero_width_and_capacity_failure_are_atomic() {
    let fixture = pair_fixture();
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

    // An attribute code is in-domain but absent from the entity axis, so the
    // exact EAV Pair witness is a live zero-width interval.
    let (zero, _, _, _) = enqueue_pair_case(
        &gpu,
        &program,
        SuccinctRotation::Eav,
        fixture.attributes[2],
        4,
        false,
    );
    let (confirmed_zero, _, _, _) = enqueue_pair_case(
        &gpu,
        &program,
        SuccinctRotation::Eav,
        fixture.attributes[2],
        4,
        true,
    );
    assert_eq!(confirmed_zero, zero);
    assert_success(&zero, 0);
    assert!(zero.segments.iter().all(|segment| segment.count == 0));
    assert!(zero
        .candidate_codes
        .iter()
        .chain(zero.candidate_owners.iter())
        .chain(zero.proposer_arms.iter())
        .chain(zero.child_body.iter())
        .all(|&word| word == RESIDENT_U32_SENTINEL));

    let expected = expected_pair_ids(&fixture, SuccinctRotation::Eav, fixture.entities[0]);
    assert_eq!(expected.len(), 3);
    let (short, _, _, _) = enqueue_pair_case(
        &gpu,
        &program,
        SuccinctRotation::Eav,
        fixture.entities[0],
        expected.len() - 1,
        false,
    );
    let (confirmed_short, _, _, _) = enqueue_pair_case(
        &gpu,
        &program,
        SuccinctRotation::Eav,
        fixture.entities[0],
        expected.len() - 1,
        true,
    );
    assert_eq!(confirmed_short, short);
    assert_eq!(short.status, STATUS_CAPACITY);
    assert_eq!(short.required, expected.len() as u32);
    assert_eq!(short.logical_len, 0);
    assert_eq!((short.dispatch_x, short.dispatch_y), (0, 1));
    assert!(short
        .candidate_codes
        .iter()
        .chain(short.candidate_owners.iter())
        .chain(short.proposer_arms.iter())
        .chain(short.child_body.iter())
        .all(|&word| word == RESIDENT_U32_SENTINEL));
}

#[test]
fn pair_query_generation_rejects_out_of_range_segment_before_cell_alias() {
    let fixture = pair_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let context = gpu.context();

    // S=1,R=1 makes segment=1 invalid. The following packed bases
    // deliberately contain plausible values at the would-be aliased cell so
    // the scalar segment bound, rather than an incidental buffer value, is
    // what prevents route publication.
    let mut workspace_words = vec![0u32; 16];
    let counts_base = 0usize;
    let local_offsets_base = 2usize;
    let block_offsets_base = 4usize;
    let row_arms_base = 5usize;
    let row_families_base = 6usize;
    let row_physicals_base = 7usize;
    let row_segments_base = 8usize;
    let row_counts_base = 9usize;
    let row_enum_los_base = 10usize;
    workspace_words[counts_base + 1] = 1;
    workspace_words[local_offsets_base + 1] = 0;
    workspace_words[block_offsets_base] = 0;
    workspace_words[row_arms_base] = 7;
    workspace_words[row_families_base] = FAMILY_PAIR_DISTINCT;
    workspace_words[row_physicals_base] = SuccinctRotation::Eav.index() as u32;
    workspace_words[row_segments_base] = 1;
    workspace_words[row_counts_base] = 1;
    workspace_words[row_enum_los_base] = 0;
    let workspace = context.upload_u32(&workspace_words).unwrap();

    let pair_counts_base = 0usize;
    let pair_local_offsets_base = 1usize;
    let pair_block_offsets_base = 2usize;
    let pair_workspace = context.upload_u32(&[1, 0, 0]).unwrap();
    let control = context.upload_u32(&[STATUS_OK, 1, 1, 1]).unwrap();
    let mut queries = context.upload_u32(&[55]).unwrap();
    let mut routes = context.upload_u32(&[55]).unwrap();
    let mut candidates = context
        .upload_u32(&[RESIDENT_U32_SENTINEL; CANDIDATE_RECORD_FIELDS])
        .unwrap();
    unsafe {
        generate_pair_queries::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.input_arg(),
            pair_workspace.input_arg(),
            control.input_arg(),
            queries.output_arg(),
            routes.output_arg(),
            candidates.output_arg(),
            1,
            1,
            1,
            0,
            SuccinctRotation::Eav.index() as u32,
            1,
            counts_base as u32,
            local_offsets_base as u32,
            block_offsets_base as u32,
            row_arms_base as u32,
            row_families_base as u32,
            row_physicals_base as u32,
            row_segments_base as u32,
            row_counts_base as u32,
            row_enum_los_base as u32,
            pair_counts_base as u32,
            pair_local_offsets_base as u32,
            pair_block_offsets_base as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
        );
    }
    assert_eq!(queries.read(), [RESIDENT_U32_SENTINEL]);
    assert_eq!(routes.read(), [RESIDENT_U32_SENTINEL]);
    assert_eq!(
        candidates.read(),
        [RESIDENT_U32_SENTINEL; CANDIDATE_RECORD_FIELDS]
    );
}

#[test]
fn pair_lf_reuses_rotation_scratch_across_sixty_five_sparse_rows() {
    const ROWS: usize = 65;
    let fixture = pair_fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&fixture.set).into();
    let gpu = crate::WgpuSuccinctArchive::new(archive).unwrap();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let present_entity = ProgramVariable::new(3);
    let present_attribute = ProgramVariable::new(4);
    let present_value = ProgramVariable::new(5);
    let program = QueryProgram::compile(
        gpu.archive(),
        6,
        [
            QueryPattern::new(entity, attribute, value),
            QueryPattern::new(present_entity, present_attribute, present_value),
        ],
    )
    .unwrap();
    let cpu = gpu.archive();
    let changes = cpu.pair_changes(SuccinctRotation::Eav);
    let current = cpu.ring_col(SuccinctRotation::Eav);
    let adjacent = cpu.ring_col(SuccinctRotation::Vea);
    let pair_count = changes.rank1(changes.len()).unwrap();
    let rank_off_by_one_is_visible = (0..pair_count).any(|q| {
        let index = changes.select1(q).unwrap();
        let last = current.access(index).unwrap();
        let selected = cpu.v_a.select1(last).unwrap();
        let rank = current.rank(index, last).unwrap();
        let position = selected - last + rank;
        rank > 0
            && position + 1 < adjacent.len()
            && adjacent.access(position) != adjacent.access(position + 1)
    });
    assert!(
        rank_off_by_one_is_visible,
        "fixture must distinguish rank(idx,last) from rank(idx+1,last)"
    );
    let round = WgpuResidentRound::new(&gpu, &program, &[entity]).unwrap();
    let eav_arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::PairDistinct {
                    rotation: SuccinctRotation::Eav,
                    ..
                }
            )
        })
        .unwrap();
    let eva_arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::PairDistinct {
                    rotation: SuccinctRotation::Eva,
                    ..
                }
            )
        })
        .unwrap();
    let present_entity_arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::Present {
                    axis: ResidentAxis::Entity,
                    ..
                }
            )
        })
        .unwrap();
    let present_attribute_arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::Present {
                    axis: ResidentAxis::Attribute,
                    ..
                }
            )
        })
        .unwrap();
    let present_value_arm = round
        .proposal_arm_specs()
        .iter()
        .position(|spec| {
            matches!(
                spec,
                ArmSpec::Present {
                    axis: ResidentAxis::Value,
                    ..
                }
            )
        })
        .unwrap();
    let peer_ids: Vec<_> = (0..ROWS)
        .map(|row| match row % 4 {
            0 => fixture.entities[0],
            1 => fixture.entities[0],
            2 => fixture.entities[2],
            _ => fixture.entities[1],
        })
        .collect();
    let peer_codes: Vec<_> = peer_ids
        .iter()
        .map(|&peer| program.encode(&raw(peer)).unwrap().get())
        .collect();
    let host = program
        .frontier_from_indices(vec![entity], peer_codes.clone(), ROWS)
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let mut choice_words = Vec::with_capacity(ROWS * CHOICE_WORDS);
    let mut selected_arms = Vec::with_capacity(ROWS);
    for row in 0..ROWS {
        let arm = match row {
            1 => present_entity_arm,
            3 => present_attribute_arm,
            5 => present_value_arm,
            _ if row % 2 == 0 => eav_arm,
            _ => eva_arm,
        };
        let target = round.metadata().arms()[arm].target_variable();
        let witness = (arm * ROWS + row) * PROPOSAL_WITNESS_WORDS;
        let count = witnesses[witness + 3] - witnesses[witness + 2];
        choice_words.extend([target.index() as u32, arm as u32, count]);
        selected_arms.push(arm);
    }
    let late_eav_witness = (eav_arm * ROWS + 2) * PROPOSAL_WITNESS_WORDS;
    assert!(
        witnesses[late_eav_witness + 2] > 0,
        "late EAV peer must exercise nonzero enum_lo"
    );
    let choices = round
        .force_choice_words_from_inputs_for_test(&frontier, &inputs, &choice_words)
        .unwrap();

    let mut expected_codes = Vec::new();
    let mut expected_owners = Vec::new();
    let mut expected_arms = Vec::new();
    let mut expected_children = Vec::new();
    let mut expected_rotation_counts = [0usize; 6];
    for target in 0..6 {
        for row in 0..ROWS {
            let arm = selected_arms[row];
            if round.metadata().arms()[arm].target_variable().index() != target {
                continue;
            }
            let ids = match round.proposal_arm_specs()[arm] {
                ArmSpec::PairDistinct { rotation, .. } => {
                    let ids = expected_pair_ids(&fixture, rotation, peer_ids[row]);
                    expected_rotation_counts[rotation.index()] += ids.len();
                    ids
                }
                ArmSpec::Present { axis, .. } => match axis {
                    ResidentAxis::Entity => fixture.entities.to_vec(),
                    ResidentAxis::Attribute => fixture.attributes.to_vec(),
                    ResidentAxis::Value => fixture.values.to_vec(),
                },
                ArmSpec::Restricted { .. } => unreachable!(),
            };
            for code in codes(&program, &ids) {
                expected_codes.push(code);
                expected_owners.push(row as u32);
                expected_arms.push(arm as u32);
                expected_children.extend([peer_codes[row], code]);
            }
        }
    }
    let capacity = expected_codes.len() + 7;
    let confirmed = round
        .enqueue_confirmed_generic_proposals_for_test(&frontier, &choices, capacity)
        .unwrap()
        .inspect();
    assert_success(&confirmed, expected_codes.len());
    assert_eq!(
        &confirmed.candidate_codes[..expected_codes.len()],
        expected_codes
    );
    assert_eq!(
        &confirmed.candidate_owners[..expected_owners.len()],
        expected_owners
    );
    assert_eq!(
        &confirmed.proposer_arms[..expected_arms.len()],
        expected_arms
    );
    assert_eq!(
        &confirmed.child_body[..expected_children.len()],
        expected_children
    );
    assert!(confirmed.candidate_codes[expected_codes.len()..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(confirmed.child_body[expected_children.len()..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));

    let max_groups_y = capacity.div_ceil(THREADS as usize) as u32;
    let first = round
        .enqueue_generic_proposals_with_dispatch_limits_for_test(
            &frontier,
            &choices,
            capacity,
            1,
            max_groups_y,
        )
        .unwrap();
    let second = round
        .enqueue_generic_proposals_with_dispatch_limits_for_test(
            &frontier,
            &choices,
            capacity,
            1,
            max_groups_y,
        )
        .unwrap();
    let second_trace = second.read_pair_rotation_trace_for_test();
    let first_trace = first.read_pair_rotation_trace_for_test();
    assert_eq!(first_trace, second_trace);
    assert_eq!(
        first_trace[SuccinctRotation::Eav.index()],
        [
            expected_rotation_counts[SuccinctRotation::Eav.index()] as u32,
            1,
            2
        ],
        "EAV must consume an actual two-dimensional private dispatch"
    );
    let second = second.inspect();
    let first = first.inspect();
    assert_eq!(
        first, second,
        "reused immutable inputs changed queued output"
    );
    assert_success(&first, expected_codes.len());
    assert_eq!(
        &first.candidate_codes[..expected_codes.len()],
        expected_codes
    );
    assert_eq!(
        &first.candidate_owners[..expected_owners.len()],
        expected_owners
    );
    assert_eq!(&first.proposer_arms[..expected_arms.len()], expected_arms);
    assert_eq!(
        &first.child_body[..expected_children.len()],
        expected_children
    );
    assert!(first.candidate_codes[expected_codes.len()..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));
    assert!(first.child_body[expected_children.len()..]
        .iter()
        .all(|&word| word == RESIDENT_U32_SENTINEL));

    // Row-homomorphism at the 32/33 split: concatenate each canonical
    // variable segment independently, rebasing only right-half owners.
    let enqueue_range = |range: std::ops::Range<usize>| {
        let rows = range.len();
        let host = program
            .frontier_from_indices(vec![entity], peer_codes[range.clone()].to_vec(), rows)
            .unwrap();
        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();
        let mut words = Vec::with_capacity(rows * CHOICE_WORDS);
        for (local_row, global_row) in range.clone().enumerate() {
            let arm = selected_arms[global_row];
            let target = round.metadata().arms()[arm].target_variable();
            let witness = (arm * rows + local_row) * PROPOSAL_WITNESS_WORDS;
            let count = witnesses[witness + 3] - witnesses[witness + 2];
            words.extend([target.index() as u32, arm as u32, count]);
        }
        let choices = round
            .force_choice_words_from_inputs_for_test(&frontier, &inputs, &words)
            .unwrap();
        round
            .enqueue_generic_proposals_for_test(&frontier, &choices, 512)
            .unwrap()
            .inspect()
    };
    let split = 32usize;
    let left = live_segments(&enqueue_range(0..split));
    let right = live_segments(&enqueue_range(split..ROWS));
    let whole = live_segments(&first);
    for (variable, expected) in whole {
        let left = left.get(&variable).unwrap();
        let right = right.get(&variable).unwrap();
        let mut codes = left.0.clone();
        codes.extend_from_slice(&right.0);
        let mut owners = left.1.clone();
        owners.extend(right.1.iter().map(|owner| owner + split as u32));
        let mut arms = left.2.clone();
        arms.extend_from_slice(&right.2);
        let mut bodies = left.3.clone();
        bodies.extend_from_slice(&right.3);
        assert_eq!(codes, expected.0, "variable {variable} split codes");
        assert_eq!(owners, expected.1, "variable {variable} split owners");
        assert_eq!(arms, expected.2, "variable {variable} split arms");
        assert_eq!(bodies, expected.3, "variable {variable} split bodies");
    }
}

#[test]
fn generic_pair_descriptors_cover_all_six_rotations_and_classify_exact_witnesses() {
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
    let bound_codes = [
        program.encode(&raw(fixture.entities[0])).unwrap().get(),
        program.encode(&raw(fixture.attributes[0])).unwrap().get(),
        program.encode(&raw(fixture.values[0])).unwrap().get(),
    ];
    let expected = [
        [(1usize, SuccinctRotation::Eav), (2, SuccinctRotation::Eva)],
        [(0usize, SuccinctRotation::Aev), (2, SuccinctRotation::Ave)],
        [(0usize, SuccinctRotation::Vea), (1, SuccinctRotation::Vae)],
    ];
    let mut seen = [false; 6];

    for bound_index in 0..3 {
        let round = WgpuResidentRound::new(&gpu, &program, &[variables[bound_index]]).unwrap();
        let admission = lower_proposal_admission(&round, ProposerPolicy::PairGeneric).unwrap();
        let host = program
            .frontier_from_indices(
                vec![variables[bound_index]],
                vec![bound_codes[bound_index]],
                1,
            )
            .unwrap();
        let frontier = round.upload_frontier(&host).unwrap();
        let inputs = round.initialize_inputs(&frontier).unwrap();
        let witnesses = inputs.read_proposal_witnesses_for_test();

        for &(target_index, rotation) in &expected[bound_index] {
            let arm = round
                .metadata()
                .arms()
                .iter()
                .position(|identity| identity.target_variable() == variables[target_index])
                .unwrap();
            let descriptor = arm * ARM_DESCRIPTOR_WORDS;
            assert_eq!(admission.arm_descriptors[descriptor], target_index as u32);
            assert_eq!(
                admission.arm_descriptors[descriptor + 1],
                FAMILY_PAIR_DISTINCT
            );
            assert_eq!(
                admission.arm_descriptors[descriptor + 2],
                rotation.index() as u32
            );
            assert_eq!(round.proposal_arm_pair_rotation(arm), Some(rotation));
            seen[rotation.index()] = true;

            let witness = arm * PROPOSAL_WITNESS_WORDS;
            let count = witnesses[witness + 3] - witnesses[witness + 2];
            let choice = [target_index as u32, arm as u32, count];
            let classified = classify_direct(
                &round,
                &admission,
                &admission.arm_descriptors,
                1,
                &choice,
                &witnesses,
                count as usize,
            );
            assert_eq!(&classified.control[..2], &[STATUS_OK, count]);
            assert_eq!(classified.workspace[classified.layout.row_arms], arm as u32);
            assert_eq!(
                classified.workspace[classified.layout.row_families],
                FAMILY_PAIR_DISTINCT
            );
            assert_eq!(
                classified.workspace[classified.layout.row_physicals],
                rotation.index() as u32
            );
            let segment = admission.variable_to_segment[target_index];
            assert_eq!(
                classified.workspace[classified.layout.row_segments],
                segment
            );
            assert_eq!(classified.workspace[classified.layout.row_counts], count);
            assert_eq!(
                classified.workspace[classified.layout.row_enum_los],
                witnesses[witness + 2]
            );
            assert_eq!(
                classified.workspace[classified.layout.counts + segment as usize],
                count
            );
            let record = segment as usize * SEGMENT_RECORD_WORDS;
            assert_eq!(classified.segments[record + 1], count);
            assert_eq!(classified.segments[record + 2], target_index as u32);
        }
    }
    assert!(seen.into_iter().all(|seen| seen));
}

#[test]
fn pair_classifier_rejects_bad_tags_limits_and_interval_shapes_before_capacity() {
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
    let round = WgpuResidentRound::new(&gpu, &program, &[variables[0]]).unwrap();
    let admission = lower_proposal_admission(&round, ProposerPolicy::PairGeneric).unwrap();
    let arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == variables[1])
        .unwrap();
    let descriptor = arm * ARM_DESCRIPTOR_WORDS;
    let host = program
        .frontier_from_indices(
            vec![variables[0]],
            vec![program.encode(&raw(fixture.entities[0])).unwrap().get()],
            1,
        )
        .unwrap();
    let frontier = round.upload_frontier(&host).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let witness = arm * PROPOSAL_WITNESS_WORDS;
    let count = witnesses[witness + 3] - witnesses[witness + 2];
    let choice = [variables[1].index() as u32, arm as u32, count];

    let mut bad_descriptors = Vec::new();
    for (word, value) in [
        (0usize, variables[2].index() as u32),
        (1, 9),
        (2, SuccinctRotation::ALL.len() as u32),
        (3, admission.arm_descriptors[descriptor + 3] + 1),
    ] {
        let mut descriptors = admission.arm_descriptors.clone();
        descriptors[descriptor + word] = value;
        bad_descriptors.push(descriptors);
    }
    let mut coherent_unknown_physical = admission.arm_descriptors.clone();
    coherent_unknown_physical[descriptor + 2] = SuccinctRotation::ALL.len() as u32;
    coherent_unknown_physical[descriptor + 3] = RESIDENT_U32_SENTINEL;
    bad_descriptors.push(coherent_unknown_physical);
    for descriptors in bad_descriptors {
        let classified =
            classify_direct(&round, &admission, &descriptors, 1, &choice, &witnesses, 0);
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }

    let (ring_len, pair_counts) = checked_proposal_physical_limits(&round).unwrap();
    let pair_limit = pair_counts[SuccinctRotation::Eav.index()];
    let mut bad_witnesses = Vec::new();
    for replacement in [
        [0, ring_len + 1, 0, count],
        [0, ring_len, 0, pair_limit + 1],
        [1, 2, 2, 2],
        [0, 1, 0, 2],
        [RESIDENT_U32_SENTINEL; PROPOSAL_WITNESS_WORDS],
    ] {
        let mut malformed = witnesses.clone();
        malformed[witness..witness + PROPOSAL_WITNESS_WORDS].copy_from_slice(&replacement);
        bad_witnesses.push(malformed);
    }
    for malformed in bad_witnesses {
        let classified = classify_direct(
            &round,
            &admission,
            &admission.arm_descriptors,
            1,
            &choice,
            &malformed,
            0,
        );
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }

    let other_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == variables[2])
        .unwrap();
    let other_witness = other_arm * PROPOSAL_WITNESS_WORDS;
    let mut impossible_unchosen = witnesses.clone();
    impossible_unchosen[other_witness..other_witness + PROPOSAL_WITNESS_WORDS].copy_from_slice(&[
        0,
        ring_len + 1,
        0,
        0,
    ]);
    for selected in [choice, [RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 0]] {
        let classified = classify_direct(
            &round,
            &admission,
            &admission.arm_descriptors,
            1,
            &selected,
            &impossible_unchosen,
            0,
        );
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }
    let other_descriptor = other_arm * ARM_DESCRIPTOR_WORDS;
    let mut impossible_unchosen_descriptor = admission.arm_descriptors.clone();
    impossible_unchosen_descriptor[other_descriptor + 2] = SuccinctRotation::ALL.len() as u32;
    impossible_unchosen_descriptor[other_descriptor + 3] = RESIDENT_U32_SENTINEL;
    for selected in [choice, [RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 0]] {
        let classified = classify_direct(
            &round,
            &admission,
            &impossible_unchosen_descriptor,
            1,
            &selected,
            &witnesses,
            0,
        );
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }

    let mut zero_witnesses = witnesses.clone();
    zero_witnesses[witness..witness + PROPOSAL_WITNESS_WORDS].copy_from_slice(&[0, 0, 0, 0]);
    let zero = classify_direct(
        &round,
        &admission,
        &admission.arm_descriptors,
        1,
        &[variables[1].index() as u32, arm as u32, 0],
        &zero_witnesses,
        0,
    );
    assert_eq!(&zero.control[..2], &[STATUS_OK, 0]);
    assert_eq!(zero.workspace[zero.layout.row_arms], arm as u32);
    assert_eq!(zero.workspace[zero.layout.row_counts], 0);
    let dead = classify_direct(
        &round,
        &admission,
        &admission.arm_descriptors,
        1,
        &[RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 0],
        &zero_witnesses,
        0,
    );
    assert_eq!(&dead.control[..2], &[STATUS_OK, 0]);
    assert_eq!(dead.workspace[dead.layout.row_arms], RESIDENT_U32_SENTINEL);
    assert_eq!(dead.workspace[dead.layout.row_counts], 0);
}

#[test]
fn present_classifier_authenticates_unchosen_physical_witnesses_before_semantics() {
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
    let round = WgpuResidentRound::new(&gpu, &program, &[]).unwrap();
    let admission = lower_present_admission(&round).unwrap();
    let frontier = round.upload_frontier(&ProgramFrontier::seed()).unwrap();
    let inputs = round.initialize_inputs(&frontier).unwrap();
    let witnesses = inputs.read_proposal_witnesses_for_test();
    let selected_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == variables[0])
        .unwrap();
    let unchosen_arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == variables[1])
        .unwrap();
    let selected_witness = selected_arm * PROPOSAL_WITNESS_WORDS;
    let count = witnesses[selected_witness + 3] - witnesses[selected_witness + 2];
    let mut impossible_unchosen = witnesses.clone();
    let unchosen_witness = unchosen_arm * PROPOSAL_WITNESS_WORDS;
    impossible_unchosen[unchosen_witness..unchosen_witness + PROPOSAL_WITNESS_WORDS]
        .copy_from_slice(&[1, 1, 0, 0]);
    for choice in [
        [variables[0].index() as u32, selected_arm as u32, count],
        [RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL, 0],
    ] {
        let classified = classify_direct(
            &round,
            &admission,
            &admission.arm_descriptors,
            0,
            &choice,
            &impossible_unchosen,
            0,
        );
        assert_eq!(
            &classified.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
        );
    }
}

#[test]
fn restricted_admission_is_test_only_and_pair_policy_still_rejects_it() {
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
    let round = WgpuResidentRound::new(&gpu, &program, &variables[..2]).unwrap();
    assert!(matches!(
        lower_proposal_admission(&round, ProposerPolicy::PairGeneric),
        Err(ResidentProposalError::UnsupportedProposer { .. })
    ));
    let admission = lower_proposal_admission(&round, ProposerPolicy::RestrictedNatural).unwrap();
    assert!(admission.admits_restricted());
    assert_eq!(
        admission.restricted_sources,
        [COLUMN_SOURCE, 0, COLUMN_SOURCE, 1]
    );
}

#[test]
fn generic_admission_authenticates_same_target_pair_rotation_independently() {
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
    let round = WgpuResidentRound::new(&gpu, &program, &[variables[2]]).unwrap();
    let arm = round
        .metadata()
        .arms()
        .iter()
        .position(|identity| identity.target_variable() == variables[1])
        .unwrap();
    let ArmSpec::PairDistinct { peer, .. } = round.proposal_arm_specs()[arm] else {
        panic!("attribute arm with bound value must be PairDistinct");
    };
    assert_eq!(
        round.proposal_arm_pair_rotation(arm),
        Some(SuccinctRotation::Vae)
    );
    let (ring_len, pair_counts) = checked_proposal_physical_limits(&round).unwrap();
    assert!(matches!(
        lower_proposal_arm_descriptor(
            &round,
            arm,
            ArmSpec::PairDistinct {
                arm: arm as u32,
                rotation: SuccinctRotation::Eav,
                peer,
            },
            ProposerPolicy::PairGeneric,
            ring_len,
            pair_counts,
        ),
        Err(ResidentProposalError::MalformedPlan)
    ));
}

#[test]
fn shared_destination_gate_rejects_every_structural_route_and_completeness_fault() {
    let (layout, workspace, segments, canonical) = canonical_gate_fixture();
    let valid = gate_direct(
        [STATUS_OK, 3, 1, 1],
        &workspace,
        layout,
        3,
        1,
        4,
        3,
        &segments,
        &canonical,
        &[55; 4],
    );
    assert_eq!(&valid.control[..2], &[STATUS_OK, 3]);
    assert_eq!(valid.logical_len, 3);
    assert_eq!(valid.verdicts, [1, 1, 1, 0]);
    assert_eq!(valid.indirect_marker, 1);
    assert_eq!(valid.failure_indirect_marker, 0);

    // Exact family semantics deliberately stay outside this shared proof:
    // a different in-domain code on the same authenticated route is accepted.
    let mut semantically_wrong_but_structural = canonical.clone();
    semantically_wrong_but_structural[0] = 1;
    let structural_only = gate_direct(
        [STATUS_OK, 3, 1, 1],
        &workspace,
        layout,
        3,
        1,
        4,
        3,
        &segments,
        &semantically_wrong_but_structural,
        &[55; 4],
    );
    assert_eq!(&structural_only.control[..2], &[STATUS_OK, 3]);
    assert_eq!(structural_only.verdicts, [1, 1, 1, 0]);

    let mut faults = Vec::new();
    let mut middle_hole = canonical.clone();
    middle_hole[1] = RESIDENT_U32_SENTINEL;
    faults.push(("middle hole", middle_hole));
    let mut wrong_owner = canonical.clone();
    wrong_owner[4 + 1] = 0;
    faults.push(("wrong owner", wrong_owner));
    let mut wrong_proposer = canonical.clone();
    wrong_proposer[8 + 1] = 7;
    faults.push(("wrong proposer", wrong_proposer));
    let mut out_of_domain = canonical.clone();
    out_of_domain[1] = 3;
    faults.push(("out of domain", out_of_domain));
    let mut live_tail = canonical.clone();
    live_tail[3] = 0;
    faults.push(("non-poison code tail", live_tail));
    let mut live_owner_tail = canonical.clone();
    live_owner_tail[4 + 3] = 0;
    faults.push(("non-poison owner tail", live_owner_tail));
    let mut live_proposer_tail = canonical.clone();
    live_proposer_tail[8 + 3] = 7;
    faults.push(("non-poison proposer tail", live_proposer_tail));
    let mut duplicate_route = canonical.clone();
    duplicate_route[1] = duplicate_route[0];
    duplicate_route[4 + 1] = duplicate_route[4];
    duplicate_route[8 + 1] = duplicate_route[8];
    faults.push(("duplicate route leaves canonical hole", duplicate_route));

    for (name, candidates) in faults {
        let failed = gate_direct(
            [STATUS_OK, 3, 1, 1],
            &workspace,
            layout,
            3,
            1,
            4,
            3,
            &segments,
            &candidates,
            &[55; 4],
        );
        assert_eq!(
            &failed.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "{name}"
        );
        assert_eq!((failed.control[2], failed.control[3]), (0, 1), "{name}");
        assert_eq!(failed.logical_len, 0, "{name}");
        assert_eq!(failed.indirect_marker, 0, "{name}");
        assert_eq!(failed.failure_indirect_marker, 1, "{name}");
        assert!(
            failed
                .segments
                .iter()
                .chain(&failed.candidates)
                .chain(&failed.child)
                .all(|&word| word == RESIDENT_U32_SENTINEL),
            "{name}"
        );
    }

    let mut wrong_segment = workspace.clone();
    wrong_segment[layout.row_segments + 1] = 1;
    let mut wrong_row_count = workspace.clone();
    wrong_row_count[layout.row_counts + 1] = 2;
    let mut wrong_canonical_count = workspace.clone();
    wrong_canonical_count[layout.counts + 1] = 2;
    for (name, malformed_workspace) in [
        ("wrong retained segment", wrong_segment),
        ("wrong retained row count", wrong_row_count),
        ("wrong canonical cell count", wrong_canonical_count),
    ] {
        let failed = gate_direct(
            [STATUS_OK, 3, 1, 1],
            &malformed_workspace,
            layout,
            3,
            1,
            4,
            3,
            &segments,
            &canonical,
            &[55; 4],
        );
        assert_eq!(
            &failed.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "{name}"
        );
        assert_eq!(failed.indirect_marker, 0, "{name}");
    }

    let exact_capacity_candidates = [0, 1, 2, 0, 1, 2, 7, 8, 9];
    let exact_capacity = gate_direct(
        [STATUS_OK, 3, 1, 1],
        &workspace,
        layout,
        3,
        1,
        3,
        3,
        &segments,
        &exact_capacity_candidates,
        &[55; 3],
    );
    assert_eq!(&exact_capacity.control[..2], &[STATUS_OK, 3]);
    assert_eq!(exact_capacity.verdicts, [1, 1, 1]);
    assert_eq!(exact_capacity.indirect_marker, 1);
    assert_eq!(exact_capacity.failure_indirect_marker, 0);
}

#[test]
fn owner_indexed_device_gate_rejects_range_sentinel_and_cell_overflow_faults() {
    let (layout, workspace, segments, canonical) = canonical_gate_fixture();
    let assert_failed = |name: &str,
                         malformed_workspace: &[u32],
                         malformed_layout: WorkspaceLayout,
                         segment_count: usize,
                         malformed_candidates: &[u32]| {
        let failed = gate_direct(
            [STATUS_OK, 3, 1, 1],
            malformed_workspace,
            malformed_layout,
            3,
            segment_count,
            4,
            3,
            &segments,
            malformed_candidates,
            &[55; 4],
        );
        assert_eq!(
            &failed.control[..2],
            &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL],
            "{name}"
        );
        assert_eq!(failed.logical_len, 0, "{name}");
        assert_eq!(failed.indirect_marker, 0, "{name}");
    };

    let mut out_of_range_arm_workspace = workspace.clone();
    out_of_range_arm_workspace[layout.row_arms] = 10;
    let mut out_of_range_arm_candidates = canonical.clone();
    out_of_range_arm_candidates[8] = 10;
    assert_failed(
        "coherent out-of-range arm",
        &out_of_range_arm_workspace,
        layout,
        1,
        &out_of_range_arm_candidates,
    );

    let mut overflowing_cell_workspace = workspace.clone();
    overflowing_cell_workspace[layout.row_segments] = RESIDENT_U32_SENTINEL - 1;
    assert_failed(
        "segment times rows overflows the non-sentinel cell domain",
        &overflowing_cell_workspace,
        layout,
        RESIDENT_U32_SENTINEL as usize,
        &canonical,
    );

    let mut dead_owner_candidates = canonical.clone();
    dead_owner_candidates[4] = RESIDENT_U32_SENTINEL;
    assert_failed("dead owner", &workspace, layout, 1, &dead_owner_candidates);
    let mut out_of_range_segment_workspace = workspace.clone();
    out_of_range_segment_workspace[layout.row_segments] = 1;
    assert_failed(
        "out-of-range retained segment",
        &out_of_range_segment_workspace,
        layout,
        1,
        &canonical,
    );

    let mut malformed_values = Vec::new();
    let mut dead_canonical_count = workspace.clone();
    dead_canonical_count[layout.counts] = RESIDENT_U32_SENTINEL;
    malformed_values.push(("dead canonical count", dead_canonical_count));
    let mut dead_local = workspace.clone();
    dead_local[layout.local_offsets] = RESIDENT_U32_SENTINEL;
    malformed_values.push(("dead local prefix", dead_local));
    let mut dead_block = workspace.clone();
    dead_block[layout.block_offsets] = RESIDENT_U32_SENTINEL;
    malformed_values.push(("dead block prefix", dead_block));
    let mut overflowing_local = workspace.clone();
    overflowing_local[layout.block_offsets] = RESIDENT_U32_SENTINEL - 2;
    overflowing_local[layout.local_offsets] = 2;
    malformed_values.push(("local plus block reaches sentinel", overflowing_local));
    let mut overflowing_count = workspace.clone();
    overflowing_count[layout.block_offsets] = RESIDENT_U32_SENTINEL - 2;
    overflowing_count[layout.local_offsets] = 1;
    malformed_values.push(("count plus start reaches sentinel", overflowing_count));
    for (name, malformed_workspace) in malformed_values {
        assert_failed(name, &malformed_workspace, layout, 1, &canonical);
    }

    let dead = RESIDENT_U32_SENTINEL as usize;
    let mut malformed_layouts = Vec::new();
    let mut sentinel_arm_base = layout;
    sentinel_arm_base.row_arms = dead;
    malformed_layouts.push(("sentinel arm base", sentinel_arm_base));
    let mut sentinel_segment_base = layout;
    sentinel_segment_base.row_segments = dead;
    malformed_layouts.push(("sentinel segment base", sentinel_segment_base));
    let mut sentinel_count_base = layout;
    sentinel_count_base.row_counts = dead;
    malformed_layouts.push(("sentinel retained-count base", sentinel_count_base));
    let mut sentinel_canonical_count_base = layout;
    sentinel_canonical_count_base.counts = dead;
    malformed_layouts.push((
        "sentinel canonical-count base",
        sentinel_canonical_count_base,
    ));
    let mut sentinel_local_base = layout;
    sentinel_local_base.local_offsets = dead;
    malformed_layouts.push(("sentinel local-prefix base", sentinel_local_base));
    let mut sentinel_block_base = layout;
    sentinel_block_base.block_offsets = dead;
    malformed_layouts.push(("sentinel block-prefix base", sentinel_block_base));
    let mut exhausted_row_base = layout;
    exhausted_row_base.row_arms = workspace.len();
    malformed_layouts.push(("row base with no remaining words", exhausted_row_base));

    for (name, malformed_layout) in malformed_layouts {
        assert_failed(name, &workspace, malformed_layout, 1, &canonical);
    }

    let poison_candidates = vec![RESIDENT_U32_SENTINEL; 4 * CANDIDATE_RECORD_FIELDS];
    let zero_rows = gate_direct(
        [STATUS_OK, 0, 99, 99],
        &workspace,
        layout,
        0,
        0,
        4,
        3,
        &segments,
        &poison_candidates,
        &[55; 4],
    );
    assert_eq!(&zero_rows.control[..2], &[STATUS_OK, 0]);
    assert_eq!(zero_rows.verdicts, [0, 0, 0, 0]);
    assert_eq!(zero_rows.indirect_marker, 0);
    assert_eq!(zero_rows.failure_indirect_marker, 0);

    // Isolate the validator from failure cleanup so malformed buffer shapes
    // can prove fail-closed bounds without asking a downstream poison kernel
    // to write the deliberately truncated candidate planes.
    let validate_only = |planning_words: &[u32],
                         candidate_words: &[u32],
                         verdict_base: u32,
                         initial_verdicts: &[u32]| {
        let context = crate::WgpuContext::on_wgpu();
        let workspace = context.upload_u32(&workspace).unwrap();
        let candidates = context.upload_u32(candidate_words).unwrap();
        let planning = context.upload_u32(planning_words).unwrap();
        let mut verdicts = context.upload_u32(initial_verdicts).unwrap();
        let launch = context
            .static_batch_dispatch(4, 4, CubeDim::new_1d(THREADS))
            .unwrap();
        unsafe {
            validate_proposal_destinations::launch_unchecked::<WgpuRuntime>(
                context.client(),
                launch.cube_count(),
                launch.cube_dim(),
                workspace.input_arg(),
                candidates.input_arg(),
                planning.input_arg(),
                verdicts.output_arg(),
                3,
                1,
                4,
                3,
                10,
                layout.row_arms as u32,
                layout.row_segments as u32,
                layout.row_counts as u32,
                layout.counts as u32,
                layout.local_offsets as u32,
                layout.block_offsets as u32,
                verdict_base,
                BLOCK_ITEMS,
                RESIDENT_U32_SENTINEL,
                STATUS_OK,
            );
        }
        verdicts.read()
    };
    assert_eq!(
        validate_only(
            &[STATUS_OK, 3, 1, 1],
            &canonical[..canonical.len() - 1],
            0,
            &[0; 4],
        ),
        [RESIDENT_U32_SENTINEL; 4],
        "truncated candidate-record storage must fail every lane closed"
    );
    assert_eq!(
        validate_only(&[STATUS_OK], &canonical, 0, &[0; 4]),
        [RESIDENT_U32_SENTINEL; 4],
        "truncated planning control must fail every lane closed"
    );
    assert_eq!(
        validate_only(
            &[STATUS_OK, 3, 1, 1],
            &canonical,
            RESIDENT_U32_SENTINEL,
            &[42; 4],
        ),
        [42; 4],
        "sentinel verdict base must form no output address"
    );
}

#[test]
fn destination_gate_preserves_the_closed_upstream_error_lattice() {
    let (layout, workspace, _, _) = canonical_gate_fixture();
    let dead = RESIDENT_U32_SENTINEL;
    let poison_segments = vec![dead; SEGMENT_RECORD_WORDS];
    let poison_candidates = vec![dead; 4 * CANDIDATE_RECORD_FIELDS];
    let run = |planning, candidates: &[u32]| {
        gate_direct(
            planning,
            &workspace,
            layout,
            3,
            1,
            4,
            3,
            &poison_segments,
            candidates,
            &[55; 4],
        )
    };

    let capacity = run([STATUS_CAPACITY, 5, 0, 1], &poison_candidates);
    assert_eq!(&capacity.control[..2], &[STATUS_CAPACITY, 5]);
    assert_eq!(capacity.logical_len, 0);
    assert_eq!(capacity.indirect_marker, 0);
    assert_eq!(capacity.failure_indirect_marker, 1);
    assert!(capacity
        .segments
        .iter()
        .chain(&capacity.candidates)
        .chain(&capacity.child)
        .all(|&word| word == dead));

    let mut polluted = poison_candidates.clone();
    polluted[0] = 0;
    let capacity_pollution = run([STATUS_CAPACITY, 5, 0, 1], &polluted);
    assert_eq!(
        &capacity_pollution.control[..2],
        &[STATUS_DEVICE_INVARIANT, dead]
    );
    let invariant = run([STATUS_DEVICE_INVARIANT, 0, 0, 1], &poison_candidates);
    assert_eq!(&invariant.control[..2], &[STATUS_DEVICE_INVARIANT, dead]);
    let unknown = run([99, dead, 0, 1], &poison_candidates);
    assert_eq!(&unknown.control[..2], &[STATUS_DEVICE_INVARIANT, dead]);
    let geometry = run([STATUS_GEOMETRY, dead, 0, 1], &polluted);
    assert_eq!(&geometry.control[..2], &[STATUS_GEOMETRY, dead]);
    for (name, failed) in [
        ("capacity pollution", &capacity_pollution),
        ("device invariant", &invariant),
        ("unknown status", &unknown),
        ("geometry", &geometry),
    ] {
        assert_eq!(failed.logical_len, 0, "{name}");
        assert_eq!(failed.indirect_marker, 0, "{name}");
        assert_eq!(failed.failure_indirect_marker, 1, "{name}");
        assert!(
            failed
                .segments
                .iter()
                .chain(&failed.candidates)
                .chain(&failed.child)
                .all(|&word| word == dead),
            "{name} leaked semantic bytes"
        );
    }
    for malformed in [[STATUS_CAPACITY, dead, 0, 1], [STATUS_CAPACITY, 4, 0, 1]] {
        let result = run(malformed, &poison_candidates);
        assert_eq!(&result.control[..2], &[STATUS_DEVICE_INVARIANT, dead]);
    }

    let zero = run([STATUS_OK, 0, 0, 1], &poison_candidates);
    assert_eq!(&zero.control[..2], &[STATUS_OK, 0]);
    assert_eq!(zero.failure_indirect_marker, 0);
    let zero_pollution = run([STATUS_OK, 0, 0, 1], &polluted);
    assert_eq!(
        &zero_pollution.control[..2],
        &[STATUS_DEVICE_INVARIANT, dead]
    );
    let ignored_zero_planning_dispatch = run([STATUS_OK, 0, 1, 99], &poison_candidates);
    assert_eq!(
        &ignored_zero_planning_dispatch.control[..2],
        &[STATUS_OK, 0]
    );
    assert_eq!(
        (
            ignored_zero_planning_dispatch.control[2],
            ignored_zero_planning_dispatch.control[3],
        ),
        (0, 1)
    );
    for malformed in [[STATUS_OK, 5, 1, 1], [STATUS_OK, dead, 1, 1]] {
        let result = run(malformed, &poison_candidates);
        assert_eq!(&result.control[..2], &[STATUS_DEVICE_INVARIANT, dead]);
    }

    let (_, _, segments, candidates) = canonical_gate_fixture();
    let ignored_live_planning_dispatch = gate_direct(
        [STATUS_OK, 3, RESIDENT_U32_SENTINEL, RESIDENT_U32_SENTINEL],
        &workspace,
        layout,
        3,
        1,
        4,
        3,
        &segments,
        &candidates,
        &[55; 4],
    );
    assert_eq!(
        &ignored_live_planning_dispatch.control[..2],
        &[STATUS_OK, 3]
    );
    assert_eq!(
        (
            ignored_live_planning_dispatch.control[2],
            ignored_live_planning_dispatch.control[3],
        ),
        (1, 1)
    );
    assert_eq!(ignored_live_planning_dispatch.indirect_marker, 1);
}

#[test]
fn destination_finalizer_rejects_equal_total_with_a_hole_and_live_tail() {
    let context = crate::WgpuContext::on_wgpu();
    let capacity = 4usize;
    let layout = confirmation_workspace_layout(capacity).unwrap();
    let mut words = vec![0; layout.words];
    words[layout.keep..layout.keep + capacity].copy_from_slice(&[1, 0, 1, 1]);
    let mut workspace = context.upload_u32(&words).unwrap();
    let scan = context
        .static_batch_dispatch(layout.block_count, layout.block_count, CubeDim::new_1d(1))
        .unwrap();
    unsafe {
        scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
            context.client(),
            scan.cube_count(),
            scan.cube_dim(),
            workspace.output_arg(),
            capacity as u32,
            layout.block_count as u32,
            layout.keep as u32,
            layout.pending as u32,
            layout.semantic_status as u32,
            layout.local_offsets as u32,
            layout.block_sums as u32,
            layout.block_errors as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_DEVICE_INVARIANT,
        );
    }
    let planning = context.upload_u32(&[STATUS_OK, 3, 1, 1]).unwrap();
    let mut control = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    let dispatch = context
        .batch_dispatch(0, capacity, CubeDim::new_1d(THREADS))
        .unwrap();
    unsafe {
        finalize_proposal_destinations::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            workspace.output_arg(),
            planning.input_arg(),
            control.output_arg(),
            capacity as u32,
            layout.block_count as u32,
            dispatch.max_groups_x(),
            dispatch.max_groups_y(),
            THREADS,
            layout.local_offsets as u32,
            layout.block_sums as u32,
            layout.block_errors as u32,
            layout.block_offsets as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    assert_eq!(
        &control.read()[..2],
        &[STATUS_DEVICE_INVARIANT, RESIDENT_U32_SENTINEL]
    );

    let mut valid_words = vec![0; layout.words];
    valid_words[layout.keep..layout.keep + capacity].copy_from_slice(&[1, 1, 1, 0]);
    let mut valid_workspace = context.upload_u32(&valid_words).unwrap();
    unsafe {
        scan_confirmation_blocks::launch_unchecked::<WgpuRuntime>(
            context.client(),
            scan.cube_count(),
            scan.cube_dim(),
            valid_workspace.output_arg(),
            capacity as u32,
            layout.block_count as u32,
            layout.keep as u32,
            layout.pending as u32,
            layout.semantic_status as u32,
            layout.local_offsets as u32,
            layout.block_sums as u32,
            layout.block_errors as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_DEVICE_INVARIANT,
        );
    }
    let mut malformed_envelope = context.upload_u32(&[STATUS_OK, 0, 0, 1]).unwrap();
    unsafe {
        finalize_proposal_destinations::launch_unchecked::<WgpuRuntime>(
            context.client(),
            CubeCount::new_single(),
            CubeDim::new_single(),
            valid_workspace.output_arg(),
            planning.input_arg(),
            malformed_envelope.output_arg(),
            capacity as u32,
            layout.block_count as u32,
            0,
            dispatch.max_groups_y(),
            THREADS,
            layout.local_offsets as u32,
            layout.block_sums as u32,
            layout.block_errors as u32,
            layout.block_offsets as u32,
            BLOCK_ITEMS,
            RESIDENT_U32_SENTINEL,
            STATUS_OK,
            STATUS_CAPACITY,
            STATUS_DEVICE_INVARIANT,
            STATUS_GEOMETRY,
        );
    }
    assert_eq!(
        &malformed_envelope.read()[..2],
        &[STATUS_GEOMETRY, RESIDENT_U32_SENTINEL]
    );
}

struct ProposalBenchmarkSample {
    candidate_method: cubecl::profile::TimingMethod,
    candidate_seconds: f64,
    destination_gate_method: cubecl::profile::TimingMethod,
    destination_gate_seconds: f64,
    verdict_scan_method: cubecl::profile::TimingMethod,
    verdict_scan_seconds: f64,
    late_cleanup_method: cubecl::profile::TimingMethod,
    late_cleanup_seconds: f64,
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
    let mut destination_gate_seconds = Vec::with_capacity(SAMPLES);
    let mut verdict_scan_seconds = Vec::with_capacity(SAMPLES);
    let mut late_cleanup_seconds = Vec::with_capacity(SAMPLES);
    let mut child_body_seconds = Vec::with_capacity(SAMPLES);
    let mut candidate_method = None;
    let mut destination_gate_method = None;
    let mut verdict_scan_method = None;
    let mut late_cleanup_method = None;
    let mut child_body_method = None;
    for sample in 0..SAMPLES {
        let mut arena = round
            .enqueue_present_proposals_profiled_for_benchmark(&frontier, &choices, total)
            .unwrap();
        let profiles = arena.resolve_stage_profiles();
        assert_eq!(arena.completion_fence(), total as u32);
        candidate_method = Some(profiles.candidate_method);
        destination_gate_method = Some(profiles.destination_gate_method);
        verdict_scan_method = Some(profiles.verdict_scan_method);
        late_cleanup_method = Some(profiles.late_cleanup_method);
        child_body_method = Some(profiles.child_body_method);
        candidate_seconds.push(profiles.candidate_duration.as_secs_f64());
        destination_gate_seconds.push(profiles.destination_gate_duration.as_secs_f64());
        verdict_scan_seconds.push(profiles.verdict_scan_duration.as_secs_f64());
        late_cleanup_seconds.push(profiles.late_cleanup_duration.as_secs_f64());
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
        destination_gate_method: destination_gate_method.unwrap(),
        destination_gate_seconds: median(destination_gate_seconds),
        verdict_scan_method: verdict_scan_method.unwrap(),
        verdict_scan_seconds: median(verdict_scan_seconds),
        late_cleanup_method: late_cleanup_method.unwrap(),
        late_cleanup_seconds: median(late_cleanup_seconds),
        child_body_method: child_body_method.unwrap(),
        child_body_seconds: median(child_body_seconds),
        wall_seconds: median(wall_seconds),
    }
}

#[test]
#[ignore = "manual release-mode GPU benchmark"]
fn benchmark_present_proposal_candidate_geometry() {
    println!(
        "case,rows,width,total,candidate_timing,candidate_us,candidate_mproposal_s,gate_timing,gate_us,gate_mproposal_s,verdict_scan_timing,verdict_scan_us,cleanup_timing,cleanup_us,body_timing,body_us,body_mword_s,wall_us,wall_mproposal_s"
    );
    let mut validated = false;
    let mut run = |label: &str, rows: usize, width: usize, seed: bool| {
        let total = rows * width;
        let sample = measure_present_proposal_case(width, rows, seed, !validated);
        validated = true;
        println!(
            "{label},{rows},{width},{total},{},{:.3},{:.3},{},{:.3},{:.3},{},{:.3},{},{:.3},{},{:.3},{:.3},{:.3},{:.3}",
            sample.candidate_method,
            sample.candidate_seconds * 1e6,
            total as f64 / sample.candidate_seconds / 1e6,
            sample.destination_gate_method,
            sample.destination_gate_seconds * 1e6,
            total as f64 / sample.destination_gate_seconds / 1e6,
            sample.verdict_scan_method,
            sample.verdict_scan_seconds * 1e6,
            sample.late_cleanup_method,
            sample.late_cleanup_seconds * 1e6,
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
