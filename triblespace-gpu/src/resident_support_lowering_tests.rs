use super::*;
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

struct Fixture {
    archive: SuccinctArchive<OrderedUniverse>,
    entities: [Id; 5],
    attributes: [Id; 2],
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
        attributes,
        values,
    }
}

fn v(index: u8) -> ProgramVariable {
    ProgramVariable::new(index)
}

fn constant(id: Id) -> QueryTerm {
    QueryTerm::Constant(GenId::inline_from(id).raw)
}

fn code(program: &QueryProgram<'_, OrderedUniverse>, id: Id) -> CodeSource {
    CodeSource::Constant(program.encode(&GenId::inline_from(id).raw).unwrap().get())
}

#[test]
fn all_twelve_target_peer_states_lower_exactly() {
    let fixture = fixture();
    let program =
        QueryProgram::compile(&fixture.archive, 3, [QueryPattern::new(v(0), v(1), v(2))]).unwrap();

    let plan = ResidentRoundPlan::lower(&program, &[]).unwrap();
    assert_eq!(
        plan.arm_specs(),
        &[
            ArmSpec::Present {
                arm: 0,
                count: fixture.archive.entity_count as u32,
            },
            ArmSpec::Present {
                arm: 1,
                count: fixture.archive.attribute_count as u32,
            },
            ArmSpec::Present {
                arm: 2,
                count: fixture.archive.value_count as u32,
            },
        ]
    );
    assert_eq!(plan.arm_groups().len(), 1);
    assert_eq!(plan.arm_groups()[0].kind(), ArmGroupKind::Present);
    assert_eq!(plan.arm_groups()[0].arm_ids(), &[0, 1, 2]);

    for (bound, expected) in [
        (
            v(0),
            [
                ArmSpec::PairDistinct {
                    arm: 0,
                    rotation: SuccinctRotation::Eav,
                    peer: CodeSource::Column(0),
                },
                ArmSpec::PairDistinct {
                    arm: 1,
                    rotation: SuccinctRotation::Eva,
                    peer: CodeSource::Column(0),
                },
            ],
        ),
        (
            v(1),
            [
                ArmSpec::PairDistinct {
                    arm: 0,
                    rotation: SuccinctRotation::Aev,
                    peer: CodeSource::Column(0),
                },
                ArmSpec::PairDistinct {
                    arm: 1,
                    rotation: SuccinctRotation::Ave,
                    peer: CodeSource::Column(0),
                },
            ],
        ),
        (
            v(2),
            [
                ArmSpec::PairDistinct {
                    arm: 0,
                    rotation: SuccinctRotation::Vea,
                    peer: CodeSource::Column(0),
                },
                ArmSpec::PairDistinct {
                    arm: 1,
                    rotation: SuccinctRotation::Vae,
                    peer: CodeSource::Column(0),
                },
            ],
        ),
    ] {
        assert_eq!(
            ResidentRoundPlan::lower(&program, &[bound])
                .unwrap()
                .arm_specs(),
            &expected
        );
    }

    for (bound, expected) in [
        (
            [v(0), v(1)],
            ArmSpec::Restricted {
                arm: 0,
                rotation: SuccinctRotation::Eva,
                first: CodeSource::Column(0),
                last: CodeSource::Column(1),
            },
        ),
        (
            [v(0), v(2)],
            ArmSpec::Restricted {
                arm: 0,
                rotation: SuccinctRotation::Eav,
                first: CodeSource::Column(0),
                last: CodeSource::Column(1),
            },
        ),
        (
            [v(1), v(2)],
            ArmSpec::Restricted {
                arm: 0,
                rotation: SuccinctRotation::Aev,
                first: CodeSource::Column(0),
                last: CodeSource::Column(1),
            },
        ),
    ] {
        assert_eq!(
            ResidentRoundPlan::lower(&program, &bound)
                .unwrap()
                .arm_specs(),
            &[expected]
        );
    }
}

#[test]
fn constants_use_the_same_one_and_two_peer_mappings() {
    let fixture = fixture();
    let entity = fixture.entities[0];
    let attribute = fixture.attributes[0];
    let value = fixture.values[0];
    let two_peer = QueryProgram::compile(
        &fixture.archive,
        3,
        [
            QueryPattern::new(v(0), constant(attribute), constant(value)),
            QueryPattern::new(constant(entity), v(1), constant(value)),
            QueryPattern::new(constant(entity), constant(attribute), v(2)),
        ],
    )
    .unwrap();
    assert_eq!(
        ResidentRoundPlan::lower(&two_peer, &[])
            .unwrap()
            .arm_specs(),
        &[
            ArmSpec::Restricted {
                arm: 0,
                rotation: SuccinctRotation::Aev,
                first: code(&two_peer, attribute),
                last: code(&two_peer, value),
            },
            ArmSpec::Restricted {
                arm: 1,
                rotation: SuccinctRotation::Eav,
                first: code(&two_peer, entity),
                last: code(&two_peer, value),
            },
            ArmSpec::Restricted {
                arm: 2,
                rotation: SuccinctRotation::Eva,
                first: code(&two_peer, entity),
                last: code(&two_peer, attribute),
            },
        ]
    );

    for (pattern, rotations, peer) in [
        (
            QueryPattern::new(v(0), constant(attribute), v(1)),
            [SuccinctRotation::Aev, SuccinctRotation::Ave],
            attribute,
        ),
        (
            QueryPattern::new(v(0), v(1), constant(value)),
            [SuccinctRotation::Vea, SuccinctRotation::Vae],
            value,
        ),
        (
            QueryPattern::new(constant(entity), v(0), v(1)),
            [SuccinctRotation::Eav, SuccinctRotation::Eva],
            entity,
        ),
    ] {
        let program = QueryProgram::compile(&fixture.archive, 2, [pattern]).unwrap();
        let source = code(&program, peer);
        assert_eq!(
            ResidentRoundPlan::lower(&program, &[]).unwrap().arm_specs(),
            &[
                ArmSpec::PairDistinct {
                    arm: 0,
                    rotation: rotations[0],
                    peer: source,
                },
                ArmSpec::PairDistinct {
                    arm: 1,
                    rotation: rotations[1],
                    peer: source,
                },
            ]
        );
    }

    let constant_first = QueryProgram::compile(
        &fixture.archive,
        2,
        [QueryPattern::new(v(0), constant(attribute), v(1))],
    )
    .unwrap();
    assert_eq!(
        ResidentRoundPlan::lower(&constant_first, &[v(1)])
            .unwrap()
            .arm_specs(),
        &[ArmSpec::Restricted {
            arm: 0,
            rotation: SuccinctRotation::Aev,
            first: code(&constant_first, attribute),
            last: CodeSource::Column(0),
        }]
    );

    let constant_last = QueryProgram::compile(
        &fixture.archive,
        2,
        [QueryPattern::new(v(0), v(1), constant(value))],
    )
    .unwrap();
    assert_eq!(
        ResidentRoundPlan::lower(&constant_last, &[v(0)])
            .unwrap()
            .arm_specs(),
        &[ArmSpec::Restricted {
            arm: 0,
            rotation: SuccinctRotation::Eav,
            first: CodeSource::Column(0),
            last: code(&constant_last, value),
        }]
    );
}

#[test]
fn fully_bound_supports_cover_columns_constants_nonfacts_and_partial_patterns() {
    let fixture = fixture();
    let entity = fixture.entities[0];
    let attribute = fixture.attributes[0];
    let value = fixture.values[0];
    let program = QueryProgram::compile(
        &fixture.archive,
        3,
        [
            QueryPattern::new(v(0), v(1), v(2)),
            QueryPattern::new(v(0), constant(attribute), v(1)),
            QueryPattern::new(constant(entity), v(2), constant(value)),
            QueryPattern::new(constant(entity), constant(attribute), constant(value)),
            QueryPattern::new(
                constant(fixture.entities[0]),
                constant(fixture.attributes[1]),
                constant(fixture.values[2]),
            ),
        ],
    )
    .unwrap();
    let partial = ResidentRoundPlan::lower(&program, &[v(0), v(1)]).unwrap();
    assert_eq!(
        partial
            .fully_bound_supports()
            .iter()
            .map(|support| support.source_pattern_index())
            .collect::<Vec<_>>(),
        vec![1, 3, 4]
    );

    let plan = ResidentRoundPlan::lower(&program, &[v(0), v(1), v(2)]).unwrap();
    assert!(plan.arm_specs().is_empty());
    assert!(plan.arm_groups().is_empty());
    let supports = plan
        .fully_bound_supports()
        .iter()
        .copied()
        .map(|support| {
            (
                support.source_pattern_index(),
                support.entity(),
                support.attribute(),
                support.value(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        supports,
        vec![
            (
                0,
                CodeSource::Column(0),
                CodeSource::Column(1),
                CodeSource::Column(2),
            ),
            (
                1,
                CodeSource::Column(0),
                code(&program, attribute),
                CodeSource::Column(1),
            ),
            (
                2,
                code(&program, entity),
                CodeSource::Column(2),
                code(&program, value),
            ),
            (
                3,
                code(&program, entity),
                code(&program, attribute),
                code(&program, value),
            ),
            (
                4,
                code(&program, fixture.entities[0]),
                code(&program, fixture.attributes[1]),
                code(&program, fixture.values[2]),
            ),
        ]
    );
}

#[test]
fn grouping_preserves_semantic_arm_ids_and_duplicate_patterns() {
    let fixture = fixture();
    let program = QueryProgram::compile(
        &fixture.archive,
        6,
        [
            QueryPattern::new(v(2), v(3), v(4)),
            QueryPattern::new(v(0), v(5), v(3)),
            QueryPattern::new(v(2), v(1), v(0)),
        ],
    )
    .unwrap();
    let plan = ResidentRoundPlan::lower(&program, &[v(0), v(1)]).unwrap();
    assert_eq!(
        plan.metadata()
            .arms()
            .iter()
            .map(|arm| (arm.source_pattern_index(), arm.target_variable().index()))
            .collect::<Vec<_>>(),
        vec![(0, 2), (0, 3), (0, 4), (1, 3), (1, 5), (2, 2)]
    );
    assert_eq!(
        plan.arm_specs()
            .iter()
            .map(|spec| spec.arm())
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        plan.arm_groups()
            .iter()
            .map(|group| (group.kind(), group.arm_ids().to_vec()))
            .collect::<Vec<_>>(),
        vec![
            (ArmGroupKind::Present, vec![0, 1, 2]),
            (ArmGroupKind::PairDistinct(SuccinctRotation::Eav), vec![4]),
            (ArmGroupKind::PairDistinct(SuccinctRotation::Eva), vec![3]),
            (ArmGroupKind::Restricted(SuccinctRotation::Aev), vec![5]),
        ]
    );

    let duplicate = QueryProgram::compile(
        &fixture.archive,
        2,
        [
            QueryPattern::new(v(0), constant(fixture.attributes[0]), v(1)),
            QueryPattern::new(v(0), constant(fixture.attributes[0]), v(1)),
        ],
    )
    .unwrap();
    let duplicate = ResidentRoundPlan::lower(&duplicate, &[]).unwrap();
    assert_eq!(
        duplicate
            .arm_groups()
            .iter()
            .map(|group| (group.kind(), group.arm_ids().to_vec()))
            .collect::<Vec<_>>(),
        vec![
            (
                ArmGroupKind::PairDistinct(SuccinctRotation::Ave),
                vec![1, 3]
            ),
            (
                ArmGroupKind::PairDistinct(SuccinctRotation::Aev),
                vec![0, 2]
            ),
        ]
    );
}

#[test]
fn missing_constant_kills_the_whole_plan_even_without_an_arm() {
    let fixture = fixture();
    let program = QueryProgram::compile(
        &fixture.archive,
        3,
        [
            QueryPattern::new(v(0), v(1), v(2)),
            QueryPattern::new(
                constant(fixture.entities[0]),
                constant(ordered_id(250)),
                constant(fixture.values[0]),
            ),
        ],
    )
    .unwrap();
    let plan = ResidentRoundPlan::lower(&program, &[]).unwrap();
    assert!(plan.is_global_dead());
    assert_eq!(plan.metadata().arms().len(), 3);
    assert!(plan.arm_specs().is_empty());
    assert!(plan.arm_groups().is_empty());
    assert!(plan.fully_bound_supports().is_empty());
}
