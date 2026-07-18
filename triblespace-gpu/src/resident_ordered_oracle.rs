//! Test-only ordered-segment oracle for the resident query scheduler.
//!
//! The production CPU reference deliberately exposes only affine transitions
//! and a final execution result. Resident tests need to observe the logical
//! segment boundary at every depth without adding provenance to that public
//! API. This module reconstructs the missing witness through singleton row
//! slices, then checks that regrouping those rows reproduces the whole-parent
//! transition byte for byte.

use std::collections::BTreeMap;

use crate::query_program::{
    ArchiveCode, ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryProgramError,
    QueryTerm,
};
use triblespace_core::blob::encodings::succinctarchive::{
    OrderedUniverse, SuccinctArchive, Universe,
};
use triblespace_core::id::{ExclusiveId, Id};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::{InlineEncoding, RawInline};
use triblespace_core::trible::{Trible, TribleSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateWitness {
    pub(crate) parent_row: usize,
    pub(crate) surviving_ordinal: usize,
    pub(crate) code: ArchiveCode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedSegment {
    pub(crate) parent_segment: Option<usize>,
    pub(crate) variable: Option<ProgramVariable>,
    pub(crate) frontier: ProgramFrontier,
    pub(crate) candidates: Vec<CandidateWitness>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedExecution {
    pub(crate) levels: Vec<Vec<OrderedSegment>>,
    pub(crate) terminal: ProgramFrontier,
}

#[derive(Default)]
struct SegmentBuilder {
    values: Vec<ArchiveCode>,
    rows: usize,
    candidates: Vec<CandidateWitness>,
}

/// Reconstructs one parent's exact child segments and row provenance.
///
/// `parent_row_base` lets consecutive slices retain their original row IDs
/// before like-variable children are coalesced again.
pub(crate) fn witnessed_transition<U: Universe>(
    program: &QueryProgram<'_, U>,
    parent_segment: usize,
    parent: &ProgramFrontier,
    parent_row_base: usize,
) -> Result<Vec<OrderedSegment>, QueryProgramError> {
    let whole = program.transition(parent)?;
    let mut builders = BTreeMap::<ProgramVariable, SegmentBuilder>::new();

    for parent_row in 0..parent.len() {
        let singleton = parent.slice(parent_row..parent_row + 1)?;
        let singleton_children = program.transition(&singleton)?;
        assert!(
            singleton_children.len() <= 1,
            "one row selected more than one next variable"
        );

        for child in singleton_children {
            let variable = added_variable(parent, &child);
            let insertion = child
                .variables()
                .binary_search(&variable)
                .expect("the added variable is a child column");
            let builder = builders.entry(variable).or_default();
            builder.values.extend_from_slice(child.values());
            builder.rows += child.len();
            builder
                .candidates
                .extend((0..child.len()).map(|surviving_ordinal| CandidateWitness {
                    parent_row: parent_row_base + parent_row,
                    surviving_ordinal,
                    code: child.row(surviving_ordinal)[insertion],
                }));
        }
    }

    let rebuilt = builders
        .into_iter()
        .map(|(variable, builder)| {
            let variables = child_variables(parent, variable);
            Ok(OrderedSegment {
                parent_segment: Some(parent_segment),
                variable: Some(variable),
                frontier: ProgramFrontier::new(variables, builder.values, builder.rows)?,
                candidates: builder.candidates,
            })
        })
        .collect::<Result<Vec<_>, QueryProgramError>>()?;

    assert_eq!(
        rebuilt.len(),
        whole.len(),
        "singleton regrouping changed the child count"
    );
    for (rebuilt, whole) in rebuilt.iter().zip(&whole) {
        assert_eq!(
            &rebuilt.frontier, whole,
            "singleton regrouping changed a child frontier"
        );
    }
    Ok(rebuilt)
}

/// Exposes every semantic depth while retaining equal-schema sibling segments.
pub(crate) fn ordered_execution<U: Universe>(
    program: &QueryProgram<'_, U>,
) -> Result<OrderedExecution, QueryProgramError> {
    let mut levels = Vec::with_capacity(program.variable_count() + 1);
    levels.push(vec![OrderedSegment {
        parent_segment: None,
        variable: None,
        frontier: ProgramFrontier::seed(),
        candidates: Vec::new(),
    }]);

    for depth in 0..program.variable_count() {
        let mut next = Vec::new();
        for (parent_segment, parent) in levels[depth].iter().enumerate() {
            next.extend(witnessed_transition(
                program,
                parent_segment,
                &parent.frontier,
                0,
            )?);
        }
        levels.push(next);
    }

    let terminal = program.execute()?;
    if program.variable_count() != 0 {
        assert_eq!(
            flatten_complete_level(program, levels.last().expect("the seed level exists"))?,
            terminal,
            "ordered depth traversal diverged from FIFO terminal routing"
        );
    }
    Ok(OrderedExecution { levels, terminal })
}

/// Recombines consecutive pieces of one logical parent by ascending variable.
pub(crate) fn coalesce_like_children(
    parts: impl IntoIterator<Item = Vec<OrderedSegment>>,
) -> Result<Vec<OrderedSegment>, QueryProgramError> {
    type Coalesced = (
        usize,
        Vec<ProgramVariable>,
        Vec<ArchiveCode>,
        usize,
        Vec<CandidateWitness>,
    );

    let mut groups = BTreeMap::<ProgramVariable, Coalesced>::new();
    for part in parts {
        for segment in part {
            let parent_segment = segment
                .parent_segment
                .expect("only expanded child segments can be coalesced");
            let variable = segment
                .variable
                .expect("only expanded child segments can be coalesced");
            let entry = groups.entry(variable).or_insert_with(|| {
                (
                    parent_segment,
                    segment.frontier.variables().to_vec(),
                    Vec::new(),
                    0,
                    Vec::new(),
                )
            });
            assert_eq!(entry.0, parent_segment, "a coalesced slot crossed parents");
            assert_eq!(
                entry.1.as_slice(),
                segment.frontier.variables(),
                "a coalesced slot changed schema"
            );
            entry.2.extend_from_slice(segment.frontier.values());
            entry.3 += segment.frontier.len();
            entry.4.extend(segment.candidates);
        }
    }

    groups
        .into_iter()
        .map(
            |(variable, (parent_segment, variables, values, rows, candidates))| {
                Ok(OrderedSegment {
                    parent_segment: Some(parent_segment),
                    variable: Some(variable),
                    frontier: ProgramFrontier::new(variables, values, rows)?,
                    candidates,
                })
            },
        )
        .collect()
}

fn flatten_complete_level<U: Universe>(
    program: &QueryProgram<'_, U>,
    level: &[OrderedSegment],
) -> Result<ProgramFrontier, QueryProgramError> {
    let variables = (0..program.variable_count())
        .map(|index| ProgramVariable::new(index as u8))
        .collect::<Vec<_>>();
    let mut values = Vec::new();
    let mut rows = 0usize;
    for segment in level {
        assert_eq!(
            segment.frontier.variables(),
            variables,
            "the final depth contained an incomplete schema"
        );
        values.extend_from_slice(segment.frontier.values());
        rows += segment.frontier.len();
    }
    ProgramFrontier::new(variables, values, rows)
}

fn child_variables(parent: &ProgramFrontier, variable: ProgramVariable) -> Vec<ProgramVariable> {
    let insertion = parent
        .variables()
        .partition_point(|&bound| bound < variable);
    let mut variables = parent.variables().to_vec();
    variables.insert(insertion, variable);
    variables
}

fn added_variable(parent: &ProgramFrontier, child: &ProgramFrontier) -> ProgramVariable {
    assert_eq!(
        child.variables().len(),
        parent.variables().len() + 1,
        "a transition did not add exactly one variable"
    );
    let mut added = child
        .variables()
        .iter()
        .copied()
        .filter(|variable| parent.variables().binary_search(variable).is_err());
    let variable = added.next().expect("a child must add one variable");
    assert!(added.next().is_none(), "a child added multiple variables");
    variable
}

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture IDs are non-zero")
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
    let entities = [fixture_id(1), fixture_id(2), fixture_id(3)];
    let attributes = [fixture_id(11), fixture_id(12)];
    let values = [fixture_id(21), fixture_id(22), fixture_id(23)];
    let mut set = TribleSet::new();

    insert(&mut set, entities[0], attributes[0], values[0]);
    insert(&mut set, entities[0], attributes[0], values[1]);
    insert(&mut set, entities[0], attributes[1], values[2]);
    insert(&mut set, entities[1], attributes[0], values[1]);
    insert(&mut set, entities[1], attributes[1], values[0]);
    insert(&mut set, entities[2], attributes[1], values[2]);
    insert(&mut set, values[0], attributes[1], values[2]);
    insert(&mut set, values[1], attributes[1], values[2]);
    insert(&mut set, values[2], attributes[1], values[0]);

    Fixture {
        set,
        entities,
        attributes,
        values,
    }
}

fn decoded<U: Universe>(
    program: &QueryProgram<'_, U>,
    frontier: &ProgramFrontier,
) -> Vec<Vec<RawInline>> {
    program.decode_frontier(frontier).unwrap()
}

fn expected_rows(rows: &[Vec<Id>]) -> Vec<Vec<RawInline>> {
    rows.iter()
        .map(|row| row.iter().copied().map(raw).collect())
        .collect()
}

fn assert_segment<U: Universe>(
    program: &QueryProgram<'_, U>,
    segment: &OrderedSegment,
    parent_segment: usize,
    variable: ProgramVariable,
    rows: &[Vec<Id>],
    witnesses: &[(usize, usize, Id)],
) {
    assert_eq!(segment.parent_segment, Some(parent_segment));
    assert_eq!(segment.variable, Some(variable));
    assert_eq!(decoded(program, &segment.frontier), expected_rows(rows));
    assert_eq!(
        segment.candidates,
        witnesses
            .iter()
            .map(|&(parent_row, surviving_ordinal, id)| CandidateWitness {
                parent_row,
                surviving_ordinal,
                code: program.encode(&raw(id)).unwrap(),
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn ordered_depths_preserve_parent_variable_row_candidate_order_and_duplicate_schemas() {
    let Fixture {
        set,
        entities: [e0, e1, e2],
        attributes: [a0, a1],
        values: [v0, v1, v2],
    } = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(&archive, 3, [QueryPattern::new(entity, attribute, value)]).unwrap();
    let execution = ordered_execution(&program).unwrap();

    assert_eq!(
        execution.levels.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 1, 2, 2]
    );
    assert_eq!(execution.levels[0][0].parent_segment, None);
    assert_eq!(execution.levels[0][0].variable, None);
    assert_eq!(execution.levels[0][0].frontier, ProgramFrontier::seed());

    assert_segment(
        &program,
        &execution.levels[1][0],
        0,
        attribute,
        &[vec![a0], vec![a1]],
        &[(0, 0, a0), (0, 1, a1)],
    );
    assert_segment(
        &program,
        &execution.levels[2][0],
        0,
        entity,
        &[vec![e0, a0], vec![e1, a0]],
        &[(0, 0, e0), (0, 1, e1)],
    );
    assert_segment(
        &program,
        &execution.levels[2][1],
        0,
        value,
        &[vec![a1, v0], vec![a1, v2]],
        &[(1, 0, v0), (1, 1, v2)],
    );
    assert_segment(
        &program,
        &execution.levels[3][0],
        0,
        value,
        &[vec![e0, a0, v0], vec![e0, a0, v1], vec![e1, a0, v1]],
        &[(0, 0, v0), (0, 1, v1), (1, 0, v1)],
    );
    assert_segment(
        &program,
        &execution.levels[3][1],
        1,
        entity,
        &[
            vec![e1, a1, v0],
            vec![v2, a1, v0],
            vec![e0, a1, v2],
            vec![e2, a1, v2],
            vec![v0, a1, v2],
            vec![v1, a1, v2],
        ],
        &[
            (0, 0, e1),
            (0, 1, v2),
            (1, 0, e0),
            (1, 1, e2),
            (1, 2, v0),
            (1, 3, v1),
        ],
    );

    let first_terminal = &execution.levels[3][0].frontier;
    let second_terminal = &execution.levels[3][1].frontier;
    assert_eq!(first_terminal.variables(), second_terminal.variables());
    assert_ne!(first_terminal.values(), second_terminal.values());
    assert_eq!(
        decoded(&program, &execution.terminal),
        expected_rows(&[
            vec![e0, a0, v0],
            vec![e0, a0, v1],
            vec![e1, a0, v1],
            vec![e1, a1, v0],
            vec![v2, a1, v0],
            vec![e0, a1, v2],
            vec![e2, a1, v2],
            vec![v0, a1, v2],
            vec![v1, a1, v2],
        ])
    );
}

#[test]
fn singleton_reconstruction_and_every_consecutive_split_preserve_mixed_groups() {
    let Fixture {
        set,
        attributes: [a0, a1],
        ..
    } = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(&archive, 3, [QueryPattern::new(entity, attribute, value)]).unwrap();
    let parent_values = [a1, a0, a1, a0]
        .into_iter()
        .map(|id| program.encode(&raw(id)).unwrap())
        .collect();
    let parent = ProgramFrontier::new(vec![attribute], parent_values, 4).unwrap();
    let whole = witnessed_transition(&program, 7, &parent, 0).unwrap();

    assert_eq!(
        whole
            .iter()
            .map(|segment| segment.variable.unwrap())
            .collect::<Vec<_>>(),
        vec![entity, value]
    );
    for split in 0..=parent.len() {
        let left = witnessed_transition(&program, 7, &parent.slice(0..split).unwrap(), 0).unwrap();
        let right = witnessed_transition(
            &program,
            7,
            &parent.slice(split..parent.len()).unwrap(),
            split,
        )
        .unwrap();
        let regrouped = coalesce_like_children([left, right]).unwrap();
        assert_eq!(regrouped, whole, "consecutive split at row {split}");
    }
}

#[test]
fn terminal_routing_handles_zero_variables_and_missing_constant_empty_rounds() {
    let Fixture {
        set,
        entities: [e0, _, e2],
        attributes: [a0, _],
        values: [v0, _, _],
    } = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let present = QueryProgram::compile(
        &archive,
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(e0)),
            QueryTerm::Constant(raw(a0)),
            QueryTerm::Constant(raw(v0)),
        )],
    )
    .unwrap();
    let absent = QueryProgram::compile(
        &archive,
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(e2)),
            QueryTerm::Constant(raw(a0)),
            QueryTerm::Constant(raw(v0)),
        )],
    )
    .unwrap();

    let present = ordered_execution(&present).unwrap();
    let absent = ordered_execution(&absent).unwrap();
    assert_eq!(present.levels.len(), 1);
    assert_eq!(absent.levels.len(), 1);
    assert_eq!(present.levels[0][0].frontier, ProgramFrontier::seed());
    assert_eq!(absent.levels[0][0].frontier, ProgramFrontier::seed());
    assert_eq!(present.terminal.len(), 1);
    assert_eq!(absent.terminal.len(), 0);
    assert!(present.terminal.variables().is_empty());
    assert!(absent.terminal.variables().is_empty());

    let x = ProgramVariable::new(0);
    let y = ProgramVariable::new(1);
    let missing = fixture_id(99);
    let dead = QueryProgram::compile(
        &archive,
        2,
        [QueryPattern::new(x, QueryTerm::Constant(raw(missing)), y)],
    )
    .unwrap();
    let dead = ordered_execution(&dead).unwrap();
    assert_eq!(
        dead.levels.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 0, 0]
    );
    assert_eq!(dead.terminal.variables(), &[x, y]);
    assert!(dead.terminal.is_empty());
}

#[test]
fn sibling_confirmation_retains_original_proposer_order_with_a_gap() {
    let e0 = fixture_id(1);
    let e1 = fixture_id(2);
    let a0 = fixture_id(11);
    let a1 = fixture_id(12);
    let values = [
        fixture_id(21),
        fixture_id(22),
        fixture_id(23),
        fixture_id(24),
    ];
    let mut set = TribleSet::new();
    for &value in &values[..3] {
        insert(&mut set, e0, a0, value);
    }
    for &value in &[values[0], values[2], values[3]] {
        insert(&mut set, e1, a1, value);
    }
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let variable = ProgramVariable::new(0);
    let program = QueryProgram::compile(
        &archive,
        1,
        [
            QueryPattern::new(
                QueryTerm::Constant(raw(e0)),
                QueryTerm::Constant(raw(a0)),
                variable,
            ),
            QueryPattern::new(
                QueryTerm::Constant(raw(e1)),
                QueryTerm::Constant(raw(a1)),
                variable,
            ),
        ],
    )
    .unwrap();
    let execution = ordered_execution(&program).unwrap();

    assert_eq!(
        execution.levels.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert_segment(
        &program,
        &execution.levels[1][0],
        0,
        variable,
        &[vec![values[0]], vec![values[2]]],
        &[(0, 0, values[0]), (0, 1, values[2])],
    );

    // Both source patterns have width three, so the lower source index is the
    // proposer. Confirmation removes its middle value but cannot reorder the
    // two survivors: their original proposer ordinals remain 0 and 2.
    let proposer = values[..3]
        .iter()
        .map(|&id| program.encode(&raw(id)).unwrap())
        .collect::<Vec<_>>();
    let original_ordinals = execution.levels[1][0]
        .candidates
        .iter()
        .map(|witness| {
            proposer
                .iter()
                .position(|&code| code == witness.code)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(original_ordinals, vec![0, 2]);
}
