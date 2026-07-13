use std::collections::BTreeMap;

use triblespace::core::blob::encodings::succinctarchive::query_program::{
    ArchiveCode, ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryTerm,
};
use triblespace::core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::inline::RawInline;
use triblespace::core::query::{
    CandidateSink, Candidates, Constraint, Query, RowsView, Term, VariableContext,
};
use triblespace::prelude::*;

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture ids are non-zero")
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

fn fixture() -> (TribleSet, [Id; 3], [Id; 2], [Id; 3]) {
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

    // Value ids also act as entities in the second half of a join.
    insert(&mut set, values[0], attributes[1], values[2]);
    insert(&mut set, values[1], attributes[1], values[2]);
    insert(&mut set, values[2], attributes[1], values[0]);

    (set, entities, attributes, values)
}

fn sorted(mut rows: Vec<Vec<RawInline>>) -> Vec<Vec<RawInline>> {
    rows.sort_unstable();
    rows
}

fn program_rows<U: triblespace::core::blob::encodings::succinctarchive::Universe>(
    program: &QueryProgram<'_, U>,
) -> Vec<Vec<RawInline>> {
    let frontier = program.execute().expect("program executes");
    program
        .decode_frontier(&frontier)
        .expect("program output decodes")
}

#[test]
fn single_pattern_matches_canonical_dag_through_zero_one_and_two_bound_arms() {
    let (set, _, _, _) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(&archive, 3, [QueryPattern::new(entity, attribute, value)]).unwrap();

    // One transition per level forces the same pattern through its zero-,
    // one-, and two-bound proposal arms before full rows appear.
    let level_one = program.transition(&ProgramFrontier::seed()).unwrap();
    assert!(!level_one.is_empty());
    assert!(level_one
        .iter()
        .all(|frontier| frontier.variables().len() == 1));
    let level_two: Vec<_> = level_one
        .iter()
        .flat_map(|frontier| program.transition(frontier).unwrap())
        .collect();
    assert!(!level_two.is_empty());
    assert!(level_two
        .iter()
        .all(|frontier| frontier.variables().len() == 2));
    let level_three: Vec<_> = level_two
        .iter()
        .flat_map(|frontier| program.transition(frontier).unwrap())
        .collect();
    assert!(!level_three.is_empty());
    assert!(level_three
        .iter()
        .all(|frontier| frontier.variables().len() == 3));

    let canonical = find!(
        (e: Inline<GenId>, a: Inline<GenId>, v: Inline<GenId>),
        archive.pattern(e, a, v)
    )
    .solve_dag()
    .into_iter()
    .map(|(e, a, v)| vec![e.raw, a.raw, v.raw])
    .collect();

    assert_eq!(sorted(program_rows(&program)), sorted(canonical));
}

#[test]
fn flat_conjunction_with_constants_matches_canonical_dag_multiset() {
    let (set, _, attributes, _) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = ProgramVariable::new(0);
    let middle = ProgramVariable::new(1);
    let tail = ProgramVariable::new(2);
    let program = QueryProgram::compile(
        &archive,
        3,
        [
            QueryPattern::new(entity, QueryTerm::Constant(raw(attributes[0])), middle),
            QueryPattern::new(middle, QueryTerm::Constant(raw(attributes[1])), tail),
        ],
    )
    .unwrap();

    let canonical = find!(
        (e: Inline<GenId>, m: Inline<GenId>, t: Inline<GenId>),
        and!(
            archive.pattern(e, attributes[0].to_inline(), m),
            archive.pattern(m, attributes[1].to_inline(), t)
        )
    )
    .solve_dag()
    .into_iter()
    .map(|(e, m, t)| vec![e.raw, m.raw, t.raw])
    .collect();

    assert_eq!(sorted(program_rows(&program)), sorted(canonical));
}

#[test]
fn every_constant_variable_term_shape_matches_the_canonical_dag() {
    let (set, entities, attributes, values) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();

    // Each bit pins E/A/V respectively. Across all eight masks this checks
    // source lowering and exact constant/variable result semantics.
    for constant_mask in 0u8..8 {
        let mut context = VariableContext::new();
        let mut projected = Vec::new();
        let mut next_program_variable = 0u8;

        let e_term = if constant_mask & 1 != 0 {
            Term::Const(GenId::inline_from(entities[0]))
        } else {
            let variable = context.next_variable::<GenId>();
            projected.push(variable);
            Term::Var(variable)
        };
        let e_program = if constant_mask & 1 != 0 {
            QueryTerm::Constant(raw(entities[0]))
        } else {
            let variable = ProgramVariable::new(next_program_variable);
            next_program_variable += 1;
            QueryTerm::Variable(variable)
        };

        let a_term = if constant_mask & 2 != 0 {
            Term::Const(GenId::inline_from(attributes[0]))
        } else {
            let variable = context.next_variable::<GenId>();
            projected.push(variable);
            Term::Var(variable)
        };
        let a_program = if constant_mask & 2 != 0 {
            QueryTerm::Constant(raw(attributes[0]))
        } else {
            let variable = ProgramVariable::new(next_program_variable);
            next_program_variable += 1;
            QueryTerm::Variable(variable)
        };

        let v_term = if constant_mask & 4 != 0 {
            Term::Const(GenId::inline_from(values[0]))
        } else {
            let variable = context.next_variable::<GenId>();
            projected.push(variable);
            Term::Var(variable)
        };
        let v_program = if constant_mask & 4 != 0 {
            QueryTerm::Constant(raw(values[0]))
        } else {
            let variable = ProgramVariable::new(next_program_variable);
            next_program_variable += 1;
            QueryTerm::Variable(variable)
        };

        let program = QueryProgram::compile(
            &archive,
            next_program_variable as usize,
            [QueryPattern::new(e_program, a_program, v_program)],
        )
        .unwrap();
        let canonical = Query::new(archive.pattern(e_term, a_term, v_term), move |binding| {
            Some(
                projected
                    .iter()
                    .map(|variable| *binding.get(variable.index).unwrap())
                    .collect::<Vec<_>>(),
            )
        })
        .solve_dag();

        assert_eq!(
            sorted(program_rows(&program)),
            sorted(canonical),
            "constant mask {constant_mask:03b}"
        );
    }
}

#[test]
fn every_direct_ring_proposal_arm_matches_the_canonical_constraint() {
    let (set, entities, attributes, values) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let program_variables = [
        ProgramVariable::new(0),
        ProgramVariable::new(1),
        ProgramVariable::new(2),
    ];
    let program = QueryProgram::compile(
        &archive,
        3,
        [QueryPattern::new(
            program_variables[0],
            program_variables[1],
            program_variables[2],
        )],
    )
    .unwrap();

    let mut context = VariableContext::new();
    let entity = context.next_variable::<GenId>();
    let attribute = context.next_variable::<GenId>();
    let value = context.next_variable::<GenId>();
    let query_variables = [entity.index, attribute.index, value.index];
    let constraint = archive.pattern(entity, attribute, value);

    let complete_rows = [
        [raw(entities[0]), raw(attributes[0]), raw(values[0])],
        [raw(entities[0]), raw(attributes[0]), raw(values[1])],
        [raw(entities[0]), raw(attributes[1]), raw(values[2])],
        [raw(entities[1]), raw(attributes[0]), raw(values[1])],
        [raw(entities[1]), raw(attributes[1]), raw(values[0])],
        [raw(entities[2]), raw(attributes[1]), raw(values[2])],
        [raw(values[0]), raw(attributes[1]), raw(values[2])],
        [raw(values[1]), raw(attributes[1]), raw(values[2])],
        [raw(values[2]), raw(attributes[1]), raw(values[0])],
    ];

    // For each proposed axis, force all four peer-binding shapes: neither
    // peer, either peer individually, and both peers. This reaches the three
    // zero-bound, six one-bound, and three two-bound direct Ring arms without
    // relying on scheduler choice.
    for bound_mask in 0u8..8 {
        if bound_mask == 0b111 {
            continue;
        }
        let bound_axes: Vec<_> = (0..3)
            .filter(|axis| bound_mask & (1 << axis) != 0)
            .collect();
        let bound_program_variables: Vec<_> = bound_axes
            .iter()
            .map(|&axis| program_variables[axis])
            .collect();
        let bound_query_variables: Vec<_> = bound_axes
            .iter()
            .map(|&axis| query_variables[axis])
            .collect();
        let raw_rows: Vec<_> = if bound_axes.is_empty() {
            Vec::new()
        } else {
            complete_rows
                .iter()
                .flat_map(|row| bound_axes.iter().map(|&axis| row[axis]))
                .collect()
        };
        let row_count = if bound_axes.is_empty() {
            1
        } else {
            complete_rows.len()
        };
        let code_rows = raw_rows
            .iter()
            .map(|value| program.encode(value).unwrap().get())
            .collect();
        let frontier = program
            .frontier_from_indices(bound_program_variables, code_rows, row_count)
            .unwrap();
        let view = RowsView::new(&bound_query_variables, &raw_rows);

        for candidate_axis in 0..3 {
            if bound_mask & (1 << candidate_axis) != 0 {
                continue;
            }

            let child = program
                .transition_on(program_variables[candidate_axis], &frontier)
                .unwrap();
            let actual = program.decode_frontier(&child).unwrap();

            let mut candidates: Candidates = Vec::new();
            constraint.propose(
                query_variables[candidate_axis],
                &view,
                &mut CandidateSink::Tagged(&mut candidates),
            );
            let insertion = bound_axes.partition_point(|&axis| axis < candidate_axis);
            let expected = candidates
                .into_iter()
                .map(|(row_index, candidate)| {
                    let parent = view.row(row_index as usize);
                    let mut row = Vec::with_capacity(parent.len() + 1);
                    row.extend_from_slice(&parent[..insertion]);
                    row.push(candidate);
                    row.extend_from_slice(&parent[insertion..]);
                    row
                })
                .collect();

            assert_eq!(
                sorted(actual),
                sorted(expected),
                "bound mask {bound_mask:03b}, candidate axis {candidate_axis}"
            );
        }
    }

    assert!(program
        .frontier_from_indices(vec![program_variables[0]], vec![u32::MAX], 1)
        .is_err());
}

fn concatenate_children(
    sets: impl IntoIterator<Item = Vec<ProgramFrontier>>,
) -> BTreeMap<Vec<ProgramVariable>, (Vec<ArchiveCode>, usize)> {
    let mut combined = BTreeMap::new();
    for children in sets {
        for child in children {
            let entry = combined
                .entry(child.variables().to_vec())
                .or_insert_with(|| (Vec::new(), 0));
            entry.0.extend_from_slice(child.values());
            entry.1 += child.len();
        }
    }
    combined
}

#[test]
fn frontier_transition_is_a_row_homomorphism() {
    let (set, entities, _, values) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let entity = ProgramVariable::new(0);
    let attribute = ProgramVariable::new(1);
    let value = ProgramVariable::new(2);
    let program =
        QueryProgram::compile(&archive, 3, [QueryPattern::new(entity, attribute, value)]).unwrap();
    let rows = [entities[0], entities[1], entities[2], values[0]]
        .into_iter()
        .map(|id| program.encode(&raw(id)).unwrap())
        .collect();
    let frontier = ProgramFrontier::new(vec![entity], rows, 4).unwrap();

    let whole = concatenate_children([program.transition(&frontier).unwrap()]);
    let split = concatenate_children([
        program.transition(&frontier.slice(0..2).unwrap()).unwrap(),
        program.transition(&frontier.slice(2..4).unwrap()).unwrap(),
    ]);

    assert_eq!(whole, split);
}

#[test]
fn adding_facts_cannot_remove_positive_program_results() {
    let e1 = fixture_id(1);
    let e2 = fixture_id(2);
    let a1 = fixture_id(11);
    let a2 = fixture_id(12);
    let m1 = fixture_id(21);
    let m2 = fixture_id(22);
    let tail = fixture_id(23);

    let mut base = TribleSet::new();
    insert(&mut base, e1, a1, m1);
    insert(&mut base, m1, a2, tail);
    let mut extended = base.clone();
    insert(&mut extended, e2, a1, m2);
    insert(&mut extended, m2, a2, tail);

    let run = |set: &TribleSet| {
        let archive: SuccinctArchive<OrderedUniverse> = set.into();
        let program = QueryProgram::compile(
            &archive,
            3,
            [
                QueryPattern::new(
                    ProgramVariable::new(0),
                    QueryTerm::Constant(raw(a1)),
                    ProgramVariable::new(1),
                ),
                QueryPattern::new(
                    ProgramVariable::new(1),
                    QueryTerm::Constant(raw(a2)),
                    ProgramVariable::new(2),
                ),
            ],
        )
        .unwrap();
        sorted(program_rows(&program))
    };

    let before = run(&base);
    let after = run(&extended);
    assert!(before.iter().all(|row| after.binary_search(row).is_ok()));
    assert!(after.len() > before.len());
}

#[test]
fn constant_only_patterns_are_exact_at_the_seed() {
    let (set, entities, attributes, values) = fixture();
    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let present = QueryProgram::compile(
        &archive,
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(entities[0])),
            QueryTerm::Constant(raw(attributes[0])),
            QueryTerm::Constant(raw(values[0])),
        )],
    )
    .unwrap();
    let absent = QueryProgram::compile(
        &archive,
        0,
        [QueryPattern::new(
            QueryTerm::Constant(raw(entities[2])),
            QueryTerm::Constant(raw(attributes[0])),
            QueryTerm::Constant(raw(values[0])),
        )],
    )
    .unwrap();

    assert_eq!(present.execute().unwrap().len(), 1);
    assert_eq!(absent.execute().unwrap().len(), 0);
}
