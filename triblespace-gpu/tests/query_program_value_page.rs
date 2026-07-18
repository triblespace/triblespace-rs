use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::RawInline;
use triblespace_core::prelude::*;
use triblespace_gpu::query_program::{
    ProgramFrontier, ProgramVariable, QueryPattern, QueryProgram, QueryProgramError, QueryTerm,
};

fn fixture_id(prefix: u8, ordinal: usize) -> Id {
    let mut raw = [0u8; 16];
    raw[0] = prefix;
    raw[8..].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
    Id::new(raw).expect("fixture id is non-zero")
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

fn fixture() -> (SuccinctArchive<OrderedUniverse>, [Id; 5], [Id; 2], [Id; 6]) {
    let entities = std::array::from_fn(|i| fixture_id(1, i));
    let attributes = std::array::from_fn(|i| fixture_id(2, i));
    let values = std::array::from_fn(|i| fixture_id(3, i));
    let mut set = TribleSet::new();

    // Attribute zero deliberately spans empty, singleton, and longer ranges.
    for (entity, fanout) in entities.iter().zip([0, 1, 3, 6, 2]) {
        for &value in values.iter().take(fanout) {
            insert(&mut set, *entity, attributes[0], value);
        }
    }
    // Keep every fixture family in the archive domain and provide another
    // real axis against which wrong-peer rows can navigate to empty ranges.
    for (index, &entity) in entities.iter().enumerate() {
        insert(
            &mut set,
            entity,
            attributes[1],
            values[(index + 1) % values.len()],
        );
    }

    ((&set).into(), entities, attributes, values)
}

fn frontier(
    program: &QueryProgram<'_, OrderedUniverse>,
    variables: Vec<ProgramVariable>,
    rows: impl IntoIterator<Item = Vec<Id>>,
) -> ProgramFrontier {
    let rows: Vec<_> = rows.into_iter().collect();
    let values = rows
        .iter()
        .flat_map(|row| {
            row.iter().map(|id| {
                program
                    .encode(&raw(*id))
                    .expect("fixture value is in the archive domain")
            })
        })
        .collect();
    ProgramFrontier::new(variables, values, rows.len()).unwrap()
}

fn assert_every_slice_matches_transition(
    program: &QueryProgram<'_, OrderedUniverse>,
    target: ProgramVariable,
    parent: &ProgramFrontier,
) {
    for input in 0..parent.len() {
        let singleton =
            ProgramFrontier::new(parent.variables().to_vec(), parent.row(input).to_vec(), 1)
                .unwrap();
        let full = program.transition_on(target, &singleton).unwrap();
        for offset in 0..=full.len() {
            for limit in 1..=full.len() + 2 {
                let page = program
                    .transition_on_two_bound_page(target, &singleton, &[offset], &[limit])
                    .unwrap()
                    .expect("fixture uses the admitted one-pattern value arm");
                let examined = limit.min(full.len() - offset);
                assert_eq!(
                    page.child(),
                    &full.slice(offset..offset + examined).unwrap(),
                    "input {input}, offset {offset}, limit {limit}"
                );
                assert_eq!(page.receipts().len(), 1);
                assert_eq!(page.receipts()[0].examined(), examined);
                assert_eq!(
                    page.receipts()[0].next_offset(),
                    (offset + examined < full.len()).then_some(offset + examined)
                );
            }
        }
    }
}

fn assert_batched_pages_reconstruct_transition(
    program: &QueryProgram<'_, OrderedUniverse>,
    target: ProgramVariable,
    parent: &ProgramFrontier,
) {
    let expected = program.transition_on(target, parent).unwrap();
    let child_variables = expected.variables().to_vec();
    let mut offsets = vec![0usize; parent.len()];
    let mut per_input = vec![Vec::new(); parent.len()];
    let mut round = 0usize;

    loop {
        let limits: Vec<_> = (0..parent.len())
            .map(|input| 1 + (round + input * 2) % 4)
            .collect();
        let page = program
            .transition_on_two_bound_page(target, parent, &offsets, &limits)
            .unwrap()
            .expect("fixture uses the admitted one-pattern value arm");
        let mut child_row = 0usize;
        let mut has_resume = false;
        for (input, receipt) in page.receipts().iter().copied().enumerate() {
            for _ in 0..receipt.examined() {
                per_input[input].extend_from_slice(page.child().row(child_row));
                child_row += 1;
            }
            offsets[input] += receipt.examined();
            if let Some(next) = receipt.next_offset() {
                assert_eq!(next, offsets[input]);
                has_resume = true;
            }
        }
        assert_eq!(child_row, page.child().len());
        if !has_resume {
            break;
        }
        round += 1;
    }

    let values = per_input.into_iter().flatten().collect::<Vec<_>>();
    let actual = ProgramFrontier::new(child_variables, values, expected.len()).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn variable_peer_pages_are_exact_for_all_axes_schemas_and_target_columns() {
    let (archive, entities, attributes, values) = fixture();
    let peer_rows = [
        // (A,V) -> E, including a duplicate and a domain-valid empty pair.
        vec![
            [entities[0], attributes[0], values[0]],
            [entities[0], attributes[0], values[5]],
            [entities[0], attributes[0], values[0]],
            [entities[0], attributes[1], values[0]],
        ],
        // (E,V) -> A, including a two-attribute pair and an empty pair.
        vec![
            [entities[3], attributes[0], values[4]],
            [entities[3], attributes[0], values[0]],
            [entities[3], attributes[0], values[4]],
            [entities[0], attributes[0], values[0]],
        ],
        // (E,A) -> V, including duplicate, empty, and wrong-axis peers.
        vec![
            [entities[3], attributes[0], values[0]],
            [entities[1], attributes[0], values[0]],
            [entities[3], attributes[0], values[0]],
            [entities[0], attributes[0], values[0]],
            [values[5], attributes[0], values[0]],
            [entities[3], values[5], values[0]],
        ],
    ];

    for (target_axis, target_peer_rows) in peer_rows.iter().enumerate() {
        for target_index in 0..3 {
            let target = ProgramVariable::new(target_index);
            let mut axis_variables = [target; 3];
            let mut peer_indices = (0..3).filter(|&index| index != target_index);
            for (axis, variable) in axis_variables.iter_mut().enumerate() {
                if axis != target_axis {
                    *variable = ProgramVariable::new(peer_indices.next().unwrap());
                }
            }
            let program = QueryProgram::compile(
                &archive,
                3,
                [QueryPattern::new(
                    axis_variables[0],
                    axis_variables[1],
                    axis_variables[2],
                )],
            )
            .unwrap();
            let mut parent_axes: Vec<_> = (0..3)
                .filter(|&axis| axis != target_axis)
                .map(|axis| (axis_variables[axis], axis))
                .collect();
            parent_axes.sort_unstable_by_key(|&(variable, _)| variable);
            let parent_variables = parent_axes.iter().map(|&(variable, _)| variable).collect();
            let rows = target_peer_rows
                .iter()
                .map(|triple| parent_axes.iter().map(|&(_, axis)| triple[axis]).collect());
            let parent = frontier(&program, parent_variables, rows);

            assert_every_slice_matches_transition(&program, target, &parent);
            assert_batched_pages_reconstruct_transition(&program, target, &parent);

            let empty_parent =
                ProgramFrontier::new(parent.variables().to_vec(), Vec::new(), 0).unwrap();
            let empty_page = program
                .transition_on_two_bound_page(target, &empty_parent, &[], &[])
                .unwrap()
                .expect("an empty batch retains the admitted two-bound schema");
            assert_eq!(
                empty_page.child(),
                &program.transition_on(target, &empty_parent).unwrap()
            );
            assert!(empty_page.receipts().is_empty());
        }
    }
}

#[test]
fn constant_and_missing_peer_pages_cover_all_axes_and_preserve_seed_multiplicity() {
    let (archive, entities, attributes, values) = fixture();
    let witness = [entities[3], attributes[0], values[0]];

    for target_axis in 0..3 {
        let target = ProgramVariable::new(0);
        let terms: [QueryTerm; 3] = std::array::from_fn(|axis| {
            if axis == target_axis {
                QueryTerm::Variable(target)
            } else {
                QueryTerm::Constant(raw(witness[axis]))
            }
        });
        let constants = QueryProgram::compile(
            &archive,
            1,
            [QueryPattern::new(terms[0], terms[1], terms[2])],
        )
        .unwrap();
        let virtual_parents = ProgramFrontier::new(Vec::new(), Vec::new(), 3).unwrap();
        assert_every_slice_matches_transition(&constants, target, &virtual_parents);
        assert_batched_pages_reconstruct_transition(&constants, target, &virtual_parents);

        let missing_terms: [QueryTerm; 3] = std::array::from_fn(|axis| {
            if axis == target_axis {
                QueryTerm::Variable(target)
            } else if axis == (target_axis + 1) % 3 {
                QueryTerm::Constant(raw(fixture_id(9, target_axis)))
            } else {
                QueryTerm::Constant(raw(witness[axis]))
            }
        });
        let missing = QueryProgram::compile(
            &archive,
            1,
            [QueryPattern::new(
                missing_terms[0],
                missing_terms[1],
                missing_terms[2],
            )],
        )
        .unwrap();
        let page = missing
            .transition_on_two_bound_page(target, &ProgramFrontier::seed(), &[0], &[7])
            .unwrap()
            .expect("a missing peer is an admitted empty interval");
        assert!(page.child().is_empty());
        assert_eq!(page.receipts()[0].examined(), 0);
        assert_eq!(page.receipts()[0].next_offset(), None);
        assert_eq!(
            page.child(),
            &missing
                .transition_on(target, &ProgramFrontier::seed())
                .unwrap()
        );
    }
}

#[test]
fn page_contract_fails_closed_and_declines_every_nonresident_shape() {
    let (archive, entities, attributes, _values) = fixture();
    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program = QueryProgram::compile(&archive, 3, [QueryPattern::new(e, a, v)]).unwrap();
    let parent = frontier(&program, vec![e, a], [vec![entities[3], attributes[0]]]);
    let interval = program.transition_on(v, &parent).unwrap().len();

    assert_eq!(
        program
            .transition_on_two_bound_page(v, &parent, &[], &[1])
            .unwrap_err(),
        QueryProgramError::TwoBoundPageShape {
            rows: 1,
            offsets: 0,
            limits: 1,
        }
    );
    assert_eq!(
        program
            .transition_on_two_bound_page(v, &parent, &[0], &[0])
            .unwrap_err(),
        QueryProgramError::ZeroTwoBoundPageLimit { input: 0 }
    );
    assert_eq!(
        program
            .transition_on_two_bound_page(v, &parent, &[interval + 1], &[1])
            .unwrap_err(),
        QueryProgramError::TwoBoundPageOffsetBeyondInterval {
            input: 0,
            offset: interval + 1,
            interval,
        }
    );

    let peer_unbound = frontier(&program, vec![e], [vec![entities[3]]]);
    assert!(program
        .transition_on_two_bound_page(v, &peer_unbound, &[0], &[1])
        .unwrap()
        .is_none());

    let multi = QueryProgram::compile(
        &archive,
        3,
        [QueryPattern::new(e, a, v), QueryPattern::new(e, a, v)],
    )
    .unwrap();
    let multi_parent = frontier(&multi, vec![e, a], [vec![entities[3], attributes[0]]]);
    assert!(multi
        .transition_on_two_bound_page(v, &multi_parent, &[0], &[1])
        .unwrap()
        .is_none());
}
