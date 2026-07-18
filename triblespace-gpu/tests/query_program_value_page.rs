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
                    .transition_on_value_page(target, &singleton, &[offset], &[limit])
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
            .transition_on_value_page(target, parent, &offsets, &limits)
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
fn variable_peer_pages_are_exact_for_every_offset_limit_and_target_column() {
    let (archive, entities, attributes, values) = fixture();
    let rows = vec![
        vec![entities[3], attributes[0]],
        vec![entities[1], attributes[0]],
        vec![entities[3], attributes[0]], // duplicate parent occurrence
        vec![entities[0], attributes[0]], // real E/A pair with no values
        vec![values[5], attributes[0]],   // domain-valid but absent entity peer
        vec![entities[3], values[5]],     // domain-valid but absent attribute peer
    ];

    for (target_index, entity_index, attribute_index) in [(0, 1, 2), (1, 0, 2), (2, 0, 1)] {
        let target = ProgramVariable::new(target_index);
        let entity = ProgramVariable::new(entity_index);
        let attribute = ProgramVariable::new(attribute_index);
        let program =
            QueryProgram::compile(&archive, 3, [QueryPattern::new(entity, attribute, target)])
                .unwrap();
        let mut parent_variables = vec![entity, attribute];
        parent_variables.sort_unstable();
        let parent = frontier(&program, parent_variables, rows.clone());

        assert_every_slice_matches_transition(&program, target, &parent);
        assert_batched_pages_reconstruct_transition(&program, target, &parent);

        let empty_parent =
            ProgramFrontier::new(parent.variables().to_vec(), Vec::new(), 0).unwrap();
        let empty_page = program
            .transition_on_value_page(target, &empty_parent, &[], &[])
            .unwrap()
            .expect("an empty batch retains the admitted schema");
        assert_eq!(
            empty_page.child(),
            &program.transition_on(target, &empty_parent).unwrap()
        );
        assert!(empty_page.receipts().is_empty());
    }
}

#[test]
fn constant_and_missing_peer_pages_match_the_reference_and_preserve_seed_multiplicity() {
    let (archive, entities, attributes, _values) = fixture();
    let target = ProgramVariable::new(0);
    let constants = QueryProgram::compile(
        &archive,
        1,
        [QueryPattern::new(
            QueryTerm::Constant(raw(entities[3])),
            QueryTerm::Constant(raw(attributes[0])),
            target,
        )],
    )
    .unwrap();
    let virtual_parents = ProgramFrontier::new(Vec::new(), Vec::new(), 3).unwrap();
    assert_every_slice_matches_transition(&constants, target, &virtual_parents);
    assert_batched_pages_reconstruct_transition(&constants, target, &virtual_parents);

    let missing = QueryProgram::compile(
        &archive,
        1,
        [QueryPattern::new(
            QueryTerm::Constant(raw(fixture_id(9, 0))),
            QueryTerm::Constant(raw(attributes[0])),
            target,
        )],
    )
    .unwrap();
    let page = missing
        .transition_on_value_page(target, &ProgramFrontier::seed(), &[0], &[7])
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

#[test]
fn page_contract_fails_closed_and_declines_every_nonresident_shape() {
    let (archive, entities, attributes, values) = fixture();
    let e = ProgramVariable::new(0);
    let a = ProgramVariable::new(1);
    let v = ProgramVariable::new(2);
    let program = QueryProgram::compile(&archive, 3, [QueryPattern::new(e, a, v)]).unwrap();
    let parent = frontier(&program, vec![e, a], [vec![entities[3], attributes[0]]]);
    let interval = program.transition_on(v, &parent).unwrap().len();

    assert_eq!(
        program
            .transition_on_value_page(v, &parent, &[], &[1])
            .unwrap_err(),
        QueryProgramError::ValuePageShape {
            rows: 1,
            offsets: 0,
            limits: 1,
        }
    );
    assert_eq!(
        program
            .transition_on_value_page(v, &parent, &[0], &[0])
            .unwrap_err(),
        QueryProgramError::ZeroValuePageLimit { input: 0 }
    );
    assert_eq!(
        program
            .transition_on_value_page(v, &parent, &[interval + 1], &[1])
            .unwrap_err(),
        QueryProgramError::ValuePageOffsetBeyondInterval {
            input: 0,
            offset: interval + 1,
            interval,
        }
    );

    let peer_unbound = frontier(&program, vec![e], [vec![entities[3]]]);
    assert!(program
        .transition_on_value_page(v, &peer_unbound, &[0], &[1])
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
        .transition_on_value_page(v, &multi_parent, &[0], &[1])
        .unwrap()
        .is_none());

    let constant_value = QueryProgram::compile(
        &archive,
        2,
        [QueryPattern::new(e, a, QueryTerm::Constant(raw(values[0])))],
    )
    .unwrap();
    let constant_parent = frontier(&constant_value, vec![a], [vec![attributes[0]]]);
    assert!(constant_value
        .transition_on_value_page(e, &constant_parent, &[0], &[1])
        .unwrap()
        .is_none());
}
