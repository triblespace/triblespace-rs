use std::collections::{BTreeSet, VecDeque};

use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::{
    Binding, Constraint, ProposalBuffer, ProposeCursor, Query, Variable,
};
use triblespace_paths::{
    Automaton, GraphEdge, PathConstraint, PathIndex, ProductPoint, Step, Transition,
};

fn vertex(byte: u8) -> RawInline {
    [byte; 32]
}

fn attribute(byte: u8) -> [u8; 16] {
    [byte; 16]
}

fn edge(source: u8, label: u8, target: u8) -> GraphEdge {
    GraphEdge {
        source: vertex(source),
        attribute: attribute(label),
        target: vertex(target),
    }
}

fn plus(label: u8) -> Automaton {
    Automaton::new(
        2,
        [0],
        [1],
        [
            Transition::new(0, 1, Step::Forward(attribute(label))),
            Transition::new(1, 1, Step::Forward(attribute(label))),
        ],
    )
    .unwrap()
}

fn accepted(index: &PathIndex) -> BTreeSet<(RawInline, RawInline)> {
    index.accepted_pairs().collect()
}

fn product(index: &PathIndex) -> BTreeSet<(ProductPoint, ProductPoint)> {
    index.product_pairs().collect()
}

#[test]
fn segment_merge_closes_cross_segment_reentry() {
    let automaton = plus(9);
    let first = PathIndex::from_edges(automaton.clone(), [edge(1, 9, 2), edge(3, 9, 4)]);
    let second = PathIndex::from_edges(automaton, [edge(2, 9, 3)]);
    let merged = first.merge(&second).unwrap();

    // The witness alternates first -> second -> first. Unioning the two
    // accepted endpoint relations cannot derive it.
    assert!(merged.contains(&vertex(1), &vertex(4)));
    assert!(!first.contains(&vertex(1), &vertex(4)));
    assert!(!second.contains(&vertex(1), &vertex(4)));
}

#[test]
fn merge_matches_monolithic_build_and_is_a_semilattice() {
    let automaton = plus(7);
    let a = PathIndex::from_edges(automaton.clone(), [edge(1, 7, 2)]);
    let b = PathIndex::from_edges(automaton.clone(), [edge(2, 7, 3)]);
    let c = PathIndex::from_edges(automaton.clone(), [edge(3, 7, 4)]);
    let monolithic =
        PathIndex::from_edges(automaton, [edge(1, 7, 2), edge(2, 7, 3), edge(3, 7, 4)]);

    let left = a.merge(&b).unwrap().merge(&c).unwrap();
    let right = a.merge(&b.merge(&c).unwrap()).unwrap();
    let commuted = c.merge(&b).unwrap().merge(&a).unwrap();
    let idempotent = monolithic.merge(&monolithic).unwrap();

    for candidate in [&left, &right, &commuted, &idempotent] {
        assert_eq!(accepted(candidate), accepted(&monolithic));
        assert_eq!(product(candidate), product(&monolithic));
    }
}

#[test]
fn graph_term_and_automaton_state_are_both_part_of_identity() {
    let automaton = Automaton::new(
        4,
        [0],
        [3],
        [
            Transition::new(0, 1, Step::Forward(attribute(10))),
            Transition::new(0, 2, Step::Forward(attribute(11))),
            Transition::new(1, 3, Step::Forward(attribute(12))),
        ],
    )
    .unwrap();
    let index = PathIndex::from_edges(automaton, [edge(1, 11, 2), edge(2, 12, 3)]);

    assert!(index.product_reaches(
        ProductPoint {
            vertex: vertex(1),
            state: 0,
        },
        ProductPoint {
            vertex: vertex(2),
            state: 2,
        },
    ));
    assert!(!index.product_reaches(
        ProductPoint {
            vertex: vertex(1),
            state: 0,
        },
        ProductPoint {
            vertex: vertex(2),
            state: 1,
        },
    ));
    assert!(!index.contains(&vertex(1), &vertex(3)));
}

#[test]
fn nullable_identity_is_scoped_to_the_graph_universe() {
    let nullable = Automaton::new(1, [0], [0], []).unwrap();
    let index = PathIndex::from_edges(nullable, [edge(1, 99, 2)]);

    assert!(index.contains(&vertex(1), &vertex(1)));
    assert!(index.contains(&vertex(2), &vertex(2)));
    assert!(!index.contains(&vertex(3), &vertex(3)));
    assert_eq!(index.metrics().accepted_pairs, 2);
}

#[test]
fn one_bridge_can_create_a_quadratic_number_of_answers() {
    const N: u8 = 12;
    let automaton = plus(5);
    let mut outer = Vec::new();
    for i in 0..N {
        outer.push(GraphEdge {
            source: vertex(10 + i),
            attribute: attribute(5),
            target: vertex(100),
        });
        outer.push(GraphEdge {
            source: vertex(101),
            attribute: attribute(5),
            target: vertex(200 + i),
        });
    }
    let outer = PathIndex::from_edges(automaton.clone(), outer);
    let bridge = PathIndex::from_edges(automaton, [edge(100, 5, 101)]);
    let merged = outer.merge(&bridge).unwrap();

    let cross_pairs = merged
        .accepted_pairs()
        .filter(|(source, target)| {
            (10..10 + N).any(|i| *source == vertex(i))
                && (200..200 + N).any(|i| *target == vertex(i))
        })
        .count();
    assert_eq!(cross_pairs, usize::from(N) * usize::from(N));
    assert!(merged.build_stats().derived_pairs >= cross_pairs);
}

#[test]
fn inverse_and_negated_steps_are_product_edges_not_engine_special_cases() {
    let automaton = Automaton::new(
        3,
        [0],
        [2],
        [
            Transition::new(0, 1, Step::Reverse(attribute(1))),
            Transition::new(1, 2, Step::ForwardExcept(vec![attribute(2)])),
        ],
    )
    .unwrap();
    let index = PathIndex::from_edges(automaton, [edge(1, 1, 2), edge(1, 2, 3), edge(1, 3, 4)]);

    assert!(index.contains(&vertex(2), &vertex(4)));
    assert!(!index.contains(&vertex(2), &vertex(3)));
}

#[test]
fn closure_matches_a_direct_product_bfs_oracle() {
    let automaton = Automaton::new(
        4,
        [0, 1],
        [2, 3],
        [
            Transition::new(0, 1, Step::Forward(attribute(1))),
            Transition::new(1, 2, Step::ForwardExcept(vec![attribute(2)])),
            Transition::new(1, 3, Step::Reverse(attribute(3))),
            Transition::new(3, 1, Step::forward_any()),
        ],
    )
    .unwrap();
    let edges = vec![
        edge(1, 1, 2),
        edge(2, 2, 3),
        edge(2, 4, 4),
        edge(5, 3, 2),
        edge(4, 8, 5),
    ];
    let index = PathIndex::from_edges(automaton.clone(), edges.clone());
    let vertices = edges
        .iter()
        .flat_map(|edge| [edge.source, edge.target])
        .collect::<BTreeSet<_>>();

    let mut expected = BTreeSet::new();
    for &origin in &vertices {
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::new();
        for state in automaton.initial_states() {
            seen.insert((origin, state));
            queue.push_back((origin, state));
        }
        while let Some((at, state)) = queue.pop_front() {
            if automaton.is_accepting(state) {
                expected.insert((origin, at));
            }
            for transition in automaton
                .transitions()
                .iter()
                .filter(|transition| transition.from == state)
            {
                for edge in &edges {
                    if !match &transition.step {
                        Step::Forward(a) | Step::Reverse(a) => a == &edge.attribute,
                        Step::ForwardExcept(excluded) | Step::ReverseExcept(excluded) => {
                            !excluded.contains(&edge.attribute)
                        }
                    } {
                        continue;
                    }
                    let next = match transition.step {
                        Step::Forward(_) | Step::ForwardExcept(_) if edge.source == at => {
                            Some(edge.target)
                        }
                        Step::Reverse(_) | Step::ReverseExcept(_) if edge.target == at => {
                            Some(edge.source)
                        }
                        _ => None,
                    };
                    if let Some(next) = next {
                        if seen.insert((next, transition.to)) {
                            queue.push_back((next, transition.to));
                        }
                    }
                }
            }
        }
    }

    assert_eq!(accepted(&index), expected);
}

#[test]
fn classic_constraint_has_exact_fibers_confirmation_and_chunking() {
    let index = PathIndex::from_edges(plus(6), [edge(1, 6, 2), edge(2, 6, 3), edge(3, 6, 4)]);
    let start = Variable::<UnknownInline>::new(0);
    let end = Variable::<UnknownInline>::new(1);
    let constraint = PathConstraint::new(&index, start, end);
    let mut binding = Binding::default();

    assert_eq!(constraint.estimate(start.index, &binding), Some(3));
    binding.set(start.index, &vertex(1));
    assert_eq!(constraint.estimate(end.index, &binding), Some(3));

    let mut eager = ProposalBuffer::new();
    constraint.propose(end.index, &binding, &mut eager);
    assert_eq!(
        eager.live_values(0).copied().collect::<Vec<_>>(),
        vec![vertex(2), vertex(3), vertex(4)]
    );

    let mut chunked = ProposalBuffer::new();
    let mut cursor = ProposeCursor::default();
    loop {
        let more = constraint.propose_chunk(end.index, &binding, &mut cursor, 1, &mut chunked);
        if !more {
            break;
        }
    }
    assert_eq!(
        chunked.live_values(0).copied().collect::<Vec<_>>(),
        eager.live_values(0).copied().collect::<Vec<_>>()
    );

    binding.unset(start.index);
    binding.set(end.index, &vertex(4));
    let mut candidates = ProposalBuffer::new();
    candidates.extend([vertex(1), vertex(2), vertex(4)]);
    constraint.confirm(start.index, &binding, &mut candidates.region(0));
    assert_eq!(
        candidates.live_values(0).copied().collect::<Vec<_>>(),
        vec![vertex(1), vertex(2)]
    );
}

#[test]
fn zero_budget_does_not_advance_past_the_zero_inline() {
    let index = PathIndex::from_edges(
        plus(6),
        [GraphEdge {
            source: [0; 32],
            attribute: attribute(6),
            target: vertex(1),
        }],
    );
    let start = Variable::<UnknownInline>::new(0);
    let constraint = index.constraint(start, Inline::<UnknownInline>::new(vertex(1)));
    let mut cursor = ProposeCursor::default();
    let mut proposals = ProposalBuffer::new();

    assert!(constraint.propose_chunk(
        start.index,
        &Binding::default(),
        &mut cursor,
        0,
        &mut proposals,
    ));
    assert!(!cursor.started);
    assert!(!constraint.propose_chunk(
        start.index,
        &Binding::default(),
        &mut cursor,
        1,
        &mut proposals,
    ));
    assert_eq!(
        proposals.live_values(0).copied().collect::<Vec<_>>(),
        vec![[0; 32]]
    );
}

#[test]
fn classic_query_enumerates_the_index_relation_exactly() {
    let index = PathIndex::from_edges(plus(6), [edge(1, 6, 2), edge(2, 6, 3), edge(4, 6, 5)]);
    let start = Variable::<UnknownInline>::new(0);
    let end = Variable::<UnknownInline>::new(1);
    let actual = Query::new(index.constraint(start, end), |binding: &Binding| {
        Some((*binding.get(0)?, *binding.get(1)?))
    })
    .collect::<BTreeSet<_>>();

    assert_eq!(actual, accepted(&index));
}

#[test]
fn same_variable_and_constant_endpoints_keep_relational_semantics() {
    let acyclic = PathIndex::from_edges(plus(4), [edge(1, 4, 2)]);
    let variable = Variable::<UnknownInline>::new(0);
    let same = acyclic.constraint(variable, variable);
    assert_eq!(same.estimate(variable.index, &Binding::default()), Some(0));
    assert!(!same.satisfied(&Binding::default()));

    let constant_start = Inline::<UnknownInline>::new(vertex(1));
    let constant_end = Inline::<UnknownInline>::new(vertex(2));
    let present = acyclic.constraint(constant_start, constant_end);
    assert!(present.satisfied(&Binding::default()));

    let absent = acyclic.constraint(
        Inline::<UnknownInline>::new(vertex(2)),
        Inline::<UnknownInline>::new(vertex(1)),
    );
    assert!(!absent.satisfied(&Binding::default()));
}
