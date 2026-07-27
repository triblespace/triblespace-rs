use std::collections::{BTreeMap, BTreeSet, VecDeque};

use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::{
    Binding, Constraint, ProposalBuffer, ProposeCursor, Query, Variable,
};
use triblespace_paths::{
    Automaton, GraphEdge, PathConstraint, PathIndex, ProductPoint, Step, Transition,
};

type ProductRelation = BTreeSet<(ProductPoint, ProductPoint)>;
type AcceptedRelation = BTreeSet<(RawInline, RawInline)>;

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

fn direct_product_bfs_oracle(
    automaton: &Automaton,
    edges: &[GraphEdge],
) -> (ProductRelation, AcceptedRelation) {
    let vertices = edges
        .iter()
        .flat_map(|edge| [edge.source, edge.target])
        .collect::<BTreeSet<_>>();
    let points = vertices
        .iter()
        .flat_map(|&vertex| {
            (0..automaton.state_count()).map(move |state| ProductPoint { vertex, state })
        })
        .collect::<Vec<_>>();

    // Construct product arcs directly from the public graph and automaton
    // semantics, without consulting PathIndex or its closure representation.
    let mut adjacency = BTreeMap::<ProductPoint, BTreeSet<ProductPoint>>::new();
    for transition in automaton.transitions() {
        for edge in edges {
            let matches = match &transition.step {
                Step::Forward(expected) | Step::Reverse(expected) => expected == &edge.attribute,
                Step::ForwardExcept(excluded) | Step::ReverseExcept(excluded) => {
                    !excluded.contains(&edge.attribute)
                }
            };
            if !matches {
                continue;
            }
            let (source, target) = match &transition.step {
                Step::Forward(_) | Step::ForwardExcept(_) => (edge.source, edge.target),
                Step::Reverse(_) | Step::ReverseExcept(_) => (edge.target, edge.source),
            };
            adjacency
                .entry(ProductPoint {
                    vertex: source,
                    state: transition.from,
                })
                .or_default()
                .insert(ProductPoint {
                    vertex: target,
                    state: transition.to,
                });
        }
    }

    let initial_states = automaton.initial_states().collect::<BTreeSet<_>>();
    let mut product_pairs = BTreeSet::new();
    let mut accepted_pairs = BTreeSet::new();
    for &origin in &points {
        let mut seen = BTreeSet::from([origin]);
        let mut queue = VecDeque::from([origin]);
        while let Some(at) = queue.pop_front() {
            product_pairs.insert((origin, at));
            if initial_states.contains(&origin.state) && automaton.is_accepting(at.state) {
                accepted_pairs.insert((origin.vertex, at.vertex));
            }
            if let Some(targets) = adjacency.get(&at) {
                for &target in targets {
                    if seen.insert(target) {
                        queue.push_back(target);
                    }
                }
            }
        }
    }

    (product_pairs, accepted_pairs)
}

fn assert_matches_oracle(
    index: &PathIndex,
    expected_product: &ProductRelation,
    expected_accepted: &AcceptedRelation,
    context: &str,
) {
    assert_eq!(
        &product(index),
        expected_product,
        "{context}: product closure"
    );
    assert_eq!(
        &accepted(index),
        expected_accepted,
        "{context}: accepted projection"
    );
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

fn seeded_graph(case: usize) -> Vec<GraphEdge> {
    let vertex_count = 2 + case % 5;
    let mut edges = BTreeSet::new();

    // Every generated graph has a directed cycle through its full universe.
    for source in 0..vertex_count {
        edges.insert(edge(
            source as u8,
            (1 + (source + case) % 5) as u8,
            ((source + 1) % vertex_count) as u8,
        ));
    }

    let mut random = Lcg(0x6a09_e667_f3bc_c909 ^ case as u64);
    for _ in 0..(2 * vertex_count + 3) {
        edges.insert(edge(
            random.below(vertex_count) as u8,
            (1 + random.below(5)) as u8,
            random.below(vertex_count) as u8,
        ));
    }

    edges.into_iter().collect()
}

fn partition_edges(edges: &[GraphEdge], case: usize, layout: usize) -> [Vec<GraphEdge>; 3] {
    let mut partitions = [Vec::new(), Vec::new(), Vec::new()];
    let mut random = Lcg(0xbb67_ae85_84ca_a73b ^ ((case as u64) << 8) ^ layout as u64);
    for (position, &edge) in edges.iter().enumerate() {
        let partition = match layout {
            0 => (position + case) % partitions.len(),
            1 => random.below(partitions.len()),
            _ => unreachable!("the test defines exactly two partition layouts"),
        };
        partitions[partition].push(edge);
    }
    partitions
}

#[test]
fn product_pairs_stay_globally_sorted_across_noncontiguous_scc_members() {
    let automaton = Automaton::new(
        1,
        [0],
        [0],
        [Transition::new(0, 0, Step::Forward(attribute(13)))],
    )
    .unwrap();
    // Points 0 and 2 form one SCC and both reach point 1. Grouping a row by
    // Kosaraju component number would emit targets 0, 2, 1 instead of the
    // baseline's global point order 0, 1, 2.
    let index = PathIndex::from_edges(automaton, [edge(0, 13, 2), edge(2, 13, 0), edge(0, 13, 1)]);
    let actual = index.product_pairs().collect::<Vec<_>>();
    let mut sorted = actual.clone();
    sorted.sort_unstable();

    assert_eq!(actual, sorted);
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
    let stats = merged.build_stats();
    assert!(stats.derived_pairs >= cross_pairs);
    assert!(stats.rectangle_cells_considered >= stats.pairs_added);
    assert_eq!(
        stats.rectangle_log2_counts.iter().sum::<usize>(),
        stats.effective_insertions
    );
    assert_eq!(
        stats.rectangle_log2_cells.iter().sum::<usize>(),
        stats.rectangle_cells_considered
    );
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
    let (expected_product, expected_accepted) = direct_product_bfs_oracle(&automaton, &edges);

    assert_matches_oracle(&index, &expected_product, &expected_accepted, "fixed graph");
}

#[test]
fn seeded_small_graphs_match_product_bfs_across_merge_orders() {
    let automata = [
        Automaton::new(
            4,
            [0, 2],
            [1, 3],
            [
                Transition::new(0, 1, Step::Forward(attribute(1))),
                Transition::new(0, 2, Step::Reverse(attribute(2))),
                Transition::new(1, 3, Step::ForwardExcept(vec![attribute(3)])),
                Transition::new(2, 1, Step::ReverseExcept(vec![attribute(4)])),
                Transition::new(3, 0, Step::Forward(attribute(2))),
            ],
        )
        .unwrap(),
        Automaton::new(
            3,
            [0, 1],
            [0, 2],
            [
                Transition::new(0, 0, Step::forward_any()),
                Transition::new(0, 2, Step::Reverse(attribute(1))),
                Transition::new(1, 2, Step::Forward(attribute(2))),
                Transition::new(2, 1, Step::ReverseExcept(vec![attribute(3)])),
            ],
        )
        .unwrap(),
    ];

    for case in 0..40 {
        let edges = seeded_graph(case);
        for (automaton_number, automaton) in automata.iter().enumerate() {
            let (expected_product, expected_accepted) =
                direct_product_bfs_oracle(automaton, &edges);
            let monolithic = PathIndex::from_edges(automaton.clone(), edges.iter().copied());
            assert_matches_oracle(
                &monolithic,
                &expected_product,
                &expected_accepted,
                &format!("case {case}, automaton {automaton_number}, monolithic"),
            );

            for layout in 0..2 {
                let partitions = partition_edges(&edges, case, layout);
                let leaves = partitions
                    .into_iter()
                    .map(|partition| PathIndex::from_edges(automaton.clone(), partition))
                    .collect::<Vec<_>>();

                let forward = PathIndex::merge_all(leaves.iter()).unwrap();
                assert_matches_oracle(
                    &forward,
                    &expected_product,
                    &expected_accepted,
                    &format!(
                        "case {case}, automaton {automaton_number}, layout {layout}, forward merge"
                    ),
                );

                let reverse = PathIndex::merge_all(leaves.iter().rev()).unwrap();
                assert_matches_oracle(
                    &reverse,
                    &expected_product,
                    &expected_accepted,
                    &format!(
                        "case {case}, automaton {automaton_number}, layout {layout}, reverse merge"
                    ),
                );

                let rotation = 1 + case % (leaves.len() - 1);
                let rotated = PathIndex::merge_all(
                    (0..leaves.len()).map(|offset| &leaves[(offset + rotation) % leaves.len()]),
                )
                .unwrap();
                assert_matches_oracle(
                    &rotated,
                    &expected_product,
                    &expected_accepted,
                    &format!(
                        "case {case}, automaton {automaton_number}, layout {layout}, rotated merge"
                    ),
                );

                let fold_order = [
                    case % leaves.len(),
                    (case + 2) % leaves.len(),
                    (case + 1) % leaves.len(),
                ];
                let mut folded = leaves[fold_order[0]].clone();
                for &position in &fold_order[1..] {
                    folded = folded.merge(&leaves[position]).unwrap();
                }
                assert_matches_oracle(
                    &folded,
                    &expected_product,
                    &expected_accepted,
                    &format!(
                        "case {case}, automaton {automaton_number}, layout {layout}, folded merge"
                    ),
                );
            }
        }
    }
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
