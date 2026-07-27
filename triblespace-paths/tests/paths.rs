use std::collections::{BTreeMap, BTreeSet, VecDeque};

use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::{
    Binding, BindingStore, Constraint, ProposalBuffer, Query, Variable,
};
use triblespace_paths::{
    Automaton, GraphEdge, PathConstraint, PathIndex, PathSummary, Step, Transition,
};

type Accepted = BTreeSet<(RawInline, RawInline)>;
type Point = (RawInline, u32);

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

fn accepted(index: &PathIndex) -> Accepted {
    index.accepted_pairs().collect()
}

fn step_matches(step: &Step, edge: &GraphEdge) -> Option<(RawInline, RawInline)> {
    let matches = match step {
        Step::Forward(expected) | Step::Reverse(expected) => expected == &edge.attribute,
        Step::ForwardExcept(excluded) | Step::ReverseExcept(excluded) => {
            !excluded.contains(&edge.attribute)
        }
    };
    if !matches {
        return None;
    }
    Some(match step {
        Step::Forward(_) | Step::ForwardExcept(_) => (edge.source, edge.target),
        Step::Reverse(_) | Step::ReverseExcept(_) => (edge.target, edge.source),
    })
}

/// Deliberately independent semantic oracle: construct the direct product and
/// run one ordinary BFS per graph vertex and initial state.
fn bfs_oracle(automaton: &Automaton, edges: &[GraphEdge]) -> Accepted {
    let vertices = edges
        .iter()
        .flat_map(|edge| [edge.source, edge.target])
        .collect::<BTreeSet<_>>();
    let mut adjacency = BTreeMap::<Point, BTreeSet<Point>>::new();
    for transition in automaton.transitions() {
        for edge in edges {
            if let Some((source, target)) = step_matches(&transition.step, edge) {
                adjacency
                    .entry((source, transition.from))
                    .or_default()
                    .insert((target, transition.to));
            }
        }
    }

    let mut accepted = BTreeSet::new();
    for &source in &vertices {
        for initial in automaton.initial_states() {
            let origin = (source, initial);
            let mut seen = BTreeSet::from([origin]);
            let mut queue = VecDeque::from([origin]);
            while let Some(point) = queue.pop_front() {
                if automaton.is_accepting(point.1) {
                    accepted.insert((source, point.0));
                }
                if let Some(targets) = adjacency.get(&point) {
                    for &target in targets {
                        if seen.insert(target) {
                            queue.push_back(target);
                        }
                    }
                }
            }
        }
    }
    accepted
}

#[test]
fn cross_segment_reentry_is_closed_exactly() {
    let automaton = plus(9);
    let first = PathIndex::from_edges(automaton.clone(), [edge(1, 9, 2), edge(3, 9, 4)]).unwrap();
    let second = PathIndex::from_edges(automaton, [edge(2, 9, 3)]).unwrap();
    let merged = first.merge(&second).unwrap();

    assert!(merged.contains(&vertex(1), &vertex(4)));
    assert!(!first.contains(&vertex(1), &vertex(4)));
    assert!(!second.contains(&vertex(1), &vertex(4)));
}

#[test]
fn summaries_and_indexes_obey_semilattice_merge_orders() {
    let automaton = plus(7);
    let a = PathSummary::from_edges(automaton.clone(), [edge(1, 7, 2)]);
    let b = PathSummary::from_edges(automaton.clone(), [edge(2, 7, 3)]);
    let c = PathSummary::from_edges(automaton.clone(), [edge(3, 7, 4)]);
    let monolithic =
        PathSummary::from_edges(automaton, [edge(1, 7, 2), edge(2, 7, 3), edge(3, 7, 4)]);

    let left = a.merge(&b).unwrap().merge(&c).unwrap();
    let right = a.merge(&b.merge(&c).unwrap()).unwrap();
    let commuted = c.merge(&b).unwrap().merge(&a).unwrap();
    let idempotent = monolithic.merge(&monolithic).unwrap();
    assert_eq!(left, monolithic);
    assert_eq!(right, monolithic);
    assert_eq!(commuted, monolithic);
    assert_eq!(idempotent, monolithic);

    let expected = accepted(&PathIndex::from_summary(monolithic).unwrap());
    for summary in [left, right, commuted, idempotent] {
        assert_eq!(
            accepted(&PathIndex::from_summary(summary).unwrap()),
            expected
        );
    }
}

#[test]
fn nullable_identity_is_scoped_to_the_complete_vertex_universe() {
    let nullable = Automaton::new(1, [0], [0], []).unwrap();
    let index = PathIndex::from_edges(nullable, [edge(1, 99, 2)]).unwrap();

    assert_eq!(index.vertex_count(), 2);
    assert_eq!(index.accepted_pair_count(), 2);
    assert_eq!(
        accepted(&index),
        BTreeSet::from([(vertex(1), vertex(1)), (vertex(2), vertex(2))])
    );
    assert!(!index.contains(&vertex(3), &vertex(3)));
}

#[test]
fn nonnullable_domain_is_exactly_matched_edge_support() {
    let automaton = plus(7);
    let matched = [edge(1, 7, 2), edge(2, 7, 3)];
    let all = [matched[0], edge(8, 1, 9), matched[1], edge(10, 2, 11)];
    let filtered = PathSummary::from_edges(automaton.clone(), matched);
    let unfiltered = PathSummary::from_edges(automaton, all);

    assert_eq!(unfiltered, filtered);
    assert_eq!(unfiltered.vertices(), &[vertex(1), vertex(2), vertex(3)]);
    assert_eq!(
        accepted(&PathIndex::from_summary(unfiltered).unwrap()),
        BTreeSet::from([
            (vertex(1), vertex(2)),
            (vertex(1), vertex(3)),
            (vertex(2), vertex(3)),
        ])
    );
}

#[test]
fn nullable_closure_uses_matched_support_plus_full_identity() {
    let automaton = Automaton::new(
        2,
        [0],
        [0, 1],
        [
            Transition::new(0, 1, Step::Forward(attribute(7))),
            Transition::new(1, 1, Step::Forward(attribute(7))),
        ],
    )
    .unwrap();
    let edges = [edge(1, 7, 2), edge(2, 7, 3), edge(8, 1, 9), edge(10, 2, 11)];
    let index = PathIndex::from_edges(automaton.clone(), edges).unwrap();

    assert_eq!(accepted(&index), bfs_oracle(&automaton, &edges));
    assert_eq!(index.vertex_count(), 7);
    assert_eq!(index.accepted_pair_count(), 10);
    for unmatched in [vertex(8), vertex(9), vertex(10), vertex(11)] {
        assert!(index.contains(&unmatched, &unmatched));
        assert_eq!(
            index.reachable_from(&unmatched).collect::<Vec<_>>(),
            [unmatched]
        );
    }
}

#[test]
fn nullable_unmatched_leaf_merges_into_identity_without_affecting_paths() {
    let automaton = Automaton::new(
        2,
        [0],
        [0, 1],
        [
            Transition::new(0, 1, Step::Forward(attribute(7))),
            Transition::new(1, 1, Step::Forward(attribute(7))),
        ],
    )
    .unwrap();
    let matched = PathSummary::from_edges(automaton.clone(), [edge(1, 7, 2)]);
    let unmatched = PathSummary::from_edges(automaton.clone(), [edge(8, 1, 9)]);
    let merged = matched.merge(&unmatched).unwrap();
    let monolithic = PathSummary::from_edges(automaton.clone(), [edge(8, 1, 9), edge(1, 7, 2)]);

    assert_eq!(merged, monolithic);
    assert_eq!(
        accepted(&PathIndex::from_summary(merged).unwrap()),
        bfs_oracle(&automaton, &[edge(8, 1, 9), edge(1, 7, 2)])
    );
}

#[test]
fn nullable_active_row_keeps_identity_sorted_between_targets() {
    let automaton = Automaton::new(
        1,
        [0],
        [0],
        [Transition::new(0, 0, Step::Forward(attribute(7)))],
    )
    .unwrap();
    let edges = [edge(2, 7, 1), edge(2, 7, 3), edge(8, 1, 9)];
    let index = PathIndex::from_edges(automaton, edges).unwrap();

    assert_eq!(
        index.reachable_from(&vertex(2)).collect::<Vec<_>>(),
        [vertex(1), vertex(2), vertex(3)]
    );
    assert!(index.contains(&vertex(2), &vertex(2)));
    assert_eq!(
        index.reachable_from(&vertex(8)).collect::<Vec<_>>(),
        [vertex(8)]
    );
}

#[test]
fn inverse_negation_and_wildcards_are_automaton_semantics() {
    let automaton = Automaton::new(
        4,
        [0, 1],
        [2, 3],
        [
            Transition::new(0, 1, Step::Reverse(attribute(1))),
            Transition::new(1, 2, Step::ForwardExcept(vec![attribute(2)])),
            Transition::new(1, 3, Step::reverse_any()),
        ],
    )
    .unwrap();
    let edges = vec![edge(1, 1, 2), edge(1, 2, 3), edge(1, 3, 4), edge(5, 8, 1)];
    let index = PathIndex::from_edges(automaton.clone(), edges.iter().copied()).unwrap();

    assert_eq!(accepted(&index), bfs_oracle(&automaton, &edges));
    assert!(index.contains(&vertex(2), &vertex(4)));
    assert!(!index.contains(&vertex(2), &vertex(3)));
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

fn partition(edges: &[GraphEdge], case: usize) -> [Vec<GraphEdge>; 3] {
    let mut result = [Vec::new(), Vec::new(), Vec::new()];
    let mut random = Lcg(0xbb67_ae85_84ca_a73b ^ case as u64);
    for &edge in edges {
        let len = result.len();
        result[random.below(len)].push(edge);
    }
    result
}

#[test]
fn forty_seeded_cyclic_graphs_match_bfs_across_merge_orders() {
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
        for automaton in &automata {
            let expected = bfs_oracle(automaton, &edges);
            let monolithic =
                PathIndex::from_edges(automaton.clone(), edges.iter().copied()).unwrap();
            assert_eq!(accepted(&monolithic), expected, "case {case}: monolithic");

            let leaves = partition(&edges, case)
                .into_iter()
                .map(|edges| PathIndex::from_edges(automaton.clone(), edges).unwrap())
                .collect::<Vec<_>>();
            for indexes in [
                vec![&leaves[0], &leaves[1], &leaves[2]],
                vec![&leaves[2], &leaves[1], &leaves[0]],
                vec![&leaves[1], &leaves[2], &leaves[0]],
            ] {
                assert_eq!(
                    accepted(&PathIndex::merge_all(indexes).unwrap()),
                    expected,
                    "case {case}: merged"
                );
            }
            let folded = leaves[2]
                .merge(&leaves[0])
                .unwrap()
                .merge(&leaves[1])
                .unwrap();
            assert_eq!(accepted(&folded), expected, "case {case}: folded");
        }
    }
}

#[test]
fn endpoint_views_are_sorted_exact_and_mutually_derived() {
    let index = PathIndex::from_edges(
        plus(6),
        [edge(1, 6, 2), edge(2, 6, 3), edge(3, 6, 4), edge(8, 9, 9)],
    )
    .unwrap();
    assert_eq!(
        index.reachable_from(&vertex(1)).collect::<Vec<_>>(),
        vec![vertex(2), vertex(3), vertex(4)]
    );
    assert_eq!(
        index.reaching(&vertex(4)).collect::<Vec<_>>(),
        vec![vertex(1), vertex(2), vertex(3)]
    );
    assert_eq!(
        index.starts().collect::<Vec<_>>(),
        vec![vertex(1), vertex(2), vertex(3)]
    );
    assert_eq!(
        index.ends().collect::<Vec<_>>(),
        vec![vertex(2), vertex(3), vertex(4)]
    );
    assert!(index.diagonal().next().is_none());
}

#[test]
fn constraint_supports_constants_and_repeated_variables() {
    let index =
        PathIndex::from_edges(plus(6), [edge(1, 6, 2), edge(2, 6, 3), edge(3, 6, 4)]).unwrap();
    let start = Variable::<UnknownInline>::new(0);
    let end = Variable::<UnknownInline>::new(1);
    let constraint = PathConstraint::new(&index, start, end);
    let mut binding = BindingStore::new();

    assert_eq!(constraint.estimate(start.index, &binding.view()), Some(3));
    binding.bind(start.index, &vertex(1));
    assert_eq!(constraint.estimate(end.index, &binding.view()), Some(3));
    let mut proposed = ProposalBuffer::new();
    constraint.propose(end.index, &binding.view(), &mut proposed);
    assert_eq!(
        proposed.live_values(0).copied().collect::<Vec<_>>(),
        vec![vertex(2), vertex(3), vertex(4)]
    );
    // The batched form is the same enumeration, segmented by parent row.
    let mut batched = ProposalBuffer::new();
    constraint.propose_frontier(end.index, &binding.frontier(), &mut batched);
    assert_eq!(batched.segments(), 1);
    assert_eq!(
        batched.live_values(0).copied().collect::<Vec<_>>(),
        proposed.live_values(0).copied().collect::<Vec<_>>()
    );

    binding.unset(start.index);
    binding.bind(end.index, &vertex(4));
    let mut candidates = ProposalBuffer::new();
    candidates.extend([vertex(1), vertex(2), vertex(4)]);
    constraint.confirm(start.index, &binding.view(), &mut candidates.region(0));
    assert_eq!(
        candidates.live_values(0).copied().collect::<Vec<_>>(),
        vec![vertex(1), vertex(2)]
    );

    let present = index.constraint(
        Inline::<UnknownInline>::new(vertex(1)),
        Inline::<UnknownInline>::new(vertex(4)),
    );
    assert!(present.satisfied(&Binding::default()));
    let absent = index.constraint(
        Inline::<UnknownInline>::new(vertex(4)),
        Inline::<UnknownInline>::new(vertex(1)),
    );
    assert!(!absent.satisfied(&Binding::default()));
    let same = index.constraint(start, start);
    assert_eq!(same.estimate(start.index, &Binding::default()), Some(0));
    assert!(!same.satisfied(&Binding::default()));

    let cyclic = PathIndex::from_edges(plus(6), [edge(1, 6, 2), edge(2, 6, 1)]).unwrap();
    let same = cyclic.constraint(start, start);
    assert_eq!(same.estimate(start.index, &Binding::default()), Some(2));
    assert!(same.satisfied(&Binding::default()));
    let mut proposals = ProposalBuffer::new();
    same.propose(start.index, &Binding::default(), &mut proposals);
    assert_eq!(
        proposals.live_values(0).copied().collect::<Vec<_>>(),
        vec![vertex(1), vertex(2)]
    );
}

#[test]
fn query_enumerates_the_endpoint_set_exactly() {
    let index =
        PathIndex::from_edges(plus(5), [edge(1, 5, 2), edge(2, 5, 3), edge(4, 5, 5)]).unwrap();
    let start = Variable::<UnknownInline>::new(0);
    let end = Variable::<UnknownInline>::new(1);
    let actual = Query::new(index.constraint(start, end), |binding: &Binding| {
        Some((*binding.get(0)?, *binding.get(1)?))
    })
    .collect::<BTreeSet<_>>();

    assert_eq!(actual, accepted(&index));
}

#[test]
fn automaton_canonicalization_and_mismatch_are_explicit() {
    let first = Automaton::new(
        2,
        [0, 0],
        [1],
        [
            Transition::new(
                0,
                1,
                Step::ForwardExcept(vec![attribute(2), attribute(1), attribute(1)]),
            ),
            Transition::new(0, 1, Step::ForwardExcept(vec![attribute(1), attribute(2)])),
        ],
    )
    .unwrap();
    let canonical = Automaton::new(
        2,
        [0],
        [1],
        [Transition::new(
            0,
            1,
            Step::ForwardExcept(vec![attribute(1), attribute(2)]),
        )],
    )
    .unwrap();
    assert_eq!(first, canonical);

    let other = Automaton::new(1, [0], [0], []).unwrap();
    let a = PathSummary::from_edges(first, [edge(1, 3, 2)]);
    let b = PathSummary::from_edges(other, [edge(1, 3, 2)]);
    assert!(a.merge(&b).is_err());
}
