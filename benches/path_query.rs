use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hint::black_box;
use triblespace::core::id::{Id, RawId, ID_LEN};
use triblespace::core::inline::encodings::genid::GenId;
use triblespace::core::inline::IntoInline;
use triblespace::core::patch::{Entry, IdentitySchema, PATCH};
use triblespace::core::query::intersectionconstraint::IntersectionConstraint;
use triblespace::core::query::{Binding, Constraint, Query, TriblePattern, VariableContext};
use triblespace::core::trible::{EAVOrder, TribleSet, TRIBLE_LEN};
use triblespace::prelude::*;

mod bench_social {
    use triblespace::prelude::*;
    attributes! {
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" as follows: inlineencodings::GenId;
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB" as likes: inlineencodings::GenId;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Approach 1: NFA + materialized HashMap (the original)
// ═══════════════════════════════════════════════════════════════════════════

fn value_to_raw_id(v: &[u8; 32]) -> Option<RawId> {
    if v[..16] == [0; 16] {
        Some(v[16..32].try_into().unwrap())
    } else {
        None
    }
}

mod nfa_materialized {
    use super::*;

    const STATE_LEN: usize = core::mem::size_of::<u64>();
    const EDGE_KEY_LEN: usize = STATE_LEN * 2 + ID_LEN;
    const NIL_ID: RawId = [0; ID_LEN];

    #[derive(Clone)]
    struct Automaton {
        transitions: PATCH<EDGE_KEY_LEN, IdentitySchema, ()>,
        start: u64,
        accept: u64,
    }

    impl Automaton {
        fn new(attr: &RawId) -> Self {
            // Simple a+ automaton: s0 --(attr)--> s1, s1 --(attr)--> s1
            let mut trans = PATCH::<EDGE_KEY_LEN, IdentitySchema, ()>::new();
            let mut key = [0u8; EDGE_KEY_LEN];
            // s0 -> s1 via attr
            key[..STATE_LEN].copy_from_slice(&0u64.to_be_bytes());
            key[STATE_LEN..STATE_LEN + ID_LEN].copy_from_slice(attr);
            key[STATE_LEN + ID_LEN..].copy_from_slice(&1u64.to_be_bytes());
            trans.insert(&Entry::new(&key));
            // s1 -> s1 via attr (loop)
            key[..STATE_LEN].copy_from_slice(&1u64.to_be_bytes());
            key[STATE_LEN + ID_LEN..].copy_from_slice(&1u64.to_be_bytes());
            trans.insert(&Entry::new(&key));
            Automaton {
                transitions: trans,
                start: 0,
                accept: 1,
            }
        }

        fn transitions_from(&self, state: &u64, label: &RawId) -> Vec<u64> {
            let mut prefix = [0u8; STATE_LEN + ID_LEN];
            prefix[..STATE_LEN].copy_from_slice(&state.to_be_bytes());
            prefix[STATE_LEN..].copy_from_slice(label);
            let mut dests = Vec::new();
            self.transitions
                .infixes::<{ STATE_LEN + ID_LEN }, { STATE_LEN }, _>(&prefix, |to| {
                    dests.push(u64::from_be_bytes(*to));
                });
            dests
        }

        fn epsilon_closure(&self, states: Vec<u64>) -> Vec<u64> {
            let mut result = states.clone();
            let mut stack = states;
            while let Some(s) = stack.pop() {
                for dest in self.transitions_from(&s, &NIL_ID) {
                    if !result.contains(&dest) {
                        result.push(dest);
                        stack.push(dest);
                    }
                }
            }
            result.sort();
            result.dedup();
            result
        }
    }

    pub fn reachable_from(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let automaton = Automaton::new(attr);
        // Materialize all edges into HashMap.
        let mut edges: HashMap<RawId, Vec<(RawId, RawId)>> = HashMap::new();
        for t in set.iter() {
            let e: RawId = t.data[0..ID_LEN].try_into().unwrap();
            let a: RawId = t.data[ID_LEN..ID_LEN * 2].try_into().unwrap();
            let v = &t.data[32..64];
            if v[..ID_LEN] == [0; ID_LEN] {
                let dest: RawId = v[ID_LEN..].try_into().unwrap();
                edges.entry(e).or_default().push((a, dest));
            }
        }

        // BFS with NFA state tracking.
        let start_states = automaton.epsilon_closure(vec![automaton.start]);
        let mut queue: VecDeque<(RawId, Vec<u64>)> = VecDeque::new();
        queue.push_back((*start, start_states.clone()));
        let mut visited: HashSet<(RawId, Vec<u64>)> = HashSet::new();
        visited.insert((*start, start_states));
        let mut results = HashSet::new();

        while let Some((node, states)) = queue.pop_front() {
            if states.contains(&automaton.accept) {
                results.insert(node);
            }
            if let Some(node_edges) = edges.get(&node) {
                for (a, dest) in node_edges {
                    let mut next_states = Vec::new();
                    for s in &states {
                        next_states.extend(automaton.transitions_from(s, a));
                    }
                    if next_states.is_empty() {
                        continue;
                    }
                    let closure = automaton.epsilon_closure(next_states);
                    if visited.insert((*dest, closure.clone())) {
                        queue.push_back((*dest, closure));
                    }
                }
            }
        }
        results
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Approach 2: NFA + lazy PATCH scans (intermediate)
// ═══════════════════════════════════════════════════════════════════════════

mod nfa_lazy {
    use super::*;

    const STATE_LEN: usize = core::mem::size_of::<u64>();
    const EDGE_KEY_LEN: usize = STATE_LEN * 2 + ID_LEN;
    const NIL_ID: RawId = [0; ID_LEN];

    #[derive(Clone)]
    struct Automaton {
        transitions: PATCH<EDGE_KEY_LEN, IdentitySchema, ()>,
        start: u64,
        accept: u64,
    }

    impl Automaton {
        fn new(attr: &RawId) -> Self {
            let mut trans = PATCH::<EDGE_KEY_LEN, IdentitySchema, ()>::new();
            let mut key = [0u8; EDGE_KEY_LEN];
            key[..STATE_LEN].copy_from_slice(&0u64.to_be_bytes());
            key[STATE_LEN..STATE_LEN + ID_LEN].copy_from_slice(attr);
            key[STATE_LEN + ID_LEN..].copy_from_slice(&1u64.to_be_bytes());
            trans.insert(&Entry::new(&key));
            key[..STATE_LEN].copy_from_slice(&1u64.to_be_bytes());
            key[STATE_LEN + ID_LEN..].copy_from_slice(&1u64.to_be_bytes());
            trans.insert(&Entry::new(&key));
            Automaton {
                transitions: trans,
                start: 0,
                accept: 1,
            }
        }

        fn transitions_from(&self, state: &u64, label: &RawId) -> Vec<u64> {
            let mut prefix = [0u8; STATE_LEN + ID_LEN];
            prefix[..STATE_LEN].copy_from_slice(&state.to_be_bytes());
            prefix[STATE_LEN..].copy_from_slice(label);
            let mut dests = Vec::new();
            self.transitions
                .infixes::<{ STATE_LEN + ID_LEN }, { STATE_LEN }, _>(&prefix, |to| {
                    dests.push(u64::from_be_bytes(*to));
                });
            dests
        }

        fn epsilon_closure(&self, states: Vec<u64>) -> Vec<u64> {
            let mut result = states.clone();
            let mut stack = states;
            while let Some(s) = stack.pop() {
                for dest in self.transitions_from(&s, &NIL_ID) {
                    if !result.contains(&dest) {
                        result.push(dest);
                        stack.push(dest);
                    }
                }
            }
            result.sort();
            result.dedup();
            result
        }
    }

    fn for_each_edge(
        eav: &PATCH<TRIBLE_LEN, EAVOrder, ()>,
        entity: &RawId,
        mut f: impl FnMut(&RawId, &RawId),
    ) {
        let mut attrs: Vec<RawId> = Vec::new();
        eav.infixes::<{ ID_LEN }, { ID_LEN }, _>(entity, |attr| {
            if !attrs.contains(attr) {
                attrs.push(*attr);
            }
        });
        for attr in &attrs {
            let mut prefix = [0u8; ID_LEN * 2];
            prefix[..ID_LEN].copy_from_slice(entity);
            prefix[ID_LEN..].copy_from_slice(attr);
            eav.infixes::<{ ID_LEN * 2 }, 32, _>(&prefix, |value: &[u8; 32]| {
                if value[..ID_LEN] == [0; ID_LEN] {
                    let dest: &[u8; ID_LEN] = value[ID_LEN..].try_into().unwrap();
                    f(attr, dest);
                }
            });
        }
    }

    pub fn reachable_from(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let automaton = Automaton::new(attr);
        let start_states = automaton.epsilon_closure(vec![automaton.start]);
        let mut queue: VecDeque<(RawId, Vec<u64>)> = VecDeque::new();
        queue.push_back((*start, start_states.clone()));
        let mut visited: HashSet<(RawId, Vec<u64>)> = HashSet::new();
        visited.insert((*start, start_states));
        let mut results = HashSet::new();

        while let Some((node, states)) = queue.pop_front() {
            if states.contains(&automaton.accept) {
                results.insert(node);
            }
            for_each_edge(&set.eav, &node, |a, dest| {
                let mut next_states = Vec::new();
                for s in &states {
                    next_states.extend(automaton.transitions_from(s, a));
                }
                if next_states.is_empty() {
                    return;
                }
                let closure = automaton.epsilon_closure(next_states);
                if visited.insert((*dest, closure.clone())) {
                    queue.push_back((*dest, closure));
                }
            });
        }
        results
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Approach 3: Recursive sub-joins via WCO engine (current, paper's approach)
// ═══════════════════════════════════════════════════════════════════════════

mod recursive_join {
    use super::*;

    pub fn reachable_from(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let attr_id = Id::new(*attr).unwrap();
        let mut visited: HashSet<RawId> = HashSet::new();
        let mut results: HashSet<RawId> = HashSet::new();
        let mut frontier: VecDeque<RawId> = VecDeque::new();
        frontier.push_back(*start);
        visited.insert(*start);

        while let Some(node) = frontier.pop_front() {
            let node_id = Id::new(node).unwrap();
            let mut ctx = VariableContext::new();
            let e = ctx.next_variable::<GenId>();
            let a = ctx.next_variable::<GenId>();
            let v = ctx.next_variable::<GenId>();
            let constraints: Vec<Box<dyn Constraint<'static>>> = vec![
                Box::new(e.is(node_id.to_inline())),
                Box::new(a.is(attr_id.to_inline())),
                Box::new(set.pattern(e, a, v)),
            ];
            let dest_idx = v.index;
            let constraint = IntersectionConstraint::new(constraints);
            let reached: HashSet<RawId> = Query::new(constraint, move |binding: &Binding| {
                let raw = binding.get(dest_idx)?;
                super::value_to_raw_id(raw)
            })
            .collect();

            for dest in reached {
                results.insert(dest);
                if visited.insert(dest) {
                    frontier.push_back(dest);
                }
            }
        }
        results
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Approach 4: Hybrid — direct PATCH scan for Attr, WCO join for Concat
// ═══════════════════════════════════════════════════════════════════════════

mod hybrid {
    use super::*;

    fn eval_attr(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let mut results = HashSet::new();
        let mut prefix = [0u8; ID_LEN * 2];
        prefix[..ID_LEN].copy_from_slice(start);
        prefix[ID_LEN..].copy_from_slice(attr);
        set.eav
            .infixes::<{ ID_LEN * 2 }, 32, _>(&prefix, |value: &[u8; 32]| {
                if value[..ID_LEN] == [0; ID_LEN] {
                    let dest: RawId = value[ID_LEN..].try_into().unwrap();
                    results.insert(dest);
                }
            });
        results
    }

    pub fn reachable_from(set: &TribleSet, attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let mut visited: HashSet<RawId> = HashSet::new();
        let mut results: HashSet<RawId> = HashSet::new();
        let mut frontier: VecDeque<RawId> = VecDeque::new();
        frontier.push_back(*start);
        visited.insert(*start);

        while let Some(node) = frontier.pop_front() {
            for dest in eval_attr(set, attr, &node) {
                results.insert(dest);
                if visited.insert(dest) {
                    frontier.push_back(dest);
                }
            }
        }
        results
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Approach 5: Pure path! macro (transitive closure as an engine constraint)
// ═══════════════════════════════════════════════════════════════════════════
//
// `path!(set, s social::follows+ d)` compiles into a `RegularPathConstraint`
// that the engine evaluates natively — no manual BFS, no HashSet-dedup, no
// post-filter Rust. The query *is* the query. Hardcoded to
// `bench_social::follows` because path! requires the attribute as a
// compile-time path expression; the other approaches accept `attr` at
// runtime and ignore the macro form.

mod path_macro {
    use super::*;
    use triblespace::core::inline::Inline;

    pub fn reachable_from(set: &TribleSet, _attr: &RawId, start: &RawId) -> HashSet<RawId> {
        let start_id = Id::new(*start).unwrap();
        let start_val: Inline<GenId> = (&start_id).to_inline();
        find!(
            (s: Inline<GenId>, d: Inline<GenId>),
            and!(
                s.is(start_val),
                path!(set.clone(), s bench_social::follows+ d)
            )
        )
        .filter_map(|(_, d)| value_to_raw_id(&d.raw))
        .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Graph builders
// ═══════════════════════════════════════════════════════════════════════════

fn build_chain(len: usize) -> (TribleSet, Vec<ExclusiveId>) {
    let nodes: Vec<ExclusiveId> = (0..len).map(|_| fucid()).collect();
    let mut set = TribleSet::new();
    for i in 0..len - 1 {
        set += entity! { &nodes[i] @ bench_social::follows: &nodes[i + 1] };
    }
    (set, nodes)
}

fn build_tree(depth: usize) -> (TribleSet, ExclusiveId) {
    let root = fucid();
    let mut set = TribleSet::new();
    let mut frontier = vec![root.id];
    for _ in 0..depth {
        let mut next = Vec::new();
        for parent in &frontier {
            let left = fucid();
            let right = fucid();
            set += entity! { ExclusiveId::force_ref(parent) @ bench_social::follows: &left };
            set += entity! { ExclusiveId::force_ref(parent) @ bench_social::follows: &right };
            next.push(left.id);
            next.push(right.id);
        }
        frontier = next;
    }
    (set, root)
}

fn build_sparse(total_nodes: usize, reachable_len: usize) -> (TribleSet, ExclusiveId) {
    let (mut set, chain) = build_chain(reachable_len);
    let start = ExclusiveId::force(chain[0].id);
    let noise: Vec<ExclusiveId> = (0..total_nodes - reachable_len).map(|_| fucid()).collect();
    for i in 0..noise.len().saturating_sub(1) {
        set += entity! { &noise[i] @ bench_social::follows: &noise[i + 1] };
    }
    (set, start)
}

fn follows_attr() -> RawId {
    bench_social::follows.id().into()
}

// ═══════════════════════════════════════════════════════════════════════════
// Comparison benchmarks
// ═══════════════════════════════════════════════════════════════════════════

fn bench_compare_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare/chain");
    let attr = follows_attr();

    for len in [10, 50, 100, 500] {
        let (set, nodes) = build_chain(len);
        let start: RawId = nodes[0].id.into();

        group.bench_with_input(BenchmarkId::new("nfa_materialized", len), &len, |b, _| {
            b.iter(|| black_box(nfa_materialized::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("nfa_lazy", len), &len, |b, _| {
            b.iter(|| black_box(nfa_lazy::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("recursive_join", len), &len, |b, _| {
            b.iter(|| black_box(recursive_join::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("hybrid", len), &len, |b, _| {
            b.iter(|| black_box(hybrid::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("path_macro", len), &len, |b, _| {
            b.iter(|| black_box(path_macro::reachable_from(&set, &attr, &start)))
        });
    }
    group.finish();
}

fn bench_compare_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare/tree");
    let attr = follows_attr();

    for depth in [3, 5, 7] {
        let (set, root) = build_tree(depth);
        let start: RawId = root.id.into();

        group.bench_with_input(
            BenchmarkId::new("nfa_materialized", depth),
            &depth,
            |b, _| b.iter(|| black_box(nfa_materialized::reachable_from(&set, &attr, &start))),
        );
        group.bench_with_input(BenchmarkId::new("nfa_lazy", depth), &depth, |b, _| {
            b.iter(|| black_box(nfa_lazy::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("recursive_join", depth), &depth, |b, _| {
            b.iter(|| black_box(recursive_join::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("hybrid", depth), &depth, |b, _| {
            b.iter(|| black_box(hybrid::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("path_macro", depth), &depth, |b, _| {
            b.iter(|| black_box(path_macro::reachable_from(&set, &attr, &start)))
        });
    }
    group.finish();
}

fn bench_compare_sparse(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare/sparse");
    let attr = follows_attr();

    for total in [100, 1_000, 10_000] {
        let (set, root) = build_sparse(total, 10);
        let start: RawId = root.id.into();

        group.bench_with_input(
            BenchmarkId::new("nfa_materialized", total),
            &total,
            |b, _| b.iter(|| black_box(nfa_materialized::reachable_from(&set, &attr, &start))),
        );
        group.bench_with_input(BenchmarkId::new("nfa_lazy", total), &total, |b, _| {
            b.iter(|| black_box(nfa_lazy::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("recursive_join", total), &total, |b, _| {
            b.iter(|| black_box(recursive_join::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("hybrid", total), &total, |b, _| {
            b.iter(|| black_box(hybrid::reachable_from(&set, &attr, &start)))
        });
        group.bench_with_input(BenchmarkId::new("path_macro", total), &total, |b, _| {
            b.iter(|| black_box(path_macro::reachable_from(&set, &attr, &start)))
        });
    }
    group.finish();
}

criterion_group!(
    path_benches,
    bench_compare_chain,
    bench_compare_tree,
    bench_compare_sparse,
);
criterion_main!(path_benches);
