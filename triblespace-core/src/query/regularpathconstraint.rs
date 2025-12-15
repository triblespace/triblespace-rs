use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use crate::id::id_from_value;
use crate::id::id_into_value;
use crate::id::RawId;
use crate::id::ID_LEN;
use crate::patch::Entry;
use crate::patch::IdentitySchema;
use crate::patch::PATCH;
use crate::query::Binding;
use crate::query::Constraint;
use crate::query::Variable;
use crate::query::VariableId;
use crate::query::VariableSet;
use crate::trible::TribleSet;
use crate::trible::A_END;
use crate::trible::A_START;
use crate::trible::E_END;
use crate::trible::E_START;
use crate::trible::V_START;
use crate::value::schemas::genid::GenId;
use crate::value::RawValue;

#[derive(Clone)]
pub enum PathOp {
    Attr(RawId),
    Concat,
    Union,
    Star,
    Plus,
}

pub trait PathEngine {
    fn has_path(&self, from: &RawId, to: &RawId) -> bool;
}

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
    /// Builds an NFA using Thompson's construction as described in
    /// "Regular expression search algorithm" (Thompson, 1968). The
    /// sequence of `PathOp`s is expected in postfix order.
    fn new(ops: &[PathOp]) -> Self {
        #[derive(Clone)]
        struct Frag {
            start: u64,
            accept: u64,
        }

        fn new_state(counter: &mut u64) -> u64 {
            let id = *counter;
            *counter += 1;
            id
        }

        fn insert_edge(
            patch: &mut PATCH<EDGE_KEY_LEN, IdentitySchema, ()>,
            from: &u64,
            label: &RawId,
            to: &u64,
        ) {
            let mut key = [0u8; EDGE_KEY_LEN];
            key[..STATE_LEN].copy_from_slice(&from.to_be_bytes());
            key[STATE_LEN..STATE_LEN + ID_LEN].copy_from_slice(label);
            key[STATE_LEN + ID_LEN..].copy_from_slice(&to.to_be_bytes());
            patch.insert(&Entry::new(&key));
        }

        let mut trans = PATCH::<EDGE_KEY_LEN, IdentitySchema, ()>::new();
        let mut counter: u64 = 0;
        let mut stack: Vec<Frag> = Vec::new();

        for op in ops {
            match op {
                PathOp::Attr(id) => {
                    let s = new_state(&mut counter);
                    let e = new_state(&mut counter);
                    insert_edge(&mut trans, &s, id, &e);
                    stack.push(Frag {
                        start: s,
                        accept: e,
                    });
                }
                PathOp::Concat => {
                    let b = stack.pop().unwrap();
                    let a = stack.pop().unwrap();
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &b.start);
                    stack.push(Frag {
                        start: a.start,
                        accept: b.accept,
                    });
                }
                PathOp::Union => {
                    let b = stack.pop().unwrap();
                    let a = stack.pop().unwrap();
                    let s = new_state(&mut counter);
                    let e = new_state(&mut counter);
                    insert_edge(&mut trans, &s, &NIL_ID, &a.start);
                    insert_edge(&mut trans, &s, &NIL_ID, &b.start);
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &e);
                    insert_edge(&mut trans, &b.accept, &NIL_ID, &e);
                    stack.push(Frag {
                        start: s,
                        accept: e,
                    });
                }
                PathOp::Star => {
                    let a = stack.pop().unwrap();
                    let s = new_state(&mut counter);
                    let e = new_state(&mut counter);
                    insert_edge(&mut trans, &s, &NIL_ID, &a.start);
                    insert_edge(&mut trans, &s, &NIL_ID, &e);
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &a.start);
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &e);
                    stack.push(Frag {
                        start: s,
                        accept: e,
                    });
                }
                PathOp::Plus => {
                    let a = stack.pop().unwrap();
                    let s = new_state(&mut counter);
                    let e = new_state(&mut counter);
                    insert_edge(&mut trans, &s, &NIL_ID, &a.start);
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &a.start);
                    insert_edge(&mut trans, &a.accept, &NIL_ID, &e);
                    stack.push(Frag {
                        start: s,
                        accept: e,
                    });
                }
            }
        }

        let frag = stack.pop().unwrap();
        Automaton {
            transitions: trans,
            start: frag.start,
            accept: frag.accept,
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

    /// Returns the epsilon-closure of the given states. The resulting set is
    /// sorted and deduplicated to allow canonical comparisons.
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

pub struct ThompsonEngine {
    automaton: Automaton,
    edges: HashMap<RawId, Vec<(RawId, RawId)>>,
}

impl ThompsonEngine {
    pub fn new(set: TribleSet, ops: &[PathOp]) -> (Self, Vec<RawValue>) {
        let automaton = Automaton::new(ops);
        let mut edges: HashMap<RawId, Vec<(RawId, RawId)>> = HashMap::new();
        let mut node_set: HashSet<RawId> = HashSet::new();
        for t in set.iter() {
            let e: RawId = t.data[E_START..=E_END].try_into().unwrap();
            let a: RawId = t.data[A_START..=A_END].try_into().unwrap();
            let v = &t.data[V_START..(V_START + 32)];
            if v[0..16] == [0; 16] {
                let dest: RawId = v[16..32].try_into().unwrap();
                edges.entry(e).or_default().push((a, dest));
                node_set.insert(e);
                node_set.insert(dest);
            }
        }
        let nodes: Vec<RawValue> = node_set.iter().map(id_into_value).collect();
        (ThompsonEngine { automaton, edges }, nodes)
    }
}

impl PathEngine for ThompsonEngine {
    fn has_path(&self, from: &RawId, to: &RawId) -> bool {
        let start_states = self.automaton.epsilon_closure(vec![self.automaton.start]);
        let mut queue: VecDeque<(RawId, Vec<u64>)> = VecDeque::new();
        queue.push_back((*from, start_states.clone()));
        let mut visited: HashSet<(RawId, Vec<u64>)> = HashSet::new();
        visited.insert((*from, start_states.clone()));
        while let Some((node, states)) = queue.pop_front() {
            if states.contains(&self.automaton.accept) && node == *to {
                return true;
            }
            if let Some(edges) = self.edges.get(&node) {
                for (attr, dest) in edges {
                    let mut next_states = Vec::new();
                    for s in &states {
                        next_states.extend(self.automaton.transitions_from(s, attr));
                    }
                    if next_states.is_empty() {
                        continue;
                    }
                    let closure = self.automaton.epsilon_closure(next_states);
                    if visited.insert((*dest, closure.clone())) {
                        queue.push_back((*dest, closure));
                    }
                }
            }
        }
        false
    }
}

pub struct RegularPathConstraint<E: PathEngine> {
    start: VariableId,
    end: VariableId,
    engine: E,
    nodes: Vec<RawValue>,
}

impl RegularPathConstraint<ThompsonEngine> {
    pub fn new(
        set: TribleSet,
        start: Variable<GenId>,
        end: Variable<GenId>,
        ops: &[PathOp],
    ) -> Self {
        let (engine, nodes) = ThompsonEngine::new(set, ops);
        RegularPathConstraint {
            start: start.index,
            end: end.index,
            engine,
            nodes,
        }
    }
}

impl<'a, E: PathEngine> Constraint<'a> for RegularPathConstraint<E> {
    fn variables(&self) -> VariableSet {
        let mut vars = VariableSet::new_empty();
        vars.set(self.start);
        vars.set(self.end);
        vars
    }

    fn estimate(&self, variable: VariableId, _binding: &Binding) -> Option<usize> {
        if variable == self.start || variable == self.end {
            Some(self.nodes.len())
        } else {
            None
        }
    }

    fn propose(&self, variable: VariableId, _binding: &Binding, proposals: &mut Vec<RawValue>) {
        if variable == self.start || variable == self.end {
            proposals.extend(self.nodes.iter().cloned());
        }
    }

    fn confirm(&self, variable: VariableId, binding: &Binding, proposals: &mut Vec<RawValue>) {
        if variable == self.start {
            if let Some(end_val) = binding.get(self.end) {
                if let Some(end_id) = id_from_value(end_val) {
                    proposals.retain(|v| {
                        if let Some(start_id) = id_from_value(v) {
                            self.engine.has_path(&start_id, &end_id)
                        } else {
                            false
                        }
                    });
                } else {
                    proposals.clear();
                }
            }
        } else if variable == self.end {
            if let Some(start_val) = binding.get(self.start) {
                if let Some(start_id) = id_from_value(start_val) {
                    proposals.retain(|v| {
                        if let Some(end_id) = id_from_value(v) {
                            self.engine.has_path(&start_id, &end_id)
                        } else {
                            false
                        }
                    });
                } else {
                    proposals.clear();
                }
            }
        }
    }
}
