//! Executable falsification model for a smaller query-engine kernel.
//!
//! This is deliberately an integration-test-local semantic model, not a
//! second public engine.  Its IR is a tiny positive Datalog:
//!
//! - a relation is a mathematical set of fixed-width raw tuples;
//! - AND is one rule body, solved by variable-at-a-time propose + confirm;
//! - OR is several rules inserting into the same head relation;
//! - recursion feeds newly inserted tuples through the same work deque.
//!
//! Fibers are finite SETs with arbitrary enumeration. The kernel never asks
//! for a least value, a successor, or sorted storage. A backend may exploit
//! order internally, but that is an optional physical optimization rather
//! than part of the semantic protocol.
//!
//! The IR is intentionally positive and monotone. Negation, difference,
//! deletion, and non-monotone aggregation are outside this kernel rather than
//! hidden behind capabilities or a second execution strategy.
//!
//! One work item is also the complete quiescence ledger.  Taking it is a
//! transaction that returns at most one fact plus an optional remainder.
//! Committing a novel fact creates successor work items.  Consequently an
//! empty deque is exact quiescence; no producer-credit side channel exists.
//!
//! The model intentionally records one negative boundary.  A fresh rule
//! activation materializes its finite propose-confirm join before its first
//! tuple is committed. Demand one can still publish before a *global
//! recursive* fixed point saturates, but it does not yet guarantee one-tuple
//! physical work inside an individual high-fanout activation. Replacing that
//! boundary requires a resumable, backend-owned join capsule, not another
//! semantic execution strategy.
//!
//! Projection uses a claimed SET of raw head tuples in declared column order.
//! It does not use a sorted "last value" cursor: recursive discovery and
//! unordered fibers provide no monotone publication order.

use std::collections::{BTreeMap, HashSet, VecDeque};

type Value = u8;
type Tuple = Vec<Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Relation(usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Variable(usize);

#[derive(Clone, Debug)]
struct Atom {
    relation: Relation,
    terms: Vec<Variable>,
}

impl Atom {
    fn new(relation: Relation, terms: &[Variable]) -> Self {
        Self {
            relation,
            terms: terms.to_vec(),
        }
    }
}

#[derive(Clone, Debug)]
struct Rule {
    head: Atom,
    body: Vec<Atom>,
}

impl Rule {
    fn new(head: Atom, body: Vec<Atom>) -> Self {
        Self { head, body }
    }
}

#[derive(Clone, Debug)]
struct Program {
    arities: Vec<usize>,
    rules: Vec<Rule>,
}

impl Program {
    fn new(arities: Vec<usize>, rules: Vec<Rule>) -> Self {
        for rule in &rules {
            assert!(!rule.body.is_empty(), "facts enter through seed tuples");
            Self::validate_atom(&arities, &rule.head);
            for atom in &rule.body {
                Self::validate_atom(&arities, atom);
            }

            let body_variables: HashSet<_> = rule
                .body
                .iter()
                .flat_map(|atom| atom.terms.iter().copied())
                .collect();
            assert!(
                rule.head
                    .terms
                    .iter()
                    .all(|variable| body_variables.contains(variable)),
                "every head variable must be bound by the body"
            );
        }
        Self { arities, rules }
    }

    fn validate_atom(arities: &[usize], atom: &Atom) {
        assert!(
            atom.relation.0 < arities.len(),
            "atom names an unknown relation"
        );
        assert_eq!(
            atom.terms.len(),
            arities[atom.relation.0],
            "atom arity does not match its relation"
        );
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fact {
    relation: Relation,
    tuple: Tuple,
}

impl Fact {
    fn new(relation: Relation, tuple: impl Into<Tuple>) -> Self {
        Self {
            relation,
            tuple: tuple.into(),
        }
    }
}

/// The sole live-work representation.
///
/// `Join` is a fresh semi-naive activation fixed by one newly inserted body
/// tuple. `Emit` owns an opaque physical continuation for that activation. No
/// other object can keep the machine non-quiescent.
#[derive(Clone, Debug)]
enum WorkItem {
    Join {
        rule: usize,
        pivot: usize,
        delta: Tuple,
    },
    Emit {
        relation: Relation,
        resume: OpaqueResume,
    },
}

/// Backend-owned continuation with no semantic ordering contract.
///
/// A real backend could replace this with a scalar stack, a SIMD lane mask,
/// or a device-buffer handle without changing `WorkItem::take`.
#[derive(Clone, Debug)]
struct OpaqueResume {
    tuples: Box<[Tuple]>,
    next: usize,
}

impl OpaqueResume {
    fn from_unordered(tuples: Vec<Tuple>) -> Option<(Tuple, Self)> {
        let first = tuples.first()?.clone();
        Some((
            first,
            Self {
                tuples: tuples.into_boxed_slice(),
                next: 1,
            },
        ))
    }

    fn take(mut self) -> (Tuple, Option<Self>) {
        let tuple = self.tuples[self.next].clone();
        self.next += 1;
        let remainder = (self.next < self.tuples.len()).then_some(self);
        (tuple, remainder)
    }

    fn has_more(&self) -> bool {
        self.next < self.tuples.len()
    }
}

#[derive(Debug)]
struct Taken {
    effect: Option<Fact>,
    remainder: Option<WorkItem>,
    materialized_outputs: usize,
}

impl WorkItem {
    /// Transactionally consumes one item into one effect and a remainder.
    fn take(self, machine: &Machine) -> Taken {
        match self {
            Self::Join { rule, pivot, delta } => {
                let tuples = machine.solve_activation(rule, pivot, &delta);
                let materialized_outputs = tuples.len();
                let relation = machine.program.rules[rule].head.relation;
                let Some((first, resume)) = OpaqueResume::from_unordered(tuples) else {
                    return Taken {
                        effect: None,
                        remainder: None,
                        materialized_outputs,
                    };
                };
                let remainder = resume.has_more().then_some(Self::Emit { relation, resume });
                Taken {
                    effect: Some(Fact::new(relation, first)),
                    remainder,
                    materialized_outputs,
                }
            }
            Self::Emit { relation, resume } => {
                let (tuple, resume) = resume.take();
                let remainder = resume.map(|resume| Self::Emit { relation, resume });
                Taken {
                    effect: Some(Fact::new(relation, tuple)),
                    remainder,
                    materialized_outputs: 0,
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MachineStats {
    takes: usize,
    joins_materialized: usize,
    max_materialized_outputs: usize,
    novel_facts: usize,
}

/// Row-major delta buffers grouped by relation.
///
/// This is the physical boundary a scalar loop, SIMD loop, or accelerator can
/// consume. Every item in one transaction is taken against the same immutable
/// relation snapshot; effects are committed only after the batch returns.
/// The semantic store remains a set, so width changes physical scheduling
/// without changing the least fixed point.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DeltaBatch {
    values: BTreeMap<Relation, Vec<Value>>,
}

impl DeltaBatch {
    fn push(&mut self, fact: &Fact, arity: usize) {
        assert_eq!(fact.tuple.len(), arity);
        self.values
            .entry(fact.relation)
            .or_default()
            .extend_from_slice(&fact.tuple);
    }

    fn rows(&self, relation: Relation, arity: usize) -> Vec<Tuple> {
        self.values
            .get(&relation)
            .map(|values| values.chunks_exact(arity).map(<[Value]>::to_vec).collect())
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
struct Machine {
    program: Program,
    tables: Vec<HashSet<Tuple>>,
    work: VecDeque<WorkItem>,
    reverse_enumeration: bool,
    stats: MachineStats,
}

impl Machine {
    fn new(program: Program, seeds: impl IntoIterator<Item = Fact>) -> Self {
        Self::with_enumeration(program, seeds, false)
    }

    fn with_enumeration(
        program: Program,
        seeds: impl IntoIterator<Item = Fact>,
        reverse_enumeration: bool,
    ) -> Self {
        let table_count = program.arities.len();
        let mut machine = Self {
            program,
            tables: vec![HashSet::new(); table_count],
            work: VecDeque::new(),
            reverse_enumeration,
            stats: MachineStats::default(),
        };
        for seed in seeds {
            machine.commit(seed, None);
        }
        machine
    }

    fn relation(&self, relation: Relation) -> &HashSet<Tuple> {
        &self.tables[relation.0]
    }

    fn is_quiescent(&self) -> bool {
        self.work.is_empty()
    }

    fn pending_items(&self) -> usize {
        self.work.len()
    }

    fn stats(&self) -> MachineStats {
        self.stats
    }

    /// Executes at most `limit` ledger transactions and returns their novel
    /// effects in row-major relation buffers.
    fn transact(&mut self, limit: usize) -> DeltaBatch {
        assert!(limit > 0);
        let mut taken = Vec::with_capacity(limit);
        for _ in 0..limit {
            let Some(item) = self.work.pop_front() else {
                break;
            };
            // All takes happen before any effect is committed. This is the
            // snapshot boundary a parallel CPU or device kernel observes.
            taken.push(item.take(self));
        }

        let mut delta = DeltaBatch::default();
        for taken in taken {
            self.stats.takes += 1;
            if taken.materialized_outputs != 0 {
                self.stats.joins_materialized += 1;
                self.stats.max_materialized_outputs = self
                    .stats
                    .max_materialized_outputs
                    .max(taken.materialized_outputs);
            }

            // The remainder is custody of the exact unconsumed suffix.
            if let Some(remainder) = taken.remainder {
                self.work.push_back(remainder);
            }
            if let Some(effect) = taken.effect {
                self.commit(effect, Some(&mut delta));
            }
        }
        delta
    }

    fn saturate(&mut self, transaction_width: usize) {
        while !self.is_quiescent() {
            self.transact(transaction_width);
        }
    }

    fn commit(&mut self, fact: Fact, mut delta: Option<&mut DeltaBatch>) {
        let arity = self.program.arities[fact.relation.0];
        assert_eq!(fact.tuple.len(), arity);
        if !self.tables[fact.relation.0].insert(fact.tuple.clone()) {
            return;
        }
        self.stats.novel_facts += 1;
        if let Some(delta) = delta.as_mut() {
            delta.push(&fact, arity);
        }

        let mut successors = Vec::new();
        for (rule_index, rule) in self.program.rules.iter().enumerate() {
            for (pivot, atom) in rule.body.iter().enumerate() {
                if atom.relation == fact.relation {
                    successors.push(WorkItem::Join {
                        rule: rule_index,
                        pivot,
                        delta: fact.tuple.clone(),
                    });
                }
            }
        }
        self.work.extend(successors);
    }

    /// Evaluates one semi-naive rule activation with variable-at-a-time
    /// propose + confirm over unordered finite fibers.
    fn solve_activation(&self, rule_index: usize, pivot: usize, delta: &[Value]) -> Vec<Tuple> {
        let rule = &self.program.rules[rule_index];
        let pivot_atom = &rule.body[pivot];
        if delta.len() != pivot_atom.terms.len() {
            return Vec::new();
        }

        let variable_count = rule
            .body
            .iter()
            .flat_map(|atom| atom.terms.iter())
            .chain(rule.head.terms.iter())
            .map(|variable| variable.0 + 1)
            .max()
            .unwrap_or(0);
        let mut binding = vec![None; variable_count];
        for (&variable, &value) in pivot_atom.terms.iter().zip(delta) {
            match binding[variable.0] {
                Some(bound) if bound != value => return Vec::new(),
                Some(_) => {}
                None => binding[variable.0] = Some(value),
            }
        }

        let mut outputs = HashSet::new();
        self.search_rule(rule, &mut binding, &mut outputs);
        let mut outputs: Vec<_> = outputs.into_iter().collect();
        if self.reverse_enumeration {
            outputs.reverse();
        }
        outputs
    }

    fn search_rule(
        &self,
        rule: &Rule,
        binding: &mut [Option<Value>],
        outputs: &mut HashSet<Tuple>,
    ) {
        let unbound: HashSet<_> = rule
            .body
            .iter()
            .flat_map(|atom| atom.terms.iter().copied())
            .filter(|variable| binding[variable.0].is_none())
            .collect();
        if unbound.is_empty() {
            if self.body_holds(&rule.body, binding) {
                outputs.insert(
                    rule.head
                        .terms
                        .iter()
                        .map(|variable| binding[variable.0].expect("head variable is body-bound"))
                        .collect(),
                );
            }
            return;
        }

        let (variable, candidates) = unbound
            .into_iter()
            .map(|variable| {
                let candidates = self.body_fiber(&rule.body, variable, binding);
                (variable, candidates)
            })
            .min_by_key(|(variable, candidates)| (candidates.len(), *variable))
            .expect("the unbound set is non-empty");

        for value in candidates {
            binding[variable.0] = Some(value);
            self.search_rule(rule, binding, outputs);
        }
        binding[variable.0] = None;
    }

    /// AND proposes the smallest finite fiber, then confirms every candidate
    /// against the other relevant fibers. No seek or enumeration order is
    /// required.
    fn body_fiber(
        &self,
        body: &[Atom],
        variable: Variable,
        binding: &[Option<Value>],
    ) -> Vec<Value> {
        let fibers: Vec<_> = body
            .iter()
            .filter(|atom| atom.terms.contains(&variable))
            .map(|atom| self.atom_fiber(atom, variable, binding))
            .collect();
        let proposal_index = fibers
            .iter()
            .enumerate()
            .min_by_key(|(_, fiber)| fiber.len())
            .map(|(index, _)| index)
            .expect("every selected variable occurs in the body");
        let mut confirmed: Vec<_> = fibers[proposal_index]
            .iter()
            .copied()
            .filter(|value| {
                fibers
                    .iter()
                    .enumerate()
                    .all(|(index, fiber)| index == proposal_index || fiber.contains(value))
            })
            .collect();
        if self.reverse_enumeration {
            confirmed.reverse();
        }
        confirmed
    }

    fn atom_fiber(
        &self,
        atom: &Atom,
        variable: Variable,
        binding: &[Option<Value>],
    ) -> HashSet<Value> {
        self.tables[atom.relation.0]
            .iter()
            .filter(|tuple| Self::tuple_compatible(atom, tuple, binding))
            .filter_map(|tuple| {
                atom.terms
                    .iter()
                    .position(|term| *term == variable)
                    .map(|column| tuple[column])
            })
            .collect()
    }

    fn tuple_compatible(atom: &Atom, tuple: &[Value], binding: &[Option<Value>]) -> bool {
        for (column, variable) in atom.terms.iter().copied().enumerate() {
            if binding[variable.0].is_some_and(|value| value != tuple[column]) {
                return false;
            }
            for earlier in 0..column {
                if atom.terms[earlier] == variable && tuple[earlier] != tuple[column] {
                    return false;
                }
            }
        }
        true
    }

    fn body_holds(&self, body: &[Atom], binding: &[Option<Value>]) -> bool {
        body.iter().all(|atom| {
            let tuple: Tuple = atom
                .terms
                .iter()
                .map(|variable| binding[variable.0].expect("all body variables are bound"))
                .collect();
            self.tables[atom.relation.0].contains(&tuple)
        })
    }
}

/// A query-local SET gate over raw head tuples in declared column order.
#[derive(Clone, Debug)]
struct Projection {
    relation: Relation,
    columns: Vec<usize>,
    claimed: HashSet<Tuple>,
}

impl Projection {
    fn new(relation: Relation, columns: &[usize]) -> Self {
        Self {
            relation,
            columns: columns.to_vec(),
            claimed: HashSet::new(),
        }
    }

    /// Pulls at most `demand` new raw head tuples, advancing physical work in
    /// transactions of `work_width`. `claimed`, not physical enumeration, is
    /// semantic identity.
    fn pull(&mut self, machine: &mut Machine, demand: usize, work_width: usize) -> Vec<Tuple> {
        assert!(demand > 0);
        assert!(work_width > 0);
        let arity = machine.program.arities[self.relation.0];
        assert!(self.columns.iter().all(|column| *column < arity));

        let mut output = Vec::new();
        loop {
            for row in machine.relation(self.relation) {
                let key: Tuple = self.columns.iter().map(|column| row[*column]).collect();
                if self.claimed.insert(key.clone()) {
                    output.push(key);
                    if output.len() == demand {
                        return output;
                    }
                }
            }
            if machine.is_quiescent() {
                return output;
            }
            machine.transact(work_width);
        }
    }
}

fn drain_projection(
    machine: &mut Machine,
    projection: &mut Projection,
    demand: usize,
    work_width: usize,
) -> HashSet<Tuple> {
    let mut output = HashSet::new();
    loop {
        let page = projection.pull(machine, demand, work_width);
        let empty = page.is_empty();
        output.extend(page);
        if empty && machine.is_quiescent() {
            return output;
        }
    }
}

fn unary_program() -> Program {
    let a = Relation(0);
    let b = Relation(1);
    let intersection = Relation(2);
    let union = Relation(3);
    let x = Variable(0);
    Program::new(
        vec![1, 1, 1, 1],
        vec![
            Rule::new(
                Atom::new(intersection, &[x]),
                vec![Atom::new(a, &[x]), Atom::new(b, &[x])],
            ),
            Rule::new(Atom::new(union, &[x]), vec![Atom::new(a, &[x])]),
            Rule::new(Atom::new(union, &[x]), vec![Atom::new(b, &[x])]),
        ],
    )
}

fn reach_program() -> Program {
    let edge = Relation(0);
    let reach = Relation(1);
    let x = Variable(0);
    let y = Variable(1);
    let z = Variable(2);
    Program::new(
        vec![2, 2],
        vec![
            Rule::new(Atom::new(reach, &[x, y]), vec![Atom::new(edge, &[x, y])]),
            Rule::new(
                Atom::new(reach, &[x, z]),
                vec![Atom::new(reach, &[x, y]), Atom::new(edge, &[y, z])],
            ),
        ],
    )
}

fn graph_facts(mask: usize, domain: Value) -> Vec<Fact> {
    let edge = Relation(0);
    let mut facts = Vec::new();
    let mut bit = 0usize;
    for source in 0..domain {
        for target in 0..domain {
            if mask & (1usize << bit) != 0 {
                facts.push(Fact::new(edge, vec![source, target]));
            }
            bit += 1;
        }
    }
    facts
}

fn reachability_oracle(edges: &[Fact], domain: Value) -> HashSet<Tuple> {
    let mut reachable = vec![vec![false; domain as usize]; domain as usize];
    for edge in edges {
        reachable[edge.tuple[0] as usize][edge.tuple[1] as usize] = true;
    }
    for middle in 0..domain as usize {
        for source in 0..domain as usize {
            for target in 0..domain as usize {
                reachable[source][target] |= reachable[source][middle] && reachable[middle][target];
            }
        }
    }
    let mut result = HashSet::new();
    for source in 0..domain {
        for target in 0..domain {
            if reachable[source as usize][target as usize] {
                result.insert(vec![source, target]);
            }
        }
    }
    result
}

#[test]
fn exhaustive_and_or_are_intersection_and_union() {
    const DOMAIN: Value = 3;
    let a = Relation(0);
    let b = Relation(1);
    let intersection = Relation(2);
    let union = Relation(3);

    for a_mask in 0usize..1 << DOMAIN {
        for b_mask in 0usize..1 << DOMAIN {
            let seeds = (0..DOMAIN)
                .flat_map(|value| {
                    let mut facts = Vec::new();
                    if a_mask & (1 << value) != 0 {
                        facts.push(Fact::new(a, vec![value]));
                    }
                    if b_mask & (1 << value) != 0 {
                        facts.push(Fact::new(b, vec![value]));
                    }
                    facts
                })
                .collect::<Vec<_>>();

            let mut scalar = Machine::new(unary_program(), seeds.clone());
            scalar.saturate(1);
            let mut batched = Machine::with_enumeration(unary_program(), seeds, true);
            batched.saturate(32);

            let expected_intersection: HashSet<_> = (0..DOMAIN)
                .filter(|value| a_mask & (1 << value) != 0 && b_mask & (1 << value) != 0)
                .map(|value| vec![value])
                .collect();
            let expected_union: HashSet<_> = (0..DOMAIN)
                .filter(|value| a_mask & (1 << value) != 0 || b_mask & (1 << value) != 0)
                .map(|value| vec![value])
                .collect();

            assert_eq!(scalar.relation(intersection), &expected_intersection);
            assert_eq!(scalar.relation(union), &expected_union);
            assert_eq!(scalar.tables, batched.tables);
        }
    }
}

#[test]
fn exhaustive_unordered_propose_confirm_triangle_join_matches_bruteforce() {
    const DOMAIN: Value = 3;
    let edge = Relation(0);
    let triangle = Relation(1);
    let x = Variable(0);
    let y = Variable(1);
    let z = Variable(2);
    let program = Program::new(
        vec![2, 3],
        vec![Rule::new(
            Atom::new(triangle, &[x, y, z]),
            vec![
                Atom::new(edge, &[x, y]),
                Atom::new(edge, &[y, z]),
                Atom::new(edge, &[z, x]),
            ],
        )],
    );

    for mask in 0usize..1 << (DOMAIN as usize * DOMAIN as usize) {
        let edges = graph_facts(mask, DOMAIN);
        let edge_set: HashSet<_> = edges.iter().map(|fact| fact.tuple.clone()).collect();
        let mut expected = HashSet::new();
        for x in 0..DOMAIN {
            for y in 0..DOMAIN {
                for z in 0..DOMAIN {
                    if edge_set.contains(&vec![x, y])
                        && edge_set.contains(&vec![y, z])
                        && edge_set.contains(&vec![z, x])
                    {
                        expected.insert(vec![x, y, z]);
                    }
                }
            }
        }

        let mut scalar = Machine::new(program.clone(), edges.clone());
        scalar.saturate(1);
        let mut batched_reverse = Machine::with_enumeration(program.clone(), edges, true);
        batched_reverse.saturate(17);
        assert_eq!(scalar.relation(triangle), &expected, "graph mask {mask}");
        assert_eq!(
            batched_reverse.relation(triangle),
            &expected,
            "reverse graph mask {mask}"
        );
    }
}

#[test]
fn exhaustive_recursive_fixed_point_is_schedule_and_width_confluent() {
    const DOMAIN: Value = 3;
    let reach = Relation(1);

    for mask in 0usize..1 << (DOMAIN as usize * DOMAIN as usize) {
        let forward = graph_facts(mask, DOMAIN);
        let expected = reachability_oracle(&forward, DOMAIN);
        let mut reverse = forward.clone();
        reverse.reverse();

        let mut scalar = Machine::new(reach_program(), forward);
        scalar.saturate(1);
        let mut batched_reverse = Machine::with_enumeration(reach_program(), reverse, true);
        batched_reverse.saturate(31);

        assert_eq!(scalar.relation(reach), &expected, "graph mask {mask}");
        assert_eq!(
            batched_reverse.relation(reach),
            &expected,
            "reverse graph mask {mask}"
        );
        assert!(scalar.is_quiescent());
        assert!(batched_reverse.is_quiescent());
    }
}

#[test]
fn exhaustive_raw_head_projection_collapses_hidden_witnesses() {
    const DOMAIN: Value = 2;
    let source = Relation(0);
    let proof = Relation(1);
    let x = Variable(0);
    let witness = Variable(1);
    let program = Program::new(
        vec![2, 2],
        vec![Rule::new(
            Atom::new(proof, &[x, witness]),
            vec![Atom::new(source, &[x, witness])],
        )],
    );

    for mask in 0usize..1 << (DOMAIN as usize * DOMAIN as usize) {
        let mut seeds = Vec::new();
        let mut expected_x = HashSet::new();
        let mut expected_raw_head = HashSet::new();
        let mut bit = 0usize;
        for x in 0..DOMAIN {
            for witness in 0..DOMAIN {
                if mask & (1 << bit) != 0 {
                    seeds.push(Fact::new(source, vec![x, witness]));
                    expected_x.insert(vec![x]);
                    expected_raw_head.insert(vec![witness, x]);
                }
                bit += 1;
            }
        }

        let mut scalar = Machine::new(program.clone(), seeds.clone());
        let mut projected_x = Projection::new(proof, &[0]);
        let scalar_x = drain_projection(&mut scalar, &mut projected_x, 1, 1);
        assert_eq!(scalar_x, expected_x, "hidden-witness mask {mask}");

        let mut batched = Machine::with_enumeration(program.clone(), seeds, true);
        let mut reversed_head = Projection::new(proof, &[1, 0]);
        let batched_head = drain_projection(&mut batched, &mut reversed_head, 8, 32);
        assert_eq!(
            batched_head, expected_raw_head,
            "raw head order mask {mask}"
        );
    }
}

#[test]
fn demand_one_yields_before_the_recursive_fixed_point_saturates() {
    let reach = Relation(1);
    let edges = vec![
        Fact::new(Relation(0), vec![0, 1]),
        Fact::new(Relation(0), vec![1, 2]),
        Fact::new(Relation(0), vec![2, 3]),
    ];
    let complete = reachability_oracle(&edges, 4);
    let mut machine = Machine::new(reach_program(), edges);
    let mut projection = Projection::new(reach, &[0, 1]);

    let first = projection.pull(&mut machine, 1, 1);
    assert_eq!(first.len(), 1);
    assert!(
        !machine.is_quiescent(),
        "one published path must not imply global quiescence"
    );
    assert!(
        machine.relation(reach).len() < complete.len(),
        "the first answer must precede whole-fixpoint saturation"
    );
    assert_eq!(machine.stats().takes, 1);

    let rest = drain_projection(&mut machine, &mut projection, 1, 1);
    let mut all: HashSet<_> = first.into_iter().collect();
    all.extend(rest);
    assert_eq!(all, complete);
}

#[test]
fn activation_materialization_is_an_explicit_remaining_boundary() {
    let a = Relation(0);
    let b = Relation(1);
    let product = Relation(2);
    let x = Variable(0);
    let y = Variable(1);
    let program = Program::new(
        vec![1, 1, 2],
        vec![Rule::new(
            Atom::new(product, &[x, y]),
            vec![Atom::new(a, &[x]), Atom::new(b, &[y])],
        )],
    );
    let seeds = vec![
        Fact::new(a, vec![7]),
        Fact::new(b, vec![0]),
        Fact::new(b, vec![1]),
        Fact::new(b, vec![2]),
    ];
    let mut machine = Machine::new(program, seeds);

    let delta = machine.transact(1);
    assert_eq!(delta.rows(product, 2).len(), 1);
    assert_eq!(machine.relation(product).len(), 1);
    assert_eq!(machine.stats().max_materialized_outputs, 3);
    assert!(
        machine.pending_items() > 0,
        "the work-item remainder must own the unconsumed product"
    );

    machine.saturate(8);
    assert_eq!(machine.relation(product).len(), 3);
    assert!(machine.is_quiescent());
}
