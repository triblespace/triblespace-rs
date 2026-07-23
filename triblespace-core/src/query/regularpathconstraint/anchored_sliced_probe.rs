//! Ignored empirical falsifier for a candidate-sliced RPQ executor.
//!
//! This module deliberately changes no production policy. The shared arm is
//! the ordinary HYBRID query path and therefore uses the current grouped RPQ
//! Confirm Program. The isolated arm calls the already-existing BOTH-BOUND
//! Support Program once per distinct raw `(source, target)` candidate and
//! aggregates those Boolean results outside the production reducer.
//!
//! The isolated scheduler is intentionally explicit:
//!
//! - candidate rows come lazily from the selected TriblePattern backend;
//! - only one complete source group (plus one-row lookahead) is admitted
//!   before traversal starts;
//! - compatible typed work receives positive one-unit limits under a global
//!   `1, 2, 4, ...` receipt grant, reset to one after a new Support witness;
//! - a positive candidate is logically complete at its first sound witness,
//!   while a negative candidate is complete only at quiescence;
//! - the first parent becomes publishable only after all `k` of its candidates
//!   are logically complete;
//! - live positive tails are retained and physically drained after every
//!   candidate has been classified. Nothing here claims cancellation.
//!
//! `ProgramPage` and RPQ-transition counters are reported independently. On
//! this exact BOTH-BOUND Support route every generic page is a transition
//! page, and the probe asserts (rather than assumes silently) that equality.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hint::black_box;
use std::time::{Duration, Instant};

use crate::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use crate::id::{Id, RawId};
use crate::inline::encodings::genid::GenId;
use crate::inline::{Inline, IntoInline, RawInline};
use crate::query::intersectionconstraint::IntersectionConstraint;
use crate::query::program::ProgramActivation;
use crate::query::residual::{ResidualLowering, ResidualStateIter, ResidualStateStats};
use crate::query::{
    Binding, Constraint, ProgramAction, ProgramBatch, ProgramBatchEffects, ProgramCompletion,
    ProgramExposure, ProgramGrouping, ProgramRef, ProgramRequest, ProgramResume, ProgramRoute,
    ProgramSeedBatch, ProgramSeedEffects, ProgramStratum, ProgramWork, Query, RowsView,
    TriblePattern, Variable, VariableSet,
};
use crate::trible::{Trible, TribleSet};

use super::{PathOp, RegularPathConstraint};

type Pair = (RawInline, RawInline);

const COMPONENTS: usize = 8;
const CORE_NODES: usize = 64;
const SOURCES_PER_COMPONENT: usize = 32;
const CANDIDATES_PER_SIDE: usize = 8;
const CORE_FANOUT_PER_ATTRIBUTE: usize = 16;
const WIDTHS: [usize; 4] = [2, 4, 8, 16];
const PARENT_COUNT: usize = COMPONENTS * SOURCES_PER_COMPONENT;
const EXPECTED_GRAPH_DIGEST: u64 = 0x77d4_6972_7d59_14b5;
const MAX_GRANT: usize = 4096;

const KIND: RawId = [
    0x52, 0x2E, 0xB8, 0x35, 0x1D, 0xA6, 0x09, 0x56, 0xD2, 0xD1, 0x6E, 0x6E, 0xD9, 0x74, 0x5B, 0xA7,
];
const P: RawId = [
    0xFD, 0xD4, 0x9F, 0x6E, 0x08, 0xAC, 0x2C, 0xCB, 0x79, 0xEE, 0x6C, 0x8B, 0x12, 0x56, 0xAD, 0x02,
];
const Q: RawId = [
    0xA4, 0xD0, 0x8A, 0xA5, 0x92, 0x73, 0xB3, 0x36, 0xF5, 0xB9, 0x77, 0xCE, 0x15, 0x11, 0xD1, 0x41,
];
// Minted on the frozen crossover branch with `trible genid`.
const CANDIDATE: RawId = [
    0x94, 0xE9, 0xC8, 0x66, 0xF2, 0x97, 0x9C, 0xFE, 0x08, 0x86, 0x45, 0x77, 0x93, 0x8A, 0x14, 0xBA,
];

fn id(raw: RawId) -> Id {
    Id::new(raw).expect("probe schema IDs are non-zero")
}

fn genid_inline(value: &Id) -> Inline<GenId> {
    (*value).to_inline()
}

fn fixture_id(namespace: u64, ordinal: u64) -> Id {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&namespace.to_be_bytes());
    raw[8..].copy_from_slice(&ordinal.checked_add(1).unwrap().to_be_bytes());
    Id::new(raw).expect("fixture namespace is non-zero")
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: RawId, to: &Id) {
    set.insert(&Trible::force(from, &id(attribute), &genid_inline(to)));
}

fn byte_digest<'a>(chunks: impl IntoIterator<Item = &'a [u8]>) -> u64 {
    chunks
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325, |mut hash, chunk| {
            for &byte in chunk {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            hash
        })
}

fn tribleset_byte_digest(set: &TribleSet) -> u64 {
    let mut tribles: Vec<_> = set.iter().map(|trible| trible.data).collect();
    tribles.sort_unstable();
    byte_digest(tribles.iter().map(|trible| trible.as_slice()))
}

fn pair_order_digest(rows: &[Pair]) -> u64 {
    byte_digest(
        rows.iter()
            .flat_map(|(source, target)| [source.as_slice(), target.as_slice()]),
    )
}

fn pair_set_digest(rows: &[Pair]) -> u64 {
    let mut rows = rows.to_vec();
    rows.sort_unstable();
    rows.dedup();
    pair_order_digest(&rows)
}

struct Fixture {
    graph: TribleSet,
    components: Vec<Vec<Id>>,
    sources: Vec<Id>,
    local_targets: Vec<Vec<Id>>,
    remote_targets: Vec<Vec<Id>>,
    seed: Id,
    graph_digest: u64,
}

struct Cell {
    width: usize,
    candidates: TribleSet,
    candidate_bag: Vec<Pair>,
    expected: Vec<Pair>,
}

impl Fixture {
    fn new() -> Self {
        const CORE_NAMESPACE: u64 = 0xC055_0001_0000_0001;
        const LOCAL_NAMESPACE: u64 = 0xC055_0001_0000_0002;
        const REMOTE_NAMESPACE: u64 = 0xC055_0001_0000_0003;
        const MARKER_NAMESPACE: u64 = 0xC055_0001_0000_0004;

        let seed = fixture_id(MARKER_NAMESPACE, 0);
        let components: Vec<Vec<Id>> = (0..COMPONENTS)
            .map(|component| {
                (0..CORE_NODES)
                    .map(|position| {
                        fixture_id(CORE_NAMESPACE, (component * CORE_NODES + position) as u64)
                    })
                    .collect()
            })
            .collect();
        let sources: Vec<Id> = components
            .iter()
            .flat_map(|component| component.iter().take(SOURCES_PER_COMPONENT).copied())
            .collect();
        assert_eq!(sources.len(), PARENT_COUNT);

        let target_table = |namespace| {
            (0..PARENT_COUNT)
                .map(|source| {
                    (0..CANDIDATES_PER_SIDE)
                        .map(|candidate| {
                            fixture_id(namespace, (source * CANDIDATES_PER_SIDE + candidate) as u64)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let local_targets = target_table(LOCAL_NAMESPACE);
        let remote_targets = target_table(REMOTE_NAMESPACE);

        let mut graph = TribleSet::new();
        for (component_index, component) in components.iter().enumerate() {
            for (position, source) in component.iter().enumerate() {
                for offset in 1..=CORE_FANOUT_PER_ATTRIBUTE {
                    insert_relation(
                        &mut graph,
                        source,
                        P,
                        &component[(position + offset) % CORE_NODES],
                    );
                    insert_relation(
                        &mut graph,
                        source,
                        Q,
                        &component[(position + CORE_FANOUT_PER_ATTRIBUTE + offset) % CORE_NODES],
                    );
                }
            }

            for source_position in 0..SOURCES_PER_COMPONENT {
                let source_ordinal = component_index * SOURCES_PER_COMPONENT + source_position;
                let source = &component[source_position];
                let next_source = &components[(component_index + 1) % COMPONENTS][source_position];
                for candidate in 0..CANDIDATES_PER_SIDE {
                    let attribute = if candidate % 2 == 0 { P } else { Q };
                    insert_relation(
                        &mut graph,
                        source,
                        attribute,
                        &local_targets[source_ordinal][candidate],
                    );
                    insert_relation(
                        &mut graph,
                        next_source,
                        attribute,
                        &remote_targets[source_ordinal][candidate],
                    );
                }
            }
        }

        let all_targets: BTreeSet<_> = local_targets
            .iter()
            .chain(&remote_targets)
            .flatten()
            .map(|target| genid_inline(target).raw)
            .collect();
        assert_eq!(all_targets.len(), PARENT_COUNT * CANDIDATES_PER_SIDE * 2);
        let mut inverse_degree = BTreeMap::<RawInline, usize>::new();
        for trible in graph.iter() {
            let target = trible.v::<GenId>().raw;
            if all_targets.contains(&target) {
                *inverse_degree.entry(target).or_default() += 1;
            }
        }
        assert_eq!(inverse_degree.len(), all_targets.len());
        assert!(inverse_degree.values().all(|degree| *degree == 1));

        let graph_digest = tribleset_byte_digest(&graph);
        assert_eq!(
            graph_digest, EXPECTED_GRAPH_DIGEST,
            "frozen graph digest changed"
        );
        Self {
            graph,
            components,
            sources,
            local_targets,
            remote_targets,
            seed,
            graph_digest,
        }
    }

    fn cell(&self, width: usize) -> Cell {
        assert!(WIDTHS.contains(&width));
        let side = width / 2;
        let mut candidates = TribleSet::new();
        for (source_ordinal, source) in self.sources.iter().enumerate() {
            insert_relation(&mut candidates, source, KIND, &self.seed);
            for target in self.local_targets[source_ordinal].iter().take(side) {
                insert_relation(&mut candidates, source, CANDIDATE, target);
            }
            for target in self.remote_targets[source_ordinal].iter().take(side) {
                insert_relation(&mut candidates, source, CANDIDATE, target);
            }
        }

        let mut candidate_bag = Vec::new();
        let mut forward = BTreeMap::<RawInline, usize>::new();
        let mut inverse = BTreeMap::<RawInline, usize>::new();
        for trible in candidates.iter() {
            if trible.a().raw() != CANDIDATE {
                continue;
            }
            let pair = (genid_inline(trible.e()).raw, trible.v::<GenId>().raw);
            candidate_bag.push(pair);
            *forward.entry(pair.0).or_default() += 1;
            *inverse.entry(pair.1).or_default() += 1;
        }
        candidate_bag.sort_unstable();
        assert_eq!(candidate_bag.len(), PARENT_COUNT * width);
        assert_eq!(forward.len(), PARENT_COUNT);
        assert!(forward.values().all(|count| *count == width));
        assert_eq!(inverse.len(), candidate_bag.len());
        assert!(inverse.values().all(|count| *count == 1));
        assert_eq!(tribleset_byte_digest(&self.graph), self.graph_digest);

        let expected = self.independent_oracle(&candidates);
        assert_eq!(expected.len(), PARENT_COUNT * side);
        Cell {
            width,
            candidates,
            candidate_bag,
            expected,
        }
    }

    /// Independent nested SET oracle. It knows nothing about the construction
    /// tables and reads only marker/candidate facts plus graph adjacency.
    fn independent_oracle(&self, candidates: &TribleSet) -> Vec<Pair> {
        let mut marked = BTreeSet::new();
        let mut proposed = BTreeMap::<RawInline, Vec<RawInline>>::new();
        let seed = genid_inline(&self.seed).raw;
        for trible in candidates.iter() {
            let source = genid_inline(trible.e()).raw;
            if trible.a().raw() == KIND && trible.v::<GenId>().raw == seed {
                marked.insert(source);
            } else if trible.a().raw() == CANDIDATE {
                proposed
                    .entry(source)
                    .or_default()
                    .push(trible.v::<GenId>().raw);
            }
        }
        assert_eq!(marked.len(), PARENT_COUNT);

        let mut adjacency = BTreeMap::<RawInline, Vec<RawInline>>::new();
        for trible in self.graph.iter() {
            if trible.a().raw() == P || trible.a().raw() == Q {
                adjacency
                    .entry(genid_inline(trible.e()).raw)
                    .or_default()
                    .push(trible.v::<GenId>().raw);
            }
        }

        let mut expected = BTreeSet::new();
        for source in marked {
            let mut reachable = BTreeSet::new();
            let mut queue = VecDeque::from([source]);
            while let Some(node) = queue.pop_front() {
                for &target in adjacency.get(&node).into_iter().flatten() {
                    if reachable.insert(target) {
                        queue.push_back(target);
                    }
                }
            }
            for &target in proposed.get(&source).into_iter().flatten() {
                if reachable.contains(&target) {
                    expected.insert((source, target));
                }
            }
        }
        expected.into_iter().collect()
    }
}

fn path(fixture: &Fixture) -> RegularPathConstraint {
    let source = Variable::<GenId>::new(0);
    let target = Variable::<GenId>::new(1);
    RegularPathConstraint::new(
        fixture.graph.clone(),
        source,
        target,
        &[
            PathOp::Attr(P),
            PathOp::Attr(Q),
            PathOp::Union,
            PathOp::Plus,
        ],
    )
}

type DynConstraint<'a> = Box<dyn Constraint<'a> + Send + Sync + 'a>;

fn candidate_query<'a, S>(store: &'a S, fixture: &'a Fixture) -> impl Iterator<Item = Pair> + 'a
where
    S: TriblePattern + 'a,
    S::PatternConstraint<'a>: Send + Sync + 'a,
{
    let source = Variable::<GenId>::new(0);
    let target = Variable::<GenId>::new(1);
    let root: IntersectionConstraint<DynConstraint<'a>> = IntersectionConstraint::new(vec![
        Box::new(store.pattern::<GenId>(
            source,
            genid_inline(&id(KIND)),
            genid_inline(&fixture.seed),
        )),
        Box::new(store.pattern::<GenId>(source, genid_inline(&id(CANDIDATE)), target)),
    ]);
    Query::new(root, |binding: &Binding| {
        Some((*binding.get(0)?, *binding.get(1)?))
    })
}

type SharedRoot<'a> = IntersectionConstraint<DynConstraint<'a>>;
type PairProjector = fn(&Binding) -> Option<Pair>;
type SharedIter<'a> = ResidualStateIter<SharedRoot<'a>, PairProjector, Pair>;

fn project_pair(binding: &Binding) -> Option<Pair> {
    Some((*binding.get(0)?, *binding.get(1)?))
}

fn shared_query<'a, S>(store: &'a S, fixture: &'a Fixture) -> SharedIter<'a>
where
    S: TriblePattern + 'a,
    S::PatternConstraint<'a>: Send + Sync + 'a,
{
    let source = Variable::<GenId>::new(0);
    let target = Variable::<GenId>::new(1);
    let root: IntersectionConstraint<DynConstraint<'a>> = IntersectionConstraint::new(vec![
        Box::new(store.pattern::<GenId>(
            source,
            genid_inline(&id(KIND)),
            genid_inline(&fixture.seed),
        )),
        Box::new(store.pattern::<GenId>(source, genid_inline(&id(CANDIDATE)), target)),
        Box::new(path(fixture)),
    ]);
    Query::new(root, project_pair as PairProjector)
        .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Work {
    generic_pages: usize,
    generic_examined: usize,
    transition_pages: usize,
    transition_examined: usize,
}

impl Work {
    fn add_receipt(&mut self, effects: &ProgramBatchEffects) {
        self.generic_pages += effects.pages.len();
        self.generic_examined += effects
            .pages
            .iter()
            .map(|page| page.examined)
            .sum::<usize>();
        self.transition_pages += effects.transition_pages;
        self.transition_examined += effects.transition_examined;
        assert_eq!(
            self.generic_pages, self.transition_pages,
            "BOTH-BOUND RPQ Support emitted a non-transition generic page"
        );
        assert_eq!(
            self.generic_examined, self.transition_examined,
            "BOTH-BOUND RPQ Support generic/transition work diverged"
        );
        assert_eq!(effects.source_pages, 0);
        assert_eq!(effects.source_examined, 0);
        assert_eq!(effects.source_roots, 0);
    }

    fn from_shared(stats: &ResidualStateStats) -> Self {
        // The selected shared route is bound-endpoint RPQ Confirm. Its seed
        // opens product states directly; it has no source/support page kind.
        Self {
            generic_pages: stats.delta_transition_pages,
            generic_examined: stats.delta_transition_candidates_examined,
            transition_pages: stats.delta_transition_pages,
            transition_examined: stats.delta_transition_candidates_examined,
        }
    }
}

#[derive(Clone, Debug)]
struct Run {
    rows: Vec<Pair>,
    candidate_rows: Option<Vec<Pair>>,
    first_witness: Option<Duration>,
    first_exact_parent: Duration,
    logical_complete: Duration,
    drain_complete: Duration,
    enumeration: Duration,
    setup_admission: Duration,
    work_at_first_witness: Work,
    work_at_first_parent: Work,
    work_at_logical: Work,
    work_at_drain: Work,
    first_witness_grant: usize,
    candidate_groups: usize,
    support_seed_roots: usize,
    shared_confirm_rows: usize,
    shared_candidates_confirmed: usize,
}

fn run_shared<S>(store: &S, fixture: &Fixture, width: usize) -> Run
where
    S: TriblePattern,
    for<'a> S::PatternConstraint<'a>: Send + Sync,
{
    let started = Instant::now();
    let setup_started = Instant::now();
    let mut query = shared_query(store, fixture);
    let setup_admission = setup_started.elapsed();

    let first = query
        .next()
        .expect("crossover fixture must have a reachable candidate");
    black_box(first);
    // ParentAtomic Confirm cannot publish any surviving child until the
    // complete candidate bag for its parent has quiesced and been finalized.
    // The Program's private first-witness time is not exposed by this arm.
    let first_exact_parent = started.elapsed();
    let first_stats = query.stats().clone();
    let work_at_first = Work::from_shared(&first_stats);
    assert!(work_at_first.transition_pages > 0);
    assert!(first_stats.confirm_rows >= 1);
    assert!(
        first_stats.max_confirm_candidates >= width,
        "first shared publication did not traverse one complete width-k parent bag"
    );

    let mut rows = vec![first];
    rows.extend(query.by_ref());
    black_box(&rows);
    let logical_complete = started.elapsed();
    let final_stats = query.stats().clone();
    let work_at_drain = Work::from_shared(&final_stats);
    assert_eq!(work_at_drain.generic_pages, work_at_drain.transition_pages);
    assert_eq!(
        work_at_drain.generic_examined,
        work_at_drain.transition_examined
    );
    assert!(
        final_stats.confirm_action_pops > 0 && final_stats.delta_transition_pages > 0,
        "shared arm did not execute current typed Confirm"
    );

    Run {
        rows,
        candidate_rows: None,
        first_witness: None,
        first_exact_parent,
        logical_complete,
        drain_complete: logical_complete,
        enumeration: Duration::ZERO,
        setup_admission,
        work_at_first_witness: work_at_first,
        work_at_first_parent: work_at_first,
        work_at_logical: work_at_drain,
        work_at_drain,
        first_witness_grant: 0,
        candidate_groups: PARENT_COUNT,
        support_seed_roots: 0,
        shared_confirm_rows: final_stats.confirm_rows,
        shared_candidates_confirmed: final_stats.candidates_confirmed,
    }
}

struct Scheduled {
    candidate: usize,
    work: ProgramWork,
}

#[derive(Default)]
struct CandidateState {
    outstanding: usize,
    positive: bool,
    classified: bool,
}

fn take_compatible(queue: &mut VecDeque<Scheduled>, grant: usize) -> Vec<Scheduled> {
    assert!(grant > 0);
    let Some(first) = queue.pop_front() else {
        return Vec::new();
    };
    let dispatch = first.work.dispatch;
    let pacing = first.work.pacing;
    let mut selected = vec![first];
    while selected.len() < grant
        && queue
            .front()
            .is_some_and(|task| task.work.dispatch == dispatch && task.work.pacing == pacing)
    {
        selected.push(queue.pop_front().unwrap());
    }
    selected
}

#[allow(clippy::too_many_arguments)]
fn step_selected(
    program: ProgramRef<'_>,
    runtime: &mut crate::query::ProgramRuntime,
    route: ProgramRoute,
    pairs: &[Pair],
    activations: &[ProgramActivation],
    selected: Vec<Scheduled>,
    states: &mut [CandidateState],
    ready: &mut VecDeque<Scheduled>,
    cleanup: &mut VecDeque<Scheduled>,
    rows: &mut Vec<Pair>,
    work: &mut Work,
    logical: bool,
) -> bool {
    assert!(!selected.is_empty());
    let vars = [0, 1];
    let mut batch_rows = Vec::with_capacity(selected.len() * 2);
    let mut batch_activations = Vec::with_capacity(selected.len());
    let mut batch_work = Vec::with_capacity(selected.len());
    for task in &selected {
        let pair = pairs[task.candidate];
        batch_rows.extend([pair.0, pair.1]);
        batch_activations.push(activations[task.candidate]);
        batch_work.push(task.work.clone());
        states[task.candidate].outstanding -= 1;
    }
    let view = RowsView::new(&vars, &batch_rows);
    let candidate_sets = vec![None; selected.len()];
    let limits = vec![1; selected.len()];
    let mut effects = ProgramBatchEffects::default();
    program.step_batch(
        runtime,
        ProgramBatch {
            stratum: route.stratum,
            view,
            candidate_sets: &candidate_sets,
            activations: &batch_activations,
            work: &batch_work,
            limits: &limits,
        },
        &mut effects,
    );
    work.add_receipt(&effects);
    assert_eq!(effects.pages.len(), selected.len());
    assert!(effects.direct.is_empty());

    let mut new_work: Vec<Vec<ProgramWork>> = (0..selected.len()).map(|_| Vec::new()).collect();
    let mut witnessed = vec![false; selected.len()];
    for child in effects.children {
        let input = child.input as usize;
        assert!(input < selected.len());
        if let Some(value) = child.accepted {
            assert_eq!(value, pairs[selected[input].candidate].1);
            witnessed[input] = true;
        }
        new_work[input].push(child.work);
    }
    for (input, value) in effects.accepted {
        let input = input as usize;
        assert_eq!(value, pairs[selected[input].candidate].1);
        witnessed[input] = true;
    }
    for (input, ()) in effects.supported {
        witnessed[input as usize] = true;
    }
    for (input, page) in effects.pages.into_iter().enumerate() {
        match page.resume {
            Some(ProgramResume::Immediate(work)) => new_work[input].push(work),
            Some(ProgramResume::AfterChildren(_)) | Some(ProgramResume::AfterChildrenDone) => {
                panic!("BOTH-BOUND RPQ Support unexpectedly requested a join barrier")
            }
            None => {}
        }
    }

    let mut any_new_witness = false;
    let mut newly_positive = BTreeSet::new();
    for (input, task) in selected.iter().enumerate() {
        let candidate = task.candidate;
        states[candidate].outstanding += new_work[input].len();
        if logical && witnessed[input] && !states[candidate].positive {
            states[candidate].positive = true;
            states[candidate].classified = true;
            rows.push(pairs[candidate]);
            newly_positive.insert(candidate);
            any_new_witness = true;
        }
        let destination = if states[candidate].positive {
            &mut *cleanup
        } else {
            &mut *ready
        };
        destination.extend(
            new_work[input]
                .drain(..)
                .map(|work| Scheduled { candidate, work }),
        );
    }

    if !newly_positive.is_empty() {
        let mut kept = VecDeque::with_capacity(ready.len());
        while let Some(task) = ready.pop_front() {
            if newly_positive.contains(&task.candidate) {
                cleanup.push_back(task);
            } else {
                kept.push_back(task);
            }
        }
        *ready = kept;
    }
    any_new_witness
}

fn timed_next<I: Iterator<Item = Pair>>(iter: &mut I, enumeration: &mut Duration) -> Option<Pair> {
    let started = Instant::now();
    let row = iter.next();
    *enumeration += started.elapsed();
    row
}

fn next_group<I: Iterator<Item = Pair>>(
    iter: &mut I,
    lookahead: &mut Option<Pair>,
    enumeration: &mut Duration,
) -> Option<Vec<Pair>> {
    let first = lookahead.take().or_else(|| timed_next(iter, enumeration))?;
    let mut group = vec![first];
    loop {
        match timed_next(iter, enumeration) {
            Some(pair) if pair.0 == first.0 => group.push(pair),
            Some(pair) => {
                *lookahead = Some(pair);
                break;
            }
            None => break,
        }
    }
    Some(group)
}

fn run_sliced<S>(store: &S, fixture: &Fixture, width: usize) -> Run
where
    S: TriblePattern,
    for<'a> S::PatternConstraint<'a>: Send + Sync,
{
    let started = Instant::now();
    let setup_started = Instant::now();
    let path = path(fixture);
    let program = ProgramRef::new(&path);
    let mut bound = VariableSet::new_singleton(0);
    bound.set(1);
    let request = ProgramRequest {
        action: ProgramAction::Support,
        bound,
    };
    let route = program
        .route(request)
        .expect("BOTH-BOUND RPQ Support route disappeared");
    assert_eq!(route.stratum, ProgramStratum::Fixpoint);
    assert_eq!(route.grouping, ProgramGrouping::ParentAtomic);
    assert_eq!(route.completion, ProgramCompletion::PageableOnly);
    assert_eq!(route.exposure, ProgramExposure::Production);
    let mut runtime = program.new_runtime();
    let mut candidates = candidate_query(store, fixture);
    let mut setup_admission = setup_started.elapsed();

    let vars = [0, 1];
    let mut lookahead = None;
    let mut enumeration = Duration::ZERO;
    let mut pairs = Vec::with_capacity(PARENT_COUNT * width);
    let mut activations = Vec::with_capacity(PARENT_COUNT * width);
    let mut states = Vec::with_capacity(PARENT_COUNT * width);
    let mut ready = VecDeque::new();
    let mut cleanup = VecDeque::new();
    let mut rows = Vec::with_capacity(PARENT_COUNT * width / 2);
    let mut seen_sources = BTreeSet::new();
    let mut work = Work::default();
    let mut first_witness = None;
    let mut first_exact_parent = None;
    let mut work_at_first_witness = Work::default();
    let mut work_at_first_parent = Work::default();
    let mut first_witness_grant = 0;
    let mut next_activation = 1u64;
    let mut candidate_groups = 0usize;
    let mut grant = 1usize;

    while let Some(group) = next_group(&mut candidates, &mut lookahead, &mut enumeration) {
        assert_eq!(
            group.len(),
            width,
            "backend query did not expose one exact width-k source group"
        );
        assert!(
            seen_sources.insert(group[0].0),
            "backend query split one source across physical groups"
        );
        candidate_groups += 1;
        let group_start = pairs.len();

        let admission_started = Instant::now();
        for pair in group {
            let candidate = pairs.len();
            pairs.push(pair);
            let activation = ProgramActivation(next_activation);
            next_activation += 1;
            activations.push(activation);
            states.push(CandidateState::default());
            let row = [pair.0, pair.1];
            let view = RowsView::new(&vars, &row);
            let mut seed = ProgramSeedEffects::default();
            program.seed_batch(
                &mut runtime,
                ProgramSeedBatch {
                    request,
                    route,
                    view,
                    activations: std::slice::from_ref(&activation),
                },
                &mut seed,
            );
            assert_eq!(seed.work.len(), 1);
            let root = seed.work.pop().unwrap();
            assert_eq!(root.parent, 0);
            assert!(
                root.accepted.is_none(),
                "non-nullable + route unexpectedly accepted at seed"
            );
            states[candidate].outstanding = 1;
            ready.push_back(Scheduled {
                candidate,
                work: root.work,
            });
        }
        setup_admission += admission_started.elapsed();

        let group_end = pairs.len();
        while states[group_start..group_end]
            .iter()
            .any(|state| !state.classified)
        {
            let selected = take_compatible(&mut ready, grant);
            assert!(
                !selected.is_empty(),
                "unclassified candidate lost all affine work"
            );
            let used = selected.len();
            assert!(used > 0 && used <= grant);
            let witnessed = step_selected(
                program,
                &mut runtime,
                route,
                &pairs,
                &activations,
                selected,
                &mut states,
                &mut ready,
                &mut cleanup,
                &mut rows,
                &mut work,
                true,
            );
            if witnessed && first_witness.is_none() {
                first_witness = Some(started.elapsed());
                work_at_first_witness = work;
                first_witness_grant = grant;
            }

            for state in &mut states[group_start..group_end] {
                if !state.classified && state.outstanding == 0 {
                    state.classified = true;
                }
            }
            grant = if witnessed {
                1
            } else {
                grant.saturating_mul(2).min(MAX_GRANT)
            };
        }

        if first_exact_parent.is_none() {
            first_exact_parent = Some(started.elapsed());
            work_at_first_parent = work;
        }
    }
    assert!(lookahead.is_none());
    assert_eq!(candidate_groups, PARENT_COUNT);
    assert_eq!(pairs.len(), PARENT_COUNT * width);
    assert!(states.iter().all(|state| state.classified));
    let logical_complete = started.elapsed();
    let work_at_logical = work;

    // Positive candidates stopped at their first witness. Drain every retained
    // affine tail through the same typed route, without treating duplicate
    // witnesses as new logical publications and without claiming cancellation.
    while !cleanup.is_empty() {
        let selected = take_compatible(&mut cleanup, grant);
        assert!(!selected.is_empty());
        let used = selected.len();
        assert!(used > 0 && used <= grant);
        let mut ignored_ready = VecDeque::new();
        step_selected(
            program,
            &mut runtime,
            route,
            &pairs,
            &activations,
            selected,
            &mut states,
            &mut ignored_ready,
            &mut cleanup,
            &mut rows,
            &mut work,
            false,
        );
        assert!(ignored_ready.is_empty());
        grant = grant.saturating_mul(2).min(MAX_GRANT);
    }
    assert!(states.iter().all(|state| state.outstanding == 0));
    program.retire_activations(&mut runtime, route.key, &activations);
    let drain_complete = started.elapsed();
    let work_at_drain = work;

    Run {
        rows,
        candidate_rows: Some(pairs),
        first_witness: Some(first_witness.expect("fixture has reachable candidates")),
        first_exact_parent: first_exact_parent.expect("fixture has source groups"),
        logical_complete,
        drain_complete,
        enumeration,
        setup_admission,
        work_at_first_witness,
        work_at_first_parent,
        work_at_logical,
        work_at_drain,
        first_witness_grant,
        candidate_groups,
        support_seed_roots: activations.len(),
        shared_confirm_rows: 0,
        shared_candidates_confirmed: 0,
    }
}

fn sorted_set(rows: &[Pair]) -> Vec<Pair> {
    let mut set = rows.to_vec();
    set.sort_unstable();
    set.dedup();
    set
}

fn validate_run(label: &str, run: &Run, cell: &Cell) {
    assert_eq!(
        run.rows.len(),
        cell.expected.len(),
        "{label} emitted a duplicate or omitted a survivor"
    );
    assert_eq!(
        sorted_set(&run.rows),
        cell.expected,
        "{label} disagrees with independent raw-tuple SET oracle"
    );
    if let Some(candidate_rows) = &run.candidate_rows {
        assert_eq!(candidate_rows.len(), cell.candidate_bag.len());
        assert_eq!(
            sorted_set(candidate_rows),
            cell.candidate_bag,
            "{label} backend enumeration changed the raw candidate bag"
        );
        let mut counts = BTreeMap::<RawInline, usize>::new();
        for &(source, _) in candidate_rows {
            *counts.entry(source).or_default() += 1;
        }
        assert_eq!(counts.len(), PARENT_COUNT);
        assert!(counts.values().all(|count| *count == cell.width));
    }
}

fn ns(duration: Duration) -> u128 {
    duration.as_nanos()
}

fn percentile(samples: &[u128], percentile: f64) -> u128 {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let rank = (percentile * samples.len() as f64).ceil() as usize;
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Arm {
    Shared,
    Sliced,
}

impl Arm {
    fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared_typed_confirm",
            Self::Sliced => "candidate_sliced_support",
        }
    }
}

const BALANCED_ORDERS: [[Arm; 2]; 2] = [[Arm::Shared, Arm::Sliced], [Arm::Sliced, Arm::Shared]];

fn print_run(backend: &str, width: usize, phase: &str, repeat: usize, arm: Arm, run: &Run) {
    let first_witness = run
        .first_witness
        .map(|duration| ns(duration).to_string())
        .unwrap_or_else(|| "na".to_owned());
    println!(
        "anchored_raw backend={backend:?} width={width} phase={phase} repeat={repeat} \
         arm={} private_first_witness_ns={} first_exact_parent_ns={} logical_ns={} drain_ns={} \
         enumeration_ns={} setup_admission_ns={} first_witness_grant={} \
         rows={} set_digest={:#018x} order_digest={:#018x} candidate_rows={} \
         candidate_set_digest={:#018x} candidate_order_digest={:#018x} groups={} \
         support_seed_roots={} \
         first_witness_pages={} first_witness_examined={} first_parent_pages={} \
         first_parent_examined={} logical_pages={} logical_examined={} drain_pages={} \
         drain_examined={} transition_pages={} transition_examined={} \
         shared_confirm_rows={} shared_candidates_confirmed={}",
        arm.label(),
        first_witness,
        ns(run.first_exact_parent),
        ns(run.logical_complete),
        ns(run.drain_complete),
        ns(run.enumeration),
        ns(run.setup_admission),
        run.first_witness_grant,
        run.rows.len(),
        pair_set_digest(&run.rows),
        pair_order_digest(&run.rows),
        run.candidate_rows.as_ref().map_or(0, Vec::len),
        run.candidate_rows.as_deref().map_or(0, pair_set_digest),
        run.candidate_rows.as_deref().map_or(0, pair_order_digest),
        run.candidate_groups,
        run.support_seed_roots,
        run.work_at_first_witness.generic_pages,
        run.work_at_first_witness.generic_examined,
        run.work_at_first_parent.generic_pages,
        run.work_at_first_parent.generic_examined,
        run.work_at_logical.generic_pages,
        run.work_at_logical.generic_examined,
        run.work_at_drain.generic_pages,
        run.work_at_drain.generic_examined,
        run.work_at_drain.transition_pages,
        run.work_at_drain.transition_examined,
        run.shared_confirm_rows,
        run.shared_candidates_confirmed,
    );
}

fn backend_panel<S>(
    backend: &str,
    store: &S,
    fixture: &Fixture,
    cell: &Cell,
    repetitions: usize,
) -> (Vec<Run>, Vec<Run>)
where
    S: TriblePattern,
    for<'a> S::PatternConstraint<'a>: Send + Sync,
{
    let path = path(fixture);
    let confirm_route = ProgramRef::new(&path)
        .route(ProgramRequest {
            action: ProgramAction::Confirm(1),
            // The candidate bag supplies variable 1 to Confirm; the ambient
            // parent row owns only the opposite endpoint.
            bound: VariableSet::new_singleton(0),
        })
        .expect("shared arm's typed bound-endpoint Confirm route disappeared");
    assert_eq!(confirm_route.grouping, ProgramGrouping::ParentAtomic);

    let enumerated: Vec<_> = candidate_query(store, fixture).collect();
    assert_eq!(
        sorted_set(&enumerated),
        cell.candidate_bag,
        "{backend} candidate query changed the frozen raw bag"
    );
    let mut multiplicity = BTreeMap::<RawInline, usize>::new();
    for &(source, _) in &enumerated {
        *multiplicity.entry(source).or_default() += 1;
    }
    assert_eq!(multiplicity.len(), PARENT_COUNT);
    assert!(multiplicity.values().all(|count| *count == cell.width));

    let shared = run_shared(store, fixture, cell.width);
    let sliced = run_sliced(store, fixture, cell.width);
    validate_run("shared warm", &shared, cell);
    validate_run("sliced warm", &sliced, cell);
    print_run(backend, cell.width, "correctness", 0, Arm::Shared, &shared);
    print_run(backend, cell.width, "correctness", 0, Arm::Sliced, &sliced);

    let shared_repeat = run_shared(store, fixture, cell.width);
    let sliced_repeat = run_sliced(store, fixture, cell.width);
    validate_run("shared repeat", &shared_repeat, cell);
    validate_run("sliced repeat", &sliced_repeat, cell);
    assert_eq!(
        pair_order_digest(&shared.rows),
        pair_order_digest(&shared_repeat.rows),
        "{backend} shared physical order was not repeat-stable"
    );
    assert_eq!(
        pair_order_digest(&sliced.rows),
        pair_order_digest(&sliced_repeat.rows),
        "{backend} sliced physical order was not repeat-stable"
    );
    assert_eq!(
        pair_order_digest(sliced.candidate_rows.as_ref().unwrap()),
        pair_order_digest(sliced_repeat.candidate_rows.as_ref().unwrap()),
        "{backend} candidate enumeration order was not repeat-stable"
    );
    println!(
        "anchored_order backend={backend:?} width={} shared={:#018x} sliced={:#018x} \
         candidate={:#018x} cross_arm_equal={} semantic_gate=raw_tuple_set_only",
        cell.width,
        pair_order_digest(&shared.rows),
        pair_order_digest(&sliced.rows),
        pair_order_digest(sliced.candidate_rows.as_ref().unwrap()),
        shared.rows == sliced.rows,
    );

    let mut shared_runs = Vec::with_capacity(repetitions);
    let mut sliced_runs = Vec::with_capacity(repetitions);
    for repeat in 0..repetitions {
        for arm in BALANCED_ORDERS[repeat % BALANCED_ORDERS.len()] {
            let run = match arm {
                Arm::Shared => run_shared(store, fixture, cell.width),
                Arm::Sliced => run_sliced(store, fixture, cell.width),
            };
            validate_run(arm.label(), &run, cell);
            print_run(backend, cell.width, "timing", repeat, arm, &run);
            match arm {
                Arm::Shared => shared_runs.push(run),
                Arm::Sliced => sliced_runs.push(run),
            }
        }
    }
    (shared_runs, sliced_runs)
}

fn print_summary(backend: &str, width: usize, arm: Arm, runs: &[Run]) {
    let first_witness: Vec<_> = runs
        .iter()
        .filter_map(|run| run.first_witness.map(ns))
        .collect();
    let first_parent: Vec<_> = runs.iter().map(|run| ns(run.first_exact_parent)).collect();
    let logical: Vec<_> = runs.iter().map(|run| ns(run.logical_complete)).collect();
    let drain: Vec<_> = runs.iter().map(|run| ns(run.drain_complete)).collect();
    let enumeration: Vec<_> = runs.iter().map(|run| ns(run.enumeration)).collect();
    let setup: Vec<_> = runs.iter().map(|run| ns(run.setup_admission)).collect();
    println!(
        "anchored_summary backend={backend:?} width={width} arm={} repetitions={} \
         private_first_witness_p50_ns={} private_first_witness_p95_ns={} \
         first_exact_parent_p50_ns={} first_exact_parent_p95_ns={} \
         logical_p50_ns={} logical_p95_ns={} drain_p50_ns={} drain_p95_ns={} \
         enumeration_p50_ns={} enumeration_p95_ns={} \
         setup_admission_p50_ns={} setup_admission_p95_ns={}",
        arm.label(),
        runs.len(),
        first_witness
            .is_empty()
            .then_some("na".to_owned())
            .unwrap_or_else(|| percentile(&first_witness, 0.50).to_string()),
        first_witness
            .is_empty()
            .then_some("na".to_owned())
            .unwrap_or_else(|| percentile(&first_witness, 0.95).to_string()),
        percentile(&first_parent, 0.50),
        percentile(&first_parent, 0.95),
        percentile(&logical, 0.50),
        percentile(&logical, 0.95),
        percentile(&drain, 0.50),
        percentile(&drain, 0.95),
        percentile(&enumeration, 0.50),
        percentile(&enumeration, 0.95),
        percentile(&setup, 0.50),
        percentile(&setup, 0.95),
    );
}

fn verdict(backend: &str, width: usize, shared: &[Run], sliced: &[Run]) -> bool {
    let shared_first: Vec<_> = shared
        .iter()
        .map(|run| ns(run.first_exact_parent))
        .collect();
    let sliced_first: Vec<_> = sliced
        .iter()
        .map(|run| ns(run.first_exact_parent))
        .collect();
    let shared_p50 = percentile(&shared_first, 0.50);
    let shared_p95 = percentile(&shared_first, 0.95);
    let sliced_p50 = percentile(&sliced_first, 0.50);
    let sliced_p95 = percentile(&sliced_first, 0.95);
    let retains = sliced_p50 < shared_p50 && sliced_p95 < shared_p95;
    println!(
        "anchored_verdict_cell backend={backend:?} width={width} \
         criterion=first_exact_parent_both_p50_and_p95_lower \
         shared_p50_ns={shared_p50} shared_p95_ns={shared_p95} \
         sliced_p50_ns={sliced_p50} sliced_p95_ns={sliced_p95} retains={retains}"
    );
    retains
}

#[test]
#[ignore = "release-only empirical falsifier; set RPQ_ANCHORED_REPS (default 7)"]
fn rpq_anchored_sliced_probe() {
    let repetitions = std::env::var("RPQ_ANCHORED_REPS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("RPQ_ANCHORED_REPS is an integer")
        })
        .unwrap_or(7);
    assert!(repetitions >= 3, "p95 needs at least three repetitions");
    let widths = std::env::var("RPQ_ANCHORED_WIDTHS").map_or_else(
        |_| WIDTHS.to_vec(),
        |value| {
            value
                .split(',')
                .map(|width| {
                    width
                        .parse::<usize>()
                        .expect("RPQ_ANCHORED_WIDTHS is comma-separated integers")
                })
                .inspect(|width| {
                    assert!(
                        WIDTHS.contains(width),
                        "RPQ_ANCHORED_WIDTHS contains an unsupported width"
                    )
                })
                .collect::<Vec<_>>()
        },
    );
    assert!(!widths.is_empty(), "RPQ_ANCHORED_WIDTHS is empty");

    let built = Instant::now();
    let fixture = Fixture::new();
    println!(
        "anchored_fixture components={} core_nodes={} selected_parents={} graph_tribles={} \
         graph_digest={:#018x} built_ms={:.3}",
        fixture.components.len(),
        CORE_NODES,
        fixture.sources.len(),
        fixture.graph.len(),
        fixture.graph_digest,
        built.elapsed().as_secs_f64() * 1e3,
    );
    println!(
        "anchored_preregister primary=first_exact_parent \
         retain_if=sliced_p50_and_p95_both_lower_at_k2_and_k4_on_both_backends \
         first_witness=diagnostic_only logical_completion=secondary drain=secondary \
         ordering=telemetry_only"
    );

    let mut primary_cells = Vec::new();
    for width in widths {
        let cell = fixture.cell(width);
        let archive_started = Instant::now();
        let archive: SuccinctArchive<OrderedUniverse> = (&cell.candidates).into();
        println!(
            "anchored_geometry width={} candidate_facts={} forward_groups={} group_width={} \
             local={} remote={} expected_survivors={} candidate_store_tribles={} \
             graph_digest={:#018x} archive_build_ms={:.3}",
            width,
            cell.candidate_bag.len(),
            PARENT_COUNT,
            width,
            cell.expected.len(),
            cell.candidate_bag.len() - cell.expected.len(),
            cell.expected.len(),
            cell.candidates.len(),
            fixture.graph_digest,
            archive_started.elapsed().as_secs_f64() * 1e3,
        );

        let (set_shared, set_sliced) =
            backend_panel("TribleSet", &cell.candidates, &fixture, &cell, repetitions);
        print_summary("TribleSet", width, Arm::Shared, &set_shared);
        print_summary("TribleSet", width, Arm::Sliced, &set_sliced);
        let set_retains = verdict("TribleSet", width, &set_shared, &set_sliced);

        let (archive_shared, archive_sliced) =
            backend_panel("SuccinctArchive", &archive, &fixture, &cell, repetitions);
        print_summary("SuccinctArchive", width, Arm::Shared, &archive_shared);
        print_summary("SuccinctArchive", width, Arm::Sliced, &archive_sliced);
        let archive_retains = verdict("SuccinctArchive", width, &archive_shared, &archive_sliced);
        if width == 2 || width == 4 {
            primary_cells.push(set_retains);
            primary_cells.push(archive_retains);
        }
    }

    let primary_complete = primary_cells.len() == 4;
    let retains = primary_complete && primary_cells.into_iter().all(|cell| cell);
    println!(
        "anchored_preregistered_verdict retains_latency_advantage={retains} \
         primary_complete={primary_complete} outcome={} \
         criterion=k2_k4_both_backends_first_exact_parent_p50_and_p95",
        if !primary_complete {
            "incomplete"
        } else if retains {
            "retained"
        } else {
            "falsified"
        }
    );
}
