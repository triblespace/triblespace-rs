use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::{
    ResidualLowering, ResidualShadowEpoch, ResidualShadowStatus,
};
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, DispatchClass, EstimateSink, ProgramAction,
    ProgramCompletion, ProgramExposure, ProgramGrouping, ProgramKey, ProgramPacing, ProgramRef,
    ProgramRequest, ProgramRoute, ProgramSeedBatch, ProgramStratum, Query, ResidualDeltaNode,
    ResidualDeltaOutput, ResidualDeltaSeed, ResidualDeltaSourceBatch, ResidualDeltaSourceCursor,
    ResidualDeltaSourcePage, RowsView, TypedEffectSink, TypedProgramBatch, TypedProgramSpec,
    TypedResume, TypedSeedSink, Variable, VariableId, VariableSet,
};

const START: VariableId = 0;
const END: VariableId = 1;
const OUTER: VariableId = 2;
const PARENT: VariableId = 3;

type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = Arc<IntersectionConstraint<DynConstraint>>;

fn raw(byte: u8) -> RawInline {
    [byte; 32]
}

#[derive(Default)]
struct DeltaEvidence {
    seeded_roots: AtomicUsize,
    expanded_nodes: AtomicUsize,
    continuation_mask: AtomicUsize,
    fully_bound_satisfied_calls: AtomicUsize,
    support_seeded_roots: AtomicUsize,
    support_expanded_nodes: AtomicUsize,
    support_witnesses: AtomicUsize,
    program_support_pages: Mutex<Vec<ProgramSupportPageTrace>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ProgramSupportPhase {
    Red,
    Blue,
}

impl ProgramSupportPhase {
    fn next(self) -> Self {
        match self {
            Self::Red => Self::Blue,
            Self::Blue => Self::Red,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProgramSupportPageTrace {
    phase: ProgramSupportPhase,
    value: RawInline,
    offset: usize,
    limit: usize,
    examined: usize,
    children: usize,
    supported: bool,
}

/// A deliberately non-RPQ relation: `(red, blue)+` over two in-memory edge
/// tables. Ordinary execution computes the finite relation directly, while
/// residual execution runs the equivalent two-state product machine.
struct AlternatingClosure {
    relation: Vec<(RawInline, RawInline)>,
    red: Vec<(RawInline, RawInline)>,
    blue: Vec<(RawInline, RawInline)>,
    evidence: Arc<DeltaEvidence>,
}

impl AlternatingClosure {
    fn candidates(&self, variable: VariableId, other: Option<RawInline>) -> BTreeSet<RawInline> {
        self.relation
            .iter()
            .filter_map(|&(start, end)| match (variable, other) {
                (END, None) => Some(end),
                (END, Some(bound_start)) if start == bound_start => Some(end),
                (START, None) => Some(start),
                (START, Some(bound_end)) if end == bound_end => Some(start),
                _ => None,
            })
            .collect()
    }

    fn other_column(variable: VariableId, view: &RowsView<'_>) -> Option<usize> {
        match variable {
            START => view.col(END),
            END => view.col(START),
            _ => None,
        }
    }
}

impl<'a> Constraint<'a> for AlternatingClosure {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(START).union(VariableSet::new_singleton(END))
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != START && variable != END {
            return false;
        }
        let other = Self::other_column(variable, view);
        out.extend(view.iter().map(|row| {
            self.candidates(variable, other.map(|column| row[column]))
                .len()
        }));
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != START && variable != END {
            return;
        }
        let other = Self::other_column(variable, view);
        for (parent, row) in view.iter().enumerate() {
            candidates.extend_row(
                u32::try_from(parent).expect("too many custom-delta parents"),
                self.candidates(variable, other.map(|column| row[column])),
            );
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != START && variable != END {
            return;
        }
        let other = Self::other_column(variable, view);
        candidates.retain(|parent, candidate| {
            self.candidates(
                variable,
                other.map(|column| view.row(parent as usize)[column]),
            )
            .contains(candidate)
        });
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let start = view.col(START);
        let end = view.col(END);
        if start.is_some() && end.is_some() {
            self.evidence
                .fully_bound_satisfied_calls
                .fetch_add(view.len(), Ordering::Relaxed);
        }
        view.iter().all(|row| match (start, end) {
            (Some(start), Some(end)) => self.relation.contains(&(row[start], row[end])),
            (Some(start), None) => !self.candidates(END, Some(row[start])).is_empty(),
            (None, Some(end)) => !self.candidates(START, Some(row[end])).is_empty(),
            (None, None) => !self.relation.is_empty(),
        })
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_delta_seeds(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> bool {
        if variable != END || view.col(END).is_some() {
            return false;
        }
        let Some(start) = view.col(START) else {
            return false;
        };
        let before = seeds.len();
        seeds.extend(view.iter().enumerate().map(|(parent, row)| {
            let source = row[start];
            ResidualDeltaSeed {
                parent: u32::try_from(parent).expect("too many custom-delta parents"),
                output: ResidualDeltaOutput {
                    node: ResidualDeltaNode {
                        source: Some(source),
                        value: source,
                        continuation: 0,
                    },
                    accepted: false,
                },
            }
        }));
        self.evidence
            .seeded_roots
            .fetch_add(seeds.len() - before, Ordering::Relaxed);
        true
    }

    fn residual_delta_support_seeds(
        &self,
        view: &RowsView<'_>,
        seeds: &mut Vec<ResidualDeltaSeed>,
    ) -> Option<VariableId> {
        let start = view.col(START)?;
        let end = view.col(END)?;
        let before = seeds.len();
        seeds.extend(view.iter().enumerate().map(|(parent, row)| {
            ResidualDeltaSeed {
                parent: u32::try_from(parent).expect("too many custom-support parents"),
                output: ResidualDeltaOutput {
                    node: ResidualDeltaNode {
                        // Support reuses the node's optional anchor as the
                        // fixed target and reserves continuations 2/3 for its
                        // Boolean traversal. Proposal keeps using 0/1.
                        source: Some(row[end]),
                        value: row[start],
                        continuation: 2,
                    },
                    // `(red, blue)+` has no nullable witness.
                    accepted: false,
                },
            }
        }));
        self.evidence
            .support_seeded_roots
            .fetch_add(seeds.len() - before, Ordering::Relaxed);
        Some(END)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        if variable != END {
            return false;
        }
        self.evidence
            .expanded_nodes
            .fetch_add(nodes.len(), Ordering::Relaxed);
        self.evidence.support_expanded_nodes.fetch_add(
            nodes
                .iter()
                .filter(|node| matches!(node.continuation, 2 | 3))
                .count(),
            Ordering::Relaxed,
        );
        for (tag, node) in nodes.iter().enumerate() {
            let (edges, next, support) = match node.continuation {
                0 => (&self.red, 1, false),
                1 => (&self.blue, 0, false),
                2 => (&self.red, 3, true),
                3 => (&self.blue, 2, true),
                _ => panic!("invalid custom-delta continuation"),
            };
            self.evidence
                .continuation_mask
                .fetch_or(1_usize << node.continuation, Ordering::Relaxed);
            successors.extend(
                edges
                    .iter()
                    .filter(|&&(source, _)| source == node.value)
                    .map(|&(_, target)| {
                        let accepted = if support {
                            node.continuation == 3 && node.source == Some(target)
                        } else {
                            node.continuation == 1
                        };
                        if support && accepted {
                            self.evidence
                                .support_witnesses
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        (
                            u32::try_from(tag).expect("too many custom-delta nodes"),
                            ResidualDeltaOutput {
                                node: ResidualDeltaNode {
                                    source: node.source,
                                    value: target,
                                    continuation: next,
                                },
                                accepted,
                            },
                        )
                    }),
            );
        }
        true
    }
}

/// The same non-RPQ relation with its Boolean traversal expressed through the
/// generic typed Program contract. The ordinary relation remains the oracle;
/// only a fully-bound Formula Support action is claimed by the Program.
struct ProgramAlternatingClosure(AlternatingClosure);

#[derive(Clone, Debug)]
struct ProgramSupportState {
    target: RawInline,
    value: RawInline,
    phase: ProgramSupportPhase,
    offset: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProgramSupportNovelty {
    target: RawInline,
    value: RawInline,
    phase: ProgramSupportPhase,
}

impl TypedProgramSpec for ProgramAlternatingClosure {
    type State = ProgramSupportState;
    type NoveltyKey = ProgramSupportNovelty;
    type Rank = u64;

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        (request.action == ProgramAction::Support
            && request.bound.is_set(START)
            && request.bound.is_set(END))
        .then_some(ProgramRoute {
            key: ProgramKey::new(0),
            variable: END,
            stratum: ProgramStratum::Fixpoint,
            grouping: ProgramGrouping::ParentAtomic,
            completion: ProgramCompletion::PageableOnly,
            exposure: ProgramExposure::Production,
        })
    }

    fn dispatch(&self, state: &Self::State) -> DispatchClass {
        match state.phase {
            ProgramSupportPhase::Red => DispatchClass::new(0),
            ProgramSupportPhase::Blue => DispatchClass::new(1),
        }
    }

    fn pacing(&self, _state: &Self::State) -> ProgramPacing {
        ProgramPacing::Activation
    }

    fn progress(&self, state: &Self::State) -> Self::Rank {
        u64::MAX - u64::try_from(state.offset).expect("custom Program cursor exceeds its rank limb")
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(batch.request.action, ProgramAction::Support);
        let start = batch
            .view
            .col(START)
            .expect("custom Program Support lost its start column");
        let end = batch
            .view
            .col(END)
            .expect("custom Program Support lost its target column");
        self.0
            .evidence
            .support_seeded_roots
            .fetch_add(batch.view.len(), Ordering::Relaxed);
        for (parent, row) in batch.view.iter().enumerate() {
            let state = ProgramSupportState {
                target: row[end],
                value: row[start],
                phase: ProgramSupportPhase::Red,
                offset: 0,
            };
            let novelty = ProgramSupportNovelty {
                target: state.target,
                value: state.value,
                phase: state.phase,
            };
            effects.fixpoint_root(
                u32::try_from(parent).expect("too many custom Program Support parents"),
                state,
                novelty,
                None,
            );
        }
    }

    fn step_typed(
        &self,
        states: &mut Vec<Self::State>,
        batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        assert_eq!(states.len(), batch.view.len());
        self.0
            .evidence
            .support_expanded_nodes
            .fetch_add(states.len(), Ordering::Relaxed);

        for (input, state) in states.drain(..).enumerate() {
            let edges = match state.phase {
                ProgramSupportPhase::Red => &self.0.red,
                ProgramSupportPhase::Blue => &self.0.blue,
            };
            assert!(
                state.offset <= edges.len(),
                "custom Program cursor escaped its edge table"
            );
            let limit = batch.limits[input];
            let page_end = state.offset.saturating_add(limit).min(edges.len());
            let mut examined = 0;
            let mut children = 0;
            let mut supported = false;
            let input_tag =
                u32::try_from(input).expect("too many custom Program inputs in one cohort");

            for &(source, target) in &edges[state.offset..page_end] {
                examined += 1;
                if source != state.value {
                    continue;
                }
                if state.phase == ProgramSupportPhase::Blue && target == state.target {
                    effects.support(input_tag);
                    self.0
                        .evidence
                        .support_witnesses
                        .fetch_add(1, Ordering::Relaxed);
                    supported = true;
                    break;
                }

                let child = ProgramSupportState {
                    target: state.target,
                    value: target,
                    phase: state.phase.next(),
                    offset: 0,
                };
                let novelty = ProgramSupportNovelty {
                    target: child.target,
                    value: child.value,
                    phase: child.phase,
                };
                effects.fixpoint_child(input_tag, child, novelty, None);
                children += 1;
            }

            let next_offset = state.offset + examined;
            let resume = (!supported && next_offset < edges.len()).then(|| {
                TypedResume::Immediate(ProgramSupportState {
                    offset: next_offset,
                    ..state.clone()
                })
            });
            effects.account_transition(examined);
            effects.page(examined, resume);
            self.0
                .evidence
                .program_support_pages
                .lock()
                .expect("custom Program Support trace poisoned")
                .push(ProgramSupportPageTrace {
                    phase: state.phase,
                    value: state.value,
                    offset: state.offset,
                    limit,
                    examined,
                    children,
                    supported,
                });
        }
    }
}

impl<'a> Constraint<'a> for ProgramAlternatingClosure {
    fn variables(&self) -> VariableSet {
        self.0.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.0.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }
}

fn alternating_closure(evidence: Arc<DeltaEvidence>) -> AlternatingClosure {
    alternating_closure_with_terminal(evidence, false)
}

fn alternating_closure_with_terminal(
    evidence: Arc<DeltaEvidence>,
    include_terminal: bool,
) -> AlternatingClosure {
    // The 4 -> 1 edge closes a real product-state cycle back to (1, blue).
    let mut relation: Vec<_> = [raw(0), raw(2), raw(4)]
        .into_iter()
        .flat_map(|start| [raw(2), raw(4), raw(6)].map(|end| (start, end)))
        .collect();
    let mut blue = vec![(raw(1), raw(2)), (raw(3), raw(4)), (raw(5), raw(6))];
    if include_terminal {
        relation.extend(
            [raw(0), raw(2), raw(4)]
                .into_iter()
                .map(|start| (start, raw(7))),
        );
        blue.push((raw(5), raw(7)));
    }
    AlternatingClosure {
        relation,
        red: vec![
            (raw(0), raw(1)),
            (raw(2), raw(3)),
            (raw(2), raw(5)),
            (raw(4), raw(1)),
        ],
        blue,
        evidence,
    }
}

fn program_alternating_closure_with_terminal(
    evidence: Arc<DeltaEvidence>,
    include_terminal: bool,
    disconnected_prefix: usize,
) -> ProgramAlternatingClosure {
    assert!(disconnected_prefix <= 32);
    let mut inner = alternating_closure_with_terminal(evidence, include_terminal);

    // These edges are real but disconnected from every accepted `(red, blue)+`
    // path: red destinations have no blue successors, while blue sources have
    // no red predecessors. Keeping them before the core component forces the
    // typed continuation to expose bounded negative pages without changing the
    // finite relation used by the ordinary oracle.
    let mut red = Vec::with_capacity(disconnected_prefix + inner.red.len());
    red.extend(
        (0..disconnected_prefix).map(|index| (raw(100 + index as u8), raw(140 + index as u8))),
    );
    red.append(&mut inner.red);
    inner.red = red;

    let mut blue = Vec::with_capacity(disconnected_prefix + inner.blue.len());
    blue.extend(
        (0..disconnected_prefix).map(|index| (raw(180 + index as u8), raw(220 + index as u8))),
    );
    blue.append(&mut inner.blue);
    inner.blue = blue;
    ProgramAlternatingClosure(inner)
}

#[derive(Clone)]
struct PageLocalDomain {
    variable: VariableId,
    estimate: usize,
    values: Arc<Vec<RawInline>>,
}

impl<'a> Constraint<'a> for PageLocalDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable {
            return false;
        }
        out.fill(self.estimate, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            for parent in 0..view.len() {
                candidates.extend_row(parent as u32, self.values.iter().copied());
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|_, candidate| self.values.contains(candidate));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectPageTrace {
    parent: RawInline,
    cursor: ResidualDeltaSourceCursor,
    limit: usize,
    accepted: Vec<RawInline>,
    next: Option<ResidualDeltaSourceCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectCohortTrace {
    vars: Vec<VariableId>,
    parents: Vec<RawInline>,
    candidate_mode: bool,
    cursors: Vec<ResidualDeltaSourceCursor>,
    limits: Vec<usize>,
}

#[derive(Default)]
struct DirectSourceEvidence {
    pages: Mutex<Vec<DirectPageTrace>>,
    cohorts: Mutex<Vec<DirectCohortTrace>>,
    expanded_nodes: AtomicUsize,
}

/// A deliberately non-RPQ ordered domain. Sequential execution sees an
/// ordinary finite proposer; residual execution may consume its exact same
/// values as rootless direct source effects, one bounded page at a time.
struct PagedDirectDomain {
    variable: VariableId,
    values: Arc<Vec<RawInline>>,
    evidence: Arc<DirectSourceEvidence>,
}

impl PagedDirectDomain {
    fn source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        if !(variable == self.variable && view.col(PARENT).is_some()) {
            return None;
        }
        assert!(
            candidates.is_none(),
            "a proposal source has no parent candidates"
        );
        assert_eq!(view.len(), 1, "each source activation owns one affine row");
        assert!(
            roots.is_empty(),
            "direct pages must not inherit traversal roots"
        );
        assert!(limit > 0);

        let begin = match cursor {
            ResidualDeltaSourceCursor::Start => 0,
            ResidualDeltaSourceCursor::Offset(offset) => usize::try_from(offset)
                .expect("custom source cursor does not fit this address space"),
            ResidualDeltaSourceCursor::After(_) => {
                panic!("occurrence-bearing custom source received a value cursor")
            }
        };
        assert!(
            begin <= self.values.len(),
            "custom source cursor out of range"
        );
        let end = begin.saturating_add(limit).min(self.values.len());
        let page_values = self.values[begin..end].to_vec();
        accepted.extend(page_values.iter().copied());
        let next = (end < self.values.len()).then(|| {
            ResidualDeltaSourceCursor::Offset(
                u64::try_from(end).expect("custom source cursor exceeds u64"),
            )
        });
        let parent = view.row(0)[view.col(PARENT).expect("paged source parent")];
        self.evidence
            .pages
            .lock()
            .expect("direct source trace poisoned")
            .push(DirectPageTrace {
                parent,
                cursor,
                limit,
                accepted: page_values,
                next,
            });
        Some(ResidualDeltaSourcePage {
            next,
            examined: end - begin,
        })
    }
}

impl<'a> Constraint<'a> for PagedDirectDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != self.variable {
            return false;
        }
        // Keep the hidden affine parent ahead of this source in the plan.
        out.fill(self.values.len() + 32, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            for parent in 0..view.len() {
                candidates.extend_row(parent as u32, self.values.iter().copied());
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|_, candidate| self.values.contains(candidate));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        variable == self.variable && view.col(PARENT).is_some()
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        self.source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_source_pages(
        &self,
        variable: VariableId,
        batch: ResidualDeltaSourceBatch<'_>,
        pages: &mut Vec<ResidualDeltaSourcePage>,
        roots: &mut Vec<(u32, ResidualDeltaOutput)>,
        accepted: &mut Vec<(u32, RawInline)>,
    ) -> bool {
        let parent_column = batch.view.col(PARENT).expect("paged source parent");
        let candidate_mode = batch
            .candidate_sets
            .first()
            .is_some_and(|candidates| candidates.is_some());
        assert!(batch
            .candidate_sets
            .iter()
            .all(|candidates| candidates.is_some() == candidate_mode));
        self.evidence
            .cohorts
            .lock()
            .expect("direct source cohort trace poisoned")
            .push(DirectCohortTrace {
                vars: batch.view.vars.to_vec(),
                parents: batch.view.iter().map(|row| row[parent_column]).collect(),
                candidate_mode,
                cursors: batch.cursors.to_vec(),
                limits: batch.limits.to_vec(),
            });

        for row in 0..batch.view.len() {
            let mut row_roots = Vec::new();
            let mut row_accepted = Vec::new();
            let Some(page) = self.source_page(
                variable,
                &batch.view.row_view(row),
                batch.candidate_sets[row],
                batch.cursors[row],
                batch.limits[row],
                &mut row_roots,
                &mut row_accepted,
            ) else {
                return false;
            };
            let tag = u32::try_from(row).expect("custom source cohort exceeds u32 tags");
            pages.push(page);
            roots.extend(row_roots.into_iter().map(|root| (tag, root)));
            accepted.extend(row_accepted.into_iter().map(|value| (tag, value)));
        }
        true
    }

    fn residual_delta_expand(
        &self,
        _variable: VariableId,
        nodes: &[ResidualDeltaNode],
        _successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        self.evidence
            .expanded_nodes
            .fetch_add(nodes.len(), Ordering::Relaxed);
        false
    }
}

/// The same source law without a native cohort override. This pins the
/// production default that lowers a real compatible block to scalar pages.
struct ScalarPagedDirectDomain(PagedDirectDomain);

impl<'a> Constraint<'a> for ScalarPagedDirectDomain {
    fn variables(&self) -> VariableSet {
        self.0.variables()
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.0.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.0.confirm(variable, view, candidates);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.0.satisfied(view)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.0.residual_confirm_is_page_local()
    }

    fn residual_proposal_source_is_paged(&self, variable: VariableId, view: &RowsView<'_>) -> bool {
        self.0.residual_proposal_source_is_paged(variable, view)
    }

    fn residual_delta_source_page(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: Option<&[RawInline]>,
        cursor: ResidualDeltaSourceCursor,
        limit: usize,
        roots: &mut Vec<ResidualDeltaOutput>,
        accepted: &mut Vec<RawInline>,
    ) -> Option<ResidualDeltaSourcePage> {
        self.0
            .source_page(variable, view, candidates, cursor, limit, roots, accepted)
    }

    fn residual_delta_expand(
        &self,
        variable: VariableId,
        nodes: &[ResidualDeltaNode],
        successors: &mut Vec<(u32, ResidualDeltaOutput)>,
    ) -> bool {
        self.0.residual_delta_expand(variable, nodes, successors)
    }
}

fn direct_source_fixture(values: Vec<RawInline>, evidence: Arc<DirectSourceEvidence>) -> Root {
    assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(PageLocalDomain {
            variable: PARENT,
            estimate: 2,
            values: Arc::new(vec![raw(8), raw(9)]),
        }) as DynConstraint,
        Box::new(PagedDirectDomain {
            variable: START,
            values: Arc::new(values),
            evidence,
        }) as DynConstraint,
    ]))
}

fn scalar_direct_source_fixture(
    values: Vec<RawInline>,
    evidence: Arc<DirectSourceEvidence>,
) -> Root {
    assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(PageLocalDomain {
            variable: PARENT,
            estimate: 2,
            values: Arc::new(vec![raw(8), raw(9)]),
        }) as DynConstraint,
        Box::new(ScalarPagedDirectDomain(PagedDirectDomain {
            variable: START,
            values: Arc::new(values),
            evidence,
        })) as DynConstraint,
    ]))
}

fn fixture(evidence: Arc<DeltaEvidence>) -> Root {
    let machine = alternating_closure(evidence);
    let arm = Box::new(IntersectionConstraint::new(vec![
        Box::new(machine) as DynConstraint,
        Box::new(PageLocalDomain {
            variable: END,
            estimate: 100,
            values: Arc::new(vec![raw(2), raw(6)]),
        }) as DynConstraint,
    ])) as DynConstraint;
    Arc::new(IntersectionConstraint::new(vec![
        Box::new(Variable::<UnknownInline>::new(START).is(Inline::<UnknownInline>::new(raw(0))))
            as DynConstraint,
        Box::new(PageLocalDomain {
            variable: OUTER,
            estimate: 2,
            values: Arc::new(vec![raw(8), raw(9)]),
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![arm])) as DynConstraint,
    ]))
}

/// Places the custom transition machine under both recursive Boolean
/// connectives. The inner OR has a deliberately impossible finite arm; the
/// guarded outer arm is an AND, and its sibling keeps the outer OR live when
/// transition Support proves false. PARENT is deliberately projected away so
/// its two affine rows become observable bag multiplicity.
fn recursive_support_fixture(
    evidence: Arc<DeltaEvidence>,
    include_terminal: bool,
    guarded_value: RawInline,
    sibling_value: RawInline,
) -> Root {
    let target = raw(7);
    let start = Variable::<UnknownInline>::new(START);
    let end = Variable::<UnknownInline>::new(END);
    let impossible = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(250)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(raw(251)))) as DynConstraint,
    ])) as DynConstraint;
    let inner_or = Box::new(UnionConstraint::new(vec![
        Box::new(alternating_closure_with_terminal(
            evidence,
            include_terminal,
        )) as DynConstraint,
        impossible,
    ])) as DynConstraint;
    let guarded = Box::new(IntersectionConstraint::new(vec![
        inner_or,
        Box::new(PageLocalDomain {
            variable: OUTER,
            estimate: 32,
            values: Arc::new(vec![guarded_value]),
        }) as DynConstraint,
    ])) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(0)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(target))) as DynConstraint,
        Box::new(PageLocalDomain {
            variable: OUTER,
            estimate: 64,
            values: Arc::new(vec![sibling_value]),
        }) as DynConstraint,
    ])) as DynConstraint;

    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(0)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(target))) as DynConstraint,
        Box::new(PageLocalDomain {
            variable: PARENT,
            estimate: 2,
            values: Arc::new(vec![raw(8), raw(9)]),
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![guarded, sibling])) as DynConstraint,
    ]))
}

fn program_recursive_support_fixture(
    evidence: Arc<DeltaEvidence>,
    include_terminal: bool,
    disconnected_prefix: usize,
    parent_count: usize,
    guarded_value: RawInline,
    sibling_value: RawInline,
) -> Root {
    assert!(parent_count > 0);
    let target = raw(7);
    let start = Variable::<UnknownInline>::new(START);
    let end = Variable::<UnknownInline>::new(END);
    let impossible = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(250)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(raw(251)))) as DynConstraint,
    ])) as DynConstraint;
    let inner_or = Box::new(UnionConstraint::new(vec![
        Box::new(program_alternating_closure_with_terminal(
            evidence,
            include_terminal,
            disconnected_prefix,
        )) as DynConstraint,
        impossible,
    ])) as DynConstraint;
    let guarded = Box::new(IntersectionConstraint::new(vec![
        inner_or,
        Box::new(PageLocalDomain {
            variable: OUTER,
            estimate: 32,
            values: Arc::new(vec![guarded_value]),
        }) as DynConstraint,
    ])) as DynConstraint;
    let sibling = Box::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(0)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(target))) as DynConstraint,
        Box::new(PageLocalDomain {
            variable: OUTER,
            estimate: 64,
            values: Arc::new(vec![sibling_value]),
        }) as DynConstraint,
    ])) as DynConstraint;
    let parents = (0..parent_count)
        .map(|parent| raw(20 + parent as u8))
        .collect();

    Arc::new(IntersectionConstraint::new(vec![
        Box::new(start.is(Inline::<UnknownInline>::new(raw(0)))) as DynConstraint,
        Box::new(end.is(Inline::<UnknownInline>::new(target))) as DynConstraint,
        Box::new(PageLocalDomain {
            variable: PARENT,
            estimate: parent_count,
            values: Arc::new(parents),
        }) as DynConstraint,
        Box::new(UnionConstraint::new(vec![guarded, sibling])) as DynConstraint,
    ]))
}

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

fn project_start(binding: &Binding) -> Option<RawInline> {
    binding.get(START).copied()
}

fn project_outer(binding: &Binding) -> Option<RawInline> {
    binding.get(OUTER).copied()
}

fn sorted(mut values: Vec<RawInline>) -> Vec<RawInline> {
    values.sort_unstable();
    values
}

fn assert_recursive_support_case(include_terminal: bool) -> Vec<RawInline> {
    let reachable = include_terminal;
    let guarded_value = raw(10);
    let sibling_value = raw(11);
    let expected = if reachable {
        sorted(vec![
            guarded_value,
            guarded_value,
            sibling_value,
            sibling_value,
        ])
    } else {
        sorted(vec![sibling_value, sibling_value])
    };

    let oracle_evidence = Arc::new(DeltaEvidence::default());
    let oracle = sorted(
        Query::new(
            recursive_support_fixture(
                Arc::clone(&oracle_evidence),
                include_terminal,
                guarded_value,
                sibling_value,
            ),
            project_outer,
        )
        .sequential()
        .collect(),
    );
    assert_eq!(oracle, expected);
    assert!(
        oracle_evidence
            .fully_bound_satisfied_calls
            .load(Ordering::Relaxed)
            > 0,
        "the finite oracle must exercise the ordinary fully-bound relation"
    );
    assert_eq!(
        oracle_evidence.support_seeded_roots.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        oracle_evidence
            .support_expanded_nodes
            .load(Ordering::Relaxed),
        0
    );

    let residual_evidence = Arc::new(DeltaEvidence::default());
    let residual = Query::new(
        recursive_support_fixture(
            Arc::clone(&residual_evidence),
            include_terminal,
            guarded_value,
            sibling_value,
        ),
        project_outer,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1)
    .collect_profiled();
    let residual_results = sorted(residual.results);

    assert_eq!(residual_results, oracle);
    assert_eq!(residual_results, expected);
    assert_eq!(
        residual_evidence
            .fully_bound_satisfied_calls
            .load(Ordering::Relaxed),
        0,
        "native Support must replace the synchronous fully-bound oracle"
    );
    assert_eq!(
        residual_evidence
            .support_seeded_roots
            .load(Ordering::Relaxed),
        2,
        "each projected-away PARENT row owns one affine Support activation"
    );
    assert!(
        residual_evidence
            .support_expanded_nodes
            .load(Ordering::Relaxed)
            > 0,
        "the Boolean guard must execute the custom transition machine"
    );
    assert_eq!(
        residual_evidence.continuation_mask.load(Ordering::Relaxed) & 0b1100,
        0b1100,
        "both custom Support continuation states must execute"
    );
    assert!(
        residual.stats.support_action_pops > 0,
        "the recursive finite formula must expose a Support action"
    );
    if reachable {
        assert_eq!(
            residual_evidence.support_witnesses.load(Ordering::Relaxed),
            2,
            "each affine parent must publish its own witness"
        );
    } else {
        assert_eq!(
            residual_evidence.support_witnesses.load(Ordering::Relaxed),
            0,
            "an unreachable guard may become false only after quiescence"
        );
    }
    residual_results
}

#[test]
fn custom_support_recursive_formula_is_affine_and_monotone() {
    let unreachable = assert_recursive_support_case(false);
    let reachable_extension = assert_recursive_support_case(true);
    let mut extension_only = reachable_extension;
    for inherited in unreachable {
        let position = extension_only
            .iter()
            .position(|value| *value == inherited)
            .expect("adding one edge must not remove an inherited bag occurrence");
        extension_only.remove(position);
    }
    assert_eq!(extension_only, vec![raw(10), raw(10)]);
}

fn assert_program_support_case(
    include_terminal: bool,
    parent_count: usize,
    disconnected_prefix: usize,
) -> Vec<RawInline> {
    let guarded_value = raw(10);
    let sibling_value = raw(11);
    let mut expected = vec![sibling_value; parent_count];
    if include_terminal {
        expected.extend(std::iter::repeat_n(guarded_value, parent_count));
    }
    expected.sort_unstable();

    let oracle_evidence = Arc::new(DeltaEvidence::default());
    let oracle = sorted(
        Query::new(
            program_recursive_support_fixture(
                Arc::clone(&oracle_evidence),
                include_terminal,
                disconnected_prefix,
                parent_count,
                guarded_value,
                sibling_value,
            ),
            project_outer,
        )
        .sequential()
        .collect(),
    );
    assert_eq!(oracle, expected);
    assert!(
        oracle_evidence
            .fully_bound_satisfied_calls
            .load(Ordering::Relaxed)
            > 0,
        "the finite oracle must use the ordinary relation"
    );
    assert_eq!(
        oracle_evidence.support_seeded_roots.load(Ordering::Relaxed),
        0
    );

    let residual_evidence = Arc::new(DeltaEvidence::default());
    let residual = Query::new(
        program_recursive_support_fixture(
            Arc::clone(&residual_evidence),
            include_terminal,
            disconnected_prefix,
            parent_count,
            guarded_value,
            sibling_value,
        ),
        project_outer,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(32)
    .start_width(1)
    .collect_profiled();
    let actual = sorted(residual.results);
    assert_eq!(actual, oracle);
    assert_eq!(actual, expected);
    assert_eq!(
        residual_evidence
            .fully_bound_satisfied_calls
            .load(Ordering::Relaxed),
        0,
        "typed Program Support must replace the synchronous fully-bound oracle"
    );
    assert_eq!(
        residual_evidence
            .support_seeded_roots
            .load(Ordering::Relaxed),
        parent_count,
        "each duplicate outer parent needs an independent Program activation"
    );
    assert_eq!(
        residual_evidence.support_witnesses.load(Ordering::Relaxed),
        if include_terminal { parent_count } else { 0 }
    );
    assert!(residual.stats.support_action_pops > 0);
    assert!(residual.stats.delta_transition_pages > 0);
    assert!(residual.stats.delta_transition_candidates_examined > 0);

    let pages = residual_evidence
        .program_support_pages
        .lock()
        .expect("custom Program Support trace poisoned");
    assert!(!pages.is_empty());
    assert!(pages.iter().all(|page| page.examined <= page.limit));
    assert!(
        pages.iter().any(|page| page.offset > 0),
        "the typed traversal never retained a live page cursor"
    );
    assert!(
        pages.iter().any(|page| page.limit > 1),
        "negative Program pages never received geometric work growth"
    );
    assert!(
        pages.iter().any(|page| page.children > 0),
        "the Program never emitted an affine transition child"
    );
    if include_terminal {
        assert!(pages.iter().any(|page| page.supported));
    } else {
        assert!(pages.iter().all(|page| !page.supported));
        assert!(residual.stats.width_increases > 0);
    }
    actual
}

#[test]
fn generic_program_support_is_affine_monotone_and_geometrically_paged() {
    let parent_count = 4;
    let inherited = assert_program_support_case(false, parent_count, 15);
    let mut extension = assert_program_support_case(true, parent_count, 15);
    for value in inherited {
        let position = extension
            .iter()
            .position(|candidate| *candidate == value)
            .expect("monotone graph growth removed an inherited affine occurrence");
        extension.remove(position);
    }
    assert_eq!(extension, vec![raw(10); parent_count]);
}

#[test]
fn live_program_support_clone_is_exact_and_matches_rayon_workers() {
    let parent_count = 16;
    let guarded_value = raw(10);
    let sibling_value = raw(11);
    let mut expected = vec![guarded_value; parent_count];
    expected.extend(std::iter::repeat_n(sibling_value, parent_count));
    expected.sort_unstable();

    let evidence = Arc::new(DeltaEvidence::default());
    let mut query = Query::new(
        program_recursive_support_fixture(
            Arc::clone(&evidence),
            true,
            7,
            parent_count,
            guarded_value,
            sibling_value,
        ),
        project_outer,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1);
    let first = query
        .next()
        .expect("the positive Program formula has a nonempty affine bag");
    assert!(query.stats().delta_transition_pages > 0);
    assert!(
        evidence.support_seeded_roots.load(Ordering::Relaxed) > 0,
        "the clone point preceded typed Program admission"
    );
    assert!(
        evidence.support_witnesses.load(Ordering::Relaxed) < parent_count,
        "the clone point no longer retained a live Program Support remainder"
    );

    let clone = query.clone();
    let remainder: Vec<_> = query.collect();
    let cloned_remainder: Vec<_> = clone.collect();
    assert_eq!(
        cloned_remainder, remainder,
        "cloning live typed Support changed the exact affine remainder"
    );
    let mut reconstructed: Vec<_> = std::iter::once(first).chain(remainder).collect();
    reconstructed.sort_unstable();
    assert_eq!(reconstructed, expected);

    #[cfg(feature = "parallel")]
    for workers in [1, 4] {
        let evidence = Arc::new(DeltaEvidence::default());
        let query = Query::new(
            program_recursive_support_fixture(
                Arc::clone(&evidence),
                true,
                7,
                parent_count,
                guarded_value,
                sibling_value,
            ),
            project_outer,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(2)
        .start_width(1);
        let mut actual = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap()
            .install(|| query.into_par_iter().collect::<Vec<_>>());
        actual.sort_unstable();
        assert_eq!(actual, expected, "workers={workers}");
        assert_eq!(
            evidence.support_seeded_roots.load(Ordering::Relaxed),
            parent_count,
            "workers={workers}"
        );
        assert!(
            evidence.support_expanded_nodes.load(Ordering::Relaxed) > 0,
            "workers={workers}"
        );
    }
}

#[test]
fn custom_direct_source_first_pull_is_rootless_and_drop_cancels_the_frontier() {
    let evidence = Arc::new(DirectSourceEvidence::default());
    let mut query = Query::new(
        direct_source_fixture(vec![raw(1), raw(2), raw(3), raw(4)], Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1);

    assert_eq!(query.next(), Some(raw(1)));
    let pages = evidence
        .pages
        .lock()
        .expect("direct source trace poisoned")
        .clone();
    assert_eq!(pages.len(), 1);
    assert!(matches!(pages[0].parent, value if value == raw(8) || value == raw(9)));
    assert_eq!(pages[0].cursor, ResidualDeltaSourceCursor::Start);
    assert_eq!(pages[0].limit, 1);
    assert_eq!(pages[0].accepted, [raw(1)]);
    assert_eq!(pages[0].next, Some(ResidualDeltaSourceCursor::Offset(1)));
    assert_eq!(query.stats().delta_source_pages, 1);
    assert_eq!(query.stats().delta_source_candidates_examined, 1);
    assert_eq!(query.stats().delta_source_roots, 0);
    assert_eq!(
        evidence.expanded_nodes.load(Ordering::Relaxed),
        0,
        "a rootless direct page must resume without traversal lineage"
    );

    drop(query);
    assert_eq!(
        evidence
            .pages
            .lock()
            .expect("direct source trace poisoned")
            .as_slice(),
        pages,
        "dropping the query must leave the ordered source frontier untouched"
    );
    assert_eq!(evidence.expanded_nodes.load(Ordering::Relaxed), 0);
}

fn assert_direct_source_case(values: Vec<RawInline>, expected: Vec<RawInline>) -> Vec<RawInline> {
    let oracle_evidence = Arc::new(DirectSourceEvidence::default());
    let oracle = sorted(
        Query::new(
            direct_source_fixture(values.clone(), Arc::clone(&oracle_evidence)),
            project_start,
        )
        .sequential()
        .collect(),
    );
    assert_eq!(oracle, expected);
    assert!(
        oracle_evidence
            .pages
            .lock()
            .expect("direct source trace poisoned")
            .is_empty(),
        "the sequential oracle must use the ordinary proposer"
    );

    let residual_evidence = Arc::new(DirectSourceEvidence::default());
    let residual = Query::new(
        direct_source_fixture(values, Arc::clone(&residual_evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1)
    .collect_profiled();
    let actual = sorted(residual.results);
    assert_eq!(actual, oracle);
    assert_eq!(actual, expected);
    assert_eq!(residual.stats.delta_source_roots, 0);
    assert_eq!(residual_evidence.expanded_nodes.load(Ordering::Relaxed), 0);

    let pages = residual_evidence
        .pages
        .lock()
        .expect("direct source trace poisoned");
    assert!(pages.iter().all(|page| page.accepted.len() <= page.limit));
    let affine_values: Vec<_> = actual.chunks_exact(2).map(|pair| pair[0]).collect();
    for parent in [raw(8), raw(9)] {
        let parent_values: Vec<_> = pages
            .iter()
            .filter(|page| page.parent == parent)
            .flat_map(|page| page.accepted.iter().copied())
            .collect();
        assert_eq!(parent_values, affine_values);
    }
    actual
}

#[test]
fn custom_direct_source_preserves_distinct_full_bindings_and_monotone_growth() {
    let inherited =
        assert_direct_source_case(vec![raw(1), raw(3)], vec![raw(1), raw(1), raw(3), raw(3)]);
    let mut extension = assert_direct_source_case(
        vec![raw(1), raw(2), raw(3)],
        vec![raw(1), raw(1), raw(2), raw(2), raw(3), raw(3)],
    );
    for value in inherited {
        let position = extension
            .iter()
            .position(|candidate| *candidate == value)
            .expect("monotone source growth must preserve every affine occurrence");
        extension.remove(position);
    }
    assert_eq!(extension, [raw(2), raw(2)]);
}

#[test]
fn custom_direct_source_duplicate_occurrences_collapse_per_parent_at_width_one() {
    // Equal occurrences straddle pages deliberately. The ordinal cursor is
    // what makes their two positions representable despite equal values.
    let values = vec![raw(1), raw(1)];
    let oracle = sorted(
        Query::new(
            direct_source_fixture(values.clone(), Arc::default()),
            project_start,
        )
        .sequential()
        .collect(),
    );
    // The two parents remain distinct complete bindings, while equal source
    // occurrences under the same parent share one raw projection key.
    assert_eq!(oracle, [raw(1), raw(1)]);

    let evidence = Arc::new(DirectSourceEvidence::default());
    let residual = Query::new(
        direct_source_fixture(values, Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1)
    .collect_profiled();
    let actual = sorted(residual.results);
    assert_eq!(actual, oracle);
    let pages = evidence.pages.lock().expect("direct source trace poisoned");
    assert_eq!(pages.len(), 4, "each affine parent owns two source pages");
    for parent in [raw(8), raw(9)] {
        let accepted: Vec<_> = pages
            .iter()
            .filter(|page| page.parent == parent)
            .flat_map(|page| page.accepted.iter().copied())
            .collect();
        assert_eq!(accepted, [raw(1), raw(1)]);
    }
}

#[test]
fn custom_direct_source_batches_compatible_parents_with_one_global_budget() {
    let values = vec![raw(1), raw(2), raw(3)];
    let oracle = sorted(
        Query::new(
            direct_source_fixture(values.clone(), Arc::default()),
            project_start,
        )
        .sequential()
        .collect(),
    );
    let conservative = sorted(
        Query::new(
            direct_source_fixture(values.clone(), Arc::default()),
            project_start,
        )
        .solve_residual_state_lazy()
        .collect(),
    );
    assert_eq!(conservative, oracle);
    let evidence = Arc::new(DirectSourceEvidence::default());
    let residual = Query::new(
        direct_source_fixture(values, Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(3)
    .start_width(3)
    .collect_profiled();

    assert_eq!(sorted(residual.results), oracle);
    assert_eq!(residual.stats.max_delta_source_cohort, 2);
    assert_eq!(residual.stats.delta_source_cohorts, 2);
    assert_eq!(residual.stats.delta_source_pages, 4);
    let cohorts = evidence
        .cohorts
        .lock()
        .expect("direct source cohort trace poisoned");
    assert_eq!(cohorts.len(), 2);
    for (page, cohort) in cohorts.iter().enumerate() {
        assert_eq!(cohort.vars, [PARENT]);
        assert_eq!(cohort.parents.len(), 2);
        assert!(!cohort.candidate_mode);
        assert_eq!(cohort.limits, [2, 1]);
        assert_eq!(cohort.limits.iter().sum::<usize>(), 3);
        match page {
            0 => assert!(cohort
                .cursors
                .iter()
                .all(|cursor| *cursor == ResidualDeltaSourceCursor::Start)),
            1 => {
                for (&parent, &cursor) in cohort.parents.iter().zip(&cohort.cursors) {
                    let initial = cohorts[0]
                        .parents
                        .iter()
                        .position(|candidate| *candidate == parent)
                        .expect("affine parent disappeared between source cohorts");
                    assert_eq!(
                        cursor,
                        ResidualDeltaSourceCursor::Offset(cohorts[0].limits[initial] as u64)
                    );
                }
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn default_scalar_source_pages_consume_one_real_compatible_cohort() {
    let values = vec![raw(1), raw(2), raw(3)];
    let oracle = sorted(
        Query::new(
            scalar_direct_source_fixture(values.clone(), Arc::default()),
            project_start,
        )
        .sequential()
        .collect(),
    );
    let evidence = Arc::new(DirectSourceEvidence::default());
    let residual = Query::new(
        scalar_direct_source_fixture(values, Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(3)
    .start_width(3)
    .collect_profiled();

    assert_eq!(sorted(residual.results), oracle);
    assert_eq!(residual.stats.delta_source_cohorts, 2);
    assert_eq!(residual.stats.max_delta_source_cohort, 2);
    assert_eq!(residual.stats.delta_source_pages, 4);
    assert!(
        evidence
            .cohorts
            .lock()
            .expect("direct source cohort trace poisoned")
            .is_empty(),
        "the scalar leaf must use the trait default rather than a native override"
    );
    let pages = evidence.pages.lock().expect("direct source trace poisoned");
    assert_eq!(pages.len(), 4);
    assert!(pages
        .chunks_exact(2)
        .all(|cohort| cohort.iter().map(|page| page.limit).eq([2, 1])));
}

#[test]
fn live_custom_direct_source_clones_exactly_and_matches_rayon_workers() {
    let values = vec![raw(1), raw(2), raw(3), raw(4)];
    let mut expected: Vec<_> = values.iter().flat_map(|value| [*value, *value]).collect();
    expected.sort_unstable();

    let evidence = Arc::new(DirectSourceEvidence::default());
    let mut query = Query::new(
        direct_source_fixture(values.clone(), Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(1)
    .start_width(1);
    let first = query
        .next()
        .expect("the direct source has eight affine occurrences");
    assert_eq!(
        query.stats().delta_source_candidates_examined,
        1,
        "the clone point must retain a genuinely suspended source frontier"
    );
    let clone = query.clone();
    let remainder: Vec<_> = query.collect();
    let cloned_remainder: Vec<_> = clone.collect();
    assert_eq!(
        cloned_remainder, remainder,
        "a live direct-source clone changed the exact affine remainder"
    );
    let mut reconstructed: Vec<_> = std::iter::once(first).chain(remainder).collect();
    reconstructed.sort_unstable();
    assert_eq!(reconstructed, expected);
    assert_eq!(evidence.expanded_nodes.load(Ordering::Relaxed), 0);

    #[cfg(feature = "parallel")]
    for workers in [1, 4] {
        let evidence = Arc::new(DirectSourceEvidence::default());
        let query = Query::new(
            direct_source_fixture(values.clone(), Arc::clone(&evidence)),
            project_start,
        )
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(1)
        .start_width(1);
        let mut actual = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap()
            .install(|| query.into_par_iter().collect::<Vec<_>>());
        actual.sort_unstable();
        assert_eq!(actual, expected, "workers={workers}");
        assert!(
            !evidence
                .pages
                .lock()
                .expect("direct source trace poisoned")
                .is_empty(),
            "workers={workers}"
        );
        assert_eq!(
            evidence.expanded_nodes.load(Ordering::Relaxed),
            0,
            "workers={workers}"
        );
    }
}

#[test]
fn custom_cyclic_delta_composes_with_recursive_root_formula() {
    let sequential_evidence = Arc::new(DeltaEvidence::default());
    let sequential = sorted(
        Query::new(fixture(Arc::clone(&sequential_evidence)), project_end)
            .sequential()
            .collect(),
    );
    assert_eq!(sequential_evidence.seeded_roots.load(Ordering::Relaxed), 0);
    assert_eq!(
        sequential_evidence.expanded_nodes.load(Ordering::Relaxed),
        0
    );

    let production_evidence = Arc::new(DeltaEvidence::default());
    let production = sorted(
        Query::new(fixture(Arc::clone(&production_evidence)), project_end).collect(),
    );
    assert_eq!(
        production_evidence.seeded_roots.load(Ordering::Relaxed),
        0,
        "production regions leave explicit-only recursive formulas fused"
    );
    assert_eq!(
        production_evidence.expanded_nodes.load(Ordering::Relaxed),
        0
    );

    let residual_evidence = Arc::new(DeltaEvidence::default());
    let residual = Query::new(fixture(Arc::clone(&residual_evidence)), project_end)
        .solve_residual_state_lazy_with(ResidualLowering::FULL)
        .cap(2)
        .start_width(1)
        .collect_profiled();
    let residual = sorted(residual.results);

    let expected = sorted(vec![raw(2), raw(2), raw(6), raw(6)]);
    assert_eq!(sequential, expected);
    assert_eq!(production, sequential);
    assert_eq!(residual, sequential);
    assert!(residual_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
    assert!(residual_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        residual_evidence.continuation_mask.load(Ordering::Relaxed) & 0b11,
        0b11,
        "both constraint-defined continuation states must execute"
    );

    #[cfg(feature = "parallel")]
    {
        let parallel_evidence = Arc::new(DeltaEvidence::default());
        let parallel = sorted(
            Query::new(fixture(Arc::clone(&parallel_evidence)), project_end)
                .solve_residual_state_lazy_with(ResidualLowering::FULL)
                .cap(4)
                .start_width(4)
                .into_par_iter()
                .collect(),
        );
        assert_eq!(parallel, sequential);
        assert!(parallel_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
        assert!(parallel_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
    }
}

#[test]
fn custom_cyclic_delta_shadow_preserves_native_execution() {
    let lowering = ResidualLowering::FULL;

    let direct_evidence = Arc::new(DeltaEvidence::default());
    let direct = Query::new(fixture(Arc::clone(&direct_evidence)), project_end)
        .solve_residual_state_lazy_with(lowering)
        .cap(2)
        .start_width(1)
        .collect_profiled();
    assert!(direct_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
    assert!(direct_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        direct_evidence.continuation_mask.load(Ordering::Relaxed) & 0b11,
        0b11,
        "the direct run must execute both constraint-defined continuation states"
    );

    let shadow_evidence = Arc::new(DeltaEvidence::default());
    let epoch = ResidualShadowEpoch::new();
    let shadow = Query::new(fixture(Arc::clone(&shadow_evidence)), project_end)
        .solve_residual_state_lazy_with(lowering)
        .cap(2)
        .start_width(1)
        .shadow(epoch.clone())
        .collect_profiled();

    assert_eq!(shadow.results, direct.results);
    assert_eq!(shadow.stats, direct.stats);
    assert!(shadow_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
    assert!(shadow_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        shadow_evidence.continuation_mask.load(Ordering::Relaxed) & 0b11,
        0b11,
        "shadowing must not replace native delta execution with ordinary formula actions"
    );
    assert_eq!(shadow.shadow.status, ResidualShadowStatus::Closed);
    assert_eq!(epoch.status(), ResidualShadowStatus::Closed);
    assert_eq!(
        shadow.shadow.events.len(),
        shadow.stats.support_action_pops
            + shadow.stats.propose_action_pops
            + shadow.stats.confirm_action_pops,
        "each selected native action site must be observed exactly once"
    );

    #[cfg(feature = "parallel")]
    {
        let parallel_evidence = Arc::new(DeltaEvidence::default());
        let parallel_epoch = ResidualShadowEpoch::new();
        let mut parallel: Vec<_> = Query::new(fixture(Arc::clone(&parallel_evidence)), project_end)
            .solve_residual_state_lazy_with(lowering)
            .cap(4)
            .start_width(4)
            .shadow(parallel_epoch.clone())
            .into_par_iter()
            .collect();
        let mut expected = direct.results;
        parallel.sort_unstable();
        expected.sort_unstable();
        assert_eq!(parallel, expected);
        assert!(parallel_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
        assert!(parallel_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
        assert_eq!(
            parallel_evidence.continuation_mask.load(Ordering::Relaxed) & 0b11,
            0b11,
            "parallel shadow shards must retain both native continuation states"
        );
        assert_eq!(parallel_epoch.status(), ResidualShadowStatus::Closed);
        let snapshot = parallel_epoch.snapshot();
        assert!(!snapshot.events.is_empty());
        assert!(
            snapshot
                .events
                .iter()
                .all(|event| event.completion.is_some()),
            "a normally drained parallel epoch left an action event live"
        );
    }
}
