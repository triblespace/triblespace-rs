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
    Binding, CandidateSink, Constraint, EstimateSink, Query, ResidualDeltaNode,
    ResidualDeltaOutput, ResidualDeltaSeed, ResidualDeltaSourceCursor, ResidualDeltaSourcePage,
    RowsView, Variable, VariableId, VariableSet,
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

#[derive(Default)]
struct DirectSourceEvidence {
    pages: Mutex<Vec<DirectPageTrace>>,
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
        if !self.residual_proposal_source_is_paged(variable, view) {
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
            ResidualDeltaSourceCursor::After(previous) => {
                self.values.partition_point(|value| *value <= previous)
            }
            ResidualDeltaSourceCursor::Offset(_) => {
                panic!("raw-ordered custom source received an ordinal cursor")
            }
        };
        let end = begin.saturating_add(limit).min(self.values.len());
        let page_values = self.values[begin..end].to_vec();
        accepted.extend(page_values.iter().copied());
        let next = (end < self.values.len())
            .then(|| ResidualDeltaSourceCursor::After(self.values[end - 1]));
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
    assert_eq!(
        pages[0].next,
        Some(ResidualDeltaSourceCursor::After(raw(1)))
    );
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
fn custom_direct_source_preserves_affine_bag_and_monotone_growth() {
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
fn custom_direct_source_preserves_duplicate_occurrences_within_one_page() {
    // Both equal occurrences fit in one page. A failure therefore isolates
    // occurrence handling after the hook from value-cursor resumption.
    let values = vec![raw(1), raw(1)];
    let oracle = sorted(
        Query::new(
            direct_source_fixture(values.clone(), Arc::default()),
            project_start,
        )
        .sequential()
        .collect(),
    );
    assert_eq!(oracle, [raw(1), raw(1), raw(1), raw(1)]);

    let evidence = Arc::new(DirectSourceEvidence::default());
    let residual = Query::new(
        direct_source_fixture(values, Arc::clone(&evidence)),
        project_start,
    )
    .solve_residual_state_lazy_with(ResidualLowering::FULL)
    .cap(2)
    .start_width(2)
    .collect_profiled();
    let actual = sorted(residual.results);
    let pages = evidence.pages.lock().expect("direct source trace poisoned");
    assert_eq!(pages.len(), 2, "each affine parent owns one source page");
    assert!(
        pages.iter().all(|page| page.accepted == [raw(1), raw(1)]),
        "each affine activation must hand both duplicate occurrences to the registry"
    );
    drop(pages);
    assert_eq!(actual, oracle);
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

    let ordinary_evidence = Arc::new(DeltaEvidence::default());
    let ordinary =
        sorted(Query::new(fixture(Arc::clone(&ordinary_evidence)), project_end).collect());
    assert!(ordinary_evidence.seeded_roots.load(Ordering::Relaxed) > 0);
    assert!(ordinary_evidence.expanded_nodes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        ordinary_evidence.continuation_mask.load(Ordering::Relaxed) & 0b11,
        0b11,
        "the full-switch ordinary path must run both custom continuation states"
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
    assert_eq!(ordinary, sequential);
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
