use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, RawInline};
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::residual::{
    ResidualCapabilities, ResidualShadowEpoch, ResidualShadowStatus,
};
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, ResidualDeltaNode,
    ResidualDeltaOutput, ResidualDeltaSeed, RowsView, Variable, VariableId, VariableSet,
};

const START: VariableId = 0;
const END: VariableId = 1;
const OUTER: VariableId = 2;

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
        for (tag, node) in nodes.iter().enumerate() {
            let (edges, next) = match node.continuation {
                0 => (&self.red, 1),
                1 => (&self.blue, 0),
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
                        (
                            u32::try_from(tag).expect("too many custom-delta nodes"),
                            ResidualDeltaOutput {
                                node: ResidualDeltaNode {
                                    source: node.source,
                                    value: target,
                                    continuation: next,
                                },
                                accepted: node.continuation == 1,
                            },
                        )
                    }),
            );
        }
        true
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

fn fixture(evidence: Arc<DeltaEvidence>) -> Root {
    // The 4 -> 1 edge closes a real product-state cycle back to (1, blue).
    let machine = AlternatingClosure {
        relation: [raw(0), raw(2), raw(4)]
            .into_iter()
            .flat_map(|start| [raw(2), raw(4), raw(6)].map(|end| (start, end)))
            .collect(),
        red: vec![
            (raw(0), raw(1)),
            (raw(2), raw(3)),
            (raw(2), raw(5)),
            (raw(4), raw(1)),
        ],
        blue: vec![(raw(1), raw(2)), (raw(3), raw(4)), (raw(5), raw(6))],
        evidence,
    };
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

fn project_end(binding: &Binding) -> Option<RawInline> {
    binding.get(END).copied()
}

fn sorted(mut values: Vec<RawInline>) -> Vec<RawInline> {
    values.sort_unstable();
    values
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
        .solve_residual_state_lazy_with(ResidualCapabilities::default().root_formula().cyclic_rpq())
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
                .solve_residual_state_lazy_with(
                    ResidualCapabilities::default().root_formula().cyclic_rpq(),
                )
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
    let capabilities = ResidualCapabilities::default().root_formula().cyclic_rpq();

    let direct_evidence = Arc::new(DeltaEvidence::default());
    let direct = Query::new(fixture(Arc::clone(&direct_evidence)), project_end)
        .solve_residual_state_lazy_with(capabilities)
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
        .solve_residual_state_lazy_with(capabilities)
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
        shadow.stats.propose_action_pops + shadow.stats.confirm_action_pops,
        "each selected native action site must be observed exactly once"
    );

    #[cfg(feature = "parallel")]
    {
        let parallel_evidence = Arc::new(DeltaEvidence::default());
        let parallel_epoch = ResidualShadowEpoch::new();
        let mut parallel: Vec<_> = Query::new(fixture(Arc::clone(&parallel_evidence)), project_end)
            .solve_residual_state_lazy_with(capabilities)
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
