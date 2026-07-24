//! Executable representability and scheduler-parity probes for query constraints.
//!
//! These fixtures deliberately distinguish a canonical structural control key
//! from activation-private affine state. These fixtures explicitly include
//! activation IDs in their query heads, so distinct activation identities
//! remain distinct projected rows even when their other bindings agree. A
//! reducer may deduplicate alternatives *within* each occurrence, and a
//! fixpoint may deduplicate path witnesses *within* each occurrence, but
//! neither may accidentally merge the projected activation identities.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace::core::debug::query::EstimateOverrideConstraint;
use triblespace::core::query::equalityconstraint::EqualityConstraint;
use triblespace::core::query::residual::{
    FormulaScope, ProgramScope, ResidualLowering, ResidualShadowEpoch, ResidualShadowStatus,
};
use triblespace::core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, ProgramAction, ProgramExposure, ProgramRef,
    ProgramRequest, ProposalCoverage, ProposalLayout, Query, RowsView, TypedProgramSpec,
    VariableId, VariableSet,
};
use triblespace::prelude::inlineencodings::GenId;
use triblespace::prelude::*;

mod parity {
    use triblespace::prelude::*;

    attributes! {
        "02F7829F30F4D71ACAB744AD72F49ED6" as edge: inlineencodings::GenId;
        "6F86595AC3C32EDBA80FA5F97FEEA2F1" as activates: inlineencodings::GenId;
    }
}

fn fixture_id(tag: u8) -> Id {
    Id::new([tag; 16]).expect("fixture tags are non-zero")
}

fn insert_edge(set: &mut TribleSet, from: &Id, to: &Id) {
    insert_relation(set, from, &parity::edge, to);
}

fn insert_relation(set: &mut TribleSet, from: &Id, attribute: &Attribute<GenId>, to: &Id) {
    set.insert(&Trible::new::<GenId>(
        ExclusiveId::force_ref(from),
        &attribute.id(),
        &to.to_inline(),
    ));
}

fn multiset<T: Eq + Hash>(items: impl IntoIterator<Item = T>) -> HashMap<T, usize> {
    let mut counts = HashMap::new();
    for item in items {
        *counts.entry(item).or_default() += 1;
    }
    counts
}

fn combined_effects() -> ResidualLowering {
    ResidualLowering::new(FormulaScope::UnionLeaves, ProgramScope::All)
}

/// Transparent equality leaf which observes only the ordinary proposal seam.
///
/// Its typed Program is still the built-in equality Program. Under Production
/// Program policy that Explicit route must be rejected, leaving the certified
/// Formula machine to invoke this wrapper's ordinary action.
struct ObservedExplicitEquality {
    inner: EqualityConstraint,
    a: VariableId,
    b: VariableId,
    ordinary_proposals: Arc<AtomicUsize>,
}

impl ObservedExplicitEquality {
    fn new(a: VariableId, b: VariableId, ordinary_proposals: Arc<AtomicUsize>) -> Self {
        Self {
            inner: EqualityConstraint::new(a, b),
            a,
            b,
            ordinary_proposals,
        }
    }
}

impl Clone for ObservedExplicitEquality {
    fn clone(&self) -> Self {
        Self::new(self.a, self.b, Arc::clone(&self.ordinary_proposals))
    }
}

impl<'a> Constraint<'a> for ObservedExplicitEquality {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn fixed_denotation(&self) -> bool {
        self.inner.fixed_denotation()
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        self.inner.proposal_coverage(variable, bound)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        self.inner.estimate(variable, view, out)
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.ordinary_proposals.fetch_add(1, Ordering::SeqCst);
        self.inner.propose(variable, view, candidates);
    }

    fn confirm(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        self.inner.confirm(variable, view, candidates);
    }

    fn propose_certified_with_receipt(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.ordinary_proposals.fetch_add(1, Ordering::SeqCst);
        self.inner
            .propose_certified_with_receipt(variable, view, candidates)
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        self.inner.satisfied(view)
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        self.inner.residual_confirm_is_page_local()
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        self.inner.residual_program()
    }
}

/// Runs the stable schedulers around one replaceable residual-capability seam.
///
/// Union/RPQ lowering experiments can swap only `run_residual`; the oracle and
/// all surrounding engine comparisons remain unchanged, so capability work
/// does not proliferate public solver entry points or bespoke fixtures.
fn assert_scheduler_matrix<'a, C, P, R, Make>(label: &str, expected: Vec<R>, make_query: Make)
where
    C: Constraint<'a> + Clone + Send + 'a,
    P: Fn(&Binding) -> Option<R> + Clone + Send,
    R: Clone + Debug + Eq + Hash + Send,
    Make: Fn() -> Query<C, P, R>,
{
    let expected = multiset(expected);
    let assert_engine = |engine: &str, actual: Vec<R>| {
        assert_eq!(multiset(actual), expected, "{label}: {engine}");
    };

    assert_engine("sequential", make_query().sequential().collect());
    assert_engine("ordinary residual-default", make_query().collect());
    assert_engine("lazy DAG", make_query().solve_dag_lazy().collect());

    let mut lazy = make_query().solve_dag_lazy();
    let first = lazy.next();
    assert_eq!(
        first.is_some(),
        !expected.is_empty(),
        "{label}: lazy DAG first-result liveness"
    );
    let lazy_all = first.into_iter().chain(lazy).collect();
    assert_engine("lazy DAG after first pull", lazy_all);

    // This is the single integration seam for every fixture below. Composite
    // effects are selected together; adding another structural capability
    // must extend this value rather than create a solver-specific entry point.
    let run_residual = |query: Query<C, P, R>| {
        query
            .solve_residual_state_lazy_with(combined_effects())
            .collect()
    };
    assert_engine("combined residual capability", run_residual(make_query()));

    #[cfg(feature = "parallel")]
    {
        assert_engine(
            "parallel scalar",
            make_query().into_par_iter().collect::<Vec<_>>(),
        );
        assert_engine(
            "parallel DAG",
            make_query().into_par_dag_iter().collect::<Vec<_>>(),
        );
        assert_engine(
            "parallel residual",
            make_query()
                .solve_residual_state_lazy_with(combined_effects())
                .into_par_iter()
                .collect::<Vec<_>>(),
        );
    }
}

/// A finite reducer's branch accumulator is activation-private.
///
/// Three distinct projected activation entities map to values `[a, a, b]`.
/// Unlike duplicate candidates for one parent (which set-valued constraints
/// may legitimately deduplicate), these are three distinct relational rows.
/// Exactly one Union arm is live for each row. Lowering is allowed to cohort
/// their shared structural action, but must neither let a dead row-local arm
/// contribute nor merge the two activation identities that map to `a`.
#[test]
fn union_dead_arms_are_row_local_and_duplicate_activations_are_affine() {
    let a = fixture_id(1);
    let b = fixture_id(2);
    let y_a = fixture_id(11);
    let y_b = fixture_id(12);
    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let y_a_v: Inline<GenId> = (&y_a).to_inline();
    let y_b_v: Inline<GenId> = (&y_b).to_inline();
    let activation_1 = fixture_id(31);
    let activation_2 = fixture_id(32);
    let activation_3 = fixture_id(33);
    let activation_1_v: Inline<GenId> = (&activation_1).to_inline();
    let activation_2_v: Inline<GenId> = (&activation_2).to_inline();
    let activation_3_v: Inline<GenId> = (&activation_3).to_inline();
    let mut activations = TribleSet::new();
    insert_relation(&mut activations, &activation_1, &parity::activates, &a);
    insert_relation(&mut activations, &activation_2, &parity::activates, &a);
    insert_relation(&mut activations, &activation_3, &parity::activates, &b);

    let expected = vec![
        (activation_1_v, a_v, y_a_v),
        (activation_2_v, a_v, y_a_v),
        (activation_3_v, b_v, y_b_v),
    ];
    assert_scheduler_matrix("row-varying-dead-union-arm", expected, || {
        find!((activation: Inline<GenId>, x: Inline<GenId>, y: Inline<GenId>),
            and!(
                pattern!(&activations, [{ ?activation @ parity::activates: ?x }]),
                or!(
                    and!(x.is(a_v), y.is(y_a_v)),
                    and!(x.is(b_v), y.is(y_b_v)),
                ),
            )
        )
    });
}

/// Union confirmation is a finite set reducer, not a pointwise filter: equal
/// candidate values for one parent collapse after the live arms are merged.
#[test]
fn union_confirm_deduplicates_equal_candidates_within_one_parent() {
    let a_v: Inline<GenId> = (&fixture_id(41)).to_inline();
    let candidates = [a_v, a_v];
    let candidates = SortedSlice::new(&candidates).unwrap();

    assert_scheduler_matrix("union-confirm-deduplicates-one-parent", vec![a_v], || {
        find!(x: Inline<GenId>, {
            let mut source = EstimateOverrideConstraint::new(candidates.has(x));
            source.set_estimate(x.index, 0);
            and!(source, or!(x.is(a_v), x.is(a_v)))
        })
    });
}

/// A recursive fixpoint's visited/frontier/result sets are activation-private.
///
/// `a -> c` and `a -> b -> c` are duplicate witnesses for `c`; each projected
/// source activation must emit `c` once. The `a <-> b` cycle makes the Plus
/// closure include its origin via a non-empty path. Finally two distinct
/// activation IDs mapped to `a` must survive independently under SET
/// projection because their full heads differ.
#[test]
fn rpq_plus_deduplicates_witnesses_not_outer_activations() {
    let a = fixture_id(21);
    let b = fixture_id(22);
    let c = fixture_id(23);
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &a, &b);
    insert_edge(&mut graph, &b, &a);
    insert_edge(&mut graph, &a, &c);
    insert_edge(&mut graph, &b, &c);

    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let c_v: Inline<GenId> = (&c).to_inline();
    let activation_1 = fixture_id(51);
    let activation_2 = fixture_id(52);
    let activation_3 = fixture_id(53);
    let activation_1_v: Inline<GenId> = (&activation_1).to_inline();
    let activation_2_v: Inline<GenId> = (&activation_2).to_inline();
    let activation_3_v: Inline<GenId> = (&activation_3).to_inline();
    let mut activations = TribleSet::new();
    insert_relation(&mut activations, &activation_1, &parity::activates, &a);
    insert_relation(&mut activations, &activation_2, &parity::activates, &a);
    insert_relation(&mut activations, &activation_3, &parity::activates, &b);

    let expected = vec![
        (activation_1_v, a_v, a_v),
        (activation_1_v, a_v, b_v),
        (activation_1_v, a_v, c_v),
        (activation_2_v, a_v, a_v),
        (activation_2_v, a_v, b_v),
        (activation_2_v, a_v, c_v),
        (activation_3_v, b_v, a_v),
        (activation_3_v, b_v, b_v),
        (activation_3_v, b_v, c_v),
    ];
    assert_scheduler_matrix("rpq-plus-affine-fixpoint", expected, || {
        find!(
            (
                activation: Inline<GenId>,
                source: Inline<GenId>,
                target: Inline<GenId>
            ),
            and!(
                pattern!(&activations, [{ ?activation @ parity::activates: ?source }]),
                path!(graph.clone(), source parity::edge+ target),
            )
        )
    });
}

/// In contrast to Union, RPQ confirmation is a pointwise reachability filter.
/// It may preserve equal internal candidate occurrences supplied by another
/// constraint, but terminal SET projection collapses their equal raw head.
#[test]
fn rpq_confirm_equal_candidates_collapse_at_projection() {
    let source_id = fixture_id(51);
    let target_id = fixture_id(52);
    let source_v: Inline<GenId> = (&source_id).to_inline();
    let target_v: Inline<GenId> = (&target_id).to_inline();
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &source_id, &target_id);
    let target_candidates = [target_v, target_v];
    let target_candidates = SortedSlice::new(&target_candidates).unwrap();

    assert_scheduler_matrix(
        "rpq-confirm-preserves-candidate-occurrences",
        vec![(source_v, target_v)],
        || {
            find!((source: Inline<GenId>, target: Inline<GenId>), {
                let mut start = EstimateOverrideConstraint::new(source.is(source_v));
                start.set_estimate(source.index, 0);
                let mut targets = EstimateOverrideConstraint::new(target_candidates.has(target));
                targets.set_estimate(target.index, 1);
                and!(
                    start,
                    targets,
                    path!(graph.clone(), source parity::edge+ target),
                )
            })
        },
    );
}

/// Both effect families activate sequentially in one root without sharing
/// affine state. The finite Union proposes `source`; once that value is bound,
/// the eligible `edge+` RPQ delta-proposes `target` for each resulting row.
#[test]
fn finite_union_and_cyclic_rpq_coexist_in_one_root() {
    let a = fixture_id(61);
    let b = fixture_id(62);
    let c = fixture_id(63);
    let d = fixture_id(64);
    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let c_v: Inline<GenId> = (&c).to_inline();
    let d_v: Inline<GenId> = (&d).to_inline();
    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &a, &b);
    insert_edge(&mut graph, &b, &a);
    insert_edge(&mut graph, &b, &c);
    insert_edge(&mut graph, &d, &c);

    let expected = vec![(a_v, a_v), (a_v, b_v), (a_v, c_v), (d_v, c_v)];
    assert_scheduler_matrix("finite-union-plus-cyclic-rpq", expected, || {
        find!(
            (source: Inline<GenId>, target: Inline<GenId>),
            and!(
                or!(source.is(a_v), source.is(d_v)),
                path!(graph.clone(), source parity::edge+ target),
            )
        )
    });
}

/// WholeRoot Formula exposure and Production Program selection are
/// independent controls inside one query.
///
/// Equality advertises an Explicit Program, so the ordinary policy must leave
/// it on the certified action seam. The repeated path advertises a Production
/// Program and must run natively in the same exposed Formula. Two physical
/// source occurrences carry `a`, while `a -> c` has two path witnesses; raw
/// projected SET identity collapses both forms of duplicate without losing
/// any affine parent or retaining action debt at full drain.
#[test]
fn whole_root_production_mixes_ordinary_explicit_equality_with_native_rpq() {
    let a = fixture_id(71);
    let b = fixture_id(72);
    let c = fixture_id(73);
    let d = fixture_id(74);
    let a_v: Inline<GenId> = (&a).to_inline();
    let b_v: Inline<GenId> = (&b).to_inline();
    let c_v: Inline<GenId> = (&c).to_inline();
    let d_v: Inline<GenId> = (&d).to_inline();

    let mut graph = TribleSet::new();
    insert_edge(&mut graph, &a, &b);
    insert_edge(&mut graph, &b, &a);
    insert_edge(&mut graph, &a, &c);
    insert_edge(&mut graph, &b, &c);
    insert_edge(&mut graph, &d, &c);

    let source_values = [a_v, a_v, d_v];
    let sources = SortedSlice::new(&source_values).unwrap();
    let source = Variable::<GenId>::new(0);
    let mirror = Variable::<GenId>::new(1);
    let target = Variable::<GenId>::new(2);
    let ordinary_equality_proposals = Arc::new(AtomicUsize::new(0));
    let equality = ObservedExplicitEquality::new(
        source.index,
        mirror.index,
        Arc::clone(&ordinary_equality_proposals),
    );
    let source_bound = VariableSet::new_singleton(source.index);
    let equality_route = equality
        .inner
        .route(ProgramRequest {
            action: ProgramAction::Propose(mirror.index),
            bound: source_bound,
        })
        .expect("peer-bound equality proposal route");
    assert_eq!(equality_route.exposure, ProgramExposure::Explicit);

    let traversal = path!(graph, mirror parity::edge+ target);
    let mirror_bound = VariableSet::new_singleton(mirror.index);
    let traversal_route = traversal
        .route(ProgramRequest {
            action: ProgramAction::Propose(target.index),
            bound: mirror_bound,
        })
        .expect("source-bound repeated path proposal route");
    assert_eq!(traversal_route.exposure, ProgramExposure::Production);

    let epoch = ResidualShadowEpoch::new();
    let solved = Query::new_projected(
        and!(sources.has(source), equality, traversal),
        [source.index, target.index],
        move |binding| Some((*binding.get(source.index)?, *binding.get(target.index)?)),
    )
    .solve_residual_state_lazy_with(ResidualLowering::WHOLE_ROOT_PRODUCTION)
    .cap(8)
    .start_width(1)
    .growth(2)
    .shadow(epoch)
    .collect_profiled();

    let result_len = solved.results.len();
    let results: BTreeSet<_> = solved.results.into_iter().collect();
    let expected = BTreeSet::from([
        (a_v.raw, a_v.raw),
        (a_v.raw, b_v.raw),
        (a_v.raw, c_v.raw),
        (d_v.raw, c_v.raw),
    ]);
    assert_eq!(results, expected);
    assert_eq!(
        result_len,
        results.len(),
        "public projection must already have raw SET identity"
    );
    assert!(
        ordinary_equality_proposals.load(Ordering::SeqCst) > 0,
        "Production Program scope must defer Explicit equality to its ordinary action"
    );
    assert!(
        solved.stats.delta_transition_pages > 0,
        "the Production RPQ sibling must execute through its native transition Program"
    );
    assert_eq!(solved.shadow.status, ResidualShadowStatus::Closed);
    assert!(!solved.shadow.events.is_empty());
    assert!(
        solved
            .shadow
            .events
            .iter()
            .all(|event| event.completion.is_some()),
        "the fully drained mixed Formula retained affine action debt"
    );
}
