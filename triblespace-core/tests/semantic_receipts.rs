use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use triblespace_core::blob::encodings::succinctarchive::{OrderedUniverse, SuccinctArchive};
use triblespace_core::debug::query::{DebugConstraint, EstimateOverrideConstraint};
use triblespace_core::id::Id;
use triblespace_core::inline::encodings::genid::GenId;
use triblespace_core::inline::encodings::UnknownInline;
use triblespace_core::inline::{Inline, IntoInline, RawInline};
use triblespace_core::query::constantconstraint::ConstantConstraint;
use triblespace_core::query::equalityconstraint::EqualityConstraint;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::rangeconstraint::InlineRange;
use triblespace_core::query::unionconstraint::UnionConstraint;
use triblespace_core::query::{
    residual::{FormulaScope, ProgramScope, ResidualLowering},
    Binding, CandidateSink, Constraint, ConstraintShape, DispatchClass, EstimateSink, PathOp,
    ProgramAction, ProgramCompletion, ProgramExposure, ProgramGrouping, ProgramKey, ProgramRef,
    ProgramRequest, ProgramRoute, ProgramSeedBatch, ProgramStratum, ProposalCoverage, Query,
    RegularPathConstraint, RowsView, TriblePattern, TypedEffectSink, TypedProgramBatch,
    TypedProgramSpec, TypedSeedSink, Variable, VariableId, VariableSet,
};
use triblespace_core::trible::{Trible, TribleSet};

const X: VariableId = 0;
const Y: VariableId = 1;
const MEMBER: RawInline = [0x31; 32];
const FALSE_POSITIVE: RawInline = [0x72; 32];

#[derive(Clone, Copy)]
struct ReceiptDomain {
    variable: VariableId,
    fixed: bool,
    coverage: ProposalCoverage,
}

impl ReceiptDomain {
    fn new(fixed: bool, coverage: ProposalCoverage) -> Self {
        Self {
            variable: X,
            fixed,
            coverage,
        }
    }
}

impl Constraint<'_> for ReceiptDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn fixed_denotation(&self) -> bool {
        self.fixed
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if self.fixed && variable == self.variable && !bound.is_set(variable) {
            self.coverage
        } else {
            ProposalCoverage::None
        }
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
        out.fill(2, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != self.variable {
            return;
        }
        for row in 0..view.len() as u32 {
            // Exactness is a support property: duplicate proposal occurrences
            // do not weaken an Exact receipt.
            candidates.push(row, MEMBER);
            candidates.push(row, MEMBER);
            if self.coverage == ProposalCoverage::Covering {
                candidates.push(row, FALSE_POSITIVE);
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
            candidates.retain(|_, value| *value == MEMBER);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| row[column] == MEMBER))
    }
}

/// A coherent relation which deliberately opts into neither receipt.
struct DefaultReceiptDomain;

impl Constraint<'_> for DefaultReceiptDomain {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != X {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            for row in 0..view.len() as u32 {
                candidates.push(row, MEMBER);
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            candidates.retain(|_, value| *value == MEMBER);
        }
    }
}

type DynConstraint = Box<dyn Constraint<'static>>;

fn boxed(fixed: bool, coverage: ProposalCoverage) -> DynConstraint {
    Box::new(ReceiptDomain::new(fixed, coverage))
}

fn proposed<'a, C>(constraint: &C, variable: VariableId) -> Vec<RawInline>
where
    C: Constraint<'a> + ?Sized,
{
    let mut values = Vec::new();
    constraint.propose(
        variable,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut values),
    );
    values
}

fn project_x(binding: &Binding) -> Option<RawInline> {
    binding.get(X).copied()
}

fn project_xy(binding: &Binding) -> Option<(RawInline, RawInline)> {
    Some((binding.get(X).copied()?, binding.get(Y).copied()?))
}

fn unary_scheduler_results<C>(make: impl Fn() -> C) -> Vec<Vec<RawInline>>
where
    C: Constraint<'static> + 'static,
{
    vec![
        Query::new(make(), project_x).sequential().collect(),
        Query::new(make(), project_x)
            .solve_dag_lazy()
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect(),
        Query::new(make(), project_x)
            .solve_dag_lazy()
            .start_width(usize::MAX)
            .collect(),
        Query::new(make(), project_x)
            .solve_residual_state_lazy_with(ResidualLowering::CONSERVATIVE)
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect(),
        Query::new(make(), project_x)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect(),
        Query::new(make(), project_x).collect(),
    ]
}

#[test]
fn receipt_strength_is_a_conservative_proof_order() {
    assert_eq!(ProposalCoverage::default(), ProposalCoverage::None);
    assert!(ProposalCoverage::None < ProposalCoverage::Covering);
    assert!(ProposalCoverage::Covering < ProposalCoverage::Exact);
}

#[test]
fn custom_constraints_remain_uncertified_by_default() {
    let constraint = DefaultReceiptDomain;
    assert!(!constraint.fixed_denotation());
    assert_eq!(
        constraint.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );
}

#[test]
fn exact_and_covering_receipts_describe_support_not_multiplicity() {
    let exact = ReceiptDomain::new(true, ProposalCoverage::Exact);
    let mut exact_support = proposed(&exact, X);
    assert_eq!(exact_support, vec![MEMBER, MEMBER]);
    exact_support.sort_unstable();
    exact_support.dedup();
    assert_eq!(exact_support, vec![MEMBER]);

    let covering = ReceiptDomain::new(true, ProposalCoverage::Covering);
    let mut candidates = proposed(&covering, X);
    assert!(candidates.contains(&MEMBER));
    assert!(candidates.contains(&FALSE_POSITIVE));
    covering.confirm(
        X,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut candidates),
    );
    assert_eq!(candidates, vec![MEMBER, MEMBER]);
}

#[test]
fn transparent_wrappers_forward_semantic_receipts() {
    let inner = ReceiptDomain::new(true, ProposalCoverage::Exact);
    let boxed: Box<ReceiptDomain> = Box::new(inner);
    let shared = Arc::new(inner);
    let debug = DebugConstraint::new(inner, Rc::new(RefCell::new(Vec::new())));
    let estimated = EstimateOverrideConstraint::new(inner);

    for constraint in [
        &*boxed as &dyn Constraint<'static>,
        &*shared,
        &debug,
        &estimated,
    ] {
        assert!(constraint.fixed_denotation());
        assert_eq!(
            constraint.proposal_coverage(X, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );
    }
}

#[test]
fn conjunction_receipts_require_fixed_children_and_weaken_intersections() {
    let singleton = IntersectionConstraint::new(vec![boxed(true, ProposalCoverage::Exact)]);
    assert!(singleton.fixed_denotation());
    assert_eq!(
        singleton.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let exact_intersection = IntersectionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(true, ProposalCoverage::Exact),
    ]);
    assert_eq!(
        exact_intersection.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );

    let one_source = IntersectionConstraint::new(vec![
        boxed(true, ProposalCoverage::None),
        boxed(true, ProposalCoverage::Covering),
    ]);
    assert_eq!(
        one_source.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );

    let validators = IntersectionConstraint::new(vec![
        boxed(true, ProposalCoverage::None),
        boxed(true, ProposalCoverage::None),
    ]);
    assert_eq!(
        validators.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );

    let uncertified = IntersectionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(false, ProposalCoverage::None),
    ]);
    assert!(!uncertified.fixed_denotation());
    assert_eq!(
        uncertified.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );
}

#[test]
fn disjunction_receipts_are_the_meet_of_all_arms() {
    let exact = UnionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(true, ProposalCoverage::Exact),
    ]);
    assert!(exact.fixed_denotation());
    assert_eq!(
        exact.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let covering = UnionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(true, ProposalCoverage::Covering),
    ]);
    assert_eq!(
        covering.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );

    let no_source = UnionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(true, ProposalCoverage::None),
    ]);
    assert_eq!(
        no_source.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );

    let uncertified = UnionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        boxed(false, ProposalCoverage::None),
    ]);
    assert!(!uncertified.fixed_denotation());
    assert_eq!(
        uncertified.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );
}

#[test]
fn structural_receipts_reject_bound_or_unrelated_targets() {
    let constraint = ReceiptDomain::new(true, ProposalCoverage::Exact);
    assert_eq!(
        constraint.proposal_coverage(Y, VariableSet::new_empty()),
        ProposalCoverage::None
    );
    assert_eq!(
        constraint.proposal_coverage(X, VariableSet::new_singleton(X)),
        ProposalCoverage::None
    );
}

#[test]
fn finite_builtin_receipts_distinguish_sources_from_validators() {
    let x = Variable::<UnknownInline>::new(X);
    let constant = ConstantConstraint::new(x, Inline::new(MEMBER));
    assert!(constant.fixed_denotation());
    assert_eq!(
        constant.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let equality = EqualityConstraint::new(X, Y);
    assert!(equality.fixed_denotation());
    assert_eq!(
        equality.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );
    assert_eq!(
        equality.proposal_coverage(X, VariableSet::new_singleton(Y)),
        ProposalCoverage::Exact
    );

    let range = InlineRange::new(x, Inline::new([0x20; 32]), Inline::new([0x40; 32]));
    assert!(range.fixed_denotation());
    assert_eq!(
        range.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::None
    );
}

#[test]
fn indexed_patterns_and_attached_ranges_publish_exact_support() {
    let entity_id = Id::new([0x11; 16]).unwrap();
    let attribute_id = Id::new([0x22; 16]).unwrap();
    let member = Inline::<UnknownInline>::new(MEMBER);
    let mut set = TribleSet::new();
    set.insert(&Trible::force(&entity_id, &attribute_id, &member));
    let entity_constant: Inline<GenId> = entity_id.to_inline();
    let attribute_constant: Inline<GenId> = attribute_id.to_inline();
    let entity = Variable::<GenId>::new(X);
    let attribute = Variable::<GenId>::new(Y);
    let value = Variable::<UnknownInline>::new(2);
    let pattern = set.pattern(entity, attribute, value);
    assert!(pattern.fixed_denotation());
    for variable in [X, Y, 2] {
        assert_eq!(
            pattern.proposal_coverage(variable, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );
    }

    let value_fiber = set.pattern(entity_constant, attribute_constant, value);
    assert_eq!(
        value_fiber.proposal_coverage(2, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );
    assert_eq!(proposed(&value_fiber, 2), vec![MEMBER]);

    let value_range = set.value_in_range(value, Inline::new([0x30; 32]), Inline::new([0x40; 32]));
    let entity_range = set.entity_in_range(
        entity,
        Id::new([0x10; 16]).unwrap(),
        Id::new([0x12; 16]).unwrap(),
    );
    let attribute_range = set.attribute_in_range(
        attribute,
        Id::new([0x20; 16]).unwrap(),
        Id::new([0x23; 16]).unwrap(),
    );
    for (constraint, variable, expected) in [
        (&value_range as &dyn Constraint<'static>, 2, MEMBER),
        (&entity_range, X, entity_constant.raw),
        (&attribute_range, Y, attribute_constant.raw),
    ] {
        assert!(constraint.fixed_denotation());
        assert_eq!(
            constraint.proposal_coverage(variable, VariableSet::new_empty()),
            ProposalCoverage::Exact
        );
        assert_eq!(proposed(constraint, variable), vec![expected]);
    }

    let archive: SuccinctArchive<OrderedUniverse> = (&set).into();
    let succinct = archive.pattern(entity, attribute, value);
    assert!(succinct.fixed_denotation());
    assert_eq!(
        succinct.proposal_coverage(2, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );
    let succinct_value_fiber = archive.pattern(entity_constant, attribute_constant, value);
    assert_eq!(
        succinct_value_fiber.proposal_coverage(2, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );
    assert_eq!(proposed(&succinct_value_fiber, 2), vec![MEMBER]);
    let succinct_range =
        archive.value_in_range(value, Inline::new([0x30; 32]), Inline::new([0x40; 32]));
    assert!(succinct_range.fixed_denotation());
    assert_eq!(
        succinct_range.proposal_coverage(2, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );
    assert_eq!(proposed(&succinct_range, 2), vec![MEMBER]);
}

#[test]
fn regular_paths_are_covering_sources_for_free_endpoints() {
    let path = RegularPathConstraint::new(
        TribleSet::new(),
        Variable::<GenId>::new(X),
        Variable::<GenId>::new(Y),
        &[PathOp::Attr([0x44; 16])],
    );
    assert!(path.fixed_denotation());
    assert_eq!(
        path.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    assert_eq!(
        path.proposal_coverage(Y, VariableSet::new_singleton(X)),
        ProposalCoverage::Exact
    );
    assert_eq!(
        path.proposal_coverage(X, VariableSet::new_singleton(Y)),
        ProposalCoverage::Exact
    );
    assert_eq!(
        path.proposal_coverage(Y, VariableSet::new_singleton(Y)),
        ProposalCoverage::None
    );
}

#[test]
fn estimate_override_preserves_same_variable_rpq_program_receipt() {
    let variable = Variable::<GenId>::new(X);
    let path = RegularPathConstraint::new(
        TribleSet::new(),
        variable,
        variable,
        &[PathOp::Attr([0x45; 16])],
    );
    assert_eq!(
        path.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    assert_eq!(
        path.residual_program_proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let estimated = EstimateOverrideConstraint::new(path);
    assert_eq!(
        estimated.residual_program_proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact,
        "a cardinality-only wrapper must preserve the routed Program receipt"
    );
}

#[derive(Clone, Copy)]
struct QuotelessExact;

impl Constraint<'static> for QuotelessExact {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == X && !bound.is_set(X) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _out: &mut EstimateSink<'_>,
    ) -> bool {
        false
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            for row in 0..view.len() as u32 {
                candidates.push(row, MEMBER);
            }
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            candidates.retain(|_, value| *value == MEMBER);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(X)
            .is_none_or(|column| view.iter().all(|row| row[column] == MEMBER))
    }
}

#[derive(Clone)]
struct CountedExact {
    confirms: Arc<AtomicUsize>,
}

impl Constraint<'static> for CountedExact {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == X && !bound.is_set(X) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != X {
            return false;
        }
        out.fill(1, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            for row in 0..view.len() as u32 {
                candidates.push(row, MEMBER);
            }
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        self.confirms.fetch_add(1, Ordering::Relaxed);
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(X)
            .is_none_or(|column| view.iter().all(|row| row[column] == MEMBER))
    }
}

#[derive(Clone, Copy)]
struct ClosedFalse;

impl Constraint<'static> for ClosedFalse {
    fn variables(&self) -> VariableSet {
        VariableSet::new_empty()
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn estimate(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _out: &mut EstimateSink<'_>,
    ) -> bool {
        false
    }

    fn propose(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        candidates.retain(|_, _| false);
    }

    fn satisfied(&self, _view: &RowsView<'_>) -> bool {
        false
    }
}

struct OptimisticExposedAnd {
    inner: IntersectionConstraint<DynConstraint>,
}

impl Constraint<'static> for OptimisticExposedAnd {
    fn variables(&self) -> VariableSet {
        self.inner.variables()
    }

    fn fixed_denotation(&self) -> bool {
        true
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

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        if self
            .variables()
            .into_iter()
            .all(|variable| view.col(variable).is_some())
        {
            self.inner.satisfied(view)
        } else {
            true
        }
    }

    fn residual_shape(&self) -> ConstraintShape<'_, 'static> {
        ConstraintShape::And(&self.inner)
    }
}

#[test]
fn covering_roots_self_confirm_before_every_scheduler_publishes() {
    for results in unary_scheduler_results(|| ReceiptDomain::new(true, ProposalCoverage::Covering))
    {
        assert_eq!(results, vec![MEMBER]);
    }
}

#[test]
fn quote_less_exact_sources_remain_enabled_at_unknown_cost() {
    for results in unary_scheduler_results(|| QuotelessExact) {
        assert_eq!(results, vec![MEMBER]);
    }
}

#[test]
fn exact_sources_do_not_pay_a_redundant_self_confirm() {
    let confirms = Arc::new(AtomicUsize::new(0));
    for results in unary_scheduler_results(|| CountedExact {
        confirms: confirms.clone(),
    }) {
        assert_eq!(results, vec![MEMBER]);
    }
    assert_eq!(confirms.load(Ordering::Relaxed), 0);
}

fn dynamic_equality_root() -> IntersectionConstraint<DynConstraint> {
    IntersectionConstraint::new(vec![
        boxed(true, ProposalCoverage::Exact),
        Box::new(EqualityConstraint::new(X, Y)),
    ])
}

fn assert_missing_source_at_construction<T>(construct: impl FnOnce() -> T) {
    let payload = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(construct)) {
        Ok(_) => panic!("source-less certified query constructed successfully"),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    assert!(
        message.contains("fixed-denotation query state has no covering proposal source"),
        "unexpected construction panic: {message}"
    );
}

#[test]
fn source_less_certified_roots_fail_at_query_construction() {
    assert_missing_source_at_construction(|| Query::new(EqualityConstraint::new(X, Y), project_xy));

    let x = Variable::<UnknownInline>::new(X);
    assert_missing_source_at_construction(|| {
        Query::new(
            InlineRange::new(x, Inline::new([0x20; 32]), Inline::new([0x40; 32])),
            project_x,
        )
    });

    assert_missing_source_at_construction(|| {
        Query::new(
            UnionConstraint::new(vec![
                boxed(true, ProposalCoverage::Exact),
                boxed(true, ProposalCoverage::None),
            ]),
            project_x,
        )
    });
}

#[test]
fn a_seed_proven_false_needs_no_proposal_source() {
    let make = || OptimisticExposedAnd {
        inner: IntersectionConstraint::new(vec![
            Box::new(ClosedFalse) as DynConstraint,
            boxed(true, ProposalCoverage::None),
        ]),
    };
    for results in unary_scheduler_results(make) {
        assert!(results.is_empty());
    }
}

#[test]
fn equality_becomes_a_source_only_after_its_peer_is_bound() {
    let mut result_sets = vec![
        Query::new(dynamic_equality_root(), project_xy)
            .sequential()
            .collect::<Vec<_>>(),
        Query::new(dynamic_equality_root(), project_xy)
            .solve_dag_lazy()
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect(),
        Query::new(dynamic_equality_root(), project_xy)
            .solve_residual_state_lazy_with(ResidualLowering::FULL)
            .cap(1)
            .start_width(1)
            .growth(1)
            .collect(),
    ];
    for results in &mut result_sets {
        results.sort_unstable();
        assert_eq!(results, &[(MEMBER, MEMBER)]);
    }
}

#[test]
fn exposed_closed_false_child_kills_an_optimistic_certified_seed() {
    let make = || OptimisticExposedAnd {
        inner: IntersectionConstraint::new(vec![
            Box::new(ClosedFalse) as DynConstraint,
            boxed(true, ProposalCoverage::Exact),
        ]),
    };
    for results in unary_scheduler_results(make) {
        assert!(results.is_empty());
    }
}

#[derive(Clone)]
struct ProposalProbe {
    fixed: bool,
    coverage: ProposalCoverage,
    quote: Option<usize>,
    proposals: Arc<AtomicUsize>,
}

impl Constraint<'static> for ProposalProbe {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn fixed_denotation(&self) -> bool {
        self.fixed
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if self.fixed && variable == X && !bound.is_set(X) {
            self.coverage
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        let Some(quote) = self.quote.filter(|_| variable == X) else {
            return false;
        };
        out.fill(quote, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != X {
            return;
        }
        self.proposals.fetch_add(1, Ordering::Relaxed);
        for row in 0..view.len() as u32 {
            candidates.push(row, MEMBER);
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            candidates.retain(|_, value| *value == MEMBER);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(X)
            .is_none_or(|column| view.iter().all(|row| row[column] == MEMBER))
    }
}

fn proposal_probe(
    fixed: bool,
    coverage: ProposalCoverage,
    quote: Option<usize>,
    proposals: &Arc<AtomicUsize>,
) -> DynConstraint {
    Box::new(ProposalProbe {
        fixed,
        coverage,
        quote,
        proposals: proposals.clone(),
    })
}

fn nested_intersection_mode_probe(
    certified_root: bool,
    legacy_proposals: &Arc<AtomicUsize>,
    exact_proposals: &Arc<AtomicUsize>,
) -> IntersectionConstraint<DynConstraint> {
    let passive = Arc::new(AtomicUsize::new(0));
    let nested = IntersectionConstraint::new(vec![
        proposal_probe(true, ProposalCoverage::None, Some(1), legacy_proposals),
        proposal_probe(true, ProposalCoverage::Exact, Some(10), exact_proposals),
    ]);
    IntersectionConstraint::new(vec![
        Box::new(nested),
        proposal_probe(certified_root, ProposalCoverage::None, None, &passive),
    ])
}

fn nested_union_mode_probe(
    certified_root: bool,
    union_proposals: &Arc<AtomicUsize>,
    sibling_proposals: &Arc<AtomicUsize>,
) -> IntersectionConstraint<DynConstraint> {
    let passive = Arc::new(AtomicUsize::new(0));
    let nested = UnionConstraint::new(vec![
        proposal_probe(true, ProposalCoverage::Exact, Some(1), union_proposals),
        proposal_probe(true, ProposalCoverage::Exact, None, union_proposals),
    ]);
    IntersectionConstraint::new(vec![
        Box::new(nested),
        proposal_probe(true, ProposalCoverage::Exact, Some(10), sibling_proposals),
        proposal_probe(certified_root, ProposalCoverage::None, None, &passive),
    ])
}

#[test]
fn receipt_planning_activates_only_when_the_whole_root_is_certified() {
    let legacy_proposals = Arc::new(AtomicUsize::new(0));
    let exact_proposals = Arc::new(AtomicUsize::new(0));
    for results in unary_scheduler_results(|| {
        nested_intersection_mode_probe(false, &legacy_proposals, &exact_proposals)
    }) {
        assert_eq!(results, vec![MEMBER]);
    }
    assert!(legacy_proposals.load(Ordering::Relaxed) > 0);
    assert_eq!(exact_proposals.load(Ordering::Relaxed), 0);

    legacy_proposals.store(0, Ordering::Relaxed);
    exact_proposals.store(0, Ordering::Relaxed);
    for results in unary_scheduler_results(|| {
        nested_intersection_mode_probe(true, &legacy_proposals, &exact_proposals)
    }) {
        assert_eq!(results, vec![MEMBER]);
    }
    assert_eq!(legacy_proposals.load(Ordering::Relaxed), 0);
    assert!(exact_proposals.load(Ordering::Relaxed) > 0);

    let union_proposals = Arc::new(AtomicUsize::new(0));
    let sibling_proposals = Arc::new(AtomicUsize::new(0));
    for results in unary_scheduler_results(|| {
        nested_union_mode_probe(false, &union_proposals, &sibling_proposals)
    }) {
        assert_eq!(results, vec![MEMBER]);
    }
    assert!(union_proposals.load(Ordering::Relaxed) > 0);
    assert_eq!(sibling_proposals.load(Ordering::Relaxed), 0);

    union_proposals.store(0, Ordering::Relaxed);
    sibling_proposals.store(0, Ordering::Relaxed);
    for results in unary_scheduler_results(|| {
        nested_union_mode_probe(true, &union_proposals, &sibling_proposals)
    }) {
        assert_eq!(results, vec![MEMBER]);
    }
    assert_eq!(union_proposals.load(Ordering::Relaxed), 0);
    assert!(sibling_proposals.load(Ordering::Relaxed) > 0);
}

#[test]
fn opaque_transparent_wrapper_forwards_certified_actions() {
    let legacy_proposals = Arc::new(AtomicUsize::new(0));
    let exact_proposals = Arc::new(AtomicUsize::new(0));

    for results in unary_scheduler_results(|| {
        EstimateOverrideConstraint::new(nested_intersection_mode_probe(
            true,
            &legacy_proposals,
            &exact_proposals,
        ))
    }) {
        assert_eq!(results, vec![MEMBER]);
    }

    assert_eq!(legacy_proposals.load(Ordering::Relaxed), 0);
    assert!(exact_proposals.load(Ordering::Relaxed) > 0);
}

#[derive(Clone)]
struct CoveringProposalWithExactProgram {
    ordinary_proposes: Arc<AtomicUsize>,
    ordinary_confirms: Arc<AtomicUsize>,
    program_seeds: Arc<AtomicUsize>,
    exposure: ProgramExposure,
}

impl TypedProgramSpec for CoveringProposalWithExactProgram {
    type State = ();
    type NoveltyKey = ();
    type Rank = u8;

    fn route(&self, request: ProgramRequest) -> Option<ProgramRoute> {
        if request.action != ProgramAction::Propose(X) || request.bound.is_set(X) {
            return None;
        }
        Some(ProgramRoute {
            key: ProgramKey::new(0),
            variable: X,
            stratum: ProgramStratum::Finite,
            grouping: ProgramGrouping::PageLocal,
            completion: ProgramCompletion::PageableOnly,
            exposure: self.exposure,
        })
    }

    fn dispatch(&self, _state: &Self::State) -> DispatchClass {
        DispatchClass::new(0)
    }

    fn progress(&self, _state: &Self::State) -> Self::Rank {
        0
    }

    fn seed_typed(
        &self,
        batch: ProgramSeedBatch<'_>,
        effects: &mut TypedSeedSink<Self::State, Self::NoveltyKey>,
    ) {
        self.program_seeds.fetch_add(1, Ordering::Relaxed);
        for parent in 0..batch.view.len() {
            effects.finite_root(parent as u32, (), Some(MEMBER));
        }
    }

    fn step_typed(
        &self,
        states: triblespace_core::query::TypedProgramStateBatch<Self::State>,
        _batch: TypedProgramBatch<'_>,
        effects: &mut TypedEffectSink<Self::State, Self::NoveltyKey>,
    ) {
        for () in states {
            effects.page(0, None);
        }
    }
}

impl Constraint<'static> for CoveringProposalWithExactProgram {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == X && !bound.is_set(X) {
            ProposalCoverage::Covering
        } else {
            ProposalCoverage::None
        }
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != X {
            return false;
        }
        out.fill(2, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != X {
            return;
        }
        self.ordinary_proposes.fetch_add(1, Ordering::Relaxed);
        for row in 0..view.len() as u32 {
            candidates.push(row, MEMBER);
            candidates.push(row, FALSE_POSITIVE);
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            self.ordinary_confirms.fetch_add(1, Ordering::Relaxed);
            candidates.retain(|_, value| *value == MEMBER);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(X)
            .is_none_or(|column| view.iter().all(|row| row[column] == MEMBER))
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }

    fn residual_program(&self) -> Option<ProgramRef<'_>> {
        Some(ProgramRef::new(self))
    }

    fn residual_program_proposal_coverage(
        &self,
        variable: VariableId,
        bound: VariableSet,
    ) -> ProposalCoverage {
        if variable == X && !bound.is_set(X) {
            ProposalCoverage::Exact
        } else {
            ProposalCoverage::None
        }
    }
}

#[derive(Clone)]
struct NonPageLocalUniversalSibling {
    confirms: Arc<AtomicUsize>,
}

impl Constraint<'static> for NonPageLocalUniversalSibling {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != X {
            return false;
        }
        out.fill(100, view.len());
        true
    }

    fn propose(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        panic!("a receipt-less sibling became the proposal source")
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        if variable == X {
            self.confirms.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn satisfied(&self, _view: &RowsView<'_>) -> bool {
        true
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        false
    }
}

#[test]
fn exact_program_receipt_does_not_discharge_covering_stable_fallback() {
    let ordinary_proposes = Arc::new(AtomicUsize::new(0));
    let ordinary_confirms = Arc::new(AtomicUsize::new(0));
    let program_seeds = Arc::new(AtomicUsize::new(0));
    let sibling_confirms = Arc::new(AtomicUsize::new(0));
    let proposer = CoveringProposalWithExactProgram {
        ordinary_proposes: ordinary_proposes.clone(),
        ordinary_confirms: ordinary_confirms.clone(),
        program_seeds: program_seeds.clone(),
        exposure: ProgramExposure::Production,
    };
    assert_eq!(
        proposer.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    assert_eq!(
        proposer.residual_program_proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let root = IntersectionConstraint::new(vec![
        Box::new(proposer) as DynConstraint,
        Box::new(NonPageLocalUniversalSibling {
            confirms: sibling_confirms.clone(),
        }) as DynConstraint,
    ]);
    let results: Vec<_> = Query::new(root, project_x)
        .solve_residual_state_lazy_with(ResidualLowering::new(
            FormulaScope::OpaqueLeaves,
            ProgramScope::All,
        ))
        .collect();

    assert_eq!(results, vec![MEMBER]);
    assert!(ordinary_proposes.load(Ordering::Relaxed) > 0);
    assert!(
        ordinary_confirms.load(Ordering::Relaxed) > 0,
        "ordinary Covering output must self-confirm after Program seeding is rejected"
    );
    assert!(sibling_confirms.load(Ordering::Relaxed) > 0);
    assert_eq!(
        program_seeds.load(Ordering::Relaxed),
        0,
        "the non-page-local sibling must keep this proposal on stable execution"
    );
}

#[test]
fn deferred_exact_program_receipt_does_not_discharge_covering_ordinary_source() {
    let ordinary_proposes = Arc::new(AtomicUsize::new(0));
    let ordinary_confirms = Arc::new(AtomicUsize::new(0));
    let program_seeds = Arc::new(AtomicUsize::new(0));
    let proposer = CoveringProposalWithExactProgram {
        ordinary_proposes: ordinary_proposes.clone(),
        ordinary_confirms: ordinary_confirms.clone(),
        program_seeds: program_seeds.clone(),
        exposure: ProgramExposure::Explicit,
    };
    assert_eq!(
        proposer.proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    assert_eq!(
        proposer.residual_program_proposal_coverage(X, VariableSet::new_empty()),
        ProposalCoverage::Exact
    );

    let results: Vec<_> = Query::new(proposer, project_x)
        .solve_residual_state_lazy_with(ResidualLowering::HYBRID)
        .collect();

    assert_eq!(results, vec![MEMBER]);
    assert!(ordinary_proposes.load(Ordering::Relaxed) > 0);
    assert!(
        ordinary_confirms.load(Ordering::Relaxed) > 0,
        "a deferred Exact Program receipt must leave Covering self-confirmation active"
    );
    assert_eq!(
        program_seeds.load(Ordering::Relaxed),
        0,
        "a deferred Explicit route must not seed its Program"
    );
}
