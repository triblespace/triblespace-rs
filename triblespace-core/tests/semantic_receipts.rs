use std::cell::RefCell;
use std::rc::Rc;
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
    CandidateSink, Constraint, EstimateSink, PathOp, ProposalCoverage, RegularPathConstraint,
    RowsView, TriblePattern, Variable, VariableId, VariableSet,
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
        ProposalCoverage::Covering
    );
    assert_eq!(
        path.proposal_coverage(Y, VariableSet::new_singleton(Y)),
        ProposalCoverage::None
    );
}
