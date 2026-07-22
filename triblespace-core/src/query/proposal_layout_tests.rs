use std::cell::RefCell;
use std::hint::black_box;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::debug::query::{DebugConstraint, EstimateOverrideConstraint};
use crate::inline::encodings::UnknownInline;
use crate::query::constantconstraint::ConstantConstraint;
use crate::query::equalityconstraint::EqualityConstraint;
use crate::query::intersectionconstraint::IntersectionConstraint;
use crate::query::unionconstraint::UnionConstraint;

use super::*;

const TARGET: VariableId = 0;
const SELECTOR: VariableId = 1;
const A: RawInline = [0x17; 32];
const B: RawInline = [0x83; 32];
const SELECT_LEFT: RawInline = [0x31; 32];
const SELECT_RIGHT: RawInline = [0x72; 32];

#[derive(Clone)]
struct BasicSource {
    values: &'static [RawInline],
    layout_is_set: bool,
    coverage: ProposalCoverage,
    accepted: &'static [RawInline],
    confirm_calls: Option<Arc<AtomicUsize>>,
}

impl Constraint<'static> for BasicSource {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(TARGET)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == TARGET && !bound.is_set(TARGET) {
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
        if variable != TARGET {
            return false;
        }
        out.fill(self.values.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == TARGET {
            for row in 0..view.len() as u32 {
                candidates.extend_row(row, self.values.iter().copied());
            }
        }
    }

    fn propose_certified_with_receipt(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.propose(variable, view, candidates);
        if self.layout_is_set {
            ProposalLayout::grouped_set()
        } else {
            ProposalLayout::default()
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != TARGET {
            return;
        }
        if let Some(calls) = &self.confirm_calls {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        candidates.retain(|_, value| self.accepted.contains(value));
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(TARGET)
            .is_none_or(|column| view.iter().all(|row| self.accepted.contains(&row[column])))
    }
}

#[derive(Clone)]
struct CountingValidator {
    accepted: &'static [RawInline],
    calls: Arc<AtomicUsize>,
}

impl Constraint<'static> for CountingValidator {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(TARGET)
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
        if variable != TARGET {
            return false;
        }
        out.fill(self.accepted.len(), view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
        assert_ne!(variable, TARGET, "confirmation-only child became a source");
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == TARGET {
            self.calls.fetch_add(1, Ordering::Relaxed);
            candidates.retain(|_, value| self.accepted.contains(value));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(TARGET)
            .is_none_or(|column| view.iter().all(|row| self.accepted.contains(&row[column])))
    }
}

#[derive(Clone)]
struct AdaptiveSource {
    preferred_selector: RawInline,
    layout_is_set: bool,
}

impl Constraint<'static> for AdaptiveSource {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(SELECTOR).union(VariableSet::new_singleton(TARGET))
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == TARGET && bound.is_set(SELECTOR) && !bound.is_set(TARGET) {
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
        if variable != TARGET {
            return false;
        }
        let Some(selector) = view.col(SELECTOR) else {
            return false;
        };
        out.extend(view.iter().map(|row| {
            if row[selector] == self.preferred_selector {
                1
            } else {
                8
            }
        }));
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable != TARGET {
            return;
        }
        let copies = if self.layout_is_set { 1 } else { 2 };
        for row in 0..view.len() as u32 {
            candidates.extend_row(row, std::iter::repeat_n(A, copies));
        }
    }

    fn propose_certified_with_receipt(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.propose(variable, view, candidates);
        if self.layout_is_set {
            ProposalLayout::grouped_set()
        } else {
            ProposalLayout::default()
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == TARGET {
            candidates.retain(|_, value| *value == A);
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(TARGET)
            .is_none_or(|column| view.iter().all(|row| row[column] == A))
    }
}

#[test]
fn conservative_bag_receipt_keeps_reverse_stable_tail_layout() {
    let mut query = Query::new(
        BasicSource {
            values: &[A, B, A],
            layout_is_set: false,
            coverage: ProposalCoverage::Exact,
            accepted: &[A, B],
            confirm_calls: None,
        },
        |_| Some(()),
    );

    query.push_next_variable();

    assert_eq!(query.values[TARGET].as_deref(), Some(&[B, A][..]));
    assert!(
        query.value_admission.capacity() > 0,
        "the conservative receipt must retain scalar hash admission"
    );
}

#[test]
fn grouped_set_receipt_preserves_exact_raw_tail_order_without_hashing() {
    let mut query = Query::new(
        BasicSource {
            values: &[B, A],
            layout_is_set: true,
            coverage: ProposalCoverage::Exact,
            accepted: &[A, B],
            confirm_calls: None,
        },
        |_| Some(()),
    );

    query.push_next_variable();

    assert_eq!(query.values[TARGET].as_deref(), Some(&[B, A][..]));
    assert_eq!(query.value_admission.capacity(), 0);
}

#[test]
fn covering_grouped_set_survives_outer_confirm_without_validation_discharge() {
    let validator_calls = Arc::new(AtomicUsize::new(0));
    let source_confirm_calls = Arc::new(AtomicUsize::new(0));
    let root: Arc<IntersectionConstraint<Box<dyn Constraint<'static> + Send + Sync>>> =
        Arc::new(IntersectionConstraint::new(vec![
            Box::new(BasicSource {
                values: &[B, A],
                layout_is_set: true,
                coverage: ProposalCoverage::Exact,
                accepted: &[A, B],
                confirm_calls: Some(Arc::clone(&source_confirm_calls)),
            }),
            Box::new(CountingValidator {
                accepted: &[A],
                calls: Arc::clone(&validator_calls),
            }),
        ]));
    assert_eq!(
        root.proposal_coverage(TARGET, VariableSet::new_empty()),
        ProposalCoverage::Covering
    );
    let mut query = Query::new(root, |_| Some(()));

    query.push_next_variable();

    assert_eq!(query.values[TARGET].as_deref(), Some(&[A][..]));
    assert_eq!(query.value_admission.capacity(), 0);
    assert_eq!(
        validator_calls.load(Ordering::Relaxed),
        2,
        "the composite proposal confirms once and the outer Covering boundary confirms again"
    );
    assert_eq!(source_confirm_calls.load(Ordering::Relaxed), 1);
}

fn nonuniform_nested_layout(second_is_set: bool) -> (ProposalLayout, Candidates) {
    let nested = IntersectionConstraint::new(vec![AdaptiveSource {
        preferred_selector: SELECT_RIGHT,
        layout_is_set: second_is_set,
    }]);
    let root: IntersectionConstraint<Box<dyn Constraint<'static>>> =
        IntersectionConstraint::new(vec![
            Box::new(AdaptiveSource {
                preferred_selector: SELECT_LEFT,
                layout_is_set: true,
            }),
            Box::new(nested),
        ]);
    let rows = [SELECT_LEFT, SELECT_RIGHT];
    let view = RowsView::new(&[SELECTOR], &rows);
    let mut candidates = Vec::new();
    let layout = root.propose_certified_with_receipt(
        TARGET,
        &view,
        &mut CandidateSink::Tagged(&mut candidates),
    );
    (layout, candidates)
}

#[test]
fn nested_nonuniform_intersection_downgrades_if_any_selected_row_is_a_bag() {
    let (all_set, set_candidates) = nonuniform_nested_layout(true);
    assert!(all_set.is_grouped_set());
    assert_eq!(set_candidates, [(0, A), (1, A)]);

    let (mixed, bag_candidates) = nonuniform_nested_layout(false);
    assert!(!mixed.is_grouped_set());
    assert_eq!(bag_candidates, [(0, A), (1, A), (1, A)]);
}

#[test]
fn union_constant_and_equality_issue_construction_proven_sets() {
    let union: UnionConstraint<Box<dyn Constraint<'static>>> = UnionConstraint::new(vec![
        Box::new(BasicSource {
            values: &[B, A, A],
            layout_is_set: false,
            coverage: ProposalCoverage::Exact,
            accepted: &[A, B],
            confirm_calls: None,
        }),
        Box::new(BasicSource {
            values: &[B, B],
            layout_is_set: false,
            coverage: ProposalCoverage::Exact,
            accepted: &[B],
            confirm_calls: None,
        }),
    ]);
    let mut union_values = Vec::new();
    let union_layout = union.propose_certified_with_receipt(
        TARGET,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut union_values),
    );
    assert!(union_layout.is_grouped_set());
    assert_eq!(union_values, [A, B]);

    let variable = Variable::<UnknownInline>::new(TARGET);
    let constant = ConstantConstraint::new(variable, Inline::new(B));
    let mut constant_values = Vec::new();
    let constant_layout = constant.propose_certified_with_receipt(
        TARGET,
        &RowsView::EMPTY,
        &mut CandidateSink::Values(&mut constant_values),
    );
    assert!(constant_layout.is_grouped_set());
    assert_eq!(constant_values, [B]);

    let equality = EqualityConstraint::new(TARGET, SELECTOR);
    let mut equality_values = Vec::new();
    let equality_layout = equality.propose_certified_with_receipt(
        TARGET,
        &RowsView::new(&[SELECTOR], &[A]),
        &mut CandidateSink::Values(&mut equality_values),
    );
    assert!(equality_layout.is_grouped_set());
    assert_eq!(equality_values, [A]);
}

#[test]
fn diagnostic_wrappers_forward_the_opaque_receipt() {
    let inner = BasicSource {
        values: &[B, A],
        layout_is_set: true,
        coverage: ProposalCoverage::Exact,
        accepted: &[A, B],
        confirm_calls: None,
    };
    let override_constraint = EstimateOverrideConstraint::new(inner);
    let record = Rc::new(RefCell::new(Vec::new()));
    let debug = DebugConstraint::new(override_constraint, Rc::clone(&record));
    let mut query = Query::new(debug, |_| Some(()));

    query.push_next_variable();

    assert_eq!(&*record.borrow(), &[TARGET]);
    assert_eq!(query.values[TARGET].as_deref(), Some(&[B, A][..]));
    assert_eq!(query.value_admission.capacity(), 0);
}

#[derive(Clone, Copy)]
struct WideSource {
    len: usize,
    layout_is_set: bool,
}

fn ordinal_raw(ordinal: usize) -> RawInline {
    let mut raw = [0; 32];
    raw[24..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    raw
}

impl Constraint<'static> for WideSource {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(TARGET)
    }

    fn fixed_denotation(&self) -> bool {
        true
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == TARGET && !bound.is_set(TARGET) {
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
        if variable != TARGET {
            return false;
        }
        out.fill(self.len, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == TARGET {
            for row in 0..view.len() as u32 {
                candidates.extend_row(
                    row,
                    (0..self.len).map(|ordinal| black_box(ordinal_raw(ordinal))),
                );
            }
        }
    }

    fn propose_certified_with_receipt(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) -> ProposalLayout {
        self.propose(variable, view, candidates);
        if self.layout_is_set {
            ProposalLayout::grouped_set()
        } else {
            ProposalLayout::default()
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(TARGET).is_none_or(|column| {
            view.iter().all(|row| {
                row[column][..24].iter().all(|byte| *byte == 0)
                    && u64::from_be_bytes(row[column][24..].try_into().unwrap()) < self.len as u64
            })
        })
    }
}

/// Release-only causal probe of the real scalar receipt path. The ABBA order
/// alternates allocation/thermal drift; no timing threshold is a correctness
/// condition.
#[test]
#[ignore = "release-only scalar GroupedSet receipt timing probe"]
fn scalar_grouped_set_receipt_release_probe() {
    assert!(
        !cfg!(debug_assertions),
        "run with cargo test --release -- --ignored --nocapture"
    );
    const LEN: usize = 1 << 18;
    const ROUNDS: usize = 6;

    println!("probe=scalar-grouped-set-receipt-v1 base=7bf4ac81 len={LEN} rounds={ROUNDS}");
    for round in 0..ROUNDS {
        for (position, layout_is_set) in [false, true, true, false].into_iter().enumerate() {
            let mut query = Query::new(
                WideSource {
                    len: LEN,
                    layout_is_set,
                },
                |_| Some(()),
            );
            let start = Instant::now();
            query.push_next_variable();
            let elapsed = start.elapsed();
            assert_eq!(query.values[TARGET].as_ref().unwrap().len(), LEN);
            if layout_is_set {
                assert_eq!(query.value_admission.capacity(), 0);
            } else {
                assert!(query.value_admission.capacity() >= LEN);
            }
            black_box(query.values[TARGET].as_ref().unwrap().as_ptr());
            println!(
                "round={round} position={position} layout={} elapsed_ns={}",
                if layout_is_set { "set" } else { "bag" },
                elapsed.as_nanos()
            );
        }
    }
}
