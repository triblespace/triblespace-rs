//! Ordinary candidate paging needs weak support refinement, not page
//! homomorphism.
//!
//! The confirmer below deliberately retains different conservative false
//! positives when one candidate relation is presented whole or in pages. It
//! still preserves the true existential fiber and becomes exact after the
//! other occurrence variable is bound.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use triblespace_core::inline::RawInline;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, ProposalCoverage, Query, RowsView,
    VariableId, VariableSet,
};

const X: VariableId = 0;
const Y: VariableId = 1;

fn raw(byte: u8) -> RawInline {
    [byte; 32]
}

type DynConstraint = Box<dyn Constraint<'static> + Send + Sync>;
type Root = Arc<IntersectionConstraint<DynConstraint>>;

struct EnumeratedSource {
    variable: VariableId,
    values: Vec<RawInline>,
    estimate: usize,
}

impl<'a> Constraint<'a> for EnumeratedSource {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(self.variable)
    }

    fn proposal_coverage(&self, variable: VariableId, bound: VariableSet) -> ProposalCoverage {
        if variable == self.variable && !bound.is_set(variable) {
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
        if variable != self.variable {
            return;
        }
        for parent in 0..view.len() {
            candidates.extend_row(
                u32::try_from(parent).expect("too many test parents"),
                self.values.iter().copied(),
            );
        }
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        if variable == self.variable {
            candidates.retain(|_, value| self.values.contains(value));
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        view.col(self.variable)
            .is_none_or(|column| view.iter().all(|row| self.values.contains(&row[column])))
    }
}

/// The fixed relation `{(1, 9)}`.
///
/// With the peer variable unbound, confirmation preserves `(1, _)` and may
/// conservatively retain any other candidate in a page containing at least
/// two values. Thus whole and singleton-page confirmation are observably not
/// homomorphic. Once the peer is bound, confirmation is exact.
struct PageSensitiveWeakRefinement {
    partial_x_pages: Arc<Mutex<Vec<Vec<RawInline>>>>,
}

impl PageSensitiveWeakRefinement {
    fn relation(x: RawInline, y: RawInline) -> bool {
        x == raw(1) && y == raw(9)
    }
}

impl<'a> Constraint<'a> for PageSensitiveWeakRefinement {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(X).union(VariableSet::new_singleton(Y))
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != X && variable != Y {
            return false;
        }
        out.fill(16, view.len());
        true
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
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        let mut pages = vec![Vec::new(); view.len()];
        candidates.for_each(|parent, value| pages[parent as usize].push(*value));

        match (variable, view.col(X), view.col(Y)) {
            (X, _, None) => {
                self.partial_x_pages
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .extend(pages.iter().cloned());
                candidates.retain(|parent, candidate| {
                    *candidate == raw(1) || pages[parent as usize].len() > 1
                });
            }
            (X, _, Some(y)) => candidates.retain(|parent, candidate| {
                Self::relation(*candidate, view.row(parent as usize)[y])
            }),
            (Y, Some(x), _) => candidates.retain(|parent, candidate| {
                Self::relation(view.row(parent as usize)[x], *candidate)
            }),
            (Y, None, _) => candidates.retain(|parent, candidate| {
                *candidate == raw(9) || pages[parent as usize].len() > 1
            }),
            _ => {}
        }
    }

    fn satisfied(&self, view: &RowsView<'_>) -> bool {
        let x = view.col(X);
        let y = view.col(Y);
        view.iter().all(|row| match (x, y) {
            (Some(x), Some(y)) => Self::relation(row[x], row[y]),
            (Some(x), None) => row[x] == raw(1),
            (None, Some(y)) => row[y] == raw(9),
            (None, None) => true,
        })
    }
}

fn fixture() -> (Root, Arc<Mutex<Vec<Vec<RawInline>>>>) {
    let pages = Arc::new(Mutex::new(Vec::new()));
    let root = Arc::new(IntersectionConstraint::new(vec![
        Box::new(EnumeratedSource {
            variable: X,
            // The duplicate makes pre-split SET admission externally visible:
            // the weak confirmer must see three occurrences, not four.
            values: vec![raw(1), raw(2), raw(2), raw(3)],
            estimate: 1,
        }) as DynConstraint,
        Box::new(EnumeratedSource {
            variable: Y,
            values: vec![raw(9)],
            estimate: 2,
        }) as DynConstraint,
        Box::new(PageSensitiveWeakRefinement {
            partial_x_pages: Arc::clone(&pages),
        }) as DynConstraint,
    ]));
    (root, pages)
}

fn project(binding: &Binding) -> Option<(RawInline, RawInline)> {
    Some((*binding.get(X)?, *binding.get(Y)?))
}

fn run() -> (BTreeSet<(RawInline, RawInline)>, Vec<Vec<RawInline>>) {
    let (root, pages) = fixture();
    let profiled = Query::new(root, project)
        .solve_residual_state_lazy()
        .collect_profiled();
    let pages = pages
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    (profiled.results.into_iter().collect(), pages)
}

#[test]
fn weak_refinement_need_not_be_a_candidate_page_homomorphism() {
    let pages = Arc::new(Mutex::new(Vec::new()));
    let constraint = PageSensitiveWeakRefinement {
        partial_x_pages: pages,
    };

    let mut whole = vec![raw(1), raw(2), raw(3)];
    constraint.confirm(X, &RowsView::EMPTY, &mut CandidateSink::Values(&mut whole));
    assert_eq!(whole, vec![raw(1), raw(2), raw(3)]);

    let mut paged = Vec::new();
    for value in [raw(1), raw(2), raw(3)] {
        let mut page = vec![value];
        constraint.confirm(X, &RowsView::EMPTY, &mut CandidateSink::Values(&mut page));
        paged.extend(page);
    }
    assert_eq!(paged, vec![raw(1)]);
    assert_ne!(paged, whole, "the obsolete strong page law must be false");
}

#[test]
fn ordinary_paging_set_admits_then_reaches_the_exact_raw_relation() {
    let expected = BTreeSet::from([(raw(1), raw(9))]);
    let (actual, pages) = run();
    assert_eq!(actual, expected);
    assert_eq!(
        pages.iter().map(Vec::len).sum::<usize>(),
        3,
        "duplicate proposals crossed SET admission"
    );
}

#[cfg(feature = "parallel")]
#[test]
fn parallel_affine_partitioning_preserves_the_weak_refinement_set() {
    let (root, _) = fixture();
    let actual: BTreeSet<_> = Query::new(root, project)
        .solve_residual_state_lazy()
        .into_par_iter()
        .collect();
    assert_eq!(actual, BTreeSet::from([(raw(1), raw(9))]));
}
