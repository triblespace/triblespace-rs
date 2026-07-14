//! Regression gates for candidate-granular residual paging.
//!
//! The zero-stride case is ignored while the 89bf2799 probe is known to
//! panic: independently filtered pages can reconverge as multiple virtual
//! empty parents, but a zero-column `RowsView` represents only one seed row.
//! Run it explicitly with:
//!
//! ```text
//! cargo test -p triblespace-core --test residual_candidate_pages_regression \
//!     candidate_pages_preserve_zero_stride_parent -- --ignored
//! ```

use triblespace_core::inline::RawInline;
use triblespace_core::query::intersectionconstraint::IntersectionConstraint;
use triblespace_core::query::{
    Binding, CandidateSink, Constraint, EstimateSink, Query, RowsView, VariableId, VariableSet,
};

type Leaf = Box<dyn Constraint<'static> + Send + Sync>;

fn raw(byte: u8) -> RawInline {
    let mut value = [0; 32];
    value[0] = byte;
    value
}

#[derive(Clone, Copy)]
struct Fanout {
    variable: VariableId,
    count: u8,
}

impl Constraint<'static> for Fanout {
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
        out.fill(self.count as usize, view.len());
        true
    }

    fn propose(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, self.variable);
        for row in 0..view.len() {
            candidates.extend_row(row as u32, (0..self.count).map(raw));
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }
}

#[derive(Clone, Copy)]
struct Parity {
    variable: VariableId,
    estimate: usize,
}

impl Constraint<'static> for Parity {
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
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }

    fn confirm(
        &self,
        variable: VariableId,
        _view: &RowsView<'_>,
        candidates: &mut CandidateSink<'_>,
    ) {
        assert_eq!(variable, self.variable);
        candidates.retain(|_, value| value[0] % 2 == 0);
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

#[derive(Clone, Copy)]
struct AllPass {
    variable: VariableId,
    estimate: usize,
}

impl Constraint<'static> for AllPass {
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
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }

    fn residual_confirm_is_page_local(&self) -> bool {
        true
    }
}

fn filter_root(variable: VariableId) -> IntersectionConstraint<Leaf> {
    IntersectionConstraint::new(vec![
        Box::new(Fanout { variable, count: 8 }) as Leaf,
        Box::new(Parity {
            variable,
            estimate: 9,
        }) as Leaf,
        Box::new(AllPass {
            variable,
            estimate: 10,
        }) as Leaf,
    ])
}

fn solve_zero_stride(sequential: bool) -> Vec<RawInline> {
    let query = Query::new(filter_root(0), |binding: &Binding| binding.get(0).copied());
    let mut rows: Vec<_> = if sequential {
        query.sequential().collect()
    } else {
        query
            .solve_residual_state_lazy()
            .cap(8)
            .start_width(1)
            .growth(2)
            .collect()
    };
    rows.sort_unstable();
    rows
}

#[test]
#[ignore = "known 89bf2799 blocker: zero-stride page reconvergence creates an invalid multi-seed RowsView"]
fn candidate_pages_preserve_zero_stride_parent() {
    let expected = vec![raw(0), raw(2), raw(4), raw(6)];
    assert_eq!(solve_zero_stride(true), expected);
    assert_eq!(solve_zero_stride(false), expected);
}

#[derive(Clone, Copy)]
struct ParentFanout;

impl Constraint<'static> for ParentFanout {
    fn variables(&self) -> VariableSet {
        VariableSet::new_singleton(0)
    }

    fn estimate(
        &self,
        variable: VariableId,
        view: &RowsView<'_>,
        out: &mut EstimateSink<'_>,
    ) -> bool {
        if variable != 0 {
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
        assert_eq!(variable, 0);
        for row in 0..view.len() {
            candidates.extend_row(row as u32, [raw(10), raw(11)]);
        }
    }

    fn confirm(
        &self,
        _variable: VariableId,
        _view: &RowsView<'_>,
        _candidates: &mut CandidateSink<'_>,
    ) {
    }
}

fn solve_nonzero_stride(sequential: bool) -> Vec<(RawInline, RawInline)> {
    let root = IntersectionConstraint::new(vec![
        Box::new(ParentFanout) as Leaf,
        Box::new(Fanout {
            variable: 1,
            count: 8,
        }) as Leaf,
        Box::new(Parity {
            variable: 1,
            estimate: 9,
        }) as Leaf,
        Box::new(AllPass {
            variable: 1,
            estimate: 10,
        }) as Leaf,
    ]);
    let query = Query::new(root, |binding: &Binding| {
        Some((*binding.get(0)?, *binding.get(1)?))
    });
    let mut rows: Vec<_> = if sequential {
        query.sequential().collect()
    } else {
        query
            .solve_residual_state_lazy()
            .cap(8)
            .start_width(1)
            .growth(2)
            .collect()
    };
    rows.sort_unstable();
    rows
}

#[test]
fn candidate_pages_preserve_nonzero_stride_parent_tags() {
    assert_eq!(solve_nonzero_stride(false), solve_nonzero_stride(true));
}
