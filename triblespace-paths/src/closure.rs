use std::collections::{BTreeMap, BTreeSet};

use triblespace_core::inline::RawInline;

use crate::automaton::StateId;
use crate::index::{BuildStats, ProductPoint};

/// A reflexive, transitively closed relation over a fixed product carrier.
/// Rows and columns are redundant on purpose: one inserted edge then closes by
/// exactly `predecessors(source) × successors(target)`.
#[derive(Clone, Debug)]
pub(crate) struct Closure {
    points: Vec<ProductPoint>,
    position: BTreeMap<ProductPoint, usize>,
    rows: Vec<BTreeSet<usize>>,
    columns: Vec<BTreeSet<usize>>,
}

impl Closure {
    pub(crate) fn new(vertices: &[RawInline], state_count: StateId) -> Self {
        let point_count = vertices
            .len()
            .checked_mul(state_count as usize)
            .expect("product carrier exceeds the address space");
        let mut points = Vec::with_capacity(point_count);
        for &vertex in vertices {
            for state in 0..state_count {
                points.push(ProductPoint { vertex, state });
            }
        }

        let position = points
            .iter()
            .copied()
            .enumerate()
            .map(|(index, point)| (point, index))
            .collect::<BTreeMap<_, _>>();
        let mut rows = vec![BTreeSet::new(); points.len()];
        let mut columns = vec![BTreeSet::new(); points.len()];
        for index in 0..points.len() {
            rows[index].insert(index);
            columns[index].insert(index);
        }

        Self {
            points,
            position,
            rows,
            columns,
        }
    }

    /// Inserts one edge into an already closed relation.
    ///
    /// If `R` is reflexive and transitive, then
    /// `rtc(R ∪ {u→v}) = R ∪ (pred_R(u) × succ_R(v))`. The predecessor and
    /// successor sets are snapped before mutation, so rows and columns remain
    /// exact mirrors of the same closure.
    pub(crate) fn insert(
        &mut self,
        source: ProductPoint,
        target: ProductPoint,
        stats: &mut BuildStats,
    ) {
        stats.seed_pairs_considered += 1;
        let source = self.position[&source];
        let target = self.position[&target];
        if self.rows[source].contains(&target) {
            return;
        }

        let predecessors = self.columns[source].clone();
        let successors = self.rows[target].clone();
        stats.effective_insertions += 1;
        stats.largest_rectangle = stats
            .largest_rectangle
            .max(predecessors.len().saturating_mul(successors.len()));

        let mut added = 0usize;
        for predecessor in predecessors {
            for &successor in &successors {
                if self.rows[predecessor].insert(successor) {
                    self.columns[successor].insert(predecessor);
                    added += 1;
                }
            }
        }

        // The direct pair is necessarily novel; everything else is closure
        // created by the insertion.
        stats.pairs_added += added;
        stats.derived_pairs += added - 1;
    }

    pub(crate) fn reaches(&self, source: ProductPoint, target: ProductPoint) -> bool {
        let Some(&source) = self.position.get(&source) else {
            return false;
        };
        let Some(&target) = self.position.get(&target) else {
            return false;
        };
        self.rows[source].contains(&target)
    }

    pub(crate) fn point_index(&self, point: ProductPoint) -> Option<usize> {
        self.position.get(&point).copied()
    }

    pub(crate) fn row(&self, source: usize) -> &BTreeSet<usize> {
        &self.rows[source]
    }

    pub(crate) fn point(&self, index: usize) -> ProductPoint {
        self.points[index]
    }

    pub(crate) fn pair_count(&self) -> usize {
        self.rows.iter().map(BTreeSet::len).sum()
    }

    pub(crate) fn pairs(&self) -> impl Iterator<Item = (ProductPoint, ProductPoint)> + '_ {
        self.rows.iter().enumerate().flat_map(move |(source, row)| {
            row.iter()
                .map(move |&target| (self.points[source], self.points[target]))
        })
    }
}
